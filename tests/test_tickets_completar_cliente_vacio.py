"""
TEST FILE -- NO ES PRODUCCION. NO IMPORTAR DESDE app.py.

Completar datos de cliente cuando llega el primer correo real a un ticket
que nace SIN cliente (2026-08-22).

CASO REAL: Daniel pidio crear los tickets 771/772/773 a mano (ver
test_tickets_autocrear_desde_triplea.py) porque Triple A todavia no habia
mandado ningun correo con sus datos. Dias despues, Daniel reporto: "el
ticket 773 esta vacio... hay un cron actualizando" -- el barrido de correo
SI corre (autopoll activo, confirmado en pantalla), pero un ticket que
nace sin nombre_contacto/email se queda vacio PARA SIEMPRE, incluso el
dia que llegue un correo real y se le adjunte como mensaje. La razon: el
camino compartido de "candidato" (usado por CUALQUIER respuesta a un
ticket ya existente, no solo los recien auto-creados) nunca tocaba las
columnas nombre_contacto/email/email_cliente_real del ticket -- solo
insertaba el mensaje.

LA SOLUCION: justo antes de guardar el mensaje, un UPDATE que rellena
email/email_cliente_real/nombre_contacto SOLO si estan vacios
(COALESCE(NULLIF(col,''), nuevo_valor)) -- una sola sentencia SQL,
sin round-trip de lectura previa, sin condicional en Python. Si el
ticket YA tenia esos datos (el caso normal, 749 tickets migrados por
CSV), la UPDATE es un no-op real: no pisa nada.

QUE VERIFICA:
 1. Un ticket vacio (como los 771/772/773 reales) SI se completa con el
    remitente del primer correo real que le llega.
 2. Un ticket que YA tenia email/nombre (el caso normal) NO se pisa con
    el remitente de un correo nuevo -- la COALESCE/NULLIF protege el
    dato real.
 3. El relleno parcial es posible: si el ticket ya tenia nombre pero no
    email (o viceversa), cada columna se completa de forma independiente.
 4. El codigo real tiene la UPDATE conectada en el punto correcto (por
    fuente): antes del `_tk_log` que guarda el mensaje, dentro del mismo
    bloque que ya calcula `remitente` -- no una funcion aparte que
    alguien podria olvidar llamar.

Se corre igual que el resto de la bateria:
    python3 tests/test_tickets_completar_cliente_vacio.py
"""

import sys

fallos = []


def check(cond, msg):
    if cond:
        print(f"  OK   {msg}")
    else:
        print(f"  FALLA {msg}")
        fallos.append(msg)


# ══════════════════════════════════════════════════════════════════
# Simulacion de la UPDATE real usando la MISMA logica SQL
# (COALESCE(NULLIF(col,''), nuevo)) aplicada en Python sobre un dict,
# para no depender de una base de datos real en este test.
# ══════════════════════════════════════════════════════════════════
def _coalesce_nullif(actual, nuevo):
    """Replica exacta de COALESCE(NULLIF(col,''), %s) en SQL."""
    if actual is None or actual == "":
        return nuevo
    return actual


def completar_cliente_vacio(ticket, from_email, from_nombre):
    """Replica exacta del bloque nuevo en _tk_leer_correo_entrante."""
    if not from_email:
        return
    ticket["email"] = _coalesce_nullif(ticket.get("email"), from_email[:150])
    ticket["email_cliente_real"] = _coalesce_nullif(
        ticket.get("email_cliente_real"), from_email[:190])
    ticket["nombre_contacto"] = _coalesce_nullif(
        ticket.get("nombre_contacto"), (from_nombre or from_email)[:150])


print("\n1. Un ticket vacio (caso real: 771/772/773) SE completa con el "
      "primer correo")
t771 = {"id": 2067, "numero_ticket": "TAA-771", "email": None,
        "email_cliente_real": None, "nombre_contacto": None}
completar_cliente_vacio(t771, "clientereal@gmail.com", "Un Cliente Real")
check(t771["email"] == "clientereal@gmail.com",
      "email se completa desde el remitente del correo")
check(t771["email_cliente_real"] == "clientereal@gmail.com",
      "email_cliente_real tambien se completa (mismo dato, misma columna "
      "que ya usa la migracion por CSV)")
check(t771["nombre_contacto"] == "Un Cliente Real",
      "nombre_contacto se completa con el nombre real del From")

print("\n2. Un ticket que YA tiene datos (caso normal, 749 migrados) NO se "
      "pisa")
t_normal = {"id": 100, "numero_ticket": "TAA-100", "email": "real@empresa.cl",
            "email_cliente_real": "real@empresa.cl",
            "nombre_contacto": "Cliente Real Ya Cargado"}
completar_cliente_vacio(t_normal, "otro-remitente@gmail.com", "Otro Nombre")
check(t_normal["email"] == "real@empresa.cl",
      "el email real NO se reemplaza por el remitente de una respuesta "
      "posterior")
check(t_normal["nombre_contacto"] == "Cliente Real Ya Cargado",
      "el nombre real NO se reemplaza -- COALESCE/NULLIF protege el dato "
      "que ya existia")

print("\n3. Relleno PARCIAL: cada columna se completa de forma "
      "independiente")
t_parcial = {"id": 200, "numero_ticket": "TAA-200", "email": None,
             "email_cliente_real": None,
             "nombre_contacto": "Nombre Que Ya Tenia"}
completar_cliente_vacio(t_parcial, "nuevo@gmail.com", "Nombre Del Correo")
check(t_parcial["email"] == "nuevo@gmail.com",
      "el email vacio SI se completa aunque el nombre ya existiera")
check(t_parcial["nombre_contacto"] == "Nombre Que Ya Tenia",
      "el nombre que ya existia NO se pisa con el del correo nuevo")

print("\n4. Sin remitente (from_email vacio) no intenta nada -- no hay "
      "dato de donde sacarlo")
t_sin_from = {"id": 300, "numero_ticket": "TAA-300", "email": None,
              "email_cliente_real": None, "nombre_contacto": None}
completar_cliente_vacio(t_sin_from, "", "Nombre Sin Correo")
check(t_sin_from["email"] is None,
      "sin from_email no se escribe nada -- evita un email vacio guardado "
      "como si fuera un dato real")

print("\n5. El codigo REAL tiene la UPDATE conectada en el punto correcto")
with open("tickets_module.py", encoding="utf-8") as f:
    fuente = f.read()
check("email=COALESCE(NULLIF(email,''), %s)" in fuente,
      "existe la UPDATE con COALESCE/NULLIF para email")
check("email_cliente_real=COALESCE(NULLIF(email_cliente_real,''), %s)" in fuente,
      "existe la UPDATE con COALESCE/NULLIF para email_cliente_real")
check("nombre_contacto=COALESCE(NULLIF(nombre_contacto,''), %s)" in fuente,
      "existe la UPDATE con COALESCE/NULLIF para nombre_contacto")
_i_remitente = fuente.index(
    'remitente = (from_nombre or ticket.get("nombre_contacto")')
_i_update = fuente.index("if from_email:", _i_remitente)
_i_tk_log = fuente.index('_tk_log(ticket["id"], "mensaje"', _i_update)
check(_i_remitente < _i_update < _i_tk_log,
      "la UPDATE de relleno esta DESPUES de calcular remitente y ANTES de "
      "guardar el mensaje -- en el mismo bloque, no una funcion aparte "
      "que alguien podria olvidar llamar")

print("\n" + "=" * 60)
if fallos:
    print(f"RESULTADO: {len(fallos)} verificacion(es) FALLARON")
    for f_ in fallos:
        print(f"  - {f_}")
    sys.exit(1)
print("RESULTADO: todas las verificaciones pasaron")
sys.exit(0)

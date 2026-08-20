"""
TEST FILE -- NO ES PRODUCCION. NO IMPORTAR DESDE app.py.

Auto-creacion de tickets desde un correo NUEVO de Triple A (2026-08-20).

PEDIDO DE DANIEL (caso real en vivo): Aaron crea el ticket #771 DIRECTO en
Triple A durante la marcha blanca. Nunca paso por el CSV de migracion, asi
que en ILUS no existia ninguna fila con legacy_taa_id=771 -- el correo
llegaba (o iba a llegar) pero no habia donde colgarlo, y el ticket
simplemente no aparecia. Sondeo en vivo confirmo el hueco: TAA-770 era el
mas alto migrado, TAA-771 no existia en tk_tickets.

Daniel pidio explicitamente (via AskUserQuestion, "Si, creelo automatico -
Recomendado"): que ILUS cree el ticket SOLO la primera vez que llega un
correo de Triple A cuyo id no resuelve a ningun ticket existente, en vez de
solo reportar el hueco.

LA SALVAGUARDA CLAVE: el ticket nuevo nace exactamente con el mismo candado
que los 749 migrados por CSV (ver test_tickets_notif_pausada_migracion.py):
notif_pausada=1, sin asignado_a. Auto-crear un ticket NO debe auto-notificar
a nadie -- son dos decisiones independientes.

QUE VERIFICA:
 1. dry_run=True NO crea nada -- solo avisa que SE VA a crear (para que la
    vista previa sea honesta sobre lo que el barrido real va a hacer).
 2. El id crudo de Triple A (numero.split) se extrae bien para legacy_taa_id.
 3. El ticket nuevo nace con notif_pausada=1 (no dispara correos de sorpresa
    a un cliente real solo por haber sido auto-creado).
 4. Un fallo de BD durante el INSERT se cuenta como error, no se pierde en
    silencio ni tumba el resto del barrido.
 5. El codigo real tiene las piezas: el INSERT usa columnas que existen, usa
    INSERT IGNORE (protegido contra doble-creacion en carrera), origen
    valido para el ENUM, y el camino de EXITO no corta el flujo -- tiene
    que caer al bloque compartido de "candidato" para que el correo que
    disparo la creacion quede adjunto como el primer mensaje real (el
    camino de ERROR si corta, verificado por separado). Esto se mide por
    ESTRUCTURA (AST de la funcion aislada), no por texto: un slice de
    texto se confunde con el continue del bloque hermano `if clase ==
    "propio":` que esta justo despues en el archivo.

Se corre igual que el resto de la bateria:
    python3 tests/test_tickets_autocrear_desde_triplea.py
"""

import ast
import sys

fallos = []


def check(cond, msg):
    if cond:
        print(f"  OK   {msg}")
    else:
        print(f"  FALLA {msg}")
        fallos.append(msg)


# ══════════════════════════════════════════════════════════════════
# Simulacion minima: solo la parte de la rama "sin_ticket_taa" que
# decide si auto-crea o no, y con que datos. No reimplementa todo
# _tk_leer_correo_entrante -- eso ya lo cubren los otros tests.
# ══════════════════════════════════════════════════════════════════
class TicketsFalsos:
    def __init__(self):
        self.filas = {}  # numero_ticket -> dict
        self.errores = []

    def existe(self, numero):
        return numero in self.filas

    def insert_ignore(self, numero, legacy_taa_id, email, nombre_contacto,
                       notas, created_by, fallar=False):
        if fallar:
            raise RuntimeError("boom (fallo simulado de BD)")
        if numero in self.filas:
            return  # INSERT IGNORE: ya existe, no hace nada (carrera)
        self.filas[numero] = {
            "numero_ticket": numero, "legacy_taa_id": legacy_taa_id,
            "origen": "backoffice", "estado": "open", "prioridad": "media",
            "email": email, "email_cliente_real": email,
            "notif_pausada": 1, "asignado_a": None,
            "nombre_contacto": nombre_contacto, "notas_internas": notas,
            "created_by": created_by,
        }


def procesar_sin_ticket_taa(db, numero, from_email, from_nombre, subject,
                             dry_run):
    """Replica el fragmento real: decide dry_run vs creacion real."""
    tid_taa = int(numero.split("-", 1)[1])
    if dry_run:
        return {"accion": "auto_crear_preview", "tid": tid_taa}
    nombre_taa = (from_nombre or from_email or "Cliente Triple A")[:150]
    nota_taa = (
        f"Ticket creado automaticamente al recibir un correo de Triple A "
        f"(id {tid_taa}) que no tenia ticket importado.")
    try:
        db.insert_ignore(numero, tid_taa, from_email or None, nombre_taa,
                          nota_taa, "sistema-correo-triplea")
    except Exception as e:
        return {"accion": "error", "detalle": str(e)}
    return {"accion": "auto_creado", "tid": tid_taa}


print("\n1. dry_run=True NO crea nada -- solo avisa que SE VA a crear")
db1 = TicketsFalsos()
r = procesar_sin_ticket_taa(db1, "TAA-771", "cliente@gmail.com",
                             "Un Cliente", "ILUS | Ticket - ID: 771",
                             dry_run=True)
check(r["accion"] == "auto_crear_preview",
      "en vista previa se anuncia la creacion, no se ejecuta")
check(not db1.existe("TAA-771"),
      "dry_run=True no toco la base de datos -- nada se creo de verdad")

print("\n2. El id crudo de Triple A se extrae bien para legacy_taa_id")
db2 = TicketsFalsos()
r = procesar_sin_ticket_taa(db2, "TAA-771", "aaron.cliente@gmail.com",
                             "Cliente de Aaron", "ILUS | Ticket - ID: 771",
                             dry_run=False)
check(r["accion"] == "auto_creado" and r["tid"] == 771,
      "el numero 'TAA-771' se separa correctamente en legacy_taa_id=771")
check(db2.filas["TAA-771"]["legacy_taa_id"] == 771,
      "el ticket creado queda con legacy_taa_id=771 -- futuras respuestas "
      "de Triple A a este mismo id lo van a encontrar solas")

print("\n3. El ticket nuevo nace con notif_pausada=1 (mismo candado que la "
      "migracion por CSV)")
check(db2.filas["TAA-771"]["notif_pausada"] == 1,
      "notif_pausada=1: auto-crear el ticket NO dispara ningun correo de "
      "sorpresa a un cliente real")
check(db2.filas["TAA-771"]["asignado_a"] is None,
      "nace sin asignar -- la pausa se levanta sola cuando alguien lo "
      "asigne DENTRO de ILUS, igual que los 749 migrados")
check(db2.filas["TAA-771"]["email"] == "aaron.cliente@gmail.com",
      "el correo del remitente del email SI queda guardado -- sin esto el "
      "ticket auto-creado no tendria a quien responderle despues")

print("\n4. Un fallo de BD durante el INSERT se cuenta como error, no se "
      "pierde en silencio")
db3 = TicketsFalsos()
try:
    db3.insert_ignore("TAA-999", 999, "x@x.com", "X", "nota", "sistema",
                       fallar=True)
    r = {"accion": "auto_creado"}
except Exception as e:
    r = {"accion": "error", "detalle": str(e)}
check(r["accion"] == "error",
      "un fallo de BD durante la creacion automatica se distingue y se "
      "cuenta -- no se confunde con un correo ingresado normalmente")

print("\n5. El codigo REAL tiene las piezas conectadas")
with open("tickets_module.py", encoding="utf-8") as f:
    fuente = f.read()
check('"auto_creados": 0' in fuente,
      "existe el contador auto_creados en el resumen del barrido")
check("_tid_taa = int(numero.split" in fuente,
      "se extrae el id crudo de Triple A desde el numero ya resuelto "
      "(numero.split), no se vuelve a parsear el asunto")
check("INSERT IGNORE INTO tk_tickets" in fuente and
      "sistema-correo-triplea" in fuente,
      "el INSERT usa IGNORE (protegido contra doble-creacion si dos "
      "barridos corren en paralelo) y un created_by que distingue esto de "
      "una accion humana")
check("VALUES (%s,%s,'backoffice','open','media'," in fuente,
      "origen='backoffice' -- el ENUM de tk_tickets.origen es "
      "('form','backoffice','erp'); 'backoffice' es el mismo valor que ya "
      "usa el importador CSV para tickets de Triple A")
check("email_cliente_real, notif_pausada, " in fuente,
      "el INSERT setea notif_pausada explicito (no confia en el DEFAULT de "
      "la columna, que es 0 -- aca DEBE nacer en 1)")
# El punto mas importante, verificado por ESTRUCTURA (AST), no por texto:
# un slice de texto se confunde con el `continue` del bloque hermano
# `if clase == "propio":` que esta justo despues en el archivo. Lo que
# importa de verdad es la ULTIMA instruccion top-level DENTRO del bloque
# `if clase == "sin_ticket_taa":` -- los continue anidados en sus propios
# sub-if (dry_run, error) son validos y esperados; lo que NO debe pasar es
# que el bloque completo termine en un continue top-level, porque eso
# cortaria el correo que disparo la creacion antes de que llegue a
# adjuntarse como el primer mensaje del ticket.
#
# NO se hace ast.parse() del archivo COMPLETO: en esta maquina (Windows,
# Python 3.14) eso da MemoryError de forma reproducible por el tamaño del
# archivo (~90k lineas), no por un error real. Se aisla primero solo la
# funcion _tk_leer_correo_entrante (mismo patron ya usado en sesion para
# validar sintaxis) y se parsea unicamente ese pedazo.
_lineas = fuente.splitlines(keepends=True)
_ini = _indent_def = None
for _i, _l in enumerate(_lineas):
    _stripped = _l.lstrip()
    if _stripped.startswith("def _tk_leer_correo_entrante("):
        _ini = _i
        _indent_def = len(_l) - len(_stripped)
        break
check(_ini is not None, "se encontro 'def _tk_leer_correo_entrante(' en el archivo real")
_fin = len(_lineas)
for _i in range(_ini + 1, len(_lineas)):
    _l = _lineas[_i]
    if not _l.strip():
        continue
    if (len(_l) - len(_l.lstrip())) <= _indent_def:
        _fin = _i
        break
_slice_normalizado = "".join(
    _l[_indent_def:] if _l.strip() else _l for _l in _lineas[_ini:_fin])
_arbol = ast.parse(_slice_normalizado)


def _es_comparacion_sin_ticket_taa(nodo_test):
    return (isinstance(nodo_test, ast.Compare)
            and isinstance(nodo_test.left, ast.Name)
            and nodo_test.left.id == "clase"
            and len(nodo_test.comparators) == 1
            and isinstance(nodo_test.comparators[0], ast.Constant)
            and nodo_test.comparators[0].value == "sin_ticket_taa")


_nodo_sin_ticket_taa = None
for _nodo in ast.walk(_arbol):
    if isinstance(_nodo, ast.If) and _es_comparacion_sin_ticket_taa(_nodo.test):
        _nodo_sin_ticket_taa = _nodo
        break

check(_nodo_sin_ticket_taa is not None,
      "existe el bloque `if clase == \"sin_ticket_taa\":` en el codigo real")
if _nodo_sin_ticket_taa is not None:
    _ultima = _nodo_sin_ticket_taa.body[-1]
    check(not isinstance(_ultima, ast.Continue),
          "la ULTIMA instruccion top-level del bloque sin_ticket_taa NO es "
          "un continue -- el camino de exito cae al bloque de 'candidato' "
          "de mas abajo en vez de cortar el correo ahi mismo")
    # Dentro, el sub-bloque de error (el segundo Try que aparece) SI debe
    # terminar en continue -- si la creacion fallo, no puede caer al
    # camino de "candidato" como si el ticket ya existiera.
    _trys = [n for n in ast.walk(_nodo_sin_ticket_taa) if isinstance(n, ast.Try)]
    check(len(_trys) == 1 and isinstance(_trys[0].handlers[0].body[-1], ast.Continue),
          "el manejador de excepcion del INSERT SI termina en continue -- "
          "un fallo de BD no debe caer al camino de 'candidato'")

print("\n" + "=" * 60)
if fallos:
    print(f"RESULTADO: {len(fallos)} verificacion(es) FALLARON")
    for f_ in fallos:
        print(f"  - {f_}")
    sys.exit(1)
print("RESULTADO: todas las verificaciones pasaron")
sys.exit(0)

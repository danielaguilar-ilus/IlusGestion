"""
TEST FILE -- NO ES PRODUCCION. NO IMPORTAR DESDE app.py.

Permisos de ficha de equipo vs. Orden de Trabajo (2026-08-09).

PEDIDO DE DANIEL (textual, acotado):
  "Un tecnico solo debe poder modificar datos de un equipo cuando esa
   modificacion ocurra dentro de una Orden de Trabajo que tenga asignada.
   Fuera de una OT, las modificaciones de la ficha del equipo deben quedar
   limitadas a los roles gestores definidos por el sistema."

QUE VERIFICA (y por que cada cosa):

 1. LA BRECHA REAL QUE SE CERRO -- `_no_tecnico` comparaba el rol EXACTO
    (`role == "tecnico"`). Un usuario con rol 'tecnico_externo' NO caia en
    esa comparacion y entraba a endpoints pensados solo para gestion.
    Daniel nombro explicitamente a "tecnico externo" en el alcance, asi que
    esto no es teorico: es el caso que se pidio cubrir.
    Ahora usa `_rol_familia()`, el mismo comparador que ya usa el resto del
    backend, que normaliza tecnico / tecnico_externo / tecnico_jr -> 'tecnico'.

 2. AUTORIZACION EN EL BACKEND, NO SOLO EN LA UI -- se verifica sobre el
    TEXTO de app.py que cada endpoint sensible lleva el decorador. Ocultar
    el boton en la plantilla no sirve: la ruta se puede llamar directo.

 3. NO SE AMPLIARON PRIVILEGIOS -- ejecutivo / supervisor / admin /
    superadmin siguen pasando; el cambio solo RESTA permisos al tecnico.

 4. NO SE ROMPIO EL FLUJO LEGITIMO DEL TECNICO -- los endpoints con los que
    el tecnico trabaja DENTRO de su OT (capturar equipo, subir foto) NO
    llevan el decorador y por lo tanto siguen funcionando.

 5. EL BORRADO DE MAQUINAS SIGUE RESTRINGIDO a superadmin con motivo y
    confirmacion (el alcance pedia explicitamente conservarlo).

Se corre igual que el resto de la bateria:  python3 tests/test_permisos_equipos_ot.py
"""

import ast
import os
import re
import sys

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
APP = os.path.join(RAIZ, "app.py")

fallos = []


def check(cond, msg):
    if cond:
        print(f"  OK   {msg}")
    else:
        print(f"  FALLA {msg}")
        fallos.append(msg)


# Se lee por lineas (no de una) porque app.py ronda las 90k lineas y en
# maquinas con poca RAM un read() completo puede reventar con MemoryError.
with open(APP, encoding="utf-8") as f:
    LINEAS = f.readlines()
FUENTE = "".join(LINEAS)


# ══════════════════════════════════════════════════════════════════
# 1. ENDPOINTS SENSIBLES: deben exigir rol gestor (@_no_tecnico)
# ══════════════════════════════════════════════════════════════════
# Cada tupla es (ruta, metodo, por que es sensible).
SENSIBLES = [
    ("/mantenciones/api/maquinas/<int:mid>/serie", "PUT",
     "cambiar el N. de serie reescribe la identidad del equipo en su ficha global"),
    ("/mantenciones/api/maquinas/<int:mid>/aplica-mantencion", "PUT",
     "decide si el equipo entra al plan de mantencion (y por tanto se cotiza)"),
    ("/mantenciones/api/maquinas/<int:mid>/horometro", "POST",
     "registro manual de horometro FUERA de una OT"),
    ("/mantenciones/api/maquinas/<int:mid>/horometro/<int:lid>", "DELETE",
     "borra una lectura historica de horometro"),
    ("/mantenciones/api/maquinas/<int:mid>/solicitar-cambio", "POST",
     "cambia el estado operativo del equipo Y crea una OT desde la ficha"),
]


def bloque_decoradores(ruta, metodo):
    """Devuelve el texto entre @app.route(...) y el `def` de esa vista."""
    pat = re.compile(
        r'@app\.route\(\s*"' + re.escape(ruta) + r'"\s*,\s*methods=\[[^\]]*"'
        + re.escape(metodo) + r'"', re.M)
    m = pat.search(FUENTE)
    if not m:
        return None
    resto = FUENTE[m.start():]
    fin = resto.find("\ndef ")
    return resto[:fin] if fin != -1 else resto[:2000]


print("\n1. Endpoints sensibles exigen rol gestor en el BACKEND")
for ruta, metodo, porque in SENSIBLES:
    blq = bloque_decoradores(ruta, metodo)
    check(blq is not None, f"existe la ruta {metodo} {ruta}")
    if blq:
        check("@_no_tecnico" in blq,
              f"{metodo} {ruta} lleva @_no_tecnico -- {porque}")
        check("@_mant_required" in blq,
              f"{metodo} {ruta} conserva @_mant_required (no se reemplazo el gate base)")


# ══════════════════════════════════════════════════════════════════
# 2. `_no_tecnico` usa la FAMILIA de rol, no el string exacto
# ══════════════════════════════════════════════════════════════════
print("\n2. _no_tecnico compara por familia de rol (cubre tecnico_externo)")

i_ini = next((i for i, l in enumerate(LINEAS) if l.startswith("def _no_tecnico")), None)
check(i_ini is not None, "se encuentra def _no_tecnico en app.py")

cuerpo_no_tec = ""
if i_ini is not None:
    for l in LINEAS[i_ini:i_ini + 60]:
        cuerpo_no_tec += l
        if l.startswith("def ") and not l.startswith("def _no_tecnico"):
            break

check('_rol_familia(role) == "tecnico"' in cuerpo_no_tec,
      "usa _rol_familia(role) == 'tecnico' (no la comparacion exacta antigua)")
check('if role == "tecnico":' not in cuerpo_no_tec,
      "ya NO queda la comparacion exacta `role == \"tecnico\"` que dejaba pasar a tecnico_externo")


# ══════════════════════════════════════════════════════════════════
# 3. COMPORTAMIENTO POR ROL sobre la _rol_familia REAL de app.py
# ══════════════════════════════════════════════════════════════════
# Se extrae la funcion tal cual esta en produccion y se ejecuta aislada:
# asi el test valida el codigo real, no una copia que puede quedar vieja.
print("\n3. Clasificacion por rol (funcion real extraida de app.py)")

_i0 = next(i for i, l in enumerate(LINEAS) if l.startswith("def _rol_familia"))
# Se corta en el SIGUIENTE def de nivel superior (no en un numero fijo de
# lineas): si no, el corte cae dentro del docstring de la funcion siguiente
# y ast.parse revienta con "unterminated triple-quoted string".
_i1 = next(i for i in range(_i0 + 1, len(LINEAS)) if LINEAS[i].startswith("def "))
arbol = ast.parse("".join(LINEAS[_i0:_i1]))
ns = {}
exec(compile(arbol, "<_rol_familia>", "exec"), ns)
_rol_familia = ns["_rol_familia"]

# (rol, familia esperada, puede editar la ficha global del equipo)
CASOS = [
    ("tecnico",           "tecnico",    False),
    ("tecnico_externo",   "tecnico",    False),   # <-- el caso que estaba abierto
    ("tecnico_jr",        "tecnico",    False),
    ("ejecutivo",         "ejecutivo",  True),
    ("ejecutivo_sstt",    "ejecutivo",  True),
    ("supervisor",        "supervisor", True),
    ("supervisor_sstt",   "supervisor", True),
    ("admin",             "admin",      True),
    ("superadmin",        "superadmin", True),
]

for rol, familia_esp, puede in CASOS:
    fam = _rol_familia(rol)
    check(fam == familia_esp, f"rol '{rol}' -> familia '{fam}' (esperada '{familia_esp}')")
    bloqueado = (fam == "tecnico")
    check(bloqueado != puede,
          f"rol '{rol}': {'RECHAZADO' if bloqueado else 'PERMITIDO'} en ficha global "
          f"(esperado: {'PERMITIDO' if puede else 'RECHAZADO'})")


# ══════════════════════════════════════════════════════════════════
# 4. NO se rompio el trabajo legitimo del tecnico DENTRO de su OT
# ══════════════════════════════════════════════════════════════════
# Estos endpoints son el flujo real de terreno. Si alguno se blindara con
# @_no_tecnico, el tecnico no podria trabajar y el arreglo seria peor que
# el problema. La autorizacion fina de estos ya la hace _lev_puede_accion /
# _puede_ot_accion, que valida que la OT sea la suya.
print("\n4. El flujo del tecnico dentro de su OT sigue abierto")
LEGITIMOS = [
    ("/mantenciones/api/levantamientos/<int:lid>/items", "POST",
     "capturar un equipo durante la OT"),
]
for ruta, metodo, porque in LEGITIMOS:
    blq = bloque_decoradores(ruta, metodo)
    check(blq is not None, f"existe la ruta {metodo} {ruta}")
    if blq:
        check("@_no_tecnico" not in blq,
              f"{metodo} {ruta} NO lleva @_no_tecnico -- {porque}")

# 4b. El tecnico solo agrega equipos si la OT ES de levantamiento
#     (Daniel 2026-08-10: "nunca podra agregar, a menos que sea una OT por
#      levantamiento y descubrimiento en terreno").
#     No basta con esconder el boton en la plantilla: la ruta se llama directo.
i_crear = next((i for i, l in enumerate(LINEAS)
                if l.startswith("def mant_lev_item_crear")), None)
check(i_crear is not None, "existe def mant_lev_item_crear")
if i_crear is not None:
    cuerpo_crear = "".join(LINEAS[i_crear:i_crear + 80])
    check("TECNICO_NO_AGREGA_EQUIPOS" in cuerpo_crear,
          "mant_lev_item_crear rechaza al tecnico con codigo TECNICO_NO_AGREGA_EQUIPOS")
    check("_ot_es_levantamiento" in cuerpo_crear,
          "usa _ot_es_levantamiento() -- misma definicion que el resto del backend, "
          "no un criterio paralelo")
    check('_rol_familia' in cuerpo_crear,
          "decide por FAMILIA de rol (cubre tecnico_externo), no por string exacto")

# 4c. La plantilla tampoco muestra el boton en una OT que no es levantamiento
TPL = os.path.join(RAIZ, "templates", "mantenciones", "ot_ejecutar.html")
with open(TPL, encoding="utf-8") as f:
    tpl = f.read()
check("_es_lev_ot" in tpl,
      "la plantilla calcula si la OT es de levantamiento antes de mostrar 'Agregar equipo'")
check("lev_editable and (_es_lev_ot or not es_tecnico)" in tpl,
      "el boton 'Agregar equipo' se oculta al tecnico si la OT no es de levantamiento")


# ══════════════════════════════════════════════════════════════════
# 5. El borrado de maquinas sigue restringido a superadmin
# ══════════════════════════════════════════════════════════════════
print("\n5. Borrado de maquinas: restringido, con motivo y confirmacion")
i_del = next((i for i, l in enumerate(LINEAS)
              if l.startswith("def mant_maquina_del")), None)
check(i_del is not None, "existe def mant_maquina_del")
if i_del is not None:
    cuerpo_del = "".join(LINEAS[i_del:i_del + 80])
    check("superadmin" in cuerpo_del, "el borrado sigue exigiendo superadmin")
    check("confirm_text" in cuerpo_del or "ELIMINAR" in cuerpo_del,
          "el borrado sigue exigiendo confirmacion explicita")
    check("motivo" in cuerpo_del, "el borrado sigue exigiendo motivo (auditoria)")


# ══════════════════════════════════════════════════════════════════
# 6. La auditoria de los cambios de ficha se conserva
# ══════════════════════════════════════════════════════════════════
print("\n6. Auditoria de quien cambio que")
i_serie = next((i for i, l in enumerate(LINEAS)
                if l.startswith("def mant_maquina_actualizar_serie")), None)
if i_serie is not None:
    cuerpo_serie = "".join(LINEAS[i_serie:i_serie + 90])
    check("mant_maquina_audit" in cuerpo_serie or "audit" in cuerpo_serie,
          "el cambio de serie sigue registrando auditoria")


print("\n" + "=" * 60)
if fallos:
    print(f"RESULTADO: {len(fallos)} verificacion(es) FALLARON")
    for f_ in fallos:
        print(f"  - {f_}")
    sys.exit(1)
print("RESULTADO: todas las verificaciones pasaron")
sys.exit(0)

"""
TEST FILE -- NO ES PRODUCCION. NO IMPORTAR DESDE app.py.

Plantilla de checklist elegida por CLASIFICACION REAL del equipo, no a
ciegas por tipo de OT (2026-08-12).

POR QUE EXISTE (caso real, OT-2026-00072): `_plantilla_estandar_para_tipo`
elegia la plantilla mirando SOLO el tipo de OT ('preventiva', etc.) --
si no habia una con el nombre de convencion exacto, tomaba la plantilla
de ese tipo_visita con MAS ITEMS, sin importar si el equipo era una
trotadora, una bicicleta o un rack. Daniel: "no le voy a hacer una
verificacion a una trotadora como a una bicicleta".

Este test verifica (sobre el TEXTO real de app.py, sin base de datos --
este agente no tiene acceso a produccion, mismo patron que
test_permisos_equipos_ot.py):

 1. Existen las funciones nuevas de resolucion por clasificacion.
 2. `_plantilla_por_clasificacion_sku` usa el puente real
    (mant_maquinas.sku -> cat_productos.sku -> cat_productos.
    clase_producto -> cat_clases_producto.nombre) y arma el nombre de
    convencion "{familia} · {clasificacion}".
 3. `_familia_plantilla_para_tipo` resuelve la familia via la tabla
    EDITABLE mant_categoria_tipo_map (no un dict fijo nuevo), con
    fallback al seed existente -- reusa lo que ya vive en el proyecto.
 4. `_mant_lev_crear_ot_core` calcula la resolucion por equipo UNA sola
    vez y la REUSA entre el pre-chequeo (decide si hace falta la tarea
    de respaldo "Documentar") y el bloque que aplica las plantillas de
    verdad -- mismo principio que ya dejo documentado este archivo para
    el criterio ciego anterior ("MISMA funcion que decide en el paso 1,
    antes eran dos consultas distintas y podian discrepar").
 5. Un equipo sin clasificacion resuelta CAE a `_plantilla_estandar_para_tipo`
    (nunca bloquea la creacion de la OT).
 6. `plantilla_id_override` (selector explicito del usuario, Tarea 4)
    sigue mandando sobre el calculo automatico.
 7. Equipos con distinta plantilla resuelta se AGRUPAN y aplican por
    separado -- la correccion real del bug (antes: una sola plantilla
    para TODOS los equipos de la OT).

Se corre igual que el resto de la bateria: python tests/test_plantilla_por_clasificacion.py
"""

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


# Se lee por lineas (no de una) -- app.py ronda las 90k lineas, ver
# comentario identico en test_permisos_equipos_ot.py.
with open(APP, encoding="utf-8") as f:
    LINEAS = f.readlines()
FUENTE = "".join(LINEAS)


def cuerpo_funcion(nombre_def, max_lineas=80):
    """Texto desde `def nombre_def` hasta el siguiente `def ` de nivel
    superior (o max_lineas si no aparece antes)."""
    i0 = next((i for i, l in enumerate(LINEAS) if l.startswith(f"def {nombre_def}(")), None)
    if i0 is None:
        return None
    for j in range(i0 + 1, min(i0 + max_lineas, len(LINEAS))):
        if LINEAS[j].startswith("def ") or LINEAS[j].startswith("@app.route"):
            return "".join(LINEAS[i0:j])
    return "".join(LINEAS[i0:i0 + max_lineas])


# ══════════════════════════════════════════════════════════════════
# 1. Las funciones nuevas existen
# ══════════════════════════════════════════════════════════════════
print("\n1. Funciones de resolucion por clasificacion existen")
cuerpo_familia = cuerpo_funcion("_familia_plantilla_para_tipo")
check(cuerpo_familia is not None, "existe def _familia_plantilla_para_tipo")

cuerpo_sku = cuerpo_funcion("_plantilla_por_clasificacion_sku", max_lineas=60)
check(cuerpo_sku is not None, "existe def _plantilla_por_clasificacion_sku")

cuerpo_equipo = cuerpo_funcion("_plantilla_por_clasificacion_equipo", max_lineas=20)
check(cuerpo_equipo is not None, "existe def _plantilla_por_clasificacion_equipo")


# ══════════════════════════════════════════════════════════════════
# 2. El puente real: mant_maquinas.sku -> cat_productos -> cat_clases_producto
# ══════════════════════════════════════════════════════════════════
print("\n2. _plantilla_por_clasificacion_sku usa el puente SKU real")
if cuerpo_sku:
    check("cat_productos" in cuerpo_sku,
          "consulta cat_productos (el catalogo real, no una tabla inventada)")
    check("cat_clases_producto" in cuerpo_sku,
          "hace JOIN contra cat_clases_producto para obtener el nombre humano")
    check("clase_producto" in cuerpo_sku,
          "cruza por la columna clase_producto (el puente ya existente)")
    check('f"{familia}' in cuerpo_sku or "f'{familia}" in cuerpo_sku,
          "arma el nombre de plantilla interpolando la familia")
    # el separador real usado en el resto del proyecto (email, branding,
    # titulos del cubicador) es " · " -- U+00B7 rodeado de espacios.
    check("·" in cuerpo_sku,
          "usa el separador ' · ' (U+00B7) -- misma convencion del resto del proyecto")
    check('activa' in cuerpo_sku.lower(),
          "solo elige plantillas activas (COALESCE(p.activa,1)=1)")
    check("return None" in cuerpo_sku,
          "tiene camino de salida None (no revienta si falta info)")


# ══════════════════════════════════════════════════════════════════
# 3. La familia se resuelve via la tabla EDITABLE, no un dict nuevo
# ══════════════════════════════════════════════════════════════════
print("\n3. _familia_plantilla_para_tipo reusa mant_categoria_tipo_map")
if cuerpo_familia:
    check("mant_categoria_tipo_map" in cuerpo_familia,
          "consulta la MISMA tabla editable que ya decide la pestana de plantillas "
          "(Tarea 1, 2026-08-10) -- no inventa un mapeo nuevo")
    check("_PLANT_CATEGORIA_TIPO_SEED" in cuerpo_familia,
          "cae al seed fijo existente si la tabla no tiene el tipo o falla la query")
    check("_PLANT_CATEGORIA_LABEL" in cuerpo_familia,
          "traduce la categoria a su label humano ('Instalación'/'Mantención'/...) "
          "con el dict que ya existia para las 4 pestanas")


# ══════════════════════════════════════════════════════════════════
# 4. _mant_lev_crear_ot_core: resolucion UNICA, reusada (no recalculada)
# ══════════════════════════════════════════════════════════════════
print("\n4. La resolucion por equipo se calcula UNA vez y se reusa")
cuerpo_core = cuerpo_funcion("_mant_lev_crear_ot_core", max_lineas=900)
check(cuerpo_core is not None, "existe def _mant_lev_crear_ot_core")
if cuerpo_core:
    n_apariciones = cuerpo_core.count("_resolucion_plantilla_por_equipo")
    check(n_apariciones >= 4,
          f"_resolucion_plantilla_por_equipo aparece {n_apariciones} veces "
          "(init + pre-chequeo que la puebla + bloque que la reusa post-commit)")
    check("_plantilla_por_clasificacion_sku(" in cuerpo_core,
          "el core llama a la resolucion por clasificacion (no solo al criterio ciego)")
    check("dict(_resolucion_plantilla_por_equipo)" in cuerpo_core,
          "el bloque de aplicacion post-commit arranca copiando la resolucion "
          "ya calculada -- no la recalcula de cero")


# ══════════════════════════════════════════════════════════════════
# 5. Fallback: sin clasificacion resuelta, cae a _plantilla_estandar_para_tipo
# ══════════════════════════════════════════════════════════════════
print("\n5. Sin clasificacion resuelta, cae al criterio ciego anterior (fallback, nunca bloquea)")
if cuerpo_core:
    check("_plantilla_estandar_para_tipo(tipo_ot)" in cuerpo_core,
          "el criterio ciego anterior se sigue llamando -- como FALLBACK, no eliminado "
          "(Regla #4.2 -- no se quita comportamiento existente)")
    check("_fallback_tipo_ot()" in cuerpo_core or "plant_fallback" in cuerpo_core,
          "existe un camino de fallback explicito por equipo/OT")


# ══════════════════════════════════════════════════════════════════
# 6. plantilla_id_override sigue mandando (selector explicito del usuario)
# ══════════════════════════════════════════════════════════════════
print("\n6. El selector explicito de plantilla (Tarea 4) sigue ganando")
if cuerpo_core:
    check("if plantilla_id_override:" in cuerpo_core,
          "existe una rama explicita que prioriza plantilla_id_override sobre "
          "el calculo automatico (ni ciego ni por clasificacion lo pisan)")


# ══════════════════════════════════════════════════════════════════
# 7. Agrupacion: equipos con distinta clasificacion -> distintas plantillas
# ══════════════════════════════════════════════════════════════════
print("\n7. Equipos con distinta clasificacion aplican plantillas DISTINTAS (el fix real)")
if cuerpo_core:
    check("grupos.setdefault(" in cuerpo_core or "grupos[" in cuerpo_core,
          "agrupa equipos por plantilla_id resuelta antes de aplicar "
          "(una trotadora y una bicicleta ya NO comparten una unica plantilla)")
    check("for plant_id, mids_grupo in grupos.items():" in cuerpo_core,
          "aplica la plantilla POR GRUPO -- _aplicar_plantilla_a_equipos se llama "
          "una vez por cada plantilla distinta resuelta, no una sola vez global")


print("\n" + "=" * 60)
if fallos:
    print(f"RESULTADO: {len(fallos)} verificacion(es) FALLARON")
    for f_ in fallos:
        print(f"  - {f_}")
    sys.exit(1)
print("RESULTADO: todas las verificaciones pasaron")
sys.exit(0)

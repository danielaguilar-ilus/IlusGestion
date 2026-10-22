"""REGLA #40 — llave estable clasificación ↔ plantilla (`mant_tarea_plantillas.clase_id`).

Las funciones de la cascada viven en app.py (que no se importa: es enorme y toca la base). Se extraen por `ast` y se
ejecutan contra una base de mentira, así se prueba el comportamiento real y no solo el texto.
"""
import ast
import os
import re

import pytest

import plantillas_serie_gps as psg

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _leer(*ruta):
    with open(os.path.join(RAIZ, *ruta), encoding="utf-8") as f:
        return f.read()


_APP = _leer("app.py")
_FUNCS = ("_plcls_nombre", "_plcls_ligar_si_coincide", "_plcls_renombrar", "_plcls_activar", "_plcls_crear_para_clase")


class BD:
    """Base de mentira: plantillas, clases y un registro de lo que se hizo."""

    def __init__(self, plantillas, clases):
        self.pl = [dict(p) for p in plantillas]
        self.clases = [dict(c) for c in clases]
        self.logs = []
        self.creadas = []

    # --- API mínima que usan las funciones ---
    def fetchone(self, sql, params=()):
        r = self.fetchall(sql, params)
        return r[0] if r else None

    def fetchall(self, sql, params=()):
        s = " ".join(sql.split())
        if s.startswith("SELECT id FROM cat_clases_producto WHERE nombre=%s AND activo=1"):
            return [{"id": c["id"]} for c in self.clases if c["nombre"] == params[0] and c.get("activo", 1)]
        if s.startswith("SELECT id FROM mant_tarea_plantillas WHERE clase_id=%s AND categoria_admin=%s AND id<>%s"):
            return [{"id": p["id"]} for p in self.pl if p.get("clase_id") == params[0]
                    and p["categoria_admin"] == params[1] and p["id"] != params[2]]
        if s.startswith("SELECT id, nombre, categoria_admin FROM mant_tarea_plantillas WHERE clase_id=%s"):
            return [dict(p) for p in self.pl if p.get("clase_id") == params[0]]
        if s.startswith("SELECT id FROM mant_tarea_plantillas WHERE nombre=%s AND categoria_admin <=> %s AND id<>%s"):
            return [{"id": p["id"]} for p in self.pl if p["nombre"] == params[0]
                    and p["categoria_admin"] == params[1] and p["id"] != params[2]]
        if s.startswith("SELECT id, nombre FROM mant_tarea_plantillas WHERE clase_id=%s AND COALESCE(activa,1)<>%s"):
            return [dict(p) for p in self.pl if p.get("clase_id") == params[0] and int(p.get("activa", 1)) != params[1]]
        if s.startswith("SELECT id FROM mant_tarea_plantillas WHERE (clase_id=%s OR nombre=%s) AND categoria_admin=%s"):
            return [{"id": p["id"]} for p in self.pl if (p.get("clase_id") == params[0] or p["nombre"] == params[1])
                    and p["categoria_admin"] == params[2]]
        raise AssertionError("SQL no previsto: " + s)

    def execute(self, sql, params=()):
        s = " ".join(sql.split())
        if s.startswith("UPDATE mant_tarea_plantillas SET nombre=%s WHERE id=%s"):
            for p in self.pl:
                if p["id"] == params[1]:
                    p["nombre"] = params[0]
            return
        if s.startswith("UPDATE mant_tarea_plantillas SET activa=%s WHERE id=%s"):
            for p in self.pl:
                if p["id"] == params[1]:
                    p["activa"] = params[0]
            return
        if s.startswith("UPDATE mant_tarea_plantillas SET clase_id=%s WHERE id=%s AND clase_id IS NULL"):
            for p in self.pl:
                if p["id"] == params[1] and not p.get("clase_id"):
                    p["clase_id"] = params[0]
            return
        raise AssertionError("SQL no previsto: " + s)


@pytest.fixture(scope="module")
def modulo_app():
    arbol = ast.parse(_APP)
    nodos = [n for n in arbol.body if isinstance(n, ast.FunctionDef) and n.name in _FUNCS]
    asignaciones = [n for n in arbol.body if isinstance(n, ast.Assign)
                    and any(isinstance(t, ast.Name) and t.id in ("_PLCLS_SUFIJO_TI", "_PLCLS_CATEGORIAS") for t in n.targets)]
    assert {n.name for n in nodos} == set(_FUNCS)
    return asignaciones + nodos


def _ns(modulo_app, bd, ok=True):
    ns = {
        "mysql_fetchone": bd.fetchone, "mysql_fetchall": bd.fetchall, "mysql_execute": bd.execute,
        "_mant_log": lambda ent, i, acc, det="": bd.logs.append((ent, i, acc, det)),
        "_plcls_ok": lambda: ok,
        "_psg_crear_clase": lambda clase, cat, user, clase_id=None: (bd.creadas.append((clase, cat, clase_id)) or ("creada", len(bd.creadas))),
    }
    exec(compile(ast.Module(body=modulo_app, type_ignores=[]), "app_extraido", "exec"), ns)
    return ns


def _pl(i, nombre, cat, clase_id=None, activa=1):
    return {"id": i, "nombre": nombre, "categoria_admin": cat, "clase_id": clase_id, "activa": activa}


# ── renombrar: nunca crea, renombra en TODAS las categorías ─────────────────────────────────────────────────────
def test_renombrar_clasificacion_renombra_todas_sus_plantillas(modulo_app):
    bd = BD([_pl(1, "Dual Cable Lite", "instalacion", 7), _pl(2, "Dual Cable Lite", "mantencion", 7),
             _pl(3, "Dual Cable Lite", "visitas", 7), _pl(4, "Dual Cable Lite (trabajo interno)", "trabajo_interno", 7),
             _pl(5, "Otra", "instalacion", 8)], [{"id": 7, "nombre": "Dual Cable"}])
    r = _ns(modulo_app, bd)["_plcls_renombrar"](7, "Dual Cable", "daniel")
    assert r == {"renombradas": 4, "conflictos": []}
    assert [p["nombre"] for p in bd.pl] == ["Dual Cable", "Dual Cable", "Dual Cable", "Dual Cable (trabajo interno)", "Otra"]
    assert len(bd.pl) == 5 and not bd.creadas          # no se creó ninguna plantilla
    assert len(bd.logs) == 4 and all(l[2] == "renombrada_por_clasificacion" for l in bd.logs)


def test_renombrar_no_pisa_otra_plantilla_con_ese_nombre(modulo_app):
    bd = BD([_pl(1, "Rack", "instalacion", 3), _pl(2, "Rack Pro", "instalacion", None)], [{"id": 3, "nombre": "Rack Pro"}])
    r = _ns(modulo_app, bd)["_plcls_renombrar"](3, "Rack Pro", "daniel")
    assert r["renombradas"] == 0 and r["conflictos"][0]["plantilla_id"] == 1
    assert bd.pl[0]["nombre"] == "Rack"                  # intacta


def test_renombrar_sin_columna_no_hace_nada(modulo_app):
    bd = BD([_pl(1, "A", "instalacion", 1)], [])
    assert _ns(modulo_app, bd, ok=False)["_plcls_renombrar"](1, "B", "d") == {"renombradas": 0, "conflictos": []}
    assert bd.pl[0]["nombre"] == "A"


def test_renombrar_mismo_nombre_no_escribe(modulo_app):
    bd = BD([_pl(1, "A", "instalacion", 1)], [])
    assert _ns(modulo_app, bd)["_plcls_renombrar"](1, "A", "d")["renombradas"] == 0 and not bd.logs


# ── desactivar / reactivar ──────────────────────────────────────────────────────────────────────────────────────
def test_desactivar_y_reactivar_siguen_a_la_clasificacion(modulo_app):
    bd = BD([_pl(1, "A", "instalacion", 1), _pl(2, "A", "mantencion", 1), _pl(3, "B", "instalacion", 2)], [])
    f = _ns(modulo_app, bd)["_plcls_activar"]
    assert f(1, False, "d") == {"cambiadas": 2}
    assert [p["activa"] for p in bd.pl] == [0, 0, 1] and len(bd.pl) == 3      # soft: nada se borra
    assert f(1, False, "d") == {"cambiadas": 0}                                # idempotente
    assert f(1, True, "d") == {"cambiadas": 2} and [p["activa"] for p in bd.pl] == [1, 1, 1]


# ── liga al crear ────────────────────────────────────────────────────────────────────────────────────────────────
def test_ligar_si_coincide(modulo_app):
    bd = BD([_pl(1, "Bicicleta", "instalacion"), _pl(2, "Bicicleta (trabajo interno)", "trabajo_interno"),
             _pl(3, "Bicicleta", "trabajo_interno"), _pl(4, "Nada", "instalacion")],
            [{"id": 9, "nombre": "Bicicleta", "activo": 1}])
    f = _ns(modulo_app, bd)["_plcls_ligar_si_coincide"]
    assert f(1, "Bicicleta", "instalacion") == 9 and bd.pl[0]["clase_id"] == 9
    assert f(2, "Bicicleta (trabajo interno)", "trabajo_interno") == 9
    assert f(3, "Bicicleta", "trabajo_interno") is None        # sin el sufijo no es la de filtro
    assert f(4, "Nada", "instalacion") is None


def test_no_liga_si_ya_hay_otra_ligada_en_esa_categoria(modulo_app):
    bd = BD([_pl(1, "X", "instalacion", 9), _pl(2, "X", "instalacion")], [{"id": 9, "nombre": "X", "activo": 1}])
    assert _ns(modulo_app, bd)["_plcls_ligar_si_coincide"](2, "X", "instalacion") is None


# ── crear una clasificación crea sus plantillas (4 categorías), solo las que faltan ─────────────────────────────
def test_crear_clasificacion_crea_las_cuatro_plantillas(modulo_app):
    bd = BD([], [])
    creadas = _ns(modulo_app, bd)["_plcls_crear_para_clase"](5, "Banco Scott", "daniel")
    assert [c["categoria"] for c in creadas] == ["instalacion", "mantencion", "visitas", "trabajo_interno"]
    assert [x[2] for x in bd.creadas] == [5, 5, 5, 5]                         # todas ligadas por la llave


def test_crear_clasificacion_no_duplica_lo_que_ya_existe(modulo_app):
    bd = BD([_pl(1, "Banco Scott", "instalacion", 5), _pl(2, "Banco Scott (trabajo interno)", "trabajo_interno")], [])
    creadas = _ns(modulo_app, bd)["_plcls_crear_para_clase"](5, "Banco Scott", "daniel")
    assert [c["categoria"] for c in creadas] == ["mantencion", "visitas"]


# ── módulo puro: faltantes por llave y por nombre ────────────────────────────────────────────────────────────────
def test_faltantes_cuenta_la_plantilla_ligada_aunque_tenga_otro_nombre():
    clases = [{"id": 7, "nombre": "Dual Cable"}]
    plantillas = [{"nombre": "Dual Cable Lite", "categoria_admin": "instalacion", "clase_id": 7}]
    pares = {(f["clase"], f["categoria"]) for f in psg.faltantes_por_clase(clases, plantillas)}
    assert ("Dual Cable", "instalacion") not in pares and ("Dual Cable", "mantencion") in pares


def test_faltantes_trabajo_interno_usa_el_sufijo():
    clases = [{"id": 7, "nombre": "Dual Cable"}]
    con_sufijo = [{"nombre": "Dual Cable (trabajo interno)", "categoria_admin": "trabajo_interno"}]
    sin_sufijo = [{"nombre": "Dual Cable", "categoria_admin": "trabajo_interno"}]
    assert ("Dual Cable", "trabajo_interno") not in {(f["clase"], f["categoria"]) for f in psg.faltantes_por_clase(clases, con_sufijo)}
    assert ("Dual Cable", "trabajo_interno") in {(f["clase"], f["categoria"]) for f in psg.faltantes_por_clase(clases, sin_sufijo)}


def test_nombre_para_categoria():
    assert psg.nombre_para_categoria("Rack", "instalacion") == "Rack"
    assert psg.nombre_para_categoria("Rack", "trabajo_interno") == "Rack (trabajo interno)"


# ── cableado ────────────────────────────────────────────────────────────────────────────────────────────────────
def test_cableado_en_app():
    for marca in ("def _ensure_plantilla_clase_id", "_n_clase_bf = _ensure_plantilla_clase_id()",
                  '@app.route("/mantenciones/api/plantillas/adoptar"', "NOMBRE_DE_CLASIFICACION",
                  "p.clase_id = c.id AND COALESCE(p.categoria_admin,'') <> 'trabajo_interno'",
                  "((p.clase_id = %s) OR (p.clase_id IS NULL AND p.nombre = %s))",
                  "categoria != \"trabajo_interno\"", "_plcls_ligar_si_coincide(pid, nombre, cat_admin)",
                  "_de_clasificacion = bool(plant.get(\"clase_id\"))"):
        assert marca in _APP, marca


def test_desligar_es_el_inverso_de_adoptar_y_no_borra_nada():
    i = _APP.index("def mant_plantillas_desligar")
    f = _APP[i:i + 3200]
    assert '@app.route("/mantenciones/api/plantillas/desligar"' in _APP
    assert "clase_id=NULL" in f and "NOMBRE_EN_USO" in f and "desligada_de_clasificacion" in f
    assert "DELETE" not in f                                     # solo suelta la liga, jamás borra
    assert f.index("desligada_de_clasificacion") < f.index("UPDATE mant_tarea_plantillas")   # bitácora antes de escribir


def test_clasificar_producto_desde_la_plantilla_usa_la_llave():
    i = _APP.index("def ot2_producto_clasificar") if "def ot2_producto_clasificar" in _APP else _APP.index("plantilla_no_es_clase") - 3000
    f = _APP[i:i + 4500]
    assert 'plant.get("clase_id")' in f and "WHERE id=%s AND activo=1" in f


def test_la_matriz_pone_p_estrella_primero():
    """Si `p.*` fuera después del alias, la columna clase_id de la plantilla pisaría a la de la clase (c.id)."""
    i = _APP.index("SELECT p.*,\n                   c.id     AS clase_id")
    assert i > 0


def test_el_catalogo_propaga_renombrar_activar_y_crear():
    c = _leer("catalogo_module.py")
    for marca in ('ctx.get("_plcls_renombrar")', 'ctx.get("_plcls_activar")', 'ctx.get("_plcls_crear_para_clase")'):
        assert marca in c, marca


def test_la_llave_es_barata_y_una_clausula_por_sentencia():
    i = _APP.index("def _ensure_plantilla_clase_id")
    cuerpo = _APP[i:i + 2500]
    assert cuerpo.count("ALTER TABLE mant_tarea_plantillas ADD ") == 2          # columna e índice, cada uno por separado
    assert "COLUMN_NAME='clase_id'" in _APP and "ADD COLUMN clase_id" in cuerpo


def test_el_editor_bloquea_el_nombre_de_una_plantilla_ligada():
    h = _leer("templates", "mantenciones", "plantillas.html")
    for marca in ("plBloquearNombreClase(", "ePNombreClase", "se cambia en Catálogo", "inp.readOnly"):
        assert marca in h, marca

# -*- coding: utf-8 -*-
"""2026-10-25 · Pestaña «Desinstalación» en Plantillas y «Mover a…» (Daniel: «¿dónde se guardan las plantillas de desinstalación?… agrega
una que diga Desinstalación… crear una pantalla para que los de Trabajo interno reducirle las cantidades»).

- La categoría nueva solo se AGREGA al final del ENUM (REGLA #18: barato, y la guardia del arranque lo salta cuando ya está).
- Nada se mueve solo (REGLA #32): «Mover a…» lo hace una persona; solo cambia la pestaña (categoria_admin), nunca el checklist.
- La cobertura por clase sigue con sus 4 categorías (desinstalación no espera una plantilla por clase).

Rápido (no parsea app.py entero):   py -m pytest tests/test_plantillas_desinstalacion_modulo.py -q
"""
import os
import re
import shutil
import subprocess

import pytest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _leer(*p):
    with open(os.path.join(RAIZ, *p), encoding="utf-8") as f:
        return f.read()


APP = _leer("app.py")
HTML = _leer("templates", "mantenciones", "plantillas.html")


def _bloque(src, inicio, fin_patron):
    i = src.index(inicio)
    m = re.compile(fin_patron, re.M).search(src, i + len(inicio))
    return src[i:m.start() if m else len(src)]


class TestCategoria:
    def test_desinstalacion_es_una_pestana_con_su_nombre(self):
        bloque = _bloque(APP, "_PLANT_CATEGORIAS_ADMIN = (", r"^_PLANT_CATEGORIA_TIPO_SEED")
        assert '"desinstalacion"' in bloque and '"Desinstalación"' in bloque

    def test_el_enum_solo_agrega_el_valor_al_final(self):
        # cada ENUM de categoria_admin / categoria: los 4 de siempre EN EL MISMO ORDEN y desinstalacion al final (nunca reordenar)
        enums = re.findall(r"ENUM\(('instalacion'[^)]*)\)", APP)
        assert enums, "no encontré los ENUM de categoría"
        for e in enums:
            if "'trabajo_interno'" not in e:
                continue
            assert e == "'instalacion','mantencion','visitas','trabajo_interno','desinstalacion'", e

    def test_la_guardia_del_arranque_reconoce_los_modify(self):
        patron = re.search(r'_DDLG_MOD_ENUM\s*=\s*re\.compile\(r"(.+?)", re\.I \| re\.S\)', APP).group(1)
        rx = re.compile(patron, re.I | re.S)
        for tabla, col in (("mant_tarea_plantillas", "categoria_admin"), ("mant_categoria_tipo_map", "categoria")):
            i = APP.index(f"ALTER TABLE {tabla} MODIFY COLUMN {col} ")
            sentencia = APP[i:APP.index(")", APP.index("ENUM(", i)) + 1].replace('" + "', "").replace('"\n            "', "")
            m = rx.match(sentencia)
            assert m and m.group(1) == tabla and m.group(2) == col, sentencia

    def test_la_cobertura_por_clase_no_suma_desinstalacion(self):
        assert "cats = list(_PLCLS_CATEGORIAS)" in APP
        plcls = re.search(r"_PLCLS_CATEGORIAS = \(([^)]*)\)", APP).group(1)
        assert "desinstalacion" not in plcls
        assert re.search(r'_COBERTURA_CATEGORIAS_ESPERADAS = \("instalacion", "mantencion"\)', APP)

    def test_las_etiquetas_de_la_ficha_y_el_asistente_la_conocen(self):
        assert "desinstalacion:'Desinstalación'" in _leer("templates", "ot2", "detalle.html")
        assert "desinstalacion:'Desinstalación'" in _leer("templates", "ot2", "_modal_crear.html")


# ── «Mover a…»: se ejecuta la función de verdad con una base de mentira ──────────────────────────────────────────────────────────
class _Req:
    def __init__(self, body):
        self._b = body

    def get_json(self, silent=False):
        return self._b


@pytest.fixture()
def mover():
    src = _bloque(APP, "def mant_plantillas_mover_categoria():", r"^@app\.route")
    estado = {"plantillas": {1: {"id": 1, "nombre": "Desinstalación y embalaje", "categoria_admin": "instalacion"},
                             2: {"id": 2, "nombre": "(LU) Bancos plano / ajustable", "categoria_admin": "trabajo_interno"},
                             3: {"id": 3, "nombre": "Ya está", "categoria_admin": "desinstalacion"}},
              "sql": [], "logs": [], "mapa": {}}

    def fetchall(sql, params=()):
        return [dict(estado["plantillas"][i]) for i in params if i in estado["plantillas"]]

    def execute(sql, params=()):
        estado["sql"].append(" ".join(sql.split()))
        if sql.startswith("UPDATE mant_tarea_plantillas"):
            estado["plantillas"][params[1]]["categoria_admin"] = params[0]
        elif "mant_categoria_tipo_map" in sql:
            estado["mapa"][params[0]] = params[1]

    ns = {"request": None, "jsonify": lambda d: d, "mysql_fetchall": fetchall, "mysql_execute": execute,
          "current_username": lambda: "daniel", "_mant_log": lambda *a: estado["logs"].append(a),
          "_PLANT_CATEGORIAS_ADMIN": ("instalacion", "desinstalacion", "mantencion", "visitas", "trabajo_interno"),
          "_PLANT_CATEGORIA_LABEL": {"instalacion": "Instalación", "desinstalacion": "Desinstalación", "mantencion": "Mantención",
                                     "visitas": "Visitas", "trabajo_interno": "Trabajo interno"},
          "_TIPO_OT_LABEL": {"desinstalacion": "Desinstalación", "instalacion": "Instalación"}}
    exec(src, ns)

    def llamar(body):
        ns["request"] = _Req(body)
        r = ns["mant_plantillas_mover_categoria"]()
        return (r[0], r[1]) if isinstance(r, tuple) else (r, 200)
    llamar.estado = estado
    return llamar


class TestMover:
    def test_mueve_solo_la_pestana_y_deja_rastro(self, mover):
        d, st = mover({"ids": [1, 2, 3], "categoria": "desinstalacion", "tipo_ot": "desinstalacion"})
        assert st == 200 and d["ok"] and d["movidas"] == [1, 2] and d["ya_estaban"] == 1 and d["mapa_actualizado"]
        e = mover.estado
        assert e["plantillas"][1]["categoria_admin"] == "desinstalacion" and e["mapa"] == {"desinstalacion": "desinstalacion"}
        updates = [q for q in e["sql"] if q.startswith("UPDATE mant_tarea_plantillas")]
        assert updates and all(q == "UPDATE mant_tarea_plantillas SET categoria_admin=%s WHERE id=%s" for q in updates)  # nada más
        assert len([l for l in e["logs"] if l[2] == "categoria_movida"]) == 2
        assert "de «Instalación» a «Desinstalación»" in e["logs"][0][3]

    @pytest.mark.parametrize("body", [{"ids": [1], "categoria": "otra"}, {"ids": [], "categoria": "desinstalacion"},
                                      {"ids": [1], "categoria": "desinstalacion", "tipo_ot": "inventado"}])
    def test_rechaza_lo_que_no_corresponde_sin_tocar_nada(self, mover, body):
        _d, st = mover(body)
        assert st == 400 and mover.estado["sql"] == []

    def test_la_ruta_es_solo_para_gestion(self):
        i = APP.index("def mant_plantillas_mover_categoria():")
        deco = APP[APP.rfind("@app.route", 0, i):i]
        assert '"/mantenciones/api/plantillas/mover-categoria", methods=["POST"]' in deco
        assert "@_mant_required" in deco and "@_no_tecnico" in deco


class TestPantalla:
    def test_pestana_boton_y_aviso(self):
        assert "{key:'desinstalacion',  label:'Desinstalación'" in HTML
        assert 'onclick="moverPlantillasSeleccionadas()"' in HTML and "Mover a…" in HTML
        assert "ilusActionSheet(" in HTML and "function traerDesinstalacion" in HTML
        assert "_moverPlantillas(fuera.map(p => p.id), 'desinstalacion', 'desinstalacion')" in HTML

    def test_sin_popups_nativos_en_lo_nuevo(self):
        nuevo = _bloque(HTML, "async function _moverPlantillas(", r"^async function eliminarPlantillasSeleccionadas")
        for nativo in ("alert(", "confirm(", "prompt("):
            limpio = nuevo.replace("ilusConfirm(", "").replace("ilusAlert(", "").replace("ilusPrompt(", "")
            assert nativo not in limpio.replace("ilusToast(", "")

    @pytest.mark.skipif(not shutil.which("node"), reason="sin Node")
    def test_encuentra_las_de_desinstalacion_que_quedaron_en_otra_pestana(self):
        fn = _bloque(HTML, "function _plDesinstFuera(){", r"^function _plAvisoDesinstHtml")
        js = ("var _plantillas = [{id:1,nombre:'Desinstalación y embalaje',familia_checklist:'desinstalacion',categoria_admin:'instalacion'},"
              "{id:2,nombre:'Revisión — Banco',familia_checklist:'otro',categoria_admin:'trabajo_interno'},"
              "{id:3,nombre:'Checklist de desinstalación rápida',familia_checklist:'otro',categoria_admin:'trabajo_interno'},"
              "{id:4,nombre:'Desinstalación — Trotadora',familia_checklist:'desinstalacion',categoria_admin:'desinstalacion'}];\n"
              + fn + "\nconsole.log(JSON.stringify(_plDesinstFuera().map(p => p.id)));")
        out = subprocess.run(["node", "-e", js], capture_output=True, text=True, timeout=30)
        assert out.returncode == 0, out.stderr
        assert out.stdout.strip() == "[1,3]"

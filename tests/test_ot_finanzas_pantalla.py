"""Pantalla de la OT con la cuenta única de finanzas ("Cobré − Me cobraron = Queda") — Daniel, 2026-10-07.

Qué se vigila:
  · /ot/api/finanzas/<vid> (el dueño de la fila financiera): el 0 es un dato, `costo` ya no se escribe ("Total
    al cliente" es la suma), el cobro escrito a mano queda 'manual' y con motivo, el valorizado se guarda aparte,
    reenviar lo que ya estaba no reescribe nada, y la cobertura (garantía/contrato/cortesía) solo cambia si la
    petición la cambia de verdad (el modal de cierre la estaba borrando).
  · La tarjeta "Finanzas de la OT" pinta la cuenta única (static/ot_finanzas.js) en vez de una fórmula propia.
Sin BD ni Flask: se extraen las funciones de app.py con ast y se ejecutan con una BD simulada.
Correr con:  py -m unittest tests.test_ot_finanzas_pantalla
"""
import ast
import json
import os
import re
import types
import unittest

from tests.test_incidencias_bajas import _codigo_y_arbol

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

FUNCS = ("ot2_api_finanzas", "_ot2_err", "_ot_fin_rep_de", "_ot_fin_base", "_ot_fin_rep_liviano",
         "_ot_finanzas", "_ot_cobertura", "_ot_es_interna", "_ot_fin_num", "_ot_fin_clp")
CONSTS = ("_OT_FIN_SQL_CONTRATO_REAL", "_OT_FIN_ORIGENES_NO_COBRO", "_OT_FIN_FUENTE_COBRO", "_OT_FIN_ZZ_NO_SERVICIO",
          "_OT_FIN_COBERTURA_TXT", "_OT_FIN_UMBRAL_BAJO", "_OT_FIN_BASE_CAMPOS", "_OT_FIN_BASE_MONTOS",
          "_OT_FIN_VALORIZADO_FUENTES", "_OT2_LINEA_ZZ", "_OT2_VALOR_ORIGENES", "_OT2_VALOR_ORIGENES_CON_MOTIVO")

FILA = {"id": 249, "numero_ot": "OT-2026-00249", "tipo": "instalacion", "cliente_id": 5, "costo": 252101,
        "costo_proveedor": 200000, "costo_despacho": 50000, "centro_costo": "sstt", "zz_codigo": "ZZINSTALACION",
        "zz_monto": 252101, "zz_envio_monto": 0, "modalidad_cobro": "pagado", "cubierto_por": "cliente",
        "garantia_motivo": None, "factura_tido": "VD", "factura_nudo": "10666", "valor_origen": "zz",
        "zz_motivo_manual": None, "proveedor_tipo": "externo", "valorizado_clp": None, "valorizado_fuente": None,
        "estado_facturacion": "facturado", "finanzas_at": None, "finanzas_por": None, "contrato_real": 0}

_AMB = None


def _base_ambito():
    global _AMB
    if _AMB is None:
        _, arbol = _codigo_y_arbol()
        amb = {}
        for nodo in arbol.body:
            if isinstance(nodo, ast.Assign) and len(nodo.targets) == 1 and isinstance(nodo.targets[0], ast.Name) \
                    and nodo.targets[0].id in CONSTS:
                exec(compile(ast.Module(body=[nodo], type_ignores=[]), "<app>", "exec"), amb)
            if isinstance(nodo, ast.FunctionDef) and nodo.name in FUNCS:
                nodo = ast.FunctionDef(**{k: getattr(nodo, k) for k in nodo._fields})
                nodo.decorator_list = []
                exec(compile(ast.fix_missing_locations(ast.Module(body=[nodo], type_ignores=[])), "<app>", "exec"),
                     amb)
        faltan = [n for n in FUNCS + CONSTS if n not in amb]
        assert not faltan, f"no se encontraron en app.py: {faltan}"
        _AMB = amb
    return _AMB


class _Req:
    method = "POST"

    def __init__(self, cuerpo):
        self._c = cuerpo

    def get_json(self, silent=True):
        return self._c


def _llamar(cuerpo, fila=None):
    """Llama a ot2_api_finanzas con una BD simulada; devuelve (respuesta, http, updates)."""
    fila = dict(FILA, **(fila or {}))
    updates = []
    amb = dict(_base_ambito())
    amb.update({
        "request": _Req(cuerpo), "jsonify": lambda d: d, "print": lambda *a, **k: None,
        "g": types.SimpleNamespace(user={"role": "admin", "username": "daniel"}),
        "mysql_fetchone": lambda sql, p=None: dict(fila),
        "mysql_fetchall": lambda sql, p=None: [],
        "mysql_execute_returning_rowcount": lambda sql, params: (updates.append((sql, params)) or 1),
        "_mant_log": lambda *a, **k: None, "_mant_notificar": lambda *a, **k: None,
        "current_username": lambda: "daniel",
        "_ot2_finanzas_estado": lambda v: (True, []),
        "_ot_repuestos_desglose": lambda vids: {},
        "_OT2_CENTROS_COSTO": [("sstt", "Servicio Técnico"), ("logistica", "Logística")],
        "_ot_zz_topes_reales": lambda *a, **k: {"excluidos": [], "documentos": [1], "tope_servicio": 10 ** 9,
                                                "tope_despacho": 10 ** 9},
        "_ot_doc_real_a_usuario": lambda t, n: (t, n),
    })
    for nombre in FUNCS:     # que las funciones vean ESTE ámbito (con la BD simulada)
        f = amb[nombre]
        amb[nombre] = types.FunctionType(f.__code__, amb, f.__name__, f.__defaults__, f.__closure__)
    r = amb["ot2_api_finanzas"](249)
    if isinstance(r, tuple):
        return r[0], r[1], updates
    return r, 200, updates


def _sets(updates):
    """Columnas que escribe el UPDATE de mant_visitas -> {columna: valor}."""
    assert updates, "no hubo UPDATE"
    sql, params = updates[0]
    cuerpo = sql.split(" SET ", 1)[1].split(" WHERE ", 1)[0]
    partes = [p.strip() for p in cuerpo.split(",")]
    out, i = {}, 0
    for p in partes:
        col = p.split("=", 1)[0].strip()
        if "%s" in p:
            out[col] = params[i]
            i += 1
        else:
            out[col] = p
    return out


class TestEndpointFinanzas(unittest.TestCase):
    def test_reenviar_lo_mismo_no_reescribe_el_cobro_ni_la_cobertura(self):
        # Lo que mandaba el modal de cierre (guardarCosto) con la foto de la fila.
        r, http, up = _llamar({"costo_proveedor": 200000, "costo": 252101, "zz_monto": 252101,
                               "zz_codigo": "ZZINSTALACION", "garantia_aplica": False, "garantia_motivo": ""})
        self.assertEqual(http, 200)
        s = _sets(up)
        for col in ("zz_monto", "zz_codigo", "costo", "modalidad_cobro", "cubierto_por", "garantia_motivo",
                    "valor_origen"):
            self.assertNotIn(col, s, col)

    def test_no_borra_una_garantia_marcada_por_cubierto_por(self):
        r, http, up = _llamar({"costo_proveedor": 1000, "garantia_aplica": False},
                              fila={"modalidad_cobro": "pagado", "cubierto_por": "garantia",
                                    "garantia_motivo": "falla de fábrica"})
        s = _sets(up)
        self.assertNotIn("cubierto_por", s)
        self.assertNotIn("garantia_motivo", s)

    def test_no_convierte_un_contrato_en_pagado(self):
        r, http, up = _llamar({"costo_proveedor": 1000, "garantia_aplica": False},
                              fila={"cubierto_por": "contrato", "garantia_motivo": "Plan anual 2026"})
        s = _sets(up)
        self.assertNotIn("cubierto_por", s)
        self.assertNotIn("garantia_motivo", s)

    def test_retractar_una_garantia_si_cambia(self):
        r, http, up = _llamar({"garantia_aplica": False},
                              fila={"modalidad_cobro": "garantia", "cubierto_por": "garantia"})
        s = _sets(up)
        self.assertEqual((s["modalidad_cobro"], s["cubierto_por"]), ("pagado", "cliente"))

    def test_declarar_garantia_si_cambia(self):
        r, http, up = _llamar({"garantia_aplica": True, "garantia_motivo": "falla dentro del período"})
        s = _sets(up)
        self.assertEqual((s["modalidad_cobro"], s["cubierto_por"]), ("garantia", "garantia"))

    def test_costo_ya_no_se_escribe(self):
        r, http, up = _llamar({"costo": 999999, "costo_proveedor": 1})
        self.assertNotIn("costo", _sets(up))
        self.assertNotIn("costo=", up[0][0].replace("costo_", "x_"))

    def test_el_cero_se_guarda_como_cero(self):
        r, http, up = _llamar({"zz_monto": 0, "zz_envio_monto": 0, "zz_motivo_manual": "No se cobró: cortesía"},
                              fila={"zz_envio_monto": None})
        s = _sets(up)
        self.assertEqual(s["zz_monto"], 0)
        self.assertEqual(s["zz_envio_monto"], 0, "COALESCE(0, ...) = 0: antes llegaba None y no se guardaba")

    def test_cobro_a_mano_queda_manual_y_pide_motivo(self):
        r, http, up = _llamar({"zz_monto": 300000})
        self.assertEqual((http, r["codigo"]), (400, "COBRO_MANUAL_SIN_MOTIVO"))
        self.assertEqual(up, [], "sin motivo no se escribe nada")
        r, http, up = _llamar({"zz_monto": 300000, "zz_motivo_manual": "El documento no trae línea de servicio"})
        s = _sets(up)
        self.assertEqual((s["zz_monto"], s["valor_origen"]), (300000, "manual"))
        self.assertEqual(s["zz_codigo"], "ZZINSTALACION", "se ajusta la MISMA línea de servicio")

    def test_cobro_traido_del_documento_no_pide_motivo(self):
        r, http, up = _llamar({"zz_monto": 280000, "valor_origen": "zz"})
        self.assertEqual(http, 200)
        self.assertEqual(_sets(up)["valor_origen"], "zz")

    def test_un_zzretiro_no_sigue_siendo_la_linea_del_cobro(self):
        r, http, up = _llamar({"zz_monto": 150000, "zz_motivo_manual": "Se cobró la instalación"},
                              fila={"zz_monto": 1, "zz_codigo": "ZZRETIRO"})
        self.assertEqual(_sets(up)["zz_codigo"], "ZZINSTALACION")

    def test_valorizado_aparte_y_con_fuente_conocida(self):
        r, http, up = _llamar({"valorizado_clp": 280000, "valorizado_fuente": "cotizador"})
        s = _sets(up)
        self.assertEqual((s["valorizado_clp"], s["valorizado_fuente"]), (280000, "cotizador"))
        r, http, up = _llamar({"valorizado_clp": 280000, "valorizado_fuente": "<script>"})
        self.assertEqual(_sets(up)["valorizado_fuente"], "a_mano")
        r, http, up = _llamar({"valorizado_clp": ""})
        s = _sets(up)
        self.assertEqual((s["valorizado_clp"], s["valorizado_fuente"]), (None, None))

    def test_la_respuesta_trae_la_cuenta_unica(self):
        r, http, up = _llamar({"costo_proveedor": 200000})
        self.assertIn("fin", r)
        self.assertEqual(r["fin"]["cobre"]["total"], 252101)
        self.assertEqual(r["base"]["zz_monto"], 252101)
        self.assertNotIn("items", r["rep"])


class TestPantalla(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with open(os.path.join(RAIZ, "templates", "ot2", "detalle.html"), encoding="utf-8") as fh:
            cls.html = fh.read().replace(chr(13) + chr(10), chr(10))
        with open(os.path.join(RAIZ, "app.py"), encoding="utf-8") as fh:
            cls.app = fh.read().replace(chr(13) + chr(10), chr(10))

    def test_carga_la_cuenta_del_navegador_con_cache_busting_automatico(self):
        self.assertIn("<script src=\"{{ url_for('static', filename='ot_finanzas.js') }}\"></script>", self.html)
        self.assertNotIn("ot_finanzas.js?v=", self.html)

    def test_la_tarjeta_ya_no_tiene_formula_propia(self):
        i = self.html.index("window.otdFinCuenta = function")
        cuerpo = self.html[i:i + 600]
        self.assertIn("ilusOtFinanzas", cuerpo)
        self.assertNotIn("c.cTot", self.html, "la fórmula vieja de la tarjeta no debe volver")
        i = self.html.index("function otdFinCorrCalc(")
        self.assertIn("ilusOtFinanzas", self.html[i:i + 2500])

    def test_palabras_de_daniel(self):
        for t in ("Cobré − Me cobraron = Queda", ">Cobré</span>", ">Me cobraron</span>", "Nos costó",
                  "Lo que dicen los documentos", "Valorizar (opcional)"):
            self.assertIn(t, self.html, t)
        self.assertNotIn("Guardado en la OT (suma de los documentos)</div>", self.html)

    def test_total_al_cliente_es_la_suma_y_no_se_escribe(self):
        i = self.html.index('id="otdFinCosto"')
        self.assertIn("readonly", self.html[i:i + 300])
        i = self.html.index("window.otdGuardarFinanzas = async function")
        cuerpo = self.html[i:self.html.index("/* ══ Actividad de la OT", i)]
        self.assertNotIn("costo: window.otdMoneyVal('otdFinCosto')", cuerpo)
        self.assertIn("valorizado_clp", cuerpo)

    def test_el_modal_de_cierre_ya_no_reenvia_la_foto(self):
        i = self.html.index("async function guardarCosto(")
        cuerpo = self.html[i:i + 3500]
        for k in ("garantia_aplica:", "zz_monto:", "factura_nudo:", "costo: OTF_FIN_ACTUAL"):
            self.assertNotIn(k, cuerpo, k)

    def test_el_tecnico_no_recibe_montos(self):
        # En ot2_detalle la cuenta solo se calcula para gestión (REGLA #19).
        i = self.app.index("def ot2_detalle(vid):")
        cuerpo = self.app[i:self.app.index("\n@app.route", i)]
        j = cuerpo.index("fin = fin_base = fin_rep = None")
        self.assertIn("if not _es_rol_tecnico():", cuerpo[j:j + 200])
        self.assertIn("fin: {{ fin | tojson if fin else 'null' }}", self.html)

    def test_resultado_financiero_agrega_sin_quitar(self):
        i = self.app.index("def ot2_api_resultado_financiero(vid):")
        cuerpo = self.app[i:i + 3500]
        self.assertIn("_ot_resultado_financiero(v, rep)", cuerpo, "las claves de antes siguen")
        self.assertIn("_ot_finanzas(v, rep)", cuerpo)
        self.assertIn("_es_rol_tecnico()", cuerpo)


if __name__ == "__main__":
    unittest.main()

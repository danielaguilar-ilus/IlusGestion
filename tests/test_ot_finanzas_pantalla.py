"""Pantalla de la OT con la cuenta única de finanzas ("Cobré − Me cobraron = Queda") — Daniel, 2026-10-07.

Qué se vigila:
  · /ot/api/finanzas/<vid> (el dueño de la fila financiera): el 0 es un dato, `costo` ya no se escribe ("Total
    al cliente" es la suma), el cobro escrito a mano queda 'manual' y con motivo, el valorizado se guarda aparte,
    reenviar lo que ya estaba no reescribe nada, y la cobertura (garantía/contrato/cortesía) solo cambia si la
    petición la cambia de verdad (el modal de cierre la estaba borrando).
  · La tarjeta "Finanzas de la OT" pinta la cuenta única (static/ot_finanzas.js) en vez de una fórmula propia.
  · Revisión 2026-10-07: el Paso 3 del modal de cierre usa la misma cuenta; `costo` acompaña a lo cobrado solo si
    ya existía (lo lee lo que ve el cliente); la OT interna valorizada no dice "falta cuánto vale"; y escribir a
    mano el cobro de una preventiva de contrato real ya no esconde el campo: se avisa y se pregunta al guardar.
Sin BD ni Flask: se extraen las funciones de app.py con ast y se ejecutan con una BD simulada.
Correr con:  py -m unittest tests.test_ot_finanzas_pantalla
"""
import ast
import json
import os
import re
import shutil
import subprocess
import tempfile
import types
import unittest

from tests.test_incidencias_bajas import _codigo_y_arbol

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

FUNCS = ("ot2_api_finanzas", "_ot2_err", "_ot_fin_rep_de", "_ot_fin_base", "_ot_fin_rep_liviano",
         "_ot_finanzas", "_ot_cobertura", "_ot_es_interna", "_ot_fin_num", "_ot_fin_clp")
CONSTS = ("_OT_FIN_SQL_CONTRATO_REAL", "_OT_FIN_ORIGENES_NO_COBRO", "_OT_FIN_ORIGENES_COBRO_RESPALDO", "_OT_FIN_FUENTE_COBRO", "_OT_FIN_ZZ_NO_SERVICIO",
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
        # 2026-10-07 (documento absoluto): declarar garantía pasa por Daniel; acá se simula "superadmin ya lo registró".
        "_ot_cobro_cero_desde_peticion": lambda vid, d, origen="": (None, None),
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


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  2026-10-07 — revisión adversarial del frente "pantalla de la OT"
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
OT201 = {"modalidad_cobro": "garantia", "cubierto_por": "garantia", "tipo": "instalacion", "cliente_id": 5,
         "contrato_real": 0, "costo": 200000, "zz_monto": 1, "zz_codigo": "ZZRETIRO", "zz_envio_monto": None,
         "valor_origen": "zz", "costo_proveedor": 130000, "costo_despacho": 70000, "proveedor_tipo": "externo",
         "valorizado_clp": None, "valorizado_fuente": None, "tecnico_nombre": "Técnico externo"}
COBRADA = {"modalidad_cobro": "pagado", "cubierto_por": "cliente", "tipo": "instalacion", "cliente_id": 5,
           "contrato_real": 0, "costo": 180000, "zz_monto": 150000, "zz_codigo": "ZZINSTALACION",
           "zz_envio_monto": 30000, "valor_origen": "zz", "costo_proveedor": 100000, "costo_despacho": 20000,
           "proveedor_tipo": "externo", "valorizado_clp": None, "valorizado_fuente": None, "tecnico_nombre": "X"}


def _fin(v):
    return _base_ambito()["_ot_finanzas"](v, None)


class TestModalCierrePaso3(unittest.TestCase):
    """El Paso 3 del modal "Firmar y cerrar OT" usa la cuenta única (antes: costo − proveedor − despacho)."""

    @classmethod
    def setUpClass(cls):
        import jinja2
        with open(os.path.join(RAIZ, "templates", "ot2", "detalle.html"), encoding="utf-8") as fh:
            html = fh.read().replace(chr(13) + chr(10), chr(10))
        i = html.index("{% macro _fclp(n) %}")
        macro = html[i:html.index("{% endmacro %}", i) + len("{% endmacro %}")]
        j = html.index("{# ══ PASO 3 — FINANZAS")
        paso3 = html[j:html.index("{# ══ PASO 4", j)]
        cls.tpl = jinja2.Environment(autoescape=True).from_string(macro + paso3)

    def _render(self, v, fin, es_int=False):
        return self.tpl.render(v=v, fin=fin, _es_int=es_int, puede_cobertura=True,
                               _falta_costo=(not es_int) and v.get("costo_proveedor") is None)

    def test_garantia_ot201_no_dice_le_cobre_ni_gane(self):
        out = self._render(OT201, _fin(OT201))
        self.assertNotIn("Le cobré al cliente", out)
        self.assertNotIn("Gané", out)
        self.assertNotIn("% de margen", out)
        self.assertIn("Garantía: no se le cobra", out)
        self.assertIn("Nos costó", out)
        self.assertIn("$200.000", out)
        self.assertIn('class="v info"', out, "lo que nos costó va en azul informativo, no como pérdida")

    def test_ot_cobrada_cobre_me_cobraron_queda(self):
        out = self._render(COBRADA, _fin(COBRADA))
        for t in (">Cobré</span>", "$180.000", ">Me cobraron</span>", "$120.000", ">Queda</span>", "$60.000",
                  "33,3 % de margen"):
            self.assertIn(t, out, t)
        self.assertNotIn("Le cobré al cliente", out)

    def test_falta_lo_del_tecnico(self):
        v = dict(COBRADA, costo_proveedor=None)
        out = self._render(v, _fin(v))
        self.assertIn("Por declarar", out)
        self.assertIn("No se puede calcular", out)

    def test_sin_fin_queda_el_calculo_de_antes(self):
        out = self._render(COBRADA, None)
        self.assertIn("Le cobré al cliente", out)

    def test_trabajo_interno_dice_lo_que_nos_costo(self):
        v = dict(COBRADA, cliente_id=None, modalidad_cobro="interno", proveedor_tipo="interno",
                 costo_proveedor=None, costo_despacho=None, zz_monto=None, zz_envio_monto=None, costo=None)
        out = self._render(v, _fin(v), es_int=True)
        self.assertIn("Trabajo interno: no se le cobra", out)


def _llamar_vivo(cuerpo, fila=None):
    """Como _llamar, pero la BD simulada APLICA los UPDATE (así la relectura ve lo guardado)."""
    fila = dict(FILA, **(fila or {}))
    updates, vistos = [], []

    def _ejecutar(sql, params):
        updates.append((sql, params))
        sets_sql = sql.split(" SET ", 1)[1].split(" WHERE ", 1)[0]
        i = 0
        for p in sets_sql.split(","):
            if "=" not in p or "%s" not in p:
                continue
            col = p.split("=", 1)[0].strip()
            val = params[i]
            i += 1
            if "COALESCE" in p and val is None:
                continue
            fila[col] = val
        return 1

    amb = dict(_base_ambito())
    amb.update({
        "request": _Req(cuerpo), "jsonify": lambda d: d, "print": lambda *a, **k: None,
        "g": types.SimpleNamespace(user={"role": "admin", "username": "daniel"}),
        "mysql_fetchone": lambda sql, p=None: dict(fila),
        "mysql_fetchall": lambda sql, p=None: [],
        "mysql_execute_returning_rowcount": _ejecutar,
        "_mant_log": lambda *a, **k: None, "_mant_notificar": lambda *a, **k: None,
        "current_username": lambda: "daniel",
        "_ot2_finanzas_estado": lambda v: (vistos.append(dict(v)) or (True, [])),
        "_ot_repuestos_desglose": lambda vids: {},
        "_OT2_CENTROS_COSTO": [("sstt", "Servicio Técnico"), ("logistica", "Logística")],
        "_ot_zz_topes_reales": lambda *a, **k: {"excluidos": [], "documentos": [1], "tope_servicio": 10 ** 9,
                                                "tope_despacho": 10 ** 9},
        "_ot_doc_real_a_usuario": lambda t, n: (t, n),
        # 2026-10-07 (documento absoluto): declarar garantía pasa por Daniel; acá se simula "superadmin ya lo registró".
        "_ot_cobro_cero_desde_peticion": lambda vid, d, origen="": (None, None),
    })
    for nombre in FUNCS:
        f = amb[nombre]
        amb[nombre] = types.FunctionType(f.__code__, amb, f.__name__, f.__defaults__, f.__closure__)
    r = amb["ot2_api_finanzas"](249)
    if isinstance(r, tuple):
        r = r[0]
    return r, updates, fila, vistos


class TestPrecioAlClienteComoHistoria(unittest.TestCase):
    """`costo` lo sigue leyendo lo que ve el cliente (correo "visita agendada"): si ya existía, acompaña a lo
    cobrado cuando la tarjeta lo cambia (y desde la integración del 2026-10-07 también se llena si estaba vacío).
    Nunca en una OT que no se cobra, nunca en una cerrada."""

    def test_acompana_a_lo_cobrado_si_ya_existia(self):
        r, up, fila, _ = _llamar_vivo({"zz_monto": 280000, "valor_origen": "zz"})
        self.assertEqual(len(up), 2, up)
        sql, params = up[1]
        self.assertTrue(sql.startswith("UPDATE mant_visitas SET costo=%s WHERE id=%s"), sql)
        self.assertIn("estado NOT IN ('completada','cerrada','cancelada','anulada')", sql)
        self.assertEqual(params, (280000, 249))
        self.assertEqual(fila["costo"], 280000)
        self.assertEqual(r["fin"]["cobre"]["total"], 280000)
        self.assertFalse([a for a in r["fin"]["avisos"] if "Precio al cliente" in a])

    def test_se_llena_si_estaba_vacio(self):
        """2026-10-07 (integración fin-paso3): antes la tarjeta escribía `costo` siempre; si queda vacío, la OT
        que declara su cobro saldría en el correo "visita técnica programada" sin «Costo estimado»."""
        r, up, fila, _ = _llamar_vivo({"zz_monto": 280000, "valor_origen": "zz"}, fila={"costo": None})
        self.assertEqual(len(up), 2, up)
        self.assertEqual(fila["costo"], 280000)
        self.assertFalse([a for a in r["fin"]["avisos"] if "Precio al cliente" in a])

    def test_solo_despacho_sin_servicio_no_crea_costo(self):
        r, up, fila, _ = _llamar_vivo({"zz_envio_monto": 30000}, fila={"costo": None, "zz_monto": None})
        self.assertEqual(len(up), 1, up)
        self.assertIsNone(fila["costo"])

    def test_no_se_toca_si_el_cobro_no_cambia(self):
        r, up, fila, _ = _llamar_vivo({"costo_proveedor": 1000, "costo": 1})
        self.assertEqual(len(up), 1)
        self.assertEqual(fila["costo"], 252101)

    def test_no_se_toca_en_una_ot_que_no_se_cobra(self):
        r, up, fila, _ = _llamar_vivo({"zz_envio_monto": 30000},
                                      fila={"modalidad_cobro": "garantia", "cubierto_por": "garantia",
                                            "garantia_motivo": "falla de fábrica"})
        self.assertEqual(len(up), 1)
        self.assertEqual(fila["costo"], 252101)

    def test_no_se_toca_si_lo_cobrado_sale_del_mismo_precio(self):
        r, up, fila, _ = _llamar_vivo({"zz_envio_monto": 20000}, fila={"zz_monto": None, "costo": 100000})
        self.assertEqual(len(up), 1)
        self.assertEqual(fila["costo"], 100000)

    def test_lo_que_falta_se_calcula_con_la_relectura_completa(self):
        r, up, fila, vistos = _llamar_vivo({"costo_proveedor": 1000})
        self.assertTrue(vistos)
        for k in ("valorizado_clp", "cliente_id", "contrato_real"):
            self.assertIn(k, vistos[-1], k)


class TestFaltanTrabajoInterno(unittest.TestCase):
    """Una OT interna valorizada en valorizado_clp (donde la deja la tarjeta) ya no dice "falta cuánto vale"."""

    @classmethod
    def setUpClass(cls):
        _, arbol = _codigo_y_arbol()
        amb = dict(_base_ambito())
        for nodo in arbol.body:
            if isinstance(nodo, ast.FunctionDef) and nodo.name == "_ot2_finanzas_estado":
                exec(compile(ast.Module(body=[nodo], type_ignores=[]), "<app>", "exec"), amb)
        cls.estado = staticmethod(amb["_ot2_finanzas_estado"])

    def _faltan(self, **kw):
        v = {"centro_costo": "sstt", "cliente_id": None, "modalidad_cobro": "interno", "tipo": "revision_interna",
             "costo": None, "valorizado_clp": None}
        v.update(kw)
        return self.estado(v)[1]

    def test_valorizado_resuelve(self):
        self.assertEqual(self._faltan(valorizado_clp=50000), [])

    def test_costo_antiguo_tambien(self):
        self.assertEqual(self._faltan(costo=40000), [])

    def test_sin_ninguno_falta(self):
        self.assertTrue([f for f in self._faltan() if "cuánto vale" in f])


@unittest.skipUnless(shutil.which("node"), "node no está instalado")
class TestCoberturaEnVivo(unittest.TestCase):
    """Preventiva de contrato real: bajar a mano el cobro del documento la vuelve "contrato" (Cobré $0).
    La tarjeta ya no esconde el campo que se está escribiendo: avisa, y Guardar / Corregir preguntan antes."""

    @classmethod
    def setUpClass(cls):
        with open(os.path.join(RAIZ, "templates", "ot2", "detalle.html"), encoding="utf-8") as fh:
            cls.html = fh.read().replace(chr(13) + chr(10), chr(10))

    def _node(self, codigo):
        js = os.path.join(RAIZ, "static", "ot_finanzas.js")
        with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False, encoding="utf-8") as fh:
            fh.write("const m = require(%s);\n" % json.dumps(js))
            fh.write(codigo)
            script = fh.name
        try:
            return json.loads(subprocess.run(["node", script], capture_output=True, text=True, encoding="utf-8",
                                             check=True).stdout)
        finally:
            os.unlink(script)

    def _region(self):
        i = self.html.index("window.otdFinFmt = function")
        j = self.html.index("window.otfPintarPaso3 = function", i)
        k = self.html.index(chr(10) + "};" + chr(10), j) + 3
        return self.html[i:k]

    def test_la_regla_cambia_la_cobertura_al_escribir_a_mano(self):
        base = {"modalidad_cobro": "pagado", "cubierto_por": "contrato", "tipo": "preventiva", "cliente_id": 5,
                "contrato_real": 1, "zz_monto": 100000, "zz_codigo": "ZZMANTENCION", "valor_origen": "zz",
                "costo_proveedor": 40000}
        out = self._node("const b = %s;\nprocess.stdout.write(JSON.stringify([m.finanzas(b, null).cobertura, "
                         "m.finanzas(Object.assign({}, b, {zz_monto: 80000, valor_origen: 'manual'}), null).cobertura]));"
                         % json.dumps(base))
        self.assertEqual(out, ["cobra", "contrato"])

    def test_el_bloque_no_se_esconde_mientras_se_escribe(self):
        i = self.html.index("function pintarResultado(){")
        cuerpo = self.html[i:i + 4000]
        self.assertIn("bloqueCobro.hidden = !(r.cobra || cobraGuardada)", cuerpo)
        self.assertNotIn("bloqueCobro.hidden = !r.cobra;", cuerpo)
        self.assertIn("otdFinCobVivoAviso", cuerpo)

    def test_guardar_y_corregir_preguntan_antes(self):
        i = self.html.index("window.otdGuardarFinanzas = async function")
        self.assertIn("Esta OT dejaría de cobrarse", self.html[i:i + 9000])
        i = self.html.index("async function otdFinCorrGuardar(")
        self.assertIn("_otdFcPierde", self.html[i:i + 2500])

    def test_aviso_y_paso3_en_el_navegador(self):
        # OJO: la región trae '%' (otdFinPct): los datos se pegan aparte, sin formatear con %.
        codigo = (
            "const window = {}; const elems = {otfFin3Box: {innerHTML: ''}};\n"
            "const document = {getElementById: id => elems[id] || null};\n"
            + self._region() + "\n"
            + "const g = m.finanzas(" + json.dumps(OT201) + ", null);\n"
            + "const c = m.finanzas(" + json.dumps(COBRADA) + ", null);\n"
            "window.otfPintarPaso3(g); const paso3gar = elems.otfFin3Box.innerHTML;\n"
            "window.otfPintarPaso3(c); const paso3cob = elems.otfFin3Box.innerHTML;\n"
            "const aviso = window.otdFinPierdeCobroHtml({cobertura: 'contrato', "
            "cobertura_txt: 'Mantención de contrato: se paga con el contrato'});\n"
            "process.stdout.write(JSON.stringify([paso3gar, paso3cob, aviso]));\n"
        )
        paso3gar, paso3cob, aviso = self._node(codigo)
        self.assertIn("Nos costó", paso3gar)
        self.assertIn("$200.000", paso3gar)
        self.assertIn("Garantía: no se le cobra", paso3gar)
        self.assertNotIn("Le cobré", paso3gar)
        for t in ("$180.000", "$120.000", "$60.000", "33,3 % de margen"):
            self.assertIn(t, paso3cob, t)
        self.assertIn("Mantención de contrato", aviso)
        self.assertIn("$0", aviso)


if __name__ == "__main__":
    unittest.main()

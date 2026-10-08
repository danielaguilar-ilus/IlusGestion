"""Los que leen la plata de una OT FUERA de su pantalla dicen lo mismo que la OT. Daniel, 2026-10-07.

Modelo único "Cobré − Me cobraron = Queda" (+ Valorizado aparte), ver tests/test_ot_finanzas_modelo.py.
Vida del cliente, Facturación de proveedores, Facturas de proveedor, los Excel, el panel de costos por técnico,
el Monitor, el Agente/Radar, Analytics, la ficha del cliente y el calendario ya no tienen fórmula propia:
leen _ot_finanzas (o la suma de varias, _ot_fin_agregado). Estas pruebas lo fijan con casos con números
(empezando por la OT-2026-00201) y revisan el texto fuente de cada lector.

Sin BD ni Flask: las funciones se extraen de app.py con ast.
Correr con:  py -m unittest tests.test_ot_finanzas_lectores
"""
import ast
import re
import unittest

from tests.test_incidencias_bajas import _codigo_y_arbol
from tests.test_incidencias_repuesto_tercera_fuente import _fuente_de

FUNCS = ("_ot_es_interna", "_ot_cobertura", "_ot_fin_num", "_ot_fin_clp", "_ot_finanzas", "_vida_margen_clase",
         "_ot_fin_agregado", "_ot_fin_de_fila", "_ot_cobro_de_fin", "_ot_cobro_facprov", "_ot_cobro_cliente",
         "_ot_tv_fin_resumen", "_ot_fin_sql_contrato_real", "_ot_fin_cols_sql", "_ot_fin_contrato_real_de",
         "_ot_fin_lote", "_mfp_fila_ot", "_mfp_nombre_proveedor_ot", "_mfp_resumen_filas", "mant_cliente_finanzas")
CONSTS = ("_OT_FIN_ORIGENES_NO_COBRO", "_OT_FIN_ORIGENES_COBRO_RESPALDO", "_OT_FIN_FUENTE_COBRO", "_OT_FIN_ZZ_NO_SERVICIO", "_OT_FIN_COBERTURA_TXT",
          "_OT_FIN_UMBRAL_BAJO", "_OT_FIN_COBERTURA_CORTA", "_OT_FIN_AGG_APARTE", "_OT_FIN_COLS",
          "_OT_FIN_SQL_CONTRATO_REAL", "_MFP_ESTADOS_FACTURABLES", "_COBRO_ORIGENES_REALES")

AMB = None


def _amb():
    """Ámbito con las funciones reales de app.py + dobles de BD/Flask (lo que el lote consulta queda anotado)."""
    global AMB
    if AMB is not None:
        return AMB
    _, arbol = _codigo_y_arbol()
    consultas = []

    def _fetchall(sql, params=None):
        consultas.append(sql)
        # contrato_real: la visita 7 apunta a un contrato real
        return [{"id": int(p), "contrato_real": 1 if int(p) == 7 else 0} for p in (params or ())]

    amb = {"re": re, "print": lambda *a, **k: None, "mysql_fetchall": _fetchall, "_CONSULTAS": consultas,
           "_ot_repuestos_desglose": lambda ids: {i: {"costo": 15000.0, "por_origen": {"bodega": 15000.0, "compra": 0.0,
                                                                                       "manual": 0.0},
                                                     "n_sin_costo": 0} for i in ids if i == 3},
           "chile_fmt_filter": lambda v, fmt=None: "01/10/2026", "_TIPO_OT_LABEL": {},
           "_ot_tv_hhmm": lambda h: "", "_OT2_CENTROS_COSTO": [("sstt", "Servicio Técnico")]}
    for nodo in arbol.body:
        if isinstance(nodo, ast.Assign) and len(nodo.targets) == 1 and isinstance(nodo.targets[0], ast.Name) \
                and nodo.targets[0].id in CONSTS:
            exec(compile(ast.Module(body=[nodo], type_ignores=[]), "<app>", "exec"), amb)
        if isinstance(nodo, ast.FunctionDef) and nodo.name in FUNCS:
            nodo.decorator_list = []
            exec(compile(ast.Module(body=[nodo], type_ignores=[]), "<app>", "exec"), amb)
    faltan = [n for n in FUNCS + CONSTS if n not in amb]
    assert not faltan, faltan
    AMB = amb
    return amb


def V(**kw):
    base = {"id": 1, "modalidad_cobro": "pagado", "cubierto_por": "cliente", "tipo": "instalacion", "cliente_id": 5,
            "contrato_id": None, "contrato_real": 0, "costo": None, "zz_monto": None, "zz_codigo": None,
            "zz_envio_monto": None, "valor_origen": None, "costo_proveedor": None, "costo_despacho": None,
            "estado": "cerrada", "numero_ot": "OT-X"}
    base.update(kw)
    return base


def FIN(**kw):
    rep = kw.pop("rep", None)
    return _amb()["_ot_finanzas"](V(**kw), rep)


OT201 = dict(modalidad_cobro="garantia", costo=200000, zz_monto=1, zz_codigo="ZZRETIRO", valor_origen="zz",
             costo_proveedor=130000, costo_despacho=70000)
NORMAL = dict(zz_monto=150000, valor_origen="zz", zz_envio_monto=30000, costo_proveedor=100000, costo_despacho=20000)


class TestCobroDeFacturacion(unittest.TestCase):
    """_ot_cobro_cliente / _mfp_fila_ot: el cobro es el "Cobré" de la OT; lo pagado al proveedor, sin repuestos."""

    def test_ot201_garantia_no_cobra_y_se_paga_igual(self):
        a = _amb()
        self.assertEqual(a["_ot_cobro_cliente"](V(**OT201)), (0.0, 0.0, ""))
        d = a["_mfp_fila_ot"](V(**OT201))
        self.assertTrue(d["es_garantia"])
        self.assertEqual(d["cobertura_corta"], "Garantía")
        self.assertEqual(d["cobrado_cliente"], 0)
        self.assertEqual(d["sugerido"], 200000, "lo que se le paga al técnico no cambia: técnico + despacho")
        self.assertEqual(d["margen"], -200000)
        self.assertIsNone(d["margen_pct"])
        self.assertFalse(d["sin_cobro_declarado"])

    def test_ot201_si_no_fuera_garantia_el_precio_al_cliente_sin_origen_no_es_cobro(self):
        # Revisión 2026-10-07: ZZRETIRO no es cobro del servicio, y en estas pantallas el «Precio al cliente»
        # (`costo`) solo cuenta si viene de una cotización, un contrato o lo escribió una persona (regla del
        # 2026-09-20). Acá su origen es 'zz' (el de la línea ZZRETIRO): falta lo que cobraste.
        a = _amb()
        v = dict(OT201, modalidad_cobro="pagado")
        self.assertEqual(a["_ot_cobro_cliente"](V(**v)), (0.0, 0.0, ""))
        d = a["_mfp_fila_ot"](V(**v))
        self.assertTrue(d["sin_cobro_declarado"])
        self.assertEqual(d["cobrado_cliente"], 0)

    def test_precio_al_cliente_de_cotizacion_contrato_o_a_mano_si_es_cobro(self):
        a = _amb()
        for origen in ("cotizacion", "contrato", "manual", "supuesto"):
            v = V(costo=200000, valor_origen=origen, costo_proveedor=130000, zz_motivo_manual="cobro aparte")
            self.assertEqual(a["_ot_cobro_cliente"](v), (200000.0, 0.0, "valor_ot"), origen)
            self.assertFalse(a["_mfp_fila_ot"](v)["sin_cobro_declarado"], origen)

    def test_factura_copiada_en_precio_al_cliente_no_da_un_margen_falso(self):
        # Caso del revisor: OT de ticket sin línea ZZ; «Asociar factura» copió en `costo` el bruto de una FCV de
        # $1.500.000 que incluye un equipo. Antes de esta revisión daba cobrado 1.500.000 y "Margen sano".
        a = _amb()
        v = V(tipo="correctiva", costo=1500000, valor_origen=None, costo_proveedor=80000, costo_despacho=20000)
        self.assertEqual(a["_ot_cobro_cliente"](v), (0.0, 0.0, ""))
        d = a["_mfp_fila_ot"](v)
        self.assertEqual((d["cobrado_cliente"], d["sugerido"]), (0, 100000))
        self.assertTrue(d["sin_cobro_declarado"])
        self.assertIsNone(d["margen_pct"])
        self.assertFalse(d["es_garantia"])
        self.assertTrue(any("no cuenta como cobro" in x and "$1.500.000" in x for x in d["fin_avisos"]))
        self.assertFalse(any("no separa servicio y despacho" in x for x in d["fin_avisos"]),
                         "el aviso de la cuenta («sale del Precio al cliente») se reemplaza, no se suma")
        t = a["_mfp_resumen_filas"]([d])
        self.assertEqual((t["n_comparable"], t["n_sin_cobro"], t["pagado_sin_cobro"], t["cobrado"]),
                         (0, 1, 100000.0, 0.0))

    def test_facturacion_usa_el_resguardo(self):
        for nombre in ("_facprov_datos", "_mfp_fila_ot", "mant_facturas_proveedor_xlsx"):
            src = _fuente_de(nombre)
            self.assertIn("_ot_cobro_facprov(", src, nombre)
        self.assertNotIn("tampoco se cae a `costo`", _fuente_de("_facprov_datos"),
                         "el comentario viejo decía lo contrario de lo que hace el código")

    def test_normal_con_documento(self):
        a = _amb()
        self.assertEqual(a["_ot_cobro_cliente"](V(**NORMAL)), (150000.0, 30000.0, "zz"))
        d = a["_mfp_fila_ot"](V(**NORMAL))
        self.assertEqual((d["cobrado_cliente"], d["sugerido"], d["margen"]), (180000.0, 120000.0, 60000.0))
        self.assertEqual(round(d["margen_pct"], 1), 33.3)

    def test_columnas_renombradas_de_facturas_de_proveedor(self):
        # _MFP_SELECT_OT trae los montos como cliente_*: se leen igual.
        v = V(costo_proveedor=100000, costo_despacho=20000, valor_origen="zz",
              cliente_zz_monto=150000, cliente_zz_envio_monto=30000, cliente_costo=None)
        self.assertEqual(_amb()["_ot_cobro_cliente"](v), (150000.0, 30000.0, "zz"))

    def test_un_estimado_no_es_cobro_y_queda_como_falta(self):
        d = _amb()["_mfp_fila_ot"](V(zz_monto=80000, valor_origen="estimado", costo=80000, costo_proveedor=50000))
        self.assertEqual(d["cobrado_cliente"], 0)
        self.assertTrue(d["sin_cobro_declarado"])
        self.assertFalse(d["es_garantia"])

    def test_cobro_declarado_en_cero_entra_al_margen(self):
        a = _amb()
        d = a["_mfp_fila_ot"](V(zz_monto=0, valor_origen="zz", costo_proveedor=30000))
        self.assertFalse(d["sin_cobro_declarado"])
        t = a["_mfp_resumen_filas"]([d])
        self.assertEqual((t["n_comparable"], t["n_sin_cobro"], t["margen"]), (1, 0, -30000.0))

    def test_contrato_real_no_se_cobra(self):
        d = _amb()["_mfp_fila_ot"](V(tipo="preventiva", contrato_real=1, costo=60000, costo_proveedor=30000))
        self.assertTrue(d["es_garantia"])
        self.assertEqual(d["cobertura_corta"], "Contrato")
        self.assertEqual(d["cobrado_cliente"], 0)

    def test_resumen_separa_no_cobra_y_sin_cobro(self):
        a = _amb()
        filas = [a["_mfp_fila_ot"](V(**x)) for x in (OT201, NORMAL, dict(zz_monto=100000, valor_origen="estimado",
                                                                          costo_proveedor=40000))]
        t = a["_mfp_resumen_filas"](filas)
        self.assertEqual((t["n_garantia"], t["pagado_garantia"]), (1, 200000.0))
        self.assertEqual((t["n_sin_cobro"], t["pagado_sin_cobro"]), (1, 40000.0))
        self.assertEqual((t["n_comparable"], t["cobrado"], t["margen"]), (1, 180000.0, 60000.0))


class TestAgregado(unittest.TestCase):
    """_ot_fin_agregado: el resultado de varias OT es la SUMA de lo que dice cada una."""

    @classmethod
    def setUpClass(cls):
        rep = {"costo": 15000, "por_origen": {"bodega": 10000, "compra": 5000, "manual": 0}, "n_sin_costo": 0}
        cls.fins = {
            "normal": FIN(rep=rep, **NORMAL),
            "ot201": FIN(**OT201),
            "interno": FIN(cliente_id=None, costo=40000),
            "contrato": FIN(tipo="preventiva", contrato_real=1, costo=60000, costo_proveedor=30000),
            "falta_tecnico": FIN(zz_monto=100000, valor_origen="zz"),
            "sin_despacho": FIN(zz_monto=100000, valor_origen="zz", costo_proveedor=50000),
        }
        cls.t = _amb()["_ot_fin_agregado"](list(cls.fins.values()))

    def test_suma_lo_que_dice_cada_ot(self):
        f, t = self.fins, self.t
        dentro = [f["normal"], f["ot201"], f["sin_despacho"]]
        self.assertEqual(t["cobre"], sum(x["cobre"]["total"] for x in dentro))
        self.assertEqual(t["me_cobraron"], sum(x["me_cobraron"]["total"] for x in dentro))
        self.assertEqual(t["queda"], round(t["cobre"] - t["me_cobraron"], 2))
        self.assertEqual(t["n"], 3)

    def test_numeros_fijos(self):
        t = self.t
        self.assertEqual(t["cobre"], 280000)            # 180.000 + 0 (garantía) + 100.000
        self.assertEqual(t["me_cobraron"], 385000)      # 135.000 + 200.000 + 50.000
        self.assertEqual(t["queda"], -105000)
        self.assertEqual(t["clase"], "rojo")
        self.assertEqual(t["tecnico"], 170000)          # técnico + despacho de las que se cobran
        self.assertEqual((t["rep_bodega"], t["rep_compra"]), (10000, 5000))
        self.assertEqual((t["n_no_cobra"], t["costo_no_cobra"]), (1, 200000))

    def test_lo_que_no_se_puede_medir_va_aparte(self):
        self.assertEqual((self.t["n_fuera"], self.t["cobre_fuera"]), (1, 100000))

    def test_interno_y_contrato_aparte(self):
        ap = self.t["aparte"]
        self.assertEqual(ap["interno"]["n"], 1)
        self.assertEqual((ap["contrato"]["n"], ap["contrato"]["costo"]), (1, 30000))

    def test_vacio(self):
        t = _amb()["_ot_fin_agregado"]([])
        self.assertEqual((t["cobre"], t["queda"], t["pct"], t["clase"]), (0, 0, None, "sin_dato"))

    def test_verde_con_ot_sin_medir_baja_a_ambar(self):
        t = _amb()["_ot_fin_agregado"]([FIN(**NORMAL), FIN(zz_monto=100000, valor_origen="zz")])
        self.assertEqual(t["clase"], "bajo")

    def test_resumen_del_excel_cuadra(self):
        # Revisión 2026-10-07 (Excel del panel, ot2_reporte_xlsx): los costos del Resumen se suman solo de las
        # OT que entran al margen (no contrato/interno, y que se pueden medir). Con ese criterio
        # Cobrado − Proveedor − Despacho − Repuestos = Margen (Queda).
        a = _amb()
        filas = [dict(NORMAL), dict(OT201), dict(cliente_id=None, costo=40000),
                 dict(tipo="preventiva", contrato_real=1, costo=60000, costo_proveedor=30000),
                 dict(zz_monto=100000, valor_origen="zz"),
                 dict(zz_monto=100000, valor_origen="zz", costo_proveedor=50000)]
        rep = {"costo": 15000, "por_origen": {"bodega": 15000, "compra": 0, "manual": 0}, "n_sin_costo": 0}
        prov = desp = reps = 0.0
        fins = []
        for i, kw in enumerate(filas):
            v = V(**kw)
            fin = a["_ot_finanzas"](v, rep if i == 0 else None)
            fins.append(fin)
            if fin["cobertura"] not in a["_OT_FIN_AGG_APARTE"] and fin["queda"]["mostrar"]:
                prov += float(v.get("costo_proveedor") or 0)
                desp += float(v.get("costo_despacho") or 0)
                reps += float(fin["me_cobraron"]["repuestos"] or 0)
        t = a["_ot_fin_agregado"](fins)
        self.assertEqual(round(t["cobre"] - prov - desp - reps, 2), t["queda"])
        self.assertEqual(t["queda"], -105000)   # contrato (-30.000) e interno NO restan

    def test_excel_del_panel_suma_como_el_agregado(self):
        src = _fuente_de("ot2_reporte_xlsx")
        self.assertIn("_ot_fin_agregado(_fins_res)", src)
        self.assertIn("_OT_FIN_AGG_APARTE", src)
        self.assertIn("Cobrado en OT sin medir", src)
        # La Queda de una OT que no se cobra se pinta azul ("nos costó"), no roja.
        self.assertRegex(src, r'_COL_MARGEN and not _fin\["cobra"\]:\s*\n(.*\n){0,3}.*BLUEL')
        for viejo in ('fin_tot["margen"] += margen', 'fin_tot["no_sstt_margen"] += margen',
                      'a["margen"] += margen', 'b["margen"] += margen', 't["margen"] += margen'):
            self.assertNotIn(viejo, src)


class TestFinanzasDelCliente(unittest.TestCase):
    """mant_cliente_finanzas (revisión 2026-10-07): el contrato se cuenta con el MISMO criterio que la bandera de
    contrato real que deja la mantención de contrato en Cobré $0."""

    def _correr(self, visitas, contratos, meses="12"):
        import datetime as _dt
        from types import SimpleNamespace
        a = _amb()
        llamadas = []

        def _fake(sql, params=None):
            llamadas.append((sql, params))
            if "FROM mant_repuestos" in sql:
                return []
            if "FROM mant_visitas" in sql:
                return [dict(x) for x in visitas]
            if "FROM mant_contratos" in sql and "monto_mensual" in sql:
                return [dict(x) for x in contratos]
            return []

        viejo = {k: a.get(k) for k in ("mysql_fetchall", "request", "jsonify", "_es_rol_tecnico", "datetime",
                                       "timedelta")}
        a.update({"mysql_fetchall": _fake, "request": SimpleNamespace(args={"meses": meses}),
                  "jsonify": lambda d: d, "_es_rol_tecnico": lambda: False, "datetime": _dt.datetime,
                  "timedelta": _dt.timedelta})
        try:
            out = a["mant_cliente_finanzas"](5)
        finally:
            for k, val in viejo.items():
                if val is None:
                    a.pop(k, None)
                else:
                    a[k] = val
        # Ojo: la consulta de visitas también nombra mant_contratos (la bandera contrato_real va en un EXISTS);
        # la de contratos es la que trae monto_mensual.
        sql_ctr = [x for x in llamadas if "FROM mant_contratos" in x[0] and "monto_mensual" in x[0]]
        self.assertEqual(len(sql_ctr), 1)
        return out, sql_ctr[0]

    @staticmethod
    def _hace_meses(n):
        import datetime as _dt
        hoy = _dt.date.today()
        idx = hoy.year * 12 + hoy.month - 1 - n
        return _dt.date(idx // 12, idx % 12 + 1, 1)

    def _prev(self, vid, **kw):
        import datetime as _dt
        base = V(id=vid, tipo="preventiva", contrato_real=1, costo=50000, costo_proveedor=30000)
        base["fecha_programada"] = _dt.date.today()
        base.update(kw)
        return base

    def test_contrato_indefinido_entra_y_la_preventiva_no_se_cuenta_dos_veces(self):
        ctr = [{"id": 3, "estado": "indefinido", "monto_mensual": 100000, "fecha_inicio": self._hace_meses(6),
                "fecha_vencimiento": None, "es_indefinido": 1}]
        out, (sql, params) = self._correr([self._prev(1), self._prev(2)], ctr)
        self.assertIn("estado IN ('vigente','por_vencer','indefinido')", sql)
        self.assertNotIn("estado='vigente'", sql)
        self.assertIn("Contenedor de documentos", sql)
        self.assertEqual(params, (5,), "sin mantenciones que apunten a un contrato, no se pide ninguno extra")
        self.assertEqual(out["contrato_estimado"], 600000)
        self.assertEqual(out["visitas_costo"], 0, "la preventiva de contrato tiene Cobré $0")
        self.assertEqual(out["visitas_costo_tecnicos"], 60000)
        self.assertEqual(out["ingresos_total"], 600000)
        self.assertEqual((out["contrato_mantenciones"], out["contrato_mantenciones_sin_monto"]), (2, 0))
        self.assertEqual(out["avisos"], [])

    def test_contrato_vencido_al_que_apunta_la_ot_cuenta_hasta_su_vencimiento(self):
        ctr = [{"id": 9, "estado": "vencido", "monto_mensual": 100000, "fecha_inicio": self._hace_meses(10),
                "fecha_vencimiento": self._hace_meses(2), "es_indefinido": 0}]
        out, (sql, params) = self._correr([self._prev(1, contrato_id=9)], ctr)
        self.assertIn(9, params, "el contrato vencido al que apunta la OT se pide")
        self.assertEqual(out["contrato_estimado"], 800000)   # 8 meses: del inicio al vencimiento

    def test_contrato_sin_monto_se_avisa(self):
        ctr = [{"id": 3, "estado": "vigente", "monto_mensual": 0, "fecha_inicio": self._hace_meses(6),
                "fecha_vencimiento": None, "es_indefinido": 0}]
        out, _ = self._correr([self._prev(1), self._prev(2)], ctr)
        self.assertEqual(out["contrato_mantenciones_sin_monto"], 2)
        self.assertTrue(out["avisos"])


class TestLote(unittest.TestCase):
    def test_completa_contrato_real_y_repuestos_en_una_pasada(self):
        a = _amb()
        filas = [V(id=7, tipo="preventiva", costo=60000, costo_proveedor=30000),
                 V(id=3, **NORMAL)]
        for f in filas:
            f.pop("contrato_real")
        a["_CONSULTAS"].clear()
        out = a["_ot_fin_lote"](filas)
        self.assertEqual(len(a["_CONSULTAS"]), 1, "una sola consulta para la bandera de contrato")
        self.assertEqual(out[7]["cobertura"], "contrato")
        self.assertEqual(out[3]["me_cobraron"]["repuestos"], 15000)
        self.assertEqual(out[3]["queda"]["total"], 45000)

    def test_columnas_del_select(self):
        sql = _amb()["_ot_fin_cols_sql"]("v")
        for c in ("v.zz_monto", "v.zz_codigo", "v.valor_origen", "v.valorizado_clp", "v.proveedor_tipo"):
            self.assertIn(c, sql)
        self.assertIn("AS contrato_real", sql)
        with self.assertRaises(ValueError):
            _amb()["_ot_fin_cols_sql"]("v; DROP")

    def test_resumen_del_monitor(self):
        r = _amb()["_ot_tv_fin_resumen"](FIN(**OT201))
        self.assertEqual((r["cobra"], r["cobertura_corta"], r["me_cobraron"]), (False, "Garantía", 200000))
        self.assertIsNone(_amb()["_ot_tv_fin_resumen"](None))


class TestLectoresUsanLaCuentaUnica(unittest.TestCase):
    """Por texto fuente: cada lector pasa por la cuenta única y ya no tiene su fórmula propia."""

    LECTORES = ("mant_vida_cliente_api", "mant_dashboard_costos_tecnico", "_facprov_datos", "_mfp_fila_ot",
                "ot2_reporte_xlsx", "_ot_tv_datos", "_cliente_inteligencia", "mant_analytics_modalidad",
                "mant_analisis", "mant_cliente_finanzas", "mant_finanzas_servicios", "mant_facturas_proveedor_xlsx",
                "mant_visitas_api", "mant_plan_mejora", "mant_cliente_ai_analisis", "_mant_ficha_impl",
                "mant_ot_ficha")

    def test_todos_pasan_por_la_cuenta_unica(self):
        for nombre in self.LECTORES:
            src = _fuente_de(nombre)
            # _ot_cobro_facprov = _ot_fin_de_fila + el resguardo de Facturación sobre el «Precio al cliente».
            self.assertTrue(re.search(r"_ot_fin(_lote|_de_fila|anzas|_agregado)\(|_ot_cobro_facprov\(", src), nombre)

    def test_ninguno_suma_costo_ni_zz_en_sql(self):
        for nombre in self.LECTORES:
            # Sin comentarios: los comentarios explican la fórmula de antes ("antes era SUM(costo)").
            src = "\n".join(ln for ln in _fuente_de(nombre).splitlines() if not ln.strip().startswith("#"))
            src = src.replace(" ", "").upper()
            for prohibido in ("SUM(COSTO)", "SUM(COALESCE(COSTO", "SUM(CASEWHENCOALESCE(ZZ_MONTO",
                              "SUM(COALESCE(ZZ_ENVIO_MONTO", "SUM(COALESCE(COSTO_PROVEEDOR"):
                self.assertNotIn(prohibido, src, f"{nombre}: {prohibido}")

    def test_vida_ya_no_usa_la_cuenta_de_antes(self):
        src = _fuente_de("mant_vida_cliente_api")
        self.assertNotIn("_ot_resultado_financiero(", src)
        self.assertIn("_ot_fin_agregado(", src)

    def test_costos_por_tecnico_sin_canceladas(self):
        src = _fuente_de("mant_dashboard_costos_tecnico")
        self.assertIn('("cancelada", "anulada")', src)

    def test_lo_pagado_al_proveedor_sigue_sin_repuestos(self):
        for nombre in ("_mfp_fila_ot", "_facprov_datos"):
            src = _fuente_de(nombre)
            # 2026-10-07: lo pagado ya no es una fórmula propia: es el a_pagar_proveedor del motor (sin repuestos).
            self.assertIn('_fin["a_pagar_proveedor"]', src, nombre)
            self.assertNotIn("_ot_repuestos_desglose", src, nombre)

    def test_finanzas_servicios_bloquea_al_tecnico(self):
        codigo, arbol = _codigo_y_arbol()
        for nodo in ast.walk(arbol):
            if isinstance(nodo, ast.FunctionDef) and nodo.name == "mant_finanzas_servicios":
                decos = [ast.unparse(d) for d in nodo.decorator_list]
                self.assertIn("_no_tecnico", decos)
                return
        self.fail("no está mant_finanzas_servicios")

    def test_monitor_y_calendario_no_le_mandan_montos_al_tecnico(self):
        self.assertIn("if incluir_finanzas else None", _fuente_de("_ot_tv_datos").split('"fin":')[1][:200])
        self.assertIn("not _es_rol_tecnico()", _fuente_de("mant_visitas_api"))
        self.assertIn("not _es_rol_tecnico()", _fuente_de("mant_ot_ficha"))

    def test_lo_que_ve_el_cliente_no_cambio(self):
        # REGLAS #22/#23: el PDF público sigue decidiendo "Datos para transferencia" con modalidad_cobro
        # (unificarlo cambiaría lo que ve el cliente: queda para que Daniel lo decida).
        import os
        raiz = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        with open(os.path.join(raiz, "templates", "mantenciones", "ot_pdf_levantamiento.html"), encoding="utf-8") as fh:
            self.assertIn("_mod_cobro not in ('garantia', 'sin_costo', 'interno')", fh.read())


if __name__ == "__main__":
    unittest.main()

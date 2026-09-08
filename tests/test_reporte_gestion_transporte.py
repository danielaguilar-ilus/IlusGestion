"""Reporte de gestión para gerencia de logística (2026-09-08).

Pedido de Daniel, relayando a Alison: "reporte de pedidos en términos de
entrega, en términos de valor, filtrando fechas o semana o mes y además
por courriers... algo atómico... considerando la capacidad de
reportabilidad además de exportar un excel con calidad de datos para
realizar cálculos, reportes y seguimientos".

En vez de una página nueva, se extendió logistica_kpi.py -- el módulo que
YA calculaba fill rate / tasa de entrega / tasa de incidencias / lead time
para el Dashboard de Transporte, con la filosofía explícita en su propio
docstring: "si el día de mañana se define [un KPI nuevo], el lugar
natural es _calcular_kpis() más abajo -- no duplicar la página". Se
agregó ahí mismo el KPI de VALOR (cobrado/costo/margen), un preset
"semana", filtro por courier en los 4 KPIs existentes, y un exportador de
Excel (_listar_compromisos_export) que reusa el MISMO filtro que ya
usaba la tabla en pantalla (_compromisos_where, extraído para no
duplicarlo).

Este archivo testea logistica_kpi.py como módulo Python real (no vive
embebido en app.py, así que no hace falta el patrón ast.unparse+exec que
usan las demás suites de este proyecto) -- se importa directo y se
mockea sys.modules['app'] con mysql_fetchone/mysql_fetchall falsos.

Correr con:  py -m unittest tests.test_reporte_gestion_transporte -v
"""
import sys
import types
import unittest
from datetime import date, datetime, timedelta

import logistica_kpi as lk


class _FakeApp:
    """Reemplaza sys.modules['app'] para que _h('mysql_fetchone') etc.
    resuelvan a estos dobles en vez de tocar MySQL de verdad."""
    def __init__(self, fetchone_seq=None, fetchall_seq=None):
        self._fetchone_seq = list(fetchone_seq or [])
        self._fetchall_seq = list(fetchall_seq or [])
        self.fetchone_calls = []
        self.fetchall_calls = []

    def mysql_fetchone(self, sql, params=()):
        self.fetchone_calls.append((sql, params))
        return self._fetchone_seq.pop(0) if self._fetchone_seq else {}

    def mysql_fetchall(self, sql, params=()):
        self.fetchall_calls.append((sql, params))
        return self._fetchall_seq.pop(0) if self._fetchall_seq else []


class _ConModulo(unittest.TestCase):
    """Instala un app falso en sys.modules['app'] para la duración del test."""

    def _instalar(self, fetchone_seq=None, fetchall_seq=None):
        fake = _FakeApp(fetchone_seq, fetchall_seq)
        mod = types.ModuleType("app")
        mod.mysql_fetchone = fake.mysql_fetchone
        mod.mysql_fetchall = fake.mysql_fetchall
        self._orig = sys.modules.get("app")
        sys.modules["app"] = mod
        self.fake = fake
        return fake

    def tearDown(self):
        if getattr(self, "_orig", None) is not None:
            sys.modules["app"] = self._orig
        else:
            sys.modules.pop("app", None)


class TestRangoFechasSemana(unittest.TestCase):
    """_rango_fechas(): el preset 'semana' pedido por Daniel."""

    def test_semana_es_ventana_movil_de_7_dias(self):
        import flask
        app = flask.Flask(__name__)
        with app.test_request_context("/?periodo=semana"):
            periodo, desde, hasta = lk._rango_fechas()
        hoy = date.today()
        self.assertEqual(periodo, "semana")
        self.assertEqual(hasta, hoy.strftime("%Y-%m-%d"))
        self.assertEqual(desde, (hoy - timedelta(days=7)).strftime("%Y-%m-%d"))

    def test_semana_no_rompe_los_presets_existentes(self):
        import flask
        app = flask.Flask(__name__)
        with app.test_request_context("/?periodo=mes"):
            periodo, desde, hasta = lk._rango_fechas()
        self.assertEqual(periodo, "mes")


class TestCourierSubfiltro(unittest.TestCase):
    """_courier_subfiltro(): fragmento reusado por los 5 KPIs."""

    def test_sin_courier_no_agrega_nada(self):
        sql, params = lk._courier_subfiltro("")
        self.assertEqual(sql, "")
        self.assertEqual(params, ())

    def test_con_courier_filtra_por_manifiesto(self):
        sql, params = lk._courier_subfiltro("Transporte Felca", alias="c")
        self.assertIn("c.id IN", sql)
        self.assertIn("transport_manifest_items", sql)
        self.assertIn("transport_manifests", sql)
        self.assertIn("m3.courier = %s", sql)
        self.assertEqual(params, ("Transporte Felca",))

    def test_usa_el_alias_correcto(self):
        sql, _ = lk._courier_subfiltro("FedEx", alias="xyz")
        self.assertIn("xyz.id IN", sql)


class TestCalcularKpisValor(_ConModulo):
    """_calcular_kpis(): el KPI de valor nuevo (cobrado/costo/margen)."""

    def test_calcula_cobrado_costo_margen(self):
        # Orden real de llamadas dentro de _calcular_kpis: fill_rate, tasa
        # entrega/incidencias, lead_time, valor (ver definicion del cuerpo).
        self._instalar(fetchone_seq=[
            {"despachado": 80, "pedido": 100},                       # 1) fill rate
            {"total": 10, "entregados": 7, "incidencias": 1},         # 2/3) entrega/incidencias
            {"lead_avg": 2.5, "n": 5},                                 # 4) lead time
            {"cobrado": 500000, "costo": 350000, "docs": 4},           # 5) valor
        ])
        kpis = lk._calcular_kpis("2026-09-01", "2026-09-08")
        v = kpis["valor"]
        self.assertEqual(v["cobrado"], 500000)
        self.assertEqual(v["costo"], 350000)
        self.assertEqual(v["margen"], 150000)
        self.assertEqual(v["docs"], 4)
        self.assertAlmostEqual(v["margen_pct"], 30.0, places=1)

    def test_sin_cobros_margen_pct_es_none_no_explota(self):
        self._instalar(fetchone_seq=[
            {"despachado": 0, "pedido": 0},
            {"total": 0, "entregados": 0, "incidencias": 0},
            {"lead_avg": None, "n": 0},
            {"cobrado": 0, "costo": 0, "docs": 0},
        ])
        kpis = lk._calcular_kpis("2026-09-01", "2026-09-08")
        self.assertIsNone(kpis["valor"]["margen_pct"])
        self.assertEqual(kpis["valor"]["docs"], 0)

    def test_courier_se_propaga_a_las_queries(self):
        fake = self._instalar(fetchone_seq=[
            {"despachado": 0, "pedido": 0},
            {"total": 0, "entregados": 0, "incidencias": 0},
            {"lead_avg": None, "n": 0},
            {"cobrado": 0, "costo": 0, "docs": 0},
        ])
        lk._calcular_kpis("2026-09-01", "2026-09-08", courier="FedEx")
        # Las 4 llamadas deben llevar 'FedEx' en algun lugar de sus params.
        todas_params = [p for _, p in fake.fetchone_calls]
        self.assertTrue(any("FedEx" in p for p in todas_params),
                         f"ningun call llevo el courier en sus params: {todas_params}")

    def test_conserva_los_4_kpis_originales(self):
        self._instalar(fetchone_seq=[
            {"despachado": 80, "pedido": 100},
            {"total": 10, "entregados": 7, "incidencias": 1},
            {"lead_avg": 2.5, "n": 5},
            {"cobrado": 0, "costo": 0, "docs": 0},
        ])
        kpis = lk._calcular_kpis("2026-09-01", "2026-09-08")
        self.assertEqual(kpis["fill_rate"]["valor"], 80.0)
        self.assertEqual(kpis["tasa_entrega"]["valor"], 70.0)
        self.assertEqual(kpis["tasa_incidencias"]["valor"], 10.0)
        self.assertEqual(kpis["lead_time"]["dias"], 2.5)


class TestCompromisosWhereCompartido(unittest.TestCase):
    """_compromisos_where(): una sola definición para pantalla y Excel."""

    def test_sin_filtros_es_1_igual_1(self):
        where_sql, params, base_from = lk._compromisos_where("", "", "", "", "", "")
        self.assertEqual(where_sql, "1=1")
        self.assertEqual(params, [])
        self.assertIn("transport_commitments c", base_from)

    def test_q_prevalece_sobre_los_demas_filtros(self):
        # Mismo comportamiento que antes de extraer la función: q busca
        # libre y NO se combina con desde/hasta/estado/comuna/courier.
        where_sql, params, _ = lk._compromisos_where(
            "2026-01-01", "2026-01-31", "Pendiente", "FedEx", "Providencia", "11234")
        self.assertNotIn("fecha_emision", where_sql)
        self.assertNotIn("estado", where_sql)

    def test_filtros_normales_se_combinan_con_and(self):
        where_sql, params, _ = lk._compromisos_where(
            "2026-01-01", "2026-01-31", "Pendiente", "FedEx", "", "")
        self.assertIn("c.fecha_emision >= %s", where_sql)
        self.assertIn("c.fecha_emision <= %s", where_sql)
        self.assertIn("c.estado = %s", where_sql)
        self.assertIn("m2.courier = %s", where_sql)
        self.assertEqual(params, ["2026-01-01", "2026-01-31", "Pendiente", "FedEx"])


class TestListarCompromisosExport(_ConModulo):
    """_listar_compromisos_export(): fila por documento para el Excel."""

    def _fila_base(self, **over):
        base = {
            "id": 1, "tido": "BLV", "nudo": "00012345", "cliente_nombre": "Juan Pérez",
            "comuna": "Providencia", "estado": "Pendiente",
            "fecha_emision": date(2026, 9, 1), "fecha_agenda": None, "delivered_at": None,
            "cobrado": 100000, "costo": 70000,
            "courier": "FedEx", "estado_entrega": "Entregado",
            "manifiesto_correlativo": "MAN-2026-0010", "fecha_manifiesto": date(2026, 9, 1),
            "cant_comprada": 100.0, "cant_despachada": 80.0,
            "en_courier_at": datetime(2026, 9, 1, 10, 0),
            "entregado_at": datetime(2026, 9, 3, 10, 0),
        }
        base.update(over)
        return base

    def test_calcula_margen_fill_rate_y_dias_transito(self):
        self._instalar(
            fetchone_seq=[{"n": 1}],
            fetchall_seq=[[self._fila_base()]],
        )
        filas, truncado = lk._listar_compromisos_export("2026-09-01", "2026-09-08", "", "", "", "")
        self.assertFalse(truncado)
        self.assertEqual(len(filas), 1)
        f = filas[0]
        self.assertEqual(f["margen"], 30000)
        self.assertEqual(f["fill_rate_pct"], 80.0)
        self.assertEqual(f["dias_transito"], 2.0)
        self.assertEqual(f["nudo_display"], "12345")  # sin ceros a la izquierda

    def test_sin_fechas_de_evento_dias_transito_es_none(self):
        self._instalar(
            fetchone_seq=[{"n": 1}],
            fetchall_seq=[[self._fila_base(en_courier_at=None, entregado_at=None)]],
        )
        filas, _ = lk._listar_compromisos_export("2026-09-01", "2026-09-08", "", "", "", "")
        self.assertIsNone(filas[0]["dias_transito"])

    def test_sin_cantidad_comprada_fill_rate_es_none_no_zero_division(self):
        self._instalar(
            fetchone_seq=[{"n": 1}],
            fetchall_seq=[[self._fila_base(cant_comprada=0.0, cant_despachada=0.0)]],
        )
        filas, _ = lk._listar_compromisos_export("2026-09-01", "2026-09-08", "", "", "", "")
        self.assertIsNone(filas[0]["fill_rate_pct"])

    def test_marca_truncado_cuando_hay_mas_que_el_tope(self):
        self._instalar(
            fetchone_seq=[{"n": lk._EXPORT_MAX_FILAS + 1}],
            fetchall_seq=[[self._fila_base()]],
        )
        _, truncado = lk._listar_compromisos_export("2026-09-01", "2026-09-08", "", "", "", "")
        self.assertTrue(truncado)

    def test_no_truncado_cuando_calza_exacto_en_el_tope(self):
        self._instalar(
            fetchone_seq=[{"n": lk._EXPORT_MAX_FILAS}],
            fetchall_seq=[[self._fila_base()]],
        )
        _, truncado = lk._listar_compromisos_export("2026-09-01", "2026-09-08", "", "", "", "")
        self.assertFalse(truncado)

    def test_respeta_el_limite_de_filas_en_la_query(self):
        fake = self._instalar(fetchone_seq=[{"n": 1}], fetchall_seq=[[]])
        lk._listar_compromisos_export("2026-09-01", "2026-09-08", "", "", "", "")
        sql, params = fake.fetchall_calls[0]
        self.assertIn("LIMIT %s", sql)
        self.assertNotIn("OFFSET", sql)  # el export nunca pagina
        self.assertEqual(params[-1], lk._EXPORT_MAX_FILAS)


class TestEndpointYTemplateExcel(unittest.TestCase):
    """El endpoint real reusa las mismas funciones (no reimplementa el
    filtro ni el cálculo) y el botón vive en el Dashboard, no en una
    página aparte -- 'algo atómico', no una pantalla nueva por request."""

    def setUp(self):
        with open("app.py", encoding="utf-8", errors="ignore") as f:
            self.app_src = f.read()
        with open("templates/transporte/dashboard.html", encoding="utf-8", errors="ignore") as f:
            self.dash_html = f.read()

    def test_endpoint_existe_bajo_tr_required(self):
        i = self.app_src.find("def tr_dashboard_export_xlsx")
        self.assertGreater(i, 0)
        cab = self.app_src[max(0, i - 200):i]
        self.assertIn("/transporte/dashboard/export.xlsx", cab)
        self.assertIn("@_tr_required", cab)

    def test_endpoint_reusa_las_funciones_del_modulo_no_las_duplica(self):
        i = self.app_src.find("def tr_dashboard_export_xlsx")
        bloque = self.app_src[i:i + 3000]
        self.assertIn("_listar_compromisos_export", bloque)
        self.assertIn("_calcular_kpis", bloque)
        self.assertIn("_rango_fechas", bloque)

    def test_boton_en_el_dashboard_manda_los_mismos_filtros(self):
        i = self.dash_html.find("Exportar Excel")
        self.assertGreater(i, 0)
        bloque = self.dash_html[max(0, i - 500):i]
        self.assertIn("tr_dashboard_export_xlsx", bloque)
        self.assertIn("hist_filtros.courier", bloque)
        self.assertIn("hist_filtros.desde", bloque)

    def test_chip_esta_semana_existe(self):
        self.assertIn("periodo='semana'", self.dash_html)
        self.assertIn("Esta semana", self.dash_html)


if __name__ == "__main__":
    unittest.main(verbosity=2)

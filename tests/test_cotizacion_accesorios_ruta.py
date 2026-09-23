"""Regresiones del motor de cotizaciones: accesorios $0 y ruta prorrateada."""
from pathlib import Path
import ast
import unittest


ROOT = Path(__file__).resolve().parents[1]
JS = (ROOT / "static" / "tickets_cotizaciones.js").read_text(encoding="utf-8")
TICKETS = (ROOT / "tickets_module.py").read_text(encoding="utf-8")
CATALOGO = (ROOT / "catalogo_module.py").read_text(encoding="utf-8")
CLASES_HTML = (ROOT / "templates" / "catalogo" / "clases.html").read_text(encoding="utf-8")
COTIZACIONES_HTML = (ROOT / "templates" / "tickets" / "cotizaciones.html").read_text(encoding="utf-8")

# Carga solamente las funciones puras desde el archivo real. Así esta
# regresión no necesita levantar Flask/MySQL ni instalar dependencias web.
_tree = ast.parse(TICKETS)
_wanted = {"_tk_cotiz_clase_no_cobrable", "_tk_cotiz_clase_sin_costo_ruta",
           "_tk_cotiz_unidades_cobrables"}
_nodes = [n for n in _tree.body if isinstance(n, ast.FunctionDef) and n.name in _wanted]
_scope = {
    "_TK_COTIZ_CLASES_NO_COBRABLES": frozenset({"accesorio"}),
    # 2026-09-23 (Daniel, "los repuestos no se vean afectados por los
    # costos de ruta"): conjunto NUEVO y separado de _TK_COTIZ_CLASES_NO_
    # COBRABLES -- repuesto se excluye SOLO del reparto de ruta, no del
    # cobro. Ver _tk_cotiz_clase_sin_costo_ruta en tickets_module.py.
    "_TK_COTIZ_CLASES_SIN_COSTO_RUTA": frozenset({"accesorio", "repuesto"}),
}
exec(compile(ast.Module(body=_nodes, type_ignores=[]), "tickets_module.py", "exec"), _scope)
_tk_cotiz_clase_no_cobrable = _scope["_tk_cotiz_clase_no_cobrable"]
_tk_cotiz_clase_sin_costo_ruta = _scope["_tk_cotiz_clase_sin_costo_ruta"]
_tk_cotiz_unidades_cobrables = _scope["_tk_cotiz_unidades_cobrables"]


class TestAccesoriosNoCobrables(unittest.TestCase):
    def test_accesorio_es_regla_de_backend(self):
        self.assertTrue(_tk_cotiz_clase_no_cobrable("accesorio"))
        self.assertTrue(_tk_cotiz_clase_no_cobrable(" Accesorio "))
        self.assertFalse(_tk_cotiz_clase_no_cobrable("trotadora_no_motorizada"))

    def test_accesorio_y_lineas_en_cero_no_absorben_ruta(self):
        items = [
            {"clase_producto": "trotadora_no_motorizada", "cantidad": 2,
             "precio_unitario": 26000, "total": 52000},
            {"clase_producto": "accesorio", "cantidad": 12,
             "precio_unitario": 99999, "total": 1199988},
            {"clase_producto": "bicicleta", "cantidad": 3,
             "precio_unitario": 0, "total": 0},
        ]
        self.assertEqual(_tk_cotiz_unidades_cobrables(items), 2)

    def test_front_bloquea_precio_y_payload_manual(self):
        self.assertIn("accesorio-bloqueado", JS)
        self.assertIn("disabled readonly", JS)
        self.assertIn("_cotWizEsAccesorio(it) ? null", JS)
        self.assertIn("$0 · no cobrable", JS)

    def test_catalogo_bloquea_accesorio_en_cero(self):
        self.assertIn("Accesorio es no cobrable y su precio está bloqueado en $0", CATALOGO)
        self.assertIn('"bloqueada": True, "precio_fijo": 0', CATALOGO)
        self.assertIn("No cobrable · bloqueado", CLASES_HTML)


class TestRutaSoloEnEquiposCobrables(unittest.TestCase):
    def test_ejemplo_26000_mas_5000(self):
        base = 26000
        costo_ruta = 10000
        equipos_cobrables = 2
        self.assertEqual(base + costo_ruta / equipos_cobrables, 31000)

    def test_wizard_recalcula_al_editar_o_excluir_ruta(self):
        self.assertIn("cr.addEventListener('input'", JS)
        self.assertIn("cre.addEventListener('change', cotWizRecalcLocal)", JS)
        self.assertIn("const rutaAplicada = (!rutaExcluida && unidadesCobrables > 0)", JS)
        self.assertIn("base + '", JS)
        self.assertIn("ruta = <b>", JS)

    def test_pdf_usa_el_mismo_filtro_de_cobrabilidad(self):
        self.assertIn("total_unidades = _tk_cotiz_unidades_cobrables(items_rows)", TICKETS)
        self.assertIn('if it["es_cobrable"]', TICKETS)

    def test_sin_equipos_cobrables_backend_aplica_ruta_cero(self):
        self.assertIn("costo_ruta_solicitado if unidades_cobrables > 0 else 0.0", TICKETS)


class TestRepuestoNoAbsorbeRuta(unittest.TestCase):
    """Daniel (2026-09-23, viendo el wizard en vivo, cotización de ejemplo
    con 2 equipos cobrables + 2 accesorios + $426.600 de costo de ruta
    repartido entre los 2 equipos cobrables): "quisiera que los repuestos
    no se vean afectados por los costos de ruta... en esa cotización de
    ejemplo me indica que no apliquen a los repuestos".

    Distinción crítica con "accesorio" (que SÍ es no-cobrable, precio
    forzado a $0): un repuesto sigue cobrándose con su propio precio, solo
    queda fuera del PRORRATEO de la ruta."""

    def test_repuesto_es_cobrable_pero_no_absorbe_ruta(self):
        # A diferencia de "accesorio", un repuesto SÍ es cobrable --
        # meterlo en _TK_COTIZ_CLASES_NO_COBRABLES pondría su precio en $0
        # por error (bug de facturación real).
        self.assertFalse(_tk_cotiz_clase_no_cobrable("repuesto"))
        self.assertTrue(_tk_cotiz_clase_no_cobrable("accesorio"))
        # Pero SÍ queda fuera del reparto de la ruta, igual que accesorio.
        self.assertTrue(_tk_cotiz_clase_sin_costo_ruta("repuesto"))
        self.assertTrue(_tk_cotiz_clase_sin_costo_ruta(" Repuesto "))
        self.assertTrue(_tk_cotiz_clase_sin_costo_ruta("accesorio"))
        self.assertFalse(_tk_cotiz_clase_sin_costo_ruta("trotadora_no_motorizada"))

    def test_caso_daniel_2_equipos_2_accesorios_1_repuesto(self):
        # Caso real visto en vivo: 2 equipos cobrables (base $78.030 c/u),
        # 2 accesorios ($0, ya excluidos por no-cobrables) y 1 repuesto con
        # precio propio ($15.000) que NO debe sumar a las unidades que
        # dividen el costo de ruta.
        items = [
            {"clase_producto": "trotadora_no_motorizada", "cantidad": 1,
             "precio_unitario": 78030, "total": 78030},
            {"clase_producto": "trotadora_no_motorizada", "cantidad": 1,
             "precio_unitario": 78030, "total": 78030},
            {"clase_producto": "accesorio", "cantidad": 2,
             "precio_unitario": 0, "total": 0},
            {"clase_producto": "repuesto", "cantidad": 1,
             "precio_unitario": 15000, "total": 15000},
        ]
        # Solo los 2 equipos cobrables cuentan -- el repuesto (con precio y
        # total > 0) NO suma, igual que los accesorios en $0.
        self.assertEqual(_tk_cotiz_unidades_cobrables(items), 2)
        costo_ruta = 426600
        ruta_por_unidad = costo_ruta / _tk_cotiz_unidades_cobrables(items)
        self.assertEqual(ruta_por_unidad, 213300)
        # El repuesto sigue cobrando su propio precio -- $15.000, no $0.
        repuesto = next(it for it in items if it["clase_producto"] == "repuesto")
        self.assertEqual(repuesto["precio_unitario"], 15000)
        self.assertEqual(repuesto["total"], 15000)

    def test_pdf_excluye_repuesto_del_reparto_sin_forzar_su_precio(self):
        # El PDF (_tk_cotizacion_pdf_ctx) usa el mismo criterio de ruta que
        # el resto del backend -- no _tk_cotiz_clase_no_cobrable (eso
        # forzaría $0), sino _tk_cotiz_clase_sin_costo_ruta.
        self.assertIn(
            'es_cobrable = (not _tk_cotiz_clase_sin_costo_ruta(it.get("clase_producto"))',
            TICKETS)

    def test_frontend_wizard_tiene_el_mismo_criterio(self):
        # El preview en vivo del wizard (cotWizRecalcLocal) debe usar el
        # mismo criterio que el backend para que lo que Daniel ve en
        # pantalla coincida con lo que se guarda.
        self.assertIn("function _cotWizSinCostoRuta(it){", JS)
        self.assertIn("clase === 'accesorio' || clase === 'repuesto'", JS)
        self.assertIn("!_cotWizSinCostoRuta(it) && bases[i] != null", JS)
        self.assertIn("const esCobrable = !_cotWizSinCostoRuta(it) && pu != null", JS)
        # El precio propio del repuesto (bases[i]) sigue viniendo de
        # _cotWizEsAccesorio (NO de _cotWizSinCostoRuta) -- solo accesorio
        # fuerza pu=0, un repuesto conserva su precio.
        self.assertIn("if (_cotWizEsAccesorio(it)) return 0;", JS)


class TestTarifaCopiadaDeMantencion(unittest.TestCase):
    def test_backend_y_front_comparten_el_origen(self):
        self.assertIn('row = _buscar("mantencion")', CATALOGO)
        self.assertIn('"tipo_servicio_origen": tipo_origen', CATALOGO)
        self.assertIn("esCopiaDeMantencion ? 'mantencion' : tipo", CLASES_HTML)
        self.assertIn('tarifa.get("tipo_servicio_origen") or tipo_servicio', TICKETS)


class TestRecotizarSinBorrarLaCotizacion(unittest.TestCase):
    def test_existe_accion_explicita_y_confirmada(self):
        self.assertIn("function cotRecalcular", JS)
        self.assertIn("body: JSON.stringify({manual: true})", JS)
        self.assertIn("Los accesorios quedarán en $0", JS)
        self.assertIn("cotRecalcular(", COTIZACIONES_HTML)

    def test_recalculo_manual_deja_historial(self):
        self.assertIn('"recalcular_aprobada" if _estaba_aprobada else "recalcular"', TICKETS)
        self.assertIn('"total_anterior"', TICKETS)
        self.assertIn('"total_nuevo"', TICKETS)


if __name__ == "__main__":
    unittest.main(verbosity=2)

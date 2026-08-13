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

# Carga solamente las dos funciones puras desde el archivo real. Así esta
# regresión no necesita levantar Flask/MySQL ni instalar dependencias web.
_tree = ast.parse(TICKETS)
_wanted = {"_tk_cotiz_clase_no_cobrable", "_tk_cotiz_unidades_cobrables"}
_nodes = [n for n in _tree.body if isinstance(n, ast.FunctionDef) and n.name in _wanted]
_scope = {"_TK_COTIZ_CLASES_NO_COBRABLES": frozenset({"accesorio"})}
exec(compile(ast.Module(body=_nodes, type_ignores=[]), "tickets_module.py", "exec"), _scope)
_tk_cotiz_clase_no_cobrable = _scope["_tk_cotiz_clase_no_cobrable"]
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

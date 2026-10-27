"""«Margen %» propio de la cotización (Daniel 27-oct-2026: «requiero que podamos agregar 40% de margen» en el editor de cotización).

Qué se vigila:
  · Las cotizaciones que no tocan el margen NO cambian de precio: margen None = el global (tk_settings.cotiz_margen_pct, 40).
  · Solo el superadministrador lo cambia: de cualquier otro usuario el servidor lo ignora; vacío vuelve al global; fuera de 0–300 se ignora.
  · El precio del ítem es costo × (1 + margen) con el margen de la cotización (mismo cálculo de _tk_cotiz_calcular_item).
  · El cableado: columna NULL en el ALTER, guardado best-effort antes de recalcular, y el editor (campo, vista previa, guardado).
Sin BD ni Flask (mismo patrón de tests/test_descuento_garantia.py).  Correr con:  python -m pytest -q tests/test_cotizacion_margen.py
"""
import ast
import math
import os
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _leer(rel):
    with open(os.path.join(RAIZ, rel.replace("/", os.sep)), encoding="utf-8", newline="") as f:
        return f.read().replace("\r\n", "\n")


_SRC = _leer("tickets_module.py")
_ARB = ast.parse(_SRC)


def _fn(nombre, ambito=None):
    nodo = next(n for n in ast.walk(_ARB) if isinstance(n, ast.FunctionDef) and n.name == nombre)
    ns = dict(ambito or {})
    exec(compile(ast.Module(body=[nodo], type_ignores=[]), "<tickets_module>", "exec"), ns)
    return ns[nombre]


class TestMargenPropio(unittest.TestCase):
    def _entrada(self, superadmin=True):
        return _fn("_tk_margen_in", {"_tk_solo_superadmin": lambda: superadmin})

    def test_es_generico_lo_elige_quien_arma_la_cotizacion_no_solo_el_superadmin(self):
        for sa in (True, False):
            self.assertEqual(self._entrada(sa)({"margen_pct": 45}), (True, 45.0), "genérico para todos (Daniel 27-oct)")

    def test_vacio_vuelve_al_global_y_lo_invalido_se_ignora(self):
        f = self._entrada(True)
        self.assertEqual(f({"margen_pct": ""}), (True, None))
        self.assertEqual(f({"margen_pct": None}), (True, None))
        self.assertEqual(f({}), (False, None), "sin la clave no se toca nada")
        for malo in (-1, 301, "abc", float("nan")):
            self.assertEqual(f({"margen_pct": malo}), (False, None), malo)
        self.assertEqual(f({"margen_pct": "0"}), (True, 0.0), "0 % es un margen válido")
        self.assertEqual(f({"margen_pct": 33.333}), (True, 33.33))

    def test_sin_margen_propio_la_cotizacion_usa_el_global(self):
        cfg = {"valor_hh": {"mantencion": 20000.0}, "margen_pct": 40.0, "iva_pct": 19.0}
        f = _fn("_tk_cfg_margen")
        self.assertIs(f(cfg, None), cfg, "las cotizaciones viejas no cambian de precio")
        self.assertEqual(f(cfg, 55)["margen_pct"], 55.0)
        self.assertEqual(cfg["margen_pct"], 40.0, "no pisa la configuración global")
        self.assertIs(f(cfg, "x"), cfg)

    def test_el_precio_del_item_sigue_costo_por_uno_mas_margen(self):
        calc = _fn("_tk_cotiz_calcular_item", {
            "_tk_cotiz_clase_no_cobrable": lambda c: False,
            "_tk_money_round": lambda x: int(round(x)),
            "ctx": {}, "math": math})
        tarifa = {"horas": 1.0, "tecnicos": 1, "precio_fijo": None, "precio_piso": None, "tipo_servicio_origen": None}
        base = {"valor_hh": {"mantencion": 20000.0}, "iva_pct": 19.0}
        r40 = calc("trotadora", "mantencion", 1, 0, dict(base, margen_pct=40.0), tarifa=tarifa)
        r50 = calc("trotadora", "mantencion", 1, 0, dict(base, margen_pct=50.0), tarifa=tarifa)
        self.assertEqual(r40["precio_unitario"], 28000)
        self.assertEqual(r50["precio_unitario"], 30000)


class TestCableado(unittest.TestCase):
    def test_columna_nula_y_guardado_antes_de_recalcular(self):
        self.assertIn("ADD COLUMN margen_pct DECIMAL(6,2) NULL", _SRC)
        self.assertIn("_tk_margen_guardar(cot_id, *_tk_margen_in(d))", _SRC)
        i = _SRC.index("_tk_margen_guardar(cid, *_tk_margen_in(d))")
        self.assertLess(i, _SRC.index("totales = _tk_cotiz_recalcular(cid, user=user)", i), "el margen se guarda ANTES de recalcular")

    def test_recalculo_detalle_y_vista_previa_usan_el_margen_de_la_cotizacion(self):
        self.assertIn("_tk_cfg_margen(_tk_cotiz_pricing_config(), _tk_margen_de(cid))", _SRC)
        self.assertIn('_tk_cfg_margen(_tk_cotiz_pricing_config(), cot.get("margen_pct"))', _SRC)
        self.assertIn('"margen_global": _margen_global', _SRC)

    def test_el_editor_tiene_el_campo_para_todos_y_parte_en_30(self):
        html = _leer("templates/tickets/cotizaciones.html")
        self.assertIn('id="cotWizMargen"', html)
        campo = html[html.index('id="cotWizMargen"'):html.index('id="cotWizMargen"') + 260]
        self.assertIn('value="30"', campo)
        self.assertNotIn("disabled", campo, "genérico: nadie lo tiene deshabilitado")
        self.assertIn("_TK_COTIZ_MARGEN_DEFECTO = 30.0", _SRC)
        js0 = _leer("static/tickets_cotizaciones.js")
        self.assertIn("_WIZ.margenDefecto = 30", js0)
        js = _leer("static/tickets_cotizaciones.js")
        self.assertIn("function cotWizMargenCambio()", js)
        self.assertIn("_cotWizMargenPayload()", js)
        # solo lo agregado para el margen (el archivo trae otros avisos de antes que no son de esta función)
        propio = js[js.index("function _cotWizMargenPayload()"):js.index("let _wizPrecioSeq = 0;")]
        for nativo in ("alert(", "confirm(", "prompt("):
            self.assertNotIn(nativo, propio)


if __name__ == "__main__":
    unittest.main()

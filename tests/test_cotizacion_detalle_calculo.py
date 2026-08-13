"""El botón "Ver detalle de cálculo" (icono calculadora, solo superadmin) en
/transporte/cotizaciones estaba roto en dos niveles distintos:

1. BUG #1 (reportado por Daniel, 2026-08-13, en vivo): la ruta
   /transporte/cotizaciones/<id>/detalle-calculo directamente NO estaba
   registrada en app.py (sí existía en logistica_cotizaciones.py, pero el
   link abría en target="_blank" -- Daniel pidió que abra en el modal
   global de PDF (openPdf, ya usado por el botón vecino "Ver PDF") con su
   barra de progreso ILUS, y que el detalle muestre peso real/volumétrico/
   m³ para no tener que abrir el cubicador aparte.

2. BUG #2 (encontrado AL VERIFICAR el fix de arriba, no reportado por
   Daniel): la plantilla usaba `data.items` en vez de `data['items']`.
   Como `data` es un dict de Python y los dicts YA TIENEN un método
   `.items()` propio, Jinja2 resuelve `data.items` como ESE MÉTODO (no
   como la clave "items" del dict) -- TypeError real:
   "object of type 'builtin_function_or_method' has no len()".
   Este bug YA estaba en el render_template_string original (antes de
   este fix), así que el botón habría fallado con un 500 aunque la ruta
   hubiera estado registrada en app.py desde el principio.

Correr con:  py -m unittest tests.test_cotizacion_detalle_calculo -v
"""
import ast
import os
import sys
import unittest

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LC_SRC = open(os.path.join(BASE_DIR, "logistica_cotizaciones.py"),
              encoding="utf-8", errors="ignore").read()
_ARBOL = ast.parse(LC_SRC)

TPL_PATH = os.path.join(BASE_DIR, "templates", "transporte",
                         "cotizacion_detalle_calculo.html")
TPL_SRC = open(TPL_PATH, encoding="utf-8", errors="ignore").read()

COTIZ_LIST_PATH = os.path.join(BASE_DIR, "templates", "transporte", "cotizaciones.html")
COTIZ_LIST_SRC = open(COTIZ_LIST_PATH, encoding="utf-8", errors="ignore").read()


def _fn_source(nombre):
    for n in ast.walk(_ARBOL):
        if isinstance(n, ast.FunctionDef) and n.name == nombre:
            return ast.unparse(n)
    raise AssertionError(f"no se encontró la función {nombre} en logistica_cotizaciones.py")


class TestRutaRegistrada(unittest.TestCase):
    def test_ruta_detalle_calculo_existe_en_logistica_cotizaciones(self):
        self.assertIn('@app.route("/transporte/cotizaciones/<int:cid>/detalle-calculo")', LC_SRC)
        self.assertIn("def lc_cotizacion_detalle_calculo(cid):", LC_SRC)

    def test_gate_solo_superadmin_se_mantiene(self):
        src = _fn_source("lc_cotizacion_detalle_calculo")
        self.assertIn("_lc_solo_superadmin()", src)


class TestBotonAbreEnModal(unittest.TestCase):
    """Daniel: 'que lo abra en un modal, no en otra pantalla, con la barra
    de progreso que ya teníamos en transporte' -- mismo patrón que el
    botón 'Ver PDF' vecino, que ya usa openPdf()."""

    def test_boton_calc_llama_openpdf(self):
        idx = COTIZ_LIST_SRC.index('class="trc-row-btn trc-row-btn-calc"')
        bloque = COTIZ_LIST_SRC[idx:idx + 500]
        self.assertIn("openPdf(", bloque,
                       "el botón calculadora debe abrir vía openPdf(), no target=_blank directo")
        self.assertIn("return false;", bloque,
                       "debe cancelar la navegación default del <a>, igual que el botón PDF vecino")


class TestNoDataItemsConDot(unittest.TestCase):
    """BUG #2: data.items (dot) resuelve al método dict.items(), no a la
    clave "items". Debe ser data['items'] (brackets) en TODO el template."""

    def test_no_queda_dataitems_con_punto(self):
        # Ojo: "data.items" como SUBSTRING aparece en el comentario Jinja
        # que explica el bug -- se quitan los bloques {# ... #} antes de
        # buscar el acceso real.
        import re
        sin_comentarios = re.sub(r"\{#.*?#\}", "", TPL_SRC, flags=re.S)
        self.assertNotIn("data.items", sin_comentarios,
                          "quedó un acceso Jinja data.items con punto (fuera de comentarios)")

    def test_usa_dataitems_con_corchetes(self):
        self.assertIn("data['items']", TPL_SRC)


class TestPesoVolumenEnLaAPI(unittest.TestCase):
    """Daniel: 'un detalle más afinado... el kilo, peso volumétrico,
    metros cúbicos' -- antes lc_calculo solo exponía el predominante."""

    def test_items_detalle_expone_peso_real_y_volumetrico_por_item(self):
        # ast.unparse() normaliza los strings a comillas simples.
        src = _fn_source("lc_calculo")
        for campo in ("peso_real_kg", "peso_vol_kg", "volumen_m3", "peso_predominante_kg"):
            self.assertIn(f"'{campo}'", src, f"falta el campo {campo} en items_detalle")

    def test_header_expone_volumen_m3(self):
        src = _fn_source("lc_calculo")
        self.assertIn("'volumen_m3'", src)


class TestTemplateRealRender(unittest.TestCase):
    """Real-render con Flask (no solo texto/AST): confirma que el template
    de verdad compila y muestra los datos con el shape exacto que produce
    la ruta -- mismo patrón usado el resto de la sesión para bugs de Jinja."""

    def test_render_real_sin_datos_faltantes(self):
        sys.path.insert(0, BASE_DIR)
        from flask import Flask, render_template

        app = Flask(__name__, template_folder=os.path.join(BASE_DIR, "templates"),
                    static_folder=os.path.join(BASE_DIR, "static"))

        cot = {"numero": "CTR-000099", "empresa": "Cliente de Prueba", "rut": "1-9",
               "estado": "sent", "estado_label": "Enviada", "created_at_fmt": "01/01/2026 00:00"}
        data = {
            "ok": True,
            "courier": {"courier_nombre": "Felca", "costo_courier": 1000,
                        "peso_kg": 1.0, "peso_vol_kg": 2.0, "peso_facturable_kg": 2.0,
                        "volumen_m3": 0.008, "comuna_resuelta": "Ñuñoa"},
            "items": [{
                "item_id": 1, "descripcion": "Item test", "cantidad": 1, "total_bultos": 1,
                "peso_real_kg": 1.0, "peso_vol_kg": 2.0, "volumen_m3": 0.008,
                "peso_predominante_kg": 2.0, "share_pct_del_flete": 100.0,
                "formula": "formula de prueba",
                "costo_courier_prorrateo": 1000, "margen_pct_aplicado": 30.0,
                "margen_monto": 300, "descuento_pct": 0.0, "subtotal": 1300, "total": 1300,
            }],
            "header": {"margen_pct": 30.0, "margen_monto": 300, "subtotal_items": 1300,
                       "descuento_pct": 0.0, "descuento_monto": 0, "subtotal": 1300,
                       "iva_pct": 19.0, "iva_monto": 247, "total": 1547,
                       "formula": "formula header de prueba"},
        }
        with app.test_request_context():
            html = render_template("transporte/cotizacion_detalle_calculo.html",
                                    cot=cot, data=data, divisor_volumetrico=4000.0,
                                    generado_at="01/01/2026 00:00")
        self.assertIn("CTR-000099", html)
        self.assertIn("Ñuñoa", html)
        self.assertIn("Item test", html)
        self.assertNotIn("Traceback", html)


if __name__ == "__main__":
    unittest.main()

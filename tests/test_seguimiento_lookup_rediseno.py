"""Rediseño visual de templates/transporte/seguimiento_lookup.html
(2026-08-13, Daniel viendo la página en vivo: "está bonito pero muy
insípido... header gigante, sin contenedor... el footer sácalo... trabaja
con contundencia").

Es un cambio 100% de plantilla (sin tocar backend): el formulario sigue
mandando los MISMOS campos (doc_type con los mismos 4 values, doc_number,
customer_rut) al mismo endpoint (seguimiento_buscar), así que estos tests
se centran en que el contrato con el backend no se haya movido un pelo
mientras cambiaba toda la piel visual -- el <select> de tipo de documento
pasó a ser un grupo de radios estilo "chip", y hay que confirmar que sigue
preseleccionando el mismo valor que antes cuando vuelve un prefill (typo
en el RUT, por ejemplo).

Correr con:  py -m unittest tests.test_seguimiento_lookup_rediseno -v
"""
import os
import sys
import unittest

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TPL_PATH = os.path.join(BASE_DIR, "templates", "transporte", "seguimiento_lookup.html")
TPL_SRC = open(TPL_PATH, encoding="utf-8", errors="ignore").read()

sys.path.insert(0, BASE_DIR)


def _render(**ctx):
    from flask import Flask, render_template

    app = Flask(__name__, template_folder=os.path.join(BASE_DIR, "templates"),
                static_folder=os.path.join(BASE_DIR, "static"))

    @app.route("/transporte/seguimiento/buscar", methods=["POST"])
    def seguimiento_buscar():
        return "stub"

    with app.test_request_context():
        return render_template("transporte/seguimiento_lookup.html", **ctx)


class TestContratoConBackendIntacto(unittest.TestCase):
    """El rediseño no debe tocar nombres/values de campos ni el endpoint."""

    def test_action_apunta_a_seguimiento_buscar(self):
        self.assertIn('action="/transporte/seguimiento/buscar"', _render(error=None, prefill={}))

    def test_metodo_sigue_siendo_post(self):
        self.assertIn('method="POST"', TPL_SRC)

    def test_campos_con_los_mismos_nombres(self):
        for campo in ('name="doc_type"', 'name="doc_number"', 'name="customer_rut"'):
            self.assertIn(campo, TPL_SRC)

    def test_los_4_values_de_doc_type_se_mantienen(self):
        for value in ('value=""', 'value="BLV"', 'value="FCV"', 'value="GDV"'):
            self.assertIn(value, TPL_SRC)


class TestChipsPreseleccionanIgualQueElSelectViejo(unittest.TestCase):
    """El <select> con option selected pasó a radios con checked -- debe
    seguir marcando el mismo tipo cuando el backend manda un prefill
    (ej: el usuario mandó mal el RUT y vuelve a la página con lo que ya
    había tipeado)."""

    def test_prefill_fcv_marca_el_radio_de_factura(self):
        html = _render(error=None, prefill={"doc_type": "FCV", "doc_number": "12345", "customer_rut": ""})
        self.assertIn('id="dtFCV" name="doc_type" value="FCV" checked', html)
        self.assertNotIn('id="dtAny" name="doc_type" value="" checked', html)

    def test_sin_prefill_marca_cualquiera_por_defecto(self):
        html = _render(error=None, prefill={})
        self.assertIn('id="dtAny" name="doc_type" value="" checked', html)

    def test_prefill_conserva_el_numero_y_el_rut_tipeados(self):
        html = _render(error=None, prefill={"doc_type": "", "doc_number": "99887",
                                             "customer_rut": "11.111.111-1"})
        self.assertIn('value="99887"', html)
        self.assertIn('value="11.111.111-1"', html)


class TestErrorSigueRenderizando(unittest.TestCase):
    def test_mensaje_de_error_aparece(self):
        html = _render(error="No encontramos ese documento.", prefill={})
        self.assertIn("No encontramos ese documento.", html)
        self.assertIn('role="alert"', html)

    def test_sin_error_no_aparece_el_bloque(self):
        html = _render(error=None, prefill={})
        self.assertNotIn('role="alert"', html)


class TestPedidoExplicitoDeDaniel(unittest.TestCase):
    """'el footer tampoco me gusta, sácalo' + 'header gigante, sin
    contenedor' -- verificable en el HTML resultante, no solo en el
    código fuente."""

    def test_no_queda_footer(self):
        html = _render(error=None, prefill={})
        # Las clases/textos del footer viejo (seg-footer, copyright, links
        # de ayuda) no deben aparecer en ningún lado del render.
        for rastro in ("seg-footer", "¿Necesitas ayuda?", "Escríbenos", "© <strong>"):
            self.assertNotIn(rastro, html, f"quedó un rastro del footer viejo: {rastro!r}")

    def test_hero_no_tiene_max_width_propio(self):
        # El .hero (fondo/textura) debe correr borde a borde -- el max-width
        # vive en .shell (el contenido de la tarjeta), no en .hero.
        idx_hero = TPL_SRC.index(".hero{")
        bloque_hero = TPL_SRC[idx_hero:idx_hero + 400]
        self.assertNotIn("max-width", bloque_hero)

    def test_hero_incluye_la_previsualizacion_de_ruta(self):
        # "que contenga también el tracking de toda la del envío" -- el
        # mini-mapa de 3 paradas dentro del hero.
        html = _render(error=None, prefill={})
        self.assertIn("route-stop", html)
        self.assertIn("Pedido", html)
        self.assertIn("En camino", html)
        self.assertIn("Entregado", html)


if __name__ == "__main__":
    unittest.main()

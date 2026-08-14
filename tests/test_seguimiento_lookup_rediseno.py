"""Rediseño visual de templates/transporte/seguimiento_lookup.html
(2026-08-13, Daniel viendo la página en vivo: "está bonito pero muy
insípido... header gigante, sin contenedor... el footer sácalo... trabaja
con contundencia").

ACTUALIZADO la MISMA noche, horas después: Daniel trajo una auditoría de
ChatGPT y pidió sacar el RUT del formulario ("no quiero que solicites el
RUT"). El <select>/chips de tipo de documento + campo RUT que este archivo
probaba originalmente YA NO son el formulario principal -- se reemplazaron
por un único campo de código de seguimiento (ver seguimiento_por_codigo,
app.py, y tests/test_seguimiento_sin_rut.py para la cobertura del diseño
nuevo). Los tests que dependían de esos campos/chips se retiraron de acá
(no tenía sentido dejarlos fallando contra un formulario que ya no existe
a propósito) -- quedan solo los que siguen siendo ciertos hoy: método
POST, manejo de errores, footer fuera, hero sin max-width y con la
previsualización de ruta.

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

    @app.route("/seguimiento/buscar", methods=["POST"])
    def seguimiento_buscar():
        return "stub"

    @app.route("/seguimiento/codigo", methods=["POST"])
    def seguimiento_por_codigo():
        return "stub"

    @app.route("/seguimiento/recuperar", methods=["GET"])
    def seguimiento_recuperar_page():
        return "stub"

    with app.test_request_context():
        return render_template("transporte/seguimiento_lookup.html", **ctx)


class TestContratoConBackendIntacto(unittest.TestCase):
    def test_metodo_sigue_siendo_post(self):
        self.assertIn('method="POST"', TPL_SRC)


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

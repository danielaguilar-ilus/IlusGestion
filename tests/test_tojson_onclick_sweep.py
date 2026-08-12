"""Sweep completo (2026-08-12) del bug real de "Eliminar manifiesto"
(tests/test_eliminar_manifiesto_html_no_se_corta.py): |tojson dentro de un
onclick="..." de comillas DOBLES corta el atributo HTML en la primera
comilla incrustada -- el clic no ejecuta nada, sin ningún error visible.

Encontró y corrigió el MISMO patrón en otros 3 lugares:
  - templates/transporte/courier_ficha.html -- "Resetear PIN" del chofer
  - templates/admin_roles.html -- "Eliminar rol"
  - templates/admin/retiros_avisos.html -- eliminar aviso (botón basurero)

Y confirmó como SEGUROS (falsos positivos, no se tocaron):
  - templates/transporte/manifiesto_detalle.html -- usa onclick='...' de
    comilla SIMPLE, donde |tojson SÍ es seguro (Flask escapa ' -> \\u0027).

Correr con:  py -m unittest tests.test_tojson_onclick_sweep -v
"""
import unittest


def _leer(archivo):
    with open(archivo, encoding="utf-8") as fh:
        return fh.read()


class TestCourierFichaResetearPin(unittest.TestCase):

    def _bloque(self):
        src = _leer("templates/transporte/courier_ficha.html")
        i = src.find("resetearPinChofer")
        self.assertGreater(i, 0)
        return src[max(0, i - 200):i + 200]

    def test_ya_no_usa_tojson(self):
        self.assertNotIn("|tojson", self._bloque())

    def test_usa_data_nombre(self):
        bloque = self._bloque()
        self.assertIn("data-nombre=", bloque)
        self.assertIn("this.dataset.nombre", bloque)

    def test_render_real_no_se_corta(self):
        from flask import Flask, render_template_string
        app = Flask(__name__)
        tpl = ('<a href="#" data-nombre="{{ ch.nombre or \'\' }}" '
               'onclick="resetearPinChofer({{ ch.id }}, this.dataset.nombre);return false;">x</a>')
        with app.test_request_context():
            html = render_template_string(tpl, ch={"id": 7, "nombre": 'Juan "El Rápido" Pérez'})
        self.assertIn('data-nombre="Juan &#34;El Rápido&#34; Pérez"', html)
        i = html.find('onclick="')
        valor = html[i + len('onclick="'):]
        valor = valor[:valor.find('"')]
        self.assertIn("this.dataset.nombre", valor)


class TestAdminRolesEliminarRol(unittest.TestCase):

    def _bloque(self):
        src = _leer("templates/admin_roles.html")
        i = src.find("confirmarEliminarRol")
        self.assertGreater(i, 0)
        return src[max(0, i - 300):i + 200]

    def test_ya_no_usa_tojson(self):
        self.assertNotIn("|tojson", self._bloque())

    def test_usa_data_slug_y_nombre(self):
        bloque = self._bloque()
        self.assertIn("data-slug=", bloque)
        self.assertIn("data-nombre=", bloque)
        self.assertIn("this.dataset.slug", bloque)
        self.assertIn("this.dataset.nombre", bloque)

    def test_render_real_no_se_corta(self):
        from flask import Flask, render_template_string
        app = Flask(__name__)
        tpl = ('<button data-slug="{{ r.slug }}" data-nombre="{{ r.nombre }}" '
               'onclick="confirmarEliminarRol(this.dataset.slug, this.dataset.nombre)">x</button>')
        with app.test_request_context():
            html = render_template_string(tpl, r={"slug": "tecnico", "nombre": 'Técnico "Senior"'})
        self.assertIn('data-nombre="Técnico &#34;Senior&#34;"', html)
        i = html.find('onclick="')
        valor = html[i + len('onclick="'):]
        valor = valor[:valor.find('"')]
        self.assertEqual(valor, "confirmarEliminarRol(this.dataset.slug, this.dataset.nombre)")


class TestRetirosAvisosEliminar(unittest.TestCase):

    def _bloque(self):
        src = _leer("templates/admin/retiros_avisos.html")
        i = src.find("confirmarEliminarAviso")
        self.assertGreater(i, 0)
        return src[max(0, i - 200):i + 100]

    def test_ya_no_usa_tojson(self):
        self.assertNotIn("|tojson", self._bloque())

    def test_usa_data_titulo(self):
        bloque = self._bloque()
        self.assertIn("data-titulo=", bloque)
        self.assertIn("this.dataset.titulo", bloque)

    def test_render_real_no_se_corta(self):
        from flask import Flask, render_template_string
        app = Flask(__name__)
        tpl = ('<button data-titulo="{{ av.titulo or \'aviso\' }}" '
               'onclick="confirmarEliminarAviso(this, this.dataset.titulo)">x</button>')
        with app.test_request_context():
            html = render_template_string(tpl, av={"titulo": 'Aviso "urgente" de bodega'})
        self.assertIn('data-titulo="Aviso &#34;urgente&#34; de bodega"', html)
        i = html.find('onclick="')
        valor = html[i + len('onclick="'):]
        valor = valor[:valor.find('"')]
        self.assertEqual(valor, "confirmarEliminarAviso(this, this.dataset.titulo)")


class TestManifiestoDetalleQuedaIntacto(unittest.TestCase):
    """Falso positivo confirmado por el sweep: usa onclick='...' de comilla
    SIMPLE, donde |tojson SÍ es seguro -- no debía tocarse, y no se tocó."""

    def test_sigue_usando_onclick_de_comilla_simple_con_tojson(self):
        src = _leer("templates/transporte/manifiesto_detalle.html")
        i = src.find("abrirEditarEstado(")
        self.assertGreater(i, 0)
        bloque = src[max(0, i - 30):i + 100]
        self.assertIn("onclick='abrirEditarEstado(", bloque)
        self.assertIn("|tojson", bloque)


if __name__ == "__main__":
    unittest.main(verbosity=2)

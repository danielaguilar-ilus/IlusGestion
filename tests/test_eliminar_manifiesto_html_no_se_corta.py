"""BUG REAL, causa raíz de fondo (2026-08-12): "Eliminar manifiesto" no hacía
NADA -- ni error, ni confirmación, nada -- pese a que el fix anterior
(tests/test_eliminar_manifiesto_no_falla_en_silencio.py) ya blindó el JS.
Daniel volvió a probar después de mergear ese fix y reportó lo mismo: "NO
SALE NADA DE MENSAJES SOLO NO LO BORRA".

CAUSA REAL, confirmada renderizando la plantilla real con Flask/Jinja: el
onclick de "Eliminar manifiesto" usaba `|tojson` para pasar el correlativo,
DENTRO de un atributo `onclick="..."` con comillas DOBLES. `tojson` envuelve
cualquier string en comillas dobles LITERALES (así es JSON válido) -- eso
choca con las comillas dobles del propio atributo HTML y lo corta ahí
mismo. Confirmado con Flask real:

    onclick="foo({{ x|tojson }})"  con x="MAN-2026-0033"
    → onclick="foo("MAN-2026-0033")"   ← se corta en la primera comilla

El navegador cierra el atributo `onclick` justo después de la coma, así
que lo que de verdad queda como handler es `eliminarManifiesto(33, ` --
JavaScript inválido. El error ocurre AL COMPILAR el handler (la primera
vez que se necesita, o sea al hacer clic), antes de ejecutar una sola
línea de eliminarManifiesto() -- por eso el fix anterior (blindar el
try/catch DENTRO de la función) no alcanzaba: el bug estaba afuera,
en el HTML.

`|tojson` SÍ es seguro dentro de un atributo de comilla SIMPLE (Flask lo
escapa: ' → \\u0027), pero acá se cambió al patrón data-* que esta misma
página ya usaba de forma segura (ver data-etiquetas-title, línea ~343) --
ni siquiera hace falta tojson para eso: el auto-escape normal de Jinja en
contexto de atributo alcanza.

Correr con:  py -m unittest tests.test_eliminar_manifiesto_html_no_se_corta -v
"""
import unittest


class TestElAtributoOnclickYaNoSeCorta(unittest.TestCase):

    def _fuente_boton(self):
        with open("templates/transporte/manifiestos.html", encoding="utf-8") as fh:
            src = fh.read()
        i = src.find("js-eliminar-manifiesto")
        self.assertGreater(i, 0, "no se encontro el boton de eliminar manifiesto")
        # Retrocede hasta el <a que lo contiene.
        ini = src.rfind("<a ", 0, i)
        fin = src.find("</a>", i)
        return src[ini:fin]

    def test_ya_no_usa_tojson_dentro_del_onclick(self):
        """El bug real: tojson dentro de un atributo de comilla doble."""
        bloque = self._fuente_boton()
        self.assertNotIn("|tojson", bloque)

    def test_usa_data_attributes_para_el_correlativo(self):
        bloque = self._fuente_boton()
        self.assertIn("data-mid=", bloque)
        self.assertIn("data-correlativo=", bloque)
        self.assertIn("this.dataset.mid", bloque)
        self.assertIn("this.dataset.correlativo", bloque)

    def test_renderiza_completo_sin_cortarse_con_un_correlativo_real(self):
        """La prueba definitiva: renderizar la plantilla REAL (no una
        reconstrucción) con Flask/Jinja de verdad, con un correlativo típico,
        y confirmar que el atributo onclick sale COMPLETO -- ni una sola
        comilla incrustada que lo corte a mitad de camino."""
        from flask import Flask, render_template_string
        app = Flask(__name__)
        bloque = self._fuente_boton()
        # Envuelve el fragmento real (que usa `m`) en un contexto minimo,
        # igual patron que el resto de la suite para probar fragmentos
        # reales de app.py/templates sin levantar toda la app.
        tpl = "{% set m = {'id': mid, 'correlativo': correlativo} %}" + bloque
        with app.test_request_context():
            html = render_template_string(tpl, mid=33, correlativo="MAN-2026-0033")
        # Si el HTML se hubiera cortado (bug viejo), "Eliminar manifiesto"
        # -- el texto DENTRO del <a> -- ni siquiera aparecería completo, o
        # el onclick tendria una comilla suelta que rompe el parseo.
        self.assertIn("Eliminar manifiesto", html)
        self.assertIn('data-correlativo="MAN-2026-0033"', html)
        # Ninguna comilla doble suelta DENTRO del valor de onclick (que
        # cortaria el atributo). Como ya no hay tojson, esto no puede pasar,
        # pero se deja como guarda explicita del sintoma real.
        i_onclick = html.find("onclick=\"")
        self.assertGreater(i_onclick, 0)
        valor = html[i_onclick + len('onclick="'):]
        cierre = valor.find('"')
        atributo_completo = valor[:cierre]
        self.assertIn("this.dataset.correlativo", atributo_completo)
        self.assertIn("eliminarManifiesto(", atributo_completo)


if __name__ == "__main__":
    unittest.main(verbosity=2)

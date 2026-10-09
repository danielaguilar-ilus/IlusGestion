# -*- coding: utf-8 -*-
"""Daniel 2026-10-09: «quién creó la OT» siempre a la vista, debajo del título, sin tener que abrir la actividad."""
import os
import unittest
import jinja2

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _src():
    return open(os.path.join(RAIZ, "templates", "ot2", "detalle.html"), encoding="utf-8").read()


class TestCreadaPorVisible(unittest.TestCase):
    def test_el_encabezado_dice_quien_creo_la_ot(self):
        s = _src()
        i = s.index('id="otdCreadaPor"')
        # va dentro del bloque del título (entre el título y el sello de estado), no dentro de un {% if not es_tecnico %}
        self.assertLess(s.index('class="otd-dir"'), i)
        self.assertLess(i, s.index('class="otd-sello'))
        self.assertIn("Creada", s[i:i + 600])
        self.assertIn("v.created_by", s[i - 300:i + 600])

    def test_se_ve_para_cualquiera_no_es_dato_de_plata(self):
        s = _src()
        i = s.index('id="otdCreadaPor"')
        antes = s[max(0, i - 900):i]
        self.assertNotIn("es_tecnico", antes.split("otd-dir")[-1])

    def test_sigue_la_tarjeta_de_informacion(self):
        self.assertIn("Creada por <b>{{ v.created_by }}</b>", _src())

    def test_la_plantilla_compila(self):
        jinja2.Environment().parse(_src())


if __name__ == "__main__":
    unittest.main()

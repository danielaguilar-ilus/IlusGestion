"""El botón "Ver manifiesto" del banner verde de éxito (cubicador → /asignar,
tras enviar una factura) abría en pestaña nueva (target="_blank").

BUG REAL (2026-08-12). Daniel: "EL BOTON NO FUNCIONABA TE ENVIA ABRIR LA APP
Y QUEDA EN NEGRO TAMBIEN" -- en el celular, target="_blank" sobre un link
INTERNO de la propia app dispara el selector "Abrir en la app"/una pestaña
nueva que queda en blanco/negro en vez de navegar. Es un link interno
(mismo origen, /transporte/manifiestos/<id>): no hay ninguna razón real
para forzar pestaña nueva -- Daniel ya está adentro de ILUS, y navegar en
la MISMA pestaña es lo esperado (puede volver con el botón atrás).

Correr con:  py -m unittest tests.test_ver_manifiesto_misma_pestana -v
"""
import unittest

with open("static/cubicador_asignar.js", encoding="utf-8", errors="ignore") as fh:
    JS_SRC = fh.read()


class TestVerManifiestoNavegaEnLaMismaPestana(unittest.TestCase):

    def _bloque_boton(self):
        i = JS_SRC.find("Ver manifiesto")
        self.assertGreater(i, 0, "no se encontro el boton 'Ver manifiesto'")
        return JS_SRC[max(0, i - 400):i + 20]

    def test_ya_no_usa_target_blank(self):
        bloque = self._bloque_boton()
        self.assertNotIn('target="_blank"', bloque)

    def test_el_link_sigue_apuntando_al_manifiesto(self):
        bloque = self._bloque_boton()
        self.assertIn("/transporte/manifiestos/${info.manifest_id}", bloque)


if __name__ == "__main__":
    unittest.main(verbosity=2)

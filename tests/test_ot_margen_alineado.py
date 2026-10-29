"""La política de margen es UNA sola — REGLA #57 (Daniel, 2026-10-29).

«El margen autorizado de todos los servicios es 30 %, movible con una tolerancia hasta 20; de ahí hacia abajo amarillo, y rojo los negativos.»

El número vive en `ot_margen_politica.py` (TOLERANCIA_PCT = 20). Como el proyecto tiene espejos que no pueden importar ese módulo (JS del navegador, una
plantilla Jinja, una constante que otras pruebas cortan de app.py con ast), esta prueba falla si CUALQUIERA se desvía: así «la ficha dice una cosa y el
tablero otra» deja de poder pasar sin que salte una prueba.
"""
import os
import re
import unittest

import ot_doc_cadena
import ot_gerencial
import ot_margen_politica as pol
from tests import _fuente_app as FA


def _leer(*ruta):
    with open(os.path.join(FA.RAIZ, *ruta), encoding="utf-8") as fh:
        return fh.read()


class TestPoliticaUnica(unittest.TestCase):
    def test_la_politica_dice_30_y_20(self):
        self.assertEqual((pol.OBJETIVO_PCT, pol.TOLERANCIA_PCT, pol.PERDIDA_PCT), (30.0, 20.0, 0.0))

    def test_las_bandas(self):
        self.assertEqual([pol.banda(x) for x in (45, 30, 29.9, 20, 19.9, 0, -0.1)],
                         ["objetivo", "objetivo", "tolerancia", "tolerancia", "amarillo", "amarillo", "rojo"])
        self.assertIsNone(pol.banda(None))
        self.assertEqual(pol.banda(None, queda=-5000), "rojo")                      # sin % pero perdiendo plata: rojo, nunca «sin medir»


class TestTodosLosEspejosDicenLoMismo(unittest.TestCase):
    TOL = int(pol.TOLERANCIA_PCT)

    def test_la_constante_del_servidor(self):
        m = re.search(r"^_OT_FIN_UMBRAL_BAJO\s*=\s*(\d+)", FA.texto(), re.M)
        self.assertIsNotNone(m)
        self.assertEqual(int(m.group(1)), self.TOL)

    def test_el_espejo_javascript_de_la_cuenta(self):
        m = re.search(r"var UMBRAL_BAJO\s*=\s*(\d+)", _leer("static", "ot_finanzas.js"))
        self.assertIsNotNone(m)
        self.assertEqual(int(m.group(1)), self.TOL)

    def test_la_ficha_de_la_ot(self):
        m = re.search(r"window\.OTD_FIN_UMBRAL_BAJO\s*=\s*(\d+)", _leer("templates", "ot2", "detalle.html"))
        self.assertIsNotNone(m)
        self.assertEqual(int(m.group(1)), self.TOL)

    def test_el_tablero_resumen_y_la_trazabilidad(self):
        self.assertEqual(ot_gerencial.UMBRAL_BAJO, self.TOL)
        self.assertEqual(ot_doc_cadena.MARGEN_SANO, float(self.TOL))

    def test_el_semaforo_de_la_vida_del_cliente_se_comporta_igual(self):
        f = FA.cargar(funciones=["_vida_margen_clase"])["_vida_margen_clase"]
        self.assertEqual([f(x, 1000) for x in (45, 30, 20, 19.9, 0, -0.1)], ["ok", "ok", "ok", "bajo", "bajo", "rojo"])
        self.assertEqual(f(None, -5000), "rojo")
        self.assertEqual(f(None, None), "sin_dato")

    def test_el_resumen_y_la_trazabilidad_marcan_igual_que_la_politica(self):
        # una OT con 19 % ya no es «sana»; con 21 % sí (antes el corte era 10 %)
        self.assertEqual(ot_gerencial.clase_margen(19, 1000), "bajo")
        self.assertEqual(ot_gerencial.clase_margen(21, 1000), "ok")
        self.assertEqual(ot_gerencial.clase_margen(-1, -10), "rojo")
        self.assertEqual(ot_doc_cadena.margen_nivel(19), "ambar")
        self.assertEqual(ot_doc_cadena.margen_nivel(21), "verde")
        self.assertEqual(ot_doc_cadena.margen_nivel(-1), "rojo")

    def test_la_cuenta_unica_etiqueta_con_el_corte_de_la_politica(self):
        src = FA.fuente_funcion("_ot_finanzas")
        self.assertIn('f"Margen bajo (menos de {_OT_FIN_UMBRAL_BAJO} %)"', src)      # el texto sale de la constante, no de un número escrito a mano


class TestElTableroUsaLaPolitica(unittest.TestCase):
    def test_explorar_pinta_con_ot_margen_politica(self):
        self.assertIn("ot_margen_politica", _leer("ot_costeo.py"))


if __name__ == "__main__":
    unittest.main()

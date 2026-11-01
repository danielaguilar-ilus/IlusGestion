# -*- coding: utf-8 -*-
"""🔒 REGLA #26 — la LISTA de cotizaciones y el historial de cotizaciones de un cliente se cierran a la familia técnico (Daniel, 31-oct: «sí, ciérrala»).

Hallazgo de la auditoría del 4-nov: `/tickets/cotizaciones` solo llevaba `@_no_tecnico_externo` (cierra al técnico EXTERNO), así que el técnico interno
y el elevado (Jaizer, Lenin, Dave) la abrían y veían cliente, RUT, total de cada cotización y el indicador «Margen aprobado»; y
`/mantenciones/api/clientes/<id>/cotizaciones` devolvía el total de cada cotización con solo `@_mant_required`. El barrido por nombre de la prueba de
confidencialidad no las detectaba (ya van en sus listas explícitas, tests/test_confidencialidad_financiera_tecnicos.py).

Se prueba (sin Flask ni BD): decoradores de las dos rutas, el enlace del menú por familia (no por rol exacto) y el botón «Generar cotización» de la ficha.
Correr:  python -m pytest tests/test_cotizaciones_cerradas_a_tecnicos.py -q -p no:warnings
"""
import re
import unittest

from tests import _fuente_app as FA

BASE = FA.texto("templates/base.html")
FICHA = FA.texto("templates/tickets/ficha.html")
MOD = FA.texto("tickets_module.py")


def _decoradores_indentados(nombre):
    """Las líneas `@...` contiguas justo encima de `def nombre(` en tickets_module.py (las rutas viven dentro de una función: van con sangría)."""
    m = re.search(r"((?:^[ \t]+@.*\n)+)^[ \t]+def %s\(" % re.escape(nombre), MOD, re.M)
    if not m:
        raise AssertionError("no se encontró %s con decoradores en tickets_module.py" % nombre)
    return m.group(1)


class TestListaDeCotizacionesCerradaALosTecnicos(unittest.TestCase):
    def test_la_lista_lleva_el_candado_sin_tecnicos(self):
        deco = _decoradores_indentados("tk_cotizaciones_list")
        self.assertIn('"/tickets/cotizaciones"', deco)
        self.assertIn("@_tk_sin_cotizaciones_tecnico", deco, "la familia técnico recibe el aviso amable y vuelve a Tickets")
        self.assertIn("@_no_tecnico_externo", deco, "el candado de siempre sigue")

    def test_el_historial_por_cliente_lleva_no_tecnico(self):
        deco = " ".join(FA.decoradores("mant_cliente_cotizaciones"))
        self.assertIn("/mantenciones/api/clientes/<int:cid>/cotizaciones", deco)
        self.assertIn("_no_tecnico", deco)

    def test_el_candado_de_la_familia_usa_la_misma_fuente_que_el_resto(self):
        i = MOD.index("def _tk_sin_cotizaciones_tecnico(")
        src = MOD[i:i + 2500]
        self.assertIn("_tk_es_tecnico()", src)
        self.assertIn("COTIZACION_SIN_ACCESO", src, "las APIs responden 403 JSON")


class TestMenuYFicha(unittest.TestCase):
    def test_el_enlace_del_menu_se_decide_por_familia_no_por_rol_exacto(self):
        i = BASE.index("url_for('tk_cotizaciones_list')")
        antes = BASE[max(0, i - 2500):i]
        self.assertIn("{% if current_user and not es_tecnico %}", antes)
        self.assertNotIn("current_user.role != 'tecnico'", antes, "la comparación literal dejaba pasar al técnico elevado")

    def test_el_boton_generar_cotizacion_no_se_ofrece_a_la_familia_tecnico(self):
        i = FICHA.index("Generar cotización")
        trozo = FICHA[max(0, i - 700):i + 200]
        self.assertTrue(re.search(r"es_tecnico", trozo), "el alta ya respondía 403: era un callejón; ahora ni se ofrece")


if __name__ == "__main__":
    unittest.main()

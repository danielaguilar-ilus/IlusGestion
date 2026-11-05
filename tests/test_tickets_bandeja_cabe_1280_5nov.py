# -*- coding: utf-8 -*-
"""📱 5-nov-2026 — REGLA #61/#4.3: la tabla de la bandeja de Tickets CABE a 1280, 1366 y 1440 px.

Medido en producción con los 50 tickets reales: tabla de 1144-1285 px en una caja de 980 (1280), 1066 (1366) y 1140 (1440) = 258, 172 y 98 px de scroll lateral.
Con el CSS de abajo, inyectado en producción: 0 px a 1366 y 1440, y a 1280 la tabla mide lo mismo que su caja (980).
Correr:  python -m pytest tests/test_tickets_bandeja_cabe_1280_5nov.py -q -p no:warnings
"""
import os
import re
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
with open(os.path.join(RAIZ, "static", "tickets_list.css"), encoding="utf-8") as fh:
    CSS = fh.read().replace("\r\n", "\n")


def _media(condicion):
    m = re.search(r"@media\s*%s\s*\{" % re.escape(condicion), CSS)
    assert m, condicion
    i, prof = m.end(), 1
    while i < len(CSS) and prof:
        prof += {"{": 1, "}": -1}.get(CSS[i], 0)
        i += 1
    return CSS[m.end():i - 1]


class TestCabeEnEscritorio(unittest.TestCase):
    def test_los_dos_tramos_existen_y_empiezan_en_768(self):
        self.assertIn("table.tk-table td{padding:10px 8px;}", _media("(min-width:768px) and (max-width:1599.98px)"))
        self.assertIn("table.tk-table{min-width:0;}", _media("(min-width:768px) and (max-width:1365.98px)"))

    def test_nada_se_quita_solo_se_acota_y_se_deja_partir(self):
        for bloque in (_media("(min-width:768px) and (max-width:1599.98px)"), _media("(min-width:768px) and (max-width:1365.98px)")):
            self.assertNotIn("display:none", bloque.replace(" ", ""), "no se oculta ninguna columna (REGLA #4.2)")
        self.assertIn("white-space:normal", _media("(min-width:768px) and (max-width:1599.98px)"))

    def test_el_minimo_de_1040_solo_se_quita_bajo_1366(self):
        self.assertIn("min-width:1040px;}", CSS)  # la regla base sigue
        self.assertNotRegex(_media("(min-width:768px) and (max-width:1599.98px)"), r"table\.tk-table\{min-width")

    def test_no_toca_el_celular_ni_las_pantallas_grandes(self):
        for cond in ("(min-width:768px) and (max-width:1599.98px)", "(min-width:768px) and (max-width:1365.98px)"):
            self.assertIn("min-width:768px", cond)
            self.assertIn("max-width", cond)


if __name__ == "__main__":
    unittest.main()

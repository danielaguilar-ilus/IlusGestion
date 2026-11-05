# -*- coding: utf-8 -*-
"""📱 5-nov-2026 — REGLA #61/#4.3: la tabla del Catálogo CABE a 1280 y 1366 px (medido en producción: 522 y 436 px de scroll lateral antes; 0 después).
Correr:  python -m pytest tests/test_catalogo_cabe_1280_5nov.py -q -p no:warnings"""
import os
import re
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
with open(os.path.join(RAIZ, "static", "catalogo_list.css"), encoding="utf-8") as fh:
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
    def test_los_dos_tramos(self):
        a = _media("(min-width:768px) and (max-width:1599.98px)")
        b = _media("(min-width:768px) and (max-width:1365.98px)")
        self.assertIn("table.cat-table{min-width:0;}", a)
        self.assertIn(".cat-nombre{min-width:120px;max-width:165px;}", b)

    def test_no_se_oculta_ninguna_columna(self):
        for cond in ("(min-width:768px) and (max-width:1599.98px)", "(min-width:768px) and (max-width:1365.98px)"):
            self.assertNotIn("display:none", _media(cond).replace(" ", ""))

    def test_la_regla_base_sigue_y_el_nombre_se_lee_entero(self):
        self.assertIn("min-width:960px;}", CSS)
        self.assertIn("white-space:normal", _media("(min-width:768px) and (max-width:1599.98px)"))


if __name__ == "__main__":
    unittest.main()

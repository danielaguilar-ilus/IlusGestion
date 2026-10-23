# -*- coding: utf-8 -*-
"""2026-10-23 · NVI 687, Daniel: «cuando subimos una OT es necesario que podamos ver el detalle de las cantidades… en una revisión puede
actuar como las mantenciones, duplicando las filas por cantidad, para que no genere una sensación de ausencia de material».

En el asistente, una revisión (checklist heredado) abre UNA FILA POR UNIDAD también para los productos de clase repetible (discos,
mancuernas, bancos): «Banco Ajustable ×2» → «(1 de 2)», «(2 de 2)». Si son más de O2M_SPLIT_MAX unidades o no caben en el tope de
150 filas por OT, quedan en una línea con su cantidad (para contar).
"""
import os
import re
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
with open(os.path.join(RAIZ, "templates", "ot2", "_modal_crear.html"), encoding="utf-8") as _fh:
    M = _fh.read()


class TestRevisionAbreUnaFilaPorUnidad(unittest.TestCase):
    def test_codigo(self):
        self.assertIn("var O2M_SPLIT_MAX = 20;", M)
        self.assertIn("if (porLinea && it.qty > 1 && it.qty <= O2M_SPLIT_MAX && _filasEst + it.qty <= O2M_MAX_FILAS) porLinea = false;", M)
        self.assertIn("var n = porLinea ? 1 : it.qty;", M)
        self.assertIn("var porLinea = h.repetible === true;", M)       # sigue existiendo el caso «en una línea, para contar»

    def test_la_nota_ya_no_dice_que_los_discos_van_por_linea(self):
        self.assertNotIn("Discos y mancuernas: una por línea, para contar.", M)
        self.assertIn("Cada unidad del documento es una fila", M)

    def test_la_regla_en_numeros(self):
        """La misma decisión del código, ejecutada con casos reales de la NVI 687."""
        def decide(qty, repetible, filas_antes, max_split=20, max_filas=150):
            por_linea = bool(repetible)
            if por_linea and qty > 1 and qty <= max_split and filas_antes + qty <= max_filas:
                por_linea = False
            return 1 if por_linea else qty
        self.assertEqual(decide(2, True, 0), 2)           # Banco Ajustable ILUS Xtend ×2
        self.assertEqual(decide(3, True, 5), 3)           # Par Discos ×3 PR
        self.assertEqual(decide(1, True, 0), 1)
        self.assertEqual(decide(3, False, 0), 3)          # máquina: siempre por unidad
        self.assertEqual(decide(200, True, 0), 1)         # 200 discos: una línea para contar
        self.assertEqual(decide(10, True, 145), 1)        # no cabe en las 150 filas de la OT
        # el JS y el modelo deben decir lo mismo: los mismos números dentro del código
        self.assertTrue(re.search(r"O2M_SPLIT_MAX = 20", M) and re.search(r"O2M_MAX_FILAS = 150", M))


if __name__ == "__main__":
    unittest.main()

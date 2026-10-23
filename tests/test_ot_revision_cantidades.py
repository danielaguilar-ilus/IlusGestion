# -*- coding: utf-8 -*-
"""2026-10-23 · NVI 687, Daniel: «los accesorios no hace falta segmentarlos: heredan la cantidad; si es un EQUIPO con plantilla, una línea
por unidad: esa es la regla para las OT».

En el asistente, con checklist heredado (revisión): un equipo se abre UNA FILA POR UNIDAD («(1 de 3)»); un accesorio (clase repetible:
discos, mancuernas) es UNA línea con la cantidad del documento (editable). Se probó abrir también los accesorios por unidad y Daniel lo
descartó: no volver a hacerlo.
"""
import os
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
with open(os.path.join(RAIZ, "templates", "ot2", "_modal_crear.html"), encoding="utf-8") as _fh:
    M = _fh.read()


class TestEquipoPorUnidadAccesorioPorLinea(unittest.TestCase):
    def test_equipo_una_fila_por_unidad_y_accesorio_una_linea_con_cantidad(self):
        self.assertIn("var porLinea = h.repetible === true;", M)
        self.assertIn("var n = porLinea ? 1 : it.qty;", M)
        self.assertIn("unidad: porLinea ? null : u, unidades: porLinea ? null : it.qty,", M)
        self.assertIn("cantidad: porLinea ? it.qty : 1,", M)
        self.assertIn("descripcion: porLinea ? ('Cantidad según el documento: ' + it.qty) : '',", M)

    def test_no_se_parten_los_accesorios(self):
        self.assertNotIn("O2M_SPLIT_MAX", M)
        self.assertNotIn("porLinea = false;", M)

    def test_la_nota_lo_dice(self):
        self.assertIn("Equipos: una fila por unidad", M)
        self.assertIn("Accesorios (discos, mancuernas): una línea que hereda la cantidad del documento.", M)


if __name__ == "__main__":
    unittest.main()

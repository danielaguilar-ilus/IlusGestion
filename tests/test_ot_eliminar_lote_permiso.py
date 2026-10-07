"""Eliminar OT en lote: superadmin O quien tenga el permiso "Eliminar OT" (Daniel, 2026-10-07).

Sin BD ni Flask: funciones puras de app.py extraídas con ast + revisiones de la plantilla.
Correr con:  py -m unittest tests.test_ot_eliminar_lote_permiso
"""
import os
import re
import types
import unittest

from tests.test_incidencias_bajas import _cargar

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class TestGateDelLote(unittest.TestCase):
    def _gate(self, permisos):
        amb = _cargar(["_ot2_lote_gate_superadmin"], extra={
            "g": types.SimpleNamespace(permissions=permisos, user={"id": 1, "username": "x"}),
            "jsonify": lambda d: d, "print": lambda *a, **k: None})
        return amb["_ot2_lote_gate_superadmin"]()

    def test_superadmin_pasa(self):
        self.assertIsNone(self._gate({"superadmin": True}))

    def test_quien_tiene_eliminar_ot_pasa(self):
        self.assertIsNone(self._gate({"mant_eliminar": True}))

    def test_sin_el_permiso_recibe_403(self):
        cuerpo, http = self._gate({"mantenciones": True, "mant_eliminar": False})
        self.assertEqual(http, 403)
        self.assertFalse(cuerpo["ok"])

    def test_sin_permisos_recibe_403(self):
        self.assertEqual(self._gate({})[1], 403)


class TestPlantillaDelLote(unittest.TestCase):
    def test_los_tres_puntos_de_la_interfaz_usan_la_misma_condicion(self):
        with open(os.path.join(RAIZ, "templates", "ot2", "panel.html"), encoding="utf-8") as fh:
            html = fh.read()
        self.assertEqual(html.count("permissions.superadmin or permissions.mant_eliminar"), 3)
        self.assertNotIn("{% set puede_lote = permissions and permissions.superadmin %}", html)


if __name__ == "__main__":
    unittest.main()

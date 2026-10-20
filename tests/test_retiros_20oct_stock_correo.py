# -*- coding: utf-8 -*-
"""2026-10-20 · El correo al cliente dice la VERDAD del stock (Daniel, con el correo de Ivo Simunovic: «indica que los productos están en
stock… transmite la verdad e indícale al cliente que no hay nomás»).

Antes, la tabla «Productos de tu retiro» ponía «✓ en stock» a todo producto que no estuviera marcado sin saldo, sin mirar el stock. Ahora
usa el stock físico de la bodega del MISMO diagnóstico que ve la ficha (ERP en solo lectura): En stock / Sin stock / Hay N de M / Por confirmar.

    py -m pytest tests/test_retiros_20oct_stock_correo.py -q
"""
import os
import sys

import pytest

_TESTS = os.path.dirname(os.path.abspath(__file__))
RAIZ = os.path.dirname(_TESTS)
sys.path.insert(0, RAIZ)

import pickups_module as pm  # noqa: E402


class TestReglaPura:
    @pytest.mark.parametrize("fisico,cant,clave,texto", [
        (5, 1, "ok", "✓ En stock"),
        (1, 1, "ok", "✓ En stock"),
        (0, 1, "sin", "✕ Sin stock"),
        (-2, 1, "sin", "✕ Sin stock"),
        (1, 3, "parcial", "Hay 1 de 3"),
        (1.5, 2, "parcial", "Hay 1,50 de 2"),
        (None, 1, "?", "Por confirmar"),          # ERP sin responder: nunca se afirma stock sin verlo
        ("x", 1, "?", "Por confirmar"),
    ])
    def test_cada_caso(self, fisico, cant, clave, texto):
        assert pm.stock_para_cliente(fisico, cant) == (clave, texto)


class TestCorreo:
    def _fuente(self):
        with open(os.path.join(RAIZ, "pickups_module.py"), encoding="utf-8") as f:
            src = f.read()
        i = src.index("    def _pickup_productos_html(req):")
        return src[i:src.index("\n    def ", i + 10)]

    def test_ya_no_afirma_stock_sin_mirarlo(self):
        fn = self._fuente()
        assert '">✓ en stock</span>' not in fn and "sin saldo ERP</span>" not in fn      # ya no hay un rótulo fijo
        assert "_pickup_productos_diagnostico(int(rid))" in fn and "stock_para_cliente(_fis, qty)" in fn

    def test_avisa_con_transparencia_solo_si_falta_algo(self):
        fn = self._fuente()
        assert "if _hay_falta:" in fn and "hoy no están disponibles en nuestra bodega" in fn
        assert "except Exception as _e_st:" in fn          # el stock que no se pudo leer no rompe el correo

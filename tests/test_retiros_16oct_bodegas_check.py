# -*- coding: utf-8 -*-
"""2026-10-16 · ¿En qué bodega de Check está cada producto? (pedido de logística vía Daniel: «hay productos que tengo en bodegas externas;
si puedo identificar las bodegas podría decidir si aceptar la fecha o correrla»). Se ve en el modal «Agregar factura o boleta».

Check SOLO LECTURA (REGLA #4.4): se usa el reporte de stock ya cacheado de app.py; aquí, filas de mentira.

    py -m pytest tests/test_retiros_16oct_bodegas_check.py -q
"""
import os
import sys
from types import SimpleNamespace

import pytest

_TESTS = os.path.dirname(os.path.abspath(__file__))
RAIZ = os.path.dirname(_TESTS)
sys.path.insert(0, _TESTS)
sys.path.insert(0, RAIZ)

import _arnes_retiros as A  # noqa: E402
import retiros_check as RC  # noqa: E402

FILAS = [
    {"codigo": "FZAMC037S1800", "bodega": "BODEGA 02", "ubicacion": "PISOD-12", "ua": "UA1", "stFisico": "2", "disponible": "1"},
    {"codigo": "FZAMC037S1800", "bodega": "BODEGA  EXTERNA LAMPA", "ubicacion": "RACK-3", "ua": "UA2", "stFisico": 6, "disponible": 6},
    {"codigo": "FZAMC037S1800", "bodega": "BODEGA EXTERNA LAMPA", "ubicacion": "RACK-4", "ua": "UA3", "stFisico": 4, "disponible": 4},
    {"codigo": "FZAMC037S1800", "bodega": "BODEGA 13", "ubicacion": "INC02-01", "ua": "UA4", "stFisico": 0, "disponible": 0},
    {"codigo": "otro", "bodega": "BODEGA 02", "ubicacion": "X", "ua": "UA5", "stFisico": 9},
    {"codigo": "BANCO01", "bodega": "BODEGA 02", "ubicacion": "PISOD-01", "ua": "UA6", "stFisico": "no-es-numero"},
]


class TestFuncionPura:
    def test_agrupa_por_bodega_con_la_mayor_primero_y_sin_stock_cero(self):
        r = RC.bodegas_por_sku(FILAS, ["fzamc037s1800", "BANCO01", "NOEXISTE"])
        lampa, b02 = r["FZAMC037S1800"]
        assert lampa["bodega"] == "BODEGA EXTERNA LAMPA" and lampa["fisico"] == 10 and lampa["uas"] == 2
        assert lampa["ubicaciones"] == ["RACK-3", "RACK-4"] and lampa["disponible"] == 10
        assert b02["bodega"] == "BODEGA 02" and b02["fisico"] == 2 and b02["disponible"] == 1
        assert all(b["bodega"] != "BODEGA 13" for b in r["FZAMC037S1800"])          # stock 0 no se muestra
        assert r["BANCO01"] == [] and r["NOEXISTE"] == []                            # número raro → nunca inventa stock
        assert "OTRO" not in r                                                       # solo los SKU pedidos

    def test_corta_ubicaciones_y_dice_cuantas_quedan(self):
        filas = [{"codigo": "A", "bodega": "B1", "ubicacion": f"U{i}", "stFisico": 1} for i in range(6)]
        b = RC.bodegas_por_sku(filas, ["A"])["A"][0]
        assert b["ubicaciones"] == ["U0", "U1", "U2"] and b["mas_ubicaciones"] == 3

    def test_datos_vacios_o_raros(self):
        assert RC.bodegas_por_sku(None, ["A"]) == {"A": []}
        assert RC.bodegas_por_sku([None, "x", {"codigo": "A"}], ["A"]) == {"A": []}
        assert RC.bodegas_por_sku(FILAS, []) == {}


@pytest.fixture()
def cli():
    def construir(filas):
        app, db, ctx, esp = A.construir_app(extra={"_checkwms_stock_rows": (lambda: filas)})
        return SimpleNamespace(app=app, cli=app.test_client(), esp=esp)
    return construir


class TestRuta:
    def test_devuelve_las_bodegas_de_los_skus_pedidos(self, cli):
        e = cli(FILAS)
        d = e.cli.get("/retiros/api/check-bodegas?skus=FZAMC037S1800,BANCO01").get_json()
        assert d["ok"] and [b["bodega"] for b in d["bodegas"]["FZAMC037S1800"]] == ["BODEGA EXTERNA LAMPA", "BODEGA 02"]
        assert d["bodegas"]["BANCO01"] == []
        assert e.esp.check.llamadas == []                       # no llama a Check por su cuenta: usa el reporte ya cacheado

    def test_check_caido_lo_dice_sin_inventar(self, cli):
        e = cli(None)
        r = e.cli.get("/retiros/api/check-bodegas?skus=A")
        assert r.status_code == 503 and r.get_json()["ok"] is False

    def test_sin_skus_no_consulta_nada(self, cli):
        assert cli(FILAS).cli.get("/retiros/api/check-bodegas").get_json() == {"ok": True, "bodegas": {}, "edad_min": None}


class TestModal:
    def test_cada_producto_del_modal_tiene_su_lugar_para_la_bodega(self):
        with open(os.path.join(RAIZ, "static", "retiros_internal_detail.js"), encoding="utf-8") as f:
            js = f.read()
        assert js.count('<div class="ln-bod" data-bod-sku=') == 2                  # pestaña «Por documento» y «Por RUT»
        assert js.count("_rbaCargarBodegas(") >= 3                                  # definición + las dos pestañas
        assert "/retiros/api/check-bodegas?skus=" in js and "Sin stock en Check" in js

    def test_resumen_de_bodegas_del_documento_para_decidir_la_fecha(self):
        # Daniel 2026-10-16: «si el producto está en múltiples ubicaciones, hazme un resumen de las múltiples bodegas dónde está»
        with open(os.path.join(RAIZ, "static", "retiros_internal_detail.js"), encoding="utf-8") as f:
            js = f.read()
        i = js.index("async function _rbaCargarBodegas(")
        fn = js[i:js.index("\nfunction ", i)]
        assert "rba-bod-resumen" in fn and "¿Dónde está este pedido según Check?" in fn
        assert "Está repartido en " in fn and "sin stock en Check" in fn          # varias bodegas + los que Check no tiene
        assert "ln-bod-varias" in fn and "u. en total" in fn                       # producto en varias bodegas: su total
        assert fn.index("rba-bod-resumen") < fn.index("cajas.forEach(c => {\n")         # el resumen va arriba de los productos
        with open(os.path.join(RAIZ, "static", "retiros_internal_detail.css"), encoding="utf-8") as f:
            assert ".rba-bod-resumen{" in f.read()

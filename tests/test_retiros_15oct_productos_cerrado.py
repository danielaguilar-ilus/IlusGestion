# -*- coding: utf-8 -*-
"""2026-10-15 · Retiro ya entregado que seguía en rojo (captura de Daniel, retiro 105).

La tarjeta «Productos a retirar» compara con el saldo ACTUAL del ERP. Con el retiro ya retirado, la guía de despacho de ESE retiro dejó el
saldo en 0 y la ficha decía «No se puede entregar todavía · N productos con problema» y «Ya salió: no entregar». Daniel: «esto tiene que
congelarse». En un retiro terminado la tarjeta dice lo que pasó; en uno abierto todo sigue igual.

Sin BD real, sin ERP real (consulta de mentira), sin correo:   py -m pytest tests/test_retiros_15oct_productos_cerrado.py -q
"""
import datetime as dt
import os
import sys
from types import SimpleNamespace

import pytest

_TESTS = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _TESTS)
sys.path.insert(0, os.path.dirname(_TESTS))

import _arnes_retiros as A  # noqa: E402
import pickups_module  # noqa: E402

RID = 1
CREADO_UTC = dt.datetime(2026, 10, 13, 15, 0)      # 13/10/2026 12:00 hora Chile


class ERPFalso:
    """Responde las 4 consultas de _prod_erp_doc (SOLO LECTURA). `guia` = fecha de la guía que consumió la línea (o None)."""

    def __init__(self, saldo=0.0, guia=dt.date(2026, 10, 14)):
        self.saldo, self.guia = saldo, guia

    def __call__(self, sql, params=(), max_rows=None):
        s = " ".join(sql.split())
        if "FROM MAEEDO e WHERE e.TIDO" in s:
            return [{"IDMAEEDO": 9, "TIDO": "BLV", "NUDO": "0000023732", "ENDO": "E1", "FEEMDO": dt.date(2026, 10, 9)}]
        if "FROM MAEDDO d WHERE d.IDMAEEDO" in s:
            return [{"NULIDO": 1, "sku": "DISCO25", "nombre": "Set Discos 2,5 - 5 kg", "caprco": 2, "caprad": 2 - self.saldo,
                     "caprex": 0, "caprnc": 0, "eslido": "", "bodega": "02"}]
        if "WHERE d.TIDOPA" in s:
            return ([{"origen_linea": 1, "tido": "GDV", "nudo": "0000033758", "sku": "DISCO25", "cantidad": 2,
                      "fecha": self.guia, "estado": ""}] if self.guia else [])
        if "FROM MAEST" in s:
            return [{"sku": "DISCO25", "bodega": "02", "fisico": 11, "devengado": 0, "comprometido": 0}]
        return []


def armar(monkeypatch, estado, erp, **kw):
    for k in ("RETIROS_PREP_AUTO", "RETIROS_EXIGE_RESPONSABLE", "RETIROS_CHECK_AUTO", "RETIROS_RETIRO_AUTO"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setattr(pickups_module, "_RELOJ_CHILE", lambda: dt.datetime(2026, 10, 15, 9, 0))
    app, db, ctx, esp = A.construir_app(extra={"_random_sql_query": erp})
    db.nueva_solicitud(RID, status=estado, responsable_user_id=7, responsable_nombre="Sam", created_at=CREADO_UTC,
                       confirmed_date="2026-10-14", **kw)
    db.agregar_doc(RID, "BLV", "0000023732", snapshot={"lineas": [{"sku": "DISCO25", "descripcion_erp": "Set Discos", "cantidad": 2}]})
    db.reiniciar_registro()
    return SimpleNamespace(app=app, db=db, esp=esp, cli=app.test_client())


def diag(env):
    r = env.cli.get(f"/retiros/{RID}/productos-erp")
    assert r.status_code == 200
    d = r.get_json()
    assert d["ok"]
    return d, d["docs"][0]["lineas"][0]


def sin_mensajes(env):
    A.esperar_hilos_de_aviso()
    assert env.esp.correo.call_args_list == [] and env.esp.whatsapp.call_count == 0


class TestRetiroEntregado:
    def test_el_retiro_105_ya_no_sale_en_rojo_sino_entregado_con_su_guia(self, monkeypatch):
        env = armar(monkeypatch, "retirada", ERPFalso(saldo=0, guia=dt.date(2026, 10, 14)), retirado_por_nombre="Ana")
        d, l = diag(env)
        assert (l["estado"], l["estado_txt"], l["tono"]) == ("entregado_retiro", "Entregado en este retiro", "ok")
        assert l["consumos"][0]["numero"] == "33758" and l["consumos"][0]["fecha"] == "14/10/2026"
        v = d["veredicto"]
        assert v["estado"] == "entregado" and v["titulo"] == "Retiro completado"
        assert "Guía de despacho 33758 · 14/10/2026" in v["detalle"] and "No se puede entregar" not in v["titulo"]
        assert d["contadores"]["bloqueados"] == 0 and d["contadores"]["entregados"] == 1
        sin_mensajes(env)

    def test_cerrada_con_quien_retiro_tambien_es_entregada(self, monkeypatch):
        env = armar(monkeypatch, "cerrada", ERPFalso(), retirado_por_nombre="Ana")
        d, l = diag(env)
        assert l["estado"] == "entregado_retiro" and d["veredicto"]["estado"] == "entregado"

    def test_si_la_guia_es_anterior_al_retiro_dice_que_ya_habia_salido(self, monkeypatch):
        env = armar(monkeypatch, "retirada", ERPFalso(saldo=0, guia=dt.date(2026, 10, 1)))
        _d, l = diag(env)
        assert (l["estado"], l["tono"]) == ("salio_antes", "gris") and l["estado_txt"] == "Ya había salido antes de este retiro"

    def test_sin_guia_todavia_queda_entregado_con_un_aviso_neutro(self, monkeypatch):
        env = armar(monkeypatch, "retirada", ERPFalso(saldo=2, guia=None))
        _d, l = diag(env)
        assert l["estado"] == "entregado_retiro" and l["tono"] == "ok"
        assert l["avisos"] == ["El ERP todavía no registra la guía de despacho de esta entrega."]


class TestRetiroCerradoSinEntrega:
    @pytest.mark.parametrize("estado", ["rechazada", "fallida"])
    def test_rechazado_o_fallido_no_alarma(self, monkeypatch, estado):
        env = armar(monkeypatch, estado, ERPFalso())
        d, l = diag(env)
        assert (l["estado"], l["tono"]) == ("sin_entrega", "gris")
        assert d["veredicto"]["estado"] == "cerrado" and d["contadores"]["bloqueados"] == 0


class TestRetiroAbiertoSigueIgual:
    @pytest.mark.parametrize("estado", ["agenda_confirmada", "en_preparacion", "en_revision"])
    def test_sin_saldo_sigue_en_rojo_y_bloqueado(self, monkeypatch, estado):
        env = armar(monkeypatch, estado, ERPFalso(saldo=0))
        d, l = diag(env)
        assert (l["estado"], l["tono"]) == ("sin_saldo", "rojo") and l["estado_txt"] == "Ya salió: no entregar"
        assert d["veredicto"]["estado"] == "bloqueado"

    def test_con_saldo_sigue_listo(self, monkeypatch):
        env = armar(monkeypatch, "agenda_confirmada", ERPFalso(saldo=2, guia=None))
        d, l = diag(env)
        # sin ficha logística en la muestra → «Revisar» en ámbar, igual que antes: lo que importa es que un retiro abierto no cambió
        assert l["estado"] in ("ok", "revisar") and l["tono"] != "rojo" and d["veredicto"]["estado"] in ("ok", "revisar")


class TestFicha:
    def test_el_js_conoce_los_estados_congelados(self):
        with open(os.path.join(os.path.dirname(_TESTS), "static", "retiros_internal_detail.js"), encoding="utf-8") as f:
            js = f.read()
        for k in ("entregado: { cls: 'is-ok'", "cerrado:   { cls: 'is-vacio'", "entregado_retiro: 'bi-check2-circle'", "c.entregados"):
            assert k in js
        with open(os.path.join(os.path.dirname(_TESTS), "static", "retiros_guia.js"), encoding="utf-8") as f:
            assert "STATUS === 'retirada' || (STATUS === 'cerrada' && !(G && G.terminal))" in f.read()

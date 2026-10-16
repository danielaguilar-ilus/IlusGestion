# -*- coding: utf-8 -*-
"""2026-10-16 · «Alertar, no trabar» (Daniel, caso RET-XQHPMT de HD LAB TRAINING: 10,53 m³ en un bloque de 5 m³; Juan no podía agendar).

Peso y volumen sobre el tope del bloque ya NO bloquean proponer la fecha: avisan («Pedido grande o bloque sobrecargado») y queda en la
bitácora. Lo que sí es un choque de agenda (bloque lleno por cantidad de retiros) sigue frenando.

    py -m pytest tests/test_retiros_16oct_capacidad_aviso.py -q
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

RID = 1
FECHA = {"date": "2026-10-21", "time_from": "10:00", "time_to": "10:30"}       # miércoles hábil


@pytest.fixture()
def env(monkeypatch):
    for k in ("RETIROS_PREP_AUTO", "RETIROS_EXIGE_RESPONSABLE", "RETIROS_CHECK_AUTO", "RETIROS_RETIRO_AUTO"):
        monkeypatch.delenv(k, raising=False)
    app, db, ctx, esp = A.construir_app()
    return SimpleNamespace(app=app, db=db, esp=esp, cli=app.test_client())


def retiro(env, **kw):
    datos = dict(status="en_revision", responsable_user_id=7, responsable_nombre="Juan", document_number="VD00010930")
    datos.update(kw)
    env.db.nueva_solicitud(RID, **datos)
    env.db.agregar_doc(RID, "VD", "VD00010930")
    env.db.reiniciar_registro()


class TestPedidoGrande:
    def test_un_pedido_que_supera_el_volumen_del_bloque_se_puede_proponer_con_aviso(self, env):
        retiro(env, total_volume_m3=10.53, total_weight_kg=1919)
        r = env.cli.post(f"/retiros/{RID}/proposal", data=FECHA, headers={"Accept": "application/json", "X-Requested-With": "XMLHttpRequest"})
        d = r.get_json()
        assert r.status_code == 200 and d["ok"] is True, d
        assert "Pedido grande o bloque sobrecargado" in d["message"] and "Coordina con bodega" in d["message"]
        assert "volumen" in d["message"] and "peso" in d["message"]
        logs = [l for l in env.db.logs if l["action"] == "capacidad_excedida"]
        assert len(logs) == 1 and "10.53" in logs[0]["notes"]
        assert not [l for l in env.db.logs if l["action"] == "propuesta_bloqueada"]

    def test_un_pedido_normal_no_lleva_aviso(self, env):
        retiro(env, total_volume_m3=0.4, total_weight_kg=42)
        d = env.cli.post(f"/retiros/{RID}/proposal", data=FECHA, headers={"Accept": "application/json", "X-Requested-With": "XMLHttpRequest"}).get_json()
        assert d["ok"] is True and "Pedido grande" not in d["message"]
        assert not [l for l in env.db.logs if l["action"] == "capacidad_excedida"]


class TestCodigo:
    def test_el_bloque_lleno_por_cantidad_sigue_frenando_y_el_peso_volumen_solo_avisan(self):
        with open(os.path.join(RAIZ, "pickups_module.py"), encoding="utf-8") as f:
            src = f.read().replace("\r\n", "\n")
        i = src.index("    def _validar_disponibilidad_slot(")
        blk = src[i:src.index("\n    def ", i + 10)]
        assert 'return False, f"Slot lleno:' in blk
        assert "Capacidad de volumen excedida" not in blk and "Capacidad de peso excedida" not in blk
        assert 'return True, " ".join(_aviso_cap)' in blk
        # el cliente que acepta la propuesta tampoco queda frenado por peso/volumen (bajo el lock)
        assert 'confirm_motivo = f"Capacidad de volumen excedida' not in src and 'confirm_motivo = f"Capacidad de peso excedida' not in src

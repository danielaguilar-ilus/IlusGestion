# -*- coding: utf-8 -*-
"""2026-10-07 · Peso / volumen que no se guardaban (RET-VQJ58N: la ficha mostraba 16 kg y el Monitor 0 kg).

Causas confirmadas leyendo el código:
  1. El Monitor (y las tarjetas) pintan `total_weight_kg` / `total_volumetric_weight`, que el formulario público deja en 0, aunque el retiro ya
     tuviera `peso_real_kg` / `peso_vol_kg`.
  2. El peso y los m³ de los PRODUCTOS solo se guardaban al abrir la ficha o al tocar los documentos: un retiro que nadie abría seguía en 0.
  3. La franja «HOY» sumaba solo `total_weight_kg` / `total_volumetric_weight`.

Todo corre sin BD real, sin Check, sin ERP y con el correo de mentira (tests/_arnes_retiros.py):

    py -m pytest tests/test_retiros_07oct_totales.py -q
"""
import datetime as dt
import os
import re
import sys
from types import SimpleNamespace
from unittest import mock

import flask
import pytest

_TESTS = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _TESTS)
sys.path.insert(0, os.path.dirname(_TESTS))

import _arnes_retiros as A  # noqa: E402
import pickups_module  # noqa: E402

RID = 1
AHORA = dt.datetime(2026, 10, 7, 11, 0)
CLIENTE = "cliente.real@example.com"
LINEAS = {"lineas": [
    {"sku": "DISCO25", "descripcion_erp": "Set Discos 2,5 - 5 kg", "cantidad": 2, "peso_kg_u": 8, "vol_u": 2500, "peso_vol_u": 9}]}   # 16 kg · 0,005 m³


@pytest.fixture()
def env(monkeypatch):
    for k in ("RETIROS_PREP_AUTO", "RETIROS_EXIGE_RESPONSABLE", "RETIROS_CHECK_AUTO", "RETIROS_RETIRO_AUTO"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setattr(pickups_module, "_RELOJ_CHILE", lambda: AHORA)
    app, db, ctx, esp = A.construir_app()
    return SimpleNamespace(app=app, db=db, ctx=ctx, esp=esp, cli=app.test_client())


def retiro(env, **kw):
    env.db.nueva_solicitud(RID, status="agenda_confirmada", responsable_user_id=7, responsable_nombre="Sam",
                           confirmed_date="2026-10-08", document_number="0000023732", **kw)
    env.db.agregar_doc(RID, "BLV", "0000023732", snapshot=LINEAS)
    env.db.reiniciar_registro()


def logs_totales(env):
    return [x for x in env.db.logs if x["action"] == "totales_sincronizados"]


def sin_mensajes(env):
    A.esperar_hilos_de_aviso()
    assert env.esp.correo.call_args_list == [] and env.esp.whatsapp.call_count == 0


class TestProductosErpCompletaLosTotales:
    def test_el_endpoint_de_productos_completa_lo_que_esta_en_cero(self, env):
        retiro(env, peso_real_kg=0, peso_vol_kg=0, total_volume_m3=0)
        d = env.cli.get(f"/retiros/{RID}/productos-erp").get_json()
        assert d["ok"] and d["totales"]["peso_total_kg"] == 16.0
        fila = env.db.solicitudes[RID]
        assert fila["peso_real_kg"] == 16.0 and fila["peso_vol_kg"] == 18.0 and fila["total_volume_m3"] == pytest.approx(0.005)
        (e,) = logs_totales(env)
        assert e["actor_name"] == "ILUS (automático)" and "0 →" in e["notes"]
        sin_mensajes(env)
        assert fila["status"] == "agenda_confirmada"

    def test_una_sola_entrada_en_la_bitacora_aunque_se_cargue_varias_veces(self, env):
        retiro(env, peso_real_kg=0, peso_vol_kg=0, total_volume_m3=0)
        for _ in range(4):
            env.cli.get(f"/retiros/{RID}/productos-erp")
        assert len(logs_totales(env)) == 1

    def test_nunca_pisa_un_valor_puesto_por_una_persona(self, env):
        retiro(env, peso_real_kg=50, peso_vol_kg=0, total_volume_m3=0)
        env.cli.get(f"/retiros/{RID}/productos-erp")
        fila = env.db.solicitudes[RID]
        assert fila["peso_real_kg"] == 50 and fila["peso_vol_kg"] == 18.0
        (e,) = logs_totales(env)
        assert "peso_real_kg" not in e["notes"]

    def test_si_ya_estan_los_tres_no_escribe_nada(self, env):
        retiro(env, peso_real_kg=16, peso_vol_kg=18, total_volume_m3=0.005)
        env.cli.get(f"/retiros/{RID}/productos-erp")
        assert logs_totales(env) == []
        assert not [e for e in env.db.escrituras if e[0].startswith("UPDATE `pickup_requests`")]

    def test_si_los_productos_no_traen_medidas_no_inventa_nada(self, env):
        env.db.nueva_solicitud(RID, status="agenda_confirmada", peso_real_kg=0, peso_vol_kg=0, total_volume_m3=0)
        env.db.agregar_doc(RID, "BLV", "0000023732", snapshot={"lineas": [{"sku": "X", "descripcion_erp": "Sin ficha", "cantidad": 1}]})
        env.db.reiniciar_registro()
        env.cli.get(f"/retiros/{RID}/productos-erp")
        assert logs_totales(env) == [] and env.db.solicitudes[RID]["peso_real_kg"] == 0

    def test_si_otro_proceso_ya_lo_completo_no_deja_una_segunda_entrada(self, env):
        """La ficha y el endpoint corren a la vez: el UPDATE exige «sigue en 0», así que solo uno escribe y solo uno deja la bitácora."""
        retiro(env, peso_real_kg=0, peso_vol_kg=0, total_volume_m3=0)
        def otro_proceso_escribe():                 # justo después de leer el retiro, el otro proceso completa los totales
            env.db.solicitudes[RID].update(peso_real_kg=16.0, peso_vol_kg=18.0, total_volume_m3=0.005)
            env.db.agregar_log(RID, "totales_sincronizados", "agenda_confirmada", "otro proceso")
        env.db.al_leer.append((r"select status, peso_real_kg", otro_proceso_escribe))
        env.cli.get(f"/retiros/{RID}/productos-erp")
        assert len(logs_totales(env)) == 1 and logs_totales(env)[0]["notes"] == "otro proceso"


def abrir_monitor(env):
    original = env.db._fetchall

    def con_filas(sql, params=()):
        if re.match(r"^select id, code, status, document_type", re.sub(r"\s+", " ", sql).strip(), re.I):
            return [dict(r) for r in env.db.solicitudes.values()]
        return original(sql, params)
    env.db._fetchall = con_filas
    with mock.patch.object(pickups_module, "render_template", return_value="ok") as rt:
        with env.app.test_request_context("/retiros"):
            flask.g.user = {"id": 7, "nombre": "Sam", "username": "sam@sphs.cl"}
            env.app.view_functions["pickup_dashboard"]()
    return rt


class TestMonitor:
    def test_la_fila_muestra_los_kg_reales_y_no_cero(self, env):
        retiro(env, peso_real_kg=16, peso_vol_kg=18, total_volume_m3=0.005, total_weight_kg=0, total_volumetric_weight=0)
        rt = abrir_monitor(env)
        (fila,) = rt.call_args.kwargs["rows"]
        assert fila["total_weight_kg"] == 16.0 and fila["total_volumetric_weight"] == 18.0      # lo que pintan las plantillas
        assert env.db.solicitudes[RID]["total_weight_kg"] == 0                                    # y nada se guardó

    def test_la_franja_hoy_suma_el_peso_real_con_respaldo_en_el_del_formulario(self, env):
        retiro(env, peso_real_kg=16, peso_vol_kg=18, total_volume_m3=0.005)
        abrir_monitor(env)
        sql = [s for s, _ in env.db.consultas if "AS bultos" in s]
        assert sql, "no se pidió la franja HOY"
        assert "COALESCE(NULLIF(peso_real_kg,0), total_weight_kg)" in sql[0]
        assert "COALESCE(NULLIF(peso_vol_kg,0), total_volumetric_weight)" in sql[0]

    def test_un_retiro_que_nadie_abrio_se_completa_al_cargar_el_monitor_sin_mandar_nada(self, env):
        retiro(env, peso_real_kg=0, peso_vol_kg=0, total_volume_m3=0, total_weight_kg=0)
        rt = abrir_monitor(env)
        assert env.db.solicitudes[RID]["peso_real_kg"] == 16.0
        (fila,) = rt.call_args.kwargs["rows"]
        assert fila["total_weight_kg"] == 16.0
        assert len(logs_totales(env)) == 1
        sin_mensajes(env)

    def test_el_monitor_no_insiste_con_los_retiros_que_no_tienen_con_que_completarse(self, env):
        env.db.nueva_solicitud(RID, status="agenda_confirmada", document_number="1", peso_real_kg=0, peso_vol_kg=0, total_volume_m3=0)
        env.db.agregar_doc(RID, "BLV", "0000023732", snapshot={"lineas": [{"sku": "X", "descripcion_erp": "Sin ficha", "cantidad": 1}]})
        env.db.reiniciar_registro()
        abrir_monitor(env)
        n1 = len([s for s, _ in env.db.consultas if "FROM pickup_request_docs" in s])
        abrir_monitor(env)
        n2 = len([s for s, _ in env.db.consultas if "FROM pickup_request_docs" in s])
        assert n2 == n1, "la segunda carga volvió a consultar los productos de un retiro que no tiene datos"

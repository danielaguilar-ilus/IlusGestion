# -*- coding: utf-8 -*-
"""2026-10-09 (Daniel, con BLV 23732 de Gerd Müller como modelo): «habíamos quedado que cuando se asignara el picking se iba a gestionar la
preparación, y cuando se integrara y se expidiera, la entrega… al meterme a otros pedidos queda como en picking de material».

  · El registro de Check de un retiro cerrado hace pocos días se completa con la OT de CONTROL DE SALIDA (la ficha y el cron). Solo se agrega:
    lo guardado nunca se pierde. Nunca cambia el estado ni le escribe a nadie. Check solo se consulta (REGLA #4.4).
  · La expedición cierra el retiro en la JORNADA de la bodega (07:30–20:00, días hábiles); «Enviar a preparación» sigue en la cobertura (REGLA #20).

Todo corre sin BD real, sin Check, sin ERP y con el correo de mentira (tests/_arnes_retiros.py):

    py -m pytest tests/test_retiros_09oct_registro_salida.py -q
"""
import datetime as dt
import json
import os
import sys
import threading
import time
from types import SimpleNamespace

import pytest

_TESTS = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _TESTS)
sys.path.insert(0, os.path.dirname(_TESTS))

import _arnes_retiros as A  # noqa: E402
import pickups_module  # noqa: E402

RID = 1
CLIENTE = "cliente.real@example.com"
AHORA = dt.datetime(2026, 10, 7, 11, 0)          # miércoles 11:00: hábil, dentro de la cobertura y de la jornada de la bodega
PICKING = {"ot": "481516", "tipoOT": "PICKING", "estado": "TERMINADA", "doc": "BLV-0000023732", "feInicioOT": "2026-10-07T08:11:10",
           "fechaFin": "2026-10-07T08:26:55", "usuarioPicking": "LBOURRE", "codigo": "MANC10", "descripcion": "Mancuerna 10 kg", "sol": 3, "ejec": 3}
SALIDA = {"ot": "CSAL000123", "tipoOT": "Control Salida / Integraciones", "estado": "FINALIZADA", "doc": "BLV-0000023732",
          "feInicioOT": "2026-10-07T10:50:10", "fechaFin": "2026-10-07T10:56:12", "usuarioSalida": "JPEREZ", "codigo": "MANC10", "sol": 3, "ejec": 3}
EXPEDIDO = A.respuesta_check(A.fila_check(solicitado=3, despachado=3))
EMPEZO = A.respuesta_check(A.fila_check(solicitado=3, pickeado=1))


@pytest.fixture()
def env(monkeypatch):
    monkeypatch.setenv("RETIROS_CHECK_CONFIRMACION_S", "0")
    monkeypatch.setenv("ILUS_CRON_TOKEN", "secreto-cron")
    for k in ("RETIROS_RETIRO_AUTO", "RETIROS_PREP_AUTO", "RETIROS_PREP_AUTO_DIAS", "RETIROS_CHECK_AUTO", "RETIROS_EXIGE_RESPONSABLE",
              "RETIROS_COBERTURA_DESDE", "RETIROS_COBERTURA_HASTA", "RETIROS_JORNADA_BODEGA"):
        monkeypatch.delenv(k, raising=False)
    reloj = {"ahora": AHORA}
    monkeypatch.setattr(pickups_module, "_RELOJ_CHILE", lambda: reloj["ahora"])
    app, db, ctx, esp = A.construir_app()
    ctx["_comm_render_email_document"] = lambda asunto, cuerpo: cuerpo
    db.admins = [{"id": 11}]
    return SimpleNamespace(app=app, db=db, ctx=ctx, esp=esp, cli=app.test_client(), reloj=reloj)


def esperar():
    t0 = time.time()
    while time.time() - t0 < 5:
        vivos = [t for t in threading.enumerate() if t.name.startswith(("pickup-notify", "pickup-team-notify"))]
        if not vivos:
            return
        for t in vivos:
            t.join(0.2)


def correos_al_cliente(env):
    esperar()
    return [c for c in env.esp.correo.call_args_list if (c.args[0] or "").lower() == CLIENTE]


def sin_mensajes(env):
    esperar()
    assert env.esp.correo.call_args_list == [] and env.esp.whatsapp.call_count == 0


def logs(env, accion):
    return [x for x in env.db.logs if x["request_id"] == RID and x["action"] == accion]


def retiro(env, status="en_preparacion", **kw):
    kw.setdefault("responsable_user_id", 7)
    kw.setdefault("responsable_nombre", "Milagros")
    kw.setdefault("confirmed_date", AHORA.date().isoformat())
    env.db.nueva_solicitud(RID, status=status, **kw)
    env.db.agregar_doc(RID, "BLV", "0000023732")
    env.db.reiniciar_registro()


def cache(env, filas, edad_s=0, traer=None):
    env.ctx["_CHECKWMS_TRAZA"] = {"ts": time.time() - edad_s, "rows": filas, "lock": threading.Lock()}
    llamadas = []

    def _traer(forzar=False):
        llamadas.append(forzar)
        return traer(env) if traer else env.ctx["_CHECKWMS_TRAZA"]["rows"]
    env.ctx["_checkwms_trazabilidad_rows"] = _traer
    return llamadas


def actividad(env):
    r = env.cli.get(f"/retiros/{RID}/check-actividad")
    assert r.status_code == 200
    return r.get_json()


def ots_guardadas(env):
    return {o["ot"] for d in json.loads(env.db.snapshots[RID]["payload"])["documentos"] for o in d["ots"]}


def cerrar(env, hace=dt.timedelta(hours=2), status="retirada"):
    """El retiro se cierra con el registro de Check que se guardó mientras se preparaba (solo el picking)."""
    env.db.solicitudes[RID]["status"] = status
    env.db.solicitudes[RID]["closed_at"] = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None) - hace
    env.db.reiniciar_registro()


def preparado_y_cerrado(env, hace=dt.timedelta(hours=2), status="retirada"):
    retiro(env)
    cache(env, [PICKING])
    actividad(env)                                  # mientras se preparaba: queda guardado solo el picking
    assert ots_guardadas(env) == {"481516"}
    cerrar(env, hace, status)


def cron(env, query=""):
    return env.cli.get("/retiros/cron/check-barrido" + query, headers={"X-Cron-Token": "secreto-cron"}).get_json()


# ══════════════════════════════════════════════════════════════════════════════
#  La ficha de un retiro cerrado completa el registro con la OT de control de salida
# ══════════════════════════════════════════════════════════════════════════════
class TestFichaCompletaElRegistro:
    @pytest.mark.parametrize("status", ["retirada", "cerrada"])
    def test_cerrado_hace_poco_agrega_la_salida_a_lo_guardado(self, env, status):
        preparado_y_cerrado(env, status=status)
        llamadas = cache(env, [PICKING, SALIDA])          # Check ya informa la expedición
        d = actividad(env)
        assert d["estado"] == "listo" and d["desde_registro"] is True
        assert {o["ot"] for o in d["documentos"][0]["ots"]} == {"481516", "CSAL000123"}
        assert ots_guardadas(env) == {"481516", "CSAL000123"}
        assert llamadas == [] and env.esp.check.llamadas == []      # el reporte en memoria estaba fresco: no se le pidió nada a Check
        assert env.db.solicitudes[RID]["status"] == status
        sin_mensajes(env)

    def test_lo_guardado_nunca_se_pierde_aunque_check_ya_no_traiga_el_picking(self, env):
        preparado_y_cerrado(env)
        cache(env, [SALIDA])
        d = actividad(env)
        assert {o["ot"] for o in d["documentos"][0]["ots"]} == {"481516", "CSAL000123"}

    def test_cerrado_hace_mas_de_una_semana_solo_se_muestra_lo_guardado(self, env):
        preparado_y_cerrado(env, hace=dt.timedelta(days=8))
        llamadas = cache(env, [PICKING, SALIDA], edad_s=5000)
        d = actividad(env)
        assert {o["ot"] for o in d["documentos"][0]["ots"]} == {"481516"} and d["refrescando"] is False
        assert llamadas == [] and [e for e in env.db.escrituras if "pickup_check_snapshots" in e[0].lower()] == []

    def test_sin_fecha_de_cierre_solo_se_muestra_lo_guardado(self, env):
        preparado_y_cerrado(env)
        env.db.solicitudes[RID]["closed_at"] = None
        llamadas = cache(env, [PICKING, SALIDA], edad_s=5000)
        assert {o["ot"] for o in actividad(env)["documentos"][0]["ots"]} == {"481516"}
        assert llamadas == []

    def test_si_ya_tiene_la_salida_no_vuelve_a_mirar(self, env):
        retiro(env)
        cache(env, [PICKING, SALIDA])
        actividad(env)
        cerrar(env)
        llamadas = cache(env, None, edad_s=5000)
        d = actividad(env)
        assert d["desde_registro"] is True and d["refrescando"] is False and llamadas == []

    def test_si_check_esta_trayendo_el_reporte_la_ficha_lo_dice_para_volver_a_preguntar(self, env):
        preparado_y_cerrado(env)
        suelta = threading.Event()

        def _lento(env_):
            suelta.wait(5)
            env_.ctx["_CHECKWMS_TRAZA"].update({"ts": time.time(), "rows": [PICKING, SALIDA]})
            return env_.ctx["_CHECKWMS_TRAZA"]["rows"]
        llamadas = cache(env, [PICKING], edad_s=5000, traer=_lento)
        d = actividad(env)
        assert d["desde_registro"] is True and d["refrescando"] is True
        assert {o["ot"] for o in d["documentos"][0]["ots"]} == {"481516"}
        suelta.set()
        t0 = time.time()
        while time.time() - t0 < 5 and actividad(env)["refrescando"]:
            time.sleep(0.05)
        d = actividad(env)                                 # la siguiente consulta de la ficha ya trae la salida
        assert {o["ot"] for o in d["documentos"][0]["ots"]} == {"481516", "CSAL000123"} and d["refrescando"] is False
        assert llamadas == [True]
        sin_mensajes(env)

    def test_la_ficha_vuelve_a_preguntar_mientras_el_servidor_trae_la_salida(self):
        js = open(os.path.join(os.path.dirname(_TESTS), "static", "retiros_guia.js"), encoding="utf-8").read()
        assert "if (esRetirado() && ACT && ACT.estado === 'listo' && !ACT.refrescando) return;" in js
        assert "ACT.desde_registro && ACT.refrescando && ACT_REINTENTOS < 8" in js


# ══════════════════════════════════════════════════════════════════════════════
#  El cron completa el registro aunque nadie abra la ficha
# ══════════════════════════════════════════════════════════════════════════════
class TestCronCompletaElRegistro:
    def test_trae_el_reporte_una_vez_y_completa_el_registro(self, env):
        preparado_y_cerrado(env)

        def _trae(env_):
            env_.ctx["_CHECKWMS_TRAZA"].update({"ts": time.time(), "rows": [PICKING, SALIDA]})
            return env_.ctx["_CHECKWMS_TRAZA"]["rows"]
        llamadas = cache(env, [PICKING], edad_s=5000, traer=_trae)
        d = cron(env)
        code = env.db.solicitudes[RID]["code"]
        assert d["registro_check"] == {"pendientes": 1, "completados": [code], "consulto_check": True}
        assert llamadas == [True] and ots_guardadas(env) == {"481516", "CSAL000123"}
        assert env.db.solicitudes[RID]["status"] == "retirada"
        sin_mensajes(env)
        d = cron(env)                                       # ya está completo: no vuelve a pedirle nada a Check
        assert d["registro_check"]["pendientes"] == 0 and llamadas == [True]

    def test_no_vuelve_a_pedir_el_reporte_antes_de_una_hora(self, env):
        preparado_y_cerrado(env)
        llamadas = cache(env, [PICKING], edad_s=5000)       # Check todavía no informa la salida
        cron(env)
        env.ctx["_CHECKWMS_TRAZA"]["ts"] = time.time() - 5000
        d = cron(env)
        assert llamadas == [True] and d["registro_check"]["consulto_check"] is False

    def test_con_el_reporte_fresco_no_le_pide_nada_a_check(self, env):
        preparado_y_cerrado(env)
        llamadas = cache(env, [PICKING, SALIDA])
        d = cron(env)
        assert llamadas == [] and d["registro_check"]["completados"] == [env.db.solicitudes[RID]["code"]]

    def test_cerrado_hace_mas_de_5_dias_no_se_revisa(self, env):
        preparado_y_cerrado(env, hace=dt.timedelta(days=6))
        llamadas = cache(env, [PICKING, SALIDA], edad_s=5000)
        assert cron(env)["registro_check"]["pendientes"] == 0 and llamadas == []
        assert ots_guardadas(env) == {"481516"}

    def test_sin_retiros_cerrados_no_le_pide_nada_a_check(self, env):
        llamadas = cache(env, [PICKING], edad_s=5000)
        assert cron(env)["registro_check"]["pendientes"] == 0 and llamadas == []

    def test_en_dry_no_escribe_nada(self, env):
        preparado_y_cerrado(env)
        llamadas = cache(env, [PICKING, SALIDA], edad_s=5000)
        d = cron(env, "?dry=1")
        assert "registro_check" not in d and llamadas == [] and env.db.escrituras == []

    def test_de_noche_no_hace_nada(self, env):
        preparado_y_cerrado(env)
        env.reloj["ahora"] = dt.datetime(2026, 10, 7, 23, 0)
        llamadas = cache(env, [PICKING, SALIDA], edad_s=5000)
        d = cron(env)
        assert "registro_check" not in d and llamadas == [] and env.db.escrituras == []

    def test_con_el_cierre_automatico_apagado_no_hace_nada(self, env, monkeypatch):
        monkeypatch.setenv("RETIROS_RETIRO_AUTO", "0")
        preparado_y_cerrado(env)
        llamadas = cache(env, [PICKING, SALIDA], edad_s=5000)
        assert "registro_check" not in cron(env) and llamadas == []

    def test_si_algo_falla_el_cron_sigue(self, env):
        preparado_y_cerrado(env)

        def _revienta(env_):
            raise RuntimeError("Check no responde")
        cache(env, [PICKING], edad_s=5000, traer=_revienta)
        d = cron(env)
        assert d["registro_check"]["completados"] == [] and env.db.solicitudes[RID]["status"] == "retirada"


# ══════════════════════════════════════════════════════════════════════════════
#  La expedición cierra en la jornada de la bodega; «Enviar a preparación» sigue en la cobertura
# ══════════════════════════════════════════════════════════════════════════════
class TestJornadaDeLaBodega:
    @pytest.mark.parametrize("ahora", [dt.datetime(2026, 10, 7, 7, 30),          # abre la bodega (antes de la cobertura)
                                       dt.datetime(2026, 10, 7, 13, 30),          # colación del equipo
                                       dt.datetime(2026, 10, 7, 17, 30),          # terminó la cobertura
                                       dt.datetime(2026, 10, 7, 19, 59)])         # último minuto de la bodega
    def test_la_expedicion_cierra_en_tiempo_real(self, env, monkeypatch, ahora):
        monkeypatch.setenv("RETIROS_RETIRO_AUTO", "activo")
        env.reloj["ahora"] = ahora
        retiro(env, confirmed_date=ahora.date().isoformat())
        env.esp.check.respuestas["*"] = EXPEDIDO
        r = env.cli.get(f"/retiros/{RID}/check-preparacion").get_json()
        assert r["retiro_auto_ahora"] is True and env.db.solicitudes[RID]["status"] == "retirada"
        assert len(correos_al_cliente(env)) == 1

    def test_la_jornada_se_puede_cambiar_por_entorno(self, env, monkeypatch):
        monkeypatch.setenv("RETIROS_RETIRO_AUTO", "activo")
        monkeypatch.setenv("RETIROS_JORNADA_BODEGA", "08:00-18:00")
        env.reloj["ahora"] = dt.datetime(2026, 10, 7, 18, 30)
        retiro(env)
        env.esp.check.respuestas["*"] = EXPEDIDO
        assert env.cli.get(f"/retiros/{RID}/check-preparacion").get_json()["retiro_auto_ahora"] is False
        assert env.db.solicitudes[RID]["status"] == "en_preparacion"

    def test_a_las_18_el_picking_espera_la_cobertura_y_la_expedicion_cierra(self, env, monkeypatch):
        # Caso real RET-HYYW7J (BLV 24060): picking 18:01–18:13, fuera de la cobertura
        monkeypatch.setenv("RETIROS_RETIRO_AUTO", "activo")
        env.reloj["ahora"] = dt.datetime(2026, 10, 7, 18, 5)
        retiro(env, status="agenda_confirmada")
        env.esp.check.respuestas["*"] = EMPEZO
        d = cron(env)
        assert d["en_horario"] is False and d["en_jornada_bodega"] is True and d["pasaron"] == [] and d["con_senal"] == []
        assert env.db.solicitudes[RID]["status"] == "agenda_confirmada" and correos_al_cliente(env) == []
        env.reloj["ahora"] = dt.datetime(2026, 10, 7, 18, 30)       # el cliente llega y bodega expide
        reloj_real = time.time
        monkeypatch.setattr(time, "time", lambda: reloj_real() + 1500)   # pasaron 25 min: la memoria de 45 s de Check ya venció
        env.esp.check.respuestas["*"] = EXPEDIDO
        env.ctx["_CHECKWMS_TRAZA"] = {"ts": time.time(), "rows": [PICKING, SALIDA], "lock": threading.Lock()}
        d = cron(env)
        code = env.db.solicitudes[RID]["code"]
        assert d["expedidos_actuaron"] == [code] and env.db.solicitudes[RID]["status"] == "retirada"
        prep, ret = logs(env, "estado_actualizado")
        assert prep["new_status"] == "en_preparacion" and ret["new_status"] == "retirada"
        (c,) = correos_al_cliente(env)                               # solo «Retiro completado»
        assert "prepar" not in (c.args[1] or "").lower()

    def test_sombra_tambien_avisa_al_equipo_despues_de_la_cobertura(self, env):
        env.reloj["ahora"] = dt.datetime(2026, 10, 7, 18, 30)
        retiro(env)
        env.esp.check.respuestas["*"] = EXPEDIDO
        env.cli.get(f"/retiros/{RID}/check-preparacion")
        assert len(logs(env, "check_expedido")) == 1 and env.db.solicitudes[RID]["status"] == "en_preparacion"
        assert correos_al_cliente(env) == []


# ══════════════════════════════════════════════════════════════════════════════
#  Salvaguarda: un documento que ya no tenía saldo al asociarlo no cierra solo desde «Cita confirmada»
# ══════════════════════════════════════════════════════════════════════════════
class TestDocumentoSinSaldoAlAsociar:
    def _retiro(self, env, status, con_saldo):
        env.db.nueva_solicitud(RID, status=status, responsable_user_id=7, responsable_nombre="Milagros", confirmed_date=AHORA.date().isoformat())
        env.db.agregar_doc(RID, "BLV", "0000023732", con_saldo=con_saldo)
        env.db.reiniciar_registro()

    def test_con_la_cita_confirmada_avisa_al_equipo_y_no_le_escribe_al_cliente(self, env, monkeypatch):
        monkeypatch.setenv("RETIROS_RETIRO_AUTO", "activo")
        self._retiro(env, "agenda_confirmada", 0)
        env.esp.check.respuestas["*"] = EXPEDIDO
        assert env.cli.get(f"/retiros/{RID}/check-preparacion").get_json()["retiro_auto_ahora"] is True
        assert env.db.solicitudes[RID]["status"] == "agenda_confirmada" and logs(env, "estado_actualizado") == []
        (e,) = logs(env, "check_expedido")
        assert "ya no tenía saldo" in e["notes"] and "BLV 0000023732" in e["notes"]
        assert correos_al_cliente(env) == []
        for _ in range(3):                                   # el aviso al equipo sale una sola vez
            env.cli.get(f"/retiros/{RID}/check-preparacion")
        assert len(logs(env, "check_expedido")) == 1

    @pytest.mark.parametrize("con_saldo", [1, None])
    def test_con_saldo_o_sin_verificar_si_cierra(self, env, monkeypatch, con_saldo):
        monkeypatch.setenv("RETIROS_RETIRO_AUTO", "activo")
        self._retiro(env, "agenda_confirmada", con_saldo)
        env.esp.check.respuestas["*"] = EXPEDIDO
        env.cli.get(f"/retiros/{RID}/check-preparacion")
        assert env.db.solicitudes[RID]["status"] == "retirada" and len(correos_al_cliente(env)) == 1

    def test_en_preparacion_si_cierra_como_siempre(self, env, monkeypatch):
        monkeypatch.setenv("RETIROS_RETIRO_AUTO", "activo")
        self._retiro(env, "en_preparacion", 0)
        env.esp.check.respuestas["*"] = EXPEDIDO
        env.cli.get(f"/retiros/{RID}/check-preparacion")
        assert env.db.solicitudes[RID]["status"] == "retirada" and len(correos_al_cliente(env)) == 1


# ══════════════════════════════════════════════════════════════════════════════
#  Revisión adversarial 2026-10-09: lo que una persona decidió, el automático no lo deshace
# ══════════════════════════════════════════════════════════════════════════════
class TestPersonaDecidio:
    def _retiro(self, env, status, historia=()):
        env.db.nueva_solicitud(RID, status=status, responsable_user_id=7, responsable_nombre="Milagros", confirmed_date=AHORA.date().isoformat())
        env.db.agregar_doc(RID, "BLV", "0000023732")
        for estado in historia:
            env.db.agregar_log(RID, "estado_actualizado", new_status=estado)
        env.db.reiniciar_registro()

    @pytest.mark.parametrize("status, historia", [
        ("agenda_confirmada", ("en_preparacion", "retirada", "agenda_confirmada")),    # se cerró solo y una persona lo reabrió
        ("en_preparacion", ("en_preparacion", "retirada", "en_preparacion")),          # reabierto a «En preparación»
        ("agenda_confirmada", ("en_preparacion", "agenda_confirmada")),                # devuelto desde «En preparación» (REGLA #20)
    ])
    def test_no_se_vuelve_a_cerrar_solo_ni_le_escribe_otra_vez_al_cliente(self, env, monkeypatch, status, historia):
        monkeypatch.setenv("RETIROS_RETIRO_AUTO", "activo")
        self._retiro(env, status, historia)
        env.esp.check.respuestas["*"] = EXPEDIDO
        for _ in range(3):                                              # la ficha abierta pregunta cada minuto
            env.cli.get(f"/retiros/{RID}/check-preparacion")
        assert env.db.solicitudes[RID]["status"] == status
        assert [x for x in env.db.logs if x["action"] == "estado_actualizado" and x["notes"].startswith("Automático")] == []
        (e,) = logs(env, "check_expedido")                              # el equipo sí se entera, una vez
        assert "reabierto o devuelto" in e["notes"]
        assert correos_al_cliente(env) == []

    def test_el_cron_tampoco(self, env, monkeypatch):
        monkeypatch.setenv("RETIROS_RETIRO_AUTO", "activo")
        self._retiro(env, "en_preparacion", ("en_preparacion", "retirada", "en_preparacion"))
        env.esp.check.respuestas["*"] = EXPEDIDO
        cron(env)
        assert env.db.solicitudes[RID]["status"] == "en_preparacion" and correos_al_cliente(env) == []


class TestRegistroSoloDeEsteRetiro:
    def test_las_ot_de_otro_retiro_posterior_con_la_misma_factura_no_se_cuelan(self, env):
        from zoneinfo import ZoneInfo
        preparado_y_cerrado(env)                                        # cerrado hace 2 h
        despues = (dt.datetime.now(ZoneInfo("America/Santiago")) + dt.timedelta(hours=10)).strftime("%Y-%m-%dT%H:%M:%S")
        otro = dict(PICKING, ot="999001", feInicioOT=despues, fechaFin=despues, usuarioPicking="OTRO")
        otra_salida = dict(SALIDA, ot="CSAL999002", feInicioOT=despues, fechaFin=despues)
        cache(env, [PICKING, SALIDA, otro, otra_salida])
        d = actividad(env)
        assert {o["ot"] for o in d["documentos"][0]["ots"]} == {"481516", "CSAL000123"}
        assert ots_guardadas(env) == {"481516", "CSAL000123"}

    def test_el_cron_no_le_pide_nada_a_check_por_un_retiro_cerrado_sin_actividad(self, env):
        retiro(env, status="cerrada", closed_at=dt.datetime.now(dt.timezone.utc).replace(tzinfo=None) - dt.timedelta(hours=1))
        llamadas = cache(env, [PICKING], edad_s=5000)
        d = cron(env)
        assert d["registro_check"]["pendientes"] == 0 and llamadas == []


# ══════════════════════════════════════════════════════════════════════════════
#  Revisión técnica 2026-10-09: robustez del registro y del cron
# ══════════════════════════════════════════════════════════════════════════════
class TestRobustez:
    def test_un_volcado_mas_viejo_no_devuelve_una_ot_terminada_a_en_proceso(self, env):
        retiro(env)
        cache(env, [PICKING])
        actividad(env)                                               # guardada con su fin
        viejo = {k: v for k, v in PICKING.items() if k != "fechaFin"}
        viejo["estado"] = "EN PROCESO"
        cache(env, [viejo])                                          # otro proceso con un volcado anterior
        actividad(env)
        (o,) = json.loads(env.db.snapshots[RID]["payload"])["documentos"][0]["ots"]
        assert o["estado"] == "TERMINADA" and o["fin"]

    def test_si_completar_falla_la_ficha_muestra_lo_guardado(self, env):
        preparado_y_cerrado(env)
        cache(env, [PICKING, SALIDA])
        env.db.falla_si.append((r"from pickup_request_docs", RuntimeError("BD caída")))
        d = actividad(env)
        assert d["estado"] == "listo" and d["desde_registro"] is True and d["refrescando"] is False
        assert {o["ot"] for o in d["documentos"][0]["ots"]} == {"481516"}

    def test_el_indicador_cargando_no_queda_pegado(self, env, monkeypatch):
        preparado_y_cerrado(env)
        import pickups_module as pm

        class HiloQueNoArranca:
            def __init__(self, *a, **kw):
                pass

            def start(self):
                raise RuntimeError("sin hilos")
        cache(env, [PICKING], edad_s=5000)
        monkeypatch.setattr(pm.threading, "Thread", HiloQueNoArranca)
        d = actividad(env)
        assert d["refrescando"] is False

    def test_el_cron_no_descarga_si_la_peticion_ya_va_larga(self, env):
        preparado_y_cerrado(env)
        llamadas = cache(env, [PICKING], edad_s=5000)
        with env.app.app_context():
            res = env.ctx["_retiros_prep_auto_barrido"](max_s=70, t_peticion=time.time() - 130)
        assert res["registro_check"]["consulto_check"] is False and llamadas == []

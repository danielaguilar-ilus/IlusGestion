# -*- coding: utf-8 -*-
"""«Enviar a preparación» AUTOMÁTICO según Check (Daniel 2026-10-02): «necesito que envíes a preparación en automático. Por supuesto, lleva
control de hora, fecha, todo. Y usuario. Internamente… al cliente le va a dar fecha nada más».

Cuando Check muestra que bodega YA EMPEZÓ a juntar el pedido, un retiro con la cita confirmada y cercana pasa solo a «En preparación» por el
mismo camino del botón (checklist de bodega, correo al cliente con la fecha, aviso interno) y la bitácora lo deja como «Automático · Check WMS».

Todo corre sin BD, sin Check, sin ERP y sin correo (tests/_arnes_retiros.py):

    py -m pytest tests/test_retiros_prep_auto.py -q
"""
import datetime as dt
import os
import sys
import threading
import time
from types import SimpleNamespace
from unittest import mock

import pytest

_TESTS = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _TESTS)
sys.path.insert(0, os.path.dirname(_TESTS))

import _arnes_retiros as A  # noqa: E402
import flask  # noqa: E402
import pickups_module  # noqa: E402
from _arnes_retiros import fila_check, respuesta_check  # noqa: E402

RID = 1
DOC = "0000023732"
CLIENTE = "cliente.real@example.com"
EMPEZO = respuesta_check(fila_check(solicitado=3, asignado=2, pickeado=1))        # bodega ya juntó 1 de 3
NO_EMPEZO = respuesta_check(fila_check(solicitado=3, asignado=3))                 # stock reservado, nadie ha juntado nada
YA_DESPACHADO = respuesta_check(fila_check(solicitado=3, pickeado=2, despachado=1))


# «Ahora» de las pruebas (hora Chile, sin zona): miércoles 7-oct-2026 a las 11:00 = día hábil dentro del horario de la bodega. Así los
# resultados no dependen del día ni de la hora en que corren (el envío automático solo actúa en horario de bodega y cuenta días hábiles).
AHORA = dt.datetime(2026, 10, 7, 11, 0)


def hoy_chile():
    return AHORA.date()


def cita(dias):
    return (AHORA.date() + dt.timedelta(days=dias)).isoformat()


@pytest.fixture()
def env(monkeypatch):
    monkeypatch.setenv("RETIROS_CHECK_CONFIRMACION_S", "0")        # sin espera entre las dos lecturas
    monkeypatch.delenv("RETIROS_PREP_AUTO", raising=False)         # encendido (por defecto)
    monkeypatch.delenv("RETIROS_PREP_AUTO_DIAS", raising=False)
    monkeypatch.delenv("RETIROS_CHECK_AUTO", raising=False)
    monkeypatch.delenv("RETIROS_EXIGE_RESPONSABLE", raising=False)
    monkeypatch.delenv("ILUS_CRON_TOKEN", raising=False)
    reloj = {"ahora": AHORA}
    monkeypatch.setattr(pickups_module, "_RELOJ_CHILE", lambda: reloj["ahora"])
    app, db, ctx, esp = A.construir_app()
    return SimpleNamespace(app=app, db=db, ctx=ctx, esp=esp, cli=app.test_client(), reloj=reloj)


def retiro(env, rid=RID, status="agenda_confirmada", dias=1, docs=(DOC,), **kw):
    kw.setdefault("confirmed_date", cita(dias))
    kw.setdefault("responsable_user_id", 7)
    kw.setdefault("responsable_nombre", "Milagros")
    env.db.nueva_solicitud(rid, status=status, **kw)
    for numero in docs:
        env.db.agregar_doc(rid, numero=numero)
    env.db.reiniciar_registro()


def esperar_avisos():
    t0 = time.time()
    while time.time() - t0 < 5:
        vivos = [t for t in threading.enumerate() if t.name.startswith(("pickup-notify", "pickup-team-notify"))]
        if not vivos:
            return
        for t in vivos:
            t.join(0.2)


def correos_al_cliente(env):
    esperar_avisos()
    return [c for c in env.esp.correo.call_args_list if (c.args[0] or "").lower() == CLIENTE]


def cambios_de_estado(env, rid=RID):
    return [x for x in env.db.logs if x["request_id"] == rid and x["action"] == "estado_actualizado"]


def revisar(env, query=""):
    return env.cli.get(f"/retiros/{RID}/check-preparacion{query}").get_json()


# ══════════════════════════════════════════════════════════════════════════════
#  El camino feliz: lo que pasa, lo que se registra y lo que le llega al cliente
# ══════════════════════════════════════════════════════════════════════════════
class TestPasaSolo:
    def test_pasa_a_preparacion_y_la_bitacora_queda_completa(self, env):
        retiro(env)
        env.esp.check.respuestas["*"] = EMPEZO
        d = revisar(env)
        assert d["ok"] and d["prep_auto_ahora"] is True and d["prep_auto_activo"] is True
        assert env.db.solicitudes[RID]["status"] == "en_preparacion"
        (e,) = cambios_de_estado(env)
        assert (e["actor_type"], e["actor_name"]) == ("sistema", "Check WMS (automático)")
        assert (e["old_status"], e["new_status"]) == ("agenda_confirmada", "en_preparacion")
        assert e["notes"].startswith("Automático")
        assert "1 de 3 unidades pickeadas" in e["notes"]
        assert "Registrado el 07/10/2026 11:00 (hora Chile)" in e["notes"]  # fecha y hora del registro
        assert "solo se le comunica la fecha" in e["notes"]              # lo interno se queda adentro
        assert "segunda lectura" not in e["notes"]                       # con la espera en 0 no hubo segunda lectura: no lo afirma
        assert "OT y usuario: Check aún no los informa" in e["notes"]    # sin reporte de movimientos todavía

    def test_hace_lo_mismo_que_el_boton_checklist_propuestas_y_aviso_interno(self, env):
        retiro(env)
        env.esp.check.respuestas["*"] = EMPEZO
        revisar(env)
        sqls = [s for s, _ in env.db.escrituras]
        assert any(s.startswith("UPDATE `pickup_requests` SET status='en_preparacion' WHERE id=%s AND status='agenda_confirmada'") for s in sqls)
        assert any(s.startswith("UPDATE `pickup_proposals` SET status='superseded'") for s in sqls)
        assert any(s.startswith("INSERT IGNORE INTO pickup_picking_items") for s in sqls)      # lista de bodega
        esperar_avisos()
        assert env.esp.mant_notif.called or env.esp.correo.called                              # aviso al equipo interno

    def test_al_cliente_le_llega_un_solo_correo_y_solo_con_la_fecha(self, env):
        retiro(env)
        env.esp.check.respuestas["*"] = EMPEZO
        revisar(env)
        (c,) = correos_al_cliente(env)
        asunto, html = c.args[1], c.args[2]
        assert "preparando" in (asunto + html).lower()
        for interno in ("Check WMS", "Automático", "pickeado", "OT "):
            assert interno not in html, f"el correo al cliente trae algo interno: {interno}"
        assert env.esp.whatsapp.call_count == 0

    def test_la_bitacora_trae_la_ot_y_el_usuario_que_informe_check(self, env):
        retiro(env)
        env.esp.check.respuestas["*"] = EMPEZO
        env.ctx["_CHECKWMS_TRAZA"] = {"ts": time.time(), "lock": threading.Lock(), "rows": [
            {"ot": "481516", "tipoOT": "PICKING", "estado": "EN CURSO", "doc": "BLV-0000023732", "feInicioOT": "2026-10-02T15:32:10",
             "usuarioPicking": "JPEREZ", "codigo": "MANC10", "sol": 2, "ejec": 1}]}
        revisar(env)
        (e,) = cambios_de_estado(env)
        assert "OT 481516" in e["notes"] and "JPEREZ" in e["notes"] and "02/10/2026 15:32" in e["notes"]
        assert "aún no los informa" not in e["notes"]

    def test_es_idempotente_la_segunda_revision_no_repite_nada(self, env):
        retiro(env)
        env.esp.check.respuestas["*"] = EMPEZO
        revisar(env)
        esperar_avisos()
        n_correos, n_logs = len(correos_al_cliente(env)), len(cambios_de_estado(env))
        env.db.reiniciar_registro()
        for _ in range(3):
            d = revisar(env)
            assert d["prep_auto_ahora"] is False
        esperar_avisos()
        assert (len(correos_al_cliente(env)), len(cambios_de_estado(env))) == (n_correos, n_logs) == (1, 1)

    def test_basta_un_documento_con_picking_aunque_el_otro_no_este_en_check(self, env):
        retiro(env, docs=(DOC, "0000099999"))
        env.esp.check.respuestas["23732"] = EMPEZO
        env.esp.check.respuestas["*"] = None
        assert revisar(env)["prep_auto_ahora"] is True
        assert env.db.solicitudes[RID]["status"] == "en_preparacion"


# ══════════════════════════════════════════════════════════════════════════════
#  Cuándo NO se mueve solo
# ══════════════════════════════════════════════════════════════════════════════
def assert_quieto(env):
    assert env.db.solicitudes[RID]["status"] == "agenda_confirmada"
    assert cambios_de_estado(env) == []
    assert env.db.escrituras == [], f"no debía escribir nada: {env.db.escrituras}"
    esperar_avisos()
    assert env.esp.correo.call_args_list == [] and env.esp.whatsapp.call_count == 0


class TestNoSeMueve:
    def test_si_bodega_no_ha_empezado(self, env):
        retiro(env)
        env.esp.check.respuestas["*"] = NO_EMPEZO
        assert revisar(env)["prep_auto_ahora"] is False
        assert_quieto(env)

    def test_si_check_ya_da_algo_por_despachado_no_se_mueve_solo(self, env):
        """Con unidades despachadas el pedido pudo entregarse antes: no se mueve ni se le escribe al cliente (sí se avisa al equipo: ver
        TestDesfaseCheckDespachado)."""
        retiro(env)
        env.esp.check.respuestas["*"] = YA_DESPACHADO
        assert revisar(env)["prep_auto_ahora"] is False
        assert env.db.solicitudes[RID]["status"] == "agenda_confirmada"
        assert cambios_de_estado(env) == [] and correos_al_cliente(env) == []

    def test_si_check_no_responde(self, env):
        retiro(env)
        env.esp.check.respuestas["*"] = None
        d = revisar(env)
        assert d["prep_auto_ahora"] is False and d["conexion"]["estado"] == "sin_conexion"
        assert_quieto(env)

    def test_con_el_interruptor_apagado(self, env, monkeypatch):
        monkeypatch.setenv("RETIROS_PREP_AUTO", "0")
        retiro(env)
        env.esp.check.respuestas["*"] = EMPEZO
        d = revisar(env)
        assert d["prep_auto_ahora"] is False and d["prep_auto_activo"] is False
        assert_quieto(env)

    @pytest.mark.parametrize("dias", [-5, -2, -1, 2, 3, 10, 90])
    def test_si_la_cita_no_esta_cerca(self, env, dias):
        retiro(env, dias=dias)
        env.esp.check.respuestas["*"] = EMPEZO
        assert revisar(env)["prep_auto_ahora"] is False
        assert_quieto(env)

    @pytest.mark.parametrize("dias", [0, 1])
    def test_si_la_cita_esta_cerca_si_se_mueve(self, env, dias):
        retiro(env, dias=dias)
        env.esp.check.respuestas["*"] = EMPEZO
        assert revisar(env)["prep_auto_ahora"] is True

    def test_la_ventana_se_puede_cambiar_por_entorno(self, env, monkeypatch):
        monkeypatch.setenv("RETIROS_PREP_AUTO_DIAS", "7")
        retiro(env, dias=6)
        env.esp.check.respuestas["*"] = EMPEZO
        assert revisar(env)["prep_auto_ahora"] is True

    def test_si_el_cliente_pidio_cambiar_la_fecha_y_nadie_le_ha_respondido(self, env):
        retiro(env)
        env.db.propuestas.append({"id": 1, "request_id": RID, "status": "pending", "proposed_by": "cliente"})
        env.esp.check.respuestas["*"] = EMPEZO
        assert revisar(env)["prep_auto_ahora"] is False
        assert_quieto(env)

    def test_en_solo_lectura_nunca_cambia_nada(self, env):
        retiro(env)
        env.esp.check.respuestas["*"] = EMPEZO
        assert revisar(env, "?solo_lectura=1")["prep_auto_ahora"] is False
        assert_quieto(env)

    @pytest.mark.parametrize("estado", ["solicitud_recibida", "propuesta_enviada", "reagendada", "en_preparacion", "retirada",
                                        "cerrada", "rechazada"])
    def test_solo_un_retiro_con_la_cita_confirmada(self, env, estado):
        retiro(env, status=estado)
        env.esp.check.respuestas["*"] = EMPEZO
        assert revisar(env)["prep_auto_ahora"] is False
        assert cambios_de_estado(env) == []

    def test_dos_lecturas_separadas_la_primera_solo_toma_nota(self, env, monkeypatch):
        monkeypatch.setenv("RETIROS_CHECK_CONFIRMACION_S", "0.25")
        retiro(env)
        env.esp.check.respuestas["*"] = EMPEZO
        assert revisar(env)["prep_auto_ahora"] is False
        assert_quieto(env)
        time.sleep(0.4)
        assert revisar(env)["prep_auto_ahora"] is True
        assert len(cambios_de_estado(env)) == 1

    def test_si_la_senal_desaparece_entre_lecturas_se_olvida(self, env, monkeypatch):
        monkeypatch.setenv("RETIROS_CHECK_CONFIRMACION_S", "0.25")
        real = time.time
        retiro(env)
        env.esp.check.respuestas["*"] = EMPEZO
        revisar(env)                                             # 1.ª lectura: toma nota
        env.esp.check.respuestas["*"] = NO_EMPEZO                # dato pasajero: ya no hay picking
        with mock.patch("time.time", lambda: real() + 60):       # (la memoria de 45 s de Check ya venció)
            revisar(env)                                         # la señal desapareció: se olvida
        env.esp.check.respuestas["*"] = EMPEZO
        with mock.patch("time.time", lambda: real() + 120):
            assert revisar(env)["prep_auto_ahora"] is False      # vuelve a empezar la cuenta
        assert env.db.solicitudes[RID]["status"] == "agenda_confirmada"
        assert cambios_de_estado(env) == []


# ══════════════════════════════════════════════════════════════════════════════
#  Carreras: el botón, varias pestañas y estados que cambian mientras se lee Check
# ══════════════════════════════════════════════════════════════════════════════
class TestCarreras:
    def test_si_alguien_apreto_el_boton_antes_no_se_repite_nada(self, env):
        retiro(env, status="en_preparacion")
        env.esp.check.respuestas["*"] = EMPEZO
        d = revisar(env)
        assert d["prep_auto_ahora"] is False
        assert cambios_de_estado(env) == []
        assert correos_al_cliente(env) == []

    def test_varias_pestanas_a_la_vez_un_solo_cambio_y_un_solo_correo(self, env):
        retiro(env)
        env.esp.check.respuestas["*"] = EMPEZO
        n = 6
        barrera = threading.Barrier(n)
        resultados = []

        def pestana():
            cli = env.app.test_client()
            barrera.wait(timeout=10)
            resultados.append(cli.get(f"/retiros/{RID}/check-preparacion").get_json())

        hilos = [threading.Thread(target=pestana) for _ in range(n)]
        [h.start() for h in hilos]
        [h.join(20) for h in hilos]
        assert len(resultados) == n
        assert sum(1 for d in resultados if d["prep_auto_ahora"]) == 1
        assert len(cambios_de_estado(env)) == 1
        assert len(correos_al_cliente(env)) == 1

    def test_si_el_estado_cambia_mientras_se_lee_check_no_se_hace_nada(self, env):
        retiro(env)

        class CambiaElEstado(dict):
            def __contains__(self_inner, k):
                env.db.solicitudes[RID]["status"] = "retirada"       # alguien lo cerró durante la espera
                return super().__contains__(k)

        env.esp.check.respuestas = CambiaElEstado({"*": EMPEZO})
        assert revisar(env)["prep_auto_ahora"] is False
        assert cambios_de_estado(env) == []
        assert correos_al_cliente(env) == []

    def test_si_el_cliente_pide_cambiar_la_fecha_justo_antes_de_aplicar_no_se_mueve(self, env):
        retiro(env)

        class PideCambio(dict):
            def __contains__(self_inner, k):
                env.db.propuestas.append({"id": 9, "request_id": RID, "status": "pending", "proposed_by": "cliente"})
                return super().__contains__(k)

        env.esp.check.respuestas = PideCambio({"*": EMPEZO})
        assert revisar(env)["prep_auto_ahora"] is False
        assert env.db.solicitudes[RID]["status"] == "agenda_confirmada"
        assert correos_al_cliente(env) == []


# ══════════════════════════════════════════════════════════════════════════════
#  Trabajo de Cloud Scheduler (corre aunque nadie tenga la ficha abierta)
# ══════════════════════════════════════════════════════════════════════════════
class TestCron:
    def test_sin_token_correcto_es_403_y_no_toca_nada(self, env, monkeypatch):
        monkeypatch.setenv("ILUS_CRON_TOKEN", "secreto-cron")
        retiro(env)
        env.esp.check.respuestas["*"] = EMPEZO
        assert env.cli.get("/retiros/cron/check-barrido").status_code == 403
        assert env.cli.get("/retiros/cron/check-barrido", headers={"X-Cron-Token": "otro"}).status_code == 403
        assert env.cli.get("/retiros/cron/check-barrido?token=secreto-cron").status_code == 403    # por la URL no (queda en los logs)
        assert env.esp.check.llamadas == [] and env.db.solicitudes[RID]["status"] == "agenda_confirmada"

    def test_con_token_pasa_a_preparacion_los_que_ya_tienen_picking(self, env, monkeypatch):
        monkeypatch.setenv("ILUS_CRON_TOKEN", "secreto-cron")
        retiro(env, rid=1)
        retiro(env, rid=2, docs=("0000099999",))
        env.esp.check.respuestas["23732"] = EMPEZO
        env.esp.check.respuestas["99999"] = NO_EMPEZO
        r = env.cli.get("/retiros/cron/check-barrido", headers={"X-Cron-Token": "secreto-cron"})
        assert r.status_code == 200
        d = r.get_json()
        assert d["ok"] and d["revisados"] == 2 and d["pasaron"] == [env.db.solicitudes[1]["code"]]
        assert env.db.solicitudes[1]["status"] == "en_preparacion" and env.db.solicitudes[2]["status"] == "agenda_confirmada"
        (e,) = cambios_de_estado(env, 1)
        assert e["actor_name"] == "Check WMS (automático)"
        assert len(correos_al_cliente(env)) == 1

    def test_dry_solo_mira_y_dice_cuales_tienen_senal(self, env, monkeypatch):
        monkeypatch.setenv("ILUS_CRON_TOKEN", "secreto-cron")
        retiro(env)
        env.esp.check.respuestas["*"] = EMPEZO
        d = env.cli.get("/retiros/cron/check-barrido?dry=1", headers={"X-Cron-Token": "secreto-cron"}).get_json()
        assert d["con_senal"] == [env.db.solicitudes[RID]["code"]] and d["pasaron"] == []
        assert env.db.solicitudes[RID]["status"] == "agenda_confirmada" and env.db.escrituras == []

    def test_confirma_releyendo_a_check_y_si_la_senal_se_fue_no_mueve(self, env, monkeypatch):
        monkeypatch.setenv("ILUS_CRON_TOKEN", "secreto-cron")
        retiro(env)

        class SoloLaPrimeraVez:
            def __init__(self):
                self.n = 0

            def get(self, k, d=None):
                self.n += 1
                return EMPEZO if self.n == 1 else NO_EMPEZO

            def __contains__(self, k):
                return False

        env.esp.check.respuestas = SoloLaPrimeraVez()
        d = env.cli.get("/retiros/cron/check-barrido", headers={"X-Cron-Token": "secreto-cron"}).get_json()
        assert d["con_senal"] and d["pasaron"] == []
        assert env.db.solicitudes[RID]["status"] == "agenda_confirmada" and correos_al_cliente(env) == []

    def test_con_el_interruptor_apagado_no_revisa_nada(self, env, monkeypatch):
        monkeypatch.setenv("ILUS_CRON_TOKEN", "secreto-cron")
        monkeypatch.setenv("RETIROS_PREP_AUTO", "0")
        retiro(env)
        env.esp.check.respuestas["*"] = EMPEZO
        d = env.cli.get("/retiros/cron/check-barrido", headers={"X-Cron-Token": "secreto-cron"}).get_json()
        assert d["activo"] is False and d["revisados"] == 0
        assert env.esp.check.llamadas == []

    def test_sin_token_configurado_solo_acepta_localhost(self, env):
        assert env.cli.get("/retiros/cron/check-barrido").status_code == 200
        r = env.cli.get("/retiros/cron/check-barrido", environ_overrides={"REMOTE_ADDR": "8.8.8.8"})
        assert r.status_code == 403


# ══════════════════════════════════════════════════════════════════════════════
#  Bitácora: «Manual» (el botón) deja el usuario; Check solo se consulta
# ══════════════════════════════════════════════════════════════════════════════
class TestBitacoraYCandados:
    def test_el_boton_manual_deja_quien_lo_presiono(self, env):
        retiro(env)
        r = env.cli.post(f"/retiros/{RID}/status", data={"status": "en_preparacion"})
        assert r.status_code in (200, 302)
        (e,) = cambios_de_estado(env)
        assert e["notes"].startswith("Manual: Samantha Blacio pasó el retiro a «En preparación»")
        assert e["actor_name"] != "Check WMS (automático)"

    def test_check_solo_se_consulta_por_el_seguimiento_de_la_lista_blanca(self, env):
        retiro(env)
        env.esp.check.respuestas["*"] = EMPEZO
        revisar(env)
        assert env.esp.check.rutas == {"/api/ext/GetSeguimientoDespacho"}
        assert env.esp.erp.mock_calls == [] if hasattr(env.esp, "erp") else True


# ══════════════════════════════════════════════════════════════════════════════
#  Ventana en DÍAS HÁBILES y horario de la bodega (revisor adversarial: hallazgos 3 y 7)
# ══════════════════════════════════════════════════════════════════════════════
class TestVentanaYHorario:
    @pytest.mark.parametrize("ahora,confirmada,mueve", [
        (dt.datetime(2026, 10, 2, 11, 0), "2026-10-05", True),     # viernes → lunes: 1 día hábil aunque haya un fin de semana en medio
        (dt.datetime(2026, 10, 9, 11, 0), "2026-10-13", True),     # viernes → martes, con el lunes 12 FERIADO: 1 día hábil
        (dt.datetime(2026, 10, 7, 11, 0), "2026-10-09", False),    # miércoles → viernes: 2 días hábiles
        (dt.datetime(2026, 10, 7, 11, 0), "2026-10-07", True),     # hoy
        (dt.datetime(2026, 10, 7, 11, 0), "2026-10-06", False),    # ayer: una cita pasada la decide una persona
    ])
    def test_la_ventana_cuenta_dias_habiles_y_nunca_una_cita_pasada(self, env, ahora, confirmada, mueve):
        env.reloj["ahora"] = ahora
        retiro(env, confirmed_date=confirmada)
        env.esp.check.respuestas["*"] = EMPEZO
        assert revisar(env)["prep_auto_ahora"] is mueve

    @pytest.mark.parametrize("ahora", [
        dt.datetime(2026, 10, 7, 3, 0),       # de madrugada
        dt.datetime(2026, 10, 7, 7, 59),      # un minuto antes de que empiece la cobertura
        dt.datetime(2026, 10, 7, 13, 0),      # colación (13:00–14:00)
        dt.datetime(2026, 10, 7, 13, 59),
        dt.datetime(2026, 10, 7, 17, 0),      # terminó la cobertura (Daniel 2026-10-06: «no mostrar este mensaje a las cinco de la tarde»)
        dt.datetime(2026, 10, 7, 20, 0),
        dt.datetime(2026, 10, 7, 23, 50),
        dt.datetime(2026, 10, 3, 11, 0),      # sábado
        dt.datetime(2026, 10, 4, 11, 0),      # domingo
        dt.datetime(2026, 10, 12, 11, 0),     # lunes FERIADO
    ])
    def test_fuera_del_horario_de_la_bodega_no_se_mueve_ni_se_escribe_al_cliente(self, env, ahora):
        env.reloj["ahora"] = ahora
        retiro(env, confirmed_date=ahora.date().isoformat())
        env.esp.check.respuestas["*"] = EMPEZO
        assert revisar(env)["prep_auto_ahora"] is False
        assert_quieto(env)

    @pytest.mark.parametrize("ahora", [dt.datetime(2026, 10, 7, 8, 0), dt.datetime(2026, 10, 7, 12, 59),
                                       dt.datetime(2026, 10, 7, 14, 0), dt.datetime(2026, 10, 7, 16, 59)])
    def test_en_el_horario_de_cobertura_si(self, env, ahora):
        env.reloj["ahora"] = ahora
        retiro(env, confirmed_date=ahora.date().isoformat())
        env.esp.check.respuestas["*"] = EMPEZO
        assert revisar(env)["prep_auto_ahora"] is True

    def test_el_cron_fuera_de_horario_no_le_pregunta_nada_a_check(self, env, monkeypatch):
        monkeypatch.setenv("ILUS_CRON_TOKEN", "secreto-cron")
        env.reloj["ahora"] = dt.datetime(2026, 10, 7, 2, 0)
        retiro(env, confirmed_date="2026-10-07")
        env.esp.check.respuestas["*"] = EMPEZO
        d = env.cli.get("/retiros/cron/check-barrido", headers={"X-Cron-Token": "secreto-cron"}).get_json()
        assert d["activo"] is True and d["en_horario"] is False and d["revisados"] == 0 and d["pasaron"] == []
        assert env.esp.check.llamadas == [] and env.db.escrituras == []

    def test_el_cron_en_modo_dry_mira_aunque_sea_de_noche(self, env, monkeypatch):
        monkeypatch.setenv("ILUS_CRON_TOKEN", "secreto-cron")
        env.reloj["ahora"] = dt.datetime(2026, 10, 7, 2, 0)
        retiro(env, confirmed_date="2026-10-07")
        env.esp.check.respuestas["*"] = EMPEZO
        d = env.cli.get("/retiros/cron/check-barrido?dry=1", headers={"X-Cron-Token": "secreto-cron"}).get_json()
        assert d["en_horario"] is False and d["con_senal"] == [env.db.solicitudes[RID]["code"]] and d["pasaron"] == []
        assert env.db.escrituras == []


# ══════════════════════════════════════════════════════════════════════════════
#  Frenos: una persona lo devolvió, factura compartida, sin responsable, interruptores (hallazgos 1, 2 y 8)
# ══════════════════════════════════════════════════════════════════════════════
class TestFrenos:
    def test_si_una_persona_lo_devuelve_a_cita_confirmada_el_automatico_no_lo_repite(self, env):
        retiro(env)
        env.esp.check.respuestas["*"] = EMPEZO
        assert revisar(env)["prep_auto_ahora"] is True
        # una persona lo devuelve a «Cita confirmada» (Cambiar estado o Kanban): el cliente recibe SU correo de «confirmada»
        env.cli.post(f"/retiros/{RID}/status", data={"status": "agenda_confirmada"})
        assert env.db.solicitudes[RID]["status"] == "agenda_confirmada"
        esperar_avisos()
        correos = len(env.esp.correo.call_args_list)
        for _ in range(3):
            assert revisar(env)["prep_auto_ahora"] is False
        esperar_avisos()
        assert env.db.solicitudes[RID]["status"] == "agenda_confirmada"
        assert len(env.esp.correo.call_args_list) == correos                      # nada nuevo le llegó al cliente
        assert sum(1 for x in cambios_de_estado(env) if x["new_status"] == "en_preparacion") == 1

    def test_el_cron_ni_siquiera_mira_a_los_que_una_persona_ya_devolvio(self, env, monkeypatch):
        monkeypatch.setenv("ILUS_CRON_TOKEN", "secreto-cron")
        retiro(env)
        env.esp.check.respuestas["*"] = EMPEZO
        revisar(env)
        env.cli.post(f"/retiros/{RID}/status", data={"status": "agenda_confirmada"})
        env.esp.check.llamadas.clear()
        d = env.cli.get("/retiros/cron/check-barrido", headers={"X-Cron-Token": "secreto-cron"}).get_json()
        assert d["revisados"] == 0 and d["pasaron"] == [] and env.esp.check.llamadas == []

    def test_si_la_factura_esta_en_otro_retiro_activo_no_se_mueve_solo(self, env):
        """Check informa por DOCUMENTO: el picking que ve podría ser del otro retiro."""
        retiro(env, rid=1)
        retiro(env, rid=2, dias=1)                                 # misma factura, otro retiro, también activo
        env.esp.check.respuestas["*"] = EMPEZO
        assert revisar(env)["prep_auto_ahora"] is False
        assert env.db.solicitudes[1]["status"] == "agenda_confirmada" and cambios_de_estado(env) == []
        assert correos_al_cliente(env) == []

    def test_si_el_otro_retiro_ya_termino_no_estorba(self, env):
        retiro(env, rid=1)
        retiro(env, rid=2, status="retirada")
        env.esp.check.respuestas["*"] = EMPEZO
        assert revisar(env)["prep_auto_ahora"] is True

    def test_la_factura_compartida_se_reconoce_aunque_el_numero_traiga_ceros_distintos(self, env):
        retiro(env, rid=1, docs=("0000023732",))
        retiro(env, rid=2, docs=("23732",))
        env.esp.check.respuestas["*"] = EMPEZO
        assert revisar(env)["prep_auto_ahora"] is False

    def test_sin_responsable_declarado_el_automatico_igual_avanza_y_el_aviso_lo_dice(self, env):
        """2026-10-05: el retiro real no tenía responsable y bodega terminó sola; el automático NO es una persona, así que no se frena por eso
        (la regla «para avanzar debe declarar el responsable» es de las personas). El aviso al equipo pide que alguien toque «Me hago cargo»."""
        env.db.admins = [{"id": 11}]
        retiro(env, responsable_user_id=None, responsable_nombre=None)
        env.esp.check.respuestas["*"] = EMPEZO
        assert revisar(env)["prep_auto_ahora"] is True
        assert env.db.solicitudes[RID]["status"] == "en_preparacion"
        esperar_avisos()
        cuerpos = " ".join(c.kwargs.get("cuerpo", "") for c in env.esp.mant_notif.call_args_list)
        assert "NO tiene responsable" in cuerpos and "Me hago cargo" in cuerpos

    def test_con_responsable_el_aviso_no_menciona_la_falta(self, env):
        env.db.admins = [{"id": 11}]
        retiro(env)
        env.esp.check.respuestas["*"] = EMPEZO
        assert revisar(env)["prep_auto_ahora"] is True
        esperar_avisos()
        assert "NO tiene responsable" not in " ".join(c.kwargs.get("cuerpo", "") for c in env.esp.mant_notif.call_args_list)

    @pytest.mark.parametrize("valor", ["0", "false", "no", "off"])
    def test_check_solo_informa_tambien_apaga_el_envio_automatico(self, env, monkeypatch, valor):
        """RETIROS_CHECK_AUTO=0 significa «Check solo informa y no escribe nada»: el cambio de estado es una escritura."""
        monkeypatch.setenv("RETIROS_CHECK_AUTO", valor)
        retiro(env)
        env.esp.check.respuestas["*"] = EMPEZO
        d = revisar(env)
        assert d["prep_auto_ahora"] is False and d["prep_auto_activo"] is False
        assert_quieto(env)

    def test_el_barrido_del_monitor_con_el_interruptor_apagado_ni_siquiera_le_pregunta_a_check(self, env, monkeypatch):
        monkeypatch.setenv("RETIROS_PREP_AUTO", "0")
        retiro(env)
        env.esp.check.respuestas["*"] = EMPEZO
        capturados = []

        class HiloFalso:                                        # el Monitor lanza el barrido en un hilo: aquí se captura y se corre a mano
            def __init__(self, group=None, target=None, name=None, args=(), kwargs=None, *, daemon=None):
                self._t = target

            def start(self):
                capturados.append(self._t)

        with mock.patch.object(pickups_module.threading, "Thread", HiloFalso),                 mock.patch.object(pickups_module, "render_template", side_effect=RuntimeError("sin render")),                 env.app.test_request_context("/retiros"):
            flask.g.user = {"id": 7, "nombre": "Samantha Blacio", "username": "sam@sphs.cl"}
            try:
                env.app.view_functions["pickup_dashboard"]()
            except Exception:
                pass
        (objetivo,) = [t for t in capturados if getattr(t, "__name__", "") == "_check_barrido"]
        with env.app.app_context():
            objetivo()
        assert env.esp.check.llamadas == [] and env.db.solicitudes[RID]["status"] == "agenda_confirmada"


# ══════════════════════════════════════════════════════════════════════════════
#  Lectura fresca: dos lecturas REALES a Check y una guarda a prueba de lecturas viejas (hallazgos 4 y 6)
# ══════════════════════════════════════════════════════════════════════════════
class TestLecturaFresca:
    def test_la_segunda_lectura_se_le_pide_de_verdad_a_check_sin_su_memoria_de_45_s(self, env, monkeypatch):
        monkeypatch.setenv("RETIROS_CHECK_CONFIRMACION_S", "25")
        real = time.time
        retiro(env)
        env.esp.check.respuestas["*"] = EMPEZO
        assert revisar(env)["prep_auto_ahora"] is False           # 1.ª: solo toma nota
        antes = len(env.esp.check.llamadas)
        with mock.patch("time.time", lambda: real() + 30):        # 30 s después: la memoria de 45 s de Check seguiría vigente…
            d = revisar(env)
        assert len(env.esp.check.llamadas) > antes                # …pero la 2.ª lectura se pidió de verdad
        assert d["prep_auto_ahora"] is True
        (e,) = cambios_de_estado(env)
        assert "confirmado con una segunda lectura a Check" in e["notes"]

    def test_si_la_lectura_fresca_ya_no_ve_picking_no_se_mueve(self, env, monkeypatch):
        monkeypatch.setenv("RETIROS_CHECK_CONFIRMACION_S", "25")
        real = time.time
        retiro(env)
        env.esp.check.respuestas["*"] = EMPEZO
        revisar(env)
        env.esp.check.respuestas["*"] = NO_EMPEZO                 # el dato pasajero desapareció
        with mock.patch("time.time", lambda: real() + 30):
            assert revisar(env)["prep_auto_ahora"] is False
        assert_quieto(env)

    def test_una_primera_lectura_de_hace_horas_no_cuenta(self, env, monkeypatch):
        monkeypatch.setenv("RETIROS_CHECK_CONFIRMACION_S", "25")
        real = time.time
        retiro(env)
        env.esp.check.respuestas["*"] = EMPEZO
        revisar(env)                                              # t = 0: toma nota
        with mock.patch("time.time", lambda: real() + 3 * 3600):  # 3 horas después: la nota ya no vale, vuelve a empezar la cuenta
            assert revisar(env)["prep_auto_ahora"] is False
        assert_quieto(env)
        with mock.patch("time.time", lambda: real() + 3 * 3600 + 30):
            assert revisar(env)["prep_auto_ahora"] is True

    def test_si_el_cliente_pide_cambiar_la_fecha_y_la_lectura_era_vieja_el_update_lo_frena(self, env):
        """REPEATABLE READ: la relectura dentro del candado puede seguir viendo el mundo de hace un minuto; la guarda va también DENTRO del UPDATE."""
        retiro(env)
        env.esp.check.respuestas["*"] = EMPEZO
        env.db.snapshot_viejo = True
        env.db.propuestas.append({"id": 9, "request_id": RID, "status": "pending", "proposed_by": "cliente"})
        assert revisar(env)["prep_auto_ahora"] is False
        assert env.db.solicitudes[RID]["status"] == "agenda_confirmada"
        assert any("NOT EXISTS" in sql for sql, _ in env.db.escrituras)         # el UPDATE sí corrió, con la guarda adentro
        assert cambios_de_estado(env) == [] and correos_al_cliente(env) == []
        assert not any(sql.startswith("UPDATE `pickup_proposals`") for sql, _ in env.db.escrituras)   # y no pisó la contrapropuesta

    def test_antes_de_releer_se_cierra_la_transaccion_vieja(self, env):
        retiro(env)
        env.esp.check.respuestas["*"] = EMPEZO
        env.ctx["get_db"].return_value.commit.reset_mock()
        assert revisar(env)["prep_auto_ahora"] is True
        assert env.ctx["get_db"].return_value.commit.called

    def test_si_la_fecha_cambia_mientras_se_lee_check_se_vuelve_a_juzgar_la_ventana(self, env):
        retiro(env)

        class CambiaLaCita(dict):
            def __contains__(self_inner, k):
                env.db.solicitudes[RID]["confirmed_date"] = cita(10)      # alguien reagendó la cita para dentro de 10 días
                return super().__contains__(k)

        env.esp.check.respuestas = CambiaLaCita({"*": EMPEZO})
        assert revisar(env)["prep_auto_ahora"] is False
        assert env.db.solicitudes[RID]["status"] == "agenda_confirmada" and correos_al_cliente(env) == []


# ══════════════════════════════════════════════════════════════════════════════
#  El botón manual y el automático no se pisan (hallazgo 5)
# ══════════════════════════════════════════════════════════════════════════════
class TestBotonManualAtomico:
    def test_si_el_automatico_ya_lo_paso_el_boton_de_una_pantalla_vieja_no_repite_ni_pisa_la_autoria(self, env):
        retiro(env)
        env.esp.check.respuestas["*"] = EMPEZO
        revisar(env)
        esperar_avisos()
        correos, cambios = len(env.esp.correo.call_args_list), len(cambios_de_estado(env))
        env.db.reiniciar_registro()
        r = env.cli.post(f"/retiros/{RID}/status", data={"status": "en_preparacion"})        # pestaña vieja
        assert r.status_code == 302
        esperar_avisos()
        assert env.db.escrituras == []                                                      # ni UPDATE ni bitácora ni nada
        assert len(env.esp.correo.call_args_list) == correos and len(cambios_de_estado(env)) == cambios == 1
        assert cambios_de_estado(env)[-1]["actor_name"] == "Check WMS (automático)"          # la autoría sigue siendo del automático

    def test_carrera_si_entre_la_lectura_y_el_update_lo_paso_otro_el_boton_pierde_en_silencio(self, env):
        retiro(env)
        env.db.al_leer.append((r"^select \* from `pickup_requests` where id=%s",
                               lambda: env.db.solicitudes[RID].update(status="en_preparacion")))   # lo pasó el automático justo después
        r = env.cli.post(f"/retiros/{RID}/status", data={"status": "en_preparacion"})
        assert r.status_code == 302
        esperar_avisos()
        assert cambios_de_estado(env) == [] and env.esp.correo.call_args_list == []
        with env.cli.session_transaction() as sesion:
            assert any("ya pasó este retiro a preparación" in m for _, m in sesion.get("_flashes", []))

    def test_el_boton_normal_sigue_funcionando(self, env):
        retiro(env)
        r = env.cli.post(f"/retiros/{RID}/status", data={"status": "en_preparacion"})
        assert r.status_code == 302 and env.db.solicitudes[RID]["status"] == "en_preparacion"
        assert len(correos_al_cliente(env)) == 1


# ══════════════════════════════════════════════════════════════════════════════
#  El cron manda el correo dentro de la petición (hallazgo 9)
# ══════════════════════════════════════════════════════════════════════════════
class TestCronSincrono:
    def test_el_correo_al_cliente_sale_antes_de_responder_y_sin_hilos(self, env, monkeypatch):
        monkeypatch.setenv("ILUS_CRON_TOKEN", "secreto-cron")
        retiro(env)
        env.esp.check.respuestas["*"] = EMPEZO
        creados = []
        real_thread = threading.Thread

        class Espia(real_thread):
            def __init__(self, *a, **k):
                creados.append(k.get("name") or "")
                super().__init__(*a, **k)

        with mock.patch.object(threading, "Thread", Espia):
            d = env.cli.get("/retiros/cron/check-barrido", headers={"X-Cron-Token": "secreto-cron"}).get_json()
        assert d["pasaron"] == [env.db.solicitudes[RID]["code"]]
        al_cliente = [c for c in env.esp.correo.call_args_list if (c.args[0] or "").lower() == CLIENTE]
        assert len(al_cliente) == 1                                           # ya salió, sin esperar a ningún hilo
        assert not [n for n in creados if n.startswith("pickup-notify-")]     # y no se dejó en un hilo que Cloud Run puede congelar

    def test_la_ficha_abierta_si_lo_manda_en_un_hilo_para_no_hacer_esperar_a_la_persona(self, env):
        retiro(env)
        env.esp.check.respuestas["*"] = EMPEZO
        creados = []
        real_thread = threading.Thread

        class Espia(real_thread):
            def __init__(self, *a, **k):
                creados.append(k.get("name") or "")
                super().__init__(*a, **k)

        with mock.patch.object(threading, "Thread", Espia):
            assert revisar(env)["prep_auto_ahora"] is True
        assert [n for n in creados if n.startswith("pickup-notify-")]
        assert len(correos_al_cliente(env)) == 1

    def test_con_varios_candidatos_no_se_pasa_del_tiempo(self, env, monkeypatch):
        monkeypatch.setenv("ILUS_CRON_TOKEN", "secreto-cron")
        for rid, numero in ((1, "0000011111"), (2, "0000022222"), (3, "0000033333")):
            retiro(env, rid=rid, docs=(numero,))
            env.esp.check.respuestas[str(int(numero))] = respuesta_check(
                fila_check(numeroDocumento=str(int(numero)), solicitado=3, asignado=2, pickeado=1))
        d = env.cli.get("/retiros/cron/check-barrido", headers={"X-Cron-Token": "secreto-cron"}).get_json()
        assert len(d["pasaron"]) == 3 and d["errores"] == 0


# ══════════════════════════════════════════════════════════════════════════════
#  Check ya preparó Y despachó pero el retiro sigue en «Cita confirmada» (2026-10-05, retiro real RET-VQJ58N)
# ══════════════════════════════════════════════════════════════════════════════
TODO_DESPACHADO = respuesta_check(fila_check(solicitado=1, pickeado=0, despachado=1))      # bodega terminó y despachó antes de que nadie lo pasara a preparación


def logs_desfase(env):
    return [x for x in env.db.logs if x["request_id"] == RID and x["action"] == "check_desfase"]


class TestDesfaseCheckDespachado:
    """El aviso de desfase («Check ya preparó y despachó… márcalo RETIRADO») sigue valiendo con la expedición automática APAGADA. Encendida (por
    defecto «sombra») de ese mismo caso se encarga el aviso «Check ya expidió el pedido» (tests/test_retiros_07oct_expedicion.py): no se avisa dos veces."""

    @pytest.fixture(autouse=True)
    def _expedicion_apagada(self, monkeypatch):
        monkeypatch.setenv("RETIROS_RETIRO_AUTO", "0")

    def test_no_se_mueve_ni_se_le_escribe_al_cliente_pero_se_avisa_al_equipo(self, env):
        env.db.admins = [{"id": 11}, {"id": 12}]
        retiro(env, responsable_user_id=None, responsable_nombre=None)           # tal cual el retiro real: sin responsable
        env.esp.check.respuestas["*"] = TODO_DESPACHADO
        d = revisar(env)
        assert d["prep_auto_ahora"] is False and d["evaluacion"]["listo"] is True and d["evaluacion"]["despachadas"] == 1
        assert env.db.solicitudes[RID]["status"] == "agenda_confirmada" and cambios_de_estado(env) == []
        assert correos_al_cliente(env) == []
        (e,) = logs_desfase(env)
        assert e["actor_name"] == "Check WMS" and "RETIRADO" in e["notes"] and "07/10/2026 11:00" in e["notes"]
        esperar_avisos()
        titulos = {c.kwargs.get("titulo") for c in env.esp.mant_notif.call_args_list}
        assert any("Check ya preparó y despachó el pedido" in (t or "") for t in titulos)
        assert {c.kwargs["destino_user_id"] for c in env.esp.mant_notif.call_args_list} == {11, 12}
        cuerpos = " ".join(c.kwargs.get("cuerpo", "") for c in env.esp.mant_notif.call_args_list)
        assert "RETIRADO" in cuerpos and "No lo envíes a preparación" in cuerpos

    def test_se_avisa_una_sola_vez_aunque_se_revise_muchas_veces(self, env):
        retiro(env)
        env.esp.check.respuestas["*"] = TODO_DESPACHADO
        for _ in range(4):
            revisar(env)
        assert len(logs_desfase(env)) == 1

    @pytest.mark.parametrize("ahora", [dt.datetime(2026, 10, 7, 3, 0), dt.datetime(2026, 10, 10, 11, 0)])      # de madrugada / sábado
    def test_fuera_del_horario_de_la_bodega_no_se_avisa(self, env, ahora):
        env.reloj["ahora"] = ahora
        retiro(env, confirmed_date=ahora.date().isoformat())
        env.esp.check.respuestas["*"] = TODO_DESPACHADO
        revisar(env)
        assert logs_desfase(env) == []

    def test_con_la_cita_lejana_no_se_avisa(self, env):
        retiro(env, dias=10)
        env.esp.check.respuestas["*"] = TODO_DESPACHADO
        revisar(env)
        assert logs_desfase(env) == []

    def test_si_check_no_lo_da_por_despachado_no_hay_aviso_de_desfase(self, env):
        retiro(env)
        env.esp.check.respuestas["*"] = EMPEZO
        revisar(env)
        assert logs_desfase(env) == []

    def test_en_modo_solo_lectura_no_escribe_nada(self, env):
        retiro(env)
        env.esp.check.respuestas["*"] = TODO_DESPACHADO
        revisar(env, "?solo_lectura=1")
        assert env.db.escrituras == []

    def test_el_cron_tambien_avisa_y_lo_cuenta(self, env, monkeypatch):
        monkeypatch.setenv("ILUS_CRON_TOKEN", "secreto-cron")
        retiro(env)
        env.esp.check.respuestas["*"] = TODO_DESPACHADO
        d = env.cli.get("/retiros/cron/check-barrido", headers={"X-Cron-Token": "secreto-cron"}).get_json()
        assert d["desfase"] == [env.db.solicitudes[RID]["code"]] and d["pasaron"] == []
        assert len(logs_desfase(env)) == 1 and correos_al_cliente(env) == []

    def test_el_cron_en_dry_solo_mira(self, env, monkeypatch):
        monkeypatch.setenv("ILUS_CRON_TOKEN", "secreto-cron")
        retiro(env)
        env.esp.check.respuestas["*"] = TODO_DESPACHADO
        d = env.cli.get("/retiros/cron/check-barrido?dry=1", headers={"X-Cron-Token": "secreto-cron"}).get_json()
        assert d["desfase"] == [] and env.db.escrituras == []

    def test_el_estado_retirada_desde_cita_confirmada_funciona_sin_pasar_por_preparacion(self, env):
        """Lo que hay que hacer con ese retiro: marcarlo RETIRADO. El cliente recibe SOLO el correo de «retiro completado»."""
        retiro(env)
        r = env.cli.post(f"/retiros/{RID}/status", data={"status": "retirada", "retirado_por": "Gerd Müller", "retirado_por_rut": "18.433.872-6"})
        assert r.status_code == 302 and env.db.solicitudes[RID]["status"] == "retirada"
        (e,) = cambios_de_estado(env)
        assert (e["old_status"], e["new_status"]) == ("agenda_confirmada", "retirada")
        asuntos = " ".join((c.args[1] or "").lower() for c in correos_al_cliente(env))
        assert "preparando" not in asuntos


def test_el_modal_de_marcar_retirado_existe_tambien_con_la_cita_confirmada():
    """2026-10-05: antes solo se dibujaba en «En preparación» y desde «Cita confirmada» no había cómo marcar el retiro entregado."""
    ruta = os.path.join(os.path.dirname(_TESTS), "templates", "retiros", "internal_detail.html")
    with open(ruta, encoding="utf-8") as f:
        html = f.read()
    i = html.index('id="modalRetirar"')
    antes = html[max(0, i - 700):i]
    assert "req.status in ['en_preparacion', 'agenda_confirmada']" in antes

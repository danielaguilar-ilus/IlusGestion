# -*- coding: utf-8 -*-
"""EXPEDICIÓN EN CHECK = RETIRADO (Daniel 2026-10-06): «que de manera automática, cuando bodega expida en Check, el retiro se dé por retirado — eso le
manda al cliente el correo de retiro. Que cuando se envíe a preparar se alerte al personal de que NO tiene que expedir en Check hasta que llegue el
cliente… Y en el correo de retirado, muy sutilmente, que el proceso se cierra al momento de despachar el producto».

Caso real: RET-VQJ58N, cita 05/10 11:00; en Check el picking fue 08:11–08:26 y el CONTROL DE SALIDA (OT CSAL…) a las 08:56.

Modo (RETIROS_RETIRO_AUTO): «sombra» (POR DEFECTO: avisa al equipo, no cambia nada, no escribe al cliente) · «activo» · «0».
Todo corre sin BD real, sin Check, sin ERP y con el correo de mentira (tests/_arnes_retiros.py):

    py -m pytest tests/test_retiros_07oct_expedicion.py -q
"""
import datetime as dt
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
from _arnes_retiros import fila_check, respuesta_check  # noqa: E402

RID = 1
DOC = "0000023732"
CLIENTE = "cliente.real@example.com"
EXPEDIDO = respuesta_check(fila_check(solicitado=3, despachado=3))                        # todo despachado
PARCIAL = respuesta_check(fila_check(solicitado=3, pickeado=1, despachado=2))             # 2 de 3 despachadas
SOLO_PICKEADO = respuesta_check(fila_check(solicitado=3, pickeado=3))                     # junto, pero nada despachado
AHORA = dt.datetime(2026, 10, 7, 11, 0)                                                   # miércoles 11:00: hábil y dentro de la cobertura
NOTA = "Registramos tu retiro en el momento en que bodega despachó tu pedido"
PLANTILLA_SIN_VARIABLE = {
    "asunto": "Tu retiro {{code}} fue completado",
    "cuerpo": '<p>Hola {{persona_retira}}, retiro completado el {{fecha_confirmada}}.</p>'
              '<table><tr><td><a href="{{link_seguimiento}}">Ver mi retiro</a></td></tr></table><p>Gracias</p>'}


def cita(dias=0):
    return (AHORA.date() + dt.timedelta(days=dias)).isoformat()


@pytest.fixture()
def env(monkeypatch):
    monkeypatch.setenv("RETIROS_CHECK_CONFIRMACION_S", "0")        # sin espera entre las dos lecturas (la prueba de «una sola lectura» la cambia)
    for k in ("RETIROS_RETIRO_AUTO", "RETIROS_PREP_AUTO", "RETIROS_CHECK_AUTO", "RETIROS_EXIGE_RESPONSABLE", "ILUS_CRON_TOKEN",
              "RETIROS_COBERTURA_DESDE", "RETIROS_COBERTURA_HASTA"):
        monkeypatch.delenv(k, raising=False)
    reloj = {"ahora": AHORA}
    monkeypatch.setattr(pickups_module, "_RELOJ_CHILE", lambda: reloj["ahora"])
    app, db, ctx, esp = A.construir_app()
    ctx["_comm_render_email_document"] = lambda asunto, cuerpo: cuerpo         # el correo final = el cuerpo de la plantilla
    db.admins = [{"id": 11}]
    db.plantillas[("retirada", "email")] = PLANTILLA_SIN_VARIABLE      # la plantilla de la BD (el arnés no la modifica jamás)
    return SimpleNamespace(app=app, db=db, ctx=ctx, esp=esp, cli=app.test_client(), reloj=reloj)


def retiro(env, rid=RID, status="en_preparacion", docs=(DOC,), **kw):
    kw.setdefault("confirmed_date", cita(0))
    kw.setdefault("responsable_user_id", 7)
    kw.setdefault("responsable_nombre", "Milagros")
    env.db.nueva_solicitud(rid, status=status, **kw)
    for numero in docs:
        env.db.agregar_doc(rid, numero=numero)
    env.db.reiniciar_registro()


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


def revisar(env, query=""):
    return env.cli.get(f"/retiros/{RID}/check-preparacion{query}").get_json()


def logs(env, accion, rid=RID):
    return [x for x in env.db.logs if x["request_id"] == rid and x["action"] == accion]


def campanas(env):
    esperar()
    return [(c.kwargs.get("titulo") or "", c.kwargs.get("cuerpo") or "") for c in env.esp.mant_notif.call_args_list]


def poner_salida_en_check(env):
    env.ctx["_CHECKWMS_TRAZA"] = {"ts": time.time(), "lock": threading.Lock(), "rows": [
        {"ot": "CSAL000123", "tipoOT": "Control Salida / Integraciones", "estado": "FINALIZADA", "doc": "BLV-0000023732",
         "feInicioOT": "2026-10-05T08:50:10", "fechaFin": "2026-10-05T08:56:12", "usuarioSalida": "JPEREZ", "codigo": "MANC10", "sol": 3, "ejec": 3},
        {"ot": "481516", "tipoOT": "PICKING", "estado": "TERMINADA", "doc": "BLV-0000023732", "feInicioOT": "2026-10-05T08:11:10",
         "fechaFin": "2026-10-05T08:26:55", "usuarioPicking": "LBOURRE", "codigo": "MANC10", "sol": 3, "ejec": 3}]}


# ══════════════════════════════════════════════════════════════════════════════
#  MODO SOMBRA (el que corre por defecto)
# ══════════════════════════════════════════════════════════════════════════════
class TestSombra:
    def test_el_modo_por_defecto_es_sombra(self, env):
        retiro(env)
        env.esp.check.respuestas["*"] = SOLO_PICKEADO
        assert revisar(env)["retiro_auto_modo"] == "sombra"

    @pytest.mark.parametrize("valor", ["", "sombra", "SOMBRA", "cualquier cosa", "1", "si"])
    def test_cualquier_valor_que_no_sea_activo_ni_apagado_es_sombra(self, env, monkeypatch, valor):
        monkeypatch.setenv("RETIROS_RETIRO_AUTO", valor)
        retiro(env)
        env.esp.check.respuestas["*"] = SOLO_PICKEADO
        assert revisar(env)["retiro_auto_modo"] == "sombra"

    def test_avisa_al_equipo_y_no_cambia_nada_ni_le_escribe_al_cliente(self, env):
        retiro(env)
        poner_salida_en_check(env)
        env.esp.check.respuestas["*"] = EXPEDIDO
        d = revisar(env)
        assert d["retiro_auto_ahora"] is True
        assert env.db.solicitudes[RID]["status"] == "en_preparacion"                  # el estado NO cambia
        assert logs(env, "estado_actualizado") == []
        assert correos_al_cliente(env) == [] and env.esp.whatsapp.call_count == 0
        assert env.esp.correo.call_args_list == []                                    # ni al cliente ni a nadie por correo
        (e,) = logs(env, "check_expedido")
        assert e["actor_name"] == "Check WMS" and e["actor_type"] == "sistema"
        assert "SOMBRA" in e["notes"] and "07/10/2026 11:00" in e["notes"]
        assert "CSAL000123" in e["notes"] and "JPEREZ" in e["notes"] and "08:56" in e["notes"]      # OT, usuario y hora de la salida en Check
        (titulo, cuerpo), = campanas(env)
        assert "Check ya expidió el pedido" in titulo
        assert "a las 08:56" in cuerpo and "márcalo como RETIRADO" in cuerpo and "cierre automático esté activo" in cuerpo

    def test_se_avisa_una_sola_vez_aunque_se_revise_muchas_veces(self, env):
        retiro(env)
        env.esp.check.respuestas["*"] = EXPEDIDO
        for _ in range(5):
            revisar(env)
        assert len(logs(env, "check_expedido")) == 1 and len(campanas(env)) == 1

    def test_sin_el_reporte_de_movimientos_usa_la_hora_en_que_lo_detecto(self, env):
        retiro(env)
        env.esp.check.respuestas["*"] = EXPEDIDO
        revisar(env)
        (_, cuerpo), = campanas(env)
        assert "detectado a las 11:00" in cuerpo
        assert "aún no los informa" in logs(env, "check_expedido")[0]["notes"]

    def test_con_la_cita_confirmada_dentro_de_la_ventana_tambien_avisa_y_el_desfase_no_duplica(self, env):
        retiro(env, status="agenda_confirmada", confirmed_date=cita(1))
        env.esp.check.respuestas["*"] = EXPEDIDO
        revisar(env)
        assert len(logs(env, "check_expedido")) == 1
        assert logs(env, "check_desfase") == [], "el aviso de desfase y el de expedición no deben duplicarse"
        assert len(campanas(env)) == 1
        assert env.db.solicitudes[RID]["status"] == "agenda_confirmada"


# ══════════════════════════════════════════════════════════════════════════════
#  MODO ACTIVO
# ══════════════════════════════════════════════════════════════════════════════
@pytest.fixture()
def activo(env, monkeypatch):
    monkeypatch.setenv("RETIROS_RETIRO_AUTO", "activo")
    return env


class TestActivo:
    def test_pasa_a_retirada_y_le_llega_un_solo_correo_con_la_linea_sutil(self, activo):
        env = activo
        retiro(env)
        poner_salida_en_check(env)
        env.esp.check.respuestas["*"] = EXPEDIDO
        d = revisar(env)
        assert d["retiro_auto_ahora"] is True and d["retiro_auto_modo"] == "activo"
        fila = env.db.solicitudes[RID]
        assert fila["status"] == "retirada" and fila["closed_at"]
        assert not fila.get("retirado_por_nombre"), "quién retiró queda vacío para que una persona lo complete"
        (e,) = logs(env, "estado_actualizado")
        assert (e["actor_type"], e["actor_name"]) == ("sistema", "Check WMS (automático)")
        assert (e["old_status"], e["new_status"]) == ("en_preparacion", "retirada")
        assert e["notes"].startswith("Automático") and "CSAL000123" in e["notes"] and "JPEREZ" in e["notes"]
        assert "una persona debe anotarlo" in e["notes"] and "07/10/2026 11:00" in e["notes"]
        (c,) = correos_al_cliente(env)
        cuerpo = c.args[2]
        assert NOTA in cuerpo, "el correo de retiro lleva la línea sutil"
        assert "Check" not in cuerpo and "CSAL" not in cuerpo and "JPEREZ" not in cuerpo        # nada interno
        assert logs(env, "check_expedido") == []                                                 # en activo no es un aviso de sombra
        (titulo, cuerpo_eq), = campanas(env)
        assert "cerrado automáticamente" in titulo and "anotar quién retiró" in cuerpo_eq

    def test_es_idempotente_la_segunda_revision_no_repite_nada(self, activo):
        env = activo
        retiro(env)
        env.esp.check.respuestas["*"] = EXPEDIDO
        revisar(env)
        n = len(correos_al_cliente(env))
        env.db.reiniciar_registro()
        for _ in range(4):
            assert revisar(env)["retiro_auto_ahora"] is False
        assert len(correos_al_cliente(env)) == n == 1 and len(logs(env, "estado_actualizado")) == 1

    def test_desde_cita_confirmada_cierra_y_registra_la_preparacion_sin_correo_de_preparacion(self, activo):
        # 2026-10-09 (Daniel, con BLV 23732 de modelo): «cuando se asignara el picking se iba a gestionar la preparación, y cuando se
        # integrara y se expidiera, la entrega». El candado del 08/10 dejaba en «Cita confirmada» un pedido ya entregado (RET-YWN4D4).
        env = activo
        retiro(env, status="agenda_confirmada", confirmed_date=cita(1))
        poner_salida_en_check(env)
        env.esp.check.respuestas["*"] = EXPEDIDO
        assert revisar(env)["retiro_auto_ahora"] is True
        assert env.db.solicitudes[RID]["status"] == "retirada"
        prep, ret = logs(env, "estado_actualizado")
        assert (prep["old_status"], prep["new_status"]) == ("agenda_confirmada", "en_preparacion")
        assert (ret["old_status"], ret["new_status"]) == ("en_preparacion", "retirada")
        assert prep["actor_name"] == ret["actor_name"] == "Check WMS (automático)"
        assert "preparación se registra junto con la entrega" in prep["notes"] and "no se le envía «Estamos preparando»" in prep["notes"]
        assert "481516" in prep["notes"] or "CSAL000123" in prep["notes"]          # lo que Check informa del pedido
        (c,) = correos_al_cliente(env)                                             # UN solo correo: «Retiro completado», nunca «Estamos preparando»
        assert NOTA in c.args[2] and "prepar" not in (c.args[1] or "").lower()
        assert logs(env, "check_expedido") == []
        (titulo, _cuerpo), = campanas(env)
        assert "cerrado automáticamente" in titulo

    def test_desde_cita_confirmada_fuera_de_la_ventana_no_cierra(self, activo):
        env = activo
        retiro(env, status="agenda_confirmada", confirmed_date=cita(5))
        env.esp.check.respuestas["*"] = EXPEDIDO
        assert revisar(env)["retiro_auto_ahora"] is False
        assert_quieto(env, "agenda_confirmada")

    def test_el_cierre_manual_no_lleva_la_linea_sutil(self, activo):
        env = activo
        retiro(env)
        r = env.cli.post(f"/retiros/{RID}/status", data={"status": "retirada", "retirado_por": "Gerd Müller"})
        assert r.status_code == 302 and env.db.solicitudes[RID]["status"] == "retirada"
        (c,) = correos_al_cliente(env)
        assert NOTA not in c.args[2] and "bodega despachó" not in c.args[2]

    def test_el_cliente_con_un_cambio_de_fecha_pendiente_no_se_cierra_solo(self, activo):
        env = activo
        retiro(env)
        env.db.propuestas.append({"id": 1, "request_id": RID, "status": "pending", "proposed_by": "cliente"})
        env.esp.check.respuestas["*"] = EXPEDIDO
        assert revisar(env)["retiro_auto_ahora"] is False
        assert env.db.solicitudes[RID]["status"] == "en_preparacion" and correos_al_cliente(env) == []

    def test_si_una_persona_lo_marco_retirado_justo_antes_no_se_repite_el_correo(self, activo):
        env = activo
        retiro(env)
        env.esp.check.respuestas["*"] = EXPEDIDO
        env.db.al_leer.append((r"^select \* from `pickup_requests` where id=%s$",
                               lambda: env.db.solicitudes[RID].update(status="retirada")))
        revisar(env)
        assert correos_al_cliente(env) == [] and logs(env, "estado_actualizado") == []


# ══════════════════════════════════════════════════════════════════════════════
#  La línea sutil en el correo (la plantilla de la BD NO se modifica)
# ══════════════════════════════════════════════════════════════════════════════
class TestLineaSutil:
    def test_la_plantilla_de_la_bd_sin_variable_la_recibe_antes_del_boton(self, activo):
        env = activo
        env.db.plantillas[("retirada", "email")] = PLANTILLA_SIN_VARIABLE
        retiro(env)
        env.esp.check.respuestas["*"] = EXPEDIDO
        revisar(env)
        (c,) = correos_al_cliente(env)
        cuerpo = c.args[2]
        assert NOTA in cuerpo and cuerpo.index(NOTA) < cuerpo.index("Ver mi retiro") and cuerpo.index("Hola") < cuerpo.index(NOTA)
        assert cuerpo.count(NOTA) == 1 and "{{" not in cuerpo

    def test_una_plantilla_que_trae_la_variable_la_ubica_donde_quiera(self, activo):
        env = activo
        env.db.plantillas[("retirada", "email")] = {"asunto": "Retiro {{code}}", "cuerpo": "<p>{{nota_cierre}}</p><p>Hola</p>"}
        retiro(env)
        env.esp.check.respuestas["*"] = EXPEDIDO
        revisar(env)
        (c,) = correos_al_cliente(env)
        assert c.args[2].index(NOTA) < c.args[2].index("Hola") and c.args[2].count(NOTA) == 1

    def test_el_cierre_manual_con_la_misma_plantilla_sale_sin_la_linea(self, activo):
        env = activo
        env.db.plantillas[("retirada", "email")] = PLANTILLA_SIN_VARIABLE
        retiro(env)
        env.cli.post(f"/retiros/{RID}/status", data={"status": "retirada"})
        (c,) = correos_al_cliente(env)
        assert NOTA not in c.args[2] and "{{" not in c.args[2]

    def test_sin_plantilla_en_la_bd_el_correo_de_respaldo_tambien_la_lleva(self, activo):
        env = activo
        env.db.plantillas.clear()                      # sin plantilla en la BD: sale el correo de respaldo
        retiro(env)
        env.esp.check.respuestas["*"] = EXPEDIDO
        revisar(env)
        esperar()
        llamada = env.ctx["_ilus_email_html"].call_args
        assert llamada is not None and any(NOTA in p for p in llamada.kwargs["parrafos"])

    def test_el_texto_es_calido_y_no_alarma(self):
        t = pickups_module.__dict__.get("_NOTA_CIERRE_AUTO")        # vive dentro de register_pickup_routes: se lee del código
        fuente = open(os.path.join(os.path.dirname(_TESTS), "pickups_module.py"), encoding="utf-8").read()
        i = fuente.index("_NOTA_CIERRE_AUTO = (")
        texto = fuente[i:fuente.index(")\n", i)]
        assert t is None
        assert "no te preocupes" in texto and "bodega" in texto and "fecha acordada" in texto
        assert "error" not in texto.lower() and "alarm" not in texto.lower() and "tiempo" not in texto.lower()


# ══════════════════════════════════════════════════════════════════════════════
#  Cuándo NO actúa (en sombra y en activo por igual)
# ══════════════════════════════════════════════════════════════════════════════
def assert_quieto(env, estado="en_preparacion"):
    assert env.db.solicitudes[RID]["status"] == estado
    assert logs(env, "check_expedido") == [] and logs(env, "estado_actualizado") == []
    esperar()
    assert env.esp.correo.call_args_list == [] and env.esp.whatsapp.call_count == 0 and campanas(env) == []


@pytest.fixture(params=["sombra", "activo"])
def modo(request, env, monkeypatch):
    monkeypatch.setenv("RETIROS_RETIRO_AUTO", request.param)
    return env


class TestNoActua:
    def test_si_no_esta_todo_despachado(self, modo):
        env = modo
        retiro(env)
        env.esp.check.respuestas["*"] = PARCIAL
        assert revisar(env)["retiro_auto_ahora"] is False
        assert_quieto(env)

    def test_si_esta_junto_pero_nada_despachado(self, modo):
        env = modo
        retiro(env)
        env.esp.check.respuestas["*"] = SOLO_PICKEADO
        assert revisar(env)["retiro_auto_ahora"] is False
        assert env.db.solicitudes[RID]["status"] == "en_preparacion"          # (el aviso «listo para entrega» de siempre es otra función)
        assert logs(env, "check_expedido") == [] and logs(env, "estado_actualizado") == [] and correos_al_cliente(env) == []

    def test_una_linea_sobre_despachada_no_compensa_a_otra(self, modo):
        env = modo
        retiro(env)
        env.esp.check.respuestas["*"] = respuesta_check(fila_check(codigo="A", solicitado=1, despachado=5),
                                                        fila_check(codigo="B", solicitado=2, despachado=0, pickeado=2))
        assert revisar(env)["retiro_auto_ahora"] is False
        assert_quieto(env)

    def test_si_check_no_responde(self, modo):
        env = modo
        retiro(env)
        env.esp.check.respuestas["*"] = None
        assert revisar(env)["retiro_auto_ahora"] is False
        assert_quieto(env)

    def test_si_a_uno_de_los_documentos_check_no_lo_conoce(self, modo):
        env = modo
        retiro(env, docs=(DOC, "0000099999"))
        env.esp.check.respuestas["23732"] = EXPEDIDO
        env.esp.check.respuestas["*"] = None
        assert revisar(env)["retiro_auto_ahora"] is False
        assert_quieto(env)

    def test_con_dos_documentos_hay_que_ver_los_dos_despachados(self, modo):
        env = modo
        retiro(env, docs=(DOC, "0000099999"))
        env.esp.check.respuestas["23732"] = EXPEDIDO
        env.esp.check.respuestas["99999"] = SOLO_PICKEADO
        assert revisar(env)["retiro_auto_ahora"] is False
        assert_quieto(env)

    def test_si_los_numeros_de_check_no_se_entienden(self, modo):
        env = modo
        retiro(env)
        env.esp.check.respuestas["*"] = respuesta_check(fila_check(solicitado="tres", despachado=3))
        assert revisar(env)["retiro_auto_ahora"] is False
        assert_quieto(env)

    def test_si_un_documento_esta_tambien_en_otro_retiro_activo(self, modo):
        env = modo
        retiro(env)
        retiro(env, rid=2, status="agenda_confirmada", confirmed_date=cita(1), code="RET-OTRO", public_token="tok2")
        env.esp.check.respuestas["*"] = EXPEDIDO
        assert revisar(env)["retiro_auto_ahora"] is False
        assert_quieto(env)

    # 2026-10-09: la expedición se registra en tiempo real durante la JORNADA de la bodega (07:30–20:00, días hábiles), no solo en la cobertura
    @pytest.mark.parametrize("ahora", [dt.datetime(2026, 10, 7, 3, 0),          # de madrugada
                                       dt.datetime(2026, 10, 7, 7, 29),          # antes de que abra la bodega
                                       dt.datetime(2026, 10, 7, 20, 0),          # la bodega ya cerró
                                       dt.datetime(2026, 10, 7, 23, 30),
                                       dt.datetime(2026, 10, 10, 11, 0),         # sábado
                                       dt.datetime(2026, 10, 12, 11, 0)])        # lunes feriado
    def test_fuera_de_la_jornada_de_la_bodega(self, modo, ahora):
        env = modo
        env.reloj["ahora"] = ahora
        retiro(env, confirmed_date=ahora.date().isoformat())
        env.esp.check.respuestas["*"] = EXPEDIDO
        assert revisar(env)["retiro_auto_ahora"] is False
        assert_quieto(env)

    def test_una_sola_lectura_no_basta(self, modo, monkeypatch):
        env = modo
        monkeypatch.setenv("RETIROS_CHECK_CONFIRMACION_S", "0.25")
        retiro(env)
        env.esp.check.respuestas["*"] = EXPEDIDO
        assert revisar(env)["retiro_auto_ahora"] is False
        assert_quieto(env)
        llamadas_antes = len(env.esp.check.llamadas)
        time.sleep(0.3)
        assert revisar(env)["retiro_auto_ahora"] is True                       # la segunda lectura (pedida de verdad a Check) lo confirma
        assert len(env.esp.check.llamadas) == llamadas_antes + 1     # la segunda lectura no usó la memoria de 45 s de Check

    def test_si_la_segunda_lectura_ya_no_lo_ve_expedido_no_actua(self, modo, monkeypatch):
        env = modo
        monkeypatch.setenv("RETIROS_CHECK_CONFIRMACION_S", "0.2")
        retiro(env)
        env.esp.check.respuestas["*"] = EXPEDIDO
        revisar(env)
        time.sleep(0.25)
        env.esp.check.respuestas["*"] = SOLO_PICKEADO
        assert revisar(env)["retiro_auto_ahora"] is False
        assert_quieto(env)

    def test_con_la_funcion_apagada(self, env, monkeypatch):
        monkeypatch.setenv("RETIROS_RETIRO_AUTO", "0")
        retiro(env)
        env.esp.check.respuestas["*"] = EXPEDIDO
        d = revisar(env)
        assert d["retiro_auto_modo"] == "apagado" and d["retiro_auto_ahora"] is False
        assert_quieto(env)

    def test_con_check_solo_informa_tampoco(self, env, monkeypatch):
        monkeypatch.setenv("RETIROS_CHECK_AUTO", "0")
        monkeypatch.setenv("RETIROS_RETIRO_AUTO", "activo")
        retiro(env)
        env.esp.check.respuestas["*"] = EXPEDIDO
        assert revisar(env)["retiro_auto_modo"] == "apagado"
        assert_quieto(env)

    def test_en_modo_solo_lectura_nunca_cambia_nada(self, modo):
        env = modo
        retiro(env)
        env.esp.check.respuestas["*"] = EXPEDIDO
        assert revisar(env, "?solo_lectura=1")["retiro_auto_ahora"] is False
        assert_quieto(env)
        assert env.db.escrituras == []

    @pytest.mark.parametrize("estado", ["solicitud_recibida", "propuesta_enviada", "reagendada", "retirada", "cerrada", "rechazada", "fallida"])
    def test_solo_un_retiro_en_preparacion_o_con_la_cita_confirmada(self, modo, estado):
        env = modo
        retiro(env, status=estado)
        env.esp.check.respuestas["*"] = EXPEDIDO
        assert revisar(env)["retiro_auto_ahora"] is False
        assert_quieto(env, estado)

    @pytest.mark.parametrize("dias", [-3, 5, 40])
    def test_con_la_cita_confirmada_lejos_no_actua(self, modo, dias):
        env = modo
        retiro(env, status="agenda_confirmada", confirmed_date=cita(dias))
        env.esp.check.respuestas["*"] = EXPEDIDO
        assert revisar(env)["retiro_auto_ahora"] is False
        assert env.db.solicitudes[RID]["status"] == "agenda_confirmada"
        assert logs(env, "check_expedido") == [] and logs(env, "estado_actualizado") == [] and correos_al_cliente(env) == []


# ══════════════════════════════════════════════════════════════════════════════
#  Check es SOLO LECTURA (REGLA #4.4)
# ══════════════════════════════════════════════════════════════════════════════
class TestCheckSoloLectura:
    @pytest.mark.parametrize("valor", ["sombra", "activo"])
    def test_solo_se_consulta_por_la_lista_blanca_y_con_lectura(self, env, monkeypatch, valor):
        monkeypatch.setenv("RETIROS_RETIRO_AUTO", valor)
        monkeypatch.setenv("RETIROS_CHECK_CONFIRMACION_S", "0.01")
        retiro(env)
        env.esp.check.respuestas["*"] = EXPEDIDO
        revisar(env)
        time.sleep(0.05)
        revisar(env)
        assert env.esp.check.llamadas, "no consultó a Check"
        assert env.esp.check.rutas == {"/api/ext/GetSeguimientoDespacho"}

    def test_el_codigo_nuevo_no_escribe_en_check(self):
        fuente = open(os.path.join(os.path.dirname(_TESTS), "pickups_module.py"), encoding="utf-8").read()
        i = fuente.index("EXPEDICIÓN EN CHECK = RETIRADO")
        bloque = fuente[i:fuente.index("def _prep_auto_candidatos(", i)]
        import re
        assert not re.search(r"requests\.|urllib|\.post\(|\.put\(|\.delete\(|\.patch\(", bloque)
        assert "_checkwms_get" not in bloque and "/api/ext/" not in bloque        # solo reutiliza _check_prep_retiro (GetSeguimientoDespacho)


# ══════════════════════════════════════════════════════════════════════════════
#  Alerta al personal al enviar a preparación: «no expidan en Check hasta que llegue el cliente»
# ══════════════════════════════════════════════════════════════════════════════
class TestAlertaNoExpedir:
    TEXTO_SOMBRA = "⚠️ No expidan en Check hasta que el cliente esté retirando: al expedir, ILUS avisará que el pedido ya salió."
    TEXTO_ACTIVO = "⚠️ No expidan en Check hasta que el cliente esté retirando: al expedir, ILUS lo da por RETIRADO y le avisa al cliente."

    @pytest.mark.parametrize("valor, esperado, ausente", [("", TEXTO_SOMBRA, "RETIRADO y le avisa"), ("activo", TEXTO_ACTIVO, "avisará que el pedido ya salió")])
    def test_el_boton_de_enviar_a_preparacion_avisa_al_equipo(self, env, monkeypatch, valor, esperado, ausente):
        if valor:
            monkeypatch.setenv("RETIROS_RETIRO_AUTO", valor)
        retiro(env, status="agenda_confirmada", confirmed_date=cita(1))
        r = env.cli.post(f"/retiros/{RID}/status", data={"status": "en_preparacion"})
        assert r.status_code == 302 and env.db.solicitudes[RID]["status"] == "en_preparacion"
        cuerpos = " ".join(c for _, c in campanas(env))
        assert esperado in cuerpos and ausente not in cuerpos

    def test_el_envio_automatico_a_preparacion_tambien_avisa_al_equipo(self, env):
        retiro(env, status="agenda_confirmada", confirmed_date=cita(1))
        env.esp.check.respuestas["*"] = respuesta_check(fila_check(solicitado=3, asignado=2, pickeado=1))
        assert revisar(env)["prep_auto_ahora"] is True
        cuerpos = " ".join(c for _, c in campanas(env))
        assert self.TEXTO_SOMBRA in cuerpos

    def test_con_la_funcion_apagada_no_se_dice_nada(self, env, monkeypatch):
        monkeypatch.setenv("RETIROS_RETIRO_AUTO", "0")
        retiro(env, status="agenda_confirmada", confirmed_date=cita(1))
        env.cli.post(f"/retiros/{RID}/status", data={"status": "en_preparacion"})
        assert "No expidan" not in " ".join(c for _, c in campanas(env))

    def test_el_aviso_al_cliente_de_preparacion_no_trae_el_aviso_interno(self, env):
        retiro(env, status="agenda_confirmada", confirmed_date=cita(1))
        env.cli.post(f"/retiros/{RID}/status", data={"status": "en_preparacion"})
        for c in correos_al_cliente(env):
            assert "expidan" not in c.args[2].lower() and "expedir" not in c.args[2].lower()

    def test_la_tarjeta_de_la_ficha_lo_muestra(self, env):
        from unittest import mock
        import flask
        retiro(env, status="en_preparacion")
        with mock.patch.object(pickups_module, "render_template", return_value="ok") as rt:
            with env.app.test_request_context(f"/retiros/{RID}"):
                flask.g.user = {"id": 7, "nombre": "Sam", "username": "sam@sphs.cl"}
                env.app.view_functions["pickup_detail"](RID)
        assert rt.call_args.kwargs["aviso_no_expedir"] == self.TEXTO_SOMBRA and rt.call_args.kwargs["retiro_auto_modo"] == "sombra"
        plantilla = open(os.path.join(os.path.dirname(_TESTS), "templates", "retiros", "internal_detail.html"), encoding="utf-8").read()
        i = plantilla.index("<h5>Bodega está preparando el pedido</h5>")
        assert "aviso_no_expedir" in plantilla[i:i + 1600]


# ══════════════════════════════════════════════════════════════════════════════
#  El cron (Cloud Scheduler)
# ══════════════════════════════════════════════════════════════════════════════
def cron(env, query=""):
    return env.cli.get("/retiros/cron/check-barrido" + query, headers={"X-Cron-Token": "secreto-cron"}).get_json()


class TestCron:
    @pytest.fixture(autouse=True)
    def _token(self, monkeypatch):
        monkeypatch.setenv("ILUS_CRON_TOKEN", "secreto-cron")

    def test_en_sombra_avisa_una_vez_y_lo_cuenta(self, env):
        retiro(env)
        env.esp.check.respuestas["*"] = EXPEDIDO
        d = cron(env)
        code = env.db.solicitudes[RID]["code"]
        assert d["retiro_auto_modo"] == "sombra" and d["expedidos"] == [code] and d["expedidos_actuaron"] == [code]
        assert env.db.solicitudes[RID]["status"] == "en_preparacion" and len(logs(env, "check_expedido")) == 1
        cron(env)
        assert len(logs(env, "check_expedido")) == 1 and correos_al_cliente(env) == []

    def test_en_activo_cierra_y_manda_el_correo_en_la_misma_peticion(self, env, monkeypatch):
        monkeypatch.setenv("RETIROS_RETIRO_AUTO", "activo")
        retiro(env)
        env.esp.check.respuestas["*"] = EXPEDIDO
        d = cron(env)
        assert d["expedidos_actuaron"] == [env.db.solicitudes[RID]["code"]]
        assert env.db.solicitudes[RID]["status"] == "retirada"
        (c,) = correos_al_cliente(env)
        assert NOTA in c.args[2]

    def test_en_dry_solo_mira(self, env, monkeypatch):
        monkeypatch.setenv("RETIROS_RETIRO_AUTO", "activo")
        retiro(env)
        env.esp.check.respuestas["*"] = EXPEDIDO
        d = cron(env, "?dry=1")
        assert d["expedidos"] == [env.db.solicitudes[RID]["code"]] and d["expedidos_actuaron"] == []
        assert env.db.escrituras == [] and env.db.solicitudes[RID]["status"] == "en_preparacion"

    def test_apagado_no_lee_ni_escribe(self, env, monkeypatch):
        monkeypatch.setenv("RETIROS_RETIRO_AUTO", "0")
        monkeypatch.setenv("RETIROS_PREP_AUTO", "0")
        retiro(env)
        env.esp.check.respuestas["*"] = EXPEDIDO
        d = cron(env)
        assert d["expedidos"] == [] and env.esp.check.llamadas == [] and env.db.escrituras == []

    def test_fuera_de_horario_no_hace_nada(self, env):
        env.reloj["ahora"] = dt.datetime(2026, 10, 7, 3, 0)
        retiro(env)
        env.esp.check.respuestas["*"] = EXPEDIDO
        d = cron(env)
        assert d["en_horario"] is False and d["expedidos"] == [] and env.db.escrituras == []

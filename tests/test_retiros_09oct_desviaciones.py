# -*- coding: utf-8 -*-
"""2026-10-09 (Daniel): «si el cliente no viene… es necesario tomar también una opción de reagendar o cancelar el pedido… varias
desviaciones que pueden pasar a lo largo de la operación: decisiones humanas, incidencia, no pudo llegar».

Decisiones de Daniel: cita vencida → aviso al responsable (al cliente nada solo) · correo al cliente SOLO si el responsable lo marca ·
motivo obligatorio de una lista · primera entrega: no vino + cita vencida, reagendar/cancelar ordenados, incidencias, aviso a bodega.

Todo corre sin BD real, sin Check y con el correo de mentira (tests/_arnes_retiros.py):

    py -m pytest tests/test_retiros_09oct_desviaciones.py -q
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
import retiros_desviaciones as rdv  # noqa: E402
import retiros_guia as rg  # noqa: E402

RAIZ = os.path.dirname(_TESTS)
RID = 1
CLIENTE = "cliente.real@example.com"
AHORA = dt.datetime(2026, 10, 7, 11, 0)          # miércoles 11:00: hábil, dentro de la cobertura y de la jornada de la bodega
HOY = AHORA.date().isoformat()


@pytest.fixture()
def env(monkeypatch):
    monkeypatch.setenv("RETIROS_CHECK_CONFIRMACION_S", "0")
    monkeypatch.setenv("ILUS_CRON_TOKEN", "secreto-cron")
    for k in ("RETIROS_RETIRO_AUTO", "RETIROS_PREP_AUTO", "RETIROS_CHECK_AUTO", "RETIROS_EXIGE_RESPONSABLE", "RETIROS_JORNADA_BODEGA"):
        monkeypatch.delenv(k, raising=False)
    reloj = {"ahora": AHORA}
    monkeypatch.setattr(pickups_module, "_RELOJ_CHILE", lambda: reloj["ahora"])
    app, db, ctx, esp = A.construir_app()
    ctx["_comm_render_email_document"] = lambda asunto, cuerpo: cuerpo
    db.admins = [{"id": 11}]
    return SimpleNamespace(app=app, db=db, ctx=ctx, esp=esp, cli=app.test_client(), reloj=reloj)


def retiro(env, status="agenda_confirmada", **kw):
    kw.setdefault("responsable_user_id", 7)
    kw.setdefault("responsable_nombre", "Milagros")
    kw.setdefault("confirmed_date", HOY)
    kw.setdefault("confirmed_time_from", "09:00")
    kw.setdefault("confirmed_time_to", "09:30")
    env.db.nueva_solicitud(RID, status=status, **kw)
    env.db.agregar_doc(RID, "BLV", "0000023732")
    env.db.reiniciar_registro()


def esperar():
    t0 = time.time()
    while time.time() - t0 < 5:
        vivos = [t for t in threading.enumerate() if t.name.startswith(("pickup-notify", "pickup-team-notify"))]
        if not vivos:
            return
        for t in vivos:
            t.join(0.2)


def correos_a(env, dest):
    esperar()
    return [c for c in env.esp.correo.call_args_list if (c.args[0] or "").lower() == dest]


def logs(env, accion):
    return [x for x in env.db.logs if x["request_id"] == RID and x["action"] == accion]


def registrar(env, **datos):
    r = env.cli.post(f"/retiros/{RID}/desviacion", json=datos)
    return r.status_code, r.get_json()


# ══════════════════════════════════════════════════════════════════════════════
#  Catálogo y reglas (puras)
# ══════════════════════════════════════════════════════════════════════════════
class TestReglas:
    def test_cada_tipo_tiene_motivos_y_acciones(self):
        for t in rdv.TIPOS:
            assert rdv.MOTIVOS[t] and rdv.ACCIONES_POR_TIPO[t]
            assert any(c == "otro" for c, _ in rdv.MOTIVOS[t])
            for a in rdv.ACCIONES_POR_TIPO[t]:
                assert a in rdv.ACCIONES

    @pytest.mark.parametrize("datos, estado, error", [
        ({}, "agenda_confirmada", "Elige qué pasó"),
        ({"tipo": "cancelar", "motivo": "duplicado"}, "retirada", "solo lectura"),
        ({"tipo": "no_vino", "motivo": "no_aviso", "accion": "esperar"}, "propuesta_enviada", "cita confirmada"),
        ({"tipo": "cancelar", "motivo": "inventado"}, "agenda_confirmada", "motivo de la lista"),
        ({"tipo": "cancelar", "motivo": "otro", "detalle": "x"}, "agenda_confirmada", "Otro motivo"),
        ({"tipo": "no_vino", "motivo": "no_aviso"}, "agenda_confirmada", "qué hacer"),
        ({"tipo": "no_vino", "motivo": "no_aviso", "accion": "cancelar"}, "agenda_confirmada", "qué hacer"),
    ])
    def test_lo_que_no_se_acepta(self, datos, estado, error):
        n, err = rdv.validar(datos, estado, tiene_cita=True, cita_vencida=True)
        assert n is None and error in err

    def test_no_vino_solo_con_la_cita_ya_vencida(self):
        # Revisión adversarial 2026-10-09: con la cita por venir, «No pudimos concretar tu retiro» le llegaría antes de su fecha
        n, err = rdv.validar({"tipo": "no_vino", "motivo": "no_aviso", "accion": "no_concretado", "avisar_cliente": True},
                             "agenda_confirmada", tiene_cita=True, cita_vencida=False)
        assert n is None and "cuando la cita ya pasó" in err

    def test_una_sola_accion_se_toma_sola_y_el_correo_solo_si_la_accion_lo_permite(self):
        n, _ = rdv.validar({"tipo": "incidencia", "motivo": "otra_persona", "avisar_cliente": True}, "en_preparacion", True)
        assert n["accion"] == "registrar" and n["estado_nuevo"] is None and n["avisar_cliente"] is False
        n, _ = rdv.validar({"tipo": "cancelar", "motivo": "desistio", "avisar_cliente": True}, "agenda_confirmada", True)
        assert n["accion"] == "cancelar" and n["estado_nuevo"] == "rechazada" and n["avisar_cliente"] is True
        n, _ = rdv.validar({"tipo": "no_vino", "motivo": "no_aviso", "accion": "no_concretado"}, "en_preparacion", True, True)
        assert n["estado_nuevo"] == "fallida" and n["avisar_cliente"] is False

    def test_el_detalle_se_limpia_y_se_acota(self):
        n, _ = rdv.validar({"tipo": "incidencia", "motivo": "otro", "detalle": "  llegó   con  camioneta \n chica " + "x" * 2000},
                           "agenda_confirmada", True)
        assert n["detalle"].startswith("llegó con camioneta chica") and len(n["detalle"]) == rdv.DETALLE_MAX

    @pytest.mark.parametrize("cita, ahora, vencida", [
        ({"confirmed_date": "2026-10-07", "confirmed_time_from": "09:00", "confirmed_time_to": "09:30"}, dt.datetime(2026, 10, 7, 9, 59), False),
        ({"confirmed_date": "2026-10-07", "confirmed_time_from": "09:00", "confirmed_time_to": "09:30"}, dt.datetime(2026, 10, 7, 10, 0), True),
        ({"confirmed_date": "2026-10-07", "confirmed_time_from": dt.timedelta(hours=9), "confirmed_time_to": None}, dt.datetime(2026, 10, 7, 10, 0), True),
        ({"confirmed_date": "2026-10-07"}, dt.datetime(2026, 10, 7, 23, 0), False),          # sin hora: vence al terminar el día
        ({"confirmed_date": "2026-10-06"}, dt.datetime(2026, 10, 7, 8, 0), True),
        ({"confirmed_date": "2026-10-08", "confirmed_time_from": "09:00"}, dt.datetime(2026, 10, 7, 23, 0), False),
    ])
    def test_cita_vencida(self, cita, ahora, vencida):
        assert rdv.cita_vencida(dict(cita, status="agenda_confirmada"), ahora) is vencida

    @pytest.mark.parametrize("hasta", [dt.timedelta(0), "08:30", "09:00"])
    def test_una_hora_de_termino_rara_no_adelanta_el_vencimiento(self, hasta):
        # 00:00 o un término antes del inicio: se toma el inicio + 30 min (no «vencida» desde la madrugada)
        req = {"status": "agenda_confirmada", "confirmed_date": "2026-10-07", "confirmed_time_from": "09:00", "confirmed_time_to": hasta}
        assert rdv.cita_vencida(req, dt.datetime(2026, 10, 7, 8, 0)) is False
        assert rdv.cita_vencida(req, dt.datetime(2026, 10, 7, 10, 0)) is True

    @pytest.mark.parametrize("estado", ["propuesta_enviada", "retirada", "cerrada", "rechazada", "fallida", "solicitud_recibida", "reagendada"])
    def test_sin_cita_vigente_nunca_esta_vencida(self, estado):
        assert rdv.cita_vencida({"status": estado, "confirmed_date": "2026-10-01"}, AHORA) is False

    def test_la_bitacora_dice_quien_que_y_si_se_aviso(self):
        n, _ = rdv.validar({"tipo": "no_vino", "motivo": "no_aviso", "accion": "no_concretado", "avisar_bodega": True}, "agenda_confirmada", True, True)
        t = rdv.texto_bitacora(n, "Milagros")
        for parte in ("El cliente no vino", "No se presentó y no avisó", "«No concretado»", "Aviso al cliente: no", "Aviso a bodega: sí",
                      "Decidió: Milagros"):
            assert parte in t


# ══════════════════════════════════════════════════════════════════════════════
#  La guía: con la cita vencida pregunta «¿Qué pasó?» en vez de «Enviar a preparación»
# ══════════════════════════════════════════════════════════════════════════════
class TestGuia:
    BASE = {"status": "agenda_confirmada", "n_docs": 1, "docs_firma": "a", "docs_conf": "a", "prod_n": 1, "prod_firma": "b", "prod_conf": "b",
            "responsable": "Milagros", "cita": True, "correo_ok": True}

    def test_con_la_cita_vigente_sigue_enviar_a_preparacion(self):
        g = rg.evaluar(dict(self.BASE))
        assert g["pasos"][4]["accion"]["tipo"] == "preparacion"

    def test_con_la_cita_vencida_pregunta_que_paso_y_no_ofrece_el_correo(self):
        g = rg.evaluar(dict(self.BASE, cita_vencida=True, cita_txt="07/10/2026 09:00"))
        p5 = g["pasos"][4]
        assert p5["accion"]["tipo"] == "que_paso" and p5["correo"] is False and "07/10/2026 09:00" in p5["faltan"][0]

    @pytest.mark.parametrize("preparado", [True, False])
    def test_en_preparacion_con_la_cita_vencida_pregunta_en_el_paso_que_toca(self, preparado):
        # Revisión técnica 2026-10-09: con el picking sin terminar, el «¿Qué pasó?» quedaba en un paso que no era el siguiente
        g = rg.evaluar(dict(self.BASE, status="en_preparacion", cita_vencida=True, preparado=preparado, picking_total=3, picking_hechos=1))
        assert g["pasos"][4]["accion"]["tipo"] == "que_paso" and g["siguiente"] == 5
        assert g["pasos"][5]["accion"]["tipo"] == "retirar"            # «Marcar como RETIRADO» sigue a mano

    def test_rechazada_ya_no_dice_que_el_cliente_rechazo(self):
        g = rg.evaluar(dict(self.BASE, status="rechazada"))
        assert "se canceló" in (g.get("terminal") or "") and "o lo canceló el equipo" in (g.get("terminal") or "")

    def test_el_js_con_check_listo_y_cita_vencida_no_empuja_el_correo(self):
        js = open(os.path.join(RAIZ, "static", "retiros_guia.js"), encoding="utf-8").read()
        i = js.index("function ajustarPorCheck()")
        cuerpo = js[i:js.index("function retirarDesdeCheck()", i)]
        assert "p5.accion.tipo === 'que_paso'" in cuerpo and "p6.correo = false" in cuerpo

    def test_el_js_de_la_guia_abre_el_modal(self):
        js = open(os.path.join(RAIZ, "static", "retiros_guia.js"), encoding="utf-8").read()
        assert "a.tipo === 'que_paso'" in js and "window.abrirQuePaso" in js


# ══════════════════════════════════════════════════════════════════════════════
#  GET /retiros/<id>/desviaciones
# ══════════════════════════════════════════════════════════════════════════════
class TestLectura:
    def test_dice_si_la_cita_vencio_y_a_quien_se_avisaria(self, env):
        env.db.config = {"aviso_bodega_emails": "bodega@ilusfitness.com, malo, jefe.bodega@sphs.cl"}
        retiro(env)
        d = env.cli.get(f"/retiros/{RID}/desviaciones").get_json()
        assert d["ok"] and d["vencida"] is True and d["tiene_cita"] is True and d["cita"] == "07/10/2026 09:00"
        assert d["bodega_emails"] == ["bodega@ilusfitness.com", "jefe.bodega@sphs.cl"]
        assert d["correo_cliente"] is True and d["terminado"] is False
        assert {t["clave"] for t in d["catalogo"]["tipos"]} == {"no_vino", "reagendar", "cancelar", "incidencia"}

    def test_leer_no_escribe_nada_ni_avisa(self, env):
        retiro(env)
        env.cli.get(f"/retiros/{RID}/desviaciones")
        assert [e for e in env.db.escrituras if not e[0].lower().startswith("create table")] == []
        esperar()
        assert env.esp.correo.call_args_list == [] and env.esp.mant_notif.call_args_list == []


# ══════════════════════════════════════════════════════════════════════════════
#  POST /retiros/<id>/desviacion
# ══════════════════════════════════════════════════════════════════════════════
class TestRegistrar:
    def test_no_vino_y_esperar_no_cambia_nada_ni_le_escribe_al_cliente(self, env):
        retiro(env)
        st, d = registrar(env, tipo="no_vino", motivo="no_aviso", accion="esperar", detalle="No contestó a las 10:00")
        assert st == 200 and d["ok"] and d["estado"] == "agenda_confirmada" and d["siguiente"] is None
        assert env.db.solicitudes[RID]["status"] == "agenda_confirmada"
        (f,) = env.db.desviaciones
        assert (f["tipo"], f["motivo"], f["accion"], f["usuario_nombre"]) == ("no_vino", "no_aviso", "esperar", "Samantha Blacio")
        (e,) = logs(env, "desviacion")
        assert "No se presentó y no avisó" in e["notes"] and "No contestó a las 10:00" in e["notes"] and e["actor_name"] == "Samantha Blacio"
        assert correos_a(env, CLIENTE) == []

    def test_no_concretado_sin_marcar_el_aviso_cierra_sin_correo_al_cliente(self, env):
        retiro(env)
        st, d = registrar(env, tipo="no_vino", motivo="no_contesta", accion="no_concretado")
        assert st == 200 and d["estado"] == "fallida" and d["cliente_avisado"] is False
        fila = env.db.solicitudes[RID]
        assert fila["status"] == "fallida" and fila["closed_at"]
        (e,) = logs(env, "estado_actualizado")
        assert (e["old_status"], e["new_status"]) == ("agenda_confirmada", "fallida") and e["notes"].startswith("Desviación")
        assert correos_a(env, CLIENTE) == []

    def test_no_concretado_con_el_aviso_le_llega_un_solo_correo_y_no_dice_cancelado(self, env):
        env.db.plantillas[("fallida", "email")] = {"asunto": "No pudimos concretar tu retiro {{code}}", "cuerpo": "<p>No pudimos concretar {{code}}</p>"}
        env.db.plantillas[("rechazada", "email")] = {"asunto": "Solicitud de retiro {{code}} cancelada", "cuerpo": "<p>cancelado</p>"}
        retiro(env)
        st, d = registrar(env, tipo="no_vino", motivo="no_aviso", accion="no_concretado", avisar_cliente=True)
        assert st == 200 and d["cliente_avisado"] is True
        (c,) = correos_a(env, CLIENTE)
        assert "No pudimos concretar" in c.args[1] and "cancelad" not in c.args[1].lower()

    def test_cancelar_con_el_aviso_usa_el_correo_de_cancelacion(self, env):
        env.db.plantillas[("rechazada", "email")] = {"asunto": "Solicitud de retiro {{code}} cancelada", "cuerpo": "<p>cancelado</p>"}
        retiro(env, status="propuesta_enviada", confirmed_date=None)
        env.db.propuestas.append({"id": 1, "request_id": RID, "status": "pending", "proposed_by": "ilus"})
        st, d = registrar(env, tipo="cancelar", motivo="duplicado", avisar_cliente=True)
        assert st == 200 and env.db.solicitudes[RID]["status"] == "rechazada"
        (c,) = correos_a(env, CLIENTE)
        assert "cancelada" in c.args[1]
        assert any("update `pickup_proposals` set status='superseded'" in e[0].lower() for e in env.db.escrituras)

    def test_reagendar_no_cambia_el_estado_y_lleva_a_la_agenda(self, env):
        retiro(env)
        st, d = registrar(env, tipo="reagendar", motivo="cliente_pidio", detalle="Pide el lunes")
        assert st == 200 and d["siguiente"] == "proponer" and env.db.solicitudes[RID]["status"] == "agenda_confirmada"
        assert correos_a(env, CLIENTE) == []

    def test_incidencia_queda_registrada_sin_tocar_el_retiro(self, env):
        retiro(env, status="en_preparacion")
        st, d = registrar(env, tipo="incidencia", motivo="entrega_parcial", detalle="Faltó 1 disco de 5 kg", avisar_cliente=True)
        assert st == 200 and d["estado"] == "en_preparacion" and env.db.solicitudes[RID]["status"] == "en_preparacion"
        assert env.db.desviaciones[0]["aviso_cliente"] == 0 and correos_a(env, CLIENTE) == []

    def test_lo_registrado_se_ve_al_volver_a_abrir(self, env):
        retiro(env)
        registrar(env, tipo="no_vino", motivo="aviso_no_puede", accion="esperar")
        d = env.cli.get(f"/retiros/{RID}/desviaciones").get_json()
        (r,) = d["registradas"]
        assert r["titulo"] == "El cliente no vino" and r["motivo"] == "Avisó que no podía venir" and r["quien"] == "Samantha Blacio"
        assert r["cuando"] == "07/10/2026 11:00"                        # hora Chile (REGLA #6)

    @pytest.mark.parametrize("datos", [{"tipo": "cancelar", "motivo": "otro"}, {"tipo": "no_vino", "motivo": "no_aviso"}, {"tipo": "x"}])
    def test_datos_incompletos_no_registran_nada(self, env, datos):
        retiro(env)
        st, d = registrar(env, **datos)
        assert st == 400 and not d["ok"] and env.db.desviaciones == [] and env.db.solicitudes[RID]["status"] == "agenda_confirmada"

    @pytest.mark.parametrize("estado", ["retirada", "cerrada", "rechazada", "fallida"])
    def test_un_retiro_terminado_queda_en_solo_lectura(self, env, estado):
        retiro(env, status=estado)
        st, d = registrar(env, tipo="incidencia", motivo="otra_persona")
        assert st == 409 and "solo lectura" in d["error"] and env.db.desviaciones == []

    def test_sin_responsable_no_se_cierra_pero_si_se_registra_una_incidencia(self, env, monkeypatch):
        retiro(env, responsable_user_id=None, responsable_nombre=None, created_at=dt.datetime(2026, 10, 7, 12, 0))
        st, d = registrar(env, tipo="cancelar", motivo="desistio")
        assert st == 409 and d.get("code") == "SIN_RESPONSABLE" and env.db.solicitudes[RID]["status"] == "agenda_confirmada"
        st, d = registrar(env, tipo="incidencia", motivo="llego_tarde")
        assert st == 200

    def test_si_el_retiro_cambio_mientras_tanto_no_se_pisa(self, env):
        retiro(env)
        env.db.al_leer.append((r"^select \* from `pickup_requests` where id=%s$",
                               lambda: env.db.solicitudes[RID].update(status="retirada")))      # una persona lo marcó retirado justo antes
        st, d = registrar(env, tipo="no_vino", motivo="no_aviso", accion="no_concretado", avisar_cliente=True)
        assert st == 409 and env.db.solicitudes[RID]["status"] == "retirada" and env.db.desviaciones == []
        assert correos_a(env, CLIENTE) == []

    def test_nunca_le_habla_a_check(self, env):
        retiro(env)
        registrar(env, tipo="no_vino", motivo="no_aviso", accion="no_concretado")
        assert env.esp.check.llamadas == []


class TestBodega:
    def test_avisa_a_bodega_que_no_expida_y_nunca_al_cliente(self, env):
        env.db.config = {"aviso_bodega_emails": "bodega@ilusfitness.com"}
        retiro(env, status="en_preparacion")
        st, d = registrar(env, tipo="no_vino", motivo="no_aviso", accion="esperar", avisar_bodega=True)
        assert st == 200 and d["bodega"]["ok"] is True and d["bodega"]["a"] == ["bodega@ilusfitness.com"]
        (c,) = correos_a(env, "bodega@ilusfitness.com")
        cuerpo = " ".join(env.ctx["_ilus_email_html"].call_args.kwargs["parrafos"])
        assert "NO lo expidan en Check" in cuerpo and "No se presentó y no avisó" in cuerpo
        assert correos_a(env, CLIENTE) == []

    def test_sin_correos_de_bodega_lo_dice_y_no_falla(self, env):
        retiro(env, status="en_preparacion")
        st, d = registrar(env, tipo="cancelar", motivo="desistio", avisar_bodega=True)
        assert st == 200 and d["bodega"]["ok"] is False and "No hay correos de bodega" in d["bodega"]["motivo"]
        assert env.db.solicitudes[RID]["status"] == "rechazada"

    def test_la_configuracion_guarda_los_correos_de_bodega_validos(self):
        src = open(os.path.join(RAIZ, "pickups_module.py"), encoding="utf-8").read()
        assert 'if "aviso_bodega_emails" in data:' in src and "ADD COLUMN aviso_bodega_emails" in src
        html = open(os.path.join(RAIZ, "templates", "retiros", "internal_dashboard.html"), encoding="utf-8").read()
        assert 'name="aviso_bodega_emails"' in html


# ══════════════════════════════════════════════════════════════════════════════
#  Cron: aviso de cita vencida al responsable (una vez por cita, nunca al cliente)
# ══════════════════════════════════════════════════════════════════════════════
def cron(env):
    return env.cli.get("/retiros/cron/check-barrido", headers={"X-Cron-Token": "secreto-cron"}).get_json()


class TestCitaVencida:
    def test_avisa_una_vez_al_responsable_y_al_equipo(self, env):
        retiro(env)
        d = cron(env)
        assert d["citas_vencidas"] == [env.db.solicitudes[RID]["code"]]
        (e,) = logs(env, "cita_vencida")
        assert "cita 07/10/2026 09:00" in e["notes"] and "al cliente no se le envía nada" in e["notes"]
        esperar()
        destinos = [c.kwargs.get("destino_user_id") for c in env.esp.mant_notif.call_args_list]
        assert 7 in destinos                                            # el responsable
        assert env.db.solicitudes[RID]["status"] == "agenda_confirmada"
        cron(env)
        cron(env)
        assert len(logs(env, "cita_vencida")) == 1
        assert correos_a(env, CLIENTE) == []

    def test_una_cita_nueva_del_mismo_retiro_se_vuelve_a_avisar(self, env):
        retiro(env)
        cron(env)
        env.db.solicitudes[RID].update(confirmed_time_from="10:00", confirmed_time_to="10:30")
        cron(env)
        assert len(logs(env, "cita_vencida")) == 2

    def test_una_cita_que_no_ha_pasado_no_avisa(self, env):
        retiro(env, confirmed_time_from="10:45", confirmed_time_to="11:15")
        assert cron(env)["citas_vencidas"] == [] and logs(env, "cita_vencida") == []

    def test_fuera_de_la_jornada_no_avisa(self, env):
        retiro(env)
        env.reloj["ahora"] = dt.datetime(2026, 10, 7, 21, 0)
        assert "citas_vencidas" not in cron(env) and logs(env, "cita_vencida") == []

    @pytest.mark.parametrize("estado", ["retirada", "fallida", "rechazada", "propuesta_enviada"])
    def test_solo_retiros_que_esperan_al_cliente(self, env, estado):
        retiro(env, status=estado)
        assert cron(env)["citas_vencidas"] == []

    def test_citas_de_hace_mas_de_3_dias_no_se_avisan(self, env):
        retiro(env, confirmed_date="2026-10-02")
        assert cron(env)["citas_vencidas"] == []


class TestFicha:
    def test_el_modal_solo_aparece_en_retiros_abiertos(self):
        html = open(os.path.join(RAIZ, "templates", "retiros", "internal_detail.html"), encoding="utf-8").read()
        i = html.index('{% include "retiros/_desviacion_modal.html" %}')
        bloque = html[html.rfind("{% if", 0, i): i]
        assert "permissions.retiros" in bloque and "'retirada','cerrada','rechazada','fallida'" in bloque

    def test_el_modal_no_usa_popups_nativos_y_pide_confirmacion(self):
        html = open(os.path.join(RAIZ, "templates", "retiros", "_desviacion_modal.html"), encoding="utf-8").read()
        for nativo in ("alert(", "confirm(", "prompt("):
            assert ("window." + nativo) not in html and (" " + nativo) not in html.replace("ilusConfirm(", "").replace("ilusAlert(", "")
        assert "ilusConfirm" in html and "Al cliente le llega un aviso" in html and "Su página de seguimiento mostrará" in html

    def test_el_correo_no_concretado_tiene_su_plantilla(self):
        src = open(os.path.join(RAIZ, "app.py"), encoding="utf-8").read()
        assert "('fallida', 'email'," in src and "No pudimos concretar tu retiro {{code}}" in src
        mod = open(os.path.join(RAIZ, "pickups_module.py"), encoding="utf-8").read()
        assert '"failed":            "fallida"' in mod


# ══════════════════════════════════════════════════════════════════════════════
#  Revisión adversarial 2026-10-09 (lente cliente)
# ══════════════════════════════════════════════════════════════════════════════
EMPEZO = A.respuesta_check(A.fila_check(solicitado=3, pickeado=1))


class TestRevision:
    def test_no_vino_con_la_cita_por_venir_se_rechaza_y_no_escribe(self, env):
        retiro(env, confirmed_time_from="15:00", confirmed_time_to="15:30")          # hoy a las 15:00; son las 11:00
        st, d = registrar(env, tipo="no_vino", motivo="no_aviso", accion="no_concretado", avisar_cliente=True)
        assert st == 400 and "cuando la cita ya pasó" in d["error"]
        assert env.db.solicitudes[RID]["status"] == "agenda_confirmada" and correos_a(env, CLIENTE) == []

    def test_con_la_cita_vencida_no_sale_estamos_preparando_solo(self, env):
        # Cita de hoy 09:00-09:30, son las 11:00 y bodega empieza a pickear: no pasa solo a preparación ni le escribe al cliente
        retiro(env)
        env.esp.check.respuestas["*"] = EMPEZO
        for _ in range(2):
            env.cli.get(f"/retiros/{RID}/check-preparacion")
        cron(env)
        assert env.db.solicitudes[RID]["status"] == "agenda_confirmada" and correos_a(env, CLIENTE) == []

    def test_con_la_cita_vigente_el_envio_automatico_sigue_igual(self, env):
        retiro(env, confirmed_time_from="15:00", confirmed_time_to="15:30")
        env.esp.check.respuestas["*"] = EMPEZO
        cron(env)
        assert env.db.solicitudes[RID]["status"] == "en_preparacion"

    def test_cambiar_estado_con_la_ficha_vieja_no_se_aplica(self, env):
        # A cerró como «No concretado»; B, con la ficha de antes, toca «Marcar como RETIRADO»
        retiro(env, status="fallida")
        r = env.cli.post(f"/retiros/{RID}/status", data={"status": "retirada", "estado_visto": "agenda_confirmada", "retirado_por": "X"})
        assert r.status_code == 302 and env.db.solicitudes[RID]["status"] == "fallida"
        assert correos_a(env, CLIENTE) == []
        r = env.cli.post(f"/retiros/{RID}/status", data={"status": "retirada", "estado_visto": "agenda_confirmada"},
                         headers={"X-Requested-With": "XMLHttpRequest"})
        assert r.status_code == 409 and r.get_json()["code"] == "ESTADO_CAMBIO"

    def test_los_formularios_de_la_ficha_mandan_el_estado_visto(self):
        html = open(os.path.join(RAIZ, "templates", "retiros", "internal_detail.html"), encoding="utf-8").read()
        assert html.count("url_for('pickup_update_status', rid=req.id)") == html.count('name="estado_visto" value="{{ req.status }}"')

    @pytest.mark.parametrize("datos, frase", [
        ({"tipo": "incidencia", "motivo": "otra_persona"}, "Se registró una incidencia"),
        ({"tipo": "reagendar", "motivo": "cliente_pidio"}, "se va a reagendar"),
        ({"tipo": "no_vino", "motivo": "no_aviso", "accion": "esperar"}, "no vino a su cita"),
        ({"tipo": "cancelar", "motivo": "desistio"}, "se cerró sin entrega"),
    ])
    def test_el_correo_a_bodega_dice_lo_que_corresponde(self, env, datos, frase):
        env.db.config = {"aviso_bodega_emails": "bodega@ilusfitness.com"}
        retiro(env, status="en_preparacion")
        st, d = registrar(env, avisar_bodega=True, **datos)
        assert st == 200
        cuerpo = " ".join(env.ctx["_ilus_email_html"].call_args.kwargs["parrafos"])
        assert frase in cuerpo

    def test_el_aviso_de_cita_vencida_se_puede_apagar(self, env, monkeypatch):
        monkeypatch.setenv("RETIROS_AVISO_CITA_VENCIDA", "0")
        retiro(env)
        assert "citas_vencidas" not in cron(env) and logs(env, "cita_vencida") == []

    def test_sin_responsable_la_bitacora_no_dice_que_se_aviso_al_responsable(self, env):
        retiro(env, responsable_user_id=None, responsable_nombre=None)
        cron(env)
        (e,) = logs(env, "cita_vencida")
        assert "no tiene responsable" in e["notes"]

    def test_reagendada_no_se_avisa_como_vencida(self, env):
        retiro(env, status="reagendada")
        assert cron(env)["citas_vencidas"] == []

    def test_el_correo_no_concretado_promete_lo_mismo_que_el_seguimiento(self):
        src = open(os.path.join(RAIZ, "app.py"), encoding="utf-8").read()
        i = src.index("('fallida', 'email',")
        assert "te contactaremos para coordinar una nueva fecha" in src[i:i + 1500]
        assert "fecha agendada" not in src[i:i + 1500]
        seg = open(os.path.join(RAIZ, "templates", "retiros", "public_tracking.html"), encoding="utf-8").read()
        assert "Nos pondremos en contacto para reagendar" in seg


class TestCampana:
    def test_todo_tipo_de_aviso_de_retiros_cabe_en_el_enum_de_la_campana(self):
        # Revisión técnica 2026-10-09: un tipo fuera del ENUM hace fallar el INSERT en silencio (modo estricto) y la campana nunca suena
        import re as _re
        mod = open(os.path.join(RAIZ, "pickups_module.py"), encoding="utf-8").read()
        usados = set(_re.findall(r'tipo=\(?"(retiro_[a-z_]+)"', mod)) | set(_re.findall(r'"(retiro_[a-z_]+)" if new_status', mod)) \
            | set(_re.findall(r'else "(retiro_[a-z_]+)"\)', mod))
        assert {"retiro_desviacion", "retiro_cita_vencida", "retiro_expedido", "retiro_desfase", "retiro_propuesta",
                "retiro_cierre", "retiro_anulado"} <= usados
        app_src = open(os.path.join(RAIZ, "app.py"), encoding="utf-8").read()
        enums = _re.findall(r"ALTER TABLE mant_notificaciones MODIFY COLUMN tipo \"\s*\"\s*ENUM\((.*?)\) DEFAULT 'otro'", app_src, _re.S)
        assert len(enums) == 2, "el ENUM se define en init_db y en _ensure_mant_notif_tipo_ot_firmada_cliente"
        for e in enums:
            valores = set(_re.findall(r"'([a-z_]+)'", e))
            assert usados <= valores, usados - valores

    def test_quien_entra_por_retiros_ve_solo_lo_suyo(self):
        app_src = open(os.path.join(RAIZ, "app.py"), encoding="utf-8").read()
        for ruta in ("/mantenciones/api/notif-interna\"", "/mantenciones/api/notif-interna/<int:nid>/leida", "/mantenciones/api/notif-interna/<int:nid>/archivar",
                     "/mantenciones/api/notif-interna/contador"):
            i = app_src.index(ruta)
            assert "@_campana_required" in app_src[i:i + 200], ruta
        assert app_src.count("_campana_solo_propias()") >= 5

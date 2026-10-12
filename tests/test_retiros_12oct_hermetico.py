# -*- coding: utf-8 -*-
"""2026-10-12 · Retiro cerrado = HERMÉTICO (Daniel: «el pedido se tiene que bloquear una vez que se retire completamente… tiene que ser un
sistema hermético», y «no generes correos ni alteres agenda»).

Brecha que se cerró: /retiros/<rid>/status dejaba sacar de su estado a un retiro terminado (retirada, cerrada, rechazada, fallida) desde el
Kanban, el calendario o «Cambiar estado», y eso le escribía al cliente (p. ej. arrastrar una tarjeta «Retirada» a Preparación → «estamos
preparando tu retiro»). Ahora:

- Desde el Kanban o el calendario (fetch) un retiro terminado no se mueve: 409, nada escrito, nada enviado — ni siquiera para un superadmin.
- Desde la ficha, solo un SUPERADMIN lo reabre, escribiendo el motivo (≥ 10 caracteres). Reabrir no le escribe al cliente ni avisa al equipo
  como si fuera un paso normal; queda en la bitácora con nombre y motivo.
- Volver a mandar el mismo estado (p. ej. «retirada» otra vez) no reescribe nada: ni la hora de cierre ni quién retiró.
- Con el retiro abierto todo sigue igual.

Todo corre sin BD real, sin Check, sin ERP y con correo/WhatsApp de mentira (tests/_arnes_retiros.py):

    py -m pytest tests/test_retiros_12oct_hermetico.py -q
"""
import datetime as dt
import os
import re
import sys
import threading
from types import SimpleNamespace

import pytest

_TESTS = os.path.dirname(os.path.abspath(__file__))
_RAIZ = os.path.dirname(_TESTS)
sys.path.insert(0, _TESTS)
sys.path.insert(0, _RAIZ)

import _arnes_retiros as A  # noqa: E402
import pickups_module  # noqa: E402

RID = 1
AHORA = dt.datetime(2026, 10, 12, 11, 0)
CIERRE_UTC = dt.datetime(2026, 10, 9, 18, 30)
TERMINALES = ["retirada", "cerrada", "rechazada", "fallida"]
SUPER = {"id": 1, "nombre": "Daniel Aguilar", "username": "daniel", "role": "superadmin"}
OPERADORA = {"id": 7, "nombre": "Samantha Blacio", "username": "sam@sphs.cl", "role": "supervisor"}
KANBAN = {"Sec-Fetch-Dest": "empty"}          # así llega el fetch del Kanban (FormData, sin X-Requested-With)
CALENDARIO = {"X-Requested-With": "XMLHttpRequest"}


def _armar(monkeypatch, usuario):
    for k in ("RETIROS_PREP_AUTO", "RETIROS_EXIGE_RESPONSABLE", "RETIROS_CHECK_AUTO", "RETIROS_RETIRO_AUTO"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setattr(pickups_module, "_RELOJ_CHILE", lambda: AHORA)
    hilos = []
    _start = threading.Thread.start

    def _start_espia(self):
        hilos.append(self.name)
        return _start(self)

    monkeypatch.setattr(threading.Thread, "start", _start_espia)
    app, db, ctx, esp = A.construir_app(usuario=usuario)
    return SimpleNamespace(app=app, db=db, ctx=ctx, esp=esp, cli=app.test_client(), hilos=hilos)


@pytest.fixture()
def env(monkeypatch):
    return _armar(monkeypatch, OPERADORA)


@pytest.fixture()
def env_super(monkeypatch):
    return _armar(monkeypatch, SUPER)


def retiro(env, estado, **kw):
    datos = dict(status=estado, responsable_user_id=7, responsable_nombre="Sam", confirmed_date="2026-10-09",
                 confirmed_time_from="11:00:00", document_number="0000023732", closed_at=CIERRE_UTC,
                 retirado_por_nombre="Gerd Muller", retirado_por_rut="11.111.111-1")
    datos.update(kw)
    env.db.nueva_solicitud(RID, **datos)
    env.db.agregar_doc(RID, "BLV", "0000023732")
    env.db.reiniciar_registro()
    del env.hilos[:]


def escrituras(env):
    return [e for e in env.db.escrituras if not e[0].upper().startswith(("CREATE", "ALTER"))]


def avisos_al_cliente(env):
    A.esperar_hilos_de_aviso()
    return [h for h in env.hilos if h.startswith("pickup-notify-")]


def avisos_al_equipo(env):
    A.esperar_hilos_de_aviso()
    return [h for h in env.hilos if h.startswith("pickup-team-notify")]


def nada_cambio(env, estado):
    assert env.db.solicitudes[RID]["status"] == estado
    assert env.db.solicitudes[RID]["closed_at"] == CIERRE_UTC
    assert env.db.solicitudes[RID]["retirado_por_nombre"] == "Gerd Muller"
    assert escrituras(env) == [], escrituras(env)
    assert avisos_al_cliente(env) == [] and avisos_al_equipo(env) == []
    assert env.esp.correo.call_args_list == [] and env.esp.whatsapp.call_args_list == []


# ── Kanban y calendario: un retiro terminado no se mueve, para nadie ──────────────────────────────────────────────────────────
class TestKanbanYCalendario:
    @pytest.mark.parametrize("estado", TERMINALES)
    @pytest.mark.parametrize("destino", ["en_preparacion", "agenda_confirmada", "rechazada", "fallida", "solicitud_recibida"])
    def test_arrastrar_una_tarjeta_terminada_da_409_y_no_escribe_ni_avisa(self, env, estado, destino):
        if destino == estado:
            return
        retiro(env, estado)
        r = env.cli.post(f"/retiros/{RID}/status", data={"status": destino, "nota": "kanban drag"}, headers=KANBAN)
        assert r.status_code == 409
        d = r.get_json()
        assert d["ok"] is False and d["code"] == "RETIRO_CERRADO"
        assert "superadministrador" in d["error"] and "No se cambió nada" in d["error"]
        nada_cambio(env, estado)

    def test_el_caso_real_retirada_a_preparacion_ya_no_le_escribe_al_cliente(self, env):
        retiro(env, "retirada")
        r = env.cli.post(f"/retiros/{RID}/status", data={"status": "en_preparacion"}, headers=KANBAN)
        assert r.status_code == 409
        nada_cambio(env, "retirada")
        assert not [x for x in env.db.escrituras if "pickup_picking" in x[0].lower()]      # tampoco rearma el checklist de bodega

    @pytest.mark.parametrize("estado", TERMINALES)
    def test_ni_un_superadmin_lo_mueve_desde_el_calendario(self, env_super, estado):
        retiro(env_super, estado)
        r = env_super.cli.post(f"/retiros/{RID}/status", data={"status": "agenda_confirmada", "notes": "desde calendario"},
                               headers=CALENDARIO)
        assert r.status_code == 409 and r.get_json()["code"] == "RETIRO_CERRADO"
        nada_cambio(env_super, estado)


# ── Ficha: solo el superadmin reabre, con motivo y sin escribirle al cliente ──────────────────────────────────────────────────
class TestReabrirDesdeLaFicha:
    @pytest.mark.parametrize("estado", TERMINALES)
    def test_una_persona_que_no_es_superadmin_no_lo_reabre(self, env, estado):
        retiro(env, estado)
        r = env.cli.post(f"/retiros/{RID}/status", data={"status": "agenda_confirmada", "estado_visto": estado,
                                                         "notes": "quiero reabrirlo por una razón cualquiera"})
        assert r.status_code == 302 and f"/retiros/{RID}" in r.headers["Location"]
        nada_cambio(env, estado)
        with env.cli.session_transaction() as s:
            avisos = [m for _c, m in s.get("_flashes", [])]
        assert any("Solo un superadministrador puede reabrirlo" in m for m in avisos), avisos

    @pytest.mark.parametrize("motivo", ["", "   ", "porque sí"])
    def test_el_superadmin_sin_motivo_no_lo_reabre(self, env_super, motivo):
        retiro(env_super, "retirada")
        r = env_super.cli.post(f"/retiros/{RID}/status", data={"status": "agenda_confirmada", "estado_visto": "retirada", "notes": motivo})
        assert r.status_code == 302
        nada_cambio(env_super, "retirada")
        with env_super.cli.session_transaction() as s:
            avisos = [m for _c, m in s.get("_flashes", [])]
        assert any("escribe el motivo" in m for m in avisos), avisos

    @pytest.mark.parametrize("estado", TERMINALES)
    @pytest.mark.parametrize("destino", ["agenda_confirmada", "en_preparacion", "en_revision", "rechazada", "fallida", "retirada"])
    def test_el_superadmin_con_motivo_lo_reabre_sin_avisar_a_nadie(self, env_super, estado, destino):
        if destino == estado:
            return
        retiro(env_super, estado)
        motivo = "El cliente vuelve por un producto que faltó"
        r = env_super.cli.post(f"/retiros/{RID}/status", data={"status": destino, "estado_visto": estado, "notes": motivo})
        assert r.status_code == 302
        assert env_super.db.solicitudes[RID]["status"] == destino
        assert avisos_al_cliente(env_super) == [], "reabrir no le escribe al cliente"
        assert avisos_al_equipo(env_super) == [], "reabrir no se anuncia como un paso normal («cita confirmada», «completado»…)"
        assert env_super.esp.correo.call_args_list == [] and env_super.esp.whatsapp.call_args_list == []
        log = [x for x in env_super.db.logs_de(RID, "estado_actualizado") if x["new_status"] == destino]
        assert log, "la reapertura queda en la bitácora"
        assert "Reabierto por Daniel Aguilar (superadmin)" in log[-1]["notes"] and motivo in log[-1]["notes"]
        assert "Sin aviso al cliente" in log[-1]["notes"]

    def test_despues_de_reabrir_el_retiro_se_gestiona_como_siempre(self, env_super):
        retiro(env_super, "retirada")
        env_super.cli.post(f"/retiros/{RID}/status", data={"status": "en_revision", "notes": "Hay que corregir la factura asociada"})
        A.esperar_hilos_de_aviso()
        env_super.db.reiniciar_registro()
        r = env_super.cli.patch(f"/retiros/{RID}/field", json={"field": "internal_notes", "value": "corregido"})
        assert r.status_code == 200 and r.get_json()["ok"] is True


# ── Mismo estado otra vez: no reescribe la evidencia ─────────────────────────────────────────────────────────────────────────
class TestMismoEstadoOtraVez:
    @pytest.mark.parametrize("usuario", ["env", "env_super"])
    def test_marcar_retirado_dos_veces_no_pisa_quien_retiro_ni_la_hora_de_cierre(self, request, usuario):
        e = request.getfixturevalue(usuario)
        retiro(e, "retirada")
        r = e.cli.post(f"/retiros/{RID}/status", data={"status": "retirada", "retirado_por": "Otra Persona",
                                                       "retirado_por_rut": "22.222.222-2", "notes": "otra vez"})
        assert r.status_code == 302
        nada_cambio(e, "retirada")
        assert e.db.solicitudes[RID]["retirado_por_rut"] == "11.111.111-1"

    def test_el_mismo_estado_desde_el_kanban_responde_409_sin_escribir(self, env):
        retiro(env, "cerrada")
        r = env.cli.post(f"/retiros/{RID}/status", data={"status": "cerrada"}, headers=KANBAN)
        assert r.status_code == 409 and "No se cambió nada" in r.get_json()["error"]
        nada_cambio(env, "cerrada")


# ── Con el retiro abierto, nada cambia ──────────────────────────────────────────────────────────────────────────────────────
class TestRetiroAbiertoSigueIgual:
    def test_una_operadora_sigue_cambiando_el_estado_de_un_retiro_abierto_y_el_cliente_recibe_su_aviso(self, env):
        retiro(env, "agenda_confirmada", closed_at=None)
        r = env.cli.post(f"/retiros/{RID}/status", data={"status": "rechazada", "estado_visto": "agenda_confirmada", "notes": "cliente canceló"})
        assert r.status_code == 302
        assert env.db.solicitudes[RID]["status"] == "rechazada"
        assert avisos_al_cliente(env), "control: el aviso normal al cliente sigue saliendo (el espía de hilos funciona)"

    def test_el_kanban_sigue_moviendo_retiros_abiertos(self, env):
        retiro(env, "solicitud_recibida", closed_at=None)
        r = env.cli.post(f"/retiros/{RID}/status", data={"status": "en_revision"}, headers=KANBAN)
        assert r.status_code == 302
        assert env.db.solicitudes[RID]["status"] == "en_revision"
        A.esperar_hilos_de_aviso()


# ── Pantallas: lo muestran cerrado ───────────────────────────────────────────────────────────────────────────────────────────
class TestPantallas:
    def _leer(self, *partes):
        with open(os.path.join(_RAIZ, *partes), encoding="utf-8") as f:
            return f.read()

    def test_el_kanban_no_deja_arrastrar_ni_mover_una_tarjeta_terminada(self):
        src = self._leer("templates", "retiros", "internal_dashboard.html")
        assert "{% set _kb_cerrado = r.status in ['retirada', 'cerrada', 'rechazada', 'fallida'] %}" in src
        assert "draggable=\"{{ 'false' if _kb_cerrado else 'true' }}\"" in src
        assert re.search(r"\{% if _kb_cerrado %\}\s*<span class=\"kanban-lock\"", src)
        drag = src.split("board.addEventListener('dragstart'")[1].split("});")[0]
        assert "e.preventDefault()" in drag and "'retirada', 'cerrada', 'rechazada', 'fallida'" in drag

    def test_la_ficha_cerrada_muestra_la_cita_fija_y_esconde_la_agenda_navegable(self):
        src = self._leer("templates", "retiros", "internal_detail.html")
        assert 'id="rdCitaCerrada"' in src
        assert '{% if cierre and cierre.cerrado %} hidden{% endif %}>' in src.split('id="sec-calendario"')[1].split("\n")[0]

    def test_quien_retira_no_es_un_boton_de_edicion_con_el_retiro_cerrado(self):
        src = self._leer("templates", "retiros", "internal_detail.html")
        bloque = src.split('{% if cierre and cierre.cerrado %}\n    {# Retiro cerrado (Daniel 2026-10-12, «hermético»)')[1].split("{% else %}")[0]
        assert '<div id="heroRetiraTile"' in bloque and "data-bs-toggle" not in bloque and "bi-pencil" not in bloque

    def test_cambiar_estado_solo_aparece_para_el_superadmin_con_motivo_obligatorio(self):
        src = self._leer("templates", "retiros", "internal_detail.html")
        pane = src.split('id="tab-acciones"')[1].split("</section>")[0]
        assert "{% if _cerrado_acc and not _es_super_acc %}" in pane and 'id="rdReabrirLock"' in pane
        assert 'required minlength="10"' in pane and "no le avisa nada al cliente" in pane
        assert 'data-reabrir="1"' in pane

    def test_el_js_pide_motivo_y_avisa_que_no_se_le_escribe_al_cliente(self):
        js = self._leer("static", "retiros_internal_detail.js")
        f = js.split("async function _confirmarCambioEstado")[1].split("\n}\n")[0]
        assert "form.dataset.reabrir === '1'" in f and "motivo.length < 10" in f and "ilusConfirm(" in f
        assert "Al cliente no le llega ningún correo" in f
        assert "alert(" not in f.replace("ilusAlert(", "") and "confirm(" not in f.replace("ilusConfirm(", "")

    def test_el_css_esconde_lo_que_solo_sirve_para_editar(self):
        css = self._leer("static", "retiros_internal_detail.css")
        assert 'body.rd-cerrado [onclick^="quitarDoc"]' in css and "body.rd-cerrado #saldoSugHead" in css
        assert ".rd-cita-cerrada" in css and ".rd-reabrir-lock" in css

    def test_el_candado_del_servidor_va_antes_de_cualquier_escritura(self):
        src = self._leer("pickups_module.py")
        cuerpo = src.split("def pickup_update_status(")[1].split("\n    @app.route")[0]
        i_candado = cuerpo.index("if old_status in _ESTADOS_TERMINADOS:")
        assert i_candado < cuerpo.index("mysql_execute("), "el candado tiene que ir antes del primer UPDATE"
        assert re.search(r"if _reapertura:[^\n]*\n\s+kind = None", cuerpo), "reabrir no le escribe al cliente"
        assert "if old_status != new_status and not _reapertura:" in cuerpo, "reabrir no avisa al equipo como un paso normal"

# -*- coding: utf-8 -*-
"""2026-10-16 · Cliente ADELANTADO = «Sí retiró» en «¿Qué pasó?» (Daniel, retiro de Inversiones Fit Paradise SpA: «me están llegando clientes
entre adelantados y yo atrasado, entonces necesito gestionarlos como retirados… en el ¿Qué pasó? solamente me da reagendar, cancelar o
registrar una incidencia»).

Un retiro abierto SIN cita confirmada se puede marcar RETIRADO con un motivo de la lista (obligatorio): queda en la bitácora quién, cuándo y
por qué. Sigue pidiendo responsable (REGLA #21). Sin el motivo, la guarda de secuencia de siempre (no se cierra sin cita) no cambia.

Todo con el arnés (correo de mentira, sin BD real, REGLA #22):

    py -m pytest tests/test_retiros_16oct_retiro_adelantado.py -q
"""
import os
import sys
import threading
import time
from types import SimpleNamespace

import pytest

_TESTS = os.path.dirname(os.path.abspath(__file__))
RAIZ = os.path.dirname(_TESTS)
sys.path.insert(0, _TESTS)
sys.path.insert(0, RAIZ)

import _arnes_retiros as A  # noqa: E402
import retiros_desviaciones as rdv  # noqa: E402

RID = 1
CLIENTE = "cliente.real@example.com"


@pytest.fixture()
def env(monkeypatch):
    for k in ("RETIROS_RETIRO_AUTO", "RETIROS_PREP_AUTO", "RETIROS_CHECK_AUTO", "RETIROS_EXIGE_RESPONSABLE"):
        monkeypatch.delenv(k, raising=False)
    app, db, ctx, esp = A.construir_app()
    ctx["_comm_render_email_document"] = lambda asunto, cuerpo: cuerpo
    return SimpleNamespace(app=app, db=db, ctx=ctx, esp=esp, cli=app.test_client())


def retiro(env, status="en_revision", **kw):
    kw.setdefault("responsable_user_id", 7)
    kw.setdefault("responsable_nombre", "Samantha Blacio")
    env.db.nueva_solicitud(RID, status=status, **kw)
    env.db.agregar_doc(RID, "FCV", "0000011878")
    env.db.reiniciar_registro()


def correos_al_cliente(env):
    t0 = time.time()
    while time.time() - t0 < 5:
        vivos = [t for t in threading.enumerate() if t.name.startswith(("pickup-notify", "pickup-team-notify"))]
        if not vivos:
            break
        for t in vivos:
            t.join(0.2)
    return [c for c in env.esp.correo.call_args_list if (c.args[0] or "").lower() == CLIENTE]


def marcar(env, estado_visto="en_revision", **extra):
    datos = {"status": "retirada", "estado_visto": estado_visto, "retirado_por": "Pedro Pérez", "retirado_por_rut": "11.111.111-1"}
    datos.update(extra)
    return env.cli.post(f"/retiros/{RID}/status", data=datos)


class TestReglas:
    @pytest.mark.parametrize("estado", ["solicitud_recibida", "en_revision", "informacion_incompleta", "esperando_cliente",
                                        "propuesta_enviada", "reagendada"])
    def test_abierto_sin_cita_es_adelantado(self, estado):
        assert rdv.retiro_adelantado(estado)

    @pytest.mark.parametrize("estado", ["agenda_confirmada", "en_preparacion", "retirada", "cerrada", "rechazada", "fallida", ""])
    def test_con_cita_o_terminado_no(self, estado):
        assert not rdv.retiro_adelantado(estado)

    def test_motivo_obligatorio_de_la_lista_y_detalle_con_otro(self):
        assert rdv.validar_adelantado("en_revision", "")[1]
        assert rdv.validar_adelantado("en_revision", "inventado")[1]
        assert rdv.validar_adelantado("en_revision", "otro", "x")[1]
        n, err = rdv.validar_adelantado("en_revision", "otro", "  vino   con su factura ")
        assert err is None and n["detalle"] == "vino con su factura"
        n, err = rdv.validar_adelantado("en_revision", "llego_antes")
        assert err is None and n["motivo_texto"].startswith("El cliente llegó a retirar antes")
        assert rdv.validar_adelantado("retirada", "llego_antes")[1]               # terminado: solo lectura
        assert rdv.validar_adelantado("agenda_confirmada", "llego_antes")[1]      # con cita: cierre normal

    def test_bitacora_dice_que_paso_y_quien(self):
        n, _ = rdv.validar_adelantado("en_revision", "bodega_entrego", "lo pidió Juan")
        t = rdv.texto_bitacora_adelantado(n, "En revisión", "Samantha Blacio")
        assert "sin cita confirmada" in t and "«En revisión»" in t and "Bodega lo entregó" in t
        assert "Detalle: lo pidió Juan" in t and "Decidió: Samantha Blacio" in t


class TestQuePaso:
    def test_ofrece_si_retiro_en_un_retiro_sin_cita(self, env):
        retiro(env)
        d = env.cli.get(f"/retiros/{RID}/desviaciones").get_json()
        assert d["puede_retirar"] is True and d["retiro_adelantado"] is True

    def test_con_la_cita_confirmada_sigue_siendo_el_cierre_normal(self, env):
        retiro(env, status="agenda_confirmada", confirmed_date="2026-10-21")
        d = env.cli.get(f"/retiros/{RID}/desviaciones").get_json()
        assert d["puede_retirar"] is True and d["retiro_adelantado"] is False

    def test_terminado_no_ofrece_nada(self, env):
        retiro(env, status="retirada")
        d = env.cli.get(f"/retiros/{RID}/desviaciones").get_json()
        assert d["puede_retirar"] is False and d["terminado"] is True


class TestCerrarAdelantado:
    def test_con_motivo_queda_retirado_con_bitacora_y_un_solo_correo(self, env):
        retiro(env)
        r = marcar(env, adelantado="1", adelantado_motivo="llego_antes", adelantado_detalle="Vino el jueves con la factura")
        assert r.status_code == 302
        fila = env.db.solicitudes[RID]
        assert fila["status"] == "retirada"
        assert any("Pedro Pérez" in (x.get("notes") or "") for x in env.db.logs_de(RID, "retiro_evidencia"))   # quién se lo llevó
        logs = env.db.logs_de(RID, "retiro_adelantado")
        assert len(logs) == 1 and "sin cita confirmada" in logs[0]["notes"] and "Vino el jueves" in logs[0]["notes"]
        assert "Decidió: Samantha Blacio" in logs[0]["notes"]
        assert len(correos_al_cliente(env)) == 1          # solo «Retiro completado»: nada de fecha ni «estamos preparando»

    def test_sin_motivo_no_cambia_nada_ni_escribe(self, env):
        retiro(env)
        marcar(env, adelantado="1", adelantado_motivo="")
        assert env.db.solicitudes[RID]["status"] == "en_revision"
        assert not env.db.logs_de(RID, "retiro_adelantado") and correos_al_cliente(env) == []

    def test_sin_la_marca_de_adelantado_la_guarda_de_siempre_sigue(self, env):
        retiro(env)
        marcar(env)
        assert env.db.solicitudes[RID]["status"] == "en_revision" and correos_al_cliente(env) == []

    def test_sin_responsable_no_se_cierra(self, env):
        retiro(env, responsable_user_id=None, responsable_nombre=None)
        marcar(env, adelantado="1", adelantado_motivo="llego_antes")
        assert env.db.solicitudes[RID]["status"] == "en_revision" and correos_al_cliente(env) == []

    def test_desde_el_kanban_tampoco_se_salta_el_motivo(self, env):
        retiro(env)
        r = env.cli.post(f"/retiros/{RID}/status", data={"status": "retirada", "adelantado": "1"},
                         headers={"X-Requested-With": "XMLHttpRequest"})
        assert r.status_code == 409 and r.get_json()["code"] == "ADELANTADO_SIN_MOTIVO"
        assert env.db.solicitudes[RID]["status"] == "en_revision"

    def test_con_la_cita_confirmada_no_deja_marca_de_adelantado(self, env):
        retiro(env, status="agenda_confirmada", confirmed_date="2026-10-21")
        marcar(env, estado_visto="agenda_confirmada")
        assert env.db.solicitudes[RID]["status"] == "retirada" and not env.db.logs_de(RID, "retiro_adelantado")


class TestFicha:
    def _ficha(self):
        with open(os.path.join(RAIZ, "templates", "retiros", "internal_detail.html"), encoding="utf-8") as f:
            return f.read()

    def test_el_modal_de_retirado_tambien_se_dibuja_sin_cita_y_pide_el_motivo(self):
        html = self._ficha()
        i = html.index('id="modalRetirar"')
        antes = html[max(0, i - 900):i]
        assert "or _rtm_adel" in antes and "'en_revision'" in antes and "'propuesta_enviada'" in antes
        m = html[i:html.index("</form>", i)]
        j = m.index("{% if _rtm_adel %}")
        bloque = m[j:m.index("{% endif %}", j)]
        assert 'name="adelantado" value="1"' in bloque and 'name="adelantado_motivo"' in bloque and "required" in bloque
        assert "retiro_motivos_adelantado()" in bloque and 'name="adelantado_detalle"' in bloque

    def test_la_firma_no_se_pide_en_el_adelantado(self):
        m = self._ficha()
        k = m.index("_firma_bloque.html")
        assert "not _rtm_adel and retiros_vista_previa is defined" in m[m.rfind("{% elif", 0, k):k]

    def test_que_paso_lleva_a_me_hago_cargo_sin_responsable(self):
        with open(os.path.join(RAIZ, "templates", "retiros", "_desviacion_modal.html"), encoding="utf-8") as f:
            js = f.read()
        assert "D.retiro_adelantado" in js and "btnTomarRetiro" in js and "Me hago cargo" in js

# -*- coding: utf-8 -*-
"""El TIEMPO ESTIMADO nunca se le muestra al cliente (Daniel 2026-10-06: «el tiempo estimado, tener precaución: no mostrárselo nunca al cliente»).

`tiempo_estimado_min` (lo calcula ILUS por líneas de la factura: máx(15, 5 + 2 por línea)) es de uso interno: la ficha, el Monitor y el modal
de «Proponer fecha» lo ven; el cliente NO. Estas pruebas fallan si vuelve a aparecer en:
  · la página de seguimiento (plantilla y datos que se le pasan),
  · el formulario público,
  · la API pública /retiros/seguimiento/<token>/status,
  · los correos y WhatsApp (variables de plantilla, también las de plantillas editadas en la BD).

    py -m pytest tests/test_retiros_07oct_tiempo.py -q
"""
import datetime as dt
import os
import re
import sys
from types import SimpleNamespace
from unittest import mock

import pytest

_TESTS = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _TESTS)
sys.path.insert(0, os.path.dirname(_TESTS))

import _arnes_retiros as A  # noqa: E402
import pickups_module  # noqa: E402

RID = 1
TOKEN = "tokenpublico" + "x" * 20
CLIENTE = "cliente.real@example.com"
TIEMPO = 47                                    # número reconocible: no coincide con ningún otro dato del retiro de prueba
RAIZ = os.path.dirname(_TESTS)


@pytest.fixture()
def env(monkeypatch):
    monkeypatch.setattr(pickups_module, "_RELOJ_CHILE", lambda: dt.datetime(2026, 10, 7, 11, 0))
    app, db, ctx, esp = A.construir_app()
    ctx["_comm_render_email_document"] = lambda asunto, cuerpo: cuerpo         # el correo final = el cuerpo de la plantilla
    db.nueva_solicitud(RID, status="en_preparacion", public_token=TOKEN, code="RET-TIEMPO", tiempo_estimado_min=TIEMPO,
                       peso_real_kg=16, total_volume_m3=0.005, confirmed_date="2026-10-08", contact_name="Cliente de Prueba",
                       document_number="0000023732", document_type="BLV")
    db.agregar_doc(RID, "BLV", "0000023732")
    db.reiniciar_registro()
    return SimpleNamespace(app=app, db=db, ctx=ctx, esp=esp, cli=app.test_client())


def leer(ruta):
    with open(os.path.join(RAIZ, ruta), encoding="utf-8") as f:
        return f.read()


# ── Lo que se le pasa al cliente ──────────────────────────────────────────────
class TestPaginaPublica:
    def test_los_datos_que_recibe_la_pagina_de_seguimiento_no_traen_el_tiempo(self, env):
        with mock.patch.object(pickups_module, "render_template", return_value="ok") as rt:
            assert env.cli.get(f"/retiros/seguimiento/{TOKEN}").status_code == 200
        req = rt.call_args.kwargs["req"]
        assert not [k for k in req if "tiempo" in k.lower()], f"la página pública recibe: {list(req)}"
        assert str(TIEMPO) not in repr(rt.call_args.kwargs.get("docs_asociados"))

    @pytest.mark.parametrize("ruta", ["templates/retiros/public_tracking.html", "templates/retiros/public_request.html"])
    def test_las_plantillas_publicas_no_pintan_el_tiempo_estimado(self, ruta):
        html = leer(ruta)
        assert not re.search(r"tiempo[_ ]?est", html, re.I), f"{ruta} menciona el tiempo estimado"
        assert "tiempo_estimado_min" not in html

    def test_la_api_publica_de_estado_no_lo_trae(self, env):
        d = env.cli.get(f"/retiros/seguimiento/{TOKEN}/status").get_json()
        assert d["ok"] is True
        assert not [k for k in d if "tiempo" in k.lower() or "minut" in k.lower()], f"campos: {list(d)}"
        assert str(TIEMPO) not in repr(d)


# ── Correos y WhatsApp ────────────────────────────────────────────────────────
VARIABLES_DE_TIEMPO = ("tiempo_estimado_min", "tiempo_estimado", "tiempo_est", "tiempo", "tiempo_carga", "duracion", "minutos", "demora")


def plantilla_con_todas_las_variables_de_tiempo():
    return {"asunto": "Retiro {{code}}",
            "cuerpo": "<p>Fecha {{fecha_confirmada}} {{horario}}</p>" + "".join(f"<i>«{{{{{v}}}}}»</i>" for v in VARIABLES_DE_TIEMPO)
                      + "<b>carga {{carga_txt}} {{kg}} kg</b>"}


class TestCorreosYWhatsapp:
    @pytest.mark.parametrize("tipo, estado", [("preparing", "en_preparacion"), ("confirmed", "agenda_confirmada"), ("done", "retirada"),
                                              ("proposal", "propuesta_enviada"), ("created", "solicitud_recibida"),
                                              ("reminder_24h", "recordatorio_24h")])
    def test_una_plantilla_de_la_bd_que_pida_el_tiempo_sale_sin_el(self, env, tipo, estado):
        env.db.plantillas[(estado, "email")] = plantilla_con_todas_las_variables_de_tiempo()
        req = dict(env.db.solicitudes[RID])
        req["contact_email"] = CLIENTE
        env.ctx["_pickup_notify"](req, tipo)
        A.esperar_hilos_de_aviso()
        enviados = [c for c in env.esp.correo.call_args_list if (c.args[0] or "").lower() == CLIENTE]
        assert enviados, "no salió el correo de prueba (el arnés no lo mandó a ningún lado real)"
        cuerpo = enviados[0].args[2]
        assert str(TIEMPO) not in cuerpo, "el correo trae el tiempo estimado"
        assert "{{" not in cuerpo                                   # y nada quedó sin reemplazar
        assert re.findall(r"«(.*?)»", cuerpo) == [""] * len(VARIABLES_DE_TIEMPO)    # cada variable de tiempo quedó vacía

    def test_la_plantilla_de_whatsapp_tampoco(self, monkeypatch):
        monkeypatch.setattr(pickups_module, "_RELOJ_CHILE", lambda: dt.datetime(2026, 10, 7, 11, 0))
        app, db, ctx, esp = A.construir_app(extra={"_canal_activo": lambda canal: True,
                                                  "_get_wa_cfg": lambda: {"account_sid": "x", "auth_token": "y", "from_number": "+10000000"}})
        db.plantillas[("en_preparacion", "whatsapp")] = {"asunto": "", "cuerpo": "Tu retiro {{code}} · [{{tiempo_estimado_min}}] [{{tiempo}}]"}
        req = {"id": RID, "code": "RET-TIEMPO", "status": "en_preparacion", "public_token": TOKEN, "tiempo_estimado_min": TIEMPO,
               "contact_phone": "+56900000000", "contact_email": "", "customer_name": "Cliente"}
        ctx["_pickup_notify"](req, "preparing")
        assert esp.whatsapp.call_count == 1
        cuerpo = esp.whatsapp.call_args.args[4]
        assert str(TIEMPO) not in cuerpo and "[] []" in cuerpo

    def test_ninguna_variable_de_plantilla_habla_de_tiempo(self, env):
        """Con una plantilla que lista TODAS las variables que existen, ninguna lleva un número de minutos."""
        env.db.plantillas[("en_preparacion", "email")] = {
            "asunto": "x", "cuerpo": "".join(f"<i>[{{{{{k}}}}}]</i>" for k in (
                "code", "cliente", "persona_retira", "documento", "fecha_solicitada", "fecha_propuesta", "fecha_confirmada", "horario",
                "n_bultos", "kg", "m3", "carga_txt", "warehouse_name", "warehouse_addr", "link_seguimiento", "productos_html",
                "mensaje_propuesta", "fecha_retiro"))}
        req = dict(env.db.solicitudes[RID])
        req["contact_email"] = CLIENTE
        env.ctx["_pickup_notify"](req, "preparing")
        A.esperar_hilos_de_aviso()
        cuerpo = [c for c in env.esp.correo.call_args_list if (c.args[0] or "").lower() == CLIENTE][0].args[2]
        assert re.search(rf"(?<![\d.,]){TIEMPO}(?![\d])", re.sub(r"https?://\S+", "", cuerpo)) is None


class TestFuenteSinTiempoHaciaElCliente:
    def test_el_codigo_que_arma_las_variables_descarta_cualquier_variable_de_tiempo(self):
        fuente = leer("pickups_module.py")
        i = fuente.index("def _render_pickup_vars(")
        bloque = fuente[i:fuente.index("def _apply_template(", i)]
        assert '"tiempo"' in bloque and "_vars.pop(" in bloque
        assert not re.search(r'"[a-z_]*tiempo[a-z_]*"\s*:', bloque.split("_vars = {")[1].split("# El TIEMPO")[0]), \
            "se agregó una variable de tiempo a las que van al cliente"

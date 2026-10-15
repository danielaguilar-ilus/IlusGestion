# -*- coding: utf-8 -*-
"""2026-10-15 · Firma de recepción por WhatsApp (Daniel: «escribiendo un número de teléfono para que firme y quede guardado en el sistema…
antes del modal registrar la firma me indique un número de teléfono… y ahí tú te conectas a WhatsApp por la aplicación»).

  · POST /retiros/<rid>/firma/enlace → enlace de UN uso (48 h) + wa.me con el mensaje listo. El sistema NO envía nada.
  · GET/POST /retiros/firmar/<token> → página pública; misma validación y hash que la firma en la ficha (_firma_registrar).
  · GET /retiros/<rid>/firma/estado → la ficha espera la firma y se recarga sola.

Sin BD real, sin WhatsApp ni correo reales:   py -m pytest tests/test_retiros_15oct_firma_whatsapp.py -q
"""
import datetime as dt
import os
import re
import sys
from types import SimpleNamespace
from urllib.parse import unquote

import pytest

_TESTS = os.path.dirname(os.path.abspath(__file__))
RAIZ = os.path.dirname(_TESTS)
sys.path.insert(0, _TESTS)
sys.path.insert(0, RAIZ)

import _arnes_retiros as A  # noqa: E402
from test_retiros_firma import cuerpo, png_data_url  # noqa: E402,F401

RID = 1


@pytest.fixture()
def env(monkeypatch):
    for k in ("RETIROS_PREP_AUTO", "RETIROS_EXIGE_RESPONSABLE", "RETIROS_CHECK_AUTO", "RETIROS_RETIRO_AUTO", "RETIROS_FIRMA_CORREO"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("RETIROS_VISTA_PREVIA_USUARIOS", "sam@sphs.cl")      # el usuario del arnés ve la vista previa
    app, db, ctx, esp = A.construir_app()
    return SimpleNamespace(app=app, db=db, esp=esp, cli=app.test_client(), mp=monkeypatch)


def retiro(env, estado="retirada", **kw):
    datos = dict(status=estado, responsable_user_id=7, responsable_nombre="Sam", confirmed_date="2026-10-14",
                 document_number="0000023732", pickup_person_name="Ana Pérez", pickup_person_rut="11.111.111-1")
    datos.update(kw)
    env.db.nueva_solicitud(RID, **datos)
    env.db.agregar_doc(RID, "BLV", "0000023732")
    env.db.reiniciar_registro()


def enlace(env, tel="9 1234 5678"):
    return env.cli.post(f"/retiros/{RID}/firma/enlace", json={"telefono": tel})


def token_de(r):
    return r.get_json()["url"].rsplit("/", 1)[1]


def nada_automatico(env):
    A.esperar_hilos_de_aviso()
    assert env.esp.correo.call_args_list == [] and env.esp.whatsapp.call_count == 0


class TestCrearEnlace:
    def test_crea_un_enlace_de_un_uso_y_devuelve_el_whatsapp_listo_sin_enviar_nada(self, env):
        retiro(env)
        r = enlace(env, "+56 9 1234 5678")
        d = r.get_json()
        assert r.status_code == 200 and d["ok"]
        assert d["wa_url"].startswith("https://wa.me/56912345678?text=")
        texto = unquote(d["wa_url"].split("text=", 1)[1])
        assert "RET-T00001" in texto and d["url"] in texto and "ILUS Fitness" in texto
        assert d["telefono"] == "+56 9 1234 5678" and re.fullmatch(r"\d{2}/\d{2}/\d{4} \d{2}:\d{2}", d["vence"])
        e = env.db.enlaces[0]
        assert e["telefono"] == "56912345678" and len(e["token_hash"]) == 64 and token_de(r) not in e["token_hash"]
        assert any(l["action"] == "firma_enlace" for l in env.db.logs)
        nada_automatico(env)                                   # el colaborador aprieta «enviar» en SU WhatsApp

    @pytest.mark.parametrize("tel", ["", "12345", "+1 202 555 0100", "2 2345 6789"])
    def test_celular_invalido(self, env, tel):
        retiro(env)
        assert enlace(env, tel).status_code == 400 and env.db.enlaces == []

    @pytest.mark.parametrize("estado", ["solicitud_recibida", "en_revision", "rechazada", "fallida"])
    def test_solo_al_entregar(self, env, estado):
        retiro(env, estado)
        assert enlace(env).status_code == 409 and env.db.enlaces == []

    def test_sin_vista_previa_no(self, env):
        env.mp.setenv("RETIROS_VISTA_PREVIA_USUARIOS", "nadie@ejemplo.cl")
        retiro(env)
        assert enlace(env).status_code == 403

    def test_ya_firmado_no(self, env):
        retiro(env)
        assert env.cli.post(f"/retiros/{RID}/firma", json=cuerpo()).status_code == 200
        assert enlace(env).status_code == 409

    def test_un_enlace_nuevo_deja_sin_efecto_el_anterior(self, env):
        retiro(env)
        t1 = token_de(enlace(env))
        enlace(env, "9 8765 4321")
        r = env.cli.get(f"/retiros/firmar/{t1}")
        assert r.status_code == 410 and "venció" in r.get_data(as_text=True)


class TestPaginaPublica:
    def test_muestra_el_retiro_y_lo_que_recibe(self, env):
        retiro(env)
        r = env.cli.get(f"/retiros/firmar/{token_de(enlace(env))}")
        html = r.get_data(as_text=True)
        assert r.status_code == 200 and "Firma la recepción de tu retiro" in html and "RET-T00001" in html
        assert "Ana Pérez" in html and "Ley 21.719" in html
        assert r.headers["Cache-Control"] == "no-store" and "noindex" in r.headers["X-Robots-Tag"]

    def test_firmar_guarda_la_firma_una_sola_vez_y_marca_el_enlace(self, env):
        retiro(env)
        tok = token_de(enlace(env))
        r = env.cli.post(f"/retiros/firmar/{tok}", json=cuerpo(nombre="Ana Pérez"))
        assert r.status_code == 200 and r.get_json() == {"ok": True}         # al público no se le devuelve hash ni comprobante
        f = env.db.firmas[RID]
        assert f["firmante_nombre"] == "Ana Pérez" and f["creado_por_nombre"] == "Firma remota por WhatsApp"
        assert env.db.enlaces[0]["usado_en"] is not None
        assert any("firmado por WhatsApp" in (l.get("notes") or "") for l in env.db.logs if l["action"] == "firma_recepcion")
        r2 = env.cli.post(f"/retiros/firmar/{tok}", json=cuerpo(nombre="Otra Persona"))
        assert r2.status_code == 410 and env.db.firmas[RID]["firmante_nombre"] == "Ana Pérez"    # una firma nunca se pisa
        g = env.cli.get(f"/retiros/firmar/{tok}")
        assert g.status_code == 200 and "Este retiro ya está firmado" in g.get_data(as_text=True)
        nada_automatico(env)

    def test_valida_igual_que_en_la_ficha(self, env):
        retiro(env)
        tok = token_de(enlace(env))
        assert env.cli.post(f"/retiros/firmar/{tok}", json=cuerpo(rut="12.345.678-0")).status_code == 400
        assert env.cli.post(f"/retiros/firmar/{tok}", json=cuerpo(firma="data:image/png;base64,AAAA")).status_code == 400
        assert RID not in env.db.firmas and env.db.enlaces[0]["usado_en"] is None

    def test_enlace_vencido_o_inventado(self, env):
        retiro(env)
        tok = token_de(enlace(env))
        env.db.enlaces[0]["vence_en"] = dt.datetime(2020, 1, 1)
        assert env.cli.get(f"/retiros/firmar/{tok}").status_code == 410
        assert env.cli.post(f"/retiros/firmar/{tok}", json=cuerpo()).status_code == 410
        assert env.cli.get("/retiros/firmar/" + "x" * 32).status_code == 410
        assert env.cli.get("/retiros/firmar/corto").status_code == 410
        assert RID not in env.db.firmas


class TestEstado:
    def test_la_ficha_sabe_si_firmo(self, env):
        retiro(env, "en_preparacion")
        d0 = env.cli.get(f"/retiros/{RID}/firma/estado").get_json()
        assert d0["ok"] and d0["firmada"] is False and d0["enlace"] is None
        tok = token_de(enlace(env))
        d1 = env.cli.get(f"/retiros/{RID}/firma/estado").get_json()
        assert d1["enlace"]["telefono"] == "+56 9 1234 5678"
        env.cli.post(f"/retiros/firmar/{tok}", json=cuerpo())
        assert env.cli.get(f"/retiros/{RID}/firma/estado").get_json()["firmada"] is True


class TestCodigo:
    def test_csrf_exenta_solo_la_pagina_publica_de_firma(self):
        with open(os.path.join(RAIZ, "app.py"), encoding="utf-8") as f:
            src = f.read()
        patron = re.search(r're\.fullmatch\(r"(/retiros/firmar/[^"]+)"', src).group(1)
        assert re.fullmatch(patron, "/retiros/firmar/" + "a" * 32)
        assert not re.fullmatch(patron, "/retiros/1/firma/enlace") and not re.fullmatch(patron, "/retiros/firmar/x/../../admin")

    def test_la_ficha_pide_el_celular_antes_de_firmar(self):
        with open(os.path.join(RAIZ, "templates", "retiros", "internal_detail.html"), encoding="utf-8") as f:
            html = f.read()
        i = html.find('id="modalFirmaWsp"')
        assert i > 0 and "retiros_vista_previa()" in html[html.rfind("{% if", 0, i):i]
        assert "¿A qué celular le enviamos el enlace?" in html and "Enviar por WhatsApp" in html
        with open(os.path.join(RAIZ, "static", "retiros_firma.js"), encoding="utf-8") as f:
            js = f.read()
        assert "/firma/enlace" in js and "/firma/estado" in js and "if (enlazarFirmaWsp()) abrir = null;" in js

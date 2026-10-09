# -*- coding: utf-8 -*-
"""2026-10-07 · Firma digital de recepción + comprobante público (Daniel, dueño).

Todo corre sin BD real, sin Check, sin ERP y con el correo de mentira (tests/_arnes_retiros.py):

    py -m pytest tests/test_retiros_firma.py -q
"""
import base64
import os
import struct
import sys
import zlib
from types import SimpleNamespace

import pytest

_TESTS = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _TESTS)
sys.path.insert(0, os.path.dirname(_TESTS))

import _arnes_retiros as A  # noqa: E402
import retiros_firma as F  # noqa: E402

RID = 1
RUT_OK = "12.345.678-5"


def png_data_url(ancho=200, alto=80, relleno=0):
    """PNG real mínimo (escala de grises). `relleno` agrega bytes de ruido para probar el tope de tamaño."""
    def chunk(tipo, datos):
        c = struct.pack(">I", len(datos)) + tipo + datos
        return c + struct.pack(">I", zlib.crc32(tipo + datos) & 0xffffffff)
    crudo = b"".join(b"\x00" + os.urandom(ancho) if relleno else b"\x00" + bytes(ancho) for _ in range(alto))
    png = b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", struct.pack(">IIBBBBB", ancho, alto, 8, 0, 0, 0, 0)) + \
        chunk(b"IDAT", zlib.compress(crudo)) + chunk(b"IEND", b"")
    return "data:image/png;base64," + base64.b64encode(png).decode()


def cuerpo(**kw):
    d = {"nombre": "Gerd Müller", "rut": RUT_OK, "relacion": "cliente", "conformidad": True, "observaciones": "", "firma": png_data_url()}
    d.update(kw)
    return d


@pytest.fixture()
def env(monkeypatch):
    for k in ("RETIROS_PREP_AUTO", "RETIROS_EXIGE_RESPONSABLE", "RETIROS_CHECK_AUTO", "RETIROS_RETIRO_AUTO", "RETIROS_FIRMA_CORREO"):
        monkeypatch.delenv(k, raising=False)
    # Vista previa (2026-10-09: por ahora solo Daniel ve la firma): estas pruebas son del funcionamiento, con el usuario del arnés habilitado;
    # el candado «solo Daniel» tiene sus propias pruebas al final del archivo.
    monkeypatch.setenv("RETIROS_VISTA_PREVIA_USUARIOS", "sam@sphs.cl")
    app, db, ctx, esp = A.construir_app()
    return SimpleNamespace(app=app, db=db, ctx=ctx, esp=esp, cli=app.test_client(), mp=monkeypatch)


def retiro(env, estado, **kw):
    datos = dict(status=estado, responsable_user_id=7, responsable_nombre="Sam", confirmed_date="2026-10-08", document_number="0000023732")
    datos.update(kw)
    env.db.nueva_solicitud(RID, **datos)
    env.db.agregar_doc(RID, "BLV", "0000023732")
    env.db.reiniciar_registro()


def firmar(env, **kw):
    return env.cli.post(f"/retiros/{RID}/firma", json=cuerpo(**kw))


def inserts_firma(env):
    return [e for e in env.db.escrituras if e[0].lower().startswith("insert into pickup_firmas")]


def token(env):
    return F.generar_token(env.app.secret_key, RID)


# ── firmar ──────────────────────────────────────────────────────────────
@pytest.mark.parametrize("estado", ["en_preparacion", "agenda_confirmada"])
def test_firmar_con_el_retiro_abierto_guarda_y_no_manda_correo(env, estado):
    retiro(env, estado)
    r = firmar(env)
    A.esperar_hilos_de_aviso()
    d = r.get_json()
    assert r.status_code == 200 and d["ok"] and d["comprobante_enviado"] is False
    fila = env.db.firmas[RID]
    assert fila["firmante_rut"] == "12345678-5" and fila["firmante_nombre"] == "Gerd Müller" and fila["creado_por_nombre"] == "Samantha Blacio"
    assert len(fila["hash_sha256"]) == 64 and "DISCO25" in fila["productos_json"]
    assert env.esp.correo.call_count == 0 and env.esp.whatsapp.call_count == 0
    assert env.db.logs_de(RID, "firma_recepcion")
    assert not env.db.logs_de(RID, "comprobante_enviado")


def test_firmar_con_estado_que_no_corresponde_no_escribe(env):
    retiro(env, "solicitud_recibida")
    r = firmar(env)
    assert r.status_code == 409 and not inserts_firma(env)


# ── retiro cerrado: excepción al solo-lectura + correo apagado por defecto ──
def test_cerrado_sin_firma_se_puede_firmar_pero_el_correo_esta_apagado_por_defecto(env):
    retiro(env, "retirada")
    r = firmar(env)
    A.esperar_hilos_de_aviso()
    assert r.status_code == 200 and r.get_json()["comprobante_enviado"] is False
    assert RID in env.db.firmas
    assert env.esp.correo.call_count == 0 and env.esp.whatsapp.call_count == 0
    avisos = env.db.logs_de(RID, "comprobante_no_enviado")
    assert len(avisos) == 1 and "correo al cliente desactivado" in avisos[0]["notes"]


@pytest.mark.parametrize("valor", ["", "0", "true", "yes", " 1"])
def test_solo_el_valor_exacto_1_enciende_el_correo(env, valor):
    env.mp.setenv("RETIROS_FIRMA_CORREO", valor)
    retiro(env, "cerrada")
    firmar(env)
    A.esperar_hilos_de_aviso()
    assert env.esp.correo.call_count == 0


def test_con_la_variable_en_1_se_manda_un_solo_comprobante(env):
    env.mp.setenv("RETIROS_FIRMA_CORREO", "1")
    retiro(env, "retirada")
    r = firmar(env)
    A.esperar_hilos_de_aviso()
    assert r.status_code == 200 and r.get_json()["comprobante_enviado"] is True
    assert env.esp.correo.call_count == 1
    destino, asunto, html = env.esp.correo.call_args.args[:3]
    assert destino == "cliente.real@example.com"
    assert "Comprobante de tu retiro RET-T00001" in asunto
    assert "/retiros/comprobante/" + token(env) in html and "Ver comprobante" in html and "Gerd Müller" in html
    assert env.db.logs_de(RID, "comprobante_enviado")
    # una segunda firma no se acepta ni manda nada más
    r2 = firmar(env)
    A.esperar_hilos_de_aviso()
    assert r2.status_code == 409 and env.esp.correo.call_count == 1


def test_enviar_comprobante_no_repite_si_la_bitacora_ya_lo_tiene(env):
    env.mp.setenv("RETIROS_FIRMA_CORREO", "1")
    retiro(env, "retirada")
    firmar(env)
    A.esperar_hilos_de_aviso()
    enviar = env.ctx.get("_enviar_comprobante")           # solo existe si el módulo lo expone; si no, basta con lo anterior
    if callable(enviar):
        enviar(RID, env.db.solicitudes[RID])
    assert env.esp.correo.call_count == 1


def test_kill_switch_de_correo_apagado_no_envia(env):
    env.mp.setenv("RETIROS_FIRMA_CORREO", "1")
    env.ctx["comm_is_enabled"] = lambda canal: False
    retiro(env, "retirada")
    firmar(env)
    A.esperar_hilos_de_aviso()
    assert env.esp.correo.call_count == 0 and env.db.logs_de(RID, "comprobante_no_enviado")


# ── una firma es evidencia ─────────────────────────────────────────────
def test_segunda_firma_da_409_y_no_escribe(env):
    retiro(env, "en_preparacion")
    assert firmar(env).status_code == 200
    env.db.reiniciar_registro()
    r = firmar(env, nombre="Otra Persona")
    assert r.status_code == 409 and r.get_json()["code"] == "FIRMA_YA_REGISTRADA"
    assert not inserts_firma(env) and env.db.firmas[RID]["firmante_nombre"] == "Gerd Müller"
    assert not [e for e in env.db.escrituras if e[0].lower().startswith(("update", "delete"))]


def test_carrera_unique_devuelve_409(env):
    """Otra pestaña insertó entre la lectura y el INSERT: el UNIQUE rechaza y el servidor responde 409 sin pisar nada."""
    retiro(env, "en_preparacion")
    leidas = {"n": 0}
    original = env.db._fetchone

    def mentira(sql, params=()):
        if sql.lower().startswith("select * from pickup_firmas"):
            leidas["n"] += 1
            if leidas["n"] == 1:
                return None                                  # la primera lectura (antes de insertar) no la ve
        return original(sql, params)
    env.db._fetchone = mentira
    env.db.firmas[RID] = {"request_id": RID, "firmante_nombre": "Primera", "hash_sha256": "x" * 64}
    r = firmar(env)
    assert r.status_code == 409 and env.db.firmas[RID]["firmante_nombre"] == "Primera"


# ── validaciones: 400 sin escribir ─────────────────────────────────────
@pytest.mark.parametrize("campo,valor", [
    ("rut", "12.345.678-9"), ("rut", ""), ("rut", "abc"), ("nombre", " "), ("relacion", "hacker"),
    ("observaciones", "x" * 501),
    ("firma", ""), ("firma", "data:image/jpeg;base64,AAAA"), ("firma", "data:image/png;base64,@@@@"),
    ("firma", "data:image/png;base64," + base64.b64encode(b"no es un png" * 20).decode()),
    ("firma", "data:image/svg+xml;base64," + base64.b64encode(b"<svg onload=alert(1)>").decode()),
])
def test_datos_invalidos_dan_400_y_no_escriben(env, campo, valor):
    retiro(env, "en_preparacion")
    r = firmar(env, **{campo: valor})
    assert r.status_code == 400 and r.get_json()["ok"] is False
    assert not inserts_firma(env) and not env.db.firmas


def test_png_enorme_da_400(env):
    retiro(env, "en_preparacion")
    r = firmar(env, firma=png_data_url(ancho=1500, alto=400, relleno=1))      # ruido: no comprime, pasa de 300 KB
    assert r.status_code == 400 and not env.db.firmas


def test_rut_con_k_y_formato_libre(env):
    assert F.rut_normalizado("12345678-5") == "12345678-5" and F.rut_formateado("123456785") == "12.345.678-5"
    assert F.rut_normalizado("11.111.111-1") == "11111111-1"
    assert F.rut_normalizado("11.111.111-2") is None


# ── comprobante público ────────────────────────────────────────────────
def test_comprobante_con_token_valido(env):
    retiro(env, "retirada", customer_name="Cliente de Prueba")
    firmar(env, observaciones="Una caja con el empaque dañado")
    r = env.cli.get("/retiros/comprobante/" + token(env))
    html = r.get_data(as_text=True)
    assert r.status_code == 200
    for esperado in ("RET-T00001", "Sport and Health Solutions SPA", "76.996.964-0", "ILUS Fitness", "12.345.678-5", "Gerd Müller",
                     "DISCO25", "Set Discos 2,5 - 5 kg", "Mancuerna hexagonal 10 kg", "data:image/png;base64,", "empaque dañado",
                     "Logística verde", "coincide con el código", "@media print"):
        assert esperado in html, esperado
    assert "no-store" in r.headers["Cache-Control"] and "noindex" in r.headers["X-Robots-Tag"]
    assert "retirada" not in html          # nada interno del retiro


def test_hora_del_comprobante_es_hora_chile(env):
    import datetime as dt
    retiro(env, "retirada")
    firmar(env)
    env.db.firmas[RID]["creado_en"] = dt.datetime(2026, 10, 7, 18, 30, 0)        # 18:30 UTC → 15:30 en Chile (UTC-3 en octubre)
    html = env.cli.get("/retiros/comprobante/" + token(env)).get_data(as_text=True)
    assert "07/10/2026 15:30" in html


@pytest.mark.parametrize("malo", ["", "basura", "1-" + "0" * 24, "zz-" + "a" * 24, "1-" + "A" * 24, "../../etc/passwd"])
def test_token_invalido_da_404(env, malo):
    retiro(env, "retirada")
    firmar(env)
    r = env.cli.get("/retiros/comprobante/" + malo)
    assert r.status_code == 404 and "Gerd" not in r.get_data(as_text=True)


def test_token_de_otro_retiro_o_sin_firma_da_404(env):
    retiro(env, "retirada")
    assert env.cli.get("/retiros/comprobante/" + token(env)).status_code == 404       # token auténtico pero sin firma
    firmar(env)
    t = token(env)
    assert env.cli.get("/retiros/comprobante/" + t).status_code == 200
    assert env.cli.get("/retiros/comprobante/" + t[:-1] + ("0" if t[-1] != "0" else "1")).status_code == 404   # un carácter cambiado
    assert env.cli.get("/retiros/comprobante/" + F.generar_token(env.app.secret_key, 2)).status_code == 404    # retiro que no existe
    assert env.cli.get("/retiros/comprobante/" + F.generar_token("otra-clave", RID)).status_code == 404         # firmado con otra clave


def test_el_token_no_es_el_public_token_del_seguimiento(env):
    retiro(env, "retirada", public_token="tok1")
    assert "tok1" not in token(env)
    assert env.cli.get("/retiros/comprobante/tok1").status_code == 404


# ── escape de HTML ─────────────────────────────────────────────────────
def test_el_nombre_con_script_se_escapa_en_pagina_y_correo(env):
    env.mp.setenv("RETIROS_FIRMA_CORREO", "1")
    retiro(env, "retirada")
    malo = "<script>alert(1)</script>"
    assert firmar(env, nombre=malo, observaciones='<img src=x onerror=alert(2)>').status_code == 200
    A.esperar_hilos_de_aviso()
    html = env.cli.get("/retiros/comprobante/" + token(env)).get_data(as_text=True)
    assert "<script>alert(1)" not in html and "&lt;script&gt;" in html and "<img src=x" not in html
    correo = env.esp.correo.call_args.args[2]
    assert "<script>alert(1)" not in correo and "&lt;script&gt;" in correo and "<img src=x" not in correo


# ── quién retiró: solo se rellena ──────────────────────────────────────
def test_completa_quien_retiro_si_estaba_vacio(env):
    retiro(env, "retirada")                      # cierre automático por Check: retirado_por_nombre vacío
    firmar(env)
    A.esperar_hilos_de_aviso()
    fila = env.db.solicitudes[RID]
    assert fila["retirado_por_nombre"] == "Gerd Müller" and fila["retirado_por_rut"] == RUT_OK
    notas = [x["notes"] for x in env.db.logs_de(RID, "retiro_evidencia")]
    assert notas == ["Quién retiró: Gerd Müller (desde la firma de recepción)"]


def test_no_pisa_lo_que_una_persona_ya_anoto(env):
    retiro(env, "retirada", retirado_por_nombre="Ana Pérez", retirado_por_rut="9.999.999-9")
    firmar(env)
    A.esperar_hilos_de_aviso()
    fila = env.db.solicitudes[RID]
    assert fila["retirado_por_nombre"] == "Ana Pérez" and fila["retirado_por_rut"] == "9.999.999-9"
    assert not env.db.logs_de(RID, "retiro_evidencia")


# ── el candado de solo lectura sigue igual para todo lo demás ──────────
def test_firmar_no_abre_el_resto_del_solo_lectura(env):
    retiro(env, "retirada")
    firmar(env)
    env.db.reiniciar_registro()
    for metodo, ruta, kw in [("post", f"/retiros/{RID}/field", {"json": {"field": "internal_notes", "value": "x"}}),
                             ("post", f"/retiros/{RID}/docs/agregar", {"json": {"document_type": "FCV", "document_number": "9"}}),
                             ("post", f"/retiros/{RID}/customer", {"json": {"customer_name": "Otro"}})]:
        r = getattr(env.cli, metodo)(ruta, **kw)
        assert r.status_code == 409 and r.get_json()["code"] == "RETIRO_CERRADO", ruta
    assert not [e for e in env.db.escrituras if e[0].lower().startswith(("update", "delete"))]


# ── plantillas y scripts ───────────────────────────────────────────────
def test_las_plantillas_y_el_js_estan_sanos():
    import subprocess
    from jinja2 import Environment, FileSystemLoader
    raiz = os.path.dirname(_TESTS)
    j = Environment(loader=FileSystemLoader(os.path.join(raiz, "templates")))
    for t in ("retiros/comprobante_firma.html", "retiros/_firma_bloque.html", "retiros/_firma_ficha.html", "retiros/internal_detail.html"):
        j.parse(open(os.path.join(raiz, "templates", t), encoding="utf-8").read())
    try:
        subprocess.run(["node", "--check", os.path.join(raiz, "static", "retiros_firma.js")], check=True, capture_output=True)
    except FileNotFoundError:
        pytest.skip("node no está instalado")


# ══════════════════════════════════════════════════════════════════════════════
#  VISTA PREVIA (Daniel 2026-10-09: «de momento, los cambios de la encuesta y la firma los vea yo nada más»)
# ══════════════════════════════════════════════════════════════════════════════
class TestFirmaSoloDanielPorAhora:
    def test_el_equipo_no_puede_registrar_una_firma(self, env):
        env.mp.delenv("RETIROS_VISTA_PREVIA_USUARIOS", raising=False)          # el valor por defecto: solo Daniel
        env.db.nueva_solicitud(1, status="retirada", responsable_user_id=7, responsable_nombre="Sam")
        r = env.cli.post("/retiros/1/firma", json={"nombre": "Gerd Müller", "rut": "11.111.111-1", "relacion": "titular",
                                                   "conformidad": True, "firma": "data:image/png;base64,AAAA"})
        assert r.status_code == 403 and env.db.firmas == {}
        assert env.esp.correo.call_args_list == []

    def test_daniel_si_puede(self, monkeypatch):
        monkeypatch.delenv("RETIROS_VISTA_PREVIA_USUARIOS", raising=False)
        app, db, ctx, esp = A.construir_app(usuario={"id": 1, "nombre": "Daniel Aguilar", "username": "daniel.aguilar@sphs.cl"})
        db.nueva_solicitud(1, status="retirada", responsable_user_id=1, responsable_nombre="Daniel")
        r = app.test_client().post("/retiros/1/firma", json={"nombre": "Gerd", "rut": "11.111.111-1", "relacion": "titular",
                                                             "conformidad": True, "firma": "data:image/png;base64,AAAA"})
        assert r.status_code != 403

    def test_la_ficha_solo_muestra_la_firma_y_la_encuesta_en_vista_previa(self):
        import re as _re
        html = open(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "templates", "retiros", "internal_detail.html"),
                    encoding="utf-8").read()
        for parcial in ("_firma_ficha.html", "_firma_bloque.html", "_encuesta_ficha.html"):
            for m in _re.finditer(_re.escape(parcial), html):
                bloque = html[html.rfind("{% if", 0, m.start()): m.start()]      # el {% if %} que la envuelve (en la línea o el del modal)
                assert "retiros_vista_previa()" in bloque, bloque[:200]
        i = html.index('id="modalFirma"')
        assert "retiros_vista_previa()" in html[html.rfind("{% if", 0, i): i]
        j = html.index("retiros_firma.js")
        assert "retiros_vista_previa()" in html[html.rfind("{% if", 0, j): j]

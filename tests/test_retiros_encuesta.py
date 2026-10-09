# -*- coding: utf-8 -*-
"""Pruebas del MÓDULO DE ENCUESTA DE SATISFACCIÓN de Retiros (retiros_encuesta.py): creado, NO lanzado.

Sin MySQL real, sin correo, sin WhatsApp, sin Check: la base es un SQLite en memoria con una capa que traduce el
poco MySQL que usa el módulo (así las consultas se EJECUTAN de verdad: JOIN, GROUP BY, UNIQUE, INSERT IGNORE) y los
espías prueban «esto NO se llamó». Nada toca retiros reales.
Correr SOLO este archivo:  py -m pytest tests/test_retiros_encuesta.py -q
"""
import json
import os
import re
import sqlite3
import sys
from datetime import datetime, timedelta
from functools import wraps
from unittest.mock import MagicMock

import pytest
from flask import Flask, g, render_template_string
from jinja2 import ChoiceLoader, DictLoader, FileSystemLoader

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if RAIZ not in sys.path:
    sys.path.insert(0, RAIZ)

import retiros_encuesta as enc  # noqa: E402

T0 = datetime(2026, 10, 6, 15, 0, 0)       # «ahora» fijo de las pruebas (UTC sin zona)


# ─────────────────────────────────────────────────────────────────────
#  MySQL de mentira sobre SQLite
# ─────────────────────────────────────────────────────────────────────
def _a_sqlite(sql):
    if re.match(r"\s*CREATE TABLE", sql, re.I):
        lineas = []
        for l in sql.splitlines():
            l = l.strip()
            if not l or re.match(r"KEY ", l):
                continue
            l = re.sub(r"COMMENT '[^']*'", "", l)
            l = l.replace("INT AUTO_INCREMENT PRIMARY KEY", "INTEGER PRIMARY KEY AUTOINCREMENT")
            l = re.sub(r"UNIQUE KEY \w+ \(", "UNIQUE (", l)
            lineas.append(l)
        s = " ".join(lineas)
        s = re.sub(r",\s*\)\s*ENGINE.*$", " )", s)
        s = re.sub(r"\)\s*ENGINE.*$", ")", s)
        return s
    s = re.sub(r"\s+", " ", sql).strip()
    s = s.replace("INSERT IGNORE INTO", "INSERT OR IGNORE INTO")
    return s.replace("%s", "?")


def _param(v):
    if isinstance(v, datetime):
        return v.isoformat(sep=" ")
    return v


class BDFalsa:
    def __init__(self):
        self.con = sqlite3.connect(":memory:", check_same_thread=False)
        self.con.row_factory = sqlite3.Row
        self.con.execute("CREATE TABLE pickup_requests (id INTEGER PRIMARY KEY, code TEXT, status TEXT, "
                         "public_token TEXT, closed_at TEXT)")
        self.ddl = []
        self.escrituras = []

    def retiro(self, rid=1, status="retirada", public_token=None, closed_at=None):
        self.con.execute("INSERT OR REPLACE INTO pickup_requests VALUES (?,?,?,?,?)",
                         (rid, f"RET-T{rid:05d}", status, public_token or f"pub-token-largo-{rid}",
                          closed_at.isoformat(sep=" ") if closed_at else None))

    def fetchall(self, sql, params=()):
        cur = self.con.execute(_a_sqlite(sql), tuple(_param(p) for p in params))
        return [dict(r) for r in cur.fetchall()]

    def fetchone(self, sql, params=()):
        f = self.fetchall(sql, params)
        return f[0] if f else None

    def execute(self, sql, params=()):
        if re.match(r"\s*CREATE TABLE", sql, re.I):
            self.ddl.append(re.sub(r"\s+", " ", sql).strip())
        else:
            self.escrituras.append((re.sub(r"\s+", " ", sql).strip(), params))
        cur = self.con.execute(_a_sqlite(sql), tuple(_param(p) for p in params))
        self.con.commit()
        return cur.rowcount

    def contar(self, tabla, donde="1=1"):
        return self.con.execute(f"SELECT COUNT(*) FROM {tabla} WHERE {donde}").fetchone()[0]


class Espias:
    """Todo lo que podría escribirle a alguien o llamar a un sistema externo."""

    def __init__(self):
        self.correo = MagicMock(name="_send_ilus_email")
        self.whatsapp = MagicMock(name="_send_whatsapp")
        self.mant_notif = MagicMock(name="_mant_notificar")
        self.check = MagicMock(name="_checkwms_get")

    def hubo_envios(self):
        return any(m.called for m in (self.correo, self.whatsapp, self.mant_notif, self.check))


STUB_BASE = ("<!doctype html><html><head><title>{% block title %}{% endblock %}</title>"
             "{% block head_extra %}{% endblock %}</head><body><div class='page'>"
             "{% block content %}{% endblock %}</div></body></html>")


def _require_permission(perm):
    def deco(vista):
        @wraps(vista)
        def envuelta(*a, **kw):
            if not g.user:
                return ("login", 302)
            if not (g.permissions.get("superadmin") or g.permissions.get(perm)):
                return ("sin permiso", 403)
            return vista(*a, **kw)
        return envuelta
    return deco


def construir(monkeypatch, con_rowcount=True, vista_previa="*"):
    monkeypatch.delenv("RETIROS_ENCUESTA_ACTIVA", raising=False)
    # Vista previa (2026-10-09: por ahora solo Daniel ve la encuesta): estas pruebas son del funcionamiento, así que todo el equipo la ve;
    # el candado «solo Daniel» tiene sus propias pruebas al final del archivo.
    if vista_previa is None:
        monkeypatch.delenv("RETIROS_VISTA_PREVIA_USUARIOS", raising=False)
    else:
        monkeypatch.setenv("RETIROS_VISTA_PREVIA_USUARIOS", vista_previa)
    monkeypatch.setattr(enc, "_ahora", lambda: T0)
    db, esp = BDFalsa(), Espias()
    sesion = {"user": None, "permissions": {}}
    app = Flask(__name__, template_folder=os.path.join(RAIZ, "templates"))
    app.config["TESTING"] = True
    app.secret_key = "clave-de-prueba"
    app.jinja_loader = ChoiceLoader([DictLoader({"base.html": STUB_BASE}),
                                     FileSystemLoader(os.path.join(RAIZ, "templates"))])

    @app.before_request
    def _usuario():
        g.user = dict(sesion["user"]) if sesion["user"] else None
        g.permissions = dict(sesion["permissions"])

    ctx = {"mysql_fetchone": db.fetchone, "mysql_fetchall": db.fetchall,
           "mysql_execute": (lambda sql, p=(): (db.execute(sql, p), None)[1]),   # como en producción: no devuelve rowcount
           "require_permission": _require_permission, "g": g, "PICKUP_REQUESTS_TABLE": "pickup_requests",
           "_send_ilus_email": esp.correo, "_send_whatsapp": esp.whatsapp,
           "_mant_notificar": esp.mant_notif, "_checkwms_get": esp.check}
    if con_rowcount:
        ctx["mysql_execute_returning_rowcount"] = lambda sql, p=(): int(db.execute(sql, p) or 0)
    enc.register_encuesta_routes(app, ctx)

    class E:
        pass
    e = E()
    e.app, e.db, e.esp, e.sesion, e.client, e.monkeypatch = app, db, esp, sesion, app.test_client(), monkeypatch
    e.token = lambda rid=1: app.extensions["retiros_encuesta"]["token_para"](
        rid, db.fetchone("SELECT public_token FROM pickup_requests WHERE id=%s", (rid,))["public_token"])
    e.personal = lambda: sesion.update(user={"id": 7, "nombre": "Sam"}, permissions={"retiros": True})
    e.admin = lambda: sesion.update(user={"id": 1, "nombre": "Daniel"}, permissions={"retiros": True, "admin": True})
    e.anonimo = lambda: sesion.update(user=None, permissions={})
    e.activar = lambda: monkeypatch.setenv("RETIROS_ENCUESTA_ACTIVA", "1")
    e.preguntas = lambda: db.fetchall("SELECT * FROM pickup_encuesta_preguntas WHERE vigente=1 ORDER BY orden, id")
    db.retiro(1)
    return e


@pytest.fixture
def entorno(monkeypatch):
    return construir(monkeypatch)


def form_ok(e, **cambios):
    """Respuesta válida para las preguntas activas actuales (más el consentimiento)."""
    datos = {}
    for p in e.db.fetchall("SELECT * FROM pickup_encuesta_preguntas WHERE vigente=1 AND activa=1 ORDER BY orden, id"):
        c = f"p_{p['id']}"
        if p["tipo"] == "estrellas":
            datos[c] = "5"
        elif p["tipo"] == "si_no":
            datos[c] = "si"
        elif p["tipo"] == "opcion":
            datos[c] = json.loads(p["opciones"])[0]["valor"]
        else:
            datos[c] = "Todo muy bien, gracias"
    datos["consentimiento"] = "1"
    datos.update(cambios)
    return datos


def pid(e, clave):
    return e.db.fetchone("SELECT id FROM pickup_encuesta_preguntas WHERE clave=%s AND vigente=1", (clave,))["id"]


def sembrar(e):
    """Fuerza el primer uso (crea tablas + semilla) sin lanzar nada."""
    e.admin()
    e.client.get("/retiros/encuesta")
    e.anonimo()


# ═════════════════════════════════════════════════════════════════════
#  1. SIN LANZAR: el cliente no accede; el personal sí (vista previa)
# ═════════════════════════════════════════════════════════════════════
def test_interruptor_apagado_por_defecto(entorno):
    assert enc.encuesta_activa() is False
    for v in ("1", "true", "SI", "on", "yes"):
        entorno.monkeypatch.setenv("RETIROS_ENCUESTA_ACTIVA", v)
        assert enc.encuesta_activa() is True
    for v in ("0", "", "no", "false", "off"):
        entorno.monkeypatch.setenv("RETIROS_ENCUESTA_ACTIVA", v)
        assert enc.encuesta_activa() is False


def test_sin_lanzar_el_cliente_no_accede_y_no_se_toca_la_bd(entorno):
    t = entorno.token()
    for r in (entorno.client.get(f"/retiros/encuesta/{t}"),
              entorno.client.post(f"/retiros/encuesta/{t}", data={"consentimiento": "1"})):
        assert r.status_code == 404
        assert "aún no está disponible" in r.get_data(as_text=True)
        assert "<form" not in r.get_data(as_text=True)
    assert entorno.db.ddl == [] and entorno.db.escrituras == []


def test_token_invalido_404_igual(entorno):
    entorno.activar()
    for t in ("basura", "1-" + "0" * 24, "zz-" + "a" * 24, "1-xyz"):
        assert entorno.client.get(f"/retiros/encuesta/{t}").status_code == 404


def test_sin_lanzar_el_personal_ve_vista_previa_del_token(entorno):
    entorno.personal()
    r = entorno.client.get(f"/retiros/encuesta/{entorno.token()}")
    html = r.get_data(as_text=True)
    assert r.status_code == 200 and "VISTA PREVIA DEL PERSONAL" in html and 'name="consentimiento"' in html


def test_vista_previa_dedicada_muestra_las_preguntas_semilla(entorno):
    entorno.personal()
    r = entorno.client.get("/retiros/encuesta/vista-previa")
    html = r.get_data(as_text=True)
    assert r.status_code == 200 and "VISTA PREVIA DEL PERSONAL" in html
    assert "¿Tu pedido estaba listo cuando llegaste?" in html
    assert "Tuve que esperar menos de 10 minutos" in html and "Esperé más de 10 minutos" in html
    assert "desde que pediste tu retiro" in html and "atención del equipo en bodega" in html
    assert "(opcional)" in html and 'data-total="3"' in html


def test_permisos_vista_previa_resultados_y_editor(entorno):
    assert entorno.client.get("/retiros/encuesta/vista-previa").status_code == 302          # sin sesión
    entorno.sesion.update(user={"id": 9}, permissions={})                                    # sesión sin permiso
    for url in ("/retiros/encuesta/vista-previa", "/retiros/encuesta", "/retiros/encuesta/preguntas",
                "/retiros/encuesta/ficha/1"):
        assert entorno.client.get(url).status_code == 403, url
    entorno.personal()                                                                       # retiros, sin gestión
    assert entorno.client.get("/retiros/encuesta").status_code == 200
    assert entorno.client.get("/retiros/encuesta/preguntas").status_code == 403
    assert entorno.client.get("/retiros/encuesta/preguntas/datos").status_code == 403
    r = entorno.client.post("/retiros/encuesta/preguntas/guardar", json={"texto": "x" * 20, "tipo": "estrellas"})
    assert r.status_code == 403
    entorno.sesion.update(user={"id": 3}, permissions={"retiros": True, "ret_horarios": True})   # jefatura de Retiros
    assert entorno.client.get("/retiros/encuesta/preguntas").status_code == 200
    entorno.admin()
    assert entorno.client.get("/retiros/encuesta/preguntas").status_code == 200


def test_vista_previa_no_guarda_nada(entorno):
    entorno.personal()
    entorno.client.get("/retiros/encuesta/vista-previa")
    r = entorno.client.post("/retiros/encuesta/vista-previa", data=form_ok(entorno))
    assert r.status_code == 200 and "Gracias por tu opinión" in r.get_data(as_text=True)
    assert "VISTA PREVIA" in r.get_data(as_text=True)
    assert entorno.db.contar("pickup_encuesta_respuestas") == 0
    assert entorno.db.contar("pickup_encuesta_invitaciones") == 0


def test_vista_previa_valida_igual(entorno):
    entorno.personal()
    entorno.client.get("/retiros/encuesta/vista-previa")
    r = entorno.client.post("/retiros/encuesta/vista-previa", data=form_ok(entorno, consentimiento=""))
    assert r.status_code == 422 and "aceptes el aviso de privacidad" in r.get_data(as_text=True)


def test_personal_con_token_real_activa_tampoco_guarda(entorno):
    """Si el personal abre el enlace de un cliente y lo envía, NO se guarda: taparía la respuesta real del cliente."""
    entorno.activar()
    entorno.personal()
    entorno.client.get(f"/retiros/encuesta/{entorno.token()}")
    r = entorno.client.post(f"/retiros/encuesta/{entorno.token()}", data=form_ok(entorno))
    assert r.status_code == 200 and "VISTA PREVIA" in r.get_data(as_text=True)
    assert entorno.db.contar("pickup_encuesta_respuestas") == 0 and entorno.db.contar("pickup_encuesta_invitaciones") == 0


# ═════════════════════════════════════════════════════════════════════
#  2. LANZADA: formulario, privacidad, consentimiento, guardado idempotente
# ═════════════════════════════════════════════════════════════════════
def test_lanzada_formulario_con_aviso_de_privacidad_y_consentimiento_sin_premarcar(entorno):
    sembrar(entorno)
    entorno.activar()
    r = entorno.client.get(f"/retiros/encuesta/{entorno.token()}")
    html = r.get_data(as_text=True)
    assert r.status_code == 200 and "VISTA PREVIA" not in html
    # aviso: responsable, RUT, finalidad, conservación, derechos, contacto y versión
    for texto in ("Sport and Health Solutions SPA", "76.996.964-0", "mejorar el servicio de retiros", "24 meses",
                  "acceso, rectificación, supresión, oposición y portabilidad", "soportetec@sphs.cl",
                  f"versión {enc.AVISO_VERSION}", "no guardamos tu dirección IP"):
        assert texto in html, texto
    m = re.search(r'<input[^>]*name="consentimiento"[^>]*>', html)
    assert m and "checked" not in m.group(0)
    assert "Responder es voluntario" in html and "puedes retirar tu consentimiento" in html
    assert "no incluyas datos personales" in html
    assert "ILUS Fitness" in html and "Sport & Health" not in html and "Sport &amp; Health" not in html


def test_formulario_no_pide_datos_personales(entorno):
    sembrar(entorno)
    entorno.personal()
    html = entorno.client.get("/retiros/encuesta/vista-previa").get_data(as_text=True)
    nombres = set(re.findall(r'<(?:input|textarea|select)[^>]*name="([A-Za-z_0-9]+)"', html))
    assert nombres and all(n in ("csrf_token", "consentimiento") or re.fullmatch(r"p_\d+", n) for n in nombres), nombres


def test_lanzada_retiro_no_retirado_no_recibe_encuesta(entorno):
    entorno.db.retiro(2, status="en_preparacion")
    entorno.activar()
    assert entorno.client.get(f"/retiros/encuesta/{entorno.token(2)}").status_code == 404


def test_lanzada_token_de_otro_retiro_o_adulterado_404(entorno):
    entorno.db.retiro(2)
    entorno.activar()
    t1, t2 = entorno.token(1), entorno.token(2)
    falso = t2.split("-")[0] + "-" + t1.split("-")[1]
    assert entorno.client.get(f"/retiros/encuesta/{falso}").status_code == 404
    assert entorno.client.get(f"/retiros/encuesta/{t1.upper()}").status_code == 404


def test_una_invitacion_por_retiro_y_vence_a_los_30_dias(entorno):
    entorno.activar()
    t = entorno.token()
    entorno.client.get(f"/retiros/encuesta/{t}")
    entorno.client.get(f"/retiros/encuesta/{t}")
    inv = entorno.db.fetchall("SELECT * FROM pickup_encuesta_invitaciones")
    assert len(inv) == 1
    assert inv[0]["request_id"] == 1 and inv[0]["token_hash"] == enc._hash_token(t)
    assert enc._a_dt(inv[0]["vence_en"]) - enc._a_dt(inv[0]["creada_en"]) == timedelta(days=enc.DIAS_VIGENCIA_ENLACE)
    assert inv[0]["enviada_en"] is None and inv[0]["respondida_en"] is None


def test_invitacion_vencida_404(entorno):
    entorno.activar()
    t = entorno.token()
    entorno.client.get(f"/retiros/encuesta/{t}")
    entorno.db.execute("UPDATE pickup_encuesta_invitaciones SET vence_en=%s", (T0 - timedelta(days=1),))
    r = entorno.client.get(f"/retiros/encuesta/{t}")
    assert r.status_code == 404 and "aún no está disponible" in r.get_data(as_text=True)
    assert entorno.client.post(f"/retiros/encuesta/{t}", data=form_ok(entorno)).status_code == 404


@pytest.mark.parametrize("con_rowcount", [True, False])
def test_guardar_respuesta_registra_consentimiento_y_es_idempotente(monkeypatch, con_rowcount):
    e = construir(monkeypatch, con_rowcount=con_rowcount)
    e.activar()
    t = e.token()
    url = f"/retiros/encuesta/{t}"
    e.client.get(url)
    r = e.client.post(url, data=form_ok(e))
    assert r.status_code == 303 and r.headers["Location"].endswith(url)
    inv = e.db.fetchone("SELECT * FROM pickup_encuesta_invitaciones")
    assert inv["respondida_en"] and inv["consentimiento_en"] and inv["version_aviso"] == enc.AVISO_VERSION
    assert inv["token_hash"] == enc._hash_token(t) and t not in str(inv)
    filas = e.db.fetchall("SELECT * FROM pickup_encuesta_respuestas ORDER BY pregunta_id")
    assert len(filas) == 4                                           # 3 operacionales + comentario
    por_clave = {e.db.fetchone("SELECT clave FROM pickup_encuesta_preguntas WHERE id=%s", (f["pregunta_id"],))["clave"]: f
                 for f in filas}
    assert por_clave["pedido_listo"]["valor_texto"] == "listo" and por_clave["tiempo_total"]["valor_num"] == 5
    assert por_clave["comentario"]["valor_texto"] == "Todo muy bien, gracias"
    # un segundo envío (aunque distinto) no pisa ni duplica
    e.client.post(url, data=form_ok(e, **{f"p_{pid(e, 'tiempo_total')}": "1"}))
    assert e.db.contar("pickup_encuesta_respuestas") == 4
    assert e.db.fetchone("SELECT valor_num FROM pickup_encuesta_respuestas WHERE pregunta_id=%s",
                         (pid(e, "tiempo_total"),))["valor_num"] == 5
    g_ = e.client.get(url)
    assert g_.status_code == 200 and "Ya recibimos tu opinión" in g_.get_data(as_text=True)
    assert "<form" not in g_.get_data(as_text=True)


def test_no_se_guarda_la_ip_ni_datos_personales(entorno):
    entorno.activar()
    entorno.client.get(f"/retiros/encuesta/{entorno.token()}", environ_overrides={"REMOTE_ADDR": "200.1.2.3"})
    entorno.client.post(f"/retiros/encuesta/{entorno.token()}", data=form_ok(entorno),
                        environ_overrides={"REMOTE_ADDR": "200.1.2.3"})
    prohibidas = {"ip", "ip_hash", "ip_cliente", "client_ip", "user_agent", "rut", "correo", "email", "telefono", "nombre"}
    for tabla in ("pickup_encuesta_invitaciones", "pickup_encuesta_respuestas", "pickup_encuesta_preguntas"):
        cols = {c[1].lower() for c in entorno.db.con.execute(f"PRAGMA table_info({tabla})")}
        assert not (cols & prohibidas), cols
    volcado = json.dumps([list(map(str, r)) for t in ("pickup_encuesta_invitaciones", "pickup_encuesta_respuestas")
                          for r in entorno.db.con.execute(f"SELECT * FROM {t}")])
    assert "200.1.2.3" not in volcado


def test_el_consentimiento_es_obligatorio(entorno):
    entorno.activar()
    t = entorno.token()
    entorno.client.get(f"/retiros/encuesta/{t}")
    r = entorno.client.post(f"/retiros/encuesta/{t}", data=form_ok(entorno, consentimiento=""))
    assert r.status_code == 422 and "aceptes el aviso de privacidad" in r.get_data(as_text=True)
    assert entorno.db.contar("pickup_encuesta_respuestas") == 0
    assert entorno.db.fetchone("SELECT respondida_en FROM pickup_encuesta_invitaciones")["respondida_en"] is None


@pytest.mark.parametrize("cambio", ["estrellas_0", "estrellas_6", "estrellas_x", "estrellas_vacia", "opcion_mala",
                                    "opcion_vacia", "texto_vacio_ok"])
def test_validacion_de_rangos(entorno, cambio):
    sembrar(entorno)
    entorno.activar()
    t = entorno.token()
    entorno.client.get(f"/retiros/encuesta/{t}")
    est, opc, txt = f"p_{pid(entorno, 'tiempo_total')}", f"p_{pid(entorno, 'pedido_listo')}", f"p_{pid(entorno, 'comentario')}"
    cambios = {"estrellas_0": {est: "0"}, "estrellas_6": {est: "6"}, "estrellas_x": {est: "x"},
               "estrellas_vacia": {est: ""}, "opcion_mala": {opc: "inventada"}, "opcion_vacia": {opc: ""},
               "texto_vacio_ok": {txt: ""}}[cambio]
    r = entorno.client.post(f"/retiros/encuesta/{t}", data=form_ok(entorno, **cambios))
    if cambio == "texto_vacio_ok":            # el comentario es opcional
        assert r.status_code == 303
        assert entorno.db.contar("pickup_encuesta_respuestas") == 3
    else:
        assert r.status_code == 422 and entorno.db.contar("pickup_encuesta_respuestas") == 0


def test_validar_respuestas_tipos_y_limites():
    ps = [{"id": 1, "tipo": "estrellas", "obligatoria": True, "opciones": []},
          {"id": 2, "tipo": "si_no", "obligatoria": True, "opciones": []},
          {"id": 3, "tipo": "opcion", "obligatoria": True, "opciones": [{"valor": "a"}, {"valor": "b"}]},
          {"id": 4, "tipo": "texto", "obligatoria": False, "opciones": []}]
    r, e = enc.validar_respuestas(ps, {"p_1": "3", "p_2": "no", "p_3": "b", "p_4": "x" * 5000, "consentimiento": "1"})
    assert not e
    d = {x["pregunta_id"]: x for x in r}
    assert d[1]["valor_num"] == 3 and d[2]["valor_num"] == 0 and d[3]["valor_texto"] == "b"
    assert len(d[4]["valor_texto"]) == enc.MAX_TEXTO
    r2, e2 = enc.validar_respuestas(ps, {"p_1": "9", "p_2": "tal vez", "p_3": "z", "consentimiento": "on"})
    assert set(e2) == {"p_1", "p_2", "p_3"}
    _, e3 = enc.validar_respuestas(ps, {"p_1": "3", "p_2": "si", "p_3": "a"})
    assert set(e3) == {"consentimiento"}
    r4, _ = enc.validar_respuestas(ps, {"p_1": "3", "p_2": "si", "p_3": "a", "p_4": "hola\x00\x07 mundo", "consentimiento": "1"})
    assert [x for x in r4 if x["pregunta_id"] == 4][0]["valor_texto"] == "hola mundo"


def test_comentario_con_html_se_escapa(entorno):
    sembrar(entorno)
    entorno.activar()
    t = entorno.token()
    entorno.client.get(f"/retiros/encuesta/{t}")
    entorno.client.post(f"/retiros/encuesta/{t}", data=form_ok(entorno, **{f"p_{pid(entorno, 'comentario')}": "<script>alert(1)</script>"}))
    entorno.admin()
    html = entorno.client.get("/retiros/encuesta").get_data(as_text=True)
    assert "<script>alert(1)</script>" not in html and "&lt;script&gt;" in html


# ═════════════════════════════════════════════════════════════════════
#  3. BASE DE DATOS: esquema perezoso, semilla, versionado
# ═════════════════════════════════════════════════════════════════════
def test_esquema_perezoso_tres_tablas_y_semilla_una_sola_vez(entorno):
    assert entorno.db.ddl == []                                 # registrar el módulo NO crea nada
    entorno.admin()
    entorno.client.get("/retiros/encuesta")
    entorno.client.get("/retiros/encuesta")
    assert len(entorno.db.ddl) == 3
    assert all(re.match(r"CREATE TABLE IF NOT EXISTS pickup_encuesta_(preguntas|invitaciones|respuestas) \(", d) for d in entorno.db.ddl)
    assert "UNIQUE KEY uq_encinv_request (request_id)" in entorno.db.ddl[1]
    assert "UNIQUE KEY uq_encresp (invitacion_id, pregunta_id)" in entorno.db.ddl[2]
    ps = entorno.preguntas()
    assert [p["clave"] for p in ps] == ["pedido_listo", "tiempo_total", "atencion_bodega", "comentario"]
    assert [p["obligatoria"] for p in ps] == [1, 1, 1, 0]
    assert sum(1 for p in ps if p["tipo"] != "texto") == 3      # máximo 3 operacionales + 1 comentario opcional
    # un segundo «primer uso» (otro worker) no duplica la semilla
    entorno.db.execute("INSERT OR IGNORE INTO pickup_encuesta_preguntas (clave, version, texto, tipo, creado_en) "
                       "VALUES ('pedido_listo', 1, 'dup', 'texto', '2026-10-06 00:00:00')")
    assert entorno.db.contar("pickup_encuesta_preguntas") == 4


def test_crear_pregunta_y_validaciones_del_editor(entorno):
    entorno.admin()
    entorno.client.get("/retiros/encuesta/preguntas/datos")
    ok = entorno.client.post("/retiros/encuesta/preguntas/guardar", json={
        "texto": "¿Cómo conociste ILUS Fitness?", "tipo": "opcion", "obligatoria": False,
        "opciones": [{"etiqueta": "Instagram", "color": "verde"}, {"etiqueta": "Un amigo", "color": "verde"},
                     {"etiqueta": "Otro", "color": "no-existe"}]})
    assert ok.status_code == 200 and ok.get_json()["modo"] == "creada"
    p = entorno.db.fetchone("SELECT * FROM pickup_encuesta_preguntas WHERE texto LIKE %s", ("%conociste%",))
    ops = json.loads(p["opciones"])
    assert [o["valor"] for o in ops] == ["o1", "o2", "o3"] and ops[2]["color"] == "gris"
    assert p["orden"] == 50 and p["obligatoria"] == 0 and p["vigente"] == 1 and p["creado_por"] == "Daniel"
    for malo in ({"texto": "corta", "tipo": "estrellas"}, {"texto": "Pregunta suficientemente larga", "tipo": "raro"},
                 {"texto": "Pregunta suficientemente larga", "tipo": "opcion", "opciones": [{"etiqueta": "Una sola"}]},
                 {"texto": "Pregunta suficientemente larga", "tipo": "opcion", "opciones": [{"etiqueta": str(i)} for i in range(7)]}):
        r = entorno.client.post("/retiros/encuesta/preguntas/guardar", json=malo)
        assert r.status_code == 422 and r.get_json()["errores"]
    assert entorno.db.contar("pickup_encuesta_preguntas") == 5


def test_editar_sin_respuestas_corrige_en_el_mismo_lugar(entorno):
    entorno.admin()
    entorno.client.get("/retiros/encuesta/preguntas/datos")
    i = pid(entorno, "atencion_bodega")
    r = entorno.client.post("/retiros/encuesta/preguntas/guardar",
                            json={"id": i, "texto": "¿Qué tal fue la atención en bodega?", "ayuda": "", "tipo": "estrellas", "obligatoria": True})
    assert r.get_json()["modo"] == "editada"
    f = entorno.db.fetchone("SELECT * FROM pickup_encuesta_preguntas WHERE id=%s", (i,))
    assert f["texto"] == "¿Qué tal fue la atención en bodega?" and f["version"] == 1 and entorno.db.contar("pickup_encuesta_preguntas") == 4
    # sin cambios reales
    same = entorno.client.post("/retiros/encuesta/preguntas/guardar",
                               json={"id": i, "texto": "¿Qué tal fue la atención en bodega?", "ayuda": "", "tipo": "estrellas", "obligatoria": True})
    assert same.get_json()["modo"] == "sin_cambios"
    f2 = entorno.db.fetchone("SELECT ayuda FROM pickup_encuesta_preguntas WHERE id=%s", (i,))
    assert f2["ayuda"] in (None, "")


def test_editar_con_respuestas_crea_version_nueva_y_las_viejas_conservan_su_texto(entorno):
    sembrar(entorno)
    entorno.activar()
    t = entorno.token()
    entorno.client.get(f"/retiros/encuesta/{t}")
    entorno.client.post(f"/retiros/encuesta/{t}", data=form_ok(entorno))          # respuesta a la versión 1
    viejo = pid(entorno, "tiempo_total")
    entorno.admin()
    r = entorno.client.post("/retiros/encuesta/preguntas/guardar", json={
        "id": viejo, "texto": "¿Qué tan conforme quedaste con el tiempo total del retiro?", "ayuda": "", "tipo": "estrellas", "obligatoria": True})
    assert r.get_json() == {"ok": True, "modo": "nueva_version", "version": 2}
    filas = entorno.db.fetchall("SELECT * FROM pickup_encuesta_preguntas WHERE clave='tiempo_total' ORDER BY version")
    assert [(f["version"], f["vigente"]) for f in filas] == [(1, 0), (2, 1)]
    assert "desde que pediste tu retiro" in filas[0]["texto"] and "tan conforme" in filas[1]["texto"]
    assert (filas[1]["orden"], filas[1]["activa"]) == (filas[0]["orden"], filas[0]["activa"])
    # la respuesta vieja sigue apuntando a la versión 1 (el texto que vio el cliente)
    resp = entorno.db.fetchone("SELECT pregunta_id FROM pickup_encuesta_respuestas WHERE pregunta_id=%s", (viejo,))
    assert resp is not None
    # el cliente nuevo ve SOLO el texto nuevo
    entorno.db.retiro(2)
    entorno.anonimo()
    html = entorno.client.get(f"/retiros/encuesta/{entorno.token(2)}").get_data(as_text=True)
    assert "tan conforme quedaste" in html and "desde que pediste tu retiro" not in html
    entorno.client.post(f"/retiros/encuesta/{entorno.token(2)}", data=form_ok(entorno, **{f"p_{filas[1]['id']}": "3"}))
    # los resultados agrupan por clave a través de las versiones: 2 respuestas (5 y 3) → promedio 4,0
    entorno.admin()
    res = entorno.client.get("/retiros/encuesta").get_data(as_text=True)
    assert "tan conforme quedaste" in res and "4,0" in res


def test_archivar_y_activar_no_borra_nada(entorno):
    entorno.admin()
    entorno.client.get("/retiros/encuesta/preguntas/datos")
    i = pid(entorno, "atencion_bodega")
    assert entorno.client.post(f"/retiros/encuesta/preguntas/{i}/activa", json={"activa": False}).get_json()["activa"] is False
    assert entorno.db.contar("pickup_encuesta_preguntas") == 4
    html = entorno.client.get("/retiros/encuesta/vista-previa").get_data(as_text=True)
    assert "atención del equipo en bodega" not in html and 'data-total="2"' in html
    assert entorno.client.post(f"/retiros/encuesta/preguntas/{i}/activa", json={"activa": True}).get_json()["activa"] is True
    assert "atención del equipo en bodega" in entorno.client.get("/retiros/encuesta/vista-previa").get_data(as_text=True)
    assert entorno.client.post("/retiros/encuesta/preguntas/9999/activa", json={"activa": True}).status_code == 404


def test_mover_cambia_el_orden(entorno):
    entorno.admin()
    entorno.client.get("/retiros/encuesta/preguntas/datos")
    i = pid(entorno, "atencion_bodega")
    assert entorno.client.post(f"/retiros/encuesta/preguntas/{i}/mover", json={"dir": "arriba"}).get_json()["ok"]
    assert [p["clave"] for p in entorno.preguntas()] == ["pedido_listo", "atencion_bodega", "tiempo_total", "comentario"]
    entorno.client.post(f"/retiros/encuesta/preguntas/{i}/mover", json={"dir": "arriba"})
    entorno.client.post(f"/retiros/encuesta/preguntas/{i}/mover", json={"dir": "arriba"})       # ya es la primera: no pasa nada
    assert entorno.preguntas()[0]["clave"] == "atencion_bodega"
    assert entorno.client.post(f"/retiros/encuesta/preguntas/{i}/mover", json={"dir": "izquierda"}).status_code == 400
    d = entorno.client.get("/retiros/encuesta/preguntas/datos").get_json()
    assert [p["clave"] for p in d["preguntas"]][0] == "atencion_bodega" and d["activas"] == 4


def test_la_pagina_del_editor_trae_su_configuracion(entorno):
    entorno.admin()
    html = entorno.client.get("/retiros/encuesta/preguntas").get_data(as_text=True)
    assert "Nueva pregunta" in html and 'id="encPrev"' in html and "/retiros/encuesta/vista-previa" in html
    assert "aún NO lanzada" in html


# ═════════════════════════════════════════════════════════════════════
#  4. RESULTADOS INTERNOS
# ═════════════════════════════════════════════════════════════════════
def test_resultados_aviso_grande_no_lanzada_y_botones(entorno):
    entorno.admin()
    r = entorno.client.get("/retiros/encuesta")
    html = r.get_data(as_text=True)
    assert r.status_code == 200
    assert "Encuesta creada, aún NO lanzada: ningún cliente la ha recibido" in html
    assert "Vista previa de la encuesta" in html and "Editar preguntas" in html and "Todavía no hay respuestas" in html
    entorno.personal()                                                  # sin gestión: no ve «Editar preguntas»
    assert "Editar preguntas" not in entorno.client.get("/retiros/encuesta").get_data(as_text=True)


def _responder(e, n, estrellas="5", opcion=None, comentario=None, desde=100):
    for k in range(n):
        rid = desde + k
        e.db.retiro(rid)
        t = e.token(rid)
        e.anonimo()
        e.client.get(f"/retiros/encuesta/{t}")
        cambios = {f"p_{pid(e, 'tiempo_total')}": estrellas, f"p_{pid(e, 'atencion_bodega')}": estrellas}
        if opcion:
            cambios[f"p_{pid(e, 'pedido_listo')}"] = opcion
        cambios[f"p_{pid(e, 'comentario')}"] = comentario(rid) if callable(comentario) else (comentario or "")
        e.client.post(f"/retiros/encuesta/{t}", data=form_ok(e, **cambios))


def test_resultados_calculan_promedios_y_porcentajes(entorno):
    sembrar(entorno)
    entorno.activar()
    _responder(entorno, 3, estrellas="5", opcion="listo", desde=100)
    _responder(entorno, 1, estrellas="2", opcion="espera_larga", desde=200)
    entorno.admin()
    html = entorno.client.get("/retiros/encuesta").get_data(as_text=True)
    assert "Encuesta ACTIVA" in html and "Encuesta creada, aún NO lanzada" not in html
    assert "<div class=\"encr-kpi-n\">4</div>" in html                  # 4 respuestas
    assert "4,2" in html                                                # (5+5+5+2)/4 = 4,25 → 4,3? ver abajo
    assert "75,0%" in html                                              # 3 de 4 eligieron «listo»
    assert "orientativas" in html


def test_resultados_pocos_datos_avisa_y_no_cuenta_invitaciones_sin_responder(entorno):
    sembrar(entorno)
    entorno.activar()
    entorno.client.get(f"/retiros/encuesta/{entorno.token()}")           # abre pero no responde
    entorno.admin()
    html = entorno.client.get("/retiros/encuesta").get_data(as_text=True)
    assert "Todavía no hay respuestas" in html


def test_resultados_paginacion_estilo_etiquetas(entorno):
    sembrar(entorno)
    entorno.activar()
    _responder(entorno, 23, comentario=lambda rid: f"comentario numero {rid}")
    entorno.admin()
    html = entorno.client.get("/retiros/encuesta?por_pagina=10").get_data(as_text=True)
    assert "<b>1–10</b>" in html and "<b>23</b>" in html and "Página 1 de 3" in html and "Siguiente" in html
    assert "RET-T00122" in html                                           # el más reciente primero
    html3 = entorno.client.get("/retiros/encuesta?por_pagina=10&pagina=3").get_data(as_text=True)
    assert "<b>21–23</b>" in html3 and "Página 3 de 3" in html3
    assert "Página 3 de 3" in entorno.client.get("/retiros/encuesta?por_pagina=10&pagina=99").get_data(as_text=True)
    assert 'value="25" selected' in entorno.client.get("/retiros/encuesta?por_pagina=7").get_data(as_text=True)


def test_datos_de_ejemplo_no_guardan_respuestas_y_se_rotulan(entorno):
    entorno.admin()
    r = entorno.client.get("/retiros/encuesta?demo=1")
    html = r.get_data(as_text=True)
    assert r.status_code == 200 and "DATOS DE EJEMPLO" in html and "EJEMPLO-" in html
    assert entorno.db.contar("pickup_encuesta_respuestas") == 0 and entorno.db.contar("pickup_encuesta_invitaciones") == 0
    ps = [enc._fila_a_pregunta(f) for f in entorno.preguntas()]
    assert enc.datos_de_ejemplo(ps)[0] == enc.datos_de_ejemplo(ps)[0]     # misma semilla, mismos datos


def test_armar_resultados_y_semaforos():
    assert enc._sem_escala(4.5) == "verde" and enc._sem_escala(4.0) == "ambar" and enc._sem_escala(3.0) == "rojo"
    assert enc._sem_pct(95) == "verde" and enc._sem_pct(80) == "ambar" and enc._sem_pct(50) == "rojo"
    ps = [{"id": 1, "clave": "a", "texto": "A", "tipo": "estrellas", "activa": True, "orden": 1, "version": 1, "opciones": []},
          {"id": 2, "clave": "b", "texto": "B", "tipo": "opcion", "activa": True, "orden": 2, "version": 1,
           "opciones": [{"valor": "x", "etiqueta": "Bien", "color": "verde"}, {"valor": "y", "etiqueta": "Mal", "color": "rojo"}]},
          {"id": 3, "clave": "c", "texto": "C", "tipo": "texto", "activa": True, "orden": 3, "version": 1, "opciones": []}]
    filas = [{"clave": "a", "valor_num": 5, "valor_texto": None, "c": 3}, {"clave": "a", "valor_num": 1, "valor_texto": None, "c": 1},
             {"clave": "b", "valor_num": None, "valor_texto": "x", "c": 9}, {"clave": "b", "valor_num": None, "valor_texto": "viejo", "c": 1}]
    t = enc.armar_resultados(ps, filas)
    assert [x["clave"] for x in t] == ["a", "b"]                          # el texto libre no es tarjeta
    assert t[0]["prom"] == 4.0 and t[0]["color"] == "ambar" and t[0]["n"] == 4
    assert t[1]["pct"] == 90.0 and t[1]["color"] == "verde" and "versión anterior" in t[1]["barras"][-1]["etiqueta"]
    assert enc.armar_resultados(ps, [])[0]["kpi"] == "–"


# ═════════════════════════════════════════════════════════════════════
#  5. FICHA DEL RETIRO: enlace + estado (solo lectura)
# ═════════════════════════════════════════════════════════════════════
def test_ficha_muestra_enlace_y_estado_sin_crear_invitacion(entorno):
    entorno.personal()
    j = entorno.client.get("/retiros/encuesta/ficha/1").get_json()
    assert j["ok"] and j["estado"] == "sin_invitacion" and j["estado_txt"] == "No enviada" and j["activa"] is False
    assert j["url"].endswith(f"/retiros/encuesta/{entorno.token()}") and j["url"].startswith("http")
    assert j["elegible"] is True and j["respuestas"] == []
    assert entorno.db.contar("pickup_encuesta_invitaciones") == 0         # solo mirar: no escribe
    assert entorno.client.get("/retiros/encuesta/ficha/999").status_code == 404


def test_ficha_respondida_muestra_el_resultado(entorno):
    sembrar(entorno)
    entorno.activar()
    _responder(entorno, 1, estrellas="4", opcion="espera_corta", comentario="muy bien", desde=1)
    entorno.personal()
    j = entorno.client.get("/retiros/encuesta/ficha/1").get_json()
    assert j["estado"] == "respondida" and j["estado_txt"] == "Respondida" and j["respondida"]
    legibles = {r["pregunta"]: r["valor"] for r in j["respuestas"]}
    assert legibles["¿Tu pedido estaba listo cuando llegaste?"] == "Tuve que esperar menos de 10 minutos"
    assert "4 de 5" in legibles.values()


def test_ficha_estados_creada_y_enviada(entorno):
    entorno.activar()
    entorno.client.get(f"/retiros/encuesta/{entorno.token()}")           # se crea la invitación (aún sin enviar)
    entorno.personal()
    assert entorno.client.get("/retiros/encuesta/ficha/1").get_json()["estado"] == "creada"
    entorno.app.extensions["retiros_encuesta"]["marcar_enviada"](1)
    assert entorno.client.get("/retiros/encuesta/ficha/1").get_json()["estado"] == "enviada"


def test_include_de_la_ficha_se_renderiza_con_solo_req_id(entorno):
    with entorno.app.test_request_context("/"):
        html = render_template_string('{% include "retiros/_encuesta_ficha.html" %}', req={"id": 42})
    assert 'data-rid="42"' in html and "/retiros/encuesta/ficha/42" in html and "Copiar enlace" in html
    assert "ilusToast" in html and "alert(" not in html and "confirm(" not in html


# ═════════════════════════════════════════════════════════════════════
#  6. NADA SE ENVÍA A NADIE (y el enganche queda apagado)
# ═════════════════════════════════════════════════════════════════════
def test_ningun_correo_whatsapp_ni_aviso_en_todo_el_flujo(entorno):
    entorno.admin()
    for url in ("/retiros/encuesta", "/retiros/encuesta?demo=1", "/retiros/encuesta/vista-previa",
                "/retiros/encuesta/preguntas", "/retiros/encuesta/preguntas/datos", "/retiros/encuesta/ficha/1"):
        entorno.client.get(url)
    entorno.client.post("/retiros/encuesta/vista-previa", data=form_ok(entorno))
    entorno.client.post("/retiros/encuesta/preguntas/guardar", json={"texto": "Una pregunta nueva de prueba", "tipo": "si_no"})
    entorno.anonimo()
    entorno.client.get(f"/retiros/encuesta/{entorno.token()}")           # sin lanzar
    entorno.activar()
    entorno.client.get(f"/retiros/encuesta/{entorno.token()}")
    entorno.client.post(f"/retiros/encuesta/{entorno.token()}", data=form_ok(entorno))
    ext = entorno.app.extensions["retiros_encuesta"]
    ext["invitar"](1)
    ext["marcar_enviada"](1)
    ext["purgar"](dry_run=False, ahora=T0 + timedelta(days=900))
    assert not entorno.esp.hubo_envios()


def test_el_modulo_no_tiene_codigo_de_envio():
    fuente = open(os.path.join(RAIZ, "retiros_encuesta.py"), encoding="utf-8").read()
    codigo = re.sub(r'""".*?"""', "", fuente, flags=re.S)
    codigo = "\n".join(l.split("# ")[0] for l in codigo.splitlines())
    for prohibido in ("smtplib", "import requests", "urllib", "_send_ilus_email", "_send_whatsapp", "_mant_notificar",
                      "_checkwms_get", "twilio", "resend", "sendgrid"):
        assert prohibido not in codigo, prohibido


def test_mientras_no_se_lance_nada_del_cliente_apunta_a_la_encuesta():
    """GUARDIA DE «NO LANZADA»: las páginas del cliente no enlazan la encuesta. Al lanzarla (paso aparte que Daniel
    aprueba) esta prueba se actualiza junto con el enganche."""
    for rel in ("templates/retiros/public_tracking.html", "templates/retiros/public_request.html"):
        ruta = os.path.join(RAIZ, rel)
        if os.path.exists(ruta):
            assert "retiros/encuesta" not in open(ruta, encoding="utf-8").read(), rel


def test_enganche_invitar_apagado_no_escribe_y_encendido_solo_crea_la_invitacion(entorno):
    ext = entorno.app.extensions["retiros_encuesta"]
    antes = len(entorno.db.escrituras)
    assert ext["invitar"](1) is None and len(entorno.db.escrituras) == antes and entorno.db.ddl == []
    entorno.activar()
    entorno.db.retiro(2, status="en_preparacion")
    assert ext["invitar"](2) is None                                       # aún no retirado
    inv = ext["invitar"](1)
    assert inv["ruta"] == f"/retiros/encuesta/{entorno.token()}" and inv["token"] == entorno.token()
    assert inv["vence"] == T0 + timedelta(days=30)
    fila = entorno.db.fetchone("SELECT * FROM pickup_encuesta_invitaciones")
    assert fila["enviada_en"] is None                                      # el que envíe lo marca después
    assert ext["marcar_enviada"](1) == 1 and ext["marcar_enviada"](1) == 0
    assert not entorno.esp.hubo_envios()


# ═════════════════════════════════════════════════════════════════════
#  7. CONSERVACIÓN (Ley 21.719): anonimización
# ═════════════════════════════════════════════════════════════════════
def test_constantes_de_conservacion_y_aviso():
    assert enc.MESES_CONSERVACION == 24 and enc.AVISO_VERSION and enc.DIAS_VIGENCIA_ENLACE == 30
    assert enc.RESPONSABLE_RAZON == "Sport and Health Solutions SPA" and enc.RESPONSABLE_RUT == "76.996.964-0"
    assert enc.CONTACTO_DERECHOS == "soportetec@sphs.cl"


def test_purgar_anonimiza_lo_vencido_y_no_borra_filas(entorno):
    sembrar(entorno)
    entorno.activar()
    _responder(entorno, 2, opcion="listo", comentario="datos que podrian ser personales", desde=10)
    ext = entorno.app.extensions["retiros_encuesta"]
    tok = entorno.token(10)
    # nada vencido todavía
    assert ext["purgar"](dry_run=True) == {"candidatas": 0, "anonimizadas": 0}
    # una respuesta de hace 25 meses, otra reciente
    entorno.db.execute("UPDATE pickup_encuesta_invitaciones SET respondida_en=%s WHERE request_id=10",
                       (T0 - timedelta(days=25 * 31),))
    entorno.db.execute("UPDATE pickup_requests SET closed_at=%s WHERE id=10", (T0 - timedelta(days=26 * 31),))
    assert ext["purgar"](dry_run=True) == {"candidatas": 1, "anonimizadas": 0}
    assert entorno.db.contar("pickup_encuesta_invitaciones", "anonimizada_en IS NOT NULL") == 0     # dry-run no toca nada
    assert ext["purgar"](dry_run=False) == {"candidatas": 1, "anonimizadas": 1}
    viejo = entorno.db.fetchone("SELECT * FROM pickup_encuesta_invitaciones WHERE anonimizada_en IS NOT NULL")
    assert viejo["request_id"] is None and viejo["token_hash"] != enc._hash_token(tok)
    # el texto libre se borra; los números y opciones (estadística anónima) quedan; no se borró ninguna fila
    assert entorno.db.contar("pickup_encuesta_respuestas", "valor_texto IS NOT NULL AND pregunta_id=%d" % pid(entorno, "comentario")) == 1
    assert entorno.db.contar("pickup_encuesta_respuestas") == 8 and entorno.db.contar("pickup_encuesta_invitaciones") == 2
    assert entorno.db.contar("pickup_encuesta_respuestas", "valor_num IS NOT NULL") == 4
    # el enlace viejo ya no abre nada
    entorno.anonimo()
    assert entorno.client.get(f"/retiros/encuesta/{tok}").status_code == 404
    # es idempotente
    assert ext["purgar"](dry_run=False) == {"candidatas": 0, "anonimizadas": 0}


# ═════════════════════════════════════════════════════════════════════
#  8. TOKEN Y CABECERAS
# ═════════════════════════════════════════════════════════════════════
def test_token_derivado_no_expone_el_public_token_ni_abre_el_seguimiento():
    t = enc.generar_token("secreto", 42, "PUBLIC-TOKEN-DEL-SEGUIMIENTO")
    assert re.fullmatch(r"[0-9a-z]+-[0-9a-f]{24}", t)
    assert "PUBLIC-TOKEN" not in t and t != "PUBLIC-TOKEN-DEL-SEGUIMIENTO"
    assert t == enc.generar_token("secreto", 42, "PUBLIC-TOKEN-DEL-SEGUIMIENTO")
    assert t != enc.generar_token("otro-secreto", 42, "PUBLIC-TOKEN-DEL-SEGUIMIENTO")
    assert t != enc.generar_token("secreto", 42, "otro-public-token") and t != enc.generar_token("secreto", 43, "PUBLIC-TOKEN-DEL-SEGUIMIENTO")
    assert enc.generar_token("", 1, "x") == "" and enc.generar_token("s", 1, "") == ""
    assert enc._id_de_token(t) == 42 and enc._id_de_token("x") is None and enc._id_de_token("1-zz") is None


def test_cabeceras_sin_cache_sin_indexar_sin_referer(entorno):
    entorno.activar()
    for url in (f"/retiros/encuesta/{entorno.token()}", "/retiros/encuesta/basura"):
        r = entorno.client.get(url)
        assert r.headers["Cache-Control"] == "no-store" and "noindex" in r.headers["X-Robots-Tag"]
        assert r.headers["Referrer-Policy"] == "no-referrer"


def test_plantillas_publicas_sin_alert_confirm_ni_nombre_viejo():
    for rel in ("_encuesta_base.html", "encuesta_publica.html", "encuesta_gracias.html", "encuesta_no_disponible.html",
                "encuesta_resultados.html", "encuesta_editar.html", "_encuesta_ficha.html"):
        txt = open(os.path.join(RAIZ, "templates", "retiros", rel), encoding="utf-8").read()
        assert "Sport & Health" not in txt and "Sport &amp; Health" not in txt, rel
        assert not re.search(r"(?<![\w.])(alert|confirm|prompt)\(", txt), rel
    for rel in ("retiros_encuesta.js", "retiros_encuesta_editar.js"):
        txt = open(os.path.join(RAIZ, "static", rel), encoding="utf-8").read()
        assert not re.search(r"(?<![\w.])(alert|prompt)\(", txt) and "window.confirm" not in txt, rel


# ══════════════════════════════════════════════════════════════════════════════
#  VISTA PREVIA (Daniel 2026-10-09: «de momento, los cambios de la encuesta y la firma los vea yo nada más»)
# ══════════════════════════════════════════════════════════════════════════════
class TestSoloDanielPorAhora:
    def _entorno(self, monkeypatch):
        e = construir(monkeypatch, vista_previa=None)            # sin variable: el valor por defecto (solo Daniel)
        e.equipo = lambda: e.sesion.update(user={"id": 7, "nombre": "Sam", "username": "sam@sphs.cl"},
                                           permissions={"retiros": True, "admin": True, "ret_horarios": True})
        e.daniel = lambda: e.sesion.update(user={"id": 1, "nombre": "Daniel", "username": "daniel.aguilar@sphs.cl"},
                                           permissions={"retiros": True, "superadmin": True})
        return e

    @pytest.mark.parametrize("ruta", ["/retiros/encuesta", "/retiros/encuesta/vista-previa", "/retiros/encuesta/preguntas",
                                      "/retiros/encuesta/preguntas/datos", "/retiros/encuesta/ficha/1"])
    def test_el_equipo_no_la_ve_aunque_sea_admin(self, monkeypatch, ruta):
        e = self._entorno(monkeypatch)
        e.equipo()
        assert e.client.get(ruta).status_code in (403, 404)

    @pytest.mark.parametrize("ruta", ["/retiros/encuesta", "/retiros/encuesta/vista-previa", "/retiros/encuesta/preguntas",
                                      "/retiros/encuesta/ficha/1"])
    def test_daniel_si(self, monkeypatch, ruta):
        e = self._entorno(monkeypatch)
        e.daniel()
        assert e.client.get(ruta).status_code == 200

    def test_el_equipo_no_puede_guardar_preguntas(self, monkeypatch):
        e = self._entorno(monkeypatch)
        e.equipo()
        r = e.client.post("/retiros/encuesta/preguntas/guardar", json={"texto": "¿Otra?", "tipo": "escala"})
        assert r.status_code in (403, 404)

    def test_el_enlace_del_cliente_no_se_abre_para_el_equipo_mientras_no_se_lance(self, monkeypatch):
        e = self._entorno(monkeypatch)
        e.equipo()
        assert e.client.get(f"/retiros/encuesta/{e.token()}").status_code == 404

    @pytest.mark.parametrize("valor, esperado", [("*", True), ("sam@sphs.cl, otro@sphs.cl", True), ("otro@sphs.cl", False), ("", False)])
    def test_la_lista_se_puede_ampliar_por_entorno(self, monkeypatch, valor, esperado):
        monkeypatch.setenv("RETIROS_VISTA_PREVIA_USUARIOS", valor)
        if valor == "":
            esperado = False                                     # vacía = el valor por defecto (solo Daniel)
        assert enc.usuario_en_vista_previa({"username": "sam@sphs.cl"}) is esperado

    def test_sin_sesion_nunca(self, monkeypatch):
        monkeypatch.setenv("RETIROS_VISTA_PREVIA_USUARIOS", "*")
        assert enc.usuario_en_vista_previa(None) is False

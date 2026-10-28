# -*- coding: utf-8 -*-
"""2026-10-27 · Modal «Nuevo Ticket» con el formato de Retiros (Tickets 2.0).

Pedido de Daniel: «los objetos, las formas, el mismo formato, eso quiero que lo
heredes» — el modal manual de la bandeja hereda el formato del «Nuevo retiro
interno» (pasos con semáforo, «N de 5 pasos listos», «Falta: …», acción grande
«+ Agregar…», «Editar manualmente», botón principal deshabilitado), y suma la
OT existente como origen, el motivo principal y el responsable.

Qué se verifica:
  · tickets_nuevo_motivos.py (puro): todos los tipos de TK_TIPOS tienen motivos
    sugeridos, ninguno pasa de 160 caracteres, normalizar_motivo recorta.
  · templates/tickets/list.html: Jinja válido; el modal conserva TODOS los ids
    del modal anterior (REGLA #4.2); tiene los 5 pasos, el progreso, «Falta»,
    «Seleccionar/deseleccionar todo» (REGLA #14), «Editar manualmente»; sin
    alert/confirm/prompt nativos (REGLA #1); el CSS nuevo está enlazado.
  · static/tickets_list.js: sintaxis válida (Node) y el bloque del modal manda
    visita_id, motivo_principal, asignado_a y documentos; sin nativos.
  · Rutas REALES contra el MySQL 8 de PRUEBA (127.0.0.1:3399, base propia
    `tk_nuevo_modal`; se salta si no está): buscador de OT (sin plata, REGLA
    #26), detalle con equipos y documentos, POST /tickets/api/tickets con OT +
    motivo + documentos + equipos, 409 si la OT ya tiene ticket, 400 si no
    existe, «sin asignar» respetado. Correo y avisos de MENTIRA (REGLA #22).

    py -m pytest tests/test_tickets_nuevo_modal.py -q
"""
import io
import os
import re
import shutil
import subprocess
import sys

import pytest

_TESTS = os.path.dirname(os.path.abspath(__file__))
_RAIZ = os.path.dirname(_TESTS)
sys.path.insert(0, _RAIZ)

import tickets_nuevo_motivos as motivos  # noqa: E402

PLANTILLA = os.path.join(_RAIZ, "templates", "tickets", "list.html")
JS = os.path.join(_RAIZ, "static", "tickets_list.js")
CSS = os.path.join(_RAIZ, "static", "tickets_nuevo.css")


def _leer(p):
    return io.open(p, encoding="utf-8").read()


def _modal_html():
    html = _leer(PLANTILLA)
    i = html.index('id="ntModal"')
    j = html.index("MODAL: Recuperar correos")
    return html[i:j]


def _bloque_modal_js():
    js = _leer(JS)
    i = js.index("MODAL Nuevo Ticket")
    j = js.index("Crear ticket desde documento ERP")
    return js[i:j]


# ── 1. Motivos sugeridos (puro) ─────────────────────────────────────────────
def test_motivos_para_todos_los_tipos():
    import tickets_module as tm
    for t in tm.TK_TIPOS:
        lista = motivos.motivos_para(t)
        assert lista and all(isinstance(m, str) and m.strip() for m in lista), t
        assert all(len(m) <= motivos.LARGO_MAXIMO for m in lista), t
    # tipos que otro trabajo está sumando: ya tienen sugerencias
    assert motivos.motivos_para("facturacion") and motivos.motivos_para("consulta")
    # un tipo desconocido recibe los genéricos, nunca una lista vacía
    assert motivos.motivos_para("no_existe") == list(motivos._GENERICOS)
    todos = motivos.motivos_todos()
    assert isinstance(todos, dict) and "tech_support" in todos


def test_normalizar_motivo():
    assert motivos.normalizar_motivo("  Equipo   no  enciende ") == "Equipo no enciende"
    assert motivos.normalizar_motivo("") is None and motivos.normalizar_motivo(None) is None
    assert len(motivos.normalizar_motivo("x" * 500)) == 160


# ── 2. Plantilla ────────────────────────────────────────────────────────────
IDS_DE_SIEMPRE = [
    "ntModal", "ntTipos", "ntErrTipo", "fTipoWrap", "ntGarWrap", "ntCardCliente", "ntClienteOpcionalHint",
    "ntBuscarCliWrap", "ntBuscarCli", "ntSearchTag", "ntAcCli", "fRutWrap", "ntRut", "ntErrRut", "fEmpresaWrap",
    "ntEmpresa", "ntErrEmpresa", "fContactoWrap", "ntContacto", "ntErrContacto", "fPhoneWrap", "ntPhone", "ntVPhone",
    "ntErrPhone", "fEmailWrap", "ntEmail", "ntVEmail", "ntErrEmail", "ntSucursal", "ntCardDireccion", "ntDirAsterisco",
    "ntDireccionOpcionalHint", "fDirWrap", "ntDireccion", "ntDirLat", "ntDirLng", "ntDirPlace", "ntComuna", "ntRegion",
    "ntDirHint", "ntErrDir", "ntBuscarEq", "ntAcEq", "ntEqCount", "ntEqCountTxt", "ntListaEq", "fDescWrap", "ntDesc",
    "ntDescCount", "ntErrDesc", "ntPrio", "ntDoc", "ntDrop", "ntFile", "ntFiles", "ntBtnCrear",
]
IDS_NUEVOS = [
    "ntwProgreso", "ntwProgresoTxt", "ntwProgresoBar", "ntwProgresoAvisos",
    "ntwStep1", "ntwStep2", "ntwStep3", "ntwStep4", "ntwStep5",
    "ntwEstado1", "ntwEstado2", "ntwEstado3", "ntwEstado4", "ntwEstado5",
    "ntwBtnOt", "ntwOtSearch", "ntwOtQ", "ntwOtAc", "ntwBtnDoc", "ntwOrigenes",
    "ntwClienteManualBtn", "ntwClienteManualBadge", "ntwCliChips", "ntwEqToggleAll",
    "ntMotivo", "ntwMotivos", "fMotivoWrap", "ntResp",
]


def test_plantilla_jinja_valida():
    from jinja2 import Environment
    Environment().parse(_leer(PLANTILLA))


def test_modal_conserva_ids_y_formato_retiros():
    m = _modal_html()
    for i in IDS_DE_SIEMPRE + IDS_NUEVOS:
        assert re.search(r'id="%s"' % re.escape(i), m), f"falta id={i}"
    assert "0 de 5 pasos listos" in m
    assert m.count('class="ntw-step"') == 5
    assert m.count("ntw-step-circle-label") == 5
    assert "Seleccionar/deseleccionar todo" in m           # REGLA #14
    assert "Editar manualmente" in m
    assert "Agregar factura, boleta o nota de venta" in m
    assert "Asociar una OT existente" in m
    assert 'id="ntBtnCrear" disabled' in m                  # deshabilitado hasta completar
    assert "modal-xl" in m
    # el correo del ERP nunca se autocompleta: hay chips
    assert "ntwCliChips" in m
    # RUT/Empresa nacen como vitrina (readonly) con «Editar manualmente»
    assert re.search(r'id="ntEmpresa"[^>]*readonly', m) and re.search(r'id="ntRut"[^>]*readonly', m)


def test_sin_nativos_y_css_enlazado():
    m = _modal_html()
    js = _bloque_modal_js()
    for nativo in ("alert(", "confirm(", "prompt("):
        assert not re.search(r"(?<![A-Za-z])" + re.escape(nativo), m), nativo
        assert not re.search(r"(?<![A-Za-z])" + re.escape(nativo), js), nativo
    html = _leer(PLANTILLA)
    assert "tickets_nuevo.css" in html
    assert "MOTIVOS:" in html and "tk_motivos_sugeridos" in html
    assert os.path.exists(CSS)
    css = _leer(CSS)
    for cls in (".ntw-step", ".ntw-step-circle", ".ntw-step-estado", ".ntw-add", ".ntw-progreso", ".ntw-chip", ".ntw-locked"):
        assert cls in css, cls


# ── 3. JavaScript ───────────────────────────────────────────────────────────
@pytest.mark.skipif(shutil.which("node") is None, reason="sin Node")
def test_js_sintaxis_node():
    r = subprocess.run(["node", "--check", JS], capture_output=True, text=True)
    assert r.returncode == 0, r.stderr


def test_js_manda_los_campos_nuevos():
    js = _bloque_modal_js()
    for campo in ("visita_id:", "motivo_principal:", "asignado_a:", "sin_asignar:", "documentos:", "tido:e.tido", "serie:e.serie"):
        assert campo in js, campo
    assert "/tickets/api/ot/buscar" in js and "/tickets/api/responsables" in js
    assert "mode: 'seleccionar'" in js                       # buscador estándar de documentos
    assert "OT_YA_TIENE_TICKET" in js                        # salida cuando la OT ya tiene ticket
    assert "ntwEqToggleAll" in js and "some(e=>!e.sel)" in js  # REGLA #14 como toggle
    assert "ntwRutValido" in js


# ── 4. Rutas reales contra el MySQL de prueba ───────────────────────────────
PORT = 3399
DB = "tk_nuevo_modal"


def _mysql_disponible():
    try:
        import pymysql
        c = pymysql.connect(host="127.0.0.1", port=PORT, user="root", password="", connect_timeout=2)
        c.close()
        return True
    except Exception:
        return False


@pytest.fixture(scope="module")
def entorno():
    if not _mysql_disponible():
        pytest.skip("MySQL de prueba 127.0.0.1:%d no disponible" % PORT)
    import pymysql
    from flask import Flask, g, request

    c0 = pymysql.connect(host="127.0.0.1", port=PORT, user="root", password="", autocommit=True)
    c0.cursor().execute(f"DROP DATABASE IF EXISTS {DB}")
    c0.cursor().execute(f"CREATE DATABASE {DB} CHARACTER SET utf8mb4")

    def _con(db=DB):
        return pymysql.connect(host="127.0.0.1", port=PORT, user="root", password="", database=db,
                               autocommit=True, charset="utf8mb4", cursorclass=pymysql.cursors.DictCursor)

    conn = _con()

    def mysql_execute(sql, params=None):
        with conn.cursor() as cur:
            return cur.execute(sql, params)

    def mysql_fetchone(sql, params=None):
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone()

    def mysql_fetchall(sql, params=None):
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()

    for ddl in (
        "CREATE TABLE app_users (id INT PRIMARY KEY, username VARCHAR(190), nombre VARCHAR(190), role VARCHAR(40), active TINYINT DEFAULT 1)",
        "CREATE TABLE rol_permisos (rol_slug VARCHAR(40), modulo VARCHAR(40), accion VARCHAR(60), permitido TINYINT)",
        "CREATE TABLE usuario_permisos (user_id INT, permiso VARCHAR(80), permitido TINYINT)",
        "CREATE TABLE mant_clientes (id INT PRIMARY KEY, razon_social VARCHAR(200), rut VARCHAR(20), contacto_nombre VARCHAR(200), "
        " contacto_cargo VARCHAR(100), contacto_tel VARCHAR(40), contacto_email VARCHAR(150), contacto2_nombre VARCHAR(200), "
        " contacto2_cargo VARCHAR(100), contacto2_tel VARCHAR(40), contacto2_email VARCHAR(150), direccion TEXT, comuna VARCHAR(100), "
        " region VARCHAR(100), INDEX(rut))",
        "CREATE TABLE mant_sucursales (id INT PRIMARY KEY, cliente_id INT, nombre VARCHAR(100), encargado_nombre VARCHAR(100), "
        " encargado_cargo VARCHAR(100), encargado_tel VARCHAR(40), encargado_email VARCHAR(150), activo TINYINT DEFAULT 1)",
        "CREATE TABLE mant_visitas (id INT PRIMARY KEY, numero_ot VARCHAR(30), titulo VARCHAR(200), tipo VARCHAR(30), estado VARCHAR(30), "
        " fecha_programada DATE, cliente_id INT, costo DECIMAL(12,2), costo_proveedor DECIMAL(12,2))",
        "CREATE TABLE mant_maquinas (id INT PRIMARY KEY, cliente_id INT, sku VARCHAR(100), nombre VARCHAR(400), serie VARCHAR(100))",
        "CREATE TABLE mant_visita_equipos (id INT AUTO_INCREMENT PRIMARY KEY, visita_id INT, maquina_id INT)",
        "CREATE TABLE mant_visita_documentos (id INT AUTO_INCREMENT PRIMARY KEY, visita_id INT, erp_tido VARCHAR(10), erp_nudo VARCHAR(30), "
        " es_principal TINYINT DEFAULT 0, monto DECIMAL(14,2))",
        "CREATE TABLE mant_repuestos_compras (id INT PRIMARY KEY, ticket_id INT)",
    ):
        mysql_execute(ddl)
    mysql_execute("INSERT INTO app_users (id,username,nombre,role) VALUES (1,'daniel@x.cl','Daniel Aguilar','superadmin'),"
                  "(2,'victor@x.cl','Victor Betancur','ejecutivo')")
    mysql_execute("INSERT INTO rol_permisos VALUES ('ejecutivo','tickets','es_ejecutivo',1)")
    mysql_execute("INSERT INTO mant_clientes (id,razon_social,rut,contacto_nombre,contacto_tel,contacto_email,direccion,comuna,region) VALUES "
                  "(10,'Gimnasio De Mentira SpA','76123456-7','Juana Prueba','+56 9 1111 2222','juana@demo.cl','Av. Falsa 123','Ñuñoa','Metropolitana')")
    mysql_execute("INSERT INTO mant_visitas (id,numero_ot,titulo,tipo,estado,fecha_programada,cliente_id,costo,costo_proveedor) VALUES "
                  "(501,'OT-2026-00501','Instalación trotadora','instalacion','cerrada','2026-10-20',10,350000,120000),"
                  "(502,'OT-2026-00502','Mantención sin cliente','revision_interna','programada','2099-12-31',NULL,0,0)")
    mysql_execute("INSERT INTO mant_maquinas (id,cliente_id,sku,nombre,serie) VALUES (7,10,'TR-9000','Trotadora Comercial 9000 con pantalla táctil','SER-777'),"
                  "(8,10,'BK-200','Bicicleta estática BK-200','')")
    mysql_execute("INSERT INTO mant_visita_equipos (visita_id,maquina_id) VALUES (501,7),(501,8)")
    mysql_execute("INSERT INTO mant_visita_documentos (visita_id,erp_tido,erp_nudo,es_principal,monto) VALUES (501,'FCV','11439',1,350000),(501,'NVV','2233',0,350000)")

    notifs, correos = [], []
    app = Flask(__name__, template_folder=os.path.join(_RAIZ, "templates"))
    app.secret_key = "x"
    perfiles = {"super": ({"superadmin": True, "admin": True, "tk_ver": True}, "daniel@x.cl"),
                "victor": ({"tk_es_ejecutivo": True}, "victor@x.cl")}

    @app.before_request
    def _fake_login():
        perm, user = perfiles[request.headers.get("X-Perfil", "super")]
        g.permissions = dict(perm)
        g._usr = user

    @app.route("/")
    def index():
        return "ok"

    ctx = {
        "mysql_fetchone": mysql_fetchone, "mysql_fetchall": mysql_fetchall, "mysql_execute": mysql_execute,
        "get_mysql": _con, "login_required": (lambda f: f),
        "current_username": lambda: getattr(g, "_usr", None),
        "_mant_notificar": lambda uid, tipo, titulo, **k: notifs.append((uid, tipo, titulo)),
        "_send_ilus_email": lambda to, subj, html, **k: correos.append((to, k.get("correo"))) or True,
        "normalizar_rut": lambda r: (r or "").replace(".", "").upper(),
        "_no_tecnico_externo": (lambda f: f),
    }
    import tickets_module as tm

    class _SinHilo:
        def __init__(self, *a, **k):
            pass

        def start(self):
            pass

    hilo_real = tm.threading.Thread
    tm.threading.Thread = _SinHilo
    try:
        tm.register_tickets_routes(app, ctx)
    finally:
        tm.threading.Thread = hilo_real
    cols = {r["COLUMN_NAME"] for r in mysql_fetchall(
        "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=%s AND TABLE_NAME='tk_tickets'", (DB,))}
    assert {"motivo_principal", "visita_id"} <= cols
    yield {"cl": app.test_client(), "q1": mysql_fetchone, "qa": mysql_fetchall, "correos": correos}
    conn.close()
    c0.cursor().execute(f"DROP DATABASE IF EXISTS {DB}")
    c0.close()


H = {"Accept": "application/json"}


def _sin_plata(obj):
    txt = str(obj).lower()
    for palabra in ("costo", "monto", "margen", "proveedor"):
        assert palabra not in txt, palabra


def test_ot_buscar_por_numero_cliente_y_rut(entorno):
    cl = entorno["cl"]
    r = cl.get("/tickets/api/ot/buscar?q=OT-2026-005", headers=H)
    assert r.status_code == 200
    j = r.get_json()
    assert j["ok"] and {x["id"] for x in j["resultados"]} == {501, 502}
    _sin_plata(j)
    ot = next(x for x in j["resultados"] if x["id"] == 501)
    assert ot["numero_ot"] == "OT-2026-00501" and ot["tipo_label"] == "Instalación" and ot["estado_label"] == "Cerrada"
    assert ot["fecha"] == "20/10/2026" and ot["equipos_n"] == 2 and ot["ticket_existente"] is None
    assert ot["cliente"]["razon_social"] == "Gimnasio De Mentira SpA" and ot["cliente"]["rut"] == "76123456-7"
    interna = next(x for x in j["resultados"] if x["id"] == 502)
    assert interna["cliente"] is None and interna["fecha"] == ""      # centinela 2099 = sin fecha
    # por nombre del cliente y por RUT con puntos y guion
    assert {x["id"] for x in cl.get("/tickets/api/ot/buscar?q=mentira", headers=H).get_json()["resultados"]} == {501}
    assert {x["id"] for x in cl.get("/tickets/api/ot/buscar?q=76.123.456-7", headers=H).get_json()["resultados"]} == {501}
    assert cl.get("/tickets/api/ot/buscar?q=x", headers=H).get_json()["resultados"] == []


def test_ot_detalle_trae_equipos_y_documentos_sin_plata(entorno):
    cl = entorno["cl"]
    j = cl.get("/tickets/api/ot/501", headers=H).get_json()
    assert j["ok"]
    _sin_plata(j)
    ot = j["ot"]
    assert [e["sku"] for e in ot["equipos"]] == ["TR-9000", "BK-200"]
    assert ot["equipos"][0]["nombre"] == "Trotadora Comercial 9000 con pantalla táctil" and ot["equipos"][0]["serie"] == "SER-777"
    assert [(d["tido"], d["nudo"], d["principal"]) for d in ot["documentos"]] == [("FCV", "11439", True), ("NVV", "2233", False)]
    assert cl.get("/tickets/api/ot/999999", headers=H).status_code == 404


def _payload(**extra):
    base = {
        "tipo": "tech_support", "prioridad": "alta", "rut": "76.123.456-7", "empresa": "Gimnasio De Mentira SpA",
        "nombre_contacto": "Juana Prueba", "phone": "+56 9 1111 2222", "email": "juana@demo.cl",
        "direccion": "Av. Falsa 123, Ñuñoa", "descripcion": "La trotadora hace un ruido fuerte al correr",
        "motivo_principal": "  Ruido o   vibración anormal ",
        "documentos": [{"tido": "fcv", "nudo": "11439"}, {"tido": "NVV", "nudo": "2233"}],
        "equipos": [{"kopr": "TR-9000", "sku": "TR-9000", "nombre": "Trotadora Comercial 9000 con pantalla táctil",
                     "serie": "SER-777", "notas": "ruido en la banda", "cantidad": 1}],
    }
    base.update(extra)
    return base


def test_crear_con_ot_motivo_documentos_y_equipos(entorno):
    cl, q1, qa = entorno["cl"], entorno["q1"], entorno["qa"]
    r = cl.post("/tickets/api/tickets", json=_payload(visita_id=501, asignado_a="Victor Betancur"), headers=H)
    assert r.status_code == 200, r.data[:300]
    j = r.get_json()
    assert j["ok"] and j["numero_ticket"].startswith("TK-")
    t = q1("SELECT motivo_principal, visita_id, asignado_a, numero_documento FROM tk_tickets WHERE id=%s", (j["id"],))
    assert t["motivo_principal"] == "Ruido o vibración anormal" and t["visita_id"] == 501 and t["asignado_a"] == "Victor Betancur"
    docs = {(d["erp_tido"], d["erp_nudo"]) for d in qa("SELECT erp_tido, erp_nudo FROM tk_ticket_documentos WHERE ticket_id=%s", (j["id"],))}
    assert docs == {("FCV", "11439"), ("NVV", "2233")}
    eq = qa("SELECT sku, serie, notas FROM tk_ticket_equipos WHERE ticket_id=%s", (j["id"],))
    assert len(eq) == 1 and eq[0]["serie"] == "SER-777" and eq[0]["notas"] == "ruido en la banda"
    log = q1("SELECT contenido FROM tk_mensajes WHERE ticket_id=%s AND tipo='creacion' LIMIT 1", (j["id"],))
    assert "OT-2026-00501" in log["contenido"] and "Ruido o vibración anormal" in log["contenido"]
    # el buscador ahora avisa que la OT ya tiene ticket
    ot = next(x for x in cl.get("/tickets/api/ot/buscar?q=OT-2026-00501", headers=H).get_json()["resultados"] if x["id"] == 501)
    assert ot["ticket_existente"] == {"id": j["id"], "numero_ticket": j["numero_ticket"]}
    # una OT, un ticket: el segundo intento se rechaza con una salida clara
    r2 = cl.post("/tickets/api/tickets", json=_payload(visita_id=501), headers=H)
    assert r2.status_code == 409
    j2 = r2.get_json()
    assert j2["error_codigo"] == "OT_YA_TIENE_TICKET" and j2["ticket_id"] == j["id"] and "OT-2026-00501" in j2["error"]
    assert q1("SELECT COUNT(*) n FROM tk_tickets WHERE visita_id=501")["n"] == 1


def test_crear_rechaza_ot_inexistente_y_respeta_sin_asignar(entorno):
    cl, q1 = entorno["cl"], entorno["q1"]
    r = cl.post("/tickets/api/tickets", json=_payload(visita_id=999999), headers=H)
    assert r.status_code == 400 and r.get_json()["error_codigo"] == "OT_NO_EXISTE"
    r = cl.post("/tickets/api/tickets", json=_payload(visita_id="abc"), headers=H)
    assert r.status_code == 400 and r.get_json()["error_codigo"] == "OT_INVALIDA"
    # sin OT, sin motivo y «sin asignar» explícito: se crea igual (recomendado, no obligatorio)
    r = cl.post("/tickets/api/tickets", json=_payload(motivo_principal="", documentos=[], equipos=[], asignado_a="", sin_asignar=1), headers=H)
    assert r.status_code == 200, r.data[:300]
    t = q1("SELECT motivo_principal, visita_id, asignado_a FROM tk_tickets WHERE id=%s", (r.get_json()["id"],))
    assert t["motivo_principal"] is None and t["visita_id"] is None and t["asignado_a"] is None
    # sin «sin_asignar»: el comportamiento de siempre (quien crea)
    r = cl.post("/tickets/api/tickets", json=_payload(asignado_a=""), headers=H)
    assert q1("SELECT asignado_a FROM tk_tickets WHERE id=%s", (r.get_json()["id"],))["asignado_a"] == "daniel@x.cl"
    # ningún correo real: solo la lista de mentira recibió algo
    assert all(isinstance(c, tuple) for c in entorno["correos"])

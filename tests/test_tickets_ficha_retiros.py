"""Tickets 2.0 · ficha fiel a Retiros (27/28-oct): rutas REALES del módulo contra
un MySQL 8 de PRUEBA (127.0.0.1:3399, root sin clave; nunca producción, REGLA #22).

Cubre: el bloque sla_resp y la guía en tk_api_get, motivo/diagnóstico por el
PATCH, buscar y asociar una OT existente (candados), quitar el vínculo, los
tickets del cliente paginados para la ficha del cliente, el dictado por voz
(configuración correcta de Google Speech) y el render de las plantillas.

Uso: python -m pytest tests/test_tickets_ficha_retiros.py -q
"""
import os
import sys
import types
from datetime import datetime, timedelta

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

pymysql = pytest.importorskip("pymysql")
from flask import Flask, g, request  # noqa: E402

PORT = int(os.environ.get("TK_TEST_MYSQL_PORT") or 3399)
DB = "tk_ficha_retiros"

try:
    _c0 = pymysql.connect(host="127.0.0.1", port=PORT, user="root", password="", autocommit=True, connect_timeout=3)
except Exception as _e:  # pragma: no cover - sin MySQL de prueba
    pytest.skip(f"Sin MySQL de prueba en 127.0.0.1:{PORT}: {_e}", allow_module_level=True)

_c0.cursor().execute(f"DROP DATABASE IF EXISTS {DB}")
_c0.cursor().execute(f"CREATE DATABASE {DB} CHARACTER SET utf8mb4")


def _con():
    return pymysql.connect(host="127.0.0.1", port=PORT, user="root", password="", database=DB,
                           autocommit=True, charset="utf8mb4", cursorclass=pymysql.cursors.DictCursor)


_CONN = _con()


def mysql_execute(sql, params=None):
    with _CONN.cursor() as cur:
        return cur.execute(sql, params)


def mysql_fetchone(sql, params=None):
    with _CONN.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchone()


def mysql_fetchall(sql, params=None):
    with _CONN.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


for ddl in (
    "CREATE TABLE app_users (id INT PRIMARY KEY, username VARCHAR(190), nombre VARCHAR(190), role VARCHAR(40), active TINYINT DEFAULT 1)",
    "CREATE TABLE rol_permisos (rol_slug VARCHAR(40), modulo VARCHAR(40), accion VARCHAR(60), permitido TINYINT)",
    "CREATE TABLE usuario_permisos (user_id INT, permiso VARCHAR(80), permitido TINYINT)",
    "CREATE TABLE mant_clientes (id INT PRIMARY KEY, razon_social VARCHAR(200), rut VARCHAR(20), direccion VARCHAR(200), comuna VARCHAR(80), "
    " region VARCHAR(80), email_empresa VARCHAR(150), tel_empresa VARCHAR(40), contacto_nombre VARCHAR(200), "
    " contacto_cargo VARCHAR(100), contacto_tel VARCHAR(40), contacto_email VARCHAR(150), contacto2_nombre VARCHAR(200), "
    " contacto2_cargo VARCHAR(100), contacto2_tel VARCHAR(40), contacto2_email VARCHAR(150), INDEX(rut))",
    "CREATE TABLE mant_sucursales (id INT PRIMARY KEY, cliente_id INT, nombre VARCHAR(100), encargado_nombre VARCHAR(100), "
    " encargado_cargo VARCHAR(100), encargado_tel VARCHAR(40), encargado_email VARCHAR(150), activo TINYINT DEFAULT 1)",
    "CREATE TABLE mant_visitas (id INT PRIMARY KEY, cliente_id INT, numero_ot VARCHAR(30), titulo VARCHAR(200), estado VARCHAR(40), "
    " fecha_programada DATE, tecnico VARCHAR(200))",
    "CREATE TABLE mant_repuestos_compras (id INT PRIMARY KEY, ticket_id INT)",
    "CREATE TABLE mant_ot_repuesto_solicitudes (id INT PRIMARY KEY, ticket_id INT)",
):
    mysql_execute(ddl)
for u in [(1, "daniel@x.cl", "Daniel Aguilar", "superadmin"), (2, "victor@x.cl", "Victor Betancur", "ejecutivo"),
          (3, "matias@x.cl", "Matias Rio", "ejecutivo"), (4, "lenin@x.cl", "Lenin Urbina", "tecnico")]:
    mysql_execute("INSERT INTO app_users (id,username,nombre,role) VALUES (%s,%s,%s,%s)", u)
mysql_execute("INSERT INTO mant_clientes (id,razon_social,rut,contacto_nombre,contacto_tel,contacto_email) VALUES "
              "(1,'Gimnasio Uno SpA','76123456-7','Ana Pérez','+56912345678','ana@uno.cl'), (2,'Otro Gym Ltda','77999888-1','Pedro','','')")
mysql_execute("INSERT INTO mant_visitas VALUES (10,1,'OT-2026-00010','Mantención trotadora','en_curso','2026-10-30','Lenin Urbina'),"
              "(11,1,'OT-2026-00011','Instalación','cerrada','2026-09-01','Dave'),"
              "(12,2,'OT-2026-00012','Revisión bici','programada','2026-11-02','Jaizer'),"
              "(13,1,'OT-2026-00013','Anulada','anulada','2026-08-01','')")

NOTIFS, CORREOS, LOGS_MANT = [], [], []
PERFILES = {"super": ({"superadmin": True, "admin": True, "tk_ver": True, "mantenciones": True}, "daniel@x.cl", False),
            "victor": ({"tk_es_ejecutivo": True, "mantenciones": True}, "victor@x.cl", False),
            "lenin": ({"tk_es_tecnico": True}, "lenin@x.cl", True)}

app = Flask(__name__, template_folder=os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "templates"))
app.secret_key = "x"


@app.before_request
def _fake_login():
    perm, user, tec = PERFILES[request.headers.get("X-Perfil", "super")]
    g.permissions = dict(perm)
    g._usr = user
    g._tec = tec


@app.route("/")
def index():
    return "ok"


ctx = {
    "mysql_fetchone": mysql_fetchone, "mysql_fetchall": mysql_fetchall, "mysql_execute": mysql_execute,
    "get_mysql": _con,   # el módulo cierra lo que recibe: conexión nueva cada vez
    "login_required": (lambda f: f),
    "current_username": lambda: getattr(g, "_usr", None),
    "_mant_notificar": lambda uid, tipo, titulo, **k: NOTIFS.append((uid, tipo, titulo)),
    "_send_ilus_email": lambda to, subj, html, **k: CORREOS.append((to, k.get("correo"))) or True,
    "normalizar_rut": lambda r: (r or "").replace(".", "").upper(),   # como app.py: sin puntos, con guion
    "_no_tecnico_externo": (lambda f: f),
    "_es_rol_tecnico": lambda: bool(getattr(g, "_tec", False)),
    "_mant_log": lambda *a, **k: LOGS_MANT.append(a),
}
import tickets_module as tm  # noqa: E402


class _SinHilo:
    def __init__(self, *a, **k): pass
    def start(self): pass


tm.register_tickets_routes(app, ctx)
# Solo dentro de tickets_module (no el threading de todo Python: subprocess y otros
# usan hilos reales). Lock real; Thread apagado.
import threading as _th_real, types as _types
tm.threading = _types.SimpleNamespace(Thread=_SinHilo, Lock=_th_real.Lock)

ahora = datetime.utcnow().replace(microsecond=0)
f = lambda d: d.strftime("%Y-%m-%d %H:%M:%S")  # noqa: E731
mysql_execute("INSERT INTO tk_tickets (id,numero_ticket,origen,estado,prioridad,titulo,descripcion,rut,empresa,nombre_contacto,phone,email,asignado_a,created_at,updated_at) VALUES "
              "(1,'TK-2026-000001','form','open','media','Trotadora no enciende','No prende','76123456-7','Gimnasio Uno SpA','Ana Pérez','+56912345678','ana@uno.cl',NULL,%s,%s),"
              "(2,'TK-2026-000002','backoffice','in_progress','alta','Bici con ruido','Ruido','76123456-7','Gimnasio Uno SpA','Ana Pérez','','ana@uno.cl','Matias Rio',%s,%s),"
              "(3,'TK-2026-000003','backoffice','closed','baja','Viejo','x','76123456-7','Gimnasio Uno SpA','Ana','','','Victor Betancur',%s,%s),"
              "(4,'TK-2026-000004','backoffice','open','media','De otro cliente','x','77999888-1','Otro Gym Ltda','Pedro','','','',%s,%s)",
              (f(ahora - timedelta(hours=3)), f(ahora), f(ahora - timedelta(days=2)), f(ahora), f(ahora - timedelta(days=30)), f(ahora), f(ahora), f(ahora)))
mysql_execute("INSERT INTO tk_mensajes (ticket_id,tipo,contenido,es_interno,usuario,created_at) VALUES "
              "(1,'client_message','<p>Hola, no prende</p>',0,NULL,%s), (2,'mensaje','<p>Respondido</p>',0,'Matias Rio',%s)",
              (f(ahora - timedelta(hours=3)), f(ahora - timedelta(hours=1))))
mysql_execute("UPDATE tk_tickets SET visita_id=12 WHERE id=4")

cl = app.test_client()
H = {"Accept": "application/json", "Content-Type": "application/json"}


def get(url, perfil="super"):
    return cl.get(url, headers={**H, "X-Perfil": perfil})


def post(url, body=None, perfil="super", method="POST"):
    return cl.open(url, method=method, json=body or {}, headers={**H, "X-Perfil": perfil})


# ── tk_api_get: SLA de respuesta, guía, OT ───────────────────────────────────
def test_ficha_trae_sla_guia_y_ot():
    r = get("/tickets/api/tickets/1")
    assert r.status_code == 200, r.data[:300]
    d = r.get_json()
    t = d["ticket"]
    assert t["sla_resp"]["nivel"] in ("ok", "atencion", "vencido"), t["sla_resp"]   # el cliente espera (form sin respuesta)
    assert t["sla_resp"]["pill"]
    assert d["guia"]["siguiente"]["clave"] == "responsable" and "tomar" in d["guia"]["siguiente"]["acciones"]
    assert [p["clave"] for p in d["guia"]["pasos"]][:3] == ["cliente", "contactos", "problema"]
    assert d["contactos"] and d["cliente_ficha"]["id"] == 1
    assert t["visita_id"] is None and t["visita_estado_label"] == ""
    # ticket 4: con OT vinculada llega estado + fecha + técnico de la OT
    t4 = get("/tickets/api/tickets/4").get_json()["ticket"]
    assert t4["visita_numero_ot"] == "OT-2026-00012" and t4["visita_estado_label"] == "Programada"
    assert t4["visita_fecha"] == "02/11/2026" and t4["visita_tecnico"] == "Jaizer"


def test_ticket_respondido_no_espera_y_guia_pide_motivo():
    d = get("/tickets/api/tickets/2").get_json()
    assert d["ticket"]["sla_resp"]["nivel"] == "respondido"
    assert d["guia"]["siguiente"]["clave"] == "motivo"


def test_patch_motivo_y_diagnostico_mueven_la_guia():
    r = post("/tickets/api/tickets/2", {"motivo_principal": "Rodamiento de la bici gastado"}, method="PATCH")
    assert r.status_code == 200 and r.get_json()["ok"]
    d = get("/tickets/api/tickets/2").get_json()
    assert d["ticket"]["motivo_principal"] == "Rodamiento de la bici gastado"
    assert d["guia"]["siguiente"]["clave"] == "equipos"          # sin equipos: sugiere asociarlos
    mysql_execute("INSERT INTO tk_ticket_equipos (ticket_id, erp_kopr, nombre, cantidad) VALUES (2,'BIC-1','Bicicleta spinning',1)")
    d = get("/tickets/api/tickets/2").get_json()
    assert d["guia"]["siguiente"]["clave"] == "gestion"
    r = post("/tickets/api/tickets/2", {"diagnostico_solucion": "Se cambió el rodamiento y quedó operativa."}, method="PATCH")
    assert r.get_json()["ok"]
    d = get("/tickets/api/tickets/2").get_json()
    assert d["ticket"]["diagnostico_solucion"].startswith("Se cambió")
    assert [p for p in d["guia"]["pasos"] if p["clave"] == "diagnostico"][0]["estado"] == "hecho"


# ── Asociar OT existente ─────────────────────────────────────────────────────
def test_buscar_ot_lista_las_del_cliente_y_marca_vinculadas():
    r = get("/tickets/api/tickets/1/ot-buscar")
    assert r.status_code == 200
    ots = r.get_json()["ots"]
    nums = {o["numero_ot"] for o in ots}
    assert nums == {"OT-2026-00010", "OT-2026-00011", "OT-2026-00013"} and all(o["mismo_cliente"] for o in ots)
    assert [o for o in ots if o["numero_ot"] == "OT-2026-00010"][0]["estado_label"] == "En curso"
    r = get("/tickets/api/tickets/1/ot-buscar?q=00012")
    o = r.get_json()["ots"][0]
    assert o["numero_ot"] == "OT-2026-00012" and not o["mismo_cliente"] and o["otro_ticket"] == "TK-2026-000004"


def test_un_tecnico_no_busca_ni_asocia_ot():
    assert get("/tickets/api/tickets/1/ot-buscar", perfil="lenin").status_code == 403
    assert post("/tickets/api/tickets/1/asociar-ot", {"visita_id": 10}, perfil="lenin").status_code == 403


def test_asociar_ot_candados_y_exito():
    r = post("/tickets/api/tickets/1/asociar-ot", {"visita_id": 999})
    assert r.status_code == 404
    r = post("/tickets/api/tickets/1/asociar-ot", {"visita_id": 13})
    assert r.status_code == 409 and r.get_json()["error_codigo"] == "OT_ANULADA"
    r = post("/tickets/api/tickets/1/asociar-ot", {"visita_id": 12})
    assert r.status_code == 409 and r.get_json()["error_codigo"] == "OT_DE_OTRO_TICKET"
    # OT de OTRO cliente: avisa y pide confirmación con motivo
    mysql_execute("INSERT INTO mant_visitas VALUES (14,2,'OT-2026-00014','Otra','programada','2026-11-05','Dave')")
    r = post("/tickets/api/tickets/1/asociar-ot", {"visita_id": 14})
    assert r.status_code == 409 and r.get_json()["error_codigo"] == "CLIENTE_DISTINTO"
    r = post("/tickets/api/tickets/1/asociar-ot", {"visita_id": 14, "forzar": True, "motivo": "corto"})
    assert r.status_code == 409
    # Éxito con la OT del mismo cliente (sin motivo)
    r = post("/tickets/api/tickets/1/asociar-ot", {"visita_id": 10})
    assert r.status_code == 200 and r.get_json()["numero_ot"] == "OT-2026-00010", r.data[:300]
    t = get("/tickets/api/tickets/1").get_json()
    assert t["ticket"]["visita_id"] == 10 and t["ticket"]["estado"] == "open"     # el estado NO cambia solo (REGLA #32)
    assert any("OT-2026-00010 asociada" in (m.get("contenido") or "") for m in t["mensajes"])
    assert LOGS_MANT and LOGS_MANT[-1][:3] == ("visita", 10, "asociada_a_ticket")
    # ya tiene OT → 409
    r = post("/tickets/api/tickets/1/asociar-ot", {"visita_id": 11})
    assert r.status_code == 409 and r.get_json()["error_codigo"] == "YA_TIENE_OT"
    # la guía ahora dice «seguir la OT»
    g_ = t["guia"]["siguiente"]
    # (sin responsable sigue pidiendo hacerse cargo primero)
    assert g_["clave"] == "responsable"
    post("/tickets/api/tickets/1", {"asignado_a": "Daniel Aguilar"}, method="PATCH")
    mysql_execute("UPDATE tk_tickets SET espera_desde=NULL WHERE id=1")  # como si ya se respondió
    mysql_execute("DELETE FROM tk_mensajes WHERE ticket_id=1 AND tipo='client_message'")
    mysql_execute("UPDATE tk_tickets SET origen='backoffice', motivo_principal='Banda' WHERE id=1")
    mysql_execute("INSERT INTO tk_ticket_equipos (ticket_id, erp_kopr, nombre, cantidad) VALUES (1,'TRO-1','Trotadora',1)")
    d = get("/tickets/api/tickets/1").get_json()
    assert d["guia"]["siguiente"]["clave"] == "gestion" and d["guia"]["siguiente"]["tono"] == "espera"
    assert "OT-2026-00010" in d["guia"]["siguiente"]["titulo"]


def test_quitar_vinculo_solo_admin_con_motivo():
    assert post("/tickets/api/tickets/1/asociar-ot", {"motivo": "me equivoqué de OT"}, perfil="victor", method="DELETE").status_code == 403
    r = post("/tickets/api/tickets/1/asociar-ot", {"motivo": "corto"}, method="DELETE")
    assert r.status_code == 400
    r = post("/tickets/api/tickets/1/asociar-ot", {"motivo": "me equivoqué de OT"}, method="DELETE")
    assert r.status_code == 200 and r.get_json()["numero_ot"] == "OT-2026-00010"
    assert get("/tickets/api/tickets/1").get_json()["ticket"]["visita_id"] is None
    assert post("/tickets/api/tickets/1/asociar-ot", {"motivo": "otra vez, pero ya no tiene"}, method="DELETE").status_code == 409


# ── Tickets del cliente (ficha del cliente) ──────────────────────────────────
def test_tickets_del_cliente_paginados_y_con_visibilidad():
    r = get("/tickets/api/clientes-ficha/1/tickets?per_page=5")
    assert r.status_code == 200, r.data[:300]
    d = r.get_json()
    assert d["total"] == 3 and d["abiertos"] == 2 and d["total_pages"] == 1
    assert [t["numero"] for t in d["tickets"]] == ["TK-2026-000003", "TK-2026-000002", "TK-2026-000001"]
    t2 = [t for t in d["tickets"] if t["id"] == 2][0]
    assert t2["motivo_principal"] == "Rodamiento de la bici gastado" and t2["sla_resp"]["nivel"] == "respondido"
    # Página 2 de 5 (per_page mínimo 5): con 3 tickets, se acota a la última página
    d2 = get("/tickets/api/clientes-ficha/1/tickets?per_page=5&page=9").get_json()
    assert d2["page"] == 1
    # Victor no ve todo: solo los suyos y los sin asignar (el de Matías queda fuera)
    dv = get("/tickets/api/clientes-ficha/1/tickets", perfil="victor").get_json()
    esperado = {r["id"] for r in mysql_fetchall("SELECT id FROM tk_tickets WHERE rut_norm LIKE '76123456%%' AND "
                                                "(asignado_a IS NULL OR asignado_a='' OR asignado_a='Victor Betancur')")}
    assert {t["id"] for t in dv["tickets"]} == esperado and 2 not in esperado
    # Un técnico sin acceso a Servicio Técnico: 403
    assert get("/tickets/api/clientes-ficha/1/tickets", perfil="lenin").status_code == 403
    assert get("/tickets/api/clientes-ficha/999/tickets").status_code == 404
    mysql_execute("INSERT INTO mant_clientes (id,razon_social,rut) VALUES (3,'Sin RUT','')")
    assert get("/tickets/api/clientes-ficha/3/tickets").get_json()["sin_rut"] is True


# ── Dictado por voz: la configuración que Google acepta ──────────────────────
def test_transcribir_manda_tasa_de_muestreo_y_cae_al_modelo_por_defecto():
    import tickets_module as tm_  # noqa: F401

    class _Cfg(dict):
        def __init__(self, **k): super().__init__(**k)

    class _Audio:
        def __init__(self, content): self.content = content

    class _Res:
        def __init__(self, txt):
            alt = types.SimpleNamespace(transcript=txt)
            self.results = [types.SimpleNamespace(alternatives=[alt])]

    intentos = []

    class _Cli:
        def recognize(self, config, audio):
            intentos.append(dict(config))
            if config.get("model") == "latest_long":
                raise RuntimeError("400 Model latest_long is not supported for es-CL")
            return _Res("hola desde la prueba")

    fake = types.SimpleNamespace()
    fake.RecognitionConfig = _Cfg
    fake.RecognitionConfig.AudioEncoding = types.SimpleNamespace(WEBM_OPUS="WEBM_OPUS")
    fake.RecognitionAudio = _Audio
    fake.SpeechClient = _Cli
    google = types.ModuleType("google"); cloud = types.ModuleType("google.cloud")
    sys.modules["google"] = google; sys.modules["google.cloud"] = cloud; sys.modules["google.cloud.speech"] = fake
    cloud.speech = fake; google.cloud = cloud
    try:
        r = cl.post("/tickets/api/transcribir", data={"audio": (__import__("io").BytesIO(b"x" * 2000), "dictado.webm")},
                    headers={"X-Perfil": "super"}, content_type="multipart/form-data")
        assert r.status_code == 200, r.data[:300]
        assert r.get_json() == {"ok": True, "texto": "hola desde la prueba"}
        assert len(intentos) == 2 and intentos[0]["model"] == "latest_long" and "model" not in intentos[1]
        assert all(c["sample_rate_hertz"] == 48000 and c["encoding"] == "WEBM_OPUS" for c in intentos)
        # Sin audio: 400 claro
        assert cl.post("/tickets/api/transcribir", data={}, headers={"X-Perfil": "super"},
                       content_type="multipart/form-data").status_code == 400
    finally:
        for k in ("google.cloud.speech", "google.cloud", "google"):
            sys.modules.pop(k, None)


# ── Plantillas: se compilan y la ficha del cliente trae la pestaña Tickets ───
def test_plantillas_compilan():
    from jinja2 import Environment, FileSystemLoader
    base = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "templates")
    env = Environment(loader=FileSystemLoader(base))
    for name in ("tickets/ficha.html", "mantenciones/ficha.html"):
        env.parse(open(os.path.join(base, name), encoding="utf-8").read())
    src = open(os.path.join(base, "tickets/ficha.html"), encoding="utf-8").read()
    for marca in ('id="tkGuia"', 'id="pasoMotivo"', 'id="pasoDiagnostico"', 'id="tkAsocOtModal"', 'id="tkCotModal"',
                  'id="btnTomarTicket"', 'id="hRutRow"', 'id="tileSla"', 'id="cardActividad"'):
        assert marca in src, marca
    src2 = open(os.path.join(base, "mantenciones/ficha.html"), encoding="utf-8").read()
    assert 'id="tab-tickets"' in src2 and "cargarTicketsFicha" in src2


def test_firma_del_latido_cambia_con_cotizacion_u_ot():
    firma = ctx["_tk_firma_ticket"] if "_tk_firma_ticket" in ctx else None
    v1 = get("/tickets/api/tickets/2").get_json()["ver"]
    mysql_execute("INSERT INTO tk_cotizaciones (numero_cotizacion, estado, ticket_id, rut, empresa, created_at) VALUES ('COT-000001','draft',2,'76123456-7','Gimnasio Uno SpA',NOW())")
    v2 = get("/tickets/api/tickets/2").get_json()["ver"]
    assert v1 != v2, (v1, v2, firma)

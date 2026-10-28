"""Tickets 2.0 · prueba de carga de las RUTAS REALES contra un MySQL 8 de prueba.

Levanta tickets_module.register_tickets_routes en una app Flask mínima (corre
las migraciones reales), siembra N tickets / M mensajes DE MENTIRA y mide:
bandeja, semáforo del SLA, orden por espera, búsqueda, ficha, latidos,
responsables, visibilidad «míos + sin asignar» y los recordatorios del SLA.
Verifica que los resultados sean CORRECTOS (no solo rápidos).

Uso:  python tests/bench_tickets_rutas_mysql.py [tickets] [mensajes] [puerto]
Requiere un MySQL de PRUEBA sin clave en 127.0.0.1 (nunca producción: REGLA #22).
"""
import os
import random
import sys
import time
from datetime import datetime, timedelta

import pymysql
from flask import Flask, g, request

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

N_T = int(sys.argv[1]) if len(sys.argv) > 1 else 100000
N_M = int(sys.argv[2]) if len(sys.argv) > 2 else 1000000
PORT = int(sys.argv[3]) if len(sys.argv) > 3 else 3399
DB = "tk_rutas"


def _con(db=DB):
    return pymysql.connect(host="127.0.0.1", port=PORT, user="root", password="", database=db,
                           autocommit=True, charset="utf8mb4", cursorclass=pymysql.cursors.DictCursor)


c0 = pymysql.connect(host="127.0.0.1", port=PORT, user="root", password="", autocommit=True)
c0.cursor().execute(f"DROP DATABASE IF EXISTS {DB}")
c0.cursor().execute(f"CREATE DATABASE {DB} CHARACTER SET utf8mb4")
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


# Tablas de otros módulos que Tickets lee (mínimas, de mentira).
for ddl in (
    "CREATE TABLE app_users (id INT PRIMARY KEY, username VARCHAR(190), nombre VARCHAR(190), role VARCHAR(40), active TINYINT DEFAULT 1)",
    "CREATE TABLE rol_permisos (rol_slug VARCHAR(40), modulo VARCHAR(40), accion VARCHAR(60), permitido TINYINT)",
    "CREATE TABLE usuario_permisos (user_id INT, permiso VARCHAR(80), permitido TINYINT)",
    "CREATE TABLE mant_clientes (id INT PRIMARY KEY, razon_social VARCHAR(200), rut VARCHAR(20), contacto_nombre VARCHAR(200), "
    " contacto_cargo VARCHAR(100), contacto_tel VARCHAR(40), contacto_email VARCHAR(150), contacto2_nombre VARCHAR(200), "
    " contacto2_cargo VARCHAR(100), contacto2_tel VARCHAR(40), contacto2_email VARCHAR(150), INDEX(rut))",
    "CREATE TABLE mant_sucursales (id INT PRIMARY KEY, cliente_id INT, nombre VARCHAR(100), encargado_nombre VARCHAR(100), "
    " encargado_cargo VARCHAR(100), encargado_tel VARCHAR(40), encargado_email VARCHAR(150), activo TINYINT DEFAULT 1)",
    "CREATE TABLE mant_visitas (id INT PRIMARY KEY, numero_ot VARCHAR(30))",
    "CREATE TABLE mant_repuestos_compras (id INT PRIMARY KEY, ticket_id INT)",
):
    mysql_execute(ddl)
USERS = [(1, "daniel@x.cl", "Daniel Aguilar", "superadmin"), (2, "victor@x.cl", "Victor Betancur", "ejecutivo"),
         (3, "matias@x.cl", "Matias Rio", "ejecutivo"), (4, "alison@x.cl", "Alison Perez", "ejecutivo")]
for u in USERS:
    mysql_execute("INSERT INTO app_users (id,username,nombre,role) VALUES (%s,%s,%s,%s)", u)
mysql_execute("INSERT INTO rol_permisos VALUES ('ejecutivo','tickets','es_ejecutivo',1)")

NOTIFS, CORREOS = [], []
PERFILES = {"super": ({"superadmin": True, "admin": True, "tk_ver": True}, "daniel@x.cl"),
            "victor": ({"tk_es_ejecutivo": True}, "victor@x.cl")}

app = Flask(__name__)
app.secret_key = "x"


@app.before_request
def _fake_login():
    perm, user = PERFILES[request.headers.get("X-Perfil", "super")]
    g.permissions = dict(perm)
    g._usr = user


@app.route("/")
def index():
    return "ok"


ctx = {
    "mysql_fetchone": mysql_fetchone, "mysql_fetchall": mysql_fetchall, "mysql_execute": mysql_execute,
    "get_mysql": _con, "login_required": (lambda f: f),
    "current_username": lambda: getattr(g, "_usr", None),
    "_mant_notificar": lambda uid, tipo, titulo, **k: NOTIFS.append((uid, tipo, titulo)),
    "_send_ilus_email": lambda to, subj, html, **k: CORREOS.append((to, k.get("correo"))) or True,
    "normalizar_rut": lambda r: (r or "").replace(".", "").upper(),
    "_no_tecnico_externo": (lambda f: f),
}
import tickets_module as tm
# El latido de la bandeja lanza el barrido de recordatorios en un hilo: en la
# prueba se apaga ese hilo para que los recordatorios se midan de forma exacta.
class _SinHilo:
    def __init__(self, *a, **k): pass
    def start(self): pass
_HiloReal = tm.threading.Thread
t = time.time()
tm.register_tickets_routes(app, ctx)
tm.threading.Thread = _SinHilo
print(f"migraciones reales del módulo: {time.time() - t:.1f}s", flush=True)
cols = {r["COLUMN_NAME"] for r in mysql_fetchall(
    "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=%s AND TABLE_NAME='tk_tickets'", (DB,))}
faltan = {"espera_desde", "ultima_resp_at", "sin_leer", "ord_estado", "ord_prio", "ultimo_cliente_at"} - cols
assert not faltan, f"faltan columnas: {faltan}"

# ── Siembra ──────────────────────────────────────────────────────────────
rnd = random.Random(3)
ahora = datetime.utcnow().replace(microsecond=0)
f = lambda d: d.strftime("%Y-%m-%d %H:%M:%S")
EST = ["closed"] * 55 + ["resolved"] * 15 + ["open"] * 15 + ["in_progress"] * 10 + ["pending"] * 5
NOMBRES = ["Victor Betancur", "Matias Rio", "Alison Perez", None, None]
t = time.time()
for ini in range(0, N_T, 10000):
    filas = []
    for i in range(ini + 1, min(ini + 10000, N_T) + 1):
        cr = ahora - timedelta(minutes=rnd.randint(0, 600 * 24 * 60))
        filas.append((i, f"TK-2026-{i:06d}", rnd.choice(["form", "backoffice", "erp"]), rnd.choice(EST),
                      rnd.choice(["media"] * 6 + ["alta", "baja", "urgente"]), f"Problema {i}", "detalle " * 20,
                      f"7{i % 900000:06d}-K", f"Empresa {i % 4000}", f"Contacto {i}", f"+569{rnd.randint(10000000, 99999999)}",
                      rnd.choice(NOMBRES), f(cr + timedelta(hours=rnd.randint(0, 900))) if rnd.random() < .7 else None,
                      f(cr), f(cr + timedelta(hours=rnd.randint(0, 900)))))
    with _CONN.cursor() as cur:
        cur.executemany("INSERT INTO tk_tickets (id,numero_ticket,origen,estado,prioridad,titulo,descripcion,rut,empresa,"
                        "nombre_contacto,phone,asignado_a,staff_last_read_at,created_at,updated_at) VALUES "
                        "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", filas)
TIPOS = [("client_message", 1)] * 40 + [("mensaje", 0)] * 30 + [("comentario", 1)] * 15 + [("cambio_estado", 1)] * 15
for ini in range(0, N_M, 20000):
    filas = []
    for _ in range(min(20000, N_M - ini)):
        tp, interno = rnd.choice(TIPOS)
        filas.append((rnd.randint(1, N_T), tp, "<p>Hola, " + "texto " * 30 + "</p>", interno, "u",
                      f(ahora - timedelta(minutes=rnd.randint(0, 600 * 24 * 60)))))
    with _CONN.cursor() as cur:
        cur.executemany("INSERT INTO tk_mensajes (ticket_id,tipo,contenido,es_interno,usuario,created_at) "
                        "VALUES (%s,%s,%s,%s,%s,%s)", filas)
mysql_execute("ANALYZE TABLE tk_tickets, tk_mensajes")
print(f"siembra: {N_T} tickets / {N_M} mensajes en {time.time() - t:.0f}s", flush=True)

cl = app.test_client()
H = {"Accept": "application/json"}


def get(url, perfil="super", veces=3):
    mejor, r = None, None
    for _ in range(veces):
        s = time.time()
        r = cl.get(url, headers={**H, "X-Perfil": perfil})
        d = time.time() - s
        mejor = d if mejor is None else min(mejor, d)
    return r, mejor


def linea(etq, ms, extra=""):
    print(f"  {etq:<58}{ms * 1000:9.0f} ms  {extra}", flush=True)


print("\n== Primera carga (relleno completo de columnas y SLA, una sola vez)")
r, d = get("/tickets/api/tickets?limit=50", veces=1)
assert r.status_code == 200, r.data[:300]
linea("bandeja + conciliación inicial", d)

print("\n== Bandeja (rutas reales, 100.000 tickets)")
r, d = get("/tickets/api/tickets?limit=50"); j = r.get_json(); linea("página 1 (orden por actualizado)", d, f"total={j['total']}")
k = j["kpis"]
print(f"     KPI: rojo={k['sla_rojo']} ámbar={k['sla_ambar']} esperando={k['sla_esperando']} sin_asignar={k['sin_asignar']}")
for etq, url in (("orden inteligente (sin leer primero)", "/tickets/api/tickets?limit=50&sort="),
                 ("semáforo rojo", "/tickets/api/tickets?limit=50&sla=rojo"),
                 ("semáforo ámbar", "/tickets/api/tickets?limit=50&sla=ambar"),
                 ("orden por espera (Tiempo)", "/tickets/api/tickets?limit=50&sort=espera_desde&dir=asc"),
                 ("filtro estado=open", "/tickets/api/tickets?limit=50&estado=open"),
                 ("página 200 (offset 9.950)", "/tickets/api/tickets?limit=50&page=200"),
                 ("búsqueda de texto q=Empresa 12", "/tickets/api/tickets?limit=50&q=Empresa%2012"),
                 ("fecha_desde/hasta (rango)", f"/tickets/api/tickets?limit=50&fecha_desde={(ahora - timedelta(days=30)).date()}&fecha_hasta={ahora.date()}"),
                 ("latido de la bandeja", "/tickets/api/bandeja/latido"),
                 ("no leídos", "/tickets/api/unread-summary"),
                 ("responsables", "/tickets/api/responsables")):
    r, d = get(url)
    assert r.status_code == 200, (url, r.status_code, r.data[:300])
    j = r.get_json()
    linea(etq, d, f"total={j.get('total', '')}" if isinstance(j, dict) and "total" in j else "")
    if "sla=rojo" in url:
        assert j["total"] == k["sla_rojo"], "el filtro rojo debe dar el mismo número que su tarjeta"
        assert all(x["sla_resp"]["nivel"] == "vencido" for x in j["tickets"]), "todas las filas rojas deben estar vencidas"

print("\n== Correctitud del SLA (corte indexado vs evaluar ticket por ticket)")
import tickets_sla as s
c_venc, _ = s.cortes_utc(datetime.utcnow().replace(second=0, microsecond=0))
vivos = mysql_fetchall("SELECT espera_desde FROM tk_tickets WHERE espera_desde IS NOT NULL "
                       "AND estado NOT IN ('resolved','closed','cancelado') LIMIT 3000")
ahora_cl = s.utc_a_chile(datetime.utcnow())
dif = sum(1 for v in vivos if (s.evaluar(s.utc_a_chile(v["espera_desde"]), ahora_cl, "open")["nivel"] == "vencido")
          != (v["espera_desde"] <= c_venc) and abs((v["espera_desde"] - c_venc).total_seconds()) > 60)
print(f"  {len(vivos)} tickets esperando revisados · diferencias: {dif}")
assert dif == 0

print("\n== Conciliación (mensajes escritos por OTRO camino, como app.py)")
tid_x = 77
mysql_execute("UPDATE tk_tickets SET estado='open' WHERE id=%s", (tid_x,))
mysql_execute("INSERT INTO tk_mensajes (ticket_id,tipo,contenido,es_interno,created_at) VALUES (%s,'mensaje','resp',0,%s)", (tid_x, f(ahora - timedelta(hours=1))))
mysql_execute("INSERT INTO tk_mensajes (ticket_id,tipo,contenido,es_interno,created_at) VALUES (%s,'client_message','otra',1,%s)", (tid_x, f(ahora - timedelta(minutes=20))))
time.sleep(31)
r, _ = get("/tickets/api/tickets?limit=5", veces=1)
row = mysql_fetchone("SELECT espera_desde, ultima_resp_at FROM tk_tickets WHERE id=%s", (tid_x,))
print(f"  ticket {tid_x}: espera_desde={row['espera_desde']} ultima_resp_at={row['ultima_resp_at']}")
assert row["espera_desde"] == ahora - timedelta(minutes=20) and row["ultima_resp_at"] == ahora - timedelta(hours=1)
mal = mysql_fetchone(
    "SELECT COUNT(*) n FROM tk_tickets t WHERE NOT (t.espera_desde <=> ( "
    " SELECT MIN(m.created_at) FROM tk_mensajes m WHERE m.ticket_id=t.id AND m.tipo='client_message' "
    "  AND m.created_at > COALESCE((SELECT MAX(m2.created_at) FROM tk_mensajes m2 WHERE m2.ticket_id=t.id "
    "   AND m2.tipo='mensaje' AND m2.es_interno=0 AND COALESCE(m2.estado_envio,'')<>'fallido'),'1970-01-01'))) "
    " AND NOT (t.origen='form' AND t.ultima_resp_at IS NULL)")
print(f"  tickets cuyo espera_desde no calza con el cálculo desde cero: {mal['n']}")
assert mal["n"] == 0

print("\n== Ficha")
r, d = get("/tickets/api/tickets/500"); assert r.status_code == 200; linea("ficha completa (ticket 500)", d, f"{len(r.get_json()['mensajes'])} mensajes")
r, d = get("/tickets/api/tickets/500/latido"); assert r.status_code == 200; linea("latido de la ficha", d)

print("\n== Visibilidad «míos + sin asignar» (Victor, sin «Ver todos»)")
r, d = get("/tickets/api/tickets?limit=50", perfil="victor"); j = r.get_json()
esperado = mysql_fetchone("SELECT COUNT(*) n FROM tk_tickets WHERE asignado_a IS NULL OR asignado_a='' "
                          "OR asignado_a IN ('Victor Betancur','victor@x.cl')")["n"]
linea("bandeja de Victor", d, f"total={j['total']} (esperado {esperado})")
assert j["total"] == esperado and all((x["asignado_a"] or "") in ("", "Victor Betancur") for x in j["tickets"])
ajeno = mysql_fetchone("SELECT id FROM tk_tickets WHERE asignado_a='Matias Rio' LIMIT 1")["id"]
for url in (f"/tickets/api/tickets/{ajeno}", f"/tickets/api/tickets/{ajeno}/latido"):
    rr = cl.get(url, headers={**H, "X-Perfil": "victor"})
    assert rr.status_code == 403, (url, rr.status_code)
print(f"  ticket ajeno {ajeno}: 403 en ficha y latido ✓")

print("\n== Recordatorios del SLA")
s.rm.proxima_apertura = lambda *a, **k: None          # simula horario hábil
barrido = ctx["_tk_sla_barrido"]
# Otras alertas del sistema (p.ej. «recepción de correos caída»: la prueba no tiene clave IMAP) no son del SLA.
NOTIFS[:] = [n for n in NOTIFS if n[1] == "tk_sla"]; CORREOS[:] = [c for c in CORREOS if (c[1] or "").startswith("tk_sla")]
r1 = barrido(forzar=True); print(f"  1.ª pasada (siembra silenciosa): {r1} · avisos={len(NOTIFS)} correos={len(CORREOS)}")
assert "sembrado" in r1 and not NOTIFS and not CORREOS
mysql_execute("UPDATE tk_tickets SET estado='open', asignado_a='Victor Betancur', espera_desde=%s WHERE id=901",
              (ahora - timedelta(days=3),))
r2 = barrido(forzar=True); print(f"  2.ª pasada: {r2} · avisos={NOTIFS[-1:]} correos={CORREOS[-1:]}")
assert r2.get("avisos") == 1 and NOTIFS[-1][0] == 2 and NOTIFS[-1][1] == "tk_sla" and CORREOS[-1][1] == "tk_sla_vencido"
r3 = barrido(forzar=True); print(f"  3.ª pasada (no repite): {r3}")
assert r3.get("avisos") == 0

print("\nTODO OK")

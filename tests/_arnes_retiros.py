# -*- coding: utf-8 -*-
"""Arnés de pruebas LOCAL para las rutas de Retiros (pickups_module.register_pickup_routes).

Sin BD real, sin Check, sin ERP, sin correo: todo es de mentira y todo queda registrado para poder
afirmar «esto NO se llamó».

    app, db, ctx, esp = construir_app()

  db    BDFalsa: despacha por patrón de SQL (no es un MySQL: solo conoce las consultas de la guía de
        6 pasos y de la preparación por Check; cualquier otro SELECT devuelve None/[] y queda en
        db.sin_manejar). Guarda TODO lo que se escribe en db.escrituras.
  ctx   dict de app.py de mentira: llaves faltantes → MagicMock (así las ~50 dependencias que el
        módulo pide al registrarse no rompen), pero las que importan son espías explícitos:
          esp.correo      _send_ilus_email
          esp.whatsapp    _send_whatsapp
          esp.mant_notif  _mant_notificar  (campana interna)
          esp.check       _checkwms_get    (con respuestas programables: esp.check.respuestas)
  esp   EspiasFalsos

Este archivo empieza con «_» a propósito: pytest no lo recoge como prueba, solo lo importa
tests/test_retiros_guia_backend.py.
"""
import os
import re
import sys
import tempfile
import threading
import time
from unittest.mock import MagicMock

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if RAIZ not in sys.path:
    sys.path.insert(0, RAIZ)

from flask import Flask, g  # noqa: E402

TABLAS = {
    "PICKUP_REQUESTS_TABLE": "pickup_requests",
    "PICKUP_PACKAGES_TABLE": "pickup_packages",
    "PICKUP_PROPOSALS_TABLE": "pickup_proposals",
    "PICKUP_LOGS_TABLE": "pickup_logs",
    "PICKUP_ATTACHMENTS_TABLE": "pickup_attachments",
    "PICKUP_SIGNATURES_TABLE": "pickup_signatures",
    "PICKUP_SETTINGS_TABLE": "pickup_settings",
    "PICKUP_TEMPLATES_TABLE": "pickup_templates",
}


def _n(sql):
    return re.sub(r"\s+", " ", sql or "").strip()


class BDFalsa:
    """MySQL de mentira para la guía de 6 pasos. Estado en listas/dicts de Python."""

    def __init__(self):
        self.solicitudes = {}    # id -> fila de pickup_requests
        self.docs = []           # filas de pickup_request_docs
        self.logs = []           # filas de pickup_logs
        self.picking = []        # filas de pickup_picking_items
        self.propuestas = []     # filas de pickup_proposals
        self.lineas_sel = []     # filas de pickup_doc_lineas
        self.escrituras = []     # [(sql_normalizado, params)] de todo lo que NO es SELECT
        self.consultas = []      # [(sql_normalizado, params)] de todo SELECT
        self.sin_manejar = []    # SELECT que el arnés no conoce
        self._id_log = 0
        self.falla_si = []       # [(regex, Excepcion)]: hace fallar consultas que coincidan
        self.falla_si_params = []   # [(regex, predicado(params)->bool, Excepcion)]: falla solo si el predicado da True
        self.admins = []            # [{'id': 11}, …]: quienes reciben la campana de avisos al equipo
        self.falla_escritura_si = []  # [(regex, Excepcion)]: hace fallar INSERT/UPDATE/DELETE que coincidan
        self.snapshots = {}         # request_id -> {'payload': str, 'huella': str}: registro de Check guardado en ILUS
        self.prep_tiempos = {}      # request_id -> fila de pickup_prep_tiempos (análisis de tiempos guardado como evidencia)
        self.prep_productos = []    # filas de pickup_prep_productos (minutos de picking por producto)
        self.snapshot_viejo = False  # True: la lectura «¿hay un cambio pedido por el cliente?» sigue viendo el mundo de hace un minuto
        self.plantillas = {}        # (estado, canal) -> {'asunto','cuerpo'}: plantillas de Retiros de comm_templates (la BD de plantillas)
        self.al_leer = []           # [(regex, funcion)]: tras la PRIMERA lectura que coincide, corre la función (alguien cambia algo justo después)

    # ── construcción de datos ─────────────────────────────────────────────
    def nueva_solicitud(self, rid=1, **kw):
        fila = {"id": rid, "code": f"RET-T{rid:05d}", "status": "solicitud_recibida",
                "customer_name": "Cliente de Prueba", "contact_email": "cliente.real@example.com",
                "public_token": f"tok{rid}", "confirmed_date": None, "proposed_date": None,
                "responsable_user_id": None, "responsable_nombre": None}
        fila.update(kw)
        self.solicitudes[rid] = fila
        return fila

    def agregar_doc(self, rid=1, tipo="BLV", numero="0000023732", snapshot=None, **kw):
        import json
        fila = {"id": len(self.docs) + 1, "request_id": rid, "document_type": tipo, "document_number": numero,
                "cliente_nombre": "Gerd Müller", "cliente_rut": "11.111.111-1", "con_saldo": 1, "motivo_otro_rut": None,
                "has_seleccion_lineas": 0,
                "erp_snapshot": json.dumps(snapshot if snapshot is not None else {"lineas": [
                    {"sku": "DISCO25", "descripcion_erp": "Set Discos 2,5 - 5 kg", "cantidad": 1},
                    {"sku": "MANC10", "descripcion_erp": "Mancuerna hexagonal 10 kg", "cantidad": 2},
                ]})}
        fila.update(kw)
        self.docs.append(fila)
        return fila

    def agregar_log(self, rid, action, new_status=None, notes="", actor="Alguien"):
        self._id_log += 1
        fila = {"id": self._id_log, "request_id": rid, "actor_type": "interno", "actor_name": actor,
                "action": action, "old_status": None, "new_status": new_status, "notes": notes}
        self.logs.append(fila)
        return fila

    def agregar_picking(self, rid, sku, cantidad=1, picked=0):
        fila = {"id": len(self.picking) + 1, "request_id": rid, "sku": sku, "descripcion": sku,
                "cantidad": cantidad, "picked": picked, "picked_by": None, "picked_at": None}
        self.picking.append(fila)
        return fila

    # ── utilidades de las pruebas ─────────────────────────────────────────
    def reiniciar_registro(self):
        self.escrituras.clear()
        self.consultas.clear()
        self.sin_manejar.clear()

    def logs_de(self, rid, action=None):
        return [x for x in self.logs if x["request_id"] == rid and (action is None or x["action"] == action)]

    def inserts_en_logs(self):
        return [e for e in self.escrituras if e[0].upper().startswith("INSERT INTO `PICKUP_LOGS`")
                or e[0].upper().startswith("INSERT INTO PICKUP_LOGS")]

    # ── las tres funciones que el módulo recibe por ctx ───────────────────
    def _por_id(self, params):
        return self.solicitudes.get(int(params[0]))

    def _ganchos(self, s):
        for par in list(self.al_leer):
            if re.search(par[0], s, re.I):
                self.al_leer.remove(par)
                par[1]()

    def fetchall(self, sql, params=()):
        res = self._fetchall(sql, params)
        self._ganchos(_n(sql))
        return res

    def _fetchall(self, sql, params=()):
        s = _n(sql)
        self.consultas.append((s, params))
        for patron, exc in self.falla_si:
            if re.search(patron, s, re.I):
                raise exc
        for patron, pred, exc in self.falla_si_params:
            if re.search(patron, s, re.I) and pred(params):
                raise exc
        low = s.lower()
        if low.startswith("select") and "from pickup_request_docs" in low and "where request_id=%s" in low:
            rid = int(params[0])
            filas = [dict(d) for d in self.docs if d["request_id"] == rid]
            # Las columnas que el SELECT pide (así una columna inexistente falla como en MySQL).
            m = re.match(r"select (.*?) from pickup_request_docs", s, re.I)
            cols = [c.strip() for c in m.group(1).split(",")]
            for c in cols:
                if filas and c not in filas[0]:
                    raise RuntimeError(f"Unknown column '{c}'")
            return [{c: f[c] for c in cols} for f in filas]
        if low.startswith("select") and "from pickup_doc_lineas" in low:
            return [dict(x) for x in self.lineas_sel if x["doc_id"] == int(params[0])]
        if low.startswith("select id, action, notes, actor_name from `pickup_logs`"):
            rid = int(params[0])
            filas = [x for x in self.logs if x["request_id"] == rid
                     and x["action"] in ("docs_confirmadas", "productos_confirmados")]
            return [dict(x) for x in sorted(filas, key=lambda x: -x["id"])][:20]
        if low.startswith("select id, sku, descripcion, cantidad, picked, picked_by, picked_at from pickup_picking_items"):
            rid = int(params[0])
            return [dict(x) for x in self.picking if x["request_id"] == rid]
        if low.startswith("select id from `app_users` where active=1"):
            return [dict(a) for a in self.admins]
        if low.startswith("select id, confirmed_date from `pickup_requests` where status='agenda_confirmada' and confirmed_date between"):
            import datetime as _dt
            desde, hasta = params[0], params[1]

            def _f(v):
                if isinstance(v, _dt.datetime):
                    return v.date()
                return v if isinstance(v, _dt.date) else _dt.date.fromisoformat(str(v)[:10])
            # NOT EXISTS (…): los que YA estuvieron en preparación alguna vez no son candidatos
            ya = {x["request_id"] for x in self.logs if x["action"] == "estado_actualizado" and x["new_status"] == "en_preparacion"}
            return [{"id": r["id"], "confirmed_date": r["confirmed_date"]}
                    for r in sorted(self.solicitudes.values(), key=lambda r: (str(r.get("confirmed_date")), r["id"]))
                    if r["status"] == "agenda_confirmada" and r.get("confirmed_date") and desde <= _f(r["confirmed_date"]) <= hasta
                    and r["id"] not in ya][:int(params[2])]
        if low.startswith("select id from `pickup_requests` where status='en_preparacion'"):
            return [{"id": r["id"]} for r in sorted(self.solicitudes.values(), key=lambda r: -r["id"])
                    if r["status"] == "en_preparacion"][:30]
        self.sin_manejar.append(s)
        return []

    def fetchone(self, sql, params=()):
        res = self._fetchone(sql, params)
        self._ganchos(_n(sql))
        return res

    def _fetchone(self, sql, params=()):
        s = _n(sql)
        low = s.lower()
        if low == "select * from `pickup_requests` where id=%s":
            self.consultas.append((s, params))
            fila = self._por_id(params)
            return dict(fila) if fila else None
        if low.startswith("select (select max(id) from `pickup_logs`"):
            self.consultas.append((s, params))
            rid = int(params[0])
            lis = [x["id"] for x in self.logs if x["request_id"] == rid and x["action"] == "picking_completo"]
            prep = [x["id"] for x in self.logs if x["request_id"] == rid and x["action"] == "estado_actualizado"
                    and x["new_status"] == "en_preparacion"]
            return {"listo": max(lis) if lis else None, "prep": max(prep) if prep else None}
        if low.startswith("select id from `pickup_proposals` where request_id=%s and status='pending'"):
            self.consultas.append((s, params))
            if self.snapshot_viejo:
                return None
            rid = int(params[0])
            for p in self.propuestas:
                if p["request_id"] == rid and p["status"] == "pending" and (p.get("proposed_by") or "").lower() == "cliente":
                    return {"id": p["id"]}
            return None
        if low.startswith("select huella, en_curso from pickup_prep_tiempos where request_id=%s") or                 low.startswith("select payload from pickup_prep_tiempos where request_id=%s"):
            self.consultas.append((s, params))
            f = self.prep_tiempos.get(int(params[0]))
            return dict(f) if f else None
        if low.startswith("select payload, huella from pickup_check_snapshots where request_id=%s"):
            self.consultas.append((s, params))
            return dict(self.snapshots[int(params[0])]) if int(params[0]) in self.snapshots else None
        m_est = re.match(r"^select id from `pickup_logs` where request_id=%s and action='estado_actualizado' and new_status='(\w+)'", low)
        if m_est:
            # «¿pasó alguna vez por ese estado?»: la evidencia de que un retiro «cerrado» sí se retiró (retirada) y el freno del envío
            # automático (en_preparacion)
            self.consultas.append((s, params))
            rid = int(params[0])
            for x in self.logs:
                if x["request_id"] == rid and x["action"] == "estado_actualizado" and x["new_status"] == m_est.group(1):
                    return {"id": x["id"]}
            return None
        if low.startswith("select id from `pickup_logs` where request_id=%s and action=%s and created_at >="):
            # «¿ya se avisó al equipo por esto?» (el tiempo no se simula: basta que exista el evento)
            self.consultas.append((s, params))
            for x in self.logs:
                if x["request_id"] == int(params[0]) and x["action"] == params[1]:
                    return {"id": x["id"]}
            return None
        if low.startswith("select otro_r.code as code from pickup_request_docs mia"):
            # factura o boleta que el retiro comparte con OTRO retiro activo
            self.consultas.append((s, params))
            rid = int(params[0])
            terminales = ("retirada", "cerrada", "rechazada", "fallida")
            for mia in self.docs:
                if mia["request_id"] != rid:
                    continue
                for otra in self.docs:
                    if otra["request_id"] != rid and otra["document_type"] == mia["document_type"] \
                            and otra["document_number"].lstrip("0") == mia["document_number"].lstrip("0") \
                            and self.solicitudes[otra["request_id"]]["status"] not in terminales:
                        return {"code": self.solicitudes[otra["request_id"]]["code"]}
            return None
        if low.startswith("select") and "from `pickup_requests` where public_token=%s" in low:
            # el seguimiento público y su /status buscan el retiro por el token del enlace
            self.consultas.append((s, params))
            for r in self.solicitudes.values():
                if r.get("public_token") == params[0]:
                    return dict(r)
            return None
        if low.startswith("select") and "from comm_templates where modulo='retiros'" in low:
            self.consultas.append((s, params))
            fila = self.plantillas.get((params[0], params[1]))
            return dict(fila) if fila else None
        if re.match(r"^select (?!\*).+ from `pickup_requests` where id=%s$", low):
            # «SELECT status FROM pickup_requests WHERE id=%s» (el estado se relee dentro del candado) y similares
            self.consultas.append((s, params))
            fila = self._por_id(params)
            return dict(fila) if fila else None
        if low.startswith("select responsable_user_id from `pickup_requests`") or \
                low.startswith("select status, customer_name, responsable_nombre from `pickup_requests`") or \
                low.startswith("select code, customer_name from `pickup_requests`") or \
                low.startswith("select public_token from `pickup_requests`"):
            self.consultas.append((s, params))
            fila = self._por_id(params)
            return dict(fila) if fila else None
        filas = self.fetchall(sql, params)  # registra la consulta y aplica falla_si
        return filas[0] if filas else None

    def execute(self, sql, params=()):
        s = _n(sql)
        low = s.lower()
        for patron, exc in self.falla_escritura_si:
            if re.search(patron, low):
                raise exc
        self.escrituras.append((s, params))
        if low.startswith("insert into pickup_check_snapshots"):
            rid, payload, huella = params
            self.snapshots[int(rid)] = {"payload": payload, "huella": huella}
            return 1
        if low.startswith("insert into pickup_prep_tiempos"):
            cols = [c.strip() for c in s[s.index("(") + 1:s.index(")")].split(",")]
            self.prep_tiempos[int(params[0])] = dict(zip(cols, params))
            return 1
        if low.startswith("delete from pickup_prep_productos where request_id=%s"):
            antes = len(self.prep_productos)
            self.prep_productos = [p for p in self.prep_productos if p["request_id"] != int(params[0])]
            return antes - len(self.prep_productos)
        if low.startswith("insert into pickup_prep_productos"):
            cols = [c.strip() for c in s[s.index("(") + 1:s.index(")")].split(",")]
            self.prep_productos.append(dict(zip(cols, params)))
            return 1
        if low.startswith("delete from pickup_check_snapshots"):
            return 1 if self.snapshots.pop(int(params[0]), None) else 0
        if low.startswith("insert into `pickup_logs`") or low.startswith("insert into pickup_logs"):
            (rid, actor_type, actor_name, action, old_status, new_status, notes, _ip, _ua) = params
            self._id_log += 1
            self.logs.append({"id": self._id_log, "request_id": rid, "actor_type": actor_type,
                              "actor_name": actor_name, "action": action, "old_status": old_status,
                              "new_status": new_status, "notes": notes})
            return 1
        if low.startswith("update `pickup_requests` set status='en_preparacion' where id=%s and status='agenda_confirmada'"):
            rid = int(params[0])
            fila = self.solicitudes.get(rid)
            if " not exists (select 1 from `pickup_proposals`" in low and any(
                    p["request_id"] == rid and p["status"] == "pending" and (p.get("proposed_by") or "").lower() == "cliente"
                    for p in self.propuestas):
                return 0            # el cliente pidió cambiar la fecha: el UPDATE no toca nada (aunque la lectura previa fuera vieja)
            if fila and fila.get("status") == "agenda_confirmada":
                fila["status"] = "en_preparacion"
                return 1
            return 0
        if low.startswith("update `pickup_requests` set status=%s, closed_at="):
            nuevo, _, rid = params
            fila = self.solicitudes.get(int(rid))
            if fila:
                fila["status"] = nuevo
                return 1
            return 0
        if low.startswith("update `pickup_requests` set status='retirada', closed_at=now() where id=%s and status in ('en_preparacion','agenda_confirmada')"):
            # «Check expidió» = retirado (modo activo): atómico, y no toca el retiro si el cliente pidió cambiar la fecha
            rid = int(params[0])
            fila = self.solicitudes.get(rid)
            if " not exists (select 1 from `pickup_proposals`" in low and any(
                    p["request_id"] == rid and p["status"] == "pending" and (p.get("proposed_by") or "").lower() == "cliente"
                    for p in self.propuestas):
                return 0
            if fila and fila.get("status") in ("en_preparacion", "agenda_confirmada"):
                fila["status"] = "retirada"
                fila["closed_at"] = "NOW()"
                return 1
            return 0
        if low.startswith("update pickup_picking_items set picked=1"):
            rid = int(params[0])
            n = 0
            for x in self.picking:
                if x["request_id"] == rid and not x["picked"]:
                    x["picked"], x["picked_by"] = 1, "Check WMS"
                    n += 1
            return n
        m_tot = re.match(r"^update `pickup_requests` set ((?:\w+=%s(?:, )?)+) where id=%s and \(((?:coalesce\(\w+,0\)=0(?: or )?)+)\)$", low)
        if m_tot:
            # Totales (peso / peso volumétrico / m³) que se completan SOLO si siguen en 0: devuelve las filas «encontradas», como MySQL
            cols = [c.split("=")[0] for c in m_tot.group(1).split(", ")]
            fila = self.solicitudes.get(int(params[-1]))
            if fila and any(not float(fila.get(c) or 0) for c in cols):
                for c, v in zip(cols, params[:-1]):
                    fila[c] = v
                return 1
            return 0
        if low.startswith("update `pickup_requests` set responsable_user_id"):
            uid, nombre, rid = params
            fila = self.solicitudes[int(rid)]
            if not fila.get("responsable_user_id"):
                fila["responsable_user_id"], fila["responsable_nombre"] = uid, nombre
                return 1
            return 0
        return 0


class Ctx(dict):
    """dict de app.py de mentira: una llave que no está devuelve un MagicMock (no rompe al registrar)."""

    def __missing__(self, clave):
        self[clave] = MagicMock(name=f"ctx[{clave}]")
        return self[clave]


class EspiasFalsos:
    """Todo lo que podría escribirle a alguien (cliente o equipo) o llamar a un sistema externo."""

    def __init__(self):
        self.correo = MagicMock(name="_send_ilus_email")
        self.whatsapp = MagicMock(name="_send_whatsapp")
        self.mant_notif = MagicMock(name="_mant_notificar")
        self.check = EspiaCheck()


class EspiaCheck:
    """_checkwms_get de mentira. `respuestas`: {numdoc|None: respuesta} (None = Check no responde).
    Si un numdoc no está, usa respuestas['*']; si tampoco, devuelve None (sin respuesta)."""

    def __init__(self):
        self.llamadas = []      # [(path, params)]
        self.respuestas = {}
        self.excepcion = None   # si se asigna, _checkwms_get lanza esto

    def __call__(self, path, params, timeout=60):
        self.llamadas.append((path, dict(params)))
        if self.excepcion:
            raise self.excepcion
        if params.get("numdoc") in self.respuestas:
            return self.respuestas[params["numdoc"]]
        return self.respuestas.get("*")

    @property
    def rutas(self):
        return {p for p, _ in self.llamadas}


def respuesta_check(*filas):
    """Forma real de Check: {'estado': {...}, 'body': {'response': [fila, ...]}} con todo como texto."""
    return {"estado": {"codigo": 200}, "body": {"response": list(filas)}}


def fila_check(**kw):
    base = {"tipoDocumento": "BLV", "numeroDocumento": "23732", "solicitado": "0", "noAsignado": "0",
            "asignado": "0", "pickeado": "0", "revisado": "0", "despachado": "0", "cancelado": "0",
            "devuelto": "0"}
    base.update({k: str(v) for k, v in kw.items()})
    return base


def construir_app(usuario=None, extra=None):
    """Registra las rutas REALES de Retiros sobre un Flask de mentira. Devuelve (app, db, ctx, esp).
    `extra`: llaves de ctx que se fijan ANTES de registrar (el módulo toma varias al registrarse, p. ej. `_canal_activo`)."""
    import pickups_module

    db = BDFalsa()
    esp = EspiasFalsos()
    base = tempfile.mkdtemp(prefix="retiros_arnes_")
    app = Flask(__name__, template_folder=os.path.join(RAIZ, "templates"))
    app.config["TESTING"] = True
    app.secret_key = "arnes"

    ctx = Ctx(TABLAS)
    ctx.update({
        "mysql_fetchone": db.fetchone, "mysql_fetchall": db.fetchall, "mysql_execute": db.execute,
        "mysql_execute_returning_rowcount": lambda sql, params=(): int(db.execute(sql, params) or 0),
        "get_db": MagicMock(name="get_db"), "get_mysql": MagicMock(name="get_mysql"),
        "require_permission": lambda *a, **k: (lambda f: f),     # transparente: el permiso no se prueba aquí
        "EMAIL_RE": re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$"),
        "BASE_DIR": base, "g": g,
        "_send_ilus_email": esp.correo, "_send_whatsapp": esp.whatsapp,
        "_mant_notificar": esp.mant_notif, "_checkwms_get": esp.check,
        "_ilus_email_html": MagicMock(return_value="<html></html>"),
        "_get_wa_cfg": MagicMock(return_value={}),
    })
    # Estas dos el módulo las lee con ctx.get(): que sean None (como en un entorno de pruebas) y no un MagicMock
    for k in ("_random_sql_query", "_random_sql_pool", "erp_engine", "_comm_render_email_document",
              "permission_set", "rate_limited", "_cubicador_fetch", "_rut_cuerpo"):
        ctx.setdefault(k, None)
    ctx["rate_limited"] = None
    ctx.update(extra or {})

    usuario = usuario if usuario is not None else {"id": 7, "nombre": "Samantha Blacio", "username": "sam@sphs.cl"}

    @app.before_request
    def _fijar_usuario():
        g.user = dict(usuario) if usuario else None

    pickups_module.register_pickup_routes(app, ctx)
    db.reiniciar_registro()
    return app, db, ctx, esp


def esperar_hilos_de_aviso(timeout=5.0):
    """El aviso interno al equipo corre en un thread daemon: espera a que terminen los del módulo."""
    t0 = time.time()
    while time.time() - t0 < timeout:
        vivos = [t for t in threading.enumerate() if t.name.startswith("pickup-team-notify")]
        if not vivos:
            return
        for t in vivos:
            t.join(0.2)

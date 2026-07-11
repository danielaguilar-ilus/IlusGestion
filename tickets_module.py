"""
Modulo TICKETS CENTRAL — Fase 1 (CRUD interno).

Replica en NUESTRO codigo (Flask + MySQL) el sistema de tickets de
ilus-back / ilus-front, como un modulo propio y central (prefijo `tk_`),
SIN tocar los tickets de Mantenciones (mant_tickets*, Regla #4.2).

Se registra desde app.py con:
    from tickets_module import register_tickets_routes
    register_tickets_routes(app, globals())

El modulo saca todas sus dependencias del ctx (= globals() de app.py):
helpers MySQL, decoradores, uploader GCS, validador de RUT, etc. Asi no
duplica logica ni credenciales (Regla #4).

Ver BLUEPRINT-TICKETS-CENTRAL.md para el diseno completo (fases 2..7:
formulario publico, correo bidireccional, pestana Acciones, cotizaciones,
documentos ERP, migracion desde mant_tickets).
"""
import json
import time
from functools import wraps
from datetime import datetime, timezone, date

try:
    from zoneinfo import ZoneInfo
    _CL_TZ = ZoneInfo("America/Santiago")
except Exception:  # pragma: no cover
    _CL_TZ = None

from flask import request, jsonify, render_template, redirect, url_for, g


def _chile_now_year():
    """Ano actual en hora Chile (para numerar TK-YYYY-NNNNN correctamente aun
    en la ventana de fin de ano en que UTC ya cambio de anio pero Chile no)."""
    try:
        if _CL_TZ is not None:
            return datetime.now(timezone.utc).astimezone(_CL_TZ).year
    except Exception:
        pass
    return datetime.utcnow().year


# ─────────────────────────────────────────────────────────────────────────
#  Enums (fuente unica — tomados del modelo ilus-back ticket_details_*_enum)
# ─────────────────────────────────────────────────────────────────────────
TK_ESTADOS = (
    "open", "in_progress", "pending", "resolved", "closed",
    "ot_pending_approval", "ot_generated", "ot_in_progress", "cancelado",
)
TK_TIPOS = (
    "install", "tech_support", "shipping", "quotation", "return",
    "tech_evaluation", "maintenance", "spare_parts", "equipment_transfer",
    "warranty", "repair", "spare_parts_store", "spare_parts_import",
)
TK_PRIORIDADES = ("baja", "media", "alta", "urgente")
TK_ORIGENES = ("form", "backoffice", "erp")

ESTADO_LABEL = {
    "open": "Abierto", "in_progress": "En Curso", "pending": "Pendiente",
    "resolved": "Resuelto", "closed": "Cerrado",
    "ot_pending_approval": "OT Pendiente de Aprobacion",
    "ot_generated": "OT Generada", "ot_in_progress": "OT En Curso",
    "cancelado": "Cancelado",
}
TIPO_LABEL = {
    "install": "Instalacion", "tech_support": "Soporte Tecnico",
    "shipping": "Despacho", "quotation": "Cotizacion", "return": "Devolucion",
    "tech_evaluation": "Evaluacion Tecnica", "maintenance": "Mantenimiento",
    "spare_parts": "Repuestos", "equipment_transfer": "Movimiento de Equipos",
    "warranty": "Garantia", "repair": "Reparacion",
    "spare_parts_store": "Repuestos bodega", "spare_parts_import": "Repuestos importacion",
}
# Los 8 tipos expuestos al publico / mas usados en el backoffice.
TK_TIPOS_PUBLICOS = (
    "install", "tech_support", "maintenance", "warranty",
    "spare_parts", "quotation", "shipping", "return",
)

# Orden de negocio para el listado (bandeja): abiertos primero.
_ESTADO_ORDER = {e: i for i, e in enumerate(
    ["open", "in_progress", "pending", "ot_pending_approval",
     "ot_generated", "ot_in_progress", "resolved", "closed", "cancelado"]
)}
_PRIO_ORDER = {"urgente": 0, "alta": 1, "media": 2, "baja": 3}

# ── Mapas de migracion desde los tickets de Mantenciones (mant_tickets*) ──
# Blueprint §7. Se usan al centralizar; conservan el dato sin romper el origen.
_MANT_ESTADO_MAP = {
    "abierto": "open", "en_proceso": "in_progress",
    "esperando_cliente": "pending", "esperando_repuesto": "pending",
    "resuelto": "resolved", "cerrado": "closed", "cancelado": "cancelado",
}
_MANT_TIPO_MAP = {
    "cambio": "equipment_transfer", "garantia": "warranty", "falla": "repair",
    "consulta": "tech_support", "cotizacion": "quotation",
    "presupuesto": "quotation", "seguimiento": "tech_support", "otro": "tech_support",
}
_MANT_BITACORA_TIPO_MAP = {
    "comentario": "comentario", "cambio_estado": "cambio_estado",
    "asignacion": "asignacion", "archivo": "archivo",
    "email_enviado": "mensaje", "whatsapp_enviado": "otro",
    "creacion": "creacion", "cierre": "cierre", "reapertura": "reapertura",
    "otro": "otro",
}


def register_tickets_routes(app, ctx):
    # ── Dependencias inyectadas desde app.py (globals) ──
    mysql_fetchone = ctx["mysql_fetchone"]
    mysql_fetchall = ctx["mysql_fetchall"]
    mysql_execute = ctx["mysql_execute"]
    get_mysql = ctx["get_mysql"]
    login_required = ctx["login_required"]
    current_username = ctx.get("current_username") or (lambda: None)
    _uploader_upload = ctx.get("_uploader_upload")
    _uploader_destroy = ctx.get("_uploader_destroy")
    validar_rut = ctx.get("validar_rut")
    _audit = ctx.get("_audit") or (lambda *a, **k: None)
    chile_fmt = ctx.get("chile_fmt")
    # ERP read-only: reusamos los helpers PROBADOS de app.py (pymssql directo a
    # SQL Server sobre MAEEN/MAEDDO/MAEEDO). NO la REST API (que no responde en prod).
    _erp_buscar_clientes = ctx.get("_erp_buscar_clientes")
    _random_sql_query = ctx.get("_random_sql_query")
    _rut_cuerpo = ctx.get("_rut_cuerpo")
    _RANDOM_TIDOS_VENTA = ctx.get("_RANDOM_TIDOS_VENTA") or (
        "FCV", "BLV", "NVI", "NVV", "GDV", "GDP", "GTR", "GRD", "FCO", "COV")

    def _fmt_dt(value, only_date=False):
        """Formatea un datetime/date de MySQL (UTC naive) a hora Chile como
        string listo para la UI (Regla #6). Usa el chile_fmt del proyecto si
        esta disponible; si no, cae a un formateo local con zoneinfo."""
        if value is None:
            return None
        if isinstance(value, str):
            return value
        # DATE puro (fecha_limite): sin conversion de zona
        if isinstance(value, date) and not isinstance(value, datetime):
            return value.strftime("%d/%m/%Y")
        if chile_fmt is not None:
            try:
                return chile_fmt(value, "%d/%m/%Y %H:%M") if only_date is False else chile_fmt(value, "%d/%m/%Y")
            except Exception:
                pass
        try:
            aware = value.replace(tzinfo=timezone.utc)
            if _CL_TZ is not None:
                aware = aware.astimezone(_CL_TZ)
            return aware.strftime("%d/%m/%Y" if only_date else "%d/%m/%Y %H:%M")
        except Exception:
            return str(value)

    def _fmt_row(row, dt_keys=("created_at", "updated_at", "cerrado_at",
                              "message_date", "staff_last_read_at"),
                 date_keys=("fecha_limite", "fecha")):
        """Devuelve un dict con los campos de fecha convertidos a hora Chile."""
        d = dict(row)
        for k in dt_keys:
            if k in d:
                d[k] = _fmt_dt(d[k])
        for k in date_keys:
            if k in d:
                d[k] = _fmt_dt(d[k], only_date=True)
        return d

    # ─────────────────────────────────────────────────────────────────
    #  Migracion idempotente (patron _ensure_*). Corre al registrar el
    #  modulo, dentro de app_context, para funcionar aun con
    #  ILUS_SKIP_MIGRATIONS=1 en prod. Todos los CREATE llevan IF NOT EXISTS.
    # ─────────────────────────────────────────────────────────────────
    def _ensure_tickets_tables():
        mysql_execute("""
            CREATE TABLE IF NOT EXISTS tk_tickets (
              id                INT AUTO_INCREMENT PRIMARY KEY,
              numero_ticket     VARCHAR(30) NULL UNIQUE,
              origen            ENUM('form','backoffice','erp') NOT NULL DEFAULT 'backoffice',
              estado            ENUM('open','in_progress','pending','resolved','closed',
                                     'ot_pending_approval','ot_generated','ot_in_progress',
                                     'cancelado') NOT NULL DEFAULT 'open',
              tipo              ENUM('install','tech_support','shipping','quotation','return',
                                     'tech_evaluation','maintenance','spare_parts','equipment_transfer',
                                     'warranty','repair','spare_parts_store','spare_parts_import') NULL,
              prioridad         ENUM('baja','media','alta','urgente') NOT NULL DEFAULT 'media',
              titulo            VARCHAR(300) NULL,
              descripcion       TEXT NULL,
              rut               VARCHAR(12)  NULL,
              empresa           VARCHAR(150) NULL,
              sucursal          VARCHAR(100) NULL,
              nombre_contacto   VARCHAR(150) NULL,
              email             VARCHAR(150) NULL,
              phone             VARCHAR(20)  NULL,
              direccion         VARCHAR(255) NULL,
              direccion_lat     DECIMAL(10,7) NULL,
              direccion_lng     DECIMAL(10,7) NULL,
              direccion_place_id VARCHAR(200) NULL,
              region_nombre     VARCHAR(120) NULL,
              comuna_nombre     VARCHAR(120) NULL,
              producto          TEXT NULL,
              marca             VARCHAR(100) NULL,
              sku               VARCHAR(100) NULL,
              numero_documento  TEXT NULL,
              erp_idmaeen       INT NULL,
              erp_koen          VARCHAR(50) NULL,
              asignado_a        VARCHAR(190) NULL,
              tecnico_id        INT NULL,
              visita_id         INT NULL,
              mant_ticket_id    INT NULL,
              staff_last_read_at DATETIME NULL,
              fecha_limite      DATE NULL,
              notas_internas    TEXT NULL,
              created_by        VARCHAR(190) NULL,
              created_at        DATETIME DEFAULT CURRENT_TIMESTAMP,
              updated_at        DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
              cerrado_at        DATETIME NULL,
              cerrado_por       VARCHAR(190) NULL,
              KEY idx_estado          (estado),
              KEY idx_tipo            (tipo),
              KEY idx_origen          (origen),
              KEY idx_prioridad       (prioridad),
              KEY idx_asignado        (asignado_a),
              KEY idx_created         (created_at),
              KEY idx_estado_updated  (estado, updated_at),
              KEY idx_erp_idmaeen     (erp_idmaeen),
              KEY idx_rut             (rut),
              KEY idx_mant_ticket     (mant_ticket_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        mysql_execute("""
            CREATE TABLE IF NOT EXISTS tk_mensajes (
              id             INT AUTO_INCREMENT PRIMARY KEY,
              ticket_id      INT NOT NULL,
              tipo           ENUM('comentario','mensaje','client_message','cambio_estado',
                                  'asignacion','creacion','cierre','reapertura','archivo','otro')
                                  NOT NULL DEFAULT 'comentario',
              contenido      MEDIUMTEXT NULL,
              metadata       TEXT NULL,
              mail_message_id VARCHAR(150) NULL,
              es_interno     TINYINT(1) NOT NULL DEFAULT 1,
              usuario        VARCHAR(190) NULL,
              message_date   DATETIME NULL,
              created_at     DATETIME DEFAULT CURRENT_TIMESTAMP,
              KEY idx_ticket   (ticket_id, created_at),
              KEY idx_unread   (ticket_id, tipo, created_at),
              CONSTRAINT fk_tkmsg_ticket FOREIGN KEY (ticket_id)
                 REFERENCES tk_tickets(id) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        mysql_execute("""
            CREATE TABLE IF NOT EXISTS tk_adjuntos (
              id             INT AUTO_INCREMENT PRIMARY KEY,
              ticket_id      INT NOT NULL,
              mensaje_id     INT NULL,
              archivo_url    VARCHAR(500) NOT NULL,
              archivo_path   VARCHAR(500) NULL,
              archivo_nombre VARCHAR(300) NULL,
              mime_type      VARCHAR(150) NULL,
              file_size_kb   INT NULL,
              origen         ENUM('form','backoffice','cliente') NOT NULL DEFAULT 'backoffice',
              subido_por     VARCHAR(190) NULL,
              created_at     DATETIME DEFAULT CURRENT_TIMESTAMP,
              KEY idx_ticket (ticket_id),
              CONSTRAINT fk_tkadj_ticket FOREIGN KEY (ticket_id)
                 REFERENCES tk_tickets(id) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        mysql_execute("""
            CREATE TABLE IF NOT EXISTS tk_ticket_equipos (
              id           INT AUTO_INCREMENT PRIMARY KEY,
              ticket_id    INT NOT NULL,
              erp_kopr     VARCHAR(100) NULL,
              nombre       VARCHAR(300) NULL,
              tipo         VARCHAR(100) NULL,
              sku          VARCHAR(100) NULL,
              serie        VARCHAR(120) NULL,
              cantidad     INT NOT NULL DEFAULT 1,
              maquina_id   INT NULL,
              notas        VARCHAR(500) NULL,
              created_at   DATETIME DEFAULT CURRENT_TIMESTAMP,
              UNIQUE KEY uq_ticket_kopr (ticket_id, erp_kopr),
              KEY idx_ticket (ticket_id),
              CONSTRAINT fk_tkeq_ticket FOREIGN KEY (ticket_id)
                 REFERENCES tk_tickets(id) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        # Esqueleto del modulo Cotizaciones (Blueprint §2.8-2.9). Tablas
        # creadas ahora; la logica (armar items, PDF, enviar) se construye
        # de a poco en fases siguientes -- Daniel pidio el modulo vacio
        # primero para no bloquear Tickets con un scope enorme de una vez.
        mysql_execute("""
            CREATE TABLE IF NOT EXISTS tk_cotizaciones (
              id                INT AUTO_INCREMENT PRIMARY KEY,
              numero_cotizacion VARCHAR(30) NULL UNIQUE,
              ticket_id         INT NULL,
              estado            ENUM('draft','sent','approved','rejected','expired')
                                    NOT NULL DEFAULT 'draft',
              erp_idmaeen       INT NULL,
              erp_koen          VARCHAR(50) NULL,
              rut               VARCHAR(12) NULL,
              empresa           VARCHAR(150) NULL,
              costo_tecnico     INT NOT NULL DEFAULT 0,
              costo_ruta        INT NOT NULL DEFAULT 0,
              subtotal_items    INT NOT NULL DEFAULT 0,
              subtotal          INT NOT NULL DEFAULT 0,
              descuento_pct     DECIMAL(5,2) NOT NULL DEFAULT 0,
              descuento_monto   INT NOT NULL DEFAULT 0,
              iva_pct           DECIMAL(5,2) NOT NULL DEFAULT 19,
              iva_monto         INT NOT NULL DEFAULT 0,
              total             INT NOT NULL DEFAULT 0,
              valida_hasta      DATE NULL,
              notas             TEXT NULL,
              created_by        VARCHAR(190) NULL,
              created_at        DATETIME DEFAULT CURRENT_TIMESTAMP,
              updated_at        DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
              KEY idx_ticket (ticket_id),
              KEY idx_estado (estado),
              CONSTRAINT fk_tkcot_ticket FOREIGN KEY (ticket_id)
                 REFERENCES tk_tickets(id) ON DELETE SET NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        mysql_execute("""
            CREATE TABLE IF NOT EXISTS tk_cotizacion_items (
              id             INT AUTO_INCREMENT PRIMARY KEY,
              cotizacion_id  INT NOT NULL,
              item_tipo      ENUM('producto','servicio','ruta','otro') NOT NULL DEFAULT 'producto',
              erp_kopr       VARCHAR(100) NULL,
              descripcion    VARCHAR(300) NULL,
              cantidad       INT NOT NULL DEFAULT 1,
              precio_unitario INT NOT NULL DEFAULT 0,
              subtotal       INT NOT NULL DEFAULT 0,
              descuento_pct  DECIMAL(5,2) NOT NULL DEFAULT 0,
              total          INT NOT NULL DEFAULT 0,
              desde_ticket   TINYINT(1) NOT NULL DEFAULT 0,
              notas          TEXT NULL,
              KEY idx_cotizacion (cotizacion_id),
              CONSTRAINT fk_tkcotit_cot FOREIGN KEY (cotizacion_id)
                 REFERENCES tk_cotizaciones(id) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        mysql_execute("""
            CREATE TABLE IF NOT EXISTS tk_ticket_documentos (
              id            INT AUTO_INCREMENT PRIMARY KEY,
              ticket_id     INT NOT NULL,
              erp_tido      VARCHAR(10) NULL,
              erp_nudo      VARCHAR(40) NULL,
              erp_idmaeedo  INT NULL,
              fecha         DATE NULL,
              monto         INT NULL,
              created_at    DATETIME DEFAULT CURRENT_TIMESTAMP,
              UNIQUE KEY uq_ticket_doc (ticket_id, erp_tido, erp_nudo),
              KEY idx_ticket (ticket_id),
              CONSTRAINT fk_tkdoc_ticket FOREIGN KEY (ticket_id)
                 REFERENCES tk_tickets(id) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)

    with app.app_context():
        try:
            _ensure_tickets_tables()
            print("[ILUS] Tablas tk_* garantizadas (Tickets central).", flush=True)
        except Exception as _e:
            print(f"[ILUS][WARN] _ensure_tickets_tables: {_e}", flush=True)

    # ─────────────────────────────────────────────────────────────────
    #  Helpers internos
    # ─────────────────────────────────────────────────────────────────
    def _is_ajaxish():
        return (
            request.headers.get("X-Requested-With") == "XMLHttpRequest"
            or (request.headers.get("Accept") or "").startswith("application/json")
            or request.is_json
            or request.path.startswith("/tickets/api/")
        )

    def _tickets_required(view):
        """Gate de Fase 1: reutiliza el permiso 'mantenciones' (o superadmin)
        para no tocar la matriz de roles todavia. En una fase posterior se
        puede crear una permission key 'tickets' dedicada (ver blueprint §9)."""
        @wraps(view)
        def wrapped(*a, **k):
            perms = g.get("permissions") or {}
            if not (perms.get("mantenciones") or perms.get("superadmin")):
                if _is_ajaxish():
                    return jsonify({
                        "ok": False,
                        "error": "Tu usuario no tiene permiso para Tickets.",
                        "error_codigo": "SIN_PERMISO_TICKETS",
                    }), 403
                return redirect(url_for("index"))
            return view(*a, **k)
        return login_required(wrapped)

    def _tk_log(ticket_id, tipo, contenido, usuario=None, metadata=None, es_interno=True):
        """Escribe un evento/mensaje en tk_mensajes. Nunca rompe el flujo."""
        try:
            mysql_execute(
                "INSERT INTO tk_mensajes (ticket_id, tipo, contenido, metadata, usuario, es_interno) "
                "VALUES (%s,%s,%s,%s,%s,%s)",
                (ticket_id, tipo, contenido,
                 json.dumps(metadata, ensure_ascii=False) if metadata else None,
                 usuario or (current_username() or "sistema"),
                 1 if es_interno else 0),
            )
        except Exception as _e:
            print(f"[tk_log] error: {_e}", flush=True)

    def _norm_enum(value, allowed, default):
        v = (value or "").strip().lower()
        return v if v in allowed else default

    def _row(sql, params=None):
        return mysql_fetchone(sql, params)

    # ─────────────────────────────────────────────────────────────────
    #  PAGINAS (HTML)
    # ─────────────────────────────────────────────────────────────────
    @app.route("/tickets")
    @_tickets_required
    def tk_list():
        return render_template(
            "tickets/list.html",
            estado_label=ESTADO_LABEL, tipo_label=TIPO_LABEL,
            tk_tipos=TK_TIPOS, tk_estados=TK_ESTADOS, tk_prioridades=TK_PRIORIDADES,
        )

    @app.route("/tickets/nuevo")
    @_tickets_required
    def tk_nuevo():
        return render_template(
            "tickets/nuevo.html",
            tipo_label=TIPO_LABEL, tk_tipos_publicos=TK_TIPOS_PUBLICOS,
            tk_prioridades=TK_PRIORIDADES,
        )

    @app.route("/tickets/<int:tid>")
    @_tickets_required
    def tk_ficha(tid):
        t = _row("SELECT id FROM tk_tickets WHERE id=%s", (tid,))
        if not t:
            return redirect(url_for("tk_list"))
        return render_template(
            "tickets/ficha.html",
            ticket_id=tid,
            estado_label=ESTADO_LABEL, tipo_label=TIPO_LABEL,
            tk_estados=TK_ESTADOS, tk_tipos=TK_TIPOS, tk_prioridades=TK_PRIORIDADES,
        )

    # ─────────────────────────────────────────────────────────────────
    #  PAGINA — Cotizaciones (esqueleto, Fase 5 del blueprint)
    # ─────────────────────────────────────────────────────────────────
    @app.route("/tickets/cotizaciones")
    @_tickets_required
    def tk_cotizaciones_list():
        rows = mysql_fetchall(
            "SELECT id, numero_cotizacion, estado, empresa, rut, total, created_at "
            "FROM tk_cotizaciones ORDER BY created_at DESC LIMIT 100")
        return render_template("tickets/cotizaciones.html",
                                cotizaciones=[_fmt_row(r) for r in rows])

    # ─────────────────────────────────────────────────────────────────
    #  API — listado + KPIs
    # ─────────────────────────────────────────────────────────────────
    @app.route("/tickets/api/tickets", methods=["GET"])
    @_tickets_required
    def tk_api_list():
        where, params = [], []
        estado = (request.args.get("estado") or "").strip().lower()
        if estado in TK_ESTADOS:
            where.append("t.estado=%s"); params.append(estado)
        tipo = (request.args.get("tipo") or "").strip().lower()
        if tipo in TK_TIPOS:
            where.append("t.tipo=%s"); params.append(tipo)
        prio = (request.args.get("prioridad") or "").strip().lower()
        if prio in TK_PRIORIDADES:
            where.append("t.prioridad=%s"); params.append(prio)
        origen = (request.args.get("origen") or "").strip().lower()
        if origen in TK_ORIGENES:
            where.append("t.origen=%s"); params.append(origen)
        asign = (request.args.get("asignado_a") or "").strip()
        if asign:
            where.append("t.asignado_a=%s"); params.append(asign)
        rut = (request.args.get("rut") or "").strip()
        if rut:
            where.append("t.rut LIKE %s"); params.append(f"%{rut}%")
        q = (request.args.get("q") or "").strip()
        if q:
            like = f"%{q}%"
            where.append(
                "(t.numero_ticket LIKE %s OR t.empresa LIKE %s OR t.nombre_contacto LIKE %s "
                "OR t.descripcion LIKE %s OR t.titulo LIKE %s OR t.rut LIKE %s)"
            )
            params.extend([like, like, like, like, like, like])

        wsql = (" WHERE " + " AND ".join(where)) if where else ""

        try:
            page = max(1, int(request.args.get("page", 1)))
        except Exception:
            page = 1
        try:
            limit = min(200, max(5, int(request.args.get("limit", 50))))
        except Exception:
            limit = 50
        offset = (page - 1) * limit

        total_row = mysql_fetchone(f"SELECT COUNT(*) AS n FROM tk_tickets t{wsql}", tuple(params))
        total = int(total_row["n"]) if total_row else 0

        rows = mysql_fetchall(
            "SELECT t.id, t.numero_ticket, t.origen, t.estado, t.tipo, t.prioridad, "
            "       t.titulo, t.empresa, t.rut, t.nombre_contacto, t.asignado_a, "
            "       t.created_at, t.updated_at, t.fecha_limite, "
            "       (SELECT COUNT(*) FROM tk_mensajes m "
            "          WHERE m.ticket_id=t.id AND m.tipo='client_message' "
            "            AND m.created_at > COALESCE(t.staff_last_read_at,'1970-01-01')) AS unread_count "
            f"FROM tk_tickets t{wsql} "
            "ORDER BY FIELD(t.estado,'open','in_progress','pending','ot_pending_approval',"
            "'ot_generated','ot_in_progress','resolved','closed','cancelado'), "
            "FIELD(t.prioridad,'urgente','alta','media','baja'), t.updated_at DESC, t.id DESC "
            "LIMIT %s OFFSET %s",
            tuple(params) + (limit, offset),
        )

        kpi = mysql_fetchone(
            "SELECT "
            "  COUNT(*) AS total, "
            "  SUM(estado IN ('open','in_progress')) AS activos, "
            "  SUM(prioridad='urgente' AND estado NOT IN ('resolved','closed','cancelado')) AS urgentes, "
            "  SUM(fecha_limite IS NOT NULL AND fecha_limite < CURDATE() "
            "      AND estado NOT IN ('resolved','closed','cancelado')) AS vencidos "
            "FROM tk_tickets"
        ) or {}

        return jsonify({
            "ok": True,
            "tickets": [_fmt_row(r) for r in rows],
            "total": total, "page": page, "limit": limit,
            "kpis": {
                "total": int(kpi.get("total") or 0),
                "activos": int(kpi.get("activos") or 0),
                "urgentes": int(kpi.get("urgentes") or 0),
                "vencidos": int(kpi.get("vencidos") or 0),
            },
        })

    # ─────────────────────────────────────────────────────────────────
    #  API — crear (backoffice)
    # ─────────────────────────────────────────────────────────────────
    @app.route("/tickets/api/tickets", methods=["POST"])
    @_tickets_required
    def tk_api_create():
        d = request.get_json(silent=True) or {}
        descripcion = (d.get("descripcion") or "").strip()
        empresa = (d.get("empresa") or "").strip()
        if not descripcion and not empresa and not (d.get("titulo") or "").strip():
            return jsonify({"ok": False, "error": "Ingresa al menos empresa/cliente o una descripcion."}), 400

        tipo = _norm_enum(d.get("tipo"), TK_TIPOS, None)
        prio = _norm_enum(d.get("prioridad"), TK_PRIORIDADES, "media")
        user = current_username() or "sistema"

        rut = (d.get("rut") or "").strip()
        if rut and validar_rut:
            ok_rut, rut_norm = validar_rut(rut)
            if ok_rut:
                rut = rut_norm

        conn = get_mysql()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO tk_tickets "
                    "(origen, estado, tipo, prioridad, titulo, descripcion, rut, empresa, "
                    " sucursal, nombre_contacto, email, phone, direccion, direccion_lat, "
                    " direccion_lng, direccion_place_id, region_nombre, comuna_nombre, "
                    " producto, marca, sku, numero_documento, erp_idmaeen, erp_koen, "
                    " asignado_a, fecha_limite, notas_internas, created_by) "
                    "VALUES (%s,'open',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
                    "        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (
                        _norm_enum(d.get("origen"), TK_ORIGENES, "backoffice"),
                        tipo, prio,
                        (d.get("titulo") or "").strip()[:300] or None,
                        descripcion[:5000] or None,
                        rut[:12] or None, empresa[:150] or None,
                        (d.get("sucursal") or "").strip()[:100] or None,
                        (d.get("nombre_contacto") or "").strip()[:150] or None,
                        (d.get("email") or "").strip()[:150] or None,
                        (d.get("phone") or "").strip()[:20] or None,
                        (d.get("direccion") or "").strip()[:255] or None,
                        d.get("direccion_lat") or None,
                        d.get("direccion_lng") or None,
                        (d.get("direccion_place_id") or "").strip()[:200] or None,
                        (d.get("region_nombre") or "").strip()[:120] or None,
                        (d.get("comuna_nombre") or "").strip()[:120] or None,
                        (d.get("producto") or "").strip() or None,
                        (d.get("marca") or "").strip()[:100] or None,
                        (d.get("sku") or "").strip()[:100] or None,
                        (d.get("numero_documento") or "").strip() or None,
                        int(d["erp_idmaeen"]) if str(d.get("erp_idmaeen") or "").strip().isdigit() else None,
                        (d.get("erp_koen") or "").strip()[:50] or None,
                        (d.get("asignado_a") or "").strip()[:190] or user,
                        d.get("fecha_limite") or None,
                        (d.get("notas_internas") or "").strip()[:5000] or None,
                        user,
                    ),
                )
                tid = cur.lastrowid
                # Numeracion race-free: derivar del id autoincrement (atomico).
                # El anio se calcula en hora Chile (no UTC) para no numerar
                # TK-{anio+1} en la ventana de fin de anio (Regla #6).
                cur.execute(
                    "UPDATE tk_tickets SET numero_ticket = "
                    "CONCAT('TK-', %s, '-', LPAD(id,5,'0')) WHERE id=%s",
                    (_chile_now_year(), tid),
                )
                # Equipos que vengan del form (lista de dicts)
                for eq in (d.get("equipos") or []):
                    try:
                        cur.execute(
                            "INSERT IGNORE INTO tk_ticket_equipos "
                            "(ticket_id, erp_kopr, nombre, tipo, sku, cantidad) VALUES (%s,%s,%s,%s,%s,%s)",
                            (tid, (eq.get("kopr") or "").strip()[:100] or None,
                             (eq.get("nombre") or "").strip()[:300] or None,
                             (eq.get("tipo") or "").strip()[:100] or None,
                             (eq.get("sku") or "").strip()[:100] or None,
                             int(eq.get("cantidad") or 1)),
                        )
                    except Exception:
                        pass
            conn.commit()
        finally:
            conn.close()

        numero_row = mysql_fetchone("SELECT numero_ticket FROM tk_tickets WHERE id=%s", (tid,))
        numero = numero_row["numero_ticket"] if numero_row else None
        _tk_log(tid, "creacion",
                f"Ticket {numero} creado por {user}"
                + (f" — tipo: {TIPO_LABEL.get(tipo, tipo)}" if tipo else "")
                + f" · prioridad: {prio}")
        try:
            _audit("tk_ticket_create", target_type="tk_ticket", target_id=tid,
                   details={"numero": numero, "tipo": tipo})
        except Exception:
            pass
        return jsonify({"ok": True, "id": tid, "numero_ticket": numero})

    # ─────────────────────────────────────────────────────────────────
    #  API — detalle
    # ─────────────────────────────────────────────────────────────────
    @app.route("/tickets/api/tickets/<int:tid>", methods=["GET"])
    @_tickets_required
    def tk_api_get(tid):
        t = mysql_fetchone("SELECT * FROM tk_tickets WHERE id=%s", (tid,))
        if not t:
            return jsonify({"ok": False, "error": "Ticket no encontrado"}), 404
        equipos = mysql_fetchall(
            "SELECT id, erp_kopr, nombre, tipo, sku, serie, cantidad, maquina_id, notas "
            "FROM tk_ticket_equipos WHERE ticket_id=%s ORDER BY id", (tid,))
        documentos = mysql_fetchall(
            "SELECT id, erp_tido, erp_nudo, fecha, monto FROM tk_ticket_documentos "
            "WHERE ticket_id=%s ORDER BY id", (tid,))
        mensajes = mysql_fetchall(
            "SELECT id, tipo, contenido, metadata, usuario, es_interno, message_date, created_at "
            "FROM tk_mensajes WHERE ticket_id=%s ORDER BY created_at ASC, id ASC", (tid,))
        adjuntos = mysql_fetchall(
            "SELECT id, mensaje_id, archivo_url, archivo_nombre, mime_type, file_size_kb, origen, created_at "
            "FROM tk_adjuntos WHERE ticket_id=%s ORDER BY id", (tid,))
        return jsonify({
            "ok": True,
            "ticket": _fmt_row(t),
            "equipos": [dict(r) for r in equipos],
            "documentos": [_fmt_row(r) for r in documentos],
            "mensajes": [_fmt_row(r) for r in mensajes],
            "adjuntos": [_fmt_row(r) for r in adjuntos],
            "estado_label": ESTADO_LABEL, "tipo_label": TIPO_LABEL,
        })

    # ─────────────────────────────────────────────────────────────────
    #  API — actualizar
    # ─────────────────────────────────────────────────────────────────
    @app.route("/tickets/api/tickets/<int:tid>", methods=["PATCH"])
    @_tickets_required
    def tk_api_update(tid):
        prev = mysql_fetchone(
            "SELECT estado, prioridad, tipo, asignado_a FROM tk_tickets WHERE id=%s", (tid,))
        if not prev:
            return jsonify({"ok": False, "error": "Ticket no encontrado"}), 404

        d = request.get_json(silent=True) or {}
        allowed = (
            "titulo", "descripcion", "tipo", "prioridad", "estado", "sucursal",
            "nombre_contacto", "email", "phone", "direccion", "empresa", "rut",
            "asignado_a", "tecnico_id", "fecha_limite", "notas_internas",
            "producto", "marca", "sku",
        )
        sets, params = [], []
        for key in allowed:
            if key not in d:
                continue
            val = d[key]
            if key == "estado":
                val = _norm_enum(val, TK_ESTADOS, prev["estado"])
            elif key == "prioridad":
                val = _norm_enum(val, TK_PRIORIDADES, prev["prioridad"])
            elif key == "tipo":
                val = _norm_enum(val, TK_TIPOS, prev["tipo"]) if val else None
            elif isinstance(val, str):
                val = val.strip() or None
            sets.append(f"{key}=%s"); params.append(val)

        if not sets:
            return jsonify({"ok": False, "error": "Sin cambios validos"}), 400

        user = current_username() or "sistema"
        nuevo_estado = _norm_enum(d.get("estado"), TK_ESTADOS, None) if "estado" in d else None
        if nuevo_estado in ("resolved", "closed") and prev["estado"] not in ("resolved", "closed"):
            sets.append("cerrado_at=NOW()"); sets.append("cerrado_por=%s"); params.append(user)

        params.append(tid)
        mysql_execute(f"UPDATE tk_tickets SET {', '.join(sets)} WHERE id=%s", tuple(params))

        # Bitacora de cambios relevantes
        if nuevo_estado and nuevo_estado != prev["estado"]:
            _tk_log(tid, "cambio_estado",
                    f"Estado: {ESTADO_LABEL.get(prev['estado'], prev['estado'])} → "
                    f"{ESTADO_LABEL.get(nuevo_estado, nuevo_estado)}", usuario=user,
                    metadata={"campo": "estado", "antes": prev["estado"], "nuevo": nuevo_estado})
        if "asignado_a" in d and (d.get("asignado_a") or "") != (prev["asignado_a"] or ""):
            _tk_log(tid, "asignacion",
                    f"Asignado a: {d.get('asignado_a') or '(sin asignar)'}", usuario=user,
                    metadata={"campo": "asignado_a", "antes": prev["asignado_a"], "nuevo": d.get("asignado_a")})
        return jsonify({"ok": True})

    # ─────────────────────────────────────────────────────────────────
    #  API — eliminar (superadmin o creador; audit ANTES de borrar)
    # ─────────────────────────────────────────────────────────────────
    @app.route("/tickets/api/tickets/<int:tid>", methods=["DELETE"])
    @_tickets_required
    def tk_api_delete(tid):
        t = mysql_fetchone("SELECT numero_ticket, created_by FROM tk_tickets WHERE id=%s", (tid,))
        if not t:
            return jsonify({"ok": False, "error": "Ticket no encontrado"}), 404
        perms = g.get("permissions") or {}
        user = current_username() or ""
        if not (perms.get("superadmin") or (t.get("created_by") and t["created_by"] == user)):
            return jsonify({"ok": False, "error": "Solo el creador o un superadministrador puede eliminar."}), 403
        try:
            _audit("tk_ticket_delete", target_type="tk_ticket", target_id=tid,
                   details={"numero": t.get("numero_ticket")})
        except Exception:
            pass
        mysql_execute("DELETE FROM tk_tickets WHERE id=%s", (tid,))
        return jsonify({"ok": True})

    # ─────────────────────────────────────────────────────────────────
    #  API — comentario interno (conversacion Fase 1)
    # ─────────────────────────────────────────────────────────────────
    @app.route("/tickets/api/tickets/<int:tid>/comentario", methods=["POST"])
    @_tickets_required
    def tk_api_comentario(tid):
        if not mysql_fetchone("SELECT id FROM tk_tickets WHERE id=%s", (tid,)):
            return jsonify({"ok": False, "error": "Ticket no encontrado"}), 404
        d = request.get_json(silent=True) or {}
        contenido = (d.get("contenido") or "").strip()
        if not contenido:
            return jsonify({"ok": False, "error": "El comentario esta vacio"}), 400
        es_interno = bool(d.get("es_interno", True))
        _tk_log(tid, "comentario", contenido[:5000], es_interno=es_interno)
        mysql_execute("UPDATE tk_tickets SET updated_at=NOW() WHERE id=%s", (tid,))
        return jsonify({"ok": True})

    # ─────────────────────────────────────────────────────────────────
    #  API — marcar leido (sin subir el ticket en la bandeja)
    # ─────────────────────────────────────────────────────────────────
    @app.route("/tickets/api/tickets/<int:tid>/marcar-leido", methods=["PATCH"])
    @_tickets_required
    def tk_api_marcar_leido(tid):
        # updated_at = updated_at para NO disparar ON UPDATE CURRENT_TIMESTAMP.
        mysql_execute(
            "UPDATE tk_tickets SET staff_last_read_at=NOW(), updated_at=updated_at WHERE id=%s", (tid,))
        return jsonify({"ok": True})

    @app.route("/tickets/api/unread-summary", methods=["GET"])
    @_tickets_required
    def tk_api_unread_summary():
        row = mysql_fetchone(
            "SELECT COUNT(DISTINCT t.id) AS n FROM tk_tickets t "
            "JOIN tk_mensajes m ON m.ticket_id=t.id AND m.tipo='client_message' "
            "WHERE t.estado NOT IN ('resolved','closed','cancelado') "
            "  AND m.created_at > COALESCE(t.staff_last_read_at,'1970-01-01')")
        return jsonify({"ok": True, "unread": int(row["n"]) if row else 0})

    # ─────────────────────────────────────────────────────────────────
    #  API — equipos
    # ─────────────────────────────────────────────────────────────────
    @app.route("/tickets/api/tickets/<int:tid>/equipos", methods=["POST"])
    @_tickets_required
    def tk_api_add_equipo(tid):
        if not mysql_fetchone("SELECT id FROM tk_tickets WHERE id=%s", (tid,)):
            return jsonify({"ok": False, "error": "Ticket no encontrado"}), 404
        d = request.get_json(silent=True) or {}
        nombre = (d.get("nombre") or "").strip()
        kopr = (d.get("kopr") or "").strip()
        if not nombre and not kopr:
            return jsonify({"ok": False, "error": "Falta el producto"}), 400
        try:
            cant = max(1, int(d.get("cantidad") or 1))
        except Exception:
            cant = 1
        mysql_execute(
            "INSERT INTO tk_ticket_equipos (ticket_id, erp_kopr, nombre, tipo, sku, cantidad, notas) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE cantidad=VALUES(cantidad), notas=VALUES(notas)",
            (tid, kopr[:100] or None, nombre[:300] or None,
             (d.get("tipo") or "").strip()[:100] or None,
             (d.get("sku") or "").strip()[:100] or None,
             cant, (d.get("notas") or "").strip()[:500] or None))
        _tk_log(tid, "otro", f"Equipo agregado: {nombre or kopr}")
        return jsonify({"ok": True})

    @app.route("/tickets/api/tickets/<int:tid>/equipos/<int:eid>", methods=["DELETE"])
    @_tickets_required
    def tk_api_del_equipo(tid, eid):
        mysql_execute("DELETE FROM tk_ticket_equipos WHERE id=%s AND ticket_id=%s", (eid, tid))
        _tk_log(tid, "otro", f"Equipo #{eid} quitado")
        return jsonify({"ok": True})

    # ─────────────────────────────────────────────────────────────────
    #  API — adjuntos (subida a GCS via /f/<key>)
    # ─────────────────────────────────────────────────────────────────
    @app.route("/tickets/api/tickets/<int:tid>/adjuntos", methods=["POST"])
    @_tickets_required
    def tk_api_upload_adjunto(tid):
        if not mysql_fetchone("SELECT id FROM tk_tickets WHERE id=%s", (tid,)):
            return jsonify({"ok": False, "error": "Ticket no encontrado"}), 404
        if not _uploader_upload:
            return jsonify({"ok": False, "error": "Almacenamiento no disponible"}), 503
        f = request.files.get("file") or request.files.get("archivo")
        if not f or not f.filename:
            return jsonify({"ok": False, "error": "No llego ningun archivo"}), 400

        mime = (f.mimetype or "").lower()
        rt = "image"
        if mime.startswith("video"):
            rt = "video"
        elif not mime.startswith("image"):
            rt = "raw"
        try:
            res = _uploader_upload(f, folder="tickets", resource_type=rt)
        except Exception as _e:
            print(f"[tk_upload] error: {_e}", flush=True)
            return jsonify({"ok": False, "error": "No se pudo subir el archivo"}), 500
        url = res.get("secure_url") or res.get("url")
        if not url:
            return jsonify({"ok": False, "error": "Subida sin URL"}), 500

        # tamano
        size_kb = None
        try:
            if res.get("bytes"):
                size_kb = int(res["bytes"] // 1024)
        except Exception:
            pass

        conn = get_mysql()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO tk_adjuntos "
                    "(ticket_id, archivo_url, archivo_path, archivo_nombre, mime_type, file_size_kb, origen, subido_por) "
                    "VALUES (%s,%s,%s,%s,%s,%s,'backoffice',%s)",
                    (tid, url[:500], (res.get("public_id") or "")[:500] or None,
                     f.filename[:300], mime[:150] or None, size_kb,
                     current_username() or "sistema"))
                adj_id = cur.lastrowid
            conn.commit()
        except Exception as _e:
            # El archivo ya se subio a GCS; si el INSERT falla, borramos el
            # blob huerfano (si tenemos el helper) para no dejar basura.
            print(f"[tk_upload] INSERT fallo, limpiando blob: {_e}", flush=True)
            try:
                if _uploader_destroy and res.get("public_id"):
                    _uploader_destroy(res["public_id"])
            except Exception:
                pass
            return jsonify({"ok": False, "error": "No se pudo registrar el adjunto"}), 500
        finally:
            conn.close()
        _tk_log(tid, "archivo", f"Adjunto: {f.filename}")
        return jsonify({"ok": True, "id": adj_id, "url": url, "nombre": f.filename})

    # ─────────────────────────────────────────────────────────────────
    #  API — ERP: previsualizar CUALQUIER documento (no solo ventas) para
    #  detonar un ticket desde ahi. Reusa el motor unificado erp_engine
    #  (Regla: nunca duplicar logica ERP fuera del motor) -- el mismo que
    #  usan cubicador/asignar/mantenciones-stock. Read-only.
    # ─────────────────────────────────────────────────────────────────
    @app.route("/tickets/api/erp/documento/<tido>/<nudo>", methods=["GET"])
    @_tickets_required
    def tk_api_erp_documento(tido, nudo):
        try:
            import erp_engine
            doc = erp_engine.get_client().fetch_document((tido or "").strip(), (nudo or "").strip())
        except Exception as _e:
            print(f"[tk_erp_documento] error tido={tido} nudo={nudo}: {_e}", flush=True)
            return jsonify({"ok": False, "error": "ERP no disponible ahora"}), 200
        if not doc:
            return jsonify({"ok": False, "error": "Documento no encontrado en el ERP"}), 200

        lineas = []
        for ln in (doc.get("lineas_raw") or []):
            sku = str(ln.get("KOPRCT") or ln.get("koprct") or "").strip()
            nombre = str(ln.get("NOKOPR") or ln.get("nokopr") or "").strip()
            if not (sku or nombre):
                continue
            lineas.append({"sku": sku, "nombre": nombre,
                            "cantidad": ln.get("CAPRCO1") or ln.get("caprco1") or 1})

        return jsonify({"ok": True, "documento": {
            "tido": tido, "nudo": nudo,
            "fecha": str(doc.get("fecha") or "")[:10],
            "cliente_nombre": doc.get("cliente_nombre") or "",
            "cliente_rut": doc.get("cliente_rut") or "",
            "email": doc.get("email") or "", "telefono": doc.get("telefono") or "",
            "direccion": doc.get("direccion") or "", "comuna": doc.get("comuna") or "",
            "lineas": lineas,
        }})

    # ─────────────────────────────────────────────────────────────────
    #  API — crear TICKET a partir de uno o varios documentos ERP
    #  (cualquier tipo: factura, boleta, guia, orden, etc.)
    # ─────────────────────────────────────────────────────────────────
    @app.route("/tickets/api/tickets/desde-documento", methods=["POST"])
    @_tickets_required
    def tk_api_crear_desde_documento():
        d = request.get_json(silent=True) or {}
        docs = d.get("documentos") or []  # [{tido, nudo}]
        if not docs:
            return jsonify({"ok": False, "error": "Falta al menos un documento"}), 400
        try:
            import erp_engine
            engine = erp_engine.get_client()
        except Exception as _e:
            return jsonify({"ok": False, "error": f"ERP no disponible: {_e}"}), 200

        primero = None
        todas_lineas, docs_ok = [], []
        for item in docs[:10]:
            tido = str(item.get("tido") or "").strip()
            nudo = str(item.get("nudo") or "").strip()
            if not (tido and nudo):
                continue
            try:
                doc = engine.fetch_document(tido, nudo)
            except Exception as _e:
                print(f"[tk_desde_documento] error {tido}/{nudo}: {_e}", flush=True)
                doc = None
            if not doc:
                continue
            if primero is None:
                primero = doc
            docs_ok.append({"tido": tido, "nudo": nudo, "fecha": doc.get("fecha")})
            for ln in (doc.get("lineas_raw") or []):
                sku = str(ln.get("KOPRCT") or ln.get("koprct") or "").strip()
                nombre = str(ln.get("NOKOPR") or ln.get("nokopr") or "").strip()
                if sku or nombre:
                    todas_lineas.append({"sku": sku, "nombre": nombre,
                                          "cantidad": ln.get("CAPRCO1") or ln.get("caprco1") or 1})

        if primero is None:
            return jsonify({"ok": False, "error": "Ningún documento fue encontrado en el ERP"}), 200

        tipo = _norm_enum(d.get("tipo"), TK_TIPOS, "tech_support")
        prio = _norm_enum(d.get("prioridad"), TK_PRIORIDADES, "media")
        user = current_username() or "sistema"
        rut = (primero.get("cliente_rut") or "").strip()[:12] or None

        conn = get_mysql()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO tk_tickets "
                    "(origen, estado, tipo, prioridad, descripcion, rut, empresa, email, phone, "
                    " direccion, comuna_nombre, numero_documento, asignado_a, created_by) "
                    "VALUES ('erp','open',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (tipo, prio, (d.get("descripcion") or "").strip()[:5000] or None,
                     rut, (primero.get("cliente_nombre") or "")[:150] or None,
                     (primero.get("email") or "")[:150] or None,
                     (primero.get("telefono") or "")[:20] or None,
                     (primero.get("direccion") or "")[:255] or None,
                     (primero.get("comuna") or "")[:120] or None,
                     ", ".join(f"{x['tido']}-{x['nudo']}" for x in docs_ok)[:1000] or None,
                     user, user))
                tid = cur.lastrowid
                cur.execute(
                    "UPDATE tk_tickets SET numero_ticket = "
                    "CONCAT('TK-', %s, '-', LPAD(id,5,'0')) WHERE id=%s",
                    (_chile_now_year(), tid))
                for dc in docs_ok:
                    try:
                        cur.execute(
                            "INSERT IGNORE INTO tk_ticket_documentos (ticket_id, erp_tido, erp_nudo) "
                            "VALUES (%s,%s,%s)", (tid, dc["tido"][:10], dc["nudo"][:40]))
                    except Exception:
                        pass
                vistos = set()
                for ln in todas_lineas:
                    key = ln["sku"] or ln["nombre"]
                    if key in vistos:
                        continue
                    vistos.add(key)
                    try:
                        cur.execute(
                            "INSERT IGNORE INTO tk_ticket_equipos (ticket_id, erp_kopr, nombre, cantidad) "
                            "VALUES (%s,%s,%s,%s)",
                            (tid, ln["sku"][:100] or None, ln["nombre"][:300] or "Equipo",
                             int(ln.get("cantidad") or 1) if str(ln.get("cantidad") or 1).isdigit() else 1))
                    except Exception:
                        pass
            conn.commit()
        finally:
            conn.close()

        numero = mysql_fetchone("SELECT numero_ticket FROM tk_tickets WHERE id=%s", (tid,))
        numero = numero["numero_ticket"] if numero else None
        _tk_log(tid, "creacion", f"Ticket {numero} creado desde documento(s) ERP: "
                + ", ".join(f"{x['tido']}-{x['nudo']}" for x in docs_ok))
        return jsonify({"ok": True, "id": tid, "numero_ticket": numero})

    # ─────────────────────────────────────────────────────────────────
    #  API — ERP: buscar cliente (por RUT o razon social) — read-only
    #
    #  NO reusamos _erp_buscar_clientes tal cual: esa funcion filtra
    #  "AND TIEN IN ('C','A')" (pensado para clientes empresa de
    #  Mantenciones). Un RUT de PERSONA NATURAL (ej. el de Daniel) puede
    #  tener otro TIEN y ese filtro lo excluye en silencio (el query corre
    #  bien, devuelve 0 filas) -- exactamente el bug reportado: ilus-front
    #  SI lo encuentra porque no aplica ese filtro. Aca hacemos la MISMA
    #  query sobre MAEEN pero sin restringir por TIEN, y logueamos
    #  timing+filas para poder diagnosticar sin volver a re-desplegar.
    # ─────────────────────────────────────────────────────────────────
    @app.route("/tickets/api/erp/buscar-cliente", methods=["GET"])
    @_tickets_required
    def tk_api_erp_buscar_cliente():
        q = (request.args.get("q") or "").strip()
        if len(q) < 2:
            return jsonify({"ok": True, "resultados": []})
        if not (_random_sql_query and _rut_cuerpo):
            return jsonify({"ok": False, "error": "Motor ERP no disponible", "resultados": []}), 200

        q_upper = q.upper()
        q_like = f"%{q_upper}%"
        q_cuerpo = _rut_cuerpo(q)
        q_cuerpo_like = f"%{q_cuerpo}%" if (q_cuerpo and len(q_cuerpo) >= 4) else q_like

        t0 = time.time()
        try:
            rows = _random_sql_query(
                """
                SELECT DISTINCT TOP 15
                       LTRIM(RTRIM(COALESCE(en.NOKOENAMP, en.NOKOEN, ''))) AS razon_social,
                       LTRIM(RTRIM(COALESCE(en.RTEN, '')))                 AS rut,
                       LTRIM(RTRIM(COALESCE(en.TIEN, '')))                 AS tien
                  FROM MAEEN en
                 WHERE (
                       UPPER(LTRIM(RTRIM(COALESCE(en.NOKOEN,    '')))) LIKE %s
                    OR UPPER(LTRIM(RTRIM(COALESCE(en.NOKOENAMP, '')))) LIKE %s
                    OR LTRIM(RTRIM(COALESCE(en.RTEN, '')))             LIKE %s
                    OR LTRIM(RTRIM(COALESCE(en.RTEN, '')))             LIKE %s
                 )
                 ORDER BY
                    CASE WHEN LTRIM(RTRIM(COALESCE(en.TIEN,''))) IN ('C','A') THEN 0 ELSE 1 END,
                    razon_social
                """,
                (q_like, q_like, q_like, q_cuerpo_like),
                max_rows=15,
            ) or []
        except Exception as _e:
            print(f"[tk_erp_buscar_cliente] error q={q!r}: {_e}", flush=True)
            return jsonify({"ok": False, "error": "ERP no disponible ahora", "resultados": []}), 200

        elapsed_ms = int((time.time() - t0) * 1000)
        print(f"[tk_erp_buscar_cliente] q={q!r} -> {len(rows)} filas en {elapsed_ms}ms "
              f"tien={[r.get('tien') for r in rows]}", flush=True)

        resultados = [{"empresa": r.get("razon_social") or "", "rut": r.get("rut") or ""}
                      for r in rows if r.get("razon_social") or r.get("rut")]
        return jsonify({"ok": True, "resultados": resultados})

    # ─────────────────────────────────────────────────────────────────
    #  API — ERP: buscar EQUIPO/producto del cliente por SKU o nombre
    #  Busca en los documentos del cliente (lo que realmente compro),
    #  asi el equipo queda asociado al cliente. Read-only.
    # ─────────────────────────────────────────────────────────────────
    @app.route("/tickets/api/erp/buscar-producto", methods=["GET"])
    @_tickets_required
    def tk_api_erp_buscar_producto():
        rut = (request.args.get("rut") or "").strip()
        q = (request.args.get("q") or "").strip()
        if not rut:
            return jsonify({"ok": False, "error": "Selecciona primero el cliente", "resultados": []}), 200
        if len(q) < 2:
            return jsonify({"ok": True, "resultados": []})
        if not (_random_sql_query and _rut_cuerpo):
            return jsonify({"ok": False, "error": "Motor ERP no disponible", "resultados": []}), 200
        rut_base = _rut_cuerpo(rut)
        if not rut_base:
            return jsonify({"ok": False, "error": "RUT invalido", "resultados": []}), 200
        q_like = f"%{q.upper()[:60]}%"
        tidos_in = "','".join(_RANDOM_TIDOS_VENTA)
        try:
            rows = _random_sql_query(f"""
                SELECT TOP 40
                    LTRIM(RTRIM(d.KOPRCT)) AS sku,
                    LTRIM(RTRIM(d.NOKOPR)) AS nombre,
                    LTRIM(RTRIM(d.TIDO))   AS tido,
                    LTRIM(RTRIM(d.NUDO))   AS nudo,
                    e.FEEMDO               AS fecha,
                    SUM(d.CAPRCO1)         AS cantidad
                FROM MAEDDO d
                JOIN MAEEDO e
                    ON LTRIM(RTRIM(e.TIDO)) = LTRIM(RTRIM(d.TIDO))
                   AND LTRIM(RTRIM(e.NUDO)) = LTRIM(RTRIM(d.NUDO))
                WHERE (e.ENDO LIKE %s OR e.ENDO LIKE %s)
                  AND (UPPER(d.NOKOPR) LIKE %s OR UPPER(d.KOPRCT) LIKE %s)
                  AND d.PRCT = '.f.'
                  AND LTRIM(RTRIM(d.TIDO)) IN ('{tidos_in}')
                  AND (e.ESDO IS NULL OR LTRIM(RTRIM(e.ESDO)) <> 'NULO')
                GROUP BY d.KOPRCT, d.NOKOPR, d.TIDO, d.NUDO, e.FEEMDO
                ORDER BY e.FEEMDO DESC
            """, (f"{rut_base}%", f"{rut_base}-%", q_like, q_like), max_rows=40) or []
        except Exception as _e:
            print(f"[tk_erp_buscar_producto] error: {_e}", flush=True)
            return jsonify({"ok": False, "error": "ERP no disponible ahora", "resultados": []}), 200

        # dedup por SKU (el mismo producto puede venir en varios documentos)
        vistos, out = set(), []
        for r in rows:
            sku = (r.get("sku") or "").strip()
            key = sku or (r.get("nombre") or "")
            if key in vistos:
                continue
            vistos.add(key)
            fecha = r.get("fecha")
            try:
                fstr = fecha.strftime("%d/%m/%Y") if hasattr(fecha, "strftime") else ""
            except Exception:
                fstr = ""
            out.append({"sku": sku, "nombre": (r.get("nombre") or "").strip(),
                        "tido": r.get("tido"), "nudo": r.get("nudo"), "fecha": fstr})
        return jsonify({"ok": True, "resultados": out})

    # ─────────────────────────────────────────────────────────────────
    #  MIGRACION / CENTRALIZACION desde mant_tickets (Blueprint §7)
    #  Aditiva e idempotente: dedup por mant_ticket_id. NUNCA modifica ni
    #  borra mant_tickets* (Regla #4.2). Con dry_run=1 solo reporta.
    # ─────────────────────────────────────────────────────────────────
    def _mant_tables_exist():
        try:
            row = mysql_fetchone(
                "SELECT COUNT(*) AS n FROM information_schema.tables "
                "WHERE table_schema=DATABASE() AND table_name='mant_tickets'")
            return bool(row and row["n"])
        except Exception:
            return False

    def _tk_import_desde_mant(dry_run=True):
        resumen = {"origen_total": 0, "ya_migrados": 0, "migrados": 0,
                   "bitacora": 0, "equipos": 0, "errores": 0, "dry_run": dry_run,
                   "muestra": []}
        if not _mant_tables_exist():
            resumen["error"] = "No existe mant_tickets en esta base."
            return resumen

        origen = mysql_fetchall(
            "SELECT t.*, c.razon_social, c.rut AS cli_rut, c.contacto_nombre, "
            "       c.contacto_tel, c.contacto_email "
            "FROM mant_tickets t LEFT JOIN mant_clientes c ON c.id=t.cliente_id "
            "ORDER BY t.id")
        resumen["origen_total"] = len(origen)

        # ids ya migrados (para idempotencia)
        ya = mysql_fetchall(
            "SELECT mant_ticket_id FROM tk_tickets WHERE mant_ticket_id IS NOT NULL")
        ya_set = {r["mant_ticket_id"] for r in ya}

        for m in origen:
            if m["id"] in ya_set:
                resumen["ya_migrados"] += 1
                continue
            estado = _MANT_ESTADO_MAP.get((m.get("estado") or "").lower(), "open")
            tipo = _MANT_TIPO_MAP.get((m.get("tipo") or "").lower(), "tech_support")
            prio = (m.get("prioridad") or "media").lower()
            if prio not in TK_PRIORIDADES:
                prio = "media"
            empresa = (m.get("razon_social") or m.get("solicitante") or "")[:150] or None
            contacto = (m.get("contacto_nombre") or m.get("solicitante") or "")[:150] or None
            email = (m.get("solicitante_email") or m.get("contacto_email") or "")[:150] or None
            phone = (m.get("solicitante_tel") or m.get("contacto_tel") or "")[:20] or None
            rut = (m.get("cli_rut") or "")[:12] or None

            if dry_run:
                resumen["migrados"] += 1
                if len(resumen["muestra"]) < 8:
                    resumen["muestra"].append({
                        "mant_id": m["id"], "numero": m.get("numero_ticket"),
                        "empresa": empresa, "estado_origen": m.get("estado"),
                        "estado_destino": estado, "tipo_origen": m.get("tipo"),
                        "tipo_destino": tipo})
                continue

            try:
                conn = get_mysql()
                try:
                    with conn.cursor() as cur:
                        cur.execute(
                            "INSERT INTO tk_tickets "
                            "(numero_ticket, origen, estado, tipo, prioridad, titulo, descripcion, "
                            " rut, empresa, nombre_contacto, email, phone, asignado_a, tecnico_id, "
                            " visita_id, fecha_limite, notas_internas, mant_ticket_id, created_by, created_at) "
                            "VALUES (%s,'backoffice',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                            (m.get("numero_ticket"), estado, tipo, prio,
                             (m.get("titulo") or "")[:300] or None,
                             m.get("descripcion"), rut, empresa, contacto, email, phone,
                             (m.get("asignado_a") or "")[:190] or None,
                             m.get("tecnico_id"), m.get("visita_id"),
                             m.get("fecha_limite"), m.get("notas_internas"),
                             m["id"], (m.get("created_by") or "")[:190] or None,
                             m.get("created_at")))
                        new_id = cur.lastrowid
                        # bitacora -> tk_mensajes
                        bita = mysql_fetchall(
                            "SELECT tipo, contenido, metadata, usuario, es_interno, created_at "
                            "FROM mant_ticket_bitacora WHERE ticket_id=%s ORDER BY id", (m["id"],))
                        for b in bita:
                            cur.execute(
                                "INSERT INTO tk_mensajes (ticket_id, tipo, contenido, metadata, usuario, es_interno, created_at) "
                                "VALUES (%s,%s,%s,%s,%s,%s,%s)",
                                (new_id,
                                 _MANT_BITACORA_TIPO_MAP.get((b.get("tipo") or "").lower(), "otro"),
                                 b.get("contenido"), b.get("metadata"), b.get("usuario"),
                                 1 if b.get("es_interno", 1) else 0, b.get("created_at")))
                            resumen["bitacora"] += 1
                        # equipos -> tk_ticket_equipos (con datos de la maquina)
                        eqs = mysql_fetchall(
                            "SELECT e.maquina_id, e.cantidad, e.notas, "
                            "       mm.nombre, mm.sku, mm.serie "
                            "FROM mant_ticket_equipos e "
                            "LEFT JOIN mant_maquinas mm ON mm.id=e.maquina_id "
                            "WHERE e.ticket_id=%s", (m["id"],))
                        for e in eqs:
                            cur.execute(
                                "INSERT INTO tk_ticket_equipos (ticket_id, nombre, sku, serie, cantidad, maquina_id, notas) "
                                "VALUES (%s,%s,%s,%s,%s,%s,%s)",
                                (new_id, (e.get("nombre") or "Equipo")[:300],
                                 (e.get("sku") or "")[:100] or None,
                                 (e.get("serie") or "")[:120] or None,
                                 int(e.get("cantidad") or 1), e.get("maquina_id"),
                                 (e.get("notas") or "")[:500] or None))
                            resumen["equipos"] += 1
                    conn.commit()
                    resumen["migrados"] += 1
                finally:
                    conn.close()
            except Exception as _e:
                resumen["errores"] += 1
                print(f"[tk_import] error en mant_ticket {m.get('id')}: {_e}", flush=True)

        return resumen

    @app.route("/tickets/api/admin/importar-mant", methods=["POST"])
    @_tickets_required
    def tk_api_import_mant():
        perms = g.get("permissions") or {}
        if not perms.get("superadmin"):
            return jsonify({"ok": False, "error": "Solo superadministrador"}), 403
        dry = str(request.args.get("dry_run", "1")).lower() in ("1", "true", "yes")
        resumen = _tk_import_desde_mant(dry_run=dry)
        if not dry:
            try:
                _audit("tk_import_mant", target_type="tk_ticket",
                       details={"migrados": resumen.get("migrados")})
            except Exception:
                pass
        return jsonify({"ok": True, "resumen": resumen})

    # exponer para tests / uso programatico
    app.config["_tk_import_desde_mant"] = _tk_import_desde_mant

    print("[ILUS] Modulo Tickets central registrado (/tickets).", flush=True)

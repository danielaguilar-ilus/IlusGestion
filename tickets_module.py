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
import html as _html_mod
import json
import os
import re
import time
from functools import wraps
from html.parser import HTMLParser
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


def _chile_hoy():
    """Fecha de HOY en hora Chile (Regla #6 -- MySQL CURDATE() usa UTC, lo
    que corre el corte del dia varias horas antes de medianoche en Chile)."""
    try:
        if _CL_TZ is not None:
            return datetime.now(timezone.utc).astimezone(_CL_TZ).date()
    except Exception:
        pass
    return datetime.utcnow().date()


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
    # Con tildes correctas (calcan el texto EXACTO del formulario de
    # referencia que Daniel pidio copiar -- ilus-front/formulario.html).
    "install": "Instalación", "tech_support": "Soporte técnico",
    "shipping": "Despacho", "quotation": "Cotización", "return": "Devolución",
    "tech_evaluation": "Evaluación técnica", "maintenance": "Mantenimiento",
    "spare_parts": "Repuesto y piezas", "equipment_transfer": "Movimiento de equipos",
    "warranty": "Garantía", "repair": "Reparación",
    "spare_parts_store": "Repuestos bodega", "spare_parts_import": "Repuestos importación",
}
# Los 8 tipos expuestos al publico / mas usados en el backoffice.
TK_TIPOS_PUBLICOS = (
    "install", "tech_support", "maintenance", "warranty",
    "spare_parts", "quotation", "shipping", "return",
)
# Tipos del MODAL interno (backoffice): TODOS menos 'warranty' — la garantia
# es un toggle separado (es_garantia) porque puede aplicar a cualquier tipo
# (pedido de Daniel 2026-07-11: "la garantia puede ser de todo").
TK_TIPOS_MODAL = tuple(t for t in TK_TIPOS if t != "warranty")

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

# ─────────────────────────────────────────────────────────────────────────
#  Sanitizador de HTML para el contenido de tk_mensajes (whitelist, solo
#  libreria estandar -- sin agregar dependencias nuevas al deploy).
#
#  El composer de Respuestas usa un editor rico (Quill) que genera HTML
#  (negrita/listas/links). Ese HTML se guarda TAL CUAL y se re-renderiza
#  sin escapar en la conversacion (necesario para mostrar el formato) --
#  eso es una superficie de XSS real: cualquier <script>/onerror/etc que
#  llegue en `contenido` (via API directa, no solo desde la UI) se
#  ejecutaria en la sesion de OTRO miembro del staff que vea el ticket, y
#  a futuro en mensajes entrantes de clientes (Fase 3). Se sanitiza aqui,
#  al momento de guardar, quedandonos solo con las etiquetas que el
#  toolbar de Quill puede producir + <a href="http(s)/mailto"> segura.
# ─────────────────────────────────────────────────────────────────────────
_TAGS_PERMITIDOS = {"p", "br", "b", "strong", "i", "em", "u", "ol", "ul", "li", "a", "span"}
_HREF_OK = re.compile(r"^(https?://|mailto:)", re.I)


class _SanitizadorHTMLMensaje(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.out = []

    def handle_starttag(self, tag, attrs):
        if tag not in _TAGS_PERMITIDOS:
            return
        extra = ""
        if tag == "a":
            for k, v in attrs:
                if k == "href" and v and _HREF_OK.match(v.strip()):
                    extra = f' href="{_html_mod.escape(v.strip(), quote=True)}" target="_blank" rel="noopener"'
                    break
        self.out.append(f"<{tag}{extra}>")

    def handle_startendtag(self, tag, attrs):
        if tag == "br":
            self.out.append("<br>")

    def handle_endtag(self, tag):
        if tag in _TAGS_PERMITIDOS:
            self.out.append(f"</{tag}>")

    def handle_data(self, data):
        self.out.append(_html_mod.escape(data))


def _sanitizar_html_mensaje(raw):
    """Whitelist de tags: p/br/b/strong/i/em/u/ol/ul/li/span/a(href seguro).
    Todo el resto (script, on*, style, iframe, svg, atributos no listados)
    se descarta; el texto se preserva escapado."""
    if not raw:
        return ""
    parser = _SanitizadorHTMLMensaje()
    try:
        parser.feed(raw)
        parser.close()
        return "".join(parser.out)
    except Exception:
        return _html_mod.escape(str(raw))


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

    # Bodega desde la que se busca el catalogo general (Daniel 2026-07-11:
    # "necesito que traiga todos los productos de la bodega 02"). Codigo
    # TABBO.KOBO real segun docs/erp/TABLAS-BD-Random.pdf; configurable por
    # env var por si el codigo exacto no es '02'. Usado tanto por el
    # formulario publico como por el buscador de equipo del modal interno
    # cuando aun no hay cliente seleccionado (Daniel: "no me deja avanzar
    # porque no me muestra los equipos, necesita un cliente" -- ahora cae
    # al catalogo general en vez de bloquearse).
    BODEGA_SOPORTE = os.environ.get("TK_BODEGA_SOPORTE", "02").strip()
    MAX_PRODUCTOS_BUSCADOS = 20

    def _buscar_catalogo_bodega(q):
        """Catalogo general (MAEPR) con stock en BODEGA_SOPORTE (MAEST).
        Compartido por /soporte/api/erp/productos (publico) y
        /tickets/api/erp/buscar-producto (interno, sin cliente aun)."""
        if not _random_sql_query:
            return None, "Catálogo no disponible"
        q_like = f"%{q.upper()[:60]}%"
        try:
            rows = _random_sql_query(
                f"""
                SELECT DISTINCT TOP {MAX_PRODUCTOS_BUSCADOS}
                       LTRIM(RTRIM(pr.KOPR)) AS sku, LTRIM(RTRIM(pr.NOKOPR)) AS nombre
                  FROM MAEPR pr
                 WHERE (UPPER(pr.NOKOPR) LIKE %s OR UPPER(pr.KOPR) LIKE %s)
                   AND EXISTS (
                       SELECT 1 FROM MAEST st
                        WHERE LTRIM(RTRIM(st.KOPR)) = LTRIM(RTRIM(pr.KOPR))
                          AND LTRIM(RTRIM(st.KOBO)) = %s
                   )
                 ORDER BY nombre
                """,
                (q_like, q_like, BODEGA_SOPORTE), max_rows=MAX_PRODUCTOS_BUSCADOS,
            ) or []
        except Exception as _e:
            print(f"[_buscar_catalogo_bodega] error (bodega={BODEGA_SOPORTE}): {_e}", flush=True)
            return None, "Catálogo no disponible ahora"
        return [{"sku": r.get("sku") or "", "nombre": r.get("nombre") or ""}
                for r in rows if r.get("nombre")], None

    # Correo saliente real al cliente: reusar el estandar de marca ILUS
    # (Regla: un solo `_ilus_email_master`/`_send_ilus_email`, no duplicar).
    # La plantilla de tickets vive en comm_templates (modulo='tickets',
    # estado='respuesta') -- editable por Daniel desde /comunicaciones,
    # sembrada al boot por _ensure_comm_template_tickets(). El cuerpo
    # SIEMPRE se envuelve con _comm_render_email_document (header negro+logo
    # + footer de marca) -- ningun correo de tickets sale "pelado".
    _send_ilus_email = ctx.get("_send_ilus_email")
    _brand_subject = ctx.get("_brand_subject") or (lambda tema: tema)
    _render_comm_template = ctx.get("_render_comm_template")
    _comm_render_email_document = ctx.get("_comm_render_email_document")
    # Llave de paso por modulo (Daniel 2026-07-11): "tickets" nace CERRADA.
    # _send_ilus_email(..., modulo="tickets") YA la respeta automaticamente
    # (bloquea y loguea en email_log) -- esto solo sirve para dar un mensaje
    # de error claro en la UI en vez de un generico "no se pudo enviar".
    _modulo_canal_bloqueado = ctx.get("_modulo_canal_bloqueado")

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
            CREATE TABLE IF NOT EXISTS tk_plantillas (
              id           INT AUTO_INCREMENT PRIMARY KEY,
              titulo       VARCHAR(150) NOT NULL,
              cuerpo       MEDIUMTEXT NOT NULL,
              created_by   VARCHAR(190) NULL,
              created_at   DATETIME DEFAULT CURRENT_TIMESTAMP,
              KEY idx_titulo (titulo)
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
        # Daniel 2026-07-11: "tengo que tener datos de quien lo abrio, cuando
        # lo abrio, la hora, el dia" -- 1 fila por (ticket,usuario), se
        # actualiza cada vez que ese usuario abre la ficha (ON DUPLICATE KEY).
        # No usa tk_mensajes a proposito: no queremos spamear el hilo de
        # conversacion con un evento cada vez que alguien recarga la pagina.
        mysql_execute("""
            CREATE TABLE IF NOT EXISTS tk_vistas (
              ticket_id     INT NOT NULL,
              usuario       VARCHAR(190) NOT NULL,
              primera_vez   DATETIME DEFAULT CURRENT_TIMESTAMP,
              visto_at      DATETIME DEFAULT CURRENT_TIMESTAMP
                            ON UPDATE CURRENT_TIMESTAMP,
              PRIMARY KEY (ticket_id, usuario),
              CONSTRAINT fk_tkvista_ticket FOREIGN KEY (ticket_id)
                 REFERENCES tk_tickets(id) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)

    def _ensure_tk_tickets_columns():
        """Migracion aditiva de tk_tickets (patron _ensure_transporte_columns):
        - es_garantia: la garantia es un flag SEPARADO del tipo (puede aplicar
          a cualquier tipo de solicitud — pedido de Daniel 2026-07-11).
        - resuelto_at: fecha real de resolucion/cierre (base para SLA).
        - legacy_taa_id: ID del ticket en el sistema Triple A (CSV importado);
          UNIQUE para que la importacion sea idempotente."""
        try:
            existentes = {r["COLUMN_NAME"] for r in mysql_fetchall(
                "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='tk_tickets'")}
        except Exception as _e:
            print(f"[ILUS][WARN] _ensure_tk_tickets_columns (schema check): {_e}", flush=True)
            return
        alters = []
        if "es_garantia" not in existentes:
            alters.append("ADD COLUMN es_garantia TINYINT(1) NOT NULL DEFAULT 0")
        if "resuelto_at" not in existentes:
            alters.append("ADD COLUMN resuelto_at DATETIME NULL")
        if "legacy_taa_id" not in existentes:
            alters.append("ADD COLUMN legacy_taa_id INT NULL")
        for a in alters:
            try:
                mysql_execute(f"ALTER TABLE tk_tickets {a}")
            except Exception as _e:
                print(f"[ILUS][WARN] ALTER tk_tickets {a}: {_e}", flush=True)
        try:
            mysql_execute("CREATE UNIQUE INDEX uq_tk_legacy_taa ON tk_tickets (legacy_taa_id)")
        except Exception:
            pass  # ya existe

    def _ensure_tk_mensajes_columns():
        """Migracion aditiva por columnas (patron _ensure_transporte_columns):
        agrega to_email/cc_email/estado_envio a tk_mensajes si faltan. Necesario
        para el composer de respuesta al cliente (De/Para/CC + estado de envio)."""
        try:
            existentes = {r["COLUMN_NAME"] for r in mysql_fetchall(
                "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='tk_mensajes'")}
        except Exception as _e:
            print(f"[ILUS][WARN] _ensure_tk_mensajes_columns (schema check): {_e}", flush=True)
            return
        alters = []
        if "to_email" not in existentes:
            alters.append("ADD COLUMN to_email VARCHAR(150) NULL")
        if "cc_email" not in existentes:
            alters.append("ADD COLUMN cc_email VARCHAR(300) NULL")
        if "estado_envio" not in existentes:
            alters.append("ADD COLUMN estado_envio ENUM('enviado','fallido') NULL")
        for a in alters:
            try:
                mysql_execute(f"ALTER TABLE tk_mensajes {a}")
            except Exception as _e:
                print(f"[ILUS][WARN] tk_mensajes {a}: {_e}", flush=True)

    with app.app_context():
        try:
            _ensure_tickets_tables()
            _ensure_tk_tickets_columns()
            _ensure_tk_mensajes_columns()
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

    def _tk_log(ticket_id, tipo, contenido, usuario=None, metadata=None, es_interno=True,
                to_email=None, cc_email=None, estado_envio=None):
        """Escribe un evento/mensaje en tk_mensajes. Nunca rompe el flujo.
        Devuelve el id del mensaje insertado (o None si fallo) -- lo usan
        responder-cliente/comentario para vincular adjuntos al mensaje."""
        base_user = usuario or (current_username() or "sistema")
        try:
            mysql_execute(
                "INSERT INTO tk_mensajes "
                "(ticket_id, tipo, contenido, metadata, usuario, es_interno, to_email, cc_email, estado_envio) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (ticket_id, tipo, contenido,
                 json.dumps(metadata, ensure_ascii=False) if metadata else None,
                 base_user, 1 if es_interno else 0, to_email, cc_email, estado_envio),
            )
        except Exception as _e:
            # Fallback defensivo: si la migracion de columnas (to_email/cc_email/
            # estado_envio) no llego a correr por algun motivo, no debe romperse
            # TODA la conversacion -- se guarda igual sin esas columnas.
            print(f"[tk_log] insert con columnas nuevas fallo, fallback: {_e}", flush=True)
            try:
                mysql_execute(
                    "INSERT INTO tk_mensajes (ticket_id, tipo, contenido, metadata, usuario, es_interno) "
                    "VALUES (%s,%s,%s,%s,%s,%s)",
                    (ticket_id, tipo, contenido,
                     json.dumps(metadata, ensure_ascii=False) if metadata else None,
                     base_user, 1 if es_interno else 0),
                )
            except Exception as _e2:
                print(f"[tk_log] error: {_e2}", flush=True)
                return None
        try:
            row = mysql_fetchone("SELECT LAST_INSERT_ID() AS id")
            return int(row["id"]) if row and row.get("id") else None
        except Exception:
            return None

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
        # BUG FIX 2026-07-11: tk_tipos_publicos NO se estaba pasando -> el modal
        # renderizaba CERO pastillas de tipo, y como el tipo es obligatorio era
        # IMPOSIBLE crear un ticket desde el modal (reporte de Daniel).
        # El modal interno muestra TODOS los tipos menos 'warranty': la garantia
        # ahora es un toggle SEPARADO (puede aplicar a cualquier tipo).
        return render_template(
            "tickets/list.html",
            estado_label=ESTADO_LABEL, tipo_label=TIPO_LABEL,
            tk_tipos=TK_TIPOS, tk_estados=TK_ESTADOS, tk_prioridades=TK_PRIORIDADES,
            tk_tipos_publicos=TK_TIPOS_MODAL,
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

        # Daniel 2026-07-11: "cuando el cliente responda... las respuestas mas
        # nuevas se van posicionando mas arriba" -- un ticket con mensajes de
        # cliente sin leer sube al tope de la bandeja, ANTES que el orden por
        # estado/prioridad (para que nunca se pierda una respuesta nueva).
        rows = mysql_fetchall(
            "SELECT t.id, t.numero_ticket, t.origen, t.estado, t.tipo, t.prioridad, "
            "       t.titulo, t.empresa, t.rut, t.nombre_contacto, t.asignado_a, "
            "       t.created_at, t.updated_at, t.fecha_limite, t.es_garantia, "
            "       (SELECT COUNT(*) FROM tk_mensajes m "
            "          WHERE m.ticket_id=t.id AND m.tipo='client_message' "
            "            AND m.created_at > COALESCE(t.staff_last_read_at,'1970-01-01')) AS unread_count "
            f"FROM tk_tickets t{wsql} "
            "ORDER BY "
            "  (SELECT COUNT(*) FROM tk_mensajes m2 "
            "     WHERE m2.ticket_id=t.id AND m2.tipo='client_message' "
            "       AND m2.created_at > COALESCE(t.staff_last_read_at,'1970-01-01')) > 0 DESC, "
            "  FIELD(t.estado,'open','in_progress','pending','ot_pending_approval',"
            "'ot_generated','ot_in_progress','resolved','closed','cancelado'), "
            "FIELD(t.prioridad,'urgente','alta','media','baja'), t.updated_at DESC, t.id DESC "
            "LIMIT %s OFFSET %s",
            tuple(params) + (limit, offset),
        )

        # KPIs deben respetar los MISMOS filtros (wsql/params) que el listado
        # de arriba -- si no, el conteo siempre escanea la tabla completa sin
        # importar lo que el usuario esta filtrando. "Hoy" se calcula en hora
        # Chile (Regla #6), no con CURDATE() de MySQL (UTC).
        kpi = mysql_fetchone(
            "SELECT "
            "  COUNT(*) AS total, "
            "  SUM(estado IN ('open','in_progress')) AS activos, "
            "  SUM(prioridad='urgente' AND estado NOT IN ('resolved','closed','cancelado')) AS urgentes, "
            "  SUM(fecha_limite IS NOT NULL AND fecha_limite < %s "
            "      AND estado NOT IN ('resolved','closed','cancelado')) AS vencidos "
            f"FROM tk_tickets t{wsql}",
            (_chile_hoy(),) + tuple(params),
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
        tipo_in = (d.get("tipo") or "").strip()

        # Obligatorios pedidos por Daniel (equipo NO es obligatorio):
        # tipo, RUT, empresa, contacto, telefono, correo, direccion, descripcion.
        faltantes = []
        if not tipo_in: faltantes.append("tipo de solicitud")
        if not (d.get("rut") or "").strip(): faltantes.append("RUT")
        if not empresa: faltantes.append("empresa")
        if not (d.get("nombre_contacto") or "").strip(): faltantes.append("nombre de contacto")
        if not (d.get("phone") or "").strip(): faltantes.append("teléfono")
        if not (d.get("email") or "").strip(): faltantes.append("correo")
        if not (d.get("direccion") or "").strip(): faltantes.append("dirección")
        if not descripcion: faltantes.append("descripción del problema")
        if faltantes:
            return jsonify({"ok": False, "error": "Faltan campos obligatorios: " + ", ".join(faltantes)}), 400

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
                    " asignado_a, fecha_limite, notas_internas, created_by, es_garantia) "
                    "VALUES (%s,'open',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
                    "        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
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
                        1 if d.get("es_garantia") else 0,
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
                    except Exception as _e:
                        print(f"[tk_api_create] equipo no insertado tid={tid}: {_e}", flush=True)
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
        try:
            _tk_notificar_lifecycle(tid, "creacion")
        except Exception as _e:
            print(f"[tk_api_create] notificacion creacion no enviada tid={tid}: {_e}", flush=True)
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
        try:
            mensajes = mysql_fetchall(
                "SELECT id, tipo, contenido, metadata, usuario, es_interno, message_date, created_at, "
                "       to_email, cc_email, estado_envio "
                "FROM tk_mensajes WHERE ticket_id=%s ORDER BY created_at ASC, id ASC", (tid,))
        except Exception as _e:
            # Defensivo: si la migracion de columnas (to_email/cc_email/
            # estado_envio) no corrio, no debe romperse la ficha entera.
            print(f"[tk_api_get] mensajes con columnas nuevas fallo, fallback: {_e}", flush=True)
            mensajes = mysql_fetchall(
                "SELECT id, tipo, contenido, metadata, usuario, es_interno, message_date, created_at "
                "FROM tk_mensajes WHERE ticket_id=%s ORDER BY created_at ASC, id ASC", (tid,))
            for _m in mensajes:
                _m["to_email"] = None; _m["cc_email"] = None; _m["estado_envio"] = None
        adjuntos = mysql_fetchall(
            "SELECT id, mensaje_id, archivo_url, archivo_nombre, mime_type, file_size_kb, origen, created_at "
            "FROM tk_adjuntos WHERE ticket_id=%s ORDER BY id", (tid,))

        # Registrar la vista de ESTE usuario (quien lo abrio, cuando) -- 1 fila
        # por usuario, se actualiza cada vez que reabre. No bloquea la
        # respuesta si falla (ej. tabla no migrada aun).
        _user_actual = current_username() or "desconocido"
        try:
            mysql_execute(
                "INSERT INTO tk_vistas (ticket_id, usuario) VALUES (%s,%s) "
                "ON DUPLICATE KEY UPDATE visto_at=NOW()", (tid, _user_actual))
        except Exception as _e:
            print(f"[tk_api_get] no se pudo registrar vista tid={tid} user={_user_actual}: {_e}", flush=True)
        try:
            vistas = mysql_fetchall(
                "SELECT usuario, primera_vez, visto_at FROM tk_vistas "
                "WHERE ticket_id=%s ORDER BY visto_at DESC", (tid,))
        except Exception:
            vistas = []

        return jsonify({
            "ok": True,
            "ticket": _fmt_row(t),
            "equipos": [dict(r) for r in equipos],
            "documentos": [_fmt_row(r) for r in documentos],
            "mensajes": [_fmt_row(r) for r in mensajes],
            "adjuntos": [_fmt_row(r) for r in adjuntos],
            "vistas": [_fmt_row(r) for r in vistas],
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
            # Daniel 2026-07-11: la ficha ahora tambien edita region/comuna
            # (antes solo se guardaban al crear el ticket, nunca al editar).
            "region_nombre", "comuna_nombre", "numero_documento",
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

        # Notificacion automatica al cliente en los hitos resuelto/cerrado
        # (Daniel 2026-07-11) -- respeta la llave de paso del modulo 'tickets'.
        if nuevo_estado == "resolved" and prev["estado"] not in ("resolved", "closed"):
            try:
                _tk_notificar_lifecycle(tid, "resuelto")
            except Exception as _e:
                print(f"[tk_api_update] notificacion resuelto no enviada tid={tid}: {_e}", flush=True)
        elif nuevo_estado == "closed" and prev["estado"] != "closed":
            try:
                _tk_notificar_lifecycle(tid, "cerrado")
            except Exception as _e:
                print(f"[tk_api_update] notificacion cerrado no enviada tid={tid}: {_e}", flush=True)
        return jsonify({"ok": True})

    # ─────────────────────────────────────────────────────────────────
    #  API — eliminar (SOLO superadmin + confirm_text=numero_ticket;
    #  hard delete en cascada -- Regla #5: tabla critica -> triple
    #  proteccion: solo superadmin, confirm_text exacto, audit ANTES de borrar)
    # ─────────────────────────────────────────────────────────────────
    @app.route("/tickets/api/tickets/<int:tid>", methods=["DELETE"])
    @_tickets_required
    def tk_api_delete(tid):
        t = mysql_fetchone("SELECT numero_ticket, created_by FROM tk_tickets WHERE id=%s", (tid,))
        if not t:
            return jsonify({"ok": False, "error": "Ticket no encontrado"}), 404
        perms = g.get("permissions") or {}
        if not perms.get("superadmin"):
            return jsonify({"ok": False, "error": "Solo un superadministrador puede eliminar un ticket."}), 403

        numero = (t.get("numero_ticket") or "").strip()
        d = request.get_json(silent=True) or {}
        confirm = (d.get("confirm_text") or "").strip().upper()
        if not numero or confirm != numero.upper():
            return jsonify({
                "ok": False,
                "error": "Para confirmar, escribe exactamente el número del ticket.",
                "expected": numero,
            }), 400

        try:
            _audit("tk_ticket_delete", target_type="tk_ticket", target_id=tid,
                   details={"numero": numero})
        except Exception:
            pass
        mysql_execute("DELETE FROM tk_tickets WHERE id=%s", (tid,))
        return jsonify({"ok": True})

    def _tk_link_adjuntos(tid, mensaje_id, adjunto_ids):
        """Vincula adjuntos ya subidos (POST .../adjuntos) al mensaje recien
        creado, para que la conversacion los agrupe correctamente."""
        ids = [int(i) for i in (adjunto_ids or []) if str(i).isdigit()]
        if not ids or not mensaje_id:
            return
        try:
            placeholders = ",".join(["%s"] * len(ids))
            mysql_execute(
                f"UPDATE tk_adjuntos SET mensaje_id=%s WHERE ticket_id=%s AND id IN ({placeholders})",
                (mensaje_id, tid, *ids))
        except Exception as _e:
            print(f"[tk_link_adjuntos] error: {_e}", flush=True)

    # ─────────────────────────────────────────────────────────────────
    #  Notificaciones automaticas del ciclo de vida (creacion/resuelto/
    #  cerrado) -- Daniel 2026-07-11: "que funcione la mensajeria por los
    #  tickets". Reusan el MISMO estandar de marca + plantilla editable que
    #  la respuesta manual, y respetan la MISMA llave de paso del modulo
    #  'tickets' (via _send_ilus_email(modulo="tickets")) -- nace cerrada,
    #  asi que hoy no envian nada real hasta que Daniel la abra desde
    #  /comunicaciones. Si el ticket no tiene email de contacto, no hacen
    #  nada (no hay a quien avisar).
    # ─────────────────────────────────────────────────────────────────
    _TK_LIFECYCLE_DEFAULTS = {
        "creacion": (
            "Recibimos tu solicitud — ticket {numero}",
            "<p style=\"font-size:14px;color:#374151\">Estimado/a {cliente},</p>"
            "<p style=\"font-size:14px;color:#374151\">Ya registramos tu solicitud. A partir de "
            "ahora puedes seguirla con el número <strong>{numero}</strong>.</p>"),
        "resuelto": (
            "Tu ticket {numero} fue resuelto",
            "<p style=\"font-size:14px;color:#374151\">Estimado/a {cliente},</p>"
            "<p style=\"font-size:14px;color:#374151\">Te contamos que tu solicitud "
            "<strong>{numero}</strong> ya fue resuelta por nuestro equipo.</p>"),
        "cerrado": (
            "Tu ticket {numero} fue cerrado",
            "<p style=\"font-size:14px;color:#374151\">Estimado/a {cliente},</p>"
            "<p style=\"font-size:14px;color:#374151\">Tu ticket <strong>{numero}</strong> "
            "ha sido cerrado. Gracias por confiar en ILUS Sport &amp; Health.</p>"),
    }

    def _tk_notificar_lifecycle(tid, estado_slug):
        if not _send_ilus_email or estado_slug not in _TK_LIFECYCLE_DEFAULTS:
            return
        t = mysql_fetchone(
            "SELECT numero_ticket, email, empresa, nombre_contacto FROM tk_tickets WHERE id=%s", (tid,))
        if not t or not (t.get("email") or "").strip():
            return
        to_email = t["email"].strip()
        numero = t.get("numero_ticket") or f"#{tid}"
        cliente_nombre = t.get("nombre_contacto") or t.get("empresa") or "cliente"

        tema_default, cuerpo_tpl = _TK_LIFECYCLE_DEFAULTS[estado_slug]
        tema = tema_default.format(numero=numero)
        cuerpo_email = cuerpo_tpl.format(cliente=_html_mod.escape(cliente_nombre), numero=numero)
        if _render_comm_template:
            try:
                tpl = _render_comm_template(
                    estado_slug, "email", {"cliente": cliente_nombre, "numero_ticket": numero},
                    modulo="tickets")
                if tpl:
                    _asu, _cue = tpl
                    if (_asu or "").strip():
                        tema = _asu.strip()
                    if (_cue or "").strip():
                        cuerpo_email = _cue
            except Exception as _e:
                print(f"[tk_notificar_lifecycle] plantilla '{estado_slug}' no usada: {_e}", flush=True)

        # Salvaguarda: el numero de ticket SIEMPRE debe estar en el asunto
        # (identificacion del cliente + futura agrupacion de hilos por
        # asunto), aunque Daniel edite la plantilla en Comunicaciones y
        # se le olvide incluir {{numero_ticket}}.
        if numero not in tema:
            tema = f"{tema} · {numero}"

        subject = _brand_subject(tema)
        # El boton solo va en el CORREO -- lo que se persiste en tk_mensajes
        # (y por tanto se ve en la ficha interna y en el hilo del portal) es
        # solo el texto del mensaje, sin el boton que apunta al mismo portal.
        try:
            cuerpo_correo = cuerpo_email + _tk_boton_portal_html(tid)
        except Exception as _e:
            print(f"[tk_notificar_lifecycle] boton portal no agregado: {_e}", flush=True)
            cuerpo_correo = cuerpo_email
        html_final = (_comm_render_email_document(subject, cuerpo_correo, subtitle=f"Ticket {numero} · ILUS Fitness")
                      if _comm_render_email_document else cuerpo_correo)
        try:
            enviado = _send_ilus_email(to_email, subject, html_final,
                                        evento=f"ticket_{estado_slug}", modulo="tickets")
        except Exception as _e:
            print(f"[tk_notificar_lifecycle] error enviando tid={tid} estado={estado_slug}: {_e}", flush=True)
            enviado = False

        _tk_log(tid, "mensaje", cuerpo_email, es_interno=False,
                to_email=to_email[:150], estado_envio="enviado" if enviado else "fallido",
                metadata={"subject": subject, "auto": True, "estado_slug": estado_slug})

    # ─────────────────────────────────────────────────────────────────
    #  API — comentario interno (conversacion Fase 1)
    # ─────────────────────────────────────────────────────────────────
    @app.route("/tickets/api/tickets/<int:tid>/comentario", methods=["POST"])
    @_tickets_required
    def tk_api_comentario(tid):
        if not mysql_fetchone("SELECT id FROM tk_tickets WHERE id=%s", (tid,)):
            return jsonify({"ok": False, "error": "Ticket no encontrado"}), 404
        d = request.get_json(silent=True) or {}
        contenido = _sanitizar_html_mensaje((d.get("contenido") or "").strip())
        if not contenido:
            return jsonify({"ok": False, "error": "El comentario esta vacio"}), 400
        es_interno = bool(d.get("es_interno", True))
        msg_id = _tk_log(tid, "comentario", contenido[:20000], es_interno=es_interno)
        _tk_link_adjuntos(tid, msg_id, d.get("adjunto_ids"))
        mysql_execute("UPDATE tk_tickets SET updated_at=NOW() WHERE id=%s", (tid,))
        return jsonify({"ok": True, "mensaje_id": msg_id})

    # ─────────────────────────────────────────────────────────────────
    #  API — RESPONDER AL CLIENTE (correo real, De/Para/CC)
    #  Reusa el estandar de marca ILUS (_send_ilus_email/_brand_subject) --
    #  el MISMO que usan retiros/mantenciones/transporte. El "envio
    #  inteligente" (detectar sin conexion, mostrar que paso) vive en el
    #  front (ficha.html); aca devolvemos ok:false con detalle claro si
    #  el correo no se pudo enviar, y SIEMPRE dejamos registro en
    #  tk_mensajes (estado_envio='enviado'|'fallido') para trazabilidad.
    # ─────────────────────────────────────────────────────────────────
    @app.route("/tickets/api/tickets/<int:tid>/responder-cliente", methods=["POST"])
    @_tickets_required
    def tk_api_responder_cliente(tid):
        t = mysql_fetchone(
            "SELECT numero_ticket, email, empresa, nombre_contacto FROM tk_tickets WHERE id=%s", (tid,))
        if not t:
            return jsonify({"ok": False, "error": "Ticket no encontrado"}), 404
        d = request.get_json(silent=True) or {}
        contenido = _sanitizar_html_mensaje((d.get("contenido") or "").strip())
        to_email = (d.get("to") or t.get("email") or "").strip()
        cc_email = (d.get("cc") or "").strip()
        if not contenido:
            return jsonify({"ok": False, "error": "El mensaje esta vacio"}), 400
        if not to_email or "@" not in to_email:
            return jsonify({"ok": False, "error": "Falta un correo de destino valido"}), 400
        if not _send_ilus_email:
            return jsonify({"ok": False, "error": "El envio de correo no esta disponible en este entorno"}), 200

        numero = t.get("numero_ticket") or f"#{tid}"
        cliente_nombre = t.get("nombre_contacto") or t.get("empresa") or "cliente"

        # Plantilla EDITABLE (comm_templates / modulo 'tickets') + diseno
        # maestro ILUS. Si Daniel edita la plantilla en /comunicaciones, su
        # asunto/cuerpo mandan; si no existe/esta apagada/vacia, cae al texto
        # de abajo (Regla #4.2: el correo nunca deja de salir con buen diseno).
        tema_default = f"Respuesta a tu ticket {numero}"
        cuerpo_default = (
            f'<p style="font-size:14px;color:#374151">Estimado/a {_html_mod.escape(cliente_nombre)},</p>'
            f'<p style="font-size:14px;color:#374151">{contenido}</p>'
            f'<p style="font-size:13px;color:#6b7280;margin-top:16px">'
            f'Este mensaje es parte del seguimiento de tu ticket <strong>{_html_mod.escape(numero)}</strong>.</p>')
        tema, cuerpo_email = tema_default, cuerpo_default
        if _render_comm_template:
            try:
                tpl = _render_comm_template(
                    "respuesta", "email",
                    {"cliente": cliente_nombre, "numero_ticket": numero, "mensaje": contenido},
                    modulo="tickets")
                if tpl:
                    _asu, _cue = tpl
                    if (_asu or "").strip():
                        tema = _asu.strip()
                    if (_cue or "").strip():
                        cuerpo_email = _cue  # HTML editable por Daniel -> NO escapar
            except Exception as _e:
                print(f"[tk_responder_cliente] plantilla editable no usada: {_e}", flush=True)

        # Salvaguarda: el numero de ticket SIEMPRE debe estar en el asunto
        # (identificacion del cliente + futura agrupacion de hilos por
        # asunto), aunque Daniel edite la plantilla en Comunicaciones y
        # se le olvide incluir {{numero_ticket}}.
        if numero not in tema:
            tema = f"{tema} · {numero}"

        subject = _brand_subject(tema)
        try:
            cuerpo_email = cuerpo_email + _tk_boton_portal_html(tid)
        except Exception as _e:
            print(f"[tk_responder_cliente] boton portal no agregado: {_e}", flush=True)
        html_final = (_comm_render_email_document(subject, cuerpo_email, subtitle=f"Ticket {numero} · ILUS Fitness")
                      if _comm_render_email_document else cuerpo_email)

        try:
            kwargs = {"evento": "ticket_respuesta", "modulo": "tickets"}
            if cc_email:
                kwargs["cc"] = cc_email
            enviado = _send_ilus_email(to_email, subject, html_final, **kwargs)
        except Exception as _e:
            print(f"[tk_responder_cliente] error enviando tid={tid}: {_e}", flush=True)
            enviado = False

        # Se persiste el mensaje SANITIZADO plano (sin el envoltorio de marca)
        # -- el hilo interno muestra el contenido, no el email completo con logo.
        msg_id = _tk_log(
            tid, "mensaje", contenido[:20000], es_interno=False,
            to_email=to_email[:150], cc_email=cc_email[:300] or None,
            estado_envio="enviado" if enviado else "fallido",
            metadata={"subject": subject})
        _tk_link_adjuntos(tid, msg_id, d.get("adjunto_ids"))
        mysql_execute("UPDATE tk_tickets SET updated_at=NOW() WHERE id=%s", (tid,))

        if not enviado:
            if _modulo_canal_bloqueado and _modulo_canal_bloqueado("tickets", "email"):
                error_msg = ("Los envíos de correo del módulo Tickets están CERRADOS "
                             "(llave de paso). Actívalos en Comunicaciones → Tickets. "
                             "Tu mensaje quedó guardado, puedes reintentar cuando se abra.")
            else:
                error_msg = ("El correo no se pudo enviar (revisa la config de correo o intenta de nuevo). "
                             "Tu mensaje quedó guardado, puedes reintentar.")
            return jsonify({"ok": False, "mensaje_id": msg_id, "error": error_msg}), 200
        return jsonify({"ok": True, "mensaje_id": msg_id})

    # ─────────────────────────────────────────────────────────────────
    #  API — Plantillas de mensajes (canned responses)
    # ─────────────────────────────────────────────────────────────────
    @app.route("/tickets/api/plantillas", methods=["GET"])
    @_tickets_required
    def tk_api_plantillas_list():
        rows = mysql_fetchall(
            "SELECT id, titulo, cuerpo FROM tk_plantillas ORDER BY titulo LIMIT 100")
        return jsonify({"ok": True, "plantillas": [dict(r) for r in rows]})

    @app.route("/tickets/api/plantillas", methods=["POST"])
    @_tickets_required
    def tk_api_plantillas_create():
        d = request.get_json(silent=True) or {}
        titulo = (d.get("titulo") or "").strip()[:150]
        cuerpo = (d.get("cuerpo") or "").strip()
        if not titulo or not cuerpo:
            return jsonify({"ok": False, "error": "Falta título o contenido"}), 400
        mysql_execute(
            "INSERT INTO tk_plantillas (titulo, cuerpo, created_by) VALUES (%s,%s,%s)",
            (titulo, cuerpo, current_username() or "sistema"))
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
            "INSERT INTO tk_ticket_equipos (ticket_id, erp_kopr, nombre, tipo, sku, cantidad, notas, serie) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) "
            "ON DUPLICATE KEY UPDATE cantidad=VALUES(cantidad), notas=VALUES(notas), serie=VALUES(serie)",
            (tid, kopr[:100] or None, nombre[:300] or None,
             (d.get("tipo") or "").strip()[:100] or None,
             (d.get("sku") or "").strip()[:100] or None,
             cant, (d.get("notas") or "").strip()[:500] or None,
             (d.get("serie") or "").strip()[:120] or None))
        _tk_log(tid, "otro", f"Equipo agregado: {nombre or kopr}")
        return jsonify({"ok": True})

    @app.route("/tickets/api/tickets/<int:tid>/equipos/<int:eid>", methods=["DELETE"])
    @_tickets_required
    def tk_api_del_equipo(tid, eid):
        mysql_execute("DELETE FROM tk_ticket_equipos WHERE id=%s AND ticket_id=%s", (eid, tid))
        _tk_log(tid, "otro", f"Equipo #{eid} quitado")
        return jsonify({"ok": True})

    # ─────────────────────────────────────────────────────────────────
    #  API — traer equipos a un ticket YA EXISTENTE desde un documento ERP
    #  (Daniel 2026-07-11: "ya tenemos las conexiones a random, así que no
    #  debería hacérsenos difícil" -- reusa erp_engine, mismo motor que
    #  "crear ticket desde documento", pero para un ticket que ya existe).
    # ─────────────────────────────────────────────────────────────────
    @app.route("/tickets/api/tickets/<int:tid>/equipos-desde-documento", methods=["POST"])
    @_tickets_required
    def tk_api_equipos_desde_documento(tid):
        if not mysql_fetchone("SELECT id FROM tk_tickets WHERE id=%s", (tid,)):
            return jsonify({"ok": False, "error": "Ticket no encontrado"}), 404
        d = request.get_json(silent=True) or {}
        tido = (d.get("tido") or "").strip().upper()
        nudo = (d.get("nudo") or "").strip()
        if not (tido and nudo):
            return jsonify({"ok": False, "error": "Falta tipo y número de documento"}), 400
        try:
            import erp_engine
            doc = erp_engine.get_client().fetch_document(tido, nudo)
        except Exception as _e:
            print(f"[tk_equipos_desde_doc] error tid={tid} {tido}/{nudo}: {_e}", flush=True)
            return jsonify({"ok": False, "error": "ERP no disponible ahora"}), 200
        if not doc:
            return jsonify({"ok": False, "error": "Documento no encontrado en el ERP"}), 200

        lineas = []
        for ln in (doc.get("lineas_raw") or []):
            sku = str(ln.get("KOPRCT") or ln.get("koprct") or "").strip()
            nombre = str(ln.get("NOKOPR") or ln.get("nokopr") or "").strip()
            if sku or nombre:
                lineas.append({"sku": sku, "nombre": nombre,
                                "cantidad": ln.get("CAPRCO1") or ln.get("caprco1") or 1})
        if not lineas:
            return jsonify({"ok": False, "error": "El documento no tiene líneas de producto"}), 200

        agregados = 0
        try:
            mysql_execute(
                "INSERT IGNORE INTO tk_ticket_documentos (ticket_id, erp_tido, erp_nudo, fecha) "
                "VALUES (%s,%s,%s,%s)",
                (tid, tido[:10], nudo[:40], str(doc.get("fecha") or "")[:10] or None))
        except Exception as _e:
            print(f"[tk_equipos_desde_doc] documento no registrado tid={tid}: {_e}", flush=True)
        for ln in lineas:
            try:
                cant = int(ln.get("cantidad") or 1) if str(ln.get("cantidad") or 1).isdigit() else 1
                mysql_execute(
                    "INSERT IGNORE INTO tk_ticket_equipos (ticket_id, erp_kopr, nombre, sku, cantidad) "
                    "VALUES (%s,%s,%s,%s,%s)",
                    (tid, ln["sku"][:100] or None, ln["nombre"][:300] or "Equipo",
                     ln["sku"][:100] or None, cant))
                agregados += 1
            except Exception as _e:
                print(f"[tk_equipos_desde_doc] equipo no insertado tid={tid} sku={ln.get('sku')}: {_e}", flush=True)

        _tk_log(tid, "otro", f"{agregados} equipo(s) agregado(s) desde documento ERP {tido}-{nudo}")
        return jsonify({"ok": True, "agregados": agregados, "total_lineas": len(lineas)})

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

        # Mismas validaciones que el endpoint publico equivalente
        # (tk_soporte_api_adjuntos): extension whitelist + tope de MB.
        # _EXT_PERMITIDAS / MAX_ADJUNTO_MB se definen mas abajo en este
        # mismo closure (seccion formulario publico) pero ya estan
        # asignadas cuando esta ruta se ejecuta (se registran todas al
        # llamar register_tickets_routes antes de que Flask reciba pedidos).
        ext = ("." + f.filename.rsplit(".", 1)[-1].lower()) if "." in f.filename else ""
        if ext not in _EXT_PERMITIDAS:
            return jsonify({"ok": False, "error": f"Tipo de archivo no permitido ({ext or 'sin extensión'})"}), 400

        f.seek(0, 2)
        size_mb = f.tell() / (1024 * 1024)
        f.seek(0)
        if size_mb > MAX_ADJUNTO_MB:
            return jsonify({"ok": False, "error": f"El archivo supera el máximo de {MAX_ADJUNTO_MB} MB"}), 400

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
                    except Exception as _e:
                        print(f"[tk_desde_documento] documento no insertado tid={tid} "
                              f"{dc.get('tido')}/{dc.get('nudo')}: {_e}", flush=True)
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
                    except Exception as _e:
                        print(f"[tk_desde_documento] equipo no insertado tid={tid} "
                              f"sku={ln.get('sku')}: {_e}", flush=True)
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
            # FIX 2026-07-11 (verificado contra el ERP real con pymssql):
            # 1) La version anterior fallaba en SQL Server con error 145:
            #    "ORDER BY items must appear in the select list if SELECT
            #    DISTINCT is specified" (el CASE del ORDER BY no estaba en el
            #    SELECT). _random_sql_query devolvia vacio y el modal mostraba
            #    "Sin resultados" para CUALQUIER busqueda. Ahora ord_tien va
            #    en el select list.
            # 2) COALESCE(NOKOENAMP, NOKOEN) devolvia '' cuando NOKOENAMP es
            #    cadena VACIA (no NULL) aunque NOKOEN tuviera el nombre real
            #    (caso RUT 25547065), y devolvia el placeholder 'BOLETA' en
            #    boletas sin entidad. Ahora se elige el primer campo con
            #    contenido real.
            rows = _random_sql_query(
                """
                SELECT DISTINCT TOP 15
                       CASE WHEN LTRIM(RTRIM(COALESCE(en.NOKOEN,'')))
                                 NOT IN ('','BOLETA','FACTURA','CLIENTE')
                            THEN LTRIM(RTRIM(en.NOKOEN))
                            ELSE LTRIM(RTRIM(COALESCE(en.NOKOENAMP,''))) END AS razon_social,
                       LTRIM(RTRIM(COALESCE(en.RTEN, '')))                   AS rut,
                       LTRIM(RTRIM(COALESCE(en.TIEN, '')))                   AS tien,
                       CASE WHEN LTRIM(RTRIM(COALESCE(en.TIEN,''))) IN ('C','A')
                            THEN 0 ELSE 1 END                                AS ord_tien
                  FROM MAEEN en
                 WHERE (
                       UPPER(LTRIM(RTRIM(COALESCE(en.NOKOEN,    '')))) LIKE %s
                    OR UPPER(LTRIM(RTRIM(COALESCE(en.NOKOENAMP, '')))) LIKE %s
                    OR LTRIM(RTRIM(COALESCE(en.RTEN, '')))             LIKE %s
                    OR LTRIM(RTRIM(COALESCE(en.RTEN, '')))             LIKE %s
                 )
                 ORDER BY ord_tien, razon_social
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

        resultados = [{"empresa": (r.get("razon_social") or "").strip() or "(Sin nombre en el ERP)",
                       "rut": r.get("rut") or ""}
                      for r in rows if (r.get("razon_social") or "").strip() or r.get("rut")]
        return jsonify({"ok": True, "resultados": resultados})

    # ─────────────────────────────────────────────────────────────────
    #  API — ERP: buscar EQUIPO/producto (catalogo general, bodega de
    #  soporte). Read-only.
    #
    #  FIX 2026-07-12 (Daniel): antes esta ruta buscaba primero en el
    #  HISTORIAL DE COMPRAS del cliente (por RUT) y solo caia al catalogo
    #  general si esa busqueda no encontraba nada. Daniel senalo que eso es
    #  una restriccion real y equivocada: un cliente puede haber comprado el
    #  mismo equipo con OTRA razon social (RUT personal vs RUT empresa), asi
    #  que limitar la sugerencia a "lo que este RUT especifico compro" deja
    #  fuera equipos que el cliente si tiene. La logica correcta es mas
    #  simple: siempre mostrar el catalogo completo de bodega 02 (donde estan
    #  TODOS los productos para la venta) sin importar el RUT seleccionado.
    #  Los repuestos no entran aca por diseno: bodega 02 es la bodega de
    #  equipos/maquinas para la venta (confirmado contra el ERP real), y un
    #  repuesto de todas formas se asocia a la MAQUINA del ticket, no se
    #  selecciona como si fuera un equipo en si mismo.
    # ─────────────────────────────────────────────────────────────────
    @app.route("/tickets/api/erp/buscar-producto", methods=["GET"])
    @_tickets_required
    def tk_api_erp_buscar_producto():
        q = (request.args.get("q") or "").strip()
        if len(q) < 2:
            return jsonify({"ok": True, "resultados": []})
        resultados, err = _buscar_catalogo_bodega(q)
        if err:
            return jsonify({"ok": False, "error": err, "resultados": []}), 200
        return jsonify({"ok": True, "resultados": resultados, "catalogo_general": True})

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

    # ─────────────────────────────────────────────────────────────────
    #  IMPORTADOR CSV TRIPLE A — "la migracion" que pidio Daniel:
    #  trae los tickets historicos del sistema Triple A (Reporte Tickets
    #  + Reporte SLA exportados en CSV con ';'). Idempotente por
    #  legacy_taa_id (UNIQUE). El email de contacto queda en
    #  daniel.aguilar@sphs.cl (editable) para que los correos de PRUEBA
    #  le lleguen a el y JAMAS a clientes reales; el email original se
    #  conserva en notas_internas.
    # ─────────────────────────────────────────────────────────────────
    _TAA_EMAIL_PRUEBAS = "daniel.aguilar@sphs.cl"
    _TAA_ESTADO = {
        "resuelto": "resolved", "cerrado": "closed", "en curso": "in_progress",
        "abierto": "open", "pendiente": "pending", "ot generada": "ot_generated",
        "ot pendiente de aprobación": "ot_pending_approval",
        "ot pendiente de aprobacion": "ot_pending_approval",
        "ot en curso": "ot_in_progress", "cancelado": "cancelado",
    }
    _TAA_TIPO = {
        "instalación": "install", "instalacion": "install",
        "garantía": "warranty", "garantia": "warranty",
        "mantenimiento": "maintenance", "soporte técnico": "tech_support",
        "soporte tecnico": "tech_support", "repuestos": "spare_parts",
        "reparación": "repair", "reparacion": "repair",
        "cotización": "quotation", "cotizacion": "quotation",
        "envío": "shipping", "envio": "shipping",
        "evaluación técnica": "tech_evaluation", "evaluacion tecnica": "tech_evaluation",
        "devolución": "return", "devolucion": "return",
        "repuestos importación": "spare_parts_import",
        "repuestos importacion": "spare_parts_import",
        "repuestos bodega": "spare_parts_store",
        "movimiento de equipos": "equipment_transfer",
    }

    def _taa_fecha(s):
        """'03-02-2026 09:00' (DD-MM-YYYY HH:MM) -> datetime | None."""
        s = (s or "").strip()
        if not s:
            return None
        for fmt in ("%d-%m-%Y %H:%M", "%d-%m-%Y", "%d/%m/%Y %H:%M", "%d/%m/%Y"):
            try:
                return datetime.strptime(s, fmt)
            except ValueError:
                continue
        return None

    def _tk_import_desde_taa(csv_tickets_text, csv_sla_text=None, dry_run=True):
        import csv as _csv
        import io as _io
        resumen = {"filas_csv": 0, "validos": 0, "ya_importados": 0,
                   "importados": 0, "invalidos": 0, "errores": 0,
                   "dry_run": dry_run, "muestra": []}

        # SLA por Ticket ID (para resuelto_at + duracion)
        sla = {}
        if csv_sla_text:
            try:
                for r in _csv.DictReader(_io.StringIO(csv_sla_text), delimiter=";"):
                    tid_raw = (r.get("Ticket ID") or "").strip()
                    if tid_raw.isdigit():
                        sla[int(tid_raw)] = {
                            "resuelto": _taa_fecha(r.get("Resuelto/Cerrado En")),
                            "duracion": (r.get("Duración (Días)") or r.get("Duracion (Dias)") or "").strip(),
                        }
            except Exception as _e:
                print(f"[tk_import_taa] CSV SLA ilegible (se ignora): {_e}", flush=True)

        try:
            filas = list(_csv.DictReader(_io.StringIO(csv_tickets_text), delimiter=";"))
        except Exception as _e:
            resumen["error"] = f"CSV de tickets ilegible: {_e}"
            return resumen
        resumen["filas_csv"] = len(filas)

        ya = set()
        try:
            ya = {r["legacy_taa_id"] for r in mysql_fetchall(
                "SELECT legacy_taa_id FROM tk_tickets WHERE legacy_taa_id IS NOT NULL") or []}
        except Exception:
            pass

        user = current_username() or "importador_taa"
        conn = None if dry_run else get_mysql()
        try:
            for r in filas:
                tid_raw = (r.get("Ticket ID") or "").strip()
                if not tid_raw.isdigit():
                    # filas corridas por comillas rotas en el export de Triple A
                    resumen["invalidos"] += 1
                    continue
                taa_id = int(tid_raw)
                resumen["validos"] += 1
                if taa_id in ya:
                    resumen["ya_importados"] += 1
                    continue

                estado = _TAA_ESTADO.get((r.get("Estado") or "").strip().lower(), "open")
                tipo_raw = (r.get("Tipo") or "").strip().lower()
                tipo = _TAA_TIPO.get(tipo_raw)
                es_garantia = 1 if tipo == "warranty" else 0
                origen = (r.get("Origen") or "").strip().lower()
                if origen not in TK_ORIGENES:
                    origen = "backoffice"
                creado = _taa_fecha(r.get("Fecha Creación") or r.get("Fecha Creacion"))
                actualizado = _taa_fecha(r.get("Fecha Actualización") or r.get("Fecha Actualizacion"))
                s = sla.get(taa_id) or {}

                email_orig = (r.get("Email") or "").strip()
                notas = [f"Importado del sistema Triple A (Ticket #{taa_id})."]
                if email_orig:
                    notas.append(f"Email original del cliente: {email_orig}")
                if s.get("duracion"):
                    notas.append(f"SLA Triple A: {s['duracion']} día(s) hasta resolver/cerrar.")

                if len(resumen["muestra"]) < 5:
                    resumen["muestra"].append(
                        f"TAA-{taa_id} · {r.get('Empresa') or r.get('Nombre Contacto') or 'sin cliente'}"
                        f" · {estado}" + (f" · {tipo}" if tipo else ""))
                if dry_run:
                    resumen["importados"] += 1
                    continue

                try:
                    with conn.cursor() as cur:
                        cur.execute(
                            "INSERT IGNORE INTO tk_tickets "
                            "(numero_ticket, legacy_taa_id, origen, estado, tipo, es_garantia, "
                            " prioridad, descripcion, rut, empresa, nombre_contacto, phone, "
                            " email, region_nombre, comuna_nombre, direccion, asignado_a, "
                            " producto, marca, sku, notas_internas, created_by, created_at, "
                            " updated_at, resuelto_at) "
                            "VALUES (%s,%s,%s,%s,%s,%s,'media',%s,%s,%s,%s,%s,%s,%s,%s,%s,"
                            "        %s,%s,%s,%s,%s,%s,COALESCE(%s,NOW()),COALESCE(%s,NOW()),%s)",
                            (
                                f"TAA-{taa_id}", taa_id, origen, estado, tipo, es_garantia,
                                (r.get("Descripción") or r.get("Descripcion") or "").strip()[:5000] or None,
                                (r.get("RUT") or "").strip()[:12] or None,
                                (r.get("Empresa") or "").strip()[:150] or None,
                                (r.get("Nombre Contacto") or "").strip()[:150] or None,
                                (r.get("Teléfono") or r.get("Telefono") or "").strip()[:20] or None,
                                _TAA_EMAIL_PRUEBAS,
                                (r.get("Región") or r.get("Region") or "").strip()[:120] or None,
                                (r.get("Comuna") or "").strip()[:120] or None,
                                (r.get("Dirección") or r.get("Direccion") or "").strip()[:255] or None,
                                (r.get("Ejecutivo") or "").strip()[:190] or None,
                                (r.get("Producto") or "").strip() or None,
                                (r.get("Marca") or "").strip()[:100] or None,
                                (r.get("SKU") or "").strip()[:100] or None,
                                " | ".join(notas)[:5000],
                                user, creado, actualizado, s.get("resuelto"),
                            ),
                        )
                        if cur.rowcount:
                            nuevo_id = cur.lastrowid
                            resumen["importados"] += 1
                            cur.execute(
                                "INSERT INTO tk_mensajes (ticket_id, tipo, contenido, usuario, es_interno) "
                                "VALUES (%s,'creacion',%s,%s,1)",
                                (nuevo_id,
                                 f"Ticket importado del sistema Triple A (#{taa_id})"
                                 + (f" · SLA: {s['duracion']} día(s)" if s.get("duracion") else ""),
                                 user))
                        else:
                            resumen["ya_importados"] += 1
                except Exception as _e:
                    resumen["errores"] += 1
                    print(f"[tk_import_taa] error TAA-{taa_id}: {_e}", flush=True)
            if conn is not None:
                conn.commit()
        finally:
            if conn is not None:
                conn.close()
        return resumen

    @app.route("/tickets/api/admin/importar-taa", methods=["POST"])
    @_tickets_required
    def tk_api_import_taa():
        perms = g.get("permissions") or {}
        if not perms.get("superadmin"):
            return jsonify({"ok": False, "error": "Solo superadministrador"}), 403
        f_tickets = request.files.get("csv_tickets")
        if not f_tickets or not f_tickets.filename:
            return jsonify({"ok": False, "error": "Adjunta el CSV 'Reporte Tickets' de Triple A."}), 400
        f_sla = request.files.get("csv_sla")
        try:
            texto_tickets = f_tickets.read().decode("utf-8-sig", errors="replace")
            texto_sla = f_sla.read().decode("utf-8-sig", errors="replace") if (f_sla and f_sla.filename) else None
        except Exception as _e:
            print(f"[tk_import_taa] error leyendo archivos: {_e}", flush=True)
            return jsonify({"ok": False, "error": "No se pudieron leer los archivos."}), 400
        dry = str(request.args.get("dry_run", "1")).lower() in ("1", "true", "yes")
        resumen = _tk_import_desde_taa(texto_tickets, texto_sla, dry_run=dry)
        if not dry:
            try:
                _audit("tk_import_taa", target_type="tk_ticket",
                       details={"importados": resumen.get("importados"),
                                "errores": resumen.get("errores")})
            except Exception:
                pass
        return jsonify({"ok": True, "resumen": resumen})

    # exponer para tests / uso programatico
    app.config["_tk_import_desde_taa"] = _tk_import_desde_taa

    # ═══════════════════════════════════════════════════════════════════
    #  FORMULARIO PUBLICO (Fase 2) — copia fiel de ilus-front/formulario.html
    #  Sin login (Regla: una ruta sin @login_required ya es publica -- no
    #  hay gate global). Prefijo /soporte/ exento de CSRF (app.py). Rate
    #  limit propio (in-memory, por IP) en creacion/busqueda/adjuntos.
    # ═══════════════════════════════════════════════════════════════════
    # (BODEGA_SOPORTE / MAX_PRODUCTOS_BUSCADOS ya definidos arriba, junto a
    # _buscar_catalogo_bodega -- compartidos con el buscador interno.)
    MAX_ADJUNTOS = 15
    MAX_ADJUNTO_MB = 25  # Cloud Run limita CADA request HTTP a 32MB; se sube
                         # 1 archivo por request (igual que el composer interno),
                         # asi que el techo real es por-archivo, no por-lote.
    _EXT_PERMITIDAS = {
        ".jpg", ".jpeg", ".png", ".webp", ".gif", ".heic",
        ".mp4", ".mov", ".webm", ".avi",
        ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".txt", ".csv",
    }

    # Firmas (magic bytes) de los tipos mas comunes -- validacion LIGERA sin
    # librerias nuevas. Solo cubre jpg/png/gif/pdf (firma simple); el resto
    # (webp/heic/mp4/doc/docx/xls/xlsx/etc.) queda sin chequear porque sus
    # firmas son mas complejas de validar sin dependencias. Ademas se rechaza
    # cualquier archivo cuyo contenido empiece como ejecutable/script, sin
    # importar la extension declarada (mismatch claro = sospechoso).
    _MAGIC_BYTES = {
        ".jpg": (b"\xff\xd8\xff",), ".jpeg": (b"\xff\xd8\xff",),
        ".png": (b"\x89PNG",), ".gif": (b"GIF8",), ".pdf": (b"%PDF",),
    }
    _FIRMAS_SOSPECHOSAS = (b"MZ", b"#!")  # ejecutables Windows / scripts

    def _tk_magic_bytes_ok(ext, file_storage):
        try:
            file_storage.stream.seek(0)
            head = file_storage.stream.read(16)
            file_storage.stream.seek(0)
        except Exception:
            return True  # si no se puede leer la cabecera, no bloqueamos (fail-open)
        if not head:
            return True
        if any(head.startswith(f) for f in _FIRMAS_SOSPECHOSAS):
            return False
        firmas = _MAGIC_BYTES.get(ext)
        if firmas and not any(head.startswith(f) for f in firmas):
            return False
        return True

    _RL_PUBLICO = {}  # ip -> [timestamps] (ventana deslizante, in-memory)

    def _rl_ok(clave, max_req, ventana_seg):
        ahora = time.time()
        arr = [t for t in (_RL_PUBLICO.get(clave) or []) if (ahora - t) < ventana_seg]
        if len(arr) >= max_req:
            _RL_PUBLICO[clave] = arr
            return False
        arr.append(ahora)
        _RL_PUBLICO[clave] = arr
        return True

    def _ip_cliente():
        # Cloud Run va detras de un proxy -- X-Forwarded-For trae la IP real.
        xff = request.headers.get("X-Forwarded-For", "")
        return (xff.split(",")[0].strip() if xff else request.remote_addr) or "desconocida"

    def _tk_upload_key():
        k = app.secret_key
        return k.encode() if isinstance(k, str) else (k or b"ilus-tickets")

    def _tk_upload_token(tid, ttl_horas=72):
        """Token firmado (HMAC) sin estado en BD -- mismo patron que
        _ot_firma_token. Gatea la subida de adjuntos del formulario publico:
        sin el token de ESTE ticket, nadie puede subir archivos a otro."""
        import hmac as _hmac, hashlib as _hl, base64 as _b64
        exp = int(time.time()) + int(ttl_horas) * 3600
        sig = _hmac.new(_tk_upload_key(), f"{tid}:{exp}".encode(), _hl.sha256).hexdigest()[:24]
        return _b64.urlsafe_b64encode(f"{tid}:{exp}:{sig}".encode()).decode().rstrip("=")

    def _tk_upload_token_validar(token, tid):
        import hmac as _hmac, hashlib as _hl, base64 as _b64
        try:
            if not token or len(token) > 200:
                return False
            raw = _b64.urlsafe_b64decode(token + "=" * (-len(token) % 4)).decode()
            tid_s, exp_s, sig = raw.split(":")
            if int(tid_s) != int(tid):
                return False
            exp = int(exp_s)
            good = _hmac.new(_tk_upload_key(), f"{tid_s}:{exp}".encode(), _hl.sha256).hexdigest()[:24]
            if not _hmac.compare_digest(good, sig) or time.time() > exp:
                return False
            return True
        except Exception:
            return False

    def _tel_chileno_valido(v):
        n = re.sub(r"[^\d]", "", str(v or ""))
        n = n[2:] if n.startswith("56") else n
        return bool(re.match(r"^(9\d{8}|[2-9]\d{7,8})$", n))

    def _email_valido(v):
        return bool(re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]{2,}$", str(v or "").strip()))

    # ── GET /soporte — pagina publica (standalone, sin sidebar) ──
    @app.route("/soporte")
    def tk_soporte_publico():
        return render_template(
            "tickets/soporte_publico.html",
            tk_tipos_publicos=TK_TIPOS_PUBLICOS, tipo_label=TIPO_LABEL,
            max_adjuntos=MAX_ADJUNTOS, max_adjunto_mb=MAX_ADJUNTO_MB)

    # ── GET /soporte/api/erp/productos — catalogo general (read-only) ──
    @app.route("/soporte/api/erp/productos", methods=["GET"])
    def tk_soporte_api_productos():
        if not _rl_ok("prod:" + _ip_cliente(), 30, 60):
            return jsonify({"ok": False, "error": "Demasiadas búsquedas, espera un momento", "resultados": []}), 200
        q = (request.args.get("q") or "").strip()
        if len(q) < 2:
            return jsonify({"ok": True, "resultados": []})
        # Comparte la MISMA query que el buscador interno sin cliente
        # (_buscar_catalogo_bodega, definida arriba) -- una sola fuente de
        # verdad para "catalogo general filtrado por bodega".
        resultados, err = _buscar_catalogo_bodega(q)
        if err:
            return jsonify({"ok": False, "error": err, "resultados": []}), 200
        return jsonify({"ok": True, "resultados": resultados})

    # ── POST /soporte/api/crear — crea el ticket publico ──
    @app.route("/soporte/api/crear", methods=["POST"])
    def tk_soporte_api_crear():
        if not _rl_ok("crear:" + _ip_cliente(), 8, 300):
            return jsonify({"ok": False, "error": "Demasiadas solicitudes desde tu conexión. Intenta en unos minutos."}), 429
        d = request.get_json(silent=True) or {}

        tipo = (d.get("tipo") or "").strip().lower()
        if tipo not in TK_TIPOS_PUBLICOS:
            return jsonify({"ok": False, "error": "Selecciona un tipo de solicitud válido."}), 400

        rut_raw = (d.get("rut") or "").strip()
        rut = rut_raw
        if validar_rut:
            ok_rut, rut_or_msg = validar_rut(rut_raw)
            if not ok_rut:
                return jsonify({"ok": False, "error": f"RUT inválido: {rut_or_msg}"}), 400
            rut = rut_or_msg
        if not rut:
            return jsonify({"ok": False, "error": "Ingresa un RUT válido."}), 400

        nombre_contacto = (d.get("nombre_contacto") or "").strip()
        if not nombre_contacto:
            return jsonify({"ok": False, "error": "Ingresa un nombre de contacto."}), 400

        phone = (d.get("phone") or "").strip()
        if not _tel_chileno_valido(phone):
            return jsonify({"ok": False, "error": "Ingresa un teléfono válido."}), 400

        email = (d.get("email") or "").strip()
        if not _email_valido(email):
            return jsonify({"ok": False, "error": "Ingresa un correo válido."}), 400

        direccion = (d.get("direccion") or "").strip()
        if not direccion:
            return jsonify({"ok": False, "error": "Ingresa una dirección."}), 400
        if d.get("direccion_lat") in (None, "", "null") or d.get("direccion_lng") in (None, "", "null"):
            return jsonify({"ok": False, "error": "Selecciona una dirección sugerida por Google para validarla."}), 400

        productos = d.get("productos") or []  # [{sku, nombre}]
        if not productos:
            return jsonify({"ok": False, "error": "Selecciona al menos un producto."}), 400

        descripcion = (d.get("descripcion") or "").strip()
        if not descripcion:
            return jsonify({"ok": False, "error": "Describe el problema."}), 400

        conn = get_mysql()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO tk_tickets "
                    "(origen, estado, tipo, prioridad, descripcion, rut, empresa, nombre_contacto, "
                    " email, phone, direccion, direccion_lat, direccion_lng, direccion_place_id, "
                    " comuna_nombre, sucursal, producto, sku, numero_documento, created_by) "
                    "VALUES ('form','open',%s,'media',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'cliente')",
                    (tipo, descripcion[:2000], rut[:12],
                     (d.get("empresa") or "").strip()[:150] or None,
                     nombre_contacto[:150], email[:150], phone[:20],
                     direccion[:255],
                     d.get("direccion_lat") or None, d.get("direccion_lng") or None,
                     (d.get("direccion_place_id") or "").strip()[:200] or None,
                     (d.get("comuna_nombre") or "").strip()[:120] or None,
                     (d.get("sucursal") or "").strip()[:100] or None,
                     ", ".join(p.get("nombre", "") for p in productos if p.get("nombre"))[:2000] or None,
                     ", ".join(p.get("sku", "") for p in productos if p.get("sku"))[:500] or None,
                     (d.get("numero_documento") or "").strip()[:500] or None))
                tid = cur.lastrowid
                cur.execute(
                    "UPDATE tk_tickets SET numero_ticket = "
                    "CONCAT('TK-', %s, '-', LPAD(id,5,'0')) WHERE id=%s",
                    (_chile_now_year(), tid))
                for p in productos[:20]:
                    try:
                        cur.execute(
                            "INSERT IGNORE INTO tk_ticket_equipos (ticket_id, erp_kopr, nombre, cantidad) "
                            "VALUES (%s,%s,%s,1)",
                            (tid, (p.get("sku") or "").strip()[:100] or None,
                             (p.get("nombre") or "").strip()[:300] or "Equipo"))
                    except Exception as _e:
                        print(f"[tk_soporte_api_crear] equipo no insertado tid={tid} "
                              f"sku={p.get('sku')}: {_e}", flush=True)
            conn.commit()
        finally:
            conn.close()

        numero_row = mysql_fetchone("SELECT numero_ticket FROM tk_tickets WHERE id=%s", (tid,))
        numero = numero_row["numero_ticket"] if numero_row else None
        _tk_log(tid, "creacion", f"Ticket {numero} creado desde el formulario público", usuario="cliente")
        try:
            _audit("tk_ticket_create_publico", target_type="tk_ticket", target_id=tid,
                   details={"numero": numero, "tipo": tipo, "ip": _ip_cliente()})
        except Exception:
            pass
        try:
            _tk_notificar_lifecycle(tid, "creacion")
        except Exception as _e:
            print(f"[tk_soporte_api_crear] notificacion creacion no enviada tid={tid}: {_e}", flush=True)

        return jsonify({
            "ok": True, "id": tid, "numero_ticket": numero,
            "upload_token": _tk_upload_token(tid),
        })

    # ── POST /soporte/api/adjuntos/<tid> — 1 archivo por request, gated por token ──
    @app.route("/soporte/api/adjuntos/<int:tid>", methods=["POST"])
    def tk_soporte_api_adjuntos(tid):
        if not _rl_ok("adj:" + _ip_cliente(), 30, 300):
            return jsonify({"ok": False, "error": "Demasiadas subidas, espera un momento"}), 429
        token = request.form.get("token") or request.headers.get("X-Upload-Token") or ""
        if not _tk_upload_token_validar(token, tid):
            return jsonify({"ok": False, "error": "Token de subida inválido o expirado"}), 403
        # FIX 2026-07-11: antes exigia origen='form' -- bloqueaba el portal de
        # respuesta del cliente (Daniel) para tickets creados internamente
        # (origen='backoffice'/'erp'). El TOKEN HMAC ya es la proteccion real
        # (solo quien recibio el correo de ESE ticket lo tiene); el origen no
        # agrega seguridad, solo limitaba el caso de uso sin necesidad.
        if not mysql_fetchone("SELECT id FROM tk_tickets WHERE id=%s", (tid,)):
            return jsonify({"ok": False, "error": "Ticket no encontrado"}), 404
        if not _uploader_upload:
            return jsonify({"ok": False, "error": "Almacenamiento no disponible"}), 503

        f = request.files.get("file")
        if not f or not f.filename:
            return jsonify({"ok": False, "error": "No llegó ningún archivo"}), 400

        actuales = mysql_fetchone("SELECT COUNT(*) n FROM tk_adjuntos WHERE ticket_id=%s", (tid,))
        if actuales and int(actuales["n"]) >= MAX_ADJUNTOS:
            return jsonify({"ok": False, "error": f"Máximo {MAX_ADJUNTOS} archivos por solicitud"}), 400

        ext = ("." + f.filename.rsplit(".", 1)[-1].lower()) if "." in f.filename else ""
        if ext not in _EXT_PERMITIDAS:
            return jsonify({"ok": False, "error": f"Tipo de archivo no permitido ({ext or 'sin extensión'})"}), 400
        if not _tk_magic_bytes_ok(ext, f):
            return jsonify({"ok": False, "error": "El contenido del archivo no coincide con su extensión"}), 400

        f.seek(0, 2)
        size_mb = f.tell() / (1024 * 1024)
        f.seek(0)
        if size_mb > MAX_ADJUNTO_MB:
            return jsonify({"ok": False, "error": f"El archivo supera el máximo de {MAX_ADJUNTO_MB} MB"}), 400

        mime = (f.mimetype or "").lower()
        rt = "image" if mime.startswith("image") else ("video" if mime.startswith("video") else "raw")
        try:
            res = _uploader_upload(f, folder="tickets", resource_type=rt)
        except Exception as _e:
            print(f"[tk_soporte_adjuntos] error subiendo tid={tid}: {_e}", flush=True)
            return jsonify({"ok": False, "error": "No se pudo subir el archivo"}), 500
        url = res.get("secure_url") or res.get("url")
        if not url:
            return jsonify({"ok": False, "error": "Subida sin URL"}), 500

        mysql_execute(
            "INSERT INTO tk_adjuntos "
            "(ticket_id, archivo_url, archivo_path, archivo_nombre, mime_type, file_size_kb, origen, subido_por) "
            "VALUES (%s,%s,%s,%s,%s,%s,'form','cliente')",
            (tid, url[:500], (res.get("public_id") or "")[:500] or None,
             f.filename[:300], mime[:150] or None, int(size_mb * 1024)))
        # id del adjunto recien insertado -- lo necesita el portal del cliente
        # para vincularlo al mensaje via _tk_link_adjuntos (adjunto_ids).
        adj_id = None
        try:
            row = mysql_fetchone("SELECT LAST_INSERT_ID() AS id")
            adj_id = int(row["id"]) if row and row.get("id") else None
        except Exception:
            pass
        return jsonify({"ok": True, "id": adj_id, "url": url, "nombre": f.filename})

    # ═══════════════════════════════════════════════════════════════════
    #  PORTAL DE RESPUESTA DEL CLIENTE (Daniel 2026-07-11)
    #  "recibir y enviar mensajes... con toda la calidad de informacion,
    #  imagenes... datos persistentes dentro del ticket". En vez de recepcion
    #  real de correo (requeriria OAuth a Gmail o esperar el DNS de Resend --
    #  Daniel pidio explicitamente dejar Resend fuera por ahora), cada correo
    #  saliente incluye un boton "Responder" a este portal publico: pagina
    #  sin login, gateada por el MISMO token HMAC que ya protege la subida de
    #  adjuntos del formulario publico (_tk_upload_token, vida larga aqui).
    #  El cliente ve el hilo (sin notas internas) y responde con texto +
    #  adjuntos, que quedan en tk_mensajes/tk_adjuntos como cualquier otro
    #  mensaje -- persistente, visible para el staff en la ficha normal.
    # ═══════════════════════════════════════════════════════════════════
    def _tk_portal_url(tid):
        """Link de por vida (1 anio) para el boton 'Responder' de los correos.
        Se regenera solo -- no se guarda en BD, es determinístico por HMAC."""
        token = _tk_upload_token(tid, ttl_horas=24 * 365)
        base = (os.environ.get("ILUS_APP_BASE_URL")
                or "https://ilus-app-469212710544.southamerica-west1.run.app").rstrip("/")
        return f"{base}/portal/ticket/{tid}?token={token}"

    def _tk_boton_portal_html(tid):
        """HTML del boton 'Responder' que se agrega a TODO correo saliente de
        tickets (creacion/resuelto/cerrado/respuesta manual) -- estilos inline
        (los clientes de correo no cargan CSS externo)."""
        url = _tk_portal_url(tid)
        url_esc = _html_mod.escape(url, quote=True)
        return (
            '<div style="text-align:center;margin:24px 0 8px">'
            f'<a href="{url_esc}" style="background:#dc2626;color:#ffffff;text-decoration:none;'
            'font-weight:700;font-size:14px;padding:12px 28px;border-radius:8px;display:inline-block">'
            'Responder en el portal</a></div>'
            '<p style="font-size:12px;color:#9ca3af;text-align:center;margin:8px 0 0">'
            f'O copia este enlace: <a href="{url_esc}" style="color:#dc2626">{url_esc}</a></p>'
        )

    @app.route("/portal/ticket/<int:tid>")
    def tk_portal_ver(tid):
        token = (request.args.get("token") or "").strip()
        if not _tk_upload_token_validar(token, tid):
            return render_template("tickets/portal_error.html"), 403
        if not mysql_fetchone("SELECT id FROM tk_tickets WHERE id=%s", (tid,)):
            return render_template("tickets/portal_error.html"), 404
        return render_template("tickets/portal_cliente.html", tid=tid, token=token,
                                max_adjuntos=MAX_ADJUNTOS, max_adjunto_mb=MAX_ADJUNTO_MB)

    @app.route("/portal/ticket/<int:tid>/datos")
    def tk_portal_datos(tid):
        token = (request.args.get("token") or "").strip()
        if not _tk_upload_token_validar(token, tid):
            return jsonify({"ok": False, "error": "Enlace inválido o expirado"}), 403
        t = mysql_fetchone(
            "SELECT numero_ticket, estado, tipo, empresa, nombre_contacto, created_at "
            "FROM tk_tickets WHERE id=%s", (tid,))
        if not t:
            return jsonify({"ok": False, "error": "Ticket no encontrado"}), 404
        # Solo mensajes NO internos (nunca notas_internas, cambios de estado/
        # asignacion/equipos, ni comentarios marcados como internos por staff).
        mensajes = mysql_fetchall(
            "SELECT id, tipo, contenido, usuario, created_at FROM tk_mensajes "
            "WHERE ticket_id=%s AND es_interno=0 AND tipo IN ('mensaje','client_message','comentario') "
            "ORDER BY created_at ASC, id ASC", (tid,))
        adjuntos = mysql_fetchall(
            "SELECT id, mensaje_id, archivo_url, archivo_nombre, mime_type, created_at FROM tk_adjuntos "
            "WHERE ticket_id=%s ORDER BY id", (tid,))
        return jsonify({
            "ok": True,
            "ticket": {
                "numero_ticket": t["numero_ticket"], "estado": t["estado"],
                "estado_label": ESTADO_LABEL.get(t["estado"], t["estado"]),
                "tipo_label": TIPO_LABEL.get(t["tipo"], t["tipo"] or ""),
                "cliente": t.get("nombre_contacto") or t.get("empresa") or "",
                "creado": _fmt_dt(t.get("created_at")),
            },
            "mensajes": [{
                "id": m["id"], "contenido": m["contenido"], "usuario": m["usuario"],
                "es_cliente": m["tipo"] == "client_message",
                "fecha": _fmt_dt(m.get("created_at")),
            } for m in mensajes],
            "adjuntos": [{"id": a["id"], "mensaje_id": a.get("mensaje_id"),
                          "url": a["archivo_url"], "nombre": a["archivo_nombre"],
                          "mime": a.get("mime_type"), "fecha": _fmt_dt(a.get("created_at"))}
                         for a in adjuntos],
        })

    @app.route("/portal/ticket/<int:tid>/responder", methods=["POST"])
    def tk_portal_responder(tid):
        if not _rl_ok("portal:" + _ip_cliente(), 20, 300):
            return jsonify({"ok": False, "error": "Demasiados envíos, espera un momento"}), 429
        d = request.get_json(silent=True) or {}
        token = (d.get("token") or "").strip()
        if not _tk_upload_token_validar(token, tid):
            return jsonify({"ok": False, "error": "Enlace inválido o expirado"}), 403
        t = mysql_fetchone(
            "SELECT numero_ticket, nombre_contacto, empresa FROM tk_tickets WHERE id=%s", (tid,))
        if not t:
            return jsonify({"ok": False, "error": "Ticket no encontrado"}), 404
        contenido = _sanitizar_html_mensaje((d.get("contenido") or "").strip())
        if not contenido:
            return jsonify({"ok": False, "error": "Escribe un mensaje"}), 400
        adjunto_ids = d.get("adjunto_ids") or []
        cliente_nombre = t.get("nombre_contacto") or t.get("empresa") or "Cliente"
        msg_id = _tk_log(tid, "client_message", contenido[:20000], usuario=cliente_nombre, es_interno=False)
        _tk_link_adjuntos(tid, msg_id, adjunto_ids)
        mysql_execute("UPDATE tk_tickets SET updated_at=NOW() WHERE id=%s", (tid,))
        try:
            _audit("tk_portal_respuesta_cliente", target_type="tk_ticket", target_id=tid,
                   details={"numero": t.get("numero_ticket")})
        except Exception:
            pass
        return jsonify({"ok": True, "mensaje_id": msg_id})

    print("[ILUS] Modulo Tickets central registrado (/tickets).", flush=True)

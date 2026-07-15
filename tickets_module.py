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
import email as _email_mod
import html as _html_mod
import imaplib
import io
import json
import os
import re
import threading
import time
from email.header import decode_header, make_header
from email.utils import parseaddr, parsedate_to_datetime
from functools import wraps
from html.parser import HTMLParser
from datetime import datetime, timezone, date, timedelta

try:
    from zoneinfo import ZoneInfo
    _CL_TZ = ZoneInfo("America/Santiago")
except Exception:  # pragma: no cover
    _CL_TZ = None

from flask import request, jsonify, render_template, redirect, url_for, g

# Validacion server-side del "correo que da la cara" (Reply-To de tickets).
# Mismo patron que app._EMAIL_RE, para no confiar solo en el front (Regla #4).
_TK_REPLY_EMAIL_RE = re.compile(r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$")


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
# Subconjunto que el STAFF puede setear a mano desde el <select> de la ficha.
# Los 3 automaticos (ot_generated/ot_in_progress/ot_pending_approval) los
# controla EXCLUSIVAMENTE _tk_set_estado_automatico (ciclo de vida de la OT
# vinculada) -- ver tk_api_update, que rechaza estos valores via PATCH manual.
TK_ESTADOS_MANUALES = ("open", "in_progress", "pending", "resolved", "closed", "cancelado")
TK_ESTADOS_AUTOMATICOS = ("ot_generated", "ot_in_progress", "ot_pending_approval")
# Estados TERMINALES para efectos de SLA (Daniel 2026-07-14: "nos paso los
# SLA, me tiene que avisar con un estado"): un ticket resuelto/cerrado/
# cancelado ya no corre reloj. Los estados ot_* SI cuentan (sigue abierto).
TK_ESTADOS_CERRADOS = ("resolved", "closed", "cancelado")
# Umbral de SLA por DEFECTO en horas. Orden de precedencia real en runtime
# (ver _tk_sla_horas_umbral): regla de negocio editable 'tk_sla_horas'
# (mant_reglas_negocio, /mantenciones/configuracion) → env TK_SLA_HORAS →
# este default (48 h).
try:
    TK_SLA_HORAS_DEFAULT = max(1, int(float(os.environ.get("TK_SLA_HORAS", "48"))))
except Exception:
    TK_SLA_HORAS_DEFAULT = 48
TK_TIPOS = (
    "install", "tech_support", "shipping", "quotation", "return",
    "tech_evaluation", "maintenance", "spare_parts", "equipment_transfer",
    "warranty", "repair", "spare_parts_store", "spare_parts_import",
    # 2026-07-15 (Daniel): tipos 100% INTERNOS de bodega, sin cliente. Ver
    # TK_TIPOS_SIN_CLIENTE mas abajo -- estos 3 NO entran a TK_TIPOS_PUBLICOS
    # (no se ofrecen en el formulario publico ni en /tickets/nuevo).
    "control_calidad", "trabajo_bodega", "capacitacion",
)
# Tipos internos SIN cliente obligatorio (Daniel 2026-07-15: "internas de
# bodega: control de calidad, trabajos de bodega, capacitacion"). Usado por
# tk_api_create para condicionar la validacion de RUT/empresa/contacto/
# telefono/correo/direccion -- para estos tipos, solo tipo+descripcion son
# obligatorios. El frontend (list.html) debe replicar esta misma condicion
# en validarNtTodo() para no bloquear al usuario con campos que el backend
# ya no exige.
TK_TIPOS_SIN_CLIENTE = ("control_calidad", "trabajo_bodega", "capacitacion")

# 🗓️ FASE 1 (Daniel 2026-07-15, alcance explícito): "hagamoslo por mientras
# solamente con mi correo" -- el correo de confirmación tipo "reserva de
# clínica" que se manda al generar una OT desde un ticket (.ics + botones
# Google/Outlook, patrón calcado de pickups_module._build_pickup_ics) SIEMPRE
# va a este destinatario fijo, NUNCA al técnico real todavía -- eso es una
# fase futura explícitamente fuera de alcance por ahora. Ver
# tk_api_generar_ot / _build_ot_ics / _tk_enviar_confirmacion_ot más abajo.
TK_OT_CONFIRMACION_EMAIL_TEST = "daniel.aguilar@sphs.cl"
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
    # 2026-07-15: tipos internos de bodega (sin cliente) — ver TK_TIPOS_SIN_CLIENTE.
    "control_calidad": "Control de calidad", "trabajo_bodega": "Trabajo de bodega",
    "capacitacion": "Capacitación",
}
# Daniel 2026-07-13: "pienso que los 8 tipos del formulario se quedan
# cortos" -- se amplia a los 12 tipos reales de TK_TIPOS (antes solo se
# exponian 8 al publico, aunque el backoffice ya usaba los 12).
TK_TIPOS_PUBLICOS = (
    "install", "tech_support", "shipping", "quotation", "return",
    "tech_evaluation", "maintenance", "spare_parts", "equipment_transfer",
    "warranty", "repair", "spare_parts_store", "spare_parts_import",
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

# ── Orden por columna del listado (tabla help-desk estilo Triple A) ──
# WHITELIST ESTRICTA param -> expresion SQL. JAMAS se interpola el valor
# crudo del request en el SQL: solo lo que sale de este dict (Regla #4).
_TK_SORT_COLS = {
    "id": "t.id",
    "numero_ticket": "t.numero_ticket",
    "created_at": "t.created_at",
    "updated_at": "t.updated_at",
    "origen": "t.origen",
    "asignado_a": "t.asignado_a",
    "rut": "t.rut",
    "empresa": "t.empresa",
    # estado/prioridad se ordenan por su ORDEN DE NEGOCIO (no alfabetico):
    # asc = abiertos/urgentes primero.
    "estado": ("FIELD(t.estado,'open','in_progress','pending','ot_pending_approval',"
               "'ot_generated','ot_in_progress','resolved','closed','cancelado')"),
    "tipo": "t.tipo",
    "prioridad": "FIELD(t.prioridad,'urgente','alta','media','baja')",
}

# ORDER BY "inteligente" por defecto de la bandeja (comportamiento historico,
# NO cambiar sin permiso): primero tickets con mensajes de cliente sin leer,
# luego estado, prioridad, updated_at DESC.
_TK_ORDER_DEFAULT = (
    "  (SELECT COUNT(*) FROM tk_mensajes m2 "
    "     WHERE m2.ticket_id=t.id AND m2.tipo='client_message' "
    "       AND m2.created_at > COALESCE(t.staff_last_read_at,'1970-01-01')) > 0 DESC, "
    "  FIELD(t.estado,'open','in_progress','pending','ot_pending_approval',"
    "'ot_generated','ot_in_progress','resolved','closed','cancelado'), "
    "FIELD(t.prioridad,'urgente','alta','media','baja'), t.updated_at DESC, t.id DESC"
)

_TK_FECHA_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _tk_tz_offset():
    """Offset ACTUAL de Chile vs UTC como string MySQL ('-04:00' invierno,
    '-03:00' verano) para CONVERT_TZ con offsets numericos (que SI funciona
    sin las tablas de zona horaria de MySQL). Honesto: justo en el borde de
    un cambio de DST el corte del dia puede correr 1 hora para fechas
    historicas -- aceptable para un filtro de fechas de tickets."""
    try:
        if _CL_TZ is not None:
            off = datetime.now(timezone.utc).astimezone(_CL_TZ).utcoffset()
            mins = int(off.total_seconds() // 60)
            sign = "-" if mins < 0 else "+"
            mins = abs(mins)
            return f"{sign}{mins // 60:02d}:{mins % 60:02d}"
    except Exception:
        pass
    return "-04:00"


def _tk_sort_order(args):
    """ORDER BY explicito si viene ?sort= valido (whitelist); None si no.
    dir solo asc|desc (default desc). t.id DESC como desempate SIEMPRE."""
    expr = _TK_SORT_COLS.get((args.get("sort") or "").strip().lower())
    if not expr:
        return None
    direction = "ASC" if (args.get("dir") or "").strip().lower() == "asc" else "DESC"
    return f"{expr} {direction}, t.id DESC"


def _tk_list_where(args):
    """Construye el WHERE del listado de tickets desde los query params.
    COMPARTIDO por tk_api_list y los reportes CSV (una sola fuente de
    verdad, sin duplicar logica). Devuelve (wsql, params) con SQL SIEMPRE
    parametrizado %s (Regla #4)."""
    where, params = [], []
    estado = (args.get("estado") or "").strip().lower()
    if estado in TK_ESTADOS:
        where.append("t.estado=%s"); params.append(estado)
    tipo = (args.get("tipo") or "").strip().lower()
    if tipo in TK_TIPOS:
        where.append("t.tipo=%s"); params.append(tipo)
    prio = (args.get("prioridad") or "").strip().lower()
    if prio in TK_PRIORIDADES:
        where.append("t.prioridad=%s"); params.append(prio)
    origen = (args.get("origen") or "").strip().lower()
    if origen in TK_ORIGENES:
        where.append("t.origen=%s"); params.append(origen)
    asign = (args.get("asignado_a") or "").strip()
    if asign == "__sin_asignar__":
        # Sentinel del select "Responsable" de la bandeja (frontend list.html):
        # tickets SIN ejecutivo responsable (NULL o vacio). Aplica tambien a
        # los reportes CSV porque comparten este WHERE.
        where.append("(t.asignado_a IS NULL OR t.asignado_a='')")
    elif asign:
        where.append("t.asignado_a=%s"); params.append(asign)
    rut = (args.get("rut") or "").strip()
    if rut:
        where.append("t.rut LIKE %s"); params.append(f"%{rut}%")
    q = (args.get("q") or "").strip()
    if q:
        like = f"%{q}%"
        where.append(
            "(t.numero_ticket LIKE %s OR t.empresa LIKE %s OR t.nombre_contacto LIKE %s "
            "OR t.descripcion LIKE %s OR t.titulo LIKE %s OR t.rut LIKE %s)"
        )
        params.extend([like, like, like, like, like, like])

    # ── Filtros nuevos (tabla help-desk) ──
    # ticket: id exacto (si es numerico) O numero_ticket parcial, para que
    # "355" y "TK-2026-00355" encuentren el mismo ticket.
    ticket = (args.get("ticket") or "").strip()
    if ticket:
        if ticket.isdigit():
            where.append("(t.id=%s OR t.numero_ticket LIKE %s)")
            params.extend([int(ticket), f"%{ticket}%"])
        else:
            where.append("t.numero_ticket LIKE %s")
            params.append(f"%{ticket}%")

    # Rango de fechas sobre created_at, pensado en HORA CHILE (Regla #6):
    # created_at se guarda en UTC -> se convierte con CONVERT_TZ + offset
    # numerico actual antes de tomar el DATE(). hoy=1 es el atajo de
    # fecha_desde=fecha_hasta=hoy (hora Chile).
    fecha_desde = (args.get("fecha_desde") or "").strip()
    fecha_hasta = (args.get("fecha_hasta") or "").strip()
    if (args.get("hoy") or "").strip() == "1":
        fecha_desde = fecha_hasta = _chile_hoy().isoformat()
    tz_off = _tk_tz_offset()
    if fecha_desde and _TK_FECHA_RE.match(fecha_desde):
        where.append("DATE(CONVERT_TZ(t.created_at, '+00:00', %s)) >= %s")
        params.extend([tz_off, fecha_desde])
    if fecha_hasta and _TK_FECHA_RE.match(fecha_hasta):
        where.append("DATE(CONVERT_TZ(t.created_at, '+00:00', %s)) <= %s")
        params.extend([tz_off, fecha_hasta])

    wsql = (" WHERE " + " AND ".join(where)) if where else ""
    return wsql, params

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
    normalizar_rut = ctx.get("normalizar_rut")
    _audit = ctx.get("_audit") or (lambda *a, **k: None)
    chile_fmt = ctx.get("chile_fmt")
    # ERP read-only: reusamos los helpers PROBADOS de app.py (pymssql directo a
    # SQL Server sobre MAEEN/MAEDDO/MAEEDO). NO la REST API (que no responde en prod).
    _erp_buscar_clientes = ctx.get("_erp_buscar_clientes")
    _random_sql_query = ctx.get("_random_sql_query")
    _rut_cuerpo = ctx.get("_rut_cuerpo")
    # Resolver de comuna (TABCM) + dirección: mismo motor ya probado que usa
    # el resto de ILUS para direcciones ERP (Regla #4.1: solo SELECT).
    _resolve_comuna_erp = ctx.get("_resolve_comuna_erp")
    # 2026-07-12 (Daniel): "los otros [tickets] seguimos teniendo vacia la
    # region... para eso es Google, para separarlo" -- TABCM (arriba) NO
    # trae region, asi que los tickets creados server-side (desde documento
    # ERP, automatizacion) usan Google Geocoding para resolver Region +
    # Comuna (mismo criterio de administrative_area_level_1/locality que ya
    # usa el navegador via ilusPlacesAutocomplete en el modal manual).
    _google_geocode_region_comuna = ctx.get("_google_geocode_region_comuna")
    # "Generar OT" (Tickets -> mant_visitas real): reusa el motor de OTs de
    # Mantenciones tal cual, sin duplicar logica (Regla #4 / arquitectura
    # acordada: estas funciones viven en app.py, un solo lugar versionado).
    _next_ot_number = ctx.get("_next_ot_number")
    _next_ot_number_atomic = ctx.get("_next_ot_number_atomic")
    _validar_disponibilidad_visita = ctx.get("_validar_disponibilidad_visita")
    _normalize_hora = ctx.get("_normalize_hora")
    _parse_garantia_aplica = ctx.get("_parse_garantia_aplica")
    _mapear_garantia_a_cobertura = ctx.get("_mapear_garantia_a_cobertura")
    _mant_log = ctx.get("_mant_log") or (lambda *a, **k: None)
    # Nucleo COMPARTIDO de creacion de Levantamiento/OT (app.py) -- el mismo
    # que usa el modal real de Mantenciones (#modalLevSelector). El wrapper
    # "Generar OT" de Tickets (mas abajo) NO reimplementa el INSERT en
    # mant_visitas -- arma el payload y delega aqui (Regla #4).
    _mant_lev_crear_ot_core = ctx.get("_mant_lev_crear_ot_core")

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

    # Buzon de soporte de Tickets (Daniel 2026-07-12): "necesito que el cliente
    # vea su version en Gmail... y me responda por correo, y eso lo vea
    # reflejado yo en el ticket". El Reply-To de marca compartido apunta a
    # soportetec@sphs.cl (usado por retiros/transporte/mantenciones) -- Tickets
    # necesita un Reply-To propio, apuntando al buzon que se integrara con
    # Gmail API para leer las respuestas de los clientes (pendiente: cuenta de
    # servicio + delegacion de dominio en Google Workspace, autorizada por un
    # super-admin -- ver conversacion). Ahora es EDITABLE EN VIVO desde
    # Comunicaciones -> Tickets (tabla tk_settings, clave 'reply_to') sin
    # depender de una env var ni de un deploy (Daniel 2026-07-12: "necesito
    # que en la parte de comunicaciones si configures cual es el correo que va
    # a estar dando la cara"). _tk_reply_to() resuelve el valor efectivo en
    # cada envio (1 SELECT liviano) con prioridad tk_settings -> env -> default.
    def _tk_reply_to():
        """Correo que da la cara (Reply-To) de los correos de TICKETS.
        Prioridad: (1) tk_settings.reply_to si existe y no esta vacio,
        (2) env var TK_SUPPORT_REPLY_TO, (3) default daniel.aguilar@sphs.cl.
        Independiente del Reply-To de marca GLOBAL (comm_brand) que usan
        retiros/transporte/mantenciones."""
        try:
            row = mysql_fetchone(
                "SELECT valor FROM tk_settings WHERE clave='reply_to'")
            if row:
                v = (row.get("valor") or "").strip()
                if v:
                    return v
        except Exception as _e:
            print(f"[_tk_reply_to] fallback a env/default: {_e}", flush=True)
        return (os.environ.get("TK_SUPPORT_REPLY_TO") or "daniel.aguilar@sphs.cl").strip()

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

    # ─────────────────────────────────────────────────────────────────
    #  ERP — motor UNICO para "documento -> lineas con saldo real"
    #  (Daniel 2026-07-12: "que se busque por factura, que se busque por
    #  RUT y que se asigne... que vea si tiene saldo o no tiene saldo,
    #  tal cual como lo haciamos en los retiros").
    #
    #  _cubicador_fetch (app.py) es el MISMO motor que usan retiros/
    #  cubicador/asignar/mantenciones: ya calcula el saldo oficial por
    #  linea (CAPRCO1-CAPRAD1-CAPREX1-CAPRNC1, forzado a 0 si ESLIDO
    #  indica despacho total) y ya filtra/marca servicios ZZ. Reusarlo
    #  aca evita duplicar esa formula por TERCERA vez (Regla: un solo
    #  motor ERP). Fallback a erp_engine.fetch_document (sin saldo real,
    #  saldo=cantidad total) SOLO si _cubicador_fetch no esta disponible,
    #  para que el flujo nunca quede muerto.
    # ─────────────────────────────────────────────────────────────────
    def _tk_fetch_doc_lineas(tido, nudo):
        """Trae cabecera+lineas de un documento ERP con saldo por linea.

        Devuelve (hdr:dict|None, lineas:list[dict], via:str).
        hdr:    {cliente_nombre, cliente_rut, email, telefono, direccion,
                 comuna, fecha}
        lineas: [{sku, nombre, cantidad, saldo, es_zz}, ...]
        """
        try:
            from app import _cubicador_fetch
            hdr_raw, lineas_raw = _cubicador_fetch((tido or "").strip().upper(), (nudo or "").strip())
        except Exception as _e:
            print(f"[_tk_fetch_doc_lineas] _cubicador_fetch fallo {tido}/{nudo}: {_e}", flush=True)
            hdr_raw, lineas_raw = None, None
        if hdr_raw:
            lineas = []
            for ln in (lineas_raw or []):
                sku = (ln.get("sku") or "").strip()
                if not sku:
                    continue
                nombre = (ln.get("descripcion_erp") or ln.get("nombre_app") or "").strip() or "Equipo"
                try:
                    cantidad = float(ln.get("cantidad") or 0)
                except Exception:
                    cantidad = 0.0
                try:
                    saldo = float(ln.get("saldo") or 0)
                except Exception:
                    saldo = 0.0
                lineas.append({"sku": sku, "nombre": nombre, "cantidad": cantidad,
                                "saldo": saldo, "es_zz": bool(ln.get("es_zz"))})
            hdr = {
                "cliente_nombre": hdr_raw.get("cliente_nombre") or "",
                "cliente_rut":    hdr_raw.get("cliente_rut") or "",
                "email":          hdr_raw.get("email") or "",
                "telefono":       hdr_raw.get("telefono") or "",
                "direccion":      hdr_raw.get("direccion") or "",
                "comuna":         hdr_raw.get("comuna") or "",
                "fecha":          hdr_raw.get("fecha") or "",
            }
            return hdr, lineas, "cubicador"

        # Fallback: erp_engine (motor viejo, sin saldo real -- se asume
        # saldo = cantidad total de la linea).
        try:
            import erp_engine
            doc = erp_engine.get_client().fetch_document((tido or "").strip().upper(), (nudo or "").strip())
        except Exception as _e:
            print(f"[_tk_fetch_doc_lineas] erp_engine fallo {tido}/{nudo}: {_e}", flush=True)
            return None, [], "error"
        if not doc:
            return None, [], "not_found"
        lineas = []
        for ln in (doc.get("lineas_raw") or []):
            sku = str(ln.get("KOPRCT") or ln.get("koprct") or "").strip()
            nombre = str(ln.get("NOKOPR") or ln.get("nokopr") or "").strip()
            if not (sku or nombre):
                continue
            try:
                cantidad = float(ln.get("CAPRCO1") or ln.get("caprco1") or 1)
            except Exception:
                cantidad = 1.0
            lineas.append({"sku": sku, "nombre": nombre or "Equipo", "cantidad": cantidad,
                            "saldo": cantidad, "es_zz": sku.upper().startswith("ZZ")})
        hdr = {
            "cliente_nombre": doc.get("cliente_nombre") or "",
            "cliente_rut":    doc.get("cliente_rut") or "",
            "email":          doc.get("email") or "",
            "telefono":       doc.get("telefono") or "",
            "direccion":      doc.get("direccion") or "",
            "comuna":         doc.get("comuna") or "",
            "fecha":          doc.get("fecha") or "",
        }
        return hdr, lineas, "erp_engine_fallback"

    def _tk_filtrar_lineas_seleccion(lineas_reales, seleccion):
        """Aplica una seleccion GRANULAR de lineas (checkboxes del modal)
        sobre las lineas REALES del documento (nunca confiar a ciegas en
        lo que manda el navegador -- la cantidad pedida se clampa contra
        la cantidad real del ERP).

        seleccion: [{sku, cantidad|qty, nombre?}, ...] (del frontend).
        Si viene vacia/None: comportamiento HISTORICO -- todas las lineas
        no-ZZ del documento (para no romper llamadas viejas sin seleccion).
        """
        no_zz = [l for l in lineas_reales if not l.get("es_zz")]
        if not seleccion:
            return no_zz
        by_sku = {l["sku"].upper(): l for l in no_zz if l.get("sku")}
        out = []
        for sel in seleccion:
            sku = str((sel or {}).get("sku") or "").strip()
            if not sku:
                continue
            real = by_sku.get(sku.upper())
            if not real:
                continue  # SKU que no pertenece a este documento -- se ignora
            qty_raw = sel.get("cantidad")
            if qty_raw is None:
                qty_raw = sel.get("qty")
            try:
                qty_pedida = float(qty_raw or 0)
            except Exception:
                qty_pedida = 0.0
            cantidad_real = real.get("cantidad") or 0
            qty_final = max(0.0, min(qty_pedida, cantidad_real)) if cantidad_real else 0.0
            if qty_final <= 0:
                continue
            out.append({"sku": real["sku"],
                        "nombre": (sel.get("nombre") or real.get("nombre") or "Equipo"),
                        "cantidad": qty_final,
                        # Daniel 2026-07-12: "la gerencia tiene que saber el
                        # por que... vamos a necesitar un comentario... todo
                        # registrado con el historial". marcada_sin_saldo se
                        # RECALCULA aca contra el saldo REAL (no se confia en
                        # lo que afirma el frontend); motivo_sin_saldo es la
                        # justificacion que el usuario escribio en el modal.
                        "marcada_sin_saldo": (real.get("saldo") or 0) <= 0,
                        "motivo_sin_saldo": str((sel or {}).get("motivo_sin_saldo") or "").strip()[:500]})
        return out

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
    # Reglas de negocio editables (mant_reglas_negocio, /mantenciones/
    # configuracion) -- para el umbral de SLA de tickets ('tk_sla_horas').
    _reglas_cargar = ctx.get("_reglas_cargar")
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
                              "message_date", "staff_last_read_at",
                              "visto_at", "primera_vez"),
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
    #  SLA (Daniel 2026-07-14, URGENTE: "nos paso los SLA, me tiene que
    #  avisar con un estado"). El backend expone 2 campos CALCULADOS por
    #  ticket (sla_horas / sla_vencido) + el umbral vigente
    #  (sla_umbral_horas) en tk_api_list y tk_api_get; el front los pinta.
    #  NO se agrega columna nueva a tk_tickets: se calcula al vuelo desde
    #  created_at (que MySQL guarda en UTC via NOW(), Regla #6).
    # ─────────────────────────────────────────────────────────────────
    def _tk_sla_horas_umbral():
        """Umbral de SLA en horas. Precedencia: regla editable
        'tk_sla_horas' (mant_reglas_negocio) → env TK_SLA_HORAS →
        default 48. Jamas rompe: ante cualquier error cae al default."""
        if _reglas_cargar:
            try:
                v = (_reglas_cargar() or {}).get("tk_sla_horas")
                if v is not None:
                    v = int(float(v))
                    if v > 0:
                        return v
            except Exception as _e:
                print(f"[tk_sla] regla tk_sla_horas ilegible: {_e}", flush=True)
        return TK_SLA_HORAS_DEFAULT

    def _tk_sla_info(estado, created_at, umbral_horas):
        """(sla_horas, sla_vencido) para un ticket. created_at debe ser el
        datetime CRUDO de MySQL (UTC naive, ANTES de pasar por _fmt_row).
        Estados terminales (TK_ESTADOS_CERRADOS) no corren reloj →
        (None, False). Defensivo: si la fecha no se puede leer, (None, False)."""
        if (estado or "") in TK_ESTADOS_CERRADOS:
            return (None, False)
        try:
            if isinstance(created_at, str):
                created_at = datetime.strptime(created_at[:19], "%Y-%m-%d %H:%M:%S")
            if not isinstance(created_at, datetime):
                return (None, False)
            horas = (datetime.utcnow() - created_at).total_seconds() / 3600.0
            horas = round(max(0.0, horas), 1)
            return (horas, bool(horas > float(umbral_horas)))
        except Exception as _e:
            print(f"[tk_sla] created_at ilegible ({created_at!r}): {_e}", flush=True)
            return (None, False)

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
                                     'warranty','repair','spare_parts_store','spare_parts_import',
                                     'control_calidad','trabajo_bodega','capacitacion') NULL,
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
        # 2026-07-13 (Daniel, URGENTE): tabla de rutas a nivel nacional
        # (origen QUILICURA -> comuna destino), para calcular el "costo_ruta"
        # de tk_cotizaciones automaticamente segun la comuna del cliente.
        # Se importa desde el CSV real que Daniel entrego (idempotente por
        # csv_id, mismo patron que el import de repuestos).
        mysql_execute("""
            CREATE TABLE IF NOT EXISTS tk_cotiz_rutas (
              id             INT AUTO_INCREMENT PRIMARY KEY,
              csv_id         INT NULL UNIQUE,
              origen         VARCHAR(100) NOT NULL DEFAULT 'QUILICURA',
              region         VARCHAR(100) NULL,
              comuna         VARCHAR(100) NOT NULL,
              km             DECIMAL(8,1) NOT NULL DEFAULT 0,
              peaje          INT NOT NULL DEFAULT 0,
              tag            INT NOT NULL DEFAULT 0,
              precio_bruto   INT NOT NULL DEFAULT 0,
              precio_final   INT NOT NULL DEFAULT 0,
              tiempo_min     INT NOT NULL DEFAULT 0,
              activa         TINYINT(1) NOT NULL DEFAULT 1,
              notas          VARCHAR(500) NULL,
              creada_csv     DATETIME NULL,
              actualizada_csv DATETIME NULL,
              created_at     DATETIME DEFAULT CURRENT_TIMESTAMP,
              updated_at     DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
              KEY idx_comuna (comuna),
              KEY idx_region (region)
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
        # Settings clave-valor del modulo Tickets (Daniel 2026-07-12): guarda el
        # "correo que da la cara" (Reply-To de los correos de tickets) editable
        # desde Comunicaciones sin depender de una env var ni de un deploy. Es
        # PROPIO de tickets — NO se reutiliza comm_brand (ese es el Reply-To de
        # marca GLOBAL de todos los modulos). Se siembra la clave 'reply_to' con
        # INSERT IGNORE para no pisar un valor ya editado por el admin.
        mysql_execute("""
            CREATE TABLE IF NOT EXISTS tk_settings (
              clave       VARCHAR(64) PRIMARY KEY,
              valor       VARCHAR(255) NULL,
              updated_by  VARCHAR(190) NULL,
              updated_at  DATETIME DEFAULT CURRENT_TIMESTAMP
                          ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        try:
            mysql_execute(
                "INSERT IGNORE INTO tk_settings (clave, valor) VALUES ('reply_to', %s)",
                ("daniel.aguilar@sphs.cl",))
        except Exception as _e:
            print(f"[ILUS][WARN] seed tk_settings.reply_to: {_e}", flush=True)

        # Deduplicacion del lector IMAP de respuestas de clientes (Daniel
        # 2026-07-12: "responde el correo, tu ubicas el asunto y listo").
        # Cada correo ingresado se registra por su Message-ID; si el lector
        # vuelve a ver el mismo correo (corre cada pocos minutos sobre una
        # ventana de dias), lo salta. PRIMARY KEY = idempotencia real.
        mysql_execute("""
            CREATE TABLE IF NOT EXISTS tk_mail_ingeridos (
              message_id  VARCHAR(255) PRIMARY KEY,
              ticket_id   INT NULL,
              from_email  VARCHAR(190) NULL,
              subject     VARCHAR(300) NULL,
              created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
              KEY idx_ticket (ticket_id)
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
        # 2026-07-13 (Daniel, URGENTE): "que mande... el codigo postal aparte"
        # -- Google Places lo trae como componente 'postal_code', separado de
        # comuna/region (ya soportados). Columna propia para no perderlo.
        if "codigo_postal" not in existentes:
            alters.append("ADD COLUMN codigo_postal VARCHAR(20) NULL")
        for a in alters:
            try:
                mysql_execute(f"ALTER TABLE tk_tickets {a}")
            except Exception as _e:
                print(f"[ILUS][WARN] ALTER tk_tickets {a}: {_e}", flush=True)
        try:
            mysql_execute("CREATE UNIQUE INDEX uq_tk_legacy_taa ON tk_tickets (legacy_taa_id)")
        except Exception:
            pass  # ya existe
        # 2026-07-15 (Daniel): 3 tipos nuevos 100% internos de bodega
        # (control_calidad/trabajo_bodega/capacitacion — ver TK_TIPOS_SIN_CLIENTE).
        # MODIFY COLUMN con el ENUM ampliado es idempotente y seguro de correr
        # en TODO boot (patron ya usado en app.py, ej. mant_visitas.tipo,
        # mant_clientes.tipo_cliente): amplia el ENUM preservando filas
        # existentes, nunca las trunca. Sin esto, producción (donde la tabla
        # ya existe) rechazaría el INSERT con "Data truncated for column 'tipo'"
        # porque el CREATE TABLE IF NOT EXISTS de arriba no altera tablas ya
        # creadas.
        try:
            mysql_execute(
                "ALTER TABLE tk_tickets MODIFY COLUMN tipo "
                "  ENUM('install','tech_support','shipping','quotation','return',"
                "       'tech_evaluation','maintenance','spare_parts','equipment_transfer',"
                "       'warranty','repair','spare_parts_store','spare_parts_import',"
                "       'control_calidad','trabajo_bodega','capacitacion') NULL"
            )
        except Exception as _e:
            print(f"[ILUS][WARN] ALTER tk_tickets MODIFY tipo (tipos internos bodega): {_e}", flush=True)

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

    def _ensure_tk_ticket_equipos_garantia_columns():
        """Migracion aditiva de tk_ticket_equipos (patron _ensure_tk_tickets_columns):
        con_garantia/documento_garantia/fecha_emision/garantia_meses/fecha_vencimiento
        para registrar la garantia de CADA equipo agregado a un ticket. Legal: 6 meses
        por defecto (ley del consumidor chilena para electrodomesticos), editable caso
        a caso porque un proveedor puede dar mas."""
        try:
            existentes = {r["COLUMN_NAME"] for r in mysql_fetchall(
                "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='tk_ticket_equipos'")}
        except Exception as _e:
            print(f"[ILUS][WARN] _ensure_tk_ticket_equipos_garantia_columns (schema check): {_e}", flush=True)
            return
        alters = []
        if "con_garantia" not in existentes:
            alters.append("ADD COLUMN con_garantia TINYINT(1) NOT NULL DEFAULT 0")
        if "documento_garantia" not in existentes:
            alters.append("ADD COLUMN documento_garantia VARCHAR(150) NULL")
        if "fecha_emision" not in existentes:
            alters.append("ADD COLUMN fecha_emision DATE NULL")
        if "garantia_meses" not in existentes:
            alters.append("ADD COLUMN garantia_meses INT NOT NULL DEFAULT 6")
        if "fecha_vencimiento" not in existentes:
            alters.append("ADD COLUMN fecha_vencimiento DATE NULL")
        for a in alters:
            try:
                mysql_execute(f"ALTER TABLE tk_ticket_equipos {a}")
            except Exception as _e:
                print(f"[ILUS][WARN] ALTER tk_ticket_equipos {a}: {_e}", flush=True)

    def _ensure_tk_tickets_visita_link():
        """Blinda tk_tickets.visita_id (patron _ensure_tk_tickets_columns) +
        UNIQUE index para que 1 ticket no pueda vincularse a 2 visitas.
        `visita_id` ya viene en el CREATE TABLE original de
        _ensure_tickets_tables() -- este _ensure_* es solo por si alguna
        instancia de prod creo la tabla antes de que ese campo se agregara,
        y para garantizar el UNIQUE index (Regla #5) siempre, incluso con
        ILUS_SKIP_MIGRATIONS=1."""
        try:
            existentes = {r["COLUMN_NAME"] for r in mysql_fetchall(
                "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='tk_tickets'")}
        except Exception as _e:
            print(f"[ILUS][WARN] _ensure_tk_tickets_visita_link (schema check): {_e}", flush=True)
            return
        if "visita_id" not in existentes:
            try:
                mysql_execute("ALTER TABLE tk_tickets ADD COLUMN visita_id INT NULL")
            except Exception as _e:
                print(f"[ILUS][WARN] ALTER tk_tickets ADD visita_id: {_e}", flush=True)
        try:
            mysql_execute("CREATE UNIQUE INDEX uq_tk_tickets_visita ON tk_tickets (visita_id)")
        except Exception:
            pass  # ya existe (MySQL permite multiples NULL en UNIQUE)

    def _ensure_tk_zz_instalacion_scan_table():
        """Tabla de control de idempotencia del automatismo 'ZZ-Instalacion'
        (Daniel 2026-07-12): registra que documento(s) ERP (tido+nudo) ya
        fueron revisados por el escaneo de instalaciones, para no crear el
        mismo ticket 2 veces aunque el boton se apriete varias veces o el
        primer intento haya fallado a mitad de camino (ticket_id nullable:
        se reintenta mientras siga NULL). Solo LECTURA contra el ERP -- esta
        tabla vive en MySQL Clever Cloud (Regla #4.1, el ERP nunca se toca)."""
        mysql_execute("""
            CREATE TABLE IF NOT EXISTS tk_zz_instalacion_scan (
              id          INT AUTO_INCREMENT PRIMARY KEY,
              tido        VARCHAR(10) NOT NULL,
              nudo        VARCHAR(40) NOT NULL,
              fecha_doc   DATETIME NULL,
              ticket_id   INT NULL,
              creado_at   DATETIME DEFAULT CURRENT_TIMESTAMP,
              creado_por  VARCHAR(190) NULL,
              UNIQUE KEY uq_tk_zzscan_doc (tido, nudo),
              KEY idx_tk_zzscan_fecha (fecha_doc),
              KEY idx_tk_zzscan_ticket (ticket_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)

    def _ensure_import_rutas_csv_daniel():
        """Importa (idempotente, por csv_id) el CSV real de rutas nacionales
        que Daniel entrego: rutas_2026-07-13.csv (253 filas, separador ';',
        BOM UTF-8). Columnas: ID;Origen;Region;Comuna;Km;Peaje;Tag;
        PrecioBruto;PrecioFinal;Min;Activa;Notas;Creada;Actualizada.

        FIX de encoding (Daniel: "creo que la ñ esta mal"): el CSV real trae
        3 filas con "¥" en vez de "Ñ" (HUALA¥E, DO¥IHUE, VICU¥A) -- mojibake
        de un guardado previo en una codificacion distinta a UTF-8. Se
        corrige con un reemplazo literal antes de insertar.

        Ruta configurable via env RUTAS_CSV_PATH; si no existe el archivo
        (ej. en Cloud Run), no hace nada -- mismo patron que el import de
        repuestos (corre en el proximo boot donde el archivo este presente)."""
        import csv as _csv
        import os as _os
        from datetime import datetime as _dt

        path = _os.environ.get(
            "RUTAS_CSV_PATH", r"C:\Users\DANIE\Downloads\rutas_2026-07-13.csv"
        )
        if not _os.path.isfile(path):
            return 0

        def _fix_enye(s):
            return (s or "").replace("¥", "Ñ")

        def _parse_fecha(s):
            s = (s or "").strip()
            if not s:
                return None
            try:
                return _dt.strptime(s, "%d-%m-%Y %H:%M")
            except Exception:
                return None

        def _num(s):
            s = (s or "").strip()
            try:
                return float(s) if s else 0.0
            except Exception:
                return 0.0

        importados = 0
        with open(path, "r", encoding="utf-8-sig", newline="") as f:
            reader = _csv.DictReader(f, delimiter=";")
            for row in reader:
                csv_id = (row.get("ID") or "").strip()
                if not csv_id:
                    continue
                try:
                    csv_id_int = int(csv_id)
                except ValueError:
                    continue
                ya = mysql_fetchone(
                    "SELECT id FROM tk_cotiz_rutas WHERE csv_id=%s", (csv_id_int,))
                if ya:
                    continue  # idempotente: ya importado en un boot anterior
                try:
                    mysql_execute(
                        "INSERT INTO tk_cotiz_rutas "
                        "(csv_id, origen, region, comuna, km, peaje, tag, "
                        " precio_bruto, precio_final, tiempo_min, activa, notas, "
                        " creada_csv, actualizada_csv) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                        (csv_id_int,
                         _fix_enye((row.get("Origen") or "").strip())[:100] or "QUILICURA",
                         _fix_enye((row.get("Region") or "").strip())[:100] or None,
                         _fix_enye((row.get("Comuna") or "").strip())[:100],
                         _num(row.get("Km")), int(_num(row.get("Peaje"))),
                         int(_num(row.get("Tag"))), int(_num(row.get("PrecioBruto"))),
                         int(_num(row.get("PrecioFinal"))), int(_num(row.get("Min"))),
                         1 if (row.get("Activa") or "").strip().lower() == "true" else 0,
                         (row.get("Notas") or "").strip()[:500] or None,
                         _parse_fecha(row.get("Creada")), _parse_fecha(row.get("Actualizada"))))
                    importados += 1
                except Exception as _e:
                    print(f"[rutas_import] fila csv_id={csv_id_int} no importada: {_e}", flush=True)
        return importados

    def _ensure_tk_cotizaciones_columns():
        """Migracion aditiva de tk_cotizaciones (patron _ensure_transporte_columns/
        _ensure_tk_tickets_columns): agrega email/telefono si faltan.

        Blueprint Cotizaciones Fase 1 (2026-07-15, Daniel): "traer los datos
        reales del cliente... hoy quedan siempre NULL, es un bug confirmado".
        empresa/rut YA existen en el CREATE TABLE original -- solo faltaban
        email/telefono para poder guardar el header completo que ahora manda
        el modal (_tka_modal.html) junto a los items seleccionados."""
        try:
            existentes = {r["COLUMN_NAME"] for r in mysql_fetchall(
                "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='tk_cotizaciones'")}
        except Exception as _e:
            print(f"[ILUS][WARN] _ensure_tk_cotizaciones_columns (schema check): {_e}", flush=True)
            return
        alters = []
        if "email" not in existentes:
            alters.append("ADD COLUMN email VARCHAR(190) NULL")
        if "telefono" not in existentes:
            alters.append("ADD COLUMN telefono VARCHAR(50) NULL")
        for a in alters:
            try:
                mysql_execute(f"ALTER TABLE tk_cotizaciones {a}")
            except Exception as _e:
                print(f"[ILUS][WARN] ALTER tk_cotizaciones {a}: {_e}", flush=True)

    def _ensure_tk_cotizacion_items_columns():
        """Migracion aditiva de tk_cotizacion_items: clase_producto (snapshot
        denormalizado de cat_productos.clase_producto al momento de agregar
        el item, igual criterio que Triple A copia el precio del documento --
        snapshot, no referencia viva) + vaneli_original (valor de linea que
        trajo el ERP, para trazabilidad; NULL si el modal aun no lo expone).
        Ver Blueprint Cotizaciones Fase 1, §2/§3.2.C."""
        try:
            existentes = {r["COLUMN_NAME"] for r in mysql_fetchall(
                "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='tk_cotizacion_items'")}
        except Exception as _e:
            print(f"[ILUS][WARN] _ensure_tk_cotizacion_items_columns (schema check): {_e}", flush=True)
            return
        alters = []
        if "clase_producto" not in existentes:
            alters.append("ADD COLUMN clase_producto VARCHAR(30) NULL")
        if "vaneli_original" not in existentes:
            alters.append("ADD COLUMN vaneli_original INT NULL")
        for a in alters:
            try:
                mysql_execute(f"ALTER TABLE tk_cotizacion_items {a}")
            except Exception as _e:
                print(f"[ILUS][WARN] ALTER tk_cotizacion_items {a}: {_e}", flush=True)

    def _ensure_tk_sla_regla():
        """Siembra la regla editable 'tk_sla_horas' en mant_reglas_negocio
        (mismo patron INSERT IGNORE que _ensure_reglas_terreno de app.py:
        jamas pisa lo que Daniel edite en /mantenciones/configuracion).
        Si la tabla aun no existe en este boot, el try/except lo absorbe y
        el modulo cae al env TK_SLA_HORAS / default 48 sin romper nada."""
        try:
            mysql_execute(
                "INSERT IGNORE INTO mant_reglas_negocio "
                "(clave, valor, tipo_dato, categoria, label, unidad, orden) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s)",
                ("tk_sla_horas", str(TK_SLA_HORAS_DEFAULT), "int", "tickets",
                 "SLA de tickets: horas maximas desde la creacion antes de marcarse vencido",
                 "horas", 10))
        except Exception as _e:
            print(f"[ILUS][WARN] seed tk_sla_horas: {_e}", flush=True)

    with app.app_context():
        try:
            _ensure_tickets_tables()
            _n_rutas = _ensure_import_rutas_csv_daniel()
            if _n_rutas:
                print(f"[ILUS] Rutas nacionales importadas: {_n_rutas}", flush=True)
            _ensure_tk_tickets_columns()
            _ensure_tk_mensajes_columns()
            _ensure_tk_ticket_equipos_garantia_columns()
            _ensure_tk_tickets_visita_link()
            _ensure_tk_zz_instalacion_scan_table()
            _ensure_tk_cotizaciones_columns()
            _ensure_tk_cotizacion_items_columns()
            _ensure_tk_sla_regla()
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
        """Gate de Tickets: acepta el permiso legacy 'mantenciones' (o
        superadmin) Y, de forma puramente aditiva (2026-07-12), los flags
        dedicados de la matriz de roles tk_ver/tk_es_tecnico/tk_es_ejecutivo
        (módulo "tickets" en PERMISSIONS_MATRIX, app.py). El OR con
        "mantenciones" se mantiene para no quitarle acceso a nadie que hoy
        ya entra por ese camino — ver REGLA #4.2 en CLAUDE.md."""
        @wraps(view)
        def wrapped(*a, **k):
            perms = g.get("permissions") or {}
            if not (
                perms.get("mantenciones")
                or perms.get("tk_ver")
                or perms.get("tk_es_tecnico")
                or perms.get("tk_es_ejecutivo")
                or perms.get("superadmin")
            ):
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
                to_email=None, cc_email=None, estado_envio=None, message_date=None):
        """Escribe un evento/mensaje en tk_mensajes. Nunca rompe el flujo.
        Devuelve el id del mensaje insertado (o None si fallo) -- lo usan
        responder-cliente/comentario para vincular adjuntos al mensaje.
        message_date: fecha REAL del mensaje (ej. header Date del correo del
        cliente) -- distinta de created_at (hora de INGESTA/registro). Sin
        esto el hilo se ordena por cuando el barrido IMAP alcanzo a leer el
        correo, no por cuando el cliente realmente lo envio."""
        base_user = usuario or (current_username() or "sistema")
        try:
            mysql_execute(
                "INSERT INTO tk_mensajes "
                "(ticket_id, tipo, contenido, metadata, usuario, es_interno, to_email, cc_email, estado_envio, message_date) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (ticket_id, tipo, contenido,
                 json.dumps(metadata, ensure_ascii=False) if metadata else None,
                 base_user, 1 if es_interno else 0, to_email, cc_email, estado_envio, message_date),
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
    #  TRADUCTOR (Google Cloud Translation) — 2026-07-12 (Daniel)
    #  "quiero un traductor nativo... yo escribo en español y me lo
    #  traduces, y con un boton traducir lo que envia el cliente/
    #  proveedor". Usa el mismo patron de credenciales que GCS
    #  (Application Default Credentials del propio servicio de Cloud
    #  Run — el proyecto ya es el mismo "hosting", sin API key nueva).
    #  Requiere que la Cloud Translation API este habilitada en el
    #  proyecto GCP; si no, degrada con un error claro (no rompe nada).
    # ─────────────────────────────────────────────────────────────────
    _GT_CLIENT = [None]
    _GT_INIT_DONE = [False]

    def _gt_client():
        if _GT_INIT_DONE[0]:
            return _GT_CLIENT[0]
        _GT_INIT_DONE[0] = True
        try:
            from google.cloud import translate_v2 as _gt_lib
            _GT_CLIENT[0] = _gt_lib.Client()
            print("[tickets] Google Translate listo", flush=True)
        except Exception as e:
            print(f"[tickets] Google Translate no disponible: {e}", flush=True)
            _GT_CLIENT[0] = None
        return _GT_CLIENT[0]

    @app.route("/tickets/api/traducir", methods=["POST"])
    @_tickets_required
    def tk_api_traducir():
        d = request.get_json(silent=True) or {}
        texto = (d.get("texto") or "").strip()
        target = (d.get("target") or "en").strip()[:10]
        if not texto:
            return jsonify({"ok": False, "error": "Texto vacío"}), 400
        if len(texto) > 5000:
            texto = texto[:5000]
        cli = _gt_client()
        if not cli:
            return jsonify({
                "ok": False,
                "error": "El traductor no está disponible todavía — falta habilitar "
                         "\"Cloud Translation API\" en el proyecto de Google Cloud "
                         "(ilus-app-498503) y darle el rol al servicio.",
            }), 503
        try:
            res = cli.translate(texto, target_language=target)
            traduccion = _html_mod.unescape(res.get("translatedText") or "")
            return jsonify({
                "ok": True,
                "traduccion": traduccion,
                "idioma_detectado": res.get("detectedSourceLanguage") or "",
            })
        except Exception as e:
            print(f"[tk_api_traducir] error: {e}", flush=True)
            return jsonify({"ok": False, "error": "No se pudo traducir en este momento."}), 500

    # ─────────────────────────────────────────────────────────────────
    #  DICTADO POR VOZ ROBUSTO (Google Cloud Speech-to-Text) — 2026-07-12
    #  Daniel: el dictado nativo del navegador (SpeechRecognition) da
    #  "not-allowed" en computador de forma persistente (una vez que Chrome
    #  deniega el permiso de reconocimiento de voz, no vuelve a preguntar
    #  automaticamente, y es un permiso DISTINTO al de microfono normal).
    #  Reemplazo: se graba el audio con MediaRecorder (permiso de
    #  microfono ESTANDAR, mucho mas confiable/consistente entre
    #  navegadores) y se transcribe en el servidor. Mismo patron ADC que
    #  Traductor/GCS -- sin API key nueva que gestionar.
    # ─────────────────────────────────────────────────────────────────
    _SPEECH_CLIENT = [None]
    _SPEECH_INIT_DONE = [False]

    def _speech_client():
        if _SPEECH_INIT_DONE[0]:
            return _SPEECH_CLIENT[0]
        _SPEECH_INIT_DONE[0] = True
        try:
            from google.cloud import speech as _speech_lib
            _SPEECH_CLIENT[0] = _speech_lib.SpeechClient()
            print("[tickets] Google Speech-to-Text listo", flush=True)
        except Exception as e:
            print(f"[tickets] Google Speech-to-Text no disponible: {e}", flush=True)
            _SPEECH_CLIENT[0] = None
        return _SPEECH_CLIENT[0]

    @app.route("/tickets/api/transcribir", methods=["POST"])
    @_tickets_required
    def tk_api_transcribir():
        audio = request.files.get("audio")
        if not audio or not audio.filename:
            return jsonify({"ok": False, "error": "No se recibió audio"}), 400
        audio_bytes = audio.read()
        # Limite generoso (un mensaje de voz normal no pasa de 1-2 min) --
        # evita subidas gigantes por error o abuso.
        if len(audio_bytes) > 10 * 1024 * 1024:
            return jsonify({"ok": False, "error": "El audio es demasiado largo (máx. ~2 min)."}), 400
        cli = _speech_client()
        if not cli:
            return jsonify({
                "ok": False,
                "error": "El dictado por voz no está disponible todavía — falta habilitar "
                         "\"Cloud Speech-to-Text API\" en el proyecto de Google Cloud "
                         "(ilus-app-498503).",
            }), 503
        try:
            from google.cloud import speech as _speech_lib
            rec_audio = _speech_lib.RecognitionAudio(content=audio_bytes)
            config = _speech_lib.RecognitionConfig(
                encoding=_speech_lib.RecognitionConfig.AudioEncoding.WEBM_OPUS,
                language_code="es-CL",
                alternative_language_codes=["es-419", "en-US"],
                enable_automatic_punctuation=True,
                model="latest_long",
            )
            res = cli.recognize(config=config, audio=rec_audio)
            texto = " ".join(
                r.alternatives[0].transcript.strip()
                for r in res.results if r.alternatives
            ).strip()
            if not texto:
                return jsonify({"ok": False, "error": "No se detectó voz en el audio."}), 200
            return jsonify({"ok": True, "texto": texto})
        except Exception as e:
            print(f"[tk_api_transcribir] error: {e}", flush=True)
            return jsonify({"ok": False, "error": "No se pudo transcribir el audio."}), 500

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
            # El <select> de estado solo ofrece los estados MANUALES -- los
            # 3 automaticos (ot_generated/ot_in_progress/ot_pending_approval)
            # los controla el ciclo de vida de la OT vinculada, no el staff
            # a mano (ver _tk_set_estado_automatico + tk_api_update).
            tk_estados=TK_ESTADOS_MANUALES, tk_tipos=TK_TIPOS, tk_prioridades=TK_PRIORIDADES,
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
    #  API — crear cotizacion en borrador desde el modal ERP compartido
    #  (_tka_modal.html, mode:'seleccionar'). Daniel pidio que Cotizaciones
    #  sea el primer modulo (ademas de Tickets) que llame al ERP con ese
    #  mismo modal. Precios quedan en 0 -- fase de tarifas es futura, no
    #  se inventa logica de pricing aca.
    #
    #  2026-07-15 (Blueprint Cotizaciones Fase 1, Daniel): se extiende para
    #  (a) recibir el `header` del documento (cliente/rut/email/telefono,
    #  ya resuelto por el modal al buscar el documento) y guardarlo real en
    #  vez de quedar siempre NULL (bug confirmado — el frontend nunca lo
    #  mandaba), y (b) clasificar automaticamente cada item contra
    #  cat_productos.clase_producto, devolviendo `sin_clasificar` para que
    #  el frontend pida clasificacion inline de lo que quede pendiente.
    # ─────────────────────────────────────────────────────────────────
    @app.route("/tickets/api/cotizaciones/desde-erp", methods=["POST"])
    @_tickets_required
    def tk_api_cotizacion_desde_erp():
        d = request.get_json(silent=True) or {}
        items = d.get("items") or []
        if not isinstance(items, list) or not items:
            return jsonify({"ok": False, "error": "No se recibió ningún ítem seleccionado del ERP"}), 400

        user = current_username() or "sistema"

        # Header del documento (cliente real): _tka_modal.html lo manda como
        # 2do argumento de onSeleccionar(items, header) -- ver comentario en
        # tkaAsociarSeleccion() del modal. Fallback a d.get(...) directo por
        # si algun caller viejo/futuro sigue mandando los campos sueltos.
        header = d.get("header") if isinstance(d.get("header"), dict) else {}
        empresa = (header.get("cliente") or header.get("empresa") or d.get("empresa") or "").strip()[:150] or None
        rut = (header.get("rut") or d.get("rut") or "").strip()[:12] or None
        email = (header.get("email") or d.get("email") or "").strip()[:190] or None
        if email and not _TK_REPLY_EMAIL_RE.match(email):
            email = None  # dato sucio del ERP -- mejor NULL que guardar basura
        telefono = (header.get("telefono") or d.get("telefono") or "").strip()[:50] or None

        erp_idmaeen = None
        erp_koen = None
        try:
            first_item = items[0] if items and isinstance(items[0], dict) else {}
            first_tido = first_item.get("tido")
            if str(first_tido or "").strip().isdigit():
                erp_idmaeen = int(first_tido)
            erp_koen = first_item.get("koen")
        except Exception:
            pass

        # Clasificacion automatica por SKU contra el Catalogo (Daniel:
        # "los productos, si no tienen clasificacion, tendran que
        # clasificarse, y eso tambien va a estar en los catalogos"). Reusa
        # la MISMA funcion de creacion/reuso de producto que ya usa
        # Catalogo (POST /catalogo/api/productos/desde-erp), inyectada via
        # ctx -- ver comentario en catalogo_module.py junto a
        # _cat_crear_o_reusar_producto_desde_erp. Si el modulo Catalogo no
        # llego a registrar (orden de arranque) o algo falla, se degrada
        # SIN bloquear la creacion de la cotizacion (Regla #4.2 -- aditivo,
        # nunca rompe el flujo existente de crear en $0).
        _cat_crear_o_reusar = ctx.get("_cat_crear_o_reusar_producto_desde_erp")
        clases_por_sku = {}
        sin_clasificar = []
        if _cat_crear_o_reusar:
            for it in items:
                if not isinstance(it, dict):
                    continue
                sku_cls = (it.get("sku") or "").strip().upper()
                if not sku_cls or sku_cls in clases_por_sku:
                    continue
                try:
                    res_cat = _cat_crear_o_reusar(sku_cls, (it.get("nombre") or "").strip())
                except Exception as _e_cat:
                    print(f"[tk_api_cotizacion_desde_erp] clasificacion sku={sku_cls}: {_e_cat}", flush=True)
                    res_cat = None
                clase = (res_cat or {}).get("clase_producto")
                pid_cat = (res_cat or {}).get("id")
                clases_por_sku[sku_cls] = clase
                if pid_cat and not clase:
                    sin_clasificar.append({
                        "sku": sku_cls, "producto_id": pid_cat,
                        "nombre": (it.get("nombre") or "").strip(),
                    })

        conn = get_mysql()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO tk_cotizaciones "
                    "(estado, erp_idmaeen, erp_koen, rut, empresa, email, telefono, created_by) "
                    "VALUES ('draft', %s, %s, %s, %s, %s, %s, %s)",
                    (erp_idmaeen, (erp_koen or "")[:50] or None, rut, empresa, email, telefono, user),
                )
                cot_id = cur.lastrowid
                # Numeracion race-free derivada del id autoincrement, mismo
                # patron que TK-{anio}-{id} para tk_tickets (hora Chile,
                # Regla #6, evita numerar en el limite de anio via UTC).
                cur.execute(
                    "UPDATE tk_cotizaciones SET numero_cotizacion = "
                    "CONCAT('COT-', %s, '-', LPAD(id,5,'0')) WHERE id=%s",
                    (_chile_now_year(), cot_id),
                )
                for it in items:
                    if not isinstance(it, dict):
                        continue
                    sku = (it.get("sku") or "").strip()[:100] or None
                    sku_up = (sku or "").upper()
                    descripcion = (it.get("nombre") or "").strip()[:300] or None
                    try:
                        cantidad = int(it.get("qty") or 1)
                    except Exception:
                        cantidad = 1
                    if cantidad < 1:
                        cantidad = 1
                    clase_producto = clases_por_sku.get(sku_up)
                    # vaneli_original: valor de linea real del ERP, si el
                    # item lo trae -- el modal aun no lo expone (fase de
                    # pricing es Fase 2), asi que hoy siempre queda NULL
                    # sin bloquear nada (Blueprint §3.2.C.4).
                    vaneli_original = None
                    try:
                        _v = it.get("vaneli") or it.get("valor_linea") or it.get("precio_unitario")
                        vaneli_original = int(float(_v)) if _v not in (None, "") else None
                    except Exception:
                        vaneli_original = None
                    cur.execute(
                        "INSERT INTO tk_cotizacion_items "
                        "(cotizacion_id, item_tipo, erp_kopr, descripcion, cantidad, "
                        " precio_unitario, subtotal, total, desde_ticket, clase_producto, vaneli_original) "
                        "VALUES (%s,'producto',%s,%s,%s,0,0,0,0,%s,%s)",
                        (cot_id, sku, descripcion, cantidad, clase_producto, vaneli_original),
                    )
            conn.commit()
        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            print(f"[tk_api_cotizacion_desde_erp] CRASH: {e}", flush=True)
            return jsonify({"ok": False, "error": "No se pudo crear la cotización"}), 500
        finally:
            conn.close()

        numero_row = mysql_fetchone(
            "SELECT numero_cotizacion FROM tk_cotizaciones WHERE id=%s", (cot_id,))
        numero = numero_row["numero_cotizacion"] if numero_row else None
        try:
            _audit("tk_cotizacion_create", target_type="tk_cotizacion", target_id=cot_id,
                   details={"numero": numero, "items": len(items), "sin_clasificar": len(sin_clasificar)})
        except Exception:
            pass
        return jsonify({"ok": True, "id": cot_id, "numero_cotizacion": numero,
                         "sin_clasificar": sin_clasificar})

    # ─────────────────────────────────────────────────────────────────
    #  API — listado + KPIs
    # ─────────────────────────────────────────────────────────────────
    @app.route("/tickets/api/tickets", methods=["GET"])
    @_tickets_required
    def tk_api_list():
        # Auto-barrido del buzon de respuestas (fire-and-forget, hilo con
        # app_context; se auto-limita a 1 barrido cada 5 min). Definido mas
        # abajo en este mismo closure -- resuelve en tiempo de llamada.
        try:
            _tk_autopoll_correo()
        except Exception:
            pass
        # WHERE compartido con los reportes CSV (una sola fuente de verdad).
        # Filtros: estado, tipo, prioridad, origen, asignado_a, rut, q,
        # + nuevos: ticket, fecha_desde, fecha_hasta, hoy=1.
        wsql, params = _tk_list_where(request.args)

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
        pages = (total + limit - 1) // limit  # ceil(total/limit); 0 si no hay filas

        # ORDER BY: si viene ?sort= valido (whitelist _TK_SORT_COLS) se ordena
        # SOLO por esa columna + dir (t.id DESC de desempate). Si NO viene,
        # se mantiene EXACTO el orden inteligente historico (Daniel 2026-07-11:
        # "cuando el cliente responda... las respuestas mas nuevas se van
        # posicionando mas arriba" -- un ticket con mensajes de cliente sin
        # leer sube al tope de la bandeja, ANTES que el orden por estado/
        # prioridad). order_sql sale SOLO del dict whitelist o de la
        # constante: jamas del request crudo.
        order_sql = _tk_sort_order(request.args) or _TK_ORDER_DEFAULT
        rows = mysql_fetchall(
            "SELECT t.id, t.numero_ticket, t.origen, t.estado, t.tipo, t.prioridad, "
            "       t.titulo, t.empresa, t.rut, t.nombre_contacto, t.asignado_a, "
            "       t.created_at, t.updated_at, t.fecha_limite, t.es_garantia, "
            "       (SELECT COUNT(*) FROM tk_mensajes m "
            "          WHERE m.ticket_id=t.id AND m.tipo='client_message' "
            "            AND m.created_at > COALESCE(t.staff_last_read_at,'1970-01-01')) AS unread_count "
            f"FROM tk_tickets t{wsql} "
            f"ORDER BY {order_sql} "
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

        # SLA calculado por fila (Daniel 2026-07-14). Se computa ANTES de
        # _fmt_row porque necesita el created_at crudo (datetime UTC), no
        # el string ya formateado a hora Chile.
        sla_umbral = _tk_sla_horas_umbral()
        tickets_out = []
        for r in rows:
            d = _fmt_row(r)
            d["sla_horas"], d["sla_vencido"] = _tk_sla_info(
                r.get("estado"), r.get("created_at"), sla_umbral)
            tickets_out.append(d)

        return jsonify({
            "ok": True,
            "tickets": tickets_out,
            "sla_umbral_horas": sla_umbral,
            "total": total, "page": page, "limit": limit, "pages": pages,
            "kpis": {
                "total": int(kpi.get("total") or 0),
                "activos": int(kpi.get("activos") or 0),
                "urgentes": int(kpi.get("urgentes") or 0),
                "vencidos": int(kpi.get("vencidos") or 0),
            },
        })

    # ─────────────────────────────────────────────────────────────────
    #  API — Reportes CSV (Excel es-CL: delimitador ';' + BOM UTF-8 para
    #  que las tildes abran bien). Respetan los MISMOS filtros que
    #  tk_api_list via _tk_list_where (una sola fuente del WHERE).
    # ─────────────────────────────────────────────────────────────────
    def _tk_csv_response(filename, header, rows):
        import csv as _csv
        import io as _io
        buf = _io.StringIO()
        w = _csv.writer(buf, delimiter=";", lineterminator="\r\n")
        w.writerow(header)
        for r in rows:
            w.writerow(r)
        # BOM UTF-8 explicito para que Excel es-CL abra las tildes bien.
        resp = app.response_class(
            chr(0xFEFF) + buf.getvalue(), mimetype="text/csv; charset=utf-8")
        resp.headers["Content-Disposition"] = f"attachment; filename={filename}"
        return resp

    @app.route("/tickets/api/reporte/tickets.csv", methods=["GET"])
    @_tickets_required
    def tk_reporte_tickets_csv():
        wsql, params = _tk_list_where(request.args)
        # Reporte orientado a fechas: created_at DESC por defecto; honra
        # ?sort=&dir= (misma whitelist del listado) si vienen.
        order_sql = _tk_sort_order(request.args) or "t.created_at DESC, t.id DESC"
        rows = mysql_fetchall(
            "SELECT t.numero_ticket, t.created_at, t.origen, t.asignado_a, t.rut, "
            "       t.empresa, t.nombre_contacto, t.estado, t.tipo, t.prioridad, "
            "       t.es_garantia, t.titulo "
            f"FROM tk_tickets t{wsql} "
            f"ORDER BY {order_sql} "
            "LIMIT 10000",  # tope de proteccion
            tuple(params),
        ) or []
        salida = []
        for r in rows:
            salida.append([
                r.get("numero_ticket") or "",
                _fmt_dt(r.get("created_at")) or "",       # hora Chile (Regla #6)
                r.get("origen") or "",
                r.get("asignado_a") or "",
                r.get("rut") or "",
                (r.get("empresa") or r.get("nombre_contacto") or ""),
                ESTADO_LABEL.get(r.get("estado"), r.get("estado") or ""),
                TIPO_LABEL.get(r.get("tipo"), r.get("tipo") or ""),
                r.get("prioridad") or "",
                "Si" if r.get("es_garantia") else "No",
                r.get("titulo") or "",
            ])
        return _tk_csv_response(
            "reporte_tickets_ILUS.csv",
            ["numero_ticket", "fecha_ingreso", "origen", "responsable", "rut",
             "cliente", "estado", "tipo", "prioridad", "es_garantia", "titulo"],
            salida)

    @app.route("/tickets/api/reporte/sla.csv", methods=["GET"])
    @_tickets_required
    def tk_reporte_sla_csv():
        wsql, params = _tk_list_where(request.args)
        rows = mysql_fetchall(
            "SELECT t.numero_ticket, t.empresa, t.nombre_contacto, t.rut, t.tipo, "
            "       t.estado, t.created_at, t.updated_at, t.fecha_limite "
            f"FROM tk_tickets t{wsql} "
            "ORDER BY t.created_at DESC, t.id DESC "
            "LIMIT 10000",
            tuple(params),
        ) or []
        hoy_cl = _chile_hoy()
        ahora_utc = datetime.utcnow()  # created_at/updated_at son UTC naive
        terminales = ("resolved", "closed", "cancelado")
        salida = []
        for r in rows:
            estado = r.get("estado") or ""
            creado = r.get("created_at")
            cambiado = r.get("updated_at")
            # duracion_dias: estados terminales -> created_at..updated_at;
            # el resto -> created_at..ahora (ticket aun corriendo).
            dur = ""
            if isinstance(creado, datetime):
                fin = cambiado if (estado in terminales and isinstance(cambiado, datetime)) else ahora_utc
                try:
                    dur = round(max(0.0, (fin - creado).total_seconds()) / 86400.0, 1)
                except Exception:
                    dur = ""
            fl = r.get("fecha_limite")
            if isinstance(fl, datetime):
                fl = fl.date()
            vencido = "Si" if (isinstance(fl, date) and fl < hoy_cl
                               and estado not in terminales) else "No"
            salida.append([
                r.get("numero_ticket") or "",
                (r.get("empresa") or r.get("nombre_contacto") or ""),
                r.get("rut") or "",
                TIPO_LABEL.get(r.get("tipo"), r.get("tipo") or ""),
                ESTADO_LABEL.get(estado, estado),
                _fmt_dt(creado) or "",
                _fmt_dt(cambiado) or "",
                dur,
                vencido,
            ])
        return _tk_csv_response(
            "reporte_sla_tickets_ILUS.csv",
            ["numero_ticket", "cliente", "rut", "tipo", "estado", "fecha_creacion",
             "fecha_ultimo_cambio", "duracion_dias", "vencido"],
            salida)

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
        # EXCEPCION 2026-07-15: los 3 tipos internos de bodega (TK_TIPOS_SIN_CLIENTE
        # -- control_calidad/trabajo_bodega/capacitacion) NO tienen cliente, por lo
        # tanto solo tipo+descripcion son obligatorios. El frontend (list.html,
        # modal "Nuevo Ticket") debe replicar esta misma condicion en
        # validarNtTodo() -- ver contrato documentado en el reporte del agente.
        _es_interno_sin_cliente = tipo_in in TK_TIPOS_SIN_CLIENTE
        faltantes = []
        if not tipo_in: faltantes.append("tipo de solicitud")
        if not _es_interno_sin_cliente:
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
                # FIX 2026-07-15: documento_garantia no se llenaba en el ALTA
                # (solo via PATCH manual de superadmin). Si el equipo trae su
                # PROPIO documento de origen (eq.tido/eq.nudo -- ej. seleccion
                # granular por equipo), se usa ese; si no, cae al
                # numero_documento a nivel de ticket. Mismo formato que
                # tk_api_equipos_desde_documento ("TIDO-NUDO", ej. "VD-6162").
                doc_ticket_fallback = (d.get("numero_documento") or "").strip()[:150] or None
                for eq in (d.get("equipos") or []):
                    try:
                        eq_tido = (eq.get("tido") or "").strip().upper()
                        eq_nudo = (eq.get("nudo") or "").strip()
                        doc_garantia = f"{eq_tido}-{eq_nudo}"[:150] if (eq_tido and eq_nudo) \
                            else doc_ticket_fallback
                        cur.execute(
                            "INSERT IGNORE INTO tk_ticket_equipos "
                            "(ticket_id, erp_kopr, nombre, tipo, sku, cantidad, notas, documento_garantia) "
                            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                            (tid, (eq.get("kopr") or "").strip()[:100] or None,
                             (eq.get("nombre") or "").strip()[:300] or None,
                             (eq.get("tipo") or "").strip()[:100] or None,
                             (eq.get("sku") or "").strip()[:100] or None,
                             int(eq.get("cantidad") or 1),
                             (eq.get("notas") or "").strip()[:500] or None,
                             doc_garantia),
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
        # FIX 2026-07-12 (Daniel, en vivo): el auto-barrido del buzon SOLO
        # estaba en tk_api_list (la bandeja) -- si el staff entra directo a
        # la ficha de un ticket (como paso probando el ping-pong: ficha ->
        # Respuestas) sin haber cargado la bandeja antes, la respuesta del
        # cliente nunca se revisaba. Ahora TAMBIEN se dispara aca.
        try:
            _tk_autopoll_correo()
        except Exception:
            pass
        # FIX 2026-07-12: LEFT JOIN a mant_visitas para traer el numero_ot
        # REAL (formato OT-YYYY-NNNNN) cuando el ticket ya está vinculado a
        # una visita -- antes la ficha solo tenía visita_id (un entero
        # interno) y mostraba "OT #<id>" en vez del correlativo real. Sin
        # colisión de nombres: tk_tickets no tiene columna numero_ot propia.
        t = mysql_fetchone(
            "SELECT t.*, v.numero_ot AS visita_numero_ot "
            "  FROM tk_tickets t LEFT JOIN mant_visitas v ON v.id = t.visita_id "
            " WHERE t.id=%s", (tid,))
        if not t:
            return jsonify({"ok": False, "error": "Ticket no encontrado"}), 404
        # FIX 2026-07-12: faltaban las 5 columnas de garantía por equipo
        # (con_garantia/documento_garantia/fecha_emision/garantia_meses/
        # fecha_vencimiento) -- se guardaban bien vía PATCH pero la ficha
        # nunca las traía de vuelta, así que la garantía "desaparecía" al
        # recargar.
        equipos = mysql_fetchall(
            "SELECT id, erp_kopr, nombre, tipo, sku, serie, cantidad, maquina_id, notas, "
            "       con_garantia, documento_garantia, fecha_emision, garantia_meses, fecha_vencimiento "
            "FROM tk_ticket_equipos WHERE ticket_id=%s ORDER BY id", (tid,))
        documentos = mysql_fetchall(
            "SELECT id, erp_tido, erp_nudo, fecha, monto FROM tk_ticket_documentos "
            "WHERE ticket_id=%s ORDER BY id", (tid,))
        try:
            mensajes = mysql_fetchall(
                "SELECT id, tipo, contenido, metadata, usuario, es_interno, message_date, created_at, "
                "       to_email, cc_email, estado_envio "
                "FROM tk_mensajes WHERE ticket_id=%s "
                "ORDER BY COALESCE(message_date, created_at) ASC, id ASC", (tid,))
        except Exception as _e:
            # Defensivo: si la migracion de columnas (to_email/cc_email/
            # estado_envio) no corrio, no debe romperse la ficha entera.
            print(f"[tk_api_get] mensajes con columnas nuevas fallo, fallback: {_e}", flush=True)
            mensajes = mysql_fetchall(
                "SELECT id, tipo, contenido, metadata, usuario, es_interno, message_date, created_at "
                "FROM tk_mensajes WHERE ticket_id=%s "
                "ORDER BY COALESCE(message_date, created_at) ASC, id ASC", (tid,))
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

        # Contador de mensajes de cliente sin leer PARA ESTA ficha (badge de
        # la pestaña Respuestas). Daniel 2026-07-12: "cuando las lea ya,
        # quiero que se borren" -- antes el badge contaba TODOS los
        # client_message del historial (nunca bajaba de ahi). Se calcula
        # en SQL (no en JS) porque comparar los datetimes ya formateados a
        # texto Chile (dd/mm/aaaa) no ordena cronologicamente. `t` aqui
        # todavia es el dict CRUDO (antes de _fmt_row), asi que
        # staff_last_read_at es un datetime real, no un string.
        try:
            unread_row = mysql_fetchone(
                "SELECT COUNT(*) AS n FROM tk_mensajes WHERE ticket_id=%s "
                "AND tipo='client_message' "
                "AND created_at > COALESCE(%s, '1970-01-01')",
                (tid, t.get("staff_last_read_at")))
            unread_count = int(unread_row["n"]) if unread_row else 0
        except Exception:
            unread_count = 0

        # La pestaña Actividad (que consume "vistas") queda oculta en la UI
        # para quien no es admin/superadmin -- pero ocultar solo en pantalla
        # no evita que alguien inspeccione la respuesta de red y lea los
        # datos igual. Se filtra tambien aca, a nivel de datos.
        _perms = g.get("permissions") or {}
        _puede_ver_actividad = bool(_perms.get("superadmin") or _perms.get("admin"))

        # SLA calculado (Daniel 2026-07-14) -- mismos campos que el listado.
        # Se computa con el created_at CRUDO de `t` (antes de _fmt_row).
        sla_umbral = _tk_sla_horas_umbral()
        ticket_out = _fmt_row(t)
        ticket_out["sla_horas"], ticket_out["sla_vencido"] = _tk_sla_info(
            t.get("estado"), t.get("created_at"), sla_umbral)

        return jsonify({
            "ok": True,
            "ticket": ticket_out,
            "sla_umbral_horas": sla_umbral,
            "equipos": [dict(r) for r in equipos],
            "documentos": [_fmt_row(r) for r in documentos],
            "mensajes": [_fmt_row(r) for r in mensajes],
            "adjuntos": [_fmt_row(r) for r in adjuntos],
            "vistas": [_fmt_row(r) for r in vistas] if _puede_ver_actividad else [],
            "unread_count": unread_count,
            "estado_label": ESTADO_LABEL, "tipo_label": TIPO_LABEL,
        })

    # ─────────────────────────────────────────────────────────────────
    #  API — actualizar
    # ─────────────────────────────────────────────────────────────────
    @app.route("/tickets/api/tickets/<int:tid>", methods=["PATCH"])
    @_tickets_required
    def tk_api_update(tid):
        prev = mysql_fetchone(
            "SELECT estado, prioridad, tipo, asignado_a, numero_ticket, titulo "
            "FROM tk_tickets WHERE id=%s", (tid,))
        if not prev:
            return jsonify({"ok": False, "error": "Ticket no encontrado"}), 404

        d = request.get_json(silent=True) or {}
        # 🔐 Los 3 estados AUTOMATICOS del ciclo de vida de la OT
        # (ot_generated/ot_in_progress/ot_pending_approval) no se pueden
        # setear a mano via este PATCH -- solo _tk_set_estado_automatico
        # (invocado internamente desde la OT) los controla. Esto blinda el
        # backend aunque alguien edite el DOM o llame el PATCH directo.
        if "estado" in d and (d.get("estado") or "").strip().lower() in TK_ESTADOS_AUTOMATICOS:
            return jsonify({
                "ok": False,
                "error": "Ese estado lo controla automáticamente la OT vinculada; no se puede setear a mano.",
                "error_codigo": "ESTADO_AUTOMATICO_NO_MANUAL",
            }), 400
        allowed = (
            "titulo", "descripcion", "tipo", "prioridad", "estado", "sucursal",
            "nombre_contacto", "email", "phone", "direccion", "empresa", "rut",
            "asignado_a", "tecnico_id", "fecha_limite", "notas_internas",
            "producto", "marca", "sku",
            # Daniel 2026-07-11: la ficha ahora tambien edita region/comuna
            # (antes solo se guardaban al crear el ticket, nunca al editar).
            "region_nombre", "comuna_nombre", "numero_documento",
            # Daniel 2026-07-12: "dame la opcion de editar, validarlo... esto
            # tiene que ser a nivel general" -- editar Direccion ahora pasa
            # por Google Places (perfil logistico de Daniel), asi que el
            # guardado debe poder llevar tambien las coordenadas/place_id
            # que antes solo se guardaban al CREAR el ticket, nunca al editar.
            "direccion_lat", "direccion_lng", "direccion_place_id",
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
            # Notificacion de asignacion (aditivo 2026-07-12) -- campana +
            # correo al usuario recien asignado. Nunca debe romper el
            # guardado del ticket (try/except, mismo patron defensivo que
            # las notificaciones de resuelto/cerrado).
            try:
                _tk_notificar_asignacion(
                    tid, d.get("asignado_a"),
                    prev.get("numero_ticket") or f"#{tid}",
                    prev.get("titulo"), actor_username=user)
            except Exception as _e:
                print(f"[tk_api_update] notificacion asignacion no enviada tid={tid}: {_e}", flush=True)

        # Notificacion automatica al cliente en los hitos del lifecycle
        # (Daniel 2026-07-11/12) -- respeta la llave de paso del modulo
        # 'tickets'. Mapeo generico y ADITIVO (ver diseño
        # lifecycle_estados_extra): 'ot_pending_approval' (aprobacion
        # interna) y 'open' (reapertura) se dejan fuera a proposito, sin
        # plantilla, hasta que Daniel pida explicitamente notificarlos.
        _ESTADO_NOTIF_SLUG = {
            "resolved": "resuelto",
            "closed": "cerrado",
            "in_progress": "en_curso",
            "pending": "pendiente",
            "ot_generated": "ot_generada",
            "ot_in_progress": "ot_en_curso",
            "cancelado": "cancelado",
        }
        if nuevo_estado and nuevo_estado != prev["estado"] and nuevo_estado in _ESTADO_NOTIF_SLUG:
            # Matiz historico solo para 'resolved': evita renotificar en un
            # bounce closed->resolved (el cliente ya recibio el aviso de
            # cerrado). Para el resto de estados basta con != al anterior.
            if nuevo_estado == "resolved" and prev["estado"] in ("resolved", "closed"):
                pass
            else:
                slug = _ESTADO_NOTIF_SLUG[nuevo_estado]
                try:
                    _tk_notificar_lifecycle(tid, slug)
                except Exception as _e:
                    print(f"[tk_api_update] notificacion '{slug}' no enviada tid={tid}: {_e}", flush=True)
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
        # Confirmacion obligatoria (Regla #5: hard delete = superadmin +
        # confirmacion). Se acepta 'confirm' (body JSON o query param, nombre
        # que usa el frontend de la tabla) o 'confirm_text' (nombre historico),
        # y debe coincidir con el numero_ticket (TK-...) o con el id del ticket.
        confirm = (str(d.get("confirm") or d.get("confirm_text") or "").strip()
                   or (request.args.get("confirm") or "").strip())
        coincide = bool(confirm) and (
            (bool(numero) and confirm.upper() == numero.upper())
            or confirm == str(tid)
        )
        if not coincide:
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

    # ─────────────────────────────────────────────────────────────────
    #  API — purga masiva "dejar solo los tickets de un correo" (Daniel
    #  2026-07-12: limpieza antes de operar en serio, confirmado
    #  explicitamente que SI incluye los migrados de Triple A). Mismo
    #  patron que el importador CSV: dry_run por defecto (nunca borra sin
    #  que el front pida dry_run=false explicitamente) + confirm exacto
    #  ligado al correo (evita reusar el mismo texto para otro alcance) +
    #  audit ANTES de borrar (Regla #5). El DELETE en tk_tickets cascadea
    #  via FK a tk_mensajes/tk_adjuntos/tk_ticket_equipos/
    #  tk_ticket_documentos/tk_vistas; tk_cotizaciones queda con
    #  ticket_id=NULL (ON DELETE SET NULL, la cotizacion no se borra).
    # ─────────────────────────────────────────────────────────────────
    @app.route("/tickets/api/admin/purgar-por-correo", methods=["POST"])
    @_tickets_required
    def tk_api_purgar_por_correo():
        perms = g.get("permissions") or {}
        if not perms.get("superadmin"):
            return jsonify({"ok": False, "error": "Solo un superadministrador puede purgar tickets."}), 403
        d = request.get_json(silent=True) or {}
        keep_email = (d.get("keep_email") or "").strip().lower()
        if not keep_email or "@" not in keep_email:
            return jsonify({"ok": False, "error": "Falta un correo válido a conservar."}), 400
        dry_run = d.get("dry_run", True)
        if isinstance(dry_run, str):
            dry_run = dry_run.strip().lower() not in ("0", "false", "no")

        where = "WHERE LOWER(TRIM(COALESCE(email,''))) != %s"
        total = int((mysql_fetchone("SELECT COUNT(*) AS n FROM tk_tickets") or {}).get("n") or 0)
        a_borrar = int((mysql_fetchone(f"SELECT COUNT(*) AS n FROM tk_tickets {where}",
                                       (keep_email,)) or {}).get("n") or 0)
        a_borrar_taa = int((mysql_fetchone(
            f"SELECT COUNT(*) AS n FROM tk_tickets {where} AND legacy_taa_id IS NOT NULL",
            (keep_email,)) or {}).get("n") or 0)
        resumen = {"total": total, "a_borrar": a_borrar, "a_borrar_triple_a": a_borrar_taa,
                   "a_conservar": total - a_borrar, "keep_email": keep_email}

        if dry_run:
            return jsonify({"ok": True, "dry_run": True, "resumen": resumen})

        confirm_esperado = "BORRAR " + keep_email.upper()
        confirm = (d.get("confirm") or "").strip().upper()
        if confirm != confirm_esperado:
            return jsonify({
                "ok": False,
                "error": f"Para confirmar, escribe exactamente: {confirm_esperado}",
                "expected": confirm_esperado, "resumen": resumen,
            }), 400

        try:
            _audit("tk_purga_masiva_por_correo", details=resumen)
        except Exception:
            pass
        try:
            mysql_execute(f"DELETE FROM tk_tickets {where}", (keep_email,))
        except Exception as _e:
            print(f"[tk_purgar_por_correo] error: {_e}", flush=True)
            return jsonify({"ok": False, "error": "No se pudo completar la purga, intenta de nuevo."}), 500
        return jsonify({"ok": True, "dry_run": False, "eliminados": a_borrar, "resumen": resumen})

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
    # REDISENO 2026-07-12 (Daniel): mensaje protagonista, tipografia grande,
    # sin texto de relleno. Mismo bloque destacado que la respuesta manual.
    # Numero de ticket AL INICIO del asunto (Daniel 2026-07-12, con screenshot
    # de Gmail movil): la app trunca asuntos largos en la lista de bandeja, y
    # el numero quedaba cortado al ir al final ("...ticket TK-2026-0...").
    # 2026-07-12 (Daniel): "el nombre del ticket... lo dice demasiado" --
    # el numero ya aparece en el asunto (title) y en el subtitulo del header
    # (ver _tk_notificar_lifecycle mas abajo), asi que el cuerpo YA NO lo
    # repite. Primer recorte de la redundancia; el diseño completo de estos
    # correos se retrabaja mas adelante como propuesta aparte.
    _TK_LIFECYCLE_DEFAULTS = {
        "creacion": (
            "{numero} — Recibimos tu solicitud",
            "<p style=\"font-size:14px;color:#6b7280;margin:0 0 14px\">Hola {cliente},</p>"
            "<div style=\"border-left:4px solid #dc2626;background:#fafafa;border-radius:0 10px 10px 0;"
            "padding:18px 20px;margin:0 0 6px\">"
            "<div style=\"font-size:16px;color:#111827;line-height:1.6\">Ya registramos tu solicitud. "
            "Nuestro equipo la revisará y te contactará a la brevedad.</div></div>"),
        "resuelto": (
            "{numero} — Resuelto",
            "<p style=\"font-size:14px;color:#6b7280;margin:0 0 14px\">Hola {cliente},</p>"
            "<div style=\"border-left:4px solid #16a34a;background:#f0fdf4;border-radius:0 10px 10px 0;"
            "padding:18px 20px;margin:0 0 6px\">"
            "<div style=\"font-size:16px;color:#111827;line-height:1.6\">✅ Tu solicitud "
            "ya fue resuelta por nuestro equipo.</div></div>"),
        "cerrado": (
            "{numero} — Cerrado",
            "<p style=\"font-size:14px;color:#6b7280;margin:0 0 14px\">Hola {cliente},</p>"
            "<div style=\"border-left:4px solid #6b7280;background:#f9fafb;border-radius:0 10px 10px 0;"
            "padding:18px 20px;margin:0 0 6px\">"
            "<div style=\"font-size:16px;color:#111827;line-height:1.6\">Tu solicitud ha sido cerrada. "
            "Gracias por confiar en ILUS Sport &amp; Health.</div></div>"),
        # Estados extra del lifecycle (aditivo 2026-07-12). Fallback hardcoded
        # por si _render_comm_template no encuentra la fila sembrada aun --
        # el texto real editable vive en _tickets_tpl_seed() (app.py).
        "en_curso": (
            "{numero} — En curso",
            "<p style=\"font-size:14px;color:#6b7280;margin:0 0 14px\">Hola {cliente},</p>"
            "<div style=\"border-left:4px solid #3b82f6;background:#eff6ff;border-radius:0 10px 10px 0;"
            "padding:18px 20px;margin:0 0 6px\">"
            "<div style=\"font-size:16px;color:#111827;line-height:1.6\">Tu equipo ya está trabajando "
            "en tu solicitud.</div></div>"),
        "pendiente": (
            "{numero} — Pendiente",
            "<p style=\"font-size:14px;color:#6b7280;margin:0 0 14px\">Hola {cliente},</p>"
            "<div style=\"border-left:4px solid #f59e0b;background:#fff8e1;border-radius:0 10px 10px 0;"
            "padding:18px 20px;margin:0 0 6px\">"
            "<div style=\"font-size:16px;color:#111827;line-height:1.6\">Tu solicitud quedó pendiente "
            "— puede que necesitemos información adicional de tu parte.</div></div>"),
        "ot_generada": (
            "{numero} — Orden de Trabajo generada",
            "<p style=\"font-size:14px;color:#6b7280;margin:0 0 14px\">Hola {cliente},</p>"
            "<div style=\"border-left:4px solid #3b82f6;background:#eff6ff;border-radius:0 10px 10px 0;"
            "padding:18px 20px;margin:0 0 6px\">"
            "<div style=\"font-size:16px;color:#111827;line-height:1.6\">Se generó una Orden de Trabajo "
            "para tu solicitud.</div></div>"),
        "ot_en_curso": (
            "{numero} — Técnico en terreno",
            "<p style=\"font-size:14px;color:#6b7280;margin:0 0 14px\">Hola {cliente},</p>"
            "<div style=\"border-left:4px solid #0d9488;background:#f0fdfa;border-radius:0 10px 10px 0;"
            "padding:18px 20px;margin:0 0 6px\">"
            "<div style=\"font-size:16px;color:#111827;line-height:1.6\">El técnico ya está trabajando "
            "en terreno en tu Orden de Trabajo.</div></div>"),
        "cancelado": (
            "{numero} — Cancelado",
            "<p style=\"font-size:14px;color:#6b7280;margin:0 0 14px\">Hola {cliente},</p>"
            "<div style=\"border-left:4px solid #dc2626;background:#fee2e2;border-radius:0 10px 10px 0;"
            "padding:18px 20px;margin:0 0 6px\">"
            "<div style=\"font-size:16px;color:#111827;line-height:1.6\">Tu solicitud fue "
            "cancelada.</div></div>"),
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
        # MODO PRUEBA (TK_TEST_EMAIL_TO): to_email/subject de arriba se
        # conservan intactos para el log (_tk_log mas abajo) -- solo el
        # ENVIO real se redirige, para que la trazabilidad del ticket siga
        # mostrando el destinatario real aunque el correo haya salido a
        # la casilla de pruebas.
        to_envio, subject_envio = _tk_test_redirect(to_email, subject)
        html_final = (_comm_render_email_document(subject_envio, cuerpo_correo, subtitle="Servicio Técnico · ILUS Fitness")
                      if _comm_render_email_document else cuerpo_correo)
        try:
            enviado = _send_ilus_email(to_envio, subject_envio, html_final,
                                        evento=f"ticket_{estado_slug}", modulo="tickets",
                                        reply_to=_tk_reply_to())
        except Exception as _e:
            print(f"[tk_notificar_lifecycle] error enviando tid={tid} estado={estado_slug}: {_e}", flush=True)
            enviado = False

        _tk_log(tid, "mensaje", cuerpo_email, es_interno=False,
                to_email=to_email[:150], estado_envio="enviado" if enviado else "fallido",
                metadata={"subject": subject, "auto": True, "estado_slug": estado_slug})

    def _tk_set_estado_automatico(tid, nuevo_estado_tk, motivo, visita_id=None):
        """Sincroniza tk_tickets.estado automáticamente desde el ciclo de vida
        de la OT vinculada (mant_visitas). NUNCA se expone como ruta Flask --
        solo la invocan otras funciones del backend (creación de OT, inicio,
        firmas, cierre). Diseñado para no romper jamás el flujo llamador:
        cualquier error se traga y se loguea, retornando False.

        Reutiliza el mismo mecanismo de bitácora que ya usa tk_api_update
        (_tk_log con tipo='cambio_estado', tabla tk_mensajes) y el mismo
        mapeo de notificación al cliente (_ESTADO_NOTIF_SLUG) para que un
        cambio automático de estado avise igual que uno manual.

        Retorna True si aplicó el cambio, False si fue no-op o falló
        (ticket inexistente, estado invalido, o ya estaba en ese estado).
        """
        try:
            nuevo_estado_tk = _norm_enum(nuevo_estado_tk, TK_ESTADOS, None)
            if not nuevo_estado_tk:
                print(f"[tk_set_estado_automatico] estado invalido tid={tid}: {nuevo_estado_tk}", flush=True)
                return False
            prev = mysql_fetchone("SELECT estado FROM tk_tickets WHERE id=%s", (tid,))
            if not prev:
                return False
            if prev["estado"] == nuevo_estado_tk:
                return False  # no-op: evita bitácora/correos duplicados en reintentos
            # 🔒 Hallazgo de revisión adversarial 2026-07-12: 'closed'/'cancelado'
            # son estados TERMINALES decididos por una persona -- un evento
            # posterior de la OT (reinicio de visita, segunda vía de cierre)
            # no debe "resucitar" el ticket ni reenviar notificaciones de un
            # ticket que el cliente ya cree cerrado/cancelado.
            if prev["estado"] in ("closed", "cancelado"):
                print(f"[tk_set_estado_automatico] tid={tid} en estado terminal "
                      f"'{prev['estado']}' -- se ignora el intento de pasarlo a '{nuevo_estado_tk}'", flush=True)
                return False

            user = "sistema"
            sets, params = ["estado=%s"], [nuevo_estado_tk]
            if nuevo_estado_tk in ("resolved", "closed") and prev["estado"] not in ("resolved", "closed"):
                sets.append("cerrado_at=NOW()"); sets.append("cerrado_por=%s"); params.append(user)
            params.append(tid)
            mysql_execute(f"UPDATE tk_tickets SET {', '.join(sets)} WHERE id=%s", tuple(params))

            _tk_log(tid, "cambio_estado",
                    f"Estado: {ESTADO_LABEL.get(prev['estado'], prev['estado'])} → "
                    f"{ESTADO_LABEL.get(nuevo_estado_tk, nuevo_estado_tk)} ({motivo})",
                    usuario=user,
                    metadata={"campo": "estado", "antes": prev["estado"], "nuevo": nuevo_estado_tk,
                              "motivo": motivo, "origen": "automatico", "visita_id": visita_id})

            _ESTADO_NOTIF_SLUG_AUTO = {
                "resolved": "resuelto", "closed": "cerrado", "in_progress": "en_curso",
                "pending": "pendiente", "ot_generated": "ot_generada",
                "ot_in_progress": "ot_en_curso", "cancelado": "cancelado",
            }
            if nuevo_estado_tk in _ESTADO_NOTIF_SLUG_AUTO:
                if not (nuevo_estado_tk == "resolved" and prev["estado"] in ("resolved", "closed")):
                    try:
                        _tk_notificar_lifecycle(tid, _ESTADO_NOTIF_SLUG_AUTO[nuevo_estado_tk])
                    except Exception as _e:
                        print(f"[tk_set_estado_automatico] notif '{nuevo_estado_tk}' no enviada tid={tid}: {_e}", flush=True)
            return True
        except Exception as e:
            print(f"[tk_set_estado_automatico] error tid={tid} nuevo={nuevo_estado_tk}: {e}", flush=True)
            return False

    ctx["_tk_set_estado_automatico"] = _tk_set_estado_automatico  # visible desde app.py (OT lifecycle)

    def _tk_notificar_asignacion(tid, asignado_a_nuevo, numero, titulo, actor_username=None):
        """Notifica (campana + correo) al usuario que quedó asignado a un
        ticket. Aditivo 2026-07-12 -- nunca debe romper el guardado del
        ticket (se llama envuelto en try/except desde tk_api_update).

        Resolucion del destinatario: 'asignado_a' guarda el NOMBRE (o
        username si no tiene nombre) tal como lo puebla
        /mantenciones/api/ejecutivos (COALESCE(nombre,username)), no un id.
        Si dos usuarios activos comparten el mismo nombre para mostrar, esto
        podria matchear al que no es (riesgo documentado, ver diseño) -- no
        hay id disponible en el dato historico para desambiguar mejor.
        """
        asignado_a_nuevo = (asignado_a_nuevo or "").strip()
        if not asignado_a_nuevo:
            # Desasignacion: no hay a quien notificar. El _tk_log ya deja
            # constancia en Actividad -- eso basta (ver diseño, guard abierto).
            return
        row = mysql_fetchone(
            "SELECT id, username, COALESCE(nombre, username) AS nombre "
            "FROM app_users WHERE active=1 AND COALESCE(nombre, username)=%s LIMIT 1",
            (asignado_a_nuevo,))
        if not row:
            # Texto libre historico o usuario dado de baja -- ya contemplado
            # en el frontend (linea ~2206). No hay a quien avisar.
            return
        destino_id = row.get("id")
        destino_email = (row.get("username") or "").strip()

        # Auto-asignacion: omitir ruido (la persona ya sabe que se asigno
        # a si misma).
        if actor_username and destino_email and actor_username.strip().lower() == destino_email.strip().lower():
            return

        extracto = (titulo or "").strip()[:180]

        # Campana in-app (dirigida, no broadcast).
        try:
            _mant_notificar = ctx.get("_mant_notificar")
            if _mant_notificar and destino_id:
                _mant_notificar(
                    destino_id, "otro",
                    f"Te asignaron el ticket {numero}", cuerpo=extracto,
                    url_accion=f"/tickets/{tid}", prioridad="media")
        except Exception as _en:
            print(f"[tk_notificar_asignacion] no se pudo crear notif interna tid={tid}: {_en}", flush=True)

        # Correo al usuario asignado (NO al cliente del ticket -- ese es
        # _tk_notificar_lifecycle, un flujo distinto). 2026-07-12 (Daniel:
        # "falta la plantilla de ticket asignado... algo completamente
        # editable en el front") -- ahora pasa por el MISMO motor editable
        # (comm_templates, modulo='tickets', estado='asignacion') que ya
        # usan resuelto/cerrado/etc. El HTML hardcodeado de abajo es SOLO
        # el fallback si la plantilla no existe/esta apagada (ver diseño
        # de _render_comm_template: None => degradar, nunca omitir).
        if not (_send_ilus_email and destino_email):
            return
        try:
            extracto_html = (
                (": " + _html_mod.escape(extracto)) if extracto else ""
            )
            tema_default = f"Te asignaron el ticket {numero}"
            cuerpo_default = (
                "<div style=\"border-left:4px solid #3b82f6;background:#eff6ff;"
                "border-radius:0 10px 10px 0;padding:18px 20px;margin:0 0 6px\">"
                "<div style=\"font-size:16px;color:#111827;line-height:1.6\">"
                + (_html_mod.escape(extracto) if extracto else "Revisa el ticket para ver el detalle.")
                + "</div></div>")
            tema, cuerpo_html = tema_default, cuerpo_default
            if _render_comm_template:
                try:
                    tpl = _render_comm_template(
                        "asignacion", "email",
                        {"destinatario": row.get("nombre") or destino_email,
                         "numero_ticket": numero, "extracto": extracto_html},
                        modulo="tickets")
                    if tpl:
                        _asu, _cue = tpl
                        if (_asu or "").strip():
                            tema = _asu.strip()
                        if (_cue or "").strip():
                            cuerpo_html = _cue
                except Exception as _e:
                    print(f"[tk_notificar_asignacion] plantilla no usada tid={tid}: {_e}", flush=True)
            # Salvaguarda: el numero de ticket SIEMPRE debe estar en el
            # asunto (mismo patron que _tk_notificar_lifecycle), aunque
            # Daniel edite la plantilla y se le olvide {{numero_ticket}}.
            if numero not in tema:
                tema = f"{tema} · {numero}"
            tema = _brand_subject(tema)
            html_final = (_comm_render_email_document(tema, cuerpo_html, subtitle="Servicio Técnico · ILUS Fitness")
                          if _comm_render_email_document else cuerpo_html)
            enviado = _send_ilus_email(destino_email, tema, html_final,
                                        evento="ticket_asignacion", modulo="tickets",
                                        reply_to=_tk_reply_to())
        except Exception as _e:
            print(f"[tk_notificar_asignacion] error enviando tid={tid}: {_e}", flush=True)

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
            "SELECT numero_ticket, email, empresa, nombre_contacto, estado "
            "FROM tk_tickets WHERE id=%s", (tid,))
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
        # REDISENO 2026-07-12 (Daniel, con screenshot del correo recibido):
        # "lo menos que se hace entender es el mensaje... queremos transmitir
        # que le llegue el mensaje al cliente". El mensaje del ejecutivo es
        # EL PROTAGONISTA: tipografia mas grande, bloque destacado con acento
        # rojo ILUS, y CERO texto de relleno alrededor (se quito la linea
        # "parte del seguimiento" -- el numero ya va en asunto y subtitulo).
        # Numero primero (Daniel 2026-07-12, screenshot Gmail movil): la app
        # trunca asuntos largos en la lista de bandeja y el numero, al ir al
        # final, quedaba cortado ("Respuesta a tu ticket TK-2026-0...").
        tema_default = f"{numero} — Respuesta a tu ticket"
        cuerpo_default = (
            f'<p style="font-size:14px;color:#6b7280;margin:0 0 14px">Hola {_html_mod.escape(cliente_nombre)},</p>'
            f'<div style="border-left:4px solid #dc2626;background:#fafafa;border-radius:0 10px 10px 0;'
            f'padding:18px 20px;margin:0 0 6px">'
            f'<div style="font-size:16px;color:#111827;line-height:1.6">{contenido}</div>'
            f'</div>')
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
        # MODO PRUEBA (TK_TEST_EMAIL_TO): to_email/subject originales se
        # conservan para el log de abajo; solo el envio real se redirige.
        # El CC se descarta en modo prueba -- un cc real igual llegaria a un
        # cliente real si solo redirigimos el "to".
        to_envio, subject_envio = _tk_test_redirect(to_email, subject)
        cc_envio = cc_email if to_envio == to_email else ""
        html_final = (_comm_render_email_document(subject_envio, cuerpo_email, subtitle="Servicio Técnico · ILUS Fitness")
                      if _comm_render_email_document else cuerpo_email)

        try:
            kwargs = {"evento": "ticket_respuesta", "modulo": "tickets", "reply_to": _tk_reply_to()}
            if cc_envio:
                kwargs["cc"] = cc_envio
            enviado = _send_ilus_email(to_envio, subject_envio, html_final, **kwargs)
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

        # AUTO-ESTADO (Daniel 2026-07-14, URGENTE): "cuando respondamos un
        # ticket, ya tiene que pasar a en curso". Solo aplica si:
        #   1) la respuesta AL CLIENTE se envio de verdad (enviado=True) --
        #      un envio fallido deja el mensaje guardado para reintentar,
        #      y el estado cambiara cuando el reintento salga; y
        #   2) el ticket sigue en su estado INICIAL 'open' -- JAMAS se
        #      retrocede uno que ya avanzo (pending/resolved/closed/ot_*).
        # El WHERE repite estado='open' como guarda anti-carrera (si otro
        # usuario lo movio entre el SELECT de arriba y este UPDATE, no se
        # pisa). Nota: NO se dispara _tk_notificar_lifecycle aqui a
        # proposito -- el cliente acaba de recibir la respuesta; mandarle
        # ademas el correo de "tu ticket esta en curso" seria spam.
        if enviado and (t.get("estado") or "") == "open":
            try:
                mysql_execute(
                    "UPDATE tk_tickets SET estado='in_progress' "
                    "WHERE id=%s AND estado='open'", (tid,))
                _tk_log(tid, "cambio_estado",
                        f"Estado: {ESTADO_LABEL['open']} → {ESTADO_LABEL['in_progress']} "
                        "(cambiado automáticamente al responder al cliente)",
                        metadata={"campo": "estado", "antes": "open",
                                  "nuevo": "in_progress", "auto": True,
                                  "motivo": "respuesta_al_cliente"})
            except Exception as _e:
                # Nunca rompe el flujo de la respuesta (el correo YA salio).
                print(f"[tk_responder_cliente] auto in_progress fallo tid={tid}: {_e}", flush=True)

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
    #  API — Usuarios asignables como "responsable" de un ticket.
    #  2026-07-12 (Daniel, insistiendo): "cuando tienes que asignar un
    #  responsable, salen TODOS -- nosotros habíamos definido en la matriz
    #  de roles quién es ejecutivo/técnico, esa lista no puede ser tan
    #  amplia". El dropdown usaba /mantenciones/api/ejecutivos (a
    #  propósito SIN filtrar por rol, según su propio docstring -- ese
    #  endpoint es de Mantenciones, anterior a que existieran los flags
    #  tk_es_ejecutivo/tk_es_tecnico). Este es el reemplazo específico de
    #  Tickets: solo usuarios cuyo ROL tiene el flag tickets.es_ejecutivo
    #  (superadmin/admin heredan el flag automáticamente, igual que en
    #  _legacy_permission_set) O tickets.es_tecnico (para asignar también
    #  a un técnico como responsable si aplica).
    # ─────────────────────────────────────────────────────────────────
    @app.route("/tickets/api/asignables", methods=["GET"])
    @_tickets_required
    def tk_api_asignables():
        rows = mysql_fetchall(
            "SELECT u.id, COALESCE(u.nombre, u.username) AS nombre, "
            "       u.username AS email, u.role "
            "  FROM app_users u "
            " WHERE u.active=1 "
            "   AND ( "
            "     u.role IN ('superadmin','admin') "
            "     OR EXISTS (SELECT 1 FROM rol_permisos rp "
            "                 WHERE rp.rol_slug=u.role AND rp.modulo='tickets' "
            "                   AND rp.accion IN ('es_ejecutivo','es_tecnico') "
            "                   AND rp.permitido=1) "
            "   ) "
            " ORDER BY nombre"
        ) or []
        return jsonify([dict(r) for r in rows])

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
    #  API — config del modulo: "correo que da la cara" (Reply-To tickets)
    #  Editable desde Comunicaciones -> Tickets sin env var ni deploy.
    #  Persiste en tk_settings (clave 'reply_to'). Independiente del
    #  Reply-To de marca GLOBAL (comm_brand) de los demas modulos.
    # ─────────────────────────────────────────────────────────────────
    @app.route("/tickets/api/config/reply-to", methods=["GET"])
    @_tickets_required
    def tk_api_config_reply_to_get():
        return jsonify({"ok": True, "reply_to": _tk_reply_to()})

    @app.route("/tickets/api/config/reply-to", methods=["POST", "PUT"])
    @_tickets_required
    def tk_api_config_reply_to_set():
        d = request.get_json(silent=True) or {}
        correo = (d.get("correo") or "").strip().lower()
        if not correo:
            return jsonify({"ok": False, "error": "El correo no puede estar vacío."}), 400
        if len(correo) > 200:
            return jsonify({"ok": False, "error": "Correo muy largo (máx 200 caracteres)."}), 400
        if not _TK_REPLY_EMAIL_RE.match(correo):
            return jsonify({"ok": False, "error": "Formato de correo inválido."}), 400
        user = current_username() or "sistema"
        try:
            mysql_execute(
                "INSERT INTO tk_settings (clave, valor, updated_by) "
                "VALUES ('reply_to', %s, %s) "
                "ON DUPLICATE KEY UPDATE valor=VALUES(valor), updated_by=VALUES(updated_by)",
                (correo, user))
        except Exception as _e:
            print(f"[tk_config_reply_to] error guardando: {_e}", flush=True)
            return jsonify({"ok": False,
                            "error": "No se pudo guardar el correo, intenta de nuevo."}), 500
        try:
            _audit("tk_config_reply_to_set", target_type="tk_settings",
                   target_id="reply_to", details={"reply_to": correo})
        except Exception:
            pass
        return jsonify({"ok": True, "reply_to": correo})

    # ─────────────────────────────────────────────────────────────────
    #  API — VISTA PREVIA REAL de plantilla (editor de /comunicaciones)
    #  Daniel 2026-07-12: el editor de plantillas de tickets debe ser
    #  "bien potente" -- hoy Daniel edita a ciegas porque el cuerpo se
    #  envuelve SIEMPRE server-side con el diseno maestro de marca.
    #  Este endpoint toma el BORRADOR actual (sin guardar), reemplaza
    #  los placeholders con datos de muestra y arma el HTML COMPLETO
    #  con el MISMO pipeline de un envio real (cuerpo + invitacion a
    #  responder + _comm_render_email_document). Solo previsualiza --
    #  no guarda ni envia nada.
    # ─────────────────────────────────────────────────────────────────
    # 2026-07-12: estaba desactualizado (solo 4 de los 9 slugs ya sembrados
    # en _tickets_tpl_seed) -- "Restaurar original"/"Vista previa" del
    # editor de Comunicaciones devolvia 400 para en_curso/pendiente/
    # ot_generada/ot_en_curso/cancelado, y ahora tambien para 'asignacion'.
    TK_TPL_ESTADOS_VALIDOS = ("creacion", "respuesta", "asignacion", "en_curso",
                              "pendiente", "ot_generada", "ot_en_curso",
                              "resuelto", "cerrado", "cancelado")

    @app.route("/tickets/api/config/preview-plantilla", methods=["POST"])
    @_tickets_required
    def tk_api_config_preview_plantilla():
        d = request.get_json(silent=True) or {}
        asunto = str(d.get("asunto") or "").strip()
        cuerpo = str(d.get("cuerpo") or "")
        # Datos de muestra realistas (mismos placeholders que el envio real)
        muestras = {
            "cliente": "Ana Contreras",
            "numero_ticket": "TK-2026-00123",
            "mensaje": ("Revisamos tu equipo y el repuesto ya está en camino. "
                        "Te avisaremos apenas llegue para coordinar la visita."),
            "destinatario": "Juan Pablo Martínez",
            "extracto": ": Máquina no enciende — Gimnasio GoFit Providencia",
        }
        for var, valor in muestras.items():
            for tok in ("{{%s}}" % var, "{{ %s }}" % var):
                asunto = asunto.replace(tok, valor)
                cuerpo = cuerpo.replace(tok, valor)
        if not asunto:
            asunto = "Vista previa de plantilla"
        # Igual que _tk_notificar_lifecycle / tk_api_responder_cliente:
        # invitacion verde a responder el correo al final del cuerpo.
        try:
            cuerpo_final = cuerpo + _tk_boton_portal_html(0)
        except Exception as _e:
            print(f"[tk_preview_plantilla] invitacion no agregada: {_e}", flush=True)
            cuerpo_final = cuerpo
        asunto_final = _brand_subject(asunto)
        html_final = (_comm_render_email_document(
                          asunto_final, cuerpo_final,
                          subtitle="Ticket TK-2026-00123 · ILUS Fitness")
                      if _comm_render_email_document else cuerpo_final)
        return jsonify({"ok": True, "html": html_final, "asunto": asunto_final})

    # ─────────────────────────────────────────────────────────────────
    #  API — RESTAURAR plantilla a la version original ILUS
    #  La semilla vive en app.py (_tickets_tpl_seed, inyectada por ctx)
    #  -- fuente unica de verdad, la misma que siembra comm_templates
    #  al boot. UPDATE parametrizado + audit log (Reglas #4 y #5).
    # ─────────────────────────────────────────────────────────────────
    @app.route("/tickets/api/config/restaurar-plantilla", methods=["POST"])
    @_tickets_required
    def tk_api_config_restaurar_plantilla():
        d = request.get_json(silent=True) or {}
        estado = (d.get("estado") or "").strip().lower()
        if estado not in TK_TPL_ESTADOS_VALIDOS:
            return jsonify({"ok": False, "error": "Estado de plantilla no válido."}), 400
        # Resuelto en TIEMPO DE REQUEST (no al registrar el modulo): app.py
        # define _tickets_tpl_seed() ~1500 lineas DESPUES de la linea donde
        # llama register_tickets_routes(app, globals()) -- extraerlo ahi
        # (a nivel de modulo, como se hace con _send_ilus_email y otros)
        # capturaria None para siempre, porque en ese momento la funcion aun
        # no existe en el dict. Pero ctx SI es el dict vivo de globals(), y
        # por la hora en que llega una request real el arranque de app.py ya
        # termino -- por eso se resuelve aqui adentro, no arriba.
        _tickets_tpl_seed = ctx.get("_tickets_tpl_seed")
        if not _tickets_tpl_seed:
            return jsonify({"ok": False,
                            "error": "La versión original no está disponible en este entorno."}), 200
        try:
            seed = _tickets_tpl_seed() or {}
        except Exception as _e:
            print(f"[tk_restaurar_plantilla] error leyendo semilla: {_e}", flush=True)
            return jsonify({"ok": False, "error": "No se pudo leer la versión original."}), 500
        if estado not in seed:
            return jsonify({"ok": False,
                            "error": "Esta plantilla no tiene una versión original."}), 200
        asunto, cuerpo = seed[estado]
        try:
            mysql_execute(
                "UPDATE comm_templates SET asunto=%s, cuerpo=%s "
                "WHERE modulo='tickets' AND canal='email' AND estado=%s",
                (asunto, cuerpo, estado))
        except Exception as _e:
            print(f"[tk_restaurar_plantilla] error guardando: {_e}", flush=True)
            return jsonify({"ok": False,
                            "error": "No se pudo restaurar la plantilla, intenta de nuevo."}), 500
        try:
            _audit("tk_plantilla_restaurada", target_type="comm_templates",
                   target_id=estado, details={"modulo": "tickets", "canal": "email"})
        except Exception:
            pass
        return jsonify({"ok": True, "asunto": asunto, "cuerpo": cuerpo})

    # ─────────────────────────────────────────────────────────────────
    #  API — marcar leido (sin subir el ticket en la bandeja)
    # ─────────────────────────────────────────────────────────────────
    @app.route("/tickets/api/tickets/<int:tid>/marcar-leido", methods=["PATCH"])
    @_tickets_required
    def tk_api_marcar_leido(tid):
        # updated_at = updated_at para NO disparar ON UPDATE CURRENT_TIMESTAMP.
        mysql_execute(
            "UPDATE tk_tickets SET staff_last_read_at=NOW(), updated_at=updated_at WHERE id=%s", (tid,))
        # Limpia tambien el aviso de la campana de notificaciones (Daniel
        # 2026-07-12: "cuando entra la respuesta y se visualice el mensaje,
        # borre la notificacion") -- mismo "ya lo vi" que staff_last_read_at
        # de arriba, pero para el otro canal de aviso (visible en toda la
        # app, no solo esta ficha). Se matchea por url_accion, el mismo valor
        # usado al crearla en _tk_leer_correo_entrante.
        try:
            mysql_execute(
                "UPDATE mant_notificaciones SET leida_at=NOW() "
                "WHERE url_accion=%s AND leida_at IS NULL",
                (f"/tickets/{tid}",))
        except Exception as _e:
            print(f"[tk_marcar_leido] no se pudo limpiar notif interna: {_e}", flush=True)
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

    # ── CALENDARIO de la OT (.ics + links Google/Outlook) — Daniel 2026-07-15:
    #    "asi como en las clinicas, correo de agenda... me mandan un correo y
    #    la reserva queda tambien se muestra en mi calendario". Patrón CALCADO
    #    de pickups_module._build_pickup_ics / _pickup_calendar_links (Regla
    #    #4: se replica el patrón ya probado, no se reinventa el formato
    #    iCalendar). FASE 1: destinatario fijo TK_OT_CONFIRMACION_EMAIL_TEST
    #    (ver constante arriba) -- NO se manda al técnico real todavía.
    def _tk_ot_event_dt(fecha_programada, hora_inicio, hora_fin):
        """(start, end) como datetime NAIVE en hora local Chile, o
        (None, None) si no hay fecha resoluble. Si falta hora_fin, +1h
        (mismo criterio que _pickup_event_dt)."""
        try:
            d = datetime.strptime(str(fecha_programada)[:10], "%Y-%m-%d").date()
        except Exception:
            return None, None
        def _as_hm(v):
            s = str(v) if v else ""
            if not s:
                return None
            parts = s.split(":")
            try:
                h = int(parts[0]); m = int(parts[1]) if len(parts) > 1 else 0
                return h % 24, max(0, min(59, m))
            except Exception:
                return None
        hm_i = _as_hm(hora_inicio)
        if not hm_i:
            return None, None
        start = datetime(d.year, d.month, d.day, hm_i[0], hm_i[1])
        hm_f = _as_hm(hora_fin)
        if hm_f:
            end = datetime(d.year, d.month, d.day, hm_f[0], hm_f[1])
            if end <= start:
                end = start + timedelta(hours=1)
        else:
            end = start + timedelta(hours=1)
        return start, end

    def _tk_ot_url(tid):
        base = (os.environ.get("ILUS_APP_BASE_URL")
                or "https://ilus-app-469212710544.southamerica-west1.run.app").rstrip("/")
        return f"{base}/tickets/{tid}"

    def _build_ot_ics(tid, numero_ot, fecha_programada, hora_inicio, hora_fin,
                       titulo, direccion, tecnicos_nombres):
        """Bytes del .ics (VEVENT) de la OT, o None si no hay fecha/hora
        resoluble. UID ESTABLE por OT (ot-{numero}@ilusfitness.com) -- un
        reenvío ACTUALIZA el evento en vez de duplicarlo (mismo criterio que
        retiros). Hora en UTC (sufijo Z, sin VTIMEZONE)."""
        try:
            start, end = _tk_ot_event_dt(fecha_programada, hora_inicio, hora_fin)
            if not start or not end:
                return None
            _tz = ZoneInfo("America/Santiago") if ZoneInfo else None
            _utc = ZoneInfo("UTC") if ZoneInfo else None
            if not _tz or not _utc:
                return None
            su = start.replace(tzinfo=_tz).astimezone(_utc)
            eu = end.replace(tzinfo=_tz).astimezone(_utc)
            numero = numero_ot or f"TID{tid}"
            link = _tk_ot_url(tid)

            def _esc(s):
                return (str(s or "").replace("\\", "\\\\").replace(",", "\\,")
                        .replace(";", "\\;").replace("\n", "\\n").replace("\r", ""))

            resumen_titulo = f"OT {numero} — {titulo}" if titulo else f"OT {numero}"
            desc = f"Orden de trabajo {numero}."
            if tecnicos_nombres:
                desc += f" Técnico(s): {', '.join(tecnicos_nombres)}."
            desc += f" Ticket: {link}"
            lines = [
                "BEGIN:VCALENDAR", "VERSION:2.0",
                "PRODID:-//ILUS Sport & Health//Tickets//ES",
                "CALSCALE:GREGORIAN", "METHOD:PUBLISH",
                "BEGIN:VEVENT",
                f"UID:ot-{numero}@ilusfitness.com",
                f"DTSTAMP:{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}",
                f"DTSTART:{su.strftime('%Y%m%dT%H%M%SZ')}",
                f"DTEND:{eu.strftime('%Y%m%dT%H%M%SZ')}",
                f"SUMMARY:{_esc(resumen_titulo)}",
                f"LOCATION:{_esc(direccion or '')}",
                f"DESCRIPTION:{_esc(desc)}",
                f"URL:{_esc(link)}",
                "STATUS:CONFIRMED",
                "BEGIN:VALARM", "TRIGGER:-PT2H", "ACTION:DISPLAY",
                f"DESCRIPTION:{_esc('Recordatorio OT ' + numero)}",
                "END:VALARM",
                "END:VEVENT", "END:VCALENDAR",
            ]
            return ("\r\n".join(lines) + "\r\n").encode("utf-8")
        except Exception as exc:
            print(f"[tk-ot-ics] no se pudo construir .ics de OT #{tid}: {exc}", flush=True)
            return None

    def _ot_calendar_links(numero_ot, fecha_programada, hora_inicio, hora_fin,
                            titulo, direccion, tid):
        """{'google':url,'outlook':url} para los botones 'Agregar a
        calendario' del correo de confirmación de la OT. Vacío si no hay
        fecha/hora resoluble. Mismo patrón que _pickup_calendar_links
        (Google/Outlook en UTC con sufijo Z)."""
        try:
            from urllib.parse import quote_plus
            start, end = _tk_ot_event_dt(fecha_programada, hora_inicio, hora_fin)
            if not start or not end or not ZoneInfo:
                return {"google": "", "outlook": ""}
            _tz = ZoneInfo("America/Santiago"); _utc = ZoneInfo("UTC")
            su = start.replace(tzinfo=_tz).astimezone(_utc)
            eu = end.replace(tzinfo=_tz).astimezone(_utc)
            numero = numero_ot or f"TID{tid}"
            title = f"OT {numero}" + (f" — {titulo}" if titulo else "")
            link = _tk_ot_url(tid)
            details = f"Orden de trabajo {numero}. Ticket: {link}"
            google = (
                "https://calendar.google.com/calendar/render?action=TEMPLATE"
                "&text=" + quote_plus(title)
                + "&dates=" + su.strftime("%Y%m%dT%H%M%SZ") + "/" + eu.strftime("%Y%m%dT%H%M%SZ")
                + "&location=" + quote_plus(direccion or "")
                + "&details=" + quote_plus(details)
            )
            outlook = (
                "https://outlook.live.com/calendar/0/deeplink/compose?path=/calendar/action/compose&rru=addevent"
                "&subject=" + quote_plus(title)
                + "&startdt=" + su.strftime("%Y-%m-%dT%H:%M:%SZ")
                + "&enddt=" + eu.strftime("%Y-%m-%dT%H:%M:%SZ")
                + "&location=" + quote_plus(direccion or "")
                + "&body=" + quote_plus(details)
            )
            return {"google": google, "outlook": outlook}
        except Exception:
            return {"google": "", "outlook": ""}

    def _tk_enviar_confirmacion_ot(tid, numero_ot, fecha_programada, hora_inicio,
                                    hora_fin, titulo, direccion, tecnicos_nombres,
                                    cliente_nombre):
        """Correo de confirmación 'reserva de clínica' al generar la OT desde
        un ticket (Daniel 2026-07-15). FASE 1 (alcance explícito): SIEMPRE a
        TK_OT_CONFIRMACION_EMAIL_TEST -- el técnico real NO recibe este correo
        todavía (fase futura). Best-effort: nunca bloquea la creación de la OT
        si el envío falla (mismo criterio que el resto del módulo). Devuelve
        True/False (para loguear en tk_mensajes)."""
        if not _send_ilus_email:
            return False
        try:
            ics_bytes = _build_ot_ics(tid, numero_ot, fecha_programada, hora_inicio,
                                       hora_fin, titulo, direccion, tecnicos_nombres)
            links = _ot_calendar_links(numero_ot, fecha_programada, hora_inicio,
                                        hora_fin, titulo, direccion, tid)
            numero = numero_ot or f"TID{tid}"
            hora_txt = f"{hora_inicio or ''}" + (f" – {hora_fin}" if hora_fin else "")
            html = f"""
            <p>Se agendó la <strong>OT {_html_mod.escape(numero)}</strong>
               generada desde el ticket <a href="{_html_mod.escape(_tk_ot_url(tid))}">#{tid}</a>.</p>
            <table style="border-collapse:collapse;margin:12px 0">
              <tr><td style="padding:4px 12px 4px 0;color:#6b7280">Fecha</td>
                  <td style="padding:4px 0"><strong>{_html_mod.escape(str(fecha_programada or ''))}</strong></td></tr>
              <tr><td style="padding:4px 12px 4px 0;color:#6b7280">Horario</td>
                  <td style="padding:4px 0"><strong>{_html_mod.escape(hora_txt) or 'Sin horario definido'}</strong></td></tr>
              <tr><td style="padding:4px 12px 4px 0;color:#6b7280">Cliente</td>
                  <td style="padding:4px 0">{_html_mod.escape(cliente_nombre or 'Sin cliente')}</td></tr>
              <tr><td style="padding:4px 12px 4px 0;color:#6b7280">Dirección</td>
                  <td style="padding:4px 0">{_html_mod.escape(direccion or 'Sin dirección')}</td></tr>
              <tr><td style="padding:4px 12px 4px 0;color:#6b7280">Técnico(s)</td>
                  <td style="padding:4px 0">{_html_mod.escape(', '.join(tecnicos_nombres) or 'Sin asignar')}</td></tr>
            </table>
            """
            if links.get("google"):
                html += (f'<p><a href="{_html_mod.escape(links["google"])}" '
                          'style="background:#3b82f6;color:#fff;padding:8px 14px;'
                          'border-radius:6px;text-decoration:none;margin-right:8px">'
                          'Agregar a Google Calendar</a>')
                if links.get("outlook"):
                    html += (f'<a href="{_html_mod.escape(links["outlook"])}" '
                              'style="background:#0078d4;color:#fff;padding:8px 14px;'
                              'border-radius:6px;text-decoration:none">'
                              'Agregar a Outlook</a>')
                html += "</p>"
            attachments = None
            if ics_bytes:
                attachments = [(f"ot_{numero}.ics", ics_bytes, "text/calendar")]
            subject = _brand_subject(f"OT {numero} agendada")
            return bool(_send_ilus_email(
                TK_OT_CONFIRMACION_EMAIL_TEST, subject, html,
                evento="ot_agendada", modulo="tickets", attachments=attachments))
        except Exception as exc:
            print(f"[tk-ot-confirmacion] no se pudo enviar correo de OT tid={tid}: {exc}", flush=True)
            return False

    # ─────────────────────────────────────────────────────────────────
    #  API — Generar OT (Tickets crea una mant_visita REAL, ligada
    #  bidireccionalmente vía tk_tickets.visita_id). Wizard de 3 pasos del
    #  frontend (Cliente/equipo -> Técnico/horario -> Confirmación). Reusa
    #  el motor de OTs de Mantenciones (_next_ot_number,
    #  _validar_disponibilidad_visita, garantía) tal cual -- NO se duplica
    #  lógica (Regla #4). NO confundir con
    #  /mantenciones/api/tickets/<id>/convertir-en-ot (esa es la tabla vieja
    #  mant_tickets, un sistema distinto -- no se toca, Regla #4.2).
    #
    #  REESCRITO 2026-07-12: este endpoint dejo de tener su PROPIO INSERT en
    #  mant_visitas (implementacion paralela divergente -- no creaba
    #  mant_levantamientos, no soportaba direccion_visita/contacto_*/
    #  acceso_*/plantillas/adjuntos, y exigia cliente_id ya existente). Ahora
    #  es un WRAPPER delgado: arma el payload que espera el nucleo
    #  compartido _mant_lev_crear_ot_core (app.py, el MISMO que usa el modal
    #  real de Mantenciones #modalLevSelector) y lo invoca con ticket_id=tid
    #  para que el vinculo tk_tickets.visita_id quede atomico con la
    #  creacion de la OT. Se CONSERVAN tal cual el GET_LOCK anti-condicion-
    #  de-carrera y el chequeo de feriado/choque de horario ya probados en
    #  la ronda anterior (2026-07-12) -- solo se conectan al flujo correcto.
    # ─────────────────────────────────────────────────────────────────
    @app.route("/tickets/api/tickets/<int:tid>/generar-ot", methods=["POST"])
    @_tickets_required
    def tk_api_generar_ot(tid):
        t = mysql_fetchone("SELECT * FROM tk_tickets WHERE id=%s", (tid,))
        if not t:
            return jsonify({"ok": False, "error": "Ticket no encontrado"}), 404
        if t.get("visita_id"):
            _v_exist = mysql_fetchone(
                "SELECT numero_ot FROM mant_visitas WHERE id=%s", (t["visita_id"],))
            return jsonify({
                "ok": False,
                "error": "Este ticket ya tiene una OT vinculada",
                "visita_id_existente": t["visita_id"],
                "numero_ot_existente": (_v_exist or {}).get("numero_ot"),
            }), 409
        if not _mant_lev_crear_ot_core:
            return jsonify({"ok": False, "error": "Motor de OT no disponible (contacta a soporte)."}), 500

        d = request.get_json(silent=True) or {}

        # ── Técnico(s) -- multi-tecnico igual que el modal real; se acepta
        #    tecnico_ids (lista, preferido) o tecnico_user_id (compat) ──
        tecnico_ids = []
        for x in (d.get("tecnico_ids") or []):
            try:
                tecnico_ids.append(int(x))
            except (TypeError, ValueError):
                pass
        if not tecnico_ids and d.get("tecnico_user_id"):
            try:
                tecnico_ids = [int(d.get("tecnico_user_id"))]
            except (TypeError, ValueError):
                tecnico_ids = []
        tecnico_principal = tecnico_ids[0] if tecnico_ids else None
        fecha_programada = (d.get("fecha_programada") or "").strip()

        if not tecnico_principal or not fecha_programada:
            return jsonify({
                "ok": False,
                "error": "Faltan campos requeridos: técnico y fecha programada.",
            }), 400
        try:
            datetime.strptime(fecha_programada, "%Y-%m-%d")
        except Exception:
            return jsonify({"ok": False, "error": "Fecha programada inválida (usa AAAA-MM-DD)."}), 400

        tecnico = mysql_fetchone(
            "SELECT COALESCE(nombre, username) AS nm FROM app_users "
            "WHERE id=%s AND role='tecnico' AND active=1", (tecnico_principal,))
        if not tecnico:
            return jsonify({"ok": False, "error": "El técnico indicado no existe o no está activo."}), 400

        hora_inicio = _normalize_hora(d.get("hora_inicio")) if _normalize_hora else None
        hora_fin = _normalize_hora(d.get("hora_fin")) if _normalize_hora else None
        fecha_fin = (d.get("fecha_fin") or "").strip()[:10] or None

        forzar_feriado = bool(d.get("forzar_feriado"))
        forzar_choque = bool(d.get("forzar_choque"))

        # ── Advertencias (feriado + choque de horario) ANTES de tomar el
        #    lock -- solo sobre el técnico PRINCIPAL (mismo alcance que la
        #    version anterior; extenderlo a multi-tecnico completo queda
        #    fuera de este cambio -- ver reporte). ──
        advertencias = (_validar_disponibilidad_visita(
            tecnico_principal, fecha_programada, hora_inicio, hora_fin,
            exclude_visita_id=0) if _validar_disponibilidad_visita else {}) or {}
        pend_feriado = advertencias.get("feriado") if not forzar_feriado else None
        pend_choque = advertencias.get("choque") if not forzar_choque else None
        if pend_feriado or pend_choque:
            return jsonify({
                "ok": False,
                "requiere_confirmacion": True,
                "advertencias": {"feriado": pend_feriado, "choque": pend_choque},
            })

        # ── tipo_ot: whitelist real del modal (#otTipo) -- mismo vocabulario
        #    que usa mant_lev_crear_o_listar/_mant_lev_crear_ot_core (NO el
        #    vocabulario legado tipo_visita/'garantia', que ahora es un flag
        #    aparte -- aplica_garantia). Si el wizard no manda uno valido,
        #    se infiere del tipo del ticket (mapeo TK_TIPOS -> tipo_ot). ──
        tipos_ot_ok = ("levantamiento", "instalacion", "preventiva",
                       "correctiva", "visita_tecnica", "inspeccion")
        tipo_ot = (d.get("tipo_ot") or "").strip().lower()
        if tipo_ot not in tipos_ot_ok:
            _map_tk_a_ot = {
                "install": "instalacion", "repair": "correctiva",
                "maintenance": "preventiva", "tech_support": "visita_tecnica",
                "tech_evaluation": "inspeccion",
            }
            tipo_ot = _map_tk_a_ot.get(t.get("tipo"), "preventiva")

        # ── Descubrimiento (levantamiento puro, sin equipos previos) --
        #    SOLO tiene sentido si tipo_ot == 'levantamiento' (igual que el
        #    modal real: las tarjetas de modalidad solo aparecen ahi). ──
        descubrimiento = bool(d.get("descubrimiento")) and tipo_ot == "levantamiento"

        # ── Garantía transversal del ticket (es_garantia) -- flag aparte,
        #    igual que ya hace mant_visita_crear/_mant_lev_crear_ot_core
        #    (aplica_garantia=False fuerza modalidad 'pagado', no se
        #    reinventa la logica de cobertura aqui: la resuelve el nucleo). ──
        aplica_garantia = bool(d.get("aplica_garantia"))
        if not aplica_garantia and _parse_garantia_aplica:
            try:
                aplica_garantia = bool(_parse_garantia_aplica(t.get("es_garantia")))
            except Exception:
                aplica_garantia = False

        titulo_final = (d.get("titulo") or "").strip()
        if not titulo_final:
            titulo_final = f"{t.get('titulo') or ('Ticket #' + str(tid))}"
        titulo_final = titulo_final[:200]
        notas_final = (d.get("notas") or "").strip()
        if not notas_final:
            notas_final = f"Generada desde ticket #{tid}\n\n{t.get('descripcion') or ''}"

        # ── Resolver cliente_id: (1) si el frontend ya lo trae explicito
        #    (ficha de Mantenciones ya resuelta al abrir el modal), usarlo
        #    validando que exista; (2) si no, buscar por RUT del ticket
        #    (mismo patron que app.py: mant_clientes WHERE rut=%s -- Regla
        #    contrato "fuente_de_equipos_tickets"). ──
        cliente_id = None
        try:
            cliente_id = int(d.get("cliente_id")) if d.get("cliente_id") else None
        except (TypeError, ValueError):
            cliente_id = None
        if cliente_id and not mysql_fetchone("SELECT id FROM mant_clientes WHERE id=%s", (cliente_id,)):
            cliente_id = None

        cliente_recien_creado = False
        if not cliente_id:
            rut_ticket = (t.get("rut") or "").strip()
            rut_norm = (normalizar_rut(rut_ticket) if normalizar_rut else rut_ticket) or None
            match = mysql_fetchone(
                "SELECT id, razon_social FROM mant_clientes WHERE rut=%s LIMIT 1",
                (rut_norm,)) if rut_norm else None
            if match:
                cliente_id = match["id"]
            else:
                # ── Cliente/equipo 100% nuevo (instalacion sin ficha aun) ──
                # mant_visitas.cliente_id es NOT NULL/FK -- no hay forma de
                # crear la OT sin una fila en mant_clientes. Se crea una
                # ficha MINIMA (razon_social+rut+contacto+direccion, todo
                # tomado del propio ticket) en vez de bloquear "Generar OT".
                # DECISION DE PRODUCTO ASUMIDA (no confirmada explicitamente
                # por Daniel -- contrato la deja como riesgo abierto #1;
                # ver reporte). Reversible: es solo un alta en mant_clientes,
                # editable despues desde la ficha normal.
                razon_social_nueva = (t.get("empresa") or "").strip()[:200] \
                    or f"Cliente ticket #{tid}"
                mysql_execute(
                    "INSERT INTO mant_clientes "
                    "(razon_social, rut, contacto_nombre, contacto_tel, contacto_email, "
                    " direccion, comuna, region, estado, created_by) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'prospecto',%s)",
                    (razon_social_nueva, rut_norm,
                     (t.get("nombre_contacto") or "").strip()[:200] or None,
                     (t.get("phone") or "").strip()[:50] or None,
                     (t.get("email") or "").strip()[:200] or None,
                     (t.get("direccion") or "").strip()[:400] or None,
                     (t.get("comuna_nombre") or "").strip()[:100] or None,
                     (t.get("region_nombre") or "").strip()[:100] or None,
                     current_username() or "sistema")
                )
                _cli_new = mysql_fetchone("SELECT LAST_INSERT_ID() AS id")
                cliente_id = (_cli_new or {}).get("id")
                cliente_recien_creado = True
                _tk_log(tid, "otro",
                        f"Cliente creado automaticamente en Mantenciones (ficha minima, "
                        f"sin confirmacion explicita) para poder generar la OT: "
                        f"«{razon_social_nueva}» (ID {cliente_id}).")
        if not cliente_id:
            return jsonify({"ok": False, "error": "No fue posible resolver ni crear el cliente para la OT."}), 400

        # ── Equipos: de tk_ticket_equipos (NO del DOM de una ficha, NO se
        #    pide a mant_maquinas directo -- Regla contrato
        #    "fuente_de_equipos_tickets"). Los que YA tienen maquina_id
        #    (equipo real en la ficha) van por equipo_ids (igual que el
        #    modal real); los que NO tienen ficha van por equipos_ticket
        #    (nucleo los inserta con maquina_id=NULL). Si el cliente es
        #    NUEVO (recien creado arriba), no existe ninguna ficha real
        #    todavia -- TODOS los equipos del ticket se preseleccionan tal
        #    cual, no hay checkboxes que ofrecer. ──
        eq_ticket_rows = mysql_fetchall(
            "SELECT id, nombre, sku, serie, maquina_id, notas "
            "  FROM tk_ticket_equipos WHERE ticket_id=%s ORDER BY id", (tid,)) or []
        if cliente_recien_creado:
            equipo_ids = []
            equipos_ticket_payload = [
                {"nombre": r.get("nombre"), "sku": r.get("sku"), "serie": r.get("serie"),
                 "maquina_id": None}
                for r in eq_ticket_rows
            ]
        else:
            equipo_ids_body = []
            for x in (d.get("equipo_ids") or d.get("maquina_ids") or []):
                try:
                    equipo_ids_body.append(int(x))
                except (TypeError, ValueError):
                    pass
            if equipo_ids_body:
                equipo_ids = equipo_ids_body
            else:
                # Sin seleccion explicita del wizard: cae a TODOS los equipos
                # del ticket que YA tienen maquina_id (mismo fallback de la
                # implementacion anterior).
                equipo_ids = [r["maquina_id"] for r in eq_ticket_rows if r.get("maquina_id")]
            equipos_ticket_payload = [
                {"nombre": r.get("nombre"), "sku": r.get("sku"), "serie": r.get("serie"),
                 "maquina_id": None}
                for r in eq_ticket_rows if not r.get("maquina_id")
            ]

        # ── Dirección de la visita: explícita del wizard o default del
        #    propio ticket (si el cliente no tiene ficha, no hay
        #    DATA.cliente_direccion de donde traerla -- Regla contrato
        #    "frontend_adaptacion"). ──
        direccion_visita = (d.get("direccion_visita") or "").strip()[:400] \
            or (t.get("direccion") or "").strip()[:400] or None
        try:
            direccion_lat = float(d.get("direccion_lat")) if d.get("direccion_lat") is not None \
                else (float(t["direccion_lat"]) if t.get("direccion_lat") is not None else None)
        except (TypeError, ValueError):
            direccion_lat = None
        try:
            direccion_lng = float(d.get("direccion_lng")) if d.get("direccion_lng") is not None \
                else (float(t["direccion_lng"]) if t.get("direccion_lng") is not None else None)
        except (TypeError, ValueError):
            direccion_lng = None
        direccion_place_id = (d.get("direccion_place_id") or "").strip()[:200] \
            or (t.get("direccion_place_id") or None)

        # ── Contacto en sitio: explícito o default del ticket. ──
        contacto_nombre = (d.get("contacto_nombre") or "").strip()[:200] \
            or (t.get("nombre_contacto") or "").strip()[:200] or None
        contacto_tel = (d.get("contacto_tel") or "").strip()[:50] \
            or (t.get("phone") or "").strip()[:50] or None
        contacto_email = (d.get("contacto_email") or "").strip()[:200] \
            or (t.get("email") or "").strip()[:200] or None
        contacto_cargo = (d.get("contacto_cargo") or "").strip()[:120] or None
        contacto_origen = (d.get("contacto_origen") or "").strip()[:40] or "ticket"

        # ── GET_LOCK anti condición de carrera (SIN CAMBIOS respecto a la
        #    version anterior, ya probada 2026-07-12) -- serializa el
        #    re-chequeo de choque de horario del MISMO técnico principal.
        #    La colisión de numero_ot (global) la cubre por su cuenta
        #    _next_ot_number_atomic (row-lock dedicado) DENTRO del nucleo
        #    compartido, que se invoca mas abajo. ──
        lock_name = f"ot_choque_{tecnico_principal}_{fecha_programada}"
        conn = get_mysql()
        lock_held = False
        advertencias2 = {"feriado": None, "choque": None}
        try:
            with conn.cursor() as cur0:
                cur0.execute("SELECT GET_LOCK(%s, 5) AS l", (lock_name,))
                lk = cur0.fetchone()
            if not lk or lk.get("l") != 1:
                return jsonify({
                    "ok": False,
                    "error": "Otro usuario está agendando a este técnico en este momento, "
                             "intenta de nuevo en unos segundos.",
                }), 503
            lock_held = True

            # ── Re-chequeo defensivo DENTRO del lock (idéntico al de la
            #    version anterior, ver comentario historico ahi) ──
            _fecha_dt2 = date.fromisoformat(str(fecha_programada))
            pend_feriado2 = None
            pend_choque2 = None
            if not forzar_feriado:
                try:
                    from cl_feriados import es_dia_habil, feriados_chile
                    if not es_dia_habil(_fecha_dt2):
                        _nombre2 = feriados_chile(_fecha_dt2.year).get(_fecha_dt2.isoformat())
                        pend_feriado2 = {"fecha": _fecha_dt2.isoformat(),
                                          "nombre": _nombre2 or "Fin de semana"}
                except Exception:
                    pend_feriado2 = None
            if not forzar_choque:
                with conn.cursor() as cur_chk:
                    cur_chk.execute(
                        "SELECT id, numero_ot, titulo, hora_inicio, hora_fin, tecnico, estado "
                        "  FROM mant_visitas "
                        " WHERE tecnico_user_id = %s "
                        "   AND fecha_programada = %s "
                        "   AND estado NOT IN ('cancelada') "
                        "   AND ("
                        "        hora_inicio IS NULL OR hora_fin IS NULL "
                        "        OR %s IS NULL OR %s IS NULL "
                        "        OR (hora_inicio < %s AND hora_fin > %s)"
                        "   ) "
                        " ORDER BY hora_inicio",
                        (tecnico_principal, _fecha_dt2.isoformat(),
                         hora_inicio, hora_fin, hora_fin, hora_inicio)
                    )
                    _choques2 = cur_chk.fetchall() or []
                if _choques2:
                    with conn.cursor() as cur_tec:
                        cur_tec.execute(
                            "SELECT COALESCE(nombre, username) AS nm FROM app_users WHERE id=%s",
                            (tecnico_principal,))
                        _tec2 = cur_tec.fetchone()
                    pend_choque2 = {
                        "tecnico_nombre": (_tec2 or {}).get("nm") or "Técnico",
                        "visitas": [
                            {
                                "visita_id": c["id"],
                                "numero_ot": c.get("numero_ot"),
                                "hora_inicio": str(c["hora_inicio"]) if c.get("hora_inicio") is not None else None,
                                "hora_fin": str(c["hora_fin"]) if c.get("hora_fin") is not None else None,
                                "titulo": c.get("titulo"),
                            }
                            for c in _choques2
                        ],
                    }
            advertencias2 = {"feriado": pend_feriado2, "choque": pend_choque2}
            if pend_feriado2 or pend_choque2:
                return jsonify({
                    "ok": False,
                    "requiere_confirmacion": True,
                    "advertencias": {"feriado": pend_feriado2, "choque": pend_choque2},
                })

            # ── Delegar al NUCLEO COMPARTIDO (app.py) -- crea
            #    mant_levantamientos + items + OT espejo en mant_visitas +
            #    multi-tecnico + plantillas + vinculo tk_tickets.visita_id,
            #    TODO dentro de su propia transaccion atomica (numero_ot vía
            #    _next_ot_number_atomic). Mismo motor que usa el modal real
            #    de Mantenciones -- Regla #4, no se duplica el INSERT. ──
            lev_payload = {
                "titulo": titulo_final,
                "notas": notas_final,
                "equipo_ids": equipo_ids,
                "descubrimiento": descubrimiento,
                "equipos_ticket": equipos_ticket_payload,
                "fecha_programada": fecha_programada,
                "hora_inicio": hora_inicio,
                "hora_fin": hora_fin,
                "fecha_fin": fecha_fin,
                "tecnico_ids": tecnico_ids,
                "tipo_ot": tipo_ot,
                "aplica_garantia": aplica_garantia,
                "direccion_visita": direccion_visita,
                "direccion_lat": direccion_lat,
                "direccion_lng": direccion_lng,
                "direccion_place_id": direccion_place_id,
                "contacto_nombre": contacto_nombre,
                "contacto_cargo": contacto_cargo,
                "contacto_tel": contacto_tel,
                "contacto_email": contacto_email,
                "contacto_origen": contacto_origen,
                "acceso_ascensor": d.get("acceso_ascensor"),
                "acceso_estacionamiento": d.get("acceso_estacionamiento"),
                "acceso_piso": d.get("acceso_piso"),
                "acceso_notas": d.get("acceso_notas"),
                "plantillas_por_equipo": d.get("plantillas_por_equipo") or {},
            }
            resultado, http_status = _mant_lev_crear_ot_core(cliente_id, lev_payload, ticket_id=tid)
        finally:
            if lock_held:
                try:
                    with conn.cursor() as cur_rel:
                        cur_rel.execute("SELECT RELEASE_LOCK(%s) AS r", (lock_name,))
                except Exception:
                    pass
            conn.close()

        if not resultado.get("ok"):
            return jsonify(resultado), http_status

        vid = resultado.get("visita_id")
        numero_ot = resultado.get("numero_ot")

        # 2026-07-13 (Daniel, URGENTE): "es necesario que le llegue esa
        # informacion al tecnico a la orden de trabajo" -- el motivo/
        # observacion por equipo (tk_ticket_equipos.notas, capturado en el
        # formulario multi-maquina o cargado a mano en la ficha) NUNCA
        # llegaba a mant_visitas.observaciones (la unica que el tecnico ve
        # en "Datos de la OT" -- _mant_lev_crear_ot_core solo escribe la
        # columna `notas`, de uso interno/administrativo, no la que el
        # tecnico lee). Se arma un resumen y se actualiza observaciones
        # DESPUES de crear la OT (update aislado, no toca el nucleo
        # compartido) -- no bloquea la creacion si falla.
        try:
            partes_obs = []
            desc_ticket = (t.get("descripcion") or "").strip()
            if desc_ticket:
                partes_obs.append(desc_ticket)
            motivos_eq = [
                f"• {r.get('nombre') or 'Equipo'}: {r['notas'].strip()}"
                for r in eq_ticket_rows if (r.get("notas") or "").strip()
            ]
            if motivos_eq:
                partes_obs.append("Detalle por equipo:\n" + "\n".join(motivos_eq))
            observaciones_tecnico = "\n\n".join(partes_obs)[:4000]
            if observaciones_tecnico:
                mysql_execute(
                    "UPDATE mant_visitas SET observaciones=%s WHERE id=%s",
                    (observaciones_tecnico, vid))
        except Exception as _e_obs:
            print(f"[tk_api_generar_ot] observaciones no propagadas a la OT vid={vid}: {_e_obs}", flush=True)

        # ── Bitácora (fuera del lock -- trabajo secundario no crítico) ──
        _tk_log(tid, "otro",
                f"Ticket convertido en OT {numero_ot} (visita #{vid}) — fecha {fecha_programada}")
        try:
            _mant_log("visita", vid, "creada_desde_ticket", f"Ticket #{tid} → {numero_ot}")
        except Exception:
            pass
        if forzar_feriado and advertencias2.get("feriado"):
            _f = advertencias2["feriado"]
            _tk_log(tid, "otro",
                    f"⚠ Agendada en día no hábil ({_f.get('nombre')}, {_f.get('fecha')}) "
                    f"— forzado por el usuario.")
        if forzar_choque and advertencias2.get("choque"):
            _c = advertencias2["choque"]
            _otras = ", ".join(
                (v.get("numero_ot") or f"#{v.get('visita_id')}") for v in (_c.get("visitas") or []))
            _tk_log(tid, "otro",
                    f"⚠ Agendada con choque de horario contra {_c.get('tecnico_nombre')} "
                    f"({_otras}) — forzado por el usuario.")

        # ── Correo de confirmación "reserva de clínica" (Daniel 2026-07-15:
        #    "asi como en las clinicas, correo de agenda... me mandan un
        #    correo y la reserva queda tambien se muestra en mi calendario").
        #    FASE 1: SIEMPRE a TK_OT_CONFIRMACION_EMAIL_TEST (ver constante),
        #    NUNCA al técnico real todavía. Best-effort -- no bloquea la
        #    respuesta si falla. ──
        try:
            _tec_rows = mysql_fetchall(
                "SELECT COALESCE(nombre, username) AS nm FROM app_users "
                f" WHERE id IN ({','.join(['%s'] * len(tecnico_ids))})",
                tuple(tecnico_ids)
            ) if tecnico_ids else []
            _tecnicos_nombres = [r["nm"] for r in (_tec_rows or []) if r.get("nm")]
            _cliente_nombre = None
            if cliente_id:
                _crow = mysql_fetchone(
                    "SELECT razon_social FROM mant_clientes WHERE id=%s", (cliente_id,))
                _cliente_nombre = (_crow or {}).get("razon_social")
            _cliente_nombre = _cliente_nombre or (t.get("empresa") or "").strip() or None
            _envio_ok = _tk_enviar_confirmacion_ot(
                tid, numero_ot, fecha_programada, hora_inicio, hora_fin,
                titulo_final, direccion_visita, _tecnicos_nombres, _cliente_nombre)
            _tk_log(tid, "otro",
                    f"Correo de confirmación de agenda de {numero_ot} "
                    f"{'enviado' if _envio_ok else 'NO pudo enviarse'} a "
                    f"{TK_OT_CONFIRMACION_EMAIL_TEST} (fase 1 — destinatario fijo).")
        except Exception as _e_conf:
            print(f"[tk_api_generar_ot] correo de confirmacion no enviado vid={vid}: {_e_conf}", flush=True)

        return jsonify({
            "ok": True,
            "visita_id": vid,
            "numero_ot": numero_ot,
            "fecha_programada": fecha_programada,
            "tareas_creadas": resultado.get("items_plantilla_aplicados"),
            "n_items": resultado.get("n_items"),
            "ot_url": resultado.get("ot_url"),
            "cliente_id": cliente_id,
            "cliente_creado": cliente_recien_creado,
            "tipo_ot": tipo_ot,
        })

    # ─────────────────────────────────────────────────────────────────
    #  API — garantia de un equipo del ticket (Daniel: registrar documento/
    #  fecha de emision/meses de garantia por equipo; default legal 6 meses).
    #  fecha_vencimiento se RECALCULA aqui en cada PATCH -- este es el UNICO
    #  endpoint que escribe estos 5 campos juntos, asi que nunca queda
    #  desincronizada mientras la edicion pase por aca.
    # ─────────────────────────────────────────────────────────────────
    @app.route("/tickets/api/tickets/<int:tid>/equipos/<int:eid>", methods=["PATCH"])
    @_tickets_required
    def tk_api_update_equipo_garantia(tid, eid):
        prev = mysql_fetchone(
            "SELECT * FROM tk_ticket_equipos WHERE id=%s AND ticket_id=%s", (eid, tid))
        if not prev:
            return jsonify({"ok": False, "error": "Equipo no encontrado"}), 404

        d = request.get_json(silent=True) or {}
        # 2026-07-13 (Daniel, URGENTE): "quiero saber de que documento viene...
        # y eso queda registrado" -- se agrega `notas` (comentario libre del
        # equipo, columna ya existente en tk_ticket_equipos, nunca se
        # exponia en este modal).
        allowed = ("con_garantia", "documento_garantia", "notas", "fecha_emision", "garantia_meses")
        if not any(k in d for k in allowed):
            return jsonify({"ok": False, "error": "Sin cambios validos"}), 400

        # con_garantia: bool laxo (acepta true/false, 1/0, "on"/"")
        con_garantia = bool(d["con_garantia"]) if "con_garantia" in d else bool(prev["con_garantia"])

        # garantia_meses: entero > 0, default legal 6 si viene vacio/invalido
        if "garantia_meses" in d:
            try:
                meses = int(d["garantia_meses"])
                if meses <= 0:
                    raise ValueError
            except Exception:
                return jsonify({"ok": False, "error": "Meses de garantía inválidos"}), 400
        else:
            meses = int(prev["garantia_meses"] or 6)

        # fecha_emision: 'YYYY-MM-DD' o null
        if "fecha_emision" in d:
            raw_fecha = (d.get("fecha_emision") or "").strip()
            if raw_fecha:
                try:
                    fecha_emision = datetime.strptime(raw_fecha, "%Y-%m-%d").date()
                except Exception:
                    return jsonify({"ok": False, "error": "Fecha de emisión inválida (usa AAAA-MM-DD)"}), 400
            else:
                fecha_emision = None
        else:
            fecha_emision = prev["fecha_emision"]

        documento = ((d.get("documento_garantia") or "").strip()[:150] or None) \
            if "documento_garantia" in d else prev["documento_garantia"]
        notas = ((d.get("notas") or "").strip()[:500] or None) \
            if "notas" in d else prev["notas"]

        # fecha_vencimiento se recalcula SIEMPRE aqui (unico lugar que escribe
        # estos 3 campos juntos) -- suma de meses sin depender de relativedelta
        # para no agregar una dependencia nueva al proyecto.
        fecha_vencimiento = None
        if fecha_emision:
            total_meses = fecha_emision.month - 1 + meses
            anio = fecha_emision.year + total_meses // 12
            mes = total_meses % 12 + 1
            import calendar
            dia = min(fecha_emision.day, calendar.monthrange(anio, mes)[1])
            fecha_vencimiento = date(anio, mes, dia)

        mysql_execute(
            "UPDATE tk_ticket_equipos SET con_garantia=%s, documento_garantia=%s, notas=%s, "
            "fecha_emision=%s, garantia_meses=%s, fecha_vencimiento=%s "
            "WHERE id=%s AND ticket_id=%s",
            (con_garantia, documento, notas, fecha_emision, meses, fecha_vencimiento, eid, tid))

        user = current_username() or "sistema"
        _tk_log(tid, "otro",
                f"Garantía actualizada — equipo #{eid} ({prev.get('nombre') or prev.get('erp_kopr') or ''}): "
                f"{'con' if con_garantia else 'sin'} garantía"
                + (f", vence {fecha_vencimiento}" if fecha_vencimiento else ""),
                usuario=user,
                metadata={"campo": "garantia", "equipo_id": eid, "con_garantia": con_garantia,
                          "garantia_meses": meses, "fecha_vencimiento": str(fecha_vencimiento) if fecha_vencimiento else None})

        return jsonify({"ok": True, "equipo": {
            "id": eid, "con_garantia": con_garantia, "documento_garantia": documento,
            "fecha_emision": str(fecha_emision) if fecha_emision else None,
            "garantia_meses": meses,
            "fecha_vencimiento": str(fecha_vencimiento) if fecha_vencimiento else None,
        }})

    # ─────────────────────────────────────────────────────────────────
    #  API — traer equipos a un ticket YA EXISTENTE desde un documento ERP
    #  (Daniel 2026-07-11: "ya tenemos las conexiones a random, así que no
    #  debería hacérsenos difícil"). Extendido 2026-07-12 para aceptar
    #  seleccion GRANULAR de lineas (checkboxes del modal de busqueda
    #  avanzada, con saldo por linea -- mismo motor que Retiros via
    #  _tk_fetch_doc_lineas/_cubicador_fetch). Si no vienen `lineas`,
    #  mantiene el comportamiento historico (todas las lineas no-ZZ).
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

        hdr, lineas_reales, via = _tk_fetch_doc_lineas(tido, nudo)
        if not hdr:
            return jsonify({"ok": False, "error": "Documento no encontrado en el ERP"}), 200

        seleccion = d.get("lineas") or []
        if not isinstance(seleccion, list):
            seleccion = []
        lineas = _tk_filtrar_lineas_seleccion(lineas_reales, seleccion)
        if not lineas:
            return jsonify({"ok": False, "error": "El documento no tiene líneas de producto seleccionables"}), 200

        agregados = 0
        try:
            mysql_execute(
                "INSERT IGNORE INTO tk_ticket_documentos (ticket_id, erp_tido, erp_nudo, fecha) "
                "VALUES (%s,%s,%s,%s)",
                (tid, tido[:10], nudo[:40], str(hdr.get("fecha") or "")[:10] or None))
        except Exception as _e:
            print(f"[tk_equipos_desde_doc] documento no registrado tid={tid}: {_e}", flush=True)
        for ln in lineas:
            try:
                cant = max(1, int(round(ln.get("cantidad") or 1)))
                # FIX 2026-07-15: documento_garantia no se llenaba aqui pese a
                # que tido/nudo ya estan en scope -- mismo formato "TIDO-NUDO"
                # que usa el log de mas abajo.
                mysql_execute(
                    "INSERT IGNORE INTO tk_ticket_equipos (ticket_id, erp_kopr, nombre, sku, cantidad, documento_garantia) "
                    "VALUES (%s,%s,%s,%s,%s,%s)",
                    (tid, ln["sku"][:100] or None, ln["nombre"][:300] or "Equipo",
                     ln["sku"][:100] or None, cant, f"{tido}-{nudo}"[:150]))
                agregados += 1
                # Daniel 2026-07-12: "si voy a comprometer una maquina que no
                # tiene saldo, la gerencia tiene que saber el por que... todo
                # registrado con el historial". Nota INTERNA (es_interno=True
                # por defecto en _tk_log) -- no se manda al cliente.
                if ln.get("marcada_sin_saldo"):
                    motivo = ln.get("motivo_sin_saldo") or "(sin justificación registrada)"
                    _tk_log(tid, "otro",
                            f"⚠️ Equipo agregado SIN saldo disponible: {ln['nombre']} "
                            f"(SKU {ln['sku'] or '—'}), doc {tido}-{nudo}. Motivo: {motivo}")
            except Exception as _e:
                print(f"[tk_equipos_desde_doc] equipo no insertado tid={tid} sku={ln.get('sku')}: {_e}", flush=True)

        _tk_log(tid, "otro", f"{agregados} equipo(s) agregado(s) desde documento ERP {tido}-{nudo}"
                + (" (selección manual)" if seleccion else ""))
        return jsonify({"ok": True, "agregados": agregados, "total_lineas": len(lineas),
                         "seleccion_aplicada": bool(seleccion), "motor": via})

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

        mime = _tk_mime_confiable(f.mimetype, ext)
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
        return jsonify({"ok": True, "id": adj_id, "url": url, "nombre": f.filename, "mime": mime})

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
    #
    #  Extendido 2026-07-12: cada documento puede traer, ADEMAS de
    #  {tido, nudo}, una lista OPCIONAL `lineas` con la seleccion GRANULAR
    #  hecha en el modal de busqueda avanzada (checkboxes con saldo real,
    #  mismo patron que Retiros). Si un documento NO trae `lineas`,
    #  mantiene el comportamiento historico: se importan TODAS sus lineas
    #  no-ZZ (no rompe nada que ya funcione).
    # ─────────────────────────────────────────────────────────────────
    @app.route("/tickets/api/tickets/desde-documento", methods=["POST"])
    @_tickets_required
    def tk_api_crear_desde_documento():
        d = request.get_json(silent=True) or {}
        docs = d.get("documentos") or []  # [{tido, nudo, lineas?:[{sku,cantidad,nombre}]}]
        if not docs:
            return jsonify({"ok": False, "error": "Falta al menos un documento"}), 400

        primero = None
        todas_lineas, docs_ok = [], []
        for item in docs[:10]:
            tido = str((item or {}).get("tido") or "").strip()
            nudo = str((item or {}).get("nudo") or "").strip()
            if not (tido and nudo):
                continue
            hdr, lineas_reales, via = _tk_fetch_doc_lineas(tido, nudo)
            if not hdr:
                continue
            if primero is None:
                primero = hdr
            docs_ok.append({"tido": tido, "nudo": nudo, "fecha": hdr.get("fecha")})
            seleccion = (item or {}).get("lineas") or []
            if not isinstance(seleccion, list):
                seleccion = []
            for ln in _tk_filtrar_lineas_seleccion(lineas_reales, seleccion):
                # FIX 2026-07-15: se marca de que documento vino cada linea
                # para poder llenar documento_garantia al insertar (mismo
                # formato "TIDO-NUDO" que el resto de los flujos ERP).
                ln["_doc_garantia"] = f"{tido}-{nudo}"[:150]
                todas_lineas.append(ln)

        if primero is None:
            return jsonify({"ok": False, "error": "Ningún documento fue encontrado en el ERP"}), 200

        tipo = _norm_enum(d.get("tipo"), TK_TIPOS, "tech_support")
        prio = _norm_enum(d.get("prioridad"), TK_PRIORIDADES, "media")
        user = current_username() or "sistema"
        rut = (primero.get("cliente_rut") or "").strip()[:12] or None
        # 2026-07-12 (Daniel): TABCM (comuna del ERP) no trae Region -- se
        # resuelve por Google Geocoding server-side (creacion via ERP no
        # pasa por el navegador). Fail-open: "" si Google no responde.
        _geo_erp = {"region": ""}
        try:
            if _google_geocode_region_comuna:
                _geo_erp = _google_geocode_region_comuna(
                    primero.get("direccion") or "", primero.get("comuna") or "")
        except Exception as _e_geo:
            print(f"[tk_desde_erp] geocode region: {_e_geo}", flush=True)

        conn = get_mysql()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO tk_tickets "
                    "(origen, estado, tipo, prioridad, descripcion, rut, empresa, email, phone, "
                    " direccion, comuna_nombre, region_nombre, numero_documento, asignado_a, created_by) "
                    "VALUES ('erp','open',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (tipo, prio, (d.get("descripcion") or "").strip()[:5000] or None,
                     rut, (primero.get("cliente_nombre") or "")[:150] or None,
                     (primero.get("email") or "")[:150] or None,
                     (primero.get("telefono") or "")[:20] or None,
                     (primero.get("direccion") or "")[:255] or None,
                     (primero.get("comuna") or "")[:120] or None,
                     (_geo_erp.get("region") or "")[:120] or None,
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
                        try:
                            cant = max(1, int(round(float(ln.get("cantidad") or 1))))
                        except Exception:
                            cant = 1
                        cur.execute(
                            "INSERT IGNORE INTO tk_ticket_equipos (ticket_id, erp_kopr, nombre, cantidad, documento_garantia) "
                            "VALUES (%s,%s,%s,%s,%s)",
                            (tid, ln["sku"][:100] or None, ln["nombre"][:300] or "Equipo", cant,
                             ln.get("_doc_garantia")))
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
        # Daniel 2026-07-12: misma trazabilidad que al agregar equipos a un
        # ticket ya existente -- si alguna linea se incluyo sin saldo
        # disponible, queda una nota INTERNA con el motivo que el usuario
        # escribio en el modal (visible para superadmin en la Actividad).
        for ln in todas_lineas:
            if ln.get("marcada_sin_saldo"):
                motivo = ln.get("motivo_sin_saldo") or "(sin justificación registrada)"
                _tk_log(tid, "otro",
                        f"⚠️ Equipo agregado SIN saldo disponible: {ln['nombre']} "
                        f"(SKU {ln.get('sku') or '—'}). Motivo: {motivo}")
        return jsonify({"ok": True, "id": tid, "numero_ticket": numero})

    # ─────────────────────────────────────────────────────────────────
    #  AUTOMATISMO "ZZ-Instalacion" (Daniel 2026-07-12): boton manual,
    #  SOLO superadmin, que revisa documentos ERP nuevos con el SKU de
    #  servicio "ZZINSTALACION" (frozenset ZZ_CODES en erp_engine.py L663-
    #  665 -- OJO: es una sola palabra, sin espacio) y crea un ticket
    #  tipo='install' sin responsable asignado por cada documento nuevo,
    #  para que ninguna instalacion se quede sin ticket. 100% READ-ONLY
    #  contra el ERP (Regla #4.1): solo SELECT via _random_sql_query sobre
    #  MAEEDO/MAEDDO, igual patron que /tickets/api/erp/buscar-cliente-
    #  documentos. NO hay cron/scheduler real (no existe infraestructura
    #  APScheduler/Cloud Scheduler en este proyecto hoy) -- es
    #  deliberadamente bajo demanda; automatizarlo con Cloud Scheduler
    #  externo (mismo patron que FEDEX_CRON_TOKEN) queda para una decision
    #  de infraestructura aparte con Daniel.
    #
    #  Idempotencia: tabla tk_zz_instalacion_scan con UNIQUE(tido,nudo) --
    #  barrera real a nivel de esquema, no solo chequeo en app. Si un
    #  documento quedo registrado pero SIN ticket_id (fallo a mitad de
    #  camino), se reintenta en la proxima corrida.
    # ─────────────────────────────────────────────────────────────────
    def _tk_crear_ticket_zz_instalacion(tido, nudo, user):
        """Crea 1 ticket tipo='install' (sin responsable) a partir de un
        documento ERP con SKU ZZINSTALACION. Reusa el MISMO motor de
        lectura (_tk_fetch_doc_lineas) que /tickets/api/tickets/desde-
        documento -- no se duplica logica de saldo/ERP (Regla: un solo
        motor). Devuelve (ticket_id, numero_ticket) o (None, None) si el
        documento ya no se pudo leer del ERP."""
        hdr, lineas_reales, _via = _tk_fetch_doc_lineas(tido, nudo)
        if not hdr:
            return None, None
        rut = (hdr.get("cliente_rut") or "").strip()[:12] or None
        # 2026-07-12 (Daniel): misma resolucion de Region que en tk_desde_erp
        # -- TABCM no trae region, se resuelve por Google (fail-open).
        _geo_zz = {"region": ""}
        try:
            if _google_geocode_region_comuna:
                _geo_zz = _google_geocode_region_comuna(
                    hdr.get("direccion") or "", hdr.get("comuna") or "")
        except Exception as _e_geo:
            print(f"[tk_zz_auto] geocode region: {_e_geo}", flush=True)
        conn = get_mysql()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO tk_tickets "
                    "(origen, estado, tipo, prioridad, descripcion, rut, empresa, email, phone, "
                    " direccion, comuna_nombre, region_nombre, numero_documento, created_by) "
                    "VALUES ('erp','open','install','media',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (f"Instalación detectada automáticamente desde documento ERP {tido}-{nudo}.",
                     rut, (hdr.get("cliente_nombre") or "")[:150] or None,
                     (hdr.get("email") or "")[:150] or None,
                     (hdr.get("telefono") or "")[:20] or None,
                     (hdr.get("direccion") or "")[:255] or None,
                     (hdr.get("comuna") or "")[:120] or None,
                     (_geo_zz.get("region") or "")[:120] or None,
                     f"{tido}-{nudo}"[:1000], user))
                tid = cur.lastrowid
                cur.execute(
                    "UPDATE tk_tickets SET numero_ticket = "
                    "CONCAT('TK-', %s, '-', LPAD(id,5,'0')) WHERE id=%s",
                    (_chile_now_year(), tid))
                try:
                    cur.execute(
                        "INSERT IGNORE INTO tk_ticket_documentos (ticket_id, erp_tido, erp_nudo) "
                        "VALUES (%s,%s,%s)", (tid, tido[:10], nudo[:40]))
                except Exception as _e:
                    print(f"[tk_zz_instalacion] documento no insertado tid={tid} "
                          f"{tido}/{nudo}: {_e}", flush=True)
                for ln in (lineas_reales or []):
                    sku = (ln.get("sku") or "").strip()
                    nombre = (ln.get("nombre") or "Equipo").strip()
                    try:
                        cant = max(1, int(round(float(ln.get("cantidad") or 1))))
                    except Exception:
                        cant = 1
                    try:
                        cur.execute(
                            "INSERT IGNORE INTO tk_ticket_equipos (ticket_id, erp_kopr, nombre, cantidad, documento_garantia) "
                            "VALUES (%s,%s,%s,%s,%s)",
                            (tid, sku[:100] or None, nombre[:300], cant, f"{tido}-{nudo}"[:150]))
                    except Exception as _e:
                        print(f"[tk_zz_instalacion] equipo no insertado tid={tid} sku={sku}: {_e}", flush=True)
            conn.commit()
        finally:
            conn.close()
        numero = mysql_fetchone("SELECT numero_ticket FROM tk_tickets WHERE id=%s", (tid,))
        numero = numero["numero_ticket"] if numero else None
        _tk_log(tid, "creacion",
                f"Ticket {numero} creado automáticamente por el escaneo ZZ-Instalación "
                f"desde el documento ERP {tido}-{nudo}.")
        return tid, numero

    def _tk_zz_instalacion_scan(dias_default=7, actor=None):
        """Revisa documentos ERP nuevos (MAEEDO+MAEDDO, SOLO LECTURA) con
        SKU exacto 'ZZINSTALACION' desde la ultima fecha escaneada (o los
        ultimos `dias_default` dias si es la primera corrida) y crea 1
        ticket tipo='install' por cada documento nuevo. Devuelve un resumen
        JSON-serializable."""
        resumen = {"documentos_revisados": 0, "tickets_creados": [], "ya_existian": 0, "errores": []}
        if not _random_sql_query:
            resumen["errores"].append("Motor ERP no disponible")
            return resumen

        row = mysql_fetchone(
            "SELECT MAX(fecha_doc) AS ultima FROM tk_zz_instalacion_scan")
        desde = row.get("ultima") if row else None
        if not desde:
            desde = datetime.utcnow() - timedelta(days=int(dias_default))

        try:
            docs = _random_sql_query(
                """
                SELECT DISTINCT
                       e.IDMAEEDO,
                       LTRIM(RTRIM(e.TIDO)) AS TIDO,
                       LTRIM(RTRIM(e.NUDO)) AS NUDO,
                       e.FEEMDO
                  FROM MAEEDO e
                  JOIN MAEDDO d ON d.IDMAEEDO = e.IDMAEEDO
                 WHERE UPPER(LTRIM(RTRIM(d.KOPRCT))) = 'ZZINSTALACION'
                   AND e.FEEMDO >= %s
                   AND (e.ESDO IS NULL OR LTRIM(RTRIM(e.ESDO)) <> 'NULO')
                 ORDER BY e.FEEMDO ASC
                """,
                (desde,), max_rows=500,
            ) or []
        except Exception as _e:
            print(f"[tk_zz_instalacion_scan] error consultando ERP: {_e}", flush=True)
            resumen["errores"].append("No se pudo consultar el ERP ahora")
            return resumen

        resumen["documentos_revisados"] = len(docs)
        for r in docs:
            tido = (r.get("TIDO") or "").strip()
            nudo = (r.get("NUDO") or "").strip()
            fecha_doc = r.get("FEEMDO")
            if not (tido and nudo):
                continue
            existente = mysql_fetchone(
                "SELECT id, ticket_id FROM tk_zz_instalacion_scan WHERE tido=%s AND nudo=%s",
                (tido, nudo))
            if existente and existente.get("ticket_id"):
                resumen["ya_existian"] += 1
                continue
            # RECLAMAR el documento ANTES de crear el ticket (no despues):
            # la fila de control con UNIQUE(tido,nudo) es lo que serializa
            # dos corridas concurrentes -- si el INSERT de reclamo falla por
            # duplicado, es que otra ejecucion ya se adelanto con ESTE mismo
            # documento justo ahora, asi que se salta sin crear un segundo
            # ticket. Antes el ticket se creaba PRIMERO y el registro de
            # control despues, dejando una ventana real de carrera (2 clics
            # casi simultaneos, o 2 superadmins) donde ambas corridas pasaban
            # el chequeo de "existente" y cada una creaba su propio ticket
            # duplicado para el mismo documento.
            if not existente:
                try:
                    mysql_execute(
                        "INSERT INTO tk_zz_instalacion_scan (tido, nudo, fecha_doc, ticket_id, creado_por) "
                        "VALUES (%s,%s,%s,NULL,%s)",
                        (tido, nudo, fecha_doc, actor or "sistema"))
                except Exception:
                    # Ya reclamado por otra ejecucion concurrente en este mismo
                    # instante -- se deja para la proxima corrida, no se crea
                    # un ticket duplicado.
                    resumen["ya_existian"] += 1
                    continue
            try:
                tid, numero = _tk_crear_ticket_zz_instalacion(tido, nudo, actor or "sistema")
            except Exception as _e:
                print(f"[tk_zz_instalacion_scan] error creando ticket {tido}/{nudo}: {_e}", flush=True)
                resumen["errores"].append(f"{tido}-{nudo}: {_e}")
                continue
            if not tid:
                resumen["errores"].append(f"{tido}-{nudo}: documento no encontrado en el ERP")
                continue
            try:
                mysql_execute(
                    "UPDATE tk_zz_instalacion_scan SET ticket_id=%s, fecha_doc=%s WHERE tido=%s AND nudo=%s",
                    (tid, fecha_doc, tido, nudo))
            except Exception as _e:
                print(f"[tk_zz_instalacion_scan] no se pudo registrar control {tido}/{nudo}: {_e}", flush=True)
            resumen["tickets_creados"].append({"tido": tido, "nudo": nudo, "id": tid, "numero_ticket": numero})
        return resumen

    @app.route("/tickets/api/zz-instalacion/escanear", methods=["POST"])
    @_tickets_required
    def tk_api_zz_instalacion_escanear():
        perms = g.get("permissions") or {}
        if not perms.get("superadmin"):
            return jsonify({"ok": False, "error": "Solo un superadministrador puede ejecutar este escaneo."}), 403
        d = request.get_json(silent=True) or {}
        try:
            dias = int(d.get("dias") or 7)
        except Exception:
            dias = 7
        dias = max(1, min(dias, 30))
        user = current_username() or "sistema"
        resumen = _tk_zz_instalacion_scan(dias_default=dias, actor=user)
        try:
            _audit("tk_zz_instalacion_scan", target_type="tk_ticket",
                   details={"documentos_revisados": resumen.get("documentos_revisados"),
                             "creados": len(resumen.get("tickets_creados") or []),
                             "ya_existian": resumen.get("ya_existian")})
        except Exception:
            pass
        # Campos al nivel raiz (ademas de "resumen") para el front ya
        # cableado en templates/tickets/list.html (btnZzInstalacion).
        return jsonify({"ok": True, "resumen": resumen, **resumen})

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
                       LTRIM(RTRIM(COALESCE(en.DIEN, '')))                   AS direccion,
                       LTRIM(RTRIM(COALESCE(en.CMEN, '')))                   AS cmen,
                       LTRIM(RTRIM(COALESCE(en.CIEN, '')))                   AS cien,
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

        # 2026-07-12 (Daniel): "hay RUT con varias direcciones -- necesito
        # identificar cual GoFit/sucursal es" -- se agrega direccion+comuna
        # resuelta (TABCM) a cada resultado, y si ese RUT YA tiene ficha en
        # Clientes (mant_clientes) se marca para que la conexion sea "muy
        # cercana" (evita duplicar un cliente que ya existe).
        ruts_erp = [(r.get("rut") or "").strip() for r in rows if (r.get("rut") or "").strip()]
        clientes_existentes = {}
        if ruts_erp:
            placeholders = ",".join(["%s"] * len(ruts_erp))
            filas_cli = mysql_fetchall(
                f"SELECT id, rut, razon_social FROM mant_clientes WHERE rut IN ({placeholders})",
                tuple(ruts_erp)) or []
            for fc in filas_cli:
                clientes_existentes[(fc.get("rut") or "").strip()] = fc

        resultados = []
        for r in rows:
            if not ((r.get("razon_social") or "").strip() or r.get("rut")):
                continue
            rut = (r.get("rut") or "").strip()
            comuna = ""
            if r.get("cmen") and _resolve_comuna_erp:
                try:
                    comuna = _resolve_comuna_erp(r.get("cmen"), r.get("cien") or "") or ""
                except Exception:
                    comuna = ""
            cli_existente = clientes_existentes.get(rut)
            resultados.append({
                "empresa": (r.get("razon_social") or "").strip() or "(Sin nombre en el ERP)",
                "rut": rut,
                "direccion": (r.get("direccion") or "").strip(),
                "comuna": comuna,
                "ya_es_cliente": bool(cli_existente),
                "cliente_id": cli_existente.get("id") if cli_existente else None,
            })
        return jsonify({"ok": True, "resultados": resultados})

    # ─────────────────────────────────────────────────────────────────
    #  API — ERP: "Búsqueda avanzada de productos" para TICKETS -- motor B
    #  (Daniel 2026-07-12): "algo bien inteligente... que se busque por
    #  factura, que se busque por RUT y que se asigne... que vea si tiene
    #  saldo o no tiene saldo, tal cual como lo haciamos en los retiros".
    #
    #  Replica /retiros/api/buscar-erp (pickups_module.py) pero SIN
    #  acoplarse a conceptos de retiros: el candado aca es "documento ya
    #  asociado a ALGUN ticket" (tk_ticket_documentos + tk_tickets), no
    #  "ya tiene retiro". No se toca pickups_module.py (regla: es
    #  referencia de solo lectura).
    #
    #  100% READ-ONLY (Regla #4.1): unicamente SELECT via _random_sql_query
    #  (whitelist SELECT/WITH, blacklist de escritura, autocommit=False,
    #  siempre parametrizado con %s -- jamas f-strings con el VALOR de q;
    #  los unicos f-strings arman placeholders fijos o la lista fija de
    #  TIDOs, nunca datos de usuario).
    #
    #  Body JSON: {q: str, ticket_id?: int}
    #  Response:  {ok, modo, documentos:[{tido,nudo,tido_display,
    #              nudo_display,rut,razon_social,fecha,fecha_iso,
    #              valor_neto,valor_total,estado_pago,saldo_zz,
    #              saldo_real_unidades,tiene_saldo,n_lineas,ya_asociado,
    #              asociado_ticket_id,asociado_numero_ticket,
    #              asociado_es_este_ticket}], count, query}
    #
    #  Este endpoint SOLO entrega metadata + saldo agregado por documento
    #  (igual que en retiros). Las lineas reales con saldo POR LINEA se
    #  piden aparte contra el motor unico /api/erp/documento (ya generico,
    #  ya calcula saldo -- no se duplica esa logica).
    # ─────────────────────────────────────────────────────────────────
    @app.route("/tickets/api/erp/buscar-cliente-documentos", methods=["POST"])
    @_tickets_required
    def tk_api_erp_buscar_cliente_documentos():
        d = request.get_json(silent=True) or {}
        q = (d.get("q") or "").strip()
        try:
            ticket_id_ctx = int(d.get("ticket_id")) if d.get("ticket_id") else None
        except Exception:
            ticket_id_ctx = None
        if len(q) < 3:
            return jsonify({"ok": False, "error": "Mínimo 3 caracteres", "documentos": []}), 400
        if not _random_sql_query:
            return jsonify({"ok": True, "documentos": [], "modo": "", "count": 0, "query": q,
                             "error": "Motor ERP no disponible", "sin_conexion": True})

        q_clean = q.replace(".", "").replace(" ", "").replace("-", "").upper()
        is_digits = q_clean.isdigit()
        tidos_in = "','".join(("FCV", "BLV", "NVI", "NVV", "GDV", "GDP", "VD", "WEB"))

        docs, modo = [], ""
        try:
            # ── Modo RUT (7-9 dígitos) ──────────────────────────────
            if is_digits and 7 <= len(q_clean) <= 9:
                modo = "rut"
                rut_base = q_clean[:-1] if len(q_clean) >= 8 else q_clean
                docs = _random_sql_query(f"""
                    SELECT TOP 80
                        e.IDMAEEDO,
                        LTRIM(RTRIM(e.TIDO)) AS TIDO,
                        LTRIM(RTRIM(e.NUDO)) AS NUDO,
                        LTRIM(RTRIM(e.ENDO)) AS ENDO,
                        LTRIM(RTRIM(COALESCE(e.SUENDO,''))) AS SUENDO,
                        e.FEEMDO, e.VANEDO, e.VABRDO,
                        LTRIM(RTRIM(COALESCE(e.ESPGDO,''))) AS ESPGDO
                    FROM MAEEDO e
                    WHERE (e.ENDO LIKE %s OR e.ENDO LIKE %s)
                      AND LTRIM(RTRIM(e.TIDO)) IN ('{tidos_in}')
                      AND (e.ESDO IS NULL OR LTRIM(RTRIM(e.ESDO)) <> 'NULO')
                    ORDER BY e.FEEMDO DESC
                """, (f"{rut_base}%", f"%{q_clean}%"), max_rows=80) or []

            # ── Modo Número documento (1-7 dígitos) ─────────────────
            if not docs and is_digits and 1 <= len(q_clean) <= 7:
                modo = "numero"
                nudo_padded = q_clean.zfill(10)
                nudo_vd  = f"VD{q_clean.zfill(8)}"
                nudo_web = f"WEB{q_clean.zfill(7)}"
                docs = _random_sql_query(f"""
                    SELECT TOP 30
                        e.IDMAEEDO,
                        LTRIM(RTRIM(e.TIDO)) AS TIDO,
                        LTRIM(RTRIM(e.NUDO)) AS NUDO,
                        LTRIM(RTRIM(e.ENDO)) AS ENDO,
                        LTRIM(RTRIM(COALESCE(e.SUENDO,''))) AS SUENDO,
                        e.FEEMDO, e.VANEDO, e.VABRDO,
                        LTRIM(RTRIM(COALESCE(e.ESPGDO,''))) AS ESPGDO
                    FROM MAEEDO e
                    WHERE e.NUDO IN (%s, %s, %s)
                      AND LTRIM(RTRIM(e.TIDO)) IN ('{tidos_in}')
                      AND (e.ESDO IS NULL OR LTRIM(RTRIM(e.ESDO)) <> 'NULO')
                    ORDER BY e.FEEMDO DESC
                """, (nudo_padded, nudo_vd, nudo_web), max_rows=30) or []

            # ── Modo Razón social (texto) ────────────────────────────
            if not docs and not is_digits:
                modo = "nombre"
                q_like = f"%{q.upper()}%"
                ruts = _random_sql_query("""
                    SELECT TOP 20 LTRIM(RTRIM(RTEN)) AS rut,
                                  LTRIM(RTRIM(COALESCE(NOKOENAMP, NOKOEN, ''))) AS razon
                      FROM MAEEN
                     WHERE (UPPER(NOKOEN) LIKE %s OR UPPER(COALESCE(NOKOENAMP,'')) LIKE %s)
                       AND TIEN IN ('C','A')
                """, (q_like, q_like)) or []
                if ruts:
                    rut_map = {r['rut']: r['razon'] for r in ruts if r.get('rut')}
                    if rut_map:
                        like_clauses = " OR ".join(["e.ENDO LIKE %s"] * len(rut_map))
                        params = tuple(f"{rk}%" for rk in rut_map.keys())
                        docs = _random_sql_query(f"""
                            SELECT TOP 60
                                e.IDMAEEDO,
                                LTRIM(RTRIM(e.TIDO)) AS TIDO,
                                LTRIM(RTRIM(e.NUDO)) AS NUDO,
                                LTRIM(RTRIM(e.ENDO)) AS ENDO,
                                LTRIM(RTRIM(COALESCE(e.SUENDO,''))) AS SUENDO,
                                e.FEEMDO, e.VANEDO, e.VABRDO,
                                LTRIM(RTRIM(COALESCE(e.ESPGDO,''))) AS ESPGDO
                            FROM MAEEDO e
                            WHERE ({like_clauses})
                              AND LTRIM(RTRIM(e.TIDO)) IN ('{tidos_in}')
                              AND (e.ESDO IS NULL OR LTRIM(RTRIM(e.ESDO)) <> 'NULO')
                            ORDER BY e.FEEMDO DESC
                        """, params, max_rows=60) or []

            # ── Deduplicar por IDMAEEDO ──────────────────────────────
            seen_ids, unique_docs = set(), []
            for r in docs:
                idm = r.get("IDMAEEDO")
                if idm in seen_ids:
                    continue
                seen_ids.add(idm)
                unique_docs.append(r)
            docs = unique_docs

            # ── Enriquecer: saldo agregado por doc + nombre por RUT ──
            if docs:
                idmaeedos = [r.get("IDMAEEDO") for r in docs if r.get("IDMAEEDO") is not None]
                if idmaeedos:
                    placeholders = ",".join(["%s"] * len(idmaeedos))
                    try:
                        saldo_rows = _random_sql_query(f"""
                            SELECT d.IDMAEEDO,
                                   COALESCE(SUM(CASE
                                       WHEN UPPER(LTRIM(RTRIM(d.KOPRCT))) LIKE 'ZZ%%'
                                            AND (d.CAPRCO1 - COALESCE(d.CAPRAD1,0)) > 0
                                       THEN d.CAPRCO1 - COALESCE(d.CAPRAD1,0)
                                       ELSE 0
                                   END), 0) AS saldo_zz,
                                   COALESCE(SUM(CASE
                                       WHEN UPPER(LTRIM(RTRIM(d.KOPRCT))) NOT LIKE 'ZZ%%'
                                            AND UPPER(LTRIM(RTRIM(COALESCE(d.ESLIDO,'')))) NOT IN ('C','T','TOTAL','CERRADO','DESPACHADO')
                                            AND (d.CAPRCO1 - COALESCE(d.CAPRAD1,0) - COALESCE(d.CAPREX1,0) - COALESCE(d.CAPRNC1,0)) > 0
                                       THEN (d.CAPRCO1 - COALESCE(d.CAPRAD1,0) - COALESCE(d.CAPREX1,0) - COALESCE(d.CAPRNC1,0))
                                       ELSE 0
                                   END), 0) AS saldo_real_unidades,
                                   COUNT(*) AS n_lineas
                              FROM MAEDDO d
                             WHERE d.IDMAEEDO IN ({placeholders})
                             GROUP BY d.IDMAEEDO
                        """, tuple(idmaeedos), max_rows=len(idmaeedos)) or []
                        sm = {s.get("IDMAEEDO"): s for s in saldo_rows}
                    except Exception as e:
                        print(f"[tk_buscar_cliente_docs] saldo lookup falló: {e}", flush=True)
                        sm = {}
                    for r in docs:
                        s = sm.get(r.get("IDMAEEDO")) or {}
                        r["saldo_zz"] = s.get("saldo_zz") or 0
                        r["saldo_real_unidades"] = s.get("saldo_real_unidades") or 0
                        r["n_lineas"] = s.get("n_lineas") or 0

                ruts_needed = set()
                for r in docs:
                    endo = (r.get("ENDO") or "").strip()
                    if not endo:
                        continue
                    rut_clean = endo.split("-")[0] if "-" in endo else endo
                    if rut_clean and len(rut_clean) >= 4:
                        ruts_needed.add(rut_clean)
                ruts_needed.discard("")

                nombre_map = {}
                if ruts_needed:
                    rph = ",".join(["%s"] * len(ruts_needed))
                    try:
                        nm_rows = _random_sql_query(f"""
                            SELECT LTRIM(RTRIM(RTEN)) AS rut,
                                   LTRIM(RTRIM(COALESCE(
                                       NULLIF(LTRIM(RTRIM(NOKOENAMP)),''),
                                       NOKOEN, ''
                                   ))) AS razon
                              FROM MAEEN
                             WHERE LTRIM(RTRIM(RTEN)) IN ({rph})
                        """, tuple(ruts_needed), max_rows=len(ruts_needed) * 4) or []
                        for nm in nm_rows:
                            rut = (nm.get("rut") or "").strip()
                            razon = (nm.get("razon") or "").strip()
                            if rut and razon and not nombre_map.get(rut):
                                nombre_map[rut] = razon
                    except Exception as e:
                        print(f"[tk_buscar_cliente_docs] nombre lookup falló: {e}", flush=True)

                for r in docs:
                    endo = (r.get("ENDO") or "").strip()
                    rut_clean = endo.split("-")[0] if "-" in endo else endo
                    nombre = nombre_map.get(rut_clean, "") or (r.get("SUENDO") or "").strip()
                    r["NOMBRE"] = nombre or "Consumidor final"

        except PermissionError as pe:
            return jsonify({"ok": False, "error": f"Bloqueado por seguridad: {pe}",
                             "documentos": []}), 403
        except Exception as exc:
            print(f"[tk_buscar_cliente_docs] error inesperado: {exc}", flush=True)
            return jsonify({"ok": False, "error": f"Error consultando ERP: {str(exc)[:200]}",
                             "documentos": []})

        # ── Calcular tido_display/nudo_display por doc (misma logica que
        #    /retiros/api/buscar-erp: NVV con prefijo VD/WEB en NUDO se
        #    muestra con el tido "real" que el usuario reconoce) ──
        for r in docs:
            nudo_raw = (r.get("NUDO") or "").strip()
            tido_raw = (r.get("TIDO") or "").strip()
            if tido_raw == "NVV" and nudo_raw.startswith("VD"):
                r["_tido_display"] = "VD"
                r["_nudo_display"] = nudo_raw[2:].lstrip("0") or "0"
            elif tido_raw == "NVV" and nudo_raw.startswith("WEB"):
                r["_tido_display"] = "WEB"
                r["_nudo_display"] = nudo_raw[3:].lstrip("0") or "0"
            else:
                r["_tido_display"] = tido_raw
                r["_nudo_display"] = nudo_raw.lstrip("0") or "0"

        # ── Candado: doc ya asociado a ALGUN ticket (tk_ticket_documentos).
        #    erp_tido/erp_nudo se guardan en formato DISPLAY (lo que el
        #    operador tipeo/vio, ej. "FCV"/"12345" -- no el NUDO crudo
        #    zero-padded de MAEEDO), asi que comparamos contra ese mismo
        #    formato para que el match funcione. ──
        ya_asociados = {}
        try:
            pares = sorted({(r["_tido_display"], r["_nudo_display"]) for r in docs
                            if r.get("_tido_display") and r.get("_nudo_display")})
            if pares:
                placeholders = ",".join(["(%s,%s)"] * len(pares))
                params = tuple(x for par in pares for x in par)
                rows_doc = mysql_fetchall(
                    f"SELECT td.ticket_id, td.erp_tido, td.erp_nudo, t.numero_ticket "
                    f"FROM tk_ticket_documentos td "
                    f"JOIN tk_tickets t ON t.id = td.ticket_id "
                    f"WHERE (td.erp_tido, td.erp_nudo) IN ({placeholders})",
                    params
                ) or []
                for r in rows_doc:
                    key = f"{(r.get('erp_tido') or '').upper()}|{(r.get('erp_nudo') or '').strip()}"
                    ya_asociados[key] = {"ticket_id": r.get("ticket_id"),
                                          "numero_ticket": r.get("numero_ticket")}
        except Exception as e:
            print(f"[tk_buscar_cliente_docs] ya_asociados fallback: {e}", flush=True)

        # ── Formatear respuesta ──────────────────────────────────────
        out = []
        for r in docs:
            tido_raw = (r.get("TIDO") or "").strip()
            nudo_raw = (r.get("NUDO") or "").strip()
            tido_display = r["_tido_display"]
            nudo_display = r["_nudo_display"]
            fe = r.get("FEEMDO")
            saldo_zz = float(r.get("saldo_zz") or 0)
            saldo_real_unidades = float(r.get("saldo_real_unidades") or 0)
            endo = (r.get("ENDO") or "").strip()
            rut_clean = endo.split("-")[0] if "-" in endo else endo
            key = f"{tido_display.upper()}|{nudo_display}"
            asociado = ya_asociados.get(key)

            out.append({
                "idmaeedo":     r.get("IDMAEEDO"),
                "tido":         tido_raw,
                "nudo":         nudo_raw,
                "tido_display": tido_display,
                "nudo_display": nudo_display,
                "rut":          rut_clean,
                "razon_social": (r.get("NOMBRE") or "").strip().title(),
                "fecha":        fe.strftime("%d/%m/%Y") if fe else "",
                "fecha_iso":    fe.strftime("%Y-%m-%d") if fe else "",
                "valor_neto":   float(r.get("VANEDO") or 0),
                "valor_total":  float(r.get("VABRDO") or 0),
                "estado_pago":  (r.get("ESPGDO") or "").strip(),
                "saldo_zz":            saldo_zz,
                "saldo_real_unidades": saldo_real_unidades,
                "tiene_saldo":         saldo_real_unidades > 0,
                "n_lineas":            int(r.get("n_lineas") or 0),
                "ya_asociado":            bool(asociado),
                "asociado_ticket_id":     (asociado or {}).get("ticket_id"),
                "asociado_numero_ticket": (asociado or {}).get("numero_ticket"),
                "asociado_es_este_ticket": bool(
                    asociado and ticket_id_ctx and asociado.get("ticket_id") == ticket_id_ctx),
            })

        return jsonify({"ok": True, "modo": modo, "documentos": out, "count": len(out), "query": q})

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

    # 2026-07-12 (Daniel — tickets 590/591): fotos adjuntadas desde el celular
    # (ej. imagen de WhatsApp guardada y subida desde el picker de archivos de
    # iOS/Android) a veces llegan con `f.mimetype` vacío o generico
    # ("application/octet-stream") aunque el archivo SI sea una imagen real —
    # el navegador/SO no siempre setea el Content-Type del multipart. Eso
    # hacia que se guardara un mime_type incorrecto en tk_adjuntos y el chip
    # se mostrara como archivo generico (sin miniatura) y el visor universal
    # cayera al fallback "Vista previa no disponible" para una imagen que en
    # realidad SI se puede mostrar. Fallback por extension cuando el mime del
    # navegador no es reconocible como imagen/video.
    _MIME_POR_EXT = {
        ".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".png": "image/png",
        ".webp": "image/webp", ".gif": "image/gif", ".heic": "image/heic",
        ".mp4": "video/mp4", ".mov": "video/quicktime", ".webm": "video/webm",
        ".avi": "video/x-msvideo",
    }

    def _tk_mime_confiable(mime_navegador, ext):
        """Devuelve un mime_type confiable para guardar/clasificar el adjunto.
        Si el navegador ya dio un image/* o video/* coherente, se respeta tal
        cual. Si vino vacio o generico (application/octet-stream y similares),
        se completa por extension conocida — asi el archivo SI se reconoce
        como imagen/video y el visor lo muestra en vez de caer al fallback."""
        m = (mime_navegador or "").lower().strip()
        if m.startswith("image/") or m.startswith("video/"):
            return m
        return _MIME_POR_EXT.get(ext, m)

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
        # 2026-07-13 (Daniel, URGENTE): "saca la garantia... dejemoslo mas
        # abajo en un toggle en equipos" -- mismo criterio que el modal
        # interno (TK_TIPOS_MODAL): la garantia deja de ser un TIPO de
        # solicitud propio y pasa a ser el toggle transversal es_garantia
        # (ya existe la columna, solo faltaba exponerla en este formulario).
        return render_template(
            "tickets/soporte_publico.html",
            tk_tipos_publicos=TK_TIPOS_MODAL, tipo_label=TIPO_LABEL,
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

        # 2026-07-13 (Daniel, URGENTE): "quitale el obligatorio al producto...
        # es valido avanzar" -- el equipo ya no es requerido (antes bloqueaba
        # el envio si el cliente no sabia el nombre exacto de la maquina).
        productos = d.get("productos") or []  # [{sku, nombre}]

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
                    " comuna_nombre, region_nombre, codigo_postal, sucursal, producto, sku, numero_documento, "
                    " es_garantia, created_by) "
                    "VALUES ('form','open',%s,'media',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'cliente')",
                    (tipo, descripcion[:2000], rut[:12],
                     (d.get("empresa") or "").strip()[:150] or None,
                     nombre_contacto[:150], email[:150], phone[:20],
                     direccion[:255],
                     d.get("direccion_lat") or None, d.get("direccion_lng") or None,
                     (d.get("direccion_place_id") or "").strip()[:200] or None,
                     (d.get("comuna_nombre") or "").strip()[:120] or None,
                     (d.get("region_nombre") or "").strip()[:120] or None,
                     (d.get("codigo_postal") or "").strip()[:20] or None,
                     (d.get("sucursal") or "").strip()[:100] or None,
                     ", ".join(p.get("nombre", "") for p in productos if p.get("nombre"))[:2000] or None,
                     ", ".join(p.get("sku", "") for p in productos if p.get("sku"))[:500] or None,
                     (d.get("numero_documento") or "").strip()[:500] or None,
                     1 if d.get("es_garantia") else 0))
                tid = cur.lastrowid
                cur.execute(
                    "UPDATE tk_tickets SET numero_ticket = "
                    "CONCAT('TK-', %s, '-', LPAD(id,5,'0')) WHERE id=%s",
                    (_chile_now_year(), tid))
                for p in productos[:20]:
                    try:
                        # 2026-07-13 (Daniel, URGENTE): "si es multimaquina...
                        # se diferencia un problema por maquina" -- si el
                        # cliente activo el toggle "problema distinto por
                        # maquina", cada producto trae su propio motivo (el
                        # frontend lo manda en p.motivo); se guarda en `notas`
                        # (columna ya existente en tk_ticket_equipos) para
                        # que quede registrado por equipo, no solo mezclado
                        # en la descripcion general del ticket.
                        motivo_eq = (p.get("motivo") or "").strip()[:500] or None
                        cur.execute(
                            "INSERT IGNORE INTO tk_ticket_equipos (ticket_id, erp_kopr, nombre, cantidad, notas) "
                            "VALUES (%s,%s,%s,1,%s)",
                            (tid, (p.get("sku") or "").strip()[:100] or None,
                             (p.get("nombre") or "").strip()[:300] or "Equipo", motivo_eq))
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
        # 2026-07-13 (Daniel, URGENTE — gerente general probando el formulario
        # para Shopify): "optimiza la velocidad... necesito que suba de manera
        # inmediata". _tk_notificar_lifecycle manda un correo real por SMTP de
        # forma SINCRONA -- eso podia tardar varios segundos (o mas si el SMTP
        # esta lento) y el navegador quedaba esperando esa respuesta antes de
        # mostrar la confirmacion. El ticket YA esta creado en este punto; el
        # correo de aviso se manda en un hilo de fondo para no bloquear la
        # respuesta HTTP. Necesita su propio app_context (get_db() usa `g`,
        # que no existe fuera del request/hilo original).
        import threading as _threading
        def _notificar_creacion_bg(_tid=tid):
            with app.app_context():
                try:
                    _tk_notificar_lifecycle(_tid, "creacion")
                except Exception as _e:
                    print(f"[tk_soporte_api_crear] notificacion creacion no enviada tid={_tid}: {_e}", flush=True)
        _threading.Thread(target=_notificar_creacion_bg, daemon=True).start()

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

        mime = _tk_mime_confiable(f.mimetype, ext)
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
        """Pie de respuesta que se agrega a TODO correo saliente de tickets
        (creacion/resuelto/cerrado/respuesta manual) -- estilos inline (los
        clientes de correo no cargan CSS externo).

        GIRO 2026-07-12 (Daniel, orden explicita): "necesito que reciba las
        respuestas directas, olvida esa metodologia [portal]... responde el
        correo, tu ubicas el asunto y listo". El correo ahora INVITA a
        responder directo (como Triple A): la respuesta llega al buzon de
        soporte (Reply-To = _tk_reply_to()) y el lector IMAP la ubica por el
        numero de ticket del asunto y la ingresa al ticket como mensaje del
        cliente. El portal ya NO se ofrece en el correo; sus rutas siguen
        vivas (Regla #4.2) pero sin linkear."""
        return (
            '<div style="background:#f0fdf4;border:1px solid #bbf7d0;border-radius:8px;'
            'padding:12px 16px;margin:20px 0 8px;text-align:center">'
            '<p style="font-size:13px;color:#166534;margin:0;font-weight:600">'
            '💬 ¿Necesitas agregar algo? Simplemente <strong>responde a este correo</strong> '
            '(sin cambiar el asunto) y tu mensaje quedará registrado en tu ticket.</p>'
            '</div>'
        )

    def _tk_test_redirect(to_email, subject):
        """MODO PRUEBA especifico de Tickets (Daniel 2026-07-12): mientras se
        prueba el ping-pong de correo con los tickets reales migrados de
        Triple A, ningun correo debe llegarle a un cliente real. En vez de
        borrar datos (que no vamos a hacer -- puede haber tickets organicos
        sin respaldo en Triple A), si la env var TK_TEST_EMAIL_TO esta seteada,
        CUALQUIER correo saliente de tickets se redirige a esa casilla,
        mostrando el destinatario real en el asunto. Deliberadamente separado
        de ILUS_COMM_TEST_TO (ese es global y afectaria retiros/transporte/
        mantenciones tambien mientras se prueba tickets) -- sin la env var,
        passthrough exacto, cero impacto."""
        test_to = (os.environ.get("TK_TEST_EMAIL_TO") or "").strip()
        if test_to and to_email and str(to_email).strip().lower() != test_to.lower():
            real_to = to_email
            print(f"[TK_TEST_MODE] correo de ticket redirigido: real={real_to} -> {test_to}", flush=True)
            return test_to, f"[PRUEBA→{real_to}] {subject}"
        return to_email, subject

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
            "SELECT id, tipo, contenido, usuario, message_date, created_at FROM tk_mensajes "
            "WHERE ticket_id=%s AND es_interno=0 AND tipo IN ('mensaje','client_message','comentario') "
            "ORDER BY COALESCE(message_date, created_at) ASC, id ASC", (tid,))
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

    # ═══════════════════════════════════════════════════════════════════
    #  LECTOR DE CORREO ENTRANTE (Daniel 2026-07-12, orden explicita):
    #  "necesito que reciba las respuestas directas... yo mando una
    #  respuesta, llega el correo al cliente, el responde... tu ubicas el
    #  asunto y listo". Metodologia Triple A (gmail-thread-monitor):
    #  el cliente responde el correo en su Gmail -> la respuesta llega al
    #  buzon de soporte (Reply-To de tickets) -> este lector la ubica por
    #  el numero TK-AAAA-NNNNN del asunto y la ingresa al ticket como
    #  mensaje del cliente (con sus adjuntos).
    #
    #  Acceso al buzon: IMAP (imap.gmail.com) con la MISMA app password que
    #  ya usa el SMTP saliente (SMTP_USER/SMTP_PASS) -- factibilidad
    #  verificada en vivo el 2026-07-12 (LOGIN_OK). Sin Gmail API, sin
    #  delegacion de dominio, sin credenciales nuevas.
    #
    #  Seguridad/casa ajena: el buzon es la casilla de TRABAJO de Daniel.
    #  Por eso: (1) SIEMPRE BODY.PEEK y select readonly -- JAMAS se marca
    #  nada como leido ni se modifica el buzon; (2) solo se consideran
    #  correos con "TK-" en el asunto dentro de una ventana de dias;
    #  (3) dedup por Message-ID en tk_mail_ingeridos.
    # ═══════════════════════════════════════════════════════════════════
    _TK_NUM_TICKET_RE = re.compile(r"TK-\d{4}-\d{5}", re.I)
    # Marcadores tipicos donde empieza la cola citada de una respuesta
    # (Gmail/Outlook es/en). Todo lo que siga se descarta del mensaje.
    _TK_QUOTE_RE = re.compile(
        r"^\s*(El\s.{0,120}escribi[oó]:?\s*$|On\s.{0,120}wrote:?\s*$|"
        r"-{2,}\s*(Mensaje original|Original Message|Forwarded message)|"
        r"_{5,}\s*$|De:\s.+|From:\s.+|"
        r"(Enviado|Sent)\s+(desde|from)\s+(mi|my)\s*(iPhone|iPad|Android|celular|tel[eé]fono).*$|"
        r"Get\s+Outlook\s+for\s+(iOS|Android).*$)", re.I)

    def _tk_imap_creds():
        user = (os.environ.get("SMTP_USER") or "").strip()
        pwd = (os.environ.get("SMTP_PASS") or os.environ.get("SMTP_PASSWORD") or "").strip()
        return user, pwd

    def _tk_extraer_cuerpo_mail(msg):
        """Texto del mensaje del cliente, SIN la cola citada del hilo.
        Prefiere text/plain; si solo hay HTML, quita etiquetas (el hilo
        interno renderiza client_message escapado como texto plano)."""
        plano, html = None, None
        for part in msg.walk():
            if part.get_content_maintype() == "multipart" or part.get_filename():
                continue
            ctype = part.get_content_type()
            try:
                payload = part.get_payload(decode=True)
                if payload is None:
                    continue
                charset = part.get_content_charset() or "utf-8"
                texto = payload.decode(charset, "replace")
            except Exception:
                continue
            if ctype == "text/plain" and plano is None:
                plano = texto
            elif ctype == "text/html" and html is None:
                html = texto
        if plano is None and html is not None:
            # HTML -> texto: fuera tags, entidades decodificadas
            sin_tags = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", html,
                              flags=re.S | re.I)
            sin_tags = re.sub(r"<br\s*/?>|</p>|</div>", "\n", sin_tags, flags=re.I)
            sin_tags = re.sub(r"<[^>]+>", " ", sin_tags)
            try:
                import html as _h
                plano = _h.unescape(sin_tags)
            except Exception:
                plano = sin_tags
        if not plano:
            return ""
        lineas = []
        for ln in plano.replace("\r\n", "\n").split("\n"):
            if _TK_QUOTE_RE.match(ln):
                break  # empezo la cola citada -> el resto no es del cliente
            if ln.lstrip().startswith(">"):
                continue  # linea citada suelta
            lineas.append(ln.rstrip())
        cuerpo = "\n".join(lineas).strip()
        # colapsar saltos multiples
        return re.sub(r"\n{3,}", "\n\n", cuerpo)

    def _tk_leer_correo_entrante(dias=7, max_correos=50):
        """Barrido del buzon de soporte: ubica respuestas por numero de
        ticket en el asunto y las ingresa como mensajes del cliente.
        Devuelve resumen dict. Nunca lanza (errores -> resumen)."""
        user, pwd = _tk_imap_creds()
        resumen = {"ok": True, "candidatos": 0, "ingresados": 0, "duplicados": 0,
                   "sin_ticket": 0, "propios": 0, "adjuntos": 0, "errores": 0}
        if not (user and pwd):
            return {"ok": False, "error": "Sin credenciales SMTP_USER/SMTP_PASS para IMAP"}
        # Fecha IMAP en ingles SIEMPRE (strftime %b depende del locale)
        _MES = ("Jan", "Feb", "Mar", "Apr", "May", "Jun",
                "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")
        d = datetime.now(timezone.utc) - timedelta(days=max(1, int(dias)))
        desde_imap = f"{d.day:02d}-{_MES[d.month - 1]}-{d.year}"
        try:
            M = imaplib.IMAP4_SSL("imap.gmail.com", 993)
            M.login(user, pwd)
            M.select("INBOX", readonly=True)  # readonly: JAMAS tocar el buzon
        except Exception as _e:
            print(f"[tk_mail] no se pudo conectar a IMAP: {_e}", flush=True)
            return {"ok": False, "error": f"IMAP no disponible: {_e}"}
        try:
            # Gmail tokeniza la busqueda (encuentra "TK-2026-..." aunque el
            # SUBJECT sea aproximado); el regex de abajo es el filtro REAL.
            typ, data = M.search(None, f'(SINCE {desde_imap} SUBJECT "TK-")')
            ids = (data[0].split() if data and data[0] else [])[-max_correos:]
            for mid in ids:
                try:
                    typ, msgdata = M.fetch(mid, "(BODY.PEEK[])")
                    raw = msgdata[0][1] if msgdata and msgdata[0] else None
                    if not raw:
                        continue
                    msg = _email_mod.message_from_bytes(raw)
                    subject = str(make_header(decode_header(msg.get("Subject", "") or "")))
                    m = _TK_NUM_TICKET_RE.search(subject)
                    if not m:
                        continue  # "TK FRESHDESK" y similares: no son nuestros
                    resumen["candidatos"] += 1
                    numero = m.group(0).upper()
                    from_nombre, from_email = parseaddr(
                        str(make_header(decode_header(msg.get("From", "") or ""))))
                    from_email = (from_email or "").strip().lower()
                    # No ingresar nuestros propios envios ni rebotes
                    if (not from_email or from_email == user.lower()
                            or "mailer-daemon" in from_email
                            or "noreply" in from_email or "no-reply" in from_email):
                        resumen["propios"] += 1
                        continue
                    ticket = mysql_fetchone(
                        "SELECT id, numero_ticket, nombre_contacto, empresa "
                        "FROM tk_tickets WHERE numero_ticket=%s", (numero,))
                    if not ticket:
                        resumen["sin_ticket"] += 1
                        continue
                    message_id = (msg.get("Message-ID") or "").strip()[:255]
                    if not message_id:
                        # fallback estable para correos sin Message-ID
                        import hashlib
                        message_id = "sin-id-" + hashlib.sha1(raw).hexdigest()[:40]
                    if mysql_fetchone(
                            "SELECT message_id FROM tk_mail_ingeridos WHERE message_id=%s",
                            (message_id,)):
                        resumen["duplicados"] += 1
                        continue
                    cuerpo = _tk_extraer_cuerpo_mail(msg) or "(Mensaje sin texto)"
                    remitente = (from_nombre or ticket.get("nombre_contacto")
                                 or from_email or "Cliente")[:190]
                    # Fecha REAL del correo (header Date), no la hora de
                    # ingesta/barrido -- si el cliente responde y el staff
                    # manda otro mensaje ANTES del siguiente barrido, sin
                    # esto el mensaje del cliente queda despues en el hilo
                    # (Daniel 2026-07-12: orden tipo WhatsApp).
                    msg_date = None
                    try:
                        _dt = parsedate_to_datetime(msg.get("Date", ""))
                        if _dt is not None:
                            msg_date = (_dt.astimezone(timezone.utc).replace(tzinfo=None)
                                        if _dt.tzinfo else _dt)
                    except Exception:
                        msg_date = None
                    msg_id_db = _tk_log(
                        ticket["id"], "client_message", cuerpo[:20000],
                        usuario=remitente, es_interno=False, message_date=msg_date,
                        metadata={"via": "email", "message_id": message_id,
                                  "from": from_email, "subject": subject[:300]})
                    # Adjuntos del correo -> GCS -> tk_adjuntos (mismas
                    # validaciones de extension/tamano que el resto)
                    adj_ids = []
                    if _uploader_upload:
                        for part in msg.walk():
                            fn = part.get_filename()
                            if not fn or part.get_content_maintype() == "multipart":
                                continue
                            try:
                                fn_dec = str(make_header(decode_header(fn)))[:300]
                                ext = ("." + fn_dec.rsplit(".", 1)[-1].lower()) if "." in fn_dec else ""
                                if ext not in _EXT_PERMITIDAS:
                                    continue
                                contenido_adj = part.get_payload(decode=True) or b""
                                if not contenido_adj or len(contenido_adj) > MAX_ADJUNTO_MB * 1024 * 1024:
                                    continue
                                from werkzeug.datastructures import FileStorage
                                ctype_adj = part.get_content_type() or "application/octet-stream"
                                fs = FileStorage(stream=io.BytesIO(contenido_adj),
                                                 filename=fn_dec, content_type=ctype_adj)
                                rt = ("image" if ctype_adj.startswith("image")
                                      else "video" if ctype_adj.startswith("video") else "raw")
                                res = _uploader_upload(fs, folder="tickets", resource_type=rt)
                                url = res.get("secure_url") or res.get("url")
                                if not url:
                                    continue
                                mysql_execute(
                                    "INSERT INTO tk_adjuntos "
                                    "(ticket_id, mensaje_id, archivo_url, archivo_path, archivo_nombre, "
                                    " mime_type, file_size_kb, origen, subido_por) "
                                    "VALUES (%s,%s,%s,%s,%s,%s,%s,'cliente',%s)",
                                    (ticket["id"], msg_id_db, url[:500],
                                     (res.get("public_id") or "")[:500] or None,
                                     fn_dec, ctype_adj[:150],
                                     max(1, len(contenido_adj) // 1024), from_email[:190]))
                                resumen["adjuntos"] += 1
                            except Exception as _ea:
                                print(f"[tk_mail] adjunto no ingresado ({fn}): {_ea}", flush=True)
                    mysql_execute(
                        "INSERT IGNORE INTO tk_mail_ingeridos "
                        "(message_id, ticket_id, from_email, subject) VALUES (%s,%s,%s,%s)",
                        (message_id, ticket["id"], from_email[:190], subject[:300]))
                    mysql_execute("UPDATE tk_tickets SET updated_at=NOW() WHERE id=%s",
                                  (ticket["id"],))
                    resumen["ingresados"] += 1
                    print(f"[tk_mail] respuesta de {from_email} ingresada en "
                          f"{numero} (msg {msg_id_db})", flush=True)
                    # Aviso app-wide (campana de notificaciones, ya visible en
                    # TODA la app, no solo la bandeja de tickets) -- Daniel
                    # 2026-07-12: "estuvo como WhatsApp... metamos la
                    # tecnologia" -- si solo avisamos en la bandeja/ficha, un
                    # mensaje nuevo pasa desapercibido si el staff esta en
                    # otro modulo, y se "lee" solo con abrir la ficha (antes
                    # de que alguien note el aviso). broadcast (destino=None)
                    # = lo ve cualquier admin/superadmin; se resuelve en
                    # tiempo de request (mismo patron que _tickets_tpl_seed).
                    try:
                        _mant_notificar = ctx.get("_mant_notificar")
                        if _mant_notificar:
                            # tipo='otro': el ENUM de mant_notificaciones (app.py
                            # ~linea 35634) no incluye un valor propio de tickets
                            # y agregar uno requeriria una migracion ALTER TABLE:
                            # 'otro' ya es el catch-all valido: el titulo/url_accion
                            # (unicos por ticket) siguen distinguiendo cada aviso.
                            extracto = cuerpo[:180] + ("…" if len(cuerpo) > 180 else "")
                            _mant_notificar(
                                None, "otro",
                                f"Nuevo mensaje en {numero}", cuerpo=extracto,
                                url_accion=f"/tickets/{ticket['id']}", prioridad="alta")
                    except Exception as _en:
                        print(f"[tk_mail] no se pudo crear notif interna: {_en}", flush=True)
                except Exception as _em:
                    resumen["errores"] += 1
                    print(f"[tk_mail] error procesando correo: {_em}", flush=True)
        finally:
            try:
                M.logout()
            except Exception:
                pass
        return resumen

    # Endpoint para Cloud Scheduler (token) o disparo manual (admin logueado)
    @app.route("/tickets/api/cron/leer-correo", methods=["GET", "POST"])
    def tk_cron_leer_correo():
        token = (request.args.get("token") or request.headers.get("X-Cron-Token") or "").strip()
        token_cfg = (os.environ.get("TK_MAIL_CRON_TOKEN") or "").strip()
        token_ok = bool(token_cfg) and token == token_cfg
        sesion_ok = False
        try:
            perms = getattr(g, "permissions", None) or {}
            sesion_ok = bool(perms.get("superadmin") or perms.get("admin")
                             or perms.get("mantenciones"))
        except Exception:
            pass
        if not (token_ok or sesion_ok):
            return jsonify({"ok": False, "error": "No autorizado"}), 403
        return jsonify({"ok": True, "resumen": _tk_leer_correo_entrante()})

    # Auto-poll oportunista: cada vez que alguien mira la bandeja O la ficha
    # de un ticket y el ultimo barrido tiene mas de _TK_AUTOPOLL_SEG, se
    # dispara uno en segundo plano (hilo con app_context -- leccion del bug
    # OT-31). Daniel 2026-07-12: "necesito que sea inmediata la velocidad,
    # menos de diez segundos" -- bajado de 300s a 45s y ahora a 8s (el lock
    # global asegura maximo 1 login IMAP real cada 8s sin importar cuanta
    # gente tenga la app abierta, asi que el buzon de Gmail no se satura).
    # Para verdadera inmediatez 24/7 (sin depender de que alguien tenga la
    # app abierta) hace falta Cloud Scheduler golpeando
    # /tickets/api/cron/leer-correo -- pendiente, requiere agregar
    # TK_MAIL_CRON_TOKEN a la config persistente (GCP_ENV_VARS), no algo que
    # se deba hacer por fuera del pipeline de deploy.
    _TK_AUTOPOLL_SEG = 8
    _TK_MAIL_POLL = {"ts": 0.0, "lock": threading.Lock()}

    def _tk_autopoll_correo():
        user, pwd = _tk_imap_creds()
        if not (user and pwd):
            return
        ahora = time.monotonic()
        if ahora - _TK_MAIL_POLL["ts"] < _TK_AUTOPOLL_SEG:
            return
        if not _TK_MAIL_POLL["lock"].acquire(blocking=False):
            return  # ya hay un barrido corriendo
        _TK_MAIL_POLL["ts"] = ahora

        def _correr():
            try:
                with app.app_context():
                    r = _tk_leer_correo_entrante()
                    if r.get("ingresados"):
                        print(f"[tk_mail_autopoll] {r}", flush=True)
            except Exception as _e:
                print(f"[tk_mail_autopoll] error: {_e}", flush=True)
            finally:
                _TK_MAIL_POLL["lock"].release()

        threading.Thread(target=_correr, daemon=True).start()

    ctx["_tk_autopoll_correo"] = _tk_autopoll_correo  # visible para diagnostico

    print("[ILUS] Modulo Tickets central registrado (/tickets).", flush=True)

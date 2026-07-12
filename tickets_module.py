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
                        "cantidad": qty_final})
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

        return jsonify({
            "ok": True,
            "tickets": [_fmt_row(r) for r in rows],
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
                            "(ticket_id, erp_kopr, nombre, tipo, sku, cantidad, notas) VALUES (%s,%s,%s,%s,%s,%s,%s)",
                            (tid, (eq.get("kopr") or "").strip()[:100] or None,
                             (eq.get("nombre") or "").strip()[:300] or None,
                             (eq.get("tipo") or "").strip()[:100] or None,
                             (eq.get("sku") or "").strip()[:100] or None,
                             int(eq.get("cantidad") or 1),
                             (eq.get("notas") or "").strip()[:500] or None),
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

        return jsonify({
            "ok": True,
            "ticket": _fmt_row(t),
            "equipos": [dict(r) for r in equipos],
            "documentos": [_fmt_row(r) for r in documentos],
            "mensajes": [_fmt_row(r) for r in mensajes],
            "adjuntos": [_fmt_row(r) for r in adjuntos],
            "vistas": [_fmt_row(r) for r in vistas],
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
    _TK_LIFECYCLE_DEFAULTS = {
        "creacion": (
            "{numero} — Recibimos tu solicitud",
            "<p style=\"font-size:14px;color:#6b7280;margin:0 0 14px\">Hola {cliente},</p>"
            "<div style=\"border-left:4px solid #dc2626;background:#fafafa;border-radius:0 10px 10px 0;"
            "padding:18px 20px;margin:0 0 6px\">"
            "<div style=\"font-size:16px;color:#111827;line-height:1.6\">Ya registramos tu solicitud "
            "con el número <strong>{numero}</strong>. Nuestro equipo la revisará y te contactará "
            "a la brevedad.</div></div>"),
        "resuelto": (
            "{numero} — Resuelto",
            "<p style=\"font-size:14px;color:#6b7280;margin:0 0 14px\">Hola {cliente},</p>"
            "<div style=\"border-left:4px solid #16a34a;background:#f0fdf4;border-radius:0 10px 10px 0;"
            "padding:18px 20px;margin:0 0 6px\">"
            "<div style=\"font-size:16px;color:#111827;line-height:1.6\">✅ Tu solicitud "
            "<strong>{numero}</strong> ya fue resuelta por nuestro equipo.</div></div>"),
        "cerrado": (
            "{numero} — Cerrado",
            "<p style=\"font-size:14px;color:#6b7280;margin:0 0 14px\">Hola {cliente},</p>"
            "<div style=\"border-left:4px solid #6b7280;background:#f9fafb;border-radius:0 10px 10px 0;"
            "padding:18px 20px;margin:0 0 6px\">"
            "<div style=\"font-size:16px;color:#111827;line-height:1.6\">Tu ticket "
            "<strong>{numero}</strong> ha sido cerrado. Gracias por confiar en "
            "ILUS Sport &amp; Health.</div></div>"),
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
        html_final = (_comm_render_email_document(subject_envio, cuerpo_correo, subtitle=f"Ticket {numero} · ILUS Fitness")
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
        html_final = (_comm_render_email_document(subject_envio, cuerpo_email, subtitle=f"Ticket {numero} · ILUS Fitness")
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
    TK_TPL_ESTADOS_VALIDOS = ("creacion", "respuesta", "resuelto", "cerrado")

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
                mysql_execute(
                    "INSERT IGNORE INTO tk_ticket_equipos (ticket_id, erp_kopr, nombre, sku, cantidad) "
                    "VALUES (%s,%s,%s,%s,%s)",
                    (tid, ln["sku"][:100] or None, ln["nombre"][:300] or "Equipo",
                     ln["sku"][:100] or None, cant))
                agregados += 1
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
                todas_lineas.append(ln)

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
                        try:
                            cant = max(1, int(round(float(ln.get("cantidad") or 1))))
                        except Exception:
                            cant = 1
                        cur.execute(
                            "INSERT IGNORE INTO tk_ticket_equipos (ticket_id, erp_kopr, nombre, cantidad) "
                            "VALUES (%s,%s,%s,%s)",
                            (tid, ln["sku"][:100] or None, ln["nombre"][:300] or "Equipo", cant))
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

# -*- coding: utf-8 -*-
"""CORREOS AUTOMÁTICOS A TÉCNICOS cuando se agenda, cambia o cancela una OT (Daniel, 19-oct-2026).

Daniel: «ya necesito que activemos por el front y por la parte de comunicaciones los mensajes, los mails hacia los
técnicos. Cuando agendemos una orden de trabajo, tenemos que avisarle a los técnicos… por un mail con plantillas,
todo bien estructuradito». Decisiones (19-oct): al técnico le llega correo
  1) al ASIGNARLE una OT (nace con él o lo reasignan),
  2) al CAMBIAR la fecha u horario de una OT suya o al CANCELARLA/anularla,
  3) RECORDATORIO el día hábil anterior a las 17:00 (hora Chile) con su agenda del siguiente día hábil,
  4) al QUITARLE una OT (se la dieron a otro).
El correo al CLIENTE sigue con su propia llave y no cambia.

Este módulo es PURO (sin base de datos, sin Flask, sin correo): decide QUÉ avisar y a quién, arma las variables y el HTML,
y calcula las fechas del recordatorio. Las lecturas/escrituras y el envío viven en app.py (`_ot_tec_*`). Pruebas:
tests/test_ot_correos_tecnicos.py.

CONFIDENCIALIDAD (REGLAS #19 y #26): un técnico NUNCA recibe montos, cobros, costos, márgenes, proveedores, OC ni
autorizaciones. Aquí no existe ninguna variable con plata; el texto libre que escribe una persona (descripción, motivo)
pasa por `limpiar_texto_tecnico`, que descarta las líneas que mencionen plata.
"""
import datetime as _dt
import hashlib
import html as _html
import re

# ── Identificadores ──────────────────────────────────────────────────────────────────────────────────────────────
MODULO_LLAVE = "ot_tecnicos"            # llave de paso propia (comm_killswitch) — el cliente sigue con 'mantenciones'
MODULO_PLANTILLAS = "mantenciones"      # las plantillas viven en el editor de «Servicio Técnico» (comm_templates)

EVENTOS = ("asignada", "reagendada", "cancelada", "desasignada")
PLANTILLA_DE = {
    "asignada": "ot_asignada",                  # ya existía (2026-09-09)
    "reagendada": "ot_reagendada_tecnico",
    "cancelada": "ot_cancelada_tecnico",
    "desasignada": "ot_desasignada_tecnico",
}
EVENTO_LOG = dict(PLANTILLA_DE)                 # nombre del evento en email_log / Historial de Comunicaciones
PLANTILLA_AGENDA = "agenda_tecnico_manana"
EVENTO_LOG_AGENDA = "agenda_tecnico_manana"

ESTADOS_CANCELADOS = frozenset({"cancelada", "anulada"})
# El trabajo ya se hizo (o está firmado esperando cierre): no tiene sentido «asignar» ni «reagendar».
ESTADOS_TERMINADOS = frozenset({"cerrada", "completada", "firmada_tecnico", "pendiente_aprobacion"})
ESTADOS_SIN_AGENDA = ESTADOS_CANCELADOS | ESTADOS_TERMINADOS

HORA_AGENDA = 17                # el recordatorio sale desde las 17:00 (hora Chile) del día hábil anterior
DIAS_ES = ("lunes", "martes", "miércoles", "jueves", "viernes", "sábado", "domingo")

# Variables que documenta el editor de Comunicaciones (static/comunicaciones_index.js → TPL_VARS.ot_tecnico).
VARIABLES_DOC = (
    ("ot", "N° de la orden de trabajo"),
    ("cliente", "Cliente (o «Trabajo interno»)"),
    ("nombre_tecnico", "Nombre del técnico que recibe el correo"),
    ("tecnico", "Igual que nombre_tecnico"),
    ("tecnicos", "Todos los técnicos de la OT"),
    ("tipo_mantencion", "Tipo de orden de trabajo"),
    ("fecha", "Fecha (o rango de fechas) de la OT"),
    ("horario", "Horario"),
    ("direccion", "Dirección del trabajo"),
    ("maquina", "Equipos de la OT"),
    ("link_ot", "Link a la ficha de la OT"),
    ("notas_html", "Fila «Qué hay que hacer» (vacía si la OT no la tiene)"),
    ("motivo_html", "Fila «Motivo» (vacía si no se indicó)"),
    ("fecha_antes", "Fecha anterior (solo reagendada)"),
    ("horario_antes", "Horario anterior (solo reagendada)"),
    ("fecha_ahora", "Fecha nueva (solo reagendada)"),
    ("horario_ahora", "Horario nuevo (solo reagendada)"),
    ("antes_ahora_html", "Bloque ANTES → AHORA (solo reagendada)"),
    ("dia", "Día de la agenda, ej. lunes 19/10/2026 (solo agenda)"),
    ("cuando", "«de mañana» o «del lunes» (solo agenda)"),
    ("n_ot", "Cantidad de OT de la agenda (solo agenda)"),
    ("n_ot_txt", "«1 orden de trabajo» / «3 órdenes de trabajo» (solo agenda)"),
    ("agenda_html", "Una fila por OT, ordenada por hora (solo agenda)"),
    ("link_agenda", "Link a la lista de OT (solo agenda)"),
)


# ── Formatos (REGLA #6: día/mes/año, hora Chile; nunca ISO crudo) ──────────────────────────────────────────────
def a_fecha(v):
    """date | datetime | 'YYYY-MM-DD…' → date (o None)."""
    if v is None or v == "":
        return None
    if isinstance(v, _dt.datetime):
        return v.date()
    if isinstance(v, _dt.date):
        return v
    try:
        return _dt.date.fromisoformat(str(v).strip()[:10])
    except ValueError:
        return None


def hhmm(v):
    """TIME de pymysql (timedelta), time, datetime o texto → 'HH:MM'. ('' si no hay)."""
    if v is None or v == "":
        return ""
    if isinstance(v, _dt.timedelta):
        seg = int(v.total_seconds()) % 86400
        return f"{seg // 3600:02d}:{(seg % 3600) // 60:02d}"
    if isinstance(v, (_dt.datetime, _dt.time)):
        return v.strftime("%H:%M")
    m = re.match(r"^\s*(\d{1,2}):(\d{2})", str(v))
    return f"{int(m.group(1)):02d}:{m.group(2)}" if m else ""


def fecha_txt(v):
    d = a_fecha(v)
    return d.strftime("%d/%m/%Y") if d else ""


def dia_txt(v):
    """lunes 19/10/2026"""
    d = a_fecha(v)
    return f"{DIAS_ES[d.weekday()]} {d.strftime('%d/%m/%Y')}" if d else ""


def rango_txt(f_ini, f_fin=None):
    a, b = a_fecha(f_ini), a_fecha(f_fin)
    if not a:
        return "por confirmar"
    if b and b > a:
        return f"{a.strftime('%d/%m/%Y')} al {b.strftime('%d/%m/%Y')}"
    return a.strftime("%d/%m/%Y")


def horario_txt(h_ini, h_fin=None):
    a, b = hhmm(h_ini), hhmm(h_fin)
    if not a:
        return "por confirmar"
    return f"{a} – {b}" if b else a


def esc(v):
    return _html.escape("" if v is None else str(v), quote=True)


# ── Confidencialidad ─────────────────────────────────────────────────────────────────────────────────────────────
_RE_PLATA = re.compile(
    r"(\$|\bclp\b|\bpesos?\b|\bcost[oa]s?\b|\bcobr\w*|\bprecio\w*|\bmonto\w*|\bmargen\w*|\bfactur\w*|\bproveedor\w*"
    r"|\bboleta\w*|\boc\b|\bo\.c\.|orden de compra|\bneto\b|\biva\b|\bpresupuesto\w*|\bcotiz\w*"
    r"|\bpag(?:o|os|ar|ado|ada|ó)\b|\bautorizaci(?:ón|on|ones)\b|\bvalorizad\w*)",
    re.I,
)


def hay_plata(texto):
    """True si el texto menciona plata, proveedores u OC (lo que un técnico no puede recibir)."""
    return bool(_RE_PLATA.search(str(texto or "")))


def limpiar_texto_tecnico(texto, max_chars=280):
    """Texto libre de una persona (descripción de la OT, motivo del cambio) apto para un técnico: descarta las líneas
    que mencionan plata/proveedor/OC, junta espacios y acorta. Devuelve texto plano (se escapa después)."""
    if not texto:
        return ""
    lineas = []
    # Por línea y por frase: una sola mención de plata no se lleva puesta toda la instrucción.
    for ln in re.split(r"[\r\n]+|(?<=[.;!?])\s+", str(texto)):
        ln = re.sub(r"\s+", " ", ln).strip(" \t•-–—·")
        if not ln or hay_plata(ln):
            continue
        lineas.append(ln)
    out = " · ".join(lineas)
    if len(out) > max_chars:
        out = out[: max_chars - 1].rstrip() + "…"
    return out


# ── Foto de una OT (lo que el técnico "sabe") y decisión de a quién se avisa ────────────────────────────────────
def foto(estado, fecha, fecha_fin, hora_inicio, hora_fin, tecnicos):
    """Normaliza el estado de una OT para compararlo antes/después.
    tecnicos: {correo: nombre}. fecha_fin igual a fecha cuenta como «sin fecha_fin» (no es un cambio)."""
    d_ini, d_fin = a_fecha(fecha), a_fecha(fecha_fin)
    f = d_ini.isoformat() if d_ini else ""
    ff = d_fin.isoformat() if d_fin else ""
    if ff == f:
        ff = ""
    return {
        "estado": str(estado or "").strip().lower(),
        "fecha": f,
        "fecha_fin": ff,
        "hora_inicio": hhmm(hora_inicio),
        "hora_fin": hhmm(hora_fin),
        "tecnicos": {str(e).strip().lower(): n for e, n in (tecnicos or {}).items() if e and "@" in str(e)},
    }


def decidir_avisos(antes, despues):
    """Qué correos salen por un cambio. antes=None → la OT acaba de nacer. Devuelve [{email, evento, nombre}].

    · Cancelada/anulada: a todos los que la tenían (antes) y a los que la tienen (después). Solo ese aviso.
    · OT terminada (cerrada, completada, firmada…): nada.
    · Nacer o reabrirse tras cancelada: todos los técnicos actuales reciben «asignada».
    · Técnico que entra: «asignada» (con la fecha nueva). Técnico que sale: «desasignada».
    · Técnico que se queda y cambia fecha u horario: «reagendada». Si en la misma edición cambian el técnico y la fecha,
      el nuevo recibe «asignada» y el anterior «desasignada» — nunca un «reagendada» repetido."""
    if not despues:
        return []
    est = despues.get("estado") or ""
    tec_d = dict(despues.get("tecnicos") or {})
    tec_a = dict((antes or {}).get("tecnicos") or {})
    out = []

    def _add(email, ev):
        out.append({"email": email, "evento": ev, "nombre": tec_d.get(email) or tec_a.get(email) or ""})

    if est in ESTADOS_CANCELADOS:
        if antes is None or (antes.get("estado") or "") in ESTADOS_CANCELADOS:
            return []                       # ya estaba cancelada (o nació así): nadie tiene nada que saber
        if (antes.get("estado") or "") in ESTADOS_TERMINADOS:
            return []                       # 2026-10-19: el trabajo ya se hizo (cerrada/completada…): «no necesitas ir» sería ruido
        for e in sorted(set(tec_a) | set(tec_d)):
            _add(e, "cancelada")
        return out
    if est in ESTADOS_TERMINADOS:
        return []
    reabierta = antes is not None and (antes.get("estado") or "") in ESTADOS_CANCELADOS
    if antes is None or reabierta:
        for e in sorted(tec_d):
            _add(e, "asignada")
        return out
    for e in sorted(set(tec_d) - set(tec_a)):
        _add(e, "asignada")
    if (antes.get("estado") or "") not in ESTADOS_TERMINADOS:
        for e in sorted(set(tec_a) - set(tec_d)):
            _add(e, "desasignada")
    cambio = any(antes.get(k) != despues.get(k) for k in ("fecha", "fecha_fin", "hora_inicio", "hora_fin"))
    if cambio:
        for e in sorted(set(tec_d) & set(tec_a)):
            _add(e, "reagendada")
    return out


def huella(evento, email, antes, despues):
    """Identifica «este aviso por este cambio» (idempotencia: el mismo aviso no sale dos veces)."""
    def _k(f):
        f = f or {}
        return "|".join(str(f.get(k, "")) for k in ("estado", "fecha", "fecha_fin", "hora_inicio", "hora_fin"))
    base = f"{evento}|{str(email).lower()}|{_k(antes) if evento == 'reagendada' else ''}|{_k(despues)}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()


def ventana(ts, segundos=60):
    """Cubeta de tiempo para la clave única: dos peticiones simultáneas del mismo cambio chocan y sale un solo correo."""
    return int(ts // segundos)


# ── Variables del correo de UNA OT ───────────────────────────────────────────────────────────────────────────────
def _fila(etiqueta, valor_html, color_valor="#555", negrita=False):
    peso = "font-weight:700;" if negrita else ""
    return ('<tr><td style="padding:5px 0;font-size:13px;color:#555"><strong style="color:#222">'
            f'{esc(etiqueta)}:</strong>&nbsp; <span style="color:{color_valor};{peso}">{valor_html}</span></td></tr>')


def fila_notas(texto):
    t = limpiar_texto_tecnico(texto)
    return _fila("Qué hay que hacer", esc(t)) if t else ""


def fila_motivo(texto):
    t = limpiar_texto_tecnico(texto, 200)
    return _fila("Motivo", esc(t)) if t else ""


def bloque_antes_ahora(antes_txt, ahora_txt):
    """Dos casillas: lo que tenía (tachado, gris) y lo que tiene ahora (verde, en negrita)."""
    return (
        '<table cellpadding="0" cellspacing="0" width="100%" style="margin:18px 0"><tr>'
        '<td width="48%" style="background:#f3f4f6;border-radius:6px;padding:12px 14px;vertical-align:top">'
        '<div style="font-size:11px;font-weight:800;letter-spacing:.1em;color:#6b7280;margin-bottom:4px">ANTES</div>'
        f'<div style="font-size:14px;color:#6b7280;text-decoration:line-through">{esc(antes_txt)}</div></td>'
        '<td width="4%" style="text-align:center;font-size:18px;color:#6b7280">&rarr;</td>'
        '<td width="48%" style="background:#dcfce7;border-radius:6px;padding:12px 14px;vertical-align:top">'
        '<div style="font-size:11px;font-weight:800;letter-spacing:.1em;color:#166534;margin-bottom:4px">AHORA</div>'
        f'<div style="font-size:14px;color:#166534;font-weight:700">{esc(ahora_txt)}</div></td>'
        '</tr></table>')


def equipos_txt(nombres, tope=8):
    nombres = [str(n).strip() for n in (nombres or []) if str(n or "").strip()]
    if not nombres:
        return "—"
    if len(nombres) > tope:
        return ", ".join(nombres[:tope]) + f" y {len(nombres) - tope} más"
    return ", ".join(nombres)


def construir_variables(ot, *, nombre_tecnico="", tecnicos_nombres=None, base_url="", antes=None, motivo=""):
    """Variables de los correos de evento. `ot`: id, numero_ot, cliente, tipo_label, fecha, fecha_fin, hora_inicio,
    hora_fin, direccion, equipos (lista de nombres), descripcion. `antes`: foto previa (para «reagendada»).
    Todo va ESCAPADO (lo escribe una persona: razón social, dirección…); los *_html ya vienen armados."""
    numero = ot.get("numero_ot") or f"V-{int(ot.get('id') or 0):05d}"
    fecha_t = rango_txt(ot.get("fecha"), ot.get("fecha_fin"))
    horario_t = horario_txt(ot.get("hora_inicio"), ot.get("hora_fin"))
    nombre = (nombre_tecnico or "").strip() or "equipo"
    v = {
        "ot": esc(numero),
        "cliente": esc(ot.get("cliente") or "Trabajo interno"),
        "nombre_tecnico": esc(nombre),
        "tecnico": esc(nombre),
        "tecnicos": esc(", ".join(tecnicos_nombres or []) or nombre),
        "tipo_mantencion": esc(ot.get("tipo_label") or "Visita"),
        "fecha": esc(fecha_t),
        "horario": esc(horario_t),
        "direccion": esc(ot.get("direccion") or "—"),
        "maquina": esc(equipos_txt(ot.get("equipos"))),
        "link_ot": esc(f"{(base_url or '').rstrip('/')}/ot/{int(ot.get('id') or 0)}"),
        "notas_html": fila_notas(ot.get("descripcion")),
        "motivo_html": fila_motivo(motivo),
        "fecha_antes": "", "horario_antes": "", "fecha_ahora": esc(fecha_t), "horario_ahora": esc(horario_t),
        "antes_ahora_html": "",
    }
    if antes:
        fa = rango_txt(antes.get("fecha"), antes.get("fecha_fin"))
        ha = horario_txt(antes.get("hora_inicio"), antes.get("hora_fin"))
        v["fecha_antes"], v["horario_antes"] = esc(fa), esc(ha)
        v["antes_ahora_html"] = bloque_antes_ahora(f"{fa} · {ha}", f"{fecha_t} · {horario_t}")
    return v


# ── Agenda de mañana (recordatorio de las 17:00) ─────────────────────────────────────────────────────────────────
def ordenar_agenda(items):
    """Por hora de inicio (las que no tienen hora van al final) y luego por número de OT."""
    return sorted(items or [], key=lambda i: (hhmm(i.get("hora_inicio")) or "99:99", str(i.get("numero_ot") or "")))


def agenda_html(items, base_url=""):
    """Una fila por OT: hora a la izquierda; N° de OT, cliente, tipo, dirección y equipos a la derecha."""
    base = (base_url or "").rstrip("/")
    filas = []
    for it in ordenar_agenda(items):
        numero = it.get("numero_ot") or f"V-{int(it.get('id') or 0):05d}"
        hi, hf = hhmm(it.get("hora_inicio")), hhmm(it.get("hora_fin"))
        hora_html = (esc(hi) + (f'<br><span style="font-weight:500;color:#6b7280">{esc(hf)}</span>' if hf else "")) \
            if hi else '<span style="font-weight:500;color:#6b7280">Sin hora</span>'
        ff, fi = a_fecha(it.get("fecha_fin")), a_fecha(it.get("fecha"))
        varios = (f'<br><span style="color:#6b7280">Varios días: {esc(rango_txt(fi, ff))}</span>'
                  if (fi and ff and ff > fi) else "")
        filas.append(
            '<table cellpadding="0" cellspacing="0" width="100%" style="margin:0 0 10px">'
            '<tr>'
            '<td width="86" style="background:#fee2e2;border-left:4px solid #dc2626;padding:12px 10px;vertical-align:top;'
            f'font-size:13px;font-weight:800;color:#991b1b;text-align:center;white-space:nowrap">{hora_html}</td>'
            '<td style="background:#f5f5f7;padding:12px 14px;vertical-align:top;font-size:13px;color:#555;line-height:1.55">'
            f'<a href="{esc(base)}/ot/{int(it.get("id") or 0)}" style="color:#dc2626;font-weight:800;text-decoration:none">'
            f'{esc(numero)}</a> &middot; <strong style="color:#222">{esc(it.get("cliente") or "Trabajo interno")}</strong><br>'
            f'{esc(it.get("tipo_label") or "Visita")}<br>'
            f'{esc(it.get("direccion") or "—")}<br>'
            f'<span style="color:#6b7280">Equipos:</span> {esc(equipos_txt(it.get("equipos")))}{varios}'
            '</td></tr></table>')
    return "".join(filas)


def n_ot_txt(n):
    return "1 orden de trabajo" if n == 1 else f"{n} órdenes de trabajo"


def construir_variables_agenda(items, *, objetivo, hoy, nombre_tecnico="", base_url=""):
    n = len(items or [])
    objetivo, hoy = a_fecha(objetivo), a_fecha(hoy)
    cuando = "de mañana" if (objetivo and hoy and (objetivo - hoy).days == 1) \
        else f"del {DIAS_ES[objetivo.weekday()]}" if objetivo else "de mañana"
    nombre = (nombre_tecnico or "").strip() or "equipo"
    return {
        "nombre_tecnico": esc(nombre), "tecnico": esc(nombre),
        "dia": esc(dia_txt(objetivo)), "fecha": esc(fecha_txt(objetivo)), "cuando": esc(cuando),
        "n_ot": str(n), "n_ot_txt": esc(n_ot_txt(n)),
        "agenda_html": agenda_html(items, base_url),
        "link_agenda": esc(f"{(base_url or '').rstrip('/')}/ot/"),
    }


# ── Fechas del recordatorio ──────────────────────────────────────────────────────────────────────────────────────
def siguiente_dia_habil(d, es_habil):
    """Primer día hábil ESTRICTAMENTE posterior a d. `es_habil(date)->bool` (cl_feriados.es_dia_habil en producción)."""
    d = a_fecha(d)
    for _ in range(21):
        d = d + _dt.timedelta(days=1)
        if es_habil(d):
            return d
    return None


def dia_agenda_a_avisar(ahora, es_habil, hora=HORA_AGENDA):
    """Fecha de la agenda que hay que avisar AHORA (o None). `ahora`: datetime en hora Chile.
    Solo si HOY es día hábil y ya pasaron las 17:00: nunca sale sábado, domingo ni feriado. El viernes avisa la agenda
    del lunes; antes de un feriado, la del siguiente día hábil."""
    hoy = ahora.date()
    if not es_habil(hoy):
        return None
    if (ahora.hour, ahora.minute) < (hora, 0):
        return None
    return siguiente_dia_habil(hoy, es_habil)


def texto_sin_marcas(html_text):
    """Texto visible de un HTML (para revisar que no se escape plata/proveedor en ningún correo a técnico)."""
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html_text or "")).strip()


# ── Cuerpos de correo (sembrados en comm_templates SOLO si no existen; también sirven de respaldo) ────────────
def _p(texto):
    return f'<p style="margin:0 0 14px;font-size:14px;color:#444;line-height:1.65">{texto}</p>'


def _saludo(color="#dc2626"):
    return f'<p style="margin:0 0 16px;font-size:15px;color:{color};font-weight:700">Hola {{{{nombre_tecnico}}}}</p>'


def _tabla(color_borde, filas):
    return ('<table cellpadding="0" cellspacing="0" width="100%" style="background:#f5f5f7;'
            f'border-left:4px solid {color_borde};border-radius:4px;padding:14px 18px;margin:18px 0">' + filas + '</table>')


def _f(etiqueta, token, destacado=False):
    val = (f'<span style="color:#dc2626;font-weight:700">{token}</span>' if destacado else token)
    return ('<tr><td style="padding:5px 0;font-size:13px;color:#555"><strong style="color:#222">'
            f'{etiqueta}:</strong>&nbsp; {val}</td></tr>')


def _boton(texto="Ver orden de trabajo", token="{{link_ot}}"):
    return (f'<p style="margin:14px 0 0;font-size:13px"><a href="{token}" style="background:#dc2626;color:#fff;'
            'padding:10px 22px;border-radius:6px;text-decoration:none;font-weight:bold;display:inline-block">'
            f'{texto}</a></p>')


_FILAS_OT = (_f("OT", "{{ot}}", True) + _f("Cliente", "{{cliente}}") + _f("Tipo", "{{tipo_mantencion}}")
             + _f("Dirección", "{{direccion}}") + _f("Equipos", "{{maquina}}"))

# Nota: ninguna de estas plantillas lleva montos, costos, proveedores ni OC (REGLAS #19 y #26).
SEMILLAS = {
    # Nueva versión de la que ya existía (agrega Dirección y «Qué hay que hacer», que el técnico necesita para ir).
    # Solo reemplaza a la anterior si NADIE la había editado (ver _ensure_comm_template_ot_tecnicos en app.py).
    "ot_asignada": (
        "Nueva OT asignada — {{ot}}",
        _saludo() + _p("Se te asignó una nueva orden de trabajo. Revisa los detalles y agéndala en tu calendario.")
        + _tabla("#f59e0b", _f("OT", "{{ot}}", True) + _f("Cliente", "{{cliente}}") + _f("Tipo", "{{tipo_mantencion}}")
                 + _f("Fecha", "<strong>{{fecha}}</strong> · {{horario}}") + _f("Dirección", "{{direccion}}")
                 + _f("Equipos", "{{maquina}}") + "{{notas_html}}")
        + _boton()),
    "ot_reagendada_tecnico": (
        "OT {{ot}} reagendada — ahora {{fecha_ahora}}",
        _saludo("#0ea5e9")
        + _p("Cambió la <strong>fecha u horario</strong> de una orden de trabajo que tienes asignada. "
             "<strong>Actualiza tu agenda.</strong>")
        + "{{antes_ahora_html}}"
        + _tabla("#0ea5e9", _FILAS_OT + "{{motivo_html}}{{notas_html}}")
        + _boton()),
    "ot_cancelada_tecnico": (
        "OT {{ot}} cancelada — no necesitas ir",
        _saludo("#dc2626")
        + _p("La siguiente orden de trabajo <strong>fue cancelada</strong>. Ya no tienes que realizarla.")
        + _tabla("#dc2626", _FILAS_OT + _f("Estaba agendada para", "{{fecha}} · {{horario}}") + "{{motivo_html}}")
        + _boton("Ver orden de trabajo")),
    "ot_desasignada_tecnico": (
        "OT {{ot}} ya no está a tu cargo",
        _saludo("#6b7280")
        + _p("La orden de trabajo de abajo <strong>se asignó a otra persona</strong>. Ya no la tienes a tu cargo y "
             "puedes liberar ese horario.")
        + _tabla("#6b7280", _FILAS_OT + _f("Estaba agendada para", "{{fecha}} · {{horario}}") + "{{motivo_html}}")
        + _boton("Ver orden de trabajo")),
    "agenda_tecnico_manana": (
        "Tu agenda {{cuando}} · {{n_ot_txt}}",
        _saludo("#16a34a")
        + _p("Estas son las órdenes de trabajo que tienes asignadas para <strong>{{dia}}</strong>:")
        + "{{agenda_html}}"
        + _boton("Ver mis órdenes de trabajo", "{{link_agenda}}")
        + '<p style="margin:18px 0 0;font-size:12px;color:#888;line-height:1.5">Si algo no calza (dirección, horario o '
          'equipo), responde este correo antes de salir.</p>'),
}

# Se siembran SOLO si no existen. 'ot_asignada' se maneja aparte (ya existía; ver arriba).
SEMILLAS_NUEVAS = tuple(k for k in SEMILLAS if k != "ot_asignada")

_TOKEN = re.compile(r"\{\{\s*(\w+)\s*\}\}")


def render_simple(texto, variables):
    """Mismo reemplazo que `_render_comm_template` (sin Jinja): {{var}} → valor; lo desconocido se borra.
    Sirve de respaldo cuando la plantilla editable no está disponible."""
    vs = variables or {}
    return _TOKEN.sub(lambda m: "" if vs.get(m.group(1)) is None else str(vs.get(m.group(1))), texto or "")

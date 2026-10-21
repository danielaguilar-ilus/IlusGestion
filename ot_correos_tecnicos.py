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
# Clave de cada aviso en el REGISTRO DE CORREOS (comm_correos_registro.py): es la que lleva `correo=` hacia la puerta única de
# envío, para que Comunicaciones → «Control de correos» pueda apagar cada uno por separado (2026-10-20).
CORREO_DE = {
    "asignada": "ot_tec_asignada",
    "reagendada": "ot_tec_reagendada",
    "cancelada": "ot_tec_cancelada",
    "desasignada": "ot_tec_desasignada",
}
CORREO_AGENDA = "ot_tec_agenda_manana"
CORREO_VISITA_COPIA = "ot_tec_visita_copia"       # la copia al técnico del botón manual «Enviar email» de la visita

ESTADOS_CANCELADOS = frozenset({"cancelada", "anulada"})
# El trabajo ya se hizo (o está firmado esperando cierre): no tiene sentido «asignar» ni «reagendar».
ESTADOS_TERMINADOS = frozenset({"cerrada", "completada", "firmada_tecnico", "pendiente_aprobacion"})
ESTADOS_SIN_AGENDA = ESTADOS_CANCELADOS | ESTADOS_TERMINADOS

HORA_AGENDA = 17                # el recordatorio sale desde las 17:00 (hora Chile) del día hábil anterior
HORA_AGENDA_TOPE = 21           # 2026-10-19 (revisión): ...y solo hasta las 21:00. Si la llave se abre o el trabajo se retoma de
                                # noche, la agenda de mañana no sale a las 23:30.
AGENDA_RESERVA_MIN = 15         # un técnico "reservado" que no llegó a "enviado" en este tiempo se retoma (el trabajo se cortó)
# OT ya visitadas que esperan algo para cerrarse: siguen en la agenda (la visita puede repetirse) pero con su estado a la vista.
ESTADOS_ESPERA = {"pendiente_info": "Falta información", "pendiente_repuesto": "Falta repuesto"}
# Mientras el Anexo de Servicios del proveedor externo no esté firmado, la app no le deja ver la OT (_anexo_bloquea_ot): el correo
# tampoco le adelanta dónde ni qué hay que hacer.
OCULTO_ANEXO = "Se informa al firmar el Anexo de Servicios"
DIAS_ES = ("lunes", "martes", "miércoles", "jueves", "viernes", "sábado", "domingo")

# Variables que documenta el editor de Comunicaciones (static/comunicaciones_index.js → TPL_VARS.ot_tecnico).
VARIABLES_DOC = (
    ("ot", "N° de la orden de trabajo"),
    ("cliente", "Cliente (o «Trabajo interno»)"),
    ("nombre_tecnico", "Nombre del técnico que recibe el correo"),
    ("tecnico", "Igual que nombre_tecnico"),
    ("tecnicos", "Los técnicos (personas) de la OT; nunca la empresa proveedora"),
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
    ("dia", "Día(s) de la agenda, ej. lunes 19/10/2026 o «sábado 24/10/2026 y lunes 26/10/2026» (solo agenda)"),
    ("cuando", "«de mañana», «del lunes» o «del sábado al lunes» (solo agenda)"),
    ("n_ot", "Cantidad de OT de la agenda (solo agenda)"),
    ("n_ot_txt", "«1 orden de trabajo» / «3 órdenes de trabajo» (solo agenda)"),
    ("agenda_html", "Una fila por OT, ordenada por hora; con un encabezado por día si abarca varios (solo agenda)"),
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
# 2026-10-19 (revisión adversarial): ESTA es la única lista del módulo y cubre, como mínimo, todo lo que ya veda la campana del
# técnico (`_NOTIF_TEC_RE_VEDADO` en app.py: la prueba lo comprueba término a término). Antes dejaba pasar deuda, saldo, valor,
# tarifa, abono, descuento, transferencia, «se paga contra entrega», «50 lucas», montos escritos sin «$»... A propósito se
# sobre-filtra: perder una línea de instrucciones es barato; que un técnico lea una deuda del cliente no lo es (REGLAS #19 y #26).
# Fuera a propósito (se leerían como instrucciones normales): «ingreso» (acceso por portería) y «estimado» («tiempo estimado»).
_FRAGMENTOS_PLATA = (
    r"\$", r"€",
    r"\bclp\b", r"\busd\b", r"\buf\b", r"\bpesos?\b", r"\bd[oó]lar\w*", r"\bplata\b", r"\bdinero\b",
    r"\bcost(?:o|os|ó|a|as|ear|eo|ar|ará|aría|e|an|aron)\b", r"\bcuest(?:a|an)\b",
    r"\bprecio\w*", r"\bmonto\w*", r"valor\w*", r"\btarif\w*", r"\bdescuent\w*", r"\brecargo\w*",
    r"\bhonorario\w*", r"\bsueldo\w*", r"\bcuotas?\b", r"\bdesembols\w*",
    r"\bcobr\w*", r"\bpag(?!in)\w*", r"\bsald(?:o|os|ar|ado|ada|ó)\b", r"\bdeud\w*", r"\bmoros\w*",
    r"\babon(?:o|os|ar|ado|ada|ó)\b", r"\banticipo\w*", r"\btransfer(?:encia|encias|ir|ido|ida|ió)\b",
    r"\bcr[eé]dito\w*", r"\bd[eé]bito\w*", r"\bcheque\w*", r"\bcontra\s+entrega\b",
    r"\bmargen\w*", r"\bgananc\w*", r"\bp[eé]rdid\w*", r"\butilidad\w*", r"\bingresos\b",
    r"\bpresupuest\w*", r"\bcotiz\w*", r"factur\w*", r"\bboleta\w*", r"\bnota\s+de\s+(?:venta|cr[eé]dito|d[eé]bito)\b",
    r"\b(?:fcv|fce|fcc|blv|nvv|nvi|ncv|ncc|ocv|occ)\b",
    r"proveedor\w*", r"\boc\b", r"\bo\.c\.", r"\bo/c\b", r"\borden(?:es)?\s+de\s+compra\b", r"\bneto\b", r"\biva\b",
    r"autoriz\w*", r"finanz\w*",
    # montos sin «$»: 120.000 · 2,500 · 85 mil · 50 lucas · 3 palos · 2 millones
    r"\b\d{1,3}(?:[.,]\d{3})+\b",
    r"\b\d[\d.,]*\s*(?:mil|lucas?|lukas?|palos?|millon\w*)\b",
    r"\b(?:un|dos|tres|cuatro|cinco|seis|siete|ocho|nueve|diez|veinte|treinta|cuarenta|cincuenta|cien|ciento|medio)\s+"
    r"(?:mil|lucas?|lukas?|palos?|millon\w*)\b",
)
_RE_PLATA = re.compile("(" + "|".join(_FRAGMENTOS_PLATA) + ")", re.I)


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
    """Identifica «este aviso por este cambio» (idempotencia: el mismo aviso no sale dos veces).

    2026-10-19 (revisión adversarial): la clave mira SIEMPRE cómo estaba la OT antes y cómo quedó después (estado, fecha,
    horario y la lista de técnicos). Antes, «asignada», «desasignada» y «cancelada» solo miraban el estado final y la
    cubeta de un minuto: A (asignada) → B (A pierde la OT) → de vuelta a A en el mismo minuto daba la MISMA clave que la
    creación y el «asignada» final se descartaba como «ya enviado»: A se quedaba con «ya no está a tu cargo» teniéndola.
    Dos peticiones idénticas a la vez (doble clic) siguen chocando entre sí, que es lo que se quiere."""
    def _k(f):
        if not f:
            return "-"
        tec = ",".join(sorted(str(e).lower() for e in (f.get("tecnicos") or {})))
        return "|".join(str(f.get(k, "")) for k in ("estado", "fecha", "fecha_fin", "hora_inicio", "hora_fin")) + "|" + tec
    base = f"{evento}|{str(email).lower()}|{_k(antes)}|{_k(despues)}"
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


def construir_variables(ot, *, nombre_tecnico="", tecnicos_nombres=None, base_url="", antes=None, motivo="",
                        ocultar_detalle=False):
    """Variables de los correos de evento. `ot`: id, numero_ot, cliente, tipo_label, fecha, fecha_fin, hora_inicio,
    hora_fin, direccion, equipos (lista de nombres), descripcion. `antes`: foto previa (para «reagendada»).
    `ocultar_detalle`: el Anexo de Servicios del proveedor aún no está firmado → sin dirección ni «Qué hay que hacer».
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
        "direccion": esc(OCULTO_ANEXO if ocultar_detalle else (ot.get("direccion") or "—")),
        "maquina": esc(equipos_txt(ot.get("equipos"))),
        "link_ot": esc(f"{(base_url or '').rstrip('/')}/ot/{int(ot.get('id') or 0)}"),
        "notas_html": "" if ocultar_detalle else fila_notas(ot.get("descripcion")),
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


def _fila_agenda(it, base):
    """Una OT de la agenda: hora a la izquierda; N° de OT, cliente, tipo, dirección y equipos a la derecha."""
    numero = it.get("numero_ot") or f"V-{int(it.get('id') or 0):05d}"
    hi, hf = hhmm(it.get("hora_inicio")), hhmm(it.get("hora_fin"))
    hora_html = (esc(hi) + (f'<br><span style="font-weight:500;color:#6b7280">{esc(hf)}</span>' if hf else "")) \
        if hi else '<span style="font-weight:500;color:#6b7280">Sin hora</span>'
    ff, fi = a_fecha(it.get("fecha_fin")), a_fecha(it.get("fecha"))
    varios = (f'<br><span style="color:#6b7280">Varios días: {esc(rango_txt(fi, ff))}</span>'
              if (fi and ff and ff > fi) else "")
    # Una OT que ya se visitó y espera información o un repuesto sigue en la agenda (la visita puede repetirse), pero
    # con su estado a la vista para que no se lea como una visita normal.
    espera = ESTADOS_ESPERA.get(str(it.get("estado") or "").strip().lower())
    estado_html = f'<br><span style="color:#b45309;font-weight:700">Estado: {esc(espera)}</span>' if espera else ""
    direccion = OCULTO_ANEXO if it.get("oculta_detalle") else (it.get("direccion") or "—")
    return (
        '<table cellpadding="0" cellspacing="0" width="100%" style="margin:0 0 10px">'
        '<tr>'
        '<td width="86" style="background:#fee2e2;border-left:4px solid #dc2626;padding:12px 10px;vertical-align:top;'
        f'font-size:13px;font-weight:800;color:#991b1b;text-align:center;white-space:nowrap">{hora_html}</td>'
        '<td style="background:#f5f5f7;padding:12px 14px;vertical-align:top;font-size:13px;color:#555;line-height:1.55">'
        f'<a href="{esc(base)}/ot/{int(it.get("id") or 0)}" style="color:#dc2626;font-weight:800;text-decoration:none">'
        f'{esc(numero)}</a> &middot; <strong style="color:#222">{esc(it.get("cliente") or "Trabajo interno")}</strong><br>'
        f'{esc(it.get("tipo_label") or "Visita")}<br>'
        f'{esc(direccion)}<br>'
        f'<span style="color:#6b7280">Equipos:</span> {esc(equipos_txt(it.get("equipos")))}{varios}{estado_html}'
        '</td></tr></table>')


def agrupar_por_dia(items, dias):
    """[(fecha, [OT])] solo de los días que tienen OT. Cada OT va UNA vez, en el primer día de `dias` que cubre
    (fecha_programada ≤ día ≤ fecha_fin): una instalación de sábado a martes no se repite en tres encabezados."""
    dias = [d for d in (a_fecha(x) for x in (dias or [])) if d]
    grupos = {d: [] for d in dias}
    for it in (items or []):
        ini = a_fecha(it.get("fecha"))
        fin = a_fecha(it.get("fecha_fin")) or ini
        if ini and fin and fin < ini:
            fin = ini
        destino = next((d for d in dias if ini is None or (ini <= d <= fin)), None)
        if destino is None and dias:
            destino = dias[-1]
        if destino is not None:
            grupos[destino].append(it)
    return [(d, grupos[d]) for d in dias if grupos[d]]


def agenda_html(items, base_url="", dias=None):
    """Una fila por OT (ver _fila_agenda), ordenadas por hora. Con `dias` (varios días de agenda en un mismo correo) las
    agrupa bajo un encabezado por día, p. ej. «sábado 24/10/2026»."""
    base = (base_url or "").rstrip("/")
    if dias is None:
        return "".join(_fila_agenda(it, base) for it in ordenar_agenda(items))
    partes = []
    for d, grupo in agrupar_por_dia(items, dias):
        partes.append('<p style="margin:16px 0 8px;font-size:12px;font-weight:800;letter-spacing:.06em;'
                      f'color:#991b1b;text-transform:uppercase">{esc(dia_txt(d))}</p>')
        partes.extend(_fila_agenda(it, base) for it in ordenar_agenda(grupo))
    return "".join(partes)


def n_ot_txt(n):
    return "1 orden de trabajo" if n == 1 else f"{n} órdenes de trabajo"


def _lista_es(xs):
    xs = [x for x in xs if x]
    return xs[0] if len(xs) == 1 else (", ".join(xs[:-1]) + " y " + xs[-1] if xs else "")


def construir_variables_agenda(items, *, objetivo, hoy, nombre_tecnico="", base_url="", dias=None):
    """Variables del correo de la agenda. `objetivo`: el día hábil de la agenda. `dias`: todos los días que cubre este correo
    (de mañana hasta el siguiente día hábil: el viernes son sábado, domingo y lunes; antes de un feriado también el feriado).
    Con OT en UN solo día el correo es el de siempre («de mañana» / «del lunes»); con OT en varios días las agrupa bajo un
    encabezado por día, y `dia`/`cuando` pasan a decir «sábado 24/10/2026 y lunes 26/10/2026» / «del sábado al lunes»."""
    n = len(items or [])
    objetivo, hoy = a_fecha(objetivo), a_fecha(hoy)
    grupos = agrupar_por_dia(items, dias if dias else ([objetivo] if objetivo else []))
    nombre = (nombre_tecnico or "").strip() or "equipo"
    if len(grupos) > 1:
        primero, ultimo = grupos[0][0], grupos[-1][0]
        cuando = f"del {DIAS_ES[primero.weekday()]} al {DIAS_ES[ultimo.weekday()]}"
        dia = _lista_es([dia_txt(d) for d, _g in grupos])
        fecha = f"{fecha_txt(primero)} al {fecha_txt(ultimo)}"
        agenda = agenda_html(items, base_url, dias=[d for d, _g in grupos])
    else:
        d0 = grupos[0][0] if grupos else objetivo
        cuando = "de mañana" if (d0 and hoy and (d0 - hoy).days == 1) \
            else f"del {DIAS_ES[d0.weekday()]}" if d0 else "de mañana"
        dia, fecha = dia_txt(d0), fecha_txt(d0)
        agenda = agenda_html(items, base_url)
    return {
        "nombre_tecnico": esc(nombre), "tecnico": esc(nombre),
        "dia": esc(dia), "fecha": esc(fecha), "cuando": esc(cuando),
        "n_ot": str(n), "n_ot_txt": esc(n_ot_txt(n)),
        "agenda_html": agenda,
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


def dia_agenda_a_avisar(ahora, es_habil, hora=HORA_AGENDA, tope=HORA_AGENDA_TOPE):
    """Fecha de la agenda (el siguiente día hábil) que hay que avisar AHORA (o None). `ahora`: datetime en hora Chile.
    Solo si HOY es día hábil y es entre las 17:00 y las 21:00: nunca sale sábado, domingo, feriado ni de noche.
    El viernes avisa la agenda del lunes; antes de un feriado, la del siguiente día hábil."""
    hoy = ahora.date()
    if not es_habil(hoy):
        return None
    if (ahora.hour, ahora.minute) < (hora, 0) or ahora.hour >= tope:
        return None
    return siguiente_dia_habil(hoy, es_habil)


def dias_agenda_a_avisar(ahora, es_habil, hora=HORA_AGENDA, tope=HORA_AGENDA_TOPE):
    """Todos los días que cubre el recordatorio de AHORA: desde mañana hasta el siguiente día hábil, inclusive (lista vacía
    si no toca avisar). Lunes a jueves es solo mañana; el viernes son sábado, domingo y lunes; antes de un feriado también
    el feriado. 2026-10-19 (revisión): antes solo se miraba el siguiente día hábil, así que una instalación agendada en
    sábado o en un feriado (se permite, con aviso) nunca aparecía en ninguna agenda."""
    ultimo = dia_agenda_a_avisar(ahora, es_habil, hora, tope)
    if not ultimo:
        return []
    out, d = [], ahora.date() + _dt.timedelta(days=1)
    while d <= ultimo:
        out.append(d)
        d += _dt.timedelta(days=1)
    return out


def avisa_eliminacion(estado, fecha, fecha_fin, hoy):
    """¿Borrar esta OT merece un «cancelada» al técnico? Solo si todavía estaba por hacerse: ni cancelada/anulada (ya se le
    dijo), ni terminada (el trabajo ya se hizo) ni con la fecha ya pasada. Sin fecha (por confirmar) sí avisa."""
    est = str(estado or "").strip().lower()
    if est in ESTADOS_CANCELADOS or est in ESTADOS_TERMINADOS:
        return False
    ini = a_fecha(fecha)
    fin = a_fecha(fecha_fin) or ini
    h = a_fecha(hoy)
    if ini and h and max(ini, fin or ini) < h:
        return False
    return True


def cuerpo_sin_montos(body, *, quitar=(), saludo=None, saludo_nuevo="Hola equipo"):
    """El correo que se armó para el CLIENTE, listo para un técnico: sin los trozos que llevan montos (costo estimado,
    repuestos con su costo) y con un saludo que no es el del cliente. Revisión 2026-10-19: «Enviar email» de la visita le
    mandaba a cada técnico la misma copia del cliente, con plata en el respaldo en HTML (REGLAS #19 y #26)."""
    out = body or ""
    for trozo in (quitar or ()):
        if trozo:
            out = out.replace(trozo, "")
    if saludo:
        out = out.replace(saludo, saludo_nuevo, 1)
    return out


def texto_sin_marcas(html_text):
    """Texto visible de un HTML (para revisar que no se escape plata/proveedor en ningún correo a técnico)."""
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html_text or "")).strip()


# ── Archivo de calendario (.ics) adjunto al correo del técnico ──────────────────────────────────────────────────
# Daniel (decisión recomendada y aceptada, 20-oct-2026): el correo de «asignada» y «reagendada» lleva un .ics (METHOD:REQUEST)
# y el de «cancelada» y «desasignada» otro con METHOD:CANCEL; así la OT queda en el calendario del técnico y se actualiza o
# se borra sola. Lógica PURA (sin Flask ni base): el envío vive en app.py (`_ot_tec_despachar`).
#
#  · UID ESTABLE por OT (`ot-{numero}@ilusfitness.com`, igual que `_build_ot_ics` de Tickets): reenviar ACTUALIZA el evento
#    en vez de duplicarlo, y el CANCEL lo borra.
#  · SEQUENCE creciente: segundos desde 2026-01-01 UTC (sin guardar nada en la base: cada correo posterior tiene un número
#    mayor, así un reagendar siempre gana sobre el anterior y el CANCEL sobre ambos).
#  · Horas: la OT está en hora de Chile (America/Santiago); se convierten a UTC con sufijo Z (cambio de horario de verano
#    incluido, lo resuelve zoneinfo). Sin hora definida: evento de día completo.
#  · CONFIDENCIALIDAD (REGLAS #19 y #26): el texto libre de una persona (qué hay que hacer, motivo) pasa por
#    `limpiar_texto_tecnico`; nunca hay montos, costos, proveedores ni OC. Con el Anexo de Servicios sin firmar
#    (`ocultar_detalle`) no lleva dirección ni «qué hay que hacer», igual que el correo.
SEQ_EPOCH = 1767225600                  # 2026-01-01T00:00:00Z
ICS_DOMINIO = "ilusfitness.com"
ICS_MAX_DIAS = 60                       # tope de ocurrencias de una OT de varios días con hora
ICS_METODO = {"asignada": "REQUEST", "reagendada": "REQUEST", "cancelada": "CANCEL", "desasignada": "CANCEL"}
ICS_ALARMA = "-PT2H"                    # aviso 2 horas antes (solo en REQUEST y solo si hay hora)
_TZ_CHILE = "America/Santiago"


def secuencia_desde(ts):
    """SEQUENCE a partir de la hora del envío (epoch en segundos). Siempre creciente; nunca negativa."""
    try:
        return max(0, int(float(ts)) - SEQ_EPOCH)
    except (TypeError, ValueError):
        return 0


def ics_uid(numero):
    """Identificador estable del evento de una OT (mismo patrón que Tickets)."""
    limpio = re.sub(r"[^A-Za-z0-9._-]", "", str(numero or ""))
    return f"ot-{limpio}@{ICS_DOMINIO}"


def ics_nombre_archivo(numero):
    limpio = re.sub(r"[^A-Za-z0-9._-]", "_", str(numero or "")) or "OT"
    return f"OT_{limpio}.ics"


def ics_escapar(v):
    """Escape de un valor TEXT (RFC 5545 §3.3.11): \\ ; , y saltos de línea."""
    s = "" if v is None else str(v)
    return (s.replace("\\", "\\\\").replace(";", "\\;").replace(",", "\\,")
            .replace("\r\n", "\\n").replace("\n", "\\n").replace("\r", "\\n"))


def _param(v):
    """Valor de parámetro entre comillas (CN): sin comillas, dos puntos ni saltos que rompan la línea."""
    s = re.sub(r'["\r\n;:]', " ", str(v or "")).strip()
    return f'"{s}"' if s else ""


def ics_plegar(linea):
    """Pliega una línea a máx. 75 octetos UTF-8 (continuación: CRLF + un espacio), sin partir un carácter."""
    b = linea.encode("utf-8")
    if len(b) <= 75:
        return linea
    partes, actual, largo = [], "", 0
    limite = 75
    for ch in linea:
        n = len(ch.encode("utf-8"))
        if largo + n > limite:
            partes.append(actual)
            actual, largo, limite = ch, n, 74          # las continuaciones llevan 1 octeto de espacio inicial
        else:
            actual += ch
            largo += n
    partes.append(actual)
    return "\r\n ".join(partes)


def _utc_z(d, hm):
    """date + 'HH:MM' en hora de Chile → 'YYYYMMDDTHHMMSSZ' (UTC)."""
    from zoneinfo import ZoneInfo
    h, m = int(hm[:2]), int(hm[3:5])
    local = _dt.datetime(d.year, d.month, d.day, h, m, tzinfo=ZoneInfo(_TZ_CHILE))
    return local.astimezone(_dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _fin_hm(hi, hf):
    """Hora de término: la indicada si es posterior al inicio; si no (o falta), una hora después."""
    if hf and hf > hi:
        return hf
    h, m = int(hi[:2]), int(hi[3:5])
    tot = min(h * 60 + m + 60, 23 * 60 + 59)
    return f"{tot // 60:02d}:{tot % 60:02d}"


def _lineas_fecha(ini, fin, hi, hf):
    """DTSTART/DTEND (+ RDATE si dura varios días con hora). ini/fin: date; hi/hf: 'HH:MM' o ''."""
    if not hi:                                                          # día completo (DTEND exclusivo)
        sig = (fin or ini) + _dt.timedelta(days=1)
        return [f"DTSTART;VALUE=DATE:{ini.strftime('%Y%m%d')}", f"DTEND;VALUE=DATE:{sig.strftime('%Y%m%d')}"], False
    hf = _fin_hm(hi, hf)
    out = [f"DTSTART:{_utc_z(ini, hi)}", f"DTEND:{_utc_z(ini, hf)}"]
    if fin and fin > ini:                                               # mismo horario cada día del rango
        dias, d = [], ini + _dt.timedelta(days=1)
        while d <= fin and len(dias) < ICS_MAX_DIAS - 1:
            dias.append(_utc_z(d, hi))
            d += _dt.timedelta(days=1)
        if dias:
            out.append("RDATE:" + ",".join(dias))
    return out, True


def ics_evento(ot, evento, *, correo, nombre="", ts, base_url="", organizador="soportetec@sphs.cl",
               ocultar_detalle=False, link_lista=False, motivo=""):
    """Bytes del .ics de UNA OT para UN técnico, o None si no se puede armar (evento desconocido o OT sin fecha).

    ot: id, numero_ot, cliente, tipo_label, fecha, fecha_fin, hora_inicio, hora_fin, direccion, equipos, descripcion.
        Para «cancelada» y «desasignada» pasar la fecha/hora que el técnico tenía en su calendario (la que se borra).
    evento: asignada | reagendada → METHOD:REQUEST · cancelada | desasignada → METHOD:CANCEL.
    ts: epoch en segundos del envío (SEQUENCE y DTSTAMP). link_lista: la OT ya no existe (link a la lista de OT)."""
    metodo = ICS_METODO.get(evento)
    ini = a_fecha(ot.get("fecha"))
    if not metodo or not ini:
        return None
    fin = a_fecha(ot.get("fecha_fin"))
    if fin and fin < ini:
        fin = ini
    hi, hf = hhmm(ot.get("hora_inicio")), hhmm(ot.get("hora_fin"))
    try:
        fechas, con_hora = _lineas_fecha(ini, fin, hi, hf)
    except Exception:                                   # sin base de zonas horarias: sin archivo (el correo sale igual)
        return None
    numero = ot.get("numero_ot") or f"V-{int(ot.get('id') or 0):05d}"
    cliente = (ot.get("cliente") or "").strip() or "Trabajo interno"
    tipo = (ot.get("tipo_label") or "").strip()
    etiqueta = str(numero) if str(numero).upper().startswith("OT") else f"OT {numero}"      # «OT-2026-00005» ya dice OT
    titulo = f"{etiqueta} — {cliente}" + (f" ({tipo})" if tipo else "")
    base = (base_url or "").rstrip("/")
    link = f"{base}/ot/" if link_lista else f"{base}/ot/{int(ot.get('id') or 0)}"
    cancela = metodo == "CANCEL"

    desc = [f"Orden de trabajo {numero}.", f"Cliente: {cliente}."]
    if tipo:
        desc.append(f"Tipo: {tipo}.")
    equipos = equipos_txt(ot.get("equipos"))
    if equipos != "—":
        desc.append(f"Equipos: {equipos}.")
    if cancela:
        desc.append("Esta orden de trabajo ya no está en tu agenda.")
        m = limpiar_texto_tecnico(motivo, 200)
        if m:
            desc.append(f"Motivo: {m}.")
    elif not ocultar_detalle:
        q = limpiar_texto_tecnico(ot.get("descripcion"))
        if q:
            desc.append(f"Qué hay que hacer: {q}")
        if evento == "reagendada":
            m = limpiar_texto_tecnico(motivo, 200)
            if m:
                desc.append(f"Motivo del cambio: {m}.")
    desc.append(f"Ficha: {link}")

    direccion = "" if ocultar_detalle else (ot.get("direccion") or "").strip()
    if direccion in ("—", "-"):
        direccion = ""

    stamp = _dt.datetime.fromtimestamp(float(ts), _dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    org = f"ORGANIZER;CN={_param('ILUS Fitness')}:mailto:{organizador}"
    att = ("ATTENDEE;" + (f"CN={_param(nombre)};" if _param(nombre) else "")
           + f"CUTYPE=INDIVIDUAL;ROLE=REQ-PARTICIPANT;PARTSTAT=NEEDS-ACTION;RSVP=FALSE:mailto:{correo}")
    L = ["BEGIN:VCALENDAR", "VERSION:2.0", "PRODID:-//ILUS Fitness//Ordenes de trabajo//ES",
         "CALSCALE:GREGORIAN", f"METHOD:{metodo}", "BEGIN:VEVENT",
         f"UID:{ics_uid(numero)}", f"DTSTAMP:{stamp}", f"SEQUENCE:{secuencia_desde(ts)}"]
    L += fechas
    L += [f"SUMMARY:{ics_escapar(titulo)}"]
    if direccion:
        L.append(f"LOCATION:{ics_escapar(direccion)}")
    L += [f"DESCRIPTION:{ics_escapar(chr(10).join(desc))}", f"URL:{link}", org, att,
          f"STATUS:{'CANCELLED' if cancela else 'CONFIRMED'}", "TRANSP:OPAQUE"]
    if not cancela and con_hora:                        # sin hora no hay "2 h antes" que avisar
        L += ["BEGIN:VALARM", f"TRIGGER:{ICS_ALARMA}", "ACTION:DISPLAY",
              f"DESCRIPTION:{ics_escapar('Recordatorio ' + etiqueta)}", "END:VALARM"]
    L += ["END:VEVENT", "END:VCALENDAR"]
    return ("\r\n".join(ics_plegar(x) for x in L) + "\r\n").encode("utf-8")


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
        _saludo() + _p("Se te asignó una nueva orden de trabajo. Revisa los detalles y abre el archivo adjunto: "
                                  "la orden de trabajo queda en tu calendario.")
        + _tabla("#f59e0b", _f("OT", "{{ot}}", True) + _f("Cliente", "{{cliente}}") + _f("Tipo", "{{tipo_mantencion}}")
                 + _f("Fecha", "<strong>{{fecha}}</strong> · {{horario}}") + _f("Dirección", "{{direccion}}")
                 + _f("Equipos", "{{maquina}}") + "{{notas_html}}")
        + _boton()),
    "ot_reagendada_tecnico": (
        "OT {{ot}} reagendada — ahora {{fecha_ahora}}",
        _saludo("#0ea5e9")
        + _p("Cambió la <strong>fecha u horario</strong> de una orden de trabajo que tienes asignada. "
             "<strong>Abre el archivo adjunto</strong> y tu calendario se actualiza solo.")
        + "{{antes_ahora_html}}"
        + _tabla("#0ea5e9", _FILAS_OT + "{{motivo_html}}{{notas_html}}")
        + _boton()),
    "ot_cancelada_tecnico": (
        "OT {{ot}} cancelada — no necesitas ir",
        _saludo("#dc2626")
        + _p("La siguiente orden de trabajo <strong>fue cancelada</strong>. Ya no tienes que realizarla. "
             "Abre el archivo adjunto y el evento se borra de tu calendario.")
        + _tabla("#dc2626", _FILAS_OT + _f("Estaba agendada para", "{{fecha}} · {{horario}}") + "{{motivo_html}}")
        + _boton("Ver orden de trabajo")),
    # 2026-10-20 (Daniel: «no digas "te quité la OT"… di algo más sutil, porque se sentiría como un castigo»): el aviso
    # de una OT que pasa a otra persona habla de un AJUSTE de la agenda y agradece; nunca de quitar ni de «ya no figuras».
    "ot_desasignada_tecnico": (
        "Ajuste en tu agenda · OT {{ot}}",
        _saludo("#6b7280")
        + _p("Hubo un <strong>ajuste en tu agenda</strong>: la orden de trabajo de abajo la atenderá otra persona del "
             "equipo. Gracias por tu disposición; no tienes que hacer nada más.")
        + _p("Si abres el archivo adjunto, ese horario se libera en tu calendario.")
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

# Versiones ANTERIORES de una semilla ya sembrada. Si en la base la plantilla sigue siendo EXACTAMENTE una de estas (nadie la
# editó), se actualiza a la versión vigente de SEMILLAS; si una persona la tocó, queda como está (mismo criterio que
# 'ot_asignada'). Se usa para mejorar el tono de un texto que todavía no editó nadie (ver _ensure_comm_template_ot_tecnicos).
SEMILLAS_ANTERIORES = {
    "ot_desasignada_tecnico": [(
        "OT {{ot}} ya no está a tu cargo",
        _saludo("#6b7280")
        + _p("Ya <strong>no figuras como técnico</strong> de la orden de trabajo de abajo (se reasignó o se reorganizó el "
             "equipo). Ya no la tienes a tu cargo y puedes liberar ese horario: "
             "abre el archivo adjunto y el evento se borra de tu calendario.")
        + _tabla("#6b7280", _FILAS_OT + _f("Estaba agendada para", "{{fecha}} · {{horario}}") + "{{motivo_html}}")
        + _boton("Ver orden de trabajo")),
    ],
}

_TOKEN = re.compile(r"\{\{\s*(\w+)\s*\}\}")


def render_simple(texto, variables):
    """Mismo reemplazo que `_render_comm_template` (sin Jinja): {{var}} → valor; lo desconocido se borra.
    Sirve de respaldo cuando la plantilla editable no está disponible."""
    vs = variables or {}
    return _TOKEN.sub(lambda m: "" if vs.get(m.group(1)) is None else str(vs.get(m.group(1))), texto or "")

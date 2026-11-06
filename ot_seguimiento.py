# -*- coding: utf-8 -*-
"""SEGUIMIENTO EN VIVO DE LA ORDEN DE TRABAJO PARA EL CLIENTE (Daniel, 6-nov-2026) — REGLA #67.

Daniel, viendo en Outlook el correo «Tu visita técnica está agendada»: «Es necesario mejorar el tracking y colocar algo dinámico, bien bonito…
no me gusta tanta letra: puede ser muy sencilla la información». Quiere que el CLIENTE vea en qué punto va su servicio.

Este módulo es PURO (sin base de datos, sin Flask, sin correo). Dados los datos de una OT (sin plata), decide:
  · en qué PASO del servicio está (agendada → recordatorio → visita y firma → documento firmado), con un título y un mensaje cortos,
    coherentes con el estado REAL (nunca se promete lo que no va a pasar: la página de una OT terminada no dice «te enviaremos un recordatorio»);
  · qué se le muestra al cliente (`a_json`: SOLO las claves de CLAVES_PUBLICAS) y qué JAMÁS (montos, costos, proveedores, OC, margen,
    autorizaciones, notas internas, teléfonos o correos de técnicos: REGLAS #19 y #26);
  · el token del enlace público (`token` / `validar`): HMAC con su PROPIO propósito («seg»), distinto del de la firma remota y del documento
    firmado, así un enlace de seguimiento no sirve para otra cosa. Sin estado en la base. Vence a los TTL_DIAS de emitirse y, además, una OT
    cerrada deja de mostrarse DIAS_TRAS_CIERRE días después del cierre (`vigente_tras_cierre`).

Las lecturas y las rutas viven en app.py (`_ot_seg_*`, `/ot-seguimiento/<token>` y `/ot-seguimiento/<token>/estado`). Pruebas:
tests/test_ot_seguimiento.py.
"""
import base64 as _b64
import datetime as _dt
import hashlib as _hl
import hmac as _hmac

import ot_correos_tecnicos as _otc

# ── Enlace ───────────────────────────────────────────────────────────────────────────────────────────────────────────────
PROPOSITO = "seg"            # va dentro de lo que se firma: un token de firma remota o del documento firmado NO abre esta página
TTL_DIAS = 90                # cada correo emite un enlace nuevo; el último (OT terminada) cubre de sobra los 30 días de después del cierre
DIAS_TRAS_CIERRE = 30        # una OT cerrada se sigue viendo hasta 30 días después del cierre (igual que el documento firmado, REGLA #37)
LARGO_MAX_TOKEN = 200

# ── Los pasos del servicio (las MISMAS claves y el mismo orden que ot_comunicacion.PASOS_SERVICIO: una prueba lo vigila) ───
PASOS = (("agendada", "Visita agendada"), ("recordatorio", "Recordatorio"), ("visita", "Visita y firma"), ("documento", "Documento firmado"))
CLAVES_PASOS = tuple(c for c, _n in PASOS)

# Estados REALES de mant_visitas.estado (ENUM) agrupados por lo que significan para el cliente.
EST_CANCELADA = ("cancelada", "anulada")
EST_CERRADA = ("cerrada",)
EST_VISITA_HECHA = ("firmada_tecnico", "pendiente_aprobacion", "completada")      # el técnico ya terminó: faltan firmas o la aprobación
EST_EN_PAUSA = ("pendiente_info", "pendiente_repuesto")
EST_EN_CURSO = ("en_curso", "en_ejecucion")
EST_AGENDADA = ("creada", "programada", "asignada", "reagendada")

SIT_CURSO, SIT_TERMINADA, SIT_CANCELADA = "curso", "terminada", "cancelada"
PROGRESO = {"agendada": 12, "recordatorio": 38, "visita": 62, "documento": 100}

# Lo ÚNICO que viaja al cliente (página y JSON). Agregar una clave acá es una decisión consciente; la prueba de confidencialidad las recorre.
CLAVES_PUBLICAS = (
    "numero", "tipo", "cliente", "situacion", "paso", "pasos", "progreso", "linea", "tono", "icono", "titulo", "mensaje", "que_sigue",
    "fecha_txt", "dia_txt", "horario_txt", "cuando", "tecnicos", "n_equipos", "equipos_txt", "equipos", "firmas",
    "documento_listo", "link_firmada", "link_soporte", "actualizado",
)

# Íconos de la página (un sprite SVG en la plantilla; el navegador solo cambia cuál se muestra).
ICONOS = ("calendario", "campana", "tecnico", "firma", "documento", "pausa", "alerta", "cancelada")


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  El enlace
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
def _clave(k):
    if isinstance(k, str):
        return k.encode()
    return k or b"ilus-seguimiento"


def _firma(vid, exp, clave):
    return _hmac.new(_clave(clave), f"{PROPOSITO}:{int(vid)}:{int(exp)}".encode(), _hl.sha256).hexdigest()[:24]


def token(vid, exp_epoch, clave):
    """Token del enlace de seguimiento de la OT `vid`, válido hasta `exp_epoch` (segundos desde 1970). Lleva el propósito dentro de la firma."""
    vid, exp = int(vid), int(exp_epoch)
    return _b64.urlsafe_b64encode(f"{PROPOSITO}:{vid}:{exp}:{_firma(vid, exp, clave)}".encode()).decode().rstrip("=")


def validar(tok, clave, ahora_epoch):
    """`vid` si el token es de seguimiento, está bien firmado y no venció; si no, None. Un token de otro propósito (firma remota, documento
    firmado) da None aunque esté bien firmado con la misma clave."""
    try:
        tok = str(tok or "")
        if not tok or len(tok) > LARGO_MAX_TOKEN:
            return None
        raw = _b64.urlsafe_b64decode(tok + "=" * (-len(tok) % 4)).decode()
        prop, vid_s, exp_s, sig = raw.split(":")
        if prop != PROPOSITO:
            return None
        vid, exp = int(vid_s), int(exp_s)
        if vid <= 0 or not _hmac.compare_digest(_firma(vid, exp, clave), sig) or float(ahora_epoch) > exp:
            return None
        return vid
    except Exception:
        return None


def vigente_tras_cierre(cerrada_at, ahora):
    """Una OT cerrada se muestra hasta DIAS_TRAS_CIERRE días después del cierre. `cerrada_at` y `ahora` naive en la MISMA zona (UTC de la base).
    Sin fecha de cierre, se muestra."""
    c = _a_datetime(cerrada_at)
    if c is None:
        return True
    a = _a_datetime(ahora)
    if a is None:
        return True
    if a.tzinfo is not None:
        a = a.replace(tzinfo=None)
    return a <= c + _dt.timedelta(days=DIAS_TRAS_CIERRE)


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  Dónde va el servicio
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
def _a_datetime(v):
    if v is None or v == "":
        return None
    if isinstance(v, _dt.datetime):
        return v
    if isinstance(v, _dt.date):
        return _dt.datetime(v.year, v.month, v.day)
    try:
        return _dt.datetime.fromisoformat(str(v).strip()[:19])
    except Exception:
        return None


def _a_hora(v):
    """'HH:MM', time, timedelta → time (o None)."""
    if v is None or v == "":
        return None
    if isinstance(v, _dt.time):
        return v
    if isinstance(v, _dt.timedelta):
        s = int(v.total_seconds()) % 86400
        return _dt.time(s // 3600, (s % 3600) // 60)
    try:
        hh, mm = str(v).strip()[:5].split(":")[:2]
        return _dt.time(int(hh), int(mm))
    except Exception:
        return None


def _nombres(lista):
    return [str(n).strip() for n in (lista or []) if str(n or "").strip()]


def _plural(n, uno, varios):
    return uno if n == 1 else varios


def firmas_de(d):
    """Las tres firmas del documento, como las ve el cliente (sin horas internas): técnico · cliente («Tú») · Servicio Técnico. Una OT interna
    no lleva la del cliente."""
    out = [{"clave": "tecnico", "nombre": "Técnico", "ok": bool(d.get("firma_tecnico_at") or d.get("firma_tecnico_url"))}]
    if not d.get("interna"):
        out.append({"clave": "cliente", "nombre": "Tú", "ok": bool(d.get("firma_cliente_at") or d.get("firma_cliente_url"))})
    out.append({"clave": "supervisor", "nombre": "Servicio Técnico",
                "ok": bool(d.get("firma_supervisor_at") or d.get("firma_supervisor_url") or d.get("cerrada_at"))})
    return out


def documento_listo(d):
    """Cerrada + las tres firmas (la del cliente no se exige en un trabajo interno): el mismo criterio barato de ot_comunicacion.documento_listo."""
    if str(d.get("estado") or "").strip().lower() not in EST_CERRADA:
        return False
    if not (d.get("firma_cliente_url") or d.get("firma_cliente_at")) and not d.get("interna"):
        return False
    if not (d.get("firma_tecnico_url") or d.get("firma_tecnico_at")):
        return False
    return bool(d.get("firma_supervisor_url") or d.get("firma_supervisor_at"))


def cuando_es(fecha, hoy):
    f = _otc.a_fecha(fecha)
    if not f:
        return "sin_fecha"
    if f == hoy:
        return "hoy"
    if f == hoy + _dt.timedelta(days=1):
        return "manana"
    return "pasado" if f < hoy else "futuro"


def evaluar(d, ahora, promesas=None):
    """`d`: estado, interna, fecha (programada), hora_inicio, hora_fin, hora_real_inicio, firma_*_at / firma_*_url, cerrada_at, recordatorio_enviado,
    tecnicos (nombres), equipos (nombres), numero_ot, tipo_label, cliente. `ahora`: datetime de CHILE (aware o naive, ya convertido).
    `promesas`: {recordatorio, documento} = lo que el sistema de verdad va a mandar (los interruptores); sin ellas no se promete nada.
    Devuelve el estado del servicio para el cliente: situación, paso, pasos, progreso, tono, título, mensaje, qué sigue, firmas, fechas."""
    d = d or {}
    p = {"recordatorio": False, "documento": False}
    p.update(promesas or {})
    est = str(d.get("estado") or "").strip().lower()
    hoy = ahora.date() if isinstance(ahora, _dt.datetime) else ahora
    ahora_t = ahora.time() if isinstance(ahora, _dt.datetime) else None
    fecha = _otc.a_fecha(d.get("fecha"))
    cuando = cuando_es(fecha, hoy)
    horario = _otc.horario_txt(d.get("hora_inicio"), d.get("hora_fin"))
    dia = _otc.dia_txt(fecha)
    h_ini = _a_hora(d.get("hora_inicio"))
    firmas = [] if est in EST_CANCELADA else firmas_de(d)
    sin_cliente = bool(d.get("interna"))
    doc_listo = documento_listo(d)
    falta_firma_cliente = (not sin_cliente) and not (d.get("firma_cliente_at") or d.get("firma_cliente_url"))

    situacion, paso, tono, icono = SIT_CURSO, "agendada", "rojo", "calendario"
    titulo = mensaje = que_sigue = ""
    detalle = {}

    if est in EST_CANCELADA:
        situacion, paso, tono, icono = SIT_CANCELADA, None, "gris", "cancelada"
        titulo = "Esta visita quedó sin efecto" if est == "anulada" else "Esta visita fue cancelada"
        mensaje = "Si necesitas agendar de nuevo, crea un ticket de soporte y lo coordinamos contigo."
    elif est in EST_CERRADA:
        situacion, paso, tono, icono = SIT_TERMINADA, "documento", "verde", "documento"
        titulo = "Trabajo terminado"
        mensaje = ("Tu orden quedó cerrada y firmada. Puedes ver el documento abajo." if doc_listo
                   else "Tu orden quedó cerrada. El documento firmado estará disponible pronto.")
    elif est in EST_VISITA_HECHA:
        paso = "visita"
        if falta_firma_cliente:
            titulo, tono, icono = "El técnico terminó: falta tu firma", "ambar", "firma"
            mensaje = "Si recibiste el enlace de firma, firma desde tu celular. Si estás con el técnico, puedes firmar en su pantalla."
            que_sigue = "Con tu firma, nuestro equipo cierra la orden."
        else:
            titulo, tono, icono = "Trabajo realizado", "verde", "firma"
            mensaje = "Nuestro equipo revisa y cierra la orden."
            que_sigue = ("Te enviaremos el documento firmado por correo." if p["documento"]
                         else "El documento firmado quedará disponible en esta misma página.")
        detalle["visita"] = "Falta tu firma" if falta_firma_cliente else "Firmas listas"
    elif est in EST_EN_PAUSA:
        paso, tono, icono = "visita", "ambar", "pausa"
        titulo = "Tu servicio está en pausa"
        mensaje = ("Estamos gestionando un repuesto para continuar. Te avisaremos cuando retomemos." if est == "pendiente_repuesto"
                   else "Necesitamos confirmar un dato para continuar. Nuestro equipo te contactará.")
        detalle["visita"] = "En pausa"
    elif est in EST_EN_CURSO or (d.get("hora_real_inicio") and est in EST_AGENDADA):
        paso, icono = "visita", "tecnico"
        titulo = "Visita en curso"
        mensaje = "Nuestro técnico está trabajando en tus equipos."
        que_sigue = "Al terminar, te pedirá confirmar el trabajo con tu firma."
        detalle["visita"] = "En curso"
    else:
        # creada / programada / asignada / reagendada (y cualquier estado desconocido: se trata como agendada, sin inventar más)
        if cuando == "sin_fecha":
            titulo = "Visita por agendar"
            mensaje = "Te avisaremos cuando quede la fecha."
        elif cuando == "hoy" and h_ini and ahora_t and ahora_t >= h_ini:
            paso, icono = "visita", "tecnico"
            titulo = "Hoy es tu visita"
            mensaje = f"Nuestro técnico llega en el horario indicado ({horario})."
            que_sigue = "Al terminar, te pedirá confirmar el trabajo con tu firma."
            detalle["visita"] = "Hoy"
        elif cuando in ("hoy", "manana"):
            paso, icono = "recordatorio", "campana"
            titulo = "Tu visita es hoy" if cuando == "hoy" else "Tu visita es mañana"
            mensaje = f"Te esperamos {dia}, {horario}." if horario != "por confirmar" else f"Te esperamos {dia}; el horario está por confirmar."
            que_sigue = "Nuestro técnico llega en el horario indicado y, al terminar, te pedirá confirmar el trabajo con tu firma."
        elif cuando == "pasado":
            paso, tono, icono = "visita", "ambar", "alerta"
            titulo = "Visita pendiente de registro"
            mensaje = "La fecha ya pasó y el trabajo todavía no está registrado. Si tienes dudas, crea un ticket de soporte."
            detalle["visita"] = "Sin registrar"
        else:
            paso = "agendada"
            titulo = "Tu visita tiene nueva fecha" if est == "reagendada" else "Tu visita está agendada"
            mensaje = f"Te esperamos {dia}, {horario}." if horario != "por confirmar" else f"Te esperamos {dia}; el horario está por confirmar."
            que_sigue = ("Te enviaremos un recordatorio 24 horas antes de la visita." if p["recordatorio"]
                         else "Nuestro técnico llegará en el horario indicado.")

    # Detalles cortos de cada paso: lo que SÍ pasó. «24 h antes» solo se anuncia si el recordatorio de verdad va a salir y todavía no toca.
    detalle.setdefault("agendada", dia or "")
    detalle.setdefault("recordatorio", "Enviado" if d.get("recordatorio_enviado") else ("24 h antes" if (p["recordatorio"] and paso == "agendada") else ""))
    detalle.setdefault("visita", "Firmas listas" if doc_listo else "")
    detalle.setdefault("documento", "Listo" if doc_listo else "")

    pasos = []
    ia = CLAVES_PASOS.index(paso) if paso in CLAVES_PASOS else -1
    for i, (clave, nombre) in enumerate(PASOS):
        if ia < 0:
            e = "inactivo"
        elif i < ia or (i == ia and clave == CLAVES_PASOS[-1]):
            e = "hecho"
        elif i == ia:
            e = "actual"
        else:
            e = "pendiente"
        pasos.append({"clave": clave, "nombre": nombre, "estado": e, "detalle": detalle.get(clave) or ""})

    progreso = 0 if ia < 0 else PROGRESO[paso]
    if paso == "visita" and est in EST_VISITA_HECHA:
        progreso = 80
    linea = 0 if ia < 0 else int(round(ia * 100 / (len(PASOS) - 1)))      # hasta dónde llega la línea que une los círculos (0 · 33 · 67 · 100)
    tecnicos = _nombres(d.get("tecnicos"))
    equipos = _nombres(d.get("equipos"))
    n_eq = len(equipos)
    return {
        "situacion": situacion, "paso": paso, "pasos": pasos, "progreso": progreso, "linea": linea, "tono": tono, "icono": icono,
        "titulo": titulo, "mensaje": mensaje, "que_sigue": que_sigue,
        "fecha_txt": _otc.fecha_txt(fecha), "dia_txt": dia, "horario_txt": horario, "cuando": cuando,
        "tecnicos": tecnicos, "n_equipos": n_eq, "equipos": equipos,
        "equipos_txt": f"{n_eq} {_plural(n_eq, 'equipo', 'equipos')}" if n_eq else "—",
        "firmas": firmas, "documento_listo": doc_listo,
        "numero": str(d.get("numero_ot") or "").strip() or "OT", "tipo": str(d.get("tipo_label") or "Visita técnica").strip(),
        "cliente": str(d.get("cliente") or "").strip(),
    }


def a_json(ev, *, link_firmada="", link_soporte="", actualizado=""):
    """Lo que viaja al navegador: SOLO las claves públicas. `link_firmada` se incluye únicamente si el documento está listo."""
    out = {k: ev.get(k) for k in CLAVES_PUBLICAS if k in ev}
    out["link_firmada"] = link_firmada if ev.get("documento_listo") else ""
    out["link_soporte"] = link_soporte or ""
    out["actualizado"] = actualizado or ""
    return {k: out.get(k, "") for k in CLAVES_PUBLICAS}

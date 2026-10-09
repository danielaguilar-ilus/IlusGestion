# -*- coding: utf-8 -*-
"""ILUS Fitness · Retiros — DESVIACIONES de un retiro (funciones puras, sin BD ni Flask).

Daniel 2026-10-09: «si el cliente no viene… es necesario tomar también una opción de reagendar o cancelar el pedido en caso de que no venga.
Hay que tomar varias desviaciones que pueden pasar a lo largo de la operación. Decisiones humanas, incidencia, no pudo llegar».
Decisiones de Daniel (mismo día):
  · Cita vencida → se AVISA al responsable (campana + Monitor). Al cliente no le sale nada solo.
  · Correo al cliente → solo si el responsable lo marca (con el texto a la vista).
  · Motivo OBLIGATORIO, de una lista (+ detalle libre; con «Otro motivo» el detalle es obligatorio).
  · Primera entrega: no vino + cita vencida, reagendar y cancelar ordenados, incidencias sin cambio de estado, aviso a bodega.

Aquí vive el catálogo (tipos, motivos, acciones), la validación de lo que manda la pantalla y la regla de «cita vencida».
El módulo de rutas (pickups_module.py) es el que lee/escribe la BD y manda los avisos.
"""
from datetime import date, datetime, time, timedelta

# ── Catálogo ─────────────────────────────────────────────────────────────────────────────────────────────────────────────
TIPOS = {
    "no_vino": {"titulo": "El cliente no vino", "icono": "bi-person-x", "color": "rojo",
                "ayuda": "La cita pasó y el cliente no llegó a retirar."},
    "reagendar": {"titulo": "Reagendar", "icono": "bi-calendar2-week", "color": "ambar",
                  "ayuda": "Hay que mover la cita a otra fecha."},
    "cancelar": {"titulo": "Cancelar el retiro", "icono": "bi-x-octagon", "color": "rojo",
                 "ayuda": "El retiro no va a ocurrir."},
    "incidencia": {"titulo": "Registrar una incidencia", "icono": "bi-exclamation-triangle", "color": "azul",
                   "ayuda": "Algo salió distinto, pero el retiro sigue igual (no cambia el estado)."},
}

MOTIVOS = {
    "no_vino": [
        ("no_aviso", "No se presentó y no avisó"),
        ("aviso_no_puede", "Avisó que no podía venir"),
        ("fuera_horario", "Llegó fuera del horario de la bodega"),
        ("no_contesta", "No contesta el teléfono ni el correo"),
        ("otro", "Otro motivo"),
    ],
    "reagendar": [
        ("cliente_pidio", "El cliente pidió otra fecha"),
        ("no_vino", "No vino a su cita"),
        ("bodega_no_listo", "Bodega no alcanzó a preparar el pedido"),
        ("falta_stock", "Falta stock o un producto"),
        ("falta_documento", "Falta un documento o un pago"),
        ("otro", "Otro motivo"),
    ],
    "cancelar": [
        ("desistio", "El cliente desistió del retiro"),
        ("duplicado", "Solicitud duplicada"),
        ("error_solicitud", "Error en la solicitud"),
        ("ya_entregado", "Ya se entregó por otro medio o el documento no tiene saldo"),
        ("despacho", "Se enviará por despacho"),
        ("otro", "Otro motivo"),
    ],
    "incidencia": [
        ("otra_persona", "Retiró otra persona"),
        ("entrega_parcial", "Faltó un producto (entrega parcial)"),
        ("producto_danado", "Producto dañado"),
        ("llego_tarde", "El cliente llegó tarde"),
        ("problema_bodega", "Problema en bodega (Check, stock o ubicación)"),
        ("vehiculo", "El vehículo del cliente no alcanzaba"),
        ("otro", "Otro motivo"),
    ],
}

ACCIONES = {
    "esperar": {"titulo": "Esperar a que el cliente se comunique", "estado": None,
                "ayuda": "El retiro queda abierto; queda registrado que no vino."},
    "reagendar": {"titulo": "Proponer una fecha nueva", "estado": None,
                  "ayuda": "Se abre la agenda: al enviar la propuesta, el cliente recibe la fecha nueva para aceptarla."},
    "no_concretado": {"titulo": "Dejarlo como «No concretado»", "estado": "fallida",
                      "ayuda": "El retiro se cierra sin entrega y libera su cupo en la agenda."},
    "cancelar": {"titulo": "Cancelar el retiro", "estado": "rechazada",
                 "ayuda": "El retiro se cierra como cancelado y libera su cupo en la agenda."},
    "registrar": {"titulo": "Solo registrar", "estado": None,
                  "ayuda": "Queda en la bitácora como evidencia; el retiro sigue igual."},
}

ACCIONES_POR_TIPO = {
    "no_vino": ("reagendar", "esperar", "no_concretado"),
    "reagendar": ("reagendar",),
    "cancelar": ("cancelar",),
    "incidencia": ("registrar",),
}

# Estados en los que se puede registrar cada tipo (los terminados quedan en solo lectura, REGLA #23)
ESTADOS_ABIERTOS = ("solicitud_recibida", "en_revision", "informacion_incompleta", "esperando_cliente", "propuesta_enviada",
                    "agenda_confirmada", "reagendada", "en_preparacion")
ESTADOS_CON_CITA = ("agenda_confirmada", "en_preparacion")     # «reagendada» queda fuera: su fecha puede ser la vieja
TERMINADOS = ("retirada", "cerrada", "rechazada", "fallida")

# Correo al cliente (solo si el responsable lo marca): qué correo le llega con cada acción. Ninguno = no hay correo posible.
CORREO_POR_ACCION = {
    "no_concretado": ("failed", "«No pudimos concretar tu retiro»: le pedimos que nos escriba para coordinar una nueva fecha."),
    "cancelar": ("rejected", "«Tu retiro fue cancelado»: si fue un error, puede responder el correo."),
}

DETALLE_MAX = 800
DETALLE_MIN_OTRO = 5
MARGEN_VENCIDA_MIN = 30


def catalogo():
    """Lo que necesita la pantalla para armar el formulario (orden incluido)."""
    return {
        "tipos": [dict(clave=k, **v) for k, v in TIPOS.items()],
        "motivos": {t: [{"clave": c, "texto": x} for c, x in lista] for t, lista in MOTIVOS.items()},
        "acciones": {k: dict(clave=k, **v) for k, v in ACCIONES.items()},
        "acciones_por_tipo": {k: list(v) for k, v in ACCIONES_POR_TIPO.items()},
        "correo_por_accion": {k: {"kind": v[0], "texto": v[1]} for k, v in CORREO_POR_ACCION.items()},
        "detalle_max": DETALLE_MAX,
    }


def texto_motivo(tipo, motivo):
    for c, x in MOTIVOS.get(tipo, []):
        if c == motivo:
            return x
    return ""


def _limpio(v, maximo):
    s = " ".join(str(v or "").split())
    return s[:maximo]


def validar(datos, estado, tiene_cita=False, cita_vencida=False):
    """Revisa lo que manda la pantalla. Devuelve (normalizado, None) o (None, «mensaje para la persona»).
    «No vino» solo con la cita confirmada YA vencida (revisión adversarial 2026-10-09: con la cita por venir, «No pudimos concretar tu
    retiro» le llegaría al cliente antes de su fecha).
    normalizado = {tipo, motivo, motivo_texto, detalle, accion, estado_nuevo, avisar_cliente, avisar_bodega}."""
    d = datos if isinstance(datos, dict) else {}
    tipo = str(d.get("tipo") or "").strip()
    if tipo not in TIPOS:
        return None, "Elige qué pasó."
    if estado in TERMINADOS:
        return None, "El retiro ya está terminado: queda en solo lectura. Para corregir algo, reábrelo desde «Cambiar estado»."
    if estado not in ESTADOS_ABIERTOS:
        return None, "Este retiro no admite cambios ahora."
    if tipo == "no_vino" and (estado not in ESTADOS_CON_CITA or not tiene_cita):
        return None, "«No vino» se registra en un retiro con la cita confirmada."
    if tipo == "no_vino" and not cita_vencida:
        return None, "«No vino» se registra cuando la cita ya pasó. Si el cliente avisó que no vendrá, usa «Reagendar» o «Cancelar»."
    motivo = str(d.get("motivo") or "").strip()
    texto = texto_motivo(tipo, motivo)
    if not texto:
        return None, "Elige el motivo de la lista."
    detalle = _limpio(d.get("detalle"), DETALLE_MAX)
    if motivo == "otro" and len(detalle) < DETALLE_MIN_OTRO:
        return None, "Con «Otro motivo», escribe en el detalle qué pasó."
    accion = str(d.get("accion") or "").strip() or (ACCIONES_POR_TIPO[tipo][0] if len(ACCIONES_POR_TIPO[tipo]) == 1 else "")
    if accion not in ACCIONES_POR_TIPO[tipo]:
        return None, "Elige qué hacer con el retiro."
    avisar_cliente = bool(d.get("avisar_cliente")) and accion in CORREO_POR_ACCION
    return {
        "tipo": tipo, "motivo": motivo, "motivo_texto": texto, "detalle": detalle, "accion": accion,
        "estado_nuevo": ACCIONES[accion]["estado"], "avisar_cliente": avisar_cliente,
        "avisar_bodega": bool(d.get("avisar_bodega")),
    }, None


def _a_fecha(v):
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    try:
        return datetime.strptime(str(v or "")[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def _a_hora(v):
    """TIME de MySQL (timedelta), time, «HH:MM[:SS]» → time, o None."""
    if isinstance(v, time):
        return v
    if isinstance(v, timedelta):
        s = int(v.total_seconds()) % 86400
        return time(s // 3600, (s % 3600) // 60)
    try:
        p = str(v or "").strip().split(":")
        return time(int(p[0]), int(p[1])) if len(p) >= 2 else None
    except (ValueError, IndexError):
        return None


def fin_de_cita(req):
    """Momento (hora Chile, sin zona) en que termina la cita confirmada: fecha + hora de término (o de inicio + 30 min; sin hora, fin del día)."""
    f = _a_fecha((req or {}).get("confirmed_date"))
    if not f:
        return None
    hasta = _a_hora(req.get("confirmed_time_to"))
    desde = _a_hora(req.get("confirmed_time_from"))
    if hasta and (hasta == time(0, 0) or (desde and hasta <= desde)):
        hasta = None                                   # 00:00 o un término antes del inicio: dato raro, se usa el inicio
    if hasta:
        return datetime.combine(f, hasta)
    if desde:
        return datetime.combine(f, desde) + timedelta(minutes=30)
    return datetime.combine(f, time(23, 59))


def cita_vencida(req, ahora, margen_min=MARGEN_VENCIDA_MIN):
    """¿La cita confirmada ya pasó (más el margen) y el retiro sigue esperando al cliente? `ahora` en hora Chile (con o sin zona)."""
    if (req or {}).get("status") not in ESTADOS_CON_CITA:
        return False
    fin = fin_de_cita(req)
    if not fin or ahora is None:
        return False
    return ahora.replace(tzinfo=None) >= fin + timedelta(minutes=margen_min)


def texto_bitacora(n, quien=""):
    """Una línea clara para la bitácora: «No vino · No se presentó y no avisó · Acción: Esperar… · Detalle: … · Aviso al cliente: no»."""
    partes = [TIPOS[n["tipo"]]["titulo"], n["motivo_texto"], "Acción: " + ACCIONES[n["accion"]]["titulo"]]
    if n.get("detalle"):
        partes.append("Detalle: " + n["detalle"])
    if n["accion"] in CORREO_POR_ACCION:
        partes.append("Aviso al cliente: " + ("sí" if n.get("avisar_cliente") else "no"))
    partes.append("Aviso a bodega: " + ("sí" if n.get("avisar_bodega") else "no"))
    if quien:
        partes.append("Decidió: " + quien)
    return " · ".join(partes)[:900]

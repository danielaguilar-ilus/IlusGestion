# -*- coding: utf-8 -*-
"""NINGÚN CORREO AL CLIENTE DICE «RESPONDE ESTE CORREO»: todo va por TICKET (Daniel, 6-nov-2026).

Daniel (con la captura de un recordatorio): «nunca digas que responden al correo, solo por ticket. Nunca se responde al correo, nunca. Nadie va a responder si
responden… Hay que reparar eso de manera urgente: elimina responder este correo y coloca un botón para que se vayan a registrar un ticket.»

El botón es el pie de TODO correo de ILUS (`_ilus_email_master`: «Crear ticket de soporte»); estas frases solo mandan hacia él: «crea un ticket de soporte con el
botón de abajo».

Módulo PURO (sin Flask ni base de datos). Lo usa el arranque (`_ensure_correos_sin_responder` en app.py) para cambiar SOLO esas frases dentro de las plantillas que
ya están guardadas en la base (Comunicaciones → Plantillas), sin tocar nada más del texto: una plantilla que alguien editó conserva sus cambios, y si esa frase
no está tal cual no se toca. Se compara por FRASE exacta, nunca por el cuerpo entero: la fila de la base puede ser de una versión más vieja que la semilla del
código (lección de la «OT completada», 6-nov).

NO se tocan, a propósito:
  · el módulo `tickets`: ahí el lector IMAP SÍ procesa las respuestas y las agrega al ticket (el canal real es responder el correo);
  · `comunicacion_interna` (correos entre el equipo) y WhatsApp/SMS (otro canal);
  · los correos al PROVEEDOR (facturas de proveedor: ahí responder es la vía).
"""
import re

# Frase que reemplaza a «responde este correo» (corta a propósito: Daniel, «no me gusta tanta letra»).
COMO_CREAR_TICKET = "crea un ticket de soporte con el botón de abajo"

# Módulos cuyas plantillas NO se reescriben (ver arriba).
MODULOS_EXCLUIDOS = ("tickets", "comunicacion_interna")

# (frase exacta que está hoy, frase nueva). El orden importa: lo más largo primero (una frase no contiene a la siguiente).
REEMPLAZOS = (
    # ── Retiros ──
    ("¿Necesitas reagendar o tienes una duda? Responde este correo y te ayudamos.",
     "¿Necesitas reagendar o tienes una duda? Crea un ticket de soporte con el botón de abajo y te ayudamos."),
    ("¿Necesitas asesoría o servicio post-venta? Responde este correo y te ayudamos.",
     "¿Necesitas asesoría o servicio post-venta? Crea un ticket de soporte con el botón de abajo y te ayudamos."),
    ("También puedes responder este correo con cualquier dato que te haya faltado.",
     "Si quieres agregar algún dato, crea un ticket de soporte con el botón de abajo."),
    ("también puedes responder este correo y te ayudamos a la brevedad.",
     "también puedes crear un ticket de soporte con el botón de abajo y te ayudamos."),
    ("Si fue un error o quieres reagendar, escríbenos respondiendo este correo y te ayudamos a la brevedad.",
     "Si fue un error o quieres reagendar, crea un ticket de soporte con el botón de abajo y te ayudamos."),
    ("Si fue un error, escríbenos respondiendo este correo",
     "Si fue un error, crea un ticket de soporte con el botón de abajo"),
    ("¿Imprevistos? Responde este correo o llámanos. Es mejor avisar a no llegar.",
     "¿Imprevistos? Avísanos con un ticket de soporte (botón de abajo). Es mejor avisar a no llegar."),
    # ── Mantenciones: plan propuesto (trato formal) ──
    ("Para confirmar o ajustar fechas, responda este correo y coordinamos la agenda con su ejecutivo.",
     "Para confirmar o ajustar fechas, cree un ticket de soporte con el botón de abajo y coordinamos la agenda con su ejecutivo."),
    # ── Mantenciones: aviso de agenda al TÉCNICO (él no crea tickets: avisa a su coordinador) ──
    ("Si algo no calza (dirección, horario o equipo), responde este correo antes de salir.",
     "Si algo no calza (dirección, horario o equipo), avisa a tu coordinador de Servicio Técnico antes de salir."),
    # ── Mantenciones y Prospectos: ofertas del plan de mantención ──
    ("Responde este correo o escríbenos y te contactamos para armar un plan a la medida de tu centro.",
     "Crea un ticket de soporte con el botón de abajo y te contactamos para armar un plan a la medida de tu centro."),
    ("Responde este correo y armamos el contrato con tus condiciones.",
     "Crea un ticket de soporte con el botón de abajo y armamos el contrato con tus condiciones."),
    ("También puedes responder este correo; tu respuesta llega directo a tu ejecutivo.",
     "¿Prefieres hablar con tu ejecutivo? Crea un ticket de soporte con el botón de abajo."),
    ("¿Dudas? Responde este correo y te contactamos.",
     "¿Dudas? Crea un ticket de soporte con el botón de abajo y te contactamos."),
    # ── Mantenciones: «OT creada», «Enviar email» (versiones originales y la de ayer, por si alguna fila quedó con ellas) ──
    ("Si necesitas reagendar o cancelar, responde este correo o contacta a tu ejecutivo.",
     "¿Necesitas reagendar o cancelar? Crea un ticket de soporte con el botón de abajo."),
    ("Si tienes alguna observación, responde este correo y te contactaremos a la brevedad.",
     "Si tienes alguna observación, crea un ticket de soporte con el botón de abajo."),
)

# «OT OT-2026-00123»: el número de la OT ya trae el prefijo «OT-», y varios asuntos guardados escribían «OT {{ot}}» (Daniel, 6-nov: «sigue diciendo OT OT, ¿por qué?»).
_RX_OT_DOBLE = re.compile(r"\bOT\s+(\{\{\s*ot\s*\}\})")


def asunto_sin_ot_doble(asunto):
    """(asunto_nuevo, cambió). Quita el «OT » que antecede a la variable {{ot}}: «OT {{ot}} terminada» → «{{ot}} terminada»."""
    if not asunto:
        return asunto, False
    nuevo = _RX_OT_DOBLE.sub(r"\1", asunto)
    return nuevo, nuevo != asunto


# Cualquier forma de «responde(r) este correo» que quede en un correo al cliente (para las pruebas y para el aviso del arranque).
_RX_RESPONDER = re.compile(
    r"respond[ae]\w*\s+(?:a\s+|en\s+)?(?:este|el|tu|ese)\s+(?:correo|mail|email)|escr[ií]benos\s+respondiendo|respuesta\s+a\s+este\s+correo",
    re.IGNORECASE)


def aplicar(texto):
    """(texto_nuevo, cuántas_frases_cambió). Cambia SOLO las frases de REEMPLAZOS que estén tal cual; el resto del texto no se toca. Idempotente."""
    if not texto:
        return texto, 0
    nuevo, n = texto, 0
    for viejo, nueva in REEMPLAZOS:
        if viejo in nuevo:
            nuevo = nuevo.replace(viejo, nueva)
            n += 1
    return nuevo, n


def dice_responder(texto):
    """True si el texto todavía le pide al destinatario que responda el correo (después de quitar las etiquetas HTML)."""
    plano = re.sub(r"<[^>]*>", " ", texto or "")
    return bool(_RX_RESPONDER.search(plano))

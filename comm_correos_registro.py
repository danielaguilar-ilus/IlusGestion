# -*- coding: utf-8 -*-
"""REGISTRO ÚNICO DE LOS CORREOS DE ILUS FITNESS (Comunicaciones → «Control de correos», Daniel, 20-oct-2026).

Daniel: «el control de correos en Comunicaciones: una pestaña donde veo TODOS los correos que el sistema envía, puedo
APAGAR cada uno (con motivo), ver su estado real, la vista previa, mandarme una prueba y ver cuántos salieron o se
frenaron». Y: «tal vez separar las órdenes de trabajo de servicio técnico en general».

Este módulo es PURO (sin base de datos, sin Flask, sin correo): la lista de TODOS los correos con su clave, su nombre,
a quién le llegan, qué los dispara, qué llave de paso los gobierna y qué variables de entorno los condicionan. Lo usan:
  · `app.py` → la puerta única de envío (`_send_ilus_email`) resuelve la clave de cada correo y consulta su interruptor;
  · la pantalla «Control de correos» → arma las tarjetas con `estado_efectivo()`;
  · las pruebas (`tests/test_comm_control_correos.py`) → exigen que TODA llamada de envío del proyecto lleve una clave de
    este registro o figure en una lista de excepciones con su razón.

REGLA #33 (CLAUDE.md): un correo nuevo NACE aquí, con su clave, y sale por la puerta única. Sin fila en la tabla
`comm_correo_control` un correo está ENCENDIDO: el día que se sube esto no cambia nada (lo que hoy sale sigue saliendo y lo
que hoy está apagado por código, variable o llave sigue apagado).

El interruptor de la pantalla SOLO puede APAGAR. Un correo a clientes que hoy está apagado por una variable de entorno
(RETIROS_FIRMA_CORREO, MANT_RECORDATORIOS_EMAIL_ENABLED, PROSPECTOS_OFERTA_AUTO…) se muestra «Apagado por configuración» y
no se puede encender desde la pantalla: encenderlo sigue pasando por la variable y por el «sí» explícito de Daniel (REGLAS #22
y #23).

CÓMO AGREGAR UN CORREO NUEVO (3 pasos; la prueba de cobertura te dice cuál falta):
  1. Agrega UNA llamada `_c(...)` a `REGISTRO`, con su clave (`ot_terminada_cliente`, `ot_hallazgo_responsable`…), su nombre
     completo, el grupo de la pantalla (`modulo_ui`), la llave de paso que ya lo gobierna (`llave` = el `modulo=` con que sale),
     QUIÉN lo recibe (`destinatario`: cliente · tecnico · responsable_ot · bodega · proveedor_courier · interno · usuario),
     qué lo dispara y su plantilla (modulo, estado) si el texto es editable en Comunicaciones.
  2. En el envío, pásale la clave a la puerta única: `_send_ilus_email(..., modulo="mantenciones", correo="ot_terminada_cliente")`
     (o `correo=` en la función que lo envuelve). NUNCA dentro de `**kwargs` ni de `kwargs_json` de la cola.
     Si el envío vive en otra rama que todavía no pasa `correo=`, basta sumar su evento (constante) a `_EVENTO_A_CLAVE`: la puerta
     resuelve la clave sola y el interruptor ya funciona. Aun así, al mezclar conviene agregar `correo="clave"` en el envío.
  3. Si lo condiciona una variable de entorno, decláralo en `condiciones` (`_cond(...)`): la pantalla lo muestra como
     «Apagado por configuración» y no deja encenderlo.
  4. Si el correo NO lo envía ILUS sino un tercero (FedEx, SimpliRoute), regístralo con `tipo="externo"`: ILUS solo decide si le
     ENTREGA el correo del destinatario. Se consulta con `_correo_externo_bloqueo(clave)` justo donde ILUS arma lo que le manda; obedece
     al interruptor general y al suyo, no a la llave del módulo (REGLA #33).
Un grupo o un destinatario nuevos se suman en `MODULOS_UI` / `DESTINATARIOS` (y su orden). Nada más: la pantalla, el interruptor, la
vista previa y las cifras salen solas del registro.
"""
import os
import re

# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  Catálogos de la pantalla
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════
# Grupos de la pantalla (campo `modulo_ui`). NO son llaves de paso: la llave que gobierna cada correo es `llave` (la misma
# que ya se pasaba en `modulo=`); el interruptor por correo da el control fino.
MODULOS_UI = (
    {"clave": "ot", "nombre": "Órdenes de trabajo", "icono": "bi-clipboard-check",
     "descripcion": "Lo que una orden de trabajo avisa a clientes, técnicos, proveedores externos y al equipo."},
    {"clave": "tickets", "nombre": "Tickets e incidencias", "icono": "bi-ticket-perforated",
     "descripcion": "El ciclo de vida de un ticket, las respuestas al cliente y los avisos internos de Tickets."},
    {"clave": "mantenciones", "nombre": "Contratos y mantención programada", "icono": "bi-calendar2-week",
     "descripcion": "Planes anuales, recordatorios de visita y avisos desde la ficha del cliente."},
    {"clave": "retiros", "nombre": "Retiros", "icono": "bi-box-arrow-up",
     "descripcion": "Cada paso del retiro en bodega para el cliente, más los avisos internos a bodega y al equipo."},
    {"clave": "transporte", "nombre": "Transporte", "icono": "bi-truck",
     "descripcion": "Seguimiento del despacho al cliente y avisos a los couriers."},
    {"clave": "cotizaciones", "nombre": "Cotizaciones", "icono": "bi-file-earmark-text",
     "descripcion": "Cotizaciones de transporte y de tickets que se envían al cliente en PDF."},
    {"clave": "prospectos", "nombre": "Prospectos de mantención", "icono": "bi-megaphone",
     "descripcion": "Ofertas del plan de mantención a clientes de instalación (pasos 1 a 4 y campaña automática)."},
    {"clave": "repuestos", "nombre": "Repuestos y compras", "icono": "bi-gear-wide-connected",
     "descripcion": "Solicitudes de compra de repuestos a proveedores y avisos internos de los pedidos de repuesto de las OT."},
    {"clave": "catalogo", "nombre": "Catálogo", "icono": "bi-box-seam",
     "descripcion": "Manuales de producto que se envían por correo."},
    {"clave": "accesos", "nombre": "Accesos y usuarios", "icono": "bi-shield-lock",
     "descripcion": "Invitaciones, cambios y recuperación de contraseña de los usuarios del sistema."},
    # Solo para las dos herramientas de Comunicaciones (no son tarjetas: no se apagan por interruptor).
    {"clave": "comunicaciones", "nombre": "Herramientas de Comunicaciones", "icono": "bi-tools",
     "descripcion": "«Enviar prueba» y «Reintentar» del Historial: no se apagan aquí."},
)
MODULOS_UI_CLAVES = tuple(m["clave"] for m in MODULOS_UI)

# QUIÉN recibe el correo (Daniel, 20-oct: «que cada correo quede identificado por quién lo recibe»). La pantalla agrupa por
# módulo y, dentro de cada módulo, por este valor. Para sumar un destinatario nuevo basta agregarlo aquí y en el orden.
DESTINATARIOS = {
    "cliente": "Cliente",
    "tecnico": "Técnico",
    "responsable_ot": "Responsable de la OT",
    "bodega": "Bodega",
    "proveedor_courier": "Proveedor o courier",
    "interno": "Equipo ILUS (interno)",
    "usuario": "Usuario del sistema",
}
DESTINATARIOS_ORDEN = ("cliente", "tecnico", "responsable_ot", "bodega", "proveedor_courier", "interno", "usuario")

MODOS = {"automatico": "Automático", "manual": "Manual", "mixto": "Automático y manual"}
TIPOS = ("ilus", "externo", "herramienta")      # ilus = lo envía ILUS · externo = lo envía FedEx/SimpliRoute · herramienta = no se apaga

# Texto corto de cada llave de paso (el mismo vocabulario de la pestaña Plantillas).
LLAVES_TXT = {
    "transporte": "Transporte", "retiros": "Retiros", "mantenciones": "Servicio Técnico",
    "comunicacion_interna": "Comunicación interna", "general": "General", "tickets": "Tickets", "catalogo": "Catálogo",
    "ot_tecnicos": "Correos a técnicos (OT)",
}

# Qué pasa con el correo si su plantilla se APAGA en el editor (comm_templates.activo=0):
PLANTILLA_APAGADA = {
    "texto_codigo": "Si la plantilla se apaga en el editor, el correo sale igual con el texto que trae el código.",
    "no_sale": "Si la plantilla se apaga en el editor, este correo NO sale.",
}

# Variables de mentira para la vista previa (se suman a `_comm_demo_replacements()` de app.py). Un {{token}} que no esté
# aquí ni allá se muestra como «[token demo]»: la vista previa nunca deja llaves sueltas ni lee datos reales.
DEMO_EXTRA = {
    "fecha_retiro": "21/10/2026", "mensaje_propuesta": "", "nota_cierre": "", "encuesta": "", "horario": "10:00 - 12:00",
    "numero_ticket": "TK-2026-00123", "extracto": ": Trotadora que no enciende", "destinatario": "Pedro Pérez",
    "mensaje": "Este es un texto de ejemplo del mensaje.", "numero_cotizacion": "COT-2026-0045",
    "empresa": "Gimnasio Demo SpA", "total": "$1.250.000", "valida_hasta": ", válida hasta el 30/11/2026",
    "sku": "ILUS-TRT-9000", "producto": "Trotadora ILUS Pro 9000", "manual_nombre": "manual_trotadora.pdf",
    "cliente_nombre": "Gimnasio Demo SpA", "contacto_nombre": "María", "fecha_instalacion": "15/07/2026",
    "numero_cotizacion_plan": "COT-2026-0046", "total_cotizacion": "$980.000", "visitas_anio": "4", "descuento_txt": " con 10% de descuento",
    "equipos_html": "<ul><li>Trotadora ILUS Pro 9000</li><li>Bicicleta estática ILUS</li></ul>",
    "escalera_html": "", "incluye_html": "<ul><li>Revisión completa</li><li>Repuestos de desgaste</li></ul>",
    "botones_html": "<p>[Sí, quiero la cotización] [Llámenme] [No por ahora]</p>",
    "boton_propuesta_html": "<p>[Ver y aceptar mi cotización]</p>",
    "mensaje_extra": "", "link_agenda": "https://ilusfitness.com/ot/",
    # OT terminada y «Enviar OT por correo» (REGLA #37): variables de las cuatro plantillas nuevas.
    "saludo": "Hola María,", "nombre_destinatario": "María", "nombre_contacto": "María", "fecha_realizada": "21/10/2026",
    "link_ot_firmada": "https://ilusfitness.com/ot-firmada/demo", "mensaje_html": "", "aprobado_por": "Aarón Rojas",
    "cerrada_en": "21/10/2026 11:30", "resumen_envio_html": "", "dias_vigencia": "30", "enviado_por": "Aarón Rojas",
}

# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  Condiciones de configuración (variables de entorno que hoy ya condicionan un correo)
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════
# Cada lector replica EXACTAMENTE cómo lee su variable el código que la usa (por eso hay uno por semántica distinta).
# Devuelve True si la condición PERMITE el correo.
def _lee_exacto_1(v):
    return (v or "") == "1"                                                        # RETIROS_FIRMA_CORREO == "1"


def _lee_verdadero_mant(v):
    return ((v if v is not None else "0") or "0").strip().lower() in ("1", "true", "yes", "on")   # MANT_RECORDATORIOS_EMAIL_ENABLED


def _lee_verdadero_prosp(v):
    return ((v if v is not None else "0") or "0").strip().lower() in ("1", "true", "si", "yes")   # PROSPECTOS_OFERTA_AUTO


def _lee_no_apagado(v):
    return ((v or "1").strip().lower()) not in ("0", "false", "no", "off")        # RETIROS_PREP_AUTO / RETIROS_CHECK_AUTO


def _lee_modo_activo(v):
    return ((v or "sombra").strip().lower()) in ("activo", "activa")               # RETIROS_RETIRO_AUTO = activo


def _lee_verdadero_amplio(v):
    return ((v or "").strip().lower()) in ("1", "true", "yes", "on", "si", "sí")  # RETIROS_ENCUESTA_ACTIVA


def _lee_vacio(v):
    return not (v or "").strip()                                                   # TK_TEST_EMAIL_TO: vacío = sin modo prueba


LECTORES = {
    "exacto_1": _lee_exacto_1, "verdadero_mant": _lee_verdadero_mant, "verdadero_prosp": _lee_verdadero_prosp,
    "no_apagado": _lee_no_apagado, "modo_activo": _lee_modo_activo, "verdadero_amplio": _lee_verdadero_amplio,
    "vacio": _lee_vacio,
}

# alcance: «todo»  → si la condición NO se cumple, el correo completo está apagado por configuración (no se puede encender
#                    desde la pantalla);
#          «auto»   → solo condiciona el disparo AUTOMÁTICO (el manual sigue funcionando);
#          «extra»  → condiciona una parte del correo (p. ej. el botón de la encuesta), no el correo;
#          «info»   → se muestra como dato (modo prueba, pausas): no apaga nada por sí misma.
ALCANCES = ("todo", "auto", "extra", "info")


def _cond(variable, lector, alcance, texto, texto_apagada=""):
    return {"variable": variable, "lector": lector, "alcance": alcance, "texto": texto, "texto_apagada": texto_apagada}


def evaluar_condicion(cond, environ=None):
    """True si la condición se cumple (permite), con las variables de `environ` (por defecto, las del proceso)."""
    env = os.environ if environ is None else environ
    lector = LECTORES.get(cond.get("lector"))
    if lector is None or not cond.get("variable"):
        return True
    return bool(lector(env.get(cond["variable"])))


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  El registro
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════
def _c(clave, nombre, modulo_ui, llave, destinatario, disparo, *, modo="manual", plantilla=None, tipo="ilus",
       protegido=False, sensible=False, condiciones=(), apagado_en_codigo=False, tambien_whatsapp=False,
       plantilla_apagada="texto_codigo", demo=None, avisa_a="", adjunta="", nota="", nace_apagado=False):
    # nace_apagado (Daniel, 26-oct-2026: «agrégalos apagados para encenderlos desde el front»): sin fila en comm_correo_control
    # el correo está APAGADO; una persona lo enciende (o lo vuelve a apagar) desde la pantalla. Solo para avisos INTERNOS: a un
    # cliente nada se enciende desde la pantalla (REGLA #22).
    return {
        "clave": clave, "nombre": nombre, "modulo_ui": modulo_ui, "llave": llave, "destinatario": destinatario,
        "disparo": disparo, "modo": modo, "plantilla": plantilla, "tipo": tipo, "protegido": protegido,
        "sensible": sensible, "condiciones": tuple(condiciones), "apagado_en_codigo": apagado_en_codigo,
        "tambien_whatsapp": tambien_whatsapp, "plantilla_apagada": plantilla_apagada, "demo": dict(demo or {}),
        "avisa_a": avisa_a, "adjunta": adjunta, "nota": nota, "nace_apagado": bool(nace_apagado),
    }


# Muestra del bloque «Qué se envió» del aviso al equipo (OT terminada): solo para la vista previa, con datos de mentira.
_DEMO_RESUMEN_ENVIO = (
    '<table cellpadding="0" cellspacing="0" width="100%" style="background:#f5f5f7;border-left:4px solid #6b7280;border-radius:4px;'
    'padding:12px 18px;margin:18px 0"><tr><td style="font-size:11px;font-weight:800;letter-spacing:.1em;color:#6b7280;'
    'padding-bottom:4px">QUÉ SE ENVIÓ</td></tr>'
    '<tr><td style="padding:5px 0;font-size:13px;color:#555"><strong style="color:#222">Cliente:</strong>&nbsp; '
    '<span style="color:#166534;font-weight:600">se le envió el documento firmado a contacto@gimnasiodemo.cl</span></td></tr>'
    '<tr><td style="padding:5px 0;font-size:13px;color:#555"><strong style="color:#222">Técnico:</strong>&nbsp; '
    '<span style="color:#166534;font-weight:600">recibió su copia (sin montos): Pedro Pérez</span></td></tr></table>')

_TK_TEST = _cond("TK_TEST_EMAIL_TO", "vacio", "info",
                 "Modo prueba de Tickets apagado: los correos salen a sus destinatarios reales.",
                 "Modo prueba de Tickets encendido (TK_TEST_EMAIL_TO): estos correos se redirigen a una casilla de pruebas.")

_TK_PAUSA = "No sale si el ticket tiene las notificaciones pausadas (tickets importados sin asignar)."

# Los correos internos de Repuestos nacen APAGADOS (2026-10-21): los enciende Daniel con su «sí» (misma lógica de OT_TERMINADA_CLIENTE_ACTIVA).
_REP_CORREOS = _cond("REPUESTOS_CORREOS_ACTIVOS", "verdadero_amplio", "todo",
                     "Los correos internos de Repuestos están encendidos (REPUESTOS_CORREOS_ACTIVOS).",
                     "Apagado por configuración: REPUESTOS_CORREOS_ACTIVOS no está encendida. Encenderla requiere la autorización explícita de "
                     "Daniel (REGLAS #22 y #33). Mientras tanto el aviso llega solo por la campana.")

REGISTRO = (
    # ─────────────────────────────── ÓRDENES DE TRABAJO ───────────────────────────────
    _c("ot_creada_cliente", "OT creada: aviso de la visita agendada al cliente", "ot", "mantenciones", "cliente",
       "Al crear una OT de cliente (asistente «Nueva OT», ficha del cliente o ticket). No sale en trabajos internos.",
       modo="automatico", plantilla=("mantenciones", "visita_agendada"),
       avisa_a="Al contacto de la OT y a los correos de la ficha del cliente"),
    _c("ot_visita_agendada_manual", "Visita agendada: envío manual al cliente", "ot", "mantenciones", "cliente",
       "Botón «Enviar email» de la ficha de la OT (lo manda una persona).",
       plantilla=("mantenciones", "visita_agendada"), avisa_a="Al cliente (o al destinatario que se escriba)"),
    _c("ot_tec_visita_copia", "Copia de la visita agendada para los técnicos (sin montos)", "ot", "ot_tecnicos", "tecnico",
       "Va junto al botón «Enviar email» de la ficha de la OT: la misma información, sin costo ni repuestos.",
       plantilla=("mantenciones", "visita_agendada"), avisa_a="A los técnicos de la visita",
       nota="Usa el texto de «Visita agendada» (la misma plantilla del correo al cliente)."),
    _c("ot_tec_asignada", "OT asignada: aviso al técnico", "ot", "ot_tecnicos", "tecnico",
       "Cuando se asigna una OT a un técnico (nace con él o lo reasignan). Lleva el archivo de calendario (.ics).",
       modo="automatico", plantilla=("mantenciones", "ot_asignada"), plantilla_apagada="no_sale",
       adjunta="Archivo de calendario (.ics)", avisa_a="A cada técnico de la OT"),
    _c("ot_tec_reagendada", "OT reagendada: aviso al técnico", "ot", "ot_tecnicos", "tecnico",
       "Cuando cambia la fecha u horario de una OT del técnico (muestra ANTES → AHORA). Lleva el .ics.",
       modo="automatico", plantilla=("mantenciones", "ot_reagendada_tecnico"), plantilla_apagada="no_sale",
       adjunta="Archivo de calendario (.ics)", avisa_a="A cada técnico de la OT"),
    _c("ot_tec_cancelada", "OT cancelada: aviso al técnico", "ot", "ot_tecnicos", "tecnico",
       "Cuando se cancela o anula una OT del técnico: ya no necesita ir. Lleva el .ics de cancelación.",
       modo="automatico", plantilla=("mantenciones", "ot_cancelada_tecnico"), plantilla_apagada="no_sale",
       adjunta="Archivo de calendario (.ics)", avisa_a="A cada técnico de la OT"),
    # 2026-10-20 (Daniel: «no digas "te quité la OT"… algo más sutil, se sentiría como un castigo»): el nombre y el texto hablan de un
    # ajuste de la agenda, nunca de quitar. La clave y la plantilla siguen siendo las mismas.
    _c("ot_tec_desasignada", "OT pasa a otra persona: aviso de ajuste de agenda al técnico anterior", "ot", "ot_tecnicos", "tecnico",
       "Cuando la OT pasa a otra persona o se reorganiza el equipo: un aviso amable de ajuste de agenda, sin reproche.",
       modo="automatico", plantilla=("mantenciones", "ot_desasignada_tecnico"), plantilla_apagada="no_sale",
       adjunta="Archivo de calendario (.ics)", avisa_a="Al técnico que deja de atenderla"),
    _c("ot_tec_agenda_manana", "Agenda del técnico para el siguiente día hábil", "ot", "ot_tecnicos", "tecnico",
       "Cada día hábil entre las 17:00 y las 21:00 (hora Chile), con las OT del técnico del siguiente día hábil. "
       "Nunca sale en sábado, domingo ni feriado.",
       modo="automatico", plantilla=("mantenciones", "agenda_tecnico_manana"), plantilla_apagada="no_sale",
       avisa_a="A cada técnico con OT ese día"),
    _c("ot_informe_servicio", "Informe de servicio de la OT (con Word adjunto)", "ot", "mantenciones", "cliente",
       "Botón «Enviar» de un informe de servicio de la ficha del cliente.",
       plantilla=("mantenciones", "ot_completada"), adjunta="Informe en Word", avisa_a="A los destinatarios que se elijan"),
    _c("ot_firma_remota", "Enlace para que el cliente firme la OT", "ot", "mantenciones", "cliente",
       "Botón «Enviar link de firma» de la OT (el cliente firma desde su celular).", avisa_a="Al correo que se indique"),
    _c("ot_pendiente_aprobacion", "OT por aprobar: aviso al equipo de gestión", "ot", "mantenciones", "interno",
       "Cuando firma el técnico o el cliente: la OT queda esperando aprobación.",
       modo="automatico", sensible=True, avisa_a="Al creador de la OT y a los usuarios de gestión activos",
       nota="La bandeja de aprobaciones sigue funcionando: solo deja de llegar el correo."),
    _c("ot_cliente_firmo_tecnico", "El cliente firmó conforme: aviso al técnico", "ot", "mantenciones", "tecnico",
       "Cuando el cliente firma la OT: avisa a los técnicos asignados.", modo="automatico",
       avisa_a="A los técnicos de la OT",
       nota="Hoy el técnico con cuenta puede recibir este aviso dos veces (dos rutas del código lo envían)."),
    _c("ot_autorizacion_pendiente", "Autorización pendiente: aviso a los superadministradores", "ot", "comunicacion_interna",
       "interno", "Cuando alguien pide una autorización (documento, $0, exceso de saldo…).", modo="automatico",
       sensible=True, avisa_a="A los superadministradores activos",
       nota="La bandeja de autorizaciones sigue funcionando: solo deja de llegar el correo. Sale por la llave de Comunicación interna "
            "(2026-10-23: antes salía por la de Servicio Técnico, que estaba cerrada «por pruebas», y el correo no llegaba)."),
    _c("ot_anexo_auto", "Anexo de servicios al crear una OT con técnico externo", "ot", "general", "proveedor_courier",
       "Al crear una OT con técnico externo: le llega el enlace para firmar el Anexo de Servicios.", modo="automatico",
       avisa_a="Al proveedor externo asignado", nota="Sale por la llave «General» (el código no indica módulo)."),
    _c("ot_anexo_envio", "Anexo de servicios: envío al proveedor", "ot", "general", "proveedor_courier",
       "Botón «Enviar» del Anexo de Servicios de una OT.", plantilla=("mantenciones", "anexo_nueva_ot"),
       adjunta="Anexo en PDF (opcional)", avisa_a="Al correo del proveedor",
       nota="Sale por la llave «General» (el código no indica módulo)."),
    _c("ot_anexo_firmado_aviso", "Aviso: el proveedor firmó el anexo", "ot", "general", "interno",
       "Cuando el proveedor firma el anexo desde su enlace.", modo="automatico",
       avisa_a="A quien envió el anexo (o al buzón de soporte)", nota="Sale por la llave «General»."),
    _c("ot_acta_recepcion", "Acta de recepción a logística (PDF y Excel)", "ot", "mantenciones", "interno",
       "Botón «Enviar a logística» del acta de recepción de una OT interna de revisión de equipos.",
       adjunta="Acta en PDF y Excel", avisa_a="Solo a correos internos (@sphs.cl o @ilusfitness.com)"),
    # 2026-10-20 («¿Qué pasó?» de la OT, rama aparte que se mezcla después): se registra ANTES de que llegue su envío para que,
    # al mezclar las ramas, el interruptor ya exista. Lo envía `_send_ilus_email(..., evento="ot_que_paso_bodega",
    # modulo="comunicacion_interna")`; la puerta resuelve la clave sola por el evento (`_EVENTO_A_CLAVE`) aunque ese envío
    # todavía no lleve `correo=`.
    # 2026-10-26 (Daniel: «agrégalos apagados para encenderlos desde el front»): los avisos de Inquietudes de clientes y de Hallazgos
    # también por correo. Hoy avisan por la campana; estos correos son INTERNOS (nunca al cliente ni a un técnico), sin montos, y
    # NACEN APAGADOS: se encienden en Comunicaciones → Control de correos.
    _c("inq_nueva_gestion", "Inquietud de cliente nueva: aviso a la gestión", "ot", "comunicacion_interna", "responsable_ot",
       "Cuando alguien anota una inquietud de un cliente (desde la OT, la ficha del cliente o la bandeja).",
       modo="automatico", nace_apagado=True,
       avisa_a="Al responsable de la inquietud o, si no tiene, a la gestión de la OT / quienes tienen el permiso «Responsable de OT»",
       nota="Interno. La campana avisa igual aunque el correo esté apagado."),
    _c("inq_asignada_responsable", "Inquietud asignada: aviso al nuevo responsable", "ot", "comunicacion_interna", "responsable_ot",
       "Cuando la gestión le asigna una inquietud a una persona (Víctor, Aarón o Juan Pablo).",
       modo="automatico", nace_apagado=True, avisa_a="A la persona que queda a cargo",
       nota="Interno. Si la persona se la adjudica a sí misma no se le escribe."),
    _c("inq_sla_vencido", "Inquietud con el plazo por vencer o vencido", "ot", "comunicacion_interna", "responsable_ot",
       "Cuando el reloj de la inquietud (primera respuesta o resolución) entra en ámbar o rojo.",
       modo="automatico", nace_apagado=True, avisa_a="Al responsable (o a la gestión si aún no tiene)",
       nota="Interno. Un aviso por cada cambio de nivel, no uno por minuto."),
    _c("hall_nuevos_gestion", "Hallazgos nuevos en una OT: aviso a la gestión", "ot", "comunicacion_interna", "responsable_ot",
       "Cuando el técnico reporta una falla, alerta, baja, repuesto o incidencia en la OT (REGLA #38).",
       modo="automatico", nace_apagado=True,
       avisa_a="A la gestión de la OT (el responsable primero), nunca al técnico ni al cliente",
       nota="Interno y agrupado: un correo por OT y persona cuando nace la campana; si llegan más en 30 minutos no se repite."),
    _c("hall_tomado_responsable", "Hallazgo tomado por otra persona: aviso al responsable de la OT", "ot", "comunicacion_interna",
       "responsable_ot", "Cuando alguien de la gestión toma un hallazgo de una OT que tiene otro responsable.",
       modo="automatico", nace_apagado=True, avisa_a="Al responsable de la OT",
       nota="Interno, tono suave: solo informa quién tomó el caso."),
    _c("ot_que_paso_bodega", "Falta de stock: aviso a bodega", "ot", "comunicacion_interna", "bodega",
       "Cuando «¿Qué pasó?» deja una OT con productos pendientes por falta de stock: aviso interno para que bodega lo sepa.",
       modo="automatico", avisa_a="A los correos de bodega de Retiros → Configuración (nunca al cliente ni al técnico)",
       nota="Es un aviso interno: no sale a clientes ni a técnicos. Usa los mismos correos de bodega que «Avisar a bodega» de Retiros "
            "(Retiros → Configuración). Sale por la llave de Comunicación interna."),
    # 2026-10-21 (Daniel, comunicación de la OT, REGLA #37): «OT terminada» automática al aprobar el cierre y «Enviar OT por correo» manual.
    # El del CLIENTE nace APAGADO (variable de entorno, REGLA #22) y solo Daniel lo enciende; los demás salen por la puerta de siempre.
    _c("ot_terminada_cliente", "OT terminada: aviso al cliente (documento firmado, sin montos)", "ot", "mantenciones", "cliente",
       "Al APROBAR el cierre de una OT de cliente: el documento firmado (sin montos) al contacto de la OT y a los correos de la ficha "
       "del cliente. No sale en un trabajo interno ni en una desinstalación.",
       modo="automatico", plantilla=("mantenciones", "ot_terminada"),
       condiciones=(_cond("OT_TERMINADA_CLIENTE_ACTIVA", "verdadero_amplio", "todo",
                          "El aviso automático al cliente está encendido (OT_TERMINADA_CLIENTE_ACTIVA).",
                          "Apagado por configuración: OT_TERMINADA_CLIENTE_ACTIVA no está encendida. Encenderla requiere la "
                          "autorización explícita de Daniel (REGLA #22). Mientras tanto la gestión lo envía a mano con «Enviar OT por correo»."),),
       avisa_a="Al contacto de la OT y a los correos de la ficha del cliente", adjunta="Enlace al documento firmado (sin montos)",
       nota="Nace APAGADO. Al encenderlo no se manda nada retroactivo: solo las OT que se aprueben desde ese momento. Una OT terminada "
            "cuyo aviso al cliente no salió muestra en su ficha «todavía no se le envió al cliente» con el botón «Enviar OT por correo»."),
    _c("ot_terminada_responsable", "OT terminada: aviso a quien la creó, al ejecutivo o al responsable", "ot", "mantenciones",
       "responsable_ot",
       "Al APROBAR el cierre de una OT: quien la creó, el ejecutivo del cliente o el responsable de la OT (los decide "
       "`_ot_gestion_destinatarios`), con lo que se le envió al cliente y al técnico.",
       modo="automatico", plantilla=("mantenciones", "ot_terminada_interno"), demo={"resumen_envio_html": _DEMO_RESUMEN_ENVIO},
       avisa_a="A quien creó la OT, al ejecutivo del cliente y al responsable (nunca al cliente ni al técnico)"),
    _c("ot_tec_terminada", "OT terminada: copia para el técnico que la hizo (sin montos)", "ot", "ot_tecnicos", "tecnico",
       "Al APROBAR el cierre de una OT: el técnico (o la empresa externa) que la hizo recibe su copia, sin montos ni proveedores.",
       modo="automatico", plantilla=("mantenciones", "ot_terminada_tecnico"), plantilla_apagada="no_sale",
       avisa_a="A cada técnico de la OT"),
    _c("ot_envio_manual", "Enviar OT por correo (envío manual a cliente, ejecutivo u otros)", "ot", "mantenciones", "cliente",
       "Botón «Enviar OT por correo» de la ficha de una OT cerrada (solo gestión): el documento firmado, sin montos, a los correos que se "
       "elijan. Prellenado con el cliente, el ejecutivo y los técnicos; cada destinatario ve el mismo texto.",
       plantilla=("mantenciones", "ot_envio_manual"), adjunta="Enlace al documento firmado (sin montos)",
       avisa_a="A los correos que se elijan en el modal",
       nota="Dice la verdad por destinatario: «Se envió a…» o «No salió porque…». Un técnico nunca lo recibe por esta llave: sale por la "
            "de «Correos a técnicos (OT)» (ot_tec_envio_manual)."),
    _c("ot_tec_envio_manual", "Enviar OT por correo: copia al técnico (sin montos)", "ot", "ot_tecnicos", "tecnico",
       "Cuando el envío manual de la OT incluye a un técnico (de esa OT o de cualquiera): sale por la llave de «Correos a técnicos (OT)» y "
       "el mensaje que escribió quien envía pierde las líneas que mencionen plata, proveedores u OC.",
       plantilla=("mantenciones", "ot_envio_manual"), avisa_a="A los técnicos que se incluyan en el envío",
       nota="Usa el texto de «Enviar OT por correo» (la misma plantilla)."),
    _c("mant_invitacion_tecnico_externo", "Invitación a un técnico externo", "ot", "mantenciones", "proveedor_courier",
       "Botón «Invitar» de la ficha del técnico externo.", avisa_a="Al contacto del proveedor"),

    # ─────────────────────────────── TICKETS E INCIDENCIAS ───────────────────────────────
    _c("tk_creacion", "Ticket creado: confirmación al cliente", "tickets", "tickets", "cliente",
       "Al crear un ticket (desde el sistema o el formulario público de soporte).", modo="automatico",
       plantilla=("tickets", "creacion"), condiciones=(_TK_TEST,), avisa_a="Al correo de contacto del ticket",
       nota=_TK_PAUSA),
    _c("tk_en_curso", "Ticket en curso", "tickets", "tickets", "cliente",
       "Cuando una persona pasa el ticket a «En curso».", plantilla=("tickets", "en_curso"), condiciones=(_TK_TEST,),
       avisa_a="Al correo de contacto del ticket", nota=_TK_PAUSA),
    _c("tk_pendiente", "Ticket pendiente", "tickets", "tickets", "cliente",
       "Cuando el ticket queda «Pendiente» (falta información del cliente).", plantilla=("tickets", "pendiente"),
       condiciones=(_TK_TEST,), avisa_a="Al correo de contacto del ticket", nota=_TK_PAUSA),
    _c("tk_ot_generada", "OT generada desde el ticket", "tickets", "tickets", "cliente",
       "Cuando se genera una orden de trabajo desde el ticket.", modo="automatico",
       plantilla=("tickets", "ot_generada"), condiciones=(_TK_TEST,), avisa_a="Al correo de contacto del ticket",
       nota="Junto a este correo sale el de «OT creada» (llave de Servicio Técnico) y una copia fija interna. " + _TK_PAUSA),
    _c("tk_ot_en_curso", "Técnico en terreno", "tickets", "tickets", "cliente",
       "Cuando el técnico inicia la OT en terreno.", modo="automatico", plantilla=("tickets", "ot_en_curso"),
       condiciones=(_TK_TEST,), avisa_a="Al correo de contacto del ticket", nota=_TK_PAUSA),
    _c("tk_resuelto", "Ticket resuelto", "tickets", "tickets", "cliente",
       "Cuando una persona resuelve el ticket, al cerrar la OT o al aprobar su cierre.", modo="mixto",
       plantilla=("tickets", "resuelto"), condiciones=(_TK_TEST,), avisa_a="Al correo de contacto del ticket",
       nota=_TK_PAUSA),
    _c("tk_cerrado", "Ticket cerrado", "tickets", "tickets", "cliente",
       "Cuando una persona cierra el ticket.", plantilla=("tickets", "cerrado"), condiciones=(_TK_TEST,),
       avisa_a="Al correo de contacto del ticket", nota=_TK_PAUSA),
    _c("tk_cancelado", "Ticket cancelado", "tickets", "tickets", "cliente",
       "Cuando una persona cancela el ticket.", plantilla=("tickets", "cancelado"), condiciones=(_TK_TEST,),
       avisa_a="Al correo de contacto del ticket", nota=_TK_PAUSA),
    # Tickets 2.0 (Daniel 2026-10-27: «SLA de respuesta… recordatorio… campana y correo interno»).
    _c("tk_sla_por_vencer", "SLA de respuesta por vencer: aviso al responsable", "tickets", "comunicacion_interna",
       "interno", "Cuando a un mensaje del cliente le queda 1 h hábil o menos de las 4 h para responderlo "
       "(una vez por mensaje, solo en horario hábil).", modo="automatico",
       avisa_a="Al responsable del ticket; si no tiene, a los responsables de tickets",
       nota="Nunca le escribe al cliente. También llega por la campana. Nace apagado: Daniel 2026-10-27 «evita enviar mensajes masivos»; se enciende desde esta pantalla.",
       nace_apagado=True),
    _c("tk_sla_vencido", "SLA de respuesta vencido: aviso al responsable", "tickets", "comunicacion_interna",
       "interno", "Cuando un mensaje del cliente pasa las 4 h hábiles sin respuesta (una vez por mensaje, "
       "solo en horario hábil).", modo="automatico",
       avisa_a="Al responsable del ticket; si no tiene, a los responsables de tickets",
       nota="Nunca le escribe al cliente. También llega por la campana. Nace apagado: Daniel 2026-10-27 «evita enviar mensajes masivos»; se enciende desde esta pantalla.",
       nace_apagado=True),
    _c("tk_respuesta", "Respuesta al cliente desde el ticket", "tickets", "tickets", "cliente",
       "Botón «Responder al cliente» de la ficha del ticket.", plantilla=("tickets", "respuesta"),
       condiciones=(_TK_TEST,), avisa_a="Al cliente y, si se pide, a una copia"),
    _c("tk_asignacion", "Ticket asignado: aviso al usuario asignado", "tickets", "tickets", "interno",
       "Cuando alguien asigna el ticket a otra persona (no sale si se lo asigna a sí mismo).",
       plantilla=("tickets", "asignacion"), avisa_a="Al usuario asignado"),
    _c("tk_ot_agendada", "OT agendada desde un ticket (copia fija interna)", "tickets", "tickets", "interno",
       "Al generar la OT desde un ticket: confirmación con calendario, hoy siempre a una casilla interna fija.",
       modo="automatico", adjunta="Archivo de calendario (.ics)", avisa_a="A la casilla interna de pruebas del módulo"),
    _c("tk_alerta_recepcion_caida", "Alerta: la recepción de correos de Tickets está caída", "tickets", "tickets", "interno",
       "Cuando el buzón de Tickets deja de recibir (máximo un aviso cada 12 horas).", modo="automatico",
       avisa_a="Al buzón de alertas de Tickets",
       nota="Tickets tiene además su propio ajuste «aviso por correo apagado» (configuración de Tickets)."),

    # ─────────────────────────────── CONTRATOS Y MANTENCIÓN PROGRAMADA ───────────────────────────────
    _c("mant_plan_propuesto", "Propuesta de plan anual de mantención", "mantenciones", "mantenciones", "cliente",
       "Botón «Proponer plan por correo» de la ficha del cliente.", plantilla=("mantenciones", "plan_propuesto"),
       avisa_a="A los correos de la ficha del cliente"),
    _c("mant_recordatorio_visita", "Recordatorio de visita (de 3 a 7 días antes)", "mantenciones", "mantenciones", "cliente",
       "Cada mañana a las 06:00 para las visitas que ocurren entre 3 y 7 días después.", modo="automatico",
       plantilla=("mantenciones", "recordatorio_visita"),
       condiciones=(_cond("MANT_RECORDATORIOS_EMAIL_ENABLED", "verdadero_mant", "todo",
                          "Los recordatorios por correo están encendidos (MANT_RECORDATORIOS_EMAIL_ENABLED).",
                          "Apagado por configuración: MANT_RECORDATORIOS_EMAIL_ENABLED no está encendida. Encenderla "
                          "requiere la autorización explícita de Daniel."),),
       avisa_a="Al contacto de la visita"),
    _c("mant_factura_pendiente", "Aviso interno: visita completada sin facturar", "mantenciones", "mantenciones", "interno",
       "Cada mañana a las 06:00 para visitas completadas hace más de 7 días sin facturar.", modo="automatico",
       condiciones=(_cond("MANT_RECORDATORIOS_EMAIL_ENABLED", "verdadero_mant", "todo",
                          "Los recordatorios por correo están encendidos (MANT_RECORDATORIOS_EMAIL_ENABLED).",
                          "Apagado por configuración: MANT_RECORDATORIOS_EMAIL_ENABLED no está encendida. Encenderla "
                          "requiere la autorización explícita de Daniel."),),
       avisa_a="A una casilla interna fija del código"),
    _c("mant_email_manual", "Email manual desde la ficha del cliente", "mantenciones", "mantenciones", "cliente",
       "Botón «Email manual» de la ficha del cliente (texto libre).", avisa_a="Al destinatario que se escriba"),
    _c("mant_notificacion_envio", "Notificación enviada desde Servicio Técnico", "mantenciones", "mantenciones", "cliente",
       "Botón «Enviar» de una notificación de la ficha del cliente.", avisa_a="Al destinatario de la notificación"),

    # ─────────────────────────────── RETIROS ───────────────────────────────
    _c("ret_solicitud_recibida", "Solicitud de retiro recibida", "retiros", "retiros", "cliente",
       "Cuando alguien envía el formulario público de solicitud de retiro.", modo="automatico",
       plantilla=("retiros", "solicitud_recibida"), tambien_whatsapp=True,
       avisa_a="A la persona del formulario y a las copias del retiro"),
    _c("ret_propuesta", "ILUS propone una fecha de retiro", "retiros", "retiros", "cliente",
       "Botón «Proponer fecha» y también al crear un retiro interno.", plantilla=("retiros", "propuesta_enviada"),
       tambien_whatsapp=True, avisa_a="Al contacto del retiro y a sus copias"),
    _c("ret_cita_confirmada", "Cita de retiro confirmada", "retiros", "retiros", "cliente",
       "Cuando el cliente confirma, se acepta su contrapropuesta, se cambia el estado o se propone la misma fecha que pidió.",
       modo="mixto", plantilla=("retiros", "agenda_confirmada"), tambien_whatsapp=True,
       adjunta="Archivo de calendario (.ics)", avisa_a="Al contacto del retiro y a sus copias"),
    _c("ret_en_preparacion", "Estamos preparando tu retiro", "retiros", "retiros", "cliente",
       "Al enviar el retiro a preparación, a mano o solo cuando Check informa que bodega empezó a juntar el pedido.",
       modo="mixto", plantilla=("retiros", "en_preparacion"), tambien_whatsapp=True,
       condiciones=(_cond("RETIROS_PREP_AUTO", "no_apagado", "auto",
                          "El envío automático a preparación está encendido (RETIROS_PREP_AUTO).",
                          "El envío automático a preparación está apagado (RETIROS_PREP_AUTO); a mano sigue funcionando."),
                    _cond("RETIROS_CHECK_AUTO", "no_apagado", "auto",
                          "El marcado automático según Check está encendido (RETIROS_CHECK_AUTO).",
                          "«Check solo informa» (RETIROS_CHECK_AUTO apagada): el envío automático no corre.")),
       avisa_a="Al contacto del retiro y a sus copias",
       nota="El disparo automático corre solo dentro del horario de cobertura de la bodega."),
    _c("ret_retiro_completado", "Retiro completado (con botón de la encuesta)", "retiros", "retiros", "cliente",
       "Cuando una persona marca el retiro como retirado o cuando bodega expide todo el pedido en Check.",
       modo="mixto", plantilla=("retiros", "retirada"), tambien_whatsapp=True,
       condiciones=(_cond("RETIROS_RETIRO_AUTO", "modo_activo", "auto",
                          "El cierre automático por expedición en Check está activo (RETIROS_RETIRO_AUTO=activo).",
                          "El cierre automático por Check no está activo (RETIROS_RETIRO_AUTO); a mano sigue funcionando."),
                    _cond("RETIROS_ENCUESTA_ACTIVA", "verdadero_amplio", "extra",
                          "La encuesta de satisfacción está activa y va como botón dentro de este correo.",
                          "La encuesta está apagada (RETIROS_ENCUESTA_ACTIVA): el correo sale sin el botón.")),
       avisa_a="Al contacto del retiro y a sus copias",
       nota="Si este correo se apaga, el cambio a «Retirada» ocurre igual, pero la encuesta no se activa "
            "(la encuesta se activa solo con este correo enviado)."),
    _c("ret_cancelado", "Retiro cancelado o rechazado", "retiros", "retiros", "cliente",
       "Cuando el cliente cancela, se rechaza la solicitud o «¿Qué pasó?» pide avisar al cliente.", modo="mixto",
       plantilla=("retiros", "rechazada"), tambien_whatsapp=True, avisa_a="Al contacto del retiro y a sus copias"),
    _c("ret_no_concretado", "No pudimos concretar tu retiro", "retiros", "retiros", "cliente",
       "Cuando el retiro pasa a «No concretado» o «¿Qué pasó?» pide avisar al cliente.",
       plantilla=("retiros", "fallida"), tambien_whatsapp=True, avisa_a="Al contacto del retiro y a sus copias"),
    _c("ret_reagendado", "Retiro reagendado", "retiros", "retiros", "cliente",
       "Cuando una persona reagenda el retiro.", plantilla=("retiros", "reagendada"), tambien_whatsapp=True,
       avisa_a="Al contacto del retiro y a sus copias"),
    _c("ret_info_incompleta", "Falta información para tu retiro", "retiros", "retiros", "cliente",
       "Cuando el retiro pasa a «Información incompleta» o «Esperando al cliente».",
       plantilla=("retiros", "informacion_incompleta"), tambien_whatsapp=True,
       avisa_a="Al contacto del retiro y a sus copias"),
    _c("ret_recordatorio_24h", "Recordatorio: tu retiro es mañana", "retiros", "retiros", "cliente",
       "Cada mañana a las 06:00 para los retiros confirmados al día siguiente, o desde el botón de recordatorios.",
       modo="mixto", plantilla=("retiros", "recordatorio_24h"), tambien_whatsapp=True,
       adjunta="Archivo de calendario (.ics)", avisa_a="Al contacto del retiro y a sus copias"),
    _c("ret_mensaje_cliente", "Mensaje al cliente desde la ficha del retiro", "retiros", "retiros", "cliente",
       "Cuando el equipo escribe un mensaje al cliente desde el retiro, o como acuse cuando el cliente contrapropone.",
       modo="mixto", tambien_whatsapp=True, avisa_a="Al contacto del retiro y a sus copias"),
    _c("ret_comprobante_firma", "Comprobante de recepción firmado", "retiros", "retiros", "cliente",
       "Después de registrar la firma de recepción del retiro.", modo="automatico",
       condiciones=(_cond("RETIROS_FIRMA_CORREO", "exacto_1", "todo",
                          "El comprobante por correo está encendido (RETIROS_FIRMA_CORREO=1).",
                          "Apagado por configuración: RETIROS_FIRMA_CORREO no vale 1. Encenderlo requiere la "
                          "autorización explícita de Daniel (REGLA #23)."),),
       avisa_a="Al contacto del retiro y a sus copias"),
    _c("ret_aviso_soporte_rechazo", "Aviso interno: el cliente rechazó la propuesta", "retiros", "retiros", "interno",
       "Cuando el cliente responde «No puedo asistir» a la fecha propuesta.", modo="automatico",
       avisa_a="Al buzón de soporte de la marca",
       nota="Sale por la llave de Retiros (la del cliente), no por la de comunicación interna."),
    _c("ret_aviso_equipo", "Aviso al equipo de Retiros (nuevas solicitudes y gestiones)", "retiros", "comunicacion_interna",
       "interno", "En cada gestión del retiro: solicitud nueva, respuesta del cliente, cita vencida, alerta de volumen…",
       modo="automatico", avisa_a="Al responsable del retiro y a la lista de avisos al equipo",
       nota="Las direcciones se editan en Retiros → Equipo y alertas."),
    _c("ret_aviso_bodega", "Aviso a bodega (¿Qué pasó?)", "retiros", "comunicacion_interna", "bodega",
       "Cuando «¿Qué pasó?» registra una desviación y se marca «Avisar a bodega».", avisa_a="A la lista de correos de bodega"),

    # ─────────────────────────────── TRANSPORTE ───────────────────────────────
    _c("tr_en_preparacion", "Pedido en preparación", "transporte", "transporte", "cliente",
       "Al asignar la factura a un manifiesto, al reenviar la notificación o al cancelar la visita de SimpliRoute.",
       modo="mixto", plantilla=("transporte", "programado"), avisa_a="Al correo del cliente del despacho",
       nota="Si se repite el mismo estado dentro de 10 minutos, el sistema no lo vuelve a mandar."),
    _c("tr_despachado", "Pedido despachado (entregado a transporte)", "transporte", "transporte", "cliente",
       "Al generar la etiqueta de FedEx, al firmar el retiro del manifiesto o por el escaneo del chofer.",
       modo="mixto", plantilla=("transporte", "en_ruta"), avisa_a="Al correo del cliente del despacho"),
    _c("tr_en_ruta", "Pedido en camino", "transporte", "transporte", "cliente",
       "Cuando FedEx o SimpliRoute informan que el pedido va en reparto (el lector corre las 24 horas), o por el chofer "
       "o un estado puesto a mano.", modo="mixto", plantilla=("transporte", "en_camino"),
       avisa_a="Al correo del cliente del despacho"),
    _c("tr_entregado", "Pedido entregado", "transporte", "transporte", "cliente",
       "Cuando el courier o el chofer confirman la entrega, o se registra la entrega a mano.", modo="mixto",
       plantilla=("transporte", "entregado"), avisa_a="Al correo del cliente del despacho"),
    _c("tr_entrega_fallida", "Problema con la entrega", "transporte", "transporte", "cliente",
       "Cuando el courier reporta una excepción o una persona marca «Problema» con su motivo.", modo="mixto",
       plantilla=("transporte", "fallido"), avisa_a="Al correo del cliente del despacho"),
    _c("tr_recuperar_enlaces", "Recuperar enlaces de seguimiento", "transporte", "transporte", "cliente",
       "Formulario público «Recuperar mis enlaces» (máximo 5 por hora).", modo="automatico",
       avisa_a="A la persona que escribió su correo en el formulario"),
    _c("tr_manifiesto_pdf", "Manifiesto firmado (PDF) al courier", "transporte", "transporte", "proveedor_courier",
       "Botón «Enviar por correo» de la firma del manifiesto.", adjunta="Manifiesto en PDF",
       avisa_a="A los correos que se escriban"),
    _c("tr_manifiesto_retiro_firmado", "Retiro del manifiesto firmado por el chofer", "transporte", "transporte",
       "proveedor_courier", "Al guardar la firma del chofer en el retiro del manifiesto.", modo="automatico",
       adjunta="Comprobante en PDF", avisa_a="Al correo de la ficha del courier"),
    _c("tr_aviso_courier_excel", "Aviso de carga al courier (Excel)", "transporte", "transporte", "proveedor_courier",
       "Botón «Avisar al courier» del manifiesto.", adjunta="Excel de carga", avisa_a="Al correo de la ficha del courier"),

    # ─────────────────────────────── COTIZACIONES ───────────────────────────────
    _c("tr_cotizacion_envio", "Cotización de transporte al cliente (PDF)", "cotizaciones", "transporte", "cliente",
       "Botón «Enviar» de una cotización de transporte.", plantilla=("transporte_cotizaciones", "cotizacion_envio"),
       adjunta="Cotización en PDF", avisa_a="A los correos que se elijan",
       nota="No tiene llave propia: sale por la llave de Transporte."),
    _c("tk_cotizacion_envio", "Cotización de un ticket al cliente (PDF)", "cotizaciones", "tickets", "cliente",
       "Botón «Enviar cotización» de un ticket.", plantilla=("tickets", "cotizacion_envio"), adjunta="Cotización en PDF",
       avisa_a="A los correos que se elijan"),

    # ─────────────────────────────── PROSPECTOS DE MANTENCIÓN ───────────────────────────────
    _c("prospecto_p1_oferta", "Paso 1: oferta del plan de mantención (desde el ticket)", "prospectos", "tickets", "cliente",
       "Botón «Ofrecer plan» de la ficha del prospecto: el correo sale desde el ticket de la oferta.",
       plantilla=("prospectos", "p1_oferta"), condiciones=(_TK_TEST,), avisa_a="Al contacto del cliente",
       nota="Sale por la llave de Tickets."),
    _c("prospecto_p2_cotizacion", "Paso 2: cotización del plan de mantención", "prospectos", "tickets", "cliente",
       "Cuando una persona revisa la cotización del plan y la envía al cliente.",
       plantilla=("prospectos", "p2_cotizacion"), condiciones=(_TK_TEST,), avisa_a="Al contacto del cliente",
       nota="Sale por la llave de Tickets."),
    _c("prospecto_plan_oferta", "Propuesta del plan de mantención (versión anterior)", "prospectos", "mantenciones",
       "cliente", "Botón «Enviar propuesta» del plan de mantención (versión sin botones, anterior al paso 1).",
       plantilla=("prospectos", "plan_mantencion_oferta"), avisa_a="Al correo del cliente"),
    _c("prospecto_oferta_auto_1", "Oferta automática (15 días después de la instalación)", "prospectos", "mantenciones",
       "cliente", "Campaña automática: barrido cada 10 minutos, lunes a viernes de 08:00 a 19:00.", modo="automatico",
       plantilla=("prospectos", "prospecto_oferta_1"),
       condiciones=(_cond("PROSPECTOS_OFERTA_AUTO", "verdadero_prosp", "todo",
                          "La campaña automática de ofertas está encendida (PROSPECTOS_OFERTA_AUTO).",
                          "Apagado por configuración: PROSPECTOS_OFERTA_AUTO no está encendida. Encenderla requiere la "
                          "autorización explícita de Daniel."),),
       avisa_a="Al contacto del cliente"),
    _c("prospecto_oferta_auto_2", "Recordatorio automático (3 meses después de la instalación)", "prospectos", "mantenciones",
       "cliente", "Campaña automática: barrido cada 10 minutos, lunes a viernes de 08:00 a 19:00.", modo="automatico",
       plantilla=("prospectos", "prospecto_oferta_2"),
       condiciones=(_cond("PROSPECTOS_OFERTA_AUTO", "verdadero_prosp", "todo",
                          "La campaña automática de ofertas está encendida (PROSPECTOS_OFERTA_AUTO).",
                          "Apagado por configuración: PROSPECTOS_OFERTA_AUTO no está encendida. Encenderla requiere la "
                          "autorización explícita de Daniel."),),
       avisa_a="Al contacto del cliente"),

    # ─────────────────────────────── REPUESTOS Y COMPRAS ───────────────────────────────
    _c("compra_repuesto_proveedor", "Solicitud de compra de repuestos al proveedor", "repuestos", "tickets",
       "proveedor_courier", "Al crear una compra de repuestos con la casilla «Enviar por correo» marcada.",
       condiciones=(_TK_TEST,), avisa_a="Al correo del proveedor", nota="Sale por la llave de Tickets."),
    # 2026-10-21 (Daniel, solicitud de repuesto desde la OT, REGLA #41): tres correos INTERNOS que nacen APAGADOS por configuración
    # (REPUESTOS_CORREOS_ACTIVOS, sin valor en deploy.yml: los enciende Daniel con su «sí»). Nunca al cliente (REGLA #22); al técnico jamás
    # proveedor, costo ni N° de OC (REGLAS #19 y #26). La campana avisa igual mientras estos correos estén apagados.
    _c("rep_nueva", "Pedido de repuesto grave: aviso al dueño y a bodega", "repuestos", "comunicacion_interna", "bodega",
       "Cuando un técnico pide un repuesto desde una OT y el equipo queda FUERA DE SERVICIO o el diagnóstico dice que puede lastimar a "
       "alguien. Un pedido normal avisa solo por la campana.",
       modo="automatico", condiciones=(_REP_CORREOS,),
       avisa_a="Al dueño del pedido (responsable de la OT) y a los correos de bodega de Retiros → Configuración (nunca al cliente ni al técnico)",
       nota="Nace APAGADO. Sale por la llave de Comunicación interna."),
    _c("rep_vencida", "Pedidos de repuesto sin primera respuesta: aviso al dueño y a supervisión", "repuestos", "comunicacion_interna",
       "responsable_ot",
       "Cuando un pedido lleva más de 8 horas hábiles sin que nadie lo responda (el plazo es de 1 día hábil): el barrido de cada 10 minutos "
       "avisa al dueño y escala a supervisores y administradores. Un solo correo por persona con la lista de lo vencido.",
       modo="automatico", condiciones=(_REP_CORREOS,),
       avisa_a="Al dueño del pedido y a los supervisores y administradores activos (nunca al cliente ni al técnico)",
       nota="Nace APAGADO. Solo en horario de cobertura (lunes a viernes hábiles, 08:00–17:00, colación 13:00–14:00) y una vez por pedido."),
    _c("rep_lista_tecnico", "Repuesto en bodega: aviso al técnico que lo pidió", "repuestos", "ot_tecnicos", "tecnico",
       "Cuando la pieza que pidió el técnico llega a bodega y queda lista para su OT. Sin proveedor, costo ni N° de OC.",
       modo="automatico", condiciones=(_REP_CORREOS,), avisa_a="Al técnico que pidió el repuesto",
       nota="Nace APAGADO. Sale por la llave de correos a técnicos (OT). El técnico recibe antes la campana de cada paso."),

    # ─────────────────────────────── CATÁLOGO ───────────────────────────────
    _c("catalogo_manual", "Manual de producto (PDF)", "catalogo", "catalogo", "cliente",
       "Botón «Enviar manual por correo» de la ficha del producto.", plantilla=("catalogo", "manual_envio"),
       adjunta="Manual en PDF", avisa_a="Al correo que se escriba (y una copia opcional)"),

    # ─────────────────────────────── ACCESOS Y USUARIOS ───────────────────────────────
    _c("acceso_invitacion", "Invitación para crear la contraseña (usuario nuevo)", "accesos", "comunicacion_interna",
       "usuario", "Al crear un usuario y con «Reenviar invitación».", plantilla=("comunicacion_interna", "usuario_nuevo"),
       protegido=True, tambien_whatsapp=True, avisa_a="Al usuario"),
    _c("acceso_cambio_clave", "Cambio de contraseña pedido por un administrador", "accesos", "comunicacion_interna",
       "usuario", "Botón «Enviar enlace de cambio de contraseña» de la ficha del usuario.",
       plantilla=("comunicacion_interna", "cambio_clave"), protegido=True, tambien_whatsapp=True, avisa_a="Al usuario"),
    _c("acceso_olvido_clave", "Recuperar la contraseña («Olvidé mi contraseña»)", "accesos", "comunicacion_interna",
       "usuario", "Formulario público «Olvidé mi contraseña».", modo="automatico",
       plantilla=("comunicacion_interna", "olvido_contrasena"), protegido=True, avisa_a="Al usuario que lo pide"),
    _c("acceso_habilitado", "Acceso al portal habilitado", "accesos", "comunicacion_interna", "usuario",
       "Al crear o editar un usuario con una clave escrita a mano.", avisa_a="Al usuario"),
    _c("acceso_alerta_login", "Alerta de inicio de sesión", "accesos", "comunicacion_interna", "usuario",
       "Aviso de seguridad al iniciar sesión desde un dispositivo nuevo.", modo="automatico",
       plantilla=("comunicacion_interna", "inicio_sesion"), apagado_en_codigo=True,
       nota="Apagado en el código desde 08-06-2026 (Daniel: «un inicio de sesión no amerita un correo»)."),

    # ─────────────────────────────── LO QUE ENVÍAN TERCEROS (fase 3) ───────────────────────────────
    # Estos cuatro NO los envía ILUS: los envía el tercero. El interruptor decide si ILUS le ENTREGA el correo del destinatario. Obedecen
    # al interruptor general y a su propio interruptor (decisión de Daniel, 20-oct-2026); la llave de paso del módulo no los frena.
    _c("ext_fedex_notif_envio", "Aviso de FedEx al destinatario del envío", "transporte", "transporte", "cliente",
       "Casilla «Notificar» al crear una guía de FedEx (crear, masivo o reemitir). El correo lo envía FedEx.",
       tipo="externo", avisa_a="Al correo del destinatario de la guía",
       nota="ILUS decide si le entrega a FedEx el correo del destinatario. Obedece al interruptor general y a este interruptor, "
            "no a la llave de Transporte."),
    _c("ext_fedex_notif_retiro", "Aviso de FedEx por el retiro programado", "transporte", "transporte", "proveedor_courier",
       "«Programar retiro FedEx» con un correo escrito en la notificación. El correo lo envía FedEx.",
       tipo="externo", avisa_a="Al correo escrito en el formulario",
       nota="ILUS decide si le entrega a FedEx ese correo. Obedece al interruptor general y a este interruptor, "
            "no a la llave de Transporte."),
    _c("ext_fedex_export_email", "Correo del cliente en el Excel de carga de FedEx", "transporte", "transporte", "cliente",
       "Exportar el Excel de carga masiva de FedEx o «Avisar al courier»: el Excel lleva el correo del cliente y FedEx le avisa.",
       tipo="externo", avisa_a="FedEx envía los avisos al correo del Excel",
       nota="ILUS decide si pone el correo del cliente en el Excel (columna del destinatario). Obedece al interruptor general y a "
            "este interruptor, no a la llave de Transporte."),
    _c("ext_simpliroute_contacto", "Correo del cliente en la visita de SimpliRoute", "transporte", "transporte", "cliente",
       "Al subir o reenviar una visita a SimpliRoute (también en el redespacho automático) y al exportar el Excel de SimpliRoute. "
       "El aviso lo envía SimpliRoute si su cuenta lo tiene configurado.", modo="mixto", tipo="externo",
       avisa_a="SimpliRoute avisa al correo de contacto",
       nota="ILUS decide si incluye el correo del cliente en la visita. Obedece al interruptor general y a este interruptor, "
            "no a la llave de Transporte."),

    # ─────────────────────────────── HERRAMIENTAS (no se apagan por interruptor) ───────────────────────────────
    _c("comm_prueba_manual", "Enviar prueba (Comunicaciones)", "comunicaciones", "general", "interno",
       "Botón «Enviar prueba» de la pestaña Email: un correo de ejemplo al destinatario que escribe el superadministrador.",
       tipo="herramienta", avisa_a="Al destinatario que se escriba",
       nota="No se apaga aquí: solo respeta el interruptor general."),
    _c("comm_reenviar_historial", "Reintentar un envío del Historial", "comunicaciones", "general", "interno",
       "Botón «Reenviar este correo» del Historial.", tipo="herramienta", avisa_a="Al destinatario original",
       nota="Reintenta con la clave del correo original: si ese correo está apagado, no sale."),
)

# La herramienta «Reintentar» y «Enviar prueba» no pertenecen a ningún grupo de la pantalla: se listan aparte.
HERRAMIENTAS = tuple(c["clave"] for c in REGISTRO if c["tipo"] == "herramienta")

_POR_CLAVE = {c["clave"]: c for c in REGISTRO}


def switch_de_fila(clave_o_correo, fila):
    """¿El interruptor de ESTE correo está encendido? Con fila en comm_correo_control manda la fila; sin fila, encendido salvo que
    el correo `nace_apagado` (avisos internos nuevos, Daniel 26-oct-2026)."""
    if fila is not None:
        return bool(fila.get("activo", True))
    c = clave_o_correo if isinstance(clave_o_correo, dict) else por_clave(clave_o_correo)
    return not bool((c or {}).get("nace_apagado"))


def por_clave(clave):
    """El registro de un correo, o None si la clave no existe."""
    return _POR_CLAVE.get(clave)


def claves(incluir_herramientas=False, tipo=None):
    """Las claves registradas (por defecto sin las herramientas)."""
    return tuple(c["clave"] for c in REGISTRO
                 if (incluir_herramientas or c["tipo"] != "herramienta") and (tipo is None or c["tipo"] == tipo))


def listar(tipo="ilus"):
    """Los correos que la pantalla muestra como tarjeta (por defecto, los que envía ILUS)."""
    return tuple(c for c in REGISTRO if c["tipo"] == tipo)


def es_protegido(clave):
    c = _POR_CLAVE.get(clave)
    return bool(c and c["protegido"])


def nombre_de(clave):
    c = _POR_CLAVE.get(clave)
    return c["nombre"] if c else str(clave or "")


def validar(llaves_validas=None):
    """Revisa que el registro esté completo y coherente. Devuelve la lista de problemas (vacía = todo bien).
    `llaves_validas`: las llaves de paso que existen en app.py (`_MODULOS_KILLSWITCH_VALIDOS`)."""
    problemas = []
    vistos = set()
    patron = re.compile(r"^[a-z][a-z0-9_]{2,59}$")
    for c in REGISTRO:
        k = c.get("clave") or ""
        if not patron.match(k):
            problemas.append(f"clave inválida: {k!r}")
        if k in vistos:
            problemas.append(f"clave repetida: {k}")
        vistos.add(k)
        for campo in ("nombre", "modulo_ui", "llave", "destinatario", "disparo", "modo", "tipo"):
            if not str(c.get(campo) or "").strip():
                problemas.append(f"{k}: falta {campo}")
        if c.get("modulo_ui") not in MODULOS_UI_CLAVES:
            problemas.append(f"{k}: modulo_ui desconocido ({c.get('modulo_ui')})")
        if c.get("destinatario") not in DESTINATARIOS:
            problemas.append(f"{k}: destinatario desconocido ({c.get('destinatario')})")
        if c.get("modo") not in MODOS:
            problemas.append(f"{k}: modo desconocido ({c.get('modo')})")
        if c.get("tipo") not in TIPOS:
            problemas.append(f"{k}: tipo desconocido ({c.get('tipo')})")
        if llaves_validas is not None and c.get("llave") not in llaves_validas:
            problemas.append(f"{k}: la llave {c.get('llave')!r} no existe")
        if c.get("plantilla") is not None:
            p = c["plantilla"]
            if not (isinstance(p, tuple) and len(p) == 2 and all(isinstance(x, str) and x for x in p)):
                problemas.append(f"{k}: plantilla mal formada ({p!r})")
        if c.get("plantilla_apagada") not in PLANTILLA_APAGADA:
            problemas.append(f"{k}: plantilla_apagada desconocida")
        for cond in c.get("condiciones") or ():
            if cond.get("alcance") not in ALCANCES:
                problemas.append(f"{k}: alcance de condición desconocido ({cond.get('alcance')})")
            if cond.get("lector") and cond["lector"] not in LECTORES:
                problemas.append(f"{k}: lector de condición desconocido ({cond.get('lector')})")
        if c.get("protegido") and any(x.get("alcance") == "todo" for x in c.get("condiciones") or ()):
            problemas.append(f"{k}: un correo protegido no puede depender de una variable")
        if c.get("tipo") == "herramienta" and c.get("protegido"):
            problemas.append(f"{k}: una herramienta no puede ser protegida")
    return problemas


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  Qué clave le corresponde a un envío que NO la declaró (solo los eventos que no tienen dos lecturas posibles)
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════
_EVENTO_A_CLAVE = {
    # Transporte: `_tr_notificar_cliente` arma el evento como `tracking_<estado>` (esa función no se toca: otras pruebas la
    # protegen). Con tilde y sin tilde, porque el evento nace de `estado.lower()`.
    "tracking_en_preparación": "tr_en_preparacion", "tracking_en_preparacion": "tr_en_preparacion",
    "tracking_entregado_a_transporte": "tr_despachado", "tracking_en_ruta": "tr_en_ruta",
    "tracking_entregado": "tr_entregado", "tracking_entrega_fallida": "tr_entrega_fallida",
    "tracking_problema": "tr_entrega_fallida",
    "seguimiento_recuperar": "tr_recuperar_enlaces", "manifiesto_avisar_courier": "tr_aviso_courier_excel",
    # Accesos
    "login_alert": "acceso_alerta_login", "acceso_habilitado": "acceso_habilitado",
    # Retiros
    "pickup_reminder_24h": "ret_recordatorio_24h", "pickup_reject_operador": "ret_aviso_soporte_rechazo",
    "retiros_equipo": "ret_aviso_equipo", "retiros_bodega": "ret_aviso_bodega",
    # Servicio Técnico / OT
    "mant_recordatorio_visita": "mant_recordatorio_visita", "mant_recordatorio_factura": "mant_factura_pendiente",
    "mant_sla_manual": "mant_email_manual", "mant_notificacion": "mant_notificacion_envio",
    "tecnico_externo_invitacion": "mant_invitacion_tecnico_externo", "plan_propuesto": "mant_plan_propuesto",
    "reporte_servicio": "ot_informe_servicio", "ot_firma_remota": "ot_firma_remota",
    "ot_pendiente_aprobacion": "ot_pendiente_aprobacion", "ot_firmada_cliente": "ot_cliente_firmo_tecnico",
    "ot_autorizacion_pendiente": "ot_autorizacion_pendiente", "ot_acta_recepcion": "ot_acta_recepcion",
    "ot_asignada": "ot_tec_asignada", "ot_reagendada_tecnico": "ot_tec_reagendada",
    "ot_cancelada_tecnico": "ot_tec_cancelada", "ot_desasignada_tecnico": "ot_tec_desasignada",
    "agenda_tecnico_manana": "ot_tec_agenda_manana",
    # «¿Qué pasó?» de la OT (rama aparte): su envío puede llegar sin `correo=`; con esta línea la puerta igual lo resuelve.
    "ot_que_paso_bodega": "ot_que_paso_bodega",
    # Prospectos
    "plan_mantencion_oferta": "prospecto_plan_oferta", "prospecto_oferta_1": "prospecto_oferta_auto_1",
    "prospecto_oferta_2": "prospecto_oferta_auto_2",
    # Tickets y otros
    "ticket_asignacion": "tk_asignacion", "ticket_respuesta": "tk_respuesta", "ot_agendada": "tk_ot_agendada",
    "tk_mail_recepcion_caida": "tk_alerta_recepcion_caida", "compra_repuesto": "compra_repuesto_proveedor",
    "catalogo_manual": "catalogo_manual",
}
for _slug in ("creacion", "en_curso", "pendiente", "ot_generada", "ot_en_curso", "resuelto", "cerrado", "cancelado"):
    _EVENTO_A_CLAVE["ticket_" + _slug] = "tk_" + _slug


def inferir(modulo, evento):
    """La clave que corresponde a un envío que no declaró `correo=`, o None si no hay una sola lectura posible.
    Quita el prefijo «retry_» (reintentos del Historial) y «prueba_» (pruebas de la pantalla)."""
    ev = str(evento or "").strip().lower()
    if ev.startswith("retry_"):
        ev = ev[6:]
    if ev.startswith("prueba_"):
        cand = ev[7:]
        return cand if cand in _POR_CLAVE else None
    clave = _EVENTO_A_CLAVE.get(ev)
    return clave if clave in _POR_CLAVE else None


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  El estado real de un correo (lo calcula la pantalla; la puerta de envío usa los mismos pasos)
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════
PASOS = (
    (1, "general", "Interruptor general"),
    (2, "llave", "Llave del módulo"),
    (3, "correo", "Este correo"),
    (4, "config", "Configuración"),
)


def estado_efectivo(c, *, general_activo=True, llave_bloqueada=False, switch_activo=True, environ=None):
    """Estado real de UN correo con lo que se sabe hoy.

    Los pasos se revisan en el mismo orden que la puerta de envío: general → llave del módulo → este correo → configuración
    (variables de entorno y código). El primero que lo frena es `frena_en`; los siguientes quedan «saltados».

    Cada paso: `estado` ∈ ok · apagado (lo apagó una persona: ámbar) · config (lo apaga la configuración o el código: gris)
    · protegido (correo de seguridad: no se apaga aquí) · saltado.
    `efectivo` ∈ sale · general · llave · correo · config · codigo.
    `puede_encender`: el interruptor de ESTE correo solo se puede ENCENDER si ninguna variable de configuración lo mantiene
    apagado (la pantalla solo apaga; encender un correo apagado por variable pasa por la variable y por el «sí» de Daniel).
    """
    env = os.environ if environ is None else environ
    conds = []
    bloquea_config = False
    for cond in c.get("condiciones") or ():
        activa = evaluar_condicion(cond, env) if cond.get("lector") else True
        # «info» sin lector: la pantalla la completa con el dato real (p. ej. mail_alerta_off).
        conds.append({"variable": cond["variable"], "alcance": cond["alcance"], "activa": activa,
                      "texto": cond["texto"] if activa else (cond.get("texto_apagada") or cond["texto"])})
        if cond["alcance"] == "todo" and not activa:
            bloquea_config = True
    codigo = bool(c.get("apagado_en_codigo"))
    protegido = bool(c.get("protegido"))
    # Los avisos que envía un tercero (FedEx, SimpliRoute: tipo «externo») NO pasan por la llave de paso del módulo: solo por el
    # interruptor general y el suyo (decisión de Daniel, 20-oct-2026). Su paso 2 se muestra «no aplica» y nunca frena.
    aplica_llave = c.get("tipo") != "externo"
    pasos = []
    frena = 0
    for n, clave_paso, nombre_paso in PASOS:
        if frena:
            est = "saltado"
        elif n == 1 and not general_activo:
            est, frena = "apagado", 1
        elif n == 2 and not aplica_llave:
            est = "saltado"
        elif n == 2 and llave_bloqueada:
            est, frena = "apagado", 2
        elif n == 3 and not protegido and not switch_activo:
            est, frena = "apagado", 3
        elif n == 4 and (codigo or bloquea_config):
            est, frena = "config", 4
        elif n == 3 and protegido:
            est = "protegido"
        else:
            est = "ok"
        pasos.append({"n": n, "clave": clave_paso, "nombre": nombre_paso, "estado": est})
    # Lo que muestra cada paso cuando el correo es protegido y aún no lo frena otro paso.
    efectivo = "sale"
    if frena == 1:
        efectivo = "general"
    elif frena == 2:
        efectivo = "llave"
    elif frena == 3:
        efectivo = "correo"
    elif frena == 4:
        efectivo = "codigo" if codigo else "config"
    return {
        "efectivo": efectivo, "frena_en": frena, "pasos": pasos, "condiciones": conds, "aplica_llave": aplica_llave,
        "bloquea_config": bloquea_config, "apagado_en_codigo": codigo,
        "puede_encender": (not protegido) and (not bloquea_config) and (not codigo),
        "puede_apagar": not protegido,
    }

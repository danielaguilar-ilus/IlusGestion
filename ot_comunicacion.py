# -*- coding: utf-8 -*-
"""COMUNICACIÓN DE LA ORDEN DE TRABAJO (Daniel, 20-oct-2026) — REGLA #37.

Daniel: «necesito que esto sea CONTROLABLE, que avises cuándo lo tienes que enviar y además habilita, así como las cotizaciones, un
envío por mail directo de las órdenes de trabajo… y quiero que avise a QUIÉN se le está enviando: al momento de crearse y enviarse los
correos se vaya educando el personal de cuándo genera comunicación tanto con los técnicos como con los clientes».

Este módulo es PURO (sin base de datos, sin Flask, sin correo): decide A QUIÉN se avisa y en qué estado queda cada aviso, arma las variables
y los textos de los correos, y juzga si la ficha debe recordar que a la OT terminada le falta enviarse al cliente. Las lecturas, las
escrituras y el envío viven en app.py (`_ot_com_*`, `_ot_terminada_*`). Pruebas: tests/test_ot_comunicacion_envio.py.

LAS TRES COMUNICACIONES
  1. OT TERMINADA (automática, al APROBAR EL CIERRE — la OT ya es definitiva y su documento no cambia): al cliente, a quien creó la OT / el
     ejecutivo / el responsable (los decide `_ot_gestion_destinatarios`, la ÚNICA función que sabe a quién avisa la gestión) y al técnico que
     la hizo (copia SIN montos). El correo al CLIENTE nace APAGADO y solo Daniel lo enciende (variable `OT_TERMINADA_CLIENTE_ACTIVA`: REGLA #22).
  2. ENVIAR OT POR CORREO (manual, para la gestión, como el «Enviar cotización»): destinatarios prellenados y editables, a la vista; después
     dice la verdad por destinatario («Se envió a…» / «No salió porque…»).
  3. DESTINATARIOS A LA VISTA: antes de cerrar (el técnico al firmar, la gestión al aprobar) y antes de crear la OT.

CONFIDENCIALIDAD (REGLAS #19 y #26): ninguno de estos correos lleva montos, costos, proveedores, OC ni autorizaciones. El documento que se enlaza
es el MISMO que ya ve el cliente (`/ot-firmada/<token>`, sin costos). A un técnico se le quita del texto libre lo que mencione plata.
"""
import datetime as _dt
import html as _html
import json as _json
import os
import re

import comm_correos_registro as _ccr
import ot_correos_tecnicos as _otc

# ── Identificadores ──────────────────────────────────────────────────────────────────────────────────────────────────────
ENV_CLIENTE_ACTIVA = "OT_TERMINADA_CLIENTE_ACTIVA"      # nace APAGADA; encenderla requiere el «sí» explícito de Daniel (REGLA #22)
TOKEN_DIAS = 30                                          # vigencia del enlace al documento firmado (stateless: HMAC con expiración)
MAX_DESTINATARIOS = 15                                   # igual que «Enviar cotización»
MAX_ASUNTO = 150
MAX_MENSAJE = 1000

MODULO_GENERAL = "mantenciones"                         # llave de paso del cliente y del equipo interno
MODULO_TECNICOS = _otc.MODULO_LLAVE                      # «ot_tecnicos»: la llave propia de los correos a técnicos
MODULO_PLANTILLAS = _otc.MODULO_PLANTILLAS               # las plantillas viven en el editor de «Servicio Técnico»

# Claves del REGISTRO DE CORREOS (comm_correos_registro.py): llevan `correo=` hacia la puerta única de envío.
CORREO_TERMINADA_CLIENTE = "ot_terminada_cliente"
CORREO_TERMINADA_RESPONSABLE = "ot_terminada_responsable"
CORREO_TERMINADA_TECNICO = "ot_tec_terminada"
CORREO_MANUAL = "ot_envio_manual"
CORREO_MANUAL_TECNICO = "ot_tec_envio_manual"
# Recordatorio de 24 horas (Daniel, 5-nov-2026): al cliente y al técnico, cada uno con su interruptor en Control de correos.
CORREO_RECORDATORIO_CLIENTE = "ot_recordatorio_cliente"
CORREO_RECORDATORIO_TECNICO = "ot_tec_recordatorio"
CORREOS = (CORREO_TERMINADA_CLIENTE, CORREO_TERMINADA_RESPONSABLE, CORREO_TERMINADA_TECNICO, CORREO_MANUAL, CORREO_MANUAL_TECNICO)
CORREOS_RECORDATORIO = (CORREO_RECORDATORIO_CLIENTE, CORREO_RECORDATORIO_TECNICO)       # los envía el barrido `_ot_recordatorio_barrido`

PLANTILLA_TERMINADA = "ot_terminada"
PLANTILLA_TERMINADA_INTERNO = "ot_terminada_interno"
PLANTILLA_TERMINADA_TECNICO = "ot_terminada_tecnico"
PLANTILLA_MANUAL = "ot_envio_manual"
PLANTILLA_RECORDATORIO = "ot_recordatorio_24h"
PLANTILLA_RECORDATORIO_TECNICO = "ot_recordatorio_24h_tecnico"
PLANTILLAS = (PLANTILLA_TERMINADA, PLANTILLA_TERMINADA_INTERNO, PLANTILLA_TERMINADA_TECNICO, PLANTILLA_MANUAL,
              PLANTILLA_RECORDATORIO, PLANTILLA_RECORDATORIO_TECNICO)

# Nombre del evento en email_log / Historial de Comunicaciones.
EVENTO_TERMINADA_CLIENTE = "ot_terminada_cliente"
EVENTO_TERMINADA_RESPONSABLE = "ot_terminada_responsable"
EVENTO_TERMINADA_TECNICO = "ot_terminada_tecnico"
EVENTO_MANUAL = "ot_envio_manual"
EVENTO_MANUAL_TECNICO = "ot_envio_manual_tecnico"
EVENTO_RECORDATORIO_CLIENTE = "ot_recordatorio_24h_cliente"
EVENTO_RECORDATORIO_TECNICO = "ot_recordatorio_24h_tecnico"

TIPO_TERMINADA = "terminada"        # fila de mant_ot_comunicaciones: el aviso automático de UNA aprobación del cierre
TIPO_MANUAL = "manual"              # fila de mant_ot_comunicaciones: un «Enviar OT por correo»
TIPO_CREACION = "creacion"          # fila de mant_ot_comunicaciones: lo que dijeron los correos al crear la OT

# ── Roles de quien recibe ────────────────────────────────────────────────────────────────────────────────────────────────
ROLES_TXT = {
    "cliente": "Cliente", "tecnico": "Técnico", "ejecutivo": "Ejecutivo", "creador": "Quien creó la OT",
    "responsable": "Responsable de la OT", "respaldo": "Gestión", "otro": "Otro destinatario",
}
GRUPO_DE_ROL = {"cliente": "cliente", "tecnico": "tecnico", "ejecutivo": "responsable", "creador": "responsable",
                "responsable": "responsable", "respaldo": "responsable", "otro": "responsable"}

# ── Estados de UN aviso (antes de enviarlo) ──────────────────────────────────────────────────────────────────────────────
EST_SALE = "sale"                       # se enviará
EST_APAGADO_CONFIG = "apagado_config"   # el correo al cliente nace apagado (variable de entorno)
EST_GENERAL = "general"                 # interruptor general de correos
EST_LLAVE = "llave"                     # llave de paso del módulo
EST_CORREO = "correo"                   # interruptor de ESE correo (Comunicaciones → Control de correos)
EST_SIN_CORREO = "sin_correo"           # la persona no tiene un correo registrado
EST_SIN_DOCUMENTO = "sin_documento"     # el documento firmado todavía no está completo: el enlace no abriría
EST_NO_APLICA = "no_aplica"             # trabajo interno / desinstalación: al cliente no se le escribe
EST_PLANTILLA = "plantilla"             # la plantilla del correo al técnico está apagada en el editor (así lo decide el editor: no sale)
# Los que SÍ se intentan enviar (la puerta de envío los frena y deja el rastro en el Historial); los demás ni se intentan.
ESTADOS_INTENTAR = (EST_SALE, EST_GENERAL, EST_LLAVE, EST_CORREO)
BLOQUEO_A_ESTADO = {"global": EST_GENERAL, "modulo": EST_LLAVE, "correo": EST_CORREO}

# ── Resultado de UN aviso (después de enviarlo) ──────────────────────────────────────────────────────────────────────────
RES_ENVIADO = "enviado"
RES_FRENADO = "frenado"
RES_FALLO = "fallo"
RES_NO_ENVIADO = "no_enviado"           # nunca se intentó (estaba apagado por configuración, sin correo, sin documento…)

# ── Estado del AVISO AL CLIENTE de una OT terminada (mant_ot_comunicaciones.cliente_estado) ──────────────────────────────
CLI_ENVIADO = "enviado"
CLI_APAGADO = "apagado"
CLI_FRENADO = "frenado"
CLI_SIN_CORREO = "sin_correo"
CLI_SIN_DOCUMENTO = "sin_documento"
CLI_FALLO = "fallo"
CLI_NO_APLICA = "no_aplica"
CLI_PENDIENTE = "pendiente"             # la fila se reservó pero el trabajo se cortó antes de terminar
# A la OT terminada le FALTA enviarse al cliente cuando el estado es uno de estos (la ficha lo recuerda, en ámbar, sin bloquear nada):
CLI_FALTA_ENVIAR = (CLI_APAGADO, CLI_FRENADO, CLI_SIN_CORREO, CLI_SIN_DOCUMENTO, CLI_FALLO, CLI_PENDIENTE)

_RX_CORREO = re.compile(r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$")      # el mismo criterio de «Enviar cotización» (Tickets)


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  Correos
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
def normalizar_correo(c):
    return str(c or "").strip().lower()


def correo_valido(c):
    c = normalizar_correo(c)
    return bool(c) and len(c) <= 180 and ".." not in c and bool(_RX_CORREO.match(c))


def cliente_activo(environ=None):
    """¿Está permitido el aviso automático al cliente por CONFIGURACIÓN del servidor? Desde el 5-nov-2026 (Daniel: «le avisemos al cliente a
    partir de ahora… déjalo todo por el front») el aviso se enciende y se apaga SOLO desde Comunicaciones → Control de correos (el
    interruptor de «OT terminada: aviso al cliente») y desde la llave de Servicio Técnico: ambos los muestra el plan de envío como
    «apagado» y la puerta única los respeta. La variable de entorno quedó solo como FRENO DE EMERGENCIA: si alguien la pone explícitamente en
    0/false/no/off, el aviso queda apagado aunque el interruptor del frente esté encendido; ausente o vacía = no frena nada."""
    env = os.environ if environ is None else environ
    return (env.get(ENV_CLIENTE_ACTIVA) or "").strip().lower() not in ("0", "false", "no", "off")


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  La aprobación del cierre y el documento
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
def cierre_clave(cerrada_at):
    """Identifica «ESTA aprobación del cierre» (idempotencia: el aviso no sale dos veces por la misma aprobación). Es el instante en que se
    aprobó (`mant_visitas.cerrada_at`). Si una OT se reabriera y se aprobara de nuevo, tendría otra clave y otro aviso."""
    if isinstance(cerrada_at, _dt.datetime):
        return cerrada_at.strftime("%Y-%m-%d %H:%M:%S")
    s = str(cerrada_at or "").strip()
    return s[:19] if s else "0"


def documento_listo(fila, es_interna=False):
    """¿El documento firmado de la OT ya está completo (cerrada + las tres firmas)? Espejo BARATO de las comprobaciones de
    `_ot_pdf_context` (la fuente de verdad, que arma la página pública `/ot-firmada/<token>`): sin esto, el enlace llevaría a una
    página que dice «esta orden todavía está en curso». Devuelve (listo, motivo). El servidor, al enviar, usa `_ot_pdf_context`."""
    f = fila or {}
    if str(f.get("estado") or "").strip().lower() != "cerrada":
        return False, "La OT todavía no está cerrada."
    if not f.get("firma_cliente_url") and not es_interna:
        return False, "Falta la firma del cliente."
    if not f.get("firma_tecnico_url"):
        return False, "Falta la firma del técnico."
    if not f.get("firma_supervisor_url"):
        return False, "Falta la firma del responsable/supervisor."
    return True, ""


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  A quién se avisa y en qué estado queda cada aviso
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
def motivo_estado(estado, *, llave_txt="Servicio Técnico", correo_nombre=""):
    """La frase (para una persona) de por qué un aviso no saldrá. '' si sale."""
    if estado == EST_SALE:
        return ""
    if estado == EST_APAGADO_CONFIG:
        return "El correo al cliente está apagado: no se enviará hasta que se active."
    if estado == EST_GENERAL:
        return "El envío de correos está apagado (interruptor general): no saldrá."
    if estado == EST_LLAVE:
        return f"El envío de correos de {llave_txt} está apagado: no saldrá."
    if estado == EST_CORREO:
        return (f"El correo «{correo_nombre}» está apagado en Comunicaciones → Control de correos: no saldrá."
                if correo_nombre else "Este correo está apagado en Comunicaciones → Control de correos: no saldrá.")
    if estado == EST_SIN_CORREO:
        return "No tiene un correo registrado."
    if estado == EST_SIN_DOCUMENTO:
        return "El documento firmado todavía no está completo, así que el enlace no abriría."
    if estado == EST_PLANTILLA:
        return "La plantilla de este correo está apagada en el editor de Comunicaciones: no saldrá."
    if estado == EST_NO_APLICA:
        return "No corresponde."
    return ""


def _item(grupo, rol, nombre, correo, estado, motivo="", proveedor=False, origen="", nota=""):
    return {"grupo": grupo, "rol": rol, "rol_txt": ROLES_TXT.get(rol, rol), "nombre": (nombre or "").strip(),
            "correo": normalizar_correo(correo), "estado": estado, "motivo": motivo,
            "intentar": estado in ESTADOS_INTENTAR, "saldra": estado == EST_SALE, "proveedor": bool(proveedor), "origen": origen,
            "nota": nota}


CLAVES_CIERRE = {"cliente": CORREO_TERMINADA_CLIENTE, "responsable": CORREO_TERMINADA_RESPONSABLE, "tecnico": CORREO_TERMINADA_TECNICO}


def armar_plan_cierre(*, cliente_nombre="", cliente_correos=(), aplica_cliente=True, motivo_no_cliente="", cliente_encendido=False,
                      documento_ok=True, gestion=(), tecnicos=None, fichas=(), bloqueos=None, llaves_txt=None, nombres_correo=None,
                      plantilla_tecnico_apagada=False, claves=None, notas=None):
    """El plan de «OT terminada»: la lista de personas que recibirán un aviso al aprobar el cierre, cada una con su rol y su estado.
    (Sirve también para «al crear esta OT se enviará…»: sin `gestion`, con `cliente_encendido=True` y las `claves` de los correos de creación.)

    · cliente_correos: [{"correo", "nombre", "origen"}] (el contacto de la OT primero y después los correos de la ficha del cliente).
    · aplica_cliente: False en un trabajo interno o en una desinstalación (al cliente no se le escribe: REGLA #22) → un solo renglón «no aplica»
      si hay `motivo_no_cliente`, ninguno si no.
    · cliente_encendido: el aviso al cliente nace apagado (ENV_CLIENTE_ACTIVA); apagado, cada correo del cliente queda «apagado por configuración».
    · gestion: lo que devuelve `_ot_gestion_destinatarios` [{"id","nombre","correo","por"}] (por: responsable | creador | ejecutivo | respaldo).
    · tecnicos: {correo: nombre}; fichas: los correos que son de la FICHA de una empresa externa (su nombre es el de un proveedor).
    · bloqueos: {"cliente"|"responsable"|"tecnico": None | 'global' | 'modulo' | 'correo'} (lo que diría hoy la puerta de envío).
    · plantilla_tecnico_apagada: la plantilla del aviso al técnico está apagada en el editor → ese correo no sale.
    · claves: {"cliente"|"responsable"|"tecnico": clave del correo en el registro} (para decir CUÁL correo está apagado). notas: {grupo: texto corto}.
    Un mismo correo recibe UN solo aviso (cliente > equipo de gestión > técnico). Devuelve la lista de renglones (ver `_item`)."""
    bloqueos = bloqueos or {}
    llaves_txt = llaves_txt or {}
    nombres_correo = nombres_correo or {}
    claves = dict(CLAVES_CIERRE, **(claves or {}))
    notas = notas or {}
    out, vistos = [], set()

    def _estado_por_puerta(grupo_bloqueo):
        b = bloqueos.get(grupo_bloqueo)
        return BLOQUEO_A_ESTADO.get(b, EST_SALE)

    def _motivo(estado, llave, grupo):
        return motivo_estado(estado, llave_txt=llaves_txt.get(llave, "Servicio Técnico"),
                             correo_nombre=nombres_correo.get(claves.get(grupo), ""))

    # ── 1) el cliente ──
    if not aplica_cliente:
        if motivo_no_cliente:
            out.append(_item("cliente", "cliente", cliente_nombre, "", EST_NO_APLICA, motivo_no_cliente))
    else:
        correos = []
        for c in cliente_correos or ():
            cc = normalizar_correo((c or {}).get("correo"))
            if correo_valido(cc) and cc not in {x["correo"] for x in correos}:
                correos.append({"correo": cc, "nombre": ((c or {}).get("nombre") or cliente_nombre or "").strip(),
                                "origen": (c or {}).get("origen") or ""})
        if not correos:
            out.append(_item("cliente", "cliente", cliente_nombre, "", EST_SIN_CORREO, motivo_estado(EST_SIN_CORREO)))
        for c in correos:
            if not cliente_encendido:
                estado = EST_APAGADO_CONFIG
            elif not documento_ok:
                estado = EST_SIN_DOCUMENTO
            else:
                estado = _estado_por_puerta("cliente")
            vistos.add(c["correo"])
            out.append(_item("cliente", "cliente", c["nombre"] or cliente_nombre, c["correo"], estado,
                             _motivo(estado, MODULO_GENERAL, "cliente"), origen=c["origen"], nota=notas.get("cliente", "")))

    # ── 2) quien creó la OT / el ejecutivo / el responsable ──
    for g in gestion or ():
        rol = (g or {}).get("por") or "respaldo"
        rol = rol if rol in ROLES_TXT else "respaldo"
        cc = normalizar_correo((g or {}).get("correo"))
        if not correo_valido(cc):
            out.append(_item("responsable", rol, (g or {}).get("nombre"), "", EST_SIN_CORREO, motivo_estado(EST_SIN_CORREO),
                             nota=notas.get("responsable", "")))
            continue
        if cc in vistos:
            continue
        vistos.add(cc)
        estado = _estado_por_puerta("responsable")
        out.append(_item("responsable", rol, (g or {}).get("nombre") or cc, cc, estado,
                         _motivo(estado, MODULO_GENERAL, "responsable"), nota=notas.get("responsable", "")))

    # ── 3) el técnico que la hizo ──
    en_fichas = {normalizar_correo(x) for x in (fichas or ())}
    for correo, nombre in sorted((tecnicos or {}).items(), key=lambda kv: (str(kv[1] or "").lower(), kv[0])):
        cc = normalizar_correo(correo)
        if not correo_valido(cc) or cc in vistos:
            continue
        vistos.add(cc)
        estado = _estado_por_puerta("tecnico")
        if estado == EST_SALE and plantilla_tecnico_apagada:
            estado = EST_PLANTILLA
        out.append(_item("tecnico", "tecnico", nombre or cc.split("@")[0], cc, estado, _motivo(estado, MODULO_TECNICOS, "tecnico"),
                         proveedor=(cc in en_fichas), nota=notas.get("tecnico", "")))
    return out


def a_intentar(items, grupo=None):
    """Los renglones que el envío debe intentar (la puerta de envío frena los que corresponda y deja el rastro)."""
    return [i for i in items if i.get("intentar") and (grupo is None or i.get("grupo") == grupo)]


def etiqueta(item):
    """«Nombre (Rol)» — o solo el rol cuando no hay nombre."""
    n = (item.get("nombre") or item.get("correo") or "").strip()
    r = item.get("rol_txt") or ROLES_TXT.get(item.get("rol"), "")
    return f"{n} ({r})" if n and r else (n or r)


GRUPO_TXT = {"cliente": "Cliente", "responsable": "Equipo de gestión", "tecnico": "Técnico"}


def resumen_se_enviara(items, titulo="Al aprobar el cierre se enviará esta OT a"):
    """Texto del aviso previo: «Al aprobar el cierre se enviará esta OT a: Nombre (Cliente) · Nombre (Técnico)». Después, UNA frase por
    cada aviso que NO saldrá y por qué (el correo al cliente está apagado, la llave cerrada, sin correo…). Sin nadie, lo dice."""
    saldran = [i for i in items if i.get("saldra")]
    partes = []
    if saldran:
        partes.append(f"{titulo}: " + " · ".join(etiqueta(i) for i in saldran) + ".")
    else:
        partes.append("Al aprobar el cierre no se enviará ningún correo.")
    vistos = set()
    for i in items:
        if i.get("saldra") or not i.get("motivo"):
            continue
        if i.get("estado") == EST_SIN_CORREO and i.get("grupo") != "cliente":
            k, sujeto = (i.get("grupo"), i.get("estado"), i.get("nombre")), etiqueta(i)      # «sin correo» es de UNA persona
        else:
            k, sujeto = (i.get("grupo"), i.get("estado")), GRUPO_TXT.get(i.get("grupo"), etiqueta(i))
        if k in vistos:
            continue
        vistos.add(k)
        partes.append(f"{sujeto}: {i['motivo']}")
    return " ".join(partes)


def para_tecnico(items):
    """Lo que ve un TÉCNICO del plan (REGLAS #19 y #26): nombre y rol, nunca el correo de otra persona ni el nombre de un proveedor."""
    out = []
    for i in items:
        if i.get("proveedor"):
            continue
        c = dict(i)
        c["correo"] = ""
        out.append(c)
    return out


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  Después de enviar: el estado del aviso al cliente y el recordatorio de la ficha
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
def estado_cliente(items):
    """El estado del AVISO AL CLIENTE de una OT terminada, a partir de los renglones del grupo «cliente» ya con su `resultado`."""
    cli = [i for i in items if i.get("grupo") == "cliente"]
    if not cli:
        return CLI_NO_APLICA
    if all(i.get("estado") == EST_NO_APLICA for i in cli):
        return CLI_NO_APLICA
    if any(i.get("resultado") == RES_ENVIADO for i in cli):
        return CLI_ENVIADO
    estados = {i.get("estado") for i in cli}
    if estados == {EST_APAGADO_CONFIG}:
        return CLI_APAGADO
    if estados == {EST_SIN_DOCUMENTO}:
        return CLI_SIN_DOCUMENTO
    if estados == {EST_SIN_CORREO}:
        return CLI_SIN_CORREO
    if any(i.get("resultado") == RES_FRENADO for i in cli):
        return CLI_FRENADO
    if any(i.get("resultado") == RES_FALLO for i in cli):
        return CLI_FALLO
    return CLI_PENDIENTE


def _detalle(det):
    if isinstance(det, (list, dict)):
        return det
    try:
        return _json.loads(det or "[]")
    except Exception:
        return []


FRANJA_TXT = {
    CLI_APAGADO: "El envío automático al cliente está apagado.",
    CLI_FRENADO: "El envío al cliente quedó frenado.",
    CLI_SIN_CORREO: "El cliente no tiene un correo registrado.",
    CLI_SIN_DOCUMENTO: "El documento firmado todavía no está completo, así que no se pudo enviar.",
    CLI_FALLO: "El correo al cliente no salió: el proveedor de correo no lo aceptó.",
    CLI_PENDIENTE: "El aviso al cliente no llegó a completarse.",
}


def franja_pendiente(filas, clave_actual):
    """¿La ficha debe recordar que la OT terminada todavía no se le envió al cliente? (ámbar, jamás bloquea).

    `filas` (de mant_ot_comunicaciones, la más nueva primero): {"id","tipo","cierre_clave","cliente_estado","detalle_json"}.
    Solo se recuerda cuando EXISTE el antecedente del aviso automático de ESTA aprobación del cierre (una OT cerrada antes de que existiera
    esto no tiene antecedente y NO se inventa uno) y el cliente no lo recibió ni por el aviso ni por un «Enviar OT por correo» posterior.
    Devuelve None o {"estado", "titulo", "detalle"}."""
    auto = None
    for f in filas or ():
        if f.get("tipo") == TIPO_TERMINADA and str(f.get("cierre_clave") or "") == str(clave_actual):
            auto = f
            break
    if not auto:
        return None
    estado = auto.get("cliente_estado") or CLI_PENDIENTE
    if estado not in CLI_FALTA_ENVIAR:
        return None
    try:
        id_auto = int(auto.get("id") or 0)
    except (TypeError, ValueError):
        id_auto = 0
    for f in filas or ():
        if f.get("tipo") != TIPO_MANUAL or f.get("cliente_estado") != CLI_ENVIADO:
            continue
        try:
            if int(f.get("id") or 0) > id_auto:
                return None                           # después del aviso, alguien se la envió al cliente a mano
        except (TypeError, ValueError):
            continue
    detalle = FRANJA_TXT.get(estado, "")
    if estado == CLI_FRENADO:
        for i in _detalle(auto.get("detalle_json")):
            if i.get("grupo") == "cliente" and i.get("resultado") == RES_FRENADO and i.get("motivo"):
                detalle = f"El envío al cliente quedó frenado: {str(i['motivo']).rstrip('.')}."
                break
    return {"estado": estado, "titulo": "La OT terminada todavía no se le envió al cliente", "detalle": detalle}


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  Envío manual: quién recibe y qué se le dice a cada rol
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
def normalizar_lista(correos, maximo=MAX_DESTINATARIOS):
    """(buenos, malos, repetidos, exceso) de una lista cruda de correos: minúsculas, sin repetidos y validados. `exceso` = cuántos pasan del tope."""
    buenos, malos, repetidos, vistos = [], [], 0, set()
    for c in correos or ():
        c = normalizar_correo(c)
        if not c:
            continue
        if not correo_valido(c):
            malos.append(c)
        elif c in vistos:
            repetidos += 1
        else:
            vistos.add(c)
            buenos.append(c)
    exceso = max(0, len(buenos) - maximo)
    return buenos[:maximo], malos, repetidos, exceso


def clasificar_pedidos(correos, *, conocidos, es_correo_tecnico):
    """A cada correo pedido le pone su ROL. `conocidos`: {correo: {"nombre","rol"}} de la propia OT (cliente, técnicos, gestión);
    `es_correo_tecnico(correo) -> bool`: ¿es de algún técnico aunque no sea de ESTA OT? (un técnico, interno o externo, SIEMPRE recibe la
    versión sin plata: REGLAS #19 y #26). Devuelve [{"correo","nombre","rol","rol_txt","tecnico"}]."""
    out = []
    for c in correos:
        k = conocidos.get(c)
        rol = (k or {}).get("rol") or "otro"
        tecnico = (rol == "tecnico")
        if not tecnico:
            try:
                tecnico = bool(es_correo_tecnico(c))
            except Exception:
                tecnico = True                        # ante la duda, se trata como técnico: callar plata es barato
            if tecnico and rol == "otro":
                rol = "tecnico"
        out.append({"correo": c, "nombre": (k or {}).get("nombre") or "", "rol": rol, "rol_txt": ROLES_TXT.get(rol, rol),
                    "tecnico": tecnico})
    return out


def asunto_para(asunto_pedido, por_defecto, para_tecnico=False):
    """El asunto de un correo manual. Vacío → el sugerido. A un técnico no le llega un asunto que mencione plata."""
    a = re.sub(r"\s+", " ", str(asunto_pedido or "")).strip()[:MAX_ASUNTO]
    if not a:
        return por_defecto
    if para_tecnico and _otc.hay_plata(a):
        return por_defecto
    return a


def mensaje_para(mensaje, para_tecnico=False):
    """El texto libre que escribe la persona. A un técnico se le quitan las líneas que mencionen plata, proveedores u OC."""
    m = str(mensaje or "").replace("\r\n", "\n").strip()[:MAX_MENSAJE]
    if not m:
        return ""
    if para_tecnico:
        return _otc.limpiar_texto_tecnico(m, MAX_MENSAJE)
    return m


def mensaje_html(texto):
    """El mensaje de la persona como bloque del correo (ESCAPADO: lo escribe una persona)."""
    t = str(texto or "").strip()
    if not t:
        return ""
    cuerpo = _html.escape(t, quote=True).replace("\n", "<br>")
    return ('<p style="margin:0 0 16px;padding:12px 16px;background:#f5f5f7;border-left:4px solid #dc2626;border-radius:4px;'
            f'font-size:14px;color:#374151;line-height:1.6">{cuerpo}</p>')


def resumen_destinatarios(items):
    """«Se enviará a: Nombre (Cliente) · Nombre (Técnico)» — el resumen EXPLÍCITO del envío manual."""
    if not items:
        return "No hay a quién enviarla."
    return "Se enviará a: " + " · ".join(etiqueta(i) for i in items) + "."


def resumen_resultados(resultados):
    """La verdad por destinatario después de enviar: [(texto, ok)]. «Se envió a X (Rol)» o «No salió a X (Rol) porque …»."""
    out = []
    for r in resultados or ():
        quien = etiqueta(r) or r.get("correo")
        if r.get("resultado") == RES_ENVIADO:
            out.append((f"Se envió a {quien} ({r.get('correo')}).", True))
        else:
            porque = (r.get("motivo") or "no se pudo enviar").rstrip(".")
            out.append((f"No salió a {quien} ({r.get('correo')}) porque {porque[:1].lower() + porque[1:]}.", False))
    return out


def estado_cliente_manual(resultados):
    """Para el recordatorio de la ficha: ¿el cliente recibió ESTE envío manual? 'enviado' si algún destinatario con rol cliente lo recibió;
    'frenado'/'fallo' si se le intentó y no salió; '' si en este envío no iba ningún cliente."""
    cli = [r for r in resultados or () if r.get("rol") == "cliente"]
    if not cli:
        return ""
    if any(r.get("resultado") == RES_ENVIADO for r in cli):
        return CLI_ENVIADO
    if any(r.get("resultado") == RES_FRENADO for r in cli):
        return CLI_FRENADO
    return CLI_FALLO


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  Constancia (bitácora y Historial)
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
_RES_TXT = {RES_ENVIADO: "enviado", RES_FRENADO: "frenado", RES_FALLO: "no salió", RES_NO_ENVIADO: "no se envió"}


def capitalizar(texto):
    """La frase con la primera letra en mayúscula y punto final ('' si no hay nada que decir)."""
    t = str(texto or "").strip()
    if not t:
        return ""
    t = t[0].upper() + t[1:]
    return t if t.endswith((".", "!", "?")) else t + "."


def _linea_resultado(i):
    """«Nombre (Rol) <correo> — enviado» / «… — frenado: motivo». Sin montos ni proveedores."""
    res = i.get("resultado")
    if res is None:
        res = RES_ENVIADO if i.get("saldra") else RES_NO_ENVIADO
    txt = _RES_TXT.get(res, res)
    extra = ""
    if res != RES_ENVIADO and i.get("motivo"):
        extra = f": {str(i['motivo']).rstrip('.')}"
    return f"{etiqueta(i)}{' <' + i['correo'] + '>' if i.get('correo') else ''} — {txt}{extra}"


def bitacora_envio(items, titulo):
    """Una línea para la bitácora de la OT: a quién, con qué rol y qué pasó con cada aviso. Sin montos ni proveedores."""
    partes = [_linea_resultado(i) for i in items]
    return f"{titulo}: " + (" · ".join(partes) if partes else "sin destinatarios")


def lineas_resultado(detalle):
    """Una línea por destinatario de un detalle ya guardado (JSON o lista de renglones), para el modal y la franja de la ficha."""
    return [_linea_resultado(i) for i in _detalle(detalle) if isinstance(i, dict)]


def detalle_json(items):
    """El detalle por destinatario que se guarda en mant_ot_comunicaciones (compacto; sin montos)."""
    campos = ("grupo", "rol", "nombre", "correo", "estado", "resultado", "motivo")
    filas = [{k: (str(i.get(k))[:200] if i.get(k) is not None else None) for k in campos if k in i} for i in items]
    return _json.dumps(filas, ensure_ascii=False)[:60000]


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  Variables y cuerpos de los correos (sembrados en comm_templates SOLO si no existen; también sirven de respaldo)
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
VARIABLES_DOC = (
    ("saludo", "«Hola Nombre,» o «Hola,» si no se sabe el nombre"),
    ("nombre_destinatario", "Nombre de quien recibe el correo"),
    ("ot", "N° de la orden de trabajo"),
    ("cliente", "Cliente (o «Trabajo interno»)"),
    ("tipo_mantencion", "Tipo de orden de trabajo"),
    ("fecha_realizada", "Fecha en que se hizo el trabajo"),
    ("tecnicos", "Los técnicos (personas) de la OT"),
    ("maquina", "Equipos de la OT"),
    ("direccion", "Dirección del trabajo"),
    ("link_ot_firmada", "Enlace al documento firmado (sin montos; vence a los 30 días)"),
    ("link_ot", "Enlace a la ficha de la OT (solo para quien tiene cuenta)"),
    ("mensaje_html", "El mensaje que escribió quien envía (solo «Enviar OT por correo»)"),
    ("aprobado_por", "Quién aprobó el cierre"),
    ("cerrada_en", "Cuándo se aprobó el cierre (hora Chile)"),
    ("resumen_envio_html", "Qué se envió y a quién (solo el aviso al equipo)"),
    ("encuesta_html", "Bloque «¿Cómo fue tu servicio?» con los números del 1 al 5 (solo correos al cliente; viaja únicamente con la encuesta activa)"),
    ("dias_vigencia", "Días que dura el enlace al documento"),
    ("enviado_por", "Quién envió el correo (solo «Enviar OT por correo»)"),
)


def esc(v):
    return _otc.esc(v)


# ── Bloques de VALOR para los correos (Daniel, 5-nov-2026: «que el mensaje provoque verse inteligente y aporte valor») ──────────────
# Cada bloque dice algo útil para quien lo lee (qué va a pasar, cómo dejar todo listo, cómo llegar, qué hacer) y sale de datos reales de la
# OT. Todo va ESCAPADO; nunca lleva montos, costos, proveedores ni OC (REGLAS #19 y #26). Sin datos suficientes, el bloque no se dibuja.
def caja_valor(titulo, cuerpo_html, color="#0ea5e9"):
    """Una caja gris con riel de color y un título pequeño: el mismo lenguaje de los demás correos de la OT."""
    return ('<table cellpadding="0" cellspacing="0" width="100%" style="background:#f5f5f7;'
            f'border-left:4px solid {color};border-radius:4px;padding:14px 18px;margin:18px 0">'
            '<tr><td style="font-size:12px;font-weight:800;letter-spacing:.1em;color:#4b5563;padding-bottom:6px">'
            f'{esc(str(titulo).upper())}</td></tr>'
            f'<tr><td style="font-size:13px;color:#444;line-height:1.6">{cuerpo_html}</td></tr></table>')


def lista_valor(items):
    """Lista con viñetas segura para correo; `items` son textos planos (se escapan)."""
    return ('<ul style="margin:0;padding-left:18px">'
            + "".join(f'<li style="margin:0 0 4px">{esc(i)}</li>' for i in items if str(i or "").strip()) + "</ul>")


def _plural(n, uno, varios):
    return uno if n == 1 else varios


def resumen_trabajo_html(ot, tecnicos_nombres=None):
    """«Resumen del trabajo» del correo de OT terminada: cuántos equipos se atendieron (SOLO la cantidad: los nombres van en el documento; Daniel, 6-nov),
    quién lo hizo y que el documento quedó firmado por las tres partes. Solo se manda cuando el documento está completo (`documento_listo`), así que la
    última línea es verdad."""
    # 6-nov (auditoría de diseño, «no me gusta tanta letra»): la tabla de abajo ya dice quién lo hizo, cuántos equipos y cuándo; aquí solo va lo que NO está en ningún otro
    # lado: que el documento quedó firmado por las tres partes. Una línea, no una caja con tres viñetas repetidas.
    return ('<p style="margin:0 0 14px;font-size:14px;line-height:1.6;color:#166534"><strong>&#10003; Documento firmado</strong>'
            '<span style="color:#444444"> por el técnico, por el cliente y por el responsable de Servicio Técnico.</span></p>')


# ── «Tu servicio, paso a paso» y la firma del equipo (Daniel, 5-nov-2026: «dejemos listo los correos ordenados, disparando el Dpto de SSTT, con algo
# de Marketing, gestionando un buen servicio y comunicativo») ─────────────────────────────────────────────────────────────────────────────────
# Cada correo al CLIENTE dice en qué punto del servicio está y qué sigue, con el mismo orden en todos: agendada → recordatorio → visita y firma →
# documento firmado. Es el mismo lenguaje de los demás bloques (riel de color), a prueba de clientes de correo (tablas, sin CSS moderno) y sin datos
# que no existan: lo que sigue solo se promete cuando el sistema de verdad lo va a hacer (lo decide quien arma el correo, mirando los interruptores).
PASOS_SERVICIO = (("agendada", "Visita agendada"), ("recordatorio", "Recordatorio"), ("visita", "Visita y firma"), ("documento", "Documento firmado"))


_REC_HECHO = ("#16a34a", "#166534")        # círculo y texto de un paso hecho (verde)
_REC_ACTUAL = ("#dc2626", "#b91c1c")       # el paso actual (rojo ILUS)
_REC_PENDIENTE = ("#d1d5db", "#6b7280")    # lo que falta (gris)
_REC_FUENTE = "Arial,Helvetica,sans-serif"


def _rec_circulo(color, texto, texto_color="#ffffff"):
    """Un círculo de 30 px con el número o el ✓ dentro. A prueba de Outlook de escritorio (motor Word): el color va en `bgcolor` y el relleno
    en la celda; `border-radius` es solo un adorno (donde no se respeta queda un cuadrado del mismo color, que se lee igual)."""
    return ('<table role="presentation" cellspacing="0" cellpadding="0" align="center"><tr>'
            f'<td width="30" height="30" align="center" valign="middle" bgcolor="{color}" '
            f'style="width:30px;height:30px;border-radius:15px;background:{color};color:{texto_color};font-family:{_REC_FUENTE};'
            f'font-size:14px;line-height:30px;font-weight:700;mso-line-height-rule:exactly">{texto}</td></tr></table>')


def boton_html(texto, url, color="#dc2626"):
    """Botón sólido para correo: el relleno y el color viven en la CELDA (`bgcolor` + padding en el `<td>`) y el `<a>` es de bloque; Outlook ignora
    el padding de un `<a>` y un botón armado así se veía diminuto (REGLA #66). Sin `url` no dibuja nada."""
    u = str(url or "").strip()
    if not u:
        return ""
    return ('<table role="presentation" cellspacing="0" cellpadding="0" align="center"><tr>'
            f'<td align="center" bgcolor="{color}" style="background:{color};border-radius:8px;padding:13px 26px">'
            f'<a href="{esc(u)}" target="_blank" style="display:block;color:#ffffff;text-decoration:none;font-family:{_REC_FUENTE};'
            f'font-size:14px;line-height:18px;font-weight:700">{esc(texto)}</a></td></tr></table>')


def recorrido_html(actual, que_sigue="", url_seguimiento=""):
    """«Tu servicio, paso a paso» de los correos al cliente: una fila de 4 pasos con un círculo numerado (hecho = verde con ✓, actual = rojo,
    pendiente = gris), una barra de progreso que se va llenando, el nombre de cada paso (≥ 12 px, corto) y, si se pasa `url_seguimiento`, el botón
    sólido «Ver seguimiento en vivo» hacia la página pública de la OT (REGLA #67). Sin URL no hay botón; `actual` fuera de PASOS_SERVICIO → ''.

    6-nov-2026 (Daniel, viendo Outlook: «colocar algo dinámico, bien bonito… no me gusta tanta letra»; los bloques se veían con espacios enormes):
    todo son tablas con `role="presentation"`, el color y el relleno van en cada `<td>` (`bgcolor`, `width`, `height`) y NUNCA `padding`/`margin`
    en una `<table>` ni en un `<a>` (Outlook de escritorio los ignora o los agranda). `que_sigue`: una frase corta; se escapa."""
    claves = [c for c, _n in PASOS_SERVICIO]
    if actual not in claves:
        return ""
    ia = claves.index(actual)
    circulos, barras, nombres = [], [], []
    for i, (clave, nombre) in enumerate(PASOS_SERVICIO):
        if i < ia or (i == ia and clave == claves[-1]):
            (fondo, txt), marca, peso = _REC_HECHO, "&#10003;", "700"
        elif i == ia:
            (fondo, txt), marca, peso = _REC_ACTUAL, str(i + 1), "800"
        else:
            (fondo, txt), marca, peso = _REC_PENDIENTE, str(i + 1), "600"
        circulos.append(f'<td width="25%" align="center" valign="middle" style="padding:0 2px 6px">{_rec_circulo(fondo, marca)}</td>')
        barras.append(f'<td width="25%" height="4" bgcolor="{fondo}" style="height:4px;line-height:4px;font-size:0;background:{fondo}">&nbsp;</td>')
        nombres.append(f'<td width="25%" align="center" valign="top" style="padding:7px 2px 0;font-family:{_REC_FUENTE};font-size:12px;'
                       f'line-height:16px;font-weight:{peso};color:{txt}">{esc(nombre)}</td>')
    sigue = (f'<tr><td colspan="4" style="padding:12px 2px 0;font-family:{_REC_FUENTE};font-size:13px;line-height:19px;color:#444444">'
             f'<strong style="color:#222222">Qué sigue:</strong> {esc(que_sigue)}</td></tr>') if (que_sigue or "").strip() else ""
    boton = boton_html("Ver seguimiento en vivo", url_seguimiento)
    boton_fila = f'<tr><td colspan="4" align="center" style="padding:16px 2px 0">{boton}</td></tr>' if boton else ""
    # El aire de arriba es una FILA separadora (nada de margin en la tabla: Outlook lo convierte en espacios enormes entre bloques).
    return ('<table role="presentation" width="100%" cellspacing="0" cellpadding="0">'
            '<tr><td colspan="4" height="18" style="height:18px;line-height:18px;font-size:0">&nbsp;</td></tr>'
            f'<tr><td colspan="4" style="padding:0 2px 10px;font-family:{_REC_FUENTE};font-size:12px;line-height:16px;font-weight:800;'
            'letter-spacing:.08em;color:#6b7280">TU SERVICIO, PASO A PASO</td></tr>'
            '<tr>' + "".join(circulos) + '</tr>'
            '<tr>' + "".join(barras) + '</tr>'
            '<tr>' + "".join(nombres) + '</tr>'
            + sigue + boton_fila + '</table>')


def firma_sstt_html():
    """La firma de los correos al cliente: quién le escribe (el equipo de Servicio Técnico) y un cierre cálido. Estática y sin datos."""
    return ('<p style="margin:22px 0 0;font-size:13px;line-height:1.6;color:#444"><strong style="color:#222">Equipo de Servicio Técnico</strong><br>'
            'ILUS Fitness · Gracias por confiar en nosotros.</p>')


def variables_ot(ot, *, saludo="", nombre_destinatario="", link_ot_firmada="", link_ot="", mensaje="", aprobado_por="",
                 cerrada_en="", resumen_envio_html="", enviado_por="", dias=TOKEN_DIAS, tecnicos_nombres=None, dia="", horario="",
                 recorrido="", encuesta=""):
    """Las variables de los correos de la OT, TODAS ESCAPADAS para HTML (las escribe una persona: razón social, dirección…); los `*_html`
    ya vienen armados. `ot`: id, numero_ot, cliente, tipo_label, fecha_realizada, fecha, direccion, equipos (lista de nombres)."""
    numero = ot.get("numero_ot") or f"V-{int(ot.get('id') or 0):05d}"
    nombres = [n for n in (tecnicos_nombres if tecnicos_nombres is not None else ot.get("tecnicos") or []) if n]
    nom = (nombre_destinatario or "").strip()
    sal = saludo or (f"Hola {nom}," if nom else "Hola,")
    fecha = ot.get("fecha_realizada") or ot.get("fecha")
    return {
        "saludo": esc(sal),
        "nombre_destinatario": esc(nom or "equipo"), "nombre_tecnico": esc(nom or "equipo"), "tecnico": esc(nom or "equipo"),
        "nombre_contacto": esc(nom or (ot.get("cliente") or "")),
        "ot": esc(numero), "cliente": esc(ot.get("cliente") or "Trabajo interno"),
        "tipo_mantencion": esc(ot.get("tipo_label") or "Visita"),
        "fecha_realizada": esc(_otc.fecha_txt(fecha) or "—"), "fecha": esc(_otc.fecha_txt(fecha) or "—"),
        "tecnicos": esc(", ".join(nombres) or "—"),
        "maquina": esc(_otc.equipos_txt(ot.get("equipos"))),
        "direccion": esc(ot.get("direccion") or "—"),
        "link_ot_firmada": esc(link_ot_firmada), "link_ot": esc(link_ot),
        "mensaje_html": mensaje_html(mensaje),
        "aprobado_por": esc(aprobado_por or "—"), "cerrada_en": esc(cerrada_en or "—"),
        "resumen_envio_html": resumen_envio_html or "",
        "dias_vigencia": esc(str(dias)), "enviado_por": esc(enviado_por or ""),
        "dia": esc(dia), "horario": esc(horario),      # solo los recordatorios de 24 horas los llenan (ot_recordatorio_24h.py)
        # Bloques de valor. El de OT terminada se arma acá; los del recordatorio los llena ot_recordatorio_24h.variables_recordatorio.
        "resumen_trabajo_html": resumen_trabajo_html(ot, nombres),
        "que_esperar_html": "", "preparacion_html": "", "contacto_fila": "", "como_llegar_html": "", "notas_html": "",
        "equipos_lista_html": "", "antes_de_salir_html": "",
        # «Tu servicio, paso a paso», la firma del equipo y la invitación de calendario (solo los correos al cliente las usan).
        "recorrido_html": recorrido or "", "firma_sstt_html": firma_sstt_html(), "invitacion_html": "",
        # Encuesta corta «¿Cómo fue tu servicio?» (6-nov-2026, REGLA #68): solo la llena quien arma un correo AL CLIENTE y solo con la encuesta activa
        # (`ot_encuesta.encuesta_activa()`); vacía en cualquier otro camino, y entonces el bloque no se dibuja.
        "encuesta_html": encuesta or "",
    }


def resumen_envio_html(items):
    """El bloque «Qué se envió» del aviso al equipo: una fila por grupo (cliente / técnico) con lo que pasó. Es lo que educa al personal
    sobre cuándo se genera comunicación con técnicos y clientes. Escapado."""
    def _fila(titulo, txt, ok):
        color = "#166534" if ok else "#92400e"
        return ('<tr><td style="padding:5px 0;font-size:13px;color:#555"><strong style="color:#222">'
                f'{esc(titulo)}:</strong>&nbsp; <span style="color:{color};font-weight:600">{esc(txt)}</span></td></tr>')

    filas = []
    cli = [i for i in items if i.get("grupo") == "cliente"]
    if cli:
        ok = [i for i in cli if i.get("resultado") == RES_ENVIADO]
        if ok:
            filas.append(_fila("Cliente", "se le envió el documento firmado a " + ", ".join(i["correo"] for i in ok), True))
        else:
            motivo = next((i.get("motivo") for i in cli if i.get("motivo")), "") or "no se le envió"
            filas.append(_fila("Cliente", "todavía no se le envió. " + str(motivo).rstrip(".") + ". "
                               "Puedes enviárselo con «Enviar OT por correo» en la ficha de la OT.", False))
    tec = [i for i in items if i.get("grupo") == "tecnico" and not i.get("proveedor")]
    if tec:
        ok = [i for i in tec if i.get("resultado") == RES_ENVIADO]
        if ok:
            filas.append(_fila("Técnico", "recibió su copia (sin montos): " + ", ".join(i["nombre"] or i["correo"] for i in ok), True))
        else:
            filas.append(_fila("Técnico", "su copia no salió" + (": " + str(tec[0].get("motivo")).rstrip(".") if tec[0].get("motivo") else ""), False))
    if not filas:
        return ""
    return ('<table cellpadding="0" cellspacing="0" width="100%" style="background:#f5f5f7;border-left:4px solid #6b7280;'
            'border-radius:4px;padding:12px 18px;margin:18px 0"><tr><td style="font-size:11px;font-weight:800;letter-spacing:.1em;'
            'color:#6b7280;padding-bottom:4px">QUÉ SE ENVIÓ</td></tr>' + "".join(filas) + "</table>")


# ── HTML de las plantillas (mismo lenguaje de los correos a técnicos) ──
def _p(texto):
    return f'<p style="margin:0 0 14px;font-size:14px;color:#444;line-height:1.65">{texto}</p>'


def _tabla(color_borde, filas):
    return ('<table cellpadding="0" cellspacing="0" width="100%" style="background:#f5f5f7;'
            f'border-left:4px solid {color_borde};border-radius:4px;padding:14px 18px;margin:18px 0">' + filas + '</table>')


def _f(etiqueta_, token, destacado=False):
    val = (f'<span style="color:#dc2626;font-weight:700">{token}</span>' if destacado else token)
    return ('<tr><td style="padding:5px 0;font-size:13px;color:#555"><strong style="color:#222">'
            f'{etiqueta_}:</strong>&nbsp; {val}</td></tr>')


def _boton(texto, token):
    return (f'<p style="margin:14px 0 0;font-size:13px"><a href="{token}" style="background:#dc2626;color:#fff;'
            'padding:10px 22px;border-radius:6px;text-decoration:none;font-weight:bold;display:inline-block">'
            f'{texto}</a></p>')


def _pie(texto):
    return f'<p style="margin:18px 0 0;font-size:12px;color:#888;line-height:1.5">{texto}</p>'


_FILAS = (_f("OT", "{{ot}}", True) + _f("Cliente", "{{cliente}}") + _f("Trabajo", "{{tipo_mantencion}}")
          + _f("Terminada el", "{{fecha_realizada}}") + _f("Técnico(s)", "{{tecnicos}}") + _f("Equipos", "{{maquina}}")
          + _f("Dirección", "{{direccion}}"))

# ── Versiones ANTERIORES (v1) de las tres plantillas que se mejoraron el 5-nov-2026 («que aporte valor»). Si en la base una de estas sigue
# EXACTAMENTE igual a su v1 —nadie la editó en Comunicaciones—, el arranque la pasa a la v2; si una persona la tocó, queda como la dejó.
_TERMINADA_V1 = (
    "Tu orden de trabajo {{ot}} quedó terminada",
    _p("{{saludo}}")
    + _p("Terminamos el trabajo de la orden de trabajo <strong>{{ot}}</strong>. Aquí está el documento firmado, con lo que se hizo, "
         "los equipos revisados y las fotos.")
    + _tabla("#16a34a", _FILAS)
    + _boton("Ver la orden de trabajo firmada", "{{link_ot_firmada}}")
    + _pie("El enlace es personal y se mantiene disponible por {{dias_vigencia}} días. Si algo no calza, responde este correo o crea "
           "un ticket de soporte."))
_RECORDATORIO_V1 = (
    "Recordatorio: tu visita técnica es {{dia}} ({{ot}})",
    _p("{{saludo}}")
    + _p("Te recordamos que <strong>{{dia}}</strong> nuestro equipo técnico irá a realizar el trabajo de la orden de trabajo "
         "<strong>{{ot}}</strong>.")
    + _tabla("#f59e0b", _f("OT", "{{ot}}", True) + _f("Trabajo", "{{tipo_mantencion}}") + _f("Fecha", "{{fecha}}")
             + _f("Horario", "{{horario}}") + _f("Técnico(s)", "{{tecnicos}}") + _f("Equipos", "{{maquina}}")
             + _f("Dirección", "{{direccion}}"))
    + _p("Para que todo salga bien, procura que haya una persona que pueda recibir al equipo y darle acceso a los equipos del trabajo.")
    + _pie("¿Necesitas cambiar la fecha o el acceso? Responde este correo o crea un ticket de soporte y lo coordinamos con tiempo."))
_RECORDATORIO_TECNICO_V1 = (
    "Recordatorio: {{dia}} tienes la {{ot}} · {{cliente}}",
    _p("{{saludo}}")
    + _p("Te recordamos que <strong>{{dia}}</strong> tienes la orden de trabajo <strong>{{ot}}</strong>.")
    + _tabla("#f59e0b", _f("OT", "{{ot}}", True) + _f("Cliente", "{{cliente}}") + _f("Trabajo", "{{tipo_mantencion}}")
             + _f("Fecha", "{{fecha}}") + _f("Horario", "{{horario}}") + _f("Dirección", "{{direccion}}")
             + _f("Equipos", "{{maquina}}"))
    + _boton("Ver la OT", "{{link_ot}}")
    + _pie("Si no puedes asistir, avisa lo antes posible a tu coordinador de Servicio Técnico."))

# v2 (5-nov-2026, «aporte de valor»): las dos plantillas del cliente tal como salieron en esa versión. Las mejora el recorrido «Tu servicio, paso a paso» y la
# firma del equipo (v3): si en la base siguen EXACTAMENTE iguales a su v1 o a su v2, el arranque las pasa a la v3; si alguien las editó, no se tocan.
_TERMINADA_V2 = (
    "Tu orden de trabajo {{ot}} quedó terminada",
    _p("{{saludo}}")
    + _p("Terminamos el trabajo de la orden de trabajo <strong>{{ot}}</strong>. Aquí está el documento firmado, con lo que se hizo, "
         "los equipos revisados y las fotos.")
    + "{{resumen_trabajo_html}}"
    + _tabla("#16a34a", _FILAS)
    + _boton("Ver la orden de trabajo firmada", "{{link_ot_firmada}}")
    + _pie("El enlace es personal y se mantiene disponible por {{dias_vigencia}} días. Si algo no calza, responde este correo o crea "
           "un ticket de soporte."))
_RECORDATORIO_V2 = (
    "Recordatorio: tu visita técnica es {{dia}} ({{ot}})",
    _p("{{saludo}}")
    + _p("Te recordamos que <strong>{{dia}}</strong> nuestro equipo técnico irá a realizar el trabajo de la orden de trabajo "
         "<strong>{{ot}}</strong>.")
    + _tabla("#f59e0b", _f("OT", "{{ot}}", True) + _f("Trabajo", "{{tipo_mantencion}}") + _f("Fecha", "{{fecha}}")
             + _f("Horario", "{{horario}}") + _f("Técnico(s)", "{{tecnicos}}") + _f("Equipos", "{{maquina}}")
             + _f("Dirección", "{{direccion}}"))
    + "{{que_esperar_html}}"
    + "{{preparacion_html}}"
    + _pie("¿Necesitas cambiar la fecha o el acceso? Responde este correo o crea un ticket de soporte y lo coordinamos con tiempo."))


# v3 (5-nov-2026, «aporte de valor» + recorrido + firma): TERMINADA y RECORDATORIO tal como salieron en #281, con «responde este correo» en el pie. Daniel (probó
# un correo, 5-nov): «lo que sí o sí debe hacer es crear un ticket, siempre crear un ticket» → la v4 manda SIEMPRE al ticket. Las construye la misma función que
# la vigente (`_cuerpo_terminada` / `_cuerpo_recordatorio`), con el pie de entonces: así la v3 es EXACTAMENTE lo que quedó sembrado y se puede comparar.
def _cuerpo_terminada(pie, encuesta=False):
    # `encuesta=True` (6-nov-2026, REGLA #68): «¿Cómo fue tu servicio?» va DESPUÉS del botón del documento y del recorrido, y antes del pie y de la firma
    # del equipo. Las versiones congeladas (V3 a V5) se arman con `encuesta=False`: son exactamente lo que salió sembrado entonces.
    return (_p("{{saludo}}")
            + _p("Terminamos el trabajo de la orden de trabajo <strong>{{ot}}</strong>. Aquí está el documento firmado, con lo que se hizo, "
                 "los equipos revisados y las fotos.")
            + "{{resumen_trabajo_html}}"
            + _tabla("#16a34a", _FILAS)
            + _boton("Ver la orden de trabajo firmada", "{{link_ot_firmada}}")
            + "{{recorrido_html}}"
            + ("{{encuesta_html}}" if encuesta else "")
            + _pie(pie)
            + "{{firma_sstt_html}}")


def _cuerpo_manual(encuesta=False):
    """«Enviar OT por correo». Con `encuesta=True` suma «¿Cómo fue tu servicio?» después del botón del documento (solo se dibuja para el CLIENTE y con la
    encuesta activa: para cualquier otro destinatario la variable va vacía)."""
    return (_p("{{saludo}}")
            + "{{mensaje_html}}"
            + _p("Te compartimos la orden de trabajo <strong>{{ot}}</strong>, ya terminada y firmada. Con el enlace de abajo puedes ver el "
                 "documento completo: lo que se hizo, los equipos revisados y las fotos.")
            + _tabla("#16a34a", _FILAS)
            + _boton("Ver la orden de trabajo firmada", "{{link_ot_firmada}}")
            + ("{{encuesta_html}}" if encuesta else "")
            + _pie("El enlace se mantiene disponible por {{dias_vigencia}} días. Lo envió {{enviado_por}} desde ILUS Fitness."))


def _cuerpo_recordatorio(pie):
    return (_p("{{saludo}}")
            + _p("Te recordamos que <strong>{{dia}}</strong> nuestro equipo técnico irá a realizar el trabajo de la orden de trabajo "
                 "<strong>{{ot}}</strong>.")
            + _tabla("#f59e0b", _f("OT", "{{ot}}", True) + _f("Trabajo", "{{tipo_mantencion}}") + _f("Fecha", "{{fecha}}")
                     + _f("Horario", "{{horario}}") + _f("Técnico(s)", "{{tecnicos}}") + _f("Equipos", "{{maquina}}")
                     + _f("Dirección", "{{direccion}}"))
            + "{{recorrido_html}}"
            + "{{que_esperar_html}}"
            + "{{preparacion_html}}"
            + _pie(pie)
            + "{{firma_sstt_html}}")


_TERMINADA_V3 = (
    "Tu orden de trabajo {{ot}} quedó terminada",
    _cuerpo_terminada("El enlace es personal y se mantiene disponible por {{dias_vigencia}} días. Si algo no calza, responde este correo o crea "
                      "un ticket de soporte."))
_RECORDATORIO_V3 = (
    "Recordatorio: tu visita técnica es {{dia}} ({{ot}})",
    _cuerpo_recordatorio("¿Necesitas cambiar la fecha o el acceso? Responde este correo o crea un ticket de soporte y lo coordinamos con tiempo."))

# REGLA #62 (5-nov-2026): al cliente SIEMPRE se le indica crear un ticket de soporte (el botón «Crear ticket de soporte» va al final de TODO correo de ILUS,
# `_ilus_email_master`), nunca «responde este correo»: el ticket deja la solicitud registrada y la atiende el equipo. Una frase única para todas las plantillas.
# 6-nov-2026 (Daniel: «no me gusta tanta letra»; «nunca se responde al correo»): la frase pasó a su versión CORTA. La larga de ayer (v4) queda abajo, congelada, solo
# para reconocer las plantillas que quedaron sembradas con ella y actualizarlas si nadie las editó.
COMO_CREAR_TICKET = "crea un ticket de soporte con el botón de abajo"
_COMO_CREAR_TICKET_V4 = "crea un ticket de soporte con el botón «Crear ticket de soporte» que está al final de este correo"
_PORQUE_TICKET_V4 = "así tu solicitud queda registrada y la atiende nuestro equipo de Servicio Técnico"

_TERMINADA_V4 = (
    "Tu orden de trabajo {{ot}} quedó terminada",
    _cuerpo_terminada("El enlace es personal y se mantiene disponible por {{dias_vigencia}} días. Si algo no calza o tienes una consulta, "
                      + _COMO_CREAR_TICKET_V4 + ": " + _PORQUE_TICKET_V4 + "."))
_RECORDATORIO_V4 = (
    "Recordatorio: tu visita técnica es {{dia}} ({{ot}})",
    _cuerpo_recordatorio("¿Necesitas cambiar la fecha o el acceso? " + _COMO_CREAR_TICKET_V4[0].upper() + _COMO_CREAR_TICKET_V4[1:]
                         + " y lo coordinamos con tiempo: " + _PORQUE_TICKET_V4 + "."))

# v5 de «OT terminada» (6-nov-2026, #290): la versión vigente ANTES de la encuesta corta, con el pie corto del ticket y sin `{{encuesta_html}}`. Si en la base una
# plantilla sigue EXACTAMENTE igual a esta, nadie la editó y el arranque la pasa a la vigente (REGLA #68); si alguien la tocó en Comunicaciones, queda como la dejó.
_PIE_TERMINADA = ("El enlace es personal y está disponible por {{dias_vigencia}} días. ¿Algo no calza? "
                  + COMO_CREAR_TICKET[0].upper() + COMO_CREAR_TICKET[1:] + ".")
_TERMINADA_V5 = (
    "Tu orden de trabajo {{ot}} quedó terminada",
    _cuerpo_terminada(_PIE_TERMINADA))


def invitacion_html():
    """El aviso de la invitación de calendario adjunta (solo cuando el correo de verdad la lleva)."""
    return _p("<strong>Agrégala a tu calendario:</strong> te enviamos la invitación; acéptala y la visita quedará en tu agenda.")


# Nota: ninguna de estas plantillas lleva montos, costos, proveedores ni OC (REGLAS #19 y #26).
SEMILLAS = {
    # Al CLIENTE, al aprobar el cierre (se enciende y se apaga en Control de correos). v2 (5-nov-2026): con el «Resumen del trabajo»; v3: con el recorrido
    # «Tu servicio, paso a paso» (todo hecho) y la firma del equipo de Servicio Técnico.
    # v4 (5-nov-2026, Daniel: «siempre crear un ticket»): el pie manda al ticket, nunca a «responde este correo».
    # v6 (6-nov-2026, Daniel: «agregar una encuesta con elegancia y sin aburrir»): suma «¿Cómo fue tu servicio?» (`{{encuesta_html}}`, vacío mientras la encuesta no esté activa).
    PLANTILLA_TERMINADA: (
        "Tu orden de trabajo {{ot}} quedó terminada",
        _cuerpo_terminada(_PIE_TERMINADA, encuesta=True)),
    # Al equipo (quien creó la OT, el ejecutivo o el responsable): qué pasó y qué se envió.
    PLANTILLA_TERMINADA_INTERNO: (
        "{{ot}} terminada · {{cliente}}",
        _p("{{saludo}}")
        + _p("La orden de trabajo <strong>{{ot}}</strong> quedó <strong>terminada y cerrada</strong>. El cierre lo aprobó {{aprobado_por}} "
             "el {{cerrada_en}}.")
        + _tabla("#0ea5e9", _FILAS)
        + "{{resumen_envio_html}}"
        + _boton("Ver la OT", "{{link_ot}}")),
    # Al técnico que la hizo: su copia, SIN montos ni proveedores.
    PLANTILLA_TERMINADA_TECNICO: (
        "{{ot}} terminada — gracias",
        _p("{{saludo}}")
        + _p("La orden de trabajo <strong>{{ot}}</strong> que realizaste quedó <strong>cerrada y aprobada</strong>. Gracias por el trabajo.")
        + _tabla("#16a34a", _FILAS)
        + _boton("Ver la OT", "{{link_ot}}")),
    # El envío manual («Enviar OT por correo»): el mismo texto para todos, con el mensaje opcional de quien envía.
    # v2 (6-nov-2026): con la encuesta corta (`{{encuesta_html}}`, que solo se llena para el cliente y con la encuesta activa).
    PLANTILLA_MANUAL: (
        "Orden de trabajo {{ot}} · {{cliente}}",
        _cuerpo_manual(encuesta=True)),
    # RECORDATORIO DE 24 HORAS (Daniel, 5-nov-2026). Al CLIENTE: sin enlace a la ficha interna (pide sesión); si algo cambia, responde el correo.
    # v2 (5-nov-2026): con «Qué va a pasar» (quién llega, a qué equipos, cómo cierra la visita) y «Para dejar todo listo» según el tipo de trabajo.
    # v4 (5-nov-2026, Daniel: «siempre crear un ticket»): cambiar la fecha o el acceso se pide con un ticket.
    PLANTILLA_RECORDATORIO: (
        "Recordatorio: tu visita técnica es {{dia}} ({{ot}})",
        _cuerpo_recordatorio("¿Necesitas cambiar la fecha o el acceso? " + COMO_CREAR_TICKET[0].upper() + COMO_CREAR_TICKET[1:] + ".")),
    # Al TÉCNICO (o a la empresa externa): su recordatorio, SIN montos ni proveedores. La dirección, el contacto y «qué hay que hacer» los reemplaza
    # el código mientras el Anexo no esté firmado. v2 (5-nov-2026): con «Cómo llegar», el contacto en terreno, lo que hay que hacer, la lista de
    # equipos y «Antes de salir».
    PLANTILLA_RECORDATORIO_TECNICO: (
        "Recordatorio: {{dia}} tienes la {{ot}} · {{cliente}}",
        _p("{{saludo}}")
        + _p("Te recordamos que <strong>{{dia}}</strong> tienes la orden de trabajo <strong>{{ot}}</strong>.")
        + _tabla("#f59e0b", _f("OT", "{{ot}}", True) + _f("Cliente", "{{cliente}}") + _f("Trabajo", "{{tipo_mantencion}}")
                 + _f("Fecha", "{{fecha}}") + _f("Horario", "{{horario}}") + _f("Dirección", "{{direccion}}")
                 + "{{contacto_fila}}" + _f("Equipos", "{{maquina}}"))
        + "{{como_llegar_html}}"
        + "{{notas_html}}"
        + "{{equipos_lista_html}}"
        + "{{antes_de_salir_html}}"
        + _boton("Ver la OT", "{{link_ot}}")
        + _pie("Si no puedes asistir, avisa lo antes posible a tu coordinador de Servicio Técnico.")),
}

# slug → versiones anteriores sin editar que el arranque puede reemplazar por la vigente (ver `_ensure_comm_template_ot_comunicacion`).
_MANUAL_V1 = (
    "Orden de trabajo {{ot}} · {{cliente}}",
    _cuerpo_manual())          # «Enviar OT por correo» tal como salió sembrado hasta el 6-nov-2026 (sin la encuesta)

SEMILLAS_ANTERIORES = {
    PLANTILLA_TERMINADA: (_TERMINADA_V1, _TERMINADA_V2, _TERMINADA_V3, _TERMINADA_V4, _TERMINADA_V5),
    PLANTILLA_MANUAL: (_MANUAL_V1,),
    PLANTILLA_RECORDATORIO: (_RECORDATORIO_V1, _RECORDATORIO_V2, _RECORDATORIO_V3, _RECORDATORIO_V4),
    PLANTILLA_RECORDATORIO_TECNICO: (_RECORDATORIO_TECNICO_V1,),
}

# La plantilla del PRIMER correo que ve el cliente («OT creada: visita agendada», registro `ot_creada_cliente`) es más antigua que este módulo: vive en
# `app.py` (`_mantenciones_tpl_seed`) y también la usan el botón «Enviar email» de la ficha y la copia al técnico. Su versión nueva se guarda aparte (no es
# una de las plantillas que este módulo siembra) y el arranque la pone SOLO si la fila sigue idéntica a la semilla original. Variables nuevas, todas vacías en
# los caminos que no las arman (el reemplazo borra lo que no se provee): `invitacion_html`, `recorrido_html`, `que_esperar_html`, `preparacion_html`,
# `firma_sstt_html`.
PLANTILLA_VISITA_AGENDADA = "visita_agendada"
PLANTILLA_OT_COMPLETADA = "ot_completada"


def _cuerpo_visita_agendada(pie):
    return ('<p style="margin:0 0 16px;font-size:15px;color:#dc2626;font-weight:700">Estimado/a {{cliente}}</p>'
            + _p("Te confirmamos que <strong>agendamos la visita técnica</strong> de la orden <strong>{{ot}}</strong>.")
            + "{{invitacion_html}}"
            + _tabla("#dc2626", _f("N° Orden", "{{ot}}", True) + _f("Tipo", "{{tipo_mantencion}}") + _f("Fecha", "<strong>{{fecha}}</strong> · {{horario}}")
                     + _f("Técnico(s)", "{{tecnico}}") + _f("Equipos", "{{maquina}}") + _f("Dirección", "{{direccion}}"))
            + "{{recorrido_html}}"
            + "{{que_esperar_html}}"
            + "{{preparacion_html}}"
            + _pie(pie)
            + "{{firma_sstt_html}}")


# «OT completada» (`ot_completada`): el correo del botón «Enviar email» de la ficha de la OT (con el informe adjunto). Es la plantilla más antigua de la OT y su
# semilla original vive en `app.py` (`_mantenciones_tpl_seed`). Solo cambia su última frase: «responde este correo» → el ticket (Daniel, 5-nov-2026).
def _cuerpo_ot_completada(cierre):
    return ('<p style="margin:0 0 16px;font-size:15px;color:#dc2626;font-weight:700">Estimado/a {{cliente}}</p>'
            '<p style="margin:0 0 14px;font-size:14px;color:#444;line-height:1.65">'
            'El servicio técnico se ha completado exitosamente. Encontrarás el detalle a continuación.</p>'
            '<table cellpadding="0" cellspacing="0" width="100%" style="background:#dcfce7;border-left:4px solid #16a34a;'
            'border-radius:4px;padding:14px 18px;margin:18px 0">'
            '<tr><td style="padding:5px 0;font-size:13px;color:#555"><strong style="color:#222">OT:</strong>&nbsp; '
            '<span style="color:#16a34a;font-weight:700">{{ot}}</span></td></tr>'
            '<tr><td style="padding:5px 0;font-size:13px;color:#555"><strong style="color:#222">Tipo:</strong>&nbsp; {{tipo_mantencion}}</td></tr>'
            '<tr><td style="padding:5px 0;font-size:13px;color:#555"><strong style="color:#222">Técnico:</strong>&nbsp; {{tecnico}}</td></tr>'
            '<tr><td style="padding:5px 0;font-size:13px;color:#555"><strong style="color:#222">Equipo:</strong>&nbsp; {{maquina}}</td></tr>'
            '</table>'
            '<p style="margin:0;font-size:13px;color:#444;line-height:1.6">'
            'Gracias por confiar en ILUS Fitness para el cuidado de tus equipos. ' + cierre + '</p>')


# v2 de «OT creada» (#281, con la invitación y el recorrido): su pie decía «responde este correo o contacta a tu ejecutivo». La v3 manda siempre al ticket.
_VISITA_AGENDADA_V2 = (
    "Tu visita técnica está agendada — {{ot}} ({{fecha}})",
    _cuerpo_visita_agendada("Si necesitas reagendar o cancelar, responde este correo o contacta a tu ejecutivo."))

# v3 de «OT creada» y v2 de «OT completada» (#283–#286, 5-nov): su pie ya mandaba al ticket pero con una frase LARGA; la de hoy es corta (Daniel, «no me gusta tanta letra»).
_VISITA_AGENDADA_V3 = (
    "Tu visita técnica está agendada — {{ot}} ({{fecha}})",
    _cuerpo_visita_agendada("Si necesitas reagendar o cancelar, " + _COMO_CREAR_TICKET_V4 + ": " + _PORQUE_TICKET_V4 + "."))
_OT_COMPLETADA_V2 = (
    "OT {{ot}} completada — Reporte adjunto",
    _cuerpo_ot_completada("Si tienes alguna observación, " + _COMO_CREAR_TICKET_V4 + " y te contactaremos a la brevedad."))

SEMILLAS_EXTRA = {
    PLANTILLA_VISITA_AGENDADA: (
        "Tu visita técnica está agendada — {{ot}} ({{fecha}})",
        _cuerpo_visita_agendada("¿Necesitas reagendar o cancelar? " + COMO_CREAR_TICKET[0].upper() + COMO_CREAR_TICKET[1:] + ".")),
    PLANTILLA_OT_COMPLETADA: (
        "{{ot}} completada — Reporte adjunto",
        _cuerpo_ot_completada("Si tienes alguna observación, " + COMO_CREAR_TICKET + ".")),
}
# Versiones de SEMILLAS_EXTRA que ya estuvieron sembradas y que NO son la original de `app.py` (esa la agrega el arranque al principio de la cadena).
# «OT completada» tal como quedó sembrada en producción ANTES del rebrand (commit 5df79308 cambió «ILUS Sport & Health» por «ILUS Fitness» solo en la semilla del
# código; la fila de la base seguía con el nombre viejo, por eso la comparación con la semilla actual no la reconocía como «sin editar»). Es la semilla original
# con ese único cambio de marca; pasarla a la versión nueva además corrige la marca (REGLA #0).
_OT_COMPLETADA_MARCA_ANTIGUA = (
    "OT {{ot}} completada — Reporte adjunto",
    _cuerpo_ot_completada("Si tienes alguna observación, responde este correo y te contactaremos a la brevedad.").replace(
        "ILUS Fitness", "ILUS Sport & Health"))

# …y todavía más atrás: la fila de producción es de ANTES del renombre «Mantenciones → Servicio Técnico» (commit e45349fe), que cambió «El servicio de mantención se ha
# completado» por «El servicio técnico se ha completado». Verificado contra la fila real (solo lectura): con estos DOS cambios de texto es idéntica, letra por letra.
_OT_COMPLETADA_ORIGINAL_DE_PRODUCCION = (
    _OT_COMPLETADA_MARCA_ANTIGUA[0],
    _OT_COMPLETADA_MARCA_ANTIGUA[1].replace("El servicio técnico se ha completado", "El servicio de mantención se ha completado"))

SEMILLAS_EXTRA_ANTERIORES = {
    PLANTILLA_VISITA_AGENDADA: (_VISITA_AGENDADA_V2, _VISITA_AGENDADA_V3),
    PLANTILLA_OT_COMPLETADA: (_OT_COMPLETADA_MARCA_ANTIGUA, _OT_COMPLETADA_ORIGINAL_DE_PRODUCCION, _OT_COMPLETADA_V2),
}


def render_simple(texto, variables):
    """Mismo reemplazo que `_render_comm_template` (sin Jinja): {{var}} → valor; lo desconocido se borra."""
    return _otc.render_simple(texto, variables)


def armar_correo(slug, variables, plantilla=None):
    """(asunto, cuerpo) de un correo de la OT. `plantilla` = (asunto, cuerpo) YA reemplazado por el editor de Comunicaciones, o None para
    usar la semilla del código (mismo contenido). El asunto es texto plano: se deshace el escape de las variables."""
    if plantilla and ((plantilla[0] or "").strip() or (plantilla[1] or "").strip()):
        asunto, cuerpo = plantilla
    else:
        a, c = SEMILLAS[slug]
        asunto, cuerpo = render_simple(a, variables), render_simple(c, variables)
    asunto = _html.unescape((asunto or "").strip()) or "Orden de trabajo"
    return asunto, cuerpo

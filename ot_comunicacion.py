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
CORREOS = (CORREO_TERMINADA_CLIENTE, CORREO_TERMINADA_RESPONSABLE, CORREO_TERMINADA_TECNICO, CORREO_MANUAL, CORREO_MANUAL_TECNICO)

PLANTILLA_TERMINADA = "ot_terminada"
PLANTILLA_TERMINADA_INTERNO = "ot_terminada_interno"
PLANTILLA_TERMINADA_TECNICO = "ot_terminada_tecnico"
PLANTILLA_MANUAL = "ot_envio_manual"
PLANTILLAS = (PLANTILLA_TERMINADA, PLANTILLA_TERMINADA_INTERNO, PLANTILLA_TERMINADA_TECNICO, PLANTILLA_MANUAL)

# Nombre del evento en email_log / Historial de Comunicaciones.
EVENTO_TERMINADA_CLIENTE = "ot_terminada_cliente"
EVENTO_TERMINADA_RESPONSABLE = "ot_terminada_responsable"
EVENTO_TERMINADA_TECNICO = "ot_terminada_tecnico"
EVENTO_MANUAL = "ot_envio_manual"
EVENTO_MANUAL_TECNICO = "ot_envio_manual_tecnico"

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
    """¿Está ENCENDIDO el aviso automático al cliente? Lo decide la MISMA condición que muestra Control de correos («Apagado por
    configuración»), para que la pantalla y el código jamás se contradigan. Nace apagado: vacío o ausente = apagado."""
    c = _ccr.por_clave(CORREO_TERMINADA_CLIENTE) or {}
    for cond in c.get("condiciones") or ():
        if cond.get("variable") == ENV_CLIENTE_ACTIVA:
            return bool(_ccr.evaluar_condicion(cond, environ))
    return False


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
    ("dias_vigencia", "Días que dura el enlace al documento"),
    ("enviado_por", "Quién envió el correo (solo «Enviar OT por correo»)"),
)


def esc(v):
    return _otc.esc(v)


def variables_ot(ot, *, saludo="", nombre_destinatario="", link_ot_firmada="", link_ot="", mensaje="", aprobado_por="",
                 cerrada_en="", resumen_envio_html="", enviado_por="", dias=TOKEN_DIAS, tecnicos_nombres=None):
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

# Nota: ninguna de estas plantillas lleva montos, costos, proveedores ni OC (REGLAS #19 y #26).
SEMILLAS = {
    # Al CLIENTE, al aprobar el cierre (nace apagado: OT_TERMINADA_CLIENTE_ACTIVA).
    PLANTILLA_TERMINADA: (
        "Tu orden de trabajo {{ot}} quedó terminada",
        _p("{{saludo}}")
        + _p("Terminamos el trabajo de la orden de trabajo <strong>{{ot}}</strong>. Aquí está el documento firmado, con lo que se hizo, "
             "los equipos revisados y las fotos.")
        + _tabla("#16a34a", _FILAS)
        + _boton("Ver la orden de trabajo firmada", "{{link_ot_firmada}}")
        + _pie("El enlace es personal y se mantiene disponible por {{dias_vigencia}} días. Si algo no calza, responde este correo o crea "
               "un ticket de soporte.")),
    # Al equipo (quien creó la OT, el ejecutivo o el responsable): qué pasó y qué se envió.
    PLANTILLA_TERMINADA_INTERNO: (
        "OT {{ot}} terminada · {{cliente}}",
        _p("{{saludo}}")
        + _p("La orden de trabajo <strong>{{ot}}</strong> quedó <strong>terminada y cerrada</strong>. El cierre lo aprobó {{aprobado_por}} "
             "el {{cerrada_en}}.")
        + _tabla("#0ea5e9", _FILAS)
        + "{{resumen_envio_html}}"
        + _boton("Ver la OT", "{{link_ot}}")),
    # Al técnico que la hizo: su copia, SIN montos ni proveedores.
    PLANTILLA_TERMINADA_TECNICO: (
        "OT {{ot}} terminada — gracias",
        _p("{{saludo}}")
        + _p("La orden de trabajo <strong>{{ot}}</strong> que realizaste quedó <strong>cerrada y aprobada</strong>. Gracias por el trabajo.")
        + _tabla("#16a34a", _FILAS)
        + _boton("Ver la OT", "{{link_ot}}")),
    # El envío manual («Enviar OT por correo»): el mismo texto para todos, con el mensaje opcional de quien envía.
    PLANTILLA_MANUAL: (
        "Orden de trabajo {{ot}} · {{cliente}}",
        _p("{{saludo}}")
        + "{{mensaje_html}}"
        + _p("Te compartimos la orden de trabajo <strong>{{ot}}</strong>, ya terminada y firmada. Con el enlace de abajo puedes ver el "
             "documento completo: lo que se hizo, los equipos revisados y las fotos.")
        + _tabla("#16a34a", _FILAS)
        + _boton("Ver la orden de trabajo firmada", "{{link_ot_firmada}}")
        + _pie("El enlace se mantiene disponible por {{dias_vigencia}} días. Lo envió {{enviado_por}} desde ILUS Fitness.")),
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

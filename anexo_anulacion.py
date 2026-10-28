# -*- coding: utf-8 -*-
"""📜 ANEXO ANULADO POR LA ANULACIÓN DEL SERVICIO — LÓGICA PURA (sin Flask ni base de datos), con pruebas en
tests/test_anexo_anulacion_ot_eliminada.py y tests/test_anexo_anulacion_2.py.

Daniel (2026-10-28, «es OT, no UTS»): «cuando eliminemos una OT y el anexo está firmado, que se le envíe al correo que el anexo queda
inválido por la anulación del servicio. Por ambas partes». Y después (mismo día): lo mismo para un anexo ENVIADO o VISTO sin firmar, lo
mismo al CANCELAR o ANULAR la OT (sin eliminarla) y un botón para REENVIAR el aviso. **A pedido de Daniel el aviso es SOLO TEXTO: nunca lleva
el anexo (ni un PDF) adjunto.**

Un anexo FIRMADO es evidencia: no se edita, no se borra. Si la OT que lo habilitaba se elimina, se cancela o se anula, el anexo pasa a `anulado`
(estado que la tabla ya tenía y que ningún código escribía) con el motivo, quién y cuándo, y se avisa a las DOS partes del contrato:
  · el PROVEEDOR (a donde se mandó el enlace de firma y el contacto de su ficha), sin montos ni datos internos;
  · ILUS (las personas que decide `_ot_gestion_destinatarios`: responsable, creador, ejecutivo del cliente), sin montos tampoco.
Al CLIENTE final jamás se le escribe (REGLA #22).

Un anexo ENVIADO o VISTO (sin firmar) también queda sin efecto: su enlace `/firmar-anexo/<token>` deja de permitir firmar. Un borrador nunca
enviado no se avisa.

ANEXO ÚNICO (REGLA #46): un anexo puede cubrir la instalación Y su desinstalación. La OT DUEÑA del anexo (la instalación) lo deja inválido
completo. La desinstalación que COMPARTE el anexo de su instalación NO lo invalida (la instalación sigue usándolo): si el anexo está firmado
queda firmado y se avisa que las partidas de la desinstalación quedan sin efecto; si aún no está firmado, esas partidas se retiran del anexo
(un anexo sin firmar sí se puede corregir) y se avisa si ya se había enviado.
"""
import re

import ot_anexo_costo as _oac

MOTIVO = "anulación del servicio (OT eliminada)"           # el de «eliminar» (compatibilidad)
MOTIVOS = {"eliminar": "anulación del servicio (OT eliminada)",
           "cancelar": "anulación del servicio (OT cancelada)",
           "anular": "anulación del servicio (OT anulada)"}
ACCIONES = tuple(MOTIVOS)
_VERBO = {"eliminar": "eliminada", "cancelar": "cancelada", "anular": "anulada"}
ESTADOS_FIRMA_PENDIENTE = ("enviado", "visto")               # el proveedor ya tiene el enlace y no ha firmado
ESTADOS_VIVOS = ("firmado",) + ESTADOS_FIRMA_PENDIENTE       # los que se anulan y se avisan
ESTADOS_EDITABLES = ("borrador", "enviado", "visto", "vencido")
CLAVE_PROVEEDOR = "anexo_anulado_proveedor"
CLAVE_INTERNO = "anexo_anulado_interno"
MAX_CORREOS_PROVEEDOR = 3
MAX_CORREOS_INTERNOS = 8
LINEA_REACTIVAR = "Si más adelante reactivas la OT, el anexo seguirá anulado: habrá que emitir uno nuevo."

_RX_CORREO = re.compile(r"^[a-z0-9._%+\-]+@[a-z0-9\-]+(\.[a-z0-9\-]+)+$")


def accion_valida(accion):
    a = (accion or "").strip().lower()
    return a if a in ACCIONES else "eliminar"


def motivo(accion="eliminar"):
    return MOTIVOS[accion_valida(accion)]


def verbo(accion="eliminar"):
    """«eliminada» | «cancelada» | «anulada»: cómo se dice que quedó la OT."""
    return _VERBO[accion_valida(accion)]


def accion_de_estado(estado):
    """La acción que corresponde a un estado de OT que se está escribiendo: 'cancelada' → 'cancelar', 'anulada' → 'anular', otro → None."""
    return {"cancelada": "cancelar", "anulada": "anular"}.get((estado or "").strip().lower())


def accion_de_motivo(texto):
    """La acción con que quedó anulado un anexo, leída de su motivo guardado ('… (OT cancelada)' → 'cancelar'); lo demás → 'eliminar'."""
    t = (texto or "").lower()
    return "cancelar" if "cancelada" in t else ("anular" if "anulada" in t else "eliminar")


def correos_de(*valores, maximo=MAX_CORREOS_PROVEEDOR):
    """Los correos VÁLIDOS de uno o varios textos (separados por coma, punto y coma o espacios), en minúscula, sin repetir y en el orden dado.
    Lo que no parece un correo se ignora (la ficha de un proveedor puede traer basura). Nunca lanza."""
    out = []
    for v in valores:
        for t in re.split(r"[\s,;]+", str(v or "")):
            c = t.strip().strip("<>").lower()
            if c and _RX_CORREO.match(c) and ".." not in c and c not in out:
                out.append(c)
    return out[:maximo]


def correos_invalidos(texto):
    """Lo que escribió la persona que NO es un correo válido (para decírselo en vez de ignorarlo en silencio)."""
    malos = []
    for t in re.split(r"[\s,;]+", str(texto or "")):
        c = t.strip().strip("<>").lower()
        if c and not (_RX_CORREO.match(c) and ".." not in c):
            malos.append(c)
    return malos


def partidas_salida(items):
    """Los conceptos (sin montos) de las partidas de la DESINSTALACIÓN de un anexo compartido: «Desinstalación», «Despacho (desinstalación)»."""
    return [i["concepto"] for i in _oac.parte(items, _oac.ROL_SALIDA) if i.get("concepto")]


def sin_partidas_salida(items):
    """Las partidas del anexo SIN las de la desinstalación (para un anexo compartido todavía sin firmar cuya desinstalación ya no va)."""
    return [i for i in _oac.items_de(items) if _oac.rol_de(i) != _oac.ROL_SALIDA]


def _lista(xs):
    xs = [str(x) for x in xs if str(x).strip()]
    if len(xs) <= 1:
        return "".join(xs)
    return ", ".join(xs[:-1]) + " y " + xs[-1]


# ───────────────────────── lo que ve la persona ANTES de eliminar / cancelar / anular ─────────────────────────
def _quien(a):
    if a.get("sin_firma"):
        return f"{a.get('proveedor') or 'proveedor'}, enviado el {a.get('enviado_el') or '—'} y sin firmar"
    return f"{a.get('proveedor') or 'proveedor'}, firmado el {a.get('firmado_el') or '—'}"


def lineas_confirmacion(plan, apagado_proveedor="", apagado_interno="", accion="eliminar"):
    """Las frases del aviso previo (alertar, no sorprender: REGLA #37). `plan` = {anexos:[{numero, proveedor, firmado_el, enviado_el,
    sin_firma, parcial, partidas, destinos}], interno:[{nombre, correo}]}. `apagado_*` = por qué esa puerta está cerrada ('' si no lo está).
    Para 'cancelar'/'anular' se suma la advertencia de la reactivación (una OT cancelada o anulada puede volver a abrirse)."""
    accion = accion_valida(accion)
    out = []
    for a in plan.get("anexos") or []:
        if a.get("parcial") and a.get("sin_firma"):
            out.append(f"Anexo N° {a.get('numero')} ({_quien(a)}): sigue vigente para la OT de la instalación, pero las partidas de la "
                       f"desinstalación ({_lista(a.get('partidas') or []) or 'desinstalación'}) se retirarán del anexo antes de que se firme.")
        elif a.get("parcial"):
            out.append(f"Anexo N° {a.get('numero')} ({_quien(a)}): sigue vigente para la OT de la instalación, pero las partidas de la "
                       f"desinstalación ({_lista(a.get('partidas') or []) or 'desinstalación'}) quedarán sin efecto.")
        elif a.get("sin_firma"):
            out.append(f"Anexo N° {a.get('numero')} ({_quien(a)}): quedará sin efecto por la anulación del servicio y su enlace de firma "
                       "dejará de funcionar.")
        else:
            out.append(f"Anexo N° {a.get('numero')} ({_quien(a)}): quedará inválido por la anulación del servicio.")
    destinos = []
    for a in plan.get("anexos") or []:
        for c in a.get("destinos") or []:
            if c not in destinos:
                destinos.append(c)
    if destinos:
        out.append("Se avisará por correo al proveedor: " + ", ".join(destinos) + "."
                   + (f" Hoy ese correo no saldrá: {apagado_proveedor}." if apagado_proveedor else ""))
    else:
        out.append("El proveedor no tiene un correo registrado: el aviso no podrá salir, habrá que avisarle por otro medio.")
    interno = [(p.get("nombre") or p.get("correo") or "").strip() + (f" ({p['correo']})" if p.get("correo") and p.get("nombre") else "")
               for p in plan.get("interno") or [] if p.get("correo")]
    if interno:
        out.append("Se avisará por correo al equipo ILUS: " + ", ".join(interno) + "."
                   + (f" Hoy ese correo no saldrá: {apagado_interno}." if apagado_interno else ""))
    else:
        out.append("No hay un correo interno al que avisar: queda solo la constancia en la bitácora.")
    out.append("El aviso es solo un texto: no lleva el anexo adjunto.")
    if accion in ("cancelar", "anular") and any(not a.get("parcial") for a in plan.get("anexos") or []):
        out.append(LINEA_REACTIVAR)
    return out


def para_pantalla(plan, apagado_proveedor="", apagado_interno="", accion="eliminar"):
    """El JSON del aviso previo: {aplica, titulo, lineas, anexos}. `aplica` es False cuando no hay ningún anexo que cambie."""
    anexos = [a for a in plan.get("anexos") or [] if not a.get("sin_aviso")]
    if not anexos:
        return {"aplica": False, "titulo": "", "lineas": [], "anexos": []}
    plan = dict(plan, anexos=anexos)
    completos = [a for a in anexos if not a.get("parcial")]
    titulo = ("Esta OT tiene un anexo" if len(anexos) == 1 else "Esta OT tiene anexos") + (
        ": quedará anulado por la anulación del servicio" if completos else ": parte de sus partidas quedará sin efecto")
    return {"aplica": True, "titulo": titulo, "accion": accion_valida(accion),
            "lineas": lineas_confirmacion(plan, apagado_proveedor, apagado_interno, accion),
            "anexos": [{"id": a.get("id"), "numero": a.get("numero"), "proveedor": a.get("proveedor"), "parcial": bool(a.get("parcial")),
                        "sin_firma": bool(a.get("sin_firma")), "firmado_el": a.get("firmado_el")} for a in anexos]}


# ───────────────────────── los correos (solo texto: nunca llevan el anexo adjunto) ─────────────────────────
def correo_proveedor(a, numero_ot, soporte_email, accion="eliminar", reenvio_por=""):
    """El aviso al PROVEEDOR. Sin montos, márgenes, cliente ni datos de otros proveedores. Devuelve el contenido (texto plano):
    {asunto, preheader, etiqueta, titulo, subtitulo, mensaje, filas:[(rótulo, valor)], cierre}. `a['sin_firma']`: el anexo se le había enviado
    para firmar y no lo firmó. `reenvio_por`: quien reenvía el aviso (se dice en una fila; el texto es el mismo)."""
    n = a.get("numero")
    firmado = a.get("firmado_el") or "—"
    if a.get("parcial"):
        partidas = _lista(a.get("partidas") or []) or "desinstalación"
        if a.get("sin_firma"):
            asunto = f"Anexo de Servicios N° {n}: las partidas de la desinstalación se retiraron"
            mensaje = (f"Te informamos que se anuló el servicio de desinstalación de la orden de trabajo {numero_ot}. Las partidas de ese servicio "
                       f"({partidas}) se retiraron del Anexo de Servicios N° {n} que te enviamos para firmar. El resto del anexo sigue vigente.")
            titulo = f"Anexo N° {n}: partidas retiradas"
        else:
            asunto = f"Anexo de Servicios N° {n}: las partidas de la desinstalación quedaron sin efecto"
            mensaje = (f"Te informamos que se anuló el servicio de desinstalación de la orden de trabajo {numero_ot}. Las partidas de ese servicio "
                       f"({partidas}) del Anexo de Servicios N° {n}, firmado el {firmado}, quedan sin efecto. El resto del anexo sigue vigente.")
            titulo = f"Anexo N° {n}: partidas sin efecto"
    elif a.get("sin_firma"):
        asunto = f"Anexo de Servicios N° {n}: el anexo que te enviamos para firmar quedó sin efecto"
        mensaje = (f"Te informamos que el Anexo de Servicios N° {n} que te enviamos para firmar quedó sin efecto por la anulación del servicio "
                   f"correspondiente a la orden de trabajo {numero_ot}. Ya no es posible firmarlo con el enlace que recibiste.")
        titulo = f"Anexo N° {n} sin efecto"
    else:
        asunto = f"Anexo de Servicios N° {n}: quedó inválido por la anulación del servicio"
        mensaje = (f"Te informamos que el Anexo de Servicios N° {n}, firmado el {firmado}, quedó inválido por la anulación del servicio "
                   f"correspondiente a la orden de trabajo {numero_ot}.")
        titulo = f"Anexo N° {n} inválido"
    filas = [("Anexo", f"N° {n}"), ("Orden de trabajo", numero_ot),
             ("Enviado el" if a.get("sin_firma") else "Firmado el", (a.get("enviado_el") or "—") if a.get("sin_firma") else firmado),
             ("Motivo", "Anulación del servicio")]
    if reenvio_por:
        filas.append(("Aviso", "Reenvío del aviso"))
    return {"asunto": asunto, "preheader": mensaje[:140], "etiqueta": "Anulación del servicio", "titulo": titulo,
            "subtitulo": a.get("proveedor") or "", "mensaje": mensaje, "filas": filas,
            "cierre": f"Si tienes dudas, escríbenos a {soporte_email}."}


def correo_interno(a, numero_ot, titulo_ot, quien, cuando, lote_txt="", aviso_proveedor="", accion="eliminar", reenvio_por=""):
    """El aviso al equipo ILUS (interno). Sin montos. `lote_txt`: 'estaba en el lote #8 (pagada) y salió de él' si corresponde.
    `aviso_proveedor`: qué pasó con el correo al proveedor (para que sepan si hay que avisarle por otro medio)."""
    n = a.get("numero")
    firmado = a.get("firmado_el") or "—"
    ver = verbo(accion)
    firmante = f" por {a['firmante']}" if a.get("firmante") else ""
    ot_txt = f"La {numero_ot}{' — ' + titulo_ot if titulo_ot else ''}"
    if a.get("parcial"):
        partidas = _lista(a.get("partidas") or []) or "desinstalación"
        asunto = f"Anexo N° {n}: partidas de la {numero_ot} {'retiradas' if a.get('sin_firma') else 'sin efecto'}"
        mensaje = (f"{ot_txt} (desinstalación) fue {ver} por {quien} el {cuando}. El Anexo N° {n} sigue vigente para la instalación; "
                   f"las partidas de la desinstalación ({partidas}) {'se retiraron del anexo (aún sin firmar)' if a.get('sin_firma') else 'quedan sin efecto'}.")
        titulo = f"Anexo N° {n}: partidas {'retiradas' if a.get('sin_firma') else 'sin efecto'}"
    elif a.get("sin_firma"):
        asunto = f"Anexo N° {n} sin efecto: la {numero_ot} quedó {ver}"
        mensaje = (f"{ot_txt} fue {ver} por {quien} el {cuando}. El Anexo N° {n}, que se había enviado al proveedor y no estaba firmado, "
                   "quedó sin efecto por la anulación del servicio y su enlace de firma ya no permite firmar.")
        titulo = f"Anexo N° {n} sin efecto"
    else:
        asunto = f"Anexo N° {n} inválido: la {numero_ot} quedó {ver}"
        mensaje = f"{ot_txt} fue {ver} por {quien} el {cuando}. El Anexo N° {n} quedó inválido por la anulación del servicio."
        titulo = f"Anexo N° {n} inválido"
    filas = [("Anexo", f"N° {n}"), ("Proveedor", a.get("proveedor") or "—"),
             (("Enviado el", (a.get("enviado_el") or "—")) if a.get("sin_firma") else ("Firmado el", firmado + firmante)),
             (f"OT {ver}", numero_ot), (f"{ver.capitalize()} por", quien), ("Motivo", motivo(accion))]
    if lote_txt:
        filas.append(("Lote de proveedor", lote_txt))
    if aviso_proveedor:
        filas.append(("Aviso al proveedor", aviso_proveedor))
    if reenvio_por:
        filas.append(("Aviso reenviado por", reenvio_por))
    return {"asunto": asunto, "preheader": mensaje[:140], "etiqueta": "Anexo anulado", "titulo": titulo, "subtitulo": a.get("proveedor") or "",
            "mensaje": mensaje, "filas": filas,
            "cierre": "Correo interno de ILUS Fitness: al cliente no se le envía nada. Se apaga en Comunicaciones → Control de correos."}


# ───────────────────────── lo que pasó (constancia y respuesta a la pantalla) ─────────────────────────
def texto_aviso(destinos, salio, motivo_txt=""):
    """Una frase honesta de lo que pasó con UN correo: enviado / no salió (con el motivo) / sin destinatario."""
    if not destinos:
        return "sin correo registrado: hay que avisarle por otro medio"
    if salio:
        return "enviado a " + ", ".join(destinos)
    return "NO salió" + (f" ({motivo_txt})" if motivo_txt else "") + " — destinatarios: " + ", ".join(destinos)


def lineas_resultado(anexos_res):
    """Las frases que ve la persona DESPUÉS de eliminar / cancelar / anular. `anexos_res` = [{numero, parcial, sin_firma, proveedor:{texto},
    interno:{texto}}]."""
    out = []
    for r in anexos_res or []:
        if r.get("parcial"):
            que = ("las partidas de la desinstalación se retiraron del anexo." if r.get("sin_firma")
                   else "las partidas de la desinstalación quedaron sin efecto.")
        elif r.get("sin_firma"):
            que = "quedó sin efecto por la anulación del servicio (su enlace de firma ya no sirve)."
        else:
            que = "quedó inválido por la anulación del servicio."
        out.append(f"Anexo N° {r.get('numero')}: {que}")
        out.append("Aviso al proveedor: " + ((r.get("proveedor") or {}).get("texto") or "—") + ".")
        out.append("Aviso al equipo ILUS: " + ((r.get("interno") or {}).get("texto") or "—") + ".")
    return out


def aviso_reactivada(numeros):
    """Lo que se le dice a la persona cuando REACTIVA una OT cuyo anexo quedó anulado: el anexo NO revive."""
    ns = ", ".join(f"N° {n}" for n in numeros)
    plural = len(numeros) != 1
    return (f"La OT se reactivó, pero {'los anexos' if plural else 'el anexo'} {ns} {'siguen' if plural else 'sigue'} anulado"
            f"{'s' if plural else ''}: hay que emitir {'anexos nuevos' if plural else 'un anexo nuevo'} y mandarlo a firmar al proveedor.")

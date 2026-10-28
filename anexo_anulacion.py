# -*- coding: utf-8 -*-
"""📜 ANEXO FIRMADO + OT ELIMINADA — LÓGICA PURA (sin Flask ni base de datos), con pruebas en tests/test_anexo_anulacion_ot_eliminada.py.

Daniel (2026-10-28, «es OT, no UTS»): «cuando eliminemos una OT y el anexo está firmado, que se le envíe al correo que el anexo queda
inválido por la anulación del servicio. Por ambas partes».

Un anexo FIRMADO es evidencia: no se edita, no se borra. Si la OT que lo habilitaba se elimina, el anexo pasa a `anulado` (estado que la tabla
ya tenía y que ningún código escribía) con el motivo, quién y cuándo, y se avisa a las DOS partes del contrato:
  · el PROVEEDOR (a donde se mandó el enlace de firma y el contacto de su ficha), sin montos ni datos internos;
  · ILUS (las personas que decide `_ot_gestion_destinatarios`: responsable, creador, ejecutivo del cliente), sin montos tampoco.
Al CLIENTE final jamás se le escribe (REGLA #22).

ANEXO ÚNICO (REGLA #46): un anexo firmado puede cubrir la instalación Y su desinstalación. Eliminar la OT DUEÑA del anexo (la instalación, o la
OT que lo emitió) lo deja inválido completo. Eliminar la desinstalación que COMPARTE el anexo de su instalación NO lo invalida (la instalación
sigue usándolo): el anexo queda firmado, se avisa a las dos partes que las partidas de la desinstalación quedan sin efecto, y se deja constancia.
"""
import re

import ot_anexo_costo as _oac

MOTIVO = "anulación del servicio (OT eliminada)"
CLAVE_PROVEEDOR = "anexo_anulado_proveedor"
CLAVE_INTERNO = "anexo_anulado_interno"
MAX_CORREOS_PROVEEDOR = 3
MAX_CORREOS_INTERNOS = 8

_RX_CORREO = re.compile(r"^[a-z0-9._%+\-]+@[a-z0-9\-]+(\.[a-z0-9\-]+)+$")


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


def partidas_salida(items):
    """Los conceptos (sin montos) de las partidas de la DESINSTALACIÓN de un anexo compartido: «Desinstalación», «Despacho (desinstalación)»."""
    return [i["concepto"] for i in _oac.parte(items, _oac.ROL_SALIDA) if i.get("concepto")]


def _lista(xs):
    xs = [str(x) for x in xs if str(x).strip()]
    if len(xs) <= 1:
        return "".join(xs)
    return ", ".join(xs[:-1]) + " y " + xs[-1]


# ───────────────────────── lo que ve la persona ANTES de eliminar ─────────────────────────
def lineas_confirmacion(plan, apagado_proveedor="", apagado_interno=""):
    """Las frases del aviso previo a la eliminación (alertar, no sorprender: REGLA #37). `plan` = {anexos:[{numero, proveedor, firmado_el,
    parcial, partidas, destinos}], interno:[{nombre, correo}]}. `apagado_*` = por qué esa puerta está cerrada ('' si no lo está)."""
    out = []
    for a in plan.get("anexos") or []:
        quien = f"{a.get('proveedor') or 'proveedor'}, firmado el {a.get('firmado_el') or '—'}"
        if a.get("parcial"):
            out.append(f"Anexo N° {a.get('numero')} ({quien}): sigue vigente para la OT de la instalación, pero las partidas de la "
                       f"desinstalación ({_lista(a.get('partidas') or []) or 'desinstalación'}) quedarán sin efecto.")
        else:
            out.append(f"Anexo N° {a.get('numero')} ({quien}): quedará inválido por la anulación del servicio.")
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
    return out


def para_pantalla(plan, apagado_proveedor="", apagado_interno=""):
    """El JSON del aviso previo: {aplica, titulo, lineas, anexos}. `aplica` es False cuando no hay ningún anexo firmado que cambie."""
    anexos = plan.get("anexos") or []
    if not anexos:
        return {"aplica": False, "titulo": "", "lineas": [], "anexos": []}
    completos = [a for a in anexos if not a.get("parcial")]
    titulo = ("Esta OT tiene un anexo firmado" if len(anexos) == 1 else "Esta OT tiene anexos firmados") + (
        ": quedará inválido por la anulación del servicio" if completos else ": parte de sus partidas quedará sin efecto")
    return {"aplica": True, "titulo": titulo, "lineas": lineas_confirmacion(plan, apagado_proveedor, apagado_interno),
            "anexos": [{"id": a.get("id"), "numero": a.get("numero"), "proveedor": a.get("proveedor"), "parcial": bool(a.get("parcial")),
                        "firmado_el": a.get("firmado_el")} for a in anexos]}


# ───────────────────────── los correos ─────────────────────────
def correo_proveedor(a, numero_ot, soporte_email):
    """El aviso al PROVEEDOR. Sin montos, sin márgenes, sin cliente, sin datos de otros proveedores. Devuelve el contenido (texto plano):
    {asunto, preheader, etiqueta, titulo, subtitulo, mensaje, filas:[(rótulo, valor)], cierre}."""
    n = a.get("numero")
    firmado = a.get("firmado_el") or "—"
    if a.get("parcial"):
        partidas = _lista(a.get("partidas") or []) or "desinstalación"
        asunto = f"Anexo de Servicios N° {n}: las partidas de la desinstalación quedaron sin efecto"
        mensaje = (f"Te informamos que se anuló el servicio de desinstalación de la orden de trabajo {numero_ot}. Las partidas de ese servicio "
                   f"({partidas}) del Anexo de Servicios N° {n}, firmado el {firmado}, quedan sin efecto. El resto del anexo sigue vigente.")
        titulo, etiqueta = f"Anexo N° {n}: partidas sin efecto", "Anulación del servicio"
    else:
        asunto = f"Anexo de Servicios N° {n}: quedó inválido por la anulación del servicio"
        mensaje = (f"Te informamos que el Anexo de Servicios N° {n}, firmado el {firmado}, quedó inválido por la anulación del servicio "
                   f"correspondiente a la orden de trabajo {numero_ot}.")
        titulo, etiqueta = f"Anexo N° {n} inválido", "Anulación del servicio"
    return {"asunto": asunto, "preheader": mensaje[:140], "etiqueta": etiqueta, "titulo": titulo,
            "subtitulo": a.get("proveedor") or "", "mensaje": mensaje,
            "filas": [("Anexo", f"N° {n}"), ("Orden de trabajo", numero_ot), ("Firmado el", firmado), ("Motivo", "Anulación del servicio")],
            "cierre": f"Si tienes dudas, escríbenos a {soporte_email}."}


def correo_interno(a, numero_ot, titulo_ot, quien, cuando, lote_txt="", aviso_proveedor=""):
    """El aviso al equipo ILUS (interno). Sin montos. `lote_txt`: 'estaba en el lote #8 (pagada) y salió de él' si corresponde.
    `aviso_proveedor`: qué pasó con el correo al proveedor (para que sepan si hay que avisarle por otro medio)."""
    n = a.get("numero")
    firmado = a.get("firmado_el") or "—"
    firmante = f" por {a['firmante']}" if a.get("firmante") else ""
    if a.get("parcial"):
        partidas = _lista(a.get("partidas") or []) or "desinstalación"
        asunto = f"Anexo N° {n}: partidas de la {numero_ot} sin efecto"
        mensaje = (f"La {numero_ot}{' — ' + titulo_ot if titulo_ot else ''} (desinstalación) fue eliminada por {quien} el {cuando}. "
                   f"El Anexo N° {n} sigue vigente para la instalación; las partidas de la desinstalación ({partidas}) quedan sin efecto.")
        titulo = f"Anexo N° {n}: partidas sin efecto"
    else:
        asunto = f"Anexo N° {n} inválido: se eliminó la {numero_ot}"
        mensaje = (f"La {numero_ot}{' — ' + titulo_ot if titulo_ot else ''} fue eliminada por {quien} el {cuando}. "
                   f"El Anexo N° {n} quedó inválido por la anulación del servicio.")
        titulo = f"Anexo N° {n} inválido"
    filas = [("Anexo", f"N° {n}"), ("Proveedor", a.get("proveedor") or "—"), ("Firmado el", firmado + firmante),
             ("OT eliminada", numero_ot), ("Eliminada por", quien), ("Motivo", MOTIVO)]
    if lote_txt:
        filas.append(("Lote de proveedor", lote_txt))
    if aviso_proveedor:
        filas.append(("Aviso al proveedor", aviso_proveedor))
    return {"asunto": asunto, "preheader": mensaje[:140], "etiqueta": "Anexo anulado", "titulo": titulo, "subtitulo": a.get("proveedor") or "",
            "mensaje": mensaje, "filas": filas,
            "cierre": "Correo interno de ILUS Fitness: al cliente no se le envía nada. Se apaga en Comunicaciones → Control de correos."}


# ───────────────────────── lo que pasó (constancia y respuesta a la pantalla) ─────────────────────────
def texto_aviso(destinos, salio, motivo=""):
    """Una frase honesta de lo que pasó con UN correo: enviado / no salió (con el motivo) / sin destinatario."""
    if not destinos:
        return "sin correo registrado: hay que avisarle por otro medio"
    if salio:
        return "enviado a " + ", ".join(destinos)
    return "NO salió" + (f" ({motivo})" if motivo else "") + " — destinatarios: " + ", ".join(destinos)


def lineas_resultado(anexos_res):
    """Las frases que ve la persona DESPUÉS de eliminar. `anexos_res` = [{numero, parcial, proveedor:{texto}, interno:{texto}}]."""
    out = []
    for r in anexos_res or []:
        out.append(f"Anexo N° {r.get('numero')}: "
                   + ("las partidas de la desinstalación quedaron sin efecto." if r.get("parcial")
                      else "quedó inválido por la anulación del servicio."))
        out.append("Aviso al proveedor: " + ((r.get("proveedor") or {}).get("texto") or "—") + ".")
        out.append("Aviso al equipo ILUS: " + ((r.get("interno") or {}).get("texto") or "—") + ".")
    return out

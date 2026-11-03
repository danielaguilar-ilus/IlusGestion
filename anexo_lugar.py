# -*- coding: utf-8 -*-
"""Lugar del servicio del Anexo de Servicios (3-nov-2026).

Daniel (pedido de Félix): «en el anexo de servicios necesitamos que indique dónde se realizará el servicio. Es importante dejar la dirección del
lugar en el anexo.» Hasta hoy la dirección solo aparecía, a veces, dentro de la frase del objetivo («…, en <dirección>») y no en los anexos que se
creaban sin ella (ej. el Anexo N° 245: «Ejecutar servicio de instalación al cliente …», sin dirección).

Este módulo es la parte PURA (sin Flask ni base de datos, con pruebas): arma el texto del lugar a partir de lo que la OT ya sabe. El texto se
guarda en `mant_anexos.lugar_servicio` al crear el anexo, para que un anexo firmado diga lo que decía cuando se firmó (si después se corrige la
dirección de la OT, el documento firmado no cambia solo).
"""

MAX_LARGO = 500


def componer_lugar(direccion, comuna="", detalle=""):
    """«Dirección, Comuna (detalle)». La comuna se agrega solo si la dirección no la trae ya (las que vienen de Google Places la incluyen:
    «1 Norte 4745, Talca, Maule»). Sin dirección no hay lugar: devuelve "" (nunca se inventa uno a partir de la comuna sola)."""
    d = " ".join(str(direccion or "").split())
    if not d:
        return ""
    c = " ".join(str(comuna or "").split())
    if c and c.lower() not in d.lower():
        d = f"{d}, {c}"
    det = " ".join(str(detalle or "").split())
    if det and det.lower() not in d.lower():
        d = f"{d} ({det})"
    return d[:MAX_LARGO]


def lugar_de_ot(fila):
    """Lugar del servicio de una OT. `fila` trae lo que ya guarda la OT: `direccion_visita` (la dirección DEL LUGAR, que puede diferir de la del
    cliente: una cadena tiene varias sedes), su `direccion_comuna` y `direccion_detalle`; si la OT no tiene dirección propia, la ficha del
    cliente (`cliente_direccion`, `cliente_comuna`). La comuna del cliente solo acompaña a la dirección del cliente, nunca a una dirección propia
    de la OT (mismo criterio que la ruta del técnico, `_ot_ruta_destino`)."""
    f = fila or {}
    propia = " ".join(str(f.get("direccion_visita") or "").split())
    if propia:
        return componer_lugar(propia, f.get("direccion_comuna"), f.get("direccion_detalle"))
    return componer_lugar(f.get("cliente_direccion"), f.get("cliente_comuna"))


def lugar_del_anexo(guardado, estado, firmado_at, lugar_de_la_ot):
    """Qué lugar muestra un anexo. Orden: 1) el que quedó guardado al crearlo; 2) si el anexo AÚN NO está firmado, el de la OT en vivo (es un
    borrador que se está acordando: igual que su tabla de equipos); 3) nada. Un anexo ya firmado sin lugar guardado (los anteriores a esta
    función) no muestra uno inventado: lo que se firmó no se reescribe."""
    g = " ".join(str(guardado or "").split())
    if g:
        return g[:MAX_LARGO]
    if str(estado or "") == "firmado" or firmado_at:
        return ""
    return " ".join(str(lugar_de_la_ot or "").split())[:MAX_LARGO]

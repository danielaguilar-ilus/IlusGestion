"""Imágenes de FIRMA de correo que no son evidencia del caso (Daniel, 2026-10-28).

Problema real: en la ficha de un ticket con 136 «fotos» casi todas eran el logo
de ILUS (sphs-ilus-fitness.png) y el banner «InsertPic_xxxx.jpg» de la firma.
Cada respuesta del cliente trae esas imágenes incrustadas (el correo cita el
hilo anterior, con nuestra firma, y la propia firma del cliente), y la
ingesta las guardaba como adjuntos una y otra vez.

Este módulo es PURO (sin base ni Flask) para poder probarlo. La ingesta de
correo (`tickets_module`) lo usa para NO guardar una imagen que es firma; lo que
sí es evidencia (una foto adjunta o pegada por el cliente) pasa igual que antes.

Regla de oro: solo se descarta una IMAGEN INCRUSTADA (inline / referenciada con
`cid:` en el cuerpo HTML). Un archivo adjunto «de verdad» jamás se descarta.
"""
import re

# Nombres típicos de imágenes de firma / decoración que generan Outlook, Gmail y
# los editores de firma. Solo se aplican a imágenes INCRUSTADAS.
_NOMBRE_FIRMA = re.compile(
    r"^(insertpic[_\-]|image\d{2,4}\.|outlook[-_]|~?wrd\d+|sphs[-_ ]?ilus|ilus[-_ ]?(fitness|logo)|"
    r"logo|firma|signature|banner|footer|spacer|pixel|facebook|instagram|linkedin|twitter|whatsapp)",
    re.I)

# Una imagen incrustada así de chica es un ícono o logo, nunca la foto de una falla.
TAM_MAX_ICONO_BYTES = 12 * 1024


def nombre_parece_firma(nombre):
    return bool(_NOMBRE_FIRMA.match((nombre or "").strip()))


def cids_referenciados(html):
    """Content-ID que el cuerpo HTML usa con `cid:` (imágenes incrustadas)."""
    return {c.strip("<>").lower() for c in re.findall(r"cid:([^\"'\s>)]+)", html or "", re.I)}


def es_incrustada(content_disposition, content_id, cids_html):
    """True si la parte va EN el cuerpo del correo (no es un adjunto aparte)."""
    disp = (content_disposition or "").strip().lower()
    if disp.startswith("inline"):
        return True
    cid = (content_id or "").strip("<> \r\n\t").lower()
    return bool(cid and cid in (cids_html or ()))


def clave_tam(ctype, nbytes):
    """Misma clave que se puede reconstruir desde tk_adjuntos (mime + KB)."""
    return ((ctype or "").lower(), max(1, int(nbytes or 0) // 1024))


def motivo_firma(ctype, nombre, nbytes, incrustada, ya_guardados=()):
    """Devuelve por qué la imagen es firma (texto corto) o None si es evidencia.

    ya_guardados: conjunto de `clave_tam(...)` de las imágenes del cliente que
    este ticket ya tiene (más las de este mismo correo ya aceptadas)."""
    if not (ctype or "").lower().startswith("image/") or not incrustada:
        return None
    if nombre_parece_firma(nombre):
        return "imagen de firma"
    if int(nbytes or 0) <= TAM_MAX_ICONO_BYTES:
        return "ícono o logo incrustado"
    if clave_tam(ctype, nbytes) in ya_guardados:
        return "repetida en el ticket"
    return None

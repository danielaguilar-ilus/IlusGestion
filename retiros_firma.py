# -*- coding: utf-8 -*-
"""ILUS Fitness · Retiros · Firma digital de recepción y comprobante (funciones PURAS, sin BD ni Flask).

Daniel (dueño) aprobó: «firma digital del cliente al retirar, con comprobante por correo de ILUS Fitness — logística verde, sin guías de
despacho impresas, recepción del retiro certificada». Las rutas viven en pickups_module.py; aquí solo lo que se puede probar solo:
validar el RUT y la imagen, el hash de integridad, el token del comprobante y el bloque HTML del correo.

Una firma es EVIDENCIA: una por retiro (UNIQUE request_id), nunca se pisa ni se borra.
"""
import base64
import hashlib
import hmac
import html
import json
import re

RELACIONES = {"cliente": "El cliente", "chofer": "Chofer o transportista", "tercero": "Tercero autorizado", "otro": "Otra persona"}
MAX_PNG_BYTES = 300 * 1024                 # imagen ya decodificada
MAX_DATAURL_CHARS = 420_000                # base64 de 300 KB ≈ 400 K caracteres
MAX_OBS = 500
MAX_NOMBRE = 190
_PNG_MAGICO = b"\x89PNG\r\n\x1a\n"

RAZON_SOCIAL = "Sport and Health Solutions SPA"      # ILUS_LEGAL: es un documento de recepción
RUT_EMPRESA = "76.996.964-0"                         # ILUS_RUT
MARCA = "ILUS Fitness"                               # ILUS_BRAND


# ── RUT ────────────────────────────────────────────────────────────────
def _dv(cuerpo):
    suma, mult = 0, 2
    for c in reversed(cuerpo):
        suma += int(c) * mult
        mult = 2 if mult == 7 else mult + 1
    r = 11 - (suma % 11)
    return "0" if r == 11 else ("K" if r == 10 else str(r))


def rut_normalizado(rut):
    """'12.345.678-5' → '12345678-5' si el RUT es válido (dígito verificador módulo 11); si no, None."""
    limpio = re.sub(r"[^0-9kK]", "", str(rut or "")).upper()
    if len(limpio) < 7 or len(limpio) > 9 or not limpio[:-1].isdigit():
        return None
    cuerpo, dv = limpio[:-1], limpio[-1]
    if int(cuerpo) < 1_000_000 or _dv(cuerpo) != dv:
        return None
    return f"{cuerpo}-{dv}"


def rut_formateado(rut):
    """Para mostrar (REGLA #7): '12345678-5' → '12.345.678-5'. Si no es un RUT, devuelve el texto limpio."""
    n = rut_normalizado(rut)
    if not n:
        return str(rut or "").strip()
    cuerpo, dv = n.split("-")
    return f"{int(cuerpo):,}".replace(",", ".") + "-" + dv


# ── imagen de la firma ─────────────────────────────────────────────────
def validar_firma_png(data_url):
    """(bytes_png, None) o (None, motivo). Solo `data:image/png;base64,...`, PNG real, con tope de tamaño y dimensiones razonables."""
    if not isinstance(data_url, str) or not data_url.startswith("data:image/png;base64,"):
        return None, "La firma debe ser una imagen PNG."
    if len(data_url) > MAX_DATAURL_CHARS:
        return None, "La imagen de la firma es demasiado grande."
    try:
        crudo = base64.b64decode(data_url.split(",", 1)[1], validate=True)
    except Exception:
        return None, "La imagen de la firma no es válida."
    if len(crudo) > MAX_PNG_BYTES:
        return None, "La imagen de la firma es demasiado grande."
    if len(crudo) < 60 or not crudo.startswith(_PNG_MAGICO) or crudo[12:16] != b"IHDR":
        return None, "La imagen de la firma no es un PNG válido."
    ancho = int.from_bytes(crudo[16:20], "big")
    alto = int.from_bytes(crudo[20:24], "big")
    if not (20 <= ancho <= 2400 and 20 <= alto <= 2400):
        return None, "Las dimensiones de la firma no son válidas."
    return crudo, None


# ── foto de lo entregado ───────────────────────────────────────────────
def _cant(v):
    try:
        f = float(v)
    except (TypeError, ValueError):
        return "0"
    return str(int(f)) if f == int(f) else str(round(f, 2))


def productos_snapshot(lineas):
    """Líneas consolidadas del retiro → [{'doc': 'BLV 0000023732', 'lineas': [{'sku','descripcion','cantidad'}]}] (orden estable)."""
    grupos, orden = {}, []
    for ln in lineas or []:
        doc = f"{(ln.get('doc_tipo') or '').strip()} {(ln.get('doc_numero') or '').strip()}".strip() or "Documento"
        if doc not in grupos:
            grupos[doc] = []
            orden.append(doc)
        grupos[doc].append({"sku": str(ln.get("sku") or "")[:80], "descripcion": str(ln.get("descripcion") or "")[:300],
                            "cantidad": _cant(ln.get("cantidad"))})
    return [{"doc": d, "lineas": grupos[d]} for d in orden]


def total_unidades(productos):
    t = 0.0
    for g in productos or []:
        for ln in g.get("lineas") or []:
            try:
                t += float(ln.get("cantidad") or 0)
            except (TypeError, ValueError):
                pass
    return _cant(t)


# ── hash de integridad ─────────────────────────────────────────────────
def hash_contenido(request_id, code, nombre, rut, relacion, conformidad, observaciones, fecha_iso, productos, firma_sha):
    """SHA-256 del contenido canónico (JSON ordenado, UTF-8). Cambiar cualquier dato firmado cambia el hash."""
    canon = {"request_id": int(request_id), "code": str(code or ""), "firmante_nombre": nombre, "firmante_rut": rut,
             "relacion": relacion, "conformidad": 1 if conformidad else 0, "observaciones": observaciones or "",
             "fecha": fecha_iso, "productos": productos, "firma_sha256": firma_sha}
    return hashlib.sha256(json.dumps(canon, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode("utf-8")).hexdigest()


def sha_bytes(b):
    return hashlib.sha256(b).hexdigest()


# ── token público del comprobante (HMAC de app.secret_key + id; NO es el public_token del seguimiento) ──
def _base36(n):
    d = "0123456789abcdefghijklmnopqrstuvwxyz"
    s = ""
    while n:
        n, r = divmod(n, 36)
        s = d[r] + s
    return s or "0"


def generar_token(secreto, request_id):
    if not secreto:
        return ""
    mac = hmac.new(str(secreto).encode("utf-8"), f"comprobante:v1:{int(request_id)}".encode("utf-8"), hashlib.sha256).hexdigest()[:24]
    return f"{_base36(int(request_id))}-{mac}"


def id_de_token(secreto, token):
    """Id del retiro si el token es auténtico; None si no."""
    m = re.fullmatch(r"([0-9a-z]{1,10})-([0-9a-f]{24})", token or "")
    if not m or not secreto:
        return None
    try:
        rid = int(m.group(1), 36)
    except ValueError:
        return None
    esperado = generar_token(secreto, rid)
    return rid if esperado and hmac.compare_digest(esperado, token) else None


# ── bloque HTML del correo ─────────────────────────────────────────────
def _e(v):
    return html.escape(str(v if v is not None else ""), quote=True)


def bloque_email(firma, url):
    """Resumen + botón «Ver comprobante», seguro para correo (todo escapado, estilos en línea). `firma` ya viene con textos listos."""
    conf = "Recibió conforme" if firma.get("conformidad") else "Recibió con observaciones"
    obs = (f'<tr><td style="padding:3px 0;color:#6b7280;font-size:12.5px;vertical-align:top">Observaciones</td>'
           f'<td style="padding:3px 0;color:#111827;font-size:13px">{_e(firma.get("observaciones"))}</td></tr>') if firma.get("observaciones") else ""
    return (
        '<table cellpadding="0" cellspacing="0" width="100%" style="background:#f0fdf4;border:1px solid #bbf7d0;border-left:4px solid #16a34a;'
        'border-radius:10px;margin:0 0 18px"><tr><td style="padding:16px 18px">'
        '<div style="font-size:11px;color:#166534;text-transform:uppercase;letter-spacing:.07em;font-weight:700;margin-bottom:8px">'
        'Recepción firmada</div>'
        '<table cellpadding="0" cellspacing="0" width="100%">'
        f'<tr><td style="padding:3px 0;color:#6b7280;font-size:12.5px;width:34%">Retiró</td><td style="padding:3px 0;color:#111827;font-size:13px;font-weight:700">'
        f'{_e(firma.get("nombre"))} ({_e(firma.get("rut_fmt"))})</td></tr>'
        f'<tr><td style="padding:3px 0;color:#6b7280;font-size:12.5px">Fecha y hora</td><td style="padding:3px 0;color:#111827;font-size:13px">{_e(firma.get("cuando"))}</td></tr>'
        f'<tr><td style="padding:3px 0;color:#6b7280;font-size:12.5px">Estado</td><td style="padding:3px 0;color:#111827;font-size:13px">{_e(conf)}</td></tr>'
        f'<tr><td style="padding:3px 0;color:#6b7280;font-size:12.5px">Productos</td><td style="padding:3px 0;color:#111827;font-size:13px">{_e(firma.get("unidades"))} unidades</td></tr>'
        f'{obs}'
        '</table>'
        f'<div style="margin-top:14px"><a href="{_e(url)}" style="display:inline-block;background:#0a0a0a;color:#ffffff;text-decoration:none;'
        'font-weight:700;font-size:14px;padding:11px 20px;border-radius:8px">Ver comprobante</a></div>'
        '<div style="margin-top:10px;font-size:11.5px;color:#6b7280;line-height:1.5">Logística verde: tu recepción queda certificada de forma digital, '
        f'sin guías de despacho impresas. Código de integridad {_e(firma.get("hash_corto"))}.</div>'
        '</td></tr></table>'
    )

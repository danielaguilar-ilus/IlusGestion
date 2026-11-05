# -*- coding: utf-8 -*-
"""Rebaja de un repuesto que YA EXISTE: autorización + argumento + documento de Random (PURO, sin base de datos; probado en tests/test_repuestos_rebaja.py).

Daniel (6-nov-2026): «los repuestos que están creados los están ajustando. Ahora el repuesto que esté creado necesita una autorización para
rebajar, necesita un argumento, una factura, algo, porque el ajuste manual es desorden y ya definitivamente no se va a volver a realizar».
Sus tres decisiones (AskUserQuestion, 6-nov): 1) autoriza SOLO el superadmin; 2) el respaldo es argumento + documento del ERP Random;
3) se cierra TODO ajuste manual de cantidad (las entradas siguen solo por recepción de proveedor o alta; las salidas por instalación en OT).
"""
import re

ARGUMENTO_MIN = 30
ARGUMENTO_MAX = 1000
RESOLUCION_MIN = 5
NOTA_KARDEX_MAX = 500          # largo de mant_repuestos_movimientos.nota

ESTADOS = ("pendiente", "aprobada", "rechazada", "anulada")

# Documentos del ERP que respaldan una rebaja: la lista que se ofrece en pantalla (cualquier otro código del ERP también se acepta si existe).
TIPOS_DOC = (
    ("FCV", "Factura"),
    ("BLV", "Boleta"),
    ("GDV", "Guía de despacho"),
    ("NVV", "Nota de venta"),
    ("FCC", "Factura de compra"),
    ("GRC", "Guía de recepción"),
)
_TIDO_RE = re.compile(r"^[A-Z0-9]{2,4}$")
_NUDO_RE = re.compile(r"^[A-Za-z0-9\-]{1,30}$")


def _num(v):
    try:
        n = float(str(v).replace(",", "."))
    except (TypeError, ValueError):
        return None
    if n != n or n in (float("inf"), float("-inf")):
        return None
    return round(n, 2)


def normalizar_doc(tido, nudo):
    """(TIDO en mayúsculas, número sin espacios) o ('', '') si no es un documento posible."""
    t = str(tido or "").strip().upper()
    n = str(nudo or "").strip()
    if not _TIDO_RE.match(t) or not _NUDO_RE.match(n):
        return "", ""
    return t, n


def etiqueta_doc(tido):
    """«GDV» → «Guía de despacho (GDV)»; un código fuera de la lista se muestra tal cual."""
    t = str(tido or "").strip().upper()
    for cod, nom in TIPOS_DOC:
        if cod == t:
            return f"{nom} ({cod})"
    return t


def validar_pedido(saldo, cantidad, argumento, tido, nudo):
    """None si el pedido es válido; si no, el mensaje que se le muestra a quien lo pide (claro y accionable)."""
    s = _num(saldo)
    c = _num(cantidad)
    if c is None or c <= 0:
        return "Indica cuántas unidades se rebajan (mayor a 0)."
    if s is None:
        s = 0.0
    if c > s + 0.0049:
        return f"No se puede rebajar {c:g}: el repuesto tiene {s:g} en bodega."
    if len(str(argumento or "").strip()) < ARGUMENTO_MIN:
        return (f"Explica por qué se rebaja: el argumento necesita al menos {ARGUMENTO_MIN} caracteres "
                "(qué pasó, con qué equipo o cliente, quién lo constató).")
    t, n = normalizar_doc(tido, nudo)
    if not t or not n:
        return "Indica el documento de Random que respalda la rebaja (tipo y número)."
    return None


def validar_resolucion(accion, comentario):
    """None si la decisión del superadmin es válida. Rechazar exige decir por qué (queda a la vista de quien lo pidió)."""
    if accion not in ("aprobar", "rechazar"):
        return "Acción no válida."
    if accion == "rechazar" and len(str(comentario or "").strip()) < RESOLUCION_MIN:
        return f"Para rechazar escribe el motivo (mínimo {RESOLUCION_MIN} caracteres): lo verá quien pidió la rebaja."
    return None


def nota_kardex(rebaja_id, pidio, autorizo, tido, nudo, argumento, auto_autorizada=False):
    """Texto que queda en el kardex del repuesto (hasta 500 caracteres): quién pidió, quién autorizó, el documento y el argumento."""
    t, n = normalizar_doc(tido, nudo)
    doc = f"{t} {n}".strip() or "sin documento"
    quien = (f"Rebaja #{rebaja_id} hecha por {autorizo} (superadmin)" if auto_autorizada
             else f"Rebaja #{rebaja_id} pedida por {pidio}, autorizada por {autorizo}")
    cabeza = f"{quien} · doc. {doc} · "
    arg = " ".join(str(argumento or "").split())
    cupo = max(NOTA_KARDEX_MAX - len(cabeza), 0)
    if len(arg) > cupo:
        arg = arg[:max(cupo - 1, 0)].rstrip() + "…"
    return (cabeza + arg)[:NOTA_KARDEX_MAX]


def puede_aplicarse(saldo_actual, cantidad):
    """(ok, mensaje). Al aprobar se vuelve a mirar el saldo: si entre el pedido y la decisión bajó, no se deja el stock en negativo."""
    s = _num(saldo_actual) or 0.0
    c = _num(cantidad) or 0.0
    if c <= 0:
        return False, "La cantidad a rebajar no es válida."
    if c > s + 0.0049:
        return False, (f"El saldo cambió desde el pedido: hoy hay {s:g} y se pide rebajar {c:g}. "
                       "Recházala y que se pida de nuevo con la cantidad correcta.")
    return True, ""

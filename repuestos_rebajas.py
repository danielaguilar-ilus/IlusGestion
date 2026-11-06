# -*- coding: utf-8 -*-
"""Ajuste de cantidad de un repuesto que YA EXISTE: rebaja o aumento, con autorización + argumento + documento de Random OPCIONAL
(PURO, sin base de datos; probado en tests/test_repuestos_rebaja.py y tests/test_repuestos_ajuste.py).

Daniel (6-nov-2026, mañana): «los repuestos que están creados los están ajustando. Ahora el repuesto que esté creado necesita una
autorización para rebajar, necesita un argumento, una factura, algo, porque el ajuste manual es desorden y ya definitivamente no se va a
volver a realizar». Sus tres decisiones (AskUserQuestion, 6-nov): 1) autoriza SOLO el superadmin; 2) el respaldo es argumento + documento
del ERP Random; 3) se cierra TODO ajuste manual de cantidad (las salidas por instalación en OT, las entradas por recepción de proveedor o
alta).

Daniel (6-nov-2026, después, dictado mirando la pantalla de Repuestos): «Además de rebajar, para modificar las cantidades puede ser que no
esté amarrado a una factura, pero necesitan mi autorización; y cuando soy yo, reconóceme para que no me pidas autorización». Eso REEMPLAZA
la frase «las entradas siguen solo por recepción de proveedor o alta»: ahora existe el AJUSTE AUTORIZADO en los dos sentidos:
  · tipo «rebaja»  → baja el stock (kardex: salida · motivo `rebaja_autorizada`);
  · tipo «aumento» → sube el stock (kardex: entrada · motivo `ajuste_autorizado`), para cuando NO hay recepción de proveedor que lo respalde.
El documento de Random es OPCIONAL: con documento se valida contra el ERP (solo lectura); sin documento igual se puede pedir, pero SIEMPRE
necesita la autorización del superadmin y el argumento sigue siendo obligatorio. Si quien pide es el superadmin se le reconoce: se aprueba y
aplica en el acto, y la constancia dice «hecho por <nombre> (superadmin)». El flujo normal de entrada (recepción / alta) no cambia.
"""
import re

ARGUMENTO_MIN = 30
ARGUMENTO_MAX = 1000
RESOLUCION_MIN = 5
NOTA_KARDEX_MAX = 500          # largo de mant_repuestos_movimientos.nota
AUMENTO_MAX = 100000           # tope por pedido de aumento: evita un cero de más al teclear

ESTADOS = ("pendiente", "aprobada", "rechazada", "anulada")
TIPOS = ("rebaja", "aumento")

# Cómo queda en el kardex (mant_repuestos_movimientos): (tipo del movimiento, motivo_tipo).
MOVIMIENTO = {
    "rebaja": ("salida", "rebaja_autorizada"),
    "aumento": ("entrada", "ajuste_autorizado"),
}

# Palabras de cada tipo (género incluido) para los textos que ve una persona.
_PALABRAS = {
    "rebaja": {"nombre": "Rebaja", "verbo": "rebajar", "se": "se rebajan", "hecha": "hecha",
               "pedida": "pedida", "autorizada": "autorizada"},
    "aumento": {"nombre": "Aumento", "verbo": "aumentar", "se": "se aumentan", "hecha": "hecho",
                "pedida": "pedido", "autorizada": "autorizado"},
}

# Documentos del ERP que respaldan un ajuste: la lista que se ofrece en pantalla (cualquier otro código del ERP también se acepta si existe).
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


def numero(v):
    """La cantidad como número (acepta coma decimal) o None si no es un número."""
    return _num(v)


def _miles(n):
    """100000 → «100.000» (formato chileno)."""
    return f"{int(n):,}".replace(",", ".")


# ───────────────────────────── tipo de ajuste ─────────────────────────────
def normalizar_tipo(tipo, defecto="rebaja"):
    """«rebaja» | «aumento». Vacío o ausente → `defecto` (un navegador con la pantalla vieja solo conoce la rebaja).
    Cualquier otra cosa → None (no se adivina)."""
    t = str(tipo if tipo is not None else "").strip().lower()
    if not t:
        return defecto
    return t if t in TIPOS else None


def palabras(tipo):
    return _PALABRAS.get(tipo) or _PALABRAS["rebaja"]


def etiqueta_tipo(tipo):
    """«rebaja» → «Rebaja»; «aumento» → «Aumento»."""
    return palabras(tipo)["nombre"]


def delta_con_signo(tipo, cantidad):
    """Lo que se le suma al stock: una rebaja resta, un aumento suma."""
    c = _num(cantidad) or 0.0
    return -c if tipo == "rebaja" else c


# ───────────────────────────── documento de Random (opcional) ─────────────────────────────
def normalizar_doc(tido, nudo):
    """(TIDO en mayúsculas, número sin espacios) o ('', '') si no es un documento posible."""
    t = str(tido or "").strip().upper()
    n = str(nudo or "").strip()
    if not _TIDO_RE.match(t) or not _NUDO_RE.match(n):
        return "", ""
    return t, n


def evaluar_doc(tido, nudo):
    """(tido, nudo, error) del documento OPCIONAL.
    · ambos vacíos → ('', '', None): el ajuste se pide «sin documento» (siempre con autorización del superadmin);
    · uno solo, o los dos pero mal formados → ('', '', mensaje): si se escribe algo, tiene que formar un documento válido;
    · los dos bien → (TIDO, número, None): se valida contra el ERP al pedir."""
    t0 = str(tido or "").strip()
    n0 = str(nudo or "").strip()
    if not t0 and not n0:
        return "", "", None
    if t0 and not n0:
        return "", "", "Falta el número del documento de Random (o elige «Sin documento»)."
    if n0 and not t0:
        return "", "", "Falta el tipo del documento de Random (o deja el documento vacío para pedirlo sin documento)."
    t, n = normalizar_doc(t0, n0)
    if not t or not n:
        return "", "", "El documento de Random no es válido: revisa el tipo y el número (o pídelo sin documento)."
    return t, n, None


def hay_documento(tido, nudo):
    t, n, err = evaluar_doc(tido, nudo)
    return bool(t and n and not err)


def etiqueta_doc(tido):
    """«GDV» → «Guía de despacho (GDV)»; un código fuera de la lista se muestra tal cual."""
    t = str(tido or "").strip().upper()
    for cod, nom in TIPOS_DOC:
        if cod == t:
            return f"{nom} ({cod})"
    return t


def etiqueta_doc_completa(tido, nudo):
    """«Guía de despacho (GDV) 34190» o «Sin documento de Random»."""
    t = str(tido or "").strip().upper()
    n = str(nudo or "").strip()
    if not t or not n:
        return "Sin documento de Random"
    return f"{etiqueta_doc(t)} {n}".strip()


# ───────────────────────────── validaciones ─────────────────────────────
def validar_pedido(saldo, cantidad, argumento, tido, nudo, tipo="rebaja"):
    """None si el pedido es válido; si no, el mensaje que se le muestra a quien lo pide (claro y accionable).
    El documento es opcional (ver `evaluar_doc`); el argumento NO lo es, ni siquiera para el superadmin."""
    tipo_n = normalizar_tipo(tipo, defecto=None)
    if tipo_n is None:
        return "Indica si es una rebaja o un aumento."
    p = palabras(tipo_n)
    s = _num(saldo)
    c = _num(cantidad)
    if c is None or c <= 0:
        return f"Indica cuántas unidades {p['se']} (mayor a 0)."
    if s is None:
        s = 0.0
    if tipo_n == "rebaja" and c > s + 0.0049:
        return f"No se puede rebajar {c:g}: el repuesto tiene {s:g} en bodega."
    if tipo_n == "aumento" and c > AUMENTO_MAX:
        return f"El aumento máximo por pedido es {_miles(AUMENTO_MAX)} unidades (¿sobra un cero?)."
    if len(str(argumento or "").strip()) < ARGUMENTO_MIN:
        return (f"Explica por qué se {'rebaja' if tipo_n == 'rebaja' else 'aumenta'}: el argumento necesita al menos {ARGUMENTO_MIN} "
                "caracteres (qué pasó, con qué equipo o cliente, quién lo constató).")
    return evaluar_doc(tido, nudo)[2]


def validar_resolucion(accion, comentario):
    """None si la decisión del superadmin es válida. Rechazar exige decir por qué (queda a la vista de quien lo pidió)."""
    if accion not in ("aprobar", "rechazar"):
        return "Acción no válida."
    if accion == "rechazar" and len(str(comentario or "").strip()) < RESOLUCION_MIN:
        return f"Para rechazar escribe el motivo (mínimo {RESOLUCION_MIN} caracteres): lo verá quien pidió el ajuste."
    return None


def aplica_en_el_acto(es_superadmin):
    """El superadmin se reconoce: no se le pide autorización (se aprueba y aplica en el acto, con la misma constancia).
    Cualquier otra persona, con o sin documento, queda PENDIENTE hasta que el superadmin autorice."""
    return bool(es_superadmin)


# ───────────────────────────── constancia y textos ─────────────────────────────
def nota_kardex(rebaja_id, pidio, autorizo, tido, nudo, argumento, auto_autorizada=False, tipo="rebaja"):
    """Texto que queda en el kardex del repuesto (hasta 500 caracteres): quién pidió, quién autorizó, el documento (o que no lo hay) y el
    argumento."""
    p = palabras(normalizar_tipo(tipo) or "rebaja")
    t, n = normalizar_doc(tido, nudo)
    doc = f"doc. {t} {n}" if t and n else "SIN documento de Random"
    quien = (f"{p['nombre']} #{rebaja_id} {p['hecha']} por {autorizo} (superadmin)" if auto_autorizada
             else f"{p['nombre']} #{rebaja_id} {p['pedida']} por {pidio}, {p['autorizada']} por {autorizo}")
    cabeza = f"{quien} · {doc} · "
    arg = " ".join(str(argumento or "").split())
    cupo = max(NOTA_KARDEX_MAX - len(cabeza), 0)
    if len(arg) > cupo:
        arg = arg[:max(cupo - 1, 0)].rstrip() + "…"
    return (cabeza + arg)[:NOTA_KARDEX_MAX]


def resolucion_auto(tipo, autorizo, tido, nudo):
    """Texto de `resolucion` cuando el superadmin pide y se aplica en el acto (hasta 500 caracteres)."""
    p = palabras(normalizar_tipo(tipo) or "rebaja")
    doc = "con documento de Random" if hay_documento(tido, nudo) else "sin documento de Random"
    return f"{p['nombre']} {p['hecha']} por {autorizo} (superadmin), {doc}."[:500]


def resumen_pedido(tipo, cantidad, sku, tido, nudo):
    """«pide rebajar 2 u. de SKU-1 (GDV 34190)» / «pide aumentar 5 u. de SKU-1 (sin documento)» — para el aviso al superadmin."""
    p = palabras(normalizar_tipo(tipo) or "rebaja")
    t, n = normalizar_doc(tido, nudo)
    doc = f"{t} {n}" if t and n else "sin documento"
    c = _num(cantidad) or 0.0
    return f"pide {p['verbo']} {c:g} u. de {sku} ({doc})"


def puede_aplicarse(saldo_actual, cantidad, tipo="rebaja"):
    """(ok, mensaje). Al aprobar se vuelve a mirar el saldo: si entre el pedido y la decisión bajó, no se deja el stock en negativo."""
    tipo_n = normalizar_tipo(tipo, defecto=None)
    if tipo_n is None:
        return False, "El tipo de ajuste no es válido."
    s = _num(saldo_actual) or 0.0
    c = _num(cantidad) or 0.0
    if c <= 0:
        return False, f"La cantidad a {palabras(tipo_n)['verbo']} no es válida."
    if tipo_n == "aumento":
        if c > AUMENTO_MAX:
            return False, f"El aumento máximo por pedido es {_miles(AUMENTO_MAX)} unidades."
        return True, ""
    if c > s + 0.0049:
        return False, (f"El saldo cambió desde el pedido: hoy hay {s:g} y se pide rebajar {c:g}. "
                       "Recházala y que se pida de nuevo con la cantidad correcta.")
    return True, ""

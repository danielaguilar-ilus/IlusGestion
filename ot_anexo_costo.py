# -*- coding: utf-8 -*-
"""🧾 Un SOLO valor: lo que ILUS le paga al proveedor en la OT (costo_proveedor + costo_despacho) ES lo que dice el
anexo de servicios. LÓGICA PURA (sin Flask ni base de datos), con pruebas en tests/test_ot_anexo_costo.py.

Daniel (2026-10-20, OT 239 con el Anexo 221 firmado por $370.000 y la OT diciendo «costo no declarado»): «necesito
urgentemente que las finanzas y los costos del anexo sean vinculados: no son dos valores, es uno solo. Lo que estamos
pagando tiene que estar reflejado allí. El costo de cuánto me cobran la OT tiene que estar reflejado en el anexo, y lo que
me cobraron en el anexo tiene que estar reflejado en los costos de la OT».

La regla (el anexo vivo manda):
  · «Vivo» = el anexo de la OT que no está rechazado ni anulado (borrador, enviado, visto, vencido o firmado).
  · Los ítems del anexo salen de la OT (servicio = costo_proveedor, «Despacho» = costo_despacho: itemsDesdeOT en
    _modal_anexo.html), así que la cuenta es la misma que usa el candado de cierre ANEXO_DESACTUALIZADO:
        costo_proveedor + costo_despacho == total de los ítems del anexo.
  · Si el anexo trae un ítem «Despacho», ese es el costo_despacho y el resto es el costo_proveedor. Si no lo trae, el
    costo_despacho declarado se respeta y el costo_proveedor es lo que falta para llegar al total del anexo.
  · Mientras exista un anexo vivo, el costo de la OT no se escribe «a mano» por otro camino: se cambia el anexo (si aún no
    está firmado) o se emite uno nuevo (si ya está firmado), y la OT lo sigue sola.
"""
import json
import unicodedata

ESTADOS_NO_VIVOS = ("rechazado", "anulado")
TOLERANCIA = 1.0          # pesos: la misma holgura del candado de cierre (abs(anexo − costo) > 1)
ESTADOS_OT_CERRADOS = ("cerrada", "cancelada", "anulada")


def es_vivo(estado):
    """Un anexo rechazado o anulado ya no compromete a nadie; todos los demás estados sí."""
    return (estado or "").strip().lower() not in ESTADOS_NO_VIVOS


def clp(n):
    """$370.000 (sin decimales, punto de miles)."""
    try:
        return "$" + "{:,.0f}".format(float(n or 0)).replace(",", ".")
    except (TypeError, ValueError):
        return "$0"


def _num(v):
    """float o None (None = «no declarado», distinto de 0 = «no hubo»)."""
    if v is None or (isinstance(v, str) and not v.strip()):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def items_de(raw):
    """[{'concepto', 'monto'}] desde precio_items_json (texto JSON o lista). Lo que no se pueda leer se ignora."""
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8", "replace")
    if isinstance(raw, str):
        try:
            raw = json.loads(raw or "[]")
        except ValueError:
            raw = []
    out = []
    for it in (raw if isinstance(raw, list) else []):
        if not isinstance(it, dict):
            continue
        m = _num(it.get("monto"))
        out.append({"concepto": str(it.get("concepto") or "")[:120], "monto": int(round(m)) if m is not None else 0})
    return out


def total(items):
    return int(sum(i["monto"] for i in items_de(items)))


def _sin_tildes(s):
    t = unicodedata.normalize("NFD", str(s or ""))
    return "".join(c for c in t if unicodedata.category(c) != "Mn").lower().strip()


def es_despacho(concepto):
    """El ítem de despacho es el que el modal del anexo escribe como «Despacho» (itemsDesdeOT)."""
    return _sin_tildes(concepto).startswith("despacho")


def partir(items):
    """(servicio, despacho) del total del anexo."""
    its = items_de(items)
    desp = int(sum(i["monto"] for i in its if es_despacho(i["concepto"])))
    return int(sum(i["monto"] for i in its)) - desp, desp


def alinear(costo_prov, costo_desp, items):
    """Los costos que la OT debe tener para igualar al anexo.

    Devuelve {total, prov, desp, cambia, conflicto}. `desp` es None cuando no hay que tocar costo_despacho. `conflicto` es un
    texto cuando el anexo no alcanza para el despacho que la OT ya declaró (no se escribe nada: lo decide una persona)."""
    its = items_de(items)
    t = total(its)
    serv, desp_item = partir(its)
    p_ot, d_ot = _num(costo_prov), _num(costo_desp)
    if desp_item > 0:
        n_prov, n_desp = float(serv), float(desp_item)
    elif d_ot is None:
        n_prov, n_desp = float(t), None
    else:
        if t < d_ot - TOLERANCIA:
            return {"total": t, "prov": None, "desp": None, "cambia": False,
                    "conflicto": f"El anexo ({clp(t)}) es menor que el despacho que la OT declara ({clp(d_ot)})."}
        n_prov, n_desp = float(t) - d_ot, d_ot
    cambia = p_ot is None or abs(p_ot - n_prov) > TOLERANCIA
    if n_desp is not None and (d_ot is None or abs(d_ot - n_desp) > TOLERANCIA):
        cambia = True
    return {"total": t, "prov": n_prov, "desp": n_desp, "cambia": bool(cambia), "conflicto": None}


def estado(costo_prov, costo_desp, items):
    """'sin_declarar' (la OT no declara su costo) · 'alineado' (suman lo mismo que el anexo) · 'difiere'."""
    p, d = _num(costo_prov), _num(costo_desp)
    if p is None:
        return "sin_declarar"
    return "alineado" if abs((p + (d or 0.0)) - total(items)) <= TOLERANCIA else "difiere"


def cambio_permitido(prov_nuevo, desp_nuevo, prov_actual, desp_actual, total_anexo):
    """¿Se puede escribir ese costo con un anexo vivo? Sí cuando no cambia nada (volver a guardar lo mismo no se
    bloquea: la tarjeta de Finanzas manda todos sus campos juntos) o cuando el costo resultante suma EXACTAMENTE el
    total del anexo (por ejemplo, partir el total entre servicio y despacho)."""
    pn, dn, pa, da = _num(prov_nuevo), _num(desp_nuevo), _num(prov_actual), _num(desp_actual)
    cambia = (pn is not None and (pa is None or abs(pn - pa) > 0.5)) or (dn is not None and (da is None or abs(dn - da) > 0.5))
    if not cambia:
        return True
    p = pn if pn is not None else (pa or 0.0)
    d = dn if dn is not None else (da or 0.0)
    return abs((p + d) - float(total_anexo or 0)) <= TOLERANCIA


def mensaje_costo_es_del_anexo(numero, estado_anexo, total_anexo):
    """Lo que se le dice a quien intenta cambiar el costo a mano teniendo un anexo vivo."""
    firmado = (estado_anexo or "").strip().lower() == "firmado"
    salida = ("Si cambió lo que se le paga, emite un anexo nuevo con el monto correcto." if firmado else
              "Si cambió lo que se le paga, corrige el monto en el anexo (aún no está firmado) y la OT lo sigue sola.")
    return (f"Lo que se le paga al proveedor es UN solo valor y sale del Anexo N° {numero} "
            f"({'firmado' if firmado else 'vigente'}, {clp(total_anexo)}): el costo de la OT no puede ser distinto. " + salida)

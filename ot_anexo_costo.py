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

🧾 ANEXO ÚNICO (Daniel, 2026-10-25: «el anexo debe ser único… describiendo el detalle de cuánto se me va a cobrar por
instalación, desinstalación y despacho, algo bien claro para evitar problemas»):
  · Una instalación con desinstalación tiene UN solo anexo con el proveedor (el de la OT de entrada). Cada partida lleva
    la OT a la que pertenece en `ot`: 'entrada' (la instalación; es también lo que significa una partida SIN etiqueta,
    así que todos los anexos de antes siguen valiendo igual) o 'salida' (la desinstalación).
  · Cada OT se cuadra con SU parte: todas las funciones de abajo aceptan `rol`; sin `rol` miran el anexo completo.
"""
import json
import unicodedata

ESTADOS_NO_VIVOS = ("rechazado", "anulado")
TOLERANCIA = 1.0          # pesos: la misma holgura del candado de cierre (abs(anexo − costo) > 1)
ESTADOS_OT_CERRADOS = ("cerrada", "cancelada", "anulada")

ROL_ENTRADA = "entrada"
ROL_SALIDA = "salida"
ROLES = (ROL_ENTRADA, ROL_SALIDA)
REL_DESINSTALACION = "desinstalacion"   # `ot_relacion` de la OT hija que comparte el anexo de su instalación


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


def _sin_tildes(s):
    t = unicodedata.normalize("NFD", str(s or ""))
    return "".join(c for c in t if unicodedata.category(c) != "Mn").lower().strip()


def rol_valido(v):
    """'entrada' | 'salida' | None (cualquier otra cosa no es una etiqueta)."""
    t = _sin_tildes(v)
    return t if t in ROLES else None


def items_de(raw):
    """[{'concepto', 'monto'[, 'ot']}] desde precio_items_json (texto JSON o lista). Lo que no se pueda leer se ignora. La etiqueta
    `ot` solo aparece cuando la partida la trae (una partida sin etiqueta es de la OT de entrada)."""
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
        fila = {"concepto": str(it.get("concepto") or "")[:120], "monto": int(round(m)) if m is not None else 0}
        rol = rol_valido(it.get("ot"))
        if rol:
            fila["ot"] = rol
        out.append(fila)
    return out


def rol_de(item):
    """La OT a la que pertenece una partida: la etiquetada, o la de entrada si no trae etiqueta."""
    return rol_valido((item or {}).get("ot")) or ROL_ENTRADA


def tiene_salida(items):
    """¿El anexo incluye partidas de la desinstalación? (= es un anexo compartido por la familia de OT)."""
    return any(rol_de(i) == ROL_SALIDA for i in items_de(items))


def parte(items, rol=None):
    """Las partidas de una OT. Sin `rol`, todas."""
    its = items_de(items)
    if rol is None:
        return its
    return [i for i in its if rol_de(i) == rol]


def etiquetar(items, rol):
    """Las mismas partidas, todas marcadas como de la OT `rol` (para armar un anexo compartido)."""
    r = rol_valido(rol) or ROL_ENTRADA
    return [dict(i, ot=r) for i in items_de(items)]


def total(items, rol=None):
    return int(sum(i["monto"] for i in parte(items, rol)))


def es_despacho(concepto):
    """El ítem de despacho es el que el modal del anexo escribe como «Despacho» (itemsDesdeOT)."""
    return _sin_tildes(concepto).startswith("despacho")


def partir(items, rol=None):
    """(servicio, despacho) del total del anexo — o de la parte de la OT `rol`."""
    its = parte(items, rol)
    desp = int(sum(i["monto"] for i in its if es_despacho(i["concepto"])))
    return int(sum(i["monto"] for i in its)) - desp, desp


def resumen_partidas(items):
    """Lo que el anexo le cobra a ILUS, separado por OT: {compartido, total, entrada:{items,total,servicio,despacho},
    salida:{…}}. Para las pantallas («cuánto se me va a cobrar por instalación, desinstalación y despacho»)."""
    its = items_de(items)
    out = {"compartido": tiene_salida(its), "total": int(sum(i["monto"] for i in its))}
    for rol in ROLES:
        p = [i for i in its if rol_de(i) == rol]
        serv, desp = partir(its, rol)
        out[rol] = {"items": p, "total": int(sum(i["monto"] for i in p)), "servicio": serv, "despacho": desp}
    return out


def alinear(costo_prov, costo_desp, items, rol=None):
    """Los costos que la OT debe tener para igualar al anexo (o a su parte, con `rol`).

    Devuelve {total, prov, desp, cambia, conflicto}. `desp` es None cuando no hay que tocar costo_despacho. `conflicto` es un
    texto cuando el anexo no alcanza para el despacho que la OT ya declaró (no se escribe nada: lo decide una persona)."""
    its = parte(items, rol)
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


def estado(costo_prov, costo_desp, items, rol=None):
    """'sin_declarar' (la OT no declara su costo) · 'alineado' (suman lo mismo que el anexo) · 'difiere'."""
    p, d = _num(costo_prov), _num(costo_desp)
    if p is None:
        return "sin_declarar"
    return "alineado" if abs((p + (d or 0.0)) - total(items, rol)) <= TOLERANCIA else "difiere"


def cambio_permitido(prov_nuevo, desp_nuevo, prov_actual, desp_actual, total_anexo):
    """¿Se puede escribir ese costo con un anexo vivo? Sí cuando no cambia nada (volver a guardar lo mismo no se
    bloquea: la tarjeta de Finanzas manda todos sus campos juntos) o cuando el costo resultante suma EXACTAMENTE el
    total del anexo (por ejemplo, partir el total entre servicio y despacho). Con un anexo compartido, `total_anexo` es
    el de la parte de ESA OT."""
    pn, dn, pa, da = _num(prov_nuevo), _num(desp_nuevo), _num(prov_actual), _num(desp_actual)
    cambia = (pn is not None and (pa is None or abs(pn - pa) > 0.5)) or (dn is not None and (da is None or abs(dn - da) > 0.5))
    if not cambia:
        return True
    p = pn if pn is not None else (pa or 0.0)
    d = dn if dn is not None else (da or 0.0)
    return abs((p + d) - float(total_anexo or 0)) <= TOLERANCIA


def mensaje_costo_es_del_anexo(numero, estado_anexo, total_anexo, total_general=None):
    """Lo que se le dice a quien intenta cambiar el costo a mano teniendo un anexo vivo. Con un anexo compartido
    (`total_general` distinto del de esta OT) dice cuánto es SU parte y cuánto el anexo completo."""
    firmado = (estado_anexo or "").strip().lower() == "firmado"
    salida = ("Si cambió lo que se le paga, emite un anexo nuevo con el monto correcto." if firmado else
              "Si cambió lo que se le paga, corrige el monto en el anexo (aún no está firmado) y la OT lo sigue sola.")
    cond = "firmado" if firmado else "vigente"
    if total_general is not None and abs(float(total_general) - float(total_anexo or 0)) > TOLERANCIA:
        return (f"Lo que se le paga al proveedor es UN solo valor y sale de la parte de esta OT en el Anexo N° {numero} "
                f"({cond}): {clp(total_anexo)} de {clp(total_general)} del anexo completo. "
                "El costo de la OT no puede ser distinto. " + salida)
    return (f"Lo que se le paga al proveedor es UN solo valor y sale del Anexo N° {numero} "
            f"({cond}, {clp(total_anexo)}): el costo de la OT no puede ser distinto. " + salida)

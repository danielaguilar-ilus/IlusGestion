# -*- coding: utf-8 -*-
"""Análisis de cada ítem para el «Detalle de cálculo» de una cotización (PURO, sin base de datos; probado en tests/test_cot_detalle_calculo.py).

Daniel (6-nov-2026): «podemos reparar ese PDF, hacerlo con el diseño para las dudas más adelante y el formato de PDF para las dudas».
El detalle suponía que TODO precio sale de «costo × (1 + margen)». No es así: un precio puede ser automático, escrito a mano, fijo de la categoría,
un mínimo (piso), no cobrable o no tener tarifa. Este módulo dice de dónde salió cada precio, cuánto cuesta de verdad y cuánto margen deja,
para que el documento sirva para responder dudas sin adivinar. Solo superadmin (REGLAS #19 y #26): es costo y margen.
"""
from decimal import Decimal, ROUND_HALF_UP

ORIGENES = {
    "automatico": "Automático (costo + margen)",
    "manual": "Precio escrito a mano",
    "fijo": "Precio fijo de la categoría",
    "piso": "Mínimo de la categoría",
    "no_cobrable": "No cobrable",
    "sin_tarifa": "Sin tarifa",
}


def _r(x):
    """Redondeo a peso entero, mitad hacia arriba (igual que _tk_money_round del cotizador)."""
    return int(Decimal(str(x)).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def _clp(n):
    """$1.234.567 (formato chileno)."""
    return "${:,}".format(int(n or 0)).replace(",", ".")


def _num(v, defecto=None):
    try:
        return float(v)
    except (TypeError, ValueError):
        return defecto


def analizar_item(*, cantidad, precio_unitario, total, precio_manual=None, tarifa_fijo=None, precio_piso=None,
                  horas=None, tecnicos=None, valor_hh=None, margen_pct=0, sin_tarifa=False, no_cobrable=False):
    """Dict con: origen, origen_texto, costo_unit, costo_total, margen_unit, margen_total, margen_pct_real (sobre el costo),
    precio_hoy (lo que daría HOY el cálculo automático) y difiere_hoy (el precio guardado ≠ el de hoy, solo si era automático)."""
    cant = max(int(_num(cantidad, 0) or 0), 0)
    pu = int(_num(precio_unitario, 0) or 0)
    tot = int(_num(total, 0) or 0)
    base = {"origen": None, "origen_texto": None, "costo_unit": None, "costo_total": None, "margen_unit": None,
            "margen_total": None, "margen_pct_real": None, "precio_hoy": None, "difiere_hoy": False}

    if no_cobrable:
        base.update(origen="no_cobrable")
    elif precio_manual not in (None, ""):
        base.update(origen="manual")
    elif tarifa_fijo is not None:
        base.update(origen="fijo")
    elif sin_tarifa:
        base.update(origen="sin_tarifa")
    else:
        base.update(origen="automatico")

    # Costo de la mano de obra: solo existe si la categoría se calcula por horas y técnicos (no por precio fijo) y hay tarifa.
    h, t, v = _num(horas), _num(tecnicos), _num(valor_hh)
    tiene_costo = (not sin_tarifa and tarifa_fijo is None and None not in (h, t, v))
    if tiene_costo and not no_cobrable:
        costo = _r(h * t * v)
        base["costo_unit"] = costo
        base["costo_total"] = costo * cant
        auto = _r(costo * (1 + (_num(margen_pct, 0) or 0) / 100.0))
        if precio_piso is not None and auto < _r(precio_piso):
            auto = _r(precio_piso)
            if base["origen"] == "automatico" and pu == auto:
                base["origen"] = "piso"
        base["precio_hoy"] = auto
        if base["origen"] in ("automatico", "piso"):
            base["difiere_hoy"] = pu != auto
        base["margen_unit"] = pu - costo
        base["margen_total"] = tot - costo * cant
        base["margen_pct_real"] = round((pu - costo) / costo * 100.0, 1) if costo > 0 else None
    elif tarifa_fijo is not None and not no_cobrable:
        base["precio_hoy"] = _r(tarifa_fijo)
        if base["origen"] == "fijo":
            base["difiere_hoy"] = pu != base["precio_hoy"]

    base["origen_texto"] = ORIGENES.get(base["origen"])
    return base


def resumen(items):
    """Totales para el recuadro «Resultado interno»: solo cuentan los ítems con costo conocido (los de precio fijo o sin tarifa se avisan aparte)."""
    costo = cobrado = 0
    con_costo = sin_costo = cambiaron = a_mano = perdida = 0
    for it in items or []:
        a = it.get("analisis") or {}
        if a.get("origen") == "no_cobrable":
            continue
        if a.get("difiere_hoy"):
            cambiaron += 1
        if a.get("origen") == "manual":
            a_mano += 1
        if a.get("costo_total") is not None:
            con_costo += 1
            costo += a["costo_total"]
            cobrado += int(it.get("total") or 0)
            if (a.get("margen_total") or 0) < 0:
                perdida += 1
        elif int(it.get("total") or 0) > 0:
            sin_costo += 1
    margen = cobrado - costo
    return {
        "costo_total": costo, "cobrado": cobrado, "margen": margen,
        "margen_pct": round(margen / costo * 100.0, 1) if costo > 0 else None,
        "items_con_costo": con_costo, "items_sin_costo": sin_costo,
        "items_tarifa_cambio": cambiaron, "items_a_mano": a_mano, "items_con_perdida": perdida,
    }


def dudas(res, *, tipo_servicio_label, ruta_excluida=False, costo_ruta=0):
    """Respuestas cortas a las dudas que más se repiten, con los números de ESTA cotización (el cuerpo de «Para resolver dudas»)."""
    d = []
    if res["items_con_costo"]:
        pct = ("" if res["margen_pct"] is None else f" ({str(res['margen_pct']).replace('.', ',')} % sobre el costo)")
        d.append(("¿Cuánto margen deja la mano de obra?",
                  f"Se cobra {_clp(res['cobrado'])} y cuesta {_clp(res['costo_total'])}: queda {_clp(res['margen'])}{pct}. "
                  "La ruta se cobra aparte y no cuenta como margen."))
    else:
        d.append(("¿Cuánto margen deja la mano de obra?",
                  "No se puede calcular: ninguno de los ítems tiene costo registrado (precio fijo o sin tarifa)."))
    if res["items_a_mano"]:
        n = res["items_a_mano"]
        d.append(("¿Por qué un precio no calza con «costo + margen»?",
                  f"{n} ítem{'s' if n != 1 else ''} tiene{'n' if n != 1 else ''} el precio escrito a mano: manda lo escrito, no la tarifa. "
                  "La columna «Origen del precio» dice cuáles y cuánto habría sido el automático."))
    if res["items_con_perdida"]:
        n = res["items_con_perdida"]
        d.append(("¿Hay ítems que se cobran bajo su costo?",
                  f"Sí: {n} ítem{'s' if n != 1 else ''}. Se ven en rojo en la columna «Margen real»."))
    d.append(("¿La tarifa cambió desde que se cotizó?",
              (f"Sí, en {res['items_tarifa_cambio']} ítem(s): el precio guardado es distinto del que daría hoy el cálculo automático "
               "(se marca en la tabla). Los montos de la cotización no cambian solos; para actualizarlos hay que recotizar.")
              if res["items_tarifa_cambio"] else
              "No. El precio guardado de cada ítem automático coincide con lo que daría hoy el cálculo."))
    if res["items_sin_costo"]:
        d.append(("¿Por qué algunos ítems no muestran costo?",
                  "Son de precio fijo de la categoría o no tienen horas y técnicos cargados: el sistema no conoce su costo, así que no calcula su margen."))
    d.append(("¿Y la ruta?",
              ("Está EXCLUIDA de esta cotización a pedido del cliente: el monto es solo de referencia y no suma."
               if ruta_excluida else
               (f"Se cobra aparte según la comuna del cliente ({_clp(costo_ruta)})."
                if costo_ruta else "No se cobra ruta en esta cotización."))))
    return d

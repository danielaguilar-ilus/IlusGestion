# -*- coding: utf-8 -*-
"""Referencia del costo de DESPACHO de una OT (3-nov-2026).

Daniel, mirando cómo Juan Pablo crea una OT: el despacho tiene que verse y ser editable, y la sugerencia debe venir del motor de cotización de
logística («cuánto vale llevar esos productos»). Este módulo es la parte PURA (sin Flask ni base de datos, con pruebas): suma el peso de los
productos de los documentos de la OT y elige qué cotización mostrar. El endpoint (`POST /ot/api/despacho-referencia`, app.py) lee los documentos
del ERP en SOLO LECTURA (REGLA #4.1) y consulta el comparador de couriers del Cubicador tal cual está (cubicador_plus._invocar_comparador): aquí no se
reimplementa ninguna tarifa.

Confidencialidad: solo se devuelve el courier, el monto, si es estimado y el plazo. Nada de «regla comercial», trazas ni motivos del comparador
(la política de Felca/Milling es confidencial: ver cubicador_plus._marcar_estimados). Es una REFERENCIA: lo que cobre el proveedor de la OT puede
ser distinto y la persona escribe el monto que quiera.
"""

MAX_DOCUMENTOS = 6


def _num(v):
    try:
        n = float(v)
    except (TypeError, ValueError):
        return 0.0
    return n if n == n and n not in (float("inf"), float("-inf")) else 0.0


def documentos_validos(documentos):
    """Lista limpia de {tido, nudo} (sin repetidos, máximo MAX_DOCUMENTOS). Descarta lo que no trae número."""
    out, vistos = [], set()
    for d in documentos or []:
        if not isinstance(d, dict):
            continue
        tido = str(d.get("tido") or "").strip().upper()[:10]
        nudo = str(d.get("nudo") or "").strip()[:30]
        if not tido or not nudo or (tido, nudo) in vistos:
            continue
        vistos.add((tido, nudo))
        out.append({"tido": tido, "nudo": nudo})
        if len(out) >= MAX_DOCUMENTOS:
            break
    return out


def resumir_lineas(lineas, es_servicio=None):
    """Suma el peso facturable de las líneas de PRODUCTO de un documento (las de servicio, descuento y las ZZ no se llevan en un camión).

    `lineas` = lo que devuelve `_cubicador_fetch`: cada una con `pred_tot` (peso predominante total de la línea, ya multiplicado por la cantidad),
    `cantidad`, `es_zz`, `es_descuento`. `es_servicio(sku, descripcion, es_zz)` es opcional (cubicador_plus._es_linea_servicio).
    Una línea de producto SIN medidas cargadas (pred_tot = 0) no pesa nada: se lista aparte para avisar que la referencia puede quedar corta."""
    peso, productos, sin_medidas = 0.0, 0, []
    for l in lineas or []:
        if not isinstance(l, dict) or l.get("es_zz") or l.get("es_descuento"):
            continue
        sku, desc = str(l.get("sku") or ""), str(l.get("descripcion_erp") or l.get("descripcion") or "")
        if es_servicio is not None:
            try:
                if es_servicio(sku, desc, bool(l.get("es_zz"))):
                    continue
            except Exception:
                pass
        if _num(l.get("cantidad")) <= 0:
            continue
        productos += 1
        pred = _num(l.get("pred_tot"))
        if pred > 0:
            peso += pred
        else:
            sin_medidas.append({"sku": sku, "descripcion": desc})
    return {"peso_kg": round(peso, 2), "productos": productos, "sin_medidas": sin_medidas}


def _precio(c):
    for campo in ("subtotal", "precio"):
        n = _num(c.get(campo))
        if n > 0:
            return n
    return None


def elegir_sugerencia(cotizaciones, recomendado_id=None, max_otras=2):
    """Qué cotización mostrar: la recomendada (la más barata con tarifa FIRME). Si todas son estimadas, la más barata, marcada `estimado`.
    Devuelve None si ningún courier entregó un precio utilizable. Incluye hasta `max_otras` alternativas firmes para comparar."""
    validas = [c for c in (cotizaciones or []) if isinstance(c, dict) and c.get("tiene_cobertura") and _precio(c) is not None]
    if not validas:
        return None
    firmes = [c for c in validas if not c.get("es_estimado")]
    elegido = None
    if recomendado_id is not None:
        elegido = next((c for c in firmes if c.get("courier_id") == recomendado_id), None)
    if elegido is None:
        elegido = min(firmes or validas, key=_precio)

    def _ficha(c):
        return {"courier": str(c.get("courier_nombre") or c.get("nombre") or "Courier"), "monto": int(round(_precio(c))),
                "estimado": bool(c.get("es_estimado")), "plazo": str(c.get("tiempo_transito") or "")}

    otras = sorted((c for c in firmes if c is not elegido), key=_precio)[:max_otras]
    return dict(_ficha(elegido), otras=[_ficha(c) for c in otras])

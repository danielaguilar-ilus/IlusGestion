# -*- coding: utf-8 -*-
"""Productos de documentos SIN SALDO que entran a una cotización (PURO, sin base de datos; probado en tests/test_cot_sin_saldo.py).

Daniel (6-nov-2026): «desbloquear una o varias facturas sin saldo y dejar un comentario que quede en la actividad de la cotización».
El buscador de productos (_tka_modal.html) ya pedía un motivo por línea sin saldo (`marcada_sin_saldo` + `motivo_sin_saldo`); ahora ese motivo
viaja hasta la cotización y queda en su historial (tk_cotizacion_log, acción «sin_saldo») con quién, cuándo y por qué.
"""

MAX_COMENTARIO = 500
MAX_PRODUCTOS_POR_EVENTO = 60


def _txt(v, n):
    return str(v or "").strip()[:n]


def eventos_sin_saldo(items):
    """Un evento por cada comentario distinto: {comentario, documentos:[«GDV 34190»…], productos:[{sku,nombre,documento}], total}.

    Solo cuentan las líneas marcadas sin saldo CON comentario (sin comentario no hay nada que dejar en la actividad: el buscador no deja
    pasar una línea sin saldo sin motivo, así que esto solo protege de datos mal formados). Se repite una línea (sku + documento) una sola vez.
    """
    if not isinstance(items, list):
        return []
    grupos, orden = {}, []
    for it in items:
        if not isinstance(it, dict) or not it.get("marcada_sin_saldo"):
            continue
        comentario = _txt(it.get("motivo_sin_saldo"), MAX_COMENTARIO)
        if not comentario:
            continue
        tido = _txt(it.get("tido"), 10).upper()
        nudo = _txt(it.get("nudo"), 30)
        doc = (tido + " " + nudo).strip() or "documento sin identificar"
        g = grupos.get(comentario)
        if g is None:
            g = grupos[comentario] = {"comentario": comentario, "documentos": [], "productos": [], "_vistos": set()}
            orden.append(comentario)
        clave = (_txt(it.get("sku"), 100).upper(), doc)
        if clave in g["_vistos"]:
            continue
        g["_vistos"].add(clave)
        if doc not in g["documentos"]:
            g["documentos"].append(doc)
        g["productos"].append({"sku": _txt(it.get("sku"), 100), "nombre": _txt(it.get("nombre"), 160), "documento": doc})
    salida = []
    for c in orden:
        g = grupos[c]
        total = len(g["productos"])
        salida.append({"comentario": g["comentario"], "documentos": g["documentos"],
                       "productos": g["productos"][:MAX_PRODUCTOS_POR_EVENTO], "total": total})
    return salida

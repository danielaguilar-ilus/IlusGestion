# -*- coding: utf-8 -*-
"""🎯 Política de margen de Servicio Técnico (REGLA #57, Daniel 2026-10-29).

«Margen objetivo 30 %; tolerancia hasta 20 %; bajo 20 % amarillo; bajo 0 % rojo.»

MÓDULO PURO (sin Flask, sin base, sin Random ni Check). SOLO sirve para PINTAR: el color de una OT o de un grupo de OT.
Nunca es un candado de cierre ni de pago (la pérdida real sigue pidiendo la autorización de Daniel, REGLA #28).
2026-10-29: el umbral de «margen bajo» de la ficha y de todo el proyecto quedó ALINEADO con esta tolerancia (20 %): `_OT_FIN_UMBRAL_BAJO`
y `_vida_margen_clase` en app.py, `UMBRAL_BAJO` en static/ot_finanzas.js, `window.OTD_FIN_UMBRAL_BAJO` en detalle.html, `UMBRAL_BAJO` en
ot_gerencial.py y `MARGEN_SANO` en ot_doc_cadena.py. tests/test_ot_margen_alineado.py falla si alguno de esos espejos se desvía.

Las bandas (el % es lo que queda sobre lo cobrado, «Cobré − Me cobraron = Queda»):
  · objetivo    → 30 % o más                       (verde)
  · tolerancia  → 20 % o más y menos de 30 %       (verde claro: se acepta, pero no es la meta)
  · amarillo    → 0 % o más y menos de 20 %        (ámbar: margen bajo)
  · rojo        → menos de 0 %                     (rojo: se perdió plata de verdad)
  · None        → sin medir: no hay % (la OT no se cobra, le falta un costo o el cobro es $0). Nunca se inventa 0 ni 100 %.

El navegador recibe los cortes desde aquí (`como_json`): no hay un segundo número escrito a mano en el JS.
"""
from __future__ import annotations

OBJETIVO_PCT = 30.0
TOLERANCIA_PCT = 20.0
PERDIDA_PCT = 0.0

BANDAS = ("objetivo", "tolerancia", "amarillo", "rojo")
BANDA_TXT = {
    "objetivo": "Objetivo (30 % o más)",
    "tolerancia": "Tolerancia (20 % a menos de 30 %)",
    "amarillo": "Margen bajo (0 % a menos de 20 %)",
    "rojo": "Pérdida (menos de 0 %)",
}
BANDA_CORTA = {"objetivo": "Objetivo", "tolerancia": "Tolerancia", "amarillo": "Bajo", "rojo": "Pérdida"}


def _num(x):
    if x is None or x == "":
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def banda(pct, queda=None):
    """La banda de un margen `pct` (%). Sin % pero con lo que queda en negativo (cobro declarado en $0 con costo) es
    'rojo': se perdió plata aunque no haya porcentaje. Sin % y sin pérdida → None (sin medir)."""
    p = _num(pct)
    if p is None:
        q = _num(queda)
        return "rojo" if (q is not None and q < 0) else None
    if p < PERDIDA_PCT:
        return "rojo"
    if p < TOLERANCIA_PCT:
        return "amarillo"
    if p < OBJETIVO_PCT:
        return "tolerancia"
    return "objetivo"


def texto(b):
    """El rótulo largo de una banda; «Sin medir» si no hay."""
    return BANDA_TXT.get(b, "Sin medir")


def como_json():
    """Los cortes para el navegador (una sola fuente: este módulo)."""
    return {"objetivo": OBJETIVO_PCT, "tolerancia": TOLERANCIA_PCT, "perdida": PERDIDA_PCT,
            "txt": dict(BANDA_TXT), "corta": dict(BANDA_CORTA), "orden": list(BANDAS)}

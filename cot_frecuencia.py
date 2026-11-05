# -*- coding: utf-8 -*-
"""Frecuencia de mantención RECOMENDADA en la cotización (PURO, sin base de datos; probado en tests/test_cot_frecuencia.py).

Daniel (6-nov-2026): «en las mantenciones me gustaría que la frecuencia de mantención la podamos cambiar y que indique claramente que
recomendamos una frecuencia pero la cotización es por una visita: no cubre la frecuencia anual».
La frecuencia es una RECOMENDACIÓN al cliente. Lo que se cotiza (y se cobra) es UNA visita de mantención de los equipos del detalle.
"""

POR_DEFECTO = 2
MINIMO = 1
MAXIMO = 12

AVISO_UNA_VISITA = ("Esta cotización corresponde a UNA (1) visita de mantención de los equipos del detalle. "
                    "La frecuencia anual indicada es una recomendación técnica y NO está incluida en este valor.")

_NOMBRES = {1: "una", 2: "dos", 3: "tres", 4: "cuatro", 5: "cinco", 6: "seis", 7: "siete", 8: "ocho",
            9: "nueve", 10: "diez", 11: "once", 12: "doce"}
_CADA = {2: "seis meses", 3: "cuatro meses", 4: "tres meses", 6: "dos meses", 12: "mes"}


def normalizar(valor):
    """Veces al año recomendadas, entre 1 y 12; vacío o inválido = 2 (lo de siempre)."""
    try:
        n = int(float(str(valor).replace(",", ".")))
    except (TypeError, ValueError):
        return POR_DEFECTO
    if n < MINIMO:
        return POR_DEFECTO if n == 0 else MINIMO
    return min(n, MAXIMO)


def veces_al_anio(n):
    """«una vez al año» / «dos veces al año» / «4 veces al año»."""
    n = normalizar(n)
    return "una vez al año" if n == 1 else f"{_NOMBRES.get(n, n)} veces al año"


def intervenciones(n):
    """«una intervención anual» / «dos intervenciones anuales, separadas aproximadamente cada seis meses»."""
    n = normalizar(n)
    base = "una intervención anual" if n == 1 else f"{_NOMBRES.get(n, n)} intervenciones anuales"
    cada = _CADA.get(n)
    if cada:
        base += (", una por mes" if n == 12 else f", separadas aproximadamente cada {cada}")
    return base


def parrafo_mantencion(n, uf_info=False):
    """Párrafo de la propuesta: recomienda la frecuencia y deja explícito que lo cotizado es UNA visita."""
    n = normalizar(n)
    plural = "mantención preventiva" if n == 1 else "mantenciones preventivas"
    txt = (f"Recomendamos ejecutar {n} {plural} al año"
           + (", idealmente con una separación aproximada de seis meses" if n == 2 else "")
           + ". Esta frecuencia permite anticipar fallas, preservar la operatividad de los equipos y mantener una adecuada "
             "trazabilidad técnica. " + AVISO_UNA_VISITA)
    if uf_info:
        txt += (" Las mantenciones posteriores se calcularán utilizando los mismos precios expresados en UF y la UF vigente a la "
                "fecha del servicio.")
    return txt


def recomendaciones_por_defecto(n, previas=0):
    """Líneas de «Recomendación de Servicio» cuando nadie las escribió: la frecuencia sale de la que se eligió (antes decía siempre «dos»)."""
    n = normalizar(n)
    freq = f"Frecuencia recomendada: {intervenciones(n)} (esta cotización cubre solo una visita)."
    if previas and previas > 0:
        return [
            f"Cliente recurrente: ya registra {previas} mantención(es) previa(s) con nosotros — se dará continuidad a la trazabilidad de cada equipo.",
            "Comparar hallazgos con la visita anterior para anticipar desgaste y repuestos.",
            freq,
            "Cotizar previamente repuestos o trabajos correctivos que requieran autorización.",
        ]
    return [
        "Primera mantención registrada para este cliente: se levantará la línea base del estado de cada equipo.",
        freq,
        "Emitir un informe técnico al cierre con hallazgos y recomendaciones.",
        "Cotizar previamente repuestos o trabajos correctivos que requieran autorización.",
    ]

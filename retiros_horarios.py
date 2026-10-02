# -*- coding: utf-8 -*-
"""Días especiales de la bodega de Retiros: feriados, víspera y salida temprano.

Funciones PURAS (sin BD, sin Flask): pickups_module.py les entrega los datos ya
leídos y aquí solo se decide. Así se prueban sin base de datos (tests/test_retiros_horarios.py).

Daniel 2026-09-30: "bloquear el horario cuando se salga más temprano, por ejemplo
el 12 de octubre es feriado; una inteligencia de aplicación de los feriados, bien
controlable por el front".

Convención (la misma que el "Cierra" del horario normal, ver time_allowed en
pickups_module): la hora de cierre es la ÚLTIMA HORA DE LLEGADA admitida. Con cierre
16:30 el último bloque es 16:30-17:00.
"""
from datetime import timedelta

# Feriados IRRENUNCIABLES del comercio en Chile (Código del Trabajo, art. 38 bis): no se puede obligar a
# trabajar ese día. Los demás feriados son "normales" y la bodega decide si cierra. (Los días de elección
# también son irrenunciables, pero cambian cada vez y no se pueden calcular aquí: se cierran a mano.)
IRRENUNCIABLES_MES_DIA = frozenset({(1, 1), (5, 1), (9, 18), (9, 19), (12, 25)})

TOPE_DIAS_VISPERA = 10   # ninguna racha de días cerrados seguidos en Chile pasa de esto


def hhmm_a_min(valor):
    """'HH:MM' → minutos desde medianoche. None si no es una hora válida."""
    try:
        h, m = str(valor).strip()[:5].split(":")
        h, m = int(h), int(m)
    except (ValueError, AttributeError):
        return None
    if not (0 <= h <= 23 and 0 <= m <= 59):
        return None
    return h * 60 + m


def min_a_hhmm(minutos):
    return f"{minutos // 60:02d}:{minutos % 60:02d}"


def vispera_de(dia, cerrado, feriado):
    """Si `dia` es víspera de feriado devuelve la fecha del PRIMER feriado que sigue; si no, None.

    Víspera = un día que la bodega abre y cuyo siguiente tramo cerrado incluye un
    feriado (ej.: el viernes antes de un lunes feriado, o el jueves antes de un viernes
    feriado). Un fin de semana solo, sin feriado, NO es víspera.

    cerrado(d) -> bool : la bodega no abre ese día (fin de semana, feriado, cierre total)
    feriado(d) -> bool : feriado legal no reabierto, o cierre extra declarado
    """
    if cerrado(dia):
        return None
    primero = None
    sig = dia + timedelta(days=1)
    for _ in range(TOPE_DIAS_VISPERA):
        if not cerrado(sig):
            return primero
        if primero is None and feriado(sig):
            primero = sig
        sig += timedelta(days=1)
    return primero


def es_vispera(dia, cerrado, feriado):
    return vispera_de(dia, cerrado, feriado) is not None


def cierre_efectivo(normal, regla=None, vispera=False, vispera_activa=False, vispera_hasta=None):
    """Última hora de llegada de un día ABIERTO → (hhmm, origen, motivo).

    origen: 'temprano' (regla manual del día) | 'vispera' (automática) | 'normal'.
    Prioridad: regla manual del día > víspera > horario normal. Una salida más tarde
    que el horario normal se ignora: las excepciones solo pueden ADELANTAR el cierre
    (para alargarlo se cambia el horario normal). `regla` es un dict con
    hasta (str|None), sin_vispera (bool) y motivo.
    """
    regla = regla or {}
    n = hhmm_a_min(normal)
    if n is None:
        return normal, "normal", ""
    h = hhmm_a_min(regla.get("hasta"))
    if h is not None and h < n:
        return min_a_hhmm(h), "temprano", regla.get("motivo") or ""
    if vispera and vispera_activa and not regla.get("sin_vispera"):
        v = hhmm_a_min(vispera_hasta)
        if v is not None and v < n:
            return min_a_hhmm(v), "vispera", "Víspera de feriado"
    return min_a_hhmm(n), "normal", ""


def es_irrenunciable(dia):
    """True si `dia` (date) es feriado IRRENUNCIABLE: 1 ene, 1 may, 18 y 19 sep, 25 dic."""
    return (dia.month, dia.day) in IRRENUNCIABLES_MES_DIA

# -*- coding: utf-8 -*-
"""Feriados legales de Chile + ajuste de fechas de mantención.

Módulo PURO (sin BD, sin Flask, sin red) → unit-testeable con `python -c`.
Fuente única de verdad de feriados para TODA la app:
  - Mantenciones: proyección de fechas (día preferido + mover a día hábil),
    calendario (pintar feriados), planificador anual.
  - Retiros: pickups_module._chile_holidays delega aquí (con fallback propio).

Actualizar FERIADOS_OFICIALES cada año (fuente: feriados.cl). Para años no
listados se usa el fallback calculado: feriados de fecha fija + Viernes/Sábado
Santo por Computus (algoritmo de Meeus/Butcher).

(Daniel 2026-06-05: "identifica los feriados chilenos siempre para que no los
pongas como laborables"; 2026-06-10: "hay días que pueden ser fin de semana o
no laborables" → las mantenciones proyectadas se mueven a día hábil.)
"""
from datetime import date, timedelta
from functools import lru_cache

# Feriados OFICIALES conocidos: fecha → nombre (para tooltips en calendario).
FERIADOS_OFICIALES = {
    2026: {
        "2026-01-01": "Año Nuevo",
        "2026-04-03": "Viernes Santo",
        "2026-04-04": "Sábado Santo",
        "2026-05-01": "Día del Trabajo",
        "2026-05-21": "Día de las Glorias Navales",
        "2026-06-21": "Día de los Pueblos Indígenas",
        "2026-06-29": "San Pedro y San Pablo",
        "2026-07-16": "Virgen del Carmen",
        "2026-08-15": "Asunción de la Virgen",
        "2026-09-18": "Fiestas Patrias",
        "2026-09-19": "Día de las Glorias del Ejército",
        "2026-10-12": "Encuentro de Dos Mundos",
        "2026-10-31": "Día de las Iglesias Evangélicas",
        "2026-11-01": "Día de Todos los Santos",
        "2026-12-08": "Inmaculada Concepción",
        "2026-12-25": "Navidad",
    },
}

# Feriados de fecha FIJA (fallback para años sin lista oficial).
_FIJOS = [
    ((1, 1),  "Año Nuevo"),
    ((5, 1),  "Día del Trabajo"),
    ((5, 21), "Día de las Glorias Navales"),
    ((6, 29), "San Pedro y San Pablo"),
    ((7, 16), "Virgen del Carmen"),
    ((8, 15), "Asunción de la Virgen"),
    ((9, 18), "Fiestas Patrias"),
    ((9, 19), "Día de las Glorias del Ejército"),
    ((10, 12), "Encuentro de Dos Mundos"),
    ((10, 31), "Día de las Iglesias Evangélicas"),
    ((11, 1),  "Día de Todos los Santos"),
    ((12, 8),  "Inmaculada Concepción"),
    ((12, 25), "Navidad"),
]


def _pascua(year):
    """Domingo de Pascua por Computus (Meeus/Butcher). Mismo algoritmo que
    usaba pickups_module._chile_holidays."""
    a = year % 19; b = year // 100; c = year % 100
    d_ = b // 4; e = b % 4; f = (b + 8) // 25
    g = (b - f + 1) // 3; h = (19 * a + b - d_ - g + 15) % 30
    i = c // 4; k = c % 4; l = (32 + 2 * e + 2 * i - h - k) % 7
    mm = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * mm + 114) // 31
    day = ((h + l - 7 * mm + 114) % 31) + 1
    return date(year, month, day)


@lru_cache(maxsize=16)
def feriados_chile(year):
    """dict {'YYYY-MM-DD': 'Nombre'} de feriados de Chile para `year`.
    Oficial si está en FERIADOS_OFICIALES; si no, fallback fijos + Semana Santa."""
    year = int(year)
    if year in FERIADOS_OFICIALES:
        return dict(FERIADOS_OFICIALES[year])
    out = {f"{year:04d}-{m:02d}-{d:02d}": nombre for (m, d), nombre in _FIJOS}
    try:
        p = _pascua(year)
        out[(p - timedelta(days=2)).isoformat()] = "Viernes Santo"
        out[(p - timedelta(days=1)).isoformat()] = "Sábado Santo"
    except Exception:
        pass
    return out


def feriados_set(year):
    """set {'YYYY-MM-DD'} — firma compatible con la _chile_holidays de retiros."""
    return set(feriados_chile(year).keys())


def es_dia_habil(d):
    """True si `d` (date) es lunes-viernes y NO feriado."""
    return d.isoweekday() <= 5 and d.isoformat() not in feriados_set(d.year)


def siguiente_dia_habil(d):
    """El primer día hábil >= d (si d ya es hábil, devuelve d).
    Guard de 14 iteraciones (no existe racha mayor de no-hábiles en Chile)."""
    for _ in range(14):
        if es_dia_habil(d):
            return d
        d = d + timedelta(days=1)
    return d


def ajustar_fecha_mantencion(fecha, dia_pref, mover_habil):
    """Ajusta una fecha PROYECTADA de mantención (función pura).

    1. Si `dia_pref` está en 1..28 → la fecha se mueve al día `dia_pref` del
       MISMO mes (28 máx → jamás ValueError, ni en febrero).
    2. Si `mover_habil` → se desplaza al día hábil siguiente si cae en
       sábado/domingo/feriado.

    REGLA DE ORO (no romper): el cursor de la serie de proyección avanza SIN
    este ajuste; el ajuste se aplica solo a la fecha EMITIDA. Así no hay
    deriva acumulativa. El "mes" de una mantención = mes de la fecha AJUSTADA.
    """
    if not fecha:
        return fecha
    try:
        dp = int(dia_pref) if dia_pref else None
    except (TypeError, ValueError):
        dp = None
    if dp and 1 <= dp <= 28:
        fecha = fecha.replace(day=dp)
    if mover_habil:
        fecha = siguiente_dia_habil(fecha)
    return fecha

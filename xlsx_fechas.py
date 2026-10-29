# -*- coding: utf-8 -*-
"""📅 Fechas de los Excel de ILUS Fitness: fechas REALES, en hora Chile y fáciles de filtrar (2026-10-29).

Daniel (dueño): «Cuando bajas las fechas en el reporte de la OT, la fecha viene con formato gringo y necesito que en el
Excel, en español, pueda filtrar por meses; que me salgan los meses, los días, y tener un filtro más amigable en Excel,
en el reporte más que nada.»

Qué resuelve (una sola lógica para todos los Excel de OT y costos; nadie arma la suya):
  · Una fecha se escribe como FECHA de Excel (date / datetime de Python), nunca como texto: así el filtro automático la
    agrupa por año → mes → día, se ordena bien y sirve para tablas dinámicas. Excel la muestra según la configuración
    regional de quien abre el archivo, pero siempre como fecha.
  · En hora Chile (REGLA #6): MySQL guarda con NOW() en UTC. Un datetime SIN zona se toma como UTC y se pasa a
    America/Santiago (con el cambio de horario de verano/invierno), igual que `to_chile_filter` / `chile_fmt_filter`
    de app.py. Un `date` puro (DATE de MySQL: fecha programada, fecha realizada) no se convierte: no tiene hora.
  · Formato español: dd/mm/yyyy para fechas y dd/mm/yyyy hh:mm para fecha y hora.
  · Columnas de apoyo para filtrar y armar tablas dinámicas sin fórmulas: Año, Mes (Enero…Diciembre), N° mes,
    Semana (ISO), Día (Lunes…Domingo) y Año-Mes («2026-10», texto ordenable).
  · Nunca rompe un Excel: vacío / None → celda vacía; un texto que no se puede leer como fecha queda con el texto
    ORIGINAL (no se pierde el dato) y la exportación sigue.

Textos: «dd/mm/aaaa» y «dd/mm/aaaa hh:mm» (como los deja `chile_fmt_filter`) se leen tal cual, ya están en hora
Chile. Un texto ISO con zona propia («…T14:00:00-05:00», «…Z»: FedEx, SimpliRoute) se pasa a hora Chile; un ISO sin
zona se toma tal cual. Nunca se adivina un formato gringo (mm/dd): en Chile el día va primero.

MÓDULO PURO: no importa Flask ni toca la base; no necesita openpyxl para importarse (las funciones que escriben
reciben la celda o la hoja). Pruebas: tests/test_xlsx_fechas.py.
"""
from __future__ import annotations

import re
from datetime import date, datetime, timedelta, timezone

try:
    from zoneinfo import ZoneInfo
    TZ_CL = ZoneInfo("America/Santiago")
except Exception:                                    # pragma: no cover - sin tzdata: mismo respaldo que to_chile_filter
    TZ_CL = timezone(timedelta(hours=-4))

MESES_ES = ("Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio", "Agosto", "Septiembre", "Octubre",
            "Noviembre", "Diciembre")
DIAS_ES = ("Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo")

FORMATO_FECHA = "dd/mm/yyyy"
FORMATO_FECHA_HORA = "dd/mm/yyyy hh:mm"

# Columnas de apoyo (en este orden) y su ancho sugerido.
APOYO = ("Año", "Mes", "N° mes", "Semana", "Día", "Año-Mes")
ANCHOS_APOYO = (11, 13, 11, 11, 12, 12)

_RE_DMA = re.compile(r"^(\d{1,2})[/-](\d{1,2})[/-](\d{4})(?:[ T,]+(\d{1,2}):(\d{2})(?::(\d{2})(?:\.\d+)?)?)?$")
_RE_ISO = re.compile(r"^\d{4}-\d{2}-\d{2}(?:$|[ T])")


def a_chile(dt):
    """datetime → datetime SIN zona en hora Chile. Sin zona se toma como UTC (así guarda MySQL con NOW()); con zona, se
    convierte. Excel no admite zonas: por eso se devuelve sin zona."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(TZ_CL).replace(tzinfo=None)


def _en_rango(v):
    """Excel no representa fechas antes de 1900."""
    return 1900 <= v.year <= 9999


def _desde_texto(s):
    """Un texto → date / datetime (o el mismo texto si no es una fecha que se pueda leer sin adivinar)."""
    t = s.strip()
    if not t:
        return None
    m = _RE_DMA.match(t)
    if m:
        d, mes, a, hh, mi, ss = m.groups()
        try:
            if hh is None:
                return date(int(a), int(mes), int(d))
            return datetime(int(a), int(mes), int(d), int(hh), int(mi), int(ss or 0))
        except ValueError:                           # 31/02/2026, 25:61…: no es una fecha
            return s
    if _RE_ISO.match(t):
        try:
            if len(t) == 10:
                return date.fromisoformat(t)
            v = datetime.fromisoformat(t[:-1] + "+00:00" if t.endswith(("Z", "z")) else t)
        except ValueError:
            return s
        return a_chile(v) if v.tzinfo is not None else v
    return s


def a_fecha_excel(valor, utc=True):
    """El valor listo para una celda de Excel: `date`, `datetime` SIN zona en hora Chile, `None` (celda vacía) o, si no
    es una fecha que se pueda leer, el valor original como texto (nunca se pierde el dato ni revienta la exportación).

    `utc=True` (lo normal): un datetime sin zona viene de MySQL en UTC → se pasa a hora Chile. `utc=False`: el datetime
    ya está en hora Chile (lo armó el código con una fecha y una hora locales) y se deja tal cual. Un datetime CON zona
    siempre se convierte; un `date` puro nunca."""
    if valor is None:
        return None
    try:
        if isinstance(valor, datetime):
            v = a_chile(valor) if (utc or valor.tzinfo is not None) else valor
            v = v.replace(microsecond=0)
        elif isinstance(valor, date):
            v = valor
        elif isinstance(valor, str):
            v = _desde_texto(valor)
            if v is None or isinstance(v, str):
                return v
        elif isinstance(valor, bool):
            return str(valor)
        elif isinstance(valor, (int, float)):
            return valor
        else:
            return str(valor)
        return v if _en_rango(v) else (valor if isinstance(valor, str) else str(valor))
    except Exception:                                # nunca tumba la exportación
        return valor if isinstance(valor, str) else str(valor)


def formato(valor):
    """El number_format español para un valor ya convertido; None si no es una fecha."""
    if isinstance(valor, datetime):
        return FORMATO_FECHA_HORA
    if isinstance(valor, date):
        return FORMATO_FECHA
    return None


def formatear(celda):
    """Le pone el formato español a una celda que YA tiene una fecha (por ejemplo, después de `ws.append`)."""
    f = formato(celda.value)
    if f:
        celda.number_format = f
    return celda


def escribir_fecha(celda, valor, utc=True):
    """Escribe `valor` en la celda como fecha real de Excel en hora Chile con formato español. Si no es una fecha queda
    el texto original (un texto que parte con = + - @ queda como TEXTO, nunca como fórmula). Devuelve lo escrito."""
    v = a_fecha_excel(valor, utc=utc)
    celda.value = v
    f = formato(v)
    if f:
        celda.number_format = f
    elif isinstance(v, str) and v[:1] in ("=", "+", "-", "@"):
        celda.data_type = "s"
    return v


def columnas_apoyo(fecha):
    """[Año, Mes, N° mes, Semana, Día, Año-Mes] de una fecha YA convertida (lo que devuelve `a_fecha_excel`).
    Si no es una fecha (vacía o texto) devuelve 6 vacíos: la fila no se cae ni inventa un mes.

    La semana es la ISO (lunes a domingo): el 01/01/2027 es la semana 53 de 2026 y el 29/12/2025 la semana 1."""
    if not isinstance(fecha, date):
        return [None] * len(APOYO)
    return [fecha.year, MESES_ES[fecha.month - 1], fecha.month, fecha.isocalendar()[1], DIAS_ES[fecha.weekday()],
            f"{fecha.year:04d}-{fecha.month:02d}"]


def encabezados_apoyo(de=None):
    """Los encabezados de las columnas de apoyo. `de` dice de qué fecha salen cuando la hoja tiene varias (con su «de» o
    «del»): encabezados_apoyo("de la fecha de cierre") → «Año (de la fecha de cierre)», «Mes (de la fecha de cierre)»…"""
    if not de:
        return list(APOYO)
    return [f"{h} ({de.strip()})" for h in APOYO]


def escribir_apoyo(ws, fila, col, fecha):
    """Escribe las 6 columnas de apoyo de `fecha` (ya convertida) desde la columna `col`. Devuelve las celdas."""
    celdas = []
    for i, v in enumerate(columnas_apoyo(fecha)):
        celdas.append(ws.cell(row=fila, column=col + i, value=v))
    return celdas

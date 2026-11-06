# -*- coding: utf-8 -*-
"""VIGENCIA DEL ENLACE DE FIRMA REMOTA DEL CLIENTE: 48 HORAS HÁBILES (Daniel, 5-nov-2026).

Daniel, con su manual MAN-SST-INS-01 en la mano (§4, plazos): «Firma remota del cliente: 48 horas hábiles, desde el envío del enlace» y,
al ofrecerle el cambio: «sí, cambia el enlace de firma a 48 horas hábiles». Antes el enlace duraba 120 horas CORRIDAS (5 días, fines de
semana incluidos) y los textos decían «vence en 5 días».

«Horas hábiles» se cuenta como ya lo hace Retiros con la anticipación de 48 horas (`pickups_module.inicio_minimo_habil`, Daniel 12-oct):
solo cuenta el tiempo de los DÍAS HÁBILES (lunes a viernes sin feriados de Chile); un fin de semana o un feriado no corre. Ejemplos:
  · enviado el jueves a las 15:00   → vence el lunes a las 15:00   (viernes completo + lunes hasta las 15:00; sábado y domingo no cuentan)
  · enviado el viernes a las 15:00  → vence el martes a las 15:00
  · enviado el sábado               → empieza a contar el lunes a las 00:00 y vence el martes a las 24:00 (miércoles 00:00)
Si un día en medio es feriado, tampoco cuenta.

Módulo PURO (sin base de datos, sin Flask): lo usa `app.py` para fijar la expiración del token (que es sin estado: HMAC con expiración) y
para decirle al cliente y al técnico, en texto claro, hasta cuándo sirve el enlace. Pruebas: tests/test_ot_firma_vigencia.py.
"""
import datetime as _dt

HORAS_VIGENCIA = 48
TEXTO_PLAZO = "48 horas hábiles"
_DIAS_ES = ("lunes", "martes", "miércoles", "jueves", "viernes", "sábado", "domingo")


def vence_el(desde, es_habil, horas=HORAS_VIGENCIA):
    """Momento (datetime ingenuo, hora de Chile) en que vence un enlace enviado en `desde`, contando SOLO el tiempo de los días hábiles
    (`es_habil(date)` → bool). Lo que cae en un día no hábil empieza a contar el siguiente día hábil a las 00:00. Nunca lanza por un
    calendario raro: tope de 120 días."""
    restante = _dt.timedelta(hours=max(0, horas))
    t = desde.replace(tzinfo=None)
    cache = {}
    for _ in range(120):
        d = t.date()
        if d not in cache:
            try:
                cache[d] = bool(es_habil(d))
            except Exception:
                cache[d] = d.isoweekday() <= 5            # sin calendario de feriados: al menos lunes a viernes
        fin_dia = _dt.datetime.combine(d + _dt.timedelta(days=1), _dt.time.min)
        if not cache[d]:
            t = fin_dia
            continue
        tramo = fin_dia - t
        if tramo >= restante:
            return t + restante
        restante -= tramo
        t = fin_dia
    return t


def texto_vence(venc):
    """«el lunes 09/11/2026 a las 15:00» (día/mes/año y hora de Chile, REGLA #6). Un vencimiento exactamente a las 00:00 se dice como el final
    del día anterior («a las 23:59»): así nadie entiende que le queda un día más."""
    v = venc.replace(tzinfo=None)
    if v.hour == 0 and v.minute == 0:
        v = v - _dt.timedelta(minutes=1)
    return f"el {_DIAS_ES[v.weekday()]} {v.strftime('%d/%m/%Y')} a las {v.strftime('%H:%M')}"

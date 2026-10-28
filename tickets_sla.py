"""Tickets 2.0 · SLA de respuesta (Daniel 2026-10-27).

Regla: cada mensaje del cliente se responde en 4 HORAS HÁBILES. El reloj parte
con el primer mensaje del cliente que sigue sin respuesta (o al crearse un
ticket desde el formulario público) y se detiene cuando el equipo le contesta.

Horas hábiles = las MISMAS de Retiros (retiros_monitor: lun-vie 08:00-17:00,
sin la colación 13:00-14:00 ni feriados de cl_feriados). No hay una segunda
definición del horario: si cambia la cobertura de Retiros, cambia aquí.

Escala: para filtrar y contar en la base NO se evalúa ticket por ticket. Se
calcula UNA vez el «corte» (el instante que está exactamente N horas hábiles
atrás) y la condición queda `espera_desde <= corte`, que usa un índice.

Módulo puro (sin base de datos ni Flask): todas las fechas son naive; las de
la base vienen en UTC y se convierten con utc_a_chile / chile_a_utc.
"""
import os
from datetime import datetime, timedelta

import retiros_monitor as rm

try:
    from zoneinfo import ZoneInfo
    _CL = ZoneInfo("America/Santiago")
    _UTC = ZoneInfo("UTC")
except Exception:  # pragma: no cover - sin tzdata
    _CL = _UTC = None


def _horas_env():
    try:
        return max(0.5, float(os.environ.get("TICKETS_SLA_HORAS") or 4))
    except ValueError:
        return 4.0


SLA_HORAS = _horas_env()          # plazo de respuesta, en horas hábiles
AVISO_ANTES_H = 1.0               # ámbar cuando queda 1 h hábil o menos
TERMINALES = ("resolved", "closed", "cancelado")

NIVELES = {
    # nivel: (semáforo, etiqueta corta)
    "vencido": ("rojo", "Vencido"),
    "atencion": ("ambar", "Por vencer"),
    "ok": ("verde", "En plazo"),
    "respondido": ("verde", "En orden"),
    "cerrado": ("gris", "Cerrado"),
}


def utc_a_chile(dt):
    if dt is None or _CL is None:
        return dt
    return dt.replace(tzinfo=_UTC).astimezone(_CL).replace(tzinfo=None)


def chile_a_utc(dt):
    if dt is None or _CL is None:
        return dt
    return dt.replace(tzinfo=_CL).astimezone(_UTC).replace(tzinfo=None)


_FERIADOS = {}


def feriados(*anios):
    """Feriados de Chile (dict ISO -> nombre) de los años pedidos, con memoria."""
    out = {}
    for a in anios:
        if a not in _FERIADOS:
            try:
                from cl_feriados import feriados_chile
                _FERIADOS[a] = dict(feriados_chile(a))
            except Exception:
                _FERIADOS[a] = {}
        out.update(_FERIADOS[a])
    return out


def _fer_para(desde, hasta):
    return feriados(*range(desde.year, hasta.year + 1))


def horas_habiles(desde, hasta, fer=None, cob=None):
    if not desde or not hasta or hasta <= desde:
        return 0.0
    return rm.horas_cobertura(desde, hasta, fer if fer is not None else _fer_para(desde, hasta), cob)


def corte(ahora_cl, horas, cob=None):
    """El instante (hora Chile) que queda exactamente `horas` hábiles antes de
    `ahora_cl`. Un ticket que espera desde ese instante o antes ya cumplió
    `horas`. Búsqueda binaria sobre la misma función de horas hábiles."""
    lo = ahora_cl - timedelta(days=31)
    fer = _fer_para(lo, ahora_cl)
    if horas_habiles(lo, ahora_cl, fer, cob) < horas:
        return lo
    hi = ahora_cl
    for _ in range(26):            # 31 días / 2^26 ≈ 0,04 s de precisión
        medio = lo + (hi - lo) / 2
        if horas_habiles(medio, ahora_cl, fer, cob) >= horas:
            lo = medio
        else:
            hi = medio
    return lo.replace(microsecond=0)


def evaluar(espera_desde_cl, ahora_cl, estado=None, horas=None, cob=None):
    """Estado del SLA de UN ticket. `espera_desde_cl`: desde cuándo espera
    respuesta (hora Chile) o None si no espera nada.

    Devuelve {nivel, semaforo, etiqueta, espera_h, resta_h, vence, pill, sub,
    pausa}. pill/sub son los mismos textos del reloj de Retiros."""
    horas = float(horas or SLA_HORAS)
    if (estado or "") in TERMINALES:
        sem, et = NIVELES["cerrado"]
        return {"nivel": "cerrado", "semaforo": sem, "etiqueta": et, "espera_h": 0.0,
                "resta_h": None, "vence": None, "pill": "Cerrado", "sub": "", "pausa": False}
    if not espera_desde_cl:
        sem, et = NIVELES["respondido"]
        return {"nivel": "respondido", "semaforo": sem, "etiqueta": et, "espera_h": 0.0,
                "resta_h": None, "vence": None, "pill": "En orden", "sub": "Sin mensajes por responder",
                "pausa": False}
    fer = _fer_para(espera_desde_cl, ahora_cl + timedelta(days=31))
    espera_h = horas_habiles(espera_desde_cl, ahora_cl, fer, cob)
    if espera_h >= horas:
        nivel = "vencido"
    elif horas - espera_h <= AVISO_ANTES_H:
        nivel = "atencion"
    else:
        nivel = "ok"
    vence = rm.plazo_sla(espera_desde_cl, horas, lambda a, b, f: horas_habiles(a, b, f, cob), fer)
    hoy = ahora_cl.date()
    prox = rm.proxima_apertura(ahora_cl, fer, cob)
    txt = rm.textos_reloj(espera_h * 3600, horas * 3600, prox is None,
                          rm.fmt_cuando(espera_desde_cl, hoy),
                          rm.fmt_cuando(prox, hoy) if prox else "",
                          rm.fmt_plazo(vence, hoy))
    sem, et = NIVELES[nivel]
    return {"nivel": nivel, "semaforo": sem, "etiqueta": et, "espera_h": round(espera_h, 2),
            "resta_h": round(max(horas - espera_h, 0.0), 2), "vence": vence,
            "pill": txt["pill"], "sub": txt["sub"] + (f" · en pausa {txt['tag']}" if txt.get("tag") else ""),
            "pausa": prox is not None}


def cortes_utc(ahora_utc, horas=None, cob=None):
    """Los dos cortes que necesita la base, en UTC: (vencido, atencion).
    vencido:  espera_desde <= c_venc                 -> rojo
    atención: c_venc < espera_desde <= c_aten        -> ámbar
    en plazo: espera_desde > c_aten                  -> verde"""
    horas = float(horas or SLA_HORAS)
    ahora_cl = utc_a_chile(ahora_utc)
    c_venc = corte(ahora_cl, horas, cob)
    c_aten = corte(ahora_cl, max(horas - AVISO_ANTES_H, 0.01), cob)
    return chile_a_utc(c_venc), chile_a_utc(c_aten)

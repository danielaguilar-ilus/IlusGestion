# -*- coding: utf-8 -*-
"""RECORDATORIO DE 24 HORAS DE LA ORDEN DE TRABAJO — cliente y técnico (Daniel, 5-nov-2026).

Daniel: «necesito que le recordemos también 24 horas tanto al técnico como al cliente… eso lo podemos establecer por el sistema de
comunicaciones, déjalo todo por el front». Y su manual MAN-SST-INS-01: todos los plazos son en HORAS HÁBILES (lunes a viernes de 08:00 a
17:00, con colación de 13:00 a 14:00).

Este módulo es PURO (sin base de datos, sin Flask, sin correo): decide CUÁNDO toca recordar una OT y arma las variables del texto. El
barrido que lo usa, las tablas y el envío viven en app.py (`_ot_recordatorio_barrido`), colgados del trabajo `simpliroute-poll` (cada 10 min).

REGLAS DE «CUÁNDO» (todas verificadas por pruebas en tests/test_ot_recordatorio_24h.py):
  · El recordatorio sale 24 horas antes del comienzo de la visita (sin hora definida, el comienzo se toma a las 08:00).
  · SOLO en horario de cobertura (día hábil, 08:00–13:00 y 14:00–17:00): nunca de madrugada, en la colación, de noche, fin de semana o feriado.
  · Si las 24 h caen fuera de cobertura (p. ej. visita el lunes a las 09:00 → el domingo a las 09:00), sale en el último tramo de
    cobertura anterior (el viernes por la tarde): es mejor avisar antes que no avisar.
  · Si la OT se creó DESPUÉS de ese momento (la visita es en menos de 24 h desde su creación), no se manda recordatorio: el aviso de
    creación ya le llegó hace instantes.
  · No se recuerda una visita que empieza en menos de una hora (llegaría tarde).
  · Un reagendamiento cambia la «clave de inicio» y, por tanto, vuelve a recordar la nueva fecha (nunca dos veces la misma).
CONFIDENCIALIDAD (REGLAS #19 y #26): ni el texto del cliente ni el del técnico llevan montos, costos, proveedores ni OC.
"""
import datetime as _dt

import ot_comunicacion as _ocom
import ot_correos_tecnicos as _otc

HORAS_ANTES = 24
MINUTOS_MINIMOS = 60                      # no se recuerda una visita que empieza en menos de esto
HORA_SIN_HORARIO = "08:00"                # visita sin hora definida: se toma el comienzo de la jornada
# Cobertura (manual MAN-SST-INS-01, §4): lunes a viernes 08:00–17:00 con colación 13:00–14:00. En minutos del día: [inicio, fin).
COBERTURA = ((8 * 60, 13 * 60), (14 * 60, 17 * 60))
MAX_DIAS_ATRAS = 12                       # cuántos días hacia atrás se busca un tramo de cobertura (fin de semana largo + feriados)

CORREO_CLIENTE = _ocom.CORREO_RECORDATORIO_CLIENTE
CORREO_TECNICO = _ocom.CORREO_RECORDATORIO_TECNICO


def _min(dt):
    return dt.hour * 60 + dt.minute


def inicio_ot(fecha, hora_inicio):
    """datetime (hora de Chile, ingenuo) en que empieza la visita, o None si la OT no tiene fecha (la fecha centinela de «todavía sin
    fecha», año 2099, cuenta como sin fecha)."""
    d = _otc.a_fecha(fecha)
    if not d:
        return None
    hm = _otc.hhmm(hora_inicio) or HORA_SIN_HORARIO
    return _dt.datetime(d.year, d.month, d.day, int(hm[:2]), int(hm[3:5]))


def clave_inicio(inicio):
    """Identifica «ESTA fecha y hora de visita» (idempotencia: no se recuerda dos veces lo mismo; si se reagenda, cambia)."""
    return inicio.strftime("%Y-%m-%d %H:%M") if inicio else ""


def en_cobertura(dt, es_habil):
    """¿`dt` cae dentro del horario de cobertura (día hábil, 08:00–13:00 o 14:00–17:00)?"""
    if not es_habil(dt.date()):
        return False
    m = _min(dt)
    return any(a <= m < b for a, b in COBERTURA)


def ultimo_instante_cobertura(dt, es_habil):
    """El último minuto de cobertura menor o igual a `dt` (ingenuo, Chile), o None si no hay en los últimos MAX_DIAS_ATRAS días."""
    for k in range(MAX_DIAS_ATRAS + 1):
        d = dt.date() - _dt.timedelta(days=k)
        if not es_habil(d):
            continue
        for a, b in reversed(COBERTURA):
            ini = _dt.datetime(d.year, d.month, d.day, a // 60, a % 60)
            fin = _dt.datetime(d.year, d.month, d.day, b // 60, b % 60) - _dt.timedelta(minutes=1)
            if ini > dt:
                continue
            return min(dt, fin)
    return None


def momento_envio(inicio, es_habil):
    """Desde cuándo se puede mandar el recordatorio de una visita que empieza en `inicio` (None si no se puede calcular).
    Normalmente es exactamente 24 h antes; si eso cae fuera de cobertura, es una hora antes del cierre del último tramo de cobertura previo
    (así el barrido, que corre cada 10 minutos, tiene tiempo de enviarlo dentro de ese mismo tramo)."""
    if not inicio:
        return None
    objetivo = inicio - _dt.timedelta(hours=HORAS_ANTES)
    if en_cobertura(objetivo, es_habil):
        return objetivo
    ult = ultimo_instante_cobertura(objetivo, es_habil)
    if ult is None:
        return None
    return ult - _dt.timedelta(minutes=60)


def toca_recordar(inicio, creada_chile, ahora, es_habil, ignorar_cobertura=False):
    """(sí, motivo). `ahora` y `creada_chile` son datetimes ingenuos en hora de Chile (creada puede ser None). `ignorar_cobertura` solo lo
    usa el modo «solo mirar» (dry-run) para poder revisar de noche qué se mandaría; el envío real nunca lo activa."""
    if not inicio:
        return False, "la OT no tiene fecha"
    if ahora >= inicio - _dt.timedelta(minutes=MINUTOS_MINIMOS):
        return False, "la visita empieza en menos de una hora o ya empezó"
    if not ignorar_cobertura and not en_cobertura(ahora, es_habil):
        return False, "fuera del horario de cobertura"
    envio = momento_envio(inicio, es_habil)
    if envio is None:
        return False, "sin tramo de cobertura previo"
    if ahora < envio:
        return False, "todavía no son 24 horas antes"
    if creada_chile is not None and creada_chile > envio:
        return False, "la OT se creó dentro de las 24 horas previas (ya recibió el aviso de creación)"
    return True, ""


def dia_visita_txt(inicio, hoy):
    """«hoy», «mañana» o «el martes 10/11/2026» (para el asunto y el texto)."""
    d = inicio.date()
    if d == hoy:
        return "hoy"
    if d == hoy + _dt.timedelta(days=1):
        return "mañana"
    return "el " + _otc.dia_txt(d)


def variables_recordatorio(ot, inicio, hoy, *, nombre_destinatario="", link_ot=""):
    """Variables de los dos correos (escapadas para HTML). `ot`: id, numero_ot, cliente, tipo_label, fecha, hora_inicio, hora_fin, direccion,
    equipos (lista de nombres), tecnicos (lista de personas). El horario, si no se definió, dice «por confirmar»."""
    base = _ocom.variables_ot(ot, nombre_destinatario=nombre_destinatario, link_ot=link_ot,
                              tecnicos_nombres=ot.get("tecnicos") or [])
    base["fecha"] = _otc.esc(_otc.fecha_txt(inicio.date()) or "—")
    base["dia"] = _otc.esc(dia_visita_txt(inicio, hoy))
    base["horario"] = _otc.esc(_otc.horario_txt(ot.get("hora_inicio"), ot.get("hora_fin")))
    return base

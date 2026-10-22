# -*- coding: utf-8 -*-
"""Solicitud de repuesto: DUEÑO, RELOJ DE PRIMERA RESPUESTA y textos de los avisos (módulo PURO: sin base de datos, sin Flask, sin correo).

Daniel (21-oct-2026): «que la solicitud de repuesto por las órdenes de trabajo sea práctica, ordenada, guiada, que tenga trayectoria,
que alerte a los técnicos y también a los administradores, y que los obligues a tomar una respuesta midiendo los tiempos de respuesta,
algo parecido a Retiros».

Aquí vive la ÚNICA lógica de:
  · quién es el DUEÑO de un pedido al nacer (`elegir_dueno`);
  · el reloj de la PRIMERA RESPUESTA en horas hábiles (`estado_reloj`, `horas_habiles`, `en_cobertura`): la «primera respuesta» es la primera
    vez que alguien saca el pedido de «solicitado» (validado, pedido o rechazado) y el tiempo de respuesta es cuánto tardó, medido solo en
    horas hábiles (lunes a viernes, sin feriados de Chile: `cl_feriados`);
  · cuándo avisar y a quién escalar (`nivel_pendiente`): por vencer → el dueño; vence → también supervisión; una sola vez por pedido y nivel;
  · los textos de la campana al técnico (`texto_tecnico`, sin proveedor ni costo ni N° de OC: REGLAS #19 y #26), al dueño y a la gestión;
  · el resumen de tiempos de respuesta (`resumen_tiempos`).

Todas las fechas que entran y salen son `datetime` SIN zona, en HORA DE CHILE (la conversión desde UTC la hace app.py). app.py usa estas
funciones desde `_otrep_*` (ver el bloque «DUEÑO, RELOJ DE RESPUESTA Y AVISOS»). Pruebas: tests/test_repuestos_respuesta.py.

Jornada: igual que la cola de gestión de Repuestos de siempre (lunes a viernes 08:00–17:00 = 9 h por día hábil, plazo «gestionar» = 1 día
hábil = 9 h; con el equipo fuera de servicio 4 h). Los límites se leen de las mismas variables de la cobertura de Retiros
(RETIROS_COBERTURA_DESDE / _HASTA / _COLACION_DESDE / _COLACION_HASTA) para que haya UNA sola definición de «horario de cobertura».
Los umbrales de aviso se pueden cambiar con REPUESTOS_AVISO_H (por defecto 4) y REPUESTOS_ESCALA_H (por defecto 8).
"""
import os
import re
import statistics
from datetime import datetime, date, time as _time, timedelta

VERSION = 1

# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  Constantes (los mismos plazos de la cola de gestión: `_OTREP_PLAZO_H` de app.py; una prueba los compara)
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
PLAZO_PRIMERA_H = 9.0        # 1 día hábil para dar la primera respuesta
PLAZO_PRIMERA_FS_H = 4.0     # con el equipo fuera de servicio: 4 horas hábiles
AVISO_H = 4.0                # a las 4 h hábiles sin respuesta: aviso al dueño («por vencer»)
ESCALA_H = 8.0               # a las 8 h hábiles sin respuesta: se avisa también a supervisión («escala»)
HORAS_POR_DIA = 9.0          # 08:00–17:00

NIVEL_AVISO = 1
NIVEL_ESCALA = 2

ESTADOS_RELOJ = ("en_plazo", "por_vencer", "vencida", "respondida")

# Estados desde los que un pedido «ya tuvo respuesta» (todo lo que no es «solicitado»).
ESTADO_INICIAL = "solicitado"

# Origen del dueño → cómo se dice en la bitácora y en las pantallas.
ORIGEN_TXT = {
    "ticket": "ya lo lleva el ticket de repuestos de la OT",
    "responsable": "responsable de la OT",
    "creador": "quien creó la OT",
    "ejecutivo": "ejecutivo del cliente",
    "defecto": "responsable por defecto de Repuestos",
    "respaldo": "respaldo de la gestión",
}

# Orígenes que son un dueño de verdad (el «respaldo» es solo una lista de último recurso: superadmin/admin/supervisor por nombre).
_ORIGENES_REALES = ("responsable", "creador", "ejecutivo")


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  Configuración y calendario
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
def _num(v, defecto):
    try:
        x = float(str(v).strip().replace(",", "."))
        return x if x > 0 else defecto
    except (TypeError, ValueError):
        return defecto


def _hm(texto, defecto_min):
    """«HH:MM» → minutos del día; si no se entiende, el valor por defecto (igual que `_cob_min` de Retiros)."""
    m = re.match(r"^\s*(\d{1,2}):(\d{2})\s*$", str(texto or ""))
    if not m:
        return defecto_min
    return max(0, min(int(m.group(1)) * 60 + int(m.group(2)), 24 * 60))


def config(environ=None):
    """Umbrales vigentes: {plazo_h, plazo_fs_h, aviso_h, escala_h}. `escala` siempre mayor que `aviso`."""
    env = os.environ if environ is None else environ
    aviso = _num(env.get("REPUESTOS_AVISO_H"), AVISO_H)
    escala = _num(env.get("REPUESTOS_ESCALA_H"), ESCALA_H)
    if escala <= aviso:
        escala = aviso * 2
    return {"plazo_h": PLAZO_PRIMERA_H, "plazo_fs_h": PLAZO_PRIMERA_FS_H, "aviso_h": aviso, "escala_h": escala}


def jornada(environ=None):
    """(desde, hasta, colación_desde, colación_hasta) en minutos del día. Mismas variables que la cobertura de Retiros."""
    env = os.environ if environ is None else environ
    desde = _hm(env.get("RETIROS_COBERTURA_DESDE"), 8 * 60)
    hasta = _hm(env.get("RETIROS_COBERTURA_HASTA"), 17 * 60)
    if hasta <= desde:
        desde, hasta = 8 * 60, 17 * 60
    return desde, hasta, _hm(env.get("RETIROS_COBERTURA_COLACION_DESDE"), 13 * 60), _hm(env.get("RETIROS_COBERTURA_COLACION_HASTA"), 14 * 60)


def _es_feriado(d, feriados):
    if feriados is None:
        try:
            import cl_feriados
            return d.isoformat() in cl_feriados.feriados_set(d.year)
        except Exception:
            return False
    if callable(feriados):
        try:
            return d.isoformat() in feriados(d.year)
        except Exception:
            return False
    return d.isoformat() in feriados


def es_dia_habil(d, feriados=None):
    """Lunes a viernes que no es feriado de Chile. `feriados`: None (usa cl_feriados), un set de «AAAA-MM-DD» o una función año → set."""
    if isinstance(d, datetime):
        d = d.date()
    return d.weekday() < 5 and not _es_feriado(d, feriados)


def horas_habiles(desde, hasta, feriados=None, jor=None):
    """Horas hábiles entre dos datetimes (hora Chile): solo cuenta lunes a viernes sin feriados, dentro de la jornada. Nunca negativo."""
    if not desde or not hasta or hasta <= desde:
        return 0.0
    j0, j1 = (jor or jornada())[:2]
    total, dia = 0.0, desde.date()
    for _ in range(2000):
        if dia > hasta.date():
            break
        if es_dia_habil(dia, feriados):
            base = datetime.combine(dia, _time(0, 0))
            a, b = max(base + timedelta(minutes=j0), desde), min(base + timedelta(minutes=j1), hasta)
            if b > a:
                total += (b - a).total_seconds() / 3600.0
        dia += timedelta(days=1)
    return total


def sumar_horas_habiles(desde, horas, feriados=None, jor=None):
    """El datetime (hora Chile) en que se cumplen `horas` hábiles contadas desde `desde`."""
    j0, j1 = (jor or jornada())[:2]
    actual, quedan = desde, float(horas)
    for _ in range(2000):
        d = actual.date()
        if es_dia_habil(d, feriados):
            base = datetime.combine(d, _time(0, 0))
            ini, fin = base + timedelta(minutes=j0), base + timedelta(minutes=j1)
            a = max(ini, actual)
            if a < fin:
                disp = (fin - a).total_seconds() / 3600.0
                if disp >= quedan:
                    return a + timedelta(hours=quedan)
                quedan -= disp
        actual = datetime.combine(d + timedelta(days=1), _time(0, 0)) + timedelta(minutes=j0)
    return actual


def en_cobertura(ahora, feriados=None, environ=None):
    """¿Hay cobertura ahora? {abierta, motivo}. Lunes a viernes hábiles dentro de la jornada y fuera de la colación (igual que Retiros).
    El barrido de avisos solo actúa con la cobertura abierta: ningún aviso de madrugada, en colación, de tarde ni en fin de semana o feriado."""
    d0, d1, c0, c1 = jornada(environ)
    if not es_dia_habil(ahora.date(), feriados):
        return {"abierta": False, "motivo": "día no hábil"}
    m = ahora.hour * 60 + ahora.minute
    if m < d0:
        return {"abierta": False, "motivo": "todavía no empieza la jornada"}
    if c0 <= m < c1:
        return {"abierta": False, "motivo": "colación"}
    if m >= d1:
        return {"abierta": False, "motivo": "terminó la jornada"}
    return {"abierta": True, "motivo": ""}


def fmt_horas(h):
    """«35 min», «3,5 h hábiles», «1,2 días hábiles»."""
    h = abs(float(h or 0))
    if h < 1:
        return "%d min" % max(1, int(round(h * 60)))
    if h < HORAS_POR_DIA:
        return ("%.1f" % h).rstrip("0").rstrip(".").replace(".", ",") + " h hábiles"
    dias = h / HORAS_POR_DIA
    txt = ("%.1f" % dias).rstrip("0").rstrip(".").replace(".", ",")
    return txt + (" día hábil" if round(dias, 1) == 1 else " días hábiles")


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  Primera respuesta y reloj
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
def _g(obj, clave, defecto=None):
    return obj.get(clave, defecto) if isinstance(obj, dict) else getattr(obj, clave, defecto)


def inicio_reloj(creada, eventos=()):
    """Desde cuándo corre el reloj: la creación del pedido o, si alguien lo REABRIÓ (volvió a «solicitado»), la última reapertura."""
    inicio = creada
    for e in eventos or ():
        if _g(e, "accion") == "estado" and _g(e, "a_estado") == ESTADO_INICIAL and _g(e, "de_estado") not in (None, ESTADO_INICIAL):
            cuando = _g(e, "created_at")
            if cuando and (inicio is None or cuando > inicio):
                inicio = cuando
    return inicio


def primera_respuesta(eventos, desde=None, fila=None):
    """La primera vez que el pedido salió de «solicitado»: {cuando, quien, a_estado, fuente} o None.

    `eventos`: la bitácora (`mant_ot_repuesto_eventos`: accion, de_estado, a_estado, usuario, created_at), en cualquier orden. Solo cuenta un
    cambio de estado que SALE de «solicitado» (validado, pedido, rechazado…). `desde`: ignora lo anterior a esa fecha (para medir solo el ciclo
    actual de un pedido reabierto). Si no hay bitácora (pedidos anteriores al 04-oct-2026) y `fila` trae el estado y las columnas de fechas, se
    deduce de ellas (`fuente`: «columna»)."""
    mejor = None
    for e in eventos or ():
        if _g(e, "accion") != "estado":
            continue
        de, a = _g(e, "de_estado"), _g(e, "a_estado")
        if de != ESTADO_INICIAL or a in (None, "", ESTADO_INICIAL):
            continue
        cuando = _g(e, "created_at")
        if not cuando or (desde and cuando < desde):
            continue
        if mejor is None or cuando < mejor["cuando"]:
            mejor = {"cuando": cuando, "quien": _g(e, "usuario") or "", "a_estado": a, "fuente": "evento"}
    if mejor or not fila:
        return mejor
    estado = _g(fila, "estado")
    if not estado or estado == ESTADO_INICIAL:
        return None
    cuando = _g(fila, "validado_at") or _g(fila, "pedido_at") or _g(fila, "updated_at")
    if not cuando or (desde and cuando < desde):
        return None
    quien = _g(fila, "validado_por") or _g(fila, "resuelto_por") or ""
    a_estado = "validado" if _g(fila, "validado_at") else ("pedido" if _g(fila, "pedido_at") else estado)
    return {"cuando": cuando, "quien": quien, "a_estado": a_estado, "fuente": "columna"}


def umbrales(fs, cfg=None):
    """(plazo, aviso, escala) en horas hábiles. Con el equipo fuera de servicio todo se acorta: aviso a la mitad del plazo y escala al plazo."""
    cfg = cfg or config()
    if fs:
        plazo = cfg["plazo_fs_h"]
        return plazo, min(cfg["aviso_h"], plazo * 0.5), min(cfg["escala_h"], plazo)
    return cfg["plazo_h"], cfg["aviso_h"], cfg["escala_h"]


def estado_reloj(inicio, ahora, respuesta=None, fs=False, feriados=None, cfg=None, jor=None):
    """El reloj de la primera respuesta de UN pedido.

    `respuesta`: el datetime de la primera respuesta (o None si sigue sin atender). Devuelve:
      estado     en_plazo · por_vencer (pasó el umbral de aviso) · vencida (pasó el plazo) · respondida
      nivel      0 · 1 (toca avisar al dueño) · 2 (toca escalar a supervisión); siempre 0 si ya hay respuesta
      transcurrido_h   horas hábiles desde el inicio hasta la respuesta (o hasta ahora si no hay)
      plazo_h / aviso_h / escala_h   los umbrales que se aplicaron;   vence   el datetime en que vence el plazo
      en_plazo   (solo respondida) True si respondió dentro del plazo;  tiempo_respuesta_h   (solo respondida) lo que tardó"""
    plazo, aviso, escala = umbrales(fs, cfg)
    vence = sumar_horas_habiles(inicio, plazo, feriados, jor) if inicio else None
    out = {"plazo_h": plazo, "aviso_h": aviso, "escala_h": escala, "vence": vence, "fs": bool(fs)}
    if respuesta:
        t = horas_habiles(inicio, respuesta, feriados, jor) if inicio else 0.0
        out.update({"estado": "respondida", "nivel": 0, "transcurrido_h": round(t, 2), "tiempo_respuesta_h": round(t, 2),
                    "en_plazo": t <= plazo + 1e-9})
        return out
    t = horas_habiles(inicio, ahora, feriados, jor) if inicio else 0.0
    nivel = NIVEL_ESCALA if t >= escala else (NIVEL_AVISO if t >= aviso else 0)
    estado = "vencida" if t >= plazo else ("por_vencer" if nivel >= 1 else "en_plazo")
    out.update({"estado": estado, "nivel": nivel, "transcurrido_h": round(t, 2)})
    return out


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  Avisos de plazo: qué toca y que NO se repita
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
_MARCA_RE = re.compile(r"^\s*\[n([12])\]")


def marca_detalle(nivel, texto=""):
    """El detalle del evento «plazo» que deja la marca de que ese nivel ya se avisó: «[n1] …»."""
    return ("[n%d] %s" % (int(nivel), texto or "")).strip()[:500]


def marcas_avisadas(eventos, desde=None):
    """Niveles que ya se avisaron (set de 1 y 2) en el ciclo actual (eventos «plazo» posteriores a `desde`)."""
    out = set()
    for e in eventos or ():
        if _g(e, "accion") != "plazo":
            continue
        cuando = _g(e, "created_at")
        if desde and cuando and cuando < desde:
            continue
        m = _MARCA_RE.match(str(_g(e, "detalle") or ""))
        if m:
            out.add(int(m.group(1)))
    return out


def nivel_pendiente(rel, marcas=()):
    """El nivel de aviso que toca ENVIAR ahora para un pedido (0 = ninguno). Una sola vez por pedido y nivel; si el pedido llega ya en nivel 2
    sin haber avisado nunca, se avisa el 2 (no tiene sentido decir «por vencer» de algo que ya está para escalar)."""
    if not rel or rel.get("estado") == "respondida":
        return 0
    marcas = set(marcas or ())
    n = int(rel.get("nivel") or 0)
    if n >= NIVEL_ESCALA and NIVEL_ESCALA not in marcas:
        return NIVEL_ESCALA
    if n == NIVEL_AVISO and NIVEL_AVISO not in marcas and NIVEL_ESCALA not in marcas:
        return NIVEL_AVISO
    return 0


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  El dueño de un pedido
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
def elegir_dueno(candidatos, defecto=None, ticket_asignado=None):
    """Quién es el DUEÑO del pedido al nacer. → {nombre, user_id, origen} o None.

    Orden: 1) el ticket de repuestos de la OT, si ya tiene a alguien (la solicitud y el ticket nunca quedan con dos dueños distintos);
    2) la gestión de la OT en el orden de `_ot_gestion_destinatarios` — responsable de la OT, quien la creó, ejecutivo del cliente (jamás un técnico);
    3) el «responsable por defecto» de Repuestos; 4) el respaldo de la gestión (superadmin/admin/supervisor) solo si no hay nadie más.
    `candidatos`: [{id, nombre, por}] ya sin técnicos. `defecto`: {id, nombre} o None. `ticket_asignado`: nombre o None."""
    if ticket_asignado and str(ticket_asignado).strip():
        return {"nombre": str(ticket_asignado).strip(), "user_id": None, "origen": "ticket"}
    cands = [c for c in (candidatos or []) if (c.get("nombre") or "").strip()]
    for c in cands:
        if c.get("por") in _ORIGENES_REALES:
            return {"nombre": c["nombre"].strip(), "user_id": c.get("id"), "origen": c["por"]}
    if defecto and (defecto.get("nombre") or "").strip():
        return {"nombre": defecto["nombre"].strip(), "user_id": defecto.get("id"), "origen": "defecto"}
    for c in cands:
        return {"nombre": c["nombre"].strip(), "user_id": c.get("id"), "origen": c.get("por") or "respaldo"}
    return None


def es_critico(dejo_fuera_servicio, diagnostico_txt=""):
    """¿Es un pedido grave? El equipo quedó fuera de servicio o el camino del diagnóstico dice que puede lastimar a alguien."""
    return bool(dejo_fuera_servicio) or "lastimar" in str(diagnostico_txt or "").lower()


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  Campana: cuándo se crea y cuándo se actualiza (como los hallazgos: una por OT y persona, 30 minutos)
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
VENTANA_MIN = 30


def decidir_campana(existente_leida, minutos_desde_la_ultima, ventana_min=VENTANA_MIN):
    """`existente_leida`: None (no hay campana de esa OT para esa persona), False (hay una sin leer) o True (hay una leída).
    Sin campana → crear. Sin leer → actualizar (una sola campana por OT; el conteo se pone al día). Leída y dentro de la ventana → actualizar
    (llegó algo nuevo); leída y fuera de la ventana → crear otra."""
    if existente_leida is None:
        return "crear"
    if not existente_leida:
        return "actualizar"
    return "actualizar" if (minutos_desde_la_ultima is not None and 0 <= minutos_desde_la_ultima < ventana_min) else "crear"


def _linea_pedido(it):
    cant = it.get("cantidad")
    try:
        cant_txt = "%g" % float(cant)
    except (TypeError, ValueError):
        cant_txt = str(cant or 1)
    return "%s × %s" % (it.get("repuesto_nombre") or "Repuesto", cant_txt)


linea_pedido = _linea_pedido      # «Piola × 2»: público para app.py


def campana_pedidos_nuevos(donde, items, cliente="", usuario="", fs=False, equipo="", documento=""):
    """(título, cuerpo, prioridad) de la campana «pedidos nuevos» de una OT para la gestión. Con uno solo se detalla (equipo, serie y documento);
    con varios se suman: «OT-2026-00171: 3 pedidos de repuesto nuevos»."""
    items = list(items or [])
    n = len(items) or 1
    if n == 1:
        titulo = "Nueva solicitud de repuesto" + (" · %s" % donde if donde else "")
    else:
        titulo = "%s: %d pedidos de repuesto nuevos" % (donde or "Repuestos", n)
    lineas = [_linea_pedido(it) for it in items[:6]] or ["Repuesto"]
    if len(items) > 6:
        lineas.append("y %d más" % (len(items) - 6))
    partes = [", ".join(lineas)]
    if n == 1 and equipo:
        partes.append(equipo)
    if cliente:
        partes.append(cliente)
    if n == 1 and documento:
        partes.append(documento)
    if usuario:
        partes.append("pidió %s" % usuario)
    cuerpo = " · ".join(partes) + (". Dejó el equipo FUERA DE SERVICIO." if fs else "")
    return titulo[:200], cuerpo, ("alta" if fs else "media")


def campana_plazo(nivel, items):
    """(título, cuerpo, prioridad) del aviso de plazo para UNA persona. `items`: los pedidos que le tocan en esta pasada
    [{id, repuesto_nombre, cantidad, ot, cliente, transcurrido_h, fs}]. Una sola campana por persona y nivel aunque lleguen varios a la vez."""
    items = list(items or [])
    n = len(items)
    grave = any(it.get("fs") for it in items)
    if n == 1:
        it = items[0]
        if nivel >= NIVEL_ESCALA:
            titulo = "Repuesto #%s sin respuesta: lleva %s" % (it.get("id"), fmt_horas(it.get("transcurrido_h")))
            cuerpo = ("%s · %s%s. Nadie ha tomado este pedido y el plazo es de 1 día hábil: se avisó a supervisión. Revísalo en Repuestos."
                      % (_linea_pedido(it), it.get("ot") or "sin OT", " · %s" % it["cliente"] if it.get("cliente") else ""))
        else:
            titulo = "Repuesto #%s por vencer: lleva %s sin respuesta" % (it.get("id"), fmt_horas(it.get("transcurrido_h")))
            cuerpo = ("%s · %s%s. Tienes que darle una primera respuesta (revisar si hay en bodega, pedirlo o cerrarlo)."
                      % (_linea_pedido(it), it.get("ot") or "sin OT", " · %s" % it["cliente"] if it.get("cliente") else ""))
    else:
        ids = ", ".join("#%s" % it.get("id") for it in items[:5]) + (" y %d más" % (n - 5) if n > 5 else "")
        if nivel >= NIVEL_ESCALA:
            titulo = "%d pedidos de repuesto sin respuesta: %s" % (n, ids)
        else:
            titulo = "%d pedidos de repuesto por vencer: %s" % (n, ids)
        cuerpo = "; ".join("%s (%s)" % (_linea_pedido(it), it.get("ot") or "sin OT") for it in items[:5]) \
            + (" y %d más" % (n - 5) if n > 5 else "") + ". Revisa Repuestos → Solicitudes."
    if nivel >= NIVEL_ESCALA:
        prio = "urgente" if grave else "alta"
    else:
        prio = "alta" if grave else "media"
    return titulo[:200], cuerpo, prio


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  Avisos al TÉCNICO que pidió (palabras simples; jamás proveedor, costo, N° de OC ni la nota de gestión)
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
def evento_tecnico(de, a):
    """Qué cambio de estado le interesa al técnico: revisado · pedido · recibido · instalado · rechazado, o None (un paso atrás, una reapertura…)."""
    if a == "validado" and de == "solicitado":
        return "revisado"
    if a == "pedido" and de in ("solicitado", "validado"):
        return "pedido"
    if a == "recibido" and de in ("solicitado", "validado", "pedido"):
        return "recibido"
    if a == "instalado" and de in ("solicitado", "validado", "pedido", "recibido"):
        return "instalado"
    if a == "rechazado" and de not in (None, "", "rechazado", "instalado"):
        return "rechazado"
    return None


def motivo_cierre(detalle):
    """El motivo de un cierre en palabras sencillas, a partir del PREFIJO que deja «Cerrar sin repuesto» ('Se reparó sin repuesto: …' o
    'No correspondía: …'). Solo se reconocen esas dos frases fijas: el resto del texto (la nota de gestión) NUNCA sale de aquí."""
    d = str(detalle or "").strip().lower()
    if d.startswith("se reparó sin repuesto"):
        return "Se reparó sin necesidad del repuesto."
    if d.startswith("no correspondía"):
        return "Bodega vio que este pedido no correspondía."
    return "Bodega no va a tramitar este pedido. Si todavía lo necesitas, avisa a tu responsable de la OT o vuélvelo a pedir con más detalle."


def fecha_corta(f):
    """dd/mm para un date/datetime o un texto ya formateado («dd/mm/aaaa» → «dd/mm»)."""
    if isinstance(f, (date, datetime)):
        return f.strftime("%d/%m")
    m = re.match(r"^\s*(\d{2})/(\d{2})(?:/\d{2,4})?\s*$", str(f or ""))
    return "%s/%s" % (m.group(1), m.group(2)) if m else ""


def texto_tecnico(clave, ctx):
    """(título, cuerpo, prioridad) de la campana del técnico, o None. `ctx`: repuesto_nombre, cantidad, camino ('bodega'|'ticket'|'ot'|None),
    hay_en_bodega (bool), eta (fecha o texto, ya sin proveedor), detalle (solo para decidir el motivo del cierre)."""
    pieza = _linea_pedido(ctx)
    eta = fecha_corta(ctx.get("eta"))
    if clave == "revisado":
        camino = ctx.get("camino")
        if camino == "bodega" or (not camino and ctx.get("hay_en_bodega")):
            return ("Bodega revisó tu pedido: hay en bodega",
                    "%s: hay en bodega y te lo apartamos. Te avisamos cuando esté listo." % pieza, "media")
        if camino == "ot":
            return ("Bodega revisó tu pedido: se resuelve con una OT",
                    "%s: se va a resolver con una orden de trabajo. Te avisamos de los pasos." % pieza, "media")
        return ("Bodega revisó tu pedido: lo vamos a pedir",
                "%s: no hay en bodega, lo vamos a pedir. Te avisamos cuando tengamos fecha de llegada." % pieza, "media")
    if clave == "pedido":
        return ("Tu pedido va en camino",
                "%s: ya se pidió." % pieza + (" Llega aprox. el %s." % eta if eta else " Te avisamos cuando haya fecha de llegada."), "media")
    if clave == "fecha":
        if not eta:
            return None
        return ("Ya hay fecha de llegada de tu pedido", "%s: llega aprox. el %s." % (pieza, eta), "media")
    if clave == "recibido":
        return ("Tu pieza ya está en bodega", "%s llegó a bodega y está lista." % pieza, "alta")
    if clave == "instalado":
        return ("Tu pedido quedó instalado", "%s ya está instalado." % pieza, "baja")
    if clave == "rechazado":
        return ("Se cerró tu pedido de repuesto", "%s. %s" % (pieza, motivo_cierre(ctx.get("detalle"))), "media")
    return None


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  Medición: tiempos de respuesta
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
def _redondea(x, n=1):
    return None if x is None else round(x, n)


def _agregar(rel_list):
    """Cifras de un grupo de pedidos con su reloj calculado."""
    resp = [r for r in rel_list if r["estado"] == "respondida"]
    sin = [r for r in rel_list if r["estado"] != "respondida"]
    venc = [r for r in sin if r["estado"] == "vencida"]
    ok = [r for r in resp if r.get("en_plazo")]
    tiempos = [r["tiempo_respuesta_h"] for r in resp]
    base = len(resp) + len(venc)       # los que aún están dentro del plazo no cuentan: todavía pueden cumplirlo
    return {
        "pedidos": len(rel_list), "respondidos": len(resp), "sin_responder": len(sin), "vencidos": len(venc),
        "respondidos_en_plazo": len(ok),
        "pct_en_plazo": _redondea(100.0 * len(ok) / base, 0) if base else None,
        "promedio_h": _redondea(sum(tiempos) / len(tiempos)) if tiempos else None,
        "mediana_h": _redondea(statistics.median(tiempos)) if tiempos else None,
    }


def resumen_tiempos(pedidos, ahora, feriados=None, cfg=None, jor=None, max_vencidos=50):
    """Cifras de los tiempos de PRIMERA respuesta. `pedidos`: [{id, dueno, inicio, respuesta, fs, ot, repuesto, cantidad}] (fechas en hora Chile).

    Devuelve {general: {...}, por_dueno: [{dueno, ...}], vencidos: [{id, ot, repuesto, dueno, horas}]}. «% en plazo» = respondidos dentro del plazo
    ÷ (respondidos + pedidos ya vencidos sin respuesta): lo que todavía está dentro del plazo no cuenta en contra. Solo horas hábiles; jamás montos."""
    calc = []
    for p in pedidos or []:
        rel = estado_reloj(p.get("inicio"), ahora, p.get("respuesta"), bool(p.get("fs")), feriados, cfg, jor)
        rel["dueno"] = (p.get("dueno") or "").strip() or "Sin dueño"
        rel["id"], rel["ot"], rel["repuesto"], rel["cantidad"] = p.get("id"), p.get("ot"), p.get("repuesto"), p.get("cantidad")
        calc.append(rel)
    grupos = {}
    for r in calc:
        grupos.setdefault(r["dueno"], []).append(r)
    por_dueno = []
    for nombre, lista in grupos.items():
        fila = _agregar(lista)
        fila["dueno"] = nombre
        por_dueno.append(fila)
    por_dueno.sort(key=lambda f: (-f["vencidos"], -f["pedidos"], f["dueno"]))
    vencidos = sorted((r for r in calc if r["estado"] == "vencida"), key=lambda r: -r["transcurrido_h"])[:max_vencidos]
    return {
        "general": _agregar(calc), "por_dueno": por_dueno,
        "vencidos": [{"id": r["id"], "ot": r["ot"], "repuesto": r["repuesto"], "cantidad": r["cantidad"], "dueno": r["dueno"],
                      "horas": _redondea(r["transcurrido_h"])} for r in vencidos],
    }

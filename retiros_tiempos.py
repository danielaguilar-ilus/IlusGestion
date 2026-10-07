# -*- coding: utf-8 -*-
"""Tiempos de preparación de un retiro según las OT de Check (WMS).

Daniel 2026-10-06: «que sumaran los tiempos de realización de cada OT… a veces la asigno en la tarde y el operario la termina mañana:
una separación inteligente» y «del inicio al fin puede haber una intermitencia: analizarla y calcular los minutos que se prepara el
retiro según el WMS, y tener toda la evidencia».

Funciones puras (sin BD ni red): reciben las OT ya resumidas por retiros_check_ot.resumir_ot (horas «dd/mm/aaaa hh:mm», la hora que
entrega Check) y devuelven los minutos. La misma lógica corre en static/retiros_guia.js para el panel en vivo; esta es la que queda
guardada como evidencia y la que usan los indicadores.

Criterio (queda escrito en el resultado, para que cualquiera pueda revisar el número):
  · Si un tramo pasa de un día a otro, la noche y el fin de semana NO se cuentan: solo la jornada de bodega (07:30–20:00, lunes a viernes).
    Dentro del mismo día se cuenta el reloj tal cual.
  · Trabajo = suma de lo que duró cada OT (si dos OT corren a la vez, cuentan las dos: es tiempo-persona).
  · Preparación efectiva = tiempo con AL MENOS una OT abierta (las OT que se pisan se juntan): lo que realmente tomó preparar.
  · Pausas (intermitencia) = principio a fin − preparación efectiva: tiempo en que nadie tenía una OT abierta.
"""
import os
import re
from datetime import datetime, timedelta

VERSION = 1


def _hm(txt, por_defecto):
    m = re.match(r"^\s*(\d{1,2}):(\d{2})\s*$", str(txt or ""))
    return int(m.group(1)) * 60 + int(m.group(2)) if m else por_defecto


def jornada():
    """(desde, hasta) en minutos del día. RETIROS_JORNADA_BODEGA='07:30-20:00' la cambia."""
    v = os.environ.get("RETIROS_JORNADA_BODEGA", "")
    if "-" in v:
        a, b = v.split("-", 1)
        d, h = _hm(a, 450), _hm(b, 1200)
        if d < h:
            return d, h
    return 450, 1200


def _fecha(v):
    if isinstance(v, datetime):
        return v.replace(tzinfo=None)
    m = re.match(r"^\s*(\d{2})/(\d{2})/(\d{4})(?:\s+(\d{2}):(\d{2}))?", str(v or ""))
    if not m:
        return None
    try:
        return datetime(int(m.group(3)), int(m.group(2)), int(m.group(1)), int(m.group(4) or 0), int(m.group(5) or 0))
    except ValueError:
        return None


def _fmt(d):
    return d.strftime("%d/%m/%Y %H:%M") if d else None


def _min_jornada(a, b, jor):
    tot, d = 0.0, datetime(a.year, a.month, a.day)
    for _ in range(400):
        if d >= b:
            break
        if d.weekday() < 5:
            j0, j1 = d + timedelta(minutes=jor[0]), d + timedelta(minutes=jor[1])
            x, y = max(a, j0), min(b, j1)
            if y > x:
                tot += (y - x).total_seconds() / 60
        d += timedelta(days=1)
    return tot


def dur_real(a, b, jor=None):
    """{'min': minutos que cuentan, 'reloj': minutos de reloj, 'pausa': True si se descontó noche / fin de semana (≥30 min)}."""
    if not a or not b or b <= a:
        return {"min": 0.0, "reloj": 0.0, "pausa": False}
    reloj = (b - a).total_seconds() / 60
    if a.date() == b.date():
        return {"min": reloj, "reloj": reloj, "pausa": False}
    m = _min_jornada(a, b, jor or jornada())
    return {"min": m, "reloj": reloj, "pausa": reloj - m >= 30}


def _final(o):
    return bool(re.search(r"termin|final|cerr|complet|ejecut|ok", str(o.get("estado") or "").lower()))


def _clase(o):
    t = str(o.get("tipo") or "").lower()
    if re.search(r"salida|despach|expedi|entrega", t):
        return "salida"
    if "pick" in t:
        return "picking"
    if re.search(r"revis|chequeo|control", t):
        return "revision"
    return "otra"


def _inicio(o):
    return _fecha(o.get("inicio")) or _fecha(((o.get("momentos") or [{}])[0] or {}).get("valor"))


def _fin(o):
    f = _fecha(o.get("fin"))
    if f:
        return f
    ms = o.get("momentos") or []
    return _fecha(ms[-1].get("valor")) if len(ms) > 1 and _final(o) else None


def _asignada(o):
    for m in o.get("momentos") or []:
        if "asign" in str(m.get("campo") or m.get("etiqueta") or "").lower():
            return _fecha(m.get("valor"))
    return None


def _uni(linea):
    for k in ("ejecutado", "solicitado"):
        try:
            v = float(linea.get(k))
            if v > 0:
                return v
        except (TypeError, ValueError):
            pass
    return 0.0


def _pares(ots, tramos):
    """(OT original, tramo) de las OT que traen hora, en el orden de los tramos."""
    por_id = {}
    for o in ots or []:
        if isinstance(o, dict):
            por_id.setdefault(id(o), o)
    return [(por_id[t["_id"]], t) for t in tramos if t.get("_id") in por_id]


def analizar(ots, cita=None, ahora=None, jor=None):
    """Análisis de tiempos de las OT de UN documento (o de todo el retiro). None si ninguna OT trae hora.

    cita: datetime (o 'dd/mm/aaaa hh:mm') de la cita del cliente, para comparar la salida. ahora: para las OT en curso."""
    jor = jor or jornada()
    ahora = _fecha(ahora) if ahora is not None else datetime.now()
    tramos = []
    for o in ots or []:
        if not isinstance(o, dict):
            continue
        ini = _inicio(o)
        if not ini:
            continue
        fin = _fin(o)
        curso = fin is None and not _final(o)
        tramos.append({"_id": id(o), "ot": o.get("ot") or "", "clase": _clase(o), "tipo": o.get("tipo") or "", "ini": ini,
                       "fin": fin or (ahora if curso else ini), "curso": curso, "asignada": _asignada(o)})
    if not tramos:
        return None
    tramos.sort(key=lambda t: t["ini"])

    trabajo, noches = 0.0, False
    for t in tramos:
        d = dur_real(t["ini"], t["fin"], jor)
        t["min"] = round(d["min"])
        trabajo += d["min"]
        noches = noches or d["pausa"]

    # Preparación efectiva: se juntan las OT que se pisan
    juntos = []
    for t in tramos:
        if juntos and t["ini"] <= juntos[-1][1]:
            juntos[-1][1] = max(juntos[-1][1], t["fin"])
        else:
            juntos.append([t["ini"], t["fin"]])
    efectivo = sum(dur_real(a, b, jor)["min"] for a, b in juntos)
    pausas = []
    for (a0, b0), (a1, b1) in zip(juntos, juntos[1:]):
        d = dur_real(b0, a1, jor)
        pausas.append({"desde": _fmt(b0), "hasta": _fmt(a1), "min": round(d["min"]), "reloj_min": round(d["reloj"]),
                       "noche": d["pausa"]})

    primero, ultimo = tramos[0]["ini"], max(t["fin"] for t in tramos)
    pf = dur_real(primero, ultimo, jor)
    en_curso = any(t["curso"] for t in tramos)

    pk = [t for t in tramos if t["clase"] == "picking"]
    sal = [t for t in tramos if t["clase"] == "salida"]
    listo = None
    if pk and sal and not any(t["curso"] for t in pk):
        pfin = max(t["fin"] for t in pk)
        if sal[0]["ini"] >= pfin:
            listo = round(dur_real(pfin, sal[0]["ini"], jor)["min"])
    asig = tramos[0]["asignada"]
    espera_asig = round(dur_real(asig, primero, jor)["min"]) if asig and asig <= primero else None
    salida = sal[0]["ini"] if sal else None
    c = _fecha(cita)
    vs_cita = round((c - salida).total_seconds() / 60) if (c and salida) else None
    # ¿El pedido estaba LISTO antes de la cita? (lo que más le importa al cliente: llegar y que esté preparado). Minutos entre el fin del
    # picking y la hora de la cita: positivo = listo antes; negativo = se terminó de preparar después de la hora acordada.
    pk_fin = max((t["fin"] for t in pk if not t["curso"]), default=None) if pk and not any(t["curso"] for t in pk) else None
    listo_vs_cita = round((c - pk_fin).total_seconds() / 60) if (c and pk_fin) else None

    # Por producto (Daniel 2026-10-06: «registro de cuánto se tarda por producto en promedio»): los minutos de cada OT de PICKING se reparten
    # entre sus líneas según las unidades ejecutadas; así, sumando muchos retiros, sale el promedio de minutos por unidad de cada SKU.
    productos, pk_min, pk_uni, pk_lin = [], 0.0, 0.0, 0
    for o, t in _pares(ots, tramos):
        if t["clase"] != "picking" or t["curso"]:
            continue
        lineas = [l for l in (o.get("lineas") or []) if isinstance(l, dict)]
        uni_ot = sum(_uni(l) for l in lineas)
        pk_min += t["min"]
        pk_uni += uni_ot
        pk_lin += len(lineas)
        for l in lineas:
            u = _uni(l)
            if not l.get("sku") and not l.get("descripcion"):
                continue
            productos.append({"sku": l.get("sku") or "", "descripcion": l.get("descripcion") or "", "unidades": u, "ot": t["ot"],
                              "min": round(t["min"] * (u / uni_ot), 2) if uni_ot else None})

    return {
        "version": VERSION,
        "picking_min": round(pk_min), "picking_unidades": pk_uni, "picking_lineas": pk_lin,
        "min_por_unidad": round(pk_min / pk_uni, 2) if pk_uni else None, "productos": productos,
        "criterio": ("Hora de Check. Tramos de un día a otro: solo jornada de bodega %02d:%02d–%02d:%02d lunes a viernes. "
                     "Trabajo = suma de cada OT; preparación efectiva = tiempo con al menos una OT abierta; "
                     "pausas = principio a fin − preparación efectiva.") % (jor[0] // 60, jor[0] % 60, jor[1] // 60, jor[1] % 60),
        "n_ot": len(tramos), "en_curso": en_curso,
        "inicio": _fmt(primero), "fin": None if en_curso else _fmt(ultimo),
        "trabajo_min": round(trabajo), "efectivo_min": round(efectivo),
        "principio_fin_min": round(pf["min"]), "reloj_min": round(pf["reloj"]),
        "pausas_min": max(0, round(pf["min"] - efectivo)), "pausas": pausas, "sin_noches": bool(pf["pausa"] or noches),
        "listo_esperando_min": listo, "espera_asignacion_min": espera_asig,
        "salida": _fmt(salida), "vs_cita_min": vs_cita, "listo_antes_cita_min": listo_vs_cita, "listo_en": _fmt(pk_fin),
        "ots": [{"ot": t["ot"], "tipo": t["tipo"], "inicio": _fmt(t["ini"]), "fin": None if t["curso"] else _fmt(t["fin"]),
                 "min": t["min"], "en_curso": t["curso"]} for t in tramos],
    }


def analizar_retiro(documentos, cita=None, ahora=None, jor=None):
    """Análisis por documento + uno del retiro completo (todas las OT de todos sus documentos).
    documentos: [{'rotulo': ..., 'ots': [...]}] como los arma check-actividad."""
    por_doc, todas = [], []
    for d in documentos or []:
        ots = (d or {}).get("ots") or []
        todas.extend(ots)
        a = analizar(ots, cita, ahora, jor)
        if a:
            por_doc.append({"documento": (d or {}).get("rotulo") or "", **a})
    total = analizar(todas, cita, ahora, jor)
    if not total:
        return None
    return {**total, "documentos": por_doc, "calculado": _fmt(_fecha(ahora) if ahora is not None else datetime.now())}

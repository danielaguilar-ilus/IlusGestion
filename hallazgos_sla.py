# -*- coding: utf-8 -*-
"""SLA Y TRAZABILIDAD DE LOS HALLAZGOS (Daniel, 26-oct-2026).

Daniel: «necesito implementar los SLA y poder tener trazabilidad y considerar varios puntos de salida: OT, Tickets, Abastecimiento y
almacenamiento de repuestos. Y el consumo de las OT» y «necesito tablas amigables con filas informativas y una buena sensación de guía,
ya sabes, como Retiros».

Este módulo es PURO (sin base de datos, sin Flask, sin correo): lo usan `app.py` (el monitor de Hallazgos y la tarjeta de gestión de la OT) y
las pruebas (`tests/test_hallazgos_sla.py`). Reutiliza la MISMA jornada, feriados y horas hábiles de Retiros e Inquietudes
(`repuestos_respuesta`, `inquietudes._reloj_tramo`): una sola definición de «hora hábil» en todo el proyecto.

Qué calcula:
  · `reloj(fila, ahora)`   el plazo en DOS tramos, como Inquietudes: «primera respuesta» (alguien toma el caso o ya le dio una salida) y
                           «salida» (el hallazgo ya tiene su camino: OT de seguimiento, ticket, solicitud de repuesto o descartado). Los graves
                           (falla, fuera de servicio, baja o riesgo para alguien) tienen plazos más cortos. Semáforo rojo/ámbar/verde/gris.
  · `recorrido(ctx)`       los 7 pasos de la gestión, cada uno con su semáforo: Atención → Valorización → OT de seguimiento → Ticket →
                           Abastecimiento → Bodega (almacenamiento) → Consumo (lo que la OT efectivamente gastó). Nada plegado (REGLA #15).
  · `siguiente(ctx)`       «lo que toca ahora», una frase como la del Monitor de Retiros: así cada fila guía a quien la mira.
  · `consumo_ot(...)`      cuánto consumió una OT (repuestos instalados) — plata: solo gestión (REGLAS #19 y #26).

Los plazos se ajustan por variables de entorno (HALLAZGOS_PRIMERA_GRAVE_H, HALLAZGOS_PRIMERA_H, HALLAZGOS_SALIDA_GRAVE_DIAS,
HALLAZGOS_SALIDA_DIAS, HALLAZGOS_AVISO_PCT): sin ellas rigen los valores de abajo. Nada de esto avisa ni escribe a nadie: solo se muestra.
"""
import os

import inquietudes as _iq
import repuestos_respuesta as _rr

# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  Plazos
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
PRIMERA_GRAVE_H = 2.0       # horas hábiles para tomar un hallazgo grave
PRIMERA_H = 4.0             # horas hábiles para tomar uno normal
SALIDA_GRAVE_DIAS = 2.0     # días hábiles para darle salida (OT, ticket, repuesto o descartarlo) a uno grave
SALIDA_DIAS = 5.0           # días hábiles para darle salida a uno normal
AVISO_FRACCION = 0.75       # ámbar al usar el 75 % del plazo
SEVERIDADES_GRAVES = ("falla", "fuera_servicio", "baja")


def _num(v, defecto):
    try:
        x = float(str(v).strip().replace(",", "."))
        return x if x > 0 else defecto
    except (TypeError, ValueError):
        return defecto


def config(environ=None):
    """{primera_grave_h, primera_h, salida_grave_h, salida_h, aviso_fraccion} (todo en horas hábiles; un día hábil = 9 h)."""
    env = os.environ if environ is None else environ
    pct = _num(env.get("HALLAZGOS_AVISO_PCT"), AVISO_FRACCION * 100.0)
    return {
        "primera_grave_h": _num(env.get("HALLAZGOS_PRIMERA_GRAVE_H"), PRIMERA_GRAVE_H),
        "primera_h": _num(env.get("HALLAZGOS_PRIMERA_H"), PRIMERA_H),
        "salida_grave_h": _num(env.get("HALLAZGOS_SALIDA_GRAVE_DIAS"), SALIDA_GRAVE_DIAS) * _rr.HORAS_POR_DIA,
        "salida_h": _num(env.get("HALLAZGOS_SALIDA_DIAS"), SALIDA_DIAS) * _rr.HORAS_POR_DIA,
        "aviso_fraccion": min(0.95, max(0.1, pct / 100.0)),
    }


def es_grave(severidad, riesgo=False):
    """Falla, fuera de servicio, baja o «puede lastimar a alguien»: los que tienen el plazo corto."""
    return bool(riesgo) or str(severidad or "") in SEVERIDADES_GRAVES


def _g(f, k, d=None):
    return f.get(k, d) if isinstance(f, dict) else getattr(f, k, d)


def _min_fecha(*fechas):
    v = [x for x in fechas if x]
    return min(v) if v else None


def reloj(fila, ahora, feriados=None, cfg=None, jor=None):
    """El plazo de UN hallazgo (fechas en hora Chile SIN zona, como `inquietudes.reloj`).

    `fila`: estado_ef (abierto · tomado · resuelto · descartado), severidad, riesgo, detectado_en, tomado_en, descartado_en, seguimiento_en,
    ticket_en, repuesto_en, seg_cerrada. Devuelve {primera, salida, semaforo, vencida, pill, sub, pct, grave, terminado}:
      primera  tramo «hasta que alguien toma el caso»; se cumple al tomarlo o al darle cualquier salida (lo primero que pase);
      salida   tramo «hasta que tiene su camino»; se cumple con la OT de seguimiento, el ticket, la solicitud de repuesto o al descartarlo;
      semaforo rojo (algún plazo vencido) · ámbar (cerca del plazo) · verde (con salida o en plazo) · gris (descartado).
    Un hallazgo descartado, o con OT de seguimiento ya cerrada, no corre más."""
    cfg = cfg or config()
    est = _g(fila, "estado_ef") or "abierto"
    grave = es_grave(_g(fila, "severidad"), _g(fila, "riesgo"))
    ini = _g(fila, "detectado_en")
    plazo_p = cfg["primera_grave_h"] if grave else cfg["primera_h"]
    plazo_s = cfg["salida_grave_h"] if grave else cfg["salida_h"]
    if est == "descartado":
        fin = _g(fila, "descartado_en")
        usado = _rr.horas_habiles(ini, fin, feriados, jor) if (ini and fin) else 0.0
        return {"primera": None, "salida": None, "semaforo": "gris", "vencida": False, "grave": grave, "terminado": True, "pct": 100,
                "pill": "Descartado" + (" en %s" % _rr.fmt_horas(usado) if fin else ""), "sub": "Ya no corre el plazo"}
    salida_en = _min_fecha(_g(fila, "seguimiento_en"), _g(fila, "ticket_en"), _g(fila, "repuesto_en"))
    primera_en = _min_fecha(_g(fila, "tomado_en"), salida_en)
    primera = _iq._reloj_tramo(ini, primera_en, plazo_p, ahora, cfg, feriados, jor)
    salida = _iq._reloj_tramo(ini, salida_en, plazo_s, ahora, cfg, feriados, jor)
    seg_cerrada = bool(_g(fila, "seg_cerrada"))
    estados = [primera["estado"], salida["estado"]]
    if seg_cerrada:
        sem = "verde"
    elif "vencida" in estados:
        sem = "rojo"
    elif "por_vencer" in estados:
        sem = "ambar"
    elif salida["terminado"] and "cumplida_tarde" in estados:
        sem = "ambar"
    else:
        sem = "verde"
    # Qué se dice y qué barra se dibuja: el tramo que está corriendo (primero «tomarlo», después «darle salida»).
    if seg_cerrada:
        pill, sub, activo = "OT de seguimiento cerrada", "Hallazgo resuelto", salida
    elif not primera["terminado"]:
        activo = primera
        if primera["estado"] == "vencida":
            pill = "Sin tomar hace %s" % _rr.fmt_horas(primera["transcurrido_h"])
            sub = "Plazo para tomarlo: %s%s" % (_rr.fmt_horas(plazo_p), " (grave)" if grave else "")
        else:
            pill = "Falta tomarlo: quedan %s" % _rr.fmt_horas(max(0.0, primera["restante_h"]))
            sub = "Plazo para tomarlo: %s%s" % (_rr.fmt_horas(plazo_p), " (grave)" if grave else "")
    elif not salida["terminado"]:
        activo = salida
        if salida["estado"] == "vencida":
            pill = "Sin salida hace %s" % _rr.fmt_horas(salida["transcurrido_h"] - plazo_s)
            sub = "Plazo para darle salida: %s" % _rr.fmt_horas(plazo_s)
        else:
            pill = "Elegir su salida: quedan %s" % _rr.fmt_horas(max(0.0, salida["restante_h"]))
            sub = "Plazo para darle salida: %s" % _rr.fmt_horas(plazo_s)
    else:
        activo = salida
        pill = "Con salida en %s" % _rr.fmt_horas(salida["transcurrido_h"])
        sub = "OT, ticket o repuesto en marcha"
    pct = 100 if activo["estado"] == "vencida" else int(max(0.0, min(100.0, 100.0 * activo["transcurrido_h"] / activo["plazo_h"]))) \
        if activo.get("plazo_h") else 0
    if activo.get("terminado"):
        pct = 100
    return {"primera": primera, "salida": salida, "semaforo": sem, "vencida": sem == "rojo", "grave": grave, "terminado": seg_cerrada,
            "pill": pill, "sub": sub, "pct": pct}


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  Abastecimiento, almacenamiento (bodega) y consumo
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
ESTADOS_SOL = ("solicitado", "validado", "pedido", "recibido", "instalado", "rechazado")
SOL_TXT = {"solicitado": "por validar", "validado": "validado", "pedido": "pedido al proveedor", "recibido": "recibido en bodega",
           "instalado": "instalado en la OT", "rechazado": "rechazado"}


def _cant(x):
    try:
        return float(x or 0)
    except (TypeError, ValueError):
        return 0.0


def num_txt(x):
    """3.0 → «3» · 2.5 → «2,5»."""
    x = _cant(x)
    return (("%d" % x) if x == int(x) else ("%.2f" % x).rstrip("0").rstrip(".").replace(".", ","))


def resumen_sols(sols):
    """Cuenta las solicitudes de repuesto por estado (sin las rechazadas en el total vivo). → {n, vivas, por_estado, cantidad, instaladas}."""
    por = {e: 0 for e in ESTADOS_SOL}
    cant = inst = 0.0
    for s in sols or []:
        e = str(_g(s, "estado") or "solicitado")
        por[e] = por.get(e, 0) + 1
        if e != "rechazado":
            cant += _cant(_g(s, "cantidad"))
        if e == "instalado":
            inst += _cant(_g(s, "cantidad"))
    n = len(sols or [])
    return {"n": n, "vivas": n - por["rechazado"], "por_estado": por, "cantidad": cant, "instaladas": inst}


def abastecimiento(sols):
    """El punto de salida «Abastecimiento»: dónde va lo que se pidió. → {estado, color, txt, sub}.
    estado: na (sin solicitud) · curso (por validar, validado o pedido) · hecho (todo recibido o instalado) · rechazado."""
    r = resumen_sols(sols)
    if not r["n"]:
        return {"estado": "na", "color": "gris", "txt": "Sin solicitud", "sub": "Si hace falta un repuesto, solicítalo desde aquí"}
    p = r["por_estado"]
    if r["vivas"] == 0:
        return {"estado": "rechazado", "color": "gris", "txt": "Rechazada", "sub": "Todas las solicitudes se rechazaron"}
    pendientes = p["solicitado"] + p["validado"] + p["pedido"]
    if pendientes == 0:
        return {"estado": "hecho", "color": "verde", "txt": "Repuesto en bodega" if p["instalado"] == 0 else "Repuesto recibido",
                "sub": "%d solicitud%s: recibida%s" % (r["vivas"], "" if r["vivas"] == 1 else "es", "" if r["vivas"] == 1 else "s")}
    # El paso más atrasado es el que manda: dice qué está esperando.
    if p["solicitado"]:
        txt, sub = "Por validar", "Falta que bodega o gestión lo valide"
    elif p["validado"]:
        txt, sub = "Validado: falta pedirlo", "Pedir al proveedor o sacarlo de bodega"
    else:
        txt, sub = "Pedido: esperando", "Esperando que llegue el repuesto"
    return {"estado": "curso", "color": "azul", "txt": txt, "sub": sub}


def bodega(sols, stock_por_id):
    """El punto de salida «Almacenamiento» (bodega de repuestos): ¿hay con qué cumplir lo que se pidió? Compara lo pedido de cada repuesto de
    la Bodega con su existencia física. `stock_por_id`: {repuesto_stock_id: {cantidad, ubicacion}}. → {estado, color, txt, sub}.
    Las solicitudes ya instaladas no cuentan (esa unidad ya salió de bodega)."""
    stock_por_id = stock_por_id or {}
    pedido = {}
    propio_compromete = {}      # lo de ESTE hallazgo que ya cuenta como comprometido (validado o recibido): no se resta dos veces
    sin_ficha = 0
    for s in sols or []:
        e = str(_g(s, "estado") or "solicitado")
        if e in ("instalado", "rechazado"):
            continue
        sid = _g(s, "repuesto_stock_id")
        if not sid:
            sin_ficha += 1
            continue
        pedido[int(sid)] = pedido.get(int(sid), 0.0) + _cant(_g(s, "cantidad"))
        if e in ("validado", "recibido"):
            propio_compromete[int(sid)] = propio_compromete.get(int(sid), 0.0) + _cant(_g(s, "cantidad"))
    if not pedido and not sin_ficha:
        if any(str(_g(s, "estado")) == "instalado" for s in sols or []):
            return {"estado": "hecho", "color": "verde", "txt": "Ya salió de bodega", "sub": "Lo pedido se instaló en la OT"}
        return {"estado": "na", "color": "gris", "txt": "—", "sub": "Sin repuesto que guardar o sacar"}
    faltan, alcanza, ubic = [], [], []
    for sid, cant in pedido.items():
        ficha = stock_por_id.get(sid) or {}
        fisico = _cant(ficha.get("cantidad"))
        # Lo que OTRAS solicitudes ya tienen prometido (validadas o recibidas) no está disponible para este hallazgo.
        otros = max(0.0, _cant(ficha.get("comprometido")) - propio_compromete.get(sid, 0.0))
        u = ficha.get("ubicacion")
        if u:
            ubic.append(str(u))
        (alcanza if fisico - otros >= cant else faltan).append((sid, cant, fisico, otros))
    if faltan:
        sid, cant, fis, otros = faltan[0]
        if len(faltan) > 1:
            sub = "Faltan %d repuestos" % len(faltan)
        elif otros > 0 and fis >= cant:
            sub = "Piden %s: hay %s en bodega, pero %s ya están prometidos a otras solicitudes" % (num_txt(cant), num_txt(fis), num_txt(otros))
        elif otros > 0:
            sub = "Piden %s y hay %s en bodega (%s ya prometidos a otras)" % (num_txt(cant), num_txt(fis), num_txt(otros))
        else:
            sub = "Piden %s y hay %s en bodega" % (num_txt(cant), num_txt(fis))
        return {"estado": "curso", "color": "ambar", "txt": "Sin stock suficiente" if fis else "Sin stock", "sub": sub}
    if alcanza:
        return {"estado": "hecho", "color": "verde", "txt": "Hay stock en bodega",
                "sub": ("Ubicación " + ", ".join(sorted(set(ubic)))) if ubic else "Existencia física suficiente"}
    return {"estado": "curso", "color": "ambar", "txt": "Repuesto sin ficha de bodega", "sub": "Créalo en Bodega para controlar su stock"}


def consumo(sols):
    """El punto de salida «Consumo»: lo que ya se instaló en la OT. → {estado, color, txt, sub, valor, n}. `valor` en pesos (o None si no
    hay costo): es plata, solo gestión (REGLAS #19 y #26)."""
    inst = [s for s in sols or [] if str(_g(s, "estado")) == "instalado"]
    vivas = [s for s in sols or [] if str(_g(s, "estado")) != "rechazado"]
    if not vivas:
        return {"estado": "na", "color": "gris", "txt": "Sin consumo", "sub": "Aún no se usó ningún repuesto", "valor": None, "n": 0}
    if not inst:
        return {"estado": "curso", "color": "gris", "txt": "Aún sin consumir", "sub": "Se consume al instalarlo en la OT", "valor": None, "n": 0}
    valor = 0.0
    con_costo = False
    for s in inst:
        c = _g(s, "costo_unitario")
        if c is not None and c != "":
            valor += _cant(c) * _cant(_g(s, "cantidad"))
            con_costo = True
    hechas = "%d de %d instalado%s" % (len(inst), len(vivas), "" if len(inst) == 1 else "s")
    return {"estado": "hecho" if len(inst) == len(vivas) else "curso", "color": "verde" if len(inst) == len(vivas) else "azul",
            "txt": hechas, "sub": "Consumido en la OT", "valor": int(round(valor)) if con_costo else None, "n": len(inst)}


def consumo_ot(filas):
    """El consumo de UNA OT: de las solicitudes que se instalaron en ella. `filas`: [{cantidad, costo_unitario}] → {n, valor, con_costo}."""
    n = 0.0
    valor = 0.0
    con = False
    for s in filas or []:
        n += _cant(_g(s, "cantidad"))
        c = _g(s, "costo_unitario")
        if c is not None and c != "":
            valor += _cant(c) * _cant(_g(s, "cantidad"))
            con = True
    return {"n": n, "valor": int(round(valor)) if con else None, "con_costo": con}


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  Recorrido y «lo que toca ahora»
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
def recorrido(ctx):
    """Los 7 pasos de la gestión de UN hallazgo, cada uno con su semáforo (verde hecho · azul en curso · ámbar falta · gris opcional).
    `ctx`: estado_ef, tomado_por, valor (texto «$80.990» o ""), seg {numero, cerrada} | None, ticket {numero, estado} | None, sols [],
    stock {id: {cantidad, ubicacion}}. → lista de pasos {clave, titulo, color, txt, sub, ok}."""
    est = ctx.get("estado_ef") or "abierto"
    descartado = est == "descartado"
    seg, tk = ctx.get("seg"), ctx.get("ticket")
    sols = ctx.get("sols") or []
    ab, bo, co = abastecimiento(sols), bodega(sols, ctx.get("stock")), consumo(sols)
    pasos = []
    tomado = ctx.get("tomado_por")
    pasos.append({"clave": "atencion", "titulo": "Atención", "ok": bool(tomado) or descartado or est == "resuelto",
                  "color": "verde" if (tomado or descartado or est == "resuelto") else "ambar",
                  "txt": ("Lo tomó " + tomado) if tomado else ("Descartado" if descartado else ("Con OT de seguimiento" if est == "resuelto" else "Nadie lo ha tomado")),
                  "sub": ""})
    val = ctx.get("valor")
    pasos.append({"clave": "valor", "titulo": "Valorización", "ok": bool(val), "color": "verde" if val else ("gris" if descartado else "ambar"),
                  "txt": ("Estimado " + val) if val else "Sin valorizar", "sub": ""})
    if seg:
        cerr = bool(seg.get("cerrada"))
        pasos.append({"clave": "ot", "titulo": "OT de seguimiento", "ok": cerr, "color": "verde" if cerr else "azul",
                      "txt": seg.get("numero") or "OT", "sub": "Cerrada" if cerr else "En curso"})
    else:
        pasos.append({"clave": "ot", "titulo": "OT de seguimiento", "ok": False, "color": "gris", "txt": "Sin OT", "sub": ""})
    if tk:
        pasos.append({"clave": "ticket", "titulo": "Ticket", "ok": True, "color": "verde" if tk.get("cerrado") else "azul",
                      "txt": tk.get("numero") or "Ticket", "sub": tk.get("estado_txt") or ""})
    else:
        pasos.append({"clave": "ticket", "titulo": "Ticket", "ok": False, "color": "gris", "txt": "Sin ticket", "sub": ""})
    pasos.append({"clave": "abastecimiento", "titulo": "Abastecimiento", "ok": ab["estado"] == "hecho", "color": ab["color"], "txt": ab["txt"], "sub": ab["sub"]})
    pasos.append({"clave": "bodega", "titulo": "Bodega", "ok": bo["estado"] == "hecho", "color": bo["color"], "txt": bo["txt"], "sub": bo["sub"]})
    pasos.append({"clave": "consumo", "titulo": "Consumo de la OT", "ok": co["estado"] == "hecho", "color": co["color"], "txt": co["txt"], "sub": co["sub"]})
    return pasos


def siguiente(ctx):
    """La frase de «lo que toca ahora» (como el «siguiente paso» del Monitor de Retiros). → {txt, accion} (accion: tomar · valorizar · salida ·
    validar · pedir · bodega · esperar · instalar · ot · ticket · listo · nada)."""
    est = ctx.get("estado_ef") or "abierto"
    seg, tk = ctx.get("seg"), ctx.get("ticket")
    sols = ctx.get("sols") or []
    if est == "descartado":
        return {"txt": "Descartado: no hay nada más por hacer", "accion": "nada"}
    if seg and seg.get("cerrada"):
        return {"txt": "Listo: la OT de seguimiento %s se cerró" % (seg.get("numero") or ""), "accion": "listo"}
    r = resumen_sols(sols)
    p = r["por_estado"]
    vivas = [s for s in sols if str(_g(s, "estado")) != "rechazado"]
    if r["vivas"]:
        if p["solicitado"]:
            return {"txt": "Validar el repuesto pedido (Repuestos → Solicitudes)", "accion": "validar"}
        if p["validado"]:
            validadas = [s for s in vivas if str(_g(s, "estado")) == "validado"]
            # Antes de mirar la bodega: ¿la solicitud ya tomó otro camino? (comprada, asociada a una OT o resuelta con una OT).
            if any(_g(s, "compra_id") for s in validadas):
                return {"txt": "Ya está en una compra: esperar al proveedor", "accion": "esperar"}
            if any(_g(s, "ot_generada_id") for s in validadas):
                return {"txt": "El repuesto va en una OT: seguir esa OT", "accion": "ot"}
            if any(str(_g(s, "camino") or "") == "ot" and not _g(s, "repuesto_stock_id") for s in validadas):
                return {"txt": "Se resuelve con una OT: generarla o asociarla", "accion": "ot"}
            bo = bodega(sols, ctx.get("stock"))
            if bo["estado"] == "hecho":
                return {"txt": "Hay stock: preparar el repuesto desde bodega", "accion": "bodega"}
            return {"txt": "Sin stock suficiente: pedirlo al proveedor", "accion": "pedir"}
        if p["pedido"]:
            return {"txt": "Esperando que llegue el repuesto pedido", "accion": "esperar"}
        if p["recibido"]:
            return {"txt": "Repuesto en bodega: instalarlo en la OT", "accion": "instalar"}
        if p["instalado"] and not seg:
            # Las acciones que existen son levantar la OT o descartar (con su motivo): la frase apunta a ellas.
            return {"txt": "Repuesto instalado: levanta la OT de seguimiento o descarta el hallazgo con su motivo", "accion": "salida"}
    if seg:
        return {"txt": "Esperando que se cierre la OT %s" % (seg.get("numero") or "de seguimiento"), "accion": "ot"}
    if tk:
        if tk.get("cerrado"):
            return {"txt": "Ticket %s cerrado: levanta la OT de seguimiento o descarta el hallazgo" % (tk.get("numero") or ""), "accion": "salida"}
        return {"txt": "Ticket %s abierto: seguirlo en Tickets" % (tk.get("numero") or ""), "accion": "ticket"}
    if est == "abierto" and not ctx.get("tomado_por"):
        return {"txt": "Tomar el caso (así corre su plazo y se sabe quién lo lleva)", "accion": "tomar"}
    if not ctx.get("valor"):
        return {"txt": "Valorizar y elegir su salida: OT de seguimiento, ticket o repuesto", "accion": "valorizar"}
    return {"txt": "Elegir su salida: OT de seguimiento, ticket o repuesto", "accion": "salida"}

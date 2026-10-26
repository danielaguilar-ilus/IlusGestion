# -*- coding: utf-8 -*-
"""Inquietudes de clientes (módulo PURO: sin base de datos, sin Flask, sin correo).

Daniel (25-oct-2026): «los hallazgos… son las inquietudes del cliente que pueden levantar los técnicos… verlos bien profesional como el
módulo de recepción de Retiros con SLA de atención… un CRUD con registro de actividad… que el superadmin pueda borrar y los demás puedan
adjudicarse el ticket, la OT o la solicitud… que el usuario pueda recorrer el paso a paso, tipo semáforo, y tracking de lo que debe realizar:
si es solicitar repuestos, abastecer, generar un ticket para crear una OT o crear una OT directo».

Aquí vive la ÚNICA lógica de:
  · los estados y los tipos de solicitud (qué pide la inquietud);
  · el reloj SLA (primera respuesta y resolución) en HORAS HÁBILES, con el mismo calendario que la cobertura de Retiros y la cola de
    Repuestos (`repuestos_respuesta`: lunes a viernes sin feriados de Chile, jornada 08:00-17:00);
  · el semáforo (verde · ámbar cerca del plazo · rojo vencido);
  · el recorrido paso a paso de cada tipo de solicitud y cuál es el paso en que está (`ruta`);
  · las validaciones de crear, editar, cerrar, anular y borrar y quién puede cada acción (`puede_accion`);
  · los textos de la campana.

Las fechas que entran y salen son `datetime` SIN zona, en HORA DE CHILE (la conversión desde UTC la hace app.py). Nada de aquí escribe a un
cliente (REGLA #22) ni muestra plata ni proveedores (REGLAS #19/#26). Pruebas: tests/test_inquietudes.py.
"""
import json
import os
import re
from datetime import datetime

import repuestos_respuesta as _rr     # UNA sola definición de jornada, feriados y horas hábiles

VERSION = 1

# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  Catálogos
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
EST_NUEVA, EST_ATENCION, EST_ESCALADA, EST_CERRADA, EST_ANULADA = "nueva", "en_atencion", "escalada", "cerrada", "anulada"
ESTADOS = {
    EST_NUEVA: {"texto": "Nueva", "color": "rojo", "icono": "bi-bell-fill", "orden": 1,
                "ayuda": "Nadie la ha tomado todavía: corre el reloj de la primera respuesta."},
    EST_ATENCION: {"texto": "En atención", "color": "ambar", "icono": "bi-person-check-fill", "orden": 2,
                   "ayuda": "Ya tiene responsable y se está resolviendo."},
    EST_ESCALADA: {"texto": "Escalada", "color": "azul", "icono": "bi-diagram-3-fill", "orden": 3,
                   "ayuda": "Se levantó un ticket o una OT: se sigue ahí."},
    EST_CERRADA: {"texto": "Cerrada", "color": "verde", "icono": "bi-check-circle-fill", "orden": 4,
                  "ayuda": "Resuelta, con su resolución escrita."},
    EST_ANULADA: {"texto": "Anulada", "color": "gris", "icono": "bi-slash-circle-fill", "orden": 5,
                  "ayuda": "No procede; queda el motivo."},
}
ESTADOS_ABIERTOS = (EST_NUEVA, EST_ATENCION, EST_ESCALADA)
ESTADOS_TERMINADOS = (EST_CERRADA, EST_ANULADA)

T_REPUESTO, T_ABASTECER, T_TICKET, T_OT, T_REPARACION, T_SIN = ("repuesto", "abastecer", "ticket_para_ot", "ot_directa", "reparacion",
                                                              "sin_clasificar")
TIPOS = {
    T_SIN: {"texto": "Sin clasificar", "icono": "bi-question-circle-fill",
            "ayuda": "Todavía no se ha decidido qué pide el cliente."},
    T_REPUESTO: {"texto": "Solicitar repuesto", "icono": "bi-gear-wide-connected",
                 "ayuda": "El equipo necesita una pieza: se pide con una solicitud de repuesto."},
    T_ABASTECER: {"texto": "Abastecer", "icono": "bi-box-seam-fill",
                  "ayuda": "Falta stock o material: hay que abastecer."},
    T_TICKET: {"texto": "Ticket para crear una OT", "icono": "bi-ticket-detailed-fill",
               "ayuda": "Se levanta un ticket y de él nace la OT."},
    T_OT: {"texto": "Crear una OT directa", "icono": "bi-clipboard2-plus-fill",
           "ayuda": "Se levanta la OT sin pasar por un ticket."},
    T_REPARACION: {"texto": "Reparación", "icono": "bi-wrench-adjustable-circle-fill",
                   "ayuda": "El equipo hay que repararlo: diagnóstico, reparación y verificación."},
}

ORIG_OT, ORIG_FICHA, ORIG_SUPER = "ot", "ficha_cliente", "superadmin"
ORIGENES = {ORIG_OT: "Desde una OT (técnico)", ORIG_FICHA: "Reportada por el cliente (ficha)", ORIG_SUPER: "Creada por el superadmin"}

# Longitudes
MIN_TEXTO = 10
MAX_TEXTO = 700
MAX_MOTIVO = 500
MAX_FOTOS = 3
MAX_PRODUCTO = 200

# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  Plazos (configurables)
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
PRIMERA_RESPUESTA_H = 4.0                           # 4 horas hábiles hasta que alguien la toma
RESOLUCION_DIAS = 2.0                               # 2 días hábiles hasta cerrarla o escalarla
AVISO_FRACCION = 0.75                               # ámbar al usar el 75 % del plazo


def _num(v, defecto):
    try:
        x = float(str(v).strip().replace(",", "."))
        return x if x > 0 else defecto
    except (TypeError, ValueError):
        return defecto


def config(environ=None):
    """{primera_h, resolucion_h, aviso_fraccion}. Variables: INQUIETUDES_PRIMERA_H (horas hábiles), INQUIETUDES_RESOLUCION_DIAS (días hábiles),
    INQUIETUDES_AVISO_PCT (porcentaje del plazo en que pasa a ámbar). Un día hábil = 9 h (08:00-17:00)."""
    env = os.environ if environ is None else environ
    pct = _num(env.get("INQUIETUDES_AVISO_PCT"), AVISO_FRACCION * 100.0)
    frac = min(0.95, max(0.1, pct / 100.0))
    return {"primera_h": _num(env.get("INQUIETUDES_PRIMERA_H"), PRIMERA_RESPUESTA_H),
            "resolucion_h": _num(env.get("INQUIETUDES_RESOLUCION_DIAS"), RESOLUCION_DIAS) * _rr.HORAS_POR_DIA,
            "aviso_fraccion": frac}


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  Textos y validaciones
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
def limpio(v, maximo=MAX_TEXTO):
    """Texto en una línea de espacios normales y recortado."""
    return " ".join(str(v or "").split())[:maximo]


def entero(v):
    try:
        n = int(str(v).strip())
        return n if n > 0 else None
    except (TypeError, ValueError):
        return None


def numero(iid):
    """«INQ-00042»."""
    n = entero(iid)
    return "INQ-%05d" % n if n else "INQ-?"


def nombre_con_ficha(nombre, sku=None, serie=None):
    """«Trotadora X (SKU ABC) · S/N 123», completo (REGLA #15)."""
    t = limpio(nombre, 400) or "Equipo sin nombre"
    if limpio(sku, 100):
        t += " (SKU %s)" % limpio(sku, 100)
    if limpio(serie, 100):
        t += " · S/N %s" % limpio(serie, 100)
    return t


def validar_crear(inquietud, diagnostico, maquina_id=None, producto_texto=None, origen=ORIG_OT):
    """→ (ok, mensaje). Desde la OT el técnico debe escribir las dos cosas (mínimo 10 caracteres); la gestión (ficha del cliente o
    superadmin) puede dejar el diagnóstico para después. El equipo puede ser uno de la ficha del cliente o uno escrito a mano."""
    if len(limpio(inquietud)) < MIN_TEXTO:
        return False, "Cuenta qué le preocupa al cliente (mínimo %d caracteres)." % MIN_TEXTO
    d = limpio(diagnostico)
    if origen == ORIG_OT and len(d) < MIN_TEXTO:
        return False, "Escribe tu diagnóstico del equipo (mínimo %d caracteres)." % MIN_TEXTO
    if d and len(d) < MIN_TEXTO:
        return False, "El diagnóstico es muy corto (mínimo %d caracteres)." % MIN_TEXTO
    if not entero(maquina_id) and len(limpio(producto_texto, MAX_PRODUCTO)) < 3:
        return False, "Elige el equipo en el buscador o escribe cuál es."
    return True, ""


def validar_motivo(motivo, que="Escribe el motivo"):
    if len(limpio(motivo, MAX_MOTIVO)) < MIN_TEXTO:
        return False, "%s (mínimo %d caracteres)." % (que, MIN_TEXTO)
    return True, ""


def validar_cierre(resolucion):
    return validar_motivo(resolucion, "Para cerrar escribe la resolución: qué se hizo o se decidió")


def validar_tipo(tipo):
    return tipo in TIPOS


def titulo(equipo):
    return limpio("Inquietud del cliente · " + (limpio(equipo, 250) or "equipo sin identificar"), 300)


def fotos_a_json(urls):
    """Lista de URLs → JSON (máximo MAX_FOTOS, solo textos http(s) o rutas /f/ del sitio)."""
    out = []
    for u in urls or []:
        u = str(u or "").strip()
        if u and (u.startswith("http") or u.startswith("/")) and u not in out:
            out.append(u[:500])
    return json.dumps(out[:MAX_FOTOS])


def fotos_de_json(txt):
    try:
        v = json.loads(txt) if txt else []
    except (TypeError, ValueError):
        return []
    return [str(u) for u in v if isinstance(u, str)][:MAX_FOTOS] if isinstance(v, list) else []


def pasos_de_json(txt):
    """{clave: {"por": …, "en": …}}: pasos que una persona marcó a mano."""
    try:
        v = json.loads(txt) if txt else {}
    except (TypeError, ValueError):
        return {}
    return {str(k): x for k, x in v.items() if isinstance(x, dict)} if isinstance(v, dict) else {}


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  Reloj SLA y semáforo
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
def _g(f, k, d=None):
    return f.get(k, d) if isinstance(f, dict) else getattr(f, k, d)


def _reloj_tramo(inicio, fin, plazo_h, ahora, cfg, feriados=None, jor=None):
    """Un tramo del SLA. estado: en_plazo · por_vencer · vencida · cumplida (se resolvió dentro del plazo) · cumplida_tarde."""
    if not inicio:
        return {"estado": "en_plazo", "transcurrido_h": 0.0, "plazo_h": plazo_h, "vence": None, "restante_h": plazo_h, "terminado": False}
    vence = _rr.sumar_horas_habiles(inicio, plazo_h, feriados, jor)
    corte = fin or ahora
    usado = _rr.horas_habiles(inicio, corte, feriados, jor)
    out = {"transcurrido_h": usado, "plazo_h": plazo_h, "vence": vence, "restante_h": plazo_h - usado, "terminado": bool(fin)}
    if fin:
        out["estado"] = "cumplida" if usado <= plazo_h else "cumplida_tarde"
    elif usado >= plazo_h:
        out["estado"] = "vencida"
    elif usado >= plazo_h * cfg["aviso_fraccion"]:
        out["estado"] = "por_vencer"
    else:
        out["estado"] = "en_plazo"
    return out


def reloj(fila, ahora, feriados=None, cfg=None, jor=None):
    """El SLA de UNA inquietud (hora Chile).

    `fila` trae: estado, creada_en, primera_respuesta_en, resuelta_en. Devuelve {primera, resolucion, semaforo, texto}:
      primera     tramo «hasta que alguien la toma» (se cumple al adjudicarse/asignarse o al primer movimiento de gestión);
      resolucion  tramo «hasta cerrarla» (se cumple al cerrarla; una escalada sigue contando hasta cerrarse);
      semaforo    verde (todo cumplido o en plazo) · ambar (cerca del plazo) · rojo (algún plazo vencido) · gris (anulada).
    Una inquietud terminada ya no corre."""
    cfg = cfg or config()
    est = _g(fila, "estado") or EST_NUEVA
    creada = _g(fila, "creada_en")
    if est == EST_ANULADA:
        return {"primera": None, "resolucion": None, "semaforo": "gris", "texto": "Anulada", "vencida": False}
    prim_fin = _g(fila, "primera_respuesta_en")
    res_fin = _g(fila, "resuelta_en") if est == EST_CERRADA else None
    if est == EST_CERRADA and not res_fin:
        res_fin = ahora
    primera = _reloj_tramo(creada, prim_fin, cfg["primera_h"], ahora, cfg, feriados, jor)
    reab = _g(fila, "reabierta_en")
    ini_res = reab if (reab and creada and reab > creada) else creada      # una reapertura reinicia el reloj de la resolución
    resolucion = _reloj_tramo(ini_res, res_fin, cfg["resolucion_h"], ahora, cfg, feriados, jor)
    estados = [primera["estado"], resolucion["estado"]]
    if "vencida" in estados:
        sem = "rojo"
    elif "por_vencer" in estados:
        sem = "ambar"
    elif est == EST_CERRADA and "cumplida_tarde" in estados:
        sem = "ambar"
    else:
        sem = "verde"
    return {"primera": primera, "resolucion": resolucion, "semaforo": sem, "vencida": sem == "rojo",
            "texto": texto_reloj(primera, resolucion, est)}


def texto_reloj(primera, resolucion, estado):
    """Frase corta para la tarjeta: lo que hay que mirar ahora."""
    if estado == EST_CERRADA:
        return "Cerrada en %s" % _rr.fmt_horas(resolucion["transcurrido_h"])
    if not primera["terminado"]:
        if primera["estado"] == "vencida":
            return "Sin responder hace %s (plazo %s)" % (_rr.fmt_horas(primera["transcurrido_h"]), _rr.fmt_horas(primera["plazo_h"]))
        return "Falta tomarla: quedan %s" % _rr.fmt_horas(max(0.0, primera["restante_h"]))
    if resolucion["estado"] == "vencida":
        return "Resolución vencida hace %s" % _rr.fmt_horas(resolucion["transcurrido_h"] - resolucion["plazo_h"])
    return "Para resolver: quedan %s" % _rr.fmt_horas(max(0.0, resolucion["restante_h"]))


def nivel_aviso(rel, ya_avisado=0):
    """Qué aviso toca (una sola vez por nivel): 1 = por vencer · 2 = vencida. 0 = ninguno."""
    if not rel or rel.get("semaforo") in ("verde", "gris"):
        return 0
    n = 2 if rel.get("vencida") else 1
    return n if n > int(ya_avisado or 0) else 0


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  Recorrido paso a paso
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
# Cada paso: clave · título · explicación · acción de la pantalla (`accion`) · cómo se sabe que está hecho.
#  «auto» = lo dice el sistema con lo que ya existe (ticket, OT, solicitud de repuesto); «manual» = lo marca una persona.
P_REGISTRADA, P_RESPONSABLE, P_CLASIFICAR, P_CIERRE = "registrada", "responsable", "clasificar", "cierre"


def _paso(clave, titulo_, texto, accion=None, auto=None):
    return {"clave": clave, "titulo": titulo_, "texto": texto, "accion": accion, "auto": auto}


def _pasos_de_tipo(tipo):
    if tipo == T_REPUESTO:
        return [
            _paso("solicitar_repuesto", "Solicitar el repuesto", "Pide la pieza con una solicitud de repuesto del equipo.", "solicitar_repuesto", "solicitud_repuesto"),
            _paso("repuesto_recibido", "Repuesto recibido", "La pieza llegó a bodega.", "marcar", "repuesto_recibido"),
            _paso("repuesto_instalado", "Repuesto instalado", "La pieza quedó puesta en el equipo del cliente.", "marcar", "repuesto_instalado"),
        ]
    if tipo == T_ABASTECER:
        return [
            _paso("revisar_stock", "Revisar el stock", "Confirma qué hay y qué falta en bodega.", "marcar"),
            _paso("abastecer", "Abastecer lo que falta", "Pide o compra lo que falta.", "marcar"),
            _paso("entregado", "Entregado al cliente", "Lo que faltaba ya está con el cliente.", "marcar"),
        ]
    if tipo == T_TICKET:
        return [
            _paso("levantar_ticket", "Levantar el ticket", "Crea un ticket interno (no le escribe al cliente).", "levantar_ticket", "ticket"),
            _paso("ot_desde_ticket", "Crear la OT desde el ticket", "De ese ticket nace la OT de trabajo.", "levantar_ot", "ot"),
            _paso("ot_ejecutada", "OT ejecutada", "El trabajo se hizo y la OT se terminó.", None, "ot_terminada"),
        ]
    if tipo == T_OT:
        return [
            _paso("levantar_ot", "Levantar la OT", "Abre el asistente de «Nueva OT» ya prellenado.", "levantar_ot", "ot"),
            _paso("ot_ejecutada", "OT ejecutada", "El trabajo se hizo y la OT se terminó.", None, "ot_terminada"),
        ]
    if tipo == T_REPARACION:
        return [
            _paso("diagnostico", "Diagnóstico confirmado", "Se confirmó qué tiene el equipo.", "marcar"),
            _paso("levantar_ot", "Agendar la reparación (OT)", "Levanta la OT de reparación.", "levantar_ot", "ot"),
            _paso("ot_ejecutada", "Reparación hecha", "La OT de reparación se terminó.", None, "ot_terminada"),
        ]
    return [_paso(P_CLASIFICAR, "Decidir qué pide", "Elige qué hay que hacer: repuesto, abastecer, ticket, OT o reparación.", "clasificar", "clasificada")]


def ruta(fila, ctx=None, pasos_manuales=None):
    """El recorrido de UNA inquietud. `ctx`: {solicitud_repuesto: bool, repuesto_recibido: bool, repuesto_instalado: bool, ticket: bool,
    ot: bool, ot_terminada: bool}. `pasos_manuales`: {clave: {por, en}} que una persona marcó a mano.

    → {pasos: [{clave, titulo, texto, accion, estado, quien, cuando}], actual, hechos, total, pendiente, avance}
    estado de cada paso: hecho · actual (el siguiente por hacer) · pendiente · vencido (el actual con el SLA vencido). Los dos primeros pasos son
    siempre «Registrada» y «Alguien se hace cargo»; el último, «Cerrar con resolución»."""
    ctx = ctx or {}
    manuales = pasos_manuales or {}
    est = _g(fila, "estado") or EST_NUEVA
    tipo = _g(fila, "tipo_solicitud") or T_SIN
    if tipo not in TIPOS:
        tipo = T_SIN
    terminado = est in ESTADOS_TERMINADOS
    cerrada = est == EST_CERRADA
    lista = [_paso(P_REGISTRADA, "Recibida", "El técnico o la gestión dejó anotado qué le preocupa al cliente.", None, "siempre"),
             _paso(P_RESPONSABLE, "Alguien se hace cargo", "Una persona de la gestión la toma («Adjudicarme») o se la asigna a un responsable.", "adjudicar", "responsable")]
    if tipo != T_SIN:
        lista.append(_paso(P_CLASIFICAR, "Qué se necesita: " + TIPOS[tipo]["texto"], TIPOS[tipo]["ayuda"], "clasificar", "siempre"))
    lista += _pasos_de_tipo(tipo)
    lista.append(_paso(P_CIERRE, "Cerrada con resolución", "Se escribe qué se hizo o se decidió. Sin eso no se cierra.", "cerrar", "cerrada"))
    out, actual_marcado = [], False
    for p in lista:
        m = manuales.get(p["clave"])
        a = p["auto"]
        if a == "siempre":
            hecho = True
        elif a == "responsable":
            hecho = bool(_g(fila, "responsable_user_id")) or terminado
        elif a == "clasificada":
            hecho = tipo != T_SIN
        elif a == "cerrada":
            hecho = cerrada
        elif a:
            hecho = bool(ctx.get(a)) or bool(m)
        else:
            hecho = bool(m)
        if terminado and not cerrada:
            estado_p = "hecho" if hecho else "pendiente"        # anulada: se queda como estaba
        elif hecho:
            estado_p = "hecho"
        elif not actual_marcado:
            estado_p, actual_marcado = "actual", True
        else:
            estado_p = "pendiente"
        out.append({"clave": p["clave"], "titulo": p["titulo"], "texto": p["texto"], "accion": p["accion"] if estado_p == "actual" else None, "estado": estado_p,
                    "quien": (m or {}).get("por") if m else None, "cuando": (m or {}).get("en") if m else None,
                    "marcable": p["accion"] == "marcar"})
    hechos = sum(1 for p in out if p["estado"] == "hecho")
    actual = next((p for p in out if p["estado"] == "actual"), None)
    return {"pasos": out, "actual": actual, "hechos": hechos, "total": len(out),
            "avance": int(round(100.0 * hechos / len(out))) if out else 0,
            "pendiente": (actual["titulo"] if actual and not terminado else "")}


def marcar_a_mano(ruta_, clave):
    """¿Se puede marcar este paso a mano? Solo los de tipo «marcar», y no los que ya están hechos por el sistema."""
    return any(p["clave"] == clave and p["marcable"] for p in ruta_["pasos"])


def es_pendiente_de_gestion(fila):
    """Para la vista «Pendientes de reparación o repuesto»: abiertas y con una ruta de trabajo (no sin clasificar)."""
    return (_g(fila, "estado") in ESTADOS_ABIERTOS) and (_g(fila, "tipo_solicitud") in (T_REPUESTO, T_ABASTECER, T_TICKET, T_OT, T_REPARACION))


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  Permisos de acción y transiciones
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
def puede_accion(accion, fila, es_superadmin=False):
    """→ (ok, mensaje). Las acciones de gestión las filtra además la ruta (@_no_tecnico); aquí solo las reglas del estado.

    adjudicar · asignar · clasificar · editar · nota · paso · vincular · ticket · cerrar · anular · reabrir · borrar (solo superadmin)."""
    est = _g(fila, "estado") or EST_NUEVA
    if _g(fila, "borrada"):
        return False, "Esta inquietud fue eliminada."
    if accion == "borrar":
        return (True, "") if es_superadmin else (False, "Solo el superadministrador puede borrar una inquietud. Puedes anularla o cerrarla.")
    if accion == "nota":
        return True, ""
    if accion == "reabrir":
        return (True, "") if est in ESTADOS_TERMINADOS else (False, "Esta inquietud no está cerrada.")
    if est in ESTADOS_TERMINADOS:
        return False, "Esta inquietud ya está %s. Para seguir, reábrela con un motivo." % ESTADOS[est]["texto"].lower()
    if accion == "adjudicar" and _g(fila, "responsable_user_id"):
        return False, "Ya tiene responsable. Si quieres tomarla, pide cambiarlo con «Asignar»."
    if accion == "ticket" and _g(fila, "ticket_id"):
        return False, "Ya tiene un ticket levantado."
    return True, ""


def estado_tras_asignar(est):
    """Al tomarla o asignarla: nueva → en atención. Una escalada sigue escalada."""
    return EST_ATENCION if est == EST_NUEVA else est


def estado_tras_vinculo(est):
    """Al ligar un ticket u OT: queda escalada."""
    return EST_ESCALADA if est in (EST_NUEVA, EST_ATENCION) else est


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  Textos de la campana (sin plata, sin proveedores; tono amable)
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
def campana_nueva(iid, cliente, equipo, quien):
    return ("%s: inquietud nueva de %s" % (numero(iid), limpio(cliente, 80) or "un cliente"),
            "%s reportó una inquietud sobre %s. Tómala para que no se quede esperando." % (limpio(quien, 80) or "Alguien", limpio(equipo, 120) or "un equipo"))


def campana_asignada(iid, cliente, quien):
    return ("%s: ahora estás a cargo" % numero(iid),
            "%s te dejó a cargo de la inquietud de %s." % (limpio(quien, 80) or "La gestión", limpio(cliente, 80) or "un cliente"))


def campana_sla(iid, cliente, nivel):
    if nivel >= 2:
        return ("%s: plazo vencido" % numero(iid), "La inquietud de %s pasó su plazo de atención. Míralo cuando puedas." % (limpio(cliente, 80) or "un cliente"))
    return ("%s: cerca del plazo" % numero(iid), "La inquietud de %s está por cumplir su plazo de atención." % (limpio(cliente, 80) or "un cliente"))


PRIORIDAD_NIVEL = {0: "media", 1: "alta", 2: "urgente"}


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  Monitor: filtros y paginación
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
VISTAS = {"abiertas": "Abiertas", "sin_tomar": "Sin tomar", "pendientes": "Pendientes de repuesto o reparación",
          "vencidas": "Vencidas", "cerradas": "Cerradas", "todas": "Todas"}
POR_PAGINA = (10, 20, 50)


def pagina_valida(v, defecto=20):
    n = entero(v) or defecto
    return n if n in POR_PAGINA else defecto


def filtrar_vista(filas, vista, ahora, feriados=None, cfg=None):
    """Filtra una lista de filas ya leídas por la vista elegida (las vencidas dependen del reloj, por eso se hace aquí)."""
    cfg = cfg or config()
    def _vence(f):
        return reloj(f, ahora, feriados, cfg)["vencida"]
    if vista == "sin_tomar":
        return [f for f in filas if _g(f, "estado") == EST_NUEVA]
    if vista == "pendientes":
        return [f for f in filas if es_pendiente_de_gestion(f)]
    if vista == "vencidas":
        return [f for f in filas if _g(f, "estado") in ESTADOS_ABIERTOS and _vence(f)]
    if vista == "cerradas":
        return [f for f in filas if _g(f, "estado") in ESTADOS_TERMINADOS]
    if vista == "todas":
        return list(filas)
    return [f for f in filas if _g(f, "estado") in ESTADOS_ABIERTOS]


def resumen_estados(filas):
    """{estado: n} de una lista de filas (para las tarjetas de arriba del monitor)."""
    out = {k: 0 for k in ESTADOS}
    for f in filas:
        k = _g(f, "estado")
        if k in out:
            out[k] += 1
    return out

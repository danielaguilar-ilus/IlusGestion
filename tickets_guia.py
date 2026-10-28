"""Tickets 2.0 · Guía del ticket (Daniel 2026-10-27: «cuando entras a la solicitud de
retiro quiero algo parecido… con la actividad y todo»).

Mismo espíritu que `retiros_guia.py`: el servidor calcula, a partir de lo que YA
tiene el ticket, en qué va cada paso y cuál es el SIGUIENTE paso que le toca a la
persona. **El sistema sugiere, la persona decide (REGLA #32):** nada de aquí cambia
un estado ni le escribe a nadie; solo devuelve textos y las acciones que la ficha
dibuja como botones.

Módulo puro (sin base de datos ni Flask): recibe un dict plano y devuelve dicts.
Lo consume `tk_api_get` (tickets_module.py) y lo dibuja `static/tickets_ficha_v2.js`.

Estados de un paso:
    hecho      · ya está
    actual     · es lo que toca ahora (en ámbar suave en la ficha: alertar, no gritar)
    espera     · depende de un tercero (el cliente, el técnico de la OT)
    pendiente  · todavía no toca / falta un dato
    opcional   · no es obligatorio para avanzar
"""

TERMINALES = ("resolved", "closed", "cancelado")
OT_TERMINADA = ("completada", "cerrada", "cancelada", "anulada")

# Orden fijo de los pasos. La ficha los reconoce por CLAVE, nunca por posición.
PASOS = (
    ("cliente", "Identificación del cliente"),
    ("contactos", "Contactos"),
    ("problema", "Problema"),
    ("equipos", "Equipo(s)"),
    ("adjuntos", "Fotos y documentos"),
    ("responsable", "Responsable"),
    ("respuesta", "Respuesta al cliente"),
    ("motivo", "Motivo principal"),
    ("gestion", "Gestión: OT, cotización o repuesto"),
    ("diagnostico", "Diagnóstico de solución"),
    ("cierre", "Resolver y cerrar"),
)
OPCIONALES = ("adjuntos", "gestion")


def _lleno(v):
    return bool(str(v if v is not None else "").strip())


def _n(v):
    try:
        return int(v or 0)
    except (TypeError, ValueError):
        return 0


def _paso(clave, estado, resumen="", faltan=None, acciones=None):
    titulo = dict(PASOS)[clave]
    return {"clave": clave, "titulo": titulo, "estado": estado, "resumen": resumen,
            "faltan": list(faltan or []), "acciones": list(acciones or [])}


def evaluar(t):
    """`t` es un dict plano con lo que ya sabe la ficha:

    estado, asignado_a, titulo, descripcion, rut, empresa, nombre_contacto,
    motivo_principal, diagnostico_solucion, visita_id, visita_numero_ot,
    visita_estado, n_equipos, n_adjuntos, n_cotizaciones, n_repuestos,
    contactos_con_via (contactos con teléfono o correo), unread_count,
    sla (el bloque `sla_resp` de tickets_sla.evaluar o None),
    es_tecnico (True: no se le sugiere cotizar, REGLA #26).

    Devuelve {pasos, siguiente, progreso: {hechos, total, pct}, cerrado}.
    """
    t = t or {}
    estado = (t.get("estado") or "").strip().lower()
    cerrado = estado in TERMINALES
    sla = t.get("sla") or {}
    nivel_sla = (sla.get("nivel") or "").strip().lower()
    es_tecnico = bool(t.get("es_tecnico"))
    pasos = []

    # 1 · Cliente
    faltan = []
    if not (_lleno(t.get("empresa")) or _lleno(t.get("nombre_contacto"))):
        faltan.append("Falta el nombre del cliente o de quien escribió.")
    if not _lleno(t.get("rut")):
        faltan.append("Falta el RUT: sin RUT no se cruza con la ficha del cliente ni con Random.")
    pasos.append(_paso("cliente", "hecho" if not faltan else "pendiente",
                       (t.get("empresa") or t.get("nombre_contacto") or "") if not faltan else "",
                       faltan, [] if not faltan else ["editar_cliente"]))

    # 2 · Contactos
    con_via = _n(t.get("contactos_con_via"))
    pasos.append(_paso("contactos", "hecho" if con_via else "pendiente",
                       f"{con_via} con teléfono o correo" if con_via else "",
                       [] if con_via else ["Sin teléfono ni correo para contactar al cliente."],
                       [] if con_via else ["editar_cliente"]))

    # 3 · Problema
    hay_problema = _lleno(t.get("titulo")) or _lleno(t.get("descripcion"))
    pasos.append(_paso("problema", "hecho" if hay_problema else "pendiente",
                       "" , [] if hay_problema else ["Falta describir qué le pasa al cliente."]))

    # 4 · Equipos (recomendado: sin equipo la OT y la cotización nacen vacías)
    n_eq = _n(t.get("n_equipos"))
    pasos.append(_paso("equipos", "hecho" if n_eq else "pendiente",
                       f"{n_eq} equipo{'s' if n_eq != 1 else ''}" if n_eq else "",
                       [] if n_eq else ["Aún no hay equipos asociados: búscalos por documento, RUT o catálogo."],
                       [] if n_eq else ["buscar_productos"]))

    # 5 · Fotos y documentos (opcional)
    n_adj = _n(t.get("n_adjuntos"))
    pasos.append(_paso("adjuntos", "hecho" if n_adj else "opcional",
                       f"{n_adj} archivo{'s' if n_adj != 1 else ''}" if n_adj else "Opcional"))

    # 6 · Responsable
    resp = (t.get("asignado_a") or "").strip()
    pasos.append(_paso("responsable", "hecho" if resp else ("pendiente" if cerrado else "actual"),
                       resp, [] if resp else ["Nadie se ha hecho cargo de este ticket."],
                       [] if resp else ["tomar"]))

    # 7 · Respuesta al cliente (SLA de 4 h hábiles por mensaje del cliente)
    if cerrado:
        pasos.append(_paso("respuesta", "hecho", "Ticket cerrado"))
    elif nivel_sla in ("vencido", "atencion", "ok"):
        txt = sla.get("pill") or "El cliente espera respuesta"
        pasos.append(_paso("respuesta", "actual", txt,
                           [("SLA vencido: " if nivel_sla == "vencido" else "") + "El cliente espera una respuesta."
                            + ((" " + sla["sub"]) if sla.get("sub") else "")],
                           ["responder"]))
    else:
        pasos.append(_paso("respuesta", "hecho", sla.get("pill") or "Sin mensajes por responder"))

    # 8 · Motivo principal
    motivo = (t.get("motivo_principal") or "").strip()
    pasos.append(_paso("motivo", "hecho" if motivo else "pendiente", motivo,
                       [] if motivo else ["Falta el motivo principal del ticket (una línea: qué pasó de verdad)."],
                       [] if motivo else ["motivo"]))

    # 9 · Gestión: OT / cotización / repuesto (opcional: no todo ticket la necesita)
    visita_id = t.get("visita_id")
    ot_estado = (t.get("visita_estado") or "").strip().lower()
    n_cot = _n(t.get("n_cotizaciones"))
    n_rep = _n(t.get("n_repuestos"))
    partes = []
    if visita_id:
        partes.append(f"OT {t.get('visita_numero_ot') or ('#' + str(visita_id))}"
                      + (f" · {t.get('visita_estado_label') or ot_estado}" if ot_estado else ""))
    if n_cot and not es_tecnico:
        partes.append(f"{n_cot} cotización{'es' if n_cot != 1 else ''}")
    if n_rep:
        partes.append(f"{n_rep} repuesto{'s' if n_rep != 1 else ''} solicitado{'s' if n_rep != 1 else ''}")
    acc_gestion = ["generar_ot", "asociar_ot"] + ([] if es_tecnico else ["cotizar"]) + ["repuesto"]
    if visita_id and ot_estado and ot_estado not in OT_TERMINADA:
        pasos.append(_paso("gestion", "espera", " · ".join(partes),
                           ["La OT está en manos del técnico: el ticket avanza cuando la OT termine."],
                           ["ver_ot"]))
    elif partes:
        pasos.append(_paso("gestion", "hecho", " · ".join(partes), [], ["ver_ot"] if visita_id else []))
    else:
        pasos.append(_paso("gestion", "opcional",
                           "Sin OT ni repuesto" if es_tecnico else "Sin OT, cotización ni repuesto",
                           [], acc_gestion))

    # 10 · Diagnóstico de solución
    diag = (t.get("diagnostico_solucion") or "").strip()
    pasos.append(_paso("diagnostico", "hecho" if diag else "pendiente",
                       (diag[:120] + ("…" if len(diag) > 120 else "")) if diag else "",
                       [] if diag else ["Falta el diagnóstico de solución: qué se hizo y cómo quedó."],
                       [] if diag else ["diagnostico"]))

    # 11 · Resolver y cerrar
    if estado == "closed":
        pasos.append(_paso("cierre", "hecho", "Cerrado"))
    elif estado == "cancelado":
        pasos.append(_paso("cierre", "hecho", "Cancelado"))
    elif estado == "resolved":
        pasos.append(_paso("cierre", "hecho" if diag else "actual", "Resuelto" + ("" if diag else " · sin diagnóstico"),
                           [] if diag else ["Está resuelto pero sin diagnóstico de solución registrado."],
                           ["cerrar"] if diag else ["diagnostico"]))
    else:
        pasos.append(_paso("cierre", "pendiente", "", [], []))

    # ── Siguiente paso (el primero que aplica; una sola sugerencia, clara) ──
    siguiente = _siguiente(pasos, t, estado, nivel_sla, sla, es_tecnico)

    # Progreso: solo los pasos obligatorios
    obligatorios = [p for p in pasos if p["clave"] not in OPCIONALES]
    hechos = sum(1 for p in obligatorios if p["estado"] == "hecho")
    total = len(obligatorios)
    return {"pasos": pasos, "siguiente": siguiente, "cerrado": cerrado,
            "progreso": {"hechos": hechos, "total": total,
                         "pct": int(round(100.0 * hechos / total)) if total else 0}}


def _sig(clave, titulo, texto, acciones, tono="actual"):
    return {"clave": clave, "titulo": titulo, "texto": texto, "acciones": list(acciones), "tono": tono}


def _siguiente(pasos, t, estado, nivel_sla, sla, es_tecnico):
    por = {p["clave"]: p for p in pasos}
    diag = _lleno(t.get("diagnostico_solucion"))
    if estado == "closed":
        return _sig("cierre", "Ticket cerrado", "No queda nada por hacer. Si el cliente vuelve a escribir, se reabre.", [], "fin")
    if estado == "cancelado":
        return _sig("cierre", "Ticket cancelado", "Quedó cancelado. Si fue un error, cámbiale el estado.", [], "fin")
    if estado == "resolved":
        if not diag:
            return _sig("diagnostico", "Registra el diagnóstico de solución",
                        "Está resuelto, pero no quedó escrito qué se hizo. Déjalo antes de cerrar.", ["diagnostico"])
        return _sig("cierre", "Cerrar el ticket", "Está resuelto y con diagnóstico. Si el cliente quedó conforme, ciérralo.", ["cerrar"], "fin")
    if por["responsable"]["estado"] != "hecho":
        return _sig("responsable", "Hazte cargo del ticket",
                    "Nadie lo tiene. Con responsable corre el SLA a su nombre y le llegan los avisos.", ["tomar"])
    if nivel_sla in ("vencido", "atencion", "ok"):
        pill = sla.get("pill") or ""
        return _sig("respuesta", "Responder al cliente" + (" · SLA vencido" if nivel_sla == "vencido" else ""),
                    (pill + ". " if pill else "") + "Cada mensaje del cliente se responde en 4 horas hábiles.",
                    ["responder"], "actual" if nivel_sla != "vencido" else "vencido")
    if por["cliente"]["estado"] != "hecho":
        return _sig("cliente", "Completa la identificación del cliente", " ".join(por["cliente"]["faltan"]), ["editar_cliente"])
    if por["problema"]["estado"] != "hecho":
        return _sig("problema", "Describe el problema", "Sin descripción nadie sabe qué atender.", [])
    if por["motivo"]["estado"] != "hecho":
        return _sig("motivo", "Define el motivo principal",
                    "Una línea con la causa real (ej.: «Banda de trotadora desgastada»). Ordena la bandeja y los reportes.", ["motivo"])
    if por["equipos"]["estado"] != "hecho":
        return _sig("equipos", "Asocia el o los equipos",
                    "Sin equipo, la OT y la cotización nacen vacías. Búscalos por documento de Random, RUT o catálogo.",
                    ["buscar_productos"], "sugerencia")
    g = por["gestion"]
    if g["estado"] == "espera":
        return _sig("gestion", f"Seguir la OT {t.get('visita_numero_ot') or ''}".strip(),
                    "La OT está en curso. El ticket avanza cuando el técnico la termine y se apruebe el cierre.", ["ver_ot"], "espera")
    if g["estado"] == "opcional":
        return _sig("gestion", "Decide cómo se resuelve",
                    "Genera una OT para que vaya un técnico, cotiza el servicio o pide un repuesto. Si se resolvió por correo, salta al diagnóstico.",
                    ["generar_ot", "asociar_ot"] + ([] if es_tecnico else ["cotizar"]) + ["repuesto", "diagnostico"], "sugerencia")
    if not diag:
        return _sig("diagnostico", "Registra el diagnóstico de solución",
                    "La gestión terminó: deja escrito qué se hizo y cómo quedó el equipo.", ["diagnostico"])
    return _sig("cierre", "Marca el ticket como resuelto",
                "Tiene responsable, motivo, gestión y diagnóstico. Si el cliente quedó conforme, resuélvelo.", ["resolver"], "fin")

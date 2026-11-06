# -*- coding: utf-8 -*-
"""Cabecera «enchulada» de la ficha del cliente (5-nov-2026, Daniel: «todo debe trabajar con la ficha de cliente, todo se centraliza ahí»).

Módulo PURO (sin base, sin Flask, sin relojes: la fecha de hoy llega por parámetro): decide el SEMÁFORO del cliente, sus MOTIVOS a la vista y las
tarjetas de KPI de la cabecera. `app.py` (`_mant_ficha_impl`) le pasa lo que ya calcula la ficha más un solo conteo de OT abiertas.

Reglas de diseño (todas con prueba en tests/test_clientes_cabecera.py):
  · El semáforo nunca es un misterio: cada color trae su MOTIVO escrito y a la vista (REGLA #15: datos completos, nada escondido en un tooltip).
  · Rojo solo para lo que de verdad pide acción hoy: OT atrasadas y contrato vigente ya vencido. El resto es ámbar. «Sin ejecutivo» y «cliente
    inactivo» son solo información (gris): no cambian el color (si no, casi todos los clientes saldrían en ámbar).
  · Ningún KPI lleva plata (REGLAS #19 y #26: la misma cabecera la ve un técnico).
  · Fechas siempre día/mes/año (REGLA #6).
  · Si una fuente falla (None), el KPI dice «—» y no inventa un número ni pinta un color.
"""
import datetime as _dt

OT_CERRADAS = ("cerrada", "cancelada", "anulada")            # lo que NO es una OT abierta (igual que el panel de OT)
OT_ATRASABLES = ("programada", "en_curso", "reagendada")     # una OT en estos estados con fecha pasada está atrasada
FECHA_SIN_FECHA = _dt.date(2099, 12, 31)                      # la fecha centinela de la OT «pendiente» sin fecha (REGLA #35)
SIN_VISITA_DIAS = 90
CONTRATO_AVISO_DIAS = 60
CONTENEDOR = "contenedor de documentos"                       # no es un contrato de verdad: guarda documentos sueltos

NIVEL_OK, NIVEL_AVISO, NIVEL_ALERTA = "ok", "aviso", "alerta"
TITULOS = {NIVEL_OK: "Al día", NIVEL_AVISO: "Hay cosas por revisar", NIVEL_ALERTA: "Requiere atención"}
ACCIONES_VALIDAS = ("", "editar", "tab:visitas", "tab:contrato", "tab:equipos", "tab:tickets", "tab:comunicaciones")


def fecha(v):
    """date | datetime | 'AAAA-MM-DD…' → date (o None)."""
    if isinstance(v, _dt.datetime):
        return v.date()
    if isinstance(v, _dt.date):
        return v
    s = str(v or "").strip()[:10]
    try:
        return _dt.date.fromisoformat(s) if s else None
    except ValueError:
        return None


def fecha_txt(v):
    """dd/mm/aaaa (REGLA #6) o '' si no hay fecha."""
    f = fecha(v)
    return f.strftime("%d/%m/%Y") if f else ""


def _plural(n, uno, varios):
    return f"{n} {uno if n == 1 else varios}"


def contrato_vivo(contratos, hoy):
    """(estado, dias, nombre) del contrato que manda hoy.

    estado: 'sin' (el cliente no tiene contratos), 'ninguno' (tuvo, pero ninguno sigue vivo), 'indefinido', 'vigente', 'por_vencer' (vence en
    ≤ 60 días) o 'vencido' (el contrato sigue marcado vigente pero su fecha ya pasó). Se ignora el «Contenedor de documentos»."""
    reales = [c for c in (contratos or []) if str(c.get("nombre") or "").strip().lower() != CONTENEDOR]
    if not reales:
        return "sin", None, ""
    vivos = [c for c in reales if str(c.get("estado") or "").strip().lower() in ("vigente", "por_vencer", "indefinido")]
    if not vivos:
        return "ninguno", None, ""
    # el que más tarde vence es el que manda (un contrato nuevo reemplaza al viejo que quedó marcado)
    mejor, mejor_f = None, None
    for c in vivos:
        indef = bool(c.get("es_indefinido")) or str(c.get("estado") or "").lower() == "indefinido"
        f = None if indef else fecha(c.get("fecha_vencimiento"))
        if indef:
            return "indefinido", None, str(c.get("nombre") or "")
        if f is None:
            return "vigente", None, str(c.get("nombre") or "")
        if mejor_f is None or f > mejor_f:
            mejor, mejor_f = c, f
    dias = (mejor_f - hoy).days
    nombre = str(mejor.get("nombre") or "")
    if dias < 0:
        return "vencido", dias, nombre
    if dias <= CONTRATO_AVISO_DIAS:
        return "por_vencer", dias, nombre
    return "vigente", dias, nombre


def armar(*, hoy, cliente, contratos=None, ot_abiertas=None, ot_atrasadas=None, equipos_activos=0, equipos_total=0,
          equipos_con_problema=0, ult_visita=None, prox_visita=None, tiene_correo=True):
    """Devuelve {semaforo: {nivel, titulo, motivos:[{nivel, texto, accion}]}, kpis:[...], info:[...]}.

    `ot_abiertas` / `ot_atrasadas` en None = la consulta falló (se muestra «—» y no se pinta nada con ese dato)."""
    hoy = fecha(hoy) or _dt.date.today()
    cliente = cliente or {}
    motivos, info = [], []

    # ── OT ──
    if ot_atrasadas:
        motivos.append({"nivel": NIVEL_ALERTA, "accion": "tab:visitas",
                        "texto": f"{_plural(ot_atrasadas, 'OT atrasada', 'OT atrasadas')} (fecha vencida y sin terminar)"})
    # ── contrato ──
    c_estado, c_dias, c_nombre = contrato_vivo(contratos, hoy)
    if c_estado == "vencido":
        motivos.append({"nivel": NIVEL_ALERTA, "accion": "tab:contrato",
                        "texto": f"El contrato venció hace {_plural(abs(c_dias), 'día', 'días')}" + (f" ({c_nombre})" if c_nombre else "")})
    elif c_estado == "por_vencer":
        motivos.append({"nivel": NIVEL_AVISO, "accion": "tab:contrato",
                        "texto": ("El contrato vence hoy" if c_dias == 0 else f"El contrato vence en {_plural(c_dias, 'día', 'días')}")
                                 + (f" ({c_nombre})" if c_nombre else "")})
    # ── visitas ──
    uv, pv = fecha(ult_visita), fecha(prox_visita)
    if equipos_activos and (uv is None or (hoy - uv).days > SIN_VISITA_DIAS) and not pv:
        motivos.append({"nivel": NIVEL_AVISO, "accion": "tab:visitas",
                        "texto": ("Ninguna visita realizada todavía" if uv is None else f"Sin visita hace {_plural((hoy - uv).days, 'día', 'días')}")
                                 + " y nada agendado"})
    # ── equipos ──
    if equipos_con_problema:
        motivos.append({"nivel": NIVEL_AVISO, "accion": "tab:equipos",
                        "texto": f"{_plural(equipos_con_problema, 'equipo con problema', 'equipos con problema')} (fuera de servicio, con advertencia o repuesto pendiente)"})
    # ── contacto ──
    if not tiene_correo:
        motivos.append({"nivel": NIVEL_AVISO, "accion": "editar",
                        "texto": "Sin correo registrado: no recibirá los avisos automáticos (OT creada, recordatorio, OT terminada)"})
    # ── solo información (no cambia el color) ──
    if not (cliente.get("ejecutivo_nombre") or "").strip():
        info.append({"nivel": "info", "accion": "editar", "texto": "Sin ejecutivo asignado"})
    if str(cliente.get("estado") or "").strip().lower() == "inactivo":
        info.append({"nivel": "info", "accion": "editar", "texto": "Cliente inactivo"})
    if c_estado == "ninguno":
        info.append({"nivel": "info", "accion": "tab:contrato", "texto": "Tuvo contratos, pero ninguno está vigente"})

    nivel = NIVEL_ALERTA if any(m["nivel"] == NIVEL_ALERTA for m in motivos) else (NIVEL_AVISO if motivos else NIVEL_OK)
    # rojo primero, luego ámbar
    motivos.sort(key=lambda m: 0 if m["nivel"] == NIVEL_ALERTA else 1)

    # ── KPIs (sin plata) ──
    def _kpi(clave, etiqueta, valor, detalle, tono, accion):
        return {"clave": clave, "etiqueta": etiqueta, "valor": valor, "detalle": detalle, "tono": tono, "accion": accion}

    if ot_abiertas is None:
        k_ot = _kpi("ot", "OT abiertas", "—", "no se pudo leer", "neutro", "tab:visitas")
    else:
        det = f"{ot_atrasadas} atrasada{'s' if ot_atrasadas != 1 else ''}" if ot_atrasadas else ("ninguna atrasada" if ot_abiertas else "sin trabajo pendiente")
        k_ot = _kpi("ot", "OT abiertas", str(ot_abiertas), det, NIVEL_ALERTA if ot_atrasadas else NIVEL_OK, "tab:visitas")
    k_tk = _kpi("tickets", "Tickets abiertos", "—", "leyendo…", "neutro", "tab:tickets")   # lo completa el navegador (el mismo endpoint de la pestaña)
    k_eq = _kpi("equipos", "Equipos activos", str(equipos_activos),
                (f"{equipos_con_problema} con problema" if equipos_con_problema else f"de {equipos_total} registrados"),
                NIVEL_AVISO if equipos_con_problema else NIVEL_OK, "tab:equipos")
    if pv:
        d = (pv - hoy).days
        k_pv = _kpi("proxima", "Próxima visita", fecha_txt(pv),
                    "hoy" if d == 0 else (f"en {_plural(d, 'día', 'días')}" if d > 0 else f"hace {_plural(-d, 'día', 'días')} (sin reagendar)"),
                    NIVEL_AVISO if d < 0 else NIVEL_OK, "tab:visitas")
    else:
        k_pv = _kpi("proxima", "Próxima visita", "Sin agendar", "nada programado", NIVEL_AVISO if equipos_activos else "neutro", "tab:visitas")
    if uv:
        d = (hoy - uv).days
        k_uv = _kpi("ultima", "Última visita", fecha_txt(uv), "hoy" if d == 0 else f"hace {_plural(d, 'día', 'días')}",
                    NIVEL_AVISO if (d > SIN_VISITA_DIAS and equipos_activos) else NIVEL_OK, "tab:visitas")
    else:
        k_uv = _kpi("ultima", "Última visita", "Ninguna", "todavía no se le visita", "neutro", "tab:visitas")

    kpis = [k_ot, k_tk, k_eq, k_pv, k_uv]
    for it in motivos + info + kpis:
        it["js"] = accion_js(it.get("accion") or "")
    return {"semaforo": {"nivel": nivel, "titulo": TITULOS[nivel], "motivos": motivos},
            "info": info, "kpis": kpis, "contrato": {"estado": c_estado, "dias": c_dias, "nombre": c_nombre}}


def accion_js(accion):
    """El `onclick` de un chip o tarjeta a partir de su `accion` (lista cerrada: nada del navegador ni de la base llega al HTML)."""
    if accion == "editar":
        return "abrirEditarCliente()"
    if accion in ACCIONES_VALIDAS and accion.startswith("tab:"):
        t = accion[4:]
        return f"switchTab('{t}')" + (";cargarTicketsFicha()" if t == "tickets" else ";cargarComunicaciones()" if t == "comunicaciones" else "")
    return ""

"""Monitor de Retiros (/retiros, pestaña Monitor): datos derivados para la tabla
rica y las tarjetas de KPI estilo Power BI.

Módulo PURO: sin Flask ni base de datos, para poder probarlo solo. Las
funciones que dependen del reloj hábil y de la zona horaria de pickups_module
(horas_habiles, utc_a_chile, td_hhmm) se reciben por parámetro, así hay una sola
definición de SLA en todo el proyecto (la del Centro de control).

Solo datos reales: nada estimado ni inventado (ilus_design_system).
Fechas: created_at viene en UTC; las fechas y horas de agenda ya están en hora Chile.
"""
import re
import unicodedata
from datetime import date, datetime, timedelta

DIAS_CORTOS = ("lun", "mar", "mié", "jue", "vie", "sáb", "dom")

POR_RESPONDER = ("solicitud_recibida", "en_revision", "informacion_incompleta")
ESPERA_CLIENTE = ("propuesta_enviada", "esperando_cliente")
AGENDADOS = ("agenda_confirmada", "reagendada", "en_preparacion")
COMPLETADOS = ("retirada", "cerrada")
NEGATIVOS = ("rechazada", "fallida")

# Qué le toca hacer a ILUS en cada estado (mismos textos que la ficha del retiro).
SIGUIENTE = {
    "solicitud_recibida": "Siguiente: proponer fecha al cliente",
    "en_revision": "Siguiente: proponer fecha al cliente",
    "informacion_incompleta": "Siguiente: completar la información",
    "propuesta_enviada": "Siguiente: que el cliente acepte la propuesta",
    "esperando_cliente": "Siguiente: que el cliente acepte la propuesta",
    "agenda_confirmada": "Siguiente: enviar a preparación",
    "reagendada": "Siguiente: enviar a preparación",
    "en_preparacion": "Siguiente: entregar al cliente",
    "retirada": "Retiro completado",
    "cerrada": "Retiro completado",
    "rechazada": "El cliente rechazó el retiro",
    "fallida": "El retiro no se concretó",
}

# Movimientos del historial que se entienden a simple vista. El resto
# (correos pendientes, ERP, líneas) es ruido técnico y no se muestra.
EVENTOS = {
    "creada": "Solicitud creada",
    "retiro_interno_creado": "Retiro interno creado",
    "propuesta_enviada": "Propuesta enviada al cliente",
    "propuesta_vencida": "La propuesta venció",
    "cliente_confirmo": "El cliente confirmó",
    "cliente_rechazo": "El cliente rechazó",
    "cliente_contrapropuso": "El cliente propuso otra fecha",
    "auto_confirmada_coincidencia": "Confirmada automáticamente",
    "agenda_reprogramada": "Agenda reprogramada",
    "doc_validada": "Documento validado",
    "doc_validacion_auto": "Documento validado",
    "doc_incompleta": "Documento incompleto",
    "picking_completo": "Preparación completa",
    "retiro_evidencia": "Evidencia de retiro registrada",
    "retiro_evidencia_foto": "Evidencia de retiro registrada",
    "email_enviado": "Correo enviado al cliente",
    "whatsapp_enviado": "WhatsApp enviado",
    "mensaje_enviado": "Mensaje enviado",
}


def norm(texto):
    """Minúsculas y sin tildes: para buscar 'jose' y encontrar 'José'."""
    s = unicodedata.normalize("NFD", str(texto or "").lower())
    return "".join(c for c in s if unicodedata.category(c) != "Mn").strip()


def _solo_alfanum(v):
    return re.sub(r"[^0-9a-zA-Z]", "", str(v or "")).lower()


def _a_fecha(v):
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    if v:
        try:
            return date.fromisoformat(str(v)[:10])
        except ValueError:
            return None
    return None


def _hhmm_a_min(s):
    try:
        h, m = str(s).split(":")[:2]
        return int(h) * 60 + int(m)
    except (ValueError, TypeError):
        return None


def iniciales(nombre):
    partes = [p for p in re.split(r"\s+", str(nombre or "").strip()) if p]
    return "".join(p[0] for p in partes[:2]).upper()


def plural(n, singular, plural_):
    return f"{n} {singular if n == 1 else plural_}"


def fmt_horas_habiles(h):
    if h is None:
        return ""
    if h < 1:
        return "menos de 1 h hábil"
    n = int(round(h))
    return f"{n} h hábil" if n == 1 else f"{n} h hábiles"


def relativo(dias):
    if dias == 0:
        return "Hoy"
    if dias == 1:
        return "Mañana"
    if dias == -1:
        return "Ayer"
    return f"En {dias} días" if dias > 1 else f"Hace {-dias} días"


def hace(delta):
    seg = max(int(delta.total_seconds()), 0)
    if seg < 60:
        return "hace un momento"
    if seg < 3600:
        return f"hace {seg // 60} min"
    if seg < 86400:
        return f"hace {seg // 3600} h"
    if seg < 86400 * 30:
        return f"hace {seg // 86400} d"
    return ""


def fecha_efectiva(r):
    """Fecha que manda: confirmada > propuesta > solicitada.
    Devuelve (tipo, fecha, desde, hasta); todo None si no hay ninguna."""
    for tipo, pref in (("Confirmada", "confirmed"), ("Propuesta", "proposed"), ("Solicitada", "requested")):
        d = _a_fecha(r.get(f"{pref}_date"))
        if d:
            return tipo, d, r.get(f"{pref}_time_from"), r.get(f"{pref}_time_to")
    return None, None, None, None


def plazo_sla(creado, horas, horas_habiles, feriados=()):
    """Momento (hora Chile) en que una solicitud cumple `horas` hábiles sin respuesta.
    Búsqueda binaria sobre la misma función de horas hábiles del Centro de control:
    así no hay una segunda definición del horario (lun-vie 09-18, sin feriados)."""
    lo, hi = creado, creado + timedelta(days=21)
    if horas_habiles(creado, hi, feriados) < horas:
        return None
    for _ in range(22):   # 21 días / 2^22 ≈ medio segundo de precisión
        medio = lo + (hi - lo) / 2
        if horas_habiles(creado, medio, feriados) >= horas:
            hi = medio
        else:
            lo = medio
    # horas_habiles redondea a centésimas (36 s): se lleva al minuto más cercano.
    return (hi + timedelta(seconds=30)).replace(second=0, microsecond=0)


def fmt_plazo(plazo, hoy):
    if not plazo:
        return ""
    hhmm = plazo.strftime("%H:%M")
    if plazo.date() == hoy:
        return f"las {hhmm}"
    if plazo.date() == hoy + timedelta(days=1):
        return f"mañana {hhmm}"
    return f"el {DIAS_CORTOS[plazo.weekday()]} {plazo.strftime('%d-%m')} {hhmm}"


def _alerta(st, dias, ini_min, ahora_min, horas_espera, sla_ambar, sla_rojo):
    """Semáforo de la fila: (nivel, texto, icono). Rojo = hay que actuar ya."""
    if st in POR_RESPONDER:
        nivel = "rojo" if horas_espera >= sla_rojo else ("ambar" if horas_espera >= sla_ambar else "verde")
        ico = {"rojo": "bi-exclamation-octagon-fill", "ambar": "bi-exclamation-triangle-fill",
               "verde": "bi-check-circle-fill"}[nivel]
        return nivel, f"Sin responder · {fmt_horas_habiles(horas_espera)}", ico
    if st in ESPERA_CLIENTE:
        return "gris", "Esperando al cliente", "bi-hourglass-split"
    if st in AGENDADOS:
        if dias is None:
            return "gris", "Sin fecha", "bi-question-circle-fill"
        prep = " · en preparación" if st == "en_preparacion" else ""
        if dias < 0:
            return "rojo", f"Fecha vencida hace {-dias} d{prep}", "bi-exclamation-octagon-fill"
        if dias == 0:
            if ini_min is not None and ahora_min > ini_min + 30:
                return "rojo", f"Atrasado: pasó su horario{prep}", "bi-exclamation-octagon-fill"
            return "ambar", f"Es hoy{prep}", "bi-clock-fill"
        return "verde", f"En orden{prep}", "bi-check-circle-fill"
    if st == "retirada":
        return "verde", "Retirado", "bi-check-circle-fill"
    if st == "cerrada":
        return "gris", "Cerrado", "bi-lock-fill"
    if st in NEGATIVOS:
        return "gris", "Rechazada" if st == "rechazada" else "No se concretó", "bi-x-circle-fill"
    return "gris", "", "bi-dash-circle"


def _timeline(logs, estados, utc_a_chile):
    salida = []
    for lg in logs or []:
        accion = lg.get("action") or ""
        txt = EVENTOS.get(accion)
        if accion == "estado_actualizado":
            nuevo = lg.get("new_status")
            txt = f"Estado: {estados.get(nuevo, nuevo)}" if nuevo else None
        if not txt:
            continue
        cuando = utc_a_chile(lg.get("created_at"))
        salida.append({
            "cuando": cuando.strftime("%d-%m-%Y %H:%M") if cuando else "",
            "txt": txt,
            "quien": lg.get("actor_name") or "",
        })
        if len(salida) == 4:
            break
    return salida


def enriquecer_filas(rows, *, hoy, ahora, horas_habiles, utc_a_chile, td_hhmm, estados, grupos,
                     relaciones, feriados=(), sla_ambar=2.0, sla_rojo=4.0, logs=None):
    """Agrega a cada fila (dict) las claves `m_*` que usa la tabla del Monitor.
    No cambia ninguna clave existente."""
    logs = logs or {}
    grupo_de = {}
    for g in grupos:
        for s in g["statuses"]:
            grupo_de[s] = g
    orden_estado = {k: i for i, k in enumerate(estados)}
    ahora_min = ahora.hour * 60 + ahora.minute
    lunes = hoy - timedelta(days=hoy.weekday())
    domingo = lunes + timedelta(days=6)

    for r in rows:
        st = r.get("status") or ""
        g = grupo_de.get(st) or {"key": "otros", "label": "Otros", "icon": "bi-dash-circle"}
        creado = utc_a_chile(r.get("created_at"))
        origen = "Web" if (r.get("request_source") or "web") == "web" else "Interno"

        tipo, fch, t_desde, t_hasta = fecha_efectiva(r)
        dias = (fch - hoy).days if fch else None
        desde, hasta = (td_hhmm(t_desde) if t_desde else ""), (td_hhmm(t_hasta) if t_hasta else "")
        ini_min = _hhmm_a_min(desde) if desde else None
        activo = st not in COMPLETADOS and st not in NEGATIVOS

        horas_espera = 0.0
        if st in POR_RESPONDER and creado:
            horas_espera = horas_habiles(creado, ahora, feriados)
        nivel, al_txt, al_ico = _alerta(st, dias, ini_min, ahora_min, horas_espera, sla_ambar, sla_rojo)
        # Reloj en vivo del "Sin responder" (Daniel 2026-09-29): tiempo hábil ya
        # transcurrido + hora límite; static/retiros_monitor.js lo hace avanzar.
        reloj = None
        if st in POR_RESPONDER and creado:
            reloj = {"espera_s": int(horas_espera * 3600),
                     "pct": min(100, int(horas_espera / sla_rojo * 100)) if sla_rojo else 100,
                     "plazo_txt": fmt_plazo(plazo_sla(creado, sla_rojo, horas_habiles, feriados), hoy)}

        if fch and activo:
            rel_nivel = "rojo" if dias < 0 else ("ambar" if dias == 0 else "verde")
            rel_txt = relativo(dias)
        else:
            rel_nivel, rel_txt = "gris", ""

        bultos = int(r.get("total_packages") or 0)
        kg = float(r.get("total_weight_kg") or 0)
        pv = float(r.get("total_volumetric_weight") or 0)
        m3 = float(r.get("total_volume_m3") or 0)
        cal = int(r.get("information_quality_score") or 0)
        # Los retiros internos nunca se puntúan (quedan en 0): mostrar 0 % en rojo
        # parecería un error del retiro y no lo es.
        cal_na = cal == 0 and (r.get("request_source") or "web") != "web"

        dv = (r.get("doc_validation_status") or "pendiente")
        if r.get("document_number"):
            doc_val = {"ok": ("verde", "Validado"), "incompleto": ("ambar", "Incompleto")}.get(dv, ("gris", "Por validar"))
        else:
            doc_val = ("gris", "")

        resp = (r.get("responsable_nombre") or "").strip()
        relacion = relaciones.get(r.get("pickup_person_relation") or "", "")
        ntel = re.sub(r"\D", "", str(r.get("contact_phone") or ""))
        ptel = re.sub(r"\D", "", str(r.get("pickup_person_phone") or ""))

        r["m_grupo"], r["m_grupo_lbl"], r["m_grupo_ico"] = g["key"], g["label"], g.get("icon", "")
        r["m_estado_idx"] = orden_estado.get(st, 99)
        r["m_origen"] = origen
        r["m_creado_txt"] = creado.strftime("%d-%m-%Y %H:%M") if creado else ""
        r["m_creado_ts"] = int((creado - datetime(1970, 1, 1)).total_seconds()) if creado else 0
        r["m_hace"] = hace(ahora - creado) if creado else ""
        r["m_fecha_tipo"] = tipo or ""
        r["m_fecha"] = fch
        r["m_fecha_iso"] = fch.isoformat() if fch else ""
        r["m_fecha_txt"] = f"{DIAS_CORTOS[fch.weekday()]} {fch.strftime('%d-%m-%Y')}" if fch else ""
        r["m_desde"], r["m_hasta"] = desde, hasta
        r["m_dias"] = dias
        r["m_rel_txt"], r["m_rel_nivel"] = rel_txt, rel_nivel
        # Mismas reglas que las tarjetas del dashboard: "esta semana" cuenta por fecha
        # confirmada o, si no hay, solicitada (no la propuesta) y excluye rechazadas y
        # fallidas; "hoy" es solicitada o confirmada = hoy. Así el número de la tarjeta
        # y las filas que muestra el clic coinciden.
        f_conf, f_req = _a_fecha(r.get("confirmed_date")), _a_fecha(r.get("requested_date"))
        f_kpi = f_conf or f_req
        r["m_req_iso"] = f_req.isoformat() if f_req else ""
        r["m_conf_iso"] = f_conf.isoformat() if f_conf else ""
        r["m_en_semana"] = bool(f_kpi and lunes <= f_kpi <= domingo and st not in NEGATIVOS)
        r["m_al_nivel"], r["m_al_txt"], r["m_al_ico"] = nivel, al_txt, al_ico
        r["m_reloj"] = reloj
        r["m_vencida"] = bool(activo and nivel == "rojo" and st in AGENDADOS)
        r["m_siguiente"] = SIGUIENTE.get(st, "") if st not in POR_RESPONDER or r.get("document_number") \
            else "Siguiente: agregar la factura o boleta"
        r["m_resp_ini"] = iniciales(resp)
        r["m_resp_norm"] = norm(resp)                        # valor del filtro "Responsable" y de la fila
        r["m_cliente_norm"] = norm(r.get("customer_name"))   # para ordenar por cliente sin que las tildes molesten
        r["m_relacion"] = relacion
        r["m_doc_val_nivel"], r["m_doc_val_txt"] = doc_val
        r["m_carga_txt"] = plural(bultos, "bulto", "bultos")
        r["m_sin_peso"] = kg == 0 and pv == 0
        r["m_cal_na"] = cal_na
        r["m_cal"] = cal
        r["m_timeline"] = _timeline(logs.get(int(r["id"])) if r.get("id") is not None else None,
                                    estados, utc_a_chile)
        r["m_search"] = " ".join(filter(None, (
            norm(r.get("code")), norm(r.get("customer_name")), _solo_alfanum(r.get("customer_rut")),
            norm(r.get("contact_name")), ntel, norm(r.get("contact_email")),
            norm(r.get("document_type")), _solo_alfanum(r.get("document_number")),
            norm(r.get("pickup_person_name")), _solo_alfanum(r.get("pickup_person_rut")), ptel,
            norm(resp), norm(r.get("created_by_user_name")), norm(estados.get(st, st)),
            norm(g["label"]), norm(origen), norm(al_txt))))
        r["m_csv"] = {
            "Solicitud": r.get("code") or "", "Estado": estados.get(st, st), "Cliente": r.get("customer_name") or "",
            "RUT cliente": r.get("customer_rut") or "", "Contacto": r.get("contact_name") or "",
            "Teléfono": r.get("contact_phone") or "", "Correo": r.get("contact_email") or "",
            "Documento": (f"{str(r.get('document_type') or '').upper()} {r.get('document_number')}".strip()
                          if r.get("document_number") else ""),
            "Quién retira": r.get("pickup_person_name") or "", "RUT quien retira": r.get("pickup_person_rut") or "",
            "Responsable": resp, "Fecha de retiro": fch.strftime("%d-%m-%Y") if fch else "",
            "Horario": f"{desde}-{hasta}" if desde and hasta else "", "Bultos": bultos, "Peso kg": round(kg, 1),
            "Peso volumétrico": round(pv, 1), "Volumen m3": round(m3, 3), "Semáforo": al_txt,
            "Creada": r["m_creado_txt"], "Canal": origen,
        }
    return rows


def armar_datos(rows, fmt_rut):
    """Datos que cada retiro despliega al abrir su detalle, más su fila para exportar.

    Se entregan UNA sola vez como JSON (no como HTML repetido por fila) y sin repetir
    campos: el panel de detalle se arma en el navegador con lo mismo que trae la
    exportación ("csv") más unos pocos datos extra ("x"). Así la página no engorda
    con 250 retiros. `fmt_rut` da formato chileno (12.345.678-5): se inyecta el mismo
    filtro `rut_fmt` de las plantillas."""
    datos = {}
    for r in rows:
        if "m_csv" not in r or r.get("id") is None:
            continue
        csv = dict(r["m_csv"])
        for k, campo in (("RUT cliente", "customer_rut"), ("RUT quien retira", "pickup_person_rut")):
            csv[k] = fmt_rut(r.get(campo)) if r.get(campo) else ""
        csv["Etapa"] = r["m_grupo_lbl"]
        datos[str(r["id"])] = {
            "csv": csv,
            "x": {"rel": r["m_relacion"], "ptel": r.get("pickup_person_phone") or "",
                  "dv": r["m_doc_val_txt"], "sp": r["m_sin_peso"],
                  "min": int(r.get("tiempo_estimado_min") or 0),
                  "por": r.get("created_by_user_name") or "", "tl": r["m_timeline"]},
        }
    return datos


# ── Tarjetas de KPI ─────────────────────────────────────────────────────────

def delta(actual, previo, *, menor_es_mejor=False, unidad="", decimales=0):
    """Variación contra el período anterior. None si falta alguno de los dos datos.
    bueno: True/False según el sentido del KPI; None si no cambió."""
    if actual is None or previo is None:
        return None
    dif = actual - previo
    if abs(dif) < (0.5 * 10 ** -decimales):
        return {"dir": "flat", "txt": "sin cambio", "bueno": None}
    direccion = "up" if dif > 0 else "down"
    bueno = (dif < 0) if menor_es_mejor else (dif > 0)
    signo = "+" if dif > 0 else "−"
    valor = f"{abs(dif):.{decimales}f}".replace(".", ",") if decimales else f"{abs(dif):.0f}"
    return {"dir": direccion, "txt": f"{signo}{valor}{unidad}", "bueno": bueno}


def spark(serie, ancho=132, alto=34, pad=4):
    """Puntos de una mini línea (SVG). None si hay menos de 2 valores reales."""
    vals = [float(v) for v in (serie or []) if v is not None]
    if len(vals) < 2:
        return None
    lo, hi = min(vals), max(vals)
    paso = (ancho - 2 * pad) / (len(vals) - 1)
    pts = []
    for i, v in enumerate(vals):
        y = alto / 2 if hi == lo else pad + (alto - 2 * pad) * (1 - (v - lo) / (hi - lo))
        pts.append((round(pad + i * paso, 1), round(y, 1)))
    linea = " ".join(f"{x},{y}" for x, y in pts)
    return {"linea": linea, "area": f"{pts[0][0]},{alto} {linea} {pts[-1][0]},{alto}",
            "ult_x": pts[-1][0], "ult_y": pts[-1][1], "ancho": ancho, "alto": alto}


def barras_semana(por_fecha, lunes, hoy, alto=34):
    """Siete barras (lun-dom) con los retiros de cada día de la semana."""
    conteos = [int(por_fecha.get((lunes + timedelta(days=i)).isoformat(), 0) or 0) for i in range(7)]
    maximo = max(conteos + [1])
    barras = []
    for i, n in enumerate(conteos):
        d = lunes + timedelta(days=i)
        barras.append({
            "fecha": d.isoformat(), "dia": DIAS_CORTOS[i], "n": n, "hoy": d == hoy, "futuro": d > hoy,
            "h": max(3, round(alto * n / maximo)) if n else 3, "x": i * 16,
            "tit": f"{DIAS_CORTOS[i]} {d.strftime('%d-%m')}: {plural(n, 'retiro', 'retiros')}",
        })
    return barras


def semana(hoy):
    lunes = hoy - timedelta(days=hoy.weekday())
    return lunes, lunes + timedelta(days=6)

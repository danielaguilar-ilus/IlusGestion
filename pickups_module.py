import os
import re
import secrets
import time
from datetime import datetime, timedelta

from flask import flash, jsonify, redirect, render_template, request, send_from_directory, url_for
from werkzeug.utils import secure_filename


PICKUP_STATUS = {
    "solicitud_recibida": "Solicitud recibida",
    "en_revision": "En revision",
    "informacion_incompleta": "Informacion incompleta",
    "propuesta_enviada": "Propuesta enviada",
    "esperando_cliente": "Esperando respuesta",
    "agenda_confirmada": "Agenda confirmada",
    "reagendada": "Reagendada",
    "rechazada": "Rechazada",
    "en_preparacion": "En preparacion",
    "retirada": "Retirada",
    "fallida": "Fallida",
    "cerrada": "Cerrada",
}

PICKUP_STATUS_COLORS = {
    "solicitud_recibida": "primary",
    "en_revision": "info",
    "informacion_incompleta": "warning",
    "propuesta_enviada": "warning",
    "esperando_cliente": "secondary",
    "agenda_confirmada": "success",
    "reagendada": "warning",
    "rechazada": "danger",
    "en_preparacion": "dark",
    "retirada": "success",
    "fallida": "danger",
    "cerrada": "secondary",
}

PICKUP_RELATIONS = [
    ("dueno", "Dueno / titular"),
    ("comprador", "Comprador"),
    ("familiar", "Familiar autorizado"),
    ("chofer", "Chofer"),
    ("transporte", "Transporte externo"),
    ("autorizado", "Persona autorizada"),
    ("otro", "Otro"),
]


# ── VALIDADORES CHILENOS ──────────────────────────────────────────────

def _clean_rut(rut: str) -> str:
    """Normaliza RUT: solo números y K, en mayúsculas."""
    return re.sub(r"[^0-9kK]", "", str(rut or "")).upper()


def _calc_dv(num: str) -> str:
    """Calcula el dígito verificador del RUT (módulo 11)."""
    suma, mul = 0, 2
    for ch in reversed(num):
        suma += int(ch) * mul
        mul = 2 if mul == 7 else mul + 1
    r = 11 - (suma % 11)
    return "0" if r == 11 else "K" if r == 10 else str(r)


def is_valid_rut(rut: str) -> bool:
    """Valida RUT chileno usando módulo 11. Acepta cualquier formato."""
    c = _clean_rut(rut)
    if not 8 <= len(c) <= 9:
        return False
    num, dv = c[:-1], c[-1]
    return num.isdigit() and _calc_dv(num) == dv


def format_rut(rut: str) -> str:
    """Formatea RUT como '12.345.678-9'."""
    c = _clean_rut(rut)
    if len(c) < 2:
        return c
    num, dv = c[:-1], c[-1]
    rev = "".join(reversed(num))
    parts = [rev[i:i+3] for i in range(0, len(rev), 3)]
    return ".".join("".join(reversed(p)) for p in reversed(parts)) + "-" + dv


def is_valid_cl_phone(phone: str) -> bool:
    """Valida teléfono chileno: +56 9 XXXX XXXX (móvil)."""
    c = re.sub(r"[^\d+]", "", str(phone or "")).lstrip("+")
    return bool(re.match(r"^(56)?9\d{8}$", c))


def format_cl_phone(phone: str) -> str:
    """Devuelve teléfono normalizado a formato +569XXXXXXXX (sin espacios)."""
    c = re.sub(r"[^\d+]", "", str(phone or "")).lstrip("+")
    if c.startswith("56"):
        c = c[2:]
    if c.startswith("9") and len(c) == 9:
        return "+56" + c
    return phone


def register_pickup_routes(app, ctx):
    mysql_fetchone = ctx["mysql_fetchone"]
    mysql_fetchall = ctx["mysql_fetchall"]
    mysql_execute = ctx["mysql_execute"]
    get_db = ctx["get_db"]
    require_permission = ctx["require_permission"]
    EMAIL_RE = ctx["EMAIL_RE"]
    BASE_DIR = ctx["BASE_DIR"]
    g = ctx["g"]
    _ilus_email_html = ctx["_ilus_email_html"]
    _send_ilus_email = ctx["_send_ilus_email"]
    _get_wa_cfg = ctx["_get_wa_cfg"]
    _send_whatsapp = ctx["_send_whatsapp"]

    REQ = ctx["PICKUP_REQUESTS_TABLE"]
    PKG = ctx["PICKUP_PACKAGES_TABLE"]
    PROP = ctx["PICKUP_PROPOSALS_TABLE"]
    LOG = ctx["PICKUP_LOGS_TABLE"]
    ATT = ctx["PICKUP_ATTACHMENTS_TABLE"]
    SIG = ctx["PICKUP_SIGNATURES_TABLE"]
    SET = ctx["PICKUP_SETTINGS_TABLE"]
    TPL = ctx["PICKUP_TEMPLATES_TABLE"]

    upload_dir = os.path.join(BASE_DIR, "static", "uploads", "retiros")
    os.makedirs(upload_dir, exist_ok=True)

    def ensure_marketing_columns():
        for col in ("hero_image_1", "hero_image_2", "hero_image_3"):
            try:
                mysql_execute(f"ALTER TABLE `{SET}` ADD COLUMN `{col}` VARCHAR(260) NULL")
            except Exception:
                pass

    ensure_marketing_columns()

    def ensure_reminder_columns():
        """Migración: timestamp del envío del recordatorio 24h."""
        try:
            mysql_execute(
                f"ALTER TABLE `{REQ}` ADD COLUMN reminder_24h_sent DATETIME NULL "
                f"COMMENT 'Timestamp del recordatorio 24h enviado al cliente'"
            )
        except Exception:
            pass

    ensure_reminder_columns()

    def settings():
        row = mysql_fetchone(f"SELECT * FROM `{SET}` WHERE id=1") or {}
        # Migración: subir close_time a 17:30 (bodega trabaja hasta 17:00, recibe
        # clientes hasta 16:30 — el último slot 16:30+60min termina a 17:30).
        if row and str(row.get("close_time", ""))[:5] == "16:30":
            try:
                mysql_execute(f"UPDATE `{SET}` SET close_time='17:30:00' WHERE id=1")
                row["close_time"] = "17:30:00"
            except Exception:
                pass
        return row or {
            "warehouse_name": "Bodega ILUS Quilicura",
            "warehouse_addr": "Av. Presidente Eduardo Frei Montalva 9770, Bod 30, Quilicura.",
            "maps_url": "https://www.google.com/maps/search/?api=1&query=Av.%20Presidente%20Eduardo%20Frei%20Montalva%209770%20Bod%2030%20Quilicura",
            "open_time": "09:00:00",
            "close_time": "17:30:00",
            "work_days": "1,2,3,4,5",
            "holidays": "",
            "alert_enabled": 0,
            "alert_title": "Aviso importante",
            "alert_message": "",
            "hero_image_1": "",
            "hero_image_2": "",
            "hero_image_3": "",
        }

    def date_allowed(date_str, cfg=None):
        cfg = cfg or settings()
        try:
            dt = datetime.strptime(date_str, "%Y-%m-%d").date()
        except Exception:
            return False, "Fecha no valida."
        days = {int(x) for x in (cfg.get("work_days") or "1,2,3,4,5").split(",") if x.strip().isdigit()}
        holidays = {d.strip() for d in (cfg.get("holidays") or "").replace(";", ",").split(",") if d.strip()}
        if dt.isoweekday() not in days:
            return False, "La bodega no recibe retiros ese dia."
        if date_str in holidays:
            return False, "La fecha seleccionada esta marcada como feriado o dia bloqueado."
        return True, ""

    def next_allowed_date(cfg=None):
        cfg = cfg or settings()
        base = datetime.now().date() + timedelta(days=1)
        for offset in range(0, 45):
            candidate = base + timedelta(days=offset)
            ok, _msg = date_allowed(candidate.isoformat(), cfg)
            if ok:
                return candidate.isoformat()
        return base.isoformat()

    def time_allowed(time_from, time_to, cfg=None):
        cfg = cfg or settings()
        open_t = str(cfg.get("open_time") or "09:00:00")[:5]
        close_t = str(cfg.get("close_time") or "17:30:00")[:5]
        if not time_from or not time_to or time_from >= time_to:
            return False, "Selecciona un rango horario valido."
        if time_from < open_t or time_to > close_t:
            return False, f"El horario debe estar entre {open_t} y {close_t}."
        return True, ""

    def calc_package(length_cm, width_cm, height_cm, weight_kg):
        l, w, h = [max(float(x or 0), 0) for x in (length_cm, width_cm, height_cm)]
        kg = max(float(weight_kg or 0), 0)
        return {
            "length_cm": l,
            "width_cm": w,
            "height_cm": h,
            "weight_kg": kg,
            "volumetric_weight": round((l * w * h) / 4000, 3) if l and w and h else 0,
            "volume_m3": round((l * w * h) / 1000000, 4) if l and w and h else 0,
        }

    def quality_score(data, packages, signed=False, attachments=0):
        checks = [
            bool(data.get("document_number")),
            bool(data.get("customer_name")),
            bool(data.get("customer_rut")),
            bool(EMAIL_RE.match(data.get("contact_email") or "")),
            bool(re.match(r"^\+?[0-9\s\-]{7,20}$", data.get("contact_phone") or "")),
            bool(data.get("pickup_person_name")),
            bool(data.get("pickup_person_rut")),
            bool(data.get("requested_date")),
            bool(data.get("requested_time_from") and data.get("requested_time_to")),
            bool(packages),
            all(p["length_cm"] and p["width_cm"] and p["height_cm"] for p in packages),
            all(p["weight_kg"] for p in packages),
            attachments > 0,
            signed,
        ]
        score = int(round((sum(1 for ok in checks if ok) / len(checks)) * 100))
        risk = 0
        if not data.get("customer_rut") or not data.get("pickup_person_rut"):
            risk += 25
        if (data.get("pickup_person_relation") or "") in {"chofer", "transporte", "otro"}:
            risk += 15
        if score < 70:
            risk += 20
        return min(score, 100), min(risk, 100)

    def status_badge(status):
        return {"label": PICKUP_STATUS.get(status, status), "color": PICKUP_STATUS_COLORS.get(status, "secondary")}

    def log_event(request_id, action, old_status=None, new_status=None, notes="", actor_type="sistema", actor_name=None):
        try:
            mysql_execute(
                f"""INSERT INTO `{LOG}`
                    (request_id,actor_type,actor_name,action,old_status,new_status,notes,ip,user_agent)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (
                    request_id,
                    actor_type,
                    actor_name or (g.user["nombre"] if getattr(g, "user", None) else "Cliente"),
                    action,
                    old_status,
                    new_status,
                    notes,
                    request.remote_addr,
                    (request.user_agent.string or "")[:300],
                ),
            )
        except Exception as exc:
            print(f"[ILUS][PICKUP LOG] {exc}")

    # ══════════════════════════════════════════════════════════════════
    #  VALIDACIÓN DE DISPONIBILIDAD REAL DE SLOT (cupos + bloqueos + colación)
    # ══════════════════════════════════════════════════════════════════
    def _validar_disponibilidad_slot(date, time_from, time_to, exclude_request_id=None,
                                      extra_kg=0, extra_m3=0):
        """Valida que un slot (date + time_from..time_to) pueda usarse.

        Chequea, en este orden:
          1) Día permitido (work_days + holidays) y horario dentro de open/close
          2) Solape con colación (lunch_start/lunch_end)
          3) Solape con pickup_blocks (full day o franja específica)
          4) Capacidad: max_picks_per_slot, max_kg_per_slot, max_m3_per_slot
          5) Capacidad diaria: max_picks_per_day

        Args:
            date: 'YYYY-MM-DD' (str) o date
            time_from, time_to: 'HH:MM' (str)
            exclude_request_id: id de pickup_request a excluir del conteo
                                (útil al revalidar la propia confirmación)
            extra_kg, extra_m3: peso/volumen del retiro que se está agregando
                                (se suma al conteo actual del slot)

        Returns:
            (ok: bool, motivo: str)
        """
        cfg = settings()
        try:
            date_str = date.isoformat() if hasattr(date, "isoformat") else str(date)
        except Exception:
            return False, "Fecha inválida."

        # 1) Día permitido (work_days + holidays) y rango horario válido
        ok_d, msg_d = date_allowed(date_str, cfg)
        if not ok_d:
            return False, msg_d
        ok_t, msg_t = time_allowed(time_from, time_to, cfg)
        if not ok_t:
            return False, msg_t

        # Parsear minutos
        try:
            sh, sm = [int(x) for x in str(time_from)[:5].split(":")]
            eh, em = [int(x) for x in str(time_to)[:5].split(":")]
            slot_s = sh * 60 + sm
            slot_e = eh * 60 + em
        except Exception:
            return False, "Horario con formato inválido."

        # 2) Solape con colación
        lunch_s_str = str(cfg.get("lunch_start") or "13:00")[:5]
        lunch_e_str = str(cfg.get("lunch_end") or "14:00")[:5]
        try:
            lh, lm = [int(x) for x in lunch_s_str.split(":")]
            leh, lem = [int(x) for x in lunch_e_str.split(":")]
            lunch_s = lh * 60 + lm
            lunch_e = leh * 60 + lem
            if lunch_s < lunch_e:
                # Solape si NO termina antes del almuerzo y NO empieza después
                if not (slot_e <= lunch_s or slot_s >= lunch_e):
                    return False, f"El horario solapa la colación ({lunch_s_str}-{lunch_e_str})."
        except Exception:
            pass

        # 3) Bloqueos manuales en pickup_blocks
        try:
            blk_rows = mysql_fetchall(
                "SELECT hora_inicio, hora_fin, motivo FROM pickup_blocks WHERE fecha=%s",
                (date_str,),
            ) or []
            for b in blk_rows:
                hi = b.get("hora_inicio")
                hf = b.get("hora_fin")
                motivo = (b.get("motivo") or "bloqueado").strip()
                # Día completo bloqueado
                if not hi:
                    return False, f"Día bloqueado: {motivo}"
                try:
                    bh, bm = [int(x) for x in str(hi)[:5].split(":")]
                    bs = bh * 60 + bm
                    if hf:
                        beh, bem = [int(x) for x in str(hf)[:5].split(":")]
                        be = beh * 60 + bem
                    else:
                        be = 24 * 60  # hasta cierre
                    if not (slot_e <= bs or slot_s >= be):
                        return False, f"Franja bloqueada: {motivo}"
                except Exception:
                    continue
        except Exception:
            pass  # tabla puede no existir aún

        # 4) Capacidad del slot (picks + kg + m3)
        max_picks_slot = int(cfg.get("max_picks_per_slot") or 5)
        max_kg_slot = float(cfg.get("max_kg_per_slot") or 500)
        max_m3_slot = float(cfg.get("max_m3_per_slot") or 5)
        max_picks_day = int(cfg.get("max_picks_per_day") or 30)

        # Conteo en el MISMO slot (mismo time_from). Usamos confirmed_* o proposed_*
        # según el estado real. Excluimos cancelados/cerrados.
        exclude_clause = ""
        params = [date_str, str(time_from)[:5], date_str, str(time_from)[:5]]
        if exclude_request_id:
            exclude_clause = "AND id <> %s"
            params.append(int(exclude_request_id))
        slot_row = mysql_fetchone(
            f"""SELECT COUNT(*) AS n,
                       COALESCE(SUM(total_weight_kg),0) AS kg,
                       COALESCE(SUM(total_volume_m3),0) AS m3
                FROM `{REQ}`
                WHERE status NOT IN ('rechazada','cerrada','fallida')
                  AND (
                    (confirmed_date=%s AND TIME_FORMAT(confirmed_time_from,'%%H:%%i')=%s)
                    OR
                    (confirmed_date IS NULL AND proposed_date=%s
                     AND TIME_FORMAT(proposed_time_from,'%%H:%%i')=%s)
                  )
                  {exclude_clause}""",
            tuple(params),
        ) or {}
        picks_now = int(slot_row.get("n") or 0)
        kg_now = float(slot_row.get("kg") or 0)
        m3_now = float(slot_row.get("m3") or 0)

        if picks_now + 1 > max_picks_slot:
            return False, f"Slot lleno: {picks_now} retiros (máximo {max_picks_slot})."
        if kg_now + float(extra_kg or 0) > max_kg_slot:
            return False, f"Capacidad de peso excedida: {kg_now:.1f}+{float(extra_kg or 0):.1f} kg > {max_kg_slot:.0f} kg."
        if m3_now + float(extra_m3 or 0) > max_m3_slot:
            return False, f"Capacidad de volumen excedida: {m3_now:.2f}+{float(extra_m3 or 0):.2f} m³ > {max_m3_slot:.2f} m³."

        # 5) Capacidad diaria
        day_params = [date_str, date_str]
        day_exclude = ""
        if exclude_request_id:
            day_exclude = "AND id <> %s"
            day_params.append(int(exclude_request_id))
        day_row = mysql_fetchone(
            f"""SELECT COUNT(*) AS n FROM `{REQ}`
                WHERE status NOT IN ('rechazada','cerrada','fallida')
                  AND (confirmed_date=%s OR (confirmed_date IS NULL AND proposed_date=%s))
                  {day_exclude}""",
            tuple(day_params),
        ) or {}
        picks_day = int(day_row.get("n") or 0)
        if picks_day + 1 > max_picks_day:
            return False, f"Día completo: {picks_day} retiros (máximo diario {max_picks_day})."

        return True, ""

    def _render_pickup_vars(req, proposal=None):
        """Construye el dict de variables disponibles en plantillas de retiros."""
        cfg = settings()
        def _hm(t):
            return str(t)[:5] if t else ""
        def _fmt_horario(tf, tt):
            tf, tt = _hm(tf), _hm(tt)
            return f"{tf} - {tt}" if tf else ""
        return {
            "code":              req.get("code") or "",
            "cliente":           req.get("customer_name") or "",
            "persona_retira":    req.get("pickup_person_name") or req.get("contact_name") or "",
            "documento":         f"{(req.get('document_type') or '').upper()} {req.get('document_number') or ''}".strip(),
            "fecha_solicitada":  str(req.get("requested_date") or ""),
            "fecha_propuesta":   str((proposal or {}).get("date") or req.get("proposed_date") or ""),
            "fecha_confirmada":  str(req.get("confirmed_date") or ""),
            "horario": (
                _fmt_horario((proposal or {}).get("time_from"), (proposal or {}).get("time_to"))
                or _fmt_horario(req.get("confirmed_time_from"), req.get("confirmed_time_to"))
                or _fmt_horario(req.get("proposed_time_from"), req.get("proposed_time_to"))
                or _fmt_horario(req.get("requested_time_from"), req.get("requested_time_to"))
            ),
            "n_bultos":          str(req.get("total_packages") or 0),
            "kg":                str(req.get("total_weight_kg") or 0),
            "m3":                str(req.get("total_volume_m3") or 0),
            "warehouse_name":    cfg.get("warehouse_name") or "Bodega ILUS Quilicura",
            "warehouse_addr":    cfg.get("warehouse_addr") or "",
            "link_seguimiento":  url_for("pickup_public_tracking", token=req["public_token"], _external=True),
        }


    def _apply_template(text, variables):
        """Reemplaza {{var}} con su valor (string)."""
        if not text:
            return ""
        for k, v in variables.items():
            text = text.replace("{{" + k + "}}", str(v))
            text = text.replace("{{ " + k + " }}", str(v))
        return text


    def _get_pickup_template(estado, canal):
        """Lee plantilla de comm_templates para retiros (modulo='retiros')."""
        try:
            row = mysql_fetchone(
                "SELECT asunto, cuerpo FROM comm_templates "
                "WHERE modulo='retiros' AND estado=%s AND canal=%s LIMIT 1",
                (estado, canal)
            )
            return row
        except Exception:
            return None


    # Mapeo: kind del notify() → estado en comm_templates
    _KIND_TO_ESTADO = {
        "created":   "solicitud_recibida",
        "proposal":  "propuesta_enviada",
        "confirmed": "agenda_confirmada",
        "preparing": "en_preparacion",
        "done":      "retirada",
        "rejected":  "rechazada",
        "rescheduled": "reagendada",
        "reminder_24h": "recordatorio_24h",  # nuevo kind para recordatorio del día previo
        "message":   None,    # custom — sin plantilla
    }


    def notify(req, kind="created", proposal=None, custom_message=""):
        """Envía notificación al cliente usando plantillas configuradas en
        Comunicaciones → Plantillas → Retiros (DB).

        Si la plantilla no existe en BD, cae al template hardcoded original.
        """
        cfg = settings()
        follow_url = url_for("pickup_public_tracking", token=req["public_token"], _external=True)
        variables = _render_pickup_vars(req, proposal)
        estado = _KIND_TO_ESTADO.get(kind)

        # ── EMAIL ──────────────────────────────────────────────────────
        sent_mail = False
        try:
            tpl_email = _get_pickup_template(estado, "email") if estado else None
            if tpl_email and (tpl_email.get("asunto") or tpl_email.get("cuerpo")):
                # Plantilla configurada en BD: usar con variables interpoladas
                asunto = _apply_template(tpl_email.get("asunto") or "", variables)
                cuerpo = _apply_template(tpl_email.get("cuerpo") or "", variables)
                # Envolver en el wrapper HTML oficial ILUS
                html = _ilus_email_html(
                    titulo=asunto or f"Actualización retiro {req['code']}",
                    subtitulo=f"{req['code']} - {variables['documento']}",
                    saludo=variables["persona_retira"],
                    parrafos=[cuerpo],   # cuerpo ya viene como HTML
                    btn_primario_txt="Ver solicitud",
                    btn_primario_url=follow_url,
                    btn_secundario_txt="Cómo llegar",
                    btn_secundario_url=cfg.get("maps_url"),
                    info_lineas=[
                        ("", "Bodega", variables["warehouse_name"]),
                        ("", "Dirección", variables["warehouse_addr"]),
                    ],
                )
                sent_mail = _send_ilus_email(req["contact_email"], f"ILUS — {asunto}", html)
            else:
                # Fallback: plantilla hardcoded original
                titles = {
                    "created": "Solicitud de retiro recibida",
                    "proposal": "ILUS propuso una agenda de retiro",
                    "confirmed": "Agenda de retiro confirmada",
                    "reminder_24h": "Recordatorio: tu retiro es mañana",
                    "message": "Actualización de tu solicitud de retiro",
                }
                title = titles.get(kind, "Actualización de retiro ILUS")
                if kind == "created":
                    paragraphs = [
                        "Recibimos tu solicitud. Esta solicitud aún no es una reserva confirmada.",
                        "Nuestro equipo revisará documento, identidad y disponibilidad para responder con una confirmación o propuesta.",
                    ]
                elif kind == "proposal" and proposal:
                    paragraphs = [
                        "Te enviamos una propuesta de fecha y horario para tu retiro.",
                        f"Propuesta: <strong>{proposal['date']} de {str(proposal['time_from'])[:5]} a {str(proposal['time_to'])[:5]}</strong>.",
                        proposal.get("message") or "Puedes confirmar, rechazar o proponer una nueva fecha desde el enlace.",
                    ]
                elif kind == "confirmed":
                    paragraphs = [
                        "Tu agenda de retiro fue confirmada.",
                        "Recuerda presentar documento de identidad. Si retira un tercero, debe coincidir con la persona autorizada.",
                    ]
                elif kind == "reminder_24h":
                    paragraphs = [
                        f"Te recordamos que <strong>mañana retiramos tus productos</strong>.",
                        f"Hora: <strong>{variables['horario']}</strong>.",
                        f"Dirección: {variables['warehouse_addr'] or variables['warehouse_name']}.",
                        "Por favor presentarse con documento de identidad. Si retira un tercero, debe coincidir con la persona autorizada.",
                    ]
                else:
                    paragraphs = [custom_message or "Hay una actualización disponible para tu solicitud de retiro."]
                html = _ilus_email_html(
                    titulo=title,
                    subtitulo=f"{req['code']} - {variables['documento']}",
                    saludo=variables["persona_retira"],
                    parrafos=paragraphs,
                    btn_primario_txt="Ver solicitud",
                    btn_primario_url=follow_url,
                    btn_secundario_txt="Cómo llegar",
                    btn_secundario_url=cfg.get("maps_url"),
                    info_lineas=[
                        ("", "Bodega", variables["warehouse_name"]),
                        ("", "Dirección", variables["warehouse_addr"]),
                        ("", "Horario solicitado", variables["horario"]),
                    ],
                )
                sent_mail = _send_ilus_email(req["contact_email"], f"ILUS - {title} {req['code']}", html)
        except Exception as exc:
            try:
                print(f"[ILUS][PICKUP EMAIL] {str(exc).encode('ascii', 'ignore').decode('ascii')}")
            except Exception:
                pass

        # ── WHATSAPP ───────────────────────────────────────────────────
        sent_wa = None
        try:
            wa_cfg = _get_wa_cfg()
            if wa_cfg.get("account_sid") and wa_cfg.get("auth_token") and wa_cfg.get("from_number"):
                tpl_wa = _get_pickup_template(estado, "whatsapp") if estado else None
                if tpl_wa and tpl_wa.get("cuerpo"):
                    wa_body = _apply_template(tpl_wa.get("cuerpo") or "", variables)
                else:
                    titles = {
                        "created": "Solicitud de retiro recibida",
                        "proposal": "ILUS propuso una agenda de retiro",
                        "confirmed": "Agenda de retiro confirmada",
                        "reminder_24h": "Recordatorio: tu retiro es mañana",
                        "message": "Actualización de tu retiro",
                    }
                    title = titles.get(kind, "Actualización retiro ILUS")
                    if kind == "reminder_24h":
                        wa_body = (
                            f"ILUS - {title}\n\nSolicitud: {req['code']}\n"
                            f"Mañana retiramos tus productos.\n"
                            f"Hora: {variables['horario']}\n"
                            f"Bodega: {variables['warehouse_name']}\n"
                            f"Dirección: {variables['warehouse_addr']}\n"
                            f"Seguimiento: {follow_url}"
                        )
                    else:
                        wa_body = (
                            f"ILUS - {title}\n\nSolicitud: {req['code']}\n"
                            f"Documento: {variables['documento']}\n"
                            f"Bodega: {variables['warehouse_name']}\n"
                            f"Seguimiento: {follow_url}"
                        )
                sent_wa = _send_whatsapp(
                    wa_cfg["account_sid"], wa_cfg["auth_token"], wa_cfg["from_number"],
                    req["contact_phone"], wa_body
                )
        except Exception as exc:
            print(f"[ILUS][PICKUP WA] {exc}")
        return sent_mail, sent_wa

    @app.route("/retiros/solicitar", methods=["GET", "POST"])
    def pickup_public_request():
        cfg = settings()
        if request.method == "POST":
            form = request.form
            data = {
                "document_type": (form.get("document_type") or "factura").strip(),
                "document_number": (form.get("document_number") or "").strip(),
                "customer_name": (form.get("customer_name") or "").strip(),
                "customer_rut": (form.get("customer_rut") or "").strip(),
                "contact_name": (form.get("contact_name") or "").strip(),
                "contact_email": (form.get("contact_email") or "").strip().lower(),
                "contact_phone": (form.get("contact_phone") or "").strip(),
                "whatsapp_phone": (form.get("whatsapp_phone") or "").strip(),
                "pickup_person_name": (form.get("pickup_person_name") or "").strip(),
                "pickup_person_rut": (form.get("pickup_person_rut") or "PENDIENTE").strip(),
                "pickup_person_phone": (form.get("pickup_person_phone") or "").strip(),
                "pickup_person_relation": (form.get("pickup_person_relation") or "autorizado").strip(),
                "requested_date": (form.get("requested_date") or "").strip(),
                "requested_time_from": (form.get("requested_time_from") or "").strip(),
                "requested_time_to": (form.get("requested_time_to") or "").strip(),
                "observations": (form.get("observations") or "").strip(),
                "invoice_total_amount": float(form.get("invoice_total_amount") or 0),
            }
            if not data["requested_date"]:
                data["requested_date"] = next_allowed_date(cfg)

            # ──────────────────────────────────────────────────────────────
            # FORMULARIO SIMPLIFICADO (a pedido):
            # La persona autorizada es OPCIONAL. Si el cliente NO marca el
            # toggle, asumimos que el cliente mismo retira → copiamos sus
            # datos a los campos pickup_person_*.
            # ──────────────────────────────────────────────────────────────
            auth_active = (form.get("auth_active") or "0") == "1"
            if not auth_active or not data.get("pickup_person_name"):
                # El cliente retira por sí mismo
                data["pickup_person_name"]     = data.get("customer_name") or ""
                data["pickup_person_rut"]      = data.get("customer_rut") or ""
                data["pickup_person_phone"]    = data.get("contact_phone") or ""
                data["pickup_person_relation"] = "dueno"
            else:
                # Cliente autorizó a un tercero — completar campos faltantes con respaldos
                if not data.get("pickup_person_phone"):
                    data["pickup_person_phone"] = data.get("contact_phone") or ""
                if not data.get("pickup_person_rut"):
                    data["pickup_person_rut"] = data.get("customer_rut") or ""

            # contact_name es el "nombre de quien retira" (puede ser el cliente o el autorizado)
            if not data.get("contact_name"):
                data["contact_name"] = data.get("pickup_person_name") or data.get("customer_name") or ""

            errors = []
            required = ["document_number", "customer_name", "customer_rut",
                        "contact_email", "contact_phone",
                        "requested_time_from", "requested_time_to"]
            if any(not data.get(k) for k in required):
                errors.append("Completa todos los campos obligatorios.")

            # Validación email
            if data["contact_email"] and not EMAIL_RE.match(data["contact_email"]):
                errors.append("El email no tiene formato válido.")

            # Validación teléfono chileno (+56 9)
            if data["contact_phone"] and not is_valid_cl_phone(data["contact_phone"]):
                errors.append("El teléfono debe ser un móvil chileno (+56 9 XXXX XXXX).")
            else:
                data["contact_phone"] = format_cl_phone(data["contact_phone"])
                data["pickup_person_phone"] = format_cl_phone(data["pickup_person_phone"] or data["contact_phone"])

            # Validación RUT chileno (cliente y persona autorizada)
            if data["customer_rut"] and not is_valid_rut(data["customer_rut"]):
                errors.append("El RUT del cliente no es válido (revisa el dígito verificador).")
            else:
                data["customer_rut"] = format_rut(data["customer_rut"])

            # El RUT del autorizado se valida solo si el cliente activó el toggle
            # y lo escribió. Si no lo escribió, usamos el del cliente como respaldo.
            if auth_active and data["pickup_person_rut"] and not is_valid_rut(data["pickup_person_rut"]):
                errors.append("El RUT de la persona que retira no es válido.")
            elif data["pickup_person_rut"] and is_valid_rut(data["pickup_person_rut"]):
                data["pickup_person_rut"] = format_rut(data["pickup_person_rut"])

            # Validación fecha mínima +24 horas
            if data["requested_date"]:
                try:
                    fecha_solicitada = datetime.strptime(data["requested_date"], "%Y-%m-%d").date()
                    fecha_minima = (datetime.now() + timedelta(hours=24)).date()
                    if fecha_solicitada < fecha_minima:
                        errors.append("La fecha de retiro debe ser al menos 24 horas después de hoy.")
                except ValueError:
                    errors.append("Fecha inválida.")
            ok_date = True
            msg_date = ""
            if data["requested_date"]:
                ok_date, msg_date = date_allowed(data["requested_date"], cfg)
            ok_time, msg_time = time_allowed(data["requested_time_from"], data["requested_time_to"], cfg)
            if not ok_date:
                errors.append(msg_date)
            if not ok_time:
                errors.append(msg_time)
            total_pkg = 1
            packages = [calc_package(0, 0, 0, 0)]
            if not form.get("accept_terms"):
                errors.append("Debes aceptar la declaracion de responsabilidad y autorizacion.")
            if errors:
                try:
                    _car_rows = mysql_fetchall("SELECT archivo_path, titulo, subtitulo FROM retiros_carousel WHERE activa=1 ORDER BY orden ASC, id ASC")
                    _car_imgs = [dict(r) for r in (_car_rows or [])]
                except Exception:
                    _car_imgs = []
                return render_template("retiros/public_request.html", settings=cfg, relations=PICKUP_RELATIONS, errors=errors, fd=form, carousel_images=_car_imgs)

            files = [f for f in request.files.getlist("attachments") if f and f.filename]
            quality, risk = quality_score(data, packages, signed=True, attachments=len(files))
            token = secrets.token_urlsafe(42)
            if data["whatsapp_phone"]:
                extra = f"WhatsApp informado: {data['whatsapp_phone']}"
                data["observations"] = f"{data['observations']}\n{extra}".strip()
            total_weight = round(sum(p["weight_kg"] for p in packages), 3)
            total_vw = round(sum(p["volumetric_weight"] for p in packages), 3)
            total_m3 = round(sum(p["volume_m3"] for p in packages), 4)
            conn = get_db()
            with conn.cursor() as cur:
                cur.execute(
                    f"""INSERT INTO `{REQ}`
                        (document_type,document_number,customer_name,customer_rut,contact_name,contact_email,contact_phone,
                         pickup_person_name,pickup_person_rut,pickup_person_phone,pickup_person_relation,
                         requested_date,requested_time_from,requested_time_to,status,information_quality_score,risk_score,
                         total_packages,total_weight_kg,total_volumetric_weight,total_volume_m3,invoice_total_amount,
                         observations,public_token,signature_status,created_ip,created_user_agent)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'solicitud_recibida',%s,%s,%s,%s,%s,%s,%s,%s,%s,'firmado',%s,%s)""",
                    (
                        data["document_type"], data["document_number"], data["customer_name"], data["customer_rut"],
                        data["contact_name"], data["contact_email"], data["contact_phone"],
                        data["pickup_person_name"], data["pickup_person_rut"], data["pickup_person_phone"], data["pickup_person_relation"],
                        data["requested_date"], data["requested_time_from"], data["requested_time_to"],
                        quality, risk, len(packages), total_weight, total_vw, total_m3, data["invoice_total_amount"],
                        data["observations"], token, request.remote_addr, (request.user_agent.string or "")[:300],
                    ),
                )
                rid = cur.lastrowid
                code = f"RET-{rid:06d}"
                cur.execute(f"UPDATE `{REQ}` SET code=%s WHERE id=%s", (code, rid))
                for idx, pkg in enumerate(packages, 1):
                    cur.execute(
                        f"""INSERT INTO `{PKG}` (request_id,package_number,length_cm,width_cm,height_cm,weight_kg,volumetric_weight,volume_m3)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
                        (rid, idx, pkg["length_cm"], pkg["width_cm"], pkg["height_cm"], pkg["weight_kg"], pkg["volumetric_weight"], pkg["volume_m3"]),
                    )
                cur.execute(
                    f"""INSERT INTO `{SIG}` (request_id,signer_name,signer_rut,accepted_terms,ip,user_agent)
                        VALUES (%s,%s,%s,1,%s,%s)""",
                    (rid, data["contact_name"], data["customer_rut"], request.remote_addr, (request.user_agent.string or "")[:300]),
                )
            conn.commit()
            for f in files:
                ext = f.filename.rsplit(".", 1)[-1].lower() if "." in f.filename else ""
                if ext not in {"png", "jpg", "jpeg", "webp", "pdf", "doc", "docx"}:
                    continue
                fname = f"ret_{rid}_{int(time.time())}_{secure_filename(f.filename)}"
                f.save(os.path.join(upload_dir, fname))
                mysql_execute(
                    f"""INSERT INTO `{ATT}` (request_id,filename,original_name,mime_type,uploaded_by)
                        VALUES (%s,%s,%s,%s,'cliente')""",
                    (rid, fname, f.filename, f.mimetype),
                )
            req = mysql_fetchone(f"SELECT * FROM `{REQ}` WHERE id=%s", (rid,))
            log_event(rid, "creada", None, "solicitud_recibida", "Solicitud creada desde pagina publica", "cliente", data["contact_name"])
            sent_mail, sent_wa = notify(req, "created")
            if sent_mail:
                log_event(rid, "email_enviado", "solicitud_recibida", "solicitud_recibida", f"Correo enviado a {req['contact_email']}", "sistema", "Comunicaciones")
            else:
                log_event(rid, "email_pendiente", "solicitud_recibida", "solicitud_recibida", f"No se pudo enviar correo a {req['contact_email']}. Revisar Comunicaciones.", "sistema", "Comunicaciones")
            if sent_wa:
                log_event(rid, "whatsapp_enviado", "solicitud_recibida", "solicitud_recibida", "WhatsApp de solicitud enviado.", "sistema", "Comunicaciones")
            return redirect(url_for("pickup_public_tracking", token=token, created=1))
        # Anti-cache para forzar al navegador a recargar diseño actualizado
        from flask import make_response
        # Cargar imágenes activas del carrusel desde BD (retiros_carousel)
        try:
            carousel_rows = mysql_fetchall(
                "SELECT archivo_path, titulo, subtitulo FROM retiros_carousel "
                "WHERE activa=1 ORDER BY orden ASC, id ASC"
            )
            carousel_images = [dict(r) for r in (carousel_rows or [])]
        except Exception:
            carousel_images = []
        # Cargar avisos vigentes (retiros_announcements)
        try:
            anuncios_rows = mysql_fetchall(
                "SELECT id, titulo, mensaje, tipo, icon FROM retiros_announcements "
                "WHERE activa=1 "
                "  AND (fecha_desde IS NULL OR fecha_desde <= NOW()) "
                "  AND (fecha_hasta IS NULL OR fecha_hasta >= NOW()) "
                "ORDER BY orden ASC, id DESC"
            )
            anuncios = [dict(r) for r in (anuncios_rows or [])]
        except Exception:
            anuncios = []
        resp = make_response(render_template(
            "retiros/public_request.html",
            settings=cfg, relations=PICKUP_RELATIONS, errors=[], fd={},
            carousel_images=carousel_images,
            announcements=anuncios,
        ))
        resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        resp.headers["Pragma"] = "no-cache"
        resp.headers["Expires"] = "0"
        return resp

    @app.route("/retiros/buscar")
    def pickup_buscar_publico():
        """Lookup público por código de retiro (RET-XXXXXX).
        El form público de seguimiento referencia esta ruta.
        Si encuentra el retiro, redirige a /retiros/seguimiento/<token>.
        """
        code = (request.args.get("code") or "").strip().upper()
        if not code:
            return redirect(url_for("pickup_public_request"))
        # Buscar por código exacto o LIKE (con/sin prefijo RET-)
        row = mysql_fetchone(
            f"SELECT public_token FROM `{REQ}` WHERE UPPER(code)=%s LIMIT 1",
            (code,)
        )
        if not row:
            # Probar quitando "RET-" si lo trae
            code_alt = code.replace("RET-", "").lstrip("0") or code
            row = mysql_fetchone(
                f"SELECT public_token FROM `{REQ}` WHERE code LIKE %s OR code LIKE %s LIMIT 1",
                (f"%{code_alt}%", f"%{code}%")
            )
        if not row or not row.get("public_token"):
            flash(f"No encontramos un retiro con el código '{code}'. Verifica e intenta nuevamente.", "warning")
            return redirect(url_for("pickup_public_request"))
        return redirect(url_for("pickup_public_tracking", token=row["public_token"]))


    @app.route("/retiros/seguimiento/<token>", methods=["GET", "POST"])
    def pickup_public_tracking(token):
        req = mysql_fetchone(f"SELECT * FROM `{REQ}` WHERE public_token=%s", (token,))
        if not req:
            return "Solicitud no encontrada", 404
        cfg = settings()
        if request.method == "POST":
            action = request.form.get("action")
            old = req["status"]
            if action == "confirm":
                proposal = mysql_fetchone(f"SELECT * FROM `{PROP}` WHERE request_id=%s AND status='pending' ORDER BY id DESC LIMIT 1", (req["id"],))
                if proposal:
                    # RE-VALIDAR capacidad antes de confirmar. Entre el envío de la
                    # propuesta y este click puede haberse llenado el slot (otro
                    # cliente confirmó primero, se bloqueó el día, etc.).
                    ok_slot, motivo = _validar_disponibilidad_slot(
                        proposal["date"], proposal["time_from"], proposal["time_to"],
                        exclude_request_id=req["id"],
                        extra_kg=float(req.get("total_weight_kg") or 0),
                        extra_m3=float(req.get("total_volume_m3") or 0),
                    )
                    if not ok_slot:
                        # Marcar la propuesta como declined y notificar al cliente
                        mysql_execute(
                            f"UPDATE `{PROP}` SET status='declined', answered_at=NOW() WHERE id=%s",
                            (proposal["id"],),
                        )
                        log_event(
                            req["id"], "confirm_bloqueada", old, old,
                            f"Cliente intentó confirmar pero slot ya no disponible: {motivo}",
                            "sistema", "Validación capacidad",
                        )
                        flash(
                            "Lo sentimos, este horario ya no está disponible. "
                            "Te enviamos una nueva propuesta a la brevedad.",
                            "warning",
                        )
                        return redirect(url_for("pickup_public_tracking", token=token))
                    mysql_execute(
                        f"""UPDATE `{REQ}` SET status='agenda_confirmada', confirmed_date=%s, confirmed_time_from=%s, confirmed_time_to=%s WHERE id=%s""",
                        (proposal["date"], proposal["time_from"], proposal["time_to"], req["id"]),
                    )
                    mysql_execute(f"UPDATE `{PROP}` SET status='accepted', answered_at=NOW() WHERE id=%s", (proposal["id"],))
                    log_event(req["id"], "cliente_confirmo", old, "agenda_confirmada", "Cliente acepto propuesta", "cliente", req["contact_name"])
            elif action == "reject":
                reason = request.form.get("reason", "")
                mysql_execute(f"UPDATE `{REQ}` SET status='rechazada' WHERE id=%s", (req["id"],))
                log_event(req["id"], "cliente_rechazo", old, "rechazada", reason, "cliente", req["contact_name"])
            elif action == "counter":
                date, tf, tt = request.form.get("counter_date"), request.form.get("counter_time_from"), request.form.get("counter_time_to")
                okd, md = date_allowed(date, cfg); okt, mt = time_allowed(tf, tt, cfg)
                if okd and okt:
                    mysql_execute(
                        f"""INSERT INTO `{PROP}` (request_id,proposed_by,date,time_from,time_to,message,reason,status,token)
                            VALUES (%s,'cliente',%s,%s,%s,%s,'Contrapropuesta cliente','pending',%s)""",
                        (req["id"], date, tf, tt, request.form.get("counter_message", ""), secrets.token_urlsafe(24)),
                    )
                    mysql_execute(f"UPDATE `{REQ}` SET status='en_revision' WHERE id=%s", (req["id"],))
                    log_event(req["id"], "cliente_contrapropuso", old, "en_revision", f"{date} {tf}-{tt}", "cliente", req["contact_name"])
                else:
                    flash(md or mt, "warning")
            return redirect(url_for("pickup_public_tracking", token=token))
        packages = mysql_fetchall(f"SELECT * FROM `{PKG}` WHERE request_id=%s ORDER BY package_number", (req["id"],))
        proposals = mysql_fetchall(f"SELECT * FROM `{PROP}` WHERE request_id=%s ORDER BY id DESC", (req["id"],))
        logs = mysql_fetchall(f"SELECT * FROM `{LOG}` WHERE request_id=%s ORDER BY id DESC LIMIT 20", (req["id"],))
        attachments = mysql_fetchall(f"SELECT * FROM `{ATT}` WHERE request_id=%s ORDER BY id DESC", (req["id"],))
        return render_template("retiros/public_tracking.html", req=req, packages=packages, proposals=proposals, logs=logs, attachments=attachments, settings=cfg, status_badge=status_badge, created=request.args.get("created"))

    @app.route("/retiros")
    @require_permission("retiros")
    def pickup_dashboard():
        filtros = {"q": request.args.get("q", "").strip(), "status": request.args.get("status", "").strip(), "date": request.args.get("date", "").strip(), "view": request.args.get("view", "monitor").strip()}
        where, params = ["1=1"], []
        if filtros["q"]:
            like = f"%{filtros['q']}%"; where.append("(code LIKE %s OR document_number LIKE %s OR customer_name LIKE %s OR contact_phone LIKE %s)"); params.extend([like, like, like, like])
        if filtros["status"]:
            where.append("status=%s"); params.append(filtros["status"])
        if filtros["date"]:
            where.append("(requested_date=%s OR confirmed_date=%s OR proposed_date=%s)"); params.extend([filtros["date"], filtros["date"], filtros["date"]])
        rows = mysql_fetchall(f"SELECT * FROM `{REQ}` WHERE {' AND '.join(where)} ORDER BY created_at DESC LIMIT 250", tuple(params))
        counts = mysql_fetchall(f"SELECT status, COUNT(*) AS n FROM `{REQ}` GROUP BY status")
        stats = {r["status"]: int(r["n"]) for r in counts}
        today = datetime.now().date().isoformat()
        day = mysql_fetchone(
            f"""SELECT COUNT(*) AS total, COALESCE(SUM(total_packages),0) AS bultos,
                       COALESCE(SUM(total_weight_kg),0) AS peso, COALESCE(SUM(total_volumetric_weight),0) AS pvol,
                       COALESCE(SUM(total_volume_m3),0) AS m3
                FROM `{REQ}` WHERE requested_date=%s OR confirmed_date=%s""",
            (today, today),
        ) or {}
        templates = mysql_fetchall(f"SELECT * FROM `{TPL}` WHERE active=1 ORDER BY title")
        return render_template("retiros/internal_dashboard.html", rows=rows, filtros=filtros, statuses=PICKUP_STATUS, stats=stats, day=day, settings=settings(), templates=templates, status_badge=status_badge)

    # ══════════════════════════════════════════════════════════════════
    #  BANDEJA "HACER HOY" — tareas prioritarias del equipo
    # ══════════════════════════════════════════════════════════════════
    @app.route("/retiros/api/bandeja-hoy")
    @require_permission("retiros")
    def pickup_bandeja_hoy():
        """Devuelve las 4 listas de tareas prioritarias del día.

        Grupos:
          - urgentes:         sin validar + fecha solicitada <= hoy+2 días
          - sin_respuesta:    propuestas internas con > 24h sin respuesta
          - confirmados_hoy:  agenda_confirmada con fecha hoy (a preparar)
          - sin_firma:        retirados hoy sin firma/foto registrada
        """
        def _norm(rows):
            out = []
            for r in rows or []:
                d = dict(r)
                # Serializar fechas/horas a strings
                for k in ("requested_date", "proposed_date", "confirmed_date"):
                    if d.get(k):
                        d[k] = d[k].isoformat() if hasattr(d[k], "isoformat") else str(d[k])
                for k in ("requested_time_from", "requested_time_to",
                          "proposed_time_from", "proposed_time_to",
                          "confirmed_time_from", "confirmed_time_to"):
                    if d.get(k):
                        d[k] = str(d[k])[:5]
                for k in ("created_at", "closed_at", "proposal_created_at"):
                    if d.get(k):
                        d[k] = str(d[k])[:19]
                # URLs útiles para acciones
                if d.get("id"):
                    d["detail_url"] = url_for("pickup_detail", rid=d["id"])
                if d.get("public_token"):
                    d["tracking_url"] = url_for(
                        "pickup_public_tracking",
                        token=d["public_token"],
                        _external=False,
                    )
                out.append(d)
            return out

        # 1) Urgentes: sin validar + fecha solicitada en los próximos 2 días
        urgentes = mysql_fetchall(
            f"""SELECT id, code, customer_name, contact_name, contact_phone,
                       contact_email, document_type, document_number,
                       requested_date, requested_time_from, requested_time_to,
                       status, doc_validation_status, public_token, created_at
                FROM `{REQ}`
                WHERE status IN ('solicitud_recibida','en_revision')
                  AND (doc_validation_status IS NULL
                       OR doc_validation_status IN ('pendiente','en_revision'))
                  AND requested_date IS NOT NULL
                  AND requested_date <= DATE_ADD(CURDATE(), INTERVAL 2 DAY)
                ORDER BY requested_date ASC, created_at ASC
                LIMIT 50"""
        ) or []

        # 2) Propuestas enviadas hace > 24h sin respuesta del cliente
        sin_respuesta = mysql_fetchall(
            f"""SELECT pr.id, pr.code, pr.customer_name, pr.contact_name,
                       pr.contact_phone, pr.contact_email,
                       pr.document_type, pr.document_number,
                       pr.status, pr.public_token,
                       pp.date AS proposed_date,
                       pp.time_from AS proposed_time_from,
                       pp.time_to AS proposed_time_to,
                       pp.created_at AS proposal_created_at
                FROM `{REQ}` pr
                JOIN `{PROP}` pp ON pp.request_id = pr.id
                WHERE pp.proposed_by = 'internal'
                  AND pp.status = 'pending'
                  AND pp.created_at < NOW() - INTERVAL 24 HOUR
                  AND pr.status NOT IN ('rechazada','cerrada','fallida','retirada')
                ORDER BY pp.created_at ASC
                LIMIT 50"""
        ) or []

        # 3) Confirmados para hoy (a preparar)
        confirmados_hoy = mysql_fetchall(
            f"""SELECT id, code, customer_name, contact_name, contact_phone,
                       contact_email, document_type, document_number,
                       confirmed_date, confirmed_time_from, confirmed_time_to,
                       total_packages, total_weight_kg, total_volume_m3,
                       pickup_person_name, status, public_token
                FROM `{REQ}`
                WHERE status = 'agenda_confirmada'
                  AND DATE(confirmed_date) = CURDATE()
                ORDER BY confirmed_time_from ASC
                LIMIT 50"""
        ) or []

        # 4) Retiradas hoy sin firma/foto registrada
        sin_firma = mysql_fetchall(
            f"""SELECT pr.id, pr.code, pr.customer_name, pr.contact_name,
                       pr.contact_phone, pr.contact_email,
                       pr.document_type, pr.document_number,
                       pr.confirmed_date, pr.closed_at,
                       pr.pickup_person_name, pr.status, pr.public_token
                FROM `{REQ}` pr
                WHERE pr.status = 'retirada'
                  AND DATE(pr.closed_at) = CURDATE()
                  AND pr.id NOT IN (
                       SELECT request_id FROM `{SIG}` WHERE request_id IS NOT NULL
                  )
                ORDER BY pr.closed_at DESC
                LIMIT 50"""
        ) or []

        return jsonify({
            "ok": True,
            "today": datetime.now().date().isoformat(),
            "urgentes":        _norm(urgentes),
            "sin_respuesta":   _norm(sin_respuesta),
            "confirmados_hoy": _norm(confirmados_hoy),
            "sin_firma":       _norm(sin_firma),
        })

    # ══════════════════════════════════════════════════════════════════
    #  RECORDATORIO 24h — disparo manual o cron-style
    # ══════════════════════════════════════════════════════════════════
    @app.route("/retiros/admin/enviar-recordatorios-24h", methods=["POST"])
    @require_permission("admin")
    def pickup_enviar_recordatorios_24h():
        """Envía recordatorio a clientes con retiro confirmado para MAÑANA.

        Selecciona pickup_requests con status='agenda_confirmada' y
        confirmed_date = CURDATE()+1 que no tengan reminder_24h_sent
        registrado, dispara notify(req, 'reminder_24h') y marca el envío.

        Devuelve JSON: {enviados, omitidos, errores: [{code, error}]}.
        Apto para disparo manual desde el dashboard o vía cron externo.
        """
        rows = mysql_fetchall(
            f"""SELECT * FROM `{REQ}`
                WHERE status = 'agenda_confirmada'
                  AND DATE(confirmed_date) = DATE_ADD(CURDATE(), INTERVAL 1 DAY)
                  AND reminder_24h_sent IS NULL"""
        ) or []

        enviados = 0
        omitidos = 0
        errores = []
        for r in rows:
            try:
                req_dict = dict(r)
                sent_mail, sent_wa = notify(req_dict, "reminder_24h")
                if sent_mail or sent_wa:
                    mysql_execute(
                        f"UPDATE `{REQ}` SET reminder_24h_sent=NOW() WHERE id=%s",
                        (req_dict["id"],),
                    )
                    log_event(
                        req_dict["id"], "recordatorio_24h_enviado",
                        req_dict.get("status"), req_dict.get("status"),
                        "Recordatorio 24h enviado al cliente.",
                        "sistema", "Recordatorios",
                    )
                    enviados += 1
                else:
                    omitidos += 1
                    errores.append({
                        "code": req_dict.get("code"),
                        "error": "No se pudo enviar por ningún canal (email/WA).",
                    })
            except Exception as exc:
                omitidos += 1
                errores.append({
                    "code": dict(r).get("code"),
                    "error": str(exc)[:200],
                })

        return jsonify({
            "ok": True,
            "enviados": enviados,
            "omitidos": omitidos,
            "errores": errores,
            "total_candidatos": len(rows),
        })

    @app.route("/retiros/<int:rid>")
    @require_permission("view")
    def pickup_detail(rid):
        req = mysql_fetchone(f"SELECT * FROM `{REQ}` WHERE id=%s", (rid,))
        if not req:
            flash("Solicitud de retiro no encontrada.", "danger")
            return redirect(url_for("pickup_dashboard"))
        packages = mysql_fetchall(f"SELECT * FROM `{PKG}` WHERE request_id=%s ORDER BY package_number", (rid,))
        proposals = mysql_fetchall(f"SELECT * FROM `{PROP}` WHERE request_id=%s ORDER BY id DESC", (rid,))
        logs = mysql_fetchall(f"SELECT * FROM `{LOG}` WHERE request_id=%s ORDER BY id DESC LIMIT 80", (rid,))
        attachments = mysql_fetchall(f"SELECT * FROM `{ATT}` WHERE request_id=%s ORDER BY id DESC", (rid,))
        templates = mysql_fetchall(f"SELECT * FROM `{TPL}` WHERE active=1 ORDER BY title")
        return render_template("retiros/internal_detail.html", req=req, packages=packages, proposals=proposals, logs=logs, attachments=attachments, templates=templates, statuses=PICKUP_STATUS, status_badge=status_badge, settings=settings())

    @app.route("/retiros/<int:rid>/status", methods=["POST"])
    @require_permission("edit")
    def pickup_update_status(rid):
        req = mysql_fetchone(f"SELECT * FROM `{REQ}` WHERE id=%s", (rid,))
        if not req:
            return redirect(url_for("pickup_dashboard"))
        new_status, notes = request.form.get("status"), request.form.get("notes", "")
        if new_status not in PICKUP_STATUS:
            flash("Estado no valido.", "danger")
            return redirect(url_for("pickup_detail", rid=rid))
        old_status = req.get("status") or ""
        mysql_execute(f"UPDATE `{REQ}` SET status=%s, closed_at=IF(%s IN ('cerrada','rechazada','retirada'),NOW(),closed_at) WHERE id=%s", (new_status, new_status, rid))
        log_event(rid, "estado_actualizado", old_status, new_status, notes, "interno")

        # ─── Notificar al cliente cuando ILUS cambia estado desde el calendario ─
        # Antes este endpoint NO mandaba email/WA, dejando al cliente sin saber
        # que su retiro fue confirmado/rechazado/preparado.
        kind_map = {
            "agenda_confirmada":   "confirmed",
            "rechazada":           "rejected",
            "en_preparacion":      "preparing",
            "retirada":            "done",
            "fallida":             "failed",
            "reagendada":          "rescheduled",
            "cerrada":             "closed",
            "informacion_incompleta": "info_incompleta",
        }
        kind = kind_map.get(new_status)
        if kind and old_status != new_status:
            try:
                # Re-leer la solicitud actualizada
                req_after = mysql_fetchone(f"SELECT * FROM `{REQ}` WHERE id=%s", (rid,)) or req
                notify(req_after, kind)
            except Exception as _e:
                # Si falla notificación, no rompemos el cambio de estado
                print(f"[pickups][notify] error notificando {kind} a retiro #{rid}: {_e}")

        flash("Estado actualizado.", "success")
        return redirect(url_for("pickup_detail", rid=rid))

    @app.route("/retiros/<int:rid>/validar-doc", methods=["POST"])
    @require_permission("edit")
    def pickup_validar_doc(rid):
        """Paso 1 del proceso interno: validar documentación del cliente.
        Marca el estado de validación, calcula peso/vol/tiempo estimado y
        deja todo listo para el paso 2 (proponer fecha).
        """
        req = mysql_fetchone(f"SELECT * FROM `{REQ}` WHERE id=%s", (rid,))
        if not req:
            return redirect(url_for("pickup_dashboard"))

        action = (request.form.get("action") or "").strip()
        notes  = (request.form.get("notes") or "").strip()[:1000]

        if action == "marcar_ok":
            # Validación exitosa
            peso_real = request.form.get("peso_real_kg") or req.get("peso_real_kg") or req.get("total_weight_kg") or 0
            peso_vol  = request.form.get("peso_vol_kg")  or req.get("peso_vol_kg")  or req.get("total_volumetric_weight") or 0
            tiempo    = request.form.get("tiempo_estimado_min") or req.get("tiempo_estimado_min") or 0
            try:
                peso_real = float(peso_real or 0)
                peso_vol  = float(peso_vol or 0)
                tiempo    = int(tiempo or 0)
            except Exception:
                pass
            mysql_execute(
                f"""UPDATE `{REQ}`
                    SET doc_validation_status='ok',
                        doc_validated_at=NOW(),
                        doc_validated_by=%s,
                        doc_validation_notes=%s,
                        peso_real_kg=%s,
                        peso_vol_kg=%s,
                        tiempo_estimado_min=%s,
                        status=CASE WHEN status='solicitud_recibida' THEN 'en_revision' ELSE status END
                    WHERE id=%s""",
                (g.user["nombre"] if getattr(g,"user",None) else "interno",
                 notes, peso_real, peso_vol, tiempo, rid)
            )
            log_event(rid, "doc_validada", req["status"], "en_revision",
                      f"Documentación validada · peso={peso_real}kg vol={peso_vol}kg tiempo={tiempo}min",
                      "interno")
            flash("Documentación validada. Ya puedes proponer fecha al cliente.", "success")

        elif action == "marcar_incompleto":
            mysql_execute(
                f"""UPDATE `{REQ}`
                    SET doc_validation_status='incompleto',
                        doc_validation_notes=%s,
                        status='informacion_incompleta'
                    WHERE id=%s""",
                (notes, rid)
            )
            log_event(rid, "doc_incompleta", req["status"], "informacion_incompleta",
                      f"Falta información: {notes[:200]}", "interno")
            flash("Marcado como información incompleta. Notifica al cliente para completar.", "warning")

        elif action == "guardar_erp":
            # Guardar snapshot ERP + auto-rellenar peso/vol
            erp_json = request.form.get("erp_data_json") or "{}"
            peso_real = request.form.get("peso_real_kg") or 0
            peso_vol  = request.form.get("peso_vol_kg") or 0
            tiempo    = request.form.get("tiempo_estimado_min") or 0
            mysql_execute(
                f"""UPDATE `{REQ}`
                    SET doc_erp_data=%s,
                        peso_real_kg=%s,
                        peso_vol_kg=%s,
                        tiempo_estimado_min=%s
                    WHERE id=%s""",
                (erp_json[:60000], peso_real or 0, peso_vol or 0, tiempo or 0, rid)
            )
            log_event(rid, "erp_actualizado", req["status"], req["status"],
                      f"Datos ERP cargados · peso={peso_real}kg vol={peso_vol}kg",
                      "interno")
            flash("Datos del ERP cargados al retiro.", "success")

        return redirect(url_for("pickup_detail", rid=rid))


    @app.route("/retiros/<int:rid>/proposal", methods=["POST"])
    @require_permission("edit")
    def pickup_create_proposal(rid):
        req = mysql_fetchone(f"SELECT * FROM `{REQ}` WHERE id=%s", (rid,))
        if not req:
            return redirect(url_for("pickup_dashboard"))

        # WORKFLOW: bloquear propuesta si la documentación no fue validada
        if req.get("doc_validation_status") not in ("ok",):
            flash("Antes de proponer fecha, valida la documentación del cliente (paso 1).", "warning")
            return redirect(url_for("pickup_detail", rid=rid))

        date, tf, tt = request.form.get("date"), request.form.get("time_from"), request.form.get("time_to")
        okd, md = date_allowed(date); okt, mt = time_allowed(tf, tt)
        if not okd or not okt:
            flash(md or mt, "warning")
            return redirect(url_for("pickup_detail", rid=rid))

        # Validar capacidad real del slot (cupos, kg, m³, bloqueos, colación)
        # Excluimos esta misma solicitud porque podría tener una propuesta previa
        # ocupando el slot que estamos por reemplazar.
        ok_slot, motivo = _validar_disponibilidad_slot(
            date, tf, tt,
            exclude_request_id=rid,
            extra_kg=float(req.get("total_weight_kg") or 0),
            extra_m3=float(req.get("total_volume_m3") or 0),
        )
        if not ok_slot:
            flash(f"Slot no disponible: {motivo}", "danger")
            log_event(rid, "propuesta_bloqueada", req["status"], req["status"],
                      f"Intento de proponer {date} {tf}-{tt}: {motivo}", "interno")
            return redirect(url_for("pickup_detail", rid=rid))

        mysql_execute(
            f"""INSERT INTO `{PROP}` (request_id,proposed_by,date,time_from,time_to,message,reason,status,token)
                VALUES (%s,'internal',%s,%s,%s,%s,%s,'pending',%s)""",
            (rid, date, tf, tt, request.form.get("message", ""), request.form.get("reason", ""), secrets.token_urlsafe(24)),
        )
        mysql_execute(f"UPDATE `{REQ}` SET status='propuesta_enviada', proposed_date=%s, proposed_time_from=%s, proposed_time_to=%s WHERE id=%s", (date, tf, tt, rid))
        log_event(rid, "propuesta_enviada", req["status"], "propuesta_enviada", f"{date} {tf}-{tt}", "interno")
        fresh = mysql_fetchone(f"SELECT * FROM `{REQ}` WHERE id=%s", (rid,))
        notify(fresh, "proposal", proposal={"date": date, "time_from": tf, "time_to": tt, "message": request.form.get("message", "")})
        flash("Propuesta enviada al cliente.", "success")
        return redirect(url_for("pickup_detail", rid=rid))

    @app.route("/retiros/<int:rid>/message", methods=["POST"])
    @require_permission("edit")
    def pickup_send_message(rid):
        req = mysql_fetchone(f"SELECT * FROM `{REQ}` WHERE id=%s", (rid,))
        if not req:
            return redirect(url_for("pickup_dashboard"))
        template_id = request.form.get("template_id", type=int)
        tpl = mysql_fetchone(f"SELECT * FROM `{TPL}` WHERE id=%s", (template_id,)) if template_id else None
        message = request.form.get("message", "").strip() or (tpl["body"] if tpl else "")
        if not message:
            flash("Selecciona o escribe un mensaje.", "warning")
            return redirect(url_for("pickup_detail", rid=rid))
        notify(req, "message", custom_message=message)
        log_event(rid, "mensaje_enviado", req["status"], req["status"], message, "interno")
        flash("Mensaje enviado por los canales disponibles.", "success")
        return redirect(url_for("pickup_detail", rid=rid))

    @app.route("/retiros/settings", methods=["POST"])
    @require_permission("admin")
    def pickup_settings_save():
        data = request.form
        mysql_execute(
            f"""UPDATE `{SET}`
                SET warehouse_name=%s, warehouse_addr=%s, maps_url=%s, open_time=%s, close_time=%s,
                    work_days=%s, holidays=%s, alert_enabled=%s, alert_title=%s, alert_message=%s
                WHERE id=1""",
            (
                data.get("warehouse_name", ""), data.get("warehouse_addr", ""), data.get("maps_url", ""),
                data.get("open_time", "09:00"), data.get("close_time", "17:30"),
                ",".join(data.getlist("work_days")) or "1,2,3,4,5", data.get("holidays", ""),
                1 if data.get("alert_enabled") else 0, data.get("alert_title", "Aviso importante"), data.get("alert_message", ""),
            ),
        )
        flash("Configuracion de retiros actualizada.", "success")
        return redirect(url_for("pickup_dashboard"))

    @app.route("/ajustes/marketing", methods=["GET", "POST"])
    @require_permission("admin")
    def marketing_settings():
        if request.method == "POST":
            current = settings()
            hero_paths = {
                "hero_image_1": current.get("hero_image_1") or "",
                "hero_image_2": current.get("hero_image_2") or "",
                "hero_image_3": current.get("hero_image_3") or "",
            }
            for key in hero_paths:
                f = request.files.get(key)
                if f and f.filename:
                    ext = f.filename.rsplit(".", 1)[-1].lower() if "." in f.filename else ""
                    if ext in {"png", "jpg", "jpeg", "webp"}:
                        fname = f"hero_{key}_{int(time.time())}_{secure_filename(f.filename)}"
                        f.save(os.path.join(upload_dir, fname))
                        hero_paths[key] = f"uploads/retiros/{fname}"
            mysql_execute(
                f"""UPDATE `{SET}`
                    SET hero_image_1=%s, hero_image_2=%s, hero_image_3=%s
                    WHERE id=1""",
                (hero_paths["hero_image_1"], hero_paths["hero_image_2"], hero_paths["hero_image_3"]),
            )
            flash("Marketing publico actualizado.", "success")
            return redirect(url_for("marketing_settings"))
        # Cargar imágenes de login para mostrar en el tab unificado
        try:
            login_imgs = mysql_fetchall(
                "SELECT * FROM login_images ORDER BY orden ASC, id ASC"
            )
            login_imgs = [dict(r) for r in login_imgs]
        except Exception:
            login_imgs = []
        # Cargar bloqueos de retiros próximos (60 días)
        try:
            from datetime import datetime as _dt, timedelta as _td
            blocks_rows = mysql_fetchall(
                "SELECT id, fecha, hora_inicio, hora_fin, motivo, created_by, created_at "
                "FROM pickup_blocks "
                "WHERE fecha >= %s AND fecha <= %s "
                "ORDER BY fecha ASC, hora_inicio ASC",
                (_dt.now().date(), _dt.now().date() + _td(days=60))
            ) or []
            pickup_blocks_list = [dict(r) for r in blocks_rows]
        except Exception:
            pickup_blocks_list = []
        return render_template("admin/marketing.html",
                               settings=settings(),
                               login_imagenes=login_imgs,
                               pickup_blocks=pickup_blocks_list)


    @app.route("/retiros/bloqueos/nuevo", methods=["POST"])
    @require_permission("admin")
    def pickup_blocks_new():
        """Crea un bloqueo de día/franja en pickup_blocks."""
        fecha = (request.form.get("fecha") or "").strip()
        hora_ini = (request.form.get("hora_inicio") or "").strip() or None
        hora_fin = (request.form.get("hora_fin") or "").strip() or None
        motivo = (request.form.get("motivo") or "").strip()[:200]
        if not fecha:
            flash("La fecha es obligatoria.", "danger")
            return redirect(url_for("marketing_settings"))
        try:
            mysql_execute(
                "INSERT INTO pickup_blocks (fecha, hora_inicio, hora_fin, motivo, created_by) "
                "VALUES (%s, %s, %s, %s, %s)",
                (fecha, hora_ini, hora_fin, motivo,
                 g.user["nombre"] if getattr(g,"user",None) else None)
            )
            flash("Bloqueo creado correctamente.", "success")
        except Exception as exc:
            flash(f"Error al crear bloqueo: {exc}", "danger")
        return redirect(url_for("marketing_settings"))


    @app.route("/retiros/bloqueos/batch", methods=["POST"])
    @require_permission("admin")
    def pickup_blocks_new_batch():
        """Crea múltiples bloqueos en una sola operación.

        Recibe del formulario:
          fecha: YYYY-MM-DD (obligatorio)
          motivo: str (opcional)
          slots[]: lista de pares "HH:MM-HH:MM" (uno por franja seleccionada)
          full_day: si está en 1, ignora slots y bloquea día completo

        Si no se envían slots ni full_day, falla amablemente.
        """
        fecha = (request.form.get("fecha") or "").strip()
        motivo = (request.form.get("motivo") or "").strip()[:200]
        if not fecha:
            flash("La fecha es obligatoria.", "danger")
            return redirect(url_for("marketing_settings"))
        # Día completo
        full_day = request.form.get("full_day")
        slots = request.form.getlist("slots[]") or request.form.getlist("slots")
        creado_por = g.user["nombre"] if getattr(g, "user", None) else None
        inserted = 0
        try:
            if full_day:
                mysql_execute(
                    "INSERT INTO pickup_blocks (fecha, hora_inicio, hora_fin, motivo, created_by) "
                    "VALUES (%s, NULL, NULL, %s, %s)",
                    (fecha, motivo or "Día completo bloqueado", creado_por)
                )
                inserted = 1
            elif slots:
                for s in slots:
                    if "-" not in s:
                        continue
                    hi, hf = s.split("-", 1)
                    hi = hi.strip()
                    hf = hf.strip()
                    if not hi or not hf:
                        continue
                    try:
                        mysql_execute(
                            "INSERT INTO pickup_blocks (fecha, hora_inicio, hora_fin, motivo, created_by) "
                            "VALUES (%s, %s, %s, %s, %s)",
                            (fecha, hi, hf, motivo, creado_por)
                        )
                        inserted += 1
                    except Exception:
                        continue
            else:
                flash("Selecciona al menos una franja o marca 'día completo'.", "warning")
                return redirect(url_for("marketing_settings"))
            flash(f"✅ {inserted} franja(s) bloqueadas para {fecha}.", "success")
        except Exception as exc:
            flash(f"Error al crear bloqueos: {exc}", "danger")
        return redirect(url_for("marketing_settings") + "#bloqueos")


    @app.route("/retiros/bloqueos/<int:bid>/eliminar", methods=["POST"])
    @require_permission("admin")
    def pickup_blocks_delete(bid):
        """Elimina un bloqueo."""
        try:
            mysql_execute("DELETE FROM pickup_blocks WHERE id=%s", (bid,))
            flash("Bloqueo eliminado.", "success")
        except Exception as exc:
            flash(f"Error al eliminar: {exc}", "danger")
        return redirect(url_for("marketing_settings"))

    @app.route("/retiros/adjuntos/<int:aid>")
    @require_permission("view")
    def pickup_attachment(aid):
        row = mysql_fetchone(f"SELECT * FROM `{ATT}` WHERE id=%s", (aid,))
        if not row:
            return "No encontrado", 404
        return send_from_directory(upload_dir, row["filename"], as_attachment=False, download_name=row["original_name"])

    # ══════════════════════════════════════════════════════════════════
    #  CALENDARIO OPERATIVO — vista por día/franja con capacidad
    # ══════════════════════════════════════════════════════════════════

    def _slot_label(time_from):
        """Devuelve etiqueta legible de la franja (HH:MM)."""
        try: return str(time_from)[:5]
        except Exception: return "00:00"

    def _slot_key(date_str, time_from):
        """Clave única día+franja para agrupar."""
        return f"{date_str}_{_slot_label(time_from)}"

    @app.route("/retiros/calendario")
    @require_permission("retiros")
    def pickup_calendar():
        """Vista calendario operativo de retiros."""
        return render_template("retiros/calendario.html",
            statuses=PICKUP_STATUS,
            settings=settings(),
        )

    @app.route("/retiros/api/calendario")
    @require_permission("view")
    def pickup_calendar_api():
        """Devuelve solicitudes de retiro entre 2 fechas con agregados por día/franja.

        Query params: from=YYYY-MM-DD&to=YYYY-MM-DD (defaults: hoy → +14 días)
        Response: {
          settings: {capacidad, horarios, etc.},
          dias: {
            "2026-05-08": {
              total_picks: N,
              total_kg: N,
              total_m3: N,
              ocupacion_pct: 0..100,
              slots: {
                "09:00": {picks: [...], total_kg, total_m3, capacidad_kg, capacidad_m3, ocupados, max}
              },
              picks: [{id, code, status, customer, kg, m3, time_from, time_to, doc, ...}]
            }
          }
        }
        """
        from datetime import datetime as _dt, timedelta as _td
        cfg = settings()
        try:
            d_from = _dt.strptime(request.args.get("from") or _dt.now().date().isoformat(), "%Y-%m-%d").date()
        except Exception:
            d_from = _dt.now().date()
        try:
            d_to   = _dt.strptime(request.args.get("to") or (d_from + _td(days=14)).isoformat(), "%Y-%m-%d").date()
        except Exception:
            d_to   = d_from + _td(days=14)

        rows = mysql_fetchall(
            f"""SELECT id, code, status, customer_name, customer_rut, contact_name,
                       contact_phone, contact_email, document_type, document_number,
                       requested_date, requested_time_from, requested_time_to,
                       proposed_date, proposed_time_from, proposed_time_to,
                       confirmed_date, confirmed_time_from, confirmed_time_to,
                       total_packages, total_weight_kg, total_volumetric_weight, total_volume_m3,
                       pickup_person_name, pickup_person_phone, pickup_person_relation,
                       information_quality_score, risk_score, observations, internal_notes,
                       created_at
                FROM `{REQ}`
                WHERE (requested_date BETWEEN %s AND %s
                       OR confirmed_date BETWEEN %s AND %s
                       OR proposed_date  BETWEEN %s AND %s)
                ORDER BY requested_date ASC, requested_time_from ASC""",
            (d_from, d_to, d_from, d_to, d_from, d_to)
        )

        # Capacidades
        max_picks_slot = int(cfg.get("max_picks_per_slot") or 5)
        max_kg_slot    = float(cfg.get("max_kg_per_slot") or 500)
        max_m3_slot    = float(cfg.get("max_m3_per_slot") or 5)
        max_picks_day  = int(cfg.get("max_picks_per_day") or 30)
        slot_min       = int(cfg.get("slot_minutes") or 60)

        dias = {}
        for r in rows:
            # Fecha efectiva: confirmed > proposed > requested
            fecha = r.get("confirmed_date") or r.get("proposed_date") or r.get("requested_date")
            tf    = r.get("confirmed_time_from") or r.get("proposed_time_from") or r.get("requested_time_from")
            tt    = r.get("confirmed_time_to") or r.get("proposed_time_to") or r.get("requested_time_to")
            if not fecha: continue
            fecha_str = fecha.isoformat() if hasattr(fecha,"isoformat") else str(fecha)
            slot_lbl  = _slot_label(tf)

            if fecha_str not in dias:
                dias[fecha_str] = {
                    "total_picks": 0, "total_kg": 0, "total_m3": 0,
                    "max_picks_day": max_picks_day,
                    "ocupacion_pct": 0,
                    "slots": {},
                    "picks": [],
                }
            d = dias[fecha_str]
            kg = float(r.get("total_weight_kg") or 0)
            m3 = float(r.get("total_volume_m3") or 0)
            d["total_picks"] += 1
            d["total_kg"]    += kg
            d["total_m3"]    += m3

            if slot_lbl not in d["slots"]:
                d["slots"][slot_lbl] = {
                    "ocupados": 0, "total_kg": 0, "total_m3": 0,
                    "max_picks": max_picks_slot,
                    "max_kg":    max_kg_slot,
                    "max_m3":    max_m3_slot,
                    "picks":     [],
                }
            s = d["slots"][slot_lbl]
            s["ocupados"] += 1
            s["total_kg"] += kg
            s["total_m3"] += m3

            pick = {
                "id":               r["id"],
                "code":             r.get("code"),
                "status":           r.get("status"),
                "status_label":     PICKUP_STATUS.get(r.get("status"), r.get("status")),
                "status_color":     PICKUP_STATUS_COLORS.get(r.get("status"), "secondary"),
                "customer_name":    r.get("customer_name"),
                "customer_rut":     r.get("customer_rut"),
                "contact_name":     r.get("contact_name"),
                "contact_phone":    r.get("contact_phone"),
                "contact_email":    r.get("contact_email"),
                "document_type":    r.get("document_type"),
                "document_number":  r.get("document_number"),
                "fecha":            fecha_str,
                "time_from":        _slot_label(tf),
                "time_to":          _slot_label(tt),
                "is_proposed":      bool(r.get("proposed_date") and not r.get("confirmed_date")),
                "is_confirmed":     bool(r.get("confirmed_date")),
                "total_packages":   int(r.get("total_packages") or 0),
                "total_weight_kg":  kg,
                "total_volume_m3":  m3,
                "pickup_person":    r.get("pickup_person_name"),
                "quality_score":    int(r.get("information_quality_score") or 0),
                "risk_score":       int(r.get("risk_score") or 0),
            }
            s["picks"].append(pick)
            d["picks"].append(pick)

        # Calcular % ocupación día
        for fecha_str, d in dias.items():
            d["ocupacion_pct"] = round(min(100, (d["total_picks"] * 100) / max(1, max_picks_day)))
            for slot_lbl, s in d["slots"].items():
                pct_picks = (s["ocupados"] * 100) / max(1, s["max_picks"])
                pct_kg    = (s["total_kg"] * 100) / max(1, s["max_kg"])
                pct_m3    = (s["total_m3"] * 100) / max(1, s["max_m3"])
                s["pct"]  = round(min(100, max(pct_picks, pct_kg, pct_m3)))
                s["pct_picks"] = round(min(100, pct_picks))
                s["pct_kg"]    = round(min(100, pct_kg))
                s["pct_m3"]    = round(min(100, pct_m3))

        return jsonify({
            "from": d_from.isoformat(),
            "to":   d_to.isoformat(),
            "settings": {
                "open_time":      str(cfg.get("open_time") or "09:00:00")[:5],
                "close_time":     str(cfg.get("close_time") or "17:30:00")[:5],
                "work_days":      cfg.get("work_days") or "1,2,3,4,5",
                "holidays":       cfg.get("holidays") or "",
                "slot_minutes":   slot_min,
                "max_picks_slot": max_picks_slot,
                "max_kg_slot":    max_kg_slot,
                "max_m3_slot":    max_m3_slot,
                "max_picks_day":  max_picks_day,
            },
            "dias": dias,
        })

    @app.route("/retiros/api/disponibilidad-publica")
    def pickup_disponibilidad_publica():
        """Devuelve disponibilidad de slots para los próximos 30 días (público).

        Usado por el form de solicitud para mostrar al cliente qué fechas/franjas
        tiene cupo según capacidad configurada.
        """
        from datetime import datetime as _dt, timedelta as _td
        cfg = settings()
        d_from = _dt.now().date() + _td(days=1)  # Mañana en adelante
        d_to   = d_from + _td(days=30)

        # Obtener retiros existentes en el rango
        rows = mysql_fetchall(
            f"""SELECT requested_date, requested_time_from,
                       confirmed_date, confirmed_time_from,
                       proposed_date, proposed_time_from,
                       total_weight_kg, total_volume_m3, status
                FROM `{REQ}`
                WHERE (requested_date BETWEEN %s AND %s
                       OR confirmed_date BETWEEN %s AND %s)
                  AND status NOT IN ('rechazada','cerrada','fallida')""",
            (d_from, d_to, d_from, d_to)
        )

        max_picks_slot = int(cfg.get("max_picks_per_slot") or 5)
        max_kg_slot    = float(cfg.get("max_kg_per_slot") or 500)
        max_m3_slot    = float(cfg.get("max_m3_per_slot") or 5)
        max_picks_day  = int(cfg.get("max_picks_per_day") or 30)
        slot_dur       = int(cfg.get("slot_minutes") or 60)
        # Step entre inicios: si está configurado, lo usamos; si no, half-step
        # (30min) para que el último slot termine exactamente al close_time.
        slot_step      = int(cfg.get("slot_step_minutes") or 30)
        # Colación (default 13:00 - 14:00)
        lunch_s_str = str(cfg.get("lunch_start") or "13:00")[:5]
        lunch_e_str = str(cfg.get("lunch_end")   or "14:00")[:5]
        try:
            lH,lM = [int(x) for x in lunch_s_str.split(":")]
            leH,leM = [int(x) for x in lunch_e_str.split(":")]
            lunch_start_min = lH*60 + lM
            lunch_end_min   = leH*60 + leM
        except Exception:
            lunch_start_min = lunch_end_min = -1

        # Días hábiles
        work_days = {int(x) for x in (cfg.get("work_days") or "1,2,3,4,5").split(",") if x.strip().isdigit()}
        holidays  = {h.strip() for h in (cfg.get("holidays") or "").replace(";",",").split(",") if h.strip()}

        # Slots horarios — el ÚLTIMO bloque debe TERMINAR ≤ close_time
        # Step de 30min para que con duración 60min el último slot llegue a 16:30
        # (15:30 + 60min = 16:30).
        oH,oM = [int(x) for x in str(cfg.get("open_time") or "09:00:00")[:5].split(":")]
        cH,cM = [int(x) for x in str(cfg.get("close_time") or "17:30:00")[:5].split(":")]
        # slots ahora es lista de dicts con metadata (lunch flag)
        slots = []
        m = 0
        while (oH*60+oM + m + slot_dur) <= (cH*60+cM):
            t = oH*60+oM + m
            t_end = t + slot_dur
            # Detectar solapamiento con bloque de colación
            is_lunch = (lunch_start_min < lunch_end_min and
                        not (t_end <= lunch_start_min or t >= lunch_end_min))
            slots.append({
                "hora": f"{t//60:02d}:{t%60:02d}",
                "lunch": is_lunch,
            })
            m += slot_step

        # Calcular ocupación por slot
        ocupacion = {}  # {fecha_str: {slot: {ocupados, kg, m3}}}
        for r in rows:
            fecha = r.get("confirmed_date") or r.get("proposed_date") or r.get("requested_date")
            if not fecha: continue
            tf = r.get("confirmed_time_from") or r.get("proposed_time_from") or r.get("requested_time_from")
            if not tf: continue
            fecha_str = fecha.isoformat() if hasattr(fecha,"isoformat") else str(fecha)
            slot = str(tf)[:5]
            if fecha_str not in ocupacion: ocupacion[fecha_str] = {}
            if slot not in ocupacion[fecha_str]:
                ocupacion[fecha_str][slot] = {"ocupados":0,"kg":0,"m3":0}
            ocupacion[fecha_str][slot]["ocupados"] += 1
            ocupacion[fecha_str][slot]["kg"]       += float(r.get("total_weight_kg") or 0)
            ocupacion[fecha_str][slot]["m3"]       += float(r.get("total_volume_m3") or 0)

        # Bloqueos manuales (tabla pickup_blocks): días u horas bloqueadas
        # por el administrador desde Marketing.
        blocks_by_date = {}
        try:
            blk_rows = mysql_fetchall(
                f"""SELECT fecha, hora_inicio, hora_fin, motivo
                    FROM pickup_blocks
                    WHERE fecha BETWEEN %s AND %s""",
                (d_from, d_to)
            ) or []
            for b in blk_rows:
                f = b["fecha"]
                fs = f.isoformat() if hasattr(f,"isoformat") else str(f)
                blocks_by_date.setdefault(fs, []).append({
                    "hora_inicio": str(b.get("hora_inicio") or "")[:5],
                    "hora_fin":    str(b.get("hora_fin") or "")[:5],
                    "motivo":      b.get("motivo") or "",
                })
        except Exception:
            pass  # tabla puede no existir aún

        # Generar disponibilidad por día
        dias = {}
        for offset in range(30):
            d = d_from + _td(days=offset)
            iso = d.isoformat()
            disp_dia = (d.isoweekday() in work_days) and (iso not in holidays)

            # Bloqueo de día completo (registro sin horas en pickup_blocks)
            day_blocks = blocks_by_date.get(iso, [])
            full_day_block = any(not b["hora_inicio"] for b in day_blocks)
            full_day_motivo = next((b["motivo"] for b in day_blocks if not b["hora_inicio"]), "")

            day_picks = sum(s["ocupados"] for s in ocupacion.get(iso,{}).values())
            full_dia = day_picks >= max_picks_day
            dia_disponible = disp_dia and not full_dia and not full_day_block

            dias[iso] = {
                "fecha":      iso,
                "disponible": dia_disponible,
                "razon":      "" if dia_disponible else (
                                "Día bloqueado: " + full_day_motivo if full_day_block else
                                "No laborable" if not disp_dia else "Día completo"
                              ),
                "slots":      [],
            }
            if not dia_disponible: continue

            for slot_info in slots:
                slot_hora = slot_info["hora"]
                is_lunch = slot_info["lunch"]
                ocup = ocupacion.get(iso,{}).get(slot_hora, {"ocupados":0,"kg":0,"m3":0})

                # Verificar bloqueos manuales por hora
                slot_h, slot_m = [int(x) for x in slot_hora.split(":")]
                slot_start_min = slot_h*60 + slot_m
                slot_end_min   = slot_start_min + slot_dur
                manual_block = None
                for b in day_blocks:
                    if not b["hora_inicio"]: continue  # ya manejado arriba
                    try:
                        bH, bM = [int(x) for x in b["hora_inicio"].split(":")]
                        bEH, bEM = [int(x) for x in (b["hora_fin"] or "23:59").split(":")]
                        bs, be = bH*60+bM, bEH*60+bEM
                        if not (slot_end_min <= bs or slot_start_min >= be):
                            manual_block = b["motivo"] or "Bloqueado"
                            break
                    except Exception:
                        continue

                if is_lunch:
                    razon = "Colación"
                    disponible_slot = False
                elif manual_block:
                    razon = manual_block
                    disponible_slot = False
                else:
                    razon = ""
                    disponible_slot = (ocup["ocupados"] < max_picks_slot
                                       and ocup["kg"]   < max_kg_slot
                                       and ocup["m3"]   < max_m3_slot)

                dias[iso]["slots"].append({
                    "hora":        slot_hora,
                    "disponible":  disponible_slot,
                    "ocupados":    ocup["ocupados"],
                    "max":         max_picks_slot,
                    "razon":       razon,
                    "lunch":       is_lunch,
                })

        return jsonify({
            "from": d_from.isoformat(),
            "to":   d_to.isoformat(),
            "warehouse_name": cfg.get("warehouse_name"),
            "open_time":  str(cfg.get("open_time") or "09:00")[:5],
            "close_time": str(cfg.get("close_time") or "17:30")[:5],
            "slot_minutes": slot_dur,
            "slot_step":    slot_step,
            "lunch_start":  lunch_s_str,
            "lunch_end":    lunch_e_str,
            "dias": dias,
        })

    @app.route("/retiros/api/<int:rid>/full")
    @require_permission("view")
    def pickup_full_info(rid):
        """Ficha completa de un retiro: datos + bultos + adjuntos + logs + ERP."""
        r = mysql_fetchone(f"SELECT * FROM `{REQ}` WHERE id=%s", (rid,))
        if not r: return jsonify({"error":"No encontrado"}), 404
        d = dict(r)
        # Convertir tipos
        for k in ('requested_date','proposed_date','confirmed_date'):
            if d.get(k): d[k] = d[k].isoformat() if hasattr(d[k],'isoformat') else str(d[k])
        for k in ('requested_time_from','requested_time_to','proposed_time_from','proposed_time_to','confirmed_time_from','confirmed_time_to'):
            if d.get(k): d[k] = str(d[k])[:5]
        for k in ('created_at','updated_at','closed_at'):
            if d.get(k): d[k] = str(d[k])[:19]
        for k in ('total_weight_kg','total_volumetric_weight','total_volume_m3','invoice_total_amount'):
            if d.get(k) is not None: d[k] = float(d[k] or 0)

        # Bultos
        pkgs = mysql_fetchall(
            f"SELECT * FROM `{PKG}` WHERE request_id=%s ORDER BY package_number", (rid,)
        )
        d["packages"] = []
        for p in pkgs:
            pp = dict(p)
            for k in ('length_cm','width_cm','height_cm','weight_kg','volumetric_weight','volume_m3'):
                if pp.get(k) is not None: pp[k] = float(pp[k] or 0)
            d["packages"].append(pp)

        # Logs
        logs = mysql_fetchall(
            f"SELECT id, action, old_status, new_status, notes, actor_type, actor_name, created_at "
            f"FROM `{LOG}` WHERE request_id=%s ORDER BY created_at DESC LIMIT 30", (rid,)
        )
        d["logs"] = []
        for lg in logs:
            ll = dict(lg)
            if ll.get("created_at"): ll["created_at"] = str(ll["created_at"])[:19]
            d["logs"].append(ll)

        # Propuestas
        props = mysql_fetchall(
            f"SELECT id, proposed_by, date, time_from, time_to, message, status, created_at, answered_at "
            f"FROM `{PROP}` WHERE request_id=%s ORDER BY created_at DESC", (rid,)
        )
        d["proposals"] = []
        for p in props:
            pp = dict(p)
            if pp.get("date"): pp["date"] = pp["date"].isoformat() if hasattr(pp["date"],"isoformat") else str(pp["date"])
            for k in ("time_from","time_to"):
                if pp.get(k): pp[k] = str(pp[k])[:5]
            for k in ("created_at","answered_at"):
                if pp.get(k): pp[k] = str(pp[k])[:19]
            d["proposals"].append(pp)

        # Adjuntos
        atts = mysql_fetchall(
            f"SELECT id, original_name, mime_type, uploaded_by, created_at "
            f"FROM `{ATT}` WHERE request_id=%s ORDER BY created_at DESC", (rid,)
        )
        d["attachments"] = [dict(a) for a in atts]
        for a in d["attachments"]:
            if a.get("created_at"): a["created_at"] = str(a["created_at"])[:19]
            a["url"] = url_for("pickup_attachment", aid=a["id"])

        # Estado actual + transiciones
        d["status_label"] = PICKUP_STATUS.get(d.get("status"), d.get("status"))
        d["status_color"] = PICKUP_STATUS_COLORS.get(d.get("status"), "secondary")
        d["public_url"]   = url_for("pickup_public_tracking", token=d.get("public_token"), _external=False)

        # Intentar enriquecer con ERP (best-effort, sin romper si falla)
        d["erp"] = None
        try:
            # Mapping document_type → TIDO ERP (normalizado, sin duplicados)
            tido_map = {
                "factura":     "FCV",
                "boleta":      "BLV",
                "guia":        "GDV",
                "guia_despacho":"GDV",
                "nota_venta":  "VD",
                "venta_directa":"VD",
                "pedido":      "WEB",
                "cotizacion":  "COV",
            }
            doc_type = (d.get("document_type") or "").lower().replace(" ", "_")
            tido = tido_map.get(doc_type)
            nudo = (d.get("document_number") or "").strip()
            if tido and nudo:
                # Import directo en lugar del check frágil "in dir(__import__(...))"
                try:
                    from app import _cubicador_fetch
                except ImportError:
                    _cubicador_fetch = None
                    d["erp"] = {"error": "Motor ERP no disponible (import falló)"}

                if _cubicador_fetch:
                    try:
                        hdr, lineas = _cubicador_fetch(tido, nudo)
                        if hdr:
                            # hdr ya viene normalizado por _cubicador_fetch
                            # (cliente_nombre, cliente_rut, direccion, observaciones, etc.)
                            d["erp"] = {
                                "razon_social": (hdr.get("cliente_nombre") or hdr.get("NRAZON") or "").strip(),
                                "rut":          (hdr.get("cliente_rut") or hdr.get("NRUC") or "").strip(),
                                "direccion":    (hdr.get("direccion") or "").strip(),
                                "comuna":       (hdr.get("comuna") or "").strip(),
                                "telefono":     (hdr.get("telefono") or "").strip(),
                                "email":        (hdr.get("email") or "").strip(),
                                "observaciones": (hdr.get("observaciones") or "").strip(),
                                "tido":         tido,
                                "nudo":         nudo,
                                "lineas":       [{
                                    "sku":      (ln.get("sku") or "").strip(),
                                    "nombre":   (ln.get("descripcion_erp") or ln.get("nombre_app") or "").strip(),
                                    "cantidad": float(ln.get("cantidad") or 0),
                                    "es_zz":    bool(ln.get("es_zz")),
                                    "tiene_cubicaje": bool(ln.get("tiene_ficha") and ln.get("tiene_bultos")),
                                    "peso_kg_tot": float(ln.get("peso_kg_tot") or 0),
                                } for ln in (lineas or [])]
                            }
                        else:
                            d["erp"] = {"error": f"Documento {tido} {nudo} no encontrado en ERP"}
                    except Exception as exc:
                        d["erp"] = {"error": str(exc)[:200]}
        except Exception as exc:
            d["erp"] = {"error": str(exc)[:200]}

        return jsonify(d)

# -*- coding: utf-8 -*-
"""Un «mundo» de mentira para probar la pantalla Control de correos con las funciones REALES de app.py.

Lo usan `tests/test_comm_control_correos.py` y la vista previa local de la pantalla. Nada toca una base real ni envía un correo:
  · la base (comm_correo_control, email_log, comm_templates, auditoría, llaves) vive en memoria;
  · el envío real (`_send_ilus_email_real`) solo anota lo que le pidieron;
  · las funciones de los endpoints se extraen de app.py con `tests/_fuente_codigo.py` (sin parsear los 9 MB de app.py).
"""
import datetime as dt
import json
import re
import types
from functools import wraps

import comm_correos_registro as R
import ot_correos_tecnicos as OTC
from tests import _fuente_codigo as FC

FUNCS = (
    # puerta de envío y estado de los interruptores
    "_correo_ctrl_invalidar", "_correo_ctrl_estado", "_correo_resolver_clave", "_correo_permitido", "_correo_bloqueo_previo",
    "_email_log_tiene_correo_clave", "_email_marcar_bloqueo", "_email_ultimo_bloqueo", "_email_motivo_bloqueo_txt",
    "_email_resultado", "_modulo_desde_evento", "_send_ilus_email",
    # la pantalla
    "_require_superadmin_api", "_comm_correos_fecha", "_comm_correos_version", "_comm_correos_cifras", "_comm_correos_plantillas",
    "_comm_correo_item", "_comm_correos_payload", "comm_correos_listar", "_comm_correo_o_404", "comm_correos_cambiar",
    "_comm_correo_demo", "comm_correos_vista_previa", "comm_correos_prueba", "comm_correos_cambios",
    "_comm_demo_replacements",
)
CONSTS = ("_CORREO_CTRL_CACHE", "_CORREO_CTRL_TTL", "_CORREO_AVISADOS", "_KS_MODULO_TXT", "_EMAIL_LOG_TIENE_CORREO",
          "_COMM_CORREOS_TIPOS_EN_PANTALLA", "_COMM_CORREOS_CIFRAS", "_COMM_CORREOS_CIFRAS_TTL", "_COMM_CORREOS_VENTANA_DIAS")

_TZ = None
_CODIGO = {}          # nombre → código compilado: buscar una función en 170 mil líneas cuesta; un mundo nuevo por prueba no debe repetirlo


def _extraer_cacheado(amb, rel="app.py"):
    """Como `FC.extraer`, pero compila cada función/constante UNA vez por proceso y solo la vuelve a ejecutar en cada mundo."""
    for nombre in CONSTS:
        clave = (rel, "const", nombre)
        if clave not in _CODIGO:
            _CODIGO[clave] = compile(FC.constante_top(rel, nombre), f"<{rel}:{nombre}>", "exec")
        exec(_CODIGO[clave], amb)
    for nombre in FUNCS:
        clave = (rel, "func", nombre)
        if clave not in _CODIGO:
            _CODIGO[clave] = compile(FC.funcion_top(rel, nombre), f"<{rel}:{nombre}>", "exec")
        exec(_CODIGO[clave], amb)
    return amb


def llaves_validas():
    """`_MODULOS_KILLSWITCH_VALIDOS` leído del código de app.py (una vez por proceso)."""
    if "llaves" not in _CODIGO:
        _CODIGO["llaves"] = tuple(FC.valor_literal("app.py", "_MODULOS_KILLSWITCH_VALIDOS"))
    return _CODIGO["llaves"]


def _tz():
    global _TZ
    if _TZ is None:
        import zoneinfo
        _TZ = zoneinfo.ZoneInfo("America/Santiago")
    return _TZ


def chile_fmt(valor, fmt="%d/%m/%Y %H:%M"):
    """UTC naive → hora Chile, como `chile_fmt_filter` de app.py."""
    if not valor:
        return ""
    if valor.tzinfo is None:
        valor = valor.replace(tzinfo=dt.timezone.utc)
    return valor.astimezone(_tz()).strftime(fmt)


class Resp:
    """Lo que devuelve `jsonify` de mentira: datos + código + cabeceras."""

    def __init__(self, data, status=200):
        self.data, self.status, self.headers = data, status, {}


class _G:
    """`flask.g` de mentira (con .get)."""

    def __init__(self, user=None, permisos=None):
        self.user = user
        self.permissions = permisos or {}

    def get(self, k, d=None):
        return getattr(self, k, d)


class BD:
    """Las tablas que tocan los endpoints, en memoria, con un reloj propio (UTC)."""

    def __init__(self):
        self.ctrl = {}
        self.log = []                 # filas de email_log
        self.plantillas = {}          # (modulo, estado) → {asunto, cuerpo, activo}
        self.auditoria = []
        self.llaves = {}              # modulo → {email_bloqueado, motivo}
        self.ahora = dt.datetime(2026, 10, 20, 18, 0, 0)
        self.columna_log = True
        self.global_on = True
        self.fallar_lectura = False
        self.sql = []
        self._id = 100

    def tick(self, segundos=60):
        self.ahora += dt.timedelta(seconds=segundos)
        return self.ahora

    def poner_log(self, clave, estado, hace_dias=0.0, evento="x", dest="cliente@ejemplo.cl"):
        """Una fila de email_log de MENTIRA. Su fecha es relativa al reloj REAL (UTC): el código mide «últimos 30 días» con él."""
        self._id += 1
        self.log.append({"id": self._id, "destinatario": dest, "asunto": "Asunto de ejemplo", "evento": evento, "estado": estado,
                         "correo_clave": clave, "created_at": dt.datetime.utcnow() - dt.timedelta(days=hace_dias)})
        return self._id


class _Cursor:
    def __init__(self, bd):
        self.bd, self.rowcount = bd, 0

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=()):
        bd = self.bd
        bd.sql.append((sql, params))
        s = " ".join(sql.split())
        if s.startswith("UPDATE comm_correo_control SET activo=%s, motivo=%s, actualizado_por=%s, actualizado_en=NOW() WHERE clave=%s AND actualizado_en <=> %s"):
            activo, motivo, usuario, clave, viejo = params
            fila = bd.ctrl.get(clave)
            if fila is None or fila["actualizado_en"] != viejo:
                self.rowcount = 0
                return
            fila.update(activo=activo, motivo=motivo, actualizado_por=usuario, actualizado_en=bd.tick())
            self.rowcount = 1
        elif s.startswith("INSERT IGNORE INTO comm_correo_control"):
            clave, activo, motivo, usuario = params
            if clave in bd.ctrl:
                self.rowcount = 0
                return
            bd.ctrl[clave] = {"clave": clave, "activo": activo, "motivo": motivo, "actualizado_por": usuario,
                              "actualizado_en": bd.tick()}
            self.rowcount = 1
        else:
            raise AssertionError("SQL inesperado: " + s[:120])


class _Conn:
    def __init__(self, bd):
        self.bd = bd

    def cursor(self):
        return _Cursor(self.bd)

    def commit(self):
        pass


class MundoPantalla:
    """Las funciones reales de la puerta y de los endpoints, sobre una base en memoria."""

    def __init__(self, superadmin=True, usuario="daniel.aguilar@sphs.cl", nombre="Daniel Aguilar", rol="superadmin"):
        self.bd = BD()
        self.enviados_reales = []         # lo que llegó a _send_ilus_email_real (correo real: NUNCA; es una lista)
        self.prints = []
        self.peticion = {"json": {}, "args": {}}
        self.g = _G({"username": usuario, "nombre": nombre, "role": rol, "id": 1}, {"superadmin": bool(superadmin)})
        bd = self.bd
        amb = {
            "json": json, "re": re, "datetime": dt.datetime, "timedelta": dt.timedelta, "wraps": wraps,
            "print": lambda *a, **k: self.prints.append(" ".join(str(x) for x in a)),
            "g": self.g, "_ccr": R, "_otc": OTC, "time": types.SimpleNamespace(time=__import__("time").time),
            "jsonify": lambda d: Resp(d),
            "request": types.SimpleNamespace(get_json=lambda silent=True: self.peticion["json"],
                                             args=types.SimpleNamespace(get=lambda k, d=None: self.peticion["args"].get(k, d))),
            "mysql_fetchall": self._fetchall, "mysql_fetchone": self._fetchone, "get_db": lambda: _Conn(bd),
            "chile_fmt_filter": chile_fmt, "current_username": lambda: nombre,
            "comm_is_enabled": lambda canal: bd.global_on,
            "_comm_killswitch_estado": lambda: {m: {"email_bloqueado": v.get("email_bloqueado", 0), "motivo": v.get("motivo", "")}
                                                for m, v in bd.llaves.items()},
            "_modulo_canal_bloqueado": lambda mod, canal: bool((bd.llaves.get(mod) or {}).get("email_bloqueado")),
            "_audit": lambda accion, **kw: bd.auditoria.append(
                {"ts": bd.ahora, "username": usuario, "action": accion, "target_type": kw.get("target_type"),
                 "target_id": kw.get("target_id"), "details": json.dumps(kw.get("details") or {}, ensure_ascii=False)}),
            "_email_log": self._email_log, "_email_log_norm_ref": lambda ref, asunto: (None, None, None),
            "_send_ilus_email_real": self._real, "_brand_subject": lambda t: "ILUS · " + t,
            "_comm_render_email_document": lambda titulo, cuerpo, subtitle="": (
                "<!doctype html><html><body style='font-family:Arial;margin:0;background:#f1f2f4'>"
                "<div style='background:#0a0a0a;color:#fff;padding:18px 22px;font-weight:800'>ILUS Fitness</div>"
                f"<div style='max-width:560px;margin:0 auto;background:#fff;padding:22px'><h2 style='margin:0 0 4px'>{titulo}</h2>"
                f"<div style='color:#6b7280;margin-bottom:14px'>{subtitle}</div>{cuerpo}</div></body></html>"),
            "_email_queue_kick": lambda: None, "_email_adjuntos_para_cola": lambda a: None,
            "_MODULOS_KILLSWITCH_VALIDOS": llaves_validas(),
        }
        _extraer_cacheado(amb)
        self.amb = amb
        # los endpoints reales llevan @_require_superadmin_api: se vuelve a aplicar el decorador real a cada uno
        self.rutas = {}
        for nombre_fn in ("comm_correos_listar", "comm_correos_cambiar", "comm_correos_vista_previa", "comm_correos_prueba",
                          "comm_correos_cambios"):
            self.rutas[nombre_fn] = amb["_require_superadmin_api"](amb[nombre_fn])

    # ── base de mentira ──
    def _fetchall(self, sql, params=()):
        bd = self.bd
        s = " ".join(sql.split())
        bd.sql.append((s, params))
        if bd.fallar_lectura:
            raise RuntimeError("la base no responde")
        if "FROM comm_correo_control" in s:
            return [dict(v) for v in bd.ctrl.values()]
        if "FROM email_log WHERE created_at >= %s AND correo_clave IS NOT NULL" in s:
            desde = params[0]
            grupos = {}
            for f in bd.log:
                if f["created_at"] < desde or not f["correo_clave"] or str(f["evento"]).startswith("prueba_"):
                    continue
                g = grupos.setdefault((f["correo_clave"], f["estado"]), {"n": 0, "ultimo": None, "primero": None})
                g["n"] += 1
                g["ultimo"] = max(g["ultimo"] or f["created_at"], f["created_at"])
                g["primero"] = min(g["primero"] or f["created_at"], f["created_at"])
            return [{"correo_clave": k[0], "estado": k[1], **v} for k, v in grupos.items()]
        if "FROM comm_templates WHERE canal='email'" in s:
            return [{"modulo": m, "estado": e, "activo": v.get("activo", 1),
                     "tiene": 1 if (v.get("asunto") or v.get("cuerpo")) else 0} for (m, e), v in bd.plantillas.items()]
        if "FROM app_audit_log WHERE action='comm_correo_set'" in s:
            clave = params[0]
            filas = [a for a in bd.auditoria if a["action"] == "comm_correo_set" and a["target_id"] == clave]
            return sorted(filas, key=lambda a: a["ts"], reverse=True)[:5]
        raise AssertionError("consulta inesperada: " + s[:140])

    def _fetchone(self, sql, params=()):
        bd = self.bd
        s = " ".join(sql.split())
        bd.sql.append((s, params))
        if bd.fallar_lectura:
            raise RuntimeError("la base no responde")
        if "information_schema.COLUMNS" in s and "correo_clave" in s:
            return {"x": 1} if bd.columna_log else None
        if "FROM comm_templates" in s and "LIMIT 1" in s:
            v = bd.plantillas.get((params[0], params[1]))
            return None if not v else {"asunto": v.get("asunto"), "cuerpo": v.get("cuerpo"), "activo": v.get("activo", 1)}
        if "FROM email_log WHERE correo_clave=%s" in s:
            filas = [f for f in bd.log if f["correo_clave"] == params[0] and not str(f["evento"]).startswith("prueba_")]
            return {"id": filas[-1]["id"]} if filas else None
        raise AssertionError("consulta inesperada: " + s[:140])

    def _email_log(self, destinatario, asunto, evento, estado, error_msg=None, metadata=None, *, modulo=None, ref=None, html=None,
                   adjuntos=None, proveedor=None, job_id=None, actor=None, correo=None):
        self.bd._id += 1
        self.bd.log.append({"id": self.bd._id, "destinatario": destinatario, "asunto": asunto, "evento": evento, "estado": estado,
                            "correo_clave": correo, "created_at": self.bd.ahora, "error_msg": error_msg, "metadata": metadata,
                            "html": html, "modulo": modulo, "actor": actor})
        return self.bd._id

    def _real(self, to, asunto, html, **kw):
        self.enviados_reales.append({"to": to, "asunto": asunto, "html": html, **kw})
        return True

    # ── atajos ──
    def llamar(self, ruta, *args, json_body=None, args_qs=None):
        """Ejecuta un endpoint ya decorado y devuelve (status, datos)."""
        self.peticion = {"json": json_body if json_body is not None else {}, "args": args_qs or {}}
        r = self.rutas[ruta](*args)
        if isinstance(r, tuple):
            resp, status = r[0], r[1]
        else:
            resp, status = r, getattr(r, "status", 200)
        return status, resp.data, resp

    def enviar(self, correo=None, *, modulo="retiros", evento="pickup_created", to="cliente@ejemplo.cl", **kw):
        """Un envío por la PUERTA ÚNICA REAL (`_send_ilus_email` de app.py). Nunca sale nada: el envío real es de mentira."""
        return self.amb["_send_ilus_email"](to, "Asunto de mentira", "<p>Cuerpo de mentira</p>", evento=evento, modulo=modulo,
                                            correo=correo, **kw)

    def poner_plantilla(self, modulo, estado, asunto, cuerpo, activo=1):
        self.bd.plantillas[(modulo, estado)] = {"asunto": asunto, "cuerpo": cuerpo, "activo": activo}

    def sembrar_plantillas_de_ejemplo(self):
        """Una plantilla de ejemplo para cada correo del registro que tiene plantilla editable (con variables)."""
        for c in R.REGISTRO:
            if c["plantilla"] and c["plantilla"] not in self.bd.plantillas:
                modulo, estado = c["plantilla"]
                self.poner_plantilla(
                    modulo, estado, f"{c['nombre']} — {{{{ot}}}}{{{{code}}}}",
                    f"<p>Hola <b>{{{{nombre_cliente}}}}</b>,</p><p>{c['disparo']}</p>"
                    "<p>Documento {{documento}} · Fecha {{fecha}} · Ticket {{numero_ticket}} · Variable que no existe: {{no_existe}}</p>")

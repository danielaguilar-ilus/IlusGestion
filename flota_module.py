"""Módulo FLOTA · «Solicitudes de vehículo» de ILUS Fitness.

Pedido de Daniel (02-nov-2026): ordenar las solicitudes de vehículos, registrar sus datos, hacerles seguimiento calendarizado e integrar su
GPS. En el menú va UN solo ítem, «Solicitudes de vehículo», y adentro viven la gestión de cada vehículo, los choferes y las reservas.

FASE 1a (este archivo): registro de los vehículos propios (alta, edición, habilitar / inhabilitar con motivo) con sus datos, los 4
documentos con vencimiento y la bitácora de cada cambio. La flota NO se fija en el código: cada vehículo nuevo se ingresa a mano desde la
pantalla. Este módulo no maneja precios ni costos. Siguen: choferes, solicitudes con calendario, mantención y GPS.

HOY SOLO LO VE EL SUPERADMIN (Daniel: «quiero solo verlo yo de momento»): ningún rol tiene permiso y el ítem del menú no aparece para nadie
más. Cuando se abra a otros roles se cambia SOLO `_flota_puede()`.

Wiring idéntico al de catalogo_module.py / tickets_module.py:
    from flota_module import register_flota_routes
    register_flota_routes(app, globals())

Las tablas se crean al registrar el módulo, dentro de app_context (si no, mysql_execute falla en silencio: ver el gotcha de los _ensure_*).
REGLA #18: una sentencia por DDL, `CREATE TABLE IF NOT EXISTS`, sin ALTER sobre tablas de otros módulos.
"""
import json
from datetime import date, datetime, timezone
from functools import wraps

from flask import g, jsonify, redirect, render_template, request, url_for

import flota_reglas as fr

try:
    from zoneinfo import ZoneInfo
    _TZ_CL = ZoneInfo("America/Santiago")
except Exception:  # pragma: no cover  (sin tzdata)
    _TZ_CL = None


def register_flota_routes(app, ctx):
    mysql_fetchone = ctx["mysql_fetchone"]
    mysql_fetchall = ctx["mysql_fetchall"]
    mysql_execute = ctx["mysql_execute"]
    mysql_rowcount = ctx["mysql_execute_returning_rowcount"]
    login_required = ctx["login_required"]
    current_username = ctx.get("current_username") or (lambda: None)

    # ── Tablas ──────────────────────────────────────────────────────────────
    def _ensure_flota_tables():
        try:
            mysql_execute("""
                CREATE TABLE IF NOT EXISTS flota_vehiculos (
                  id                         INT AUTO_INCREMENT PRIMARY KEY,
                  patente                    VARCHAR(12)  NOT NULL,
                  tipo                       VARCHAR(60)  NOT NULL,
                  marca                      VARCHAR(80)  NULL,
                  modelo                     VARCHAR(80)  NULL,
                  anio                       SMALLINT     NULL,
                  color                      VARCHAR(40)  NULL,
                  vin                        VARCHAR(40)  NULL,
                  capacidad_kg               INT          NULL,
                  capacidad_m3               DECIMAL(6,2) NULL,
                  combustible                VARCHAR(30)  NULL,
                  estanque_l                 INT          NULL,
                  km_actual                  INT          NULL,
                  licencia_clases            VARCHAR(40)  NOT NULL DEFAULT 'B',
                  revision_tecnica_vence     DATE         NULL,
                  permiso_circulacion_vence  DATE         NULL,
                  soap_vence                 DATE         NULL,
                  seguro_vence               DATE         NULL,
                  notas                      TEXT         NULL,
                  habilitado                 TINYINT(1)   NOT NULL DEFAULT 1,
                  inhabilitado_motivo        VARCHAR(300) NULL,
                  inhabilitado_por           VARCHAR(190) NULL,
                  inhabilitado_at            DATETIME     NULL,
                  created_by                 VARCHAR(190) NULL,
                  updated_by                 VARCHAR(190) NULL,
                  created_at                 DATETIME DEFAULT CURRENT_TIMESTAMP,
                  updated_at                 DATETIME DEFAULT CURRENT_TIMESTAMP
                                                ON UPDATE CURRENT_TIMESTAMP,
                  UNIQUE KEY uq_flota_patente (patente),
                  KEY idx_flota_habilitado (habilitado)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            # Bitácora: se AGREGA, nunca se modifica ni se borra (hay una prueba que lo vigila).
            mysql_execute("""
                CREATE TABLE IF NOT EXISTS flota_vehiculo_log (
                  id           INT AUTO_INCREMENT PRIMARY KEY,
                  vehiculo_id  INT          NOT NULL,
                  accion       VARCHAR(30)  NOT NULL,
                  detalle      TEXT         NULL,
                  usuario      VARCHAR(190) NULL,
                  created_at   DATETIME DEFAULT CURRENT_TIMESTAMP,
                  KEY idx_flota_log_veh (vehiculo_id, id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
        except Exception as e:
            print(f"[ILUS][WARN] _ensure_flota_tables: {type(e).__name__}: {e}", flush=True)

    with app.app_context():
        _ensure_flota_tables()
        print("[ILUS] Tablas flota_* garantizadas (Solicitudes de vehículo).", flush=True)

    # ── Acceso ──────────────────────────────────────────────────────────────
    def _flota_puede():
        """HOY solo el superadmin (Daniel 02-nov-2026). Es el ÚNICO lugar que decide quién entra: para abrirlo a un rol se cambia acá."""
        return bool((g.get("permissions") or {}).get("superadmin"))

    def _es_api():
        return request.path.startswith("/flota/api/")

    def _flota_required(view):
        @wraps(view)
        def wrapped(*a, **k):
            if not _flota_puede():
                if _es_api():
                    return jsonify({"ok": False, "error": "Esta sección todavía no está habilitada para tu usuario.",
                                    "error_codigo": "SIN_PERMISO_FLOTA"}), 403
                return redirect(url_for("index"))
            return view(*a, **k)
        return login_required(wrapped)

    # ── Utilidades ──────────────────────────────────────────────────────────
    def _hoy():
        try:
            return datetime.now(_TZ_CL).date() if _TZ_CL else date.today()
        except Exception:
            return date.today()

    def _hora_chile(dt):
        """Los DATETIME se guardan en UTC (NOW()): se muestran en hora Chile, dd/mm/aaaa hh:mm (REGLA #6)."""
        if not dt:
            return ""
        try:
            if isinstance(dt, str):
                dt = datetime.fromisoformat(dt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            if _TZ_CL:
                dt = dt.astimezone(_TZ_CL)
            return dt.strftime("%d/%m/%Y %H:%M")
        except Exception:
            return ""

    def _num(v):
        if v is None:
            return None
        f = float(v)
        return int(f) if f.is_integer() else f

    def _a_json(row, hoy):
        docs = fr.resumen_documentos(row, hoy)
        clases = fr.normalizar_clases(row.get("licencia_clases"))
        titulo = " ".join(x for x in (row.get("tipo"), row.get("marca"), row.get("modelo"),
                                      str(row["anio"]) if row.get("anio") else None) if x)
        out = {
            "id": row["id"], "patente": row["patente"], "patente_fmt": fr.patente_con_guion(row["patente"]),
            "tipo": row.get("tipo"), "marca": row.get("marca"), "modelo": row.get("modelo"), "anio": row.get("anio"),
            "color": row.get("color"), "vin": row.get("vin"), "titulo": titulo,
            "capacidad_kg": _num(row.get("capacidad_kg")), "capacidad_m3": _num(row.get("capacidad_m3")),
            "combustible": row.get("combustible"), "estanque_l": _num(row.get("estanque_l")), "km_actual": _num(row.get("km_actual")),
            "licencia_clases": clases, "licencia_texto": ("Clase " + " o ".join(clases)) if clases else "Sin definir",
            "documentos": docs, "docs_estado": fr.peor_estado(docs),
            "notas": row.get("notas"), "habilitado": bool(row.get("habilitado")),
            "inhabilitado_motivo": row.get("inhabilitado_motivo"), "inhabilitado_por": row.get("inhabilitado_por"),
            "inhabilitado_fmt": _hora_chile(row.get("inhabilitado_at")),
            "creado_por": row.get("created_by"), "creado_fmt": _hora_chile(row.get("created_at")),
            "actualizado_por": row.get("updated_by"), "actualizado_fmt": _hora_chile(row.get("updated_at")),
        }
        for clave, _n in fr.DOCUMENTOS:
            v = row.get(clave + "_vence")
            out[clave + "_vence"] = v.isoformat() if isinstance(v, (date, datetime)) else (v or None)
        return out

    def _config():
        return {
            "tipos": list(fr.TIPOS_SUGERIDOS), "combustibles": list(fr.COMBUSTIBLES_SUGERIDOS),
            "clases": [{"codigo": c, "nombre": fr.LICENCIA_CLASES[c]} for c in fr.ORDEN_CLASES],
            "clase_sugerida": "B", "dias_alerta": fr.DIAS_ALERTA,
        }

    def _log(vid, accion, detalle=None):
        """Agrega una línea a la bitácora. Nunca rompe la acción principal."""
        try:
            mysql_execute(
                "INSERT INTO flota_vehiculo_log (vehiculo_id, accion, detalle, usuario) VALUES (%s,%s,%s,%s)",
                (vid, accion, json.dumps(detalle, ensure_ascii=False, default=str) if detalle is not None else None,
                 (current_username() or "")[:190] or None))
        except Exception as e:
            print(f"[flota] bitácora ({accion}): {type(e).__name__}", flush=True)

    def _es_duplicado(exc):
        a = getattr(exc, "args", ())
        return bool(a and a[0] == 1062) or "Duplicate entry" in str(exc)

    def _fila(vid):
        return mysql_fetchone("SELECT * FROM flota_vehiculos WHERE id=%s", (vid,))

    def _error(msg, status=400, **extra):
        return jsonify({"ok": False, "error": msg, **extra}), status

    # ── Página ──────────────────────────────────────────────────────────────
    @app.route("/flota")
    @_flota_required
    def flota_index():
        return render_template("flota/index.html")

    # ── API ─────────────────────────────────────────────────────────────────
    @app.route("/flota/api/vehiculos", methods=["GET"])
    @_flota_required
    def flota_api_vehiculos():
        try:
            filas = mysql_fetchall("SELECT * FROM flota_vehiculos ORDER BY habilitado DESC, tipo, patente LIMIT 500") or []
        except Exception as e:
            print(f"[flota] listar: {type(e).__name__}: {e}", flush=True)
            return _error("No se pudo leer la flota. Intenta de nuevo en un momento.", 500)
        hoy = _hoy()
        vehiculos = [_a_json(dict(f), hoy) for f in filas]
        resumen = {
            "total": len(vehiculos),
            "habilitados": sum(1 for v in vehiculos if v["habilitado"]),
            "inhabilitados": sum(1 for v in vehiculos if not v["habilitado"]),
            "por_vencer": sum(1 for v in vehiculos if v["docs_estado"] == "por_vencer"),
            "vencidos": sum(1 for v in vehiculos if v["docs_estado"] == "vencido"),
        }
        return jsonify({"ok": True, "vehiculos": vehiculos, "resumen": resumen, "config": _config(), "hoy": hoy.isoformat()})

    @app.route("/flota/api/vehiculos", methods=["POST"])
    @_flota_required
    def flota_api_vehiculo_crear():
        datos, errores = fr.validar_vehiculo(request.get_json(silent=True) or {}, _hoy())
        if errores:
            return _error("Revisa los campos marcados.", 400, errores=errores)
        cols = fr.COLUMNAS
        try:
            mysql_execute(
                "INSERT INTO flota_vehiculos (" + ", ".join(cols) + ", created_by, updated_by) VALUES ("
                + ", ".join(["%s"] * len(cols)) + ", %s, %s)",
                tuple(datos[c] for c in cols) + (current_username(), current_username()))
        except Exception as e:
            if _es_duplicado(e):
                return _error(f"Ya existe un vehículo con la patente {fr.patente_con_guion(datos['patente'])}.", 409,
                              errores={"patente": "Esta patente ya está registrada."})
            print(f"[flota] crear: {type(e).__name__}: {e}", flush=True)
            return _error("No se pudo guardar el vehículo. Intenta de nuevo.", 500)
        fila = mysql_fetchone("SELECT * FROM flota_vehiculos WHERE patente=%s", (datos["patente"],))
        if not fila:
            return _error("El vehículo se guardó pero no se pudo leer de vuelta. Recarga la pantalla.", 500)
        _log(fila["id"], "creado", {"patente": datos["patente"], "tipo": datos["tipo"]})
        return jsonify({"ok": True, "vehiculo": _a_json(dict(fila), _hoy())})

    @app.route("/flota/api/vehiculos/<int:vid>", methods=["PUT"])
    @_flota_required
    def flota_api_vehiculo_editar(vid):
        actual = _fila(vid)
        if not actual:
            return _error("No encontré ese vehículo.", 404)
        datos, errores = fr.validar_vehiculo(request.get_json(silent=True) or {}, _hoy())
        if errores:
            return _error("Revisa los campos marcados.", 400, errores=errores)
        cambios = fr.cambios(dict(actual), datos)
        if not cambios:
            return jsonify({"ok": True, "sin_cambios": True, "vehiculo": _a_json(dict(actual), _hoy())})
        cols = fr.COLUMNAS
        try:
            mysql_execute(
                "UPDATE flota_vehiculos SET " + ", ".join(c + "=%s" for c in cols) + ", updated_by=%s WHERE id=%s",
                tuple(datos[c] for c in cols) + (current_username(), vid))
        except Exception as e:
            if _es_duplicado(e):
                return _error(f"Ya existe otro vehículo con la patente {fr.patente_con_guion(datos['patente'])}.", 409,
                              errores={"patente": "Esta patente ya está registrada."})
            print(f"[flota] editar: {type(e).__name__}: {e}", flush=True)
            return _error("No se pudo guardar los cambios. Intenta de nuevo.", 500)
        _log(vid, "editado", {"cambios": cambios})
        return jsonify({"ok": True, "vehiculo": _a_json(dict(_fila(vid) or actual), _hoy())})

    @app.route("/flota/api/vehiculos/<int:vid>/inhabilitar", methods=["POST"])
    @_flota_required
    def flota_api_vehiculo_inhabilitar(vid):
        if not _fila(vid):
            return _error("No encontré ese vehículo.", 404)
        motivo, err = fr.validar_motivo((request.get_json(silent=True) or {}).get("motivo"))
        if err:
            return _error(err, 400, errores={"motivo": err})
        # Atómico: si otra persona ya lo inhabilitó, esta llamada no pisa su motivo.
        n = mysql_rowcount(
            "UPDATE flota_vehiculos SET habilitado=0, inhabilitado_motivo=%s, inhabilitado_por=%s, inhabilitado_at=NOW(), updated_by=%s "
            "WHERE id=%s AND habilitado=1", (motivo, current_username(), current_username(), vid))
        if not n:
            return _error("Ese vehículo ya estaba inhabilitado.", 409)
        _log(vid, "inhabilitado", {"motivo": motivo})
        return jsonify({"ok": True, "vehiculo": _a_json(dict(_fila(vid)), _hoy())})

    @app.route("/flota/api/vehiculos/<int:vid>/habilitar", methods=["POST"])
    @_flota_required
    def flota_api_vehiculo_habilitar(vid):
        previo = _fila(vid)
        if not previo:
            return _error("No encontré ese vehículo.", 404)
        n = mysql_rowcount(
            "UPDATE flota_vehiculos SET habilitado=1, inhabilitado_motivo=NULL, inhabilitado_por=NULL, inhabilitado_at=NULL, updated_by=%s "
            "WHERE id=%s AND habilitado=0", (current_username(), vid))
        if not n:
            return _error("Ese vehículo ya estaba habilitado.", 409)
        _log(vid, "habilitado", {"motivo_anterior": previo.get("inhabilitado_motivo")})
        v = _a_json(dict(_fila(vid)), _hoy())
        # Alertar, no trabar: se habilita igual, pero se dice qué documentos conviene mirar.
        avisos = [f"{d['nombre']}: {d['texto'].lower()}" for d in v["documentos"] if d["estado"] in ("vencido", "por_vencer")]
        return jsonify({"ok": True, "vehiculo": v, "avisos": avisos})

    @app.route("/flota/api/vehiculos/<int:vid>/historial", methods=["GET"])
    @_flota_required
    def flota_api_vehiculo_historial(vid):
        if not _fila(vid):
            return _error("No encontré ese vehículo.", 404)
        filas = mysql_fetchall(
            "SELECT id, accion, detalle, usuario, created_at FROM flota_vehiculo_log WHERE vehiculo_id=%s ORDER BY id DESC LIMIT 200",
            (vid,)) or []
        titulos = {"creado": "Vehículo ingresado", "editado": "Datos modificados", "inhabilitado": "Inhabilitado", "habilitado": "Habilitado"}
        eventos = []
        for f in filas:
            try:
                det = json.loads(f.get("detalle") or "null") or {}
            except Exception:
                det = {}
            if f["accion"] == "editado":
                lineas = fr.lineas_de_cambios(det.get("cambios"))
            elif f["accion"] == "inhabilitado":
                lineas = ["Motivo: " + (det.get("motivo") or "(sin motivo)")]
            elif f["accion"] == "habilitado":
                lineas = (["Estaba inhabilitado por: " + det["motivo_anterior"]] if det.get("motivo_anterior") else [])
            else:
                lineas = []
            eventos.append({"accion": f["accion"], "titulo": titulos.get(f["accion"], f["accion"]), "lineas": lineas,
                            "usuario": f.get("usuario") or "", "fecha": _hora_chile(f.get("created_at"))})
        return jsonify({"ok": True, "eventos": eventos})

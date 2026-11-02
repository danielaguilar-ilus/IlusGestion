"""Módulo FLOTA · «Solicitudes de vehículo» de ILUS Fitness.

Pedido de Daniel (02-nov-2026): ordenar las solicitudes de vehículos, registrar sus datos, hacerles seguimiento calendarizado e integrar su
GPS. En el menú va UN solo ítem, «Solicitudes de vehículo», y adentro viven la gestión de cada vehículo, los choferes y las reservas.

FASE 1a: registro de los vehículos propios (alta, edición, habilitar / inhabilitar con motivo) con sus datos, los 4 documentos con
vencimiento y la bitácora de cada cambio. La flota NO se fija en el código: cada vehículo nuevo se ingresa a mano desde la pantalla.
FASE 1b: repositorio de choferes (cada uno es un usuario de ILUS, con su licencia) y solicitudes de uso: cualquier área reserva un
vehículo declarando día, horario, motivo y el chofer que retira la llave; la gestión confirma y marca la llave entregada; el sello
decide los choques; lista y calendario. Este módulo no maneja precios ni costos. Siguen: devolución, mantención y GPS.

HOY SOLO LO VE EL SUPERADMIN (Daniel: «quiero solo verlo yo de momento»): ningún rol tiene permiso y el ítem del menú no aparece para nadie
más. Cuando se abra a otros roles se cambia SOLO `_flota_puede()`.

Wiring idéntico al de catalogo_module.py / tickets_module.py:
    from flota_module import register_flota_routes
    register_flota_routes(app, globals())

Las tablas se crean al registrar el módulo, dentro de app_context (si no, mysql_execute falla en silencio: ver el gotcha de los _ensure_*).
REGLA #18: una sentencia por DDL, `CREATE TABLE IF NOT EXISTS`, sin ALTER sobre tablas de otros módulos.
"""
import json
import re
from datetime import date, datetime, time, timedelta, timezone
from functools import wraps

from flask import g, jsonify, redirect, render_template, request, url_for

import flota_reglas as fr

try:
    import cl_feriados          # fuente única de feriados de Chile (REGLA #31)
except Exception:  # pragma: no cover
    cl_feriados = None

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
    get_mysql = ctx.get("get_mysql")          # conexión con transacción: la usa la reserva (bloquea el vehículo mientras decide)
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
            # Fase 1b. Cada chofer es un usuario de ILUS (usuario_id): su nombre y teléfono salen del perfil; acá solo su licencia.
            mysql_execute("""
                CREATE TABLE IF NOT EXISTS flota_choferes (
                  id                   INT AUTO_INCREMENT PRIMARY KEY,
                  usuario_id           INT          NOT NULL,
                  licencia_clases      VARCHAR(40)  NOT NULL DEFAULT 'B',
                  licencia_vence       DATE         NULL,
                  notas                TEXT         NULL,
                  habilitado           TINYINT(1)   NOT NULL DEFAULT 1,
                  inhabilitado_motivo  VARCHAR(300) NULL,
                  inhabilitado_por     VARCHAR(190) NULL,
                  inhabilitado_at      DATETIME     NULL,
                  created_by           VARCHAR(190) NULL,
                  updated_by           VARCHAR(190) NULL,
                  created_at           DATETIME DEFAULT CURRENT_TIMESTAMP,
                  updated_at           DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                  UNIQUE KEY uq_flota_chofer_usuario (usuario_id),
                  KEY idx_flota_chofer_hab (habilitado)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            mysql_execute("""
                CREATE TABLE IF NOT EXISTS flota_chofer_log (
                  id          INT AUTO_INCREMENT PRIMARY KEY,
                  chofer_id   INT          NOT NULL,
                  accion      VARCHAR(30)  NOT NULL,
                  detalle     TEXT         NULL,
                  usuario     VARCHAR(190) NULL,
                  created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
                  KEY idx_flota_chlog (chofer_id, id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            # Solicitudes de uso. `inicio` y `fin` son la hora de Chile que la persona declaró (sin zona); `sello_at` es la hora del
            # servidor en UTC (NOW()), la que decide quién llegó primero.
            mysql_execute("""
                CREATE TABLE IF NOT EXISTS flota_solicitudes (
                  id                  INT AUTO_INCREMENT PRIMARY KEY,
                  numero              VARCHAR(24)  NULL,
                  vehiculo_id         INT          NOT NULL,
                  area                VARCHAR(60)  NOT NULL,
                  solicitante_id      INT          NULL,
                  solicitante_nombre  VARCHAR(190) NULL,
                  chofer_id           INT          NOT NULL,
                  inicio              DATETIME     NOT NULL,
                  fin                 DATETIME     NOT NULL,
                  motivo              VARCHAR(500) NOT NULL,
                  estado              VARCHAR(20)  NOT NULL DEFAULT 'solicitada',
                  sello_at            DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  confirmada_por      VARCHAR(190) NULL,
                  confirmada_at       DATETIME     NULL,
                  llave_por           VARCHAR(190) NULL,
                  llave_at            DATETIME     NULL,
                  llave_nota          VARCHAR(300) NULL,
                  cancelada_por       VARCHAR(190) NULL,
                  cancelada_at        DATETIME     NULL,
                  cancelada_motivo    VARCHAR(300) NULL,
                  modificada_por      VARCHAR(190) NULL,
                  modificada_at       DATETIME     NULL,
                  updated_at          DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                  UNIQUE KEY uq_flota_sol_numero (numero),
                  KEY idx_flota_sol_veh (vehiculo_id, inicio),
                  KEY idx_flota_sol_estado (estado, inicio),
                  KEY idx_flota_sol_chofer (chofer_id, inicio)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            mysql_execute("""
                CREATE TABLE IF NOT EXISTS flota_solicitud_log (
                  id            INT AUTO_INCREMENT PRIMARY KEY,
                  solicitud_id  INT          NOT NULL,
                  accion        VARCHAR(30)  NOT NULL,
                  detalle       TEXT         NULL,
                  usuario       VARCHAR(190) NULL,
                  created_at    DATETIME DEFAULT CURRENT_TIMESTAMP,
                  KEY idx_flota_sollog (solicitud_id, id)
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
            "areas": list(fr.AREAS_SUGERIDAS), "max_dias_uso": fr.MAX_DIAS_USO,
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
        v = _a_json(dict(_fila(vid)), _hoy())
        # Alertar, no trabar: se inhabilita igual, pero se dice si tenía reservas por delante.
        return jsonify({"ok": True, "vehiculo": v, "avisos": _aviso_reservas(_reservas_abiertas("vehiculo", vid), v["titulo"] or v["patente_fmt"])})

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

    # ═══════════════════════════════════════════════════════════════════════
    #  FASE 1b · CHOFERES Y SOLICITUDES DE USO
    #  Flujo (Daniel, 02-nov-2026): 1) cualquier área pide el vehículo declarando día, horario, motivo y el chofer que retira la llave;
    #  2) la gestión (Aarón) confirma y marca «llave entregada»; el SELLO (hora del servidor) decide si dos pedidos se pisan: no hay quien
    #  apruebe un choque. Es una reserva: este módulo no maneja precios ni costos. La devolución (km, bencina, fotos, firma) y el GPS
    #  vienen en las fases siguientes.
    # ═══════════════════════════════════════════════════════════════════════
    # Nombre de la tabla de usuarios: constante de configuración (nunca texto del usuario), por eso se concatena en el SQL.
    _AUTH = str(ctx.get("AUTH_TABLE") or "app_users")
    if not re.fullmatch(r"[A-Za-z0-9_]+", _AUTH):
        _AUTH = "app_users"

    _SQL_CHOF = (
        "SELECT c.*, u.nombre AS u_nombre, u.username AS u_username, u.phone AS u_phone, u.cargo AS u_cargo, u.active AS u_activo "
        "FROM flota_choferes c LEFT JOIN `" + _AUTH + "` u ON u.id = c.usuario_id")
    _SQL_SOL = (
        "SELECT s.*, v.patente AS v_patente, v.tipo AS v_tipo, v.marca AS v_marca, v.modelo AS v_modelo, "
        "u.nombre AS chofer_nombre, u.phone AS chofer_phone "
        "FROM flota_solicitudes s JOIN flota_vehiculos v ON v.id = s.vehiculo_id "
        "LEFT JOIN flota_choferes c ON c.id = s.chofer_id LEFT JOIN `" + _AUTH + "` u ON u.id = c.usuario_id")
    _SQL_OCUPA = (
        "SELECT s.id, s.numero, s.estado, s.inicio, s.fin, s.area, s.solicitante_nombre, s.sello_at, s.vehiculo_id, s.chofer_id, "
        "v.patente AS v_patente FROM flota_solicitudes s JOIN flota_vehiculos v ON v.id = s.vehiculo_id")

    def _flota_gestiona():
        """Quién gestiona la flota (Aarón: confirma, entrega la llave, mueve y cancela). HOY es la misma puerta que `_flota_puede`; cuando
        exista el rol de responsable de flota se cambia SOLO esta función."""
        return _flota_puede()

    def _flota_gestion_required(view):
        @wraps(view)
        def wrapped(*a, **k):
            if not _flota_gestiona():
                if _es_api():
                    return jsonify({"ok": False, "error": "Solo quien gestiona la flota puede hacer esto.",
                                    "error_codigo": "SIN_PERMISO_GESTION_FLOTA"}), 403
                return redirect(url_for("index"))
            return view(*a, **k)
        return login_required(wrapped)

    def _ahora_chile():
        """La hora de Chile como fecha-hora SIN zona (así se guardan inicio y fin de cada reserva: lo que la persona declaró)."""
        try:
            return datetime.now(_TZ_CL).replace(tzinfo=None) if _TZ_CL else datetime.now()
        except Exception:
            return datetime.now()

    def _uid():
        try:
            return int(g.user["id"])
        except Exception:
            return None

    def _hhmm(d):
        return d.strftime("%H:%M") if isinstance(d, datetime) else ""

    def _dia_iso(d):
        return d.date().isoformat() if isinstance(d, datetime) else None

    # ── Choferes ────────────────────────────────────────────────────────────
    def _chofer_json(r, hoy):
        clases = fr.normalizar_clases(r.get("licencia_clases"))
        try:
            vence = fr.fecha_o_none(r.get("licencia_vence"))
        except ValueError:
            vence = None
        estado = fr.estado_vencimiento(vence, hoy) or "sin_fecha"
        dias = (vence - hoy).days if vence else None
        activo = bool(r.get("u_activo")) if r.get("u_activo") is not None else False
        return {
            "id": r["id"], "usuario_id": r.get("usuario_id"),
            "nombre": r.get("u_nombre") or "(usuario sin nombre)", "username": r.get("u_username"),
            "telefono": r.get("u_phone"), "cargo": r.get("u_cargo"), "usuario_activo": activo,
            "licencia_clases": clases, "licencia_texto": ("Clase " + ", ".join(clases)) if clases else "Sin definir",
            "licencia_vence": vence.isoformat() if vence else None, "licencia_vence_fmt": fr.fecha_corta(vence),
            "licencia_estado": estado, "licencia_dias": dias,
            "licencia_texto_dias": fr.texto_dias(dias) if vence else "Sin fecha registrada",
            "notas": r.get("notas"), "habilitado": bool(r.get("habilitado")),
            "inhabilitado_motivo": r.get("inhabilitado_motivo"), "inhabilitado_por": r.get("inhabilitado_por"),
            "inhabilitado_fmt": _hora_chile(r.get("inhabilitado_at")),
            "creado_por": r.get("created_by"), "creado_fmt": _hora_chile(r.get("created_at")),
            "actualizado_por": r.get("updated_by"), "actualizado_fmt": _hora_chile(r.get("updated_at")),
            # Puede recibir una reserva hoy: habilitado, con usuario activo y la licencia sin vencer.
            "disponible": bool(r.get("habilitado")) and activo and estado not in ("vencido", "sin_fecha"),
        }

    def _chofer_fila(cid):
        return mysql_fetchone(_SQL_CHOF + " WHERE c.id=%s", (cid,))

    def _clog(cid, accion, detalle=None):
        try:
            mysql_execute(
                "INSERT INTO flota_chofer_log (chofer_id, accion, detalle, usuario) VALUES (%s,%s,%s,%s)",
                (cid, accion, json.dumps(detalle, ensure_ascii=False, default=str) if detalle is not None else None,
                 (current_username() or "")[:190] or None))
        except Exception as e:
            print(f"[flota] bitácora de chofer ({accion}): {type(e).__name__}", flush=True)

    def _reservas_abiertas(campo, ident):
        """Cuántas reservas vivas (que aún no terminan) tiene un vehículo o un chofer: sirve para AVISAR al inhabilitarlo, no para trabar."""
        sql = ("SELECT COUNT(*) AS n FROM flota_solicitudes WHERE vehiculo_id=%s AND fin >= %s AND estado IN "
               "('solicitada','confirmada','llave_entregada')") if campo == "vehiculo" else (
              "SELECT COUNT(*) AS n FROM flota_solicitudes WHERE chofer_id=%s AND fin >= %s AND estado IN "
              "('solicitada','confirmada','llave_entregada')")
        try:
            r = mysql_fetchone(sql, (ident, _ahora_chile())) or {}
            return int(r.get("n") or 0)
        except Exception:
            return 0

    def _aviso_reservas(n, quien):
        if not n:
            return []
        return [f"{quien} tiene {n} reserva{'s' if n != 1 else ''} pendiente{'s' if n != 1 else ''}: revísalas en Solicitudes."]

    @app.route("/flota/api/choferes", methods=["GET"])
    @_flota_required
    def flota_api_choferes():
        try:
            filas = mysql_fetchall(_SQL_CHOF + " ORDER BY c.habilitado DESC, u.nombre LIMIT 500") or []
        except Exception as e:
            print(f"[flota] listar choferes: {type(e).__name__}: {e}", flush=True)
            return _error("No se pudo leer el repositorio de choferes. Intenta de nuevo en un momento.", 500)
        hoy = _hoy()
        choferes = [_chofer_json(dict(f), hoy) for f in filas]
        resumen = {
            "total": len(choferes),
            "habilitados": sum(1 for c in choferes if c["habilitado"]),
            "inhabilitados": sum(1 for c in choferes if not c["habilitado"]),
            "por_vencer": sum(1 for c in choferes if c["habilitado"] and c["licencia_estado"] == "por_vencer"),
            "vencidas": sum(1 for c in choferes if c["habilitado"] and c["licencia_estado"] == "vencido"),
        }
        return jsonify({"ok": True, "choferes": choferes, "resumen": resumen, "config": _config(), "hoy": hoy.isoformat()})

    @app.route("/flota/api/choferes/usuarios", methods=["GET"])
    @_flota_gestion_required
    def flota_api_choferes_usuarios():
        """Usuarios de ILUS que todavía no son chofer: de ahí se elige a quién dar de alta."""
        try:
            filas = mysql_fetchall(
                "SELECT id, nombre, username, phone, cargo FROM `" + _AUTH + "` WHERE active=1 AND id NOT IN "
                "(SELECT usuario_id FROM flota_choferes) ORDER BY nombre LIMIT 1000") or []
        except Exception as e:
            print(f"[flota] usuarios para chofer: {type(e).__name__}: {e}", flush=True)
            return _error("No se pudo leer la lista de usuarios. Intenta de nuevo en un momento.", 500)
        return jsonify({"ok": True, "usuarios": [
            {"id": f["id"], "nombre": f.get("nombre") or f.get("username"), "username": f.get("username"),
             "telefono": f.get("phone"), "cargo": f.get("cargo")} for f in filas]})

    @app.route("/flota/api/choferes", methods=["POST"])
    @_flota_gestion_required
    def flota_api_chofer_crear():
        datos, errores = fr.validar_chofer(request.get_json(silent=True) or {})
        if errores:
            return _error("Revisa los campos marcados.", 400, errores=errores)
        u = mysql_fetchone("SELECT id, nombre, active FROM `" + _AUTH + "` WHERE id=%s", (datos["usuario_id"],))
        if not u or not u.get("active"):
            return _error("Ese usuario no existe o está desactivado.", 400, errores={"usuario_id": "Elige un usuario activo de ILUS."})
        try:
            mysql_execute(
                "INSERT INTO flota_choferes (usuario_id, licencia_clases, licencia_vence, notas, created_by, updated_by) VALUES (%s,%s,%s,%s,%s,%s)",
                (datos["usuario_id"], datos["licencia_clases"], datos["licencia_vence"], datos["notas"],
                 current_username(), current_username()))
        except Exception as e:
            if _es_duplicado(e):
                return _error(f"{u.get('nombre') or 'Ese usuario'} ya está en el repositorio de choferes.", 409,
                              errores={"usuario_id": "Ya es chofer."})
            print(f"[flota] crear chofer: {type(e).__name__}: {e}", flush=True)
            return _error("No se pudo guardar el chofer. Intenta de nuevo.", 500)
        fila = mysql_fetchone(_SQL_CHOF + " WHERE c.usuario_id=%s", (datos["usuario_id"],))
        if not fila:
            return _error("El chofer se guardó pero no se pudo leer de vuelta. Recarga la pantalla.", 500)
        _clog(fila["id"], "creado", {"usuario": u.get("nombre"), "licencia_clases": datos["licencia_clases"]})
        return jsonify({"ok": True, "chofer": _chofer_json(dict(fila), _hoy())})

    @app.route("/flota/api/choferes/<int:cid>", methods=["PUT"])
    @_flota_gestion_required
    def flota_api_chofer_editar(cid):
        actual = _chofer_fila(cid)
        if not actual:
            return _error("No encontré ese chofer.", 404)
        datos, errores = fr.validar_chofer(request.get_json(silent=True) or {}, con_usuario=False)
        if errores:
            return _error("Revisa los campos marcados.", 400, errores=errores)
        cambios = fr.cambios(dict(actual), datos, campos=fr.CHOFER_COLUMNAS)
        if not cambios:
            return jsonify({"ok": True, "sin_cambios": True, "chofer": _chofer_json(dict(actual), _hoy())})
        try:
            mysql_execute(
                "UPDATE flota_choferes SET licencia_clases=%s, licencia_vence=%s, notas=%s, updated_by=%s WHERE id=%s",
                (datos["licencia_clases"], datos["licencia_vence"], datos["notas"], current_username(), cid))
        except Exception as e:
            print(f"[flota] editar chofer: {type(e).__name__}: {e}", flush=True)
            return _error("No se pudo guardar los cambios. Intenta de nuevo.", 500)
        _clog(cid, "editado", {"cambios": cambios})
        return jsonify({"ok": True, "chofer": _chofer_json(dict(_chofer_fila(cid) or actual), _hoy())})

    @app.route("/flota/api/choferes/<int:cid>/inhabilitar", methods=["POST"])
    @_flota_gestion_required
    def flota_api_chofer_inhabilitar(cid):
        if not _chofer_fila(cid):
            return _error("No encontré ese chofer.", 404)
        motivo, err = fr.validar_motivo((request.get_json(silent=True) or {}).get("motivo"))
        if err:
            return _error(err, 400, errores={"motivo": err})
        n = mysql_rowcount(
            "UPDATE flota_choferes SET habilitado=0, inhabilitado_motivo=%s, inhabilitado_por=%s, inhabilitado_at=NOW(), updated_by=%s "
            "WHERE id=%s AND habilitado=1", (motivo, current_username(), current_username(), cid))
        if not n:
            return _error("Ese chofer ya estaba inhabilitado.", 409)
        _clog(cid, "inhabilitado", {"motivo": motivo})
        c = _chofer_json(dict(_chofer_fila(cid)), _hoy())
        return jsonify({"ok": True, "chofer": c, "avisos": _aviso_reservas(_reservas_abiertas("chofer", cid), c["nombre"])})

    @app.route("/flota/api/choferes/<int:cid>/habilitar", methods=["POST"])
    @_flota_gestion_required
    def flota_api_chofer_habilitar(cid):
        previo = _chofer_fila(cid)
        if not previo:
            return _error("No encontré ese chofer.", 404)
        n = mysql_rowcount(
            "UPDATE flota_choferes SET habilitado=1, inhabilitado_motivo=NULL, inhabilitado_por=NULL, inhabilitado_at=NULL, updated_by=%s "
            "WHERE id=%s AND habilitado=0", (current_username(), cid))
        if not n:
            return _error("Ese chofer ya estaba habilitado.", 409)
        _clog(cid, "habilitado", {"motivo_anterior": previo.get("inhabilitado_motivo")})
        c = _chofer_json(dict(_chofer_fila(cid)), _hoy())
        avisos = []
        if c["licencia_estado"] in ("vencido", "por_vencer"):
            avisos.append(f"Licencia de {c['nombre']}: {c['licencia_texto_dias'].lower()}.")
        return jsonify({"ok": True, "chofer": c, "avisos": avisos})

    @app.route("/flota/api/choferes/<int:cid>/historial", methods=["GET"])
    @_flota_required
    def flota_api_chofer_historial(cid):
        if not _chofer_fila(cid):
            return _error("No encontré ese chofer.", 404)
        filas = mysql_fetchall(
            "SELECT id, accion, detalle, usuario, created_at FROM flota_chofer_log WHERE chofer_id=%s ORDER BY id DESC LIMIT 200", (cid,)) or []
        titulos = {"creado": "Chofer ingresado", "editado": "Datos modificados", "inhabilitado": "Inhabilitado", "habilitado": "Habilitado"}
        eventos = []
        for f in filas:
            try:
                det = json.loads(f.get("detalle") or "null") or {}
            except Exception:
                det = {}
            if f["accion"] == "editado":
                lineas = fr.lineas_de_cambios_chofer(det.get("cambios"))
            elif f["accion"] == "inhabilitado":
                lineas = ["Motivo: " + (det.get("motivo") or "(sin motivo)")]
            elif f["accion"] == "habilitado":
                lineas = (["Estaba inhabilitado por: " + det["motivo_anterior"]] if det.get("motivo_anterior") else [])
            else:
                lineas = []
            eventos.append({"accion": f["accion"], "titulo": titulos.get(f["accion"], f["accion"]), "lineas": lineas,
                            "usuario": f.get("usuario") or "", "fecha": _hora_chile(f.get("created_at"))})
        return jsonify({"ok": True, "eventos": eventos})

    # ── Solicitudes ─────────────────────────────────────────────────────────
    def _sol_json(r):
        gest, uid = _flota_gestiona(), _uid()
        ini, fin = r["inicio"], r["fin"]
        est = r.get("estado")
        mia = bool(uid and r.get("solicitante_id") == uid)
        titulo = " ".join(x for x in (r.get("v_tipo"), r.get("v_marca"), r.get("v_modelo")) if x)
        return {
            "id": r["id"], "numero": r.get("numero") or "", "estado": est, "estado_texto": fr.ESTADOS.get(est, est),
            "vehiculo": {"id": r["vehiculo_id"], "patente_fmt": fr.patente_con_guion(r.get("v_patente")), "titulo": titulo},
            "area": r.get("area"), "solicitante": r.get("solicitante_nombre"),
            "chofer": {"id": r["chofer_id"], "nombre": r.get("chofer_nombre") or "(sin nombre)", "telefono": r.get("chofer_phone")},
            "fecha": _dia_iso(ini), "hora_desde": _hhmm(ini), "fecha_hasta": _dia_iso(fin), "hora_hasta": _hhmm(fin),
            "inicio": ini.strftime("%Y-%m-%dT%H:%M"), "fin": fin.strftime("%Y-%m-%dT%H:%M"), "rango_texto": fr.rango_texto(ini, fin),
            "motivo": r.get("motivo"), "sello_fmt": _hora_chile(r.get("sello_at")),
            "confirmada_por": r.get("confirmada_por"), "confirmada_fmt": _hora_chile(r.get("confirmada_at")),
            "llave_por": r.get("llave_por"), "llave_fmt": _hora_chile(r.get("llave_at")), "llave_nota": r.get("llave_nota"),
            "cancelada_por": r.get("cancelada_por"), "cancelada_fmt": _hora_chile(r.get("cancelada_at")),
            "cancelada_motivo": r.get("cancelada_motivo"),
            "modificada_por": r.get("modificada_por"), "modificada_fmt": _hora_chile(r.get("modificada_at")),
            "puede": {
                "confirmar": gest and est == "solicitada",
                "llave": gest and est == "confirmada",
                "editar": gest and est in fr.ESTADOS_EDITABLES,
                "cancelar": (gest and est in fr.ESTADOS_OCUPAN) or (mia and est in fr.ESTADOS_EDITABLES),
            },
        }

    def _sol_fila(sid):
        return mysql_fetchone(_SQL_SOL + " WHERE s.id=%s", (sid,))

    def _slog(sid, accion, detalle=None, cur=None):
        """Bitácora de la solicitud: solo se agrega. Con `cur` va dentro de la misma transacción que la reserva."""
        sql = "INSERT INTO flota_solicitud_log (solicitud_id, accion, detalle, usuario) VALUES (%s,%s,%s,%s)"
        params = (sid, accion, json.dumps(detalle, ensure_ascii=False, default=str) if detalle is not None else None,
                  (current_username() or "")[:190] or None)
        if cur is not None:
            cur.execute(sql, params)
            return
        try:
            mysql_execute(sql, params)
        except Exception as e:
            print(f"[flota] bitácora de solicitud ({accion}): {type(e).__name__}", flush=True)

    def _avisos_dia_no_laboral(ini, fin):
        """Alerta (no bloquea): ILUS no opera sábado, domingo ni feriados (REGLA #31)."""
        out, d = [], ini.date()
        while d <= fin.date() and len(out) < 3:
            nombre = None
            if cl_feriados is not None:
                try:
                    nombre = cl_feriados.feriados_chile(d.year).get(d.isoformat())
                except Exception:
                    nombre = None
            if nombre:
                out.append(f"{fr.fecha_dia_texto(d)} es feriado ({nombre}): ILUS no opera ese día.")
            elif d.weekday() >= 5:
                out.append(f"{fr.fecha_dia_texto(d)} es fin de semana: ILUS no opera ese día.")
            d += timedelta(days=1)
        return out

    def _choque_json(f):
        return {"numero": f.get("numero") or "", "estado_texto": fr.ESTADOS.get(f.get("estado"), f.get("estado")),
                "rango_texto": fr.rango_texto(f["inicio"], f["fin"]), "area": f.get("area"),
                "solicitante": f.get("solicitante_nombre"), "patente_fmt": fr.patente_con_guion(f.get("v_patente")),
                "sello_fmt": _hora_chile(f.get("sello_at"))}

    def _reservar(datos, sid=None, motivo_cambio=None):
        """Crea una reserva (sid=None) o mueve una existente. TODO en una transacción con el vehículo bloqueado (FOR UPDATE): así dos
        personas que piden a la vez el mismo vehículo no pasan las dos; gana el sello (quien llegó primero) y la otra recibe quién lo tiene.
        Devuelve (cuerpo_json, http)."""
        if get_mysql is None:
            return {"ok": False, "error": "El servidor no pudo abrir la base de datos. Intenta de nuevo en un momento."}, 500
        ahora = _ahora_chile()
        inicio, fin = datos["inicio"], datos["fin"]
        conn = None
        try:
            conn = get_mysql()
            conn.autocommit(False)
            cur = conn.cursor()
            actual = None
            if sid is not None:
                cur.execute("SELECT * FROM flota_solicitudes WHERE id=%s FOR UPDATE", (sid,))
                actual = cur.fetchone()
                if not actual:
                    conn.rollback()
                    return {"ok": False, "error": "No encontré esa solicitud."}, 404
                if actual["estado"] not in fr.ESTADOS_EDITABLES:
                    conn.rollback()
                    return {"ok": False, "error": f"Ya no se puede mover: está «{fr.ESTADOS.get(actual['estado'], actual['estado'])}»."}, 409
            cur.execute("SELECT * FROM flota_vehiculos WHERE id=%s FOR UPDATE", (datos["vehiculo_id"],))
            veh = cur.fetchone()
            if not veh:
                conn.rollback()
                return {"ok": False, "error": "No encontré ese vehículo.", "errores": {"vehiculo_id": "Elige un vehículo de la lista."}}, 400
            titulo_veh = " ".join(x for x in (veh.get("tipo"), fr.patente_con_guion(veh.get("patente"))) if x)
            if not veh.get("habilitado"):
                conn.rollback()
                msg = f"{titulo_veh} está inhabilitado" + (f": {veh['inhabilitado_motivo']}" if veh.get("inhabilitado_motivo") else ".")
                return {"ok": False, "error": msg, "errores": {"vehiculo_id": msg}}, 409
            cur.execute(_SQL_CHOF + " WHERE c.id=%s", (datos["chofer_id"],))
            ch = cur.fetchone()
            if not ch:
                conn.rollback()
                return {"ok": False, "error": "No encontré a ese chofer.", "errores": {"chofer_id": "Elige un chofer de la lista."}}, 400
            chofer_ev = {"nombre": ch.get("u_nombre"), "habilitado": bool(ch.get("habilitado")) and bool(ch.get("u_activo")),
                         "licencia_clases": ch.get("licencia_clases"), "licencia_vence": ch.get("licencia_vence")}
            err_lic, aviso_lic = fr.evaluar_chofer(chofer_ev, veh.get("licencia_clases"), fin, ahora.date())
            if err_lic:
                conn.rollback()
                return {"ok": False, "error": err_lic, "errores": {"chofer_id": err_lic}}, 400

            estados_sql = "('solicitada','confirmada','llave_entregada')"
            cur.execute(_SQL_OCUPA + " WHERE s.vehiculo_id=%s AND s.estado IN " + estados_sql + " AND s.inicio < %s AND s.fin > %s",
                        (datos["vehiculo_id"], fin, inicio))
            choque_v = fr.conflictos(inicio, fin, [dict(x) for x in (cur.fetchall() or [])], excluir_id=sid)
            if choque_v:
                conn.rollback()
                c0 = choque_v[0]
                msg = (f"{titulo_veh} ya está reservado en ese horario: {c0.get('numero') or 'otra solicitud'} · {c0.get('area')} · "
                       f"{fr.rango_texto(c0['inicio'], c0['fin'])} (pedido por {c0.get('solicitante_nombre') or 'otra persona'} "
                       f"el {_hora_chile(c0.get('sello_at'))}). Elige otro horario o el otro vehículo.")
                return {"ok": False, "error": msg, "error_codigo": "CHOQUE_VEHICULO", "errores": {"vehiculo_id": msg},
                        "choques": [_choque_json(x) for x in choque_v]}, 409
            cur.execute(_SQL_OCUPA + " WHERE s.chofer_id=%s AND s.estado IN " + estados_sql + " AND s.inicio < %s AND s.fin > %s",
                        (datos["chofer_id"], fin, inicio))
            choque_c = fr.conflictos(inicio, fin, [dict(x) for x in (cur.fetchall() or [])], excluir_id=sid)
            if choque_c:
                conn.rollback()
                c0 = choque_c[0]
                msg = (f"{ch.get('u_nombre') or 'El chofer'} ya tiene otra reserva en ese horario: {c0.get('numero') or ''} · "
                       f"{fr.patente_con_guion(c0.get('v_patente'))} · {fr.rango_texto(c0['inicio'], c0['fin'])}. Elige otro chofer u otro horario.")
                return {"ok": False, "error": msg, "error_codigo": "CHOQUE_CHOFER", "errores": {"chofer_id": msg},
                        "choques": [_choque_json(x) for x in choque_c]}, 409

            avisos = fr.avisos_vehiculo(dict(veh), inicio.date()) + ([aviso_lic] if aviso_lic else []) + _avisos_dia_no_laboral(inicio, fin)
            if sid is None:
                cur.execute(
                    "INSERT INTO flota_solicitudes (vehiculo_id, area, solicitante_id, solicitante_nombre, chofer_id, inicio, fin, motivo) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                    (datos["vehiculo_id"], datos["area"], _uid(), current_username(), datos["chofer_id"], inicio, fin, datos["motivo"]))
                sid = int(cur.lastrowid)
                cur.execute("UPDATE flota_solicitudes SET numero=%s WHERE id=%s", (fr.numero_solicitud(sid, ahora.year), sid))
                _slog(sid, "creada", {"vehiculo": titulo_veh, "chofer": ch.get("u_nombre"), "rango": fr.rango_texto(inicio, fin),
                                      "area": datos["area"], "motivo": datos["motivo"], "avisos": avisos}, cur=cur)
            else:
                cur.execute(
                    "UPDATE flota_solicitudes SET vehiculo_id=%s, area=%s, chofer_id=%s, inicio=%s, fin=%s, motivo=%s, "
                    "modificada_por=%s, modificada_at=NOW() WHERE id=%s AND estado IN ('solicitada','confirmada')",
                    (datos["vehiculo_id"], datos["area"], datos["chofer_id"], inicio, fin, datos["motivo"], current_username(), sid))
                if not cur.rowcount:
                    conn.rollback()
                    return {"ok": False, "error": "Otra persona cambió esta solicitud mientras la editabas. Recarga y vuelve a intentar."}, 409
                antes = {"vehiculo_id": actual["vehiculo_id"], "chofer_id": actual["chofer_id"], "area": actual.get("area"),
                         "inicio": fr.rango_texto(actual["inicio"], actual["fin"]), "motivo": actual.get("motivo")}
                _slog(sid, "editada", {"motivo_cambio": motivo_cambio, "antes": antes,
                                       "despues": {"vehiculo": titulo_veh, "chofer": ch.get("u_nombre"),
                                                   "rango": fr.rango_texto(inicio, fin), "area": datos["area"], "motivo": datos["motivo"]},
                                       "avisos": avisos}, cur=cur)
            conn.commit()
            return {"ok": True, "id": sid, "avisos": avisos}, 200
        except Exception as e:
            try:
                if conn is not None:
                    conn.rollback()
            except Exception:
                pass
            print(f"[flota] reservar: {type(e).__name__}: {e}", flush=True)
            return {"ok": False, "error": "No se pudo guardar la solicitud. Intenta de nuevo en un momento."}, 500
        finally:
            try:
                if conn is not None:
                    conn.close()
            except Exception:
                pass

    def _respuesta_reserva(cuerpo, http):
        if not cuerpo.get("ok"):
            return jsonify(cuerpo), http
        fila = _sol_fila(cuerpo["id"])
        if not fila:
            return _error("La solicitud se guardó pero no se pudo leer de vuelta. Recarga la pantalla.", 500)
        return jsonify({"ok": True, "solicitud": _sol_json(dict(fila)), "avisos": cuerpo.get("avisos") or []})

    @app.route("/flota/api/solicitudes", methods=["GET"])
    @_flota_required
    def flota_api_solicitudes():
        ahora = _ahora_chile()
        inicio_hoy = datetime.combine(ahora.date(), time.min)
        vista = "pasadas" if request.args.get("vista") == "pasadas" else "proximas"
        try:
            if vista == "pasadas":
                filas = mysql_fetchall(_SQL_SOL + " WHERE s.fin < %s ORDER BY s.inicio DESC, s.id DESC LIMIT 500", (inicio_hoy,)) or []
            else:
                filas = mysql_fetchall(_SQL_SOL + " WHERE s.fin >= %s ORDER BY s.inicio, s.id LIMIT 500", (inicio_hoy,)) or []
            cuentas = {r["estado"]: int(r["n"]) for r in (mysql_fetchall(
                "SELECT estado, COUNT(*) AS n FROM flota_solicitudes WHERE fin >= %s GROUP BY estado", (inicio_hoy,)) or [])}
            hoy_n = int((mysql_fetchone(
                "SELECT COUNT(*) AS n FROM flota_solicitudes WHERE estado IN ('solicitada','confirmada','llave_entregada') "
                "AND inicio < %s AND fin > %s", (inicio_hoy + timedelta(days=1), inicio_hoy)) or {}).get("n") or 0)
        except Exception as e:
            print(f"[flota] listar solicitudes: {type(e).__name__}: {e}", flush=True)
            return _error("No se pudo leer las solicitudes. Intenta de nuevo en un momento.", 500)
        return jsonify({
            "ok": True, "vista": vista, "solicitudes": [_sol_json(dict(f)) for f in filas],
            "resumen": {"por_confirmar": cuentas.get("solicitada", 0), "confirmadas": cuentas.get("confirmada", 0),
                        "llave_entregada": cuentas.get("llave_entregada", 0), "hoy": hoy_n},
            "hoy": ahora.date().isoformat(), "ahora_hhmm": ahora.strftime("%H:%M"),
            "areas": list(fr.AREAS_SUGERIDAS), "max_dias": fr.MAX_DIAS_USO,
        })

    @app.route("/flota/api/calendario", methods=["GET"])
    @_flota_required
    def flota_api_calendario():
        hoy = _ahora_chile().date()
        try:
            y, m = (int(x) for x in (request.args.get("mes") or hoy.strftime("%Y-%m")).split("-"))
            primero = date(y, m, 1)
        except Exception:
            primero = date(hoy.year, hoy.month, 1)
        siguiente = date(primero.year + (primero.month == 12), primero.month % 12 + 1, 1)
        try:
            filas = mysql_fetchall(
                _SQL_SOL + " WHERE s.inicio < %s AND s.fin > %s AND s.estado <> 'cancelada' ORDER BY s.inicio, s.id LIMIT 1000",
                (datetime.combine(siguiente, time.min), datetime.combine(primero, time.min))) or []
        except Exception as e:
            print(f"[flota] calendario: {type(e).__name__}: {e}", flush=True)
            return _error("No se pudo leer el calendario. Intenta de nuevo en un momento.", 500)
        feriados = {}
        if cl_feriados is not None:
            try:
                feriados = cl_feriados.feriados_chile(primero.year)
            except Exception:
                feriados = {}
        dias, d = [], primero
        while d < siguiente:
            nombre = feriados.get(d.isoformat())
            dias.append({"fecha": d.isoformat(), "dia": d.day, "semana": d.weekday(),
                         "no_laboral": "feriado" if nombre else ("fin_de_semana" if d.weekday() >= 5 else None), "nombre": nombre})
            d += timedelta(days=1)
        return jsonify({"ok": True, "mes": primero.strftime("%Y-%m"), "mes_texto": f"{fr.MESES[primero.month - 1].capitalize()} {primero.year}",
                        "dias": dias, "solicitudes": [_sol_json(dict(f)) for f in filas], "hoy": hoy.isoformat()})

    @app.route("/flota/api/solicitudes", methods=["POST"])
    @_flota_required
    def flota_api_solicitud_crear():
        datos, errores = fr.validar_solicitud(request.get_json(silent=True) or {}, _ahora_chile())
        if errores:
            return _error("Revisa los campos marcados.", 400, errores=errores)
        return _respuesta_reserva(*_reservar(datos))

    @app.route("/flota/api/solicitudes/<int:sid>", methods=["GET"])
    @_flota_required
    def flota_api_solicitud_detalle(sid):
        fila = _sol_fila(sid)
        if not fila:
            return _error("No encontré esa solicitud.", 404)
        eventos = []
        titulos = {"creada": "Solicitud creada", "confirmada": "Confirmada por la gestión", "llave_entregada": "Llave entregada",
                   "editada": "Reserva modificada", "cancelada": "Cancelada"}
        for f in mysql_fetchall(
                "SELECT id, accion, detalle, usuario, created_at FROM flota_solicitud_log WHERE solicitud_id=%s ORDER BY id DESC LIMIT 200",
                (sid,)) or []:
            try:
                det = json.loads(f.get("detalle") or "null") or {}
            except Exception:
                det = {}
            lineas = []
            if f["accion"] == "creada":
                lineas = [x for x in (det.get("vehiculo"), det.get("rango"), ("Chofer: " + det["chofer"]) if det.get("chofer") else None) if x]
            elif f["accion"] == "editada":
                lineas = ["Motivo del cambio: " + (det.get("motivo_cambio") or "(sin motivo)")]
                antes, despues = det.get("antes") or {}, det.get("despues") or {}
                if antes.get("inicio") and despues.get("rango") and antes["inicio"] != despues["rango"]:
                    lineas.append(f"Horario: {antes['inicio']} → {despues['rango']}")
            elif f["accion"] == "cancelada":
                lineas = ["Motivo: " + (det.get("motivo") or "(sin motivo)")]
            elif f["accion"] == "llave_entregada" and det.get("nota"):
                lineas = ["Nota: " + det["nota"]]
            eventos.append({"accion": f["accion"], "titulo": titulos.get(f["accion"], f["accion"]), "lineas": lineas,
                            "usuario": f.get("usuario") or "", "fecha": _hora_chile(f.get("created_at"))})
        return jsonify({"ok": True, "solicitud": _sol_json(dict(fila)), "eventos": eventos})

    @app.route("/flota/api/solicitudes/<int:sid>", methods=["PUT"])
    @_flota_gestion_required
    def flota_api_solicitud_editar(sid):
        if not _sol_fila(sid):
            return _error("No encontré esa solicitud.", 404)
        cuerpo = request.get_json(silent=True) or {}
        datos, errores = fr.validar_solicitud(cuerpo, _ahora_chile(), editando=True)
        motivo, err_m = fr.validar_motivo_cambio(cuerpo.get("motivo_cambio"))
        if err_m:
            errores["motivo_cambio"] = err_m
        if errores:
            return _error("Revisa los campos marcados.", 400, errores=errores)
        return _respuesta_reserva(*_reservar(datos, sid=sid, motivo_cambio=motivo))

    def _transicion(sid, sql, params, accion, detalle):
        """Cambio de estado atómico: solo si la solicitud sigue en el estado que se vio (si otra persona ya la movió, no se pisa)."""
        fila = _sol_fila(sid)
        if not fila:
            return _error("No encontré esa solicitud.", 404)
        if not mysql_rowcount(sql, params):
            actual = _sol_fila(sid) or fila
            return _error(f"Otra persona ya la movió: ahora está «{fr.ESTADOS.get(actual['estado'], actual['estado'])}». Recarga la pantalla.", 409,
                          solicitud=_sol_json(dict(actual)))
        _slog(sid, accion, detalle)
        return jsonify({"ok": True, "solicitud": _sol_json(dict(_sol_fila(sid)))})

    @app.route("/flota/api/solicitudes/<int:sid>/confirmar", methods=["POST"])
    @_flota_gestion_required
    def flota_api_solicitud_confirmar(sid):
        return _transicion(
            sid, "UPDATE flota_solicitudes SET estado='confirmada', confirmada_por=%s, confirmada_at=NOW() WHERE id=%s AND estado='solicitada'",
            (current_username(), sid), "confirmada", {})

    @app.route("/flota/api/solicitudes/<int:sid>/llave", methods=["POST"])
    @_flota_gestion_required
    def flota_api_solicitud_llave(sid):
        nota = re.sub(r"\s+", " ", str((request.get_json(silent=True) or {}).get("nota") or "")).strip()
        if len(nota) > 300:
            return _error("La nota puede tener hasta 300 caracteres.", 400, errores={"nota": "Máximo 300 caracteres."})
        return _transicion(
            sid, "UPDATE flota_solicitudes SET estado='llave_entregada', llave_por=%s, llave_at=NOW(), llave_nota=%s "
                 "WHERE id=%s AND estado='confirmada'",
            (current_username(), nota or None, sid), "llave_entregada", {"nota": nota or None})

    @app.route("/flota/api/solicitudes/<int:sid>/cancelar", methods=["POST"])
    @_flota_required
    def flota_api_solicitud_cancelar(sid):
        fila = _sol_fila(sid)
        if not fila:
            return _error("No encontré esa solicitud.", 404)
        if fila["estado"] not in fr.ESTADOS_OCUPAN:
            return _error(f"Esta solicitud ya está «{fr.ESTADOS.get(fila['estado'], fila['estado'])}»: no hay nada que cancelar.", 409,
                          solicitud=_sol_json(dict(fila)))
        if not _sol_json(dict(fila))["puede"]["cancelar"]:
            return _error("No puedes cancelar esta solicitud.", 403, error_codigo="SIN_PERMISO_CANCELAR")
        motivo, err = fr.validar_motivo_cambio((request.get_json(silent=True) or {}).get("motivo"))
        if err:
            return _error(err, 400, errores={"motivo": err})
        return _transicion(
            sid, "UPDATE flota_solicitudes SET estado='cancelada', cancelada_por=%s, cancelada_at=NOW(), cancelada_motivo=%s "
                 "WHERE id=%s AND estado IN ('solicitada','confirmada','llave_entregada')",
            (current_username(), motivo, sid), "cancelada", {"motivo": motivo, "estado_antes": fila["estado"]})

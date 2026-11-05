"""Módulo FLOTA · «Solicitudes de vehículo» de ILUS Fitness.

Pedido de Daniel (02-nov-2026): ordenar las solicitudes de vehículos, registrar sus datos, hacerles seguimiento calendarizado e integrar su
GPS. En el menú va UN solo ítem, «Solicitudes de vehículo», y adentro viven la gestión de cada vehículo, los choferes y las reservas.

FASE 1a: registro de los vehículos propios (alta, edición, habilitar / inhabilitar con motivo) con sus datos, los 4 documentos con
vencimiento y la bitácora de cada cambio. La flota NO se fija en el código: cada vehículo nuevo se ingresa a mano desde la pantalla.
FASE 1b: repositorio de choferes (cada uno es un usuario de ILUS, con su licencia) y solicitudes de uso: cualquier área reserva un
vehículo declarando día, horario, motivo y el chofer que retira la llave; la gestión confirma y marca la llave entregada; el sello
decide los choques; lista y calendario.
FASE 1c: permisos por el front (flota_solicitar / flota_gestionar / flota_documentos; agregar, editar, archivar y borrar vehículos y
choferes es solo del superadministrador), foto del vehículo y PDF de sus documentos y de la licencia del chofer (privados), datos del
propietario, motor y póliza, RUT, nombre completo y dirección de los choferes, y la calidad de la ficha. Este módulo no maneja precios ni
costos. Siguen: devolución, mantención y GPS.

HOY SOLO LO VE EL SUPERADMIN (Daniel: «quiero solo verlo yo de momento»): ningún rol tiene permiso y el ítem del menú no aparece para nadie
más. Cuando se abra a otros roles se cambia SOLO `_flota_puede()`.

Wiring idéntico al de catalogo_module.py / tickets_module.py:
    from flota_module import register_flota_routes
    register_flota_routes(app, globals())

Las tablas se crean al registrar el módulo, dentro de app_context (si no, mysql_execute falla en silencio: ver el gotcha de los _ensure_*).
REGLA #18: una sentencia por DDL, `CREATE TABLE IF NOT EXISTS`, sin ALTER sobre tablas de otros módulos.
"""
import hashlib
import json
import os
import re
import secrets
import threading
from datetime import date, datetime, time, timedelta, timezone
from functools import wraps

from flask import Response, g, jsonify, redirect, render_template, request, url_for

import time as _time_mod

import flota_licencia_lector as licl
import flota_licencia_ocr as lico
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


# Funciones que usan el asistente «Nueva OT» y la ficha de la OT (disponibilidad, reservar, reservas de una orden). Se llenan al registrar
# el módulo: app.py las llama como `flota_module.OT_API.get("reservar")` y, si el módulo no cargó, la OT se crea igual sin vehículo.
OT_API = {}


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
            # Fase 1b. Un chofer es un usuario de ILUS (usuario_id: nombre y teléfono salen del perfil) o, desde el 02-nov-2026, un chofer
            # EXTERNO sin usuario (usuario_id NULL: nombre_completo y telefono propios).
            mysql_execute("""
                CREATE TABLE IF NOT EXISTS flota_choferes (
                  id                   INT AUTO_INCREMENT PRIMARY KEY,
                  usuario_id           INT          NULL,
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
            # Fase 1c. Foto del vehículo, PDF de sus documentos y licencia del chofer. El archivo vive en GCS (privado); acá solo su ficha.
            # Reemplazar o quitar NO borra: el anterior queda con activo=0 (historial).
            mysql_execute("""
                CREATE TABLE IF NOT EXISTS flota_archivos (
                  id               INT AUTO_INCREMENT PRIMARY KEY,
                  entidad          VARCHAR(10)  NOT NULL,
                  entidad_id       INT          NOT NULL,
                  tipo             VARCHAR(30)  NOT NULL,
                  nombre_original  VARCHAR(190) NULL,
                  content_type     VARCHAR(80)  NULL,
                  bytes            INT          NULL,
                  gcs_key          VARCHAR(300) NOT NULL,
                  sha256           CHAR(64)     NULL,
                  activo           TINYINT(1)   NOT NULL DEFAULT 1,
                  subido_por       VARCHAR(190) NULL,
                  subido_at        DATETIME DEFAULT CURRENT_TIMESTAMP,
                  quitado_por      VARCHAR(190) NULL,
                  quitado_at       DATETIME     NULL,
                  KEY idx_flota_arch (entidad, entidad_id, tipo, activo)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
        except Exception as e:
            print(f"[ILUS][WARN] _ensure_flota_tables: {type(e).__name__}: {e}", flush=True)
        # Columnas nuevas de las tablas de la fase 1a/1b (REGLA #18: una sentencia por columna; mysql_execute salta la que ya existe).
        columnas_nuevas = (
            "ALTER TABLE flota_vehiculos ADD COLUMN patente_dv VARCHAR(2) NULL",
            "ALTER TABLE flota_vehiculos ADD COLUMN numero_motor VARCHAR(40) NULL",
            "ALTER TABLE flota_vehiculos ADD COLUMN cilindrada VARCHAR(20) NULL",
            "ALTER TABLE flota_vehiculos ADD COLUMN propietario VARCHAR(160) NULL",
            "ALTER TABLE flota_vehiculos ADD COLUMN propietario_rut VARCHAR(20) NULL",
            "ALTER TABLE flota_vehiculos ADD COLUMN revision_tecnica_detalle VARCHAR(160) NULL",
            "ALTER TABLE flota_vehiculos ADD COLUMN permiso_circulacion_detalle VARCHAR(160) NULL",
            "ALTER TABLE flota_vehiculos ADD COLUMN soap_detalle VARCHAR(160) NULL",
            "ALTER TABLE flota_vehiculos ADD COLUMN seguro_detalle VARCHAR(160) NULL",
            "ALTER TABLE flota_vehiculos ADD COLUMN archivado TINYINT(1) NOT NULL DEFAULT 0",
            "ALTER TABLE flota_vehiculos ADD COLUMN archivado_motivo VARCHAR(300) NULL",
            "ALTER TABLE flota_vehiculos ADD COLUMN archivado_por VARCHAR(190) NULL",
            "ALTER TABLE flota_vehiculos ADD COLUMN archivado_at DATETIME NULL",
            "ALTER TABLE flota_choferes ADD COLUMN rut VARCHAR(20) NULL",
            "ALTER TABLE flota_choferes ADD COLUMN nombre_completo VARCHAR(190) NULL",
            "ALTER TABLE flota_choferes ADD COLUMN direccion VARCHAR(300) NULL",
            "ALTER TABLE flota_choferes ADD COLUMN direccion_lat DECIMAL(10,7) NULL",
            "ALTER TABLE flota_choferes ADD COLUMN direccion_lng DECIMAL(10,7) NULL",
            "ALTER TABLE flota_choferes ADD COLUMN direccion_place_id VARCHAR(200) NULL",
            "ALTER TABLE flota_choferes ADD COLUMN archivado TINYINT(1) NOT NULL DEFAULT 0",
            "ALTER TABLE flota_choferes ADD COLUMN archivado_motivo VARCHAR(300) NULL",
            "ALTER TABLE flota_choferes ADD COLUMN archivado_por VARCHAR(190) NULL",
            "ALTER TABLE flota_choferes ADD COLUMN archivado_at DATETIME NULL",
            "ALTER TABLE flota_choferes ADD COLUMN telefono VARCHAR(30) NULL",
            "ALTER TABLE flota_solicitudes ADD COLUMN sla_aviso VARCHAR(20) NULL",
            "ALTER TABLE flota_solicitudes ADD COLUMN devuelta_por VARCHAR(190) NULL",
            "ALTER TABLE flota_solicitudes ADD COLUMN devuelta_at DATETIME NULL",
            "ALTER TABLE flota_solicitudes ADD COLUMN dev_km INT NULL",
            "ALTER TABLE flota_solicitudes ADD COLUMN dev_combustible VARCHAR(20) NULL",
            "ALTER TABLE flota_solicitudes ADD COLUMN dev_obs VARCHAR(500) NULL",
            "ALTER TABLE flota_solicitudes ADD COLUMN dev_novedad TINYINT(1) NOT NULL DEFAULT 0",
            # Reserva que nace desde «Nueva OT» (Daniel, 04-nov-2026): la solicitud recuerda de qué orden de trabajo viene.
            "ALTER TABLE flota_solicitudes ADD COLUMN ot_id INT NULL",
            "ALTER TABLE flota_solicitudes ADD COLUMN ot_numero VARCHAR(40) NULL",
            # 05-nov-2026 (Daniel): la gestión CONFIRMA la recepción de la llave y que los datos de la devolución son reales; ahí se cierra.
            "ALTER TABLE flota_solicitudes ADD COLUMN dev_confirmada_por VARCHAR(190) NULL",
            "ALTER TABLE flota_solicitudes ADD COLUMN dev_confirmada_at DATETIME NULL",
            "ALTER TABLE flota_solicitudes ADD COLUMN dev_confirmada_nota VARCHAR(500) NULL",
        )
        for sql_col in columnas_nuevas:
            try:
                mysql_execute(sql_col)
            except Exception as e:
                if "Duplicate column" not in str(e) and "1060" not in str(e):
                    print(f"[ILUS][WARN] _ensure_flota_tables (columna): {type(e).__name__}: {e}", flush=True)
        # Chofer externo (02-nov-2026): usuario_id pasa a aceptar NULL. Solo corre UNA vez: se mira antes si ya acepta NULL, así el
        # arranque no pide un lock de la tabla en cada instancia (REGLA #18).
        try:
            col = mysql_fetchone(
                "SELECT IS_NULLABLE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() "
                "AND TABLE_NAME='flota_choferes' AND COLUMN_NAME='usuario_id'")
            if col and str(col.get("IS_NULLABLE") or "").upper() == "NO":
                mysql_execute("ALTER TABLE flota_choferes MODIFY usuario_id INT NULL")
        except Exception as e:
            print(f"[ILUS][WARN] _ensure_flota_tables (chofer externo): {type(e).__name__}: {e}", flush=True)

    with app.app_context():
        _ensure_flota_tables()
        print("[ILUS] Tablas flota_* garantizadas (Solicitudes de vehículo).", flush=True)

    # ── Acceso ──────────────────────────────────────────────────────────────
    # Cuatro niveles, decididos en UN solo lugar (flota_reglas.niveles): solicitar (entrar y pedir), gestionar (confirmar, llave, mover,
    # cancelar), documentos (ver PDF y datos personales) y admin (agregar, editar, archivar y borrar vehículos y choferes: el
    # superadministrador o quien tenga la llave «administrar»). Las cuatro llaves se asignan por rol o por persona en el front
    # (Roles → Solicitudes de vehículo); nacen apagadas para todos.
    def _niv():
        return fr.niveles(g.get("permissions"))

    def _flota_puede():
        return _niv()["solicitar"]

    def _flota_gestiona():
        return _niv()["gestionar"]

    def _flota_admin():
        return _niv()["admin"]

    def _flota_docs():
        return _niv()["documentos"]

    def _es_api():
        return request.path.startswith("/flota/api/")

    def _puerta(chequeo, mensaje, codigo):
        def deco(view):
            @wraps(view)
            def wrapped(*a, **k):
                if not chequeo():
                    if _es_api():
                        return jsonify({"ok": False, "error": mensaje, "error_codigo": codigo}), 403
                    return redirect(url_for("index"))
                return view(*a, **k)
            return login_required(wrapped)
        return deco

    _flota_required = _puerta(_flota_puede, "Esta sección todavía no está habilitada para tu usuario.", "SIN_PERMISO_FLOTA")
    _flota_gestion_required = _puerta(_flota_gestiona, "Solo quien gestiona la flota puede hacer esto.", "SIN_PERMISO_GESTION_FLOTA")
    _flota_admin_required = _puerta(_flota_admin, "No tienes permiso para agregar, editar o borrar vehículos y choferes.", "SIN_PERMISO_ADMIN_FLOTA")
    _flota_docs_required = _puerta(_flota_docs, "No tienes permiso para ver los documentos de la flota.", "SIN_PERMISO_DOCUMENTOS_FLOTA")

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

    # ── Archivos: foto y PDF (privados) ─────────────────────────────────────
    # Todo vive en GCS bajo «flota_privado/» y se abre SOLO por /flota/api/archivos/<id>, que pide sesión y el permiso que corresponde.
    # /f/<key> (la ruta pública de ILUS) responde 404 a cualquier clave que empiece así. La foto la ve quien entra a la flota; los PDF solo
    # quien tenga «Documentos y datos de la flota» (o el superadministrador). Subir y quitar archivos es del superadministrador.
    _PREFIJO_PRIVADO = "flota_privado/"

    def _bucket():
        try:
            fn = ctx.get("_gcs_bucket")
            return fn() if fn else None
        except Exception as e:
            print(f"[flota] almacenamiento: {type(e).__name__}", flush=True)
            return None

    def _archivos_activos(entidad):
        """{id de la entidad: {tipo: fila}} con el archivo VIGENTE de cada tipo (el último subido)."""
        out = {}
        try:
            filas = mysql_fetchall(
                "SELECT id, entidad_id, tipo, nombre_original, content_type, bytes, subido_por, subido_at FROM flota_archivos "
                "WHERE entidad=%s AND activo=1 ORDER BY id", (entidad,)) or []
        except Exception as e:
            print(f"[flota] leer archivos: {type(e).__name__}: {e}", flush=True)
            return out
        for f in filas:
            out.setdefault(f["entidad_id"], {})[f["tipo"]] = f
        return out

    def _archivo_json(f):
        return {"id": f["id"], "tipo": f.get("tipo"), "nombre": f.get("nombre_original"), "content_type": f.get("content_type"),
                "bytes": int(f.get("bytes") or 0), "subido_por": f.get("subido_por"), "subido_fmt": _hora_chile(f.get("subido_at"))}

    # ── Vehículos ───────────────────────────────────────────────────────────
    def _a_json(row, hoy, archivos=None):
        niv = _niv()
        archivos = archivos or {}
        docs = fr.resumen_documentos(row, hoy)
        tiene_pdf = {}
        for d in docs:
            a = archivos.get(d["clave"])
            tiene_pdf[d["clave"]] = bool(a)
            d["tiene_pdf"] = bool(a)
            # El detalle (póliza, N°) y el PDF solo los ve quien tiene «Documentos y datos de la flota».
            d["detalle"] = (row.get(d["clave"] + "_detalle") or "") if niv["documentos"] else ""
            d["archivo"] = _archivo_json(a) if (a and niv["documentos"]) else None
        clases = fr.normalizar_clases(row.get("licencia_clases"))
        titulo = " ".join(x for x in (row.get("tipo"), row.get("marca"), row.get("modelo"),
                                      str(row["anio"]) if row.get("anio") else None) if x)
        foto = archivos.get("foto")
        out = {
            "id": row["id"], "patente": row["patente"], "patente_fmt": fr.patente_con_guion(row["patente"]),
            "patente_dv": row.get("patente_dv"), "patente_completa": fr.patente_completa(row["patente"], row.get("patente_dv")),
            "tipo": row.get("tipo"), "marca": row.get("marca"), "modelo": row.get("modelo"), "anio": row.get("anio"),
            "color": row.get("color"), "titulo": titulo, "cilindrada": row.get("cilindrada"), "propietario": row.get("propietario"),
            "capacidad_kg": _num(row.get("capacidad_kg")), "capacidad_m3": _num(row.get("capacidad_m3")),
            "combustible": row.get("combustible"), "estanque_l": _num(row.get("estanque_l")), "km_actual": _num(row.get("km_actual")),
            "licencia_clases": clases, "licencia_texto": ("Clase " + " o ".join(clases)) if clases else "Sin definir",
            "documentos": docs, "docs_estado": fr.peor_estado(docs), "proximo_vencimiento": fr.proximo_vencimiento(docs),
            "foto_id": foto["id"] if foto else None,
            # Una foto del MODELO (no del vehículo) se sube con un nombre que empieza con «referencial»: la tarjeta lo dice.
            "foto_referencial": bool(foto and str(foto.get("nombre_original") or "").lower().startswith("referencial")),
            "notas": row.get("notas"), "habilitado": bool(row.get("habilitado")),
            "inhabilitado_motivo": row.get("inhabilitado_motivo"), "inhabilitado_por": row.get("inhabilitado_por"),
            "inhabilitado_fmt": _hora_chile(row.get("inhabilitado_at")),
            "archivado": bool(row.get("archivado")), "archivado_motivo": row.get("archivado_motivo"),
            "archivado_por": row.get("archivado_por"), "archivado_fmt": _hora_chile(row.get("archivado_at")),
            "creado_por": row.get("created_by"), "creado_fmt": _hora_chile(row.get("created_at")),
            "actualizado_por": row.get("updated_by"), "actualizado_fmt": _hora_chile(row.get("updated_at")),
            "vin": None, "numero_motor": None, "propietario_rut": None, "propietario_rut_fmt": "", "calidad": None,
        }
        if niv["documentos"]:
            out.update(vin=row.get("vin"), numero_motor=row.get("numero_motor"), propietario_rut=row.get("propietario_rut"),
                       propietario_rut_fmt=fr.rut_formato(row.get("propietario_rut")) if row.get("propietario_rut") else "",
                       calidad=fr.calidad_ficha(row, tiene_pdf, bool(foto)))
            for clave, _n in fr.DOCUMENTOS:
                out[clave + "_detalle"] = row.get(clave + "_detalle")
        for clave, _n in fr.DOCUMENTOS:
            v = row.get(clave + "_vence")
            out[clave + "_vence"] = v.isoformat() if isinstance(v, (date, datetime)) else (v or None)
        return out

    def _config():
        return {
            "tipos": list(fr.TIPOS_SUGERIDOS), "combustibles": list(fr.COMBUSTIBLES_SUGERIDOS),
            "clases": [{"codigo": c, "nombre": fr.LICENCIA_CLASES[c]} for c in fr.ORDEN_CLASES],
            "clase_sugerida": "B", "dias_alerta": fr.DIAS_ALERTA,
            "areas": list(fr.AREAS_SUGERIDAS), "max_dias_uso": fr.MAX_DIAS_USO, "antelacion_horas": fr.MIN_ANTELACION_H,
        }

    def _log(vid, accion, detalle=None, estricto=False):
        """Agrega una línea a la bitácora. Nunca rompe la acción principal, salvo `estricto` (antes de BORRAR: si no queda anotado, no se borra)."""
        try:
            mysql_execute(
                "INSERT INTO flota_vehiculo_log (vehiculo_id, accion, detalle, usuario) VALUES (%s,%s,%s,%s)",
                (vid, accion, json.dumps(detalle, ensure_ascii=False, default=str) if detalle is not None else None,
                 (current_username() or "")[:190] or None))
        except Exception as e:
            print(f"[flota] bitácora ({accion}): {type(e).__name__}", flush=True)
            if estricto:
                raise

    def _es_duplicado(exc):
        a = getattr(exc, "args", ())
        return bool(a and a[0] == 1062) or "Duplicate entry" in str(exc)

    def _fila(vid):
        return mysql_fetchone("SELECT * FROM flota_vehiculos WHERE id=%s", (vid,))

    def _error(msg, status=400, **extra):
        return jsonify({"ok": False, "error": msg, **extra}), status

    def _vehiculo_respuesta(vid, **extra):
        f = _fila(vid)
        return jsonify({"ok": True, "vehiculo": _a_json(dict(f), _hoy(), _archivos_activos("vehiculo").get(vid)) if f else None, **extra})

    # ── Página ──────────────────────────────────────────────────────────────
    @app.route("/flota")
    @_flota_required
    def flota_index():
        return render_template("flota/index.html", flota_permisos=_niv())

    # ── API de vehículos ────────────────────────────────────────────────────
    @app.route("/flota/api/vehiculos", methods=["GET"])
    @_flota_required
    def flota_api_vehiculos():
        archivados = 1 if (request.args.get("archivados") == "1" and _flota_admin()) else 0
        try:
            filas = mysql_fetchall(
                "SELECT * FROM flota_vehiculos WHERE archivado=%s ORDER BY habilitado DESC, tipo, patente LIMIT 500", (archivados,)) or []
            n_arch = int((mysql_fetchone("SELECT COUNT(*) AS n FROM flota_vehiculos WHERE archivado=1") or {}).get("n") or 0) if _flota_admin() else 0
        except Exception as e:
            print(f"[flota] listar: {type(e).__name__}: {e}", flush=True)
            return _error("No se pudo leer la flota. Intenta de nuevo en un momento.", 500)
        hoy = _hoy()
        arch = _archivos_activos("vehiculo")
        vehiculos = [_a_json(dict(f), hoy, arch.get(f["id"])) for f in filas]
        resumen = {
            "total": len(vehiculos),
            "habilitados": sum(1 for v in vehiculos if v["habilitado"]),
            "inhabilitados": sum(1 for v in vehiculos if not v["habilitado"]),
            "por_vencer": sum(1 for v in vehiculos if v["docs_estado"] == "por_vencer"),
            "vencidos": sum(1 for v in vehiculos if v["docs_estado"] == "vencido"),
            "incompletos": sum(1 for v in vehiculos if v.get("calidad") and v["calidad"]["pct"] < 100),
            "archivados": n_arch,
        }
        return jsonify({"ok": True, "vehiculos": vehiculos, "resumen": resumen, "config": _config(), "hoy": hoy.isoformat(),
                        "permisos": _niv(), "ver_archivados": bool(archivados)})

    @app.route("/flota/api/vehiculos", methods=["POST"])
    @_flota_admin_required
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
        return _vehiculo_respuesta(fila["id"])

    @app.route("/flota/api/vehiculos/<int:vid>", methods=["PUT"])
    @_flota_admin_required
    def flota_api_vehiculo_editar(vid):
        actual = _fila(vid)
        if not actual:
            return _error("No encontré ese vehículo.", 404)
        payload = request.get_json(silent=True) or {}
        datos, errores = fr.validar_vehiculo(payload, _hoy())
        if errores:
            return _error("Revisa los campos marcados.", 400, errores=errores)
        # Motivo del cambio (Daniel, 02-nov-2026: «deja una actividad declarada»): opcional, queda en el historial con quién y cuándo.
        motivo = re.sub(r"\s+", " ", str(payload.get("motivo_cambio") or "")).strip()[:300] or None
        cambios = fr.cambios(dict(actual), datos)
        if not cambios:
            return jsonify({"ok": True, "sin_cambios": True,
                            "vehiculo": _a_json(dict(actual), _hoy(), _archivos_activos("vehiculo").get(vid))})
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
        _log(vid, "editado", {"cambios": cambios, "motivo": motivo} if motivo else {"cambios": cambios})
        return _vehiculo_respuesta(vid)

    @app.route("/flota/api/vehiculos/<int:vid>/inhabilitar", methods=["POST"])
    @_flota_admin_required
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
        f = _fila(vid)
        v = _a_json(dict(f), _hoy(), _archivos_activos("vehiculo").get(vid))
        # Alertar, no trabar: se inhabilita igual, pero se dice si tenía reservas por delante.
        return jsonify({"ok": True, "vehiculo": v, "avisos": _aviso_reservas(_reservas_abiertas("vehiculo", vid), v["titulo"] or v["patente_fmt"])})

    @app.route("/flota/api/vehiculos/<int:vid>/habilitar", methods=["POST"])
    @_flota_admin_required
    def flota_api_vehiculo_habilitar(vid):
        previo = _fila(vid)
        if not previo:
            return _error("No encontré ese vehículo.", 404)
        if previo.get("archivado"):
            return _error("Este vehículo está archivado: primero desarchívalo.", 409)
        n = mysql_rowcount(
            "UPDATE flota_vehiculos SET habilitado=1, inhabilitado_motivo=NULL, inhabilitado_por=NULL, inhabilitado_at=NULL, updated_by=%s "
            "WHERE id=%s AND habilitado=0", (current_username(), vid))
        if not n:
            return _error("Ese vehículo ya estaba habilitado.", 409)
        _log(vid, "habilitado", {"motivo_anterior": previo.get("inhabilitado_motivo")})
        f = _fila(vid)
        v = _a_json(dict(f), _hoy(), _archivos_activos("vehiculo").get(vid))
        # Alertar, no trabar: se habilita igual, pero se dice qué documentos conviene mirar.
        avisos = [f"{d['nombre']}: {d['texto'].lower()}" for d in v["documentos"] if d["estado"] in ("vencido", "por_vencer")]
        return jsonify({"ok": True, "vehiculo": v, "avisos": avisos})

    @app.route("/flota/api/vehiculos/<int:vid>/archivar", methods=["POST"])
    @_flota_admin_required
    def flota_api_vehiculo_archivar(vid):
        """Archivar = sale de las listas pero conserva todo (datos, archivos y el historial de sus reservas). Se puede desarchivar."""
        if not _fila(vid):
            return _error("No encontré ese vehículo.", 404)
        motivo, err = fr.validar_motivo_archivo((request.get_json(silent=True) or {}).get("motivo"))
        if err:
            return _error(err, 400, errores={"motivo": err})
        n_res = _reservas_abiertas("vehiculo", vid)
        if n_res:
            return _error(f"Tiene {n_res} reserva{'s' if n_res != 1 else ''} por delante: muévelas o cancélalas antes de archivarlo.", 409,
                          error_codigo="TIENE_RESERVAS")
        quien = current_username()
        n = mysql_rowcount(
            "UPDATE flota_vehiculos SET archivado=1, archivado_motivo=%s, archivado_por=%s, archivado_at=NOW(), habilitado=0, "
            "inhabilitado_motivo=%s, inhabilitado_por=%s, inhabilitado_at=NOW(), updated_by=%s WHERE id=%s AND archivado=0",
            (motivo, quien, "Archivado: " + motivo, quien, quien, vid))
        if not n:
            return _error("Ese vehículo ya estaba archivado.", 409)
        _log(vid, "archivado", {"motivo": motivo})
        return _vehiculo_respuesta(vid)

    @app.route("/flota/api/vehiculos/<int:vid>/desarchivar", methods=["POST"])
    @_flota_admin_required
    def flota_api_vehiculo_desarchivar(vid):
        if not _fila(vid):
            return _error("No encontré ese vehículo.", 404)
        n = mysql_rowcount(
            "UPDATE flota_vehiculos SET archivado=0, archivado_motivo=NULL, archivado_por=NULL, archivado_at=NULL, updated_by=%s "
            "WHERE id=%s AND archivado=1", (current_username(), vid))
        if not n:
            return _error("Ese vehículo no estaba archivado.", 409)
        _log(vid, "desarchivado", {})
        # Vuelve inhabilitado a propósito: lo habilita una persona (queda a la vista el aviso de sus documentos).
        return _vehiculo_respuesta(vid, avisos=["Volvió inhabilitado: usa «Habilitar» cuando quieras dejarlo disponible."])

    @app.route("/flota/api/vehiculos/<int:vid>", methods=["DELETE"])
    @_flota_admin_required
    def flota_api_vehiculo_eliminar(vid):
        """Borrado DEFINITIVO: solo si nunca tuvo reservas (si no, se perdería su historial: se archiva) y escribiendo la patente. Queda anotado
        en la bitácora, con todos sus datos, ANTES de borrar (REGLA #5)."""
        v = _fila(vid)
        if not v:
            return _error("No encontré ese vehículo.", 404)
        if not fr.coincide_confirmacion((request.get_json(silent=True) or {}).get("confirmacion"), v["patente"], es_patente=True):
            return _error("Para borrarlo definitivo escribe su patente.", 400, errores={"confirmacion": "No coincide con la patente."})
        usadas = int((mysql_fetchone("SELECT COUNT(*) AS n FROM flota_solicitudes WHERE vehiculo_id=%s", (vid,)) or {}).get("n") or 0)
        if usadas:
            return _error(f"Ya tuvo {usadas} reserva{'s' if usadas != 1 else ''}: no se puede borrar definitivo porque se perdería su historial. Archívalo.",
                          409, error_codigo="TIENE_HISTORIAL")
        try:
            _log(vid, "eliminado", {"datos": {c: v.get(c) for c in fr.COLUMNAS}}, estricto=True)
            mysql_execute("UPDATE flota_archivos SET activo=0, quitado_por=%s, quitado_at=NOW() WHERE entidad='vehiculo' AND entidad_id=%s AND activo=1",
                          (current_username(), vid))
            mysql_execute("DELETE FROM flota_vehiculos WHERE id=%s", (vid,))
        except Exception as e:
            print(f"[flota] eliminar vehículo: {type(e).__name__}: {e}", flush=True)
            return _error("No se pudo borrar el vehículo. No se borró nada.", 500)
        return jsonify({"ok": True, "eliminado": True})

    @app.route("/flota/api/vehiculos/<int:vid>/historial", methods=["GET"])
    @_flota_gestion_required
    def flota_api_vehiculo_historial(vid):
        if not _fila(vid):
            return _error("No encontré ese vehículo.", 404)
        filas = mysql_fetchall(
            "SELECT id, accion, detalle, usuario, created_at FROM flota_vehiculo_log WHERE vehiculo_id=%s ORDER BY id DESC LIMIT 200",
            (vid,)) or []
        return jsonify({"ok": True, "eventos": _eventos_historial(filas)})

    # Usos del vehículo (Daniel, 03-nov-2026: «se van a guardar en la ficha del camión y se van a poder registrar las entregas de
    # cada uno»): cada retiro y devolución con lo reservado contra lo real y los km de ese uso. Lo ve quien entra a la flota (los
    # mismos datos que ya muestra la lista de solicitudes); las fotos siguen en el detalle de cada solicitud.
    def _utc_a_chile(dt):
        if not dt:
            return None
        try:
            dt = dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(_TZ_CL).replace(tzinfo=None) if _TZ_CL else dt.replace(tzinfo=None)
        except Exception:
            return None

    @app.route("/flota/api/vehiculos/<int:vid>/usos", methods=["GET"])
    @_flota_required
    def flota_api_vehiculo_usos(vid):
        filas = mysql_fetchall(_SQL_SOL + " WHERE s.vehiculo_id=%s AND s.estado IN ('llave_entregada','devuelta') "
                               "ORDER BY s.inicio DESC LIMIT 200", (vid,)) or []
        usos = fr.resumen_usos([{"id": r["id"], "numero": r.get("numero") or "", "estado": r.get("estado"), "estado_texto": fr.ESTADOS.get(r.get("estado"), ""),
                 "chofer": r.get("chofer_nombre") or "(sin nombre)", "area": r.get("area"), "solicitante": r.get("solicitante_nombre"),
                 "rango_texto": fr.rango_texto(r["inicio"], r["fin"]), "inicio": r["inicio"], "fin": r["fin"],
                 "llave_cl": _utc_a_chile(r.get("llave_at")), "devuelta_cl": _utc_a_chile(r.get("devuelta_at")),
                 "llave_fmt": _hora_chile(r.get("llave_at")), "llave_por": r.get("llave_por"),
                 "devuelta_fmt": _hora_chile(r.get("devuelta_at")), "devuelta_por": r.get("devuelta_por"),
                 "dev_km": r.get("dev_km"), "combustible_texto": fr.combustible_texto(r.get("dev_combustible")),
                 "novedad": bool(r.get("dev_novedad"))} for r in (dict(x) for x in filas)])
        graficos = fr.graficos_usos(usos, _ahora_chile().date())      # km por mes, horas por área y puntualidad por persona
        for u in usos:                    # las horas crudas solo sirven para calcular el desvío: al navegador va el texto en hora Chile
            for k in ("inicio", "fin", "llave_cl", "devuelta_cl"):
                u.pop(k, None)
        return jsonify({"ok": True, "usos": usos, "graficos": graficos, "tolerancia_min": fr.DESVIO_TOLERANCIA_MIN})

    _TITULOS_HISTORIAL = {
        "creado": "Registrado", "editado": "Datos modificados", "inhabilitado": "Inhabilitado", "habilitado": "Habilitado",
        "archivado": "Archivado", "desarchivado": "Desarchivado", "eliminado": "Borrado definitivo",
        "archivo_subido": "Archivo guardado", "archivo_quitado": "Archivo quitado", "licencia_verificada": "Licencia verificada con el escáner", "licencia_sin_verificar": "Licencia registrada SIN comprobar",
    }

    def _lineas_licencia(det):
        """Lo que dejó el escáner de licencias en la bitácora del chofer (Daniel, 05-nov-2026): qué leyó, qué corrigió y quién se saltó la comprobación."""
        out = []
        ll = det.get("licencia_leida") or {}
        if ll:
            try:
                vence = fr.fecha_corta(fr.fecha_o_none(ll.get("vence")))
            except ValueError:
                vence = ""
            partes = [("RUT " + fr.rut_formato(ll["rut"])) if ll.get("rut") else "", ("vence el " + vence) if vence else "",
                      ("clase " + ", ".join(ll["clases"])) if ll.get("clases") else ""]
            out.append("La licencia se leyó con el escáner: " + " · ".join(p for p in partes if p) + ".")
        if det.get("vence_elegida"):
            try:
                elegida = fr.fecha_corta(fr.fecha_o_none(det["vence_elegida"]))
            except ValueError:
                elegida = str(det["vence_elegida"])
            out.append(f"La persona eligió {elegida} como fecha de control entre las que se leyeron en la licencia (no era la sugerida).")
        if det.get("licencia_sin_verificar"):
            out.append("Registrada SIN comprobar la licencia (solo el superadministrador puede). Motivo: " + str(det["licencia_sin_verificar"]))
        return out + [str(a) for a in (det.get("avisos") or [])]

    def _eventos_historial(filas, chofer=False):
        eventos = []
        for f in filas:
            try:
                det = json.loads(f.get("detalle") or "null") or {}
            except Exception:
                det = {}
            acc = f["accion"]
            if acc == "editado":
                lineas = fr.lineas_de_cambios_chofer(det.get("cambios")) if chofer else fr.lineas_de_cambios(det.get("cambios"))
                if det.get("motivo"):
                    lineas = ["Motivo: " + det["motivo"]] + list(lineas)
            elif acc in ("inhabilitado", "archivado"):
                lineas = ["Motivo: " + (det.get("motivo") or "(sin motivo)")]
            elif acc == "habilitado":
                lineas = (["Estaba inhabilitado por: " + det["motivo_anterior"]] if det.get("motivo_anterior") else [])
            elif acc in ("archivo_subido", "archivo_quitado"):
                lineas = [f"{fr.ETIQUETAS_ARCHIVO.get(det.get('tipo'), det.get('tipo') or 'Archivo')}: {det.get('nombre') or ''}".strip(": ")]
            else:
                lineas = []
            if chofer and acc in ("creado", "editado", "licencia_verificada", "licencia_sin_verificar") and _flota_docs():
                lineas = list(lineas) + _lineas_licencia(det)           # RUT, fechas y nombres de la licencia: solo quien ve los documentos
            eventos.append({"accion": acc, "titulo": _TITULOS_HISTORIAL.get(acc, acc), "lineas": lineas,
                            "usuario": f.get("usuario") or "", "fecha": _hora_chile(f.get("created_at"))})
        return eventos

    # ── API de archivos (foto y PDF) ────────────────────────────────────────
    def _entidad_existe(entidad, ident):
        if entidad == "vehiculo":
            return _fila(ident)
        if entidad == "chofer":
            return _chofer_fila(ident)
        return None

    def _log_entidad(entidad, ident, accion, detalle):
        (_log if entidad == "vehiculo" else _clog)(ident, accion, detalle)

    def _guardar_archivo(entidad, ident, tipo, data, nombre_original, registrar=True):
        """Valida, sube a GCS y registra el archivo (el anterior NO se borra: queda como no vigente, con su historial).
        `registrar=False` cuando el evento de la bitácora lo escribe quien llama (el alta del chofer lo anota dentro de «creado»).
        → None si salió bien, o la respuesta de error (tupla) para devolver tal cual."""
        info, err = fr.validar_archivo(entidad, tipo, data)
        if err:
            return _error(err, 400, errores={"archivo": err})
        bucket = _bucket()
        if bucket is None:
            return _error("El almacenamiento de archivos no está disponible en este momento. Intenta de nuevo en unos minutos.", 503)
        contenido, ct, ext = data, info["content_type"], info["extension"]
        if info["clase"] == "imagen":
            try:
                resize = ctx.get("_img_resize_bytes")
                if resize:
                    contenido, ct = resize(data, max_dim=1600, quality=84)
                    ext = ".png" if ct == "image/png" else (".webp" if ct == "image/webp" else ".jpg")
            except Exception as e:
                print(f"[flota] redimensionar imagen: {type(e).__name__}", flush=True)
        nombre = fr.nombre_seguro(nombre_original or fr.ETIQUETAS_ARCHIVO.get(tipo, "archivo"), ext)
        clave = f"{_PREFIJO_PRIVADO}{entidad}/{ident}/{tipo}_{secrets.token_hex(8)}{ext}"
        try:
            blob = bucket.blob(clave)
            blob.cache_control = "private, max-age=0"
            blob.upload_from_string(contenido, content_type=ct)
        except Exception as e:
            print(f"[flota] subir archivo: {type(e).__name__}: {e}", flush=True)
            return _error("No se pudo guardar el archivo. Intenta de nuevo.", 502)
        quien = current_username()
        try:
            mysql_execute("UPDATE flota_archivos SET activo=0, quitado_por=%s, quitado_at=NOW() "
                          "WHERE entidad=%s AND entidad_id=%s AND tipo=%s AND activo=1", (quien, entidad, ident, tipo))
            mysql_execute(
                "INSERT INTO flota_archivos (entidad, entidad_id, tipo, nombre_original, content_type, bytes, gcs_key, sha256, subido_por) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (entidad, ident, tipo, nombre, ct, len(contenido), clave, hashlib.sha256(contenido).hexdigest(), quien))
        except Exception as e:
            print(f"[flota] registrar archivo: {type(e).__name__}: {e}", flush=True)
            return _error("El archivo se subió pero no quedó registrado. Intenta de nuevo.", 500)
        if registrar:
            _log_entidad(entidad, ident, "archivo_subido", {"tipo": tipo, "nombre": nombre, "bytes": len(contenido)})
        return None

    @app.route("/flota/api/archivos", methods=["POST"])
    @_flota_admin_required
    def flota_api_archivo_subir():
        """Guarda (o reemplaza) la foto o un PDF. El anterior NO se borra: queda como no vigente, con su historial."""
        entidad = (request.form.get("entidad") or "").strip()
        tipo = (request.form.get("tipo") or "").strip()
        ident = fr._id(request.form.get("entidad_id"))
        archivo = request.files.get("archivo")
        if entidad not in fr.TIPOS_ARCHIVO or not ident or archivo is None:
            return _error("Faltan datos para guardar el archivo.", 400)
        if not _entidad_existe(entidad, ident):
            return _error("No encontré a quién pertenece ese archivo.", 404)
        if entidad == "chofer" and tipo == "licencia" and _lic_verificar():
            # La licencia de un chofer solo entra por «Editar chofer», donde se LEE y se comprueba que sea de esa persona: esta ruta genérica
            # guardaría cualquier PDF y dejaría al chofer «reservable» sin pasar por el escáner (revisión de seguridad, 05-nov-2026).
            return _error("La licencia de un chofer se sube desde «Editar chofer», donde se lee y se comprueba que sea de esa persona.", 409,
                          error_codigo="LICENCIA_SOLO_POR_FORMULARIO")
        data = archivo.read(max(fr.ARCHIVO_MAX_BYTES.values()) + 1)
        err = _guardar_archivo(entidad, ident, tipo, data, getattr(archivo, "filename", ""))
        if err:
            return err
        return _devolver_entidad(entidad, ident)

    def _devolver_entidad(entidad, ident):
        if entidad == "vehiculo":
            return _vehiculo_respuesta(ident)
        f = _chofer_fila(ident)
        return jsonify({"ok": True, "chofer": _chofer_json(dict(f), _hoy(), _archivos_activos("chofer").get(ident)) if f else None})

    @app.route("/flota/api/archivos/<int:aid>", methods=["GET"])
    @_flota_required
    def flota_api_archivo_ver(aid):
        """Abre un archivo guardado. La foto, quien entra a la flota; los PDF y la licencia, solo con el permiso de documentos."""
        f = mysql_fetchone("SELECT * FROM flota_archivos WHERE id=%s", (aid,))
        if not f:
            return _error("No encontré ese archivo.", 404)
        # Las fotos de una devolución son evidencia de uso: las ve quien entra a la flota (como la foto del vehículo).
        if f["tipo"] != "foto" and not str(f["tipo"]).startswith("dev_") and not _flota_docs():
            return _error("No tienes permiso para ver los documentos de la flota.", 403, error_codigo="SIN_PERMISO_DOCUMENTOS_FLOTA")
        if f["tipo"] == "foto" and not f.get("activo") and not _flota_docs():
            return _error("No encontré ese archivo.", 404)
        bucket = _bucket()
        if bucket is None:
            return _error("El almacenamiento de archivos no está disponible en este momento.", 503)
        try:
            data = bucket.blob(f["gcs_key"]).download_as_bytes()
        except Exception as e:
            print(f"[flota] leer archivo {aid}: {type(e).__name__}", flush=True)
            return _error("No se pudo leer el archivo. Intenta de nuevo.", 502)
        resp = Response(data, mimetype=f.get("content_type") or "application/octet-stream")
        nombre = fr.nombre_seguro(f.get("nombre_original"), os.path.splitext(f["gcs_key"])[1] or "")
        resp.headers["Content-Disposition"] = f'inline; filename="{nombre}"'
        resp.headers["Cache-Control"] = "private, max-age=3600" if f["tipo"] == "foto" else "private, no-store"
        resp.headers["X-Content-Type-Options"] = "nosniff"
        return resp

    @app.route("/flota/api/vehiculos/<int:vid>/archivos", methods=["GET"])
    @_flota_docs_required
    def flota_api_vehiculo_repositorio(vid):
        """Repositorio de documentos del vehículo (Daniel, 02-nov-2026: «necesito un repositorio con los documentos»): TODOS los archivos que
        se le han subido, el vigente y los reemplazados o quitados, con quién y cuándo. Nada se borra."""
        try:
            filas = mysql_fetchall(
                "SELECT id, tipo, nombre_original, content_type, bytes, subido_por, subido_at, activo, quitado_por, quitado_at "
                "FROM flota_archivos WHERE entidad='vehiculo' AND entidad_id=%s ORDER BY id DESC LIMIT 300", (vid,)) or []
        except Exception as e:
            print(f"[flota] repositorio: {type(e).__name__}: {e}", flush=True)
            return _error("No se pudo leer el repositorio de documentos. Intenta de nuevo.", 500)
        out = []
        for f in filas:
            a = _archivo_json(f)
            a.update(vigente=bool(f.get("activo")), etiqueta=fr.ETIQUETAS_ARCHIVO.get(f.get("tipo"), f.get("tipo")),
                     quitado_por=f.get("quitado_por"), quitado_fmt=_hora_chile(f.get("quitado_at")))
            out.append(a)
        return jsonify({"ok": True, "archivos": out})

    @app.route("/flota/api/archivos/<int:aid>/quitar", methods=["POST"])
    @_flota_admin_required
    def flota_api_archivo_quitar(aid):
        """Deja de ser el archivo vigente. El original sigue guardado (queda en el historial)."""
        f = mysql_fetchone("SELECT * FROM flota_archivos WHERE id=%s", (aid,))
        if not f:
            return _error("No encontré ese archivo.", 404)
        if f["entidad"] == "chofer" and f["tipo"] == "licencia" and f.get("activo"):
            # Daniel, 04-nov-2026: un chofer registrado siempre tiene su licencia escaneada. Se reemplaza subiendo la nueva (la anterior queda).
            return _error("La licencia escaneada no se puede quitar: un chofer registrado siempre la tiene. Para cambiarla, sube la nueva "
                          "(la anterior queda guardada en el historial).", 409, error_codigo="LICENCIA_OBLIGATORIA")
        n = mysql_rowcount("UPDATE flota_archivos SET activo=0, quitado_por=%s, quitado_at=NOW() WHERE id=%s AND activo=1",
                           (current_username(), aid))
        if not n:
            return _error("Ese archivo ya no estaba vigente.", 409)
        _log_entidad(f["entidad"], f["entidad_id"], "archivo_quitado", {"tipo": f["tipo"], "nombre": f.get("nombre_original")})
        return _devolver_entidad(f["entidad"], f["entidad_id"])

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
        # El chofer externo no tiene usuario: su nombre, su teléfono y «activo» salen de su propia fila.
        "SELECT c.*, COALESCE(u.nombre, c.nombre_completo) AS u_nombre, u.username AS u_username, "
        "COALESCE(u.phone, c.telefono) AS u_phone, u.cargo AS u_cargo, "
        "CASE WHEN c.usuario_id IS NULL THEN 1 ELSE u.active END AS u_activo "
        "FROM flota_choferes c LEFT JOIN `" + _AUTH + "` u ON u.id = c.usuario_id")
    _SQL_SOL = (
        "SELECT s.*, v.patente AS v_patente, v.tipo AS v_tipo, v.marca AS v_marca, v.modelo AS v_modelo, v.km_actual AS v_km, "
        "COALESCE(u.nombre, c.nombre_completo) AS chofer_nombre, COALESCE(u.phone, c.telefono) AS chofer_phone, "
        "c.usuario_id AS chofer_usuario_id "
        "FROM flota_solicitudes s JOIN flota_vehiculos v ON v.id = s.vehiculo_id "
        "LEFT JOIN flota_choferes c ON c.id = s.chofer_id LEFT JOIN `" + _AUTH + "` u ON u.id = c.usuario_id")
    _SQL_OCUPA = (
        "SELECT s.id, s.numero, s.estado, s.inicio, s.fin, s.area, s.solicitante_nombre, s.sello_at, s.vehiculo_id, s.chofer_id, "
        "v.patente AS v_patente FROM flota_solicitudes s JOIN flota_vehiculos v ON v.id = s.vehiculo_id")

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
    def _chofer_json(r, hoy, archivos=None):
        niv = _niv()
        clases = fr.normalizar_clases(r.get("licencia_clases"))
        try:
            vence = fr.fecha_o_none(r.get("licencia_vence"))
        except ValueError:
            vence = None
        estado = fr.estado_vencimiento(vence, hoy) or "sin_fecha"
        dias = (vence - hoy).days if vence else None
        activo = bool(r.get("u_activo")) if r.get("u_activo") is not None else False
        lic = (archivos or {}).get("licencia")
        out = {
            "id": r["id"], "usuario_id": r.get("usuario_id"), "externo": r.get("usuario_id") is None,
            "nombre": r.get("u_nombre") or "(usuario sin nombre)", "username": r.get("u_username"),
            "telefono": r.get("u_phone"), "cargo": r.get("u_cargo"), "usuario_activo": activo,
            "licencia_clases": clases, "licencia_texto": ("Clase " + ", ".join(clases)) if clases else "Sin definir",
            "licencia_vence": vence.isoformat() if vence else None, "licencia_vence_fmt": fr.fecha_corta(vence),
            "licencia_estado": estado, "licencia_dias": dias,
            "licencia_texto_dias": fr.texto_dias(dias) if vence else "Sin fecha registrada",
            "tiene_licencia_pdf": bool(lic),
            # Sin la licencia escaneada el chofer queda registrado pero no puede recibir reservas (04-nov-2026).
            "falta_licencia_archivo": not bool(lic),
            "habilitado": bool(r.get("habilitado")),
            "inhabilitado_motivo": r.get("inhabilitado_motivo"), "inhabilitado_por": r.get("inhabilitado_por"),
            "inhabilitado_fmt": _hora_chile(r.get("inhabilitado_at")),
            "archivado": bool(r.get("archivado")), "archivado_motivo": r.get("archivado_motivo"),
            "archivado_por": r.get("archivado_por"), "archivado_fmt": _hora_chile(r.get("archivado_at")),
            "creado_por": r.get("created_by"), "creado_fmt": _hora_chile(r.get("created_at")),
            "actualizado_por": r.get("updated_by"), "actualizado_fmt": _hora_chile(r.get("updated_at")),
            # Puede recibir una reserva hoy: habilitado, con usuario activo, la licencia escaneada y sin vencer.
            "disponible": bool(r.get("habilitado")) and activo and bool(lic) and estado not in ("vencido", "sin_fecha"),
            # Lo personal (RUT, nombre completo, dirección, notas y el PDF de la licencia) solo con «Documentos y datos de la flota».
            "rut": None, "rut_fmt": "", "nombre_completo": None, "direccion": None, "notas": None, "licencia_archivo": None,
            "direccion_verificada": False, "direccion_lat": None, "direccion_lng": None, "direccion_place_id": None,
        }
        if niv["documentos"]:
            out.update(rut=r.get("rut"), rut_fmt=fr.rut_formato(r.get("rut")) if r.get("rut") else "",
                       nombre_completo=r.get("nombre_completo"), direccion=r.get("direccion"), notas=r.get("notas"),
                       direccion_verificada=bool(r.get("direccion_place_id")), direccion_lat=_num(r.get("direccion_lat")),
                       direccion_lng=_num(r.get("direccion_lng")), direccion_place_id=r.get("direccion_place_id"),
                       licencia_archivo=_archivo_json(lic) if lic else None)
        return out

    def _chofer_fila(cid):
        return mysql_fetchone(_SQL_CHOF + " WHERE c.id=%s", (cid,))

    def _clog(cid, accion, detalle=None, estricto=False):
        try:
            mysql_execute(
                "INSERT INTO flota_chofer_log (chofer_id, accion, detalle, usuario) VALUES (%s,%s,%s,%s)",
                (cid, accion, json.dumps(detalle, ensure_ascii=False, default=str) if detalle is not None else None,
                 (current_username() or "")[:190] or None))
        except Exception as e:
            print(f"[flota] bitácora de chofer ({accion}): {type(e).__name__}", flush=True)
            if estricto:
                raise

    def _reservas_abiertas(campo, ident):
        """Cuántas reservas vivas (que aún no terminan) tiene un vehículo o un chofer: sirve para AVISAR al inhabilitarlo y para no archivarlo."""
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

    def _chofer_respuesta(cid, **extra):
        f = _chofer_fila(cid)
        return jsonify({"ok": True, "chofer": _chofer_json(dict(f), _hoy(), _archivos_activos("chofer").get(cid)) if f else None, **extra})

    @app.route("/flota/api/choferes", methods=["GET"])
    @_flota_required
    def flota_api_choferes():
        archivados = 1 if (request.args.get("archivados") == "1" and _flota_admin()) else 0
        try:
            filas = mysql_fetchall(_SQL_CHOF + " WHERE c.archivado=%s ORDER BY c.habilitado DESC, COALESCE(u.nombre, c.nombre_completo) LIMIT 500",
                                   (archivados,)) or []
            n_arch = int((mysql_fetchone("SELECT COUNT(*) AS n FROM flota_choferes WHERE archivado=1") or {}).get("n") or 0) if _flota_admin() else 0
        except Exception as e:
            print(f"[flota] listar choferes: {type(e).__name__}: {e}", flush=True)
            return _error("No se pudo leer el repositorio de choferes. Intenta de nuevo en un momento.", 500)
        hoy = _hoy()
        arch = _archivos_activos("chofer")
        choferes = [_chofer_json(dict(f), hoy, arch.get(f["id"])) for f in filas]
        resumen = {
            "total": len(choferes),
            "habilitados": sum(1 for c in choferes if c["habilitado"]),
            "inhabilitados": sum(1 for c in choferes if not c["habilitado"]),
            "por_vencer": sum(1 for c in choferes if c["habilitado"] and c["licencia_estado"] == "por_vencer"),
            "vencidas": sum(1 for c in choferes if c["habilitado"] and c["licencia_estado"] == "vencido"),
            "archivados": n_arch,
        }
        return jsonify({"ok": True, "choferes": choferes, "resumen": resumen, "config": _config(), "hoy": hoy.isoformat(),
                        "permisos": _niv(), "ver_archivados": bool(archivados)})

    @app.route("/flota/api/choferes/usuarios", methods=["GET"])
    @_flota_admin_required
    def flota_api_choferes_usuarios():
        """Usuarios de ILUS que todavía no son chofer: de ahí se elige a quién dar de alta. Trae lo que ya tiene su perfil (RUT, dirección)
        para SUGERIRLO en el formulario; la persona decide qué queda (REGLA #32)."""
        try:
            filas = mysql_fetchall(
                "SELECT id, nombre, username, phone, cargo, rut, direccion, direccion_lat, direccion_lng, direccion_place_id FROM `"
                + _AUTH + "` WHERE active=1 AND id NOT IN "
                "(SELECT usuario_id FROM flota_choferes WHERE usuario_id IS NOT NULL) ORDER BY nombre LIMIT 1000") or []
        except Exception as e:
            print(f"[flota] usuarios para chofer: {type(e).__name__}: {e}", flush=True)
            return _error("No se pudo leer la lista de usuarios. Intenta de nuevo en un momento.", 500)
        return jsonify({"ok": True, "usuarios": [
            {"id": f["id"], "nombre": f.get("nombre") or f.get("username"), "username": f.get("username"),
             "telefono": f.get("phone"), "cargo": f.get("cargo"),
             "rut": f.get("rut") or "", "direccion": f.get("direccion") or "",
             # Si su dirección del perfil ya está validada con Google, viaja con su ubicación y se puede sugerir tal cual.
             "direccion_lat": _num(f.get("direccion_lat")), "direccion_lng": _num(f.get("direccion_lng")),
             "direccion_place_id": f.get("direccion_place_id") or ""} for f in filas]})

    def _datos_de_peticion():
        """(datos, archivo) del alta o la edición de un chofer: JSON, o formulario con la licencia escaneada adjunta (campo «archivo»)."""
        if request.is_json:
            return (request.get_json(silent=True) or {}), None
        datos = {k: request.form.get(k) for k in request.form.keys()}
        datos["licencia_clases"] = [c for v in request.form.getlist("licencia_clases") for c in re.split(r"[,/;\s]+", v or "") if c]
        archivo = request.files.get("archivo")
        if archivo is not None and not (getattr(archivo, "filename", "") or "").strip():
            archivo = None                       # un campo de archivo vacío del navegador llega sin nombre: es «no adjuntó nada»
        return datos, archivo

    def _leer_licencia(archivo, obligatoria):
        """(bytes, error) de la licencia escaneada (foto o PDF) que viaja con el registro del chofer."""
        if archivo is None:
            return None, (fr.MSG_LICENCIA_SIN_ARCHIVO if obligatoria else None)
        data = archivo.read(max(fr.ARCHIVO_MAX_BYTES.values()) + 1)
        _info, err = fr.validar_archivo("chofer", "licencia", data)
        return (None, err) if err else (data, None)

    # ── ESCÁNER DE LICENCIAS (Daniel, 05-nov-2026) ──────────────────────────────────────────────────────────────────────
    # «Restricciones para las malas operaciones que logística está generando: un scanner de licencias, por el celular o subiendo una foto,
    # y que mediante ese scanner se agregue la licencia y sobre todo la fecha, así evitamos que Milagros cree choferes con su usuario».
    # Eligió el OCR PROPIO: Tesseract en el servidor, la foto no sale de ILUS. Las reglas viven en flota_licencia_lector (puro) y el motor en
    # flota_licencia_ocr; aquí solo se conectan: LEER (no guarda nada), COMPARAR y EXIGIR al guardar un chofer. Lo que la licencia dice manda:
    # el RUT tiene que ser de la persona y de su usuario de ILUS, el vencimiento sale de la licencia y no se escribe a mano.
    def _lic_verificar():
        """Encendido en producción. Las pruebas anteriores lo apagan (ctx['licencia_verificar'] = False): el OCR real necesita Tesseract."""
        return bool(ctx.get("licencia_verificar", True))

    def _lic_leer(data):
        """El lector de verdad (Tesseract) o, en pruebas, el que se inyecta en ctx['licencia_leer']."""
        return (ctx.get("licencia_leer") or lico.leer_licencia)(data)

    _LIC_OMITIR_MIN = 20                                              # letras mínimas del motivo para saltarse la comprobación (solo superadmin)
    _lic_golpes = {}
    MSG_SIN_LECTOR = ("El lector de licencias no está disponible en este momento, así que no puedo comprobar la licencia. "
                      "Intenta de nuevo en unos minutos o avisa a soporte.")

    def _es_superadmin():
        return fr.es_superadmin(g.get("permissions"))

    def _lic_ritmo_ok():
        """Máximo 12 lecturas por minuto y por persona: leer gasta CPU del servidor."""
        t = _time_mod.time()
        k = _uid() or 0
        lista = [x for x in _lic_golpes.get(k, []) if t - x < 60]
        ok = len(lista) < 12
        if ok:
            lista.append(t)
        _lic_golpes[k] = lista
        return ok

    _lic_cupo = threading.BoundedSemaphore(2)      # a lo más 2 lecturas a la vez en este proceso: cada una gasta CPU y memoria (la app tiene 2 vCPU)

    def _estado_http(r):
        """Código HTTP de una lectura que no salió bien."""
        return {"archivo": 400, "ocupado": 429}.get(r["estado"], 500)

    def _lic_leer_seguro(data):
        """{'estado': 'ok'|'sin_motor'|'archivo'|'ocupado'|'error', 'lectura', 'meta', 'mensaje'}. Nunca lanza."""
        if not _lic_cupo.acquire(blocking=False):
            return {"estado": "ocupado", "mensaje": "El lector está atendiendo otras licencias. Espera unos segundos e inténtalo de nuevo."}
        try:
            return _lic_leer_con_cupo(data)
        finally:
            _lic_cupo.release()

    def _lic_leer_con_cupo(data):
        try:
            lectura, meta = _lic_leer(data)
            return {"estado": "ok", "lectura": lectura, "meta": meta or {}}
        except lico.MotorNoDisponible as e:
            print(f"[flota] lector de licencias no disponible: {e}", flush=True)
            return {"estado": "sin_motor", "mensaje": MSG_SIN_LECTOR}
        except lico.ArchivoIlegible as e:
            return {"estado": "archivo", "mensaje": f"{e} {licl.MSG_ILEGIBLE}"}
        except Exception as e:
            print(f"[flota] leer licencia: {type(e).__name__}: {e}", flush=True)
            return {"estado": "error", "mensaje": "No pude leer la licencia por un problema del servidor. Intenta de nuevo en un momento."}

    class _UsuarioNoLeido(Exception):
        """La base no respondió al buscar al usuario: NO se puede dar por buena la comparación (no se salta en silencio)."""

    def _lic_usuario(uid):
        """{'nombre', 'rut'} del usuario ACTIVO de ILUS elegido, o None si no existe o está desactivado. Si la base falla, lanza _UsuarioNoLeido."""
        if not uid:
            return None
        try:
            u = mysql_fetchone("SELECT id, nombre, rut FROM `" + _AUTH + "` WHERE id=%s AND active=1", (uid,))
        except Exception as e:
            print(f"[flota] usuario para comparar la licencia: {type(e).__name__}", flush=True)
            raise _UsuarioNoLeido() from e
        return {"nombre": u.get("nombre"), "rut": u.get("rut")} if u else None

    def _lic_verificacion(lectura, campos):
        """Compara una lectura con lo que se está registrando (campos: usuario_id, rut, nombre_completo, licencia_clases)."""
        clases = campos.get("licencia_clases")
        if hasattr(campos, "getlist"):
            clases = [c for v in campos.getlist("licencia_clases") for c in re.split(r"[,/;\s]+", v or "") if c]
        uid = _id_o_none(campos.get("usuario_id"))
        try:
            declarada = fr.fecha_o_none(campos.get("licencia_vence"))
        except ValueError:
            declarada = None
        try:
            usuario = _lic_usuario(uid)
        except _UsuarioNoLeido:
            return {"ok": False, "errores": {"usuario_id": "No pude comprobar el usuario en este momento. Intenta de nuevo."}, "avisos": []}
        errores, avisos = licl.evaluar(
            lectura, rut_chofer=(campos.get("rut") or None), nombre_chofer=(campos.get("nombre_completo") or None),
            usuario=usuario, clases_declaradas=(clases or None), hoy=_hoy(), vence_declarado=declarada)
        return {"ok": not errores, "errores": errores, "avisos": avisos}

    def _id_o_none(v):
        try:
            n = int(str(v).strip())
        except (TypeError, ValueError):
            return None
        return n if n > 0 else None

    def _lic_motivo_omitir(payload):
        """Motivo escrito para saltarse la comprobación: SOLO lo vale si lo pide el superadministrador y trae al menos 20 caracteres."""
        motivo = re.sub(r"\s+", " ", str((payload or {}).get("licencia_sin_verificar_motivo") or "")).strip()[:300]
        return motivo if (_es_superadmin() and len(motivo) >= _LIC_OMITIR_MIN and len(motivo.split()) >= 3) else None

    def _licencia_exigir(data_lic, payload, datos, usuario_id):
        """(respuesta_de_error | None, detalle_para_la_bitácora). Con una licencia nueva en la mano, el servidor la LEE de nuevo (no confía en lo
        que dice el navegador) y exige que sea de la persona que se registra y de su usuario de ILUS. El vencimiento y las clases salen de la
        licencia: `datos` se corrige con lo leído. Si el lector no puede leerla o no coincide, no se guarda."""
        if not _lic_verificar() or data_lic is None:
            return None, {}
        motivo = _lic_motivo_omitir(payload)
        if motivo:
            return None, {"licencia_sin_verificar": motivo}
        r = _lic_leer_seguro(data_lic)
        if r["estado"] == "sin_motor":
            return _error(r["mensaje"], 503, error_codigo="LECTOR_NO_DISPONIBLE", puede_omitir=_es_superadmin()), {}
        if r["estado"] == "ocupado":
            return _error(r["mensaje"], 429, error_codigo="LECTOR_OCUPADO"), {}
        if r["estado"] != "ok":
            return _error(r["mensaje"], _estado_http(r), errores={"licencia_archivo": r["mensaje"]},
                          error_codigo="LICENCIA_ILEGIBLE", puede_omitir=_es_superadmin()), {}
        lectura = r["lectura"]
        try:
            usuario = _lic_usuario(usuario_id)
        except _UsuarioNoLeido:
            return _error("No pude comprobar el usuario de ILUS en este momento: no se registra al chofer. Intenta de nuevo en unos segundos.", 503,
                          error_codigo="USUARIO_NO_COMPROBADO"), {}
        errores, avisos = licl.evaluar(lectura, rut_chofer=datos.get("rut"), nombre_chofer=datos.get("nombre_completo"),
                                       usuario=usuario, clases_declaradas=datos.get("licencia_clases"), hoy=_hoy(),
                                       vence_declarado=datos.get("licencia_vence"))
        if errores:
            codigo = "LICENCIA_ILEGIBLE" if "licencia_archivo" in errores else "LICENCIA_NO_COINCIDE"
            return _error("La licencia no coincide con el registro: revisa los campos marcados.", 400, errores=errores, error_codigo=codigo,
                          lectura=licl.lectura_a_json(lectura), puede_omitir=_es_superadmin()), {}
        j = licl.lectura_a_json(lectura)
        detalle = {"licencia_leida": {"rut": j["rut"], "vence": j["vence"], "clases": j["clases"], "nombre": j["nombre"],
                                      "fechas": [f["iso"] for f in j["fechas"]]}}
        # La fecha que se guarda es una de las que se VIERON en la foto: la que la persona eligió (ya se comprobó que está entre ellas) o, si no
        # declaró ninguna, la que se reconoció como de control. Nunca una inventada.
        elegida, _err = licl.fecha_de_control(lectura, datos.get("licencia_vence"))
        if elegida is not None and elegida != lectura.get("vence"):
            detalle["vence_elegida"] = elegida.isoformat()
        if elegida is not None:
            datos["licencia_vence"] = elegida
        if avisos:
            detalle["avisos"] = avisos
        return None, detalle

    @app.route("/flota/api/licencia/leer", methods=["POST"])
    @_flota_admin_required
    def flota_api_licencia_leer():
        """Lee la licencia (foto del celular o archivo) y dice qué trae y si coincide con lo que se está registrando. NO guarda nada."""
        data, err = _leer_licencia(request.files.get("archivo"), obligatoria=True)
        if err:
            return _error(err, 400, errores={"licencia_archivo": err})
        if not _lic_ritmo_ok():
            return _error("Estás escaneando muy seguido. Espera un momento e inténtalo de nuevo.", 429, error_codigo="LECTURA_MUY_SEGUIDA")
        r = _lic_leer_seguro(data)
        if r["estado"] == "sin_motor":
            return _error(r["mensaje"], 503, error_codigo="LECTOR_NO_DISPONIBLE", puede_omitir=_es_superadmin())
        if r["estado"] == "ocupado":
            return _error(r["mensaje"], 429, error_codigo="LECTOR_OCUPADO")
        if r["estado"] != "ok":
            return _error(r["mensaje"], _estado_http(r), errores={"licencia_archivo": r["mensaje"]}, puede_omitir=_es_superadmin())
        meta = r["meta"] or {}
        resp = jsonify({"ok": True, "lectura": licl.lectura_a_json(r["lectura"]), "verificacion": _lic_verificacion(r["lectura"], request.form),
                        "motor": meta.get("motor"), "ms": meta.get("ms"), "puede_omitir": _es_superadmin()})
        resp.headers["Cache-Control"] = "no-store"               # lleva RUT y nombre del titular: que ningún caché lo guarde
        return resp

    @app.route("/flota/api/licencia/comparar", methods=["POST"])
    @_flota_admin_required
    def flota_api_licencia_comparar():
        """Vuelve a comparar una lectura YA hecha con lo que se está escribiendo (cambió el usuario, el RUT o las clases): sin repetir el OCR.
        Es solo la vista previa de la pantalla; la comprobación que vale es la del guardado, que lee la licencia otra vez."""
        if not _lic_ritmo_ok():
            return _error("Estás comparando muy seguido. Espera un momento.", 429, error_codigo="LECTURA_MUY_SEGUIDA")
        p = request.get_json(silent=True) or {}
        l = p.get("lectura") or {}
        try:
            vence = fr.fecha_o_none(l.get("vence"))
        except ValueError:
            vence = None
        rut = fr.normalizar_rut(l.get("rut")) if l.get("rut") else None
        rut = rut if rut and fr.rut_valido(rut) else None
        leidos = [fr.normalizar_rut(x) or str(x) for x in (l.get("ruts_leidos") or [])][:20]
        fechas = []
        for x in (l.get("fechas") or [])[:20]:
            try:
                f = fr.fecha_o_none(x.get("iso") if isinstance(x, dict) else x)
            except ValueError:
                f = None
            if f:
                fechas.append(f)
        lectura = {"rut": rut, "vence": vence, "clases": fr.normalizar_clases(l.get("clases")), "nombre": (str(l.get("nombre") or "").strip() or None),
                   "palabras": [str(x) for x in (l.get("palabras") or [])][:400], "ruts_leidos": leidos, "fechas": sorted(set(fechas)), "avisos": [],
                   "legible": bool((rut or leidos) and (vence or fechas))}
        resp = jsonify({"ok": True, "verificacion": _lic_verificacion(lectura, p)})
        resp.headers["Cache-Control"] = "no-store"
        return resp

    @app.route("/flota/api/licencia/estado", methods=["GET"])
    @_flota_admin_required
    def flota_api_licencia_estado():
        """¿Está el lector disponible? La pantalla lo pregunta al abrir el formulario del chofer para avisar antes de que la persona pierda tiempo."""
        if not _lic_verificar() or ctx.get("licencia_leer"):
            return jsonify({"ok": True, "disponible": True, "motor": "desactivado" if not _lic_verificar() else "inyectado"})
        ok, info = lico.motor_disponible()
        return jsonify({"ok": True, "disponible": bool(ok), "motor": info if ok else None})

    @app.route("/flota/api/choferes", methods=["POST"])
    @_flota_admin_required
    def flota_api_chofer_crear():
        payload, archivo = _datos_de_peticion()
        # Daniel, 03-nov-2026: «todos tienen que pertenecer a la página. Si no está registrado, no puede retirar el camión».
        if payload.get("externo") in (True, 1, "1", "true", "si") or not payload.get("usuario_id"):
            return _error("El chofer tiene que ser un usuario de ILUS: si no está registrado, no puede retirar el vehículo. "
                          "Créale su usuario y después agrégalo aquí.", 400, errores={"usuario_id": "Elige un usuario de ILUS."},
                          error_codigo="CHOFER_SIN_USUARIO")
        datos, errores = fr.validar_chofer(payload)
        data_lic, err_lic = _leer_licencia(archivo, obligatoria=True)
        if err_lic:
            errores["licencia_archivo"] = err_lic
        if errores:
            return _error("Revisa los campos marcados.", 400, errores=errores)
        u = mysql_fetchone("SELECT id, nombre, active FROM `" + _AUTH + "` WHERE id=%s", (datos["usuario_id"],))
        if not u or not u.get("active"):
            return _error("Ese usuario no existe o está desactivado.", 400, errores={"usuario_id": "Elige un usuario activo de ILUS."})
        if _bucket() is None:
            return _error("El almacenamiento de archivos no está disponible en este momento: sin guardar la licencia no se puede registrar al chofer. "
                          "Intenta de nuevo en unos minutos.", 503)
        resp_lic, det_lic = _licencia_exigir(data_lic, payload, datos, datos["usuario_id"])        # la licencia tiene que ser de esta persona y de su usuario
        if resp_lic is not None:
            return resp_lic
        try:
            mysql_execute(
                "INSERT INTO flota_choferes (usuario_id, rut, nombre_completo, direccion, direccion_lat, direccion_lng, direccion_place_id, "
                "licencia_clases, licencia_vence, notas, created_by, updated_by) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (datos["usuario_id"], datos["rut"], datos["nombre_completo"], datos["direccion"], datos["direccion_lat"],
                 datos["direccion_lng"], datos["direccion_place_id"], datos["licencia_clases"],
                 datos["licencia_vence"], datos["notas"], current_username(), current_username()))
        except Exception as e:
            if _es_duplicado(e):
                return _error(f"{u.get('nombre') or 'Ese usuario'} ya está en el repositorio de choferes.", 409,
                              errores={"usuario_id": "Ya es chofer."})
            print(f"[flota] crear chofer: {type(e).__name__}: {e}", flush=True)
            return _error("No se pudo guardar el chofer. Intenta de nuevo.", 500)
        fila = mysql_fetchone(_SQL_CHOF + " WHERE c.usuario_id=%s", (datos["usuario_id"],))
        if not fila:
            return _error("El chofer se guardó pero no se pudo leer de vuelta. Recarga la pantalla.", 500)
        # La licencia viaja con el registro: si no se puede guardar, el chofer NO queda registrado (se deshace lo recién creado).
        err_subida = _guardar_archivo("chofer", fila["id"], "licencia", data_lic, getattr(archivo, "filename", ""), registrar=False)
        if err_subida:
            try:
                mysql_execute("DELETE FROM flota_choferes WHERE id=%s", (fila["id"],))
            except Exception as e:
                print(f"[flota] deshacer chofer sin licencia: {type(e).__name__}: {e}", flush=True)
            return err_subida
        _clog(fila["id"], "creado", dict({"usuario": u.get("nombre"), "licencia_clases": datos["licencia_clases"], "licencia_escaneada": True}, **det_lic))
        return _chofer_respuesta(fila["id"])

    @app.route("/flota/api/choferes/<int:cid>", methods=["PUT"])
    @_flota_admin_required
    def flota_api_chofer_editar(cid):
        actual = _chofer_fila(cid)
        if not actual:
            return _error("No encontré ese chofer.", 404)
        externo = actual.get("usuario_id") is None
        payload, archivo = _datos_de_peticion()
        datos, errores = fr.validar_chofer(payload, con_usuario=False, direccion_actual=actual.get("direccion"), externo=externo)
        # Un chofer antiguo sin la licencia escaneada no se guarda hasta adjuntarla; si ya la tiene, adjuntar otra la reemplaza. El externo
        # antiguo (sin usuario de ILUS) no la necesita para corregirle un dato: igual no puede retirar vehículos (CHOFER_SIN_USUARIO).
        tiene_lic = bool((_archivos_activos("chofer").get(cid) or {}).get("licencia"))
        data_lic, err_lic = _leer_licencia(archivo, obligatoria=not tiene_lic and not externo)
        if err_lic:
            errores["licencia_archivo"] = err_lic
        if errores:
            return _error("Revisa los campos marcados.", 400, errores=errores)
        resp_lic, det_lic = _licencia_exigir(data_lic, payload, datos, actual.get("usuario_id"))   # con licencia nueva: se lee y tiene que coincidir
        if resp_lic is not None:
            return resp_lic
        cambios = fr.cambios(dict(actual), datos, campos=fr.CHOFER_COLUMNAS)
        if _lic_verificar() and data_lic is None and tiene_lic and any(c in cambios for c in ("rut", "licencia_vence", "licencia_clases")):
            # El RUT, las clases y el vencimiento SALEN de la licencia: con una ya escaneada no se reescriben a mano (Daniel, 05-nov-2026).
            motivo_omitir = _lic_motivo_omitir(payload)
            if not motivo_omitir:
                return _error("El RUT, las clases y el vencimiento salen de la licencia: para cambiarlos escanea la licencia nueva.", 400,
                              errores={"licencia_archivo": "Escanea la licencia nueva: el RUT, las clases y el vencimiento no se cambian a mano."},
                              error_codigo="LICENCIA_REQUIERE_ESCANEO", puede_omitir=_es_superadmin())
            det_lic = {"licencia_sin_verificar": motivo_omitir}
        if externo and (actual.get("telefono") or None) != datos.get("telefono"):
            cambios["telefono"] = {"antes": actual.get("telefono"), "despues": datos.get("telefono")}
        if datos["direccion_accion"] == "guardar" and (actual.get("direccion_place_id") or "") != (datos["direccion_place_id"] or ""):
            cambios["direccion"] = {"antes": actual.get("direccion") or None, "despues": (datos["direccion"] or "") + " (verificada con Google)"}
        if not cambios:
            if data_lic is None:
                return jsonify({"ok": True, "sin_cambios": True,
                                "chofer": _chofer_json(dict(actual), _hoy(), _archivos_activos("chofer").get(cid))})
            err_subida = _guardar_archivo("chofer", cid, "licencia", data_lic, getattr(archivo, "filename", ""))
            if det_lic and not err_subida:
                _clog(cid, "licencia_sin_verificar" if det_lic.get("licencia_sin_verificar") else "licencia_verificada", det_lic)
            return err_subida if err_subida else _chofer_respuesta(cid)
        try:
            if datos["direccion_accion"] == "mantener":       # misma dirección de antes: su ubicación no se toca
                mysql_execute(
                    "UPDATE flota_choferes SET rut=%s, nombre_completo=%s, direccion=%s, licencia_clases=%s, licencia_vence=%s, notas=%s, "
                    "updated_by=%s WHERE id=%s",
                    (datos["rut"], datos["nombre_completo"], datos["direccion"], datos["licencia_clases"], datos["licencia_vence"],
                     datos["notas"], current_username(), cid))
            else:
                mysql_execute(
                    "UPDATE flota_choferes SET rut=%s, nombre_completo=%s, direccion=%s, direccion_lat=%s, direccion_lng=%s, "
                    "direccion_place_id=%s, licencia_clases=%s, licencia_vence=%s, notas=%s, updated_by=%s WHERE id=%s",
                    (datos["rut"], datos["nombre_completo"], datos["direccion"], datos["direccion_lat"], datos["direccion_lng"],
                     datos["direccion_place_id"], datos["licencia_clases"], datos["licencia_vence"], datos["notas"], current_username(), cid))
        except Exception as e:
            print(f"[flota] editar chofer: {type(e).__name__}: {e}", flush=True)
            return _error("No se pudo guardar los cambios. Intenta de nuevo.", 500)
        if externo and "telefono" in cambios:
            try:
                mysql_execute("UPDATE flota_choferes SET telefono=%s, updated_by=%s WHERE id=%s",
                              (datos["telefono"], current_username(), cid))
            except Exception as e:
                print(f"[flota] teléfono de chofer externo: {type(e).__name__}: {e}", flush=True)
                return _error("No se pudo guardar el teléfono. Intenta de nuevo.", 500)
        _clog(cid, "editado", dict({"cambios": cambios}, **det_lic))
        if data_lic is not None:
            err_subida = _guardar_archivo("chofer", cid, "licencia", data_lic, getattr(archivo, "filename", ""))
            if err_subida:
                return err_subida
        return _chofer_respuesta(cid)

    @app.route("/flota/api/choferes/<int:cid>/inhabilitar", methods=["POST"])
    @_flota_admin_required
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
        f = _chofer_fila(cid)
        c = _chofer_json(dict(f), _hoy(), _archivos_activos("chofer").get(cid))
        return jsonify({"ok": True, "chofer": c, "avisos": _aviso_reservas(_reservas_abiertas("chofer", cid), c["nombre"])})

    @app.route("/flota/api/choferes/<int:cid>/habilitar", methods=["POST"])
    @_flota_admin_required
    def flota_api_chofer_habilitar(cid):
        previo = _chofer_fila(cid)
        if not previo:
            return _error("No encontré ese chofer.", 404)
        if previo.get("archivado"):
            return _error("Este chofer está archivado: primero desarchívalo.", 409)
        n = mysql_rowcount(
            "UPDATE flota_choferes SET habilitado=1, inhabilitado_motivo=NULL, inhabilitado_por=NULL, inhabilitado_at=NULL, updated_by=%s "
            "WHERE id=%s AND habilitado=0", (current_username(), cid))
        if not n:
            return _error("Ese chofer ya estaba habilitado.", 409)
        _clog(cid, "habilitado", {"motivo_anterior": previo.get("inhabilitado_motivo")})
        f = _chofer_fila(cid)
        c = _chofer_json(dict(f), _hoy(), _archivos_activos("chofer").get(cid))
        avisos = []
        if c["licencia_estado"] in ("vencido", "por_vencer"):
            avisos.append(f"Licencia de {c['nombre']}: {c['licencia_texto_dias'].lower()}.")
        return jsonify({"ok": True, "chofer": c, "avisos": avisos})

    @app.route("/flota/api/choferes/<int:cid>/archivar", methods=["POST"])
    @_flota_admin_required
    def flota_api_chofer_archivar(cid):
        c = _chofer_fila(cid)
        if not c:
            return _error("No encontré ese chofer.", 404)
        motivo, err = fr.validar_motivo_archivo((request.get_json(silent=True) or {}).get("motivo"))
        if err:
            return _error(err, 400, errores={"motivo": err})
        n_res = _reservas_abiertas("chofer", cid)
        if n_res:
            return _error(f"Tiene {n_res} reserva{'s' if n_res != 1 else ''} por delante: muévelas o cancélalas antes de archivarlo.", 409,
                          error_codigo="TIENE_RESERVAS")
        quien = current_username()
        n = mysql_rowcount(
            "UPDATE flota_choferes SET archivado=1, archivado_motivo=%s, archivado_por=%s, archivado_at=NOW(), habilitado=0, "
            "inhabilitado_motivo=%s, inhabilitado_por=%s, inhabilitado_at=NOW(), updated_by=%s WHERE id=%s AND archivado=0",
            (motivo, quien, "Archivado: " + motivo, quien, quien, cid))
        if not n:
            return _error("Ese chofer ya estaba archivado.", 409)
        _clog(cid, "archivado", {"motivo": motivo})
        return _chofer_respuesta(cid)

    @app.route("/flota/api/choferes/<int:cid>/desarchivar", methods=["POST"])
    @_flota_admin_required
    def flota_api_chofer_desarchivar(cid):
        if not _chofer_fila(cid):
            return _error("No encontré ese chofer.", 404)
        n = mysql_rowcount(
            "UPDATE flota_choferes SET archivado=0, archivado_motivo=NULL, archivado_por=NULL, archivado_at=NULL, updated_by=%s "
            "WHERE id=%s AND archivado=1", (current_username(), cid))
        if not n:
            return _error("Ese chofer no estaba archivado.", 409)
        _clog(cid, "desarchivado", {})
        return _chofer_respuesta(cid, avisos=["Volvió inhabilitado: usa «Habilitar» cuando quieras dejarlo disponible."])

    @app.route("/flota/api/choferes/<int:cid>", methods=["DELETE"])
    @_flota_admin_required
    def flota_api_chofer_eliminar(cid):
        """Borrado DEFINITIVO, escribiendo su nombre. Queda anotado ANTES de borrar (REGLA #5).
        Sin reservas lo hace quien administra. CON reservas (Daniel, 05-nov-2026, en plena operación: «necesito eliminar choferes así tengan
        agendas») solo el superadministrador y a propósito (`forzar`): las reservas que aún no empiezan se cancelan (con su motivo y su
        bitácora) y las pasadas se conservan. Si el chofer TIENE UNA LLAVE AFUERA nunca se borra, ni siquiera forzando: antes se registra la devolución."""
        c = _chofer_fila(cid)
        if not c:
            return _error("No encontré ese chofer.", 404)
        cuerpo = request.get_json(silent=True) or {}
        esperado = c.get("nombre_completo") or c.get("u_nombre") or ""
        if not fr.coincide_confirmacion(cuerpo.get("confirmacion"), esperado):
            return _error("Para borrarlo definitivo escribe su nombre completo.", 400, errores={"confirmacion": "No coincide con el nombre."})
        super_ = _es_superadmin()
        sql_reservas = "SELECT id, numero, estado FROM flota_solicitudes WHERE chofer_id=%s"
        reservas = mysql_fetchall(sql_reservas, (cid,)) or []
        abiertas = [r for r in reservas if r["estado"] in ("solicitada", "confirmada")]
        con_llave = [r for r in reservas if r["estado"] == "llave_entregada"]
        if con_llave:
            nums = ", ".join(str(r.get("numero") or f"#{r['id']}") for r in con_llave[:5])
            return _error(f"Tiene la llave de un vehículo en su poder (reserva {nums}): primero registra la devolución y después lo puedes borrar.",
                          409, error_codigo="LLAVE_AFUERA", puede_forzar=False, reservas=len(reservas), abiertas=len(abiertas), con_llave=len(con_llave))
        forzar = bool(cuerpo.get("forzar")) and super_
        if reservas and not forzar:
            n = len(reservas)
            return _error(f"Ya tuvo {n} reserva{'s' if n != 1 else ''}: no se puede borrar definitivo porque se perdería su historial. Archívalo.",
                          409, error_codigo="TIENE_HISTORIAL", puede_forzar=super_, reservas=n, abiertas=len(abiertas), con_llave=0)
        quien = current_username() or "el superadministrador"
        nombre = esperado or f"chofer #{cid}"
        motivo = f"Se eliminó al chofer {nombre} (por {quien}). El horario quedó libre."[:300]
        canceladas = []
        try:
            # Primero queda escrito qué se va a borrar y qué reservas tenía; si esta anotación falla, no se toca nada.
            _clog(cid, "eliminado", {
                "datos": {k: c.get(k) for k in ("usuario_id", "u_nombre", "rut", "nombre_completo", "direccion", "licencia_clases", "licencia_vence", "notas")},
                "forzado": bool(reservas), "reservas_total": len(reservas),
                "reservas": [{"numero": r.get("numero"), "estado": r["estado"]} for r in reservas[:200]]}, estricto=True)
            for r in abiertas:
                # Atómico: si otra persona la movió mientras tanto (ej. entregó la llave) no se pisa.
                if mysql_rowcount("UPDATE flota_solicitudes SET estado='cancelada', cancelada_por=%s, cancelada_at=NOW(), cancelada_motivo=%s "
                                  "WHERE id=%s AND estado IN ('solicitada','confirmada')", (quien, motivo, r["id"])):
                    _slog(r["id"], "cancelada", {"motivo": motivo, "estado_antes": r["estado"], "chofer_eliminado": nombre})
                    canceladas.append(r)
            if reservas:
                # Una llave pudo salir justo ahora: se vuelve a mirar ya con las cancelaciones hechas.
                afuera = [r for r in (mysql_fetchall(sql_reservas, (cid,)) or []) if r["estado"] == "llave_entregada"]
                if afuera:
                    return _error("Mientras tanto se entregó la llave de una de sus reservas, así que no se borró el chofer. "
                                  f"Las reservas que aún no empezaban ya quedaron canceladas (cancelaste {len(canceladas)}). "
                                  "Registra la devolución y vuelve a intentarlo.", 409, error_codigo="LLAVE_AFUERA", puede_forzar=False)
            mysql_execute("UPDATE flota_archivos SET activo=0, quitado_por=%s, quitado_at=NOW() WHERE entidad='chofer' AND entidad_id=%s AND activo=1",
                          (current_username(), cid))
            mysql_execute("DELETE FROM flota_choferes WHERE id=%s", (cid,))
        except Exception as e:
            print(f"[flota] eliminar chofer: {type(e).__name__}: {e}", flush=True)
            return _error("No se pudo borrar el chofer. Revisa la lista: si alguna reserva ya quedó cancelada, el historial lo dice.", 500)
        for r in canceladas:                       # aviso a quien pidió cada reserva (mejor esfuerzo: nunca rompe el borrado)
            try:
                fila = _sol_fila(r["id"])
                if fila and fila.get("solicitante_id") and fila["solicitante_id"] != _uid():
                    _correo_flota("flota_solicitud_cancelada", [fila["solicitante_id"]], f"Solicitud {fila.get('numero') or ''} cancelada",
                                  _cuerpo_correo(dict(fila), f"{quien} eliminó al chofer {nombre} y por eso se canceló esta reserva. El horario quedó libre."),
                                  sid=fila.get("id"))
            except Exception as e:
                print(f"[flota] aviso de reserva cancelada al borrar chofer: {type(e).__name__}", flush=True)
        return jsonify({"ok": True, "eliminado": True, "reservas_total": len(reservas), "reservas_canceladas": len(canceladas)})

    @app.route("/flota/api/choferes/<int:cid>/historial", methods=["GET"])
    @_flota_gestion_required
    def flota_api_chofer_historial(cid):
        if not _chofer_fila(cid):
            return _error("No encontré ese chofer.", 404)
        filas = mysql_fetchall(
            "SELECT id, accion, detalle, usuario, created_at FROM flota_chofer_log WHERE chofer_id=%s ORDER BY id DESC LIMIT 200", (cid,)) or []
        return jsonify({"ok": True, "eventos": _eventos_historial(filas, chofer=True)})

    # ── Solicitudes ─────────────────────────────────────────────────────────
    def _sol_json(r):
        gest, uid = _flota_gestiona(), _uid()
        ini, fin = r["inicio"], r["fin"]
        est = r.get("estado")
        mia = bool(uid and r.get("solicitante_id") == uid)
        # Quien retiró la llave es quien trae el vehículo: una reserva que nace de una OT la pide quien creó la OT, pero la devuelve el técnico
        # (04-nov-2026, revisión escéptica: sin esto el chofer no podía registrar la devolución desde su celular).
        soy_chofer = bool(uid and r.get("chofer_usuario_id") == uid)
        titulo = " ".join(x for x in (r.get("v_tipo"), r.get("v_marca"), r.get("v_modelo")) if x)
        ahora = _ahora_chile()
        fer = _feriados(ini.year, fin.year, ahora.year)
        # 03-nov-2026: la llave solo se entrega el día de la reserva; la gestión responde dentro del SLA; plan contra real.
        dia = fr.llave_dia(ini, fin, ahora.date())
        resp_at = r.get("confirmada_at") or (r.get("cancelada_at") if est == "cancelada" else None)
        sla_resp = (fr.sla_respuesta(_utc_a_chile(r.get("sello_at")), _utc_a_chile(resp_at), ahora, fer)
                    if est in ("solicitada", "cancelada") or resp_at else None)
        # Daniel, 05-nov-2026: «cuando el chofer gestione la entrega de llaves a Aarón, este las pueda confirmar y que los datos son reales, y ahí
        # sí quedaría cerrado»: una solicitud devuelta queda «por confirmar» hasta que la gestión confirma la recepción; ahí queda «Cerrada».
        cierre = None
        if est == "devuelta":
            cierre = "cerrada" if r.get("dev_confirmada_at") else "por_confirmar"
        estado_texto = {"cerrada": "Cerrada", "por_confirmar": "Devuelta · por confirmar"}.get(cierre) or fr.ESTADOS.get(est, est)
        return {
            "id": r["id"], "numero": r.get("numero") or "", "estado": est, "estado_texto": estado_texto, "cierre": cierre,
            "vehiculo": {"id": r["vehiculo_id"], "patente_fmt": fr.patente_con_guion(r.get("v_patente")), "titulo": titulo,
                         "km_actual": r.get("v_km")},
            "devolucion": ({"km": r.get("dev_km"), "combustible": r.get("dev_combustible"),
                            "combustible_texto": fr.combustible_texto(r.get("dev_combustible")), "observaciones": r.get("dev_obs"),
                            "novedad": bool(r.get("dev_novedad")), "por": r.get("devuelta_por"), "fmt": _hora_chile(r.get("devuelta_at")),
                            "confirmada_por": r.get("dev_confirmada_por"), "confirmada_fmt": _hora_chile(r.get("dev_confirmada_at")),
                            "confirmada_nota": r.get("dev_confirmada_nota")}
                           if est == "devuelta" else None),
            "area": r.get("area"), "solicitante": r.get("solicitante_nombre"),
            "ot": ({"id": r["ot_id"], "numero": r.get("ot_numero") or f"OT #{r['ot_id']}"} if r.get("ot_id") else None),
            "chofer": {"id": r["chofer_id"], "nombre": r.get("chofer_nombre") or "(sin nombre)", "telefono": r.get("chofer_phone")},
            "fecha": _dia_iso(ini), "hora_desde": _hhmm(ini), "fecha_hasta": _dia_iso(fin), "hora_hasta": _hhmm(fin),
            "inicio": ini.strftime("%Y-%m-%dT%H:%M"), "fin": fin.strftime("%Y-%m-%dT%H:%M"), "rango_texto": fr.rango_texto(ini, fin),
            "motivo": r.get("motivo"), "sello_fmt": _hora_chile(r.get("sello_at")),
            # SLA de la llave (02-nov-2026): solo corre en el horario de entrega; None si ya se entregó o aún no es la hora.
            "sla": fr.sla_llave(est, ini, fin, ahora, fer),
            "sla_respuesta": sla_resp,
            "llave_dia": dia,
            "plan_real": {"retiro": fr.desvio(ini, _utc_a_chile(r.get("llave_at"))),
                          "devolucion": fr.desvio(fin, _utc_a_chile(r.get("devuelta_at")))},
            "confirmada_por": r.get("confirmada_por"), "confirmada_fmt": _hora_chile(r.get("confirmada_at")),
            "llave_por": r.get("llave_por"), "llave_fmt": _hora_chile(r.get("llave_at")), "llave_nota": r.get("llave_nota"),
            "cancelada_por": r.get("cancelada_por"), "cancelada_fmt": _hora_chile(r.get("cancelada_at")),
            "cancelada_motivo": r.get("cancelada_motivo"),
            "modificada_por": r.get("modificada_por"), "modificada_fmt": _hora_chile(r.get("modificada_at")),
            "puede": {
                "confirmar": gest and est == "solicitada",
                "llave": gest and est == "confirmada" and dia["ok"],
                "editar": gest and est in fr.ESTADOS_EDITABLES,
                "cancelar": (gest and est in fr.ESTADOS_OCUPAN) or (mia and est in fr.ESTADOS_EDITABLES),
                # Devolver la llave (FASE 2): la gestión, que la recibe, o quien pidió el vehículo, que la trae.
                "devolver": est == "llave_entregada" and (gest or mia or soy_chofer),
                # Cerrar: la gestión, que recibe la llave, confirma que la devolución es real (05-nov-2026).
                "confirmar_recepcion": gest and cierre == "por_confirmar",
            },
        }

    # ── Correos de la flota (Daniel, 03-nov-2026: «activa en comunicaciones sus mails»; REGLA #33) ──────────────────
    def _gestores_flota():
        """ids de los usuarios ACTIVOS REGISTRADOS como gestores de la flota: el permiso «Gestionar solicitudes» marcado a la persona (por
        persona manda sobre el rol) o a su rol. Daniel, 04-nov-2026: «la app está enviando mail a todos y debería solo reconocer los que
        están registrados». Ya NO hay respaldo a todos los superadministradores (cada aviso le llegaba a siete personas) y el atajo del
        superadministrador no cuenta: si nadie está registrado no sale ningún correo, y /flota lo dice a la vista (alertar, no trabar)."""
        ids = []
        try:
            filas = mysql_fetchall(
                "SELECT u.id, "
                "(SELECT up.permitido FROM usuario_permisos up WHERE up.user_id=u.id AND up.permiso='flota_gestionar' LIMIT 1) AS ov, "
                "(SELECT rp.permitido FROM rol_permisos rp WHERE rp.rol_slug=u.role AND rp.modulo='flota' AND rp.accion='gestionar' LIMIT 1) AS rolp, "
                "u.role AS rol_slug FROM `" + _AUTH + "` u WHERE u.active=1") or []
            for f in filas:
                if f.get("ov") is not None:
                    registrado = bool(int(f["ov"]))             # lo marcado a ESA persona manda sobre su rol
                elif str(f.get("rol_slug") or "") == "superadmin":
                    registrado = False                          # el rol superadmin lo tiene todo: no es un registro de gestor
                else:
                    registrado = f.get("rolp") is not None and bool(int(f["rolp"]))
                if registrado:
                    ids.append(int(f["id"]))
        except Exception as e:
            print(f"[flota] gestores: {type(e).__name__}", flush=True)
        return ids

    def _correo_flota(clave, uids, titulo, cuerpo, sid=None):
        """Por la puerta única con su clave del registro (comm_correos_registro). Nunca rompe lo que la persona estaba haciendo.
        El botón abre ESA solicitud (03-nov-2026: con la llave entregada, directo en la devolución)."""
        fn = ctx.get("_aviso_interno_correo")
        uids = [u for u in (uids or []) if u]
        if not fn or not uids:
            return 0
        try:
            enlace = f"/flota?sol={int(sid)}#solicitudes" if sid else "/flota#solicitudes"
            return fn(clave, uids, titulo, cuerpo, enlace, "Solicitudes de vehículo")
        except Exception as e:
            print(f"[flota] correo {clave}: {type(e).__name__}", flush=True)
            return 0

    def _cuerpo_correo(r, primera=""):
        """Texto del aviso desde la fila de la solicitud (sin depender de quién está conectado: también lo usa el barrido)."""
        veh = " ".join(x for x in (fr.patente_con_guion(r.get("v_patente")), r.get("v_tipo"), r.get("v_marca"), r.get("v_modelo")) if x)
        lineas = [primera] if primera else []
        lineas += [f"Solicitud: {r.get('numero') or ''}", f"Vehículo: {veh}", f"Cuándo: {fr.rango_texto(r['inicio'], r['fin'])}",
                   f"Área: {r.get('area') or '—'} · pidió {r.get('solicitante_nombre') or '—'}",
                   f"Retira la llave: {r.get('chofer_nombre') or '—'}", f"Motivo: {r.get('motivo') or '—'}"]
        return "\n".join(lineas)

    def _sol_fila(sid):
        return mysql_fetchone(_SQL_SOL + " WHERE s.id=%s", (sid,))

    def _sol_fila_fresca(sid):
        """Relee una reserva RECIÉN guardada por `_reservar`, que escribe por OTRA conexión (get_mysql). `mysql_fetchone` usa la conexión del
        pool de la petición, que puede traer abierto desde antes un snapshot REPEATABLE READ y NO ver la fila nueva: devolvía None aunque
        estaba guardada (caso real del 05-nov: a Víctor le dijo que no se reservó y sí se reservó, y la gestión no recibió el correo).
        Una conexión nueva siempre ve lo comiteado. Si no se puede abrir, cae a la lectura de siempre."""
        conn = None
        try:
            if get_mysql is not None:
                conn = get_mysql()
                cur = conn.cursor()
                cur.execute(_SQL_SOL + " WHERE s.id=%s", (sid,))
                fila = cur.fetchone()
                if fila:
                    return fila
        except Exception as e:
            print(f"[flota] relectura fresca de la reserva: {type(e).__name__}", flush=True)
        finally:
            try:
                if conn is not None:
                    conn.close()
            except Exception:
                pass
        return _sol_fila(sid)

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

    _FERIADOS_CACHE = {}

    def _feriados(*anios):
        """Fechas ISO de los feriados de Chile de esos años (cl_feriados, REGLA #31), guardadas por año."""
        out = set()
        for a in set(anios):
            if a not in _FERIADOS_CACHE:
                try:
                    _FERIADOS_CACHE[a] = set((cl_feriados.feriados_chile(a) or {}).keys()) if cl_feriados is not None else set()
                except Exception:
                    _FERIADOS_CACHE[a] = set()
            out |= _FERIADOS_CACHE[a]
        return out

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

    def _reservar(datos, sid=None, motivo_cambio=None, solicitante=None, ot=None, reabrir=False):
        """Crea una reserva (sid=None) o mueve una existente. TODO en una transacción con el vehículo bloqueado (FOR UPDATE): así dos
        personas que piden a la vez el mismo vehículo no pasan las dos; gana el sello (quien llegó primero) y la otra recibe quién lo tiene.
        `solicitante` = (id, nombre) de quien pide cuando no es la persona conectada (una OT aprobada a distancia); `ot` = (id, número) de la
        orden de trabajo de la que nace la reserva (04-nov-2026). `reabrir=True` (la OT se movió de día u hora): al mover una reserva, vuelve a «Por
        confirmar» con sello nuevo, porque la gestión había confirmado OTRO horario. Devuelve (cuerpo_json, http)."""
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
            if ch.get("usuario_id") is None:
                conn.rollback()
                msg = f"{ch.get('u_nombre') or 'Ese chofer'} no tiene usuario de ILUS: solo retira el vehículo quien está registrado en la plataforma."
                return {"ok": False, "error": msg, "error_codigo": "CHOFER_SIN_USUARIO", "errores": {"chofer_id": msg}}, 400
            chofer_ev = {"nombre": ch.get("u_nombre"), "habilitado": bool(ch.get("habilitado")) and bool(ch.get("u_activo")),
                         "licencia_clases": ch.get("licencia_clases"), "licencia_vence": ch.get("licencia_vence"),
                         "licencia_escaneada": bool((_archivos_activos("chofer").get(datos["chofer_id"]) or {}).get("licencia"))}
            err_lic, aviso_lic = fr.evaluar_chofer(chofer_ev, veh.get("licencia_clases"), fin, ahora.date())
            if err_lic:
                conn.rollback()
                return {"ok": False, "error": err_lic, "errores": {"chofer_id": err_lic}}, 400

            estados_sql = "('solicitada','confirmada','llave_entregada')"
            cur.execute(_SQL_OCUPA + " WHERE s.vehiculo_id=%s AND s.estado IN " + estados_sql + " AND s.inicio < %s AND s.fin > %s",
                        (datos["vehiculo_id"], fin, inicio))
            choque_v = fr.conflictos(inicio, fin, [dict(x) for x in (cur.fetchall() or [])], excluir_id=sid)
            if choque_v and sid is None and not ot:
                # Reintento de la MISMA solicitud (05-nov-2026): si la primera vez se guardó pero la persona no vio la confirmación (se cortó la
                # conexión, falló la respuesta) y vuelve a tocar «Solicitar», lo que choca es su propia reserva idéntica. No es un choque: es la
                # misma solicitud. Se devuelve esa, sin crear otra ni mostrarle un error de algo que sí quedó guardado.
                quien0 = solicitante[0] if solicitante else _uid()
                for c_ig in choque_v:
                    cur.execute("SELECT id FROM flota_solicitudes WHERE id=%s AND solicitante_id=%s AND vehiculo_id=%s AND chofer_id=%s "
                                "AND area=%s AND motivo=%s AND inicio=%s AND fin=%s AND estado IN ('solicitada','confirmada')",
                                (c_ig["id"], quien0, datos["vehiculo_id"], datos["chofer_id"], datos["area"], datos["motivo"], inicio, fin))
                    if cur.fetchone():
                        conn.rollback()
                        return {"ok": True, "id": int(c_ig["id"]), "avisos": [], "repetida": True}, 200
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

            avisos = (fr.avisos_vehiculo(dict(veh), inicio.date()) + ([aviso_lic] if aviso_lic else []) + _avisos_dia_no_laboral(inicio, fin)
                      + fr.avisos_horario(inicio, fin, _feriados(inicio.year, fin.year)))
            if sid is None:
                quien_id, quien_nombre = solicitante if solicitante else (_uid(), current_username())
                if ot:
                    cur.execute(
                        "INSERT INTO flota_solicitudes (vehiculo_id, area, solicitante_id, solicitante_nombre, chofer_id, inicio, fin, motivo, "
                        "ot_id, ot_numero) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                        (datos["vehiculo_id"], datos["area"], quien_id, quien_nombre, datos["chofer_id"], inicio, fin, datos["motivo"],
                         ot[0], ot[1]))
                else:
                    cur.execute(
                        "INSERT INTO flota_solicitudes (vehiculo_id, area, solicitante_id, solicitante_nombre, chofer_id, inicio, fin, motivo) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                        (datos["vehiculo_id"], datos["area"], quien_id, quien_nombre, datos["chofer_id"], inicio, fin, datos["motivo"]))
                sid = int(cur.lastrowid)
                cur.execute("UPDATE flota_solicitudes SET numero=%s WHERE id=%s", (fr.numero_solicitud(sid, ahora.year), sid))
                _slog(sid, "creada", dict({"vehiculo": titulo_veh, "chofer": ch.get("u_nombre"), "rango": fr.rango_texto(inicio, fin),
                                           "area": datos["area"], "motivo": datos["motivo"], "avisos": avisos},
                                          **({"ot": ot[1]} if ot else {})), cur=cur)
            else:
                cur.execute(
                    "UPDATE flota_solicitudes SET vehiculo_id=%s, area=%s, chofer_id=%s, inicio=%s, fin=%s, motivo=%s, "
                    "modificada_por=%s, modificada_at=NOW()" +
                    (", estado='solicitada', confirmada_por=NULL, confirmada_at=NULL, sello_at=NOW(), sla_aviso=NULL" if reabrir else "") +
                    " WHERE id=%s AND estado IN ('solicitada','confirmada')",
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

    def _respuesta_reserva(cuerpo, http, fila=None):
        if not cuerpo.get("ok"):
            return jsonify(cuerpo), http
        extra = {"repetida": True} if cuerpo.get("repetida") else {}
        fila = fila or _sol_fila_fresca(cuerpo["id"])        # `fila`: la que el llamador ya releyó (una sola lectura por reserva)
        if not fila:
            # Ya se guardó (el commit pasó): jamás decirle a la persona que falló. Respuesta mínima con su número; la pantalla recarga la lista.
            numero = fr.numero_solicitud(cuerpo["id"], _ahora_chile().year)
            return jsonify({"ok": True, "solicitud": {"id": cuerpo["id"], "numero": numero, "estado": "solicitada"},
                            "avisos": cuerpo.get("avisos") or [], **extra})
        try:
            solicitud = _sol_json(dict(fila))
        except Exception as e:
            print(f"[flota] armar la respuesta de la reserva {cuerpo['id']}: {type(e).__name__}: {e}", flush=True)
            solicitud = {"id": cuerpo["id"], "numero": fila.get("numero") or fr.numero_solicitud(cuerpo["id"], _ahora_chile().year),
                         "estado": fila.get("estado")}
        return jsonify({"ok": True, "solicitud": solicitud, "avisos": cuerpo.get("avisos") or [], **extra})

    # ── Reserva desde «Nueva OT» (Daniel, 04-nov-2026) ──────────────────────────
    # «Cuando se seleccionen los técnicos internos al crear una OT nueva, que pueda dejar agendada la solicitud del camión y/o el furgón».
    # El asistente consulta la disponibilidad (solo lee) y, al crear la OT, el servidor reserva con las MISMAS reglas y el MISMO sello que
    # el formulario de Solicitudes de vehículo (_reservar): choque de vehículo y de chofer, licencia, chofer enrolado. Una reserva que nace
    # de una OT no manda correo (decisión de Daniel, 03-nov) y queda «Por confirmar» hasta que la gestión de flota la confirme.
    _EST_OCUPAN_SQL = "('solicitada','confirmada','llave_entregada')"

    def _titulo_vehiculo(v):
        return " ".join(x for x in (v.get("tipo"), fr.patente_con_guion(v.get("patente"))) if x)

    def _ot_disponibilidad(fecha, desde, hasta, usuario_ids):
        """Qué vehículos están libres en ese horario y cuáles de los técnicos elegidos pueden manejarlos. Solo lee: no reserva nada."""
        try:
            f, hd, hh = fr.fecha_o_none(fecha), fr.parse_hora(desde), fr.parse_hora(hasta)
        except ValueError:
            f = hd = hh = None
        inicio = datetime.combine(f, hd) if f and hd else None
        fin = datetime.combine(f, hh) if f and hh else None
        horario_ok = bool(inicio and fin and fin > inicio)
        hoy = _ahora_chile().date()
        vehiculos, vehs = [], []
        for v in mysql_fetchall("SELECT * FROM flota_vehiculos WHERE archivado=%s ORDER BY habilitado DESC, tipo, patente LIMIT 500", (0,)) or []:
            libre, motivo, choque = bool(v.get("habilitado")), None, None
            if not libre:
                motivo = "Inhabilitado" + (f": {v['inhabilitado_motivo']}" if v.get("inhabilitado_motivo") else "")
            elif horario_ok:
                ocup = fr.conflictos(inicio, fin, [dict(x) for x in (mysql_fetchall(
                    _SQL_OCUPA + " WHERE s.vehiculo_id=%s AND s.estado IN " + _EST_OCUPAN_SQL + " AND s.inicio < %s AND s.fin > %s",
                    (v["id"], fin, inicio)) or [])])
                if ocup:
                    c0 = ocup[0]
                    libre = False
                    choque = {"numero": c0.get("numero") or "", "rango_texto": fr.rango_texto(c0["inicio"], c0["fin"]), "area": c0.get("area"),
                              "solicitante": c0.get("solicitante_nombre")}
                    motivo = f"Ya está reservado: {choque['numero']} · {choque['area']} · {choque['rango_texto']}"
            vehs.append(v)
            vehiculos.append({"id": v["id"], "tipo": v.get("tipo"), "titulo": _titulo_vehiculo(v), "patente_fmt": fr.patente_con_guion(v.get("patente")),
                              "marca_modelo": " ".join(x for x in (v.get("marca"), v.get("modelo")) if x),
                              "licencia_exigida": fr.normalizar_clases(v.get("licencia_clases")), "libre": libre, "motivo": motivo, "choque": choque})
        choferes, vistos = [], set()
        arch_ch = _archivos_activos("chofer")
        for uid in usuario_ids:
            if uid in vistos:
                continue
            vistos.add(uid)
            fila = (mysql_fetchall(_SQL_CHOF + " WHERE c.usuario_id=%s", (uid,)) or [None])[0]
            if not fila or fila.get("archivado"):
                u = mysql_fetchone("SELECT id, nombre, active FROM `" + _AUTH + "` WHERE id=%s", (uid,)) or {}
                choferes.append({"usuario_id": uid, "nombre": u.get("nombre") or f"Usuario #{uid}", "chofer_id": None, "enrolado": False,
                                 "motivo": "No está enrolado como chofer: sin eso no puede retirar el vehículo ni se puede reservar.",
                                 "por_vehiculo": {}})
                continue
            nombre = fila.get("u_nombre") or f"Usuario #{uid}"
            ocupado = None
            if horario_ok:
                cruces = fr.conflictos(inicio, fin, [dict(x) for x in (mysql_fetchall(
                    _SQL_OCUPA + " WHERE s.chofer_id=%s AND s.estado IN " + _EST_OCUPAN_SQL + " AND s.inicio < %s AND s.fin > %s",
                    (fila["id"], fin, inicio)) or [])])
                if cruces:
                    ocupado = {"numero": cruces[0].get("numero") or "", "rango_texto": fr.rango_texto(cruces[0]["inicio"], cruces[0]["fin"]),
                               "patente_fmt": fr.patente_con_guion(cruces[0].get("v_patente"))}
            ch_ev = {"nombre": nombre, "habilitado": bool(fila.get("habilitado")) and bool(fila.get("u_activo")),
                     "licencia_clases": fila.get("licencia_clases"), "licencia_vence": fila.get("licencia_vence"),
                     "licencia_escaneada": bool((arch_ch.get(fila["id"]) or {}).get("licencia"))}
            por_veh = {}
            for v in vehs:
                err, aviso = fr.evaluar_chofer(ch_ev, v.get("licencia_clases"), f or hoy, hoy)
                if not err and ocupado:
                    err = (f"{nombre} ya tiene otra reserva en ese horario: {ocupado['numero']} · {ocupado['patente_fmt']} · "
                           f"{ocupado['rango_texto']}.")
                por_veh[str(v["id"])] = {"ok": not err, "error": err, "aviso": aviso}
            choferes.append({"usuario_id": uid, "nombre": nombre, "chofer_id": fila["id"], "enrolado": True, "motivo": None, "por_vehiculo": por_veh})
        avisos = []
        if horario_ok and fin <= _ahora_chile():
            # Un horario que ya terminó no se puede reservar (el servidor lo rechaza al crear): no se muestra nada como «libre».
            horario_ok = False
            avisos.append("Ese horario ya pasó: elige un día y una hora que no hayan terminado.")
        if horario_ok:
            avisos += list(_avisos_dia_no_laboral(inicio, fin))
            avisos += list(fr.avisos_horario(inicio, fin, _feriados(inicio.year, fin.year)))      # llaves: lunes a viernes hábiles, 08–13 y 14–17
        return {"ok": True, "horario_ok": horario_ok, "vehiculos": vehiculos, "choferes": choferes, "avisos": avisos,
                "horario_llaves": fr.HORARIO_TEXTO, "antelacion_horas": fr.MIN_ANTELACION_H, "sla_respuesta_min": fr.SLA_RESPUESTA_MIN}

    def _ot_puede_reservar():
        """Reservar desde una OT pasa por la MISMA puerta que Solicitudes de vehículo (`flota_solicitar`, que Daniel asigna por el front) y
        nunca la familia técnico (REGLA #19/#26). Revisión escéptica 04-nov: sin esto cualquiera que creara OT reservaba la flota a ciegas."""
        es_tec = ctx.get("_es_rol_tecnico")
        try:
            tecnico = bool(es_tec()) if es_tec else False
        except Exception:
            tecnico = True               # sin poder saber quién es, se cierra
        try:
            return bool(_flota_puede()) and not tecnico
        except Exception:
            return False

    def _ot_reservar(ot_id, numero_ot, titulo_ot, fecha, desde, hasta, vehiculo_id, chofer_id, solicitante=None):
        """Reserva el vehículo para una OT recién creada. Devuelve {ok, solicitud, avisos} o {ok: False, error}: NUNCA lanza ni deshace la OT."""
        if not _ot_puede_reservar():
            return {"ok": False, "codigo": "SIN_PERMISO_FLOTA",
                    "error": "Tu usuario no tiene el permiso «Solicitar vehículos», así que no se reservó el vehículo."}
        motivo = re.sub(r"\s+", " ", f"Para la {numero_ot}" + (f": {titulo_ot}" if titulo_ot else "")).strip()[:fr.USO_MOTIVO_MAX]
        datos, errores = fr.validar_solicitud(
            {"vehiculo_id": vehiculo_id, "chofer_id": chofer_id, "area": "Servicio Técnico", "motivo": motivo, "fecha": fecha,
             "hora_desde": desde, "hora_hasta": hasta}, _ahora_chile(), antelacion_horas=None)
        if errores:
            return {"ok": False, "error": " ".join(str(x) for x in errores.values()), "errores": errores}
        cuerpo, _http = _reservar(datos, solicitante=solicitante, ot=(int(ot_id), numero_ot))
        if not cuerpo.get("ok"):
            return {"ok": False, "error": cuerpo.get("error") or "No se pudo reservar el vehículo.", "codigo": cuerpo.get("error_codigo")}
        avisos = list(cuerpo.get("avisos") or [])
        if datos["inicio"] < _ahora_chile() + timedelta(hours=fr.MIN_ANTELACION_H):
            avisos.append(f"Faltan menos de {fr.MIN_ANTELACION_H} horas para esta reserva: avísale a la gestión de flota para que la confirme a tiempo.")
        fila = _sol_fila_fresca(cuerpo["id"])
        info = _ot_fila_json(dict(fila)) if fila else {"id": cuerpo["id"]}
        return {"ok": True, "solicitud": info, "avisos": avisos}

    def _ot_fila_json(r):
        """Lo mínimo de una reserva para mostrarla junto a su OT (sin permisos ni acciones: esas viven en Solicitudes de vehículo)."""
        titulo = " ".join(x for x in (r.get("v_tipo"), r.get("v_marca"), r.get("v_modelo")) if x)
        return {"id": r["id"], "numero": r.get("numero") or "", "estado": r.get("estado"), "estado_texto": fr.ESTADOS.get(r.get("estado"), r.get("estado")),
                "vehiculo": " ".join(x for x in (titulo, fr.patente_con_guion(r.get("v_patente"))) if x),
                "rango_texto": fr.rango_texto(r["inicio"], r["fin"]), "chofer": r.get("chofer_nombre") or "(sin nombre)",
                "enlace": f"/flota?sol={int(r['id'])}#solicitudes"}

    def _ot_de_orden(ot_id):
        """Reservas de vehículo ligadas a una OT (para su ficha). Si la columna aún no existe devuelve [] sin romper la ficha."""
        try:
            return [_ot_fila_json(dict(x)) for x in (mysql_fetchall(
                _SQL_SOL + " WHERE s.ot_id=%s ORDER BY s.inicio, s.id LIMIT 20", (int(ot_id),)) or [])]
        except Exception as e:
            print(f"[flota] reservas de la OT {ot_id}: {type(e).__name__}", flush=True)
            return []

    # ── UNA SOLA AGENDA (Daniel, 04-nov-2026: «al cancelar la OT o mover la agenda, mueve la agenda del camión; deben comportarse como una sola
    # agenda») ─ la OT y su reserva se mueven y se cancelan juntas. Sin esto, la reserva de una OT cancelada seguía bloqueando el vehículo y el
    # barrido del SLA mandaba correos de «llave atrasada» por una OT muerta (revisión escéptica). Sin correos: es interno y la reserva de una OT
    # tampoco los manda al crearse.
    def _hhmm_de(v):
        """TIME de MySQL (timedelta), time, datetime o 'HH:MM[:SS]' → time sin segundos; None si no hay hora."""
        if v is None or v == "":
            return None
        if isinstance(v, timedelta):
            t = int(v.total_seconds()) % 86400
            return time(t // 3600, (t % 3600) // 60)
        if isinstance(v, datetime):
            return v.time().replace(second=0, microsecond=0)
        if isinstance(v, time):
            return v.replace(second=0, microsecond=0)
        try:
            return fr.parse_hora(str(v))
        except ValueError:
            return None

    def _ini_fin_ot(p):
        """(fecha, fecha_fin, hora_inicio, hora_fin) de la foto de una OT, o None si no tiene fecha."""
        p = p or {}
        try:
            f = fr.fecha_o_none(p.get("fecha_programada"))
            ff = fr.fecha_o_none(p.get("fecha_fin")) or f
        except ValueError:
            return None
        return None if f is None else (f, ff, _hhmm_de(p.get("hora_inicio")), _hhmm_de(p.get("hora_fin")))

    def _cancelar_por_ot(r, motivo):
        veh = _titulo_vehiculo({"tipo": r.get("v_tipo"), "patente": r.get("v_patente")})
        if mysql_rowcount(
                "UPDATE flota_solicitudes SET estado='cancelada', cancelada_por=%s, cancelada_at=NOW(), cancelada_motivo=%s "
                "WHERE id=%s AND estado IN ('solicitada','confirmada')", (current_username(), motivo[:300], r["id"])):
            _slog(r["id"], "cancelada", {"motivo": motivo, "estado_antes": r["estado"], "origen": "ot"})
            return veh
        return None

    def _ot_sincronizar(ot_id, antes=None, despues=None, cancelar=None):
        """Mueve o cancela las reservas VIVAS de una OT. `cancelar` = motivo («La OT se canceló.») → se cancelan todas; si no, `antes` y `despues`
        son la foto de la OT (fecha_programada, fecha_fin, hora_inicio, hora_fin) y cada reserva se corre lo MISMO que se corrió la OT (la salida
        con el inicio y el regreso con el término, así se respeta lo que la persona ajustó). Una reserva movida vuelve a «Por confirmar»; si ya no
        puede ir en el horario nuevo, se cancela y se dice. Con la llave ya entregada (el vehículo salió) no se toca: se avisa.
        Devuelve {ok, acciones: [{numero, accion, texto}], avisos: [...]}. NUNCA lanza ni frena el guardado de la OT."""
        out = {"ok": True, "acciones": [], "avisos": []}
        try:
            filas = [dict(x) for x in (mysql_fetchall(
                _SQL_SOL + " WHERE s.ot_id=%s AND s.estado IN ('solicitada','confirmada','llave_entregada') ORDER BY s.inicio, s.id LIMIT 20",
                (int(ot_id),)) or [])]
            if not filas:
                return out
            d_ini = d_fin = None
            if not cancelar:
                a, d = _ini_fin_ot(antes), _ini_fin_ot(despues)
                if not a or not d:
                    return out
                if d[0].year >= 2099:                         # la OT quedó «sin fecha» (pendiente por falta de stock): no hay día al que ir
                    cancelar = "La OT quedó sin fecha."
                else:
                    sin_h = a[2] is None or d[2] is None
                    sin_f = a[3] is None or d[3] is None
                    d_ini = (datetime.combine(d[0], time.min) - datetime.combine(a[0], time.min)) if sin_h else \
                        (datetime.combine(d[0], d[2]) - datetime.combine(a[0], a[2]))
                    d_fin = (datetime.combine(d[1], time.min) - datetime.combine(a[1], time.min)) if sin_f else \
                        (datetime.combine(d[1], d[3]) - datetime.combine(a[1], a[3]))
                    if not d_ini and not d_fin:
                        return out                           # cambió otra cosa de la OT, no su día ni su hora
            ahora = _ahora_chile()
            for r in filas:
                num, ot = r.get("numero") or "", r.get("ot_numero") or "la OT"
                veh = _titulo_vehiculo({"tipo": r.get("v_tipo"), "patente": r.get("v_patente")})
                if r["estado"] == "llave_entregada":
                    out["avisos"].append(f"{veh} ya salió con la llave entregada ({num}): su reserva no se {'cancela' if cancelar else 'mueve'} sola. "
                                         f"Cuando vuelva, registra su devolución.")
                    continue
                if cancelar:
                    if _cancelar_por_ot(r, f"{ot}: {cancelar}"):
                        out["acciones"].append({"numero": num, "accion": "cancelada",
                                                "texto": f"Se canceló la reserva {num} ({veh}) porque {cancelar[0].lower() + cancelar[1:]}"})
                    continue
                ini, fin = r["inicio"] + d_ini, r["fin"] + d_fin
                payload = {"vehiculo_id": r["vehiculo_id"], "chofer_id": r["chofer_id"], "area": r.get("area"), "motivo": r.get("motivo"),
                           "fecha": ini.date().isoformat(), "hora_desde": ini.strftime("%H:%M"),
                           "fecha_hasta": fin.date().isoformat(), "hora_hasta": fin.strftime("%H:%M")}
                datos, errores = fr.validar_solicitud(payload, ahora, editando=True)
                error = " ".join(str(v) for v in errores.values()) if errores else None
                if not error:
                    cuerpo, _http = _reservar(datos, sid=r["id"], reabrir=True,
                                              motivo_cambio=f"La {ot} se movió: {fr.rango_texto(r['inicio'], r['fin'])} → {fr.rango_texto(ini, fin)}.")
                    if cuerpo.get("ok"):
                        out["acciones"].append({"numero": num, "accion": "movida",
                                                "texto": f"La reserva {num} ({veh}) se movió con la {ot} a {fr.rango_texto(ini, fin)} y quedó «Por confirmar» de nuevo."})
                        continue
                    error = cuerpo.get("error") or "no está libre"
                if _cancelar_por_ot(r, f"{ot} se movió y la reserva no cupo en el horario nuevo: {error}"):
                    out["acciones"].append({"numero": num, "accion": "cancelada",
                                            "texto": f"La {ot} se movió, pero {veh} no se pudo mover con ella ({str(error).rstrip('.')}): se canceló la reserva {num}. "
                                                     f"Pide el vehículo de nuevo para el horario nuevo."})
        except Exception as e:
            print(f"[flota] sincronizar la reserva con la OT {ot_id}: {type(e).__name__}: {e}", flush=True)
            out["ok"] = False
            out["avisos"].append("No se pudo revisar la reserva del vehículo de esta OT: revísala en Solicitudes de vehículo.")
        return out

    OT_API.update(disponibilidad=_ot_disponibilidad, reservar=_ot_reservar, de_orden=_ot_de_orden, sincronizar=_ot_sincronizar)

    _mant_req = ctx.get("_mant_required") or login_required
    _no_tec = ctx.get("_no_tecnico") or (lambda f: f)

    @app.route("/ot/api/flota/disponibilidad", methods=["GET"])
    @_mant_req
    @_no_tec
    def flota_ot_api_disponibilidad():
        """Lo que el asistente «Nueva OT» muestra en su paso Vehículo. Solo lee."""
        if not _ot_puede_reservar():
            return _error("Reservar vehículos desde una OT necesita el permiso «Solicitar vehículos» y no está disponible para técnicos.", 403,
                          error_codigo="SIN_PERMISO_FLOTA")
        ids = []
        for x in (request.args.get("tecnicos") or "").split(",")[:10]:
            x = x.strip()
            try:
                n = int(x) if x.isascii() and x.isdecimal() else 0       # «²» o «٣» pasan isdigit() pero no son un id
            except ValueError:
                n = 0
            if n > 0 and n not in ids:
                ids.append(n)
        try:
            return jsonify(_ot_disponibilidad(request.args.get("fecha"), request.args.get("desde"), request.args.get("hasta"), ids))
        except Exception as e:
            print(f"[flota] disponibilidad para la OT: {type(e).__name__}: {e}", flush=True)
            return _error("No se pudo revisar la disponibilidad de los vehículos. Intenta de nuevo en un momento.", 500)

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
                # Las devueltas que la gestión aún no confirma se ven aunque su horario ya haya pasado: son lo que falta por cerrar.
                filas = mysql_fetchall(_SQL_SOL + " WHERE s.fin >= %s OR (s.estado='devuelta' AND s.dev_confirmada_at IS NULL) "
                                       "ORDER BY s.inicio, s.id LIMIT 500", (inicio_hoy,)) or []
            cuentas = {r["estado"]: int(r["n"]) for r in (mysql_fetchall(
                "SELECT estado, COUNT(*) AS n FROM flota_solicitudes WHERE fin >= %s GROUP BY estado", (inicio_hoy,)) or [])}
            por_cerrar = int((mysql_fetchone("SELECT COUNT(*) AS n FROM flota_solicitudes WHERE estado='devuelta' AND dev_confirmada_at IS NULL")
                              or {}).get("n") or 0)
            hoy_n = int((mysql_fetchone(
                "SELECT COUNT(*) AS n FROM flota_solicitudes WHERE estado IN ('solicitada','confirmada','llave_entregada') "
                "AND inicio < %s AND fin > %s", (inicio_hoy + timedelta(days=1), inicio_hoy)) or {}).get("n") or 0)
        except Exception as e:
            print(f"[flota] listar solicitudes: {type(e).__name__}: {e}", flush=True)
            return _error("No se pudo leer las solicitudes. Intenta de nuevo en un momento.", 500)
        return jsonify({
            "ok": True, "vista": vista, "solicitudes": [_sol_json(dict(f)) for f in filas],
            "resumen": {"por_confirmar": cuentas.get("solicitada", 0), "confirmadas": cuentas.get("confirmada", 0),
                        "llave_entregada": cuentas.get("llave_entregada", 0), "hoy": hoy_n, "por_cerrar": por_cerrar},
            "horario": {"tramos": [list(t) for t in fr.HORARIO_LLAVES], "texto": fr.HORARIO_TEXTO,
                        "tolerancia_min": fr.SLA_TOLERANCIA_MIN, "critico_min": fr.SLA_CRITICO_MIN},
            "hoy": ahora.date().isoformat(), "ahora_hhmm": ahora.strftime("%H:%M"),
            "areas": list(fr.AREAS_SUGERIDAS), "max_dias": fr.MAX_DIAS_USO,
            # Cuántas personas están registradas para recibir los avisos por correo (solo lo ve quien gestiona): 0 = no sale ninguno.
            "gestores_avisos": len(_gestores_flota()) if (_flota_gestiona() or _flota_admin()) else None,
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
        payload = request.get_json(silent=True) or {}
        # Anticipación mínima (Daniel, 03-nov-2026): quien PIDE reserva con al menos 24 h de aviso; la gestión puede reservar antes.
        puede_antes = _flota_gestiona()
        datos, errores = fr.validar_solicitud(payload, _ahora_chile(), antelacion_horas=None if puede_antes else fr.MIN_ANTELACION_H)
        if errores:
            return _error("Revisa los campos marcados.", 400, errores=errores)
        # Origen (Daniel, 03-nov-2026: «cuando se cree la orden de trabajo no debería mandar correo, pero si lo hacen por el menú sí»):
        # una reserva que nace desde una OT (origen="ot") no avisa por correo; la del menú Solicitudes de vehículo, sí.
        desde_ot = str(payload.get("origen") or "").strip().lower() == "ot"
        cuerpo, http = _reservar(datos)
        if cuerpo.get("ok") and puede_antes and datos["inicio"] < _ahora_chile() + timedelta(hours=fr.MIN_ANTELACION_H):
            cuerpo.setdefault("avisos", []).append(f"Faltan menos de {fr.MIN_ANTELACION_H} horas para esta reserva: quien pide debe hacerlo con ese aviso, pero la gestión puede reservar antes.")
        fila_nueva = None
        if cuerpo.get("ok") and not desde_ot and not cuerpo.get("repetida"):
            fila_nueva = r = _sol_fila_fresca(cuerpo["id"])        # recién guardada por otra conexión: la del pool puede no verla (ver _sol_fila_fresca)
            if r:
                r = dict(r)
                _correo_flota("flota_solicitud_nueva", [u for u in _gestores_flota() if u != _uid()],
                              f"Nueva solicitud {r.get('numero') or ''}: {fr.patente_con_guion(r.get('v_patente'))}",
                              _cuerpo_correo(r, "Pidieron un vehículo de la flota. Revísala y confírmala en Solicitudes de vehículo."), sid=r.get("id"))
        return _respuesta_reserva(cuerpo, http, fila=fila_nueva)

    @app.route("/flota/api/solicitudes/<int:sid>", methods=["GET"])
    @_flota_required
    def flota_api_solicitud_detalle(sid):
        fila = _sol_fila(sid)
        if not fila:
            return _error("No encontré esa solicitud.", 404)
        eventos = []
        titulos = {"creada": "Solicitud creada", "confirmada": "Confirmada por la gestión", "llave_entregada": "Llave entregada",
                   "editada": "Reserva modificada", "cancelada": "Cancelada", "sla_aviso": "Aviso de llave atrasada a la gestión",
                   "aviso_solicitante": "Aviso de llave sin retirar a quien la pidió",
                   "devuelta": "Vehículo devuelto", "recepcion_confirmada": "Recepción confirmada · solicitud cerrada"}
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
            elif f["accion"] == "devuelta":
                lineas = [x for x in (
                    f"Kilometraje: {det.get('km'):,} km".replace(",", ".") if det.get("km") is not None else None,
                    ("Combustible: " + fr.combustible_texto(det.get("combustible"))) if det.get("combustible") else None,
                    "Con novedad o daño" if det.get("novedad") else None,
                    ("Observaciones: " + det["observaciones"]) if det.get("observaciones") else None) if x]
                lineas += list(det.get("avisos") or [])
            elif f["accion"] == "recepcion_confirmada":
                lineas = ["La gestión recibió la llave y confirmó que los datos de la devolución son reales."] + (["Nota: " + det["nota"]] if det.get("nota") else [])
            eventos.append({"accion": f["accion"], "titulo": titulos.get(f["accion"], f["accion"]), "lineas": lineas,
                            "usuario": f.get("usuario") or "", "fecha": _hora_chile(f.get("created_at"))})
        fotos = []
        try:
            for a in mysql_fetchall("SELECT id, tipo, nombre_original, content_type, bytes, subido_por, subido_at FROM flota_archivos "
                                    "WHERE entidad='solicitud' AND entidad_id=%s AND activo=1 ORDER BY id", (sid,)) or []:
                j = _archivo_json(a)
                j["etiqueta"] = dict(fr.FOTOS_DEVOLUCION).get(a["tipo"], a["tipo"])
                fotos.append(j)
        except Exception as e:
            print(f"[flota] fotos de la devolución: {type(e).__name__}", flush=True)
        return jsonify({"ok": True, "solicitud": _sol_json(dict(fila)), "eventos": eventos, "fotos_devolucion": fotos})

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
        resp = _transicion(
            sid, "UPDATE flota_solicitudes SET estado='confirmada', confirmada_por=%s, confirmada_at=NOW() WHERE id=%s AND estado='solicitada'",
            (current_username(), sid), "confirmada", {})
        if not isinstance(resp, tuple):
            r = _sol_fila(sid)
            if r and r.get("solicitante_id") and r["solicitante_id"] != _uid():
                r = dict(r)
                _correo_flota("flota_solicitud_confirmada", [r["solicitante_id"]],
                              f"Tu solicitud {r.get('numero') or ''} quedó confirmada",
                              _cuerpo_correo(r, f"{current_username() or 'La gestión'} confirmó tu reserva. La llave se entrega de 08:00 a 13:00 y de 14:00 a 17:00."), sid=r.get("id"))
        return resp

    @app.route("/flota/api/solicitudes/<int:sid>/llave", methods=["POST"])
    @_flota_gestion_required
    def flota_api_solicitud_llave(sid):
        nota = re.sub(r"\s+", " ", str((request.get_json(silent=True) or {}).get("nota") or "")).strip()
        if len(nota) > 300:
            return _error("La nota puede tener hasta 300 caracteres.", 400, errores={"nota": "Máximo 300 caracteres."})
        # Sin usuario de ILUS no se retira el vehículo (03-nov-2026): una reserva antigua con un chofer externo se cambia de chofer primero.
        fila = _sol_fila(sid)
        # «Hoy es que puedo entregar llaves, no puedo entregar las llaves otro día» (Daniel, 03-nov-2026)
        dia = fr.llave_dia(fila["inicio"], fila["fin"], _ahora_chile().date()) if fila else {"ok": True}
        if fila and fila["estado"] == "confirmada" and not dia["ok"]:
            return _error(dia["texto"], 409, error_codigo="LLAVE_OTRO_DIA", solicitud=_sol_json(dict(fila)))
        ch = mysql_fetchone("SELECT id, usuario_id FROM flota_choferes WHERE id=%s", (fila["chofer_id"],)) if fila else None
        if ch and "usuario_id" in ch and ch["usuario_id"] is None:
            return _error(f"{fila.get('chofer_nombre') or 'El chofer'} no tiene usuario de ILUS: no puede retirar el vehículo. "
                          "Cambia el chofer de la reserva por uno registrado.", 409, error_codigo="CHOFER_SIN_USUARIO")
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
        resp = _transicion(
            sid, "UPDATE flota_solicitudes SET estado='cancelada', cancelada_por=%s, cancelada_at=NOW(), cancelada_motivo=%s "
                 "WHERE id=%s AND estado IN ('solicitada','confirmada','llave_entregada')",
            (current_username(), motivo, sid), "cancelada", {"motivo": motivo, "estado_antes": fila["estado"]})
        if not isinstance(resp, tuple):
            r = dict(_sol_fila(sid) or fila)
            quien = current_username() or "Alguien"
            primera = f"{quien} canceló esta reserva. Motivo: {motivo}. El horario quedó libre."
            if r.get("solicitante_id") and r["solicitante_id"] != _uid():
                destino = [r["solicitante_id"]]
            else:
                destino = [u for u in _gestores_flota() if u != _uid()]
            _correo_flota("flota_solicitud_cancelada", destino, f"Solicitud {r.get('numero') or ''} cancelada", _cuerpo_correo(r, primera), sid=r.get("id"))
        return resp


    # ── Recepción de la llave confirmada por la gestión (05-nov-2026) ────────────────────────────────────────────────────
    # Daniel: «cuando el chofer gestione la entrega de llaves a Aarón, este las pueda confirmar y que los datos son reales, y ahí sí quedaría
    # cerrado, al igual que cuando solicitan la agenda Aarón debe confirmar la agenda». Sin correo propio: el aviso de la devolución ya le pide
    # a la gestión confirmar. Una persona de la gestión puede cerrar la solicitud aunque haya registrado ella la devolución; queda anotado quién.
    @app.route("/flota/api/solicitudes/<int:sid>/recepcion", methods=["POST"])
    @_flota_gestion_required
    def flota_api_solicitud_recepcion(sid):
        fila = _sol_fila(sid)
        if not fila:
            return _error("No encontré esa solicitud.", 404)
        fila = dict(fila)
        if fila["estado"] != "devuelta":
            return _error(f"Solo se confirma la recepción de un vehículo ya devuelto; esta está «{fr.ESTADOS.get(fila['estado'], fila['estado'])}».",
                          409, solicitud=_sol_json(fila))
        if fila.get("dev_confirmada_at"):
            return _error(f"La recepción ya la confirmó {fila.get('dev_confirmada_por') or 'la gestión'}.", 409, solicitud=_sol_json(fila))
        cuerpo = request.get_json(silent=True) or {}
        if cuerpo.get("datos_reales") not in (True, 1, "1", "true"):
            msg = "Marca que recibiste la llave y que los datos de la devolución son reales para cerrar la solicitud."
            return _error(msg, 400, errores={"datos_reales": msg})
        nota = re.sub(r"\s+", " ", str(cuerpo.get("nota") or "")).strip()
        if len(nota) > 500:
            return _error("La nota puede tener hasta 500 caracteres.", 400, errores={"nota": "Máximo 500 caracteres."})
        n = mysql_rowcount(
            "UPDATE flota_solicitudes SET dev_confirmada_por=%s, dev_confirmada_at=NOW(), dev_confirmada_nota=%s "
            "WHERE id=%s AND estado='devuelta' AND dev_confirmada_at IS NULL", (current_username(), nota or None, sid))
        if not n:
            actual = _sol_fila(sid) or fila
            return _error("Otra persona ya confirmó esta recepción. Recarga la pantalla.", 409, solicitud=_sol_json(dict(actual)))
        _slog(sid, "recepcion_confirmada", {"nota": nota or None, "km": fila.get("dev_km"), "combustible": fila.get("dev_combustible"),
                                            "novedad": bool(fila.get("dev_novedad"))})
        return jsonify({"ok": True, "solicitud": _sol_json(dict(_sol_fila(sid) or fila))})

    # ── Barrido del SLA de la llave (cuelga de Cloud Scheduler `simpliroute-poll`, cada 10 min) ─────────────────────────────
    def _flota_sla_barrido():
        """Avisa por correo a la gestión cuando una llave se atrasa (15 min hábiles) y otra vez cuando el atraso es crítico (60). Solo
        dentro del horario de llaves, una vez por nivel y por reserva (`flota_solicitudes.sla_aviso`, UPDATE atómico). Nunca levanta."""
        ahora = _ahora_chile()
        if not fr.en_horario(ahora, _feriados(ahora.year)):
            return {"fuera_de_horario": True}
        try:
            filas = mysql_fetchall(_SQL_SOL + " WHERE s.estado IN ('solicitada','confirmada') AND s.inicio <= %s AND s.fin > %s "
                                   "ORDER BY s.inicio LIMIT 200", (ahora, ahora)) or []
        except Exception as e:
            print(f"[flota] barrido SLA: {type(e).__name__}: {e}", flush=True)
            return {"error": True}
        avisos, gestores = 0, None
        for r in filas:
            r = dict(r)
            sla = fr.sla_llave(r["estado"], r["inicio"], r["fin"], ahora, _feriados(r["inicio"].year, ahora.year))
            if not sla or sla["estado"] not in ("atrasada", "critica"):
                continue
            ya = r.get("sla_aviso") or ""
            if ya == sla["estado"] or ya == "critica":
                continue
            try:
                n = mysql_rowcount("UPDATE flota_solicitudes SET sla_aviso=%s WHERE id=%s AND (sla_aviso IS NULL OR sla_aviso<>%s)",
                                   (sla["estado"], r["id"], sla["estado"]))
            except Exception as e:
                print(f"[flota] barrido SLA (marcar): {type(e).__name__}", flush=True)
                continue
            if not n:
                continue
            _slog(r["id"], "sla_aviso", {"nivel": sla["estado"], "texto": sla["texto"]})
            if gestores is None:
                gestores = _gestores_flota()
            _correo_flota("flota_llave_atrasada", gestores,
                          f"{'Atraso crítico' if sla['estado'] == 'critica' else 'Llave atrasada'}: {r.get('numero') or ''} · "
                          f"{fr.patente_con_guion(r.get('v_patente'))}",
                          _cuerpo_correo(r, sla["texto"] + ". Entrega la llave o actualiza la solicitud en Solicitudes de vehículo."), sid=r.get("id"))
            if sla["estado"] == "critica":
                n_sol = _correo_llave_solicitante(r, sla)
                if n_sol:
                    _slog(r["id"], "aviso_solicitante", {"texto": sla["texto"], "correos": n_sol})
            avisos += 1
        return {"revisadas": len(filas), "avisos": avisos}

    def _correo_llave_solicitante(r, sla):
        """Daniel, 03-nov-2026: «si no he entregado las llaves, me gustaría gestionar el envío de un correo a la persona que solicitó,
        ya que estaría fuera de tiempo». Al atraso CRÍTICO, una vez por reserva (lo asegura `sla_aviso`): a quien la pidió y al chofer
        que la retira, técnicos incluidos (es su propia reserva y no lleva montos: REGLAS #19/#26). El botón abre la solicitud.
        Por la puerta única con su clave del registro (REGLA #33). Nunca levanta → cuántos correos encolados."""
        _send_ilus_email, master = ctx.get("_send_ilus_email"), ctx.get("_ilus_email_master")
        base, asunto = ctx.get("_public_base_url") or (lambda: ""), ctx.get("_brand_subject") or (lambda t: t)
        if not (_send_ilus_email and master):
            return 0
        try:
            ids = {r.get("solicitante_id")}
            ch = mysql_fetchone("SELECT id, usuario_id FROM flota_choferes WHERE id=%s", (r["chofer_id"],))
            if ch and ch.get("usuario_id"):
                ids.add(ch["usuario_id"])
            ids = sorted(int(i) for i in ids if i)
            if not ids:
                return 0
            filas = mysql_fetchall("SELECT id, username FROM `" + _AUTH + "` WHERE active=1 AND id IN (" + ",".join(["%s"] * len(ids)) + ")",
                                   tuple(ids)) or []
            titulo = f"Tu reserva {r.get('numero') or ''} sigue sin retirar la llave"
            primera = (f"La reserva del {fr.patente_con_guion(r.get('v_patente'))} empezó a las {fr.hora_texto(r['inicio'])} y la llave sigue "
                       f"sin retirarse ({sla['texto']}). Pasa a buscarla o, si ya no la necesitas, cancélala para liberar el vehículo.")
            cuerpo = _cuerpo_correo(r, primera)
            html = master({"subject": titulo, "preheader": primera[:140], "status_label": "Solicitudes de vehículo", "title": titulo,
                           "message": "<p>" + __import__("html").escape(cuerpo).replace("\n", "<br>") + "</p>",
                           "primary_cta_url": f"{base()}/flota?sol={int(r['id'])}#solicitudes", "primary_cta_label": "Ver mi reserva",
                           "closing_message": "Correo interno de ILUS Fitness. Se apaga en Comunicaciones → Control de correos."})
        except Exception as e:
            print(f"[flota] correo al solicitante: {type(e).__name__}", flush=True)
            return 0
        n = 0
        for u in filas:
            correo = (u.get("username") or "").strip()
            if not re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", correo):
                continue
            try:
                if _send_ilus_email(correo, asunto(titulo), html, evento="flota_llave_atrasada_solicitante", modulo="comunicacion_interna",
                                    asincrono=True, correo="flota_llave_atrasada_solicitante") is not False:
                    n += 1
            except Exception as e:
                print(f"[flota] correo al solicitante {u.get('id')}: {type(e).__name__}", flush=True)
        return n

    ctx["_flota_sla_barrido"] = _flota_sla_barrido


    # ═══════════════════════════════════════════════════════════════════════
    #  FASE 2 · DEVOLUCIÓN DE LA LLAVE CON EVIDENCIA (03-nov-2026)
    #  Daniel: «el hueveo va a ser cuando declaren la gasolina, que tuviera una foto, y que declaren el odómetro con una foto… para
    #  creerles el kilometraje. Además una foto interna y una externa». Kilometraje + nivel de combustible + 4 fotos obligatorias.
    #  Un km MENOR al último no se acepta (03-nov-2026); un salto grande se AVISA y queda en la bitácora: la foto es la evidencia.
    # ═══════════════════════════════════════════════════════════════════════
    def _subir_imagen_solicitud(sid, tipo, data, nombre_original, bucket):
        """Valida, reduce y sube UNA foto de la devolución al almacenamiento privado. NO toca la base ni lee la petición: así se puede correr en un
        hilo (05-nov-2026, Daniel: «tarda mucho subiendo las fotos»: las cuatro suben a la vez). → (datos de la foto, error)."""
        info, err = fr.validar_archivo("solicitud", tipo, data)
        if err:
            return None, err
        if bucket is None:
            return None, "El almacenamiento de archivos no está disponible en este momento. Intenta de nuevo en unos minutos."
        contenido, ct, ext = data, info["content_type"], info["extension"]
        try:
            resize = ctx.get("_img_resize_bytes")
            if resize:
                contenido, ct = resize(data, max_dim=1600, quality=84)
                ext = ".png" if ct == "image/png" else (".webp" if ct == "image/webp" else ".jpg")
        except Exception as e:
            print(f"[flota] redimensionar foto de devolución: {type(e).__name__}", flush=True)
        nombre = fr.nombre_seguro(nombre_original or fr.ETIQUETAS_ARCHIVO.get(tipo, "foto"), ext)
        clave = f"{_PREFIJO_PRIVADO}solicitud/{sid}/{tipo}_{secrets.token_hex(8)}{ext}"
        try:
            blob = bucket.blob(clave)
            blob.cache_control = "private, max-age=0"
            blob.upload_from_string(contenido, content_type=ct)
        except Exception as e:
            print(f"[flota] subir foto de devolución: {type(e).__name__}: {e}", flush=True)
            return None, "No se pudo guardar la foto. Intenta de nuevo."
        return {"nombre": nombre, "ct": ct, "bytes": len(contenido), "clave": clave, "sha": hashlib.sha256(contenido).hexdigest()}, None

    def _registrar_imagen_solicitud(sid, tipo, f):
        """Anota en la base la foto ya subida (la anterior de ese tipo queda como no vigente). → texto del error o None."""
        try:
            mysql_execute("UPDATE flota_archivos SET activo=0, quitado_por=%s, quitado_at=NOW() "
                          "WHERE entidad=%s AND entidad_id=%s AND tipo=%s AND activo=1", (current_username(), "solicitud", sid, tipo))
            mysql_execute(
                "INSERT INTO flota_archivos (entidad, entidad_id, tipo, nombre_original, content_type, bytes, gcs_key, sha256, subido_por) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                ("solicitud", sid, tipo, f["nombre"], f["ct"], f["bytes"], f["clave"], f["sha"], current_username()))
        except Exception as e:
            print(f"[flota] guardar foto de devolución: {type(e).__name__}: {e}", flush=True)
            return "No se pudo guardar la foto. Intenta de nuevo."
        return None

    @app.route("/flota/api/solicitudes/<int:sid>/devolucion", methods=["POST"])
    @_flota_required
    def flota_api_solicitud_devolucion(sid):
        fila = _sol_fila(sid)
        if not fila:
            return _error("No encontré esa solicitud.", 404)
        fila = dict(fila)
        if fila["estado"] != "llave_entregada":
            return _error(f"Solo se devuelve un vehículo con la llave entregada; esta está «{fr.ESTADOS.get(fila['estado'], fila['estado'])}».",
                          409, solicitud=_sol_json(fila))
        if not _sol_json(fila)["puede"]["devolver"]:
            return _error("La devolución la registra la gestión de la flota o quien pidió el vehículo.", 403, error_codigo="SIN_PERMISO_DEVOLVER")
        datos, errores, avisos = fr.validar_devolucion(request.form, fila.get("v_km"))
        fotos = {}
        for tipo, nombre in fr.FOTOS_DEVOLUCION:
            f = request.files.get(tipo)
            if not f or not getattr(f, "filename", ""):
                errores[tipo] = f"Falta la foto: {nombre.lower()}."
            else:
                fotos[tipo] = f
        if errores:
            return _error("Revisa lo marcado: el kilometraje, el combustible y las cuatro fotos son obligatorios.", 400, errores=errores)
        # Las cuatro fotos se leen acá, se reducen y suben A LA VEZ en hilos, y la base se escribe después, en orden.
        leidas = {tipo: (f.read(), getattr(f, "filename", "")) for tipo, f in fotos.items()}
        bucket = _bucket()
        try:
            from concurrent.futures import ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=len(leidas) or 1) as pool:
                futuros = {tipo: pool.submit(_subir_imagen_solicitud, sid, tipo, datos, nombre, bucket) for tipo, (datos, nombre) in leidas.items()}
                subidas = {tipo: fu.result() for tipo, fu in futuros.items()}
        except Exception as e:
            print(f"[flota] subir las fotos de la devolución: {type(e).__name__}: {e}", flush=True)
            return _error("No se pudieron subir las fotos. Intenta de nuevo.", 502)
        for tipo, _n in fr.FOTOS_DEVOLUCION:
            if tipo in subidas and subidas[tipo][1]:
                return _error(f"{dict(fr.FOTOS_DEVOLUCION)[tipo]}: {subidas[tipo][1]}", 400, errores={tipo: subidas[tipo][1]})
        for tipo, _n in fr.FOTOS_DEVOLUCION:
            if tipo in subidas:
                err = _registrar_imagen_solicitud(sid, tipo, subidas[tipo][0])
                if err:
                    return _error(f"{dict(fr.FOTOS_DEVOLUCION)[tipo]}: {err}", 400, errores={tipo: err})
        n = mysql_rowcount(
            "UPDATE flota_solicitudes SET estado='devuelta', devuelta_por=%s, devuelta_at=NOW(), dev_km=%s, dev_combustible=%s, "
            "dev_obs=%s, dev_novedad=%s WHERE id=%s AND estado='llave_entregada'",
            (current_username(), datos["km"], datos["combustible"], datos["observaciones"], 1 if datos["novedad"] else 0, sid))
        if not n:
            actual = _sol_fila(sid) or fila
            return _error(f"Otra persona ya la movió: ahora está «{fr.ESTADOS.get(actual['estado'], actual['estado'])}». Recarga la pantalla.", 409,
                          solicitud=_sol_json(dict(actual)))
        _slog(sid, "devuelta", {"km": datos["km"], "combustible": datos["combustible"], "novedad": datos["novedad"],
                                "observaciones": datos["observaciones"], "avisos": avisos})
        # El kilometraje del vehículo sube con la devolución (nunca baja: un km menor ya lo rechazó validar_devolucion).
        try:
            if mysql_rowcount("UPDATE flota_vehiculos SET km_actual=%s, updated_by=%s WHERE id=%s AND (km_actual IS NULL OR km_actual < %s)",
                              (datos["km"], current_username(), fila["vehiculo_id"], datos["km"])):
                _log(fila["vehiculo_id"], "editado", {"cambios": {"km_actual": {"antes": fila.get("v_km"), "despues": datos["km"]}},
                                                      "motivo": f"Devolución de la solicitud {fila.get('numero') or sid}"})
        except Exception as e:
            print(f"[flota] km tras devolución: {type(e).__name__}", flush=True)
        r = dict(_sol_fila(sid) or fila)
        primera = (f"{current_username() or 'Alguien'} registró la devolución: {datos['km']:,} km · combustible "
                   f"{fr.combustible_texto(datos['combustible'])}".replace(",", ".") + (" · CON NOVEDAD O DAÑO" if datos["novedad"] else "") + ".")
        if datos["observaciones"]:
            primera += " Observaciones: " + datos["observaciones"]
        if avisos:
            primera += " Ojo: " + " ".join(avisos)
        primera += " Confirma la recepción de la llave y de los datos en Solicitudes de vehículo para cerrar la solicitud."
        _correo_flota("flota_devolucion", [u for u in _gestores_flota() if u != _uid()],
                      f"Vehículo devuelto {r.get('numero') or ''}: {fr.patente_con_guion(r.get('v_patente'))}"
                      + (" · con novedad" if datos["novedad"] else ""), _cuerpo_correo(r, primera), sid=r.get("id"))
        return jsonify({"ok": True, "solicitud": _sol_json(r), "avisos": avisos})

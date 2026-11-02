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
from datetime import date, datetime, time, timedelta, timezone
from functools import wraps

from flask import Response, g, jsonify, redirect, render_template, request, url_for

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
    # cancelar), documentos (ver PDF y datos personales) y admin (agregar, editar, archivar y borrar vehículos y choferes: solo el
    # superadministrador, sin llave). Las tres llaves se asignan por rol o por persona en el front (Roles → Solicitudes de vehículo).
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
    _flota_admin_required = _puerta(_flota_admin, "Solo el superadministrador agrega, edita o borra vehículos y choferes.", "SIN_PERMISO_ADMIN_FLOTA")
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
            "areas": list(fr.AREAS_SUGERIDAS), "max_dias_uso": fr.MAX_DIAS_USO,
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

    _TITULOS_HISTORIAL = {
        "creado": "Registrado", "editado": "Datos modificados", "inhabilitado": "Inhabilitado", "habilitado": "Habilitado",
        "archivado": "Archivado", "desarchivado": "Desarchivado", "eliminado": "Borrado definitivo",
        "archivo_subido": "Archivo guardado", "archivo_quitado": "Archivo quitado",
    }

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
        data = archivo.read(max(fr.ARCHIVO_MAX_BYTES.values()) + 1)
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
        nombre = fr.nombre_seguro(getattr(archivo, "filename", "") or fr.ETIQUETAS_ARCHIVO.get(tipo, "archivo"), ext)
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
        _log_entidad(entidad, ident, "archivo_subido", {"tipo": tipo, "nombre": nombre, "bytes": len(contenido)})
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
        if f["tipo"] != "foto" and not _flota_docs():
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
        "SELECT s.*, v.patente AS v_patente, v.tipo AS v_tipo, v.marca AS v_marca, v.modelo AS v_modelo, "
        "COALESCE(u.nombre, c.nombre_completo) AS chofer_nombre, COALESCE(u.phone, c.telefono) AS chofer_phone "
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
            "habilitado": bool(r.get("habilitado")),
            "inhabilitado_motivo": r.get("inhabilitado_motivo"), "inhabilitado_por": r.get("inhabilitado_por"),
            "inhabilitado_fmt": _hora_chile(r.get("inhabilitado_at")),
            "archivado": bool(r.get("archivado")), "archivado_motivo": r.get("archivado_motivo"),
            "archivado_por": r.get("archivado_por"), "archivado_fmt": _hora_chile(r.get("archivado_at")),
            "creado_por": r.get("created_by"), "creado_fmt": _hora_chile(r.get("created_at")),
            "actualizado_por": r.get("updated_by"), "actualizado_fmt": _hora_chile(r.get("updated_at")),
            # Puede recibir una reserva hoy: habilitado, con usuario activo y la licencia sin vencer.
            "disponible": bool(r.get("habilitado")) and activo and estado not in ("vencido", "sin_fecha"),
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

    @app.route("/flota/api/choferes", methods=["POST"])
    @_flota_admin_required
    def flota_api_chofer_crear():
        payload = request.get_json(silent=True) or {}
        if payload.get("externo") in (True, 1, "1", "true", "si"):
            return _chofer_externo_crear(payload)
        datos, errores = fr.validar_chofer(payload)
        if errores:
            return _error("Revisa los campos marcados.", 400, errores=errores)
        u = mysql_fetchone("SELECT id, nombre, active FROM `" + _AUTH + "` WHERE id=%s", (datos["usuario_id"],))
        if not u or not u.get("active"):
            return _error("Ese usuario no existe o está desactivado.", 400, errores={"usuario_id": "Elige un usuario activo de ILUS."})
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
        _clog(fila["id"], "creado", {"usuario": u.get("nombre"), "licencia_clases": datos["licencia_clases"]})
        return _chofer_respuesta(fila["id"])

    def _chofer_externo_crear(payload):
        """Chofer EXTERNO (sin usuario de ILUS): mismo formulario y mismas reglas de licencia, con teléfono propio obligatorio. Para no
        duplicar personas, su RUT no puede estar ya en el repositorio."""
        datos, errores = fr.validar_chofer(payload, externo=True)
        if errores:
            return _error("Revisa los campos marcados.", 400, errores=errores)
        otro = mysql_fetchone("SELECT id, nombre_completo FROM flota_choferes WHERE rut=%s LIMIT 1", (datos["rut"],))
        if otro:
            msg = f"Ya hay un chofer con ese RUT ({otro.get('nombre_completo') or 'en el repositorio'})."
            return _error(msg, 409, errores={"rut": msg})
        try:
            mysql_execute(
                "INSERT INTO flota_choferes (usuario_id, telefono, rut, nombre_completo, direccion, direccion_lat, direccion_lng, "
                "direccion_place_id, licencia_clases, licencia_vence, notas, created_by, updated_by) "
                "VALUES (NULL,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (datos["telefono"], datos["rut"], datos["nombre_completo"], datos["direccion"], datos["direccion_lat"],
                 datos["direccion_lng"], datos["direccion_place_id"], datos["licencia_clases"], datos["licencia_vence"],
                 datos["notas"], current_username(), current_username()))
        except Exception as e:
            print(f"[flota] crear chofer externo: {type(e).__name__}: {e}", flush=True)
            return _error("No se pudo guardar el chofer. Intenta de nuevo.", 500)
        fila = mysql_fetchone(_SQL_CHOF + " WHERE c.usuario_id IS NULL AND c.rut=%s ORDER BY c.id DESC LIMIT 1", (datos["rut"],))
        if not fila:
            return _error("El chofer se guardó pero no se pudo leer de vuelta. Recarga la pantalla.", 500)
        _clog(fila["id"], "creado", {"externo": True, "nombre": datos["nombre_completo"], "licencia_clases": datos["licencia_clases"]})
        return _chofer_respuesta(fila["id"])

    @app.route("/flota/api/choferes/<int:cid>", methods=["PUT"])
    @_flota_admin_required
    def flota_api_chofer_editar(cid):
        actual = _chofer_fila(cid)
        if not actual:
            return _error("No encontré ese chofer.", 404)
        externo = actual.get("usuario_id") is None
        datos, errores = fr.validar_chofer(request.get_json(silent=True) or {}, con_usuario=False,
                                           direccion_actual=actual.get("direccion"), externo=externo)
        if errores:
            return _error("Revisa los campos marcados.", 400, errores=errores)
        cambios = fr.cambios(dict(actual), datos, campos=fr.CHOFER_COLUMNAS)
        if externo and (actual.get("telefono") or None) != datos.get("telefono"):
            cambios["telefono"] = {"antes": actual.get("telefono"), "despues": datos.get("telefono")}
        if datos["direccion_accion"] == "guardar" and (actual.get("direccion_place_id") or "") != (datos["direccion_place_id"] or ""):
            cambios["direccion"] = {"antes": actual.get("direccion") or None, "despues": (datos["direccion"] or "") + " (verificada con Google)"}
        if not cambios:
            return jsonify({"ok": True, "sin_cambios": True,
                            "chofer": _chofer_json(dict(actual), _hoy(), _archivos_activos("chofer").get(cid))})
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
        _clog(cid, "editado", {"cambios": cambios})
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
        """Borrado DEFINITIVO: solo si nunca tuvo reservas y escribiendo su nombre. Queda anotado ANTES de borrar (REGLA #5)."""
        c = _chofer_fila(cid)
        if not c:
            return _error("No encontré ese chofer.", 404)
        esperado = c.get("nombre_completo") or c.get("u_nombre") or ""
        if not fr.coincide_confirmacion((request.get_json(silent=True) or {}).get("confirmacion"), esperado):
            return _error("Para borrarlo definitivo escribe su nombre completo.", 400, errores={"confirmacion": "No coincide con el nombre."})
        usadas = int((mysql_fetchone("SELECT COUNT(*) AS n FROM flota_solicitudes WHERE chofer_id=%s", (cid,)) or {}).get("n") or 0)
        if usadas:
            return _error(f"Ya tuvo {usadas} reserva{'s' if usadas != 1 else ''}: no se puede borrar definitivo porque se perdería su historial. Archívalo.",
                          409, error_codigo="TIENE_HISTORIAL")
        try:
            _clog(cid, "eliminado", {"datos": {k: c.get(k) for k in ("usuario_id", "u_nombre", "rut", "nombre_completo", "direccion",
                                                                        "licencia_clases", "licencia_vence", "notas")}}, estricto=True)
            mysql_execute("UPDATE flota_archivos SET activo=0, quitado_por=%s, quitado_at=NOW() WHERE entidad='chofer' AND entidad_id=%s AND activo=1",
                          (current_username(), cid))
            mysql_execute("DELETE FROM flota_choferes WHERE id=%s", (cid,))
        except Exception as e:
            print(f"[flota] eliminar chofer: {type(e).__name__}: {e}", flush=True)
            return _error("No se pudo borrar el chofer. No se borró nada.", 500)
        return jsonify({"ok": True, "eliminado": True})

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

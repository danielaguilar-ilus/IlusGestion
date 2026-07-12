"""Modulo Catalogo de Productos ILUS (independiente de Tickets y del
cubicador). Prefijo de tablas `cat_` para NO colisionar con
PRODUCTS_TABLE/PHOTOS_TABLE de app.py (esas son del cubicador, ligadas 1:1
al SKU del ERP para /cubicador). Este catalogo es de referencia general:
ficha por SKU con fotos (hasta 10) + manual PDF, sin relacion con el ERP.

Wiring identico al patron ya usado por tickets_module.py:
    from catalogo_module import register_catalogo_routes
    register_catalogo_routes(app, globals())

La migracion (_ensure_catalogo_tables) corre dentro del register, en
app_context, para funcionar aun con ILUS_SKIP_MIGRATIONS=1 en produccion.
"""
import json
import math
import os
from functools import wraps
from datetime import datetime, date, timezone

from flask import request, jsonify, render_template, redirect, url_for, g, Response

try:
    from zoneinfo import ZoneInfo
    _CAT_CL_TZ = ZoneInfo("America/Santiago")
except Exception:  # pragma: no cover
    _CAT_CL_TZ = None


def register_catalogo_routes(app, ctx):
    # ── Dependencias inyectadas desde app.py (globals) ──
    mysql_fetchone = ctx["mysql_fetchone"]
    mysql_fetchall = ctx["mysql_fetchall"]
    mysql_execute = ctx["mysql_execute"]
    get_mysql = ctx["get_mysql"]
    login_required = ctx["login_required"]
    current_username = ctx.get("current_username") or (lambda: None)
    _uploader_upload = ctx.get("_uploader_upload")
    _uploader_destroy = ctx.get("_uploader_destroy")
    _gcs_bucket = ctx.get("_gcs_bucket")
    # Regla #6 (hora Chile): mismo patron que tickets_module.py — reusa el
    # chile_fmt del proyecto si esta disponible; si no, cae a zoneinfo local.
    chile_fmt = ctx.get("chile_fmt")

    # ── Dependencias para piolas (auditoria) / sync ERP / correo del manual ──
    _audit = ctx.get("_audit")
    _random_sql_query = ctx.get("_random_sql_query")
    validar_email = ctx.get("validar_email")
    _send_ilus_email = ctx.get("_send_ilus_email")
    _brand_subject = ctx.get("_brand_subject")
    _ilus_email_master = ctx.get("_ilus_email_master")
    ILUS_SOPORTE_EMAIL = ctx.get("ILUS_SOPORTE_EMAIL") or "soportetec@sphs.cl"

    MAX_FOTOS_POR_PRODUCTO = 10
    MAX_PIOLAS_POR_PRODUCTO = 10
    MAX_MANUAL_MB = 25  # mismo techo/motivo que MAX_ADJUNTO_MB en tickets_module.py:
                        # Cloud Run limita cada request HTTP a 32MB.
    MAX_MANUALES_POR_PRODUCTO = 5  # 2026-07-12 (Daniel, wizard "Registrar producto"):
                                    # hasta 5 manuales por producto, vía cat_producto_manuales
                                    # (tabla nueva). El manual_pdf_key legado (singular) en
                                    # cat_productos SIGUE funcionando sin cambios — Regla #4.2.

    # Bodega de sincronizacion ERP (Regla #4.1: SOLO LECTURA, via
    # _random_sql_query — mismo patron que _buscar_catalogo_bodega en
    # tickets_module.py, con env var propia para no acoplar ambos modulos).
    CAT_BODEGA_SYNC = os.environ.get("CAT_BODEGA_SYNC", "02").strip()

    # ─────────────────────────────────────────────────────────────────
    #  Migracion idempotente (patron _ensure_tickets_tables). Corre al
    #  registrar el modulo, dentro de app_context.
    # ─────────────────────────────────────────────────────────────────
    def _ensure_catalogo_tables():
        try:
            mysql_execute("""
                CREATE TABLE IF NOT EXISTS cat_productos (
                  id                  INT AUTO_INCREMENT PRIMARY KEY,
                  sku                 VARCHAR(100) NOT NULL,
                  nombre              VARCHAR(300) NOT NULL,
                  familia             VARCHAR(150) NULL,
                  observacion         TEXT NULL,
                  manual_pdf_key      VARCHAR(500) NULL,
                  manual_pdf_nombre   VARCHAR(300) NULL,
                  manual_pdf_size_kb  INT NULL,
                  activo              TINYINT(1) NOT NULL DEFAULT 1,
                  created_by          VARCHAR(190) NULL,
                  updated_by          VARCHAR(190) NULL,
                  created_at          DATETIME DEFAULT CURRENT_TIMESTAMP,
                  updated_at          DATETIME DEFAULT CURRENT_TIMESTAMP
                                         ON UPDATE CURRENT_TIMESTAMP,
                  UNIQUE KEY uq_cat_sku (sku),
                  KEY idx_cat_familia (familia),
                  KEY idx_cat_activo (activo)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            mysql_execute("""
                CREATE TABLE IF NOT EXISTS cat_producto_fotos (
                  id           INT AUTO_INCREMENT PRIMARY KEY,
                  producto_id  INT NOT NULL,
                  gcs_key      VARCHAR(500) NOT NULL,
                  orden        INT NOT NULL DEFAULT 1,
                  created_at   DATETIME DEFAULT CURRENT_TIMESTAMP,
                  UNIQUE KEY uq_cat_foto_orden (producto_id, orden),
                  KEY idx_cat_foto_producto (producto_id),
                  CONSTRAINT fk_catfoto_producto FOREIGN KEY (producto_id)
                     REFERENCES cat_productos(id) ON DELETE CASCADE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            mysql_execute("""
                CREATE TABLE IF NOT EXISTS cat_producto_piolas (
                  id           INT AUTO_INCREMENT PRIMARY KEY,
                  producto_id  INT NOT NULL,
                  medida_cm    DECIMAL(6,1) NOT NULL,
                  observacion  VARCHAR(300) NOT NULL,
                  orden        INT NOT NULL DEFAULT 1,
                  activo       TINYINT(1) NOT NULL DEFAULT 1,
                  created_by   VARCHAR(190) NULL,
                  updated_by   VARCHAR(190) NULL,
                  created_at   DATETIME DEFAULT CURRENT_TIMESTAMP,
                  updated_at   DATETIME DEFAULT CURRENT_TIMESTAMP
                                  ON UPDATE CURRENT_TIMESTAMP,
                  UNIQUE KEY uq_cat_piola_orden (producto_id, orden),
                  KEY idx_cat_piola_producto (producto_id, activo),
                  CONSTRAINT fk_catpiola_producto FOREIGN KEY (producto_id)
                     REFERENCES cat_productos(id) ON DELETE CASCADE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            mysql_execute("""
                CREATE TABLE IF NOT EXISTS cat_producto_manuales (
                  id              INT AUTO_INCREMENT PRIMARY KEY,
                  producto_id     INT NOT NULL,
                  gcs_key         VARCHAR(500) NOT NULL,
                  nombre_archivo  VARCHAR(300) NOT NULL,
                  size_kb         INT NULL,
                  orden           INT NOT NULL DEFAULT 1,
                  uploaded_by     VARCHAR(190) NULL,
                  created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
                  UNIQUE KEY uq_cat_manual_orden (producto_id, orden),
                  KEY idx_cat_manual_producto (producto_id),
                  CONSTRAINT fk_catmanual_producto FOREIGN KEY (producto_id)
                     REFERENCES cat_productos(id) ON DELETE CASCADE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
        except Exception as _e:
            print(f"[ILUS][WARN] _ensure_catalogo_tables: {_e}", flush=True)

        # Regla #5: indice composite para el WHERE de 2+ columnas de
        # cat_api_list (p.activo=%s [AND p.familia=%s]). Guard por
        # information_schema (mismo patron que _ensure_mant_reportes_columns
        # en app.py) -- MySQL no soporta "CREATE INDEX IF NOT EXISTS".
        try:
            _idx = mysql_fetchone(
                "SELECT 1 AS x FROM information_schema.STATISTICS "
                "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='cat_productos' "
                "  AND INDEX_NAME='idx_cat_activo_familia' LIMIT 1")
            if not _idx:
                mysql_execute(
                    "ALTER TABLE cat_productos ADD INDEX idx_cat_activo_familia (activo, familia)")
                print("[ensure_catalogo] índice idx_cat_activo_familia creado", flush=True)
        except Exception as _e_idx:
            print(f"[ILUS][WARN] idx_cat_activo_familia: {_e_idx}", flush=True)

    with app.app_context():
        try:
            _ensure_catalogo_tables()
            print("[ILUS] Tablas cat_* garantizadas (Catalogo de Productos).", flush=True)
        except Exception as _e:
            print(f"[ILUS][WARN] _ensure_catalogo_tables (boot): {_e}", flush=True)

    # ─────────────────────────────────────────────────────────────────
    #  Helpers internos
    # ─────────────────────────────────────────────────────────────────
    def _fmt_dt(value, only_date=False):
        """Formatea un datetime/date de MySQL (UTC naive) a hora Chile como
        string listo para la UI (Regla #6). Mismo patron que
        tickets_module.py._fmt_dt — usa el chile_fmt del proyecto si esta
        disponible; si no, cae a un formateo local con zoneinfo."""
        if value is None:
            return None
        if isinstance(value, str):
            return value
        if isinstance(value, date) and not isinstance(value, datetime):
            return value.strftime("%d/%m/%Y")
        if chile_fmt is not None:
            try:
                return chile_fmt(value, "%d/%m/%Y %H:%M") if only_date is False else chile_fmt(value, "%d/%m/%Y")
            except Exception:
                pass
        try:
            aware = value.replace(tzinfo=timezone.utc)
            if _CAT_CL_TZ is not None:
                aware = aware.astimezone(_CAT_CL_TZ)
            return aware.strftime("%d/%m/%Y" if only_date else "%d/%m/%Y %H:%M")
        except Exception:
            return str(value)

    def _fmt_row(row, dt_keys=("created_at", "updated_at")):
        """Devuelve un dict con los campos de fecha convertidos a hora Chile
        (Regla #6) — antes cat_api_list/cat_api_detalle devolvian el
        datetime crudo (UTC) tal cual salia de MySQL."""
        d = dict(row)
        for k in dt_keys:
            if k in d:
                d[k] = _fmt_dt(d[k])
        return d

    def _is_ajaxish():
        return (
            request.headers.get("X-Requested-With") == "XMLHttpRequest"
            or (request.headers.get("Accept") or "").startswith("application/json")
            or request.is_json
            or request.path.startswith("/catalogo/api/")
        )

    def _catalogo_required(view):
        """Gate de Fase 1: reutiliza el permiso 'mantenciones' (o superadmin),
        mismo atajo que _tickets_required en tickets_module.py, para no tocar
        la matriz de roles todavia."""
        @wraps(view)
        def wrapped(*a, **k):
            perms = g.get("permissions") or {}
            if not (perms.get("mantenciones") or perms.get("superadmin")):
                if _is_ajaxish():
                    return jsonify({
                        "ok": False,
                        "error": "Tu usuario no tiene permiso para el Catálogo.",
                        "error_codigo": "SIN_PERMISO_CATALOGO",
                    }), 403
                return redirect(url_for("index"))
            return view(*a, **k)
        return login_required(wrapped)

    def _catalogo_admin_required(view):
        """2026-07-12 (Daniel): "solamente yo puedo hacer el CRUD [de
        productos/manuales], pero un tecnico/ejecutivo puede cargar piolas".
        Gate mas estricto que _catalogo_required (solo superadmin) para
        crear/editar/eliminar productos, fotos, manuales y sincronizar ERP.
        Las piolas se crean con _catalogo_required (broader) a proposito --
        editar/eliminar una piola SI queda en este gate estricto."""
        @wraps(view)
        def wrapped(*a, **k):
            perms = g.get("permissions") or {}
            if not perms.get("superadmin"):
                if _is_ajaxish():
                    return jsonify({
                        "ok": False,
                        "error": "Solo el superadministrador puede editar el Catálogo.",
                        "error_codigo": "SIN_PERMISO_CATALOGO_ADMIN",
                    }), 403
                return redirect(url_for("index"))
            return view(*a, **k)
        return login_required(wrapped)

    SORT_COLS = {
        "sku": "p.sku",
        "nombre": "p.nombre",
        "familia": "p.familia",
        "created_at": "p.created_at",
        "updated_at": "p.updated_at",
        "total_fotos": "total_fotos",
    }

    # ─────────────────────────────────────────────────────────────────
    #  PAGINA (HTML) — shell; la tabla se llena por JS contra la API.
    # ─────────────────────────────────────────────────────────────────
    @app.route("/catalogo")
    @_catalogo_required
    def cat_list():
        return render_template("catalogo/list.html")

    # ─────────────────────────────────────────────────────────────────
    #  API — listado (paginacion/orden/filtro, mismo contrato que
    #  tk_api_list de tickets_module.py)
    # ─────────────────────────────────────────────────────────────────
    @app.route("/catalogo/api/productos", methods=["GET"])
    @_catalogo_required
    def cat_api_list():
        try:
            page = max(1, int(request.args.get("page", 1)))
        except Exception:
            page = 1
        try:
            limit = min(200, max(5, int(request.args.get("limit", 50))))
        except Exception:
            limit = 50
        sort_key = (request.args.get("sort") or "updated_at").strip()
        sort_col = SORT_COLS.get(sort_key, "p.updated_at")
        direction = "ASC" if (request.args.get("dir") or "").strip().lower() == "asc" else "DESC"

        q = (request.args.get("q") or "").strip()
        familia = (request.args.get("familia") or "").strip()
        activo_arg = (request.args.get("activo") or "1").strip()
        activo = 0 if activo_arg == "0" else 1

        # Auto-fill al buscar (gap-fill transparente, contrato ERP sync):
        # si hay busqueda y no hay resultados locales, se intenta traer
        # desde el ERP (SOLO LECTURA) los SKUs de la bodega de soporte que
        # calcen, y se crean los que falten antes de re-ejecutar el SELECT.
        if q and activo == 1:
            try:
                _n_local = int((mysql_fetchone(
                    "SELECT COUNT(*) AS n FROM cat_productos WHERE activo=1 "
                    "AND (sku LIKE %s OR nombre LIKE %s OR familia LIKE %s)",
                    (f"%{q}%", f"%{q}%", f"%{q}%")) or {}).get("n") or 0)
                if _n_local == 0:
                    _cat_sync_erp_nuevos(q=q, limit=20)
            except Exception as _e_gap:
                print(f"[cat_api_list] gap-fill ERP falló: {_e_gap}", flush=True)

        where = ["p.activo=%s"]
        params = [activo]
        if q:
            where.append("(p.sku LIKE %s OR p.nombre LIKE %s OR p.familia LIKE %s)")
            like = f"%{q}%"
            params += [like, like, like]
        if familia:
            where.append("p.familia=%s")
            params.append(familia)
        where_sql = " AND ".join(where)

        total = int((mysql_fetchone(
            f"SELECT COUNT(*) AS n FROM cat_productos p WHERE {where_sql}",
            tuple(params)) or {}).get("n") or 0)
        pages = max(1, math.ceil(total / limit))
        page = min(page, pages)
        offset = (page - 1) * limit

        rows = mysql_fetchall(
            f"""
            SELECT p.id, p.sku, p.nombre, p.familia, p.activo, p.updated_at,
                   (SELECT COUNT(*) FROM cat_producto_fotos f WHERE f.producto_id=p.id) AS total_fotos,
                   (SELECT f2.gcs_key FROM cat_producto_fotos f2
                      WHERE f2.producto_id=p.id ORDER BY f2.orden LIMIT 1) AS foto_thumb_key,
                   CASE WHEN p.manual_pdf_key IS NOT NULL THEN 1 ELSE 0 END AS tiene_manual,
                   (SELECT COUNT(*) FROM cat_producto_manuales m WHERE m.producto_id=p.id) AS total_manuales,
                   (SELECT COUNT(*) FROM cat_producto_piolas pi
                      WHERE pi.producto_id=p.id AND pi.activo=1) AS total_piolas
            FROM cat_productos p
            WHERE {where_sql}
            ORDER BY {sort_col} {direction}
            LIMIT %s OFFSET %s
            """,
            tuple(params) + (limit, offset))

        rows_out = []
        for r in rows:
            row = _fmt_row(r)
            # foto_thumb_url: misma convención "/f/<key>" que ya usa
            # cat_api_detalle para las fotos (gcs_key -> URL pública).
            _key = row.pop("foto_thumb_key", None)
            row["foto_thumb_url"] = ("/f/" + _key) if _key else None
            # "registrado" (2026-07-12, patron "impreso/no impreso" de Etiquetas
            # aplicado al catalogo): ficha completa = familia + al menos 1 piola +
            # al menos 1 manual (nuevo multi-manual O el legado singular).
            tiene_manual_alguno = bool(row.get("tiene_manual")) or int(row.get("total_manuales") or 0) > 0
            row["registrado"] = bool(
                (row.get("familia") or "").strip()
                and int(row.get("total_piolas") or 0) > 0
                and tiene_manual_alguno
            )
            rows_out.append(row)

        return jsonify({
            "ok": True,
            "rows": rows_out,
            "total": total, "pages": pages, "page": page, "limit": limit,
        })

    # ─────────────────────────────────────────────────────────────────
    #  API — CRUD producto
    # ─────────────────────────────────────────────────────────────────
    @app.route("/catalogo/api/productos", methods=["POST"])
    @_catalogo_admin_required
    def cat_api_create():
        d = request.get_json(silent=True) or {}
        sku = (d.get("sku") or "").strip().upper()
        nombre = (d.get("nombre") or "").strip()
        if not sku:
            return jsonify({"ok": False, "error": "Falta el SKU"}), 400
        if not nombre:
            return jsonify({"ok": False, "error": "Falta el nombre"}), 400
        familia = (d.get("familia") or "").strip()[:150] or None
        observacion = (d.get("observacion") or "").strip() or None
        user = current_username() or "sistema"
        try:
            mysql_execute(
                "INSERT INTO cat_productos (sku, nombre, familia, observacion, created_by, updated_by) "
                "VALUES (%s,%s,%s,%s,%s,%s)",
                (sku[:100], nombre[:300], familia, observacion, user, user))
        except Exception as _e:
            msg = str(_e)
            if "Duplicate entry" in msg or "uq_cat_sku" in msg:
                return jsonify({"ok": False, "error": "Ya existe un producto con ese SKU"}), 409
            print(f"[cat_api_create] error: {_e}", flush=True)
            return jsonify({"ok": False, "error": "No se pudo crear el producto"}), 500

        row = mysql_fetchone("SELECT id FROM cat_productos WHERE sku=%s", (sku,))
        return jsonify({"ok": True, "id": row["id"] if row else None})

    @app.route("/catalogo/api/productos/<int:pid>", methods=["GET"])
    @_catalogo_required
    def cat_api_detalle(pid):
        p = mysql_fetchone("SELECT * FROM cat_productos WHERE id=%s", (pid,))
        if not p:
            return jsonify({"ok": False, "error": "Producto no encontrado"}), 404
        fotos = mysql_fetchall(
            "SELECT id, gcs_key, orden FROM cat_producto_fotos WHERE producto_id=%s ORDER BY orden",
            (pid,))
        piolas = mysql_fetchall(
            "SELECT id, medida_cm, observacion, orden FROM cat_producto_piolas "
            "WHERE producto_id=%s AND activo=1 ORDER BY orden", (pid,))
        manuales = mysql_fetchall(
            "SELECT id, gcs_key, nombre_archivo, size_kb, orden FROM cat_producto_manuales "
            "WHERE producto_id=%s ORDER BY orden", (pid,))
        producto = _fmt_row(p)  # Regla #6: created_at/updated_at a hora Chile
        manual_key = producto.pop("manual_pdf_key", None)
        tiene_manual_alguno = bool(manual_key) or len(manuales) > 0
        producto["registrado"] = bool(
            (producto.get("familia") or "").strip() and piolas and tiene_manual_alguno)
        return jsonify({
            "ok": True,
            "producto": producto,
            "fotos": [{"id": f["id"], "url": "/f/" + f["gcs_key"], "orden": f["orden"]} for f in fotos],
            "piolas": [{"id": pl["id"], "medida_cm": float(pl["medida_cm"]),
                        "observacion": pl["observacion"], "orden": pl["orden"]} for pl in piolas],
            "manual": {
                "tiene": bool(manual_key),
                "nombre": p.get("manual_pdf_nombre"),
                "size_kb": p.get("manual_pdf_size_kb"),
            },
            "manuales": [{"id": m["id"], "nombre": m["nombre_archivo"], "size_kb": m["size_kb"],
                          "orden": m["orden"],
                          "url": "/catalogo/api/productos/%d/manuales/%d/descargar" % (pid, m["id"])}
                         for m in manuales],
        })

    @app.route("/catalogo/api/productos/<int:pid>", methods=["PATCH"])
    @_catalogo_admin_required
    def cat_api_update(pid):
        prev = mysql_fetchone("SELECT id, sku FROM cat_productos WHERE id=%s", (pid,))
        if not prev:
            return jsonify({"ok": False, "error": "Producto no encontrado"}), 404
        d = request.get_json(silent=True) or {}
        allowed = ("sku", "nombre", "familia", "observacion")
        sets, params = [], []
        for key in allowed:
            if key not in d:
                continue
            val = d[key]
            if key == "sku":
                val = (val or "").strip().upper()[:100] or None
            elif isinstance(val, str):
                val = val.strip() or None
                if key == "familia":
                    val = val[:150] if val else None
                elif key == "nombre":
                    val = val[:300] if val else None
            sets.append(f"{key}=%s")
            params.append(val)
        if not sets:
            return jsonify({"ok": False, "error": "Sin cambios validos"}), 400
        sets.append("updated_by=%s")
        params.append(current_username() or "sistema")
        params.append(pid)
        try:
            mysql_execute(f"UPDATE cat_productos SET {', '.join(sets)} WHERE id=%s", tuple(params))
        except Exception as _e:
            msg = str(_e)
            if "Duplicate entry" in msg or "uq_cat_sku" in msg:
                return jsonify({"ok": False, "error": "Ya existe un producto con ese SKU"}), 409
            print(f"[cat_api_update] error pid={pid}: {_e}", flush=True)
            return jsonify({"ok": False, "error": "No se pudo actualizar el producto"}), 500
        return jsonify({"ok": True})

    @app.route("/catalogo/api/productos/<int:pid>", methods=["DELETE"])
    @_catalogo_admin_required
    def cat_api_delete(pid):
        p = mysql_fetchone("SELECT sku, manual_pdf_key FROM cat_productos WHERE id=%s", (pid,))
        if not p:
            return jsonify({"ok": False, "error": "Producto no encontrado"}), 404

        perms = g.get("permissions") or {}
        d = request.get_json(silent=True) or {}
        confirm_text = (d.get("confirm_text") or d.get("confirm") or "").strip()

        # Hard delete solo superadmin + confirm_text == sku exacto (Regla #5,
        # mismo patron triple-proteccion que tk_api_delete).
        if perms.get("superadmin") and confirm_text:
            if confirm_text.upper() != (p.get("sku") or "").upper():
                return jsonify({
                    "ok": False,
                    "error": "Para eliminar definitivamente, escribe exactamente el SKU.",
                    "expected": p.get("sku"),
                }), 400
            fotos = mysql_fetchall(
                "SELECT gcs_key FROM cat_producto_fotos WHERE producto_id=%s", (pid,))
            mysql_execute("DELETE FROM cat_productos WHERE id=%s", (pid,))
            if _uploader_destroy:
                for f in fotos:
                    try:
                        _uploader_destroy(f["gcs_key"])
                    except Exception:
                        pass
                if p.get("manual_pdf_key"):
                    try:
                        _uploader_destroy(p["manual_pdf_key"])
                    except Exception:
                        pass
            return jsonify({"ok": True, "hard_delete": True})

        # Soft delete por defecto (Regla #5).
        mysql_execute(
            "UPDATE cat_productos SET activo=0, updated_by=%s WHERE id=%s",
            (current_username() or "sistema", pid))
        return jsonify({"ok": True, "hard_delete": False})

    # ─────────────────────────────────────────────────────────────────
    #  API — fotos (reusa _uploader_upload/_uploader_destroy, mismo
    #  mecanismo que tk_api_upload_adjunto de tickets_module.py)
    # ─────────────────────────────────────────────────────────────────
    @app.route("/catalogo/api/productos/<int:pid>/fotos", methods=["POST"])
    @_catalogo_admin_required
    def cat_api_upload_foto(pid):
        if not mysql_fetchone("SELECT id FROM cat_productos WHERE id=%s", (pid,)):
            return jsonify({"ok": False, "error": "Producto no encontrado"}), 404
        if not _uploader_upload:
            return jsonify({"ok": False, "error": "Almacenamiento no disponible"}), 503
        f = request.files.get("file") or request.files.get("archivo")
        if not f or not f.filename:
            return jsonify({"ok": False, "error": "No llegó ningún archivo"}), 400

        total = int((mysql_fetchone(
            "SELECT COUNT(*) AS n FROM cat_producto_fotos WHERE producto_id=%s", (pid,)) or {}).get("n") or 0)
        if total >= MAX_FOTOS_POR_PRODUCTO:
            return jsonify({"ok": False, "error": f"Máximo {MAX_FOTOS_POR_PRODUCTO} fotos por producto"}), 400

        try:
            res = _uploader_upload(f, folder="catalogo", resource_type="image")
        except Exception as _e:
            print(f"[cat_upload_foto] error pid={pid}: {_e}", flush=True)
            return jsonify({"ok": False, "error": "No se pudo subir la foto"}), 500
        key = res.get("public_id")
        url = res.get("secure_url") or res.get("url")
        if not key or not url:
            return jsonify({"ok": False, "error": "Subida sin resultado válido"}), 500

        try:
            mysql_execute(
                "INSERT INTO cat_producto_fotos (producto_id, gcs_key, orden) "
                "VALUES (%s,%s, (SELECT COALESCE(MAX(orden),0)+1 FROM cat_producto_fotos WHERE producto_id=%s))",
                (pid, key, pid))
        except Exception as _e:
            print(f"[cat_upload_foto] INSERT fallo, limpiando blob pid={pid}: {_e}", flush=True)
            if _uploader_destroy:
                try:
                    _uploader_destroy(key)
                except Exception:
                    pass
            return jsonify({"ok": False, "error": "No se pudo registrar la foto"}), 500

        row = mysql_fetchone(
            "SELECT id FROM cat_producto_fotos WHERE producto_id=%s AND gcs_key=%s "
            "ORDER BY id DESC LIMIT 1", (pid, key))
        return jsonify({"ok": True, "id": row["id"] if row else None, "url": url})

    @app.route("/catalogo/api/productos/<int:pid>/fotos/<int:foto_id>", methods=["DELETE"])
    @_catalogo_admin_required
    def cat_api_delete_foto(pid, foto_id):
        foto = mysql_fetchone(
            "SELECT gcs_key FROM cat_producto_fotos WHERE id=%s AND producto_id=%s", (foto_id, pid))
        if not foto:
            return jsonify({"ok": False, "error": "Foto no encontrada"}), 404
        mysql_execute(
            "DELETE FROM cat_producto_fotos WHERE id=%s AND producto_id=%s", (foto_id, pid))
        if _uploader_destroy:
            try:
                _uploader_destroy(foto["gcs_key"])
            except Exception:
                pass
        return jsonify({"ok": True})

    # ─────────────────────────────────────────────────────────────────
    #  API — manual PDF
    # ─────────────────────────────────────────────────────────────────
    @app.route("/catalogo/api/productos/<int:pid>/manual", methods=["POST"])
    @_catalogo_admin_required
    def cat_api_upload_manual(pid):
        prev = mysql_fetchone("SELECT manual_pdf_key FROM cat_productos WHERE id=%s", (pid,))
        if not prev:
            return jsonify({"ok": False, "error": "Producto no encontrado"}), 404
        if not _uploader_upload:
            return jsonify({"ok": False, "error": "Almacenamiento no disponible"}), 503
        f = request.files.get("file") or request.files.get("archivo")
        if not f or not f.filename:
            return jsonify({"ok": False, "error": "No llegó ningún archivo"}), 400

        ext = ("." + f.filename.rsplit(".", 1)[-1].lower()) if "." in f.filename else ""
        mime = (f.mimetype or "").lower()
        if ext != ".pdf" or mime != "application/pdf":
            return jsonify({"ok": False, "error": "El manual debe ser un archivo PDF"}), 400

        f.seek(0, 2)
        size_mb = f.tell() / (1024 * 1024)
        f.seek(0)
        if size_mb > MAX_MANUAL_MB:
            return jsonify({"ok": False, "error": f"El manual supera el máximo de {MAX_MANUAL_MB} MB"}), 400

        try:
            res = _uploader_upload(f, folder="catalogo/manuales", resource_type="raw")
        except Exception as _e:
            print(f"[cat_upload_manual] error pid={pid}: {_e}", flush=True)
            return jsonify({"ok": False, "error": "No se pudo subir el manual"}), 500
        key = res.get("public_id")
        if not key:
            return jsonify({"ok": False, "error": "Subida sin resultado válido"}), 500
        size_kb = None
        try:
            if res.get("bytes"):
                size_kb = int(res["bytes"] // 1024)
        except Exception:
            pass

        old_key = prev.get("manual_pdf_key")
        mysql_execute(
            "UPDATE cat_productos SET manual_pdf_key=%s, manual_pdf_nombre=%s, manual_pdf_size_kb=%s "
            "WHERE id=%s",
            (key, f.filename[:300], size_kb, pid))
        if old_key and _uploader_destroy:
            try:
                _uploader_destroy(old_key)
            except Exception:
                pass
        return jsonify({"ok": True, "nombre": f.filename, "size_kb": size_kb})

    @app.route("/catalogo/api/productos/<int:pid>/manual", methods=["DELETE"])
    @_catalogo_admin_required
    def cat_api_delete_manual(pid):
        prev = mysql_fetchone("SELECT manual_pdf_key FROM cat_productos WHERE id=%s", (pid,))
        if not prev:
            return jsonify({"ok": False, "error": "Producto no encontrado"}), 404
        key = prev.get("manual_pdf_key")
        mysql_execute(
            "UPDATE cat_productos SET manual_pdf_key=NULL, manual_pdf_nombre=NULL, "
            "manual_pdf_size_kb=NULL WHERE id=%s", (pid,))
        if key and _uploader_destroy:
            try:
                _uploader_destroy(key)
            except Exception:
                pass
        return jsonify({"ok": True})

    # ─────────────────────────────────────────────────────────────────
    #  API — descarga del manual, GATEADA (login + rol; NO via /f/<key>
    #  publico). Pedido explicito de Daniel: el manual solo lo bajan
    #  usuarios autenticados del sistema. Gate basico Fase 1 -- la
    #  aprobacion remota antes de cada descarga queda pendiente para una
    #  fase posterior, NO construida aqui.
    # ─────────────────────────────────────────────────────────────────
    @app.route("/catalogo/api/productos/<int:pid>/manual/descargar", methods=["GET"])
    @_catalogo_required
    def cat_api_descargar_manual(pid):
        p = mysql_fetchone(
            "SELECT manual_pdf_key, manual_pdf_nombre FROM cat_productos WHERE id=%s", (pid,))
        if not p or not p.get("manual_pdf_key"):
            return jsonify({"ok": False, "error": "Este producto no tiene manual"}), 404
        if not _gcs_bucket:
            return jsonify({"ok": False, "error": "Almacenamiento no disponible"}), 503
        b = _gcs_bucket()
        if not b:
            return jsonify({"ok": False, "error": "Almacenamiento no disponible"}), 503
        try:
            data = b.blob(p["manual_pdf_key"]).download_as_bytes()
        except Exception as _e:
            print(f"[cat_descargar_manual] error pid={pid}: {_e}", flush=True)
            return jsonify({"ok": False, "error": "No se pudo leer el manual"}), 500
        nombre = p.get("manual_pdf_nombre") or "manual.pdf"
        resp = Response(data, mimetype="application/pdf")
        resp.headers["Content-Disposition"] = f'attachment; filename="{nombre}"'
        return resp

    # ─────────────────────────────────────────────────────────────────
    #  PIOLAS — cables/piolas de la maquina, con medida (cm) + observacion
    #  obligatoria (Daniel: "distinguir cual cable es"). Auditoria via
    #  app_audit_log (Regla #5), soft-delete siempre, max 10 activas.
    # ─────────────────────────────────────────────────────────────────
    @app.route("/catalogo/api/productos/<int:pid>/piolas", methods=["GET"])
    @_catalogo_required
    def cat_api_piolas_list(pid):
        if not mysql_fetchone("SELECT id FROM cat_productos WHERE id=%s", (pid,)):
            return jsonify({"ok": False, "error": "Producto no encontrado"}), 404
        rows = mysql_fetchall(
            "SELECT id, medida_cm, observacion, orden FROM cat_producto_piolas "
            "WHERE producto_id=%s AND activo=1 ORDER BY orden", (pid,))
        return jsonify({"ok": True, "piolas": [
            {"id": r["id"], "medida_cm": float(r["medida_cm"]),
             "observacion": r["observacion"], "orden": r["orden"]} for r in rows]})

    @app.route("/catalogo/api/productos/<int:pid>/piolas", methods=["POST"])
    @_catalogo_required
    def cat_api_piolas_crear(pid):
        prod = mysql_fetchone("SELECT id, sku FROM cat_productos WHERE id=%s", (pid,))
        if not prod:
            return jsonify({"ok": False, "error": "Producto no encontrado"}), 404
        d = request.get_json(silent=True) or {}
        try:
            medida_cm = float(d.get("medida_cm"))
        except (TypeError, ValueError):
            return jsonify({"ok": False, "error": "Medida inválida"}), 400
        if medida_cm <= 0:
            return jsonify({"ok": False, "error": "La medida debe ser mayor que 0"}), 400
        observacion = (d.get("observacion") or "").strip()
        if not observacion:
            return jsonify({"ok": False, "error": "La observación es obligatoria (para distinguir cuál piola es)"}), 400
        observacion = observacion[:300]

        total = int((mysql_fetchone(
            "SELECT COUNT(*) AS n FROM cat_producto_piolas WHERE producto_id=%s AND activo=1",
            (pid,)) or {}).get("n") or 0)
        if total >= MAX_PIOLAS_POR_PRODUCTO:
            return jsonify({"ok": False, "error": f"Máximo {MAX_PIOLAS_POR_PRODUCTO} piolas por producto"}), 400

        user = current_username() or "sistema"
        try:
            mysql_execute(
                "INSERT INTO cat_producto_piolas (producto_id, medida_cm, observacion, orden, created_by, updated_by) "
                "VALUES (%s,%s,%s, (SELECT t.m FROM (SELECT COALESCE(MAX(orden),0)+1 AS m FROM cat_producto_piolas WHERE producto_id=%s) t), %s,%s)",
                (pid, medida_cm, observacion, pid, user, user))
        except Exception as _e:
            print(f"[cat_piolas_crear] error pid={pid}: {_e}", flush=True)
            return jsonify({"ok": False, "error": "No se pudo crear la piola"}), 500

        row = mysql_fetchone(
            "SELECT id, orden FROM cat_producto_piolas WHERE producto_id=%s "
            "ORDER BY id DESC LIMIT 1", (pid,))
        nuevo_id = row["id"] if row else None
        if _audit:
            _audit("cat_piola_crear", target_type="cat_producto_piola", target_id=nuevo_id,
                   details={"producto_id": pid, "sku": prod.get("sku"),
                             "orden": row.get("orden") if row else None,
                             "medida_cm_antes": None, "medida_cm_despues": medida_cm,
                             "observacion_antes": None, "observacion_despues": observacion})
        return jsonify({"ok": True, "id": nuevo_id})

    @app.route("/catalogo/api/productos/<int:pid>/piolas/<int:piola_id>", methods=["PATCH"])
    @_catalogo_admin_required
    def cat_api_piolas_editar(pid, piola_id):
        prod = mysql_fetchone("SELECT sku FROM cat_productos WHERE id=%s", (pid,))
        if not prod:
            return jsonify({"ok": False, "error": "Producto no encontrado"}), 404
        prev = mysql_fetchone(
            "SELECT medida_cm, observacion FROM cat_producto_piolas "
            "WHERE id=%s AND producto_id=%s AND activo=1", (piola_id, pid))
        if not prev:
            return jsonify({"ok": False, "error": "Piola no encontrada"}), 404

        d = request.get_json(silent=True) or {}
        sets, params = [], []
        medida_despues = float(prev["medida_cm"])
        obs_despues = prev["observacion"]

        if "medida_cm" in d:
            try:
                medida_cm = float(d.get("medida_cm"))
            except (TypeError, ValueError):
                return jsonify({"ok": False, "error": "Medida inválida"}), 400
            if medida_cm <= 0:
                return jsonify({"ok": False, "error": "La medida debe ser mayor que 0"}), 400
            sets.append("medida_cm=%s")
            params.append(medida_cm)
            medida_despues = medida_cm
        if "observacion" in d:
            observacion = (d.get("observacion") or "").strip()
            if not observacion:
                return jsonify({"ok": False, "error": "La observación es obligatoria"}), 400
            observacion = observacion[:300]
            sets.append("observacion=%s")
            params.append(observacion)
            obs_despues = observacion
        if not sets:
            return jsonify({"ok": False, "error": "Sin cambios válidos"}), 400

        sets.append("updated_by=%s")
        params.append(current_username() or "sistema")
        params += [piola_id, pid]
        try:
            mysql_execute(
                f"UPDATE cat_producto_piolas SET {', '.join(sets)} WHERE id=%s AND producto_id=%s",
                tuple(params))
        except Exception as _e:
            print(f"[cat_piolas_editar] error pid={pid} piola={piola_id}: {_e}", flush=True)
            return jsonify({"ok": False, "error": "No se pudo actualizar la piola"}), 500

        if _audit:
            _audit("cat_piola_editar", target_type="cat_producto_piola", target_id=piola_id,
                   details={"producto_id": pid, "sku": prod.get("sku"),
                             "medida_cm_antes": float(prev["medida_cm"]), "medida_cm_despues": medida_despues,
                             "observacion_antes": prev["observacion"], "observacion_despues": obs_despues})
        return jsonify({"ok": True})

    @app.route("/catalogo/api/productos/<int:pid>/piolas/<int:piola_id>", methods=["DELETE"])
    @_catalogo_admin_required
    def cat_api_piolas_eliminar(pid, piola_id):
        prod = mysql_fetchone("SELECT sku FROM cat_productos WHERE id=%s", (pid,))
        if not prod:
            return jsonify({"ok": False, "error": "Producto no encontrado"}), 404
        prev = mysql_fetchone(
            "SELECT medida_cm, observacion FROM cat_producto_piolas "
            "WHERE id=%s AND producto_id=%s AND activo=1", (piola_id, pid))
        if not prev:
            return jsonify({"ok": False, "error": "Piola no encontrada"}), 404

        # Soft-delete SIEMPRE (Regla #5) — nunca se hard-delete una piola individual.
        mysql_execute(
            "UPDATE cat_producto_piolas SET activo=0, updated_by=%s WHERE id=%s AND producto_id=%s",
            (current_username() or "sistema", piola_id, pid))

        if _audit:
            _audit("cat_piola_eliminar", target_type="cat_producto_piola", target_id=piola_id,
                   details={"producto_id": pid, "sku": prod.get("sku"),
                             "medida_cm_antes": float(prev["medida_cm"]), "medida_cm_despues": None,
                             "observacion_antes": prev["observacion"], "observacion_despues": None})
        return jsonify({"ok": True})

    @app.route("/catalogo/api/productos/<int:pid>/piolas/historial", methods=["GET"])
    @_catalogo_required
    def cat_api_piolas_historial(pid):
        if not mysql_fetchone("SELECT id FROM cat_productos WHERE id=%s", (pid,)):
            return jsonify({"ok": False, "error": "Producto no encontrado"}), 404
        ids_rows = mysql_fetchall(
            "SELECT id FROM cat_producto_piolas WHERE producto_id=%s", (pid,))
        ids = [r["id"] for r in ids_rows]
        if not ids:
            return jsonify({"ok": True, "eventos": []})
        placeholders = ",".join(["%s"] * len(ids))
        rows = mysql_fetchall(
            f"SELECT ts, username, role, action, details FROM app_audit_log "
            f"WHERE target_type='cat_producto_piola' AND target_id IN ({placeholders}) "
            f"ORDER BY ts DESC",
            tuple(str(i) for i in ids))
        eventos = []
        for r in rows:
            det = r.get("details")
            if isinstance(det, str):
                try:
                    det = json.loads(det)
                except Exception:
                    pass
            eventos.append({
                "fecha": _fmt_dt(r.get("ts")),
                "usuario": r.get("username"),
                "rol": r.get("role"),
                "accion": r.get("action"),
                "detalle": det,
            })
        return jsonify({"ok": True, "eventos": eventos})

    # ─────────────────────────────────────────────────────────────────
    #  MANUALES (multi) — 2026-07-12 (Daniel, wizard "Registrar producto"):
    #  hasta 5 manuales por producto, cada uno con su propio archivo/nombre.
    #  Convive con el manual_pdf_key legado (singular) sin tocarlo — Regla
    #  #4.2 (no se elimina nada existente).
    # ─────────────────────────────────────────────────────────────────
    @app.route("/catalogo/api/productos/<int:pid>/manuales", methods=["GET"])
    @_catalogo_required
    def cat_api_manuales_list(pid):
        if not mysql_fetchone("SELECT id FROM cat_productos WHERE id=%s", (pid,)):
            return jsonify({"ok": False, "error": "Producto no encontrado"}), 404
        rows = mysql_fetchall(
            "SELECT id, nombre_archivo, size_kb, orden FROM cat_producto_manuales "
            "WHERE producto_id=%s ORDER BY orden", (pid,))
        return jsonify({"ok": True, "manuales": [
            {"id": r["id"], "nombre": r["nombre_archivo"], "size_kb": r["size_kb"], "orden": r["orden"],
             "url": "/catalogo/api/productos/%d/manuales/%d/descargar" % (pid, r["id"])}
            for r in rows]})

    @app.route("/catalogo/api/productos/<int:pid>/manuales", methods=["POST"])
    @_catalogo_admin_required
    def cat_api_manuales_upload(pid):
        if not mysql_fetchone("SELECT id FROM cat_productos WHERE id=%s", (pid,)):
            return jsonify({"ok": False, "error": "Producto no encontrado"}), 404
        if not _uploader_upload:
            return jsonify({"ok": False, "error": "Almacenamiento no disponible"}), 503
        f = request.files.get("file") or request.files.get("archivo")
        if not f or not f.filename:
            return jsonify({"ok": False, "error": "No llegó ningún archivo"}), 400

        total = int((mysql_fetchone(
            "SELECT COUNT(*) AS n FROM cat_producto_manuales WHERE producto_id=%s", (pid,)) or {}).get("n") or 0)
        if total >= MAX_MANUALES_POR_PRODUCTO:
            return jsonify({"ok": False, "error": f"Máximo {MAX_MANUALES_POR_PRODUCTO} manuales por producto"}), 400

        ext = ("." + f.filename.rsplit(".", 1)[-1].lower()) if "." in f.filename else ""
        mime = (f.mimetype or "").lower()
        if ext != ".pdf" or mime != "application/pdf":
            return jsonify({"ok": False, "error": "El manual debe ser un archivo PDF"}), 400

        f.seek(0, 2)
        size_mb = f.tell() / (1024 * 1024)
        f.seek(0)
        if size_mb > MAX_MANUAL_MB:
            return jsonify({"ok": False, "error": f"El manual supera el máximo de {MAX_MANUAL_MB} MB"}), 400

        try:
            res = _uploader_upload(f, folder="catalogo/manuales", resource_type="raw")
        except Exception as _e:
            print(f"[cat_manuales_upload] error pid={pid}: {_e}", flush=True)
            return jsonify({"ok": False, "error": "No se pudo subir el manual"}), 500
        key = res.get("public_id")
        if not key:
            return jsonify({"ok": False, "error": "Subida sin resultado válido"}), 500
        size_kb = None
        try:
            if res.get("bytes"):
                size_kb = int(res["bytes"] // 1024)
        except Exception:
            pass

        user = current_username() or "sistema"
        try:
            mysql_execute(
                "INSERT INTO cat_producto_manuales (producto_id, gcs_key, nombre_archivo, size_kb, orden, uploaded_by) "
                "VALUES (%s,%s,%s,%s, (SELECT t.m FROM (SELECT COALESCE(MAX(orden),0)+1 AS m "
                "FROM cat_producto_manuales WHERE producto_id=%s) t), %s)",
                (pid, key, f.filename[:300], size_kb, pid, user))
        except Exception as _e:
            print(f"[cat_manuales_upload] INSERT falló, limpiando blob pid={pid}: {_e}", flush=True)
            if _uploader_destroy:
                try:
                    _uploader_destroy(key)
                except Exception:
                    pass
            return jsonify({"ok": False, "error": "No se pudo registrar el manual"}), 500

        row = mysql_fetchone(
            "SELECT id FROM cat_producto_manuales WHERE producto_id=%s AND gcs_key=%s "
            "ORDER BY id DESC LIMIT 1", (pid, key))
        return jsonify({"ok": True, "id": row["id"] if row else None, "nombre": f.filename, "size_kb": size_kb})

    @app.route("/catalogo/api/productos/<int:pid>/manuales/<int:manual_id>", methods=["DELETE"])
    @_catalogo_admin_required
    def cat_api_manuales_delete(pid, manual_id):
        m = mysql_fetchone(
            "SELECT gcs_key FROM cat_producto_manuales WHERE id=%s AND producto_id=%s", (manual_id, pid))
        if not m:
            return jsonify({"ok": False, "error": "Manual no encontrado"}), 404
        mysql_execute(
            "DELETE FROM cat_producto_manuales WHERE id=%s AND producto_id=%s", (manual_id, pid))
        if _uploader_destroy:
            try:
                _uploader_destroy(m["gcs_key"])
            except Exception:
                pass
        return jsonify({"ok": True})

    @app.route("/catalogo/api/productos/<int:pid>/manuales/<int:manual_id>/descargar", methods=["GET"])
    @_catalogo_required
    def cat_api_manuales_descargar(pid, manual_id):
        m = mysql_fetchone(
            "SELECT gcs_key, nombre_archivo FROM cat_producto_manuales WHERE id=%s AND producto_id=%s",
            (manual_id, pid))
        if not m:
            return jsonify({"ok": False, "error": "Manual no encontrado"}), 404
        if not _gcs_bucket:
            return jsonify({"ok": False, "error": "Almacenamiento no disponible"}), 503
        b = _gcs_bucket()
        if not b:
            return jsonify({"ok": False, "error": "Almacenamiento no disponible"}), 503
        try:
            data = b.blob(m["gcs_key"]).download_as_bytes()
        except Exception as _e:
            print(f"[cat_manuales_descargar] error pid={pid} manual={manual_id}: {_e}", flush=True)
            return jsonify({"ok": False, "error": "No se pudo leer el manual"}), 500
        nombre = m.get("nombre_archivo") or "manual.pdf"
        resp = Response(data, mimetype="application/pdf")
        resp.headers["Content-Disposition"] = f'attachment; filename="{nombre}"'
        return resp

    # ─────────────────────────────────────────────────────────────────
    #  SYNC ERP — bajo demanda (sin cron nuevo). Trae SKUs de la bodega de
    #  soporte (Regla #4.1: SOLO LECTURA via _random_sql_query) y crea los
    #  productos del catalogo que aun no existen localmente.
    # ─────────────────────────────────────────────────────────────────
    def _cat_sync_erp_nuevos(q=None, limit=200):
        """Devuelve (creados:int, skus:list[str]) o (0, []) si el ERP no
        esta disponible / no hay novedades. Nunca lanza excepciones."""
        if not _random_sql_query:
            return 0, []
        try:
            limit = int(limit)
        except Exception:
            limit = 200
        # 2026-07-12 (Daniel): "cargar la bodega 02 sin los servicios ZZ" +
        # tope de 500 nunca dejaba sincronizar mas alla de los primeros 500
        # SKU alfabeticos (TOP siempre devolvia el mismo lote, sin paginar).
        # Se sube el tope a 5000 (backfill completo de una bodega en un solo
        # llamado es razonable) y se excluyen los SKU "ZZ*" (son codigos de
        # SERVICIO -- instalacion, envio, etc. -- no productos fisicos que
        # deban tener ficha de piolas/manual en el Catalogo).
        limit = max(1, min(5000, limit))
        try:
            if q:
                q_like = f"%{str(q).upper()[:60]}%"
                sql = f"""
                    SELECT DISTINCT TOP {limit}
                           LTRIM(RTRIM(pr.KOPR)) AS sku, LTRIM(RTRIM(pr.NOKOPR)) AS nombre
                      FROM MAEPR pr
                     WHERE EXISTS (SELECT 1 FROM MAEST st
                                    WHERE LTRIM(RTRIM(st.KOPR))=LTRIM(RTRIM(pr.KOPR))
                                      AND LTRIM(RTRIM(st.KOBO))=%s)
                       AND (UPPER(pr.NOKOPR) LIKE %s OR UPPER(pr.KOPR) LIKE %s)
                       AND UPPER(LTRIM(RTRIM(pr.KOPR))) NOT LIKE %s
                     ORDER BY sku
                """
                params = (CAT_BODEGA_SYNC, q_like, q_like, "ZZ%")
            else:
                sql = f"""
                    SELECT DISTINCT TOP {limit}
                           LTRIM(RTRIM(pr.KOPR)) AS sku, LTRIM(RTRIM(pr.NOKOPR)) AS nombre
                      FROM MAEPR pr
                     WHERE EXISTS (SELECT 1 FROM MAEST st
                                    WHERE LTRIM(RTRIM(st.KOPR))=LTRIM(RTRIM(pr.KOPR))
                                      AND LTRIM(RTRIM(st.KOBO))=%s)
                       AND UPPER(LTRIM(RTRIM(pr.KOPR))) NOT LIKE %s
                     ORDER BY sku
                """
                params = (CAT_BODEGA_SYNC, "ZZ%")
            rows = _random_sql_query(sql, params, max_rows=limit) or []
        except Exception as _e:
            print(f"[_cat_sync_erp_nuevos] error ERP (bodega={CAT_BODEGA_SYNC}): {_e}", flush=True)
            return 0, []

        erp_pairs = [((r.get("sku") or "").strip(), (r.get("nombre") or "").strip())
                     for r in rows if (r.get("sku") or "").strip()]
        if not erp_pairs:
            return 0, []
        skus_erp = [s for s, _ in erp_pairs]

        placeholders = ",".join(["%s"] * len(skus_erp))
        existentes_rows = mysql_fetchall(
            f"SELECT sku FROM cat_productos WHERE sku IN ({placeholders})", tuple(skus_erp))
        existentes = {r["sku"] for r in existentes_rows}

        creados = 0
        creados_skus = []
        for sku, nombre in erp_pairs:
            if sku in existentes or not nombre:
                continue
            try:
                mysql_execute(
                    "INSERT INTO cat_productos (sku, nombre, familia, created_by, updated_by) "
                    "VALUES (%s,%s,NULL,'sistema-erp-sync','sistema-erp-sync')",
                    (sku[:100], nombre[:300]))
                creados += 1
                creados_skus.append(sku)
            except Exception as _e_ins:
                # Duplicado (carrera) u otro error puntual: se ignora esta fila,
                # no se aborta el resto del sync.
                print(f"[_cat_sync_erp_nuevos] no se pudo crear sku={sku}: {_e_ins}", flush=True)
        return creados, creados_skus

    @app.route("/catalogo/api/sync-erp", methods=["POST"])
    @_catalogo_admin_required
    def cat_api_sync_erp():
        d = request.get_json(silent=True) or {}
        try:
            limit = int(d.get("limit") or 200)
        except Exception:
            limit = 200
        creados, skus = _cat_sync_erp_nuevos(q=None, limit=limit)
        return jsonify({"ok": True, "creados": creados, "skus": skus})

    # ─────────────────────────────────────────────────────────────────
    #  MANUAL — enviar por correo (adjunto, sin URL publica nueva).
    # ─────────────────────────────────────────────────────────────────
    @app.route("/catalogo/api/productos/<int:pid>/manual/enviar-correo", methods=["POST"])
    @_catalogo_required
    def cat_api_manual_enviar_correo(pid):
        p = mysql_fetchone(
            "SELECT sku, nombre, manual_pdf_key, manual_pdf_nombre FROM cat_productos WHERE id=%s", (pid,))
        if not p:
            return jsonify({"ok": False, "error": "Producto no encontrado"}), 404
        if not p.get("manual_pdf_key"):
            return jsonify({"ok": False, "error": "Este producto no tiene manual"}), 404

        d = request.get_json(silent=True) or {}
        email = (d.get("email") or "").strip()
        mensaje = (d.get("mensaje") or "").strip()[:1000] or None

        if validar_email:
            ok_email, val_or_err = validar_email(email)
            if not ok_email:
                return jsonify({"ok": False, "error": val_or_err or "Correo inválido"}), 400
            if not val_or_err:
                return jsonify({"ok": False, "error": "Falta el correo de destino"}), 400
            email = val_or_err
        elif not email:
            return jsonify({"ok": False, "error": "Falta el correo de destino"}), 400

        if not _gcs_bucket:
            return jsonify({"ok": False, "error": "No se pudo enviar el correo (almacenamiento no disponible)"}), 502
        b = _gcs_bucket()
        if not b:
            return jsonify({"ok": False, "error": "No se pudo enviar el correo (almacenamiento no disponible)"}), 502
        try:
            pdf_bytes = b.blob(p["manual_pdf_key"]).download_as_bytes()
        except Exception as _e:
            print(f"[cat_manual_enviar_correo] error lectura GCS pid={pid}: {_e}", flush=True)
            return jsonify({"ok": False, "error": "No se pudo leer el manual"}), 500

        nombre_producto = p.get("nombre") or p.get("sku") or "producto"
        subject = _brand_subject(f"Manual — {nombre_producto}") if _brand_subject else f"Manual — {nombre_producto}"

        from markupsafe import escape as _esc
        msg_html = f"<p style=\"margin:0 0 12px\">{_esc(mensaje)}</p>" if mensaje else ""
        body_html = (
            f"<p style=\"margin:0 0 12px;font-size:15px;line-height:24px;color:#454b54\">"
            f"Adjunto encontrarás el manual del producto "
            f"<strong>{_esc(p.get('sku') or '')}</strong> — {_esc(nombre_producto)}.</p>"
            f"{msg_html}"
        )
        if _ilus_email_master:
            html = _ilus_email_master({
                "subject": subject,
                "title": "Manual de producto",
                "subtitle": f"{p.get('sku') or ''} · {nombre_producto}",
                "body_html": body_html,
                "support_email": ILUS_SOPORTE_EMAIL,
            })
        else:
            html = f"<html><body>{body_html}</body></html>"

        manual_nombre = p.get("manual_pdf_nombre") or f"{p.get('sku') or 'manual'}.pdf"
        if not _send_ilus_email:
            return jsonify({"ok": False, "error": "No se pudo enviar el correo (canal de email no disponible)"}), 502
        try:
            enviado = _send_ilus_email(
                email, subject, html,
                evento="catalogo_manual", modulo="catalogo",
                attachments=[{"filename": manual_nombre, "content": pdf_bytes, "content_type": "application/pdf"}],
            )
        except Exception as _e:
            print(f"[cat_manual_enviar_correo] error envío pid={pid}: {_e}", flush=True)
            enviado = False

        if _audit:
            _audit("cat_manual_enviado", target_type="cat_producto", target_id=pid,
                   details={"email": email, "sku": p.get("sku"), "manual_nombre": manual_nombre,
                             "enviado": bool(enviado)})

        if not enviado:
            return jsonify({
                "ok": False,
                "error": "No se pudo enviar el correo (revisa el correo de destino o el estado del canal de email)",
            }), 502
        return jsonify({"ok": True})

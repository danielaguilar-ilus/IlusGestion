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

    MAX_FOTOS_POR_PRODUCTO = 10
    MAX_MANUAL_MB = 25  # mismo techo/motivo que MAX_ADJUNTO_MB en tickets_module.py:
                        # Cloud Run limita cada request HTTP a 32MB.

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
                   CASE WHEN p.manual_pdf_key IS NOT NULL THEN 1 ELSE 0 END AS tiene_manual
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
    @_catalogo_required
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
        producto = _fmt_row(p)  # Regla #6: created_at/updated_at a hora Chile
        manual_key = producto.pop("manual_pdf_key", None)
        return jsonify({
            "ok": True,
            "producto": producto,
            "fotos": [{"id": f["id"], "url": "/f/" + f["gcs_key"], "orden": f["orden"]} for f in fotos],
            "manual": {
                "tiene": bool(manual_key),
                "nombre": p.get("manual_pdf_nombre"),
                "size_kb": p.get("manual_pdf_size_kb"),
            },
        })

    @app.route("/catalogo/api/productos/<int:pid>", methods=["PATCH"])
    @_catalogo_required
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
    @_catalogo_required
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
    @_catalogo_required
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
    @_catalogo_required
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
    @_catalogo_required
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
    @_catalogo_required
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

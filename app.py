import base64
import io
import json
import os
import re
import secrets
import smtplib
import threading
import time
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from functools import wraps

from flask import (Flask, Response, flash, g, jsonify, redirect,
                   render_template, request, session, url_for)
from werkzeug.security import check_password_hash, generate_password_hash
from werkzeug.utils import secure_filename

from config import MAX_BULTOS, MYSQL_CONFIG, ERP_CONFIG, EMAIL_CONFIG, CLOUDINARY_CONFIG

app = Flask(__name__)
app.secret_key = "ilus-etiquetas-2026"

BASE_DIR        = os.path.dirname(os.path.abspath(__file__))
UPLOAD_FOLDER   = os.path.join(BASE_DIR, "static", "uploads")
COLABS_FOLDER   = os.path.join(BASE_DIR, "static", "uploads", "colaboradores")
ERP_TABLE_PRODUCTS = ERP_CONFIG.get("table_products", "MAEPR")
ALLOWED_EXT     = {"png", "jpg", "jpeg", "webp", "gif"}
MAX_PHOTOS      = 2

AUTH_TABLE     = MYSQL_CONFIG.get("users_table",    "app_users")
PRODUCTS_TABLE = MYSQL_CONFIG.get("products_table", "app_products")
BULTOS_TABLE   = MYSQL_CONFIG.get("bultos_table",   "app_bultos")
PHOTOS_TABLE   = MYSQL_CONFIG.get("photos_table",   "app_photos")
ERP_TABLE      = MYSQL_CONFIG["table"]

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

DEFAULT_USERS = (
    ("daniel.aguilar@sphs.cl", "Daniel Aguilar", "superadmin", "19109364Daniel"),
)

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(COLABS_FOLDER, exist_ok=True)

# ─────────────────────────────────────────────
#  Cloudinary — almacenamiento de fotos en la nube
# ─────────────────────────────────────────────
_CLD_READY = False
_cloudinary_uploader = None
try:
    import cloudinary as _cld_module
    import cloudinary.uploader as _cloudinary_uploader
    if CLOUDINARY_CONFIG.get("cloud_name") and CLOUDINARY_CONFIG.get("api_key"):
        _cld_module.config(
            cloud_name = CLOUDINARY_CONFIG["cloud_name"],
            api_key    = CLOUDINARY_CONFIG["api_key"],
            api_secret = CLOUDINARY_CONFIG["api_secret"],
            secure     = True,
        )
        _CLD_READY = True
        print("[ILUS] Cloudinary configurado —", CLOUDINARY_CONFIG["cloud_name"])
    else:
        print("[ILUS] Cloudinary sin credenciales — fotos locales.")
except Exception as _cld_err:
    print(f"[ILUS] Cloudinary no disponible: {_cld_err} — fotos locales.")


def _cloud_upload(file_obj, public_id: str, folder: str = "ilus") -> str:
    """Sube a Cloudinary y devuelve la URL segura. Lanza excepción si falla."""
    result = _cloudinary_uploader.upload(
        file_obj,
        public_id     = public_id,
        folder        = folder,
        overwrite     = True,
        resource_type = "image",
    )
    return result["secure_url"]


def _cloud_delete(url_or_filename: str) -> None:
    """Elimina de Cloudinary si es URL; del disco si es nombre local."""
    if url_or_filename.startswith("http"):
        try:
            match = re.search(r"/upload/(?:v\d+/)?(.+)\.[^.]+$", url_or_filename)
            if match and _cloudinary_uploader:
                _cloudinary_uploader.destroy(match.group(1))
        except Exception as exc:
            print(f"[ILUS] Cloudinary delete error: {exc}")
    else:
        path = os.path.join(UPLOAD_FOLDER, url_or_filename)
        if os.path.exists(path):
            os.remove(path)


# ─────────────────────────────────────────────
#  DB helpers
# ─────────────────────────────────────────────

def get_erp_conn():
    """
    Conexión de SOLO LECTURA al ERP externo (cloud.random.cl).
    Devuelve None si no se puede conectar (no interrumpe la app).
    NUNCA usar para escribir.
    """
    import pymysql, pymysql.cursors
    connect_timeout = ERP_CONFIG.get("connect_timeout", 10)
    read_timeout    = ERP_CONFIG.get("read_timeout", 15)
    try:
        return pymysql.connect(
            host=ERP_CONFIG["host"],
            port=ERP_CONFIG["port"],
            user=ERP_CONFIG["user"],
            password=ERP_CONFIG["password"],
            database=ERP_CONFIG["database"],
            connect_timeout=connect_timeout,
            read_timeout=read_timeout,
            write_timeout=read_timeout,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True,
        )
    except Exception:
        try:
            return pymysql.connect(
                host=ERP_CONFIG["host"],
                port=ERP_CONFIG["port"],
                user=ERP_CONFIG["user"],
                password=ERP_CONFIG["password"],
                database=ERP_CONFIG["database"],
                connect_timeout=connect_timeout,
                read_timeout=read_timeout,
                write_timeout=read_timeout,
                cursorclass=pymysql.cursors.DictCursor,
                autocommit=True,
            )
        except Exception:
            return None


def get_mysql():
    """Abre una conexión directa (usada solo por init_db y utilidades internas)."""
    import pymysql, pymysql.cursors
    return pymysql.connect(
        host=MYSQL_CONFIG["host"],
        port=MYSQL_CONFIG["port"],
        user=MYSQL_CONFIG["user"],
        password=MYSQL_CONFIG["password"],
        database=MYSQL_CONFIG["database"],
        connect_timeout=MYSQL_CONFIG.get("connect_timeout", 15),
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
    )


# ── Pool de conexiones ──────────────────────────────────────────
# Mantiene conexiones TCP abiertas y las reutiliza entre requests.
# Elimina el costo de ~250-400 ms por apertura en cada clic.
_db_pool = None
_db_pool_lock = threading.Lock()

def _get_pool():
    global _db_pool
    if _db_pool is not None:
        return _db_pool
    with _db_pool_lock:
        if _db_pool is not None:          # doble chequeo post-lock
            return _db_pool
        try:
            import pymysql, pymysql.cursors
            from dbutils.pooled_db import PooledDB
            _db_pool = PooledDB(
                creator     = pymysql,
                mincached   = 2,           # conexiones siempre listas
                maxcached   = 6,           # máximo en pool inactivo
                maxconnections = 10,       # total permitido
                blocking    = True,        # espera si están todas ocupadas
                host        = MYSQL_CONFIG["host"],
                port        = MYSQL_CONFIG["port"],
                user        = MYSQL_CONFIG["user"],
                password    = MYSQL_CONFIG["password"],
                database    = MYSQL_CONFIG["database"],
                connect_timeout = MYSQL_CONFIG.get("connect_timeout", 15),
                charset     = "utf8mb4",
                cursorclass = pymysql.cursors.DictCursor,
                autocommit  = False,
            )
            print("[ILUS] Pool de conexiones MySQL activo (DBUtils).")
        except ImportError:
            # Si DBUtils no está instalado, degrada a conexión directa
            print("[ILUS][WARN] DBUtils no disponible — sin pool de conexiones.")
            _db_pool = None
    return _db_pool


def get_db():
    """Devuelve una conexión del pool (o una directa si el pool no está disponible).
    Se reutiliza durante todo el request — una sola apertura TCP por página."""
    if "_db" not in g:
        pool = _get_pool()
        g._db = pool.connection() if pool else get_mysql()
    return g._db


def mysql_fetchone(query, params=None):
    with get_db().cursor() as cur:
        cur.execute(query, params or ())
        return cur.fetchone()


def mysql_fetchall(query, params=None):
    with get_db().cursor() as cur:
        cur.execute(query, params or ())
        return cur.fetchall()


# ─────────────────────────────────────────────
#  Schema init
# ─────────────────────────────────────────────

def ensure_erp_table_index():
    """Añade índice en etiquetas.SKU si no existe — mejora velocidad de búsqueda."""
    try:
        conn = get_mysql()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) AS cnt FROM information_schema.statistics
                WHERE table_schema = DATABASE()
                  AND table_name   = %s
                  AND index_name   = 'idx_sku'
            """, (ERP_TABLE,))
            if cur.fetchone()["cnt"] == 0:
                cur.execute(f"CREATE INDEX idx_sku ON `{ERP_TABLE}` (SKU(90))")
                conn.commit()
        conn.close()
    except Exception as e:
        print(f"[INFO] No se pudo crear índice en {ERP_TABLE}: {e}")


def sync_erp_table(product, bultos, conn):
    """
    Sincroniza los datos de app_products + app_bultos en la tabla compartida `etiquetas`.
    Usa INSERT ... ON DUPLICATE KEY UPDATE para manejar crear y editar.
    `Codigo` es la PK (INT) de esa tabla.
    """
    try:
        codigo_int = int(product.get("codigo") or 0)
        if not codigo_int:
            return

        pv_calc = lambda l, a, al: round(float(l or 0) * float(a or 0) * float(al or 0) / 4000.0, 4)

        sorted_b = sorted(bultos, key=lambda b: int(b.get("bulto_num", 0)))
        peso_total = round(sum(float(b.get("peso", 0)) for b in sorted_b), 2)
        pv_total   = round(sum(pv_calc(b.get("largo",0), b.get("ancho",0), b.get("alto",0)) for b in sorted_b), 4)

        data = {
            "Codigo":                   codigo_int,
            "SKU":                      product.get("sku", ""),
            "Nombre":                   product.get("nombre", ""),
            "Estado":                   product.get("estado", "Confirmado"),
            "Bultos":                   len(sorted_b),
            "Peso Total":               peso_total,
            "Peso Volumetrico total":   pv_total,
        }

        # Por bulto (posiciones 1–27)
        for i in range(1, 28):
            suf_dim = "" if i == 1 else str(i)            # "Largo ( cm )", "Largo ( cm )2"...
            suf_pv  = "" if i == 1 else f" {i}"           # "Peso Volumetrico", "Peso Volumetrico 2"...
            b = sorted_b[i - 1] if i <= len(sorted_b) else None
            if b:
                l = float(b.get("largo", 0))
                a = float(b.get("ancho", 0))
                al = float(b.get("alto", 0))
                p  = float(b.get("peso", 0))
                data[f"Largo ( cm ){suf_dim}"] = l
                data[f"Ancho ( cm ){suf_dim}"] = a
                data[f"Alto ( cm ){suf_dim}"]  = al
                data[f"Peso (kg){suf_dim}"]    = p
                data[f"Peso Volumetrico{suf_pv}"] = pv_calc(l, a, al)
            else:
                data[f"Largo ( cm ){suf_dim}"] = 0
                data[f"Ancho ( cm ){suf_dim}"] = 0
                data[f"Alto ( cm ){suf_dim}"]  = 0
                data[f"Peso (kg){suf_dim}"]    = 0
                data[f"Peso Volumetrico{suf_pv}"] = 0

        cols   = list(data.keys())
        c_sql  = ", ".join(f"`{c}`" for c in cols)
        p_sql  = ", ".join(["%s"] * len(cols))
        upd    = ", ".join(f"`{c}`=%s" for c in cols if c != "Codigo")
        sql    = f"INSERT INTO `{ERP_TABLE}` ({c_sql}) VALUES ({p_sql}) ON DUPLICATE KEY UPDATE {upd}"

        ins_vals = [data[c] for c in cols]
        upd_vals = [data[c] for c in cols if c != "Codigo"]

        with conn.cursor() as cur:
            cur.execute(sql, ins_vals + upd_vals)
    except Exception as exc:
        print(f"[WARN] sync_erp_table: {exc}")


def delete_from_erp_table(codigo, conn):
    """Elimina el registro de la tabla etiquetas por Codigo (PK INT)."""
    try:
        codigo_int = int(codigo or 0)
        if not codigo_int:
            return
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM `{ERP_TABLE}` WHERE `Codigo`=%s", (codigo_int,))
    except Exception as exc:
        print(f"[WARN] delete_from_erp_table: {exc}")


def init_mysql_schema():
    conn = get_mysql()
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS `{AUTH_TABLE}` (
                    id           INT AUTO_INCREMENT PRIMARY KEY,
                    username     VARCHAR(190) NOT NULL UNIQUE,
                    nombre       VARCHAR(190) NOT NULL,
                    password_hash VARCHAR(255) NOT NULL,
                    role         VARCHAR(20)  NOT NULL DEFAULT 'editor',
                    active       TINYINT(1)   NOT NULL DEFAULT 1,
                    created_at   TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at   TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS `{PRODUCTS_TABLE}` (
                    id         INT AUTO_INCREMENT PRIMARY KEY,
                    sku        VARCHAR(120) NOT NULL UNIQUE,
                    nombre     VARCHAR(255) NOT NULL,
                    estado     VARCHAR(40)  NOT NULL DEFAULT 'Pendiente',
                    codigo     VARCHAR(120) NOT NULL DEFAULT '',
                    erp_sync   TINYINT(1)   NOT NULL DEFAULT 0,
                    created_by VARCHAR(190) DEFAULT NULL,
                    updated_by VARCHAR(190) DEFAULT NULL,
                    created_at TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS `{BULTOS_TABLE}` (
                    id         INT AUTO_INCREMENT PRIMARY KEY,
                    product_id INT  NOT NULL,
                    bulto_num  INT  NOT NULL,
                    largo      DECIMAL(12,2) NOT NULL DEFAULT 0,
                    ancho      DECIMAL(12,2) NOT NULL DEFAULT 0,
                    alto       DECIMAL(12,2) NOT NULL DEFAULT 0,
                    peso       DECIMAL(12,2) NOT NULL DEFAULT 0,
                    UNIQUE KEY uniq_product_bulto (product_id, bulto_num),
                    CONSTRAINT fk_bulto_product
                        FOREIGN KEY (product_id) REFERENCES `{PRODUCTS_TABLE}`(id)
                        ON DELETE CASCADE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS `{PHOTOS_TABLE}` (
                    id         INT AUTO_INCREMENT PRIMARY KEY,
                    product_id INT          NOT NULL,
                    filename   VARCHAR(255) NOT NULL,
                    orden      INT          NOT NULL DEFAULT 1,
                    created_at TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT fk_photo_product
                        FOREIGN KEY (product_id) REFERENCES `{PRODUCTS_TABLE}`(id)
                        ON DELETE CASCADE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)

            for username, nombre, role, password in DEFAULT_USERS:
                cur.execute(f"SELECT id FROM `{AUTH_TABLE}` WHERE username=%s", (username,))
                existing = cur.fetchone()
                if existing:
                    cur.execute(
                        f"UPDATE `{AUTH_TABLE}` SET nombre=%s, role=%s, active=1 WHERE id=%s",
                        (nombre, role, existing["id"]),
                    )
                else:
                    cur.execute(
                        f"INSERT INTO `{AUTH_TABLE}` (username,nombre,password_hash,role,active) VALUES (%s,%s,%s,%s,1)",
                        (username, nombre, generate_password_hash(password), role),
                    )
        conn.commit()
    finally:
        conn.close()


def init_resets_table():
    """Crea la tabla de tokens de recuperación de contraseña."""
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS `{RESETS_TABLE}` (
                id         INT AUTO_INCREMENT PRIMARY KEY,
                user_id    INT NOT NULL,
                token      VARCHAR(120) NOT NULL UNIQUE,
                expires_at DATETIME NOT NULL,
                used       TINYINT(1) NOT NULL DEFAULT 0,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_token (token),
                INDEX idx_user  (user_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
    conn.commit()


def init_db():
    """Inicializa el esquema MySQL. Sin SQLite — todo va a MySQL."""
    init_mysql_schema()
    init_hrm_tables()
    init_eval_tables()
    init_resets_table()
    ensure_erp_table_index()


# ─────────────────────────────────────────────
#  Helpers de negocio
# ─────────────────────────────────────────────

def calc_pv(largo, ancho, alto):
    return round(float(largo or 0) * float(ancho or 0) * float(alto or 0) / 4000.0, 2)


def enrich(raw_bultos):
    return [{**dict(b), "peso_vol": calc_pv(b["largo"], b["ancho"], b["alto"])} for b in raw_bultos]


def to_f(value):
    try:
        return float(str(value or "0").replace(",", "."))
    except (TypeError, ValueError):
        return 0.0


def current_username():
    """Devuelve el nombre (no el correo) del usuario activo para registrar quién actuó."""
    return g.user["nombre"] if getattr(g, "user", None) else None


def permission_set(role):
    """
    Roles del sistema:
      superadmin — acceso total + preguntas genéricas + gestión avanzada
      admin      — igual que superadmin menos preguntas genéricas
      editor     — crear/editar evaluaciones y colaboradores (sin borrar, sin admin)
      lector     — solo lectura
      vendedor   — acceso al módulo Cubicador (solo lectura de productos)
    """
    base = {
        "view":       False,
        "edit":       False,
        "print":      False,
        "create":     False,
        "delete":     False,
        "admin":      False,   # gestión usuarios / configuración
        "superadmin": False,   # preguntas genéricas + acciones irreversibles
        "hrm":        False,   # módulo colaboradores
        "cubicador":  False,   # módulo cubicador de documentos
    }
    if role == "superadmin":
        return {k: True for k in base}
    if role == "admin":
        return {**base, "view": True, "edit": True, "print": True,
                "create": True, "delete": True, "admin": True, "hrm": True,
                "cubicador": True}
    if role == "editor":
        return {**base, "view": True, "edit": True, "print": True,
                "create": True, "hrm": True}
    if role == "lector":
        return {**base, "view": True}
    if role == "vendedor":
        return {**base, "view": True, "cubicador": True}
    return base


def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXT


def delete_photo_file(filename):
    """Elimina foto local o de Cloudinary según el contenido de filename."""
    _cloud_delete(filename)


@app.template_global()
def photo_src(filename, subfolder="uploads"):
    """Devuelve la URL de la foto: directa si es Cloudinary, local si no."""
    if not filename:
        return ""
    if filename.startswith("http"):
        return filename
    return url_for("static", filename=f"{subfolder}/{filename}")


# ─────────────────────────────────────────────
#  Auth helpers
# ─────────────────────────────────────────────

def get_auth_user_by_id(user_id):
    return mysql_fetchone(
        f"SELECT id,username,nombre,password_hash,role,active FROM `{AUTH_TABLE}` WHERE id=%s",
        (user_id,),
    )


def get_auth_user_by_username(username):
    return mysql_fetchone(
        f"SELECT id,username,nombre,password_hash,role,active FROM `{AUTH_TABLE}` WHERE username=%s",
        (username,),
    )


def load_current_user():
    """
    Carga el usuario actual.
    — Primer intento: datos cacheados en session (sin query a BD).
    — Si no hay caché o el ID no coincide: consulta la BD y guarda en caché.
    Elimina ~1 query MySQL por cada página cargada.
    """
    g.user = None
    g.permissions = permission_set(None)
    user_id = session.get("user_id")
    if not user_id:
        return

    # ── Intento 1: caché en session ───────────────────────────
    cached = session.get("_uc")       # _uc = user cache
    if cached and cached.get("id") == user_id:
        g.user = cached
        g.permissions = permission_set(cached["role"])
        return

    # ── Intento 2: consulta BD (y guarda en caché) ────────────
    try:
        user = get_auth_user_by_id(user_id)
    except Exception as exc:
        session.clear()
        flash(f"No fue posible validar la sesion: {exc}", "danger")
        return
    if user and user["active"]:
        g.user = user
        g.permissions = permission_set(user["role"])
        # Guarda en session sin el hash de contraseña
        session["_uc"] = {
            "id":       user["id"],
            "username": user["username"],
            "nombre":   user["nombre"],
            "role":     user["role"],
            "active":   user["active"],
        }
    else:
        session.clear()


def login_required(view):
    @wraps(view)
    def wrapped(*a, **kw):
        if not g.user:
            flash("Inicia sesion para continuar.", "warning")
            return redirect(url_for("login", next=request.path))
        return view(*a, **kw)
    return wrapped


def require_permission(permission):
    def decorator(view):
        @wraps(view)
        def wrapped(*a, **kw):
            if not g.user:
                flash("Inicia sesion para continuar.", "warning")
                return redirect(url_for("login", next=request.path))
            if not g.permissions.get(permission):
                flash("No tienes permisos para realizar esta accion.", "danger")
                return redirect(url_for("index"))
            return view(*a, **kw)
        return wrapped
    return decorator


@app.before_request
def before_request():
    load_current_user()


@app.teardown_appcontext
def close_db(exc=None):
    db = g.pop("_db", None)
    if db is not None:
        try:
            db.close()
        except Exception:
            pass


# ─────────────────────────────────────────────
#  Product queries
# ─────────────────────────────────────────────

# ── Caché simple del listado de productos ──────────────────────
# Evita re-ejecutar el UNION ALL + 4 JOINs en cada recarga de inicio.
# Se invalida automáticamente al guardar/editar cualquier producto.
_listing_cache: dict = {}
_listing_cache_lock = threading.Lock()
_LISTING_TTL = 45   # segundos

def _invalidate_listing_cache():
    """Llama esto cada vez que se crea, edita o elimina un producto."""
    with _listing_cache_lock:
        _listing_cache.clear()


def get_product_listing(search_query=""):
    """
    Devuelve TODOS los productos visibles:
    - Productos del ERP (con o sin etiqueta en app_products)
    - Productos creados directamente en la app que NO están en el ERP
    """
    cache_key = search_query.strip().lower()
    now = time.time()
    with _listing_cache_lock:
        entry = _listing_cache.get(cache_key)
        if entry and (now - entry[1]) < _LISTING_TTL:
            return entry[0]
    erp_params = []
    app_params = []
    if search_query:
        like_u = f"%{search_query.upper()}%"
        like_p = f"%{search_query}%"
        erp_where = "WHERE UPPER(TRIM(e.`SKU`)) LIKE %s OR e.`Nombre` LIKE %s"
        app_where = "AND (p.sku LIKE %s OR p.nombre LIKE %s)"
        erp_params = [like_u, like_p]
        app_params = [like_u, like_p]
    else:
        erp_where = ""
        app_where = ""

    sql = f"""
        (
          SELECT
              UPPER(TRIM(e.`SKU`))                     AS sku,
              TRIM(COALESCE(e.`Nombre`,  ''))           AS nombre,
              COALESCE(p.estado, 'Pendiente')           AS estado,
              COALESCE(p.codigo, '')                    AS codigo,
              p.id                                      AS app_product_id,
              p.created_by,
              p.updated_by,
              COALESCE(COUNT(DISTINCT b.id),  0)        AS total_bultos,
              COALESCE(SUM(b.peso), 0)                  AS peso_total,
              ROUND(COALESCE(SUM((b.largo*b.ancho*b.alto)/4000),0),2) AS pv_total,
              COALESCE(COUNT(DISTINCT ph.id), 0)        AS total_fotos
          FROM `{ERP_TABLE}` e
          LEFT JOIN `{PRODUCTS_TABLE}` p  ON p.sku = UPPER(TRIM(e.`SKU`))
          LEFT JOIN `{BULTOS_TABLE}`   b  ON b.product_id = p.id
          LEFT JOIN `{PHOTOS_TABLE}`   ph ON ph.product_id = p.id
          {erp_where}
          GROUP BY UPPER(TRIM(e.`SKU`)), TRIM(COALESCE(e.`Nombre`,'')),
                   p.id, p.estado, p.codigo, p.created_by, p.updated_by
        )
        UNION ALL
        (
          SELECT
              p.sku                                     AS sku,
              p.nombre                                  AS nombre,
              p.estado                                  AS estado,
              p.codigo                                  AS codigo,
              p.id                                      AS app_product_id,
              p.created_by,
              p.updated_by,
              COALESCE(COUNT(DISTINCT b.id),  0)        AS total_bultos,
              COALESCE(SUM(b.peso), 0)                  AS peso_total,
              ROUND(COALESCE(SUM((b.largo*b.ancho*b.alto)/4000),0),2) AS pv_total,
              COALESCE(COUNT(DISTINCT ph.id), 0)        AS total_fotos
          FROM `{PRODUCTS_TABLE}` p
          LEFT JOIN `{BULTOS_TABLE}`   b  ON b.product_id = p.id
          LEFT JOIN `{PHOTOS_TABLE}`   ph ON ph.product_id = p.id
          LEFT JOIN `{ERP_TABLE}`      e  ON p.sku = UPPER(TRIM(e.`SKU`))
          WHERE e.`SKU` IS NULL
          {app_where}
          GROUP BY p.id, p.sku, p.nombre, p.estado, p.codigo, p.created_by, p.updated_by
        )
        ORDER BY nombre, sku
    """
    result = mysql_fetchall(sql, erp_params + app_params)
    with _listing_cache_lock:
        _listing_cache[cache_key] = (result, time.time())
    return result


def get_erp_product_by_sku(sku):
    return mysql_fetchone(
        f"""SELECT UPPER(TRIM(`SKU`)) AS sku,
                   TRIM(COALESCE(`Nombre`,'')) AS nombre,
                   TRIM(COALESCE(`Estado`,'Pendiente')) AS estado,
                   TRIM(COALESCE(`Codigo`,'')) AS codigo
            FROM `{ERP_TABLE}` WHERE UPPER(TRIM(`SKU`))=%s LIMIT 1""",
        (sku.strip().upper(),),
    )


def get_full(product_id):
    product = mysql_fetchone(f"SELECT * FROM `{PRODUCTS_TABLE}` WHERE id=%s", (product_id,))
    if not product:
        return None, [], []
    bultos = mysql_fetchall(
        f"SELECT * FROM `{BULTOS_TABLE}` WHERE product_id=%s ORDER BY bulto_num", (product_id,)
    )
    photos = mysql_fetchall(
        f"SELECT * FROM `{PHOTOS_TABLE}` WHERE product_id=%s ORDER BY orden", (product_id,)
    )
    return product, enrich(bultos), photos


def ensure_product_record_from_erp(sku):
    normalized = sku.strip().upper()
    existing = mysql_fetchone(f"SELECT id FROM `{PRODUCTS_TABLE}` WHERE sku=%s", (normalized,))
    if existing:
        return existing["id"]
    erp = get_erp_product_by_sku(normalized)
    if not erp:
        return None
    codigo = next_codigo()
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute(
            f"""INSERT INTO `{PRODUCTS_TABLE}` (sku,nombre,estado,codigo,erp_sync,created_by,updated_by)
                VALUES (%s,%s,%s,%s,1,%s,%s)""",
            (erp["sku"], erp["nombre"], erp["estado"], codigo,
             current_username(), current_username()),
        )
        pid = cur.lastrowid
    conn.commit()
    return pid


def save_bultos_mysql(conn, product_id, form):
    with conn.cursor() as cur:
        for idx in range(1, MAX_BULTOS + 1):
            largo = to_f(form.get(f"largo_{idx}"))
            ancho = to_f(form.get(f"ancho_{idx}"))
            alto  = to_f(form.get(f"alto_{idx}"))
            peso  = to_f(form.get(f"peso_{idx}"))
            if largo > 0 or ancho > 0 or alto > 0 or peso > 0:
                cur.execute(
                    f"""INSERT INTO `{BULTOS_TABLE}` (product_id,bulto_num,largo,ancho,alto,peso)
                        VALUES (%s,%s,%s,%s,%s,%s)""",
                    (product_id, idx, largo, ancho, alto, peso),
                )


def next_codigo():
    """Devuelve el siguiente código de impresión autoincremental (001, 002, ...)."""
    row = mysql_fetchone(
        f"SELECT MAX(CAST(codigo AS UNSIGNED)) AS max_c "
        f"FROM `{PRODUCTS_TABLE}` WHERE codigo REGEXP '^[0-9]+$'"
    )
    nxt = (int(row["max_c"]) + 1) if row and row["max_c"] else 1
    return str(nxt).zfill(3)


def validate_bultos_form(form):
    """Devuelve lista de errores si los bultos no son válidos."""
    errors = []
    has_any = False
    for idx in range(1, MAX_BULTOS + 1):
        largo = to_f(form.get(f"largo_{idx}"))
        ancho = to_f(form.get(f"ancho_{idx}"))
        alto  = to_f(form.get(f"alto_{idx}"))
        peso  = to_f(form.get(f"peso_{idx}"))
        vals  = [largo, ancho, alto, peso]
        if any(v > 0 for v in vals):
            has_any = True
            if not all(v > 0 for v in vals):
                errors.append(f"Bulto {idx}: todos los valores (largo, ancho, alto, peso) deben ser mayores a 0.")
    if not has_any:
        errors.append("Debes agregar al menos un bulto con medidas y peso completos.")
    return errors


# ─────────────────────────────────────────────
#  PDF builder — Playwright (pixel-perfect al HTML preview)
# ─────────────────────────────────────────────

def _logo_data_url():
    """Logo PNG como data:image URL — se incrusta en el HTML standalone sin depender del servidor."""
    for fname in ("Logo.png", "logo.png", "LOGO.png"):
        path = os.path.join(BASE_DIR, "static", fname)
        if os.path.exists(path):
            try:
                with open(path, "rb") as f:
                    b64 = base64.b64encode(f.read()).decode()
                return f"data:image/png;base64,{b64}"
            except Exception:
                pass
    return ""


def build_label_pdf(product, bulto, total_bultos):
    """
    Genera el PDF de una etiqueta usando Playwright (Chromium headless).
    El resultado es pixel-perfect idéntico al HTML preview porque usa
    el mismo template label_standalone.html y el mismo @media print CSS.
    Si Playwright no está instalado, lanza ImportError con instrucción de instalación.
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        raise ImportError(
            "Playwright no instalado. Ejecuta:\n"
            "  pip install playwright\n"
            "  playwright install chromium"
        )

    fecha   = datetime.now().strftime("%d-%m-%Y %H:%M")
    enrich_b = {**dict(bulto), "peso_vol": calc_pv(
        bulto["largo"], bulto["ancho"], bulto["alto"])}

    html = render_template(
        "label_standalone.html",
        product      = product,
        bultos       = [enrich_b],
        total_bultos = total_bultos,
        fecha        = fecha,
        qty_per_bulto= {int(bulto["bulto_num"]): 1},
        logo_url     = _logo_data_url(),
    )

    with sync_playwright() as pw:
        browser = pw.chromium.launch()
        page    = browser.new_page()
        # set_content carga el HTML; wait_until='load' espera que el JS del barcode se ejecute
        page.set_content(html, wait_until="load")
        # Pausa extra para que JsBarcode termine de dibujar los SVGs
        page.wait_for_timeout(900)
        pdf_bytes = page.pdf(
            width            = "150mm",
            height           = "70mm",
            print_background = True,
            margin           = {"top": "0mm", "right": "0mm",
                                "bottom": "0mm", "left": "0mm"},
        )
        browser.close()

    return pdf_bytes


# ── Mantener la antigua firma por compatibilidad ── (ya no usa reportlab)
def _build_label_pdf_legacy(product, bulto, total_bultos):
    """Fallback ReportLab — ya no se usa, conservado como referencia."""
    from reportlab.lib.units import mm
    from reportlab.lib import colors
    from reportlab.pdfgen import canvas
    from reportlab.pdfbase import pdfmetrics
    from reportlab.graphics.barcode import code128

    W, H    = 150 * mm, 70 * mm
    HALF    = W / 2                 # 75 mm por cara
    HDR_H   = 14 * mm
    META_H  =  8 * mm
    NAME_H  =  9 * mm
    FTR_H   =  5 * mm
    hdr_y   = H - HDR_H
    meta_y  = hdr_y - META_H
    name_y  = meta_y - NAME_H
    body_y  = FTR_H
    body_h  = name_y - body_y      # ≈ 34mm

    BLACK = colors.black
    WHITE = colors.white
    GRAY  = colors.HexColor("#aaaaaa")
    DGRAY = colors.HexColor("#2c2c2c")

    buf   = io.BytesIO()
    c     = canvas.Canvas(buf, pagesize=(W, H))
    fecha = datetime.now().strftime("%d-%m-%Y %H:%M")

    # ── Intenta cargar logo (convertido a blanco via Pillow si está disponible) ──
    logo_img_path = None
    try:
        from PIL import Image as PILImage
        import tempfile
        src = os.path.join(app.static_folder, "Logo.png")
        if os.path.exists(src):
            img = PILImage.open(src).convert("RGBA")
            r, g, b, a = img.split()
            white_bg = PILImage.new("RGBA", img.size, (0, 0, 0, 255))
            # Pintar píxeles no-transparentes de blanco
            white_layer = PILImage.new("RGBA", img.size, (255, 255, 255, 255))
            white_bg.paste(white_layer, mask=a)
            tmp = tempfile.NamedTemporaryFile(suffix=".png", delete=False)
            white_bg.save(tmp.name)
            logo_img_path = tmp.name
    except Exception:
        logo_img_path = None

    def draw_half(ox):
        # ══════════════════════════════════════════
        #  HEADER  (negro, 14mm)
        # ══════════════════════════════════════════
        c.setFillColor(BLACK)
        c.rect(ox, hdr_y, HALF, HDR_H, fill=1, stroke=0)

        # Proporciones: SHS izq 40% | ILUS der 60%
        shs_w  = HALF * 0.40
        ilus_w = HALF * 0.60
        shs_cx = ox + shs_w / 2
        ilus_x = ox + shs_w          # borde derecho de SHS = borde izq de ILUS
        ilus_cx = ilus_x + ilus_w / 2

        # Separador interno
        c.setStrokeColor(DGRAY)
        c.setLineWidth(0.6)
        c.line(ilus_x, hdr_y + 1*mm, ilus_x, hdr_y + HDR_H - 1*mm)

        # — Sports Health Solutions (izquierda) —
        c.setFillColor(WHITE)
        c.setFont("Helvetica-Bold", 8)
        c.drawCentredString(shs_cx, hdr_y + 9.2*mm, "SPORTS")
        c.drawCentredString(shs_cx, hdr_y + 5.8*mm, "HEALTH")
        c.setFillColor(GRAY)
        c.setFont("Helvetica-Oblique", 4)
        c.drawCentredString(shs_cx, hdr_y + 2.2*mm, "S O L U T I O N S")

        # — Logo ILUS (derecha) —
        if logo_img_path:
            try:
                lh = HDR_H * 0.78
                lw = ilus_w * 0.82
                c.drawImage(logo_img_path,
                            ilus_cx - lw / 2,
                            hdr_y + (HDR_H - lh) / 2,
                            width=lw, height=lh,
                            preserveAspectRatio=True, mask='auto')
            except Exception:
                _draw_ilus_text(ilus_cx)
        else:
            _draw_ilus_text(ilus_cx)

    def _draw_ilus_text(cx):
        """Fallback: texto ILUS. en blanco cuando no hay imagen."""
        c.setFillColor(WHITE)
        c.setFont("Helvetica-Bold", 18)
        iw = pdfmetrics.stringWidth("ILUS.", "Helvetica-Bold", 18)
        # cx es el centro de la zona ILUS dentro de la cara actual
        c.drawCentredString(cx, hdr_y + 4*mm, "ILUS.")

    def draw_body(ox):
        # ══════════════════════════════════════════
        #  META BAR  (blanco, 8mm)
        # ══════════════════════════════════════════
        c.setFillColor(WHITE)
        c.rect(ox, meta_y, HALF, META_H, fill=1, stroke=0)
        c.setStrokeColor(BLACK)
        c.setLineWidth(0.8)
        c.line(ox, meta_y,          ox + HALF, meta_y)
        c.line(ox, meta_y + META_H, ox + HALF, meta_y + META_H)

        ty = meta_y + META_H / 2 - 1.4*mm

        # Ícono caja (aproximación con rectángulo)
        cx_icon = ox + 5*mm
        c.setFillColor(BLACK)
        c.setLineWidth(0.6)
        c.rect(cx_icon - 1.4*mm, ty + 0.3*mm, 2.8*mm, 2.8*mm, fill=0, stroke=1)

        c.setFont("Helvetica-Bold", 6.5)
        c.setFillColor(BLACK)
        c.drawString(ox + 7.5*mm, ty,
                     f"BULTO {bulto['bulto_num']} DE {total_bultos}")

        # Separador |
        sep_x = ox + HALF * 0.52
        c.setFont("Helvetica", 10)
        c.setFillColor(GRAY)
        c.drawCentredString(sep_x, ty - 0.5*mm, "|")

        c.setFont("Helvetica-Bold", 6.5)
        c.setFillColor(BLACK)
        c.drawString(sep_x + 2.5*mm, ty, "PESO BRUTO")
        c.setFont("Helvetica-Bold", 7)
        c.drawRightString(ox + HALF - 2*mm, ty,
                          f"{float(bulto['peso']):.0f} KG")

        # ══════════════════════════════════════════
        #  NOMBRE PRODUCTO  (blanco, 9mm)
        # ══════════════════════════════════════════
        c.setFillColor(WHITE)
        c.rect(ox, name_y, HALF, NAME_H, fill=1, stroke=0)
        c.setStrokeColor(BLACK)
        c.setLineWidth(0.8)
        c.line(ox, name_y, ox + HALF, name_y)

        nombre = (product["nombre"] or "").upper()
        avail  = HALF - 5*mm
        fs_n   = 7.5
        while pdfmetrics.stringWidth(nombre, "Helvetica-Bold", fs_n) > avail and fs_n > 4.5:
            fs_n -= 0.15
        c.setFont("Helvetica-Bold", fs_n)
        c.setFillColor(BLACK)
        # Si cabe en una línea
        if pdfmetrics.stringWidth(nombre, "Helvetica-Bold", fs_n) <= avail:
            c.drawString(ox + 2.5*mm, name_y + NAME_H/2 - 1.3*mm, nombre)
        else:
            # Partir en dos líneas
            mid = len(nombre) // 2
            cut = nombre.rfind(" ", 0, mid + 10) or mid
            c.setFont("Helvetica-Bold", max(fs_n - 0.3, 5.0))
            c.drawString(ox + 2.5*mm, name_y + NAME_H - 3.8*mm, nombre[:cut].strip())
            c.drawString(ox + 2.5*mm, name_y + 1.8*mm,          nombre[cut:].strip())

        # ══════════════════════════════════════════
        #  BODY  (dims izq 50% | código der 50%)
        # ══════════════════════════════════════════
        dims_w   = HALF * 0.50
        dims_end = ox + dims_w
        bc_x     = dims_end + 0.5*mm          # inicio columna código
        bc_w     = HALF - dims_w - 0.5*mm     # ancho disponible código

        # Separador vertical dims | código
        c.setStrokeColor(BLACK)
        c.setLineWidth(0.8)
        c.line(dims_end, body_y, dims_end, name_y)

        # — Dimensiones (4 filas centradas verticalmente) —
        rows = [
            ("↔",  "LARGO:",     f"{float(bulto['largo']):.0f} cm"),
            ("↕",  "ALTO:",      f"{float(bulto['alto']):.0f} cm"),
            ("⊞",  "ANCHO:",     f"{float(bulto['ancho']):.0f} cm"),
            ("🔒", "PESO VOL.:", f"{float(bulto['peso_vol']):.2f} Vol"),
        ]
        row_h  = body_h / len(rows)
        lbl_x  = ox + 3*mm      # inicio etiqueta (3mm desde borde izq)
        val_x  = dims_end - 1*mm  # valores anclados a la derecha

        for i, (ico, lbl, val) in enumerate(rows):
            ry = name_y - (i + 0.62) * row_h

            # Etiqueta
            c.setFont("Helvetica-Bold", 5.5)
            c.setFillColor(BLACK)
            c.drawString(lbl_x, ry, lbl)

            # Valor (ancla derecha)
            c.setFont("Helvetica-Bold", 6.5)
            c.drawRightString(val_x, ry, val)

        # — Barcode CODE128 —
        bc_val = (product["sku"] or "000")[:30]

        # Reservar espacio: barcode + SKU box + fecha
        sku_h    = body_h * 0.30     # caja SKU negra
        fecha_h  = 3   * mm          # línea de fecha
        bc_h     = body_h - sku_h - fecha_h - 1.5*mm  # altura barras

        bw_pt = 1.0 if len(bc_val) > 20 else (0.85 if len(bc_val) > 12 else 1.0)
        bc_y  = body_y + sku_h + fecha_h + 0.5*mm

        try:
            bar = code128.Code128(bc_val, barWidth=bw_pt,
                                  barHeight=bc_h, humanReadable=False, quiet=False)
            while bar.width > bc_w - 1*mm and bw_pt > 0.35:
                bw_pt -= 0.04
                bar = code128.Code128(bc_val, barWidth=bw_pt,
                                      barHeight=bc_h, humanReadable=False, quiet=False)
            bc_cx = bc_x + bc_w / 2
            bar.drawOn(c, bc_cx - bar.width / 2, bc_y)
        except Exception:
            pass

        # — Caja negra CÓDIGO / SKU —
        sku_y = body_y + fecha_h
        c.setFillColor(BLACK)
        c.rect(bc_x, sku_y, bc_w, sku_h, fill=1, stroke=0)

        c.setFont("Helvetica-Bold", 3.8)
        c.setFillColor(GRAY)
        c.drawCentredString(bc_x + bc_w / 2, sku_y + sku_h - 2.2*mm, "CÓDIGO / SKU")

        sku_val = product["sku"]
        fs_s    = 9.5
        while pdfmetrics.stringWidth(sku_val, "Helvetica-Bold", fs_s) > bc_w - 2*mm and fs_s > 5:
            fs_s -= 0.3
        c.setFont("Helvetica-Bold", fs_s)
        c.setFillColor(WHITE)
        c.drawCentredString(bc_x + bc_w / 2, sku_y + sku_h * 0.20, sku_val)

        # — Fecha bajo el SKU box (horizontal, no interfiere con el barcode) —
        c.setFont("Helvetica", 3.5)
        c.setFillColor(GRAY)
        c.drawCentredString(bc_x + bc_w / 2, body_y + 0.6*mm, fecha)

    # ── Dibuja las dos caras ──────────────────────────────────────────
    draw_half(0)
    draw_body(0)
    draw_half(HALF)
    draw_body(HALF)

    # ── Borde exterior ────────────────────────────────────────────────
    c.setStrokeColor(BLACK)
    c.setLineWidth(0.8)
    c.rect(0, 0, W, H, fill=0, stroke=1)

    # ── Línea de doblez central (punteada) ───────────────────────────
    c.setStrokeColor(colors.HexColor("#999999"))
    c.setLineWidth(0.6)
    c.setDash([2.5, 2.5])
    c.line(HALF, 0, HALF, H)
    c.setDash([])

    # ── Footer: franjas diagonales B&W  (-45°, igual que el HTML) ────
    c.setFillColor(BLACK)
    c.rect(0, 0, W, FTR_H, fill=1, stroke=0)
    c.setFillColor(WHITE)
    sg, sw = 8*mm, 4*mm
    for i in range(-2, int(W / sg) + 4):
        x0 = i * sg
        p  = c.beginPath()
        p.moveTo(x0, 0);            p.lineTo(x0 + sw, 0)
        p.lineTo(x0 + sw + FTR_H, FTR_H); p.lineTo(x0 + FTR_H, FTR_H)
        p.close()
        c.drawPath(p, fill=1, stroke=0)
    c.setStrokeColor(BLACK)
    c.setLineWidth(0.8)
    c.line(0, FTR_H, W, FTR_H)

    c.save()
    buf.seek(0)

    # Limpiar archivo temporal del logo blanco
    if logo_img_path:
        try:
            os.unlink(logo_img_path)
        except Exception:
            pass

    return buf.read()   # fin de _build_label_pdf_legacy


# ─────────────────────────────────────────────
#  Context processor
# ─────────────────────────────────────────────

@app.context_processor
def inject_globals():
    return {
        "has_logo":    os.path.exists(os.path.join(app.static_folder, "Logo.png")),
        "current_user": g.user,
        "permissions":  g.permissions,
        "role_label":   {
                            "superadmin": "Super Administrador",
                            "admin":      "Administrador",
                            "editor":     "Editor",
                            "lector":     "Lector",
                            "vendedor":   "Vendedor",
                        }.get(g.user["role"] if g.user else "", "Usuario"),
    }


# ─────────────────────────────────────────────
#  API — Búsqueda en ERP externo (SOLO LECTURA)
# ─────────────────────────────────────────────

@app.route("/api/product-search")
@login_required
def product_search():
    """
    Typeahead unificado: busca en la tabla ERP local (etiquetas) + app_products.
    Fuente secundaria: ERP externo (cloud.random.cl) — solo lectura, fallo silencioso.
    Devuelve JSON: [{sku, nombre, source, already_exists}]
    """
    q = request.args.get("q", "").strip()
    if len(q) < 2:
        return jsonify([])

    like_u = f"%{q.upper()}%"
    like_p = f"%{q}%"
    results = []
    seen    = set()

    # ── 1. app_products (nuestra base de etiquetas) ──────────────────────────
    try:
        rows = mysql_fetchall(
            f"""SELECT sku, nombre FROM `{PRODUCTS_TABLE}`
                WHERE UPPER(sku) LIKE %s OR UPPER(nombre) LIKE %s
                ORDER BY nombre LIMIT 12""",
            (like_u, like_u),
        )
        for r in rows:
            sku = (r.get("sku") or "").strip().upper()
            if sku and sku not in seen:
                results.append({"sku": sku, "nombre": (r.get("nombre") or "").strip(),
                                 "source": "app", "already_exists": True})
                seen.add(sku)
    except Exception:
        pass

    # ── 2. Tabla ERP local (etiquetas Clever Cloud) ──────────────────────────
    try:
        rows2 = mysql_fetchall(
            f"""SELECT UPPER(TRIM(`SKU`)) AS sku,
                       TRIM(COALESCE(`Nombre`, '')) AS nombre
                FROM `{ERP_TABLE}`
                WHERE UPPER(TRIM(`SKU`)) LIKE %s OR `Nombre` LIKE %s
                ORDER BY `Nombre` LIMIT 15""",
            (like_u, like_p),
        )
        for r in rows2:
            sku = (r.get("sku") or "").strip().upper()
            if sku and sku not in seen:
                already = bool(mysql_fetchone(
                    f"SELECT id FROM `{PRODUCTS_TABLE}` WHERE sku=%s", (sku,)
                ))
                results.append({"sku": sku, "nombre": (r.get("nombre") or "").strip(),
                                 "source": "erp-local", "already_exists": already})
                seen.add(sku)
    except Exception:
        pass

    # Ordenar: primero los que NO están en la DB (disponibles para crear),
    # después los que ya existen.
    results.sort(key=lambda x: (1 if x["already_exists"] else 0, x["nombre"].lower()))

    return jsonify(results[:20])


# ─────────────────────────────────────────────
#  Auth routes
# ─────────────────────────────────────────────

@app.route("/login", methods=["GET", "POST"])
def login():
    if g.user:
        return redirect(url_for("index"))
    next_url = request.args.get("next") or request.form.get("next") or url_for("index")
    if request.method == "POST":
        username = request.form.get("username", "").strip().lower()
        password = request.form.get("password", "")
        try:
            user = get_auth_user_by_username(username)
        except Exception as exc:
            flash(f"No fue posible conectar: {exc}", "danger")
            return render_template("login.html", next_url=next_url, username=username)
        if not user or not user["active"] or not check_password_hash(user["password_hash"], password):
            flash("Usuario o contraseña incorrectos.", "danger")
            return render_template("login.html", next_url=next_url, username=username)
        session.clear()
        session["user_id"] = user["id"]
        flash(f"Bienvenido, {user['nombre']}.", "success")
        return redirect(next_url)
    return render_template("login.html", next_url=next_url, username="")


@app.route("/logout", methods=["POST"])
@login_required
def logout():
    session.clear()
    flash("Sesion cerrada.", "success")
    return redirect(url_for("login"))


# ─────────────────────────────────────────────
#  Recuperación de contraseña
# ─────────────────────────────────────────────

def _send_recovery_email(to_addr: str, to_name: str, reset_url: str) -> bool:
    """Envía el correo HTML de recuperación. Retorna True si tuvo éxito."""
    cfg = EMAIL_CONFIG
    subject = "Recuperar contraseña — ILUS Sport & Health"

    html_body = f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Recuperar contraseña</title>
</head>
<body style="margin:0;padding:0;background:#f4f4f4;font-family:'Segoe UI',Arial,sans-serif">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#f4f4f4;padding:40px 0">
    <tr><td align="center">
      <table width="520" cellpadding="0" cellspacing="0"
             style="background:#fff;border-radius:12px;overflow:hidden;
                    box-shadow:0 4px 24px rgba(0,0,0,.10);max-width:520px;width:100%">

        <!-- Cabecera roja -->
        <tr>
          <td style="background:#CC0000;padding:28px 32px;text-align:center">
            <div style="font-size:26px;font-weight:900;letter-spacing:3px;color:#fff;
                        font-family:'Segoe UI',Arial,sans-serif">ILUS</div>
            <div style="font-size:11px;color:rgba(255,255,255,.75);letter-spacing:2px;
                        text-transform:uppercase;margin-top:2px">Sport &amp; Health</div>
          </td>
        </tr>

        <!-- Cuerpo -->
        <tr>
          <td style="padding:36px 40px 28px">
            <p style="margin:0 0 6px;font-size:22px;font-weight:700;color:#111">
              Recuperar contraseña
            </p>
            <p style="margin:0 0 22px;font-size:14px;color:#555;line-height:1.6">
              Hola <strong>{to_name}</strong>, recibimos una solicitud para restablecer
              la contraseña de tu cuenta en el sistema ILUS.
            </p>
            <p style="margin:0 0 28px;font-size:14px;color:#555;line-height:1.6">
              Haz clic en el botón a continuación para crear una nueva contraseña.
              Este enlace es válido por <strong>60 minutos</strong>.
            </p>

            <!-- Botón -->
            <table cellpadding="0" cellspacing="0" width="100%">
              <tr>
                <td align="center">
                  <a href="{reset_url}"
                     style="display:inline-block;background:#CC0000;color:#fff;
                            text-decoration:none;font-size:15px;font-weight:700;
                            padding:14px 38px;border-radius:8px;letter-spacing:.3px">
                    Restablecer contraseña
                  </a>
                </td>
              </tr>
            </table>

            <p style="margin:28px 0 8px;font-size:12px;color:#888;line-height:1.6">
              Si no solicitaste este cambio, puedes ignorar este correo —
              tu contraseña seguirá siendo la misma.
            </p>
            <p style="margin:0;font-size:11px;color:#bbb;word-break:break-all">
              O copia este enlace en tu navegador:<br>
              <a href="{reset_url}" style="color:#CC0000;text-decoration:none">{reset_url}</a>
            </p>
          </td>
        </tr>

        <!-- Pie -->
        <tr>
          <td style="background:#f8f8f8;padding:16px 40px;border-top:1px solid #eee;
                     text-align:center;font-size:11px;color:#aaa">
            ILUS Sport &amp; Health &mdash; Sistema de Gestión Interno &mdash; 2026
          </td>
        </tr>

      </table>
    </td></tr>
  </table>
</body>
</html>"""

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = f"{cfg['from_name']} <{cfg['from_addr']}>"
        msg["To"]      = to_addr
        msg.attach(MIMEText(html_body, "html", "utf-8"))

        with smtplib.SMTP(cfg["smtp_host"], cfg["smtp_port"], timeout=15) as srv:
            srv.ehlo()
            srv.starttls()
            srv.login(cfg["smtp_user"], cfg["smtp_pass"])
            srv.sendmail(cfg["from_addr"], [to_addr], msg.as_string())
        return True
    except Exception as exc:
        print(f"[ILUS][EMAIL] Error al enviar correo: {exc}")
        return False


@app.route("/auth/olvidar-contrasena", methods=["GET", "POST"])
def forgot_password():
    if g.user:
        return redirect(url_for("index"))

    if request.method == "POST":
        email_input = request.form.get("email", "").strip().lower()

        # Siempre mostrar el mismo mensaje (no revelar si el email existe)
        flash("Si el correo está registrado, recibirás instrucciones en breve.", "info")

        try:
            if EMAIL_RE.match(email_input):
                user = get_auth_user_by_username(email_input)
                if user and user.get("active"):
                    token   = secrets.token_urlsafe(48)
                    expires = datetime.utcnow() + timedelta(minutes=60)
                    conn = get_db()
                    # Invalidar tokens anteriores del mismo usuario
                    with conn.cursor() as cur:
                        cur.execute(
                            f"UPDATE `{RESETS_TABLE}` SET used=1 WHERE user_id=%s AND used=0",
                            (user["id"],)
                        )
                        cur.execute(
                            f"INSERT INTO `{RESETS_TABLE}` (user_id, token, expires_at) VALUES (%s,%s,%s)",
                            (user["id"], token, expires)
                        )
                    conn.commit()

                    reset_url = url_for("reset_password", token=token, _external=True)
                    _send_recovery_email(user["username"], user["nombre"], reset_url)
        except Exception as _e:
            print(f"[ILUS][RESET] Error en flujo de recuperación: {_e}")

        return redirect(url_for("forgot_password"))

    return render_template("forgot_password.html")


@app.route("/auth/restablecer/<token>", methods=["GET", "POST"])
def reset_password(token):
    if g.user:
        return redirect(url_for("index"))

    conn = get_db()
    row  = None
    with conn.cursor() as cur:
        cur.execute(
            f"""SELECT r.*, u.nombre, u.username
                FROM `{RESETS_TABLE}` r
                JOIN `{AUTH_TABLE}` u ON u.id = r.user_id
                WHERE r.token=%s AND r.used=0 AND r.expires_at > UTC_TIMESTAMP()""",
            (token,)
        )
        row = cur.fetchone()

    if not row:
        flash("El enlace no es válido o ha expirado.", "danger")
        return redirect(url_for("forgot_password"))

    if request.method == "POST":
        pw1 = request.form.get("password", "")
        pw2 = request.form.get("password2", "")
        if len(pw1) < 8:
            flash("La contraseña debe tener al menos 8 caracteres.", "danger")
            return render_template("reset_password.html", token=token, nombre=row["nombre"])
        if pw1 != pw2:
            flash("Las contraseñas no coinciden.", "danger")
            return render_template("reset_password.html", token=token, nombre=row["nombre"])

        new_hash = generate_password_hash(pw1)
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE `{AUTH_TABLE}` SET password_hash=%s WHERE id=%s",
                (new_hash, row["user_id"])
            )
            cur.execute(
                f"UPDATE `{RESETS_TABLE}` SET used=1 WHERE token=%s",
                (token,)
            )
        conn.commit()

        flash("Contraseña actualizada. Ahora puedes iniciar sesión.", "success")
        return redirect(url_for("login"))

    return render_template("reset_password.html", token=token, nombre=row["nombre"])


# ─────────────────────────────────────────────
#  Productos — listado
# ─────────────────────────────────────────────

@app.route("/")
@login_required
def index():
    q     = request.args.get("q", "").strip()
    exact = request.args.get("exact") == "1"

    # ── Búsqueda exacta por escáner de código de barras ──────────
    # Si viene con exact=1 (del scanner físico), busca el SKU exacto.
    # Si encuentra exactamente un producto con ficha → va directo a él.
    if exact and q:
        sku_norm = q.upper()
        hit = mysql_fetchone(
            f"SELECT id FROM `{PRODUCTS_TABLE}` WHERE sku=%s", (sku_norm,)
        )
        if hit:
            return redirect(url_for("product_detail", pid=hit["id"]))
        # No tiene ficha aún → intenta crear desde ERP y redirige al índice filtrado
        flash(f"SKU {sku_norm} encontrado en ERP pero sin ficha. Usa 'Preparar' para crearlo.", "info")
        return redirect(url_for("index", q=sku_norm))

    products = get_product_listing(q)

    # Cobertura fotográfica — se calcula en Python para evitar problemas
    # de tipos (Decimal vs int) al comparar desde MySQL
    _fotos = [int(p.get("total_fotos") or 0) for p in products]
    foto0  = sum(1 for f in _fotos if f == 0)
    foto1  = sum(1 for f in _fotos if f == 1)
    foto2  = sum(1 for f in _fotos if f >= 2)

    return render_template("index.html", products=products, q=q,
                           foto0=foto0, foto1=foto1, foto2=foto2)


@app.route("/products/refresh-cache", methods=["POST"])
@login_required
def refresh_listing_cache():
    """Limpia el caché del listado de productos para ver datos frescos."""
    _invalidate_listing_cache()
    return redirect(url_for("index"))


# ─────────────────────────────────────────────
#  Productos — quick view JSON (para modal)
# ─────────────────────────────────────────────

@app.route("/products/<int:pid>/quick")
@login_required
def product_quick(pid):
    product, bultos, photos = get_full(pid)
    if not product:
        return jsonify({"error": "not found"}), 404
    photo_urls = [url_for("static", filename=f"uploads/{ph['filename']}") for ph in photos]
    return jsonify({
        "id":          product["id"],
        "sku":         product["sku"],
        "nombre":      product["nombre"],
        "estado":      product["estado"],
        "codigo":      product["codigo"] or "",
        "created_by":  product["created_by"] or "-",
        "updated_by":  product["updated_by"] or "-",
        "peso_total":  round(sum(float(b["peso"])     for b in bultos), 2),
        "pv_total":    round(sum(float(b["peso_vol"]) for b in bultos), 2),
        "total_bultos": len(bultos),
        "bultos": [
            {
                "num":      b["bulto_num"],
                "largo":    float(b["largo"]),
                "ancho":    float(b["ancho"]),
                "alto":     float(b["alto"]),
                "peso":     float(b["peso"]),
                "peso_vol": float(b["peso_vol"]),
            }
            for b in bultos
        ],
        "photos": photo_urls,
    })


# ─────────────────────────────────────────────
#  Productos — CRUD
# ─────────────────────────────────────────────

@app.route("/products/from-erp/<path:sku>")
@require_permission("create")
def prepare_product_from_erp(sku):
    pid = ensure_product_record_from_erp(sku)
    if not pid:
        flash("No se encontro ese producto en el ERP.", "danger")
        return redirect(url_for("index"))
    flash("Producto preparado desde el ERP.", "success")
    return redirect(url_for("edit_product", pid=pid))


@app.route("/products/new", methods=["GET", "POST"])
@require_permission("create")
def new_product():
    # Pre-calcular el código que se asignará (para mostrarlo en el form)
    auto_codigo = next_codigo()

    if request.method == "POST":
        sku    = request.form.get("sku",    "").strip().upper()
        nombre = request.form.get("nombre", "").strip()
        estado = request.form.get("estado", "Confirmado").strip()
        # El código NO viene del formulario: se genera server-side
        codigo = next_codigo()

        errors = []
        if not sku:
            errors.append("El SKU es requerido.")
        if not nombre:
            errors.append("El nombre es requerido.")
        if mysql_fetchone(f"SELECT id FROM `{PRODUCTS_TABLE}` WHERE sku=%s", (sku,)):
            errors.append(f"El SKU <b>{sku}</b> ya existe.")
        errors += validate_bultos_form(request.form)

        if errors:
            return render_template("product_form.html", errors=errors, product=None,
                                   fd=request.form, max_b=MAX_BULTOS, auto_codigo=codigo)

        conn = get_db()
        with conn.cursor() as cur:
            cur.execute(
                f"""INSERT INTO `{PRODUCTS_TABLE}` (sku,nombre,estado,codigo,created_by,updated_by)
                    VALUES (%s,%s,%s,%s,%s,%s)""",
                (sku, nombre, estado, codigo, current_username(), current_username()),
            )
            pid = cur.lastrowid
            save_bultos_mysql(conn, pid, request.form)
        conn.commit()

        # Sincronizar tabla compartida etiquetas
        _, bultos_sync, _ = get_full(pid)
        p_sync = {"sku": sku, "nombre": nombre, "estado": estado, "codigo": codigo}
        sync_erp_table(p_sync, bultos_sync, conn)
        conn.commit()

        _invalidate_listing_cache()
        flash(f"Producto <b>{sku}</b> creado con código <b>{codigo}</b>.", "success")
        return redirect(url_for("product_detail", pid=pid))

    return render_template("product_form.html", errors=[], product=None, fd={},
                           max_b=MAX_BULTOS, auto_codigo=auto_codigo)


@app.route("/products/<int:pid>/edit", methods=["GET", "POST"])
@require_permission("edit")
def edit_product(pid):
    product, bultos, photos = get_full(pid)
    if not product:
        flash("Producto no encontrado.", "danger")
        return redirect(url_for("index"))

    if request.method == "POST":
        sku    = request.form.get("sku",    "").strip().upper()
        nombre = request.form.get("nombre", "").strip()
        estado = request.form.get("estado", "Pendiente").strip()
        # El código NO se puede cambiar: siempre se conserva el valor actual
        codigo = product["codigo"]

        errors = []
        if not sku:
            errors.append("El SKU es requerido.")
        if not nombre:
            errors.append("El nombre es requerido.")
        if mysql_fetchone(f"SELECT id FROM `{PRODUCTS_TABLE}` WHERE sku=%s AND id<>%s", (sku, pid)):
            errors.append(f"El SKU <b>{sku}</b> ya está en uso.")
        errors += validate_bultos_form(request.form)

        if errors:
            return render_template("product_form.html", errors=errors, product=product,
                                   bultos=bultos, fd=request.form, max_b=MAX_BULTOS, photos=photos)

        conn = get_db()
        with conn.cursor() as cur:
            cur.execute(
                f"""UPDATE `{PRODUCTS_TABLE}`
                    SET sku=%s, nombre=%s, estado=%s, updated_by=%s
                    WHERE id=%s""",
                (sku, nombre, estado, current_username(), pid),
            )
            cur.execute(f"DELETE FROM `{BULTOS_TABLE}` WHERE product_id=%s", (pid,))
            save_bultos_mysql(conn, pid, request.form)
        conn.commit()

        # Sincronizar tabla compartida etiquetas
        _, bultos_sync, _ = get_full(pid)
        p_sync = {"sku": sku, "nombre": nombre, "estado": estado, "codigo": codigo}
        sync_erp_table(p_sync, bultos_sync, conn)
        conn.commit()

        _invalidate_listing_cache()
        flash(f"Producto <b>{sku}</b> actualizado.", "success")
        return redirect(url_for("product_detail", pid=pid))

    return render_template("product_form.html", errors=[], product=product,
                           bultos=bultos, photos=photos, fd={}, max_b=MAX_BULTOS)


@app.route("/products/<int:pid>")
@login_required
def product_detail(pid):
    product, bultos, photos = get_full(pid)
    if not product:
        flash("Producto no encontrado.", "danger")
        return redirect(url_for("index"))
    peso_total = round(sum(float(b["peso"])     for b in bultos), 2)
    pv_total   = round(sum(float(b["peso_vol"]) for b in bultos), 2)
    return render_template("product_detail.html", product=product, bultos=bultos,
                           photos=photos, peso_total=peso_total, pv_total=pv_total,
                           total_bultos=len(bultos), can_add_photo=(len(photos) < MAX_PHOTOS))


@app.route("/products/<int:pid>/delete", methods=["POST"])
@require_permission("delete")
def delete_product(pid):
    product, _, photos = get_full(pid)
    if not product:
        flash("Producto no encontrado.", "danger")
        return redirect(url_for("index"))

    # Verificar confirmación escribiendo el SKU
    confirm = request.form.get("confirm_sku", "").strip().upper()
    if confirm != product["sku"]:
        flash("Confirmación incorrecta. El producto NO fue eliminado.", "danger")
        return redirect(url_for("product_detail", pid=pid))

    for photo in photos:
        delete_photo_file(photo["filename"])

    conn = get_db()
    # Eliminar de la tabla compartida etiquetas ANTES de borrar en app_products
    delete_from_erp_table(product.get("codigo"), conn)
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM `{PRODUCTS_TABLE}` WHERE id=%s", (pid,))
    conn.commit()

    _invalidate_listing_cache()
    flash(f"Producto <b>{product['sku']}</b> eliminado permanentemente.", "warning")
    return redirect(url_for("index"))


# ─────────────────────────────────────────────
#  Fotos
# ─────────────────────────────────────────────

@app.route("/products/<int:pid>/photos", methods=["POST"])
@require_permission("edit")
def upload_photo(pid):
    product, _, photos = get_full(pid)
    if not product:
        flash("Producto no encontrado.", "danger")
        return redirect(url_for("index"))
    if len(photos) >= MAX_PHOTOS:
        flash(f"Maximo {MAX_PHOTOS} fotos por producto.", "warning")
        return redirect(url_for("product_detail", pid=pid))

    file = request.files.get("photo")
    if not file or not file.filename:
        flash("No se selecciono archivo.", "warning")
        return redirect(url_for("product_detail", pid=pid))
    if not allowed_file(file.filename):
        flash("Formato no permitido. Usa JPG, PNG, WEBP o GIF.", "danger")
        return redirect(url_for("product_detail", pid=pid))

    ext       = file.filename.rsplit(".", 1)[1].lower()
    ts        = int(datetime.now().timestamp())
    if _CLD_READY:
        try:
            filename = _cloud_upload(file, public_id=f"p{pid}_{ts}", folder="ilus/products")
        except Exception as exc:
            print(f"[ILUS] Cloudinary upload error: {exc}")
            flash("Error al subir la foto a la nube. Intenta nuevamente.", "danger")
            return redirect(url_for("product_detail", pid=pid))
    else:
        filename = f"p{pid}_{ts}.{ext}"
        file.save(os.path.join(UPLOAD_FOLDER, filename))

    conn = get_db()
    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO `{PHOTOS_TABLE}` (product_id,filename,orden) VALUES (%s,%s,%s)",
            (pid, filename, len(photos) + 1),
        )
    conn.commit()

    flash("Foto agregada.", "success")
    return redirect(url_for("product_detail", pid=pid))


@app.route("/products/<int:pid>/photos/<int:photo_id>/delete", methods=["POST"])
@require_permission("delete")
def delete_photo(pid, photo_id):
    product, _, _ = get_full(pid)
    if not product:
        flash("Producto no encontrado.", "danger")
        return redirect(url_for("index"))

    # Verificar confirmación por SKU
    confirm = request.form.get("confirm_sku", "").strip().upper()
    if confirm != product["sku"]:
        flash("Confirmación incorrecta. La foto NO fue eliminada.", "danger")
        return redirect(url_for("product_detail", pid=pid))

    photo = mysql_fetchone(
        f"SELECT * FROM `{PHOTOS_TABLE}` WHERE id=%s AND product_id=%s", (photo_id, pid)
    )
    if photo:
        delete_photo_file(photo["filename"])
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM `{PHOTOS_TABLE}` WHERE id=%s", (photo_id,))
        conn.commit()
        flash("Foto eliminada.", "success")
    else:
        flash("Foto no encontrada.", "danger")
    return redirect(url_for("product_detail", pid=pid))


# ─────────────────────────────────────────────
#  Impresión — HTML
# ─────────────────────────────────────────────

@app.route("/products/<int:pid>/labels")
@require_permission("print")
def print_labels(pid):
    only = request.args.get("bulto", type=int)
    product, bultos, _ = get_full(pid)
    if not product:
        flash("Producto no encontrado.", "danger")
        return redirect(url_for("index"))

    valid = [b for b in bultos if float(b["largo"]) > 0 and float(b["ancho"]) > 0 and float(b["alto"]) > 0]
    if not valid:
        flash("Ningun bulto tiene medidas completas.", "danger")
        return redirect(url_for("product_detail", pid=pid))
    if only:
        valid = [b for b in valid if int(b["bulto_num"]) == only]

    # Cantidades por bulto (vienen del formulario de print_setup)
    qty_per_bulto = {}
    for b in valid:
        qty = request.args.get(f"qty_{b['bulto_num']}", 1, type=int)
        qty_per_bulto[int(b["bulto_num"])] = max(1, min(qty, 10))

    fecha = datetime.now().strftime("%d-%m-%Y %H:%M")
    return render_template("labels.html", product=product, bultos=valid,
                           total_bultos=len(bultos), fecha=fecha,
                           qty_per_bulto=qty_per_bulto)


@app.route("/products/<int:pid>/print-setup")
@require_permission("print")
def print_setup(pid):
    """Pantalla de configuración de impresión: copias por bulto."""
    product, bultos, _ = get_full(pid)
    if not product:
        flash("Producto no encontrado.", "danger")
        return redirect(url_for("index"))

    valid = [b for b in bultos if float(b["largo"]) > 0 and float(b["ancho"]) > 0 and float(b["alto"]) > 0]
    if not valid:
        flash("Ningun bulto tiene medidas completas.", "danger")
        return redirect(url_for("product_detail", pid=pid))

    # Si viene un bulto específico, filtrar
    only = request.args.get("bulto", type=int)
    if only:
        valid = [b for b in valid if int(b["bulto_num"]) == only]

    peso_total = round(sum(float(b["peso"]) for b in valid), 2)
    return render_template("print_setup.html", product=product, bultos=valid,
                           total_bultos=len(bultos), peso_total=peso_total,
                           only_bulto=only)


@app.route("/products/<int:pid>/download-all-pdf")
@require_permission("print")
def download_all_pdf(pid):
    """Descarga todos los bultos seleccionados como un ZIP de PDFs."""
    import zipfile
    product, bultos, _ = get_full(pid)
    if not product:
        return "Producto no encontrado", 404

    valid = [b for b in bultos if float(b["largo"]) > 0 and float(b["ancho"]) > 0 and float(b["alto"]) > 0]
    only = request.args.get("bulto", type=int)
    if only:
        valid = [b for b in valid if int(b["bulto_num"]) == only]

    if not valid:
        return "Sin bultos con medidas completas", 404

    zip_buf = io.BytesIO()
    with zipfile.ZipFile(zip_buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for b in valid:
            qty = request.args.get(f"qty_{b['bulto_num']}", 1, type=int)
            qty = max(1, min(qty, 10))
            for copy_n in range(1, qty + 1):
                try:
                    pdf_bytes = build_label_pdf(product, b, len(bultos))
                    fname = f"ILUS_{product['sku']}_B{int(b['bulto_num']):02d}_C{copy_n}.pdf"
                    zf.writestr(fname, pdf_bytes)
                except Exception:
                    pass

    zip_buf.seek(0)
    fecha = datetime.now().strftime("%Y%m%d_%H%M")
    zip_name = f"ILUS_{product['sku']}_{fecha}.zip"
    return Response(
        zip_buf.read(),
        mimetype="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{zip_name}"'},
    )


# ─────────────────────────────────────────────
#  Impresión — PDF (150 × 50 mm)
# ─────────────────────────────────────────────

@app.route("/products/<int:pid>/bulto/<int:bnum>/pdf")
@require_permission("print")
def download_pdf(pid, bnum):
    product, bultos, _ = get_full(pid)
    if not product:
        return "Producto no encontrado", 404
    bulto = next((b for b in bultos if int(b["bulto_num"]) == bnum), None)
    if not bulto or float(bulto["largo"]) == 0:
        return "Bulto sin medidas completas", 404

    try:
        pdf_bytes = build_label_pdf(product, bulto, len(bultos))
    except Exception as exc:
        return f"Error generando PDF: {exc}", 500

    filename = f"ILUS_{product['sku']}_B{bnum:02d}.pdf"
    return Response(
        pdf_bytes,
        mimetype="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ─────────────────────────────────────────────
#  Exportar Excel
# ─────────────────────────────────────────────

@app.route("/products/export/excel")
@login_required
def export_excel():
    try:
        import openpyxl
        from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    except ImportError:
        flash("Instala openpyxl: pip install openpyxl", "danger")
        return redirect(url_for("index"))

    # ── Datos ───────────────────────────────────────────────────────────
    products = mysql_fetchall(
        f"""SELECT p.id, p.sku, p.nombre, p.estado, p.codigo,
                   p.created_by, p.updated_by, p.created_at, p.updated_at
            FROM `{PRODUCTS_TABLE}` p ORDER BY p.sku"""
    )
    bultos = mysql_fetchall(
        f"""SELECT b.product_id, b.bulto_num, b.largo, b.ancho, b.alto, b.peso,
                   ROUND((b.largo*b.ancho*b.alto)/4000, 2) AS peso_vol
            FROM `{BULTOS_TABLE}` b ORDER BY b.product_id, b.bulto_num"""
    )
    # índice de bultos por product_id
    bultos_by_pid = {}
    for b in bultos:
        bultos_by_pid.setdefault(int(b["product_id"]), []).append(b)

    wb = openpyxl.Workbook()

    # ── Estilos ─────────────────────────────────────────────────────────
    RED     = "CC0000"
    BLACK   = "111111"
    WHITE   = "FFFFFF"
    LGRAY   = "F5F5F5"
    hdr_font  = Font(name="Calibri", bold=True, color=WHITE, size=11)
    hdr_fill  = PatternFill("solid", fgColor=BLACK)
    red_fill  = PatternFill("solid", fgColor=RED)
    alt_fill  = PatternFill("solid", fgColor=LGRAY)
    center    = Alignment(horizontal="center", vertical="center")
    left      = Alignment(horizontal="left",   vertical="center", wrap_text=True)
    thin      = Side(style="thin", color="DDDDDD")
    border    = Border(left=thin, right=thin, top=thin, bottom=thin)

    def style_hdr(cell, red=False):
        cell.font      = hdr_font
        cell.fill      = red_fill if red else hdr_fill
        cell.alignment = center
        cell.border    = border

    def style_cell(cell, align=None, bold=False, color=None):
        cell.font      = Font(name="Calibri", bold=bold, color=color or "000000", size=10)
        cell.alignment = align or left
        cell.border    = border

    # ══════════════════════════════════════════
    #  Hoja 1 — Resumen productos
    # ══════════════════════════════════════════
    ws1 = wb.active
    ws1.title = "Productos"
    ws1.freeze_panes = "A2"

    hdrs1 = ["SKU", "Nombre", "Estado", "Código", "Bultos",
             "Peso Total (kg)", "Peso Vol Total", "Creado por", "Actualizado por"]
    col_w1 = [16, 45, 14, 10, 8, 16, 15, 22, 22]
    for i, (h, w) in enumerate(zip(hdrs1, col_w1), 1):
        c = ws1.cell(row=1, column=i, value=h)
        style_hdr(c, red=(i <= 1))
        ws1.column_dimensions[c.column_letter].width = w
    ws1.row_dimensions[1].height = 20

    for row_i, p in enumerate(products, 2):
        blist = bultos_by_pid.get(int(p["id"]), [])
        ptotal = sum(float(b["peso"])     for b in blist)
        pvtot  = sum(float(b["peso_vol"]) for b in blist)
        fill = alt_fill if row_i % 2 == 0 else PatternFill()
        vals = [p["sku"], p["nombre"], p["estado"], p["codigo"],
                len(blist), round(ptotal, 2), round(pvtot, 2),
                p["created_by"] or "", p["updated_by"] or ""]
        for col_i, v in enumerate(vals, 1):
            c = ws1.cell(row=row_i, column=col_i, value=v)
            c.fill   = fill
            c.border = border
            c.font   = Font(name="Calibri", size=10,
                            bold=(col_i == 1),
                            color=RED if col_i == 1 else "000000")
            c.alignment = center if col_i in (1, 3, 4, 5, 6, 7) else left
        ws1.row_dimensions[row_i].height = 16

    # ══════════════════════════════════════════
    #  Hoja 2 — Detalle bultos
    # ══════════════════════════════════════════
    ws2 = wb.create_sheet("Bultos")
    ws2.freeze_panes = "A2"

    hdrs2 = ["SKU", "Nombre", "Código", "Bulto N°",
             "Largo (cm)", "Ancho (cm)", "Alto (cm)", "Peso (kg)", "Peso Vol"]
    col_w2 = [16, 45, 10, 9, 11, 11, 11, 11, 11]
    for i, (h, w) in enumerate(zip(hdrs2, col_w2), 1):
        c = ws2.cell(row=1, column=i, value=h)
        style_hdr(c, red=(i <= 1))
        ws2.column_dimensions[c.column_letter].width = w
    ws2.row_dimensions[1].height = 20

    row_i = 2
    for p in products:
        blist = bultos_by_pid.get(int(p["id"]), [])
        for b in blist:
            fill = alt_fill if row_i % 2 == 0 else PatternFill()
            vals = [p["sku"], p["nombre"], p["codigo"], int(b["bulto_num"]),
                    float(b["largo"]), float(b["ancho"]), float(b["alto"]),
                    float(b["peso"]), float(b["peso_vol"])]
            for col_i, v in enumerate(vals, 1):
                c = ws2.cell(row=row_i, column=col_i, value=v)
                c.fill   = fill
                c.border = border
                c.font   = Font(name="Calibri", size=10,
                                bold=(col_i == 1),
                                color=RED if col_i == 1 else "000000")
                c.alignment = center if col_i != 2 else left
            ws2.row_dimensions[row_i].height = 16
            row_i += 1

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    fecha = datetime.now().strftime("%Y%m%d_%H%M")
    filename = f"ILUS_Etiquetas_{fecha}.xlsx"
    return Response(
        buf.read(),
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ─────────────────────────────────────────────
#  Usuarios (solo admin)
# ─────────────────────────────────────────────

@app.route("/admin/users")
@require_permission("admin")
def users_index():
    users = mysql_fetchall(f"SELECT * FROM `{AUTH_TABLE}` ORDER BY nombre, username")
    return render_template("users.html", users=users)


@app.route("/admin/users/new", methods=["GET", "POST"])
@require_permission("admin")
def new_user():
    if request.method == "POST":
        username = request.form.get("username", "").strip().lower()
        nombre   = request.form.get("nombre",   "").strip()
        password = request.form.get("password", "")
        role     = request.form.get("role",     "editor")
        active   = 1 if request.form.get("active") == "1" else 0

        errors = []
        if not username:
            errors.append("El correo es requerido.")
        elif not EMAIL_RE.match(username):
            errors.append("El correo no tiene un formato válido.")
        if not nombre:
            errors.append("El nombre y apellido son requeridos.")
        if not password:
            errors.append("La clave es requerida.")
        if len(password) < 8:
            errors.append("La clave debe tener al menos 8 caracteres.")
        if role not in {"superadmin", "admin", "editor", "lector", "vendedor"}:
            errors.append("Rol no valido.")
        if get_auth_user_by_username(username):
            errors.append("Ese correo ya está registrado.")

        if errors:
            return render_template("user_form.html", errors=errors, user=None, fd=request.form)

        conn = get_db()
        with conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO `{AUTH_TABLE}` (username,nombre,password_hash,role,active) VALUES (%s,%s,%s,%s,%s)",
                (username, nombre, generate_password_hash(password), role, active),
            )
        conn.commit()

        flash("Usuario creado.", "success")
        return redirect(url_for("users_index"))

    return render_template("user_form.html", errors=[], user=None, fd={})


@app.route("/admin/users/<int:user_id>/edit", methods=["GET", "POST"])
@require_permission("admin")
def edit_user(user_id):
    user = get_auth_user_by_id(user_id)
    if not user:
        flash("Usuario no encontrado.", "danger")
        return redirect(url_for("users_index"))

    if request.method == "POST":
        username = request.form.get("username", "").strip().lower()
        nombre   = request.form.get("nombre",   "").strip()
        password = request.form.get("password", "")
        role     = request.form.get("role",     "editor")
        active   = 1 if request.form.get("active") == "1" else 0

        errors = []
        if not username:
            errors.append("El correo es requerido.")
        elif not EMAIL_RE.match(username):
            errors.append("El correo no tiene un formato válido.")
        if not nombre:
            errors.append("El nombre y apellido son requeridos.")
        if password and len(password) < 8:
            errors.append("La clave debe tener al menos 8 caracteres.")
        if role not in {"superadmin", "admin", "editor", "lector", "vendedor"}:
            errors.append("Rol no valido.")
        if mysql_fetchone(
            f"SELECT id FROM `{AUTH_TABLE}` WHERE username=%s AND id<>%s", (username, user_id)
        ):
            errors.append("Ese correo ya está en uso.")

        if errors:
            return render_template("user_form.html", errors=errors, user=user, fd=request.form)

        conn = get_db()
        with conn.cursor() as cur:
            if password:
                cur.execute(
                    f"UPDATE `{AUTH_TABLE}` SET username=%s,nombre=%s,password_hash=%s,role=%s,active=%s WHERE id=%s",
                    (username, nombre, generate_password_hash(password), role, active, user_id),
                )
            else:
                cur.execute(
                    f"UPDATE `{AUTH_TABLE}` SET username=%s,nombre=%s,role=%s,active=%s WHERE id=%s",
                    (username, nombre, role, active, user_id),
                )
        conn.commit()

        # Invalida caché de session si se editó el usuario actual
        if g.user and g.user["id"] == user_id:
            session.pop("_uc", None)

        flash("Usuario actualizado.", "success")
        return redirect(url_for("users_index"))

    return render_template("user_form.html", errors=[], user=user, fd={})


@app.route("/admin/users/<int:user_id>/delete", methods=["POST"])
@require_permission("admin")
def delete_user(user_id):
    if g.user and g.user["id"] == user_id:
        flash("No puedes eliminar tu propio usuario mientras estás conectado.", "danger")
        return redirect(url_for("users_index"))

    conn = get_db()
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM `{AUTH_TABLE}` WHERE id=%s", (user_id,))
    conn.commit()

    flash("Usuario eliminado.", "warning")
    return redirect(url_for("users_index"))


# ══════════════════════════════════════════════════════════════
#  MÓDULO: HRM — COLABORADORES
# ══════════════════════════════════════════════════════════════

HRM_AREAS_TABLE  = "hrm_areas"
HRM_CARGOS_TABLE = "hrm_cargos"
HRM_COLAB_TABLE  = "hrm_colaboradores"
PREG_GEN_TABLE   = "eval_preguntas_genericas"

CHILE_REGIONES = [
    "Arica y Parinacota",
    "Tarapacá",
    "Antofagasta",
    "Atacama",
    "Coquimbo",
    "Valparaíso",
    "Metropolitana de Santiago",
    "O'Higgins",
    "Maule",
    "Ñuble",
    "Biobío",
    "La Araucanía",
    "Los Ríos",
    "Los Lagos",
    "Aysén",
    "Magallanes",
]

RESETS_TABLE = "app_password_resets"

GENEROS = {
    "masculino":        "Masculino",
    "femenino":         "Femenino",
    "otro":             "Otro",
    "no_especificado":  "Prefiero no indicar",
}

ESTADOS_COLAB = {
    "activo":   "Activo",
    "inactivo": "Inactivo",
    "licencia": "Con licencia",
}


def init_hrm_tables():
    """Crea las tablas del módulo HRM si no existen."""
    os.makedirs(COLABS_FOLDER, exist_ok=True)
    conn = get_mysql()
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS `{HRM_AREAS_TABLE}` (
                    id          INT AUTO_INCREMENT PRIMARY KEY,
                    nombre      VARCHAR(120) NOT NULL UNIQUE,
                    descripcion TEXT,
                    created_at  DATETIME DEFAULT CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS `{HRM_CARGOS_TABLE}` (
                    id         INT AUTO_INCREMENT PRIMARY KEY,
                    nombre     VARCHAR(120) NOT NULL,
                    descriptor TEXT,
                    area_id    INT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT fk_cargo_area FOREIGN KEY (area_id)
                        REFERENCES `{HRM_AREAS_TABLE}`(id) ON DELETE SET NULL
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS `{HRM_COLAB_TABLE}` (
                    id              INT AUTO_INCREMENT PRIMARY KEY,
                    nombre_completo VARCHAR(200) NOT NULL,
                    rut             VARCHAR(20),
                    email           VARCHAR(190),
                    telefono        VARCHAR(30),
                    direccion       TEXT,
                    genero          ENUM('masculino','femenino','otro','no_especificado')
                                    DEFAULT 'no_especificado',
                    cargo_id        INT,
                    area_id         INT,
                    foto_filename   VARCHAR(255),
                    fecha_ingreso   DATE,
                    estado          ENUM('activo','inactivo','licencia') DEFAULT 'activo',
                    created_by      VARCHAR(190),
                    updated_by      VARCHAR(190),
                    created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at      DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    CONSTRAINT fk_colab_cargo FOREIGN KEY (cargo_id)
                        REFERENCES `{HRM_CARGOS_TABLE}`(id) ON DELETE SET NULL,
                    CONSTRAINT fk_colab_area FOREIGN KEY (area_id)
                        REFERENCES `{HRM_AREAS_TABLE}`(id) ON DELETE SET NULL
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS `{PREG_GEN_TABLE}` (
                    id             INT AUTO_INCREMENT PRIMARY KEY,
                    seccion        ENUM('tecnica','operativa','conductual','cumplimiento') NOT NULL,
                    texto          TEXT NOT NULL,
                    tipo_respuesta ENUM('escala_1_5','texto_libre','multiple','si_no','porcentaje')
                                   DEFAULT 'escala_1_5',
                    opciones       JSON,
                    es_obligatoria TINYINT(1) DEFAULT 1,
                    activa         TINYINT(1) DEFAULT 1,
                    created_by     VARCHAR(190),
                    updated_by     VARCHAR(190),
                    created_at     DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at     DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
        conn.commit()
        # ── Migración segura: agregar columnas región/comuna si no existen ──
        for col, definition in [
            ("region",  "VARCHAR(100)"),
            ("comuna",  "VARCHAR(100)"),
        ]:
            try:
                with conn.cursor() as cur:
                    cur.execute(f"""
                        ALTER TABLE `{HRM_COLAB_TABLE}`
                        ADD COLUMN IF NOT EXISTS `{col}` {definition}
                    """)
                conn.commit()
            except Exception:
                pass  # MySQL <8 no soporta IF NOT EXISTS en ALTER; ignorar si ya existe
    finally:
        conn.close()


def _get_or_create_area(nombre_raw, conn):
    """Devuelve area_id. Si no existe, la crea."""
    nombre = nombre_raw.strip()
    if not nombre:
        return None
    row = mysql_fetchone(f"SELECT id FROM `{HRM_AREAS_TABLE}` WHERE nombre=%s", (nombre,))
    if row:
        return row["id"]
    with conn.cursor() as cur:
        cur.execute(f"INSERT INTO `{HRM_AREAS_TABLE}` (nombre) VALUES (%s)", (nombre,))
        return cur.lastrowid


def _get_or_create_cargo(nombre_raw, descriptor_raw, area_id, conn):
    """Devuelve cargo_id. Si no existe, lo crea."""
    nombre = nombre_raw.strip()
    if not nombre:
        return None
    row = mysql_fetchone(
        f"SELECT id FROM `{HRM_CARGOS_TABLE}` WHERE nombre=%s AND (area_id=%s OR area_id IS NULL)",
        (nombre, area_id),
    )
    if row:
        return row["id"]
    desc = (descriptor_raw or "").strip() or None
    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO `{HRM_CARGOS_TABLE}` (nombre, descriptor, area_id) VALUES (%s,%s,%s)",
            (nombre, desc, area_id),
        )
        return cur.lastrowid


def _save_colab_foto(file, colab_id):
    """Sube la foto a Cloudinary (o disco local) y devuelve filename/URL, o None."""
    if not file or not file.filename:
        return None
    ext = file.filename.rsplit(".", 1)[-1].lower()
    if ext not in ALLOWED_EXT:
        return None
    if _CLD_READY:
        try:
            return _cloud_upload(file, public_id=f"colab_{colab_id}", folder="ilus/colabs")
        except Exception as exc:
            print(f"[ILUS] Cloudinary colab upload error: {exc}")
            return None
    else:
        fname = f"colab_{colab_id}.{ext}"
        file.save(os.path.join(COLABS_FOLDER, fname))
        return fname


# ── Listado colaboradores ─────────────────────────────────────

@app.route("/colaboradores/")
@login_required
def colab_index():
    if not g.permissions.get("hrm") and not g.permissions.get("view"):
        flash("Sin permisos.", "danger")
        return redirect(url_for("index"))
    rows = mysql_fetchall(f"""
        SELECT c.*,
               a.nombre AS area_nombre,
               ca.nombre AS cargo_nombre
        FROM `{HRM_COLAB_TABLE}` c
        LEFT JOIN `{HRM_AREAS_TABLE}` a  ON a.id  = c.area_id
        LEFT JOIN `{HRM_CARGOS_TABLE}` ca ON ca.id = c.cargo_id
        ORDER BY c.nombre_completo ASC
    """)
    return render_template("colaboradores/index.html",
                           colaboradores=rows, estados=ESTADOS_COLAB)


# ── Nueva ficha ───────────────────────────────────────────────

@app.route("/colaboradores/nuevo", methods=["GET", "POST"])
@require_permission("hrm")
def colab_nuevo():
    areas  = mysql_fetchall(f"SELECT * FROM `{HRM_AREAS_TABLE}` ORDER BY nombre")
    cargos = mysql_fetchall(f"SELECT c.*, a.nombre AS area_nombre FROM `{HRM_CARGOS_TABLE}` c LEFT JOIN `{HRM_AREAS_TABLE}` a ON a.id=c.area_id ORDER BY c.nombre")

    if request.method == "POST":
        nombre     = request.form.get("nombre_completo", "").strip()
        if not nombre:
            flash("El nombre completo es requerido.", "danger")
            return render_template("colaboradores/form.html",
                                   colab=None, fd=request.form,
                                   areas=areas, cargos=cargos,
                                   generos=GENEROS, estados=ESTADOS_COLAB,
                                   regiones=CHILE_REGIONES)

        conn = get_db()

        # Área: id existente O nombre libre → auto-crear
        area_id = request.form.get("area_id", "").strip()
        area_nueva = request.form.get("area_nueva", "").strip()
        if area_nueva:
            area_id = _get_or_create_area(area_nueva, conn)
        elif area_id:
            area_id = int(area_id)
        else:
            area_id = None

        # Cargo: id existente O nombre libre → auto-crear
        cargo_id = request.form.get("cargo_id", "").strip()
        cargo_nuevo  = request.form.get("cargo_nuevo", "").strip()
        cargo_desc   = request.form.get("cargo_descriptor", "").strip()
        if cargo_nuevo:
            cargo_id = _get_or_create_cargo(cargo_nuevo, cargo_desc, area_id, conn)
        elif cargo_id:
            cargo_id = int(cargo_id)
        else:
            cargo_id = None

        fecha_ing = request.form.get("fecha_ingreso") or None
        region_val = request.form.get("region", "").strip() or None
        comuna_val = request.form.get("comuna", "").strip() or None

        with conn.cursor() as cur:
            cur.execute(f"""
                INSERT INTO `{HRM_COLAB_TABLE}`
                    (nombre_completo, rut, email, telefono, direccion,
                     region, comuna, genero, cargo_id, area_id,
                     fecha_ingreso, estado, created_by)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                nombre,
                request.form.get("rut", "").strip() or None,
                request.form.get("email", "").strip() or None,
                request.form.get("telefono", "").strip() or None,
                request.form.get("direccion", "").strip() or None,
                region_val, comuna_val,
                request.form.get("genero", "no_especificado"),
                cargo_id, area_id, fecha_ing,
                request.form.get("estado", "activo"),
                current_username(),
            ))
            cid = cur.lastrowid

        # Foto (después de obtener el id)
        foto = request.files.get("foto")
        fname = _save_colab_foto(foto, cid)
        if fname:
            with conn.cursor() as cur:
                cur.execute(f"UPDATE `{HRM_COLAB_TABLE}` SET foto_filename=%s WHERE id=%s",
                            (fname, cid))
        conn.commit()

        flash(f"Colaborador {nombre} creado correctamente.", "success")
        return redirect(url_for("colab_ficha", cid=cid))

    return render_template("colaboradores/form.html",
                           colab=None, fd={},
                           areas=areas, cargos=cargos,
                           generos=GENEROS, estados=ESTADOS_COLAB,
                           regiones=CHILE_REGIONES)


# ── Ficha de colaborador ──────────────────────────────────────

@app.route("/colaboradores/<int:cid>/")
@login_required
def colab_ficha(cid):
    row = mysql_fetchone(f"""
        SELECT c.*,
               a.nombre  AS area_nombre,
               ca.nombre AS cargo_nombre,
               ca.descriptor AS cargo_descriptor
        FROM `{HRM_COLAB_TABLE}` c
        LEFT JOIN `{HRM_AREAS_TABLE}`  a  ON a.id  = c.area_id
        LEFT JOIN `{HRM_CARGOS_TABLE}` ca ON ca.id = c.cargo_id
        WHERE c.id=%s
    """, (cid,))
    if not row:
        flash("Colaborador no encontrado.", "danger")
        return redirect(url_for("colab_index"))
    return render_template("colaboradores/ficha.html",
                           c=row, generos=GENEROS, estados=ESTADOS_COLAB)


# ── Editar colaborador ────────────────────────────────────────

@app.route("/colaboradores/<int:cid>/editar", methods=["GET", "POST"])
@require_permission("hrm")
def colab_editar(cid):
    colab  = mysql_fetchone(f"SELECT * FROM `{HRM_COLAB_TABLE}` WHERE id=%s", (cid,))
    if not colab:
        flash("Colaborador no encontrado.", "danger")
        return redirect(url_for("colab_index"))
    areas  = mysql_fetchall(f"SELECT * FROM `{HRM_AREAS_TABLE}` ORDER BY nombre")
    cargos = mysql_fetchall(f"SELECT c.*, a.nombre AS area_nombre FROM `{HRM_CARGOS_TABLE}` c LEFT JOIN `{HRM_AREAS_TABLE}` a ON a.id=c.area_id ORDER BY c.nombre")

    if request.method == "POST":
        nombre = request.form.get("nombre_completo", "").strip()
        if not nombre:
            flash("El nombre es requerido.", "danger")
            return render_template("colaboradores/form.html",
                                   colab=colab, fd=request.form,
                                   areas=areas, cargos=cargos,
                                   generos=GENEROS, estados=ESTADOS_COLAB,
                                   regiones=CHILE_REGIONES)

        conn = get_db()

        area_id = request.form.get("area_id", "").strip()
        area_nueva = request.form.get("area_nueva", "").strip()
        if area_nueva:
            area_id = _get_or_create_area(area_nueva, conn)
        elif area_id:
            area_id = int(area_id)
        else:
            area_id = None

        cargo_id = request.form.get("cargo_id", "").strip()
        cargo_nuevo = request.form.get("cargo_nuevo", "").strip()
        cargo_desc  = request.form.get("cargo_descriptor", "").strip()
        if cargo_nuevo:
            cargo_id = _get_or_create_cargo(cargo_nuevo, cargo_desc, area_id, conn)
        elif cargo_id:
            cargo_id = int(cargo_id)
        else:
            cargo_id = None

        fecha_ing  = request.form.get("fecha_ingreso") or None
        region_val = request.form.get("region", "").strip() or None
        comuna_val = request.form.get("comuna", "").strip() or None

        with conn.cursor() as cur:
            cur.execute(f"""
                UPDATE `{HRM_COLAB_TABLE}` SET
                    nombre_completo=%s, rut=%s, email=%s, telefono=%s,
                    direccion=%s, region=%s, comuna=%s,
                    genero=%s, cargo_id=%s, area_id=%s,
                    fecha_ingreso=%s, estado=%s, updated_by=%s
                WHERE id=%s
            """, (
                nombre,
                request.form.get("rut", "").strip() or None,
                request.form.get("email", "").strip() or None,
                request.form.get("telefono", "").strip() or None,
                request.form.get("direccion", "").strip() or None,
                region_val, comuna_val,
                request.form.get("genero", "no_especificado"),
                cargo_id, area_id, fecha_ing,
                request.form.get("estado", "activo"),
                current_username(), cid,
            ))

        # Foto nueva (opcional)
        foto = request.files.get("foto")
        fname = _save_colab_foto(foto, cid)
        if fname:
            with conn.cursor() as cur:
                cur.execute(f"UPDATE `{HRM_COLAB_TABLE}` SET foto_filename=%s WHERE id=%s",
                            (fname, cid))
        conn.commit()

        flash("Ficha actualizada.", "success")
        return redirect(url_for("colab_ficha", cid=cid))

    return render_template("colaboradores/form.html",
                           colab=colab, fd={},
                           areas=areas, cargos=cargos,
                           generos=GENEROS, estados=ESTADOS_COLAB,
                           regiones=CHILE_REGIONES)


# ── Eliminar colaborador ──────────────────────────────────────

@app.route("/colaboradores/<int:cid>/eliminar", methods=["POST"])
@require_permission("delete")
def colab_eliminar(cid):
    colab = mysql_fetchone(f"SELECT foto_filename, nombre_completo FROM `{HRM_COLAB_TABLE}` WHERE id=%s", (cid,))
    if not colab:
        flash("Colaborador no encontrado.", "danger")
        return redirect(url_for("colab_index"))
    # Eliminar foto si existe
    if colab.get("foto_filename"):
        try:
            os.remove(os.path.join(COLABS_FOLDER, colab["foto_filename"]))
        except Exception:
            pass
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM `{HRM_COLAB_TABLE}` WHERE id=%s", (cid,))
    conn.commit()
    flash(f"Colaborador {colab['nombre_completo']} eliminado.", "warning")
    return redirect(url_for("colab_index"))


# ══════════════════════════════════════════════════════════════
#  MÓDULO: PREGUNTAS GENÉRICAS (solo superadmin)
# ══════════════════════════════════════════════════════════════

def _require_superadmin(view):
    @wraps(view)
    def wrapped(*a, **kw):
        if not g.user:
            flash("Inicia sesión para continuar.", "warning")
            return redirect(url_for("login", next=request.path))
        if not g.permissions.get("superadmin"):
            flash("Esta acción requiere permisos de Super Administrador.", "danger")
            return redirect(url_for("index"))
        return view(*a, **kw)
    return wrapped


@app.route("/admin/preguntas-genericas/")
@_require_superadmin
def preg_gen_index():
    rows = mysql_fetchall(f"""
        SELECT * FROM `{PREG_GEN_TABLE}`
        ORDER BY seccion, id ASC
    """)
    for r in rows:
        _parse_opciones(r)
    por_seccion = {s: [] for s in SECCIONES}
    for r in rows:
        sec = r.get("seccion", "tecnica")
        if sec in por_seccion:
            por_seccion[sec].append(r)
    return render_template("admin/preguntas_genericas.html",
                           por_seccion=por_seccion,
                           secciones=SECCIONES, tipos=TIPOS_RESPUESTA)


@app.route("/admin/preguntas-genericas/guardar", methods=["POST"])
@_require_superadmin
def preg_gen_guardar():
    pid_raw        = request.form.get("preg_id", "").strip()
    seccion        = request.form.get("seccion", "tecnica")
    texto          = request.form.get("texto", "").strip()
    tipo_respuesta = request.form.get("tipo_respuesta", "escala_1_5")
    es_obligatoria = 1 if request.form.get("es_obligatoria") else 0
    activa         = 1 if request.form.get("activa", "1") else 0
    opciones_raw   = request.form.get("opciones", "").strip()

    if not texto:
        return jsonify({"ok": False, "error": "El texto es requerido"}), 400
    if seccion not in SECCIONES:
        return jsonify({"ok": False, "error": "Sección inválida"}), 400

    opciones_json = None
    if tipo_respuesta == "multiple" and opciones_raw:
        items = [o.strip() for o in opciones_raw.split("\n") if o.strip()]
        opciones_json = json.dumps(items, ensure_ascii=False)

    conn = get_db()
    if pid_raw:
        # Editar
        with conn.cursor() as cur:
            cur.execute(f"""
                UPDATE `{PREG_GEN_TABLE}`
                SET seccion=%s, texto=%s, tipo_respuesta=%s,
                    opciones=%s, es_obligatoria=%s, activa=%s, updated_by=%s
                WHERE id=%s
            """, (seccion, texto, tipo_respuesta, opciones_json,
                  es_obligatoria, activa, current_username(), int(pid_raw)))
        conn.commit()
        p = mysql_fetchone(f"SELECT * FROM `{PREG_GEN_TABLE}` WHERE id=%s", (int(pid_raw),))
    else:
        # Crear
        with conn.cursor() as cur:
            cur.execute(f"""
                INSERT INTO `{PREG_GEN_TABLE}`
                    (seccion, texto, tipo_respuesta, opciones, es_obligatoria, activa, created_by)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
            """, (seccion, texto, tipo_respuesta, opciones_json,
                  es_obligatoria, activa, current_username()))
            pid_new = cur.lastrowid
        conn.commit()
        p = mysql_fetchone(f"SELECT * FROM `{PREG_GEN_TABLE}` WHERE id=%s", (pid_new,))

    _parse_opciones(p)
    return jsonify({"ok": True, "pregunta": dict(p)})


@app.route("/admin/preguntas-genericas/<int:pid>/toggle", methods=["POST"])
@_require_superadmin
def preg_gen_toggle(pid):
    """Activa / desactiva una pregunta genérica."""
    p = mysql_fetchone(f"SELECT id, activa FROM `{PREG_GEN_TABLE}` WHERE id=%s", (pid,))
    if not p:
        return jsonify({"ok": False, "error": "No encontrada"}), 404
    nuevo = 0 if p["activa"] else 1
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute(f"UPDATE `{PREG_GEN_TABLE}` SET activa=%s, updated_by=%s WHERE id=%s",
                    (nuevo, current_username(), pid))
    conn.commit()
    return jsonify({"ok": True, "activa": nuevo})


@app.route("/admin/preguntas-genericas/<int:pid>/eliminar", methods=["POST"])
@_require_superadmin
def preg_gen_eliminar(pid):
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM `{PREG_GEN_TABLE}` WHERE id=%s", (pid,))
    conn.commit()
    return jsonify({"ok": True})


# ══════════════════════════════════════════════════════════════
#  MÓDULO: GESTIÓN DE EVALUACIONES
# ══════════════════════════════════════════════════════════════

EVAL_TABLE = "eval_evaluaciones"
PREG_TABLE = "eval_preguntas"

SECCIONES = {
    "tecnica":      {"label": "Evaluación Técnica",         "color": "#1a4a8a", "icon": "bi-tools"},
    "operativa":    {"label": "Evaluación Operativa",        "color": "#1a7a1a", "icon": "bi-gear-fill"},
    "conductual":   {"label": "Evaluación Conductual",       "color": "#7a4a00", "icon": "bi-person-check-fill"},
    "cumplimiento": {"label": "Cumplimiento de Procesos",    "color": "#6a006a", "icon": "bi-clipboard2-check-fill"},
}

TIPOS_RESPUESTA = {
    "escala_1_5":  "Escala 1 – 5",
    "texto_libre": "Texto libre",
    "multiple":    "Selección múltiple",
    "si_no":       "Sí / No",
    "porcentaje":  "Porcentaje (0–100)",
}

TIPOS_EVAL = {
    "diagnostica": "Evaluación Diagnóstica",
    "periodica":   "Evaluación Periódica",
    "especial":    "Evaluación Especial",
}


def init_eval_tables():
    """Crea las tablas del módulo de evaluaciones si no existen."""
    conn = get_mysql()
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS `{EVAL_TABLE}` (
                    id          INT AUTO_INCREMENT PRIMARY KEY,
                    nombre      VARCHAR(200) NOT NULL,
                    descripcion TEXT,
                    tipo        ENUM('diagnostica','periodica','especial') DEFAULT 'diagnostica',
                    estado      ENUM('borrador','publicada','archivada')   DEFAULT 'borrador',
                    created_by  VARCHAR(190),
                    updated_by  VARCHAR(190),
                    created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at  DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS `{PREG_TABLE}` (
                    id             INT AUTO_INCREMENT PRIMARY KEY,
                    evaluacion_id  INT NOT NULL,
                    seccion        ENUM('tecnica','operativa','conductual','cumplimiento') NOT NULL,
                    orden          INT          DEFAULT 0,
                    texto          TEXT         NOT NULL,
                    tipo_respuesta ENUM('escala_1_5','texto_libre','multiple','si_no','porcentaje')
                                               DEFAULT 'escala_1_5',
                    opciones       JSON,
                    es_obligatoria TINYINT(1)   DEFAULT 1,
                    created_at     DATETIME     DEFAULT CURRENT_TIMESTAMP,
                    updated_at     DATETIME     DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    CONSTRAINT fk_preg_eval FOREIGN KEY (evaluacion_id)
                        REFERENCES `{EVAL_TABLE}`(id) ON DELETE CASCADE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
        conn.commit()
    finally:
        conn.close()


# ── helpers internos ──────────────────────────────────────────

def _parse_opciones(p):
    """Convierte el campo opciones JSON→list en cada fila de pregunta."""
    if p.get("opciones") and isinstance(p["opciones"], str):
        try:
            p["opciones"] = json.loads(p["opciones"])
        except Exception:
            p["opciones"] = []
    elif not p.get("opciones"):
        p["opciones"] = []
    return p


def _preguntas_por_seccion(eid):
    rows = mysql_fetchall(
        f"SELECT * FROM `{PREG_TABLE}` WHERE evaluacion_id=%s ORDER BY seccion, orden ASC",
        (eid,),
    )
    por_seccion = {s: [] for s in SECCIONES}
    for p in rows:
        _parse_opciones(p)
        sec = p.get("seccion", "tecnica")
        if sec in por_seccion:
            por_seccion[sec].append(p)
    return por_seccion


# ── Listado ───────────────────────────────────────────────────

@app.route("/evaluaciones/")
@login_required
def eval_index():
    evals = mysql_fetchall(f"""
        SELECT e.*, COUNT(p.id) AS total_preguntas
        FROM `{EVAL_TABLE}` e
        LEFT JOIN `{PREG_TABLE}` p ON p.evaluacion_id = e.id
        GROUP BY e.id
        ORDER BY e.created_at DESC
    """)
    return render_template("evaluaciones/index.html",
                           evals=evals, tipos=TIPOS_EVAL)


# ── Crear evaluación ──────────────────────────────────────────

@app.route("/evaluaciones/nueva", methods=["GET", "POST"])
@require_permission("edit")
def eval_nueva():
    if request.method == "POST":
        nombre = request.form.get("nombre", "").strip()
        desc   = request.form.get("descripcion", "").strip()
        tipo   = request.form.get("tipo", "diagnostica")
        if not nombre:
            flash("El nombre es requerido.", "danger")
            return render_template("evaluaciones/form.html",
                                   ev=None, fd=request.form, tipos=TIPOS_EVAL)
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute(f"""
                INSERT INTO `{EVAL_TABLE}` (nombre, descripcion, tipo, estado, created_by)
                VALUES (%s, %s, %s, 'borrador', %s)
            """, (nombre, desc, tipo, current_username()))
            eid = cur.lastrowid
        conn.commit()
        flash("Evaluación creada. Ahora agrega las preguntas.", "success")
        return redirect(url_for("eval_constructor", eid=eid))
    return render_template("evaluaciones/form.html",
                           ev=None, fd={}, tipos=TIPOS_EVAL)


# ── Editar metadatos ──────────────────────────────────────────

@app.route("/evaluaciones/<int:eid>/editar", methods=["GET", "POST"])
@require_permission("edit")
def eval_editar(eid):
    ev = mysql_fetchone(f"SELECT * FROM `{EVAL_TABLE}` WHERE id=%s", (eid,))
    if not ev:
        flash("Evaluación no encontrada.", "danger")
        return redirect(url_for("eval_index"))
    if request.method == "POST":
        nombre = request.form.get("nombre", "").strip()
        desc   = request.form.get("descripcion", "").strip()
        tipo   = request.form.get("tipo", "diagnostica")
        if not nombre:
            flash("El nombre es requerido.", "danger")
            return render_template("evaluaciones/form.html",
                                   ev=ev, fd=request.form, tipos=TIPOS_EVAL)
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute(f"""
                UPDATE `{EVAL_TABLE}`
                SET nombre=%s, descripcion=%s, tipo=%s, updated_by=%s
                WHERE id=%s
            """, (nombre, desc, tipo, current_username(), eid))
        conn.commit()
        flash("Evaluación actualizada.", "success")
        return redirect(url_for("eval_constructor", eid=eid))
    return render_template("evaluaciones/form.html",
                           ev=ev, fd={}, tipos=TIPOS_EVAL)


# ── Constructor de preguntas ──────────────────────────────────

@app.route("/evaluaciones/<int:eid>/constructor")
@login_required
def eval_constructor(eid):
    ev = mysql_fetchone(f"SELECT * FROM `{EVAL_TABLE}` WHERE id=%s", (eid,))
    if not ev:
        flash("Evaluación no encontrada.", "danger")
        return redirect(url_for("eval_index"))
    por_seccion = _preguntas_por_seccion(eid)
    total = sum(len(v) for v in por_seccion.values())
    return render_template("evaluaciones/constructor.html",
                           ev=ev, por_seccion=por_seccion,
                           secciones=SECCIONES, tipos=TIPOS_RESPUESTA,
                           tipos_eval=TIPOS_EVAL, total_preguntas=total)


# ── API: Agregar pregunta ─────────────────────────────────────

@app.route("/api/evaluaciones/<int:eid>/preguntas", methods=["POST"])
@require_permission("edit")
def api_agregar_pregunta(eid):
    ev = mysql_fetchone(f"SELECT id, estado FROM `{EVAL_TABLE}` WHERE id=%s", (eid,))
    if not ev:
        return jsonify({"ok": False, "error": "Evaluación no encontrada"}), 404
    if ev["estado"] == "archivada":
        return jsonify({"ok": False, "error": "Evaluación archivada, no se puede modificar"}), 403

    seccion        = request.form.get("seccion", "tecnica")
    texto          = request.form.get("texto", "").strip()
    tipo_respuesta = request.form.get("tipo_respuesta", "escala_1_5")
    es_obligatoria = 1 if request.form.get("es_obligatoria") else 0
    opciones_raw   = request.form.get("opciones", "").strip()

    if not texto:
        return jsonify({"ok": False, "error": "El texto de la pregunta es requerido"}), 400
    if seccion not in SECCIONES:
        return jsonify({"ok": False, "error": "Sección inválida"}), 400

    max_ord = mysql_fetchone(
        f"SELECT COALESCE(MAX(orden),0) AS mo FROM `{PREG_TABLE}` WHERE evaluacion_id=%s AND seccion=%s",
        (eid, seccion),
    )
    nuevo_orden = int(max_ord["mo"]) + 1

    opciones_json = None
    if tipo_respuesta == "multiple" and opciones_raw:
        items = [o.strip() for o in opciones_raw.split("\n") if o.strip()]
        opciones_json = json.dumps(items, ensure_ascii=False)

    conn = get_db()
    with conn.cursor() as cur:
        cur.execute(f"""
            INSERT INTO `{PREG_TABLE}`
                (evaluacion_id, seccion, orden, texto, tipo_respuesta, opciones, es_obligatoria)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (eid, seccion, nuevo_orden, texto, tipo_respuesta, opciones_json, es_obligatoria))
        pid = cur.lastrowid
    conn.commit()

    p = mysql_fetchone(f"SELECT * FROM `{PREG_TABLE}` WHERE id=%s", (pid,))
    return jsonify({"ok": True, "pregunta": dict(_parse_opciones(p))})


# ── API: Editar pregunta ──────────────────────────────────────

@app.route("/api/preguntas/<int:pid>", methods=["POST"])
@require_permission("edit")
def api_editar_pregunta(pid):
    p = mysql_fetchone(f"""
        SELECT p.*, e.estado FROM `{PREG_TABLE}` p
        JOIN `{EVAL_TABLE}` e ON e.id = p.evaluacion_id
        WHERE p.id=%s
    """, (pid,))
    if not p:
        return jsonify({"ok": False, "error": "Pregunta no encontrada"}), 404
    if p["estado"] == "archivada":
        return jsonify({"ok": False, "error": "Evaluación archivada, no editable"}), 403

    texto          = request.form.get("texto", "").strip()
    tipo_respuesta = request.form.get("tipo_respuesta", p["tipo_respuesta"])
    es_obligatoria = 1 if request.form.get("es_obligatoria") else 0
    opciones_raw   = request.form.get("opciones", "").strip()

    if not texto:
        return jsonify({"ok": False, "error": "El texto es requerido"}), 400

    opciones_json = None
    if tipo_respuesta == "multiple" and opciones_raw:
        items = [o.strip() for o in opciones_raw.split("\n") if o.strip()]
        opciones_json = json.dumps(items, ensure_ascii=False)

    conn = get_db()
    with conn.cursor() as cur:
        cur.execute(f"""
            UPDATE `{PREG_TABLE}`
            SET texto=%s, tipo_respuesta=%s, opciones=%s, es_obligatoria=%s
            WHERE id=%s
        """, (texto, tipo_respuesta, opciones_json, es_obligatoria, pid))
    conn.commit()

    updated = mysql_fetchone(f"SELECT * FROM `{PREG_TABLE}` WHERE id=%s", (pid,))
    return jsonify({"ok": True, "pregunta": dict(_parse_opciones(updated))})


# ── API: Eliminar pregunta ────────────────────────────────────

@app.route("/api/preguntas/<int:pid>/eliminar", methods=["POST"])
@require_permission("edit")
def api_eliminar_pregunta(pid):
    p = mysql_fetchone(f"""
        SELECT p.*, e.estado FROM `{PREG_TABLE}` p
        JOIN `{EVAL_TABLE}` e ON e.id = p.evaluacion_id
        WHERE p.id=%s
    """, (pid,))
    if not p:
        return jsonify({"ok": False, "error": "Pregunta no encontrada"}), 404
    if p["estado"] == "archivada":
        return jsonify({"ok": False, "error": "Evaluación archivada"}), 403

    eid     = p["evaluacion_id"]
    seccion = p["seccion"]

    conn = get_db()
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM `{PREG_TABLE}` WHERE id=%s", (pid,))
        # reindexar posiciones en la sección
        cur.execute(f"""
            SELECT id FROM `{PREG_TABLE}`
            WHERE evaluacion_id=%s AND seccion=%s
            ORDER BY orden ASC
        """, (eid, seccion))
        for i, r in enumerate(cur.fetchall(), 1):
            cur.execute(f"UPDATE `{PREG_TABLE}` SET orden=%s WHERE id=%s", (i, r["id"]))
    conn.commit()
    return jsonify({"ok": True})


# ── API: Mover pregunta ↑ ↓ ──────────────────────────────────

@app.route("/api/preguntas/<int:pid>/mover", methods=["POST"])
@require_permission("edit")
def api_mover_pregunta(pid):
    direccion = request.form.get("direccion")   # "up" | "down"
    p = mysql_fetchone(f"SELECT * FROM `{PREG_TABLE}` WHERE id=%s", (pid,))
    if not p:
        return jsonify({"ok": False, "error": "No encontrada"}), 404

    eid, seccion, orden = p["evaluacion_id"], p["seccion"], p["orden"]

    if direccion == "up":
        otra = mysql_fetchone(f"""
            SELECT id, orden FROM `{PREG_TABLE}`
            WHERE evaluacion_id=%s AND seccion=%s AND orden<%s
            ORDER BY orden DESC LIMIT 1
        """, (eid, seccion, orden))
    else:
        otra = mysql_fetchone(f"""
            SELECT id, orden FROM `{PREG_TABLE}`
            WHERE evaluacion_id=%s AND seccion=%s AND orden>%s
            ORDER BY orden ASC LIMIT 1
        """, (eid, seccion, orden))

    if not otra:
        return jsonify({"ok": True, "moved": False})   # ya en el límite

    conn = get_db()
    with conn.cursor() as cur:
        cur.execute(f"UPDATE `{PREG_TABLE}` SET orden=%s WHERE id=%s", (otra["orden"], pid))
        cur.execute(f"UPDATE `{PREG_TABLE}` SET orden=%s WHERE id=%s", (orden, otra["id"]))
    conn.commit()
    return jsonify({"ok": True, "moved": True, "swapped_with": otra["id"]})


# ── API: Reordenar por drag-and-drop ─────────────────────────

@app.route("/api/evaluaciones/<int:eid>/reordenar", methods=["POST"])
@require_permission("edit")
def api_reordenar(eid):
    """Body JSON: {"seccion": "tecnica", "orden": [id1, id2, ...]}"""
    data    = request.get_json(force=True) or {}
    seccion = data.get("seccion", "")
    orden   = data.get("orden", [])

    if seccion not in SECCIONES:
        return jsonify({"ok": False, "error": "Sección inválida"}), 400

    conn = get_db()
    with conn.cursor() as cur:
        for i, preg_id in enumerate(orden, 1):
            cur.execute(f"""
                UPDATE `{PREG_TABLE}` SET orden=%s
                WHERE id=%s AND evaluacion_id=%s AND seccion=%s
            """, (i, preg_id, eid, seccion))
    conn.commit()
    return jsonify({"ok": True})


# ── API: Cambiar estado evaluación ───────────────────────────

@app.route("/api/evaluaciones/<int:eid>/estado", methods=["POST"])
@require_permission("edit")
def api_eval_estado(eid):
    ev = mysql_fetchone(f"SELECT * FROM `{EVAL_TABLE}` WHERE id=%s", (eid,))
    if not ev:
        return jsonify({"ok": False, "error": "No encontrada"}), 404

    nuevo = request.form.get("estado", "")
    transiciones = {
        "borrador":  ["publicada"],
        "publicada": ["borrador", "archivada"],
        "archivada": [],
    }
    if nuevo not in transiciones.get(ev["estado"], []):
        return jsonify({"ok": False,
                        "error": f"Transición inválida: {ev['estado']} → {nuevo}"}), 400

    if nuevo == "publicada":
        total = mysql_fetchone(
            f"SELECT COUNT(*) AS c FROM `{PREG_TABLE}` WHERE evaluacion_id=%s", (eid,)
        )
        if int(total["c"]) == 0:
            return jsonify({"ok": False,
                            "error": "Agrega al menos una pregunta antes de publicar"}), 400

    conn = get_db()
    with conn.cursor() as cur:
        cur.execute(f"""
            UPDATE `{EVAL_TABLE}` SET estado=%s, updated_by=%s WHERE id=%s
        """, (nuevo, current_username(), eid))
    conn.commit()
    return jsonify({"ok": True, "estado": nuevo})


# ═══════════════════════════════════════════════════════════════
#  MÓDULO CUBICADOR
#  Busca documentos de venta en el ERP Random y cruza con
#  los datos de peso/volumen de la base de etiquetas.
#  Solo visible para rol: vendedor, admin, superadmin
# ═══════════════════════════════════════════════════════════════

TIPOS_DOC_CUBICADOR = [
    ("FCV", "Factura"),
    ("BLV", "Boleta"),
    ("NVV", "Nota de Venta"),
    ("COV", "Cotización"),
]


def _nudo_variants(nudo_raw):
    """
    El ERP guarda NUDO como string de 10 chars con ceros a la izquierda.
    Devuelve lista de variantes a probar: ['0000009344', '9344', ...]
    """
    s = str(nudo_raw).strip()
    padded = s.zfill(10)
    return list(dict.fromkeys([padded, s]))   # únicos, preservando orden


def _cubicador_fetch(tido, nudo):
    """
    Consulta el ERP externo y cruza con nuestra BD.
    Retorna (header_dict, lineas_list) o lanza excepción.
    """
    erp = get_erp_conn()
    if not erp:
        raise ConnectionError("No se pudo conectar al ERP. Intenta en unos momentos.")

    nudos = _nudo_variants(nudo)

    try:
        with erp.cursor() as cur:
            # ── Encabezado: probar variantes de NUDO ─────────────────
            header = None
            nudo_match = None
            for nv in nudos:
                cur.execute("""
                    SELECT
                        e.TIDO,
                        TRIM(e.NUDO)   AS nudo,
                        e.FEEMDO       AS fecha,
                        e.VANEDO       AS valor_neto,
                        e.VABRDO       AS valor_bruto,
                        e.VAIVDO       AS valor_iva,
                        c.NOKOEN       AS cliente_nombre,
                        c.RTEN         AS cliente_rut
                    FROM MAEEDO e
                    LEFT JOIN MAEEN c ON c.KOEN = e.ENDO
                    WHERE e.TIDO = %s AND e.NUDO = %s
                    LIMIT 1
                """, (tido, nv))
                row = cur.fetchone()
                if row:
                    header = row
                    nudo_match = nv
                    break

            if not header:
                return None, []

            # ── Líneas del documento (solo líneas de producto real) ───
            cur.execute("""
                SELECT
                    TRIM(d.KOPRCT)  AS sku,
                    TRIM(d.NOKOPR)  AS descripcion_erp,
                    d.CAPRCO1       AS cantidad,
                    d.NULIDO        AS num_linea
                FROM MAEDDO d
                WHERE d.TIDO = %s AND d.NUDO = %s
                  AND TRIM(COALESCE(d.KOPRCT, '')) != ''
                ORDER BY d.NULIDO
            """, (tido, nudo_match))
            lineas_erp = cur.fetchall()
    finally:
        erp.close()

    # ── Cruzar con BD de etiquetas ────────────────────────────────────
    lineas = []
    for l in lineas_erp:
        sku          = (l.get("sku") or "").strip().upper()
        descripcion  = (l.get("descripcion_erp") or "").strip()
        qty          = float(l.get("cantidad") or 0)

        app_data = mysql_fetchone(f"""
            SELECT
                p.id     AS app_id,
                p.nombre AS nombre_app,
                COUNT(DISTINCT b.id)                                    AS total_bultos,
                COALESCE(SUM(b.peso), 0)                                AS peso_total,
                COALESCE(SUM(b.largo * b.ancho * b.alto), 0)            AS volumen_cm3,
                ROUND(COALESCE(SUM(b.largo * b.ancho * b.alto) / 4000.0, 0), 4)
                                                                        AS peso_vol
            FROM `{PRODUCTS_TABLE}` p
            LEFT JOIN `{BULTOS_TABLE}` b ON b.product_id = p.id
            WHERE UPPER(TRIM(p.sku)) = %s
            GROUP BY p.id, p.nombre
        """, (sku,))

        tiene_ficha   = app_data is not None
        total_bultos  = int(app_data["total_bultos"] if tiene_ficha else 0)
        tiene_bultos  = tiene_ficha and float(app_data.get("volumen_cm3") or 0) > 0

        peso_kg_u  = float(app_data["peso_total"]  if tiene_ficha else 0)
        peso_vol_u = float(app_data["peso_vol"]     if tiene_ficha else 0)
        vol_u      = float(app_data["volumen_cm3"]  if tiene_ficha else 0)
        pred_u     = max(peso_kg_u, peso_vol_u)

        nombre_app = (app_data["nombre_app"] if tiene_ficha else "") or ""
        diferencia = tiene_ficha and nombre_app.strip().upper() != descripcion.upper()

        lineas.append({
            "sku":              sku,
            "descripcion_erp":  descripcion,
            "nombre_app":       nombre_app,
            "cantidad":         qty,
            "total_bultos":     total_bultos,
            "app_id":           app_data["app_id"] if tiene_ficha else None,
            "tiene_ficha":      tiene_ficha,
            "tiene_bultos":     tiene_bultos,
            # Por unidad
            "peso_kg_u":        round(peso_kg_u,  4),
            "peso_vol_u":       round(peso_vol_u, 4),
            "vol_u":            round(vol_u,      2),
            "pred_u":           round(pred_u,     4),
            # Totales (× cantidad)
            "peso_kg_tot":      round(peso_kg_u  * qty, 4),
            "peso_vol_tot":     round(peso_vol_u * qty, 4),
            "vol_tot":          round(vol_u      * qty, 2),
            "pred_tot":         round(pred_u     * qty, 4),
            # Flag
            "diferencia":       diferencia,
        })

    return header, lineas


@app.route("/cubicador", methods=["GET", "POST"])
@login_required
def cubicador():
    if not g.permissions.get("cubicador"):
        flash("No tienes acceso al módulo Cubicador.", "danger")
        return redirect(url_for("index"))

    tido      = (request.form.get("tido") or request.args.get("tido") or "FCV").strip().upper()
    nudo      = (request.form.get("nudo") or request.args.get("nudo") or "").strip()
    resultado = None
    error_msg = None

    # ── Parsear formato combinado "FCV 9344" o "FCV9344" ──────────
    import re as _re
    for _cod, _ in TIPOS_DOC_CUBICADOR:
        _m = _re.match(r'^' + _cod + r'\s*(\S+)$', nudo.upper())
        if _m:
            tido = _cod
            nudo = _m.group(1)
            break

    if request.method == "POST" and nudo:
        try:
            header, lineas = _cubicador_fetch(tido, nudo)
            if header is None:
                error_msg = f"No se encontró el documento {tido} N° {nudo} en el ERP."
            else:
                resultado = {"header": header, "lineas": lineas}
        except ConnectionError as ce:
            error_msg = str(ce)
        except Exception as ex:
            error_msg = f"Error al consultar el ERP: {ex}"

    return render_template("cubicador/index.html",
                           tipos_doc=TIPOS_DOC_CUBICADOR,
                           tido=tido, nudo=nudo,
                           resultado=resultado,
                           error_msg=error_msg)


@app.route("/cubicador/sync-nombre", methods=["POST"])
@login_required
def cubicador_sync_nombre():
    """Actualiza el nombre de un producto en nuestra BD con el nombre del ERP."""
    if not g.permissions.get("cubicador"):
        return jsonify({"error": "sin permiso"}), 403
    sku        = request.form.get("sku", "").strip().upper()
    nombre_erp = request.form.get("nombre_erp", "").strip()
    if not sku or not nombre_erp:
        return jsonify({"error": "datos incompletos"}), 400
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute(
            f"UPDATE `{PRODUCTS_TABLE}` SET nombre=%s WHERE UPPER(TRIM(sku))=%s",
            (nombre_erp, sku)
        )
    conn.commit()
    _invalidate_listing_cache()
    return jsonify({"ok": True, "sku": sku, "nombre": nombre_erp})


# ─────────────────────────────────────────────
#  Arranque — inicializar tablas al cargar módulo
#  (funciona con `python app.py` Y `flask run`)
# ─────────────────────────────────────────────

try:
    init_db()
    print("[ILUS] Tablas inicializadas correctamente.")
except Exception as _init_err:
    print(f"[ILUS][WARN] init_db: {_init_err}")

if __name__ == "__main__":
    print("=" * 45)
    print("  ILUS - Sistema de Etiquetas")
    print("  http://localhost:5000")
    print("=" * 45)
    port = int(os.environ.get("PORT", 5000))
    debug = os.environ.get("FLASK_ENV") != "production"
    app.run(debug=debug, host="0.0.0.0", port=port)

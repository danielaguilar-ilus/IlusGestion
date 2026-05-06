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

from flask import (Flask, Response, flash, g, jsonify, make_response, redirect,
                   render_template, request, session, url_for)
from werkzeug.security import check_password_hash, generate_password_hash
from werkzeug.utils import secure_filename

from config import MAX_BULTOS, MYSQL_CONFIG, ERP_CONFIG, EMAIL_CONFIG, CLOUDINARY_CONFIG
try:
    from config import ANTHROPIC_API_KEY as _ANTHROPIC_KEY_CFG
except ImportError:
    _ANTHROPIC_KEY_CFG = ""

def _get_ai_key():
    """Resuelve la API key de Anthropic: env var > config.py"""
    return os.environ.get("ANTHROPIC_API_KEY") or _ANTHROPIC_KEY_CFG or ""

app = Flask(__name__)
app.secret_key = "ilus-etiquetas-2026"

# ══════════════════════════════════════════════════════════════
#  PLAYWRIGHT BROWSER POOL — instancia única reutilizada
#  Evita el overhead de launch/close (~700ms) por cada PDF.
#  Thread-safe: un Lock protege el acceso concurrente.
# ══════════════════════════════════════════════════════════════
_pw_lock     = threading.Lock()
_pw_ctx      = None   # sync_playwright() context manager
_pw_browser  = None   # Browser instance reutilizado


def _pw_browser_get():
    """Devuelve el browser compartido; lo lanza si aún no existe o murió."""
    global _pw_ctx, _pw_browser
    with _pw_lock:
        # Verificar si el browser sigue vivo
        if _pw_browser is not None:
            try:
                _ = _pw_browser.contexts  # ping liviano
                return _pw_browser
            except Exception:
                # Murió — limpiar
                try:
                    _pw_ctx.__exit__(None, None, None)
                except Exception:
                    pass
                _pw_ctx = _pw_browser = None

        # Lanzar nuevo browser
        from playwright.sync_api import sync_playwright
        _pw_ctx     = sync_playwright()
        pw          = _pw_ctx.__enter__()
        _pw_browser = pw.chromium.launch(
            args=["--no-sandbox", "--disable-dev-shm-usage",
                  "--disable-gpu", "--disable-extensions"]
        )
        return _pw_browser


def _pw_pdf(html: str, *, width: str = None, height: str = None,
            page_format: str = None, margin: dict = None,
            wait_fn: str = None, wait_timeout: int = 5000) -> bytes:
    """
    Genera PDF con el browser pool compartido.
    - width/height → tamaño personalizado (etiquetas)
    - page_format  → 'A4', 'Letter', etc.
    - wait_fn      → JS expression string para page.wait_for_function()
    - margin       → dict top/right/bottom/left en mm ('0mm')
    """
    browser = _pw_browser_get()
    page    = browser.new_page()
    try:
        page.set_content(html, wait_until="domcontentloaded")
        if wait_fn:
            try:
                page.wait_for_function(wait_fn, timeout=wait_timeout)
            except Exception:
                pass   # timeout: seguimos con lo que hay
        pdf_kw = dict(print_background=True)
        mrg = margin or {"top": "0mm", "right": "0mm",
                         "bottom": "0mm", "left": "0mm"}
        if width and height:
            pdf_kw.update(width=width, height=height, margin=mrg)
        elif page_format:
            pdf_kw.update(format=page_format, margin=mrg)
        return page.pdf(**pdf_kw)
    finally:
        page.close()

@app.template_filter('from_json')
def from_json_filter(value):
    """Parsea un string JSON almacenado en DB; devuelve lista/dict o []."""
    if not value:
        return []
    try:
        return json.loads(value)
    except Exception:
        return []

@app.template_filter('fkg')
def fkg_filter(value, decimals=3):
    """Formato kg con coma decimal para lectura chilena: 34.0 → '34,000'"""
    try:
        return f"{float(value):.{decimals}f}".replace('.', ',')
    except Exception:
        return '—'

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
                creator        = pymysql,
                mincached      = 1,           # conexiones siempre listas
                maxcached      = 5,           # máximo en pool inactivo
                maxconnections = 10,          # total permitido
                blocking       = False,       # falla rápido si no hay conexión libre
                ping           = 1,           # verifica conexión antes de entregar (reconecta si está muerta)
                host           = MYSQL_CONFIG["host"],
                port           = MYSQL_CONFIG["port"],
                user           = MYSQL_CONFIG["user"],
                password       = MYSQL_CONFIG["password"],
                database       = MYSQL_CONFIG["database"],
                connect_timeout= MYSQL_CONFIG.get("connect_timeout", 15),
                read_timeout   = 20,
                write_timeout  = 20,
                charset        = "utf8mb4",
                cursorclass    = pymysql.cursors.DictCursor,
                autocommit     = False,
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


def init_transporte_tables():
    """Crea tablas del módulo Transporte y Distribución."""
    conn = get_mysql()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS transport_commitments (
                    id              INT AUTO_INCREMENT PRIMARY KEY,
                    tido            VARCHAR(5)   NOT NULL,
                    nudo            VARCHAR(15)  NOT NULL,
                    endo            VARCHAR(20)  NOT NULL DEFAULT '',
                    fecha_emision   DATE,
                    fecha_entrega   DATE,
                    cliente_nombre  VARCHAR(200),
                    cliente_rut     VARCHAR(20),
                    comuna          VARCHAR(100),
                    direccion       VARCHAR(300),
                    telefono        VARCHAR(50),
                    email           VARCHAR(150),
                    valor_neto      DECIMAL(14,2) DEFAULT 0,
                    valor_bruto     DECIMAL(14,2) DEFAULT 0,
                    costo_zz        DECIMAL(10,2) DEFAULT 0,
                    tiene_saldo     TINYINT(1) DEFAULT 1,
                    guia_numero     VARCHAR(20),
                    estado          VARCHAR(50) DEFAULT 'Pendiente',
                    clasificacion   ENUM('despacho','retiro','instalacion','mantencion','garantia') DEFAULT 'despacho',
                    fecha_agenda    DATE,
                    notas           TEXT,
                    erp_synced_at   DATETIME,
                    created_by      VARCHAR(190),
                    updated_by      VARCHAR(190),
                    created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at      DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    UNIQUE KEY uq_doc (tido, nudo),
                    INDEX idx_saldo    (tiene_saldo),
                    INDEX idx_estado   (estado),
                    INDEX idx_fecha    (fecha_emision),
                    INDEX idx_clasif   (clasificacion)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            # Migrar columna clasificacion si ya existía con ENUM antiguo
            try:
                cur.execute("""ALTER TABLE transport_commitments
                    MODIFY COLUMN clasificacion
                    ENUM('despacho','retiro','instalacion','mantencion','garantia')
                    DEFAULT 'despacho'""")
            except Exception:
                pass
            cur.execute("""
                CREATE TABLE IF NOT EXISTS transport_commitment_lines (
                    id              INT AUTO_INCREMENT PRIMARY KEY,
                    commitment_id   INT NOT NULL,
                    koprct          VARCHAR(30) NOT NULL,
                    nokopr          VARCHAR(300),
                    cantidad        DECIMAL(12,3) DEFAULT 0,
                    cant_despachada DECIMAL(12,3) DEFAULT 0,
                    saldo           DECIMAL(12,3) DEFAULT 0,
                    bodega          VARCHAR(10),
                    peso_unitario   DECIMAL(10,3) DEFAULT 0,
                    volumen_unitario DECIMAL(14,2) DEFAULT 0,
                    FOREIGN KEY (commitment_id)
                        REFERENCES transport_commitments(id) ON DELETE CASCADE,
                    INDEX idx_comm (commitment_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS transport_manifests (
                    id              INT AUTO_INCREMENT PRIMARY KEY,
                    correlativo     VARCHAR(20) UNIQUE,
                    fecha           DATE NOT NULL,
                    courier         VARCHAR(80) NOT NULL,
                    estado          ENUM('En preparación','En curso','Cerrado','Entregado completo')
                                    DEFAULT 'En preparación',
                    total_items     INT DEFAULT 0,
                    peso_total      DECIMAL(10,3) DEFAULT 0,
                    vol_total       DECIMAL(14,2) DEFAULT 0,
                    peso_pred_total DECIMAL(10,3) DEFAULT 0,
                    costo_total     DECIMAL(12,2) DEFAULT 0,
                    notas           TEXT,
                    created_by      VARCHAR(190),
                    created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at      DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS transport_manifest_items (
                    id              INT AUTO_INCREMENT PRIMARY KEY,
                    manifest_id     INT NOT NULL,
                    commitment_id   INT NOT NULL,
                    orden           INT DEFAULT 0,
                    estado_entrega  ENUM(
                        'En preparación','Entregado a transporte',
                        'En ruta','Entregado','Entrega fallida','Devolución'
                    ) DEFAULT 'En preparación',
                    added_at        DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (manifest_id)
                        REFERENCES transport_manifests(id) ON DELETE CASCADE,
                    FOREIGN KEY (commitment_id)
                        REFERENCES transport_commitments(id),
                    UNIQUE KEY uq_item (manifest_id, commitment_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS transport_logs (
                    id          INT AUTO_INCREMENT PRIMARY KEY,
                    entity_type ENUM('commitment','manifest','manifest_item') NOT NULL,
                    entity_id   INT NOT NULL,
                    accion      VARCHAR(80) NOT NULL,
                    detalle     TEXT,
                    usuario     VARCHAR(190),
                    created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_entity (entity_type, entity_id),
                    INDEX idx_user   (usuario)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            # ── COURIERS ──────────────────────────────────────────────────
            cur.execute("""
                CREATE TABLE IF NOT EXISTS transport_couriers (
                    id              INT AUTO_INCREMENT PRIMARY KEY,
                    nombre          VARCHAR(120) NOT NULL,
                    rut             VARCHAR(20),
                    contacto        VARCHAR(120),
                    telefono        VARCHAR(50),
                    email           VARCHAR(150),
                    tipo            ENUM('nacional','regional','local','internacional') DEFAULT 'nacional',
                    activo          TINYINT(1) DEFAULT 1,
                    notas           TEXT,
                    logo_url        VARCHAR(400),
                    peso_max_bulto  DECIMAL(10,2) DEFAULT 0   COMMENT 'kg máx por bulto (0=sin límite)',
                    peso_max_guia   DECIMAL(10,2) DEFAULT 0   COMMENT 'kg máx por guía (0=sin límite)',
                    vol_max_bulto   DECIMAL(12,2) DEFAULT 0   COMMENT 'cm³ máx por bulto (0=sin límite)',
                    factor_vol      DECIMAL(10,4) DEFAULT 5000 COMMENT 'divisor peso volumétrico (cm³/kg)',
                    created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at      DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS transport_courier_tarifas (
                    id              INT AUTO_INCREMENT PRIMARY KEY,
                    courier_id      INT NOT NULL,
                    zona            VARCHAR(80) NOT NULL DEFAULT 'General',
                    peso_desde      DECIMAL(10,3) NOT NULL DEFAULT 0,
                    peso_hasta      DECIMAL(10,3) NOT NULL DEFAULT 0   COMMENT '0=sin tope',
                    precio_base     DECIMAL(10,2) NOT NULL DEFAULT 0,
                    precio_kg_extra DECIMAL(10,2) DEFAULT 0   COMMENT 'precio por kg sobre peso_desde',
                    moneda          CHAR(3) DEFAULT 'CLP',
                    activo          TINYINT(1) DEFAULT 1,
                    FOREIGN KEY (courier_id)
                        REFERENCES transport_couriers(id) ON DELETE CASCADE,
                    INDEX idx_courier (courier_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            # ── DEFAULT COURIERS (solo si la tabla está vacía) ─────────
            cur.execute("SELECT COUNT(*) AS n FROM transport_couriers")
            row = cur.fetchone()
            if (row or {}).get('n', 1) == 0:
                _defaults = [
                    ('FedEx',                'internacional'),
                    ('Transportes Melling',  'nacional'),
                    ('Transporte Felca',     'regional'),
                    ('Envíame',              'nacional'),
                ]
                for _nom, _tipo in _defaults:
                    cur.execute(
                        "INSERT INTO transport_couriers (nombre, tipo) VALUES (%s, %s)",
                        (_nom, _tipo)
                    )
        conn.commit()
    finally:
        conn.close()


def init_db():
    """Inicializa el esquema MySQL. Sin SQLite — todo va a MySQL."""
    init_mysql_schema()
    init_hrm_tables()
    init_eval_tables()
    init_resets_table()
    init_transporte_tables()
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
      ejecutivo  — acceso al módulo Mantenciones (vista + edición) solamente
      editor     — crear/editar evaluaciones y colaboradores (sin borrar, sin admin)
      lector     — solo lectura
      vendedor   — acceso al módulo Cubicador (solo lectura de productos)
    """
    base = {
        "view":          False,
        "edit":          False,
        "print":         False,
        "create":        False,
        "delete":        False,
        "admin":         False,   # gestión usuarios / configuración
        "superadmin":    False,   # preguntas genéricas + acciones irreversibles
        "hrm":           False,   # módulo colaboradores
        "cubicador":     False,   # módulo cubicador de documentos
        "transporte":    False,   # módulo Transporte y Distribución
        "mantenciones":  False,   # módulo Mantenciones (superadmin + ejecutivo)
    }
    if role == "superadmin":
        return {k: True for k in base}
    if role == "admin":
        return {**base, "view": True, "edit": True, "print": True,
                "create": True, "delete": True, "admin": True, "hrm": True,
                "cubicador": True, "transporte": True, "mantenciones": True}
    if role == "ejecutivo":
        # Sólo mantenciones — sin acceso al resto del sistema
        return {**base, "mantenciones": True, "view": True,
                "edit": True, "create": True, "print": True}
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


def _photo_src(filename, subfolder="uploads"):
    """URL de la foto: directa si es Cloudinary, estática local si no."""
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


def _label_format(fmt):
    formats = {
        "150x100": {"key": "150x100", "w": "150mm", "h": "100mm", "label": "150 x 100 mm"},
        "100x50": {"key": "100x50", "w": "100mm", "h": "50mm", "label": "100 x 50 mm"},
    }
    return formats.get(fmt, formats["150x100"])


def _label_bulto_data(bulto):
    data = dict(bulto)
    largo = float(data.get("largo_cm") or data.get("largo") or 0)
    ancho = float(data.get("ancho_cm") or data.get("ancho") or 0)
    alto = float(data.get("alto_cm") or data.get("alto") or 0)
    kg = float(data.get("kg") or data.get("peso") or data.get("peso_bruto") or 0)
    data.update({
        "largo": largo,
        "ancho": ancho,
        "alto": alto,
        "largo_cm": largo,
        "ancho_cm": ancho,
        "alto_cm": alto,
        "kg": kg,
        "peso": kg,
        "peso_vol": calc_pv(largo, ancho, alto),
    })
    return data


def build_labels_pdf(product, label_bultos, total_bultos, fmt="150x100"):
    """
    Genera un PDF con una o mas etiquetas usando Playwright (Chromium headless).
    El resultado es pixel-perfect idéntico al HTML preview porque usa
    el mismo template label_standalone.html y el mismo @media print CSS.
    Si Playwright no está instalado, lanza ImportError con instrucción de instalación.
    """
    fecha        = datetime.now().strftime("%d-%m-%Y %H:%M")
    label_format = _label_format(fmt)
    enriched     = [_label_bulto_data(b) for b in label_bultos]

    html = render_template(
        "label_standalone.html",
        product      = product,
        bultos       = enriched,
        total_bultos = total_bultos,
        fecha        = fecha,
        qty_per_bulto= {},
        logo_url     = _logo_data_url(),
        fmt          = label_format["key"],
        label_format = label_format,
    )

    # Usa el browser pool — sin overhead de launch/close
    return _pw_pdf(
        html,
        width  = label_format["w"],
        height = label_format["h"],
        wait_fn = (
            "() => {"
            "  const codes = Array.from(document.querySelectorAll('.barcode'));"
            "  return codes.length > 0 && codes.every(c => c.dataset.rendered === '1');"
            "}"
        ),
    )


def build_label_pdf(product, bulto, total_bultos, fmt="150x100"):
    return build_labels_pdf(product, [bulto], total_bultos, fmt)


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
                            "ejecutivo":  "Ejecutivo",
                            "editor":     "Editor",
                            "lector":     "Lector",
                            "vendedor":   "Vendedor",
                        }.get(g.user["role"] if g.user else "", "Usuario"),
        "photo_src":    _photo_src,
    }


# ─────────────────────────────────────────────
#  API — Búsqueda en ERP externo (SOLO LECTURA)
# ─────────────────────────────────────────────

@app.route("/api/sku-lookup")
@login_required
def sku_lookup():
    """
    Búsqueda exacta por SKU — usada por el escáner de código de barras.
    Responde en <50ms sin recargar la página.
    Devuelve: {status, pid?, sku, nombre, prepare_url?}
    """
    sku = request.args.get("sku", "").strip().upper()
    if not sku:
        return jsonify({"status": "not_found", "sku": ""})

    # ── 1. Buscar en app_products (nuestra BD) ───────────────────────
    hit = mysql_fetchone(
        f"SELECT id, nombre, sku FROM `{PRODUCTS_TABLE}` WHERE UPPER(TRIM(sku)) = %s",
        (sku,)
    )
    if hit:
        return jsonify({
            "status":     "found_app",
            "pid":        hit["id"],
            "sku":        hit["sku"],
            "nombre":     (hit["nombre"] or "").strip(),
            "detail_url": url_for("product_detail", pid=hit["id"]),
            "edit_url":   url_for("edit_product",   pid=hit["id"]),
        })

    # ── 2. Buscar en tabla ERP local ─────────────────────────────────
    erp_hit = None
    try:
        erp_hit = mysql_fetchone(
            f"SELECT UPPER(TRIM(`SKU`)) AS sku, TRIM(COALESCE(`Nombre`,'')) AS nombre "
            f"FROM `{ERP_TABLE}` WHERE UPPER(TRIM(`SKU`)) = %s",
            (sku,)
        )
    except Exception:
        pass

    if erp_hit:
        return jsonify({
            "status":      "found_erp",
            "sku":         erp_hit["sku"],
            "nombre":      (erp_hit["nombre"] or "").strip(),
            "prepare_url": url_for("prepare_product_from_erp", sku=erp_hit["sku"]),
        })

    # ── 3. REST API ERP — búsqueda exacta por código ─────────────────
    try:
        TOKEN = ERP_CONFIG.get("api_token", "")
        body  = _erp_get(
            "/productos",
            {"kopr": sku, "empresa": "01", "fields": "KOPR,NOKOPR", "visible": "true"},
            TOKEN, timeout=6,
        )
        items = body.get("data") or []
        if items:
            p = items[0]
            p_sku  = (p.get("KOPR") or "").strip().upper()
            p_name = (p.get("NOKOPR") or "").strip()
            if p_sku:
                return jsonify({
                    "status":      "found_erp",
                    "sku":         p_sku,
                    "nombre":      p_name,
                    "prepare_url": url_for("prepare_product_from_erp", sku=p_sku),
                })
    except Exception:
        pass

    return jsonify({"status": "not_found", "sku": sku})


@app.route("/api/product-search")
@login_required
def product_search():
    """
    Typeahead unificado para el formulario de nuevo producto.
    Fuentes: app_products → tabla ERP local → ERP REST API (fallback).
    Devuelve JSON: [{sku, nombre, source, already_exists}]
    """
    q = request.args.get("q", "").strip()
    if len(q) < 2:
        return jsonify([])

    like_u  = f"%{q.upper()}%"
    like_p  = f"%{q}%"
    results = []
    seen    = set()

    # ── 1. app_products ──────────────────────────────────────────────
    try:
        rows = mysql_fetchall(
            f"""SELECT sku, nombre FROM `{PRODUCTS_TABLE}`
                WHERE UPPER(sku) LIKE %s OR UPPER(nombre) LIKE %s
                ORDER BY
                  CASE WHEN UPPER(sku) = %s THEN 0 ELSE 1 END,
                  nombre
                LIMIT 10""",
            (like_u, like_u, q.upper()),
        )
        for r in rows:
            sku = (r.get("sku") or "").strip().upper()
            if sku and sku not in seen:
                results.append({"sku": sku, "nombre": (r.get("nombre") or "").strip(),
                                 "source": "app", "already_exists": True})
                seen.add(sku)
    except Exception:
        pass

    # ── 2. Tabla ERP local ───────────────────────────────────────────
    try:
        rows2 = mysql_fetchall(
            f"""SELECT UPPER(TRIM(`SKU`)) AS sku,
                       TRIM(COALESCE(`Nombre`,'')) AS nombre
                FROM `{ERP_TABLE}`
                WHERE UPPER(TRIM(`SKU`)) LIKE %s OR UPPER(`Nombre`) LIKE %s
                ORDER BY
                  CASE WHEN UPPER(TRIM(`SKU`)) = %s THEN 0 ELSE 1 END,
                  `Nombre`
                LIMIT 20""",
            (like_u, like_u, q.upper()),
        )
        existing_skus = {r["sku"] for r in results}
        for r in rows2:
            sku = (r.get("sku") or "").strip().upper()
            if not sku or sku in seen:
                continue
            already = sku in existing_skus
            results.append({"sku": sku, "nombre": (r.get("nombre") or "").strip(),
                             "source": "erp", "already_exists": already})
            seen.add(sku)
    except Exception:
        pass

    # ── 3. REST API ERP /productos — fuente en tiempo real ───────────
    if len(q) >= 2:
        try:
            TOKEN = ERP_CONFIG.get("api_token", "")
            body  = _erp_get(
                "/productos",
                {
                    "search":  q,            # busca en código Y descripción
                    "empresa": "01",
                    "fields":  "KOPR,NOKOPR",
                    "visible": "true",
                    "venta":   "true",       # solo productos de venta (excluye servicios)
                },
                TOKEN, timeout=6,
            )
            existing_skus = {r["sku"] for r in results}
            for p in (body.get("data") or []):
                p_sku  = (p.get("KOPR") or "").strip().upper()
                p_name = (p.get("NOKOPR") or "").strip()
                if not p_sku or p_sku in seen:
                    continue
                already = p_sku in existing_skus
                results.append({
                    "sku":           p_sku,
                    "nombre":        p_name,
                    "source":        "erp-api",
                    "already_exists": already,
                })
                seen.add(p_sku)
                if len(results) >= 25:
                    break
        except Exception:
            pass   # si la API no responde, igual devolvemos lo local

    # ── Verificación final: consulta batch a app_products para todos los SKUs ──
    # Esto corrige casos donde la búsqueda por nombre falló (acentos/case) pero
    # el SKU SÍ existe en la BD (evita mostrar "No registrado" cuando ya está).
    if results:
        try:
            all_skus = list({r["sku"] for r in results})
            ph = ",".join(["%s"] * len(all_skus))
            verified = mysql_fetchall(
                f"SELECT UPPER(TRIM(sku)) AS sku FROM `{PRODUCTS_TABLE}` WHERE UPPER(TRIM(sku)) IN ({ph})",
                tuple(all_skus),
            )
            verified_set = {r["sku"] for r in (verified or [])}
            for r in results:
                r["already_exists"] = r["sku"] in verified_set
        except Exception:
            pass  # si falla la verificación, dejamos los valores anteriores

    # Ordenar: exactos primero, no-existentes primero, luego nombre
    results.sort(key=lambda x: (
        1 if x["already_exists"] else 0,
        0 if x["sku"] == q.upper() else 1,
        x["nombre"].lower()
    ))

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
#  Plantilla maestra ILUS para todos los correos
# ─────────────────────────────────────────────

def _ilus_email_html(
    titulo: str,
    subtitulo: str = "",
    saludo: str = "",
    parrafos: list = None,          # lista de strings HTML
    btn_primario_txt: str = "",
    btn_primario_url: str = "",
    btn_secundario_txt: str = "",
    btn_secundario_url: str = "",
    info_lineas: list = None,       # lista de (icono, clave, valor)
) -> str:
    """
    Genera HTML de correo con el diseño oficial ILUS:
    Header negro con logo → banda oscura título/subtítulo → cuerpo blanco → botones → footer negro.
    """
    # ── Logo y empresa desde config (base64 o URL) ──────────────────────────
    try:
        cc       = _get_client_cfg()
        logo_src = cc.get("logo_url") or ""
        company  = cc.get("company_name") or "ILUS Sport &amp; Health"
    except Exception:
        logo_src = ""
        company  = "ILUS Sport &amp; Health"
    if not logo_src:
        logo_src = "https://ilusfitness.com/cdn/shop/files/Logo_ILUS_Fitness_Blanco_equipamiento_para_gimnasios.png"

    # ── Info box ─────────────────────────────────────────────────────────────
    info_html = ""
    if info_lineas:
        rows_html = "".join(
            f'<p style="margin:0 0 4px;font-size:13px;color:#333">'
            f'<strong>{k}:</strong> {v}</p>'
            for _, k, v in info_lineas
        )
        info_html = (
            f'<div style="background:#f8f8f8;border-left:4px solid #DC143C;'
            f'padding:15px 18px;margin:20px 0;border-radius:6px">'
            f'{rows_html}</div>'
        )

    # ── Párrafos ─────────────────────────────────────────────────────────────
    saludo_html = (
        f'<p style="font-size:14px;color:#111827;line-height:1.6;margin:0 0 14px">'
        f'Hola <strong>{saludo}</strong>,</p>'
    ) if saludo else ""

    body_html = "".join(
        f'<p style="font-size:14px;color:#111827;line-height:1.6;margin:0 0 14px">{p}</p>'
        for p in (parrafos or [])
    )

    # ── Botones ──────────────────────────────────────────────────────────────
    btn1 = ""
    if btn_primario_txt and btn_primario_url:
        btn1 = (
            f'<a href="{btn_primario_url}" style="display:inline-block;background:#DC143C;'
            f'color:#ffffff;padding:12px 25px;text-decoration:none;font-size:13px;'
            f'border-radius:5px;margin:5px;font-weight:bold">{btn_primario_txt}</a>'
        )
    btn2 = ""
    if btn_secundario_txt and btn_secundario_url:
        btn2 = (
            f'<a href="{btn_secundario_url}" style="display:inline-block;background:#000;'
            f'color:#ffffff;padding:12px 25px;text-decoration:none;font-size:13px;'
            f'border-radius:5px;margin:5px;font-weight:bold">{btn_secundario_txt}</a>'
        )
    btns_html = (
        f'<div style="padding:0 30px 30px;text-align:center">{btn1}{btn2}</div>'
    ) if (btn1 or btn2) else ""

    return f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{titulo}</title>
</head>
<body style="margin:0;padding:0;background:#f1f2f4;font-family:Arial,Helvetica,sans-serif;color:#111827">

<div style="max-width:580px;margin:28px auto;background:#ffffff;border-radius:10px;
            overflow:hidden;box-shadow:0 6px 20px rgba(15,23,42,.10)">

  <!-- HEADER: negro + logo -->
  <div style="background:#000;padding:24px 28px;text-align:center">
    <img src="{logo_src}" alt="{company}"
         style="height:48px;display:block;margin:0 auto;max-width:230px;width:auto;object-fit:contain">
  </div>

  <!-- TÍTULO: banda oscura -->
  <div style="background:#111;color:#fff;text-align:center;padding:28px 32px;border-top:1px solid #202020">
    <h1 style="margin:0;font-size:22px;line-height:1.25;font-weight:800">{titulo}</h1>
    {f'<p style="margin-top:8px;font-size:13px;line-height:1.45;color:#f3f4f6;margin-bottom:0">{subtitulo}</p>' if subtitulo else ''}
  </div>

  <!-- CONTENIDO -->
  <div style="padding:32px 30px">
    {saludo_html}
    {body_html}
    {info_html}
  </div>

  <!-- BOTONES -->
  {btns_html}

  <!-- FOOTER: negro -->
  <div style="background:#000;padding:24px 32px;text-align:center">
    <div style="color:#DC143C;font-size:13px;font-weight:700;text-transform:uppercase">{company}</div>
    <div style="color:#9ca3af;font-size:11px;margin-top:5px">
      Equipamiento profesional para alto rendimiento
    </div>
    <div style="margin-top:14px;font-size:11px;line-height:1.6;color:#6b7280">
      Este correo fue generado automáticamente.<br>
      Para soporte, utiliza nuestros canales oficiales.
    </div>
  </div>

</div>
</body>
</html>"""


def _send_ilus_email(to_addr: str, subject: str, html_body: str) -> bool:
    """Envía un correo HTML usando la configuración SMTP dinámica (o EMAIL_CONFIG de respaldo)."""
    # Prioridad: config guardada en BD → EMAIL_CONFIG hardcoded
    try:
        dyn = _get_smtp_cfg()
    except Exception:
        dyn = {}
    cfg = dyn if dyn.get("smtp_host") and dyn.get("smtp_user") else EMAIL_CONFIG
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = f"{cfg.get('from_name','ILUS Sport & Health')} <{cfg.get('from_addr', cfg.get('smtp_user',''))}>"
        msg["To"]      = to_addr
        msg.attach(MIMEText(html_body, "html", "utf-8"))
        port   = int(cfg.get("smtp_port", 587))
        secure = bool(cfg.get("secure"))
        from_addr = cfg.get("from_addr") or cfg.get("smtp_user", "")
        if secure:
            with smtplib.SMTP_SSL(cfg["smtp_host"], port, timeout=15) as srv:
                srv.login(cfg["smtp_user"], cfg["smtp_pass"])
                srv.sendmail(from_addr, [to_addr], msg.as_string())
        else:
            with smtplib.SMTP(cfg["smtp_host"], port, timeout=15) as srv:
                srv.ehlo(); srv.starttls()
                srv.login(cfg["smtp_user"], cfg["smtp_pass"])
                srv.sendmail(from_addr, [to_addr], msg.as_string())
        return True
    except Exception as exc:
        try:
            g._last_email_error = str(exc)
        except Exception:
            pass
        print(f"[ILUS][EMAIL] Error al enviar a {to_addr}: {exc}")
        return False


def _password_strength_errors(password: str) -> list[str]:
    """Politica minima para claves creadas desde enlaces publicos."""
    errors = []
    if len(password or "") < 12:
        errors.append("La contraseña debe tener al menos 12 caracteres.")
    if not re.search(r"[a-z]", password or ""):
        errors.append("Incluye al menos una letra minuscula.")
    if not re.search(r"[A-Z]", password or ""):
        errors.append("Incluye al menos una letra mayuscula.")
    if not re.search(r"\d", password or ""):
        errors.append("Incluye al menos un numero.")
    if not re.search(r"[^A-Za-z0-9]", password or ""):
        errors.append("Incluye al menos un simbolo.")
    return errors


def _issue_password_token(user_id: int, minutes: int = 60) -> tuple[str, datetime]:
    """Crea un token de un solo uso e invalida enlaces anteriores del usuario."""
    token = secrets.token_urlsafe(64)
    expires = datetime.utcnow() + timedelta(minutes=minutes)
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute(
            f"UPDATE `{RESETS_TABLE}` SET used=1 WHERE user_id=%s AND used=0",
            (user_id,),
        )
        cur.execute(
            f"INSERT INTO `{RESETS_TABLE}` (user_id, token, expires_at) VALUES (%s,%s,%s)",
            (user_id, token, expires),
        )
    conn.commit()
    return token, expires


def _send_password_access_email(
    to_addr: str,
    to_name: str,
    action_url: str,
    *,
    actor_name: str = "ILUS",
    mode: str = "reset",
    minutes: int = 60,
) -> bool:
    """Envia correo ILUS para crear o cambiar clave mediante token seguro."""
    try:
        diag = _smtp_connection_diagnose(_get_smtp_cfg())
        if not diag.get("ok"):
            detail = diag.get("message") or "No se pudo validar la conexion SMTP."
            suggestions = diag.get("suggestions") or []
            if suggestions:
                detail += " " + " ".join(suggestions[:3])
            g._last_email_error = detail
            return False
    except Exception as exc:
        g._last_email_error = f"No se pudo validar SMTP antes de enviar: {exc}"
        return False

    is_setup = mode == "setup"
    titulo = "Crear contraseña de acceso" if is_setup else "Cambio de contraseña solicitado"
    subject = "ILUS - Crea tu contraseña de acceso" if is_setup else "ILUS - Cambio seguro de contraseña"
    button = "Crear mi contraseña" if is_setup else "Cambiar contraseña"
    intro = (
        f"<strong>{actor_name}</strong> creo una cuenta para ti en el sistema ILUS."
        if is_setup
        else f"<strong>{actor_name}</strong> solicito un cambio de contraseña para tu cuenta ILUS."
    )
    html_body = _ilus_email_html(
        titulo=titulo,
        subtitulo="Acceso seguro con enlace de un solo uso",
        saludo=f"Hola, {to_name}",
        parrafos=[
            intro,
            "Por seguridad, la contraseña no se envia ni se escribe manualmente. "
            "Debes definirla desde el boton de este correo.",
            f"Este enlace vence en <strong>{minutes} minutos</strong>, solo puede usarse una vez "
            "y reemplaza cualquier enlace anterior.",
            f'Si no esperabas este correo, ignoralo o avisa al administrador.<br>'
            f'<span style="font-size:11px;color:#777">Enlace directo: '
            f'<a href="{action_url}" style="color:#CC0000">{action_url}</a></span>',
        ],
        btn_primario_txt=button,
        btn_primario_url=action_url,
        info_lineas=[
            ("", "Cuenta", to_addr),
            ("", "Solicitado por", actor_name),
        ],
    )
    sent = _send_ilus_email(to_addr, subject, html_body)
    if not sent and not getattr(g, "_last_email_error", ""):
        g._last_email_error = "El servidor SMTP rechazo el envio. Revisa la configuracion en Comunicaciones."
    return sent


# ─────────────────────────────────────────────
#  Recuperación de contraseña
# ─────────────────────────────────────────────

def _send_recovery_email(to_addr: str, to_name: str, reset_url: str) -> bool:
    """Envía el correo HTML de recuperación con diseño ILUS unificado."""
    html_body = _ilus_email_html(
        titulo          = "Recuperar contraseña",
        subtitulo       = "Sistema de Gestión ILUS Sport &amp; Health",
        saludo          = f"Hola, {to_name}",
        parrafos        = [
            "Recibimos una solicitud para restablecer la contraseña de tu cuenta en el sistema ILUS.",
            "Haz clic en el botón a continuación para crear una nueva contraseña. "
            "Este enlace es válido por <strong>60 minutos</strong>.",
            f'Si no solicitaste este cambio, puedes ignorar este correo — '
            f'tu contraseña seguirá siendo la misma.<br>'
            f'<span style="font-size:11px;color:#bbb">O copia: '
            f'<a href="{reset_url}" style="color:#CC0000">{reset_url}</a></span>',
        ],
        btn_primario_txt = "Restablecer contraseña",
        btn_primario_url = reset_url,
    )
    return _send_ilus_email(to_addr, "Recuperar contraseña — ILUS Sport & Health", html_body)


def _send_invitation_email(to_addr: str, to_name: str, set_url: str, creator_name: str = "ILUS") -> bool:
    """Envía correo de invitación a nuevo usuario para que cree su contraseña."""
    html_body = _ilus_email_html(
        titulo           = "Bienvenido a ILUS",
        subtitulo        = "Sistema de Gestión ILUS Sport &amp; Health",
        saludo           = f"Hola, {to_name}",
        parrafos         = [
            f"<strong>{creator_name}</strong> ha creado una cuenta para ti en el "
            f"<strong>Sistema de Gestión ILUS</strong>.",
            "Para ingresar por primera vez, establece tu contraseña personal "
            "haciendo clic en el botón a continuación.",
            "Este enlace es válido por <strong>24 horas</strong>. "
            "Si no esperabas esta invitación, puedes ignorar este correo.",
        ],
        btn_primario_txt = "🔐 Crear mi contraseña",
        btn_primario_url = set_url,
        info_lineas      = [("", "Tu email de acceso", to_addr)],
    )
    return _send_ilus_email(
        to_addr,
        "Bienvenido a ILUS — Crea tu contraseña de acceso",
        html_body,
    )


def _send_recovery_email(to_addr: str, to_name: str, reset_url: str) -> bool:
    """Version vigente: cambio de clave con token seguro."""
    return _send_password_access_email(
        to_addr, to_name, reset_url, actor_name="ILUS", mode="reset", minutes=60
    )


def _send_invitation_email(to_addr: str, to_name: str, set_url: str, creator_name: str = "ILUS") -> bool:
    """Version vigente: alta de usuario con token seguro."""
    return _send_password_access_email(
        to_addr, to_name, set_url, actor_name=creator_name, mode="setup", minutes=1440
    )


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
                    token, _expires = _issue_password_token(user["id"], minutes=60)

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
        if False:
            flash("La contraseña debe tener al menos 8 caracteres.", "danger")
            return render_template("reset_password.html", token=token, nombre=row["nombre"])
        if pw1 != pw2:
            flash("Las contraseñas no coinciden.", "danger")
            return render_template("reset_password.html", token=token, nombre=row["nombre"])

        strength_errors = _password_strength_errors(pw1)
        if strength_errors:
            flash(" ".join(strength_errors), "danger")
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
    photo_urls = [_photo_src(ph["filename"]) for ph in photos]
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
            print(f"[ILUS] Foto subida a Cloudinary: {filename}")
        except Exception as exc:
            print(f"[ILUS] Cloudinary upload error: {exc}")
            flash(f"Error al subir la foto a la nube: {exc}", "danger")
            return redirect(url_for("product_detail", pid=pid))
    else:
        filename = f"p{pid}_{ts}.{ext}"
        try:
            file.save(os.path.join(UPLOAD_FOLDER, filename))
            print(f"[ILUS] Foto guardada localmente: {filename}")
        except Exception as exc:
            print(f"[ILUS] Error guardando foto local: {exc}")
            flash(f"Error al guardar la foto: {exc}", "danger")
            return redirect(url_for("product_detail", pid=pid))

    # Guardamos URL completa (Cloudinary) o nombre local
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO `{PHOTOS_TABLE}` (product_id,filename,orden) VALUES (%s,%s,%s)",
            (pid, filename, len(photos) + 1),
        )
    conn.commit()

    flash("Foto agregada correctamente.", "success")
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

def _render_labels_view(pid, template_name):
    only = request.args.get("bulto", type=int)
    fmt = request.args.get("fmt", "150x100")
    label_format = _label_format(fmt)
    product, bultos, _ = get_full(pid)
    if not product:
        flash("Producto no encontrado.", "danger")
        return redirect(url_for("index"))

    enriched_bultos = [_label_bulto_data(b) for b in bultos]
    valid = [b for b in enriched_bultos if b["largo"] > 0 and b["ancho"] > 0 and b["alto"] > 0]
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
    response = make_response(render_template(template_name, product=product, bultos=valid,
                                             total_bultos=len(bultos), fecha=fecha,
                                             qty_per_bulto=qty_per_bulto,
                                             fmt=label_format["key"],
                                             label_format=label_format,
                                             logo_url=_logo_data_url()))
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    return response


@app.route("/products/<int:pid>/labels")
@require_permission("print")
def print_labels(pid):
    return _render_labels_view(pid, "print_labels.html")


@app.route("/print/labels")
@require_permission("print")
def print_labels_alt():
    pid = request.args.get("pid", type=int)
    if not pid:
        flash("Selecciona un producto para imprimir etiquetas.", "warning")
        return redirect(url_for("index"))
    return _render_labels_view(pid, "print_labels.html")


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
    """Descarga las etiquetas seleccionadas como un PDF multipagina."""
    return _labels_pdf_response(pid, force_download=True)


@app.route("/products/<int:pid>/labels-preview.pdf")
@require_permission("print")
def labels_pdf_preview(pid):
    """PDF inline para previsualizacion en modal."""
    return _labels_pdf_response(pid, force_download=request.args.get("download") == "1")


def _labels_pdf_response(pid, force_download=False):
    product, bultos, _ = get_full(pid)
    if not product:
        return "Producto no encontrado", 404

    valid = [_label_bulto_data(b) for b in bultos]
    valid = [b for b in valid if b["largo"] > 0 and b["ancho"] > 0 and b["alto"] > 0]
    only = request.args.get("bulto", type=int)
    if only:
        valid = [b for b in valid if int(b["bulto_num"]) == only]

    if not valid:
        return "Sin bultos con medidas completas", 404

    selected = []
    for b in valid:
        qty = request.args.get(f"qty_{b['bulto_num']}", 1, type=int)
        qty = max(1, min(qty, 10))
        for _ in range(qty):
            selected.append(dict(b))

    try:
        pdf_bytes = build_labels_pdf(product, selected, len(bultos), request.args.get("fmt", "150x100"))
    except Exception as exc:
        return f"Error generando PDF: {exc}", 500

    fecha = datetime.now().strftime("%Y%m%d_%H%M")
    filename = f"ILUS_{product['sku']}_{fecha}.pdf"
    disposition = "attachment" if force_download else "inline"
    return Response(
        pdf_bytes,
        mimetype="application/pdf",
        headers={
            "Content-Disposition": f'{disposition}; filename="{filename}"',
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
        },
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
        pdf_bytes = build_label_pdf(product, bulto, len(bultos), request.args.get("fmt", "150x100"))
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
        username    = request.form.get("username", "").strip().lower()
        nombre      = request.form.get("nombre",   "").strip()
        role        = request.form.get("role",      "editor")
        active      = 1 if request.form.get("active") == "1" else 0
        send_invite = request.form.get("send_invite") == "1"
        wa_number   = request.form.get("wa_number", "").strip()

        errors = []
        if not username:
            errors.append("El correo es requerido.")
        elif not EMAIL_RE.match(username):
            errors.append("El correo no tiene un formato válido.")
        if not nombre:
            errors.append("El nombre y apellido son requeridos.")
        if role not in {"superadmin", "admin", "editor", "lector", "vendedor"}:
            errors.append("Rol no valido.")
        if get_auth_user_by_username(username):
            errors.append("Ese correo ya está registrado.")

        if errors:
            return render_template("user_form.html", errors=errors, user=None, fd=request.form)

        # Crear usuario con contraseña placeholder (bloqueada hasta que use el enlace)
        placeholder_hash = generate_password_hash(secrets.token_hex(32))
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO `{AUTH_TABLE}` (username,nombre,password_hash,role,active) VALUES (%s,%s,%s,%s,%s)",
                (username, nombre, placeholder_hash, role, active),
            )
        conn.commit()

        # Generar token de invitación (usa la misma tabla de resets, válido 24h)
        if send_invite:
            try:
                new_uid   = mysql_fetchone(f"SELECT id FROM `{AUTH_TABLE}` WHERE username=%s", (username,))["id"]
                token, _expires = _issue_password_token(new_uid, minutes=1440)
                set_url   = url_for("reset_password", token=token, _external=True)
                creator   = g.user["nombre"] if g.user else "ILUS"
                sent      = _send_invitation_email(username, nombre, set_url, creator)

                # Enviar WhatsApp si se indicó número
                if wa_number:
                    try:
                        wa_cfg = _get_wa_cfg()
                        if wa_cfg.get("account_sid") and wa_cfg.get("auth_token"):
                            wa_msg = (
                                f"👋 Hola {nombre}, te damos la bienvenida al sistema ILUS.\n\n"
                                f"Recibiste un email en *{username}* con un enlace para crear tu contraseña. "
                                f"Presiona el botón del correo y sigue los pasos para ingresar.\n\n"
                                f"📧 Revisa también tu carpeta de Spam si no lo encuentras.\n"
                                f"⏰ El enlace es válido por 24 horas."
                            )
                            _send_whatsapp(
                                wa_cfg["account_sid"], wa_cfg["auth_token"],
                                wa_cfg["from_number"], wa_number, wa_msg
                            )
                    except Exception as _we:
                        print(f"[ILUS][INVITE-WA] {_we}")

                if sent:
                    flash(f"Usuario creado. Invitación enviada a {username}.", "success")
                else:
                    detalle = getattr(g, "_last_email_error", "") or "Revisa la configuracion SMTP en Comunicaciones."
                    flash(f"Usuario creado, pero no se pudo enviar el correo para crear contraseña. {detalle}", "warning")
            except Exception as _ie:
                flash(f"Usuario creado, pero falló el envío de invitación: {_ie}", "warning")
        else:
            flash("Usuario creado. Recuerda establecer la contraseña o enviar la invitación después.", "success")

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
        role     = request.form.get("role",     "editor")
        active   = 1 if request.form.get("active") == "1" else 0

        errors = []
        if not username:
            errors.append("El correo es requerido.")
        elif not EMAIL_RE.match(username):
            errors.append("El correo no tiene un formato válido.")
        if not nombre:
            errors.append("El nombre y apellido son requeridos.")
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


@app.route("/admin/users/<int:user_id>/invite", methods=["POST"])
@require_permission("admin")
def invite_user(user_id):
    """Envía (o reenvía) el correo de invitación para crear contraseña."""
    user = get_auth_user_by_id(user_id)
    if not user:
        return jsonify({"error": "Usuario no encontrado"}), 404
    try:
        token, _expires = _issue_password_token(user_id, minutes=1440)
        set_url = url_for("reset_password", token=token, _external=True)
        creator = g.user["nombre"] if g.user else "ILUS"
        sent    = _send_invitation_email(user["username"], user["nombre"], set_url, creator)
        if sent:
            return jsonify({"ok": True})
        return jsonify({"error": "No se pudo enviar el email. Revisa la configuración SMTP en Comunicaciones."}), 500
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


# ══════════════════════════════════════════════════════════════
#  MÓDULO: HRM — COLABORADORES
# ══════════════════════════════════════════════════════════════

@app.route("/admin/users/<int:user_id>/password-link", methods=["POST"])
@require_permission("admin")
def user_password_link(user_id):
    """Envia un enlace seguro para que el usuario cambie su propia clave."""
    user = get_auth_user_by_id(user_id)
    if not user:
        return jsonify({"error": "Usuario no encontrado"}), 404
    try:
        token, _expires = _issue_password_token(user_id, minutes=60)
        reset_url = url_for("reset_password", token=token, _external=True)
        actor = g.user["nombre"] if g.user else "ILUS"
        sent = _send_password_access_email(
            user["username"], user["nombre"], reset_url,
            actor_name=actor, mode="reset", minutes=60
        )
        if sent:
            return jsonify({"ok": True, "message": "Enlace seguro enviado."})
        detalle = getattr(g, "_last_email_error", "") or "Revisa la configuracion SMTP en Comunicaciones."
        return jsonify({"error": f"No se pudo enviar el email. {detalle}"}), 500
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


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
    ("GDV", "Guía de Despacho"),
    ("NVV", "Nota de Venta"),
    ("NVI", "Nota de Venta Internet"),
    ("VD",  "Nota de Venta Directa"),
    ("WEB", "Nota de Venta Web"),
    ("COV", "Cotización"),
]

# VD y WEB usan TIDO=NVV en el ERP; el NUDO lleva el prefijo dentro (10 chars)
# NVI puede mapear a NVV también dependiendo del ERP
_ERP_TIDO_NUDO_MAP = {
    "VD":  ("NVV", lambda n: "VD"  + str(n).zfill(8)),
    "WEB": ("NVV", lambda n: "WEB" + str(n).zfill(7)),
}


def _nudo_variants(nudo_raw):
    """
    El ERP guarda NUDO como string de 10 chars con ceros a la izquierda.
    Devuelve lista de variantes a probar: ['0000009344', '9344', ...]
    """
    s = str(nudo_raw).strip()
    padded = s.zfill(10)
    return list(dict.fromkeys([padded, s]))   # únicos, preservando orden


def _erp_get(path, params, token, timeout=10):
    """
    GET a la REST API del ERP usando urllib (sin dependencias externas).
    Retorna el JSON decodificado o lanza excepción.
    """
    import urllib.request as _urlreq
    import urllib.parse   as _urlparse
    import json           as _json_mod

    url = ERP_CONFIG.get("api_url", "https://lab.random.cl/ilus").rstrip("/") + path
    qs  = _urlparse.urlencode(params)
    req = _urlreq.Request(
        f"{url}?{qs}",
        headers={"Authorization": f"Bearer {token}"},
    )
    with _urlreq.urlopen(req, timeout=timeout) as resp:
        return _json_mod.loads(resp.read().decode("utf-8"))


def _cubicador_fetch(tido, nudo):
    """
    Consulta el ERP vía REST API y cruza con nuestra BD local.
    Retorna (header_dict, lineas_list) o lanza excepción.
    """
    from datetime import datetime as _dt

    TOKEN = ERP_CONFIG.get("api_token", "")

    # ── Mapear VD/WEB → TIDO=NVV con NUDO prefijado (ej. VD00008827) ──
    display_tido = tido
    if tido in _ERP_TIDO_NUDO_MAP:
        erp_tido, nudo_fn = _ERP_TIDO_NUDO_MAP[tido]
        erp_nudo = nudo_fn(nudo)
    else:
        erp_tido = tido
        erp_nudo = str(nudo).strip()

    nudos = _nudo_variants(erp_nudo)

    # ── 1. Buscar el documento (probar variantes de NUDO) ──────────────
    raw_header = None
    raw_lineas = []
    for nv in nudos:
        try:
            body = _erp_get(
                "/documentos/render",
                {"tido": erp_tido, "nudo": nv, "empresa": "01"},
                TOKEN, timeout=12,
            )
            data = body.get("data") or []
            if data:
                raw_header = data[0].get("maeedo") or {}
                raw_lineas = data[0].get("maeddo") or []
                break
        except Exception as api_err:
            raise ConnectionError(f"No se pudo conectar al ERP ({api_err}). Intenta en unos momentos.")

    if not raw_header:
        return None, []

    # ── 2. Nombre del cliente (entidades) ──────────────────────────────
    endo = (raw_header.get("ENDO") or "").strip()
    cliente_nombre = ""
    cliente_rut    = endo
    if endo:
        try:
            ent_body = _erp_get("/entidades", {"rten": endo}, TOKEN, timeout=8)
            ent_data = ent_body.get("data") or []
            if ent_data:
                cliente_nombre = (ent_data[0].get("NOKOEN") or "").strip().title()
                cliente_rut    = (ent_data[0].get("RTEN")   or endo).strip()
        except Exception:
            pass   # si falla entidades igual mostramos el doc

    # ── 3. Formatear fecha ─────────────────────────────────────────────
    fecha_raw = raw_header.get("FEEMDO", "")
    try:
        fecha = _dt.fromisoformat(fecha_raw.replace("Z", "+00:00")).strftime("%d/%m/%Y")
    except Exception:
        fecha = fecha_raw

    _nudo_str = str(raw_header.get("NUDO", erp_nudo) or erp_nudo)
    header = {
        "tido":           display_tido,               # VD / WEB, no NVV
        "nudo":           str(nudo),                  # número sin prefijo
        "nudo_display":   str(nudo).lstrip("0") or str(nudo),
        "fecha":          fecha,
        "valor_neto":     float(raw_header.get("VANEDO") or 0),
        "valor_iva":      float(raw_header.get("VAIVDO") or 0),
        "valor_bruto":    float(raw_header.get("VABRDO") or 0),
        "cliente_nombre": cliente_nombre,
        "cliente_rut":    cliente_rut,
        "comuna":         (raw_header.get("NOKOZO") or raw_header.get("CMEN") or
                           raw_header.get("NOKOCOMU") or raw_header.get("NOKOCOMUNADE") or
                           raw_header.get("NOKOMUENDE") or raw_header.get("NOKOMUNEN") or
                           raw_header.get("NOKCOMENDESP") or "").strip(),
        "direccion":      (raw_header.get("DIENDESP") or raw_header.get("DIENDE") or
                           raw_header.get("OBDO") or "").strip(),
        "telefono":       "",
        "email":          "",
        "all_fields":     list(raw_header.keys()),  # para debug
    }

    # ── 4. Cruzar líneas con BD local (bultos/peso) ────────────────────
    lineas = []
    for l in raw_lineas:
        sku         = (l.get("KOPRCT") or "").strip().upper()
        descripcion = (l.get("NOKOPR") or "").strip()
        qty          = float(l.get("CAPRCO1") or 0)
        qty_desp     = float(l.get("CAPRAD1") or 0)
        saldo_linea  = max(qty - qty_desp, 0)
        es_zz        = sku.upper() in {s.upper() for s in {"ZZENVIO","ZZINGREPUESTO","ZZSERVTEC","ZZRETIRO","ZZINSTALACION","ZZINGARREQUIP"}}

        if not sku:
            continue

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

        tiene_ficha  = app_data is not None
        total_bultos = int(app_data["total_bultos"] if tiene_ficha else 0)
        tiene_bultos = tiene_ficha and float(app_data.get("volumen_cm3") or 0) > 0

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
            # Saldo / despacho
            "cantidad_despachada": qty_desp,
            "saldo":               saldo_linea,
            "es_zz":               es_zz,
        })

    return header, lineas


def _parse_docs_from_form(form):
    """Parse tido_0/nudo_0, tido_1/nudo_1, … from a form. Returns [(tido, nudo), …]."""
    import re as _re
    docs = []
    i = 0
    while i < 20:   # safety cap
        tido_i = (form.get(f"tido_{i}") or "").strip().upper()
        nudo_i = (form.get(f"nudo_{i}") or "").strip()
        if tido_i == "" and nudo_i == "":
            break
        if nudo_i:
            for _cod, _ in TIPOS_DOC_CUBICADOR:
                _m = _re.match(r"^" + _cod + r"\s*(\S+)$", nudo_i.upper())
                if _m:
                    tido_i = _cod
                    nudo_i = _m.group(1)
                    break
            docs.append((tido_i or "FCV", nudo_i))
        i += 1
    return docs


def _fetch_multi_docs(docs):
    """Fetch each doc and merge lines by SKU. Returns (headers, merged_lineas, errors)."""
    headers = []
    merged  = {}      # sku → line dict
    errors  = []

    for tido_i, nudo_i in docs:
        try:
            hdr, lineas = _cubicador_fetch(tido_i, nudo_i)
            if hdr is None:
                errors.append(f"No se encontró {tido_i} N° {nudo_i} en el ERP.")
            else:
                headers.append(hdr)
                for l in lineas:
                    sku = l["sku"]
                    if sku in merged:
                        m = merged[sku]
                        m["cantidad"]     += l["cantidad"]
                        m["peso_kg_tot"]  += l["peso_kg_tot"]
                        m["peso_vol_tot"] += l["peso_vol_tot"]
                        m["vol_tot"]      += l["vol_tot"]
                        m["pred_tot"]     += l["pred_tot"]
                    else:
                        merged[sku] = dict(l)
        except ConnectionError as ce:
            errors.append(str(ce))
        except Exception as ex:
            errors.append(f"Error {tido_i} N° {nudo_i}: {ex}")

    return headers, list(merged.values()), errors


@app.route("/cubicador", methods=["GET", "POST"])
@login_required
def cubicador():
    if not g.permissions.get("cubicador"):
        flash("No tienes acceso al módulo Cubicador.", "danger")
        return redirect(url_for("index"))

    docs      = []
    resultado = None
    error_msg = None

    if request.method == "POST":
        docs = _parse_docs_from_form(request.form)
    else:
        # GET: backward compat (?tido=FCV&nudo=9344)
        _tido = (request.args.get("tido") or "FCV").strip().upper()
        _nudo = (request.args.get("nudo") or "").strip()
        if _nudo:
            docs = [(_tido, _nudo)]

    if docs:
        headers, lineas, errors = _fetch_multi_docs(docs)
        if errors:
            error_msg = " · ".join(errors)
        if headers:
            resultado = {
                "headers": headers,
                "lineas":  lineas,
                "docs":    docs,
                "multi":   len(docs) > 1,
            }

    return render_template(
        "cubicador/index.html",
        tipos_doc=TIPOS_DOC_CUBICADOR,
        docs=docs,
        resultado=resultado,
        error_msg=error_msg,
    )


@app.route("/cubicador/export/excel", methods=["POST"])
@login_required
def cubicador_export_excel():
    """Descarga el resultado del cubicador como Excel (.xlsx) — soporta múltiples documentos."""
    if not g.permissions.get("cubicador"):
        flash("No tienes acceso al módulo Cubicador.", "danger")
        return redirect(url_for("index"))

    docs = _parse_docs_from_form(request.form)
    if not docs:
        flash("Debes ingresar al menos un documento.", "warning")
        return redirect(url_for("cubicador"))

    try:
        headers, lineas, errors = _fetch_multi_docs(docs)
    except Exception as ex:
        flash(f"Error al consultar el ERP: {ex}", "danger")
        return redirect(url_for("cubicador"))

    if not headers:
        flash(" · ".join(errors) if errors else "No se encontraron documentos.", "warning")
        return redirect(url_for("cubicador"))

    import openpyxl
    from openpyxl.styles import Font, PatternFill, Alignment
    from openpyxl.utils import get_column_letter
    from io import BytesIO

    wb = openpyxl.Workbook()
    ws = wb.active

    BLACK, RED, LGRAY = "1A1A1A", "CC0000", "F5F5F5"

    def _hdr_cell(cell, val):
        cell.value = val
        cell.font = Font(bold=True, color="FFFFFF", size=9)
        cell.fill = PatternFill("solid", fgColor=BLACK)
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)

    # ── Fila 1: título ───────────────────────────────────────────────
    if len(docs) == 1:
        tido0, nudo0 = docs[0]
        ws.title    = f"{tido0}{nudo0}"[:31]
        title_text  = f"CUBICADOR ILUS  ·  {tido0} N° {nudo0}"
    else:
        ws.title   = "Cubicador Múltiple"
        title_text = f"CUBICADOR ILUS  ·  {len(docs)} documentos combinados"

    ws.merge_cells("A1:J1")
    c = ws["A1"]
    c.value = title_text
    c.font  = Font(bold=True, size=13, color="FFFFFF")
    c.fill  = PatternFill("solid", fgColor=BLACK)
    c.alignment = Alignment(horizontal="center", vertical="center")
    ws.row_dimensions[1].height = 28

    # ── Filas 2…N: una por documento ────────────────────────────────
    row_offset = 2
    for hdr in headers:
        ws.merge_cells(f"A{row_offset}:D{row_offset}")
        ws[f"A{row_offset}"].value = (
            f"{hdr['tido']} N° {hdr.get('nudo_display', hdr.get('nudo',''))}  ·  "
            f"{hdr.get('cliente_nombre','—')}   RUT {hdr.get('cliente_rut','—')}   {hdr.get('fecha','')}"
        )
        ws[f"A{row_offset}"].font = Font(size=9)
        ws[f"A{row_offset}"].alignment = Alignment(horizontal="left", vertical="center")

        ws.merge_cells(f"E{row_offset}:J{row_offset}")
        ws[f"E{row_offset}"].value = (
            f"Neto: ${hdr.get('valor_neto',0):,.0f}   "
            f"IVA: ${hdr.get('valor_iva',0):,.0f}   "
            f"Bruto: ${hdr.get('valor_bruto',0):,.0f}"
        )
        ws[f"E{row_offset}"].font = Font(bold=True, size=9)
        ws[f"E{row_offset}"].alignment = Alignment(horizontal="right", vertical="center")
        ws.row_dimensions[row_offset].height = 18
        row_offset += 1

    # ── Fila de encabezados de columna ───────────────────────────────
    hdr_row = row_offset
    cols = ["SKU", "Descripción ERP", "Cant", "Bultos",
            "Kg/u", "PV/u", "Vol cm³/u", "Predom/u", "Total Predom", "Tipo"]
    for ci, h in enumerate(cols, 1):
        _hdr_cell(ws.cell(row=hdr_row, column=ci), h)
    ws.row_dimensions[hdr_row].height = 20
    row_offset += 1

    # ── Filas de datos ───────────────────────────────────────────────
    for ri, l in enumerate(lineas, row_offset):
        bg = LGRAY if ri % 2 == 0 else "FFFFFF"
        vals = [
            l["sku"],
            l["descripcion_erp"],
            int(l["cantidad"]),
            l["total_bultos"] if l["tiene_ficha"] else "s/f",
            l["peso_kg_u"]  if l["tiene_bultos"] else None,
            l["peso_vol_u"] if l["tiene_bultos"] else None,
            l["vol_u"]      if l["tiene_bultos"] else None,
            l["pred_u"]     if l["tiene_bultos"] else None,
            l["pred_tot"]   if l["tiene_bultos"] else None,
            ("kg" if l["peso_kg_u"] >= l["peso_vol_u"] else "pv") if l["tiene_bultos"] else None,
        ]
        for ci, val in enumerate(vals, 1):
            cell = ws.cell(row=ri, column=ci, value=val)
            cell.fill = PatternFill("solid", fgColor=bg)
            cell.font  = Font(size=9, bold=(ci == 9), color=(RED if ci == 9 else "000000"))
            cell.alignment = Alignment(
                horizontal="center" if ci in (3, 4, 10) else ("right" if ci >= 5 else "left"),
                vertical="center",
            )
            if ci in (5, 6, 7, 8, 9) and val is not None:
                cell.number_format = "#,##0.000"

    # ── Fila de totales ──────────────────────────────────────────────
    tr = row_offset + len(lineas)
    ws.merge_cells(f"A{tr}:B{tr}")
    ws.cell(row=tr, column=1, value="TOTALES").font = Font(bold=True, color="FFFFFF", size=9)
    ws.cell(row=tr, column=1).fill = PatternFill("solid", fgColor=BLACK)
    ws.cell(row=tr, column=1).alignment = Alignment(horizontal="right")

    totales = {
        3: int(sum(l["cantidad"]     for l in lineas)),
        5: sum(l["peso_kg_tot"]      for l in lineas),
        6: sum(l["peso_vol_tot"]     for l in lineas),
        7: sum(l["vol_tot"]          for l in lineas),
        9: sum(l["pred_tot"]         for l in lineas),
    }
    for ci in range(1, 11):
        cell = ws.cell(row=tr, column=ci)
        cell.fill = PatternFill("solid", fgColor=BLACK)
        if ci in totales:
            cell.value = totales[ci]
            cell.font  = Font(bold=True, color=("CC0000" if ci == 9 else "FFFFFF"), size=9)
            cell.alignment = Alignment(horizontal="center" if ci == 3 else "right")
            if ci != 3:
                cell.number_format = "#,##0.000"

    # ── Anchos de columna ────────────────────────────────────────────
    for ci, w in enumerate([14, 42, 7, 8, 10, 10, 12, 12, 14, 7], 1):
        ws.column_dimensions[get_column_letter(ci)].width = w

    ws.freeze_panes = f"A{hdr_row + 1}"

    buf = BytesIO()
    wb.save(buf)
    buf.seek(0)

    fname = "cubicador_" + "_".join(f"{t}{n}" for t, n in docs) + ".xlsx"
    return send_file(
        buf,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        as_attachment=True,
        download_name=fname[:80],
    )


@app.route("/cubicador/export/pdf", methods=["POST"])
@login_required
def cubicador_export_pdf():
    """Descarga el resultado del cubicador como PDF elegante — soporta múltiples documentos."""
    if not g.permissions.get("cubicador"):
        flash("No tienes acceso al módulo Cubicador.", "danger")
        return redirect(url_for("index"))

    docs = _parse_docs_from_form(request.form)
    if not docs:
        flash("Debes ingresar al menos un documento.", "warning")
        return redirect(url_for("cubicador"))

    try:
        headers, lineas, errors = _fetch_multi_docs(docs)
    except Exception as ex:
        flash(f"Error al consultar el ERP: {ex}", "danger")
        return redirect(url_for("cubicador"))

    if not headers:
        flash(" · ".join(errors) if errors else "No se encontraron documentos.", "warning")
        return redirect(url_for("cubicador"))

    # ── Logo ILUS ────────────────────────────────────────────────────
    import os as _os
    _logo_path = _os.path.join(_os.path.dirname(__file__), "static", "logo_pdf.txt")
    try:
        with open(_logo_path) as _f:
            logo_b64 = _f.read().strip()
        logo_tag = f'<img src="data:image/png;base64,{logo_b64}" style="height:46px;display:block">'
    except Exception:
        logo_tag = '<div style="font-size:18pt;font-weight:900;color:#CC0000;line-height:1">ILUS</div>'

    from datetime import datetime as _dt
    fecha_gen = _dt.now().strftime("%d/%m/%Y %H:%M")

    # ── Título principal ─────────────────────────────────────────────
    if len(docs) == 1 and headers:
        h0 = headers[0]
        main_title = f"{h0['tido']} N° {h0.get('nudo_display', h0.get('nudo',''))} — Cubicador ILUS"
    else:
        main_title = f"Cubicador ILUS · {len(docs)} documentos"

    # ── Tarjetas de documentos ───────────────────────────────────────
    docs_html = ""
    for hdr in headers:
        docs_html += f"""
        <div class="doc-card">
          <div class="doc-title">{hdr['tido']} N° {hdr.get('nudo_display', hdr.get('nudo',''))}</div>
          <div class="doc-fecha">{hdr.get('fecha','')}</div>
          <div class="doc-cliente">{hdr.get('cliente_nombre','—')}</div>
          <div class="doc-rut">RUT {hdr.get('cliente_rut','—')}</div>
          <div class="doc-bruto">${hdr.get('valor_bruto',0):,.0f}</div>
        </div>"""

    # ── Totales ──────────────────────────────────────────────────────
    tot_qty  = sum(l["cantidad"]     for l in lineas)
    tot_kg   = sum(l["peso_kg_tot"]  for l in lineas)
    tot_pv   = sum(l["peso_vol_tot"] for l in lineas)
    tot_vol  = sum(l["vol_tot"]      for l in lineas)
    tot_pred = sum(l["pred_tot"]     for l in lineas)
    tot_bult = sum(l["total_bultos"] for l in lineas)

    # ── Filas de la tabla ────────────────────────────────────────────
    rows_html = ""
    for i, l in enumerate(lineas):
        bg = "#f7f7f7" if i % 2 == 0 else "#ffffff"
        sf = not l["tiene_bultos"]
        rows_html += f"""
        <tr style="background:{bg}">
          <td class="mono">{l['sku']}</td>
          <td>{l['descripcion_erp']}</td>
          <td class="c">{int(l['cantidad'])}</td>
          <td class="c">{l['total_bultos'] if l['tiene_ficha'] else 's/f'}</td>
          <td class="r">{'—' if sf else f"{l['peso_kg_u']:.3f}"}</td>
          <td class="r">{'—' if sf else f"{l['peso_vol_u']:.3f}"}</td>
          <td class="r">{'—' if sf else f"{l['vol_u']:,.0f}"}</td>
          <td class="r fw red">{'—' if sf else f"{l['pred_tot']:.3f}"}</td>
        </tr>"""

    html = f"""<!DOCTYPE html><html><head><meta charset="utf-8">
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:Arial,sans-serif;font-size:8.5pt;color:#1a1a1a;padding:16px 18px}}

/* ── Cabecera de página ── */
.page-hdr{{background:#1a1a1a;padding:12px 16px;border-radius:6px 6px 0 0;
           display:flex;justify-content:space-between;align-items:center}}
.page-hdr .main-title{{font-size:12.5pt;color:#fff;font-weight:900;margin:0}}
.page-hdr .sub{{font-size:7pt;color:#aaa;margin-top:3px}}
.page-hdr .red-bar{{width:4px;background:#CC0000;height:36px;border-radius:2px;margin-right:10px;flex-shrink:0}}
.page-hdr .left{{display:flex;align-items:center}}

/* ── Franja de documentos ── */
.docs-strip{{background:#f2f2f2;border:1px solid #ddd;border-top:none;
             border-radius:0 0 6px 6px;padding:8px 14px;margin-bottom:14px;
             display:flex;flex-wrap:wrap;gap:8px}}
.doc-card{{background:#fff;border:1px solid #ddd;border-left:3px solid #CC0000;
           border-radius:0 4px 4px 0;padding:7px 10px;flex:1;min-width:150px;max-width:250px}}
.doc-title{{font-weight:900;font-size:8.5pt;color:#CC0000}}
.doc-fecha{{font-size:6.8pt;color:#888;margin-bottom:4px}}
.doc-cliente{{font-weight:700;font-size:8pt;color:#1a1a1a}}
.doc-rut{{font-size:6.8pt;color:#888}}
.doc-bruto{{font-size:9.5pt;font-weight:900;color:#1a1a1a;margin-top:4px}}

/* ── Tabla ── */
table{{width:100%;border-collapse:collapse;font-size:7.8pt}}
th{{background:#1a1a1a;color:#fff;padding:6px 5px;text-align:left;
    font-size:7pt;text-transform:uppercase;letter-spacing:.3px}}
td{{padding:5px;border-bottom:1px solid #ebebeb;vertical-align:middle}}
.c{{text-align:center}}.r{{text-align:right}}
.mono{{font-family:monospace;font-weight:bold}}.fw{{font-weight:bold}}.red{{color:#CC0000}}
tfoot tr{{background:#1a1a1a!important;color:#fff;font-weight:bold}}
tfoot td{{border:none;padding:7px 5px}}

/* ── Barra de totales ── */
.totals-bar{{margin-top:12px;background:#1a1a1a;border-radius:6px;
             padding:10px 16px;display:flex;gap:0;align-items:center}}
.t-item{{text-align:center;flex:1;padding:0 8px;border-right:1px solid #333}}
.t-item:last-child{{border-right:none}}
.t-label{{font-size:6pt;color:#888;text-transform:uppercase;letter-spacing:.5px;margin-bottom:2px}}
.t-val{{font-size:11pt;font-weight:900;color:#fff;line-height:1.1}}
.t-pred .t-val{{color:#CC0000;font-size:13pt}}

/* ── Footer ── */
.foot{{margin-top:10px;font-size:6.5pt;color:#aaa;text-align:right}}
</style></head><body>

<div class="page-hdr">
  <div class="left">
    <div class="red-bar"></div>
    <div>
      <div class="main-title">{main_title}</div>
      <div class="sub">Generado el {fecha_gen}</div>
    </div>
  </div>
  <div>{logo_tag}</div>
</div>

<div class="docs-strip">
  {docs_html}
</div>

<table>
  <thead><tr>
    <th>SKU</th><th>Descripción ERP</th>
    <th class="c">Cant</th><th class="c">Bultos</th>
    <th class="r">Kg/u</th><th class="r">PV/u</th>
    <th class="r">Vol cm³</th>
    <th class="r" style="color:#CC0000">Total Predom</th>
  </tr></thead>
  <tbody>{rows_html}</tbody>
  <tfoot><tr>
    <td colspan="2" style="text-align:right;color:#bbb;font-size:7pt;letter-spacing:.5px">TOTALES</td>
    <td class="c">{int(tot_qty)}</td>
    <td class="c">{tot_bult}</td>
    <td class="r">{tot_kg:.3f}</td>
    <td class="r">{tot_pv:.3f}</td>
    <td class="r">{tot_vol:,.0f}</td>
    <td class="r red" style="font-size:9pt">{tot_pred:.3f}</td>
  </tr></tfoot>
</table>

<div class="totals-bar">
  <div class="t-item">
    <div class="t-label">Unidades</div>
    <div class="t-val">{int(tot_qty)}</div>
  </div>
  <div class="t-item">
    <div class="t-label">Bultos</div>
    <div class="t-val">{tot_bult}</div>
  </div>
  <div class="t-item">
    <div class="t-label">Total Kg</div>
    <div class="t-val">{tot_kg:.3f}</div>
  </div>
  <div class="t-item">
    <div class="t-label">Total PV</div>
    <div class="t-val">{tot_pv:.3f}</div>
  </div>
  <div class="t-item">
    <div class="t-label">Vol cm³</div>
    <div class="t-val">{tot_vol:,.0f}</div>
  </div>
  <div class="t-item t-pred">
    <div class="t-label">Predominante</div>
    <div class="t-val">{tot_pred:.3f}</div>
  </div>
</div>

<div class="foot">ILUS Sport &amp; Health · Sistema de Gestión de Productos</div>
</body></html>"""

    from io import BytesIO as _BytesIO

    pdf_bytes = _pw_pdf(
        html,
        page_format = "A4",
        margin = {"top": "12mm", "bottom": "12mm", "left": "14mm", "right": "14mm"},
    )

    fname = "cubicador_" + "_".join(f"{t}{n}" for t, n in docs) + ".pdf"
    return send_file(
        _BytesIO(pdf_bytes),
        mimetype="application/pdf",
        as_attachment=True,
        download_name=fname[:80],
    )


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


# ═══════════════════════════════════════════════════════════════
#  MÓDULO: TRANSPORTE Y DISTRIBUCIÓN
# ═══════════════════════════════════════════════════════════════

ZZ_SKUS = {'ZZenvio', 'ZZINGREPUESTO', 'ZZSERVTEC', 'ZZRetiro', 'ZZINSTALACION', 'ZZINGARREQUIP'}

ESTADOS_COMPROMISO = [
    'Pendiente', 'En proceso', 'Despachado', 'Problema',
    'Pedido de vuelta', 'Preventa', 'Indemnización', 'Garantía',
    'Logística inversa', 'Prioridad', 'Indemnización revisada',
    'Indemnización rechazada', 'Regalo', 'Reentrega',
]
COURIERS = [
    'FedEx', 'Envíame', 'Transportes Milling', 'Starken',
    'Daniel Pulgar', 'Servicio Técnico', 'Transportes Felca', 'Dropit', 'Clickex',
]
ESTADOS_ENTREGA = [
    'En preparación', 'Entregado a transporte',
    'En ruta', 'Entregado', 'Entrega fallida', 'Devolución',
]

ESTADO_COLORS = {
    'Pendiente':              'warning',
    'En proceso':             'primary',
    'Despachado':             'success',
    'Problema':               'danger',
    'Pedido de vuelta':       'danger',
    'Preventa':               'secondary',
    'Indemnización':          'danger',
    'Garantía':               'info',
    'Logística inversa':      'secondary',
    'Prioridad':              'danger',
    'Indemnización revisada': 'warning',
    'Indemnización rechazada':'danger',
    'Regalo':                 'info',
    'Reentrega':              'warning',
}


def _tr_log(entity_type, entity_id, accion, detalle=""):
    """Registra un evento de trazabilidad en transport_logs."""
    try:
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO transport_logs (entity_type,entity_id,accion,detalle,usuario) "
                "VALUES (%s,%s,%s,%s,%s)",
                (entity_type, entity_id, accion, detalle, current_username())
            )
        conn.commit()
    except Exception:
        pass


def _parse_obdo(obdo: str) -> dict:
    """Parsea texto libre OBDO: 'Dirección - teléfono - email'"""
    import re as _re
    result = {"direccion": "", "telefono": "", "email": ""}
    if not obdo:
        return result
    email_m = _re.search(r'[\w.+-]+@[\w-]+\.[a-zA-Z]{2,}', obdo)
    if email_m:
        result["email"] = email_m.group(0)
        obdo = obdo.replace(email_m.group(0), "")
    phone_m = _re.search(r'\b[\+56]*\s*[29]\d{7,8}\b', obdo)
    if phone_m:
        result["telefono"] = phone_m.group(0).strip()
        obdo = obdo.replace(phone_m.group(0), "")
    result["direccion"] = _re.sub(r'[-–]+\s*$', '', obdo).strip(' -–')
    return result


def _clasif_from_skus(skus):
    """Determina la clasificación de un compromiso según sus SKUs ZZ."""
    skus = [s.strip().upper() for s in skus if s]
    if not skus: return "despacho"
    if all(s == "ZZRETIRO" for s in skus): return "retiro"
    if any(s == "ZZINSTALACION" for s in skus): return "instalacion"
    if any(s in ("ZZSERVTEC", "ZZINGREPUESTO", "ZZINGARREQUIP") for s in skus): return "mantencion"
    return "despacho"


def _tr_fetch_from_erp(tido, nudo):
    """
    Obtiene un documento del ERP vía API y lo guarda/actualiza en transport_commitments.
    Usa la misma lógica de _cubicador_fetch pero orientada a transporte.
    Retorna el id del commitment o None.
    """
    from datetime import datetime as _dt
    TOKEN  = ERP_CONFIG.get("api_token", "")
    nudos  = _nudo_variants(nudo)

    # Mapear VD/WEB → NVV igual que cubicador
    if tido in _ERP_TIDO_NUDO_MAP:
        erp_tido, nudo_fn = _ERP_TIDO_NUDO_MAP[tido]
        erp_nudo = nudo_fn(nudo)
        nudos = _nudo_variants(erp_nudo)
    else:
        erp_tido = tido

    raw_header, raw_lineas = None, []
    for nv in nudos:
        try:
            body = _erp_get("/documentos/render",
                            {"tido": erp_tido, "nudo": nv, "empresa": "01"},
                            TOKEN, timeout=12)
            data = body.get("data") or []
            if data:
                raw_header = data[0].get("maeedo") or {}
                raw_lineas = data[0].get("maeddo") or []
                break
        except Exception as e:
            raise ConnectionError(f"ERP no responde: {e}")

    if not raw_header:
        return None, "No encontrado en ERP"

    # Filtrar sólo líneas ZZ
    zz_lines = [l for l in raw_lineas
                if (l.get("KOPRCT") or "").strip().upper() in {s.upper() for s in ZZ_SKUS}]
    if not zz_lines:
        return None, "Documento sin líneas ZZ"

    # Calcular saldo
    saldo_total = sum(
        float(l.get("CAPRCO1") or 0) - float(l.get("CAPRAD1") or 0)
        for l in zz_lines
    )
    tiene_saldo = 1 if saldo_total > 0 else 0

    # Parsear OBDO
    obdo_str = (raw_header.get("OBDO") or raw_header.get("TEXTO1") or "").strip()
    parsed   = _parse_obdo(obdo_str)
    direccion = parsed["direccion"] or (raw_header.get("DIENDESP") or "").strip()

    # Nombre cliente
    endo = (raw_header.get("ENDO") or "").strip()
    cliente_nombre = (raw_header.get("NOKOEN") or "").strip().title()
    if not cliente_nombre and endo:
        try:
            ent = _erp_get("/entidades", {"rten": endo}, TOKEN, timeout=6)
            ed  = (ent.get("data") or [{}])[0]
            cliente_nombre = (ed.get("NOKOEN") or "").strip().title()
        except Exception:
            pass

    # Fecha
    from datetime import datetime as _dt
    def _parse_date(s):
        if not s: return None
        try: return _dt.fromisoformat(s.replace("Z", "+00:00")).date()
        except: return None

    fecha_em  = _parse_date(raw_header.get("FEEMDO"))
    fecha_ent = _parse_date(raw_header.get("FEER"))

    # Clasificación — basado en los SKUs ZZ predominantes
    skus_upper = [(l.get("KOPRCT") or "").strip().upper() for l in zz_lines]
    clasificacion = _clasif_from_skus(skus_upper)

    # Costo ZZ (suma de PPPRNE de líneas ZZ)
    costo_zz = sum(float(l.get("PPPRNE") or 0) for l in zz_lines)

    # Guía (si CAPRAD1 >= CAPRCO1 en todas → tiene guía)
    guia_numero = (raw_header.get("NUDO_GIA") or raw_header.get("NUDGIA") or "").strip() or None

    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO transport_commitments
                  (tido,nudo,endo,fecha_emision,fecha_entrega,cliente_nombre,cliente_rut,
                   comuna,direccion,telefono,email,valor_neto,valor_bruto,costo_zz,
                   tiene_saldo,guia_numero,clasificacion,erp_synced_at,created_by,updated_by)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),%s,%s)
                ON DUPLICATE KEY UPDATE
                  fecha_emision=VALUES(fecha_emision), fecha_entrega=VALUES(fecha_entrega),
                  cliente_nombre=VALUES(cliente_nombre), cliente_rut=VALUES(cliente_rut),
                  comuna=VALUES(comuna), direccion=VALUES(direccion),
                  telefono=VALUES(telefono), email=VALUES(email),
                  valor_neto=VALUES(valor_neto), valor_bruto=VALUES(valor_bruto),
                  costo_zz=CASE WHEN costo_zz=0 THEN VALUES(costo_zz) ELSE costo_zz END,
                  tiene_saldo=VALUES(tiene_saldo), guia_numero=VALUES(guia_numero),
                  clasificacion=VALUES(clasificacion), erp_synced_at=NOW(),
                  updated_by=VALUES(updated_by)
            """, (
                tido, str(nudo), endo, fecha_em, fecha_ent,
                cliente_nombre, endo,
                (raw_header.get("CMEN") or raw_header.get("NOKOZO") or
                 raw_header.get("NOKOCOMU") or raw_header.get("NOKOCOMUNADE") or
                 raw_header.get("NOKOMUENDE") or raw_header.get("NOKOMUNEN") or
                 raw_header.get("NOKCOMENDESP") or "").strip(),
                direccion, parsed["telefono"], parsed["email"],
                float(raw_header.get("VANEDO") or 0),
                float(raw_header.get("VABRDO") or 0),
                costo_zz, tiene_saldo, guia_numero, clasificacion,
                current_username(), current_username()
            ))
            comm_id = cur.lastrowid or mysql_fetchone(
                "SELECT id FROM transport_commitments WHERE tido=%s AND nudo=%s",
                (tido, str(nudo))
            )["id"]

            # Líneas ZZ
            cur.execute("DELETE FROM transport_commitment_lines WHERE commitment_id=%s", (comm_id,))
            for l in zz_lines:
                cant  = float(l.get("CAPRCO1") or 0)
                cantd = float(l.get("CAPRAD1")  or 0)
                cur.execute("""
                    INSERT INTO transport_commitment_lines
                      (commitment_id,koprct,nokopr,cantidad,cant_despachada,saldo,bodega)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                """, (comm_id,
                      (l.get("KOPRCT") or "").strip().upper(),
                      (l.get("NOKOPR") or "").strip(),
                      cant, cantd, cant - cantd,
                      (l.get("BOSULIDO") or "").strip()))
        conn.commit()
    finally:
        conn.close()

    return comm_id, None


# ── SYNC MASIVO ERP → TRANSPORTE ────────────────────────────────

def _tr_bulk_sync_erp_mysql(fecha_desde, fecha_hasta):
    """
    Sincronización masiva desde el ERP vía MySQL directo.
    Trae todos los documentos con líneas ZZ y saldo pendiente en el rango.
    Retorna (count_ok, list_errors).
    """
    conn_erp = get_erp_conn()
    if not conn_erp:
        return 0, ["No se pudo conectar al ERP (MySQL)"]

    zz_list      = list(ZZ_SKUS)
    zz_in        = ",".join(["%s"] * len(zz_list))
    TIDOS_VALIDOS = ("FCV", "BLV", "GDV", "NVV", "NVI", "COV")
    tido_in      = ",".join(["%s"] * len(TIDOS_VALIDOS))

    try:
        with conn_erp.cursor() as cur:
            cur.execute(f"""
                SELECT
                    h.TIDO, h.NUDO, h.ENDO, h.FEEMDO, h.FEER,
                    h.NOKOEN,
                    COALESCE(h.OBDO, h.TEXTO1, '') AS OBDO,
                    COALESCE(h.DIENDESP, '')        AS DIENDESP,
                    COALESCE(h.VANEDO, 0)           AS VANEDO,
                    COALESCE(h.VABRDO, 0)           AS VABRDO,
                    COALESCE(h.NOKOZO, h.CMEN, h.NOKOCOMU, h.NOKOCOMUNADE,
                             h.NOKOMUENDE, h.NOKOMUNEN, h.NOKCOMENDESP, '') AS COMUNA,
                    COALESCE(h.NUDGIA, '')           AS NUDGIA,
                    SUM(GREATEST(d.CAPRCO1 - COALESCE(d.CAPRAD1, 0), 0)) AS saldo_zz,
                    SUM(COALESCE(d.PPPRNE, 0))       AS costo_zz_sum,
                    GROUP_CONCAT(DISTINCT UPPER(d.KOPRCT) ORDER BY d.KOPRCT) AS zz_skus
                FROM MAEEDO h
                JOIN MAEDDO d ON d.TIDO = h.TIDO AND d.NUDO = h.NUDO
                WHERE d.KOPRCT IN ({zz_in})
                  AND h.TIDO IN ({tido_in})
                  AND h.FEEMDO BETWEEN %s AND %s
                  AND (d.CAPRCO1 - COALESCE(d.CAPRAD1, 0)) > 0
                GROUP BY h.TIDO, h.NUDO
                HAVING saldo_zz > 0
                ORDER BY h.FEEMDO DESC
                LIMIT 500
            """, zz_list + list(TIDOS_VALIDOS) + [fecha_desde, fecha_hasta])
            rows = cur.fetchall()
    except Exception as exc:
        return 0, [f"Error al consultar ERP: {exc}"]
    finally:
        conn_erp.close()

    count, errs = 0, []
    local_conn = get_mysql()
    try:
        for row in rows:
            try:
                tido  = (row.get("TIDO") or "").strip()
                nudo  = (row.get("NUDO") or "").strip()
                endo  = (row.get("ENDO") or "").strip()
                nombre = (row.get("NOKOEN") or "").strip().title()
                obdo  = (row.get("OBDO") or "").strip()
                parsed = _parse_obdo(obdo)
                dir_  = parsed["direccion"] or (row.get("DIENDESP") or "").strip()
                tel   = parsed["telefono"]
                mail  = parsed["email"]
                comuna = (row.get("COMUNA") or "").strip()
                vneto  = float(row.get("VANEDO") or 0)
                vbruto = float(row.get("VABRDO") or 0)
                costo_zz = float(row.get("costo_zz_sum") or 0)
                guia  = (row.get("NUDGIA") or "").strip() or None
                zz_present = (row.get("zz_skus") or "").upper()
                _zz_list_bulk = [s.strip() for s in zz_present.split(",") if s.strip()]
                clasif = _clasif_from_skus(_zz_list_bulk) if _zz_list_bulk else "despacho"

                def _pd(val):
                    if not val: return None
                    if hasattr(val, "date"): return val.date()
                    try:
                        from datetime import datetime as _dt
                        return _dt.fromisoformat(str(val).replace("Z", "")).date()
                    except Exception:
                        return None

                fecha_em  = _pd(row.get("FEEMDO"))
                fecha_ent = _pd(row.get("FEER"))

                with local_conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO transport_commitments
                          (tido,nudo,endo,fecha_emision,fecha_entrega,
                           cliente_nombre,cliente_rut,
                           comuna,direccion,telefono,email,
                           valor_neto,valor_bruto,costo_zz,
                           tiene_saldo,guia_numero,clasificacion,
                           erp_synced_at,created_by,updated_by)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,1,%s,%s,NOW(),'sync','sync')
                        ON DUPLICATE KEY UPDATE
                          fecha_emision =VALUES(fecha_emision),
                          fecha_entrega =VALUES(fecha_entrega),
                          cliente_nombre=VALUES(cliente_nombre),
                          cliente_rut   =VALUES(cliente_rut),
                          comuna        =VALUES(comuna),
                          direccion     =VALUES(direccion),
                          telefono      =VALUES(telefono),
                          email         =VALUES(email),
                          valor_neto    =VALUES(valor_neto),
                          valor_bruto   =VALUES(valor_bruto),
                          costo_zz      =CASE WHEN costo_zz=0 THEN VALUES(costo_zz) ELSE costo_zz END,
                          tiene_saldo   =1,
                          guia_numero   =VALUES(guia_numero),
                          clasificacion =VALUES(clasificacion),
                          erp_synced_at =NOW()
                    """, (tido, nudo, endo, fecha_em, fecha_ent,
                          nombre, endo, comuna, dir_, tel, mail,
                          vneto, vbruto, costo_zz, guia, clasif))
                local_conn.commit()
                count += 1
            except Exception as e2:
                errs.append(f"{row.get('TIDO')} {row.get('NUDO')}: {e2}")
    finally:
        local_conn.close()

    return count, errs or None


def _tr_import_from_excel(file_bytes, filename):
    """
    Importa documentos desde un Excel/CSV exportado del ERP.
    Detecta columnas automáticamente.
    Retorna (count_ok, list_errors, preview_rows).
    """
    import io

    # ── intentar openpyxl (xlsx) o csv ──
    rows_raw = []
    try:
        import openpyxl
        wb = openpyxl.load_workbook(io.BytesIO(file_bytes), data_only=True)
        ws = wb.active
        headers = [str(c.value or "").strip().upper() for c in next(ws.iter_rows(min_row=1, max_row=1))]
        for row in ws.iter_rows(min_row=2, values_only=True):
            rows_raw.append(dict(zip(headers, [str(v or "").strip() for v in row])))
    except Exception:
        # Fallback: CSV
        import csv
        decoded = file_bytes.decode("utf-8-sig", errors="replace")
        reader  = csv.DictReader(io.StringIO(decoded))
        for row in reader:
            rows_raw.append({k.strip().upper(): str(v or "").strip() for k, v in row.items()})

    if not rows_raw:
        return 0, ["Archivo vacío o formato no reconocido"], []

    # ── mapeo flexible de columnas ──
    COL_MAP = {
        "tido":   ["TIDO", "TIPO", "TIPO DOCUMENTO", "TIPODOC", "TIPO DOC"],
        "nudo":   ["NUDO", "NUMERO", "N°", "NDOC", "NUM DOC", "NUMERO DOC", "FOLIO"],
        "nombre": ["NOKOEN", "NOMBRE", "CLIENTE", "RAZON SOCIAL", "NOMBRE CLIENTE"],
        "fecha":  ["FEEMDO", "FECHA", "FECHA EMISION", "FECHA EMISIÓN", "FECHA DOC"],
        "sku":    ["KOPRCT", "SKU", "CODIGO", "CÓDIGO", "PRODUCTO"],
        "cant":   ["CAPRCO1", "CANTIDAD", "QTY", "CANT", "CANT PEDIDA"],
        "cantd":  ["CAPRAD1", "DESPACHADO", "DESPACHO", "CANT DESPACHADA", "ENTREGADO"],
        "costo":  ["PPPRNE", "PRECIO", "VALOR", "COSTO", "MONTO"],
        "comuna": ["CMEN", "NOKOZO", "COMUNA", "CIUDAD"],
        "dir":    ["DIENDESP", "DIRECCION", "DIRECCIÓN", "OBDO"],
        "rut":    ["ENDO", "RUT", "RUTEN", "RUT CLIENTE"],
    }

    def find_col(row, candidates):
        for c in candidates:
            if c in row:
                return row[c]
        return ""

    # Agrupar por TIDO+NUDO
    from collections import defaultdict
    docs = defaultdict(lambda: {"lineas": [], "header": {}})
    skipped = 0
    for row in rows_raw:
        tido  = find_col(row, COL_MAP["tido"]).upper().strip()
        nudo  = find_col(row, COL_MAP["nudo"]).strip()
        sku   = find_col(row, COL_MAP["sku"]).upper().strip()
        if not tido or not nudo:
            skipped += 1
            continue
        key = (tido, nudo)
        docs[key]["header"] = row
        if sku in {s.upper() for s in ZZ_SKUS}:
            docs[key]["lineas"].append(row)

    # Filtrar docs que tienen al menos una línea ZZ con saldo
    count, errs, preview = 0, [], []
    local_conn = get_mysql()
    try:
        for (tido, nudo), doc in docs.items():
            if not doc["lineas"]:
                continue  # sin líneas ZZ → ignorar
            hrow = doc["header"]
            nombre  = find_col(hrow, COL_MAP["nombre"]).title()
            fecha_s = find_col(hrow, COL_MAP["fecha"])
            comuna  = find_col(hrow, COL_MAP["comuna"])
            dir_    = find_col(hrow, COL_MAP["dir"])
            rut     = find_col(hrow, COL_MAP["rut"])
            costo   = sum(
                _safe_float(find_col(l, COL_MAP["costo"]))
                for l in doc["lineas"]
            )
            saldo   = sum(
                max(_safe_float(find_col(l, COL_MAP["cant"])) -
                    _safe_float(find_col(l, COL_MAP["cantd"])), 0)
                for l in doc["lineas"]
            )
            if saldo <= 0:
                continue
            zz_skus = {find_col(l, COL_MAP["sku"]).upper() for l in doc["lineas"]}
            clasif  = "retiro" if zz_skus == {"ZZRETIRO"} else "despacho"

            # Parsear fecha
            fecha_em = None
            if fecha_s:
                for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%d/%m/%y"):
                    try:
                        from datetime import datetime as _dt
                        fecha_em = _dt.strptime(fecha_s[:10], fmt).date()
                        break
                    except Exception:
                        continue

            try:
                with local_conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO transport_commitments
                          (tido,nudo,endo,fecha_emision,cliente_nombre,cliente_rut,
                           comuna,direccion,costo_zz,tiene_saldo,clasificacion,
                           erp_synced_at,created_by,updated_by)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,1,%s,NOW(),'excel','excel')
                        ON DUPLICATE KEY UPDATE
                          fecha_emision =VALUES(fecha_emision),
                          cliente_nombre=VALUES(cliente_nombre),
                          cliente_rut   =VALUES(cliente_rut),
                          comuna        =VALUES(comuna),
                          direccion     =VALUES(direccion),
                          costo_zz      =CASE WHEN costo_zz=0 THEN VALUES(costo_zz) ELSE costo_zz END,
                          tiene_saldo   =1,
                          clasificacion =VALUES(clasificacion),
                          erp_synced_at =NOW()
                    """, (tido, nudo, rut, fecha_em, nombre, rut,
                          comuna, dir_, costo, clasif))
                local_conn.commit()
                count += 1
                if len(preview) < 5:
                    preview.append({"tido": tido, "nudo": nudo, "cliente": nombre,
                                    "clasif": clasif, "saldo_lineas": saldo})
            except Exception as e2:
                errs.append(f"{tido} {nudo}: {e2}")
    finally:
        local_conn.close()

    return count, errs or None, preview


def _safe_float(v):
    try:
        return float(str(v).replace(",", ".").strip() or 0)
    except Exception:
        return 0.0


# ── RUTAS TRANSPORTE ─────────────────────────────────────────────

def _tr_required(fn):
    """Decorator: requiere permiso transporte."""
    from functools import wraps
    @wraps(fn)
    @login_required
    def wrapper(*a, **kw):
        if not g.permissions.get("transporte"):
            flash("Sin acceso al módulo Transporte.", "danger")
            return redirect(url_for("index"))
        return fn(*a, **kw)
    return wrapper


@app.route("/transporte/api/sync", methods=["POST"])
@_tr_required
def tr_sync():
    """Sincronización masiva desde ERP MySQL. Recibe fecha_desde y fecha_hasta."""
    data = request.get_json(silent=True) or {}
    fecha_desde = data.get("fecha_desde") or ""
    fecha_hasta = data.get("fecha_hasta") or ""

    # Validar fechas
    from datetime import datetime as _dt, date as _date, timedelta as _td
    try:
        fd = _dt.strptime(fecha_desde, "%Y-%m-%d").date()
        fh = _dt.strptime(fecha_hasta, "%Y-%m-%d").date()
    except Exception:
        # Default: últimos 60 días
        fh = _date.today()
        fd = fh - _td(days=60)

    count, errs = _tr_bulk_sync_erp_mysql(str(fd), str(fh))
    _tr_log("commitment", 0, "sync_masivo",
            f"ERP MySQL {fd}→{fh}: {count} importados, {len(errs or [])} errores")
    return jsonify({
        "ok": True,
        "importados": count,
        "errores": (errs or [])[:10],
        "rango": f"{fd.strftime('%d/%m/%Y')} → {fh.strftime('%d/%m/%Y')}",
    })


@app.route("/transporte/api/compromisos")
@_tr_required
def tr_compromisos_json():
    """Versión JSON del monitor para carga AJAX."""
    from datetime import date as _date, timedelta as _td
    periodo = request.args.get("periodo", "")
    hoy = _date.today()
    _def_desde = (hoy - _td(days=30)).strftime("%Y-%m-%d")
    _def_hasta = hoy.strftime("%Y-%m-%d")
    if periodo == "mes":   fecha_desde, fecha_hasta = _def_desde, _def_hasta
    elif periodo == "3m":  fecha_desde, fecha_hasta = (hoy-_td(days=90)).strftime("%Y-%m-%d"), _def_hasta
    elif periodo == "año": fecha_desde, fecha_hasta = hoy.strftime("%Y-01-01"), _def_hasta
    elif periodo == "todo": fecha_desde = fecha_hasta = ""
    else:
        fecha_desde = request.args.get("fecha_desde", _def_desde).strip()
        fecha_hasta = request.args.get("fecha_hasta", _def_hasta).strip()

    estado = request.args.get("estado","")
    clasif = request.args.get("clasificacion","")
    q      = request.args.get("q","").strip()

    where, params = ["tiene_saldo=1"], []
    if estado: where.append("estado=%s"); params.append(estado)
    if clasif: where.append("clasificacion=%s"); params.append(clasif)
    if q:
        where.append("(cliente_nombre LIKE %s OR nudo LIKE %s OR tido LIKE %s OR comuna LIKE %s)")
        qp = f"%{q}%"; params += [qp,qp,qp,qp]
    if fecha_desde: where.append("fecha_emision >= %s"); params.append(fecha_desde)
    if fecha_hasta: where.append("fecha_emision <= %s"); params.append(fecha_hasta)

    rows = mysql_fetchall(
        "SELECT * FROM transport_commitments WHERE " + " AND ".join(where) +
        " ORDER BY fecha_emision DESC LIMIT 500", tuple(params)
    )

    result = []
    for r in rows:
        result.append({
            "id":           r["id"],
            "tido":         r["tido"],
            "nudo":         r["nudo"],
            "fecha":        r["fecha_emision"].strftime("%d/%m/%Y") if r["fecha_emision"] else "",
            "cliente":      r["cliente_nombre"] or "—",
            "rut":          r["cliente_rut"] or "",
            "comuna":       r["comuna"] or "—",
            "estado":       r["estado"] or "Pendiente",
            "costo_zz":     float(r["costo_zz"] or 0),
            "clasificacion":r["clasificacion"] or "despacho",
        })
    return jsonify({"ok": True, "compromisos": result, "total": len(result)})


@app.route("/transporte/api/inline-bulto", methods=["POST"])
@_tr_required
def tr_inline_bulto():
    """Guarda medidas de un producto inline desde el modal de detalle.
    Acepta 'bultos': [{largo,ancho,alto,peso}, ...] o campos sueltos (compat.).
    """
    data   = request.get_json(silent=True) or {}
    sku    = (data.get("sku") or "").strip().upper()
    nombre = (data.get("nombre") or "").strip()

    if not sku:
        return jsonify({"error": "Falta el SKU"}), 400

    # Aceptar lista de bultos O campos sueltos (retrocompat.)
    bultos_raw = data.get("bultos") or []
    if not bultos_raw:
        # compatibilidad con versión anterior (campos individuales)
        if data.get("largo"):
            bultos_raw = [{
                "largo": data.get("largo"), "ancho": data.get("ancho"),
                "alto":  data.get("alto"),  "peso":  data.get("peso"),
            }]

    bultos = []
    for b in bultos_raw:
        l = float(b.get("largo") or 0)
        a = float(b.get("ancho") or 0)
        h = float(b.get("alto")  or 0)
        p = float(b.get("peso")  or 0)
        if l and a and h and p:
            bultos.append((l, a, h, p))

    if not bultos:
        return jsonify({"error": "Debes ingresar al menos un bulto con todos sus campos"}), 400

    conn = get_db()
    try:
        with conn.cursor() as cur:
            # Buscar o crear producto
            cur.execute(f"SELECT id FROM `{PRODUCTS_TABLE}` WHERE UPPER(TRIM(sku))=%s LIMIT 1", (sku,))
            row = cur.fetchone()
            if row:
                pid = row["id"]
            else:
                cur.execute(
                    f"INSERT INTO `{PRODUCTS_TABLE}` (sku, nombre, estado, created_by, updated_by) "
                    f"VALUES (%s,%s,'activo',%s,%s)",
                    (sku, nombre or sku, current_username(), current_username())
                )
                pid = cur.lastrowid

            # Reemplazar bultos existentes
            cur.execute(f"DELETE FROM `{BULTOS_TABLE}` WHERE product_id=%s", (pid,))
            for idx, (l, a, h, p) in enumerate(bultos, start=1):
                cur.execute(
                    f"INSERT INTO `{BULTOS_TABLE}` (product_id, bulto_num, largo, ancho, alto, peso) "
                    f"VALUES (%s,%s,%s,%s,%s,%s)",
                    (pid, idx, l, a, h, p)
                )
        conn.commit()
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    # Calcular y retornar totales agregados de todos los bultos
    total_kg  = round(sum(b[3] for b in bultos), 4)
    total_pv  = round(sum(b[0] * b[1] * b[2] for b in bultos) / 4000.0, 4)
    pred      = round(max(total_kg, total_pv), 4)
    return jsonify({
        "ok":      True,
        "peso_kg": total_kg,
        "peso_vol": total_pv,
        "pred":    pred,
    })


@app.route("/transporte/api/upload-foto", methods=["POST"])
@_tr_required
def tr_upload_foto():
    """Sube hasta 2 fotos a un producto (por SKU) desde el modal de transporte."""
    sku = (request.form.get("sku") or "").strip().upper()
    if not sku:
        return jsonify({"error": "Falta el SKU"}), 400
    file = request.files.get("foto")
    if not file or not file.filename:
        return jsonify({"error": "Sin archivo"}), 400
    if not allowed_file(file.filename):
        return jsonify({"error": "Formato no permitido (JPG, PNG, WEBP)"}), 400

    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT id FROM `{PRODUCTS_TABLE}` WHERE UPPER(TRIM(sku))=%s LIMIT 1", (sku,))
            row = cur.fetchone()
            if row:
                pid = row["id"]
            else:
                cur.execute(
                    f"INSERT INTO `{PRODUCTS_TABLE}` (sku,nombre,estado,created_by,updated_by) VALUES (%s,%s,'activo',%s,%s)",
                    (sku, sku, current_username(), current_username()),
                )
                pid = cur.lastrowid

            cur.execute(f"SELECT COUNT(*) AS n FROM `{PHOTOS_TABLE}` WHERE product_id=%s", (pid,))
            n = (cur.fetchone() or {}).get("n", 0)
            if n >= MAX_PHOTOS:
                return jsonify({"error": f"Máximo {MAX_PHOTOS} fotos por producto"}), 400

            ext = secure_filename(file.filename).rsplit(".", 1)[-1].lower()
            ts  = int(datetime.now().timestamp())
            if _CLD_READY:
                try:
                    filename = _cloud_upload(file, public_id=f"p{pid}_{ts}", folder="ilus/products")
                except Exception as exc:
                    return jsonify({"error": f"Error Cloudinary: {exc}"}), 500
            else:
                filename = f"p{pid}_{ts}.{ext}"
                file.save(os.path.join(UPLOAD_FOLDER, filename))

            cur.execute(
                f"INSERT INTO `{PHOTOS_TABLE}` (product_id,filename,orden) VALUES (%s,%s,%s)",
                (pid, filename, n + 1),
            )
        conn.commit()
        return jsonify({"ok": True, "url": _photo_src(filename), "total": n + 1})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/transporte/api/importar-excel", methods=["POST"])
@_tr_required
def tr_importar_excel():
    """Importa documentos desde un Excel/CSV exportado del ERP."""
    f = request.files.get("archivo")
    if not f or not f.filename:
        return jsonify({"error": "Sin archivo"}), 400
    ext = f.filename.rsplit(".", 1)[-1].lower()
    if ext not in ("xlsx", "xls", "csv"):
        return jsonify({"error": "Solo se aceptan .xlsx, .xls o .csv"}), 400
    data = f.read()
    count, errs, preview = _tr_import_from_excel(data, f.filename)
    _tr_log("commitment", 0, "importar_excel",
            f"Archivo {f.filename}: {count} importados")
    return jsonify({
        "ok": True,
        "importados": count,
        "errores": (errs or [])[:10],
        "preview": preview,
    })


@app.route("/transporte/")
@_tr_required
def transporte_index():
    from datetime import date as _date, timedelta as _td

    periodo = request.args.get("periodo", "")
    hoy     = _date.today()

    # Fechas por defecto: último mes
    _default_desde = (hoy - _td(days=30)).strftime("%Y-%m-%d")
    _default_hasta = hoy.strftime("%Y-%m-%d")

    if periodo == "mes":
        fecha_desde, fecha_hasta = _default_desde, _default_hasta
    elif periodo == "3m":
        fecha_desde = (hoy - _td(days=90)).strftime("%Y-%m-%d")
        fecha_hasta = _default_hasta
    elif periodo == "año":
        fecha_desde = hoy.strftime("%Y-01-01")
        fecha_hasta = _default_hasta
    elif periodo == "todo":
        fecha_desde, fecha_hasta = "", ""
    else:
        fecha_desde = request.args.get("fecha_desde", _default_desde).strip()
        fecha_hasta = request.args.get("fecha_hasta", _default_hasta).strip()

    filtros = {
        "estado":        request.args.get("estado", ""),
        "clasificacion": request.args.get("clasificacion", ""),
        "q":             request.args.get("q", "").strip(),
        "fecha_desde":   fecha_desde,
        "fecha_hasta":   fecha_hasta,
    }

    where, params = ["tiene_saldo=1"], []
    if filtros["estado"]:
        where.append("estado=%s"); params.append(filtros["estado"])
    if filtros["clasificacion"]:
        where.append("clasificacion=%s"); params.append(filtros["clasificacion"])
    if filtros["q"]:
        where.append("(cliente_nombre LIKE %s OR nudo LIKE %s OR tido LIKE %s OR comuna LIKE %s)")
        q = f"%{filtros['q']}%"; params += [q, q, q, q]
    if filtros["fecha_desde"]:
        where.append("fecha_emision >= %s"); params.append(filtros["fecha_desde"])
    if filtros["fecha_hasta"]:
        where.append("fecha_emision <= %s"); params.append(filtros["fecha_hasta"])

    sql = ("SELECT * FROM transport_commitments WHERE " +
           " AND ".join(where) + " ORDER BY fecha_emision DESC LIMIT 500")
    compromisos = mysql_fetchall(sql, tuple(params))

    # Manifiestos activos para asignación rápida
    manifiestos = mysql_fetchall(
        "SELECT id,correlativo,courier,estado FROM transport_manifests "
        "WHERE estado IN ('En preparación','En curso') ORDER BY id DESC"
    )
    return render_template(
        "transporte/index.html",
        compromisos=compromisos,
        filtros=filtros,
        estados=ESTADOS_COMPROMISO,
        estado_colors=ESTADO_COLORS,
        couriers=COURIERS,
        manifiestos=manifiestos,
    )


@app.route("/transporte/api/agregar", methods=["POST"])
@_tr_required
def tr_agregar():
    """Agrega un documento al monitor desde el ERP."""
    tido = (request.form.get("tido") or "FCV").strip().upper()
    nudo = (request.form.get("nudo") or "").strip()
    if not nudo:
        return jsonify({"error": "Ingresa el número de documento"}), 400
    comm_id, err = _tr_fetch_from_erp(tido, nudo)
    if err:
        return jsonify({"error": err}), 404
    _tr_log("commitment", comm_id, "agregado",
            f"Documento {tido} {nudo} importado desde ERP")
    return jsonify({"ok": True, "id": comm_id})


@app.route("/transporte/api/compromisos/<int:cid>", methods=["PUT"])
@_tr_required
def tr_update_compromiso(cid):
    """Actualiza campos operativos: estado, costo_zz, notas, fecha_agenda."""
    data   = request.get_json(silent=True) or {}
    campos = {}
    if "estado" in data and data["estado"] in ESTADOS_COMPROMISO:
        campos["estado"] = data["estado"]
    if "costo_zz" in data:
        try: campos["costo_zz"] = float(data["costo_zz"])
        except: pass
    if "notas" in data:
        campos["notas"] = data["notas"]
    if "fecha_agenda" in data:
        campos["fecha_agenda"] = data["fecha_agenda"] or None
    if not campos:
        return jsonify({"error": "sin campos válidos"}), 400

    campos["updated_by"] = current_username()
    sets   = ", ".join(f"{k}=%s" for k in campos)
    vals   = list(campos.values()) + [cid]
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute(f"UPDATE transport_commitments SET {sets} WHERE id=%s", vals)
    conn.commit()

    detalle = "; ".join(f"{k}={v}" for k, v in data.items() if k != "notas")
    _tr_log("commitment", cid, "actualizado", detalle)
    return jsonify({"ok": True})


@app.route("/transporte/api/compromisos/<int:cid>/detalle")
@_tr_required
def tr_detalle(cid):
    """
    Detalle completo de un compromiso para el modal de vista.
    Llama a _cubicador_fetch para obtener líneas con pesos y cruza con fotos locales.
    """
    c = mysql_fetchone("SELECT * FROM transport_commitments WHERE id=%s", (cid,))
    if not c:
        return jsonify({"error": "No encontrado"}), 404

    tido = (c["tido"] or "").strip()
    nudo = (c["nudo"] or "").strip()

    try:
        header, lineas = _cubicador_fetch(tido, nudo)
    except Exception as e:
        return jsonify({"error": f"ERP no disponible: {e}"}), 503

    if not header:
        return jsonify({"error": "Documento no encontrado en ERP"}), 404

    # Separar líneas ZZ de líneas de producto
    ZZ_UP = {s.upper() for s in {"ZZENVIO","ZZINGREPUESTO","ZZSERVTEC","ZZRETIRO","ZZINSTALACION","ZZINGARREQUIP"}}
    lineas_prod = []
    lineas_zz   = []
    for l in lineas:
        if l.get("es_zz") or l["sku"].upper() in ZZ_UP:
            lineas_zz.append(l)
        elif l.get("saldo", l["cantidad"]) > 0:
            # Solo incluir líneas con saldo pendiente
            lineas_prod.append(l)

    # Adjuntar fotos solo a líneas de producto
    for l in lineas_prod:
        app_id = l.get("app_id")
        photos = []
        if app_id:
            ph_rows = mysql_fetchall(
                f"SELECT filename FROM `{PHOTOS_TABLE}` WHERE product_id=%s ORDER BY orden LIMIT 3",
                (app_id,)
            )
            photos = [_photo_src(p["filename"]) for p in ph_rows if p["filename"]]
        l["fotos"] = photos

    # Totales solo de líneas con saldo
    tot_kg   = round(sum(l["peso_kg_tot"]  for l in lineas_prod), 3)
    tot_pv   = round(sum(l["peso_vol_tot"] for l in lineas_prod), 3)
    tot_pred = round(sum(l["pred_tot"]     for l in lineas_prod), 3)
    pred_tipo = "kg" if tot_kg >= tot_pv else "pv"

    # ZZenvio cost
    costo_zz_envio = sum(
        float(l.get("pred_u") or 0) for l in lineas_zz
        if l["sku"].upper() == "ZZENVIO"
    )

    # Commune: prefer DB stored, then ERP header
    comuna = c["comuna"] or header.get("comuna") or ""
    if not comuna:
        # Try to extract from direccion
        dir_val = c["direccion"] or header.get("direccion") or ""
        if " - " in dir_val:
            parts = [p.strip() for p in dir_val.split(" - ")]
            if parts:
                comuna = parts[0].split(",")[-1].strip()

    return jsonify({
        "ok": True,
        "debug_fields": header.get("all_fields", []),
        "compromiso": {
            "id":            c["id"],
            "tido":          tido,
            "nudo":          nudo,
            "cliente":       c["cliente_nombre"] or header.get("cliente_nombre") or "—",
            "rut":           c["cliente_rut"] or header.get("cliente_rut") or "",
            "comuna":        comuna,
            "direccion":     c["direccion"] or header.get("direccion") or "",
            "telefono":      c["telefono"] or header.get("telefono") or "",
            "email":         c["email"] or header.get("email") or "",
            "costo_zz":      float(c["costo_zz"] or 0),
            "costo_zz_envio":costo_zz_envio,
            "clasificacion": c["clasificacion"] or "despacho",
            "fecha_emision": c["fecha_emision"].strftime("%d/%m/%Y") if c["fecha_emision"] else "",
            "estado":        c["estado"] or "",
        },
        "lineas": lineas_prod,
        "lineas_zz": lineas_zz,
        "totales": {
            "kg":   tot_kg,
            "pv":   tot_pv,
            "pred": tot_pred,
            "pred_tipo": pred_tipo,
        },
    })


@app.route("/transporte/api/compromisos/<int:cid>/lineas")
@_tr_required
def tr_lineas(cid):
    lineas = mysql_fetchall(
        "SELECT * FROM transport_commitment_lines WHERE commitment_id=%s", (cid,)
    )
    return jsonify([dict(l) for l in lineas])


@app.route("/transporte/api/compromisos/<int:cid>/logs")
@_tr_required
def tr_commitment_logs(cid):
    logs = mysql_fetchall(
        "SELECT * FROM transport_logs WHERE entity_type='commitment' AND entity_id=%s "
        "ORDER BY created_at DESC LIMIT 50", (cid,)
    )
    return jsonify([dict(l) for l in logs])


# ── MANIFIESTOS ──────────────────────────────────────────────────

@app.route("/transporte/manifiestos")
@_tr_required
def tr_manifiestos():
    filtros = {
        "courier": request.args.get("courier", ""),
        "estado":  request.args.get("estado", ""),
    }
    where, params = ["1=1"], []
    if filtros["courier"]:
        where.append("courier=%s"); params.append(filtros["courier"])
    if filtros["estado"]:
        where.append("estado=%s"); params.append(filtros["estado"])
    manifiestos = mysql_fetchall(
        "SELECT * FROM transport_manifests WHERE " + " AND ".join(where) +
        " ORDER BY fecha DESC, id DESC", tuple(params)
    )
    return render_template(
        "transporte/manifiestos.html",
        manifiestos=manifiestos,
        filtros=filtros,
        couriers=COURIERS,
        estados_manifest=["En preparación", "En curso", "Cerrado", "Entregado completo"],
    )


@app.route("/transporte/manifiestos/nuevo", methods=["POST"])
@_tr_required
def tr_crear_manifiesto():
    courier = request.form.get("courier", "").strip()
    fecha   = request.form.get("fecha", "").strip()
    notas   = request.form.get("notas", "").strip()
    if not courier or not fecha:
        return jsonify({"error": "courier y fecha son obligatorios"}), 400

    conn = get_db()
    try:
        with conn.cursor() as cur:
            # Correlativo auto: MAN-YYYY-NNNN
            cur.execute(
                "SELECT COUNT(*)+1 AS n FROM transport_manifests "
                "WHERE YEAR(created_at)=YEAR(NOW())"
            )
            n = (cur.fetchone() or {}).get("n", 1)
            from datetime import datetime as _dt
            corr = f"MAN-{_dt.now().year}-{int(n):04d}"
            cur.execute(
                "INSERT INTO transport_manifests (correlativo,fecha,courier,notas,created_by) "
                "VALUES (%s,%s,%s,%s,%s)",
                (corr, fecha, courier, notas, current_username())
            )
            mid = cur.lastrowid
        conn.commit()
    finally:
        conn.close()

    _tr_log("manifest", mid, "creado", f"courier={courier} fecha={fecha}")
    return jsonify({"ok": True, "id": mid, "correlativo": corr})


@app.route("/transporte/manifiestos/<int:mid>")
@_tr_required
def tr_manifiesto_detalle(mid):
    manifiesto = mysql_fetchone(
        "SELECT * FROM transport_manifests WHERE id=%s", (mid,)
    )
    if not manifiesto:
        flash("Manifiesto no encontrado", "danger")
        return redirect(url_for("tr_manifiestos"))

    items = mysql_fetchall("""
        SELECT mi.*, c.tido, c.nudo, c.cliente_nombre, c.comuna,
               c.direccion, c.valor_bruto, c.costo_zz, c.clasificacion
        FROM transport_manifest_items mi
        JOIN transport_commitments c ON c.id = mi.commitment_id
        WHERE mi.manifest_id=%s
        ORDER BY mi.orden, mi.id
    """, (mid,))

    logs = mysql_fetchall(
        "SELECT * FROM transport_logs WHERE entity_type='manifest' AND entity_id=%s "
        "ORDER BY created_at DESC LIMIT 30", (mid,)
    )
    return render_template(
        "transporte/manifiesto_detalle.html",
        manifiesto=manifiesto,
        items=items,
        logs=logs,
        estados_entrega=ESTADOS_ENTREGA,
        estados_manifest=["En preparación", "En curso", "Cerrado", "Entregado completo"],
        couriers=COURIERS,
    )


@app.route("/transporte/manifiestos/<int:mid>/items", methods=["POST"])
@_tr_required
def tr_agregar_item(mid):
    cid = request.get_json(silent=True, force=True).get("commitment_id")
    if not cid:
        return jsonify({"error": "commitment_id requerido"}), 400
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT IGNORE INTO transport_manifest_items (manifest_id,commitment_id) "
                "VALUES (%s,%s)", (mid, cid)
            )
            # Recalcular totales
            cur.execute("""
                UPDATE transport_manifests m SET
                  total_items=(SELECT COUNT(*) FROM transport_manifest_items WHERE manifest_id=m.id),
                  costo_total=(SELECT COALESCE(SUM(c.costo_zz),0)
                               FROM transport_manifest_items mi
                               JOIN transport_commitments c ON c.id=mi.commitment_id
                               WHERE mi.manifest_id=m.id)
                WHERE m.id=%s
            """, (mid,))
        conn.commit()
    finally:
        conn.close()
    _tr_log("manifest", mid, "item agregado", f"commitment_id={cid}")
    return jsonify({"ok": True})


@app.route("/transporte/manifiestos/<int:mid>/items/<int:item_id>", methods=["DELETE"])
@_tr_required
def tr_quitar_item(mid, item_id):
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM transport_manifest_items WHERE id=%s AND manifest_id=%s",
            (item_id, mid)
        )
        cur.execute(
            "UPDATE transport_manifests SET "
            "total_items=(SELECT COUNT(*) FROM transport_manifest_items WHERE manifest_id=%s) "
            "WHERE id=%s", (mid, mid)
        )
    conn.commit()
    _tr_log("manifest", mid, "item eliminado", f"item_id={item_id}")
    return jsonify({"ok": True})


@app.route("/transporte/manifiestos/<int:mid>/items/<int:item_id>/estado", methods=["PUT"])
@_tr_required
def tr_estado_entrega(mid, item_id):
    estado = (request.get_json(silent=True) or {}).get("estado_entrega", "")
    if estado not in ESTADOS_ENTREGA:
        return jsonify({"error": "estado inválido"}), 400
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE transport_manifest_items SET estado_entrega=%s WHERE id=%s AND manifest_id=%s",
            (estado, item_id, mid)
        )
    conn.commit()
    _tr_log("manifest_item", item_id, "estado_entrega", estado)
    return jsonify({"ok": True})


@app.route("/transporte/manifiestos/<int:mid>/estado", methods=["PUT"])
@_tr_required
def tr_estado_manifiesto(mid):
    estado = (request.get_json(silent=True) or {}).get("estado", "")
    validos = ["En preparación", "En curso", "Cerrado", "Entregado completo"]
    if estado not in validos:
        return jsonify({"error": "estado inválido"}), 400
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute("UPDATE transport_manifests SET estado=%s WHERE id=%s", (estado, mid))
    conn.commit()
    _tr_log("manifest", mid, "estado cambiado", estado)
    return jsonify({"ok": True})


# ── MANIFIESTOS: asignar compromiso desde drag-drop ──────────────────────────

@app.route("/transporte/api/manifiestos/asignar", methods=["POST"])
@_tr_required
def tr_asignar_a_manifiesto():
    """Asigna uno o más compromisos a un manifiesto (crea si mid=None)."""
    data         = request.get_json(silent=True) or {}
    commitment_ids = data.get("commitment_ids", [])
    mid          = data.get("manifest_id")     # None → crear nuevo

    if not commitment_ids:
        return jsonify({"error": "sin compromisos"}), 400

    conn = get_db()
    with conn.cursor() as cur:
        # crear manifiesto nuevo si no se indicó uno
        if not mid:
            courier = data.get("courier", "Por asignar")
            fecha   = data.get("fecha") or __import__("datetime").date.today().isoformat()
            cur.execute(
                "SELECT COALESCE(MAX(CAST(SUBSTRING(correlativo,4) AS UNSIGNED)),0)+1 FROM transport_manifests"
            )
            num = cur.fetchone()[0] or 1
            correlativo = f"MAN{num:04d}"
            cur.execute(
                """INSERT INTO transport_manifests (correlativo, fecha, courier, created_by)
                   VALUES (%s,%s,%s,%s)""",
                (correlativo, fecha, courier, current_user.correo),
            )
            mid = cur.lastrowid
        else:
            correlativo = None

        added, dupes = 0, 0
        for cid in commitment_ids:
            try:
                cur.execute(
                    """INSERT IGNORE INTO transport_manifest_items
                       (manifest_id, commitment_id) VALUES (%s,%s)""",
                    (mid, cid),
                )
                if cur.rowcount:
                    added += 1
                else:
                    dupes += 1
            except Exception:
                dupes += 1

        # recalcular total_items
        cur.execute(
            "UPDATE transport_manifests SET total_items=(SELECT COUNT(*) FROM transport_manifest_items WHERE manifest_id=%s) WHERE id=%s",
            (mid, mid),
        )

    conn.commit()
    return jsonify({"ok": True, "manifest_id": mid, "correlativo": correlativo, "added": added, "duplicados": dupes})


# ── COURIERS ─────────────────────────────────────────────────────────────────

@app.route("/transporte/couriers")
@_tr_required
def tr_couriers():
    couriers = mysql_fetchall(
        """SELECT c.*,
           COUNT(t.id) AS total_tarifas
           FROM transport_couriers c
           LEFT JOIN transport_courier_tarifas t ON t.courier_id=c.id AND t.activo=1
           GROUP BY c.id ORDER BY c.activo DESC, c.nombre""",
        ()
    )
    return render_template("transporte/couriers.html", couriers=couriers)


@app.route("/transporte/couriers/nuevo", methods=["POST"])
@_tr_required
def tr_courier_nuevo():
    d = request.form
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO transport_couriers
               (nombre,rut,contacto,telefono,email,tipo,notas,
                peso_max_bulto,peso_max_guia,vol_max_bulto,factor_vol)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (
                d.get("nombre","").strip(),
                d.get("rut","").strip(),
                d.get("contacto","").strip(),
                d.get("telefono","").strip(),
                d.get("email","").strip(),
                d.get("tipo","nacional"),
                d.get("notas","").strip(),
                float(d.get("peso_max_bulto") or 0),
                float(d.get("peso_max_guia") or 0),
                float(d.get("vol_max_bulto") or 0),
                float(d.get("factor_vol") or 5000),
            ),
        )
    conn.commit()
    flash("Courier creado correctamente.", "success")
    return redirect(url_for("tr_couriers"))


@app.route("/transporte/couriers/<int:cid>", methods=["GET"])
@_tr_required
def tr_courier_detalle(cid):
    courier = mysql_fetchone(
        "SELECT * FROM transport_couriers WHERE id=%s", (cid,)
    )
    if not courier:
        flash("Courier no encontrado.", "danger")
        return redirect(url_for("tr_couriers"))
    tarifas = mysql_fetchall(
        "SELECT * FROM transport_courier_tarifas WHERE courier_id=%s ORDER BY zona,peso_desde",
        (cid,),
    )
    return jsonify({"courier": dict(courier), "tarifas": [dict(t) for t in tarifas]})


@app.route("/transporte/couriers/<int:cid>", methods=["PUT"])
@_tr_required
def tr_courier_editar(cid):
    d = request.get_json(silent=True) or {}
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute(
            """UPDATE transport_couriers SET
               nombre=%s, rut=%s, contacto=%s, telefono=%s, email=%s,
               tipo=%s, notas=%s, activo=%s,
               peso_max_bulto=%s, peso_max_guia=%s, vol_max_bulto=%s, factor_vol=%s
               WHERE id=%s""",
            (
                d.get("nombre","").strip(),
                d.get("rut","").strip(),
                d.get("contacto","").strip(),
                d.get("telefono","").strip(),
                d.get("email","").strip(),
                d.get("tipo","nacional"),
                d.get("notas","").strip(),
                1 if d.get("activo", True) else 0,
                float(d.get("peso_max_bulto") or 0),
                float(d.get("peso_max_guia") or 0),
                float(d.get("vol_max_bulto") or 0),
                float(d.get("factor_vol") or 5000),
                cid,
            ),
        )
    conn.commit()
    return jsonify({"ok": True})


@app.route("/transporte/couriers/<int:cid>", methods=["DELETE"])
@_tr_required
def tr_courier_eliminar(cid):
    conn = get_db()
    with conn.cursor() as cur:
        # desactivar en lugar de borrar (preserva historial)
        cur.execute("UPDATE transport_couriers SET activo=0 WHERE id=%s", (cid,))
    conn.commit()
    return jsonify({"ok": True})


# ── TARIFAS DE COURIER ────────────────────────────────────────────────────────

@app.route("/transporte/couriers/<int:cid>/tarifas", methods=["POST"])
@_tr_required
def tr_tarifa_nueva(cid):
    d = request.get_json(silent=True) or {}
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO transport_courier_tarifas
               (courier_id,zona,peso_desde,peso_hasta,precio_base,precio_kg_extra,moneda)
               VALUES (%s,%s,%s,%s,%s,%s,%s)""",
            (
                cid,
                d.get("zona","General").strip(),
                float(d.get("peso_desde") or 0),
                float(d.get("peso_hasta") or 0),
                float(d.get("precio_base") or 0),
                float(d.get("precio_kg_extra") or 0),
                d.get("moneda","CLP"),
            ),
        )
        tid = cur.lastrowid
    conn.commit()
    return jsonify({"ok": True, "id": tid})


@app.route("/transporte/couriers/<int:cid>/tarifas/<int:tid>", methods=["PUT"])
@_tr_required
def tr_tarifa_editar(cid, tid):
    d = request.get_json(silent=True) or {}
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute(
            """UPDATE transport_courier_tarifas SET
               zona=%s,peso_desde=%s,peso_hasta=%s,
               precio_base=%s,precio_kg_extra=%s,moneda=%s,activo=%s
               WHERE id=%s AND courier_id=%s""",
            (
                d.get("zona","General").strip(),
                float(d.get("peso_desde") or 0),
                float(d.get("peso_hasta") or 0),
                float(d.get("precio_base") or 0),
                float(d.get("precio_kg_extra") or 0),
                d.get("moneda","CLP"),
                1 if d.get("activo", True) else 0,
                tid, cid,
            ),
        )
    conn.commit()
    return jsonify({"ok": True})


@app.route("/transporte/couriers/<int:cid>/tarifas/<int:tid>", methods=["DELETE"])
@_tr_required
def tr_tarifa_eliminar(cid, tid):
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM transport_courier_tarifas WHERE id=%s AND courier_id=%s",
            (tid, cid),
        )
    conn.commit()
    return jsonify({"ok": True})


# ── COTIZADOR: calcular costo de envío ────────────────────────────────────────

@app.route("/transporte/api/cotizar", methods=["POST"])
@_tr_required
def tr_cotizar():
    """
    Calcula el costo estimado de envío con cada courier activo.
    Body JSON: {commitment_id, courier_id (opcional, si no devuelve todos)}
    """
    data = request.get_json(silent=True) or {}
    cid  = data.get("commitment_id")
    solo_courier = data.get("courier_id")

    if not cid:
        return jsonify({"error": "commitment_id requerido"}), 400

    # peso y volumen totales del compromiso usando las mismas tablas que _cubicador_fetch
    row = mysql_fetchone(
        f"""SELECT
           COALESCE(SUM(l.saldo * bk.peso_total),0)          AS peso_real,
           COALESCE(SUM(l.saldo * bk.volumen_cm3),0)          AS vol_total
           FROM transport_commitment_lines l
           LEFT JOIN (
               SELECT UPPER(TRIM(p.sku)) AS sku,
                      COALESCE(SUM(b.peso),0) AS peso_total,
                      COALESCE(SUM(b.largo * b.ancho * b.alto),0) AS volumen_cm3
               FROM `{PRODUCTS_TABLE}` p
               LEFT JOIN `{BULTOS_TABLE}` b ON b.product_id=p.id
               GROUP BY p.sku
           ) bk ON bk.sku=UPPER(TRIM(l.koprct))
           WHERE l.commitment_id=%s AND l.saldo>0""",
        (cid,),
    )
    peso_real = float(row["peso_real"] or 0) if row else 0
    vol_total = float(row["vol_total"] or 0) if row else 0

    couriers = mysql_fetchall(
        """SELECT c.*, GROUP_CONCAT(
               CONCAT_WS('|',t.zona,t.peso_desde,t.peso_hasta,t.precio_base,t.precio_kg_extra)
               ORDER BY t.zona,t.peso_desde SEPARATOR ';;'
           ) AS tarifas_raw
           FROM transport_couriers c
           LEFT JOIN transport_courier_tarifas t ON t.courier_id=c.id AND t.activo=1
           WHERE c.activo=1 {}
           GROUP BY c.id""".format("AND c.id=%s" if solo_courier else ""),
        (solo_courier,) if solo_courier else (),
    )

    resultados = []
    for co in couriers:
        factor_vol = float(co["factor_vol"] or 5000)
        peso_vol   = vol_total / factor_vol if vol_total else 0
        peso_pred  = max(peso_real, peso_vol)

        # validar restricciones
        advertencias = []
        if co["peso_max_bulto"] and peso_real > float(co["peso_max_bulto"]):
            advertencias.append(f"Excede peso máx por bulto ({co['peso_max_bulto']} kg)")
        if co["peso_max_guia"] and peso_pred > float(co["peso_max_guia"]):
            advertencias.append(f"Excede peso máx por guía ({co['peso_max_guia']} kg)")

        # calcular tarifa
        costo = None
        zona_aplicada = None
        zona_req = data.get("zona", "General")
        if co["tarifas_raw"]:
            for bloque in co["tarifas_raw"].split(";;"):
                partes = bloque.split("|")
                if len(partes) < 5:
                    continue
                zona_t, pd, ph, pb, pke = partes
                pd, ph, pb, pke = float(pd), float(ph), float(pb), float(pke)
                if zona_t not in (zona_req, "General"):
                    continue
                if pd <= peso_pred and (ph == 0 or peso_pred <= ph):
                    extra = max(0, peso_pred - pd) * pke if pke else 0
                    costo = pb + extra
                    zona_aplicada = zona_t
                    break

        resultados.append({
            "courier_id"   : co["id"],
            "nombre"       : co["nombre"],
            "peso_real"    : round(peso_real, 3),
            "peso_vol"     : round(peso_vol, 3),
            "peso_pred"    : round(peso_pred, 3),
            "costo"        : round(costo, 0) if costo is not None else None,
            "zona"         : zona_aplicada,
            "advertencias" : advertencias,
        })

    resultados.sort(key=lambda x: (x["costo"] is None, x["costo"] or 999999))
    return jsonify({"ok": True, "resultados": resultados})


# ── COTIZAR COLA (batch: varios commitment_ids) ───────────────────────────────

@app.route("/transporte/api/cola/cotizar", methods=["POST"])
@_tr_required
def tr_cola_cotizar():
    """
    Calcula peso/volumen acumulado para un array de commitment_ids
    y devuelve cotización con todos los couriers activos.
    Usado por el panel manifiesto para mostrar prevale en tiempo real.
    """
    data = request.get_json(silent=True) or {}
    cids = [int(c) for c in (data.get("commitment_ids") or []) if str(c).isdigit()]

    if not cids:
        return jsonify({"ok": True, "peso_real": 0, "vol_total": 0,
                        "peso_vol": 0, "peso_pred": 0, "resultados": []})

    ph = ",".join(["%s"] * len(cids))

    # Peso y volumen acumulados de todos los compromisos
    row = mysql_fetchone(
        f"""SELECT
               COALESCE(SUM(l.saldo * bk.peso_total), 0) AS peso_real,
               COALESCE(SUM(l.saldo * bk.vol_cm3),   0) AS vol_total
           FROM transport_commitment_lines l
           LEFT JOIN (
               SELECT UPPER(TRIM(p.sku)) AS sku,
                      COALESCE(SUM(b.peso), 0)                     AS peso_total,
                      COALESCE(SUM(b.largo * b.ancho * b.alto), 0) AS vol_cm3
               FROM `{PRODUCTS_TABLE}` p
               LEFT JOIN `{BULTOS_TABLE}` b ON b.product_id = p.id
               GROUP BY p.sku
           ) bk ON bk.sku = UPPER(TRIM(l.koprct))
           WHERE l.commitment_id IN ({ph}) AND l.saldo > 0""",
        tuple(cids),
    )
    peso_real = float(row["peso_real"] or 0) if row else 0
    vol_total = float(row["vol_total"] or 0) if row else 0

    # Couriers activos con sus tarifas
    couriers = mysql_fetchall(
        """SELECT c.*,
               GROUP_CONCAT(
                   CONCAT_WS('|', t.zona, t.peso_desde, t.peso_hasta,
                             t.precio_base, t.precio_kg_extra)
                   ORDER BY t.zona, t.peso_desde SEPARATOR ';;'
               ) AS tarifas_raw
           FROM transport_couriers c
           LEFT JOIN transport_courier_tarifas t ON t.courier_id = c.id AND t.activo = 1
           WHERE c.activo = 1
           GROUP BY c.id
           ORDER BY c.nombre""",
        (),
    )

    zona_req = data.get("zona", "General")
    resultados = []
    factor_base = 5000  # factor volumen por defecto

    for co in couriers:
        factor_vol = float(co["factor_vol"] or 5000)
        factor_base = factor_vol  # usar último valor para resumen
        peso_vol  = vol_total / factor_vol if vol_total else 0
        peso_pred = max(peso_real, peso_vol)

        advertencias = []
        if co["peso_max_guia"] and peso_pred > float(co["peso_max_guia"]):
            advertencias.append(f"Excede máx/guía ({co['peso_max_guia']} kg)")

        costo = None
        if co["tarifas_raw"]:
            for bloque in co["tarifas_raw"].split(";;"):
                partes = bloque.split("|")
                if len(partes) < 5:
                    continue
                zona_t, pd, ph2, pb, pke = partes
                pd, ph2, pb, pke = float(pd), float(ph2), float(pb), float(pke)
                if zona_t not in (zona_req, "General"):
                    continue
                if pd <= peso_pred and (ph2 == 0 or peso_pred <= ph2):
                    extra = max(0, peso_pred - pd) * pke if pke else 0
                    costo = pb + extra
                    break

        resultados.append({
            "courier_id"   : co["id"],
            "nombre"       : co["nombre"],
            "peso_max_guia": float(co["peso_max_guia"] or 0),
            "costo"        : round(costo, 0) if costo is not None else None,
            "advertencias" : advertencias,
        })

    resultados.sort(key=lambda x: (x["costo"] is None, x["costo"] or 999_999))

    peso_vol_def  = vol_total / factor_base if vol_total else 0
    peso_pred_def = max(peso_real, peso_vol_def)

    # Conteo de ítems por compromiso (para mostrar en el panel)
    items_rows = mysql_fetchall(
        f"""SELECT commitment_id,
               COUNT(*)                    AS n_lineas,
               COALESCE(SUM(saldo), 0)     AS total_saldo
           FROM transport_commitment_lines
           WHERE commitment_id IN ({",".join(["%s"]*len(cids))}) AND saldo > 0
           GROUP BY commitment_id""",
        tuple(cids),
    )
    items_by_cid = {r["commitment_id"]: {"n": int(r["n_lineas"]), "saldo": int(r["total_saldo"])}
                    for r in items_rows}

    return jsonify({
        "ok"        : True,
        "peso_real" : round(peso_real, 2),
        "vol_total" : round(vol_total, 0),
        "peso_vol"  : round(peso_vol_def, 2),
        "peso_pred" : round(peso_pred_def, 2),
        "resultados": resultados,
        "items"     : items_by_cid,
    })


# ── MANIFIESTOS: listar activos para panel drag-drop ─────────────────────────

@app.route("/transporte/api/manifiestos/activos")
@_tr_required
def tr_manifiestos_activos():
    rows = mysql_fetchall(
        """SELECT m.id, m.correlativo, m.fecha, m.courier,
                  m.estado, m.total_items
           FROM transport_manifests m
           WHERE m.estado IN ('En preparación','En curso')
           ORDER BY m.fecha DESC, m.id DESC LIMIT 50""",
        (),
    )
    return jsonify([dict(r) for r in rows])


# ══════════════════════════════════════════════════════════════
#  MÓDULO: COMUNICACIONES (solo superadmin)
#  Email (SMTP dinámico) + WhatsApp (Twilio)
# ══════════════════════════════════════════════════════════════

def init_comunicaciones_tables():
    conn = get_mysql()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS comm_smtp_config (
                    id          INT AUTO_INCREMENT PRIMARY KEY,
                    smtp_host   VARCHAR(200) DEFAULT 'smtp.gmail.com',
                    smtp_port   INT DEFAULT 587,
                    smtp_user   VARCHAR(200),
                    smtp_pass   VARCHAR(500),
                    from_name   VARCHAR(200) DEFAULT 'ILUS Sport & Health',
                    from_addr   VARCHAR(200),
                    secure      TINYINT(1) DEFAULT 0,
                    updated_by  VARCHAR(190),
                    updated_at  DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS comm_client_config (
                    id              INT AUTO_INCREMENT PRIMARY KEY,
                    company_name    VARCHAR(200) DEFAULT 'ILUS Sport & Health',
                    reply_to        VARCHAR(200),
                    support_email   VARCHAR(200),
                    support_phone   VARCHAR(50),
                    tracking_url    VARCHAR(400),
                    logo_url        MEDIUMTEXT,
                    corp_color      VARCHAR(20) DEFAULT '#CC0000',
                    email_cc        VARCHAR(500),
                    email_bcc       VARCHAR(500),
                    updated_by      VARCHAR(190),
                    updated_at      DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS comm_whatsapp_config (
                    id              INT AUTO_INCREMENT PRIMARY KEY,
                    account_sid     VARCHAR(120),
                    auth_token      VARCHAR(250),
                    from_number     VARCHAR(50),
                    biz_number      VARCHAR(50),
                    updated_by      VARCHAR(190),
                    updated_at      DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS comm_log (
                    id              INT AUTO_INCREMENT PRIMARY KEY,
                    canal           ENUM('email','whatsapp') NOT NULL,
                    destinatario    VARCHAR(300),
                    asunto          VARCHAR(500),
                    estado          ENUM('ok','error') NOT NULL,
                    detalle         TEXT,
                    enviado_por     VARCHAR(190),
                    created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_canal (canal),
                    INDEX idx_fecha (created_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            # ── PLANTILLAS POR ESTADO ─────────────────────────────────────
            cur.execute("""
                CREATE TABLE IF NOT EXISTS comm_templates (
                    id          INT AUTO_INCREMENT PRIMARY KEY,
                    estado      VARCHAR(60) NOT NULL,
                    canal       ENUM('email','whatsapp') NOT NULL,
                    asunto      VARCHAR(300),
                    cuerpo      MEDIUMTEXT,
                    activo      TINYINT(1) DEFAULT 1,
                    updated_by  VARCHAR(190),
                    updated_at  DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    UNIQUE KEY uq_estado_canal (estado, canal)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            # Insertar plantillas por defecto si la tabla está vacía
            cur.execute("SELECT COUNT(*) AS n FROM comm_templates")
            tpl_row = cur.fetchone()
            if (tpl_row or {}).get('n', 1) == 0:
                _ESTADOS = [
                    ('programado',       'Pedido programado'),
                    ('en_ruta',          'Pedido en ruta'),
                    ('en_camino',        'En camino — próxima entrega'),
                    ('entregado',        'Pedido entregado'),
                    ('fallido',          'Intento de entrega fallido'),
                    ('inicio_sesion',    'Inicio de sesión en tu cuenta'),
                    ('cambio_pass',      'Cambio de contraseña'),
                ]
                _EMAIL_DEFAULTS = {
                    'programado':    ('Tu pedido {{id_pedido}} fue programado',
                                     'Hola {{nombre_cliente}}, tu pedido <strong>{{id_pedido}}</strong> ha sido programado y está siendo preparado. Te avisaremos cuando salga a despacho.'),
                    'en_ruta':       ('Tu pedido {{id_pedido}} está en ruta',
                                     'Hola {{nombre_cliente}}, tu pedido <strong>{{id_pedido}}</strong> ya salió de bodega y está en camino con {{courier}}. N° de seguimiento: <strong>{{numero_seguimiento}}</strong>.'),
                    'en_camino':     ('¡Tu pedido llega hoy! — {{id_pedido}}',
                                     'Hola {{nombre_cliente}}, tu pedido <strong>{{id_pedido}}</strong> está en reparto y llegará hoy a <em>{{direccion_entrega}}</em>.'),
                    'entregado':     ('Pedido {{id_pedido}} entregado ✓',
                                     'Hola {{nombre_cliente}}, tu pedido <strong>{{id_pedido}}</strong> fue entregado exitosamente. ¡Gracias por tu confianza en ILUS!'),
                    'fallido':       ('No pudimos entregar tu pedido {{id_pedido}}',
                                     'Hola {{nombre_cliente}}, intentamos entregar tu pedido <strong>{{id_pedido}}</strong> pero no fue posible. Nos pondremos en contacto para coordinar una nueva entrega.'),
                    'inicio_sesion': ('Nuevo inicio de sesión en tu cuenta',
                                     'Hola {{nombre_usuario}}, detectamos un inicio de sesión en tu cuenta el {{fecha_hora}}. Si no fuiste tú, contáctanos de inmediato.'),
                    'cambio_pass':   ('Tu contraseña fue cambiada',
                                     'Hola {{nombre_usuario}}, tu contraseña fue actualizada el {{fecha_hora}}. Si no realizaste este cambio, contáctanos inmediatamente.'),
                }
                _WA_DEFAULTS = {
                    'programado':    '🟡 *ILUS* — Hola {{nombre_cliente}}, tu pedido *{{id_pedido}}* fue programado y está en preparación.',
                    'en_ruta':       '🚚 *ILUS* — Tu pedido *{{id_pedido}}* salió de bodega con {{courier}}. Seguimiento: {{numero_seguimiento}}',
                    'en_camino':     '📦 *ILUS* — ¡Tu pedido *{{id_pedido}}* llega hoy! Dirección: {{direccion_entrega}}',
                    'entregado':     '✅ *ILUS* — Tu pedido *{{id_pedido}}* fue entregado. ¡Gracias {{nombre_cliente}}!',
                    'fallido':       '❌ *ILUS* — No pudimos entregar tu pedido *{{id_pedido}}*. Te contactaremos para reagendar.',
                    'inicio_sesion': '🔐 *ILUS* — Inicio de sesión detectado en tu cuenta el {{fecha_hora}}.',
                    'cambio_pass':   '🔑 *ILUS* — Tu contraseña fue cambiada el {{fecha_hora}}.',
                }
                for estado_key, _ in _ESTADOS:
                    # Email template
                    asunto, cuerpo = _EMAIL_DEFAULTS.get(estado_key, ('', ''))
                    cur.execute(
                        "INSERT INTO comm_templates (estado, canal, asunto, cuerpo) VALUES (%s,'email',%s,%s)",
                        (estado_key, asunto, cuerpo)
                    )
                    # WhatsApp template
                    wa_body = _WA_DEFAULTS.get(estado_key, '')
                    cur.execute(
                        "INSERT INTO comm_templates (estado, canal, asunto, cuerpo) VALUES (%s,'whatsapp',%s,%s)",
                        (estado_key, '', wa_body)
                    )
        conn.commit()
    finally:
        conn.close()


# ── Helpers de config ─────────────────────────────────────────

def _comm_template_defaults():
    def despacho_body(intro, rows, note="", color="#CC0000"):
        row_html = "\n".join(
            "<tr><td style='padding:6px 0;font-size:13px;color:#555'>"
            f"<strong style='color:#222'>{label}:</strong>&nbsp; {value}</td></tr>"
            for label, value in rows
        )
        note_html = (
            f"<p style='margin:18px 0 0;font-size:13px;color:#777;line-height:1.55'>{note}</p>"
            if note else ""
        )
        return (
            f"<p style='margin:0 0 16px;font-size:15px;color:{color};font-weight:700'>Hola, {{{{nombre_cliente}}}}</p>\n"
            f"<p style='margin:0 0 14px;font-size:14px;color:#444;line-height:1.65'>{intro}</p>\n"
            f"<table cellpadding='0' cellspacing='0' width='100%' style='background:#f5f5f7;border-left:4px solid {color};"
            "border-radius:4px;padding:14px 18px;margin:18px 0'>\n"
            f"{row_html}\n</table>\n{note_html}"
        )

    def sistema_body(intro, rows, note="", color="#CC0000"):
        row_html = "\n".join(
            "<tr><td style='padding:6px 0;font-size:13px;color:#555'>"
            f"<strong style='color:#222'>{label}:</strong>&nbsp; {value}</td></tr>"
            for label, value in rows
        )
        note_html = (
            f"<div style='background:#fff3cd;border:1px solid #ffc107;border-radius:6px;padding:12px 16px;"
            f"font-size:13px;color:#664d03;margin-top:16px'>{note}</div>"
            if note else ""
        )
        return (
            f"<p style='margin:0 0 16px;font-size:15px;color:{color};font-weight:700'>Hola, {{{{nombre_usuario}}}}</p>\n"
            f"<p style='margin:0 0 14px;font-size:14px;color:#444;line-height:1.65'>{intro}</p>\n"
            f"<table cellpadding='0' cellspacing='0' width='100%' style='background:#f5f5f7;border-left:4px solid {color};"
            "border-radius:4px;padding:14px 18px;margin:18px 0'>\n"
            f"{row_html}\n</table>\n{note_html}"
        )

    return {
        "programado": {
            "email": (
                "Tu pedido {{id_pedido}} ha sido programado - ILUS",
                despacho_body(
                    "Tu pedido ha sido <strong>programado exitosamente</strong> y esta siendo preparado en nuestra bodega.",
                    [
                        ("Nro. pedido", "{{id_pedido}}"),
                        ("Courier", "{{courier}}"),
                        ("Entrega estimada", "{{fecha_entrega}}"),
                        ("Direccion", "{{direccion_entrega}}"),
                        ("Costo despacho", "{{costo_envio}}"),
                    ],
                    "Te avisaremos cuando salga de bodega con su numero de seguimiento.",
                ),
            ),
            "whatsapp": (
                "",
                "*ILUS Sport & Health*\n\nHola *{{nombre_cliente}}*, tu pedido *{{id_pedido}}* fue programado correctamente.\n\nCourier: {{courier}}\nEntrega estimada: {{fecha_entrega}}\nDireccion: {{direccion_entrega}}\nCosto despacho: {{costo_envio}}\n\nTe avisaremos cuando salga de bodega.",
            ),
        },
        "en_ruta": {
            "email": (
                "Tu pedido {{id_pedido}} esta en ruta - ILUS",
                despacho_body(
                    "Tu pedido <strong>{{id_pedido}}</strong> ya salio de bodega y esta en camino contigo.",
                    [
                        ("Nro. pedido", "{{id_pedido}}"),
                        ("Courier", "{{courier}}"),
                        ("Nro. seguimiento", "{{numero_seguimiento}}"),
                        ("Entrega estimada", "{{fecha_entrega}}"),
                        ("Direccion", "{{direccion_entrega}}"),
                    ],
                    "Puedes rastrear tu envio usando el numero de seguimiento del courier.",
                    "#007bff",
                ),
            ),
            "whatsapp": (
                "",
                "*ILUS Sport & Health*\n\nHola *{{nombre_cliente}}*, tu pedido *{{id_pedido}}* ya esta en ruta.\n\nCourier: {{courier}}\nSeguimiento: {{numero_seguimiento}}\nEntrega estimada: {{fecha_entrega}}\nDestino: {{direccion_entrega}}",
            ),
        },
        "en_camino": {
            "email": (
                "Tu pedido {{id_pedido}} llega hoy - ILUS",
                despacho_body(
                    "Tu pedido <strong>{{id_pedido}}</strong> esta en reparto y llegara hoy a tu domicilio.",
                    [
                        ("Nro. pedido", "{{id_pedido}}"),
                        ("Courier", "{{courier}}"),
                        ("Direccion de entrega", "{{direccion_entrega}}"),
                        ("Nro. seguimiento", "{{numero_seguimiento}}"),
                    ],
                    "Asegurate de que alguien pueda recibir el pedido.",
                    "#17a2b8",
                ),
            ),
            "whatsapp": (
                "",
                "*ILUS Sport & Health*\n\nHola *{{nombre_cliente}}*, tu pedido *{{id_pedido}}* esta en reparto y llegara hoy.\n\nDireccion: {{direccion_entrega}}\nCourier: {{courier}}\nSeguimiento: {{numero_seguimiento}}\n\nAsegurate de que alguien pueda recibirlo.",
            ),
        },
        "entregado": {
            "email": (
                "Pedido {{id_pedido}} entregado con exito - ILUS",
                despacho_body(
                    "Tu pedido <strong>{{id_pedido}}</strong> fue <strong>entregado exitosamente</strong>.",
                    [
                        ("Nro. pedido", "{{id_pedido}}"),
                        ("Entregado en", "{{direccion_entrega}}"),
                        ("Courier", "{{courier}}"),
                        ("Nro. seguimiento", "{{numero_seguimiento}}"),
                    ],
                    "Gracias por confiar en ILUS Sport & Health. Estamos disponibles si necesitas apoyo con tu equipo.",
                    "#20c997",
                ),
            ),
            "whatsapp": (
                "",
                "*ILUS Sport & Health*\n\nHola *{{nombre_cliente}}*, tu pedido *{{id_pedido}}* fue entregado exitosamente.\n\nEntregado en: {{direccion_entrega}}\nCourier: {{courier}}\nSeguimiento: {{numero_seguimiento}}\n\nGracias por confiar en ILUS.",
            ),
        },
        "fallido": {
            "email": (
                "No pudimos entregar tu pedido {{id_pedido}} - ILUS",
                despacho_body(
                    "Lamentamos informarte que no fue posible entregar tu pedido <strong>{{id_pedido}}</strong> en el intento realizado.",
                    [
                        ("Nro. pedido", "{{id_pedido}}"),
                        ("Motivo", "{{motivo_falla}}"),
                        ("Courier", "{{courier}}"),
                        ("Direccion intentada", "{{direccion_entrega}}"),
                    ],
                    "Nuestro equipo se pondra en contacto para coordinar una nueva fecha de entrega.",
                    "#dc3545",
                ),
            ),
            "whatsapp": (
                "",
                "*ILUS Sport & Health*\n\nHola *{{nombre_cliente}}*, no pudimos entregar tu pedido *{{id_pedido}}*.\n\nMotivo: {{motivo_falla}}\nDireccion: {{direccion_entrega}}\nCourier: {{courier}}\n\nNos comunicaremos contigo para reagendar la entrega.",
            ),
        },
        "inicio_sesion": {
            "email": (
                "Nuevo inicio de sesion detectado - ILUS",
                sistema_body(
                    "Detectamos un nuevo inicio de sesion en tu cuenta del sistema ILUS.",
                    [
                        ("Cuenta", "{{email_usuario}}"),
                        ("Fecha y hora", "{{fecha_hora}}"),
                        ("IP de acceso", "{{ip_acceso}}"),
                    ],
                    "Si reconoces esta actividad, puedes ignorar este mensaje. Si no fuiste tu, cambia tu contrasena y contacta al administrador.",
                    "#6f42c1",
                ),
            ),
            "whatsapp": (
                "",
                "*ILUS - Alerta de seguridad*\n\nHola *{{nombre_usuario}}*, se detecto un inicio de sesion en tu cuenta.\n\nFecha: {{fecha_hora}}\nIP: {{ip_acceso}}\nCuenta: {{email_usuario}}\n\nSi no fuiste tu, cambia tu contrasena de inmediato.",
            ),
        },
        "cambio_pass": {
            "email": (
                "Tu contrasena fue actualizada - ILUS",
                sistema_body(
                    "La contrasena de tu cuenta en el sistema ILUS fue actualizada exitosamente.",
                    [
                        ("Cuenta", "{{email_usuario}}"),
                        ("Fecha y hora", "{{fecha_hora}}"),
                    ],
                    "Si no reconoces este cambio, contacta al administrador de inmediato.",
                    "#fd7e14",
                ),
            ),
            "whatsapp": (
                "",
                "*ILUS - Cambio de contrasena*\n\nHola *{{nombre_usuario}}*, tu contrasena fue actualizada el *{{fecha_hora}}*.\n\nCuenta: {{email_usuario}}\n\nSi no realizaste este cambio, contacta al administrador.",
            ),
        },
    }


def _comm_seed_default_templates(overwrite=False):
    try:
        user = current_username() or "system"
    except Exception:
        user = "system"
    conn = get_db()
    try:
        with conn.cursor() as cur:
            for estado, channels in _comm_template_defaults().items():
                for canal, (asunto, cuerpo) in channels.items():
                    if overwrite:
                        cur.execute(
                            """INSERT INTO comm_templates (estado, canal, asunto, cuerpo, updated_by)
                               VALUES (%s,%s,%s,%s,%s)
                               ON DUPLICATE KEY UPDATE
                                 asunto=VALUES(asunto),
                                 cuerpo=VALUES(cuerpo),
                                 updated_by=VALUES(updated_by),
                                 activo=1""",
                            (estado, canal, asunto, cuerpo, user),
                        )
                    else:
                        cur.execute(
                            """INSERT IGNORE INTO comm_templates
                               (estado, canal, asunto, cuerpo, updated_by)
                               VALUES (%s,%s,%s,%s,%s)""",
                            (estado, canal, asunto, cuerpo, user),
                        )
        conn.commit()
    finally:
        conn.close()


def _get_smtp_cfg():
    """Config SMTP: primero DB, luego config.py como fallback."""
    try:
        row = mysql_fetchone(
            "SELECT * FROM comm_smtp_config ORDER BY id DESC LIMIT 1"
        )
        if row and row.get("smtp_user"):
            return {
                "smtp_host": row["smtp_host"] or "smtp.gmail.com",
                "smtp_port": int(row["smtp_port"] or 587),
                "smtp_user": row["smtp_user"],
                "smtp_pass": row["smtp_pass"] or "",
                "from_name": row["from_name"] or "ILUS Sport & Health",
                "from_addr": row["from_addr"] or row["smtp_user"],
                "secure":    bool(row.get("secure")),
            }
    except Exception:
        pass
    return dict(EMAIL_CONFIG)


def _safe_smtp_cfg(cfg=None):
    cfg = dict(cfg or _get_smtp_cfg())
    if cfg.get("smtp_pass"):
        cfg["smtp_pass"] = "••••••••"
    return cfg


def _get_client_cfg():
    try:
        row = mysql_fetchone(
            "SELECT * FROM comm_client_config ORDER BY id DESC LIMIT 1"
        )
        if row:
            return dict(row)
    except Exception:
        pass
    return {"company_name": "ILUS Sport & Health", "corp_color": "#CC0000"}


def _get_wa_cfg():
    try:
        row = mysql_fetchone(
            "SELECT * FROM comm_whatsapp_config ORDER BY id DESC LIMIT 1"
        )
        if row:
            return dict(row)
    except Exception:
        pass
    return {}


def _comm_log_entry(canal, dest, asunto, estado, detalle=""):
    try:
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO comm_log (canal,destinatario,asunto,estado,detalle,enviado_por) "
                "VALUES (%s,%s,%s,%s,%s,%s)",
                (canal, dest[:300], (asunto or "")[:500], estado,
                 (detalle or "")[:2000], current_username()),
            )
        conn.commit()
    except Exception:
        pass


def _send_email_dinamico(to, subject, html_body, cfg=None):
    """Envía email usando config SMTP dinámica (DB o config.py)."""
    import ssl as _ssl
    cfg = cfg or _get_smtp_cfg()
    cc = _get_client_cfg()
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = f"{cfg['from_name']} <{cfg.get('from_addr', cfg['smtp_user'])}>"
    msg["To"]      = to
    recipients = [to]
    if cc.get("reply_to"):
        msg["Reply-To"] = cc["reply_to"]
    cc_addrs = [a.strip() for a in (cc.get("email_cc") or "").replace(";", ",").split(",") if a.strip()]
    bcc_addrs = [a.strip() for a in (cc.get("email_bcc") or "").replace(";", ",").split(",") if a.strip()]
    if cc_addrs:
        msg["Cc"] = ", ".join(cc_addrs)
    recipients.extend(cc_addrs + bcc_addrs)
    msg.attach(MIMEText(html_body, "html", "utf-8"))
    host   = cfg["smtp_host"]
    port   = int(cfg.get("smtp_port", 587))
    secure = cfg.get("secure", False)
    if secure:
        ctx = _ssl.create_default_context()
        with smtplib.SMTP_SSL(host, port, context=ctx, timeout=15) as srv:
            srv.login(cfg["smtp_user"], cfg["smtp_pass"])
            srv.sendmail(cfg.get("from_addr", cfg["smtp_user"]), recipients, msg.as_string())
    else:
        with smtplib.SMTP(host, port, timeout=15) as srv:
            srv.ehlo()
            srv.starttls()
            srv.login(cfg["smtp_user"], cfg["smtp_pass"])
            srv.sendmail(cfg.get("from_addr", cfg["smtp_user"]), recipients, msg.as_string())


def _smtp_cfg_from_request(data=None):
    data = data or {}
    prev = _get_smtp_cfg()
    smtp_pass = (data.get("pass") or "").strip()
    is_masked = smtp_pass.startswith("â€¢") or smtp_pass == "••••••••" or set(smtp_pass or "") == {"•"}
    bullet = chr(8226)
    is_masked = is_masked or smtp_pass == (bullet * 8) or set(smtp_pass or "") == {bullet}
    if is_masked:
        smtp_pass = ""
    if not smtp_pass or smtp_pass == "••••••••":
        smtp_pass = prev.get("smtp_pass") or ""
    host = (data.get("host") or prev.get("smtp_host") or "smtp.gmail.com").strip()
    port = int(data.get("port") or prev.get("smtp_port") or 587)
    secure = bool(data.get("secure")) if "secure" in data else bool(prev.get("secure"))
    if "gmail.com" in host.lower():
        if port == 587 and secure:
            secure = False
        elif port == 465 and not secure:
            secure = True
    return {
        "smtp_host": host,
        "smtp_port": port,
        "smtp_user": (data.get("user") or prev.get("smtp_user") or "").strip(),
        "smtp_pass": smtp_pass,
        "from_name": (data.get("fromName") or prev.get("from_name") or "ILUS Sport & Health").strip(),
        "from_addr": (data.get("fromAddr") or prev.get("from_addr") or data.get("user") or prev.get("smtp_user") or "").strip(),
        "secure": secure,
    }


def _smtp_connection_diagnose(cfg):
    """Valida conexion SMTP y login sin enviar ningun correo."""
    import socket
    import ssl as _ssl

    host = (cfg.get("smtp_host") or "").strip()
    port = int(cfg.get("smtp_port") or 0)
    user = (cfg.get("smtp_user") or "").strip()
    password = cfg.get("smtp_pass") or ""
    secure = bool(cfg.get("secure"))

    checks = []
    suggestions = []
    if not host:
        return {"ok": False, "stage": "config", "message": "Falta el servidor SMTP.", "suggestions": ["Ingresa smtp.gmail.com para Gmail."]}
    if not port:
        return {"ok": False, "stage": "config", "message": "Falta el puerto SMTP.", "suggestions": ["Usa 587 con STARTTLS o 465 con SSL."]}
    if not user:
        return {"ok": False, "stage": "config", "message": "Falta el email usuario SMTP.", "suggestions": ["Ingresa la cuenta que autentica el correo."]}
    if not password:
        return {"ok": False, "stage": "config", "message": "Falta la contraseña / App Password.", "suggestions": ["Guarda una App Password de Gmail de 16 caracteres."]}

    gmail = "gmail.com" in host.lower()
    if gmail and secure and port != 465:
        suggestions.append("Para Gmail con SSL/TLS marcado usa puerto 465, o desmarca SSL/TLS y usa puerto 587.")
    if gmail and not secure and port != 587:
        suggestions.append("Para Gmail con STARTTLS normalmente se usa puerto 587 y SSL/TLS desmarcado.")
    if gmail and " " in password:
        suggestions.append("Gmail permite mostrar la App Password con espacios, pero si falla prueba guardarla sin espacios.")

    try:
        if secure:
            checks.append(f"Conectando con SSL a {host}:{port}")
            ctx = _ssl.create_default_context()
            with smtplib.SMTP_SSL(host, port, context=ctx, timeout=10) as srv:
                checks.append("Servidor SSL respondio correctamente")
                srv.ehlo()
                checks.append("Autenticando usuario SMTP")
                srv.login(user, password)
        else:
            checks.append(f"Conectando a {host}:{port}")
            with smtplib.SMTP(host, port, timeout=10) as srv:
                srv.ehlo()
                if srv.has_extn("starttls"):
                    checks.append("STARTTLS disponible; negociando cifrado")
                    ctx = _ssl.create_default_context()
                    srv.starttls(context=ctx)
                    srv.ehlo()
                else:
                    checks.append("El servidor no anuncia STARTTLS")
                    suggestions.append("Si el servidor exige cifrado, activa SSL/TLS con puerto 465 o usa puerto 587 con STARTTLS.")
                checks.append("Autenticando usuario SMTP")
                srv.login(user, password)
        checks.append("Credenciales aceptadas. La aplicacion puede enviar correos.")
        return {
            "ok": True,
            "stage": "auth",
            "message": "Conexion SMTP verificada. Host, puerto, cifrado y credenciales estan correctos.",
            "checks": checks,
            "suggestions": suggestions,
        }
    except smtplib.SMTPAuthenticationError as exc:
        code = getattr(exc, "smtp_code", "")
        detail = (getattr(exc, "smtp_error", b"") or b"").decode("utf-8", "ignore")
        hints = [
            "Revisa que el email usuario sea correcto.",
            "Si usas Gmail, no uses tu clave normal: crea una App Password de 16 caracteres.",
            "Verifica que la verificacion en 2 pasos este activa en la cuenta Gmail.",
        ]
        return {"ok": False, "stage": "auth", "message": f"Autenticacion rechazada por el servidor SMTP ({code}).", "detail": detail, "checks": checks, "suggestions": hints + suggestions}
    except (smtplib.SMTPConnectError, smtplib.SMTPServerDisconnected, socket.timeout, TimeoutError) as exc:
        hints = [
            "Revisa host y puerto.",
            "Para Gmail usa 587 sin SSL/TLS marcado, o 465 con SSL/TLS marcado.",
            "Si esta en Railway o hosting, confirma que el proveedor permita conexiones SMTP salientes.",
        ]
        return {"ok": False, "stage": "connect", "message": "No se pudo conectar correctamente al servidor SMTP.", "detail": str(exc), "checks": checks, "suggestions": hints + suggestions}
    except _ssl.SSLError as exc:
        hints = [
            "Hay una mezcla incorrecta entre puerto y tipo de cifrado.",
            "Para 587 desmarca SSL/TLS; para 465 marca SSL/TLS.",
        ]
        return {"ok": False, "stage": "tls", "message": "Error de SSL/TLS al negociar la conexion.", "detail": str(exc), "checks": checks, "suggestions": hints + suggestions}
    except OSError as exc:
        hints = [
            "El host o puerto no responde desde esta aplicacion.",
            "Revisa si el firewall o el hosting bloquea SMTP saliente.",
            "Prueba Gmail con smtp.gmail.com puerto 587 y SSL/TLS desmarcado.",
        ]
        return {"ok": False, "stage": "network", "message": "Error de red conectando al SMTP.", "detail": str(exc), "checks": checks, "suggestions": hints + suggestions}
    except smtplib.SMTPException as exc:
        return {"ok": False, "stage": "smtp", "message": "El servidor SMTP respondio con un error.", "detail": str(exc), "checks": checks, "suggestions": suggestions}


def _comm_render_email_document(title, body_html, subtitle=""):
    """Renderiza un fragmento HTML dentro del diseÃ±o corporativo actual."""
    cc = _get_client_cfg()
    color = cc.get("corp_color") or "#CC0000"
    company = cc.get("company_name") or "ILUS Sport & Health"
    logo = cc.get("logo_url") or ""
    header = _email_header_ilus(title, subtitle, color, logo, company)
    body = _email_body_section(body_html)
    footer = _email_footer_ilus(company)
    return _email_wrapper(_email_card(header, body, footer), company)


def _send_whatsapp(account_sid, auth_token, from_num, to_num, body):
    """Envía WhatsApp vía Twilio. Lanza RuntimeError si twilio no está instalado."""
    import re as _re
    try:
        from twilio.rest import Client as _TwilioClient
    except ImportError:
        raise RuntimeError(
            "El paquete 'twilio' no está instalado. "
            "Ejecuta: pip install twilio"
        )
    m = _re.search(r'(AC[a-f0-9]{32})', account_sid or "", _re.I)
    if m:
        account_sid = m.group(1)
    if not account_sid.upper().startswith("AC"):
        raise ValueError("El Account SID debe comenzar con 'AC'")
    if auth_token in ("[AuthToken]", "", None):
        raise ValueError("Ingresa un Auth Token válido")
    from_num = from_num if from_num.startswith("whatsapp:") else f"whatsapp:{from_num}"
    to_num   = to_num   if to_num.startswith("whatsapp:")   else f"whatsapp:{to_num}"
    client   = _TwilioClient(account_sid, auth_token)
    msg      = client.messages.create(from_=from_num, to=to_num, body=body)
    return msg.sid


# ── Templates de email ────────────────────────────────────────

def _email_wrapper(inner, company="ILUS Sport & Health"):
    return f"""<!DOCTYPE html><html lang="es"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Comunicacion - {company}</title></head>
<body style="margin:0;padding:0;background:#f1f2f4;font-family:Arial,Helvetica,sans-serif;color:#111827">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#f1f2f4;padding:28px 12px">
<tr><td align="center">{inner}</td></tr>
</table></body></html>"""


def _email_card(header_html, body_html, footer_html="", width=580):
    return f"""
<table width="{width}" cellpadding="0" cellspacing="0"
       style="background:#ffffff;border-radius:10px;overflow:hidden;
              max-width:{width}px;width:100%;box-shadow:0 6px 20px rgba(15,23,42,.10)">
  <tr><td>{header_html}</td></tr>
  <tr><td style="background:#fff">{body_html}</td></tr>
  {"<tr><td>" + footer_html + "</td></tr>" if footer_html else ""}
</table>"""


def _email_header_ilus(title, subtitle="", corp_color="#CC0000", logo_url=None, company="ILUS"):
    logo_url = logo_url or "https://ilusfitness.com/cdn/shop/files/Logo_ILUS_Fitness_Blanco_equipamiento_para_gimnasios.png"
    return f"""
<table width="100%" cellpadding="0" cellspacing="0">
  <tr>
    <td style="background:#000;padding:24px 28px;text-align:center">
      <img src="{logo_url}" alt="{company}"
           style="height:48px;max-width:230px;width:auto;display:block;margin:0 auto;object-fit:contain">
    </td>
  </tr>
  <tr>
    <td style="background:#111;padding:28px 32px;text-align:center;border-top:1px solid #202020">
      <div style="color:#ffffff;font-size:22px;line-height:1.25;font-weight:800">{title}</div>
      {"<div style='color:#f3f4f6;font-size:13px;line-height:1.45;margin-top:8px'>" + subtitle + "</div>" if subtitle else ""}
    </td>
  </tr>
</table>"""


def _email_body_section(content):
    return f'<div style="padding:32px 30px;font-size:14px;line-height:1.6;color:#111827">{content}</div>'


def _email_footer_ilus(company="ILUS Sport & Health"):
    return f"""
<table width="100%" cellpadding="0" cellspacing="0"
       style="background:#000">
  <tr>
    <td style="padding:24px 32px;text-align:center;color:#6b7280;font-size:11px;line-height:1.6">
      <div style="color:#DC143C;font-size:13px;font-weight:700;text-transform:uppercase">{company}</div>
      <div style="color:#9ca3af;margin-top:5px">Equipamiento profesional para alto rendimiento</div>
      <div style="color:#6b7280;margin-top:14px">
        Este correo fue generado automaticamente. Para soporte, utiliza nuestros canales oficiales.
      </div>
    </td>
  </tr>
</table>"""


def _email_info_box(rows, corp_color="#CC0000"):
    """rows = [['Label', 'Valor'], ...]"""
    items = "".join(
        f"<tr><td style='padding:6px 0;color:#888;font-size:12px;width:38%'>{r[0]}</td>"
        f"<td style='padding:6px 0;font-size:13px;font-weight:600;color:#1a1a1a'>{r[1]}</td></tr>"
        for r in rows
    )
    return f"""
<table width="100%" cellpadding="0" cellspacing="0"
       style="background:#fff8f5;border-left:4px solid {corp_color};
              border-radius:4px;padding:12px 16px;margin:16px 0">
  <tr><td><table width="100%" cellpadding="0" cellspacing="0">{items}</table></td></tr>
</table>"""


def _email_btn(text, url, corp_color="#CC0000"):
    return f"""
<table cellpadding="0" cellspacing="0" style="margin:20px 0">
  <tr>
    <td style="border-radius:8px;background:{corp_color};padding:12px 28px;text-align:center">
      <a href="{url}" style="color:#fff;font-size:14px;font-weight:700;
                             text-decoration:none;display:block">{text}</a>
    </td>
  </tr>
</table>"""


def tpl_email_prueba(sender_name, company="ILUS Sport & Health", corp_color="#CC0000"):
    header = _email_header_ilus("✅ Conexión SMTP verificada", "Prueba de correo", corp_color)
    body   = _email_body_section(f"""
      <p style="font-size:15px;color:#1a1a1a;margin:0 0 16px">
        ¡Hola! Este correo confirma que la configuración SMTP está funcionando correctamente.
      </p>
      {_email_info_box([
          ["Enviado por", sender_name],
          ["Servidor", "SMTP dinámico — ILUS Comunicaciones"],
      ], corp_color)}
      <p style="font-size:12px;color:#999;margin:20px 0 0">
        Si recibiste este mensaje, la integración de email está activa y lista para usar.
      </p>""")
    footer = _email_footer_ilus(company)
    return _email_wrapper(_email_card(header, body, footer), company)


def tpl_email_estado_pedido(data):
    """Template: actualización de estado a cliente."""
    cc    = _get_client_cfg()
    color = cc.get("corp_color", "#CC0000")
    co    = cc.get("company_name", "ILUS Sport & Health")
    logo  = cc.get("logo_url", "")
    estado_badge = {
        "En preparación": "🔵", "En ruta": "🚚", "Entregado": "✅",
        "Entrega fallida": "❌", "Pendiente": "⏳",
    }.get(data.get("status", ""), "📦")
    header = _email_header_ilus(
        f"{estado_badge} {data.get('status','Actualización')}",
        f"Pedido #{data.get('trackingCode','')}", color, logo, co
    )
    rows = [
        ["Tracking", data.get("trackingCode", "—")],
        ["Estado",   data.get("status", "—")],
    ]
    if data.get("eta"):
        rows.append(["Entrega estimada", data["eta"]])
    if data.get("conductorName"):
        rows.append(["Conductor", data["conductorName"]])
    if data.get("conductorPhone"):
        rows.append(["Teléfono conductor", data["conductorPhone"]])
    btn = _email_btn("Rastrear pedido", data.get("trackingUrl", "#"), color) if data.get("trackingUrl") else ""
    body = _email_body_section(
        f"<p style='font-size:15px;color:#1a1a1a;margin:0 0 16px'>"
        f"Hola <strong>{data.get('customerName','')}</strong>, te informamos sobre el estado de tu pedido.</p>"
        + _email_info_box(rows, color) + btn
    )
    return _email_wrapper(_email_card(header, body, _email_footer_ilus(co)), co)


# ── RUTAS: COMUNICACIONES ─────────────────────────────────────

@app.route("/comunicaciones/")
@_require_superadmin
def comm_index():
    smtp_cfg  = _get_smtp_cfg()
    client_cfg = _get_client_cfg()
    wa_cfg    = _get_wa_cfg()
    log_rows  = []
    try:
        log_rows = mysql_fetchall(
            "SELECT * FROM comm_log ORDER BY created_at DESC LIMIT 80"
        )
    except Exception:
        pass
    # Ocultar contraseña
    smtp_cfg_safe = _safe_smtp_cfg(smtp_cfg)
    resp = make_response(render_template(
        "comunicaciones/index.html",
        smtp_cfg=smtp_cfg_safe,
        client_cfg=client_cfg,
        wa_cfg=wa_cfg,
        log_rows=log_rows,
    ))
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp


@app.route("/comunicaciones/smtp/config", methods=["POST"])
@_require_superadmin
def comm_smtp_save():
    d = request.get_json(silent=True) or {}
    cfg = _smtp_cfg_from_request(d)
    prev = cfg
    smtp_pass = cfg["smtp_pass"]
    if not smtp_pass or smtp_pass == "••••••••":
        smtp_pass = prev.get("smtp_pass") or ""
    host = cfg["smtp_host"]
    port = cfg["smtp_port"]
    user = cfg["smtp_user"]
    from_name = cfg["from_name"]
    from_addr = (cfg["from_addr"] or user).strip()
    secure = 1 if cfg["secure"] else 0
    if not user:
        return jsonify({"error": "Ingresa el email usuario SMTP"}), 400
    if not smtp_pass:
        return jsonify({"error": "Ingresa y guarda la App Password"}), 400
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO comm_smtp_config
               (id,smtp_host,smtp_port,smtp_user,smtp_pass,from_name,from_addr,secure,updated_by)
               VALUES (1,%s,%s,%s,%s,%s,%s,%s,%s)
               ON DUPLICATE KEY UPDATE
                 smtp_host=VALUES(smtp_host),
                 smtp_port=VALUES(smtp_port),
                 smtp_user=VALUES(smtp_user),
                 smtp_pass=VALUES(smtp_pass),
                 from_name=VALUES(from_name),
                 from_addr=VALUES(from_addr),
                 secure=VALUES(secure),
                 updated_by=VALUES(updated_by)""",
            (host, port, user, smtp_pass, from_name, from_addr, secure, current_username()),
        )
        cur.execute("DELETE FROM comm_smtp_config WHERE id <> 1")
    conn.commit()
    warning = ""
    if "gmail.com" in host.lower() and port == 587 and d.get("secure"):
        warning = "Para Gmail con puerto 587 se usa STARTTLS; se desmarco SSL/TLS automaticamente."
    return jsonify({"ok": True, "warning": warning, "smtp": _safe_smtp_cfg({
        "smtp_host": host,
        "smtp_port": port,
        "smtp_user": user,
        "smtp_pass": smtp_pass,
        "from_name": from_name,
        "from_addr": from_addr,
        "secure": bool(secure),
    })})


@app.route("/comunicaciones/smtp/test", methods=["POST"])
@_require_superadmin
def comm_smtp_test():
    d = request.get_json(silent=True) or {}
    cfg = _smtp_cfg_from_request(d)
    try:
        result = _smtp_connection_diagnose(cfg)
        _comm_log_entry(
            "email",
            cfg.get("smtp_user") or "",
            "Diagnostico SMTP",
            "ok" if result.get("ok") else "error",
            result.get("message", ""),
        )
        return jsonify(result), (200 if result.get("ok") else 422)
    except Exception as exc:
        _comm_log_entry("email", cfg.get("smtp_user") or "", "Diagnostico SMTP", "error", str(exc))
        return jsonify({
            "ok": False,
            "stage": "internal",
            "message": "No se pudo ejecutar el diagnostico SMTP.",
            "detail": str(exc),
        }), 500


def _comm_smtp_test_send_legacy():
    d  = request.get_json(silent=True) or {}
    to = (d.get("to") or "").strip()
    if not to:
        return jsonify({"error": "Ingresa un destinatario"}), 400
    html = _ilus_email_html(
        titulo           = "✅ Prueba SMTP",
        subtitulo        = "Verificación de conexión — ILUS Comunicaciones",
        saludo           = "¡Conexión verificada!",
        parrafos         = [
            "Este correo confirma que la configuración SMTP está funcionando correctamente.",
            "Si lo recibiste, la integración de email está activa y lista para usar.",
        ],
        info_lineas      = [
            ("", "Enviado por", current_username()),
            ("", "Servidor",    "SMTP dinámico — ILUS Comunicaciones"),
        ],
    )
    try:
        _send_ilus_email(to, "🧪 Prueba SMTP — ILUS Comunicaciones", html)
        _comm_log_entry("email", to, "Prueba SMTP", "ok")
        return jsonify({"ok": True})
    except Exception as exc:
        _comm_log_entry("email", to, "Prueba SMTP", "error", str(exc))
        return jsonify({"error": str(exc)}), 500


@app.route("/comunicaciones/email/enviar", methods=["POST"])
@_require_superadmin
def comm_email_enviar():
    d       = request.get_json(silent=True) or {}
    to      = (d.get("to") or "").strip()
    subject = (d.get("subject") or "").strip()
    html    = (d.get("html") or "").strip()
    if d.get("wrap"):
        html = _comm_render_email_document(subject, html, "Comunicaciones")
    if not all([to, subject, html]):
        return jsonify({"error": "Faltan campos: to, subject, html"}), 400
    try:
        _send_email_dinamico(to, subject, html)
        _comm_log_entry("email", to, subject, "ok")
        return jsonify({"ok": True})
    except Exception as exc:
        _comm_log_entry("email", to, subject, "error", str(exc))
        return jsonify({"error": str(exc)}), 500


@app.route("/comunicaciones/email/preview", methods=["POST"])
@_require_superadmin
def comm_email_preview():
    d = request.get_json(silent=True) or {}
    title = (d.get("title") or "Vista previa de comunicacion").strip()
    body = (d.get("html") or "").strip() or (
        "<p style='margin:0 0 14px;font-size:14px;color:#111827;line-height:1.65'>"
        "Hola <strong>{{nombre_cliente}}</strong>, este es un ejemplo del mensaje que recibira el destinatario usando la plantilla oficial ILUS.</p>"
        "<table cellpadding='0' cellspacing='0' width='100%' style='background:#f5f5f7;border-left:4px solid #CC0000;border-radius:4px;padding:14px 18px;margin:18px 0'>"
        "<tr><td style='padding:5px 0;font-size:13px;color:#555'><strong style='color:#222'>Pedido:</strong>&nbsp; {{id_pedido}}</td></tr>"
        "<tr><td style='padding:5px 0;font-size:13px;color:#555'><strong style='color:#222'>Estado:</strong>&nbsp; En ruta</td></tr>"
        "<tr><td style='padding:5px 0;font-size:13px;color:#555'><strong style='color:#222'>Courier:</strong>&nbsp; {{courier}}</td></tr>"
        "</table>"
    )
    resp = jsonify({"ok": True, "html": _comm_render_email_document(
        title,
        body,
        "Gestion de solicitudes y seguimiento"
    )})
    resp.headers["Cache-Control"] = "no-store"
    return resp


@app.route("/comunicaciones/cliente/config", methods=["POST"])
@_require_superadmin
def comm_client_save():
    d = request.get_json(silent=True) or {}
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute("DELETE FROM comm_client_config")
        cur.execute(
            """INSERT INTO comm_client_config
               (company_name,reply_to,support_email,support_phone,
                tracking_url,logo_url,corp_color,email_cc,email_bcc,updated_by)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (
                (d.get("company_name") or "ILUS Sport & Health").strip(),
                (d.get("reply_to") or "").strip(),
                (d.get("support_email") or "").strip(),
                (d.get("support_phone") or "").strip(),
                (d.get("tracking_url") or "").strip(),
                (d.get("logo_url") or "").strip(),
                (d.get("corp_color") or "#CC0000").strip(),
                (d.get("email_cc") or "").strip(),
                (d.get("email_bcc") or "").strip(),
                current_username(),
            ),
        )
    conn.commit()
    return jsonify({"ok": True, "client": _get_client_cfg()})


@app.route("/comunicaciones/whatsapp/config", methods=["POST"])
@_require_superadmin
def comm_wa_save():
    d = request.get_json(silent=True) or {}
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute("DELETE FROM comm_whatsapp_config")
        cur.execute(
            """INSERT INTO comm_whatsapp_config
               (account_sid,auth_token,from_number,biz_number,updated_by)
               VALUES (%s,%s,%s,%s,%s)""",
            (
                (d.get("account_sid") or "").strip(),
                (d.get("auth_token") or "").strip(),
                (d.get("from_number") or "").strip(),
                (d.get("biz_number") or "").strip(),
                current_username(),
            ),
        )
    conn.commit()
    return jsonify({"ok": True})


@app.route("/comunicaciones/whatsapp/test", methods=["POST"])
@_require_superadmin
def comm_wa_test():
    d     = request.get_json(silent=True) or {}
    saved = _get_wa_cfg()
    sid   = (d.get("account_sid") or saved.get("account_sid") or "").strip()
    tok   = (d.get("auth_token")  or saved.get("auth_token")  or "").strip()
    frm   = (d.get("from_number") or saved.get("from_number") or "").strip()
    to    = (d.get("to") or "").strip()
    if not all([sid, tok, frm, to]):
        return jsonify({"error": "Completa: Account SID, Auth Token, From y destinatario"}), 400
    body = (f"✅ Prueba de WhatsApp — ILUS Comunicaciones\n"
            f"Enviado por: {current_username()}")
    body = (d.get("body") or "").strip() or body
    try:
        msg_sid = _send_whatsapp(sid, tok, frm, to, body)
        _comm_log_entry("whatsapp", to, "Prueba WA", "ok", msg_sid)
        return jsonify({"ok": True, "msg_sid": msg_sid})
    except Exception as exc:
        detail = str(exc)
        code = getattr(exc, "code", None) or getattr(exc, "status", None)
        more_info = getattr(exc, "more_info", "") or ""
        suggestions = []
        if "not installed" in detail.lower() or "twilio" in detail.lower() and "instal" in detail.lower():
            suggestions.append("Instala Twilio en el mismo Python que ejecuta la app y reinicia el servidor.")
        if str(code) in {"21211", "21614"} or "not a valid phone number" in detail.lower():
            suggestions.append("Revisa que el destinatario tenga formato internacional, por ejemplo +56912345678.")
        if str(code) in {"63015", "63016"} or "sandbox" in detail.lower():
            suggestions.append("En sandbox, el destinatario debe enviar primero el codigo join al numero +1 415 523 8886.")
        if str(code) in {"20003", "20404"} or "authenticate" in detail.lower() or "credentials" in detail.lower():
            suggestions.append("Revisa Account SID y Auth Token desde el Dashboard de Twilio.")
        if frm.startswith("whatsapp:+14155238886"):
            suggestions.append("Estas usando sandbox Twilio; confirma que el telefono destino ya se unio al sandbox.")
        _comm_log_entry("whatsapp", to, "Prueba WA", "error", detail)
        return jsonify({
            "error": detail,
            "code": code,
            "more_info": more_info,
            "suggestions": suggestions,
        }), 500


@app.route("/comunicaciones/templates", methods=["GET"])
@_require_superadmin
def comm_templates_get():
    """Devuelve todas las plantillas agrupadas por estado."""
    rows = mysql_fetchall(
        "SELECT * FROM comm_templates ORDER BY estado, canal"
    ) or []
    legacy_tokens = ("Dropit", "direccion_origen", "direccion_destino", "link_tracking", "nombre_conductor")
    if any(any(tok in ((r.get("asunto") or "") + " " + (r.get("cuerpo") or "")) for tok in legacy_tokens) for r in rows):
        _comm_seed_default_templates(overwrite=True)
        rows = mysql_fetchall(
            "SELECT * FROM comm_templates ORDER BY estado, canal"
        ) or []
    data = {}
    for r in rows:
        est = r["estado"]
        if est not in data:
            data[est] = {}
        data[est][r["canal"]] = {
            "id":     r["id"],
            "asunto": r.get("asunto") or "",
            "cuerpo": r.get("cuerpo") or "",
            "activo": r.get("activo", 1),
        }
    return jsonify(data)


@app.route("/comunicaciones/templates/<estado>/<canal>", methods=["PUT"])
@_require_superadmin
def comm_template_save(estado, canal):
    """Guarda/actualiza una plantilla para estado + canal."""
    if canal not in ("email", "whatsapp"):
        return jsonify({"error": "Canal inválido"}), 400
    d      = request.get_json(silent=True) or {}
    asunto = d.get("asunto", "")
    cuerpo = d.get("cuerpo", "")
    user   = current_username()
    conn   = get_db()
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO comm_templates (estado, canal, asunto, cuerpo, updated_by)
               VALUES (%s,%s,%s,%s,%s)
               ON DUPLICATE KEY UPDATE
                 asunto=VALUES(asunto), cuerpo=VALUES(cuerpo), updated_by=VALUES(updated_by)""",
            (estado, canal, asunto, cuerpo, user)
        )
    conn.commit()
    return jsonify({"ok": True})


@app.route("/comunicaciones/templates/restaurar-todo", methods=["POST"])
@_require_superadmin
def comm_templates_restore_all():
    """Restaura todas las plantillas a la base oficial ILUS."""
    _comm_seed_default_templates(overwrite=True)
    return jsonify({"ok": True})


# ══════════════════════════════════════════════════════════════
#  MÓDULO: MANTENCIONES
#  Acceso: superadmin + ejecutivo
# ══════════════════════════════════════════════════════════════

def _mant_required(view):
    """Decorador: requiere permiso 'mantenciones'."""
    @wraps(view)
    def wrapped(*args, **kwargs):
        if not g.get("permissions", {}).get("mantenciones"):
            return redirect(url_for("index"))
        return view(*args, **kwargs)
    return login_required(wrapped)


def init_mantenciones_tables():
    conn = get_mysql()
    try:
        with conn.cursor() as cur:
            # ── Clientes de mantención ──────────────────────────────
            cur.execute("""
                CREATE TABLE IF NOT EXISTS mant_clientes (
                    id               INT AUTO_INCREMENT PRIMARY KEY,
                    razon_social     VARCHAR(200) NOT NULL,
                    rut              VARCHAR(20),
                    contacto_nombre  VARCHAR(200),
                    contacto_tel     VARCHAR(50),
                    contacto_email   VARCHAR(200),
                    direccion        TEXT,
                    comuna           VARCHAR(100),
                    ciudad           VARCHAR(100),
                    notas            TEXT,
                    estado           ENUM('activo','inactivo','prospecto') DEFAULT 'activo',
                    created_by       VARCHAR(190),
                    created_at       DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at       DATETIME DEFAULT CURRENT_TIMESTAMP
                                     ON UPDATE CURRENT_TIMESTAMP,
                    INDEX idx_rut    (rut),
                    INDEX idx_estado (estado)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            # ── Máquinas por cliente (de docs ERP) ─────────────────
            cur.execute("""
                CREATE TABLE IF NOT EXISTS mant_maquinas (
                    id           INT AUTO_INCREMENT PRIMARY KEY,
                    cliente_id   INT NOT NULL,
                    sku          VARCHAR(100),
                    nombre       VARCHAR(400),
                    serie        VARCHAR(100),
                    doc_origen   VARCHAR(80),
                    doc_fecha    DATE,
                    cantidad     INT DEFAULT 1,
                    notas        TEXT,
                    estado       ENUM('activo','baja','garantia') DEFAULT 'activo',
                    created_by   VARCHAR(190),
                    created_at   DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (cliente_id) REFERENCES mant_clientes(id) ON DELETE CASCADE,
                    INDEX idx_cliente (cliente_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            # ── Contratos ───────────────────────────────────────────
            cur.execute("""
                CREATE TABLE IF NOT EXISTS mant_contratos (
                    id                  INT AUTO_INCREMENT PRIMARY KEY,
                    cliente_id          INT NOT NULL,
                    nombre              VARCHAR(200),
                    archivo_nombre      VARCHAR(300),
                    archivo_path        VARCHAR(500),
                    archivo_tipo        ENUM('pdf','word','otro') DEFAULT 'pdf',
                    fecha_inicio        DATE,
                    fecha_vencimiento   DATE,
                    es_indefinido       TINYINT(1) DEFAULT 0,
                    monto_mensual       DECIMAL(12,2),
                    monto_anual         DECIMAL(12,2),
                    frecuencia_meses    INT COMMENT 'Frecuencia mantencion en meses',
                    notas               TEXT,
                    ai_analizado        TINYINT(1) DEFAULT 0,
                    ai_fecha            DATETIME,
                    ai_resumen          TEXT,
                    ai_puntos_criticos  TEXT COMMENT 'JSON array',
                    ai_alertas          TEXT COMMENT 'JSON array',
                    ai_frecuencia_sug   INT,
                    ai_score            INT COMMENT '0-100 calidad contrato',
                    estado              ENUM('vigente','vencido','por_vencer','indefinido')
                                        DEFAULT 'vigente',
                    created_by          VARCHAR(190),
                    created_at          DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at          DATETIME DEFAULT CURRENT_TIMESTAMP
                                        ON UPDATE CURRENT_TIMESTAMP,
                    FOREIGN KEY (cliente_id) REFERENCES mant_clientes(id) ON DELETE CASCADE,
                    INDEX idx_cliente     (cliente_id),
                    INDEX idx_vencimiento (fecha_vencimiento)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            # ── Visitas / agenda ────────────────────────────────────
            cur.execute("""
                CREATE TABLE IF NOT EXISTS mant_visitas (
                    id                INT AUTO_INCREMENT PRIMARY KEY,
                    cliente_id        INT NOT NULL,
                    contrato_id       INT,
                    titulo            VARCHAR(200),
                    fecha_programada  DATE NOT NULL,
                    fecha_realizada   DATE,
                    hora_inicio       TIME,
                    hora_fin          TIME,
                    tecnico           VARCHAR(200),
                    tipo              ENUM('preventiva','correctiva','garantia','inspeccion')
                                      DEFAULT 'preventiva',
                    estado            ENUM('programada','completada','cancelada','reagendada')
                                      DEFAULT 'programada',
                    descripcion       TEXT,
                    observaciones     TEXT,
                    costo             DECIMAL(10,2),
                    created_by        VARCHAR(190),
                    created_at        DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at        DATETIME DEFAULT CURRENT_TIMESTAMP
                                      ON UPDATE CURRENT_TIMESTAMP,
                    FOREIGN KEY (cliente_id)  REFERENCES mant_clientes(id)  ON DELETE CASCADE,
                    FOREIGN KEY (contrato_id) REFERENCES mant_contratos(id) ON DELETE SET NULL,
                    INDEX idx_cliente (cliente_id),
                    INDEX idx_fecha   (fecha_programada),
                    INDEX idx_estado  (estado)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            # ── Log de actividad ────────────────────────────────────
            cur.execute("""
                CREATE TABLE IF NOT EXISTS mant_logs (
                    id          INT AUTO_INCREMENT PRIMARY KEY,
                    entidad     ENUM('cliente','maquina','contrato','visita') NOT NULL,
                    entidad_id  INT NOT NULL,
                    accion      VARCHAR(100) NOT NULL,
                    detalle     TEXT,
                    usuario     VARCHAR(190),
                    created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_entidad (entidad, entidad_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)

            # ── Adjuntos de contrato (hasta 4 extra) ───────────────
            cur.execute("""
                CREATE TABLE IF NOT EXISTS mant_adjuntos (
                    id              INT AUTO_INCREMENT PRIMARY KEY,
                    contrato_id     INT NOT NULL,
                    nombre_original VARCHAR(300),
                    archivo_path    VARCHAR(500),
                    tipo            VARCHAR(50),
                    created_by      VARCHAR(190),
                    created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (contrato_id) REFERENCES mant_contratos(id) ON DELETE CASCADE,
                    INDEX idx_contrato (contrato_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)

            # ── Columnas extra mant_maquinas (migracion) ───────────
            for col_sql in [
                "ALTER TABLE mant_maquinas ADD COLUMN ubicacion_cliente VARCHAR(200)",
                "ALTER TABLE mant_maquinas ADD COLUMN fecha_instalacion DATE",
                "ALTER TABLE mant_maquinas ADD COLUMN estado_op ENUM('operativo','critico','en_mantencion') DEFAULT 'operativo'",
            ]:
                try:
                    cur.execute(col_sql)
                except Exception:
                    pass  # columna ya existe

            # ── Columnas extra mant_contratos (migracion) ──────────
            for col_sql in [
                "ALTER TABLE mant_contratos ADD COLUMN sla_horas INT",
                "ALTER TABLE mant_contratos ADD COLUMN incluye_repuestos TINYINT(1) DEFAULT 0",
                "ALTER TABLE mant_contratos ADD COLUMN incluye_mant_gratis TINYINT(1) DEFAULT 0",
                "ALTER TABLE mant_contratos ADD COLUMN costo_por_mant DECIMAL(12,2)",
                "ALTER TABLE mant_contratos ADD COLUMN costo_total DECIMAL(12,2)",
                "ALTER TABLE mant_contratos ADD COLUMN nivel_riesgo ENUM('alto','medio','bajo')",
                "ALTER TABLE mant_contratos ADD COLUMN ai_tipo_contrato VARCHAR(200)",
                "ALTER TABLE mant_contratos ADD COLUMN ai_clausulas TEXT",
                "ALTER TABLE mant_contratos ADD COLUMN ai_mejoras TEXT",
                "ALTER TABLE mant_contratos ADD COLUMN ai_cobertura TEXT",
                "ALTER TABLE mant_contratos ADD COLUMN ai_editable TEXT COMMENT 'JSON campos editados por usuario'",
                "ALTER TABLE mant_contratos ADD COLUMN ai_vigencia_inicio DATE",
                "ALTER TABLE mant_contratos ADD COLUMN ai_vigencia_fin DATE",
            ]:
                try:
                    cur.execute(col_sql)
                except Exception:
                    pass  # columna ya existe

        conn.commit()
    finally:
        conn.close()


def _mant_log(entidad, entidad_id, accion, detalle=""):
    try:
        conn = get_mysql()
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO mant_logs (entidad,entidad_id,accion,detalle,usuario) "
                "VALUES (%s,%s,%s,%s,%s)",
                (entidad, entidad_id, accion, detalle, current_username())
            )
        conn.commit()
        conn.close()
    except Exception:
        pass


def _mant_actualizar_estado_contratos():
    """Actualiza automáticamente el estado de los contratos según fechas."""
    try:
        conn = get_mysql()
        hoy = datetime.now().date()
        pronto = hoy + timedelta(days=60)
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE mant_contratos SET estado =
                  CASE
                    WHEN es_indefinido = 1 THEN 'indefinido'
                    WHEN fecha_vencimiento IS NULL THEN 'vigente'
                    WHEN fecha_vencimiento < %s THEN 'vencido'
                    WHEN fecha_vencimiento <= %s THEN 'por_vencer'
                    ELSE 'vigente'
                  END
                WHERE estado NOT IN ('vencido')
                   OR fecha_vencimiento >= %s
            """, (hoy, pronto, hoy))
        conn.commit()
        conn.close()
    except Exception:
        pass


# ── DECORATOR de acceso ────────────────────────────────────────────────

# ── RUTAS PRINCIPALES ─────────────────────────────────────────────────

@app.route("/mantenciones")
@_mant_required
def mant_index():
    _mant_actualizar_estado_contratos()
    hoy   = datetime.now().date()
    pronto = hoy + timedelta(days=60)

    # KPIs
    clientes    = mysql_fetchone("SELECT COUNT(*) AS n FROM mant_clientes WHERE estado='activo'", ()) or {}
    contratos   = mysql_fetchone("SELECT COUNT(*) AS n FROM mant_contratos WHERE estado IN ('vigente','indefinido')", ()) or {}
    vencen      = mysql_fetchone("SELECT COUNT(*) AS n FROM mant_contratos WHERE estado='por_vencer'", ()) or {}
    prox_visitas = mysql_fetchall(
        "SELECT v.*, c.razon_social FROM mant_visitas v "
        "JOIN mant_clientes c ON c.id=v.cliente_id "
        "WHERE v.estado='programada' AND v.fecha_programada BETWEEN %s AND %s "
        "ORDER BY v.fecha_programada LIMIT 10",
        (hoy, hoy + timedelta(days=30))
    )
    alertas_contratos = mysql_fetchall(
        "SELECT ct.*, cl.razon_social FROM mant_contratos ct "
        "JOIN mant_clientes cl ON cl.id=ct.cliente_id "
        "WHERE ct.estado IN ('por_vencer','vencido') ORDER BY ct.fecha_vencimiento LIMIT 8",
        ()
    )
    sin_visita = mysql_fetchall(
        """SELECT c.id, c.razon_social,
                  MAX(v.fecha_realizada) AS ultima_visita
           FROM mant_clientes c
           LEFT JOIN mant_visitas v ON v.cliente_id=c.id AND v.estado='completada'
           WHERE c.estado='activo'
           GROUP BY c.id
           HAVING ultima_visita IS NULL OR ultima_visita < %s
           ORDER BY ultima_visita LIMIT 6""",
        (hoy - timedelta(days=180),)
    )
    # Ingresos del mes
    ingresos_mes = mysql_fetchone(
        "SELECT COALESCE(SUM(monto_mensual),0) AS total FROM mant_contratos "
        "WHERE estado IN ('vigente','indefinido')", ()
    ) or {}

    return render_template("mantenciones/index.html",
        kpi_clientes   = clientes.get("n", 0),
        kpi_contratos  = contratos.get("n", 0),
        kpi_vencen     = vencen.get("n", 0),
        kpi_ingresos   = float(ingresos_mes.get("total", 0)),
        prox_visitas   = [dict(r) for r in prox_visitas],
        alertas        = [dict(r) for r in alertas_contratos],
        sin_visita     = [dict(r) for r in sin_visita],
        hoy            = hoy,
    )


@app.route("/mantenciones/clientes")
@_mant_required
def mant_clientes():
    q      = request.args.get("q", "").strip()
    estado = request.args.get("estado", "activo")
    where, params = [], []
    if estado:
        where.append("estado=%s"); params.append(estado)
    if q:
        where.append("(razon_social LIKE %s OR rut LIKE %s OR contacto_email LIKE %s)")
        qp = f"%{q}%"; params += [qp, qp, qp]
    sql = "SELECT * FROM mant_clientes"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY razon_social LIMIT 200"
    rows = mysql_fetchall(sql, tuple(params))
    return render_template("mantenciones/clientes.html",
        clientes = [dict(r) for r in rows],
        filtros  = {"q": q, "estado": estado},
    )


@app.route("/mantenciones/clientes/wizard")
@_mant_required
def mant_cliente_wizard():
    """Wizard inteligente de 4 pasos para crear cliente de mantención."""
    return render_template("mantenciones/cliente_wizard.html")


def _erp_buscar_clientes(q, limit=20):
    """
    Busca clientes en el ERP por RUT o razón social.
    Normaliza el RUT (con/sin puntos) para máxima compatibilidad.
    """
    ERP_SALES = ERP_CONFIG.get("table_sales", "HEBDOC")
    # Normalizar RUT: buscar con y sin puntos
    q_like       = f"%{q}%"
    q_sin_puntos = q.replace(".", "").replace(" ", "")
    q_sin_like   = f"%{q_sin_puntos}%"
    erp_conn = get_erp_conn()
    if not erp_conn:
        return []
    try:
        with erp_conn.cursor() as cur:
            cur.execute(
                f"""SELECT DISTINCT
                       TRIM(d.NRAZON) AS razon_social,
                       TRIM(d.NRUC)   AS rut
                    FROM `{ERP_SALES}` d
                    WHERE (
                        TRIM(d.NRAZON) LIKE %s
                        OR TRIM(d.NRUC)   LIKE %s
                        OR REPLACE(REPLACE(TRIM(d.NRUC),'.',''),' ','') LIKE %s
                    )
                      AND d.TIDO IN ('FCV','BLV','NVV','VD','WEB','FCO','NVI','GDV')
                    ORDER BY d.NRAZON
                    LIMIT {int(limit)}""",
                (q_like, q_like, q_sin_like)
            )
            rows = cur.fetchall()
        return [{"razon_social": r["razon_social"], "rut": r["rut"]} for r in rows if r.get("razon_social")]
    except Exception:
        return []
    finally:
        try: erp_conn.close()
        except: pass


@app.route("/mantenciones/api/clientes/autocomplete")
@_mant_required
def mant_clientes_autocomplete():
    """
    Autocomplete unificado: busca en clientes locales (mant_clientes) + ERP.
    Devuelve lista ordenada con origen (local/erp) para mostrar en dropdown.
    """
    q = request.args.get("q", "").strip()
    if len(q) < 2:
        return jsonify([])

    # 1) Buscar en clientes locales (siempre disponible)
    q_like = f"%{q}%"
    locales = mysql_fetchall(
        """SELECT id, razon_social, rut, contacto_email, estado,
                  region, comuna, direccion, contacto_telefono
           FROM mant_clientes
           WHERE razon_social LIKE %s OR rut LIKE %s
           ORDER BY razon_social LIMIT 15""",
        (q_like, q_like)
    )
    resultados = []
    ids_rut_vistos = set()
    for r in locales:
        rut = (r.get("rut") or "").strip()
        resultados.append({
            "id":           r["id"],
            "razon_social": r["razon_social"],
            "rut":          rut,
            "email":        r.get("contacto_email",""),
            "region":       r.get("region",""),
            "comuna":       r.get("comuna",""),
            "direccion":    r.get("direccion",""),
            "telefono":     r.get("contacto_telefono",""),
            "estado":       r.get("estado",""),
            "origen":       "local",
        })
        if rut: ids_rut_vistos.add(rut)

    # 2) Buscar en ERP vía REST API /entidades (funciona desde Railway)
    TOKEN = ERP_CONFIG.get("api_token", "")
    try:
        body     = _erp_get("/entidades", {"search": q, "empresa": "01", "limit": "30"}, TOKEN, timeout=8)
        ent_list = body.get("data") or (body if isinstance(body, list) else [])
        for e in ent_list[:25]:
            rut    = (e.get("RTEN")   or "").strip()
            nombre = (e.get("NOKOEN") or "").strip()
            if not nombre:
                continue
            if rut and rut in ids_rut_vistos:
                continue
            # Capturar región, comuna, dirección y teléfono si el ERP los entrega
            region  = (e.get("NOKOREG")  or e.get("NOKOREGIO") or e.get("REGION") or "").strip()
            comuna  = (e.get("NOKOCOMU") or e.get("NOKOCOMUNADE") or e.get("COMUNA") or e.get("NOKOMUNNE") or "").strip()
            dir_    = (e.get("DIEN")     or e.get("DIRESP") or e.get("DIENDESP") or e.get("DIENDE") or "").strip()
            tel     = (e.get("FOEN")     or e.get("FONOEN") or e.get("TELEN")  or "").strip()
            resultados.append({
                "id":           None,
                "razon_social": nombre,
                "rut":          rut,
                "email":        (e.get("EMAIL") or e.get("COREN") or "").strip(),
                "region":       region,
                "comuna":       comuna,
                "direccion":    dir_,
                "telefono":     tel,
                "estado":       "",
                "origen":       "erp",
            })
            if rut: ids_rut_vistos.add(rut)
    except Exception:
        # Fallback: MySQL directo si REST falla (p.ej. entorno local)
        erp_rows = _erp_buscar_clientes(q, limit=15)
        for r in erp_rows:
            rut = (r.get("rut") or "").strip()
            if rut in ids_rut_vistos:
                continue
            resultados.append({
                "id":           None,
                "razon_social": r["razon_social"],
                "rut":          rut,
                "email":        "",
                "region":       "",
                "comuna":       "",
                "direccion":    "",
                "telefono":     "",
                "estado":       "",
                "origen":       "erp",
            })
            if rut: ids_rut_vistos.add(rut)

    # Ordenar locales primero, luego ERP
    resultados.sort(key=lambda x: (0 if x["origen"]=="local" else 1, x["razon_social"].lower()))
    return jsonify(resultados[:20])


@app.route("/mantenciones/api/erp-rut", methods=["POST"])
@_mant_required
def mant_erp_rut_lookup():
    """Busca un cliente en el ERP/local por RUT y devuelve sus datos básicos."""
    d   = request.get_json(silent=True) or {}
    rut = d.get("rut", "").strip()
    if not rut:
        return jsonify({"error": "RUT requerido"}), 400

    # 1) Buscar en clientes locales primero
    local = mysql_fetchone(
        "SELECT * FROM mant_clientes WHERE rut=%s OR rut LIKE %s LIMIT 1",
        (rut, f"%{rut.split('-')[0]}%")
    )
    if local:
        return jsonify({
            "encontrado":    True,
            "origen":        "local",
            "id":            local["id"],
            "razon_social":  local["razon_social"],
            "rut":           local["rut"] or rut,
            "direccion":     local.get("direccion",""),
            "comuna":        local.get("comuna",""),
            "ciudad":        local.get("ciudad",""),
            "email":         local.get("contacto_email",""),
            "contacto":      local.get("contacto_nombre",""),
            "tel":           local.get("contacto_tel",""),
        })

    # 2) Buscar en ERP
    rows = _erp_buscar_clientes(rut, limit=3)
    if rows:
        r = rows[0]
        return jsonify({
            "encontrado":   True,
            "origen":       "erp",
            "id":           None,
            "razon_social": r["razon_social"],
            "rut":          r["rut"],
            "direccion":    "",
            "comuna":       "",
            "ciudad":       "",
            "email":        "",
        })

    return jsonify({"encontrado": False})


_MANT_IMG_EXTS = ("jpg", "jpeg", "png", "webp")

@app.route("/mantenciones/api/agente-contrato", methods=["POST"])
@_mant_required
def mant_agente_contrato():
    """
    AGENTE IA DE CONTRATOS — lee PDF, Word o FOTO (cámara móvil) y extrae:
    - Datos del CLIENTE: RUT, razón social, dirección, contacto
    - Análisis del CONTRATO: tipo, vigencia, SLA, cláusulas, costos, riesgos
    - Equipos mencionados en el contrato
    Soporta: PDF, DOCX, JPG, PNG (foto del contrato desde celular).
    """
    f = request.files.get("archivo")
    if not f or not f.filename:
        return jsonify({"error": "Sin archivo"}), 400

    ext = f.filename.rsplit(".", 1)[-1].lower() if "." in f.filename else ""
    if ext not in ("pdf", "doc", "docx") + _MANT_IMG_EXTS:
        return jsonify({"error": "Formato no válido. Usa PDF, Word o imagen (JPG/PNG)."}), 400

    # Verificar API key antes de procesar el archivo
    ai_key = _get_ai_key()
    if not ai_key:
        return jsonify({"error": (
            "⚠️ API de IA no configurada. "
            "Agrega tu ANTHROPIC_API_KEY en Railway → Variables de entorno."
        )}), 503

    # Guardar temporalmente
    tmp_path = os.path.join(MANT_UPLOADS, f"tmp_{int(time.time())}_{secure_filename(f.filename)}")
    f.save(tmp_path)

    texto = ""
    img_b64 = None
    img_media_type = None
    is_image = ext in _MANT_IMG_EXTS

    try:
        if is_image:
            with open(tmp_path, "rb") as img_f:
                img_b64 = base64.b64encode(img_f.read()).decode()
            img_media_type = f"image/{'jpeg' if ext == 'jpg' else ext}"
        elif ext == "pdf":
            import pdfplumber
            with pdfplumber.open(tmp_path) as pdf:
                texto = "\n".join(p.extract_text() or "" for p in pdf.pages[:20])
        elif ext in ("doc", "docx"):
            import docx as _docx
            doc = _docx.Document(tmp_path)
            texto = "\n".join(p.text for p in doc.paragraphs)
    except Exception as e:
        return jsonify({"error": f"No se pudo leer el archivo: {e}"}), 500
    finally:
        try: os.remove(tmp_path)
        except: pass

    if not is_image and not texto.strip():
        return jsonify({"error": "No se pudo extraer texto del documento. Prueba subir el PDF o una foto del contrato."}), 422

    prompt_agente = """Eres un agente experto en análisis de contratos de mantención de equipos fitness en Chile (ILUS Fitness).
Tu tarea es extraer TODA la información del contrato y responder ÚNICAMENTE con JSON estructurado, sin texto adicional.

Estructura requerida:
{
  "cliente": {
    "razon_social": "nombre legal completo o null",
    "rut": "RUT con formato XX.XXX.XXX-X o null",
    "direccion": "dirección completa o null",
    "comuna": "comuna o null",
    "ciudad": "ciudad o null",
    "region": "región o null",
    "contacto_nombre": "nombre del contacto principal o null",
    "contacto_cargo": "cargo del contacto o null",
    "contacto_email": "email o null",
    "contacto_tel": "teléfono o null"
  },
  "prestador": {
    "razon_social": "empresa prestadora (usualmente ILUS) o null",
    "rut": "RUT prestador o null",
    "contacto_nombre": "técnico/representante o null"
  },
  "contrato": {
    "nombre": "nombre o título del contrato",
    "numero": "número de contrato o null",
    "tipo_contrato": "Preventivo|Correctivo|Full|Garantía|Mixto|Otro",
    "vigencia_inicio": "YYYY-MM-DD o null",
    "vigencia_fin": "YYYY-MM-DD o null",
    "es_indefinido": true_o_false,
    "renovacion_automatica": true_o_false,
    "dias_aviso_termino": número_entero_o_null,
    "frecuencia_meses": número_entero_o_null,
    "visitas_anuales": número_entero_o_null,
    "horario_atencion": "descripción horario o null",
    "sla_horas": número_o_null,
    "tiempo_respuesta_urgente_horas": número_o_null,
    "monto_mensual": número_o_null,
    "costo_por_mant": número_o_null,
    "costo_total": número_o_null,
    "moneda": "CLP|UF|USD",
    "forma_pago": "descripción o null",
    "incluye_mant_gratis": true_o_false,
    "incluye_repuestos": true_o_false,
    "limite_repuestos": "descripción límite de repuestos o null",
    "cobertura_descripcion": "qué cubre el contrato exactamente",
    "exclusiones": ["exclusión1", "exclusión2"],
    "penalidades": "descripción de penalidades por incumplimiento o null",
    "nivel_riesgo": "alto|medio|bajo",
    "score": número_0_a_100,
    "resumen": "2-3 oraciones resumiendo el contrato para el prestador",
    "clausulas_criticas": ["clausula crítica 1", "clausula crítica 2"],
    "alertas": ["alerta operativa 1", "alerta operativa 2"],
    "mejoras_prioritarias": ["mejora sugerida 1", "mejora sugerida 2"]
  },
  "equipos": [
    {
      "nombre": "nombre del equipo fitness",
      "marca": "marca o null",
      "modelo": "modelo o null",
      "sku": "código o null",
      "cantidad": número_entero,
      "ubicacion": "sala/piso/zona o null",
      "notas": "observaciones o null"
    }
  ],
  "instalaciones": [
    {
      "nombre": "nombre de la instalación/sede o null",
      "direccion": "dirección o null",
      "comuna": "comuna o null"
    }
  ]
}

Criterios de evaluación:
- score 80-100: contrato muy favorable para el prestador (buenas tarifas, SLA razonable, cobertura clara)
- score 50-79: contrato aceptable con algunas condiciones a revisar
- score 20-49: contrato desfavorable (tarifas bajas, SLA exigente, sin límite de repuestos)
- score 0-19: contrato de alto riesgo financiero u operativo

Si el documento es una fotografía del contrato, extrae la información visible con la misma rigurosidad.
Si un dato no aparece en el documento, usa null. No inventes información."""

    prompt_usuario = f"""Analiza este contrato de mantención de equipos fitness y extrae TODA la información:

{texto[:8000] if not is_image else '[Contrato en imagen adjunta — analiza todo el texto visible]'}

Incluye: datos del cliente, condiciones contractuales, costos, equipos mencionados, cláusulas críticas, riesgos y alertas operativas."""

    try:
        import anthropic as _anthropic
        cliente_ia = _anthropic.Anthropic(api_key=ai_key)

        if is_image:
            # Análisis por visión (foto del contrato desde celular)
            msg = cliente_ia.messages.create(
                model="claude-opus-4-5",
                max_tokens=2500,
                system=prompt_agente,
                messages=[{"role": "user", "content": [
                    {"type": "image", "source": {
                        "type": "base64",
                        "media_type": img_media_type,
                        "data": img_b64
                    }},
                    {"type": "text", "text": prompt_usuario}
                ]}]
            )
        else:
            msg = cliente_ia.messages.create(
                model="claude-opus-4-5",
                max_tokens=2500,
                system=prompt_agente,
                messages=[{"role": "user", "content": prompt_usuario}]
            )

        raw = msg.content[0].text.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"): raw = raw[4:]
        resultado = json.loads(raw)
    except Exception as e:
        return jsonify({"error": f"Error en análisis IA: {e}"}), 500

    # Cruzar RUT detectado con ERP/local para enriquecer
    rut_detectado = (resultado.get("cliente") or {}).get("rut","")
    erp_data = {}
    if rut_detectado:
        local = mysql_fetchone(
            "SELECT * FROM mant_clientes WHERE rut LIKE %s LIMIT 1",
            (f"%{rut_detectado.split('-')[0]}%",)
        )
        if local:
            erp_data = {"origen":"local","id":local["id"],
                        "razon_social":local["razon_social"],
                        "rut":local["rut"],"estado":local["estado"]}
        else:
            rows = _erp_buscar_clientes(rut_detectado.split("-")[0], limit=1)
            if rows:
                erp_data = {"origen":"erp","id":None,
                            "razon_social":rows[0]["razon_social"],
                            "rut":rows[0]["rut"]}

    resultado["_erp_match"] = erp_data
    return jsonify({"ok": True, "resultado": resultado})


@app.route("/mantenciones/api/clientes/<int:cid>/generar-calendario", methods=["POST"])
@_mant_required
def mant_generar_calendario(cid):
    """
    Genera visitas de mantención automáticas basadas en el contrato activo del cliente.
    Devuelve preview (dry_run=true) o guarda en DB.
    """
    d        = request.get_json(silent=True) or {}
    dry_run  = d.get("dry_run", True)
    desde_str= d.get("desde")     # YYYY-MM-DD
    meses    = int(d.get("meses", 12))
    tipo     = d.get("tipo", "preventiva")
    tecnico  = d.get("tecnico", "")

    # Obtener frecuencia del contrato activo
    ct = mysql_fetchone(
        """SELECT * FROM mant_contratos
           WHERE cliente_id=%s AND estado IN ('vigente','indefinido')
           ORDER BY created_at DESC LIMIT 1""",
        (cid,)
    )
    cliente = mysql_fetchone("SELECT razon_social FROM mant_clientes WHERE id=%s", (cid,))
    if not ct:
        return jsonify({"error": "Sin contrato activo para este cliente"}), 404

    frecuencia = ct.get("ai_frecuencia_sug") or ct.get("frecuencia_meses") or 3
    desde = datetime.strptime(desde_str, "%Y-%m-%d").date() if desde_str else datetime.now().date()

    visitas_preview = []
    fecha_actual = desde
    while fecha_actual <= desde + timedelta(days=meses * 30):
        visitas_preview.append({
            "fecha":    str(fecha_actual),
            "titulo":   f"Mantención {tipo.capitalize()} — {cliente['razon_social'] if cliente else ''}",
            "tipo":     tipo,
            "tecnico":  tecnico,
            "contrato_id": ct["id"],
        })
        # Siguiente visita
        mes = fecha_actual.month + frecuencia
        anio = fecha_actual.year
        while mes > 12:
            mes -= 12
            anio += 1
        try:
            fecha_actual = fecha_actual.replace(year=anio, month=mes)
        except ValueError:
            fecha_actual = fecha_actual.replace(year=anio, month=mes, day=28)

    if dry_run:
        return jsonify({"ok": True, "preview": visitas_preview, "frecuencia": frecuencia})

    # Guardar visitas
    conn = get_mysql()
    try:
        with conn.cursor() as cur:
            for v in visitas_preview:
                cur.execute(
                    """INSERT INTO mant_visitas
                       (cliente_id,contrato_id,titulo,fecha_programada,tipo,estado,created_by)
                       VALUES (%s,%s,%s,%s,%s,'programada',%s)""",
                    (cid, v["contrato_id"], v["titulo"], v["fecha"], v["tipo"], current_username())
                )
        conn.commit()
        _mant_log("cliente", cid, "calendario_generado", f"{len(visitas_preview)} visitas")
        return jsonify({"ok": True, "creadas": len(visitas_preview)})
    finally:
        conn.close()


@app.route("/mantenciones/api/contratos/<int:ctid>/ai-editar", methods=["PUT"])
@_mant_required
def mant_contrato_ai_editar(ctid):
    """Guarda los campos del análisis IA editados manualmente por el usuario."""
    d = request.get_json(silent=True) or {}
    editable_fields = [
        "nombre", "fecha_inicio", "fecha_vencimiento", "es_indefinido",
        "monto_mensual", "monto_anual", "frecuencia_meses", "notas", "estado",
        "sla_horas", "incluye_repuestos", "incluye_mant_gratis",
        "costo_por_mant", "costo_total", "nivel_riesgo",
        "ai_tipo_contrato", "ai_cobertura", "ai_vigencia_inicio", "ai_vigencia_fin",
    ]
    sets = [f"{f}=%s" for f in editable_fields if f in d]
    vals = [d[f] for f in editable_fields if f in d]
    # Guardar snapshot editable como JSON también
    sets.append("ai_editable=%s")
    vals.append(json.dumps(d, ensure_ascii=False))
    if not sets:
        return jsonify({"error": "Sin campos"}), 400
    conn = get_mysql()
    try:
        with conn.cursor() as cur:
            cur.execute(f"UPDATE mant_contratos SET {','.join(sets)} WHERE id=%s", vals + [ctid])
        conn.commit()
        _mant_log("contrato", ctid, "ai_editado_manual")
        return jsonify({"ok": True})
    finally:
        conn.close()


@app.route("/mantenciones/api/contratos/<int:ctid>/adjuntos", methods=["POST"])
@_mant_required
def mant_adjunto_subir(ctid):
    """Sube un archivo adjunto adicional al contrato (hasta 4)."""
    f = request.files.get("archivo")
    if not f or not f.filename:
        return jsonify({"error": "Sin archivo"}), 400
    # Verificar límite
    existing = mysql_fetchall("SELECT id FROM mant_adjuntos WHERE contrato_id=%s", (ctid,))
    if len(existing) >= 4:
        return jsonify({"error": "Máximo 4 adjuntos por contrato"}), 400
    ext  = f.filename.rsplit(".", 1)[-1].lower() if "." in f.filename else "bin"
    fname = secure_filename(f"adj_{ctid}_{int(time.time())}_{f.filename}")
    fpath = os.path.join(MANT_UPLOADS, fname)
    f.save(fpath)
    conn = get_mysql()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO mant_adjuntos (contrato_id,nombre_original,archivo_path,tipo,created_by) VALUES (%s,%s,%s,%s,%s)",
                (ctid, f.filename, fname, ext, current_username())
            )
            aid = cur.lastrowid
        conn.commit()
        return jsonify({"ok": True, "id": aid, "nombre": f.filename})
    finally:
        conn.close()


@app.route("/mantenciones/api/adjuntos/<int:aid>")
@_mant_required
def mant_adjunto_ver(aid):
    """Descarga un adjunto — solo superadmin puede descargar."""
    from flask import send_from_directory
    if not g.permissions.get("superadmin"):
        return "Acceso restringido — solo Superadmin puede descargar archivos de contratos.", 403
    adj = mysql_fetchone("SELECT * FROM mant_adjuntos WHERE id=%s", (aid,))
    if not adj:
        return "No encontrado", 404
    return send_from_directory(MANT_UPLOADS, adj["archivo_path"],
                               as_attachment=False,
                               download_name=adj["nombre_original"])


@app.route("/mantenciones/clientes/nuevo", methods=["GET", "POST"])
@_mant_required
def mant_cliente_nuevo():
    if request.method == "POST":
        d = request.form
        conn = get_mysql()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO mant_clientes
                       (razon_social,rut,contacto_nombre,contacto_tel,contacto_email,
                        direccion,comuna,ciudad,notas,estado,created_by)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (d.get("razon_social","").strip(), d.get("rut","").strip(),
                     d.get("contacto_nombre","").strip(), d.get("contacto_tel","").strip(),
                     d.get("contacto_email","").strip(), d.get("direccion","").strip(),
                     d.get("comuna","").strip(), d.get("ciudad","").strip(),
                     d.get("notas","").strip(), d.get("estado","activo"),
                     current_username())
                )
                cid = cur.lastrowid
            conn.commit()
            _mant_log("cliente", cid, "creado", d.get("razon_social",""))
            # Si viene del wizard (header especial), devolver JSON
            if request.headers.get("X-Wizard") == "1":
                return jsonify({"ok": True, "id": cid})
            return redirect(url_for("mant_ficha", cid=cid))
        finally:
            conn.close()
    return render_template("mantenciones/cliente_form.html", cliente=None)


@app.route("/mantenciones/api/ultimo-cliente")
@_mant_required
def mant_ultimo_cliente():
    """Devuelve el último cliente creado por el usuario actual (para el wizard)."""
    row = mysql_fetchone(
        "SELECT id, razon_social FROM mant_clientes WHERE created_by=%s ORDER BY created_at DESC LIMIT 1",
        (current_username(),)
    )
    if not row:
        return jsonify({"error": "No encontrado"}), 404
    return jsonify({"id": row["id"], "razon_social": row["razon_social"]})


@app.route("/mantenciones/clientes/<int:cid>")
@_mant_required
def mant_ficha(cid):
    cliente   = mysql_fetchone("SELECT * FROM mant_clientes WHERE id=%s", (cid,))
    if not cliente:
        return redirect(url_for("mant_clientes"))
    maquinas  = mysql_fetchall("SELECT * FROM mant_maquinas WHERE cliente_id=%s ORDER BY created_at DESC", (cid,))
    contratos = mysql_fetchall("SELECT * FROM mant_contratos WHERE cliente_id=%s ORDER BY created_at DESC", (cid,))
    visitas   = mysql_fetchall(
        "SELECT * FROM mant_visitas WHERE cliente_id=%s ORDER BY fecha_programada DESC LIMIT 50", (cid,)
    )
    logs      = mysql_fetchall(
        "SELECT * FROM mant_logs WHERE entidad='cliente' AND entidad_id=%s ORDER BY created_at DESC LIMIT 20", (cid,)
    )
    return render_template("mantenciones/ficha.html",
        cliente   = dict(cliente),
        maquinas  = [dict(r) for r in maquinas],
        contratos = [dict(r) for r in contratos],
        visitas   = [dict(r) for r in visitas],
        logs      = [dict(r) for r in logs],
        hoy       = datetime.now().date(),
    )


@app.route("/mantenciones/api/clientes/<int:cid>", methods=["PUT"])
@_mant_required
def mant_cliente_update(cid):
    d = request.get_json(silent=True) or {}
    fields = ["razon_social","rut","contacto_nombre","contacto_tel","contacto_email",
              "direccion","comuna","ciudad","notas","estado"]
    sets   = [f"{f}=%s" for f in fields if f in d]
    vals   = [d[f] for f in fields if f in d]
    if not sets:
        return jsonify({"error": "Sin campos"}), 400
    conn = get_mysql()
    try:
        with conn.cursor() as cur:
            cur.execute(f"UPDATE mant_clientes SET {','.join(sets)} WHERE id=%s",
                        vals + [cid])
        conn.commit()
        _mant_log("cliente", cid, "editado")
        return jsonify({"ok": True})
    finally:
        conn.close()


# ── MÁQUINAS ──────────────────────────────────────────────────────────

@app.route("/mantenciones/api/clientes/<int:cid>/maquinas", methods=["POST"])
@_mant_required
def mant_maquina_add(cid):
    d = request.get_json(silent=True) or {}
    conn = get_mysql()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO mant_maquinas
                   (cliente_id,sku,nombre,serie,doc_origen,doc_fecha,cantidad,notas,
                    ubicacion_cliente,estado_op,fecha_instalacion,created_by)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (cid, d.get("sku",""), d.get("nombre",""), d.get("serie",""),
                 d.get("doc_origen",""), d.get("doc_fecha") or None,
                 int(d.get("cantidad",1)), d.get("notas",""),
                 d.get("ubicacion_cliente",""),
                 d.get("estado_op","operativo"),
                 d.get("fecha_instalacion") or None,
                 current_username())
            )
            mid = cur.lastrowid
        conn.commit()
        _mant_log("maquina", mid, "agregada", d.get("nombre",""))
        return jsonify({"ok": True, "id": mid})
    finally:
        conn.close()


@app.route("/mantenciones/api/maquinas/<int:mid>", methods=["DELETE"])
@_mant_required
def mant_maquina_del(mid):
    conn = get_mysql()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM mant_maquinas WHERE id=%s", (mid,))
        conn.commit()
        return jsonify({"ok": True})
    finally:
        conn.close()


# ── CONTRATOS ─────────────────────────────────────────────────────────

MANT_UPLOADS = os.path.join(BASE_DIR, "static", "uploads", "mantenciones")
os.makedirs(MANT_UPLOADS, exist_ok=True)

ALLOWED_CONTRATO = {"pdf", "doc", "docx"}


@app.route("/mantenciones/api/clientes/<int:cid>/contratos", methods=["POST"])
@_mant_required
def mant_contrato_subir(cid):
    f = request.files.get("archivo")
    d = request.form
    if not f or not f.filename:
        return jsonify({"error": "Sin archivo"}), 400
    ext = f.filename.rsplit(".", 1)[-1].lower()
    if ext not in ALLOWED_CONTRATO:
        return jsonify({"error": "Tipo no permitido"}), 400

    fname  = secure_filename(f"{cid}_{int(time.time())}_{f.filename}")
    fpath  = os.path.join(MANT_UPLOADS, fname)
    f.save(fpath)

    tipo = "pdf" if ext == "pdf" else ("word" if ext in ("doc","docx") else "otro")
    conn = get_mysql()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO mant_contratos
                   (cliente_id,nombre,archivo_nombre,archivo_path,archivo_tipo,
                    fecha_inicio,fecha_vencimiento,es_indefinido,
                    monto_mensual,monto_anual,frecuencia_meses,notas,created_by)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (cid, d.get("nombre","Contrato"), f.filename, fname, tipo,
                 d.get("fecha_inicio") or None, d.get("fecha_vencimiento") or None,
                 1 if d.get("es_indefinido") else 0,
                 float(d.get("monto_mensual",0) or 0),
                 float(d.get("monto_anual",0) or 0),
                 int(d.get("frecuencia_meses",0) or 0),
                 d.get("notas",""), current_username())
            )
            ctid = cur.lastrowid
        conn.commit()
        _mant_log("contrato", ctid, "subido", f.filename)
        return jsonify({"ok": True, "id": ctid})
    finally:
        conn.close()


@app.route("/mantenciones/api/contratos/<int:ctid>", methods=["PUT"])
@_mant_required
def mant_contrato_update(ctid):
    d = request.get_json(silent=True) or {}
    allowed = ["nombre","fecha_inicio","fecha_vencimiento","es_indefinido",
               "monto_mensual","monto_anual","frecuencia_meses","notas","estado"]
    sets = [f"{f}=%s" for f in allowed if f in d]
    vals = [d[f] for f in allowed if f in d]
    if not sets:
        return jsonify({"error": "Sin campos"}), 400
    conn = get_mysql()
    try:
        with conn.cursor() as cur:
            cur.execute(f"UPDATE mant_contratos SET {','.join(sets)} WHERE id=%s",
                        vals + [ctid])
        conn.commit()
        return jsonify({"ok": True})
    finally:
        conn.close()


@app.route("/mantenciones/api/contratos/<int:ctid>/archivo")
@_mant_required
def mant_contrato_archivo(ctid):
    from flask import send_from_directory
    # Solo superadmin puede descargar archivos de contratos (datos confidenciales)
    if not g.permissions.get("superadmin"):
        return ("Acceso restringido — solo el Superadministrador puede "
                "descargar archivos de contratos."), 403
    ct = mysql_fetchone("SELECT * FROM mant_contratos WHERE id=%s", (ctid,))
    if not ct:
        return "No encontrado", 404
    return send_from_directory(MANT_UPLOADS, ct["archivo_path"],
                               as_attachment=False,
                               download_name=ct["archivo_nombre"])


# ── ANÁLISIS IA DE CONTRATO ───────────────────────────────────────────

@app.route("/mantenciones/api/contratos/<int:ctid>/analizar", methods=["POST"])
@_mant_required
def mant_contrato_analizar(ctid):
    """
    Llama a Claude API para analizar el contrato de mantención.
    Extrae: puntos críticos, frecuencia sugerida, alertas, score.
    """
    ct = mysql_fetchone("SELECT ct.*, cl.razon_social FROM mant_contratos ct "
                        "JOIN mant_clientes cl ON cl.id=ct.cliente_id "
                        "WHERE ct.id=%s", (ctid,))
    if not ct:
        return jsonify({"error": "Contrato no encontrado"}), 404

    # Leer texto del contrato si es PDF
    texto_contrato = ""
    fpath = os.path.join(MANT_UPLOADS, ct["archivo_path"] or "")
    if os.path.exists(fpath):
        ext = ct["archivo_path"].rsplit(".", 1)[-1].lower()
        if ext == "pdf":
            try:
                import pdfplumber
                with pdfplumber.open(fpath) as pdf:
                    texto_contrato = "\n".join(
                        p.extract_text() or "" for p in pdf.pages[:15]
                    )
            except Exception:
                pass
        elif ext in ("doc", "docx"):
            try:
                import docx
                doc = docx.Document(fpath)
                texto_contrato = "\n".join(p.text for p in doc.paragraphs)
            except Exception:
                pass

    # Datos del contrato para enriquecer el prompt
    datos_extra = (
        f"Cliente: {ct['razon_social']}\n"
        f"Fecha inicio: {ct['fecha_inicio']}\n"
        f"Fecha vencimiento: {ct['fecha_vencimiento']}\n"
        f"Monto mensual: ${ct['monto_mensual']}\n"
        f"Frecuencia declarada: {ct['frecuencia_meses']} meses\n"
    )
    if not texto_contrato:
        texto_contrato = "(Texto no extraíble — análisis basado en metadatos)"

    prompt_sistema = """Eres un experto jurídico y técnico en contratos de mantención
de equipos de fitness para gimnasios y centros deportivos en Chile (ILUS Fitness).
Analiza el contrato con criterio profesional. Responde SIEMPRE en JSON con esta estructura EXACTA:
{
  "tipo_contrato": "Preventivo|Correctivo|Full|Garantía|Inspección|Otro",
  "resumen": "2-3 oraciones resumiendo el contrato",
  "score": 0-100,
  "nivel_riesgo": "alto|medio|bajo",
  "vigencia_inicio": "YYYY-MM-DD o null",
  "vigencia_fin": "YYYY-MM-DD o null",
  "es_indefinido": true_o_false,
  "frecuencia_sugerida_meses": número_entero,
  "sla_horas": número_entero_o_null,
  "incluye_mant_gratis": true_o_false,
  "incluye_repuestos": true_o_false,
  "cobertura_descripcion": "descripción de qué cubre el contrato",
  "costo_mensual": número_o_null,
  "costo_por_mant": número_o_null,
  "costo_total": número_o_null,
  "clausulas_criticas": ["clausula1","clausula2",...],
  "puntos_criticos": ["punto1","punto2",...],
  "alertas": ["alerta1","alerta2",...],
  "mejoras_prioritarias": ["mejora1","mejora2","mejora3"]
}
Sé específico sobre equipos fitness (treadmills, bikes, elípticas, pesas, etc.).
Detecta SLA, penalidades, cláusulas de exclusión y riesgos operativos para el prestador."""

    prompt_usuario = f"""Analiza este contrato de mantención:

METADATOS:
{datos_extra}

TEXTO DEL CONTRATO:
{texto_contrato[:6000]}

Devuelve SOLO el JSON, sin texto adicional."""

    ai_key = _get_ai_key()
    if not ai_key:
        return jsonify({"error": "⚠️ API de IA no configurada. Agrega ANTHROPIC_API_KEY en Railway."}), 503
    try:
        import anthropic as _anthropic
        client = _anthropic.Anthropic(api_key=ai_key)
        msg = client.messages.create(
            model   = "claude-opus-4-5",
            max_tokens = 1500,
            system  = prompt_sistema,
            messages = [{"role": "user", "content": prompt_usuario}]
        )
        raw = msg.content[0].text.strip()
        # Limpiar posibles bloques de código
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        resultado = json.loads(raw)
    except Exception as e:
        return jsonify({"error": f"Error IA: {str(e)}"}), 500

    # Guardar resultado en DB (estructura expandida)
    conn = get_mysql()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE mant_contratos SET
                   ai_analizado=1, ai_fecha=%s,
                   ai_resumen=%s,
                   ai_puntos_criticos=%s, ai_alertas=%s, ai_mejoras=%s,
                   ai_clausulas=%s, ai_cobertura=%s, ai_tipo_contrato=%s,
                   ai_frecuencia_sug=%s, ai_score=%s,
                   ai_vigencia_inicio=%s, ai_vigencia_fin=%s,
                   nivel_riesgo=%s,
                   sla_horas=%s,
                   incluye_mant_gratis=%s, incluye_repuestos=%s,
                   costo_por_mant=%s, costo_total=%s,
                   frecuencia_meses=COALESCE(NULLIF(frecuencia_meses,0),%s),
                   es_indefinido=COALESCE(NULLIF(es_indefinido,0),%s),
                   monto_mensual=COALESCE(NULLIF(monto_mensual,0),%s)
                   WHERE id=%s""",
                (datetime.now(),
                 resultado.get("resumen",""),
                 json.dumps(resultado.get("puntos_criticos",[]),    ensure_ascii=False),
                 json.dumps(resultado.get("alertas",[]),            ensure_ascii=False),
                 json.dumps(resultado.get("mejoras_prioritarias",[]),ensure_ascii=False),
                 json.dumps(resultado.get("clausulas_criticas",[]), ensure_ascii=False),
                 resultado.get("cobertura_descripcion",""),
                 resultado.get("tipo_contrato",""),
                 resultado.get("frecuencia_sugerida_meses"),
                 resultado.get("score"),
                 resultado.get("vigencia_inicio") or None,
                 resultado.get("vigencia_fin") or None,
                 resultado.get("nivel_riesgo","medio"),
                 resultado.get("sla_horas") or None,
                 1 if resultado.get("incluye_mant_gratis") else 0,
                 1 if resultado.get("incluye_repuestos") else 0,
                 resultado.get("costo_por_mant") or None,
                 resultado.get("costo_total") or None,
                 resultado.get("frecuencia_sugerida_meses"),
                 1 if resultado.get("es_indefinido") else 0,
                 resultado.get("costo_mensual") or None,
                 ctid)
            )
        conn.commit()
        _mant_log("contrato", ctid, "analizado_ia", f"score={resultado.get('score')} riesgo={resultado.get('nivel_riesgo')}")
        return jsonify({"ok": True, "resultado": resultado})
    finally:
        conn.close()


# ── VISITAS / AGENDA ──────────────────────────────────────────────────

@app.route("/mantenciones/api/visitas", methods=["GET"])
@_mant_required
def mant_visitas_api():
    """Devuelve visitas para el calendario (formato FullCalendar)."""
    desde = request.args.get("start", "")
    hasta = request.args.get("end", "")
    cid   = request.args.get("cliente_id")
    where, params = [], []
    if desde: where.append("v.fecha_programada >= %s"); params.append(desde[:10])
    if hasta: where.append("v.fecha_programada <= %s"); params.append(hasta[:10])
    if cid:   where.append("v.cliente_id=%s"); params.append(int(cid))
    sql = ("SELECT v.*, c.razon_social FROM mant_visitas v "
           "JOIN mant_clientes c ON c.id=v.cliente_id")
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY v.fecha_programada LIMIT 500"
    rows = mysql_fetchall(sql, tuple(params))

    COLORS = {"preventiva":"#1a7a1a","correctiva":"#cc0000",
              "garantia":"#0066cc","inspeccion":"#f57c00"}
    ESTADO_CLR = {"completada":"#6c757d","cancelada":"#999","reagendada":"#ff9800"}
    events = []
    for r in rows:
        color = ESTADO_CLR.get(r["estado"], COLORS.get(r["tipo"],"#555"))
        events.append({
            "id":    r["id"],
            "title": f"{r['razon_social']} — {r['titulo'] or r['tipo'].capitalize()}",
            "start": str(r["fecha_programada"]),
            "color": color,
            "extendedProps": {
                "cliente_id":   r["cliente_id"],
                "razon_social": r["razon_social"],
                "tipo":         r["tipo"],
                "estado":       r["estado"],
                "tecnico":      r["tecnico"] or "",
                "costo":        float(r["costo"] or 0),
            }
        })
    return jsonify(events)


@app.route("/mantenciones/api/visitas", methods=["POST"])
@_mant_required
def mant_visita_crear():
    d = request.get_json(silent=True) or {}
    if not d.get("cliente_id") or not d.get("fecha_programada"):
        return jsonify({"error": "cliente_id y fecha_programada requeridos"}), 400
    conn = get_mysql()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO mant_visitas
                   (cliente_id,contrato_id,titulo,fecha_programada,hora_inicio,hora_fin,
                    tecnico,tipo,estado,descripcion,costo,created_by)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (d["cliente_id"], d.get("contrato_id") or None,
                 d.get("titulo","Mantención"), d["fecha_programada"],
                 d.get("hora_inicio") or None, d.get("hora_fin") or None,
                 d.get("tecnico",""), d.get("tipo","preventiva"),
                 d.get("estado","programada"), d.get("descripcion",""),
                 float(d.get("costo",0) or 0), current_username())
            )
            vid = cur.lastrowid
        conn.commit()
        _mant_log("visita", vid, "creada", d.get("titulo",""))
        return jsonify({"ok": True, "id": vid})
    finally:
        conn.close()


@app.route("/mantenciones/api/visitas/<int:vid>", methods=["PUT"])
@_mant_required
def mant_visita_update(vid):
    d = request.get_json(silent=True) or {}
    allowed = ["titulo","fecha_programada","fecha_realizada","hora_inicio","hora_fin",
               "tecnico","tipo","estado","descripcion","observaciones","costo","contrato_id"]
    sets = [f"{f}=%s" for f in allowed if f in d]
    vals = [d[f] for f in allowed if f in d]
    if not sets:
        return jsonify({"error": "Sin campos"}), 400
    conn = get_mysql()
    try:
        with conn.cursor() as cur:
            cur.execute(f"UPDATE mant_visitas SET {','.join(sets)} WHERE id=%s",
                        vals + [vid])
        conn.commit()
        _mant_log("visita", vid, "actualizada")
        return jsonify({"ok": True})
    finally:
        conn.close()


@app.route("/mantenciones/api/visitas/<int:vid>", methods=["DELETE"])
@_mant_required
def mant_visita_del(vid):
    conn = get_mysql()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM mant_visitas WHERE id=%s", (vid,))
        conn.commit()
        return jsonify({"ok": True})
    finally:
        conn.close()


# ── CALENDARIO ────────────────────────────────────────────────────────

@app.route("/mantenciones/calendario")
@_mant_required
def mant_calendario():
    clientes = mysql_fetchall(
        "SELECT id, razon_social FROM mant_clientes WHERE estado='activo' ORDER BY razon_social",
        ()
    )
    return render_template("mantenciones/calendario.html",
        clientes = [dict(r) for r in clientes]
    )


# ── ANÁLISIS ECONÓMICO ────────────────────────────────────────────────

@app.route("/mantenciones/analisis")
@_mant_required
def mant_analisis():
    # Ingresos por contrato (12 meses)
    ingresos_mes = mysql_fetchall(
        """SELECT YEAR(fecha_inicio) AS anio, MONTH(fecha_inicio) AS mes,
                  COALESCE(SUM(monto_mensual),0) AS total_mensual,
                  COUNT(*) AS n_contratos
           FROM mant_contratos
           WHERE estado IN ('vigente','indefinido')
             AND fecha_inicio >= DATE_SUB(NOW(), INTERVAL 12 MONTH)
           GROUP BY anio, mes ORDER BY anio, mes""",
        ()
    )
    # Proyección próximos 6 meses
    activos_total = mysql_fetchone(
        "SELECT COALESCE(SUM(monto_mensual),0) AS mrr FROM mant_contratos "
        "WHERE estado IN ('vigente','indefinido')", ()
    ) or {}
    mrr = float(activos_total.get("mrr", 0))

    # Visitas por mes (últimos 6)
    visitas_mes = mysql_fetchall(
        """SELECT YEAR(fecha_programada) AS anio, MONTH(fecha_programada) AS mes,
                  COUNT(*) AS total,
                  SUM(estado='completada') AS completadas,
                  COALESCE(SUM(costo),0) AS ingresos_visitas
           FROM mant_visitas
           WHERE fecha_programada >= DATE_SUB(NOW(), INTERVAL 6 MONTH)
           GROUP BY anio, mes ORDER BY anio, mes""",
        ()
    )
    # Top clientes por valor
    top_clientes = mysql_fetchall(
        """SELECT c.razon_social,
                  COALESCE(SUM(ct.monto_mensual),0) AS mrr,
                  COUNT(ct.id) AS n_contratos,
                  COUNT(v.id) AS n_visitas
           FROM mant_clientes c
           LEFT JOIN mant_contratos ct ON ct.cliente_id=c.id
                     AND ct.estado IN ('vigente','indefinido')
           LEFT JOIN mant_visitas v ON v.cliente_id=c.id
           WHERE c.estado='activo'
           GROUP BY c.id ORDER BY mrr DESC LIMIT 10""",
        ()
    )
    # Contratos por vencer
    por_vencer = mysql_fetchall(
        """SELECT ct.*, cl.razon_social,
                  DATEDIFF(ct.fecha_vencimiento, CURDATE()) AS dias_restantes
           FROM mant_contratos ct
           JOIN mant_clientes cl ON cl.id=ct.cliente_id
           WHERE ct.estado IN ('por_vencer','vigente')
             AND ct.fecha_vencimiento IS NOT NULL
             AND ct.es_indefinido=0
           ORDER BY ct.fecha_vencimiento LIMIT 15""",
        ()
    )

    return render_template("mantenciones/analisis.html",
        ingresos_mes = [dict(r) for r in ingresos_mes],
        mrr          = mrr,
        visitas_mes  = [dict(r) for r in visitas_mes],
        top_clientes = [dict(r) for r in top_clientes],
        por_vencer   = [dict(r) for r in por_vencer],
    )


@app.route("/api/uf-actual")
def api_uf_actual():
    """Devuelve el valor actual de la UF desde mindicador.cl"""
    try:
        import urllib.request as _ur
        req = _ur.Request(
            "https://mindicador.cl/api/uf",
            headers={"User-Agent": "ILUSApp/1.0"}
        )
        with _ur.urlopen(req, timeout=6) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        uf_val = float(data["serie"][0]["valor"])
        fecha  = data["serie"][0]["fecha"][:10]
        return jsonify({"uf": uf_val, "fecha": fecha, "ok": True})
    except Exception as e:
        return jsonify({"uf": None, "error": str(e), "ok": False})


# ── BÚSQUEDA PRODUCTOS ERP ───────────────────────────────────────────

@app.route("/mantenciones/api/productos/buscar")
@_mant_required
def mant_productos_buscar():
    """
    Typeahead de productos ERP por SKU o descripción.
    Usa la REST API (funciona desde Railway).
    Devuelve [{sku, nombre}].
    """
    q = request.args.get("q", "").strip()
    if len(q) < 2:
        return jsonify([])
    TOKEN = ERP_CONFIG.get("api_token", "")
    resultados = []
    seen = set()
    try:
        body = _erp_get(
            "/productos",
            {"search": q, "empresa": "01", "fields": "KOPR,NOKOPR,NOKOTI", "visible": "true"},
            TOKEN, timeout=8,
        )
        for p in (body.get("data") or []):
            sku    = (p.get("KOPR")   or "").strip().upper()
            nombre = (p.get("NOKOPR") or "").strip()
            tipo   = (p.get("NOKOTI") or p.get("TIPO") or "").strip()
            if not sku or sku in seen:
                continue
            resultados.append({"sku": sku, "nombre": nombre, "tipo": tipo})
            seen.add(sku)
            if len(resultados) >= 25:
                break
    except Exception:
        pass  # Si REST falla devolvemos lista vacía (mejor que 500)
    return jsonify(resultados)


# ── BÚSQUEDA ERP ─────────────────────────────────────────────────────

@app.route("/mantenciones/api/buscar-erp", methods=["POST"])
@_mant_required
def mant_buscar_erp():
    """Busca cliente/documentos en el ERP por RUT o razón social."""
    d   = request.get_json(silent=True) or {}
    q   = d.get("q", "").strip()
    if not q:
        return jsonify({"error": "Término de búsqueda requerido"}), 400
    ERP_SALES = ERP_CONFIG.get("table_sales", "HEBDOC")
    try:
        erp_conn = get_erp_conn()
        if not erp_conn:
            return jsonify({"error": "Sin conexión al ERP en este momento", "documentos": []}), 503
        with erp_conn.cursor() as cur:
            cur.execute(
                f"""SELECT DISTINCT
                       TRIM(d.NRAZON) AS razon_social,
                       TRIM(d.NRUC)   AS rut,
                       TRIM(d.TIDO)   AS tipo_doc,
                       TRIM(d.NUDO)   AS num_doc,
                       d.FEMIS        AS fecha,
                       TRIM(d.NOMBR)  AS producto,
                       TRIM(d.KOPRCT) AS sku,
                       d.CANTD        AS cantidad
                    FROM `{ERP_SALES}` d
                    WHERE (TRIM(d.NRAZON) LIKE %s OR TRIM(d.NRUC) LIKE %s)
                      AND d.TIDO IN ('FCV','BLV','NVV','VD','WEB','FCO')
                    ORDER BY d.FEMIS DESC
                    LIMIT 100""",
                (f"%{q}%", f"%{q}%")
            )
            rows = cur.fetchall()
        erp_conn.close()
        # Agrupar por documento
        docs = {}
        for r in rows:
            key = f"{r['tipo_doc']} {r['num_doc']}"
            if key not in docs:
                docs[key] = {
                    "razon_social": r["razon_social"],
                    "rut":          r["rut"],
                    "tipo_doc":     r["tipo_doc"],
                    "num_doc":      r["num_doc"],
                    "fecha":        str(r["fecha"]) if r["fecha"] else "",
                    "lineas":       []
                }
            docs[key]["lineas"].append({
                "sku":      r["sku"],
                "nombre":   r["producto"],
                "cantidad": int(r["cantidad"] or 1),
            })
        return jsonify({"ok": True, "documentos": list(docs.values())})
    except Exception as e:
        return jsonify({"error": str(e), "documentos": []}), 500


# ─────────────────────────────────────────────
#  Arranque — inicializar tablas al cargar módulo
#  (funciona con `python app.py` Y `flask run`)
# ─────────────────────────────────────────────

try:
    with app.app_context():
        init_db()
    print("[ILUS] Tablas inicializadas correctamente.")
except Exception as _init_err:
    print(f"[ILUS][WARN] init_db: {_init_err}")

# init_transporte_tables usa get_mysql() (sin app context), siempre funciona
try:
    init_transporte_tables()
    print("[ILUS] Tablas de transporte OK.")
except Exception as _tr_init_err:
    print(f"[ILUS][WARN] init_transporte_tables: {_tr_init_err}")

try:
    init_comunicaciones_tables()
    print("[ILUS] Tablas de comunicaciones OK.")
except Exception as _comm_init_err:
    print(f"[ILUS][WARN] init_comunicaciones_tables: {_comm_init_err}")

try:
    init_mantenciones_tables()
    print("[ILUS] Tablas de mantenciones OK.")
except Exception as _mant_init_err:
    print(f"[ILUS][WARN] init_mantenciones_tables: {_mant_init_err}")

if __name__ == "__main__":
    print("=" * 45)
    print("  ILUS - Sistema de Etiquetas")
    print("  http://localhost:5000")
    print("=" * 45)
    port = int(os.environ.get("PORT", 5000))
    debug = os.environ.get("FLASK_ENV") != "production"
    app.run(debug=debug, host="0.0.0.0", port=port)

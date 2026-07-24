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
import threading
import time
import urllib.request
from functools import wraps
from datetime import datetime, date, timezone

from flask import request, jsonify, render_template, redirect, url_for, g, Response

try:
    from zoneinfo import ZoneInfo
    _CAT_CL_TZ = ZoneInfo("America/Santiago")
except Exception:  # pragma: no cover
    _CAT_CL_TZ = None


# ─────────────────────────────────────────────────────────────────────
#  FOTOS DESDE EL ECOMMERCE (2026-07-14, Daniel: "tráelos automáticos y
#  déjalo vacío si da error"). La tienda Shopify de ILUS
#  (ilusfitness.com/products.json) es pública y de SOLO LECTURA (GET) —
#  NO es el ERP Random, la Regla #4.1 no aplica. Los SKUs de productos
#  marca ILUS/Optimal/Dynamis coinciden exacto con cat_productos.sku;
#  los de marcas revendidas (ej. Gymleco) no calzan y simplemente quedan
#  sin foto (el placeholder .cat-thumb-ph de la UI ya cubre ese caso).
#  Caché en memoria de proceso con TTL 12h — Cloud Run corre pocas
#  réplicas, no hace falta nada más elaborado.
# ─────────────────────────────────────────────────────────────────────
_SHOPIFY_BASE_URL = os.environ.get(
    "CAT_ECOMMERCE_PRODUCTS_URL", "https://ilusfitness.com/products.json").strip()
_SHOPIFY_TTL_S = 12 * 3600
_SHOPIFY_MAX_PAGINAS = 10       # tope de seguridad (el catálogo real son ~2 páginas de 250)
_SHOPIFY_TIMEOUT_PAGINA_S = 10
_SHOPIFY_MAX_IMG_MB = 10
_SHOPIFY_CACHE = {"data": None, "ts": 0.0}
_SHOPIFY_CACHE_LOCK = threading.Lock()


def _shopify_fotos_cache():
    """Dict {SKU_UPPER: url_imagen_principal} construido desde el ecommerce.

    Itera ?limit=250&page=N hasta recibir página vacía (tope 10 páginas).
    Cada producto puede tener varias variantes con SKUs distintos — todas
    apuntan a la MISMA imagen principal del producto. Ante CUALQUIER error
    devuelve el caché viejo si existe (o {}) — NUNCA propaga excepción.
    """
    now = time.time()
    if _SHOPIFY_CACHE["data"] is not None and (now - _SHOPIFY_CACHE["ts"]) < _SHOPIFY_TTL_S:
        return _SHOPIFY_CACHE["data"]
    with _SHOPIFY_CACHE_LOCK:
        # Re-chequear dentro del lock: otro request pudo llenar el caché
        # mientras esperábamos el lock (evita descargar 2 veces la tienda).
        now = time.time()
        if _SHOPIFY_CACHE["data"] is not None and (now - _SHOPIFY_CACHE["ts"]) < _SHOPIFY_TTL_S:
            return _SHOPIFY_CACHE["data"]
        mapping = {}
        try:
            for page in range(1, _SHOPIFY_MAX_PAGINAS + 1):
                url = f"{_SHOPIFY_BASE_URL}?limit=250&page={page}"
                req = urllib.request.Request(url, headers={"User-Agent": "ILUS-Catalogo/1.0"})
                with urllib.request.urlopen(req, timeout=_SHOPIFY_TIMEOUT_PAGINA_S) as resp:
                    data = json.loads(resp.read().decode("utf-8", "replace"))
                products = data.get("products") or []
                if not products:
                    break
                for prod in products:
                    if not isinstance(prod, dict):
                        continue
                    try:
                        img = ((prod.get("image") or {}).get("src")
                               or (((prod.get("images") or [None])[0]) or {}).get("src"))
                    except Exception:
                        img = None
                    if not img:
                        continue
                    for var in (prod.get("variants") or []):
                        if not isinstance(var, dict):
                            continue
                        sku = str(var.get("sku") or "").strip().upper()
                        if sku and sku not in mapping:
                            mapping[sku] = img
        except Exception as _e:
            print(f"[_shopify_fotos_cache] error leyendo la tienda: {_e}", flush=True)
            # Caché viejo si existe; si no, lo parcial que alcanzó a juntar
            # (posiblemente {}). No se cachea lo parcial: el próximo llamado
            # reintenta completo.
            return _SHOPIFY_CACHE["data"] if _SHOPIFY_CACHE["data"] is not None else mapping
        _SHOPIFY_CACHE["data"] = mapping
        _SHOPIFY_CACHE["ts"] = time.time()
        print(f"[_shopify_fotos_cache] {len(mapping)} SKUs indexados desde el ecommerce", flush=True)
        return mapping


def _shopify_descargar_imagen(url, timeout=15):
    """Baja los bytes de una imagen del ecommerce (CDN Shopify). Devuelve
    None si falla, si viene vacía o si excede el tope de tamaño. NUNCA lanza."""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "ILUS-Catalogo/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            tope = _SHOPIFY_MAX_IMG_MB * 1024 * 1024
            data = resp.read(tope + 1)
        if not data or len(data) > tope:
            return None
        return data
    except Exception as _e:
        print(f"[_shopify_descargar_imagen] {url}: {_e}", flush=True)
        return None


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
    _render_comm_template = ctx.get("_render_comm_template")
    ILUS_SOPORTE_EMAIL = ctx.get("ILUS_SOPORTE_EMAIL") or "soportetec@sphs.cl"

    MAX_FOTOS_POR_PRODUCTO = 10
    MAX_PIOLAS_POR_PRODUCTO = 10  # 2026-07-21 (Daniel): vuelve a 10 (lo había bajado a 6 el 2026-07-14)
    MAX_MANUAL_MB = 25  # mismo techo/motivo que MAX_ADJUNTO_MB en tickets_module.py:
                        # Cloud Run limita cada request HTTP a 32MB.
    MAX_MANUALES_POR_PRODUCTO = 5  # 2026-07-12 (Daniel, wizard "Registrar producto"):
                                    # hasta 5 manuales por producto, vía cat_producto_manuales
                                    # (tabla nueva). El manual_pdf_key legado (singular) en
                                    # cat_productos SIGUE funcionando sin cambios — Regla #4.2.
    MAX_FOTO_PIOLA_MB = 6  # 2026-07-13 (Daniel: "las piolas van a requerir fotos"),
                           # mismo techo que el patron de compresion de fotos del proyecto
                           # (foto_editor.js comprime a <300KB en navegador; este es el
                           # tope duro del lado servidor por si llega sin comprimir).

    # ── Categorías de producto (2026-07-21, Daniel: "esto también tiene
    # que ser editable... si sale un producto nuevo lo podemos crear") ──
    # Hasta acá era un ENUM FIJO en el código (10 valores). Ahora vive en
    # la tabla `cat_clases_producto` (editable desde /catalogo/clases,
    # solo superadmin) + `cat_clase_producto_tarifas` (Hora / Cantidad de
    # técnicos por categoría, separado por `tipo_servicio`: instalación vs
    # mantención — Daniel: "hay que separar si es instalación o
    # mantención", son tablas de mano de obra DISTINTAS). HH total =
    # horas×técnicos, no se guarda (se calcula), para que nunca quede
    # desincronizado. Distinta de `familia` (texto libre — Regla #4.2).
    #
    # CORRECCIÓN 2026-07-21 (Daniel adjuntó "Tarifa mantenciones.xlsx"):
    # la tabla que Daniel dictó por voz es de MANTENCIÓN, no instalación
    # (confirmado: son los MISMOS valores Hora/Técnicos que la hoja
    # "Tarifa mantencion" del Excel, verificados exactos contra 301 ítems
    # reales de cotizaciones ya emitidas — 0 discrepancias). Instalación
    # NO tiene tarifa propia todavía en ningún documento de Daniel; queda
    # vacía/editable hasta que la defina. "Rack Pro" existe en el
    # desplegable de Excel de Daniel pero SIN fila de tarifa ahí tampoco
    # (gap real en su propia planilla) -- se crea la categoría para que el
    # dropdown calce, sin inventar horas/técnicos.
    #
    # CAT_CLASES_SEED_MANTENCION es SOLO la semilla de arranque (tabla que
    # Daniel dictó) — una vez en la tabla, la fuente de verdad es la BD;
    # esta lista NO se vuelve a leer si la fila ya existe (no pisa ediciones).
    CAT_CLASES_SEED_MANTENCION = [
        # (slug, nombre, horas, cantidad_tecnicos, orden)
        ("selectorizador_pesos",      "Selectorizador de pesos",                1.0, 2, 10),
        ("selectorizador_pesos_4est", "Selectorizador de pesos (4 estaciones)", 2.0, 2, 20),
        ("bicicleta",                 "Bicicleta",                              1.0, 1, 30),
        ("trotadora",                 "Trotadora",                              1.5, 2, 40),
        ("bancos_plano_ajustable",    "Bancos plano / ajustable",               0.3, 1, 50),
        ("bancos_olimpicos",          "Bancos Olímpicos",                       1.0, 1, 60),
        ("rack_accesorios",           "Rack de accesorios",                     0.5, 1, 70),
        ("rack_basico",               "Rack Básico",                            1.0, 1, 80),
        ("rack_intermedio",           "Rack Intermedio",                        1.5, 1, 90),
        ("rack_avanzados",            "Rack Avanzados",                         1.0, 2, 100),
        ("rack_pro",                  "Rack Pro",                               None, None, 105),
        ("dual_cable_lite",           "Dual Cable Lite",                        1.5, 2, 110),
        ("dual_cable_cross",          "Dual cable Cross",                       1.5, 2, 120),
        ("dual_pulley_drax",          "Dual Pulley Drax",                       1.5, 2, 130),
        ("booty_builder_p",           "Booty Builder P",                        1.0, 1, 140),
        ("otro",                      "Otro",                                   None, None, 999),
    ]
    # Migración de compatibilidad: productos ya clasificados con el ENUM
    # viejo cuyo significado es INEQUÍVOCO se remapean al slug nuevo.
    # "banco"/"rack" viejos quedan A PROPÓSITO sin mapear -- el listado
    # nuevo los abrió en varias categorías distintas y mapear mal
    # corrompería el dato; el producto sigue guardado (nada se borra),
    # solo pide reclasificación manual (mismo flujo `sin_clasificar` que
    # ya existe para SKUs nuevos del ERP).
    CAT_CLASES_MIGRACION_ENUM_VIEJO = {
        "selector_peso": "selectorizador_pesos",
        "rack_avanzado": "rack_avanzados",
    }

    # Bodega de sincronizacion ERP (Regla #4.1: SOLO LECTURA, via
    # _random_sql_query — mismo patron que _buscar_catalogo_bodega en
    # tickets_module.py, con env var propia para no acoplar ambos modulos).
    CAT_BODEGA_SYNC = os.environ.get("CAT_BODEGA_SYNC", "02").strip()

    # 2026-07-22 (Daniel: "una clasificación para cada servicio"): los 5
    # tipos de servicio que pueden tener tarifa por categoría. Debe calzar
    # EXACTO con el ENUM de cat_clase_producto_tarifas.tipo_servicio y con
    # _TK_COTIZ_TIPOS_SERVICIO de tickets_module.py. Orden = orden de la UI.
    _CAT_TIPOS_SERVICIO_TARIFA = (
        "mantencion", "instalacion", "visita_tecnica", "venta_repuesto", "otro")
    _CAT_TIPOS_SERVICIO_LABEL = {
        "mantencion": "Mantención", "instalacion": "Instalación",
        "visita_tecnica": "Visita técnica", "venta_repuesto": "Venta de repuesto",
        "otro": "Otro"}

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
            # 2026-07-21 (Daniel, módulo Cotizaciones): categorías de
            # producto editables (reemplazan el ENUM fijo de arriba) +
            # tabla de mano de obra (Hora/Técnicos) por categoría y tipo
            # de servicio. Ver comentario junto a CAT_CLASES_SEED_MANTENCION.
            mysql_execute("""
                CREATE TABLE IF NOT EXISTS cat_clases_producto (
                  id           INT AUTO_INCREMENT PRIMARY KEY,
                  slug         VARCHAR(60) NOT NULL,
                  nombre       VARCHAR(120) NOT NULL,
                  orden        INT NOT NULL DEFAULT 0,
                  activo       TINYINT(1) NOT NULL DEFAULT 1,
                  created_by   VARCHAR(190) NULL,
                  updated_by   VARCHAR(190) NULL,
                  created_at   DATETIME DEFAULT CURRENT_TIMESTAMP,
                  updated_at   DATETIME DEFAULT CURRENT_TIMESTAMP
                                  ON UPDATE CURRENT_TIMESTAMP,
                  UNIQUE KEY uq_cat_clase_slug (slug),
                  KEY idx_cat_clase_activo (activo, orden)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            mysql_execute("""
                CREATE TABLE IF NOT EXISTS cat_clase_producto_tarifas (
                  id            INT AUTO_INCREMENT PRIMARY KEY,
                  clase_id      INT NOT NULL,
                  tipo_servicio ENUM('instalacion','mantencion') NOT NULL,
                  horas         DECIMAL(6,2) NULL,
                  tecnicos      INT NULL,
                  updated_by    VARCHAR(190) NULL,
                  updated_at    DATETIME DEFAULT CURRENT_TIMESTAMP
                                  ON UPDATE CURRENT_TIMESTAMP,
                  UNIQUE KEY uq_cat_clase_tarifa (clase_id, tipo_servicio),
                  CONSTRAINT fk_catclasetarifa_clase FOREIGN KEY (clase_id)
                     REFERENCES cat_clases_producto(id) ON DELETE CASCADE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            # 2026-07-22 (Daniel: "una clasificación para cada servicio que
            # damos, que sería mantención, visita técnica, y además hacer
            # editable una opción de venta de repuesto"): ampliar el ENUM de
            # 2 a 5 tipos de servicio. MODIFY COLUMN que ENSANCHA un ENUM es
            # idempotente y seguro en TODO boot (preserva filas existentes,
            # nunca las trunca -- mismo patrón que tk_tickets.tipo /
            # tk_cotizaciones.tipo_servicio). Los tipos nuevos nacen sin
            # tarifa (NULL) => calculan $0 hasta que Daniel los defina.
            mysql_execute(
                "ALTER TABLE cat_clase_producto_tarifas MODIFY COLUMN tipo_servicio "
                "  ENUM('instalacion','mantencion','visita_tecnica','venta_repuesto','otro') "
                "  NOT NULL")
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

        # Columnas nuevas 2026-07-13 (clase de producto + foto de piola).
        # Idempotente vía information_schema (mismo patron que arriba) para
        # que sobreviva a ILUS_SKIP_MIGRATIONS=1 en produccion.
        # 2026-07-21: ya nace VARCHAR (editable) en vez de ENUM fijo — ver
        # CAT_CLASES_MIGRACION_ENUM_VIEJO más abajo para el caso de una BD
        # que todavía tenga la columna vieja tipo ENUM.
        try:
            _col = mysql_fetchone(
                "SELECT 1 AS x FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='cat_productos' "
                "  AND COLUMN_NAME='clase_producto' LIMIT 1")
            if not _col:
                mysql_execute(
                    "ALTER TABLE cat_productos ADD COLUMN clase_producto VARCHAR(60) NULL "
                    "AFTER familia")
                mysql_execute(
                    "ALTER TABLE cat_productos ADD INDEX idx_cat_clase_producto (clase_producto)")
                print("[ensure_catalogo] columna clase_producto creada", flush=True)
        except Exception as _e_clase:
            print(f"[ILUS][WARN] clase_producto: {_e_clase}", flush=True)

        try:
            _col2 = mysql_fetchone(
                "SELECT 1 AS x FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='cat_producto_piolas' "
                "  AND COLUMN_NAME='foto_key' LIMIT 1")
            if not _col2:
                mysql_execute(
                    "ALTER TABLE cat_producto_piolas ADD COLUMN foto_key VARCHAR(500) NULL "
                    "AFTER observacion")
                print("[ensure_catalogo] columna foto_key (piolas) creada", flush=True)
        except Exception as _e_foto:
            print(f"[ILUS][WARN] cat_producto_piolas.foto_key: {_e_foto}", flush=True)

        # 2026-07-23 (Daniel, blueprint piolas -- plan con Fable, ver memoria
        # "blueprint_piolas_manuales_comunicaciones"): la piola pasa de una
        # sola "medida_cm" a diámetro (mm) + largo (m) + descripción de
        # ubicación (ahora el campo obligatorio -- "observacion" pasa a
        # opcional) + una SEGUNDA foto. medida_cm NO se borra ni se
        # renombra (piolas legadas la conservan como fallback de
        # visualización, Regla #4.2) -- solo deja de ser NOT NULL para que
        # las filas nuevas puedan omitirla.
        _piola_cols_nuevas = {
            "diametro_mm": "DECIMAL(4,1) NULL COMMENT 'mm, rango valido 3.0-10.0'",
            "largo_m":     "DECIMAL(7,2) NULL COMMENT 'metros'",
            "descripcion": "VARCHAR(300) NULL COMMENT 'ubicacion de la piola en la maquina'",
            "foto_key2":   "VARCHAR(500) NULL COMMENT 'segunda foto'",
        }
        for _col_name, _col_def in _piola_cols_nuevas.items():
            try:
                _existe = mysql_fetchone(
                    "SELECT 1 AS x FROM information_schema.COLUMNS "
                    "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='cat_producto_piolas' "
                    "  AND COLUMN_NAME=%s LIMIT 1", (_col_name,))
                if not _existe:
                    mysql_execute(f"ALTER TABLE cat_producto_piolas ADD COLUMN {_col_name} {_col_def}")
                    print(f"[ensure_catalogo] columna {_col_name} (piolas) creada", flush=True)
            except Exception as _e_pcol:
                print(f"[ILUS][WARN] cat_producto_piolas.{_col_name}: {_e_pcol}", flush=True)
        try:
            mysql_execute("ALTER TABLE cat_producto_piolas MODIFY medida_cm DECIMAL(6,1) NULL")
        except Exception as _e_med:
            print(f"[ILUS][WARN] cat_producto_piolas.medida_cm NULL: {_e_med}", flush=True)
        try:
            mysql_execute("ALTER TABLE cat_producto_piolas MODIFY observacion VARCHAR(300) NULL")
        except Exception as _e_obs:
            print(f"[ILUS][WARN] cat_producto_piolas.observacion NULL: {_e_obs}", flush=True)

        # Semilla de categorías (Daniel, tabla de mano de obra de
        # Mantención — confirmada contra "Tarifa mantenciones.xlsx",
        # 2026-07-21) — INSERT solo si el slug no existe todavía:
        # idempotente, y si Daniel ya editó nombre/horas desde
        # /catalogo/clases NO se pisa (Regla #4.2 — una vez creada, la UI
        # manda, esta semilla no vuelve a tocarla).
        try:
            for slug, nombre, horas, tecnicos, orden in CAT_CLASES_SEED_MANTENCION:
                fila = mysql_fetchone(
                    "SELECT id FROM cat_clases_producto WHERE slug=%s", (slug,))
                if not fila:
                    mysql_execute(
                        "INSERT INTO cat_clases_producto (slug, nombre, orden, created_by, updated_by) "
                        "VALUES (%s,%s,%s,'sistema','sistema')", (slug, nombre, orden))
                    fila = mysql_fetchone(
                        "SELECT id FROM cat_clases_producto WHERE slug=%s", (slug,))
                if fila and horas is not None and tecnicos is not None:
                    _tiene_tarifa = mysql_fetchone(
                        "SELECT id FROM cat_clase_producto_tarifas "
                        "WHERE clase_id=%s AND tipo_servicio='mantencion'", (fila["id"],))
                    if not _tiene_tarifa:
                        mysql_execute(
                            "INSERT INTO cat_clase_producto_tarifas "
                            "(clase_id, tipo_servicio, horas, tecnicos, updated_by) "
                            "VALUES (%s,'mantencion',%s,%s,'sistema')",
                            (fila["id"], horas, tecnicos))
            print("[ensure_catalogo] categorías de producto sembradas", flush=True)
        except Exception as _e_seed:
            print(f"[ILUS][WARN] seed cat_clases_producto: {_e_seed}", flush=True)

        # Migración clase_producto: ENUM fijo -> VARCHAR editable (Daniel
        # 2026-07-21). Idempotente vía information_schema.COLUMNS.DATA_TYPE
        # (una vez migrada la columna es VARCHAR y este bloque no hace nada).
        try:
            _tipo_col = mysql_fetchone(
                "SELECT DATA_TYPE FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='cat_productos' "
                "  AND COLUMN_NAME='clase_producto' LIMIT 1")
            if _tipo_col and (_tipo_col.get("DATA_TYPE") or "").lower() == "enum":
                mysql_execute(
                    "ALTER TABLE cat_productos MODIFY COLUMN clase_producto VARCHAR(60) NULL")
                for _viejo, _nuevo in CAT_CLASES_MIGRACION_ENUM_VIEJO.items():
                    mysql_execute(
                        "UPDATE cat_productos SET clase_producto=%s WHERE clase_producto=%s",
                        (_nuevo, _viejo))
                print("[ensure_catalogo] clase_producto migrada de ENUM a VARCHAR editable", flush=True)
        except Exception as _e_mig:
            print(f"[ILUS][WARN] migración clase_producto ENUM->VARCHAR: {_e_mig}", flush=True)

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

    _CAT_CLASES_CACHE = {"data": None, "ts": 0.0}
    _CAT_CLASES_CACHE_TTL_S = 30

    def _cat_clases_map(force=False):
        """{slug: nombre} de categorías ACTIVAS — caché en memoria de
        proceso 30s (mismo patrón que _shopify_fotos_cache de este mismo
        archivo) para no pegarle a MySQL en cada carga de un dropdown.
        Reemplaza al viejo diccionario fijo CAT_CLASES_PRODUCTO."""
        now = time.time()
        if not force and _CAT_CLASES_CACHE["data"] is not None and (now - _CAT_CLASES_CACHE["ts"]) < _CAT_CLASES_CACHE_TTL_S:
            return _CAT_CLASES_CACHE["data"]
        try:
            rows = mysql_fetchall(
                "SELECT slug, nombre FROM cat_clases_producto WHERE activo=1 ORDER BY orden, nombre") or []
            data = {r["slug"]: r["nombre"] for r in rows}
        except Exception as _e:
            print(f"[_cat_clases_map] error: {_e}", flush=True)
            data = _CAT_CLASES_CACHE["data"] or {}
        _CAT_CLASES_CACHE["data"] = data
        _CAT_CLASES_CACHE["ts"] = now
        return data

    def _cat_slugify(texto):
        """slug ascii_minusculas_con_guion_bajo, máx 60 chars — para el
        slug de una categoría nueva creada desde /catalogo/clases."""
        import unicodedata
        t = unicodedata.normalize("NFKD", texto or "").encode("ascii", "ignore").decode("ascii")
        t = t.strip().lower()
        out = []
        prev_us = False
        for ch in t:
            if ch.isalnum():
                out.append(ch)
                prev_us = False
            elif not prev_us:
                out.append("_")
                prev_us = True
        return "".join(out).strip("_")[:60]

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

    def _catalogo_eliminar_required(view):
        """2026-07-21 (Daniel, dictado): "eliminarlo solamente para el
        superadministrador con opciones a agregarlo en los roles" -- gate
        específico SOLO para cat_api_delete (archivar/soft-delete un
        producto). Acepta superadmin (siempre) O el flag granular
        g.permissions['cat_eliminar'] (matriz /admin/roles, módulo
        "catalogo" -> acción "eliminar", aditivo, nace en False para todos
        los roles hasta que Daniel lo prenda). El hard-delete definitivo
        sigue exigiendo superadmin+confirm_text adentro de cat_api_delete
        (Regla #5) -- este gate solo decide quién puede ENTRAR al endpoint.
        Resto del CRUD de productos (fotos, manuales, piolas, update, etc.)
        sigue en _catalogo_admin_required, sin tocar."""
        @wraps(view)
        def wrapped(*a, **k):
            perms = g.get("permissions") or {}
            if not (perms.get("superadmin") or perms.get("cat_eliminar")):
                if _is_ajaxish():
                    return jsonify({
                        "ok": False,
                        "error": "No tienes permiso para eliminar productos del Catálogo.",
                        "error_codigo": "SIN_PERMISO_CATALOGO_ELIMINAR",
                    }), 403
                return redirect(url_for("index"))
            return view(*a, **k)
        return login_required(wrapped)

    def _catalogo_producto_write_required(view):
        """2026-07-23 (Daniel, dictado): "ya es momento de que este módulo,
        el técnico pueda llamar a un producto nuevo y agregar las medidas de
        las piolas, y además los catálogos o manuales". REVIERTE la decisión
        del 2026-07-12 (que dejaba el CRUD de productos SOLO en superadmin,
        ver _catalogo_admin_required) para el flujo de terreno: crear/editar/
        clasificar un producto y subir fotos/manuales/medidas de piola ahora
        lo puede hacer cualquiera con acceso a Servicio Técnico (permiso
        'mantenciones' -> técnico y ejecutivo lo tienen) o superadmin.
        MISMA lógica que _catalogo_required, pero nombre propio para dejar
        explícito que es un gate de ESCRITURA de producto (no de solo lectura)
        y auditar el cambio de política. ELIMINAR productos/fotos/manuales/
        piolas sigue en su gate estricto (_catalogo_admin_required /
        _catalogo_eliminar_required) -- Daniel abrió crear/clasificar/cargar,
        NO borrar."""
        @wraps(view)
        def wrapped(*a, **k):
            perms = g.get("permissions") or {}
            if not (perms.get("mantenciones") or perms.get("superadmin")):
                if _is_ajaxish():
                    return jsonify({
                        "ok": False,
                        "error": "Tu usuario no tiene permiso para editar el Catálogo.",
                        "error_codigo": "SIN_PERMISO_CATALOGO",
                    }), 403
                return redirect(url_for("index"))
            return view(*a, **k)
        return login_required(wrapped)

    SORT_COLS = {
        "sku": "p.sku",
        "nombre": "p.nombre",
        "familia": "p.familia",
        "clase_producto": "p.clase_producto",
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

    @app.route("/catalogo/api/clases", methods=["GET"])
    @_catalogo_required
    def cat_api_clases():
        return jsonify({"ok": True, "clases": [
            {"value": k, "label": v} for k, v in _cat_clases_map().items()]})

    @app.route("/catalogo/clases")
    @_catalogo_admin_required
    def cat_clases_page():
        return render_template("catalogo/clases.html")

    @app.route("/catalogo/api/clases/admin", methods=["GET"])
    @_catalogo_admin_required
    def cat_api_clases_admin():
        """Listado completo (activas + inactivas) con horas/técnicos por
        tipo_servicio, para /catalogo/clases (solo superadmin)."""
        rows = mysql_fetchall(
            "SELECT id, slug, nombre, orden, activo FROM cat_clases_producto "
            "ORDER BY activo DESC, orden, nombre") or []
        tarifas = mysql_fetchall(
            "SELECT clase_id, tipo_servicio, horas, tecnicos FROM cat_clase_producto_tarifas") or []
        tmap = {}
        for t in tarifas:
            tmap.setdefault(t["clase_id"], {})[t["tipo_servicio"]] = t
        out = []
        for r in rows:
            r = dict(r)
            for ts in _CAT_TIPOS_SERVICIO_TARIFA:
                info = tmap.get(r["id"], {}).get(ts) or {}
                horas = info.get("horas")
                tecnicos = info.get("tecnicos")
                horas_f = float(horas) if horas is not None else None
                tecnicos_i = int(tecnicos) if tecnicos is not None else None
                r[f"{ts}_horas"] = horas_f
                r[f"{ts}_tecnicos"] = tecnicos_i
                r[f"{ts}_hh"] = (horas_f * tecnicos_i) if (horas_f is not None and tecnicos_i is not None) else None
            out.append(r)
        return jsonify({"ok": True, "clases": out,
                        "tipos_servicio": [{"key": k, "label": _CAT_TIPOS_SERVICIO_LABEL[k]}
                                           for k in _CAT_TIPOS_SERVICIO_TARIFA],
                        "config_precio": _cat_config_precio_leer()})

    # 2026-07-23 (Daniel, misma pantalla "Clasificación" del wizard de
    # cotizaciones: "el precio del técnico y de la hora técnica, y un
    # porcentaje de margen"). Son 2 valores GLOBALES (no por categoría) que
    # ya existían como tk_settings (cotiz_valor_hh/cotiz_margen_pct,
    # sembrados en tickets_module.py) -- acá solo se exponen para editarlos
    # desde /catalogo/clases en vez de tener que tocar la base directo.
    # tk_settings es una tabla clave/valor genérica ya usada cruzada entre
    # módulos (ver reply_to en app.py), no hace falta wiring especial.
    def _cat_config_precio_leer():
        rows = mysql_fetchall(
            "SELECT clave, valor FROM tk_settings "
            "WHERE clave IN ('cotiz_valor_hh','cotiz_margen_pct')") or []
        vals = {r["clave"]: r["valor"] for r in rows}
        try:
            valor_hh = float(vals.get("cotiz_valor_hh", 20000))
        except (TypeError, ValueError):
            valor_hh = 20000.0
        try:
            margen_pct = float(vals.get("cotiz_margen_pct", 40))
        except (TypeError, ValueError):
            margen_pct = 40.0
        return {"valor_hh": valor_hh, "margen_pct": margen_pct}

    @app.route("/catalogo/api/config-precio", methods=["PATCH"])
    @_catalogo_admin_required
    def cat_api_config_precio_actualizar():
        d = request.get_json(silent=True) or {}
        user = current_username() or "sistema"
        updates = []
        if "valor_hh" in d:
            try:
                v = float(d.get("valor_hh"))
            except (TypeError, ValueError):
                return jsonify({"ok": False, "error": "Valor de la hora técnica inválido"}), 400
            if v <= 0:
                return jsonify({"ok": False, "error": "El valor de la hora técnica debe ser mayor a 0"}), 400
            updates.append(("cotiz_valor_hh", str(v)))
        if "margen_pct" in d:
            try:
                v = float(d.get("margen_pct"))
            except (TypeError, ValueError):
                return jsonify({"ok": False, "error": "Porcentaje de margen inválido"}), 400
            if v < 0:
                return jsonify({"ok": False, "error": "El porcentaje de margen no puede ser negativo"}), 400
            updates.append(("cotiz_margen_pct", str(v)))
        if not updates:
            return jsonify({"ok": False, "error": "Sin cambios válidos"}), 400
        for clave, valor in updates:
            mysql_execute(
                "INSERT INTO tk_settings (clave, valor, updated_by) VALUES (%s,%s,%s) "
                "ON DUPLICATE KEY UPDATE valor=VALUES(valor), updated_by=VALUES(updated_by)",
                (clave, valor, user))
        if _audit:
            _audit("cat_config_precio_actualizado", target_type="tk_settings",
                   details={"cambios": dict(updates)})
        return jsonify({"ok": True, "config_precio": _cat_config_precio_leer()})

    # ── UF: valor actual + override manual (2026-07-22, Daniel: "actualizar
    #    el precio de la UF a diario por una API... y que se pueda editar en
    #    caso de cualquier detalle"). El motor está en app.py
    #    (_uf_valor_actual con override>API>DB); acá solo se administra el
    #    override desde la misma pantalla /catalogo/clases. ──
    @app.route("/catalogo/api/uf", methods=["GET"])
    @_catalogo_admin_required
    def cat_api_uf_estado():
        _uf = ctx.get("_uf_valor_actual")
        estado = _uf() if _uf else {"ok": False, "uf": None}
        return jsonify({"ok": True, "uf": estado})

    @app.route("/catalogo/api/uf/override", methods=["PATCH", "DELETE"])
    @_catalogo_admin_required
    def cat_api_uf_override():
        user = current_username() or "sistema"
        _invalidate = ctx.get("_uf_override_cache_invalidate")
        if request.method == "DELETE":
            # Quitar el override -> vuelve a mandar la API/DB.
            for clave in ("uf_override_valor", "uf_override_fecha", "uf_override_by", "uf_override_at"):
                mysql_execute("DELETE FROM tk_settings WHERE clave=%s", (clave,))
            if _invalidate:
                _invalidate()
            if _audit:
                _audit("uf_override_quitado", target_type="tk_settings", details={})
            _uf = ctx.get("_uf_valor_actual")
            return jsonify({"ok": True, "uf": (_uf() if _uf else None)})
        d = request.get_json(silent=True) or {}
        try:
            valor = float(d.get("valor"))
        except (TypeError, ValueError):
            return jsonify({"ok": False, "error": "Valor de UF inválido"}), 400
        if valor <= 0:
            return jsonify({"ok": False, "error": "El valor de la UF debe ser mayor a 0"}), 400
        fecha = (d.get("fecha") or "").strip()[:10] or None
        from datetime import datetime as _dt
        ahora = None
        try:
            ahora = (chile_fmt(_dt.utcnow()) if chile_fmt else str(_dt.utcnow()))
        except Exception:
            ahora = ""
        pares = [("uf_override_valor", str(valor)), ("uf_override_by", user), ("uf_override_at", ahora or "")]
        if fecha:
            pares.append(("uf_override_fecha", fecha))
        else:
            mysql_execute("DELETE FROM tk_settings WHERE clave='uf_override_fecha'")
        for clave, val in pares:
            mysql_execute(
                "INSERT INTO tk_settings (clave, valor, updated_by) VALUES (%s,%s,%s) "
                "ON DUPLICATE KEY UPDATE valor=VALUES(valor), updated_by=VALUES(updated_by)",
                (clave, val, user))
        if _invalidate:
            _invalidate()
        if _audit:
            _audit("uf_override_fijado", target_type="tk_settings",
                   details={"valor": valor, "fecha": fecha})
        _uf = ctx.get("_uf_valor_actual")
        return jsonify({"ok": True, "uf": (_uf() if _uf else None)})

    @app.route("/catalogo/api/clases", methods=["POST"])
    @_catalogo_admin_required
    def cat_api_clases_create():
        """Crear categoría nueva (Daniel: "si sale un producto nuevo lo
        podemos crear"). Sin tarifas iniciales -- se cargan aparte por
        tipo_servicio desde /catalogo/clases."""
        d = request.get_json(silent=True) or {}
        nombre = (d.get("nombre") or "").strip()[:120]
        if not nombre:
            return jsonify({"ok": False, "error": "Falta el nombre de la categoría"}), 400
        slug = _cat_slugify(nombre)
        if not slug:
            return jsonify({"ok": False, "error": "Nombre inválido"}), 400
        user = current_username() or "sistema"
        try:
            _max = mysql_fetchone("SELECT COALESCE(MAX(orden),0) AS m FROM cat_clases_producto") or {}
            orden = int(_max.get("m") or 0) + 10
            mysql_execute(
                "INSERT INTO cat_clases_producto (slug, nombre, orden, created_by, updated_by) "
                "VALUES (%s,%s,%s,%s,%s)", (slug, nombre, orden, user, user))
        except Exception as _e:
            msg = str(_e)
            if "Duplicate entry" in msg or "uq_cat_clase_slug" in msg:
                return jsonify({"ok": False, "error": "Ya existe una categoría equivalente"}), 409
            print(f"[cat_api_clases_create] error: {_e}", flush=True)
            return jsonify({"ok": False, "error": "No se pudo crear la categoría"}), 500
        _cat_clases_map(force=True)
        row = mysql_fetchone("SELECT id FROM cat_clases_producto WHERE slug=%s", (slug,))
        return jsonify({"ok": True, "id": row["id"] if row else None, "slug": slug})

    @app.route("/catalogo/api/clases/<int:clid>", methods=["PATCH"])
    @_catalogo_admin_required
    def cat_api_clases_update(clid):
        """Renombrar / activar / desactivar / reordenar una categoría.
        Desactivar es SOFT (Regla #5): no se borra, solo deja de ofrecerse
        en los dropdowns nuevos -- los productos ya clasificados con ella
        conservan el dato."""
        if not mysql_fetchone("SELECT id FROM cat_clases_producto WHERE id=%s", (clid,)):
            return jsonify({"ok": False, "error": "Categoría no encontrada"}), 404
        d = request.get_json(silent=True) or {}
        sets, params = [], []
        if "nombre" in d:
            nombre = (d.get("nombre") or "").strip()[:120]
            if not nombre:
                return jsonify({"ok": False, "error": "El nombre no puede quedar vacío"}), 400
            sets.append("nombre=%s"); params.append(nombre)
        if "activo" in d:
            sets.append("activo=%s"); params.append(1 if d.get("activo") else 0)
        if "orden" in d:
            try:
                params_orden = int(d.get("orden"))
            except Exception:
                return jsonify({"ok": False, "error": "orden inválido"}), 400
            sets.append("orden=%s"); params.append(params_orden)
        if not sets:
            return jsonify({"ok": False, "error": "Sin cambios válidos"}), 400
        sets.append("updated_by=%s")
        params.append(current_username() or "sistema")
        params.append(clid)
        mysql_execute(f"UPDATE cat_clases_producto SET {', '.join(sets)} WHERE id=%s", tuple(params))
        _cat_clases_map(force=True)
        return jsonify({"ok": True})

    @app.route("/catalogo/api/clases/<int:clid>/tarifas", methods=["PATCH"])
    @_catalogo_admin_required
    def cat_api_clases_tarifas_update(clid):
        """Fija Hora/Técnicos de UNA categoría para UN tipo_servicio
        (mantención/instalación/visita técnica/venta de repuesto/otro —
        Daniel: "una clasificación para cada servicio que damos"). HH total
        no se guarda: se calcula (horas × técnicos) en cada lectura."""
        if not mysql_fetchone("SELECT id FROM cat_clases_producto WHERE id=%s", (clid,)):
            return jsonify({"ok": False, "error": "Categoría no encontrada"}), 404
        d = request.get_json(silent=True) or {}
        tipo_servicio = (d.get("tipo_servicio") or "").strip().lower()
        if tipo_servicio not in _CAT_TIPOS_SERVICIO_TARIFA:
            return jsonify({"ok": False, "error": "tipo_servicio inválido"}), 400
        horas_in = d.get("horas")
        tecnicos_in = d.get("tecnicos")
        try:
            horas_v = round(float(horas_in), 2) if horas_in not in (None, "") else None
            tecnicos_v = int(tecnicos_in) if tecnicos_in not in (None, "") else None
        except Exception:
            return jsonify({"ok": False, "error": "Horas/técnicos inválidos"}), 400
        # 2026-07-21 (revisión adversarial): horas=0 o técnicos=0 NO se
        # aceptan como tarifa "válida" -- quedarían indistinguibles de
        # "categoría sin tarifa cargada" en _cat_obtener_tarifa_clase
        # (NULL es la única forma de decir "no configurado"; 0 pasaría el
        # filtro IS NOT NULL y produciría precio $0 silencioso, disfrazado
        # de tarifa real). Ningún servicio real necesita 0 horas o 0
        # técnicos -- para "sin definir" se deja el campo vacío (NULL).
        if horas_v is not None and horas_v <= 0:
            return jsonify({"ok": False, "error": "Las horas deben ser mayores a 0 (deja el campo vacío si aún no está definida)"}), 400
        if tecnicos_v is not None and tecnicos_v < 1:
            return jsonify({"ok": False, "error": "La cantidad de técnicos debe ser al menos 1 (deja el campo vacío si aún no está definida)"}), 400
        user = current_username() or "sistema"
        mysql_execute(
            "INSERT INTO cat_clase_producto_tarifas (clase_id, tipo_servicio, horas, tecnicos, updated_by) "
            "VALUES (%s,%s,%s,%s,%s) "
            "ON DUPLICATE KEY UPDATE horas=VALUES(horas), tecnicos=VALUES(tecnicos), updated_by=VALUES(updated_by)",
            (clid, tipo_servicio, horas_v, tecnicos_v, user))
        return jsonify({"ok": True})

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
        clase_producto = (request.args.get("clase_producto") or "").strip()
        activo_arg = (request.args.get("activo") or "1").strip()
        activo = 0 if activo_arg == "0" else 1
        # 2026-07-23 (Daniel, insiste: "necesito que me muestre todos los
        # productos trabajados... así tenga al menos una de las tres
        # solicitudes"): cada SKU que pasa por una cotización nace en
        # cat_productos aunque nadie lo haya clasificado ni le haya
        # cargado nada (_cat_crear_o_reusar_producto_desde_erp) -- esas
        # filas 100% en blanco inundaban la lista y tapaban los productos
        # con trabajo real encima. Default ON (solo_trabajados=1 salvo que
        # el front mande explícitamente "0"): mismo criterio OR que el
        # badge "Registrado" (clase O fotos O manual).
        solo_trabajados = (request.args.get("solo_trabajados") or "1").strip() != "0"

        # 2026-07-21 (Daniel): el gap-fill silencioso que llamaba a
        # _cat_sync_erp_nuevos() al buscar sin resultados locales quedó
        # DESACTIVADO a propósito -- el catálogo se llena solo con
        # productos elegidos por un humano (alta manual, selección en
        # cotización, o "+ Nuevo producto" contra el ERP). Buscar en este
        # listado ya NO crea filas nuevas. La función _cat_sync_erp_nuevos
        # y el endpoint POST /catalogo/api/sync-erp siguen existiendo
        # (infraestructura viva, sin botón en la UI hoy) -- solo se quitó
        # ESTA llamada puntual.

        # where_base/params_base: TODOS los filtros excepto solo_trabajados
        # -- separados para poder calcular el total SIN ese filtro también
        # (transparencia, ver más abajo) sin desalinear params con where.
        where_base = ["p.activo=%s", "p.created_by <> 'sistema-erp-sync'"]
        params_base = [activo]
        if q:
            where_base.append("(p.sku LIKE %s OR p.nombre LIKE %s OR p.familia LIKE %s)")
            like = f"%{q}%"
            params_base += [like, like, like]
        if familia:
            where_base.append("p.familia=%s")
            params_base.append(familia)
        if clase_producto:
            if clase_producto not in _cat_clases_map():
                return jsonify({"ok": False, "error": "Clase de producto inválida"}), 400
            where_base.append("p.clase_producto=%s")
            params_base.append(clase_producto)

        _clausula_trabajados = """(
                (p.clase_producto IS NOT NULL AND p.clase_producto <> '')
                OR p.manual_pdf_key IS NOT NULL
                OR EXISTS (SELECT 1 FROM cat_producto_fotos f WHERE f.producto_id=p.id)
                OR EXISTS (SELECT 1 FROM cat_producto_manuales m WHERE m.producto_id=p.id)
            )"""
        where = list(where_base) + ([_clausula_trabajados] if solo_trabajados else [])
        params = list(params_base)
        where_sql = " AND ".join(where)

        total = int((mysql_fetchone(
            f"SELECT COUNT(*) AS n FROM cat_productos p WHERE {where_sql}",
            tuple(params)) or {}).get("n") or 0)
        pages = max(1, math.ceil(total / limit))
        page = min(page, pages)
        offset = (page - 1) * limit

        # Transparencia (no silenciar un filtro): si "solo_trabajados" está
        # activo, cuántos hay en total SIN ese filtro -- para que el front
        # pueda decir "127 de 340" en vez de esconder que hay más filas.
        total_sin_filtro = total
        if solo_trabajados:
            where_sql_base = " AND ".join(where_base)
            total_sin_filtro = int((mysql_fetchone(
                f"SELECT COUNT(*) AS n FROM cat_productos p WHERE {where_sql_base}",
                tuple(params_base)) or {}).get("n") or 0)

        # 2026-07-21: a propósito NO se seleccionan p.created_by/p.updated_by
        # acá -- el listado nunca debe exponer el username de quién creó/editó
        # cada fila (eso vive SOLO en la sección "Auditoría" del modal de
        # ficha, gateada a superadmin en cat_api_detalle). Si algún día se
        # agregan a este SELECT, hay que replicar el mismo filtro que ahí.
        rows = mysql_fetchall(
            f"""
            SELECT p.id, p.sku, p.nombre, p.familia, p.clase_producto, p.activo, p.updated_at,
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
            row["clase_producto_label"] = _cat_clases_map().get(row.get("clase_producto") or "")
            # "registrado" (2026-07-23, Daniel: "se debería mostrar los
            # archivos que ya tengo gestionado, bien sea por clasificación,
            # bien sea por foto, bien sea por PDF... al menos tiene que
            # tener una de esas tres"). ANTES exigía family + piola + manual
            # LAS TRES juntas (2026-07-12) -- con eso casi ningún producto
            # se veía "gestionado" aunque ya tuviera trabajo real encima.
            # Ahora basta con UNA: clasificación (clase_producto) O fotos
            # O manual/PDF.
            tiene_manual_alguno = bool(row.get("tiene_manual")) or int(row.get("total_manuales") or 0) > 0
            tiene_clasificacion = bool((row.get("clase_producto") or "").strip())
            tiene_fotos = int(row.get("total_fotos") or 0) > 0
            row["registrado"] = tiene_clasificacion or tiene_fotos or tiene_manual_alguno
            rows_out.append(row)

        return jsonify({
            "ok": True,
            "rows": rows_out,
            "total": total, "pages": pages, "page": page, "limit": limit,
            "solo_trabajados": solo_trabajados,
            "total_sin_filtro": total_sin_filtro,
        })

    # ─────────────────────────────────────────────────────────────────
    #  API — CRUD producto
    # ─────────────────────────────────────────────────────────────────
    @app.route("/catalogo/api/productos", methods=["POST"])
    @_catalogo_producto_write_required
    def cat_api_create():
        d = request.get_json(silent=True) or {}
        sku = (d.get("sku") or "").strip().upper()
        nombre = (d.get("nombre") or "").strip()
        if not sku:
            return jsonify({"ok": False, "error": "Falta el SKU"}), 400
        if not nombre:
            return jsonify({"ok": False, "error": "Falta el nombre"}), 400
        familia = (d.get("familia") or "").strip()[:150] or None
        clase_producto = (d.get("clase_producto") or "").strip() or None
        if clase_producto and clase_producto not in _cat_clases_map():
            return jsonify({"ok": False, "error": "Clase de producto inválida"}), 400
        observacion = (d.get("observacion") or "").strip() or None
        user = current_username() or "sistema"
        try:
            mysql_execute(
                "INSERT INTO cat_productos (sku, nombre, familia, clase_producto, observacion, created_by, updated_by) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s)",
                (sku[:100], nombre[:300], familia, clase_producto, observacion, user, user))
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
            f"SELECT {_PIOLA_SELECT_COLS} FROM cat_producto_piolas "
            "WHERE producto_id=%s AND activo=1 ORDER BY orden", (pid,))
        manuales = mysql_fetchall(
            "SELECT id, gcs_key, nombre_archivo, size_kb, orden FROM cat_producto_manuales "
            "WHERE producto_id=%s ORDER BY orden", (pid,))
        producto = _fmt_row(p)  # Regla #6: created_at/updated_at a hora Chile
        producto["clase_producto_label"] = _cat_clases_map().get(producto.get("clase_producto") or "")
        manual_key = producto.pop("manual_pdf_key", None)
        tiene_manual_alguno = bool(manual_key) or len(manuales) > 0
        # 2026-07-23 (Daniel): mismo criterio OR que cat_api_list -- basta
        # clasificación O fotos O manual, no las tres juntas.
        tiene_clasificacion = bool((producto.get("clase_producto") or "").strip())
        producto["registrado"] = tiene_clasificacion or bool(fotos) or tiene_manual_alguno
        # 2026-07-21 (Daniel, Etapa 2 "acciones unificadas"): la sección
        # "Auditoría" (quién creó/editó el producto) es SOLO para superadmin.
        # No alcanza con ocultarla en el frontend -- el network tab expondría
        # igual el username a cualquier rol con acceso al catálogo -- así que
        # se quita del payload acá si el que pide no es superadmin.
        if not bool((g.get("permissions") or {}).get("superadmin")):
            producto.pop("created_by", None)
            producto.pop("updated_by", None)
        return jsonify({
            "ok": True,
            "producto": producto,
            "fotos": [{"id": f["id"], "url": "/f/" + f["gcs_key"], "orden": f["orden"]} for f in fotos],
            "piolas": [_piola_serializar(pl) for pl in piolas],
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
    @_catalogo_producto_write_required
    def cat_api_update(pid):
        prev = mysql_fetchone("SELECT id, sku FROM cat_productos WHERE id=%s", (pid,))
        if not prev:
            return jsonify({"ok": False, "error": "Producto no encontrado"}), 404
        d = request.get_json(silent=True) or {}
        allowed = ("sku", "nombre", "familia", "clase_producto", "observacion")
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
                elif key == "clase_producto":
                    if val and val not in _cat_clases_map():
                        return jsonify({"ok": False, "error": "Clase de producto inválida"}), 400
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
    @_catalogo_eliminar_required
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
    @_catalogo_producto_write_required
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
    @_catalogo_producto_write_required
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
    # Rango válido del diámetro en mm (Daniel: "el diámetro... lo pondría
    # a un límite de diez milímetros hasta tres" -- 3.0 a 10.0 inclusive).
    PIOLA_DIAMETRO_MIN_MM = 3.0
    PIOLA_DIAMETRO_MAX_MM = 10.0
    PIOLA_LARGO_MAX_M = 200.0  # techo sano anti-typo, no un límite real de negocio
    _PIOLA_SELECT_COLS = ("id, medida_cm, observacion, descripcion, diametro_mm, largo_m, "
                          "orden, foto_key, foto_key2")

    def _piola_serializar(r):
        """Shape único de una fila de cat_producto_piolas -> dict de API.
        Compartido por cat_api_piolas_list y cat_api_detalle (Regla: un
        solo lugar que decide el shape, no duplicar la lógica).
        2026-07-23 (blueprint piolas): filas legadas (creadas antes del
        esquema nuevo) no tienen diametro_mm/largo_m/descripcion -- se
        exponen igual con "legado": True para que el front lo muestre con
        un aviso en vez de romper, sin inventar datos."""
        es_legado = r.get("diametro_mm") is None and r.get("largo_m") is None
        return {
            "id": r["id"], "orden": r["orden"], "legado": es_legado,
            "diametro_mm": float(r["diametro_mm"]) if r.get("diametro_mm") is not None else None,
            "largo_m": float(r["largo_m"]) if r.get("largo_m") is not None else None,
            "descripcion": r.get("descripcion") or (r.get("observacion") if es_legado else None),
            "observacion": r.get("observacion"),
            "medida_cm_legada": float(r["medida_cm"]) if es_legado and r.get("medida_cm") is not None else None,
            "foto_url": ("/f/" + r["foto_key"]) if r.get("foto_key") else None,
            "foto_url2": ("/f/" + r["foto_key2"]) if r.get("foto_key2") else None,
        }

    @app.route("/catalogo/api/productos/<int:pid>/piolas", methods=["GET"])
    @_catalogo_required
    def cat_api_piolas_list(pid):
        if not mysql_fetchone("SELECT id FROM cat_productos WHERE id=%s", (pid,)):
            return jsonify({"ok": False, "error": "Producto no encontrado"}), 404
        rows = mysql_fetchall(
            f"SELECT {_PIOLA_SELECT_COLS} FROM cat_producto_piolas "
            "WHERE producto_id=%s AND activo=1 ORDER BY orden", (pid,))
        return jsonify({"ok": True, "piolas": [_piola_serializar(r) for r in rows]})

    def _piola_validar_diametro(d):
        try:
            v = float(d.get("diametro_mm"))
        except (TypeError, ValueError):
            return None, "El diámetro es inválido"
        if v < PIOLA_DIAMETRO_MIN_MM or v > PIOLA_DIAMETRO_MAX_MM:
            return None, f"El diámetro debe estar entre {PIOLA_DIAMETRO_MIN_MM:g} y {PIOLA_DIAMETRO_MAX_MM:g} mm"
        return v, None

    def _piola_validar_largo(d):
        try:
            v = float(d.get("largo_m"))
        except (TypeError, ValueError):
            return None, "El largo es inválido"
        if v <= 0 or v > PIOLA_LARGO_MAX_M:
            return None, f"El largo debe ser mayor a 0 y hasta {PIOLA_LARGO_MAX_M:g} m"
        return v, None

    @app.route("/catalogo/api/productos/<int:pid>/piolas", methods=["POST"])
    @_catalogo_required
    def cat_api_piolas_crear(pid):
        prod = mysql_fetchone("SELECT id, sku FROM cat_productos WHERE id=%s", (pid,))
        if not prod:
            return jsonify({"ok": False, "error": "Producto no encontrado"}), 404
        d = request.get_json(silent=True) or {}
        # 2026-07-23 (blueprint piolas, Regla #4.2): el wizard "Registrar
        # producto" (catw* en list.html) TODAVIA crea piolas mandando solo
        # {medida_cm, observacion} -- no se toca su JS todavia (eso es
        # Paso 2). Si el payload no trae diametro_mm/largo_m, se crea en
        # formato LEGADO (igual que una piola vieja): diametro_mm/largo_m/
        # descripcion quedan NULL, se guarda medida_cm+observacion. La UI
        # nueva (acordeón) SI mandará diametro_mm/largo_m y entra por la
        # rama nueva.
        es_legado = "diametro_mm" not in d and "largo_m" not in d
        if es_legado:
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
            diametro_mm = largo_m = descripcion = None
        else:
            medida_cm = None
            diametro_mm, _err_d = _piola_validar_diametro(d)
            if _err_d:
                return jsonify({"ok": False, "error": _err_d}), 400
            largo_m, _err_l = _piola_validar_largo(d)
            if _err_l:
                return jsonify({"ok": False, "error": _err_l}), 400
            # La "descripción" (ubicación) es el campo obligatorio para
            # distinguir cuál piola es -- "observación" pasa a opcional.
            descripcion = (d.get("descripcion") or "").strip()
            if not descripcion:
                return jsonify({"ok": False, "error": "La descripción (ubicación) es obligatoria para distinguir cuál piola es"}), 400
            descripcion = descripcion[:300]
            observacion = (d.get("observacion") or "").strip()[:300] or None

        total = int((mysql_fetchone(
            "SELECT COUNT(*) AS n FROM cat_producto_piolas WHERE producto_id=%s AND activo=1",
            (pid,)) or {}).get("n") or 0)
        if total >= MAX_PIOLAS_POR_PRODUCTO:
            return jsonify({"ok": False, "error": f"Máximo {MAX_PIOLAS_POR_PRODUCTO} piolas por producto"}), 400

        user = current_username() or "sistema"
        try:
            mysql_execute(
                "INSERT INTO cat_producto_piolas "
                "(producto_id, medida_cm, diametro_mm, largo_m, descripcion, observacion, orden, created_by, updated_by) "
                "VALUES (%s,%s,%s,%s,%s,%s, (SELECT t.m FROM (SELECT COALESCE(MAX(orden),0)+1 AS m FROM cat_producto_piolas WHERE producto_id=%s) t), %s,%s)",
                (pid, medida_cm, diametro_mm, largo_m, descripcion, observacion, pid, user, user))
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
                             "medida_cm": medida_cm, "diametro_mm": diametro_mm, "largo_m": largo_m,
                             "descripcion_despues": descripcion, "observacion_despues": observacion})
        return jsonify({"ok": True, "id": nuevo_id})

    @app.route("/catalogo/api/productos/<int:pid>/piolas/<int:piola_id>", methods=["PATCH"])
    @_catalogo_producto_write_required
    def cat_api_piolas_editar(pid, piola_id):
        prod = mysql_fetchone("SELECT sku FROM cat_productos WHERE id=%s", (pid,))
        if not prod:
            return jsonify({"ok": False, "error": "Producto no encontrado"}), 404
        prev = mysql_fetchone(
            "SELECT medida_cm, observacion, descripcion, diametro_mm, largo_m "
            "FROM cat_producto_piolas WHERE id=%s AND producto_id=%s AND activo=1", (piola_id, pid))
        if not prev:
            return jsonify({"ok": False, "error": "Piola no encontrada"}), 404

        d = request.get_json(silent=True) or {}
        sets, params = [], []
        detalle = {}

        if "diametro_mm" in d:
            diametro_mm, _err = _piola_validar_diametro(d)
            if _err:
                return jsonify({"ok": False, "error": _err}), 400
            sets.append("diametro_mm=%s"); params.append(diametro_mm)
            detalle["diametro_mm_antes"] = prev.get("diametro_mm")
            detalle["diametro_mm_despues"] = diametro_mm
        if "largo_m" in d:
            largo_m, _err = _piola_validar_largo(d)
            if _err:
                return jsonify({"ok": False, "error": _err}), 400
            sets.append("largo_m=%s"); params.append(largo_m)
            detalle["largo_m_antes"] = prev.get("largo_m")
            detalle["largo_m_despues"] = largo_m
        if "descripcion" in d:
            descripcion = (d.get("descripcion") or "").strip()
            if not descripcion:
                return jsonify({"ok": False, "error": "La descripción (ubicación) es obligatoria"}), 400
            descripcion = descripcion[:300]
            sets.append("descripcion=%s"); params.append(descripcion)
            detalle["descripcion_antes"] = prev.get("descripcion")
            detalle["descripcion_despues"] = descripcion
        if "observacion" in d:
            observacion = (d.get("observacion") or "").strip()[:300] or None
            sets.append("observacion=%s"); params.append(observacion)
            detalle["observacion_antes"] = prev.get("observacion")
            detalle["observacion_despues"] = observacion
        # medida_cm: legado, ya no se edita desde la UI nueva -- se acepta
        # igual por si algún caller viejo la manda, sin exigirla.
        if "medida_cm" in d:
            try:
                medida_cm = float(d.get("medida_cm"))
            except (TypeError, ValueError):
                return jsonify({"ok": False, "error": "Medida inválida"}), 400
            if medida_cm <= 0:
                return jsonify({"ok": False, "error": "La medida debe ser mayor que 0"}), 400
            sets.append("medida_cm=%s"); params.append(medida_cm)
            detalle["medida_cm_antes"] = float(prev["medida_cm"]) if prev.get("medida_cm") is not None else None
            detalle["medida_cm_despues"] = medida_cm
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
                   details={"producto_id": pid, "sku": prod.get("sku"), **detalle})
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

    @app.route("/catalogo/api/productos/<int:pid>/piolas/<int:piola_id>/foto", methods=["POST"])
    @_catalogo_producto_write_required
    def cat_api_piolas_foto_upload(pid, piola_id):
        # 2026-07-13 (Daniel: "las piolas van a requerir fotos"). Reusa
        # EXACTAMENTE el mismo mecanismo de subida que fotos de producto
        # (_uploader_upload -> GCS -> "/f/<key>", ver cat_api_upload_foto).
        # 2026-07-23 (blueprint piolas): ?slot=1|2 -- dos fotos por piola
        # (dos columnas, no tabla hija, cardinalidad fija). slot=1 por
        # default -> retrocompatible con callers viejos que no lo mandan.
        slot = (request.args.get("slot") or request.form.get("slot") or "1").strip()
        if slot not in ("1", "2"):
            return jsonify({"ok": False, "error": "Slot de foto inválido"}), 400
        col_foto = "foto_key" if slot == "1" else "foto_key2"
        prod = mysql_fetchone("SELECT sku FROM cat_productos WHERE id=%s", (pid,))
        if not prod:
            return jsonify({"ok": False, "error": "Producto no encontrado"}), 404
        prev = mysql_fetchone(
            f"SELECT {col_foto} AS foto_key FROM cat_producto_piolas WHERE id=%s AND producto_id=%s AND activo=1",
            (piola_id, pid))
        if not prev:
            return jsonify({"ok": False, "error": "Piola no encontrada"}), 404
        if not _uploader_upload:
            return jsonify({"ok": False, "error": "Almacenamiento no disponible"}), 503
        f = request.files.get("file") or request.files.get("archivo")
        if not f or not f.filename:
            return jsonify({"ok": False, "error": "No llegó ningún archivo"}), 400

        mime = (f.mimetype or "").lower()
        if not mime.startswith("image/"):
            return jsonify({"ok": False, "error": "La foto de la piola debe ser una imagen"}), 400

        f.seek(0, 2)
        size_mb = f.tell() / (1024 * 1024)
        f.seek(0)
        if size_mb > MAX_FOTO_PIOLA_MB:
            return jsonify({"ok": False, "error": f"La foto supera el máximo de {MAX_FOTO_PIOLA_MB} MB"}), 400

        try:
            res = _uploader_upload(f, folder="catalogo/piolas", resource_type="image")
        except Exception as _e:
            print(f"[cat_piolas_foto_upload] error pid={pid} piola={piola_id}: {_e}", flush=True)
            return jsonify({"ok": False, "error": "No se pudo subir la foto"}), 500
        key = res.get("public_id")
        url = res.get("secure_url") or res.get("url") or (("/f/" + key) if key else None)
        if not key or not url:
            return jsonify({"ok": False, "error": "Subida sin resultado válido"}), 500

        # 2026-07-13 (stress-test/revisión adversarial): NO usar el `old_key`
        # leído en `prev` de más arriba para decidir qué blob borrar — entre
        # ese SELECT y este UPDATE hay una subida a GCS de por medio (I/O
        # lento), así que dos requests casi simultáneos a la MISMA piola
        # (doble click) pueden leer el mismo `prev.foto_key`, subir cada uno
        # su propia foto, y el que escribe SEGUNDO en la tabla pisa el
        # foto_key del que escribió PRIMERO sin enterarse — ese primer blob
        # queda huérfano en GCS para siempre (nadie lo referencia ni lo
        # borra). Fix: UPDATE atómico con variable de sesión MySQL que
        # captura el foto_key que HABÍA justo antes de esta escritura (no el
        # que se leyó minutos/segundos antes) — mismo connection/sesión que
        # mysql_fetchone porque get_db() reutiliza la conexión del request.
        try:
            # col_foto sale del whitelist slot in ("1","2") de arriba --
            # nunca de input crudo, seguro de interpolar.
            mysql_execute(
                f"UPDATE cat_producto_piolas "
                f"SET {col_foto}=@cat_old_foto_key:={col_foto}, {col_foto}=%s, updated_by=%s "
                f"WHERE id=%s AND producto_id=%s",
                (key, current_username() or "sistema", piola_id, pid))
            old_key = (mysql_fetchone("SELECT @cat_old_foto_key AS k") or {}).get("k")
        except Exception as _e:
            print(f"[cat_piolas_foto_upload] UPDATE falló, limpiando blob pid={pid} piola={piola_id}: {_e}", flush=True)
            if _uploader_destroy:
                try:
                    _uploader_destroy(key)
                except Exception:
                    pass
            return jsonify({"ok": False, "error": "No se pudo registrar la foto"}), 500

        if old_key and old_key != key and _uploader_destroy:
            try:
                _uploader_destroy(old_key)
            except Exception:
                pass
        return jsonify({"ok": True, "url": url})

    @app.route("/catalogo/api/productos/<int:pid>/piolas/<int:piola_id>/foto", methods=["DELETE"])
    @_catalogo_admin_required
    def cat_api_piolas_foto_delete(pid, piola_id):
        # 2026-07-23 (blueprint piolas): ?slot=1|2, default 1 (retrocompatible).
        slot = (request.args.get("slot") or "1").strip()
        if slot not in ("1", "2"):
            return jsonify({"ok": False, "error": "Slot de foto inválido"}), 400
        col_foto = "foto_key" if slot == "1" else "foto_key2"
        prev = mysql_fetchone(
            f"SELECT {col_foto} AS foto_key FROM cat_producto_piolas WHERE id=%s AND producto_id=%s AND activo=1",
            (piola_id, pid))
        if not prev:
            return jsonify({"ok": False, "error": "Piola no encontrada"}), 404
        key = prev.get("foto_key")
        mysql_execute(
            f"UPDATE cat_producto_piolas SET {col_foto}=NULL, updated_by=%s WHERE id=%s AND producto_id=%s",
            (current_username() or "sistema", piola_id, pid))
        if key and _uploader_destroy:
            try:
                _uploader_destroy(key)
            except Exception:
                pass
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
    @_catalogo_producto_write_required
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

    # ─────────────────────────────────────────────────────────────────
    #  FOTO DESDE ECOMMERCE — helper compartido (2026-07-14, Daniel:
    #  "tráelos automáticos y déjalo vacío si da error"). Lo usan el alta
    #  puntual desde ERP (cat_api_producto_desde_erp) y el backfill masivo
    #  (cat_api_fotos_desde_ecommerce). Best-effort TOTAL: nunca lanza y
    #  nunca afecta la creación del producto.
    # ─────────────────────────────────────────────────────────────────
    def _intentar_foto_ecommerce(producto_id, sku, fotos_map=None):
        """Busca el SKU (upper/strip) en la tienda ilusfitness.com y, si hay
        match, descarga la imagen principal y la sube por el MISMO pipeline
        de fotos del catálogo (_uploader_upload → GCS → cat_producto_fotos,
        mismos folder/resource_type que cat_api_upload_foto).
        Devuelve 'ok' | 'sin_match' | 'error'."""
        try:
            if not _uploader_upload:
                return "error"
            mapa = fotos_map if fotos_map is not None else _shopify_fotos_cache()
            img_url = mapa.get((str(sku) if sku is not None else "").strip().upper())
            if not img_url:
                return "sin_match"
            data = _shopify_descargar_imagen(img_url)
            if not data:
                return "error"
            # public_id explícito y único: el default de _uploader_upload es
            # f_{segundos} — en el backfill se suben varias fotos por segundo
            # y colisionarían en la misma key de GCS.
            res = _uploader_upload(
                data,
                public_id=f"ecom_{producto_id}_{int(time.time())}",
                folder="catalogo", resource_type="image")
            key = (res or {}).get("public_id")
            if not key:
                return "error"
            try:
                # Mismo cálculo de orden que piolas/manuales (derivada con
                # alias — patrón seguro para MySQL al leer y escribir la
                # misma tabla). En productos nuevos queda orden=1.
                mysql_execute(
                    "INSERT INTO cat_producto_fotos (producto_id, gcs_key, orden) "
                    "VALUES (%s,%s, (SELECT t.m FROM (SELECT COALESCE(MAX(orden),0)+1 AS m "
                    "FROM cat_producto_fotos WHERE producto_id=%s) t))",
                    (producto_id, key, producto_id))
            except Exception as _e_ins:
                print(f"[_intentar_foto_ecommerce] INSERT falló pid={producto_id}, "
                      f"limpiando blob: {_e_ins}", flush=True)
                if _uploader_destroy:
                    try:
                        _uploader_destroy(key)
                    except Exception:
                        pass
                return "error"
            return "ok"
        except Exception as _e:
            print(f"[_intentar_foto_ecommerce] pid={producto_id} sku={sku}: {_e}", flush=True)
            return "error"

    # ─────────────────────────────────────────────────────────────────
    #  DESDE ERP (puntual) — 2026-07-12 (Daniel): en vez de sincronizar la
    #  bodega COMPLETA, buscar UN producto puntual (por SKU/documento/RUT
    #  vía el modal compartido _tka_modal.html en mode:'seleccionar') y
    #  agregarlo al catálogo ahí mismo. Idempotente: si el SKU ya existe en
    #  cat_productos, no se duplica -- se devuelve el id existente tal cual
    #  (Regla #4.2, aditivo: el botón "Sincronizar bodega desde ERP" sigue
    #  intacto, este es un camino alternativo, no un reemplazo).
    #
    #  2026-07-15 (Blueprint Cotizaciones Fase 1): la lógica de
    #  creación/reuso se factoriza a `_cat_crear_o_reusar_producto_desde_erp`
    #  para que otros módulos (tickets_module.py, cotizaciones) la reusen
    #  SIN duplicar código ni pegarle por HTTP a este mismo proceso. Se
    #  expone vía ctx (mismo patrón que tickets_module.py hace con
    #  `ctx['_tk_set_estado_automatico']`/`ctx['_tk_autopoll_correo']`):
    #  como register_catalogo_routes(app.py:69191) corre DESPUÉS de
    #  register_tickets_routes(app.py:69180), tickets_module.py NO puede
    #  capturar esta función en el top de su closure (aún no existiría en
    #  ese momento) -- debe leerla con ctx.get(...) en tiempo de REQUEST
    #  (dentro del handler), momento en el que el arranque ya terminó y
    #  ctx (globals() de app.py) ya la tiene.
    # ─────────────────────────────────────────────────────────────────
    def _cat_foto_ecommerce_background(producto_id, sku):
        """Resuelve la foto del ecommerce en un HILO DE FONDO (best-effort).
        2026-07-23 (fix "queda cargando"): _shopify_fotos_cache() hace un GET
        síncrono a ilusfitness.com paginando hasta 10 páginas -- con caché
        fría puede tardar ~100s y superar el gunicorn --timeout 90, matando
        el worker y dejando el request de cotizaciones colgado ("cargando y
        no me entrega nada"). La foto es puramente decorativa, así que se
        saca del hilo del request: el producto se crea al instante y la foto
        aparece sola segundos después. Daemon thread + app_context; jamás
        propaga (el catálogo nunca depende de esto)."""
        def _run():
            try:
                with app.app_context():
                    _intentar_foto_ecommerce(producto_id, sku)
            except Exception as _e_bg:
                print(f"[_cat_foto_ecommerce_background] pid={producto_id} sku={sku}: {_e_bg}", flush=True)
        try:
            threading.Thread(target=_run, daemon=True).start()
        except Exception as _e_th:
            print(f"[_cat_foto_ecommerce_background] no se pudo lanzar hilo: {_e_th}", flush=True)

    def _cat_crear_o_reusar_producto_desde_erp(sku, nombre="", familia=None, clase=None, traer_foto=True):
        """Crea (o reusa, idempotente por SKU) un producto de `cat_productos`
        a partir de datos traídos del ERP. Reusable desde cualquier módulo
        vía ctx['_cat_crear_o_reusar_producto_desde_erp'](sku, nombre).
        NUNCA lanza -- ante error de BD devuelve id=None + 'error'.
        Devuelve dict: {id, creado, nombre, sku, clase_producto, foto_ecommerce}.

        `clase` (opcional, 2026-07-22): si se pasa una clase válida y el SKU
        NO existía, nace YA clasificado (Daniel: "que se puedan guardar la
        clasificación conjunto con el producto"). Si el SKU YA existía se
        respeta su clase actual (NO se pisa) -- para cambiarla se usa el
        override explícito del modal / el PATCH del catálogo. Cierra el gap
        de que un SKU nuevo nacía siempre con clase NULL.

        `traer_foto` (2026-07-23): True (default) = foto del ecommerce
        SÍNCRONA (para la UI "+ Nuevo producto" del catálogo, donde el
        usuario espera y quiere ver la foto). False = foto en HILO DE FONDO
        (para Cotizaciones, donde el fetch a Shopify colgaba el request y
        Daniel veía "cargando y nada"). En ambos casos foto_ecommerce del
        retorno refleja solo el intento SÍNCRONO (en background va como
        best-effort silencioso).
        """
        try:
            sku_n = (str(sku) if sku is not None else "").strip().upper()
        except Exception:
            sku_n = ""
        if not sku_n:
            return {"id": None, "creado": False, "nombre": (nombre or ""), "sku": "",
                    "clase_producto": None, "foto_ecommerce": False, "error": "Falta el SKU"}
        nombre_n = (str(nombre) if nombre is not None else "").strip()
        familia_n = (str(familia) if familia is not None else "").strip()[:150] or None
        # Solo se acepta una clase que exista de verdad en el catálogo de
        # categorías (evita ensuciar cat_productos con slugs inventados).
        clase_n = (str(clase) if clase is not None else "").strip() or None
        if clase_n and clase_n not in _cat_clases_map():
            clase_n = None

        existente = mysql_fetchone(
            "SELECT id, nombre, clase_producto FROM cat_productos WHERE sku=%s", (sku_n,))
        if existente:
            return {"id": existente["id"], "creado": False, "nombre": existente["nombre"],
                     "sku": sku_n, "clase_producto": existente.get("clase_producto"),
                     "foto_ecommerce": False}

        if not nombre_n:
            nombre_n = sku_n  # el modal siempre trae nombre; esto es solo un resguardo
        user = current_username() or "sistema"
        try:
            mysql_execute(
                "INSERT INTO cat_productos (sku, nombre, familia, clase_producto, created_by, updated_by) "
                "VALUES (%s,%s,%s,%s,%s,%s)",
                (sku_n[:100], nombre_n[:300], familia_n, clase_n, user, user))
        except Exception as _e:
            msg = str(_e)
            if "Duplicate entry" in msg or "uq_cat_sku" in msg:
                # carrera: otro request lo creó justo antes -- lo tratamos como éxito
                row = mysql_fetchone(
                    "SELECT id, nombre, clase_producto FROM cat_productos WHERE sku=%s", (sku_n,))
                if row:
                    return {"id": row["id"], "creado": False, "nombre": row["nombre"],
                             "sku": sku_n, "clase_producto": row.get("clase_producto"),
                             "foto_ecommerce": False}
            print(f"[_cat_crear_o_reusar_producto_desde_erp] error sku={sku_n}: {_e}", flush=True)
            return {"id": None, "creado": False, "nombre": nombre_n, "sku": sku_n,
                     "clase_producto": None, "foto_ecommerce": False,
                     "error": "No se pudo crear el producto"}

        row = mysql_fetchone("SELECT id FROM cat_productos WHERE sku=%s", (sku_n,))
        nuevo_id = row["id"] if row else None

        # 2026-07-14 (Daniel: "tráelos automáticos y déjalo vacío si da
        # error"): al CREAR un producto nuevo, foto del ecommerce best-effort.
        # Si no hay match (marcas revendidas) o falla cualquier paso, el
        # producto queda sin foto y la creación NO se ve afectada.
        foto_ecom = False
        if nuevo_id:
            if traer_foto:
                # Camino síncrono (UI catálogo "+ Nuevo producto"): el usuario
                # espera y quiere la foto ahí mismo.
                try:
                    foto_ecom = _intentar_foto_ecommerce(nuevo_id, sku_n) == "ok"
                except Exception as _e_ecom:
                    print(f"[_cat_crear_o_reusar_producto_desde_erp] foto ecommerce sku={sku_n}: {_e_ecom}", flush=True)
            else:
                # Camino de Cotizaciones: foto en background para no colgar el
                # request con el fetch a Shopify (fix "queda cargando").
                _cat_foto_ecommerce_background(nuevo_id, sku_n)
        return {"id": nuevo_id, "creado": True, "nombre": nombre_n, "sku": sku_n,
                 "clase_producto": clase_n, "foto_ecommerce": foto_ecom}

    # Visible desde otros módulos (tickets_module.py — Cotizaciones Fase 1)
    # sin pegarle por HTTP a este mismo proceso. Ver comentario arriba.
    ctx["_cat_crear_o_reusar_producto_desde_erp"] = _cat_crear_o_reusar_producto_desde_erp

    def _cat_obtener_tarifa_clase(slug, tipo_servicio):
        """Hora/Técnicos de una categoría para un tipo_servicio. Reusable
        desde tickets_module.py vía ctx['_cat_obtener_tarifa_clase'](slug,
        tipo_servicio) -- mismo patrón que _cat_crear_o_reusar_producto_desde_erp
        (lookup en tiempo de REQUEST, no de arranque). Devuelve None si la
        categoría no existe, está inactiva, o no tiene tarifa cargada para
        ese tipo_servicio (ej. "Rack Pro" hoy, o cualquier categoría en un
        servicio cuya tabla Daniel aún no definió — 2026-07-21/2026-07-22).
        Ahora soporta los 5 tipos (instalacion/mantencion/visita_tecnica/
        venta_repuesto/otro) -- los que Daniel no cargó siguen dando None
        (=> $0 en el motor de precio, nunca un valor inventado)."""
        if not slug or tipo_servicio not in _CAT_TIPOS_SERVICIO_TARIFA:
            return None
        row = mysql_fetchone(
            "SELECT t.horas, t.tecnicos FROM cat_clase_producto_tarifas t "
            "JOIN cat_clases_producto c ON c.id=t.clase_id "
            "WHERE c.slug=%s AND c.activo=1 AND t.tipo_servicio=%s "
            "  AND t.horas IS NOT NULL AND t.tecnicos IS NOT NULL",
            (slug, tipo_servicio))
        if not row:
            return None
        return {"horas": float(row["horas"]), "tecnicos": int(row["tecnicos"])}

    ctx["_cat_obtener_tarifa_clase"] = _cat_obtener_tarifa_clase

    def _cat_tarifas_clases_batch(slugs, tipo_servicio):
        """Igual que _cat_obtener_tarifa_clase pero para MUCHAS clases en UNA
        sola query (escalabilidad 2026-07-22: recalcular/preview de una
        cotización de 200 ítems hacía 200 queries -- N+1). Devuelve
        {slug: {"horas","tecnicos"}} solo para las que tienen tarifa cargada
        y la categoría está activa; las demás no aparecen (=> el caller usa
        None => $0, mismo criterio que la versión de a uno)."""
        if not slugs or tipo_servicio not in _CAT_TIPOS_SERVICIO_TARIFA:
            return {}
        uniq = sorted({(s or "").strip() for s in slugs if s and str(s).strip()})
        if not uniq:
            return {}
        placeholders = ",".join(["%s"] * len(uniq))
        rows = mysql_fetchall(
            "SELECT c.slug, t.horas, t.tecnicos FROM cat_clase_producto_tarifas t "
            "JOIN cat_clases_producto c ON c.id=t.clase_id "
            f"WHERE c.activo=1 AND t.tipo_servicio=%s AND c.slug IN ({placeholders}) "
            "  AND t.horas IS NOT NULL AND t.tecnicos IS NOT NULL",
            tuple([tipo_servicio] + uniq)) or []
        return {r["slug"]: {"horas": float(r["horas"]), "tecnicos": int(r["tecnicos"])}
                for r in rows}

    ctx["_cat_tarifas_clases_batch"] = _cat_tarifas_clases_batch
    # Visible desde tickets_module.py (modal de revisión de Cotizaciones,
    # 2026-07-22) para traducir un slug de categoría a su nombre legible.
    ctx["_cat_clases_map"] = _cat_clases_map

    @app.route("/catalogo/api/productos/desde-erp", methods=["POST"])
    @_catalogo_admin_required
    def cat_api_producto_desde_erp():
        d = request.get_json(silent=True) or {}
        # 2026-07-12: si `sku`/`nombre`/`familia` llegan con un tipo no-string
        # (int/list/dict/bool -- ej. un caller directo distinto del modal, que
        # siempre manda string), (valor or "").strip() revienta con
        # AttributeError sin control -- 500 crudo, viola Regla #4. Se
        # sanea a string ANTES de tocarlos (hallazgo de la simulacion de
        # trafico 2026-07-12).
        try:
            sku_raw = d.get("sku")
            nombre_raw = d.get("nombre")
            familia_raw = d.get("familia")
            sku = (str(sku_raw) if sku_raw is not None else "").strip().upper()
            nombre = (str(nombre_raw) if nombre_raw is not None else "").strip()
            familia = (str(familia_raw) if familia_raw is not None else "").strip()[:150] or None
        except Exception:
            return jsonify({"ok": False, "error": "Datos inválidos"}), 400
        if not sku:
            return jsonify({"ok": False, "error": "Falta el SKU"}), 400

        res = _cat_crear_o_reusar_producto_desde_erp(sku, nombre, familia)
        if res.get("id") is None:
            return jsonify({"ok": False, "error": res.get("error") or "No se pudo crear el producto"}), 500
        return jsonify({"ok": True, "id": res["id"], "creado": res["creado"],
                         "nombre": res["nombre"], "foto_ecommerce": res["foto_ecommerce"]})

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
    #  BACKFILL FOTOS DESDE ECOMMERCE — 2026-07-14 (Daniel: "tráelos
    #  automáticos"). Recorre los productos activos SIN ninguna foto y les
    #  busca la imagen en la tienda (match exacto por SKU). El chequeo de
    #  match es un lookup en dict (barato, se hace para TODOS); lo caro
    #  (descargar + subir a GCS) se acota por request con un tope de
    #  subidas + presupuesto de tiempo, para no chocar con el timeout de
    #  gunicorn (--timeout 90 en el Dockerfile). Si quedan matches
    #  pendientes se informa `restantes` y se vuelve a presionar el botón.
    # ─────────────────────────────────────────────────────────────────
    @app.route("/catalogo/api/fotos-desde-ecommerce", methods=["POST"])
    @_catalogo_admin_required
    def cat_api_fotos_desde_ecommerce():
        d = request.get_json(silent=True) or {}
        try:
            tope = int(d.get("limit") or 100)
        except Exception:
            tope = 100
        tope = max(1, min(200, tope))

        rows = mysql_fetchall(
            "SELECT p.id, p.sku FROM cat_productos p "
            "WHERE p.activo=1 AND NOT EXISTS "
            "  (SELECT 1 FROM cat_producto_fotos f WHERE f.producto_id=p.id) "
            "ORDER BY p.id") or []

        fotos_map = _shopify_fotos_cache()
        if not fotos_map:
            return jsonify({
                "ok": False,
                "error": "No se pudo leer el catálogo de la tienda (intenta de nuevo en unos minutos)",
                "error_codigo": "ECOMMERCE_NO_DISPONIBLE",
            }), 502

        con_foto = sin_match = errores = restantes = 0
        presupuesto_s = 60  # margen holgado bajo el --timeout 90 de gunicorn
        t0 = time.monotonic()
        agotado = False
        for r in rows:
            sku = (r.get("sku") or "").strip().upper()
            if sku not in fotos_map:
                sin_match += 1
                continue
            if agotado or (con_foto + errores) >= tope \
                    or (time.monotonic() - t0) > presupuesto_s:
                agotado = True
                restantes += 1
                continue
            estado = _intentar_foto_ecommerce(r["id"], sku, fotos_map=fotos_map)
            if estado == "ok":
                con_foto += 1
            elif estado == "sin_match":
                sin_match += 1
            else:
                errores += 1

        return jsonify({
            "ok": True,
            "con_foto": con_foto,
            "sin_match": sin_match,
            "errores": errores,
            "restantes": restantes,
            "total_sin_foto": len(rows),
        })

    # ─────────────────────────────────────────────────────────────────
    #  BUSQUEDA APROXIMADA EN BODEGA (2026-07-13, Daniel): pestaña "Bodega
    #  02" del modal ERP compartido, solo para Catalogo. Reusa la MISMA
    #  bodega/exclusion ZZ de _cat_sync_erp_nuevos, pero en vivo (sin crear
    #  productos, solo para elegir cual agregar). Regla #4.1: SOLO LECTURA
    #  via _random_sql_query.
    # ─────────────────────────────────────────────────────────────────
    @app.route("/catalogo/api/erp/bodega-buscar", methods=["GET"])
    @_catalogo_required
    def cat_api_erp_bodega_buscar():
        # 2026-07-23: era _catalogo_admin_required (solo superadmin) -- pero
        # es una búsqueda de SOLO LECTURA que técnico/ejecutivo necesitan
        # para "Agregar de Bodega 02" desde el wizard de Cotizaciones (ya
        # habilitado hoy a escribir en el catálogo). Sin este cambio, ese
        # botón les tiraba 403 antes de siquiera poder buscar.
        q = (request.args.get("q") or "").strip()
        if len(q) < 2:
            return jsonify({"ok": True, "items": []})
        if not _random_sql_query:
            return jsonify({"ok": False, "error": "Catálogo ERP no disponible"}), 503
        q_like = f"%{q.upper()[:60]}%"
        try:
            rows = _random_sql_query(
                """
                SELECT DISTINCT TOP 30
                       LTRIM(RTRIM(pr.KOPR))   AS sku,
                       LTRIM(RTRIM(pr.NOKOPR)) AS nombre,
                       pr.STFI1                AS stock
                  FROM MAEPR pr
                 WHERE EXISTS (SELECT 1 FROM MAEST st
                                WHERE LTRIM(RTRIM(st.KOPR))=LTRIM(RTRIM(pr.KOPR))
                                  AND LTRIM(RTRIM(st.KOBO))=%s)
                   AND (UPPER(pr.NOKOPR) LIKE %s OR UPPER(pr.KOPR) LIKE %s)
                   AND UPPER(LTRIM(RTRIM(pr.KOPR))) NOT LIKE %s
                 ORDER BY nombre
                """,
                (CAT_BODEGA_SYNC, q_like, q_like, "ZZ%"), max_rows=30,
            ) or []
        except Exception as _e:
            print(f"[cat_api_erp_bodega_buscar] error ERP (bodega={CAT_BODEGA_SYNC}): {_e}", flush=True)
            return jsonify({"ok": False, "error": "No se pudo buscar en el ERP"}), 502

        def _stock_num(v):
            try:
                return float(v or 0)
            except Exception:
                return 0.0

        items = [{
            "sku": (r.get("sku") or "").strip(),
            "nombre": (r.get("nombre") or "").strip(),
            "stock": _stock_num(r.get("stock")),
        } for r in rows if (r.get("sku") or "").strip() and (r.get("nombre") or "").strip()]

        # 2026-07-23 (Daniel: "que se vea colorido... que venda"): enriquece
        # con lo que YA existe en el catálogo (clase + primera foto), para
        # que el buscador de Bodega 02 se vea vivo -- no solo texto plano --
        # y de paso conecta con el pedido de "ver los productos que ya tengo
        # gestionado". Un solo query batch por SKU (no N+1); ante cualquier
        # error se degrada a la lista simple (nunca rompe la búsqueda).
        if items:
            try:
                skus = [it["sku"].upper() for it in items]
                ph = ",".join(["%s"] * len(skus))
                cat_rows = mysql_fetchall(
                    f"""
                    SELECT p.sku, p.clase_producto,
                           (SELECT f.gcs_key FROM cat_producto_fotos f
                              WHERE f.producto_id=p.id ORDER BY f.orden LIMIT 1) AS foto_key
                    FROM cat_productos p WHERE p.sku IN ({ph})
                    """,
                    tuple(skus)) or []
                _cat_map2 = _cat_clases_map()
                por_sku = {row["sku"].upper(): row for row in cat_rows}
                for it in items:
                    cat_row = por_sku.get(it["sku"].upper())
                    it["en_catalogo"] = bool(cat_row)
                    it["clase_producto_label"] = (
                        _cat_map2.get(cat_row.get("clase_producto") or "") if cat_row else None)
                    _fkey = cat_row.get("foto_key") if cat_row else None
                    it["foto_url"] = ("/f/" + _fkey) if _fkey else None
            except Exception as _e_enr:
                print(f"[cat_api_erp_bodega_buscar] enriquecimiento catálogo: {_e_enr}", flush=True)
                for it in items:
                    it.setdefault("en_catalogo", False)
                    it.setdefault("clase_producto_label", None)
                    it.setdefault("foto_url", None)
        return jsonify({"ok": True, "items": items})

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
        # 2026-07-23 (blueprint piolas/manuales, Daniel: "se puede agregar
        # una copia" al enviar un manual): CC opcional, mismo validador que
        # el destinatario principal.
        cc = (d.get("cc") or "").strip() or None
        if cc and validar_email:
            ok_cc, val_or_err_cc = validar_email(cc)
            if not ok_cc:
                return jsonify({"ok": False, "error": "Correo de copia (CC) inválido: " + (val_or_err_cc or "")}), 400
            cc = val_or_err_cc

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
        # 2026-07-23 (Comunicaciones: modulo "catalogo"): si hay plantilla
        # editable activa para modulo='catalogo' estado='manual_envio', se
        # usa esa (asunto y cuerpo) en vez del hardcodeado de arriba. El
        # bloque de arriba queda como FALLBACK si no hay plantilla o esta
        # apagada -- nunca se omite el envio (Regla: degradar, no omitir).
        if _render_comm_template:
            try:
                _tpl = _render_comm_template(
                    "manual_envio", "email",
                    {"sku": p.get("sku") or "", "producto": nombre_producto,
                     "manual_nombre": p.get("manual_pdf_nombre") or "",
                     "mensaje": msg_html},
                    modulo="catalogo")
                if _tpl:
                    _asu, _cue = _tpl
                    if (_asu or "").strip():
                        subject = _asu.strip()
                    if (_cue or "").strip():
                        body_html = _cue
            except Exception as _e_tpl:
                print(f"[cat_manual_enviar_correo] plantilla no usada pid={pid}: {_e_tpl}", flush=True)
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
                evento="catalogo_manual", modulo="catalogo", cc=cc,
                attachments=[{"filename": manual_nombre, "content": pdf_bytes, "content_type": "application/pdf"}],
            )
        except Exception as _e:
            print(f"[cat_manual_enviar_correo] error envío pid={pid}: {_e}", flush=True)
            enviado = False

        if _audit:
            _audit("cat_manual_enviado", target_type="cat_producto", target_id=pid,
                   details={"email": email, "cc": cc, "sku": p.get("sku"), "manual_nombre": manual_nombre,
                             "enviado": bool(enviado)})

        if not enviado:
            return jsonify({
                "ok": False,
                "error": "No se pudo enviar el correo (revisa el correo de destino o el estado del canal de email)",
            }), 502
        return jsonify({"ok": True})

    # ─────────────────────────────────────────────────────────────────
    #  MANUALES (multi) — enviar por correo UNO especifico de los hasta 5
    #  de cat_producto_manuales (2026-07-21). Mismo gate, mismo payload
    #  {email, mensaje} y mismo mecanismo real de envio (helpers
    #  _brand_subject/_ilus_email_master/_send_ilus_email) que el legado
    #  cat_api_manual_enviar_correo de arriba -- solo cambia de donde sale
    #  el PDF: la fila puntual de cat_producto_manuales en vez de la
    #  columna singular manual_pdf_key. Doble filtro id+producto_id, mismo
    #  patron que cat_api_manuales_delete/cat_api_manuales_descargar (mas
    #  arriba) para que un manual_id de OTRO producto nunca calce.
    # ─────────────────────────────────────────────────────────────────
    @app.route("/catalogo/api/productos/<int:pid>/manuales/<int:manual_id>/enviar-correo", methods=["POST"])
    @_catalogo_required
    def cat_api_manuales_multi_enviar_correo(pid, manual_id):
        m = mysql_fetchone(
            "SELECT mm.gcs_key, mm.nombre_archivo, p.sku, p.nombre AS producto_nombre "
            "FROM cat_producto_manuales mm JOIN cat_productos p ON p.id = mm.producto_id "
            "WHERE mm.id=%s AND mm.producto_id=%s",
            (manual_id, pid))
        if not m:
            return jsonify({"ok": False, "error": "Manual no encontrado"}), 404

        d = request.get_json(silent=True) or {}
        email = (d.get("email") or "").strip()
        mensaje = (d.get("mensaje") or "").strip()[:1000] or None
        cc = (d.get("cc") or "").strip() or None
        if cc and validar_email:
            ok_cc, val_or_err_cc = validar_email(cc)
            if not ok_cc:
                return jsonify({"ok": False, "error": "Correo de copia (CC) inválido: " + (val_or_err_cc or "")}), 400
            cc = val_or_err_cc

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
            pdf_bytes = b.blob(m["gcs_key"]).download_as_bytes()
        except Exception as _e:
            print(f"[cat_manuales_multi_enviar_correo] error lectura GCS pid={pid} manual={manual_id}: {_e}", flush=True)
            return jsonify({"ok": False, "error": "No se pudo leer el manual"}), 500

        nombre_producto = m.get("producto_nombre") or m.get("sku") or "producto"
        subject = _brand_subject(f"Manual — {nombre_producto}") if _brand_subject else f"Manual — {nombre_producto}"

        from markupsafe import escape as _esc
        msg_html = f"<p style=\"margin:0 0 12px\">{_esc(mensaje)}</p>" if mensaje else ""
        body_html = (
            f"<p style=\"margin:0 0 12px;font-size:15px;line-height:24px;color:#454b54\">"
            f"Adjunto encontrarás el manual del producto "
            f"<strong>{_esc(m.get('sku') or '')}</strong> — {_esc(nombre_producto)}.</p>"
            f"{msg_html}"
        )
        # 2026-07-23 (Comunicaciones: modulo "catalogo"): si hay plantilla
        # editable activa para modulo='catalogo' estado='manual_envio', se
        # usa esa (asunto y cuerpo) en vez del hardcodeado de arriba. El
        # bloque de arriba queda como FALLBACK si no hay plantilla o esta
        # apagada -- nunca se omite el envio (Regla: degradar, no omitir).
        if _render_comm_template:
            try:
                _tpl = _render_comm_template(
                    "manual_envio", "email",
                    {"sku": m.get("sku") or "", "producto": nombre_producto,
                     "manual_nombre": m.get("nombre_archivo") or "",
                     "mensaje": msg_html},
                    modulo="catalogo")
                if _tpl:
                    _asu, _cue = _tpl
                    if (_asu or "").strip():
                        subject = _asu.strip()
                    if (_cue or "").strip():
                        body_html = _cue
            except Exception as _e_tpl:
                print(f"[cat_manuales_multi_enviar_correo] plantilla no usada pid={pid} manual={manual_id}: {_e_tpl}", flush=True)
        if _ilus_email_master:
            html = _ilus_email_master({
                "subject": subject,
                "title": "Manual de producto",
                "subtitle": f"{m.get('sku') or ''} · {nombre_producto}",
                "body_html": body_html,
                "support_email": ILUS_SOPORTE_EMAIL,
            })
        else:
            html = f"<html><body>{body_html}</body></html>"

        manual_nombre = m.get("nombre_archivo") or f"{m.get('sku') or 'manual'}.pdf"
        if not _send_ilus_email:
            return jsonify({"ok": False, "error": "No se pudo enviar el correo (canal de email no disponible)"}), 502
        try:
            enviado = _send_ilus_email(
                email, subject, html,
                evento="catalogo_manual", modulo="catalogo", cc=cc,
                attachments=[{"filename": manual_nombre, "content": pdf_bytes, "content_type": "application/pdf"}],
            )
        except Exception as _e:
            print(f"[cat_manuales_multi_enviar_correo] error envío pid={pid} manual={manual_id}: {_e}", flush=True)
            enviado = False

        if _audit:
            _audit("cat_manual_multi_enviado", target_type="cat_producto_manual", target_id=manual_id,
                   details={"email": email, "cc": cc, "sku": m.get("sku"), "producto_id": pid,
                             "manual_nombre": manual_nombre, "enviado": bool(enviado)})

        if not enviado:
            return jsonify({
                "ok": False,
                "error": "No se pudo enviar el correo (revisa el correo de destino o el estado del canal de email)",
            }), 502
        return jsonify({"ok": True})

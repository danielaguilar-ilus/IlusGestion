"""
Configuración central de ILUS Etiquetas.

POLÍTICA DE SECRETOS
====================
Todas las credenciales vienen EXCLUSIVAMENTE de variables de entorno.
No hay fallbacks con valores reales en este archivo.

DEPLOY EN RAILWAY
=================
Configurar en Railway → Settings → Variables:

  Requeridas para arrancar:
    FLASK_SECRET_KEY      = <string aleatorio, mínimo 48 chars>
    MYSQL_HOST            = <host Clever Cloud>
    MYSQL_PORT            = 21260
    MYSQL_USER            = <usuario>
    MYSQL_PASSWORD        = <password>
    MYSQL_DATABASE        = <nombre base>

  Requeridas para ERP Random:
    ERP_MYSQL_HOST        = cloud.random.cl
    ERP_MYSQL_PORT        = 8058
    ERP_MYSQL_USER        = <usuario>
    ERP_MYSQL_PASSWORD    = <password>
    ERP_MYSQL_DATABASE    = <base>
    ERP_API_URL           = https://lab.random.cl/ilus
    ERP_API_TOKEN         = <JWT>

  Opcionales (servicios caen graceful si faltan):
    ANTHROPIC_API_KEY     = <sk-ant-...>
    CLOUDINARY_CLOUD_NAME, CLOUDINARY_API_KEY, CLOUDINARY_API_SECRET
    SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASSWORD
    SMTP_FROM_NAME, SMTP_FROM_ADDR

DESARROLLO LOCAL
================
Copiar `.env.example` a `.env` y rellenar valores. NO commitear `.env`.
"""
import os


def _env(name, default=""):
    """Lee env var. Devuelve default si no existe o está vacía."""
    v = os.environ.get(name)
    return v if (v is not None and v != "") else default


def _env_first(*names, default=""):
    """Lee la primera env var no vacía de la lista de nombres dada.
    Útil para soportar nombres alternativos sin obligar a renombrar
    variables existentes en producción.
    Ej: _env_first("ERP_MYSQL_HOST", "RANDOM_SQL_HOST")
    """
    for n in names:
        v = os.environ.get(n)
        if v is not None and v != "":
            return v
    return default


# ─────────────────────────────────────────────
#  Base de datos principal (Clever Cloud — MySQL)
#  Si falta MYSQL_HOST: el sistema imprime warning al boot y el primer
#  query lanza una excepción clara (no intenta conectar a localhost).
# ─────────────────────────────────────────────
MYSQL_CONFIG = {
    'host':     _env('MYSQL_HOST'),
    'port':     int(_env('MYSQL_PORT', '3306')),
    'user':     _env('MYSQL_USER'),
    'password': _env('MYSQL_PASSWORD'),
    'database': _env('MYSQL_DATABASE'),
    'table':    'etiquetas',
    'users_table':    'app_users',
    'products_table': 'app_products',
    'bultos_table':   'app_bultos',
    'photos_table':   'app_photos',
    'connect_timeout': 15,
}

# ─────────────────────────────────────────────
#  ERP externo (cloud.random.cl) — SOLO LECTURA
#  Acepta DOS esquemas de naming (compat con deploys existentes):
#    Nuevo:  ERP_MYSQL_HOST/USER/PASSWORD/DATABASE/PORT
#    Viejo:  RANDOM_SQL_HOST/USER/PASS/DB/PORT (Railway producción)
#  El primero que esté definido gana.
# ─────────────────────────────────────────────
ERP_CONFIG = {
    'host':            _env_first('ERP_MYSQL_HOST',     'RANDOM_SQL_HOST'),
    'port':            int(_env_first('ERP_MYSQL_PORT', 'RANDOM_SQL_PORT', default='8058')),
    'user':            _env_first('ERP_MYSQL_USER',     'RANDOM_SQL_USER'),
    'password':        _env_first('ERP_MYSQL_PASSWORD', 'RANDOM_SQL_PASS', 'RANDOM_SQL_PASSWORD'),
    'database':        _env_first('ERP_MYSQL_DATABASE', 'RANDOM_SQL_DB',   'RANDOM_SQL_DATABASE'),
    'table_products':  'MAEPR',
    'connect_timeout': 8,
    'api_url':         _env('ERP_API_URL'),
    'api_token':       _env('ERP_API_TOKEN'),
}

MAX_BULTOS = 27

# ─────────────────────────────────────────────
#  IA — Anthropic Claude (opcional)
# ─────────────────────────────────────────────
ANTHROPIC_API_KEY = _env("ANTHROPIC_API_KEY", "")

# ─────────────────────────────────────────────
#  Cloudinary (opcional — cae a filesystem si falta)
# ─────────────────────────────────────────────
CLOUDINARY_CONFIG = {
    "cloud_name": _env('CLOUDINARY_CLOUD_NAME'),
    "api_key":    _env('CLOUDINARY_API_KEY'),
    "api_secret": _env('CLOUDINARY_API_SECRET'),
}

# ─────────────────────────────────────────────
#  Email SMTP (opcional — envío falla con warning si falta)
# ─────────────────────────────────────────────
EMAIL_CONFIG = {
    "smtp_host":  _env('SMTP_HOST', 'smtp.gmail.com'),
    "smtp_port":  int(_env('SMTP_PORT', '587')),
    "smtp_user":  _env('SMTP_USER'),
    "smtp_pass":  _env('SMTP_PASSWORD'),
    "from_name":  _env('SMTP_FROM_NAME', 'ILUS Sport & Health'),
    "from_addr":  _env('SMTP_FROM_ADDR'),
}


def _diagnose_env_status():
    """Reporta qué env vars críticas faltan al boot.
    Para ERP_MYSQL_* acepta los aliases RANDOM_SQL_* (compat producción).
    """
    required = [
        ("FLASK_SECRET_KEY",            os.environ.get("FLASK_SECRET_KEY")),
        ("MYSQL_HOST",                  MYSQL_CONFIG['host']),
        ("MYSQL_USER",                  MYSQL_CONFIG['user']),
        ("MYSQL_PASSWORD",              MYSQL_CONFIG['password']),
        ("MYSQL_DATABASE",              MYSQL_CONFIG['database']),
        ("ERP_MYSQL_HOST/RANDOM_SQL_HOST",         ERP_CONFIG['host']),
        ("ERP_MYSQL_USER/RANDOM_SQL_USER",         ERP_CONFIG['user']),
        ("ERP_MYSQL_PASSWORD/RANDOM_SQL_PASS",     ERP_CONFIG['password']),
        ("ERP_API_URL",                            ERP_CONFIG['api_url']),
        ("ERP_API_TOKEN",                          ERP_CONFIG['api_token']),
    ]
    optional = [
        ("ANTHROPIC_API_KEY",      ANTHROPIC_API_KEY),
        ("CLOUDINARY_CLOUD_NAME",  CLOUDINARY_CONFIG['cloud_name']),
        ("CLOUDINARY_API_KEY",     CLOUDINARY_CONFIG['api_key']),
        ("CLOUDINARY_API_SECRET",  CLOUDINARY_CONFIG['api_secret']),
        ("SMTP_USER",              EMAIL_CONFIG['smtp_user']),
        ("SMTP_PASSWORD",          EMAIL_CONFIG['smtp_pass']),
    ]
    req_missing = [name for name, val in required if not val]
    opt_missing = [name for name, val in optional if not val]
    return req_missing, opt_missing


# Reporte al boot — log claro de configuración
_req_missing, _opt_missing = _diagnose_env_status()
if _req_missing:
    print("=" * 70, flush=True)
    print("[CONFIG] ❌ FALTAN ENV VARS CRÍTICAS — el sistema no funcionará:", flush=True)
    for name in _req_missing:
        print(f"  ❌ {name}", flush=True)
    print("", flush=True)
    print("[CONFIG] Configúralas en Railway → Settings → Variables.", flush=True)
    print("[CONFIG] Sin estas variables, las consultas a la base de datos fallarán.", flush=True)
    print("=" * 70, flush=True)
else:
    print("[CONFIG] ✅ Todas las env vars críticas están configuradas.", flush=True)

if _opt_missing:
    print(f"[CONFIG] ⚠ Env vars opcionales faltantes (funciones reducidas): "
          + ", ".join(_opt_missing), flush=True)


def assert_mysql_configured():
    """Llamar antes de cualquier query MySQL. Lanza RuntimeError con mensaje
    explícito si falta configuración (mucho mejor que "Can't connect to localhost").
    """
    missing = []
    if not MYSQL_CONFIG['host']:     missing.append("MYSQL_HOST")
    if not MYSQL_CONFIG['user']:     missing.append("MYSQL_USER")
    if not MYSQL_CONFIG['password']: missing.append("MYSQL_PASSWORD")
    if not MYSQL_CONFIG['database']: missing.append("MYSQL_DATABASE")
    if missing:
        raise RuntimeError(
            "Configuración MySQL incompleta. Faltan variables de entorno: "
            + ", ".join(missing) +
            ". Configúralas en Railway → Settings → Variables y reinicia el servicio."
        )

"""
Configuración central de ILUS Etiquetas.

POLÍTICA DE SECRETOS
====================
- TODO secreto (passwords, API keys, tokens) DEBE venir por variable de entorno.
- Para deploy en Railway: configurar cada env var en Settings → Variables.
- Para desarrollo local: copiar `.env.example` a `.env` y rellenar valores.
- Este archivo NO debe contener credenciales reales en los fallbacks.

Si una env var crítica falta:
  * Las credenciales de DB lanzan error claro en el primer query (no se
    silencia el problema en el código).
  * Los servicios opcionales (Anthropic, Cloudinary, SMTP) se desactivan
    automáticamente cuando no hay clave configurada, en vez de exponer
    valores hardcodeados.
"""
import os


def _env(name, default=""):
    """Lee env var. Devuelve default si no existe o está vacía."""
    v = os.environ.get(name)
    return v if (v is not None and v != "") else default


def _require_env(name):
    """Lee env var. Devuelve "" si no existe — los modulos que la usen
    deben validar y rendir un error claro al primer uso."""
    return os.environ.get(name, "")


# ─────────────────────────────────────────────
#  Base de datos principal (Clever Cloud — MySQL)
#  Aquí viven TODAS las tablas de la aplicación.
#
#  IMPORTANTE: las credenciales DEBEN venir por env vars en producción.
#  Si MYSQL_HOST está vacío, el código fallará al primer query con un
#  mensaje claro — eso es preferible a usar fallbacks con credenciales
#  reales expuestas en el repo.
#
#  Para desarrollo local, definir en .env (NO commitear):
#    MYSQL_HOST=...
#    MYSQL_USER=...
#    MYSQL_PASSWORD=...
#    MYSQL_DATABASE=...
# ─────────────────────────────────────────────
MYSQL_CONFIG = {
    'host':     _require_env('MYSQL_HOST'),
    'port':     int(_env('MYSQL_PORT', '3306')),
    'user':     _require_env('MYSQL_USER'),
    'password': _require_env('MYSQL_PASSWORD'),
    'database': _require_env('MYSQL_DATABASE'),
    'table':    'etiquetas',
    'users_table':    'app_users',
    'products_table': 'app_products',
    'bultos_table':   'app_bultos',
    'photos_table':   'app_photos',
    'connect_timeout': 15,
}

# ─────────────────────────────────────────────
#  ERP externo (cloud.random.cl) — SOLO LECTURA
# ─────────────────────────────────────────────
ERP_CONFIG = {
    'host':            _require_env('ERP_MYSQL_HOST'),
    'port':            int(_env('ERP_MYSQL_PORT', '8058')),
    'user':            _require_env('ERP_MYSQL_USER'),
    'password':        _require_env('ERP_MYSQL_PASSWORD'),
    'database':        _require_env('ERP_MYSQL_DATABASE'),
    'table_products':  'MAEPR',
    'connect_timeout': 8,
    # API REST del ERP
    'api_url':   _require_env('ERP_API_URL'),
    'api_token': _require_env('ERP_API_TOKEN'),
}

MAX_BULTOS = 27

# ─────────────────────────────────────────────
#  IA — Anthropic Claude (análisis de contratos)
#  Si no está configurada, el módulo IA se desactiva graceful.
# ─────────────────────────────────────────────
ANTHROPIC_API_KEY = _env("ANTHROPIC_API_KEY", "")

# ─────────────────────────────────────────────
#  Cloudinary — almacenamiento persistente de fotos/documentos
#  Si no está configurada, el sistema cae a filesystem (no persiste
#  entre deploys en Railway).
# ─────────────────────────────────────────────
CLOUDINARY_CONFIG = {
    "cloud_name": _env('CLOUDINARY_CLOUD_NAME', ''),
    "api_key":    _env('CLOUDINARY_API_KEY',    ''),
    "api_secret": _env('CLOUDINARY_API_SECRET', ''),
}

# ─────────────────────────────────────────────
#  Email (recuperación de contraseña, alertas)
#  Si no está configurado, el envío de emails falla silenciosamente
#  con un log warning — no rompe el resto del sistema.
# ─────────────────────────────────────────────
EMAIL_CONFIG = {
    "smtp_host":  _env('SMTP_HOST', 'smtp.gmail.com'),
    "smtp_port":  int(_env('SMTP_PORT', '587')),
    "smtp_user":  _env('SMTP_USER',     ''),
    "smtp_pass":  _env('SMTP_PASSWORD', ''),
    "from_name":  _env('SMTP_FROM_NAME','ILUS Sport & Health'),
    "from_addr":  _env('SMTP_FROM_ADDR',''),
}


def _diagnose_missing():
    """Lista cuáles env vars críticas están faltando.
    Útil para llamar al boot y avisar en logs sin romper la app.
    """
    missing = []
    for label, val in [
        ("MYSQL_HOST",         MYSQL_CONFIG['host']),
        ("MYSQL_USER",         MYSQL_CONFIG['user']),
        ("MYSQL_PASSWORD",     MYSQL_CONFIG['password']),
        ("MYSQL_DATABASE",     MYSQL_CONFIG['database']),
        ("ERP_MYSQL_HOST",     ERP_CONFIG['host']),
        ("ERP_MYSQL_USER",     ERP_CONFIG['user']),
        ("ERP_MYSQL_PASSWORD", ERP_CONFIG['password']),
        ("ERP_API_URL",        ERP_CONFIG['api_url']),
        ("ERP_API_TOKEN",      ERP_CONFIG['api_token']),
    ]:
        if not val:
            missing.append(label)
    return missing


# Diagnóstico al importar el módulo — solo log, no rompe nada
_missing = _diagnose_missing()
if _missing:
    print(
        "[CONFIG] ⚠ Faltan env vars críticas: " + ", ".join(_missing) +
        ". El sistema arranca pero funciones que requieren estos servicios "
        "fallarán. Configúralas en Railway → Variables.",
        flush=True
    )

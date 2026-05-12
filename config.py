import os


def _env(name, default=""):
    """Lee env var. Devuelve default si no existe o está vacía."""
    v = os.environ.get(name)
    return v if (v is not None and v != "") else default


# ─────────────────────────────────────────────
#  Base de datos principal (Clever Cloud — MySQL)
#  Aquí viven TODAS las tablas de la aplicación.
#
#  Credenciales por env var (Railway/local .env). Si la env var no está
#  definida, hace fallback al valor de respaldo para que el sistema
#  siga funcionando — pero EN PRODUCCIÓN se debe configurar la env var
#  y rotar la credencial de respaldo.
# ─────────────────────────────────────────────
MYSQL_CONFIG = {
    'host':     _env('MYSQL_HOST',     'bexkaglyixctbjojgg24-mysql.services.clever-cloud.com'),
    'port':     int(_env('MYSQL_PORT', '21260')),
    'user':     _env('MYSQL_USER',     'ufib6bn6qfhgwpuf'),
    'password': _env('MYSQL_PASSWORD', '2Lg3NxEY9EIqE9zX4MY'),
    'database': _env('MYSQL_DATABASE', 'bexkaglyixctbjojgg24'),
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
    'host':            _env('ERP_MYSQL_HOST',     'cloud.random.cl'),
    'port':            int(_env('ERP_MYSQL_PORT', '8058')),
    'user':            _env('ERP_MYSQL_USER',     'usr_sport'),
    'password':        _env('ERP_MYSQL_PASSWORD', 'h34ltsp0rt!!'),
    'database':        _env('ERP_MYSQL_DATABASE', 'rd095bd01'),
    'table_products':  'MAEPR',
    'connect_timeout': 8,
    'api_url':   _env('ERP_API_URL',   'https://lab.random.cl/ilus'),
    'api_token': _env('ERP_API_TOKEN', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IkZFRjk5RjYzLTRGRTktRUIxMS05NDA2LUNBRDVEMTQ4ODkxMiIsInVzZXJuYW1lIjoiYWRtaW5AcmFuZG9tLmNsIiwiZXhwIjoyMDczOTI2Njk3LCJpYXQiOjE3NTg1NjY2OTd9.Yfiy1GgtdSTweUjPPBSr0k1bVnxxMu3DOjxW6arjmuY'),
}

MAX_BULTOS = 27

# ─────────────────────────────────────────────
#  IA — Anthropic Claude (análisis de contratos)
# ─────────────────────────────────────────────
ANTHROPIC_API_KEY = _env("ANTHROPIC_API_KEY", "")

# ─────────────────────────────────────────────
#  Cloudinary — almacenamiento de fotos en la nube
# ─────────────────────────────────────────────
CLOUDINARY_CONFIG = {
    "cloud_name": _env('CLOUDINARY_CLOUD_NAME', 'dbhlvyri8'),
    "api_key":    _env('CLOUDINARY_API_KEY',    '987733531454622'),
    "api_secret": _env('CLOUDINARY_API_SECRET', '1RpvVaUqMukNS84oorEVCtHwTQk'),
}

# ─────────────────────────────────────────────
#  Email (recuperación de contraseña)
# ─────────────────────────────────────────────
EMAIL_CONFIG = {
    "smtp_host":  _env('SMTP_HOST', 'smtp.gmail.com'),
    "smtp_port":  int(_env('SMTP_PORT', '587')),
    "smtp_user":  _env('SMTP_USER',     'daniel.aguilar@sphs.cl'),
    "smtp_pass":  _env('SMTP_PASSWORD', '19109364Daniel'),
    "from_name":  _env('SMTP_FROM_NAME','ILUS Sport & Health'),
    "from_addr":  _env('SMTP_FROM_ADDR','daniel.aguilar@sphs.cl'),
}

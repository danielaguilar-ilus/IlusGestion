# ─────────────────────────────────────────────
#  Base de datos principal (Clever Cloud — MySQL)
#  Aquí viven TODAS las tablas de la aplicación.
# ─────────────────────────────────────────────
MYSQL_CONFIG = {
    'host':     'bexkaglyixctbjojgg24-mysql.services.clever-cloud.com',
    'port':     21260,
    'user':     'ufib6bn6qfhgwpuf',
    'password': '2Lg3NxEY9EIqE9zX4MY',
    'database': 'bexkaglyixctbjojgg24',
    'table':    'etiquetas',              # Tabla legada ERP en Clever Cloud (solo lectura para listado)
    'users_table':    'app_users',
    'products_table': 'app_products',
    'bultos_table':   'app_bultos',
    'photos_table':   'app_photos',
    'connect_timeout': 15,
}

# ─────────────────────────────────────────────
#  ERP externo (cloud.random.cl) — SOLO LECTURA
#  NUNCA se escribe en estas tablas.
# ─────────────────────────────────────────────
ERP_CONFIG = {
    'host':            'cloud.random.cl',
    'port':            8058,
    'user':            'usr_sport',
    'password':        'h34ltsp0rt!!',
    'database':        'rd095bd01',
    'table_products':  'MAEPR',           # KOPR=SKU, NOKOPR=Nombre
    'connect_timeout': 8,
}

MAX_BULTOS = 27

# ─────────────────────────────────────────────
#  Email (recuperación de contraseña)
#  Ajusta SMTP_USER y SMTP_PASS con tu cuenta.
# ─────────────────────────────────────────────
EMAIL_CONFIG = {
    "smtp_host":  "smtp.gmail.com",
    "smtp_port":  587,
    "smtp_user":  "daniel.aguilar@sphs.cl",   # ← tu correo remitente
    "smtp_pass":  "19109364Daniel",            # ← contraseña de correo
    "from_name":  "ILUS Sport & Health",
    "from_addr":  "daniel.aguilar@sphs.cl",
}

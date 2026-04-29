# -*- coding: utf-8 -*-
"""Script de importacion inicial -- ejecutar una sola vez."""
import io, sys
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
import sqlite3, sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import MYSQL_CONFIG, MAX_BULTOS
import pymysql, pymysql.cursors

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'etiquetas.db')

def to_f(v):
    try:
        return float(str(v or '0').replace(',', '.'))
    except Exception:
        return 0.0

# Inicializar tablas
conn_s = sqlite3.connect(DB_PATH)
conn_s.execute("PRAGMA foreign_keys = ON")
conn_s.executescript("""
    CREATE TABLE IF NOT EXISTS products (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sku TEXT UNIQUE NOT NULL,
        nombre TEXT NOT NULL,
        estado TEXT DEFAULT 'Pendiente',
        codigo TEXT DEFAULT '',
        erp_sync INTEGER DEFAULT 0,
        created_at TEXT DEFAULT (datetime('now','localtime')),
        updated_at TEXT DEFAULT (datetime('now','localtime'))
    );
    CREATE TABLE IF NOT EXISTS bultos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id INTEGER NOT NULL REFERENCES products(id) ON DELETE CASCADE,
        bulto_num INTEGER NOT NULL,
        largo REAL DEFAULT 0, ancho REAL DEFAULT 0,
        alto REAL DEFAULT 0, peso REAL DEFAULT 0,
        UNIQUE (product_id, bulto_num)
    );
    CREATE TABLE IF NOT EXISTS photos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id INTEGER NOT NULL REFERENCES products(id) ON DELETE CASCADE,
        filename TEXT NOT NULL, orden INTEGER DEFAULT 1,
        created_at TEXT DEFAULT (datetime('now','localtime'))
    );
    CREATE TABLE IF NOT EXISTS sync_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sync_at TEXT DEFAULT (datetime('now','localtime')),
        nuevos INTEGER DEFAULT 0, actualizados INTEGER DEFAULT 0,
        sin_cambios INTEGER DEFAULT 0, errores INTEGER DEFAULT 0,
        detalle TEXT
    );
""")
# Migrar columna erp_sync si no existe
try:
    conn_s.execute("ALTER TABLE products ADD COLUMN erp_sync INTEGER DEFAULT 0")
    conn_s.commit()
except Exception:
    pass  # Ya existe

conn_s.commit()
print("OK - Base de datos lista")

# Conectar MySQL
print(f"  Conectando a {MYSQL_CONFIG['host']}...")
mc = pymysql.connect(
    host=MYSQL_CONFIG['host'], port=MYSQL_CONFIG['port'],
    user=MYSQL_CONFIG['user'], password=MYSQL_CONFIG['password'],
    database=MYSQL_CONFIG['database'],
    cursorclass=pymysql.cursors.DictCursor,
    connect_timeout=20, charset='utf8mb4'
)
with mc.cursor() as c:
    c.execute(f"SELECT * FROM `{MYSQL_CONFIG['table']}`")
    rows = c.fetchall()
mc.close()
print(f"✓ {len(rows)} registros leídos desde MySQL")

nuevos = omitidos = errores = 0

for i, row in enumerate(rows):
    sku    = str(row.get('SKU')    or '').strip().upper()
    nombre = str(row.get('Nombre') or '').strip()
    estado = str(row.get('Estado') or 'Pendiente').strip()
    codigo = str(row.get('Codigo') or '').strip()

    if not sku:
        omitidos += 1
        continue

    existe = conn_s.execute("SELECT id FROM products WHERE sku=?", (sku,)).fetchone()
    if existe:
        omitidos += 1
        continue

    try:
        cur = conn_s.execute(
            "INSERT INTO products (sku,nombre,estado,codigo,erp_sync) VALUES (?,?,?,?,1)",
            (sku, nombre, estado, codigo)
        )
        pid = cur.lastrowid

        for n in range(1, MAX_BULTOS + 1):
            suf = '' if n == 1 else str(n)
            l = to_f(row.get(f'Largo ( cm ){suf}'))
            a = to_f(row.get(f'Ancho ( cm ){suf}'))
            h = to_f(row.get(f'Alto ( cm ){suf}'))
            p = to_f(row.get(f'Peso (kg){suf}'))
            if l > 0 or a > 0 or h > 0 or p > 0:
                conn_s.execute(
                    "INSERT OR IGNORE INTO bultos (product_id,bulto_num,largo,ancho,alto,peso) VALUES (?,?,?,?,?,?)",
                    (pid, n, l, a, h, p)
                )

        conn_s.commit()
        nuevos += 1

        if nuevos % 100 == 0:
            print(f"  ... {nuevos} importados")

    except Exception as e:
        errores += 1
        conn_s.rollback()
        print(f"  ✗ Error en {sku}: {e}")

# Guardar log
conn_s.execute(
    "INSERT INTO sync_log (nuevos,actualizados,sin_cambios,errores,detalle) VALUES (?,0,?,?,?)",
    (nuevos, omitidos, errores, 'Importación inicial desde MySQL')
)
conn_s.commit()
conn_s.close()

print(f"""
═══════════════════════════════════
  Importación completada
  ✓ Importados : {nuevos}
  ↷ Omitidos   : {omitidos}
  ✗ Errores    : {errores}
═══════════════════════════════════
""")

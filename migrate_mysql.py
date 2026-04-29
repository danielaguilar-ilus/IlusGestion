"""
migrate_mysql.py — Importa datos desde MySQL remoto a SQLite local
================================================================
INSTRUCCIONES:
  1. Edita la sección CONFIGURACIÓN con tus datos de MySQL
  2. Ejecuta: python migrate_mysql.py
  3. Los productos que ya existen en SQLite serán omitidos (no se duplican)
"""

import sqlite3, sys, os

# ════════════════════════════════════════════════════════════════
#  CONFIGURACIÓN — edita estos valores
# ════════════════════════════════════════════════════════════════
MYSQL = {
    'host':     'TU_HOST_MYSQL',       # Ej: '192.168.1.10' o 'db.miservidor.com'
    'port':     3306,
    'user':     'TU_USUARIO',
    'password': 'TU_CONTRASEÑA',
    'database': 'TU_BASE_DE_DATOS',
    'table':    'TU_TABLA',            # Nombre exacto de la tabla en MySQL
}

# Columnas tal como están en tu tabla MySQL.
# Si los nombres de columna son distintos, ajústalos aquí.
COL = {
    'sku':    'SKU',
    'nombre': 'Nombre',
    'estado': 'Estado',
    'codigo': 'Codigo',
    # Patrón de columnas por bulto.
    # Bulto 1: 'Largo ( cm )', 'Ancho ( cm )', 'Alto ( cm )', 'Peso (kg)'
    # Bulto 2: 'Largo ( cm )2', etc.
    'largo':  'Largo ( cm )',
    'ancho':  'Ancho ( cm )',
    'alto':   'Alto ( cm )',
    'peso':   'Peso (kg)',
}

MAX_BULTOS = 27
DB_PATH    = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'etiquetas.db')
# ════════════════════════════════════════════════════════════════


def to_f(v):
    try:
        return float(str(v or '0').replace(',', '.'))
    except (ValueError, TypeError):
        return 0.0


def get_col(row, base, suffix):
    """Intenta varias variantes del nombre de columna."""
    candidates = [
        f"{base}{suffix}",
        f"{base} {suffix}",
        f"{base.lower()}{suffix}",
        f"{base.lower()}_{suffix}",
        base if suffix == '' else None,
    ]
    for c in candidates:
        if c and c in row:
            return row[c]
    return 0


def migrate():
    # ── Instalar pymysql si hace falta ──
    try:
        import pymysql
        import pymysql.cursors
    except ImportError:
        print("  Instalando pymysql...")
        import subprocess
        subprocess.run([sys.executable, '-m', 'pip', 'install', 'pymysql'], check=True)
        import pymysql
        import pymysql.cursors

    # ── Validar config ──
    if 'TU_HOST' in MYSQL['host']:
        print("\n❌  Edita el archivo migrate_mysql.py y completa los datos de conexión MySQL.")
        sys.exit(1)

    print(f"\n  Conectando a MySQL: {MYSQL['user']}@{MYSQL['host']}:{MYSQL['port']}/{MYSQL['database']}")

    try:
        mysql_conn = pymysql.connect(
            host=MYSQL['host'], port=MYSQL['port'],
            user=MYSQL['user'], password=MYSQL['password'],
            database=MYSQL['database'],
            cursorclass=pymysql.cursors.DictCursor,
            connect_timeout=10,
        )
    except Exception as e:
        print(f"\n❌  No se pudo conectar a MySQL: {e}")
        sys.exit(1)

    print(f"  ✓ Conectado. Leyendo tabla '{MYSQL['table']}'...")

    with mysql_conn.cursor() as cur:
        cur.execute(f"SELECT * FROM `{MYSQL['table']}`")
        rows = cur.fetchall()

    mysql_conn.close()
    print(f"  ✓ {len(rows)} filas encontradas en MySQL.\n")

    # ── Abrir SQLite ──
    sqlite = sqlite3.connect(DB_PATH)
    sqlite.execute("PRAGMA foreign_keys = ON")

    migrated = skipped = errors = 0

    for row in rows:
        sku    = str(row.get(COL['sku'])    or '').strip().upper()
        nombre = str(row.get(COL['nombre']) or '').strip()
        estado = str(row.get(COL['estado']) or 'Pendiente').strip()
        codigo = str(row.get(COL['codigo']) or '').strip()

        if not sku:
            skipped += 1
            continue

        # Verificar duplicado
        existing = sqlite.execute("SELECT id FROM products WHERE sku=?", (sku,)).fetchone()
        if existing:
            print(f"  ↷  Ya existe: {sku}")
            skipped += 1
            continue

        try:
            cur = sqlite.execute(
                "INSERT INTO products (sku,nombre,estado,codigo) VALUES (?,?,?,?)",
                (sku, nombre, estado, codigo)
            )
            pid = cur.lastrowid

            # Importar bultos
            for i in range(1, MAX_BULTOS + 1):
                suffix = '' if i == 1 else str(i)
                l = to_f(get_col(row, COL['largo'], suffix))
                a = to_f(get_col(row, COL['ancho'], suffix))
                h = to_f(get_col(row, COL['alto'],  suffix))
                p = to_f(get_col(row, COL['peso'],  suffix))

                if l > 0 or a > 0 or h > 0 or p > 0:
                    sqlite.execute(
                        "INSERT INTO bultos (product_id,bulto_num,largo,ancho,alto,peso) VALUES (?,?,?,?,?,?)",
                        (pid, i, l, a, h, p)
                    )

            sqlite.commit()
            migrated += 1
            print(f"  ✓  {sku} — {nombre}")

        except Exception as e:
            sqlite.rollback()
            errors += 1
            print(f"  ✗  Error en {sku}: {e}")

    sqlite.close()

    print(f"""
  ════════════════════════════════════
   Migración completada
   ✓ Migrados : {migrated}
   ↷ Omitidos : {skipped}
   ✗ Errores  : {errors}
  ════════════════════════════════════
""")


if __name__ == '__main__':
    print("=" * 45)
    print("  ILUS — Migración MySQL → SQLite")
    print("=" * 45)
    migrate()
    input("\nPresiona Enter para cerrar...")

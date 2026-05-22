"""
Backfill de mant_visitas.created_by_user_id (FK estable a app_users.id).

CONTEXTO (2026-05-22 — Daniel, brecha Aaron Urbina ejecutivo_sstt):
La columna `created_by` (VARCHAR) guardaba lo que devolvía
`current_username()` en el momento de crear la OT — a veces el email
(username), a veces el nombre legible (caso Aaron: created_by='Aaron
Urbina' vs username='urbinaaaron65@gmail.com'). Eso rompía el filtro
del listado /mantenciones/ots para el ejecutivo: el LOWER(TRIM())
comparaba contra el username y no matcheaba el nombre.

La migración (en `init_mantenciones_tables`) ya agregó la columna
`created_by_user_id INT NULL` con índice. Este script la **pobla
retroactivamente** para OTs existentes:

  1. Intenta match exacto contra `app_users.username` (lowercase, trim).
  2. Si no hay match, intenta match contra `app_users.nombre`.
  3. Si tampoco, deja NULL (no asume — auditable).

USO:
    python _admin_backfill_created_by_user_id.py        # DRY RUN
    python _admin_backfill_created_by_user_id.py --apply # Ejecuta

Es idempotente: solo toca filas donde created_by_user_id IS NULL.
Re-ejecutarlo es seguro y no duplica updates.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env'))

import pymysql

APPLY = "--apply" in sys.argv

c = pymysql.connect(
    host=os.environ['MYSQL_HOST'], port=int(os.environ['MYSQL_PORT']),
    user=os.environ['MYSQL_USER'], password=os.environ['MYSQL_PASSWORD'],
    database=os.environ['MYSQL_DATABASE'],
    cursorclass=pymysql.cursors.DictCursor,
    autocommit=False,
)
cur = c.cursor()

# 1. Verificar que la columna existe (si no, abortar — falta correr la migración)
cur.execute(
    """SELECT COUNT(*) AS n
         FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = 'mant_visitas'
          AND COLUMN_NAME = 'created_by_user_id'"""
)
if not (cur.fetchone() or {}).get("n"):
    print("[FATAL] La columna mant_visitas.created_by_user_id NO existe.")
    print("        Inicia la app al menos una vez para correr init_mantenciones_tables(),")
    print("        o crea la columna manualmente:")
    print()
    print("  ALTER TABLE mant_visitas ADD COLUMN created_by_user_id INT NULL;")
    print("  CREATE INDEX idx_v_created_by_user ON mant_visitas (created_by_user_id);")
    print()
    sys.exit(1)

# 2. Cargar usuarios (id, username, nombre) en memoria — son pocos.
cur.execute(
    "SELECT id, LOWER(TRIM(username)) AS u_lower, LOWER(TRIM(COALESCE(nombre,''))) AS n_lower "
    "  FROM app_users WHERE active=1"
)
users = cur.fetchall() or []
by_username = {}
by_nombre = {}
for u in users:
    if u["u_lower"]:
        by_username[u["u_lower"]] = u["id"]
    if u["n_lower"]:
        by_nombre.setdefault(u["n_lower"], u["id"])  # primer match gana

print(f"=== Usuarios activos cargados: {len(users)} (by_username={len(by_username)}, by_nombre={len(by_nombre)}) ===")

# 3. Buscar OTs con created_by_user_id NULL
cur.execute(
    "SELECT id, numero_ot, created_by, created_by_user_id "
    "  FROM mant_visitas "
    " WHERE created_by_user_id IS NULL AND created_by IS NOT NULL "
    " ORDER BY id"
)
ots_sin = cur.fetchall() or []
print(f"=== OTs sin created_by_user_id: {len(ots_sin)} ===")

n_match_username = 0
n_match_nombre = 0
n_sin_match = 0
updates = []  # [(uid, vid)]

for v in ots_sin:
    cb = (v.get("created_by") or "").strip().lower()
    if not cb:
        n_sin_match += 1
        continue
    uid = by_username.get(cb)
    if uid:
        n_match_username += 1
        updates.append((uid, v["id"]))
        continue
    uid = by_nombre.get(cb)
    if uid:
        n_match_nombre += 1
        updates.append((uid, v["id"]))
        continue
    n_sin_match += 1
    print(f"  - vid={v['id']:5d} ({v.get('numero_ot') or '?'}) created_by='{cb}' → sin match")

print()
print(f"  · Match por username: {n_match_username}")
print(f"  · Match por nombre:   {n_match_nombre}")
print(f"  · Sin match:          {n_sin_match}")
print(f"  · Total a actualizar: {len(updates)}")

if not updates:
    print()
    print("Nada que hacer.")
    sys.exit(0)

if not APPLY:
    print()
    print("[DRY RUN] No se aplicaron cambios. Re-ejecuta con --apply para confirmar.")
    sys.exit(0)

# 4. Ejecutar updates en batch
print()
print(f"=== Aplicando {len(updates)} updates ===")
cur.executemany(
    "UPDATE mant_visitas SET created_by_user_id=%s WHERE id=%s",
    updates,
)
c.commit()
print(f"  · {cur.rowcount} filas actualizadas.")
print()
print("Backfill completado.")

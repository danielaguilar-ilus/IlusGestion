"""
Borrado masivo de OTs autorizado por Daniel (22/05/2026).

ATENCIÓN — ESTE SCRIPT BORRA TODAS las OTs (mant_visitas) y sus hijas
(tareas, fotos, equipos, técnicos, repuestos) de la BD MySQL configurada
en el .env. Los levantamientos (mant_levantamientos) NO se borran — solo
se desligan poniendo visita_id=NULL para preservar la información histórica.

USO:
    python _admin_borrar_ots.py            # ← DRY RUN (muestra qué borraría, NO ejecuta)
    python _admin_borrar_ots.py --apply    # ← ejecuta el borrado real

Antes de borrar, escribe en stdout el snapshot de cada OT que va a borrarse
y lo persiste en mant_logs con accion='ot_borrada_masiva'.
"""
import os, sys, json
from datetime import datetime

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

# 1. Snapshot completo de las OTs a borrar
cur.execute("""
    SELECT v.id, v.numero_ot, v.cliente_id, v.titulo, v.tipo, v.estado,
           v.fecha_programada, v.tecnico_user_id, v.levantamiento_id,
           v.created_by, v.created_at, c.razon_social
      FROM mant_visitas v
      LEFT JOIN mant_clientes c ON c.id=v.cliente_id
      ORDER BY v.id
""")
ots = cur.fetchall()
print(f"=== OTs encontradas: {len(ots)} ===")
for v in ots:
    print(f"  · vid={v['id']:4d} | {v.get('numero_ot') or '(s/n)':18s} | "
          f"tipo={v['tipo']:14s} | estado={v['estado']:18s} | "
          f"cliente_id={v['cliente_id']} ({(v.get('razon_social') or '?')[:40]}) | "
          f"created_by={v.get('created_by') or 'NULL'}")

if not ots:
    print("\nNo hay OTs para borrar. Saliendo.")
    c.close()
    sys.exit(0)

if not APPLY:
    print("\n=== DRY RUN — no se ejecutó ningún DELETE ===")
    print("Para ejecutar el borrado real: python _admin_borrar_ots.py --apply")
    c.close()
    sys.exit(0)

# 2. Audit log
audit_at = datetime.now().isoformat(timespec='seconds')
motivo = "Borrado masivo autorizado por Daniel (22/05/2026): 'borrar todas las OTs'"
audit_rows = []
for v in ots:
    snap = {
        "numero_ot": v.get("numero_ot"), "cliente_id": v.get("cliente_id"),
        "cliente": v.get("razon_social"), "titulo": v.get("titulo"),
        "tipo": v.get("tipo"), "estado": v.get("estado"),
        "fecha_programada": str(v.get("fecha_programada") or ""),
        "tecnico_user_id": v.get("tecnico_user_id"),
        "levantamiento_id": v.get("levantamiento_id"),
        "created_by": v.get("created_by"),
        "created_at": str(v.get("created_at") or ""),
    }
    detalle = (f"BORRADO MASIVO {audit_at} | motivo={motivo} | "
               f"snap={json.dumps(snap, ensure_ascii=False)}")
    audit_rows.append(("visita", v["id"], "ot_borrada_masiva",
                       detalle[:2000], "daniel.aguilar@sphs.cl"))

cur.executemany(
    "INSERT INTO mant_logs (entidad, entidad_id, accion, detalle, usuario, created_at) "
    "VALUES (%s,%s,%s,%s,%s, NOW())",
    audit_rows
)
print(f"\n✓ Audit: {cur.rowcount} filas en mant_logs")

# 3. Desligar levantamientos (NO los borramos)
cur.execute("UPDATE mant_levantamientos SET visita_id=NULL WHERE visita_id IS NOT NULL")
print(f"✓ Levantamientos desligados: {cur.rowcount}")

# 4. Borrar hijas explícitamente (defensa por si CASCADE no cubre algo)
for tbl in ('mant_visita_tareas', 'mant_visita_fotos', 'mant_visita_equipos',
            'mant_visita_tecnicos', 'mant_visita_repuestos'):
    try:
        cur.execute(f"DELETE FROM {tbl}")
        print(f"  · {tbl}: {cur.rowcount} filas")
    except Exception as e:
        print(f"  · {tbl}: SKIP ({str(e)[:80]})")

# 5. DELETE de mant_visitas
cur.execute("DELETE FROM mant_visitas")
print(f"✓ mant_visitas borradas: {cur.rowcount} filas")

# 6. Reset AUTO_INCREMENT
cur.execute("ALTER TABLE mant_visitas AUTO_INCREMENT = 1")
print("✓ AUTO_INCREMENT reset a 1")

# 7. Verificar
cur.execute("SELECT COUNT(*) AS n FROM mant_visitas")
rem = cur.fetchone()["n"]
print(f"\n=== POST-BORRADO: {rem} OTs en BD ===")

cur.execute("SELECT COUNT(*) AS n FROM mant_logs "
            "WHERE accion='ot_borrada_masiva' "
            "  AND created_at >= NOW() - INTERVAL 5 MINUTE")
print(f"✓ Audit visible en mant_logs: {cur.fetchone()['n']} filas")

c.commit()
print("\n✓ COMMIT exitoso — borrado finalizado.")
c.close()

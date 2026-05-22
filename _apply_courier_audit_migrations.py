"""
Aplica las migraciones de courier_tariff_audit + carga el seed Lo Barnechea
con precios validados del Excel. Ejecutar UNA VEZ contra BD donde aún no
corren las migraciones automáticas (ILUS_SKIP_MIGRATIONS=1).

Uso (con .env local apuntando a la BD destino):
    python _apply_courier_audit_migrations.py

Idempotente: si ya está, no rompe nada.
NO modifica Railway directamente — corre contra la BD que tu .env indique.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))
except Exception:
    pass

# Forzar migraciones aunque ILUS_SKIP_MIGRATIONS=1 (solo este script)
os.environ.pop('ILUS_SKIP_MIGRATIONS', None)
os.environ['ILUS_SKIP_MIGRATIONS'] = '0'

import app  # carga config, abre pool
from app import (app as flask_app, init_transporte_tables, _seed_lo_barnechea_excel,
                  get_mysql)


def main():
    print('═' * 70)
    print('  APPLY courier_tariff_audit migrations + seed Lo Barnechea')
    print('═' * 70)
    print(f'  MYSQL_HOST: {os.environ.get("MYSQL_HOST", "?")}')
    print(f'  MYSQL_DB:   {os.environ.get("MYSQL_DATABASE", "?")}')
    print()

    with flask_app.app_context():
        # 1) Correr init_transporte_tables (crea/altera todas las tablas)
        print('[1/3] init_transporte_tables...')
        try:
            init_transporte_tables()
            print('       OK')
        except Exception as ex:
            print(f'       ERROR: {ex}')
            return 1

        # 2) Verificar que courier_tariff_audit existe
        print('[2/3] verificar courier_tariff_audit...')
        conn = get_mysql()
        try:
            with conn.cursor() as cur:
                cur.execute("SHOW TABLES LIKE 'courier_tariff_audit'")
                r = cur.fetchone()
                if not r:
                    print('       ERROR: tabla NO se creó')
                    return 1
                cur.execute("DESCRIBE courier_tariff_audit")
                cols = [row['Field'] if isinstance(row, dict) else row[0]
                        for row in cur.fetchall()]
                print(f'       OK — columnas: {", ".join(cols)}')
                needed = ['precio_costo', 'margen_pct', 'margen_clp',
                          'iva_pct', 'iva_clp', 'precio_venta']
                missing = [c for c in needed if c not in cols]
                if missing:
                    print(f'       ATENCIÓN — columnas margen+IVA faltantes: {missing}')
                else:
                    print(f'       ✓ Todas las columnas de margen+IVA presentes')
        finally:
            conn.close()

        # 3) Aplicar seed Lo Barnechea
        print('[3/3] _seed_lo_barnechea_excel...')
        conn = get_mysql()
        try:
            with conn.cursor() as cur:
                _seed_lo_barnechea_excel(cur)
            conn.commit()
            print('       OK')
        except Exception as ex:
            print(f'       ERROR: {ex}')
            conn.rollback()
            return 1
        finally:
            conn.close()

        print()
        print('  ╭──────────────────────────────────────────╮')
        print('  │  ✓ Migraciones + seed aplicados a la BD  │')
        print('  ╰──────────────────────────────────────────╯')
        print()
        print('  Ahora correr: python test_fcv10644_couriers.py')
        print()
    return 0


if __name__ == '__main__':
    sys.exit(main())

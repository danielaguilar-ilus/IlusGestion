"""Reproduce localmente el 500 del tracking público para retiro 7."""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env'))

import pymysql
c = pymysql.connect(
    host=os.environ['MYSQL_HOST'], port=int(os.environ['MYSQL_PORT']),
    user=os.environ['MYSQL_USER'], password=os.environ['MYSQL_PASSWORD'],
    database=os.environ['MYSQL_DATABASE'], cursorclass=pymysql.cursors.DictCursor
)
cur = c.cursor()
cur.execute("SELECT * FROM pickup_requests WHERE id=7")
req = cur.fetchone()
print("=== Retiro 7 — campos ===")
for k, v in req.items():
    print(f"  {k}: {repr(v)[:90]}")

print("\n=== Packages del retiro 7 ===")
cur.execute("SELECT * FROM pickup_packages WHERE request_id=7")
for p in cur.fetchall() or []:
    print(f"  {p}")

print("\n=== Proposals del retiro 7 ===")
cur.execute("SELECT * FROM pickup_proposals WHERE request_id=7")
for p in cur.fetchall() or []:
    print(f"  {p}")

c.close()

# Ahora intenta renderizar el template con esos datos
print("\n=== RENDER TEST ===")
from jinja2 import Environment, FileSystemLoader
env = Environment(loader=FileSystemLoader('templates'))
for fn_name in ['chile_fmt', 'rut_fmt', 'tel_chile_fmt', 'fkg', 'fvol',
                'fromjson_safe', 'to_chile', 'from_json', 'cloud_tx', 'hm']:
    env.filters[fn_name] = lambda v, *a, **k: str(v or '')
env.globals['url_for'] = lambda *a, **k: '/test'
env.globals['request'] = type('R', (), {'args': {}})()
env.globals['get_flashed_messages'] = lambda **k: []
env.globals['config'] = {'filters': {}}
env.globals['current_user'] = type('U', (), {'id': 1, 'role': 'admin', 'username': 'test'})()
env.globals['g'] = type('G', (), {'permissions': {'admin': True, 'superadmin': True}})()
env.globals['url_for'] = lambda *a, **k: '/test'

# Simular req_safe con m3_calculado como hace el endpoint
req_safe = dict(req)
req_safe['m3_calculado'] = 0.0495
# Quitar campos internos
for k in ('internal_notes', 'doc_validation_notes', 'doc_erp_data',
          'doc_validated_by', 'doc_validated_at', 'created_ip',
          'created_user_agent', 'risk_score', 'information_quality_score',
          'reminder_24h_sent'):
    req_safe.pop(k, None)

t = env.get_template('retiros/public_tracking.html')
try:
    out = t.render(
        req=req_safe,
        packages=[],
        proposals=[],
        logs=[],
        attachments=[],
        docs_asociados=[],
        settings={'warehouse_name': 'ILUS', 'warehouse_addr': 'X', 'maps_url': ''},
        status_badge=lambda s: '',
        created=None,
        pending=[],
    )
    print(f"RENDER OK, {len(out)} chars")
except Exception as e:
    print(f"RENDER FAILED:")
    import traceback
    traceback.print_exc()

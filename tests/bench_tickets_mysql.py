"""Prueba de carga de la bandeja de Tickets en un MySQL 8.0 TEMPORAL (puerto 3399).
Datos de mentira. Mide: consulta original, fase 1 y fase 2 (columnas generadas + indice)."""
import random, sys, time
from datetime import datetime, timedelta
import pymysql

N_T = int(sys.argv[1]) if len(sys.argv) > 1 else 100000
N_M = int(sys.argv[2]) if len(sys.argv) > 2 else 1000000
c = pymysql.connect(host="127.0.0.1", port=3399, user="root", password="", autocommit=True,
                    charset="utf8mb4", cursorclass=pymysql.cursors.DictCursor)
cur = c.cursor()
cur.execute("DROP DATABASE IF EXISTS tk_bench"); cur.execute("CREATE DATABASE tk_bench"); cur.execute("USE tk_bench")
cur.execute("""CREATE TABLE tk_tickets (
  id INT AUTO_INCREMENT PRIMARY KEY, numero_ticket VARCHAR(30) NULL UNIQUE,
  origen ENUM('form','backoffice','erp') NOT NULL DEFAULT 'backoffice',
  estado ENUM('open','in_progress','pending','resolved','closed','ot_pending_approval','ot_generated','ot_in_progress','cancelado') NOT NULL DEFAULT 'open',
  tipo ENUM('install','tech_support','shipping','quotation','return','tech_evaluation','maintenance','spare_parts') NULL,
  prioridad ENUM('baja','media','alta','urgente') NOT NULL DEFAULT 'media',
  titulo VARCHAR(300) NULL, descripcion TEXT NULL, rut VARCHAR(12) NULL, empresa VARCHAR(150) NULL,
  nombre_contacto VARCHAR(150) NULL, email VARCHAR(150) NULL, producto TEXT NULL, notas_internas TEXT NULL,
  asignado_a VARCHAR(190) NULL, staff_last_read_at DATETIME NULL, fecha_limite DATE NULL,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP, updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY idx_estado (estado), KEY idx_tipo (tipo), KEY idx_origen (origen), KEY idx_prioridad (prioridad),
  KEY idx_asignado (asignado_a), KEY idx_created (created_at), KEY idx_estado_updated (estado, updated_at), KEY idx_rut (rut)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""")
cur.execute("""CREATE TABLE tk_mensajes (
  id INT AUTO_INCREMENT PRIMARY KEY, ticket_id INT NOT NULL,
  tipo ENUM('comentario','mensaje','client_message','cambio_estado','asignacion','creacion','cierre','reapertura','archivo','otro') NOT NULL DEFAULT 'comentario',
  contenido MEDIUMTEXT NULL, usuario VARCHAR(190) NULL, created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  KEY idx_ticket (ticket_id, created_at), KEY idx_unread (ticket_id, tipo, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""")

rnd = random.Random(11)
base = datetime(2025, 1, 1)
ESTADOS = ['closed'] * 55 + ['resolved'] * 15 + ['open'] * 12 + ['in_progress'] * 8 + ['pending'] * 5 + ['cancelado'] * 3 + ['ot_generated'] * 2
PRIOS = ['media'] * 60 + ['baja'] * 20 + ['alta'] * 15 + ['urgente'] * 5
USERS = [f"ejecutivo{i}@sphs.cl" for i in range(8)]
f = lambda d: d.strftime("%Y-%m-%d %H:%M:%S")
t0 = time.time()
for ini in range(0, N_T, 10000):
    rows = []
    for i in range(ini + 1, min(ini + 10000, N_T) + 1):
        cr = base + timedelta(minutes=rnd.randint(0, 22 * 30 * 24 * 60))
        rows.append((i, f"TK-2026-{i:06d}", rnd.choice(['form', 'backoffice', 'erp']), rnd.choice(ESTADOS),
                     rnd.choice(['install', 'tech_support', 'shipping', 'spare_parts', 'maintenance']), rnd.choice(PRIOS),
                     f"Ticket de prueba {i}", "descripcion " * 25, f"{rnd.randint(7, 99)}.{rnd.randint(100, 999)}.{rnd.randint(100, 999)}-{rnd.randint(0, 9)}",
                     f"Empresa {i % 3000}", f"Contacto {i}", f"c{i}@mail.cl", "producto " * 10, "nota " * 20,
                     rnd.choice(USERS + [None]), f(cr + timedelta(hours=rnd.randint(0, 3000))) if rnd.random() < .7 else None,
                     f(cr), f(cr + timedelta(hours=rnd.randint(0, 3000)))))
    cur.executemany("INSERT INTO tk_tickets (id,numero_ticket,origen,estado,tipo,prioridad,titulo,descripcion,rut,empresa,nombre_contacto,email,producto,notas_internas,asignado_a,staff_last_read_at,created_at,updated_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", rows)
TIPOS = ['client_message'] * 35 + ['comentario'] * 35 + ['cambio_estado'] * 30
for ini in range(0, N_M, 20000):
    rows = [(rnd.randint(1, N_T), rnd.choice(TIPOS), "mensaje " * 25, "u",
             f(base + timedelta(minutes=rnd.randint(0, 24 * 30 * 24 * 60)))) for _ in range(min(20000, N_M - ini))]
    cur.executemany("INSERT INTO tk_mensajes (ticket_id,tipo,contenido,usuario,created_at) VALUES (%s,%s,%s,%s,%s)", rows)
cur.execute("ANALYZE TABLE tk_tickets, tk_mensajes")
cur.fetchall()
print(f"carga: {N_T} tickets / {N_M} mensajes en {time.time()-t0:.0f}s", flush=True)


def tm(etq, sql, n=3, args=None):
    mejor = None
    for _ in range(n):
        s = time.time(); cur.execute(sql, args); r = cur.fetchall(); d = time.time() - s
        mejor = d if mejor is None else min(mejor, d)
    print(f"  {etq:<46} {mejor*1000:9.0f} ms", flush=True)
    return r


FIELD_E = "FIELD(t.estado,'open','in_progress','pending','ot_pending_approval','ot_generated','ot_in_progress','resolved','closed','cancelado')"
FIELD_P = "FIELD(t.prioridad,'urgente','alta','media','baja')"
COLS = "t.id, t.numero_ticket, t.estado, t.prioridad, t.titulo, t.empresa, t.updated_at"
UNREAD = "(SELECT COUNT(*) FROM tk_mensajes m WHERE m.ticket_id=t.id AND m.tipo='client_message' AND m.created_at > COALESCE(t.staff_last_read_at,'1970-01-01')) AS unread_count"
OLD = f"SELECT {COLS},{UNREAD} FROM tk_tickets t{{w}} ORDER BY (SELECT COUNT(*) FROM tk_mensajes m2 WHERE m2.ticket_id=t.id AND m2.tipo='client_message' AND m2.created_at > COALESCE(t.staff_last_read_at,'1970-01-01')) > 0 DESC, {FIELD_E}, {FIELD_P}, t.updated_at DESC, t.id DESC LIMIT 50 OFFSET {{o}}"
F1 = OLD.replace("(SELECT COUNT(*) FROM tk_mensajes m2", "EXISTS (SELECT 1 FROM tk_mensajes m2").replace("COALESCE(t.staff_last_read_at,'1970-01-01')) > 0 DESC", "COALESCE(t.staff_last_read_at,'1970-01-01')) DESC")
F2 = f"SELECT {COLS},{UNREAD} FROM tk_tickets t{{w}} ORDER BY t.sin_leer DESC, t.ord_estado, t.ord_prio, t.updated_at DESC, t.id DESC LIMIT 50 OFFSET {{o}}"

print("\n== ORIGINAL (COUNT>0) y FASE 1 (EXISTS)")
r_old = tm("bandeja pagina 1 sin filtro (original)", OLD.format(w="", o=0), n=1)
r_old2 = tm("bandeja pagina 20 sin filtro (original)", OLD.format(w="", o=1000), n=1)
r_f1 = tm("bandeja pagina 1 sin filtro (fase 1)", F1.format(w="", o=0), n=1)
tm("bandeja filtro estado=open (original)", OLD.format(w=" WHERE t.estado='open'", o=0), n=1)
tm("bandeja filtro estado=open (fase 1)", F1.format(w=" WHERE t.estado='open'", o=0), n=1)
tm("no leidos JOIN DISTINCT (original)", "SELECT COUNT(DISTINCT t.id) n FROM tk_tickets t JOIN tk_mensajes m ON m.ticket_id=t.id AND m.tipo='client_message' WHERE m.created_at > COALESCE(t.staff_last_read_at,'1970-01-01')", n=1)
tm("no leidos EXISTS (fase 1)", "SELECT COUNT(*) n FROM tk_tickets t WHERE EXISTS (SELECT 1 FROM tk_mensajes m WHERE m.ticket_id=t.id AND m.tipo='client_message' AND m.created_at > COALESCE(t.staff_last_read_at,'1970-01-01'))", n=1)
tm("total + KPI en un barrido (fase 1)", "SELECT COUNT(*) total, SUM(estado IN ('open','in_progress')) a, SUM(prioridad='urgente' AND estado NOT IN ('resolved','closed','cancelado')) u FROM tk_tickets t")

print("\n== FASE 2: columnas + indice")
s = time.time()
cur.execute("ALTER TABLE tk_tickets ADD COLUMN ultimo_cliente_at DATETIME NULL"); print(f"  ADD ultimo_cliente_at {time.time()-s:.1f}s", flush=True)
s = time.time()
cur.execute("UPDATE tk_tickets t JOIN (SELECT ticket_id, MAX(created_at) mx FROM tk_mensajes WHERE tipo='client_message' GROUP BY ticket_id) m ON m.ticket_id=t.id SET t.ultimo_cliente_at=m.mx"); print(f"  backfill {time.time()-s:.1f}s", flush=True)
s = time.time()
cur.execute("ALTER TABLE tk_tickets ADD COLUMN sin_leer TINYINT GENERATED ALWAYS AS (IFNULL(ultimo_cliente_at > COALESCE(staff_last_read_at,'1970-01-01'),0)) STORED"); print(f"  ADD sin_leer (generada) {time.time()-s:.1f}s", flush=True)
s = time.time()
cur.execute("ALTER TABLE tk_tickets ADD COLUMN ord_estado TINYINT GENERATED ALWAYS AS (FIELD(estado,'open','in_progress','pending','ot_pending_approval','ot_generated','ot_in_progress','resolved','closed','cancelado')) STORED, ADD COLUMN ord_prio TINYINT GENERATED ALWAYS AS (FIELD(prioridad,'urgente','alta','media','baja')) STORED"); print(f"  ADD ord_estado/ord_prio {time.time()-s:.1f}s", flush=True)
s = time.time()
cur.execute("CREATE INDEX idx_tk_bandeja ON tk_tickets (sin_leer DESC, ord_estado, ord_prio, updated_at DESC, id DESC)"); print(f"  CREATE INDEX bandeja {time.time()-s:.1f}s", flush=True)
cur.execute("ANALYZE TABLE tk_tickets"); cur.fetchall()
print("\n== FASE 2 (consultas)")
r_f2 = tm("bandeja pagina 1 sin filtro (fase 2)", F2.format(w="", o=0))
r_f2b = tm("bandeja pagina 20 sin filtro (fase 2)", F2.format(w="", o=1000))
tm("bandeja filtro estado=open (fase 2)", F2.format(w=" WHERE t.estado='open'", o=0))
tm("no leidos = COUNT(sin_leer=1) (fase 2)", "SELECT COUNT(*) n FROM tk_tickets WHERE sin_leer=1")
ids = lambda r: [x["id"] for x in r]
print("\nMismo orden pagina 1 original vs fase 2:", ids(r_old) == ids(r_f2), "| pagina 20:", ids(r_old2) == ids(r_f2b), "| fase 1 vs original:", ids(r_old) == ids(r_f1))
cur.execute("EXPLAIN " + F2.format(w="", o=0)); e = cur.fetchall()[0]; print("EXPLAIN fase 2: key=", e["key"], "| Extra=", e["Extra"])

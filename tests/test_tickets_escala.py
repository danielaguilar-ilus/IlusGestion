"""Tickets 2.0 · fase 1: las consultas nuevas dan EXACTAMENTE lo mismo que las
viejas (solo cambia cuánto trabajo hacen). Datos de mentira, SQLite en memoria:
FIELD() se registra a mano. No toca ninguna base real (REGLA #22)."""
import random
import sqlite3
import time
from datetime import datetime, timedelta

import tickets_module as tm

ESTADOS = ['open', 'in_progress', 'pending', 'ot_pending_approval',
           'ot_generated', 'ot_in_progress', 'resolved', 'closed', 'cancelado']
PRIOS = ['urgente', 'alta', 'media', 'baja']


def _field(v, *opciones):
    return (list(opciones).index(v) + 1) if v in opciones else 0


def _bd(n_tickets=20000, n_msgs=60000, semilla=7):
    rnd = random.Random(semilla)
    c = sqlite3.connect(":memory:")
    c.create_function("FIELD", -1, _field)
    c.executescript("""
        CREATE TABLE tk_tickets(id INTEGER PRIMARY KEY, estado TEXT, prioridad TEXT,
            staff_last_read_at TEXT, updated_at TEXT, created_at TEXT);
        CREATE TABLE tk_mensajes(id INTEGER PRIMARY KEY, ticket_id INT, tipo TEXT, created_at TEXT);
        CREATE INDEX idx_unread ON tk_mensajes(ticket_id, tipo, created_at);
    """)
    base = datetime(2026, 1, 1)
    f = lambda d: d.strftime("%Y-%m-%d %H:%M:%S")
    c.executemany("INSERT INTO tk_tickets VALUES (?,?,?,?,?,?)", [
        (i, rnd.choice(ESTADOS), rnd.choice(PRIOS),
         f(base + timedelta(hours=rnd.randint(0, 6000))) if rnd.random() < .8 else None,
         f(base + timedelta(minutes=i)), f(base + timedelta(minutes=i)))
        for i in range(1, n_tickets + 1)])
    c.executemany("INSERT INTO tk_mensajes(ticket_id,tipo,created_at) VALUES (?,?,?)", [
        (rnd.randint(1, n_tickets), rnd.choice(['client_message', 'comentario', 'client_message']),
         f(base + timedelta(hours=rnd.randint(0, 6500)))) for _ in range(n_msgs)])
    c.commit()
    return c


def _order(prefijo_sin_leer):
    return (f"{prefijo_sin_leer} DESC, "
            "FIELD(t.estado,'open','in_progress','pending','ot_pending_approval',"
            "'ot_generated','ot_in_progress','resolved','closed','cancelado'), "
            "FIELD(t.prioridad,'urgente','alta','media','baja'), t.updated_at DESC, t.id DESC")


VIEJO = ("(SELECT COUNT(*) FROM tk_mensajes m2 WHERE m2.ticket_id=t.id AND m2.tipo='client_message' "
         "AND m2.created_at > COALESCE(t.staff_last_read_at,'1970-01-01')) > 0")
NUEVO = ("EXISTS (SELECT 1 FROM tk_mensajes m2 WHERE m2.ticket_id=t.id AND m2.tipo='client_message' "
         "AND m2.created_at > COALESCE(t.staff_last_read_at,'1970-01-01'))")


def test_orden_por_defecto_exists_igual_a_count():
    c = _bd()
    sql = "SELECT t.id FROM tk_tickets t ORDER BY {} LIMIT 200 OFFSET {}"
    t0 = time.perf_counter()
    viejo = c.execute(sql.format(_order(VIEJO), 0)).fetchall()
    t_viejo = time.perf_counter() - t0
    t0 = time.perf_counter()
    nuevo = c.execute(sql.format(_order(NUEVO), 0)).fetchall()
    t_nuevo = time.perf_counter() - t0
    assert viejo == nuevo
    # una página profunda también debe coincidir
    assert c.execute(sql.format(_order(VIEJO), 5000)).fetchall() == \
        c.execute(sql.format(_order(NUEVO), 5000)).fetchall()
    print(f"\n[orden] 20.000 tickets / 60.000 mensajes: COUNT>0 {t_viejo*1000:.0f} ms · EXISTS {t_nuevo*1000:.0f} ms")


def test_no_leidos_semijoin_igual_a_join_distinct():
    c = _bd()
    viejo = c.execute(
        "SELECT COUNT(DISTINCT t.id) FROM tk_tickets t "
        "JOIN tk_mensajes m ON m.ticket_id=t.id AND m.tipo='client_message' "
        "WHERE m.created_at > COALESCE(t.staff_last_read_at,'1970-01-01')").fetchone()[0]
    nuevo = c.execute(
        "SELECT COUNT(*) FROM tk_tickets t WHERE EXISTS (SELECT 1 FROM tk_mensajes m "
        "WHERE m.ticket_id=t.id AND m.tipo='client_message' "
        "AND m.created_at > COALESCE(t.staff_last_read_at,'1970-01-01'))").fetchone()[0]
    assert viejo == nuevo and nuevo > 0


def test_rango_de_dia_equivale_a_convert_tz():
    """created_at UTC; el filtro viejo comparaba DATE(created_at + offset)."""
    rnd = random.Random(3)
    for off in ("-04:00", "-03:00"):
        h = int(off[1:3])
        desde, hasta = "2026-03-10", "2026-03-12"
        ini = tm._tk_dia_local_a_utc(desde, off, 0)
        fin = tm._tk_dia_local_a_utc(hasta, off, 1)
        assert ini and fin
        for _ in range(3000):
            utc = datetime(2026, 3, 8) + timedelta(minutes=rnd.randint(0, 6 * 24 * 60))
            local = (utc - timedelta(hours=h)).date().isoformat()
            viejo = desde <= local <= hasta
            nuevo = ini <= utc.strftime("%Y-%m-%d %H:%M:%S") < fin
            assert viejo == nuevo, (off, utc)


def test_rango_de_dia_fecha_invalida_no_filtra():
    assert tm._tk_dia_local_a_utc("2026-13-45", "-04:00") is None

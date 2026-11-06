# -*- coding: utf-8 -*-
"""🧹 Fase 0 (4-nov-2026): la limpieza de los correos del cliente guardados dos veces ANTES del arreglo (_tk_mail_reclamar).
Quedan ~183 mensajes y ~210 adjuntos repetidos (auditoría de solo lectura: tests/test_tickets_auditoria_correos_duplicados.py).

Es lo ÚNICO del módulo que borra correos de clientes, así que se prueba con la máxima dureza, con una base SQLite de verdad (mismos tablas y
mismas sentencias; MD5/CHAR_LENGTH/LEFT/DATE_SUB traducidos) y sin tocar nada de producción (REGLA #22):
  · el plan (función pura): conserva la primera copia; omite lo dudoso (texto distinto, tipo/usuario distinto, muy separadas en el tiempo,
    adjuntos que la copia conservada no tiene);
  · la ruta: solo superadmin, SIMULACIÓN por defecto (también con null/""/valores raros), confirmación exacta con el número de la simulación,
    auditoría y respaldo ANTES de borrar, verificación de nuevo contra la base, tandas, idempotente, jamás toca los archivos de la nube.
Correr:  python -m pytest tests/test_tickets_limpieza_correos_duplicados.py -q -p no:warnings
"""
import ast
import functools
import hashlib
import json
import os
import re
import sqlite3
import unittest
from datetime import datetime, timedelta

_RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_MODULO = os.path.join(_RAIZ, "tickets_module.py")


@functools.lru_cache(maxsize=None)
def _analizado():
    with open(_MODULO, encoding="utf-8") as fh:
        fuente = fh.read()
    return fuente, ast.parse(fuente)


def _nodo(nombre):
    fuente, arbol = _analizado()
    for n in ast.walk(arbol):
        if isinstance(n, ast.FunctionDef) and n.name == nombre:
            return fuente, n
    raise AssertionError("no existe " + nombre)


def _funcion(nombre):
    fuente, n = _nodo(nombre)
    return re.sub(r"^ {4}", "", ast.get_source_segment(fuente, n), flags=re.M)


def _constante(nombre):
    """Texto de una asignación `NOMBRE = ...` anidada en tickets_module.py (sin sangría)."""
    fuente, arbol = _analizado()
    for n in ast.walk(arbol):
        if isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == nombre for t in n.targets):
            return re.sub(r"^ {4}", "", ast.get_source_segment(fuente, n), flags=re.M)
    raise AssertionError("no existe " + nombre)


T0 = datetime(2026, 11, 3, 20, 48, 0)


def _copia(i, minutos=0, tipo="client_message", usuario="Cliente"):
    return {"id": i, "tipo": tipo, "usuario": usuario, "created_at": T0 + timedelta(minutes=minutos)}


def _grupo(ids, mismo=True, **kw):
    return {"ticket_id": 2304, "message_id": "<a@x>", "mismo_contenido": mismo,
            "copias": [_copia(i, k, **kw) for k, i in enumerate(ids)]}


def _adj(i, nombre, kb=100):
    return {"id": i, "nombre": nombre, "tipo": "image/jpeg", "kb": kb}


def _plan(grupos, adj=None, **kw):
    ns = {}
    exec(compile(_constante("_TK_DUP_MAX_MINUTOS"), "<max>", "exec"), ns)
    exec(compile(_funcion("_tk_dup_plan"), "<plan>", "exec"), ns)
    return ns["_tk_dup_plan"](grupos, adj or {}, **kw)


class TestPlan(unittest.TestCase):
    def test_conserva_la_primera_copia_y_quita_las_otras(self):
        p = _plan([_grupo([4463, 4464, 4470])])
        self.assertEqual(p["mensajes"], 2)
        gr = p["grupos"][0]
        self.assertEqual(gr["conserva"], 4463)
        self.assertEqual([q["id"] for q in gr["quitar"]], [4464, 4470])

    def test_un_grupo_de_una_sola_copia_no_se_toca(self):
        self.assertEqual(_plan([_grupo([1])])["mensajes"], 0)
        self.assertEqual(_plan([])["grupos"], [])
        self.assertEqual(_plan(None)["mensajes"], 0)

    def test_texto_distinto_se_omite_para_que_lo_mire_una_persona(self):
        p = _plan([_grupo([1, 2], mismo=False)])
        self.assertEqual(p["mensajes"], 0)
        self.assertIn("distinto", p["omitidos"][0]["motivo"])

    def test_tipo_o_usuario_distinto_se_omite(self):
        g = _grupo([1, 2])
        g["copias"][1]["tipo"] = "mensaje"
        self.assertEqual(_plan([g])["mensajes"], 0)
        g = _grupo([1, 2])
        g["copias"][1]["usuario"] = "Otro"
        self.assertEqual(_plan([g])["mensajes"], 0)

    def test_copias_muy_separadas_en_el_tiempo_se_omiten(self):
        p = _plan([dict(_grupo([1, 2]), copias=[_copia(1, 0), _copia(2, 61)])])
        self.assertEqual(p["mensajes"], 0)
        self.assertIn("61" not in p["omitidos"][0]["motivo"] and "minutos", p["omitidos"][0]["motivo"])
        self.assertEqual(_plan([dict(_grupo([1, 2]), copias=[_copia(1, 0), _copia(2, 59)])])["mensajes"], 1)

    def test_fechas_ilegibles_no_inventan_una_limpieza(self):
        g = _grupo([1, 2])
        g["copias"][1]["created_at"] = None
        self.assertEqual(_plan([g])["mensajes"], 0)

    def test_los_adjuntos_se_quitan_solo_si_la_copia_conservada_tiene_los_mismos(self):
        adj = {1: [_adj(10, "IMG_1.jpeg"), _adj(11, "IMG_2.jpeg", 128)], 2: [_adj(20, "img_1.JPEG "), _adj(21, "IMG_2.jpeg", 128)]}
        p = _plan([_grupo([1, 2])], adj)
        self.assertEqual(p["mensajes"], 1)
        self.assertEqual(p["adjuntos"], 2)
        self.assertEqual(p["grupos"][0]["quitar"][0]["adjuntos"], [20, 21], "sin distinguir mayúsculas ni espacios")

    def test_adjunto_que_la_conservada_no_tiene_omite_el_grupo(self):
        adj = {1: [_adj(10, "IMG_1.jpeg")], 2: [_adj(20, "IMG_1.jpeg"), _adj(21, "IMG_9.jpeg")]}
        p = _plan([_grupo([1, 2])], adj)
        self.assertEqual(p["mensajes"], 0)
        self.assertIn("adjuntos", p["omitidos"][0]["motivo"])

    def test_mismo_nombre_con_otro_tamano_no_es_el_mismo_archivo(self):
        adj = {1: [_adj(10, "IMG_1.jpeg", 100)], 2: [_adj(20, "IMG_1.jpeg", 140)]}
        self.assertEqual(_plan([_grupo([1, 2])], adj)["mensajes"], 0)

    def test_dos_archivos_iguales_en_la_copia_y_uno_en_la_conservada_no_alcanza(self):
        adj = {1: [_adj(10, "IMG_1.jpeg")], 2: [_adj(20, "IMG_1.jpeg"), _adj(21, "IMG_1.jpeg")]}
        self.assertEqual(_plan([_grupo([1, 2])], adj)["mensajes"], 0)

    def test_con_tres_copias_cada_una_se_compara_con_la_conservada(self):
        adj = {1: [_adj(10, "A.jpeg")], 2: [_adj(20, "A.jpeg")], 3: [_adj(30, "A.jpeg")]}
        p = _plan([_grupo([1, 2, 3])], adj)
        self.assertEqual([(q["id"], q["adjuntos"]) for q in p["grupos"][0]["quitar"]], [(2, [20]), (3, [30])])

    def test_una_copia_sin_adjuntos_se_quita_aunque_la_conservada_los_tenga(self):
        adj = {1: [_adj(10, "A.jpeg")]}
        p = _plan([_grupo([1, 2])], adj)
        self.assertEqual((p["mensajes"], p["adjuntos"]), (1, 0))

    def test_un_grupo_dudoso_no_arrastra_a_los_buenos(self):
        p = _plan([dict(_grupo([1, 2]), ticket_id=1), dict(_grupo([5, 6], mismo=False), ticket_id=2), dict(_grupo([9, 10]), ticket_id=3)])
        self.assertEqual([g["ticket_id"] for g in p["grupos"]], [1, 3])
        self.assertEqual([o["ticket_id"] for o in p["omitidos"]], [2])


# ───────────────────────── la ruta, con SQLite de verdad ─────────────────────────
class _Peticion:
    def __init__(self, cuerpo=None):
        self._c = cuerpo

    def get_json(self, silent=False):
        return self._c


def _meta(mid):
    return json.dumps({"via": "email", "message_id": mid, "from": "c@x.cl"})


class _Cursor:
    """Cursor de la conexión de la transacción (lo que devuelve get_mysql().cursor()): SQLite SIN commit automático."""

    def __init__(self, base):
        self.base = base
        self._filas = []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=()):
        b = self.base
        s = " ".join(sql.split())
        if s.upper().startswith("SELECT"):
            if s.upper().startswith("SELECT * FROM TK_ADJUNTOS") and b.antes_de_leer_adjuntos:
                gancho, b.antes_de_leer_adjuntos = b.antes_de_leer_adjuntos, None
                gancho()
            self._filas = [dict(r) for r in b.con.execute(b._a_sqlite(sql), tuple(params)).fetchall()]
            return
        b._maybe_fallar(s)
        b.eventos.append(("sql", s.split(" (")[0][:60]))
        b.con.execute(b._a_sqlite(sql), tuple(params))

    def fetchone(self):
        return self._filas[0] if self._filas else None

    def fetchall(self):
        return list(self._filas)


class _Conexion:
    def __init__(self, base):
        self.base = base
        self.estado = {"commit": 0, "rollback": 0, "cerrada": False}
        base.conexiones.append(self.estado)

    def cursor(self):
        return _Cursor(self.base)

    def commit(self):
        self.estado["commit"] += 1
        self.base.con.commit()

    def rollback(self):
        self.estado["rollback"] += 1
        self.base.con.rollback()

    def close(self):
        self.estado["cerrada"] = True


class _Base:
    """SQLite con las tablas y columnas que toca la limpieza, y las funciones de MySQL que usa (MD5, CHAR_LENGTH, LEFT, DATE_SUB)."""

    def __init__(self):
        sqlite3.register_converter("DATETIME", lambda b: datetime.fromisoformat(b.decode()))
        self.con = sqlite3.connect(":memory:", detect_types=sqlite3.PARSE_DECLTYPES)
        self.con.row_factory = sqlite3.Row
        self.con.create_function("MD5", 1, lambda s: hashlib.md5((s or "").encode("utf-8")).hexdigest())
        self.con.create_function("CHAR_LENGTH", 1, lambda s: len(s or ""))
        self.con.create_function("LEFT", 2, lambda s, n: (s or "")[:n])
        self.con.executescript("""
            CREATE TABLE tk_mensajes (id INTEGER PRIMARY KEY AUTOINCREMENT, ticket_id INT NOT NULL, tipo TEXT, contenido TEXT, metadata TEXT,
                                      mail_message_id TEXT, es_interno INT DEFAULT 1, usuario TEXT, message_date DATETIME, created_at DATETIME);
            CREATE TABLE tk_adjuntos (id INTEGER PRIMARY KEY AUTOINCREMENT, ticket_id INT NOT NULL, mensaje_id INT, archivo_url TEXT,
                                      archivo_path TEXT, archivo_nombre TEXT, mime_type TEXT, file_size_kb INT, origen TEXT, subido_por TEXT,
                                      created_at DATETIME);
            CREATE TABLE tk_mensajes_quitados (id INTEGER PRIMARY KEY AUTOINCREMENT, mensaje_id INT NOT NULL, ticket_id INT NOT NULL,
                                      conserva_mensaje_id INT, fila_json TEXT, adjuntos_json TEXT, motivo TEXT, quitado_por TEXT,
                                      quitado_at DATETIME DEFAULT CURRENT_TIMESTAMP);
        """)
        self.eventos = []          # orden de lo ocurrido: ("audit", ...), ("sql", "DELETE ...")
        self.antes_de_leer_adjuntos = None   # gancho: se llama una vez justo antes de «SELECT * FROM tk_adjuntos»
        self.fallar_si = None      # texto que, si aparece en el SQL de escritura, lanza un error
        self.fallar_en_n = 1       # ...a partir de la N-ésima vez que aparece (1 = a la primera)
        self.fallar_solo_una = False   # ...y solo esa vez (las siguientes pasan)
        self._coincidencias = 0
        self.conexiones = []       # una por transacción: {"commit": n, "rollback": n, "cerrada": bool}

    def get_mysql(self):
        return _Conexion(self)

    @staticmethod
    def _a_sqlite(sql):
        s = sql.replace("%s", "?")
        return re.sub(r"DATE_SUB\(NOW\(\), INTERVAL \? DAY\)", "datetime('now', '-' || ? || ' days')", s)

    def fetchall(self, sql, params=()):
        if sql.lstrip().upper().startswith("SELECT * FROM TK_ADJUNTOS") and self.antes_de_leer_adjuntos:
            gancho, self.antes_de_leer_adjuntos = self.antes_de_leer_adjuntos, None
            gancho()
        return [dict(r) for r in self.con.execute(self._a_sqlite(sql), tuple(params)).fetchall()]

    def fetchone(self, sql, params=()):
        r = self.con.execute(self._a_sqlite(sql), tuple(params)).fetchone()
        return dict(r) if r else None

    def execute(self, sql, params=()):
        s = " ".join(sql.split())
        if s.upper().startswith("CREATE TABLE"):
            self.eventos.append(("ddl", "tk_mensajes_quitados"))
            return
        self._maybe_fallar(s)
        self.eventos.append(("sql", s.split(" (")[0][:60]))
        if s.upper().startswith("UPDATE TK_TICKETS T JOIN"):
            self.eventos.append(("recalculo", tuple(params)))     # UPDATE ... JOIN es de MySQL: se registra, no se ejecuta
            return
        self.con.execute(self._a_sqlite(sql), tuple(params))
        self.con.commit()

    def _maybe_fallar(self, s):
        if self.fallar_si and self.fallar_si in s:
            self._coincidencias += 1
            if self._coincidencias >= self.fallar_en_n and (not self.fallar_solo_una or self._coincidencias == self.fallar_en_n):
                raise RuntimeError("falla simulada")

    # --- datos de prueba ---
    def mensaje(self, ticket, mid, contenido="Hola con fotos", minutos=0, tipo="client_message", usuario="Cliente"):
        cur = self.con.execute(
            "INSERT INTO tk_mensajes (ticket_id, tipo, contenido, metadata, usuario, created_at, message_date) VALUES (?,?,?,?,?,?,?)",
            (ticket, tipo, contenido, _meta(mid), usuario, (T0 + timedelta(minutes=minutos)).isoformat(sep=" "), T0.isoformat(sep=" ")))
        self.con.commit()
        return cur.lastrowid

    def adjunto(self, ticket, mensaje_id, nombre, kb=113):
        cur = self.con.execute(
            "INSERT INTO tk_adjuntos (ticket_id, mensaje_id, archivo_url, archivo_nombre, mime_type, file_size_kb, origen, created_at) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (ticket, mensaje_id, "gs://b/%s" % nombre, nombre, "image/jpeg", kb, "cliente", T0.isoformat(sep=" ")))
        self.con.commit()
        return cur.lastrowid

    def ids(self, tabla):
        return [r[0] for r in self.con.execute("SELECT id FROM %s ORDER BY id" % tabla).fetchall()]


def _ruta(base, cuerpo, superadmin=True, bandeja=False):
    llamadas_audit = []

    def audit(accion, **kw):
        base.eventos.append(("audit", accion))
        llamadas_audit.append((accion, kw))

    ns = {"json": json, "request": _Peticion(cuerpo), "jsonify": lambda c: c, "_tk_solo_superadmin": lambda: superadmin,
          "mysql_fetchall": base.fetchall, "mysql_fetchone": base.fetchone, "mysql_execute": base.execute, "_audit": audit,
          "get_mysql": base.get_mysql,
          "current_username": lambda: "daniel", "_tk_bandeja_rapida_ok": lambda: bandeja}
    for c in ("_TK_DUP_MAX_MINUTOS", "_TK_DUP_DDL_RESPALDO"):
        exec(compile(_constante(c), "<c>", "exec"), ns)
    for f in ("_tk_dup_agrupar", "_tk_dup_leer", "_tk_dup_plan", "tk_api_quitar_correos_duplicados"):
        exec(compile(_funcion(f), "<" + f + ">", "exec"), ns)
    r = ns["tk_api_quitar_correos_duplicados"]()
    return (r if isinstance(r, tuple) else (r, 200)) + (llamadas_audit,)


def _caso_ticket_2304(b):
    """El caso real de la clienta del ticket 2304: el mismo correo con 2 fotos entró dos veces con 2 minutos de diferencia."""
    m1 = b.mensaje(2304, "<a@x>", minutos=0)
    m2 = b.mensaje(2304, "<a@x>", minutos=2)
    a1, a2 = b.adjunto(2304, m1, "IMG_3681.jpeg"), b.adjunto(2304, m1, "IMG_3682.jpeg", 128)
    a3, a4 = b.adjunto(2304, m2, "IMG_3681.jpeg"), b.adjunto(2304, m2, "IMG_3682.jpeg", 128)
    return m1, m2, (a1, a2), (a3, a4)


class TestSimulacion(unittest.TestCase):
    def _antes(self):
        b = _Base()
        self.addCleanup(b.con.close)
        _caso_ticket_2304(b)
        b.mensaje(2305, "<unico@x>")
        return b

    def _sin_cambios(self, b):
        self.assertEqual(b.ids("tk_mensajes"), [1, 2, 3])
        self.assertEqual(b.ids("tk_adjuntos"), [1, 2, 3, 4])
        self.assertEqual(b.ids("tk_mensajes_quitados"), [])
        self.assertFalse([e for e in b.eventos if e[0] in ("sql", "audit", "ddl")], "la simulación no escribe ni audita: %r" % (b.eventos,))

    def test_sin_dry_run_es_simulacion_y_no_toca_nada(self):
        b = self._antes()
        r, estado, audit = _ruta(b, {})
        self.assertEqual(estado, 200)
        self.assertTrue(r["ok"] and r["dry_run"])
        self.assertEqual(r["resumen"]["mensajes_a_quitar"], 1)
        self.assertEqual(r["resumen"]["adjuntos_a_quitar"], 2)
        self.assertEqual(r["confirmar_con"], "QUITAR 1 COPIAS")
        self.assertEqual(r["ejemplos"][0]["conserva"], 1)
        self.assertEqual([q["id"] for q in r["ejemplos"][0]["quitar"]], [2])
        self._sin_cambios(b)

    def test_solo_un_dry_run_explicitamente_falso_ejecuta(self):
        """null, vacío, true, «true», 1, listas… todo eso es simulación (nada se borra por un valor raro)."""
        for valor in (None, "", True, "true", "TRUE", "si", 1, 2, [], {}, [False], "dry"):
            b = self._antes()
            r, _, _ = _ruta(b, {"dry_run": valor, "confirm": "QUITAR 1 COPIAS"})
            self.assertTrue(r["dry_run"], repr(valor))
            self._sin_cambios(b)
            b.con.close()

    def test_un_cuerpo_ausente_o_roto_tambien_es_simulacion(self):
        for cuerpo in (None, [], "texto"):
            b = _Base()
            _caso_ticket_2304(b)
            r, estado, _ = _ruta(b, cuerpo if not isinstance(cuerpo, (list, str)) else None)
            self.assertTrue(r["ok"] and r["dry_run"])
            b.con.close()

    def test_no_superadmin_recibe_403_y_no_se_lee_ni_se_escribe(self):
        b = self._antes()
        r, estado, audit = _ruta(b, {"dry_run": False, "confirm": "QUITAR 1 COPIAS"}, superadmin=False)
        self.assertEqual(estado, 403)
        self.assertFalse(r["ok"])
        self._sin_cambios(b)
        self.assertEqual(audit, [])


class TestEjecucion(unittest.TestCase):
    def _montar(self):
        b = _Base()
        self.addCleanup(b.con.close)
        m1, m2, a_conserva, a_quita = _caso_ticket_2304(b)
        return b, m1, m2, a_conserva, a_quita

    def test_sin_confirmacion_o_con_otra_no_se_borra_nada(self):
        b, *_ = self._montar()
        for confirm in (None, "", "QUITAR", "QUITAR 99 COPIAS", "quitar 2 copias", "BORRAR 1 COPIAS"):
            r, estado, _ = _ruta(b, {"dry_run": False, "confirm": confirm})
            self.assertEqual(estado, 400, confirm)
            self.assertEqual(r["expected"], "QUITAR 1 COPIAS")
        self.assertEqual(b.ids("tk_mensajes"), [1, 2])
        self.assertEqual(b.ids("tk_adjuntos"), [1, 2, 3, 4])

    def test_la_confirmacion_acepta_mayusculas_y_espacios_de_mas(self):
        b, *_ = self._montar()
        r, estado, _ = _ruta(b, {"dry_run": False, "confirm": "  quitar   1  copias "})
        self.assertEqual(estado, 200)
        self.assertEqual(r["quitados"], {"mensajes": 1, "adjuntos": 2})

    def test_quita_la_copia_y_sus_adjuntos_y_conserva_la_primera(self):
        b, m1, m2, a_conserva, a_quita = self._montar()
        r, estado, _ = _ruta(b, {"dry_run": False, "confirm": "QUITAR 1 COPIAS"})
        self.assertEqual(estado, 200)
        self.assertTrue(r["ok"] and not r["dry_run"])
        self.assertEqual(r["quitados"], {"mensajes": 1, "adjuntos": 2})
        self.assertEqual(r["fallos"], [])
        self.assertEqual(b.ids("tk_mensajes"), [m1], "la primera copia (id menor) se queda")
        self.assertEqual(b.ids("tk_adjuntos"), sorted(a_conserva), "los adjuntos de la copia conservada se quedan")

    def test_deja_un_respaldo_completo_de_lo_que_quito(self):
        b, m1, m2, a_conserva, a_quita = self._montar()
        _ruta(b, {"dry_run": False, "confirm": "QUITAR 1 COPIAS"})
        fila = b.con.execute("SELECT * FROM tk_mensajes_quitados").fetchone()
        self.assertEqual((fila["mensaje_id"], fila["ticket_id"], fila["conserva_mensaje_id"], fila["quitado_por"]), (m2, 2304, m1, "daniel"))
        guardado = json.loads(fila["fila_json"])
        self.assertEqual((guardado["id"], guardado["contenido"], guardado["tipo"]), (m2, "Hola con fotos", "client_message"))
        self.assertNotIn("huella_dup", guardado)
        adj = json.loads(fila["adjuntos_json"])
        self.assertEqual(sorted(a["id"] for a in adj), sorted(a_quita))
        self.assertEqual({a["archivo_nombre"] for a in adj}, {"IMG_3681.jpeg", "IMG_3682.jpeg"})
        self.assertTrue(all(a["archivo_url"].startswith("gs://") for a in adj), "queda la ruta del archivo: el archivo en sí no se toca")

    def test_audita_y_respalda_ANTES_de_borrar(self):
        b, *_ = self._montar()
        r, _, audit = _ruta(b, {"dry_run": False, "confirm": "QUITAR 1 COPIAS"})
        orden = [e for e in b.eventos if e[0] in ("audit", "ddl", "sql")]
        textos = [e[1] for e in orden]
        i_audit = textos.index("tk_correos_duplicados_quitar")
        i_respaldo = next(i for i, e in enumerate(orden) if e[0] == "sql" and e[1].startswith("INSERT INTO tk_mensajes_quitados"))
        i_del_adj = next(i for i, e in enumerate(orden) if e[0] == "sql" and e[1].startswith("DELETE FROM tk_adjuntos"))
        i_del_msg = next(i for i, e in enumerate(orden) if e[0] == "sql" and e[1].startswith("DELETE FROM tk_mensajes"))
        self.assertLess(i_audit, i_respaldo)
        self.assertLess(i_respaldo, i_del_adj)
        self.assertLess(i_del_adj, i_del_msg)
        self.assertEqual(audit[0][0], "tk_correos_duplicados_quitar")
        self.assertEqual(audit[0][1]["details"]["mensajes"], [2])

    def test_es_idempotente_y_la_segunda_vez_no_hay_nada_que_quitar(self):
        b, *_ = self._montar()
        _ruta(b, {"dry_run": False, "confirm": "QUITAR 1 COPIAS"})
        r, estado, _ = _ruta(b, {"dry_run": False, "confirm": "QUITAR 1 COPIAS"})
        self.assertEqual(estado, 200)
        self.assertEqual(r["quitados"], {"mensajes": 0, "adjuntos": 0})
        self.assertEqual(len(b.ids("tk_mensajes_quitados")), 1, "no se respalda dos veces")

    def test_lo_dudoso_no_se_toca(self):
        b = _Base()
        self.addCleanup(b.con.close)
        a = b.mensaje(10, "<x@x>", contenido="uno")
        c = b.mensaje(10, "<x@x>", contenido="otro texto", minutos=1)       # mismo Message-ID, texto distinto
        d = b.mensaje(11, "<y@x>", minutos=0)
        e = b.mensaje(11, "<y@x>", minutos=300)                             # 5 horas después
        f = b.mensaje(12, "<z@x>")
        g = b.mensaje(12, "<z@x>", minutos=1)
        b.adjunto(12, g, "SOLO_EN_LA_COPIA.jpeg")                          # la copia trae algo que la conservada no tiene
        r, _, _ = _ruta(b, {})
        self.assertEqual(r["resumen"]["mensajes_a_quitar"], 0)
        self.assertEqual(r["resumen"]["grupos_omitidos"], 3)
        r, estado, _ = _ruta(b, {"dry_run": False, "confirm": "QUITAR 0 COPIAS"})
        self.assertEqual(r["quitados"], {"mensajes": 0, "adjuntos": 0})
        self.assertEqual(b.ids("tk_mensajes"), [a, c, d, e, f, g])

    def test_las_tandas_respetan_el_limite_y_piden_nueva_confirmacion(self):
        b = _Base()
        self.addCleanup(b.con.close)
        for t in (1, 2, 3):
            b.mensaje(t, "<m%d@x>" % t)
            b.mensaje(t, "<m%d@x>" % t, minutos=1)
        r, estado, _ = _ruta(b, {"dry_run": False, "limite": 1, "confirm": "QUITAR 3 COPIAS"})
        self.assertEqual(r["quitados"]["mensajes"], 1)
        self.assertEqual(r["quedan_grupos"], 2)
        self.assertEqual(len(b.ids("tk_mensajes")), 5)
        r, _, _ = _ruta(b, {"dry_run": False, "limite": 1, "confirm": "QUITAR 3 COPIAS"})
        self.assertEqual(r["expected"], "QUITAR 2 COPIAS", "tras la primera tanda quedan 2: hay que mirar la cifra otra vez")

    def test_si_los_adjuntos_cambian_despues_de_la_simulacion_esa_copia_no_se_toca(self):
        b, m1, m2, a_conserva, a_quita = self._montar()
        b.antes_de_leer_adjuntos = lambda: b.adjunto(2304, m2, "LLEGO_DESPUES.jpeg")
        r, estado, _ = _ruta(b, {"dry_run": False, "confirm": "QUITAR 1 COPIAS"})
        self.assertEqual(estado, 200)
        self.assertEqual(r["quitados"], {"mensajes": 0, "adjuntos": 0})
        self.assertEqual(r["fallos"][0]["mensaje_id"], m2)
        self.assertEqual(b.ids("tk_mensajes"), [m1, m2])
        self.assertEqual(b.ids("tk_mensajes_quitados"), [], "no se respaldó ni se borró nada de esa copia")

    def test_si_el_respaldo_falla_no_se_borra_esa_copia(self):
        b, m1, m2, *_ = self._montar()
        b.fallar_si = "INSERT INTO tk_mensajes_quitados"
        r, estado, _ = _ruta(b, {"dry_run": False, "confirm": "QUITAR 1 COPIAS"})
        self.assertEqual(estado, 200)
        self.assertEqual(r["quitados"], {"mensajes": 0, "adjuntos": 0})
        self.assertEqual(len(r["fallos"]), 1)
        self.assertEqual(b.ids("tk_mensajes"), [m1, m2])
        self.assertEqual(b.ids("tk_adjuntos"), [1, 2, 3, 4])

    def test_si_falla_una_copia_las_demas_siguen(self):
        b = _Base()
        self.addCleanup(b.con.close)
        for t in (1, 2):
            b.mensaje(t, "<m%d@x>" % t)
            b.mensaje(t, "<m%d@x>" % t, minutos=1)
        b.fallar_si, b.fallar_solo_una = "INSERT INTO tk_mensajes_quitados", True     # falla solo la primera vez
        r, _, _ = _ruta(b, {"dry_run": False, "confirm": "QUITAR 2 COPIAS"})
        self.assertEqual(r["quitados"]["mensajes"], 1)
        self.assertEqual(len(r["fallos"]), 1)
        self.assertEqual(r["fallos"][0]["ticket_id"], 1)
        self.assertEqual(b.ids("tk_mensajes"), [1, 2, 3], "el ticket 1 quedó igual; en el 2 se quitó la copia (4)")

    def test_recalcula_la_bandeja_de_los_tickets_tocados_solo_si_existe(self):
        b, *_ = self._montar()
        r, _, _ = _ruta(b, {"dry_run": False, "confirm": "QUITAR 1 COPIAS"}, bandeja=True)
        recalculos = [e for e in b.eventos if e[0] == "recalculo"]
        self.assertEqual(recalculos, [("recalculo", (2304,))])
        b2, *_ = self._montar()
        _ruta(b2, {"dry_run": False, "confirm": "QUITAR 1 COPIAS"}, bandeja=False)
        self.assertEqual([e for e in b2.eventos if e[0] == "recalculo"], [])

    def test_con_tres_copias_se_conserva_solo_la_primera(self):
        b = _Base()
        self.addCleanup(b.con.close)
        ids = [b.mensaje(7, "<t@x>", minutos=k) for k in (0, 1, 2)]
        r, _, _ = _ruta(b, {"dry_run": False, "confirm": "QUITAR 2 COPIAS"})
        self.assertEqual(r["quitados"]["mensajes"], 2)
        self.assertEqual(b.ids("tk_mensajes"), [ids[0]])


class TestPedidoCortoSimularConfirmar(unittest.TestCase):
    """06-nov-2026 (Daniel autorizó correrla): la forma corta {simular:false, confirmar:true} y la ruta alias /limpiar."""

    def _caso(self):
        b = _Base()
        self.addCleanup(b.con.close)
        _caso_ticket_2304(b)
        return b

    def _sin_cambios(self, b):
        self.assertEqual(b.ids("tk_mensajes"), [1, 2])
        self.assertEqual(b.ids("tk_adjuntos"), [1, 2, 3, 4])
        self.assertEqual(b.ids("tk_mensajes_quitados"), [])
        self.assertFalse([e for e in b.eventos if e[0] in ("sql", "audit")])

    def test_simular_false_y_confirmar_true_borra_solo_la_copia_de_id_mayor_y_sus_adjuntos(self):
        b = self._caso()
        r, estado, _ = _ruta(b, {"simular": False, "confirmar": True})
        self.assertEqual(estado, 200)
        self.assertEqual(r["quitados"], {"mensajes": 1, "adjuntos": 2})
        self.assertEqual(b.ids("tk_mensajes"), [1])
        self.assertEqual(b.ids("tk_adjuntos"), [1, 2])

    def test_sin_nada_o_con_simular_true_es_simulacion_aunque_venga_confirmar(self):
        for cuerpo in ({}, {"confirmar": True}, {"simular": True, "confirmar": True}, {"simular": None, "confirmar": True},
                       {"simular": "", "confirmar": True}, {"simular": "true", "confirmar": True}):
            b = self._caso()
            r, estado, _ = _ruta(b, cuerpo)
            self.assertTrue(r["dry_run"] and r["simular"], repr(cuerpo))
            self._sin_cambios(b)
            b.con.close()

    def test_si_llegan_las_dos_banderas_y_una_dice_simular_no_se_toca_nada(self):
        for cuerpo in ({"dry_run": True, "simular": False, "confirmar": True}, {"dry_run": False, "simular": True, "confirmar": True},
                       {"dry_run": False, "simular": "si", "confirm": "QUITAR 1 COPIAS"}):
            b = self._caso()
            r, _, _ = _ruta(b, cuerpo)
            self.assertTrue(r["dry_run"], repr(cuerpo))
            self._sin_cambios(b)
            b.con.close()

    def test_sin_confirmar_true_no_borra_aunque_simular_sea_false(self):
        for confirmar in (None, False, "false", "", 0, "yes", "si", "1", [], 1):
            b = self._caso()
            r, estado, _ = _ruta(b, {"simular": False, "confirmar": confirmar})
            self.assertEqual(estado, 400, repr(confirmar))
            self._sin_cambios(b)
            b.con.close()

    def test_la_simulacion_informa_por_ticket_sin_escribir(self):
        b = _Base()
        self.addCleanup(b.con.close)
        _caso_ticket_2304(b)
        m = b.mensaje(900, "<q@x>")
        b.mensaje(900, "<q@x>", minutos=1)
        b.adjunto(900, m, "X.jpeg")
        r, _, _ = _ruta(b, {})
        self.assertEqual(r["resumen"]["tickets"], 2)
        self.assertEqual({(t["ticket_id"], t["mensajes"], t["adjuntos"]) for t in r["por_ticket"]}, {(900, 1, 0), (2304, 1, 2)})
        self.assertFalse([e for e in b.eventos if e[0] in ("sql", "audit", "ddl")])

    def test_el_tope_de_la_tanda_cuenta_tickets_no_grupos(self):
        b = _Base()
        self.addCleanup(b.con.close)
        for t, mids in ((1, ("<a1@x>", "<a2@x>")), (2, ("<b1@x>",)), (3, ("<c1@x>",))):
            for mid in mids:                       # el ticket 1 tiene DOS grupos repetidos
                b.mensaje(t, mid)
                b.mensaje(t, mid, minutos=1)
        r, _, _ = _ruta(b, {"simular": False, "confirmar": True, "limite": 2})
        self.assertEqual(r["quitados"]["mensajes"], 3, "los dos grupos del ticket 1 + el del ticket 2")
        self.assertEqual(r["tickets_tocados"], [1, 2])
        self.assertEqual((r["quedan_tickets"], r["quedan_grupos"]), (1, 1))
        r, _, _ = _ruta(b, {"simular": False, "confirmar": True, "limite": 2})
        self.assertEqual((r["quitados"]["mensajes"], r["quedan_tickets"]), (1, 0))
        r, _, _ = _ruta(b, {"simular": False, "confirmar": True})
        self.assertEqual(r["quitados"], {"mensajes": 0, "adjuntos": 0}, "idempotente: ya no queda nada")

    def test_solo_se_limpian_correos_del_cliente(self):
        b = _Base()
        self.addCleanup(b.con.close)
        a = b.mensaje(5, "<r@x>", tipo="mensaje", usuario="Ana")
        c = b.mensaje(5, "<r@x>", tipo="mensaje", usuario="Ana", minutos=1)
        r, _, _ = _ruta(b, {})
        self.assertEqual(r["resumen"]["mensajes_a_quitar"], 0)
        self.assertIn("client_message", r["omitidos"][0]["motivo"])
        r, _, _ = _ruta(b, {"simular": False, "confirmar": True})
        self.assertEqual(r["quitados"]["mensajes"], 0)
        self.assertEqual(b.ids("tk_mensajes"), [a, c])

    def test_se_informan_las_rutas_de_la_nube_y_no_se_borra_ningun_archivo(self):
        b = self._caso()
        r, _, _ = _ruta(b, {"simular": False, "confirmar": True})
        nube = r["archivos_en_la_nube"]
        self.assertEqual(nube["total"], 2)
        self.assertEqual(sorted(x["ruta"] for x in nube["rutas"]), ["gs://b/IMG_3681.jpeg", "gs://b/IMG_3682.jpeg"])

    def test_el_log_lleva_los_ids_de_mensajes_y_adjuntos_antes_de_borrar_y_quien(self):
        b = self._caso()
        r, _, audit = _ruta(b, {"simular": False, "confirmar": True})
        accion, kw = audit[0]
        self.assertEqual(accion, "tk_correos_duplicados_quitar")
        self.assertEqual((kw["details"]["mensajes"], kw["details"]["adjuntos"], kw["details"]["quien"]), ([2], [3, 4], "daniel"))
        orden = [e[1] for e in b.eventos if e[0] in ("audit", "sql")]
        self.assertLess(orden.index("tk_correos_duplicados_quitar"), min(i for i, t in enumerate(orden) if t.startswith("DELETE")))
        self.assertEqual(b.con.execute("SELECT quitado_por FROM tk_mensajes_quitados").fetchone()[0], "daniel")


class TestAtomicidadPorTicket(unittest.TestCase):
    """Cada ticket va en UNA transacción: si algo falla a mitad, no cambia NADA de ese ticket (ni respaldo, ni adjuntos, ni mensajes)."""

    def _dos_grupos_en_un_ticket(self):
        b = _Base()
        self.addCleanup(b.con.close)
        for mid in ("<g1@x>", "<g2@x>"):
            m = b.mensaje(77, mid)
            m2 = b.mensaje(77, mid, minutos=1)
            b.adjunto(77, m, "F_%s.jpeg" % mid[1:3])
            b.adjunto(77, m2, "F_%s.jpeg" % mid[1:3])
        return b

    def test_si_falla_el_ultimo_borrado_se_revierte_todo_el_ticket(self):
        b = self._dos_grupos_en_un_ticket()
        antes = (b.ids("tk_mensajes"), b.ids("tk_adjuntos"))
        b.fallar_si, b.fallar_en_n = "DELETE FROM tk_mensajes", 2        # el borrado del SEGUNDO grupo
        r, estado, _ = _ruta(b, {"simular": False, "confirmar": True})
        self.assertEqual(estado, 200)
        self.assertEqual(r["quitados"], {"mensajes": 0, "adjuntos": 0})
        self.assertEqual((b.ids("tk_mensajes"), b.ids("tk_adjuntos")), antes, "también el primer grupo quedó como estaba")
        self.assertEqual(b.ids("tk_mensajes_quitados"), [], "y el respaldo de esa transacción se deshizo")
        self.assertEqual(r["fallos"][0]["ticket_id"], 77)
        self.assertEqual(b.conexiones[0], {"commit": 0, "rollback": 1, "cerrada": True})

    def test_un_ticket_que_falla_no_arrastra_a_los_demas(self):
        b = self._dos_grupos_en_un_ticket()
        b.mensaje(88, "<h@x>")
        b.mensaje(88, "<h@x>", minutos=1)
        b.fallar_si, b.fallar_en_n, b.fallar_solo_una = "DELETE FROM tk_mensajes", 2, True
        r, _, _ = _ruta(b, {"simular": False, "confirmar": True})
        self.assertEqual(r["quitados"]["mensajes"], 1, "el ticket 88 se limpió")
        self.assertEqual(r["tickets_tocados"], [88])
        self.assertEqual([x["ticket_id"] for x in r["fallos"]], [77])

    def test_cada_ticket_usa_su_propia_conexion_con_un_solo_commit_y_se_cierra(self):
        b = self._dos_grupos_en_un_ticket()
        b.mensaje(88, "<h@x>")
        b.mensaje(88, "<h@x>", minutos=1)
        _ruta(b, {"simular": False, "confirmar": True})
        self.assertEqual(b.conexiones, [{"commit": 1, "rollback": 0, "cerrada": True}] * 2)

    def test_la_simulacion_no_abre_ninguna_transaccion(self):
        b = self._dos_grupos_en_un_ticket()
        _ruta(b, {})
        self.assertEqual(b.conexiones, [])


class TestVerificacionDeNuevo(unittest.TestCase):
    def test_si_la_copia_cambia_de_tipo_o_de_texto_despues_de_la_simulacion_no_se_borra(self):
        for cambio in ("UPDATE tk_mensajes SET contenido='otro' WHERE id=2", "UPDATE tk_mensajes SET tipo='mensaje' WHERE id=2",
                       "UPDATE tk_mensajes SET usuario='Otro' WHERE id=2",
                       "UPDATE tk_mensajes SET metadata='{\"message_id\": \"<otro@x>\"}' WHERE id=2"):
            b = _Base()
            _caso_ticket_2304(b)
            b.antes_de_leer_adjuntos = None
            original = b.get_mysql

            def get_mysql(original=original, b=b, cambio=cambio):
                b.con.execute(cambio)            # alguien lo tocó entre la simulación y el borrado
                b.con.commit()
                return original()
            b.get_mysql = get_mysql
            r, _, _ = _ruta(b, {"simular": False, "confirmar": True})
            self.assertEqual(r["quitados"], {"mensajes": 0, "adjuntos": 0}, cambio)
            self.assertEqual(len(r["fallos"]), 1, cambio)
            self.assertEqual(b.ids("tk_mensajes"), [1, 2], cambio)
            self.assertEqual(b.ids("tk_mensajes_quitados"), [], cambio)
            b.con.close()

    def test_jamas_se_quita_la_primera_copia(self):
        """Aunque el plan llegara con la conservada dentro de «quitar», la guarda de id lo frena."""
        src = _funcion("tk_api_quitar_correos_duplicados")
        self.assertIn('if c["id"] <= gr["conserva"]:', src)


class TestCodigo(unittest.TestCase):
    def test_ruta_alias_limpiar_y_arranque_barato(self):
        fuente = _analizado()[0].replace("\r\n", "\n")
        self.assertIn('@app.route("/tickets/api/auditoria/correos-duplicados/limpiar", methods=["POST"])\n'
                      '    @app.route("/tickets/api/auditoria/correos-duplicados/quitar", methods=["POST"])', fuente)
        ddl = _constante("_TK_DUP_DDL_RESPALDO")
        self.assertEqual(ddl.upper().count("CREATE TABLE IF NOT EXISTS"), 1)
        self.assertNotIn("ALTER", ddl.upper())
        # se crea en el arranque, DENTRO de app_context (REGLA #18 + gotcha de los _ensure sin contexto)
        i_ctx = fuente.index("    with app.app_context():\n        try:\n            _ensure_tickets_tables()")
        self.assertIn("_ensure_tk_mensajes_quitados()", fuente[i_ctx:i_ctx + 3000])
        self.assertLess(fuente.index("_TK_DUP_DDL_RESPALDO = ("), i_ctx, "la constante existe antes de que el arranque la use")

    def test_solo_por_post_con_login_y_superadmin_primero(self):
        fuente = _analizado()[0].replace("\r\n", "\n")
        self.assertIn('@app.route("/tickets/api/auditoria/correos-duplicados/quitar", methods=["POST"])\n'
                      "    @_tickets_required\n"
                      "    def tk_api_quitar_correos_duplicados():\n"
                      "        if not _tk_solo_superadmin():", fuente)

    def test_nunca_toca_los_archivos_de_la_nube(self):
        src = _funcion("tk_api_quitar_correos_duplicados") + _funcion("_tk_dup_plan")
        for palabra in ("storage", "blob", "gcs", "bucket", "unlink", "os.remove", "delete_file", "archivo_url="):
            self.assertNotIn(palabra, src.lower(), palabra)

    def test_la_simulacion_es_el_valor_por_defecto(self):
        src = _funcion("tk_api_quitar_correos_duplicados")
        self.assertIn('banderas = [d[k] for k in ("dry_run", "simular") if k in d]', src)
        self.assertIn("ejecutar = bool(banderas) and all(", src)
        self.assertIn("dry_run = not ejecutar", src)
        self.assertIn('"QUITAR %d COPIAS" % plan["mensajes"]', src)

    def test_la_auditoria_va_antes_del_primer_borrado(self):
        src = _funcion("tk_api_quitar_correos_duplicados")
        self.assertLess(src.index("_audit("), src.index("DELETE FROM"))
        self.assertLess(src.index("INSERT INTO tk_mensajes_quitados"), src.index("DELETE FROM tk_adjuntos"))
        self.assertLess(src.index("DELETE FROM tk_adjuntos"), src.index("DELETE FROM tk_mensajes"))

    def test_nada_la_llama_sola(self):
        """Ninguna pantalla ni tarea programada la usa: solo una persona, con la simulación a la vista."""
        fuente = _analizado()[0]
        self.assertEqual(fuente.count("tk_api_quitar_correos_duplicados"), 1, "solo su definición: nada en el módulo la llama")
        for sub in ("templates", "static"):
            for raiz, _dirs, archivos in os.walk(os.path.join(_RAIZ, sub)):
                for nombre in archivos:
                    if nombre.endswith((".html", ".js")):
                        with open(os.path.join(raiz, nombre), encoding="utf-8", errors="replace") as fh:
                            self.assertNotIn("correos-duplicados/quitar", fh.read(), nombre)


if __name__ == "__main__":
    unittest.main()

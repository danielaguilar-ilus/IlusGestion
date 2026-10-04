"""Repuestos «circular» (2026-10-04, Daniel: "supongamos que pedimos 10 repuestos.
Pueden terminar 9 almacenados y uno se puede gestionar la instalación directamente
con el cliente" + "por trazabilidad tendríamos que recibir las 10 y ahí gestionar la
orden de trabajo de instalación para consumir las recibidas").

Prueba, extrayendo las funciones de app.py con ast (importar app.py levanta Flask,
la base y los crons), con dobles de BD y de request:
  - repstock_solicitud_ot_repartir: solo gestión, solo 'recibido', nunca una
    reposición, cantidad entre 0 y el total (exclusivos); la parte de bodega nace
    como hija (reposición 'recibido') y la madre queda por lo que se instala, todo
    en una transacción con candado de fila; concurrencia -> 409 sin tocar nada.
  - _otrep_resolver_ticket_si_corresponde: una reposición recibida ya no deja el
    ticket abierto para siempre.
  - Migración: columna e índice en sentencias separadas (REGLA #18).
  - Plantilla: el modal de reparto, el historial y el responsable de las manuales.

Correr con:  py -m unittest tests.test_repuestos_repartir
"""
import ast
import json
import os
import re
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
APP_PY = os.path.join(RAIZ, "app.py")
REP_HTML = os.path.join(RAIZ, "templates", "clientes_hub", "repuestos.html")

_FUNCIONES = ("repstock_solicitud_ot_repartir", "_otrep_resolver_ticket_si_corresponde")
_NODOS = {}


def _src():
    with open(APP_PY, encoding="utf-8") as fh:
        return fh.read()


def _nodos():
    if not _NODOS:
        arbol = ast.parse(_src())
        for n in arbol.body:
            if isinstance(n, ast.FunctionDef) and n.name in _FUNCIONES:
                n.decorator_list = []
                _NODOS[n.name] = n
    return _NODOS


def _cargar(nombre, extra):
    amb = {"json": json}
    amb.update(extra)
    exec(compile(ast.Module(body=[_nodos()[nombre]], type_ignores=[]), "<app>", "exec"), amb)
    return amb[nombre]


class _Conflicto(Exception):
    pass


class _Req:
    def __init__(self, body):
        self._b = body

    def get_json(self, silent=True):
        return self._b


class _Cur:
    def __init__(self, conn):
        self.c = conn
        self.rowcount = 0
        self.lastrowid = None
        self._ultimo = None

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=()):
        self.c.sqls.append((" ".join(sql.split()), params))
        self._ultimo = sql
        if "FOR UPDATE" in sql:
            self.rowcount = 1
        elif sql.strip().startswith("INSERT INTO mant_ot_repuesto_solicitudes"):
            self.lastrowid = 777
            self.rowcount = 1
        elif sql.strip().startswith("UPDATE mant_ot_repuesto_solicitudes"):
            self.rowcount = self.c.update_rowcount
        else:
            self.rowcount = 1

    def fetchone(self):
        return self.c.fila_lock


class _Conn:
    def __init__(self, fila_lock, update_rowcount=1):
        self.fila_lock = fila_lock
        self.update_rowcount = update_rowcount
        self.sqls = []
        self.commits = 0
        self.rollbacks = 0

    def cursor(self):
        return _Cur(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


def _sol(**kw):
    s = {"id": 50, "estado": "recibido", "cantidad": 10, "cantidad_recibida": 10, "es_reposicion": 0,
         "cliente_id": 3, "visita_id": 900, "incidencia_id": None, "ticket_id": 40,
         "repuesto_stock_id": 12, "repuesto_nombre": "Piola trotadora", "ot_generada_id": None}
    s.update(kw)
    return s


class TestRepartir(unittest.TestCase):

    def _llamar(self, body, sol=None, tecnico=False, conn=None, entrada=True):
        sol = sol if sol is not None else _sol()
        conn = conn or _Conn({"estado": sol.get("estado"), "cantidad": sol.get("cantidad"),
                              "cantidad_recibida": sol.get("cantidad_recibida")})
        eventos, mensajes = [], []

        def fetchone(sql, params=()):
            if "FROM mant_ot_repuesto_solicitudes WHERE id=%s" in sql:
                return dict(sol) if sol else None
            if "FROM mant_clientes" in sql:
                return {"razon_social": "Gimnasio Uno"}
            if "mant_repuestos_movimientos" in sql:
                return {"x": 1} if entrada else None
            return None

        f = _cargar("repstock_solicitud_ot_repartir", {
            "request": _Req(body), "jsonify": lambda d: d, "_es_rol_tecnico": lambda: tecnico,
            "mysql_fetchone": fetchone, "get_db": lambda: conn, "current_username": lambda: "Daniel",
            "_OtrepConflictoEstado": _Conflicto,
            "_otrep_evento": lambda *a, **k: eventos.append(a),
            "mysql_execute": lambda sql, p=(): mensajes.append((sql, p)),
            "_mant_log": lambda *a, **k: None, "_inc_log": lambda *a, **k: None,
        })
        r = f(50)
        cuerpo, http = (r if isinstance(r, tuple) else (r, 200))
        return cuerpo, http, conn, eventos, mensajes

    def test_tecnico_no_reparte(self):
        c, http, conn, _, _ = self._llamar({"instalar": 1}, tecnico=True)
        self.assertEqual(http, 403)
        self.assertEqual(conn.sqls, [])

    def test_solo_recibido(self):
        for est in ("solicitado", "validado", "pedido", "instalado", "rechazado"):
            c, http, conn, _, _ = self._llamar({"instalar": 1}, sol=_sol(estado=est))
            self.assertEqual(http, 400, est)
            self.assertEqual(conn.sqls, [], est)

    def test_reposicion_no_se_reparte(self):
        c, http, conn, _, _ = self._llamar({"instalar": 1}, sol=_sol(es_reposicion=1))
        self.assertEqual(http, 400)
        self.assertEqual(conn.sqls, [])

    def test_cantidad_fuera_de_rango(self):
        for v in (0, -1, 10, 11, "abc", None, "nan", "inf"):
            c, http, conn, _, _ = self._llamar({"instalar": v})
            self.assertEqual(http, 400, v)
            self.assertEqual(conn.sqls, [], v)

    def test_reparto_10_en_1_y_9(self):
        c, http, conn, eventos, mensajes = self._llamar({"instalar": 1})
        self.assertEqual(http, 200)
        self.assertTrue(c["ok"])
        self.assertEqual((c["instalar"], c["bodega"], c["hija_id"]), (1, 9, 777))
        self.assertEqual(conn.commits, 1)
        self.assertEqual(conn.rollbacks, 0)
        sqls = [s for s, _ in conn.sqls]
        # 1) candado de fila antes de tocar nada
        self.assertIn("FOR UPDATE", sqls[0])
        # 2) la hija: reposición recibida, ligada a la madre, por 9, con lo recibido
        ins = next((s, p) for s, p in conn.sqls if s.startswith("INSERT INTO mant_ot_repuesto_solicitudes"))
        self.assertIn("solicitud_padre_id", ins[0])
        self.assertIn("'recibido'", ins[0])
        self.assertRegex(ins[0], r"recibido_at, %s, 1, lote_id")  # resuelto_por, es_reposicion=1
        self.assertEqual(ins[1][0], 9)        # cantidad hija
        self.assertEqual(ins[1][1], 9)        # cantidad_recibida hija
        self.assertEqual(ins[1][-1], 50)      # WHERE id = madre
        # La hija no hereda cliente/equipo/OT/ticket (no deja alertas ni tickets abiertos)
        cols = ins[0].split("(", 1)[1].split(")", 1)[0]
        for c_ in ("cliente_id", "maquina_id", "visita_id", "ticket_id", "ot_generada_id"):
            self.assertNotIn(c_, cols)
        # 3) la madre queda por lo que se instala, solo si sigue recibida y con el mismo total
        upd = next((s, p) for s, p in conn.sqls if s.startswith("UPDATE mant_ot_repuesto_solicitudes"))
        self.assertIn("estado='recibido' AND cantidad=%s", upd[0])
        self.assertEqual(upd[1][0], 1)
        self.assertEqual(upd[1][1], 1.0)      # cantidad_recibida madre = 10 - 9
        # 4) evidencia compartida
        self.assertTrue(any("INSERT INTO mant_ot_repuesto_evidencias" in s for s in sqls))
        # 5) bitácora en las dos y nota en el ticket de la madre
        acciones = {(e[0], e[1]) for e in eventos}
        self.assertIn((50, "repartir"), acciones)
        self.assertIn((777, "creada"), acciones)
        self.assertTrue(any("tk_mensajes" in s and p[0] == 40 for s, p in mensajes))
        self.assertIsNone(c["aviso"])

    def test_kardex_no_se_mueve(self):
        c, http, conn, _, _ = self._llamar({"instalar": 3})
        self.assertFalse(any("mant_repuestos_movimientos" in s or "mant_repuestos_stock" in s
                             for s, _ in conn.sqls))

    def test_sin_entrada_en_kardex_avisa(self):
        c, http, _, _, _ = self._llamar({"instalar": 2}, entrada=False)
        self.assertEqual(http, 200)
        self.assertIn("kardex", c["aviso"])

    def test_concurrencia_otro_cambio_409(self):
        conn = _Conn({"estado": "instalado", "cantidad": 10, "cantidad_recibida": 10})
        c, http, conn, eventos, _ = self._llamar({"instalar": 1}, conn=conn)
        self.assertEqual(http, 409)
        self.assertEqual(conn.commits, 0)
        self.assertEqual(conn.rollbacks, 1)
        self.assertEqual(eventos, [])

    def test_concurrencia_cantidad_cambio_409(self):
        conn = _Conn({"estado": "recibido", "cantidad": 8, "cantidad_recibida": 8})
        c, http, conn, _, _ = self._llamar({"instalar": 1}, conn=conn)
        self.assertEqual(http, 409)
        self.assertEqual(conn.rollbacks, 1)

    def test_update_madre_no_calza_409(self):
        conn = _Conn({"estado": "recibido", "cantidad": 10, "cantidad_recibida": 10}, update_rowcount=0)
        c, http, conn, _, _ = self._llamar({"instalar": 1}, conn=conn)
        self.assertEqual(http, 409)
        self.assertEqual(conn.commits, 0)


class TestTicketReposicion(unittest.TestCase):

    def test_reposicion_recibida_no_deja_ticket_abierto(self):
        consultas = []

        def fetchone(sql, p=()):
            consultas.append(sql)
            return {"n": 0}

        f = _cargar("_otrep_resolver_ticket_si_corresponde", {
            "mysql_fetchone": fetchone, "mysql_execute_returning_rowcount": lambda *a: 1,
            "mysql_execute": lambda *a: None})
        f(40, "Daniel")
        self.assertIn("NOT (COALESCE(es_reposicion,0)=1 AND estado='recibido')", " ".join(consultas[0].split()))


class TestFuente(unittest.TestCase):

    def test_migracion_una_clausula_por_sentencia(self):
        src = _src()
        self.assertIn("ADD COLUMN solicitud_padre_id INT NULL", src)
        self.assertIn("ADD INDEX idx_otrep_padre (solicitud_padre_id)\")", src)
        self.assertNotRegex(src, r"solicitud_padre_id INT NULL[^\"]*\"\s*\"[^\"]*ADD INDEX")

    def test_recibir_compra_ofrece_repartir(self):
        src = _src()
        i = src.index("def repstock_compra_recibir")
        cuerpo = src[i:src.index("\n@app.route", i)]
        self.assertIn('"repartibles": repartibles', cuerpo)
        self.assertIn("COALESCE(s.es_reposicion,0)=0", cuerpo)

    def test_manual_nace_con_ticket_y_responsable(self):
        src = _src()
        i = src.index("def repstock_solicitud_manual")
        cuerpo = src[i:src.index("\n@app.route", i)]
        self.assertIn("_otrep_ticket_para_lote_manual(", cuerpo)
        self.assertIn("_otrep_responsable_pedido(d.get(\"responsable\"), user)", cuerpo)

    def _cuerpo(self, nombre):
        src = _src()
        i = src.index("def " + nombre + "(")
        j = src.find("\n@app.route", i)
        k = src.find("\ndef ", i + 5)
        fin = min(x for x in (j, k) if x > 0)
        return src[i:fin]

    def test_regla19_historial_sin_proveedor_ni_costo_para_tecnico(self):
        # Revisión 2026-10-04: los eventos de compra/costo no llegan a un técnico y el
        # detalle del resto (que puede nombrar proveedor/OC) se vacía.
        cuerpo = self._cuerpo("repstock_solicitudes_ot_listar")
        self.assertIn("_ev_privado = _oculta_proveedores()", cuerpo)
        self.assertIn('ev.get("accion") in ("compra", "costo")', cuerpo)
        self.assertIn('_ev["detalle"] = None', cuerpo)

    def test_instalar_no_usa_cantidad_vieja(self):
        # Carrera repartir <-> instalar: el UPDATE exige la misma cantidad leída.
        cuerpo = self._cuerpo("_otrep_cambiar_estado")
        self.assertIn("WHERE id=%s AND estado=%s AND cantidad=%s", cuerpo)
        self.assertIn('(actual, s.get("cantidad"))', cuerpo)

    def test_hija_no_cambia_de_estado(self):
        cuerpo = self._cuerpo("_otrep_cambiar_estado")
        i = cuerpo.index('if s.get("solicitud_padre_id"):')
        # el freno va antes de cualquier escritura
        self.assertLess(i, cuerpo.index("cur.execute("))
        fila = self._cuerpo("_otrep_fila")
        self.assertIn('s["siguientes"] = []', fila)

    def test_crear_ticket_freno_de_hija_antes_que_incidencia(self):
        cuerpo = self._cuerpo("repstock_solicitud_ot_ticket")
        self.assertLess(cuerpo.index('if s.get("solicitud_padre_id"):'), cuerpo.index('if s.get("incidencia_id"):'))
        self.assertIn("Esta solicitud ya está cerrada", cuerpo)

    def test_ticket_manual_duplicado_se_cancela(self):
        cuerpo = self._cuerpo("_otrep_ticket_para_lote_manual")
        self.assertIn("if not tomadas:", cuerpo)
        self.assertIn("estado='cancelado'", cuerpo)

    def test_hija_sin_ot_de_instalacion(self):
        self.assertIn('if s.get("solicitud_padre_id"):', self._cuerpo("repstock_solicitud_ot_preparar_ot"))

    def test_plantilla(self):
        with open(REP_HTML, encoding="utf-8") as fh:
            html = fh.read()
        self.assertIn('id="rsModalRepartir"', html)
        self.assertIn("/repartir'", html)
        self.assertIn("rsRepartirTrasRecibir(j.repartibles)", html)
        self.assertIn("function rsHistorial(", html)
        self.assertIn('id="rsmResp"', html)
        self.assertIn("fd.append('responsable', responsable)", html)
        # REGLA #1: nada de diálogos nativos en lo nuevo
        nuevo = html[html.index("Repartir lo recibido (instalar / bodega) ══"):html.index("Fase 4 (2026-09-26): UNA OT para VARIAS")]
        self.assertNotRegex(nuevo, r"\b(alert|confirm|prompt)\(")


if __name__ == "__main__":
    unittest.main()

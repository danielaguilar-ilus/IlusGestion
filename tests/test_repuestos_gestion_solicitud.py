"""Gestión de solicitudes de repuesto (2026-10-04, Daniel: "gestionar y ordenar
la solicitud... si el repuesto no existe lo tiene que crear... validar el
proveedor... para solicitar en lotes a proveedor" + "no sé qué tiene pedido
Juan Pablo, no sé en qué estado está").

Prueba, extrayendo las funciones de app.py con ast (importar app.py levanta
Flask, la base y los crons), con dobles de BD y de request:
  - _otrep_filtros_query: filtro por responsable (el del TICKET), "mías",
    "sin responsable", solicitante, búsqueda ampliada (proveedor solo para
    gestión) y reposición recibida = cerrada.
  - _otrep_cambiar_estado: validar exige proveedor (salvo técnico, REGLA #19)
    y recibir exige que el repuesto tenga ubicación.
  - repstock_crear: «por llegar» sin ubicación solo para gestión y con proveedor;
    el alta normal sigue exigiendo ubicación.

Correr con:  py -m unittest tests.test_repuestos_gestion_solicitud
"""
import ast
import os
import re
import unittest
from datetime import datetime, timezone

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
APP_PY = os.path.join(RAIZ, "app.py")

_FUNCIONES = ("_otrep_filtros_query", "_otrep_cambiar_estado", "repstock_crear")
_CONSTANTES = ("_OTREP_TRANSICIONES", "_OTREP_TRANSICIONES_GESTION", "_OTREP_ESTADO_LABEL",
               "_OTREP_ESTADOS", "_OTREP_ABIERTOS")
_NODOS = {}


def _nodos():
    if not _NODOS:
        with open(APP_PY, encoding="utf-8") as fh:
            arbol = ast.parse(fh.read())
        for n in arbol.body:
            if isinstance(n, ast.FunctionDef) and n.name in _FUNCIONES:
                n.decorator_list = []
                _NODOS[n.name] = n
            elif isinstance(n, ast.Assign) and any(getattr(t, "id", "") in _CONSTANTES for t in n.targets):
                _NODOS[n.targets[0].id] = n
    return _NODOS


def _cargar(nombre, extra):
    amb = {"re": re, "datetime": datetime, "timezone": timezone}
    for c in _CONSTANTES:
        if c in _nodos():
            exec(compile(ast.Module(body=[_nodos()[c]], type_ignores=[]), "<app>", "exec"), amb)
    amb.update(extra)
    exec(compile(ast.Module(body=[_nodos()[nombre]], type_ignores=[]), "<app>", "exec"), amb)
    return amb[nombre]


class _Args(dict):
    def get(self, k, default=None):
        return super().get(k, default)


class _Req:
    def __init__(self, args=None, json=None):
        self.args = _Args(args or {})
        self._json = json

    def get_json(self, silent=True):
        return self._json


class TestFiltrosPorPersona(unittest.TestCase):

    def _where(self, args, tecnico=False, oculta=False, yo="Juan Pablo"):
        f = _cargar("_otrep_filtros_query", {
            "request": _Req(args), "_es_rol_tecnico": lambda: tecnico,
            "_oculta_proveedores": lambda: oculta, "current_username": lambda: yo,
            "_otrep_resp_asegurar": lambda: True, "_otrep_cfg": lambda: {"responsable_defecto": "Daniel"}})
        return f()

    def test_responsable_es_el_efectivo(self):
        # 2026-10-04: el del ticket, si no el propio de la solicitud, si no el por defecto.
        w, p = self._where({"responsable": "Juan Pablo"})
        self.assertIn("tt.asignado_a", w)
        self.assertIn("NULLIF(s.responsable,'')", w)
        self.assertIn("Juan Pablo", p)
        self.assertIn("Daniel", p)          # el responsable por defecto entra al cálculo
        self.assertEqual(w.count("%s"), len(p))

    def test_mis_solicitudes_usa_el_usuario_de_la_sesion(self):
        w, p = self._where({"mias": "1", "responsable": "Otra persona"}, yo="Lenin Urbina")
        self.assertIn("Lenin Urbina", p)
        self.assertNotIn("Otra persona", p)

    def test_mis_solicitudes_sin_sesion_no_trae_nada_ajeno(self):
        w, p = self._where({"mias": "1"}, yo=None)
        self.assertIn("\u0000", p)

    def test_sin_responsable(self):
        w, p = self._where({"responsable": "__sin__"})
        self.assertIn(") IS NULL", w)
        self.assertNotIn("__sin__", p)
        self.assertEqual(w.count("%s"), len(p))

    def test_solicitante(self):
        w, p = self._where({"solicitante": "Lenin Urbina"})
        self.assertIn("s.solicitado_por=%s", w)
        self.assertIn("Lenin Urbina", p)

    def test_busqueda_por_sku_persona_y_proveedor_para_gestion(self):
        w, p = self._where({"q": "piola"})
        self.assertIn("s.repuesto_sku LIKE %s", w)
        self.assertIn("s.solicitado_por LIKE %s", w)
        self.assertIn("mant_proveedores_repuesto WHERE nombre LIKE %s", w)
        self.assertEqual(w.count("%s"), len(p))

    def test_tecnico_no_busca_por_proveedor(self):
        w, p = self._where({"q": "piola"}, tecnico=True, oculta=True)
        self.assertNotIn("mant_proveedores_repuesto", w)
        self.assertEqual(w.count("%s"), len(p))

    def test_filtro_equipo_fuera_de_servicio(self):
        # Daniel 2026-10-04: "esas se tienen que gestionar de manera inmediata"
        w, p = self._where({"fs": "1"})
        self.assertIn("dejo_fuera_servicio", w)
        self.assertIn("estado_capturado='fuera_servicio'", w)
        self.assertEqual(w.count("%s"), len(p))

    def test_reposicion_recibida_cuenta_como_cerrada(self):
        w, _ = self._where({"estado": "abiertas"})
        self.assertIn("NOT (COALESCE(s.es_reposicion,0)=1 AND s.estado='recibido')", w)
        w2, _ = self._where({"estado": "cerradas"})
        self.assertIn("COALESCE(s.es_reposicion,0)=1 AND s.estado='recibido'", w2)


class TestValidarYRecibir(unittest.TestCase):

    def _cambiar(self, sol, stock, datos, tecnico=False, existe_prov=True, ubic=7):
        ejecutados = []

        def fetchone(sql, params=()):
            if "FROM mant_ot_repuesto_solicitudes" in sql:
                return dict(sol)
            if "FROM mant_repuestos_stock" in sql and "ubicacion_id" in sql:
                return {"ubicacion_id": ubic}
            if "FROM mant_repuestos_stock" in sql:
                return dict(stock) if stock else None
            if "FROM mant_proveedores_repuesto" in sql:
                return {"id": params[0], "nombre": "Drax"} if existe_prov else None
            return None

        def no_escribir(*a, **k):
            ejecutados.append(a)
            raise RuntimeError("la prueba se detiene antes de escribir")

        f = _cargar("_otrep_cambiar_estado", {
            "mysql_fetchone": fetchone, "_es_rol_tecnico": lambda *a, **k: tecnico,
            "mysql_execute": no_escribir, "get_db": no_escribir, "mysql_fetchall": lambda *a, **k: []})
        try:
            return f(sol["id"], datos.get("estado"), "Juan Pablo", datos)
        except RuntimeError:
            return ("ESCRIBE", None, None)

    SOL = {"id": 5, "estado": "solicitado", "n_fotos": 1, "repuesto_stock_id": None, "proveedor_id": None,
           "costo_origen": None, "es_reposicion": 0, "repuesto_nombre": "Pantalla"}
    STOCK_SIN_PROV = {"id": 9, "sku": "REP-X-0001", "descripcion": "Pantalla", "cantidad": 0,
                      "proveedor_id": None, "costo_unitario": 0}

    def test_gestion_no_valida_sin_proveedor(self):
        ok, http, payload = self._cambiar(self.SOL, self.STOCK_SIN_PROV, {"estado": "validado", "repuesto_stock_id": 9})
        self.assertFalse(ok)
        self.assertEqual(http, 400)
        self.assertIn("proveedor", payload["error"].lower())

    def test_gestion_con_proveedor_pasa_la_validacion(self):
        r = self._cambiar(self.SOL, self.STOCK_SIN_PROV, {"estado": "validado", "repuesto_stock_id": 9, "proveedor_id": 3})
        self.assertEqual(r[0], "ESCRIBE")   # llegó a escribir: ya no la frena el proveedor

    def test_proveedor_del_repuesto_alcanza(self):
        stock = dict(self.STOCK_SIN_PROV, proveedor_id=4)
        r = self._cambiar(self.SOL, stock, {"estado": "validado", "repuesto_stock_id": 9})
        self.assertEqual(r[0], "ESCRIBE")

    def test_proveedor_inexistente(self):
        ok, http, payload = self._cambiar(self.SOL, self.STOCK_SIN_PROV,
                                          {"estado": "validado", "repuesto_stock_id": 9, "proveedor_id": 99},
                                          existe_prov=False)
        self.assertFalse(ok)
        self.assertIn("no existe", payload["error"])

    def test_tecnico_no_necesita_proveedor(self):
        r = self._cambiar(self.SOL, self.STOCK_SIN_PROV, {"estado": "validado", "repuesto_stock_id": 9}, tecnico=True)
        self.assertEqual(r[0], "ESCRIBE")

    def test_recibir_sin_ubicacion_se_frena(self):
        sol = dict(self.SOL, estado="pedido", repuesto_stock_id=9, proveedor_id=3)
        ok, http, payload = self._cambiar(sol, self.STOCK_SIN_PROV, {"estado": "recibido"}, ubic=None)
        self.assertFalse(ok)
        self.assertIn("ubicación", payload["error"])

    def test_recibir_con_ubicacion_sigue(self):
        sol = dict(self.SOL, estado="pedido", repuesto_stock_id=9, proveedor_id=3)
        r = self._cambiar(sol, self.STOCK_SIN_PROV, {"estado": "recibido"}, ubic=7)
        self.assertEqual(r[0], "ESCRIBE")


class _Cur:
    def __init__(self, log):
        self.log, self.lastrowid = log, 77

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=()):
        self.log.append((sql, params))


class _Conn:
    def __init__(self):
        self.log = []

    def cursor(self):
        return _Cur(self.log)

    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        pass


class TestCrearPorLlegar(unittest.TestCase):

    def _crear(self, body, gestion=True, tecnico=False, ubic_existe=True):
        conn = _Conn()

        def fetchone(sql, params=()):
            if "mant_repuestos_ubicaciones" in sql:
                return {"id": params[0]} if ubic_existe else None
            if "mant_repuestos_marcas" in sql:
                return {"nombre": "ILUS", "proveedor_id": 3}
            return None

        f = _cargar("repstock_crear", {
            "request": _Req(json=body), "jsonify": lambda d: d,
            "_oculta_proveedores": lambda: tecnico, "_otrep_puede_gestion": lambda: gestion,
            "mysql_fetchone": fetchone, "get_mysql": lambda: conn,
            "_repstock_next_sku": lambda marca, c: "REP-GEN-0001",
            "_repstock_dim_float": lambda v: None, "_repstock_bultos_int": lambda v: None,
            "current_username": lambda: "Juan Pablo", "_repstock_log_movimiento": lambda *a, **k: None,
            "_repstock_mover": lambda *a, **k: {}, "_mant_log": lambda *a, **k: None})
        r = f()
        if isinstance(r, tuple):
            return r[0], r[1], conn
        return r, 200, conn

    def _insert(self, conn):
        return next((p for sql, p in conn.log if sql.startswith("INSERT INTO mant_repuestos_stock")), None)

    def test_por_llegar_sin_ubicacion_con_proveedor(self):
        r, code, conn = self._crear({"por_llegar": True, "descripcion": "Pantalla X1", "proveedor_id": 3})
        self.assertEqual(code, 200, r)
        self.assertTrue(r["ok"])
        self.assertIsNotNone(self._insert(conn))

    def test_por_llegar_sin_proveedor_se_frena(self):
        r, code, _ = self._crear({"por_llegar": True, "descripcion": "Pantalla X1"})
        self.assertEqual(code, 400)
        self.assertIn("proveedor", r["error"].lower())

    def test_tecnico_no_puede_crear_por_llegar(self):
        r, code, _ = self._crear({"por_llegar": True, "descripcion": "Pantalla X1", "proveedor_id": 3,
                                  "cantidad": 0, "stock_minimo": 0}, tecnico=True)
        self.assertEqual(code, 400)
        self.assertIn("ubicación", r["error"])

    def test_alta_normal_sigue_exigiendo_ubicacion(self):
        r, code, _ = self._crear({"descripcion": "Correa", "cantidad": 1, "stock_minimo": 0})
        self.assertEqual(code, 400)
        self.assertIn("ubicación", r["error"])

    def test_alta_normal_con_ubicacion(self):
        r, code, conn = self._crear({"descripcion": "Correa", "cantidad": 1, "stock_minimo": 0, "ubicacion_id": 7})
        self.assertEqual(code, 200, r)
        self.assertIsNotNone(self._insert(conn))


if __name__ == "__main__":
    unittest.main()

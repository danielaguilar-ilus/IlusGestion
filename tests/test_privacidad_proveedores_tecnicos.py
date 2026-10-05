"""Ningun tecnico ve ni contacta proveedores (Daniel, 2026-10-01).

Pedido textual: "Nunca los tecnicos deben contener datos o poder comunicarse con
los proveedores. Ni siquiera pueden ver mis proveedores. Solo que puedan seleccionar
los repuestos viendo SKU, relacion/descripcion, cantidad, stock y equipo compatible."

Decisiones de Daniel: (1) NINGUN tecnico ve proveedores -- interno, "elevado"
(tecnico_ejecutivo, Jaizer, el de bodega) y externo; (2) se cierra en todo el
sistema (OT, Bodega, /repuestos, Solicitudes, Tickets de compra, CRUD de
proveedores, Incidencias, Catalogo); (3) los tecnicos SIGUEN cargando y usando la
Bodega (SKU, stock, ubicacion, modelos compatibles, fotos) pero sin ver ni tocar
proveedor ni costo. Gestion (admin, ejecutivo, superadmin...) no cambia nada.

Sin BD ni Flask: funciones de app.py / tickets_module.py / catalogo_module.py
extraidas con ast y ejecutadas con dobles (mismo criterio que
tests/test_gestion_repuestos_incidencia.py), mas revisiones del codigo fuente y de
las plantillas/JS donde la funcion es demasiado grande para ejecutarla aislada.

Correr con:  py -m unittest tests.test_privacidad_proveedores_tecnicos
"""
import ast
import copy
import functools
import os
import re
import sys
import types
import unittest
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tests.test_incidencias_bajas import _cargar, _decoradores  # noqa: E402
from tests.test_incidencias_repuesto_tercera_fuente import _codigo_y_arbol, _fuente_de  # noqa: E402
from tests.test_repuestos_buscador_motor import FILA, _Doble, _Request, _sql_principal  # noqa: E402

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _leer(*partes):
    with open(os.path.join(RAIZ, *partes), encoding="utf-8") as fh:
        return fh.read()


# ───────────────────────────── utilidades de carga ─────────────────────────────
def _extraer(funciones, extra=None, constantes=()):
    """Como tests.test_incidencias_bajas._cargar, pero SIN decoradores (las rutas llevan
    @app.route/@_mant_required, que no existen en un ambito aislado) y trabajando sobre
    COPIAS de los nodos: el AST cacheado se comparte con otras pruebas."""
    _, arbol = _codigo_y_arbol()
    ambito = dict(extra or {})
    for nodo in arbol.body:
        if (isinstance(nodo, ast.Assign) and len(nodo.targets) == 1
                and isinstance(nodo.targets[0], ast.Name) and nodo.targets[0].id in constantes):
            exec(compile(ast.Module(body=[nodo], type_ignores=[]), "<app>", "exec"), ambito)
        if isinstance(nodo, ast.FunctionDef) and nodo.name in funciones:
            copia = copy.deepcopy(nodo)
            copia.decorator_list = []
            exec(compile(ast.Module(body=[copia], type_ignores=[]), "<app>", "exec"), ambito)
    faltan = [n for n in tuple(funciones) + tuple(constantes) if n not in ambito]
    assert not faltan, f"no se encontraron en app.py: {faltan}"
    return ambito


_CACHE_MOD = {}


def _arbol_modulo(nombre_archivo):
    """AST (y codigo) de tickets_module.py / catalogo_module.py, parseado UNA vez por proceso."""
    if nombre_archivo not in _CACHE_MOD:
        codigo = _leer(nombre_archivo)
        _CACHE_MOD[nombre_archivo] = (codigo, ast.parse(codigo))
    return _CACHE_MOD[nombre_archivo]


def _nodo_funcion(nombre_archivo, nombre):
    """FunctionDef por nombre en cualquier nivel (las rutas de tickets/catalogo viven anidadas
    dentro de register_*_routes)."""
    _, arbol = _arbol_modulo(nombre_archivo)
    for nodo in ast.walk(arbol):
        if isinstance(nodo, ast.FunctionDef) and nodo.name == nombre:
            return nodo
    raise AssertionError(f"no se encontro la funcion {nombre} en {nombre_archivo}")


def _decoradores_modulo(nombre_archivo, nombre):
    nodo = _nodo_funcion(nombre_archivo, nombre)
    out = []
    for d in nodo.decorator_list:
        if isinstance(d, ast.Name):
            out.append(d.id)
        elif isinstance(d, ast.Call) and isinstance(d.func, ast.Attribute):
            out.append(ast.unparse(d.func))
    return out


def _fuente_modulo(nombre_archivo, nombre):
    codigo, _ = _arbol_modulo(nombre_archivo)
    return ast.get_source_segment(codigo, _nodo_funcion(nombre_archivo, nombre))


def _ejecutar_nodo(nombre_archivo, nombre, ambito):
    nodo = copy.deepcopy(_nodo_funcion(nombre_archivo, nombre))
    nodo.decorator_list = []
    exec(compile(ast.Module(body=[nodo], type_ignores=[]), nombre_archivo, "exec"), ambito)
    return ambito[nombre]


def _constantes_modulo(nombre_archivo, nombres, ambito):
    codigo, arbol = _arbol_modulo(nombre_archivo)
    for nodo in arbol.body:
        if (isinstance(nodo, ast.Assign) and len(nodo.targets) == 1
                and isinstance(nodo.targets[0], ast.Name) and nodo.targets[0].id in nombres):
            exec(compile(ast.Module(body=[nodo], type_ignores=[]), nombre_archivo, "exec"), ambito)
    faltan = [n for n in nombres if n not in ambito]
    assert not faltan, f"no se encontraron en {nombre_archivo}: {faltan}"


# ───────────────────────── 1. la regla central (helper) ─────────────────────────
class TestReglaCentral(unittest.TestCase):
    """`_oculta_proveedores()`: UNA sola fuente de verdad, toda la familia tecnico."""

    @classmethod
    def setUpClass(cls):
        cls.G = types.SimpleNamespace(user=None)
        amb = _cargar(["_rol_familia", "_es_rol_tecnico", "_oculta_proveedores"], extra={"g": cls.G})
        cls.oculta = staticmethod(amb["_oculta_proveedores"])

    def test_toda_la_familia_tecnico_ve_oculto(self):
        for rol in ("tecnico", "tecnico_ejecutivo", "tecnico_externo", "tecnico_externo_jr",
                    "tecnico_jr", "TECNICO", " tecnico_ejecutivo "):
            self.assertTrue(self.oculta({"role": rol}), f"{rol!r} no debe ver proveedores")

    def test_gestion_no_cambia_nada(self):
        for rol in ("superadmin", "admin", "admin_sstt", "supervisor", "supervisor_sstt",
                    "ejecutivo", "ejecutivo_sstt", "vendedor", "lector", ""):
            self.assertFalse(self.oculta({"role": rol}), f"{rol!r} (gestion) sigue viendo proveedores")

    def test_sin_usuario_no_oculta_y_no_rompe(self):
        self.G.user = None
        self.assertFalse(self.oculta())
        self.assertFalse(self.oculta(None))

    def test_toma_el_usuario_de_g_cuando_no_se_pasa(self):
        self.G.user = {"role": "tecnico_ejecutivo"}
        self.assertTrue(self.oculta())
        self.G.user = {"role": "ejecutivo"}
        self.assertFalse(self.oculta())
        self.G.user = None

    def test_no_depende_de_otrep_puede_gestion_la_raiz_de_la_fuga(self):
        # _otrep_puede_gestion() devuelve True para el tecnico INTERNO (opera la cola de bodega):
        # basar la regla ahi es justamente la fuga que se cierra.
        _, arbol = _codigo_y_arbol()
        nodo = next(n for n in arbol.body if isinstance(n, ast.FunctionDef) and n.name == "_oculta_proveedores")
        nombres = {n.id for n in ast.walk(nodo) if isinstance(n, ast.Name)}
        self.assertNotIn("_otrep_puede_gestion", nombres)
        self.assertIn("_es_rol_tecnico", nombres)

    def test_el_context_processor_la_expone_a_las_plantillas(self):
        fuente = _fuente_de("inject_globals")
        self.assertIn('"oculta_proveedores": _oculta_proveedores()', fuente)


# ───────────────────── 2. lo que sale de la bodega (_otrep_fmt_stock) ─────────────────────
class TestFmtStock(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.estado = {"oculta": False, "externo": False}
        cls.amb = _extraer(
            ["_otrep_fmt_stock"],
            extra={"re": re,
                   "_oculta_proveedores": lambda user=None: cls.estado["oculta"],
                   "_es_tecnico_externo": lambda user=None: cls.estado["externo"]},
            constantes=["_OTREP_STOCK_SOLO_GESTION", "_OTREP_STOCK_SOLO_GESTION_PROV",
                        "_OTREP_STOCK_NO_EXTERNO"])
        cls.fmt = staticmethod(cls.amb["_otrep_fmt_stock"])

    def setUp(self):
        self.estado.update(oculta=False, externo=False)

    def test_la_constante_nueva_trae_lo_que_pidio_daniel_y_no_el_stock(self):
        c = self.amb["_OTREP_STOCK_SOLO_GESTION_PROV"]
        for k in ("proveedor", "proveedor_id", "proveedor_contacto", "proveedor_telefono",
                  "proveedor_email", "proveedor_canal", "costo_unitario"):
            self.assertIn(k, c)
        # NO es _OTREP_STOCK_NO_EXTERNO: el interno SI ve stock, ubicacion y semaforo.
        for k in ("cantidad", "disponible", "semaforo", "ubicacion_codigo", "comprometido", "sku"):
            self.assertNotIn(k, c)

    def test_tecnico_interno_sin_para_ot_pierde_proveedor_y_costo_pero_conserva_stock(self):
        self.estado["oculta"] = True
        d = self.fmt(dict(FILA), para_ot=False)
        for k in ("proveedor", "proveedor_id", "proveedor_contacto", "proveedor_telefono",
                  "proveedor_email", "proveedor_canal", "costo_unitario"):
            self.assertNotIn(k, d, f"el tecnico no debe ver {k}")
        # lo que SI pidio Daniel: SKU, descripcion, cantidad, stock, equipo compatible
        for k in ("sku", "descripcion", "cantidad", "disponible", "comprometido", "semaforo",
                  "ubicacion_codigo", "marca", "con_stock"):
            self.assertIn(k, d, f"el tecnico SI debe ver {k}")
        self.assertEqual(d["disponible"], 7)
        self.assertEqual(d["semaforo"], "verde")

    def test_tecnico_interno_con_para_ot_tambien(self):
        self.estado["oculta"] = True
        d = self.fmt(dict(FILA), para_ot=True)
        self.assertNotIn("proveedor", d)
        self.assertNotIn("proveedor_email", d)
        self.assertNotIn("costo_unitario", d)
        self.assertEqual(d["cantidad"], 10.0)

    def test_gestion_no_pierde_nada(self):
        d = self.fmt(dict(FILA), para_ot=False)
        for k in ("proveedor", "proveedor_id", "proveedor_contacto", "proveedor_telefono",
                  "proveedor_email", "proveedor_canal", "costo_unitario"):
            self.assertIn(k, d, f"gestion debe seguir viendo {k}")
        self.assertEqual(d["proveedor"], "Drax Fitness")

    def test_gestion_con_para_ot_solo_pierde_el_costo_como_siempre(self):
        d = self.fmt(dict(FILA), para_ot=True)
        self.assertNotIn("costo_unitario", d)
        self.assertEqual(d["proveedor"], "Drax Fitness")

    def test_el_recorte_extra_del_externo_queda_igual(self):
        self.estado.update(oculta=True, externo=True)
        d = self.fmt(dict(FILA), para_ot=True)
        for k in ("cantidad", "comprometido", "disponible", "por_llegar", "semaforo",
                  "ubicacion_codigo", "stock_minimo", "marca_id"):
            self.assertNotIn(k, d, f"el externo no ve {k}")
        self.assertIn("con_stock", d)


# ───────────────────── 3. lo que sale de una solicitud (_otrep_fila) ─────────────────────
class TestFilaSolicitud(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.estado = {"oculta": False, "externo": False}
        extra = {
            "datetime": datetime,
            "chile_fmt_filter": lambda v: str(v),
            "_OTREP_ESTADO_LABEL": {"pedido": "Pedido al proveedor"},
            "_OTREP_ORIGEN_LABEL": {},
            "_OTREP_ABIERTOS": ("solicitado", "validado", "pedido", "recibido"),
            "_OTREP_TRANSICIONES": {},
            "_OTREP_ESTADOS_COMPROMETEN": ("validado", "recibido"),
            "_OTREP_ANTIGUEDAD_DIAS_VERDE": 3,
            "_OTREP_ANTIGUEDAD_DIAS_AMBAR": 7,
            "_OTREP_COMPRA_ESTADO_LABEL": {"pedido": "Pedido al proveedor"},
            "_es_rol_tecnico": lambda user=None: cls.estado["oculta"],
            "_oculta_proveedores": lambda user=None: cls.estado["oculta"],
            "_es_tecnico_externo": lambda user=None: cls.estado["externo"],
        }
        cls.amb = _extraer(["_otrep_fila"], extra=extra,
                           constantes=["_OTREP_SOL_SOLO_GESTION", "_OTREP_SOL_SOLO_GESTION_PROV",
                                       "_OTREP_SOL_NO_EXTERNO"])
        cls.fila = staticmethod(cls.amb["_otrep_fila"])

    def setUp(self):
        self.estado.update(oculta=False, externo=False)

    def _sol(self):
        return {
            "id": 1, "estado": "pedido", "cantidad": 2, "repuesto_stock_id": 9, "stock_cantidad": 10,
            "comprometido_otras": 0, "es_reposicion": 0, "created_at": None,
            "proveedor_id": 3, "proveedor_nombre": "Drax Fitness", "proveedor_contacto": "Juan",
            "proveedor_telefono": "+56 9 1", "proveedor_email": "j@drax.cl", "proveedor_canal": "whatsapp",
            "oc_numero": "OC-2026-1", "nota_gestion": "Pedido a Drax por WhatsApp",
            "compra_id": 77, "compra_estado": "pedido", "compra_eta": None,
            "compra_ticket_id": 501, "compra_numero_ticket": "TK-2026-00501",
            "costo_unitario": 1500, "costo_origen": "bodega",
        }

    def test_la_constante_trae_lo_que_pidio_daniel(self):
        c = self.amb["_OTREP_SOL_SOLO_GESTION_PROV"]
        for k in ("proveedor_id", "proveedor_nombre", "proveedor_contacto", "proveedor_telefono",
                  "proveedor_email", "proveedor_canal", "oc_numero", "nota_gestion", "compra_id",
                  "compra_ticket_id", "compra_numero_ticket"):
            self.assertIn(k, c)
        for k in ("estado", "estado_label", "compra_estado_label", "compra_eta"):
            self.assertNotIn(k, c, f"{k} se conserva: el tecnico si sabe que esta 'pedido' y cuando llega")

    def test_tecnico_interno_en_la_cola_completa_no_ve_proveedor_oc_ni_compra(self):
        self.estado["oculta"] = True
        s = self.fila(self._sol(), para_ot=False)   # SIN para_ot: era la fuga del tecnico interno
        for k in self.amb["_OTREP_SOL_SOLO_GESTION_PROV"]:
            self.assertNotIn(k, s, f"el tecnico no debe ver {k}")
        self.assertNotIn("costo_unitario", s)

    def test_se_conserva_estado_etiqueta_de_compra_y_eta(self):
        self.estado["oculta"] = True
        s = self.fila(self._sol(), para_ot=True)
        self.assertEqual(s["estado"], "pedido")
        self.assertEqual(s["estado_label"], "Pedido al proveedor")
        # compra_estado_label se calcula ANTES de quitar compra_id (si no, quedaria vacio)
        self.assertEqual(s["compra_estado_label"], "Pedido al proveedor")
        self.assertIn("compra_eta", s)

    def test_gestion_ve_todo_como_siempre(self):
        s = self.fila(self._sol(), para_ot=False)
        for k in ("proveedor_id", "proveedor_nombre", "proveedor_telefono", "oc_numero",
                  "nota_gestion", "compra_id", "compra_ticket_id", "compra_numero_ticket", "costo_unitario"):
            self.assertIn(k, s, f"gestion debe seguir viendo {k}")
        self.assertEqual(s["proveedor_nombre"], "Drax Fitness")
        self.assertEqual(s["compra_estado_label"], "Pedido al proveedor")


# ───────────────── 4. bodega-buscar: el motor compartido ─────────────────
class TestBodegaBuscar(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        _, arbol = _codigo_y_arbol()
        quiero = {"ot2_api_repuestos_bodega_buscar", "_otrep_fmt_stock", "_otrep_producto_de_maquina"}
        consts = {"_OTREP_STOCK_SOLO_GESTION", "_OTREP_STOCK_SOLO_GESTION_PROV", "_OTREP_STOCK_NO_EXTERNO",
                  "_OTREP_ABIERTOS", "_OTREP_ESTADOS_COMPROMETEN", "_OTREP_ESTADOS_POR_LLEGAR",
                  "_OTREP_SQL_STOCK", "_OTREP_SQL_DISPONIBLE"}
        cls.nodos = []
        for nodo in arbol.body:
            if isinstance(nodo, ast.FunctionDef) and nodo.name in quiero:
                copia = copy.deepcopy(nodo)
                copia.decorator_list = []
                cls.nodos.append(copia)
            elif (isinstance(nodo, ast.Assign) and nodo.targets
                  and getattr(nodo.targets[0], "id", "") in consts):
                cls.nodos.append(nodo)
        assert len(cls.nodos) == len(quiero) + len(consts), "faltan funciones/constantes de app.py"

    def _namespace(self):
        """Namespace NUEVO por llamada: las funciones ejecutadas leen request/_oculta_proveedores
        de SU diccionario de globales, no de una copia."""
        amb = {"re": re}
        for nodo in self.nodos:
            exec(compile(ast.Module(body=[nodo], type_ignores=[]), "<app>", "exec"), amb)
        return amb

    def _buscar(self, tecnico=False, externo=False, **args):
        amb = self._namespace()
        doble = _Doble()
        doble.filas = [dict(FILA)]
        amb["mysql_fetchall"] = doble.fetchall
        amb["mysql_fetchone"] = doble.fetchone
        amb["jsonify"] = lambda d: d
        # El tecnico INTERNO opera la cola de bodega: _otrep_puede_gestion() es True (la raiz de la fuga).
        amb["_otrep_puede_gestion"] = lambda: not externo
        amb["_es_tecnico_externo"] = lambda user=None: externo
        amb["_oculta_proveedores"] = lambda user=None: bool(externo or tecnico)
        amb["print"] = lambda *a, **k: None
        amb["request"] = _Request(**args)
        return amb["ot2_api_repuestos_bodega_buscar"](), doble

    def test_tecnico_interno_con_ctx_gestion_igual_recibe_la_respuesta_recortada(self):
        res, _ = self._buscar(tecnico=True, q="correa", ctx="gestion")
        r = res["repuestos"][0]
        for k in ("proveedor", "proveedor_id", "proveedor_contacto", "proveedor_telefono",
                  "proveedor_email", "proveedor_canal", "costo_unitario"):
            self.assertNotIn(k, r, f"el tecnico no ve {k} ni mandando ctx=gestion a mano")
        for k in ("sku", "descripcion", "cantidad", "disponible", "semaforo", "modelos"):
            self.assertIn(k, r)

    def test_tecnico_interno_no_filtra_ni_busca_por_proveedor(self):
        res, doble = self._buscar(tecnico=True, q="drax", proveedor_id="3")
        sql, params = _sql_principal(doble)
        self.assertNotIn("pv.nombre LIKE", sql, "no deduce proveedores por texto")
        self.assertNotIn("rs.proveedor_id=%s", sql, "ni filtra por proveedor")
        self.assertEqual(params.count("%drax%"), 6)

    def test_tecnico_no_filtra_por_sin_proveedor(self):
        res, doble = self._buscar(tecnico=True, proveedor_id="sin", q="correa")
        sql, _ = _sql_principal(doble)
        self.assertNotIn("rs.proveedor_id IS NULL", sql)

    def test_gestion_sigue_igual(self):
        res, doble = self._buscar(q="drax", proveedor_id="3", ctx="gestion")
        sql, params = _sql_principal(doble)
        self.assertIn("pv.nombre LIKE %s", sql)
        self.assertIn("rs.proveedor_id=%s", sql)
        r = res["repuestos"][0]
        self.assertEqual(r["proveedor"], "Drax Fitness")
        self.assertIn("costo_unitario", r)


# ───────── 5. Bodega: crear / editar no borran ni fijan proveedor y costo ─────────
class _Cur:
    def __init__(self, log):
        self.log = log
        self.lastrowid = 99
        self.rowcount = 1

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        self.log.append((sql, params))

    def fetchone(self):
        return None


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


class _Req:
    def __init__(self, cuerpo):
        self._c = cuerpo

    def get_json(self, silent=True):
        return self._c


class TestBodegaEscrituraTecnico(unittest.TestCase):
    """CRITICO: el formulario manda proveedor_id y costo_unitario SIEMPRE. Si solo se ocultara el
    campo, cada edicion de un tecnico BORRARIA el proveedor y el costo que gestion ya cargo."""

    @classmethod
    def setUpClass(cls):
        cls.estado = {"oculta": False}

    def _ambito(self):
        """Namespace NUEVO por llamada (las funciones leen sus globales de ESTE dict)."""
        return _extraer(
            ["repstock_editar", "repstock_crear", "_repstock_dim_float", "_repstock_bultos_int"],
            extra={"re": re, "print": lambda *a, **k: None, "jsonify": lambda d: d,
                   "current_username": lambda: "lenin", "_mant_log": lambda *a, **k: None,
                   "_oculta_proveedores": lambda user=None: self.estado["oculta"]})

    def setUp(self):
        self.estado["oculta"] = False

    # ---- editar ----
    def _editar(self, cuerpo):
        conn = _Conn()
        amb = self._ambito()
        amb["request"] = _Req(dict(cuerpo))
        amb["get_db"] = lambda: conn
        res = amb["repstock_editar"](5)
        return res, conn.log

    # ---- crear: marca (familia) o proveedor obligatorios (Daniel, 2026-10-05) ----
    def _crear_sin_marca_ni_proveedor(self):
        amb = self._ambito()
        amb["request"] = _Req({"descripcion": "Perno M8", "cantidad": 1, "stock_minimo": 1})
        amb["get_db"] = lambda: _Conn()
        return amb["repstock_crear"]()

    def test_crear_sin_marca_ni_proveedor_se_rechaza_para_el_tecnico(self):
        self.estado["oculta"] = True
        cuerpo, http = self._crear_sin_marca_ni_proveedor()
        self.assertEqual(http, 400)
        self.assertIn("marca (familia)", cuerpo["error"])
        self.assertNotIn("proveedor del repuesto", cuerpo["error"], "al tecnico no se le habla de elegir proveedor")

    def test_crear_sin_marca_ni_proveedor_se_rechaza_para_gestion(self):
        cuerpo, http = self._crear_sin_marca_ni_proveedor()
        self.assertEqual(http, 400)
        self.assertIn("marca o el proveedor", cuerpo["error"])

    def test_el_tecnico_que_manda_proveedor_a_mano_no_salta_la_exigencia(self):
        # el proveedor que mande un tecnico se ignora (REGLA #19): sin marca sigue siendo 400
        self.estado["oculta"] = True
        amb = self._ambito()
        amb["request"] = _Req({"descripcion": "Perno M8", "cantidad": 1, "stock_minimo": 1, "proveedor_id": 7})
        amb["get_db"] = lambda: _Conn()
        cuerpo, http = amb["repstock_crear"]()
        self.assertEqual(http, 400)

    CUERPO_EDICION = {"descripcion": "Perno M8", "stock_minimo": 2, "marca_id": 4, "ubicacion_id": 8,
                      "proveedor_id": None, "costo_unitario": "0", "notas": "x"}

    def test_editar_como_tecnico_no_toca_proveedor_ni_costo(self):
        self.estado["oculta"] = True
        res, log = self._editar(self.CUERPO_EDICION)
        self.assertTrue(res["ok"])
        updates = [sql for sql, _ in log if sql.startswith("UPDATE mant_repuestos_stock")]
        self.assertEqual(len(updates), 1)
        self.assertNotIn("proveedor_id", updates[0], "conserva el proveedor que ya tenia (no lo pisa con NULL)")
        self.assertNotIn("costo_unitario", updates[0], "conserva el costo que ya tenia (no lo pisa con 0)")
        # lo demas del repuesto SI se edita
        for campo in ("descripcion=%s", "stock_minimo=%s", "marca_id=%s", "ubicacion_id=%s"):
            self.assertIn(campo, updates[0])

    def test_editar_como_tecnico_aunque_mande_valores_a_mano_se_ignoran(self):
        self.estado["oculta"] = True
        res, log = self._editar(dict(self.CUERPO_EDICION, proveedor_id=3, costo_unitario="999999"))
        sql, params = [(s, p) for s, p in log if s.startswith("UPDATE")][0]
        self.assertNotIn("proveedor_id", sql)
        self.assertNotIn("costo_unitario", sql)
        self.assertNotIn(999999.0, params)
        self.assertNotIn(3, params)

    def test_editar_como_tecnico_solo_con_proveedor_y_costo_no_hay_nada_que_guardar(self):
        self.estado["oculta"] = True
        res, log = self._editar({"proveedor_id": 3, "costo_unitario": "5"})
        ok, http = res if isinstance(res, tuple) else (res, 200)
        self.assertFalse(ok["ok"])
        self.assertFalse([1 for sql, _ in log if sql.startswith("UPDATE")], "no se escribio nada")

    def test_editar_como_gestion_sigue_guardando_proveedor_y_costo(self):
        res, log = self._editar(dict(self.CUERPO_EDICION, proveedor_id=3, costo_unitario="1500"))
        sql, params = [(s, p) for s, p in log if s.startswith("UPDATE")][0]
        self.assertIn("proveedor_id=%s", sql)
        self.assertIn("costo_unitario=%s", sql)
        self.assertIn(3, params)
        self.assertIn(1500.0, params)

    # ---- crear ----
    def _crear(self, cuerpo, sku_inactivo=False):
        conn = _Conn()
        amb = self._ambito()
        amb["request"] = _Req(dict(cuerpo))
        amb["get_mysql"] = lambda: conn

        def _fetchone(sql, params=None):
            if "FROM mant_repuestos_marcas" in sql:
                return {"nombre": "Drax", "proveedor_id": 77}     # proveedor de REFERENCIA de la marca
            if "FROM mant_repuestos_ubicaciones" in sql:
                return {"id": 8}
            if "FROM mant_repuestos_stock WHERE sku" in sql:
                return {"id": 12, "activo": 0} if sku_inactivo else None
            return None
        amb["mysql_fetchone"] = _fetchone
        amb["_repstock_next_sku"] = lambda marca, c: "REP-DRAX-0001"
        amb["_repstock_log_movimiento"] = lambda *a, **k: None
        amb["_repstock_mover"] = lambda *a, **k: {"aviso": None}
        res = amb["repstock_crear"]()
        return res, conn.log

    CUERPO_ALTA = {"descripcion": "Perno M8", "cantidad": 4, "stock_minimo": 1, "ubicacion_id": 8,
                   "marca_id": 9, "proveedor_id": 3, "costo_unitario": "5000"}

    def test_crear_como_tecnico_ignora_proveedor_y_costo_del_formulario(self):
        self.estado["oculta"] = True
        res, log = self._crear(self.CUERPO_ALTA)
        self.assertTrue(res["ok"])
        sql, params = [(s, p) for s, p in log if s.startswith("INSERT INTO mant_repuestos_stock")][0]
        cols = [c.strip() for c in sql.split("(", 1)[1].split(")", 1)[0].split(",")]
        fila = dict(zip(cols, params))
        self.assertEqual(fila["costo_unitario"], 0, "nace con costo 0: no lo fija el tecnico")
        self.assertEqual(fila["proveedor_id"], 77, "solo hereda el proveedor de referencia de la marca (invisible al tecnico)")
        self.assertNotIn("proveedor_id", res, "ni siquiera el id del proveedor vuelve al tecnico")

    def test_crear_como_gestion_conserva_proveedor_y_costo(self):
        res, log = self._crear(self.CUERPO_ALTA)
        sql, params = [(s, p) for s, p in log if s.startswith("INSERT INTO mant_repuestos_stock")][0]
        cols = [c.strip() for c in sql.split("(", 1)[1].split(")", 1)[0].split(",")]
        fila = dict(zip(cols, params))
        self.assertEqual(fila["costo_unitario"], 5000.0)
        self.assertEqual(fila["proveedor_id"], 3)
        self.assertEqual(res["proveedor_id"], 3)

    def test_reactivar_como_tecnico_no_pisa_proveedor_ni_costo_que_ya_tenia(self):
        self.estado["oculta"] = True
        res, log = self._crear(dict(self.CUERPO_ALTA, sku_erp="ABC-1"), sku_inactivo=True)
        self.assertTrue(res["ok"] and res["reactivado"])
        sql = [s for s, _ in log if s.startswith("UPDATE mant_repuestos_stock")][0]
        self.assertNotIn("proveedor_id", sql)
        self.assertNotIn("costo_unitario", sql)

    def test_reactivar_como_gestion_sigue_actualizando_todo(self):
        res, log = self._crear(dict(self.CUERPO_ALTA, sku_erp="ABC-1"), sku_inactivo=True)
        sql = [s for s, _ in log if s.startswith("UPDATE mant_repuestos_stock")][0]
        self.assertIn("proveedor_id", sql)
        self.assertIn("costo_unitario", sql)


# ───────────────────── 6. decoradores y fuentes de los endpoints ─────────────────────
class TestEndpointsBloqueados(unittest.TestCase):

    def test_crud_de_proveedores_bloquea_a_tecnicos(self):
        for f in ("mant_proveedores_repuesto_list", "mant_proveedor_repuesto_update",
                  "mant_proveedor_repuesto_crear", "mant_proveedor_repuesto_items",
                  "repstock_marca_asignar_proveedor"):
            decos = _decoradores(f)
            self.assertIn("_mant_required", decos, f)
            self.assertIn("_no_tecnico", decos, f"{f} debe llevar @_no_tecnico (403 JSON para /mantenciones/api/)")
            self.assertLess(decos.index("_mant_required"), decos.index("_no_tecnico"), "convencion: despues de @_mant_required")

    def test_no_tecnico_responde_json_amable_a_fetch(self):
        fuente = _fuente_de("_no_tecnico")
        self.assertIn('request.path.startswith("/mantenciones/api/")', fuente)
        self.assertIn("jsonify", fuente)
        self.assertIn("403", fuente)

    def test_la_pagina_de_proveedores_ya_estaba_bloqueada(self):
        self.assertIn("_no_tecnico", _decoradores("mant_proveedores_repuesto_page"))

    def test_buscar_marcas_quita_el_proveedor_de_referencia_a_cualquier_tecnico(self):
        fuente = _fuente_de("repstock_buscar_marcas")
        self.assertIn("_oculta_proveedores()", fuente)
        self.assertRegex(fuente, r"if externo or _oculta_proveedores\(\):\s+return jsonify\(\{\"ok\": True, \"marcas\": \[\{\"id\": r\[\"id\"\], \"nombre\": r\[\"nombre\"\]\}")

    def test_excel_de_bodega_bloquea_al_externo_y_omite_columnas_al_interno(self):
        self.assertIn("_no_tecnico_externo", _decoradores("repstock_exportar_excel"))
        fuente = _fuente_de("repstock_exportar_excel")
        self.assertIn("_omitir_cols = {11, 12} if _oculta_proveedores() else set()", fuente)
        self.assertIn("headers = [h for i, h in enumerate(headers, 1) if i not in _omitir_cols]", fuente)
        self.assertIn("if ci in _omitir_cols:", fuente)
        self.assertIn("widths = [w for i, w in enumerate(widths, 1) if i not in _omitir_cols]", fuente)
        # las dos columnas que salen son justo "Costo unitario" y "Proveedor" (posiciones 11 y 12)
        self.assertRegex(fuente, r"\"Costo unitario\", \"Proveedor\", \"Modelos compatibles\"")

    def test_sondeo_erp_no_manda_costos_a_un_tecnico(self):
        fuente = _fuente_de("repstock_sondeo_erp")
        self.assertIn("_sin_costos = _oculta_proveedores()", fuente)
        self.assertIn('_prod.pop("costo_promedio", None)', fuente)
        self.assertIn('_prod.pop("costo_ultima_compra", None)', fuente)

    def test_pagina_repuestos_quita_el_dato_en_el_servidor(self):
        fuente = _fuente_de("repuestos_hub_list")
        self.assertIn("if _oculta_proveedores():", fuente)
        for k in ("costo_unitario", "proveedor", "proveedor_id", "proveedor_nombre", "proveedor_telefono",
                  "proveedor_email", "proveedor_canal", "proveedor_contacto"):
            self.assertIn(f'"{k}"', fuente)

    def test_contexto_de_bodega_quita_proveedor_y_costo_de_cada_fila(self):
        fuente = _fuente_de("_repstock_contexto_bodega")
        self.assertIn("if _oculta_proveedores():", fuente)
        self.assertIn("_OTREP_STOCK_SOLO_GESTION_PROV", fuente)
        self.assertIn("bod_count_sin_costo = 0", fuente)
        # (revisión 2026-10-01) esta consulta usa el alias proveedor_nombre: se quita todo "proveedor*"
        self.assertIn('str(_k).startswith("proveedor")', fuente)
        self.assertIn('request.args.get("sin_costo") == "1" and not _oculta_proveedores()', fuente)

    def test_bitacoras_no_muestran_proveedor_ni_nota_de_gestion(self):
        amb = _cargar(["_otrep_log_sin_proveedor"], ["_OTREP_LOG_ESTADO_RE"], extra={"re": re})
        f = amb["_otrep_log_sin_proveedor"]
        self.assertEqual(f("#12 Motor X1: pendiente → pedido · REP-0004 · proveedor Drax · Pedido a Juan +569"),
                         "#12 Motor X1: pendiente → pedido")
        self.assertEqual(f("algo raro · proveedor Drax"), "algo raro")
        app_src = open(os.path.join(RAIZ, "app.py"), encoding="utf-8").read()
        self.assertIn("_det = _otrep_log_sin_proveedor(_det)", app_src)
        self.assertIn('l["valor_despues"] = _otrep_log_sin_proveedor(l.get("valor_despues"))', app_src)

    def test_el_alta_y_la_edicion_ignoran_las_claves(self):
        for f in ("repstock_crear", "repstock_editar"):
            fuente = _fuente_de(f)
            self.assertIn("_tecnico_sin_prov = _oculta_proveedores()", fuente, f)
            self.assertIn('d.pop("proveedor_id", None)', fuente, f)
            self.assertIn('d.pop("costo_unitario", None)', fuente, f)
        # y el pop va ANTES de leer la clave
        crear = _fuente_de("repstock_crear")
        self.assertLess(crear.index('d.pop("costo_unitario", None)'), crear.index("costo_unitario = float("))
        editar = _fuente_de("repstock_editar")
        self.assertLess(editar.index('d.pop("costo_unitario", None)'), editar.index("allowed = ["))

    def test_legacy_mant_repuestos_un_tecnico_no_escribe_proveedor_ni_costo(self):
        fuente = _fuente_de("mant_repuesto_update")
        self.assertIn("_oculta_proveedores()", fuente)
        self.assertIn("f not in ('costo_unitario', 'proveedor')", fuente)

    def test_solicitudes_vista_por_proveedor_se_trata_como_lista(self):
        fuente = _fuente_de("repstock_solicitudes_ot_listar")
        self.assertIn('if vista == "proveedor" and _oculta_proveedores():', fuente)
        self.assertLess(fuente.index('vista == "proveedor" and _oculta_proveedores()'),
                        fuente.index("_otrep_listar_agrupado("))
        self.assertRegex(fuente, r'vista = "lista"')

    def test_excel_de_solicitudes_omite_proveedor_y_oc(self):
        fuente = _fuente_de("repstock_solicitudes_ot_export")
        self.assertIn('h in ("Proveedor", "N° OC")', fuente)
        self.assertIn("_oculta_proveedores()", fuente)
        self.assertIn("ws.append([_fila_xls[i] for i in _cols_ok])", fuente)

    def test_la_ficha_de_incidencia_no_trae_el_proveedor_a_un_tecnico(self):
        fuente = _fuente_de("mant_api_incidencia_ficha")
        self.assertIn("if _oculta_proveedores():", fuente)
        self.assertIn('s.pop("proveedor_nombre", None)', fuente)

    def test_alta_rapida_de_incidencia_ignora_el_proveedor_de_un_tecnico(self):
        fuente = _fuente_de("mant_api_incidencias_repuesto_crear_rapido")
        self.assertRegex(fuente, r"if _oculta_proveedores\(\):\s+#[^\n]*\n(?:\s+#[^\n]*\n)*\s+proveedor_id = None")

    def test_solicitar_repuestos_de_incidencia_ignora_proveedor_y_recordar(self):
        fuente = _fuente_de("mant_api_incidencia_solicitar_repuestos")
        i_extras = fuente.index("extras = _inc_gr_emparejar_extras(lineas_in, lineas_ok)")
        i_reset = fuente.index('_ex["proveedor_id"] = None')
        i_prov_final = fuente.index('li["prov_final"] = ex["proveedor_id"]')
        self.assertLess(i_extras, i_reset)
        self.assertLess(i_reset, i_prov_final)
        self.assertIn('_ex["guardar_proveedor"] = False', fuente)

    def test_el_sondeo_y_la_vista_agrupada_ya_estaban_cerrados_a_gestion(self):
        # Compras: nunca un tecnico (ya existia, se verifica que siga).
        self.assertIn("_es_rol_tecnico()", _fuente_de("_otrep_compra_gestion_required"))


class TestFiltrosDeLaCola(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.estado = {"oculta": False}

    def _filtros(self, **args):
        amb = _extraer(
            ["_otrep_filtros_query"],
            extra={"re": re, "datetime": datetime, "timezone": None, "request": _Request(**args),
                   "_es_rol_tecnico": lambda user=None: self.estado["oculta"],
                   "_oculta_proveedores": lambda user=None: self.estado["oculta"]},
            constantes=["_OTREP_ESTADOS"])
        return amb["_otrep_filtros_query"]()

    def test_un_tecnico_no_filtra_por_proveedor(self):
        self.estado["oculta"] = True
        where, params = self._filtros(proveedor_id="3", cliente_id="7")
        self.assertNotIn("s.proveedor_id", where)
        self.assertNotIn(3, params)
        self.assertIn("s.cliente_id=%s", where, "el resto de los filtros sigue")
        self.assertIn(7, params)

    def test_gestion_si_filtra_por_proveedor(self):
        self.estado["oculta"] = False
        where, params = self._filtros(proveedor_id="3")
        self.assertIn("s.proveedor_id=%s", where)
        self.assertIn(3, params)


# ───────────────────────── 7. Tickets (tickets_module.py) ─────────────────────────
TICKETS = "tickets_module.py"


class TestTicketsTextoSinProveedor(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        amb = {"re": re}
        _constantes_modulo(TICKETS, ["TK_TIPOS_COMPRA_PROVEEDOR", "_TK_LINEA_PROVEEDOR_RE", "_TK_LINEA_LOTE_PROV_RE"], amb)
        _ejecutar_nodo(TICKETS, "_tk_texto_sin_proveedor", amb)
        _ejecutar_nodo(TICKETS, "_tk_mensajes_sin_proveedor", amb)
        cls.amb = amb
        cls.texto = staticmethod(amb["_tk_texto_sin_proveedor"])
        cls.mensajes = staticmethod(amb["_tk_mensajes_sin_proveedor"])

    def test_los_tipos_de_compra_son_los_dos_que_dijo_daniel(self):
        self.assertEqual(self.amb["TK_TIPOS_COMPRA_PROVEEDOR"], ("spare_parts_store", "spare_parts_import"))

    def test_quita_proveedor_sugerido_proveedor_y_oc(self):
        nota = ("Solicitud de repuesto #12 · Perno M8 × 4\n"
                "Equipo: Trotadora X9\n"
                "Diagnóstico: falta el perno\n"
                "Proveedor sugerido: Drax Fitness\n"
                "Evidencia: 1 foto(s), 0 video(s).")
        r = self.texto(nota)
        self.assertNotIn("Drax", r)
        self.assertNotIn("Proveedor sugerido", r)
        self.assertEqual(r.split("\n"), ["Solicitud de repuesto #12 · Perno M8 × 4", "Equipo: Trotadora X9",
                                         "Diagnóstico: falta el perno", "Evidencia: 1 foto(s), 0 video(s)."])

    def test_quita_las_lineas_de_cambio_de_estado(self):
        nota = ("Solicitud de repuesto #12 (Perno): Validado → Pedido\n"
                "Ligada a REP-1 · Perno\n"
                "Proveedor: Drax Fitness\n"
                "OC OC-2026-77\n"
                "Pedido por WhatsApp")
        r = self.texto(nota)
        self.assertNotIn("Proveedor:", r)
        self.assertNotIn("OC OC-2026-77", r)
        self.assertIn("Ligada a REP-1 · Perno", r)
        self.assertIn("Pedido por WhatsApp", r)

    def test_la_lista_del_lote_pierde_el_proveedor_pero_no_el_resto(self):
        nota = ("Solicitud de repuestos desde la incidencia #145 (lote de 3):\n"
                "  · #901 Perno M8 × 4 — Drax Fitness\n"
                "  · #902 Pantalla táctil × 1 — sin proveedor · NO existe en la bodega (propuesto)\n"
                "  · #903 Cable — mando × 2.5 — Relax Chile\n"
                "Producto: Escaladora ILUS X1 (SKU 1010089701)\n"
                "Diagnóstico: Falta el pedal izquierdo")
        r = self.texto(nota).split("\n")
        self.assertEqual(r[1], "  · #901 Perno M8 × 4")
        self.assertEqual(r[2], "  · #902 Pantalla táctil × 1 · NO existe en la bodega (propuesto)")
        self.assertEqual(r[3], "  · #903 Cable — mando × 2.5")
        self.assertEqual(r[4], "Producto: Escaladora ILUS X1 (SKU 1010089701)")
        self.assertNotIn("Drax", "\n".join(r))
        self.assertNotIn("Relax", "\n".join(r))

    def test_no_toca_lineas_que_no_hablan_del_proveedor(self):
        nota = "Ocupado el equipo, OCupar la sala\nProveedores de la zona: ninguno\nDiagnóstico: ok"
        self.assertEqual(self.texto(nota), nota, "ni 'Ocupado' ni 'Proveedores ...' son lineas del sistema")

    def test_texto_vacio_o_none(self):
        self.assertEqual(self.texto(""), "")
        self.assertIsNone(self.texto(None))

    def test_solo_se_limpian_las_notas_ligadas_a_solicitudes_de_repuesto(self):
        filas = [
            {"id": 1, "contenido": "Proveedor: Drax", "metadata": '{"solicitud_repuesto_id": 5}'},
            {"id": 2, "contenido": "Proveedor: Drax", "metadata": '{"solicitud_repuesto_ids": [5, 6]}'},
            {"id": 3, "contenido": "Proveedor: el cliente lo menciono", "metadata": None},
            {"id": 4, "contenido": "Proveedor: otro", "metadata": '{"campo": "asignado_a"}'},
        ]
        r = self.mensajes(filas)
        self.assertEqual(r[0]["contenido"], "")
        self.assertEqual(r[1]["contenido"], "")
        self.assertEqual(r[2]["contenido"], "Proveedor: el cliente lo menciono")
        self.assertEqual(r[3]["contenido"], "Proveedor: otro")
        self.assertEqual(filas[0]["contenido"], "Proveedor: Drax", "no muta las filas originales")

    def test_tk_api_get_aplica_el_filtro_solo_a_tecnicos(self):
        fuente = _fuente_modulo(TICKETS, "tk_api_get")
        self.assertIn("_tk_mensajes_sin_proveedor(mensajes) if _finanzas_ocultas else mensajes", fuente)
        # _finanzas_ocultas sale de _tk_es_tecnico() = toda la familia tecnico
        self.assertIn("_finanzas_ocultas = _tk_es_tecnico()", fuente)


class TestTicketsDeCompra(unittest.TestCase):

    RUTAS_POR_ID = (
        "tk_ficha", "tk_api_get", "tk_api_update", "tk_api_delete", "tk_api_comentario",
        "tk_api_responder_cliente", "tk_api_marcar_leido", "tk_api_add_equipo", "tk_api_del_equipo",
        "tk_api_generar_ot", "tk_api_update_equipo_garantia", "tk_api_equipos_desde_documento",
        "tk_api_equipos_manual", "tk_api_upload_adjunto",
    )

    def test_todas_las_rutas_por_id_llevan_el_candado(self):
        for f in self.RUTAS_POR_ID:
            decos = _decoradores_modulo(TICKETS, f)
            self.assertIn("_tk_sin_tickets_de_compra", decos, f"{f} debe bloquear los tickets de compra a un tecnico")
            self.assertIn("_tickets_required", decos, f)
            self.assertLess(decos.index("_tickets_required"), decos.index("_tk_sin_tickets_de_compra"), f)

    def test_la_ficha_mantiene_el_bloqueo_del_externo(self):
        decos = _decoradores_modulo(TICKETS, "tk_ficha")
        self.assertIn("_no_tecnico_externo", decos)
        self.assertLess(decos.index("_no_tecnico_externo"), decos.index("_tk_sin_tickets_de_compra"))

    def test_el_listado_los_kpis_y_los_reportes_usan_el_where_acotado(self):
        codigo, _ = _arbol_modulo(TICKETS)
        self.assertNotIn("_tk_list_where(request.args)", codigo,
                         "listado/KPIs/CSV deben pasar por _tk_list_where_scoped")
        self.assertEqual(codigo.count("_tk_list_where_scoped(request.args)"), 3)
        self.assertIn("_tk_list_where_scoped", _fuente_modulo(TICKETS, "tk_api_list"))

    def test_la_busqueda_de_tickets_tambien_los_excluye(self):
        fuente = _fuente_modulo(TICKETS, "tk_api_tickets_buscar")
        self.assertIn("_tk_es_tecnico()", fuente)
        # (revisión 2026-10-01) por compra ligada, no por tipo: esos tipos también los usan clientes
        self.assertIn("NOT EXISTS (SELECT 1 FROM mant_repuestos_compras", fuente)

    # ---- comportamiento del decorador y del WHERE ----
    def _ambito(self, es_tecnico, es_compra, falla_bd=False):
        amb = {"wraps": functools.wraps, "request": types.SimpleNamespace(path="/tickets/api/tickets/9"),
               "_tk_es_tecnico": lambda: es_tecnico,
               "_is_ajaxish": lambda: True,
               "jsonify": lambda d: d,
               "flash": lambda *a, **k: amb.setdefault("_flasheado", True),
               "redirect": lambda u: ("REDIRECT", u),
               "url_for": lambda e: "/" + e}

        def _fetchone(sql, params=None):
            if falla_bd:
                raise RuntimeError("bd caida")
            self.assertIn("mant_repuestos_compras", sql)
            return {"si": 1} if es_compra else None
        amb["mysql_fetchone"] = _fetchone
        _constantes_modulo(TICKETS, ["TK_TIPOS_COMPRA_PROVEEDOR"], amb)
        _ejecutar_nodo(TICKETS, "_tk_ticket_es_de_compra", amb)
        _ejecutar_nodo(TICKETS, "_tk_sin_tickets_de_compra", amb)
        _ejecutar_nodo(TICKETS, "_tk_list_where_scoped", dict(amb, _tk_list_where=lambda a: ("", [])))
        return amb

    def _vista(self, amb):
        @amb["_tk_sin_tickets_de_compra"]
        def vista(tid):
            return "VISTA-OK"
        return vista

    def test_el_tecnico_no_abre_un_ticket_de_compra(self):
        for _ in (1,):
            amb = self._ambito(True, True)
            r = self._vista(amb)(tid=9)
            cuerpo, http = r
            self.assertEqual(http, 403)
            self.assertFalse(cuerpo["ok"])
            self.assertEqual(cuerpo["error_codigo"], "TICKET_COMPRA_SIN_ACCESO")
            self.assertIn("gestiona bodega/gestión", cuerpo["error"], "mensaje amable, sin detalles internos")

    def test_la_ficha_pagina_avisa_y_vuelve_al_listado(self):
        amb = self._ambito(True, True)
        amb["_is_ajaxish"] = lambda: False
        # el decorador resuelve _is_ajaxish del ambito al llamarlo
        r = self._vista(amb)(tid=9)
        self.assertEqual(r, ("REDIRECT", "/tk_list"))
        self.assertTrue(amb.get("_flasheado"))

    def test_el_tecnico_si_abre_cualquier_otro_ticket(self):
        # incluido un ticket PÚBLICO de cliente tipo "Repuestos bodega": sin compra ligada, se abre
        amb = self._ambito(True, False)
        self.assertEqual(self._vista(amb)(tid=9), "VISTA-OK")

    def test_gestion_abre_tambien_los_de_compra(self):
        amb = self._ambito(False, True)
        self.assertEqual(self._vista(amb)(tid=9), "VISTA-OK")

    def test_si_la_bd_falla_el_tecnico_queda_sin_acceso_en_vez_de_filtrar_contacto(self):
        amb = self._ambito(True, False, falla_bd=True)
        cuerpo, http = self._vista(amb)(tid=9)
        self.assertEqual(http, 403)

    def test_where_acotado_excluye_los_tipos_de_compra_solo_para_tecnicos(self):
        for es_tecnico, wsql_in, esperado_en in (
                (True, "", True), (True, " WHERE t.estado=%s", True), (False, "", False), (False, " WHERE t.estado=%s", False)):
            amb = {"_tk_es_tecnico": lambda es=es_tecnico: es,
                   "_tk_list_where": lambda a, w=wsql_in: (w, ["open"] if w else [])}
            _constantes_modulo(TICKETS, ["TK_TIPOS_COMPRA_PROVEEDOR"], amb)
            f = _ejecutar_nodo(TICKETS, "_tk_list_where_scoped", amb)
            wsql, params = f({})
            tiene = "NOT EXISTS (SELECT 1 FROM mant_repuestos_compras _c WHERE _c.ticket_id=t.id)" in wsql
            self.assertEqual(tiene, esperado_en, (es_tecnico, wsql_in, wsql))
            if wsql_in:
                self.assertTrue(wsql.startswith(wsql_in), "conserva los filtros que ya habia")
                if es_tecnico:
                    self.assertIn(" AND NOT EXISTS (", wsql)
            elif es_tecnico:
                self.assertTrue(wsql.startswith(" WHERE "))


# ───────────────────────── 8. Catalogo: no hay canal hacia proveedores ─────────────────────────
class TestCatalogoNoEnviaManuales(unittest.TestCase):

    CATALOGO = "catalogo_module.py"

    def test_los_dos_endpoints_de_correo_chequean_primero(self):
        for f in ("cat_api_manual_enviar_correo", "cat_api_manuales_multi_enviar_correo"):
            nodo = _nodo_funcion(self.CATALOGO, f)
            primero = nodo.body[0]
            # (el docstring no existe en estos dos: la primera sentencia es el chequeo)
            self.assertIsInstance(primero, ast.Assign, f)
            self.assertEqual(ast.unparse(primero), "_bloqueo = _envio_manual_bloqueado_para_tecnico()", f)
            self.assertIn("return _bloqueo", ast.unparse(nodo.body[1]), f)

    def test_el_gate_bloquea_a_un_tecnico_y_deja_pasar_a_gestion(self):
        for es_tecnico, bloquea in ((True, True), (False, False)):
            ctx = {"_oculta_proveedores": lambda es=es_tecnico: es}
            amb = {"ctx": ctx, "jsonify": lambda d: d}
            f = _ejecutar_nodo(self.CATALOGO, "_envio_manual_bloqueado_para_tecnico", amb)
            r = f()
            if bloquea:
                cuerpo, http = r
                self.assertEqual(http, 403)
                self.assertEqual(cuerpo["error_codigo"], "TECNICO_SIN_ENVIO_MANUAL")
                self.assertIn("Puedes verlos", cuerpo["error"])
            else:
                self.assertIsNone(r)

    def test_ver_y_descargar_siguen_igual(self):
        # solo los dos "enviar-correo" cambian; las rutas de ver/descargar no se tocan
        for f in ("cat_api_manuales_descargar",):
            fuente = _fuente_modulo(self.CATALOGO, f)
            self.assertNotIn("_envio_manual_bloqueado_para_tecnico", fuente)

    def test_el_boton_no_se_dibuja_a_un_tecnico(self):
        js = _leer("static", "catalogo_list.js")
        self.assertIn("const CAN_ENVIAR_MANUAL = window.CAT_LIST_DATA.canEnviarManual !== false;", js)
        self.assertIn("(CAN_ENVIAR_MANUAL\n", js.replace("\r\n", "\n"))
        # los dos botones (legado y multi) y el listener del legado van detras del flag
        self.assertEqual(js.count("CAN_ENVIAR_MANUAL"), 4)
        html = _leer("templates", "catalogo", "list.html")
        self.assertIn("canEnviarManual: {{ 'false' if oculta_proveedores else 'true' }}", html)


# ───────────────────── 9. Front: plantillas y JS ─────────────────────
class TestFrontSinProveedores(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        norm = lambda s: s.replace("\r\n", "\n")
        cls.detalle = norm(_leer("templates", "ot2", "detalle.html"))
        cls.incid = norm(_leer("templates", "mantenciones", "incidencias.html"))
        cls.repuestos = norm(_leer("templates", "clientes_hub", "repuestos.html"))
        cls.pane = norm(_leer("templates", "mantenciones", "_repuestos_bodega_pane.html"))
        cls.buscador = norm(_leer("static", "repuestos_buscador.js"))

    # ---- OT 2.0 ----
    def test_ot_el_buscador_va_sin_proveedor_para_cualquier_tecnico(self):
        self.assertIn("var _OTREP_OCULTA_PROV = {{ (oculta_proveedores or false) | tojson }};", self.detalle)
        self.assertIn("mostrarProveedor: !(_OTREP_ES_EXTERNO || _OTREP_OCULTA_PROV)", self.detalle)
        self.assertNotIn("mostrarProveedor: !_OTREP_ES_EXTERNO,", self.detalle)
        # la constante se define ANTES de usarse
        self.assertLess(self.detalle.index("var _OTREP_OCULTA_PROV"), self.detalle.index("mostrarProveedor: !("))

    # ---- Incidencias ----
    def test_incidencias_el_buscador_va_sin_proveedor_para_un_tecnico(self):
        self.assertIn("var IGR_OCULTA_PROV = {{ 'true' if oculta_proveedores else 'false' }};", self.incid)
        self.assertIn("mostrarProveedor: !IGR_OCULTA_PROV", self.incid)
        self.assertNotIn("mostrarProveedor: true", self.incid)

    def test_incidencias_nunca_pide_los_proveedores_ni_los_manda(self):
        bloque = self.incid.split("function igrProvs(){")[1].split("function igrProvNombre")[0]
        self.assertTrue(bloque.lstrip().startswith("if(IGR_OCULTA_PROV)"), "lo primero que hace es cortar")
        envio = self.incid.split("function igrEnviar(){")[1].split("function igrPintarOk")[0]
        self.assertIn("proveedor_id: IGR_OCULTA_PROV ? '' : (l.provId || '')", envio)
        self.assertIn("guardar_proveedor: !IGR_OCULTA_PROV && !!(", envio)

    def test_incidencias_no_dibuja_selector_ni_grupos_de_proveedor_a_un_tecnico(self):
        linea = self.incid.split("function igrLineaHTML(l){")[1].split("function igrGrupos(){")[0]
        self.assertIn("if(!IGR_OCULTA_PROV){", linea)
        self.assertLess(linea.index("if(!IGR_OCULTA_PROV){"), linea.index("igr-lin-prov"))
        self.assertLess(linea.index("if(!IGR_OCULTA_PROV){"), linea.index("Guardar este proveedor en el repuesto"))
        lista = self.incid.split("function igrPintarLista(){")[1].split("$('igrLista').addEventListener('click'")[0]
        self.assertIn("(g.k === 'sin') && !IGR_OCULTA_PROV", lista)
        self.assertIn("'Repuestos a solicitar'", lista)
        hist = self.incid.split("function igrPintarHist(){")[1].split("// La ficha (solicitudes")[0]
        self.assertIn("if(!IGR_OCULTA_PROV && s.proveedor_nombre)", hist)

    def test_incidencias_los_campos_de_proveedor_quedan_ocultos_para_un_tecnico(self):
        self.assertIn('<div class="igr-f"{% if oculta_proveedores %} hidden style="display:none"{% endif %}>', self.incid)
        self.assertIn('<div class="position-relative mt-2"{% if oculta_proveedores %} hidden style="display:none"{% endif %}>', self.incid)

    # ---- /repuestos ----
    def test_repuestos_toda_la_familia_tecnico_queda_sin_compras(self):
        # (los comentarios pueden nombrar la comparacion vieja; lo que no puede quedar es una SENTENCIA con ella)
        self.assertIsNone(re.search(r"\{%-?\s*if[^%]*current_user\.role\s*!=\s*'tecnico'", self.repuestos),
                          "queda un {% if current_user.role != 'tecnico' %} (rol exacto) en repuestos.html")
        self.assertIn("var RS_PUEDE_COMPRAS = {{ 'false' if oculta_proveedores else 'true' }};", self.repuestos)
        self.assertIn("var RS_OCULTA_PROV = {{ 'true' if oculta_proveedores else 'false' }};", self.repuestos)

    def test_repuestos_no_se_dibujan_el_filtro_ni_la_vista_por_proveedor(self):
        for ancla in ('<select id="rsFProveedor"', '<button type="button" id="rsVistaProveedor"',
                      '<button type="button" id="rsVistaCompras"', '<th>Costo</th>',
                      'class="rep-btn-asignar"', 'href="/mantenciones/proveedores"'):
            i = self.repuestos.index(ancla)
            antes = self.repuestos[max(0, i - 600):i]
            self.assertIn("{% if not oculta_proveedores %}", antes, f"{ancla} debe ir dentro de {{% if not oculta_proveedores %}}")

    def test_repuestos_el_js_tolera_que_el_filtro_de_proveedor_no_exista(self):
        self.assertIn("const _selProv = document.getElementById('rsFProveedor');", self.repuestos)
        self.assertIn("_rs.proveedor_id = _selProv ? (_selProv.value || '') : '';", self.repuestos)
        self.assertIn("if (_selProv2) _selProv2.value = '';", self.repuestos)
        # rsCargarProveedoresFiltro ya salia si el select no existe (no pide proveedores)
        self.assertIn("if (!sel || sel.dataset.cargado) return;", self.repuestos)

    def test_repuestos_la_tabla_legacy_no_muestra_proveedor_ni_costo_a_un_tecnico(self):
        self.assertIn('<td colspan="{{ 9 if oculta_proveedores else 10 }}"', self.repuestos)
        self.assertIn("{% if not oculta_proveedores %}<td>${{ '{:,.0f}'.format(r.costo_unitario|float)", self.repuestos)

    # ---- Bodega (panel) ----
    def test_bodega_no_manda_proveedor_ni_costo_si_es_tecnico(self):
        self.assertIn("const RB_OCULTA_PROV = {{ 'true' if oculta_proveedores else 'false' }};", self.pane)
        self.assertIn("costo_unitario: RB_OCULTA_PROV ? undefined : rbCostoRaw(),", self.pane)
        self.assertIn("proveedor_id: RB_OCULTA_PROV ? undefined : (document.getElementById('rbProveedor').value || null),", self.pane)

    def test_bodega_no_dibuja_costo_proveedor_ni_el_modal_de_alta(self):
        self.assertIn("{% if not oculta_proveedores %}<th>Costo</th>{% endif %}", self.pane)
        self.assertIn('{% if not oculta_proveedores %}{# 🔒 2026-10-01: el alta de proveedores no se dibuja para un técnico #}\n<div class="modal fade" id="modalRbProveedor"', self.pane)
        # el modal se inicializa solo si existe (si no, rompia todo el DOMContentLoaded)
        self.assertIn("RB_PROVEEDOR_MODAL = _modalProvEl ? new bootstrap.Modal(_modalProvEl) : null;", self.pane)
        # el filtro "Sin costo" tampoco
        i = self.pane.index('name="sin_costo"')
        self.assertIn("{% if not oculta_proveedores %}", self.pane[max(0, i - 900):i])

    def test_bodega_deja_campos_ocultos_para_que_el_js_no_falle(self):
        self.assertIn('<input type="hidden" id="rbCosto">', self.pane)
        self.assertIn('<div hidden aria-hidden="true">\n                <input type="hidden" id="rbProveedor">', self.pane)
        for ident in ("rbProveedorBuscar", "rbProveedorResultadosList", "rbProveedorSeleccionado", "rbProveedorSugerido"):
            self.assertIn(f'id="{ident}"', self.pane)

    def test_bodega_el_costo_del_erp_no_se_muestra_ni_se_copia(self):
        self.assertIn("const costo = RB_OCULTA_PROV ? 0 : (Number(p.costo_promedio) || 0);", self.pane)
        self.assertEqual(self.pane.count("RB_OCULTA_PROV ? 0 : (Number(p.costo_promedio) || 0)"), 2)

    def test_bodega_la_marca_no_sugiere_proveedor_a_un_tecnico(self):
        bloque = self.pane.split("function rbPickMarca(i){")[1].split("async function rbCrearMarcaRapida")[0]
        self.assertIn("if (RB_OCULTA_PROV){ sugerido.style.display = 'none'; return; }", bloque)

    # ---- buscador compartido ----
    def test_buscador_con_mostrarProveedor_false_no_pide_ni_dibuja_ni_manda_proveedores(self):
        js = self.buscador
        self.assertIn("if (!st.mostrarProveedor) {", js)
        cargar = js.split("async function cargarOpciones() {")[1].split("cargarOpciones();")[0]
        self.assertLess(cargar.index("if (!st.mostrarProveedor)"), cargar.index("/mantenciones/api/proveedores-repuesto"))
        self.assertIn("} else if (!st.provs) {", cargar, "el fetch solo corre en la rama de mostrarProveedor")
        self.assertIn("var tags = st.mostrarProveedor ? proveedorBadge(it, pal) : '';", js)
        self.assertIn("if (st.proveedor && st.mostrarProveedor) p.set('proveedor_id', st.proveedor);", js)
        self.assertIn("(st.mostrarProveedor ? '<select class=\"rpb-prov\"", js, "el selector solo se dibuja con mostrarProveedor")
        self.assertIn("st.provs = (st.mostrarProveedor && Array.isArray(lista)) ? lista : [];", js)

    def test_buscador_no_pinta_la_pastilla_si_el_dato_no_viene(self):
        fn = self.buscador.split("function proveedorBadge(it, palabras) {")[1].split("/* 🔎 2026-09-26 (Daniel: \"poder buscar")[0]
        self.assertIn("if (!('proveedor' in it) && !('proveedor_id' in it)) return '';", fn)


if __name__ == "__main__":
    unittest.main()

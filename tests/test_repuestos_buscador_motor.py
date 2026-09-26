"""Motor de busqueda de repuestos COMPARTIDO (2026-09-26 -- Daniel: "tengo que
poder buscar por proveedor y por equipo, es decir, por los modelos
compatibles... el motor de busqueda igual debera aplicar cuando el tecnico
tenga la OT y solicitara repuesto: proveedor, modelo compatible y repuesto,
obviamente SKU tambien" + "es medio debil el buscador... ojala ahorrar
espacio, algo bien espectacular").

Prueba GET /ot/api/repuestos/bodega-buscar (ot2_api_repuestos_bodega_buscar)
y el semaforo de _otrep_fmt_stock, extrayendolos de app.py con ast (importar
app.py entero levanta Flask, la base y los hilos de cron -- mismo criterio
que tests/test_bodega_alerta.py). Se reemplazan request/jsonify/mysql_* por
dobles y se inspecciona el SQL + los parametros que el endpoint arma.

Correr con:  py -m unittest tests.test_repuestos_buscador_motor
(pytest NO esta instalado en el equipo de Daniel.)
"""
import ast
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
APP_PY = os.path.join(RAIZ, "app.py")


def _leer(path):
    with open(path, encoding="utf-8") as fh:
        return fh.read()


_CACHE = {}


def _arbol():
    """app.py mide >130k lineas: se parsea UNA vez por corrida."""
    if "arbol" not in _CACHE:
        _CACHE["src"] = _leer(APP_PY)
        _CACHE["arbol"] = ast.parse(_CACHE["src"])
    return _CACHE["arbol"]


class _Args(dict):
    def get(self, k, default=None):
        return dict.get(self, k, default)


class _Request:
    def __init__(self, **args):
        self.args = _Args(args)


class _Doble:
    """Registra las llamadas a mysql_fetchall/mysql_fetchone y devuelve lo
    que el test le programo."""

    def __init__(self):
        self.fetchall_calls = []
        self.fetchone_calls = []
        self.filas = []
        self.modelos = []
        self.maquina = None
        self.producto = None

    def fetchall(self, sql, params=None):
        self.fetchall_calls.append((sql, params))
        if sql.lstrip().startswith("SELECT sm.repuesto_id, p.id"):
            return list(self.modelos)  # la query de chips de modelos compatibles
        return list(self.filas)

    def fetchone(self, sql, params=None):
        self.fetchone_calls.append((sql, params))
        if "FROM mant_maquinas" in sql:
            return self.maquina
        if "FROM cat_productos" in sql:
            return self.producto
        return None


def _cargar_motor(gestion=True, externo=False):
    arbol = _arbol()
    ambito = {"re": __import__("re")}
    quiero = {"ot2_api_repuestos_bodega_buscar", "_otrep_fmt_stock", "_otrep_producto_de_maquina"}
    consts = {"_OTREP_STOCK_SOLO_GESTION", "_OTREP_STOCK_NO_EXTERNO", "_OTREP_ABIERTOS",
              "_OTREP_ESTADOS_COMPROMETEN", "_OTREP_ESTADOS_POR_LLEGAR", "_OTREP_SQL_STOCK",
              "_OTREP_SQL_DISPONIBLE"}
    for nodo in arbol.body:
        if isinstance(nodo, ast.FunctionDef) and nodo.name in quiero:
            nodo.decorator_list = []  # sin @app.route / @_mant_required
            exec(compile(ast.Module(body=[nodo], type_ignores=[]), "<app>", "exec"), ambito)
        elif (isinstance(nodo, ast.Assign) and nodo.targets
              and getattr(nodo.targets[0], "id", "") in consts):
            exec(compile(ast.Module(body=[nodo], type_ignores=[]), "<app>", "exec"), ambito)
    for n in quiero | consts:
        assert n in ambito, f"no se encontro {n} en app.py"
    doble = _Doble()
    ambito["mysql_fetchall"] = doble.fetchall
    ambito["mysql_fetchone"] = doble.fetchone
    ambito["jsonify"] = lambda d: d
    ambito["_otrep_puede_gestion"] = lambda: gestion
    ambito["_es_tecnico_externo"] = lambda user=None: externo
    ambito["print"] = lambda *a, **k: None
    return ambito, doble


def _sql_principal(doble):
    """La query contra mant_repuestos_stock (no la de chips de modelos)."""
    for sql, params in doble.fetchall_calls:
        if "FROM mant_repuestos_stock rs" in sql:
            return sql, params
    return None, None


def _where_extra(sql):
    """Lo que se agrega al WHERE fijo (activo=1): texto/proveedor/marca/modelo.

    Ojo: _OTREP_SQL_STOCK trae WHERE y ORDER BY ANIDADOS dentro de subqueries
    de la SELECT (foto_key, comprometido, por_llegar) -- un sql.split("WHERE")
    o sql.split("ORDER BY") ingenuo (primer occurrence) cae en esos, no en el
    WHERE/ORDER BY reales de la query completa. Se ancla al literal fijo con
    el que siempre arranca el WHERE real (`where = ["COALESCE(rs.activo,1)=1"]`
    en el endpoint), que ya viene DESPUES de todos los subqueries anidados.
    """
    resto = sql.split("WHERE COALESCE(rs.activo,1)=1", 1)[1]
    return resto.split(" ORDER BY ", 1)[0]


def _orden_clause(sql):
    """El ORDER BY real de la query completa (no el anidado de foto_key)."""
    resto = sql.split("WHERE COALESCE(rs.activo,1)=1", 1)[1]
    return resto.split(" ORDER BY ", 1)[1].split(" LIMIT %s", 1)[0]


FILA = {
    "id": 5, "sku": "REP-DRAX-0005", "descripcion": "Correa de motor trotadora",
    "cantidad": 10, "stock_minimo": 2, "codigo_fabricante": "B-77", "costo_unitario": 12000,
    "proveedor_id": 3, "marca_id": 9, "marca": "Drax", "ubicacion_codigo": "A-01",
    "proveedor": "Drax Fitness", "proveedor_contacto": "Juan", "proveedor_telefono": "+56 9",
    "proveedor_email": "j@drax.cl", "proveedor_canal": "whatsapp", "foto_key": None,
    "comprometido": 3, "por_llegar": 2,
}


class TestSemaforoStock(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.amb, _ = _cargar_motor()

    def test_verde_cuando_hay_disponible(self):
        d = self.amb["_otrep_fmt_stock"](dict(FILA))
        self.assertEqual(d["disponible"], 7)
        self.assertEqual(d["por_llegar"], 2)
        self.assertEqual(d["semaforo"], "verde")
        self.assertTrue(d["con_stock"])

    def test_ambar_cuando_todo_esta_comprometido(self):
        d = self.amb["_otrep_fmt_stock"](dict(FILA, cantidad=3, comprometido=3))
        self.assertEqual(d["disponible"], 0)
        self.assertEqual(d["semaforo"], "ambar", "hay fisico pero todo prometido a otras OT")
        self.assertFalse(d["con_stock"])

    def test_rojo_sin_fisico(self):
        d = self.amb["_otrep_fmt_stock"](dict(FILA, cantidad=0, comprometido=0, por_llegar=0))
        self.assertEqual(d["semaforo"], "rojo")

    def test_externo_en_ot_no_recibe_proveedor_ni_stock(self):
        amb, _ = _cargar_motor(gestion=False, externo=True)
        d = amb["_otrep_fmt_stock"](dict(FILA), para_ot=True)
        for k in ("proveedor", "proveedor_id", "proveedor_contacto", "proveedor_telefono",
                  "proveedor_email", "proveedor_canal", "ubicacion_codigo", "cantidad",
                  "comprometido", "disponible", "por_llegar", "semaforo", "costo_unitario"):
            self.assertNotIn(k, d, f"el externo no debe ver {k}")
        self.assertIn("con_stock", d, "el si/no de stock si viaja")

    def test_gestion_nunca_pierde_costo_fuera_de_ot(self):
        d = self.amb["_otrep_fmt_stock"](dict(FILA), para_ot=False)
        self.assertIn("costo_unitario", d)
        self.assertIn("proveedor_canal", d)


class TestMotorBusqueda(unittest.TestCase):

    def _buscar(self, gestion=True, externo=False, filas=None, modelos=None, maquina=None, producto=None, **args):
        amb, doble = _cargar_motor(gestion=gestion, externo=externo)
        doble.filas = filas if filas is not None else [dict(FILA)]
        doble.modelos = modelos if modelos is not None else []
        doble.maquina = maquina
        doble.producto = producto
        amb["request"] = _Request(**args)
        res = amb["ot2_api_repuestos_bodega_buscar"]()
        return res, doble

    def test_sin_texto_ni_filtros_no_consulta(self):
        res, doble = self._buscar(q="a")
        self.assertEqual(res["repuestos"], [])
        self.assertFalse(doble.fetchall_calls, "sin criterio no se toca la base (igual que antes)")

    def test_texto_busca_marca_modelo_y_proveedor(self):
        res, doble = self._buscar(q="drax")
        sql, params = _sql_principal(doble)
        self.assertIn("rs.sku LIKE %s", sql)
        self.assertIn("rs.descripcion LIKE %s", sql)
        self.assertIn("rs.codigo_fabricante LIKE %s", sql)
        self.assertIn("mk.nombre LIKE %s", sql)
        self.assertIn("pq.nombre LIKE %s OR pq.sku LIKE %s", sql, "nombre/SKU de modelo compatible")
        self.assertIn("pv.nombre LIKE %s", sql, "gestion si busca por nombre de proveedor")
        self.assertEqual(params.count("%drax%"), 7)
        # ranking: SKU exacto > "empieza con" > stock disponible > A-Z
        orden = _orden_clause(sql)
        self.assertLess(orden.index("(rs.sku = %s) DESC"), orden.index("(rs.descripcion LIKE %s) DESC"),
                         "SKU exacto antes que 'empieza con'")
        self.assertLess(orden.index("(rs.descripcion LIKE %s) DESC"), orden.index("rs.cantidad -"),
                         "'empieza con' antes que el desempate de stock disponible")
        self.assertTrue(orden.rstrip().endswith("rs.descripcion"), "alfabetico es el ultimo desempate")
        self.assertIn("drax", params)
        self.assertIn("drax%", params)
        self.assertEqual(params[-1], 26, "LIMIT 25 + 1 para detectar truncado")
        self.assertFalse(res["truncado"])

    def test_busca_por_sku_de_modelo_compatible(self):
        # 🔎 Daniel: "buscar por los modelos compatibles... obviamente SKU
        # tambien" -- una sola palabra que sea el SKU del MODELO (no del
        # repuesto) debe entrar por la misma EXISTS de modelos compatibles.
        res, doble = self._buscar(q="TX9-PRO")
        sql, params = _sql_principal(doble)
        self.assertIn("pq.sku LIKE %s", sql)
        self.assertIn("%TX9-PRO%", params)

    def test_multipalabra_es_and_entre_palabras(self):
        # 🔎 Daniel (2026-09-26): "es medio debil el buscador" -- varias
        # palabras deben combinarse con AND, cada una calzando en ALGUN
        # campo (no todas en el mismo). "cable trotadora" debe encontrar un
        # cable cuyo MODELO compatible se llama "Trotadora X9" aunque esa
        # palabra no este en la descripcion del repuesto mismo.
        res, doble = self._buscar(q="cable trotadora")
        sql, params = _sql_principal(doble)
        where = _where_extra(sql)
        # 2 palabras -> 2 grupos OR de 7 campos, unidos por AND
        self.assertEqual(where.count(") AND ("), 1, "2 palabras -> exactamente 1 union AND entre sus 2 grupos")
        self.assertEqual(params.count("%cable%"), 7)
        self.assertEqual(params.count("%trotadora%"), 7)
        self.assertIn("pq.nombre LIKE %s OR pq.sku LIKE %s", sql)

    def test_externo_multipalabra_nunca_ve_proveedor(self):
        res, doble = self._buscar(gestion=False, externo=True, q="cable trotadora", ctx="ot")
        sql, params = _sql_principal(doble)
        self.assertNotIn("pv.nombre LIKE", sql, "ni con varias palabras el externo busca por proveedor")
        self.assertEqual(params.count("%cable%"), 6, "sin el LIKE de proveedor son 6, no 7")
        self.assertEqual(params.count("%trotadora%"), 6)

    def test_externo_no_busca_por_nombre_de_proveedor(self):
        res, doble = self._buscar(gestion=False, externo=True, q="drax", ctx="ot", proveedor_id="3")
        sql, params = _sql_principal(doble)
        self.assertNotIn("pv.nombre LIKE", sql, "el externo no puede deducir proveedores por texto")
        self.assertNotIn("rs.proveedor_id=%s", sql, "ni filtrar por proveedor")
        self.assertNotIn("proveedor", res["repuestos"][0])
        self.assertIn("modelos", res["repuestos"][0], "los chips de modelo (catalogo) si los ve")

    def test_filtro_proveedor_y_marca_sin_texto(self):
        res, doble = self._buscar(proveedor_id="3", marca_id="9")
        sql, params = _sql_principal(doble)
        self.assertIn("rs.proveedor_id=%s", sql)
        self.assertIn("rs.marca_id=%s", sql)
        self.assertNotIn("LIKE", _where_extra(sql), "sin texto no hay LIKE")
        self.assertEqual(tuple(params[:2]), (3, 9))
        self.assertEqual(params[-1], 61, "con filtros el techo sube a 60")

    def test_filtro_sin_proveedor(self):
        res, doble = self._buscar(proveedor_id="sin")
        sql, _ = _sql_principal(doble)
        self.assertIn("rs.proveedor_id IS NULL", sql)

    def test_compatibles_con_equipo_filtra_y_marca_es_compatible(self):
        maquina = {"id": 40, "nombre": "Trotadora X9", "sku": "TX9"}
        producto = {"id": 700, "sku": "TX9", "nombre": "Trotadora X9 Pro"}
        modelos = [{"repuesto_id": 5, "id": 700, "sku": "TX9", "nombre": "Trotadora X9 Pro"}]
        res, doble = self._buscar(maquina_id="40", maquina=maquina, producto=producto, modelos=modelos)
        sql, params = _sql_principal(doble)
        self.assertIn("smf.producto_id=%s", sql, "filtra compatibles con el modelo")
        self.assertIn(700, params)
        self.assertEqual(res["modelo"], {"id": 700, "sku": "TX9", "nombre": "Trotadora X9 Pro"})
        self.assertFalse(res["compatibles_sin_modelo"])
        self.assertTrue(res["repuestos"][0]["es_compatible"])
        self.assertEqual(res["repuestos"][0]["modelos"][0]["nombre"], "Trotadora X9 Pro")

    def test_toda_la_bodega_rankea_compatibles_primero(self):
        maquina = {"id": 40, "nombre": "Trotadora X9", "sku": "TX9"}
        producto = {"id": 700, "sku": "TX9", "nombre": "Trotadora X9 Pro"}
        res, doble = self._buscar(maquina_id="40", solo_compat="0", q="correa", maquina=maquina, producto=producto)
        sql, params = _sql_principal(doble)
        where = _where_extra(sql)
        self.assertNotIn("smf.producto_id", where, "con solo_compat=0 NO se filtra")
        orden = _orden_clause(sql)
        self.assertLess(orden.index("(rs.sku = %s) DESC"), orden.index("smf.producto_id=%s"))
        self.assertLess(orden.index("smf.producto_id=%s"), orden.index("(rs.descripcion LIKE %s) DESC"))
        self.assertLess(orden.index("(rs.descripcion LIKE %s) DESC"), orden.index("rs.cantidad -"),
                         "el desempate de stock disponible va despues de 'empieza con'")
        self.assertEqual(params[-1], 26, "sin filtro real el techo sigue en 25")

    def test_equipo_sin_modelo_en_catalogo_avisa(self):
        maquina = {"id": 41, "nombre": "Bici sin SKU", "sku": None}
        res, doble = self._buscar(maquina_id="41", maquina=maquina)
        self.assertTrue(res["compatibles_sin_modelo"])
        self.assertIsNone(res["modelo"])
        self.assertEqual(res["repuestos"], [])

    def test_modelo_directo_sin_equipo(self):
        producto = {"id": 800, "sku": "LP-1", "nombre": "Leg Press"}
        res, doble = self._buscar(modelo_id="800", producto=producto)
        sql, params = _sql_principal(doble)
        self.assertIn("smf.producto_id=%s", sql)
        self.assertIn(800, params)
        self.assertEqual(res["modelo"]["nombre"], "Leg Press")

    def test_truncado_cuando_llega_al_techo(self):
        filas = [dict(FILA, id=i) for i in range(1, 27)]
        res, doble = self._buscar(q="correa", filas=filas)
        self.assertTrue(res["truncado"])
        self.assertEqual(len(res["repuestos"]), 25)

    def test_texto_del_usuario_nunca_va_en_el_sql(self):
        # Una sola "palabra" (sin espacios) -- ejercita el caso simple de
        # siempre, con y sin el AND multipalabra de por medio.
        malo = "x'OR1=1--"
        res, doble = self._buscar(q=malo)
        sql, params = _sql_principal(doble)
        self.assertNotIn(malo, sql, "REGLA #4: solo %s con parametros")
        self.assertIn("%" + malo + "%", params)

    def test_multipalabra_del_usuario_nunca_va_en_el_sql(self):
        # 🔒 2026-09-26: con el AND multipalabra, CADA palabra debe seguir
        # viajando como su propio parametro -- ninguna, junta ni separada,
        # se cuela como texto literal en el SQL (REGLA #4). Palabras elegidas
        # para NO coincidir por accidente con ningun token real del SQL
        # (SELECT/FROM/WHERE/JOIN/AND/OR/IN/ORDER/BY/DESC/LIKE/EXISTS/...).
        malo = "hax0r'; DROP TABLE zz --"
        res, doble = self._buscar(q=malo)
        sql, params = _sql_principal(doble)
        for palabra in malo.split():
            self.assertNotIn(palabra, sql, f"la palabra {palabra!r} no debe ir literal en el SQL")
            self.assertIn("%" + palabra + "%", params, f"{palabra!r} viaja como su propio parametro escapado")

    def test_comodines_like_se_escapan(self):
        res, doble = self._buscar(q="REP_DRAX100%")
        sql, params = _sql_principal(doble)
        self.assertIn("%REP\\_DRAX100\\%%", params, "_ y % del usuario van escapados")
        self.assertIn("REP\\_DRAX100\\%%", params, "tambien en el ranking 'empieza con'")
        self.assertIn("REP_DRAX100%", params, "el SKU exacto se compara sin escapar")

    def test_comodines_like_se_escapan_por_palabra(self):
        # Con 2 palabras cada una se escapa POR SEPARADO antes de armar su
        # propio "%...%" -- no alcanza con escapar la frase completa.
        res, doble = self._buscar(q="REP_DRAX 100%")
        sql, params = _sql_principal(doble)
        self.assertIn("%REP\\_DRAX%", params, "el _ de la 1a palabra se escapa")
        self.assertIn("%100\\%%", params, "el % de la 2a palabra se escapa")

    def test_gestion_recibe_contacto_del_proveedor(self):
        res, _ = self._buscar(q="correa", ctx="gestion")
        r = res["repuestos"][0]
        self.assertEqual(r["proveedor"], "Drax Fitness")
        self.assertEqual(r["proveedor_canal"], "whatsapp")
        self.assertEqual(r["semaforo"], "verde")
        # Gestion (ctx=gestion) SI recibe el costo, como antes de este cambio
        # (_OTREP_STOCK_SOLO_GESTION solo se recorta con para_ot).
        self.assertIn("costo_unitario", r)


class TestEndpointManualProveedorPorLinea(unittest.TestCase):
    """Reglas de proveedor por linea de POST /repuestos/api/solicitudes/manual
    (se lee el codigo fuente: la funcion depende de get_db/current_username/
    secrets/_mant_log, demasiado para ejecutarla aislada; lo que se fija aca
    es que las validaciones EXISTAN con los mensajes que ve el usuario)."""

    @classmethod
    def setUpClass(cls):
        arbol = _arbol()
        cls.src = None
        for nodo in arbol.body:
            if isinstance(nodo, ast.FunctionDef) and nodo.name == "repstock_solicitud_manual":
                cls.src = ast.get_source_segment(_CACHE["src"], nodo)
        assert cls.src, "no se encontro repstock_solicitud_manual"

    def test_rechaza_bodega_sin_proveedor(self):
        self.assertIn("if stock and not proveedor_id:", self.src)
        self.assertIn("no tiene proveedor", self.src)

    def test_valida_que_el_proveedor_exista(self):
        self.assertIn("SELECT id, nombre FROM mant_proveedores_repuesto WHERE id=%s", self.src)
        self.assertIn("ya no existe", self.src)

    def test_guarda_proveedor_en_el_repuesto_dentro_de_la_transaccion(self):
        self.assertIn("UPDATE mant_repuestos_stock SET proveedor_id=%s, updated_by=%s WHERE id=%s", self.src)
        self.assertIn('_mant_log("repuesto_stock"', self.src)
        # el UPDATE va ANTES del commit del lote (misma transaccion)
        self.assertLess(self.src.index("UPDATE mant_repuestos_stock SET proveedor_id"),
                        self.src.index("\n        conn.commit()"))

    def test_insert_usa_el_proveedor_de_la_linea(self):
        self.assertIn('it.get("proveedor_id"),', self.src)
        self.assertNotIn('(stock.get("proveedor_id") if stock else None),', self.src)


if __name__ == "__main__":
    unittest.main()

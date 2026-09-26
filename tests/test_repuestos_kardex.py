"""Kardex de bodega (Fase 3, 2026-09-26 -- Daniel: "Sí, es la base";
objetivo: "almacenamiento inteligente", que el "disponible" diga siempre
la verdad). Incluye la revisión posterior (mismo día) de hallazgos ALTA/
MEDIA/BAJA sobre el commit daa67adf: reposición de stock propio no
compromete, concurrencia con guard de estado + rowcount, "recibido" sin
respaldo de entrada al instalar, ajuste con `objetivo` bajo FOR UPDATE.

Prueba _repstock_mover() de app.py (idempotencia por (solicitud_id,
motivo_tipo), saldo resultante correcto, delta negativo que avisa pero
no bloquea, `objetivo` recalculado bajo el lock, rollback si falla sin
`cur`), _otrep_fila() (comprometido "propio" correcto tras la revisión:
'pedido' ya no cuenta, 'solicitado' sí cuenta como proyección, una
reposición nunca cuenta), la regla de comprometido/por llegar
(_OTREP_ESTADOS_COMPROMETEN / _OTREP_ESTADOS_POR_LLEGAR), y -- por texto
fuente, no por ejecución -- que las consultas SQL de comprometido excluyan
`es_reposicion` y que los guards de concurrencia/recepción vivan en
_otrep_cambiar_estado (2026-09-26, Fase 4: el cuerpo se movió ahí desde
repstock_solicitud_ot_estado, que quedó como wrapper delgado).

Ninguna de estas funciones es 100% pura (las reales tocan Flask/MySQL),
pero se extraen con ast y se exec-ean en un ámbito aislado con los
símbolos mínimos que necesitan (constantes, un cursor o conexión FALSOS)
-- mismo criterio que tests/test_repuestos_lote_validacion.py usa para
_otrep_validar_lineas_lote (evita levantar Flask/BD/hilos de cron
importando app.py completo).

Correr con:  py -m unittest tests.test_repuestos_kardex
(pytest NO esta instalado en el equipo de Daniel.)
"""
import ast
import os
import sys
import unittest
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
APP_PY = os.path.join(RAIZ, "app.py")


def _leer(path):
    with open(path, encoding="utf-8") as fh:
        return fh.read()


def _extraer_funcion(nombre, contexto=None):
    """Extrae UNA función top-level de app.py por nombre y la ejecuta en un
    ámbito aislado (mismo patrón que test_repuestos_lote_validacion._cargar_
    validador). No importa app.py completo -- eso levanta Flask, la BD y los
    hilos de cron. `contexto`: símbolos globales que la función necesita
    (constantes de módulo, funciones auxiliares) que no vienen con ella."""
    arbol = ast.parse(_leer(APP_PY))
    ambito = dict(contexto or {})
    for nodo in arbol.body:
        if isinstance(nodo, ast.FunctionDef) and nodo.name == nombre:
            exec(compile(ast.Module(body=[nodo], type_ignores=[]), "<app>", "exec"), ambito)
            break
    assert nombre in ambito, f"no se encontró {nombre} en app.py"
    return ambito[nombre]


def _extraer_fuente_funcion(nombre):
    """Devuelve el CÓDIGO FUENTE (sin ejecutar nada) de una función top-level
    de app.py -- para revisar por texto que un guard/condición sigue
    presente, cuando la función en sí no es extraíble/ejecutable de forma
    aislada (depende de Flask, mysql_fetchone, request, etc.)."""
    arbol = ast.parse(_leer(APP_PY))
    for nodo in arbol.body:
        if isinstance(nodo, ast.FunctionDef) and nodo.name == nombre:
            return ast.unparse(nodo)
    raise AssertionError(f"no se encontró {nombre} en app.py")


def _extraer_constante_tupla(nombre):
    """Extrae el VALOR literal de una constante top-level de app.py definida
    como `NOMBRE = (...)` -- sin ejecutar nada de app.py, solo lee el
    literal del AST (ast.literal_eval). Sirve para tuplas de strings o
    números simples (_OTREP_ESTADOS_COMPROMETEN, _OTREP_ANTIGUEDAD_DIAS_*)."""
    arbol = ast.parse(_leer(APP_PY))
    for nodo in arbol.body:
        if isinstance(nodo, ast.Assign) and len(nodo.targets) == 1:
            target = nodo.targets[0]
            if isinstance(target, ast.Name) and target.id == nombre:
                return ast.literal_eval(nodo.value)
    raise AssertionError(f"no se encontró la constante {nombre} en app.py")


def _extraer_sql_constante(nombre, contexto):
    """Extrae una constante top-level que es una expresión SQL armada por
    concatenación de strings (ej. `_OTREP_SQL_STOCK`, que usa
    "','".join(_OTREP_ESTADOS_COMPROMETEN) -- no es un literal puro, así que
    ast.literal_eval no sirve). Se ejecuta SOLO esa asignación con las
    constantes de las que depende ya provistas en `contexto`."""
    arbol = ast.parse(_leer(APP_PY))
    for nodo in arbol.body:
        if isinstance(nodo, ast.Assign) and len(nodo.targets) == 1:
            target = nodo.targets[0]
            if isinstance(target, ast.Name) and target.id == nombre:
                ambito = dict(contexto)
                exec(compile(ast.Module(body=[nodo], type_ignores=[]), "<app>", "exec"), ambito)
                return ambito[nombre]
    raise AssertionError(f"no se encontró la constante {nombre} en app.py")


class FakeCursor:
    """Cursor falso: registra cada `execute(sql, params)` en `self.executed`
    y devuelve `fetchone()` en el orden de `self.respuestas` (una cola) --
    así la prueba controla exactamente qué "ve" _repstock_mover sin tocar
    MySQL de verdad."""

    def __init__(self, respuestas):
        self.executed = []
        self._respuestas = list(respuestas)

    def execute(self, sql, params=None):
        self.executed.append((" ".join(sql.split()), params))

    def fetchone(self):
        return self._respuestas.pop(0) if self._respuestas else None


class TestRepstockMover(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.mover = staticmethod(_extraer_funcion("_repstock_mover"))

    def test_entrada_suma_al_saldo_y_registra_movimiento(self):
        cur = FakeCursor([{"cantidad": 10}])
        r = self.mover(55, 5, "entrada", "recepcion_proveedor",
                        solicitud_id=None, usuario="tester", cur=cur)
        self.assertTrue(r["movido"])
        self.assertFalse(r["ya_existia"])
        self.assertEqual(r["saldo"], 15)
        self.assertIsNone(r["aviso"])
        # SELECT ... FOR UPDATE primero (mutex real contra otro movimiento
        # simultáneo del mismo repuesto).
        self.assertIn("FOR UPDATE", cur.executed[0][0])
        self.assertEqual(cur.executed[0][1], (55,))
        # UPDATE con el saldo YA sumado, no el delta crudo.
        self.assertIn("UPDATE mant_repuestos_stock SET cantidad=%s", cur.executed[1][0])
        self.assertEqual(cur.executed[1][1], (15, 55))
        # INSERT del movimiento con el mismo saldo_resultante.
        self.assertIn("INSERT INTO mant_repuestos_movimientos", cur.executed[2][0])
        self.assertEqual(cur.executed[2][1][3], 5)   # cantidad (con signo)
        self.assertEqual(cur.executed[2][1][4], 15)  # saldo_resultante

    def test_salida_resta_del_saldo(self):
        cur = FakeCursor([{"cantidad": 8}])
        r = self.mover(9, -3, "salida", "instalacion", usuario="tester", cur=cur)
        self.assertEqual(r["saldo"], 5)
        self.assertIsNone(r["aviso"])

    def test_delta_negativo_deja_saldo_negativo_avisa_no_bloquea(self):
        # Mismo criterio "avisa pero no bloquea" que ya usa el resto del
        # módulo de repuestos (sobrecompromiso al validar una solicitud).
        cur = FakeCursor([{"cantidad": 2}])
        r = self.mover(9, -5, "salida", "instalacion", usuario="tester", cur=cur)
        self.assertTrue(r["movido"], "el movimiento se aplica igual, no se rechaza")
        self.assertEqual(r["saldo"], -3)
        self.assertIsNotNone(r["aviso"])
        self.assertIn("negativo", r["aviso"])

    def test_ajuste_manual_con_delta_positivo(self):
        cur = FakeCursor([{"cantidad": 20}])
        r = self.mover(1, 4.5, "ajuste", "ajuste_manual",
                        nota="Conteo físico", usuario="bodega", cur=cur)
        self.assertEqual(r["saldo"], 24.5)
        # La nota viaja hasta el INSERT del movimiento.
        self.assertEqual(cur.executed[-1][1][8], "Conteo físico")

    def test_idempotencia_recepcion_proveedor_no_duplica(self):
        # Doble click, o un reintento de red, en la transición a "recibido"
        # de la MISMA solicitud: la segunda llamada no debe volver a sumar
        # stock ni insertar un segundo movimiento.
        cur = FakeCursor([{"id": 1, "saldo_resultante": 12}])
        r = self.mover(3, 6, "entrada", "recepcion_proveedor",
                        solicitud_id=42, usuario="tester", cur=cur)
        self.assertFalse(r["movido"])
        self.assertTrue(r["ya_existia"])
        self.assertEqual(r["saldo"], 12)
        # Solo se ejecutó el SELECT de idempotencia -- nunca tocó
        # mant_repuestos_stock ni insertó un movimiento nuevo.
        self.assertEqual(len(cur.executed), 1)
        self.assertIn("mant_repuestos_movimientos", cur.executed[0][0])
        self.assertIn("solicitud_id=%s AND motivo_tipo=%s", cur.executed[0][0])
        self.assertEqual(cur.executed[0][1], (42, "recepcion_proveedor"))

    def test_idempotencia_instalacion_no_duplica(self):
        cur = FakeCursor([{"id": 2, "saldo_resultante": 3}])
        r = self.mover(3, -6, "salida", "instalacion",
                        solicitud_id=99, usuario="tester", cur=cur)
        self.assertFalse(r["movido"])
        self.assertTrue(r["ya_existia"])
        self.assertEqual(len(cur.executed), 1)

    def test_ajuste_manual_con_solicitud_id_no_es_idempotente(self):
        # Idempotencia (spec Fase 3) es SOLO para recepcion_proveedor/
        # instalacion -- un ajuste manual corre siempre, aunque por algún
        # motivo trajera un solicitud_id.
        cur = FakeCursor([{"cantidad": 10}])
        r = self.mover(3, 1, "ajuste", "ajuste_manual",
                        solicitud_id=42, usuario="tester", cur=cur)
        self.assertTrue(r["movido"])
        self.assertIn("FOR UPDATE", cur.executed[0][0])

    def test_repuesto_inexistente_lanza(self):
        cur = FakeCursor([None])   # el SELECT ... FOR UPDATE no encuentra fila
        with self.assertRaises(ValueError):
            self.mover(999999, 1, "ajuste", "ajuste_manual", usuario="tester", cur=cur)

    def test_objetivo_calcula_delta_bajo_el_lock_no_con_el_delta_viejo(self):
        # Revisión Fase 3, hallazgo BAJA #11: el caller (repstock_editar) ya
        # NO calcula el delta con una lectura de ANTES del FOR UPDATE -- pasa
        # `objetivo` (la cantidad final que quiere) y acá se recalcula contra
        # lo que la fila tenga EN ESE INSTANTE (12, no los 10 que el caller
        # pudo haber leído hace un momento). `delta` (aquí 999, un valor
        # obviamente viejo/incorrecto) debe ser TOTALMENTE ignorado.
        cur = FakeCursor([{"cantidad": 12}])
        r = self.mover(7, 999, "ajuste", "ajuste_manual", objetivo=20, usuario="tester", cur=cur)
        self.assertEqual(r["saldo"], 20)
        # El movimiento queda con el delta REAL (20-12=8), no con el 999
        # que se pasó (y que jamás debió usarse) ni con el 10 viejo.
        self.assertEqual(cur.executed[-1][1][3], 8)

    def test_objetivo_igual_al_actual_no_mueve_nada_pero_no_falla(self):
        cur = FakeCursor([{"cantidad": 15}])
        r = self.mover(7, 0, "ajuste", "ajuste_manual", objetivo=15, usuario="tester", cur=cur)
        self.assertEqual(r["saldo"], 15)
        self.assertEqual(cur.executed[-1][1][3], 0)

    def test_sin_cur_hace_rollback_si_falla(self):
        # Revisión Fase 3, hallazgo BAJA #10: sin `cur` (transacción propia
        # sobre get_db()), un fallo a mitad de camino debe hacer rollback
        # antes de relanzar -- nunca dejar la conexión del request colgada
        # en una transacción a medias.
        class FakeConn:
            def __init__(self, cur):
                self._cur = cur
                self.committed = False
                self.rolled_back = False

            def cursor(self):
                conn = self

                class _Ctx:
                    def __enter__(self_ctx):
                        return conn._cur

                    def __exit__(self_ctx, *a):
                        return False
                return _Ctx()

            def commit(self):
                self.committed = True

            def rollback(self):
                self.rolled_back = True

        cur = FakeCursor([None])   # repuesto no existe -> _do lanza ValueError
        conn = FakeConn(cur)
        mover_sin_cur = _extraer_funcion("_repstock_mover", contexto={"get_db": lambda: conn})
        with self.assertRaises(ValueError):
            mover_sin_cur(999999, 1, "ajuste", "ajuste_manual", usuario="tester")
        self.assertTrue(conn.rolled_back, "debe hacer rollback si _do() lanza")
        self.assertFalse(conn.committed, "nunca debe comitear un fallo")

    def test_sin_cur_comitea_si_sale_bien(self):
        class FakeConn:
            def __init__(self, cur):
                self._cur = cur
                self.committed = False
                self.rolled_back = False

            def cursor(self):
                conn = self

                class _Ctx:
                    def __enter__(self_ctx):
                        return conn._cur

                    def __exit__(self_ctx, *a):
                        return False
                return _Ctx()

            def commit(self):
                self.committed = True

            def rollback(self):
                self.rolled_back = True

        cur = FakeCursor([{"cantidad": 5}])
        conn = FakeConn(cur)
        mover_sin_cur = _extraer_funcion("_repstock_mover", contexto={"get_db": lambda: conn})
        r = mover_sin_cur(3, 2, "entrada", "recepcion_proveedor", usuario="tester")
        self.assertEqual(r["saldo"], 7)
        self.assertTrue(conn.committed)
        self.assertFalse(conn.rolled_back)


class TestOtrepFilaComprometidoPropio(unittest.TestCase):
    """_otrep_fila calcula `stock_comprometido`/`stock_disponible` de CADA
    solicitud para la cola -- revisión Fase 3, hallazgo MEDIA #6: antes
    contaba la cantidad "propia" de la fila cuando `abierta` (que incluía
    'pedido', que YA NO compromete) y dejaba fuera 'solicitado' (que SÍ
    debería contar como proyección). Hallazgo ALTA #1: una reposición nunca
    cuenta como "propia" del comprometido."""

    @classmethod
    def setUpClass(cls):
        comprometen = _extraer_constante_tupla("_OTREP_ESTADOS_COMPROMETEN")
        contexto = {
            "_OTREP_ESTADO_LABEL": {},
            "_OTREP_ORIGEN_LABEL": {},
            "_OTREP_ABIERTOS": _extraer_constante_tupla("_OTREP_ABIERTOS"),
            "_OTREP_TRANSICIONES": {},
            "_OTREP_ESTADOS_COMPROMETEN": comprometen,
            "_OTREP_ANTIGUEDAD_DIAS_VERDE": _extraer_constante_tupla("_OTREP_ANTIGUEDAD_DIAS_VERDE"),
            "_OTREP_ANTIGUEDAD_DIAS_AMBAR": _extraer_constante_tupla("_OTREP_ANTIGUEDAD_DIAS_AMBAR"),
            "datetime": datetime,
            # 🔒 2026-09-26 (revisión Opus, hallazgo ALTA #1 -- Vida del
            # cliente Etapa A): _otrep_fila ahora empieza llamando a
            # _es_rol_tecnico() para esconder costo_unitario/costo_origen
            # de cualquier técnico. Stub neutro (False) -- estas pruebas
            # son sobre comprometido/disponible, no sobre ese candado.
            "_es_rol_tecnico": lambda *a, **k: False,
            "_OTREP_SOL_SOLO_GESTION": ("costo_unitario", "costo_origen"),
        }
        cls.fila = staticmethod(_extraer_funcion("_otrep_fila", contexto=contexto))

    def _base(self, **over):
        s = {"id": 1, "estado": "solicitado", "cantidad": 5, "repuesto_stock_id": 9,
             "stock_cantidad": 20, "comprometido_otras": 3, "es_reposicion": 0}
        s.update(over)
        return s

    def test_pedido_ya_no_cuenta_como_propio(self):
        r = self.fila(self._base(estado="pedido"))
        # comprometido_otras (3) + propia (0, 'pedido' no comprometen más)
        self.assertEqual(r["stock_comprometido"], 3.0)
        self.assertEqual(r["stock_disponible"], 17.0)

    def test_solicitado_cuenta_como_proyeccion(self):
        r = self.fila(self._base(estado="solicitado"))
        self.assertEqual(r["stock_comprometido"], 3.0 + 5.0)

    def test_validado_cuenta(self):
        r = self.fila(self._base(estado="validado"))
        self.assertEqual(r["stock_comprometido"], 3.0 + 5.0)

    def test_recibido_cuenta(self):
        r = self.fila(self._base(estado="recibido"))
        self.assertEqual(r["stock_comprometido"], 3.0 + 5.0)

    def test_instalado_no_cuenta(self):
        r = self.fila(self._base(estado="instalado"))
        self.assertEqual(r["stock_comprometido"], 3.0)

    def test_rechazado_no_cuenta(self):
        r = self.fila(self._base(estado="rechazado"))
        self.assertEqual(r["stock_comprometido"], 3.0)

    def test_reposicion_nunca_cuenta_aunque_este_validada(self):
        r = self.fila(self._base(estado="validado", es_reposicion=1))
        self.assertEqual(r["stock_comprometido"], 3.0,
                          "una reposición trae stock, no lo consume -- nunca es 'propia'")


class TestSqlComprometidoExcluyeReposicion(unittest.TestCase):
    """Hallazgo ALTA #1 (revisión Fase 3): una reposición de stock propio no
    compite por el físico existente -- TODAS las consultas de comprometido
    deben excluir `es_reposicion=1`. Se revisa por TEXTO de la consulta
    (no hay BD real en estos tests) -- constantes SQL exec-eadas con sus
    dependencias, y funciones grandes revisadas por su fuente."""

    def test_otrep_sql_stock_excluye_reposicion(self):
        ctx = {
            "_OTREP_ESTADOS_COMPROMETEN": _extraer_constante_tupla("_OTREP_ESTADOS_COMPROMETEN"),
            "_OTREP_ESTADOS_POR_LLEGAR": _extraer_constante_tupla("_OTREP_ESTADOS_POR_LLEGAR"),
        }
        sql = _extraer_sql_constante("_OTREP_SQL_STOCK", ctx)
        self.assertIn("es_reposicion", sql)
        self.assertIn("validado", sql)

    def test_otrep_sql_sol_comprometido_otras_excluye_reposicion(self):
        ctx = {"_OTREP_ESTADOS_COMPROMETEN": _extraer_constante_tupla("_OTREP_ESTADOS_COMPROMETEN")}
        sql = _extraer_sql_constante("_OTREP_SQL_SOL", ctx)
        self.assertIn("es_reposicion", sql)

    def test_otrep_stock_comprometido_excluye_reposicion(self):
        fuente = _extraer_fuente_funcion("_otrep_stock_comprometido")
        self.assertIn("es_reposicion", fuente)

    def test_repstock_contexto_bodega_excluye_reposicion(self):
        fuente = _extraer_fuente_funcion("_repstock_contexto_bodega")
        self.assertIn("es_reposicion", fuente)


class TestGuardsSolicitudEstado(unittest.TestCase):
    """_otrep_cambiar_estado no es extraíble/ejecutable aislado (Flask,
    mysql_fetchone, request, _repstock_mover...) -- se revisa por TEXTO
    fuente que los guards de la revisión Fase 3 siguen presentes. Cambia
    de intención a "no se borró el guard", no a "el guard funciona con
    datos reales" (eso ya lo cubre _repstock_mover más arriba, que sí es
    ejecutable).

    🏗️ Fase 4 (2026-09-26, merge con main): el cuerpo entero de
    `repstock_solicitud_ot_estado` (guards de Fase 3 incluidos) se movió a
    `_otrep_cambiar_estado` -- la ruta original quedó como wrapper delgado
    que solo llama a esa función y traduce el resultado a JSON. Se revisa
    la fuente de la función NUEVA, que es donde estos guards viven ahora."""

    @classmethod
    def setUpClass(cls):
        cls.fuente = _extraer_fuente_funcion("_otrep_cambiar_estado")

    def test_concurrencia_exige_estado_actual_en_el_update(self):
        # MEDIA #2: el UPDATE debe filtrar también por el estado leído al
        # principio, y revisar rowcount antes de seguir.
        self.assertIn("estado=%s", self.fuente)
        self.assertIn("rowcount", self.fuente)
        self.assertIn("_OtrepConflictoEstado", self.fuente)

    def test_reposicion_bloquea_instalado(self):
        # ALTA #1: no se puede "instalar" una reposición. ast.unparse()
        # normaliza las comillas a simples, así que se busca sin asumir
        # cuáles usaba el código fuente original.
        self.assertIn('es_reposicion', self.fuente)
        self.assertRegex(
            self.fuente,
            r"nuevo == ['\"]instalado['\"] and s\.get\(['\"]es_reposicion['\"]\)")

    def test_instalado_desde_recibido_exige_movimiento_de_entrada_previo(self):
        # MEDIA #3: sin el 'recepcion_proveedor' de ESTA solicitud, no se
        # descuenta stock al instalar.
        self.assertIn("recepcion_proveedor", self.fuente)
        self.assertIn("_tiene_entrada", self.fuente)

    def test_recibido_exige_pedido_at_salvo_reposicion(self):
        # BAJA #7: sin pasar por 'pedido' (pedido_at) y sin ser reposición,
        # no se suma stock solo por llegar a 'recibido'.
        self.assertIn('pedido_at', self.fuente)


class TestReglaComprometidoPorLlegar(unittest.TestCase):
    """2026-09-26: 'pedido' deja de comprometer stock existente (está
    esperando al proveedor, no consumiendo lo que ya hay en bodega) --
    comprometen solo 'validado' y 'recibido'; 'pedido' pasa a "por llegar"."""

    def test_comprometen_es_exactamente_validado_y_recibido(self):
        comprometen = _extraer_constante_tupla("_OTREP_ESTADOS_COMPROMETEN")
        self.assertEqual(set(comprometen), {"validado", "recibido"})

    def test_por_llegar_es_exactamente_pedido(self):
        por_llegar = _extraer_constante_tupla("_OTREP_ESTADOS_POR_LLEGAR")
        self.assertEqual(set(por_llegar), {"pedido"})

    def test_comprometido_y_por_llegar_son_disjuntos(self):
        comprometen = set(_extraer_constante_tupla("_OTREP_ESTADOS_COMPROMETEN"))
        por_llegar = set(_extraer_constante_tupla("_OTREP_ESTADOS_POR_LLEGAR"))
        self.assertEqual(comprometen & por_llegar, set(),
                          "un estado no puede comprometer Y estar 'por llegar' a la vez")

    def test_solicitado_y_rechazado_no_comprometen(self):
        # 'solicitado' todavía no fue validado por bodega contra un
        # repuesto real -- y 'rechazado'/'instalado' ya no están "vivos".
        comprometen = _extraer_constante_tupla("_OTREP_ESTADOS_COMPROMETEN")
        self.assertNotIn("solicitado", comprometen)
        self.assertNotIn("rechazado", comprometen)
        self.assertNotIn("instalado", comprometen)


if __name__ == "__main__":
    unittest.main()

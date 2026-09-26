"""Kardex de bodega (Fase 3, 2026-09-26 -- Daniel: "Sí, es la base";
objetivo: "almacenamiento inteligente", que el "disponible" diga siempre
la verdad).

Prueba _repstock_mover() de app.py (idempotencia por (solicitud_id,
motivo_tipo), saldo resultante correcto, delta negativo que avisa pero
no bloquea) y la regla de comprometido/por llegar (_OTREP_ESTADOS_
COMPROMETEN / _OTREP_ESTADOS_POR_LLEGAR): 'pedido' dejó de comprometer
stock físico, solo 'validado'/'recibido' lo hacen.

_repstock_mover NO es una función 100% pura (en el caso general abre su
propia conexión con get_db()), pero cuando el caller le pasa `cur` (nuestro
caso de prueba) nunca toca get_db()/current_username -- así que se puede
extraer con ast y exec-earla en un ámbito aislado con un cursor FALSO,
mismo criterio que tests/test_repuestos_lote_validacion.py usa para
_otrep_validar_lineas_lote (evita levantar Flask/BD/hilos de cron
importando app.py completo).

Correr con:  py -m unittest tests.test_repuestos_kardex
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


def _extraer_funcion(nombre):
    """Extrae UNA función top-level de app.py por nombre y la ejecuta en un
    ámbito aislado (mismo patrón que test_repuestos_lote_validacion._cargar_
    validador). No importa app.py completo -- eso levanta Flask, la BD y los
    hilos de cron."""
    arbol = ast.parse(_leer(APP_PY))
    ambito = {}
    for nodo in arbol.body:
        if isinstance(nodo, ast.FunctionDef) and nodo.name == nombre:
            exec(compile(ast.Module(body=[nodo], type_ignores=[]), "<app>", "exec"), ambito)
            break
    assert nombre in ambito, f"no se encontró {nombre} en app.py"
    return ambito[nombre]


def _extraer_constante_tupla(nombre):
    """Extrae el VALOR literal de una constante top-level de app.py definida
    como `NOMBRE = (...)` -- sin ejecutar nada de app.py, solo lee el
    literal del AST (ast.literal_eval). Sirve para _OTREP_ESTADOS_
    COMPROMETEN / _OTREP_ESTADOS_POR_LLEGAR, que son tuplas de strings."""
    arbol = ast.parse(_leer(APP_PY))
    for nodo in arbol.body:
        if isinstance(nodo, ast.Assign) and len(nodo.targets) == 1:
            target = nodo.targets[0]
            if isinstance(target, ast.Name) and target.id == nombre:
                return ast.literal_eval(nodo.value)
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

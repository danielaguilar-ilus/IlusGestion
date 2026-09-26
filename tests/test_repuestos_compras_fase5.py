"""Fase 5 (2026-09-26) -- compra a proveedor con ticket y seguimiento
(Daniel: "si no tiene stock, nos vayamos a los tickets, generemos un
ticket de atencion con los datos de los proveedores... con total
trazabilidad y viendo los estados"; "puedo hacer dos tickets... agrupar
las solicitudes de repuestos por proveedor").

Prueba dos funciones PURAS de app.py (sin BD ni Flask):

  _otrep_compra_elegible(s)
      Que solicitudes puede agrupar una Compra nueva. 🔒 2026-09-26
      (revision post-Fase 5, hallazgo ALTA #3): se simplifico a proposito
      -- ya NO existe el camino "solicitado ya ligado a proveedor" (eso
      hubiera exigido tocar el mapa GLOBAL _OTREP_TRANSICIONES para dejar
      saltar 'solicitado' -> 'pedido'). Ahora solo 'validado' (con stock
      insuficiente, hallazgo MEDIA #9) o 'pedido' sin compra_id (Fase <5).

  _otrep_recepcion_es_completa(cantidad_recibida, cantidad_requerida)
      La regla de "recepcion parcial" del endpoint POST
      .../compras/<cid>/recibir: si lo recibido ACUMULADO no alcanza lo
      pedido, la linea sigue 'pedido' con una nota en vez de pasar a
      'recibido'.

Ambas son funciones puras: se extraen de app.py con ast en vez de
importarlo entero (importar app.py completo levanta Flask, la base y los
hilos de cron -- mismo criterio que tests/test_repuestos_lote_validacion.py
y tests/test_bodega_alerta.py).

Correr con:  py -m unittest tests.test_repuestos_compras_fase5
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


def _cargar_funcion(nombre):
    """Extrae UNA funcion de app.py por nombre y la ejecuta en un ambito
    aislado. Solo sirve para funciones PURAS (que no llaman a otros
    simbolos de app.py, ni a Flask/MySQL)."""
    arbol = ast.parse(_leer(APP_PY))
    ambito = {}
    for nodo in arbol.body:
        if isinstance(nodo, ast.FunctionDef) and nodo.name == nombre:
            exec(compile(ast.Module(body=[nodo], type_ignores=[]), "<app>", "exec"), ambito)
            break
    assert nombre in ambito, f"no se encontro {nombre} en app.py"
    return ambito[nombre]


class TestCompraElegible(unittest.TestCase):
    """_otrep_compra_elegible: ver el docstring en app.py (2026-09-26,
    revision post-Fase 5) para la definicion completa de las 2 categorias
    elegibles."""

    @classmethod
    def setUpClass(cls):
        cls.elegible = staticmethod(_cargar_funcion("_otrep_compra_elegible"))

    def test_validado_sin_stock_suficiente_es_elegible(self):
        # Bodega ya la reviso y la ligo a un repuesto real, pero el
        # disponible no alcanza -- eso es lo que hay que comprarle a
        # alguien (hallazgo MEDIA #9: "sin stock suficiente").
        s = {"estado": "validado", "repuesto_stock_id": 5, "proveedor_id": None,
             "compra_id": None, "cantidad": 3, "stock_disponible": 1}
        self.assertTrue(self.elegible(s))

    def test_validado_con_stock_de_sobra_no_es_elegible(self):
        # Si el disponible alcanza y sobra, no hay nada que comprarle a
        # nadie -- REGLA de negocio nueva (hallazgo MEDIA #9).
        s = {"estado": "validado", "repuesto_stock_id": 5, "proveedor_id": None,
             "compra_id": None, "cantidad": 2, "stock_disponible": 5}
        self.assertFalse(self.elegible(s))

    def test_validado_disponible_exactamente_igual_no_es_elegible(self):
        s = {"estado": "validado", "repuesto_stock_id": 5, "proveedor_id": None,
             "compra_id": None, "cantidad": 3, "stock_disponible": 3}
        self.assertFalse(self.elegible(s))

    def test_validado_sin_dato_de_stock_disponible_es_elegible(self):
        # Defensivo: si por lo que sea no vino stock_disponible (None), se
        # prefiere dejarla pasar (mejor ofrecerla de mas que esconderla).
        s = {"estado": "validado", "repuesto_stock_id": 5, "proveedor_id": None,
             "compra_id": None, "cantidad": 3, "stock_disponible": None}
        self.assertTrue(self.elegible(s))

    def test_solicitado_ya_no_es_elegible_bajo_ningun_caso(self):
        # 🔒 ALTA #3: se elimino el camino "solicitado ya ligado a
        # proveedor" -- ahora SIEMPRE hay que pasar por 'validado' primero,
        # sin excepcion, aunque tenga proveedor_id y no tenga stock ligado.
        s = {"estado": "solicitado", "repuesto_stock_id": None, "proveedor_id": 3, "compra_id": None}
        self.assertFalse(self.elegible(s))

    def test_pedido_sin_compra_es_elegible_solo_para_adjuntar(self):
        # Ya se le pidio al proveedor ANTES de que existiera esta Compra
        # (Fase <5, individual): se adjunta a la Compra nueva.
        s = {"estado": "pedido", "repuesto_stock_id": None, "proveedor_id": 3, "compra_id": None}
        self.assertTrue(self.elegible(s))

    def test_pedido_con_compra_ya_asignada_no_es_elegible(self):
        # Ya esta en OTRA compra -- no se puede meter en una segunda.
        s = {"estado": "pedido", "repuesto_stock_id": None, "proveedor_id": 3, "compra_id": 42}
        self.assertFalse(self.elegible(s))

    def test_recibido_no_es_elegible(self):
        s = {"estado": "recibido", "repuesto_stock_id": 9, "proveedor_id": 3, "compra_id": None}
        self.assertFalse(self.elegible(s))

    def test_instalado_no_es_elegible(self):
        s = {"estado": "instalado", "repuesto_stock_id": 9, "proveedor_id": 3, "compra_id": None}
        self.assertFalse(self.elegible(s))

    def test_rechazado_no_es_elegible(self):
        s = {"estado": "rechazado", "repuesto_stock_id": None, "proveedor_id": 3, "compra_id": None}
        self.assertFalse(self.elegible(s))


class TestRecepcionEsCompleta(unittest.TestCase):
    """_otrep_recepcion_es_completa: regla de "recepcion parcial" del
    endpoint POST /repuestos/api/compras/<cid>/recibir."""

    @classmethod
    def setUpClass(cls):
        cls.completa = staticmethod(_cargar_funcion("_otrep_recepcion_es_completa"))

    def test_recibido_igual_a_lo_pedido_es_completa(self):
        self.assertTrue(self.completa(5, 5))

    def test_recibido_de_mas_es_completa(self):
        self.assertTrue(self.completa(6, 5))

    def test_recibido_de_menos_es_parcial(self):
        self.assertFalse(self.completa(3, 5))

    def test_tolerancia_de_punto_flotante(self):
        # 0.1 + 0.2 == 0.30000000000000004 en float -- la tolerancia evita
        # que una cantidad EXACTA quede marcada como parcial por un error
        # de redondeo binario.
        self.assertTrue(self.completa(0.1 + 0.2, 0.3))

    def test_cero_recibido_no_es_completa(self):
        self.assertFalse(self.completa(0, 5))

    def test_valor_no_numerico_no_revienta_y_no_es_completa(self):
        self.assertFalse(self.completa("no-numero", 5))
        self.assertFalse(self.completa(None, 5))

    def test_requerida_cero_con_algo_recibido_es_completa(self):
        # Caso borde (no deberia darse en la practica: una solicitud
        # siempre pide > 0), pero la funcion no debe reventar.
        self.assertTrue(self.completa(1, 0))


if __name__ == "__main__":
    unittest.main()

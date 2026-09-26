"""Validacion del lote "una OT para VARIAS solicitudes del mismo cliente"
(Fase 4, 2026-09-26 -- Daniel: "puedo gestionar una orden de trabajo en
base a que vayan a instalar todos esos repuestos o uno solo, con el filtro
de que tiene que ser siempre el mismo cliente"; "para generar una OT, por
cliente").

Prueba _otrep_validar_lote_mismo_cliente() de app.py: mismo cliente_id (no
NULL), ninguna solicitud con ot_generada_id ya seteado, y estado
'recibido' o 'validado' con stock_disponible >= 0 (el "stock agregado del
lote" ya viene resuelto en el propio stock_disponible de cada fila -- ver
_otrep_fila, que lo calcula GLOBAL sobre TODAS las solicitudes abiertas de
ese repuesto, no solo las del lote). Es una funcion PURA (sin BD ni
Flask), asi que se extrae de app.py con ast en vez de importarlo entero --
importar app.py completo levanta Flask, la base y los hilos de cron (mismo
criterio que tests/test_repuestos_lote_validacion.py y
tests/test_bodega_alerta.py).

Correr con:  py -m unittest tests.test_preparar_ot_lote_validacion
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


def _cargar_validador():
    """Extrae _otrep_validar_lote_mismo_cliente de app.py y la ejecuta en
    un ambito aislado. Funcion pura: no necesita ningun otro simbolo de
    app.py (no toca BD, Flask ni globals del modulo)."""
    arbol = ast.parse(_leer(APP_PY))
    ambito = {}
    for nodo in arbol.body:
        if isinstance(nodo, ast.FunctionDef) and nodo.name == "_otrep_validar_lote_mismo_cliente":
            exec(compile(ast.Module(body=[nodo], type_ignores=[]), "<app>", "exec"), ambito)
            break
    assert "_otrep_validar_lote_mismo_cliente" in ambito, (
        "no se encontro _otrep_validar_lote_mismo_cliente en app.py")
    return ambito["_otrep_validar_lote_mismo_cliente"]


def _sol(id, cliente_id=1, estado="recibido", ot_generada_id=None,
         stock_disponible=None, repuesto_nombre="Correa", stock_sku=None,
         estado_label=None):
    """Fila mínima con la MISMA forma que devuelve _otrep_fila (solo los
    campos que _otrep_validar_lote_mismo_cliente realmente lee)."""
    return {
        "id": id, "cliente_id": cliente_id, "estado": estado,
        "ot_generada_id": ot_generada_id, "stock_disponible": stock_disponible,
        "repuesto_nombre": repuesto_nombre, "stock_sku": stock_sku,
        "estado_label": estado_label or {
            "recibido": "Recibido en bodega", "validado": "Validado en bodega",
            "pedido": "Pedido al proveedor", "solicitado": "Solicitado · por validar en bodega",
            "instalado": "Instalado", "rechazado": "Rechazado",
        }.get(estado, estado),
    }


class TestValidarLotePreparaOt(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.validar = staticmethod(_cargar_validador())

    # ── mismo cliente ───────────────────────────────────────────────────
    def test_lote_del_mismo_cliente_recibido_pasa(self):
        sols = [_sol(1, cliente_id=5), _sol(2, cliente_id=5)]
        err, http = self.validar(sols)
        self.assertIsNone(err)
        self.assertIsNone(http)

    def test_lote_de_clientes_distintos_se_rechaza(self):
        sols = [_sol(1, cliente_id=5), _sol(2, cliente_id=6)]
        err, http = self.validar(sols)
        self.assertIsNotNone(err)
        self.assertIn("MISMO cliente", err)
        self.assertEqual(http, 400)

    def test_reposicion_sin_cliente_no_se_puede_mezclar(self):
        # "reposición de stock propio, sin cliente, no se puede mezclar en
        # una OT de cliente" -- cliente_id None en CUALQUIER fila rechaza
        # todo el lote, aunque las demás compartan cliente.
        sols = [_sol(1, cliente_id=5), _sol(2, cliente_id=None)]
        err, http = self.validar(sols)
        self.assertIsNotNone(err)
        self.assertEqual(http, 400)

    def test_mensaje_especifico_cual_solicitud_no_tiene_cliente(self):
        # B3 (coordinación post-merge, 2026-09-26): el mensaje debe decir
        # CUÁL solicitud es la que no tiene cliente, no solo "deben ser del
        # mismo cliente" -- así el usuario sabe qué sacar del lote.
        sols = [_sol(1, cliente_id=5), _sol(2, cliente_id=None)]
        err, http = self.validar(sols)
        self.assertIn("#2", err)
        self.assertIn("no tiene cliente asignado", err)
        self.assertEqual(http, 400)

    def test_lote_vacio_se_rechaza(self):
        err, http = self.validar([])
        self.assertIsNotNone(err)
        self.assertEqual(http, 400)

    # ── ya tiene OT generada ────────────────────────────────────────────
    def test_solicitud_con_ot_generada_rechaza_todo_el_lote(self):
        sols = [_sol(1, cliente_id=5), _sol(2, cliente_id=5, ot_generada_id=99)]
        err, http = self.validar(sols)
        self.assertIsNotNone(err)
        self.assertIn("#2", err)
        self.assertEqual(http, 409)

    # ── estados ─────────────────────────────────────────────────────────
    def test_estado_pedido_no_es_apto_se_rechaza(self):
        sols = [_sol(1, cliente_id=5, estado="pedido")]
        err, http = self.validar(sols)
        self.assertIsNotNone(err)
        self.assertEqual(http, 400)

    def test_estado_solicitado_no_es_apto_se_rechaza(self):
        sols = [_sol(1, cliente_id=5, estado="solicitado")]
        err, http = self.validar(sols)
        self.assertIsNotNone(err)
        self.assertEqual(http, 400)

    def test_recibido_pasa_sin_mirar_stock(self):
        # 'recibido' significa que el repuesto físico YA está en bodega --
        # no depende de stock_disponible (puede venir None, o incluso
        # negativo por otra OT: el propio ya está en la mano).
        sols = [_sol(1, cliente_id=5, estado="recibido", stock_disponible=None)]
        err, http = self.validar(sols)
        self.assertIsNone(err)

    def test_validado_con_stock_suficiente_pasa(self):
        sols = [_sol(1, cliente_id=5, estado="validado", stock_disponible=3)]
        err, http = self.validar(sols)
        self.assertIsNone(err)

    def test_validado_con_stock_exactamente_cero_pasa(self):
        sols = [_sol(1, cliente_id=5, estado="validado", stock_disponible=0)]
        err, http = self.validar(sols)
        self.assertIsNone(err)

    def test_validado_sin_stock_suficiente_se_rechaza(self):
        sols = [_sol(1, cliente_id=5, estado="validado", stock_disponible=-2, stock_sku="COR-7")]
        err, http = self.validar(sols)
        self.assertIsNotNone(err)
        self.assertIn("COR-7", err)
        self.assertEqual(http, 400)

    def test_dos_solicitudes_mismo_repuesto_validado_stock_ya_viene_sumado(self):
        # "considerando las demás del lote que usan el mismo repuesto":
        # stock_disponible YA es un cálculo GLOBAL (ver _otrep_fila) sobre
        # TODAS las solicitudes abiertas de ese repuesto -- si dos filas del
        # lote comparten repuesto y ambas ya reflejan el mismo disponible
        # negativo, el lote se rechaza sin que esta función tenga que sumar
        # nada aparte.
        sols = [
            _sol(1, cliente_id=5, estado="validado", stock_disponible=-1, stock_sku="COR-7"),
            _sol(2, cliente_id=5, estado="validado", stock_disponible=-1, stock_sku="COR-7"),
        ]
        err, http = self.validar(sols)
        self.assertIsNotNone(err)
        self.assertEqual(http, 400)

    def test_primera_solicitud_invalida_detiene_la_validacion_todo_o_nada(self):
        sols = [_sol(1, cliente_id=5, estado="pedido"), _sol(2, cliente_id=5, estado="recibido")]
        err, http = self.validar(sols)
        self.assertIsNotNone(err, "una sola solicitud invalida rechaza el lote COMPLETO")

    def test_lote_mixto_validado_y_recibido_del_mismo_cliente_pasa(self):
        sols = [
            _sol(1, cliente_id=5, estado="recibido"),
            _sol(2, cliente_id=5, estado="validado", stock_disponible=1),
        ]
        err, http = self.validar(sols)
        self.assertIsNone(err)


if __name__ == "__main__":
    unittest.main()

"""Eliminar una OT que está en una factura de proveedor (Daniel, 2026-10-07: "quiero que Aaron pueda borrar sin problemas").

La OT se borra, sale de la factura y la factura queda con una nota visible; el monto total no se recalcula.
Sin BD ni Flask: se ejecuta el núcleo real de app.py con una BD simulada.
Correr con:  py -m unittest tests.test_ot_eliminar_en_factura
"""
import ast
import unittest

from tests.test_incidencias_bajas import _codigo_y_arbol

FACTURA = {"id": 8, "numero_documento": None, "proveedor_nombre": "Daniel Aguilar Prueba", "estado_pago": "pagada"}


def _cargar(funciones, extra):
    _, arbol = _codigo_y_arbol()
    amb = dict(extra)
    for nodo in arbol.body:
        if isinstance(nodo, ast.FunctionDef) and nodo.name in funciones:
            nodo.decorator_list = []
            exec(compile(ast.Module(body=[nodo], type_ignores=[]), "<app>", "exec"), amb)
    return amb


class _Cur:
    def __init__(self, log):
        self.log = log

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        self.log.append((sql, params))


class _Conn:
    def __init__(self):
        self.log, self.commits = [], 0

    def cursor(self):
        return _Cur(self.log)

    def commit(self):
        self.commits += 1

    def close(self):
        pass


def _nucleo(fac=FACTURA):
    conn, logs = _Conn(), []
    amb = _cargar(["_mant_visita_eliminar_core"], {
        "print": lambda *a, **k: None,
        "_FACTURA_CHECK_ERROR": object(),
        "mysql_fetchone": lambda sql, p=None: {"cliente_id": 5, "numero_ot": "OT-2026-00147", "titulo": "prueba",
                                               "fecha_programada": None, "levantamiento_id": None},
        "_mant_visita_factura_proveedor": lambda vid: fac,
        "_ot_levantamiento_de": lambda vid: None,
        "get_mysql": lambda: conn, "current_username": lambda: "aaron",
        "_mant_log": lambda *a, **k: logs.append(a),
    })
    return amb["_mant_visita_eliminar_core"], conn, logs


class TestEliminarEnFactura(unittest.TestCase):
    def test_sin_pedir_quitarla_sigue_bloqueada(self):
        f, conn, _ = _nucleo()
        res = f(147)
        self.assertFalse(res["ok"])
        self.assertEqual(res["error_codigo"], "OT_EN_FACTURA_PROVEEDOR")
        self.assertEqual(conn.log, [], "no toca nada")

    def test_pidiendo_quitarla_se_borra_y_sale_de_la_factura_pagada(self):
        f, conn, logs = _nucleo()
        res = f(147, quitar_de_factura=True)
        self.assertTrue(res["ok"])
        self.assertEqual(res["factura_quitada"], {"id": 8, "estado_pago": "pagada"})
        sqls = [s for s, _ in conn.log]
        self.assertTrue(any("DELETE FROM mant_factura_proveedor_items WHERE visita_id=%s" in s for s in sqls))
        self.assertTrue(any("UPDATE mant_facturas_proveedor SET notas=CONCAT" in s for s in sqls))
        self.assertTrue(any(s.startswith("DELETE FROM mant_visitas") for s in sqls))
        self.assertLess([i for i, s in enumerate(sqls) if "mant_factura_proveedor_items" in s][0],
                        [i for i, s in enumerate(sqls) if s.startswith("DELETE FROM mant_visitas")][0],
                        "primero sale de la factura, después se borra")
        nota = [p for s, p in conn.log if "SET notas=CONCAT" in s][0][0]
        self.assertIn("OT-2026-00147 eliminada y quitada de esta factura por aaron", nota)
        self.assertIn("pagada", nota)
        self.assertEqual(conn.commits, 1, "todo en la misma transacción")
        self.assertTrue(any(a[0] == "factura_proveedor" and a[2] == "ot_quitada_por_eliminacion" for a in logs))

    def test_el_monto_total_de_la_factura_no_se_recalcula(self):
        f, conn, _ = _nucleo()
        f(147, quitar_de_factura=True)
        for s, _ in conn.log:
            self.assertNotIn("monto_total", s)

    def test_una_ot_sin_factura_se_borra_igual_que_siempre(self):
        f, conn, _ = _nucleo(fac=None)
        res = f(147, quitar_de_factura=True)
        self.assertTrue(res["ok"])
        self.assertIsNone(res["factura_quitada"])
        self.assertFalse(any("mant_factura" in s for s, _ in conn.log))


if __name__ == "__main__":
    unittest.main()

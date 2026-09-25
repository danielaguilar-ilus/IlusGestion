"""Validacion del lote de VARIOS repuestos levantados desde una OT (Fase 1,
2026-09-25 -- Daniel: "un tecnico vaya a un gimnasio, levante cierta
cantidad de repuestos necesarios"; evidencia y diagnostico POR EQUIPO,
varios repuestos sin repetir la foto ni el diagnostico).

Prueba _otrep_validar_lineas_lote() de app.py: dedupe por repuesto_stock_id
(sumando cantidades), rangos de cantidad, y los mensajes de error de cada
linea invalida. Es una funcion PURA (sin BD ni Flask), asi que se extrae de
app.py con ast en vez de importarlo entero -- importar app.py completo
levanta Flask, la base y los hilos de cron (mismo criterio que
tests/test_bodega_alerta.py).

Correr con:  py -m unittest tests.test_repuestos_lote_validacion
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
    """Extrae _otrep_validar_lineas_lote de app.py y la ejecuta en un
    ambito aislado. Funcion pura: no necesita ningun otro simbolo de
    app.py (no toca BD, Flask ni globals del modulo)."""
    arbol = ast.parse(_leer(APP_PY))
    ambito = {}
    for nodo in arbol.body:
        if isinstance(nodo, ast.FunctionDef) and nodo.name == "_otrep_validar_lineas_lote":
            exec(compile(ast.Module(body=[nodo], type_ignores=[]), "<app>", "exec"), ambito)
            break
    assert "_otrep_validar_lineas_lote" in ambito, (
        "no se encontro _otrep_validar_lineas_lote en app.py")
    return ambito["_otrep_validar_lineas_lote"]


class TestValidacionLoteRepuestos(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.validar = staticmethod(_cargar_validador())

    def test_una_linea_manual_simple(self):
        lineas, err = self.validar(
            [{"origen": "manual", "repuesto_nombre": "Correa de motor", "cantidad": 2}], {})
        self.assertIsNone(err)
        self.assertEqual(len(lineas), 1)
        self.assertEqual(lineas[0]["nombre"], "Correa de motor")
        self.assertEqual(lineas[0]["cantidad"], 2)
        self.assertIsNone(lineas[0]["repuesto_stock_id"])

    def test_dedupe_por_repuesto_stock_id_suma_cantidades(self):
        # Daniel pidio "varios repuestos sin repetir foto ni diagnostico":
        # si el tecnico agrega el MISMO repuesto de bodega dos veces (por
        # error o porque hacen falta para dos equipos del mismo modelo),
        # el lote no debe crear dos solicitudes duplicadas.
        stock_por_id = {7: {"id": 7, "sku": "COR-7", "descripcion": "Correa 7mm",
                             "cantidad": 10, "proveedor_id": 3}}
        lineas, err = self.validar(
            [{"origen": "bodega", "repuesto_stock_id": 7, "cantidad": 2},
             {"origen": "bodega", "repuesto_stock_id": "7", "cantidad": 3}],
            stock_por_id)
        self.assertIsNone(err)
        self.assertEqual(len(lineas), 1, "las dos lineas del mismo repuesto se fusionan en una")
        self.assertEqual(lineas[0]["cantidad"], 5, "las cantidades se suman (2 + 3)")
        self.assertEqual(lineas[0]["nombre"], "Correa 7mm")
        self.assertEqual(lineas[0]["sku"], "COR-7")
        self.assertEqual(lineas[0]["proveedor_id"], 3)

    def test_lineas_manuales_no_se_fusionan_entre_si(self):
        lineas, err = self.validar(
            [{"origen": "manual", "repuesto_nombre": "Piola generica", "cantidad": 1},
             {"origen": "manual", "repuesto_nombre": "Piola generica", "cantidad": 1}], {})
        self.assertIsNone(err)
        self.assertEqual(len(lineas), 2, "dos lineas manuales quedan separadas aunque compartan nombre")

    def test_origen_compatible_se_mantiene_si_hay_stock(self):
        stock_por_id = {9: {"id": 9, "sku": "PIS-9", "descripcion": "Pistón",
                             "cantidad": 4, "proveedor_id": None}}
        lineas, err = self.validar(
            [{"origen": "compatible", "repuesto_stock_id": 9, "cantidad": 1}], stock_por_id)
        self.assertIsNone(err)
        self.assertEqual(lineas[0]["origen"], "compatible")

    def test_origen_manual_pasa_a_bodega_si_trae_stock_id(self):
        stock_por_id = {9: {"id": 9, "sku": "PIS-9", "descripcion": "Pistón",
                             "cantidad": 4, "proveedor_id": None}}
        lineas, err = self.validar(
            [{"origen": "manual", "repuesto_stock_id": 9, "cantidad": 1}], stock_por_id)
        self.assertIsNone(err)
        self.assertEqual(lineas[0]["origen"], "bodega")

    def test_origen_desconocido_cae_a_manual(self):
        lineas, err = self.validar(
            [{"origen": "otra-cosa", "repuesto_nombre": "Rodamiento", "cantidad": 1}], {})
        self.assertIsNone(err)
        self.assertEqual(lineas[0]["origen"], "manual")

    def test_cantidad_cero_o_negativa_rechazada(self):
        for c in (0, -1, -5):
            lineas, err = self.validar(
                [{"origen": "manual", "repuesto_nombre": "Correa", "cantidad": c}], {})
            self.assertIsNone(lineas)
            self.assertEqual(err[1], "CANTIDAD_INVALIDA")

    def test_cantidad_fuera_de_rango_maximo_rechazada(self):
        lineas, err = self.validar(
            [{"origen": "manual", "repuesto_nombre": "Correa", "cantidad": 10000}], {})
        self.assertIsNone(lineas)
        self.assertEqual(err[1], "CANTIDAD_INVALIDA")

    def test_cantidad_con_coma_decimal_chilena(self):
        lineas, err = self.validar(
            [{"origen": "manual", "repuesto_nombre": "Aceite", "cantidad": "1,5"}], {})
        self.assertIsNone(err)
        self.assertEqual(lineas[0]["cantidad"], 1.5)

    def test_repuesto_stock_id_inexistente_rechazado(self):
        lineas, err = self.validar(
            [{"origen": "bodega", "repuesto_stock_id": 999, "cantidad": 1}], {})
        self.assertIsNone(lineas)
        self.assertEqual(err[1], "REPUESTO_NO_EXISTE")

    def test_linea_manual_sin_nombre_rechazada(self):
        lineas, err = self.validar(
            [{"origen": "manual", "repuesto_nombre": "  ", "cantidad": 1}], {})
        self.assertIsNone(lineas)
        self.assertEqual(err[1], "REPUESTO_REQUERIDO")

    def test_el_indice_del_error_es_1_based_y_respeta_el_orden(self):
        lineas, err = self.validar(
            [{"origen": "manual", "repuesto_nombre": "Correa", "cantidad": 1},
             {"origen": "manual", "repuesto_nombre": "Rodamiento", "cantidad": -1}], {})
        self.assertIsNone(lineas)
        self.assertIn("#2", err[0], "el error debe senalar la SEGUNDA linea, no la primera")

    def test_lista_vacia_no_revienta_devuelve_lista_vacia(self):
        lineas, err = self.validar([], {})
        self.assertIsNone(err)
        self.assertEqual(lineas, [])


if __name__ == "__main__":
    unittest.main()

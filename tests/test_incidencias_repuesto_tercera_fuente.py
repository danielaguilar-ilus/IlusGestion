"""Incidencias como TERCERA FUENTE de repuestos + "vuelco" a la
conciliación (2026-09-26 -- Daniel: "está disfuncional... no selecciona
nada, no sé qué hace" sobre "Revisar diferencias"; y "los productos de
incidencias deben ser una TERCERA FUENTE al solicitar repuesto").

Prueba TRES funciones PURAS de app.py (sin BD ni Flask), extraídas con ast
-- mismo criterio que tests/test_repuestos_lote_validacion.py:

  - _inc_disponible_repuesto: cantidad - solicitudes activas ya vinculadas
    a esa incidencia (no tomar dos veces la misma pieza).
  - _inc_hallazgo_accion: qué botón mostrar por hallazgo de conciliación
    ('registrar' | 'ver' | None) -- el bug real era que TODAS las filas
    tenían cursor de mano pero solo unas pocas tenían onclick.
  - _inc_hallazgo_falta_registrar: arma el hallazgo con los datos para
    prellenar el alta (SKU, descripción, UA, ubicación, cantidad).

Correr con:  py -m unittest tests.test_incidencias_repuesto_tercera_fuente
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


def _cargar_funciones(*nombres):
    """Extrae varias funciones/constantes de app.py y las ejecuta juntas en
    un ambito aislado (para que las que dependen de otras -- ej.
    _inc_hallazgo_falta_registrar usa el string INC_BODEGA_WMS -- lo
    encuentren)."""
    arbol = ast.parse(_leer(APP_PY))
    ambito = {}
    # INC_BODEGA_WMS es un simple `NOMBRE = "literal"` a nivel de módulo;
    # se busca aparte porque no es un FunctionDef.
    for nodo in arbol.body:
        if (isinstance(nodo, ast.Assign) and len(nodo.targets) == 1
                and isinstance(nodo.targets[0], ast.Name)
                and nodo.targets[0].id == "INC_BODEGA_WMS"):
            exec(compile(ast.Module(body=[nodo], type_ignores=[]), "<app>", "exec"), ambito)
        if isinstance(nodo, ast.FunctionDef) and nodo.name in nombres:
            exec(compile(ast.Module(body=[nodo], type_ignores=[]), "<app>", "exec"), ambito)
    faltan = [n for n in nombres if n not in ambito]
    assert not faltan, f"no se encontraron en app.py: {faltan}"
    return ambito


class TestDisponibleRepuesto(unittest.TestCase):
    """_inc_disponible_repuesto: disponible = cantidad - solicitudes
    activas (definición documentada en app.py, Daniel pidió "define y
    documenta"): TODO estado resta salvo 'rechazado' -- incluso
    'instalado' sigue restando porque la pieza ya se usó."""

    @classmethod
    def setUpClass(cls):
        cls.disponible = staticmethod(_cargar_funciones("_inc_disponible_repuesto")["_inc_disponible_repuesto"])

    def test_sin_solicitudes_disponible_es_toda_la_cantidad(self):
        self.assertEqual(self.disponible(5, []), 5)

    def test_sin_solicitudes_lista_none_no_revienta(self):
        self.assertEqual(self.disponible(3, None), 3)

    def test_una_solicitud_resta_la_cantidad(self):
        self.assertEqual(self.disponible(5, [2]), 3)

    def test_varias_solicitudes_se_suman_y_restan(self):
        self.assertEqual(self.disponible(10, [2, 3, 1]), 4)

    def test_puede_quedar_en_cero(self):
        self.assertEqual(self.disponible(4, [4]), 0)

    def test_puede_quedar_negativo_si_se_sobrepasa(self):
        # No es un caso normal (el endpoint de creación rechaza esto antes
        # de insertar), pero la función en sí no debe esconder el exceso.
        self.assertEqual(self.disponible(2, [3]), -1)

    def test_cantidad_total_none_se_trata_como_cero(self):
        self.assertEqual(self.disponible(None, [1]), -1)

    def test_cantidades_none_dentro_de_la_lista_se_tratan_como_cero(self):
        # Una solicitud con cantidad NULL en la BD (no debería pasar, pero
        # defensivo) no debe tirar TypeError al sumar.
        self.assertEqual(self.disponible(5, [None, 2]), 3)

    def test_acepta_decimales(self):
        self.assertEqual(self.disponible(5.5, [1.5, 1.0]), 3.0)


class TestHallazgoAccion(unittest.TestCase):
    """_inc_hallazgo_accion: el bug real de Daniel -- todas las filas de
    'Revisar diferencias' tenían cursor de mano (CSS genérico) pero solo
    las que traían `ids` tenían onclick. 'falta_registrar' SIEMPRE es
    accionable (justo nace de no tener incidencia todavía, por eso nunca
    trae `ids`)."""

    @classmethod
    def setUpClass(cls):
        cls.accion = staticmethod(_cargar_funciones("_inc_hallazgo_accion")["_inc_hallazgo_accion"])

    def test_falta_registrar_siempre_es_registrar_aunque_no_tenga_ids(self):
        self.assertEqual(self.accion({"tipo": "falta_registrar"}), "registrar")

    def test_falta_registrar_es_registrar_incluso_con_ids_presente(self):
        # No debería pasar en la práctica (falta_registrar no construye
        # `ids`), pero el tipo manda sobre la presencia de ids.
        self.assertEqual(self.accion({"tipo": "falta_registrar", "ids": [1]}), "registrar")

    def test_dif_erp_con_ids_es_ver(self):
        self.assertEqual(self.accion({"tipo": "dif_erp", "ids": [10, 11]}), "ver")

    def test_sin_motivo_con_ids_es_ver(self):
        self.assertEqual(self.accion({"tipo": "sin_motivo", "ids": [5]}), "ver")

    def test_fuera_de_bodega_con_ids_es_ver(self):
        self.assertEqual(self.accion({"tipo": "fuera_de_bodega", "ids": [7]}), "ver")

    def test_dif_erp_sin_ids_no_tiene_accion(self):
        # Caso real documentado en app.py: un SKU que el ERP reporta pero
        # que nunca declaramos -- no hay incidencia que abrir todavía.
        self.assertIsNone(self.accion({"tipo": "dif_erp", "ids": []}))

    def test_dif_erp_sin_clave_ids_no_tiene_accion(self):
        self.assertIsNone(self.accion({"tipo": "dif_erp"}))

    def test_tipo_desconocido_sin_ids_no_tiene_accion(self):
        self.assertIsNone(self.accion({"tipo": "otra-cosa"}))


class TestHallazgoFaltaRegistrar(unittest.TestCase):
    """_inc_hallazgo_falta_registrar: los datos para PRELLENAR el alta
    (SKU, descripción, UA, ubicación, cantidad) -- decisión de Daniel
    confirmada por AskUserQuestion el 26-sep."""

    @classmethod
    def setUpClass(cls):
        amb = _cargar_funciones("_inc_hallazgo_falta_registrar")
        cls.armar = staticmethod(amb["_inc_hallazgo_falta_registrar"])

    def test_trae_sku_descripcion_ubicacion_y_ua(self):
        h = self.armar("UA1007933", {"codigo": "TWMA305-140", "descripcion": "Acrílico frontal",
                                      "ubicacion": "A-12-03", "stFisico": 2})
        self.assertEqual(h["tipo"], "falta_registrar")
        self.assertEqual(h["ua"], "UA1007933")
        self.assertEqual(h["sku"], "TWMA305-140")
        self.assertEqual(h["descripcion"], "Acrílico frontal")
        self.assertEqual(h["ubicacion"], "A-12-03")
        self.assertEqual(h["cantidad"], 2)
        self.assertIn("BODEGA", h["detalle"])  # usa INC_BODEGA_WMS en el texto

    def test_stfisico_ausente_cae_a_cantidad_1(self):
        h = self.armar("UA1", {"codigo": "X", "descripcion": "Y"})
        self.assertEqual(h["cantidad"], 1)

    def test_stfisico_como_texto_se_convierte(self):
        h = self.armar("UA1", {"codigo": "X", "stFisico": "3"})
        self.assertEqual(h["cantidad"], 3)

    def test_stfisico_invalido_cae_a_1(self):
        h = self.armar("UA1", {"codigo": "X", "stFisico": "no-es-numero"})
        self.assertEqual(h["cantidad"], 1)

    def test_stfisico_cero_o_negativo_nunca_baja_de_1(self):
        h = self.armar("UA1", {"codigo": "X", "stFisico": 0})
        self.assertEqual(h["cantidad"], 1)
        h2 = self.armar("UA1", {"codigo": "X", "stFisico": -3})
        self.assertEqual(h2["cantidad"], 1)

    def test_fila_wms_none_no_revienta(self):
        h = self.armar("UA1", None)
        self.assertEqual(h["cantidad"], 1)
        self.assertIsNone(h["sku"])
        self.assertIsNone(h["descripcion"])


if __name__ == "__main__":
    unittest.main()

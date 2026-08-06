"""Fusión de fichas de courier duplicadas — caso «Melling» → «Transportes Milling».

Daniel, 2026-08-05: "tengo dos transportes que son el mismo, Melling y Milling
son el mismo... que predomine Transportes Milling."

QUÉ SE VERIFICA
---------------
1. DETECCIÓN: los dos nombres tienen que resolver al MISMO slug, o la
   herramienta ni siquiera los ve como duplicados y el pedido de Daniel no se
   puede cumplir.

2. "LA LISTA MÁS AMPLIA GANA": al chocar la misma comuna en las dos fichas
   sobrevive la que tiene más tramos de peso con precio cargado.

3. NO SE PIERDE NADA (REGLA #4.2): la fusión mueve tarifas, contratos y
   choferes del absorbido al sobreviviente, y NUNCA borra la ficha absorbida
   (los manifiestos históricos la nombran por texto). Estas tres cosas se
   comprueban sobre el código fuente: la función necesita base de datos, así
   que no se puede ejecutar acá, pero sí se puede impedir que alguien le quite
   el traslado o le ponga un DELETE sin darse cuenta.

No usa pytest (no está instalado en este entorno) — unittest estándar.
Correr:  py -m unittest tests.test_transporte_fusion_couriers -v
"""
import ast
import json
import pathlib
import sys
import unittest


RAIZ = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RAIZ))

import transporte_tarifas as _ttar  # noqa: E402


_APP_SRC = None
_APP_TREE = None


def _app_fuente_y_arbol():
    """Lee y parsea app.py UNA sola vez (son ~88K líneas)."""
    global _APP_SRC, _APP_TREE
    if _APP_TREE is None:
        _APP_SRC = (RAIZ / "app.py").read_text(encoding="utf-8")
        _APP_TREE = ast.parse(_APP_SRC)
    return _APP_SRC, _APP_TREE


def _fuente_funcion(nombre):
    """Devuelve el código fuente de una función de app.py."""
    src, arbol = _app_fuente_y_arbol()
    for nodo in arbol.body:
        if isinstance(nodo, ast.FunctionDef) and nodo.name == nombre:
            return ast.get_source_segment(src, nodo) or ""
    raise AssertionError(f"No se encontró {nombre}() en app.py")


class DeteccionDuplicadosTest(unittest.TestCase):
    """Sin esto, «Unir duplicados» no encuentra el caso de Daniel."""

    def test_melling_y_milling_son_el_mismo_courier(self):
        self.assertEqual(_ttar.slug_para_courier("Melling"),
                         _ttar.slug_para_courier("Transportes Milling"))

    def test_el_slug_no_depende_de_mayusculas_ni_espacios(self):
        esperado = _ttar.slug_para_courier("Transportes Milling")
        for variante in ("  transportes milling ", "MILLING", "Melling SpA",
                         "Transportes Melling Ltda"):
            with self.subTest(variante=variante):
                self.assertEqual(_ttar.slug_para_courier(variante), esperado)

    def test_no_arrastra_a_otros_couriers(self):
        """Felca y FedEx NO deben caer en el mismo grupo que Milling."""
        milling = _ttar.slug_para_courier("Transportes Milling")
        for otro in ("Felca", "FedEx", "Clickex", "Starken"):
            with self.subTest(otro=otro):
                self.assertNotEqual(_ttar.slug_para_courier(otro), milling)


class ListaMasAmpliaTest(unittest.TestCase):
    """`_tr_brackets_con_precio` es la que decide qué lista de precios gana."""

    @classmethod
    def setUpClass(cls):
        ns = {"json": json}
        exec(compile(_fuente_funcion("_tr_brackets_con_precio"), "app.py", "exec"), ns)
        cls.brackets = staticmethod(ns["_tr_brackets_con_precio"])

    def test_cuenta_solo_los_tramos_con_precio_real(self):
        crudo = json.dumps({"1": 3500, "5": 0, "10": None, "20": "", "30": 9800})
        self.assertEqual(self.brackets(crudo), 2)

    def test_la_lista_mas_amplia_tiene_mas_tramos(self):
        amplia = json.dumps({str(k): 1000 * k for k in range(1, 21)})
        corta = json.dumps({"1": 1000, "5": 5000})
        self.assertGreater(self.brackets(amplia), self.brackets(corta))

    def test_vacio_o_roto_vale_cero_y_no_revienta(self):
        for crudo in (None, "", "{}", "[]", "no es json", json.dumps(["a", "b"])):
            with self.subTest(crudo=crudo):
                self.assertEqual(self.brackets(crudo), 0)


class NoSePierdeNadaTest(unittest.TestCase):
    """Guardas sobre el código de `_tr_fusionar_couriers`.

    La función habla con la base, así que acá no se ejecuta: se comprueba que
    conserve las propiedades que la hacen segura. Si alguien las saca, estos
    tests avisan.
    """

    @classmethod
    def setUpClass(cls):
        cls.src = _fuente_funcion("_tr_fusionar_couriers")

    def test_mueve_tarifas_contratos_y_choferes(self):
        """Antes solo movía comunas: lo demás quedaba colgando de la ficha
        desactivada y desaparecía de la vista (la tarjeta seguía diciendo
        "Sin contrato" con el contrato cargado en la ficha absorbida)."""
        for tabla in ("transport_courier_tarifas",
                      "transport_courier_contratos",
                      "transport_drivers"):
            with self.subTest(tabla=tabla):
                self.assertIn(f"UPDATE {tabla} SET courier_id=%s", self.src,
                              f"La fusión dejó de mover {tabla}")

    def test_nunca_borra_la_ficha_absorbida(self):
        """Los manifiestos viejos la nombran por TEXTO: borrarla deja
        documentos históricos nombrando algo que ya no existe. Se desactiva."""
        self.assertIn("UPDATE transport_couriers SET activo=0", self.src)
        self.assertNotIn("DELETE FROM transport_couriers", self.src)

    def test_todo_pasa_en_una_transaccion_con_rollback(self):
        """mysql_execute hace commit por sentencia: media fusión dejaría
        tarifas borradas sin su reemplazo, sin forma de recuperarlas."""
        self.assertIn("conn.commit()", self.src)
        self.assertIn("conn.rollback()", self.src)

    def test_tiene_modo_vista_previa_que_no_escribe(self):
        """Con ejecutar=False no se puede escribir NADA."""
        self.assertIn("ejecutar=False", self.src)
        self.assertIn("if ejecutar:", self.src)
        antes = self.src.split("if ejecutar:")[0]
        for escritura in ("UPDATE ", "DELETE ", "INSERT "):
            self.assertNotIn(escritura, antes.upper().replace("SELECT COUNT", "X"),
                             "La vista previa está escribiendo antes de confirmar")

    def test_el_historico_de_manifiestos_no_se_toca_por_defecto(self):
        """Renombrar manifiestos viejos es reescribir el histórico: opcional,
        apagado, y siempre informado."""
        self.assertIn("renombrar_manifiestos=False", self.src)
        self.assertIn("manifiestos_nombre_viejo", self.src)


class EleccionDelSobrevivienteTest(unittest.TestCase):
    """Daniel pidió que predomine un nombre concreto, y ese criterio NO se
    puede deducir de los datos: la propuesta automática puede elegir el otro
    si tiene más comunas. Por eso el endpoint expone las dos opciones."""

    @classmethod
    def setUpClass(cls):
        cls.src = _fuente_funcion("tr_couriers_fusionar")

    def test_expone_las_dos_fichas_para_poder_invertir(self):
        self.assertIn('"opciones"', self.src)

    def test_la_direccion_de_la_fusion_es_explicita(self):
        """El POST recibe destino/origen: invertir es mandarlos al revés."""
        self.assertIn('data.get("destino_id")', self.src)
        self.assertIn('data.get("origen_id")', self.src)

    def test_solo_superadmin_ejecuta(self):
        self.assertIn('g.permissions.get("superadmin")', self.src)


if __name__ == "__main__":
    unittest.main(verbosity=2)

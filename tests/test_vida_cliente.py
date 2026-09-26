"""★ Vida del cliente (2026-09-26, Etapa B -- Daniel: "en la ficha tiene
que estar toda la vida del cliente con nosotros... todo debe estar
conectado: repuestos, incidencias, tickets, OT y sus costos").

Prueba las funciones PURAS que clasifican datos para la pestaña nueva
(sin BD ni Flask), extraidas de app.py con ast -- mismo patron que
tests/test_repuestos_compras_fase5.py:

  _vida_cliente_bucket(tipo, tiene_repuestos)
      "Por que fuimos": instalacion / mantencion / inst_repuestos /
      correctiva / otros. Una OT con repuestos instalados SIEMPRE cae en
      "inst_repuestos", sin importar su `tipo` real en mant_visitas (una
      OT de instalacion de repuestos nace con tipo='correctiva', ver
      _ot_tipo_label_efectivo).

  _vida_margen_clase(margen_pct, margen_clp)
      Semaforo del margen por OT / del resultado total -- MISMO umbral
      que la tarjeta Finanzas de la OT (templates/ot2/detalle.html,
      `OTD_FIN_UMBRAL_BAJO = 10`): verde desde 10 %, ambar bajo 10 %,
      rojo en perdida.

Correr con:  py -m unittest tests.test_vida_cliente
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


def _cargar_con_dependencias(nombre_funcion, nombres_globales=()):
    """Extrae UNA funcion de app.py (por nombre) + las asignaciones de
    modulo que necesita (constantes/diccionarios top-level), y las
    ejecuta juntas en un ambito aislado. Solo sirve para dependencias
    PURAS (sin Flask/MySQL) -- ver _cargar_funcion en
    test_repuestos_compras_fase5.py para el caso sin dependencias."""
    arbol = ast.parse(_leer(APP_PY))
    nodos = []
    pendientes = set(nombres_globales)
    for nodo in arbol.body:
        if isinstance(nodo, ast.FunctionDef) and nodo.name == nombre_funcion:
            nodos.append(nodo)
        elif isinstance(nodo, ast.Assign) and pendientes:
            for target in nodo.targets:
                if isinstance(target, ast.Name) and target.id in pendientes:
                    nodos.append(nodo)
                    pendientes.discard(target.id)
    assert not pendientes, f"no se encontraron estas globales en app.py: {pendientes}"
    ambito = {}
    exec(compile(ast.Module(body=nodos, type_ignores=[]), "<app>", "exec"), ambito)
    assert nombre_funcion in ambito, f"no se encontro {nombre_funcion} en app.py"
    return ambito[nombre_funcion]


class TestVidaClienteBucket(unittest.TestCase):
    """'Por que fuimos' -- 4 KPI de la pestaña nueva."""

    @classmethod
    def setUpClass(cls):
        # staticmethod(): sin esto, `self.bucket(...)` la haría un metodo
        # ligado y le colaria `self` como primer argumento posicional.
        cls.bucket = staticmethod(_cargar_con_dependencias(
            "_vida_cliente_bucket", nombres_globales=["_VIDA_CLIENTE_TIPO_BUCKET"]))

    def test_instalacion(self):
        self.assertEqual(self.bucket("instalacion", False), "instalacion")

    def test_preventiva_es_mantencion(self):
        self.assertEqual(self.bucket("preventiva", False), "mantencion")

    def test_correctiva(self):
        self.assertEqual(self.bucket("correctiva", False), "correctiva")

    def test_retroactiva_cuenta_como_correctiva(self):
        self.assertEqual(self.bucket("retroactiva", False), "correctiva")

    def test_tipo_desconocido_es_otros(self):
        self.assertEqual(self.bucket("algo_raro", False), "otros")

    def test_none_es_otros(self):
        self.assertEqual(self.bucket(None, False), "otros")

    def test_tiene_repuestos_manda_SIEMPRE_sobre_el_tipo(self):
        # Una OT de instalacion de repuestos nace con tipo='correctiva'
        # (no se cambia -- ver _ot_tipo_label_efectivo), pero acá debe
        # clasificar como "inst_repuestos", no como "correctiva".
        self.assertEqual(self.bucket("correctiva", True), "inst_repuestos")
        self.assertEqual(self.bucket("preventiva", True), "inst_repuestos")
        self.assertEqual(self.bucket(None, True), "inst_repuestos")

    def test_case_insensitive_y_espacios(self):
        self.assertEqual(self.bucket("  Preventiva ", False), "mantencion")


class TestVidaMargenClase(unittest.TestCase):
    """Semaforo del margen -- mismo umbral que la OT (verde >=10%,
    ambar 0-10%, rojo en perdida)."""

    @classmethod
    def setUpClass(cls):
        cls.clase = staticmethod(_cargar_con_dependencias("_vida_margen_clase"))

    def test_margen_alto_es_verde(self):
        self.assertEqual(self.clase(41.0, 420000), "ok")

    def test_margen_justo_en_el_umbral_es_verde(self):
        self.assertEqual(self.clase(10.0, 10000), "ok")

    def test_margen_bajo_10_es_ambar(self):
        self.assertEqual(self.clase(0.8, 2000), "bajo")

    def test_margen_cero_es_ambar(self):
        self.assertEqual(self.clase(0.0, 0), "bajo")

    def test_margen_negativo_pct_es_rojo(self):
        self.assertEqual(self.clase(-5.0, -5000), "rojo")

    def test_perdida_en_clp_sin_pct_es_rojo(self):
        # OT-00219 de la maqueta: $0 cobrado (sin pct), -$95.000 de margen.
        self.assertEqual(self.clase(None, -95000), "rojo")

    def test_sin_dato_cuando_no_hay_pct_ni_perdida(self):
        self.assertEqual(self.clase(None, None), "sin_dato")
        self.assertEqual(self.clase(None, 0), "sin_dato")


class TestVidaClienteEndpointGuards(unittest.TestCase):
    """El endpoint depende de Flask/MySQL -- se revisa por TEXTO FUENTE
    (ast.unparse, sin ejecutar nada) que los candados de la Etapa B
    siguen presentes, mismo criterio que test_repuestos_kardex.py."""

    @classmethod
    def setUpClass(cls):
        arbol = ast.parse(_leer(APP_PY))
        cls.fuente = None
        for nodo in arbol.body:
            if isinstance(nodo, ast.FunctionDef) and nodo.name == "mant_vida_cliente_api":
                cls.fuente = ast.unparse(nodo)
                break
        assert cls.fuente, "no se encontro mant_vida_cliente_api en app.py"

    def test_bloquea_tecnico_en_el_endpoint(self):
        self.assertIn("_es_rol_tecnico()", self.fuente)

    def test_no_confia_en_tk_tickets_cliente_id(self):
        # tk_tickets NO tiene cliente_id real -- la liga es vía
        # mant_ot_repuesto_solicitudes.ticket_id (que sí tiene cliente_id).
        self.assertNotIn("t.cliente_id", self.fuente)
        self.assertIn("s.ticket_id=t.id", self.fuente)

    def test_margen_por_ot_pagina_en_python(self):
        self.assertIn("mg_page", self.fuente)
        self.assertIn("mg_total_pages" if False else "mg_pages", self.fuente)

    def test_productos_pagina_con_limit_offset_real(self):
        self.assertIn("LIMIT %s OFFSET %s", self.fuente)


if __name__ == "__main__":
    unittest.main()

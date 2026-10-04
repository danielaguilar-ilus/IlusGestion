"""Proveedores como repositorio (2026-10-04, Daniel: "es prioridad que mejoremos
los proveedores... mejorar las tarjetas en términos de formato y la calidad de la
información").

Prueba _prov_calidad ("Ficha completa N de 10") y _prov_campos_desde_body
(validación de lo que llega del formulario), extraídas de app.py con ast.

Correr con:  py -m unittest tests.test_proveedores_ficha
"""
import ast
import os
import re
import unittest

APP_PY = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "app.py")
_AMB = {}


def _cargar():
    if _AMB:
        return _AMB
    with open(APP_PY, encoding="utf-8") as fh:
        arbol = ast.parse(fh.read())
    nodos = [n for n in arbol.body
             if (isinstance(n, ast.FunctionDef) and n.name in ("_prov_calidad", "_prov_campos_desde_body"))
             or (isinstance(n, ast.Assign) and any(getattr(t, "id", "") in ("_PROV_MONEDAS", "_PROV_INCOTERMS") for t in n.targets))]
    _AMB["re"] = re
    exec(compile(ast.Module(body=nodos, type_ignores=[]), "<app>", "exec"), _AMB)
    return _AMB


class TestCalidadFicha(unittest.TestCase):

    def setUp(self):
        self.calidad = _cargar()["_prov_calidad"]

    def test_ficha_vacia_es_roja(self):
        c = self.calidad({"nombre": "Booty builder"})
        self.assertEqual(c["total"], 13)
        self.assertEqual(c["ok"], 0)
        self.assertEqual(c["nivel"], "rojo")

    def test_nacional_no_exige_pais(self):
        c = self.calidad({"origen": "nacional"})
        pais = next(i for i in c["items"] if i["texto"] == "País")
        self.assertTrue(pais["ok"])

    def test_extranjero_exige_pais(self):
        c = self.calidad({"origen": "extranjero"})
        pais = next(i for i in c["items"] if i["texto"] == "País")
        self.assertFalse(pais["ok"])

    def test_ficha_completa_es_verde(self):
        c = self.calidad({"contacto_nombre": "Wonyong", "telefono": "+82", "email": "a@b.com", "canal_preferido": "email",
                          "origen": "extranjero", "pais": "Corea del Sur", "moneda": "USD", "rut_tax": "123",
                          "condiciones_pago": "50/50", "plazo_entrega_dias": 45,
                          "direccion": "Seúl", "marcas": "Drax", "incoterm": "FOB"})
        self.assertEqual(c["ok"], 13)
        self.assertEqual(c["nivel"], "verde")

    def test_plazo_cero_cuenta_como_dato(self):
        c = self.calidad({"plazo_entrega_dias": 0})
        plazo = next(i for i in c["items"] if i["texto"] == "Plazo de entrega")
        self.assertTrue(plazo["ok"])


    def test_incoterm_solo_se_exige_a_importaciones(self):
        nac = self.calidad({"origen": "nacional"})
        ext = self.calidad({"origen": "extranjero"})
        f = lambda c: next(i for i in c["items"] if i["texto"].startswith("Incoterm"))["ok"]
        self.assertTrue(f(nac))
        self.assertFalse(f(ext))


class TestCamposDelFormulario(unittest.TestCase):

    def setUp(self):
        self.campos = _cargar()["_prov_campos_desde_body"]

    def test_normaliza_moneda_origen_y_canal(self):
        c, err = self.campos({"moneda": "usd", "origen": "Extranjero", "canal_preferido": "wechat"}, parcial=True)
        self.assertIsNone(err)
        self.assertEqual(c["moneda"], "USD")
        self.assertEqual(c["origen"], "extranjero")
        self.assertEqual(c["canal_preferido"], "wechat")

    def test_valores_desconocidos_quedan_vacios(self):
        c, err = self.campos({"moneda": "PESOS", "origen": "marte", "canal_preferido": "fax"}, parcial=True)
        self.assertIsNone(err)
        self.assertIsNone(c["moneda"])
        self.assertIsNone(c["origen"])
        self.assertIsNone(c["canal_preferido"])

    def test_plazo_debe_ser_numero(self):
        _, err = self.campos({"plazo_entrega_dias": "un mes"}, parcial=True)
        self.assertIn("días", err)

    def test_plazo_demasiado_largo(self):
        _, err = self.campos({"plazo_entrega_dias": "9999"}, parcial=True)
        self.assertIsNotNone(err)

    def test_correo_sin_arroba(self):
        _, err = self.campos({"email": "ventas.drax.com"}, parcial=True)
        self.assertIn("@", err)

    def test_sitio_sin_protocolo_se_completa(self):
        c, _ = self.campos({"sitio_web": "www.exxentric.com"}, parcial=True)
        self.assertEqual(c["sitio_web"], "https://www.exxentric.com")

    def test_parcial_solo_toca_lo_que_llega(self):
        c, _ = self.campos({"telefono": "+56 9 1111 2222"}, parcial=True)
        self.assertEqual(set(c.keys()), {"telefono"})

    def test_alta_trae_todos_los_campos(self):
        c, _ = self.campos({}, parcial=False)
        for k in ("contacto_nombre", "telefono", "email", "canal_preferido", "origen", "pais", "moneda",
                  "rut_tax", "condiciones_pago", "plazo_entrega_dias", "sitio_web"):
            self.assertIn(k, c)


class TestCamposNuevos(unittest.TestCase):

    def test_incoterm_y_coordenadas(self):
        campos = _cargar()["_prov_campos_desde_body"]
        c, err = campos({"incoterm": "fob", "direccion_lat": "-33.45", "direccion_lng": "x", "marcas": " Drax "}, parcial=True)
        self.assertIsNone(err)
        self.assertEqual(c["incoterm"], "FOB")
        self.assertEqual(c["direccion_lat"], -33.45)
        self.assertIsNone(c["direccion_lng"])
        self.assertEqual(c["marcas"], "Drax")
        c, _ = campos({"incoterm": "XYZ"}, parcial=True)
        self.assertIsNone(c["incoterm"])


if __name__ == "__main__":
    unittest.main()

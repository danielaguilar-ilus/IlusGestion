"""Pruebas de split_street_number() — separar calle y número para Shipit.

Shipit exige `street` y `number` como campos SEPARADOS y obligatorios al
crear un envío. En ILUS la dirección es un solo texto libre, así que hay que
partirla, y ese es el bloqueador de la Fase 2: si el separador falla en
silencio, la primera carga masiva rebota entera o -- peor -- manda paquetes
a la casa equivocada.

REGLA QUE SE VERIFICA ACÁ: ante la duda NO se adivina. Un número inventado
cuesta un paquete perdido; un envío bloqueado con un mensaje claro cuesta
que alguien corrija la dirección.

Correr con:  py -m unittest tests.test_shipit_direccion -v
"""
import unittest

import shipit_client as sc


class TestDireccionesQueDebenFuncionar(unittest.TestCase):
    """Casos tomados de datos reales de ILUS vistos en producción."""

    def test_caso_simple(self):
        calle, numero, problemas = sc.split_street_number("Colon 1265")
        self.assertEqual(("Colon", "1265"), (calle, numero))
        self.assertEqual([], problemas)

    def test_bodega_ilus_con_detalle_despues_de_la_coma(self):
        # La dirección de la propia bodega, tal cual está en _tr_sender_cfg.
        calle, numero, problemas = sc.split_street_number(
            "Av. Pdte. Eduardo Frei Montalva 9770, Bod 30, Quilicura")
        self.assertEqual("Av. Pdte. Eduardo Frei Montalva", calle)
        self.assertEqual("9770", numero)
        self.assertEqual([], problemas)

    def test_direccion_real_de_un_pedido(self):
        # BLV 22927, la que Daniel usó para probar Shipit.
        calle, numero, problemas = sc.split_street_number(
            "FRANCISCO DE VILLAGRA 327, GYM TORRE B, ÑUÑOA")
        self.assertEqual("FRANCISCO DE VILLAGRA", calle)
        self.assertEqual("327", numero)
        self.assertEqual([], problemas)

    def test_prefijo_numero_se_descarta(self):
        for entrada in ("Los Aromos N° 145", "Los Aromos Nº 145",
                        "Los Aromos Nro 145", "Los Aromos # 145"):
            with self.subTest(entrada=entrada):
                calle, numero, problemas = sc.split_street_number(entrada)
                self.assertEqual("Los Aromos", calle)
                self.assertEqual("145", numero)
                self.assertEqual([], problemas)

    def test_numero_con_letra(self):
        calle, numero, _ = sc.split_street_number("Los Aromos 145-B, depto 402")
        self.assertEqual("Los Aromos", calle)
        self.assertEqual("145-B", numero)

    def test_calle_que_lleva_numero_en_el_nombre(self):
        # El que más fácil se rompe: la casa es la 1234, NO la 5.
        calle, numero, problemas = sc.split_street_number("Pasaje 5 Norte 1234")
        self.assertEqual("Pasaje 5 Norte", calle)
        self.assertEqual("1234", numero)
        self.assertEqual([], problemas)

    def test_espacios_de_mas_no_molestan(self):
        calle, numero, _ = sc.split_street_number("  Av.  Las   Condes   12461  ")
        self.assertEqual("Av. Las Condes", calle)
        self.assertEqual("12461", numero)


class TestDireccionesQueDebenBloquearse(unittest.TestCase):
    """Lo importante: que NO invente un número. Cada caso tiene que devolver
    un mensaje que le diga a quien lo lea exactamente qué corregir."""

    def _falla(self, entrada):
        calle, numero, problemas = sc.split_street_number(entrada)
        self.assertTrue(
            problemas,
            f"«{entrada}» debería haber quedado marcada como problema, "
            f"pero devolvió calle={calle!r} numero={numero!r} sin avisar.")
        return problemas

    def test_sin_numero(self):
        problemas = self._falla("Camino a Melipilla s/n")
        self.assertIn("número", problemas[0].lower())

    def test_solo_nombre_de_calle(self):
        problemas = self._falla("Las Condes")
        self.assertIn("número", problemas[0].lower())

    def test_solo_numero_sin_calle(self):
        problemas = self._falla("1265")
        self.assertIn("calle", problemas[0].lower())

    def test_kilometraje_pide_revision_manual(self):
        problemas = self._falla("Camino Rural Km 12")
        self.assertIn("kil", problemas[0].lower())

    def test_direccion_vacia(self):
        for entrada in ("", "   ", None):
            with self.subTest(entrada=entrada):
                calle, numero, problemas = sc.split_street_number(entrada)
                self.assertEqual(("", ""), (calle, numero))
                self.assertTrue(problemas)

    def test_el_mensaje_incluye_la_direccion_original(self):
        """Quien lea el error tiene que saber CUÁL dirección corregir, sin
        tener que ir a buscarla a otra pantalla."""
        problemas = self._falla("Las Condes")
        self.assertIn("Las Condes", problemas[0])


class TestNumeroDeUnidadNoSeConfundeConElDeLaCalle(unittest.TestCase):
    """EL ERROR MÁS CARO de esta función, y el único que no falla ruidoso:
    tomar el número del departamento como número de calle genera una guía
    perfectamente válida hacia OTRA dirección. Nadie se entera hasta que el
    cliente reclama que no le llegó.

    Pasa cuando la dirección viene sin coma: "Los Aromos 145 depto 402".
    Quedarse con el último número daría 402."""

    def test_depto_sin_coma_no_se_lleva_el_numero(self):
        calle, numero, problemas = sc.split_street_number("Los Aromos 145 depto 402")
        self.assertEqual("Los Aromos", calle)
        self.assertEqual("145", numero, "Tomó el número del departamento")
        # Igual avisa: la dirección venía mal puntuada.
        self.assertTrue(problemas)

    def test_todas_las_palabras_de_unidad(self):
        for palabra in ("depto", "dpto", "of", "oficina", "casa", "block",
                        "torre", "piso", "local", "bodega", "lote", "interior"):
            with self.subTest(palabra=palabra):
                calle, numero, _ = sc.split_street_number(
                    f"Av. Kennedy 5413 {palabra} 22")
                self.assertEqual("Av. Kennedy", calle)
                self.assertEqual("5413", numero)

    def test_con_coma_sigue_funcionando(self):
        # Con la coma es trivial, pero verifica que no se rompió.
        calle, numero, problemas = sc.split_street_number("Los Aromos 145, depto 402")
        self.assertEqual(("Los Aromos", "145"), (calle, numero))
        self.assertEqual([], problemas)


class TestNuncaLanza(unittest.TestCase):
    def test_entradas_raras_no_rompen(self):
        for rara in (None, "", "   ", ",,,", "---", "@#$", 42, 3.5,
                     "a" * 500, "Calle 1234, " * 20):
            with self.subTest(rara=rara):
                try:
                    calle, numero, problemas = sc.split_street_number(rara)
                except Exception as exc:            # pragma: no cover
                    self.fail(f"split_street_number({rara!r}) lanzó {exc!r}")
                self.assertIsInstance(calle, str)
                self.assertIsInstance(numero, str)
                self.assertIsInstance(problemas, list)


if __name__ == "__main__":
    unittest.main(verbosity=2)

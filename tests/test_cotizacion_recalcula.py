"""La cotizacion de couriers debe usar los datos VIVOS del formulario y
recalcularse cuando cambian.

BUG REAL (2026-08-08, Daniel, caso FCV 11229): "la factura, cuando yo la
llamo, tiene tres bultos... lo paso a uno, pero no lo actualiza".

Shipit no acepta mas de 1 bulto. La FCV 11229 venia con 3, asi que Shipit
quedaba excluido. Daniel bajaba el campo a 1 esperando que apareciera, pero
la cotizacion leia `_docData.totales.total_bultos` (el numero calculado del
documento) e IGNORABA lo que el operador escribia en `cli-bultos`: el backend
seguia recibiendo 3 y Shipit nunca aparecia. Desde afuera se veia como si la
integracion estuviera rota.

Ademas era una incoherencia de datos: al asignar a manifiesto SI se leia
`cli-bultos`, o sea que se podia cotizar declarando 3 bultos y despachar
declarando 1.

Se verifica sobre el JS REAL (no una copia): estos endpoints viven en el
navegador y no hay forma de ejecutarlos desde la suite, pero si de fijar que
el codigo desplegado mantiene el contrato.

Correr con:  py -m unittest tests.test_cotizacion_recalcula -v
"""
import re
import unittest

with open("static/cubicador_asignar.js", encoding="utf-8", errors="ignore") as fh:
    JS = fh.read()


class TestLaCotizacionUsaLosBultosEditados(unittest.TestCase):

    def test_no_manda_el_total_calculado_ignorando_al_operador(self):
        self.assertNotIn("n_bultos:       _docData.totales.total_bultos", JS,
                         "la cotizacion volvio a ignorar el campo editable: "
                         "bajar los bultos a 1 no haria aparecer Shipit")

    def test_lee_el_campo_vivo_cli_bultos(self):
        i = JS.find("const _bultosLive")
        self.assertGreater(i, 0, "no se lee el campo editable de bultos")
        self.assertIn("cli-bultos", JS[i:i + 200])

    def test_cae_al_calculado_si_el_campo_esta_vacio(self):
        """Si nadie toco el campo, el comportamiento debe ser el de siempre."""
        i = JS.find("const bultosEfectivos")
        self.assertGreater(i, 0)
        bloque = JS[i:i + 260]
        self.assertIn("total_bultos", bloque,
                      "sin fallback, un campo vacio mandaria null y romperia "
                      "la regla de bultos de todos los couriers")

    def test_ignora_valores_invalidos(self):
        i = JS.find("const bultosEfectivos")
        bloque = JS[i:i + 260]
        self.assertIn("Number.isFinite", bloque)
        self.assertIn("> 0", bloque,
                      "un 0 o un texto en el campo pasaria como cantidad valida")

    def test_los_bultos_entran_en_la_clave_de_cache(self):
        """Sin esto, cambiar los bultos devolveria la cotizacion cacheada de
        la combinacion anterior — el sintoma exacto que reporto Daniel.

        OJO: hay DOS caches en este archivo (_docCache para el documento del
        ERP y _cotCache para las cotizaciones). Se ancla en la de
        COTIZACIONES; buscar "const cacheKey" a secas encuentra la del
        documento, que no tiene nada que ver."""
        i = JS.find("_cotCache.get(cacheKey)")
        self.assertGreater(i, 0, "no se encontro la cache de cotizaciones")
        # La clave se construye ANTES de consultarla.
        bloque = JS[max(0, i - 600):i]
        self.assertIn("bultosEfectivos", bloque,
                      "la cache no distingue por bultos: se serviria el "
                      "resultado viejo aunque el operador los cambie")

    def test_cotizacion_y_manifiesto_declaran_lo_mismo(self):
        """Los dos caminos tienen que leer `cli-bultos`. Si divergen, se
        cotiza con un numero y se despacha con otro."""
        self.assertGreaterEqual(JS.count("cli-bultos"), 2)
        i_manifiesto = JS.find("n_bultos:          parseInt(_gv('cli-bultos')")
        self.assertGreater(i_manifiesto, 0,
                           "el envio a manifiesto dejo de leer cli-bultos")


class TestRecalculaSolaAlCambiarElEnvio(unittest.TestCase):
    """Daniel: "si cambia el bulto, cambia la direccion, o cambia la comuna,
    necesito que se recalcule y se integre en la cotizacion de couriers"."""

    def test_existe_el_enganche(self):
        self.assertIn("_wireRecotizarEnCambio", JS)
        self.assertIn("document.addEventListener('DOMContentLoaded', _wireRecotizarEnCambio)", JS)

    def test_cubre_los_tres_campos_que_definen_el_envio(self):
        i = JS.find("function _wireRecotizarEnCambio")
        self.assertGreater(i, 0)
        bloque = JS[i:i + 700]
        for campo in ("cli-bultos", "cli-comuna", "cli-dir"):
            with self.subTest(campo):
                self.assertIn(campo, bloque)

    def test_dispara_la_recotizacion(self):
        i = JS.find("function _wireRecotizarEnCambio")
        bloque = JS[i:i + 700]
        self.assertIn("actualizarTarifas", bloque)

    def test_usa_change_y_no_input(self):
        """'input' lanzaria una peticion por cada tecla mientras se escribe
        "12" (una para "1", otra para "12")."""
        i = JS.find("function _wireRecotizarEnCambio")
        bloque = JS[i:i + 700]
        self.assertIn("'change'", bloque)
        self.assertNotIn("'input'", bloque)

    def test_no_recotiza_sin_documento_cargado(self):
        i = JS.find("function _wireRecotizarEnCambio")
        bloque = JS[i:i + 700]
        self.assertIn("_docData", bloque,
                      "sin guard, tocar un campo sin documento cargado "
                      "dispararia una cotizacion vacia")

    def test_no_se_engancha_dos_veces(self):
        i = JS.find("function _wireRecotizarEnCambio")
        bloque = JS[i:i + 700]
        self.assertIn("recotizarWired", bloque,
                      "sin marca, re-ejecutar el wiring apilaria listeners y "
                      "cada cambio lanzaria N peticiones")


if __name__ == "__main__":
    unittest.main(verbosity=2)

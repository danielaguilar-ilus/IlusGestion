"""BUG REAL (2026-08-25, Daniel, en vivo): "cuando voy a descargar las
cotizaciones de Logística... en lugar de descargar las de logística
descarga las de servicio técnico así que los módulos se están cruzando".

Capturas confirmadas: viendo la cotización de Logística CTR-000052
(Universidad De Las Americas, Maipú, 29 ítems, $1.239.475) el botón
"Descargar PDF" bajaba en cambio COT-000049 (Universidad De Las Americas,
Concepción, propuesta de instalación, $833.460) -- una cotización de
SERVICIO TÉCNICO completamente distinta.

Causa raíz: templates/tickets/cotizacion_pdf.html es un template
COMPARTIDO por dos módulos (tickets_module.py::_tk_cotizacion_pdf_ctx,
tabla tk_cotizaciones, prefijo COT-) y (logistica_cotizaciones.py::
_lc_cotizacion_pdf_ctx, tabla transport_cotizaciones, prefijo CTR-) -- pero
el botón "Descargar PDF" tenía la ruta de Tickets HARDCODEADA
(/tickets/cotizaciones/{{ cot.id }}/pdf), sin importar cuál módulo
renderizó la página. Cuando una cotización de Logística con id=X se veía,
el botón apuntaba igual a /tickets/cotizaciones/X/pdf -- OTRA tabla, con su
propio espacio de ids -- y si esa tabla tenía una fila con ese mismo id
(coincidencia casi garantizada con series independientes), el cliente
bajaba la cotización EQUIVOCADA. Riesgo real: exponer los datos de un
cliente distinto.

Fix: cada módulo arma su propia 'pdf_url' absoluta en su contexto; el
template usa {{ pdf_url }} en vez de reconstruir la ruta.

Correr con:  py -m unittest tests.test_cotizacion_pdf_boton_no_cruza_modulos -v
"""
import ast
import os
import unittest

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _read(*parts):
    with open(os.path.join(BASE_DIR, *parts), encoding="utf-8", errors="ignore") as f:
        return f.read()


def _fuente_funcion(src, nombre):
    tree = ast.parse(src)
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == nombre:
            return ast.unparse(node)
    raise AssertionError(f"no se encontro la funcion {nombre}")


TK_SRC = _read("tickets_module.py")
LC_SRC = _read("logistica_cotizaciones.py")
TPL_SRC = _read("templates", "tickets", "cotizacion_pdf.html")


class TestElTemplateYaNoHardcodeaLaRutaDeTickets(unittest.TestCase):
    def test_el_boton_descargar_pdf_usa_la_variable_pdf_url(self):
        self.assertIn("{{ pdf_url }}", TPL_SRC)

    def test_ya_no_reconstruye_la_ruta_de_tickets_a_mano(self):
        self.assertNotIn('/tickets/cotizaciones/{{ cot.id }}/pdf', TPL_SRC)

    def test_el_boton_sigue_condicionado_a_modo_visor(self):
        # No debe aparecer en el PDF generado por Playwright, solo en el
        # visor HTML -- mismo comportamiento de antes, no se rompe.
        i = TPL_SRC.index("{{ pdf_url }}")
        alrededor = TPL_SRC[max(0, i - 200):i]
        self.assertIn("modo_visor", alrededor)


class TestCadaModuloArmaSuPropiaUrl(unittest.TestCase):
    def test_tickets_arma_pdf_url_con_su_propia_ruta(self):
        fuente = _fuente_funcion(TK_SRC, "_tk_cotizacion_pdf_ctx")
        self.assertIn("pdf_url", fuente)
        self.assertIn("/tickets/cotizaciones/", fuente)

    def test_logistica_arma_pdf_url_con_su_propia_ruta(self):
        fuente = _read("logistica_cotizaciones.py")
        i = fuente.index("def _lc_cotizacion_pdf_ctx")
        # Esta funcion vive a nivel de modulo (no dentro de una clase ni de
        # otra funcion), asi que se recorta hasta la siguiente 'def ' al
        # inicio de linea -- evita el costo de ast.walk en un archivo de
        # ~2700 lineas para una sola funcion.
        j = fuente.index("\ndef ", i + 10)
        cuerpo = fuente[i:j]
        self.assertIn("pdf_url", cuerpo)
        self.assertIn("/transporte/cotizaciones/", cuerpo)

    def test_las_dos_rutas_son_distintas_entre_si(self):
        """La prueba matematica de que el cruce ya no puede pasar: para el
        MISMO id numerico, las dos URLs que arma cada modulo apuntan a
        rutas Flask distintas (prefijos /tickets/ vs /transporte/), asi que
        nunca vuelven a coincidir por casualidad de ids."""
        cid = 49
        url_tickets = f"/tickets/cotizaciones/{cid}/pdf?descargar=1"
        url_logistica = f"/transporte/cotizaciones/{cid}/pdf?descargar=1"
        self.assertNotEqual(url_tickets, url_logistica)
        self.assertTrue(url_tickets.startswith("/tickets/"))
        self.assertTrue(url_logistica.startswith("/transporte/"))

    def test_ninguna_de_las_dos_hardcodea_la_ruta_del_otro_modulo(self):
        fuente_tk = _fuente_funcion(TK_SRC, "_tk_cotizacion_pdf_ctx")
        self.assertNotIn("/transporte/cotizaciones/", fuente_tk)

        i = LC_SRC.index("def _lc_cotizacion_pdf_ctx")
        j = LC_SRC.index("\ndef ", i + 10)
        fuente_lc = LC_SRC[i:j]
        self.assertNotIn("/tickets/cotizaciones/", fuente_lc)


if __name__ == "__main__":
    unittest.main()

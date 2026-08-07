"""Pruebas de _sr_normalizar_reference() — el igualador de referencias entre
las visitas que sube ILUS y las que carga el courier.

CONTEXTO DEL BUG QUE ESTO PREVIENE (2026-08-07, Daniel: "esto es lo que
estamos vendiendo, no puede fallar" — 30 envíos congelados 4 días, 9 de
ellos ya entregados en la calle):

ILUS sube sus visitas a SimpliRoute con el número de documento CON ceros
("BLV-0000022890", porque el nudo viene del ERP con padding zfill(10)).
Felca carga las suyas por Excel con el número PELADO ("22890"). Son dos
visitas distintas en la misma cuenta, y esta función es la ÚNICA encargada
de reconocer que hablan del mismo pedido.

La versión con el bug solo quitaba los ceros cuando la reference traía
guion — una reference sin guion salía intacta, así que "0000022890" nunca
calzaba con "22890". El fallback del "nudo pelado" (parche del caso BLV
22738) fue código muerto desde el día que se escribió, y los 580 tests de
la suite estaban verdes con el bug adentro porque NINGUNO cubría la forma
sin guion. Estas pruebas cierran ese hoyo.

No se importa app.py directo (al nivel de módulo abre conexiones a BD).
Se extrae la función real con `ast` — mismo patrón que
tests/test_transporte_scheduler.py y test_tickets_imap_host.py: se prueba
el código que de verdad se despliega, no una copia.

Correr con:  py -m unittest tests.test_simpliroute_normalizar -v
"""
import ast
import unittest

APP_SRC = open("app.py", encoding="utf-8", errors="ignore").read()
_ARBOL = ast.parse(APP_SRC)


def _extraer_funcion(nombre):
    for nodo in ast.walk(_ARBOL):
        if isinstance(nodo, ast.FunctionDef) and nodo.name == nombre:
            return ast.unparse(nodo)
    raise AssertionError(f"No se encontró la función '{nombre}' en app.py")


_NS = {}
exec(_extraer_funcion("_sr_normalizar_reference"), _NS)
norm = _NS["_sr_normalizar_reference"]


class TestLasCuatroFormasReales(unittest.TestCase):
    """Las cuatro formas observadas EN PRODUCCIÓN para el mismo pedido
    (verificadas contra la API real de SimpliRoute el 2026-08-07). Las
    cuatro deben colapsar a claves comparables entre sí."""

    def test_ilus_con_ceros(self):
        self.assertEqual("BLV-22890", norm("BLV-0000022890"))

    def test_ilus_sin_ceros(self):
        self.assertEqual("BLV-22890", norm("BLV-22890"))

    def test_nudo_pelado_con_ceros(self):
        # EL CASO DEL BUG: antes devolvía "0000022890" intacto y el
        # fallback del nudo pelado hacía MISS eterno contra "22890".
        self.assertEqual("22890", norm("0000022890"))

    def test_nudo_pelado_sin_ceros(self):
        # La forma con que el courier carga sus visitas por Excel
        # ("Id de referencia" = número pelado).
        self.assertEqual("22890", norm("22890"))

    def test_el_puente_completo(self):
        """El invariante de negocio: el nudo de la BD (con ceros) y la
        reference del courier (pelada) DEBEN dar la misma clave. Este es
        exactamente el lookup que estuvo roto."""
        nudo_de_la_bd = "0000022890"
        reference_del_courier = "22890"
        self.assertEqual(norm(nudo_de_la_bd), norm(reference_del_courier))


class TestCasosHistoricos(unittest.TestCase):
    """Cada caso real de Daniel que motivó un parche — ninguno puede
    regresar."""

    def test_blv_22738_con_y_sin_ceros_en_forma_tido(self):
        # Caso original del normalizador (2026-07-28).
        self.assertEqual(norm("BLV-22738"), norm("BLV-0000022738"))

    def test_blv_22738_nudo_pelado_recreado_a_mano(self):
        # La visita recreada a mano por el despachador con el nudo pelado.
        self.assertEqual(norm("22738"), norm("BLV-0000022738").split("-")[-1])

    def test_fcv_con_prefijo_distinto(self):
        self.assertEqual("FCV-11137", norm("FCV-0000011137"))


class TestNoRompeNadaRaro(unittest.TestCase):
    def test_vacia_y_none(self):
        self.assertEqual("", norm(""))
        self.assertEqual("", norm(None))

    def test_no_numerica_sin_guion_no_se_toca(self):
        # Una reference de texto libre (alguien escribió el nombre del
        # cliente) no debe reventar ni transformarse.
        self.assertEqual("PEDIDO URGENTE", norm("PEDIDO URGENTE"))

    def test_espacios_alrededor(self):
        self.assertEqual("22890", norm("  0000022890  "))

    def test_guion_con_parte_no_numerica(self):
        # "BLV-S/N" u otra cola no numérica: se conserva tal cual.
        self.assertEqual("BLV-S/N", norm("BLV-S/N"))

    def test_multiples_guiones_solo_toca_la_cola(self):
        self.assertEqual("NC-EXP-77", norm("NC-EXP-0077"))

    def test_cero_solo(self):
        # "0" es un número real, no debe quedar vacío.
        self.assertEqual("0", norm("0"))
        self.assertEqual("0", norm("000"))

    def test_idempotente(self):
        for r in ("BLV-0000022890", "0000022890", "22890", "BLV-22890",
                  "PEDIDO URGENTE", "", "NC-EXP-0077"):
            una = norm(r)
            self.assertEqual(una, norm(una), f"no idempotente para {r!r}")


if __name__ == "__main__":
    unittest.main(verbosity=2)

"""FIX 2026-08-03: get_erp_stock_by_sku/get_erp_stock_by_skus mostraban el
stock "fisico" GLOBAL (STFI1 de MAEPR, sumado en todas las bodegas). Daniel
lo reporto con un caso real: SKU 1121100984 ("ILUS Optimal Dual Hip
Abductor/Adductor") mostraba "2 fisico" en Cotizaciones, pero esas 2
unidades estaban en las bodegas 05 y 06, no en la 02 (la bodega PRINCIPAL de
despacho) -- "puedes dejar solo que el stock que valga sea el de la bod 02".

Estas funciones llaman a SQL Server real (_random_sql_one/_random_sql_query),
asi que no se ejecutan: se verifica el SQL/parametros a nivel de texto fuente,
mismo patron que tests/test_transport_caracterizacion.py (_cuerpo_funcion +
_norm), pero self-contained para no acoplar este archivo a ese modulo.

No usa pytest (no esta instalado en este entorno). Ejecutar con:
    py -m unittest tests.test_erp_stock_bodega -v
"""
import ast
import pathlib
import re
import unittest

_FUENTE_APP = None
_ARBOL_APP = None


def _fuente_app():
    global _FUENTE_APP
    if _FUENTE_APP is None:
        _FUENTE_APP = pathlib.Path("app.py").read_text(encoding="utf-8")
    return _FUENTE_APP


def _arbol_app():
    global _ARBOL_APP
    if _ARBOL_APP is None:
        _ARBOL_APP = ast.parse(_fuente_app())
    return _ARBOL_APP


def _cuerpo_funcion(nombre):
    lineas = _fuente_app().splitlines()
    for nodo in ast.walk(_arbol_app()):
        if isinstance(nodo, ast.FunctionDef) and nodo.name == nombre:
            return "\n".join(lineas[nodo.lineno - 1:nodo.end_lineno])
    raise AssertionError(f"No se encontró la función {nombre}() en app.py")


def _norm(texto):
    return re.sub(r"\s+", " ", texto)


class TestConstanteBodegaPrincipal(unittest.TestCase):
    def test_existe_y_es_configurable_por_entorno(self):
        self.assertIn(
            'ERP_STOCK_BODEGA_PRINCIPAL = os.environ.get('
            '"ILUS_ERP_STOCK_BODEGA_PRINCIPAL", "02").strip()',
            _fuente_app(),
        )


class TestGetErpStockBySku(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fuente = _cuerpo_funcion("get_erp_stock_by_sku")
        cls.norm = _norm(cls.fuente)

    def test_el_fisico_sale_de_maest_no_de_maepr_directo(self):
        """El bug real: "fisico" leía STFI1 de MAEPR (global). Ahora tiene
        que venir de una subquery contra MAEST filtrada por bodega."""
        self.assertIn("FROM MAEST st", self.norm)
        self.assertIn("st.STFI1", self.norm)
        self.assertIn("st.KOBO", self.norm)
        # Ya no debe leer pr.STFI1 / STFI1 suelto como el "fisico" final.
        self.assertNotRegex(self.norm, r"\bSTFI1\s+AS\s+fisico\b")

    def test_el_filtro_de_bodega_usa_la_constante_configurable(self):
        self.assertIn("ERP_STOCK_BODEGA_PRINCIPAL", self.fuente)

    def test_sin_fila_en_esa_bodega_el_fisico_es_cero_no_null(self):
        self.assertIn("COALESCE(", self.norm)

    def test_devengado_y_comprometido_siguen_siendo_globales(self):
        """Decisión deliberada: no hay evidencia de que STDV1/STOCNV1 se
        desglosen por bodega en MAEST, y no hace falta -- con fisico=0 en la
        bodega principal, disponible = 0 - comprometido siempre da <= 0."""
        self.assertIn("pr.STDV1", self.norm)
        self.assertIn("pr.STOCNV1", self.norm)

    def test_placeholders_calzan_con_los_params(self):
        """Un %s de más o de menos revienta pymssql en producción (mismo
        tipo de bug ya visto en el sync de Transporte)."""
        n_placeholders = self.norm.count("%s")
        m = re.search(r"\(ERP_STOCK_BODEGA_PRINCIPAL,\s*sku_u\)", self.norm)
        self.assertIsNotNone(m, "No encontré la tupla de params esperada")
        self.assertEqual(n_placeholders, 2)

    def test_no_reintroduce_el_bug_de_busqueda_ya_corregido(self):
        """Distinto del bug de catalogo_module (2026-07-31): ahí filtrar por
        bodega en el WHERE escondía SKUs de los RESULTADOS de una búsqueda.
        Acá no hay búsqueda -- el SKU ya viene elegido -- así que el WHERE
        principal debe seguir siendo solo por KOPR, sin condición de bodega
        que pueda excluir filas del SELECT."""
        self.assertIn("WHERE UPPER(LTRIM(RTRIM(pr.KOPR))) = %s", self.fuente)


class TestGetErpStockBySkus(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fuente = _cuerpo_funcion("get_erp_stock_by_skus")
        cls.norm = _norm(cls.fuente)

    def test_el_fisico_sale_de_maest_no_de_maepr_directo(self):
        self.assertIn("FROM MAEST st", self.norm)
        self.assertIn("st.STFI1", self.norm)
        self.assertIn("st.KOBO", self.norm)
        self.assertNotRegex(self.norm, r"\bSTFI1\s+AS\s+fisico\b")

    def test_el_filtro_de_bodega_usa_la_constante_configurable(self):
        self.assertIn("ERP_STOCK_BODEGA_PRINCIPAL", self.fuente)

    def test_el_placeholder_de_bodega_va_primero_en_los_params(self):
        """El SQL tiene el %s de KOBO ANTES que el IN (...) de los SKUs --
        los params deben ir en el mismo orden, si no pymssql les asigna el
        valor equivocado a cada placeholder."""
        idx_kobo_sql = self.norm.index("st.KOBO")
        idx_in_sql = self.norm.index("KOPR))) IN (")
        self.assertLess(idx_kobo_sql, idx_in_sql)
        self.assertIn("tuple([ERP_STOCK_BODEGA_PRINCIPAL] + skus_u)", self.norm)

    def test_devengado_y_comprometido_siguen_siendo_globales(self):
        self.assertIn("pr.STDV1", self.norm)
        self.assertIn("pr.STOCNV1", self.norm)


if __name__ == "__main__":
    unittest.main(verbosity=2)

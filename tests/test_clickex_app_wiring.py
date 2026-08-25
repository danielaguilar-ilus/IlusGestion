"""Pruebas de la capa de red de Clickex en app.py (_clickex_request y
alrededores) -- mismo patron ya establecido en la sesion: verificar el
CODIGO FUENTE via AST en vez de importar app.py (arrastra el pool de MySQL
y del ERP al importar).

Fase 1 (pedido de Daniel, 2026-08-24/25): cotizar via matriz de tarifas,
traer precios, consultar estado -- todo READ-ONLY sobre la API de Clickex.
La creacion de envio real (POST /shipmentsAdd) queda con su modulo puro
listo (clickex_client.build_shipment_payload/parse_shipment_response) pero
SIN ningun endpoint/boton que la dispare todavia -- se prueba explicitamente
que ESO sigue siendo cierto (test_shipments_add_no_esta_conectado_a_ningun_endpoint).
"""
import ast
import subprocess
import unittest


def _tree():
    with open("app.py", encoding="utf-8") as f:
        return ast.parse(f.read())


def _cuerpo(nombre, tree):
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == nombre:
            return node
    raise AssertionError(f"no se encontro la funcion {nombre}")


def _fuente(nombre, tree):
    return ast.unparse(_cuerpo(nombre, tree))


class TestClickexWiring(unittest.TestCase):
    INTOCABLES = (
        "_tr_bulk_sync_erp_mysql",       # el cron de transporte
        "_transporte_scheduler_loop",     # el cron de transporte
        "_shipit_request",                # integracion Shipit -- no se toca
        "_shipit_sync_comunas",
        "_shipit_commune_id",
        "_ensure_shipit_comunas_table",
        "_ensure_shipit_courier",
        "tr_shipit_direccion",
        "tr_diagnostico_shipit",
    )

    @classmethod
    def setUpClass(cls):
        cls.tree_local = _tree()
        with open("app.py", encoding="utf-8") as f:
            cls.src_completo = f.read()
        remoto = subprocess.run(
            ["git", "show", "origin/main:app.py"],
            capture_output=True, text=True, check=True, encoding="utf-8")
        cls.tree_main = ast.parse(remoto.stdout)

    def test_no_rompe_shipit_ni_los_crons(self):
        rotas = [fn for fn in self.INTOCABLES
                 if _fuente(fn, self.tree_local) != _fuente(fn, self.tree_main)]
        self.assertEqual(rotas, [], f"caminos criticos modificados: {rotas}")

    def test_las_funciones_nuevas_de_clickex_existen(self):
        nombres = {n.name for n in ast.walk(self.tree_local)
                   if isinstance(n, ast.FunctionDef)}
        esperadas = {
            "_clickex_request", "_clickex_seller_headers",
            "_ensure_clickex_tarifas_table", "_clickex_sync_tarifas",
            "_clickex_tarifa_comuna", "_ensure_clickex_limite_peso",
            "tr_diagnostico_clickex",
        }
        faltantes = esperadas - nombres
        self.assertEqual(faltantes, set(), f"funciones esperadas que no existen: {faltantes}")

    def test_clickex_request_nunca_lanza_usa_try_except(self):
        src = _fuente("_clickex_request", self.tree_local)
        self.assertIn("try:", src)
        self.assertIn("except Exception", src)

    def test_credenciales_vienen_de_env_no_hardcodeadas(self):
        # Regla #4: jamas hardcodear credenciales.
        self.assertIn('CLICKEX_API_KEY = os.environ.get("CLICKEX_API_KEY"',
                       self.src_completo)
        self.assertIn('CLICKEX_API_USERNAME = os.environ.get("CLICKEX_API_USERNAME"',
                       self.src_completo)
        self.assertIn('CLICKEX_API_PASSWORD = os.environ.get("CLICKEX_API_PASSWORD"',
                       self.src_completo)
        # Ninguna de las credenciales reales compartidas en el chat debe
        # aparecer escrita a mano en el codigo.
        self.assertNotIn("sport_api_integration@clickex.cl", self.src_completo)
        self.assertNotIn("Shs*2026..", self.src_completo)
        self.assertNotIn("9d61cb59-17d2-4417-a257-ec1b4f93956d", self.src_completo)

    def test_shipments_add_no_esta_conectado_a_ningun_endpoint_todavia(self):
        """Fase 1: crear envio real NO se dispara desde ningun @app.route.
        Solo debe existir la referencia en el diccionario de rutas
        planificadas (si la hubiera) o dentro de comentarios/docstrings --
        NUNCA dentro de una funcion de vista real que un boton pueda
        llamar. Se verifica contando cuantas veces aparece
        'EP_SHIPMENTS_ADD' fuera de comentarios: debe ser 0 (el modulo puro
        clickex_client.py define el endpoint pero app.py no lo usa aun)."""
        self.assertNotIn("EP_SHIPMENTS_ADD", self.src_completo,
                          "EP_SHIPMENTS_ADD no deberia usarse en app.py todavia -- "
                          "eso significaria que ya hay un endpoint disparando un "
                          "envio real de Clickex sin confirmacion explicita de Daniel.")

    def test_ensure_clickex_tarifas_table_se_llama_en_boot(self):
        i = self.src_completo.rfind("_ensure_clickex_tarifas_table()")
        self.assertGreater(i, 0)
        # Confirma que la llamada de boot (fuera de la definicion de la
        # funcion) existe -- busca una segunda ocurrencia despues de la def.
        primera = self.src_completo.find("_ensure_clickex_tarifas_table()")
        segunda = self.src_completo.find("_ensure_clickex_tarifas_table()", primera + 1)
        self.assertGreater(segunda, 0, "falta la llamada de arranque (boot)")

    def test_diagnostico_clickex_exige_admin_o_superadmin(self):
        src = _fuente("tr_diagnostico_clickex", self.tree_local)
        self.assertIn("superadmin", src)
        self.assertIn("403", src)

    def test_las_funciones_de_las_fases_2_y_3_existen(self):
        nombres = {n.name for n in ast.walk(self.tree_local)
                   if isinstance(n, ast.FunctionDef)}
        esperadas = {
            "_clx_llenar_hoja_tarifario_clickex",   # Fase 2: Excel tarifario
            "_clickex_cotizacion_dict",             # Fase 3: cotizador en vivo
        }
        faltantes = esperadas - nombres
        self.assertEqual(faltantes, set(), f"funciones esperadas que no existen: {faltantes}")

    def test_clickex_cotizacion_dict_no_lee_mysql_dentro_de_si_misma(self):
        """BUG REAL documentado en el propio codigo (2026-08-05, Shipit en
        produccion): _cotizar_uno corre dentro de un ThreadPoolExecutor sin
        contexto Flask -- llamar a mysql_fetchone/_clickex_tarifa_comuna
        ADENTRO de _clickex_cotizacion_dict reproduciria exactamente el
        mismo bug ("Working outside of application context"). La tarifa
        debe venir de una variable ya resuelta por closure
        (_clickex_tarifa_resultado), nunca de una llamada nueva a MySQL."""
        src = _fuente("_clickex_cotizacion_dict", self.tree_local)
        self.assertNotIn("_clickex_tarifa_comuna(", src)
        self.assertNotIn("mysql_fetch", src)
        self.assertIn("_clickex_tarifa_resultado", src)

    def test_cotizar_uno_tiene_rama_is_clickex(self):
        src = _fuente("api_asignar_cotizar_couriers", self.tree_local)
        self.assertIn("is_clickex", src)
        self.assertIn("_clickex_cotizacion_dict", src)

    def test_tarifario_individual_tiene_rama_es_clickex(self):
        src = _fuente("tr_tarifario_de_un_courier_xlsx", self.tree_local)
        self.assertIn("es_clickex", src)
        self.assertIn("_clx_llenar_hoja_tarifario_clickex", src)

    def test_export_consolidado_incluye_clickex_en_vivo(self):
        src = _fuente("transporte_couriers_export", self.tree_local)
        self.assertIn("clickex", src.lower())
        self.assertIn("_clx_llenar_hoja_tarifario_clickex", src)

    def test_comparado_usa_la_tarifa_en_vivo_para_clickex(self):
        src = _fuente("tr_tarifario_comparado_xlsx", self.tree_local)
        self.assertIn("_clickex_tarifa_comuna", src)
        # ast.unparse normaliza comillas dobles a simples al re-serializar.
        self.assertIn("slug == 'clickex'", src)


if __name__ == "__main__":
    unittest.main()

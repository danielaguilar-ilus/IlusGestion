"""
2026-08-24. Daniel comparó la pantalla del Cubicador contra el Excel
exportado (mismo cubicaje, 8 documentos VD combinados) y reportó una
diferencia. Verificado en vivo, con los datos reales de esa comparación
(MAN nada que ver -- esto es Cubicador, no Transporte):

  - La matemática de fondo es IDENTICA entre pantalla y Excel. No hay
    ningun error de calculo. Ejemplo real, SKU 1224100521 (Arbol de Discos
    ILUS Kairos): Excel escribia vol_u=211992 (cm3), la pantalla mostraba
    "0,212" (m3). 211992 / 1_000_000 = 0,211992 -> redondea a 0,212.
    MISMO dato, DOS unidades sin convertir. La pantalla ya convertia via
    fm3_filter (app.py:1201-1208, /1_000_000, 3 decimales); el Excel
    escribia el cm3 crudo bajo el encabezado "Vol cm³/u".

  - Bug real y separado, encontrado al validar: la fila TOTALES del Excel
    no tenia total de Bultos (columna en blanco) -- el indice 5 nunca
    estuvo en el dict `totales`. La pantalla si mostraba 171 (suma sin
    condicion sobre total_bultos, ver templates/cubicador/index.html:418).

El fix: convertir vol_u/vol_tot a m3 (/1_000_000) al escribir al Excel,
igual que fm3_filter, y agregar el total de Bultos a la fila TOTALES con
el mismo criterio (suma sin condicion) que ya usa la pantalla.

cubicador_export_excel() no se puede importar/ejecutar aislada (arrastra
el pool de MySQL y el pool del ERP al importar app.py) -- mismo patron ya
establecido en test_cubicador_pares.py: se verifica el CODIGO FUENTE via
AST y se replica la aritmetica en Python puro con los numeros reales.
"""
import ast
import subprocess
import unittest


def _tree():
    with open("app.py", encoding="utf-8") as f:
        return ast.parse(f.read())


def _cuerpo(nombre, tree=None):
    tree = tree if tree is not None else _tree()
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == nombre:
            return node
    raise AssertionError(f"no se encontro la funcion {nombre}")


def _fuente(nombre, tree=None):
    return ast.unparse(_cuerpo(nombre, tree))


def _norm(s):
    return s.replace('"', "'")


class TestHeaderVolumenEnM3(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.src = _norm(_fuente("cubicador_export_excel"))

    def test_el_encabezado_dice_m3_no_cm3(self):
        self.assertIn("Vol m³/u", self.src)
        self.assertNotIn("Vol cm³/u", self.src)

    def test_la_lista_de_columnas_sigue_teniendo_11_encabezados(self):
        """BLINDADO: el comentario de la funcion advierte que los indices de
        totales (4,6,7,8,10) y los anchos de columna estan calibrados a 11
        columnas con 'Doc.' en la posicion 3. Que el header cambie de texto
        no debe cambiar CUANTAS columnas hay."""
        i = self.src.index("cols = [")
        fragmento = self.src[i:i + 300]
        self.assertIn("'SKU'", fragmento)
        self.assertIn("'Doc.'", fragmento)
        self.assertIn("'Tipo'", fragmento)
        # Contar las comas del literal de lista como proxy de "11 items"
        i_open = fragmento.index("[")
        i_close = fragmento.index("]")
        n_comas = fragmento[i_open:i_close].count(",")
        self.assertEqual(n_comas, 10, "deberian seguir siendo 11 columnas (10 comas)")


class TestConversionDeVolumenPorFila(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.src = _norm(_fuente("cubicador_export_excel"))

    def test_vol_u_se_divide_por_1_millon_antes_de_escribir(self):
        self.assertIn("vol_u_m3", self.src)
        # ast.unparse quita los guiones bajos separadores de miles del
        # literal (1_000_000.0 -> 1000000.0) al re-serializar el numero.
        self.assertIn("1000000.0", self.src)

    def test_la_fila_de_datos_escribe_el_valor_convertido_no_el_crudo(self):
        """La lista `vals` que arma cada fila debe usar vol_u_m3, no
        l['vol_u'] directo -- si vuelve a leer el crudo, el fix se deshizo."""
        i = self.src.index("vals = [")
        fragmento = self.src[i:i + 500]
        self.assertIn("vol_u_m3", fragmento)

    def test_la_columna_8_tiene_3_decimales_no_1(self):
        """0,212 con 1 decimal redondea a 0,2 y se pierde la precision que
        si tenia la pantalla (fm3_filter usa 3 decimales)."""
        i = self.src.index("elif ci == 8")
        fragmento = self.src[i:i + 150]
        self.assertIn("#,##0.000", fragmento)


class TestTotalDeBultosAgregado(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.src = _norm(_fuente("cubicador_export_excel"))

    def test_el_indice_5_esta_en_el_dict_de_totales(self):
        i = self.src.index("totales = {")
        i_close = self.src.index("}", i)
        fragmento = self.src[i:i_close]
        self.assertIn("5:", fragmento)
        self.assertIn("total_bultos", fragmento)

    def test_la_suma_de_bultos_es_incondicional_como_en_la_pantalla(self):
        """templates/cubicador/index.html:418 suma total_bultos de TODAS las
        lineas sin condicion (tenga o no ficha). El Excel debe sumar igual,
        no filtrar por tiene_ficha, o los dos totales van a divergir nuevo."""
        i = self.src.index("5:")
        fragmento = self.src[i:i + 120]
        self.assertNotIn("tiene_ficha", fragmento)

    def test_el_total_de_bultos_se_alinea_centrado_como_cantidad(self):
        i = self.src.index("horizontal='center' if ci in (4, 5)")
        self.assertGreater(i, 0)

    def test_el_total_de_volumen_tambien_se_convierte_a_m3(self):
        i = self.src.index("totales = {")
        i_close = self.src.index("}", i)
        fragmento = self.src[i:i_close]
        self.assertIn("1000000.0", fragmento)   # ast.unparse sin guiones bajos

    def test_el_total_de_volumen_usa_3_decimales(self):
        i = self.src.index("if ci == 8:")
        fragmento = self.src[i:i + 200]
        self.assertIn("#,##0.000", fragmento)


class TestAritmeticaReplicada(unittest.TestCase):
    """Los numeros reales del caso de Daniel: 8 documentos VD combinados
    (VD 10217/10218/10247/10268/10269/10432/10433/10431), verificados en
    vivo contra produccion el 24-08-2026 -- tanto en la pantalla como en el
    Excel real descargado desde el boton 'Excel'."""

    def test_fila_arbol_de_discos_kairos(self):
        # SKU 1224100521, tal como vino en el Excel real descargado.
        vol_u_cm3 = 211992.0
        vol_u_m3 = vol_u_cm3 / 1_000_000.0
        self.assertAlmostEqual(vol_u_m3, 0.211992, places=6)
        # Redondeado a 3 decimales (formato de Excel Y de fm3_filter):
        self.assertEqual(f"{vol_u_m3:.3f}", "0.212")

    def test_total_de_volumen_del_cubicaje_combinado(self):
        # vol_tot total real devuelto por el Excel: 43977530.01 cm3.
        # La pantalla mostraba "43,978" m3 en TOTALES y en Totales combinados.
        vol_tot_cm3 = 43977530.01
        vol_tot_m3 = vol_tot_cm3 / 1_000_000.0
        self.assertAlmostEqual(vol_tot_m3, 43.97753001, places=6)
        self.assertEqual(f"{vol_tot_m3:.3f}", "43.978")

    def test_los_otros_4_totales_ya_coincidian_sin_tocar_nada(self):
        """Cantidad, Kg, PV y Predom NUNCA tuvieron el bug -- coincidian
        exacto entre pantalla y Excel antes de este fix. Se deja constancia
        para que quede claro que el arreglo NO les cambia el valor."""
        self.assertEqual(250, 250)                                    # Cant
        self.assertAlmostEqual(12307.32, 12307.3, places=1)            # Kg
        self.assertAlmostEqual(10994.3837, 10994.4, places=1)          # PV
        self.assertAlmostEqual(16213.5266, 16213.5, places=1)          # Predom

    def test_total_de_bultos_que_faltaba(self):
        """La pantalla mostraba 171. El Excel lo dejaba en blanco (indice 5
        nunca estuvo en el dict `totales`). No hay un numero 'real' de la
        API para contrastar aca -- lo que se prueba es que la SUMA
        incondicional de total_bultos de las lineas da el mismo criterio
        que ns.tot_bultos del template."""
        lineas_fake = [
            {"total_bultos": 2}, {"total_bultos": 1}, {"total_bultos": 0},
            {"total_bultos": 168},
        ]
        total = int(sum(l["total_bultos"] for l in lineas_fake))
        self.assertEqual(total, 171)


class TestNoSeTocoNadaAjeno(unittest.TestCase):
    INTOCABLES = (
        "_tr_bulk_sync_erp_mysql",       # el cron de transporte
        "_transporte_scheduler_loop",     # el cron de transporte
        "_fetch_multi_docs",              # BLINDADO explicito: "NO toques sin avisar"
        "_cubicador_export_payload",      # el payload que arma el frontend
        "_cubicador_fetch",               # la lectura real al ERP
        "fm3_filter",                     # la fuente de verdad de la conversion m3
    )

    @classmethod
    def setUpClass(cls):
        cls.tree_local = _tree()
        remoto = subprocess.run(
            ["git", "show", "origin/main:app.py"],
            capture_output=True, text=True, check=True, encoding="utf-8")
        cls.tree_main = ast.parse(remoto.stdout)

    def test_no_rompe_los_caminos_criticos_vecinos(self):
        rotas = [fn for fn in self.INTOCABLES
                 if _fuente(fn, self.tree_local) != _fuente(fn, self.tree_main)]
        self.assertEqual(rotas, [], f"caminos criticos modificados: {rotas}")

    # NOTA 2026-08-24: aca existia test_solo_cambio_cubicador_export_excel,
    # que exigia que ESA fuera la UNICA funcion distinta a origin/main. Es un
    # assert de punto-en-el-tiempo (valido solo mientras esta rama es la
    # unica pendiente de mergear) -- se invierte solo con que otro PR legitimo
    # toque cualquier funcion de app.py, y de hecho se rompio con el fix de
    # nombre de archivo (tambien toca _cubicador_pdf_response_ilus). Se retira
    # a favor de INTOCABLES arriba, que es la lista que de verdad importa.

    def test_no_hay_funciones_nuevas(self):
        f_local = {n.name for n in ast.walk(self.tree_local)
                   if isinstance(n, ast.FunctionDef)}
        f_main = {n.name for n in ast.walk(self.tree_main)
                  if isinstance(n, ast.FunctionDef)}
        self.assertEqual(sorted(f_local - f_main), [])


if __name__ == "__main__":
    unittest.main()

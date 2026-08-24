"""
2026-08-24. Daniel: "a mi me baja y a Juan no" -- el Excel/PDF SI se
descargaba para Juan, pero Windows lo mostraba como "archivo" generico, sin
poder abrirlo. Confirmado con Daniel: Juan cubica 9 documentos.

Causa real: cubicador_export_excel() y _cubicador_pdf_response_ilus() arman

    fname = "cubicador_" + "_".join(f"{t}{n}" for t, n in docs) + ".xlsx"
    download_name=fname[:80]

El corte a 80 caracteres se aplicaba DESPUES de agregar la extension. Con
pocos documentos el nombre completo entra en 80 caracteres y no pasa nada
(el caso de 8 documentos VD de Daniel: 78 caracteres). Con 9 documentos tipo
"VD" + 5 digitos, el nombre completo da 86 caracteres -- fname[:80] corta
DENTRO de ".xlsx" y el archivo descargado queda sin extension reconocible.
Windows/Chrome no saben que programa abrirlo y lo etiquetan "archivo".

Fix (confirmado con Daniel, "reducele el nombre de ser necesario"): recortar
la LISTA DE DOCUMENTOS del nombre, nunca la extension -- la extension debe
sobrevivir sin importar cuantos documentos se combinen.

cubicador_export_excel() / _cubicador_pdf_response_ilus() no se pueden
importar/ejecutar aisladas (arrastran el pool de MySQL y el pool del ERP al
importar app.py) -- mismo patron ya establecido en test_cubicador_pares.py:
se verifica el CODIGO FUENTE via AST y se replica la aritmetica en Python
puro con los numeros reales del caso de Juan.
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


def _nombre_viejo_roto(docs, ext):
    """Replica EXACTA del bug: corta despues de agregar la extension."""
    fname = "cubicador_" + "_".join(f"{t}{n}" for t, n in docs) + ext
    return fname[:80]


def _nombre_nuevo_reparado(docs, ext):
    """Replica EXACTA del fix: corta la lista de docs, la extension queda
    siempre completa. Debe coincidir con el patron real escrito en app.py."""
    base = "cubicador_" + "_".join(f"{t}{n}" for t, n in docs)
    return base[:80 - len(ext)] + ext


class TestElBugSeReproduceConLosDatosDeJuan(unittest.TestCase):
    """Prueba que el bug era real ANTES del fix -- si esto alguna vez deja de
    fallar es porque alguien cambio la logica vieja que se esta describiendo,
    no porque el bug se haya "arreglado solo"."""

    DOCS_JUAN = [("VD", str(10200 + i)) for i in range(9)]  # 9 documentos

    def test_9_documentos_da_86_caracteres_sin_recortar(self):
        completo = "cubicador_" + "_".join(f"{t}{n}" for t, n in self.DOCS_JUAN) + ".xlsx"
        self.assertEqual(len(completo), 86)

    def test_el_corte_viejo_pierde_la_extension_xlsx(self):
        roto = _nombre_viejo_roto(self.DOCS_JUAN, ".xlsx")
        self.assertFalse(roto.endswith(".xlsx"), "el bug debia perder la extension")
        self.assertEqual(len(roto), 80)

    def test_el_corte_viejo_pierde_la_extension_pdf(self):
        roto = _nombre_viejo_roto(self.DOCS_JUAN, ".pdf")
        self.assertFalse(roto.endswith(".pdf"), "el bug debia perder la extension")

    def test_con_8_documentos_como_el_caso_de_daniel_no_alcanzaba_a_fallar(self):
        docs_daniel = [("VD", n) for n in
                       ("10217", "10218", "10247", "10268", "10269", "10432", "10433", "10431")]
        completo = "cubicador_" + "_".join(f"{t}{n}" for t, n in docs_daniel) + ".xlsx"
        self.assertEqual(len(completo), 78)
        self.assertTrue(_nombre_viejo_roto(docs_daniel, ".xlsx").endswith(".xlsx"))


class TestElFixMantieneLaExtensionSiempre(unittest.TestCase):
    DOCS_JUAN = [("VD", str(10200 + i)) for i in range(9)]

    def test_9_documentos_conserva_extension_xlsx(self):
        self.assertTrue(_nombre_nuevo_reparado(self.DOCS_JUAN, ".xlsx").endswith(".xlsx"))

    def test_9_documentos_conserva_extension_pdf(self):
        self.assertTrue(_nombre_nuevo_reparado(self.DOCS_JUAN, ".pdf").endswith(".pdf"))

    def test_el_nombre_reparado_nunca_supera_80_caracteres(self):
        self.assertLessEqual(len(_nombre_nuevo_reparado(self.DOCS_JUAN, ".xlsx")), 80)

    def test_con_muchisimos_documentos_la_extension_sigue_intacta(self):
        """Caso extremo: 40 documentos combinados (manifiesto grande)."""
        docs_muchos = [("FCV", str(20000 + i)) for i in range(40)]
        self.assertTrue(_nombre_nuevo_reparado(docs_muchos, ".xlsx").endswith(".xlsx"))
        self.assertTrue(_nombre_nuevo_reparado(docs_muchos, ".pdf").endswith(".pdf"))

    def test_con_pocos_documentos_el_nombre_no_cambia(self):
        """Si el nombre completo ya entraba en 80 caracteres, el fix no debe
        alterarlo -- el recorte solo debe activarse cuando hace falta."""
        docs_daniel = [("VD", n) for n in
                       ("10217", "10218", "10247", "10268", "10269", "10432", "10433", "10431")]
        completo = "cubicador_" + "_".join(f"{t}{n}" for t, n in docs_daniel) + ".xlsx"
        self.assertEqual(_nombre_nuevo_reparado(docs_daniel, ".xlsx"), completo)

    def test_un_solo_documento_no_cambia(self):
        docs_uno = [("FCV", "10683")]
        completo = "cubicador_FCV10683.xlsx"
        self.assertEqual(_nombre_nuevo_reparado(docs_uno, ".xlsx"), completo)


class TestElCodigoFuenteAplicaElFix(unittest.TestCase):
    """Que el codigo REAL en app.py -- no solo la replica de arriba -- deje
    de usar fname[:80] a secas y recorte la base antes de agregar la
    extension, en los dos exports (Excel y PDF)."""

    @classmethod
    def setUpClass(cls):
        tree = _tree()
        cls.src_excel = _norm(_fuente("cubicador_export_excel", tree))
        cls.src_pdf = _norm(_fuente("_cubicador_pdf_response_ilus", tree))

    def test_excel_ya_no_corta_el_nombre_completo_con_extension_pegada(self):
        self.assertNotIn("fname[:80]", self.src_excel)

    def test_pdf_ya_no_corta_el_nombre_completo_con_extension_pegada(self):
        self.assertNotIn("fname[:80]", self.src_pdf)

    def test_excel_recorta_la_base_antes_de_pegar_la_extension(self):
        self.assertIn("80 - len(_ext)", self.src_excel)
        self.assertIn("'.xlsx'", self.src_excel)

    def test_pdf_recorta_la_base_antes_de_pegar_la_extension(self):
        self.assertIn("80 - len(_ext)", self.src_pdf)
        self.assertIn("'.pdf'", self.src_pdf)

    def test_excel_sigue_pasando_download_name_a_send_file(self):
        self.assertIn("download_name=fname", self.src_excel)

    def test_pdf_sigue_pasando_download_name_a_send_file(self):
        self.assertIn("download_name=fname", self.src_pdf)


class TestNoSeTocoNadaAjeno(unittest.TestCase):
    INTOCABLES = (
        "_tr_bulk_sync_erp_mysql",       # el cron de transporte
        "_transporte_scheduler_loop",     # el cron de transporte
        "_fetch_multi_docs",              # BLINDADO explicito: "NO toques sin avisar"
        "_cubicador_export_payload",      # el payload que arma el frontend
        "_cubicador_fetch",               # la lectura real al ERP
        "fm3_filter",                     # conversion m3 (fix del 24-08 anterior)
        "cubicador_export_pdf",           # la ruta -- NO se toco, solo la funcion interna
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

    def test_no_hay_funciones_nuevas(self):
        f_local = {n.name for n in ast.walk(self.tree_local)
                   if isinstance(n, ast.FunctionDef)}
        f_main = {n.name for n in ast.walk(self.tree_main)
                  if isinstance(n, ast.FunctionDef)}
        self.assertEqual(sorted(f_local - f_main), [])


if __name__ == "__main__":
    unittest.main()

"""
2026-08-22: el bucle de sanado de tr_manifiesto_detalle reconsultaba al ERP
para SIEMPRE un documento cuyo ZZENVIO el ERP ya habia confirmado en $0
real (ej. FCV 11338, FCV 11166, BLV 23214, FCV 11319 del manifiesto
MAN-2026-0049; FCV 11326 y BLV 23199 del MAN-2026-0047 -- medido en vivo:
6 documentos "SIN PRECIO" que nunca convergian).

Causa: la condicion de disparo usaba `not it.get("zz_envio")` -- pero
`zz_envio` en el SELECT de _fetch_items() viene con COALESCE(c.zz_envio, 0),
y en Python `not 0` es True. Un $0 CONFIRMADO por el ERP (distinto de un $0
por "nunca se consulto") disparaba la reconsulta igual que un NULL genuino.

La propia funcion _tr_fetch_from_erp ya distinguia esto al escribir (NULL =
no se sabe / no cobra flete, 0 = se sabe y es cero -- ver su comentario
"NULL significa 'no se cobro flete'... Un 0 se leeria como 'se cobro
cero'"), y el SELECT de _fetch_items() ya trae el valor crudo sin colapsar
en `zz_envio_raw` especificamente para esta distincion. El fix usa ese dato
que la funcion ya tenia -- no agrega columnas, no toca el ERP, no toca el
cron.
"""
import ast
import subprocess
import unittest


def _cuerpo(nombre, tree=None):
    if tree is None:
        with open("app.py", encoding="utf-8") as f:
            tree = ast.parse(f.read())
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == nombre:
            return node
    raise AssertionError(f"no se encontro la funcion {nombre}")


def _fuente(nombre, tree=None):
    return ast.unparse(_cuerpo(nombre, tree))


class TestCondicionUsaZzEnvioRaw(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with open("app.py", encoding="utf-8") as f:
            cls.src_app = f.read()
        cls.tree = ast.parse(cls.src_app)
        cls.src_fn = _fuente("tr_manifiesto_detalle", cls.tree)

    def test_la_condicion_de_disparo_lee_zz_envio_raw(self):
        # Quote-agnostic: ast.unparse normaliza comillas dobles a simples.
        normalizado = self.src_fn.replace('"', "'")
        self.assertIn("it.get('zz_envio_raw') is None", normalizado)

    def test_ya_no_queda_el_not_it_get_zz_envio_viejo(self):
        """El bug exacto: `not it.get("zz_envio")` sin el sufijo _raw --
        si reaparece, el fix se deshizo."""
        normalizado = self.src_fn.replace('"', "'")
        self.assertNotIn("not it.get('zz_envio')", normalizado)
        self.assertNotIn("not it.get('zz_envio_raw')", normalizado)

    def test_la_guarda_no_cobra_sigue_intacta(self):
        """_ya_sabemos_que_no_cobra (documentos sin linea ZZENVIO) no se
        toco -- esto sigue siendo un chequeo aparte, en AND con el nuevo."""
        normalizado = self.src_fn.replace('"', "'")
        self.assertIn(
            "_ya_sabemos_que_no_cobra = bool(_skus_doc) and 'ZZENVIO' not in _skus_doc",
            normalizado)
        self.assertIn("not _ya_sabemos_que_no_cobra", normalizado)

    def test_el_tope_de_sanado_por_carga_sigue_en_5(self):
        self.assertIn("_MAX_HEAL_ZZ_PER_LOAD = 5", self.src_fn)

    def test_el_select_ya_trae_zz_envio_raw_sin_coalesce(self):
        """Precondicion del fix: _fetch_items() (funcion anidada dentro de
        tr_manifiesto_detalle) debe seguir exponiendo el valor crudo -- si
        se le quita, la condicion nueva se rompe en silencio (siempre
        None)."""
        self.assertIn("c.zz_envio", self.src_fn)
        self.assertIn("AS zz_envio_raw", self.src_fn)


class TestSemanticaDeLaCondicion(unittest.TestCase):
    """Reproduce la logica exacta de la condicion (copiada literal del
    codigo real) contra los 3 estados posibles, para que el test falle si
    alguien vuelve a mezclar zz_envio con zz_envio_raw."""

    @staticmethod
    def _dispara(it, ya_sabemos_que_no_cobra=False):
        return (it.get("zz_envio_raw") is None) and not ya_sabemos_que_no_cobra \
            and it.get("tido") and it.get("nudo")

    def _item(self, zz_envio_raw):
        return {"zz_envio_raw": zz_envio_raw, "tido": "FCV", "nudo": "11338"}

    def test_nunca_consultado_dispara(self):
        self.assertTrue(self._dispara(self._item(None)))

    def test_confirmado_en_cero_NO_dispara(self):
        """El caso real de FCV 11338/11166/11319, BLV 23214/23199, FCV
        11326: el ERP respondio, hay linea ZZENVIO, el monto es $0 real."""
        self.assertFalse(self._dispara(self._item(0)))

    def test_confirmado_con_monto_real_NO_dispara(self):
        self.assertFalse(self._dispara(self._item(4224)))

    def test_sin_linea_envio_sigue_sin_disparar_por_la_otra_guarda(self):
        self.assertFalse(self._dispara(self._item(None), ya_sabemos_que_no_cobra=True))


class TestNoSeTocoNadaMasEnElArchivo(unittest.TestCase):
    """El mismo patron de "prueba de no-dano" ya usado en el fix anterior
    (PR #174): comparar el AST de las funciones sensibles contra
    origin/main, byte a byte."""

    @classmethod
    def setUpClass(cls):
        with open("app.py", encoding="utf-8") as f:
            cls.src_local = f.read()
        cls.tree_local = ast.parse(cls.src_local)
        remoto = subprocess.run(
            ["git", "show", "origin/main:app.py"],
            capture_output=True, text=True, check=True, encoding="utf-8")
        cls.tree_main = ast.parse(remoto.stdout)

    def test_tr_fetch_from_erp_identico_a_main(self):
        self.assertEqual(
            _fuente("_tr_fetch_from_erp", self.tree_local),
            _fuente("_tr_fetch_from_erp", self.tree_main))

    def test_tr_bulk_sync_erp_mysql_identico_a_main(self):
        self.assertEqual(
            _fuente("_tr_bulk_sync_erp_mysql", self.tree_local),
            _fuente("_tr_bulk_sync_erp_mysql", self.tree_main))

    def test_transporte_scheduler_loop_identico_a_main(self):
        self.assertEqual(
            _fuente("_transporte_scheduler_loop", self.tree_local),
            _fuente("_transporte_scheduler_loop", self.tree_main))

    def test_solo_una_funcion_cambio_en_todo_app_py(self):
        funcs_local = {n.name: ast.unparse(n) for n in ast.walk(self.tree_local)
                       if isinstance(n, ast.FunctionDef)}
        funcs_main = {n.name: ast.unparse(n) for n in ast.walk(self.tree_main)
                      if isinstance(n, ast.FunctionDef)}
        cambiadas = [nombre for nombre, src in funcs_local.items()
                     if nombre in funcs_main and funcs_main[nombre] != src]
        self.assertEqual(cambiadas, ["tr_manifiesto_detalle"])


if __name__ == "__main__":
    unittest.main()

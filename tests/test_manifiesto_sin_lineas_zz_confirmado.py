"""
2026-08-22: MAN-2026-0049 seguia reconsultando al ERP identicamente en
cargas sucesivas (4.6-4.8s, 4 documentos) incluso despues de PR #174
(zz_skus) y PR #175 (zz_envio_raw). Diagnostico en vivo (endpoint de
PR #176) confirmo la causa: estos 4 documentos NO tienen NINGUNA linea ZZ
(ni envio, ni instalacion, ni retiro) -- no es "cobra $0 confirmado", es
"no hay servicio en absoluto". Para ese caso, zz_envio_raw Y zz_skus se
quedan NULL para siempre (no hay nada que compactar en ninguno de los
dos), asi que NINGUNA de las dos guardas existentes se activaba nunca.

El fix agrega una columna nueva, dedicada, sin ambiguedad:
zz_sin_lineas_confirmado_at (DATETIME NULL) -- se escribe SOLO cuando
_tr_fetch_from_erp tuvo una lectura CONFIABLE (raw_lineas no vino vacio,
el mismo criterio "lectura sospechosa" que ya usaba el UPDATE de
zz_envio) y esa lectura no trajo ninguna linea ZZ. tr_manifiesto_detalle
usa esa marca como tercera condicion para NO reintentar.
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


def _norm(s):
    """Quote-agnostic: ast.unparse normaliza comillas dobles a simples."""
    return s.replace('"', "'")


class TestColumnaGarantizada(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.src = _fuente("_ensure_transporte_columns")

    def test_la_columna_esta_en_el_dict_needed(self):
        self.assertIn("zz_sin_lineas_confirmado_at", self.src)

    def test_el_tipo_es_datetime_null(self):
        normalizado = _norm(self.src)
        self.assertIn("'zz_sin_lineas_confirmado_at': 'DATETIME NULL'", normalizado)

    def test_se_agrega_via_information_schema_no_via_migracion_saltable(self):
        """Esta funcion corre SIEMPRE en boot (app.py, fuera de cualquier
        request), incluso con ILUS_SKIP_MIGRATIONS=1 -- es el patron ya
        usado por region/cod_postal/zz_skus/etc. Solo se verifica que el
        mecanismo (chequeo contra information_schema.COLUMNS) sigue ahi."""
        self.assertIn("information_schema.COLUMNS", self.src)
        self.assertIn("ADD COLUMN", self.src)


class TestEscrituraEnTrFetchFromErp(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.src = _fuente("_tr_fetch_from_erp")

    def test_la_marca_se_escribe_dentro_del_if_raw_lineas(self):
        """Debe estar en la MISMA rama que ya usa raw_lineas como criterio
        de "lectura confiable" (no en el else de lectura sospechosa)."""
        i_if = self.src.index("if raw_lineas:")
        i_else = self.src.index("else:", i_if)
        bloque_confiable = self.src[i_if:i_else]
        self.assertIn("zz_sin_lineas_confirmado_at", bloque_confiable)

    def test_la_marca_se_escribe_solo_si_sin_zz_lines(self):
        i_if = self.src.index("if raw_lineas:")
        i_else = self.src.index("else:", i_if)
        bloque_confiable = self.src[i_if:i_else]
        # Debe haber un "if sin_zz_lines:" ENTRE el UPDATE de zz_envio y el
        # cierre del bloque -- no se escribe incondicionalmente.
        self.assertIn("if sin_zz_lines:", bloque_confiable)
        i_sin_lineas_if = bloque_confiable.index("if sin_zz_lines:")
        self.assertIn("zz_sin_lineas_confirmado_at",
                       bloque_confiable[i_sin_lineas_if:])

    def test_usa_now_literal_en_el_sql(self):
        i_marca = self.src.index("zz_sin_lineas_confirmado_at")
        fragmento = self.src[i_marca:i_marca + 60]
        self.assertIn("NOW()", fragmento)


class TestGuardaEnLaFicha(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.src = _fuente("tr_manifiesto_detalle")

    def test_el_select_trae_la_columna_nueva(self):
        self.assertIn("c.zz_sin_lineas_confirmado_at", self.src)

    def test_la_condicion_de_disparo_exige_las_tres_negaciones(self):
        normalizado = _norm(self.src)
        i_cond = normalizado.index("if it.get('zz_envio_raw') is None")
        fragmento = normalizado[i_cond:i_cond + 400]
        self.assertIn("not _ya_sabemos_que_no_cobra", fragmento)
        self.assertIn("not _confirmado_sin_lineas_zz", fragmento)

    def test_la_variable_lee_la_columna_correcta(self):
        normalizado = _norm(self.src)
        self.assertIn(
            "_confirmado_sin_lineas_zz = bool(it.get('zz_sin_lineas_confirmado_at'))",
            normalizado)

    def test_las_dos_guardas_previas_siguen_intactas(self):
        normalizado = _norm(self.src)
        self.assertIn(
            "_ya_sabemos_que_no_cobra = bool(_skus_doc) and 'ZZENVIO' not in _skus_doc",
            normalizado)
        self.assertIn("it.get('zz_envio_raw') is None", normalizado)

    def test_el_tope_de_sanado_por_carga_sigue_en_5(self):
        self.assertIn("_MAX_HEAL_ZZ_PER_LOAD = 5", self.src)


class TestSemanticaDeLasTresGuardasJuntas(unittest.TestCase):
    """Reproduce la logica exacta (copiada literal) contra los 4 estados
    posibles de un documento."""

    @staticmethod
    def _dispara(it):
        _skus_doc = (it.get("zz_skus") or "").upper()
        _ya_sabemos_que_no_cobra = bool(_skus_doc) and "ZZENVIO" not in _skus_doc
        _confirmado_sin_lineas_zz = bool(it.get("zz_sin_lineas_confirmado_at"))
        return (it.get("zz_envio_raw") is None) and not _ya_sabemos_que_no_cobra \
            and not _confirmado_sin_lineas_zz \
            and it.get("tido") and it.get("nudo")

    def _item(self, **kw):
        base = {"zz_envio_raw": None, "zz_skus": None,
                "zz_sin_lineas_confirmado_at": None,
                "tido": "FCV", "nudo": "11338"}
        base.update(kw)
        return base

    def test_nunca_consultado_dispara(self):
        self.assertTrue(self._dispara(self._item()))

    def test_confirmado_sin_ninguna_linea_zz_NO_dispara(self):
        """El caso real de FCV 11338/11166/11319, BLV 23214: el ERP
        respondio, no hay NINGUNA linea ZZ."""
        self.assertFalse(self._dispara(self._item(
            zz_sin_lineas_confirmado_at="2026-08-22 12:00:00")))

    def test_confirmado_zzenvio_en_cero_sigue_sin_disparar(self):
        """No se rompe el caso que arreglo PR #175."""
        self.assertFalse(self._dispara(self._item(zz_envio_raw=0)))

    def test_sin_linea_envio_pero_con_otras_lineas_zz_sigue_sin_disparar(self):
        """No se rompe el caso que arreglo PR #174 (ej. solo instalacion)."""
        self.assertFalse(self._dispara(self._item(zz_skus="ZZINSTALACION")))

    def test_confirmado_con_monto_real_no_dispara(self):
        self.assertFalse(self._dispara(self._item(zz_envio_raw=4224)))


class TestNoSeTocoNadaMasEnElArchivo(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with open("app.py", encoding="utf-8") as f:
            cls.src_local = f.read()
        cls.tree_local = ast.parse(cls.src_local)
        remoto = subprocess.run(
            ["git", "show", "origin/main:app.py"],
            capture_output=True, text=True, check=True, encoding="utf-8")
        cls.tree_main = ast.parse(remoto.stdout)

    def test_tr_bulk_sync_erp_mysql_identico_a_main(self):
        self.assertEqual(
            _fuente("_tr_bulk_sync_erp_mysql", self.tree_local),
            _fuente("_tr_bulk_sync_erp_mysql", self.tree_main))

    def test_transporte_scheduler_loop_identico_a_main(self):
        self.assertEqual(
            _fuente("_transporte_scheduler_loop", self.tree_local),
            _fuente("_transporte_scheduler_loop", self.tree_main))

    def test_solo_las_4_funciones_esperadas_cambiaron(self):
        funcs_local = {n.name: ast.unparse(n) for n in ast.walk(self.tree_local)
                       if isinstance(n, ast.FunctionDef)}
        funcs_main = {n.name: ast.unparse(n) for n in ast.walk(self.tree_main)
                      if isinstance(n, ast.FunctionDef)}
        cambiadas = sorted(nombre for nombre, src in funcs_local.items()
                            if nombre in funcs_main and funcs_main[nombre] != src)
        nuevas = sorted(nombre for nombre in funcs_local if nombre not in funcs_main)
        self.assertEqual(nuevas, [])
        self.assertEqual(
            cambiadas,
            ["_ensure_transporte_columns", "_fetch_items",
             "_tr_fetch_from_erp", "tr_manifiesto_detalle"])


if __name__ == "__main__":
    unittest.main()

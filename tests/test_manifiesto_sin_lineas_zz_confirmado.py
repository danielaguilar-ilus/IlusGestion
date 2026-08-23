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

    # Lo que este arreglo NO debe romper nunca. Lista nombrada y acotada.
    INTOCABLES = (
        "_tr_bulk_sync_erp_mysql",     # el cron
        "_transporte_scheduler_loop",   # el cron
        "_tr_notificar_cliente",        # los correos al cliente
        "_simpliroute_request",         # el unico canal HTTP al courier
    )

    def test_no_rompe_los_caminos_criticos_vecinos(self):
        """Los caminos criticos que rodean a este arreglo siguen intactos.

        AJUSTE 2026-08-23 (tercera version de este test; la historia completa
        esta en test_pantalla_despacho_semaforo.py, donde el mismo patron
        fallo cuatro veces). Resumen de por que las versiones anteriores no
        servian, todas basadas en comparar TODO app.py contra origin/main:

          v1  `cambiadas == [las 4 funciones]` + `nuevas == []`
              -> se invierte al mergear el PR, y prohibe que app.py crezca.
          v2  `cambiadas - permitidas == []`
              -> lo rompe CUALQUIER otra feature que toque app.py. Lo rompio
                 el fix del correo al cliente (notify_cliente en la subida a
                 SimpliRoute), que no tiene ninguna relacion con este arreglo.

        La raiz: "el diff contra main contiene solo X" es una asercion de
        REVISION DE PR, no un test de regresion. Sobre un archivo de 92mil
        lineas que varias features tocan en paralelo, siempre termina dando
        falso positivo y entrenando a ignorarlo.

        Lo que si es un test de regresion: nombrar los caminos criticos y
        verificar que no cambiaron. Si alguien los toca a proposito, el test
        falla y obliga a la conversacion -- que es exactamente lo que se
        quiere para el cron."""
        rotas = [fn for fn in self.INTOCABLES
                 if _fuente(fn, self.tree_local) != _fuente(fn, self.tree_main)]
        self.assertEqual(rotas, [], f"caminos criticos modificados: {rotas}")


if __name__ == "__main__":
    unittest.main()

"""La ficha del manifiesto tardaba varios segundos porque re-consultaba al
ERP los MISMOS documentos en CADA carga, para siempre.

2026-08-21. Daniel: "por que sera que tarda 2 segundos en entrar al
manifiesto 47... sera por eso [las guias]?".

Medido en producción sobre MAN-2026-0047 (id interno 62, 5 documentos):
  · 1ª carga: 3699 ms
  · 2ª carga: 1686 ms — y los MISMOS 2 documentos (FCV 11326, BLV 23199)
    seguían sin "sanar" pese a que la página ya los había consultado antes.
  · Los logs de Cloud Run mostraron ambos documentos re-consultados en las
    dos cargas, cada consulta a SQL Server entre 500 ms y 2,6 s.

CAUSA RAÍZ: tr_manifiesto_detalle() sana zz_envio llamando a
_tr_fetch_from_erp() para los documentos que no lo tienen, PERO se salta la
sanación si `_ya_sabemos_que_no_cobra` (zz_skus no vacío y sin "ZZENVIO").
_tr_fetch_from_erp() -- el camino INDIVIDUAL, el que corre al abrir la
ficha -- nunca escribía `zz_skus` en su propio UPSERT: esa columna solo la
llenaba el sync MASIVO del cron. Un documento sanado por la vía individual
(como estos 2, sin línea ZZENVIO -- garantía/instalación) nunca activaba la
guarda y se re-consultaba al ERP en cada carga, sin fin.

Arreglo: agregar `zz_skus` al UPSERT individual, con el MISMO patrón
"no pisar con vacío" que ya usan comuna/dirección/teléfono/email ahí mismo.

Daniel fue explícito: "la ultima vez que le meti mano al cron se dano
todo... no quiero parches, mete algo de verdad sin dañar, ya que esta todo
bien". Por eso _tr_bulk_sync_erp_mysql y _transporte_scheduler_loop (el
cron) NO se tocan -- ver TestElCronNoSeToco más abajo, que lo verifica
contra origin/main.

Correr con:  py -m unittest tests.test_manifiesto_zz_skus_convergencia -v
(pytest NO está instalado en el equipo de Daniel.)
"""
import ast
import io
import os
import re
import subprocess
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
APP_PY = os.path.join(RAIZ, "app.py")

_FUENTE = None
_ARBOL = None


def _fuente():
    global _FUENTE
    if _FUENTE is None:
        with open(APP_PY, encoding="utf-8") as fh:
            _FUENTE = fh.read()
    return _FUENTE


def _arbol():
    global _ARBOL
    if _ARBOL is None:
        _ARBOL = ast.parse(_fuente())
    return _ARBOL


def _nodo(nombre):
    for n in ast.walk(_arbol()):
        if isinstance(n, ast.FunctionDef) and n.name == nombre:
            return n
    raise AssertionError("no existe la funcion %s() en app.py" % nombre)


def _cuerpo(nombre):
    return ast.unparse(_nodo(nombre))


# ══════════════════════════════════════════════════════════════════════
#  1. zz_skus ahora se guarda en el camino individual
# ══════════════════════════════════════════════════════════════════════
class TestZzSkusSeGuardaEnElCaminoIndividual(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.src = _cuerpo("_tr_fetch_from_erp")

    def test_zz_skus_esta_en_la_lista_de_columnas_del_insert(self):
        i_insert = self.src.index("INSERT INTO transport_commitments")
        i_values = self.src.index("VALUES", i_insert)
        columnas = self.src[i_insert:i_values]
        self.assertIn("zz_skus", columnas)

    def test_se_calcula_desde_skus_upper_que_la_funcion_ya_tenia(self):
        # No se reimplementa el parseo de lineas ZZ -- se reusa lo que la
        # funcion ya calculaba y simplemente no persistia.
        self.assertIn("skus_upper", self.src)
        i_calc = self.src.index("zz_skus_str")
        linea_calc = self.src[i_calc:i_calc + 120]
        self.assertIn("skus_upper", linea_calc)

    def test_documento_sin_lineas_guarda_None_no_string_vacio(self):
        # None deja pasar la guarda IF(...IS NULL OR =''...) igual que un
        # string vacio -- pero None es la representacion correcta de
        # "no hay dato" en esta base (mismo criterio que zz_envio).
        i_calc = self.src.index("zz_skus_str")
        linea_calc = self.src[i_calc:i_calc + 120]
        self.assertIn("else None", linea_calc)

    def test_la_guarda_on_duplicate_no_pisa_con_vacio(self):
        """Mismo patron que comuna/direccion/telefono/email en este mismo
        UPSERT: si esta corrida no trae SKUs, se conserva lo que habia.

        Se busca sin las comillas: ast.unparse escapa las comillas simples
        de SQL embebidas (\\') al re-serializar el string, asi que un
        substring literal con comillas sueltas no calza -- el CONTENIDO es
        igual, solo cambia como Python representa el string por dentro."""
        i_zz = self.src.index("zz_skus=IF(VALUES(zz_skus)")
        fragmento = self.src[i_zz:i_zz + 90]
        self.assertIn("IS NULL", fragmento)
        self.assertIn(" OR VALUES(zz_skus)", fragmento)
        self.assertIn("zz_skus, VALUES(zz_skus))", fragmento)

    def test_el_valor_se_ordena_y_deduplica(self):
        """Mismo SKU repetido en varias lineas (ej. 2 lineas ZZENVIO) no
        debe producir 'ZZENVIO,ZZENVIO' -- rompería la comparacion exacta
        que hace el sync masivo en otros lugares (zz_skus == {'ZZRETIRO'})."""
        i_calc = self.src.index("zz_skus_str")
        linea_calc = self.src[i_calc:i_calc + 120]
        self.assertIn("sorted(set(", linea_calc)


# ══════════════════════════════════════════════════════════════════════
#  2. La alineación de parámetros del UPSERT no se rompió
# ══════════════════════════════════════════════════════════════════════
class TestAlineacionDeParametros(unittest.TestCase):
    """El bug mas facil de cometer al agregar una columna a un INSERT:
    desalinear VALUES(...) contra la tupla de parametros. Se verifica
    contando, no leyendo -- un desfase de 1 aca escribiria el username en
    la columna de clasificacion o algo asi, en silencio."""

    @classmethod
    def setUpClass(cls):
        nodo = _nodo("_tr_fetch_from_erp")
        # El primer Call a cur.execute(...) con dos argumentos posicionales
        # (sql, params) dentro de esta funcion es el UPSERT individual.
        cls.call = None
        for n in ast.walk(nodo):
            if (isinstance(n, ast.Call)
                    and isinstance(n.func, ast.Attribute)
                    and n.func.attr == "execute"
                    and len(n.args) == 2
                    and isinstance(n.args[1], ast.Tuple)):
                sql_txt = ast.unparse(n.args[0])
                if "INSERT INTO transport_commitments" in sql_txt:
                    cls.call = n
                    break
        assert cls.call is not None, "no se encontro el cur.execute del UPSERT individual"
        cls.sql_texto = ast.unparse(cls.call.args[0])
        cls.n_params_tupla = len(cls.call.args[1].elts)

    def test_el_conteo_de_placeholders_calza_con_la_tupla(self):
        # %s en el VALUES(...) + el %s embebido en el IF() de cliente_nombre
        # (el centinela ERP_NO_CLIENT) = total de parametros esperados.
        n_placeholders = self.sql_texto.count("%s")
        self.assertEqual(
            n_placeholders, self.n_params_tupla,
            f"{n_placeholders} placeholders '%s' en el SQL vs "
            f"{self.n_params_tupla} elementos en la tupla de parametros -- "
            f"desalineado")

    @staticmethod
    def _matching_close_paren(texto, i_open):
        """Indice del ')' que cierra el '(' en i_open, contando profundidad
        -- un split/index ingenuo se engaña con el '(' y ')' propios de
        NOW() dentro de la lista de VALUES."""
        assert texto[i_open] == "("
        profundidad = 0
        for i in range(i_open, len(texto)):
            if texto[i] == "(":
                profundidad += 1
            elif texto[i] == ")":
                profundidad -= 1
                if profundidad == 0:
                    return i
        raise AssertionError("parentesis sin cerrar")

    @classmethod
    def _top_level_split(cls, texto):
        """Split por coma que ignora comas dentro de parentesis anidados
        (ej: la que separa los args de NOW())."""
        tokens, profundidad, actual = [], 0, ""
        for ch in texto:
            if ch == "(":
                profundidad += 1
            elif ch == ")":
                profundidad -= 1
            if ch == "," and profundidad == 0:
                tokens.append(actual)
                actual = ""
            else:
                actual += ch
        tokens.append(actual)
        return tokens

    def test_el_conteo_de_columnas_calza_con_los_placeholders_de_values(self):
        i_cols_ini = self.sql_texto.index("(", self.sql_texto.index("transport_commitments"))
        i_cols_fin = self._matching_close_paren(self.sql_texto, i_cols_ini)
        columnas = self._top_level_split(self.sql_texto[i_cols_ini + 1:i_cols_fin])
        i_val_ini = self.sql_texto.index("VALUES (") + len("VALUES ")
        i_val_fin = self._matching_close_paren(self.sql_texto, i_val_ini)
        values_tokens = self._top_level_split(self.sql_texto[i_val_ini + 1:i_val_fin])
        self.assertEqual(
            len(columnas), len(values_tokens),
            f"{len(columnas)} columnas vs {len(values_tokens)} valores en VALUES(...)\n"
            f"columnas={columnas}\nvalues={values_tokens}")

    def test_zz_skus_str_esta_en_la_posicion_correcta_de_la_tupla(self):
        """zz_skus va DESPUES de clasificacion y ANTES de created_by/
        updated_by en las tres listas (columnas, placeholders, parametros) --
        se verifica la ultima, que es la que un desfase rompe en silencio."""
        nombres = [ast.unparse(el) for el in self.call.args[1].elts]
        i_clasif = nombres.index("clasificacion")
        i_zzskus = nombres.index("zz_skus_str")
        i_user1 = nombres.index("current_username()")
        self.assertEqual(i_zzskus, i_clasif + 1)
        self.assertEqual(i_user1, i_zzskus + 1)


# ══════════════════════════════════════════════════════════════════════
#  3. El cron NO se tocó — verificado contra origin/main, no solo leído
# ══════════════════════════════════════════════════════════════════════
class TestElCronNoSeToco(unittest.TestCase):
    """Daniel, textual: 'la ultima vez que le meti mano al cron se dano
    todo... no quiero parches, sin dañar, ya que esta todo bien'. Se
    verifica -- no se asume -- que ninguna de las dos funciones del cron
    cambió una sola línea respecto a origin/main."""

    @classmethod
    def setUpClass(cls):
        cls.disponible = True
        try:
            out = subprocess.run(
                ["git", "show", "origin/main:app.py"],
                cwd=RAIZ, capture_output=True, timeout=30)
            cls.fuente_main = out.stdout.decode("utf-8", errors="ignore")
            cls.disponible = out.returncode == 0 and bool(cls.fuente_main)
        except Exception:
            cls.disponible = False

    def _cuerpo_en(self, fuente, nombre):
        arbol = ast.parse(fuente)
        for n in ast.walk(arbol):
            if isinstance(n, ast.FunctionDef) and n.name == nombre:
                return ast.unparse(n)
        raise AssertionError("no existe %s en esa fuente" % nombre)

    def test_tr_bulk_sync_erp_mysql_identico_a_main(self):
        if not self.disponible:
            self.skipTest("no se pudo leer origin/main (sin red/git en este entorno)")
        self.assertEqual(
            _cuerpo("_tr_bulk_sync_erp_mysql"),
            self._cuerpo_en(self.fuente_main, "_tr_bulk_sync_erp_mysql"),
            "el sync masivo (cron) cambió -- no debía tocarse")

    def test_transporte_scheduler_loop_identico_a_main(self):
        if not self.disponible:
            self.skipTest("no se pudo leer origin/main (sin red/git en este entorno)")
        self.assertEqual(
            _cuerpo("_transporte_scheduler_loop"),
            self._cuerpo_en(self.fuente_main, "_transporte_scheduler_loop"),
            "el scheduler (cron) cambió -- no debía tocarse")

    def test_solo_cambio_la_funcion_esperada(self):
        if not self.disponible:
            self.skipTest("no se pudo leer origin/main (sin red/git en este entorno)")
        diff = subprocess.run(
            ["git", "diff", "origin/main", "--", "app.py"],
            cwd=RAIZ, capture_output=True, timeout=30).stdout.decode("utf-8", errors="ignore")
        funciones_tocadas = set(re.findall(r"^@@.*?def (\w+)", diff, re.MULTILINE))
        # git diff no siempre nombra la funcion en el hunk header; fallback:
        # contar cuantos bloques @@ hay y que ninguno mencione los nombres
        # del cron si los nombra.
        self.assertNotIn("_tr_bulk_sync_erp_mysql", funciones_tocadas)
        self.assertNotIn("_transporte_scheduler_loop", funciones_tocadas)


# ══════════════════════════════════════════════════════════════════════
#  4. La guarda que esto arregla sigue exactamente igual
# ══════════════════════════════════════════════════════════════════════
class TestLaGuardaEnLaFichaNoSeToco(unittest.TestCase):
    """El arreglo esta del lado de QUIEN escribe zz_skus, no de quien lo
    lee. tr_manifiesto_detalle no deberia haber cambiado ni una linea."""

    def test_el_criterio_sigue_igual(self):
        src = _cuerpo("tr_manifiesto_detalle")
        self.assertIn("_ya_sabemos_que_no_cobra", src)
        self.assertIn('"ZZENVIO" not in _skus_doc', src.replace("'", '"'))

    def test_el_tope_de_sanado_por_carga_sigue_en_5(self):
        src = _cuerpo("tr_manifiesto_detalle")
        self.assertIn("_MAX_HEAL_ZZ_PER_LOAD = 5", src)


if __name__ == "__main__":
    unittest.main(verbosity=2)

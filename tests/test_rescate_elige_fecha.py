"""El rescate de visitas congeladas puede elegir el dia destino.

2026-08-18, misma tarde que PR #162. Al ir a ejecutar el rescate de las 19
visitas congeladas eran las 19:37 en Chile: el tablero del despachador de
Felca del dia ya estaba cerrado. Reprogramarlas "a hoy" las habria dejado
vencidas otra vez a la mañana siguiente -- el mismo bug que se acababa de
arreglar, ahora en version diaria.

Por eso el endpoint acepta `fecha`. Sin ella sigue siendo HOY, para no
romper a ningun caller que ya exista.

Correr con:  py -m unittest tests.test_rescate_elige_fecha
(pytest NO esta instalado en el equipo de Daniel.)
"""
import ast
import datetime as dt
import os
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


def _ejecutable(nombre, ambito=None):
    ns = dict(ambito or {})
    exec(compile(ast.Module(body=[_nodo(nombre)], type_ignores=[]),
                 "<app.py>", "exec"), ns)
    return ns[nombre]


HOY = dt.date(2026, 8, 18)   # el martes real del incidente


# ══════════════════════════════════════════════════════════════════════
#  1. La validacion de la fecha destino
# ══════════════════════════════════════════════════════════════════════
class TestFechaDestino(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # staticmethod: sin esto Python trata la funcion guardada como
        # atributo de clase como un METODO y le pasa self -> TypeError.
        cls.fn = staticmethod(_ejecutable("_sr_fecha_rescate"))

    # ── sin fecha: se conserva el comportamiento anterior ──────────────
    def test_sin_fecha_es_hoy(self):
        for vacio in (None, "", "   "):
            with self.subTest(v=vacio):
                self.assertEqual(self.fn(vacio, HOY), ("2026-08-18", None))

    # ── el caso que motiva el cambio ───────────────────────────────────
    def test_el_dia_siguiente_es_valido(self):
        """19:37 de un martes: el rescate util es al miercoles."""
        self.assertEqual(self.fn("2026-08-19", HOY), ("2026-08-19", None))

    def test_hoy_explicito_tambien_vale(self):
        self.assertEqual(self.fn("2026-08-18", HOY), ("2026-08-18", None))

    # ── nunca al pasado: es el bug original ────────────────────────────
    def test_el_pasado_se_rechaza(self):
        """Programar hacia atras es EXACTAMENTE lo que dejaba las visitas
        invisibles. El endpoint que las repara no puede reproducirlo."""
        for atras in ("2026-08-17", "2026-08-04", "2025-01-01"):
            with self.subTest(f=atras):
                fecha, err = self.fn(atras, HOY)
                self.assertIsNone(fecha)
                self.assertIn("pasado", err)

    # ── tope: un dedazo de año manda la carga al limbo ─────────────────
    def test_mas_de_30_dias_se_rechaza(self):
        fecha, err = self.fn("2027-08-19", HOY)
        self.assertIsNone(fecha)
        self.assertIn("30", err)

    def test_el_borde_de_30_dias_pasa(self):
        self.assertEqual(self.fn("2026-09-17", HOY), ("2026-09-17", None))

    def test_el_dia_31_no_pasa(self):
        self.assertIsNone(self.fn("2026-09-18", HOY)[0])

    # ── entrada basura ────────────────────────────────────────────────
    def test_formato_invalido_se_rechaza_con_mensaje_util(self):
        for basura in ("19/08/2026", "manana", "2026-13-01", "2026-02-30", "xx"):
            with self.subTest(v=basura):
                fecha, err = self.fn(basura, HOY)
                self.assertIsNone(fecha)
                self.assertIn("AAAA-MM-DD", err)

    def test_no_revienta_con_tipos_raros(self):
        for basura in (42, [], {}, 3.5):
            with self.subTest(v=basura):
                fecha, err = self.fn(basura, HOY)
                # No importa el veredicto: importa que no lance excepcion.
                self.assertTrue(fecha is None or isinstance(fecha, str))

    def test_el_mensaje_de_error_no_trae_jerga_tecnica(self):
        """REGLA #4: nunca detalles internos al usuario."""
        for basura in ("xx", "2020-01-01", "2030-01-01"):
            _, err = self.fn(basura, HOY)
            if err:
                for jerga in ("Traceback", "ValueError", "isoformat"):
                    self.assertNotIn(jerga, err)


# ══════════════════════════════════════════════════════════════════════
#  2. El endpoint usa la fecha validada en TODAS partes
# ══════════════════════════════════════════════════════════════════════
class TestElEndpointUsaLaFechaElegida(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.src = _cuerpo("tr_simpliroute_rescatar_congeladas")
        cls.plano = cls.src.replace('"', "'")

    def test_lee_el_parametro_fecha(self):
        self.assertIn("_sr_fecha_rescate(body.get('fecha'), hoy)", self.plano)

    def test_corta_con_400_si_la_fecha_es_invalida(self):
        self.assertIn("_err_fecha", self.src)
        self.assertIn("400", self.src)

    def test_el_patch_manda_la_fecha_elegida_no_hoy(self):
        """El bug seria mandar destino en el mensaje y hoy en el PATCH."""
        self.assertIn("'planned_date': destino_str", self.plano)

    def test_no_queda_ningun_hoy_str_suelto(self):
        # Si sobrevive uno, alguna rama sigue usando hoy y el rescate
        # quedaria a medias -- distinto en el log que en SimpliRoute.
        self.assertNotIn("hoy_str", self.src)

    def test_la_trazabilidad_registra_la_fecha_real_aplicada(self):
        self.assertIn("destino_str", self.src)
        self.assertIn("visita SimpliRoute rescatada", self.src)

    def test_la_respuesta_informa_fecha_y_hoy(self):
        # `hoy` va aparte para que la UI pueda decir "reprogramado a mañana".
        self.assertIn("'fecha': destino_str", self.plano)
        self.assertIn("'hoy': hoy.isoformat()", self.plano)


# ══════════════════════════════════════════════════════════════════════
#  3. Nada de lo que ya protegia se aflojo
# ══════════════════════════════════════════════════════════════════════
class TestSiguenLasProtecciones(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.src = _cuerpo("tr_simpliroute_rescatar_congeladas")

    def test_sigue_revalidando_contra_la_api_antes_del_patch(self):
        self.assertIn("_sr_visita_sin_planificar(v)", self.src)
        self.assertLess(self.src.index("'GET'"), self.src.index("'PATCH'"))

    def test_sigue_exigiendo_item_ids(self):
        self.assertIn("item_ids", self.src)

    def test_sigue_el_tope_de_100(self):
        self.assertIn("[:100]", self.src)

    def test_sigue_sin_tocar_lo_que_ya_esta_en_la_fecha_destino(self):
        self.assertIn("Ya está programada para el", self.src)

    def test_el_listado_sigue_siendo_solo_lectura(self):
        # REGLA #4.2: el parametro nuevo no puede haber contaminado el GET.
        listado = _cuerpo("tr_simpliroute_visitas_congeladas")
        for prohibido in ("mysql_execute", "'PATCH'", "'POST'", "'DELETE'"):
            self.assertNotIn(prohibido, listado)


if __name__ == "__main__":
    unittest.main(verbosity=2)

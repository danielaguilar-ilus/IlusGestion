"""El panel medía "sin planificar" con una señal que en esta cuenta no existe.

CORRECCIÓN 2026-08-20, con medición contra la API real de SimpliRoute
(cuenta de Felca, rjosencarpio@gmail.com, id 491482).

El 18-ago construí `_sr_visita_sin_planificar`: una visita estaba "congelada"
si su status era 'pending' Y no tenía `route`. La premisa era que `route`
aparece cuando el despachador asigna la visita a un vehículo.

Medido dos días después sobre los días 13, 14 y 19 de agosto: de 7 visitas
ENTREGADAS (status 'completed'), las 7 vienen con `route` en null. Felca NO
usa el módulo Router -- marca las entregas directo desde la app del chofer.

O sea que `route` no distingue nada acá, y el criterio marcaba como
"congelada" cada entrega normal que todavía no había ocurrido. Una
herramienta que grita por todo se aprende a ignorar.

════════ EL HALLAZGO QUE EXPLICA EL CASO ENTERO ════════
Midiendo quién crea cada visita por el FORMATO de su reference:

    reference con prefijo   (FCV-11303, BLV-23140)  -> la creó ILUS
        0 entregadas, 15 pendientes
    reference número pelado (11281, 23069)          -> la cargó Felca por Excel
        7 entregadas, 4 pendientes

NINGUNA visita creada por ILUS se ha entregado jamás. Las entregas que el
sistema registraba como éxito eran del propio courier, adoptadas después por
_simpliroute_reconciliar_huerfanos.

⚠️ Por eso NO sirve preguntarle a la base "¿es nuestra?": tener el visit_id
guardado no significa que la creamos. El único dato que lo distingue es el
formato de la reference.

Correr con:  py -m unittest tests.test_simpliroute_estado_real -v
(pytest NO está instalado en el equipo de Daniel.)
"""
import ast
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


# ══════════════════════════════════════════════════════════════════════
#  1. El criterio corregido: el estado, y NADA de `route`
# ══════════════════════════════════════════════════════════════════════
class TestSinEntregar(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # staticmethod: sin esto Python trata la funcion guardada como
        # atributo de clase como un METODO y le pasa self -> TypeError.
        cls.fn = staticmethod(_ejecutable("_sr_visita_sin_entregar"))

    def test_pending_sigue_sin_entregar(self):
        self.assertTrue(self.fn({"status": "pending"}))

    def test_LA_CORRECCION_pending_CON_ruta_tambien_cuenta(self):
        """El criterio viejo la daba por planificada y la excluia. Pero tener
        ruta no significa entregada -- puede quedarse ahi igual."""
        self.assertTrue(self.fn({"status": "pending", "route": {"driver_name": "Rafael"}}))

    def test_LA_CORRECCION_completed_SIN_ruta_NO_cuenta(self):
        """El caso medido: las 7 entregadas venian sin `route`. Con el
        criterio viejo igual quedaban fuera (por el status), pero la razon
        importa: `route` no es señal de nada en esta cuenta."""
        self.assertFalse(self.fn({"status": "completed", "route": None}))

    def test_los_estados_en_que_ya_paso_algo_no_cuentan(self):
        for estado in ("on_its_way", "completed", "failed", "partial", "canceled"):
            with self.subTest(estado=estado):
                self.assertFalse(self.fn({"status": estado, "route": None}))

    def test_mayusculas_y_espacios_no_cambian_el_veredicto(self):
        self.assertTrue(self.fn({"status": " PENDING "}))

    def test_una_respuesta_rara_no_revienta(self):
        for basura in (None, [], "pending", 42):
            with self.subTest(v=basura):
                self.assertFalse(self.fn(basura))

    def test_ya_no_mira_route(self):
        """Regresion dura: si alguien reintroduce la LECTURA de `route`,
        volvemos a marcar como trabada cada entrega normal pendiente.

        Se mira el codigo EJECUTABLE, no el docstring -- ahi la palabra
        aparece a proposito, explicando por que no se usa."""
        nodo = _nodo("_sr_visita_sin_entregar")
        cuerpo = [n for n in nodo.body
                  if not (isinstance(n, ast.Expr) and isinstance(n.value, ast.Constant)
                          and isinstance(n.value.value, str))]
        codigo = "\n".join(ast.unparse(n) for n in cuerpo)
        self.assertNotIn("route", codigo)
        self.assertIn("status", codigo)


# ══════════════════════════════════════════════════════════════════════
#  2. Quién creó la visita — el dato que explica todo
# ══════════════════════════════════════════════════════════════════════
class TestQuienCreoLaVisita(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.fn = staticmethod(_ejecutable("_sr_quien_creo_la_visita"))

    def test_con_prefijo_es_de_ILUS(self):
        # build_visit_payload arma "TIDO-NUDO".
        for ref in ("FCV-11303", "BLV-23140", "NVI-586", "VD-10228"):
            with self.subTest(ref=ref):
                self.assertEqual(self.fn(ref), "ILUS")

    def test_el_formato_viejo_con_ceros_tambien_es_de_ILUS(self):
        """Antes de PR #155 mandabamos el nudo con los ceros del ERP."""
        self.assertEqual(self.fn("FCV-0000011216"), "ILUS")

    def test_numero_pelado_lo_cargo_el_COURIER(self):
        # El Excel del courier pone el nudo solo en "Id de referencia".
        for ref in ("11281", "23069", "11285"):
            with self.subTest(ref=ref):
                self.assertEqual(self.fn(ref), "COURIER")

    def test_sin_referencia_es_desconocido(self):
        for vacio in (None, "", "   "):
            with self.subTest(v=vacio):
                self.assertEqual(self.fn(vacio), "DESCONOCIDO")

    def test_no_revienta_con_basura(self):
        for basura in ("???", "-", "abc def"):
            with self.subTest(v=basura):
                self.assertIn(self.fn(basura), ("ILUS", "COURIER", "DESCONOCIDO"))


# ══════════════════════════════════════════════════════════════════════
#  3. El listado dice la verdad
# ══════════════════════════════════════════════════════════════════════
class TestElListadoDiceLaVerdad(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.src = _cuerpo("tr_simpliroute_visitas_congeladas")
        cls.plano = cls.src.replace('"', "'")

    def test_usa_el_criterio_corregido(self):
        self.assertIn("_sr_visita_sin_entregar(v)", self.src)

    def test_expone_quien_creo_cada_visita(self):
        self.assertIn("_sr_quien_creo_la_visita", self.src)
        self.assertIn("'origen'", self.plano)

    def test_devuelve_la_reference_para_poder_auditarlo(self):
        self.assertIn("'reference'", self.plano)

    def test_cuenta_cuantas_son_nuestras(self):
        self.assertIn("total_creadas_por_ILUS", self.src)
        self.assertIn("total_cargadas_por_COURIER", self.src)

    def test_el_resumen_ya_no_dice_sin_planificar(self):
        """En esta cuenta el courier no usa el modulo de rutas, asi que
        'planificada' no significa nada. Decirlo confunde."""
        self.assertNotIn("sin planificar", self.src)
        self.assertIn("sin entregar", self.src)

    def test_sigue_marcando_las_vencidas(self):
        # Lo que convierte un pendiente en problema es que su dia ya paso.
        self.assertIn("vencida", self.src)
        self.assertIn("dias_en_el_pasado", self.src)

    def test_sigue_siendo_solo_lectura(self):
        for prohibido in ("mysql_execute", "'PATCH'", "'POST'", "'DELETE'"):
            with self.subTest(p=prohibido):
                self.assertNotIn(prohibido, self.src)


# ══════════════════════════════════════════════════════════════════════
#  4. Lo que no se rompió
# ══════════════════════════════════════════════════════════════════════
class TestNoSeRompioElRescate(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.src = _cuerpo("tr_simpliroute_rescatar_congeladas")

    def test_el_rescate_usa_el_criterio_nuevo(self):
        self.assertIn("_sr_visita_sin_entregar(v)", self.src)

    def test_sigue_revalidando_antes_de_tocar(self):
        self.assertLess(self.src.index("'GET'"), self.src.index("'PATCH'"))

    def test_sigue_exigiendo_item_ids_y_tope(self):
        self.assertIn("item_ids", self.src)
        self.assertIn("[:100]", self.src)

    def test_no_queda_ninguna_llamada_al_nombre_viejo(self):
        """El rename tiene que ser completo: una llamada al nombre viejo
        seria un NameError en produccion."""
        fuente = _fuente()
        self.assertNotIn("_sr_visita_sin_planificar(", fuente)


if __name__ == "__main__":
    unittest.main(verbosity=2)

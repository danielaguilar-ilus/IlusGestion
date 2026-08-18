"""Las visitas de SimpliRoute nacen con la fecha del DIA EN QUE SE SUBEN.

Levantamiento urgente del 2026-08-18. Daniel, citando a Alison y al jefe de
bodega: "estaba subiendo todo desde el boton de SimpliRoute a Transportes
Felca y no lo entrega, no aparece en SimpliRoute... al parecer no estamos
conectados a la API".

La API SI estaba conectada. Lo que fallaba era la FECHA. Evidencia medida
contra la API real ese mismo dia (endpoint /trazabilidad, campo
simpliroute_live):

  FCV 11286  manifiesto 13/08, subido el 14/08 -> planned_date 13/08
             -> status 'pending', sin chofer, 4,4 DIAS congelado.
  FCV 11273  manifiesto 17/08, subido el 18/08 -> planned_date 17/08
             -> status 'pending'.
  FCV 11292  manifiesto 17/08, subido el 17/08 -> status 'pending'.
  BLV 23088  (CONTROL) visita creada por el PROPIO Felca, fecha del dia
             -> status 'completed', ENTREGADA. Su evento en ILUS dice
             "Visita SimpliRoute reconciliada automaticamente ... encontrada
             por reference=23088", o sea ILUS no la creo: la adopto.

Conclusion: ninguna visita creada por ILUS llego nunca a entregarse. Las que
funcionaron eran del propio courier. La visita caia en un dia ya cerrado del
tablero del despachador y quedaba invisible.

Criterio de Daniel para el arreglo (textual): "el dia que se suben a
SimpliRoute, porque ahi se estaria entregando al transporte". Subir = entregar
la carga al courier.

Correr con:  py -m unittest tests.test_simpliroute_fecha_y_planificacion
(pytest NO esta instalado en el equipo de Daniel.)
"""
import ast
import datetime as dt
import os
import re
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
    raise AssertionError(f"no existe la funcion {nombre}() en app.py")


def _cuerpo(nombre):
    return ast.unparse(_nodo(nombre))


def _ejecutable(nombre, ambito=None):
    """Extrae UNA funcion pura de app.py y la devuelve ejecutable.

    Importar app.py entero levanta Flask, la base y los hilos de cron. Para
    una funcion sin dependencias basta con compilar su nodo.
    """
    ns = dict(ambito or {})
    exec(compile(ast.Module(body=[_nodo(nombre)], type_ignores=[]),
                 "<app.py>", "exec"), ns)
    return ns[nombre]


# ══════════════════════════════════════════════════════════════════════
#  1. La fecha: HOY, nunca la del manifiesto
# ══════════════════════════════════════════════════════════════════════
class TestLaFechaEsElDiaDeLaSubida(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.src = _cuerpo("tr_manifiesto_subir_simpliroute")

    def test_planned_date_sale_de_la_hora_chile_no_del_manifiesto(self):
        self.assertIn("planned_date = _now_chile().date().isoformat()", self.src)

    def test_ya_no_usa_la_fecha_del_manifiesto_como_planned_date(self):
        """El bug exacto: planned_date salia de man['fecha'].

        Si alguien restaura esa linea, los despachos vuelven a caer en dias
        cerrados y nadie se entera hasta que el cliente reclama.
        """
        self.assertNotIn(
            "planned_date = _fecha.isoformat() if hasattr(_fecha, 'isoformat')", self.src)

    def test_la_fecha_del_manifiesto_se_conserva_para_comparar(self):
        # No se borra: se usa para AVISAR que la fecha cambio (REGLA #4.2 --
        # el dato no se pierde, cambia su rol).
        self.assertIn("fecha_manifiesto", self.src)
        self.assertIn("fecha_cambiada", self.src)

    def test_usa_hora_chile_no_utc(self):
        """En Chile (UTC-4) una subida a las 21:30 es 01:30 UTC del dia
        SIGUIENTE. Con UTC, la visita nacia manana y quedaba igual de
        invisible -- el mismo bug con otro disfraz. REGLA #6."""
        self.assertIn("_now_chile()", self.src)
        self.assertNotIn("datetime.utcnow().date()", self.src)


class TestSeAvisaCuandoLaFechaCambia(unittest.TestCase):
    """Cambiar la fecha en silencio seria tan malo como el bug: Alison tiene
    que saber en que dia del tablero del courier quedo la carga."""

    @classmethod
    def setUpClass(cls):
        cls.src = _cuerpo("tr_manifiesto_subir_simpliroute")

    def test_la_respuesta_trae_avisos(self):
        self.assertIn('"avisos": avisos', self.src.replace("'", '"'))

    def test_avisa_que_la_visita_queda_sin_planificar(self):
        # El aviso que faltaba: "creadas=1, errores=0" se leia como
        # "el courier ya la tiene", y no es cierto.
        self.assertIn("SIN PLANIFICAR", self.src)

    def test_la_trazabilidad_guarda_la_fecha_usada(self):
        # Sin esto no se puede reconstruir en que dia cayo cada visita --
        # fue justo lo que costo diagnosticar el problema.
        self.assertIn("fecha_visita=", self.src)


# ══════════════════════════════════════════════════════════════════════
#  2. El criterio de "sin planificar" vive en UN solo lugar
# ══════════════════════════════════════════════════════════════════════
class TestCriterioSinPlanificar(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # staticmethod: sin esto Python trata la funcion guardada como
        # atributo de clase como un METODO y le pasa self -> TypeError.
        cls.fn = staticmethod(_ejecutable("_sr_visita_sin_planificar"))

    def test_pending_sin_ruta_es_una_visita_congelada(self):
        # El caso real de FCV 11286.
        self.assertTrue(self.fn({"status": "pending", "route": None}))
        self.assertTrue(self.fn({"status": "pending"}))

    def test_pending_con_ruta_asignada_NO_se_toca(self):
        # El despachador ya la planifico: moverle la fecha seria peor que
        # el bug original.
        self.assertFalse(self.fn({"status": "pending",
                                  "route": {"driver_name": "Rafael"}}))

    def test_los_estados_en_que_el_courier_ya_la_tomo_no_se_tocan(self):
        for estado in ("on_its_way", "completed", "failed", "partial", "canceled"):
            with self.subTest(estado=estado):
                self.assertFalse(self.fn({"status": estado, "route": None}))

    def test_mayusculas_y_espacios_no_cambian_el_veredicto(self):
        self.assertTrue(self.fn({"status": " PENDING "}))

    def test_una_respuesta_rara_no_revienta_ni_marca_congelada(self):
        # Si SimpliRoute devuelve algo inesperado, el lado seguro es NO
        # declararla congelada (no tocar nada).
        for basura in (None, [], "pending", 42):
            with self.subTest(v=basura):
                self.assertFalse(self.fn(basura))


# ══════════════════════════════════════════════════════════════════════
#  3. El listado de congeladas es SOLO LECTURA
# ══════════════════════════════════════════════════════════════════════
class TestListadoNoModificaNada(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.src = _cuerpo("tr_simpliroute_visitas_congeladas")

    def test_no_escribe_en_la_base(self):
        for prohibido in ("mysql_execute", "UPDATE ", "INSERT ", "DELETE "):
            self.assertNotIn(prohibido, self.src,
                             f"el listado no puede escribir ({prohibido})")

    def test_no_llama_a_simpliroute_con_metodos_de_escritura(self):
        for metodo in ("'PATCH'", "'POST'", "'DELETE'", "'PUT'"):
            self.assertNotIn(metodo, self.src,
                             f"el listado solo consulta, nunca {metodo}")

    def test_usa_el_criterio_compartido(self):
        self.assertIn("_sr_visita_sin_planificar(v)", self.src)

    def test_marca_cuales_quedaron_en_un_dia_vencido(self):
        self.assertIn("vencida", self.src)
        self.assertIn("dias_en_el_pasado", self.src)

    def test_un_courier_que_no_responde_no_rompe_la_lista(self):
        # Con 100 items, que uno falle no puede tumbar la revision completa.
        self.assertIn("sin_respuesta", self.src)


# ══════════════════════════════════════════════════════════════════════
#  4. El rescate: acotado y verificado ANTES de tocar
# ══════════════════════════════════════════════════════════════════════
class TestRescateEsConservador(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.src = _cuerpo("tr_simpliroute_rescatar_congeladas")

    def test_revalida_contra_la_api_antes_de_reprogramar(self):
        """Entre que se lista y que se aprieta el boton pueden pasar minutos.
        Si el despachador planifico la visita en ese rato, moverla seria
        sabotear su ruta."""
        self.assertIn("_sr_visita_sin_planificar(v)", self.src)
        # El GET de revalidacion tiene que ir ANTES del PATCH.
        self.assertLess(self.src.index("'GET'"), self.src.index("'PATCH'"))

    def test_exige_que_le_digan_cuales_rescatar(self):
        # Nunca "rescata todo lo que encuentre" por su cuenta.
        self.assertIn("item_ids", self.src)

    def test_no_reprograma_lo_que_ya_esta_para_hoy(self):
        self.assertIn("Ya está programada para hoy", self.src)

    def test_cada_rescate_queda_en_la_trazabilidad(self):
        self.assertIn("visita SimpliRoute rescatada", self.src)

    def test_reusa_el_mismo_patch_que_reprogramar(self):
        # No se reimplementa la llamada al courier.
        self.assertIn("EP_VISITS", self.src)
        self.assertIn('"planned_date": hoy_str', self.src.replace("'", '"'))

    def test_tiene_tope_de_cuantas_toca_de_una_vez(self):
        self.assertIn("[:100]", self.src)


# ══════════════════════════════════════════════════════════════════════
#  5. Lo que NO se rompio
# ══════════════════════════════════════════════════════════════════════
class TestNoSeRompioLoQueYaFuncionaba(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.src = _cuerpo("tr_manifiesto_subir_simpliroute")

    def test_sigue_siendo_idempotente(self):
        # Apretar dos veces no puede duplicar visitas.
        self.assertIn("simpliroute_visit_id", self.src)
        self.assertIn("ya_estaban", self.src)

    def test_sigue_el_fallback_uno_a_uno(self):
        self.assertIn("reintento individual", self.src)

    def test_sigue_marcando_entregado_a_transporte(self):
        self.assertIn("Entregado a transporte", self.src)

    def test_sigue_validando_courier_y_token(self):
        self.assertIn("_simpliroute_courier_integra", self.src)
        self.assertIn("_simpliroute_token_for_courier", self.src)

    def test_reprogramar_individual_sigue_existiendo(self):
        # REGLA #4.2: el endpoint nuevo se AGREGA, no reemplaza al que ya
        # estaba.
        self.assertIn("def tr_item_simpliroute_reprogramar", _fuente())


if __name__ == "__main__":
    unittest.main(verbosity=2)

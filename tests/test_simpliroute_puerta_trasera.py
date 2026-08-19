"""El bug de la fecha tenia DOS puertas y PR #162 cerro solo una.

2026-08-18, levantamiento posterior a PR #162. Daniel pregunto si el modulo
estaba lleno de parches. Revisando en serio aparecio esto:

  tr_manifiesto_subir_simpliroute  -> ya usaba HOY  (arreglado en PR #162)
  _tr_simpliroute_reenviar_item    -> seguia usando tm.fecha (LA DEL MANIFIESTO)

El segundo camino lo usan el boton "reenviar" y, peor, el REDESPACHO
AUTOMATICO: cuando una entrega falla, el sistema calcula el proximo dia
habil, se lo promete al cliente por escrito... y creaba la visita con la
fecha del manifiesto, o sea un dia ya cerrado en el tablero del courier.
ILUS le decia al cliente "llega el 19" y dejaba la visita en el 13, donde
no la ve nadie. Falla muda + promesa falsa al cliente.

Ademas se detecto que el modal ANUNCIABA la fecha del manifiesto ("visitas
para el 17/08") mientras el backend usaba hoy: si Alison seguia el texto,
buscaba en el dia equivocado, concluia que la subida fallo y volvia a subir
por Excel -- de ahi nacen las visitas duplicadas que el poller tiene que
desempatar despues.

Correr con:  py -m unittest tests.test_simpliroute_puerta_trasera
(pytest NO esta instalado en el equipo de Daniel.)
"""
import ast
import io
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


def _leer(rel):
    with io.open(os.path.join(RAIZ, rel), encoding="utf-8") as fh:
        return fh.read()


# ══════════════════════════════════════════════════════════════════════
#  1. Una sola fuente de verdad para la fecha
# ══════════════════════════════════════════════════════════════════════
class TestUnaSolaFuenteDeVerdad(unittest.TestCase):

    def test_existe_el_helper_compartido(self):
        self.assertIn("def _sr_planned_date_hoy", _fuente())

    def test_el_helper_usa_hora_chile(self):
        src = _cuerpo("_sr_planned_date_hoy")
        self.assertIn("_now_chile()", src)
        # UTC seria el mismo bug con otro disfraz: 21:30 en Chile ya es el
        # dia siguiente en UTC. REGLA #6.
        self.assertNotIn("utcnow", src)

    def test_la_subida_del_manifiesto_usa_el_helper(self):
        self.assertIn("planned_date = _sr_planned_date_hoy()",
                      _cuerpo("tr_manifiesto_subir_simpliroute"))

    def test_el_reenvio_usa_el_helper(self):
        self.assertIn("_sr_planned_date_hoy()",
                      _cuerpo("_tr_simpliroute_reenviar_item"))


# ══════════════════════════════════════════════════════════════════════
#  2. La puerta que quedaba abierta
# ══════════════════════════════════════════════════════════════════════
class TestElReenvioYaNoUsaLaFechaDelManifiesto(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.src = _cuerpo("_tr_simpliroute_reenviar_item")

    def test_ya_no_saca_planned_date_de_la_fila_del_manifiesto(self):
        """El bug exacto: planned_date salia de mi['fecha'] (tm.fecha).

        Si alguien restaura esa linea, cada reenvio y cada redespacho
        automatico vuelve a nacer en un dia cerrado.
        """
        self.assertNotIn("_fecha = mi.get('fecha')", self.src.replace('"', "'"))

    def test_acepta_una_fecha_explicita_del_caller(self):
        # La usa el redespacho automatico para calzar con lo prometido.
        self.assertIn("planned_date", ast.unparse(_nodo("_tr_simpliroute_reenviar_item").args))

    def test_sin_fecha_explicita_cae_en_hoy(self):
        plano = self.src.replace('"', "'")
        self.assertIn("planned_date = str(planned_date or '').strip()", plano)
        self.assertIn("or _sr_planned_date_hoy()", plano)


class TestElRedespachoProgramaLoQuePromete(unittest.TestCase):
    """El peor de los dos: le dice al cliente una fecha y usa otra."""

    @classmethod
    def setUpClass(cls):
        cls.src = _cuerpo("_tr_redespacho_automatico")

    def test_calcula_el_proximo_dia_habil_una_sola_vez(self):
        self.assertIn("fecha_reintento = _ra.proxima_fecha_habil(_now_chile())", self.src)

    def test_le_pasa_esa_misma_fecha_a_simpliroute(self):
        plano = self.src.replace('"', "'")
        self.assertIn("planned_date=fecha_reintento.isoformat()", plano)

    def test_el_mensaje_al_cliente_sale_de_la_misma_variable(self):
        # Si el texto y la visita se calculan por separado, vuelven a
        # divergir en cuanto alguien toque uno de los dos.
        self.assertIn("fecha_str = fecha_reintento.strftime('%d/%m/%Y')",
                      self.src.replace('"', "'"))


# ══════════════════════════════════════════════════════════════════════
#  3. La pantalla dice la verdad
# ══════════════════════════════════════════════════════════════════════
class TestElModalNoPrometeLaFechaDelManifiesto(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.html = _leer("templates/transporte/manifiesto_detalle.html")

    def test_ya_no_anuncia_visitas_para_la_fecha_del_manifiesto(self):
        self.assertNotIn("como visitas para el\n              <strong>{{ manifiesto.fecha",
                         self.html)

    def test_anuncia_hoy(self):
        self.assertIn("como visitas para <strong>HOY</strong>", self.html)

    def test_sigue_mostrando_la_fecha_del_manifiesto_como_contexto(self):
        # REGLA #4.2: el dato no se borra, cambia de rol -- Alison necesita
        # ver la diferencia para entender por que no coinciden.
        self.assertIn("El manifiesto es del", self.html)


class TestLosAvisosSePintan(unittest.TestCase):
    """PR #162 calculaba los avisos y el front no los leia: el backend
    avisaba al vacio. Esa es la definicion de falla muda."""

    @classmethod
    def setUpClass(cls):
        cls.js = _leer("static/transporte_manifiesto_detalle.js")

    def test_existe_el_pintador_de_avisos(self):
        self.assertIn("function _srAvisosHtml", self.js)

    def test_lee_el_campo_del_backend(self):
        self.assertIn("d.avisos", self.js)

    def test_se_pinta_tanto_en_exito_como_en_error_parcial(self):
        # 2 llamadas: rama verde y rama con errores.
        self.assertGreaterEqual(self.js.count("_srAvisosHtml(d)"), 2)

    def test_con_avisos_no_se_autorecarga(self):
        """Recargar a los 4,5 s se llevaria de pantalla justo el aviso."""
        self.assertIn("if (!(d.avisos && d.avisos.length))", self.js)

    def test_la_fecha_se_muestra_en_formato_chileno(self):
        # REGLA #6: nunca ISO crudo en pantalla.
        self.assertIn("function _srFechaCL", self.js)
        self.assertIn("_srFechaCL(d.fecha)", self.js)

    def test_el_formateador_no_usa_new_date(self):
        """new Date('2026-08-18') se interpreta como UTC y en Chile
        retrocede al 17 -- mostrariamos el dia equivocado."""
        i = self.js.index("function _srFechaCL")
        self.assertNotIn("new Date", self.js[i:i + 400])


# ══════════════════════════════════════════════════════════════════════
#  4. El panel de rescate: que no dependa de que alguien llame por API
# ══════════════════════════════════════════════════════════════════════
class TestPanelDeVisitasSinPlanificar(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.js   = _leer("static/transporte_visitas_congeladas.js")
        cls.html = _leer("templates/transporte/manifiestos.html")
        cls.css  = _leer("static/transporte_manifiestos.css")

    def test_el_boton_existe_en_la_pagina(self):
        self.assertIn('id="btnVisitasCongeladas"', self.html)

    def test_el_script_esta_cargado(self):
        self.assertIn("transporte_visitas_congeladas.js", self.html)

    def test_el_modal_existe(self):
        for id_ in ('id="vcModal"', 'id="vcCuerpo"', 'id="vcPie"',
                    'id="vcFecha"', 'id="vcBtnRescatar"'):
            with self.subTest(id=id_):
                self.assertIn(id_, self.html)

    def test_consulta_el_endpoint_de_solo_lectura_primero(self):
        self.assertIn("/transporte/api/simpliroute/visitas-congeladas", self.js)

    def test_manda_item_ids_y_fecha_al_rescatar(self):
        self.assertIn("/transporte/api/simpliroute/rescatar-congeladas", self.js)
        self.assertIn("item_ids: ids", self.js)
        self.assertIn("fecha: fecha", self.js)

    def test_pide_confirmacion_antes_de_tocar_al_courier(self):
        # Escribe en el tablero de un tercero: nunca de un solo clic.
        self.assertIn("await ilusConfirm", self.js)

    def test_no_usa_popups_nativos(self):
        """REGLA #1."""
        for prohibido in ("confirm(", "alert(", "prompt("):
            with self.subTest(p=prohibido):
                # ilusConfirm / ilusAlert contienen el substring: se miden
                # las llamadas NATIVAS, sin el prefijo ilus.
                self.assertNotIn(" " + prohibido, self.js.replace("ilus", "_"))

    def test_el_default_es_el_proximo_dia_habil(self):
        """Reprogramar a hoy a las 19:30 no sirve: el tablero ya cerro."""
        self.assertIn("function proximoHabil", self.js)
        self.assertIn("inp.value = proximoHabil()", self.js)

    def test_no_deja_elegir_el_pasado(self):
        self.assertIn("inp.min = hoyISO()", self.js)

    def test_las_fechas_se_muestran_dd_mm_aaaa(self):
        self.assertIn("function fechaCL", self.js)
        self.assertNotIn("new Date(iso", self.js)

    def test_escapa_lo_que_viene_del_backend(self):
        # Nombres de cliente y comunas entran al innerHTML.
        self.assertIn("function esc", self.js)
        self.assertIn("esc(c.cliente", self.js)

    def test_el_estado_sano_se_explica_en_vez_de_quedar_en_blanco(self):
        # Un panel vacio sin texto se lee como "no cargo".
        self.assertIn("Ninguna visita quedó atrapada", self.js)
        self.assertIn("Se revisaron ", self.js)

    def test_informa_las_que_no_respondieron(self):
        # Silencio != todo bien.
        self.assertIn("no respondieron a la consulta", self.js)

    def test_tiene_estilos_propios_y_responsivos(self):
        self.assertIn(".vc-header", self.css)
        self.assertIn("@media (max-width:575.98px)", self.css)


if __name__ == "__main__":
    unittest.main(verbosity=2)

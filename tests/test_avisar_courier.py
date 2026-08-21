"""Mandarle al courier el Excel que él SÍ trabaja — el canal que sí entrega.

2026-08-20. Alison llevaba semanas diciendo "aun no sube nada a transportes
felca". Medido contra la API real de SimpliRoute, cuenta de Felca, sobre los
días 13, 14, 19 y 20 de agosto:

    visitas creadas por ILUS   (reference "FCV-11303")  0 entregadas / 15 pend.
    visitas cargadas por Felca (reference "11281")      7 entregadas /  4 pend.

NINGUNA visita creada por ILUS se entregó jamás. Las entregas que el sistema
registraba como éxito eran del propio courier, adoptadas después por la
reconciliación.

La causa NO es el payload. Se descartaron con evidencia:
  · la conexión  -> GET /v1/accounts/me/ responde OK
  · la fecha     -> arreglada en PR #162-#166, y las rescatadas igual no salieron
  · las habilidades -> la flota de Rafael tiene skills:[]; las entregadas
                       tampoco declaraban ninguna
  · el peso      -> las entregadas iban con load_2:0

Es que la carga que mandamos por integración queda FUERA del flujo de trabajo
del courier: él trabaja desde el Excel que él mismo sube, y nadie le avisa que
llegaron visitas por API.

Este endpoint cierra ese hueco por el único camino con entregas comprobadas.

⚠️ DOS COSAS QUE NO DEBE HACER, y que estas pruebas vigilan:
  1. NUNCA declarar enviado un correo que no salió (la marcha blanca puede
     tener el canal cerrado).
  2. NUNCA mandar el WhatsApp solo. Se devuelve el link y una persona decide.

Correr con:  py -m unittest tests.test_avisar_courier -v
(pytest NO está instalado en el equipo de Daniel.)
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


def _ejecutable(nombre, ambito=None):
    ns = dict(ambito or {})
    exec(compile(ast.Module(body=[_nodo(nombre)], type_ignores=[]),
                 "<app.py>", "exec"), ns)
    return ns[nombre]


def _leer(rel):
    with io.open(os.path.join(RAIZ, rel), encoding="utf-8") as fh:
        return fh.read()


# ══════════════════════════════════════════════════════════════════════
#  1. El mensaje para el courier — función pura, se ejecuta de verdad
# ══════════════════════════════════════════════════════════════════════
class TestTextoParaElCourier(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # staticmethod: sin esto Python trata la funcion guardada como
        # atributo de clase como un METODO y le pasa self -> TypeError.
        cls.fn = staticmethod(_ejecutable(
            "_tr_texto_aviso_courier", {"ILUS_BRAND": "ILUS Fitness"}))

    def _texto(self, **over):
        base = dict(courier="Transporte Felca", correlativo="MAN-2026-0047",
                    n_docs=6, n_bultos=11, fecha_label="21/08/2026")
        base.update(over)
        return self.fn(**base)

    def test_dice_cuantos_pedidos_y_cuantos_bultos(self):
        t = self._texto()
        self.assertIn("6 pedido(s)", t)
        self.assertIn("11 bulto(s)", t)

    def test_dice_para_cuando(self):
        self.assertIn("21/08/2026", self._texto())

    def test_nombra_el_manifiesto(self):
        self.assertIn("MAN-2026-0047", self._texto())

    def test_saluda_al_contacto_si_lo_hay(self):
        self.assertIn("Hola Rafael", self._texto(contacto="Rafael"))

    def test_sin_contacto_no_deja_un_hola_vacio(self):
        t = self._texto(contacto=None)
        self.assertNotIn("Hola ,", t)
        self.assertNotIn("Hola None", t)

    def test_firma_con_la_marca_no_con_el_operador(self):
        """REGLA #11: los mensajes salen a nombre de ILUS Fitness."""
        self.assertIn("ILUS Fitness", self._texto())

    def test_la_fecha_va_en_formato_chileno(self):
        """REGLA #6: nunca ISO crudo delante de una persona."""
        t = self._texto(fecha_label="21/08/2026")
        self.assertNotIn("2026-08-21", t)

    def test_avisa_que_va_el_excel(self):
        # Es la instruccion concreta: el courier tiene que saber que revise
        # el correo, si no el mensaje no sirve de nada.
        self.assertIn("Excel", self._texto())


# ══════════════════════════════════════════════════════════════════════
#  2. El link de WhatsApp
# ══════════════════════════════════════════════════════════════════════
class TestLinkWhatsapp(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.fn = staticmethod(_ejecutable("_tr_link_whatsapp"))

    def test_celular_chileno_con_codigo_pais(self):
        u = self.fn("+56 9 8765 4321", "hola")
        self.assertTrue(u.startswith("https://wa.me/56987654321?text="))

    def test_celular_sin_codigo_pais_se_completa(self):
        u = self.fn("987654321", "hola")
        self.assertIn("wa.me/56987654321", u)

    def test_el_texto_va_escapado(self):
        u = self.fn("+56987654321", "hola mundo & cia")
        self.assertNotIn(" ", u)
        self.assertIn("%20", u)

    def test_sin_telefono_devuelve_None(self):
        for vacio in (None, "", "   ", "sin datos"):
            with self.subTest(v=vacio):
                self.assertIsNone(self.fn(vacio, "hola"))

    def test_telefono_demasiado_corto_devuelve_None(self):
        """Mejor no ofrecer el boton que abrir un WhatsApp a un numero que
        no existe -- Alison creeria que aviso y no aviso a nadie."""
        self.assertIsNone(self.fn("123", "hola"))


# ══════════════════════════════════════════════════════════════════════
#  3. El endpoint: honesto sobre lo que pasó
# ══════════════════════════════════════════════════════════════════════
class TestElEndpointEsHonesto(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.src = _cuerpo("tr_manifiesto_avisar_courier")
        cls.plano = cls.src.replace('"', "'")

    def test_nunca_dice_enviado_sin_verificar(self):
        """El historial de este modulo esta lleno de verdes falsos. El campo
        sale del resultado REAL de _send_ilus_email, no de haberlo intentado."""
        self.assertIn("resp['correo_enviado'] = bool(enviado)", self.plano)

    def test_si_el_correo_no_sale_lo_dice_y_ofrece_salida(self):
        self.assertIn("NO salió", self.src)
        self.assertIn("avisos", self.src)

    def test_sin_correo_del_courier_no_finge_que_mando(self):
        self.assertIn("no tiene correo guardado", self.src)

    def test_NO_manda_el_whatsapp_solo(self):
        """Se devuelve el link; mandar a nombre de la empresa a un tercero es
        una accion deliberada de una persona."""
        self.assertIn("link_whatsapp", self.src)
        for envio_directo in ("twilio", "wa_send", "enviar_whatsapp"):
            with self.subTest(x=envio_directo):
                self.assertNotIn(envio_directo, self.src.lower())

    def test_tiene_vista_previa_que_no_manda_nada(self):
        self.assertIn("solo_preview", self.src)
        self.assertIn("Vista previa: no se mandó ningún correo.", self.src)

    def test_reusa_el_MISMO_excel_que_el_courier_ya_trabaja(self):
        """No una variante nueva: el formato con entregas comprobadas."""
        self.assertIn("_tr_manifiesto_export_impl(mid, devolver_bytes=True)", self.src)

    def test_manda_el_excel_como_adjunto(self):
        self.assertIn("attachments", self.src)

    def test_respeta_el_branding_y_el_modulo(self):
        # REGLA #11 (asunto de marca) y la llave de paso por modulo.
        self.assertIn("_brand_subject", self.src)
        self.assertIn("'transporte'", self.plano)

    def test_deja_rastro_en_la_trazabilidad_solo_si_salio(self):
        i_log = self.src.index("_tr_log(")
        i_if = self.src.index("if enviado:")
        self.assertLess(i_if, i_log, "el log no puede quedar fuera del if")

    def test_no_manda_nada_si_el_manifiesto_esta_vacio(self):
        self.assertIn("no tiene pedidos que mandar", self.src)


# ══════════════════════════════════════════════════════════════════════
#  4. El Excel sigue funcionando como siempre
# ══════════════════════════════════════════════════════════════════════
class TestElExportNoSeRompio(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.src = _cuerpo("_tr_manifiesto_export_impl")

    def test_sin_el_parametro_sigue_devolviendo_el_archivo(self):
        """REGLA #4.2: el boton 'Exportar carga masiva' no se toca."""
        self.assertIn("return send_file(", self.src)

    def test_con_el_parametro_devuelve_bytes_y_nombre(self):
        self.assertIn("if devolver_bytes:", self.src)
        self.assertIn("return (buf.getvalue(), _fname)", self.src.replace("return buf.getvalue(), _fname",
                                                                          "return (buf.getvalue(), _fname)"))

    def test_el_formato_simplyroute_sigue_intacto(self):
        fuente = _fuente()
        self.assertIn('"Habilidades requeridas", "Habilidades KILOS"', fuente)


# ══════════════════════════════════════════════════════════════════════
#  5. La pantalla
# ══════════════════════════════════════════════════════════════════════
class TestLaPantalla(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.html = _leer("templates/transporte/manifiesto_detalle.html")
        cls.js = _leer("static/transporte_manifiesto_detalle.js")

    def test_el_boton_existe(self):
        self.assertIn('id="btnAvisarCourier"', self.html)

    def test_el_boton_nombra_al_courier(self):
        self.assertIn("Mandar la carga a {{ manifiesto.courier }}", self.html)

    def test_los_botones_que_ya_estaban_siguen(self):
        """REGLA #4.2."""
        self.assertIn("Exportar carga masiva", self.html)
        self.assertIn("btnSubirSR", self.html)

    def test_pide_confirmacion_antes_de_mandar(self):
        self.assertIn("await ilusConfirm", self.js)

    def test_no_usa_popups_nativos(self):
        """REGLA #1. Se miran solo las lineas de CODIGO: los comentarios del
        archivo mencionan prompt()/confirm() justamente para prohibirlos."""
        codigo = []
        for linea in self.js.split("\n"):
            l = linea.strip()
            if l.startswith("//") or l.startswith("*") or l.startswith("/*"):
                continue
            codigo.append(linea)
        limpio = "\n".join(codigo)
        for ilus in ("ilusConfirm", "ilusAlert", "ilusPrompt", "ilusToast"):
            limpio = limpio.replace(ilus, "_")
        for prohibido in (" confirm(", " alert(", " prompt(",
                          "window.confirm(", "window.prompt("):
            with self.subTest(p=prohibido):
                self.assertNotIn(prohibido, limpio)

    def test_hace_UNA_sola_llamada_no_reintenta(self):
        """Un reintento con otra URL podria mandarle el correo DOS VECES al
        courier. Una version anterior de _acPedir lo hacia."""
        i = self.js.index("async function _acPedir")
        cuerpo = self.js[i:i + 1200]
        self.assertEqual(cuerpo.count("await fetch("), 1)

    def test_muestra_la_vista_previa_antes_de_mandar(self):
        self.assertIn("_acPedir(true)", self.js)


if __name__ == "__main__":
    unittest.main(verbosity=2)

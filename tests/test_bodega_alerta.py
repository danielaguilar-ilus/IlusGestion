"""Aviso cuando una factura NO sale de la bodega principal (02).

Pedido de Daniel el 2026-08-17, textual:
  "necesito que me avisaras cuando una factura cuente con una bodega distinta
   a la bodega 02, que le avise al usuario para que este sepa que no debe
   avanzar sacando el producto de la bodega 02 siendo que tiene que sacar lo
   de una bodega distinta a la 02... te coloco un ejemplo, la factura 11163,
   el equipo BBP sale de la bodega 06... todas las ventas salen de la bodega
   02 pero existen bodegas de liquidacion 15, Incidencias 13, Motion Vitacura
   06 y Motion la dehesa 05, Repuestos 18... que el usuario sepa que hay una
   excepcion ya que el 98% sale de la 02".

Caso REAL verificado contra produccion antes de escribir el codigo:
FCV 11163 devuelve bodegas="02,06" y bodega_alerta=true. O sea, el dato ya se
estaba capturando; lo que faltaba era mostrarlo con NOMBRE y llevarlo a las
pantallas donde se decide el despacho.

Correr con:  py -m unittest tests.test_bodega_alerta
(pytest NO esta instalado en el equipo de Daniel.)
"""
import ast
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
APP_PY = os.path.join(RAIZ, "app.py")
JS_ASIGNAR = os.path.join(RAIZ, "static", "cubicador_asignar.js")
JS_MONITOR = os.path.join(RAIZ, "static", "transporte_monitor.js")
CSS_ASIGNAR = os.path.join(RAIZ, "static", "cubicador_asignar.css")


def _leer(path):
    with open(path, encoding="utf-8") as fh:
        return fh.read()


def _cargar_helpers_de_bodega(bodega_principal="2"):
    """Extrae de app.py el diccionario + los helpers de bodega y los ejecuta.

    Se prueba el COMPORTAMIENTO real, no que ciertas palabras aparezcan en el
    codigo: una prueba que busca texto pasa en verde aunque la logica este
    mal. Importar app.py entero no es opcion (levanta Flask, la base y los
    hilos de cron), asi que se extraen solo estas funciones, que son puras.
    """
    arbol = ast.parse(_leer(APP_PY))
    ambito = {"os": os, "TR_BODEGA_PRINCIPAL": bodega_principal}
    nombres = ("_tr_bodega_norm", "_tr_bodega_nombre",
               "_tr_bodega_label", "_tr_bodegas_analizar")
    encontrados = set()
    for nodo in arbol.body:
        if (isinstance(nodo, ast.Assign) and nodo.targets
                and getattr(nodo.targets[0], "id", "") == "TR_BODEGAS_NOMBRES"):
            exec(compile(ast.Module(body=[nodo], type_ignores=[]), "<app>", "exec"), ambito)
            encontrados.add("TR_BODEGAS_NOMBRES")
        elif isinstance(nodo, ast.FunctionDef) and nodo.name in nombres:
            exec(compile(ast.Module(body=[nodo], type_ignores=[]), "<app>", "exec"), ambito)
            encontrados.add(nodo.name)
    faltan = (set(nombres) | {"TR_BODEGAS_NOMBRES"}) - encontrados
    assert not faltan, f"no se encontraron en app.py: {sorted(faltan)}"
    return ambito


# ══════════════════════════════════════════════════════════════════════
#  1. Las bodegas que nombro Daniel
# ══════════════════════════════════════════════════════════════════════
class TestDiccionarioDeBodegas(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.ns = _cargar_helpers_de_bodega()

    def test_estan_las_bodegas_que_nombro_daniel(self):
        nombre = self.ns["_tr_bodega_nombre"]
        self.assertEqual(nombre("05"), "Motion La Dehesa")
        self.assertEqual(nombre("06"), "Motion Vitacura")
        self.assertEqual(nombre("13"), "Incidencias")
        self.assertEqual(nombre("15"), "Liquidación")
        self.assertEqual(nombre("18"), "Repuestos")
        # Agregada a pedido de Daniel el 2026-08-17: "considera la bodega 12
        # del gimnasio ILUS para que se incluya como alerta".
        self.assertEqual(nombre("12"), "Gimnasio ILUS")

    def test_el_cero_a_la_izquierda_no_cambia_nada(self):
        # El ERP devuelve "06"; una persona escribiria "6". Las dos formas
        # tienen que resolver a la misma bodega, o la advertencia se dispara
        # (o se pierde) por un detalle de formato. Bug real del 2026-08-02.
        nombre = self.ns["_tr_bodega_nombre"]
        self.assertEqual(nombre("06"), nombre("6"))
        self.assertEqual(nombre("  06 "), "Motion Vitacura")

    def test_una_bodega_desconocida_no_rompe_nada(self):
        # El ERP puede crear una bodega nueva sin avisar. Que no tenga nombre
        # no puede dejar la pantalla en blanco ni inventarle uno.
        self.assertEqual(self.ns["_tr_bodega_nombre"]("99"), "")
        self.assertEqual(self.ns["_tr_bodega_label"]("99"), "Bodega 99")

    def test_la_etiqueta_conserva_el_codigo_tal_como_vino(self):
        # El numero es lo que la persona va a buscar en Random: el nombre
        # ayuda, no reemplaza.
        self.assertEqual(self.ns["_tr_bodega_label"]("06"),
                         "Bodega 06 · Motion Vitacura")


# ══════════════════════════════════════════════════════════════════════
#  2. La regla: avisar solo cuando NO es la principal
# ══════════════════════════════════════════════════════════════════════
class TestCuandoSeAvisa(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # staticmethod: sin esto Python trata la funcion guardada como
        # atributo de clase como un METODO y le pasa self como primer
        # argumento -> TypeError en cada llamada.
        cls.analizar = staticmethod(
            _cargar_helpers_de_bodega()["_tr_bodegas_analizar"])

    def test_caso_real_de_daniel_fcv_11163(self):
        # Verificado en produccion: bodegas="02,06" (el equipo BBP sale de la 06).
        r = self.analizar("02,06")
        self.assertTrue(r["alerta"])
        self.assertEqual(r["resumen"], "Motion Vitacura")
        self.assertIn("Motion Vitacura", r["mensaje"])
        # Solo la 06 es excepcion; la 02 no se reporta como problema.
        self.assertEqual([e["codigo"] for e in r["excepciones"]], ["06"])

    def test_todo_de_la_bodega_principal_no_avisa(self):
        # El 98% de los casos. Si esto avisara, la advertencia perderia
        # sentido por ruido y la gente dejaria de mirarla.
        r = self.analizar("02")
        self.assertFalse(r["alerta"])
        self.assertEqual(r["resumen"], "")
        self.assertEqual(r["mensaje"], "")
        self.assertEqual(r["excepciones"], [])

    def test_varias_bodegas_distintas_se_listan_todas(self):
        r = self.analizar("02,13,15")
        self.assertTrue(r["alerta"])
        self.assertIn("Incidencias", r["resumen"])
        self.assertIn("Liquidación", r["resumen"])

    def test_documento_entero_fuera_de_la_principal(self):
        # Sin nada de la 02: igual avisa (de hecho es el caso mas claro).
        r = self.analizar("18")
        self.assertTrue(r["alerta"])
        self.assertEqual(r["resumen"], "Repuestos")

    def test_el_gimnasio_ilus_bodega_12_tambien_avisa(self):
        # Pedido de Daniel el 2026-08-17. Es ademas el caso que origino toda
        # esta advertencia el 2026-08-02 (FCV 0000011149 salio de la 12).
        r = self.analizar("02,12")
        self.assertTrue(r["alerta"])
        self.assertEqual(r["resumen"], "Gimnasio ILUS")
        self.assertIn("Bodega 12 · Gimnasio ILUS", r["mensaje"])

    def test_todas_las_bodegas_conocidas_avisan_salvo_la_principal(self):
        # Barrido completo: ninguna bodega del diccionario puede quedarse sin
        # avisar por un descuido al agregarla.
        for cod in ("05", "06", "12", "13", "15", "18"):
            with self.subTest(bodega=cod):
                self.assertTrue(self.analizar(cod)["alerta"],
                                f"la bodega {cod} deberia avisar")
        self.assertFalse(self.analizar("02")["alerta"])

    def test_bodega_repetida_no_se_duplica_en_el_aviso(self):
        r = self.analizar("06,06,02")
        self.assertEqual(len(r["excepciones"]), 1)

    def test_acepta_lista_ademas_de_texto(self):
        # El Monitor entrega "02,06" (columna de la base); Asignar y Cotizar
        # entrega una lista de bodegas por linea. Ambos usan el mismo helper.
        self.assertEqual(self.analizar(["02", "06"])["resumen"],
                         self.analizar("02,06")["resumen"])

    def test_sin_datos_nunca_avisa_ni_revienta(self):
        # Un documento sin lineas de producto, o el ERP sin devolver bodega:
        # el lado seguro es NO avisar (no inventar un problema).
        for vacio in ("", None, [], [""], [None]):
            r = self.analizar(vacio)
            self.assertFalse(r["alerta"], f"no deberia avisar con {vacio!r}")
            self.assertEqual(r["mensaje"], "")

    def test_el_mensaje_le_dice_a_la_persona_que_hacer(self):
        # No basta con "hay una excepcion": tiene que decir que NO saque de
        # la 02, que es exactamente el error que Daniel quiere evitar.
        m = self.analizar("02,06")["mensaje"]
        self.assertIn("02", m)
        self.assertIn("No lo saques", m)

    def test_la_bodega_principal_es_configurable(self):
        # Si la empresa cambia de bodega principal, se cambia la variable de
        # entorno y la regla sigue valiendo -- sin tocar codigo.
        analizar_12 = _cargar_helpers_de_bodega("12")["_tr_bodegas_analizar"]
        self.assertTrue(analizar_12("02")["alerta"])
        self.assertFalse(analizar_12("12")["alerta"])


# ══════════════════════════════════════════════════════════════════════
#  3. Que el dato llegue a las pantallas
# ══════════════════════════════════════════════════════════════════════
class TestLlegaALasPantallas(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.app = _leer(APP_PY)
        cls.js_asignar = _leer(JS_ASIGNAR)
        cls.js_monitor = _leer(JS_MONITOR)
        cls.css = _leer(CSS_ASIGNAR)

    def test_el_erp_devuelve_la_bodega_por_linea(self):
        # Sin BOSULIDO en la consulta no hay forma de saber QUE equipo sale
        # de otra bodega, solo que el documento tiene algo raro.
        self.assertIn("BOSULIDO", self.app)
        self.assertIn('AS BOSULIDO', self.app)

    def test_asignar_y_cotizar_recibe_el_resumen_del_documento(self):
        self.assertIn('"bodegas": _tr_bodegas_analizar(', self.app)

    def test_asignar_y_cotizar_recibe_la_bodega_de_cada_producto(self):
        self.assertIn('"bodega_nombre":   _tr_bodega_nombre(', self.app)

    def test_el_monitor_recibe_los_nombres(self):
        self.assertIn('"bodegas_info": _tr_bodegas_analizar(', self.app)

    def test_no_se_quito_la_bandera_vieja_del_monitor(self):
        # REGLA #4.2: el badge que ya existia sigue funcionando; los nombres
        # se AGREGAN al lado.
        self.assertIn('"bodega_alerta": bool(_bodegas_doc', self.app)
        self.assertIn('"bodegas":      r.get("bodegas")', self.app)

    def test_la_pantalla_de_cotizar_pinta_el_aviso(self):
        self.assertIn("db-bodega-alert", self.js_asignar)
        self.assertIn("bod.alerta", self.js_asignar)

    def test_el_aviso_tiene_estilo_propio(self):
        for clase in (".db-bodega-alert", ".dba-chip", ".dba-chip-num", ".dba-title"):
            self.assertIn(clase, self.css, f"falta el estilo {clase}")

    def test_el_monitor_muestra_el_nombre_no_solo_el_numero(self):
        self.assertIn("bodegas_info", self.js_monitor)
        self.assertIn("info.resumen", self.js_monitor)

    def test_el_monitor_sigue_funcionando_si_no_vienen_los_nombres(self):
        # Una respuesta cacheada de antes de este cambio no trae bodegas_info:
        # tiene que caer al numero, no quedar en blanco.
        self.assertIn("'BOD ' + (bods", self.js_monitor)

    def test_el_aviso_escapa_lo_que_pinta(self):
        # El nombre sale de un diccionario nuestro, pero el codigo viene del
        # ERP: no se interpola crudo en el HTML.
        self.assertIn("attr(titulo)", self.js_monitor)
        self.assertIn("escHtml(e.codigo)", self.js_asignar)


if __name__ == "__main__":
    unittest.main(verbosity=2)

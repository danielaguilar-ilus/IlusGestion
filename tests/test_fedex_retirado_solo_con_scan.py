"""FedEx: «Retirado por el courier» solo con el primer movimiento REAL de FedEx.

2026-10-22. Daniel, con BLV 23252: «cuando estoy subiendo información de un
manifiesto a FedEx... me dice que ya está entregado, corrige eso... cuando
FedEx actúe es que ahí ya empezó que lo tiene el transportista».

Antes, crear la OT/etiqueta marcaba el envío «Entregado a transporte»
(«Retirado» en pantalla) y el código FedEx OC (orden creada) también. Ahora la
etiqueta deja el envío «En preparación»; «Retirado» llega con PU/AR/AF y el
aviso al cliente lo manda el poller en ese momento.

Correr con:  py -m unittest tests.test_fedex_retirado_solo_con_scan -v
"""
import os
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
with open(os.path.join(RAIZ, "app.py"), encoding="utf-8") as _f:
    APP = _f.read()


def _bloque(inicio, fin):
    i = APP.index(inicio)
    return APP[i:APP.index(fin, i)]


def _estado_fn():
    mapa = _bloque("_FEDEX_STATUS_MAP = {", "\n}\n") + "\n}\n"
    fn = _bloque("def _fedex_estado_a_ilus(", "\n\n\n# ═")
    ns = {}
    exec(mapa + "\n" + fn, ns)
    return ns["_fedex_estado_a_ilus"]


estado = _estado_fn()


class TestElEstadoSoloAvanzaConMovimientoReal(unittest.TestCase):
    def test_orden_creada_no_es_retirado(self):
        self.assertEqual(estado("OC"), "En preparación")

    def test_etiqueta_creada_en_texto_no_es_retirado(self):
        for d in ("Etiqueta creada", "Información de envío enviada a FedEx",
                  "Label created", "Shipment information sent to FedEx"):
            with self.subTest(d=d):
                self.assertEqual(estado("ZZ", d), "En preparación")

    def test_el_primer_movimiento_real_si_es_retirado(self):
        for c in ("PU", "AR", "AF"):
            with self.subTest(c=c):
                self.assertEqual(estado(c), "Entregado a transporte")
        self.assertEqual(estado("ZZ", "Recolectado"), "Entregado a transporte")
        self.assertEqual(estado("ZZ", "Picked up"), "Entregado a transporte")

    def test_lo_demas_no_cambia(self):
        self.assertEqual(estado("IT"), "En ruta")
        self.assertEqual(estado("DL"), "Entregado")
        self.assertEqual(estado("DE"), "Entrega fallida")


class TestCrearLaOtNoMarcaRetirado(unittest.TestCase):
    def test_ya_no_se_marca_al_crear_la_ot_unitaria_ni_masiva(self):
        self.assertNotIn("comentario='Etiqueta FedEx generada'", APP)
        self.assertNotIn("comentario='Etiqueta FedEx generada (masivo)'", APP)

    def test_el_registro_de_la_ot_creada_sigue(self):
        self.assertIn('"OT FedEx creada"', APP)
        self.assertIn('"OT FedEx creada (masivo)"', APP)


class TestNingunPollerDevuelveAEnPreparacion(unittest.TestCase):
    def test_guarda_en_el_choke_point(self):
        fn = _bloque("def _tr_apply_carrier_status(", "\ndef _tr_redespacho_automatico(")
        i = fn.index("sin_retroceso")
        previo = fn[max(0, i - 900):i]
        self.assertIn("FUENTES_AUTOMATICAS", previo)
        self.assertIn("estado_ilus == 'En preparación'", previo)
        # La guarda va ANTES del UPDATE de estado_entrega.
        self.assertLess(fn.index("sin_retroceso"),
                        fn.index("UPDATE transport_manifest_items SET estado_entrega"))

    def test_poller_de_fedex_sigue_avisando_al_cliente_en_el_avance_real(self):
        fn = _bloque("def _fedex_poll_batch(", '@app.route("/transporte/cron/fedex-track-poll"')
        self.assertIn("notify_cliente=True", fn)


if __name__ == "__main__":
    unittest.main(verbosity=2)

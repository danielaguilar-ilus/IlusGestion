"""🚦 Aviso al elegir un documento para una OT (Daniel, 12-oct-2026).

Daniel: «La OT le debe avisar al usuario, ya que si no sabe puede duplicar el servicio con un documento con el saldo ya
consumido» y «tienes que ser inteligente de reconocer cuando una nota de venta llega a esta [factura] y cuando esa nota
de venta hereda el saldo a la factura o a la boleta».

Qué se vigila:
  · El texto en palabras y el semáforo (consumido / parcial / disponible / doble cobro / sin servicio / sin leer).
  · La relación nota de venta ↔ factura o boleta: «esta nota de venta ya se facturó en…, usa la factura» y «esta
    factura viene de la nota de venta…, que ya usó la OT…».
  · Que el aviso salga donde se elige un documento: el asistente de crear OT (con un aviso que hay que cerrar) y
    «Agregar documento» de la ficha (confirmar antes de agregar).
Sin BD, sin Flask, sin ERP.
Correr con:  py -m unittest tests.test_ot_aviso_documento
"""
import unittest

import ot_saldo_servicio as S
from tests.test_ot_saldo_servicio import _fuente, _leer

SERV = lambda total, usado: {"total": total, "usado": usado, "saldo": max(0, total - usado), "hay_lineas": total > 0,
                             "excedido": total > 0 and usado > total}
NADA = {"total": 0, "usado": 0, "saldo": 0, "hay_lineas": False, "excedido": False}
OT1 = {"vid": 1, "numero_ot": "OT-2026-00312", "cliente": "Gimnasio de ejemplo", "servicio": 90000,
       "documentos": [["FCV", "11439"]]}


class TestSemaforo(unittest.TestCase):
    def test_disponible(self):
        a = S.aviso_documento(("FCV", "11439"), "FCV 11439", SERV(100000, 0), SERV(30000, 0), [], [])
        self.assertEqual(a["semaforo"]["estado"], "disponible")
        self.assertEqual(a["semaforo"]["nivel"], "verde")
        self.assertIn("$100.000 de instalación o servicio", a["semaforo"]["texto"])
        self.assertIn("$30.000 de despacho", a["semaforo"]["texto"])
        self.assertFalse(a["alerta"])

    def test_parcial_dice_quien_y_cuanto_queda(self):
        a = S.aviso_documento(("FCV", "11439"), "FCV 11439", SERV(100000, 90000), NADA, [OT1], [])
        self.assertEqual(a["semaforo"]["estado"], "parcial")
        self.assertIn("OT-2026-00312 (Gimnasio de ejemplo)", a["semaforo"]["texto"])
        self.assertIn("Quedan $10.000", a["semaforo"]["texto"])
        self.assertFalse(a["alerta"])

    def test_consumido_avisa_que_se_cobraria_dos_veces(self):
        a = S.aviso_documento(("FCV", "11439"), "FCV 11439", SERV(100000, 100000), NADA, [OT1], [])
        self.assertEqual(a["semaforo"]["estado"], "consumido")
        self.assertEqual(a["semaforo"]["nivel"], "rojo")
        self.assertIn("cobrando dos veces el mismo servicio", a["semaforo"]["texto"])
        self.assertTrue(a["alerta"])

    def test_doble_cobro(self):
        a = S.aviso_documento(("FCV", "1"), "FCV 1", SERV(100000, 150000), NADA, [OT1], [])
        self.assertEqual(a["semaforo"]["estado"], "doble")
        self.assertTrue(a["alerta"])

    def test_sin_servicio_y_sin_leer(self):
        self.assertEqual(S.aviso_documento(("FCV", "1"), "FCV 1", NADA, NADA, [], [])["semaforo"]["estado"], "sin_lineas")
        self.assertEqual(S.aviso_documento(("FCV", "1"), "FCV 1", NADA, NADA, [], [], leido=False)["semaforo"]["estado"],
                         "sin_leer")


class TestNotaDeVentaYFactura(unittest.TestCase):
    def test_nota_de_venta_ya_facturada_dice_que_use_la_factura(self):
        a = S.aviso_documento(("VD", "10667"), "NVV VD 10667", SERV(100000, 0), NADA, [],
                              [(("FCV", "11439"), "FCV 11439")])
        self.assertTrue(any("ya se facturó en FCV 11439" in x["texto"] and "Usa la factura" in x["texto"]
                            for x in a["avisos"]))
        self.assertTrue(a["alerta"])           # interrumpe aunque tenga saldo: hay que usar la factura

    def test_nota_que_termino_en_boleta(self):
        a = S.aviso_documento(("VD", "10667"), "NVV VD 10667", SERV(100000, 0), NADA, [],
                              [(("BLV", "23375"), "BLV 23375")])
        self.assertIn("Usa la boleta", a["avisos"][0]["texto"])

    def test_factura_que_viene_de_una_nota_ya_usada_hereda_el_consumo(self):
        via_nota = dict(OT1, documentos=[["VD", "10667"]])
        a = S.aviso_documento(("FCV", "11439"), "FCV 11439", SERV(100000, 90000), NADA, [via_nota],
                              [(("VD", "10667"), "NVV VD 10667")])
        t = a["avisos"][0]["texto"]
        self.assertIn("viene de la nota de venta NVV VD 10667", t)
        self.assertIn("OT-2026-00312 (Gimnasio de ejemplo) ya la usó con la nota de venta", t)
        self.assertEqual(a["avisos"][0]["nivel"], "ambar")

    def test_factura_que_viene_de_una_nota_sin_usar_es_informativa(self):
        a = S.aviso_documento(("FCV", "11439"), "FCV 11439", SERV(100000, 0), NADA, [],
                              [(("VD", "10667"), "NVV VD 10667")])
        self.assertEqual(a["avisos"][0]["nivel"], "info")
        self.assertFalse(a["alerta"])


class TestCableado(unittest.TestCase):
    def test_el_servidor_entrega_el_aviso(self):
        self.assertIn('"aviso": _ot_saldo_aviso(s)', _fuente("ot_api_saldo_servicio_doc"))
        self.assertIn('if tido == "NVV" and nudo.isdigit():', _fuente("ot_api_saldo_servicio_doc"))
        self.assertIn('"aviso": _ot_saldo_aviso(s)', _fuente("_ot_saldo_resumen"))
        self.assertIn("_saldo.aviso_documento(", _fuente("_ot_saldo_aviso"))

    def test_el_asistente_avisa_y_hay_que_cerrar_el_aviso(self):
        h = _leer("templates/ot2/_modal_crear.html")
        self.assertIn("_o2mFinAvisarDocumentos(lista);", h)
        self.assertIn("window.ilusAlert({", h)
        self.assertIn("d.aviso.alerta && !S.fin_saldo_avisados[k]", h)   # una vez por documento
        self.assertIn("_o2mFinSemHtml(d.aviso)", h)
        self.assertNotRegex(h[h.index("function _o2mFinAvisarDocumentos"):h.index("function _o2mFinSemHtml")],
                            r"\b(alert|confirm|prompt)\(")         # REGLA #1

    def test_agregar_documento_en_la_ficha_revisa_antes(self):
        js = _leer("static/ot_fin_motor.js")
        self.assertIn("function revisarAntesDeAgregar(inst, tipo, numero)", js)
        self.assertIn("revisarAntesDeAgregar(inst, v.tipo, v.numero).then(function (seguir)", js)
        self.assertIn("okLabel: 'Agregar de todas formas', cancelLabel: 'No agregar'", js)


if __name__ == "__main__":
    unittest.main()

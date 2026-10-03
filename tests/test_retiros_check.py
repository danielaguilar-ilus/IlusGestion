"""Preparación de un retiro según Check (funciones puras de retiros_check.py).

Correr con:  py -m pytest tests/test_retiros_check.py -q
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import retiros_check as rc


def fila(**kw):
    base = {"solicitado": "0", "noAsignado": "0", "asignado": "0", "pickeado": "0", "revisado": "0",
            "despachado": "0", "cancelado": "0", "devuelto": "0"}
    base.update({k: str(v) for k, v in kw.items()})
    return base


def doc(*filas):
    return rc.resumir_filas(list(filas))


class InterpretarCheck(unittest.TestCase):
    def test_muestra_real_blv_23732_aun_sin_empezar(self):
        # Respuesta real de Check (2026-10-02): pedido cargado, nada asignado ni pickeado.
        r = rc.evaluar([doc(fila(solicitado=1, noAsignado=1))])
        self.assertEqual(r["estado"], "en_proceso")
        self.assertFalse(r["listo"])
        self.assertEqual(r["pedidas"], 1)
        self.assertEqual(r["faltan"], 1)
        self.assertEqual([e["completa"] for e in r["etapas"]], [True, False, False, False, False])
        self.assertIn("todavía no empieza", r["frase"])

    def test_todo_pickeado_es_listo(self):
        r = rc.evaluar([doc(fila(solicitado=2, pickeado=2))])
        self.assertTrue(r["listo"])
        self.assertEqual(r["estado"], "listo")
        self.assertEqual(r["faltan"], 0)

    def test_parcial_no_es_listo_y_dice_cuanto_falta(self):
        r = rc.evaluar([doc(fila(solicitado=3, asignado=1, pickeado=2))])
        self.assertFalse(r["listo"])
        self.assertEqual(r["faltan"], 1)
        self.assertIn("faltan 1 de 3", r["frase"])

    def test_revisado_y_despachado_cuentan_como_ya_pickeados(self):
        r = rc.evaluar([doc(fila(solicitado=2, pickeado=1, revisado=1))])
        self.assertTrue(r["listo"])
        self.assertEqual(r["etapas"][3]["hechas"], 1)    # revisión final: 1 de 2
        self.assertFalse(r["etapas"][3]["completa"])
        # Despachado: se MUESTRA como listo pero NO se marca solo (puede haberse entregado antes).
        r2 = rc.evaluar([doc(fila(solicitado=2, despachado=2))])
        self.assertTrue(r2["listo"])
        self.assertFalse(r2["listo_auto"])
        self.assertIn("DESPACHADAS", r2["alerta"])
        self.assertTrue(all(e["completa"] for e in r2["etapas"]))
        self.assertTrue(rc.evaluar([doc(fila(solicitado=2, pickeado=1, revisado=1))])["listo_auto"])

    def test_las_canceladas_no_se_esperan(self):
        r = rc.evaluar([doc(fila(solicitado=3, cancelado=1, pickeado=2))])
        self.assertTrue(r["listo"])
        self.assertEqual(r["pedidas"], 2)

    def test_columnas_acumulativas_nunca_dan_falso_listo(self):
        # asignado 2 + pickeado 1 != solicitado 2: no son cubetas exclusivas → se toma el máximo
        r = rc.evaluar([doc(fila(solicitado=2, asignado=2, pickeado=1))])
        self.assertFalse(r["listo"])
        r2 = rc.evaluar([doc(fila(solicitado=2, asignado=2, pickeado=2, revisado=2))])
        self.assertTrue(r2["listo"])

    def test_varias_lineas_se_suman(self):
        r = rc.evaluar([doc(fila(solicitado=1, pickeado=1), fila(solicitado=4, noAsignado=4))])
        self.assertFalse(r["listo"])
        self.assertEqual(r["pedidas"], 5)
        self.assertEqual(r["faltan"], 4)

    def test_varios_documentos_exigen_todos(self):
        completo = doc(fila(solicitado=1, pickeado=1))
        r = rc.evaluar([completo, None])
        self.assertFalse(r["listo"])
        self.assertIn("falta el resto", r["frase"])
        r2 = rc.evaluar([completo, doc(fila(solicitado=2, pickeado=1, asignado=1))])
        self.assertFalse(r2["listo"])
        r3 = rc.evaluar([completo, doc(fila(solicitado=2, pickeado=2))])
        self.assertTrue(r3["listo"])

    def test_sin_datos_y_sin_documentos(self):
        self.assertEqual(rc.evaluar([None])["estado"], "sin_datos")
        self.assertEqual(rc.evaluar([])["estado"], "sin_documento")
        self.assertFalse(rc.evaluar([None])["listo"])

    def test_numeros_como_texto_con_coma_o_vacios(self):
        r = rc.evaluar([doc({"solicitado": "1,5", "pickeado": "1,5", "asignado": "", "noAsignado": None})])
        self.assertTrue(r["listo"])
        self.assertEqual(r["pedidas"], 1.5)

    def test_pedido_en_cero_no_es_listo(self):
        self.assertFalse(rc.evaluar([doc(fila(solicitado=0))])["listo"])
        self.assertFalse(rc.evaluar([doc(fila(solicitado=2, cancelado=2))])["listo"])

    def test_una_linea_sobrepickeada_no_compensa_a_otra_sin_empezar(self):
        r = rc.evaluar([doc(fila(solicitado=1, pickeado=2), fila(solicitado=1, noAsignado=1))])
        self.assertFalse(r["listo"])
        self.assertEqual(r["faltan"], 1)

    def test_numeros_raros_nunca_dan_listo(self):
        for malo in ("inf", "1e999", "nan", "-3", "abc"):
            r = rc.evaluar([doc(fila(solicitado=1, pickeado=malo))])
            self.assertFalse(r["listo"], malo)
            self.assertFalse(r["listo_auto"], malo)
        r = rc.evaluar([doc(fila(solicitado="nan", pickeado=1))])     # antes reventaba con ValueError
        self.assertFalse(r["listo"])

    def test_filas_de_otro_documento_se_descartan(self):
        ajena = dict(fila(solicitado=1, pickeado=1), tipoDocumento="BLV", numeroDocumento="0000999999")
        propia = dict(fila(solicitado=1, noAsignado=1), tipoDocumento="BLV", numeroDocumento="0000023732")
        res = rc.resumir_filas([ajena, propia], tipo="BLV", num="23732")
        self.assertEqual(len(res["lineas"]), 1)
        self.assertFalse(rc.evaluar([res])["listo"])
        self.assertIsNone(rc.resumir_filas([ajena], tipo="BLV", num="23732"))
        otro_tipo = dict(fila(solicitado=1, pickeado=1), tipoDocumento="FCV", numeroDocumento="23732")
        self.assertIsNone(rc.resumir_filas([otro_tipo], tipo="BLV", num="23732"))

    def test_documento_cancelado_no_bloquea_ni_da_listo_solo(self):
        r = rc.evaluar([doc(fila(solicitado=1, pickeado=1)), doc(fila(solicitado=2, cancelado=2))])
        self.assertTrue(r["listo"])
        r2 = rc.evaluar([doc(fila(solicitado=2, cancelado=2))])
        self.assertFalse(r2["listo"])
        self.assertIn("cancelado", r2["frase"])

    def test_resumir_sin_filas(self):
        self.assertIsNone(rc.resumir_filas([]))
        self.assertIsNone(rc.resumir_filas(None))
        self.assertIsNone(rc.resumir_filas(["basura", 3]))


if __name__ == "__main__":
    unittest.main()


class BodegaYaEmpezo(unittest.TestCase):
    """«iniciada_auto»: la señal con la que ILUS pasa solo un retiro a «En preparación» (Daniel 2026-10-02)."""

    def test_sin_nada_pickeado_no_empezo(self):
        r = rc.evaluar([doc(fila(solicitado=3, asignado=3))])           # stock reservado, nadie ha juntado nada
        self.assertFalse(r["iniciada"])
        self.assertFalse(r["iniciada_auto"])
        self.assertEqual(r["pickeadas"], 0)

    def test_una_unidad_pickeada_ya_empezo(self):
        r = rc.evaluar([doc(fila(solicitado=3, asignado=2, pickeado=1))])
        self.assertTrue(r["iniciada"] and r["iniciada_auto"])
        self.assertEqual(r["pickeadas"], 1)

    def test_basta_con_un_documento_aunque_el_otro_no_este_en_check(self):
        r = rc.evaluar([doc(fila(solicitado=2, pickeado=2)), None])
        self.assertTrue(r["iniciada_auto"])

    def test_si_algo_ya_esta_despachado_no_se_mueve_solo(self):
        r = rc.evaluar([doc(fila(solicitado=2, pickeado=1, despachado=1))])
        self.assertTrue(r["iniciada"])
        self.assertFalse(r["iniciada_auto"])

    def test_datos_raros_no_cuentan(self):
        r = rc.evaluar([doc(fila(solicitado=2, pickeado="x"))])
        self.assertFalse(r["iniciada"] or r["iniciada_auto"])
        r = rc.evaluar([doc(fila(solicitado=2, pickeado=1, asignado=-1))])
        self.assertFalse(r["iniciada"] or r["iniciada_auto"])

    def test_sin_documentos_o_sin_datos(self):
        for r in (rc.evaluar([]), rc.evaluar([None]), rc.evaluar([None, None])):
            self.assertFalse(r["iniciada"] or r["iniciada_auto"])
            self.assertEqual(r["pickeadas"], 0)

    def test_una_cantidad_minuscula_no_es_una_unidad_pickeada(self):
        """Revisión adversarial 2026-10-02: «0,0000001» movía el retiro con la nota «0 de 3 unidades pickeadas»."""
        r = rc.evaluar([doc(fila(solicitado=3, pickeado="0.0000001"))])
        self.assertFalse(r["iniciada"] or r["iniciada_auto"])
        self.assertEqual(r["pickeadas"], 0)
        r = rc.evaluar([doc(fila(solicitado=3, pickeado="0.5"))])           # media unidad sí es una señal real
        self.assertTrue(r["iniciada_auto"])

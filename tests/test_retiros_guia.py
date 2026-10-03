"""Guía de 6 pasos de la ficha de Retiros (funciones puras de retiros_guia.py).

Correr con:  py -m pytest tests/test_retiros_guia.py -q
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import retiros_guia as rg


def estados(g):
    return [p["estado"] for p in g["pasos"]]


def base(**kw):
    c = {"status": "solicitud_recibida", "n_docs": 1, "docs": [{"rotulo": "BLV 23732", "con_saldo": 1, "otro_rut": False}],
         "docs_firma": "aaa", "prod_n": 2, "prod_firma": "bbb", "correo_ok": True}
    c.update(kw)
    return c


class GuiaDeSeisPasos(unittest.TestCase):
    def test_retiro_nuevo_sin_documentos_empieza_por_agregar_la_factura(self):
        g = rg.evaluar(base(n_docs=0, docs=[], prod_n=0, responsable="Ana"))
        self.assertEqual(estados(g)[:4], ["actual", "hecho", "bloqueado", "bloqueado"])
        self.assertEqual(g["siguiente"], 1)
        self.assertIn("factura o boleta", g["pasos"][0]["faltan"][0])
        self.assertEqual(len(g["pasos"]), 6)
        self.assertEqual([p["n"] for p in g["pasos"]], [1, 2, 3, 4, 5, 6])

    def test_con_documento_pide_confirmarlo_y_los_productos_esperan(self):
        g = rg.evaluar(base())
        self.assertEqual(g["pasos"][0]["estado"], "actual")
        self.assertEqual(g["pasos"][0]["accion"]["tipo"], "confirmar_docs")
        p3 = g["pasos"][2]
        self.assertEqual(p3["estado"], "pendiente")
        self.assertTrue(p3["accion"]["deshabilitada"])
        self.assertIn("paso 1", p3["faltan"][0])

    def test_confirmar_facturas_habilita_productos(self):
        g = rg.evaluar(base(docs_conf="aaa", docs_conf_quien="Samantha", responsable="Ana"))
        self.assertEqual(g["pasos"][0]["estado"], "hecho")
        self.assertIn("Samantha", g["pasos"][0]["resumen"])
        self.assertEqual(g["pasos"][2]["accion"]["tipo"], "confirmar_productos")
        self.assertNotIn("deshabilitada", g["pasos"][2]["accion"])

    def test_si_cambia_la_lista_de_documentos_hay_que_confirmar_de_nuevo(self):
        g = rg.evaluar(base(docs_conf="vieja"))
        self.assertEqual(g["pasos"][0]["estado"], "actual")
        self.assertIn("cambió", g["pasos"][0]["faltan"][0])

    def test_si_cambian_los_productos_hay_que_confirmar_de_nuevo(self):
        g = rg.evaluar(base(docs_conf="aaa", prod_conf="vieja"))
        self.assertEqual(g["pasos"][2]["estado"], "actual")
        self.assertIn("cambiaron", g["pasos"][2]["faltan"][0])

    def test_responsable_es_automatico(self):
        self.assertEqual(rg.evaluar(base())["pasos"][1]["accion"]["tipo"], "tomar")
        g = rg.evaluar(base(responsable="Samantha Blacio"))
        self.assertEqual(g["pasos"][1]["estado"], "hecho")
        self.assertIn("Samantha", g["pasos"][1]["resumen"])

    def test_agenda_estados(self):
        todo = dict(docs_conf="aaa", prod_conf="bbb", responsable="Ana")
        g = rg.evaluar(base(**todo))
        self.assertEqual(g["pasos"][3]["estado"], "actual")
        self.assertTrue(g["pasos"][3]["correo"])
        self.assertEqual(g["pasos"][3]["accion"]["tipo"], "proponer")
        g = rg.evaluar(base(propuesta=True, **todo))
        self.assertEqual(g["pasos"][3]["estado"], "espera")
        g = rg.evaluar(base(propuesta=True, cita=True, status="agenda_confirmada", **todo))
        self.assertEqual(g["pasos"][3]["estado"], "hecho")

    def test_proponer_sin_terminar_lo_previo_avisa_exactamente_que_falta(self):
        g = rg.evaluar(base())
        p4 = g["pasos"][3]
        self.assertEqual(p4["estado"], "pendiente")
        self.assertTrue(any("paso" in a for a in p4["avisos"]))
        previos = dict((n, t) for n, t in g["previos"]["4"])
        self.assertEqual(sorted(previos), [1, 2, 3])
        self.assertIn("confirmarla", previos[1][0])

    def test_sin_correo_valido_avisa_pero_no_bloquea(self):
        # La propuesta también sale por otros correos del retiro, SMS y WhatsApp: no se le quita la acción al operador.
        g = rg.evaluar(base(correo_ok=False, docs_conf="aaa", prod_conf="bbb", responsable="Ana"))
        p4 = g["pasos"][3]
        self.assertEqual(p4["estado"], "actual")
        self.assertFalse(p4["bloquea"])
        self.assertTrue(any("correo" in a for a in p4["avisos"]))

    def test_contrapropuesta_del_cliente_antes_de_haber_cita_le_toca_a_ilus(self):
        g = rg.evaluar(base(status="en_revision", propuesta=True, cambio_pedido=True, adelantado=True, responsable="Ana"))
        p4 = g["pasos"][3]
        self.assertEqual(p4["estado"], "actual")
        self.assertEqual(p4["ancla"], "#paso-esperando")
        self.assertEqual(g["siguiente"], 4)
        self.assertEqual(g["pasos"][4]["estado"], "bloqueado")

    def test_propuesta_esperando_apunta_a_la_tarjeta_de_espera(self):
        g = rg.evaluar(base(propuesta=True, adelantado=True, responsable="Ana"))
        self.assertEqual(g["pasos"][3]["estado"], "espera")
        self.assertEqual(g["pasos"][3]["ancla"], "#paso-esperando")

    def test_retiro_en_curso_sin_responsable_se_frena_hasta_que_alguien_se_haga_cargo(self):
        # Daniel 2026-10-02: «para avanzar debe declarar el responsable, y para agendar o liberar el calendario»
        g = rg.evaluar(base(status="en_preparacion", adelantado=True, cita=True, preparado=True))
        self.assertTrue(g["sin_responsable"])
        self.assertEqual(g["pasos"][1]["estado"], "actual")
        self.assertFalse(g["pasos"][1]["secundario"])
        self.assertEqual(g["siguiente"], 2)          # lo primero es declarar el responsable
        self.assertEqual(g["pasos"][5]["accion"]["tipo"], "retirar")
        self.assertTrue(g["pasos"][5]["accion"]["deshabilitada"])
        self.assertEqual(g["pasos"][5]["accion"]["motivo"], rg.MSG_SIN_RESPONSABLE)

    def test_con_el_interruptor_apagado_el_responsable_vuelve_a_ser_solo_un_aviso(self):
        g = rg.evaluar(base(status="en_preparacion", adelantado=True, cita=True, preparado=False, exige_responsable=False))
        self.assertFalse(g["sin_responsable"])
        self.assertEqual(g["pasos"][1]["estado"], "pendiente")
        self.assertTrue(g["pasos"][1]["secundario"])
        self.assertEqual(g["siguiente"], 5)          # lo operativo, no el responsable
        g2 = rg.evaluar(base(status="en_preparacion", adelantado=True, cita=True, preparado=True, exige_responsable=False))
        self.assertEqual(g2["siguiente"], 6)
        self.assertNotIn("deshabilitada", g2["pasos"][5]["accion"])

    def test_sin_responsable_solo_se_puede_tomar_el_retiro(self):
        g = rg.evaluar(base(status="agenda_confirmada", cita=True, adelantado=True, docs_conf="aaa", prod_conf="bbb"))
        self.assertTrue(g["sin_responsable"])
        self.assertEqual(g["siguiente"], 2)
        for p in g["pasos"]:
            a = p.get("accion")
            if not a:
                continue
            if a["tipo"] == "tomar":
                self.assertNotIn("deshabilitada", a)
            else:
                self.assertTrue(a.get("deshabilitada"), f"paso {p['n']} ({a['tipo']}) debía quedar bloqueado")
                self.assertEqual(a["motivo"], rg.MSG_SIN_RESPONSABLE)

    def test_con_responsable_nada_queda_bloqueado_por_eso(self):
        g = rg.evaluar(base(status="agenda_confirmada", cita=True, adelantado=True, responsable="Ana"))
        self.assertFalse(g["sin_responsable"])
        self.assertEqual(g["pasos"][1]["estado"], "hecho")
        for p in g["pasos"]:
            self.assertNotEqual((p.get("accion") or {}).get("motivo"), rg.MSG_SIN_RESPONSABLE)

    def test_un_retiro_terminado_no_pide_responsable(self):
        for st in ("retirada", "rechazada", "fallida"):
            g = rg.evaluar(base(status=st, adelantado=True))
            self.assertFalse(g["sin_responsable"], st)
            self.assertIsNone(g["siguiente"], st)

    def test_retiro_nuevo_sin_responsable_si_es_actual(self):
        g = rg.evaluar(base())
        self.assertEqual(g["pasos"][1]["estado"], "actual")
        self.assertFalse(g["pasos"][1]["secundario"])

    def test_texto_honesto_en_retiros_que_ya_venian_en_curso(self):
        g = rg.evaluar(base(status="agenda_confirmada", cita=True, adelantado=True, responsable="Ana"))
        self.assertIn("Ya venía en curso", g["pasos"][0]["resumen"])
        self.assertNotIn("confirmado", g["pasos"][0]["resumen"])
        g2 = rg.evaluar(base(docs_conf="aaa", docs_conf_quien="Samantha", adelantado=True, responsable="Ana"))
        self.assertIn("Samantha", g2["pasos"][0]["resumen"])

    def test_cerrada_sin_evidencia_no_es_retiro_completado(self):
        g = rg.evaluar(base(status="cerrada", adelantado=True, evidencia_retiro=False))
        self.assertIn("cerró sin", g["terminal"])
        self.assertIsNone(g["siguiente"])
        g2 = rg.evaluar(base(status="cerrada", adelantado=True, evidencia_retiro=True))
        self.assertFalse(g2["terminal"])
        self.assertEqual(set(estados(g2)), {"hecho"})

    def test_cambio_de_fecha_pedido_por_el_cliente_pone_la_agenda_en_rojo_y_frena_preparacion(self):
        g = rg.evaluar(base(status="agenda_confirmada", cita=True, cambio_pedido=True, adelantado=True, responsable="Ana"))
        self.assertEqual(g["pasos"][3]["estado"], "actual")
        self.assertEqual(g["pasos"][4]["estado"], "bloqueado")
        self.assertEqual(g["siguiente"], 4)

    def test_cita_confirmada_toca_enviar_a_preparacion_y_avisa_del_correo(self):
        g = rg.evaluar(base(status="agenda_confirmada", cita=True, adelantado=True, responsable="Ana"))
        self.assertEqual(g["pasos"][4]["estado"], "actual")
        self.assertTrue(g["pasos"][4]["correo"])
        self.assertEqual(g["pasos"][4]["accion"]["tipo"], "preparacion")
        self.assertEqual(g["siguiente"], 5)

    def test_en_preparacion_espera_y_luego_toca_entregar(self):
        g = rg.evaluar(base(status="en_preparacion", adelantado=True, responsable="Ana", picking_total=3, picking_hechos=1))
        self.assertEqual(g["pasos"][4]["estado"], "espera")
        self.assertIn("1 de 3", g["pasos"][4]["faltan"][0])
        self.assertEqual(g["pasos"][5]["estado"], "pendiente")
        g = rg.evaluar(base(status="en_preparacion", adelantado=True, responsable="Ana", preparado=True))
        self.assertEqual(g["pasos"][4]["estado"], "hecho")
        self.assertEqual(g["pasos"][5]["estado"], "actual")
        self.assertEqual(g["siguiente"], 6)

    def test_retiro_ya_avanzado_no_pide_nada_retroactivo(self):
        # Caso del cliente real (cita confirmada, en preparación, sin marca «Confirmo»): 1 y 3 hechos.
        g = rg.evaluar(base(status="en_preparacion", adelantado=True, responsable="Samantha"))
        self.assertEqual(estados(g)[:4], ["hecho", "hecho", "hecho", "hecho"])
        self.assertEqual(g["pasos"][4]["estado"], "espera")

    def test_retiro_terminado_todo_hecho(self):
        g = rg.evaluar(base(status="retirada", adelantado=True))
        self.assertEqual(set(estados(g)), {"hecho"})
        self.assertIsNone(g["siguiente"])

    def test_rechazado_o_fallido_no_pide_nada(self):
        g = rg.evaluar(base(status="rechazada"))
        self.assertTrue(g["terminal"])
        self.assertIsNone(g["siguiente"])
        self.assertTrue(all(p["accion"] is None for p in g["pasos"] if p["estado"] != "hecho"))

    def test_avisos_de_saldo_y_otro_rut(self):
        g = rg.evaluar(base(docs=[{"rotulo": "FCV 5", "con_saldo": 0, "otro_rut": True}]))
        self.assertEqual(len(g["pasos"][0]["avisos"]), 2)

    def test_firma_es_estable_y_sensible_a_cambios(self):
        self.assertEqual(rg.firma(["b", "a"]), rg.firma(["a", "b"]))
        self.assertNotEqual(rg.firma(["a"]), rg.firma(["a", "b"]))
        self.assertEqual(len(rg.firma([])), 12)


if __name__ == "__main__":
    unittest.main()

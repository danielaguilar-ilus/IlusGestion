"""Guía de 5 pasos de la ficha de Retiros (funciones puras de retiros_guia.py).

Daniel 2026-10-14: 1 Facturas y productos (una sola tarjeta, dos confirmaciones), 2 Responsable (debajo de los productos), 3 Agenda,
4 Preparación, 5 Entrega. Facturas y productos se confirman ANTES de «Me hago cargo»; el responsable se exige para agendar y avanzar.

Correr con:  py -m pytest tests/test_retiros_guia.py -q
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import retiros_guia as rg


def estados(g):
    return [p["estado"] for p in g["pasos"]]


def paso(g, clave):
    return next(p for p in g["pasos"] if p["clave"] == clave)


def base(**kw):
    c = {"status": "solicitud_recibida", "n_docs": 1, "docs": [{"rotulo": "BLV 23732", "con_saldo": 1, "otro_rut": False}],
         "docs_firma": "aaa", "prod_n": 2, "prod_firma": "bbb", "correo_ok": True}
    c.update(kw)
    return c


class GuiaDeCincoPasos(unittest.TestCase):
    def test_los_cinco_pasos_en_el_orden_de_la_ficha(self):
        g = rg.evaluar(base())
        self.assertEqual([p["n"] for p in g["pasos"]], [1, 2, 3, 4, 5])
        self.assertEqual([p["clave"] for p in g["pasos"]], ["documentos", "responsable", "agenda", "preparacion", "entrega"])
        self.assertEqual(paso(g, "documentos")["titulo"], "Facturas y productos")
        self.assertEqual(paso(g, "documentos")["ancla"], "#paso-2")
        self.assertEqual(paso(g, "responsable")["ancla"], "#paso-resp")

    def test_retiro_nuevo_sin_documentos_empieza_por_agregar_la_factura(self):
        g = rg.evaluar(base(n_docs=0, docs=[], prod_n=0, responsable="Ana"))
        self.assertEqual(estados(g)[:3], ["actual", "hecho", "bloqueado"])
        self.assertEqual(g["siguiente"], 1)
        p1 = paso(g, "documentos")
        self.assertIn("factura o boleta", p1["faltan"][0])
        self.assertEqual(p1["accion"]["tipo"], "ir")
        self.assertEqual(p1["sub"]["productos"]["estado"], "bloqueado")

    def test_con_documento_pide_confirmar_las_facturas_y_los_productos_esperan(self):
        g = rg.evaluar(base())
        p1 = paso(g, "documentos")
        self.assertEqual(p1["estado"], "actual")
        self.assertEqual(p1["accion"]["tipo"], "confirmar_docs")
        prod = p1["sub"]["productos"]
        self.assertEqual(prod["estado"], "pendiente")
        self.assertTrue(prod["accion"]["deshabilitada"])
        self.assertIn("confirma las facturas", prod["accion"]["motivo"])

    def test_confirmar_facturas_pasa_a_los_productos_del_mismo_paso(self):
        g = rg.evaluar(base(docs_conf="aaa", docs_conf_quien="Samantha", responsable="Ana"))
        p1 = paso(g, "documentos")
        self.assertEqual(p1["sub"]["facturas"]["estado"], "hecho")
        self.assertIn("Samantha", p1["sub"]["facturas"]["resumen"])
        self.assertEqual(p1["estado"], "actual")                      # el paso sigue: faltan los productos
        self.assertEqual(p1["accion"]["tipo"], "confirmar_productos")
        self.assertNotIn("deshabilitada", p1["accion"])

    def test_el_paso_1_queda_hecho_con_las_dos_confirmaciones(self):
        g = rg.evaluar(base(docs_conf="aaa", docs_conf_quien="Sam", prod_conf="bbb", prod_conf_quien="Sam", responsable="Ana"))
        p1 = paso(g, "documentos")
        self.assertEqual(p1["estado"], "hecho")
        self.assertIn("documento", p1["resumen"])
        self.assertIn("producto", p1["resumen"])

    def test_si_cambia_la_lista_de_documentos_hay_que_confirmar_de_nuevo(self):
        g = rg.evaluar(base(docs_conf="vieja"))
        self.assertEqual(paso(g, "documentos")["estado"], "actual")
        self.assertIn("cambió", paso(g, "documentos")["faltan"][0])

    def test_si_cambian_los_productos_hay_que_confirmar_de_nuevo(self):
        g = rg.evaluar(base(docs_conf="aaa", prod_conf="vieja"))
        p1 = paso(g, "documentos")
        self.assertEqual(p1["estado"], "actual")
        self.assertIn("cambiaron", p1["faltan"][0])

    def test_responsable_es_automatico(self):
        self.assertEqual(paso(rg.evaluar(base()), "responsable")["accion"]["tipo"], "tomar")
        g = rg.evaluar(base(responsable="Samantha Blacio"))
        self.assertEqual(paso(g, "responsable")["estado"], "hecho")
        self.assertIn("Samantha", paso(g, "responsable")["resumen"])

    def test_agenda_estados(self):
        todo = dict(docs_conf="aaa", prod_conf="bbb", responsable="Ana")
        g = rg.evaluar(base(**todo))
        ag = paso(g, "agenda")
        self.assertEqual(ag["estado"], "actual")
        self.assertTrue(ag["correo"])
        self.assertEqual(ag["accion"]["tipo"], "proponer")
        self.assertEqual(paso(rg.evaluar(base(propuesta=True, **todo)), "agenda")["estado"], "espera")
        self.assertEqual(paso(rg.evaluar(base(propuesta=True, cita=True, status="agenda_confirmada", **todo)), "agenda")["estado"], "hecho")

    def test_proponer_sin_terminar_lo_previo_avisa_exactamente_que_falta(self):
        g = rg.evaluar(base())
        ag = paso(g, "agenda")
        self.assertEqual(ag["estado"], "pendiente")
        self.assertTrue(any("paso" in a for a in ag["avisos"]))
        previos = dict((n, t) for n, t in g["previos"][str(ag["n"])])
        self.assertEqual(sorted(previos), [1, 2])
        self.assertIn("confirmarla", previos[1][0])

    def test_sin_correo_valido_avisa_pero_no_bloquea(self):
        # La propuesta también sale por otros correos del retiro, SMS y WhatsApp: no se le quita la acción al operador.
        g = rg.evaluar(base(correo_ok=False, docs_conf="aaa", prod_conf="bbb", responsable="Ana"))
        ag = paso(g, "agenda")
        self.assertEqual(ag["estado"], "actual")
        self.assertFalse(ag["bloquea"])
        self.assertTrue(any("correo" in a for a in ag["avisos"]))

    def test_contrapropuesta_del_cliente_antes_de_haber_cita_le_toca_a_ilus(self):
        g = rg.evaluar(base(status="en_revision", propuesta=True, cambio_pedido=True, adelantado=True, responsable="Ana"))
        ag = paso(g, "agenda")
        self.assertEqual(ag["estado"], "actual")
        self.assertEqual(ag["ancla"], "#paso-esperando")
        self.assertEqual(g["siguiente"], ag["n"])
        self.assertEqual(paso(g, "preparacion")["estado"], "bloqueado")

    def test_propuesta_esperando_apunta_a_la_tarjeta_de_espera(self):
        g = rg.evaluar(base(propuesta=True, adelantado=True, responsable="Ana"))
        self.assertEqual(paso(g, "agenda")["estado"], "espera")
        self.assertEqual(paso(g, "agenda")["ancla"], "#paso-esperando")

    # ── Responsable (REGLA #21, con el orden del 2026-10-14) ──────────────────────────────────────────────────────────
    def test_sin_responsable_la_ficha_parte_en_el_paso_1(self):
        """Daniel 2026-10-14: «comienza con el paso 2, no tiene sentido». Facturas y productos se pueden confirmar sin responsable."""
        g = rg.evaluar(base())
        self.assertTrue(g["sin_responsable"])
        self.assertEqual(g["siguiente"], 1)
        p1 = paso(g, "documentos")
        self.assertEqual(p1["estado"], "actual")
        self.assertNotIn("deshabilitada", p1["accion"])
        self.assertEqual(paso(g, "responsable")["estado"], "pendiente")       # todavía no le toca: primero el paso 1
        self.assertNotIn("deshabilitada", paso(g, "responsable")["accion"])   # pero «Me hago cargo» se puede tocar igual

    def test_sin_responsable_los_productos_tambien_se_confirman(self):
        g = rg.evaluar(base(docs_conf="aaa"))
        p1 = paso(g, "documentos")
        self.assertEqual(p1["accion"]["tipo"], "confirmar_productos")
        self.assertNotIn("deshabilitada", p1["accion"])
        self.assertNotIn("deshabilitada", p1["sub"]["productos"]["accion"])

    def test_con_el_paso_1_listo_y_sin_responsable_toca_hacerse_cargo(self):
        g = rg.evaluar(base(docs_conf="aaa", prod_conf="bbb"))
        self.assertEqual(paso(g, "documentos")["estado"], "hecho")
        self.assertEqual(paso(g, "responsable")["estado"], "actual")
        self.assertEqual(g["siguiente"], 2)
        ag = paso(g, "agenda")
        self.assertTrue(ag["accion"]["deshabilitada"])                       # agendar sí exige responsable
        self.assertEqual(ag["accion"]["motivo"], rg.MSG_SIN_RESPONSABLE)

    def test_retiro_en_curso_sin_responsable_se_frena_hasta_que_alguien_se_haga_cargo(self):
        # Daniel 2026-10-02: «para avanzar debe declarar el responsable, y para agendar o liberar el calendario»
        g = rg.evaluar(base(status="en_preparacion", adelantado=True, cita=True, preparado=True))
        self.assertTrue(g["sin_responsable"])
        self.assertEqual(paso(g, "responsable")["estado"], "actual")
        self.assertFalse(paso(g, "responsable")["secundario"])
        self.assertEqual(g["siguiente"], 2)          # el paso 1 ya venía hecho: lo primero es declarar el responsable
        ent = paso(g, "entrega")
        self.assertEqual(ent["accion"]["tipo"], "retirar")
        self.assertTrue(ent["accion"]["deshabilitada"])
        self.assertEqual(ent["accion"]["motivo"], rg.MSG_SIN_RESPONSABLE)

    def test_con_el_interruptor_apagado_el_responsable_vuelve_a_ser_solo_un_aviso(self):
        g = rg.evaluar(base(status="en_preparacion", adelantado=True, cita=True, preparado=False, exige_responsable=False))
        self.assertFalse(g["sin_responsable"])
        self.assertEqual(paso(g, "responsable")["estado"], "pendiente")
        self.assertTrue(paso(g, "responsable")["secundario"])
        self.assertEqual(g["siguiente"], paso(g, "preparacion")["n"])          # lo operativo, no el responsable
        g2 = rg.evaluar(base(status="en_preparacion", adelantado=True, cita=True, preparado=True, exige_responsable=False))
        self.assertEqual(g2["siguiente"], paso(g2, "entrega")["n"])
        self.assertNotIn("deshabilitada", paso(g2, "entrega")["accion"])

    def test_sin_responsable_solo_se_bloquea_lo_que_agenda_o_mueve_el_retiro(self):
        g = rg.evaluar(base(status="agenda_confirmada", cita=True, adelantado=True, docs_conf="aaa", prod_conf="bbb"))
        self.assertTrue(g["sin_responsable"])
        self.assertEqual(g["siguiente"], 2)
        for p in g["pasos"]:
            a = p.get("accion")
            if not a:
                continue
            if p["n"] <= 2:
                self.assertNotIn("deshabilitada", a, f"paso {p['n']} ({a['tipo']}) no debía quedar bloqueado")
            else:
                self.assertTrue(a.get("deshabilitada"), f"paso {p['n']} ({a['tipo']}) debía quedar bloqueado")
                self.assertEqual(a["motivo"], rg.MSG_SIN_RESPONSABLE)

    def test_con_responsable_nada_queda_bloqueado_por_eso(self):
        g = rg.evaluar(base(status="agenda_confirmada", cita=True, adelantado=True, responsable="Ana"))
        self.assertFalse(g["sin_responsable"])
        self.assertEqual(paso(g, "responsable")["estado"], "hecho")
        for p in g["pasos"]:
            self.assertNotEqual((p.get("accion") or {}).get("motivo"), rg.MSG_SIN_RESPONSABLE)

    def test_un_retiro_terminado_no_pide_responsable(self):
        for st in ("retirada", "rechazada", "fallida"):
            g = rg.evaluar(base(status=st, adelantado=True))
            self.assertFalse(g["sin_responsable"], st)
            self.assertIsNone(g["siguiente"], st)

    def test_retiro_nuevo_sin_responsable_lo_avisa_sin_saltarse_el_paso_1(self):
        g = rg.evaluar(base())
        self.assertEqual(paso(g, "responsable")["estado"], "pendiente")
        self.assertFalse(paso(g, "responsable")["secundario"])
        self.assertIn("Me hago cargo", paso(g, "responsable")["faltan"][0])

    # ── Resto del recorrido ─────────────────────────────────────────────────────────────────────────────────────────
    def test_texto_honesto_en_retiros_que_ya_venian_en_curso(self):
        g = rg.evaluar(base(status="agenda_confirmada", cita=True, adelantado=True, responsable="Ana"))
        self.assertIn("Ya venía en curso", paso(g, "documentos")["resumen"])
        self.assertNotIn("confirmado", paso(g, "documentos")["resumen"])
        g2 = rg.evaluar(base(docs_conf="aaa", docs_conf_quien="Samantha", adelantado=True, responsable="Ana"))
        self.assertIn("Samantha", paso(g2, "documentos")["resumen"])

    def test_cerrada_sin_evidencia_no_es_retiro_completado(self):
        g = rg.evaluar(base(status="cerrada", adelantado=True, evidencia_retiro=False))
        self.assertIn("cerró sin", g["terminal"])
        self.assertIsNone(g["siguiente"])
        g2 = rg.evaluar(base(status="cerrada", adelantado=True, evidencia_retiro=True))
        self.assertFalse(g2["terminal"])
        self.assertEqual(set(estados(g2)), {"hecho"})

    def test_cambio_de_fecha_pedido_por_el_cliente_pone_la_agenda_primero_y_frena_preparacion(self):
        g = rg.evaluar(base(status="agenda_confirmada", cita=True, cambio_pedido=True, adelantado=True, responsable="Ana"))
        self.assertEqual(paso(g, "agenda")["estado"], "actual")
        self.assertEqual(paso(g, "preparacion")["estado"], "bloqueado")
        self.assertIn("(paso 3)", paso(g, "preparacion")["faltan"][0])
        self.assertEqual(g["siguiente"], 3)

    def test_cita_confirmada_toca_enviar_a_preparacion_y_avisa_del_correo(self):
        g = rg.evaluar(base(status="agenda_confirmada", cita=True, adelantado=True, responsable="Ana"))
        pr = paso(g, "preparacion")
        self.assertEqual(pr["estado"], "actual")
        self.assertTrue(pr["correo"])
        self.assertEqual(pr["accion"]["tipo"], "preparacion")
        self.assertEqual(g["siguiente"], 4)

    def test_en_preparacion_espera_y_luego_toca_entregar(self):
        g = rg.evaluar(base(status="en_preparacion", adelantado=True, responsable="Ana", picking_total=3, picking_hechos=1))
        self.assertEqual(paso(g, "preparacion")["estado"], "espera")
        self.assertIn("1 de 3", paso(g, "preparacion")["faltan"][0])
        self.assertEqual(paso(g, "entrega")["estado"], "pendiente")
        self.assertIn("(paso 4)", paso(g, "entrega")["faltan"][0])
        g = rg.evaluar(base(status="en_preparacion", adelantado=True, responsable="Ana", preparado=True))
        self.assertEqual(paso(g, "preparacion")["estado"], "hecho")
        self.assertEqual(paso(g, "entrega")["estado"], "actual")
        self.assertEqual(g["siguiente"], 5)

    def test_retiro_ya_avanzado_no_pide_nada_retroactivo(self):
        # Caso del cliente real (cita confirmada, en preparación, sin marca «Confirmo»): facturas y productos dados por buenos.
        g = rg.evaluar(base(status="en_preparacion", adelantado=True, responsable="Samantha"))
        self.assertEqual(estados(g)[:3], ["hecho", "hecho", "hecho"])
        self.assertEqual(paso(g, "preparacion")["estado"], "espera")

    def test_retiro_terminado_todo_hecho(self):
        g = rg.evaluar(base(status="retirada", adelantado=True))
        self.assertEqual(set(estados(g)), {"hecho"})
        self.assertIsNone(g["siguiente"])

    def test_rechazado_o_fallido_no_pide_nada_ni_deja_confirmaciones_vivas(self):
        for st in ("rechazada", "fallida"):
            g = rg.evaluar(base(status=st))
            self.assertTrue(g["terminal"])
            self.assertIsNone(g["siguiente"])
            self.assertTrue(all(p["accion"] is None for p in g["pasos"] if p["estado"] != "hecho"))
            for sp in paso(g, "documentos")["sub"].values():          # tampoco dentro del paso 1
                if sp["estado"] != "hecho":
                    self.assertIsNone(sp["accion"], st)

    def test_avisos_de_saldo_y_otro_rut(self):
        g = rg.evaluar(base(docs=[{"rotulo": "FCV 5", "con_saldo": 0, "otro_rut": True}]))
        self.assertEqual(len(paso(g, "documentos")["avisos"]), 2)

    def test_firma_es_estable_y_sensible_a_cambios(self):
        self.assertEqual(rg.firma(["b", "a"]), rg.firma(["a", "b"]))
        self.assertNotEqual(rg.firma(["a"]), rg.firma(["a", "b"]))
        self.assertEqual(len(rg.firma([])), 12)


if __name__ == "__main__":
    unittest.main()

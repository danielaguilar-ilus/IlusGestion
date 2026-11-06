# -*- coding: utf-8 -*-
"""AUDITORÍA DE DISEÑO DE LOS CORREOS DE LA OT, APLICADA (Impeccable, 6-nov-2026; Daniel: «sí, aplica los cambios de Impeccable»).

Lo que la auditoría encontró (con los correos reales renderizados a 700 y 390 px y el detector de contraste/tamaño) y lo que se vigila:
  · La frase del ticket —la única acción que se le pide al cliente— salía en gris claro de 12 px (3,5:1): ahora 14 px, #374151 y «Crea un ticket de soporte» en negrita.
  · Letra de 11 px (etiquetas de las cajas y texto legal del pie): ahora 12 px, y el pie legal más claro (#9ca3af sobre #101010).
  · Demasiada letra y repetición: el subtítulo repetía el número de OT del título, «Qué sigue» repetía «Qué va a pasar», el resumen repetía la tabla.
  · El número de OT se partía en dos líneas («OT-2026-» / «00123») en el título.
  · En celular el correo se desbordaba 4 px (logo de 306 + 2 × 32 de relleno = 370 en 366 disponibles).
  · «archivo .ics» era jerga; y si Outlook bloquea el logo, el texto alternativo no tenía color.
Todo con datos de mentira. Correr:  python -m pytest tests/test_correos_impeccable.py -q -p no:warnings
"""
import re
import unittest

import email_legible as L
import ot_comunicacion as C
from tests import _fuente_codigo as FC

PIE_VIEJO = '<p style="margin:18px 0 0;font-size:12px;color:#888;line-height:1.5">¿Necesitas reagendar o cancelar? Crea un ticket de soporte con el botón de abajo.</p>'


class TestLaFraseDelTicket(unittest.TestCase):
    def test_pasa_a_14_px_oscura_y_con_la_accion_en_negrita(self):
        s = L.frase_ticket_legible(PIE_VIEJO)
        self.assertIn("font-size:14px", s)
        self.assertIn("color:#374151", s)
        self.assertNotIn("#888", s)
        self.assertIn("<strong>Crea un ticket de soporte</strong> con el botón de abajo.", s)
        self.assertIn("margin:18px 0 0", s)                                                    # el resto del estilo, tal cual

    def test_vale_para_cualquier_parrafo_con_la_frase_aunque_tenga_otro_estilo(self):
        retiros = ('<p style="font-size:12px;color:#6b7280;line-height:1.5;text-align:center;margin:18px 0 0">'
                   '¿Necesitas reagendar o tienes una duda? Crea un ticket de soporte con el botón de abajo y te ayudamos.</p>')
        s = L.frase_ticket_legible(retiros)
        self.assertIn("font-size:14px", s)
        self.assertIn("text-align:center", s)                                                  # conserva lo suyo
        self.assertIn("<strong>Crea un ticket de soporte</strong>", s)
        self.assertIn("te ayudamos.", s)

    def test_no_toca_lo_que_no_es_la_frase(self):
        otros = '<p style="font-size:12px;color:#888">Otro texto chico.</p><p>Crea un ticket cualquiera</p><table><tr><td>x</td></tr></table>'
        self.assertEqual(L.frase_ticket_legible(otros), otros)
        self.assertEqual(L.frase_ticket_legible(""), "")
        self.assertIsNone(L.frase_ticket_legible(None))

    def test_es_idempotente_y_no_duplica_la_negrita(self):
        una = L.frase_ticket_legible(PIE_VIEJO)
        self.assertEqual(L.frase_ticket_legible(una), una)
        self.assertEqual(una.count("<strong>"), 1)

    def test_la_forma_formal_del_plan_tambien(self):
        formal = '<p style="font-size:13px;color:#6b7280">Para confirmar o ajustar fechas, cree un ticket de soporte con el botón de abajo y coordinamos la agenda.</p>'
        self.assertIn("<strong>cree un ticket de soporte</strong>", L.frase_ticket_legible(formal))

    def test_en_las_plantillas_vigentes_de_la_ot_la_frase_queda_legible(self):
        for slug, (_a, cuerpo) in list(C.SEMILLAS.items()) + list(C.SEMILLAS_EXTRA.items()):
            if "ticket de soporte con el botón de abajo" not in cuerpo:
                continue
            s = L.frase_ticket_legible(cuerpo)
            self.assertIn("font-size:14px", s, slug)
            self.assertNotIn("color:#888", s, slug)

    def test_esta_cableada_al_envio_y_a_la_vista_previa(self):
        app = FC.texto("app.py")
        self.assertIn("import email_legible as _leg", app)
        self.assertIn("html_body = _eos.outlook_safe(_leg.frase_ticket_legible(html_body))", app)
        self.assertIn("cuerpo = _leg.frase_ticket_legible(cuerpo)", app)


class TestElMaestro(unittest.TestCase):
    def _master(self, **ctx):
        import datetime as dt
        amb = {"re": re, "datetime": dt.datetime, "_get_marca": lambda: {}, "ILUS_SOPORTE_EMAIL": "soportetec@sphs.cl",
               "ILUS_SOPORTE_URL": "https://ilusfitness.com/pages/soporte-tecnico"}
        FC.extraer(amb, "app.py", ("_ilus_email_master",), ())
        return amb["_ilus_email_master"](dict({"title": "Tu orden de trabajo OT-2026-00123 quedó terminada", "subtitle": "OT-2026-00123", "body_html": "<p>x</p>"}, **ctx))

    def test_el_subtitulo_repetido_se_omite(self):
        h = self._master()
        self.assertEqual(h.count("OT-2026-00123"), 1)                                          # solo el del título
        h2 = self._master(subtitle="Otro dato")
        self.assertIn("Otro dato", h2)                                                         # uno distinto se conserva

    def test_el_numero_de_ot_del_titulo_no_se_parte(self):
        h = self._master()
        self.assertIn('<span style="white-space:nowrap">OT-2026-00123</span>', h)
        self.assertIn("Recordatorio", self._master(title="Recordatorio: tu visita técnica es mañana (OT-2026-00123)"))

    def test_el_encabezado_cabe_en_un_celular_de_390(self):
        h = self._master()
        m = re.search(r'background:#050505;padding:25px (\d+)px 22px', h)
        self.assertIsNotNone(m)
        self.assertLessEqual(306 + 2 * int(m.group(1)), 390 - 24)                              # logo + relleno ≤ lo que cabe (390 menos los 12 px de cada lado)

    def test_si_outlook_bloquea_el_logo_el_texto_alternativo_se_lee(self):
        h = self._master()
        img = re.search(r'<img[^>]*alt="ILUS Fitness"[^>]*>', h).group(0)
        self.assertIn("color:#ffffff", img)
        self.assertIn("font-weight:800", img)

    def test_el_texto_legal_del_pie_es_de_12_px_y_se_lee_sobre_negro(self):
        h = self._master()
        m = re.search(r'font-size:(\d+)px;line-height:18px;color:(#[0-9a-f]{6})">Este correo fue generado', h)
        self.assertIsNotNone(m)
        self.assertGreaterEqual(int(m.group(1)), 12)
        self.assertEqual(m.group(2), "#9ca3af")
        # contraste #9ca3af sobre #101010 ≥ 4,5:1
        def lum(hx):
            c = [int(hx[i:i + 2], 16) / 255 for i in (1, 3, 5)]
            c = [x / 12.92 if x <= 0.03928 else ((x + 0.055) / 1.055) ** 2.4 for x in c]
            return 0.2126 * c[0] + 0.7152 * c[1] + 0.0722 * c[2]
        a, b = sorted((lum("#9ca3af"), lum("#101010")), reverse=True)
        self.assertGreaterEqual((a + 0.05) / (b + 0.05), 4.5)


class TestMenosLetra(unittest.TestCase):
    def test_los_titulos_de_las_cajas_son_de_12_px(self):
        h = C.caja_valor("Qué va a pasar", "<p>x</p>")
        self.assertIn("font-size:12px", h)
        self.assertNotIn("font-size:11px", h)

    def test_el_resumen_de_ot_terminada_es_una_linea(self):
        h = C.resumen_trabajo_html({"equipos": ["a", "b"], "tecnicos": ["Pedro"]})
        self.assertEqual(h.count("<p"), 1)
        self.assertNotIn("<ul", h)
        self.assertIn("Documento firmado", h)

    def test_la_invitacion_no_habla_de_archivos(self):
        self.assertNotIn(".ics", C.invitacion_html())
        self.assertNotIn("archivo", C.invitacion_html())
        self.assertIn("acéptala", C.invitacion_html())

    def test_el_recordatorio_no_repite_que_sigue(self):
        import datetime as dt
        import ot_recordatorio_24h as O
        ot = {"id": 1, "numero_ot": "OT-2026-00001", "cliente": "X", "equipos": ["a"], "tecnicos": ["Pedro"], "tipo": "instalacion", "hora_inicio": None, "hora_fin": None}
        v = O.variables_recordatorio(ot, dt.datetime(2026, 11, 10, 10, 0), dt.date(2026, 11, 9), para="cliente")
        self.assertNotIn("Qué sigue", v["recorrido_html"])
        self.assertIn("QUÉ VA A PASAR", v["que_esperar_html"].upper())


if __name__ == "__main__":
    unittest.main()

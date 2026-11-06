# -*- coding: utf-8 -*-
"""CORREOS A PRUEBA DE OUTLOOK DE ESCRITORIO, SIN DAÑAR GMAIL NI EL CELULAR (Daniel, 6-nov-2026).

Daniel, con capturas de Outlook: «en Outlook no se ve bien… evalúalo para una buena vista en Outlook sin dañar las vistas de celular y de Gmail» y «en el mensaje de
recordatorio me pide calendarizar pero en el computador no».

Qué se vigila:
  · `email_outlook_safe.outlook_safe` reescribe SOLO lo que el motor de Word dibuja mal (padding/margin de una <table>, padding de un <a>-botón) dejando el relleno,
    el fondo y la raya de color en una celda; el texto no cambia, las etiquetas quedan balanceadas y es idempotente.
  · Sobre los bloques REALES de los correos de la OT y las plantillas vigentes no queda nada que Outlook dibuje mal.
  · El envío real pasa el HTML por el filtro justo antes de salir, y la invitación de calendario al CLIENTE (METHOD:REQUEST con RSVP) sale declarada como REQUEST, adjunta
    Y como copia inline (así Outlook de escritorio muestra «Aceptar / Rechazar»); la de los técnicos (sin RSVP) queda como estaba.
Todo con datos de mentira y un servidor de correo falso (REGLA #22). Correr:  python -m pytest tests/test_email_outlook_safe.py -q -p no:warnings
"""
import datetime as dt
import email
import re
import unittest

import email_outlook_safe as E
import ot_comunicacion as C
import ot_correos_tecnicos as T
import ot_recordatorio_24h as O
from tests import _fuente_codigo as FC


def _texto(h):
    return re.sub(r"\s+", " ", re.sub(r"<[^>]*>", " ", h)).strip()


def _balance(h):
    return len(re.findall(r"<table\b", h, re.I)), len(re.findall(r"</table>", h, re.I))


CAJA = ('<table cellpadding="0" cellspacing="0" width="100%" style="background:#f5f5f7;border-left:4px solid #dc2626;border-radius:4px;'
        'padding:14px 18px;margin:18px 0"><tr><td style="padding:5px 0">Fila 1</td></tr><tr><td style="padding:5px 0">Fila 2</td></tr></table>')
BOTON = ('<p style="margin:14px 0 0"><a href="https://x.cl/a?b=1&amp;c=2" style="background:#dc2626;color:#fff;padding:10px 22px;border-radius:6px;'
         'text-decoration:none;font-weight:bold;display:inline-block">Ver la OT</a></p>')


class TestElFiltro(unittest.TestCase):
    def test_una_caja_con_relleno_y_margen_pasa_a_celdas(self):
        s = E.outlook_safe(CAJA)
        self.assertEqual(E.problemas(CAJA), ["table con padding", "table con margin"])
        self.assertEqual(E.problemas(s), [])
        self.assertIn('<td bgcolor="#f5f5f7" style="background:#f5f5f7;border-left:4px solid #dc2626;border-radius:4px;padding:14px 18px">', s)   # el relleno y la raya, en la celda
        self.assertIn('<!--[if mso]><table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0"><tr><td style="padding:18px 0"><![endif]-->', s)   # el espacio, SOLO para Outlook
        self.assertIn('style="margin:18px 0"', s)                                              # y el margen original se conserva: Gmail y el celular no cambian
        self.assertEqual(_texto(s), _texto(CAJA))
        a, c = _balance(s)
        self.assertEqual(a, c)
        self.assertIn('<td style="padding:5px 0">Fila 1</td>', s)                              # las filas, intactas

    def test_un_boton_con_relleno_en_el_enlace_pasa_a_celda(self):
        s = E.outlook_safe(BOTON)
        self.assertEqual(E.problemas(s), [])
        self.assertIn('<td align="center" bgcolor="#dc2626" style="background:#dc2626;border-radius:6px;padding:10px 22px">', s)
        self.assertIn('href="https://x.cl/a?b=1&amp;c=2"', s)                                # el enlace, tal cual (con su &amp;)
        self.assertIn("display:block", s)
        self.assertIn("font-size:13px", s)                                                    # ya no hereda el tamaño del párrafo (dentro de la celda)
        self.assertNotRegex(s, r'<a [^>]*padding')
        self.assertEqual(_texto(s), _texto(BOTON))

    def test_solo_margen_la_tabla_queda_igual_dentro_de_su_envoltorio(self):
        h = '<table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="margin:22px 0 4px"><tr><td>x</td></tr></table>'
        s = E.outlook_safe(h)
        self.assertEqual(E.problemas(s), [])
        self.assertIn('<td style="padding:22px 0 4px">', s)
        self.assertIn('style="margin:22px 0 4px"', s)                                         # la tabla queda IDÉNTICA; solo se le suma el envoltorio de Outlook
        a, c = _balance(s)
        self.assertEqual(a, c)                                                                 # balanceadas (la del envoltorio de Outlook va dentro de comentarios condicionales)

    def test_lo_que_ya_esta_bien_no_se_toca(self):
        bien = ('<table width="100%" style="background:#f6f7f9;border-left:4px solid #e30613"><tr><td style="padding:18px 20px">x</td></tr></table>'
                '<table style="margin:0 auto" width="620"><tr><td>c</td></tr></table><table style="padding:0;margin:0"><tr><td>z</td></tr></table>'
                '<a href="https://x.cl" style="color:#fff;text-decoration:none">Enlace común</a>'
                '<a href="https://x.cl" style="padding:8px;color:#111">Enlace con padding pero sin fondo</a>'
                '<a href="https://x.cl" style="padding:8px;background:linear-gradient(#000,#111)">Degradado</a>')
        self.assertEqual(E.outlook_safe(bien), bien)

    def test_es_idempotente(self):
        for h in (CAJA, BOTON, CAJA + BOTON + CAJA):
            una = E.outlook_safe(h)
            self.assertEqual(E.outlook_safe(una), una)

    def test_tablas_anidadas_y_varias_juntas_quedan_balanceadas(self):
        h = ('<table style="padding:10px;background:#eee"><tr><td><table style="padding:4px;margin:6px 0;background:#fff"><tr><td>a</td></tr></table></td></tr></table>'
             '<table><tr><td><table style="margin:5px 0"><tr><td>b</td></tr></table></td></tr></table>')
        s = E.outlook_safe(h)
        a, c = _balance(s)
        self.assertEqual(a, c)
        self.assertEqual(E.problemas(s), [])
        self.assertEqual(_texto(s), "a b")

    def test_html_roto_o_vacio_nunca_lanza(self):
        for h in ("", None, "<table style='padding:5px'>", "</table></table>", "<a style='padding:3px;background:#fff'>sin cerrar", 123):
            try:
                E.outlook_safe(h)
            except Exception as exc:                                                          # pragma: no cover
                self.fail(f"lanzó con {h!r}: {exc}")
        self.assertEqual(E.outlook_safe(""), "")
        self.assertIsNone(E.outlook_safe(None))

    def test_acepta_comillas_simples_y_mayusculas(self):
        h = "<TABLE STYLE='Padding:10px; BACKGROUND:#ABCDEF'><TR><TD>x</TD></TR></TABLE>"
        s = E.outlook_safe(h)
        self.assertEqual(E.problemas(s), [])
        self.assertIn('bgcolor="#ABCDEF"', s)

    def test_un_correo_grande_se_procesa_rapido(self):
        import time
        h = (CAJA + BOTON) * 400                                                              # ~250 KB
        t0 = time.time()
        s = E.outlook_safe(h)
        self.assertLess(time.time() - t0, 3.0)
        self.assertEqual(E.problemas(s), [])


class TestGmailYCelularNoCambian(unittest.TestCase):
    """Lo que pidió Daniel: «sin dañar las vistas de celular y de Gmail». En un Chromium real, el correo con y sin el filtro es IDÉNTICO píxel a píxel (700 y 390 px)."""

    def test_el_correo_se_ve_igual_con_y_sin_el_filtro(self):
        try:
            from playwright.sync_api import sync_playwright
            from PIL import Image, ImageChops
        except Exception:                                                                       # pragma: no cover
            self.skipTest("sin Playwright/PIL")
        import io
        v = {"ot": "OT-2026-00123", "cliente": "Gimnasio Demo SpA", "tecnico": "Pedro Pérez", "tecnicos": "Pedro Pérez", "fecha": "21/10/2026", "horario": "10:00 - 12:00",
             "direccion": "Av. Demo 1234, Las Condes", "tipo_mantencion": "Mantención preventiva", "maquina": "24 equipos", "saludo": "Hola María,", "dia": "mañana",
             "dias_vigencia": "30", "link_ot_firmada": "https://x.cl/f", "fecha_realizada": "21/10/2026",
             "recorrido_html": C.recorrido_html("recordatorio", "nuestro técnico llega en el horario indicado."),
             "que_esperar_html": O.que_esperar_html(["a"] * 24, ["Pedro Pérez"]), "preparacion_html": O.preparacion_html("mantencion"),
             "firma_sstt_html": C.firma_sstt_html(), "resumen_trabajo_html": C.resumen_trabajo_html({"equipos": ["a"] * 24, "tecnicos": ["Pedro Pérez"]})}
        cuerpos = [C.render_simple(C.SEMILLAS[C.PLANTILLA_RECORDATORIO][1], v),
                   C.render_simple(C.SEMILLAS[C.PLANTILLA_TERMINADA][1], dict(v, recorrido_html=C.recorrido_html("documento", ""))),]

        def marco(c):
            return ('<!doctype html><html lang="es"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"></head>'
                    '<body style="margin:0;background:#eef0f3;font-family:Arial,Helvetica,sans-serif;color:#15171a"><table role="presentation" width="100%" cellspacing="0" '
                    'cellpadding="0"><tr><td align="center" style="padding:24px 10px"><table role="presentation" width="620" cellspacing="0" cellpadding="0" '
                    'style="width:620px;max-width:100%;background:#fff"><tr><td style="padding:22px 40px">' + c + '</td></tr></table></td></tr></table></body></html>')
        with sync_playwright() as pw:
            nav = pw.chromium.launch()
            try:
                for i, c in enumerate(cuerpos):
                    for ancho in (700, 390):
                        fotos = []
                        for html in (marco(c), marco(E.outlook_safe(c))):
                            pg = nav.new_page(viewport={"width": ancho, "height": 900})
                            pg.set_content(html)
                            fotos.append(Image.open(io.BytesIO(pg.screenshot(full_page=True))).convert("RGB"))
                            pg.close()
                        self.assertEqual(fotos[0].size, fotos[1].size, f"correo {i} a {ancho}px: cambió el alto")
                        self.assertIsNone(ImageChops.difference(*fotos).getbbox(), f"correo {i} a {ancho}px: cambió algún píxel")
            finally:
                nav.close()


class TestSobreLosCorreosReales(unittest.TestCase):
    def test_los_bloques_y_plantillas_de_la_ot_quedan_limpios(self):
        bloques = {
            "tabla": C._tabla("#16a34a", C._FILAS),
            "boton": C._boton("Ver la orden de trabajo firmada", "https://x.cl/f"),
            "recorrido": C.recorrido_html("recordatorio", "sigue algo"),
            "resumen": C.resumen_trabajo_html({"equipos": ["a", "b"], "tecnicos": ["Pedro"]}),
            "que_va_a_pasar": O.que_esperar_html(["a"], ["Pedro"]),
            "como_llegar": O.como_llegar_html("Av. Demo 123"),
            "antes_de_salir": O.antes_de_salir_html(),
            "firma": C.firma_sstt_html(),
        }
        for slug, (_a, cuerpo) in list(C.SEMILLAS.items()) + list(C.SEMILLAS_EXTRA.items()):
            bloques["plantilla " + slug] = cuerpo
        for nombre, h in bloques.items():
            s = E.outlook_safe(h)
            self.assertEqual(E.problemas(s), [], nombre)
            self.assertEqual(_texto(s), _texto(h), nombre)
            a, c = _balance(s)
            self.assertEqual(a, c, nombre)
            self.assertEqual(E.outlook_safe(s), s, nombre)

    def test_los_pies_de_todo_correo_ya_son_botones_de_verdad_y_el_filtro_no_los_cambia(self):
        app = FC.texto("app.py")
        pie = re.search(r'<tr><td align="center" bgcolor="#e30613" style="background:#e30613;border-radius:6px;padding:14px 32px">.*?Crear ticket de soporte</a>\s*</td></tr></table>',
                        app, re.S)
        self.assertIsNotNone(pie)
        self.assertEqual(E.outlook_safe(pie.group(0)), pie.group(0))


class _Srv:
    enviados = []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def ehlo(self):
        pass

    def starttls(self):
        pass

    def login(self, *a):
        pass

    def sendmail(self, de, para, texto):
        _Srv.enviados.append((de, para, texto))


class TestElEnvioReal(unittest.TestCase):
    def setUp(self):
        _Srv.enviados = []
        import email.mime.application as _ma
        import email.mime.multipart as _mm
        import email.mime.text as _mt
        import os
        self.amb = {
            "os": os, "MIMEMultipart": _mm.MIMEMultipart, "MIMEText": _mt.MIMEText, "MIMEApplication": _ma.MIMEApplication,
            "_apply_test_redirect": lambda to, s: (to, s), "_get_marca": lambda: {"from_name": "ILUS", "from_email": "no-reply@sphs.cl", "reply_to": "soportetec@sphs.cl"},
            "_get_resend_cfg": lambda: {}, "_get_smtp_cfg": lambda: {"smtp_host": "h", "smtp_port": 587, "smtp_user": "u@sphs.cl", "smtp_pass": "x", "secure": False},
            "EMAIL_CONFIG": {}, "_open_smtp_client": lambda *a, **k: _Srv(), "_send_via_resend": lambda *a, **k: False,
            "g": type("G", (), {})(), "print": lambda *a, **k: None, "_eos": E,
        }
        FC.extraer(self.amb, "app.py", ("_send_ilus_email_real", "_email_normalize_attachments"), ())

    def _enviar(self, html, adjuntos=None):
        ok = self.amb["_send_ilus_email_real"]("cliente@gimnasio.cl", "Prueba", html, attachments=adjuntos)
        self.assertTrue(ok)
        self.assertEqual(len(_Srv.enviados), 1)
        return email.message_from_string(_Srv.enviados[0][2])

    def _ics(self, rsvp):
        ot = {"id": 5, "numero_ot": "OT-2026-00005", "cliente": "Demo", "tipo_label": "Instalación", "fecha": dt.date(2026, 11, 10),
              "hora_inicio": dt.timedelta(hours=10), "hora_fin": dt.timedelta(hours=12), "equipos": ["x"], "tecnicos": ["Lenin"]}
        return T.ics_evento(ot, "asignada", correo="cliente@gimnasio.cl", nombre="", ts=0, base_url="", organizador="soportetec@sphs.cl", rsvp=rsvp, sin_link=True)

    def _html_del(self, msg):
        for p in msg.walk():
            if p.get_content_type() == "text/html":
                return p.get_payload(decode=True).decode("utf-8")
        return ""

    def test_el_html_sale_filtrado_para_outlook(self):
        msg = self._enviar("<html><body>" + CAJA + BOTON + "</body></html>")
        h = self._html_del(msg)
        self.assertEqual(E.problemas(h), [])
        self.assertIn("Fila 1", h)

    def test_la_invitacion_al_cliente_sale_como_request_adjunta_y_en_linea(self):
        ics = self._ics(rsvp=True)
        msg = self._enviar("<html><body>x</body></html>", [("OT-2026-00005.ics", ics, "text/calendar")])
        cals = [p for p in msg.walk() if p.get_content_type() == "text/calendar"]
        self.assertEqual(len(cals), 2)                                                        # una en línea (alternative) y una adjunta
        for p in cals:
            self.assertEqual((p.get_param("method") or "").upper(), "REQUEST")
        adjuntas = [p for p in cals if "attachment" in (p.get("Content-Disposition") or "")]
        enlinea = [p for p in cals if "attachment" not in (p.get("Content-Disposition") or "")]
        self.assertEqual((len(adjuntas), len(enlinea)), (1, 1))
        self.assertEqual(enlinea[0].get_content_maintype(), "text")
        # la copia en línea va dentro del multipart/alternative, junto al HTML
        alternativa = [p for p in msg.walk() if p.get_content_type() == "multipart/alternative"][0]
        self.assertEqual(sorted(x.get_content_type() for x in alternativa.get_payload()), ["text/calendar", "text/html"])
        self.assertIn("METHOD:REQUEST", enlinea[0].get_payload(decode=True).decode("utf-8"))

    def test_la_del_tecnico_sin_rsvp_queda_como_estaba(self):
        ics = self._ics(rsvp=False)
        msg = self._enviar("<html><body>x</body></html>", [("OT.ics", ics, "text/calendar")])
        cals = [p for p in msg.walk() if p.get_content_type() == "text/calendar"]
        self.assertEqual(len(cals), 1)
        self.assertEqual((cals[0].get_param("method") or "").upper(), "PUBLISH")
        self.assertIn("attachment", cals[0].get("Content-Disposition") or "")

    def test_sin_adjuntos_el_correo_es_el_de_siempre(self):
        msg = self._enviar("<html><body>hola</body></html>")
        self.assertEqual(msg.get_content_type(), "multipart/alternative")
        self.assertEqual([p.get_content_type() for p in msg.get_payload()], ["text/html"])

    def test_un_adjunto_que_no_es_calendario_no_cambia(self):
        msg = self._enviar("<html><body>x</body></html>", [("detalle.pdf", b"%PDF-1.4 demo", "application/pdf")])
        tipos = sorted(p.get_content_type() for p in msg.walk() if not p.is_multipart())
        self.assertEqual(tipos, ["application/pdf", "text/html"])


class TestCableado(unittest.TestCase):
    def test_el_filtro_corre_antes_de_armar_el_mensaje_y_con_proteccion(self):
        app = FC.texto("app.py")
        self.assertIn("import email_outlook_safe as _eos", app)
        cod = FC.funcion_top("app.py", "_send_ilus_email_real")
        i = cod.index("html_body = _eos.outlook_safe(html_body)")
        self.assertLess(i, cod.index("att_norm = _email_normalize_attachments(attachments)"))
        self.assertLess(cod.rindex("try:", 0, i), i)
        self.assertIn("except Exception", cod[i:i + 200])


if __name__ == "__main__":
    unittest.main()

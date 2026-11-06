# -*- coding: utf-8 -*-
"""CORREOS AL CLIENTE «ORDENADOS, DE SERVICIO TÉCNICO, COMUNICATIVOS» (Daniel, 5-nov-2026).

Daniel: «dejemos listo los de los correos ordenados, disparando el Dpto de SSTT, con algo de Marketing, gestionando un buen servicio y comunicativo».

Lo que se hizo y se vigila:
  · «Tu servicio, paso a paso»: los correos al cliente (OT creada, recordatorio de 24 h, firma remota, OT terminada) muestran en qué punto del
    servicio está (agendada → recordatorio → visita y firma → documento firmado) y qué sigue. Lo que sigue SOLO se promete si de verdad va a salir
    (se mira el interruptor general, la llave de Servicio Técnico y el interruptor de cada correo).
  · Firma «Equipo de Servicio Técnico · ILUS Fitness».
  · «OT creada» (`visita_agendada`) trae qué va a pasar, cómo dejar todo listo y el aviso de la invitación de calendario; las plantillas se actualizan
    solas SOLO si nadie las editó.
Todo con datos de mentira (REGLA #22). Correr:  python -m pytest tests/test_ot_correos_recorrido.py -q -p no:warnings
"""
import datetime as dt
import re
import unittest

import comm_correos_registro as R
import ot_comunicacion as C
import ot_recordatorio_24h as O
from tests import _fuente_codigo as FC
from tests.test_ot_comunicacion_envio import MundoOT
from tests.test_ot_recordatorio_24h import MundoRec, LUN, PLATA, _t


# 6-nov-2026 (REGLA #67): el recorrido pasó a círculos numerados + barra de 4 px a prueba de Outlook; la barra de cada paso lleva su color.
_BARRA = "line-height:4px;font-size:0;background:"


def _verde(h):       # barras de los pasos ya hechos
    return h.count(_BARRA + "#16a34a")


def _rojo(h):        # la barra del paso actual
    return h.count(_BARRA + "#dc2626")


def _gris(h):        # barras de los pasos que faltan
    return h.count(_BARRA + "#d1d5db")


class TestRecorridoPuro(unittest.TestCase):
    def test_los_cuatro_pasos_en_orden(self):
        self.assertEqual([c for c, _n in C.PASOS_SERVICIO], ["agendada", "recordatorio", "visita", "documento"])
        h = C.recorrido_html("agendada")
        t = _t(h)
        for esperado in ("TU SERVICIO, PASO A PASO", "Visita agendada", "Recordatorio", "Visita y firma", "Documento firmado"):
            self.assertIn(esperado, t, esperado)
        self.assertEqual(h.count('<td width="25%" align="center" valign="top"'), 4)     # los 4 nombres
        for n in ("1", "2", "3", "4"):                                                   # y los 4 números en sus círculos
            self.assertIn(f'font-weight:700;mso-line-height-rule:exactly">{n}</td>', h)

    def test_el_paso_actual_se_resalta_los_anteriores_van_hechos_y_los_siguientes_pendientes(self):
        h = C.recorrido_html("recordatorio")
        self.assertEqual((_verde(h), _rojo(h), _gris(h)), (1, 1, 2))      # agendada hecha · recordatorio actual · visita y documento pendientes
        self.assertEqual(_t(h).count("✓"), 1)                             # un solo círculo con ✓ (el paso hecho)
        self.assertIn("Recordatorio", _t(h))
        h3 = C.recorrido_html("visita")
        self.assertEqual((_verde(h3), _rojo(h3), _gris(h3)), (2, 1, 1))

    def test_con_el_documento_firmado_los_cuatro_pasos_quedan_completos(self):
        h = C.recorrido_html("documento")
        self.assertEqual((_verde(h), _rojo(h), _gris(h)), (4, 0, 0))
        self.assertEqual(_t(h).count("✓"), 4)

    def test_un_paso_desconocido_no_dibuja_nada_y_lo_que_sigue_va_escapado(self):
        self.assertEqual(C.recorrido_html("otro"), "")
        self.assertEqual(C.recorrido_html(""), "")
        h = C.recorrido_html("agendada", "te enviaremos <b>algo</b> & más")
        self.assertIn("Qué sigue:", h)
        self.assertNotIn("<b>algo</b>", h)
        self.assertIn("&lt;b&gt;algo&lt;/b&gt; &amp; más", h)
        self.assertNotIn("Qué sigue", C.recorrido_html("agendada"))     # sin frase, no se inventa una

    def test_la_firma_del_equipo_y_la_invitacion(self):
        self.assertIn("Equipo de Servicio Técnico", C.firma_sstt_html())
        self.assertIn("Gracias por confiar en nosotros", _t(C.firma_sstt_html()))
        self.assertIn("calendario", C.invitacion_html())
        for h in (C.recorrido_html("agendada", "x"), C.firma_sstt_html(), C.invitacion_html()):
            self.assertNotRegex(_t(h), PLATA)


class TestPlantillas(unittest.TestCase):
    def test_los_correos_al_cliente_incluyen_el_recorrido_y_la_firma_y_el_del_tecnico_no(self):
        for slug in (C.PLANTILLA_TERMINADA, C.PLANTILLA_RECORDATORIO):
            cuerpo = C.SEMILLAS[slug][1]
            self.assertIn("{{recorrido_html}}", cuerpo, slug)
            self.assertIn("{{firma_sstt_html}}", cuerpo, slug)
        va = C.SEMILLAS_EXTRA[C.PLANTILLA_VISITA_AGENDADA][1]
        for tok in ("{{recorrido_html}}", "{{que_esperar_html}}", "{{preparacion_html}}", "{{firma_sstt_html}}", "{{invitacion_html}}"):
            self.assertIn(tok, va)
        for slug in (C.PLANTILLA_RECORDATORIO_TECNICO, C.PLANTILLA_TERMINADA_TECNICO):
            self.assertNotIn("{{recorrido_html}}", C.SEMILLAS[slug][1], slug)           # el técnico no recibe el recorrido del cliente
            self.assertNotIn("{{firma_sstt_html}}", C.SEMILLAS[slug][1], slug)

    def test_las_variables_nuevas_existen_en_el_diccionario_de_la_ot_y_en_la_vista_previa(self):
        v = C.variables_ot({"id": 1, "numero_ot": "OT-1", "cliente": "X", "equipos": [], "tecnicos": []}, recorrido=C.recorrido_html("agendada"))
        for k in ("recorrido_html", "firma_sstt_html", "invitacion_html"):
            self.assertIn(k, v)
        self.assertTrue(v["recorrido_html"] and v["firma_sstt_html"])
        self.assertEqual(v["invitacion_html"], "")
        for k in ("recorrido_html", "firma_sstt_html", "invitacion_html"):
            self.assertIn(k, R.DEMO_EXTRA, k)

    def test_todas_las_variables_de_las_plantillas_nuevas_estan_cubiertas(self):
        armadas = set(C.variables_ot({"id": 1, "numero_ot": "OT-1", "cliente": "X", "equipos": [], "tecnicos": []}))
        todas = dict(C.SEMILLAS)
        todas.update(C.SEMILLAS_EXTRA)
        for slug, (a, c) in todas.items():
            usadas = set(re.findall(r"\{\{(\w+)\}\}", a + c))
            # las de «OT creada» además llevan las del diccionario de la creación (ot, cliente, tecnico, fecha, horario, direccion, tipo_mantencion, maquina)
            extra = {"ot", "cliente", "tecnico", "fecha", "horario", "direccion", "tipo_mantencion", "maquina", "link_ot"}
            self.assertLessEqual(usadas, armadas | extra, slug)

    def test_las_versiones_anteriores_se_conservan_para_poder_actualizar_solo_lo_que_nadie_edito(self):
        self.assertEqual(len(C.SEMILLAS_ANTERIORES[C.PLANTILLA_TERMINADA]), 5)         # v1, v2, v3 (decía «responde este correo»), v4 (ticket, frase larga) y v5 (frase corta, sin la encuesta de la REGLA #68)
        self.assertEqual(len(C.SEMILLAS_ANTERIORES[C.PLANTILLA_RECORDATORIO]), 4)
        for slug in (C.PLANTILLA_TERMINADA, C.PLANTILLA_RECORDATORIO):
            for ant in C.SEMILLAS_ANTERIORES[slug]:
                self.assertNotEqual(ant[1], C.SEMILLAS[slug][1], slug)
            # solo las v1 y v2 son anteriores al recorrido; la v3 ya lo traía (lo que cambió en la v4 es el pie: ticket en vez de «responde»)
            self.assertNotIn("{{recorrido_html}}", "".join(a[1] for a in C.SEMILLAS_ANTERIORES[slug][:2]))
            self.assertIn("{{recorrido_html}}", C.SEMILLAS_ANTERIORES[slug][2][1])


class TestActualizacionDeLaPlantillaDeOtCreada(unittest.TestCase):
    """`visita_agendada` es la plantilla más antigua: su semilla original vive en app.py (`_mantenciones_tpl_seed`)."""

    def _mundo(self):
        w = MundoOT()
        self.addCleanup(w.cerrar)
        orig = w.amb["mysql_fetchone"]

        def con_id(sql, params=()):
            r = orig(sql, params)
            if r is not None and "FROM comm_templates" in sql and "id" not in r:
                r = dict(r, id=abs(hash(tuple(params))) % 10_000)
            return r
        w.amb["mysql_fetchone"] = con_id
        FC.extraer(w.amb, "app.py", ("_mantenciones_tpl_seed",), ())
        return w

    def _ups(self, w):
        return [(s, p) for s, p in w.sql if s.startswith("UPDATE comm_templates SET asunto")]

    def test_si_sigue_igual_a_la_semilla_original_pasa_a_la_version_nueva(self):
        w = self._mundo()
        orig = w.amb["_mantenciones_tpl_seed"]()["visita_agendada"]
        w.bd.plantillas[(C.MODULO_PLANTILLAS, "visita_agendada")] = {"asunto": orig[0], "cuerpo": orig[1], "activo": 1}
        w.f("_ensure_comm_template_ot_comunicacion")()
        ups = self._ups(w)
        self.assertEqual(len(ups), 1, ups)
        (s, p), = ups
        self.assertEqual((p[0], p[1], p[2]), (C.SEMILLAS_EXTRA["visita_agendada"][0], C.SEMILLAS_EXTRA["visita_agendada"][1], "sistema"))
        self.assertIn("AND cuerpo=%s", s)

    def test_si_alguien_la_edito_en_comunicaciones_no_se_toca(self):
        w = self._mundo()
        orig = w.amb["_mantenciones_tpl_seed"]()["visita_agendada"]
        w.bd.plantillas[(C.MODULO_PLANTILLAS, "visita_agendada")] = {"asunto": orig[0], "cuerpo": orig[1] + "<p>Texto de Daniel</p>", "activo": 1}
        w.f("_ensure_comm_template_ot_comunicacion")()
        self.assertEqual(self._ups(w), [])

    def test_la_terminada_y_el_recordatorio_en_v2_pasan_a_la_v3(self):
        w = self._mundo()
        for slug, ant in ((C.PLANTILLA_TERMINADA, C._TERMINADA_V2), (C.PLANTILLA_RECORDATORIO, C._RECORDATORIO_V2)):
            w.bd.plantillas[(C.MODULO_PLANTILLAS, slug)] = {"asunto": ant[0], "cuerpo": ant[1], "activo": 1}
        w.f("_ensure_comm_template_ot_comunicacion")()
        cuerpos = {p[1] for s, p in self._ups(w)}
        self.assertIn(C.SEMILLAS[C.PLANTILLA_TERMINADA][1], cuerpos)
        self.assertIn(C.SEMILLAS[C.PLANTILLA_RECORDATORIO][1], cuerpos)

    def test_si_la_semilla_original_no_se_puede_leer_las_demas_siguen(self):
        w = self._mundo()
        w.amb["_mantenciones_tpl_seed"] = lambda: (_ for _ in ()).throw(RuntimeError("sin semilla"))
        w.bd.plantillas[(C.MODULO_PLANTILLAS, C.PLANTILLA_RECORDATORIO)] = {"asunto": C._RECORDATORIO_V2[0], "cuerpo": C._RECORDATORIO_V2[1], "activo": 1}
        w.f("_ensure_comm_template_ot_comunicacion")()
        self.assertEqual(len(self._ups(w)), 1)


class TestCorreosDeExtremoAExtremo(unittest.TestCase):
    def _mundo(self, **kw):
        w = MundoRec(**kw)
        self.addCleanup(w.cerrar)
        # el recordatorio usa la plantilla del código (v3) cuando el editor no tiene una propia: ya es el caso del mundo de mentira
        w.amb["_ot_com_promesas"] = lambda: {"recordatorio": True, "documento": True}
        return w

    def test_el_recordatorio_al_cliente_trae_el_recorrido_en_el_paso_2_y_la_firma(self):
        w = self._mundo()
        w.barrer(LUN(10, 5))
        h = w.para("contacto@gimnasio.cl")[0]["html"]
        t = _t(h)
        for esperado in ("TU SERVICIO, PASO A PASO", "Visita agendada", "Recordatorio", "Visita y firma", "Documento firmado",
                         "Equipo de Servicio Técnico", "Gracias por confiar en nosotros", "Ver seguimiento en vivo"):
            self.assertIn(esperado, t, esperado)
        self.assertEqual((_verde(h), _rojo(h), _gris(h)), (1, 1, 2))      # paso 2 de 4
        self.assertIn('href="https://ilus.test/ot-seguimiento/SEG77"', h)        # el botón lleva al seguimiento de ESTA OT
        # el técnico NO recibe el recorrido del cliente ni su firma
        th = w.para("lenin@sphs.cl")[0]["html"]
        self.assertNotIn("TU SERVICIO, PASO A PASO", th)
        self.assertNotIn("Equipo de Servicio Técnico", _t(th))

    def test_el_correo_de_ot_creada_trae_todo_y_la_invitacion_solo_si_va_adjunta(self):
        w = self._mundo()
        FC.extraer(w.amb, "app.py", ("_ot_com_bloques_creacion", "_ot_cliente_ics_fn"), ())
        w.amb.update({"_orec": O, "_get_brand_cfg": lambda: {"reply_to": "soportetec@sphs.cl"}, "time": __import__("time")})
        w.amb["_ot_com_promesas"] = lambda: {"recordatorio": True, "documento": True}
        v = {"id": 77, "numero_ot": "OT-2026-00077", "razon_social": "Gimnasio Demo SpA", "tipo": "instalacion", "fecha_programada": dt.date(2026, 11, 10)}
        variables = {"tecnico": "Lenin Urbina, Pedro Pérez", "maquina": "Trotadora ILUS Pro 9000, Bicicleta ILUS"}
        b = w.amb["_ot_com_bloques_creacion"](v, variables, con_invitacion=True)
        t = _t(b["que_esperar_html"])
        self.assertIn("confirmar la recepción del trabajo con tu firma", t)                    # (6-nov, auditoría: «Llegará…» y «Trabajará en N equipos» ya están en la tabla)
        self.assertNotIn("Trabajará", t)
        self.assertIn("camino de acceso", _t(b["preparacion_html"]))                 # instalación
        self.assertIn("te enviaremos un recordatorio 24 horas antes de la visita", _t(b["recorrido_html"]))
        self.assertIn("calendario", b["invitacion_html"])
        sin = w.amb["_ot_com_bloques_creacion"](v, variables, con_invitacion=False)
        self.assertEqual(sin["invitacion_html"], "")

    def test_lo_que_sigue_solo_se_promete_si_va_a_salir(self):
        w = self._mundo()
        FC.extraer(w.amb, "app.py", ("_ot_com_bloques_creacion", "_ot_com_promesas"), ())
        w.amb["_orec"] = O
        v = {"id": 77, "tipo": "preventiva"}
        variables = {"tecnico": "Lenin", "maquina": "—"}
        # recordatorio encendido → lo promete
        b = w.amb["_ot_com_bloques_creacion"](v, variables)
        self.assertIn("recordatorio 24 horas antes", _t(b["recorrido_html"]))
        # el interruptor del recordatorio apagado en Control de correos → ya no lo promete
        w.apagar_correo("ot_recordatorio_cliente")
        b2 = w.amb["_ot_com_bloques_creacion"](v, variables)
        self.assertNotIn("recordatorio", _t(b2["recorrido_html"]).split("Qué sigue:")[-1])
        self.assertIn("nuestro técnico llegará en el horario indicado", _t(b2["recorrido_html"]))
        # la llave de Servicio Técnico cerrada tampoco
        w3 = self._mundo()
        FC.extraer(w3.amb, "app.py", ("_ot_com_bloques_creacion", "_ot_com_promesas"), ())
        w3.amb["_orec"] = O
        w3.cerrar_llave("mantenciones")
        self.assertEqual(w3.amb["_ot_com_promesas"](), {"recordatorio": False, "documento": False})

    def test_la_ot_terminada_muestra_los_cuatro_pasos_completos(self):
        w = MundoOT(env_cliente="1")
        self.addCleanup(w.cerrar)
        w.f("_ot_terminada_notificar")(77)
        h = next(e["html"] for e in w.m.enviados_reales if e["to"] == "contacto@gimnasio.cl")
        self.assertEqual(_verde(h), 4)
        t = _t(h)
        self.assertIn("Equipo de Servicio Técnico", t)
        self.assertIn("Documento firmado", t)
        self.assertIn('href="https://ilus.test/ot-seguimiento/SEG77"', h)
        th = next(e["html"] for e in w.m.enviados_reales if e["to"] == "lenin@sphs.cl")
        self.assertNotIn("TU SERVICIO, PASO A PASO", th)


class TestCableado(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = FC.texto("app.py")

    def test_la_creacion_arma_los_bloques_y_la_invitacion_una_sola_vez(self):
        f = FC.funcion_top("app.py", "_mant_ot_email_cliente_creacion")
        self.assertIn("_ics_fn = _ot_cliente_ics_fn(v, variables)", f)
        self.assertIn("_ot_com_bloques_creacion(v, variables, con_invitacion=bool(_ics_fn))", f)
        self.assertIn("attachments=_ics_fn", f)
        self.assertEqual(f.count("_ot_cliente_ics_fn("), 1)

    def test_la_firma_remota_muestra_el_paso_3_y_promete_solo_lo_que_saldra(self):
        f = FC.funcion_top("app.py", "mant_ot_enviar_firma_remota")
        self.assertRegex(f, r'_ocom\.recorrido_html\(\s*"visita"')
        self.assertIn("_ot_com_promesas()", f)

    def test_la_ot_terminada_pasa_el_recorrido_completo(self):
        f = FC.funcion_top("app.py", "_ot_terminada_notificar")
        self.assertIn('recorrido=_ocom.recorrido_html("documento", url_seguimiento=_ot_seg_url(v_id))', f)


if __name__ == "__main__":
    unittest.main()

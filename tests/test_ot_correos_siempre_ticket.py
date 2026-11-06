# -*- coding: utf-8 -*-
"""CORREOS AL CLIENTE DE LA OT: «SIEMPRE CREAR UN TICKET» (Daniel, 5-nov-2026).

Daniel, tras probar un correo: «dice que responda al correo y lo que sí o sí debe hacer es crear un ticket, siempre crear un ticket».

Qué se vigila:
  · Los correos de la OT que le llegan al CLIENTE (OT creada, recordatorio de 24 h, OT terminada y OT completada del botón «Enviar email»)
    le indican crear un ticket de soporte con el botón «Crear ticket de soporte» del pie del correo, y NUNCA «responde este correo».
  · Las plantillas que ya estaban sembradas con el texto de antes (v3 / v2 / la semilla original) y que nadie editó pasan solas a la versión nueva; una
    que una persona editó en Comunicaciones queda como la dejó.
  · Al TÉCNICO no se le habla de tickets (no los crea: avisa a su coordinador).
Todo con datos de mentira (REGLA #22). Correr:  python -m pytest tests/test_ot_correos_siempre_ticket.py -q -p no:warnings
"""
import re
import unittest

import ot_comunicacion as C
from tests import _fuente_codigo as FC
from tests.test_ot_comunicacion_envio import MundoOT
from tests.test_ot_recordatorio_24h import MundoRec, LUN, _t

_RESPONDER = re.compile(r"respond[ae]\b[^.<]{0,40}correo", re.IGNORECASE)
_CLIENTE = (C.PLANTILLA_TERMINADA, C.PLANTILLA_RECORDATORIO, C.PLANTILLA_VISITA_AGENDADA, C.PLANTILLA_OT_COMPLETADA)


def _semilla(slug):
    return C.SEMILLAS[slug] if slug in C.SEMILLAS else C.SEMILLAS_EXTRA[slug]


class TestElTextoVigente(unittest.TestCase):
    def test_los_cuatro_correos_al_cliente_mandan_al_ticket_y_no_a_responder(self):
        for slug in _CLIENTE:
            asunto, cuerpo = _semilla(slug)
            t = _t(cuerpo)
            self.assertIn("crea un ticket de soporte con el botón de abajo", t.lower(), slug)       # el botón es el del pie de TODO correo (6-nov: frase corta)
            self.assertIsNone(_RESPONDER.search(t), (slug, _RESPONDER.search(t)))
            self.assertNotIn("contacta a tu ejecutivo", t, slug)                 # tampoco una salida paralela al ticket

    def test_la_frase_es_una_sola_para_todas_las_plantillas(self):
        for slug in _CLIENTE:
            _a, cuerpo = _semilla(slug)
            # la frase base (con la primera letra en minúscula o mayúscula según vaya al inicio de la oración)
            base = C.COMO_CREAR_TICKET[1:]
            self.assertIn(base, cuerpo, slug)
        self.assertLessEqual(len(C.COMO_CREAR_TICKET), 60)                       # Daniel (6-nov): «no me gusta tanta letra»

    def test_el_tecnico_no_recibe_el_aviso_de_tickets(self):
        _a, cuerpo = C.SEMILLAS[C.PLANTILLA_RECORDATORIO_TECNICO]
        self.assertNotIn("ticket", _t(cuerpo).lower())
        self.assertIn("coordinador de Servicio Técnico", _t(cuerpo))

    def test_el_recordatorio_se_lee_bien_de_corrido(self):
        t = _t(C.SEMILLAS[C.PLANTILLA_RECORDATORIO][1])
        self.assertIn("¿Necesitas cambiar la fecha o el acceso? Crea un ticket de soporte con el botón de abajo.", t)

    def test_la_terminada_y_la_creada_se_leen_bien_de_corrido(self):
        t = _t(C.SEMILLAS[C.PLANTILLA_TERMINADA][1])
        self.assertIn("¿Algo no calza? Crea un ticket de soporte con el botón de abajo.", t)
        t = _t(C.SEMILLAS_EXTRA[C.PLANTILLA_VISITA_AGENDADA][1])
        self.assertIn("¿Necesitas reagendar o cancelar? Crea un ticket de soporte con el botón de abajo.", t)
        t = _t(C.SEMILLAS_EXTRA[C.PLANTILLA_OT_COMPLETADA][1])
        self.assertIn("Si tienes alguna observación, crea un ticket de soporte con el botón de abajo.", t)

    def test_el_correo_armado_con_datos_no_deja_variables_sueltas_ni_la_frase_vieja(self):
        v = C.variables_ot({"id": 1, "numero_ot": "OT-1", "cliente": "Gimnasio Demo", "equipos": ["Trotadora"], "tecnicos": ["Lenin"]})
        for slug in (C.PLANTILLA_TERMINADA, C.PLANTILLA_RECORDATORIO):
            _a, cuerpo = C.armar_correo(slug, v)
            self.assertNotIn("{{", cuerpo, slug)
            self.assertIsNone(_RESPONDER.search(_t(cuerpo)), slug)


class TestLasVersionesAnterioresSeConservan(unittest.TestCase):
    """La cadena de versiones es lo que permite actualizar SOLO lo que nadie editó: cada versión que alguna vez se sembró tiene que seguir ahí."""

    def test_la_v3_de_terminada_y_recordatorio_es_la_que_decia_responde(self):
        for slug, v3 in ((C.PLANTILLA_TERMINADA, C._TERMINADA_V3), (C.PLANTILLA_RECORDATORIO, C._RECORDATORIO_V3)):
            self.assertIn(v3, C.SEMILLAS_ANTERIORES[slug])
            self.assertIsNotNone(_RESPONDER.search(_t(v3[1])), slug)
            self.assertNotEqual(v3[1], C.SEMILLAS[slug][1], slug)
            # lo único que cambia entre la v3 y la v4 es el pie (el resto del correo —recorrido, firma, tablas— es idéntico)
            # (6-nov, REGLA #68: la vigente de «OT terminada» suma la variable `{{encuesta_html}}`; tampoco cuenta para esta comparación)
            sin_pie = lambda s: re.sub(r'<p style="margin:18px 0 0;font-size:12px;color:#888;line-height:1.5">.*?</p>', "<pie/>", s, flags=re.S).replace("{{encuesta_html}}", "")
            self.assertEqual(sin_pie(v3[1]), sin_pie(C.SEMILLAS[slug][1]), slug)

    def test_la_v2_de_ot_creada_decia_responde_y_esta_en_la_cadena_extra(self):
        self.assertEqual(C.SEMILLAS_EXTRA_ANTERIORES[C.PLANTILLA_VISITA_AGENDADA], (C._VISITA_AGENDADA_V2, C._VISITA_AGENDADA_V3))
        self.assertIsNotNone(_RESPONDER.search(_t(C._VISITA_AGENDADA_V2[1])))
        self.assertIsNone(_RESPONDER.search(_t(C._VISITA_AGENDADA_V3[1])))                      # la v3 ya mandaba al ticket, con la frase larga

    def test_las_semillas_extra_nuevas_estan_en_el_orden_y_con_las_variables_correctas(self):
        self.assertEqual(set(C.SEMILLAS_EXTRA), {C.PLANTILLA_VISITA_AGENDADA, C.PLANTILLA_OT_COMPLETADA})
        # la «OT completada» NO es una plantilla que este módulo siembre como nueva (la siembra `_ensure_comm_template_mantenciones`)
        self.assertNotIn(C.PLANTILLA_OT_COMPLETADA, C.PLANTILLAS)


class TestLaOtCompletadaSigueSiendoLaOriginalSalvoElTicket(unittest.TestCase):
    def test_con_el_texto_de_antes_es_identica_a_la_semilla_original_de_app_py(self):
        w = MundoOT()
        self.addCleanup(w.cerrar)
        FC.extraer(w.amb, "app.py", ("_mantenciones_tpl_seed",), ())
        asunto, cuerpo = w.amb["_mantenciones_tpl_seed"]()["ot_completada"]
        antes = C._cuerpo_ot_completada("Si tienes alguna observación, responde este correo y te contactaremos a la brevedad.")
        n = lambda s: s.replace("\r\n", "\n").strip()
        self.assertEqual(n(antes), n(cuerpo))
        self.assertEqual(asunto, C._OT_COMPLETADA_MARCA_ANTIGUA[0])                           # el asunto ORIGINAL decía «OT {{ot}}» (6-nov: ya no: el número trae «OT-»)
        self.assertEqual(C.SEMILLAS_EXTRA[C.PLANTILLA_OT_COMPLETADA][0], "{{ot}} completada — Reporte adjunto")
        # y la nueva solo cambia la última frase
        nuevo = C.SEMILLAS_EXTRA[C.PLANTILLA_OT_COMPLETADA][1]
        self.assertEqual(n(nuevo).split("Gracias por confiar")[0], n(cuerpo).split("Gracias por confiar")[0])
        self.assertNotEqual(n(nuevo), n(cuerpo))


class TestLaActualizacionAlArrancar(unittest.TestCase):
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

    def _poner(self, w, slug, asunto, cuerpo):
        w.bd.plantillas[(C.MODULO_PLANTILLAS, slug)] = {"asunto": asunto, "cuerpo": cuerpo, "activo": 1}

    def test_la_terminada_y_el_recordatorio_en_v3_pasan_a_la_v4(self):
        w = self._mundo()
        self._poner(w, C.PLANTILLA_TERMINADA, *C._TERMINADA_V3)
        self._poner(w, C.PLANTILLA_RECORDATORIO, *C._RECORDATORIO_V3)
        w.f("_ensure_comm_template_ot_comunicacion")()
        cuerpos = {p[1] for _s, p in self._ups(w)}
        self.assertIn(C.SEMILLAS[C.PLANTILLA_TERMINADA][1], cuerpos)
        self.assertIn(C.SEMILLAS[C.PLANTILLA_RECORDATORIO][1], cuerpos)
        for s, p in self._ups(w):
            self.assertIn("AND cuerpo=%s", s)
            self.assertEqual(p[2], "sistema")

    def test_las_versiones_de_ayer_con_la_frase_larga_pasan_a_la_corta(self):
        """6-nov (Daniel: «no me gusta tanta letra»): lo que quedó sembrado con la frase larga del ticket (#283–#286) pasa al pie corto si nadie lo editó."""
        w = self._mundo()
        self._poner(w, C.PLANTILLA_TERMINADA, *C._TERMINADA_V4)
        self._poner(w, C.PLANTILLA_RECORDATORIO, *C._RECORDATORIO_V4)
        self._poner(w, C.PLANTILLA_VISITA_AGENDADA, *C._VISITA_AGENDADA_V3)
        self._poner(w, C.PLANTILLA_OT_COMPLETADA, *C._OT_COMPLETADA_V2)
        w.f("_ensure_comm_template_ot_comunicacion")()
        nuevos = {p[1] for _s, p in self._ups(w)}
        for slug in (C.PLANTILLA_TERMINADA, C.PLANTILLA_RECORDATORIO, C.PLANTILLA_VISITA_AGENDADA, C.PLANTILLA_OT_COMPLETADA):
            self.assertIn(_semilla(slug)[1], nuevos, slug)
        self.assertEqual(len(self._ups(w)), 4)
        # el asunto de «OT completada» también pierde el «OT» doble
        asuntos = {p[0] for _s, p in self._ups(w)}
        self.assertIn("{{ot}} completada — Reporte adjunto", asuntos)

    def test_la_ot_creada_en_v2_pasa_a_la_v3(self):
        w = self._mundo()
        self._poner(w, C.PLANTILLA_VISITA_AGENDADA, *C._VISITA_AGENDADA_V2)
        w.f("_ensure_comm_template_ot_comunicacion")()
        ups = self._ups(w)
        self.assertEqual(len(ups), 1, ups)
        self.assertEqual(ups[0][1][1], C.SEMILLAS_EXTRA[C.PLANTILLA_VISITA_AGENDADA][1])

    def test_la_ot_creada_original_de_app_py_sigue_actualizandose(self):
        w = self._mundo()
        orig = w.amb["_mantenciones_tpl_seed"]()["visita_agendada"]
        self._poner(w, C.PLANTILLA_VISITA_AGENDADA, *orig)
        w.f("_ensure_comm_template_ot_comunicacion")()
        ups = self._ups(w)
        self.assertEqual(len(ups), 1, ups)
        self.assertEqual(ups[0][1][1], C.SEMILLAS_EXTRA[C.PLANTILLA_VISITA_AGENDADA][1])

    def test_la_ot_completada_original_pasa_a_la_nueva(self):
        w = self._mundo()
        orig = w.amb["_mantenciones_tpl_seed"]()["ot_completada"]
        self._poner(w, C.PLANTILLA_OT_COMPLETADA, *orig)
        w.f("_ensure_comm_template_ot_comunicacion")()
        ups = self._ups(w)
        self.assertEqual(len(ups), 1, ups)
        self.assertEqual(ups[0][1][1], C.SEMILLAS_EXTRA[C.PLANTILLA_OT_COMPLETADA][1])

    def test_la_ot_completada_con_la_marca_antigua_de_produccion_tambien_pasa_a_la_nueva(self):
        """En producción la fila quedó con «ILUS Sport & Health» (la semilla anterior al rebrand): la primera versión de este cambio no la reconocía."""
        w = self._mundo()
        self._poner(w, C.PLANTILLA_OT_COMPLETADA, *C._OT_COMPLETADA_MARCA_ANTIGUA)
        self.assertIn("ILUS Sport & Health", C._OT_COMPLETADA_MARCA_ANTIGUA[1])
        w.f("_ensure_comm_template_ot_comunicacion")()
        ups = self._ups(w)
        self.assertEqual(len(ups), 1, ups)
        nuevo = ups[0][1][1]
        self.assertEqual(nuevo, C.SEMILLAS_EXTRA[C.PLANTILLA_OT_COMPLETADA][1])
        self.assertIn("ILUS Fitness", nuevo)
        self.assertNotIn("Sport & Health", nuevo)

    def test_la_ot_completada_original_de_produccion_servicio_de_mantencion_tambien_pasa_a_la_nueva(self):
        """La fila REAL de producción (verificada en solo lectura) dice «servicio de mantención» e «ILUS Sport & Health»: la primera y la segunda versión de este
        cambio no la reconocían y se quedaba en «responde este correo»."""
        w = self._mundo()
        self._poner(w, C.PLANTILLA_OT_COMPLETADA, *C._OT_COMPLETADA_ORIGINAL_DE_PRODUCCION)
        self.assertIn("servicio de mantención se ha completado", C._OT_COMPLETADA_ORIGINAL_DE_PRODUCCION[1])
        self.assertIn("ILUS Sport & Health", C._OT_COMPLETADA_ORIGINAL_DE_PRODUCCION[1])
        w.f("_ensure_comm_template_ot_comunicacion")()
        ups = self._ups(w)
        self.assertEqual(len(ups), 1, ups)
        self.assertEqual(ups[0][1][1], C.SEMILLAS_EXTRA[C.PLANTILLA_OT_COMPLETADA][1])

    def test_la_original_de_produccion_mide_6_caracteres_mas_que_la_marca_antigua(self):
        # el dato medido en la fila real: 1216 caracteres contra 1210 del texto «servicio técnico» (la diferencia son justo estas 6 letras)
        n = lambda s: s.replace("\r\n", "\n").strip()
        self.assertEqual(len(n(C._OT_COMPLETADA_ORIGINAL_DE_PRODUCCION[1])) - len(n(C._OT_COMPLETADA_MARCA_ANTIGUA[1])), 6)
        self.assertEqual(len(n(C._OT_COMPLETADA_ORIGINAL_DE_PRODUCCION[1])), 1216)

    def test_la_marca_antigua_es_la_semilla_original_salvo_el_nombre(self):
        w = self._mundo()
        orig = w.amb["_mantenciones_tpl_seed"]()["ot_completada"]
        n = lambda s: s.replace("\r\n", "\n").strip()
        self.assertEqual(n(C._OT_COMPLETADA_MARCA_ANTIGUA[1]).replace("ILUS Sport & Health", "ILUS Fitness"), n(orig[1]))

    def test_lo_que_una_persona_edito_no_se_toca(self):
        w = self._mundo()
        for slug, v in ((C.PLANTILLA_TERMINADA, C._TERMINADA_V3), (C.PLANTILLA_RECORDATORIO, C._RECORDATORIO_V3),
                        (C.PLANTILLA_VISITA_AGENDADA, C._VISITA_AGENDADA_V2),
                        (C.PLANTILLA_OT_COMPLETADA, w.amb["_mantenciones_tpl_seed"]()["ot_completada"])):
            self._poner(w, slug, v[0], v[1] + "<p>Texto de Daniel</p>")
        w.f("_ensure_comm_template_ot_comunicacion")()
        self.assertEqual(self._ups(w), [])

    def test_una_plantilla_ya_en_la_version_vigente_no_se_vuelve_a_escribir(self):
        w = self._mundo()
        for slug in _CLIENTE:
            self._poner(w, slug, *_semilla(slug))
        w.f("_ensure_comm_template_ot_comunicacion")()
        self.assertEqual(self._ups(w), [])


class TestElCorreoQueLlegaAlCliente(unittest.TestCase):
    def test_el_recordatorio_de_24_horas_que_sale_lleva_el_ticket(self):
        w = MundoRec()
        self.addCleanup(w.cerrar)
        w.amb["_ot_com_promesas"] = lambda: {"recordatorio": True, "documento": True}
        w.barrer(LUN(10, 5))
        enviados = w.para("contacto@gimnasio.cl")
        self.assertTrue(enviados)
        t = _t(enviados[0]["html"])
        self.assertIn("Crea un ticket de soporte con el botón de abajo", t)
        self.assertIsNone(_RESPONDER.search(t))
        # el técnico recibe el suyo, sin tickets
        th = _t(w.para("lenin@sphs.cl")[0]["html"])
        self.assertNotIn("ticket", th.lower())


class TestLosTextosDeApp(unittest.TestCase):
    def test_los_textos_de_respaldo_de_la_ot_tampoco_dicen_responde(self):
        for nombre in ("mant_visita_enviar_email", "_mant_ot_email_cliente_creacion"):
            codigo = FC.funcion_top("app.py", nombre)
            self.assertIn("Crear ticket de soporte", codigo, nombre)
            self.assertIsNone(re.search(r"respond[ae]\s+este\s+correo", codigo, re.IGNORECASE), nombre)


if __name__ == "__main__":
    unittest.main()

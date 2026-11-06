# -*- coding: utf-8 -*-
"""NINGÚN CORREO AL CLIENTE DICE «RESPONDE ESTE CORREO»: TODO VA POR TICKET (Daniel, 6-nov-2026).

Daniel, con la captura de un recordatorio: «nunca digas que responden al correo, solo por ticket. Nunca se responde al correo, nunca. Nadie va a responder
si responden… Hay que reparar eso de manera urgente: elimina responder este correo y coloca un botón para que se vayan a registrar un ticket.»

Qué se vigila:
  · Cada frase que HOY está en las plantillas de producción (inventario del 6-nov, leído en solo lectura) se cambia por «crea un ticket de soporte con el botón
    de abajo», sin tocar el resto del texto.
  · El arranque (`_ensure_correos_sin_responder`) lo hace sobre las filas guardadas: solo cambia esas frases, una plantilla editada conserva sus cambios,
    `tickets` y `comunicacion_interna` no se tocan, y es idempotente.
  · Una GUARDA recorre el código y falla si alguien vuelve a escribir «responde este correo» en un correo al cliente.
  · El botón del pie de todo correo es un botón de verdad (relleno en la celda: Outlook ignora el padding de un <a>).
Todo con datos de mentira (REGLA #22). Correr:  python -m pytest tests/test_correos_sin_responder.py -q -p no:warnings
"""
import os
import re
import unittest

import correos_sin_responder as CSR
from tests import _fuente_codigo as FC

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Las frases tal cual están HOY en las plantillas de producción (inventario del 6-nov).
EN_PRODUCCION = {
    "retiros/agenda_confirmada": "¿Necesitas reagendar o tienes una duda? Responde este correo y te ayudamos.",
    "retiros/fallida": "te contactaremos para coordinar una nueva fecha; también puedes responder este correo y te ayudamos a la brevedad.",
    "retiros/informacion_incompleta": "Te contactaremos a la brevedad para confirmar qué falta. También puedes responder este correo con cualquier dato que te haya faltado.",
    "tickets/resuelto": "Si el problema persiste, responde este correo y retomamos tu caso.",
    "retiros/rechazada (título)": "Si fue un error, escríbenos respondiendo este correo",
    "retiros/rechazada (cuerpo)": "Si fue un error o quieres reagendar, escríbenos respondiendo este correo y te ayudamos a la brevedad.",
    "retiros/recordatorio_24h": "¿Imprevistos? Responde este correo o llámanos. Es mejor avisar a no llegar.",
    "retiros/retirada": "¿Necesitas asesoría o servicio post-venta? Responde este correo y te ayudamos.",
    "mantenciones/agenda_tecnico_manana": "Si algo no calza (dirección, horario o equipo), responde este correo antes de salir.",
    "mantenciones/plan_propuesto": "Para confirmar o ajustar fechas, responda este correo y coordinamos la agenda con su ejecutivo.",
    "mantenciones/prospecto_oferta": "Responde este correo o escríbenos y te contactamos para armar un plan a la medida de tu centro.",
    "mantenciones/plan_mantencion_oferta": "Responde este correo y armamos el contrato con tus condiciones.",
    "prospectos/p1_oferta": "También puedes responder este correo; tu respuesta llega directo a tu ejecutivo.",
    "prospectos/p2_cotizacion": "¿Dudas? Responde este correo y te contactamos.",
}


class TestLasFrases(unittest.TestCase):
    def test_cada_frase_de_produccion_pasa_al_ticket(self):
        for clave, frase in EN_PRODUCCION.items():
            self.assertTrue(CSR.dice_responder(frase), clave)                       # antes: dice «responde»
            nuevo, n = CSR.aplicar("<p>" + frase + "</p>")
            self.assertEqual(n, 1, clave)
            self.assertFalse(CSR.dice_responder(nuevo), (clave, nuevo))             # ahora: ya no
            if "agenda_tecnico" not in clave:                                       # al TÉCNICO se le manda a su coordinador, no a un ticket
                self.assertIn("ticket de soporte", nuevo.lower(), clave)

    def test_solo_cambia_la_frase_el_resto_del_texto_queda_igual(self):
        html = '<td style="x">Hola.</td><p style="y">¿Imprevistos? Responde este correo o llámanos. Es mejor avisar a no llegar.</p><b>Fin</b>'
        nuevo, n = CSR.aplicar(html)
        self.assertEqual(n, 1)
        self.assertTrue(nuevo.startswith('<td style="x">Hola.</td><p style="y">') and nuevo.endswith("</p><b>Fin</b>"))
        self.assertIn("(botón de abajo)", nuevo)

    def test_es_idempotente_y_lo_que_no_calza_no_se_toca(self):
        for frase in EN_PRODUCCION.values():
            una, _ = CSR.aplicar(frase)
            dos, n2 = CSR.aplicar(una)
            self.assertEqual((una, n2), (dos, 0))
        texto = "Una frase editada a mano: puedes responder este correo si quieres."        # no está tal cual en REEMPLAZOS
        self.assertEqual(CSR.aplicar(texto), (texto, 0))
        self.assertTrue(CSR.dice_responder(texto))                                            # y el aviso del arranque la ve
        self.assertEqual(CSR.aplicar(""), ("", 0))
        self.assertEqual(CSR.aplicar(None), (None, 0))

    def test_al_tecnico_se_le_manda_a_su_coordinador_no_a_un_ticket(self):
        nuevo, _ = CSR.aplicar(EN_PRODUCCION["mantenciones/agenda_tecnico_manana"])
        self.assertIn("coordinador de Servicio Técnico", nuevo)
        self.assertNotIn("ticket", nuevo.lower())

    def test_los_botones_de_texto_no_se_confunden_con_responder_el_correo(self):
        for ok in ("Responder la propuesta →", "Responder al reagendamiento", "Responde con un clic:"):
            self.assertFalse(CSR.dice_responder(ok), ok)

    def test_los_reemplazos_no_se_pisan_entre_si(self):
        viejos = [v for v, _n in CSR.REEMPLAZOS]
        for i, a in enumerate(viejos):
            for j, b in enumerate(viejos):
                if i != j:
                    self.assertNotIn(a, b, (a, b))        # ninguna frase está contenida en otra (el orden no importaría)
        for _v, nueva in CSR.REEMPLAZOS:
            self.assertFalse(CSR.dice_responder(nueva), nueva)

    def test_la_frase_es_corta(self):
        self.assertLessEqual(len(CSR.COMO_CREAR_TICKET), 60)                                   # Daniel: «no me gusta tanta letra»


class _BD:
    """Base de mentira de comm_templates: filas {id, modulo, estado, cuerpo}; registra los UPDATE."""
    def __init__(self, filas):
        self.filas = [dict({"asunto": ""}, **dict(f, canal="email")) for f in filas]
        self.updates = []
        self.updates_asunto = []

    def fetchall(self, sql, params=()):
        assert "FROM comm_templates" in sql and "canal='email'" in sql
        if "asunto LIKE" in sql:
            return [dict(f) for f in self.filas if "OT {{" in (f.get("asunto") or "")]
        return [dict(f) for f in self.filas if "espond" in f["cuerpo"]]

    def execute(self, sql, params=()):
        if sql.startswith("UPDATE comm_templates SET asunto=%s, updated_by=%s WHERE id=%s AND asunto=%s"):
            asunto, por, fid, anterior = params
            for f in self.filas:
                if f["id"] == fid and f["asunto"] == anterior:
                    f["asunto"] = asunto
                    self.updates_asunto.append((fid, por))
            return
        assert sql.startswith("UPDATE comm_templates SET cuerpo=%s, updated_by=%s WHERE id=%s AND cuerpo=%s"), sql
        cuerpo, por, fid, anterior = params
        for f in self.filas:
            if f["id"] == fid and f["cuerpo"] == anterior:
                f["cuerpo"] = cuerpo
                self.updates.append((fid, por))


class TestElArranque(unittest.TestCase):
    def _correr(self, filas):
        bd = _BD(filas)
        amb = {"mysql_fetchall": bd.fetchall, "mysql_execute": bd.execute, "_csr": CSR, "print": lambda *a, **k: None}
        FC.extraer(amb, "app.py", ("_ensure_correos_sin_responder",), ())
        return bd, amb["_ensure_correos_sin_responder"]()

    def _filas(self):
        return [
            {"id": 1, "modulo": "retiros", "estado": "agenda_confirmada",
             "cuerpo": "<p>Hola</p><p>" + EN_PRODUCCION["retiros/agenda_confirmada"] + "</p>"},
            {"id": 2, "modulo": "retiros", "estado": "rechazada",
             "cuerpo": "<h1>" + EN_PRODUCCION["retiros/rechazada (título)"] + "</h1><p>" + EN_PRODUCCION["retiros/rechazada (cuerpo)"] + "</p>"},
            {"id": 3, "modulo": "tickets", "estado": "resuelto",
             "cuerpo": "<p>Si el problema persiste, responde este correo y retomamos tu caso.</p>"},
            {"id": 4, "modulo": "mantenciones", "estado": "plan_propuesto", "cuerpo": "<p>" + EN_PRODUCCION["mantenciones/plan_propuesto"] + "</p>"},
            {"id": 5, "modulo": "prospectos", "estado": "editada",
             "cuerpo": "<p>Texto que Daniel escribió: puedes responder este correo cuando quieras.</p>"},
            {"id": 6, "modulo": "retiros", "estado": "propuesta_enviada", "cuerpo": "<a>Responder la propuesta →</a>"},
            {"id": 7, "modulo": "comunicacion_interna", "estado": "x", "cuerpo": "<p>Responde este correo y te ayudamos.</p>"},
        ]

    def test_cambia_solo_las_filas_que_corresponden(self):
        bd, n = self._correr(self._filas())
        self.assertEqual(n, 4)                                                                 # 1, 2, 3 (Tickets «Resuelto») y 4
        por_id = {f["id"]: f["cuerpo"] for f in bd.filas}
        for i in (1, 2, 3, 4):
            self.assertFalse(CSR.dice_responder(por_id[i]), i)
            self.assertIn("ticket de soporte", por_id[i].lower(), i)
        self.assertTrue(por_id[1].startswith("<p>Hola</p>"))                                   # el resto del cuerpo, intacto
        self.assertEqual([u[1] for u in bd.updates], ["sistema"] * 4)

    def test_no_toca_la_comunicacion_interna_ni_lo_editado_a_mano(self):
        bd, _ = self._correr(self._filas())
        por_id = {f["id"]: f["cuerpo"] for f in bd.filas}
        self.assertIn("responde este correo", por_id[7].lower())                               # interna
        self.assertIn("Texto que Daniel escribió", por_id[5])                                  # la frase editada no está tal cual: queda
        self.assertEqual(por_id[6], "<a>Responder la propuesta →</a>")

    def test_es_idempotente(self):
        bd, _ = self._correr(self._filas())
        otra = _BD(bd.filas)
        amb = {"mysql_fetchall": otra.fetchall, "mysql_execute": otra.execute, "_csr": CSR, "print": lambda *a, **k: None}
        FC.extraer(amb, "app.py", ("_ensure_correos_sin_responder",), ())
        self.assertEqual(amb["_ensure_correos_sin_responder"](), 0)
        self.assertEqual(otra.updates, [])

    def test_si_la_base_falla_el_arranque_sigue(self):
        def falla(*a, **k):
            raise RuntimeError("sin base")
        amb = {"mysql_fetchall": falla, "mysql_execute": falla, "_csr": CSR, "print": lambda *a, **k: None}
        FC.extraer(amb, "app.py", ("_ensure_correos_sin_responder",), ())
        self.assertEqual(amb["_ensure_correos_sin_responder"](), 0)

    def test_quita_el_OT_doble_del_asunto_de_las_plantillas_guardadas(self):
        filas = [{"id": 10, "modulo": "mantenciones", "estado": "ot_terminada_tecnico", "asunto": "OT {{ot}} terminada — gracias", "cuerpo": "<p>Gracias</p>"},
                 {"id": 11, "modulo": "mantenciones", "estado": "ot_desasignada_tecnico", "asunto": "Ajuste en tu agenda · OT {{ot}}", "cuerpo": "<p>x</p>"},
                 {"id": 12, "modulo": "mantenciones", "estado": "ot_asignada", "asunto": "Nueva OT asignada — {{ot}}", "cuerpo": "<p>x</p>"},
                 {"id": 13, "modulo": "mantenciones", "estado": "editada", "asunto": "Tu orden {{ot}}", "cuerpo": "<p>x</p>"}]
        bd, _ = self._correr(filas)
        asuntos = {f["id"]: f["asunto"] for f in bd.filas}
        self.assertEqual(asuntos[10], "{{ot}} terminada — gracias")
        self.assertEqual(asuntos[11], "Ajuste en tu agenda · {{ot}}")
        self.assertEqual(asuntos[12], "Nueva OT asignada — {{ot}}")                    # «Nueva OT asignada — OT-2026-…»: ahí el OT no se repite
        self.assertEqual(asuntos[13], "Tu orden {{ot}}")
        self.assertEqual(sorted(u[0] for u in bd.updates_asunto), [10, 11])
        bd2 = _BD(bd.filas)                                                             # idempotente
        amb = {"mysql_fetchall": bd2.fetchall, "mysql_execute": bd2.execute, "_csr": CSR, "print": lambda *a, **k: None}
        FC.extraer(amb, "app.py", ("_ensure_correos_sin_responder",), ())
        amb["_ensure_correos_sin_responder"]()
        self.assertEqual(bd2.updates_asunto, [])

    def test_el_arranque_esta_cableado_despues_de_sembrar_las_plantillas_de_la_ot(self):
        app = FC.texto("app.py")
        self.assertIn("import correos_sin_responder as _csr", app)
        i = app.index("        _ensure_comm_template_ot_comunicacion()\n        _ensure_correos_sin_responder()")
        self.assertGreater(i, 0)


# Lo que NO se toca (y por qué): cualquier otra coincidencia en estos archivos es un correo nuevo al cliente que dice «responde» y hay que arreglarlo.
_RX = re.compile(r"respond[ae]\w*\s+(?:a\s+|en\s+)?(?:este|el|tu|ese)\s+(?:correo|mail|email)|escr[ií]benos\s+respondiendo|respuesta a este correo",
                 re.IGNORECASE)
_PERMITIDOS_EN_APP = (
    "Si está conforme, responda este correo para que se emita la orden de compra.",           # correo al PROVEEDOR (facturas): responder es la vía
    "Si no responde el correo, llamar por teléfono",                                           # nota para el EJECUTIVO (el cliente no contestó), no un correo al cliente
    "Si necesitas reagendar o cancelar, responde este correo o contacta a tu ejecutivo.",     # semilla ORIGINAL (`_MANT_TPL` / `_mantenciones_tpl_seed`): base de la cadena
    "Si tienes alguna observación, responde este correo y te contactaremos a la brevedad.",   # ídem
)


class TestLaGuarda(unittest.TestCase):
    def test_los_archivos_de_correos_al_cliente_no_piden_responder(self):
        for rel in ("tickets_module.py", "pickups_module.py", "ot_correos_tecnicos.py", "ot_recordatorio_24h.py", "retiros_desviaciones.py",
                    "templates/retiros/public_tracking.html", "static/comunicaciones_index.js"):
            malos = [(i + 1, l.strip()[:120]) for i, l in enumerate(FC.texto(rel).split("\n")) if _RX.search(l) and not l.lstrip().startswith(("#", "//"))]
            self.assertEqual(malos, [], rel)

    def test_en_app_py_solo_quedan_las_excepciones_conocidas(self):
        malos = []
        for i, l in enumerate(FC.texto("app.py").split("\n")):
            if not _RX.search(l) or l.lstrip().startswith("#"):
                continue
            if "«responde este correo»" in l or l.lstrip().startswith("import "):         # la regla citada (comentario o docstring), no un correo
                continue
            if any(p in l for p in _PERMITIDOS_EN_APP):
                continue
            malos.append((i + 1, l.strip()[:140]))
        self.assertEqual(malos, [])

    def test_las_plantillas_de_la_ot_vigentes_no_piden_responder(self):
        import ot_comunicacion as C
        for slug, (_a, cuerpo) in list(C.SEMILLAS.items()) + list(C.SEMILLAS_EXTRA.items()):
            self.assertFalse(CSR.dice_responder(cuerpo), slug)

    def test_las_semillas_de_retiros_y_prospectos_del_codigo_tampoco(self):
        app = FC.texto("app.py")
        for frase, _n in CSR.REEMPLAZOS:
            if "Si necesitas reagendar o cancelar, responde" in frase or "Si tienes alguna observación, responde" in frase:
                continue                                                                      # las semillas originales de la cadena (arriba)
            self.assertNotIn(frase.replace("\n", ""), app.replace("'\n         '", "").replace("'\r\n         '", ""), frase)


class TestElBotonDelPie(unittest.TestCase):
    """«Coloca un botón»: el pie de TODO correo es un botón de verdad en Outlook (relleno en la celda con bgcolor; el <a> es de bloque y sin padding)."""

    def test_el_boton_del_maestro_y_del_pie_suelto_son_solidos(self):
        app = FC.texto("app.py")
        self.assertEqual(app.count('bgcolor="#e30613" style="background:#e30613;border-radius:6px;padding:14px 32px"'), 2)
        self.assertEqual(app.count('style="display:block;color:#ffffff;text-decoration:none;font-size:15px;line-height:20px;font-weight:700;'
                                   'font-family:Arial,Helvetica,sans-serif">Crear ticket de soporte</a>'), 2)
        self.assertNotIn('border:1px solid #e30613;border-radius:5px"><a href="{ILUS_SOPORTE_URL}"', app.replace("\n", ""))
        self.assertNotIn("display:inline-block;padding:11px 18px;color:#ffffff", app)           # el botón viejo: padding en el <a> (Outlook lo ignora)

    def test_el_boton_principal_del_maestro_lleva_el_relleno_en_la_celda(self):
        app = FC.texto("app.py")
        self.assertIn('class="email-button" align="center" bgcolor="#e30613" style="background:#e30613;border-radius:5px;padding:14px 24px"', app)
        self.assertNotIn("display:inline-block;padding:14px 24px;", app)


if __name__ == "__main__":
    unittest.main()

"""Archivo de calendario (.ics) adjunto a los correos al técnico (Daniel, 20-oct-2026).

Decisión aceptada: asignada y reagendada llevan un .ics METHOD:REQUEST; cancelada y desasignada, uno METHOD:CANCEL, todos con
el MISMO UID estable por OT (`ot-{numero}@ilusfitness.com`) para que reenviar ACTUALICE el evento y el CANCEL lo borre. La
agenda de las 17:00 NO lleva archivo (ya tienen cada evento).

Qué se vigila (todo con correo y base de MENTIRA — REGLA #22):
  · El constructor puro (`ot_correos_tecnicos.ics_evento`): formato RFC 5545 (CRLF, escapes, líneas <= 75 octetos plegadas
    sin partir un carácter), UID estable, SEQUENCE creciente, CANCEL, hora de Chile -> UTC (verano e invierno), día completo,
    OT de varios días, y que NUNCA entre plata, proveedores ni OC (REGLAS #19 y #26) ni dirección/detalle con el Anexo sin firmar.
  · app.py con un mundo en memoria: el correo real de cada evento lleva (o no) su adjunto y pide encolarlo.
  · La cola de correo: un adjunto PEQUEÑO sobrevive al encolado (base64) y se rearma al drenar; sin el opt-in todo sigue síncrono.
Correr con:  py -m unittest tests.test_ot_correos_tecnicos_ics
"""
import ast
import datetime as dt
import json
import re
import types
import unittest

import ot_correos_tecnicos as O
from tests.test_ot_correos_tecnicos import Mundo, F, OT, _bloque, _cargar

# 2026-10-20 12:00 UTC (los SEQUENCE se miden desde 2026-01-01)
TS = 1_792_497_600


def lineas(ics):
    """Contenido ya 'desplegado' (RFC 5545 §3.1: CRLF + espacio/tab se quita) y separado por línea."""
    texto = ics.decode("utf-8")
    return texto.replace("\r\n ", "").replace("\r\n\t", "").split("\r\n")


def campo(ics, nombre):
    return next((l.split(":", 1)[1] for l in lineas(ics) if l.split(":", 1)[0].split(";")[0] == nombre), None)


def ot(**kw):
    base = dict(OT, hora_inicio="09:00", hora_fin="11:00", descripcion="Instalar en el segundo piso. Llevar escalera.")
    base.update(kw)
    return base


def ics(evento="asignada", ts=TS, **kw):
    ocultar = kw.pop("ocultar_detalle", False)
    return O.ics_evento(ot(**kw), evento, correo="ana@sphs.cl", nombre="Ana Pérez", ts=ts, base_url="https://app.ilus.cl/",
                        ocultar_detalle=ocultar)


class TestFormato(unittest.TestCase):
    def test_estructura_rfc5545_con_crlf(self):
        b = ics()
        self.assertIsInstance(b, bytes)
        t = b.decode("utf-8")
        self.assertTrue(t.startswith("BEGIN:VCALENDAR\r\nVERSION:2.0\r\n"))
        self.assertTrue(t.endswith("END:VEVENT\r\nEND:VCALENDAR\r\n"))
        self.assertNotIn("\n", t.replace("\r\n", ""))              # ningún salto suelto: todos son CRLF
        for tag in ("PRODID", "METHOD", "UID", "DTSTAMP", "SEQUENCE", "DTSTART", "DTEND", "SUMMARY", "ORGANIZER", "ATTENDEE",
                    "STATUS", "URL"):
            self.assertIsNotNone(campo(b, tag), tag)

    def test_ninguna_linea_pasa_de_75_octetos_y_el_pliegue_no_parte_un_caracter(self):
        b = ics(cliente="Á" * 90, direccion="Ñandú, " + "ú" * 80, descripcion="Revisión " + "ñ" * 150)
        for l in b.decode("utf-8").split("\r\n"):
            self.assertLessEqual(len(l.encode("utf-8")), 75, l)
        b.decode("utf-8")                                           # sigue siendo UTF-8 válido (no se partió un carácter)
        self.assertIn("Á" * 90, campo(b, "SUMMARY"))                # y al desplegar el texto vuelve completo

    def test_escapes_de_coma_punto_y_coma_barra_y_salto(self):
        self.assertEqual(O.ics_escapar("a,b;c\\d\ne"), "a\\,b\\;c\\\\d\\ne")
        b = ics(cliente="Gym, Sur; SA")
        self.assertIn("Gym\\, Sur\\; SA", campo(b, "SUMMARY"))
        # el texto de la descripción separa sus líneas con \n escapado, nunca con un salto real
        self.assertIn("\\n", campo(b, "DESCRIPTION"))

    def test_plegar_no_toca_lineas_cortas(self):
        self.assertEqual(O.ics_plegar("SUMMARY:corto"), "SUMMARY:corto")
        largo = O.ics_plegar("X:" + "a" * 200)
        self.assertEqual(largo.replace("\r\n ", ""), "X:" + "a" * 200)
        self.assertTrue(all(len(p.encode()) <= 75 for p in largo.split("\r\n")))


class TestIdentidadYSecuencia(unittest.TestCase):
    def test_uid_estable_por_ot_en_los_cuatro_eventos(self):
        uids = {campo(ics(ev), "UID") for ev in O.EVENTOS}
        self.assertEqual(uids, {"ot-OT-2026-00301@ilusfitness.com"})
        self.assertEqual(O.ics_uid("OT-2026-00301"), "ot-OT-2026-00301@ilusfitness.com")
        self.assertEqual(O.ics_uid("a b/c\r\nX:evil"), "ot-abcXevil@ilusfitness.com")      # nada que rompa la línea

    def test_sequence_crece_con_el_tiempo_y_el_cancel_gana(self):
        s1 = int(campo(ics("asignada", ts=TS), "SEQUENCE"))
        s2 = int(campo(ics("reagendada", ts=TS + 3600), "SEQUENCE"))
        s3 = int(campo(ics("cancelada", ts=TS + 7200), "SEQUENCE"))
        self.assertTrue(s1 < s2 < s3, (s1, s2, s3))
        self.assertEqual(O.secuencia_desde(O.SEQ_EPOCH + 5), 5)
        self.assertEqual(O.secuencia_desde(0), 0)                   # nunca negativa
        self.assertEqual(O.secuencia_desde("basura"), 0)

    def test_metodo_por_evento(self):
        self.assertEqual({e: campo(ics(e), "METHOD") for e in O.EVENTOS},
                         {"asignada": "REQUEST", "reagendada": "REQUEST", "cancelada": "CANCEL", "desasignada": "CANCEL"})
        self.assertEqual(campo(ics("asignada"), "STATUS"), "CONFIRMED")
        self.assertEqual(campo(ics("cancelada"), "STATUS"), "CANCELLED")
        self.assertEqual(campo(ics("desasignada"), "STATUS"), "CANCELLED")

    def test_evento_desconocido_o_ot_sin_fecha_no_arma_archivo(self):
        self.assertIsNone(ics("otro"))
        self.assertIsNone(ics(fecha=None))
        self.assertIsNone(ics(fecha=""))

    def test_organizador_y_asistente_sin_pedir_respuesta(self):
        b = ics()
        self.assertEqual(campo(b, "ORGANIZER"), "mailto:soportetec@sphs.cl")
        att = next(l for l in lineas(b) if l.startswith("ATTENDEE"))
        self.assertIn("mailto:ana@sphs.cl", att)
        self.assertIn("RSVP=FALSE", att)                            # no se le pide responder (llegaría a un buzón de soporte)
        self.assertIn('CN="Ana Pérez"', att)


class TestHoras(unittest.TestCase):
    def test_hora_de_chile_pasa_a_utc_con_horario_de_verano(self):
        # 26/10/2026 Chile está en horario de verano (UTC-3); 26/08/2026 en invierno (UTC-4)
        v = ics(fecha=F(2026, 10, 26), hora_inicio="09:00", hora_fin="11:00")
        self.assertEqual(campo(v, "DTSTART"), "20261026T120000Z")
        self.assertEqual(campo(v, "DTEND"), "20261026T140000Z")
        i = ics(fecha=F(2026, 8, 26), hora_inicio="09:00", hora_fin="11:00")
        self.assertEqual(campo(i, "DTSTART"), "20260826T130000Z")
        self.assertEqual(campo(i, "DTEND"), "20260826T150000Z")

    def test_el_dia_del_cambio_de_hora_usa_el_corrimiento_correcto(self):
        # el cambio a verano 2026 es el sábado 5/9 a las 24:00 (Chile pasa de UTC-4 a UTC-3): el viernes 4 aún es -4 y el lunes 7 ya es -3
        self.assertEqual(campo(ics(fecha=F(2026, 9, 4), hora_inicio="09:00", hora_fin="10:00"), "DTSTART"), "20260904T130000Z")
        self.assertEqual(campo(ics(fecha=F(2026, 9, 7), hora_inicio="09:00", hora_fin="10:00"), "DTSTART"), "20260907T120000Z")

    def test_sin_hora_fin_o_con_hora_fin_invalida_dura_una_hora(self):
        b = ics(fecha=F(2026, 10, 26), hora_inicio="09:00", hora_fin=None)
        self.assertEqual(campo(b, "DTEND"), "20261026T130000Z")
        b = ics(fecha=F(2026, 10, 26), hora_inicio="09:00", hora_fin="08:00")          # termina antes de empezar
        self.assertEqual(campo(b, "DTEND"), "20261026T130000Z")

    def test_acepta_las_horas_de_pymysql_timedelta(self):
        b = ics(fecha=F(2026, 10, 26), hora_inicio=dt.timedelta(hours=9, minutes=30), hora_fin=dt.timedelta(hours=11))
        self.assertEqual(campo(b, "DTSTART"), "20261026T123000Z")
        self.assertEqual(campo(b, "DTEND"), "20261026T140000Z")

    def test_sin_hora_es_evento_de_dia_completo(self):
        b = ics(fecha=F(2026, 10, 26), hora_inicio=None, hora_fin=None)
        t = b.decode()
        self.assertIn("DTSTART;VALUE=DATE:20261026", t)
        self.assertIn("DTEND;VALUE=DATE:20261027", t)               # DTEND exclusivo
        self.assertNotIn("VALARM", t)                               # sin hora no hay "2 h antes"
        b2 = ics(fecha=F(2026, 10, 26), fecha_fin=F(2026, 10, 28), hora_inicio=None, hora_fin=None)
        self.assertIn("DTEND;VALUE=DATE:20261029", b2.decode())

    def test_ot_de_varios_dias_con_hora_repite_el_horario_cada_dia(self):
        b = ics(fecha=F(2026, 10, 26), fecha_fin=F(2026, 10, 28), hora_inicio="09:00", hora_fin="11:00")
        self.assertEqual(campo(b, "DTSTART"), "20261026T120000Z")
        self.assertEqual(campo(b, "RDATE"), "20261027T120000Z,20261028T120000Z")
        # un rango largo no genera un archivo enorme
        largo = ics(fecha=F(2026, 1, 1), fecha_fin=F(2026, 12, 31), hora_inicio="09:00", hora_fin="11:00")
        self.assertLessEqual(len(campo(largo, "RDATE").split(",")), O.ICS_MAX_DIAS)

    def test_alarma_2_horas_antes_solo_en_request_con_hora(self):
        self.assertIn("TRIGGER:-PT2H", ics("asignada").decode())
        self.assertIn("TRIGGER:-PT2H", ics("reagendada").decode())
        self.assertNotIn("VALARM", ics("cancelada").decode())
        self.assertNotIn("VALARM", ics("desasignada").decode())


class TestContenido(unittest.TestCase):
    def test_titulo_ubicacion_link_y_descripcion(self):
        b = ics()
        self.assertEqual(campo(b, "SUMMARY"), "OT-2026-00301 — Mall Plaza <Sur> & Cía (Instalación)")   # sin «OT OT-»
        self.assertEqual(campo(b, "LOCATION"), "Av. Vicuña Mackenna 1000\\, Santiago")
        self.assertEqual(campo(b, "URL"), "https://app.ilus.cl/ot/301")
        d = campo(b, "DESCRIPTION")
        self.assertIn("Trotadora ILUS Pro 9000", d)
        self.assertIn("Llevar escalera", d)
        self.assertIn("https://app.ilus.cl/ot/301", d)

    def test_numero_sin_prefijo_ot_lo_lleva_en_el_titulo(self):
        self.assertTrue(campo(ics(numero_ot="V-00301"), "SUMMARY").startswith("OT V-00301 — "))

    def test_trabajo_interno_sin_cliente(self):
        self.assertIn("Trabajo interno", campo(ics(cliente=""), "SUMMARY"))

    def test_cancelada_dice_que_ya_no_esta_en_la_agenda_y_con_motivo_limpio(self):
        b = O.ics_evento(ot(), "cancelada", correo="ana@sphs.cl", ts=TS, base_url="https://app.ilus.cl",
                         motivo="El cliente pidió otra fecha. Sin OC aprobada.")
        d = campo(b, "DESCRIPTION")
        self.assertIn("ya no está en tu agenda", d)
        self.assertIn("El cliente pidió otra fecha", d)
        self.assertNotIn("OC", d)

    def test_link_a_la_lista_cuando_la_ot_ya_no_existe(self):
        b = O.ics_evento(ot(), "cancelada", correo="ana@sphs.cl", ts=TS, base_url="https://app.ilus.cl", link_lista=True)
        self.assertEqual(campo(b, "URL"), "https://app.ilus.cl/ot/")

    def test_anexo_sin_firmar_no_adelanta_direccion_ni_detalle(self):
        b = ics(ocultar_detalle=True, descripcion="Instalar en el sótano. Llevar escalera.")
        t = b.decode()
        self.assertNotIn("LOCATION", t)
        self.assertNotIn("Vicuña", t)
        self.assertNotIn("escalera", t)
        self.assertNotIn("Qué hay que hacer", t)
        # igual lleva el evento (fecha, cliente, link): el técnico sabe QUE hay una OT
        self.assertIn("DTSTART:", t)
        self.assertIn("/ot/301", t)

    def test_direccion_vacia_no_deja_ubicacion(self):
        self.assertNotIn("LOCATION", ics(direccion="—").decode())
        self.assertNotIn("LOCATION", ics(direccion="").decode())


class TestSinPlata(unittest.TestCase):
    """REGLAS #19 y #26: ni montos, ni costos, ni proveedores, ni OC en NINGÚN campo que escribe una persona."""

    LIBRE = ("Instalar. Cobrar $120.000 más IVA. Pagar al proveedor Milling. Factura 4455 y OC 77. Margen del 30%. "
             "Saldo pendiente de 85 mil. Llevar escalera.")

    def _todo(self, **kw):
        out = []
        for ev in O.EVENTOS:
            b = O.ics_evento(ot(descripcion=self.LIBRE, **kw), ev, correo="ana@sphs.cl", nombre="Ana", ts=TS,
                             base_url="https://app.ilus.cl", motivo=self.LIBRE)
            out.append(b.decode("utf-8"))
        return "\n".join(out)

    def test_el_texto_libre_pasa_por_el_filtro(self):
        t = self._todo()
        for prohibido in ("120.000", "IVA", "proveedor", "Milling", "Factura", "4455", "OC 77", "Margen", "85 mil", "$", "Saldo"):
            self.assertNotIn(prohibido, t, prohibido)
        self.assertIn("Llevar escalera", t)                         # lo útil sí pasa

    def test_ningun_campo_del_archivo_menciona_plata(self):
        # el mismo criterio del correo: ni una línea (salvo el link/URL y datos fijos) que `hay_plata` reconozca
        for ev in O.EVENTOS:
            b = O.ics_evento(ot(descripcion=self.LIBRE), ev, correo="ana@sphs.cl", nombre="Ana", ts=TS,
                             base_url="https://app.ilus.cl", motivo=self.LIBRE)
            for l in lineas(b):
                k = l.split(":", 1)[0].split(";")[0]
                if k in ("SUMMARY", "DESCRIPTION", "LOCATION"):
                    valor = l.split(":", 1)[1].replace("\\n", " ").replace("\\,", ",")
                    if k == "DESCRIPTION":
                        valor = re.sub(r"https?://\S+", "", valor)       # el link a la ficha
                    self.assertFalse(O.hay_plata(valor.replace("Orden de trabajo", "")), (ev, l))


class TestEnvioDeLaOT(unittest.TestCase):
    """El correo real de cada evento (con el punto único `_ot_tec_correo` y un mundo en memoria)."""

    def setUp(self):
        self.m = Mundo()
        self.m.t = float(TS)
        self.m.amb["time"] = types.SimpleNamespace(time=lambda: self.m.t)

    def _adj(self, o):
        self.assertEqual(len(o["adjuntos"]), 1, "un solo .ics por OT por correo")
        nombre, datos, mime = o["adjuntos"][0]
        self.assertEqual(mime, "text/calendar")
        return nombre, datos

    def test_asignada_lleva_request_y_pide_encolar_el_adjunto(self):
        self.m.ot()
        self.m.amb["_ot_tec_correo"](301, antes=None)
        o = self.m.outbox[0]
        nombre, datos = self._adj(o)
        self.assertEqual(nombre, "OT_OT-2026-00301.ics")
        self.assertEqual(campo(datos, "METHOD"), "REQUEST")
        self.assertIn("mailto:ana@sphs.cl", datos.decode())
        self.assertTrue(o["encolar_adjuntos"])                       # el guardado de la OT no espera al correo
        self.assertTrue(o["asincrono"])
        self.assertEqual(o["modulo"], "ot_tecnicos")                 # sigue por la llave propia

    def test_reagendada_actualiza_el_mismo_evento_con_sequence_mayor(self):
        self.m.ot()
        self.m.amb["_ot_tec_correo"](301, antes=None)
        self.m.t += 3600
        antes = self.m.cambia(301, fecha=F(2026, 10, 28), hi="14:00", hf="16:00")
        self.m.amb["_ot_tec_correo"](301, antes, motivo="El cliente pidió otro día")
        a, r = self._adj(self.m.outbox[0])[1], self._adj(self.m.outbox[1])[1]
        self.assertEqual(self.m.outbox[1]["evento"], "ot_reagendada_tecnico")
        self.assertEqual(campo(r, "METHOD"), "REQUEST")
        self.assertEqual(campo(a, "UID"), campo(r, "UID"))
        self.assertGreater(int(campo(r, "SEQUENCE")), int(campo(a, "SEQUENCE")))
        self.assertEqual(campo(r, "DTSTART"), "20261028T170000Z")    # 14:00 Chile (UTC-3)
        self.assertIn("El cliente pidió otro día", campo(r, "DESCRIPTION"))

    def test_cancelada_y_desasignada_llevan_cancel_con_la_fecha_que_tenia(self):
        self.m.ot(tec={"ana@sphs.cl": "Ana Pérez", "beto@sphs.cl": "Beto Soto"})
        antes = self.m.cambia(301, fecha=F(2026, 10, 29), tec={"beto@sphs.cl": "Beto Soto"})
        self.m.amb["_ot_tec_correo"](301, antes)
        ana = next(o for o in self.m.outbox if o["to"] == "ana@sphs.cl")
        self.assertEqual(ana["evento"], "ot_desasignada_tecnico")
        d = self._adj(ana)[1]
        self.assertEqual(campo(d, "METHOD"), "CANCEL")
        self.assertEqual(campo(d, "DTSTART"), "20261026T120000Z")    # lo que Ana tenía en su calendario (26/10), no el 29
        beto = next(o for o in self.m.outbox if o["to"] == "beto@sphs.cl")
        self.assertEqual(campo(self._adj(beto)[1], "METHOD"), "REQUEST")
        self.assertEqual(campo(self._adj(beto)[1], "DTSTART"), "20261029T120000Z")
        # cancelar la OT entera
        m2 = Mundo()
        m2.t = float(TS)
        m2.amb["time"] = types.SimpleNamespace(time=lambda: m2.t)
        m2.ot()
        a2 = m2.cambia(301, estado="cancelada")
        m2.amb["_ot_tec_correo"](301, a2, motivo="Se canceló el contrato")
        c = m2.outbox[0]
        self.assertEqual(c["evento"], "ot_cancelada_tecnico")
        self.assertEqual(campo(self._adj(c)[1], "METHOD"), "CANCEL")
        self.assertEqual(campo(self._adj(c)[1], "STATUS"), "CANCELLED")

    def test_cancelacion_por_borrado_apunta_a_la_lista(self):
        self.m.ot()
        self.m.informados[301] = {"ana@sphs.cl"}
        snap = self.m.amb["_ot_tec_snapshot_borrado"](301, {"estado": "programada", "fecha_programada": F(2026, 10, 26),
                                                           "fecha_fin": None})
        self.m.amb["_ot_tec_correo_borrada"](301, snap)
        d = self._adj(self.m.outbox[0])[1]
        self.assertEqual(campo(d, "METHOD"), "CANCEL")
        self.assertEqual(campo(d, "URL"), "https://app.ilus.cl/ot/")

    def test_el_recordatorio_de_la_agenda_no_lleva_archivo(self):
        self.m.ot(fecha=F(2026, 10, 26))
        self.m.ahora = dt.datetime(2026, 10, 23, 17, 5)
        self.m.amb["_ot_tec_barrido_agenda"]()
        self.assertTrue(self.m.outbox)
        for o in self.m.outbox:
            self.assertEqual(o["evento"], "agenda_tecnico_manana")
            self.assertFalse(o["adjuntos"])

    def test_anexo_sin_firmar_el_archivo_no_lleva_direccion_ni_detalle(self):
        self.m.ot()
        self.m.anexo[301] = {"bloquea": True}
        self.m.amb["_ot_tec_correo"](301, antes=None)
        t = self._adj(self.m.outbox[0])[1].decode()
        self.assertNotIn("LOCATION", t)
        self.assertNotIn("Vicuña", t)
        self.assertNotIn("Instalar la trotadora", t)

    def test_ot_con_plata_en_la_descripcion_no_la_filtra_al_archivo(self):
        self.m.ot(descripcion="Instalar. Cobrar $120.000 más IVA. Pagar al proveedor Milling. Factura 4455 y OC 77")
        self.m.amb["_ot_tec_correo"](301, antes=None)
        t = self._adj(self.m.outbox[0])[1].decode()
        for prohibido in ("120.000", "IVA", "Milling", "Factura", "OC 77", "$"):
            self.assertNotIn(prohibido, t, prohibido)

    def test_sin_fecha_el_correo_sale_igual_sin_archivo(self):
        self.m.ot(fecha=None)
        self.m.amb["_ot_tec_correo"](301, antes=None)
        self.assertEqual(len(self.m.outbox), 1)
        self.assertFalse(self.m.outbox[0]["adjuntos"])
        self.assertNotIn("encolar_adjuntos", [k for k, v in self.m.outbox[0].items() if v is True])

    def test_si_falla_el_armado_del_archivo_el_correo_sale_igual(self):
        self.m.ot()
        self.m.amb["_get_brand_cfg"] = lambda: (_ for _ in ()).throw(RuntimeError("config caída"))
        self.m.amb["_ot_tec_correo"](301, antes=None)
        self.assertEqual(len(self.m.outbox), 1)
        self.assertFalse(self.m.outbox[0]["adjuntos"])

    def test_los_textos_de_las_semillas_hablan_del_archivo_adjunto(self):
        for slug in ("ot_asignada", "ot_reagendada_tecnico", "ot_cancelada_tecnico", "ot_desasignada_tecnico"):
            self.assertIn("archivo adjunto", O.SEMILLAS[slug][1], slug)
        self.assertNotIn("archivo adjunto", O.SEMILLAS["agenda_tecnico_manana"][1])       # la agenda no lleva archivo
        self.assertNotIn("agéndala en tu calendario", O.SEMILLAS["ot_asignada"][1])


class _Conn:
    def __init__(self):
        self.inserts = []

    def cursor(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=()):
        self.inserts.append((sql, params))

    def commit(self):
        pass


class TestColaDeCorreo(unittest.TestCase):
    """Un adjunto pequeño sobrevive al encolado asíncrono (si no, el .ics obligaría al envío síncrono)."""

    def _amb(self):
        conn, enviados, kicks = _Conn(), [], []
        amb = {"json": json, "print": lambda *a, **k: None, "g": types.SimpleNamespace(),
               "comm_is_enabled": lambda c: True, "_modulo_canal_bloqueado": lambda m, c: False,
               "_modulo_desde_evento": lambda e: None, "_email_marcar_bloqueo": lambda *a: None,
               "_email_log": lambda *a, **k: None, "_email_log_norm_ref": lambda ref, s: ("OT", 1, "OT-1"),
               "get_db": lambda: conn, "_email_queue_kick": lambda: kicks.append(1),
               "_send_ilus_email_real": lambda to, asunto, html, **kw: enviados.append(kw) or True,
               "mysql_execute": lambda *a, **k: None}
        _cargar(amb, ("_email_normalize_attachments", "_email_adjuntos_para_cola", "_email_adjuntos_de_cola",
                      "_send_ilus_email", "_email_queue_send_job"), ("_EMAIL_COLA_ADJ_MAX",))
        return amb, conn, enviados, kicks

    ICS = [("OT_1.ics", b"BEGIN:VCALENDAR\r\nEND:VCALENDAR\r\n", "text/calendar")]

    def test_ida_y_vuelta_de_los_adjuntos_pequenos(self):
        amb, *_ = self._amb()
        guardado = amb["_email_adjuntos_para_cola"](self.ICS)
        json.dumps(guardado)                                          # cabe en kwargs_json
        self.assertNotIn("content", guardado[0])                      # una instancia vieja lo descarta en silencio, no revienta
        self.assertEqual(amb["_email_adjuntos_de_cola"](guardado), self.ICS)

    def test_un_adjunto_grande_no_viaja_por_la_cola(self):
        amb, *_ = self._amb()
        self.assertIsNone(amb["_email_adjuntos_para_cola"]([("x.pdf", b"x" * (amb["_EMAIL_COLA_ADJ_MAX"] + 1), "application/pdf")]))
        self.assertIsNone(amb["_email_adjuntos_para_cola"](None))
        self.assertIsNone(amb["_email_adjuntos_para_cola"]([]))

    def test_con_opt_in_se_encola_con_el_adjunto(self):
        amb, conn, enviados, kicks = self._amb()
        ok = amb["_send_ilus_email"]("ana@sphs.cl", "Asunto", "<p>x</p>", evento="ot_asignada", modulo="ot_tecnicos",
                                     asincrono=True, encolar_adjuntos=True, attachments=self.ICS)
        self.assertTrue(ok)
        self.assertEqual(enviados, [])                                # NO se envió síncrono
        self.assertEqual(len(conn.inserts), 1)
        kwargs_json = next(p for p in conn.inserts[0][1] if isinstance(p, str) and p.startswith("{"))
        guardado = json.loads(kwargs_json)["attachments"]
        self.assertEqual(guardado[0]["filename"], "OT_1.ics")
        self.assertTrue(kicks)

    def test_sin_opt_in_un_adjunto_sigue_siendo_sincrono_como_siempre(self):
        amb, conn, enviados, _ = self._amb()
        amb["_send_ilus_email"]("ana@sphs.cl", "Asunto", "<p>x</p>", asincrono=True, attachments=self.ICS)
        self.assertEqual(conn.inserts, [])
        self.assertEqual(len(enviados), 1)
        self.assertEqual(enviados[0]["attachments"], self.ICS)

    def test_adjunto_grande_con_opt_in_cae_al_envio_sincrono(self):
        amb, conn, enviados, _ = self._amb()
        grande = [("x.pdf", b"x" * (amb["_EMAIL_COLA_ADJ_MAX"] + 1), "application/pdf")]
        amb["_send_ilus_email"]("ana@sphs.cl", "Asunto", "<p>x</p>", asincrono=True, encolar_adjuntos=True, attachments=grande)
        self.assertEqual(conn.inserts, [])
        self.assertEqual(len(enviados), 1)

    def test_el_worker_rearma_el_adjunto_al_drenar(self):
        amb, conn, enviados, _ = self._amb()
        guardado = amb["_email_adjuntos_para_cola"](self.ICS)
        job = {"id": 7, "destinatario": "ana@sphs.cl", "asunto": "Asunto", "cuerpo": "<p>x</p>", "evento": "ot_asignada",
               "modulo": "ot_tecnicos", "intentos": 1, "max_intentos": 5,
               "kwargs_json": json.dumps({"attachments": guardado, "cc": "otro@sphs.cl"})}
        amb["_email_queue_send_job"](job)
        self.assertEqual(enviados[0]["attachments"], self.ICS)       # bytes originales, no base64
        self.assertEqual(enviados[0]["cc"], "otro@sphs.cl")          # el resto de los kwargs sigue igual

    def test_jobs_antiguos_sin_adjuntos_se_drenan_igual(self):
        amb, conn, enviados, _ = self._amb()
        amb["_email_queue_send_job"]({"id": 8, "destinatario": "a@b.cl", "asunto": "A", "cuerpo": "x", "evento": None,
                                      "modulo": "general", "intentos": 1, "max_intentos": 5, "kwargs_json": '{"cc": "z@b.cl"}'})
        self.assertEqual(enviados, [{"cc": "z@b.cl"}])

    def test_solo_los_correos_a_tecnicos_piden_encolar_adjuntos(self):
        import os
        raiz = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        with open(os.path.join(raiz, "app.py"), encoding="utf-8", newline="") as f:
            codigo = f.read()
        self.assertEqual(codigo.count('"encolar_adjuntos": True'), 1)
        self.assertIn('"encolar_adjuntos": True', _bloque("_ot_tec_enviar"))


if __name__ == "__main__":
    unittest.main()

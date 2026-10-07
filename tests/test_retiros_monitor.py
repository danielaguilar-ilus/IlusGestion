"""Monitor de Retiros: datos derivados de la tabla rica y de las tarjetas KPI
(retiros_monitor.py, módulo puro). Corre con:
    py -m unittest tests.test_retiros_monitor -v
"""
import unittest
from datetime import date, datetime, timedelta

import retiros_monitor as rm

ESTADOS = {
    "solicitud_recibida": "Solicitud recibida", "en_revision": "En revisión",
    "informacion_incompleta": "Información incompleta", "propuesta_enviada": "Propuesta enviada",
    "esperando_cliente": "Esperando respuesta", "agenda_confirmada": "Agenda confirmada",
    "reagendada": "Reagendada", "rechazada": "Rechazada", "en_preparacion": "En preparación",
    "retirada": "Retirada", "fallida": "Fallida", "cerrada": "Cerrada",
}
GRUPOS = [
    {"key": "por_revisar", "label": "Por revisar", "icon": "bi-inbox",
     "statuses": ["solicitud_recibida", "en_revision", "informacion_incompleta"]},
    {"key": "propuesta", "label": "Propuesta", "icon": "bi-envelope-paper",
     "statuses": ["propuesta_enviada", "esperando_cliente"]},
    {"key": "agendada", "label": "Agendada", "icon": "bi-calendar-check",
     "statuses": ["agenda_confirmada", "reagendada"]},
    {"key": "preparacion", "label": "En preparación", "icon": "bi-box-seam", "statuses": ["en_preparacion"]},
    {"key": "retirada", "label": "Retirada", "icon": "bi-check2-circle", "statuses": ["retirada"]},
]
RELACIONES = {"dueno": "Dueño / titular", "chofer": "Chofer"}

COB = {"desde": 480, "hasta": 1020, "col_d": 780, "col_h": 840}   # 08:00-17:00, colación 13:00-14:00
HOY = date(2026, 9, 29)                 # martes
AHORA = datetime(2026, 9, 29, 15, 0)    # hora Chile


def _hh(td):
    s = int(td.total_seconds())
    return f"{s // 3600:02d}:{(s % 3600) // 60:02d}"


def _enriquecer(filas, logs=None, horas=None):
    return rm.enriquecer_filas(
        filas, hoy=HOY, ahora=AHORA,
        reloj_horas=horas or (lambda d, h, f: (h - d).total_seconds() / 3600.0),
        utc_a_chile=lambda dt: (dt - timedelta(hours=3)) if dt else None,
        td_hhmm=_hh, estados=ESTADOS, grupos=GRUPOS, relaciones=RELACIONES, logs=logs)


def _fila(**kw):
    base = dict(id=1, code="RET-ABC123", status="solicitud_recibida", customer_name="José Núñez Peña",
                customer_rut="12.345.678-5", contact_name="María Pérez", contact_phone="+56 9 7746 8766",
                contact_email="maria@empresa.cl", document_type="factura", document_number="1234",
                pickup_person_name="Pedro Soto", pickup_person_rut="9.876.543-3", pickup_person_phone="",
                pickup_person_relation="chofer", requested_date=date(2026, 10, 1),
                requested_time_from=timedelta(hours=9), requested_time_to=timedelta(hours=9, minutes=30),
                proposed_date=None, confirmed_date=None, total_packages=1, total_weight_kg=0,
                total_volumetric_weight=0, total_volume_m3=0, information_quality_score=79,
                request_source="web", responsable_nombre="", doc_validation_status="pendiente",
                created_by_user_name=None, created_at=datetime(2026, 9, 29, 18, 0))  # 15:00 Chile = ahora
    base.update(kw)
    return base


class TestNormalizacion(unittest.TestCase):
    def test_sin_tildes_y_minusculas(self):
        self.assertEqual(rm.norm("José Núñez"), "jose nunez")
        self.assertEqual(rm.norm(None), "")

    def test_iniciales(self):
        self.assertEqual(rm.iniciales("Samantha Rojas Díaz"), "SR")
        self.assertEqual(rm.iniciales("  "), "")
        self.assertEqual(rm.iniciales("madonna"), "M")

    def test_plural_y_horas(self):
        self.assertEqual(rm.plural(1, "bulto", "bultos"), "1 bulto")
        self.assertEqual(rm.plural(0, "bulto", "bultos"), "0 bultos")
        self.assertEqual(rm.fmt_horas_habiles(0.4), "menos de 1 h hábil")
        self.assertEqual(rm.fmt_horas_habiles(1.2), "1 h hábil")
        self.assertEqual(rm.fmt_horas_habiles(5.4), "5 h hábiles")

    def test_relativo_y_hace(self):
        self.assertEqual([rm.relativo(d) for d in (0, 1, -1, 3, -4)],
                         ["Hoy", "Mañana", "Ayer", "En 3 días", "Hace 4 días"])
        self.assertEqual(rm.hace(timedelta(seconds=20)), "hace un momento")
        self.assertEqual(rm.hace(timedelta(minutes=5)), "hace 5 min")
        self.assertEqual(rm.hace(timedelta(hours=7)), "hace 7 h")
        self.assertEqual(rm.hace(timedelta(days=3)), "hace 3 d")
        self.assertEqual(rm.hace(timedelta(days=60)), "")


class TestFechaEfectiva(unittest.TestCase):
    def test_confirmada_manda_sobre_propuesta_y_solicitada(self):
        r = _fila(confirmed_date=date(2026, 10, 5), proposed_date=date(2026, 10, 3))
        self.assertEqual(rm.fecha_efectiva(r)[:2], ("Confirmada", date(2026, 10, 5)))

    def test_propuesta_manda_sobre_solicitada(self):
        r = _fila(proposed_date=date(2026, 10, 3))
        self.assertEqual(rm.fecha_efectiva(r)[:2], ("Propuesta", date(2026, 10, 3)))

    def test_solo_solicitada(self):
        self.assertEqual(rm.fecha_efectiva(_fila())[:2], ("Solicitada", date(2026, 10, 1)))

    def test_sin_ninguna_fecha(self):
        r = _fila(requested_date=None)
        self.assertEqual(rm.fecha_efectiva(r), (None, None, None, None))

    def test_acepta_fechas_como_texto(self):
        r = _fila(requested_date="2026-10-01")
        self.assertEqual(rm.fecha_efectiva(r)[1], date(2026, 10, 1))


class TestSemaforo(unittest.TestCase):
    def _nivel(self, **kw):
        h = kw.pop("horas", None)
        f = _enriquecer([_fila(**kw)], horas=(lambda d, a, fer: h) if h is not None else None)[0]
        return f["m_al_nivel"], f["m_al_txt"]

    def test_sin_responder_verde_ambar_rojo(self):
        self.assertEqual(self._nivel(horas=0.5)[0], "verde")
        self.assertEqual(self._nivel(horas=1.99)[0], "verde")
        self.assertEqual(self._nivel(horas=2.0)[0], "ambar")
        self.assertEqual(self._nivel(horas=3.9)[0], "ambar")
        self.assertEqual(self._nivel(horas=4.0)[0], "rojo")
        self.assertIn("5 h 12 min hábiles", self._nivel(horas=5.2)[1])      # lenguaje humano: nunca HH:MM:SS
        self.assertIn("5 h hábiles", self._nivel(horas=5.0)[1])
        self.assertIn("45 min hábiles", self._nivel(horas=0.75)[1])
        self.assertNotIn(":", self._nivel(horas=0.5)[1].split("·")[1])

    def test_esperando_al_cliente_es_gris(self):
        self.assertEqual(self._nivel(status="propuesta_enviada")[0], "gris")
        self.assertEqual(self._nivel(status="esperando_cliente")[0], "gris")

    def test_agendado_para_hoy_con_horario_por_delante_es_ambar(self):
        n, txt = self._nivel(status="agenda_confirmada", confirmed_date=HOY,
                             confirmed_time_from=timedelta(hours=16), confirmed_time_to=timedelta(hours=16, minutes=30))
        self.assertEqual((n, txt), ("ambar", "Es hoy"))

    def test_agendado_hoy_pasada_media_hora_del_inicio_es_rojo(self):
        # ahora = 15:00; inicio 14:20 → pasaron 40 min
        n, txt = self._nivel(status="agenda_confirmada", confirmed_date=HOY,
                             confirmed_time_from=timedelta(hours=14, minutes=20), confirmed_time_to=timedelta(hours=15))
        self.assertEqual(n, "rojo")
        self.assertIn("Atrasado", txt)

    def test_agendado_hoy_dentro_de_los_30_min_de_gracia_no_es_rojo(self):
        n, _ = self._nivel(status="agenda_confirmada", confirmed_date=HOY,
                           confirmed_time_from=timedelta(hours=14, minutes=40), confirmed_time_to=timedelta(hours=15, minutes=10))
        self.assertEqual(n, "ambar")

    def test_agendado_con_fecha_vencida_es_rojo(self):
        n, txt = self._nivel(status="en_preparacion", confirmed_date=HOY - timedelta(days=2),
                             confirmed_time_from=timedelta(hours=9), confirmed_time_to=timedelta(hours=9, minutes=30))
        self.assertEqual(n, "rojo")
        self.assertIn("hace 2 d", txt)
        self.assertIn("en preparación", txt)

    def test_agendado_a_futuro_esta_en_orden(self):
        n, txt = self._nivel(status="agenda_confirmada", confirmed_date=HOY + timedelta(days=2),
                             confirmed_time_from=timedelta(hours=9), confirmed_time_to=timedelta(hours=9, minutes=30))
        self.assertEqual((n, txt), ("verde", "En orden"))

    def test_terminales(self):
        self.assertEqual(self._nivel(status="retirada")[0], "verde")
        self.assertEqual(self._nivel(status="cerrada")[0], "gris")
        self.assertEqual(self._nivel(status="rechazada")[0], "gris")
        self.assertEqual(self._nivel(status="fallida")[0], "gris")

    def test_vencida_solo_para_agendados_en_rojo(self):
        f = _enriquecer([_fila(status="agenda_confirmada", confirmed_date=HOY - timedelta(days=1),
                               confirmed_time_from=timedelta(hours=9), confirmed_time_to=timedelta(hours=10))])[0]
        self.assertTrue(f["m_vencida"])
        g = _enriquecer([_fila(horas_x=1)], horas=lambda d, a, fer: 9.0)[0]  # sin responder rojo, pero no agendada
        self.assertEqual(g["m_al_nivel"], "rojo")
        self.assertFalse(g["m_vencida"])


class TestHorarioDeCobertura(unittest.TestCase):
    """Daniel 2026-10-06: el reloj usa el mismo horario que el aviso de cobertura (08-17, colación 13-14), no 09-18."""

    def test_colacion_no_corre(self):
        # martes 12:00 → 15:00 = 1 h antes de la colación + 1 h después
        self.assertEqual(rm.horas_cobertura(datetime(2026, 9, 29, 12, 0), datetime(2026, 9, 29, 15, 0), (), COB), 2.0)
        # día completo = 8 h (9 h de horario menos 1 h de colación)
        self.assertEqual(rm.horas_cobertura(datetime(2026, 9, 29, 0, 0), datetime(2026, 9, 30, 0, 0), (), COB), 8.0)

    def test_fuera_de_horario_fin_de_semana_y_feriado_no_cuentan(self):
        self.assertEqual(rm.horas_cobertura(datetime(2026, 9, 29, 17, 0), datetime(2026, 9, 30, 8, 0), (), COB), 0.0)
        # viernes 16:00 → lunes 09:00: 1 h del viernes + 1 h del lunes
        self.assertEqual(rm.horas_cobertura(datetime(2026, 10, 2, 16, 0), datetime(2026, 10, 5, 9, 0), (), COB), 2.0)
        # con el lunes feriado no cuenta
        self.assertEqual(rm.horas_cobertura(datetime(2026, 10, 2, 16, 0), datetime(2026, 10, 5, 9, 0), {"2026-10-05"}, COB), 1.0)
        self.assertEqual(rm.horas_cobertura(datetime(2026, 9, 29, 15, 0), datetime(2026, 9, 29, 14, 0), (), COB), 0.0)

    def test_se_lee_de_las_mismas_variables_que_el_aviso_de_cobertura(self):
        import os
        guardado = {k: os.environ.get(k) for k in ("RETIROS_COBERTURA_DESDE", "RETIROS_COBERTURA_HASTA",
                                                  "RETIROS_COBERTURA_COLACION_DESDE", "RETIROS_COBERTURA_COLACION_HASTA")}
        try:
            for k in guardado:
                os.environ.pop(k, None)
            self.assertEqual(rm.cobertura_por_defecto(), COB)
            os.environ["RETIROS_COBERTURA_DESDE"] = "09:00"
            os.environ["RETIROS_COBERTURA_HASTA"] = "18:00"
            self.assertEqual(rm.cobertura_por_defecto(), {"desde": 540, "hasta": 1080, "col_d": 780, "col_h": 840})
            os.environ["RETIROS_COBERTURA_DESDE"] = "basura"
            self.assertEqual(rm.cobertura_por_defecto()["desde"], 480)     # un valor roto cae al horario confirmado
        finally:
            for k, v in guardado.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v

    def test_proxima_apertura(self):
        ap = lambda *a: rm.proxima_apertura(datetime(*a), (), COB)   # noqa: E731
        self.assertIsNone(ap(2026, 9, 29, 10, 0))                              # abierto
        self.assertEqual(ap(2026, 9, 29, 13, 30), datetime(2026, 9, 29, 14, 0))   # colación
        self.assertEqual(ap(2026, 9, 29, 6, 0), datetime(2026, 9, 29, 8, 0))      # antes de abrir
        self.assertEqual(ap(2026, 9, 29, 22, 41), datetime(2026, 9, 30, 8, 0))    # de noche → mañana
        self.assertEqual(ap(2026, 10, 2, 17, 30), datetime(2026, 10, 5, 8, 0))    # viernes tarde → lunes
        self.assertEqual(rm.proxima_apertura(datetime(2026, 10, 2, 17, 30), {"2026-10-05"}, COB), datetime(2026, 10, 6, 8, 0))

    def test_plazo_de_4_h_habiles_salta_la_colacion_y_la_noche(self):
        hh = lambda d, h, f=(): rm.horas_cobertura(d, h, f, COB)   # noqa: E731
        self.assertEqual(rm.plazo_sla(datetime(2026, 9, 29, 11, 0), 4.0, hh), datetime(2026, 9, 29, 16, 0))
        llego_de_noche = rm.plazo_sla(datetime(2026, 9, 29, 22, 41), 4.0, hh)    # el reloj parte a las 08:00
        self.assertEqual(llego_de_noche, datetime(2026, 9, 30, 12, 0))
        self.assertEqual(rm.fmt_cuando(llego_de_noche, date(2026, 9, 29)), "mañana 12:00")

    def test_textos_en_lenguaje_humano_sin_contador_en_cero(self):
        self.assertEqual(rm.dur_habil(0), "menos de 1 min")
        self.assertEqual(rm.dur_habil(45 * 60), "45 min hábiles")
        self.assertEqual(rm.dur_habil(3600), "1 h hábil")
        self.assertEqual(rm.dur_habil(4800), "1 h 20 min hábiles")
        self.assertEqual(rm.dur_habil(9 * 3600), "9 h hábiles")
        self.assertEqual(rm.dur_resta(2 * 3600 + 40 * 60), "2 h 40 min")
        self.assertEqual(rm.fmt_cuando(datetime(2026, 9, 28, 22, 41), date(2026, 9, 29)), "ayer 22:41")

    def test_llego_de_noche_no_dice_cero_dice_cuando_parte(self):
        t = rm.textos_reloj(0, 4 * 3600, False, "22:41", "mañana 08:00", "mañana 12:00")
        self.assertTrue(t["pausa0"])
        self.assertEqual(t["pill"], "Llegó 22:41 · el reloj parte mañana 08:00")
        self.assertEqual(t["sub"], "Plazo 4 h hábiles · vence mañana 12:00")
        self.assertEqual(t["tag"], "")
        for txt in t.values():
            self.assertNotIn("00:00", str(txt))

    def test_con_tiempo_acumulado_muestra_lo_que_queda_y_cuando_vence(self):
        pausa = rm.textos_reloj(4800, 4 * 3600, False, "10:00", "mañana 08:00", "mañana 10:40")
        self.assertFalse(pausa["pausa0"])
        self.assertEqual(pausa["pill"], "Sin responder · 1 h 20 min hábiles")
        self.assertEqual(pausa["tag"], "hasta mañana 08:00")
        self.assertEqual(pausa["sub"], "Quedan 2 h 40 min hábiles · vence mañana 10:40")
        corre = rm.textos_reloj(4800, 4 * 3600, True, "10:00", "", "hoy 15:40")
        self.assertEqual(corre["tag"], "")
        vencido = rm.textos_reloj(5 * 3600, 4 * 3600, True, "07:00", "", "hoy 12:00")
        self.assertEqual(vencido["sub"], "Plazo vencido (vencía hoy 12:00): responder ya")

    def test_si_quien_llama_pasa_horas_habiles_o_ventanas_se_usan_esas(self):
        # pickups_module pasa su _cc_horas_habiles (ya alineado a la cobertura) y mon["ventanas"]: una sola definición del horario.
        kw = dict(hoy=date(2026, 9, 29), ahora=datetime(2026, 9, 29, 22, 50),
                  utc_a_chile=lambda d: d - timedelta(hours=3), td_hhmm=lambda t: "", estados={}, grupos=[], relaciones={})
        f1 = dict(id=1, status="solicitud_recibida", created_at=datetime(2026, 9, 29, 14, 0))
        rm.enriquecer_filas([f1], horas_habiles=lambda d, h, fer=(): 3.0, **kw)
        self.assertEqual(f1["m_reloj"]["espera_s"], 3 * 3600)
        self.assertEqual(f1["m_reloj"]["ventanas"], "480-780,840-1020")
        f2 = dict(id=2, status="solicitud_recibida", created_at=datetime(2026, 9, 29, 14, 0))
        rm.enriquecer_filas([f2], ventanas=[(540, 1080)], **kw)               # horario de 09 a 18 sin colación, venido del servidor
        self.assertEqual(f2["m_reloj"]["ventanas"], "540-1080")
        self.assertEqual(f2["m_reloj"]["espera_s"], 7 * 3600)                 # 11:00 → 18:00
        self.assertEqual(f2["m_reloj"]["reanuda"], "mañana 09:00")

    def test_la_fila_que_llego_de_noche_trae_el_reloj_en_pausa(self):
        kw = dict(hoy=date(2026, 9, 29), ahora=datetime(2026, 9, 29, 22, 50), cobertura=COB,
                  utc_a_chile=lambda d: d - timedelta(hours=3), td_hhmm=lambda t: "", estados={}, grupos=[], relaciones={})
        nueva = dict(id=1, status="solicitud_recibida", created_at=datetime(2026, 9, 30, 1, 41))     # 22:41 Chile
        vieja = dict(id=2, status="solicitud_recibida", created_at=datetime(2026, 9, 29, 14, 0))     # 11:00 Chile (6 h hábiles)
        rm.enriquecer_filas([nueva, vieja], **kw)
        r = nueva["m_reloj"]
        self.assertFalse(r["abierta"])
        self.assertTrue(r["pausa0"])
        self.assertEqual((r["llego"], r["reanuda"], r["vence"]), ("22:41", "mañana 08:00", "mañana 12:00"))
        self.assertEqual(r["pill"], "Llegó 22:41 · el reloj parte mañana 08:00")
        self.assertEqual(nueva["m_al_nivel"], "verde")                       # nadie está atrasado: el reloj ni partió
        self.assertEqual(vieja["m_reloj"]["espera_s"], 5 * 3600)             # 11:00-13:00 + 14:00-17:00
        self.assertEqual(vieja["m_al_nivel"], "rojo")


class TestEnriquecer(unittest.TestCase):
    def test_no_pisa_ninguna_clave_existente(self):
        original = _fila()
        copia = dict(original)
        f = _enriquecer([original])[0]
        for k, v in copia.items():
            self.assertEqual(f[k], v, k)

    def test_busqueda_incluye_todo_lo_que_pide_daniel(self):
        f = _enriquecer([_fila()])[0]["m_search"]
        for esperado in ("ret-abc123", "jose nunez pena", "123456785", "maria perez", "56977468766",
                         "maria@empresa.cl", "factura", "1234", "pedro soto", "98765433",
                         "solicitud recibida", "por revisar", "web"):
            self.assertIn(esperado, f, esperado)

    def test_busqueda_por_responsable(self):
        f = _enriquecer([_fila(responsable_nombre="Samantha Rojas")])[0]
        self.assertIn("samantha rojas", f["m_search"])
        self.assertEqual(f["m_resp_ini"], "SR")

    def test_grupo_y_orden_de_estado(self):
        f = _enriquecer([_fila(status="en_preparacion", confirmed_date=HOY + timedelta(days=1),
                               confirmed_time_from=timedelta(hours=9), confirmed_time_to=timedelta(hours=10))])[0]
        self.assertEqual((f["m_grupo"], f["m_grupo_lbl"]), ("preparacion", "En preparación"))
        self.assertEqual(f["m_estado_idx"], list(ESTADOS).index("en_preparacion"))
        self.assertEqual(f["m_siguiente"], "Siguiente: entregar al cliente")

    def test_estado_desconocido_cae_en_otros(self):
        f = _enriquecer([_fila(status="algo_nuevo")])[0]
        self.assertEqual(f["m_grupo"], "otros")
        self.assertEqual(f["m_estado_idx"], 99)

    def test_fecha_texto_con_dia_de_la_semana_en_formato_chileno(self):
        f = _enriquecer([_fila()])[0]                    # 2026-10-01 es jueves
        self.assertEqual(f["m_fecha_txt"], "jue 01-10-2026")
        self.assertEqual((f["m_desde"], f["m_hasta"]), ("09:00", "09:30"))
        self.assertEqual(f["m_dias"], 2)
        self.assertEqual((f["m_rel_txt"], f["m_rel_nivel"]), ("En 2 días", "verde"))

    def test_fecha_de_hoy_es_ambar_y_vencida_es_roja(self):
        hoy = _enriquecer([_fila(requested_date=HOY)])[0]
        self.assertEqual((hoy["m_rel_txt"], hoy["m_rel_nivel"]), ("Hoy", "ambar"))
        ayer = _enriquecer([_fila(requested_date=HOY - timedelta(days=1))])[0]
        self.assertEqual((ayer["m_rel_txt"], ayer["m_rel_nivel"]), ("Ayer", "rojo"))

    def test_terminados_no_muestran_chip_relativo(self):
        f = _enriquecer([_fila(status="retirada", requested_date=HOY - timedelta(days=5))])[0]
        self.assertEqual((f["m_rel_txt"], f["m_rel_nivel"]), ("", "gris"))

    def test_sin_fecha(self):
        f = _enriquecer([_fila(requested_date=None)])[0]
        self.assertIsNone(f["m_dias"])
        self.assertEqual(f["m_fecha_txt"], "")

    def test_semana_actual(self):
        dentro = _enriquecer([_fila(requested_date=date(2026, 10, 2))])[0]     # viernes de esta semana
        fuera = _enriquecer([_fila(requested_date=date(2026, 10, 5))])[0]      # lunes siguiente
        rechazada = _enriquecer([_fila(status="rechazada", requested_date=date(2026, 10, 2))])[0]
        self.assertTrue(dentro["m_en_semana"])
        self.assertFalse(fuera["m_en_semana"])
        self.assertFalse(rechazada["m_en_semana"])   # igual que el KPI "Retiros esta semana"

    def test_semana_y_hoy_usan_las_mismas_reglas_que_las_tarjetas(self):
        # La tarjeta cuenta COALESCE(confirmada, solicitada): una propuesta NO cuenta.
        f = _enriquecer([_fila(requested_date=date(2026, 10, 12), proposed_date=date(2026, 10, 1))])[0]
        self.assertFalse(f["m_en_semana"])
        g = _enriquecer([_fila(requested_date=date(2026, 10, 12), confirmed_date=date(2026, 10, 1))])[0]
        self.assertTrue(g["m_en_semana"])
        self.assertEqual((g["m_req_iso"], g["m_conf_iso"]), ("2026-10-12", "2026-10-01"))
        h = _enriquecer([_fila(requested_date=None)])[0]
        self.assertEqual((h["m_req_iso"], h["m_conf_iso"]), ("", ""))

    def test_carga_y_peso_sin_confirmar(self):
        f = _enriquecer([_fila()])[0]
        self.assertEqual(f["m_carga_txt"], "1 bulto")
        self.assertTrue(f["m_sin_peso"])
        g = _enriquecer([_fila(total_packages=3, total_weight_kg=12.5)])[0]
        self.assertEqual(g["m_carga_txt"], "3 bultos")
        self.assertFalse(g["m_sin_peso"])

    def test_carga_nunca_muestra_cero_kg_como_si_fuera_un_dato(self):
        # Daniel 2026-10-06: un retiro completado salía «0.0 kg · PV 0.0». Lo que no se midió no se muestra.
        sin = _enriquecer([_fila(status="retirada", total_weight_kg=0, total_volumetric_weight=0)])[0]
        self.assertTrue(sin["m_sin_peso"])
        self.assertEqual(sin["m_carga_det"], [])
        self.assertEqual(sin["m_m3_txt"], "")
        # los valores reales (peso_real_kg / peso_vol_kg / total_volume_m3) mandan sobre los del formulario
        real = _enriquecer([_fila(status="retirada", total_weight_kg=0, peso_real_kg=48.34, peso_vol_kg=51.0,
                                  total_volume_m3=0.25)])[0]
        self.assertFalse(real["m_sin_peso"])
        self.assertEqual(real["m_carga_det"], ["48.3 kg", "PV 51.0"])
        self.assertEqual(real["m_m3_txt"], "0.25 m³")
        # solo uno de los dos pesos: se muestra únicamente ese
        solo_pv = _enriquecer([_fila(peso_vol_kg=12.0)])[0]
        self.assertEqual(solo_pv["m_carga_det"], ["PV 12.0"])
        # volumen sin ceros de relleno
        self.assertEqual(rm.fmt_m3(1.35), "1.35")
        self.assertEqual(rm.fmt_m3(0.004), "0.004")
        self.assertEqual(rm.fmt_m3(2), "2")

    def test_quien_retira_es_el_mismo_cliente(self):
        igual = _enriquecer([_fila(pickup_person_name="  JOSÉ núñez peña ")])[0]
        self.assertTrue(igual["m_ret_mismo"])
        por_rut = _enriquecer([_fila(pickup_person_name="Representante", pickup_person_rut="12345678-5")])[0]
        self.assertTrue(por_rut["m_ret_mismo"])
        otro = _enriquecer([_fila()])[0]                       # Pedro Soto, otro RUT
        self.assertFalse(otro["m_ret_mismo"])
        sin = _enriquecer([_fila(pickup_person_name="", pickup_person_rut="")])[0]
        self.assertFalse(sin["m_ret_mismo"])

    def test_orden_de_la_columna_tiempo_va_de_lo_mas_urgente_a_lo_menos(self):
        sin_resp = _enriquecer([_fila(created_at=datetime(2026, 9, 29, 9, 0))])[0]       # 06:00 Chile: 9 h → rojo
        ambar = _enriquecer([_fila(created_at=datetime(2026, 9, 29, 15, 30))])[0]        # 2,5 h → ámbar
        verde = _enriquecer([_fila(status="agenda_confirmada", confirmed_date=date(2026, 10, 5))])[0]
        gris = _enriquecer([_fila(status="propuesta_enviada")])[0]
        orden = [f["m_tiempo_ord"] for f in (sin_resp, ambar, verde, gris)]
        self.assertEqual(orden, sorted(orden, reverse=True))
        self.assertEqual(len(set(orden)), 4)
        # entre dos rojos, el que más espera va primero
        peor = _enriquecer([_fila(created_at=datetime(2026, 9, 29, 8, 0))])[0]
        self.assertGreater(peor["m_tiempo_ord"], sin_resp["m_tiempo_ord"])

    def test_calidad_de_retiro_interno_no_se_muestra_como_cero(self):
        interno = _enriquecer([_fila(request_source="backoffice", information_quality_score=0)])[0]
        self.assertTrue(interno["m_cal_na"])
        web_cero = _enriquecer([_fila(request_source="web", information_quality_score=0)])[0]
        self.assertFalse(web_cero["m_cal_na"])
        interno_con_valor = _enriquecer([_fila(request_source="backoffice", information_quality_score=64)])[0]
        self.assertFalse(interno_con_valor["m_cal_na"])

    def test_documento_y_su_validacion(self):
        ok = _enriquecer([_fila(doc_validation_status="ok")])[0]
        self.assertEqual((ok["m_doc_val_nivel"], ok["m_doc_val_txt"]), ("verde", "Validado"))
        inc = _enriquecer([_fila(doc_validation_status="incompleto")])[0]
        self.assertEqual(inc["m_doc_val_nivel"], "ambar")
        pen = _enriquecer([_fila(doc_validation_status=None)])[0]
        self.assertEqual((pen["m_doc_val_nivel"], pen["m_doc_val_txt"]), ("gris", "Por validar"))
        sin = _enriquecer([_fila(document_number=None)])[0]
        self.assertEqual(sin["m_doc_val_txt"], "")
        self.assertEqual(sin["m_siguiente"], "Siguiente: agregar la factura o boleta")

    def test_quien_retira_usa_la_relacion(self):
        self.assertEqual(_enriquecer([_fila()])[0]["m_relacion"], "Chofer")
        self.assertEqual(_enriquecer([_fila(pickup_person_relation="")])[0]["m_relacion"], "")

    def test_origen_y_hace(self):
        f = _enriquecer([_fila()])[0]
        self.assertEqual(f["m_origen"], "Web")
        self.assertEqual(f["m_creado_txt"], "29-09-2026 15:00")
        self.assertEqual(f["m_hace"], "hace un momento")
        self.assertEqual(_enriquecer([_fila(request_source="backoffice")])[0]["m_origen"], "Interno")

    def test_csv_trae_los_datos_completos(self):
        c = _enriquecer([_fila()])[0]["m_csv"]
        self.assertEqual(c["Solicitud"], "RET-ABC123")
        self.assertEqual(c["Documento"], "FACTURA 1234")
        self.assertEqual(c["Fecha de retiro"], "01-10-2026")
        self.assertEqual(c["Horario"], "09:00-09:30")
        self.assertEqual(c["Canal"], "Web")
        self.assertEqual(c["Responder antes de"], "las 19:00")   # el plazo del reloj de respuesta (columna Tiempo); aquí el reloj de prueba es corrido
        retirado = _enriquecer([_fila(status="retirada")])[0]["m_csv"]
        self.assertEqual(retirado["Responder antes de"], "")

    def test_linea_de_tiempo_solo_lo_entendible_y_maximo_4(self):
        logs = {1: [
            {"action": "email_pendiente", "created_at": datetime(2026, 9, 29, 17, 0), "actor_name": "sistema"},
            {"action": "estado_actualizado", "new_status": "en_revision", "created_at": datetime(2026, 9, 29, 16, 0), "actor_name": "Samantha"},
            {"action": "propuesta_enviada", "created_at": datetime(2026, 9, 29, 15, 0), "actor_name": "Samantha"},
            {"action": "erp_actualizado", "created_at": datetime(2026, 9, 29, 14, 0), "actor_name": "sistema"},
            {"action": "doc_validada", "created_at": datetime(2026, 9, 29, 13, 0), "actor_name": "sistema"},
            {"action": "creada", "created_at": datetime(2026, 9, 29, 12, 0), "actor_name": "Cliente"},
            {"action": "cliente_confirmo", "created_at": datetime(2026, 9, 29, 11, 0), "actor_name": "Cliente"},
        ]}
        tl = _enriquecer([_fila()], logs=logs)[0]["m_timeline"]
        self.assertEqual([e["txt"] for e in tl],
                         ["Estado: En revisión", "Propuesta enviada al cliente", "Documento validado", "Solicitud creada"])
        self.assertEqual(tl[0]["cuando"], "29-09-2026 13:00")   # 16:00 UTC → 13:00 Chile (con el desfase de prueba)
        self.assertEqual(tl[0]["quien"], "Samantha")

    def test_sin_historial(self):
        self.assertEqual(_enriquecer([_fila()])[0]["m_timeline"], [])


class TestDatosDelDetalle(unittest.TestCase):
    def test_un_bloque_por_retiro_sin_campos_repetidos(self):
        filas = _enriquecer([_fila(id=7, total_packages=3, total_weight_kg=12.55, total_volumetric_weight=8,
                                   tiempo_estimado_min=25, created_by_user_name="Samantha")])
        d = rm.armar_datos(filas, lambda v: f"<{v}>")["7"]
        c, x = d["csv"], d["x"]
        self.assertEqual((c["Solicitud"], c["Contacto"], c["Teléfono"], c["Correo"]),
                         ("RET-ABC123", "María Pérez", "+56 9 7746 8766", "maria@empresa.cl"))
        self.assertEqual((c["RUT cliente"], c["RUT quien retira"]), ("<12.345.678-5>", "<9.876.543-3>"))
        self.assertEqual((c["Quién retira"], c["Documento"], c["Bultos"], c["Peso kg"]),
                         ("Pedro Soto", "FACTURA 1234", 3, 12.6))
        self.assertEqual((c["Canal"], c["Creada"], c["Etapa"]), ("Web", "29-09-2026 15:00", "Por revisar"))
        self.assertEqual((x["rel"], x["min"], x["por"], x["sp"], x["dv"]), ("Chofer", 25, "Samantha", False, "Por validar"))
        # nada de lo que ya está en "csv" se repite en "x"
        self.assertEqual(sorted(x), ["dv", "min", "por", "ptel", "rel", "sp", "tl"])

    def test_el_rut_de_la_exportacion_sale_con_formato_chileno(self):
        filas = _enriquecer([_fila(id=8, customer_rut="123456785")])
        self.assertEqual(rm.armar_datos(filas, lambda v: "12.345.678-5")["8"]["csv"]["RUT cliente"], "12.345.678-5")

    def test_sin_rut_no_llama_al_formateador(self):
        filas = _enriquecer([_fila(id=8, customer_rut=None, pickup_person_rut="")])
        llamadas = []
        c = rm.armar_datos(filas, lambda v: llamadas.append(v) or v)["8"]["csv"]
        self.assertEqual((c["RUT cliente"], c["RUT quien retira"]), ("", ""))
        self.assertEqual(llamadas, [])

    def test_no_altera_la_fila_original(self):
        filas = _enriquecer([_fila(id=9)])
        antes = dict(filas[0]["m_csv"])
        rm.armar_datos(filas, lambda v: "X")
        self.assertEqual(filas[0]["m_csv"], antes)     # el formato chileno va solo en la copia del JSON

    def test_filas_sin_enriquecer_se_ignoran(self):
        self.assertEqual(rm.armar_datos([_fila(id=9)], lambda v: v), {})

    def test_el_json_es_serializable_y_liviano(self):
        import json
        filas = _enriquecer([_fila(id=1), _fila(id=2, requested_date=None)])
        datos = rm.armar_datos(filas, lambda v: v)
        self.assertIn("1", json.loads(json.dumps(datos)))
        self.assertLess(len(json.dumps(datos["1"], ensure_ascii=False)), 1100)


class TestTarjetasKpi(unittest.TestCase):
    def test_delta_hacia_arriba_es_bueno_cuando_mas_es_mejor(self):
        self.assertEqual(rm.delta(7, 5), {"dir": "up", "txt": "+2", "bueno": True})

    def test_delta_del_ciclo_menos_es_mejor(self):
        d = rm.delta(20.5, 26.0, menor_es_mejor=True, unidad=" h", decimales=1)
        self.assertEqual(d, {"dir": "down", "txt": "−5,5 h", "bueno": True})
        peor = rm.delta(30.0, 26.0, menor_es_mejor=True, unidad=" h", decimales=1)
        self.assertFalse(peor["bueno"])

    def test_delta_sin_cambio_y_sin_dato(self):
        self.assertEqual(rm.delta(5, 5), {"dir": "flat", "txt": "sin cambio", "bueno": None})
        self.assertIsNone(rm.delta(5, None))
        self.assertIsNone(rm.delta(None, 5))

    def test_delta_de_puntos_porcentuales(self):
        self.assertEqual(rm.delta(80, 72, unidad=" pts"), {"dir": "up", "txt": "+8 pts", "bueno": True})

    def test_spark_necesita_dos_valores_reales(self):
        self.assertIsNone(rm.spark([]))
        self.assertIsNone(rm.spark([3]))
        self.assertIsNone(rm.spark([None, 4, None]))

    def test_spark_dentro_del_lienzo(self):
        s = rm.spark([1, 5, 3, 9], ancho=100, alto=30, pad=4)
        pts = [tuple(map(float, p.split(","))) for p in s["linea"].split()]
        self.assertEqual(len(pts), 4)
        for x, y in pts:
            self.assertTrue(4 <= x <= 96 and 4 <= y <= 26, (x, y))
        self.assertEqual(pts[0][0], 4.0)
        self.assertEqual(pts[-1][0], 96.0)
        # el valor más alto queda arriba (y menor)
        self.assertEqual(min(pts, key=lambda p: p[1])[0], pts[3][0])

    def test_spark_serie_plana_va_al_medio(self):
        s = rm.spark([4, 4, 4], alto=30)
        self.assertTrue(all(float(p.split(",")[1]) == 15.0 for p in s["linea"].split()))

    def test_barras_de_la_semana(self):
        lunes, domingo = rm.semana(HOY)
        self.assertEqual((lunes, domingo), (date(2026, 9, 28), date(2026, 10, 4)))
        b = rm.barras_semana({"2026-09-28": 2, "2026-09-29": 4, "2026-10-02": 1}, lunes, HOY)
        self.assertEqual([x["n"] for x in b], [2, 4, 0, 0, 1, 0, 0])
        self.assertEqual([x["hoy"] for x in b], [False, True, False, False, False, False, False])
        self.assertEqual([x["futuro"] for x in b], [False, False, True, True, True, True, True])
        self.assertEqual(max(x["h"] for x in b), 34)
        self.assertTrue(all(x["h"] >= 3 for x in b))
        self.assertEqual(b[1]["tit"], "mar 29-09: 4 retiros")
        self.assertEqual(b[4]["tit"], "vie 02-10: 1 retiro")

    def test_barras_sin_datos_no_dividen_por_cero(self):
        b = rm.barras_semana({}, date(2026, 9, 28), HOY)
        self.assertEqual([x["n"] for x in b], [0] * 7)



class TestPlazoSla(unittest.TestCase):
    """Reloj de "Sin responder": hora límite en horas hábiles (lun-vie 09-18)."""

    @staticmethod
    def _hh(desde, hasta, feriados=()):
        total, dia = 0.0, desde.date()
        while dia <= hasta.date():
            if dia.weekday() < 5 and dia.isoformat() not in feriados:
                ini = datetime.combine(dia, datetime.min.time()).replace(hour=9)
                fin = datetime.combine(dia, datetime.min.time()).replace(hour=18)
                a, b = max(ini, desde), min(fin, hasta)
                if b > a:
                    total += (b - a).total_seconds() / 3600
            dia += timedelta(days=1)
        return round(total, 2)

    def test_cruza_la_noche(self):
        p = rm.plazo_sla(datetime(2026, 9, 29, 16, 10), 4.0, self._hh)   # martes 16:10
        self.assertEqual(p, datetime(2026, 9, 30, 11, 10))
        self.assertEqual(rm.fmt_plazo(p, date(2026, 9, 29)), "mañana 11:10")

    def test_salta_fin_de_semana_y_feriado(self):
        p = rm.plazo_sla(datetime(2026, 10, 2, 17, 0), 4.0, self._hh, {"2026-10-05"})   # viernes 17:00
        self.assertEqual(p, datetime(2026, 10, 6, 12, 0))
        self.assertEqual(rm.fmt_plazo(p, date(2026, 10, 2)), "el mar 06-10 12:00")

    def test_la_fila_trae_el_reloj(self):
        # Horario de COBERTURA real (lun-vie 08:00-17:00, colación 13:00-14:00 que no corre), no el 09-18 del Centro de control
        kw = dict(hoy=date(2026, 9, 29), ahora=datetime(2026, 9, 29, 11, 30), cobertura=COB,
                  utc_a_chile=lambda d: d - timedelta(hours=3), td_hhmm=lambda t: "",
                  estados={}, grupos=[], relaciones={})
        sin_resp = dict(id=1, status="solicitud_recibida", created_at=datetime(2026, 9, 29, 14, 0))  # 11:00 Chile
        retirado = dict(id=2, status="retirada", created_at=datetime(2026, 9, 29, 14, 0))
        rm.enriquecer_filas([sin_resp, retirado], **kw)
        self.assertEqual(sin_resp["m_reloj"]["espera_s"], 1800)     # 11:00 → 11:30
        self.assertEqual(sin_resp["m_reloj"]["plazo_txt"], "las 16:00")   # 11:00 + 4 h hábiles, saltando la colación 13-14
        self.assertEqual(sin_resp["m_reloj"]["vence"], "hoy 16:00")
        self.assertTrue(sin_resp["m_reloj"]["abierta"])
        self.assertEqual(sin_resp["m_reloj"]["ventanas"], "480-780,840-1020")   # los tramos viajan al navegador (data-ventanas)
        self.assertIsNone(retirado["m_reloj"])


if __name__ == "__main__":
    unittest.main()

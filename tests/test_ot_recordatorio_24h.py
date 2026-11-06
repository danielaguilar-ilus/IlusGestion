# -*- coding: utf-8 -*-
"""RECORDATORIO DE 24 HORAS DE LA OT al cliente y al técnico + invitación de calendario al cliente (Daniel, 5-nov-2026).

Daniel: «le avisemos al cliente a partir de ahora… necesito que le recordemos también 24 horas tanto al técnico como al cliente… eso lo
podemos establecer por el sistema de comunicaciones, déjalo todo por el front» y «que se cree con un envío de correo el calendario, una
instancia en el calendario que le pida permiso».

Se prueba:
  1. Las reglas de CUÁNDO (ot_recordatorio_24h.py, puro): 24 h antes, solo en horario de cobertura (lunes a viernes 08:00–17:00 sin la
     colación), el viernes cuando las 24 h caen en fin de semana, nada si la OT se creó dentro de las 24 h, nada a menos de una hora.
  2. Los textos (sin plata, con las variables que el código arma) y el registro (dos interruptores propios en Control de correos).
  3. El barrido REAL de app.py en un mundo de mentira (base y correo en memoria; jamás sale un correo: REGLA #22): una vez por fecha de
     visita, cada correo por su llave, el interruptor del frente lo frena SIN reservar, la falla del proveedor libera la reserva.
  4. La invitación de calendario del cliente (.ics con RSVP, sin enlace interno) y que la de los técnicos siga igual.
  5. Cableado: colgado de simpliroute-poll, ruta con token, tabla barata (REGLA #18), la OT terminada al cliente ya no depende de una variable.

Correr:  python -m pytest tests/test_ot_recordatorio_24h.py -q -p no:warnings
"""
import datetime as dt
import re
import unittest
import zoneinfo

import comm_correos_registro as R
import ot_comunicacion as C
import ot_correos_tecnicos as OTC
import ot_recordatorio_24h as O
from tests import _fuente_codigo as FC
from tests.test_ot_comunicacion_envio import MundoOT

TZ = zoneinfo.ZoneInfo("America/Santiago")
HABIL = lambda d: d.weekday() < 5                      # noqa: E731  (sin feriados: las pruebas usan días corrientes de nov-2026)
PLATA = re.compile(r"\$\s?\d|\bcosto|\bmonto|\bprecio|\bmargen|\bproveedor|\bfactur|\bsaldo|\bcobr|\bvalorizad|\bOC\b|n[°º] de oc", re.I)

# Calendario de la semana de las pruebas (2026): jue 5 · vie 6 · sáb 7 · dom 8 · lun 9 · mar 10 · mié 11 de noviembre.
def F(d, h=0, m=0):
    return dt.datetime(2026, 11, d, h, m)


class TestCuandoToca(unittest.TestCase):
    def test_sale_24_horas_antes_si_eso_cae_en_cobertura(self):
        ini = F(10, 10, 0)                                           # martes 10:00 → lunes 10:00
        self.assertEqual(O.momento_envio(ini, HABIL), F(9, 10, 0))
        self.assertEqual(O.toca_recordar(ini, None, F(9, 9, 50), HABIL)[0], False)          # todavía no son 24 horas
        self.assertEqual(O.toca_recordar(ini, None, F(9, 10, 0), HABIL)[0], True)
        self.assertEqual(O.toca_recordar(ini, None, F(9, 16, 50), HABIL)[0], True)          # el barrido corre cada 10 min: tiene todo el día

    def test_nunca_fuera_del_horario_de_cobertura(self):
        ini = F(10, 10, 0)
        for ahora, que in ((F(9, 17, 0), "17:00 ya es fuera"), (F(9, 13, 30), "colación"), (F(9, 7, 59), "antes de las 08:00"),
                           (F(9, 22, 0), "de noche")):
            self.assertFalse(O.toca_recordar(ini, None, ahora, HABIL)[0], que)

    def test_si_las_24_horas_caen_en_fin_de_semana_sale_el_viernes_por_la_tarde(self):
        ini = F(9, 8, 30)                                            # lunes 08:30 → domingo 08:30 (no hay cobertura)
        envio = O.momento_envio(ini, HABIL)
        self.assertEqual(envio, F(6, 15, 59))                        # viernes: una hora antes del cierre del último tramo
        self.assertFalse(O.toca_recordar(ini, None, F(6, 15, 50), HABIL)[0])
        self.assertTrue(O.toca_recordar(ini, None, F(6, 16, 0), HABIL)[0])
        self.assertTrue(O.toca_recordar(ini, None, F(6, 16, 50), HABIL)[0])
        self.assertFalse(O.toca_recordar(ini, None, F(6, 17, 0), HABIL)[0])                 # 17:00 ya no es cobertura

    def test_si_caen_en_la_colacion_sale_antes_de_comer(self):
        ini = F(10, 13, 30)                                          # martes 13:30 → lunes 13:30 (colación)
        self.assertEqual(O.momento_envio(ini, HABIL), F(9, 11, 59))
        self.assertTrue(O.toca_recordar(ini, None, F(9, 12, 0), HABIL)[0])

    def test_con_un_feriado_busca_el_ultimo_dia_habil_anterior(self):
        feriados = {dt.date(2026, 11, 9)}                            # lunes feriado (solo para la prueba)
        habil = lambda d: d.weekday() < 5 and d not in feriados      # noqa: E731
        ini = F(10, 9, 0)                                            # martes 09:00 → lunes 09:00 (feriado)
        self.assertEqual(O.momento_envio(ini, habil).date(), dt.date(2026, 11, 6))          # el viernes

    def test_una_ot_creada_despues_de_ese_momento_no_recibe_recordatorio(self):
        ini = F(10, 10, 0)
        self.assertTrue(O.toca_recordar(ini, F(5, 12, 0), F(9, 10, 5), HABIL)[0])           # creada hace días
        ok, motivo = O.toca_recordar(ini, F(9, 10, 30), F(9, 11, 0), HABIL)                 # creada dentro de las 24 horas previas
        self.assertFalse(ok)
        self.assertIn("aviso de creación", motivo)

    def test_no_se_recuerda_una_visita_que_empieza_en_menos_de_una_hora(self):
        ini = F(10, 10, 0)
        self.assertFalse(O.toca_recordar(ini, None, F(10, 9, 0), HABIL)[0])
        self.assertTrue(O.toca_recordar(ini, None, F(10, 8, 30), HABIL)[0])                 # si no salió el lunes, aún llega a tiempo

    def test_sin_hora_la_visita_empieza_a_las_8_y_sin_fecha_no_hay_nada(self):
        self.assertEqual(O.inicio_ot(dt.date(2026, 11, 10), None), F(10, 8, 0))
        self.assertEqual(O.inicio_ot("2026-11-10", "09:30:00"), F(10, 9, 30))
        self.assertIsNone(O.inicio_ot(dt.date(2099, 12, 31), None))                         # la fecha «sin fecha» (OT pendiente por falta de stock)
        self.assertIsNone(O.inicio_ot(None, "10:00"))
        self.assertFalse(O.toca_recordar(None, None, F(9, 10, 0), HABIL)[0])

    def test_la_clave_de_inicio_cambia_si_se_reagenda(self):
        self.assertNotEqual(O.clave_inicio(F(10, 10, 0)), O.clave_inicio(F(11, 10, 0)))
        self.assertEqual(O.clave_inicio(F(10, 10, 0)), "2026-11-10 10:00")

    def test_dia_de_la_visita_dicho_como_persona(self):
        hoy = dt.date(2026, 11, 9)
        self.assertEqual(O.dia_visita_txt(F(9, 10), hoy), "hoy")
        self.assertEqual(O.dia_visita_txt(F(10, 10), hoy), "mañana")
        self.assertEqual(O.dia_visita_txt(F(12, 10), hoy), "el jueves 12/11/2026")


class TestTextosYRegistro(unittest.TestCase):
    OT = {"id": 77, "numero_ot": "OT-2026-00077", "cliente": "Gimnasio Demo SpA", "tipo_label": "Instalación",
          "fecha": dt.date(2026, 11, 10), "hora_inicio": dt.timedelta(hours=10), "hora_fin": dt.timedelta(hours=12),
          "direccion": "Av. Demo 123, Las Condes", "equipos": ["Trotadora ILUS Pro 9000"], "tecnicos": ["Lenin Urbina"]}

    def test_las_variables_dicen_el_dia_y_el_horario(self):
        v = O.variables_recordatorio(self.OT, F(10, 10, 0), dt.date(2026, 11, 9), nombre_destinatario="Ana", link_ot="https://ilus.test/ot/77")
        self.assertEqual((v["dia"], v["fecha"], v["horario"]), ("mañana", "10/11/2026", "10:00 – 12:00"))
        self.assertEqual(v["saludo"], "Hola Ana,")

    def test_los_dos_textos_usan_solo_variables_que_el_codigo_arma_y_no_llevan_plata(self):
        v = O.variables_recordatorio(self.OT, F(10, 10, 0), dt.date(2026, 11, 9), nombre_destinatario="Ana", link_ot="https://ilus.test/ot/77")
        for slug in (C.PLANTILLA_RECORDATORIO, C.PLANTILLA_RECORDATORIO_TECNICO):
            a, c = C.SEMILLAS[slug]
            self.assertLessEqual(set(re.findall(r"\{\{(\w+)\}\}", a + c)), set(v), slug)
            asunto, cuerpo = C.armar_correo(slug, v)
            texto = re.sub(r"<[^>]+>", " ", asunto + " " + cuerpo)
            self.assertNotRegex(texto, PLATA, slug)
            self.assertNotIn("{{", asunto + cuerpo, slug)
            self.assertIn("OT-2026-00077", asunto, slug)
        # el cliente NO recibe el enlace a la ficha interna (pide sesión); el técnico sí
        self.assertNotIn("{{link_ot}}", C.SEMILLAS[C.PLANTILLA_RECORDATORIO][1])
        self.assertIn("{{link_ot}}", C.SEMILLAS[C.PLANTILLA_RECORDATORIO_TECNICO][1])

    def test_el_registro_los_pone_a_la_vista_con_su_propio_interruptor_y_su_llave(self):
        cli, tec = R.por_clave(C.CORREO_RECORDATORIO_CLIENTE), R.por_clave(C.CORREO_RECORDATORIO_TECNICO)
        self.assertEqual((cli["llave"], cli["destinatario"], cli["modulo_ui"]), ("mantenciones", "cliente", "ot"))
        self.assertEqual((tec["llave"], tec["destinatario"], tec["modulo_ui"]), ("ot_tecnicos", "tecnico", "ot"))
        for c in (cli, tec):
            self.assertEqual(c["condiciones"], ())                       # nada por variable de entorno: todo por el front
            self.assertFalse(c["protegido"])
            self.assertEqual(R.estado_efectivo(c, environ={})["efectivo"], "sale")
        self.assertEqual(cli["plantilla"], ("mantenciones", C.PLANTILLA_RECORDATORIO))
        self.assertEqual(tec["plantilla"], ("mantenciones", C.PLANTILLA_RECORDATORIO_TECNICO))

    def test_las_plantillas_se_ofrecen_en_el_editor_de_comunicaciones(self):
        js = FC.texto("static/comunicaciones_index.js")
        for slug in (C.PLANTILLA_RECORDATORIO, C.PLANTILLA_RECORDATORIO_TECNICO):
            self.assertRegex(js, r"key:\s*'" + slug + r"'[^}]*grupo:'ot_terminada'", slug)
        self.assertIn("{{dia}}", js)
        self.assertIn("{{horario}}", js)


# ═══════════════════════════════════════════════════════════════════════════════════════════════════
#  El barrido REAL de app.py en un mundo de mentira
# ═══════════════════════════════════════════════════════════════════════════════════════════════════
FUNCS_REC = ("_ot_rec_reservar", "_ot_rec_liberar", "_ot_rec_log", "_ot_recordatorio_barrido", "_ensure_ot_recordatorios_tabla")


class MundoRec(MundoOT):
    """MundoOT (la puerta de envío REAL y la base en memoria) + el barrido de recordatorios."""

    def __init__(self, **kw):
        super().__init__(estado="programada", **kw)
        self.reservas = set()
        self.bitacora = []
        self.visitas = []                                  # lo que devolvería la consulta de candidatos
        a = self.amb
        FC.extraer(a, "app.py", FUNCS_REC, ())
        self._f_all, self._f_exec, self._f_rc = a["mysql_fetchall"], a["mysql_execute"], a["mysql_execute_returning_rowcount"]
        a.update({"_orec": O, "timezone": dt.timezone, "timedelta": dt.timedelta, "datetime": dt.datetime,
                  "mysql_fetchall": self._candidatos, "mysql_execute": self._exec, "mysql_execute_returning_rowcount": self._rc,
                  "_brand_subject": lambda t: "ILUS · " + t,
                  # la ficha de la OT con un «qué hay que hacer» que MEZCLA instrucciones y plata (la plata no debe llegar al técnico)
                  "_ot_tec_filas": lambda vids: {int(v): {"id": int(v), "numero_ot": self.fila.get("numero_ot"), "cliente": self.fila.get("cli_razon"),
                                                          "tipo_label": "Instalación", "direccion": "Av. Demo 123, Las Condes",
                                                          "descripcion": "Revisar la banda de la trotadora. Cobrar $50.000 de extra al cliente."}
                                                 for v in vids}})
        self.visita()                                      # una visita por omisión: martes 10-nov 10:00, creada hace días

    def visita(self, **cambios):
        base = dict(self.fila)
        base.update(estado="programada", fecha_programada=dt.date(2026, 11, 10), hora_inicio=dt.timedelta(hours=10),
                    hora_fin=dt.timedelta(hours=12), created_at=dt.datetime(2026, 11, 2, 15, 0), ot_madre_id=None, ot_relacion=None,
                    tipo="instalacion", contacto_tel="+56 9 8765 4321")
        base.update(cambios)
        self.visitas = [base]
        return base

    def _candidatos(self, sql, params=()):
        s = " ".join(sql.split())
        if "FROM mant_visitas v LEFT JOIN mant_clientes c" in s and "fecha_programada BETWEEN" in s:
            return [dict(v) for v in self.visitas]
        return self._f_all(sql, params)

    def _exec(self, sql, params=()):
        s = " ".join(sql.split())
        if s.startswith("DELETE FROM mant_ot_recordatorios"):
            self.reservas.discard(tuple(params))
            return
        if s.startswith("INSERT INTO mant_logs"):
            self.bitacora.extend(params[i:i + 5] for i in range(0, len(params), 5))
            return
        return self._f_exec(sql, params)

    def _rc(self, sql, params=()):
        s = " ".join(sql.split())
        if s.startswith("INSERT IGNORE INTO mant_ot_recordatorios"):
            if getattr(self, "romper_tabla", False):
                raise RuntimeError("la tabla no existe")
            k = tuple(params)
            if k in self.reservas:
                return 0
            self.reservas.add(k)
            return 1
        return self._f_rc(sql, params)

    def barrer(self, ahora, **kw):
        self.m.enviados_reales.clear() if kw.pop("limpiar", False) else None
        return self.amb["_ot_recordatorio_barrido"](ahora=ahora, **kw)

    def para(self, correo):
        return [e for e in self.m.enviados_reales if e["to"] == correo]


def LUN(h, m=0):
    return dt.datetime(2026, 11, 9, h, m, tzinfo=TZ)


class _Base(unittest.TestCase):
    def mundo(self, **kw):
        w = MundoRec(**kw)
        self.addCleanup(w.cerrar)
        return w


class TestBarridoReal(_Base):
    def test_manda_al_cliente_y_al_tecnico_24_horas_antes_cada_uno_por_su_llave(self):
        w = self.mundo()
        r = w.barrer(LUN(10, 5))
        self.assertTrue(r["ok"], r)
        self.assertEqual(sorted(e["to"] for e in w.m.enviados_reales), ["admin@gimnasio.cl", "contacto@gimnasio.cl", "lenin@sphs.cl"])
        por = {f["destinatario"]: (f["correo_clave"], f["modulo"], f["estado"]) for f in w.filas_log()}
        self.assertEqual(por["contacto@gimnasio.cl"], ("ot_recordatorio_cliente", "mantenciones", "enviado"))
        self.assertEqual(por["admin@gimnasio.cl"], ("ot_recordatorio_cliente", "mantenciones", "enviado"))
        self.assertEqual(por["lenin@sphs.cl"], ("ot_tec_recordatorio", "ot_tecnicos", "enviado"))
        # el cliente ve «mañana», el horario y la dirección; el técnico el enlace a la ficha; nadie ve plata
        cli = w.para("contacto@gimnasio.cl")[0]
        self.assertIn("mañana", cli["asunto"])
        txt = re.sub(r"<[^>]+>", " ", cli["html"])
        for esperado in ("OT-2026-00077", "10/11/2026", "10:00 – 12:00", "Av. Demo 123, Las Condes", "Hola María Pérez,"):
            self.assertIn(esperado, txt)
        self.assertNotIn("/ot/77", cli["html"])
        tec = w.para("lenin@sphs.cl")[0]
        self.assertIn("https://ilus.test/ot/77", tec["html"])
        for e in w.m.enviados_reales:
            self.assertNotRegex(re.sub(r"<[^>]+>", " ", e["html"]), PLATA, e["to"])
            self.assertNotIn("1500000", e["html"])
        # queda constancia en la bitácora de la OT
        self.assertEqual(len(w.bitacora), 3)
        self.assertTrue(all(b[1] == 77 and b[2] == "recordatorio_24h" for b in w.bitacora))

    def test_una_sola_vez_por_fecha_de_visita(self):
        w = self.mundo()
        w.barrer(LUN(10, 5))
        n = len(w.m.enviados_reales)
        w.barrer(LUN(10, 15))
        w.barrer(LUN(11, 0))
        self.assertEqual(len(w.m.enviados_reales), n)
        # si se reagenda, la nueva fecha se vuelve a recordar
        w.visita(fecha_programada=dt.date(2026, 11, 11))
        w.barrer(dt.datetime(2026, 11, 10, 10, 5, tzinfo=TZ))
        self.assertEqual(len(w.m.enviados_reales), 2 * n)

    def test_fuera_del_horario_de_cobertura_no_hace_nada(self):
        w = self.mundo()
        for ahora in (LUN(18, 0), LUN(13, 30), LUN(7, 30), dt.datetime(2026, 11, 8, 11, 0, tzinfo=TZ)):          # tarde · colación · madrugada · domingo
            r = w.barrer(ahora)
            self.assertIn("cobertura", r.get("motivo", ""), ahora)
        self.assertEqual(w.m.enviados_reales, [])

    def test_el_interruptor_del_frente_frena_ese_correo_sin_reservarlo_y_al_encender_sale(self):
        w = self.mundo()
        w.apagar_correo("ot_recordatorio_cliente")
        w.barrer(LUN(10, 5))
        self.assertEqual(sorted(e["to"] for e in w.m.enviados_reales), ["lenin@sphs.cl"])      # el técnico sí; el cliente no
        self.assertFalse([k for k in w.reservas if k[1] == "cliente"])                          # nada reservado: no se pierde
        w.bd.ctrl["ot_recordatorio_cliente"]["activo"] = 1
        w.amb["_correo_ctrl_invalidar"]()
        w.barrer(LUN(10, 15))
        self.assertEqual(sorted(e["to"] for e in w.m.enviados_reales), ["admin@gimnasio.cl", "contacto@gimnasio.cl", "lenin@sphs.cl"])

    def test_la_llave_de_los_tecnicos_y_la_de_servicio_tecnico_se_respetan(self):
        w = self.mundo()
        w.cerrar_llave("ot_tecnicos")
        w.barrer(LUN(10, 5))
        self.assertEqual(sorted(e["to"] for e in w.m.enviados_reales), ["admin@gimnasio.cl", "contacto@gimnasio.cl"])
        w2 = self.mundo()
        w2.cerrar_llave("mantenciones")
        w2.barrer(LUN(10, 5))
        self.assertEqual([e["to"] for e in w2.m.enviados_reales], ["lenin@sphs.cl"])

    def test_los_dos_frenados_no_consultan_ni_reservan_nada(self):
        w = self.mundo()
        w.apagar_correo("ot_recordatorio_cliente")
        w.apagar_correo("ot_tec_recordatorio")
        r = w.barrer(LUN(10, 5))
        self.assertIn("frenados", r.get("motivo", ""))
        self.assertEqual((w.m.enviados_reales, w.reservas), ([], set()))

    def test_el_freno_de_emergencia_de_la_variable_apaga_solo_al_cliente(self):
        w = self.mundo(env_cliente="0")
        w.barrer(LUN(10, 5))
        self.assertEqual([e["to"] for e in w.m.enviados_reales], ["lenin@sphs.cl"])

    def test_una_ot_creada_dentro_de_las_24_horas_previas_no_recibe_recordatorio(self):
        w = self.mundo()
        w.visita(created_at=dt.datetime(2026, 11, 9, 13, 30))          # UTC → lunes 10:30 hora de Chile: DESPUÉS de las 10:00
        w.barrer(LUN(11, 0))
        self.assertEqual(w.m.enviados_reales, [])

    def test_un_trabajo_interno_solo_avisa_al_tecnico_y_la_desinstalacion_no_le_escribe_al_cliente(self):
        w = self.mundo()
        w.visita(cliente_id=None, cli_razon=None, contacto_email=None, contacto_nombre=None)
        w.barrer(LUN(10, 5))
        self.assertEqual([e["to"] for e in w.m.enviados_reales], ["lenin@sphs.cl"])
        w2 = self.mundo()
        w2.visita(ot_madre_id=70, ot_relacion="desinstalacion")
        w2.barrer(LUN(10, 5))
        self.assertEqual([e["to"] for e in w2.m.enviados_reales], ["lenin@sphs.cl"])

    def test_si_el_proveedor_de_correo_falla_se_libera_la_reserva_para_reintentar(self):
        w = self.mundo()
        w.m._real = lambda to, asunto, html, **kw: False
        w.amb["_send_ilus_email_real"] = w.m._real
        w.barrer(LUN(10, 5))
        self.assertEqual(w.reservas, set())
        self.assertEqual(w.bitacora, [])
        w.amb["_send_ilus_email_real"] = lambda to, asunto, html, **kw: w.m.enviados_reales.append({"to": to, "asunto": asunto, "html": html}) or True
        w.barrer(LUN(10, 15))
        self.assertEqual(len(w.m.enviados_reales), 3)

    def test_sin_la_tabla_de_constancia_no_se_manda_nada(self):
        """A diferencia de otros avisos, acá se prefiere no enviar: este barrido corre cada 10 minutos y repetiría el correo a un cliente real."""
        w = self.mundo()
        w.romper_tabla = True
        w.barrer(LUN(10, 5))
        self.assertEqual(w.m.enviados_reales, [])

    def test_dry_run_muestra_a_quien_le_mandaria_sin_enviar_ni_reservar(self):
        w = self.mundo()
        r = w.barrer(LUN(18, 0), dry=True)                         # el dry-run ignora el horario para poder revisarlo de noche
        self.assertEqual(sorted(d["correo"] for d in r["cliente"]), ["admin@gimnasio.cl", "contacto@gimnasio.cl"])
        self.assertEqual([d["correo"] for d in r["tecnico"]], ["lenin@sphs.cl"])
        self.assertEqual((w.m.enviados_reales, w.reservas), ([], set()))

    def test_una_plantilla_del_tecnico_apagada_en_el_editor_no_sale_y_el_cliente_si(self):
        w = self.mundo()
        w.plantilla_apagada[C.PLANTILLA_RECORDATORIO_TECNICO] = True
        w.barrer(LUN(10, 5))
        self.assertEqual(sorted(e["to"] for e in w.m.enviados_reales), ["admin@gimnasio.cl", "contacto@gimnasio.cl"])

    def test_el_nombre_de_una_empresa_externa_no_aparece_en_el_correo_de_otro(self):
        w = self.mundo()
        w.tecnicos = {"lenin@sphs.cl": "Lenin Urbina", "contacto@proveedor.cl": "Proveedor Secreto SpA"}
        w.fichas = {"contacto@proveedor.cl"}
        w.barrer(LUN(10, 5))
        self.assertIn("contacto@proveedor.cl", [e["to"] for e in w.m.enviados_reales])           # la empresa igual recibe su aviso
        for e in w.m.enviados_reales:
            if e["to"] != "contacto@proveedor.cl":
                self.assertNotIn("Proveedor Secreto", e["html"], e["to"])


# ═══════════════════════════════════════════════════════════════════════════════════════════════════
#  La invitación de calendario del cliente
# ═══════════════════════════════════════════════════════════════════════════════════════════════════
OT_ICS = {"id": 77, "numero_ot": "OT-2026-00077", "cliente": "Gimnasio Demo SpA", "tipo_label": "Instalación", "fecha": dt.date(2026, 11, 10),
          "hora_inicio": "10:00", "hora_fin": "12:00", "direccion": "Av. Demo 123, Las Condes", "equipos": ["Trotadora ILUS Pro 9000"],
          "descripcion": "Texto interno que el cliente no debe ver"}


class TestInvitacionDeCalendario(unittest.TestCase):
    def _ics(self, **kw):
        return OTC.ics_evento(OT_ICS, "asignada", correo="ana@cliente.cl", nombre="Ana", ts=1_800_000_000, base_url="https://ilus.test",
                              **kw).decode("utf-8").replace("\r\n ", "")

    def test_la_del_cliente_pide_aceptar_y_no_lleva_enlace_interno_ni_texto_interno(self):
        ics = self._ics(rsvp=True, sin_link=True)
        self.assertIn("METHOD:REQUEST", ics)
        self.assertIn("RSVP=TRUE", ics)
        self.assertIn("ATTENDEE;CN=\"Ana\";CUTYPE=INDIVIDUAL;ROLE=REQ-PARTICIPANT;PARTSTAT=NEEDS-ACTION;RSVP=TRUE:mailto:ana@cliente.cl", ics)
        self.assertNotIn("URL:", ics)
        self.assertNotIn("Ficha:", ics)
        self.assertNotIn("ilus.test/ot/", ics)

    def test_la_de_los_tecnicos_sigue_exactamente_igual(self):
        ics = self._ics()
        self.assertIn("RSVP=FALSE", ics)
        self.assertIn("URL:https://ilus.test/ot/77", ics)
        self.assertIn("Ficha: https://ilus.test/ot/77", ics.replace("\\n", "\n"))
        self.assertIn("Qué hay que hacer: Texto interno", ics.replace("\\n", "\n"))             # el técnico sí lo ve

    def test_el_correo_de_creacion_adjunta_a_cada_invitado_su_propia_invitacion_sin_el_texto_interno(self):
        w = MundoOT()
        self.addCleanup(w.cerrar)
        FC.extraer(w.amb, "app.py", ("_ot_cliente_ics_fn",), ())
        w.amb.update({"_get_brand_cfg": lambda: {"reply_to": "soportetec@sphs.cl"}, "time": __import__("time")})
        v = {"id": 77, "numero_ot": "OT-2026-00077", "razon_social": "Gimnasio Demo SpA", "fecha_programada": dt.date(2026, 11, 10),
             "fecha_fin": None, "hora_inicio": dt.timedelta(hours=10), "hora_fin": dt.timedelta(hours=12),
             "descripcion": "Texto interno que el cliente no debe ver"}
        variables = {"tipo_mantencion": "Instalación", "direccion": "Av. Demo 123, Las Condes", "maquina": "Trotadora ILUS Pro 9000"}
        fn = w.amb["_ot_cliente_ics_fn"](v, variables)
        a1, a2 = fn("ana@cliente.cl"), fn("luis@cliente.cl")
        self.assertEqual(a1[0][0], "OT_OT-2026-00077.ics")
        self.assertEqual(a1[0][2], "text/calendar")
        i1, i2 = (a1[0][1].decode("utf-8").replace("\r\n ", ""), a2[0][1].decode("utf-8").replace("\r\n ", ""))      # líneas plegadas (RFC 5545)
        self.assertIn("mailto:ana@cliente.cl", i1)
        self.assertNotIn("luis@cliente.cl", i1)
        self.assertIn("mailto:luis@cliente.cl", i2)
        for i in (i1, i2):
            self.assertIn("RSVP=TRUE", i)
            self.assertNotIn("Texto interno", i)
            self.assertNotIn("URL:", i)
        # una OT sin fecha no lleva invitación (el correo sale igual)
        self.assertIsNone(w.amb["_ot_cliente_ics_fn"](dict(v, fecha_programada=None), variables))


# ═══════════════════════════════════════════════════════════════════════════════════════════════════
#  Cableado
# ═══════════════════════════════════════════════════════════════════════════════════════════════════
APP = FC.texto("app.py")


def _bloque(inicio, fin):
    i = APP.index(inicio)
    return APP[i:APP.index(fin, i)]


class TestCableado(unittest.TestCase):
    def test_el_barrido_cuelga_de_simpliroute_poll_y_tiene_su_ruta_con_token(self):
        poll = _bloque('@app.route("/transporte/cron/simpliroute-poll"', '@app.route("/transporte/api/simpliroute/reparar-entregados"')
        self.assertIn("_ot_recordatorio_barrido(max_s=", poll)
        self.assertRegex(poll, r"if not dry and time\.time\(\) - _t_peticion < \d+:\s+try:\s+_rc = _ot_recordatorio_barrido")
        ruta = APP[APP.index('@app.route("/mantenciones/cron/recordatorio-ot-24h"'):][:1500]
        self.assertIn("_cron_extract_token()", ruta)
        self.assertIn("secrets.compare_digest", ruta)
        self.assertIn('"127.0.0.1"', ruta)

    def test_cada_envio_lleva_su_clave_registrada_y_reserva_antes_de_enviar(self):
        b = _bloque("def _ot_recordatorio_barrido(", '@app.route("/mantenciones/cron/recordatorio-ot-24h"')
        self.assertIn('correo="ot_recordatorio_cliente"', b)
        self.assertIn('correo="ot_tec_recordatorio"', b)
        self.assertLess(b.index('_ot_rec_reservar(vid, "cliente"'), b.index("_send_ilus_email("))
        self.assertLess(b.index('_ot_rec_reservar(vid, "tecnico"'), b.index("_ot_tec_enviar("))
        self.assertEqual(b.count("_ot_rec_liberar("), 2)
        self.assertEqual(b.count("_correo_bloqueo_previo("), 2)
        self.assertIn("_orec.en_cobertura(", b)
        self.assertNotIn("os.environ", b)                      # ninguna variable de entorno: todo por el front
        for clave in ("ot_recordatorio_cliente", "ot_tec_recordatorio"):
            self.assertIsNotNone(R.por_clave(clave))

    def test_la_tabla_es_un_solo_create_barato_y_se_crea_al_arrancar(self):
        src = FC.funcion_top("app.py", "_ensure_ot_recordatorios_tabla")
        self.assertEqual(src.count("CREATE TABLE IF NOT EXISTS mant_ot_recordatorios"), 1)
        self.assertNotIn("ALTER", src)
        self.assertIn("UNIQUE KEY uq_ot_rec (visita_id, tipo, destinatario, inicio_clave)", src)
        self.assertIn("        _ensure_ot_recordatorios_tabla()", APP)

    def test_la_creacion_adjunta_la_invitacion_y_el_envio_multiple_admite_una_funcion(self):
        b = _bloque("def _mant_ot_email_cliente_creacion(", "def _mant_ot_email_tecnico_creacion(")
        self.assertIn("_ics_fn = _ot_cliente_ics_fn(v, variables)", b)          # se arma UNA vez y viaja como adjunto de cada invitado
        self.assertIn("attachments=_ics_fn", b)
        m = FC.funcion_top("app.py", "_mant_send_email_multi")
        self.assertIn("callable(attachments)", m)
        f = FC.funcion_top("app.py", "_ot_cliente_ics_fn")
        self.assertIn("rsvp=True", f)
        self.assertIn("sin_link=True", f)
        self.assertIn('"descripcion": ""', f)

    def test_el_aviso_de_ot_terminada_al_cliente_ya_no_se_enciende_con_una_variable(self):
        self.assertEqual(R.por_clave(C.CORREO_TERMINADA_CLIENTE)["condiciones"], ())
        self.assertTrue(C.cliente_activo({}))
        deploy = FC.texto(".github/workflows/deploy.yml")
        self.assertNotIn("OT_TERMINADA_CLIENTE_ACTIVA", deploy)          # y tampoco hay que encenderla ahí: el frente manda


# ═══════════════════════════════════════════════════════════════════════════════════════════════════
#  Correos que APORTAN VALOR (Daniel, 5-nov-2026: «que el mensaje provoque verse inteligente y aporte valor»)
# ═══════════════════════════════════════════════════════════════════════════════════════════════════
def _t(html):
    import html as _h
    return re.sub(r"\s+", " ", _h.unescape(re.sub(r"<[^>]+>", " ", html or ""))).strip()


class TestBloquesDeValor(unittest.TestCase):
    def test_los_consejos_al_cliente_cambian_segun_el_tipo_de_trabajo(self):
        inst, prev, corr, gar = (O.consejos_cliente(t) for t in ("instalacion", "preventiva", "correctiva", "garantia"))
        self.assertTrue(any("camino de acceso" in c for c in inst))
        self.assertTrue(any("ruidos" in c for c in prev))
        self.assertTrue(any("desde cuándo falla" in c for c in corr))
        self.assertTrue(any("factura o boleta" in c for c in gar))
        self.assertEqual(O.consejos_cliente("mantención preventiva"), prev)
        self.assertEqual(O.consejos_cliente("algo_raro"), O._CONSEJOS_GENERALES)
        self.assertEqual(O.consejos_cliente(None), O._CONSEJOS_GENERALES)
        for k, c in O.CONSEJOS_CLIENTE.items():
            self.assertTrue(len(c) >= 2, k)
            # «factura o boleta» (garantía) es el DOCUMENTO de la compra que el técnico necesita, no plata: se permite; montos y costos, no
            self.assertNotRegex(" ".join(c), re.compile(PLATA.pattern.replace(r"|\bfactur", ""), re.I), k)

    def test_que_va_a_pasar_dice_quien_llega_a_cuantos_equipos_y_como_cierra(self):
        t = _t(O.que_esperar_html(["Trotadora ILUS Pro 9000", "Bicicleta ILUS"], ["Lenin Urbina"]))
        self.assertIn("Llegará Lenin Urbina en el horario indicado.", t)
        self.assertIn("Trabajará en 2 equipos: Trotadora ILUS Pro 9000, Bicicleta ILUS.", t)
        self.assertIn("confirmar la recepción del trabajo con tu firma", t)
        self.assertIn("1 equipo:", _t(O.que_esperar_html(["Trotadora"], [])))                  # singular
        self.assertIn("Nuestro equipo técnico llegará", _t(O.que_esperar_html([], [])))        # sin nombres: no se inventa
        self.assertNotIn("Trabajará en", _t(O.que_esperar_html([], [])))
        self.assertIn("y 4 más", _t(O.que_esperar_html([f"Equipo {i}" for i in range(10)], [])))

    def test_cómo_llegar_es_un_enlace_de_mapa_y_solo_con_una_direccion_real(self):
        u = O.url_mapa("Av. Demo 123, Las Condes")
        self.assertTrue(u.startswith("https://www.google.com/maps/search/?api=1&query="))
        self.assertIn("Av.%20Demo%20123%2C%20Las%20Condes", u)
        for sin in ("", "—", "-", None, OTC.OCULTO_ANEXO):
            self.assertEqual(O.url_mapa(sin), "", repr(sin))
            self.assertEqual(O.como_llegar_html(sin), "", repr(sin))
        self.assertIn("Cómo llegar", O.como_llegar_html("Av. Demo 123"))

    def test_el_contacto_en_terreno_solo_muestra_un_telefono_legible_y_escapa_el_nombre(self):
        f = O.contacto_fila("María <b>Pérez</b>", "+56 9 8765 4321")
        self.assertIn("Contacto en terreno", f)
        self.assertIn("María &lt;b&gt;Pérez&lt;/b&gt;", f)
        self.assertIn("+56 9 8765 4321", f)
        self.assertNotIn("<b>", f)
        self.assertNotIn("987", O.contacto_fila("María", "llamar a la casa 12"))             # no es un teléfono: no se muestra
        self.assertIn("María", O.contacto_fila("María", "123"))
        self.assertEqual(O.contacto_fila("", ""), "")
        self.assertEqual(O.contacto_fila(None, None), "")

    def test_que_hay_que_hacer_no_deja_pasar_plata(self):
        t = _t(O.notas_html("Revisar la banda. Cobrar $50.000 de extra al cliente. Lubricar el eje."))
        self.assertIn("Revisar la banda", t)
        self.assertIn("Lubricar el eje", t)
        self.assertNotIn("50.000", t)
        self.assertEqual(O.notas_html(""), "")
        self.assertEqual(O.notas_html("Cobrar $50.000 al cliente"), "")                      # si todo era plata, no se dibuja el bloque

    def test_la_lista_de_equipos_cuenta_y_acota(self):
        self.assertIn("EQUIPOS DEL TRABAJO (2)", _t(O.equipos_lista_html(["A", "B"])))
        self.assertEqual(O.equipos_lista_html([]), "")
        self.assertIn("… y 3 más", _t(O.equipos_lista_html([f"E{i}" for i in range(15)])))

    def test_las_variables_del_cliente_y_del_tecnico_llenan_solo_lo_suyo(self):
        ot = dict(TestTextosYRegistro.OT, tipo="instalacion", descripcion="Revisar la banda.", contacto_nombre="María Pérez",
                  contacto_tel="+56 9 8765 4321")
        cli = O.variables_recordatorio(ot, F(10, 10, 0), dt.date(2026, 11, 9), para="cliente")
        tec = O.variables_recordatorio(ot, F(10, 10, 0), dt.date(2026, 11, 9), para="tecnico", link_ot="https://ilus.test/ot/77")
        for k in ("que_esperar_html", "preparacion_html"):
            self.assertTrue(cli[k], k)
            self.assertEqual(tec[k], "", k)
        for k in ("como_llegar_html", "contacto_fila", "notas_html", "equipos_lista_html", "antes_de_salir_html"):
            self.assertEqual(cli[k], "", "el cliente no debe recibir " + k)
            self.assertTrue(tec[k], "el técnico debe recibir " + k)

    def test_con_el_anexo_sin_firmar_el_tecnico_no_recibe_direccion_contacto_ni_que_hacer(self):
        ot = dict(TestTextosYRegistro.OT, tipo="instalacion", descripcion="Revisar la banda.", contacto_nombre="María Pérez",
                  contacto_tel="+56 9 8765 4321", direccion=OTC.OCULTO_ANEXO)
        v = O.variables_recordatorio(ot, F(10, 10, 0), dt.date(2026, 11, 9), para="tecnico", oculto=True)
        for k in ("como_llegar_html", "contacto_fila", "notas_html"):
            self.assertEqual(v[k], "", k)
        self.assertTrue(v["equipos_lista_html"] and v["antes_de_salir_html"])                # lo demás sí sirve

    def test_el_resumen_del_trabajo_de_la_ot_terminada(self):
        t = _t(C.resumen_trabajo_html({"equipos": ["Trotadora ILUS Pro 9000"], "tecnicos": ["Lenin Urbina", "Pedro Pérez"]}))
        self.assertIn("Se atendió 1 equipo: Trotadora ILUS Pro 9000.", t)
        self.assertIn("Lo realizaron: Lenin Urbina, Pedro Pérez.", t)
        self.assertIn("firmado por el técnico, por el cliente y por el responsable de Servicio Técnico", t)
        t2 = _t(C.resumen_trabajo_html({"equipos": ["A", "B", "C"], "tecnicos": []}))
        self.assertIn("Se atendieron 3 equipos", t2)
        self.assertNotIn("Lo realiz", t2)                                                     # sin nombres, no se inventa
        self.assertNotRegex(t + t2, PLATA)

    def test_todo_va_escapado_y_sin_plata(self):
        ot = dict(TestTextosYRegistro.OT, equipos=["<script>alert(1)</script>"], tecnicos=["Ana <i>"], cliente="A & B")
        for blob in (O.que_esperar_html(ot["equipos"], ot["tecnicos"]), O.equipos_lista_html(ot["equipos"]),
                     C.resumen_trabajo_html(ot), O.notas_html("<img src=x onerror=1> instrucción")):
            self.assertNotIn("<script>", blob)
            self.assertNotIn("<img", blob)
            self.assertNotIn("<i>", blob)


class TestCorreosConValorDeExtremoAExtremo(_Base):
    def test_el_correo_al_cliente_trae_que_va_a_pasar_y_para_dejar_todo_listo_pero_nada_interno(self):
        w = self.mundo()
        w.barrer(LUN(10, 5))
        cli = w.para("contacto@gimnasio.cl")[0]
        t = _t(cli["html"])
        for esperado in ("QUÉ VA A PASAR", "Llegará Lenin Urbina", "Trabajará en 1 equipo: Trotadora ILUS Pro 9000",
                         "PARA DEJAR TODO LISTO", "camino de acceso", "firma"):
            self.assertIn(esperado, t, esperado)
        for prohibido in ("Cómo llegar", "QUÉ HAY QUE HACER", "Revisar la banda", "50.000", "8765", "ANTES DE SALIR"):
            self.assertNotIn(prohibido, t, prohibido)
        self.assertNotIn("google.com/maps", cli["html"])

    def test_el_correo_al_tecnico_trae_como_llegar_contacto_que_hacer_y_antes_de_salir_sin_plata(self):
        w = self.mundo()
        w.barrer(LUN(10, 5))
        tec = w.para("lenin@sphs.cl")[0]
        t = _t(tec["html"])
        for esperado in ("Cómo llegar", "Contacto en terreno: María Pérez · +56 9 8765 4321", "QUÉ HAY QUE HACER", "Revisar la banda de la trotadora",
                         "EQUIPOS DEL TRABAJO (1)", "ANTES DE SALIR", "foto del antes y del después"):
            self.assertIn(esperado, t, esperado)
        self.assertIn("https://www.google.com/maps/search/?api=1&amp;query=", tec["html"])     # el & del enlace va escapado en el HTML
        self.assertNotIn("50.000", t)                                                          # la plata de la descripción no llega al técnico
        self.assertNotRegex(t, PLATA)

    def test_con_el_anexo_sin_firmar_el_tecnico_no_ve_direccion_contacto_ni_que_hacer(self):
        w = self.mundo()
        w.amb["_ot_tec_oculta_detalle"] = lambda vid: True
        w.barrer(LUN(10, 5))
        tec = w.para("lenin@sphs.cl")[0]
        t = _t(tec["html"])
        for prohibido in ("Cómo llegar", "Contacto en terreno", "8765", "QUÉ HAY QUE HACER", "Revisar la banda", "Av. Demo 123"):
            self.assertNotIn(prohibido, t, prohibido)
        self.assertNotIn("google.com/maps", tec["html"])
        self.assertIn("ANTES DE SALIR", t)

    def test_el_consejo_cambia_con_el_tipo_de_trabajo(self):
        w = self.mundo()
        w.visita(tipo="correctiva")
        w.barrer(LUN(10, 5))
        t = _t(w.para("contacto@gimnasio.cl")[0]["html"])
        self.assertIn("desde cuándo falla el equipo", t)
        self.assertNotIn("camino de acceso", t)


class TestPlantillasSeMejoranSinPisarLoEditado(unittest.TestCase):
    def _mundo(self):
        w = MundoOT()
        self.addCleanup(w.cerrar)
        orig = w.amb["mysql_fetchone"]

        def con_id(sql, params=()):                                 # la plantilla de mentira no trae `id`: el UPDATE real lo usa
            r = orig(sql, params)
            if r is not None and "FROM comm_templates" in sql and "id" not in r:
                r = dict(r, id=abs(hash(tuple(params))) % 10_000)
            return r
        w.amb["mysql_fetchone"] = con_id
        return w

    def _poner(self, w, slug, par):
        w.bd.plantillas[(C.MODULO_PLANTILLAS, slug)] = {"asunto": par[0], "cuerpo": par[1], "activo": 1}

    def test_la_version_nueva_es_distinta_de_la_anterior_y_trae_los_bloques_nuevos(self):
        for slug, anteriores in C.SEMILLAS_ANTERIORES.items():
            self.assertNotEqual(C.SEMILLAS[slug][1], anteriores[0][1], slug)
        self.assertIn("{{resumen_trabajo_html}}", C.SEMILLAS[C.PLANTILLA_TERMINADA][1])
        self.assertIn("{{que_esperar_html}}", C.SEMILLAS[C.PLANTILLA_RECORDATORIO][1])
        self.assertIn("{{como_llegar_html}}", C.SEMILLAS[C.PLANTILLA_RECORDATORIO_TECNICO][1])
        self.assertNotIn("{{que_esperar_html}}", C._RECORDATORIO_V1[1])

    def test_una_plantilla_sin_editar_pasa_a_la_version_nueva_y_una_editada_queda_como_esta(self):
        w = self._mundo()
        self._poner(w, C.PLANTILLA_RECORDATORIO, C._RECORDATORIO_V1)                                   # sin editar → se actualiza
        self._poner(w, C.PLANTILLA_RECORDATORIO_TECNICO, C._RECORDATORIO_TECNICO_V1)                   # sin editar → se actualiza
        self._poner(w, C.PLANTILLA_TERMINADA, (C._TERMINADA_V1[0], C._TERMINADA_V1[1] + "<p>Texto de Daniel</p>"))   # editada → NO se toca
        w.f("_ensure_comm_template_ot_comunicacion")()
        ups = [(s, p) for s, p in w.sql if s.startswith("UPDATE comm_templates SET asunto")]
        estados_actualizados = {p[1] for s, p in ups}                    # los cuerpos que quedaron
        self.assertEqual(len(ups), 2, ups)
        self.assertIn(C.SEMILLAS[C.PLANTILLA_RECORDATORIO][1], estados_actualizados)
        self.assertIn(C.SEMILLAS[C.PLANTILLA_RECORDATORIO_TECNICO][1], estados_actualizados)
        self.assertNotIn(C.SEMILLAS[C.PLANTILLA_TERMINADA][1], estados_actualizados)
        for s, p in ups:
            self.assertIn("AND cuerpo=%s", s)                      # si alguien la edita justo ahora, no se pisa
            self.assertEqual(p[2], "sistema")

    def test_sin_plantillas_previas_solo_se_siembran_las_nuevas(self):
        w = self._mundo()
        w.f("_ensure_comm_template_ot_comunicacion")()
        self.assertEqual([s for s, p in w.sql if s.startswith("UPDATE comm_templates")], [])


if __name__ == "__main__":
    unittest.main()

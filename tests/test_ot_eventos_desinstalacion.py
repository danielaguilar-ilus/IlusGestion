"""EVENTOS y OT de DESINSTALACIÓN (madre e hija) — Daniel, 19-oct-2026.

Cubre (sin base de datos ni Flask): la lógica pura (ot_eventos.py, ot_saldo_servicio.py) y las funciones de app.py que se
EJECUTAN con dobles de la base (tests/_fuente_app.py recorta cada función del texto: no se parsea el archivo de 7 MB).
Además vigila por texto los candados que no se pueden ejecutar aquí: nada de lo nuevo entra al `allowed` del PUT, el
`ot_madre_id` nunca se lee del body, los documentos espejados quedan fuera del saldo, etc.

Correr (por archivo, la suite completa no sirve en esta máquina):  py -m pytest tests/test_ot_eventos_desinstalacion.py -q -p no:warnings
"""
import datetime as dt
import re
import unittest

import ot_eventos as E
import ot_saldo_servicio as S
from tests import _fuente_app as F

D = dt.date


def madre(**kw):
    base = {"id": 10, "numero_ot": "OT-2026-00350", "tipo": "instalacion", "estado": "programada", "cliente_id": 5,
            "ot_madre_id": None, "desinstalacion_prevista": D(2026, 11, 26), "fecha_programada": D(2026, 10, 26),
            "fecha_fin": D(2026, 10, 27), "hora_fin": "18:00", "fecha_realizada": None, "evento_id": None}
    base.update(kw)
    return base


def hija(**kw):
    base = {"id": 11, "numero_ot": "OT-2026-00351", "tipo": "desinstalacion", "estado": "programada", "cliente_id": 5,
            "ot_madre_id": 10, "ot_relacion": "desinstalacion", "fecha_programada": D(2026, 11, 26)}
    base.update(kw)
    return base


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
class TestFechas(unittest.TestCase):
    FER = {"2026-11-01": "Día de Todos los Santos"}

    def test_domingo_bloquea_con_el_mismo_mensaje_de_hoy(self):
        r = E.evaluar_fecha_desinstalacion("2026-11-29", None, None, madre(), self.FER)
        self.assertFalse(r["ok"])
        self.assertEqual(r["bloqueo"]["codigo"], "DOMINGO_BLOQUEADO")
        self.assertEqual(r["bloqueo"]["mensaje"], "Los domingos no se agendan órdenes de trabajo. Elige otro día.")

    def test_sabado_y_feriado_solo_avisan(self):
        sab = E.evaluar_fecha_desinstalacion("2026-11-28", None, None, madre(), self.FER)
        self.assertTrue(sab["ok"])
        self.assertTrue(any("sábado" in a.lower() for a in sab["avisos"]))
        # 1 nov 2026 es domingo: se prueba el feriado en un día de semana
        fer = {"2026-11-27": "Feriado de prueba"}
        r = E.evaluar_fecha_desinstalacion("2026-11-27", None, None, madre(), fer)
        self.assertTrue(r["ok"])
        self.assertTrue(any("feriado" in a.lower() for a in r["avisos"]))

    def test_la_hija_no_puede_ir_antes_que_la_madre(self):
        r = E.evaluar_fecha_desinstalacion("2026-10-26", None, None, madre(), {})
        self.assertFalse(r["ok"])
        self.assertEqual(r["bloqueo"]["codigo"], "DESINST_ANTES_DE_INSTALACION")

    def test_misma_fecha_exige_inicio_despues_del_termino(self):
        m = madre(fecha_fin=None, fecha_programada=D(2026, 10, 27), hora_fin="18:00")
        antes = E.evaluar_fecha_desinstalacion("2026-10-27", "17:00", "19:00", m, {})
        despues = E.evaluar_fecha_desinstalacion("2026-10-27", "18:00", "19:30", m, {})
        self.assertEqual(antes["bloqueo"]["codigo"], "DESINST_ANTES_DE_INSTALACION")
        self.assertTrue(despues["ok"])
        sin_hora = E.evaluar_fecha_desinstalacion("2026-10-27", None, None, m, {})
        self.assertTrue(sin_hora["ok"])
        self.assertTrue(sin_hora["avisos"], "sin hora el mismo día avisa en ámbar")

    def test_madre_de_varios_dias_se_compara_con_su_fecha_fin(self):
        # la instalación dura del 26 al 28: el 27 sigue siendo antes de que termine
        m = madre(fecha_programada=D(2026, 10, 26), fecha_fin=D(2026, 10, 28))
        self.assertEqual(E.evaluar_fecha_desinstalacion("2026-10-27", None, None, m, {})["bloqueo"]["codigo"],
                         "DESINST_ANTES_DE_INSTALACION")
        self.assertTrue(E.evaluar_fecha_desinstalacion("2026-10-28", "19:00", "20:00", m, {})["ok"])

    def test_si_la_madre_ya_se_ejecuto_manda_su_fecha_real(self):
        m = madre(fecha_programada=D(2026, 10, 20), fecha_fin=None, fecha_realizada=D(2026, 10, 24), hora_fin="10:00")
        self.assertEqual(E.evaluar_fecha_desinstalacion("2026-10-22", None, None, m, {})["bloqueo"]["codigo"],
                         "DESINST_ANTES_DE_INSTALACION")
        self.assertTrue(E.evaluar_fecha_desinstalacion("2026-10-24", None, None, m, {})["ok"])

    def test_si_la_madre_se_ejecuto_antes_de_lo_programado_manda_la_fecha_real(self):
        # programada el 26 pero se hizo el 24: la desinstalación puede ir desde el 24 (antes mandaba el máximo de las dos)
        m = madre(fecha_programada=D(2026, 10, 26), fecha_fin=D(2026, 10, 27), fecha_realizada=D(2026, 10, 24))
        self.assertEqual(E.fecha_referencia_madre(m), (D(2026, 10, 24), None))
        r = E.evaluar_fecha_desinstalacion("2026-10-24", None, None, m, {})      # sábado: avisa, no bloquea
        self.assertTrue(r["ok"])
        self.assertEqual(E.evaluar_fecha_desinstalacion("2026-10-23", None, None, m, {})["bloqueo"]["codigo"],
                         "DESINST_ANTES_DE_INSTALACION")

    def test_formatos_invalidos(self):
        self.assertEqual(E.evaluar_fecha_desinstalacion(None, None, None, madre(), {})["bloqueo"]["codigo"], "FECHA_REQUERIDA")
        self.assertEqual(E.evaluar_fecha_desinstalacion("26/13/2026", None, None, madre(), {})["bloqueo"]["codigo"], "FECHA_INVALIDA")
        self.assertEqual(E.evaluar_fecha_desinstalacion("2026-11-26", "12:00", "11:00", madre(), {})["bloqueo"]["codigo"], "HORA_INVALIDA")
        self.assertEqual(E.evaluar_fecha_desinstalacion("2026-11-26", "9x", None, madre(), {})["bloqueo"]["codigo"], "HORA_INVALIDA")

    def test_si_el_evento_termina_en_domingo_se_propone_el_siguiente_habil(self):
        p = E.proponer_fecha_desinstalacion({"fecha_fin": "2026-11-29"}, madre(), {"2026-11-30": "feriado"})
        self.assertEqual(p["fecha"], D(2026, 12, 1))
        self.assertIn("domingo", p["motivo"])
        # un día hábil se propone tal cual
        self.assertEqual(E.proponer_fecha_desinstalacion({"fecha_fin": "2026-11-26"}, madre(), {})["fecha"], D(2026, 11, 26))
        # sin evento: el día de la instalación
        self.assertEqual(E.proponer_fecha_desinstalacion(None, madre(fecha_fin=None), {})["fecha"], D(2026, 10, 26))

    def test_dias_habiles(self):
        self.assertEqual(E.restar_habiles(D(2026, 11, 2), 2, {}), D(2026, 10, 29))   # lunes - 2 hábiles = jueves
        self.assertTrue(E.es_habil(D(2026, 10, 26)))
        self.assertFalse(E.es_habil(D(2026, 10, 25)))
        self.assertEqual(E.siguiente_habil(D(2026, 10, 24), {}), D(2026, 10, 26))   # sábado -> lunes


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
class TestCubierta(unittest.TestCase):
    def test_madre_valida_pide_documento_real_o_la_autorizacion_que_la_incluia(self):
        ok, _ = E.madre_valida_para_cubrir(madre(), 5, tiene_doc_real=True)
        self.assertTrue(ok)
        ok, m = E.madre_valida_para_cubrir(madre(), 5, tiene_doc_real=False)
        self.assertFalse(ok)
        self.assertIn("documento", m)
        ok, _ = E.madre_valida_para_cubrir(madre(), 5, tiene_doc_real=False, aut_aprobada=True)
        self.assertTrue(ok)

    def test_madre_invalida(self):
        for kw, texto in ((dict(tipo="preventiva"), "instalación"), (dict(ot_madre_id=3), "cadenas"),
                          (dict(estado="cancelada"), "cancelada"), (dict(estado="anulada"), "anulada"),
                          (dict(desinstalacion_prevista=None), "prevista")):
            ok, motivo = E.madre_valida_para_cubrir(madre(**kw), 5, tiene_doc_real=True)
            self.assertFalse(ok, kw)
            self.assertIn(texto, motivo)
        ok, motivo = E.madre_valida_para_cubrir(madre(), 99, tiene_doc_real=True)
        self.assertFalse(ok)
        self.assertIn("otro cliente", motivo)

    def test_hija_cubierta_se_deduce_de_la_relacion_no_de_valor_origen(self):
        self.assertTrue(E.es_hija_cubierta(hija(), True))
        # valor_origen es input del navegador: no cambia nada
        self.assertTrue(E.es_hija_cubierta(dict(hija(), valor_origen="manual"), True))
        self.assertFalse(E.es_hija_cubierta(dict(hija(), valor_origen="cubierta", ot_relacion=None), True))
        self.assertFalse(E.es_hija_cubierta(dict(hija(), ot_madre_id=None), True))
        self.assertFalse(E.es_hija_cubierta(hija(), False))
        self.assertFalse(E.es_hija_cubierta(dict(hija(), tipo="instalacion"), True))

    def test_si_cobra_su_propia_linea_ya_no_es_cubierta(self):
        self.assertFalse(E.es_hija_cubierta(hija(), True, cobro_hija=80000))
        self.assertFalse(E.es_hija_cubierta(hija(), True, cobro_envio_hija=1))
        self.assertTrue(E.es_hija_cubierta(hija(), True, cobro_hija=0, cobro_envio_hija=None))

    def test_hija_vigente_cuenta_las_cerradas_y_no_las_canceladas(self):
        hs = [hija(id=1, estado="cancelada"), hija(id=2, estado="anulada"), hija(id=3, estado="cerrada")]
        self.assertEqual([h["id"] for h in E.hijas_vigentes(hs)], [3])
        self.assertEqual([h["id"] for h in E.hijas_vigentes(hs, excluir_id=3)], [])

    def test_una_hija_cerrada_no_libera_el_boton_de_programar(self):
        ok, motivo = E.puede_programar_desinstalacion(madre(), [hija(estado="cerrada")])
        self.assertFalse(ok)
        self.assertIn("OT-2026-00351", motivo)
        ok, _ = E.puede_programar_desinstalacion(madre(), [hija(estado="cancelada")])
        self.assertTrue(ok)
        self.assertFalse(E.puede_programar_desinstalacion(madre(tipo="preventiva"), [])[0])
        self.assertFalse(E.puede_programar_desinstalacion(madre(ot_madre_id=4), [])[0])
        self.assertFalse(E.puede_programar_desinstalacion(madre(estado="cancelada"), [])[0])

    def test_declarar_despues_pide_autorizacion_salvo_asistente_o_evento_con_fin(self):
        self.assertTrue(E.necesita_autorizacion_prevista(False, False))
        self.assertFalse(E.necesita_autorizacion_prevista(True, False))
        self.assertFalse(E.necesita_autorizacion_prevista(False, True))

    def test_eleccion_cubierta_o_cobra_la_decide_el_saldo_que_calcula_el_servidor(self):
        # sin saldo: es cubierta sin preguntar
        self.assertEqual(E.validar_cobro_desinstalacion(None, 0, 0), (None, "cubierta"))
        # con saldo y sin elección: hay que elegir
        err, _ = E.validar_cobro_desinstalacion(None, 200000, 0)
        self.assertEqual(err["codigo"], "COBRO_REQUERIDO")
        # cubierta dejando saldo libre: pide argumento
        err, _ = E.validar_cobro_desinstalacion("cubierta", 200000, 0, "corto")
        self.assertEqual(err["codigo"], "ARGUMENTO_CUBIERTA")
        self.assertEqual(E.validar_cobro_desinstalacion("cubierta", 200000, 0, "x" * 30), (None, "cubierta"))
        # cobra: necesita saldo
        self.assertEqual(E.validar_cobro_desinstalacion("cobra", 200000, 0), (None, "cobra"))
        self.assertEqual(E.validar_cobro_desinstalacion("cobra", 0, 0)[0]["codigo"], "SIN_SALDO_PARA_COBRAR")
        self.assertEqual(E.validar_cobro_desinstalacion("otra", 0, 0)[0]["codigo"], "COBRO_INVALIDO")


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
class TestGuardias(unittest.TestCase):
    def test_cancelar_la_madre_sin_hija(self):
        self.assertEqual(E.guardia_cancelar_madre([], False), ("sola", None))
        self.assertEqual(E.guardia_cancelar_madre([hija(estado="cancelada")], False), ("sola", None))

    def test_cancelar_la_madre_con_la_hija_sin_empezar_pide_confirmar_las_dos(self):
        acc, err = E.guardia_cancelar_madre([hija()], False)
        self.assertIsNone(acc)
        self.assertEqual(err["codigo"], "OT_CON_DESINSTALACION")
        self.assertEqual(E.guardia_cancelar_madre([hija()], True), ("con_hija", None))

    def test_cancelar_la_madre_con_la_hija_empezada_o_cerrada_se_rechaza_con_su_numero(self):
        for est in ("en_curso", "completada", "cerrada", "firmada_tecnico"):
            acc, err = E.guardia_cancelar_madre([hija(estado=est)], True)
            self.assertIsNone(acc, est)
            self.assertEqual(err["codigo"], "DESINST_EN_CURSO")
            self.assertIn("OT-2026-00351", err["mensaje"])

    def test_reagendar_la_madre_despues_de_la_hija(self):
        g = E.guardia_reagendar_madre("2026-12-10", None, [hija(fecha_programada=D(2026, 11, 26))])
        self.assertEqual(g["codigo"], "REAGENDAR_DESINSTALACION_PRIMERO")
        self.assertIn("OT-2026-00351", g["mensaje"])
        self.assertIsNone(E.guardia_reagendar_madre("2026-11-20", None, [hija(fecha_programada=D(2026, 11, 26))]))
        # varias fechas: manda el término de la instalación
        self.assertEqual(E.guardia_reagendar_madre("2026-11-20", "2026-12-01", [hija()])["codigo"], "REAGENDAR_DESINSTALACION_PRIMERO")
        self.assertIsNone(E.guardia_reagendar_madre("2026-12-10", None, []))
        self.assertIsNone(E.guardia_reagendar_madre("2026-12-10", None, [hija(estado="cancelada")]))

    def test_no_se_borra_una_madre_con_hija_vigente(self):
        self.assertEqual(E.guardia_borrar_madre([hija()])["codigo"], "MADRE_CON_DESINSTALACION")
        self.assertEqual(E.guardia_borrar_madre([hija(estado="cerrada")])["codigo"], "MADRE_CON_DESINSTALACION")
        self.assertIsNone(E.guardia_borrar_madre([hija(estado="cancelada")]))
        self.assertIsNone(E.guardia_borrar_madre([]))

    def test_el_tipo_queda_protegido(self):
        self.assertEqual(E.guardia_cambio_tipo(hija(), [])["codigo"], "TIPO_PROTEGIDO")
        self.assertEqual(E.guardia_cambio_tipo(madre(), [hija()])["codigo"], "TIPO_PROTEGIDO")
        self.assertEqual(E.guardia_cambio_tipo(madre(), [])["codigo"], "TIPO_PROTEGIDO")   # tiene prevista
        self.assertIsNone(E.guardia_cambio_tipo(madre(desinstalacion_prevista=None), []))

    def test_el_evento_de_la_hija_es_el_de_la_madre(self):
        self.assertEqual(E.guardia_evento_de_hija(hija())["codigo"], "EVENTO_DE_LA_MADRE")
        self.assertIsNone(E.guardia_evento_de_hija(madre()))

    def test_no_se_quita_la_prevista_con_hija_vigente(self):
        self.assertEqual(E.guardia_quitar_prevista(madre(), [hija()])["codigo"], "DESINSTALACION_AGENDADA")
        self.assertIsNone(E.guardia_quitar_prevista(madre(), [hija(estado="cancelada")]))


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
class TestEstadoYAvisos(unittest.TestCase):
    HOY = D(2026, 11, 20)

    def test_una_sola_fecha_manda(self):
        m = madre(desinstalacion_prevista=D(2026, 11, 30))
        # con hija vigente: manda la fecha de la hija (no la prevista)
        e = E.estado_desinstalacion(m, [hija(fecha_programada=D(2026, 11, 26))], self.HOY)
        self.assertEqual((e["codigo"], e["fecha"]), ("agendada", "2026-11-26"))
        self.assertIn("OT-2026-00351", e["texto"])
        self.assertIn("26/11/2026", e["texto"])
        e = E.estado_desinstalacion(m, [], self.HOY)
        self.assertEqual((e["codigo"], e["fecha"], e["nivel"]), ("sin_agendar", "2026-11-30", "ambar"))

    def test_estados_de_la_desinstalacion(self):
        m = madre(desinstalacion_prevista=D(2026, 11, 10))
        self.assertEqual(E.estado_desinstalacion(m, [], self.HOY)["codigo"], "vencida")
        self.assertEqual(E.estado_desinstalacion(madre(desinstalacion_prevista=None), [], self.HOY)["codigo"], "ninguna")
        self.assertEqual(E.estado_desinstalacion(m, [hija(estado="cerrada")], self.HOY)["codigo"], "hecha")
        self.assertEqual(E.estado_desinstalacion(m, [hija(fecha_programada=D(2026, 11, 10))], self.HOY)["codigo"], "vencida")
        self.assertEqual(E.estado_desinstalacion(m, [hija(fecha_programada=D(2026, 11, 10), estado="en_curso")], self.HOY)["codigo"], "agendada")

    def test_barrido_avisa_dos_dias_habiles_antes_si_no_hay_hija(self):
        m = madre(desinstalacion_prevista=D(2026, 11, 26))   # jueves
        self.assertIsNone(E.aviso_barrido(m, [], None, D(2026, 11, 20), {}))        # viernes: faltan 4 hábiles
        a = E.aviso_barrido(m, [], None, D(2026, 11, 24), {})                       # martes: 2 hábiles
        self.assertEqual(a["codigo"], "por_agendar")
        self.assertEqual(E.aviso_barrido(m, [], None, D(2026, 11, 27), {})["codigo"], "vencida")
        self.assertEqual(E.aviso_barrido(m, [], None, D(2026, 11, 27), {})["nivel"], "rojo")

    def test_barrido_no_avisa_si_la_desinstalacion_esta_agendada_a_tiempo_o_la_madre_esta_cancelada(self):
        m = madre(desinstalacion_prevista=D(2026, 11, 26))
        self.assertIsNone(E.aviso_barrido(m, [hija(fecha_programada=D(2026, 11, 26))], None, D(2026, 11, 24), {}))
        self.assertIsNone(E.aviso_barrido(madre(estado="cancelada"), [], None, D(2026, 11, 27), {}))
        self.assertIsNone(E.aviso_barrido(madre(desinstalacion_prevista=None), [], None, D(2026, 11, 27), {}))

    def test_barrido_avisa_cuando_el_evento_termino_con_la_instalacion_sin_desinstalar(self):
        m = madre(desinstalacion_prevista=D(2026, 12, 20))
        ev = {"fecha_fin": "2026-11-27", "estado": "activo"}
        a = E.aviso_barrido(m, [], ev, D(2026, 11, 28), {})
        self.assertEqual(a["codigo"], "evento_terminado")
        self.assertIsNone(E.aviso_barrido(m, [], dict(ev, estado="cancelado"), D(2026, 11, 28), {}))

    def test_barrido_avisa_si_llega_el_dia_de_la_hija_y_la_madre_no_se_ejecuto(self):
        m = madre(estado="programada")
        a = E.aviso_barrido(m, [hija(fecha_programada=D(2026, 11, 26))], None, D(2026, 11, 26), {})
        self.assertEqual(a["codigo"], "hija_sin_madre_ejecutada")
        m2 = madre(estado="cerrada")
        self.assertIsNone(E.aviso_barrido(m2, [hija(fecha_programada=D(2026, 11, 26))], None, D(2026, 11, 26), {}))

    def test_aviso_al_cerrar_la_madre_solo_sin_hija_vigente(self):
        self.assertIsNotNone(E.aviso_cierre_madre(madre(), []))
        self.assertIsNone(E.aviso_cierre_madre(madre(), [hija()]))
        self.assertIsNone(E.aviso_cierre_madre(madre(desinstalacion_prevista=None), []))

    def test_la_solicitud_a_daniel_dice_que_incluye_la_desinstalacion(self):
        self.assertEqual(E.resumen_instalacion_con_desinstalacion("2026-11-26"), "Incluye desinstalación 26/11")


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
class TestEventos(unittest.TestCase):
    def test_normalizar_evento(self):
        err, c = E.normalizar_evento({"nombre": "  Felipe   Fierro  ", "fecha_ini": "2026-10-26", "fecha_fin": "2026-10-27",
                                      "direccion": "Av. Kennedy 1", "lat": "-33.4", "lng": "-70.6", "cliente_id": "5"})
        self.assertIsNone(err)
        self.assertEqual(c["nombre"], "Felipe Fierro")
        self.assertEqual((c["fecha_ini"], c["fecha_fin"]), (D(2026, 10, 26), D(2026, 10, 27)))
        self.assertEqual((c["lat"], c["lng"], c["cliente_id"]), (-33.4, -70.6, 5))

    def test_normalizar_errores_y_fechas_opcionales(self):
        self.assertEqual(E.normalizar_evento({"nombre": "ab"})[0]["codigo"], "NOMBRE_REQUERIDO")
        self.assertEqual(E.normalizar_evento({"nombre": "Evento", "fecha_ini": "2026-10-27", "fecha_fin": "2026-10-26"})[0]["codigo"],
                         "FECHAS_INVERTIDAS")
        self.assertEqual(E.normalizar_evento({"nombre": "Evento", "fecha_ini": "basura"})[0]["codigo"], "FECHA_INI_INVALIDA")
        err, c = E.normalizar_evento({"nombre": "Evento sin fechas"})
        self.assertIsNone(err)
        self.assertEqual((c["fecha_ini"], c["fecha_fin"]), (None, None))
        # solo el fin: el inicio se iguala (un evento de un día)
        err, c = E.normalizar_evento({"nombre": "Evento de un día", "fecha_fin": "2026-10-27"})
        self.assertEqual((c["fecha_ini"], c["fecha_fin"]), (D(2026, 10, 27), D(2026, 10, 27)))
        # coordenadas fuera de rango se descartan
        self.assertIsNone(E.normalizar_evento({"nombre": "Evento", "lat": "999"})[1]["lat"])

    def test_estado_calculado_del_evento(self):
        ev = {"fecha_ini": "2026-10-26", "fecha_fin": "2026-10-27", "estado": "activo"}
        pend = [{"id": 1, "tipo": "instalacion", "estado": "programada"}]
        todo = [{"id": 1, "tipo": "instalacion", "estado": "cerrada"}]
        self.assertEqual(E.estado_evento(ev, [], D(2026, 10, 20))[0], "proximo")
        self.assertEqual(E.estado_evento(ev, [], D(2026, 10, 26))[0], "en_curso")
        self.assertEqual(E.estado_evento(ev, pend, D(2026, 10, 30))[0], "por_cerrar")
        self.assertEqual(E.estado_evento(ev, todo, D(2026, 10, 30))[0], "terminado")
        self.assertEqual(E.estado_evento({"estado": "activo"}, [], D(2026, 10, 30))[0], "sin_fechas")
        self.assertEqual(E.estado_evento(dict(ev, estado="cancelado"), [], D(2026, 10, 30))[0], "cancelado")

    def test_instalacion_con_desinstalacion_pendiente_mantiene_el_evento_abierto(self):
        ev = {"fecha_ini": "2026-10-26", "fecha_fin": "2026-10-27", "estado": "activo"}
        ots = [{"id": 1, "tipo": "instalacion", "estado": "cerrada", "desinstalacion_prevista": D(2026, 11, 26),
                "fecha_programada": D(2026, 10, 26)}]
        self.assertEqual(E.estado_evento(ev, ots, D(2026, 10, 30))[0], "por_cerrar")

    def test_avance_por_fase_une_los_pares_y_ignora_las_canceladas(self):
        ots = [
            {"id": 1, "numero_ot": "OT-1", "tipo": "instalacion", "estado": "cerrada", "desinstalacion_prevista": D(2026, 11, 26),
             "fecha_programada": D(2026, 10, 26)},
            {"id": 2, "numero_ot": "OT-2", "tipo": "desinstalacion", "estado": "programada", "ot_madre_id": 1,
             "ot_relacion": "desinstalacion", "fecha_programada": D(2026, 11, 26)},
            {"id": 3, "numero_ot": "OT-3", "tipo": "revision_interna", "estado": "cerrada", "fecha_programada": D(2026, 10, 20)},
            {"id": 4, "numero_ot": "OT-4", "tipo": "instalacion", "estado": "cancelada", "fecha_programada": D(2026, 10, 26)},
        ]
        av = E.avance_evento(ots, D(2026, 11, 1))
        self.assertEqual(av["total"], 3)                       # la cancelada no cuenta
        self.assertEqual(av["por_fase"]["instalacion"]["cerradas"], 1)
        self.assertEqual(av["por_fase"]["desinstalacion"]["pendientes"], 1)
        self.assertEqual(av["por_fase"]["revision"]["cerradas"], 1)
        self.assertEqual(av["pct"], 67)
        self.assertEqual(len(av["pares"]), 1)
        self.assertEqual(av["pares"][0]["hija"]["id"], 2)
        self.assertEqual(av["desinst_pendientes"], 1)
        self.assertEqual(av["proxima_fecha"], "2026-11-26")

    def test_semaforo(self):
        ev = {"fecha_ini": "2026-10-26", "fecha_fin": "2026-10-27", "estado": "activo"}
        cerr = [{"id": 1, "tipo": "instalacion", "estado": "cerrada"}]
        self.assertEqual(E.semaforo_evento(ev, cerr, D(2026, 10, 30))[0], "verde")
        pend = [{"id": 1, "tipo": "instalacion", "estado": "programada", "fecha_programada": D(2026, 10, 26)}]
        self.assertEqual(E.semaforo_evento(ev, pend, D(2026, 10, 26))[0], "ambar")
        sin_agendar = [{"id": 1, "tipo": "instalacion", "estado": "cerrada", "desinstalacion_prevista": D(2026, 11, 26)}]
        self.assertEqual(E.semaforo_evento(ev, sin_agendar, D(2026, 11, 1))[0], "rojo")   # evento terminado sin desinstalar
        vencida = [{"id": 1, "tipo": "instalacion", "estado": "cerrada", "desinstalacion_prevista": D(2026, 10, 28)}]
        self.assertEqual(E.semaforo_evento(dict(ev, fecha_fin="2026-12-30"), vencida, D(2026, 11, 1))[0], "rojo")
        self.assertEqual(E.semaforo_evento(ev, [], D(2026, 10, 30))[0], "gris")
        self.assertEqual(E.semaforo_evento(dict(ev, estado="cancelado"), cerr, D(2026, 10, 30))[0], "gris")

    def test_una_linea_por_dia(self):
        ots = [{"id": 1, "numero_ot": "OT-1", "tipo": "instalacion", "estado": "programada",
                "fecha_programada": D(2026, 10, 26), "fecha_fin": D(2026, 10, 27)},
               {"id": 2, "numero_ot": "OT-2", "tipo": "instalacion", "estado": "cancelada", "fecha_programada": D(2026, 10, 26)}]
        d = E.cuadro_por_dia(ots)
        self.assertEqual(sorted(d), ["2026-10-26", "2026-10-27"])
        self.assertEqual(len(d["2026-10-26"]), 1)

    def test_sugerir_ot_para_asociar(self):
        ev = {"doc_ref": "NVI 687", "cliente_id": 5, "fecha_ini": "2026-10-26", "fecha_fin": "2026-10-27"}
        self.assertEqual(E.sugerir_ot_para_evento({"docs": ["NVI 687"], "cliente_id": 9}, ev), 3)
        self.assertEqual(E.sugerir_ot_para_evento({"docs": ["NVI687"], "cliente_id": 9}, ev), 3)
        self.assertEqual(E.sugerir_ot_para_evento({"docs": [], "cliente_id": 5}, ev), 2)
        self.assertEqual(E.sugerir_ot_para_evento({"docs": [], "cliente_id": 9, "fecha_programada": D(2026, 10, 26)}, ev), 1)
        self.assertEqual(E.sugerir_ot_para_evento({"docs": [], "cliente_id": 9}, ev), 0)


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
class TestSaldoPrefijosFamiliaClasificacion(unittest.TestCase):
    def test_prefijo_bd_tolerante_y_los_existentes_siguen_igual(self):
        self.assertEqual(S.clave_doc("NVV", "VD00010667"), ("VD", "10667"))
        self.assertEqual(S.clave_doc("NVV", "WEB0010667"), ("WEB", "10667"))
        self.assertEqual(S.clave_doc("NVV", "BD00010961"), ("BD", "10961"))
        self.assertEqual(S.clave_doc("BD", "10961"), ("BD", "10961"))
        # sin prefijo no cambia nada (si el NUDO real de la NV 10961 no lo trae)
        self.assertEqual(S.clave_doc("NVV", "0000010961"), ("NVV", "10961"))
        # un NUDO que solo empieza con las letras (no es de esa serie) no se corta
        self.assertEqual(S.clave_doc("NVV", "BDX123"), ("NVV", "BDX123"))
        v = S.variantes_sql(("BD", "10961"))
        self.assertIn(("NVV", "BD00010961"), v)
        self.assertIn(("BD", "10961"), v)
        self.assertIn("BD", S.NOTAS_VENTA)

    def test_no_queda_una_tupla_vd_web_suelta_en_los_modulos_del_saldo(self):
        patron = re.compile(r"""[\(\[]\s*["']VD["']\s*,\s*["']WEB["']\s*[\)\]]""")
        for archivo in ("ot_saldo_servicio.py", "erp_engine.py", "app.py"):
            sueltas = [(i + 1, l.strip()[:90]) for i, l in enumerate(F.texto(archivo).split("\n"))
                       if patron.search(l) and not l.lstrip().startswith("#")]
            self.assertEqual(sueltas, [], "usa ot_saldo_servicio.PREFIJOS_NOTA_VENTA en vez de ('VD','WEB'): %s %s" % (archivo, sueltas))

    def test_la_lista_sql_de_madre_valida_coincide_con_la_del_saldo(self):
        amb = F.cargar(constantes=("_OT_FIN_SQL_MADRE_VALIDA",))
        listas = [g for g in re.findall(r"IN \(([^)]*)\)", amb["_OT_FIN_SQL_MADRE_VALIDA"]) if "'FCV'" in g]
        self.assertEqual(len(listas), 1, "debe haber una sola lista de tipos de documento")
        tipos = set(re.findall(r"'([A-Z]+)'", listas[0]))
        self.assertEqual(tipos, set(S.NOTAS_VENTA) | set(S.DOCS_COBRO))

    def test_clasificacion_cubierta(self):
        c = S.clasificar_ot(cubierta=True)
        self.assertEqual((c["codigo"], c["nivel"]), ("cubierta", "azul"))
        # va DESPUÉS de contrato y antes de sin_documento en el orden
        o = S.CLASIF_ORDEN
        self.assertGreater(o.index("cubierta"), o.index("contrato"))
        self.assertLess(o.index("cubierta"), o.index("sin_documento"))
        # una cubierta nunca se lee como «sin documento»
        self.assertEqual(S.clasificar_ot(cubierta=True, sin_documento=True)["codigo"], "cubierta")
        # una garantía manda sobre la cubierta
        self.assertEqual(S.clasificar_ot(cubierta=True, cero_motivo="garantia")["codigo"], "garantia")

    def test_equilibrio_de_la_familia_no_depende_del_orden_de_pago(self):
        # la nota de venta cobra 300.000 (madre); la hija cubierta no cobra; técnico 120.000 + 80.000
        m = [{"vid": 10, "cobre": 300000, "reduccion": 0, "pago_comprometido": 120000},
             {"vid": 11, "cobre": 0, "reduccion": 0, "pago_comprometido": 80000}]
        r_madre = S.equilibrio_familia(m, 10, 120000)
        r_hija = S.equilibrio_familia(m, 11, 80000)
        self.assertTrue(r_madre["ok"] and r_hija["ok"])
        for k in ("pago", "cobro_efectivo", "exceso", "falta_autorizar"):
            self.assertEqual(r_madre[k], r_hija[k], k)
        self.assertEqual((r_madre["pago"], r_madre["cobro_efectivo"]), (200000, 300000))

    def test_equilibrio_de_la_familia_marca_el_exceso_y_respeta_la_autorizacion_mayor(self):
        m = [{"vid": 10, "cobre": 100000, "reduccion": 0, "pago_comprometido": 90000},
             {"vid": 11, "cobre": 0, "reduccion": 0, "pago_comprometido": 60000}]
        r = S.equilibrio_familia(m, 11, 60000)
        self.assertFalse(r["ok"])
        self.assertEqual((r["exceso"], r["falta_autorizar"]), (50000, 50000))
        self.assertTrue(S.equilibrio_familia(m, 11, 60000, autorizada=50000)["ok"])
        # la reducción por saldo ya usado por otras OT baja el cobro efectivo de la madre
        m2 = [{"vid": 10, "cobre": 100000, "reduccion": 40000, "pago_comprometido": 0},
              {"vid": 11, "cobre": 0, "reduccion": 0, "pago_comprometido": 0}]
        self.assertEqual(S.equilibrio_familia(m2, 10, 50000)["cobro_efectivo"], 60000)
        # exento (cobro $0 ya autorizado): nunca falta nada
        self.assertTrue(S.equilibrio_familia(m, 11, 999999, exento=True)["ok"])

    def test_la_hija_que_cobra_su_linea_suma_su_cobro_a_la_familia(self):
        m = [{"vid": 10, "cobre": 220000, "reduccion": 0, "pago_comprometido": 100000},
             {"vid": 11, "cobre": 80000, "reduccion": 0, "pago_comprometido": 50000}]
        r = S.equilibrio_familia(m, 10, 100000)
        self.assertEqual((r["cobro_efectivo"], r["pago"]), (300000, 150000))

    def test_perdida_de_la_familia_al_cerrar(self):
        # cobra 200.000; técnico madre 120.000 + técnico hija 100.000 -> pierde 20.000
        r = S.perdida_familia(200000, [{"tecnico": 120000, "despacho": 0}, {"tecnico": 100000, "despacho": 0}])
        self.assertEqual((r["perdida"], r["ok"], r["falta_autorizar"]), (20000, False, 20000))
        self.assertTrue(S.perdida_familia(200000, [{"tecnico": 120000}, {"tecnico": 100000}], autorizada=20000)["ok"])
        self.assertTrue(S.perdida_familia(200000, [{"tecnico": 120000}, {"tecnico": 70000, "despacho": 10000}])["ok"])


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
class TestCobertura(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.a = F.cargar(["_ot_es_interna", "_ot_fin_num", "_ot_cobertura"])
        cls.cob = staticmethod(cls.a["_ot_cobertura"])

    def v(self, **kw):
        base = dict(tipo="desinstalacion", ot_relacion="desinstalacion", ot_madre_id=7, madre_valida=1, cliente_id=5,
                    modalidad_cobro="pagado", cubierto_por="cliente", zz_monto=None, zz_envio_monto=None, valor_origen=None)
        base.update(kw)
        return base

    def test_cubierta(self):
        self.assertEqual(self.cob(self.v()), "cubierta")

    def test_se_deduce_de_la_relacion_nunca_de_valor_origen(self):
        self.assertEqual(self.cob(self.v(valor_origen="cubierta")), "cubierta")          # da lo mismo lo que diga
        self.assertEqual(self.cob(self.v(ot_relacion=None, valor_origen="cubierta")), "cobra")
        self.assertEqual(self.cob(self.v(ot_madre_id=None, valor_origen="cubierta")), "cobra")
        self.assertEqual(self.cob(self.v(tipo="instalacion", valor_origen="cubierta")), "cobra")

    def test_sin_la_bandera_de_madre_valida_falla_cerrado(self):
        self.assertEqual(self.cob(self.v(madre_valida=0)), "cobra")
        v = self.v()
        del v["madre_valida"]
        self.assertEqual(self.cob(v), "cobra")

    def test_con_cobro_propio_va_por_el_camino_normal(self):
        self.assertEqual(self.cob(self.v(zz_monto=50000, valor_origen="zz")), "cobra")
        self.assertEqual(self.cob(self.v(zz_envio_monto=1000)), "cobra")
        self.assertEqual(self.cob(self.v(zz_monto=0, zz_envio_monto=0)), "cubierta")

    def test_garantia_y_regalia_mandan_sobre_la_cubierta(self):
        self.assertEqual(self.cob(self.v(modalidad_cobro="garantia")), "garantia")
        self.assertEqual(self.cob(self.v(modalidad_cobro="sin_costo", cobro_cero_motivo="regalia")), "regalia")

    def test_las_listas_de_origenes_no_se_tocaron(self):
        amb = F.cargar(constantes=("_OT2_VALOR_ORIGENES", "_OT_SALDO_ORIGENES_NO_DOC", "_OT_FIN_ORIGENES_NO_COBRO"))
        self.assertNotIn("cubierta", amb["_OT2_VALOR_ORIGENES"])
        self.assertNotIn("cubierta", amb["_OT_SALDO_ORIGENES_NO_DOC"])
        self.assertNotIn("cubierta", amb["_OT_FIN_ORIGENES_NO_COBRO"])
        # y ot_saldo_servicio.aportes_reales_ot no conoce ese origen
        self.assertNotIn("cubierta", re.sub(r"#.*", "", S.aportes_reales_ot.__code__.co_consts.__repr__()))


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
class _FakeApp:
    """Dobles de lo que `_ot_puerta_via_cubierta` y su gente leen de la base."""

    def __init__(self, filas, hijas=None, docs=None, auts=None):
        self.filas, self.hijas_por_madre, self.docs, self.auts = filas, hijas or {}, docs or {}, auts or set()


def _amb_puerta(fa):
    amb = F.cargar(["_ot_cubierta_madre_check", "_ot_puerta_via_cubierta", "_ot_madre_tiene_doc_real", "_ot_madre_tiene_factura"],
                   extra={"_evt": E, "_saldo": S, "_OT_DOCS_COBRO": ("FCV", "FCE", "BLV", "BLE"),
                          "print": lambda *a, **k: None})
    amb["_ot_fila_basica"] = lambda vid: dict(fa.filas[int(vid)]) if int(vid) in fa.filas else None
    amb["_ot_hijas_de"] = lambda mid, cur=None, estricto=False: [dict(h) for h in fa.hijas_por_madre.get(int(mid), [])]
    amb["_ot_puerta_docs_de"] = lambda vid: [dict(d) for d in fa.docs.get(int(vid), [])]
    amb["_ot_aut_aprobada"] = lambda aid: bool(aid) and int(aid) in fa.auts
    return amb


def _doc(tido, nudo="1", es_cobro=True):
    return {"tido": tido, "nudo": nudo, "origen": "erp", "es_cobro": es_cobro, "es_principal": True}


class TestPuertaViaCubierta(unittest.TestCase):
    def fa(self, madre_kw=None, hija_kw=None, docs_madre=None, hijas=None, auts=None):
        m = dict(madre(**(madre_kw or {})))
        h = dict(hija(**(hija_kw or {})), zz_monto=None, zz_envio_monto=None)
        return _FakeApp({10: m, 11: h}, hijas={10: hijas if hijas is not None else [h]},
                        docs={10: docs_madre if docs_madre is not None else [_doc("NVV", "VD00010961")]}, auts=auts)

    def test_crear_con_madre_valida_pasa_sin_documentos_propios(self):
        fa = self.fa()
        amb = _amb_puerta(fa)
        r = amb["_ot_puerta_via_cubierta"]({"cliente_id": 5, "tipo": "desinstalacion"}, "crear", madre=fa.filas[10])
        self.assertTrue(r["ok"])
        self.assertEqual(r["via"], "cubierta_madre")
        self.assertEqual(r["documentos_ok"], [])               # nunca inserta el documento de la madre como cobro de la hija
        self.assertTrue(r["docs_ya_guardados"])
        self.assertEqual(r["madre_id"], 10)

    def test_madre_sin_documento_real_no_sirve_y_con_la_autorizacion_que_la_incluia_si(self):
        fa = self.fa(docs_madre=[])
        amb = _amb_puerta(fa)
        self.assertIsNone(amb["_ot_puerta_via_cubierta"]({"cliente_id": 5}, "crear", madre=fa.filas[10]))
        fa2 = self.fa(docs_madre=[], madre_kw={"desinstalacion_aut_id": 77}, auts={77})
        self.assertTrue(_amb_puerta(fa2)["_ot_puerta_via_cubierta"]({"cliente_id": 5}, "crear", madre=fa2.filas[10])["ok"])
        # una autorización que NO está aprobada no cuenta
        fa3 = self.fa(docs_madre=[], madre_kw={"desinstalacion_aut_id": 77}, auts=set())
        self.assertIsNone(_amb_puerta(fa3)["_ot_puerta_via_cubierta"]({"cliente_id": 5}, "crear", madre=fa3.filas[10]))

    def test_una_cotizacion_o_un_cero_autorizado_en_la_madre_no_son_documento_de_random(self):
        fa = self.fa(docs_madre=[{"tido": "COT", "nudo": "5", "origen": "cotizacion", "es_cobro": False}])
        self.assertIsNone(_amb_puerta(fa)["_ot_puerta_via_cubierta"]({"cliente_id": 5}, "crear", madre=fa.filas[10]))
        fa = self.fa(docs_madre=[_doc("NVV", "VD1", es_cobro=False)])
        self.assertIsNone(_amb_puerta(fa)["_ot_puerta_via_cubierta"]({"cliente_id": 5}, "crear", madre=fa.filas[10]))

    def test_madre_cancelada_o_ajena_la_hija_vuelve_a_pedir_documento(self):
        for kw in ({"estado": "cancelada"}, {"estado": "anulada"}, {"cliente_id": 99}, {"tipo": "correctiva"},
                   {"ot_madre_id": 3}, {"desinstalacion_prevista": None}):
            fa = self.fa(madre_kw=kw)
            self.assertIsNone(_amb_puerta(fa)["_ot_puerta_via_cubierta"]({"cliente_id": 5}, "crear", madre=fa.filas[10]), kw)

    def test_cerrar_exige_factura_o_boleta_de_la_madre(self):
        fa = self.fa(docs_madre=[_doc("NVV", "VD00010961")])
        amb = _amb_puerta(fa)
        r = amb["_ot_puerta_via_cubierta"]({"id": 11}, "cerrar")
        self.assertFalse(r["ok"])
        self.assertEqual(r["falta"], "madre_factura")
        self.assertEqual(r["madre_numero"], "OT-2026-00350")
        fa2 = self.fa(docs_madre=[_doc("NVV", "VD00010961"), _doc("FCV", "11500")])
        r2 = _amb_puerta(fa2)["_ot_puerta_via_cubierta"]({"id": 11}, "cerrar")
        self.assertTrue(r2["ok"])
        self.assertEqual(r2["via"], "cubierta_madre")

    def test_madre_cerrada_sin_factura_no_basta(self):
        # «madre cerrada» pudo cerrar por cerrar_sin_documento o cobro_cero: se exige la factura o boleta
        fa = self.fa(madre_kw={"estado": "cerrada"}, docs_madre=[_doc("NVV", "VD1")])
        self.assertFalse(_amb_puerta(fa)["_ot_puerta_via_cubierta"]({"id": 11}, "cerrar")["ok"])

    def test_el_ot_madre_id_del_dict_v_se_ignora(self):
        # alguien arma v desde el body con ot_madre_id=999: la puerta relee la hija por v['id'] y usa lo que dice la base
        fa = self.fa()
        r = _amb_puerta(fa)["_ot_puerta_via_cubierta"]({"id": 11, "ot_madre_id": 999, "tipo": "desinstalacion"}, "cerrar")
        self.assertEqual(r.get("madre_id"), 10)
        # y una OT que en la base NO es hija no se vuelve cubierta porque el body lo diga
        fa2 = _FakeApp({12: dict(hija(id=12), ot_madre_id=None, ot_relacion=None, tipo="desinstalacion")}, docs={})
        self.assertIsNone(_amb_puerta(fa2)["_ot_puerta_via_cubierta"]({"id": 12, "ot_madre_id": 10, "ot_relacion": "desinstalacion"}, "cerrar"))
        # sin id ni madre del servidor: nada
        self.assertIsNone(_amb_puerta(fa)["_ot_puerta_via_cubierta"]({"tipo": "desinstalacion", "ot_madre_id": 10}, "crear"))

    def test_hija_que_cobra_su_propia_linea_va_por_el_camino_normal(self):
        fa = self.fa(hija_kw={})
        fa.filas[11]["zz_monto"] = 80000
        self.assertIsNone(_amb_puerta(fa)["_ot_puerta_via_cubierta"]({"id": 11}, "cerrar"))

    def test_una_sola_hija_vigente_es_cubierta(self):
        h1 = dict(hija(id=11, estado="cerrada"), zz_monto=None, zz_envio_monto=None)
        h2 = dict(hija(id=12), zz_monto=None, zz_envio_monto=None)
        fa = _FakeApp({10: madre(), 11: h1, 12: h2}, hijas={10: [h1, h2]}, docs={10: [_doc("NVV", "VD1"), _doc("FCV", "9")]})
        amb = _amb_puerta(fa)
        self.assertTrue(amb["_ot_puerta_via_cubierta"]({"id": 11}, "cerrar")["ok"])      # la más antigua vigente (cerrada cuenta)
        self.assertIsNone(amb["_ot_puerta_via_cubierta"]({"id": 12}, "cerrar"))           # la segunda: no hay cadena de regalos
        # si la primera se cancela, la segunda pasa a ser la vigente
        h1c = dict(h1, estado="cancelada")
        fa2 = _FakeApp({10: madre(), 11: h1c, 12: h2}, hijas={10: [h1c, h2]}, docs={10: [_doc("NVV", "VD1"), _doc("FCV", "9")]})
        self.assertTrue(_amb_puerta(fa2)["_ot_puerta_via_cubierta"]({"id": 12}, "cerrar")["ok"])


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
class TestCamposCubierta(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.a = F.cargar(["_ot_fin_campos_cubierta"], constantes=("_OT2_CENTROS_COSTO",), extra={"_evt": E, "_saldo": S})

    def test_mismas_claves_que_el_validador_normal(self):
        normal = F.claves_de_diccionario("_ot_validar_normalizar_finanzas", "campos")
        cub = F.claves_de_diccionario("_ot_fin_campos_cubierta", "campos")
        self.assertEqual(normal - cub, set(), "faltan claves que el INSERT del núcleo desempaca")
        self.assertEqual(cub - normal, {"madre_id", "madre_numero"})

    def test_la_hija_nace_sin_documento_ni_cobro_propios(self):
        err, c = self.a["_ot_fin_campos_cubierta"]({"costo_proveedor": "0", "costo_despacho": "5000"}, dict(madre(), centro_costo="sstt"))
        self.assertIsNone(err)
        self.assertEqual(c["cobertura"], "cubierta")
        self.assertIsNone(c["zz_monto"]); self.assertIsNone(c["zz_envio_monto"]); self.assertIsNone(c["factura_tido"])
        self.assertIsNone(c["valor_origen"])                         # JAMÁS 'cubierta'
        self.assertEqual((c["modalidad_cobro"], c["cubierto_por"]), ("pagado", "cliente"))   # no 'contrato'
        self.assertEqual(c["estado_facturacion"], "no_aplica")
        self.assertEqual((c["costo_proveedor"], c["costo_despacho"]), (0.0, 5000.0))
        self.assertEqual(c["documentos_validados"], [])
        self.assertIsNone(c["cobro_cero_motivo"]); self.assertIsNone(c["autorizacion_id"])
        self.assertEqual(c["centro_costo"], "sstt")
        self.assertEqual(c["madre_numero"], "OT-2026-00350")

    def test_el_costo_del_proveedor_es_obligatorio_aunque_sea_cero(self):
        err, c = self.a["_ot_fin_campos_cubierta"]({}, dict(madre(), centro_costo="sstt"))
        self.assertEqual(err["error_codigo"], "FINANZAS_SIN_COSTO_PROVEEDOR")
        err, c = self.a["_ot_fin_campos_cubierta"]({"costo_proveedor": "abc"}, dict(madre(), centro_costo="sstt"))
        self.assertEqual(err["error_codigo"], "COSTO_PROVEEDOR_INVALIDO")

    def test_sin_centro_de_costo_en_la_madre(self):
        err, c = self.a["_ot_fin_campos_cubierta"]({"costo_proveedor": 0}, dict(madre(), centro_costo=None))
        self.assertEqual(err["error_codigo"], "FINANZAS_SIN_CENTRO_COSTO")


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
class TestNormalizarYValidarPrevio(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        extra = {"_evt": E, "_saldo": S, "print": lambda *a, **k: None}
        cls.a = F.cargar(["_ot_desinst_normalizar", "_ot_desinst_validar_previo"], extra=extra)
        cls.a["_ot_validar_tecnicos_una_empresa"] = lambda ids: None
        cls.a["_ot_validar_tope_ot_sin_contrato"] = lambda ids, extra=0: None
        cls.a["_feriados_cl"] = lambda *f: {}
        cls.a["_ot_desinst_remanente_saldo"] = lambda docs, ped=None, rut=None, ex=None: dict(cls.rem)

    REM0 = {"servicio": 0, "despacho": 0, "leido": True, "por_doc": []}
    rem = dict(REM0)

    def norm(self, **kw):
        base = {"lleva": True, "fecha": "2026-11-26", "hora_inicio": "09:00", "hora_fin": "13:00", "tecnico_user_ids": [3],
                "tecnico_lider_id": 3, "costo_proveedor": 0}
        base.update(kw)
        err, dd = self.a["_ot_desinst_normalizar"](base)
        self.assertIsNone(err)
        return dd

    def test_normalizar(self):
        dd = self.norm(tecnicos=[], lider=None, nota="x" * 3000)
        self.assertEqual(dd["lleva"], "si")
        self.assertEqual(dd["tecnico_user_ids"], [3])
        self.assertEqual(len(dd["nota"]), 1500)
        self.assertEqual(self.a["_ot_desinst_normalizar"]({"lleva": "por_definir"})[1]["lleva"], "por_definir")
        self.assertEqual(self.a["_ot_desinst_normalizar"]({"lleva": False})[1]["lleva"], "no")
        self.assertEqual(self.a["_ot_desinst_normalizar"]({"lleva": True, "costo_proveedor": "mucho"})[0]["codigo"], "MONTO_INVALIDO")
        # el líder se suma a los técnicos aunque no venga en la lista
        self.assertEqual(self.norm(tecnico_user_ids=[3], tecnico_lider_id=8)["tecnico_user_ids"], [3, 8])

    def validar(self, dd, m=None, rem=None):
        self.__class__.rem = dict(self.REM0 if rem is None else rem)
        return self.a["_ot_desinst_validar_previo"](dd, m or madre(), [("NVV", "VD00010961")], None, None, None)

    def test_validar_ok_sin_saldo_remanente_es_cubierta(self):
        dd = self.norm()
        err, info = self.validar(dd)
        self.assertIsNone(err)
        self.assertEqual(dd["cobro"], "cubierta")

    def test_validar_pide_elegir_si_queda_saldo(self):
        dd = self.norm()
        err, _ = self.validar(dd, rem={"servicio": 120000, "despacho": 0, "leido": True, "por_doc": []})
        self.assertEqual(err["codigo"], "COBRO_REQUERIDO")
        dd = self.norm(cobro="cubierta", argumento_cubierta="porque el mall absorbe la desinstalación de la trotadora")
        err, _ = self.validar(dd, rem={"servicio": 120000, "despacho": 0, "leido": True, "por_doc": []})
        self.assertIsNone(err)

    def test_validar_cobra_no_puede_pasar_del_saldo_que_calcula_el_servidor(self):
        dd = self.norm(cobro="cobra", cobra_servicio=150000)
        err, _ = self.validar(dd, rem={"servicio": 120000, "despacho": 0, "leido": True, "por_doc": []})
        self.assertEqual(err["codigo"], "ZZ_SALDO_CONSUMIDO")
        dd = self.norm(cobro="cobra", cobra_servicio=100000)
        err, _ = self.validar(dd, rem={"servicio": 120000, "despacho": 0, "leido": True, "por_doc": []})
        self.assertIsNone(err)
        dd = self.norm(cobro="cobra")
        err, _ = self.validar(dd, rem={"servicio": 120000, "despacho": 0, "leido": True, "por_doc": []})
        self.assertEqual(err["codigo"], "COBRO_REQUERIDO")

    def test_validar_domingo_antes_de_la_madre_tecnico_y_costo(self):
        self.assertEqual(self.validar(self.norm(fecha="2026-11-29"))[0]["codigo"], "DOMINGO_BLOQUEADO")
        self.assertEqual(self.validar(self.norm(fecha="2026-10-20"))[0]["codigo"], "DESINST_ANTES_DE_INSTALACION")
        self.assertEqual(self.validar(self.norm(tecnico_user_ids=[], tecnico_lider_id=None))[0]["codigo"], "TECNICO_OBLIGATORIO")
        self.assertEqual(self.validar(self.norm(costo_proveedor=None))[0]["codigo"], "FINANZAS_SIN_COSTO_PROVEEDOR")
        # el sábado y el feriado solo avisan
        err, info = self.validar(self.norm(fecha="2026-11-28"))
        self.assertIsNone(err)
        self.assertTrue(info["avisos"])

    def test_por_definir_exige_fecha_estimada_y_que_no_sea_antes_de_la_madre(self):
        dd = self.norm(lleva="por_definir", fecha=None)
        self.assertEqual(self.validar(dd)[0]["codigo"], "DESINST_FECHA_REQUERIDA")
        self.assertIsNone(self.validar(self.norm(lleva="por_definir", fecha="2026-11-26"))[0])
        self.assertEqual(self.validar(self.norm(lleva="por_definir", fecha="2026-10-01"))[0]["codigo"], "DESINST_ANTES_DE_INSTALACION")


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
class _Cur:
    def __init__(self, existentes=None, madre_docs=None):
        self.q, self.params, self._rows = [], [], []
        self.madre_docs = madre_docs or []
        self.existentes = existentes or []
        self.lastrowid = 100

    def execute(self, sql, p=None):
        self.q.append(sql)
        self.params.append(p)
        s = " ".join(sql.split())
        if s.startswith("SELECT erp_tido, erp_nudo, rut FROM mant_visita_documentos"):
            self._rows = list(self.madre_docs)
        elif s.startswith("SELECT id, erp_tido, erp_nudo, heredado_de_visita_id, reemplazado_por_id FROM mant_visita_documentos"):
            self._rows = list(self.existentes)
        else:
            self._rows = []
        if s.startswith("INSERT INTO"):
            self.lastrowid += 1

    def fetchall(self):
        return self._rows


class TestEspejoDeDocumentos(unittest.TestCase):
    def test_los_documentos_de_la_madre_se_espejan_sin_cobro_ni_zz(self):
        amb = F.cargar(["_ot_espejar_docs_en_hija"], extra={"current_username": lambda: "aaron"})
        cur = _Cur(madre_docs=[{"erp_tido": "NVV", "erp_nudo": "VD00010961", "rut": "76.1-1"},
                               {"erp_tido": "FCV", "erp_nudo": "0000011500", "rut": None}])
        n = amb["_ot_espejar_docs_en_hija"](cur, 11, 10, "OT-2026-00350")
        self.assertEqual(n, 2)
        inserts = [(q, p) for q, p in zip(cur.q, cur.params) if "INSERT INTO mant_visita_documentos" in q]
        self.assertEqual(len(inserts), 2)
        sql, par = inserts[0]
        self.assertIn("es_cobro, es_principal", sql)
        self.assertIn("'erp',0,0", sql.replace(" ", ""), "es_cobro=0 y es_principal=0")
        self.assertIn("heredado_de_visita_id", sql)
        self.assertNotIn("zz_serv_monto", sql)
        self.assertEqual(par[-1], 10)
        self.assertEqual(par[4], "Heredado de OT-2026-00350")

    def test_volver_a_espejar_no_repite_lo_que_ya_esta(self):
        amb = F.cargar(["_ot_espejar_docs_en_hija"], extra={"current_username": lambda: "x"})
        cur = _Cur(madre_docs=[{"erp_tido": "NVV", "erp_nudo": "VD00010961", "rut": None},
                               {"erp_tido": "FCV", "erp_nudo": "0000011500", "rut": None}],
                   existentes=[{"id": 5, "erp_tido": "NVV", "erp_nudo": "VD00010961", "heredado_de_visita_id": 10,
                                "reemplazado_por_id": None}])
        self.assertEqual(amb["_ot_espejar_docs_en_hija"](cur, 11, 10, "OT-1"), 1)

    def test_un_documento_propio_de_la_desinstalacion_no_se_vuelve_a_espejar(self):
        # si la nota de venta trae una línea de desinstalación y la hija la agregó como SU documento, el espejo no la repite
        amb = F.cargar(["_ot_espejar_docs_en_hija"], extra={"current_username": lambda: "x"})
        cur = _Cur(madre_docs=[{"erp_tido": "NVV", "erp_nudo": "VD00010961", "rut": None},
                               {"erp_tido": "FCV", "erp_nudo": "0000011500", "rut": None}],
                   existentes=[{"id": 6, "erp_tido": "NVV", "erp_nudo": "VD00010961", "heredado_de_visita_id": None,
                                "reemplazado_por_id": None}])
        self.assertEqual(amb["_ot_espejar_docs_en_hija"](cur, 11, 10, "OT-1"), 1)
        self.assertEqual(len([q for q in cur.q if "INSERT INTO mant_visita_documentos" in q]), 1)

    def test_el_espejo_de_un_documento_reemplazado_queda_dado_de_baja_por_la_copia_vigente(self):
        # la instalación cambió su nota de venta por la factura: la copia vieja en la desinstalación no sigue vigente
        amb = F.cargar(["_ot_espejar_docs_en_hija"], extra={"current_username": lambda: "x"})
        cur = _Cur(madre_docs=[{"erp_tido": "FCV", "erp_nudo": "0000011500", "rut": None}],
                   existentes=[{"id": 5, "erp_tido": "NVV", "erp_nudo": "VD00010961", "heredado_de_visita_id": 10,
                                "reemplazado_por_id": None}])
        self.assertEqual(amb["_ot_espejar_docs_en_hija"](cur, 11, 10, "OT-1"), 1)
        bajas = [(q, p) for q, p in zip(cur.q, cur.params) if "SET reemplazado_por_id" in q]
        self.assertEqual(len(bajas), 1)
        self.assertEqual(bajas[0][1], (101, 5))            # la copia nueva (id 101) da de baja a la vieja (id 5)
        self.assertNotIn("DELETE", " ".join(cur.q).upper())  # nada se borra


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
class TestEquilibrioFamiliaEnApp(unittest.TestCase):
    """_fac_equilibrio_ot (la pública) une a la madre con su hija; el orden de pago no cambia el resultado."""

    def amb(self, solo, familia):
        a = F.cargar(["_fac_equilibrio_ot"], extra={"_saldo": S, "print": lambda *x, **k: None})
        a["_fac_equilibrio_ot_solo"] = lambda vid, pago=None, solo_cache=False: dict(solo[int(vid)])
        a["_fac_familia_de"] = lambda vid: familia
        a["_fac_pago_comprometido"] = lambda vid, declarado: _saldo_entero(declarado)
        return a

    def solo(self):
        base = {"ok": True, "code": None, "estado": "ok", "mensaje": "", "reduccion": 0, "autorizada": 0,
                "autorizada_por": None, "pendiente_id": None, "cobra": True}
        return {10: dict(base, vid=10, numero_ot="OT-2026-00350", pago=120000, cobre=300000, cobro_efectivo=300000),
                11: dict(base, vid=11, numero_ot="OT-2026-00351", pago=80000, cobre=0, cobro_efectivo=0, cobra=False,
                         estado="exento")}

    FAM = {"madre_id": 10, "hijas": [{"id": 11}], "ids": [10, 11]}

    def test_el_orden_no_cambia_el_resultado(self):
        a = self.amb(self.solo(), self.FAM)
        rm = a["_fac_equilibrio_ot"](10)
        rh = a["_fac_equilibrio_ot"](11)
        self.assertTrue(rm["ok"] and rh["ok"])
        for k in ("pago_familia", "cobro_familia", "exceso", "falta_autorizar"):
            self.assertEqual(rm[k], rh[k], k)
        self.assertEqual((rm["pago_familia"], rm["cobro_familia"]), (200000, 300000))

    def test_pasarse_del_equilibrio_de_la_familia_pide_autorizacion(self):
        s = self.solo()
        s[10]["cobre"] = s[10]["cobro_efectivo"] = 150000
        a = self.amb(s, self.FAM)
        r = a["_fac_equilibrio_ot"](11)
        self.assertFalse(r["ok"])
        self.assertEqual(r["code"], "SOBRE_EQUILIBRIO")
        self.assertEqual(r["exceso"], 50000)
        self.assertIn("se pide sobre la instalación", r["mensaje"])
        # la autorización más alta de cualquier miembro la cubre
        s[10]["autorizada"], s[10]["autorizada_por"] = 50000, "Daniel"
        a2 = self.amb(s, self.FAM)
        self.assertTrue(a2["_fac_equilibrio_ot"](11)["ok"])
        self.assertEqual(a2["_fac_equilibrio_ot"](11)["estado"], "autorizada")

    def test_sin_familia_es_la_evaluacion_individual_de_siempre(self):
        a = self.amb(self.solo(), None)
        self.assertEqual(a["_fac_equilibrio_ot"](10), self.solo()[10])

    def test_sin_documento_en_la_instalacion_no_frena_el_pago(self):
        s = self.solo()
        s[10].update(estado="sin_documento")
        r = self.amb(s, self.FAM)["_fac_equilibrio_ot"](11)
        self.assertTrue(r["ok"])
        self.assertNotIn("pago_familia", r)

    def test_lectura_incompleta_no_inventa(self):
        s = self.solo()
        s[10].update(estado="sin_verificar", code="LECTURA_INCOMPLETA", ok=False)
        r = self.amb(s, self.FAM)["_fac_equilibrio_ot"](11)
        self.assertEqual(r["estado"], "sin_verificar")
        self.assertFalse(r["ok"])
        r2 = self.amb(s, self.FAM)["_fac_equilibrio_ot"](11, solo_cache=True)
        self.assertTrue(r2["ok"], "en listas (solo_cache) la lectura incompleta no es un rechazo")


def _saldo_entero(x):
    return S._entero(x)


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
class TestMigraciones(unittest.TestCase):
    def test_una_clausula_por_sentencia_y_la_guardia_las_reconoce(self):
        fuente = F.fuente_funcion("_ensure_ot_eventos_cols")
        import ast
        sentencias = None
        for nodo in ast.walk(ast.parse(fuente)):
            if isinstance(nodo, ast.Assign) and any(getattr(t, "id", "") == "sentencias" for t in nodo.targets):
                sentencias = [ast.literal_eval(e) for e in nodo.value.elts]
        self.assertTrue(sentencias, "no se encontró la lista de sentencias")
        # la guardia de DDL de boot (REGLA #18)
        amb = {}
        t = F.texto()
        i0 = t.index("_DDL_RE = re.compile(")
        i1 = t.index("_ddl_guard_saltados = 0")
        exec(compile(ast.parse("import re\n" + t[i0:i1]), "<guardia>", "exec"), amb)
        amb["_ddl_guard_saltados"] = 0
        guardia = F.cargar(["_ddl_ya_aplicado"], extra=amb)["_ddl_ya_aplicado"]
        for s in sentencias:
            self.assertFalse(amb["_DDLG_MULTI"].search(s), "dos cláusulas en una sentencia: " + s[:60])
            # con la base "ya aplicada" (la consulta devuelve una fila) la guardia SALTA la sentencia sin pedir el lock
            self.assertTrue(guardia(lambda sql, p: {"t": "x", "1": 1}, s), "la guardia no reconoce: " + s[:70])
        txt = " ".join(sentencias)
        for col in ("evento_id", "ot_madre_id", "ot_relacion", "desinstalacion_prevista", "heredado_de_visita_id",
                    "idx_v_evento_fecha", "idx_v_madre_rel", "mant_eventos"):
            self.assertIn(col, txt)
        self.assertNotIn("DELETE", txt.upper())

    def test_tipo_de_autorizacion_nuevo_en_el_enum_y_en_las_listas(self):
        t = F.texto()
        self.assertEqual(t.count("'documento_observado','desinstalacion_cubierta')"), 2, "CREATE TABLE y MODIFY del ENUM")
        amb = F.cargar(constantes=("_OT_AUT_TIPOS", "_OT_AUT_TIPO_TXT"))
        self.assertIn("desinstalacion_cubierta", amb["_OT_AUT_TIPOS"])
        self.assertIn("desinstalacion_cubierta", amb["_OT_AUT_TIPO_TXT"])
        # el ENUM del CREATE y el del MODIFY son EL MISMO (REGLA #18: nunca dos definiciones distintas)
        enums = re.findall(r"tipo\s+ENUM\(([^)]*)\)\s+NOT NULL|MODIFY tipo ENUM\(([^)]*)\) NOT NULL", F.fuente_funcion("_ensure_mant_ot_autorizaciones"))
        valores = [set(re.findall(r"'([a-z_]+)'", a or b)) for a, b in enums]
        self.assertEqual(len(valores), 2)
        self.assertEqual(valores[0], valores[1])

    def test_el_arranque_corre_la_migracion_despues_de_la_tabla_de_documentos(self):
        t = F.texto()
        self.assertLess(t.index("_ensure_mant_visita_documentos()\nexcept"), t.index("_ensure_ot_eventos_cols()\nexcept"))


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
class TestCandadosPorTexto(unittest.TestCase):
    def test_nada_de_lo_nuevo_entra_al_allowed_del_put(self):
        src = F.fuente_funcion("mant_visita_update")
        i = src.index("allowed = [")
        j = src.index("]", src.index('"centro_costo"', i))
        lista = src[i:j]
        for col in ("evento_id", "ot_madre_id", "ot_relacion", "desinstalacion_prevista", "desinstalacion_aut_id"):
            self.assertNotIn(col, lista)

    def test_el_nucleo_no_lee_ot_madre_id_del_body(self):
        core = F.fuente_funcion("_ot2_crear_core")
        self.assertIn("def _ot2_crear_core(d, autorizacion=None, madre=None):", core)
        self.assertNotIn('d.get("ot_madre_id")', core)
        self.assertNotIn('d["ot_madre_id"]', core)
        self.assertIn('"ot_madre_id", "ot_relacion"', core, "las llaves solo del servidor se descartan del body")
        # la hija no hereda ticket, evento del body ni el forzar_choque de una aprobación
        self.assertIn('"evento_id", "ticket_id"', core)
        # y el correo al cliente se salta en la hija (REGLA #22)
        self.assertIn("if not es_interna and not es_hija:\n        try: _mant_ot_email_cliente_creacion(vid)", core)

    def test_la_puerta_acepta_madre_solo_como_parametro_y_relee_por_id(self):
        puerta = F.fuente_funcion("_ot_puerta_documento")
        self.assertIn("def _ot_puerta_documento(v, momento=\"crear\", autorizacion=None, superadmin_declara=None, madre=None):", puerta)
        via = F.fuente_funcion("_ot_puerta_via_cubierta")
        self.assertIn("_ot_fila_basica(vid)", via)
        self.assertNotIn('v.get("ot_madre_id")', via)
        self.assertNotIn('v["ot_madre_id"]', via)

    def test_los_documentos_espejados_quedan_fuera_de_saldo_libro_y_puerta(self):
        for nombre, veces in (("_ot_puerta_docs_de", 1), ("_ot_puerta_docs_lote", 1), ("_fac_aportes_ot", 1),
                              ("_fac_huella_ot", 1), ("_ot_saldo_usos", 2), ("_ot_saldo_docs_de_ot", 1),
                              ("_ot_fin_clasif_lote", 1)):
            self.assertGreaterEqual(F.fuente_funcion(nombre).count("heredado_de_visita_id IS NULL"), veces, nombre)
        self.assertIn("heredado_de_visita_id IS NULL", F.fuente_constante("_FAC_SQL_OT_CON_DOC"))

    def test_las_rutas_nuevas_y_sus_decoradores(self):
        escritura = ("ot_api_evento_crear", "ot_api_evento_editar", "ot_api_evento_asociar", "ot_api_evento_candidatas",
                     "ot_api_desinstalacion_info", "ot_api_desinstalacion_programar", "ot_api_desinstalacion_prevista")
        for f in escritura:
            decs = F.decoradores(f)
            self.assertIn("@_mant_required", decs, f)
            self.assertIn("@_no_tecnico", decs, f)
        # ver: cualquiera con Mantenciones (el técnico ve solo lo suyo, ya filtrado en la función)
        for f in ("ot_api_eventos_listar", "ot_api_evento_detalle", "ot_api_ot_evento"):
            self.assertIn("@_mant_required", F.decoradores(f), f)
            self.assertNotIn("@_no_tecnico", F.decoradores(f), f)
        # el técnico recibe sus eventos acotados y sin datos internos
        det = F.fuente_funcion("ot_api_evento_detalle")
        self.assertIn("_evento_ids_del_tecnico()", det)
        self.assertIn("con_internos=not es_tec", det)
        lista = F.fuente_funcion("ot_api_eventos_listar")
        self.assertIn("con_internos=not (es_tec or para_asistente)", lista)
        self.assertIn("if not es_tec:\n            cols += \" OR e.doc_ref LIKE %s\"", lista, "el técnico no busca por documento")

    def test_las_pantallas_de_eventos_vuelven_al_panel_si_falta_la_plantilla(self):
        self.assertIn("TemplateNotFound", F.fuente_funcion("ot_pagina_eventos"))
        self.assertIn("TemplateNotFound", F.fuente_funcion("ot_pagina_evento_detalle"))

    def test_bitacora_del_tecnico_con_las_acciones_nuevas_sin_plata(self):
        amb = F.cargar(constantes=("_ACT_TEC_PERMITIDAS",))
        for acc in ("evento_asignado", "evento_quitado", "desinstalacion_programada", "creada_como_desinstalacion"):
            self.assertIn(acc, amb["_ACT_TEC_PERMITIDAS"])
        # lo que NO se le muestra: la declaración con su argumento y la autorización
        for acc in ("desinstalacion_prevista", "desinstalacion_cubierta_autorizada", "desinstalacion_prevista_quitada"):
            self.assertNotIn(acc, amb["_ACT_TEC_PERMITIDAS"])
        # los detalles de lo que sí se ve no nombran dinero ni «cubierta»
        core = F.fuente_funcion("_ot2_crear_core")
        for m in re.finditer(r'_mant_log\("visita",[^\n]*"(creada_como_desinstalacion|desinstalacion_programada)",\s*([^\n]*)', core):
            self.assertNotRegex(m.group(2), r"cubiert|\$|monto|saldo|factura")

    def test_el_cron_de_recordatorios_no_le_escribe_al_cliente_por_la_hija(self):
        t = F.texto()
        i = t.index("# 3a — Visitas próximas: email al cliente")
        self.assertIn("AND v.ot_madre_id IS NULL", t[i:i + 3500])

    def test_el_barrido_cuelga_del_cron_y_solo_notifica_por_la_campana(self):
        self.assertIn('res["desinstalaciones"] = _desinst_barrido()', F.texto())
        barrido = F.fuente_funcion("_desinst_barrido") + F.fuente_funcion("_desinst_notificar")
        self.assertIn('"otro"', barrido)                      # tipo ya existente en el ENUM de la campana
        self.assertNotIn("_send_ilus_email", barrido)         # jamás correo
        self.assertNotIn("_mant_ot_email_cliente", barrido)

    def test_cada_tipo_de_aviso_usado_cabe_en_el_enum_de_la_campana(self):
        # no se agregaron tipos nuevos: todo va como 'otro'
        usados = set(re.findall(r'_mant_notificar\(\s*[^,()]+,\s*"([a-z_]+)"', F.fuente_funcion("_desinst_notificar")
                                + F.fuente_funcion("ot_aut_api_aprobar") + F.fuente_funcion("_ot2_crear_core")))
        self.assertEqual(usados - {"otro", "ot_asignada", "ot_pendiente_aprobacion", "ot_aprobada", "ot_rechazada"}, set())

    def test_el_panel_filtra_por_evento_con_parametros_y_lista_blanca(self):
        panel = F.fuente_funcion("ot2_panel")
        self.assertIn('f_evento = int(request.args.get("evento") or 0) or 0', panel)
        self.assertIn('extra_where_q.append("v.evento_id = %s")', panel)
        self.assertIn("f_evento=f_evento, eventos_opts=eventos_opts", panel)
        self.assertNotIn("v.evento_id = {", panel)
        self.assertIn("_ot2_panel_evento_chips(filas)", panel)
        self.assertIn("_ot2_panel_evento_chips(filas_kanban)", panel)
        self.assertIn("_ot2_panel_evento_chips(filas_cal)", panel)
        # no se tocó _OT2_SELECT_FILAS
        self.assertNotIn("evento_id", F.fuente_constante("_OT2_SELECT_FILAS"))

    def test_el_excel_agrega_evento_y_relacion_al_final(self):
        x = F.fuente_funcion("ot2_reporte_xlsx")
        self.assertIn('headers += ["Evento", "Relación (instalación ↔ desinstalación)"]', x)
        self.assertIn("vals += [f.get(\"evento_nombre\") or \"\", _rel_xl]", x)
        self.assertIn("ci == _COL_OBS and obs", x)

    def test_borrar_madre_y_lote(self):
        self.assertIn("_evt.guardia_borrar_madre(_ot_hijas_de(vid, estricto=True))", F.fuente_funcion("_mant_visita_eliminar_core"))
        self.assertIn("ot_madre_id IS NOT NULL", F.fuente_funcion("ot2_api_eliminar_lote"))

    def test_cancelar_madre_bloquea_la_fila_y_guarda_la_cascada(self):
        put = F.fuente_funcion("mant_visita_update")
        self.assertIn("SELECT id, tipo, ot_madre_id FROM mant_visitas WHERE id=%s FOR UPDATE", put)
        self.assertIn("_evt.guardia_cancelar_madre(", put)
        self.assertIn("incluir_desinstalacion", put)
        self.assertIn("HIJA_CUBIERTA_SIN_COBRO", put)
        self.assertIn("guardia_reagendar_madre", put)
        self.assertIn("guardia_cambio_tipo", put)

    def test_el_cierre_de_la_hija_exige_la_factura_de_la_madre(self):
        cierre = F.fuente_funcion("mant_ot_aprobar_cierre")
        self.assertIn('_exenta_doc_cliente = _mod_cobro in ("garantia", "sin_costo") or _cob_cierre in ("contrato", "cubierta")', cierre)
        self.assertEqual(cierre.count('"cubierta_madre"'), 2)
        self.assertIn('_cob_cierre in ("cobra", "cubierta")', cierre)
        self.assertIn("MADRE_SIN_FACTURA", F.fuente_constante("_OT_CIERRE_ACCIONES"))
        self.assertIn("madre_factura", F.fuente_constante("_OT_PUERTA_FALTA_A_CODIGO"))

    def test_la_perdida_se_ancla_en_la_instalacion(self):
        crear = F.fuente_funcion("ot_aut_api_crear")
        self.assertIn("_fam_ap = _fac_familia_de(visita_id)", crear)
        self.assertIn('visita_id = int(_fam_ap["madre_id"])', crear)
        info = F.fuente_funcion("_fac_perdida_de_ot")
        self.assertIn("fam = _fac_familia_de(vid)", info)
        self.assertIn("ancla = int(fam[\"madre_id\"]) if fam else int(vid)", info)

    def test_ficha_y_cabecera_no_dicen_sin_documento_en_la_cubierta(self):
        det = F.fuente_funcion("ot2_detalle")
        self.assertIn('elif _cob_hdr == "cubierta":', det)
        self.assertIn('"cubierta"', det)
        self.assertIn("desinst=desinst", det)
        self.assertIn("_ot_visible_para_rol(desinst[\"madre\"][\"id\"])", det)

    def test_las_pantallas_de_finanzas_traen_la_bandera_de_madre(self):
        for nombre in ("ot2_detalle", "ot2_api_finanzas", "ot_api_regularizar", "mant_ot_aprobar_cierre", "_fac_finanzas_de_ots"):
            self.assertIn("madre_valida", F.fuente_funcion(nombre).replace("MADRE_COLS", "madre_valida") if nombre != "mant_ot_aprobar_cierre"
                          else F.fuente_funcion(nombre), nombre)
        self.assertIn("_OT_FIN_SQL_MADRE_COLS", F.fuente_funcion("ot2_api_finanzas"))
        self.assertIn("madre_valida", F.fuente_funcion("_ot_fin_cols_sql"))


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
class TestDocumentoPropioDeLaHija(unittest.TestCase):
    """Revisión 19-oct: el documento ESPEJADO de la instalación no puede impedir que la desinstalación agregue, como suyo,
    el mismo documento cuando la nota de venta trae una línea propia de desinstalación (la salida que da HIJA_CUBIERTA_SIN_COBRO)."""

    def test_agregar_documento_no_toma_el_espejo_por_ya_asociado(self):
        src = F.fuente_funcion("ot2_api_documentos_agregar")
        self.assertIn(" WHERE visita_id=%s AND erp_tido=%s AND erp_nudo=%s AND heredado_de_visita_id IS NULL", src)
        self.assertIn("WHERE visita_id=%s AND origen='erp' AND heredado_de_visita_id IS NULL", src, "ni el aviso de duplicado")
        self.assertIn("documento_heredado_reemplazado", src, "el reemplazo del espejo deja constancia en la bitácora")
        # el espejo se retira ANTES de insertar el documento propio
        self.assertLess(src.index("documento_heredado_reemplazado"),
                        src.index('"   etiqueta, monto, rut, rut_ok, rut_justif, emitido_el, asociado_por, "'))

    def test_el_documento_heredado_no_se_quita_desde_la_hija(self):
        src = F.fuente_funcion("ot2_api_documentos_quitar")
        self.assertIn("DOC_HEREDADO", src)
        self.assertLess(src.index("DOC_HEREDADO"), src.index("DELETE FROM mant_visita_documentos"))

    def test_la_ficha_y_el_motor_rotulan_el_heredado_y_no_lo_cuentan(self):
        self.assertIn('"heredado": bool(f.get("heredado_de_visita_id"))', F.fuente_funcion("_ot_docs_listar"))
        pano = F.fuente_funcion("_ot_panorama_docs")
        self.assertIn('cuenta = "heredado"', pano)
        api = F.fuente_funcion("ot_api_panorama")
        self.assertIn('cont["heredados"]', api)
        self.assertIn('if d.get("heredado"):', api)


class TestOfertaDeMantencionNoVaALaInstalacionTemporal(unittest.TestCase):
    def test_el_barrido_de_ofertas_excluye_instalaciones_con_desinstalacion(self):
        src = F.fuente_funcion("_prospectos_oferta_barrido")
        self.assertIn("AND v.desinstalacion_prevista IS NULL AND v.ot_madre_id IS NULL", src)
        # y el interruptor de siempre sigue mandando (apagado por defecto)
        self.assertIn('os.environ.get("PROSPECTOS_OFERTA_AUTO", "0")', src)


class TestPrevioDelAsistente(unittest.TestCase):
    """POST /ot/api/desinstalacion/previo: lo mismo que el servidor va a exigir al crear, sin escribir nada."""
    BODY = {"cliente_id": 5, "fecha_programada": "2026-10-26", "hora_fin": "18:00",
            "documentos": [{"tipo": "NVV", "numero": "VD10961"}], "pedido_madre": {"servicio": 100000, "despacho": 0},
            "desinstalacion": {"lleva": True, "fecha": "2026-11-26", "hora_inicio": "09:00", "hora_fin": "13:00",
                               "tecnico_user_ids": [3], "tecnico_lider_id": 3, "costo_proveedor": 0}}
    VACIO = {"servicio": 0, "despacho": 0, "leido": True, "por_doc": []}

    def llamar(self, cambios=None, rem=None, choque=None, feriados=None, evento=None, documentos=None, superadmin=False,
               equipos=None):
        import copy
        import types
        body = copy.deepcopy(self.BODY)
        if equipos:
            body["equipos"] = equipos
        for k, v in (cambios or {}).items():
            body["desinstalacion"][k] = v
        if documentos is not None:
            body["documentos"] = documentos
        if evento:
            body["evento_id"] = evento["id"]
        a = F.cargar(["ot_api_desinstalacion_previo", "_ot_desinst_normalizar", "_ot_desinst_validar_previo"],
                     extra={"_evt": E, "_saldo": S, "re": re, "print": lambda *x, **k: None})
        a["request"] = types.SimpleNamespace(get_json=lambda silent=True: body)
        a["jsonify"] = lambda d: d
        a["mysql_fetchone"] = lambda sql, p=None: {"rut": "76.111.111-1"}
        a["_evento_fila"] = lambda eid: evento
        a["_feriados_cl"] = lambda *f: dict(feriados or {})
        a["_now_chile"] = lambda: dt.datetime(2026, 10, 19, 12, 0)
        a["_ot_desinst_remanente_saldo"] = lambda docs, ped=None, rut=None, ex=None: dict(rem or self.VACIO)
        a["_ot_validar_tecnicos_una_empresa"] = lambda ids: None
        a["_ot_validar_tope_ot_sin_contrato"] = lambda ids, extra=0: None
        a["_validar_disponibilidad_visita"] = lambda *x, **k: ({"choque": choque} if choque else {})
        a["_ot_aut_es_superadmin"] = lambda: superadmin
        a["_ot_desinst_sugerir_checklists"] = lambda items: [
            {"maquina_id": m, "sku": s, "plantilla_id": 50, "plantilla_nombre": "Desinstalación y embalaje", "origen": "respaldo"}
            for m, s in items]
        return a["ot_api_desinstalacion_previo"]()

    def test_sin_desinstalacion_no_pregunta_nada(self):
        r = self.llamar({"lleva": False})
        self.assertTrue(r["valido"])
        self.assertEqual(r["lleva"], "no")
        self.assertIsNone(r["remanente"])

    def test_sin_saldo_que_sobre_queda_cubierta_sin_preguntar(self):
        r = self.llamar()
        self.assertTrue(r["valido"])
        self.assertEqual(r["cobro"], "cubierta")
        self.assertEqual(r["cobro_txt"], "Cubierta por esta instalación: no consume saldo aparte")
        self.assertFalse(r["elige_cobro"])
        self.assertIsNone(r["pregunta"])
        self.assertTrue(r["tiene_documento_real"])
        self.assertFalse(r["sin_documento_madre"])

    def test_si_queda_saldo_la_eleccion_es_una_pregunta_no_un_error(self):
        rem = {"servicio": 120000, "despacho": 0, "leido": True, "por_doc": []}
        r = self.llamar(rem=rem)
        self.assertFalse(r["valido"])
        self.assertEqual(r["pregunta"], "cobro")
        self.assertIsNone(r["bloqueo"])
        self.assertTrue(r["elige_cobro"])
        self.assertEqual(r["remanente"]["servicio"], 120000)
        # elegir «cubierta» con saldo libre exige el argumento
        r2 = self.llamar({"cobro": "cubierta"}, rem=rem)
        self.assertEqual(r2["bloqueo"]["codigo"], "ARGUMENTO_CUBIERTA")
        r3 = self.llamar({"cobro": "cubierta", "argumento_cubierta": "el mall absorbe la desinstalación de la trotadora"}, rem=rem)
        self.assertTrue(r3["valido"])
        # elegir «se cobra» no puede pasar del saldo que calcula el servidor
        r4 = self.llamar({"cobro": "cobra", "cobra_servicio": 150000}, rem=rem)
        self.assertEqual(r4["bloqueo"]["codigo"], "ZZ_SALDO_CONSUMIDO")
        r5 = self.llamar({"cobro": "cobra", "cobra_servicio": 100000}, rem=rem)
        self.assertTrue(r5["valido"])
        self.assertEqual(r5["cobro_txt"], "Se cobra su línea de desinstalación")

    def test_domingo_bloquea_y_sabado_o_feriado_solo_avisan(self):
        r = self.llamar({"fecha": "2026-11-29"})
        self.assertFalse(r["valido"])
        self.assertEqual(r["bloqueo"]["codigo"], "DOMINGO_BLOQUEADO")
        r2 = self.llamar({"fecha": "2026-11-28"})
        self.assertTrue(r2["valido"])
        self.assertTrue(r2["avisos"])
        r3 = self.llamar({"fecha": "2026-11-27"}, feriados={"2026-11-27": "Feriado de prueba"})
        self.assertTrue(r3["valido"])
        self.assertTrue(any("feriado" in a.lower() for a in r3["avisos"]))

    def test_antes_de_la_instalacion_bloquea(self):
        r = self.llamar({"fecha": "2026-10-20"})
        self.assertFalse(r["valido"])
        self.assertEqual(r["bloqueo"]["codigo"], "DESINST_ANTES_DE_INSTALACION")

    def test_sin_documento_de_random_lo_dice_para_que_el_asistente_ofrezca_la_autorizacion(self):
        r = self.llamar(documentos=[])
        self.assertTrue(r["valido"])
        self.assertFalse(r["tiene_documento_real"])
        self.assertTrue(r["sin_documento_madre"])

    def test_el_choque_del_tecnico_viaja_con_la_posibilidad_de_forzar(self):
        ch = {"tecnico_nombre": "Lenin", "visitas": [{"numero_ot": "OT-1", "hora_inicio": "09:00", "hora_fin": "12:00"}]}
        r = self.llamar(choque=ch, superadmin=True)
        self.assertEqual(r["choque"]["tecnico_nombre"], "Lenin")
        self.assertTrue(r["choque"]["puede_forzar"])
        self.assertFalse(self.llamar(choque=ch)["choque"]["puede_forzar"])

    def test_la_fecha_sugerida_pasa_al_siguiente_dia_habil_si_el_evento_termina_en_domingo(self):
        ev = {"id": 7, "nombre": "Feria", "estado": "activo", "fecha_ini": D(2026, 10, 26), "fecha_fin": D(2026, 11, 29)}
        r = self.llamar(evento=ev, cambios={"lleva": False})
        self.assertEqual(r["fecha_sugerida"], "2026-11-30")

    def test_muestra_el_checklist_que_se_le_pondria_a_cada_equipo(self):
        # «el sistema SUGIERE, la persona DECIDE»: antes de crear se ve qué checklist lleva cada equipo
        r = self.llamar(equipos=[{"maquina_id": 4, "sku": "SD45"}, {"maquina_id": "x"}, {"maquina_id": 7}])
        self.assertEqual([(c["maquina_id"], c["sku"], c["plantilla_id"]) for c in r["checklists_sugeridos"]],
                         [(4, "SD45", 50), (7, None, 50)])
        self.assertEqual(self.llamar()["checklists_sugeridos"], [])
        self.assertEqual(self.llamar({"lleva": False}, equipos=[{"maquina_id": 4}])["checklists_sugeridos"], [])

    def test_la_ficha_muestra_lo_que_se_va_a_crear(self):
        src = F.fuente_funcion("ot_api_desinstalacion_info")
        for clave in ('out["equipos"]', "plantilla_sugerida_nombre", 'out["manuales"]', 'out["hereda"]', "_ot_desinst_sugerir_checklists("):
            self.assertIn(clave, src)

    def test_la_ruta_es_solo_para_gestion_y_no_escribe(self):
        self.assertEqual(F.decoradores("ot_api_desinstalacion_previo"),
                         ["@app.route(\"/ot/api/desinstalacion/previo\", methods=[\"POST\"])", "@_mant_required", "@_no_tecnico"])
        src = F.fuente_funcion("ot_api_desinstalacion_previo")
        for prohibido in ("mysql_execute", "INSERT", "UPDATE", "DELETE", "_mant_log"):
            self.assertNotIn(prohibido, src)


class TestChecklistSugeridoYElegido(unittest.TestCase):
    """Daniel (19-oct): «el sistema SUGIERE (técnico, checklist, fecha) y la persona DECIDE; lo que se ve y elige es exactamente
    lo que se crea; nada se agrega por detrás»."""

    def test_normalizar_los_equipos_elegidos(self):
        a = F.cargar(["_ot_desinst_normalizar"], extra={"_evt": E, "_saldo": S})
        err, dd = a["_ot_desinst_normalizar"]({"lleva": True, "equipos": [
            {"maquina_id": 4, "plantilla_id": 9}, {"maquina_id": 4, "plantilla_id": 1}, {"maquina_id": "7"}, "basura"]})
        self.assertIsNone(err)
        self.assertEqual(dd["equipos"], [{"maquina_id": 4, "plantilla_id": 9}, {"maquina_id": 7, "plantilla_id": None}])
        self.assertEqual(a["_ot_desinst_normalizar"]({"lleva": True, "equipos": [{"maquina_id": "x"}]})[0]["codigo"], "EQUIPO_INVALIDO")
        self.assertEqual(a["_ot_desinst_normalizar"]({"lleva": True, "equipos": [{"maquina_id": 1, "plantilla_id": "abc"}]})[0]["codigo"],
                         "PLANTILLA_INVALIDA")
        self.assertEqual(a["_ot_desinst_normalizar"]({"lleva": True})[1]["equipos"], [])

    def test_la_sugerencia_dice_de_donde_sale_el_checklist(self):
        a = F.cargar(["_ot_desinst_sugerir_checklists"], extra={"print": lambda *x, **k: None})
        a["_ot_plantilla_desinstalacion_fallback"] = lambda: 50
        a["_ot_plantilla_para_hija"] = lambda sku, fb: {"SD45": 77}.get(sku, fb)
        a["mysql_fetchall"] = lambda sql, p=None: [{"id": 77, "nombre": "Bloque trotadora"}, {"id": 50, "nombre": "Desinstalación y embalaje"}]
        r = a["_ot_desinst_sugerir_checklists"]([(1, "SD45"), (2, "XX"), (3, None)])
        self.assertEqual([(x["origen"], x["plantilla_id"], x["plantilla_nombre"]) for x in r],
                         [("clasificacion", 77, "Bloque trotadora"), ("respaldo", 50, "Desinstalación y embalaje"),
                          ("respaldo", 50, "Desinstalación y embalaje")])
        a["_ot_plantilla_desinstalacion_fallback"] = lambda: None
        a["_ot_plantilla_para_hija"] = lambda sku, fb: None
        self.assertEqual(a["_ot_desinst_sugerir_checklists"]([(1, "X")])[0]["origen"], "sin_checklist")

    # ── el payload que se le pasa al núcleo de creación ──
    MADRE = {"id": 10, "numero_ot": "OT-2026-00350", "tipo": "instalacion", "estado": "programada", "cliente_id": 5,
             "ot_madre_id": None, "desinstalacion_prevista": D(2026, 11, 26), "centro_costo": "sstt",
             "contacto_nombre": "Ana", "contacto_cargo": "Adm", "contacto_tel": "+56911111111", "contacto_email": "a@x.cl",
             "contacto_rut": None, "contacto_origen": "manual", "direccion_visita": "Av. X 123", "direccion_detalle": "Local 4",
             "direccion_lat": -33.4, "direccion_lng": -70.6, "direccion_place_id": "pid", "direccion_comuna": "Maipú",
             "direccion_region": "RM", "acceso_ascensor": 1, "acceso_estacionamiento": 0, "acceso_piso": 2,
             "acceso_notas": "Entrar por bodega"}
    BLOQUE = {"lleva": True, "fecha": "2026-11-26", "hora_inicio": "09:00", "hora_fin": "13:00", "tecnico_user_ids": [3, 8],
              "tecnico_lider_id": 3, "costo_proveedor": 0, "costo_despacho": 15000, "cobro": "cubierta", "nota": "Embalar"}

    def amb(self):
        import types
        cap = {"llamadas": 0}
        a = F.cargar(["_ot_crear_desinstalacion", "_ot_desinst_normalizar"], constantes=("_DESINST_CAMPOS_CONTRAPARTE",),
                     extra={"_evt": E, "_saldo": S, "print": lambda *x, **k: None})
        a["_ot_madre_datos_heredables"] = lambda vid: {
            "fila": dict(self.MADRE), "maquinas": [4, 7], "skus": {4: "SD45", 7: "XX"},
            "manuales": [{"titulo": "Cinta de correr extra", "cantidad": 1, "sku": "C1"}]}
        a["_ot_hijas_de"] = lambda mid, cur=None, estricto=False: []
        a["_ot_plantilla_desinstalacion_fallback"] = lambda: 50
        a["_ot_plantilla_para_hija"] = lambda sku, fb: 77 if sku == "SD45" else fb
        a["_chk_plantillas_validas"] = lambda ids, tipo: {int(i) for i in ids} - {999}
        a["_ot_saldo_docs_de_ot"] = lambda vid: [("NVV", "VD00010961")]

        def _core(d, autorizacion=None, madre=None):
            cap["llamadas"] += 1
            cap["d"], cap["madre"], cap["autorizacion"] = d, madre, autorizacion
            return types.SimpleNamespace(get_json=lambda silent=True: {
                "ok": True, "visita_id": 351, "numero_ot": "OT-2026-00351", "avisos": [], "ot_url": "/ot/351"}), 200
        a["_ot2_crear_core"] = _core
        return a, cap

    def test_sin_elegir_equipos_se_desinstalan_todos_con_el_checklist_sugerido(self):
        a, cap = self.amb()
        r = a["_ot_crear_desinstalacion"](10, dict(self.BLOQUE))
        self.assertTrue(r["ok"], r)
        self.assertEqual((r["numero_ot"], r["fecha_txt"], r["cobro"]), ("OT-2026-00351", "26/11/2026", "cubierta"))
        d = cap["d"]
        self.assertEqual(d["equipos"], [{"maquina_id": 4, "plantilla_id": 77}, {"maquina_id": 7, "plantilla_id": 50}])
        self.assertEqual(d["tareas_manuales"][0]["plantilla_id"], 50)
        self.assertEqual((d["tecnico_user_ids"], d["tecnico_lider_id"]), ([3, 8], 3))
        self.assertEqual(d["tipo_ot"], "desinstalacion")
        self.assertEqual(d["_desinst_cobro"], "cubierta")
        self.assertEqual((d["finanzas"]["centro_costo"], d["finanzas"]["costo_proveedor"], d["finanzas"]["costo_despacho"]),
                         ("sstt", 0.0, 15000.0))
        # hereda la dirección, la contraparte y el acceso de la instalación
        self.assertEqual(d["contraparte"]["direccion"], "Av. X 123")
        self.assertEqual((d["contraparte"]["nombre"], d["contraparte"]["place_id"]), ("Ana", "pid"))
        self.assertEqual(d["acceso"], {"ascensor": True, "estacionamiento": False, "piso": 2, "notas": "Entrar por bodega"})
        self.assertEqual(cap["madre"]["id"], 10)                       # la madre la pasa el servidor, nunca el body
        for k in ("ot_madre_id", "ot_relacion", "evento_id", "ticket_id", "autorizacion_id"):
            self.assertNotIn(k, d)

    def test_la_persona_decide_que_equipos_y_que_checklist(self):
        a, cap = self.amb()
        r = a["_ot_crear_desinstalacion"](10, dict(self.BLOQUE, equipos=[{"maquina_id": 7, "plantilla_id": 61}]))
        self.assertTrue(r["ok"], r)
        self.assertEqual(cap["d"]["equipos"], [{"maquina_id": 7, "plantilla_id": 61}])     # el equipo 4 queda fuera: lo decidió ella
        # un checklist vacío toma el sugerido
        a2, cap2 = self.amb()
        a2["_ot_crear_desinstalacion"](10, dict(self.BLOQUE, equipos=[{"maquina_id": 4}]))
        self.assertEqual(cap2["d"]["equipos"], [{"maquina_id": 4, "plantilla_id": 77}])

    def test_no_se_crea_nada_si_el_equipo_o_el_checklist_elegidos_no_sirven(self):
        a, cap = self.amb()
        r = a["_ot_crear_desinstalacion"](10, dict(self.BLOQUE, equipos=[{"maquina_id": 99}]))
        self.assertEqual((r["ok"], r["codigo"]), (False, "EQUIPO_NO_ES_DE_LA_INSTALACION"))
        r = a["_ot_crear_desinstalacion"](10, dict(self.BLOQUE, equipos=[{"maquina_id": 4, "plantilla_id": 999}]))
        self.assertEqual((r["ok"], r["codigo"]), (False, "PLANTILLA_INVALIDA"))
        self.assertEqual(cap["llamadas"], 0)

    def test_si_la_nota_trae_su_linea_la_hija_cobra_y_una_aprobacion_remota_no_hereda_el_choque(self):
        a, cap = self.amb()
        r = a["_ot_crear_desinstalacion"](10, dict(self.BLOQUE, cobro="cobra", cobra_servicio=50000, forzar_choque=True),
                                          autorizacion={"id": 7}, creado_por="Aaron")
        self.assertTrue(r["ok"], r)
        d = cap["d"]
        self.assertEqual(d["_desinst_cobro"], "cobra")
        self.assertEqual((d["finanzas"]["zz_monto"], d["finanzas"]["valor_origen"], d["finanzas"]["factura_tido"]), (50000, "zz", "NVV"))
        self.assertFalse(d["forzar_choque"])
        self.assertEqual(d["_creado_por"], "Aaron")


class TestPrefijosEnApp(unittest.TestCase):
    def test_saldo_real_entiende_bd(self):
        amb = F.cargar(["_ot_saldo_real"], extra={"_saldo": S})
        self.assertEqual(amb["_ot_saldo_real"](("BD", "10961")), ("NVV", "BD00010961"))
        self.assertEqual(amb["_ot_saldo_real"](("VD", "10667")), ("NVV", "VD00010667"))
        self.assertEqual(amb["_ot_saldo_real"](("FCV", "11439")), ("FCV", "0000011439"))


if __name__ == "__main__":
    unittest.main()

# -*- coding: utf-8 -*-
"""SOLICITUD DE REPUESTO: dueño al nacer, reloj de la primera respuesta, avisos y tiempos (Daniel, 21-oct-2026 · REGLA #41).

Daniel: «que sea práctico, ordenado, guiado, que tenga trayectoria, que alerte a los técnicos y también a los administradores, y que los
obligues a tomar una respuesta midiendo los tiempos de respuesta, algo parecido a Retiros».

Qué se prueba (todo con datos y correo de MENTIRA — REGLA #22: jamás un correo real ni un cliente real):
  A. El módulo puro `repuestos_respuesta.py`: horas hábiles (fin de semana, feriado, fuera de jornada), cobertura, estado del reloj (en plazo,
     por vencer, escala, vencida, respondida; con el equipo fuera de servicio), primera respuesta (bitácora y respaldo por columnas, pedido
     reabierto), niveles de aviso sin repetir, dueño, textos al técnico (sin proveedor ni plata) y resumen de tiempos.
  B. Las funciones REALES de app.py (extraídas con `tests/_fuente_codigo.py`, sin importar app.py) contra una base de mentira: dueño al crear,
     avisos agrupados por OT, avisos al técnico, el barrido (una sola vez por pedido y nivel, solo en cobertura, escala a supervisión), los
     correos nacen apagados, y el endpoint de tiempos.
  C. El cableado: el hook en el evento, el cron, el ENUM de la campana (las dos listas), el registro de correos (REGLA #33) y la confidencialidad.

Correr con:  python -m pytest tests/test_repuestos_respuesta.py -q
"""
import ast
import json
import re
import time
import types
from datetime import date, datetime, timedelta, timezone

import pytest

import comm_correos_registro as R
import repuestos_respuesta as RR
from tests import _fuente_codigo as FC

try:
    from zoneinfo import ZoneInfo
    _CL, _UTC = ZoneInfo("America/Santiago"), ZoneInfo("UTC")
except Exception:                                                             # pragma: no cover
    _CL = _UTC = None

# Fechas de referencia (2026): lunes 19-oct, viernes 16-oct, sábado 17-oct; feriados: jueves y viernes 17/18-sep no (el 18-sep es viernes), 12-oct lunes.
LUN = datetime(2026, 10, 19)
VIE = datetime(2026, 10, 16)
FER = {"2026-09-18", "2026-09-19", "2026-10-12"}


def dt(d, h=0, m=0):
    return datetime(d.year, d.month, d.day, h, m)


def a_utc(chile_naive):
    """Hora Chile (sin zona) → UTC (sin zona), como se guarda en la base."""
    return chile_naive.replace(tzinfo=_CL).astimezone(_UTC).replace(tzinfo=None)


# ═══════════════════════════════════════════════════════════════════════════════════════════════
#  A. EL MÓDULO PURO
# ═══════════════════════════════════════════════════════════════════════════════════════════════
class TestHorasHabiles:
    def test_el_mismo_dia_cuenta_solo_la_jornada(self):
        assert RR.horas_habiles(dt(LUN, 8), dt(LUN, 12), FER) == 4
        assert RR.horas_habiles(dt(LUN, 7), dt(LUN, 9), FER) == 1          # lo anterior a las 08:00 no cuenta
        assert RR.horas_habiles(dt(LUN, 16), dt(LUN, 23), FER) == 1        # ni lo posterior a las 17:00
        assert RR.horas_habiles(dt(LUN, 18), dt(LUN, 22), FER) == 0

    def test_un_dia_hábil_completo_son_9_horas(self):
        assert RR.horas_habiles(dt(LUN, 0), dt(LUN, 23, 59), FER) == 9
        assert RR.horas_habiles(dt(LUN, 8), dt(LUN + timedelta(days=1), 8), FER) == 9

    def test_el_fin_de_semana_no_corre(self):
        # viernes 16:00 → lunes 09:00 = 1 h del viernes + 1 h del lunes
        assert RR.horas_habiles(dt(VIE, 16), dt(LUN, 9), FER) == 2
        assert RR.horas_habiles(dt(date(2026, 10, 17), 9), dt(date(2026, 10, 18), 16), FER) == 0     # sábado y domingo

    def test_un_feriado_no_corre(self):
        # jueves 17-sep 16:00 → lunes 21-sep 09:00: viernes 18 (feriado), sábado 19 (feriado) y domingo no cuentan
        assert RR.horas_habiles(datetime(2026, 9, 17, 16), datetime(2026, 9, 21, 9), FER) == 2
        # lunes 12-oct es feriado: viernes 9-oct 16:00 → martes 13-oct 09:00 = 1 h + 1 h
        assert RR.horas_habiles(datetime(2026, 10, 9, 16), datetime(2026, 10, 13, 9), FER) == 2

    def test_usa_cl_feriados_por_defecto(self):
        # 18-sep-2026 es feriado de verdad en cl_feriados
        assert RR.horas_habiles(datetime(2026, 9, 17, 16), datetime(2026, 9, 21, 9)) == 2
        assert RR.es_dia_habil(date(2026, 9, 18)) is False
        assert RR.es_dia_habil(date(2026, 10, 19)) is True

    def test_acepta_una_funcion_anio_a_set(self):
        assert RR.es_dia_habil(date(2026, 10, 12), lambda anio: {"2026-10-12"}) is False

    def test_nunca_negativo(self):
        assert RR.horas_habiles(dt(LUN, 12), dt(LUN, 9), FER) == 0
        assert RR.horas_habiles(None, dt(LUN, 9), FER) == 0

    def test_sumar_horas_habiles_salta_la_noche_y_el_fin_de_semana(self):
        assert RR.sumar_horas_habiles(dt(LUN, 9), 4, FER) == dt(LUN, 13)
        assert RR.sumar_horas_habiles(dt(VIE, 16), 2, FER) == dt(LUN, 9)           # 1 h del viernes y 1 h del lunes
        assert RR.sumar_horas_habiles(dt(LUN, 8), 9, FER) == dt(LUN, 17)
        assert RR.sumar_horas_habiles(dt(LUN, 8), 9.5, FER) == dt(LUN + timedelta(days=1), 8, 30)

    def test_horas_y_suma_son_inversas(self):
        ini = dt(VIE, 15, 30)
        for h in (0.5, 3, 9, 20):
            assert abs(RR.horas_habiles(ini, RR.sumar_horas_habiles(ini, h, FER), FER) - h) < 1e-9

    def test_formato_de_horas(self):
        assert RR.fmt_horas(0.25) == "15 min"
        assert RR.fmt_horas(3.5) == "3,5 h hábiles"
        assert RR.fmt_horas(9) == "1 día hábil"
        assert RR.fmt_horas(13.5) == "1,5 días hábiles"


class TestCobertura:
    def test_abierta_en_horario(self):
        assert RR.en_cobertura(dt(LUN, 10), FER, {})["abierta"] is True

    def test_cerrada_fuera_de_horario(self):
        for h, m, motivo in ((7, 30, "todavía"), (13, 30, "colación"), (17, 0, "terminó"), (22, 0, "terminó")):
            r = RR.en_cobertura(dt(LUN, h, m), FER, {})
            assert r["abierta"] is False and motivo in r["motivo"], (h, m, r)

    def test_cerrada_fin_de_semana_y_feriado(self):
        assert RR.en_cobertura(dt(date(2026, 10, 17), 10), FER, {})["motivo"] == "día no hábil"
        assert RR.en_cobertura(datetime(2026, 10, 12, 10), FER, {})["abierta"] is False

    def test_lee_las_variables_de_cobertura_de_retiros(self):
        env = {"RETIROS_COBERTURA_DESDE": "09:00", "RETIROS_COBERTURA_HASTA": "18:00",
               "RETIROS_COBERTURA_COLACION_DESDE": "14:00", "RETIROS_COBERTURA_COLACION_HASTA": "15:00"}
        assert RR.jornada(env) == (540, 1080, 840, 900)
        assert RR.en_cobertura(dt(LUN, 8, 30), FER, env)["abierta"] is False
        assert RR.en_cobertura(dt(LUN, 13, 30), FER, env)["abierta"] is True
        assert RR.en_cobertura(dt(LUN, 14, 30), FER, env)["abierta"] is False

    def test_una_variable_rota_cae_al_valor_por_defecto(self):
        assert RR.jornada({"RETIROS_COBERTURA_DESDE": "mañana"}) == (480, 1020, 780, 840)
        assert RR.jornada({"RETIROS_COBERTURA_DESDE": "18:00", "RETIROS_COBERTURA_HASTA": "09:00"})[:2] == (480, 1020)


class TestConfig:
    def test_valores_por_defecto(self):
        c = RR.config({})
        assert (c["plazo_h"], c["plazo_fs_h"], c["aviso_h"], c["escala_h"]) == (9.0, 4.0, 4.0, 8.0)

    def test_variables_de_entorno(self):
        c = RR.config({"REPUESTOS_AVISO_H": "2", "REPUESTOS_ESCALA_H": "6,5"})
        assert (c["aviso_h"], c["escala_h"]) == (2.0, 6.5)

    def test_una_escala_menor_que_el_aviso_se_corrige(self):
        c = RR.config({"REPUESTOS_AVISO_H": "6", "REPUESTOS_ESCALA_H": "3"})
        assert c["escala_h"] == 12.0

    def test_basura_cae_al_defecto(self):
        c = RR.config({"REPUESTOS_AVISO_H": "abc", "REPUESTOS_ESCALA_H": "-4"})
        assert (c["aviso_h"], c["escala_h"]) == (4.0, 8.0)

    def test_equipo_fuera_de_servicio_acorta_todo(self):
        assert RR.umbrales(True, RR.config({})) == (4.0, 2.0, 4.0)
        assert RR.umbrales(False, RR.config({})) == (9.0, 4.0, 8.0)

    def test_los_plazos_son_los_mismos_de_la_cola_de_gestion(self):
        """Una sola definición del plazo: el módulo y la cola (`_OTREP_PLAZO_H`, `_OTREP_JORNADA` de app.py) no pueden discrepar."""
        plazos = FC.valor_literal("app.py", "_OTREP_PLAZO_H")
        assert plazos["gestionar"] == RR.PLAZO_PRIMERA_H and plazos["gestionar_fs"] == RR.PLAZO_PRIMERA_FS_H
        assert FC.valor_literal("app.py", "_OTREP_JORNADA") == (RR.jornada({})[0] // 60, RR.jornada({})[1] // 60)


class TestEstadoReloj:
    def reloj(self, horas, fs=False, resp=None, inicio=None):
        ini = inicio or dt(LUN, 8)
        return RR.estado_reloj(ini, RR.sumar_horas_habiles(ini, horas, FER), resp, fs, FER, RR.config({}))

    def test_en_plazo(self):
        r = self.reloj(2)
        assert (r["estado"], r["nivel"]) == ("en_plazo", 0)

    def test_por_vencer_a_las_4_horas(self):
        r = self.reloj(4)
        assert (r["estado"], r["nivel"]) == ("por_vencer", 1)
        assert self.reloj(3.99)["nivel"] == 0

    def test_escala_a_las_8_horas_sin_estar_aun_vencida(self):
        r = self.reloj(8)
        assert (r["estado"], r["nivel"]) == ("por_vencer", 2)

    def test_vencida_al_cumplirse_el_dia_habil(self):
        r = self.reloj(9)
        assert (r["estado"], r["nivel"]) == ("vencida", 2)
        assert r["vence"] == dt(LUN, 17)

    def test_fuera_de_servicio_acorta_el_plazo(self):
        assert self.reloj(2, fs=True)["nivel"] == 1                # aviso a la mitad del plazo
        r = self.reloj(4, fs=True)
        assert (r["estado"], r["nivel"]) == ("vencida", 2)

    def test_el_fin_de_semana_no_cuenta(self):
        # pedido del viernes 16:00; el lunes a las 09:00 lleva 2 h hábiles
        r = RR.estado_reloj(dt(VIE, 16), dt(LUN, 9), None, False, FER, RR.config({}))
        assert r["transcurrido_h"] == 2 and r["estado"] == "en_plazo"

    def test_respondida_en_plazo(self):
        r = RR.estado_reloj(dt(LUN, 8), dt(LUN, 17), dt(LUN, 10), False, FER, RR.config({}))
        assert r["estado"] == "respondida" and r["nivel"] == 0
        assert r["tiempo_respuesta_h"] == 2 and r["en_plazo"] is True

    def test_respondida_fuera_de_plazo(self):
        r = RR.estado_reloj(dt(LUN, 8), dt(LUN + timedelta(days=3), 17), dt(LUN + timedelta(days=1), 12), False, FER, RR.config({}))
        assert r["estado"] == "respondida" and r["en_plazo"] is False and r["tiempo_respuesta_h"] == 13

    def test_una_respuesta_justo_en_el_plazo_cuenta_como_en_plazo(self):
        r = RR.estado_reloj(dt(LUN, 8), dt(LUN, 17), dt(LUN, 17), False, FER, RR.config({}))
        assert r["en_plazo"] is True

    def test_un_pedido_creado_de_noche_parte_a_la_mañana(self):
        r = RR.estado_reloj(dt(LUN, 20), dt(LUN + timedelta(days=1), 9), None, False, FER, RR.config({}))
        assert r["transcurrido_h"] == 1


def ev(accion, de, a, cuando, usuario="Ana", detalle=None):
    return {"accion": accion, "de_estado": de, "a_estado": a, "usuario": usuario, "created_at": cuando, "detalle": detalle}


class TestPrimeraRespuesta:
    def test_es_el_primer_cambio_que_sale_de_solicitado(self):
        evs = [ev("creada", None, "solicitado", dt(LUN, 8)),
               ev("estado", "solicitado", "validado", dt(LUN, 10), "Bea"),
               ev("estado", "validado", "pedido", dt(LUN, 11), "Cris")]
        r = RR.primera_respuesta(evs)
        assert (r["cuando"], r["quien"], r["a_estado"], r["fuente"]) == (dt(LUN, 10), "Bea", "validado", "evento")

    def test_un_rechazo_directo_tambien_es_respuesta(self):
        r = RR.primera_respuesta([ev("estado", "solicitado", "rechazado", dt(LUN, 9), "Dan")])
        assert r["a_estado"] == "rechazado" and r["quien"] == "Dan"

    def test_sin_orden_toma_la_mas_antigua(self):
        evs = [ev("estado", "solicitado", "pedido", dt(LUN, 12)), ev("estado", "solicitado", "validado", dt(LUN, 9))]
        assert RR.primera_respuesta(evs)["cuando"] == dt(LUN, 9)

    def test_lo_que_no_sale_de_solicitado_no_cuenta(self):
        evs = [ev("estado", "validado", "pedido", dt(LUN, 9)), ev("tomar", None, None, dt(LUN, 9)), ev("plazo", None, None, dt(LUN, 9))]
        assert RR.primera_respuesta(evs) is None

    def test_sin_bitacora_se_deduce_de_las_columnas(self):
        fila = {"estado": "validado", "validado_at": dt(LUN, 10), "validado_por": "Eva", "updated_at": dt(LUN, 12)}
        r = RR.primera_respuesta([], fila=fila)
        assert (r["cuando"], r["quien"], r["fuente"]) == (dt(LUN, 10), "Eva", "columna")

    def test_un_pedido_pendiente_sin_bitacora_no_tiene_respuesta(self):
        assert RR.primera_respuesta([], fila={"estado": "solicitado", "validado_at": None}) is None

    def test_rechazado_sin_bitacora_usa_la_ultima_actualizacion(self):
        r = RR.primera_respuesta([], fila={"estado": "rechazado", "resuelto_por": "Fer", "updated_at": dt(LUN, 15)})
        assert (r["cuando"], r["quien"], r["a_estado"]) == (dt(LUN, 15), "Fer", "rechazado")

    def test_la_bitacora_manda_sobre_las_columnas(self):
        r = RR.primera_respuesta([ev("estado", "solicitado", "validado", dt(LUN, 9))],
                                 fila={"estado": "pedido", "validado_at": dt(LUN, 11), "pedido_at": dt(LUN, 12)})
        assert r["cuando"] == dt(LUN, 9) and r["fuente"] == "evento"


class TestReapertura:
    def test_el_reloj_parte_de_la_ultima_reapertura(self):
        evs = [ev("estado", "solicitado", "validado", dt(LUN, 9)), ev("estado", "validado", "solicitado", dt(LUN + timedelta(days=1), 10))]
        assert RR.inicio_reloj(dt(LUN, 8), evs) == dt(LUN + timedelta(days=1), 10)

    def test_sin_reapertura_parte_de_la_creacion(self):
        assert RR.inicio_reloj(dt(LUN, 8), [ev("estado", "solicitado", "validado", dt(LUN, 9))]) == dt(LUN, 8)

    def test_el_ciclo_actual_ignora_la_respuesta_anterior(self):
        evs = [ev("estado", "solicitado", "validado", dt(LUN, 9)), ev("estado", "validado", "solicitado", dt(LUN, 12))]
        inicio = RR.inicio_reloj(dt(LUN, 8), evs)
        assert RR.primera_respuesta(evs, desde=inicio) is None
        assert RR.primera_respuesta(evs)["cuando"] == dt(LUN, 9)       # la primera respuesta «de siempre» sí existe


class TestNivelesDeAviso:
    def rel(self, nivel, estado="por_vencer"):
        return {"estado": estado, "nivel": nivel}

    def test_se_avisa_una_sola_vez_por_nivel(self):
        assert RR.nivel_pendiente(self.rel(1), set()) == 1
        assert RR.nivel_pendiente(self.rel(1), {1}) == 0
        assert RR.nivel_pendiente(self.rel(2), {1}) == 2
        assert RR.nivel_pendiente(self.rel(2, "vencida"), {1, 2}) == 0

    def test_si_llega_ya_en_nivel_2_se_avisa_solo_el_2(self):
        assert RR.nivel_pendiente(self.rel(2), set()) == 2
        assert RR.nivel_pendiente(self.rel(1), {2}) == 0

    def test_nunca_avisa_de_un_pedido_respondido_o_en_plazo(self):
        assert RR.nivel_pendiente(self.rel(2, "respondida"), set()) == 0
        assert RR.nivel_pendiente(self.rel(0, "en_plazo"), set()) == 0
        assert RR.nivel_pendiente(None, set()) == 0

    def test_las_marcas_se_leen_de_la_bitacora_del_ciclo_actual(self):
        evs = [ev("plazo", None, None, dt(LUN, 12), "sistema", RR.marca_detalle(1, "x")),
               ev("plazo", None, None, dt(LUN, 15), "sistema", RR.marca_detalle(2, "y")),
               ev("tomar", None, None, dt(LUN, 15), "sistema", "[n2] no es un aviso")]
        assert RR.marcas_avisadas(evs) == {1, 2}
        assert RR.marcas_avisadas(evs, desde=dt(LUN, 13)) == {2}          # lo anterior a la reapertura no cuenta
        assert RR.marcas_avisadas([ev("plazo", None, None, dt(LUN, 9), "s", "sin marca")]) == set()


class TestDueno:
    def cands(self, *pares):
        return [{"id": i + 1, "nombre": n, "por": p} for i, (n, p) in enumerate(pares)]

    def test_el_responsable_de_la_ot_va_primero(self):
        d = RR.elegir_dueno(self.cands(("Víctor", "responsable"), ("Aarón", "creador"), ("Juan", "ejecutivo")), {"id": 9, "nombre": "Defecto"})
        assert (d["nombre"], d["origen"], d["user_id"]) == ("Víctor", "responsable", 1)

    def test_luego_creador_y_ejecutivo(self):
        assert RR.elegir_dueno(self.cands(("Aarón", "creador"), ("Juan", "ejecutivo")))["origen"] == "creador"
        assert RR.elegir_dueno(self.cands(("Juan", "ejecutivo")))["origen"] == "ejecutivo"

    def test_el_respaldo_no_es_un_dueno_si_hay_responsable_por_defecto(self):
        d = RR.elegir_dueno(self.cands(("Daniel", "respaldo"), ("Aarón", "respaldo")), {"id": 9, "nombre": "Bodega Ana"})
        assert (d["nombre"], d["origen"]) == ("Bodega Ana", "defecto")

    def test_sin_nadie_mas_el_respaldo_es_el_dueno(self):
        d = RR.elegir_dueno(self.cands(("Daniel", "respaldo")))
        assert (d["nombre"], d["origen"]) == ("Daniel", "respaldo")

    def test_el_ticket_ya_asignado_manda(self):
        d = RR.elegir_dueno(self.cands(("Víctor", "responsable")), {"id": 9, "nombre": "Defecto"}, "Zoe")
        assert (d["nombre"], d["origen"]) == ("Zoe", "ticket")

    def test_nadie_es_none(self):
        assert RR.elegir_dueno([], None) is None
        assert RR.elegir_dueno([{"id": 1, "nombre": "  ", "por": "creador"}], None, "  ") is None

    def test_cada_origen_se_explica_en_la_bitacora(self):
        for o in ("ticket", "responsable", "creador", "ejecutivo", "defecto", "respaldo"):
            assert RR.ORIGEN_TXT[o]

    def test_es_critico(self):
        assert RR.es_critico(True, "") is True
        assert RR.es_critico(False, "¿Qué le pasa? → Hace ruido → Puede lastimar a alguien") is True
        assert RR.es_critico(False, "Se cortó la piola") is False
        assert RR.es_critico(0, None) is False


class TestCampana:
    def test_decidir_campana(self):
        assert RR.decidir_campana(None, None) == "crear"
        assert RR.decidir_campana(False, 500) == "actualizar"           # sin leer: siempre se suma
        assert RR.decidir_campana(True, 10) == "actualizar"             # leída pero dentro de la ventana de 30 min
        assert RR.decidir_campana(True, 31) == "crear"
        assert RR.decidir_campana(True, None) == "crear"

    def test_pedido_nuevo_uno_solo_lleva_el_detalle(self):
        t, c, p = RR.campana_pedidos_nuevos("OT-2026-00171", [{"repuesto_nombre": "Piola", "cantidad": 2}], "Gimnasio Demo", "Pedro",
                                            False, "Trotadora X · S/N 123", "Factura 555")
        assert t == "Nueva solicitud de repuesto · OT-2026-00171"
        assert c == "Piola × 2 · Trotadora X · S/N 123 · Gimnasio Demo · Factura 555 · pidió Pedro" and p == "media"

    def test_pedidos_nuevos_varios_se_suman(self):
        items = [{"repuesto_nombre": "Piola", "cantidad": 2}, {"repuesto_nombre": "Pin", "cantidad": 1}, {"repuesto_nombre": "Buje", "cantidad": 4}]
        t, c, p = RR.campana_pedidos_nuevos("OT-2026-00171", items, "Gimnasio Demo", "Pedro", True, "equipo ignorado")
        assert t == "OT-2026-00171: 3 pedidos de repuesto nuevos"
        assert "Piola × 2, Pin × 1, Buje × 4" in c and "equipo ignorado" not in c and "FUERA DE SERVICIO" in c and p == "alta"

    def test_aviso_de_plazo_uno_y_varios(self):
        it = {"id": 7, "repuesto_nombre": "Piola", "cantidad": 2, "ot": "OT-1", "cliente": "Gim", "transcurrido_h": 4.5, "fs": False}
        t1, c1, p1 = RR.campana_plazo(1, [it])
        assert "por vencer" in t1 and "#7" in t1 and p1 == "media" and "primera respuesta" in c1
        t2, c2, p2 = RR.campana_plazo(2, [it])
        assert "sin respuesta" in t2 and p2 == "alta" and "supervisión" in c2
        _, _, p3 = RR.campana_plazo(2, [dict(it, fs=True)])
        assert p3 == "urgente"
        t4, c4, _ = RR.campana_plazo(2, [dict(it, id=i) for i in range(1, 9)])
        assert t4.startswith("8 pedidos") and "y 3 más" in c4

    def test_los_textos_internos_no_hablan_de_plata_ni_proveedores(self):
        it = {"id": 1, "repuesto_nombre": "Piola", "cantidad": 1, "ot": "OT-1", "cliente": "Gim", "transcurrido_h": 5, "fs": False}
        for n in (1, 2):
            t, c, _ = RR.campana_plazo(n, [it])
            assert not re.search(r"proveedor|costo|\$|factur", t + c, re.I)


class TestTextosAlTecnico:
    CTX = {"repuesto_nombre": "Piola", "cantidad": 2, "camino": None, "hay_en_bodega": False, "eta": None, "detalle": None}

    def textos(self):
        out = {}
        for clave in ("revisado", "pedido", "fecha", "recibido", "instalado", "rechazado"):
            for extra in ({}, {"camino": "bodega"}, {"camino": "ticket"}, {"camino": "ot"}, {"hay_en_bodega": True},
                           {"eta": date(2026, 10, 28)}, {"detalle": "Se reparó sin repuesto: se cambió el cable"},
                           {"detalle": "No correspondía: era garantía"}):
                t = RR.texto_tecnico(clave, dict(self.CTX, **extra))
                if t:
                    out[(clave, tuple(sorted(extra.items(), key=str)))] = t
        return out

    def test_mapa_de_cambios_de_estado(self):
        assert RR.evento_tecnico("solicitado", "validado") == "revisado"
        assert RR.evento_tecnico("solicitado", "pedido") == "pedido"
        assert RR.evento_tecnico("validado", "pedido") == "pedido"
        assert RR.evento_tecnico("pedido", "recibido") == "recibido"
        assert RR.evento_tecnico("recibido", "instalado") == "instalado"
        assert RR.evento_tecnico("validado", "rechazado") == "rechazado"

    def test_los_pasos_atras_no_avisan(self):
        for de, a in (("pedido", "validado"), ("validado", "solicitado"), ("rechazado", "solicitado"), ("instalado", "rechazado"),
                      ("solicitado", "solicitado"), (None, "solicitado"), ("recibido", "validado")):
            assert RR.evento_tecnico(de, a) is None, (de, a)

    def test_revisado_distingue_los_caminos(self):
        assert "hay en bodega" in RR.texto_tecnico("revisado", dict(self.CTX, camino="bodega"))[0]
        assert "hay en bodega" in RR.texto_tecnico("revisado", dict(self.CTX, hay_en_bodega=True))[0]
        assert "lo vamos a pedir" in RR.texto_tecnico("revisado", dict(self.CTX, camino="ticket"))[0]
        assert "lo vamos a pedir" in RR.texto_tecnico("revisado", self.CTX)[0]
        assert "OT" in RR.texto_tecnico("revisado", dict(self.CTX, camino="ot"))[0]

    def test_pedido_con_y_sin_fecha(self):
        con = RR.texto_tecnico("pedido", dict(self.CTX, eta=date(2026, 10, 28)))
        sin = RR.texto_tecnico("pedido", self.CTX)
        assert "28/10" in con[1] and "28/10" not in sin[1] and "Te avisamos" in sin[1]
        assert RR.texto_tecnico("fecha", dict(self.CTX, eta="28/10/2026"))[1].endswith("llega aprox. el 28/10.")
        assert RR.texto_tecnico("fecha", self.CTX) is None

    def test_el_motivo_del_cierre_son_tres_frases_fijas_nunca_la_nota(self):
        nota = "No correspondía: el proveedor Acme cobra $15.000 por la OC 4455 y es una garantía interna"
        t = RR.texto_tecnico("rechazado", dict(self.CTX, detalle=nota))
        assert t[1].endswith("Bodega vio que este pedido no correspondía.")
        assert not re.search(r"Acme|15\.000|4455|garant", t[0] + t[1])
        assert "Se reparó sin necesidad del repuesto." in RR.texto_tecnico("rechazado", dict(self.CTX, detalle="Se reparó sin repuesto: x"))[1]
        gen = RR.texto_tecnico("rechazado", dict(self.CTX, detalle="Duplicado del pedido 33; proveedor Acme no lo tiene"))[1]
        assert "no va a tramitar" in gen and "Acme" not in gen

    def test_ningun_texto_menciona_proveedor_costo_ni_oc(self):
        """REGLAS #19 y #26: ni siquiera con datos de proveedor en el contexto."""
        for k, (titulo, cuerpo, prio) in self.textos().items():
            assert not re.search(r"proveedor|costo|precio|valor|\$|\bOC\b|orden de compra|factur|cobr|pago|monto", titulo + " " + cuerpo, re.I), (k, titulo, cuerpo)
            assert prio in ("baja", "media", "alta")

    def test_ningun_texto_es_filtrado_por_la_campana_del_tecnico(self):
        """La campana del técnico oculta lo que huela a plata (`_NOTIF_TEC_RE_VEDADO` de app.py): estos avisos tienen que pasar el filtro."""
        amb = {"re": re}
        FC.extraer(amb, "app.py", (), ("_NOTIF_TEC_RE_VEDADO",))
        vedado = amb["_NOTIF_TEC_RE_VEDADO"]
        for k, (titulo, cuerpo, _p) in self.textos().items():
            assert not vedado.search("repuesto_tecnico %s %s" % (titulo, cuerpo)), (k, titulo, cuerpo)

    def test_fecha_corta(self):
        assert RR.fecha_corta(date(2026, 10, 28)) == "28/10"
        assert RR.fecha_corta(datetime(2026, 10, 28, 9)) == "28/10"
        assert RR.fecha_corta("28/10/2026") == "28/10"
        assert RR.fecha_corta("basura") == "" and RR.fecha_corta(None) == ""


class TestResumenTiempos:
    def pedido(self, i, dueno, inicio, resp=None, fs=False):
        return {"id": i, "dueno": dueno, "inicio": inicio, "respuesta": resp, "fs": fs, "ot": "OT-%d" % i, "repuesto": "Piola", "cantidad": 1}

    def test_cifras_generales_y_por_dueno(self):
        ahora = dt(LUN + timedelta(days=4), 17)
        ps = [self.pedido(1, "Ana", dt(LUN, 8), dt(LUN, 10)),                                       # 2 h, en plazo
              self.pedido(2, "Ana", dt(LUN, 8), dt(LUN, 12)),                                       # 4 h, en plazo
              self.pedido(3, "Bea", dt(LUN, 8), dt(LUN + timedelta(days=1), 12)),                   # 13 h, fuera de plazo
              self.pedido(4, "Bea", dt(LUN, 8), None),                                              # sin responder y vencido
              self.pedido(5, "", dt(LUN + timedelta(days=4), 15), None)]                            # sin responder, dentro del plazo
        r = RR.resumen_tiempos(ps, ahora, FER)
        g = r["general"]
        assert (g["pedidos"], g["respondidos"], g["sin_responder"], g["vencidos"], g["respondidos_en_plazo"]) == (5, 3, 2, 1, 2)
        assert g["promedio_h"] == 6.3 and g["mediana_h"] == 4.0
        assert g["pct_en_plazo"] == 50          # 2 de (3 respondidos + 1 vencido); el pedido 5 todavía puede cumplir
        por = {f["dueno"]: f for f in r["por_dueno"]}
        assert set(por) == {"Ana", "Bea", "Sin dueño"}
        assert por["Ana"]["pct_en_plazo"] == 100 and por["Ana"]["promedio_h"] == 3.0
        assert por["Bea"]["vencidos"] == 1 and por["Bea"]["pct_en_plazo"] == 0
        assert r["por_dueno"][0]["dueno"] == "Bea"                       # primero quien más tiene vencido
        assert [v["id"] for v in r["vencidos"]] == [4] and r["vencidos"][0]["horas"] >= 9

    def test_sin_pedidos(self):
        r = RR.resumen_tiempos([], dt(LUN, 12), FER)
        assert r["general"]["pedidos"] == 0 and r["general"]["pct_en_plazo"] is None and r["general"]["promedio_h"] is None
        assert r["por_dueno"] == [] and r["vencidos"] == []

    def test_no_trae_montos(self):
        r = RR.resumen_tiempos([self.pedido(1, "Ana", dt(LUN, 8), None)], dt(LUN + timedelta(days=2), 12), FER)
        assert not re.search(r"costo|monto|proveedor|precio", json.dumps(r, default=str), re.I)


# ═══════════════════════════════════════════════════════════════════════════════════════════════
#  B. LAS FUNCIONES REALES DE app.py CONTRA UNA BASE DE MENTIRA
# ═══════════════════════════════════════════════════════════════════════════════════════════════
FUNCIONES = ("_otrep_correos_activos", "_otrep_usuario_por_nombre", "_otrep_correo_de", "_otrep_defecto_usuario", "_otrep_destinos_gestion",
             "_otrep_dueno_resolver", "_otrep_asignar_dueno", "_otrep_ticket_dueno_asegurar", "_otrep_enviar_correo",
             "_otrep_avisar_nueva_ot", "_otrep_avisar_tecnico", "_otrep_avisar_tecnico_clave", "_otrep_avisar_eta_compra",
             "_otrep_eventos_lote", "_otrep_reloj_de", "_otrep_escalamiento_destinos", "_otrep_barrido_respuesta",
             "repstock_tiempos_respuesta", "_otrep_reloj_json", "_otrep_adjuntar_reloj", "_otrep_a_chile", "_otrep_feriados", "_rol_familia")
CONSTANTES = ("_GESTION_CORREO_RE", "_OTREP_FERIADOS")


class _Cur:
    def __init__(self, mundo):
        self.m = mundo
        self._fila = None

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=()):
        if "GET_LOCK" in sql:
            ok = 0 if self.m.lock_ocupado else 1
            self.m.lock_tomado = bool(ok)
            self._fila = {"l": ok}
        elif "RELEASE_LOCK" in sql:
            self.m.lock_liberado = True
            self._fila = {"r": 1}

    def fetchone(self):
        return self._fila


class _Conn:
    def __init__(self, mundo):
        self.m = mundo

    def cursor(self):
        return _Cur(self.m)

    def commit(self):
        pass

    def close(self):
        self.m.conn_cerrada = True


class Mundo:
    """Base de mentira: usuarios, solicitudes, bitácora y campanas. Las funciones de app.py son las REALES."""

    def __init__(self, ahora_chile=None):
        self.ahora_chile = ahora_chile or datetime(2026, 10, 19, 11, 0)          # lunes a media mañana
        self.users = [
            {"id": 1, "username": "daniel@sphs.cl", "nombre": "Daniel", "role": "superadmin"},
            {"id": 2, "username": "victor@sphs.cl", "nombre": "Víctor", "role": "supervisor"},
            {"id": 3, "username": "aaron@sphs.cl", "nombre": "Aarón", "role": "admin"},
            {"id": 4, "username": "pedro@sphs.cl", "nombre": "Pedro Técnico", "role": "tecnico"},
            {"id": 5, "username": "ana@sphs.cl", "nombre": "Ana Bodega", "role": "ejecutivo"},
            {"id": 6, "username": "zoe", "nombre": "Zoe Sin Correo", "role": "ejecutivo"},
        ]
        self.sol = {}                  # id → dict
        self.eventos = []              # dicts con solicitud_id
        self.notifs = []               # campanas
        self.correos = []              # correos de mentira que "salieron"
        self.gestion = {}              # vid → [candidatos de _hall_destinos]
        self.defecto = ""
        self.ticket_asignado = {}      # vid → nombre
        self.tickets_asignados = []    # (tid, nombre)
        self.lock_ocupado = self.lock_tomado = self.lock_liberado = self.conn_cerrada = False
        self.env = {}
        self.avisos = []
        self.sin_eventos_tabla = False
        self.compras = {}
        amb = {"json": json, "re": re, "time": time, "datetime": datetime, "timedelta": timedelta, "os": types.SimpleNamespace(environ=self.env),
               "print": lambda *a, **k: self.avisos.append(" ".join(str(x) for x in a)), "_rresp": RR,
               "_ccr": R, "_pm": __import__("ot_piezas_medidas"),
               "mysql_fetchone": self._fetchone, "mysql_fetchall": self._fetchall, "mysql_execute": self._execute,
               "mysql_execute_returning_rowcount": self._rowcount, "get_mysql": lambda: _Conn(self),
               "_mant_notificar": self._notificar, "_mant_notif_cache_invalidar": lambda uid=None: None,
               "_hall_destinos": lambda vid, v, excluir_uid=None: [dict(p) for p in self.gestion.get(vid, []) if not (excluir_uid and p["id"] == excluir_uid)],
               "_otrep_cfg": lambda: {"responsable_defecto": self.defecto},
               "_otrep_eventos_asegurar": lambda: not self.sin_eventos_tabla, "_otrep_resp_asegurar": lambda: True,
               "_otrep_evento": self._evento,
               "_send_ilus_email": self._enviar, "_ilus_email_master": lambda ctx: "<html>" + ctx["title"] + "</html>",
               "_ilus_email_rows": lambda filas: "".join(f"<tr>{k}:{v}</tr>" for k, v in filas if v),
               "_brand_subject": lambda s: s, "_public_base_url": lambda: "https://ilus.test", "_qp_correos_bodega": lambda: ["bodega@sphs.cl"],
               "current_username": lambda: "Pedro Técnico", "request": types.SimpleNamespace(args={}),
               "jsonify": lambda *a, **k: (a[0] if a else k), "_NOTIF_TEC": None}
        FC.extraer(amb, "app.py", FUNCIONES, CONSTANTES)
        # El evento y el reloj de pared son los de mentira: la hora «ahora» la fija la prueba.
        self.amb = amb
        self.f = types.SimpleNamespace(**{k: amb[k] for k in FUNCIONES})
        amb["_otrep_evento"] = self._evento

    # ── SQL de mentira ──
    def _fetchone(self, sql, params=()):
        s = " ".join(sql.split())
        if "SELECT NOW() AS t" in s:
            return {"t": a_utc(self.ahora_chile)}
        if "FROM app_users WHERE active=1 AND (username=%s OR nombre=%s)" in s:
            return next((dict(u) for u in self.users if params[0] in (u["username"], u["nombre"])), None)
        if "FROM app_users WHERE active=1 AND COALESCE(nombre, username)=%s" in s:
            return next((dict(u) for u in self.users if (u["nombre"] or u["username"]) == params[0]), None)
        if "JOIN tk_tickets t ON t.id=s.ticket_id" in s and "spare_parts" in s:
            n = self.ticket_asignado.get(params[0])
            return {"asignado_a": n} if n else None
        if s.startswith("SELECT id, visita_id, responsable, creada_desde FROM mant_ot_repuesto_solicitudes"):
            r = self.sol.get(params[0])
            return {k: r.get(k) for k in ("id", "visita_id", "responsable", "creada_desde")} if r else None
        if "LEFT JOIN mant_repuestos_compras cp" in s:
            r = self.sol.get(params[0])
            return dict(r) if r else None
        if "FROM mant_notificaciones WHERE tipo='repuesto_nuevo'" in s:
            vid, uid = params
            cand = [n for n in self.notifs if n["tipo"] == "repuesto_nuevo" and n.get("visita_id") == vid and n["destino"] == uid and not n.get("archivada")]
            if not cand:
                return None
            return {"id": cand[-1]["id"], "leida_at": (cand[-1]["created_at"] if cand[-1]["leida"] else None), "created_at": cand[-1]["created_at"]}
        raise AssertionError("fetchone no previsto: " + s)

    def _fetchall(self, sql, params=()):
        s = " ".join(sql.split())
        if "FROM mant_ot_repuesto_eventos" in s:
            ids = set(params)
            return [dict(e) for e in self.eventos if e["solicitud_id"] in ids and e["accion"] in ("estado", "plazo")]
        if "WHERE s.estado='solicitado'" in s:
            return [dict(r) for r in self.sol.values() if r["estado"] == "solicitado" and (r.get("creada_desde") or "") != "manual"]
        if s.startswith("SELECT s.id, s.visita_id, s.repuesto_nombre") and "s.created_at >= %s" in s:
            return [dict(r) for r in self.sol.values()]
        if s == "SELECT id, username, nombre, role FROM app_users WHERE active=1":
            return [dict(u) for u in self.users]
        if "FROM mant_ot_repuesto_solicitudes WHERE visita_id=%s AND created_at >= %s" in s:
            vid, desde = params
            return [dict(r) for r in self.sol.values() if r.get("visita_id") == vid and a_utc(r["creada_chile"]) >= desde]
        if "FROM mant_ot_repuesto_solicitudes WHERE compra_id=%s AND estado='pedido'" in s:
            return [{"id": r["id"]} for r in self.sol.values() if r.get("compra_id") == params[0] and r["estado"] == "pedido"]
        raise AssertionError("fetchall no previsto: " + s)

    def _execute(self, sql, params=()):
        s = " ".join(sql.split())
        if s.startswith("UPDATE mant_notificaciones SET titulo"):
            titulo, cuerpo = params[0], params[1]
            n = next(n for n in self.notifs if n["id"] == params[-1])
            n.update(titulo=titulo, cuerpo=cuerpo, leida=False, prioridad=params[3] if len(params) > 5 else params[2])
            return
        if s.startswith("INSERT INTO tk_mensajes"):
            self.tickets_asignados.append(("msg", params[0]))
            return
        raise AssertionError("execute no previsto: " + s)

    def _rowcount(self, sql, params=()):
        s = " ".join(sql.split())
        if s.startswith("UPDATE mant_ot_repuesto_solicitudes SET responsable=%s WHERE id=%s"):
            r = self.sol[params[1]]
            if not (r.get("responsable") or "").strip():
                r["responsable"] = params[0]
                return 1
            return 0
        if s.startswith("UPDATE tk_tickets SET asignado_a=%s WHERE id=%s"):
            if (params[1], params[0]) in self.tickets_asignados:
                return 0
            self.tickets_asignados.append((params[1], params[0]))
            return 1
        raise AssertionError("rowcount no previsto: " + s)

    def _evento(self, sid, accion, usuario, de_estado=None, a_estado=None, detalle=None):
        self.eventos.append({"solicitud_id": sid, "accion": accion, "usuario": usuario, "de_estado": de_estado, "a_estado": a_estado,
                             "detalle": detalle, "created_at": a_utc(self.ahora_chile)})

    def _notificar(self, destino, tipo, titulo, cuerpo="", url_accion="", prioridad="media", cliente_id=None, visita_id=None):
        # Misma idempotencia que el real: una sin leer igual (tipo + cliente + visita + destino, o url + título sin visita) no se repite.
        for n in self.notifs:
            if n["tipo"] == tipo and not n["leida"] and n["destino"] == destino and n.get("cliente_id") == cliente_id and n.get("visita_id") == visita_id:
                if visita_id is None and cliente_id is None and (n["url"] != url_accion or n["titulo"] != titulo):
                    continue
                return n["id"]
        n = {"id": len(self.notifs) + 1, "destino": destino, "tipo": tipo, "titulo": titulo, "cuerpo": cuerpo, "url": url_accion,
             "prioridad": prioridad, "cliente_id": cliente_id, "visita_id": visita_id, "leida": False,
             "created_at": a_utc(self.ahora_chile)}
        self.notifs.append(n)
        return n["id"]

    def _enviar(self, to, asunto, html, **kw):
        self.correos.append({"to": to, "asunto": asunto, "html": html, **kw})
        return True

    # ── utilidades de las pruebas ──
    def pedido(self, sid, vid=10, creada=None, estado="solicitado", **kw):
        creada = creada or self.ahora_chile
        base = {"id": sid, "visita_id": vid, "cliente_id": 99, "repuesto_nombre": "Piola", "cantidad": 2, "estado": estado,
                "creada_chile": creada, "created_at": a_utc(creada), "responsable": None, "dejo_fuera_servicio": 0, "creada_desde": "ot",
                "numero_ot": "OT-2026-00171", "cliente": "Gimnasio Demo", "solicitado_por": "Pedro Técnico", "es_reposicion": 0,
                "solicitud_padre_id": None, "ticket_asignado_a": None, "maquina_estado": None, "camino": None, "repuesto_stock_id": None,
                "compra_eta": None, "equipo_ref": "Trotadora X", "serie": "S123", "documento": "FCV 555", "diagnostico_json": None}
        base.update(kw)
        self.sol[sid] = base
        return base

    def cobertura_cerrada(self):
        self.ahora_chile = datetime(2026, 10, 17, 11, 0)                   # sábado
        return self


@pytest.fixture
def mundo():
    return Mundo()


def gestion(*pares):
    return [{"id": i, "nombre": n, "correo": c, "por": p} for i, n, c, p in pares]


class TestDuenoAlCrear:
    def test_el_pedido_nace_con_el_responsable_de_la_ot(self, mundo):
        mundo.gestion[10] = gestion((2, "Víctor", "victor@sphs.cl", "responsable"), (3, "Aarón", "aaron@sphs.cl", "creador"))
        mundo.pedido(1)
        d = mundo.f._otrep_asignar_dueno(1)
        assert d["nombre"] == "Víctor" and mundo.sol[1]["responsable"] == "Víctor"
        (e,) = [e for e in mundo.eventos if e["accion"] == "tomar"]
        assert "Víctor" in e["detalle"] and "responsable de la OT" in e["detalle"] and e["usuario"] == "sistema"

    def test_nunca_pisa_un_responsable_ya_escrito(self, mundo):
        mundo.gestion[10] = gestion((2, "Víctor", "victor@sphs.cl", "responsable"))
        mundo.pedido(1, responsable="Ana Bodega")
        assert mundo.f._otrep_asignar_dueno(1) is None
        assert mundo.sol[1]["responsable"] == "Ana Bodega" and mundo.eventos == []

    def test_sin_gestion_usa_el_responsable_por_defecto(self, mundo):
        mundo.defecto = "Ana Bodega"
        mundo.pedido(1)
        assert mundo.f._otrep_asignar_dueno(1)["origen"] == "defecto"
        assert mundo.sol[1]["responsable"] == "Ana Bodega"

    def test_el_respaldo_pierde_contra_el_responsable_por_defecto(self, mundo):
        mundo.gestion[10] = gestion((1, "Daniel", "daniel@sphs.cl", "respaldo"))
        mundo.defecto = "Ana Bodega"
        mundo.pedido(1)
        assert mundo.f._otrep_asignar_dueno(1)["nombre"] == "Ana Bodega"

    def test_el_responsable_por_defecto_nunca_es_un_tecnico(self, mundo):
        mundo.defecto = "Pedro Técnico"
        mundo.pedido(1)
        assert mundo.f._otrep_asignar_dueno(1) is None and mundo.sol[1]["responsable"] is None

    def test_si_el_ticket_de_la_ot_ya_tiene_dueno_se_usa_el_mismo(self, mundo):
        mundo.gestion[10] = gestion((2, "Víctor", "victor@sphs.cl", "responsable"))
        mundo.ticket_asignado[10] = "Ana Bodega"
        mundo.pedido(1)
        assert mundo.f._otrep_asignar_dueno(1)["nombre"] == "Ana Bodega"

    def test_los_que_levanta_la_gestion_o_no_son_de_una_ot_no_se_tocan(self, mundo):
        mundo.gestion[10] = gestion((2, "Víctor", "victor@sphs.cl", "responsable"))
        for i, kw in enumerate(({"creada_desde": "manual"}, {"creada_desde": "incidencia"}, {"creada_desde": "ticket"}, {"visita_id": None}), 1):
            mundo.pedido(i, **kw)
            assert mundo.f._otrep_asignar_dueno(i) is None, kw

    def test_un_error_no_rompe_la_creacion(self, mundo):
        mundo.amb["mysql_fetchone"] = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("base caída"))
        assert mundo.f._otrep_asignar_dueno(1) is None
        assert any("asignar dueño" in a for a in mundo.avisos)

    def test_el_ticket_queda_a_nombre_del_dueno_solo_si_estaba_sin_asignar(self, mundo):
        mundo.gestion[10] = gestion((2, "Víctor", "victor@sphs.cl", "responsable"))
        mundo.f._otrep_ticket_dueno_asegurar(77, 10, "Pedro Técnico")
        assert (77, "Víctor") in mundo.tickets_asignados and ("msg", 77) in mundo.tickets_asignados
        mundo.f._otrep_ticket_dueno_asegurar(77, 10, "Pedro Técnico")                      # segunda vez: no vuelve a asignar
        assert mundo.tickets_asignados.count((77, "Víctor")) == 1
        # si el ticket de la OT ya tiene dueño, no se toca
        mundo.ticket_asignado[10] = "Zoe Sin Correo"
        mundo.f._otrep_ticket_dueno_asegurar(78, 10, "Pedro Técnico")
        assert (78, "Zoe Sin Correo") not in mundo.tickets_asignados and (78, "Víctor") not in mundo.tickets_asignados


class TestAvisoDePedidoNuevo:
    def test_va_a_la_gestion_de_la_ot_y_no_a_todos_los_admin(self, mundo):
        mundo.gestion[10] = gestion((2, "Víctor", "victor@sphs.cl", "responsable"), (3, "Aarón", "aaron@sphs.cl", "creador"))
        s = mundo.pedido(1, responsable="Víctor")
        assert mundo.f._otrep_avisar_nueva_ot(s, "Pedro Técnico") == 2
        assert sorted(n["destino"] for n in mundo.notifs) == [2, 3]                     # Daniel (superadmin) NO recibe
        assert {n["tipo"] for n in mundo.notifs} == {"repuesto_nuevo"}
        assert all(n["titulo"] == "Nueva solicitud de repuesto · OT-2026-00171" for n in mundo.notifs)
        assert "Piola × 2" in mundo.notifs[0]["cuerpo"] and "pidió Pedro Técnico" in mundo.notifs[0]["cuerpo"]

    def test_el_responsable_por_defecto_tambien_se_entera_pero_un_tecnico_nunca(self, mundo):
        mundo.gestion[10] = gestion((2, "Víctor", "victor@sphs.cl", "responsable"))
        mundo.defecto = "Ana Bodega"
        s = mundo.pedido(1, responsable="Pedro Técnico")                        # un dueño técnico (dato malo) no recibe la campana
        mundo.f._otrep_avisar_nueva_ot(s, "Pedro Técnico")
        assert sorted(n["destino"] for n in mundo.notifs) == [2, 5]

    def test_quien_lo_pide_no_se_avisa_a_si_mismo(self, mundo):
        mundo.gestion[10] = gestion((2, "Víctor", "victor@sphs.cl", "responsable"), (3, "Aarón", "aaron@sphs.cl", "creador"))
        s = mundo.pedido(1, solicitado_por="Víctor")
        mundo.f._otrep_avisar_nueva_ot(s, "Víctor")
        assert [n["destino"] for n in mundo.notifs] == [3]

    def test_sin_gestion_devuelve_none_y_sigue_el_aviso_de_siempre(self, mundo):
        s = mundo.pedido(1)
        assert mundo.f._otrep_avisar_nueva_ot(s, "Pedro Técnico") is None and mundo.notifs == []

    def test_si_solo_estaba_quien_lo_pidio_no_hay_a_quien_avisar_pero_tampoco_se_manda_a_todos(self, mundo):
        mundo.gestion[10] = gestion((2, "Víctor", "victor@sphs.cl", "responsable"))
        s = mundo.pedido(1, solicitado_por="Víctor")
        assert mundo.f._otrep_avisar_nueva_ot(s, "Víctor") == 0 and mundo.notifs == []
        assert "_otrep_avisar_nueva_ot(s, usuario) is not None" in FC.funcion_top("app.py", "_otrep_avisar_nueva")

    def test_si_ninguna_campana_pudo_salir_sigue_el_aviso_de_siempre(self, mundo):
        mundo.gestion[10] = gestion((2, "Víctor", "victor@sphs.cl", "responsable"))
        mundo.amb["_mant_notificar"] = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("sin campana"))
        assert mundo.f._otrep_avisar_nueva_ot(mundo.pedido(1), "Pedro Técnico") is None

    def test_varios_pedidos_de_la_misma_ot_se_suman_en_una_sola_campana(self, mundo):
        mundo.gestion[10] = gestion((2, "Víctor", "victor@sphs.cl", "responsable"))
        mundo.pedido(1)
        mundo.f._otrep_avisar_nueva_ot(mundo.sol[1], "Pedro Técnico")
        mundo.ahora_chile += timedelta(minutes=3)
        mundo.pedido(2, repuesto_nombre="Pin", cantidad=1, creada=mundo.ahora_chile)
        mundo.f._otrep_avisar_nueva_ot(mundo.sol[2], "Pedro Técnico")
        mundo.ahora_chile += timedelta(minutes=3)
        mundo.pedido(3, repuesto_nombre="Buje", cantidad=4, creada=mundo.ahora_chile)
        mundo.f._otrep_avisar_nueva_ot(mundo.sol[3], "Pedro Técnico")
        (n,) = mundo.notifs
        assert n["titulo"] == "OT-2026-00171: 3 pedidos de repuesto nuevos"
        assert "Piola × 2, Pin × 1, Buje × 4" in n["cuerpo"]

    def test_una_campana_leida_dentro_de_30_minutos_se_actualiza_y_pasada_la_ventana_se_crea_otra(self, mundo):
        mundo.gestion[10] = gestion((2, "Víctor", "victor@sphs.cl", "responsable"))
        mundo.pedido(1)
        mundo.f._otrep_avisar_nueva_ot(mundo.sol[1], "Pedro Técnico")
        mundo.notifs[0]["leida"] = True
        mundo.ahora_chile += timedelta(minutes=10)
        mundo.pedido(2, creada=mundo.ahora_chile)
        mundo.f._otrep_avisar_nueva_ot(mundo.sol[2], "Pedro Técnico")
        assert len(mundo.notifs) == 1 and mundo.notifs[0]["leida"] is False and "2 pedidos" in mundo.notifs[0]["titulo"]
        mundo.notifs[0]["leida"] = True
        mundo.ahora_chile += timedelta(minutes=45)
        mundo.pedido(3, creada=mundo.ahora_chile)
        mundo.f._otrep_avisar_nueva_ot(mundo.sol[3], "Pedro Técnico")
        assert len(mundo.notifs) == 2

    def test_equipo_fuera_de_servicio_sube_la_prioridad(self, mundo):
        mundo.gestion[10] = gestion((2, "Víctor", "victor@sphs.cl", "responsable"))
        s = mundo.pedido(1, dejo_fuera_servicio=1)
        mundo.f._otrep_avisar_nueva_ot(s, "Pedro Técnico")
        assert mundo.notifs[0]["prioridad"] == "alta" and "FUERA DE SERVICIO" in mundo.notifs[0]["cuerpo"]


class TestCorreosInternosNacenApagados:
    def graves(self, mundo):
        mundo.gestion[10] = gestion((2, "Víctor", "victor@sphs.cl", "responsable"), (6, "Zoe Sin Correo", "", "creador"))
        return mundo.pedido(1, dejo_fuera_servicio=1, responsable="Víctor")

    def test_sin_la_variable_no_sale_ningun_correo(self, mundo):
        mundo.f._otrep_avisar_nueva_ot(self.graves(mundo), "Pedro Técnico")
        assert mundo.correos == [] and len(mundo.notifs) == 2                       # la campana sí avisa

    def test_con_la_variable_sale_rep_nueva_solo_si_es_grave(self, mundo):
        mundo.env["REPUESTOS_CORREOS_ACTIVOS"] = "1"
        mundo.f._otrep_avisar_nueva_ot(self.graves(mundo), "Pedro Técnico")
        assert {c["correo"] for c in mundo.correos} == {"rep_nueva"}
        assert {c["to"] for c in mundo.correos} == {"victor@sphs.cl", "bodega@sphs.cl"}          # quien no tiene correo no recibe; bodega sí
        assert all(c["modulo"] == "comunicacion_interna" and c["evento"] == "rep_nueva" for c in mundo.correos)
        assert all(c["ref"]["tipo"] == "OT" for c in mundo.correos)
        # un pedido normal no manda correo aunque estén encendidos
        mundo.correos.clear()
        mundo.f._otrep_avisar_nueva_ot(mundo.pedido(2), "Pedro Técnico")
        assert mundo.correos == []

    def test_riesgo_de_lesion_tambien_cuenta_como_grave(self, mundo):
        mundo.env["REPUESTOS_CORREOS_ACTIVOS"] = "si"
        mundo.gestion[10] = gestion((2, "Víctor", "victor@sphs.cl", "responsable"))
        diag = json.dumps({"camino": [{"r": "¿Qué le pasa?"}, {"r": "Puede lastimar a alguien"}]})
        mundo.f._otrep_avisar_nueva_ot(mundo.pedido(1, diagnostico_json=diag), "Pedro Técnico")
        assert {c["correo"] for c in mundo.correos} == {"rep_nueva"}

    def test_valores_que_no_encienden(self, mundo):
        for v in ("", "0", "no", "false", "apagado", "2"):
            mundo.env["REPUESTOS_CORREOS_ACTIVOS"] = v
            assert mundo.f._otrep_correos_activos() is False, v
        for v in ("1", "true", "YES", " on ", "sí", "si"):
            mundo.env["REPUESTOS_CORREOS_ACTIVOS"] = v
            assert mundo.f._otrep_correos_activos() is True, v

    def test_el_correo_del_tecnico_sale_solo_al_recibirse_y_sin_plata(self, mundo):
        mundo.env["REPUESTOS_CORREOS_ACTIVOS"] = "1"
        mundo.pedido(1, estado="recibido", repuesto_nombre="Piola", compra_eta=date(2026, 10, 28))
        mundo.f._otrep_avisar_tecnico(1, "pedido", "recibido", "Ana Bodega", None)
        (c,) = mundo.correos
        assert c["correo"] == "rep_lista_tecnico" and c["to"] == "pedro@sphs.cl" and c["modulo"] == "ot_tecnicos"
        assert not re.search(r"proveedor|costo|\bOC\b|\$", c["asunto"] + c["html"], re.I)
        mundo.correos.clear()
        mundo.f._otrep_avisar_tecnico(1, "solicitado", "validado", "Ana Bodega", None)          # revisado: solo campana
        assert mundo.correos == []

    def test_el_barrido_manda_rep_vencida_solo_con_la_variable(self, mundo):
        mundo.sol.clear()
        mundo.pedido(1, creada=datetime(2026, 10, 16, 8, 0), responsable="Ana Bodega")
        mundo.ahora_chile = datetime(2026, 10, 19, 11, 0)
        mundo.f._otrep_barrido_respuesta(ahora=mundo.ahora_chile)
        assert mundo.correos == []
        mundo.sol[1]["responsable"] = "Ana Bodega"
        mundo.eventos.clear()
        mundo.notifs.clear()
        mundo.env["REPUESTOS_CORREOS_ACTIVOS"] = "1"
        mundo.f._otrep_barrido_respuesta(ahora=mundo.ahora_chile)
        assert {c["correo"] for c in mundo.correos} == {"rep_vencida"}
        assert all(c["modulo"] == "comunicacion_interna" for c in mundo.correos)
        assert {c["to"] for c in mundo.correos} == {"ana@sphs.cl", "victor@sphs.cl", "aaron@sphs.cl"}     # dueño + supervisión; nunca un técnico


class TestAvisosAlTecnico:
    def test_revisado_pedido_recibido_instalado_y_cerrado(self, mundo):
        mundo.pedido(1, camino="ticket", compra_eta=date(2026, 10, 28))
        casos = [("solicitado", "validado", "lo vamos a pedir"), ("validado", "pedido", "28/10"), ("pedido", "recibido", "ya está en bodega"),
                 ("recibido", "instalado", "quedó instalado"), ("validado", "rechazado", "Se cerró")]
        for de, a, frase in casos:
            assert mundo.f._otrep_avisar_tecnico(1, de, a, "Ana Bodega", None) == 1, (de, a)
            n = mundo.notifs[-1]
            assert n["destino"] == 4 and n["tipo"] == "repuesto_tecnico" and n["url"] == "/ot/10?rep=1"
            assert frase in n["titulo"] + n["cuerpo"], (de, a, n)
        assert len(mundo.notifs) == 5

    def test_no_avisa_de_un_paso_atras_ni_a_quien_hizo_el_cambio(self, mundo):
        mundo.pedido(1)
        assert mundo.f._otrep_avisar_tecnico(1, "pedido", "validado", "Ana Bodega", None) == 0
        assert mundo.f._otrep_avisar_tecnico(1, "solicitado", "validado", "Pedro Técnico", None) == 0         # lo hizo él mismo
        assert mundo.f._otrep_avisar_tecnico(1, "solicitado", "validado", "pedro@sphs.cl", None) == 0
        assert mundo.notifs == []

    def test_solo_pedidos_de_una_ot_que_no_son_de_la_gestion(self, mundo):
        for i, kw in enumerate(({"visita_id": None}, {"es_reposicion": 1}, {"solicitud_padre_id": 5}, {"creada_desde": "manual"},
                                {"solicitado_por": "Fulano Inexistente"}), 1):
            mundo.pedido(i, **kw)
            assert mundo.f._otrep_avisar_tecnico(i, "solicitado", "validado", "Ana Bodega", None) == 0, kw
        assert mundo.notifs == []

    def test_el_rechazo_da_el_motivo_sencillo_nunca_la_nota_de_gestion(self, mundo):
        mundo.pedido(1)
        nota = "No correspondía: el proveedor Acme cobra $15.000 (OC 4455)"
        mundo.f._otrep_avisar_tecnico(1, "validado", "rechazado", "Ana Bodega", "; ".join(("proveedor Acme", nota)))
        n = mundo.notifs[-1]
        assert "Bodega no va a tramitar" in n["cuerpo"] and not re.search(r"Acme|15\.000|4455", n["titulo"] + n["cuerpo"])
        mundo.pedido(2)
        mundo.f._otrep_avisar_tecnico(2, "validado", "rechazado", "Ana Bodega", nota)
        assert "Bodega vio que este pedido no correspondía." in mundo.notifs[-1]["cuerpo"]

    def test_la_fecha_de_llegada_de_una_compra_avisa_sin_proveedor(self, mundo):
        mundo.pedido(1, estado="pedido", compra_id=8, compra_eta=date(2026, 11, 2))
        mundo.pedido(2, estado="pedido", compra_id=9, compra_eta=date(2026, 11, 9))
        mundo.f._otrep_avisar_eta_compra(8)
        (n,) = mundo.notifs
        assert "02/11" in n["cuerpo"] and n["url"] == "/ot/10?rep=1" and not re.search(r"proveedor", n["titulo"] + n["cuerpo"], re.I)

    def test_el_mismo_aviso_no_se_duplica_mientras_siga_sin_leer(self, mundo):
        mundo.pedido(1)
        mundo.f._otrep_avisar_tecnico(1, "solicitado", "validado", "Ana Bodega", None)
        mundo.f._otrep_avisar_tecnico(1, "solicitado", "validado", "Ana Bodega", None)
        assert len(mundo.notifs) == 1

    def test_dos_pedidos_de_la_misma_ot_no_se_pisan_entre_si(self, mundo):
        mundo.pedido(1)
        mundo.pedido(2, repuesto_nombre="Pin")
        mundo.f._otrep_avisar_tecnico(1, "solicitado", "validado", "Ana Bodega", None)
        mundo.f._otrep_avisar_tecnico(2, "solicitado", "validado", "Ana Bodega", None)
        assert len(mundo.notifs) == 2


class TestBarrido:
    def cargar(self, mundo, horas_habiles_atras, **kw):
        """Un pedido creado hace `horas_habiles_atras` horas hábiles, con `ahora` el lunes 19-oct 11:00 (2 h hábiles ya corridas ese día)."""
        mundo.ahora_chile = datetime(2026, 10, 19, 11, 0)
        # se retrocede por días hábiles completos de 9 h
        creada = datetime(2026, 10, 19, 11, 0)
        faltan = horas_habiles_atras
        while faltan > 0:
            ini = datetime.combine(creada.date(), datetime.min.time()).replace(hour=8)
            usable = (creada - ini).total_seconds() / 3600 if creada > ini else 0
            if usable >= faltan:
                creada -= timedelta(hours=faltan)
                break
            faltan -= usable
            creada = datetime.combine(creada.date() - timedelta(days=1), datetime.min.time()).replace(hour=17)
            while creada.weekday() >= 5:
                creada -= timedelta(days=1)
        return mundo.pedido(1, creada=creada, **kw)

    def corre(self, mundo, **kw):
        return mundo.f._otrep_barrido_respuesta(ahora=mundo.ahora_chile, **kw)

    def test_dentro_del_plazo_no_avisa_a_nadie(self, mundo):
        self.cargar(mundo, 1, responsable="Ana Bodega")
        out = self.corre(mundo)
        assert out["revisados"] == 1 and out["avisados"] == 0 and mundo.notifs == [] and mundo.eventos == []

    def test_a_las_4_horas_avisa_solo_al_dueno_y_deja_la_marca(self, mundo):
        self.cargar(mundo, 4.5, responsable="Ana Bodega")
        out = self.corre(mundo)
        assert out["avisados"] == 1 and out["escalados"] == 0
        (n,) = mundo.notifs
        assert n["destino"] == 5 and n["tipo"] == "repuesto_plazo" and "por vencer" in n["titulo"] and n["url"] == "/repuestos?solicitud=1#solicitudes"
        (m,) = [e for e in mundo.eventos if e["accion"] == "plazo"]
        assert m["detalle"].startswith("[n1]") and m["usuario"] == "sistema"

    def test_no_se_repite_en_la_pasada_siguiente(self, mundo):
        self.cargar(mundo, 4.5, responsable="Ana Bodega")
        self.corre(mundo)
        mundo.notifs.clear()
        mundo.ahora_chile += timedelta(minutes=10)
        out = self.corre(mundo)
        assert out["avisados"] == 0 and mundo.notifs == []

    def test_a_las_8_horas_escala_al_dueno_y_a_supervision_una_sola_vez(self, mundo):
        self.cargar(mundo, 8.5, responsable="Ana Bodega")
        out = self.corre(mundo)
        assert out["escalados"] == 1 and out["campanas"] == 3
        assert sorted(n["destino"] for n in mundo.notifs) == [2, 3, 5]             # dueño (5), supervisor (2) y admin (3); ni Daniel ni el técnico
        assert all("sin respuesta" in n["titulo"] and n["prioridad"] == "alta" for n in mundo.notifs)
        assert [e["detalle"][:4] for e in mundo.eventos if e["accion"] == "plazo"] == ["[n2]"]
        mundo.notifs.clear()
        mundo.ahora_chile += timedelta(minutes=10)
        assert self.corre(mundo)["escalados"] == 0 and mundo.notifs == []

    def test_si_el_nivel_1_ya_se_aviso_el_2_se_avisa_aparte(self, mundo):
        self.cargar(mundo, 4.5, responsable="Ana Bodega")
        self.corre(mundo)
        mundo.sol[1]["created_at"] = a_utc(datetime(2026, 10, 16, 8, 0))             # el mismo pedido, ahora mucho más viejo
        mundo.notifs.clear()
        out = self.corre(mundo)
        assert out["escalados"] == 1 and out["avisados"] == 0
        assert [e["detalle"][:4] for e in mundo.eventos if e["accion"] == "plazo"] == ["[n1]", "[n2]"]

    def test_no_avisa_de_pedidos_ya_respondidos_ni_manuales(self, mundo):
        self.cargar(mundo, 9, responsable="Ana Bodega", estado="validado")
        mundo.pedido(2, creada=datetime(2026, 10, 16, 8), creada_desde="manual", responsable="Ana Bodega")
        out = self.corre(mundo)
        assert out["revisados"] == 0 and mundo.notifs == []

    def test_un_pedido_reabierto_parte_de_cero(self, mundo):
        self.cargar(mundo, 9, responsable="Ana Bodega")
        mundo.eventos += [{"solicitud_id": 1, "accion": "estado", "de_estado": "solicitado", "a_estado": "validado", "usuario": "Ana",
                           "detalle": None, "created_at": a_utc(datetime(2026, 10, 16, 9))},
                          {"solicitud_id": 1, "accion": "estado", "de_estado": "validado", "a_estado": "solicitado", "usuario": "Ana",
                           "detalle": None, "created_at": a_utc(datetime(2026, 10, 19, 10))}]
        assert self.corre(mundo)["avisados"] == 0                       # reabierto hace 1 h hábil: todavía en plazo

    def test_un_pedido_sin_dueno_se_avisa_a_supervision(self, mundo):
        self.cargar(mundo, 4.5, visita_id=None)
        out = self.corre(mundo)
        assert out["avisados"] == 1 and sorted(n["destino"] for n in mundo.notifs) == [2, 3]

    def test_el_dueno_se_deduce_de_la_ot_si_el_pedido_no_lo_tiene(self, mundo):
        mundo.gestion[10] = gestion((5, "Ana Bodega", "ana@sphs.cl", "responsable"))
        self.cargar(mundo, 4.5)
        self.corre(mundo)
        assert [n["destino"] for n in mundo.notifs] == [5]

    def test_el_ticket_manda_sobre_la_solicitud(self, mundo):
        self.cargar(mundo, 4.5, responsable="Ana Bodega", ticket_asignado_a="Víctor")
        self.corre(mundo)
        assert [n["destino"] for n in mundo.notifs] == [2]

    def test_varios_pedidos_de_una_pasada_llegan_en_una_campana_por_persona(self, mundo):
        for i in (1, 2, 3):
            mundo.pedido(i, creada=datetime(2026, 10, 16, 8), responsable="Ana Bodega", repuesto_nombre="Pieza %d" % i)
        out = self.corre(mundo)
        assert out["escalados"] == 3
        ana = [n for n in mundo.notifs if n["destino"] == 5]
        assert len(ana) == 1 and ana[0]["titulo"].startswith("3 pedidos de repuesto sin respuesta")
        assert sorted(e["solicitud_id"] for e in mundo.eventos if e["accion"] == "plazo") == [1, 2, 3]

    def test_fuera_de_cobertura_no_hace_nada(self, mundo):
        self.cargar(mundo, 9, responsable="Ana Bodega")
        for ahora, motivo in ((datetime(2026, 10, 17, 11), "día no hábil"), (datetime(2026, 10, 19, 13, 30), "colación"),
                              (datetime(2026, 10, 19, 18, 0), "terminó"), (datetime(2026, 10, 12, 11), "día no hábil")):
            mundo.ahora_chile = ahora
            out = mundo.f._otrep_barrido_respuesta(ahora=ahora)
            assert motivo in out["omitido"] and out["revisados"] == 0, ahora
        assert mundo.notifs == [] and mundo.eventos == []

    def test_dry_solo_calcula(self, mundo):
        self.cargar(mundo, 9, responsable="Ana Bodega")
        out = self.corre(mundo, dry=True)
        assert out["pendientes"] == [{"solicitud": 1, "nivel": 2}] and mundo.notifs == [] and mundo.eventos == []

    def test_otro_barrido_en_curso_se_omite(self, mundo):
        self.cargar(mundo, 9, responsable="Ana Bodega")
        mundo.lock_ocupado = True
        out = self.corre(mundo)
        assert out["omitido"] == "otro barrido en curso" and mundo.notifs == [] and mundo.conn_cerrada

    def test_el_candado_se_libera_siempre(self, mundo):
        self.cargar(mundo, 9, responsable="Ana Bodega")
        self.corre(mundo)
        assert mundo.lock_tomado and mundo.lock_liberado and mundo.conn_cerrada

    def test_un_fallo_no_rompe_el_poll(self, mundo):
        self.cargar(mundo, 9, responsable="Ana Bodega")
        mundo.amb["_otrep_eventos_lote"] = lambda ids: (_ for _ in ()).throw(RuntimeError("base caída"))
        out = self.corre(mundo)
        assert out.get("error") is True and mundo.lock_liberado

    def test_si_la_campana_falla_no_se_marca_y_se_reintenta(self, mundo):
        self.cargar(mundo, 4.5, responsable="Ana Bodega")
        mundo.amb["_mant_notificar"] = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("sin campana"))
        self.corre(mundo)
        assert [e for e in mundo.eventos if e["accion"] == "plazo"] == []
        mundo.amb["_mant_notificar"] = mundo._notificar
        assert self.corre(mundo)["avisados"] == 1

    def test_con_equipo_fuera_de_servicio_el_aviso_llega_antes(self, mundo):
        self.cargar(mundo, 2.5, responsable="Ana Bodega", dejo_fuera_servicio=1)
        assert self.corre(mundo)["avisados"] == 1                       # aviso a la mitad del plazo de 4 h
        mundo.notifs.clear()
        mundo.sol[1]["created_at"] = a_utc(datetime(2026, 10, 19, 8, 0))     # 3 h hábiles después... ya es nivel 2 con fuera de servicio (4 h)
        mundo.sol[1]["creada_chile"] = datetime(2026, 10, 19, 8, 0)
        mundo.ahora_chile = datetime(2026, 10, 19, 12, 30)
        assert mundo.f._otrep_barrido_respuesta(ahora=mundo.ahora_chile)["escalados"] == 1
        assert any(n["prioridad"] == "urgente" for n in mundo.notifs)

    def test_ninguna_campana_del_barrido_habla_de_plata_ni_proveedores(self, mundo):
        self.cargar(mundo, 9, responsable="Ana Bodega")
        self.corre(mundo)
        for n in mundo.notifs:
            assert not re.search(r"proveedor|costo|\$|\bOC\b|factur", n["titulo"] + n["cuerpo"], re.I), n


class TestRelojEnLaCola:
    def fijar(self, mundo, ahora):
        mundo.ahora_chile = ahora
        mundo.amb["datetime"] = types.SimpleNamespace(utcnow=lambda: a_utc(ahora), combine=datetime.combine, min=datetime.min)

    def test_cada_pedido_de_la_pagina_lleva_su_reloj(self, mundo):
        self.fijar(mundo, datetime(2026, 10, 20, 12, 0))
        rows = [mundo.pedido(1, creada=datetime(2026, 10, 19, 8), estado="validado", validado_at=a_utc(datetime(2026, 10, 19, 10)), validado_por="Ana"),
                mundo.pedido(2, creada=datetime(2026, 10, 20, 9)),
                mundo.pedido(3, creada=datetime(2026, 10, 16, 8))]
        mundo.eventos.append({"solicitud_id": 1, "accion": "estado", "de_estado": "solicitado", "a_estado": "validado", "usuario": "Ana",
                              "detalle": None, "created_at": a_utc(datetime(2026, 10, 19, 10))})
        sols = [{"id": r["id"]} for r in rows]
        mundo.f._otrep_adjuntar_reloj(rows, sols)
        r1, r2, r3 = (s["reloj"] for s in sols)
        assert r1["estado"] == "respondida" and r1["respuesta"] == {"cuando": "19/10/2026 10:00", "quien": "Ana", "a_estado": "validado",
                                                                      "tiempo_h": 2.0, "en_plazo": True, "texto": "2 h hábiles"}
        assert (r2["estado"], r2["nivel"], r2["texto"]) == ("en_plazo", 0, "3 h hábiles") and r2["respuesta"] is None
        assert r3["estado"] == "vencida" and r3["nivel"] == 2 and r3["vence"] == "16/10/2026 17:00"
        assert r2["inicio"] == "20/10/2026 09:00"

    def test_sin_bitacora_usa_las_columnas(self, mundo):
        self.fijar(mundo, datetime(2026, 10, 20, 12, 0))
        rows = [mundo.pedido(1, creada=datetime(2026, 10, 19, 8), estado="pedido", pedido_at=a_utc(datetime(2026, 10, 19, 15)),
                             validado_at=a_utc(datetime(2026, 10, 19, 11)), validado_por="Bea", updated_at=a_utc(datetime(2026, 10, 19, 15)))]
        sols = [{"id": 1}]
        mundo.f._otrep_adjuntar_reloj(rows, sols)
        assert sols[0]["reloj"]["respuesta"]["quien"] == "Bea" and sols[0]["reloj"]["respuesta"]["tiempo_h"] == 3.0

    def test_el_equipo_fuera_de_servicio_de_la_cola_acorta_el_plazo(self, mundo):
        self.fijar(mundo, datetime(2026, 10, 19, 14, 0))
        rows = [mundo.pedido(1, creada=datetime(2026, 10, 19, 8), maquina_estado_capturado="fuera_servicio")]
        sols = [{"id": 1}]
        mundo.f._otrep_adjuntar_reloj(rows, sols)
        assert sols[0]["reloj"]["plazo_h"] == 4.0 and sols[0]["reloj"]["estado"] == "vencida"

    def test_un_error_deja_la_cola_sin_reloj_pero_viva(self, mundo):
        mundo.amb["_otrep_eventos_lote"] = lambda ids: (_ for _ in ()).throw(RuntimeError("base caída"))
        sols = [{"id": 1}]
        mundo.f._otrep_adjuntar_reloj([mundo.pedido(1)], sols)
        assert "reloj" not in sols[0] and any("reloj de la cola" in a for a in mundo.avisos)

    def test_la_cola_lo_llama_y_el_reloj_no_lleva_montos(self, mundo):
        assert "_otrep_adjuntar_reloj(rows, sols)" in FC.funcion_top("app.py", "repstock_solicitudes_ot_listar")
        self.fijar(mundo, datetime(2026, 10, 20, 12, 0))
        sols = [{"id": 1}]
        mundo.f._otrep_adjuntar_reloj([mundo.pedido(1, creada=datetime(2026, 10, 16, 8))], sols)
        assert not re.search(r"costo|monto|proveedor|precio", json.dumps(sols[0]["reloj"]), re.I)


class TestEndpointTiempos:
    def llamar(self, mundo, dias=None):
        mundo.amb["request"].args = {"dias": dias} if dias else {}
        return mundo.f.repstock_tiempos_respuesta()

    def test_cifras_de_los_ultimos_dias(self, mundo):
        mundo.ahora_chile = datetime(2026, 10, 20, 12, 0)
        mundo.amb["datetime"] = types.SimpleNamespace(utcnow=lambda: a_utc(mundo.ahora_chile), combine=datetime.combine, min=datetime.min)
        mundo.pedido(1, creada=datetime(2026, 10, 19, 8), responsable="Ana Bodega", estado="validado")
        mundo.eventos.append({"solicitud_id": 1, "accion": "estado", "de_estado": "solicitado", "a_estado": "validado", "usuario": "Ana",
                              "detalle": None, "created_at": a_utc(datetime(2026, 10, 19, 10))})
        mundo.pedido(2, creada=datetime(2026, 10, 16, 8), responsable="Víctor")                       # sin responder y vencido
        r = self.llamar(mundo)
        assert r["ok"] is True and r["dias"] == 30
        g = r["general"]
        assert (g["pedidos"], g["respondidos"], g["vencidos"], g["promedio_h"], g["mediana_h"], g["pct_en_plazo"]) == (2, 1, 1, 2.0, 2.0, 50)
        assert {f["dueno"] for f in r["por_dueno"]} == {"Ana Bodega", "Víctor"}
        assert r["vencidos"][0]["id"] == 2 and r["plazo"]["escala_h"] == 8.0

    def test_no_trae_montos_proveedores_ni_costos(self, mundo):
        mundo.ahora_chile = datetime(2026, 10, 20, 12, 0)
        mundo.amb["datetime"] = types.SimpleNamespace(utcnow=lambda: a_utc(mundo.ahora_chile), combine=datetime.combine, min=datetime.min)
        mundo.pedido(1, creada=datetime(2026, 10, 19, 8), responsable="Ana Bodega")
        assert not re.search(r"proveedor|costo|monto|precio|oc_numero", json.dumps(self.llamar(mundo), default=str), re.I)

    def test_la_ruta_es_solo_de_gestion_y_no_para_tecnicos(self):
        i = FC.texto("app.py").index('@app.route("/repuestos/api/tiempos-respuesta"')
        cabecera = FC.texto("app.py")[i:i + 260]
        assert '@_otrep_gestion_required' in cabecera and '@_no_tecnico' in cabecera and 'methods=["GET"]' in cabecera
        assert 'def repstock_tiempos_respuesta' in cabecera


# ═══════════════════════════════════════════════════════════════════════════════════════════════
#  C. EL CABLEADO Y LAS REGLAS DEL PROYECTO
# ═══════════════════════════════════════════════════════════════════════════════════════════════
class TestCableado:
    def test_el_evento_dispara_el_dueno_el_aviso_y_el_aviso_al_tecnico(self):
        src = FC.funcion_top("app.py", "_otrep_evento")
        assert src.index("_otrep_referenciar(sid)") < src.index("_otrep_asignar_dueno(sid)") < src.index("_otrep_avisar_nueva(sid, usuario)")
        assert 'elif accion == "estado" and de_estado and a_estado and de_estado != a_estado' in src
        assert "_otrep_avisar_tecnico(sid, de_estado, a_estado, usuario, detalle)" in src

    def test_el_aviso_de_siempre_sigue_para_lo_que_no_es_de_una_ot(self):
        src = FC.funcion_top("app.py", "_otrep_avisar_nueva")
        assert '("superadmin", "admin")' in src and '!= "tecnico"' in src and '"repuesto_nuevo"' in src
        assert "_otrep_avisar_nueva_ot(s, usuario)" in src and "_send_ilus_email" not in src

    def test_el_cambio_de_estado_no_se_toco(self):
        """Los candados (foto, ubicación, proveedor, concurrencia) siguen intactos: el hook vive solo en `_otrep_evento`."""
        src = FC.funcion_top("app.py", "_otrep_cambiar_estado")
        assert "_otrep_avisar_tecnico" not in src and "_otrep_avisar_nueva" not in src and "_rresp" not in src

    def test_el_ticket_nace_con_dueno(self):
        src = FC.funcion_top("app.py", "_otrep_ticket_para_ot")
        assert src.count("_otrep_ticket_dueno_asegurar(") == 2          # al reutilizar el ticket y al crearlo

    def test_la_fecha_de_la_compra_avisa_al_tecnico(self):
        src = FC.funcion_top("app.py", "repstock_compra_estado")
        assert "_otrep_avisar_eta_compra(cid)" in src and 'nuevo != "cancelada"' in src

    def test_el_cron_cuelga_el_barrido_sin_poder_romper_el_poll(self):
        src = FC.funcion_top("app.py", "tr_cron_simpliroute_poll")
        i = src.index("_otrep_barrido_respuesta(")
        previo = src[max(0, i - 400):i]
        assert "if not dry" in previo and "try:" in previo
        assert "except Exception as _e_rr" in src[i:i + 700] and "repuestos_respuesta" in src[i:i + 700]
        assert src.index("_desinst_barrido()") < i < src.index("return jsonify(res)")

    def test_el_barrido_solo_actua_en_cobertura_y_con_candado(self):
        src = FC.funcion_top("app.py", "_otrep_barrido_respuesta")
        assert "_rresp.en_cobertura(" in src and "GET_LOCK" in src and "RELEASE_LOCK" in src
        assert "marca_detalle" in src and "nivel_pendiente" in src

    def test_el_enum_de_la_campana_tiene_el_tipo_nuevo_al_final_de_las_dos_listas(self):
        app = FC.texto("app.py")
        enums = re.findall(r"ALTER TABLE mant_notificaciones MODIFY COLUMN tipo \"\s*\"\s*ENUM\((.*?)\) DEFAULT 'otro'", app, re.S)
        assert len(enums) == 2
        for e in enums:
            valores = re.findall(r"'([a-z_]+)'", e)
            # 2026-10-27 (Tickets 2.0): detrás vino «tk_sla»; lo anterior no se reordena.
            # 2026-10-29 (logística de las guías): detrás vino «ot_guia_diferencia».
            assert valores[-4:] == ["repuesto_nuevo", "repuesto_tecnico", "tk_sla", "ot_guia_diferencia"], \
                "al FINAL: nada se reordena (REGLA #18)"
            assert valores.count("repuesto_tecnico") == 1 and "repuesto_plazo" in valores

    def test_todo_tipo_de_campana_que_usa_el_bloque_cabe_en_el_enum(self):
        app = FC.texto("app.py")
        i = app.index("def _otrep_correos_activos")
        bloque = app[i:app.index('@app.route("/repuestos/api/solicitudes-ot/config"', i)]
        usados = set(re.findall(r'_mant_notificar\(\s*[^,()]+,\s*"([a-z_]+)"', bloque))
        assert usados == {"repuesto_nuevo", "repuesto_tecnico", "repuesto_plazo"}
        e = re.findall(r"ALTER TABLE mant_notificaciones MODIFY COLUMN tipo \"\s*\"\s*ENUM\((.*?)\) DEFAULT 'otro'", app, re.S)[0]
        assert usados <= set(re.findall(r"'([a-z_]+)'", e))

    def test_la_campana_tiene_icono_para_los_tres_tipos(self):
        html = FC.texto("templates/mantenciones/notificaciones.html")
        for t in ("repuesto_nuevo", "repuesto_plazo", "repuesto_tecnico"):
            assert re.search(t + r":\s*\{ icon:", html), t


class TestRegistroDeCorreos:
    CLAVES = {"rep_nueva": ("comunicacion_interna", "bodega"), "rep_vencida": ("comunicacion_interna", "responsable_ot"),
              "rep_lista_tecnico": ("ot_tecnicos", "tecnico")}

    def test_los_tres_estan_registrados_en_el_grupo_de_repuestos(self):
        for clave, (llave, dest) in self.CLAVES.items():
            c = R.por_clave(clave)
            assert c and c["modulo_ui"] == "repuestos" and c["llave"] == llave and c["destinatario"] == dest, clave
            assert c["tipo"] == "ilus" and not c["protegido"]

    def test_nacen_apagados_por_configuracion_y_no_se_encienden_desde_la_pantalla(self):
        for clave in self.CLAVES:
            c = R.por_clave(clave)
            sin = R.estado_efectivo(c, environ={})
            assert sin["efectivo"] == "config" and sin["puede_encender"] is False, clave
            assert R.estado_efectivo(c, environ={"REPUESTOS_CORREOS_ACTIVOS": "1"})["efectivo"] == "sale", clave
            assert any(x["variable"] == "REPUESTOS_CORREOS_ACTIVOS" for x in sin["condiciones"])

    def test_la_variable_no_esta_en_el_despliegue(self):
        """REGLA #22/#33: solo Daniel la enciende con su «sí»; no puede quedar escrita en deploy.yml ni en ningún archivo del repositorio."""
        import os
        raiz = FC.RAIZ
        for ruta in (".github/workflows/deploy.yml", "Dockerfile", "config.py", "nixpacks.toml", "Procfile"):
            p = os.path.join(raiz, ruta)
            if os.path.exists(p):
                assert "REPUESTOS_CORREOS_ACTIVOS" not in open(p, encoding="utf-8", errors="replace").read(), ruta

    def test_las_llamadas_de_envio_usan_la_clave_y_la_llave_del_registro(self):
        app = FC.texto("app.py")
        i = app.index("def _otrep_enviar_correo")
        bloque = app[i:app.index('@app.route("/repuestos/api/tiempos-respuesta"', i)]
        llamadas = re.findall(r'_otrep_enviar_correo\(\s*"(rep_[a-z_]+)",\s*"([a-z_]+)"', bloque)
        assert {c for c, _ in llamadas} == set(self.CLAVES)
        for clave, llave in llamadas:
            assert R.por_clave(clave)["llave"] == llave, clave
        assert "_send_ilus_email(to, asunto, html, evento=clave, modulo=modulo, asincrono=True, correo=clave, ref=ref)" in bloque

    def test_el_registro_se_valida_solo(self):
        llaves = tuple(FC.valor_literal("app.py", "_MODULOS_KILLSWITCH_VALIDOS"))
        assert R.validar(llaves) == []

    def test_los_correos_no_llegan_jamas_al_cliente(self):
        app = FC.texto("app.py")
        i = app.index("def _otrep_enviar_correo")
        bloque = app[i:app.index('@app.route("/repuestos/api/tiempos-respuesta"', i)]
        # los únicos destinatarios son gestión, bodega, supervisión, el dueño y el técnico que pidió: nunca columnas del cliente
        assert not re.search(r"contacto_email|cliente_email|\.email\b", bloque)


class TestConfidencialidad:
    def test_los_avisos_al_tecnico_no_tocan_proveedor_ni_costo(self):
        src = FC.funcion_top("app.py", "_otrep_avisar_tecnico_clave")
        consulta = src[src.index("SELECT"):src.index("(sid,))")]
        assert not re.search(r"proveedor|costo|oc_numero|nota_gestion|monto", consulta, re.I)
        assert "nota_gestion" not in src

    def test_los_textos_del_tecnico_se_arman_solo_con_el_modulo_puro(self):
        src = FC.funcion_top("app.py", "_otrep_avisar_tecnico_clave")
        assert "_rresp.texto_tecnico(" in src and 'f"' not in src.split("_rresp.texto_tecnico(")[1].split("titulo, cuerpo, prio")[0]

    def test_el_endpoint_de_tiempos_no_consulta_dinero(self):
        src = FC.funcion_top("app.py", "repstock_tiempos_respuesta")
        assert not re.search(r"costo|monto|proveedor|oc_numero|precio", src, re.I)

    def test_el_barrido_y_los_avisos_no_le_escriben_al_cliente(self):
        app = FC.texto("app.py")
        i = app.index("def _otrep_correos_activos")
        bloque = app[i:app.index('@app.route("/repuestos/api/solicitudes-ot/config"', i)]
        # el nombre del cliente viaja SOLO en el texto interno; ninguna dirección ni teléfono del cliente se lee en este bloque
        assert not re.search(r"contacto_email|contacto_tel|email_cliente|notify_cliente|whatsapp|twilio", bloque, re.I)

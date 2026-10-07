# -*- coding: utf-8 -*-
"""Tiempos de preparación según las OT de Check (retiros_tiempos.py). Datos de los casos reales del 02 y 05-oct-2026."""
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import retiros_tiempos as rt  # noqa: E402

JOR = (450, 1200)


def ot(n, tipo, ini, fin, estado="FINALIZADA", asign=None):
    momentos = []
    if asign:
        momentos.append({"campo": "feAsignacion", "etiqueta": "Fecha asignación", "valor": asign})
    momentos.append({"campo": "feInicioOT", "etiqueta": "Inicio", "valor": ini})
    if fin:
        momentos.append({"campo": "fechaFin", "etiqueta": "Fin", "valor": fin})
    return {"ot": n, "tipo": tipo, "estado": estado, "inicio": ini, "fin": fin, "momentos": momentos}


GERD = [ot("PCKM000238742", "PICKING MATERIAL", "05/10/2026 08:11", "05/10/2026 08:26"),
        ot("CSAL000016307", "CONTROL SALIDA / INTEGRACIONES", "05/10/2026 08:56", "05/10/2026 08:56")]


def test_caso_real_gerd():
    a = rt.analizar(GERD, cita="05/10/2026 11:00", jor=JOR)
    assert a["trabajo_min"] == 15            # 15 de picking + 0 de control de salida
    assert a["efectivo_min"] == 15
    assert a["principio_fin_min"] == 45
    assert a["pausas_min"] == 30
    assert a["listo_esperando_min"] == 30
    assert a["salida"] == "05/10/2026 08:56"
    assert a["vs_cita_min"] == 124           # salió 2 h 4 min antes de la cita
    assert a["pausas"] == [{"desde": "05/10/2026 08:26", "hasta": "05/10/2026 08:56", "min": 30, "reloj_min": 30, "noche": False}]
    assert not a["en_curso"] and not a["sin_noches"]


def test_asignada_en_la_tarde_y_terminada_al_dia_siguiente_no_cuenta_la_noche():
    # Daniel: «la puedo asignar en la tarde y el operario la termina mañana»
    o = [ot("PCK1", "PICKING", "06/10/2026 16:00", "07/10/2026 09:00", asign="06/10/2026 15:30")]
    a = rt.analizar(o, jor=JOR)
    assert a["reloj_min"] == 17 * 60
    assert a["trabajo_min"] == 4 * 60 + 90   # 16:00–20:00 + 07:30–09:00
    assert a["principio_fin_min"] == 330
    assert a["sin_noches"] is True
    assert a["espera_asignacion_min"] == 30


def test_viernes_a_lunes_no_cuenta_el_fin_de_semana():
    o = [ot("PCK1", "PICKING", "09/10/2026 19:00", "12/10/2026 08:00")]   # viernes 19:00 → lunes 08:00
    a = rt.analizar(o, jor=JOR)
    assert a["trabajo_min"] == 60 + 30


def test_ot_que_se_pisan_cuentan_una_vez_en_la_preparacion_efectiva():
    o = [ot("A", "PICKING", "06/10/2026 10:00", "06/10/2026 10:30"), ot("B", "PICKING", "06/10/2026 10:10", "06/10/2026 10:40")]
    a = rt.analizar(o, jor=JOR)
    assert a["trabajo_min"] == 60            # suma de cada OT (tiempo-persona)
    assert a["efectivo_min"] == 40           # lo que realmente tomó
    assert a["pausas_min"] == 0 and a["pausas"] == []


def test_mismo_dia_fuera_de_jornada_se_cuenta_el_reloj():
    o = [ot("A", "PICKING", "06/10/2026 07:00", "06/10/2026 07:20")]
    assert rt.analizar(o, jor=JOR)["trabajo_min"] == 20


def test_ot_en_curso_corre_hasta_ahora():
    o = [ot("A", "PICKING", "06/10/2026 10:00", None, estado="EN PROCESO")]
    a = rt.analizar(o, ahora=datetime(2026, 10, 6, 10, 25), jor=JOR)
    assert a["en_curso"] and a["trabajo_min"] == 25 and a["fin"] is None


def test_sin_horas_devuelve_none():
    assert rt.analizar([{"ot": "X", "tipo": "PICKING", "estado": "", "momentos": []}], jor=JOR) is None
    assert rt.analizar([], jor=JOR) is None
    assert rt.analizar_retiro([], jor=JOR) is None


def test_analizar_retiro_junta_documentos():
    docs = [{"rotulo": "FCV 10953", "ots": [ot("481516", "PICKING", "02/10/2026 15:32", "02/10/2026 15:40", estado="TERMINADA")]},
            {"rotulo": "BLV 23732", "ots": GERD}]
    a = rt.analizar_retiro(docs, ahora=datetime(2026, 10, 6, 12, 0), jor=JOR)
    assert [d["documento"] for d in a["documentos"]] == ["FCV 10953", "BLV 23732"]
    assert a["documentos"][0]["trabajo_min"] == 8
    assert a["trabajo_min"] == 23
    assert a["calculado"] == "06/10/2026 12:00"
    assert "jornada de bodega 07:30–20:00" in a["criterio"]


def test_jornada_por_variable(monkeypatch):
    monkeypatch.setenv("RETIROS_JORNADA_BODEGA", "08:00-18:00")
    assert rt.jornada() == (480, 1080)
    monkeypatch.setenv("RETIROS_JORNADA_BODEGA", "basura")
    assert rt.jornada() == (450, 1200)


def test_minutos_por_producto_se_reparten_por_unidades():
    o = ot("481516", "PICKING", "02/10/2026 15:32", "02/10/2026 15:40", estado="TERMINADA")
    o["lineas"] = [{"sku": "FZADI0275", "descripcion": "Set Discos", "ejecutado": 3, "solicitado": 3},
                   {"sku": "BANCO01", "descripcion": "Banco plano", "ejecutado": 1, "solicitado": 1}]
    a = rt.analizar([o], jor=JOR)
    assert a["picking_min"] == 8 and a["picking_unidades"] == 4 and a["picking_lineas"] == 2
    assert a["min_por_unidad"] == 2.0
    assert [(p["sku"], p["unidades"], p["min"]) for p in a["productos"]] == [("FZADI0275", 3, 6.0), ("BANCO01", 1, 2.0)]


def test_control_de_salida_no_cuenta_como_picking_por_producto():
    a = rt.analizar(GERD, jor=JOR)
    assert a["picking_min"] == 15 and a["productos"] == [] and a["min_por_unidad"] is None

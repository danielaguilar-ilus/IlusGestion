"""Tickets 2.0 · SLA de respuesta: 4 h hábiles por mensaje del cliente
(horario de Retiros: lun-vie 08-17, colación 13-14, sin feriados)."""
from datetime import datetime, timedelta
import random

import tickets_sla as s

COB = {"desde": 8 * 60, "hasta": 17 * 60, "col_d": 13 * 60, "col_h": 14 * 60}


def test_horas_habiles_saltan_colacion_noche_y_fin_de_semana():
    # mar 27/10/2026 12:00 -> 15:00 = 2 h hábiles (12-13 y 14-15)
    assert s.horas_habiles(datetime(2026, 10, 27, 12), datetime(2026, 10, 27, 15), cob=COB) == 2.0
    # vie 30/10 16:00 -> lun 02/11 09:00 = 1 + 1 = 2 h (sáb/dom no cuentan)
    assert s.horas_habiles(datetime(2026, 10, 30, 16), datetime(2026, 11, 2, 9), cob=COB) == 2.0


def test_niveles_del_semaforo():
    ahora = datetime(2026, 10, 27, 16, 0)            # martes 16:00
    assert s.evaluar(None, ahora, "open", cob=COB)["nivel"] == "respondido"
    assert s.evaluar(datetime(2026, 10, 27, 15), ahora, "closed", cob=COB)["nivel"] == "cerrado"
    assert s.evaluar(datetime(2026, 10, 27, 15), ahora, "open", cob=COB)["nivel"] == "ok"        # 1 h
    a = s.evaluar(datetime(2026, 10, 27, 12), ahora, "open", cob=COB)                              # 3 h
    assert a["nivel"] == "atencion" and a["semaforo"] == "ambar"
    v = s.evaluar(datetime(2026, 10, 27, 9), ahora, "open", cob=COB)                               # 6 h
    assert v["nivel"] == "vencido" and v["semaforo"] == "rojo" and "vencido" in v["sub"].lower()


def test_vence_respeta_horario():
    # llega martes 15:30 -> 4 h hábiles = 1,5 h martes + 2,5 h miércoles -> mié 10:30
    r = s.evaluar(datetime(2026, 10, 27, 15, 30), datetime(2026, 10, 27, 15, 45), "open", cob=COB)
    assert r["vence"] == datetime(2026, 10, 28, 10, 30)


def test_fuera_de_horario_el_reloj_esta_en_pausa():
    r = s.evaluar(datetime(2026, 10, 27, 19), datetime(2026, 10, 27, 21), "open", cob=COB)
    assert r["pausa"] and r["espera_h"] == 0 and r["nivel"] == "ok"


def test_corte_coincide_con_evaluar_uno_por_uno():
    """La condición SQL `espera_desde <= corte` da exactamente lo mismo que
    evaluar cada ticket (lo que permite filtrar 100.000 tickets con índice)."""
    rnd = random.Random(5)
    for _ in range(40):
        ahora = datetime(2026, 10, 19) + timedelta(minutes=rnd.randint(0, 9 * 24 * 60))
        c = s.corte(ahora, 4, cob=COB)
        for _ in range(60):
            esp = ahora - timedelta(minutes=rnd.randint(1, 5 * 24 * 60))
            vencido = s.evaluar(esp, ahora, "open", cob=COB)["nivel"] == "vencido"
            # tolerancia de 1 s por la precisión de la búsqueda binaria
            if abs((esp - c).total_seconds()) > 1:
                assert vencido == (esp <= c), (ahora, esp, c)


def test_conversion_utc_chile_ida_y_vuelta():
    dt = datetime(2026, 10, 27, 20, 15)
    assert s.chile_a_utc(s.utc_a_chile(dt)) == dt

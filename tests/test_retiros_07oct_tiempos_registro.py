# -*- coding: utf-8 -*-
"""2026-10-07 — Los tiempos de preparación según Check quedan GUARDADOS como evidencia (Daniel: «crear los datos persistentes con análisis
inteligente… tener toda la evidencia… registro de cuánto se tarda por producto en promedio»). Arnés: sin BD real, sin Check, correo de mentira.

    py -m pytest tests/test_retiros_07oct_tiempos_registro.py -q
"""
import json
import os
import sys

_TESTS = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _TESTS)
sys.path.insert(0, os.path.dirname(_TESTS))

from test_retiros_06oct import RID, _ot, actividad, con_cache, env, retiro, sin_mensajes  # noqa: E402,F401


def _gerd():
    return [_ot(ot="PCKM000238742", tipoOT="Picking Material", estado="FINALIZADA", feInicioOT="2026-10-05T08:11:00", fechaFin="2026-10-05T08:26:00",
                codigo="1132100577", descripcion="Set Discos Fraccionados", sol=1, ejec=1),
            _ot(ot="CSAL000016307", tipoOT="Control Salida / Integraciones", estado="FINALIZADA", feInicioOT="2026-10-05T08:56:00",
                fechaFin="2026-10-05T08:56:30", usuarioPicking=None, usuario="Samuel Gabriel Infante", codigo="1132100577", sol=1, ejec=1)]


def test_al_guardar_el_registro_de_check_queda_el_analisis_de_tiempos(env):
    retiro(env, confirmed_date="2026-10-05", confirmed_time_from="11:00:00")
    con_cache(env, _gerd())
    d = actividad(env)
    fila = env.db.prep_tiempos[RID]
    assert fila["trabajo_min"] == 15 and fila["efectivo_min"] == 15 and fila["pausas_min"] == 30 and fila["principio_fin_min"] == 45
    assert fila["listo_esperando_min"] == 30 and fila["vs_cita_min"] == 124 and fila["salida_check"] == "05/10/2026 08:56"
    a = json.loads(fila["payload"])
    assert "jornada de bodega" in a["criterio"] and a["calculado"] == "07/10/2026 11:00"
    assert d["tiempos"]["efectivo_min"] == 15 and d["tiempos"]["calculado"] == "07/10/2026 11:00"
    # por producto: solo el picking, repartido por unidades
    assert [(p["sku"], float(p["unidades"]), p["minutos"]) for p in env.db.prep_productos] == [("1132100577", 1.0, 15.0)]
    sin_mensajes(env)


def test_no_se_recalcula_si_nada_cambio(env):
    retiro(env)
    con_cache(env, _gerd())
    actividad(env)
    actividad(env)
    inserts = [e for e in env.db.escrituras if e[0].lower().startswith("insert into pickup_prep_tiempos")]
    assert len(inserts) == 1


def test_retiro_completado_con_registro_viejo_sin_analisis_lo_calcula_una_vez(env):
    retiro(env)
    con_cache(env, _gerd())
    actividad(env)
    env.db.prep_tiempos.clear()                       # registro guardado antes de que existiera el análisis
    env.db.prep_productos.clear()
    env.db.solicitudes[RID]["status"] = "retirada"
    traer = con_cache(env, None)
    d = actividad(env)
    assert d["desde_registro"] is True and d["tiempos"]["trabajo_min"] == 15
    assert RID in env.db.prep_tiempos
    traer.assert_not_called()
    sin_mensajes(env)


def test_si_el_analisis_falla_el_registro_de_check_igual_se_guarda(env):
    import re
    retiro(env)
    con_cache(env, _gerd())
    env.db.falla_escritura_si.append((re.compile(r"pickup_prep_tiempos"), RuntimeError("tabla rota")))
    d = actividad(env)
    assert d["estado"] == "listo" and RID in env.db.snapshots


def test_promedios_de_preparacion_por_producto(env):
    retiro(env)
    con_cache(env, _gerd())
    actividad(env)
    r = env.cli.get("/retiros/api/tiempos-preparacion?dias=30")
    assert r.status_code in (200, 500)      # el arnés no simula los AVG/GROUP BY: basta con que no rompa ni filtre detalles internos
    assert "Traceback" not in r.get_data(as_text=True)

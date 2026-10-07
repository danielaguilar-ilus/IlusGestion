# -*- coding: utf-8 -*-
"""2026-10-07 — Panel de KPIs de Retiros para gerencia: fila «Gestión operacional» (primera respuesta, preparación según Check, listo
antes de la cita, reacción de bodega) y tasa de confirmación sobre solicitudes ya decididas. Solo plantilla y SQL (sin BD real).

    py -m pytest tests/test_retiros_07oct_kpis.py -q
"""
import os
import re

import jinja2

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _leer(*p):
    with open(os.path.join(RAIZ, *p), encoding="utf-8") as f:
        return f.read()


def _render(ope=None, **kpis_extra):
    env = jinja2.Environment(loader=jinja2.DictLoader({"k": _leer("templates", "retiros", "_monitor_kpis.html")}), autoescape=True)
    env.globals["url_for"] = lambda ep, **kw: "/" + ep
    kpis = {"semana": 3, "tasa_conf": 80, "ciclo_h": 20.0, "msgs": 4, "tasa_pend": 2, "tasa_base": 10}
    kpis.update(kpis_extra)
    return env.get_template("k").render(kpis=kpis, stats={}, day={}, filtros={"view": "monitor"}, mon={"ok": True},
                                        kx={"ope": ope} if ope is not None else {})


def _txt(html):
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html))


def test_gestion_operacional_con_datos():
    t = _txt(_render({"resp": {"prom": "1 h 10 min hábiles", "mediana": "45 min hábiles", "en_sla": 85, "n": 20, "sin": 1,
                               "sla": "2 h hábiles", "nivel": "verde"},
                      "prep": {"n": 12, "efectivo": 18, "pausas": 25, "min_unidad": "3,8"},
                      "listo": {"pct": 92, "n": 12, "listos": 11, "nivel": "verde"},
                      "asig": {"prom": 34, "n": 9}}))
    assert "Primera respuesta 30d" in t and "1 h 10 min hábiles" in t and "85% dentro de 2 h hábiles" in t and "1 sin gestión aún" in t
    assert "Preparación en bodega" in t and "18 min" in t and "Picking: 3,8 min por unidad" in t
    assert "Listo antes de la cita" in t and "92 %" in t.replace("92%", "92 %") and "11 de 12 con cita" in t
    assert "Reacción de bodega" in t and "34 min" in t


def test_sin_datos_dice_por_que_y_no_inventa_ceros():
    t = _txt(_render({}))
    assert "Sin solicitudes gestionadas en 30 días" in t
    assert "Se llena solo al abrir las fichas con OT de Check" in t
    assert "Aún sin retiros con cita y picking en Check" in t
    assert "Check aún no informa asignaciones" in t


def test_las_tarjetas_de_siempre_siguen():
    t = _txt(_render({}))
    for k in ("Retiros esta semana", "Tasa confirmación 30d", "Ciclo promedio 90d", "En preparación", "Por revisar", "Mensajes sin leer",
              "Retiros hoy", "Peso real", "Volumen"):
        assert k in t, k


def test_tasa_explica_su_base():
    t = _txt(_render({}))
    assert "Sobre 10 ya decididas · 2 aún en gestión" in t


def test_la_tasa_excluye_las_solicitudes_que_aun_se_gestionan():
    pm = _leer("pickups_module.py")
    assert "_ABIERTAS_SQL = \"'solicitud_recibida','en_revision','informacion_incompleta','propuesta_enviada','esperando_cliente'\"" in pm
    assert pm.count("status NOT IN ({_ABIERTAS_SQL})") == 2


def test_el_reloj_del_sla_usa_la_cobertura_no_09_18():
    pm = _leer("pickups_module.py")
    i = pm.index("def _cc_horas_habiles(")
    cuerpo = pm[i:i + 1500]
    assert "_cc_ventanas()" in cuerpo and "replace(hour=9)" not in cuerpo and "replace(hour=18)" not in cuerpo
    assert '"ventanas": _cc_ventanas()' in pm

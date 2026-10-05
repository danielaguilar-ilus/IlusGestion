# -*- coding: utf-8 -*-
"""Campaña de mantención a clientes de instalación (Daniel 2026-10-04): toques a los 15 días y a los
3 meses, ticket sin asignar + correo con plantilla editable. Corre sin BD ni correo: se extraen de
app.py solo las funciones de la campaña y se ejecutan con dobles.

    py -m pytest tests/test_prospectos_oferta.py -q
"""
import datetime as dt
import os
from unittest import mock

import pytest

_APP = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "app.py")
_INICIO = "_PROSP_TOQUES = {1: 15, 2: 90}"
_FIN = '@app.route("/mantenciones/cron/prospectos-oferta"'
_FUENTE = []


def _cargar():
    # app.py pesa ~7 MB y parsearlo entero tarda ~80 s: se corta SOLO el bloque de la campaña
    # (constantes + funciones, contiguo en el archivo) y se ejecuta con dobles.
    if not _FUENTE:
        src = open(_APP, encoding="utf-8").read()
        a, b = src.index(_INICIO), src.index(_FIN)
        _FUENTE.append(src[a:b])
    ns = {"datetime": dt.datetime, "timedelta": dt.timedelta, "os": os, "print": print}
    exec(_FUENTE[0], ns)
    return ns


class _Mundo:
    """Doble de BD/correo: guarda lo que el barrido hace."""
    def __init__(self, cands, emails=("c@x.cl",), hechos=()):
        self.cands, self.emails = cands, list(emails)
        self.toques = {(c, t) for c, t in hechos}
        self.enviados, self.tickets, self.logs = [], [], []

    def conectar(self, ns, ahora):
        class _DT(dt.datetime):
            @classmethod
            def now(cls, tz=None):
                return ahora
        ns["datetime"] = _DT
        ns["mysql_fetchall"] = self._fetchall
        ns["mysql_execute"] = lambda q, p=(): self.toques.add((p[0], p[1])) if "INSERT IGNORE INTO mant_prospecto_toques" in q else None
        ns["mysql_execute_returning_rowcount"] = self._claim
        ns["_ensure_mant_prospecto_toques"] = lambda: None
        ns["_mant_get_cliente_emails"] = lambda cid: self.emails
        ns["comm_is_enabled"] = lambda canal: True
        ns["_render_comm_template"] = lambda *a, **k: ("Asunto", "<p>Cuerpo</p>")
        ns["_comm_render_email_document"] = lambda a, b, subtitle="": b
        ns["_brand_subject"] = lambda t: t
        ns["_send_ilus_email"] = lambda to, subj, html, **k: self.enviados.append(to) or True
        ns["_prospecto_crear_ticket"] = lambda cli, toque, ref, txt: self.tickets.append((cli["id"], toque, txt)) or 900 + len(self.tickets)
        ns["_mant_log"] = lambda *a, **k: self.logs.append(a)

    def _fetchall(self, q, p=()):
        if "FROM mant_prospecto_toques" in q:
            return [{"toque": t} for (c, t) in self.toques if c == p[0]]
        return self.cands

    def _claim(self, q, p=()):
        if (p[0], p[1]) in self.toques:
            return 0
        self.toques.add((p[0], p[1]))
        return 1


@pytest.fixture(autouse=True)
def _auto_encendida(monkeypatch):
    """La campaña viene APAGADA por defecto; las pruebas la encienden explícitamente."""
    monkeypatch.setenv("PROSPECTOS_OFERTA_AUTO", "1")


def test_viene_apagada_por_defecto(monkeypatch):
    monkeypatch.delenv("PROSPECTOS_OFERTA_AUTO", raising=False)
    ns = _cargar()
    m = _Mundo([_cli(1, 16, LUNES_10H)])
    m.conectar(ns, LUNES_10H)
    assert ns["_prospectos_oferta_barrido"]().get("apagado") and m.tickets == [] and m.enviados == []


def _cli(cid, dias_atras, ahora):
    return {"id": cid, "razon_social": f"Cliente {cid}", "rut": "1-9", "contacto_nombre": "Ana",
            "contacto_email": "c@x.cl", "email_empresa": None, "contacto_tel": None,
            "tel_empresa": None, "direccion": "x", "comuna": "y",
            "ref": (ahora - dt.timedelta(days=dias_atras)).date()}


LUNES_10H = dt.datetime(2026, 10, 5, 10, 0)


def _correr(cands, **kw):
    ns = _cargar()
    m = _Mundo(cands, **kw)
    m.conectar(ns, kw.pop("ahora", LUNES_10H) if False else LUNES_10H)
    return m, ns["_prospectos_oferta_barrido"](max_n=15)


def test_15_dias_crea_ticket_y_manda_correo():
    m, res = _correr([_cli(1, 16, LUNES_10H)])
    assert [t[:2] for t in m.tickets] == [(1, 1)]
    assert m.enviados == ["c@x.cl"] and res["correos"] == 1


def test_antes_de_15_dias_no_hace_nada():
    m, _ = _correr([_cli(1, 10, LUNES_10H)])
    assert m.tickets == [] and m.enviados == []


def test_a_los_3_meses_es_el_toque_2_y_omite_el_1():
    m, _ = _correr([_cli(1, 91, LUNES_10H)])
    assert [t[:2] for t in m.tickets] == [(1, 2)]
    assert (1, 1) in m.toques          # el toque 1 quedó marcado omitido, no se manda doble


def test_toque_muy_vencido_crea_ticket_pero_no_manda_correo_solo():
    m, _ = _correr([_cli(1, 15 + 40, LUNES_10H)])   # el toque 1 venció hace 40 días; aún no llega el 2
    assert len(m.tickets) == 1 and m.enviados == []
    assert "NO salió solo" in m.tickets[0][2]


def test_no_repite_un_toque_ya_hecho():
    m, _ = _correr([_cli(1, 16, LUNES_10H)], hechos=[(1, 1)])
    assert m.tickets == [] and m.enviados == []


def test_sin_correo_deja_ticket_para_llamar():
    m, _ = _correr([_cli(1, 16, LUNES_10H)], emails=[])
    assert len(m.tickets) == 1 and m.enviados == []
    assert "teléfono" in m.tickets[0][2]


def test_fin_de_semana_y_de_noche_no_corre():
    ns = _cargar()
    for ahora in (dt.datetime(2026, 10, 3, 10, 0), dt.datetime(2026, 10, 5, 22, 0)):   # sábado / lunes 22h
        m = _Mundo([_cli(1, 16, ahora)])
        m.conectar(ns, ahora)
        res = ns["_prospectos_oferta_barrido"]()
        assert res.get("fuera_de_horario") and m.tickets == []


def test_interruptor_apaga_la_campana():
    ns = _cargar()
    m = _Mundo([_cli(1, 16, LUNES_10H)])
    m.conectar(ns, LUNES_10H)
    with mock.patch.dict(os.environ, {"PROSPECTOS_OFERTA_AUTO": "0"}):
        assert ns["_prospectos_oferta_barrido"]().get("apagado")
    assert m.tickets == []


def test_tope_por_corrida():
    cands = [_cli(i, 16, LUNES_10H) for i in range(1, 31)]
    m, _ = _correr(cands)
    assert len(m.tickets) == 15


# ───────── Plan de mantención: cálculo y tope de descuento ─────────
def _plan():
    src = open(_APP, encoding="utf-8").read()
    a = src.index("_PLAN_DEFAULTS = {")
    b = src.index("def _ensure_mant_plan_config")
    ns = {}
    exec(src[a:b], ns)
    return ns


CFG = {"descuento_pct": "10", "descuento_max_pct": "20", "visitas_anio": "4", "valor_visita_equipo": "50000"}


def test_plan_no_inventa_precio_lo_da_el_cotizador():
    c = _plan()["_plan_calc"](CFG, 3)
    assert c["n_equipos"] == 3 and c["visitas_anio"] == 4 and c["descuento_pct"] == 10.0
    assert not any(k in c for k in ("total_anual", "subtotal_anual", "has_price"))


def test_plan_descuento_no_pasa_del_tope():
    c = _plan()["_plan_calc"](CFG, 1, descuento_pct="80")
    assert c["descuento_pct"] == 20.0


def test_plan_tope_nunca_menor_al_descuento_base():
    cfg = dict(CFG, descuento_max_pct="0")
    assert _plan()["_plan_calc"](cfg, 1)["descuento_pct"] == 10.0


def test_plan_entradas_raras_no_rompen():
    c = _plan()["_plan_calc"](CFG, 2, descuento_pct="abc", visitas="x")
    assert c["visitas_anio"] == 4 and c["descuento_pct"] == 10.0


def test_formato_pesos_chilenos():
    assert _plan()["_clp_fmt"](1234567) == "$1.234.567"


# ───────── Tracking de pasos ─────────
def _pasos():
    src = open(_APP, encoding="utf-8").read()
    a = src.index("_PROSP_PASOS = [")
    b = src.index("def _prospecto_info")
    ns = {}
    exec(src[a:b], ns)
    return ns["_prospecto_pasos"]


def _estados(pasos):
    return [p["estado"] for p in pasos]


def test_tracking_recien_instalado():
    pasos, n, sig = _pasos()({"etapa": "por_ofrecer", "origen_ot": "OT-1", "origen_fecha": "02/09/2026"})
    assert _estados(pasos) == ["hecho", "actual", "pendiente", "pendiente", "pendiente"] and n == 1 and sig == "Contactar al cliente"


def test_tracking_contactado_sin_cotizar():
    pasos, n, sig = _pasos()({"etapa": "ofrecida", "ofrecida_at": "05/10/2026", "ofrecida_por": "Aaron"})
    assert _estados(pasos)[:3] == ["hecho", "hecho", "actual"] and sig == "Enviar la cotización"


def test_tracking_cotizacion_borrador_sigue_pendiente_de_enviar():
    pasos, n, sig = _pasos()({"etapa": "ofrecida", "ofrecida_at": "x"}, {"numero": "COT-1", "estado": "draft", "fecha": "05/10/2026"})
    assert pasos[2]["estado"] == "actual" and "borrador" in pasos[2]["detalle"]


def test_tracking_cotizacion_enviada_espera_respuesta():
    pasos, n, sig = _pasos()({"etapa": "ofrecida", "ofrecida_at": "x"}, {"numero": "COT-1", "estado": "sent", "fecha": "05/10/2026"})
    assert _estados(pasos) == ["hecho", "hecho", "hecho", "actual", "pendiente"] and sig == "Esperar su respuesta"


def test_tracking_rechazo_corta_el_proceso():
    pasos, n, sig = _pasos()({"etapa": "rechazada", "ofrecida_at": "x"}, {"numero": "COT-1", "estado": "sent", "fecha": "x"})
    assert pasos[3]["estado"] == "fallido" and pasos[4]["estado"] == "pendiente" and sig == "Rechazó el plan"


def test_tracking_con_contrato_queda_completo():
    pasos, n, sig = _pasos()({"etapa": "ofrecida"}, None, "10/10/2026")
    assert set(_estados(pasos)) == {"hecho"} and n == 5 and sig == "Cliente de mantención"

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


# ───────── Fichas de clientes que entran por instalación ─────────
def _ficha_ns(existente=None):
    src = open(_APP, encoding="utf-8").read()
    a0 = src.index("_NOMBRES_RELLENO = (")
    b0 = src.index("def _instalaciones_auditar")
    a = src.index("def _ficha_instalacion_desde_grupo")
    b = src.index('@app.route("/mantenciones/api/instalaciones/auditoria"')
    calls = {"insert": [], "log": []}

    def fetchone(q, p=()):
        if "LAST_INSERT_ID" in q:
            return {"id": 777}
        return {"id": existente} if existente else None

    ns = {"os": os, "re": __import__("re"),
          "validar_rut": lambda r: (True, "123456785") if (r or "").replace(".", "").startswith("1234567") else (False, "RUT muy corto"),
          "_rut_cuerpo": lambda r: "12345678",
          "mysql_fetchone": fetchone,
          "mysql_execute": lambda q, p=(): calls["insert"].append((q, p)),
          "_mant_log": lambda *a, **k: calls["log"].append(a),
          "_instalaciones_auditar": lambda desde=None: {"tickets_sin_ficha": []}}
    exec(src[a0:b0], ns)
    exec(src[a:b], ns)
    return ns, calls


def _rut_real():
    """validar_rut + _rut_canon REALES de app.py (sin dobles), para probar los formatos que llegan de los tickets."""
    src = open(_APP, encoding="utf-8").read()

    def bloque(nombre):
        i = src.index("\ndef " + nombre + "(")
        j = src.index("\ndef ", i + 5)
        return src[i:j]

    ns = {"re": __import__("re")}
    for n in ("normalizar_rut", "_calcular_dv_rut", "validar_rut", "_rut_canon"):
        exec(bloque(n), ns)
    return ns["_rut_canon"]


@pytest.mark.parametrize("entrada,esperado", [
    ("09.918.126-5", "9918126-5"),   # con cero adelante
    ("099181265", "9918126-5"),      # cero adelante, sin guion
    ("99181265", "9918126-5"),       # cuerpo de 7 sin guion (el que _rut_cuerpo confundía)
    ("9918126", "9918126-5"),        # sin dígito verificador
    ("09908128", "9908128-7"),       # sin DV y con cero adelante
    ("76.996.964-0", "76996964-0"),  # RUT de ILUS
    ("abc", None),
])
def test_rut_canonico(entrada, esperado):
    assert _rut_real()(entrada) == esperado


def test_variantes_del_rut_para_buscar_sus_tickets():
    src = open(_APP, encoding="utf-8").read()
    ns = {"re": __import__("re")}
    for n in ("normalizar_rut", "_calcular_dv_rut", "validar_rut", "_rut_canon", "_rut_variantes_ticket"):
        i = src.index("\ndef " + n + "(")
        exec(src[i:src.index("\ndef ", i + 5)], ns)
    assert ns["_rut_variantes_ticket"]("09.918.126-5") == sorted({"99181265", "099181265", "9918126", "09918126"})
    assert ns["_rut_variantes_ticket"]("") == []


def test_ficha_se_crea_como_prospecto_de_instalacion():
    ns, calls = _ficha_ns()
    cid, est = ns["_ficha_instalacion_desde_grupo"]({"rut": "1234567", "empresa": "Gimnasio X", "tickets": [{"numero": "TAA-1"}]}, "daniel")
    assert (cid, est) == (777, "creada")
    q, p = calls["insert"][0]
    assert "'prospecto','instalacion'" in q and p[0] == "Gimnasio X" and p[1] == "12345678-5"
    assert calls["log"]


def test_ficha_no_usa_nombres_de_relleno():
    ns, calls = _ficha_ns()
    cid, est = ns["_ficha_instalacion_desde_grupo"]({"rut": "1234567", "empresa": "Cliente no informado por ERP",
                                                      "contacto": "Francisco Posada", "tickets": []}, "d")
    assert est == "creada" and calls["insert"][0][1][0] == "Francisco Posada"


def test_ficha_no_duplica_si_el_rut_ya_existe():
    ns, calls = _ficha_ns(existente=55)
    assert ns["_ficha_instalacion_desde_grupo"]({"rut": "1234567", "empresa": "X", "tickets": []}, "d") == (55, "ya_existia")
    assert calls["insert"] == []


def test_ficha_rut_invalido_o_sin_nombre_no_crea_nada():
    ns, calls = _ficha_ns()
    assert ns["_ficha_instalacion_desde_grupo"]({"rut": "99", "empresa": "X"}, "d")[0] is None
    assert ns["_ficha_instalacion_desde_grupo"]({"rut": "1234567", "empresa": "", "contacto": ""}, "d")[0] is None
    assert calls["insert"] == []


def test_vigilancia_se_puede_apagar(monkeypatch):
    ns, _ = _ficha_ns()
    monkeypatch.setenv("INSTALACIONES_FICHA_AUTO", "0")
    assert ns["_instalaciones_asegurar_fichas"]().get("apagado")


# ───────── Productos de la instalación → equipos de la ficha ─────────
def _equipos_ns(header=True, asignados=None, equipos_ticket=None, numero_documento="FCV-0001234", en_ficha=None):
    src = open(_APP, encoding="utf-8").read()
    a = src.index("_TIDOS_VENTA = (")
    b = src.index("def _instalaciones_asegurar_fichas")
    import re as _re
    calls = {"insert": [], "log": []}
    ticket = {"id": 1, "numero_ticket": "TAA-1", "rut": "1-9", "estado": "resolved",
              "numero_documento": numero_documento, "cerrado_at": "2026-03-10", "created_at": "2026-03-01"}

    def fetchall(q, p=()):
        if "FROM tk_ticket_documentos" in q:
            return []
        if "FROM tk_ticket_equipos" in q:
            return equipos_ticket or []
        if "FROM mant_maquinas" in q:
            return [{"sku": k, "n": v} for k, v in (en_ficha or {}).items()]
        return []

    def fetchone(q, p=()):
        if "LAST_INSERT_ID" in q:
            return {"id": 900 + len(calls["insert"])}
        return {"id": 5, "rut": "1-9", "razon_social": "Gym"}

    lineas = [{"sku": "ZZINSTALACION", "cantidad": 1, "es_zz": True},
              {"sku": "TROT1", "cantidad": 2, "nombre_app": "Trotadora", "tiene_ficha": True},
              {"sku": "KB20", "cantidad": 4, "nombre_app": "Kettlebell 20", "tiene_ficha": True},
              {"sku": "DE", "cantidad": 828000, "descripcion_erp": "glosa de la factura"},      # basura real (FCV 11150)
              {"sku": "HTE", "cantidad": 1, "descripcion_erp": "no es un producto"}]
    ns = {"re": _re, "print": print,
          "_rut_canon": lambda r: "1-9", "_rut_cuerpo": lambda r: "1",
          "mysql_fetchall": fetchall, "mysql_fetchone": fetchone,
          "mysql_execute": lambda q, p=(): calls["insert"].append(p),
          "_mant_erp_doc_cached": lambda t, n: (({"fecha": "2026-02-20"}, lineas) if header else (None, [])),
          "_doc_origen_key": lambda t, n: f"{t} {n}",
          "_doc_origen_normalizar": lambda d: d,
          "_asignados_por_sku": lambda t, n: dict(asignados or {}),
          "_inc_clasificacion_skus_batch": lambda skus: {"KB20": {"repetible": True}},
          "_generar_serie_ilus": lambda cid, sku, _intento=0: f"S-{sku}-1",
          "_mant_log": lambda *x, **k: calls["log"].append(x),
          "current_username": lambda: "daniel"}
    exec(src[a:b], ns)
    return ns, calls, [ticket]


def test_docs_se_leen_del_texto_del_ticket():
    ns, _, tk = _equipos_ns(numero_documento="FCV-0001234, BLV 55 y una NCV 9")
    docs = ns["_docs_de_tickets"](tk)
    assert [(d[0], d[1]) for d in docs] == [("FCV", "1234"), ("BLV", "55")]


def test_equipos_vista_previa_sin_servicios_zz_ni_glosas():
    ns, calls, tk = _equipos_ns()
    ok, d = ns["_ficha_equipos_desde_instalacion"](5, confirmar=False, tickets_cache=tk)
    assert ok and d["preview"] and d["total_a_crear"] == 6 and not calls["insert"]
    assert {c["sku"] for c in d["candidatos"]} == {"TROT1", "KB20"}
    assert {o["sku"] for o in d["omitidos_detalle"]} == {"DE", "HTE"}     # no están en el maestro de productos


def test_equipos_se_cargan_uno_por_unidad_y_accesorios_fuera_del_plan():
    ns, calls, tk = _equipos_ns()
    ok, d = ns["_ficha_equipos_desde_instalacion"](5, confirmar=True, tickets_cache=tk)
    assert ok and d["creados"] == 6 and len(calls["insert"]) == 6
    trot = [p for p in calls["insert"] if p[1] == "TROT1"]
    kb = [p for p in calls["insert"] if p[1] == "KB20"]
    assert len(trot) == 2 and len(kb) == 4
    assert trot[0][4] == "FCV 1234" and trot[0][5] == "2026-02-20"     # documento y fecha de emisión
    assert trot[0][8] == 1 and kb[0][8] == 0                              # el accesorio no entra al plan


def test_misma_venta_en_varios_documentos_cuenta_una_vez():
    # FCV 11150 y VD 10212 traían los mismos productos (caso real): no se crean dos veces.
    ns, calls, tk = _equipos_ns(numero_documento="VD 10212, FCV-11150")
    ok, d = ns["_ficha_equipos_desde_instalacion"](5, confirmar=False, tickets_cache=tk)
    assert d["total_a_crear"] == 6
    assert {c["doc_key"] for c in d["candidatos"]} == {"FCV 11150"}       # la factura manda como referencia


def test_descuenta_lo_que_la_ficha_ya_tiene():
    ns, calls, tk = _equipos_ns(en_ficha={"TROT1": 2})
    ok, d = ns["_ficha_equipos_desde_instalacion"](5, confirmar=False, tickets_cache=tk)
    assert {c["sku"]: c["cantidad"] for c in d["candidatos"]} == {"KB20": 4}


def test_equipos_no_se_duplican():
    ns, calls, tk = _equipos_ns(asignados={"TROT1": 2, "KB20": 4})
    ok, d = ns["_ficha_equipos_desde_instalacion"](5, confirmar=True, tickets_cache=tk)
    assert d["creados"] == 0 and not calls["insert"] and not d["respaldo_ticket"]


def test_equipos_respaldo_del_ticket_si_la_factura_no_esta_en_el_erp():
    eq = [{"ticket_id": 1, "erp_kopr": "BICI9", "sku": None, "nombre": "Bicicleta", "cantidad": 1, "documento_garantia": None}]
    ns, calls, tk = _equipos_ns(header=False, equipos_ticket=eq)
    ok, d = ns["_ficha_equipos_desde_instalacion"](5, confirmar=True, tickets_cache=tk)
    assert d["respaldo_ticket"] and d["creados"] == 1 and calls["insert"][0][1] == "BICI9"


# ───────── Calendario de mantenciones desde la factura ─────────
def _cal():
    src = open(_APP, encoding="utf-8").read()
    a = src.index("def _sumar_meses")
    b = src.index("def _prospecto_info")
    ns = {"datetime": dt.datetime}
    exec(src[a:b], ns)
    return ns


def test_sumar_meses_respeta_fin_de_mes():
    f = _cal()["_sumar_meses"]
    assert f(dt.date(2026, 1, 31), 1) == dt.date(2026, 2, 28)
    assert f(dt.date(2026, 11, 15), 3) == dt.date(2027, 2, 15)


def test_escalera_parse_ignora_basura():
    assert _cal()["_escalera_parse"]("100, 50;25%, abc, 150") == [100.0, 50.0, 25.0]


def test_calendario_desde_la_factura_con_escalera_activa():
    c = _cal()["_plan_calendario"](dt.date(2026, 2, 20), dt.date(2026, 10, 5), 6, 4, [100, 50, 25], True, 10)
    assert c["primera"] == "20/08/2026" and c["estado_primera"] == "vencida" and c["dias_primera"] < 0
    assert [v["fecha"] for v in c["visitas"]][:3] == ["20/08/2026", "20/11/2026", "20/02/2027"]
    assert [v["descuento"] for v in c["visitas"]] == [100, 50, 25, 10]   # después de la escalera, el descuento del plan
    assert c["visitas"][0]["gratis"] and c["cada_meses"] == 3


def test_calendario_con_escalera_apagada_usa_solo_el_descuento_base():
    c = _cal()["_plan_calendario"](dt.date(2026, 9, 1), dt.date(2026, 10, 5), 6, 2, [100, 50], False, 15)
    assert c["estado_primera"] == "futura" and {v["descuento"] for v in c["visitas"]} == {15.0}
    assert not c["escalera_activa"] and c["cada_meses"] == 6


def _zonas():
    src = open(_APP, encoding="utf-8").read()
    a = src.index("_COMUNAS_RM = {")
    b = src.index("def _cobertura_datos")
    ns = {"re": __import__("re")}
    exec(src[a:b], ns)
    return ns["_zona_de"]


@pytest.mark.parametrize("comuna,zona", [
    ("LAS CONDES", "Oriente"), ("Ñuñoa", "Oriente"), ("Comuna de Maipú", "Poniente"),
    ("Lo Barnechea, Santiago", "Oriente"), ("Puente Alto", "Sur"), ("Viña del Mar", "Regiones"),
    ("", "Sin comuna"), ("  huechuraba ", "Norte"), ("Estación Central", "Centro"),
])
def test_zona_por_comuna(comuna, zona):
    assert _zonas()(comuna)[0] == zona


def test_calendario_proxima_y_sin_fecha():
    cal = _cal()["_plan_calendario"]
    assert cal(dt.date(2026, 4, 20), dt.date(2026, 10, 5), 6, 4)["estado_primera"] == "proxima"
    assert cal(None, dt.date(2026, 10, 5), 6, 4) is None

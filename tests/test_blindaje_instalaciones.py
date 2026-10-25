# -*- coding: utf-8 -*-
"""Blindaje de la creación de clientes de instalación (Daniel 2026-10-19: «estamos creando los clientes para
proponer esto: debe estar muy robusto para establecer las mantenciones como área de negocio de la empresa»).

Cubre la puerta del ERP (ventas con ZZINSTALACION sin ticket ni OT), el RUT tomado de la factura, la ficha
«lista para cotizar», los controles del panel «Salud del proceso» y el monto obligatorio del contrato.
Corre sin base de datos ni ERP: se extraen de app.py solo las funciones y se ejecutan con dobles.

    py -m pytest tests/test_blindaje_instalaciones.py -q
"""
import datetime as dt
import os
import re
import time

import pytest

_APP = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "app.py")
_SRC = []
_RELLENO = ("cliente no informado por erp", "cliente no informado", "sin nombre", "s/n", "n/a", "na", "-")


def _src():
    if not _SRC:
        _SRC.append(open(_APP, encoding="utf-8").read())
    return _SRC[0]


def _fn(nombre, hasta="\n\n\n"):
    """El código de una función de primer nivel de app.py (hasta las dos líneas en blanco que la separan)."""
    s = _src()
    i = s.index("\ndef " + nombre + "(")
    return s[i:s.index(hasta, i + 5)]


def _ns(*nombres, **extra):
    ns = {"re": re, "time": time, "datetime": dt.datetime, "print": lambda *a, **k: None,
          "_NOMBRES_RELLENO": _RELLENO,
          "_doc_origen_key": lambda t, n: f"{(t or '').upper()} {str(n or '').lstrip('0')}",
          # 2026-10-24: _ficha_equipos_desde_instalacion ya no cuenta servicios/descuentos como equipos (23-oct); la prueba le presta la misma regla.
          "_sku_es_servicio": lambda sku, nombre=None: (str(sku or "").strip().upper().startswith("ZZ")
                                                          or str(sku or "").strip().upper() == "DE"
                                                          or str(nombre or "").strip().lower().startswith("descuento")),
          "chile_fmt_filter": lambda v, f: v.strftime(f) if hasattr(v, "strftime") else str(v)}
    for n in ("normalizar_rut", "_calcular_dv_rut", "validar_rut", "_rut_canon", "_rut_cuerpo"):
        exec(_fn(n, "\ndef "), ns)
    ns.update(extra)
    for n in nombres:
        exec(_fn(n), ns)
    return ns


# ───────── Puerta del ERP: ventas con instalación agrupadas con los tickets ─────────
def _t(id_, rut, **k):
    return {"id": id_, "numero_ticket": f"TK-{id_}", "rut": rut, "estado": "open",
            "created_at": dt.datetime(2026, 10, 10, 15, 0), **k}


def _v(tido, nudo, rut, **k):
    return {"tido": tido, "nudo": nudo, "cliente_rut": rut, "fecha_emision": dt.date(2026, 10, 8), **k}


def test_venta_del_erp_sin_ticket_ni_ot_tambien_es_un_cliente():
    agrupar = _ns("_agrupar_instalaciones")["_agrupar_instalaciones"]
    grupos, sin_rut = agrupar([], [_v("FCV", "11900", "76.123.456-0", cliente_nombre="Gimnasio Andes SpA",
                                      email="a@andes.cl", telefono="+56 9 1111 2222", comuna="Las Condes")], {})
    assert len(grupos) == 1 and not sin_rut
    g = grupos[0]
    assert g["empresa"] == "Gimnasio Andes SpA" and g["email"] == "a@andes.cl" and g["comuna"] == "Las Condes"
    assert g["facturas"] == [{"doc": "FCV 11900", "fecha": "08/10/2026"}] and g["tickets"] == []
    assert g["fuente"] == "factura del ERP"


def test_ticket_y_factura_del_mismo_rut_son_un_solo_cliente_y_manda_el_contacto_del_ticket():
    agrupar = _ns("_agrupar_instalaciones")["_agrupar_instalaciones"]
    grupos, _ = agrupar([_t(1, "76123456-0", empresa="Gimnasio Andes", email="ana@andes.cl", nombre_contacto="Ana")],
                        [_v("FCV", "11900", "76.123.456-0", cliente_nombre="GIMNASIO ANDES SPA", email="facturas@andes.cl",
                            telefono="+56 2 2222 3333")], {})
    assert len(grupos) == 1
    g = grupos[0]
    assert g["empresa"] == "Gimnasio Andes" and g["email"] == "ana@andes.cl"       # el ticket manda
    assert g["telefono"] == "+56 2 2222 3333"                                       # la factura completa lo que falta
    assert g["fuente"] == "ticket y factura del ERP" and len(g["tickets"]) == 1 and len(g["facturas"]) == 1


def test_cliente_con_ficha_no_aparece_aunque_el_rut_venga_escrito_distinto():
    agrupar = _ns("_agrupar_instalaciones")["_agrupar_instalaciones"]
    fichas = {"76123456-0": 10}                       # la ficha guarda el RUT canónico
    grupos, _ = agrupar([_t(1, "76.123.456-0")], [_v("FCV", "1", "076123456-0")], fichas)
    assert grupos == []
    grupos, _ = agrupar([], [_v("FCV", "1", "76.123.456-0")], {"76123456": 11})   # ficha antigua sin DV
    assert grupos == []


@pytest.mark.parametrize("entrada,esperado", [
    ("076123456-0", "76123456-0"),      # cero adelante que deja el cuerpo en 9 dígitos
    ("0.076.123.456-0", "76123456-0"),
    ("76.123.456-0", "76123456-0"),
    ("76.123.456-1", None),             # dígito verificador incorrecto: no se inventa
])
def test_rut_canonico_con_ceros_adelante(entrada, esperado):
    assert _ns()["_rut_canon"](entrada) == esperado


def test_ticket_sin_rut_queda_aparte_para_buscarle_el_rut():
    agrupar = _ns("_agrupar_instalaciones")["_agrupar_instalaciones"]
    grupos, sin_rut = agrupar([_t(7, "", empresa="Gym sin RUT")], [], {})
    assert grupos == [] and sin_rut[0]["id"] == 7 and sin_rut[0]["empresa"] == "Gym sin RUT"


def test_la_misma_factura_dos_veces_cuenta_una_y_los_nombres_de_relleno_no_entran():
    agrupar = _ns("_agrupar_instalaciones")["_agrupar_instalaciones"]
    grupos, _ = agrupar([], [_v("FCV", "5", "76123456-0", cliente_nombre="Cliente no informado por ERP"),
                             _v("FCV", "5", "76123456-0", cliente_nombre="Gym Real")], {})
    assert len(grupos[0]["facturas"]) == 1 and grupos[0]["empresa"] == "Gym Real"


# ───────── RUT desde la factura ─────────
def test_rut_desde_documentos_toma_el_primer_documento_con_rut_valido():
    leer = {("FCV", "1"): None, ("FCV", "2"): {"cliente_rut": "basura"},
            ("BLV", "3"): {"cliente_rut": "76.123.456-0", "cliente_nombre": " Gym Andes "}}
    f = _ns("_rut_desde_documentos")["_rut_desde_documentos"]
    assert f([("FCV", "1"), ("FCV", "2"), ("BLV", "3")], leer=lambda t, n: (leer[(t, n)], [])) == ("76123456-0", "BLV 3", "Gym Andes")
    assert f([("FCV", "1")], leer=lambda t, n: (None, [])) == (None, None, None)


def _rut_ns(tickets, header):
    calls = {"update": [], "msg": []}

    def fetchall(q, p=()):
        if "FROM tk_tickets" in q:
            return [dict(t) for t in tickets]
        return []                                      # tk_ticket_documentos

    def rowcount(q, p=()):
        calls["update"].append(p)
        return 1

    s = _src()
    a = s.index("_TIDOS_VENTA = (")
    ns = _ns("_rut_desde_documentos", "_docs_de_tickets", "_tickets_rut_desde_factura",
             mysql_fetchall=fetchall, mysql_execute_returning_rowcount=rowcount,
             mysql_execute=lambda q, p=(): calls["msg"].append(p),
             _mant_erp_doc_cached=lambda t, n: (header, []), _RUT_FACTURA_INTENTOS={})
    exec(s[a:s.index("\n\n\ndef _tickets_instalacion_del_cliente", a)], ns)
    return ns, calls


def test_ticket_sin_rut_toma_el_rut_de_su_factura_y_deja_constancia():
    ns, calls = _rut_ns([{"id": 5, "numero_ticket": "TK-5", "empresa": "", "numero_documento": "FCV 11900"}],
                        {"cliente_rut": "76.123.456-0", "cliente_nombre": "Gym Andes"})
    res = ns["_tickets_rut_desde_factura"](max_n=10)
    assert res["resueltos"] == 1 and calls["update"][0][0] == "76123456-0" and calls["update"][0][2] == 5
    assert "FCV 11900" in calls["msg"][0][1] and "76123456-0" in calls["msg"][0][1]
    # La revisión automática no vuelve a leer el ERP por el mismo ticket el mismo día…
    assert ns["_tickets_rut_desde_factura"](max_n=10)["resueltos"] == 0
    # …pero el administrador sí puede pedirlo a propósito.
    assert ns["_tickets_rut_desde_factura"](max_n=10, ids=[5])["resueltos"] == 1


def test_ticket_sin_documento_o_sin_rut_en_el_erp_no_inventa_nada():
    ns, calls = _rut_ns([{"id": 6, "numero_ticket": "TK-6", "empresa": "", "numero_documento": "sin factura"}], None)
    assert ns["_tickets_rut_desde_factura"](max_n=10)["sin_documento"] == 1 and not calls["update"]
    ns, calls = _rut_ns([{"id": 7, "numero_ticket": "TK-7", "empresa": "", "numero_documento": "FCV 1"}], {"cliente_rut": ""})
    assert ns["_tickets_rut_desde_factura"](max_n=10)["sin_rut_en_erp"] == 1 and not calls["update"]


# ───────── Salud del proceso ─────────
def test_ot_cerrada_sin_sus_equipos_en_la_ficha():
    f = _ns("_ot_sin_equipos_filtrar")["_ot_sin_equipos_filtrar"]
    ots = [{"id": 1, "numero_ot": "OT-1", "cliente_id": 10, "tido": "FCV", "nudo": "00500", "razon_social": "A"},
           {"id": 2, "numero_ot": "OT-2", "cliente_id": 11, "tido": "FCV", "nudo": "600", "razon_social": "B"},
           {"id": 3, "numero_ot": "OT-3", "cliente_id": 12, "tido": "FCV", "nudo": "700", "razon_social": "C"},
           {"id": 4, "numero_ot": "OT-4", "cliente_id": 13, "tido": "", "nudo": "", "razon_social": "D"}]
    out = f(ots, {(10, "FCV 500")}, {3})
    assert [o["vid"] for o in out] == [2] and out[0]["doc"] == "FCV 600"


@pytest.mark.parametrize("valor,monto,hay_error", [
    ("250000", 250000.0, False), (250000, 250000.0, False), ("1000", 1000.0, False),
    ("0", None, True), ("", None, True), (None, None, True), ("12.5", None, True), ("abc", None, True),
])
def test_monto_mensual_del_contrato_obligatorio_y_en_pesos(valor, monto, hay_error):
    m, err = _ns("_contrato_monto_valido", _CONTRATO_MONTO_MIN=1000)["_contrato_monto_valido"](valor)
    assert m == monto and bool(err) == hay_error
    if valor == "12.5":
        assert "UF" in err


def test_contrato_sin_monto_se_rechaza_antes_de_subir_el_archivo():
    s = _src()
    i = s.index("def mant_contrato_subir(cid):")
    cuerpo = s[i:s.index("_validar_y_reparar_pdf(", i)]
    assert "_contrato_monto_valido(d.get(\"monto_mensual\"))" in cuerpo and "MONTO_MENSUAL_OBLIGATORIO" in cuerpo


@pytest.mark.parametrize("contacto,equipos,sin_tarifa,faltan", [
    (True, 5, 0, []),
    (False, 5, 0, ["contacto"]),
    (True, 0, 0, ["equipos"]),
    (True, 5, 2, ["tarifa"]),
    (False, 0, 3, ["contacto", "equipos"]),        # sin equipos, la tarifa no aplica todavía
])
def test_ficha_lista_para_cotizar(contacto, equipos, sin_tarifa, faltan):
    f = _ns("_prospecto_faltan")["_prospecto_faltan"]
    assert [x["key"] for x in f(contacto, equipos, sin_tarifa)] == faltan
    if sin_tarifa and equipos:
        assert f(contacto, equipos, sin_tarifa)[-1]["texto"] == f"la tarifa de {sin_tarifa} equipos"


# ───────── Productos desde la venta del ERP (sin ticket) ─────────
def test_productos_de_la_venta_del_erp_aunque_no_haya_ticket():
    s = _src()
    a, b = s.index("_TIDOS_VENTA = ("), s.index("def _instalaciones_asegurar_fichas")
    calls = {"insert": []}

    def fetchall(q, p=()):
        if "FROM mant_maquinas" in q or "FROM tk_" in q or "FROM cat_productos" in q:
            return []
        return []

    def fetchone(q, p=()):
        if "LAST_INSERT_ID" in q:
            return {"id": 900 + len(calls["insert"])}
        return {"id": 5, "rut": "76123456-0", "razon_social": "Gym Andes"}

    lineas = [{"sku": "ZZINSTALACION", "cantidad": 1, "es_zz": True},
              {"sku": "TROT1", "cantidad": 2, "nombre_app": "Trotadora", "tiene_ficha": True}]
    ns = _ns(mysql_fetchall=fetchall, mysql_fetchone=fetchone,
             mysql_execute=lambda q, p=(): calls["insert"].append(p),
             _fecha_doc_a_date=lambda v: dt.date(2026, 10, 8) if v else None,
             _mant_erp_doc_cached=lambda t, n: ({"fecha": "08/10/2026"}, lineas),
             _doc_origen_normalizar=lambda d: d, _asignados_por_sku=lambda t, n: {},
             _inc_clasificacion_skus_batch=lambda skus: {}, _generar_serie_ilus=lambda cid, sku, _intento=0: f"S-{sku}-1",
             _mant_log=lambda *x, **k: None, current_username=lambda: "sistema")
    exec(s[a:b], ns)
    ventas = [{"tido": "FCV", "nudo": "11900", "cliente_rut": "76.123.456-0", "fecha_emision": dt.date(2026, 10, 8)}]
    ok, d = ns["_ficha_equipos_desde_instalacion"](5, confirmar=True, tickets_cache=[], erp_cache=ventas)
    assert ok and d["creados"] == 2
    assert calls["insert"][0][5] == "FCV 11900" and "venta con instalación FCV 11900" in calls["insert"][0][8]


# ───────── La franja avisa qué le falta (ámbar) sin trabar el paso ─────────
def _franja(pr, en_ficha=False):
    import jinja2
    env = jinja2.Environment(loader=jinja2.FileSystemLoader(os.path.join(os.path.dirname(_APP), "templates")),
                             autoescape=True)
    env.globals["url_for"] = lambda ep, **k: f"/servicio-tecnico/clientes/{k.get('cid')}"
    base = {"pasos": [], "pasos_hechos": 1, "pasos_total": 5, "paso_actual": "", "etapa": "por_ofrecer",
            "etapa_label": "Por ofrecer", "link": "", "accion": {"key": "ofrecer", "titulo": "Ofrecer mantención", "detalle": ""}}
    base.update(pr)
    c = {"id": 7, "razon_social": "Gym Sur", "contacto_tel": "+56 9 1234 5678", "pr": base}
    tpl = env.from_string('{% from "mantenciones/_prosp_strip.html" import prosp_strip %}{{ prosp_strip(c, cfg, en_ficha=ficha) }}')
    return tpl.render(c=c, cfg={"descuento": "0", "visitas": "4", "mensaje_wa": ""}, ficha=en_ficha)


def test_franja_dice_que_falta_y_lleva_a_completarlo():
    faltan = [{"key": "contacto", "texto": "teléfono o correo"}, {"key": "tarifa", "texto": "la tarifa de 2 equipos"}]
    h = _franja({"faltan": faltan})
    assert "Para cotizar le falta" in h and "?accion=contacto" in h and 'href="/catalogo/clases"' in h
    assert "prospOfrecer" in h                          # avisa, no traba: el paso sigue disponible
    h2 = _franja({"faltan": faltan}, en_ficha=True)
    assert "abrirEditarCliente()" in h2


def test_franja_lista_para_cotizar():
    h = _franja({"faltan": [], "listo": True, "precio_mantencion": 185000})
    assert "Lista para cotizar" in h and "185.000" in h and "Para cotizar le falta" not in h


def test_panel_de_salud_y_clientes_parsean():
    import jinja2
    env = jinja2.Environment(loader=jinja2.FileSystemLoader(os.path.join(os.path.dirname(_APP), "templates")))
    for t in ("mantenciones/_salud_proceso.html", "mantenciones/clientes.html", "mantenciones/ficha.html"):
        env.parse(open(os.path.join(os.path.dirname(_APP), "templates", t), encoding="utf-8").read())
    clientes = open(os.path.join(os.path.dirname(_APP), "templates", "mantenciones", "clientes.html"), encoding="utf-8").read()
    assert '{% include "mantenciones/_salud_proceso.html" %}' in clientes

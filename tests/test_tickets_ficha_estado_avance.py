"""Ficha del ticket (Daniel 2026-10-28): «Estado y avance» con barra de tiempo
como Retiros, y la identificación del cliente con cada dato en su propia ficha
(antes eran tres hijos sueltos en una grilla de 2 columnas y cada etiqueta
caía corrida un lugar: el RUT quedaba bajo «EMPRESA»)."""
import os
import re

_RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _leer(*ruta):
    return open(os.path.join(_RAIZ, *ruta), encoding="utf-8").read()


def test_cada_dato_del_cliente_es_una_ficha_con_su_etiqueta_y_su_valor():
    js = _leer("static", "tickets_ficha.js")
    i = js.index("function tkIdFilaHtml(campo, t){")
    cuerpo = js[i:i + 1500]
    # Un solo contenedor por dato: etiqueta (dt) y valor (dd) juntos.
    assert cuerpo.count("tk-id-item") >= 1
    assert "<dt class=\"tk-id-item-top\">" in cuerpo and "<dd data-field=" in cuerpo
    assert "</dd></div>" in cuerpo
    # El ícono ya no es un hijo suelto de la grilla.
    assert "return '<i class=\"bi bi-'+campo.icon" not in cuerpo


def test_la_ficha_trae_el_bloque_estado_y_avance_con_barra_de_tiempo():
    html = _leer("templates", "tickets", "ficha.html")
    for ident in ("tkEstadoAvance", "eaEstado", "eaPct", "eaBarraAv", "eaSla", "eaBarraT", "eaSlaSub"):
        assert f'id="{ident}"' in html, ident
    # va antes de la guía (a la vista apenas se abre el ticket)
    assert html.index('id="tkEstadoAvance"') < html.index('id="tkGuia"')


def test_el_js_pinta_estado_avance_y_plazo_desde_lo_que_calcula_el_servidor():
    js = _leer("static", "tickets_ficha_v2.js")
    assert "function pintarEstadoAvance(t, d)" in js
    assert "pintarEstadoAvance(t, d)" in js.split("window.tkV2AlCargar = function")[1]
    cuerpo = js[js.index("function pintarEstadoAvance"):js.index("window.tkV2AlCargar = function")]
    assert "d.guia" in cuerpo and "t.sla_resp" in cuerpo and "espera_h" in cuerpo
    # la ficha nunca se cae por este bloque
    assert re.search(r"try \{ pintarEstadoAvance\(t, d\); \} catch", js)


def test_la_bandeja_tambien_muestra_la_barra_de_tiempo_del_sla():
    js = _leer("static", "tickets_list.js")
    i = js.index("function tkSlaCelda(tk)")
    assert 'class="barra"' in js[i:i + 2500]
    assert ".tkc-sla .barra" in _leer("static", "tickets_list.css")

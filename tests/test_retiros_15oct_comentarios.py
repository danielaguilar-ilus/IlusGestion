# -*- coding: utf-8 -*-
"""2026-10-15 · Comentarios de clientes que el equipo no veía (Daniel: «hay algunos comentarios que dejan los clientes y no los veo por
ningún lado… lo estamos omitiendo, ¿cierto?»).

  1. «¿Algo más que debamos saber?» (pickup_requests.observations) → tarjeta en la ficha + en el aviso «Nueva solicitud».
  2. Comentario al pedir otra fecha (pickup_proposals.message) → bitácora, aviso al equipo y banner de la ficha; y la tarjeta para
     aceptarla aparece aunque ILUS no haya propuesto antes.
  3. Actividad ya no corta a 400 caracteres; las observaciones de la firma quedan en la bitácora.

    py -m pytest tests/test_retiros_15oct_comentarios.py -q
"""
import os
import re
import sys

_TESTS = os.path.dirname(os.path.abspath(__file__))
RAIZ = os.path.dirname(_TESTS)
sys.path.insert(0, _TESTS)
sys.path.insert(0, RAIZ)

import _arnes_retiros as A  # noqa: E402


def _leer(*p):
    with open(os.path.join(RAIZ, *p), encoding="utf-8") as f:
        return f.read().replace("\r\n", "\n")


def notas(obs, fuente="web"):
    app, _db, _ctx, _esp = A.construir_app()
    return app.jinja_env.globals["retiro_notas"]({"observations": obs, "request_source": fuente})


class TestNotasDelRetiro:
    def test_lo_que_escribio_el_cliente_completo(self):
        largo = "Voy en camión 3/4 y necesito grúa horquilla.\nLlego con dos personas. " + "x" * 600
        n = notas(largo)
        assert n == [{"titulo": "Lo que escribió el cliente", "texto": largo.strip(), "ico": "bi-chat-quote-fill"}]

    def test_la_declaracion_de_tercero_no_se_repite_y_el_whatsapp_va_aparte(self):
        obs = ("Por favor tener todo listo temprano.\n[DECLARACIÓN AUTORIZACIÓN TERCERO · 14/10/2026 10:00]\nEl documento está a nombre de X")
        assert [x["texto"] for x in notas(obs)] == ["Por favor tener todo listo temprano."]
        n = notas("Llamar antes\nWhatsApp informado: +56 9 1234 5678")
        assert [(x["titulo"], x["texto"]) for x in n] == [("Lo que escribió el cliente", "Llamar antes"),
                                                          ("WhatsApp informado por el cliente", "+56 9 1234 5678")]

    def test_retiro_interno_es_nota_del_equipo(self):
        assert notas("Cliente frecuente, entregar por portón 2", "backoffice")[0]["titulo"] == "Nota del equipo (retiro interno)"

    def test_sin_nada_escrito_no_hay_tarjeta(self):
        assert notas("") == [] and notas(None) == [] and notas("[DECLARACIÓN AUTORIZACIÓN TERCERO · x]\nsolo esto") == []


class TestFicha:
    def test_la_ficha_muestra_las_notas_completas_bajo_la_cabecera(self):
        html = _leer("templates", "retiros", "internal_detail.html")
        i = html.find("retiro_notas(req)")
        assert 0 < i < html.find('<div class="pipe-wrap">')
        assert "{{ n.texto }}" in html[i:i + 800]
        css = _leer("static", "retiros_internal_detail.css")
        assert re.search(r"\.rd-nota-t p\{[^}]*white-space:pre-wrap", css), "texto completo con sus saltos de línea, sin cortar"

    def test_el_comentario_de_la_contrapropuesta_sale_en_los_banners_y_en_la_tarjeta(self):
        html = _leer("templates", "retiros", "internal_detail.html")
        assert html.count("Su comentario: “{{ _contraprop_cliente.message }}”") == 3

    def test_la_contrapropuesta_sin_propuesta_previa_se_puede_aceptar(self):
        html = _leer("templates", "retiros", "internal_detail.html")
        assert "{% if (_step4_done and not _step5_done) or _cambio_pedido or _counter_sin_prop %}" in html
        assert "El cliente pidió otra fecha — te toca responder" in html


class TestServidor:
    def test_la_bitacora_y_el_aviso_llevan_el_comentario(self):
        src = _leer("pickups_module.py")
        assert '(f" · Comentario del cliente: «{_cm_txt}»" if _cm_txt else "")' in src
        assert '(f"Comentario del cliente: «{_cm_txt[:400]}». " if _cm_txt else "")' in src

    def test_el_aviso_de_nueva_solicitud_lleva_la_nota_del_cliente(self):
        src = _leer("pickups_module.py")
        assert '(f" Nota del cliente: «{_nota_cliente_corta(req_bg)}»" if _nota_cliente_corta(req_bg) else "")' in src

    def test_actividad_no_corta_a_400_y_la_firma_deja_sus_observaciones(self):
        src = _leer("pickups_module.py")
        assert '"detalle": _act_corta(detalle, 1200)' in src
        assert '(f" Observaciones: «{obs}»" if obs else "")' in src

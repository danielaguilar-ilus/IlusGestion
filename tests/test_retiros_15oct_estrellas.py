# -*- coding: utf-8 -*-
"""2026-10-15 · Las estrellas del seguimiento se guardan en el retiro, como parte de la encuesta (Daniel eligió esa opción: misma fuente de
datos y mismo aviso de privacidad, Ley 21.719).

Tocar una estrella en el seguimiento lleva a la encuesta con «¿Cómo fue la atención del equipo en bodega?» YA marcada (?atencion=N). Solo
pre-marca: nada se guarda hasta que el cliente confirma con el aviso. Sin encuesta disponible, las estrellas siguen como antes.

    py -m pytest tests/test_retiros_15oct_estrellas.py -q
"""
import os
import re
import sys

_TESTS = os.path.dirname(os.path.abspath(__file__))
RAIZ = os.path.dirname(_TESTS)
sys.path.insert(0, _TESTS)
sys.path.insert(0, RAIZ)

from test_retiros_encuesta import construir, form_ok, pid  # noqa: E402


def _marcada(html, campo, n):
    return re.search(r'name="%s" id="%s-%s" value="%s" checked' % (campo, campo, n, n), html) is not None


class TestEncuestaPreMarcada:
    def test_la_estrella_llega_marcada_en_la_atencion_y_no_guarda_nada_todavia(self, monkeypatch):
        e = construir(monkeypatch)
        e.activar(); e.anonimo()
        url = f"/retiros/encuesta/{e.token()}"
        html = e.client.get(url + "?atencion=4").get_data(as_text=True)
        campo = f"p_{pid(e, 'atencion_bodega')}"
        assert _marcada(html, campo, 4)
        assert not _marcada(html, f"p_{pid(e, 'tiempo_total')}", 4)          # solo la atención en bodega
        assert e.db.contar("pickup_encuesta_respuestas") == 0                    # pre-marcar no es responder

    def test_al_confirmar_queda_guardada_esa_nota(self, monkeypatch):
        e = construir(monkeypatch)
        e.activar(); e.anonimo()
        url = f"/retiros/encuesta/{e.token()}"
        e.client.get(url + "?atencion=2")
        campo = f"p_{pid(e, 'atencion_bodega')}"
        r = e.client.post(url, data=form_ok(e, **{campo: "2"}))
        assert r.status_code == 303
        fila = e.db.fetchone("SELECT valor_num FROM pickup_encuesta_respuestas WHERE pregunta_id=%s", (pid(e, "atencion_bodega"),))
        assert fila["valor_num"] == 2

    def test_valores_raros_se_ignoran(self, monkeypatch):
        e = construir(monkeypatch)
        e.activar(); e.anonimo()
        url = f"/retiros/encuesta/{e.token()}"
        e.client.get(url)                                                       # crea las preguntas semilla
        campo = f"p_{pid(e, 'atencion_bodega')}"
        for v in ("0", "6", "x", "4' onmouseover=1", "", "44"):
            html = e.client.get(url + "?atencion=" + v).get_data(as_text=True)
            assert not any(_marcada(html, campo, n) for n in range(1, 6)), v
            assert "onmouseover" not in html


class TestSeguimiento:
    def test_las_estrellas_saben_a_que_encuesta_ir(self):
        with open(os.path.join(RAIZ, "templates", "retiros", "public_tracking.html"), encoding="utf-8") as f:
            html = f.read()
        assert '<div class="stars" id="ratingStars"{% if encuesta_url %} data-encuesta="{{ encuesta_url }}"{% endif %}>' in html
        with open(os.path.join(RAIZ, "static", "retiros_public_tracking.js"), encoding="utf-8") as f:
            js = f.read()
        assert "wrap.dataset.encuesta" in js and "'atencion=' + chosen" in js

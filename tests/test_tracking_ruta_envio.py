"""Tarjeta "Ruta del envío" en templates/transporte/public_tracking.html.

Daniel (2026-08-13), mostrando una referencia visual con mapas y datos
elaborados (confianza %, ventana de hora, "2 paradas antes"): "podemos
hacer algo así parecido con muchísima tecnología... integrando los mapas
tipo ILUS e información interesante".

Se le presentó qué de esa referencia es un dato REAL en el sistema hoy y
qué no, y confirmó explícitamente 3 decisiones:
  1. Sin datos inventados (confianza %, ventana de hora, conteo de
     paradas) -- solo lo que el sistema calcula de verdad.
  2. Stepper de 4 pasos reales (no 5 como la referencia).
  3. Todo en una sola pasada.

Este archivo verifica que el nuevo módulo (.route-card) cumple ESO:
usa solo datos reales (bodega ILUS fija + destino real del cliente, sin
paradas intermedias fabricadas, sin ETA con confianza falsa), y que no
rompió ninguno de los hooks de JS que ya dependían de este template (ver
memoria tracking_publico_ilus.md: tkState/tkStateLabel/tkStateSub/
tkSteps/tkTimeline/liveMap/.tk-step/.tk-step-bar/s-{color}).

Correr con:  py -m unittest tests.test_tracking_ruta_envio -v
"""
import os
import sys
import unittest

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TPL_PATH = os.path.join(BASE_DIR, "templates", "transporte", "public_tracking.html")
TPL_SRC = open(TPL_PATH, encoding="utf-8", errors="ignore").read()

sys.path.insert(0, BASE_DIR)


def _chile_fmt(dt, fmt="%d/%m/%Y %H:%M"):
    if not dt:
        return ""
    if isinstance(dt, str):
        return dt
    return dt.strftime(fmt)


def _make_app():
    from flask import Flask

    app = Flask(__name__, template_folder=os.path.join(BASE_DIR, "templates"),
                static_folder=os.path.join(BASE_DIR, "static"))
    app.jinja_env.filters["chile_fmt"] = _chile_fmt
    app.jinja_env.filters["rut_fmt"] = lambda v: v

    @app.route("/t/<token>")
    def tr_public_tracking(token):
        return "stub"

    @app.route("/t/<token>/status")
    def tr_public_tracking_status(token):
        return "stub"

    return app


def _base_payload(estado, **overrides):
    step_por_estado = {"En preparación": 1, "Entregado a transporte": 2, "En ruta": 3,
                        "Entregado": 4, "Problema": 4, "Devolución": 4}
    color_por_estado = {"En preparación": "secondary", "Entregado a transporte": "info",
                         "En ruta": "primary", "Entregado": "success",
                         "Problema": "danger", "Devolución": "warning"}
    p = {
        "ok": True, "doc": "FCV-11290", "cliente": "Cliente de prueba",
        "destino": "Las Condes", "region": "Región Metropolitana",
        "courier": "Transporte Felca", "courier_logo": "", "manifiesto": "MAN-2026-0042",
        "estado": estado, "estado_color": color_por_estado[estado],
        "estado_icon": "bi-truck", "estado_step": step_por_estado[estado],
        "eventos": [], "proof": None, "entregado_at": None,
        "bultos": 6, "fecha_emision": "2026-08-10", "fecha_entrega": "2026-08-13",
        "driver_live": None,
    }
    p.update(overrides)
    return p


def _render(data):
    from flask import render_template
    app = _make_app()
    with app.test_request_context():
        return render_template("transporte/public_tracking.html",
                                token="x" * 40, data=data, gmaps_key="")


class TestSoloDatosReales(unittest.TestCase):
    """Decisión de Daniel #1: nada de confianza %, ventana de hora, ni
    conteo de paradas fabricado."""

    def test_no_hay_porcentaje_de_confianza_falso(self):
        # "confianza" en minúscula aparece en el comentario Jinja que
        # explica esta misma decisión -- se busca la frase tal como
        # aparecería en la UI, no cualquier mención textual.
        html = _render(_base_payload("En ruta"))
        for rastro in ("Confianza alta", "87%"):
            self.assertNotIn(rastro, html)

    def test_no_hay_ventana_de_hora_inventada(self):
        html = _render(_base_payload("En ruta"))
        # Formato "16:40–18:10" (ventana con guion largo) no debe aparecer.
        self.assertNotIn("–18", html)

    def test_no_hay_conteo_de_paradas_fabricado(self):
        html = _render(_base_payload("En ruta"))
        self.assertNotIn("paradas antes", html)

    def test_origen_es_la_bodega_real_ilus(self):
        html = _render(_base_payload("En preparación"))
        self.assertIn("Quilicura", html)

    def test_destino_es_el_dato_real_del_pedido(self):
        html = _render(_base_payload("En preparación", destino="Ñuñoa"))
        self.assertIn("Ñuñoa", html)

    def test_no_se_muestra_si_no_hay_destino_real(self):
        html = _render(_base_payload("En preparación", destino=""))
        self.assertNotIn('class="route-card', html)


class TestStepperSigueSiendo4Pasos(unittest.TestCase):
    """Decisión de Daniel #2: 4 pasos reales, no 5."""

    def test_solo_4_pasos_en_el_stepper(self):
        html = _render(_base_payload("En preparación"))
        self.assertEqual(html.count('class="step tk-step'), 4)

    def test_no_aparece_preparacion_y_control_como_paso_aparte(self):
        self.assertNotIn("Preparación y control", TPL_SRC)


class TestMarcadorPorEstado(unittest.TestCase):
    """El marcador de la ruta debe reflejar honestamente el estado real:
    quieto en preparación/retirado, viajando (animación, no posición
    exacta) en ruta, en el destino cuando ya se entregó."""

    def test_preparacion_marcador_cerca_del_origen(self):
        html = _render(_base_payload("En preparación"))
        self.assertIn("--route-pct:4%", html)

    def test_en_ruta_marcador_animado_no_fijo(self):
        html = _render(_base_payload("En ruta"))
        self.assertIn('route-marker is-live', html)

    def test_entregado_marcador_en_destino_con_check(self):
        html = _render(_base_payload("Entregado", entregado_at="13/08/2026 10:00"))
        self.assertIn("--route-pct:96%", html)
        self.assertIn("route-pt dest is-done", html)

    def test_problema_marcador_detenido_no_animado(self):
        html = _render(_base_payload("Problema"))
        self.assertNotIn('route-marker is-live', html)
        self.assertIn('route-card is-warn', html)


class TestHooksDeJsIntactos(unittest.TestCase):
    """Ver tracking_publico_ilus.md: estos IDs/clases alimentan el
    sondeo en vivo (JS) -- si se renombran, el seguimiento deja de
    actualizarse solo. La tarjeta nueva no debía tocarlos."""

    def test_ids_criticos_presentes(self):
        for hook in ("id=\"tkState\"", "id=\"tkStateLabel\"", "id=\"tkStateSub\"",
                     "id=\"tkSteps\"", "id=\"tkTimeline\"", "id=\"liveMap\""):
            self.assertIn(hook, TPL_SRC, f"falta el hook crítico {hook}")

    def test_clases_criticas_presentes(self):
        for hook in ("tk-step", "tk-step-bar", "tk-step-dot", "tk-step-label"):
            self.assertIn(hook, TPL_SRC, f"falta la clase crítica {hook}")


class TestRenderRealSinErrores(unittest.TestCase):
    def test_los_6_estados_renderizan_sin_traceback(self):
        for estado in ("En preparación", "Entregado a transporte", "En ruta",
                       "Entregado", "Problema", "Devolución"):
            html = _render(_base_payload(estado))
            self.assertNotIn("Traceback", html, f"estado {estado} rompió el render")
            self.assertIn("route-card", html, f"estado {estado} no mostró la ruta")


if __name__ == "__main__":
    unittest.main()

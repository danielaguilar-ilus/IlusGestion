# -*- coding: utf-8 -*-
"""2026-10-23 · Daniel: «mejorar el performance de este modal (Generar OT)… tecnología del 2028».

El asistente (~765 KB) ya no viaja dentro de las tres páginas que lo usan: se descarga bajo demanda desde /ot/modal-crear con un cargador
de ~3 KB (static/ot_modal_lazy.js), en un momento ocioso / al acercar el mouse o el dedo / de inmediato con ?crear=1.
"""
import os
import re
import shutil
import subprocess
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _leer(*p):
    with open(os.path.join(RAIZ, *p), encoding="utf-8") as fh:
        return fh.read()


APP = _leer("app.py")
LAZY = _leer("static", "ot_modal_lazy.js")
MODAL = _leer("templates", "ot2", "_modal_crear.html")
PAGINAS = {"panel": _leer("templates", "ot2", "panel.html"), "ticket": _leer("templates", "tickets", "ficha.html"),
           "repuestos": _leer("templates", "clientes_hub", "repuestos.html")}


class TestLasPaginasYaNoTraenElAsistente(unittest.TestCase):
    def test_ninguna_pagina_lo_incluye_y_las_tres_cargan_el_cargador(self):
        for nombre, h in PAGINAS.items():
            self.assertNotIn('{% include "ot2/_modal_crear.html" %}', h, nombre)
            self.assertIn("ot_modal_lazy.js", h, nombre)

    def test_el_cargador_es_chico(self):
        self.assertLess(len(LAZY.encode("utf-8")), 8 * 1024)
        self.assertGreater(len(MODAL.encode("utf-8")), 400 * 1024)


class TestRutaDelFragmento(unittest.TestCase):
    def setUp(self):
        i = APP.index('@app.route("/ot/modal-crear", methods=["GET"])')
        self.f = APP[i:APP.index('@app.route("/ot/")', i)]

    def test_exige_sesion_y_valida_con_etag(self):
        self.assertIn("@_mant_required", self.f)
        self.assertIn('request.headers.get("If-None-Match") == etag', self.f)
        self.assertIn("make_response(\"\", 304)", self.f)
        self.assertIn('"private, no-cache"', self.f)

    def test_se_recuerda_por_combinacion_de_permisos(self):
        self.assertIn('p.get("superadmin")', self.f)
        self.assertIn('p.get("mant_tipo_interno_crear")', self.f)
        self.assertIn("_es_rol_tecnico()", self.f)
        self.assertIn('p.get("cat_clasificacion")', self.f)
        self.assertIn("len(_OT_MODAL_CREAR_CACHE) < 16", self.f)


class TestElCargador(unittest.TestCase):
    def test_metodos_publicos_que_usan_las_paginas(self):
        for m in ("abrir", "abrirDesdeTicket", "abrirDesdeHallazgos", "cerrar"):
            self.assertIn("'" + m + "'", LAZY)

    def test_ejecuta_scripts_en_orden_y_espera_los_externos(self):
        self.assertIn("nuevo.async = false;", LAZY)
        self.assertIn("nuevo.onload = function () { ejecutar(scripts, i + 1, ok, fallo); };", LAZY)

    def test_se_puede_reintentar_si_falla_y_avisa(self):
        self.assertIn("promesa = null;      // permite reintentar", LAZY)
        self.assertIn("No se pudo cargar el formulario de OT", LAZY)

    def test_precarga_al_estar_ocioso_y_por_intencion_y_con_crear_1(self):
        self.assertIn("requestIdleCallback", LAZY)
        self.assertIn("'pointerover'", LAZY)
        self.assertIn("'touchstart'", LAZY)
        self.assertIn("get('crear') === '1'", LAZY)

    def test_el_cascaron_se_quita_antes_de_ejecutar_el_asistente(self):
        self.assertLess(LAZY.index("delete window.OT2C"), LAZY.index("ejecutar(scripts, 0"))

    @unittest.skipUnless(shutil.which("node"), "node no está instalado")
    def test_sintaxis_del_cargador(self):
        r = subprocess.run(["node", "--check", os.path.join(RAIZ, "static", "ot_modal_lazy.js")], capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, r.stderr)


class TestElAsistenteSirveCargadoTarde(unittest.TestCase):
    def test_el_inicio_ya_no_depende_de_DOMContentLoaded(self):
        self.assertIn("function _o2mInicio(){", MODAL)
        self.assertIn("if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', _o2mInicio);", MODAL)
        self.assertIn("else setTimeout(_o2mInicio, 0);", MODAL)
        self.assertNotIn("document.addEventListener('DOMContentLoaded', function(){\n    var a = document.getElementById('o2mAtras')", MODAL)


if __name__ == "__main__":
    unittest.main()

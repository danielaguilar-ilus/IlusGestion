"""Sesión expirada / pérdida de lo que se estaba cargando (Daniel + Lenin, 2026-10-06).

Sin BD ni Flask: revisiones del código fuente. Correr con:  py -m unittest tests.test_sesion_borrador_bodega
"""
import os
import re
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _leer(*p):
    with open(os.path.join(RAIZ, *p), encoding="utf-8") as fh:
        return fh.read().replace(chr(13) + chr(10), chr(10))


class TestBorradorDelRepuesto(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.pane = _leer("templates", "mantenciones", "_repuestos_bodega_pane.html")

    def test_cada_cambio_de_paso_respalda_el_borrador(self):
        i = self.pane.index("function rbRefreshStepStates(){")
        cuerpo = self.pane[i:i + 1200]
        self.assertIn("rbDraftGuardar", cuerpo)
        self.assertIn("modalRepStock", cuerpo)

    def test_se_respalda_justo_antes_de_enviar(self):
        i = self.pane.index("rbDraftGuardar();   // 2026-10-06")
        self.assertIn("repuestos-stock", self.pane[i:i + 300])

    def test_al_cargar_la_pantalla_se_ofrece_retomar_sin_tocar_nuevo_repuesto(self):
        self.assertIn("Tienes un repuesto sin guardar", self.pane)
        self.assertIn("rbAbrirNuevo(false, {recuperarYa: true})", self.pane)
        self.assertIn("opts.recuperarYa", self.pane)

    def test_el_borrador_se_limpia_al_guardar_de_verdad(self):
        i = self.pane.index("RB_DIRTY = true;")
        self.assertIn("rbDraftLimpiar();", self.pane[i:i + 700])


class TestSesionExpirada(unittest.TestCase):
    def test_un_401_por_inactividad_avisa_en_cualquier_pantalla_y_vuelve_a_la_misma(self):
        base = _leer("templates", "base.html")
        self.assertIn("r.status === 401", base)
        self.assertIn("SESSION_IDLE_EXPIRED", base)
        self.assertIn("'/login?next=' + encodeURIComponent(", base)

    def test_next_del_login_solo_acepta_rutas_propias(self):
        app = _leer("app.py")
        i = app.index('next_url = request.args.get("next")')
        bloque = app[i:i + 700]
        self.assertIn('next_url.startswith("/")', bloque)
        self.assertIn('next_url.startswith("//")', bloque)


if __name__ == "__main__":
    unittest.main()

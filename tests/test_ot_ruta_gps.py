"""Ruta de la OT (Daniel, 2026-10-01): tocar Ruta ya NO captura la ubicación de ejecución (el técnico
está en la oficina o en su casa); la ubicación real la captura el botón "Capturar mi ubicación" del
checklist y esa captura es la que valida la ubicación en el PDF.

Sin BD ni Flask: revisiones del código fuente. Correr con:  py -m unittest tests.test_ot_ruta_gps
"""
import os
import re
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _leer(*partes):
    with open(os.path.join(RAIZ, *partes), encoding="utf-8") as fh:
        return fh.read().replace("\r\n", "\n")


def _funcion_js(src, firma):
    i = src.index(firma)
    k = src.index("{", i)
    prof = 0
    for p in range(k, len(src)):
        if src[p] == "{":
            prof += 1
        elif src[p] == "}":
            prof -= 1
            if prof == 0:
                return src[i:p + 1]
    raise AssertionError("no cierra " + firma)


class TestRutaNoCapturaUbicacion(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.html = _leer("templates", "ot2", "detalle.html")
        cls.ruta = _funcion_js(cls.html, "function otdIniciarRuta(app)")

    def test_otdIniciarRuta_ya_no_llama_al_checkin(self):
        codigo = re.sub(r"/\*.*?\*/", "", self.ruta, flags=re.S)
        self.assertNotIn("otdGpsCheckin()", codigo)

    def test_el_ping_de_ruta_sigue_solo_si_va_en_camino(self):
        self.assertIn("otdPingIniciar();", self.ruta)
        self.assertLess(self.ruta.index("app === 'saltado'"), self.ruta.index("otdPingIniciar();"))

    def test_el_boton_capturar_mi_ubicacion_sigue_existiendo(self):
        self.assertIn("Capturar mi ubicación", self.html)
        self.assertIn("otdCapturarGps(", self.html)


class TestPdfValidaConElBotonGps(unittest.TestCase):
    def test_la_tarea_gps_con_coordenadas_valida_la_ubicacion(self):
        app = _leer("app.py")
        i = app.index("ubic_validada = bool(")
        bloque = app[i:i + 1200]
        self.assertIn('(_t.get("tipo_respuesta") or "").lower() != "gps"', bloque)
        self.assertIn("ubic_validada = True", bloque)
        self.assertIn('_vj.get("lat")', bloque)


class TestBotonManualPorEquipo(unittest.TestCase):
    """Daniel (2026-10-01): acceso directo al manual del equipo desde la tarjeta de cada equipo."""

    def test_el_boton_abre_el_modal_en_la_pestana_manual(self):
        html = _leer("templates", "ot2", "detalle.html")
        self.assertIn("otdRepAbrir({{ e.id }}, 'repuesto', 'docs')", html)
        self.assertIn("window.otdRepAbrir = async function(mid, modo, tabInicial)", html)
        self.assertIn("if (tabInicial === 'docs')", html)

    def test_el_boton_esta_dentro_del_bloque_de_quien_puede_pedir_repuestos(self):
        html = _leer("templates", "ot2", "detalle.html")
        i = html.index('<div class="otd-desv-btns">')
        j = html.index("otdRepAbrir({{ e.id }}, 'repuesto', 'docs')")
        self.assertLess(i, j, "va con los demás botones, bajo el mismo permiso que 'Solicitar repuesto'")


if __name__ == "__main__":
    unittest.main()

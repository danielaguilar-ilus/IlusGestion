# -*- coding: utf-8 -*-
"""2026-10-23 · Paso «Desinstalación» del asistente «Nueva OT» (Daniel, probando un pedido con desinstalación): la fecha y el horario se eligen
en UN calendario compacto con su «ficha» al lado, en vez de un campo de fecha + dos horas + cinco botones sueltos; y los objetos del paso se
achican sin perder nitidez (todo en texto/bordes/degradados, nada escalado).

Texto de los archivos (sin cargar app.py). Lo que se ve es lo que viaja: el calendario escribe solo S.desinst.fecha.
"""
import os
import re
import shutil
import subprocess
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _leer(*partes):
    with open(os.path.join(RAIZ, *partes), encoding="utf-8") as fh:
        return fh.read().replace("\r\n", "\n")


class TestCalendarioDeLaDesinstalacion(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.js = _leer("templates", "ot2", "_modal_crear.html")
        cls.css = _leer("static", "ot_evento.css")
        i = cls.js.index("function _diRegFecha(el){")
        cls.fecha = cls.js[i:cls.js.index("function _diRegAvisos(el){", i)]

    def test_ya_no_hay_campo_de_fecha_ni_cinco_botones_sueltos(self):
        self.assertNotIn('type="date"', self.fecha)
        self.assertNotIn("o2mDiFechaIn", self.fecha)
        self.assertIn("dc-grid", self.fecha)
        self.assertIn("OT2C.diFecha(", self.js)

    def test_domingos_y_dias_anteriores_a_la_instalacion_no_se_pueden_elegir(self):
        g = self.js[self.js.index("function _diCalGrid(){"):self.js.index("function _diAtajos(){")]
        self.assertIn("dow === 0", g)                       # domingo
        self.assertIn("f < min", g)                         # antes de la instalación (o pasado)
        self.assertIn('aria-disabled="true"', g)            # y se anuncia como no disponible
        self.assertIn("(f === S.fecha ? ' inst'", g)        # día de la instalación marcado

    def test_el_calendario_escribe_solo_la_fecha_de_la_desinstalacion(self):
        reg = self.fecha + self.js[self.js.index("function diCalMes("):self.js.index("function diBuscarHueco(")]
        self.assertNotIn("S.fecha =", reg)
        self.assertNotIn("setFecha(", reg)
        self.assertIn("diCalMes:diCalMes", self.js)         # exportado a OT2C
        # comparte el mes del servidor con el calendario de la Agenda (no inventa otra fuente)
        self.assertIn("/servicio-tecnico/api/calendario/mes/", self.js[self.js.index("function _diCalCargar()"):self.js.index("function _diDiasEntre(")])

    def test_horario_y_atajos_siguen_ahi(self):
        for pedazo in ('id="o2mDiHini"', 'id="o2mDiHfin"', "OT2C.diBuscarHueco()", "fecha_sugerida", "_diHabil("):
            self.assertIn(pedazo, self.js[self.js.index("function _diAtajos(){"):self.js.index("function _diRegAvisos(el){")], pedazo)

    def test_estilos_de_la_ficha_y_objetos_mas_chicos(self):
        for clase in (".o2m-dc{", ".dc-d.sel{", ".dc-side{", ".dc-big b{", ".dc-hrs input[type=time]{"):
            self.assertIn(clase, self.css, clase)
        # objetos más chicos que antes (el de 76 px / 1.4rem / 14 px de relleno ya no existe)
        self.assertNotIn("min-height:76px;display:block", self.css)
        self.assertIn("min-height:56px;display:block", self.css)
        # REGLA #3: en celular los botones siguen en 44 px (el achique es solo de escritorio)
        self.assertRegex(self.css, r"\.dc-at\{min-height:44px")
        self.assertRegex(self.css, r"@media \(min-width:641px\)\{\.dc-at\{min-height:30px\}\}")
        # nada escalado con transform en reposo (la nitidez no se pierde): solo el hover levanta 1 px
        self.assertNotRegex(self.css, r"\.o2m-dc[^{]*\{[^}]*transform:\s*scale")

    @unittest.skipUnless(shutil.which("node"), "node no está instalado")
    def test_script_valido(self):
        bloques = re.findall(r"<script(?![^>]*src)[^>]*>(.*?)</script>", self.js, re.S)
        import tempfile
        b = re.sub(r"\{#.*?#\}", "", bloques[0], flags=re.S)
        b = re.sub(r"\{\{.*?\}\}", "0", b, flags=re.S)
        b = re.sub(r"\{%.*?%\}", "", b, flags=re.S)
        with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False, encoding="utf-8") as f:
            f.write(b)
        r = subprocess.run(["node", "--check", f.name], capture_output=True, text=True)
        os.unlink(f.name)
        self.assertEqual(r.returncode, 0, r.stderr[:300])


if __name__ == "__main__":
    unittest.main()

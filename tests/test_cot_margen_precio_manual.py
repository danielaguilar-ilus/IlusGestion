# -*- coding: utf-8 -*-
"""💰 6-nov-2026 — Cotizador, cuadro «Costo y margen» (solo interno): con un precio ESCRITO A MANO el margen ya no sale $0 (Daniel: «el cuadro indica que no
hay margen, eso está mal, ¿no?»). Si se conoce el costo, margen = cobrado − costo (puede ser pérdida); sin costo registrado (precio fijo de la categoría)
dice «sin desglose» en vez de mostrar $0.
Correr:  python -m pytest tests/test_cot_margen_precio_manual.py -q -p no:warnings"""
import os
import re
import shutil
import subprocess
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _leer(*ruta):
    with open(os.path.join(RAIZ, *ruta), encoding="utf-8") as fh:
        return fh.read().replace("\r\n", "\n")


JS = _leer("static", "tickets_cotizaciones.js")
HTML = _leer("templates", "tickets", "cotizaciones.html")
PDF = _leer("templates", "tickets", "cotizacion_pdf.html")


class TestCalculo(unittest.TestCase):
    def test_con_costo_conocido_el_margen_se_calcula_tambien_con_precio_a_mano(self):
        i = JS.index("if (!accesorio && totBase != null && totBase > 0){")
        f = JS[i:i + 900]
        self.assertIn("if (it._costoUnit != null){", f)
        self.assertNotIn("it.precio_manual == null && it._costoUnit != null", f)     # antes: a mano => «sin desglose» y margen $0
        self.assertIn("manual: it.precio_manual != null", f)
        self.assertIn("intFijo += totBase;", f)                                       # sin costo registrado sigue siendo «precio fijo»

    def test_la_linea_de_la_fila_distingue_a_mano_perdida_y_precio_fijo(self):
        self.assertIn("(precio escrito a mano; no va al PDF)", JS)
        self.assertIn("pierdes ", JS)
        self.assertIn("precio fijo de la categoría, sin costo registrado", JS)
        self.assertIn(".cot-int-linea.perdida{color:#dc2626", _leer("static", "tickets_cotizaciones.css"))

    def test_nada_de_esto_llega_al_pdf(self):
        for prohibido in ("costo_unitario", "margen_pct", "cotWizInterno", "_int.", "interno:", "pierdes"):
            self.assertNotIn(prohibido, PDF, prohibido)

    def test_el_texto_del_cuadro_ya_no_dice_a_mano_porque_ahora_si_se_desglosa(self):
        self.assertIn("Ítems con precio fijo de la categoría (sin costo registrado: no se calcula su margen)", HTML)
        self.assertNotIn("Ítems con precio fijo o escrito a mano (sin desglose)", HTML)


@unittest.skipUnless(shutil.which("node"), "node no está instalado")
class TestCaja(unittest.TestCase):
    def _pintar(self, interno, desc=0):
        m = re.search(r"function _cotWizInternoPintar\(desc\)\{.*?\n\}\n", JS, re.S)
        self.assertTrue(m)
        codigo = m.group(0) + """
const els = {}; const mk = id => (els[id] = els[id] || {textContent:'', style:{}});
globalThis.document = { getElementById: id => (id === 'cotWizInterno' ? {} : mk(id)) };
globalThis._WIZ = { interno: %s };
globalThis._wizCLP = n => '$' + Math.round(n).toLocaleString('es-CL');
globalThis._cotWizActualizarUf = () => {};
_cotWizInternoPintar(%d);
console.log(JSON.stringify({costo: els.cotWizIntCosto.textContent, margen: els.cotWizIntMargen.textContent, txt: els.cotWizIntMargenTxt.textContent,
  rojo: els.cotWizIntMargen.style.color, fijoVisible: els.cotWizIntFijoRow.style.display}));""" % (interno, desc)
        out = subprocess.run(["node", "-"], input=codigo.encode("utf-8"), capture_output=True)
        self.assertEqual(out.returncode, 0, out.stderr.decode("utf-8", "replace"))
        return __import__("json").loads(out.stdout.decode("utf-8").strip())

    def test_solo_precio_fijo_dice_sin_desglose_no_cero(self):
        r = self._pintar("{costo: 0, margen: 0, fijo: 72369}")
        self.assertEqual(r["costo"], "sin desglose")
        self.assertEqual(r["margen"], "sin desglose")
        self.assertEqual(r["fijoVisible"], "")                       # la fila de precio fijo se ve

    def test_con_costo_muestra_el_margen_real_y_su_porcentaje(self):
        r = self._pintar("{costo: 55668, margen: 16701, fijo: 0}")
        self.assertEqual(r["margen"], "$16.701")
        self.assertIn("30% sobre el costo", r["txt"])
        self.assertEqual(r["rojo"], "")

    def test_un_precio_a_mano_bajo_el_costo_se_ve_como_perdida_en_rojo(self):
        r = self._pintar("{costo: 80000, margen: -7631, fijo: 0}")
        self.assertEqual(r["margen"], "−$7.631")
        self.assertEqual(r["rojo"], "#dc2626")

    def test_mezcla_de_fijo_y_con_costo_muestra_ambos(self):
        r = self._pintar("{costo: 40000, margen: 12000, fijo: 50000}")
        self.assertEqual(r["costo"], "$40.000")
        self.assertEqual(r["margen"], "$12.000")
        self.assertEqual(r["fijoVisible"], "")


if __name__ == "__main__":
    unittest.main()

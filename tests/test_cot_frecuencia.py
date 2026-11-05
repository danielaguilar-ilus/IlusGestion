# -*- coding: utf-8 -*-
"""📅 6-nov-2026 — Cotización de MANTENCIÓN: la frecuencia recomendada se puede cambiar y el PDF dice claramente que es una recomendación y que
lo cotizado es UNA visita (Daniel: «recomendamos una frecuencia pero la cotización es por una visita: no cubre la frecuencia anual»).
Correr:  python -m pytest tests/test_cot_frecuencia.py -q -p no:warnings"""
import os
import re
import shutil
import subprocess
import sys
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, RAIZ)

import cot_frecuencia as cf  # noqa: E402


def _leer(*ruta):
    with open(os.path.join(RAIZ, *ruta), encoding="utf-8") as fh:
        return fh.read().replace("\r\n", "\n")


PY = _leer("tickets_module.py")
PDF = _leer("templates", "tickets", "cotizacion_pdf.html")
HTML = _leer("templates", "tickets", "cotizaciones.html")
JS = _leer("static", "tickets_cotizaciones.js")
CSS = _leer("static", "tickets_cotizaciones.css")


class TestTextos(unittest.TestCase):
    def test_normalizar(self):
        self.assertEqual(cf.normalizar(None), 2)
        self.assertEqual(cf.normalizar(""), 2)
        self.assertEqual(cf.normalizar("abc"), 2)
        self.assertEqual(cf.normalizar(0), 2)
        self.assertEqual(cf.normalizar(4), 4)
        self.assertEqual(cf.normalizar("6"), 6)
        self.assertEqual(cf.normalizar(99), 12)
        self.assertEqual(cf.normalizar(-3), 1)

    def test_el_parrafo_recomienda_y_aclara_que_es_una_visita(self):
        for n in (1, 2, 4, 12):
            p = cf.parrafo_mantencion(n)
            self.assertIn("Recomendamos ejecutar", p)
            self.assertIn("UNA (1) visita", p)
            self.assertIn("NO está incluida", p)
        self.assertIn("separación aproximada de seis meses", cf.parrafo_mantencion(2))
        self.assertNotIn("seis meses", cf.parrafo_mantencion(4))
        self.assertIn("4 mantenciones preventivas al año", cf.parrafo_mantencion(4))
        self.assertIn("1 mantención preventiva al año", cf.parrafo_mantencion(1))

    def test_ya_no_dice_que_la_cotizacion_cubre_la_frecuencia(self):
        self.assertNotIn("Se recomienda ejecutar", cf.parrafo_mantencion(2))

    def test_uf_solo_si_hay_uf(self):
        self.assertIn("UF vigente", cf.parrafo_mantencion(2, True))
        self.assertNotIn("UF vigente", cf.parrafo_mantencion(2, False))

    def test_las_recomendaciones_siguen_la_frecuencia_elegida(self):
        # antes decían siempre «dos intervenciones anuales»
        r4 = " ".join(cf.recomendaciones_por_defecto(4, 0))
        self.assertIn("cuatro intervenciones anuales", r4)
        self.assertIn("cada tres meses", r4)
        self.assertNotIn("dos intervenciones", r4)
        r1 = " ".join(cf.recomendaciones_por_defecto(1, 3))
        self.assertIn("una intervención anual", r1)
        self.assertIn("3 mantención(es) previa(s)", r1)
        self.assertIn("esta cotización cubre solo una visita", r1)
        self.assertEqual(len(cf.recomendaciones_por_defecto(2, 0)), 4)
        self.assertEqual(len(cf.recomendaciones_por_defecto(2, 5)), 4)

    def test_frases(self):
        self.assertEqual(cf.veces_al_anio(1), "una vez al año")
        self.assertEqual(cf.veces_al_anio(2), "dos veces al año")
        self.assertEqual(cf.veces_al_anio(12), "doce veces al año")
        self.assertIn("una por mes", cf.intervenciones(12))


class TestServidorYPdf(unittest.TestCase):
    def test_el_servidor_usa_el_modulo_en_vez_de_textos_fijos(self):
        self.assertIn("import cot_frecuencia as _cotfrec", PY)
        self.assertIn("_cotfrec.recomendaciones_por_defecto(cot.get(\"frecuencia_anual\"), _previas)", PY)
        self.assertIn("_cotfrec.parrafo_mantencion(_frecuencia, bool(uf_info))", PY)
        self.assertIn('"aviso_una_visita": _cotfrec.AVISO_UNA_VISITA', PY)
        self.assertNotIn("Programar dos intervenciones anuales", PY)
        self.assertNotIn("La presente cotización corresponde al valor", PY)

    def test_el_pdf_muestra_la_frecuencia_como_recomendada_y_el_aviso(self):
        self.assertIn("Frecuencia recomendada (veces al año)", PDF)
        self.assertNotIn('<div class="kpi-lbl">Mantenciones al año</div>', PDF)
        self.assertIn('<div class="aviso-una-visita">', PDF)
        self.assertIn("rico_textos.aviso_una_visita and kpis.mantenciones_anio", PDF)   # solo en mantención

    def test_el_aviso_no_aparece_en_visita_tecnica_ni_instalacion(self):
        # mantenciones_anio solo viene con valor para mantención (ver kpis en tickets_module.py)
        self.assertIn('_cotfrec.normalizar(cot.get("frecuencia_anual")) if cot.get("tipo_servicio") == "mantencion" else None', PY)


class TestCotizador(unittest.TestCase):
    def test_el_campo_es_editable_con_atajos_y_aviso_a_la_vista(self):
        self.assertIn('id="cotWizFrecuenciaAnual" type="number" min="1" max="12"', HTML)
        self.assertIn("Frecuencia de mantención que recomendamos (veces al año)", HTML)
        self.assertIn("cotWizFrecuenciaElegir({{ n }})", HTML)
        self.assertIn("esta cotización cubre UNA visita y no incluye la frecuencia anual", HTML)
        self.assertIn('id="campoContenidoPdfMantencion4"', HTML)       # el id que oculta/muestra según el tipo de servicio sigue

    def test_los_atajos_miden_44px_y_el_elegido_va_en_rojo_ilus(self):
        self.assertIn(".cot-wiz-frec-chip{min-width:44px;min-height:44px", CSS)
        self.assertIn(".cot-wiz-frec-chip.on{background:#dc2626", CSS)

    def test_el_campo_se_sincroniza_al_editar_al_limpiar_y_desde_el_enlace_del_plan(self):
        self.assertIn("setV('cotWizFrecuenciaAnual', c.frecuencia_anual);", JS)
        self.assertGreaterEqual(JS.count("cotWizFrecuenciaCambio();"), 3)
        i = JS.index("function cotWizFrecuenciaElegir(n)")
        self.assertIn("el.value = String(n);", JS[i:i + 300])

    @unittest.skipUnless(shutil.which("node"), "node no está instalado")
    def test_el_aviso_del_cotizador_se_actualiza_con_el_numero(self):
        m = re.search(r"function cotWizFrecuenciaCambio\(\)\{.*?\n\}\n", JS, re.S)
        self.assertTrue(m)
        codigo = m.group(0) + """
const hint = {textContent: ''}; const chips = [1,2,4].map(n => ({dataset:{n:String(n)}, on:false, classList:{toggle(c, v){ this.v = v; }}}));
globalThis.document = { getElementById: id => id === 'cotWizFrecuenciaAnual' ? {value:'4'} : (id === 'cotWizFrecuenciaHint' ? hint : null),
  querySelectorAll: () => chips };
cotWizFrecuenciaCambio();
console.log(hint.textContent, '|', chips.map(c => c.classList.v).join(','));
document.getElementById = id => id === 'cotWizFrecuenciaAnual' ? {value:''} : (id === 'cotWizFrecuenciaHint' ? hint : null);
cotWizFrecuenciaCambio(); console.log(hint.textContent);"""
        out = subprocess.run(["node", "-"], input=codigo.encode("utf-8"), capture_output=True)
        self.assertEqual(out.returncode, 0, out.stderr.decode("utf-8", "replace"))
        l1, l2 = out.stdout.decode("utf-8").strip().split("\n")
        self.assertIn("Recomendamos 4 mantenciones al año.", l1)
        self.assertIn("UNA visita", l1)
        self.assertTrue(l1.endswith("| false,false,true"))              # el atajo «4» queda encendido
        self.assertIn("sin valor: 2 al año", l2)


if __name__ == "__main__":
    unittest.main()

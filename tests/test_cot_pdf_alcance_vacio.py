# -*- coding: utf-8 -*-
"""📄 6-nov-2026 — PDF de cotización: «Alcance de la mantención» ya no sale VACÍO cuando solo se escribió la recomendación (COT-000210), y la lista de
«Recomendación de Servicio» no se parte entre páginas.
Correr:  python -m pytest tests/test_cot_pdf_alcance_vacio.py -q -p no:warnings"""
import os
import re
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _leer(*ruta):
    with open(os.path.join(RAIZ, *ruta), encoding="utf-8") as fh:
        return fh.read().replace("\r\n", "\n")


PY = _leer("tickets_module.py")
PDF = _leer("templates", "tickets", "cotizacion_pdf.html")


def _tramo(desde, hasta):
    i = PY.index(desde)
    return PY[i:PY.index(hasta, i)]


class TestCadaBloqueSeCompletaPorSeparado(unittest.TestCase):
    def test_mantencion_se_rellena_si_falta_alguno_de_los_dos(self):
        self.assertIn('if cot.get("tipo_servicio") == "mantencion" and (not alcance_lineas or not recomendacion_lineas):', PY)
        self.assertNotIn('"tipo_servicio") == "mantencion" and not alcance_lineas and not recomendacion_lineas', PY)
        t = _tramo('if cot.get("tipo_servicio") == "mantencion" and (not alcance_lineas', 'elif cot.get("tipo_servicio") == "visita_tecnica"')
        self.assertIn("alcance_lineas = alcance_lineas or [", t)                 # lo escrito a mano manda
        self.assertIn("recomendacion_lineas = recomendacion_lineas or _cotfrec.recomendaciones_por_defecto(", t)

    def test_visita_tecnica_e_instalacion_igual(self):
        t = _tramo('elif cot.get("tipo_servicio") == "visita_tecnica"', "modo_rico = ")
        self.assertIn('(not alcance_lineas or not recomendacion_lineas)', t)
        self.assertEqual(t.count("alcance_lineas = alcance_lineas or"), 2)       # visita técnica (lista) e instalación (textos)
        self.assertEqual(t.count("recomendacion_lineas = recomendacion_lineas or"), 3)
        self.assertNotIn("and not alcance_lineas and not recomendacion_lineas", t)

    def test_lo_que_la_persona_escribio_no_se_pisa(self):
        # alcance_lineas/recomendacion_lineas se leen del texto escrito ANTES; el relleno es siempre «x or defecto»
        i = PY.index("alcance_lineas = _lineas(cot.get(\"alcance\"))")
        self.assertIn("recomendacion_lineas = _lineas(cot.get(\"recomendacion\"))", PY[i:i + 200])
        for linea in re.findall(r"^\s+(?:alcance_lineas|recomendacion_lineas) = (?!_lineas)(.*)$", _tramo(
                'if cot.get("tipo_servicio") == "mantencion" and (not alcance_lineas', "modo_rico = "), re.M):
            self.assertRegex(linea, r"^(alcance_lineas|recomendacion_lineas) or|^_a_inst2|^_r_inst2|^\[", linea)


class TestPaginacionDelPdf(unittest.TestCase):
    def test_la_recomendacion_no_se_parte_ni_su_titulo_queda_solo(self):
        self.assertIn(".recomendacion-box { page-break-inside: avoid; break-inside: avoid; }", PDF)
        self.assertIn(".barra-titulo { page-break-after: avoid; break-after: avoid; }", PDF)

    def test_el_aviso_de_una_visita_usa_las_unidades_del_pdf_y_no_se_parte(self):
        i = PDF.index(".aviso-una-visita {")
        f = PDF[i:i + 400]
        self.assertIn("font-size: 7.8pt", f)                          # la letra va en pt como el resto del PDF (los bordes de 1px también existen en el resto)
        self.assertNotRegex(f, r"font-size:\s*[0-9.]+px")
        self.assertIn("page-break-inside: avoid", f)

    def test_el_estilo_de_recomendacion_quedo_ordenado(self):
        self.assertIn("  .recomendacion-box ul { margin: 0 0 4mm; padding-left: 4mm; }", PDF)


if __name__ == "__main__":
    unittest.main()

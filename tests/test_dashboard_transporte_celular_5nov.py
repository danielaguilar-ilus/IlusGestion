# -*- coding: utf-8 -*-
"""📱 5-nov-2026 — Fase 1.2 (REGLA #61): «Compromisos de despacho» del Dashboard de Transporte en el celular.

Medido en producción a 390 px con los 50 compromisos reales: la tabla (9 columnas, 880 px de mínimo) se deslizaba 608 px por dentro.
Bajo 768 px cada compromiso pasa a UNA tarjeta con las MISMAS 9 celdas (REGLA #4.2) y cada dato con su rótulo; el escritorio no cambia.
DESPUÉS (CSS inyectado en producción a 390 px con los 50 compromisos): sin desborde lateral, tarjetas de 256 px, revisado con captura.
Correr:  python -m pytest tests/test_dashboard_transporte_celular_5nov.py -q -p no:warnings
"""
import os
import re
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

with open(os.path.join(RAIZ, "templates", "transporte", "dashboard.html"), encoding="utf-8") as fh:
    HTML = fh.read().replace("\r\n", "\n")

CELDAS = ("dh-c-doc", "dh-c-cli", "dh-c-com", "dh-c-cou", "dh-c-est", "dh-c-ent", "dh-c-emi", "dh-c-fin", "dh-c-acc")


def _bloque():
    i = HTML.index("COMPROMISOS EN EL CELULAR (2026-11-05, REGLA #61)")
    m = re.compile(r"@media\s*\(\s*max-width\s*:\s*767\.98px\s*\)\s*\{").search(HTML, i)
    j, prof = m.end(), 1
    while j < len(HTML) and prof:
        prof += {"{": 1, "}": -1}.get(HTML[j], 0)
        j += 1
    return HTML[m.end():j - 1]


BLOQUE = _bloque()


class TestCeldas(unittest.TestCase):
    def test_las_nueve_celdas_en_el_orden_de_siempre(self):
        i = HTML.index("{% for c in compromisos %}")
        fila = HTML[i:HTML.index("</tbody>", i)]
        self.assertEqual(list(CELDAS), re.findall(r'<td class="(dh-c-[a-z]+)"', fila))
        self.assertIn('<a href="{{ url_for(\'transporte_index\', q=c.nudo) }}"', fila)       # REGLA #4.2: Monitor y Tracking siguen
        self.assertIn("tr_public_tracking", fila)

    def test_los_nueve_encabezados_siguen(self):
        i = HTML.index('<table class="trx-table">')
        for th in ("Documento", "Cliente", "Comuna", "Courier", "Estado", "Entrega", "Fecha emisión", "Entregado", "Acciones"):
            self.assertIn("<th>%s</th>" % th, HTML[i:i + 800])


class TestTarjeta(unittest.TestCase):
    def test_todo_va_bajo_dh_scroll_o_su_envoltorio(self):
        sin = re.sub(r"/\*.*?\*/", "", BLOQUE, flags=re.S)
        for m in re.finditer(r"([^{}]+)\{[^{}]*\}", sin):
            for sel in m.group(1).split(","):
                sel = " ".join(sel.split())
                self.assertTrue(sel.startswith((".dh-scroll", ".trx-table-wrap:has(> .dh-scroll)")), "selector fuera del dashboard: %r" % sel)

    def test_cada_celda_tiene_su_lugar_distinto(self):
        ordenes = sorted(int(re.search(r"td\.%s\{ order:(\d+);" % c, BLOQUE).group(1)) for c in CELDAS)
        self.assertEqual(list(range(1, 10)), ordenes)

    def test_la_tabla_deja_de_ser_un_cuadro_con_scroll(self):
        self.assertIn(".dh-scroll{ overflow:visible; }", BLOQUE)
        self.assertIn(".dh-scroll table{ min-width:0; display:block; width:100%; }", BLOQUE)
        self.assertRegex(BLOQUE, r"\.dh-scroll tbody tr\{ display:flex; flex-wrap:wrap;")

    def test_solo_se_oculta_el_encabezado(self):
        for m in re.finditer(r"([^{}]+)\{([^{}]*)\}", re.sub(r"/\*.*?\*/", "", BLOQUE, flags=re.S)):
            if "display:none" in m.group(2).replace(" ", ""):
                self.assertEqual(".dh-scroll thead", " ".join(m.group(1).split()))

    def test_rotulos_botones_y_letra(self):
        for celda, rotulo in (("com", "Comuna"), ("cou", "Courier"), ("emi", "Emisión"), ("fin", "Entregado")):
            self.assertIn('td.dh-c-%s::before{ content:"%s"; }' % (celda, rotulo), BLOQUE)
        self.assertIn(".dh-scroll .dh-row-actions a{ min-height:44px;", BLOQUE)
        self.assertIn(".dh-scroll .dh-badge-estado, .dh-scroll .dh-badge-entrega{ font-size:.75rem; }", BLOQUE)

    def test_el_color_del_estado_se_conserva(self):
        for cls, color in (("trx-row-ok", "#16a34a"), ("trx-row-dang", "#dc2626"), ("trx-row-info", "#3b82f6")):
            self.assertIn(".dh-scroll tbody tr.%s{ border-left-color:%s; }" % (cls, color), BLOQUE)


class TestEscritorio(unittest.TestCase):
    def test_el_escritorio_no_cambia(self):
        fuera = HTML.replace(BLOQUE, "")
        self.assertIn(".dh-scroll{ overflow-x:auto; }", fuera)
        self.assertIn(".dh-scroll table{ min-width:880px; }", fuera)
        self.assertNotIn("dh-c-", re.sub(r"<td class=\"dh-c-[a-z]+\"", "", re.sub(r"\{#.*?#\}", "", fuera, flags=re.S)))


if __name__ == "__main__":
    unittest.main()

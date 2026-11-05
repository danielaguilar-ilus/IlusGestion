# -*- coding: utf-8 -*-
"""📱 5-nov-2026 — Fase 1.2 (REGLA #61): el Catálogo de Productos en el celular.

Medido en producción a 390 px con los 50 productos reales de la primera página: la tabla (11 columnas, 1509 px de ancho real, 960 px de mínimo) se
deslizaba 1179 px por dentro y en un celular se veía la foto y casi el SKU.

Bajo 768 px cada producto pasa a UNA tarjeta con las MISMAS celdas (REGLA #4.2: no se quita nada, tampoco el casillero de «seleccionar todos» del
superadmin ni los encabezados que ordenan, que pasan a una tira de botones de 44 px). La tabla de escritorio no cambia.

DESPUÉS (CSS inyectado en producción a 390 px con los 50 productos reales, antes de escribirlo): sin desborde lateral (scrollWidth = clientWidth) y
tarjetas de 232 a 259 px; revisado con captura (foto, nombre completo, SKU, estado, clase, fotos, manual, creado / modificado con quién y cuándo, y
las acciones).

Se prueba aquí (texto, sin navegador): que cada celda lleve su clase, que el CSS ubique todas, que nada se oculte ni se salga del @media y que el
escritorio no cambie. La medición con navegador se repite en producción tras el despliegue (390, 820 y 1280) y queda en REGLA #61.
Correr:  python -m pytest tests/test_catalogo_celular_5nov.py -q -p no:warnings
"""
import os
import re
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _leer(*partes):
    with open(os.path.join(RAIZ, *partes), encoding="utf-8") as fh:
        return fh.read().replace("\r\n", "\n")


JS = _leer("static", "catalogo_list.js")
CSS = _leer("static", "catalogo_list.css")

CELDAS = ("cat-c-foto", "cat-c-sku", "cat-c-nom", "cat-c-fam", "cat-c-cla", "cat-c-fot", "cat-c-man", "cat-c-reg", "cat-c-cre", "cat-c-mod", "cat-c-acc")


def _cuerpo_media(css, condicion):
    ultimo = None
    for m in re.finditer(r"@media\s*\(\s*max-width\s*:\s*%s\s*\)\s*\{" % re.escape(condicion), css):
        i, prof = m.end(), 1
        while i < len(css) and prof:
            prof += {"{": 1, "}": -1}.get(css[i], 0)
            i += 1
        ultimo = css[m.end():i - 1]
    return ultimo


BLOQUE = _cuerpo_media(CSS, "767.98px")


def _fila_html():
    i = JS.index("function catFilaHtml(row){")
    return JS[i:JS.index("\n}\n", i)]


class TestCadaCeldaLlevaSuClase(unittest.TestCase):
    def test_las_celdas_en_el_orden_de_siempre(self):
        f = _fila_html()
        tds = re.findall(r"<td class=\\?\"?([\w-]+)", f)
        # el casillero del superadmin va primero y solo para él; el resto, en el mismo orden de siempre
        self.assertEqual(["cat-c-chk"] + list(CELDAS), tds)
        self.assertEqual(12, len(re.findall(r"'<td class=\"cat-c-", f)), "las 11 celdas de siempre + el casillero del superadmin")

    def test_el_casillero_y_las_acciones_no_se_tocan(self):
        f = _fila_html()
        self.assertIn("catToggleSeleccion(", f)
        self.assertIn("event.stopPropagation();", f)
        self.assertIn("catAccionesCelda(row)", f)
        self.assertIn("onclick=\"catAbrirFicha('+row.id+')\"", f)

    def test_los_encabezados_siguen_ordenando_y_el_seleccionar_todos_sigue(self):
        self.assertEqual(11, len(re.findall(r"\['[^']+',\s*'[^']*'\]", re.search(r"const CAT_COLS = \[(.*?)\n\];", JS, re.S).group(1))))
        self.assertIn("onclick=\"catSortClick(", JS)
        self.assertIn('id="catChkTodos"', JS)


class TestTarjetaEnCelular(unittest.TestCase):
    def test_existe_el_bloque_y_va_al_final_del_archivo(self):
        self.assertIsNotNone(BLOQUE, "debe existir @media (max-width:767.98px)")
        self.assertGreater(CSS.index("CATÁLOGO EN EL CELULAR (2026-11-05, REGLA #61)"), CSS.index(".cata-mas{"))

    def test_todo_va_bajo_la_tabla_del_catalogo_o_su_envoltorio(self):
        sin_coment = re.sub(r"/\*.*?\*/", "", BLOQUE, flags=re.S)
        for m in re.finditer(r"([^{}]+)\{[^{}]*\}", sin_coment):
            for sel in m.group(1).split(","):
                sel = " ".join(sel.split())
                self.assertTrue(sel.startswith(("table.cat-table", ".cat-table-wrap", ".cat-pg-btn")),
                                "selector fuera del catálogo: %r (afectaría a otras pantallas)" % sel)

    def test_cada_celda_tiene_su_lugar_y_son_todos_distintos(self):
        ordenes = {}
        for celda in CELDAS + ("cat-c-chk",):
            m = re.search(r"td\.%s\{order:(\d+);" % celda, BLOQUE)
            self.assertIsNotNone(m, celda)
            ordenes[celda] = int(m.group(1))
        self.assertEqual(list(range(1, 13)), sorted(ordenes.values()), "los 12 lugares de la tarjeta, sin repetirse")
        # la foto y el nombre arriba; las acciones al final
        self.assertEqual(1, ordenes["cat-c-foto"])
        self.assertEqual(2, ordenes["cat-c-nom"])
        self.assertEqual(12, ordenes["cat-c-acc"])

    def test_ninguna_celda_se_oculta(self):
        """REGLA #4.2: las celdas siguen en la tarjeta. Solo se ocultan los encabezados que no ordenan ni son el «seleccionar todos»."""
        for m in re.finditer(r"([^{}]+)\{([^{}]*)\}", re.sub(r"/\*.*?\*/", "", BLOQUE, flags=re.S)):
            sel, props = " ".join(m.group(1).split()), m.group(2)
            if "display:none" in props.replace(" ", ""):
                self.assertIn(sel, ("table.cat-table thead th:not(.cat-th-sort):not(.cat-th-chk)", "table.cat-table thead tr::-webkit-scrollbar"),
                              "se oculta algo más que los encabezados sin orden: %r" % sel)

    def test_creado_y_modificado_dicen_que_son_sin_perder_quien_ni_cuando(self):
        self.assertIn('td.cat-c-cre::before{content:"Creado";}', BLOQUE)
        self.assertIn('td.cat-c-mod::before{content:"Última modificación";}', BLOQUE)

    def test_el_nombre_va_completo_y_las_filas_son_tarjetas(self):
        """REGLA #15: sin ancho mínimo ni máximo que lo corte; una tarjeta por producto, sin scroll interno."""
        self.assertIn("table.cat-table .cat-nombre{min-width:0;max-width:none;}", BLOQUE)
        self.assertRegex(BLOQUE, r"\.cat-table-wrap\{max-height:none;overflow:visible;")
        self.assertRegex(BLOQUE, r"table\.cat-table\{display:block;width:100%;min-width:0;\}")
        self.assertRegex(BLOQUE, r"table\.cat-table tbody tr\{display:flex;flex-wrap:wrap;")

    def test_botones_de_44px_y_letra_de_12px(self):
        self.assertRegex(BLOQUE, r"table\.cat-table thead th\{[^}]*min-height:44px")
        self.assertRegex(BLOQUE, r"\.cat-act,\.cat-pg-btn\{width:44px;height:44px;min-width:44px;min-height:44px;\}")
        for clase in (".cat-chip-modelo", ".cat-qc-quien", ".cat-badge-fotos", ".cat-badge-reg", ".cat-badge-clase", ".cat-sku"):
            self.assertIn("table.cat-table %s" % clase, BLOQUE)
        self.assertRegex(BLOQUE, r"\.cat-sku\{font-size:\.75rem;\}")


class TestElEscritorioNoCambia(unittest.TestCase):
    def test_la_tabla_de_escritorio_sigue_igual(self):
        fuera = CSS.replace(BLOQUE, "")
        self.assertIn("table.cat-table{width:100%;border-collapse:separate;border-spacing:0;font-size:.83rem;min-width:960px;}", fuera)
        self.assertIn("overflow:auto;max-height:calc(100vh - 300px);", fuera)
        self.assertIn("table.cat-table thead th{position:sticky;top:0;z-index:2;background:#0a0a0a;", fuera)

    def test_las_clases_nuevas_no_tienen_estilo_fuera_del_celular(self):
        fuera = CSS.replace(BLOQUE, "")
        self.assertNotIn("cat-c-", re.sub(r"/\*.*?\*/", "", fuera, flags=re.S), "las clases cat-c-* solo se usan dentro del @media")


if __name__ == "__main__":
    unittest.main()

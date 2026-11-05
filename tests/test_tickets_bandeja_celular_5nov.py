# -*- coding: utf-8 -*-
"""📱 5-nov-2026 — Fase 1.2 (REGLA #61): la bandeja de Tickets en el celular.

Medido en producción a 390 px con los 50 tickets reales de la primera página: la tabla (8 columnas, 1285 px de ancho real, 1040 px de mínimo) se
deslizaba por dentro (955 px de scroll) y en un celular solo se veían una o dos columnas a la vez.

Bajo 768 px cada ticket pasa a UNA tarjeta con las MISMAS ocho celdas (REGLA #4.2: no se quita nada) y los encabezados que ordenan siguen ahí, como una
tira de botones de 44 px. La tabla de escritorio —que Daniel pidió el 11-jul, «que sea una TABLA»— no cambia.

DESPUÉS (CSS inyectado en producción a 390 px con los 50 tickets reales, antes de escribirlo): sin desborde lateral (scrollWidth = clientWidth) y
tarjetas de 320 a 430 px; la letra más chica que queda son las iniciales del avatar (11,2 px, decorativas).

Se prueba aquí (texto, sin navegador): que cada celda lleve su clase, que el CSS ubique las ocho celdas, que nada se oculte ni se salga del @media y que
el escritorio no cambie. La medición con navegador se repite en producción tras el despliegue (390, 820 y 1280) y queda en REGLA #61.
Correr:  python -m pytest tests/test_tickets_bandeja_celular_5nov.py -q -p no:warnings
"""
import os
import re
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _leer(*partes):
    with open(os.path.join(RAIZ, *partes), encoding="utf-8") as fh:
        return fh.read().replace("\r\n", "\n")


JS = _leer("static", "tickets_list.js")
CSS = _leer("static", "tickets_list.css")

CELDAS = ("tk-c-num", "tk-c-cli", "tk-c-tipo", "tk-c-est", "tk-c-resp", "tk-c-sla", "tk-c-act", "tk-c-acc")
AREAS = {"tk-c-num": "num", "tk-c-est": "est", "tk-c-cli": "cli", "tk-c-tipo": "tipo", "tk-c-resp": "resp", "tk-c-sla": "sla", "tk-c-act": "act",
         "tk-c-acc": "acc"}


def _cuerpo_media(css, condicion):
    """Cuerpo del `@media (max-width:…)` cuyo ancho coincide con `condicion` (ej. «767.98px»), con las llaves anidadas bien contadas. Toma el último."""
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
    i = JS.index("function tkFilaHtml(tk){")
    return JS[i:JS.index("\n}\n", i)]


class TestCadaCeldaLlevaSuClase(unittest.TestCase):
    def test_las_ocho_celdas_en_el_orden_de_siempre(self):
        f = _fila_html()
        tds = re.findall(r"'<td( class=\"([\w-]+)\")?>", f)
        self.assertEqual(8, len(tds), "tkFilaHtml debe seguir generando las 8 celdas de la tabla")
        self.assertEqual(list(CELDAS), [c for _, c in tds])

    def test_los_encabezados_siguen_siendo_ocho_y_ordenan(self):
        cols = re.search(r"const TK_COLS = \[(.*?)\n\];", JS, re.S).group(1)
        self.assertEqual(8, len(re.findall(r"\['[^']+',\s*'[^']*'\]", cols)), "si se agrega una columna hay que agregarle su área a la tarjeta")
        self.assertIn("onclick=\"tkSortClick(", JS, "el orden por columna sigue funcionando")

    def test_la_fila_sigue_abriendo_el_ticket_y_las_acciones_no_se_tocan(self):
        f = _fila_html()
        self.assertIn("onclick=\"location.href=\\'/tickets/'+tk.id+'\\'\"", f)
        self.assertIn("tkAccionesCelda(tk)", f)
        for fn in ("tkAccionCerrar", "tkAccionEliminar"):   # REGLA #4.2
            self.assertIn("window.%s = async function" % fn, JS)


class TestTarjetaEnCelular(unittest.TestCase):
    def test_existe_el_bloque_y_va_al_final_del_archivo(self):
        self.assertIsNotNone(BLOQUE, "debe existir @media (max-width:767.98px)")
        self.assertGreater(CSS.index("BANDEJA EN EL CELULAR (2026-11-05, REGLA #61)"), CSS.index(".tkc-sla.rojo .barra i"),
                           "el bloque va después de las reglas que sobrescribe")

    def test_todo_va_bajo_la_tabla_de_la_bandeja_o_su_envoltorio(self):
        sin_coment = re.sub(r"/\*.*?\*/", "", BLOQUE, flags=re.S)
        for m in re.finditer(r"([^{}]+)\{[^{}]*\}", sin_coment):
            for sel in m.group(1).split(","):
                sel = " ".join(sel.split())
                self.assertTrue(sel.startswith(("table.tk-table", ".tk-table-wrap", ".tk-pg-btn")),
                                "selector fuera de la bandeja: %r (afectaría a otras pantallas)" % sel)

    def test_cada_celda_tiene_su_area_y_las_areas_existen_en_la_grilla(self):
        m = re.search(r'grid-template-areas:([^;]+);', BLOQUE)
        self.assertIsNotNone(m)
        areas = set(re.findall(r"\b[a-z]+\b", m.group(1)))
        self.assertEqual(set(AREAS.values()), areas, "la grilla debe nombrar las 8 áreas")
        for celda, area in AREAS.items():
            self.assertRegex(BLOQUE, r"td\.%s\{grid-area:%s;" % (celda, area))

    def test_las_columnas_no_crecen_con_el_contenido(self):
        """La causa de los desbordes de Técnicos y del Monitor: `1fr` a secas es minmax(auto,1fr)."""
        self.assertIn("grid-template-columns:minmax(0,1fr) auto", BLOQUE)
        self.assertNotRegex(BLOQUE, r"grid-template-columns:\s*1fr")

    def test_ninguna_celda_se_oculta(self):
        """REGLA #4.2: las 8 celdas siguen en la tarjeta. Solo se oculta el encabezado «Acciones», que no ordena (la tarjeta es la fila)."""
        for m in re.finditer(r"([^{}]+)\{([^{}]*)\}", re.sub(r"/\*.*?\*/", "", BLOQUE, flags=re.S)):
            sel, props = " ".join(m.group(1).split()), m.group(2)
            if "display:none" in props.replace(" ", ""):
                self.assertIn(sel, ("table.tk-table thead th:not(.tk-th-sort)", "table.tk-table thead tr::-webkit-scrollbar"),
                              "se oculta algo más que el encabezado que no ordena: %r" % sel)

    def test_los_encabezados_que_ordenan_son_botones_de_44px(self):
        self.assertRegex(BLOQUE, r"table\.tk-table thead th\{[^}]*min-height:44px")
        self.assertRegex(BLOQUE, r"table\.tk-table thead th\{[^}]*position:static")   # el sticky de escritorio se apaga

    def test_botones_de_44px_y_letra_de_12px(self):
        self.assertRegex(BLOQUE, r"\.tk-act,\.tk-pg-btn\{width:44px;height:44px;min-width:44px;min-height:44px;\}")
        self.assertRegex(BLOQUE, r"table\.tk-table \.tkc-sla \.sub,[^{]*\{font-size:\.75rem;\}")
        for clase in (".tk-badge", ".tk-origen-pill", ".tk-unread", ".tk-gar-badge", ".tkc-urg", ".tkc-hace", ".tk-num"):
            self.assertIn("table.tk-table %s" % clase, BLOQUE)

    def test_nombres_y_textos_completos(self):
        """REGLA #15: el cliente y el título ya no tienen ancho máximo ni se cortan."""
        self.assertRegex(BLOQUE, r"table\.tk-table \.tkc-cliente,[^{]*\{max-width:none;\}")

    def test_la_tabla_deja_de_ser_un_cuadro_con_scroll(self):
        self.assertRegex(BLOQUE, r"\.tk-table-wrap\{max-height:none;overflow:visible;")
        self.assertRegex(BLOQUE, r"table\.tk-table\{display:block;width:100%;min-width:0;\}")


class TestElEscritorioNoCambia(unittest.TestCase):
    def test_la_tabla_de_escritorio_sigue_igual(self):
        fuera = CSS.replace(BLOQUE, "")
        self.assertIn("table.tk-table{width:100%;border-collapse:separate;border-spacing:0;font-size:.9rem;min-width:1040px;}", fuera)
        self.assertIn("overflow:auto;max-height:calc(100vh - 300px);", fuera)
        self.assertIn("table.tk-table thead th{position:sticky;top:0;z-index:2;background:#0a0a0a;", fuera)

    def test_las_clases_nuevas_no_tienen_estilo_fuera_del_celular(self):
        fuera = CSS.replace(BLOQUE, "")
        self.assertNotIn("tk-c-", re.sub(r"/\*.*?\*/", "", fuera, flags=re.S), "las clases tk-c-* solo se usan dentro del @media")


if __name__ == "__main__":
    unittest.main()

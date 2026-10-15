"""Cotizaciones en celular (2026-10-15, Daniel mostrándole Cotizaciones a su jefe en un iPhone:
"hay muchos botones... no se veían bien los botones en lo responsivo" y "siempre mantenerlo
en responsivo").

Vigila, leyendo los archivos (sin levantar Flask):
  - cada acción de la fila dice qué es (data-acc / data-lbl): el menú «Más» del celular las
    muestra por su nombre y las «toca» (no repite su lógica); ninguna se quitó (REGLA #4.2);
  - Eliminar sigue confirmando con ilusConfirm (nunca confirm() nativo, REGLA #1);
  - la lista se pagina (REGLA #4.3) y filtrar o limpiar el filtro vuelve a la página 1;
  - el pie del asistente («Cancelar / Crear Cotización») quedó fuera de #cotWizPaso2 para
    poder quedar fijo abajo en celular;
  - el Histórico carga el CSS de su encabezado.

Correr con:  py -m unittest tests.test_cotizaciones_movil
"""
import os
import re
import unittest
from html.parser import HTMLParser

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _leer(*partes):
    with open(os.path.join(RAIZ, *partes), encoding="utf-8") as fh:
        return fh.read()


HTML = _leer("templates", "tickets", "cotizaciones.html")
JS = _leer("static", "tickets_cotizaciones.js")
CSS = _leer("static", "tickets_cotizaciones.css")


def _funcion(nombre):
    m = re.search(r"(async\s+)?function\s+" + nombre + r"\s*\(", JS)
    assert m, nombre
    i = JS.index("{", JS.index(")", m.end()))
    prof, j = 0, i
    while True:
        if JS[j] == "{":
            prof += 1
        elif JS[j] == "}":
            prof -= 1
            if prof == 0:
                return JS[m.start():j + 1]
        j += 1


class AccionesDeLaFila(unittest.TestCase):
    def _celda(self):
        a = HTML.index('<td data-label="Acciones"')
        return HTML[a:HTML.index("</td>", a)]

    def test_cada_accion_tiene_nombre(self):
        celda = self._celda()
        botones = re.findall(r"<(?:a|button)\b[^>]*class=\"tk-row-btn-[^\"]*\"[^>]*>", celda)
        self.assertEqual(len(botones), 8, "PDF, Enviar, Aprobar, Rechazar, Eliminar, Recotizar, Cálculo, Historial")
        for b in botones:
            self.assertIn("data-acc=", b)
            self.assertIn("data-lbl=", b)
        accs = re.findall(r'data-acc="([a-z]+)"', celda)
        self.assertEqual(sorted(accs), sorted(["pdf", "enviar", "aprobar", "rechazar", "eliminar",
                                               "recotizar", "calculo", "historial"]))

    def test_el_menu_toca_los_botones_originales(self):
        src = _funcion("cotAccAbrir")
        self.assertIn("b.click()", src)
        self.assertIn("'eliminar'", src)  # va aparte, en la zona de peligro
        self.assertIn("prim.click()", _funcion("cotAccMovilInit"))

    def test_eliminar_confirma_con_ilus(self):
        src = _funcion("cotEliminar")
        self.assertIn("ilusConfirm", src)
        self.assertIn("danger: true", src)

    def test_sin_dialogos_nativos(self):
        for nombre in ("cotAccMovilInit", "cotAccAbrir", "cotAccCerrar", "cotPaginar", "cotFiltrarLista"):
            src = _funcion(nombre)
            self.assertNotRegex(src, r"(?<![\w.])(alert|confirm|prompt)\(", nombre)


class Paginacion(unittest.TestCase):
    def test_filtrar_o_limpiar_vuelve_a_la_pagina_1(self):
        src = _funcion("cotFiltrarLista")
        self.assertIn("_cotPag.pag = 1", src)
        self.assertIn("cotPaginar()", src)

    def test_pie_con_el_patron_de_etiquetas(self):
        src = _funcion("cotPaginar")
        for txt in ("Mostrando", "por página", "Anterior", "Página", "Siguiente"):
            self.assertIn(txt, src)
        self.assertIn('id="cotPie"', HTML)


class _Pila(HTMLParser):
    """Encuentra el id del ancestro más cercano de un elemento con cierta clase."""
    VACIOS = {"input", "br", "img", "hr", "meta", "link", "source", "area", "col", "wbr"}

    def __init__(self, clase):
        super().__init__()
        self.clase, self.pila, self.ancestros = clase, [], None

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        if tag in self.VACIOS:
            return
        if self.ancestros is None and self.clase in (a.get("class") or "").split():
            self.ancestros = [x for x in self.pila if x]
        self.pila.append(a.get("id"))

    def handle_endtag(self, tag):
        if tag in self.VACIOS:
            return
        if self.pila:
            self.pila.pop()


class Asistente(unittest.TestCase):
    def test_pie_fuera_de_la_seccion_5(self):
        sin_jinja = re.sub(r"\{#.*?#\}", "", HTML, flags=re.S)
        sin_jinja = re.sub(r"\{%.*?%\}|\{\{.*?\}\}", "", sin_jinja, flags=re.S)
        p = _Pila("cot-wiz-foot")
        p.feed(sin_jinja)
        self.assertIsNotNone(p.ancestros, "no se encontró el pie del asistente")
        self.assertIn("cotWizModal", p.ancestros)
        self.assertNotIn("cotWizPaso2", p.ancestros)
        self.assertIn("btnCotWizCrear", HTML)

    def test_plan_marcar_todos_tambien_en_celular(self):
        self.assertIn('id="btnCotWizTogPlanTodosM"', HTML)
        self.assertIn("btnTodosM.hidden", JS)

    def test_tarjetas_con_rotulo(self):
        for rot in ("Cantidad", "Característica", "Precio base por unidad", "Total con ruta", "Plan"):
            self.assertIn('data-label="' + rot + '"', JS)


class HistoricoYEstilos(unittest.TestCase):
    def test_historico_carga_su_css(self):
        self.assertIn("tickets_cotizaciones.css", _leer("templates", "tickets", "cotizaciones_historico.html"))

    def test_objetivos_tactiles(self):
        self.assertRegex(CSS, r"\.cot-acc-prim,\.cot-acc-mas\{[^}]*min-height:44px")
        self.assertIn("env(safe-area-inset-bottom", CSS)


if __name__ == "__main__":
    unittest.main()

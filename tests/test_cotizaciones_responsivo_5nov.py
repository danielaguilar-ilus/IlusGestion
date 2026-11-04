# -*- coding: utf-8 -*-
"""📱 5-nov-2026 — Cotizaciones: lista que CABE en 1280/1366/1440 y tarjetas COMPACTAS en celular (REGLA #61).

Daniel (4-nov, con la auditoría de diseño): «la tabla no cabe a 1280… en el celular las tarjetas son muy grandes y feas, con letras muy chicas;
no quiero letras chicas, pero tampoco mucho scroll… siempre te he pedido que como regla dejemos la parte responsiva».

Medido ANTES en la vista previa local (CSS y JS reales del repo, Chromium): a 1280 la tabla medía 1354 px en un contenedor de 1018 (la columna
Acciones quedaba fuera de pantalla) y en celular cada tarjeta medía 342–431 px con letra de 10–11,5 px. DESPUÉS: la tabla mide lo mismo que su
contenedor a 1240/1280/1366/1440, las tarjetas 187–272 px y no queda texto de menos de 12 px (los íconos aparte).

Se prueba aquí (texto, sin navegador): la plantilla, el CSS y el JS que lo hacen posible, y que no vuelvan las letras de menos de 12 px, los grises
sin contraste ni el rebote. La medición con navegador se hizo a mano (anchos 390, 820, 1100, 1240, 1280, 1366, 1440) y queda escrita en REGLA #61.
Correr:  python -m pytest tests/test_cotizaciones_responsivo_5nov.py -q -p no:warnings
"""
import os
import re
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _leer(*partes):
    with open(os.path.join(RAIZ, *partes), encoding="utf-8") as fh:
        return fh.read().replace("\r\n", "\n")


HTML = _leer("templates", "tickets", "cotizaciones.html")
CSS = _leer("static", "tickets_cotizaciones.css")
KPI = _leer("static", "cotiz_kpis.css")
JS = _leer("static", "tickets_cotizaciones.js")
PDF = _leer("templates", "tickets", "cotizacion_pdf.html")
MOD = _leer("tickets_module.py")


def _reglas(css):
    """[(selector, {propiedad: valor})] de las reglas planas (sin anidar @media: se aplanan sus reglas internas)."""
    sin_coment = re.sub(r"/\*.*?\*/", "", css, flags=re.S)
    out = []
    for m in re.finditer(r"([^{}@][^{}]*)\{([^{}]*)\}", sin_coment):
        sel = " ".join(m.group(1).split())
        props = {}
        for d in m.group(2).split(";"):
            if ":" in d:
                k, v = d.split(":", 1)
                props[k.strip().lower()] = v.strip().lower()
        out.append((sel, props))
    return out


# Textos de menos de 12 px que SÍ pueden quedar: son íconos o glifos, no letras.
ICONOS_PEQUENOS = (".tk-row-uf i", ".cot-wiz-eqficha-plan summary::before", ".tk-row-btn-unlink")


class TestListaResponsiva(unittest.TestCase):
    def test_la_tabla_lleva_la_clase_de_la_lista_y_no_empuja_con_nowrap_en_linea(self):
        self.assertIn('class="table ilus-table mb-0 table-mobile-cards cot-lista"', HTML)
        for col in ("Estado", "Fecha", "Ticket", "Acciones"):
            m = re.search(r'<td data-label="%s"[^>]*>' % col, HTML)
            self.assertIsNotNone(m, col)
            self.assertNotIn("white-space:nowrap", m.group(0), "%s: el nowrap en línea empujaba la tabla fuera de la pantalla" % col)

    def test_los_botones_de_ticket_pasan_a_mas_sin_quitarse(self):
        """REGLA #4.2: Generar / Asociar / Quitar del ticket siguen existiendo; en tarjeta compacta el menú «Más» los «toca»."""
        for acc, lbl in (("ticket_nuevo", "Generar ticket"), ("ticket_asociar", "Asociar a un ticket que ya existe"),
                         ("ticket_quitar", "Quitar del ticket")):
            self.assertIn('data-acc="%s" data-lbl="%s"' % (acc, lbl), HTML)
        self.assertIn("cotGenerarTicket(", HTML)
        self.assertIn("cotAsociarTicket(", HTML)
        self.assertIn("cotDesasociarTicket(", HTML)

    def test_el_menu_mas_incluye_la_celda_ticket(self):
        i = JS.index("function _cotAcciones(row){")
        f = JS[i:JS.index("function _cotPrincipal", i)]
        self.assertIn('td[data-label="Acciones"]', f)
        self.assertIn('td[data-label="Ticket"]', f)
        self.assertIn("querySelectorAll('[data-acc]')", f)

    def test_el_css_trae_la_tabla_que_cabe_y_las_tarjetas_compactas(self):
        self.assertIn("LISTA RESPONSIVA (2026-11-05)", CSS)
        self.assertIn("@media (max-width:1239.98px){", CSS, "bajo 1240 px la tabla no cabe (menú de 212 px + márgenes): pasa a tarjetas")
        self.assertIn('grid-template-areas:"num est" "cli tot" "fec tot" "doc tic" "acc acc"', CSS, "tarjeta de 4 filas")
        self.assertIn('td[data-label="Ticket"] [data-acc]{display:none !important}', CSS)
        self.assertIn(".cot-lista .cot-acc-m{display:flex", CSS)
        # la tabla de escritorio: celdas que se parten y acciones en 2 filas (no 332 px de una línea)
        # 184 px: 8 íconos caben en 2 filas parejas (4+4); con 176 px el octavo caía solo en una 3.ª fila, y con 186 px la tabla ya no cabía
        # a 1240 de ventana (medido en Chromium, 5-nov)
        self.assertIn('.cot-lista td[data-label="Acciones"]{width:184px;min-width:184px', CSS)
        # el relleno lateral de las celdas: 5 px. Con 6 px y Acciones de 184 px la tabla con datos reales medía 965 px y entre 1240 y 1244 px de
        # ventana asomaban 1-4 px de scroll interno (medido en producción, 4-nov)
        self.assertIn(".cot-lista th,.cot-lista td{padding:10px 5px;vertical-align:middle}", CSS)
        self.assertIn(".cot-lista .cot-docs-chips{display:flex;flex-direction:column", CSS)

    def test_todo_va_bajo_cot_lista_para_no_tocar_las_otras_tablas(self):
        i = CSS.rindex("/*", 0, CSS.index("LISTA RESPONSIVA (2026-11-05)"))   # desde el inicio de su comentario
        bloque = CSS[i:]
        for sel, _ in _reglas(bloque):
            for parte in sel.split(","):
                parte = parte.strip()
                if parte.startswith(("0", "1", "2", "3", "4")):   # texto residual del comentario
                    continue
                self.assertIn("cot-lista", parte, "regla fuera de .cot-lista: %r (afectaría a otras pantallas)" % parte)


class TestLetraYContraste(unittest.TestCase):
    def test_ningun_texto_de_cotizaciones_baja_de_12_px(self):
        malos = []
        for css, nombre in ((CSS, "tickets_cotizaciones.css"), (KPI, "cotiz_kpis.css")):
            for sel, props in _reglas(css):
                fs = props.get("font-size", "")
                m = re.match(r"^\.(\d+)rem", fs)
                if m and float("0." + m.group(1)) < 0.75 and not any(sel.startswith(x) or sel == x for x in ICONOS_PEQUENOS):
                    # los íconos dentro de otra regla (selector que termina en « i») también quedan fuera
                    if sel.endswith(" i") or "::before" in sel:
                        continue
                    malos.append((nombre, sel[:70], fs))
        self.assertEqual(malos, [], "letra de menos de 12 px (.75rem) en la lista, el asistente o los indicadores: %r" % (malos,))

    def test_ninguna_letra_en_linea_de_la_plantilla_baja_de_12_px(self):
        """El chip de la UF (cabecera de la pantalla) traía `font-size:.7rem` EN LÍNEA: su valor «UF $41.268,88» medía 11,2 px en escritorio (el CSS ya lo
        subía en celular, por eso la medición de 390 px no lo vio). La prueba de arriba solo mira hojas de estilo: aquí se revisan los style="" de la plantilla."""
        malos = []
        for m in re.finditer(r"font-size\s*:\s*([0-9]*\.?[0-9]+)\s*(rem|em|px)", HTML):
            valor, unidad = float(m.group(1)), m.group(2)
            px = valor * 16 if unidad in ("rem", "em") else valor
            if px < 12:
                malos.append(m.group(0))
        self.assertEqual(malos, [], "letra en línea de menos de 12 px en cotizaciones.html: %r" % (malos,))
        self.assertIn("padding:3px 12px;font-size:.8rem;font-weight:800;margin-left:8px", HTML, "el chip de la UF quedó en .8rem como en celular")

    def test_los_grises_de_texto_sobre_blanco_ya_no_son_9ca3af(self):
        por_sel = {sel: props for sel, props in _reglas(CSS)}
        self.assertEqual(por_sel[".tk-row-uf"]["color"], "#6b7280", "las UF bajo el total: 2,5:1 antes, 4,8:1 ahora")
        self.assertEqual(por_sel[".cb-draft"]["color"], "#4b5563")
        self.assertEqual(por_sel[".tk-row-total.cero"]["color"], "#6b7280", "«sin precio» era #c1c5cc (1,8:1)")
        self.assertIn('id="cotEnvInputHint" style="font-size:.78rem;color:#6b7280;"', HTML)

    def test_el_gris_claro_no_se_usa_como_color_de_texto(self):
        """#9ca3af se lee sobre negro (8:1) pero no sobre blanco (2,5:1). En este CSS no queda ninguno como color (textos ni íconos):
        usa #6b7280 (4,8:1). Si hace falta un gris claro sobre una banda negra, ponlo en una regla de esa banda y documenta el fondo."""
        malos = [sel for sel, props in _reglas(CSS) if props.get("color") == "#9ca3af"]
        self.assertEqual(malos, [], "color:#9ca3af en %r: usa #6b7280" % (malos,))

    def test_los_indicadores_no_usan_grises_sin_contraste_sobre_negro(self):
        por_sel = {sel: props for sel, props in _reglas(KPI)}
        self.assertEqual(por_sel[".ck-c .v.na"]["color"], "#a1a1aa", "«—» era #52525b sobre negro (2,6:1)")
        self.assertEqual(por_sel[".c-mar.is-cero .s"]["color"], "#a1a1aa", "era #71717a (4,1:1)")

    def test_sin_rebote_en_las_animaciones(self):
        self.assertNotIn("cubic-bezier(.34,1.56", CSS, "el rebote (overshoot) se cambió por una salida suave")
        self.assertIn("cubic-bezier(.22,1,.36,1)", CSS)

    def test_los_verdes_sobre_verde_claro_pasan_4_5_a_1(self):
        for texto in ("--tc:#15803d;--tbg:#f0fdf4;", "background:#f0fdf4;color:#15803d;border:1.5px solid #16a34a"):
            self.assertIn(texto, HTML)
        self.assertIn(".cot-wiz-plan-todos-m{background:#f0fdf4;color:#15803d;", CSS)


class TestDocumentoYEncabezado(unittest.TestCase):
    def test_el_banner_del_visor_no_se_sale_en_celular(self):
        i = PDF.index("@media (max-width: 768px) {")
        bloque = PDF[i:PDF.index("</style>", i)]
        self.assertIn(".visor-head { flex-wrap: wrap;", bloque)
        self.assertIn(".visor-head .vh-logos { flex: 1 1 100%; flex-wrap: wrap;", bloque)
        self.assertIn(".visor-head .vh-doc { flex: 1 1 100%; min-width: 0; text-align: left; }", bloque)
        self.assertIn(".sheet-pad { padding-left: 4mm; padding-right: 4mm; }", bloque)

    def test_la_fecha_de_emision_ya_no_mide_7_5_px(self):
        self.assertIn(".visor-head .vh-fecha { font-size: 12px;", PDF, "en pantalla (visor)")
        self.assertIn('font-size:10px;color:#9ca3af;margin-top:1px;">FECHA EMISIÓN:', MOD, "en el encabezado real del PDF")
        self.assertNotIn("font-size:7.5px", MOD)
        self.assertNotIn("font-size: 7.5px", PDF)

    def test_la_tipografia_de_impresion_del_documento_no_se_toco(self):
        """El PDF es el documento que recibe el cliente: sus tamaños en pt (diseño de impresión) siguen igual; solo el banner del visor cambió."""
        for pt in ("font-size: 9pt;", "font-size: 7.8pt;", "font-size: 8.5pt;", "font-size: 6.5pt;"):
            self.assertIn(pt, PDF)
        self.assertIn("margin: 42mm 0mm 16mm;", PDF, "el margen superior calibrado para el encabezado del PDF sigue igual")


class TestVisorItemsEnCelular(unittest.TestCase):
    """Medido EN PRODUCCIÓN (4-nov, iframe a 390 px, cotización COT-000205): la tabla de ítems del visor medía 417 px en una hoja de 369 y la página
    tenía ≈ 45 px de scroll de lado, con letra de 9 a 11 px. Bajo 768 px cada ítem pasa a una tarjeta de 2 líneas (12-13 px, 57 px de alto,
    339 px de ancho, sin scroll). SOLO el visor: el PDF de Playwright y la impresión no usan estas reglas."""

    @staticmethod
    def _bloque_movil():
        i = PDF.index("@media (max-width: 768px) {")
        return PDF[i:PDF.index("</style>", i)]

    def test_el_cuerpo_marca_el_visor_y_el_pdf_no(self):
        self.assertIn('<body class="{% if modo_visor %}es-visor{% endif %}">', PDF)

    def test_las_reglas_de_items_van_solo_bajo_body_es_visor(self):
        """Cada selector que toca `table.items` en el bloque móvil empieza por `body.es-visor`: sin la clase (PDF e impresión) no se aplica ninguna."""
        sin_coment = re.sub(r"/\*.*?\*/", "", self._bloque_movil(), flags=re.S)
        vistas = 0
        for m in re.finditer(r"([^{}]+)\{([^{}]*)\}", sin_coment):
            sel = " ".join(m.group(1).split())
            if "table.items" not in sel:
                continue
            vistas += 1
            for parte in sel.split(","):
                self.assertTrue(parte.strip().startswith("body.es-visor "), "regla de ítems sin body.es-visor: %r" % parte.strip())
        self.assertGreaterEqual(vistas, 15, "las reglas de las tarjetas de ítems deben estar todas")

    def test_los_items_apilados_no_bajan_de_12_px(self):
        sin_coment = re.sub(r"/\*.*?\*/", "", self._bloque_movil(), flags=re.S)
        for m in re.finditer(r"([^{}]+)\{([^{}]*)\}", sin_coment):
            if "table.items" not in m.group(1):
                continue
            for px in re.findall(r"font-size:\s*(\d+(?:\.\d+)?)px", m.group(2)):
                self.assertGreaterEqual(float(px), 12.0, "letra de ítems < 12 px en %r" % " ".join(m.group(1).split()))

    def test_se_conservan_todas_las_columnas_como_etiquetas(self):
        """Nada se quita (REGLA #4.2): el # y la descripción, el documento, la cantidad (con «Cant.»), el precio UF (con «UF») y el total siguen."""
        b = self._bloque_movil()
        for trozo in ("td.num { order: 1", "td:not([class]) { order: 2", "td.doc { order: 4", "td.cant { order: 5", 'content: "Cant. "',
                      "td.puf { order: 6", 'content: "UF "', "td.tot { order: 7"):
            self.assertIn(trozo, b)
        self.assertIn("tr.col-row { display: none; }", b, "los encabezados de columna sobran: cada dato lleva su etiqueta")

    def test_el_pdf_real_no_usa_el_ancho_movil(self):
        """Las reglas móviles solo aplican bajo 768 px de ancho: la hoja del PDF mide 794 px (@page A4 con márgenes laterales en 0)."""
        self.assertIn("margin: 42mm 0mm 16mm;", PDF)
        self.assertIn("margin: 0;", PDF)


if __name__ == "__main__":
    unittest.main()

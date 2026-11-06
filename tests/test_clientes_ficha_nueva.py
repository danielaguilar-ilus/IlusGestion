# -*- coding: utf-8 -*-
"""FICHA DEL CLIENTE NUEVA (Daniel, 5-nov-2026): «ese módulo es viejo, quisiera ENCHULARLO y AHORRAR PESTAÑAS… los clientes son para mí importantes».

Fase 1a — las 15 pestañas de la ficha se agrupan en 5 SECCIONES sin quitar ninguna (REGLA #4.2), con «Vista clásica» de respaldo:
  · cada `data-tab` de siempre sigue existiendo, sigue siendo alcanzable y está en UNA sola sección;
  · `switchTab()` sigue siendo la única puerta (mant_ficha.js intacto) y los botones originales se MUEVEN (no se clonan): sus ids, contadores y
    onclick siguen funcionando;
  · la vista clásica devuelve la barra de 15 pestañas en su orden original;
  · medidas (REGLA #61): botones de 44 px, letra ≥ 13 px, sin scroll de lado a 390 / 820 / 1280 / 1366 / 1440.

Se prueba en un navegador real (Chromium de Playwright) con el CSS y el JS REALES y la barra de pestañas de templates/mantenciones/ficha.html
renderizada con Jinja; `switchTab` se copia del código de la ficha (static/mant_ficha.js). Sin base de datos, sin red.
Correr:  python -m pytest tests/test_clientes_ficha_nueva.py -q -p no:warnings
"""
import os
import re
import sys
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, RAIZ)


def _leer(*ruta):
    with open(os.path.join(RAIZ, *ruta), encoding="utf-8", newline="") as fh:
        return fh.read().replace("\r\n", "\n")


FICHA = _leer("templates", "mantenciones", "ficha.html")
JS_GRUPOS = _leer("static", "ficha_grupos.js")
CSS_GRUPOS = _leer("static", "ficha_grupos.css")
MANT_FICHA_JS = _leer("static", "mant_ficha.js")

TABS_DE_SIEMPRE = ["vida", "resumen", "equipos", "visitas", "contrato", "reportes", "intel", "repuestos", "finanzas",
                   "cotizaciones", "tickets", "documentos", "evidencias", "comunicaciones", "historial"]
ONCLICK_DE_SIEMPRE = {      # lo que cada botón original ejecuta (cargas perezosas incluidas): NO debe cambiar
    "vida": "switchTab('vida');cargarVidaCliente()", "resumen": "switchTab('resumen')", "equipos": "switchTab('equipos')",
    "visitas": "switchTab('visitas')", "contrato": "switchTab('contrato')", "reportes": "switchTab('reportes');cargarReportes()",
    "intel": "switchTab('intel');cargarInteligencia()", "repuestos": "switchTab('repuestos');cargarRepuestos()",
    "finanzas": "switchTab('finanzas');cargarFinanzas()", "cotizaciones": "switchTab('cotizaciones');cargarCotizacionesFicha()",
    "tickets": "switchTab('tickets');cargarTicketsFicha()", "documentos": "switchTab('documentos');cargarDocumentos()",
    "evidencias": "switchTab('evidencias');cargarEvidencias()", "comunicaciones": "switchTab('comunicaciones');cargarComunicaciones()",
    "historial": "switchTab('historial')"}
CARGAS = ["cargarVidaCliente", "cargarReportes", "cargarInteligencia", "cargarRepuestos", "cargarFinanzas", "cargarCotizacionesFicha",
          "cargarTicketsFicha", "cargarDocumentos", "cargarEvidencias", "cargarComunicaciones"]


class TestEstructura(unittest.TestCase):
    """Sin navegador: lo que NO puede cambiar en la plantilla y en el JS."""

    def test_las_15_pestanas_de_siempre_siguen_con_su_onclick(self):
        for tab in TABS_DE_SIEMPRE:
            m = re.search(r'<button class="ftab-btn[^"]*" data-tab="%s" onclick="([^"]*)"' % tab, FICHA)
            self.assertIsNotNone(m, "falta la pestaña " + tab)
            self.assertEqual(m.group(1), ONCLICK_DE_SIEMPRE[tab], tab)
        self.assertEqual(len(re.findall(r'class="ftab-btn', FICHA)), 15)

    def test_cada_pestana_pertenece_a_una_sola_seccion_y_ninguna_queda_afuera(self):
        grupos = re.findall(r"\{ id: '(\w+)',\s+tabs: \[([^\]]*)\] \}", JS_GRUPOS)
        self.assertEqual([g[0] for g in grupos], ["resumen", "equipos", "servicio", "comercial", "comunicacion"])
        todas = []
        for _id, lista in grupos:
            todas += re.findall(r"'(\w+)'", lista)
        self.assertEqual(sorted(todas), sorted(TABS_DE_SIEMPRE))          # exactamente las 15, sin repetir ni faltar
        self.assertEqual(len(todas), len(set(todas)))

    def test_las_cinco_secciones_del_html_son_las_del_js(self):
        ids_html = re.findall(r'<button type="button" class="fg-btn" id="fgBtn-(\w+)" data-grupo="\1"', FICHA)
        self.assertEqual(ids_html, ["resumen", "equipos", "servicio", "comercial", "comunicacion"])
        for g in ids_html:
            self.assertIn('<div class="fg-sub" data-grupo="%s"' % g, FICHA)
        self.assertIn('id="fgVista"', FICHA)

    def test_switchtab_sigue_siendo_la_unica_puerta_y_no_se_toco(self):
        i = MANT_FICHA_JS.index("function switchTab(name) {")
        cuerpo = MANT_FICHA_JS[i:MANT_FICHA_JS.index("\n}\n", i)]
        for pieza in ("document.querySelectorAll('.ftab-pane').forEach(p => p.classList.remove('active'));",
                      "document.querySelectorAll('.ftab-btn').forEach(b => b.classList.remove('active'));",
                      "document.getElementById(`tab-${name}`).classList.add('active');",
                      "document.querySelector(`.ftab-btn[data-tab=\"${name}\"]`).classList.add('active');",
                      "localStorage.setItem(TAB_KEY, name);"):
            self.assertIn(pieza, cuerpo)
        self.assertNotIn("fgBtn", MANT_FICHA_JS)                           # la capa nueva vive en su propio archivo
        self.assertIn("window.switchTab = envuelta", JS_GRUPOS)            # se ENVUELVE, no se reemplaza
        self.assertIn("original.apply(this, arguments)", JS_GRUPOS)

    def test_los_botones_se_mueven_no_se_clonan(self):
        self.assertNotIn("cloneNode", JS_GRUPOS)
        self.assertIn("cont.appendChild(b)", JS_GRUPOS)
        self.assertIn("barra.appendChild(b)", JS_GRUPOS)

    def test_se_carga_despues_de_mant_ficha_y_con_su_css(self):
        self.assertIn("filename='ficha_grupos.css'", FICHA)
        self.assertLess(FICHA.index("filename='mant_ficha.js'"), FICHA.index("filename='ficha_grupos.js'"))
        self.assertLess(FICHA.index("filename='tickets_ficha.js'"), FICHA.index("filename='ficha_grupos.js'"))
        # la vista se marca ANTES de pintar la barra (sin parpadeo) y con try/catch (sin almacenamiento no se rompe)
        pre = FICHA[:FICHA.index('<div class="ficha-tabs-wrap"')]
        # 6-nov (Daniel: «necesito todo ordenado como estaba antes»): la barra de 15 de siempre es la de OMISIÓN; solo «nueva» agrupa. Clave nueva (`_2`).
        self.assertIn("getItem('ficha_vista_2')==='nueva'", pre)
        self.assertIn("catch(e)", pre[pre.index("ficha_vista_2"):])
        self.assertNotIn("getItem('ficha_vista')", FICHA)                           # la clave de la primera noche ya no cuenta
        self.assertIn("var KEY_VISTA = 'ficha_vista_2';", JS_GRUPOS)
        self.assertIn("function vistaActual() { return lsGet(KEY_VISTA) === 'nueva' ? 'nueva' : 'clasica'; }", JS_GRUPOS)
        # el botón de la página nace con el texto de la vista de partida (clásica → ofrece la nueva), sin esperar al JS
        self.assertIn('id="fgVista" data-vista="clasica"', FICHA)
        self.assertIn("<span>Vista nueva</span>", FICHA[FICHA.index('id="fgVista"'):FICHA.index('id="fgVista"') + 400])

    def test_sin_popups_nativos_y_con_almacenamiento_protegido(self):
        for nombre, src in (("ficha_grupos.js", JS_GRUPOS),):
            self.assertNotRegex(src, r"\balert\(|\bconfirm\(|\bprompt\(", nombre)
        self.assertRegex(JS_GRUPOS, r"function lsGet\(k\) \{ try \{")
        self.assertRegex(JS_GRUPOS, r"function lsSet\(k, v\) \{ try \{")

    def test_el_css_cumple_la_regla_61(self):
        # botones ≥ 44 px y letra ≥ 13 px en cada regla de la capa nueva
        self.assertIn("min-height: 44px", CSS_GRUPOS)
        tams = [float(t) for t in re.findall(r"font-size:\s*([\d.]+)rem", CSS_GRUPOS)]
        self.assertGreaterEqual(min(tams), 0.75)                           # nada bajo 12 px (los contadores); los botones, ≥ 13 px (se mide en el navegador)
        self.assertIn("prefers-reduced-motion", CSS_GRUPOS)
        self.assertIn("@media (max-width: 767.98px)", CSS_GRUPOS)
        self.assertIn("fgRevelar", CSS_GRUPOS)                             # si el JS no carga, la barra clásica reaparece sola


# ═══════════════════════════════════════════════════════════════════════════════════════════════════
#  En un navegador real
# ═══════════════════════════════════════════════════════════════════════════════════════════════════
def _barra_renderizada():
    """El bloque de la barra de pestañas de ficha.html (de `ficha-tabs-wrap` hasta la primera pestaña), renderizado con Jinja y datos de mentira."""
    import jinja2
    ini = FICHA.index("<script>try{if(window.localStorage.getItem('ficha_vista_2')")
    marca = FICHA.index("TAB: ★ VIDA DEL CLIENTE")
    fin = FICHA.rfind("{#", 0, marca)
    bloque = FICHA[ini:fin]
    bloque = re.sub(r"\{#.*?#\}", "", bloque, flags=re.S)                  # comentarios Jinja
    return jinja2.Template(bloque).render(maquinas=[1] * 50, visitas=[1] * 3, contratos=[1], logs=[1] * 9)


def _switchtab_real():
    i = MANT_FICHA_JS.index("const TAB_KEY = `ficha_tab_${CID}`;")
    j = MANT_FICHA_JS.index("\n}\n", MANT_FICHA_JS.index("function switchTab(name) {", i)) + 3
    return MANT_FICHA_JS[i:j]


def _pagina():
    css = _leer("static", "mantenciones_ficha.css") + "\n" + CSS_GRUPOS
    paneles = "".join('<div class="ftab-pane%s" id="tab-%s">Panel %s</div>' % (" active" if t == "resumen" else "", t, t)
                      for t in TABS_DE_SIEMPRE)
    stubs = "".join("function %s(){ (window.__cargas = window.__cargas || []).push('%s'); }" % (n, n) for n in CARGAS)
    return ("<!doctype html><html lang='es'><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'>"
            "<style>:root{--border:#e5e7eb;--rad:12px;--ilus-red:#dc2626;--shadow:0 1px 3px rgba(0,0,0,.1)}"
            "*{box-sizing:border-box}body{margin:0;font-family:Arial,sans-serif;font-size:14px}"
            ".badge{display:inline-block;padding:2px 7px;border-radius:50px;font-weight:700}.ms-1{margin-left:4px}"
            ".main{max-width:1100px;margin:0 auto;padding:12px}</style>"
            "<style>" + css + "</style></head><body><div class='main'>" + _barra_renderizada() + paneles + "</div>"
            "<script>const CID = 139;" + stubs + _switchtab_real() + "</script>"
            "<script>" + JS_GRUPOS + "</script></body></html>")


try:
    from playwright.sync_api import sync_playwright
except Exception:                      # pragma: no cover
    sync_playwright = None

URL = "https://ficha.test/clientes/139"


@unittest.skipIf(sync_playwright is None, "Playwright no está instalado")
class TestNavegadorReal(unittest.TestCase):
    """UN solo navegador, UN solo contexto y UNA sola página para todas las pruebas (con poca memoria abrir y cerrar navegadores tarda minutos).
    La página se sirve desde un origen falso (https://ficha.test) para que `localStorage` exista y se pueda guardar la vista elegida."""

    @classmethod
    def setUpClass(cls):
        cls._pw = sync_playwright().start()
        try:
            cls.nav = cls._pw.chromium.launch()
        except Exception as e:         # sin Chromium instalado
            cls._pw.stop()
            raise unittest.SkipTest("Chromium no disponible: %s" % e)
        cls.html_base = _pagina()
        cls.html_actual = cls.html_base
        cls.ctx = cls.nav.new_context(viewport={"width": 1280, "height": 900})
        cls.ctx.set_default_timeout(8000)
        cls.ctx.route("https://ficha.test/**",
                      lambda ruta: ruta.fulfill(status=200, content_type="text/html; charset=utf-8", body=cls.html_actual))
        cls.pg = cls.ctx.new_page()
        cls.errores = []
        cls.pg.on("pageerror", lambda e: cls.errores.append(str(e)))

    @classmethod
    def tearDownClass(cls):
        try:
            cls.ctx.close()
            cls.nav.close()
        finally:
            cls._pw.stop()

    def abrir(self, ancho=1280, vista="nueva", storage=None, html=None, limpiar=True):
        """`vista="nueva"` por omisión: casi todas las pruebas miran la vista agrupada, que ahora hay que pedir; `vista=None` = sin nada guardado (la de verdad)."""
        type(self).html_actual = html or self.html_base
        pg = self.pg
        pg.set_viewport_size({"width": ancho, "height": 900})
        pg.goto(URL)
        if limpiar:
            pg.evaluate("localStorage.clear()")
            if vista:
                pg.evaluate("localStorage.setItem('ficha_vista_2', %r)" % vista)
            for k, v in (storage or {}).items():
                pg.evaluate("localStorage.setItem(%r, %r)" % (k, v))
            pg.reload()
        return pg

    # ── por omisión: la barra de 15 de siempre (Daniel, 6-nov) ──
    def test_por_omision_la_ficha_abre_con_las_15_pestanas_de_siempre_en_su_orden(self):
        pg = self.abrir(vista=None)                                                  # nada guardado en el navegador
        self.assertNotIn("fg-nueva", pg.evaluate("document.documentElement.className"))
        orden = pg.evaluate("[...document.querySelectorAll('.ficha-tabs .ftab-btn')].map(b => b.dataset.tab)")
        self.assertEqual(orden, TABS_DE_SIEMPRE)
        self.assertEqual(pg.locator(".fg-sub .ftab-btn").count(), 0)
        self.assertEqual(pg.evaluate("getComputedStyle(document.getElementById('fgNav')).display"), "none")
        self.assertEqual(pg.locator(".ficha-tabs").evaluate("e => getComputedStyle(e).display"), "flex")
        self.assertIn("Vista nueva", pg.locator("#fgVista").inner_text())            # y ofrece la agrupada, sin imponerla
        self.assertEqual(self.errores, [])
        for tab in TABS_DE_SIEMPRE:                                                  # y cada pestaña conserva su onclick de siempre
            self.assertEqual(pg.locator(".ftab-btn[data-tab=%s]" % tab).get_attribute("onclick"), ONCLICK_DE_SIEMPRE[tab], tab)

    def test_la_clave_de_la_primera_noche_ya_no_agrupa(self):
        pg = self.abrir(vista=None, storage={"ficha_vista": "nueva"})
        self.assertNotIn("fg-nueva", pg.evaluate("document.documentElement.className"))
        self.assertEqual(pg.locator(".ficha-tabs .ftab-btn").count(), 15)

    def test_elegir_la_vista_nueva_se_recuerda_y_la_clasica_vuelve_a_ser_la_de_siempre(self):
        pg = self.abrir(vista=None)
        pg.click("#fgVista")
        self.assertIn("fg-nueva", pg.evaluate("document.documentElement.className"))
        self.assertEqual(pg.evaluate("localStorage.getItem('ficha_vista_2')"), "nueva")
        pg.reload()
        self.assertIn("fg-nueva", pg.evaluate("document.documentElement.className"))
        self.assertEqual(pg.locator(".fg-btn").count(), 5)
        self.assertIn("Vista clásica", pg.locator("#fgVista").inner_text())
        pg.click("#fgVista")
        self.assertNotIn("fg-nueva", pg.evaluate("document.documentElement.className"))
        self.assertEqual(pg.locator(".ficha-tabs .ftab-btn").count(), 15)

    # ── agrupar (vista nueva, opcional) ──
    def test_con_la_vista_nueva_las_pestanas_van_en_cinco_secciones(self):
        pg = self.abrir()
        clases = pg.evaluate("document.documentElement.className")
        self.assertIn("fg-nueva", clases)
        self.assertIn("fg-listo", clases)
        self.assertEqual(pg.locator(".fg-btn").count(), 5)
        self.assertEqual(pg.locator(".ficha-tabs .ftab-btn").count(), 0)          # la barra de 15 quedó vacía
        self.assertEqual(pg.locator(".fg-sub .ftab-btn").count(), 15)             # y las 15 viven en sus secciones
        self.assertEqual(pg.evaluate("getComputedStyle(document.querySelector('.ficha-tabs')).display"), "none")
        self.assertEqual(pg.locator(".fg-btn.on").get_attribute("data-grupo"), "resumen")
        visibles = pg.evaluate("[...document.querySelectorAll('.fg-sub.on .ftab-btn')].map(b => b.dataset.tab)")
        self.assertEqual(visibles, ["resumen", "vida", "intel"])
        self.assertEqual(pg.locator(".ftab-btn.active").get_attribute("data-tab"), "resumen")
        self.assertEqual(self.errores, [])

    def test_los_botones_conservan_sus_ids_contadores_y_onclick(self):
        pg = self.abrir()
        # los contadores (badge) siguen DENTRO de su botón original
        self.assertEqual(pg.locator(".ftab-btn[data-tab=equipos] .badge").inner_text(), "50")
        self.assertEqual(pg.locator(".ftab-btn[data-tab=repuestos] #repCountBadge").count(), 1)
        self.assertEqual(pg.locator(".ftab-btn[data-tab=cotizaciones] #cotFichaCountBadge").count(), 1)
        self.assertEqual(pg.locator(".ftab-btn[data-tab=tickets] #tkFichaCountBadge").count(), 1)
        for tab in TABS_DE_SIEMPRE:
            self.assertEqual(pg.locator(".ftab-btn[data-tab=%s]" % tab).get_attribute("onclick"), ONCLICK_DE_SIEMPRE[tab], tab)

    def test_abrir_una_seccion_abre_su_primera_pestana_y_corre_su_carga(self):
        pg = self.abrir()
        pg.click("#fgBtn-servicio")
        self.assertEqual(pg.locator(".ftab-btn.active").get_attribute("data-tab"), "visitas")
        self.assertTrue(pg.locator("#tab-visitas").evaluate("e => e.classList.contains('active')"))
        self.assertEqual(pg.locator(".fg-btn.on").get_attribute("data-grupo"), "servicio")
        self.assertEqual(pg.evaluate("[...document.querySelectorAll('.fg-sub.on .ftab-btn')].map(b => b.dataset.tab)"),
                         ["visitas", "tickets", "repuestos", "evidencias"])
        pg.click(".ftab-btn[data-tab=tickets]")                                      # el chip ES el botón original: corre su cargarX()
        self.assertEqual(pg.evaluate("window.__cargas"), ["cargarTicketsFicha"])
        self.assertTrue(pg.locator("#tab-tickets").evaluate("e => e.classList.contains('active')"))

    def test_cada_seccion_recuerda_su_ultima_pestana(self):
        pg = self.abrir()
        pg.click("#fgBtn-servicio")
        pg.click(".ftab-btn[data-tab=repuestos]")
        pg.click("#fgBtn-comercial")
        self.assertEqual(pg.locator(".ftab-btn.active").get_attribute("data-tab"), "cotizaciones")
        pg.click("#fgBtn-servicio")                                                  # vuelve a Repuestos, no a la primera
        self.assertEqual(pg.locator(".ftab-btn.active").get_attribute("data-tab"), "repuestos")
        self.assertIn("cargarRepuestos", pg.evaluate("window.__cargas"))

    def test_los_enlaces_profundos_resaltan_la_seccion_correcta(self):
        """Otros módulos y la propia ficha llaman a switchTab('equipos'), ('finanzas'), ('visitas')… directamente."""
        pg = self.abrir()
        esperado = {"equipos": "equipos", "finanzas": "comercial", "visitas": "servicio", "historial": "comunicacion", "intel": "resumen",
                    "documentos": "comercial", "evidencias": "servicio", "contrato": "equipos", "reportes": "equipos", "vida": "resumen"}
        for tab, grupo in esperado.items():
            pg.evaluate("switchTab('%s')" % tab)
            self.assertEqual(pg.locator(".fg-btn.on").get_attribute("data-grupo"), grupo, tab)
            self.assertEqual(pg.locator(".ftab-btn.active").get_attribute("data-tab"), tab, tab)
            self.assertEqual(pg.locator(".fg-sub.on").get_attribute("data-grupo"), grupo, tab)

    def test_una_pestana_recordada_del_almacenamiento_arranca_en_su_seccion(self):
        html = self.html_base.replace("</script><script>" + JS_GRUPOS,
                                      "switchTab(localStorage.getItem(TAB_KEY)||'resumen');</script><script>" + JS_GRUPOS, 1)
        pg = self.abrir(html=html, storage={"ficha_tab_139": "tickets"})
        self.assertEqual(pg.locator(".fg-btn.on").get_attribute("data-grupo"), "servicio")
        self.assertEqual(pg.locator(".ftab-btn.active").get_attribute("data-tab"), "tickets")

    # ── vista clásica ──
    def test_la_vista_clasica_devuelve_las_15_pestanas_en_su_orden_y_se_recuerda(self):
        pg = self.abrir()
        pg.click("#fgBtn-comercial")
        pg.click("#fgVista")
        self.assertNotIn("fg-nueva", pg.evaluate("document.documentElement.className"))
        orden = pg.evaluate("[...document.querySelectorAll('.ficha-tabs .ftab-btn')].map(b => b.dataset.tab)")
        self.assertEqual(orden, TABS_DE_SIEMPRE)                                   # el MISMO orden de siempre
        self.assertEqual(pg.evaluate("getComputedStyle(document.getElementById('fgNav')).display"), "none")
        self.assertEqual(pg.evaluate("localStorage.getItem('ficha_vista_2')"), "clasica")
        self.assertIn("Vista nueva", pg.locator("#fgVista").inner_text())
        pg.click(".ftab-btn[data-tab=documentos]")                                   # y sigue funcionando como antes
        self.assertTrue(pg.locator("#tab-documentos").evaluate("e => e.classList.contains('active')"))
        pg.click("#fgVista")                                                         # de vuelta a la nueva
        self.assertIn("fg-nueva", pg.evaluate("document.documentElement.className"))
        self.assertEqual(pg.locator(".fg-btn.on").get_attribute("data-grupo"), "comercial")

    def test_si_la_vista_clasica_esta_guardada_la_pagina_abre_clasica(self):
        pg = self.abrir(vista="clasica")
        self.assertNotIn("fg-nueva", pg.evaluate("document.documentElement.className"))
        self.assertEqual(pg.locator(".ficha-tabs .ftab-btn").count(), 15)
        self.assertEqual(pg.locator(".fg-sub .ftab-btn").count(), 0)
        self.assertEqual(pg.locator(".ficha-tabs").evaluate("e => getComputedStyle(e).display"), "flex")    # la barra de siempre

    def test_sin_almacenamiento_funciona_igual(self):
        bloqueo = "<script>Object.defineProperty(window,'localStorage',{get(){throw new Error('bloqueado')}});</script>"
        html = self.html_base.replace("<head>", "<head>" + bloqueo, 1).replace(
            "localStorage.setItem(TAB_KEY, name);", "try{localStorage.setItem(TAB_KEY, name)}catch(e){}")
        pg = self.abrir(html=html, limpiar=False)
        self.assertEqual(pg.locator(".ficha-tabs .ftab-btn").count(), 15)           # sin almacenamiento = la barra de siempre
        pg.click(".ftab-btn[data-tab=visitas]")
        self.assertEqual(pg.locator(".ftab-btn.active").get_attribute("data-tab"), "visitas")
        pg.click("#fgVista")                                                         # alternar sin almacenamiento tampoco rompe: pasa a la nueva…
        self.assertEqual(pg.locator(".fg-btn").count(), 5)
        pg.click("#fgBtn-servicio")
        self.assertEqual(pg.locator(".ftab-btn.active").get_attribute("data-tab"), "visitas")
        pg.click("#fgVista")                                                         # …y vuelve a la clásica (la vista se lee de la página)
        self.assertEqual(pg.locator(".ficha-tabs .ftab-btn").count(), 15)

    def test_una_pestana_nueva_que_nadie_mapeo_no_se_pierde(self):
        html = self.html_base.replace('<button class="ftab-btn" data-tab="historial"',
                                      '<button class="ftab-btn" data-tab="futura" onclick="switchTab(\'futura\')">Futura</button>'
                                      '<button class="ftab-btn" data-tab="historial"', 1)
        html = html.replace('<div class="ftab-pane" id="tab-historial">',
                            '<div class="ftab-pane" id="tab-futura">F</div><div class="ftab-pane" id="tab-historial">', 1)
        pg = self.abrir(html=html)
        self.assertEqual(pg.locator(".fg-btn").count(), 6)                          # aparece la sección «Más»
        pg.click("#fgBtn-mas")
        self.assertEqual(pg.locator(".ftab-btn.active").get_attribute("data-tab"), "futura")

    # ── medidas (REGLA #61) ──
    def test_sin_scroll_de_lado_botones_de_44_y_letra_minima_en_cinco_anchos(self):
        for ancho in (390, 820, 1280, 1366, 1440):
            pg = self.abrir(ancho=ancho)
            for grupo in ("resumen", "servicio", "comercial", "comunicacion", "equipos"):
                pg.click("#fgBtn-%s" % grupo)
                m = pg.evaluate("""() => {
                    const de = document.documentElement, bajos = [], letra = [];
                    for (const e of document.querySelectorAll('.fg-btn, .fg-vista, .fg-sub.on .ftab-btn')) {
                        const r = e.getBoundingClientRect(); if (!r.width) continue;
                        if (r.height < 43.5) bajos.push(e.textContent.trim().slice(0, 20) + ':' + Math.round(r.height));
                        const fs = parseFloat(getComputedStyle(e).fontSize); if (fs < 12.99) letra.push(e.textContent.trim().slice(0, 20) + ':' + fs);
                    }
                    const sub = document.querySelector('.fg-sub.on');
                    return {scrollW: de.scrollWidth, W: window.innerWidth, bajos, letra,
                            subScroll: sub.scrollWidth - sub.clientWidth,
                            alto: Math.round(document.querySelector('.ficha-tabs-wrap').getBoundingClientRect().height)};
                }""")
                etiqueta = "%d px · %s" % (ancho, grupo)
                self.assertLessEqual(m["scrollW"], m["W"], etiqueta + " scroll de lado de la página")
                self.assertLessEqual(m["subScroll"], 0, etiqueta + " scroll de lado de los chips")
                self.assertEqual(m["bajos"], [], etiqueta + " botones bajo 44 px")
                self.assertEqual(m["letra"], [], etiqueta + " letra bajo 13 px")
            if ancho >= 820:        # la barra completa (secciones + chips) cabe en poco alto; antes eran 45 px pero con 256 px de pestañas CORTADAS a la derecha
                self.assertLessEqual(m["alto"], 160, "%d px: alto de la barra %s" % (ancho, m["alto"]))
        self.assertEqual(self.errores, [])


if __name__ == "__main__":
    unittest.main()

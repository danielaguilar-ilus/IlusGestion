# -*- coding: utf-8 -*-
"""LISTA DE CLIENTES (/clientes) — repaso del 5-nov-2026 (Daniel: «ese módulo es viejo, quisiera enchularlo y ahorrar pestañas»).

Lo que NO puede pasar (REGLA #4.2): que el repaso quite una acción. Lo que SÍ tiene que pasar (REGLA #61 y #15): letra ≥ 12 px, botones de 44 px,
datos completos (no cortados con «…»), tarjeta compacta en celular y los dos avisos de arriba en una sola fila en escritorio.

  · Guardas de texto: cada id, cada onclick, cada url_for e include que tenía la plantilla antes del repaso sigue ahí.
  · Navegador real (Chromium): el contenido REAL de la plantilla, renderizado con Jinja y datos de mentira, con style.css + mobile.css + Bootstrap reales
    y su JS real (filtros y paginado), medido a 390 / 820 / 1280 / 1366 / 1440 px.
Correr:  python -m pytest tests/test_clientes_lista.py -q -p no:warnings
Necesita Bootstrap 5.3.3 (lo baja una vez a la carpeta temporal; sin red, las pruebas de navegador se saltan).
"""
import datetime as dt
import os
import re
import sys
import tempfile
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, RAIZ)
from tests import _fuente_codigo as FC  # noqa: E402

HTML = FC.texto("templates/mantenciones/clientes.html")
CSS = FC.texto("static/clientes_lista.css")

# Lo que la plantilla tenía ANTES del repaso (commit 4fbf1717): todo debe seguir.
IDS = ["btnGrid", "btnList", "clientGrid", "duplicadosBody", "fichasOcultasQuery", "fichasOcultasResultado", "fltChips", "fltClear", "fltEmpty",
       "fltOcultos", "fltOcultosTxt", "fltResult", "fltSearchClear", "huerfanosCont", "incluirActivos", "modalDuplicados", "modalFichasOcultas",
       "modalHuerfanos", "modalPlanConfig", "pagCtrl", "pageInfo", "pcDesc", "pcDescMax", "pcEscActiva", "pcEscalera", "pcGuardar", "pcIncluye",
       "pcMesesPrimera", "pcMsgWa", "pcVigencia", "pcVisitas", "perPage", "planCfgAviso", "searchInput"]
ONCLICKS = ["ILUSFilter.clear", "ILUSFilter.remove", "ILUSFilter.set", "ILUSFilter.setSearch", "ILUSFilter.verOcultos", "abrirDuplicados", "abrirPlanConfig",
            "abrirReporteExcel", "aplicarKpiFiltro", "buscarFichasOcultas", "fusionarClientes", "guardarPlanConfig", "restaurarCliente", "setView", "verHuerfanos"]
URLS = ["mant_cliente_wizard", "mant_cobertura_page", "mant_ficha"]
INCLUDES = ["mantenciones/_clientes_botones.html", "mantenciones/_reporte_excel_modal.html", "mantenciones/_salud_proceso.html"]


class TestNadaSeQuito(unittest.TestCase):
    def test_cada_id_de_siempre_sigue(self):
        faltan = [i for i in IDS if not re.search(r'\bid="%s"' % re.escape(i), HTML)]
        self.assertEqual(faltan, [])

    def test_cada_accion_de_siempre_sigue(self):
        faltan = [o for o in ONCLICKS if not re.search(r'onclick="%s\(' % re.escape(o), HTML)]
        self.assertEqual(faltan, [])
        for u in URLS:
            self.assertIn("url_for('%s'" % u, HTML)
        for inc in INCLUDES:
            self.assertIn('{% include "' + inc + '" %}', HTML)

    def test_los_nueve_kpis_y_las_cuatro_filas_de_filtros_siguen(self):
        self.assertEqual(HTML.count('class="kpi-card'), 9)
        i = HTML.index("{% set FILTER_GROUPS = [")
        grupos = HTML[i:HTML.index("] %}", i)]
        for fila in ("Tipo", "Estado", "Contrato", "Equipos"):
            self.assertRegex(grupos, r"label['\"]?\s*:\s*['\"]" + fila)
        self.assertIn("{% for g in FILTER_GROUPS %}", HTML)

    def test_la_tarjeta_conserva_todos_sus_datos(self):
        i = HTML.index('<div class="client-card-v2"')
        j = HTML.index("</div><!-- /client-card-v2 -->")
        tarjeta = HTML[i:j]
        for dato in ("CONTRATO", "EQUIPOS", "PRÓX. VISITA", "COMPLETAJE", "TIPO DE CONTRATO", "INICIO", "TÉRMINO", "VALOR MENSUAL", "CONTACTO PRINCIPAL",
                     "prosp_strip(c, plan_cfg)", "No cuenta con contrato activo", "c.contacto_email", "c.contacto_tel", "c.comuna"):
            self.assertIn(dato, tarjeta)


class TestAvisos(unittest.TestCase):
    def test_los_dos_avisos_comparten_contenedor_y_cada_uno_sale_una_vez(self):
        i = HTML.index('<div class="cl-avisos">')
        j = HTML.index("</div>\n", HTML.index('{% endif %}\n</div>', HTML.index("inst-alerta", i)))
        cont = HTML[i:j + 6]
        self.assertIn('{% include "mantenciones/_salud_proceso.html" %}', cont)
        self.assertIn('<div class="inst-alerta">', cont)
        self.assertEqual(HTML.count('<div class="inst-alerta">'), 1)
        self.assertIn("Ver clientes", cont)
        self.assertIn("aplicarKpiFiltro('instalacion')", cont)

    def test_el_permiso_de_salud_del_proceso_no_cambio(self):
        i = HTML.index('{% include "mantenciones/_salud_proceso.html" %}')
        self.assertIn("{% if permissions.admin or permissions.superadmin %}", HTML[i - 400:i])

    def test_el_css_va_despues_de_todos_los_estilos_de_la_pagina(self):
        link = HTML.index("<link rel=\"stylesheet\" href=\"{{ url_for('static', filename='clientes_lista.css') }}\">")
        self.assertEqual(HTML.count("filename='clientes_lista.css'"), 1)
        self.assertGreater(link, HTML.rindex("</style>"))
        self.assertGreater(link, HTML.rindex("<style>"))


class TestCss(unittest.TestCase):
    def test_regla_61(self):
        tams = [float(t) for t in re.findall(r"font-size:\s*([\d.]+)rem", CSS)]
        self.assertGreaterEqual(min(tams), 0.75)
        self.assertIn("min-height: 44px", CSS)
        self.assertIn("@media (max-width: 575.98px)", CSS)
        self.assertIn("prefers-reduced-motion", CSS)

    def test_ya_no_se_cortan_los_datos_con_puntos_suspensivos(self):
        self.assertRegex(CSS, r"\.cc2-kpi-val[^{]*\{[^}]*white-space: normal")
        self.assertRegex(CSS, r"text-overflow: clip")


# ═══════════════════════════════════════════════════════════════════════════════════════════════════
#  En un navegador real
# ═══════════════════════════════════════════════════════════════════════════════════════════════════
BOOTSTRAP = os.path.join(tempfile.gettempdir(), "ilus_bootstrap_5.3.3.min.css")


def _bootstrap():
    if os.path.exists(BOOTSTRAP) and os.path.getsize(BOOTSTRAP) > 100000:
        return open(BOOTSTRAP, "rb").read()
    try:
        import urllib.request
        data = urllib.request.urlopen("https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css", timeout=30).read()
        open(BOOTSTRAP, "wb").write(data)
        return data
    except Exception:
        return None


def _clientes_falsos():
    hoy = dt.date(2026, 11, 5)
    nombres = ["Gimnasio Fit Paradise Sociedad por Acciones Limitada de Las Condes", "A.Y.M ASOCIADOS", "Club Deportivo y Social Los Leones Chicureo", "110Xciento"]
    out = []
    for n in range(60):
        out.append({
            "id": 100 + n, "razon_social": nombres[n % 4] + ("" if n < 4 else " %d" % n), "estado": "activo" if n % 3 else "prospecto",
            "badge": ["activo", "prospecto", "inactivo", "advertencia"][n % 4], "tipo_cliente": ["mantencion", "instalacion", "arriendo", "leasing"][n % 4],
            "contrato_estado": ["vigente", "sin_contrato", "por_vencer", "vencido"][n % 4], "completaje": 40 + n * 5, "maquinas_count": n % 5,
            "contratos_count": n % 3, "prox_visita": hoy + dt.timedelta(days=n) if n % 2 else None, "contrato_freq": 3 if n % 2 else 0,
            "contrato_indef": n % 5 == 0, "contrato_inicio": dt.date(2025, 3, 1) if n % 2 else None,
            "contrato_vencimiento": dt.date(2026, 12, 31) if n % 2 else None, "contrato_monto": 1234567 if n % 2 else 0,
            "comuna": "Las Condes", "ciudad": "Santiago", "contacto_tel": "+56 9 1234 5678",
            "contacto_email": "contacto.administracion@gimnasiofitparadisechile.cl" if n % 2 else "ana@fit.cl",
            "contacto_nombre": "Carla Pérez González de la Fuente" if n % 2 else "", "pr": None, "maquinas": []})
    return out


def _contenido():
    """El bloque `content` REAL de la plantilla renderizado con datos de mentira."""
    import jinja2
    ini = HTML.index("{% block content %}") + len("{% block content %}")
    fin = HTML.rindex("{% endblock %}")
    cuerpo = HTML[ini:fin]
    botones = FC.texto("templates/mantenciones/_clientes_botones.html")
    salud = ('<div id="repInstAviso" class="sal-aviso"><i class="bi bi-clipboard2-pulse"></i><div><b>Salud del proceso de instalaciones: 7 puntos por revisar</b>'
             '<span>113 · ventas con instalación sin hoja — 4 · clientes mal clasificados — 60 · hojas sin productos — 17 · sin equipos — 12 · sin teléfono o correo.</span></div>'
             '<button type="button" class="btn btn-sm">Ver salud del proceso</button></div>')
    env = jinja2.Environment(loader=jinja2.DictLoader({
        "mantenciones/_prosp_strip.html": "{% macro prosp_strip(c, plan_cfg) %}{% endmacro %}",
        "mantenciones/_clientes_botones.html": botones, "mantenciones/_reporte_excel_modal.html": "", "mantenciones/_salud_proceso.html": salud}))
    import types
    env.globals["url_for"] = lambda ep, **kw: ("/static/" + kw.get("filename", "")) if ep == "static" else "#"
    env.globals["request"] = types.SimpleNamespace(endpoint="mant_clientes", path="/clientes", args={})
    gs = {"total": 404, "activos": 50, "inst_pendientes": 163, "inst_sin_contactar": 163, "inst_listos": 50, "inst_vencida_1a": 4, "inst_vencidos": 0,
          "con_contrato": 28, "con_equipos": 309, "visitas_30d": 11, "completaje_avg": 75}
    return env.from_string(cuerpo).render(clientes=_clientes_falsos(), global_stats=gs, filtros={}, permissions={"admin": True, "superadmin": True},
                                          plan_cfg={}, vista="grid")


def _pagina():
    return ("<!doctype html><html lang='es'><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'>"
            "<link rel='stylesheet' href='https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css'>"
            "<link rel='stylesheet' href='/static/style.css'><link rel='stylesheet' href='/static/mobile.css'></head><body>"
            "<script>window.bootstrap={Modal:function(){this.show=function(){};this.hide=function(){}}};window.ilusToast=function(){};"
            "window.fetch=function(){return Promise.resolve({ok:true,json:function(){return Promise.resolve({})}})};</script>"
            "<div class='main-wrapper'>" + _contenido() + "</div></body></html>")


try:
    from playwright.sync_api import sync_playwright
except Exception:                      # pragma: no cover
    sync_playwright = None

ANCHOS = (390, 820, 1280, 1366, 1440)
MEDIR = r"""() => {
  const de = document.documentElement, out = {scroll: de.scrollWidth - de.clientWidth, chicas: [], bajos: [], cortados: [], tarjeta: 0, errores: window.__errores || []};
  const zonas = document.querySelectorAll('.mod-hero, .cl-avisos, .kpi-strip, #clientGrid, .cli-footer, #fltResult, .flt-pill');
  const visto = new Set();
  zonas.forEach(z => { const w = document.createTreeWalker(z, NodeFilter.SHOW_TEXT);
    while (w.nextNode()) { const t = w.currentNode, el = t.parentElement; if (!t.textContent.trim() || !el || visto.has(t)) continue; visto.add(t);
      if (el.closest('script,style,[hidden],.modal')) continue; const r = el.getBoundingClientRect(); if (!r.width || !r.height) continue;
      const fs = parseFloat(getComputedStyle(el).fontSize); if (fs < 11.99) out.chicas.push(el.className.toString().slice(0, 24) + '|' + t.textContent.trim().slice(0, 18) + '|' + fs.toFixed(1)); } });
  document.querySelectorAll('.mod-hero .btn, .cl-avisos .btn, .flt-pill, #perPage, .cli-footer .page-link, .cc2-prosp-acts .pa, .kpi-clickable').forEach(e => {
    const r = e.getBoundingClientRect(); if (r.width && r.height < 43.5) out.bajos.push(e.className.toString().slice(0, 30) + ' ' + Math.round(r.height)); });
  document.querySelectorAll('.client-card-v2 .cc2-kpi-val, .client-card-v2 .cc2-contact-name, .client-card-v2 .cc2-ci-row span').forEach(e => {
    if (e.scrollWidth > e.clientWidth + 1) out.cortados.push(e.textContent.trim().slice(0, 30)); });
  const c = document.querySelector('.card-wrapper:not([hidden]) .client-card-v2'); out.tarjeta = c ? Math.round(c.getBoundingClientRect().height) : 0;
  return out; }"""


@unittest.skipIf(sync_playwright is None, "Playwright no está instalado")
class TestNavegadorReal(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bs = _bootstrap()
        if cls.bs is None:
            raise unittest.SkipTest("sin red para bajar Bootstrap")
        cls._pw = sync_playwright().start()
        try:
            cls.nav = cls._pw.chromium.launch()
        except Exception as e:
            cls._pw.stop()
            raise unittest.SkipTest("Chromium no disponible: %s" % e)
        cls.html = _pagina()
        cls.ctx = cls.nav.new_context(viewport={"width": 1280, "height": 900})
        cls.ctx.set_default_timeout(8000)

        def ruta(r):
            u = r.request.url
            if u.startswith("https://cdn.jsdelivr.net/"):
                return r.fulfill(status=200, content_type="text/css", body=cls.bs)
            if u.startswith("https://lista.test/static/"):
                f = os.path.join(RAIZ, "static", u.split("/static/", 1)[1].split("?")[0].replace("/", os.sep))
                if os.path.exists(f):
                    return r.fulfill(status=200, content_type="text/css" if f.endswith(".css") else "application/javascript", body=open(f, "rb").read())
                return r.fulfill(status=404, body="")
            if u.startswith("https://lista.test/"):
                return r.fulfill(status=200, content_type="text/html; charset=utf-8", body=cls.html)
            return r.abort()
        cls.ctx.route("**/*", ruta)
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

    def abrir(self, ancho):
        self.pg.set_viewport_size({"width": ancho, "height": 900})
        self.pg.goto("https://lista.test/clientes")
        self.pg.wait_for_selector(".client-card-v2")
        return self.pg

    def test_el_js_real_de_filtros_y_paginado_arranca_sin_errores(self):
        pg = self.abrir(1280)
        self.assertEqual(self.errores, [], self.errores)
        self.assertGreaterEqual(pg.locator("#pagCtrl .page-link").count(), 1, "el paginado se dibuja")
        self.assertGreater(pg.locator(".card-wrapper").count(), 0)

    def test_los_dos_avisos_comparten_fila_en_escritorio_y_se_apilan_en_celular(self):
        pg = self.abrir(1440)
        arriba = pg.evaluate("[...document.querySelectorAll('.sal-aviso, .inst-alerta')].map(e => Math.round(e.getBoundingClientRect().top))")
        self.assertEqual(len(arriba), 2)
        self.assertEqual(arriba[0], arriba[1], "a 1440 px los dos avisos van lado a lado")
        pg = self.abrir(390)
        arriba = pg.evaluate("[...document.querySelectorAll('.sal-aviso, .inst-alerta')].map(e => Math.round(e.getBoundingClientRect().top))")
        self.assertLess(arriba[0], arriba[1], "en celular van uno bajo el otro")

    def test_regla_61_y_datos_completos_en_cinco_anchos(self):
        for ancho in ANCHOS:
            m = self.abrir(ancho).evaluate(MEDIR)
            e = "a %d px" % ancho
            self.assertLessEqual(m["scroll"], 0, "scroll de lado " + e)
            self.assertEqual(m["chicas"], [], "letra menor a 12 px " + e)
            self.assertEqual(m["bajos"], [], "objetivos menores a 44 px " + e)
            self.assertEqual(m["cortados"], [], "datos cortados " + e)

    def test_la_tarjeta_en_celular_es_compacta(self):
        m = self.abrir(390).evaluate(MEDIR)
        print("alto de la primera tarjeta a 390 px:", m["tarjeta"])
        self.assertLessEqual(m["tarjeta"], 400, "antes del repaso: 657 px de alto en escritorio y 8 filas apiladas en celular")
        self.assertEqual(self.pg.evaluate("getComputedStyle(document.querySelector('.cc2-banner')).display"), "none")
        self.assertEqual(self.abrir(1280).evaluate("getComputedStyle(document.querySelector('.cc2-banner')).display"), "flex", "en escritorio la decoración sigue")

    def test_en_celular_el_menu_de_arriba_va_en_dos_columnas(self):
        pg = self.abrir(390)
        cols = pg.evaluate("getComputedStyle(document.querySelector('.mod-hero > .d-flex')).gridTemplateColumns.split(' ').length")
        self.assertEqual(cols, 2)

    def test_filtrar_sigue_funcionando(self):
        pg = self.abrir(1280)
        total = pg.locator(".card-wrapper:visible").count()
        pg.locator(".flt-pill", has_text="Instalación").first.click()
        self.assertLess(pg.locator(".card-wrapper:visible").count(), total)
        pg.locator("#fltClear").first.click() if pg.locator("#fltClear:visible").count() else pg.locator(".kpi-card", has_text="Total clientes").click()
        self.assertEqual(pg.locator(".card-wrapper:visible").count(), total)


if __name__ == "__main__":
    unittest.main()

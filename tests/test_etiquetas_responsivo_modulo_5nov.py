# -*- coding: utf-8 -*-
"""📱 5-nov-2026 — Etiquetas, módulo completo en celular y tableta (REGLA #61).

Daniel (5-nov): «La parte responsiva de las etiquetas… revisa todo el módulo: la parte de productos, la impresión y además la parte de carga masiva».
Lo que quedó medido ANTES (vista previa local con los CSS/JS/plantillas reales y datos de peor caso: nombres de 110 caracteres, 5 bultos):
  · carga masiva a 390 px: 2 tablas con scroll de lado (361/309 px), letra de 10,4 px, select de 34 px;
  · ficha del producto: tabla de bultos con scroll de lado (560/313), letra de 10,4 px;
  · configurar impresión: tabla (534/313), 16 botones < 44 px (el − y el + medían 28×44);
  · vista de etiquetas: la vista previa de 567 px no cabía en 303 px útiles (scroll de lado), letra de 11,5 px y botones de 36 px;
  · el catálogo en el teléfono no tenía forma de ordenar (los encabezados clicables solo existen en la tabla de escritorio).
DESPUÉS: ninguna pantalla corre la página de lado ni tiene scroll interno a 390/820/1280/1366/1440; letra mínima 12 px; en celular todo lo que
se toca mide 44 px; las tablas pasan a tarjetas compactas (las mismas celdas, nada se quita: REGLA #4.2) y la vista previa de la etiqueta se escala.

Aquí se prueba (texto + render de Jinja, sin navegador): que esas piezas sigan puestas y que no vuelvan las letras chicas ni el rebote.
Correr:  python -m pytest tests/test_etiquetas_responsivo_modulo_5nov.py -q -p no:warnings
"""
import datetime
import os
import re
import unittest

from jinja2 import ChoiceLoader, DictLoader, Environment, FileSystemLoader

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _leer(*partes):
    with open(os.path.join(RAIZ, *partes), encoding="utf-8") as fh:
        return fh.read().replace("\r\n", "\n")


CSS = _leer("static", "style.css")
APPJS = _leer("static", "app.js")
FOTOJS = _leer("static", "foto_editor.js")
INDEX = _leer("templates", "index.html")
DETALLE = _leer("templates", "product_detail.html")
FORM = _leer("templates", "product_form.html")
SETUP = _leer("templates", "print_setup.html")
LABELS = _leer("templates", "print_labels.html")
MASIVO = _leer("templates", "etiquetas_masivo.html")
APP = _leer("app.py")

INI = "ETIQUETAS — resto del módulo"
BLOQUE = CSS[CSS.rindex("/*", 0, CSS.index(INI)):CSS.rindex("/*", 0, CSS.index("TABLA DE EQUIPOS DEL CLIENTE"))]

CHICA = re.compile(r"font-size:\s*(\.(?:[0-6]\d*|7[0-4]\d*)\s*rem|\.?(?:[0-9]|1[01])(?:\.\d+)?\s*px)")


def _reglas(css):
    sin = re.sub(r"/\*.*?\*/", "", css, flags=re.S)
    out = []
    for m in re.finditer(r"([^{}@][^{}]*)\{([^{}]*)\}", sin):
        sel = " ".join(m.group(1).split())
        props = {}
        for d in m.group(2).split(";"):
            if ":" in d:
                k, v = d.split(":", 1)
                props[k.strip().lower()] = v.strip().lower()
        out.append((sel, props))
    return out


class TestCssDelModulo(unittest.TestCase):
    def test_el_bloque_existe_y_todo_cuelga_de_clases_propias(self):
        """No toca otras pantallas: cada selector parte de una clase del módulo (.eti-scope, .eti-tcard, .bf-…, .ps-acciones, .eti-orden…)."""
        permitidos = (".eti-scope", ".eti-tcard", ".bf-", ".ps-acciones", ".eti-orden", ".eti-mobile-list", ".ilus-table-fullview")
        reglas = _reglas(BLOQUE)
        self.assertGreater(len(reglas), 25)
        for sel, _ in reglas:
            for parte in [p.strip() for p in sel.split(",")]:
                self.assertTrue(parte.startswith(permitidos), "selector global en el bloque de Etiquetas: %r" % parte)

    def test_ninguna_letra_de_menos_de_12_px_en_el_bloque(self):
        self.assertIsNone(CHICA.search(BLOQUE), CHICA.search(BLOQUE) and CHICA.search(BLOQUE).group(0))

    def test_las_columnas_de_grilla_usan_minmax_cero(self):
        """`1fr` suelto es minmax(auto,1fr): no baja del tamaño del contenido y empuja la página (lección de Técnicos externos)."""
        for _, props in _reglas(BLOQUE):
            v = re.sub(r"minmax\([^)]*\)", "", props.get("grid-template-columns", ""))
            self.assertNotRegex(v, r"(^|[\s(,])1fr", v)

    def test_tableta_y_celular_todo_lo_que_se_toca_mide_44(self):
        i = BLOQUE.index("@media (max-width: 991.98px)")
        f = BLOQUE[i:i + 900]
        self.assertIn("min-height: 44px", f)
        self.assertIn("min-width: 44px", f)
        self.assertIn(".eti-scope .form-select", f)
        self.assertIn("font-size: 16px", f)  # anti zoom de iOS

    def test_tabla_a_tarjeta_solo_bajo_768_y_no_quita_celdas(self):
        i = BLOQUE.index("@media (max-width: 767.98px) {\n  .eti-tcard")
        f = BLOQUE[i:BLOQUE.index("Editar / nuevo producto", i)]
        self.assertIn(".eti-tcard thead { display: none; }", f)
        self.assertIn("content: attr(data-lbl)", f)
        self.assertNotIn("td { display: none", f)  # REGLA #4.2: ninguna celda desaparece

    def test_el_ancho_minimo_de_la_tabla_solo_aplica_en_pantalla_ancha(self):
        self.assertIn("@media (min-width: 768px) { .eti-tcard { min-width: 480px; } }", BLOQUE)

    def test_la_tabla_de_bultos_del_formulario_tiene_letra_y_campos_de_celular(self):
        self.assertIn("#bultosTable th {\n  font-size: .75rem;", CSS)
        self.assertIn('.eti-scope #bultosTable input[type="number"] { font-size: 16px; }', BLOQUE)

    def test_el_paso_de_copias_es_comodo_de_tocar(self):
        self.assertIn(".eti-scope .qty-input { width: 64px !important; }", BLOQUE)
        self.assertIn(".eti-scope .qty-dec, .eti-scope .qty-inc {\n    min-width: 44px;", BLOQUE)

    def test_cada_medida_cae_en_su_casilla_de_la_tarjeta_del_formulario(self):
        """El CSS usa nth-child: N°(1), largo(2), ancho(3), alto(4), peso(5). Si alguien cambia el orden de app.js, esto avisa."""
        for n, area in ((2, "largo"), (3, "ancho"), (4, "alto"), (5, "peso")):
            self.assertIn(".bf-tabla .bf-c-in:nth-child(%d) { grid-area: %s; }" % (n, area), BLOQUE)

    def test_selector_de_orden_del_telefono_tiene_su_css(self):
        self.assertIn(".eti-orden {", BLOQUE)
        self.assertIn(".eti-orden .form-select { grid-area: sel; min-height: 44px; font-size: 16px; }", BLOQUE)

    def test_el_pie_de_totales_del_formulario_no_aparece_si_esta_oculto(self):
        """#totalsRow nace con display:none en línea: la tarjeta solo lo muestra si el JS lo des-oculta."""
        self.assertIn('#totalsRow:not([style*="display:none"]):not([style*="display: none"])', BLOQUE)


class TestPlantillasDelModulo(unittest.TestCase):
    def test_ficha_y_configurar_impresion_usan_la_misma_tarjeta_y_no_cortan_nombres(self):
        for nombre, t in (("detalle", DETALLE), ("setup", SETUP)):
            self.assertIn("eti-tcard", t, nombre)
            self.assertNotIn("min-width:480px", t, nombre)
            self.assertNotIn("min-width:560px", t, nombre)
            self.assertNotIn("text-truncate", t, "%s: el nombre del producto va completo (REGLA #15)" % nombre)
            self.assertIn('class="eti-scope"', t, nombre)
            for celda in ("tc-num", "tc-dim"):
                self.assertIn(celda, t, nombre)
        self.assertEqual(SETUP.count("tc-dim"), 5)
        self.assertEqual(DETALLE.count("tc-dim"), 5)
        for rot in ("Largo", "Ancho", "Alto", "Peso", "Peso Vol"):
            self.assertIn('data-lbl="%s"' % rot, SETUP)
            self.assertIn('data-lbl="%s"' % rot, DETALLE)

    def test_el_paso_de_copias_tiene_nombre_accesible(self):
        self.assertEqual(SETUP.count("aria-label="), 3)
        self.assertIn("Una copia menos del bulto", SETUP)
        self.assertIn("Una copia más del bulto", SETUP)

    def test_los_botones_de_impresion_de_la_ficha_siguen_ahi(self):
        """REGLA #4.2: en el celular el CSS global los oculta (no-mobile-print) y queda el botón flotante; en tableta y escritorio se ven."""
        for frag in ("Imprimir todas", "Configurar copias", "fab-print", "Editar medidas"):
            self.assertIn(frag, DETALLE)

    def test_formulario_de_producto_cada_bulto_es_tarjeta(self):
        self.assertIn('class="eti-scope"', FORM)
        self.assertIn('class="table mb-0 bf-tabla" id="bultosTable"', FORM)
        self.assertIn('data-lbl="Peso total (kg)"', FORM)
        self.assertIn('data-lbl="Peso Vol total"', FORM)
        self.assertIn("bf-acciones", FORM)

    def test_la_fila_de_bulto_que_arma_app_js_trae_clases_etiquetas_y_teclado_numerico(self):
        i = APPJS.index("tr.innerHTML = `")
        f = APPJS[i:APPJS.index("`;", i)]
        self.assertIn("bf-c-num", f)
        self.assertEqual(f.count("bf-c-in"), 4)
        self.assertEqual(f.count('inputmode="decimal"'), 4)
        for rot in ("Largo (cm)", "Ancho (cm)", "Alto (cm)", "Peso (kg)"):
            self.assertIn('data-lbl="%s"' % rot, f)
            self.assertIn('aria-label="%s del bulto' % rot, f)
        self.assertIn("bf-c-pv", f)
        self.assertIn("bf-c-del", f)
        self.assertIn('aria-label="Eliminar bulto', f)
        # el orden de las celdas lo usa el CSS (nth-child): N°, largo, ancho, alto, peso, PV, borrar
        self.assertLess(f.index("bf-c-num"), f.index('name="largo_'))
        self.assertLess(f.index('name="largo_'), f.index('name="ancho_'))
        self.assertLess(f.index('name="ancho_'), f.index('name="alto_'))
        self.assertLess(f.index('name="alto_'), f.index('name="peso_'))

    def test_buscador_erp_sin_nombres_cortados_ni_letra_chica(self):
        self.assertNotIn("text-overflow:ellipsis", FORM)
        self.assertNotIn("white-space:nowrap", FORM)
        self.assertIsNone(CHICA.search(FORM), CHICA.search(FORM) and CHICA.search(FORM).group(0))
        self.assertNotIn("color:#999", FORM)  # 2,8:1 sobre blanco

    def test_carga_masiva_sin_letra_chica_y_con_tarjetas(self):
        self.assertIsNone(CHICA.search(MASIVO), CHICA.search(MASIVO) and CHICA.search(MASIVO).group(0))
        self.assertIn('<div class="em-page">', MASIVO)
        self.assertIn("grid-template-columns:80px minmax(0,1fr) auto auto", MASIVO)
        self.assertGreaterEqual(MASIVO.count('class="table table-sm mb-0 em-tabla'), 3)
        for c in ("em-c-chk", "em-c-sku", "em-c-nom", "em-c-cant", "em-c-act", "em-c-fila", "em-c-main", "em-c-estado", "em-c-res"):
            self.assertIn(c, MASIVO)
        self.assertIn("@media (max-width:767px)", MASIVO)

    def test_carga_masiva_no_pierde_ninguna_accion(self):
        """REGLA #4.2: todo lo que existía sigue existiendo."""
        for frag in ("prevToggleAll(true)", "prevToggleAll(false)", "importarTodosErp()", "importarUnoErp(", "Agregar medidas",
                     "Crear manual", "crearTodosLosFaltantes()", "Crear ahora", "btnZip", "procesar()", "analizarPreview()"):
            self.assertIn(frag, MASIVO)

    def test_vista_de_etiquetas_escala_la_vista_previa_y_la_impresion_no(self):
        self.assertIn("function fitPreview()", LABELS)
        self.assertIn('window.addEventListener("resize"', LABELS)
        i = LABELS.index("@media print {")
        self.assertIn("transform: none !important;", LABELS[i:LABELS.index("</style>", i)])
        self.assertIn(".label-title > a { min-width: 44px; min-height: 44px;", LABELS)
        # letra de pantalla >= 12 px (la etiqueta impresa es diseño de impresión y no se toca)
        for sel in (".group-meta {", ".copy-foot {"):
            j = LABELS.index(sel)
            self.assertNotRegex(LABELS[j:LABELS.index("}", j)], r"font-size:\s*\.7[0-9]?rem")
        self.assertIn(".copy-foot {\n  width: var(--label-w);", LABELS)  # el diseño de escritorio no cambia

    def test_la_vista_de_etiquetas_conserva_las_acciones(self):
        for frag in ("Vista previa PDF", "Descargar PDF", "openPdf(", "window.print()", "Imprimir página"):
            self.assertIn(frag, LABELS)

    def test_el_editor_de_fotos_mide_44_en_tableta_y_celular(self):
        self.assertIn("@media (max-width:991px){.ife-card-actions button{width:44px;height:44px}}", FOTOJS)
        self.assertNotIn('color:#bbb;font-size:.7rem">Clic o arrastra', FOTOJS)


class TestOrdenEnElTelefono(unittest.TestCase):
    def test_las_opciones_son_exactamente_la_lista_blanca_del_servidor(self):
        m = re.search(r"_ORDEN_SQL = \{(.*?)\n    \}", APP, flags=re.S)
        self.assertIsNotNone(m)
        servidor = set(re.findall(r'"(\w+)":\s+"u\.', m.group(1)))
        i = INDEX.index('<form method="get" class="eti-orden"')
        f = INDEX[i:INDEX.index("</form>", i)]
        opciones = set(re.findall(r'<option value="(\w+)"', f))
        self.assertEqual(opciones, servidor)

    def test_preserva_busqueda_filtro_ver_y_tamano(self):
        i = INDEX.index('<form method="get" class="eti-orden"')
        f = INDEX[i:INDEX.index("</form>", i)]
        for n in ('name="q"', 'name="fotos"', 'name="ver"', 'name="size"', 'name="dir"'):
            self.assertIn(n, f)
        self.assertIn('onchange="this.form.submit()"', f)
        self.assertIn("Tocar para invertir", f)

    def test_vive_en_la_lista_de_tarjetas_y_no_en_la_tabla(self):
        self.assertGreater(INDEX.index("eti-orden"), INDEX.index('<div class="d-md-none eti-mobile-list">'))


# ──────────────────────────────────────────────── render de verdad (Jinja) ────────────────────────────────────────────────
_BASE = """<!doctype html><html><head><title>{% block title %}{% endblock %}</title></head><body>
{% block content %}{% endblock %}{% block scripts %}{% endblock %}</body></html>"""


def _env():
    env = Environment(loader=ChoiceLoader([DictLoader({"base.html": _BASE}), FileSystemLoader(os.path.join(RAIZ, "templates"))]), autoescape=True)
    env.filters["chile_fmt"] = lambda v, fmt="%d/%m/%Y %H:%M": v.strftime(fmt) if v else ""
    env.globals["url_for"] = lambda endpoint, **kw: "#" + endpoint
    env.globals["csrf_token"] = lambda: "x"
    env.globals["photo_src"] = lambda fn: "/f/" + str(fn)
    return env


class _Req:
    def __init__(self, args=None):
        self.args = args or {}
    referrer = ""


LARGO = "Soporte para Búlgaras Premium ILUS (Single Leg Standing Squat Stand Adj Bench con Respaldo Acolchado XL Reforzado)"
PERMS = dict(create=True, edit=True, delete=True, print=True, superadmin=True)
PROD = dict(id=7, sku="1221101048", nombre=LARGO, estado="Confirmado", codigo="1307", unidades_por_venta=1, created_by="Fran", updated_by="Daniel",
            created_at=datetime.datetime(2026, 4, 25, 13, 30), updated_at=datetime.datetime(2026, 11, 3, 9, 41))


def _bultos(n):
    return [dict(bulto_num=i, largo=181.0 + i, ancho=61.5, alto=30.25, peso=48.75 + i, peso_vol=70.0 + i,
                 largo_cm=181.0 + i, ancho_cm=61.5, alto_cm=30.25, kg=48.75 + i) for i in range(1, n + 1)]


class TestRenderDeVerdad(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.env = _env()

    def _r(self, tpl, **ctx):
        base = dict(permissions=PERMS, request=_Req(), product=PROD)
        base.update(ctx)
        return self.env.get_template(tpl).render(**base)

    def test_ficha_del_producto_renderiza_tarjetas_con_nombre_completo(self):
        b = _bultos(5)
        h = self._r("product_detail.html", bultos=b, photos=[], peso_total=250.0, pv_total=350.0, total_bultos=5, can_add_photo=True)
        self.assertIn('class="eti-scope"', h)
        self.assertIn("eti-tcard", h)
        self.assertEqual(h.count('class="text-center tc-num"'), 5)
        self.assertEqual(h.count('tc-dim" data-lbl="Largo"'), 5)
        self.assertIn(LARGO, h)
        self.assertNotIn("text-truncate", h)

    def test_configurar_impresion_renderiza_el_paso_de_copias_con_cinco_bultos(self):
        h = self._r("print_setup.html", bultos=_bultos(5), total_bultos=5, peso_total=250.0, only_bulto=None)
        self.assertEqual(h.count("px-2 py-0 qty-dec"), 5)
        self.assertEqual(h.count("px-2 py-0 qty-inc"), 5)
        self.assertEqual(h.count('tc-act" data-lbl="Copias"'), 5)
        self.assertIn("Una copia más del bulto 5", h)
        self.assertIn("ps-acciones", h)
        self.assertIn(LARGO, h)

    def test_formulario_editar_y_nuevo_renderizan_sin_romperse(self):
        h1 = self._r("product_form.html", errors=[], bultos=_bultos(3), fd={}, max_b=10, photos=[], auto_codigo="1308")
        h2 = self._r("product_form.html", product=None, errors=["El SKU ya existe"], bultos=[], fd={}, max_b=10, photos=[], auto_codigo="1308")
        for h in (h1, h2):
            self.assertIn('id="bultosTable"', h)
            self.assertIn("bf-tabla", h)
            self.assertIn("bf-acciones", h)
        self.assertIn("productSearchInput", h2)
        self.assertNotIn("productSearchInput", h1)

    def test_vista_de_etiquetas_renderiza_los_dos_formatos(self):
        for fm in (dict(key="150x100", w="150mm", h="100mm", label="100 x 150 mm"), dict(key="100x50", w="100mm", h="50mm", label="100 x 50 mm")):
            b = _bultos(2)
            h = self._r("print_labels.html", bultos=b, total_bultos=2, fecha="05/11/2026", qty_per_bulto={1: 1, 2: 2}, fmt=None, label_format=fm, logo_url=None)
            self.assertEqual(h.count('data-label'), 3)  # 1 + 2 copias
            self.assertIn("function fitPreview()", h)
            self.assertIn("@media print", h)

    def test_carga_masiva_renderiza(self):
        h = self._r("etiquetas_masivo.html")
        self.assertIn('<div class="em-page">', h)
        self.assertEqual(h.count("table table-sm mb-0 em-tabla"), 3)
        self.assertIn('id="tblImpr"', h)
        self.assertIn('id="tblMed"', h)
        self.assertIn('id="tblProd"', h)

    def test_catalogo_en_el_telefono_trae_el_selector_con_lo_elegido(self):
        prods = [dict(sku="1237101029", nombre="Cinturón XL", estado="Confirmado", codigo="1", app_product_id=1, created_by="a", updated_by="b",
                      created_at=datetime.datetime(2026, 4, 25), updated_at=datetime.datetime(2026, 4, 26), total_bultos=1, peso_total=1.0,
                      pv_total=0.3, total_fotos=1)]
        ctx = dict(products=prods, q="cinturon", foto0=1, foto1=0, foto2=0, total=1, total_global=1, foto_filter="con", orden="peso", orden_dir="desc",
                   page=1, page_size=50, total_pages=1, request=_Req({"ver": "etiquetas"}))
        h = self._r("index.html", **ctx)
        i = h.index('<form method="get" class="eti-orden"')
        f = h[i:h.index("</form>", i)]
        self.assertIn('<option value="peso"        selected>Peso (kg)</option>', f)
        self.assertIn('name="dir" id="etiOrdenDir" value="desc"', f)
        self.assertIn('name="q" value="cinturon"', f)
        self.assertIn('name="fotos" value="con"', f)
        self.assertIn('name="ver" value="etiquetas"', f)
        self.assertIn('name="size" value="50"', f)
        self.assertIn("De mayor a menor", f)
        # sin orden elegido el servidor ordena por nombre: eso es lo que se ve seleccionado
        ctx.update(orden="", orden_dir="asc", q="", foto_filter="")
        h2 = self._r("index.html", **ctx)
        f2 = h2[h2.index('<form method="get" class="eti-orden"'):]
        self.assertIn('<option value="nombre"      selected>Nombre</option>', f2[:f2.index("</form>")])


if __name__ == "__main__":
    unittest.main()

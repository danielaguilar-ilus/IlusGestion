# -*- coding: utf-8 -*-
"""📱 5-nov-2026 — Fase 1.2 (REGLA #61): la lista de Repuestos (pestaña Bodega) en el celular.

Medido en producción a 390 px con los 100 repuestos de la primera página: la tabla (11 columnas, 1313 px de ancho real) se deslizaba 964 px por dentro y en
un celular se veía el SKU y la descripción. Además el paginador («100 por página · Anterior · Página 1 de N · Siguiente», 418 px en una pantalla de
368) dejaba «Siguiente» CORTADO fuera de la pantalla: lo tapaba el `overflow:hidden` de la tabla.

Bajo 768 px cada repuesto pasa a UNA tarjeta con las MISMAS celdas (REGLA #4.2: no se quita nada) y cada dato con su rótulo. El escritorio no cambia.
REGLAS #19 y #26: la celda de Costo sigue saliendo SOLO si `not oculta_proveedores` (un técnico nunca ve costos ni proveedores; el dato ni siquiera llega al
navegador): estas pruebas lo vigilan.

DESPUÉS (CSS inyectado en producción a 390 px con los 100 repuestos reales, antes de escribirlo): sin desborde lateral (scrollWidth = clientWidth), tarjetas de
335 a 358 px, paginador que envuelve; revisado con captura.
Correr:  python -m pytest tests/test_repuestos_celular_5nov.py -q -p no:warnings
"""
import os
import re
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _leer(*partes):
    with open(os.path.join(RAIZ, *partes), encoding="utf-8") as fh:
        return fh.read().replace("\r\n", "\n")


PANE = _leer("templates", "mantenciones", "_repuestos_bodega_pane.html")
PAG = _leer("templates", "mantenciones", "_repuestos_bodega_pag.html")

CELDAS = ("rb-c-chk", "rb-c-sku", "rb-c-desc", "rb-c-cant", "rb-c-stock", "rb-c-ubi", "rb-c-costo", "rb-c-marca", "rb-c-mod", "rb-c-aso", "rb-c-acc")


def _bloque_celular():
    """Cuerpo del @media (max-width:767.98px) que sigue al comentario «REPUESTOS EN EL CELULAR»."""
    i = PANE.index("REPUESTOS EN EL CELULAR (2026-11-05, REGLA #61)")
    m = re.compile(r"@media\s*\(\s*max-width\s*:\s*767\.98px\s*\)\s*\{").search(PANE, i)
    j, prof = m.end(), 1
    while j < len(PANE) and prof:
        prof += {"{": 1, "}": -1}.get(PANE[j], 0)
        j += 1
    return PANE[m.end():j - 1]


BLOQUE = _bloque_celular()


def _filas():
    i = PANE.index("{% for r in bod_repuestos %}")
    return PANE[i:PANE.index("</tbody>", i)]   # hasta el cierre de la tabla: adentro hay otros {% for %} (los chips de modelos)


class TestCadaCeldaLlevaSuClase(unittest.TestCase):
    def test_las_celdas_en_el_orden_de_siempre(self):
        f = _filas()
        clases = re.findall(r"<td[^>]*\bclass=\"([^\"]*)\"", f)
        marcadas = [c for tok in clases for c in tok.split() if c.startswith("rb-c-")]
        self.assertEqual(list(CELDAS), marcadas)

    def test_el_costo_sigue_oculto_para_el_tecnico(self):
        """REGLAS #19 y #26: la celda y su encabezado solo salen si `not oculta_proveedores`."""
        self.assertIn('{% if not oculta_proveedores %}<td class="rb-c-costo">', _filas())
        self.assertIn("{% if not oculta_proveedores %}<th>Costo</th>{% endif %}", PANE)
        # el CSS no puede hacer aparecer una celda que el servidor no mandó: no hay contenido generado con costos ni proveedores
        self.assertNotIn("proveedor", BLOQUE.lower())

    def test_el_casillero_el_paso_a_editar_y_las_acciones_no_se_tocan(self):
        f = _filas()
        self.assertIn('class="rb-row-check"', f)
        self.assertIn("onclick='rbAbrirEditar(", f)
        for accion in ("rbImprimirUno(", "rbmAbrir(", "rbEliminar("):
            self.assertIn(accion, f)
        self.assertIn('class="text-nowrap rb-c-acc" onclick="event.stopPropagation()"', f)


class TestTarjetaEnCelular(unittest.TestCase):
    def test_todo_va_bajo_la_tabla_de_repuestos_o_su_paginador(self):
        sin_coment = re.sub(r"/\*.*?\*/", "", BLOQUE, flags=re.S)
        for m in re.finditer(r"([^{}]+)\{[^{}]*\}", sin_coment):
            for sel in m.group(1).split(","):
                sel = " ".join(sel.split())
                self.assertTrue(sel.startswith((".rb-table", ".rb-pag")), "selector fuera de la lista de repuestos: %r" % sel)

    def test_cada_celda_tiene_su_lugar_y_son_todos_distintos(self):
        ordenes = {}
        for celda in CELDAS:
            m = re.search(r"td\.%s\{order:(\d+);" % celda, BLOQUE)
            self.assertIsNotNone(m, celda)
            ordenes[celda] = int(m.group(1))
        self.assertEqual(list(range(1, 12)), sorted(ordenes.values()), "los 11 lugares de la tarjeta, sin repetirse")
        self.assertEqual(1, ordenes["rb-c-chk"])
        self.assertEqual(2, ordenes["rb-c-sku"])
        self.assertEqual(3, ordenes["rb-c-acc"], "las acciones van arriba, junto al SKU: no ocupan una fila entera")

    def test_cada_dato_lleva_su_rotulo(self):
        for celda, rotulo in (("cant", "Cantidad"), ("stock", "Stock"), ("ubi", "Ubicación"), ("costo", "Costo"), ("marca", "Marca"),
                              ("mod", "Modelos compatibles"), ("aso", "Asociado a")):
            self.assertIn('td.rb-c-%s::before{content:"%s"}' % (celda, rotulo), BLOQUE)

    def test_solo_se_oculta_el_encabezado_que_no_ordena(self):
        for m in re.finditer(r"([^{}]+)\{([^{}]*)\}", re.sub(r"/\*.*?\*/", "", BLOQUE, flags=re.S)):
            sel, props = " ".join(m.group(1).split()), m.group(2)
            if "display:none" in props.replace(" ", ""):
                self.assertEqual(".rb-table thead", sel, "se oculta algo más que el encabezado (cuyos rótulos pasan a cada celda): %r" % sel)

    def test_la_lista_deja_de_ser_un_cuadro_con_scroll_y_las_filas_son_tarjetas(self):
        self.assertIn(".rb-table-scroll{overflow:visible}", BLOQUE)
        self.assertIn(".rb-table{display:block;width:100%}", BLOQUE)
        self.assertRegex(BLOQUE, r"\.rb-table tbody tr\{display:flex;flex-wrap:wrap;")
        self.assertIn(".rb-table-wrap:has(> .rb-table-scroll){background:transparent;border:0;border-radius:0;overflow:visible}", BLOQUE)

    def test_modelos_y_clientes_van_completos_y_letra_de_12px(self):
        """REGLA #15: con nowrap un modelo largo ensanchaba la tarjeta (medido: 75 px de desborde del documento)."""
        self.assertRegex(BLOQUE, r"\.rb-table \.rb-chip\{white-space:normal;overflow-wrap:anywhere;max-width:100%\}")
        self.assertRegex(BLOQUE, r"\.rb-table \.rb-chip,\.rb-table \.rb-bajo-min-badge\{font-size:\.75rem\}")
        self.assertIn(".rb-table td.rb-c-stock{font-size:.75rem !important;white-space:normal}", BLOQUE)   # el tamaño de esa celda viene en línea

    def test_los_botones_son_de_44px(self):
        self.assertIn(".rb-table td.rb-c-acc .btn{min-height:44px;padding:0 16px}", BLOQUE)
        self.assertIn(".rb-pag .btn,.rb-pag .form-select{min-height:44px}", BLOQUE)
        self.assertRegex(BLOQUE, r"rb-row-check\{width:22px;height:22px;")


class TestElPaginador(unittest.TestCase):
    def test_el_grupo_de_controles_envuelve_y_lleva_su_clase(self):
        self.assertIn('<div class="d-flex flex-wrap align-items-center gap-2 rb-pag">', PAG)

    def test_el_paginador_no_pierde_ningun_control(self):
        for texto in ("100 por pagina", "200 por pagina", "500 por pagina", "Anterior", "Siguiente", "Pagina {{ bod_page }} de {{ bod_total_pages }}"):
            self.assertIn(texto, PAG)


class TestElEscritorioNoCambia(unittest.TestCase):
    def test_las_reglas_de_escritorio_siguen_igual(self):
        self.assertIn(".rb-table{width:100%;border-collapse:collapse;font-size:.84rem}", PANE)
        self.assertIn(".rb-table-wrap{background:#fff;border:1px solid #e5e7eb;border-radius:12px;overflow:hidden}", PANE)
        self.assertIn("max-height:calc(100dvh - 430px);", PANE)
        self.assertIn(".rb-table thead th{position:sticky;top:0;z-index:5;box-shadow:0 2px 4px rgba(0,0,0,.08)}", PANE)

    def test_las_clases_nuevas_no_tienen_estilo_fuera_del_celular(self):
        fuera = re.sub(r"/\*.*?\*/", "", PANE.replace(BLOQUE, ""), flags=re.S)
        solo_css = re.sub(r"<(?!/?style)[^>]*>", "", fuera)   # fuera de las etiquetas HTML de las filas
        hojas = "".join(re.findall(r"<style[^>]*>(.*?)</style>", PANE.replace(BLOQUE, ""), flags=re.S))
        self.assertNotIn("rb-c-", re.sub(r"/\*.*?\*/", "", hojas, flags=re.S), "las clases rb-c-* solo se estilizan dentro del @media")
        self.assertTrue(solo_css)


if __name__ == "__main__":
    unittest.main()

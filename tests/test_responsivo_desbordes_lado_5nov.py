# -*- coding: utf-8 -*-
"""📱 5-nov-2026 — Fase 1.2 (REGLA #61): tres pantallas cuya página ENTERA se corría de lado en el celular.

Medido en producción, con datos reales, a 390 px de ancho:
  · Técnicos externos (/servicio-tecnico/tecnicos-externos): 294 px. La columna `1fr` crecía hasta lo que mide el nombre sin cortar (656 px).
  · Monitor de Transporte (/transporte/): 84 px. Los filtros `1fr 1fr` crecían hasta el tamaño propio del <select> y de la fecha (≈180 px cada uno).
  · Marketing (/ajustes/marketing): 87 px. Las 3 pestañas con su nombre completo no caben en una fila.

La causa de las dos primeras es la misma, y es la que ya costó la tabla de Etiquetas: `1fr` a secas es `minmax(auto,1fr)`, así que la columna no baja
del tamaño de su contenido. Hay que escribir `minmax(0,1fr)`. (Y `min-w-0` NO existe en Bootstrap 5.)

DESPUÉS (CSS candidato inyectado en cada pantalla de producción a 390 px antes de escribirlo aquí): el desborde queda en 0 en las tres.
Aquí se prueba (texto, sin navegador) que el arreglo está y que no vuelve el patrón que lo causó. La medición real se repite en producción tras el
despliegue (a 390, 820 y 1280) y queda en REGLA #61.
Correr:  python -m pytest tests/test_responsivo_desbordes_lado_5nov.py -q -p no:warnings
"""
import os
import re
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _leer(*partes):
    with open(os.path.join(RAIZ, *partes), encoding="utf-8") as fh:
        return fh.read().replace("\r\n", "\n")


TX = _leer("templates", "mantenciones", "tecnicos_externos_list.html")
MON = _leer("static", "transporte_monitor.css")
MK = _leer("templates", "admin", "marketing.html")


def _bloques_media_max(css):
    """[(condicion, cuerpo)] de cada `@media (max-width:…){ … }` (con llaves anidadas bien contadas)."""
    out = []
    for m in re.finditer(r"@media\s*\(\s*max-width\s*:\s*([\d.]+)px\s*\)\s*\{", css):
        i, prof = m.end(), 1
        while i < len(css) and prof:
            prof += {"{": 1, "}": -1}.get(css[i], 0)
            i += 1
        out.append((float(m.group(1)), css[m.end():i - 1]))
    return out


def _fr_sueltos(cuerpo):
    """Declaraciones `grid-template-columns` de un bloque que usan `1fr` SIN envolver en minmax(): el patrón que ensancha la columna."""
    malas = []
    for m in re.finditer(r"grid-template-columns\s*:\s*([^;}]+)", cuerpo):
        valor = m.group(1)
        sin_minmax = re.sub(r"minmax\([^()]*(?:\([^()]*\)[^()]*)*\)", "", valor)   # quita minmax(…) (admite un min() adentro)
        if re.search(r"(?<![\w.])\d*\.?\d*fr\b", sin_minmax):
            malas.append(valor.strip())
    return malas


class TestTecnicosExternos(unittest.TestCase):
    def test_la_grilla_del_celular_no_crece_con_el_nombre(self):
        bloques = [c for ancho, c in _bloques_media_max(TX) if ancho >= 600]
        self.assertTrue(bloques, "debe existir el bloque de celular (max-width:640px)")
        celu = " ".join(bloques)
        self.assertIn(".tx-grid{grid-template-columns:minmax(0,1fr)", celu)
        self.assertEqual([], _fr_sueltos(celu), "un `1fr` sin minmax(0,…) vuelve a correr la página de lado")

    def test_la_grilla_de_escritorio_no_obliga_a_310_px_en_una_pantalla_mas_angosta(self):
        self.assertIn("repeat(auto-fill,minmax(min(310px,100%),1fr))", TX)

    def test_la_tarjeta_puede_encogerse(self):
        m = re.search(r"\.tx-card\{([^}]*)\}", TX)
        self.assertIsNotNone(m)
        self.assertIn("min-width:0", m.group(1).replace(" ", ""))

    def test_en_celular_el_nombre_y_el_giro_van_completos(self):
        """REGLA #15: nunca cortados con «…». Con nowrap + ellipsis el nombre era lo que ensanchaba la columna; ahora se parte en líneas."""
        celu = " ".join(c for ancho, c in _bloques_media_max(TX) if ancho >= 600)
        self.assertRegex(celu, r"\.tx-razon,\.tx-giro\{[^}]*white-space:normal")
        self.assertRegex(celu, r"\.tx-razon,\.tx-giro\{[^}]*overflow-wrap:anywhere")

    def test_la_etiqueta_gris_de_los_indicadores_tiene_contraste(self):
        m = re.search(r"\.kpi-lbl\{([^}]*)\}", TX)
        self.assertIsNotNone(m)
        self.assertNotIn("#9ca3af", m.group(1).lower(), "#9ca3af sobre blanco da 2,5:1; el gris neutro del proyecto (#6b7280) da 4,8:1")


class TestMonitorDeTransporte(unittest.TestCase):
    def test_los_filtros_no_crecen_con_el_select_ni_la_fecha(self):
        # solo la fila de filtros: el resto del archivo tiene repeat(N, 1fr) de tarjetas de indicadores, de texto corto, que no se desbordan
        reglas = re.findall(r"\.tr-filters-row\s*\{([^}]*)\}", " ".join(c for _, c in _bloques_media_max(MON)))
        self.assertTrue(reglas)
        self.assertEqual([], _fr_sueltos(" ".join(reglas)), "los filtros del monitor usaban `1fr 1fr` (84 px de desborde en el celular)")
        self.assertIn(".tr-filters-row { grid-template-columns:minmax(0,1fr) minmax(0,1fr); }", MON)

    def test_en_un_celular_angosto_cada_filtro_ocupa_todo_el_ancho(self):
        bloques = {ancho: c for ancho, c in _bloques_media_max(MON)}
        self.assertIn(480.0, bloques)
        self.assertIn("grid-template-columns:minmax(0,1fr)", bloques[480.0])

    def test_el_select_y_la_fecha_pueden_encogerse_y_son_tocables(self):
        angosto = " ".join(c for ancho, c in _bloques_media_max(MON) if ancho == 1279.98)
        m = re.search(r"\.tr-filter-select, \.tr-filter-date \{([^}]*)\}", angosto)
        self.assertIsNotNone(m)
        self.assertIn("min-width:0", m.group(1))
        tactil = " ".join(c for ancho, c in _bloques_media_max(MON) if ancho == 980)
        m = re.search(r"\.tr-filter-select, \.tr-filter-date, \.tr-filter-search input \{([^}]*)\}", tactil)
        self.assertIsNotNone(m)
        self.assertIn("min-height:44px", m.group(1), "REGLA #3: objetivos táctiles de 44 px (solo donde hay dedo, no con mouse)")

    def test_la_fila_de_filtros_de_seis_columnas_solo_con_1280_o_mas(self):
        """Medido en producción: la fila de 6 columnas necesita ≈1260 px; a 1000, 1100, 1180 y 1240 px la PÁGINA se corría 259, 159, 79 y 19 px."""
        bloques = {ancho: c for ancho, c in _bloques_media_max(MON)}
        self.assertIn(1279.98, bloques)
        self.assertIn(".tr-filters-row { grid-template-columns:minmax(0,1fr) minmax(0,1fr); }", bloques[1279.98])
        self.assertIn(".tr-filter-search { grid-column:1 / -1; }", bloques[1279.98])
        self.assertIn(".tr-filter-actions { grid-column:1 / -1; }", bloques[1279.98])
        self.assertNotIn(980.0, [a for a, c in _bloques_media_max(MON) if ".tr-filters-row" in c], "el cambio de columnas ya no vive en 980 px")

    def test_el_control_de_vista_no_empuja_la_pagina(self):
        """Entre 641 y ~1100 px el grupo «Pendientes / En camino / Entregados / Todos / Problema» (976 px) corría la página 391 px a 820 px."""
        m = re.search(r"\.tr-segment-group \{([^}]*)\}", MON)
        self.assertIsNotNone(m)
        self.assertIn("max-width:100%", m.group(1))
        self.assertIn("overflow-x:auto", m.group(1))
        b = re.search(r"\.tr-segment-btn \{([^}]*)\}", MON)
        self.assertIn("flex:0 0 auto", b.group(1))
        self.assertIn("white-space:nowrap", b.group(1))

    def test_el_escritorio_no_cambia(self):
        """La grilla de escritorio (6 columnas) sigue igual: todo lo nuevo va dentro de max-width."""
        self.assertIn("grid-template-columns: minmax(180px, 2fr) 1fr 1fr 130px 130px auto;", MON)


class TestMarketing(unittest.TestCase):
    def test_en_celular_las_pestanas_pasan_a_lista_vertical_con_su_nombre_completo(self):
        celu = " ".join(c for ancho, c in _bloques_media_max(MK) if ancho == 640)
        self.assertIn(".mk-tabs{flex-direction:column", celu)
        self.assertRegex(celu, r"\.mk-tab\{[^}]*width:100%")
        self.assertRegex(celu, r"\.mk-tab\{[^}]*min-height:48px", "REGLA #3: objetivos táctiles de 44 px o más")

    def test_las_cifras_de_las_pestanas_y_las_etiquetas_de_estado_miden_12px(self):
        celu = " ".join(c for ancho, c in _bloques_media_max(MK) if ancho == 640)
        self.assertRegex(celu, r"\.mk-tab \.badge\{font-size:\.75rem !important")   # el badge trae font-size en línea (.62rem): hace falta !important
        self.assertRegex(celu, r"\.li-status-on,\.li-status-off\{font-size:\.75rem")

    def test_los_grises_de_texto_tienen_contraste(self):
        self.assertIn('id="bkCounter" style="color:#6b7280', MK)
        self.assertIn("counter.style.color = sel.length ? '#166534' : '#6b7280'", MK)
        self.assertNotIn("#9ca3af' : '#9ca3af", MK)
        m = re.search(r"^\.li-status-off\{([^}]*)\}", MK, re.M)   # la regla base (la del celular está dentro de un @media, con sangría)
        self.assertIsNotNone(m)
        self.assertIn("color:#4b5563", m.group(1).replace(" ", ""), "#6b7280 sobre #f3f4f6 da 4,4:1; #4b5563 da 7,6:1")

    def test_el_texto_de_ayuda_ya_no_baja_de_12px(self):
        self.assertNotIn('style="font-size:.74rem"', MK)


class TestElPatronNoVuelve(unittest.TestCase):
    def test_el_ayudante_detecta_el_patron_malo(self):
        self.assertEqual(["1fr"], _fr_sueltos(".a{grid-template-columns:1fr;}"))
        self.assertEqual(["1fr 1fr"], _fr_sueltos(".a{grid-template-columns:1fr 1fr;}"))
        self.assertEqual([], _fr_sueltos(".a{grid-template-columns:minmax(0,1fr) minmax(0,1fr);}"))
        self.assertEqual([], _fr_sueltos(".a{grid-template-columns:repeat(auto-fill,minmax(min(310px,100%),1fr));}"), "repeat(auto-fill,minmax(…)) está bien")
        self.assertEqual([], _fr_sueltos(".a{grid-template-columns:130px 130px auto;}"))


if __name__ == "__main__":
    unittest.main()

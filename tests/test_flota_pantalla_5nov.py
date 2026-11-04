# -*- coding: utf-8 -*-
"""🚚 FLOTA · pantalla del 05-nov-2026 (Daniel): «sácale ese mensaje» (el aviso «Tú administras la flota»), «las pestañas las puedes mejorar»,
«la parte de revisión técnica, permiso, SOAP y seguro debería estar arriba de ver ficha, editar», «una separación más pronunciada de solicitud
entre solicitud» en el celular, y «es información delicada que me gustaría que solamente los que administramos podamos verla».

Correr:  python -m pytest tests/test_flota_pantalla_5nov.py -q -p no:warnings
"""
import os
import re
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from test_flota_solicitudes import _leer  # noqa: E402

HTML = _leer("templates", "flota", "index.html")
CSS = _leer("static", "flota.css")
JS_VEH = _leer("static", "flota.js")
JS_USO = _leer("static", "flota_uso.js")


class TestSinElAvisoDeQuienAdministra(unittest.TestCase):
    def test_el_mensaje_ya_no_esta_en_la_plantilla(self):
        self.assertNotIn("Tú administras la flota", HTML)
        self.assertNotIn('class="fl-preview"', HTML)


class TestPestanas(unittest.TestCase):
    def test_las_tres_pestanas_traen_su_contador(self):
        for t in ("solicitudes", "vehiculos", "choferes"):
            self.assertIn(f'id="flTabN-{t}"', HTML, t)
            self.assertIn(f'data-tab="{t}"', HTML, t)
        self.assertIn('class="fl-tab-n fl-tab-n--aviso" id="flTabN-solicitudes"', HTML)          # lo por confirmar va en ámbar

    def test_los_contadores_se_llenan_al_cargar_cada_lista(self):
        self.assertIn("function ponerNTab(", JS_USO)
        self.assertIn("ponerNTab('solicitudes', (r.por_confirmar || 0) + (r.por_cerrar || 0), 'por confirmar')", JS_USO)
        self.assertIn("ponerNTab('vehiculos',", JS_USO)
        self.assertIn("ponerNTab('choferes', r.habilitados, 'choferes habilitados')", JS_USO)

    def test_un_contador_vacio_no_se_ve_y_los_otros_dicen_que_cuentan(self):
        i = JS_USO.index("function ponerNTab(")
        cuerpo = JS_USO[i:i + 520]
        self.assertIn("el.hidden = !n", cuerpo)
        self.assertIn("aria-label", cuerpo)

    def test_la_pestana_activa_es_negra_y_es_tactil(self):
        self.assertRegex(CSS, r"\.fl-tab\.on\{background:#0a0a0a;color:#fff")
        self.assertRegex(CSS, r"\.fl-tab\{[^}]*min-height:48px")                   # REGLA #3: blanco táctil


class TestTarjetaDeVehiculo(unittest.TestCase):
    def test_los_cuatro_documentos_van_arriba_de_ver_ficha_y_editar(self):
        i = JS_VEH.index("function filaVeh(v)")
        fila = JS_VEH[i:JS_VEH.index("function pintarLista()", i)]
        self.assertLess(fila.index("fl-vtar-docs"), fila.index("fl-vtar-pie"))
        self.assertLess(fila.index("fl-vtar-info"), fila.index("fl-vtar-docs"))        # y dentro de la columna de datos, no debajo de la tarjeta
        self.assertEqual(fila.count("fl-vtar-docs"), 1)
        self.assertEqual(fila.count("data-acc=\"ficha\""), 1)                          # los botones siguen ahí

    def test_los_documentos_de_la_tarjeta_se_acomodan_en_el_celular(self):
        self.assertIn(".fl-vtar-info .fl-vtar-docs{", CSS)
        self.assertRegex(CSS, r"\.fl-vtar-docs\{grid-template-columns:repeat\(2,minmax\(0,1fr\)\)\}")


class TestSolicitudesEnElCelular(unittest.TestCase):
    def test_cada_solicitud_es_su_propia_tarjeta_separada_de_la_siguiente(self):
        self.assertIn("fl-tabla-wrap fl-tabla-wrap--sol", HTML)
        i = CSS.index("Solicitudes en el celular (05-nov-2026")
        bloque = CSS[i:]
        self.assertIn("@media (max-width: 860px)", bloque)
        self.assertRegex(bloque, r"\.fl-tabla--sol tbody\{display:flex;flex-direction:column;gap:20px\}")
        self.assertRegex(bloque, r"border-radius:16px")
        self.assertIn("var(--fl-sem,transparent)", bloque)                              # conserva su franja de semáforo

    def test_el_contenedor_deja_de_ser_una_sola_caja_en_el_celular(self):
        i = CSS.index("Solicitudes en el celular (05-nov-2026")
        self.assertRegex(CSS[i:], r"\.fl-tabla-wrap--sol\{background:transparent;border:0")


class TestDocumentosSoloParaQuienAdministra(unittest.TestCase):
    def test_el_texto_del_permiso_dice_que_ya_no_abre_nada(self):
        app = _leer("app.py")
        self.assertIn("Sin efecto desde el 05-nov: los PDF, el RUT y la dirección de los choferes los ve solo quien tiene «Administrar»", app)
        self.assertNotIn('"label": "Ver los PDF de los documentos y el RUT y la dirección de los choferes"', app)

    def test_la_pantalla_ya_no_promete_el_permiso_de_documentos(self):
        self.assertNotIn("Solo lo ven quienes tienen el permiso de documentos", JS_VEH)
        self.assertIn("Solo lo ve quien administra la flota", JS_VEH)

    def test_la_regla_unica_lo_deja_en_admin(self):
        reglas = _leer("flota_reglas.py")
        self.assertRegex(reglas, r'"documentos": admin,')
        self.assertNotIn('bool(p.get("flota_documentos"))', reglas)


if __name__ == "__main__":
    unittest.main()

# -*- coding: utf-8 -*-
"""2026-10-23 · Daniel (Repuestos → Bodega): «un filtro de proveedores… y que los creen los administradores, no los técnicos: hay varios
repetidos porque se crean con mayúscula o con errores; deben llamarse en orden por proveedores».

  · Filtro «Proveedor» en la Bodega (pantalla, paginado y Excel), en orden alfabético sin distinguir mayúsculas ni tildes. Un técnico nunca
    lo ve ni lo aplica (REGLA #19).
  · Alta de proveedores solo para superadmin / admin / supervisor; y no se deja crear (ni renombrar a) un nombre que se escriba casi igual.
"""
import os
import re
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _leer(*p):
    with open(os.path.join(RAIZ, *p), encoding="utf-8") as fh:
        return fh.read()


APP = _leer("app.py")
PANE = _leer("templates", "mantenciones", "_repuestos_bodega_pane.html")
PAG = _leer("templates", "mantenciones", "_repuestos_bodega_pag.html")
HUB = _leer("templates", "clientes_hub", "repuestos.html")


def _clave():
    i = APP.index("def _prov_clave_nombre(nombre):")
    j = APP.index("\n\n\ndef ", i)
    ns = {"re": re}
    exec(APP[i:j], ns)
    return ns["_prov_clave_nombre"]


class TestClaveDeNombre(unittest.TestCase):
    def test_mayusculas_tildes_signos_y_espacios_son_el_mismo_proveedor(self):
        k = _clave()
        for variante in ("Drax", "DRAX", " drax ", "Drax.", "dRaX  "):
            self.assertEqual(k(variante), "drax", variante)
        self.assertEqual(k("Distribuidora Peña  y Cía."), k("DISTRIBUIDORA PENA Y CIA"))

    def test_proveedores_distintos_no_se_mezclan(self):
        k = _clave()
        self.assertNotEqual(k("Drax"), k("Drax Chile"))
        self.assertEqual(k(""), "")
        self.assertEqual(k(None), "")

    def test_el_orden_alfabetico_ignora_mayusculas_y_tildes(self):
        k = _clave()
        nombres = ["zeta", "Álamo", "beta", "Alfa", "ÁLAMO sur"]
        self.assertEqual(sorted(nombres, key=k), ["Álamo", "ÁLAMO sur", "Alfa", "beta", "zeta"])


class TestFiltroEnLaBodega(unittest.TestCase):
    def test_pantalla_y_excel_aplican_el_mismo_filtro(self):
        self.assertEqual(APP.count('proveedor_id = "" if _oculta_proveedores() else (request.args.get("proveedor_id") or "").strip()'), 2)
        self.assertEqual(APP.count('where.append("(r.proveedor_id IS NULL OR r.proveedor_id=0)")'), 2)
        self.assertEqual(APP.count('where.append("r.proveedor_id=%s"); params.append(int(proveedor_id))'), 2)

    def test_el_selector_solo_se_dibuja_para_gestion_y_conserva_el_filtro_al_paginar_y_exportar(self):
        i = PANE.index('<select name="proveedor_id"')
        self.assertIn("{% if not oculta_proveedores %}", PANE[i - 400:i])
        self.assertIn("Todos los proveedores", PANE)
        self.assertIn("Sin proveedor asignado", PANE)
        self.assertIn("'&proveedor_id=' ~ bod_proveedor_id", PANE)      # Excel
        self.assertIn("'&proveedor_id=' ~ bod_proveedor_id", PAG)       # paginado
        self.assertIn("p.get('proveedor_id')", HUB)                     # al filtrar abre la pestaña Bodega
        self.assertIn("or bod_proveedor_id or", PANE)                   # «Limpiar» aparece

    def test_un_tecnico_no_recibe_la_lista(self):
        self.assertIn('"bod_proveedores": [] if _oculta_proveedores() else _repstock_proveedores_ordenados(),', APP)


class TestAltaSoloAdministradores(unittest.TestCase):
    def test_crear_exige_admin_y_rechaza_nombres_parecidos(self):
        i = APP.index("def mant_proveedor_repuesto_crear():")
        f = APP[i:i + 2600]
        self.assertIn("if not _prov_puede_crear():", f)
        self.assertIn('"error_codigo": "SOLO_ADMIN"}), 403', f)
        self.assertIn("_prov_nombre_parecido(nombre)", f)
        self.assertLess(f.index("_prov_puede_crear"), f.index("get_json"))

    def test_renombrar_tampoco_deja_un_nombre_parecido(self):
        i = APP.index("def mant_proveedor_repuesto_update(pid):")
        self.assertIn("_prov_nombre_parecido(nombre, excluir_id=pid)", APP[i:i + 2600])

    def test_quienes_pueden_crear(self):
        i = APP.index("def _prov_puede_crear():")
        f = APP[i:i + 700]
        self.assertIn('("superadmin", "admin", "supervisor")', f)
        self.assertIn("_es_rol_tecnico()", f)

    def test_el_boton_nuevo_proveedor_solo_si_puede_crear(self):
        self.assertIn("RB_PUEDE_CREAR_PROV", PANE)
        self.assertIn("!existe && !RB_PROV_SOLO_NOMBRE && RB_PUEDE_CREAR_PROV", PANE)
        self.assertIn('"bod_puede_crear_proveedor": _prov_puede_crear(),', APP)


if __name__ == "__main__":
    unittest.main()

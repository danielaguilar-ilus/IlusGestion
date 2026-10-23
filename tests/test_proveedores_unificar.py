# -*- coding: utf-8 -*-
"""2026-10-23 · Daniel: «Sí, unifica los proveedores duplicados». Pantalla «Unificar duplicados» (solo superadmin) en Proveedores.

Detección (pura, con casos reales de «mayúscula o errores»), reglas de la ruta de unificación y la pantalla.
"""
import os
import re
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _leer(*p):
    with open(os.path.join(RAIZ, *p), encoding="utf-8") as fh:
        return fh.read()


APP = _leer("app.py")
HTML = _leer("templates", "mantenciones", "proveedores.html")


def _puras():
    ns = {"re": re}
    i = APP.index("def _prov_clave_nombre(nombre):")
    exec(APP[i:APP.index("\n\n\ndef ", i)], ns)
    j = APP.index("def _prov_grupos_duplicados(filas):")
    exec(APP[j:APP.index("\n\n\ndef ", j)], ns)
    return ns


def _f(i, nombre):
    return {"id": i, "nombre": nombre}


class TestDeteccion(unittest.TestCase):
    def test_identicos_por_mayuscula_tilde_y_signos(self):
        g = _puras()["_prov_grupos_duplicados"]
        grupos = g([_f(1, "Drax"), _f(2, "DRAX"), _f(3, "drax."), _f(4, "Booybuilder")])
        self.assertEqual(len(grupos), 1)
        self.assertEqual(sorted(x["id"] for x in grupos[0]), [1, 2, 3])

    def test_parecidos_por_error_de_tipeo(self):
        g = _puras()["_prov_grupos_duplicados"]
        grupos = g([_f(1, "Distribuidora Fitness"), _f(2, "Distribuidora Fitnes"), _f(3, "Importadora Sur")])
        self.assertEqual(len(grupos), 1)
        self.assertEqual(sorted(x["id"] for x in grupos[0]), [1, 2])

    def test_distintos_no_se_mezclan(self):
        g = _puras()["_prov_grupos_duplicados"]
        self.assertEqual(g([_f(1, "Drax"), _f(2, "Drax Chile"), _f(3, "Gymleco"), _f(4, "Gymtech")]), [])
        self.assertEqual(g([]), [])
        self.assertEqual(g([_f(1, ""), _f(2, "  ")]), [])


class TestRutaUnificar(unittest.TestCase):
    def setUp(self):
        i = APP.index('@app.route("/mantenciones/api/proveedores-repuesto/unificar", methods=["POST"])')
        self.f = APP[i:APP.index("_PROV_FAMILIAS_CACHE = ", i)]

    def test_solo_superadmin_en_las_dos_rutas(self):
        for ruta in ('"/mantenciones/api/proveedores-repuesto/duplicados", methods=["GET"]',
                     '"/mantenciones/api/proveedores-repuesto/unificar", methods=["POST"]'):
            i = APP.index("@app.route(" + ruta + ")")
            self.assertIn("@_require_superadmin_api", APP[i:i + 160])

    def test_nada_se_borra_los_duplicados_quedan_de_baja_con_motivo(self):
        self.assertNotIn("DELETE FROM mant_proveedores_repuesto", self.f)
        self.assertIn("SET activo=0, baja_motivo=%s, baja_por=%s, baja_at=NOW()", self.f)
        self.assertIn("Unificado con", self.f)

    def test_auditoria_antes_de_mover_y_todo_en_una_transaccion(self):
        self.assertLess(self.f.index('_audit("proveedor_repuesto_unificar"'), self.f.index("UPDATE {tabla} SET proveedor_id"))
        self.assertIn("conn.commit()", self.f)
        self.assertIn("conn.rollback()", self.f)

    def test_mueve_las_tablas_que_apuntan_al_proveedor(self):
        i = APP.index("_PROV_TABLAS_REF = (")
        t = APP[i:i + 400]
        for tabla in ("mant_repuestos_stock", "mant_ot_repuesto_solicitudes", "mant_repuestos_compras", "mant_repuestos_marcas",
                      "mant_proveedor_contactos", "mant_proveedor_notas"):
            self.assertIn(tabla, t)

    def test_no_pisa_la_ficha_del_que_se_queda(self):
        self.assertIn("if str(dest.get(col) or \"\").strip() or not str(org.get(col) or \"\").strip():", self.f)

    def test_validaciones(self):
        self.assertIn("sorted({int(x) for x in (d.get(\"origen_ids\") or [])} - {destino})", self.f)
        self.assertIn("len(origenes) > 30", self.f)
        self.assertIn("ya no existe o ya estaba dado de baja", self.f)


class TestPantalla(unittest.TestCase):
    def test_boton_y_modal_solo_para_superadmin(self):
        i = HTML.index("onclick=\"provUnificarAbrir()\"")
        self.assertIn("{% if permissions and permissions.superadmin %}", HTML[i - 400:i])
        j = HTML.index('id="modalProvUnificar"')
        self.assertIn("{% if permissions and permissions.superadmin %}", HTML[j - 900:j])

    def test_usa_modales_ilus_y_escapa_los_nombres(self):
        i = HTML.index("async function provUnificarGrupo(gi)")
        f = HTML[i:i + 1800]
        self.assertIn("ilusConfirm", f)
        self.assertIn("_pvuEsc(", f)
        self.assertNotIn("confirm(", f.replace("ilusConfirm(", ""))

    def test_los_parecidos_no_se_unifican_en_masa(self):
        i = HTML.index("async function provUnificarTodosIdenticos()")
        f = HTML[i:i + 600]
        self.assertIn("g.tipo === 'identicos'", f)

    def test_la_plantilla_compila(self):
        from jinja2 import Environment
        Environment().parse(HTML)


if __name__ == "__main__":
    unittest.main()

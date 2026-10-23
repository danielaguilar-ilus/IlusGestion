# -*- coding: utf-8 -*-
"""2026-10-23 · «Hay solicitudes de repuestos de prueba y quiero eliminarlas por el front, masivo, solo el superadmin» (Daniel).

Ruta POST /repuestos/api/solicitudes-ot/eliminar (solo superadmin, con motivo, omite las que ya movieron stock / están en una compra /
tienen OT, deja la foto en la auditoría) y la selección múltiple de la lista con «Eliminar seleccionadas (N)» visible solo al superadmin.
"""
import os
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _leer(*partes):
    with open(os.path.join(RAIZ, *partes), encoding="utf-8") as fh:
        return fh.read()


APP = _leer("app.py")
HTML = _leer("templates", "clientes_hub", "repuestos.html")


class TestRutaEliminar(unittest.TestCase):
    def setUp(self):
        i = APP.index('@app.route("/repuestos/api/solicitudes-ot/eliminar", methods=["POST"])')
        self.cab = APP[i:i + 200]
        fin = APP.index("# ═══", i)          # hasta el siguiente bloque: la ruta termina antes
        self.f = APP[i:fin]

    def test_solo_superadmin(self):
        self.assertIn("@_require_superadmin_api", self.cab)
        self.assertNotIn("_otrep_gestion_required", self.cab)

    def test_exige_motivo_y_tope(self):
        self.assertIn("len(motivo) < 5", self.f)
        self.assertIn("len(ids) > 300", self.f)

    def test_omite_lo_que_no_se_debe_borrar(self):
        for pista in ("mant_repuestos_movimientos", 's.get("compra_id")', 's.get("ot_generada_id")', "solicitud_padre_id=%s"):
            self.assertIn(pista, self.f)

    def test_audita_antes_de_borrar(self):
        self.assertLess(self.f.index('_audit("repuesto_solicitud_eliminada"'),
                        self.f.index('DELETE FROM mant_ot_repuesto_solicitudes WHERE id=%s'))

    def test_no_toca_tickets(self):
        self.assertNotIn("tk_tickets", self.f)

    def test_el_listado_dice_si_es_superadmin(self):
        self.assertIn('"es_superadmin": bool((getattr(g, "permissions", None) or {}).get("superadmin"))', APP)


class TestFront(unittest.TestCase):
    def test_boton_y_funcion(self):
        self.assertIn('id="rsListaBtnDel"', HTML)
        self.assertIn("async function rsListaEliminar()", HTML)
        self.assertIn("if (!_rs.esSuper) return;", HTML)
        self.assertIn("_rs.esSuper = !!j.es_superadmin;", HTML)

    def test_usa_los_modales_de_ilus_y_pide_motivo(self):
        i = HTML.index("async function rsListaEliminar()")
        f = HTML[i:i + 3500]
        self.assertIn("ilusConfirm", f)
        self.assertIn("ilusPrompt", f)
        self.assertIn("required: true", f)
        self.assertNotIn("confirm(", f.replace("ilusConfirm(", ""))

    def test_seleccionar_todo_existe_y_el_checkbox_sale_al_superadmin(self):
        self.assertIn("rsListaToggleTodo()", HTML)
        self.assertIn("(_rs.puedeGenerarOT || _rs.esSuper) ? '<input type=\"checkbox\" class=\"rst-chk\"", HTML)

    def test_la_plantilla_compila(self):
        from jinja2 import Environment
        Environment().parse(HTML)


if __name__ == "__main__":
    unittest.main()

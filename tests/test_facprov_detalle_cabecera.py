"""🧭 Detalle del lote de factura de proveedor: cabecera que informa, paso a paso y «Copiar solicitud» (Daniel, 12-oct-2026).

Daniel (lote #19, Transportes Milling): «el botón [de Excel] no se ve… las tarjetas las veo muy grandes e inútiles… un
header que de verdad informe… el seguimiento puede ser un paso a paso con botoncitos» y «que sea cómodo analizar y
mandar a facturar».

Qué se vigila:
  · Los botones de la cabecera (fondo negro) no usan btn-outline-dark: era negro sobre negro y el Excel no se veía.
  · «Qué sigue» y el paso a paso existen, con los mismos botones de siempre (no se quitó ninguna acción, REGLA #4.2).
  · Las tarjetas de montos conservan sus id: el panel en vivo de «Asignar OT» las proyecta.
  · «Copiar solicitud para el proveedor» solo lleva lo que el proveedor factura: nada de lo cobrado al cliente ni el
    margen, y a quién facturar es la razón social (REGLA #0). No envía nada (solo copia) y usa ilusConfirm (REGLA #1).
Sin BD, sin Flask.
Correr con:  py -m unittest tests.test_facprov_detalle_cabecera
"""
import os
import re
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
T = open(os.path.join(RAIZ, "templates", "mantenciones", "factura_proveedor_detalle.html"), encoding="utf-8").read()


def _bloque(ini, fin):
    i = T.index(ini)
    return T[i:T.index(fin, i)]


class TestCabecera(unittest.TestCase):
    def test_botones_de_la_cabecera_se_ven_sobre_fondo_negro(self):
        acc = _bloque('<div class="fpd-hero-acc">', '<div class="fpd-facts">')
        self.assertNotIn("btn-outline-dark", acc)
        self.assertIn("btn btn-sm btn-light fw-bold fpd-hbtn", acc)          # el Excel, claro y visible
        self.assertIn("mant_factura_proveedor_excel", acc)

    def test_ninguna_accion_se_perdio(self):
        cab = _bloque('<div class="page-header fpd-hero">', '{# ── Montos del lote')
        for f in ("fpdEditar()", "fpdSubirArchivo(this)", "fpdPagar()", "fpdCompletar()", "fpdReabrir()", "fpdAnular()",
                  "accion_completar", "Ver todas las facturas de este proveedor", "factura.notas", "factura.archivo_url"):
            self.assertIn(f, cab, f)

    def test_que_sigue_y_paso_a_paso(self):
        self.assertIn('<div class="fpd-next', T)
        self.assertIn("Qué sigue", T)
        self.assertEqual(T.count('<li class="fpd-step'), 4)
        self.assertRegex(T, r"\.fpd-mini,\.fpd-hbtn\{min-height:44px\}")    # REGLA #3 en celular

    def test_tarjetas_de_montos_conservan_sus_id(self):
        for i in ("fpdCellN", "fpdCellAsig", "fpdCellDif", "fpdCellCob", "fpdCellMg"):
            self.assertIn('id="%s"' % i, T)


class TestSolicitudParaElProveedor(unittest.TestCase):
    def test_solo_lo_que_el_proveedor_factura(self):
        datos = _bloque("const FPD_SOLICITUD = {", "function fpdTextoSolicitud")
        for prohibido in ("cobrado_cliente", "margen", "total_cobrado_cliente"):
            self.assertNotIn(prohibido, datos)
        self.assertIn("ilus_legal", datos)                                  # facturar a la razón social (REGLA #0)

    def test_copia_y_no_envia(self):
        f = _bloque("async function fpdCopiarSolicitud()", "\n}\n")
        self.assertIn("ilusConfirm(", f)
        self.assertIn("navigator.clipboard", f)
        self.assertNotIn("fetch(", f)
        self.assertIsNone(re.search(r"(?<![\w.])(alert|confirm|prompt)\(", f))


if __name__ == "__main__":
    unittest.main()

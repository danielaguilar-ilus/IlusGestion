# -*- coding: utf-8 -*-
"""2026-10-24 · Daniel, probando el asistente «Generar OT»:
  · Paso 8 (Acceso): «veo cuatro cosas y no entiendo qué completar… más dinámico, más amigable».
  · Paso 9 (Costos): «primero el centro de costo; luego pregúntame por el documento (líneas ZZ, o pagado en otro documento, o garantía); recién ahí
    cuánto. El paso 13 se resume con el 2, bien organizado».
  · Paso 11 (Anexo): «empoderarlo un poquito más en la gestión de creación de la OT».
Solo cambia el orden y la presentación: mismos datos, mismas reglas.
"""
import os
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
with open(os.path.join(RAIZ, "templates", "ot2", "_modal_crear.html"), encoding="utf-8") as _fh:
    M = _fh.read()


def _fn(nombre, largo=6000):
    i = M.index("function " + nombre)
    return M[i:i + largo]


class TestAcceso(unittest.TestCase):
    def test_dos_preguntas_obligatorias_y_lo_demas_plegado(self):
        f = _fn("pintarAcceso(b)", 3200)
        self.assertEqual(f.count("_o2mAccPregunta('"), 2)
        self.assertIn("¿El lugar tiene ascensor?", f)
        self.assertIn("¿Hay estacionamiento?", f)
        self.assertIn('<details class="o2m-accmas"', f)
        self.assertIn("Más datos del lugar", f)
        self.assertIn("2 respondidas", f)

    def test_la_tarjeta_pasa_de_rojo_a_verde_al_responder(self):
        f = _fn("setAcc(k,v)", 900)
        self.assertIn("c.classList.toggle('ok', S.acceso[k] !== null)", f)
        self.assertIn("cnt.textContent = n+' de 2 respondidas'", f)
        self.assertIn(".o2m-accq.ok{border-color:#86efac;border-left-color:#16a34a", M)

    def test_el_dato_y_la_regla_siguen_siendo_los_mismos(self):
        self.assertIn("S.acceso[k] = v==='' ? null : parseInt(v);", M)
        self.assertIn("_o2mAccToggle(k, val, ic)", M)

    def test_botones_grandes_de_44px_o_mas(self):
        self.assertIn(".o2m-accq .o2m-acc-btn{min-height:52px", M)


class TestCostos(unittest.TestCase):
    def setUp(self):
        self.f = _fn("pintarFinanzas(b)", 14000)

    def test_primero_centro_luego_como_se_cobra_luego_cuanto_luego_costo(self):
        f = self.f
        orden = [f.index("_o2mFpasoHtml('centro', 1"), f.index("_o2mFpasoHtml('cobro', 2"), f.index("_o2mFpasoHtml('valor', interno ? 2 : 3"),
                 f.index("_o2mFpasoHtml('costo', 4")]
        self.assertEqual(orden, sorted(orden))

    def test_agregar_otro_documento_vive_pegado_a_la_pregunta_del_documento(self):
        f = self.f
        i = f.index("_o2mFpasoHtml('cobro', 2")
        self.assertIn('<div id="o2mDocExtra" class="mt-2"></div>', f[i:i + 900])
        self.assertEqual(f.count('id="o2mDocExtra"'), 1, "un solo contenedor (los pintores lo buscan por id)")

    def test_el_trabajo_interno_sigue_con_solo_dos_subpasos(self):
        self.assertIn("(interno ? '' : _o2mFpasoHtml('costo', 4", self.f)
        self.assertIn("interno ? '' : (", self.f)

    def test_el_senalador_sigue_el_orden_de_la_pantalla(self):
        self.assertIn("['centro','cobro','valor','costo']", M)

    def test_los_cuatro_subpasos_siguen_existiendo(self):
        for sub in ("'centro'", "'cobro'", "'valor'", "'costo'"):
            self.assertIn("_o2mFpasoHtml(" + sub, self.f)


class TestAnexo(unittest.TestCase):
    def test_cabecera_camino_y_lista_de_productos_con_cantidades(self):
        f = _fn("_o2mAnexoResumenInterior(ok, a, total, prods, uds)", 3200)
        for pista in ("o2m-ax-kpis", "Se le paga", "o2m-ax-camino", "El proveedor firma", "La OT se libera", "Qué firma el proveedor",
                      '<th class="n">Cant.</th>'):
            self.assertIn(pista, f)

    def test_se_actualiza_sin_repintar_los_campos(self):
        self.assertIn("function refrescarAnclas(){ pintarAnclas(); try { _o2mAnexoResumenRefrescar(); } catch (e) {} }", M)
        f = _fn("_o2mAnexoResumenRefrescar()", 800)
        self.assertIn("document.getElementById('o2mAxResumen'); if (!el) return;", f)

    def test_se_pinta_antes_de_los_campos_y_no_toca_las_reglas_del_anexo(self):
        f = _fn("pintarAnexo(b)", 900)
        self.assertLess(f.index("_o2mAnexoResumenHtml()"), f.index("oninput=\"OT2C.setAnexoCampo(\\'proveedor_nombre\\'"))
        self.assertIn("no podrá ver ni iniciar esta OT hasta firmar este anexo", f)


if __name__ == "__main__":
    unittest.main()

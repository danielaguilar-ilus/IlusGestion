"""Desglose de stock (físico / comprometido / devengado) al cotizar o retirar.

Daniel, 2026-08-17, con captura real del comparador "Asignar y Cotizar" (dos
Gymleco "1 u. saldo 1" marcados en rojo "Sin stock bod. 02"): "creo que fui
yo quien te pidió que solo tomaras el stock teórico [fisico - comprometido -
devengado], pero yo requiero mapear la información completa... si hay al
menos uno físico positivo y cuántos están comprometidos, para tomar la
decisión -- es decir, hay 1 y está comprometido, yo veré si avanzo o no".

El backend (get_erp_stock_by_sku/get_erp_stock_by_skus, app.py) YA devolvía
el desglose completo desde el 2026-08-03 -- eso no cambió y sus pruebas
viven en tests/test_erp_stock_bodega.py, sin tocar. Lo que cambió es la
PRESENTACIÓN: antes se colapsaba todo a un booleano (hay_stock) y el mismo
"Sin stock bod. 02" en rojo salía tanto si el físico era 0 como si era 1
pero ya comprometido -- dos situaciones muy distintas para quien decide.

Este archivo verifica:
  1. El comportamiento real de _tkaStockEstado/_tkaStockBadge (tickets/
     _tka_modal.html) y su espejo _rbaStockEstado/_rbaStockBadge (retiros/
     internal_detail.js), vía el harness de node (test_stock_fisico_
     comprometido.js) -- mismo patrón que test_shipit_operador_seleccion.py.
  2. Que la SELECCIÓN siga igual de cautelosa que antes en los dos estados
     ('sin' y 'comprometido'): ninguno viene pre-marcado, "seleccionar
     todas" sigue sin arrastrarlos. Ese comportamiento NO cambió a propósito
     -- ver el comentario en ambos archivos fuente.
  3. Que los dos archivos sigan siendo un espejo exacto (mismo criterio,
     mismas clases CSS) -- si se desalinean, el próximo bug será "en
     Retiros sí se ve bien pero en Cotizaciones no".

Si node no está instalado, la parte 1 se salta (no falla); 2 y 3 son texto
puro y corren siempre.

Correr con:  py -m unittest tests.test_stock_fisico_comprometido -v
"""
import os
import shutil
import subprocess
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TPL_TKA = os.path.join(RAIZ, "templates", "tickets", "_tka_modal.html")
JS_RBA = os.path.join(RAIZ, "static", "retiros_internal_detail.js")
CSS_RBA = os.path.join(RAIZ, "static", "retiros_internal_detail.css")

NODE = shutil.which("node")


def _leer(path):
    with open(path, encoding="utf-8") as fh:
        return fh.read()


def _correr_node(script, *args):
    proc = subprocess.run(
        [NODE, os.path.join(RAIZ, "tests", script)] + list(args),
        capture_output=True, text=True, encoding="utf-8", errors="replace",
        cwd=RAIZ, timeout=120,
    )
    if proc.returncode != 0:
        raise AssertionError(
            f"{script} falló (exit {proc.returncode})\n"
            f"--- stdout ---\n{proc.stdout}\n--- stderr ---\n{proc.stderr}"
        )
    return proc.stdout


# ══════════════════════════════════════════════════════════════════════════
#  1) Comportamiento real (harness de node) -- ambos archivos, mismos casos
# ══════════════════════════════════════════════════════════════════════════
@unittest.skipIf(NODE is None, "node no está instalado en este equipo")
class TestComportamientoEnElNavegador(unittest.TestCase):

    def test_los_seis_casos_en_ambos_archivos(self):
        salida = _correr_node("test_stock_fisico_comprometido.js")
        self.assertIn("stock fisico/comprometido OK", salida, salida)
        self.assertIn("12 verificaciones OK", salida, salida)
        # El caso real de Daniel, explícito, para que un cambio futuro que
        # rompa justo ESE caso no pase desapercibido detrás del conteo.
        for marca in (
            'tka: 1 físico + 1 comprometido -> "comprometido", ámbar, con el desglose a la vista',
            'rba: 1 físico + 1 comprometido -> "comprometido", ámbar, con el desglose a la vista',
            'tka: 0 físico -> "sin", rojo, "Sin stock bod. 02"',
            'rba: 0 físico -> "sin", rojo, "Sin stock bod. 02"',
        ):
            self.assertIn(marca, salida, salida)

    def test_el_javascript_de_retiros_compila(self):
        proc = subprocess.run([NODE, "--check", JS_RBA],
                              capture_output=True, text=True, cwd=RAIZ, timeout=60)
        self.assertEqual(0, proc.returncode, proc.stderr)


# ══════════════════════════════════════════════════════════════════════════
#  2) La selección sigue igual de cautelosa (decisión deliberada, no un bug)
# ══════════════════════════════════════════════════════════════════════════
class TestLaSeleccionNoCambio(unittest.TestCase):
    """Daniel pidió VER el desglose para decidir él mismo -- no pidió que el
    sistema pre-seleccione líneas con stock disputado. Cambiar el default de
    selección tiene riesgo de negocio real (auto-incluir en un despacho algo
    que ya está comprometido para otro pedido); el pedido textual fue sobre
    INFORMACIÓN, no sobre automatizar la decisión. Estas pruebas fijan que
    ese comportamiento se mantuvo."""

    @classmethod
    def setUpClass(cls):
        cls.tka = _leer(TPL_TKA)
        cls.rba = _leer(JS_RBA)

    def test_ninguno_de_los_dos_estados_viene_premarcado(self):
        # sinStockBod sigue siendo true para AMBOS estados ('sin' Y
        # 'comprometido') -- solo cambió qué clase visual se le pone a la fila.
        for fuente, nombre in ((self.tka, "_tka_modal.html"), (self.rba, "retiros_internal_detail.js")):
            self.assertIn("const sinStockBod = stockEstado !== '';", fuente,
                          f"{nombre}: sinStockBod debe seguir cubriendo los 2 estados")

    def test_seleccionar_todas_sigue_sin_arrastrar_stock_disputado(self):
        # El guard de "toggle all" compara contra dataset.sinStock, que sigue
        # poblándose desde el mismo sinStockBod de arriba -- no se tocó.
        self.assertIn("if (checked && (isZero || sinStockBod)) return;", self.tka)
        self.assertIn("if (checked && (isZero || sinStockBod)) return;", self.rba)


# ══════════════════════════════════════════════════════════════════════════
#  3) Los dos archivos siguen siendo un espejo exacto
# ══════════════════════════════════════════════════════════════════════════
class TestEspejoEntreCotizacionesYRetiros(unittest.TestCase):
    """Si Cotizaciones y Retiros dejan de compartir el mismo criterio, el
    próximo reporte de Daniel es "en Retiros se ve bien pero en Cotizaciones
    no" -- el mismo tipo de bug que esta feature vino a arreglar."""

    @classmethod
    def setUpClass(cls):
        cls.tka = _leer(TPL_TKA)
        cls.rba = _leer(JS_RBA)
        cls.css_rba = _leer(CSS_RBA)

    def test_ambos_tienen_el_helper_de_estado(self):
        self.assertIn("function _tkaStockEstado(l){", self.tka)
        self.assertIn("function _rbaStockEstado(l){", self.rba)

    def test_ambos_distinguen_sin_de_comprometido(self):
        for fuente in (self.tka, self.rba):
            self.assertIn("return (parseFloat(st.fisico) || 0) <= 0 ? 'sin' : 'comprometido';", fuente)

    def test_ambos_tienen_las_dos_clases_css_de_fila(self):
        self.assertIn(".tka-line.is-no-stock", self.tka)
        self.assertIn(".tka-line.is-stock-comprometido", self.tka)
        self.assertIn(".rba-line.is-no-stock", self.css_rba)
        self.assertIn(".rba-line.is-stock-comprometido", self.css_rba)

    def test_ninguno_perdio_el_badge_rojo_original(self):
        # REGLA #4.2: se AGREGA el estado 'comprometido', no se reemplaza
        # el aviso rojo cuando de verdad no hay nada.
        for fuente in (self.tka, self.rba):
            self.assertIn("ln-stock-warn", fuente)
            self.assertIn("Sin stock bod. 02", fuente)

    def test_el_badge_informativo_muestra_el_desglose_no_solo_el_tooltip(self):
        for fuente in (self.tka, self.rba):
            self.assertIn("ln-stock-info", fuente)
            self.assertIn("' físico'", fuente)
            self.assertIn("' comp.'", fuente)


if __name__ == "__main__":
    unittest.main(verbosity=2)

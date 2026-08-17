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
class TestLaPreseleccionDeComprometidoCambio(unittest.TestCase):
    """FIX 2026-08-17-b. Daniel, viendo el resultado del pase anterior (dos
    Gymleco con 1 físico + 1 comprometido, sin marcar): "si está cargado
    positivamente quiero que lo seleccione para avanzar, ya que da la
    sensación que no está cargado positivamente... si hay 1 en stock y está
    comprometido -- puede ser por el mismo documento que estoy cotizando --
    yo veré si avanzo o no... solo selecciónalo si hay al menos 1 y notifica
    que está comprometido".

    O sea: el primer pase (no pre-marcar NINGUNO de los dos estados) fue
    DEMASIADO cauteloso -- leía como "esto está roto" cuando en realidad
    había una unidad física real disponible. Daniel confirma explícitamente
    que asume el riesgo de la posible confusión ("esto puede ocasionar una
    tremenda confusión") a cambio de no frenar el flujo cuando sí hay algo
    físico.

    Regla vigente: SOLO 'sin' (0 físico) deja la línea sin marcar. 'comprometido'
    (físico >= 1) se pre-selecciona iguial que una línea sana, con el aviso
    ámbar siempre visible encima."""

    @classmethod
    def setUpClass(cls):
        cls.tka = _leer(TPL_TKA)
        cls.rba = _leer(JS_RBA)

    def test_solo_sin_bloquea_la_preseleccion(self):
        for fuente, nombre in ((self.tka, "_tka_modal.html"), (self.rba, "retiros_internal_detail.js")):
            self.assertIn("const sinStockBod = stockEstado === 'sin';", fuente,
                          f"{nombre}: sinStockBod debe cubrir SOLO 'sin', no 'comprometido'")
            self.assertNotIn("const sinStockBod = stockEstado !== '';", fuente,
                             f"{nombre}: quedó el criterio viejo (bloqueaba los 2 estados)")

    def test_comprometido_participa_en_seleccionar_todas(self):
        # El guard de "toggle all" no cambió de código -- compara contra
        # dataset.sinStock, que ahora solo es true para 'sin'. Por
        # construcción, 'comprometido' ya participa en el bulk-select.
        self.assertIn("if (checked && (isZero || sinStockBod)) return;", self.tka)
        self.assertIn("if (checked && (isZero || sinStockBod)) return;", self.rba)

    def test_el_aviso_ambar_sobrevive_estando_seleccionada(self):
        # Riesgo real: .is-selected (verde) y .is-stock-comprometido (ámbar)
        # ahora coexisten en la misma fila -- sin el selector compuesto, el
        # verde tapaba el aviso justo en el caso que más importa mostrarlo.
        self.assertIn(".tka-line.is-selected.is-stock-comprometido{", self.tka)
        css_rba = _leer(CSS_RBA)
        self.assertIn(".rba-line.is-selected.is-stock-comprometido{", css_rba)


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

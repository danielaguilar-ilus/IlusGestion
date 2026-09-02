"""El botón "Quitar" de manifiesto_detalle.html seguía con `disabled` DURO
para cualquier item "en gestión con el courier" -- exactamente el caso de
una factura duplicada. El backend (tr_quitar_item, ver
test_manifiesto_quitar_factura_duplicada.py) ya sabe permitir la excepción,
pero el botón nunca llegaba a dispararlo: en el HTML servido, el atributo
`disabled` estaba puesto sin condición para saber si ERA un duplicado.

Pregunta real de Daniel (2026-08-27, tras ver el backend + JS ya listos):
"va a quedar por el front?" -- la respuesta antes de ese fix era NO.

AMPLIADO 2026-09-02: Alison seguía sin poder quitar duplicadas reales.
Daniel, en vivo: "recuerda que la única restricción es que tenga
movimiento con el courier". El botón (igual que el backend) exigía ADEMÁS
que `_fetch_items()` encontrara una copia hermana calzando el criterio
(`duplicada_en_otro_manifiesto`) -- si esa copia ya no calificaba, el botón
quedaba deshabilitado igual aunque el usuario tuviera permiso. Ahora la
única condición para habilitar el botón es tener el permiso -- el dato de
"hay otra copia" solo cambia el tooltip, ya no gatea el botón.

EL FIX (vigente):
  1. `_fetch_items()` (dentro de tr_manifiesto_detalle) calcula en LOTE --
     una sola query, no N+1 -- si cada item tiene una copia hermana (mismo
     commitment_id) en OTRO manifiesto no eliminado. Es solo INFORMATIVO
     para el tooltip; ya no condiciona si el botón se habilita.
  2. El template calcula `_puede_quitar_con_gestion` = está en gestión con
     el courier Y (is_superadmin o permissions.tr_eliminar). Si es true, el
     botón queda habilitado (con tooltip distinto según si se detectó o no
     una copia duplicada) aunque `_en_gestion_courier` sea true. Sin el
     permiso, sigue bloqueado sin excepción.

app.py tiene 90k+ líneas -- se extrae el cuerpo de tr_manifiesto_detalle()
por slicing de texto, mismo patrón que el resto de los tests de este módulo.

Correr con:  py -m unittest tests.test_manifiesto_quitar_duplicada_boton_frontend -v
"""
import os
import unittest

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _fuente_tr_manifiesto_detalle():
    with open(os.path.join(BASE_DIR, "app.py"), encoding="utf-8", errors="ignore") as f:
        src = f.read()
    i = src.index("\ndef tr_manifiesto_detalle(")
    j = src.index("\ndef ", i + 10)
    return src[i:j]


SRC = _fuente_tr_manifiesto_detalle()

with open(os.path.join(BASE_DIR, "templates", "transporte", "manifiesto_detalle.html"),
          encoding="utf-8", errors="ignore") as _f:
    HTML_SRC = _f.read()


class TestQueryBatchDeDuplicados(unittest.TestCase):
    """La detección de duplicados se hace en UNA sola query dentro de
    _fetch_items(), no una consulta por item (evita N+1 en manifiestos
    grandes). Sigue existiendo como dato informativo para el tooltip."""

    def test_agrega_la_columna_duplicada_en_otro_manifiesto(self):
        self.assertIn("AS duplicada_en_otro_manifiesto", SRC)

    def test_usa_el_mismo_criterio_que_tr_quitar_item(self):
        # Mismas 2 condiciones que la guarda real en tr_quitar_item(): excluye
        # la propia copia y manifiestos eliminados. AMPLIADO 2026-08-27 (Daniel:
        # "no son duplicados, quiero que ella pueda dejar limpio"): ya NO exige
        # estado_entrega = 'Entregado' -- cualquier otra copia activa cuenta.
        i = SRC.index("AS duplicada_en_otro_manifiesto")
        fragmento = SRC[max(0, i - 500):i]
        self.assertIn("mi3.id != mi.id", fragmento)
        self.assertIn("m3.eliminado = 0", fragmento)
        self.assertNotIn("estado_entrega", fragmento)

    def test_esta_dentro_de_fetch_items_no_en_otra_funcion(self):
        i_fetch = SRC.index("def _fetch_items():")
        i_col = SRC.index("AS duplicada_en_otro_manifiesto")
        i_fin_fetch = SRC.index("items = _fetch_items()")
        self.assertLess(i_fetch, i_col)
        self.assertLess(i_col, i_fin_fetch)


class TestElBotonSeHabilitaConSoloElPermiso(unittest.TestCase):
    """AMPLIADO 2026-09-02: las DOS vistas (tabla de escritorio + tarjeta
    móvil) deben llegar a la misma conclusión: en gestión con el courier +
    permiso -> botón clickeable, exista o no una copia duplicada detectada."""

    def _fragmentos_boton_quitar(self):
        # Hay 2 ocurrencias de quitarItem( en el HTML (desktop + mobile card).
        ocurrencias = []
        idx = 0
        while True:
            i = HTML_SRC.find('_puede_quitar_con_gestion = _en_gestion_courier', idx)
            if i == -1:
                break
            fin = HTML_SRC.index("</button>", i)
            ocurrencias.append(HTML_SRC[i:fin])
            idx = fin
        return ocurrencias

    def test_hay_dos_ocurrencias_desktop_y_mobile(self):
        frags = self._fragmentos_boton_quitar()
        self.assertEqual(len(frags), 2,
            "se esperaban 2 botones 'Quitar' con la logica nueva (tabla + tarjeta movil)")

    def test_la_condicion_ya_no_exige_duplicada_detectada(self):
        """El bug real: antes se exigia ADEMAS item.get('duplicada_en_otro_manifiesto')
        para habilitar -- eso volvia a bloquear si esa copia ya no calificaba."""
        for frag in self._fragmentos_boton_quitar():
            i_set = frag.index("_puede_quitar_con_gestion = _en_gestion_courier")
            fin_set = frag.index("%}", i_set)
            condicion = frag[i_set:fin_set]
            self.assertNotIn("duplicada_en_otro_manifiesto", condicion)
            self.assertIn("is_superadmin", condicion)
            self.assertIn("permissions.tr_eliminar", condicion)

    def test_el_disabled_ahora_tiene_excepcion(self):
        """El bug real: antes 'disabled' se ponia SIEMPRE que
        _en_gestion_courier fuera true, sin mirar el permiso."""
        for frag in self._fragmentos_boton_quitar():
            i_disabled = frag.index("disabled")
            antes = frag[:i_disabled]
            self.assertIn("_en_gestion_courier and not _puede_quitar_con_gestion", antes)

    def test_el_boton_habilitado_sigue_llamando_quitaritem(self):
        for frag in self._fragmentos_boton_quitar():
            self.assertIn("onclick=\"quitarItem(", frag)

    def test_sin_permiso_el_boton_sigue_bloqueado_sin_excepcion(self):
        """El único candado real ahora es el permiso, no la existencia de
        una copia duplicada detectada."""
        for frag in self._fragmentos_boton_quitar():
            self.assertIn(
                "_en_gestion_courier and (is_superadmin or permissions.tr_eliminar)",
                frag)


class TestRenderReal(unittest.TestCase):
    """No basta con el texto crudo -- la prueba definitiva (mismo patron que
    test_eliminar_manifiesto_html_no_se_corta.py) es renderizar el fragmento
    REAL con Flask/Jinja y confirmar el HTML resultante en los 4 casos que
    importan: con permiso (habilitado, exista o no una duplicada detectada),
    y sin permiso (bloqueado, exista o no una duplicada detectada)."""

    def _fragmento_boton_desktop(self):
        i = HTML_SRC.index("_puede_quitar_con_gestion = _en_gestion_courier")
        # Retrocede al {% set que lo contiene y avanza hasta el </button> del
        # boton de basurero (el primero despues del set).
        ini = HTML_SRC.rfind("{% set", 0, i)
        fin = HTML_SRC.index("</button>", i) + len("</button>")
        return HTML_SRC[ini:fin]

    def _renderizar(self, *, duplicada, tiene_permiso, en_gestion=True):
        from flask import Flask, render_template_string
        app = Flask(__name__)
        frag = self._fragmento_boton_desktop()
        tpl = (
            "{% set _en_gestion_courier = en_gestion %}"
            + frag
        )
        with app.test_request_context():
            return render_template_string(
                tpl,
                en_gestion=en_gestion,
                manifiesto={"id": 77},
                item={"id": 501, "duplicada_en_otro_manifiesto": 1 if duplicada else None},
                is_superadmin=False,
                permissions={"tr_eliminar": tiene_permiso},
            )

    def test_duplicada_con_permiso_queda_habilitado(self):
        html = self._renderizar(duplicada=True, tiene_permiso=True)
        self.assertNotIn("disabled", html)
        self.assertIn("onclick=\"quitarItem(77, 501)\"", html)

    def test_duplicada_sin_permiso_sigue_bloqueado(self):
        html = self._renderizar(duplicada=True, tiene_permiso=False)
        self.assertIn("disabled", html)
        self.assertIn("Eliminar manifiestos y pedidos", html)
        self.assertNotIn("onclick=\"quitarItem(", html)

    def test_sin_duplicada_pero_con_permiso_ahora_queda_habilitado(self):
        """AMPLIADO 2026-09-02 (Daniel: "la única restricción es que tenga
        movimiento con el courier"): antes esto seguía bloqueado aunque
        tuviera permiso -- exactamente el caso real que Alison reportó."""
        html = self._renderizar(duplicada=False, tiene_permiso=True)
        self.assertNotIn("disabled", html)
        self.assertIn("onclick=\"quitarItem(77, 501)\"", html)

    def test_sin_duplicada_y_sin_permiso_sigue_bloqueado(self):
        html = self._renderizar(duplicada=False, tiene_permiso=False)
        self.assertIn("disabled", html)
        self.assertNotIn("onclick=\"quitarItem(", html)

    def test_sin_en_gestion_courier_el_boton_esta_libre_como_siempre(self):
        """Caso mas comun: la factura no tiene tracking todavia -- el boton
        de siempre, sin ninguna de las ramas nuevas."""
        html = self._renderizar(duplicada=False, tiene_permiso=False, en_gestion=False)
        self.assertNotIn("disabled", html)
        self.assertIn("onclick=\"quitarItem(77, 501)\"", html)


if __name__ == "__main__":
    unittest.main(verbosity=2)

"""El botón "Quitar" de manifiesto_detalle.html seguía con `disabled` DURO
para cualquier item "en gestión con el courier" -- exactamente el caso de
una factura duplicada. El backend (tr_quitar_item, ver
test_manifiesto_quitar_factura_duplicada.py) ya sabe permitir la excepción,
pero el botón nunca llegaba a dispararlo: en el HTML servido, el atributo
`disabled` estaba puesto sin condición para saber si ERA un duplicado.

Pregunta real de Daniel (2026-08-27, tras ver el backend + JS ya listos):
"va a quedar por el front?" -- la respuesta antes de este fix era NO.

EL FIX:
  1. `_fetch_items()` (dentro de tr_manifiesto_detalle) ahora calcula en LOTE
     -- una sola query, no N+1 -- si cada item tiene una copia hermana
     (mismo commitment_id) YA entregada en OTRO manifiesto no eliminado.
     Mismo criterio EXACTO que la guarda de tr_quitar_item(), para que lo
     que el botón promete coincida con lo que el backend después autoriza.
  2. El template calcula `_puede_quitar_duplicada` = es duplicado Y
     (is_superadmin o permissions.tr_eliminar). Si es true, el botón queda
     habilitado (con tooltip distinto) aunque `_en_gestion_courier` sea
     true. Si NO es duplicado, sigue bloqueado sin excepción (REGLA #4.2:
     no se relaja lo que no se acordó).

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
    grandes)."""

    def test_agrega_la_columna_duplicada_entregada_en_otro(self):
        self.assertIn("AS duplicada_entregada_en_otro", SRC)

    def test_usa_el_mismo_criterio_que_tr_quitar_item(self):
        # Mismas 3 condiciones que la guarda real en tr_quitar_item(): estado
        # Entregado, manifiesto no eliminado, y excluye la propia copia.
        i = SRC.index("AS duplicada_entregada_en_otro")
        fragmento = SRC[max(0, i - 500):i]
        self.assertIn("mi3.id != mi.id", fragmento)
        self.assertIn("mi3.estado_entrega = 'Entregado'", fragmento)
        self.assertIn("m3.eliminado = 0", fragmento)

    def test_esta_dentro_de_fetch_items_no_en_otra_funcion(self):
        i_fetch = SRC.index("def _fetch_items():")
        i_col = SRC.index("AS duplicada_entregada_en_otro")
        i_fin_fetch = SRC.index("items = _fetch_items()")
        self.assertLess(i_fetch, i_col)
        self.assertLess(i_col, i_fin_fetch)


class TestElBotonSeHabilitaParaDuplicadasConPermiso(unittest.TestCase):
    """Las DOS vistas (tabla de escritorio + tarjeta móvil) deben llegar a
    la misma conclusión: duplicada + permiso -> boton clickeable."""

    def _fragmentos_boton_quitar(self):
        # Hay 2 ocurrencias de quitarItem( en el HTML (desktop + mobile card).
        ocurrencias = []
        idx = 0
        while True:
            i = HTML_SRC.find('_puede_quitar_duplicada = _en_gestion_courier', idx)
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

    def test_la_condicion_exige_duplicada_y_permiso(self):
        for frag in self._fragmentos_boton_quitar():
            self.assertIn("item.get('duplicada_entregada_en_otro')", frag)
            self.assertIn("is_superadmin", frag)
            self.assertIn("permissions.tr_eliminar", frag)

    def test_el_disabled_ahora_tiene_excepcion(self):
        """El bug real: antes 'disabled' se ponia SIEMPRE que
        _en_gestion_courier fuera true, sin mirar si era un duplicado."""
        for frag in self._fragmentos_boton_quitar():
            i_disabled = frag.index("disabled")
            antes = frag[:i_disabled]
            # La condicion que antecede al disabled debe incluir la negacion
            # de _puede_quitar_duplicada -- si no, el fix no esta aplicado.
            self.assertIn("_en_gestion_courier and not _puede_quitar_duplicada", antes)

    def test_el_boton_habilitado_sigue_llamando_quitaritem(self):
        for frag in self._fragmentos_boton_quitar():
            self.assertIn("onclick=\"quitarItem(", frag)

    def test_sin_duplicada_el_boton_sigue_bloqueado_sin_excepcion(self):
        """REGLA #4.2: si NO hay duplicada entregada en otro lado, el
        candado de siempre se mantiene -- no se toca el caso general."""
        for frag in self._fragmentos_boton_quitar():
            # _puede_quitar_duplicada es False si duplicada_entregada_en_otro
            # es None/False -- el 'and' en la definicion garantiza esto,
            # verificado en la propia condicion de habilitacion.
            self.assertIn(
                "_en_gestion_courier and item.get('duplicada_entregada_en_otro') and",
                frag)


class TestRenderReal(unittest.TestCase):
    """No basta con el texto crudo -- la prueba definitiva (mismo patron que
    test_eliminar_manifiesto_html_no_se_corta.py) es renderizar el fragmento
    REAL con Flask/Jinja y confirmar el HTML resultante en los 3 casos que
    importan: duplicada+permiso (habilitado), duplicada sin permiso
    (bloqueado con el tooltip nuevo), y sin duplicada (bloqueado como
    siempre, sin tocar el caso general)."""

    def _fragmento_boton_desktop(self):
        i = HTML_SRC.index("_puede_quitar_duplicada = _en_gestion_courier")
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
                item={"id": 501, "duplicada_entregada_en_otro": 1 if duplicada else None},
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

    def test_sin_duplicada_sigue_bloqueado_aunque_tenga_permiso(self):
        """REGLA #4.2: tener el permiso no relaja el candado general -- solo
        aplica al caso de duplicado confirmado."""
        html = self._renderizar(duplicada=False, tiene_permiso=True)
        self.assertIn("disabled", html)
        self.assertIn("ya está en gestión con el courier", html)
        self.assertNotIn("onclick=\"quitarItem(", html)

    def test_sin_en_gestion_courier_el_boton_esta_libre_como_siempre(self):
        """Caso mas comun: la factura no tiene tracking todavia -- el boton
        de siempre, sin ninguna de las ramas nuevas."""
        html = self._renderizar(duplicada=False, tiene_permiso=False, en_gestion=False)
        self.assertNotIn("disabled", html)
        self.assertIn("onclick=\"quitarItem(77, 501)\"", html)


if __name__ == "__main__":
    unittest.main(verbosity=2)

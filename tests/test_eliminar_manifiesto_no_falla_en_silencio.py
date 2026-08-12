"""eliminarManifiesto() (Manifiestos → "···" → Eliminar manifiesto) podía morir
en silencio: nada en pantalla, ningún error visible.

BUG REAL EN PRODUCCIÓN (2026-08-12). Daniel, superadmin, con captura del menú
"···" abierto mostrando "Eliminar manifiesto": "lo del borrado le doy a los
3 puntos y no lo elimina" -- ningún toast, ninguna alerta, nada.

CAUSA CONFIRMADA leyendo el código: el `await ilusConfirm({...})` vivía
FUERA del try/catch de la función. Si por cualquier motivo ilusConfirm no
estaba listo en ese instante (ej. una carrera de carga de scripts), la
excepción se convertía en un unhandled promise rejection -- invisible para
el usuario, sin ningún rastro. Otras páginas del proyecto
(transporte_monitor.js, mantenciones_ot_ejecutar.js, retiros_internal_detail.js,
transporte_etiquetas_page.js) ya se cuidan de esto comprobando
`typeof ilusConfirm === 'function'` antes de usarlo -- esta función no lo
tenía.

Correr con:  py -m unittest tests.test_eliminar_manifiesto_no_falla_en_silencio -v
"""
import unittest

with open("static/transporte_manifiestos.js", encoding="utf-8", errors="ignore") as fh:
    JS_SRC = fh.read()


def _cuerpo_eliminar_manifiesto():
    i = JS_SRC.find("async function eliminarManifiesto")
    assert i > 0, "no se encontro eliminarManifiesto en transporte_manifiestos.js"
    # Corta en el primer "\n}\n" a nivel de columna 0 despues del inicio --
    # la funcion cierra con `}` sin indentar (fin de funcion top-level).
    fin = JS_SRC.find("\n}\n", i)
    assert fin > 0
    return JS_SRC[i:fin + 2]


class TestEliminarManifiestoNoFallaEnSilencio(unittest.TestCase):

    def test_ilusconfirm_queda_dentro_del_try(self):
        """El bug real: ilusConfirm() vivia ANTES del try{ -- cualquier
        excepcion ahi no la atajaba nada."""
        cuerpo = _cuerpo_eliminar_manifiesto()
        i_try = cuerpo.find("try {")
        i_confirm = cuerpo.find("await ilusConfirm(")
        self.assertGreater(i_try, 0, "no se encontro el try{ de la funcion")
        self.assertGreater(i_confirm, 0, "no se encontro la llamada a ilusConfirm")
        self.assertLess(i_try, i_confirm,
            "ilusConfirm() sigue quedando FUERA (antes) del try -- "
            "una excepcion ahi vuelve a morir en silencio")

    def test_verifica_que_ilusconfirm_exista_antes_de_usarlo(self):
        cuerpo = _cuerpo_eliminar_manifiesto()
        self.assertIn("typeof ilusConfirm !== 'function'", cuerpo)

    def test_el_catch_deja_rastro_en_consola(self):
        """Aunque ilusAlert tambien fallara, console.error debe quedar --
        antes no habia NINGUN rastro, ni siquiera en devtools."""
        cuerpo = _cuerpo_eliminar_manifiesto()
        i_catch = cuerpo.find("} catch (e) {")
        self.assertGreater(i_catch, 0)
        bloque_catch = cuerpo[i_catch:]
        self.assertIn("console.error(", bloque_catch)

    def test_el_catch_no_puede_el_mismo_morir_en_silencio(self):
        """Si ilusAlert tambien esta roto, un segundo catch anidado evita
        que ESE fallo vuelva a ser un unhandled rejection silencioso."""
        cuerpo = _cuerpo_eliminar_manifiesto()
        i_catch = cuerpo.find("} catch (e) {")
        bloque_catch = cuerpo[i_catch:]
        self.assertIn("catch (_e2)", bloque_catch)

    def test_sigue_usando_ilusconfirm_no_confirm_nativo(self):
        """REGLA #1 del proyecto -- la guarda nueva no debe convertirse en
        una excusa para caer a confirm() nativo."""
        cuerpo = _cuerpo_eliminar_manifiesto()
        self.assertNotIn("window.confirm(", cuerpo)
        self.assertNotRegex(cuerpo, r"[^.]\bconfirm\(['\"]")


if __name__ == "__main__":
    unittest.main(verbosity=2)

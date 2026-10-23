# -*- coding: utf-8 -*-
"""2026-10-23 · «Víctor pidió una autorización (instalación con desinstalación) y nunca me llegó» (Daniel).

El correo interno de «Autorización pendiente» salía por la llave «Servicio Técnico» (mantenciones), cerrada «por pruebas»: la puerta
lo frenaba en silencio (quedaba «bloqueado» en el Historial) y solo llegaba la campana. Es un aviso INTERNO al dueño: sale por la llave de
«Comunicación interna», y el registro (Control de correos) dice lo mismo que el envío.
"""
import os
import unittest

import comm_correos_registro as R

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class TestCorreoDeAutorizacionPendiente(unittest.TestCase):
    def test_el_envio_sale_por_comunicacion_interna(self):
        with open(os.path.join(RAIZ, "app.py"), encoding="utf-8") as fh:
            app = fh.read()
        i = app.index("def _ot_aut_notificar_superadmins(a):")
        f = app[i:i + 6000]
        self.assertIn('evento="ot_autorizacion_pendiente", modulo="comunicacion_interna"', f)
        self.assertNotIn('modulo="mantenciones"', f)
        self.assertIn('correo="ot_autorizacion_pendiente"', f)
        self.assertIn("_mant_notificar(sa[\"id\"], \"otro\"", f, "la campana a cada superadministrador sigue")

    def test_el_registro_dice_la_misma_llave(self):
        c = R.por_clave("ot_autorizacion_pendiente")
        self.assertEqual(c["llave"], "comunicacion_interna")
        self.assertEqual(c["destinatario"], "interno")
        self.assertTrue(c["sensible"])


if __name__ == "__main__":
    unittest.main()

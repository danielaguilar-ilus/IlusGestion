# -*- coding: utf-8 -*-
"""2026-10-23 · «Felipe, al ser superadmin, recibe los avisos de autorización; sácale eso por el front» (Daniel).

El aviso de «Autorización pendiente» (campana + correo) iba a TODO superadministrador activo. Ahora lo recibe quien tenga encendido
«Recibe avisos de autorizaciones de OT» (permiso de tipo BLOQUEO: nace concedido, se apaga por persona en «Permisos individuales»).
También: el título del correo dice «no tiene un documento que respalde el cobro» (no «no tiene documento»).
"""
import os
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
with open(os.path.join(RAIZ, "app.py"), encoding="utf-8") as _fh:
    APP = _fh.read()


class TestReceptoresDeAutorizaciones(unittest.TestCase):
    def test_el_permiso_esta_en_todas_las_tablas(self):
        self.assertIn('    "mant_ot_autoriza_aviso",\n)', APP, "llave en PERMS_KEYS (permisos individuales)")
        self.assertIn('("mant_reagendar", "mant_reasignar_tecnico", "mant_ot_autoriza_aviso")', APP, "nace concedido (bloqueo)")
        self.assertIn('("mantenciones", "ot_autoriza_aviso"),', APP, "concedido por omisión en la matriz de roles")
        self.assertIn('base["mant_ot_autoriza_aviso"] = (man.get("ot_autoriza_aviso") is None', APP)
        self.assertIn('"ot_autoriza_aviso": {"label"', APP)
        self.assertIn('"tipo": "bloqueo", "icon": "bi-bell"', APP)

    def test_el_aviso_usa_los_receptores_y_no_a_todos_los_superadmins(self):
        i = APP.index("def _ot_aut_notificar_superadmins(a):")
        f = APP[i:i + 3000]
        self.assertIn("for sa in _ot_aut_receptores():", f)
        self.assertNotIn("for sa in _ot_aut_superadmins():", f)

    def test_los_receptores_respetan_la_persona_sobre_el_rol_y_nunca_quedan_vacios(self):
        i = APP.index("def _ot_aut_receptores():")
        f = APP[i:i + 2500]
        self.assertIn("usuario_permisos", f)
        self.assertIn("rol_permisos", f)
        self.assertIn("(ov is not None and not ov) or (ov is None and rolp is not None and not rolp)", f)
        self.assertIn("return elegidos or todos", f, "si se apagara a todos, avisa a todos")

    def test_titulo_del_correo(self):
        self.assertIn("Te piden autorizar algo que no tiene un documento que respalde el cobro", APP)
        self.assertNotIn('"Te piden autorizar algo que no tiene documento"', APP)


if __name__ == "__main__":
    unittest.main()

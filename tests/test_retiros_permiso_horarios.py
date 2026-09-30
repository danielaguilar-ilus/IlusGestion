"""«Horarios y alertas» de Retiros se controla desde Roles (y Permisos individuales).

2026-09-30. Daniel: "no tengo cómo activar esto con los roles... a Juan Espinosa no le
sale". El botón, el modal y sus 5 endpoints exigían el permiso `admin` (Administración →
usuarios/roles), que un jefe de Retiros no tiene. Ahora existe `ret_horarios`
(Roles → Retiros → «Horarios y alertas»), aditivo: los gates aceptan admin OR ret_horarios.

Correr con:  py -m unittest tests.test_retiros_permiso_horarios -v
"""
import os
import re
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _leer(*partes):
    with open(os.path.join(RAIZ, *partes), encoding="utf-8") as fh:
        return fh.read()


class PermisoHorariosRetiros(unittest.TestCase):
    def test_llave_existe_y_se_construye_desde_la_matriz(self):
        app = _leer("app.py")
        self.assertRegex(app, r'\n\s+"ret_horarios",\n')
        self.assertIn('base["ret_horarios"]   = bool(ret.get("horarios"))', app)

    def test_chip_en_la_matriz_de_roles(self):
        app = _leer("app.py")
        self.assertIn('"acciones":["ver","gestionar","monitor","marketing","horarios"]', app)
        self.assertRegex(app, r'"horarios":\s+\{"label": "Horarios y alertas')

    def test_los_cinco_endpoints_aceptan_admin_o_el_permiso(self):
        src = _leer("pickups_module.py")
        for ruta in ('"/retiros/settings"', '"/retiros/excepciones"', '"/retiros/bloqueos/nuevo"',
                     '"/retiros/bloqueos/batch"', '"/retiros/bloqueos/<int:bid>/eliminar"'):
            self.assertRegex(src, r'@app\.route\(%s, methods=\["POST"\]\)\n\s+@_require_horarios\n' % re.escape(ruta), ruta)
        # ...y ninguno de los cinco sigue exigiendo solo admin
        self.assertNotRegex(src, r'@app\.route\("/retiros/(settings|excepciones)"[^\n]*\n\s+@require_permission\("admin"\)')

    def test_boton_y_modal_se_muestran_con_el_permiso(self):
        html = _leer("templates", "retiros", "internal_dashboard.html")
        self.assertEqual(html.count("{% if permissions.admin or permissions.ret_horarios %}"), 3)
        self.assertNotIn("{% if permissions.admin %}", html)


if __name__ == "__main__":
    unittest.main()

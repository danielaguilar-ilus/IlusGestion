"""Base de manifiestos en Excel, controlable desde Usuarios y roles.

2026-10-22. Daniel: "una bajada de información de los manifiestos, de los
envíos... bien ordenadito para que logística pueda hacer sus ediciones... y
déjalo controlable por el front". Mismo patrón que tr_eliminar y
tr_cambiar_estado: flag aditivo, apagado por defecto, admin/superadmin
siempre. Pruebas por texto (app.py pesa demasiado para un ast completo).

Correr con:  py -m unittest tests.test_tr_exportar_base -v
"""
import os
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _leer(*partes):
    with open(os.path.join(RAIZ, *partes), encoding="utf-8") as fh:
        return fh.read()


APP = _leer("app.py")
HTML = _leer("templates", "transporte", "manifiestos.html")


def _funcion(nombre):
    i = APP.index(f"\ndef {nombre}(")
    j = APP.index("\n@app.route(", i)
    return APP[i:j]


class TestElPermisoExiste(unittest.TestCase):
    def test_esta_en_perms_keys_y_en_la_matriz(self):
        self.assertIn('    "tr_exportar_base",\n', APP)
        self.assertIn('"exportar_base"]},', APP)
        self.assertIn('"exportar_base": {"label": "Descargar base de manifiestos (Excel)", "tipo": "bloqueo"', APP)

    def test_se_calcula_desde_la_matriz_y_nace_en_false(self):
        self.assertIn('base["tr_exportar_base"] = bool(tra.get("exportar_base"))', APP)

    def test_no_es_de_los_concedidos_por_omision(self):
        i = APP.index("_PERMS_CONCEDIDOS_POR_OMISION = {")
        j = APP.index("}", i)
        self.assertNotIn("exportar_base", APP[i:j])


class TestLaRutaExigeElPermiso(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.src = _funcion("tr_manifiestos_base_xlsx")

    def test_ruta_y_decorador(self):
        i = APP.index("def tr_manifiestos_base_xlsx(")
        self.assertIn('@app.route("/transporte/manifiestos/base.xlsx")', APP[i - 120:i])
        self.assertIn("@_tr_required", APP[i - 120:i])

    def test_gate_admin_superadmin_o_flag(self):
        self.assertIn('("admin", "superadmin")', self.src)
        self.assertIn('g.permissions.get("tr_exportar_base")', self.src)

    def test_es_solo_lectura(self):
        for prohibido in ("INSERT ", "UPDATE ", "DELETE "):
            self.assertNotIn(prohibido, self.src)

    def test_deja_rastro_en_la_bitacora(self):
        self.assertIn("base_manifiestos_excel", self.src)

    def test_no_mete_tablas_ni_filas_combinadas(self):
        self.assertNotIn("merge_cells", self.src)


class TestElBotonSigueAlPermiso(unittest.TestCase):
    def test_el_boton_solo_se_dibuja_con_permiso(self):
        i = HTML.index('id="btnBaseManifiestos"')
        antes = HTML[max(0, i - 700):i]
        self.assertIn("permissions.tr_exportar_base", antes)
        self.assertIn("is_superadmin", antes)


if __name__ == "__main__":
    unittest.main(verbosity=2)

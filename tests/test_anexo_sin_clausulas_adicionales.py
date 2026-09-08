"""Pruebas aisladas: no importar app.py (su arranque puede ejecutar migraciones)."""
import ast
from pathlib import Path
import unittest

from jinja2 import Environment, FileSystemLoader

ROOT = Path(__file__).resolve().parents[1]


def funciones():
    names = {"_anexo_clausulas_defecto"}
    tree = ast.parse((ROOT / "app.py").read_text(encoding="utf-8"))
    module = ast.Module(body=[n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names], type_ignores=[])
    ns = {}
    exec(compile(module, "anexo_aislado", "exec"), ns)
    return ns


def anexo_base(**extra):
    data = dict(
        numero=999, fecha="08/09/2026",
        proveedor_nombre="Proveedor QA", proveedor_rut="11.111.111-1",
        objetivo_servicio="Servicio para Cliente QA <prueba>",
        cliente_nombre="Cliente QA",
        precio_items=[dict(concepto="Correctiva", monto=90000)],
        fecha_inicio="10/09/2026", fecha_termino="10/09/2026",
        niveles_servicio="Niveles QA", hitos_pago="Hitos QA", alcance_servicio="Alcance QA",
        clausulas_adicionales="",
    )
    data.update(extra)
    return data


class AnexoSinClausulasAdicionalesTest(unittest.TestCase):
    def test_clausulas_defecto_nace_vacia_y_sin_lenguaje_parametrizado(self):
        ns = funciones()
        defecto = ns["_anexo_clausulas_defecto"]()
        self.assertEqual(defecto["clausulas_adicionales"], "")
        for campo in ("niveles_servicio", "hitos_pago", "alcance_servicio"):
            texto = defecto[campo]
            self.assertTrue(texto)
            # El texto parametrizado (plazos/penalidad configurados) ya no existe.
            for prohibido in ("penalidad", "días corridos", "minutos siguientes"):
                self.assertNotIn(prohibido, texto)

    def test_anexo_nuevo_no_muestra_clausulas_adicionales(self):
        env = Environment(loader=FileSystemLoader(ROOT / "templates"), autoescape=True)
        html = env.get_template("ot2/anexo_documento.html").render(
            anexo=anexo_base(), firma=None, productos=[], es_vista_previa=False)
        labels = ["Anexo de Servicios N° 999", "Objetivo del Servicio:",
                  "Duración del Servicio:", "Niveles de Servicio:", "Hitos de Pago:",
                  "Alcance del Servicio:", "Precio", "Total Neto"]
        positions = [html.index(label) for label in labels]
        self.assertEqual(positions, sorted(positions))
        self.assertNotIn("Cláusulas Adicionales", html)
        for unwanted in ("Isabel", "IMPORTCAYU"):
            self.assertNotIn(unwanted, html)
        self.assertIn("&lt;prueba&gt;", html)
        self.assertIn("$90.000", html)

    def test_anexo_legacy_con_clausulas_las_conserva(self):
        """Un anexo viejo (clausulas_adicionales ya escritas en su fila) no
        debe perder su contenido solo porque hoy los anexos nuevos nacen
        sin cláusulas -- el render depende del DATO, no de una bandera."""
        env = Environment(loader=FileSystemLoader(ROOT / "templates"), autoescape=True)
        html = env.get_template("ot2/anexo_documento.html").render(
            anexo=anexo_base(clausulas_adicionales="Primero: texto uno\n\nSegundo: texto dos"),
            firma=None, productos=[], es_vista_previa=False)
        self.assertIn("Cláusulas Adicionales", html)
        self.assertIn("Primero:", html)
        self.assertIn("texto uno", html)


if __name__ == "__main__":
    unittest.main()

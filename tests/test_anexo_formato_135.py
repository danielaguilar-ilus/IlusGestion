"""Pruebas aisladas: no importar app.py (su arranque puede ejecutar migraciones)."""
import ast
import json
from pathlib import Path
import unittest

from jinja2 import Environment, FileSystemLoader

ROOT = Path(__file__).resolve().parents[1]


def funciones():
    names = {"_anexo_formato", "_anexo_clausulas_defecto", "_anexo_dict"}
    tree = ast.parse((ROOT / "app.py").read_text(encoding="utf-8"))
    module = ast.Module(body=[n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names], type_ignores=[])
    ns = {}
    exec(compile(module, "anexo_aislado", "exec"), ns)
    return ns


class Formato135Test(unittest.TestCase):
    def test_version_persistida_y_legacy(self):
        ns = funciones()
        for estado in ("borrador", "enviado", "firmado"):
            for version in (None, "135-v1"):
                item = {"concepto": "Servicio", "monto": 90000}
                if version:
                    item["formato_anexo"] = version
                row = ns["_anexo_dict"]({"estado": estado, "precio_items_json": json.dumps([item])})
                self.assertEqual(ns["_anexo_formato"](row), version or "legacy")

    def test_secciones_productos_y_datos_dinamicos(self):
        ns = funciones()
        env = Environment(loader=FileSystemLoader(ROOT / "templates"), autoescape=True)
        data = dict(formato="135-v1", numero=999, fecha="08/09/2026",
                    proveedor_nombre="Proveedor QA", proveedor_rut="",
                    objetivo_servicio="Servicio para Cliente QA <prueba>",
                    precio_items=[dict(concepto="Correctiva", monto=90000)],
                    fecha_inicio="10/09/2026", fecha_termino="10/09/2026",
                    productos=[dict(sku="SKU-QA", nombre="Equipo QA", cantidad=2)],
                    **ns["_anexo_clausulas_defecto"]())
        html = env.get_template("ot2/anexo_documento.html").render(anexo=data, firma=None)
        labels = ["El presente Anexo", "Objetivo del Servicio:", "Precio:", "Duración del Servicio:", "Niveles de Servicio:", "Hitos de Pago:", "Alcance del Servicio:", "SKU-QA"]
        positions = [html.index(label) for label in labels]
        self.assertEqual(positions, sorted(positions))
        for unwanted in ("Isabel", "IMPORTCAYU", "Cláusulas Adicionales", "Indemnidad", "Limitación de responsabilidad"):
            self.assertNotIn(unwanted, html)
        self.assertIn("&lt;prueba&gt;", html)
        self.assertIn("$90.000", html)
        self.assertEqual(data["clausulas_adicionales"], "")


if __name__ == "__main__":
    unittest.main()

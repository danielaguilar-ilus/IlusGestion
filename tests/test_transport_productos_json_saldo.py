"""FASE 0 (plan de mejora integral de Transporte, 2026-07-31): caracteriza
_tr_parse_productos_json -- la funcion que convierte
transport_commitments.productos_json en las lineas que muestran los
modales de tracking y alimentan la Fase 2 (estado por producto).

Por que existe esta prueba: la noche del 2026-07-31 se encontro que
transport_commitment_lines solo guarda lineas ZZ (flete), nunca
productos reales -- _tr_parse_productos_json es la fuente REAL desde
entonces. Si alguien la rompe (por ejemplo, vuelve a la formula
saldo = cantidad - despachada de 2 terminos, en vez de derivar
cant_despachada = cantidad - saldo), el modal vuelve a mostrar
"0/1 pendiente, saldo 1" en documentos que el ERP ya marca como
completamente despachados -- el bug exacto que reporto Daniel con la
factura BLV 22719.

No usa pytest (no esta instalado en este entorno) -- unittest estandar,
mismo patron que el resto de tests/test_*.py: extraer la funcion via AST
en vez de importar app.py completo (evita arrastrar Flask/MySQL/ERP).
Ejecutar con: py tests/test_transport_productos_json_saldo.py
"""
import ast
import json
import pathlib
import unittest


def load_parser():
    source = pathlib.Path("app.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    function = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef)
        and node.name == "_tr_parse_productos_json"
    )
    namespace = {"json": json, "print": lambda *a, **k: None}
    module = ast.Module(body=[function], type_ignores=[])
    exec(compile(module, "app.py", "exec"), namespace)
    return namespace["_tr_parse_productos_json"]


class ParseProductosJsonTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.parse = staticmethod(load_parser())

    def test_none_o_vacio_devuelve_lista_vacia(self):
        self.assertEqual(self.parse(None), [])
        self.assertEqual(self.parse(""), [])

    def test_json_invalido_no_revienta_devuelve_vacio(self):
        self.assertEqual(self.parse("{esto no es json"), [])

    def test_excluye_lineas_zz_de_servicio(self):
        raw = json.dumps([
            {"koprct": "ZZENVIO", "nokopr": "Despacho", "cantidad": 1, "saldo": 0},
            {"koprct": "SKU-1", "nokopr": "Producto real", "cantidad": 2, "saldo": 0},
        ])
        out = self.parse(raw)
        self.assertEqual([l["koprct"] for l in out], ["SKU-1"])

    def test_documento_ya_despachado_saldo_cero_no_queda_pendiente(self):
        # Caso real BLV 22719 (2026-07-31): el ERP ya reporta saldo=0 para
        # las 4 lineas -- el modal NO puede seguir mostrando "0/1 pendiente".
        raw = json.dumps([
            {"koprct": "SKU-1", "nokopr": "Producto", "cantidad": 1, "saldo": 0},
        ])
        out = self.parse(raw)
        self.assertEqual(out[0]["saldo"], 0)
        self.assertEqual(out[0]["cant_despachada"], 1)  # cantidad - saldo

    def test_documento_con_saldo_pendiente_real(self):
        raw = json.dumps([
            {"koprct": "SKU-1", "nokopr": "Producto", "cantidad": 10, "saldo": 4},
        ])
        out = self.parse(raw)
        self.assertEqual(out[0]["saldo"], 4)
        self.assertEqual(out[0]["cant_despachada"], 6)

    def test_saldo_ausente_se_trata_como_cero_no_como_pendiente_total(self):
        # productos_json puede venir sin la clave "saldo" (snapshots viejos
        # del cubicador, anteriores a este fix) -- debe leerse como
        # "sin saldo pendiente conocido", NUNCA como "0 despachado".
        raw = json.dumps([
            {"koprct": "SKU-1", "nokopr": "Producto", "cantidad": 3},
        ])
        out = self.parse(raw)
        self.assertEqual(out[0]["saldo"], 0)
        self.assertEqual(out[0]["cant_despachada"], 3)

    def test_sku_vacio_se_descarta(self):
        raw = json.dumps([
            {"koprct": "", "nokopr": "Sin sku", "cantidad": 1, "saldo": 1},
            {"koprct": "SKU-1", "nokopr": "Valido", "cantidad": 1, "saldo": 1},
        ])
        out = self.parse(raw)
        self.assertEqual([l["koprct"] for l in out], ["SKU-1"])

    def test_no_items_no_dict_se_ignoran_sin_reventar(self):
        raw = json.dumps(["esto no es un dict", 123, None])
        self.assertEqual(self.parse(raw), [])


if __name__ == "__main__":
    unittest.main()

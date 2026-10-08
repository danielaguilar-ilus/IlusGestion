"""El espejo del navegador (static/ot_finanzas.js) da EXACTAMENTE la misma cuenta que _ot_finanzas de app.py.

Daniel 2026-10-07: una sola cuenta en todas partes. Si alguien cambia la regla en un lado y no en el otro, esto falla.
Necesita `node` instalado (se salta si no está). Correr con:  py -m unittest tests.test_ot_finanzas_js_espejo
"""
import json
import os
import shutil
import subprocess
import tempfile
import unittest

from tests.test_ot_finanzas_modelo import F

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
JS = os.path.join(RAIZ, "static", "ot_finanzas.js")

BASE = {"modalidad_cobro": "pagado", "cubierto_por": "cliente", "tipo": "instalacion", "cliente_id": 5,
        "contrato_id": None, "costo": None, "zz_monto": None, "zz_codigo": None, "zz_envio_monto": None,
        "valor_origen": None, "costo_proveedor": None, "costo_despacho": None}
REP = {"costo": 20000, "por_origen": {"bodega": 20000, "compra": 0, "manual": 0}, "n_sin_costo": 1}
CASOS = [
    dict(modalidad_cobro="garantia", costo=200000, zz_monto=1, zz_codigo="ZZRETIRO", valor_origen="zz",
         costo_proveedor=130000, costo_despacho=70000),
    dict(zz_monto=150000, valor_origen="zz", zz_envio_monto=30000, costo_proveedor=100000, costo_despacho=20000),
    dict(zz_monto=80000, valor_origen="estimado", costo=80000, costo_proveedor=50000),
    dict(costo=100000, costo_proveedor=60000),
    dict(zz_monto=100000, valor_origen="supuesto", costo_proveedor=40000),
    dict(zz_monto=1, zz_codigo="ZZRETIRO", valor_origen="zz", costo_proveedor=130000, costo_despacho=70000),
    dict(zz_monto=0, valor_origen="zz", costo_proveedor=30000),
    dict(zz_monto=100000, valor_origen="zz", costo_proveedor=50000),
    dict(zz_monto=100000, valor_origen="zz", zz_envio_monto=20000, costo_proveedor=50000),
    dict(zz_monto=100000, valor_origen="zz"),
    dict(zz_monto=100000, valor_origen="zz", costo_proveedor=95000),
    dict(zz_monto=100000, valor_origen="zz", costo_proveedor=120000),
    dict(zz_monto=100000, valor_origen="zz", costo=150000, costo_proveedor=50000),
    dict(cliente_id=None, costo=40000),
    dict(tipo="preventiva", contrato_real=1, contrato_id=7, costo=60000, costo_proveedor=30000),
    dict(tipo="preventiva", contrato_real=0, contrato_id=7, costo=60000, costo_proveedor=30000),
    dict(tipo="preventiva", contrato_real=1, zz_monto=50000, valor_origen="zz", costo_proveedor=20000),
    dict(modalidad_cobro="sin_costo", costo_proveedor=10000),
    dict(cubierto_por="garantia", valorizado_clp=180000, valorizado_fuente="cotizador", costo_proveedor=100000),
    dict(modalidad_cobro="garantia"),
    dict(zz_monto=100000, valor_origen="zz", costo_proveedor=40000, costo_despacho=0, _rep=True),
    dict(zz_monto=99999, valor_origen="cotizacion", zz_envio_monto=12345, costo_proveedor=33333, costo_despacho=4444),
    # 2026-10-07: motivos del $0 (regalía / arriendo-leasing) y "interno" CON cliente (ya no exime).
    dict(modalidad_cobro="sin_costo", cobro_cero_motivo="regalia", costo_proveedor=10000),
    dict(modalidad_cobro="sin_costo", cobro_cero_motivo="arriendo_leasing", costo_proveedor=10000, valorizado_clp=50000,
         valorizado_fuente="contrato"),
    dict(modalidad_cobro="pagado", cobro_cero_motivo="garantia", costo_proveedor=20000),
    dict(modalidad_cobro="interno", cliente_id=5, costo_proveedor=0, zz_monto=30000, valor_origen="manual"),
    dict(modalidad_cobro="interno", cliente_id=None, costo=40000),
    # 2026-10-08: el «Precio al cliente» suelto es un precio anotado, no un cobro (con y sin respaldo).
    dict(costo=200000, costo_proveedor=200000),
    dict(costo=200000, costo_proveedor=50000, valor_origen="doc_total"),
    dict(costo=200000, costo_proveedor=50000, valor_origen="manual", zz_envio_monto=20000, costo_despacho=5000),
    dict(zz_monto=1, zz_codigo="ZZRETIRO", valor_origen="zz", costo=200000, costo_proveedor=1000),
    dict(costo=90000, valor_origen="estimado", costo_proveedor=1000),
    dict(tipo="revision_interna", cliente_id=9, zz_monto=10000, valor_origen="zz", costo_proveedor=5000),
]
CAMPOS = ("cobertura", "cobra", "clase", "label", "a_pagar_proveedor", "avisos", "precio_anotado")


@unittest.skipUnless(shutil.which("node"), "node no está instalado")
class TestEspejo(unittest.TestCase):
    def test_python_y_navegador_dan_lo_mismo(self):
        entradas = []
        for c in CASOS:
            c = dict(c)
            rep = REP if c.pop("_rep", False) else None
            v = dict(BASE, **c)
            entradas.append({"v": v, "rep": rep})
        with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False, encoding="utf-8") as fh:
            fh.write("const m = require(%s);\n" % json.dumps(JS))
            fh.write("const casos = %s;\n" % json.dumps(entradas))
            fh.write("process.stdout.write(JSON.stringify(casos.map(c => m.finanzas(c.v, c.rep))));\n")
            script = fh.name
        try:
            js = json.loads(subprocess.run(["node", script], capture_output=True, text=True, encoding="utf-8",
                                           check=True).stdout)
        finally:
            os.unlink(script)
        for i, (e, j) in enumerate(zip(entradas, js)):
            rep = e["rep"]
            kw = {k: val for k, val in e["v"].items()}
            if rep:
                kw["rep"] = rep
            p = F(**{k: val for k, val in kw.items() if k not in ("cliente_id",) or True})
            for campo in CAMPOS:
                self.assertEqual(p[campo], j[campo], f"caso {i} campo {campo}")
            for grupo, claves in (("cobre", ("servicio", "despacho", "total", "fuente", "hay")),
                                  ("me_cobraron", ("tecnico", "despacho", "repuestos", "total", "falta_tecnico",
                                                   "falta_despacho")),
                                  ("queda", ("servicio", "despacho", "repuestos", "total", "mostrar")),
                                  ("valorizado", ("monto", "fuente"))):
                for k in claves:
                    self.assertEqual(p[grupo][k], j[grupo][k], f"caso {i} {grupo}.{k}")
            if p["queda"]["pct"] is None:
                self.assertIsNone(j["queda"]["pct"], f"caso {i} pct")
            else:
                self.assertAlmostEqual(p["queda"]["pct"], j["queda"]["pct"], delta=0.11, msg=f"caso {i} pct")


if __name__ == "__main__":
    unittest.main()

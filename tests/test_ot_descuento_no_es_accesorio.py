# -*- coding: utf-8 -*-
"""2026-10-23 · OT de Isabel: «el descuento por ventas aún sigue entrampando la OT» (firma: «DESCUENTO VENTAS: falta indicar si llegó»).

El Catálogo puede clasificar la línea «DESCUENTO VENTAS» (SKU «DE») como «accesorio», y entonces la OT le exigía «¿Llegó?». Un descuento o
un servicio (ZZ…, DE) no es un producto: `_ot_accesorios_ids` los saca con la misma regla única `_sku_es_servicio`.
"""
import os
import re
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
with open(os.path.join(RAIZ, "app.py"), encoding="utf-8") as _fh:
    APP = _fh.read()


class TestDescuentoNoEsAccesorio(unittest.TestCase):
    def test_los_accesorios_no_incluyen_servicios_ni_descuentos(self):
        i = APP.index("def _ot_accesorios_ids(vid):")
        f = APP[i:i + 3200]
        self.assertIn("SELECT DISTINCT m.id, m.sku, m.nombre FROM (", f)
        self.assertIn("filas = [r for r in filas if not _sku_es_servicio(r.get(\"sku\"), r.get(\"nombre\"))]", f)
        self.assertLess(f.index("_sku_es_servicio"), f.index("_inc_clasificacion_skus_batch"))

    def test_la_regla_unica_reconoce_el_descuento(self):
        i = APP.index("_SERVICIO_NOMBRE_RE = ")
        j = APP.index("def _sku_es_servicio(sku, nombre=\"\"):")
        ns = {"re": re}
        exec(APP[i:APP.index("\n\n\n", j)], ns)          # solo la regla pura (regex + función), sin cargar la app
        es = ns["_sku_es_servicio"]
        self.assertTrue(es("DE", "DESCUENTO VENTAS"))
        self.assertTrue(es("", "DESCUENTO VENTAS"))
        self.assertTrue(es("ZZENVIO", "Despacho"))
        self.assertFalse(es("ILUS-TRO-01", "Trotadora ILUS"))


if __name__ == "__main__":
    unittest.main()

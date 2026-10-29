"""Las OT que NO se cobran no piden factura ni alarman por saldo — 2026-10-29 (Daniel, OT-2026-00241 y OT-2026-00182).

OT-241: garantía con solo la nota de venta VD 10658: el recorrido decía «Falta la factura» / «agrega la factura o boleta, o pide la autorización».
OT-182: «Mantención de contrato · no se cobra» y aun así una alarma roja «Esta OT cobra más de lo que le queda a sus documentos».

La decisión de la puerta se prueba en tests/test_ot_puerta_documento.py (TestGarantiaNoPideFactura). Aquí: el recorrido, el candado de cierre, el
saldo del panorama y el motor de pantalla (static/ot_fin_motor.js), sobre el código real.
"""
import os
import re
import shutil
import subprocess
import tempfile
import unittest

from tests import _fuente_app as FA

MOTOR = os.path.join(FA.RAIZ, "static", "ot_fin_motor.js")


class TestRecorrido(unittest.TestCase):
    def setUp(self):
        self.src = FA.fuente_funcion("ot_api_recorrido")

    def test_una_ot_que_no_se_cobra_con_nota_de_venta_no_pide_factura(self):
        i_ref = self.src.index('elif nv and cob != "cobra":')
        i_falta = self.src.index("Solo hay nota de venta (")
        self.assertLess(i_ref, i_falta)                                            # la rama de «no se cobra» va ANTES de la que pide la factura
        trozo = self.src[i_ref:i_falta]
        self.assertIn('"estado": "no_aplica"', trozo)
        self.assertIn("Esta OT no se cobra: no lleva factura.", trozo)

    def test_el_cartel_de_falta_factura_solo_vale_para_una_ot_que_se_cobra(self):
        self.assertIn('"falta_factura": bool(nv and not cobro_doc and cob == "cobra")', self.src)

    def test_una_ot_que_se_cobra_sigue_pidiendo_la_factura(self):
        self.assertIn("es provisoria, falta la factura para poder cerrar.", self.src)


class TestCandadoDeCierre(unittest.TestCase):
    def test_la_garantia_la_regalia_y_el_arriendo_no_piden_documento_del_cliente(self):
        src = FA.texto()
        i = src.index("_exenta_doc_cliente = (")
        trozo = src[i:i + 400]
        for cob in ('"contrato"', '"cubierta"', '"garantia"', '"sin_costo"', '"regalia"', '"arriendo_leasing"'):
            self.assertIn(cob, trozo, cob)

    def test_los_dos_candados_del_documento_respetan_la_exencion(self):
        src = FA.texto()
        self.assertGreaterEqual(src.count("and not _exenta_doc_cliente"), 2)       # SIN_FACTURA y SOLO_NOTA_VENTA


class TestSaldoDelPanorama(unittest.TestCase):
    def test_una_ot_que_no_se_cobra_no_compara_su_zz_guardado_con_el_saldo(self):
        src = FA.fuente_funcion("ot_api_panorama")
        i_cob = src.index('_ot_cobertura(v) == "cobra"')
        i_cobro = src.index("cobro_s = (")
        self.assertLess(i_cob, i_cobro)
        self.assertIn('else {"servicio": 0, "despacho": 0})', src[i_cobro:i_cobro + 400])

    def test_el_panorama_trae_las_columnas_de_la_cobertura(self):
        src = FA.fuente_funcion("ot_api_panorama")
        self.assertIn('_ot_fin_cols_sql("v")', src)
        self.assertIn("v.cobro_cero_motivo", src)

    def test_la_logica_del_saldo_con_cobro_cero_nunca_excede(self):
        import ot_saldo_servicio as sv
        comb = {"servicio": {"hay_lineas": True, "total": 768471, "usado": 0, "saldo": 768471},
                "despacho": {"hay_lineas": False, "total": 0, "usado": 0, "saldo": 0}}
        self.assertTrue(sv.evaluar_pedido(comb, {"servicio": 0, "despacho": 0})["ok"])
        self.assertFalse(sv.evaluar_pedido(comb, {"servicio": 913933, "despacho": 0})["ok"])     # lo que se veía en la OT-182


@unittest.skipUnless(shutil.which("node"), "Node no está disponible")
class TestMotorDePantalla(unittest.TestCase):
    def test_el_motor_dice_referencia_de_la_venta_cuando_la_ot_no_se_cobra(self):
        with open(MOTOR, encoding="utf-8") as fh:
            js = fh.read()
        i = js.index("var _noSeCobra = false;")
        j = js.index("/* SALDO por línea de servicio")
        trozo = js[i:j]
        self.assertIn("Referencia de la venta: esta OT no se cobra, no lleva factura", trozo)
        self.assertIn("Promesa de cobro: falta la factura", trozo)                  # y para una OT que se cobra, igual que antes
        self.assertIn("_noSeCobra = !!(fin && fin.cobra === false);", js)
        # Ejecuta la función de verdad con las dos situaciones.
        arnes = (
            "const fs=require('fs');const src=fs.readFileSync(process.argv[2],'utf8');"
            "const f=new Function('sumaMontos','clp',src+';return {set(v){_noSeCobra=v;},ap:aporteDoc};');"
            "const api=f(a=>0,n=>'$'+n);const nv={categoria:'nota_venta',es_cobro:true,origen:'erp',lineas:{servicio:[],despacho:[]}};"
            "api.set(false);const a=api.ap(nv);api.set(true);const b=api.ap(nv);"
            "console.log(JSON.stringify({cobra:a,garantia:b}));")
        k = js.index("function aporteDoc(d) {")
        fin = js.index("/* SALDO por línea de servicio")
        cuerpo = "var _noSeCobra = false;\n" + js[k:fin]
        with tempfile.TemporaryDirectory() as d:
            s, h = os.path.join(d, "m.js"), os.path.join(d, "h.js")
            with open(s, "w", encoding="utf-8") as fh:
                fh.write(cuerpo)
            with open(h, "w", encoding="utf-8") as fh:
                fh.write(arnes)
            r = subprocess.run([shutil.which("node"), h, s], capture_output=True, text=True, encoding="utf-8", timeout=60)
            self.assertEqual(r.returncode, 0, r.stderr)
        import json
        o = json.loads(r.stdout.strip().splitlines()[-1])
        self.assertEqual(o["cobra"]["cls"], "nv")
        self.assertEqual(o["garantia"]["cls"], "no")
        self.assertIn("no lleva factura", o["garantia"]["txt"])


if __name__ == "__main__":
    unittest.main()

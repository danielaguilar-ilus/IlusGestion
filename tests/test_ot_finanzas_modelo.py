"""Modelo único de finanzas de una OT: "Cobré − Me cobraron = Queda" (+ Valorizado aparte). Daniel, 2026-10-07.

Casos con números reales, empezando por la OT-2026-00201 que mostraba tres resultados distintos.
Sin BD ni Flask: se ejecuta _ot_finanzas de app.py (extraída con ast).
Correr con:  py -m unittest tests.test_ot_finanzas_modelo
"""
import ast
import os
import unittest
from datetime import datetime, timedelta, timezone, time as dt_time

from tests.test_incidencias_bajas import _codigo_y_arbol

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FUNCS = ("_ot_es_interna", "_ot_cobertura", "_ot_fin_num", "_ot_fin_clp", "_ot_finanzas")
CONSTS = ("_OT_FIN_ORIGENES_NO_COBRO", "_OT_FIN_FUENTE_COBRO", "_OT_FIN_ZZ_NO_SERVICIO",
          "_OT_FIN_COBERTURA_TXT", "_OT_FIN_UMBRAL_BAJO")


def _cargar():
    _, arbol = _codigo_y_arbol()
    amb = {"datetime": datetime, "timedelta": timedelta, "timezone": timezone, "dt_time": dt_time}
    for nodo in arbol.body:
        if isinstance(nodo, ast.Assign) and len(nodo.targets) == 1 and isinstance(nodo.targets[0], ast.Name) \
                and nodo.targets[0].id in CONSTS:
            exec(compile(ast.Module(body=[nodo], type_ignores=[]), "<app>", "exec"), amb)
        if isinstance(nodo, ast.FunctionDef) and nodo.name in FUNCS:
            nodo.decorator_list = []
            exec(compile(ast.Module(body=[nodo], type_ignores=[]), "<app>", "exec"), amb)
    faltan = [n for n in FUNCS + CONSTS if n not in amb]
    assert not faltan, faltan
    return amb


AMB = None
ANTES = datetime(2026, 9, 20, 10, 0)
DESPUES = datetime(2026, 10, 20, 10, 0)


def F(**kw):
    global AMB
    if AMB is None:
        AMB = _cargar()
    base = {"modalidad_cobro": "pagado", "cubierto_por": "cliente", "tipo": "instalacion", "cliente_id": 5,
            "contrato_id": None, "created_at": DESPUES, "costo": None, "zz_monto": None, "zz_codigo": None,
            "zz_envio_monto": None, "valor_origen": None, "costo_proveedor": None, "costo_despacho": None}
    rep = kw.pop("rep", None)
    base.update(kw)
    return AMB["_ot_finanzas"](base, rep)


class TestOT201(unittest.TestCase):
    """La OT que lo destapó: garantía, $200.000 en «Precio al cliente», $1 en ZZRETIRO, técnico 130.000 + despacho 70.000."""

    def setUp(self):
        self.r = F(modalidad_cobro="garantia", costo=200000, zz_monto=1, zz_codigo="ZZRETIRO",
                   valor_origen="zz", costo_proveedor=130000, costo_despacho=70000)

    def test_una_sola_lectura(self):
        r = self.r
        self.assertEqual(r["cobertura"], "garantia")
        self.assertEqual(r["cobre"]["total"], 0)
        self.assertEqual(r["me_cobraron"]["total"], 200000)
        self.assertEqual(r["queda"]["total"], -200000)
        self.assertEqual(r["valorizado"]["monto"], 200000)
        self.assertEqual(r["valorizado"]["fuente"], "dato_antiguo")
        self.assertEqual(r["clase"], "info", "una garantía no es una 'pérdida' roja: es lo que nos costó")
        self.assertIn("Nos costó $200.000", r["frase"])

    def test_avisa_la_linea_que_no_es_de_servicio(self):
        self.assertTrue(any("ZZRETIRO" in a and "no es de servicio" in a for a in self.r["avisos"]))


class TestCobra(unittest.TestCase):
    def test_con_documento(self):
        r = F(zz_monto=150000, valor_origen="zz", zz_envio_monto=30000, costo_proveedor=100000, costo_despacho=20000)
        self.assertEqual(r["cobre"]["total"], 180000)
        self.assertEqual(r["me_cobraron"]["total"], 120000)
        self.assertEqual(r["queda"]["total"], 60000)
        self.assertEqual(r["queda"]["pct"], 33.3)
        self.assertEqual(r["clase"], "ok")
        self.assertEqual(r["cobre"]["fuente"], "línea del documento")
        self.assertEqual(r["a_pagar_proveedor"], 120000)

    def test_un_estimado_no_es_un_cobro(self):
        r = F(zz_monto=80000, valor_origen="estimado", costo=80000, costo_proveedor=50000)
        self.assertEqual(r["cobre"]["total"], 0)
        self.assertEqual(r["clase"], "gris")
        self.assertEqual(r["label"], "Falta lo que cobraste")
        self.assertEqual(r["valorizado"]["monto"], 80000)

    def test_sin_zz_usa_el_precio_al_cliente_marcado(self):
        r = F(costo=100000, costo_proveedor=60000)
        self.assertEqual(r["cobre"]["total"], 100000)
        self.assertTrue(r["cobre"]["fuente"].startswith("precio al cliente"))
        self.assertTrue(any("no separa servicio y despacho" in a for a in r["avisos"]))

    def test_escrito_a_mano_cuenta_como_cobro_tenga_o_no_sugerencia(self):
        for origen in ("supuesto", "manual"):
            r = F(zz_monto=100000, valor_origen=origen, costo_proveedor=40000)
            self.assertEqual(r["cobre"]["total"], 100000, origen)
            self.assertEqual(r["cobre"]["fuente"], "escrito a mano")

    def test_zzretiro_nunca_es_cobro_del_servicio(self):
        r = F(zz_monto=1, zz_codigo="ZZRETIRO", valor_origen="zz", costo_proveedor=130000, costo_despacho=70000)
        self.assertEqual(r["cobre"]["total"], 0)
        self.assertEqual(r["label"], "Falta lo que cobraste")

    def test_cobro_declarado_en_cero(self):
        r = F(zz_monto=0, valor_origen="zz", costo_proveedor=30000)
        self.assertEqual(r["label"], "Cobro declarado en $0")
        self.assertEqual(r["clase"], "rojo")

    def test_sin_despacho_no_falta_nada(self):
        r = F(zz_monto=100000, valor_origen="zz", costo_proveedor=50000)
        self.assertFalse(r["me_cobraron"]["falta_despacho"])
        self.assertEqual(r["queda"]["total"], 50000)
        self.assertEqual(r["clase"], "ok")

    def test_despacho_cobrado_sin_su_costo(self):
        r = F(zz_monto=100000, valor_origen="zz", zz_envio_monto=20000, costo_proveedor=50000)
        self.assertTrue(r["me_cobraron"]["falta_despacho"])
        self.assertEqual(r["label"], "Falta el costo del despacho")
        self.assertFalse(r["queda"]["mostrar"])

    def test_cero_declarado_no_es_falta(self):
        r = F(zz_monto=100000, valor_origen="zz", costo_proveedor=0)
        self.assertFalse(r["me_cobraron"]["falta_tecnico"])
        self.assertEqual(r["queda"]["total"], 100000)

    def test_tecnico_sin_declarar(self):
        r = F(zz_monto=100000, valor_origen="zz")
        self.assertEqual(r["label"], "Falta lo que te cobró el técnico")

    def test_repuestos_suman_a_me_cobraron_pero_no_a_pagar_al_proveedor(self):
        r = F(zz_monto=100000, valor_origen="zz", costo_proveedor=40000, costo_despacho=0,
              rep={"costo": 20000, "por_origen": {"bodega": 20000, "compra": 0, "manual": 0}, "n_sin_costo": 0})
        self.assertEqual(r["me_cobraron"]["total"], 60000)
        self.assertEqual(r["a_pagar_proveedor"], 40000)
        self.assertEqual(r["queda"]["total"], 40000)

    def test_margen_bajo_y_perdida(self):
        self.assertEqual(F(zz_monto=100000, valor_origen="zz", costo_proveedor=95000)["clase"], "bajo")
        self.assertEqual(F(zz_monto=100000, valor_origen="zz", costo_proveedor=120000)["clase"], "rojo")

    def test_precio_al_cliente_distinto_de_lo_cobrado_avisa(self):
        r = F(zz_monto=100000, valor_origen="zz", costo=150000, costo_proveedor=50000)
        self.assertEqual(r["cobre"]["total"], 100000, "manda lo cobrado")
        self.assertTrue(any("no coincide" in a for a in r["avisos"]))


class TestNoSeCobra(unittest.TestCase):
    def test_interno_sin_costo_del_tecnico_propio(self):
        r = F(cliente_id=None, costo=40000)
        self.assertEqual(r["cobertura"], "interno")
        self.assertEqual(r["cobre"]["total"], 0)
        self.assertEqual(r["clase"], "info", "el técnico propio no se le paga aparte: no 'falta'")
        self.assertEqual(r["valorizado"], {"monto": 40000, "fuente": "interno"})

    def test_contrato_real_y_preventiva(self):
        r = F(tipo="preventiva", contrato_real=1, contrato_id=7, costo=60000, costo_proveedor=30000)
        self.assertEqual(r["cobertura"], "contrato")
        self.assertEqual(r["cobre"]["total"], 0)
        self.assertEqual(r["queda"]["total"], -30000)

    def test_contenedor_de_documentos_no_es_contrato(self):
        r = F(tipo="preventiva", contrato_real=0, contrato_id=7, costo=60000, costo_proveedor=30000)
        self.assertEqual(r["cobertura"], "cobra")

    def test_correctiva_de_cliente_con_contrato_se_cobra(self):
        self.assertEqual(F(tipo="correctiva", contrato_real=1, contrato_id=7)["cobertura"], "cobra")

    def test_preventiva_de_contrato_facturada_se_cobra(self):
        r = F(tipo="preventiva", contrato_real=1, zz_monto=50000, valor_origen="zz", costo_proveedor=20000)
        self.assertEqual(r["cobertura"], "cobra")

    def test_visita_tipo_garantia_antigua(self):
        self.assertEqual(F(tipo="garantia")["cobertura"], "garantia")

    def test_cortesia(self):
        self.assertEqual(F(modalidad_cobro="sin_costo", costo_proveedor=10000)["cobertura"], "sin_costo")

    def test_regalia_y_arriendo_leasing_se_distinguen_por_el_motivo(self):
        """Daniel 2026-10-07: los únicos motivos de $0 son Garantía, Regalía y Arriendo o leasing."""
        r = F(modalidad_cobro="sin_costo", cobro_cero_motivo="regalia", costo_proveedor=10000)
        self.assertEqual((r["cobertura"], r["cobertura_txt"], r["cobre"]["total"]),
                         ("regalia", "Regalía: no se le cobra", 0))
        r = F(modalidad_cobro="sin_costo", cobro_cero_motivo="arriendo_leasing", costo_proveedor=10000)
        self.assertEqual((r["cobertura"], r["cobertura_txt"]),
                         ("arriendo_leasing", "Arriendo o leasing: incluido en el arriendo"))
        self.assertIn("Arriendo o leasing · nos costó $10.000", r["label"])
        self.assertEqual(F(modalidad_cobro="pagado", cobro_cero_motivo="garantia")["cobertura"], "garantia")

    def test_interno_con_cliente_ya_no_es_interno(self):
        """2026-10-07: la exención por trabajo interno es SOLO sin cliente."""
        self.assertEqual(F(modalidad_cobro="interno", cliente_id=5, costo_proveedor=0)["cobertura"], "cobra")
        self.assertEqual(F(modalidad_cobro="interno", cliente_id=None)["cobertura"], "interno")

    def test_garantia_por_cubierto_por_tambien_cuenta(self):
        self.assertEqual(F(cubierto_por="garantia")["cobertura"], "garantia")

    def test_valorizado_propio_manda(self):
        r = F(modalidad_cobro="garantia", costo=200000, valorizado_clp=180000, valorizado_fuente="cotizador",
              costo_proveedor=100000)
        self.assertEqual(r["valorizado"], {"monto": 180000, "fuente": "cotizador"})

    def test_garantia_sin_costo_del_tecnico_lo_pide(self):
        r = F(modalidad_cobro="garantia")
        self.assertEqual(r["label"], "Falta lo que te cobró el técnico")


class TestComparacionYCopia(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with open(os.path.join(RAIZ, "app.py"), encoding="utf-8") as fh:
            cls.app = fh.read().replace(chr(13) + chr(10), chr(10))

    def test_rutas_solo_superadmin(self):
        for nombre in ("ot2_finanzas_modelo", "ot2_api_finanzas_modelo_copiar"):
            i = self.app.index(f"def {nombre}(")
            self.assertIn("_ot_fin_es_superadmin()", self.app[i:i + 900], nombre)

    def test_la_copia_solo_llena_un_casillero_vacio(self):
        i = self.app.index("def ot2_api_finanzas_modelo_copiar(")
        cuerpo = self.app[i:i + 3000]
        self.assertIn("UPDATE mant_visitas SET valorizado_clp=costo, valorizado_fuente=%s, updated_at=updated_at ", cuerpo)
        self.assertIn("INSERT INTO mant_logs", cuerpo, "la constancia va en la misma transacción")
        self.assertIn(" WHERE id=%s AND valorizado_clp IS NULL AND costo > 0", cuerpo)
        self.assertIn('!= "COPIAR"', cuerpo)

    def test_columnas_nuevas_y_ocultas_en_el_pdf_publico(self):
        self.assertIn('("valorizado_clp",    "DECIMAL(12,2) NULL', self.app)
        i = self.app.index('"finanzas_at", "finanzas_por",\n                   "valorizado_clp", "valorizado_fuente")')
        self.assertGreater(i, 0)


if __name__ == "__main__":
    unittest.main()

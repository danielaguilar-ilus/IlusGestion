"""Facturación de proveedor conectada al motor único (_ot_finanzas). Daniel, 2026-10-07.

"Me preocupa que también esto esté sufriendo de los varios cálculos que tenía el sistema. Conectémonos al motor."

Lo que fijan estas pruebas, con casos de números fijos (garantía $0, sin despacho, repuestos que NO entran a lo que se paga,
técnico interno):
  · Lo que se le paga al proveedor en una fila (Facturas de proveedor, detalle del lote, Facturación del mes y sus Excel)
    es el `a_pagar_proveedor` de _ot_finanzas: técnico + despacho, SIN repuestos. No hay fórmula propia.
  · Los totales de la pantalla (_mfp_resumen_filas, _facprov_datos) son la suma de lo que dice el motor, OT por OT.
  · El espejo del navegador (static/facprov_inteligente.js) suma EXACTAMENTE igual que el servidor y su sugerencia calza.
  · Ya no queda `float(...costo_proveedor...)` + `float(...costo_despacho...)` armando un pago a mano en esas rutas.

Sin BD ni Flask: las funciones se extraen de app.py con ast.
Correr con:  py -m unittest tests.test_facprov_motor
"""
import json
import os
import re
import shutil
import subprocess
import tempfile
import unittest

from tests.test_incidencias_repuesto_tercera_fuente import _fuente_de
from tests.test_ot_finanzas_lectores import V, _amb

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
JS = os.path.join(RAIZ, "static", "facprov_inteligente.js")

REP_15K = {"costo": 15000.0, "por_origen": {"bodega": 15000.0, "compra": 0.0, "manual": 0.0}, "n_sin_costo": 0}

# (nombre, fila de mant_visitas)
CASOS = {
    "normal": dict(zz_monto=150000, valor_origen="zz", zz_envio_monto=30000, costo_proveedor=100000, costo_despacho=20000),
    "garantia_cero": dict(modalidad_cobro="garantia", costo=200000, zz_monto=1, zz_codigo="ZZRETIRO", valor_origen="zz",
                          costo_proveedor=130000, costo_despacho=70000),
    "sin_despacho": dict(zz_monto=100000, valor_origen="zz", costo_proveedor=60000),
    "despacho_cobrado_sin_costo": dict(zz_monto=100000, valor_origen="zz", zz_envio_monto=20000, costo_proveedor=60000),
    "falta_tecnico": dict(zz_monto=100000, valor_origen="zz", costo_despacho=10000),
    "regalia": dict(modalidad_cobro="sin_costo", cobro_cero_motivo="regalia", costo_proveedor=40000),
    "interno_sin_cliente": dict(cliente_id=None, costo=40000),
    "perdida": dict(zz_monto=100000, valor_origen="zz", costo_proveedor=120000),
}


def _fila(i, **kw):
    return _amb()["_mfp_fila_ot"](V(id=i, numero_ot=f"OT-{i}", **kw))


class TestPagoSaleDelMotor(unittest.TestCase):
    def test_lo_que_se_paga_es_a_pagar_proveedor(self):
        a = _amb()
        for nombre, kw in CASOS.items():
            v = V(**kw)
            fin = a["_ot_finanzas"](v, None)
            d = a["_mfp_fila_ot"](v)
            self.assertEqual(d["sugerido"], fin["a_pagar_proveedor"], nombre)
            self.assertEqual(d["servicio"] + d["despacho"], d["sugerido"], nombre)
            self.assertEqual(d["servicio"], float(fin["me_cobraron"]["tecnico"] or 0), nombre)
            self.assertEqual(d["despacho"], float(fin["me_cobraron"]["despacho"] or 0), nombre)

    def test_repuestos_no_entran_a_lo_que_se_paga(self):
        # Los repuestos instalados suman a «me cobraron» de la OT, pero NO a lo que se le paga al proveedor.
        a = _amb()
        v = V(**CASOS["normal"])
        con_rep = a["_ot_finanzas"](v, REP_15K)
        sin_rep = a["_ot_finanzas"](v, None)
        self.assertEqual(con_rep["me_cobraron"]["total"], sin_rep["me_cobraron"]["total"] + 15000)
        self.assertEqual(con_rep["a_pagar_proveedor"], sin_rep["a_pagar_proveedor"])
        d = a["_mfp_fila_ot"](v)
        self.assertEqual(d["sugerido"], 120000.0)
        self.assertEqual(d["margen"], 180000 - 120000)

    def test_garantia_cero_se_paga_igual_y_no_se_cobra(self):
        d = _fila(1, **CASOS["garantia_cero"])
        self.assertEqual(d["sugerido"], 200000.0)
        self.assertEqual(d["cobrado_cliente"], 0)
        self.assertTrue(d["es_garantia"])
        self.assertEqual(d["estado_fila"], "garantia")
        self.assertTrue(any(x["n"] == "azul" for x in d["avisos_fila"]))
        self.assertIn("se le paga al proveedor igual", " ".join(x["t"] for x in d["avisos_fila"]).lower())

    def test_regalia_se_reconoce_como_cero(self):
        # Antes el SELECT de estas pantallas no traía cobro_cero_motivo y una regalía se leía como «falta lo que cobraste».
        d = _fila(2, **CASOS["regalia"])
        self.assertTrue(d["es_garantia"])
        self.assertEqual(d["cobertura"], "regalia")
        self.assertEqual(d["cobrado_cliente"], 0)
        self.assertEqual(d["sugerido"], 40000.0)

    def test_sin_despacho_es_cero_y_la_fila_esta_lista(self):
        d = _fila(3, **CASOS["sin_despacho"])
        self.assertEqual(d["sugerido"], 60000.0)
        self.assertEqual(d["despacho"], 0)
        self.assertFalse(d["falta_despacho"])
        self.assertTrue(d["listo"])

    def test_despacho_cobrado_sin_costo_marca_falta(self):
        d = _fila(4, **CASOS["despacho_cobrado_sin_costo"])
        self.assertTrue(d["falta_despacho"])
        self.assertFalse(d["listo"])
        self.assertEqual(d["estado_fila"], "falta")

    def test_falta_tecnico_no_esta_lista_y_dice_por_que(self):
        d = _fila(5, **CASOS["falta_tecnico"])
        self.assertTrue(d["falta_tecnico"])
        self.assertFalse(d["listo"])
        self.assertTrue(any("Falta lo que cobró el técnico" in x["t"] for x in d["avisos_fila"]))

    def test_tecnico_interno_sin_cliente_no_se_paga(self):
        d = _fila(6, **CASOS["interno_sin_cliente"])
        self.assertEqual(d["sugerido"], 0)
        self.assertTrue(d["es_garantia"])           # «no se cobra»
        self.assertEqual(d["cobertura"], "interno")
        self.assertFalse(d["falta_tecnico"])

    def test_perdida_se_pinta_roja_y_explica(self):
        d = _fila(7, **CASOS["perdida"])
        self.assertEqual(d["estado_fila"], "perdida")
        self.assertEqual(d["margen"], -20000)
        self.assertTrue(any(x["n"] == "rojo" and "Pérdida" in x["t"] for x in d["avisos_fila"]))

    def test_ot_no_cerrada_avisa_el_estado(self):
        d = _fila(8, estado="programada", **CASOS["normal"])
        self.assertFalse(d["listo"])
        self.assertTrue(any("no está cerrada (programada)" in x["t"] for x in d["avisos_fila"]))

    def test_autorizacion_del_cero_se_muestra(self):
        d = _fila(9, cero_aut_por="Daniel Aguilar", cero_aut_at=None, **CASOS["garantia_cero"])
        self.assertIn("autorizado por Daniel Aguilar", " ".join(x["t"] for x in d["avisos_fila"]))

    def test_ya_en_otra_factura_se_avisa(self):
        d = _fila(10, fac_id=33, **CASOS["normal"])
        self.assertTrue(any("Ya está en la factura #33" in x["t"] for x in d["avisos_fila"]))


class TestTotalesDePantallaIgualMotor(unittest.TestCase):
    def test_resumen_de_filas_es_la_suma_del_motor(self):
        a = _amb()
        filas, fins = [], []
        for i, kw in enumerate(CASOS.values(), start=1):
            v = V(id=i, **kw)
            filas.append(a["_mfp_fila_ot"](v))
            fins.append(a["_ot_finanzas"](v, None))
        t = a["_mfp_resumen_filas"](filas)
        self.assertEqual(t["pagado"], sum(f["a_pagar_proveedor"] for f in fins))
        self.assertEqual(t["n"], len(CASOS))
        # lo cobrado de las OT con cobro declarado = «Cobré» del motor
        comparables = [f for f, d in zip(fins, filas) if f["cobra"] and not d["sin_cobro_declarado"]]
        self.assertEqual(t["cobrado"], sum(f["cobre"]["total"] for f in comparables))
        self.assertEqual(t["margen"], sum(f["cobre"]["total"] - f["a_pagar_proveedor"] for f in comparables))
        no_cobran = [f for f in fins if not f["cobra"]]
        self.assertEqual(t["n_garantia"], len(no_cobran))
        self.assertEqual(t["pagado_garantia"], sum(f["a_pagar_proveedor"] for f in no_cobran))

    def test_facprov_datos_usa_lo_pagado_del_motor(self):
        # Facturación del mes: total pagado por proveedor = suma de a_pagar_proveedor de sus OT.
        a = _amb()
        rows = []
        for i, kw in enumerate(CASOS.values(), start=1):
            r = V(id=i, numero_ot=f"OT-{i}", prov_ficha="DAP", prov_ficha_id=9, tecnico_nombre="Daniel", cliente="C",
                  cerrada_at=None, proveedor_nombre="DAP", **kw)
            rows.append(r)
        esperado = sum(a["_ot_finanzas"](V(id=i, **kw), None)["a_pagar_proveedor"] for i, kw in enumerate(CASOS.values(), start=1))
        src = _fuente_de("_facprov_datos")
        self.assertIn('_fin["a_pagar_proveedor"]', src)
        self.assertNotIn('float(f.get("costo_proveedor")', src)
        self.assertNotIn('float(f.get("costo_despacho")', src)
        self.assertEqual(esperado, 100000 + 20000 + 200000 + 60000 + 60000 + 10000 + 40000 + 0 + 120000)

    def test_rutas_y_excel_no_arman_el_pago_a_mano(self):
        for nombre in ("_mfp_fila_ot", "_facprov_datos", "mant_facturacion_proveedores_xlsx"):
            try:
                src = _fuente_de(nombre)
            except AssertionError:
                continue
            self.assertNotRegex(src, r'float\(\s*\w+\.get\("costo_proveedor"\)', nombre)
            self.assertNotRegex(src, r'float\(\s*\w+\.get\("costo_despacho"\)', nombre)
        self.assertIn('_fin["a_pagar_proveedor"]', _fuente_de("_mfp_fila_ot"))

    def test_resumen_por_facturar_sale_de_las_mismas_filas(self):
        src = _fuente_de("_mfp_resumen")
        self.assertIn("_mfp_por_facturar(", src)
        self.assertNotIn("COALESCE(v.costo_proveedor", src)


@unittest.skipUnless(shutil.which("node"), "node no está instalado")
class TestEspejoNavegador(unittest.TestCase):
    def _node(self, codigo, datos):
        with tempfile.TemporaryDirectory() as d:
            ruta = os.path.join(d, "t.js")
            ruta_datos = os.path.join(d, "datos.json")
            open(ruta_datos, "w", encoding="utf-8").write(json.dumps(datos))
            open(ruta, "w", encoding="utf-8").write(
                "const fpi=require(%s);const d=JSON.parse(require('fs').readFileSync(%s,'utf8'));%s"
                % (json.dumps(JS), json.dumps(ruta_datos), codigo))
            out = subprocess.run(["node", ruta], capture_output=True, text=True, encoding="utf-8", timeout=60)
            self.assertEqual(out.returncode, 0, out.stderr)
            return json.loads(out.stdout)

    def test_sumar_del_navegador_igual_al_servidor(self):
        a = _amb()
        filas = [a["_mfp_fila_ot"](V(id=i, **kw)) for i, kw in enumerate(CASOS.values(), start=1)]
        items = [{"id": str(d["id"]), "numero": d["numero_ot"], "pagar": d["sugerido"], "cobrado": d["cobrado_cliente"],
                  "garantia": d["es_garantia"], "sincobro": d["sin_cobro_declarado"], "listo": d["listo"]} for d in filas]
        js = self._node("console.log(JSON.stringify(fpi.sumar(d)))", items)
        py = a["_mfp_resumen_filas"](filas)
        self.assertEqual(js["pagar"], py["pagado"])
        self.assertEqual(js["cobrado"], py["cobrado"])
        self.assertEqual(js["margen"], py["margen"])
        self.assertEqual(js["gar_n"], py["n_garantia"])
        self.assertEqual(js["gar_pagar"], py["pagado_garantia"])
        self.assertEqual(js["sin_n"], py["n_sin_cobro"])
        self.assertEqual(js["comp_n"], py["n_comparable"])

    def test_semaforo_verde_ambar_rojo(self):
        r = self._node("console.log(JSON.stringify([fpi.semaforo(400000,400000),fpi.semaforo(400000,380000),"
                       "fpi.semaforo(400000,420000),fpi.semaforo(0,100)]))", None)
        self.assertEqual([x["clase"] for x in r], ["ok", "warn", "neg", "muted"])
        self.assertEqual(r[1]["dif"], 20000)
        self.assertEqual(r[2]["dif"], -20000)

    def test_sugerencia_calza_exacto_y_prefiere_las_listas(self):
        cands = [{"id": "1", "numero": "OT-1", "pagar": 120000, "listo": True}, {"id": "2", "numero": "OT-2", "pagar": 80000, "listo": False},
                 {"id": "3", "numero": "OT-3", "pagar": 180000, "listo": True}, {"id": "4", "numero": "OT-4", "pagar": 100000, "listo": True},
                 {"id": "5", "numero": "OT-5", "pagar": 0, "listo": True}]
        r = self._node("console.log(JSON.stringify(fpi.sugerir(d.c,d.o)))", {"c": cands, "o": 400000})
        self.assertTrue(r["exacto"])
        self.assertEqual(r["suma"], 400000)
        self.assertEqual(sum(x["pagar"] for x in r["items"]), 400000)
        self.assertNotIn("5", [x["id"] for x in r["items"]], "una OT de $0 no se sugiere")
        # no calza exacto: la combinación más cercana sin pasarse
        r2 = self._node("console.log(JSON.stringify(fpi.sugerir(d.c,d.o)))", {"c": cands, "o": 410000})
        self.assertFalse(r2["exacto"])
        self.assertEqual(r2["suma"], 400000)
        self.assertEqual(r2["dif"], 10000)

    def test_frase_dice_que_pasa_en_palabras(self):
        datos = {"n": 4, "objetivo": 400000, "provNombre": "Juan Pérez",
                 "tot": {"pagar": 380000, "gar_n": 0, "gar_pagar": 0, "faltan": 0},
                 "marcadas": [], "noMarcadas": [{"numero": "OT-9", "pagar": 20000}]}
        f = self._node("console.log(JSON.stringify(fpi.frase(d)))", datos)
        self.assertIn("Seleccionaste 4 OT de Juan Pérez por $380.000", f)
        self.assertIn("la factura dice $400.000", f.replace("La factura", "la factura"))
        self.assertIn("faltan $20.000", f)
        self.assertIn("OT-9", f)


class TestPantallasConectadas(unittest.TestCase):
    def _leer(self, ruta):
        return open(os.path.join(RAIZ, ruta), encoding="utf-8").read()

    def test_las_dos_pantallas_cargan_el_ayudante_y_el_panel_en_vivo(self):
        lista = self._leer("templates/mantenciones/facturas_proveedor.html")
        det = self._leer("templates/mantenciones/factura_proveedor_detalle.html")
        for t in (lista, det):
            self.assertIn("facprov_inteligente.js", t)
            self.assertIn("facprov_inteligente.css", t)
            self.assertIn("Marcar las sugeridas", t)
        self.assertIn('id="fpvLive"', lista)
        self.assertIn('id="fpdLive"', det)
        # lo de arriba cambia al marcar: las tarjetas del resumen tienen ids que el JS reescribe
        for i in ("fpdCellAsig", "fpdCellDif", "fpdCellCob", "fpdCellMg"):
            self.assertIn(i, det)
        # REGLA #14: marcar/desmarcar todo sigue
        self.assertIn("Seleccionar/deseleccionar todo", det)
        self.assertIn("fpvOtToggleAllBtn", lista)

    def test_sin_alert_ni_confirm_nativos_en_lo_nuevo(self):
        for ruta in ("static/facprov_inteligente.js", "templates/mantenciones/facturas_proveedor.html",
                     "templates/mantenciones/factura_proveedor_detalle.html"):
            self.assertIsNone(re.search(r"(?<![\w.])(alert|confirm|prompt)\(", self._leer(ruta)), ruta)

    def test_el_tecnico_no_ve_estas_pantallas(self):
        # REGLA #19: las rutas siguen detrás de _no_técnico + permiso de facturación.
        for nombre in ("mant_facturas_proveedor", "mant_factura_proveedor_detalle", "mant_factura_proveedor_ot_disponibles"):
            src = _fuente_de(nombre)
            self.assertIn("_facprov_puede()", src, nombre)


if __name__ == "__main__":
    unittest.main()

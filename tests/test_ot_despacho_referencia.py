# -*- coding: utf-8 -*-
"""3-nov-2026 · Referencia del costo de despacho al crear una OT (Daniel, viendo a Juan Pablo crear una OT: «la sugerencia debe venir del motor de
cotización de logística, cuánto vale llevar esos productos»).

Se vigila:
  · el módulo puro (ot_despacho_ref): suma solo líneas de PRODUCTO con medidas, avisa las que no tienen medidas, elige la cotización firme más barata y
    nunca devuelve trazas ni motivos del comparador (la política comercial de Felca/Milling es confidencial);
  · el endpoint (POST /ot/api/despacho-referencia): nunca para un técnico (cuesta plata: REGLAS #19/#26), solo LECTURA, usa el comparador del Cubicador tal
    cual está (sin reimplementar tarifas) y le abre la puerta SOLO ahí, sin ensanchar el acceso directo al comparador.
Correr con:  py -m unittest tests.test_ot_despacho_referencia
"""
import os
import re
import unittest

import ot_despacho_ref as odr

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _leer(rel):
    with open(os.path.join(RAIZ, rel), encoding="utf-8", newline="") as f:
        return f.read().replace("\r\n", "\n")


APP = _leer("app.py")


def _endpoint():
    i = APP.index('@app.route("/ot/api/despacho-referencia", methods=["POST"])')
    return APP[i:APP.index('@app.route("/ot/api/lineas-zz/<tido>/<nudo>", methods=["GET"])', i)]


class TestDocumentosValidos(unittest.TestCase):
    def test_limpia_repetidos_vacios_y_pone_en_mayusculas(self):
        r = odr.documentos_validos([{"tido": "fcv", "nudo": " 11439 "}, {"tido": "FCV", "nudo": "11439"}, {"tido": "", "nudo": "1"},
                                    {"tido": "BLV", "nudo": ""}, "basura", None, {"tido": "NVV", "nudo": "VD00010667"}])
        self.assertEqual(r, [{"tido": "FCV", "nudo": "11439"}, {"tido": "NVV", "nudo": "VD00010667"}])

    def test_tope_de_documentos(self):
        docs = [{"tido": "FCV", "nudo": str(i)} for i in range(1, 30)]
        self.assertEqual(len(odr.documentos_validos(docs)), odr.MAX_DOCUMENTOS)
        self.assertEqual(odr.documentos_validos(None), [])


class TestResumirLineas(unittest.TestCase):
    def test_solo_cuentan_los_productos_con_medidas(self):
        lineas = [
            {"sku": "TROT-1", "descripcion_erp": "Trotadora", "cantidad": 2, "pred_tot": 180.5},
            {"sku": "MAN-2", "descripcion_erp": "Mancuerna", "cantidad": 10, "pred_tot": 40},
            {"sku": "SIN-3", "descripcion_erp": "Sin ficha", "cantidad": 1, "pred_tot": 0},
            {"sku": "ZZINSTALACION", "descripcion_erp": "Instalación", "cantidad": 1, "es_zz": True, "pred_tot": 0},
            {"sku": "DESC1", "descripcion_erp": "Descuento", "cantidad": 1, "es_descuento": True, "pred_tot": 5},
            {"sku": "CERO", "descripcion_erp": "Sin cantidad", "cantidad": 0, "pred_tot": 99},
        ]
        r = odr.resumir_lineas(lineas)
        self.assertEqual(r["peso_kg"], 220.5)
        self.assertEqual(r["productos"], 3)
        self.assertEqual(r["sin_medidas"], [{"sku": "SIN-3", "descripcion": "Sin ficha"}], "el producto sin medidas se avisa: la referencia puede quedar corta")

    def test_usa_el_filtro_de_servicios_del_cubicador(self):
        lineas = [{"sku": "FLETE", "descripcion_erp": "Flete", "cantidad": 1, "pred_tot": 50}, {"sku": "A", "descripcion_erp": "Caja", "cantidad": 1, "pred_tot": 10}]
        r = odr.resumir_lineas(lineas, lambda sku, desc, zz: sku == "FLETE")
        self.assertEqual((r["peso_kg"], r["productos"]), (10.0, 1))

    def test_datos_raros_no_rompen(self):
        r = odr.resumir_lineas([None, "x", {"sku": "A", "cantidad": "n/a", "pred_tot": "n/a"}, {"sku": "B", "cantidad": "2", "pred_tot": "7,5"}])
        self.assertEqual(r["peso_kg"], 0.0)
        self.assertEqual(odr.resumir_lineas(None), {"peso_kg": 0.0, "productos": 0, "sin_medidas": []})


class TestElegirSugerencia(unittest.TestCase):
    COTS = [
        {"courier_id": 1, "courier_nombre": "FedEx", "tiene_cobertura": True, "subtotal": 126933, "precio": 126933, "tiempo_transito": "1-2 días"},
        {"courier_id": 2, "courier_nombre": "Felca", "tiene_cobertura": True, "subtotal": 98000, "tiempo_transito": "2 días"},
        {"courier_id": 3, "courier_nombre": "Barato estimado", "tiene_cobertura": True, "subtotal": 50000, "es_estimado": True},
        {"courier_id": 4, "courier_nombre": "Sin cobertura", "tiene_cobertura": False, "subtotal": 10},
        {"courier_id": 5, "courier_nombre": "Sin precio", "tiene_cobertura": True, "subtotal": None, "precio": 0},
    ]

    def test_prefiere_la_recomendada_y_firme(self):
        s = odr.elegir_sugerencia(self.COTS, recomendado_id=2)
        self.assertEqual((s["courier"], s["monto"], s["estimado"], s["plazo"]), ("Felca", 98000, False, "2 días"))
        self.assertEqual([o["courier"] for o in s["otras"]], ["FedEx"], "las alternativas son solo firmes; la estimada no se ofrece")

    def test_sin_recomendada_toma_la_firme_mas_barata_nunca_la_estimada(self):
        self.assertEqual(odr.elegir_sugerencia(self.COTS)["courier"], "Felca")

    def test_si_todas_son_estimadas_se_ofrece_la_mas_barata_marcada(self):
        s = odr.elegir_sugerencia([c for c in self.COTS if c["courier_id"] in (3,)])
        self.assertEqual((s["courier"], s["monto"], s["estimado"]), ("Barato estimado", 50000, True))

    def test_sin_precio_utilizable_no_hay_sugerencia(self):
        self.assertIsNone(odr.elegir_sugerencia([self.COTS[3], self.COTS[4]]))
        self.assertIsNone(odr.elegir_sugerencia([]))
        self.assertIsNone(odr.elegir_sugerencia(None))

    def test_no_filtra_trazas_ni_motivos_del_comparador(self):
        cots = [dict(self.COTS[1], trace={"fuente": "regla", "advertencias": ["secreto"]}, motivo_regla="Felca -10% sobre FedEx", es_regla_comercial=True)]
        s = odr.elegir_sugerencia(cots)
        self.assertEqual(set(s), {"courier", "monto", "estimado", "plazo", "otras"})
        self.assertNotIn("secreto", repr(s))
        self.assertNotIn("regla", repr(s).lower())


class TestEndpoint(unittest.TestCase):
    def setUp(self):
        self.f = _endpoint()

    def test_un_tecnico_nunca_lo_ve(self):
        self.assertIn("@_mant_required\n@_no_tecnico\ndef ot2_api_despacho_referencia", self.f)

    def test_solo_lee_y_reusa_el_comparador_sin_reimplementar_tarifas(self):
        self.assertIn("_cp._invocar_comparador(app,", self.f)
        self.assertIn("_mant_erp_doc_cached(", self.f)
        for prohibido in ("INSERT ", "UPDATE ", "DELETE ", "mysql_execute", "requests.post", ".commit("):
            self.assertNotIn(prohibido, self.f, prohibido)

    def test_la_puerta_del_comparador_se_abre_solo_ahi_y_se_restaura(self):
        i = self.f.index("g.permissions = dict(_perm_previos or {}, logistica_cotizaciones=True)")
        self.assertIn("finally:\n        g.permissions = _perm_previos", self.f[i:])
        # y el comparador sigue exigiendo su permiso para quien lo llame directo (no se ensanchó)
        j = APP.index("def api_asignar_cotizar_couriers():")
        self.assertIn('g.permissions.get("cubicador") or g.permissions.get("transporte")', APP[j:j + 3500])
        self.assertNotIn("mant_ot", APP[j:j + 3500].split("import concurrent.futures")[0])

    def test_no_devuelve_trazas_y_nunca_5xx_por_un_dato_que_falta(self):
        self.assertNotIn("trace", self.f)
        self.assertNotIn("motivo_regla", self.f)
        self.assertNotRegex(self.f, r"\), 5\d\d")
        for estado in ("sin_documento", "sin_comuna", "sin_medidas", "sin_tarifa", "erp_error", '"error"', '"ok"'):
            self.assertIn(estado, self.f, estado)

    def test_la_comuna_cae_al_cliente_y_al_documento_y_la_columna_existe(self):
        self.assertIn("SELECT comuna FROM mant_clientes WHERE id=%s", self.f)
        i = APP.index("CREATE TABLE IF NOT EXISTS mant_clientes (")
        tabla = APP[i:APP.index(") ENGINE", i)]
        columnas = {l.split()[0] for l in tabla.splitlines()[1:] if l.strip() and l.split()[0].isidentifier()}
        self.assertIn("comuna", columnas, "lección del 3-nov: toda columna pedida existe en su CREATE TABLE")
        self.assertIn('_hdr.get("comuna")', self.f)

    def test_los_estados_pasajeros_no_se_recuerdan(self):
        self.assertIn('if estado in ("ok", "sin_tarifa"):', self.f)


if __name__ == "__main__":
    unittest.main()

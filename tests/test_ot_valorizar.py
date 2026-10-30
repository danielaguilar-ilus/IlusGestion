"""🧮 Valorización sugerida desde las cotizaciones — REGLA #60 (Daniel, 2026-10-29).

«Necesito que TODO sea VALORIZADO. Si no está valorizado por el usuario, TÚ lo SUGIERES: busca en las COTIZACIONES cuál es el precio más
acorde… Trabajos internos también valorizados para saber cuánto se nos escapa.»

Todo con datos de MENTIRA (nunca producción, nunca Random ni Check):
  · el módulo puro ot_valorizar.py: la escalera A–E, montos netos (descuento de cabecera prorrateado, precio a mano), rechazadas fuera,
    borradores solo si no hay otra cosa, otro cliente, región y país, mediana y percentiles, confianza, cobertura < 80 %, sin datos dice
    qué falta, reglas por cobertura (una OT que se cobra no ofrece guardar) y la decisión al aplicar;
  · las rutas (funciones de app.py recortadas con tests/_fuente_app.py y dobles de base): 403 a técnicos ANTES de leer nada, sugerir no
    escribe, «aplicar» recalcula en el servidor, rechaza un monto manipulado como fuente validada (o lo deja a mano con motivo), escribe
    SOLO las dos columnas y deja bitácora + registro inmutable + constancia en ot_valorizado_sugerencias;
  · el JavaScript de la ficha y de la pantalla en lote ejecutado en Node (paginación, nada marcado, revisar no escribe, aplicar de a 3,
    confirmación).
Correr con:  python -m unittest tests.test_ot_valorizar
"""
import ast
import json
import os
import re
import shutil
import subprocess
import tempfile
import types
import unittest
from datetime import datetime

import ot_valorizar as V
from tests import _fuente_app as FA

RAIZ = FA.RAIZ
AHORA = datetime(2026, 10, 29, 15, 0, 0)


def _leer(*partes):
    with open(os.path.join(RAIZ, *partes), encoding="utf-8") as fh:
        return fh.read()


def _cot(cid, unit, estado="approved", cliente=50, region="Región de Valparaíso", clase="trotadora", cant=2, ts="mantencion",
         fecha="2026-08-15 12:00:00", desc=0, empresa=None, **extra):
    """Una cotización de mentira con UNA línea de mano de obra (`unit` neto por equipo antes del descuento de cabecera)."""
    sub_items = unit * cant
    cab = {"id": cid, "numero_cotizacion": f"COT-{cid:06d}", "estado": estado, "tipo_servicio": ts, "cliente_id": cliente,
           "empresa": empresa or f"Gimnasio {cid} SpA", "region": region, "subtotal_items": sub_items, "subtotal": sub_items,
           "descuento_monto": desc, "costo_ruta": 0, "ruta_excluida": 0, "created_at": fecha, "eliminada": 0}
    cab.update(extra)
    return V.normalizar_cotizacion(cab, [{"item_tipo": "producto", "erp_kopr": "SKU-" + clase.upper(), "clase_producto": clase,
                                          "cantidad": cant, "total": sub_items}])


EQ_TROT = [{"maquina_id": 11, "nombre": "Trotadora Matrix T70 con consola XR", "sku": "T70", "cantidad": 4, "clase": "trotadora",
            "clase_nombre": "Trotadora"}]


def _ot(**kw):
    base = {"id": 900, "tipo": "preventiva", "cobertura": "garantia", "cliente_id": 9, "region": "Valparaíso"}
    base.update(kw)
    return base


# ═══════════════════════════════════════ módulo puro ═══════════════════════════════════════
class TestNeto(unittest.TestCase):
    def test_descuento_de_cabecera_se_prorratea_y_el_total_con_iva_no_se_usa(self):
        cab = {"id": 1, "estado": "approved", "tipo_servicio": "mantencion", "subtotal_items": 600000, "subtotal": 635000,
               "descuento_monto": 63500, "costo_ruta": 35000, "ruta_excluida": 0, "iva_monto": 108585, "total": 680085,
               "created_at": "2026-09-01"}
        c = V.normalizar_cotizacion(cab, [{"item_tipo": "producto", "clase_producto": "trotadora", "cantidad": 4, "total": 600000}])
        self.assertAlmostEqual(c["lineas"][0]["unit"], 150000 * 0.9)          # (635.000 − 63.500) / 635.000 = 0,9
        self.assertEqual(c["traslado"], 31500)                                # 35.000 de ruta, también con el descuento
        self.assertNotIn(680085, [round(l["total"]) for l in c["lineas"]])   # el total con IVA no aparece

    def test_precio_a_mano_con_descuento_del_item_si_el_total_quedo_en_cero(self):
        lin = V.lineas_netas({"subtotal": 0}, [{"item_tipo": "servicio", "clase_producto": "banca", "cantidad": 2, "total": 0,
                                               "precio_manual": 100000, "descuento_pct": 10}])
        self.assertAlmostEqual(lin[0]["unit"], 90000)

    def test_el_total_del_item_ya_trae_su_precio_a_mano_y_su_descuento(self):
        lin = V.lineas_netas({"subtotal": 0}, [{"item_tipo": "producto", "clase_producto": "banca", "cantidad": 3, "total": 270000,
                                               "precio_manual": 100000, "descuento_pct": 10}])
        self.assertAlmostEqual(lin[0]["unit"], 90000)

    def test_ruta_y_lineas_en_cero_no_son_mano_de_obra(self):
        lin = V.lineas_netas({}, [{"item_tipo": "ruta", "cantidad": 1, "total": 40000},
                                  {"item_tipo": "producto", "clase_producto": "accesorio", "cantidad": 5, "total": 0},
                                  {"item_tipo": "producto", "clase_producto": "trotadora", "cantidad": 0, "total": 100}])
        self.assertEqual(lin, [])

    def test_ruta_excluida_no_es_traslado(self):
        self.assertEqual(V.traslado_neto({"subtotal": 640000, "subtotal_items": 600000, "ruta_excluida": 1}), 0)

    def test_clase_desde_el_catalogo_si_la_linea_no_la_guardo(self):
        lin = V.lineas_netas({}, [{"item_tipo": "producto", "erp_kopr": "t70", "clase_producto": None, "cantidad": 1, "total": 100}],
                             {"T70": "trotadora"})
        self.assertEqual(lin[0]["clase"], "trotadora")


class TestEstadisticas(unittest.TestCase):
    def test_mediana_y_percentiles(self):
        self.assertEqual(V.percentil([(100, 1), (200, 1)], 0.5), 150)
        self.assertEqual(V.percentil([(1, 1), (2, 1), (3, 1), (4, 1)], 0.5), 2.5)
        self.assertEqual(V.percentil([(10, 1)], 0.25), 10)
        self.assertIsNone(V.percentil([], 0.5))
        # pesos: el valor de la aprobada pesa más que el del borrador
        self.assertLess(V.percentil([(100, 1.0), (300, 0.5)], 0.5), 200)

    def test_region_normalizada(self):
        for s in ("Región Metropolitana de Santiago", "RM", "metropolitana", "Region Metropolitana"):
            self.assertEqual(V.normalizar_region(s), "metropolitana", s)
        self.assertEqual(V.normalizar_region("Región de Valparaíso"), V.normalizar_region("Valparaíso"))
        self.assertIsNone(V.normalizar_region(""))


class TestEscalera(unittest.TestCase):
    def _indice(self, cots=(), ots=()):
        return V.indice_mercado(list(cots), list(ots), AHORA)

    def test_A_la_cotizacion_de_esta_ot_manda_y_calza_por_sku_aunque_no_este_clasificado(self):
        prop = V.normalizar_cotizacion(
            {"id": 77, "numero_cotizacion": "COT-000077", "estado": "approved", "tipo_servicio": "mantencion", "subtotal": 500000,
             "subtotal_items": 500000, "descuento_monto": 0, "created_at": "2025-01-01"},   # vieja: para A no hay ventana
            [{"item_tipo": "producto", "erp_kopr": "T70", "clase_producto": "trotadora", "cantidad": 4, "total": 400000},
             {"item_tipo": "producto", "erp_kopr": "BANCA-9", "clase_producto": None, "cantidad": 1, "total": 100000}])
        eq = EQ_TROT + [{"maquina_id": 12, "nombre": "Banca regulable", "sku": "BANCA-9", "cantidad": 1, "clase": None}]
        mercado = self._indice([_cot(i, 999999, cliente=9) for i in range(300, 305)])
        s = V.sugerir(_ot(), eq, mercado, [prop])
        self.assertEqual(s["monto"], 500000)
        self.assertEqual({c["fuente"] for c in s["por_clase"]}, {"cotiz_propia"})
        self.assertEqual(s["confianza"], "alta")
        self.assertEqual(s["cobertura_equipos"], 1.0)
        self.assertEqual(s["base"]["cotizaciones"], [77])
        self.assertIn("COT-000077", s["texto"])

    def test_A_propia_rechazada_no_cuenta(self):
        prop = _cot(78, 120000, estado="rejected", cliente=9)
        self.assertFalse(prop["valida"])
        s = V.sugerir(_ot(), EQ_TROT, self._indice(), [prop])
        self.assertFalse(s["hay"])

    def test_B_mismo_cliente_con_dos_cotizaciones_y_otro_cliente_no_entra(self):
        cots = [_cot(1, 100000, cliente=9), _cot(2, 120000, cliente=9, estado="sent"),
                _cot(3, 900000, cliente=50), _cot(4, 900000, cliente=51), _cot(5, 900000, cliente=52)]
        s = V.sugerir(_ot(), EQ_TROT, self._indice(cots))
        c = s["por_clase"][0]
        self.assertEqual(c["fuente"], "cotiz_cliente")
        self.assertEqual(sorted(c["cot_ids"]), [1, 2])
        self.assertTrue(100000 <= c["unit"] <= 120000)
        self.assertEqual(c["confianza"], "media")                             # solo 2: «pocos comparables»
        self.assertEqual(s["fuente"], "cotiz_cliente")

    def test_B_con_tres_aprobadas_es_confianza_alta(self):
        cots = [_cot(i, 100000 + i, cliente=9) for i in range(1, 4)]
        self.assertEqual(V.sugerir(_ot(), EQ_TROT, self._indice(cots))["confianza"], "alta")

    def test_rechazadas_eliminadas_y_viejas_nunca_cuentan(self):
        cots = [_cot(1, 100000, cliente=9, estado="rejected"), _cot(2, 100000, cliente=9, eliminada=1),
                _cot(3, 100000, cliente=9, fecha="2025-06-01"), _cot(4, 100000, cliente=9, estado="expired", fecha="2025-01-01")]
        s = V.sugerir(_ot(), EQ_TROT, self._indice(cots))
        self.assertFalse(s["hay"])
        self.assertIn("sin_precio", s["motivos"])

    def test_borradores_solo_si_no_hay_otra_cosa_y_bajan_la_confianza(self):
        cots = [_cot(1, 100000, cliente=9), _cot(2, 110000, cliente=9, estado="draft")]
        s = V.sugerir(_ot(), EQ_TROT, self._indice(cots))
        self.assertEqual(s["por_clase"][0]["fuente"], "cotiz_cliente")
        self.assertTrue(s["por_clase"][0]["respaldo"])
        self.assertEqual(s["confianza"], "baja")
        # con dos aprobadas, el borrador ya no entra
        cots.append(_cot(3, 120000, cliente=9))
        c = V.sugerir(_ot(), EQ_TROT, self._indice(cots))["por_clase"][0]
        self.assertEqual(sorted(c["cot_ids"]), [1, 3])
        self.assertFalse(c["respaldo"])

    def test_C_primero_la_region_y_despues_el_pais(self):
        region = [_cot(10 + i, 150000 + i * 1000, cliente=60 + i, region="Valparaíso") for i in range(3)]
        pais = [_cot(20 + i, 300000, cliente=70 + i, region="Región Metropolitana de Santiago") for i in range(6)]
        s = V.sugerir(_ot(cliente_id=999), EQ_TROT, self._indice(region + pais))
        c = s["por_clase"][0]
        self.assertEqual((c["fuente"], c["alcance"]), ("cotiz_comparable", "region"))
        self.assertEqual(sorted(c["cot_ids"]), [10, 11, 12])
        self.assertEqual(c["confianza"], "baja")                              # 3 < 5: pocos
        s2 = V.sugerir(_ot(cliente_id=999, region="Biobío"), EQ_TROT, self._indice(region + pais))
        c2 = s2["por_clase"][0]
        self.assertEqual(c2["alcance"], "pais")
        self.assertEqual(c2["n"], 9)
        self.assertEqual(c2["confianza"], "media")
        self.assertIn("todo el país", s2["texto"])

    def test_D_ot_cobradas_comparables_de_una_sola_clase(self):
        eqs1 = [{"clase": "trotadora", "cantidad": 2}, {"clase": "accesorio", "cantidad": 5}]
        o = V.ot_comparable_de({"id": 5, "numero_ot": "OT-5", "tipo": "correctiva", "cobra": True, "valor_origen": "zz", "zz_monto": 240000,
                                "cerrada_at": "2026-09-01"}, eqs1, no_cobrables={"accesorio"})
        self.assertEqual((o["clase"], o["unit"], o["tipo_servicio"]), ("trotadora", 120000, "mantencion"))
        malas = [
            ({"cobra": False}, eqs1), ({"valor_origen": "manual"}, eqs1), ({"zz_codigo": "ZZRETIRO"}, eqs1), ({"zz_monto": 0}, eqs1),
            ({}, [{"clase": "trotadora", "cantidad": 1}, {"clase": "bicicleta", "cantidad": 1}]),
            ({}, [{"clase": "trotadora", "cantidad": 1}, {"clase": None, "cantidad": 1}]), ({"tipo": "levantamiento"}, eqs1)]
        for cambio, eqs in malas:
            ot = dict({"id": 6, "tipo": "correctiva", "cobra": True, "valor_origen": "zz", "zz_monto": 240000}, **cambio)
            self.assertIsNone(V.ot_comparable_de(ot, eqs, {"accesorio"}), cambio)
        ots = [dict(o, id=100 + i, unit=100000 + 10000 * i) for i in range(5)]
        s = V.sugerir(_ot(cliente_id=999), EQ_TROT, self._indice([], ots))
        c = s["por_clase"][0]
        self.assertEqual(c["fuente"], "ot_comparable")
        self.assertEqual(c["unit"], 120000)
        self.assertEqual((c["p25"], c["p75"]), (107500, 132500))             # punto medio: posiciones 0,1 · 0,3 · 0,5 · 0,7 · 0,9
        self.assertEqual(s["rango"], {"p25": 430000, "p75": 530000})
        self.assertEqual(c["confianza"], "media")

    def test_E_la_tarifa_del_cotizador_al_final_y_con_confianza_baja(self):
        s = V.sugerir(_ot(), EQ_TROT, self._indice(), [], {("mantencion", "trotadora"): {"unit": 95000}})
        self.assertEqual((s["monto"], s["fuente"], s["confianza"]), (380000, "cotizador", "baja"))
        self.assertIsNone(s["rango"])
        self.assertIn("tarifa del cotizador", s["texto"])

    def test_cobertura_bajo_80_baja_un_escalon_y_dice_que_quedan_fuera(self):
        cots = [_cot(i, 100000 + i, cliente=9) for i in range(1, 4)]
        eq = EQ_TROT + [{"maquina_id": 20, "nombre": "Banca sin clasificar", "sku": "B1", "cantidad": 2, "clase": None}]
        s = V.sugerir(_ot(), eq, self._indice(cots))
        self.assertAlmostEqual(s["cobertura_equipos"], 4 / 6, places=3)
        self.assertEqual(s["confianza"], "media")                             # B con 3 era alta: baja un escalón
        self.assertIn("Quedan fuera 1 equipo sin clasificar", s["texto"])
        self.assertEqual(s["sin_clasificar"][0]["nombre"], "Banca sin clasificar")

    def test_accesorios_no_cuentan_en_la_cobertura(self):
        eq = EQ_TROT + [{"maquina_id": 21, "nombre": "Kit de bandas", "cantidad": 10, "clase": "accesorio", "clase_nombre": "Accesorio"}]
        s = V.sugerir(_ot(), eq, self._indice(), [], {("mantencion", "trotadora"): {"unit": 1000}}, no_cobrables={"accesorio"})
        self.assertEqual(s["cobertura_equipos"], 1.0)
        self.assertEqual(s["accesorios"][0]["nombre"], "Kit de bandas")

    def test_sin_datos_dice_que_falta_y_nunca_inventa(self):
        casos = [([], "sin_equipos"),
                 ([{"maquina_id": 1, "nombre": "X", "sku": "X", "cantidad": 1, "clase": None}], "sin_clasificacion")]
        for eq, motivo in casos:
            s = V.sugerir(_ot(), eq, self._indice())
            self.assertFalse(s["hay"])
            self.assertIsNone(s["monto"])
            self.assertIn(motivo, s["motivos"])
            self.assertTrue(s["texto"].startswith("Sin valor sugerido"))
        s = V.sugerir(_ot(tipo="cambio_equipo"), EQ_TROT, self._indice())
        self.assertIn("sin_tipo_servicio", s["motivos"])
        s = V.sugerir(_ot(), EQ_TROT, self._indice())
        self.assertIn("sin_precio", s["motivos"])

    def test_trabajo_de_bodega_se_compara_con_mantencion_y_baja_la_confianza(self):
        cots = [_cot(i, 100000 + i, cliente=None, region="X") for i in range(1, 7)]
        s = V.sugerir(_ot(tipo="revision_interna", cobertura="interno", cliente_id=None, region=None), EQ_TROT, self._indice(cots))
        self.assertTrue(s["aproximado"])
        self.assertEqual(s["confianza"], "baja")
        self.assertTrue(s["puede_guardar"])
        self.assertIn("trabajo de bodega", s["texto"])


class TestPorCobertura(unittest.TestCase):
    def setUp(self):
        self.idx = V.indice_mercado([_cot(i, 150000, cliente=9) for i in range(1, 5)], [], AHORA)

    def test_una_ot_que_se_cobra_no_ofrece_guardar_y_avisa_si_cobramos_muy_bajo(self):
        s = V.sugerir(_ot(cobertura="cobra", cobrado_servicio=300000), EQ_TROT, self.idx)
        self.assertEqual(s["modo"], "mercado")
        self.assertFalse(s["puede_guardar"])
        self.assertTrue(s["comparacion"]["alerta"])                           # 300.000 vs 600.000: 50 % bajo, confianza alta
        self.assertIn("50 % bajo lo sugerido", s["texto"])
        self.assertIn("no se guarda", s["texto"])
        self.assertFalse(V.sugerir(_ot(cobertura="cobra", cobrado_servicio=500000), EQ_TROT, self.idx)["comparacion"]["alerta"])

    def test_con_confianza_baja_no_hay_alerta(self):
        s = V.sugerir(_ot(cobertura="cobra", cobrado_servicio=10), EQ_TROT, V.indice_mercado([], [], AHORA), [],
                      {("mantencion", "trotadora"): {"unit": 150000}})
        self.assertEqual(s["confianza"], "baja")
        self.assertFalse(s["comparacion"]["alerta"])

    def test_garantia_regalia_arriendo_cortesia_contrato_e_interno_se_pueden_guardar(self):
        for cob in ("garantia", "regalia", "arriendo_leasing", "sin_costo", "contrato", "interno"):
            s = V.sugerir(_ot(cobertura=cob), EQ_TROT, self.idx)
            self.assertEqual((s["modo"], s["puede_guardar"]), ("valorizar", True), cob)

    def test_desinstalacion_inspeccion_y_levantamiento_solo_con_partida_explicita(self):
        for tipo, cob in (("desinstalacion", "cubierta"), ("inspeccion", "garantia"), ("levantamiento", "interno"),
                          ("instalacion", "cubierta")):
            s = V.sugerir(_ot(tipo=tipo, cobertura=cob), EQ_TROT, self.idx, [], {("instalacion", "trotadora"): {"unit": 1}})
            self.assertFalse(s["hay"], tipo)
            self.assertIn("solo_explicita", s["motivos"], tipo)
        prop = _cot(500, 80000, cliente=9, ts="instalacion")
        s = V.sugerir(_ot(tipo="desinstalacion", cobertura="cubierta"), EQ_TROT, self.idx, [prop])
        self.assertTrue(s["hay"])
        self.assertEqual(s["fuente"], "cotiz_propia")


class TestDecidir(unittest.TestCase):
    def setUp(self):
        idx = V.indice_mercado([_cot(i, 150000, cliente=9) for i in range(1, 5)], [], AHORA)
        self.sug = V.sugerir(_ot(), EQ_TROT, idx)
        self.merc = V.sugerir(_ot(cobertura="cobra"), EQ_TROT, idx)

    def test_aceptada_con_el_monto_exacto_lleva_la_fuente_de_la_sugerencia(self):
        r = V.decidir(self.sug, "aceptada", self.sug["monto"], "")
        self.assertEqual((r["ok"], r["escribe"], r["monto"], r["fuente"]), (True, True, 600000, "cotiz_cliente"))

    def test_un_monto_manipulado_no_queda_como_fuente_validada(self):
        r = V.decidir(self.sug, "aceptada", 999999, "")
        self.assertEqual((r["ok"], r["codigo"], r["http"]), (False, "SUGERENCIA_CAMBIO", 409))
        r = V.decidir(self.sug, "aceptada", 999999, "el cliente tiene un precio especial de contrato")
        self.assertEqual((r["ok"], r["fuente"], r["monto"]), (True, "a_mano", 999999))

    def test_editada_pide_motivo_y_queda_a_mano(self):
        self.assertEqual(V.decidir(self.sug, "editada", 500000, "corto")["codigo"], "MOTIVO_CORTO")
        self.assertEqual(V.decidir(self.sug, "editada", 500000, "x" * 15)["fuente"], "a_mano")
        self.assertEqual(V.decidir(self.sug, "editada", 600000, "x" * 15)["fuente"], "cotiz_cliente")   # igual al sugerido

    def test_descartada_no_escribe_y_lo_que_se_cobra_no_se_guarda(self):
        self.assertEqual(V.decidir(self.sug, "descartada", None, ""), {"ok": True, "escribe": False, "monto": None, "fuente": None})
        self.assertEqual(V.decidir(self.merc, "aceptada", self.merc["monto"], "")["codigo"], "SE_COBRA")
        self.assertEqual(V.decidir(self.sug, "otra", 1, "")["codigo"], "DECISION_INVALIDA")
        for malo in (0, -5, "abc", None, 3_000_000_000):
            self.assertEqual(V.decidir(self.sug, "editada", malo, "x" * 20)["codigo"], "MONTO_INVALIDO", malo)

    def test_base_json_guarda_ids_y_percentiles_sin_nombres(self):
        b = V.base_json(self.sug)
        self.assertEqual(b["cotizaciones"], [1, 2, 3, 4])
        self.assertIn("p25", b["por_clase"][0])
        self.assertNotIn("Gimnasio", json.dumps(b))


class TestLote(unittest.TestCase):
    def test_parametros_raros_vuelven_al_defecto(self):
        p = V.leer_parametros_lote({"modo": "drop table", "estado": "x", "por": "7", "page": "-3", "cobertura": "nada",
                                    "confianza": "suprema", "q": "a" * 200})
        self.assertEqual((p["modo"], p["estado"], p["por"], p["page"], p["cobertura"], p["confianza"]), ("todo", "cerradas", 50, 1, "", ""))
        self.assertEqual(len(p["q"]), 80)

    def test_filtrar_paginar_y_totales(self):
        filas = [{"id": i, "numero": f"OT-{i}", "cliente": "Gym Uno" if i % 2 else "Otro", "cobertura": "interno" if i < 3 else "garantia",
                  "confianza": "alta", "descartada": i == 9, "monto": 1000} for i in range(1, 11)]
        self.assertEqual(len(V.filtrar_filas(filas, {"q": "gym uno"})), 4)               # 1, 3, 5, 7 (la 9 está descartada)
        self.assertEqual(len(V.filtrar_filas(filas, {})), 9)                            # la descartada se oculta
        self.assertEqual(len(V.filtrar_filas(filas, {"ver_descartadas": True})), 10)
        self.assertEqual(len(V.filtrar_filas(filas, {"cobertura": "interno"})), 2)
        pg = V.paginar(list(range(130)), 3, 50)
        self.assertEqual((pg["desde"], pg["hasta"], pg["paginas"], len(pg["filas"])), (101, 130, 3, 30))
        self.assertEqual(V.paginar([], 5, 50)["desde"], 0)
        k = V.kpis_lote([{"cobertura": "interno", "monto": 100000}, {"cobertura": "garantia", "monto": 50000}],
                        [{"cobertura": "interno", "monto": 30000}, {"cobertura": "garantia", "monto": None}])
        self.assertEqual((k["dejamos_de_cobrar"], k["internos"], k["por_valorizar_n"], k["por_valorizar_sugerido"],
                          k["por_valorizar_internos"]), (150000, 100000, 2, 30000, 30000))


class TestAlineadoConApp(unittest.TestCase):
    def test_el_mapa_de_tipos_es_el_del_cotizador(self):
        amb = FA.cargar(constantes=["_OT2_TIPO_A_TIPO_SERVICIO_COSTO"])
        self.assertEqual(amb["_OT2_TIPO_A_TIPO_SERVICIO_COSTO"], V.TIPO_SERVICIO_BASE)

    def test_las_fuentes_estan_en_el_vocabulario_y_caben_en_la_columna(self):
        amb = FA.cargar(constantes=["_OT_FIN_VALORIZADO_FUENTES", "_OT_FIN_VALORIZADO_FUENTES_SERVIDOR"])
        for f in V.FUENTES:
            self.assertIn(f, amb["_OT_FIN_VALORIZADO_FUENTES"])
            self.assertLessEqual(len(f), 20)
        self.assertEqual(amb["_OT_FIN_VALORIZADO_FUENTES_SERVIDOR"], V.FUENTES_SOLO_SERVIDOR)
        self.assertNotIn("cotizador", V.FUENTES_SOLO_SERVIDOR)                # el botón de siempre sigue igual (REGLA #4.2)

    def test_la_pantalla_sabe_rotular_cada_fuente(self):
        det = _leer("templates", "ot2", "detalle.html")
        i = det.index("const _VAL_FUENTE_TXT = {")
        bloque = det[i:det.index("};", i)]
        for f in V.FUENTES:
            self.assertRegex(bloque, r"\b%s:" % f)

    def test_el_navegador_no_puede_fijar_una_fuente_de_sugerencia(self):
        amb = FA.cargar(funciones=["_ot_fin_valorizado_fuente"], constantes=[
            "_OT_FIN_VALORIZADO_FUENTE_POR_ORIGEN", "_OT_FIN_VALORIZADO_FUENTES", "_OT_FIN_VALORIZADO_FUENTES_SERVIDOR"])
        f = amb["_ot_fin_valorizado_fuente"]
        for s in V.FUENTES_SOLO_SERVIDOR:
            self.assertEqual(f(None, s), "a_mano", s)
        self.assertEqual(f(None, "cotizador"), "cotizador")
        cuerpo = FA.fuente_funcion("ot2_api_finanzas")
        self.assertIn('_val_fuente in globals().get("_OT_FIN_VALORIZADO_FUENTES_SERVIDOR", ())', cuerpo)


# ═══════════════════════════════════════ rutas ═══════════════════════════════════════
class _Prohibido(Exception):
    pass


class _Cur:
    def __init__(self, log):
        self.log = log

    def execute(self, sql, params=None):
        self.log.append((sql, params))

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _Conn:
    def __init__(self):
        self.log, self.commits, self.cerrada = [], 0, False

    def cursor(self):
        return _Cur(self.log)

    def commit(self):
        self.commits += 1

    def rollback(self):
        pass

    def close(self):
        self.cerrada = True


RUTAS = ("ot2_api_valorizacion_sugerida", "ot2_api_valorizacion_aplicar", "ot_api_valorizacion_lote", "ot2_valorizar_pagina")


class TestRutas(unittest.TestCase):
    def test_decoradores_de_gestion_en_las_cuatro_rutas(self):
        for n in RUTAS:
            deco = " ".join(FA.decoradores(n))
            self.assertIn("_mant_required", deco, n)
            self.assertIn("_no_tecnico", deco, n)                             # REGLAS #19/#26
            self.assertIn("valoriz", n)                                       # la atrapa tests/test_confidencialidad_financiera_tecnicos.py
        self.assertIn("_ot_can_view", " ".join(FA.decoradores("ot2_api_valorizacion_sugerida")))
        for n in ("ot2_api_valorizacion_aplicar", "ot_api_valorizacion_lote", "ot2_valorizar_pagina"):
            self.assertIn("_ot_fin_es_superadmin()", FA.fuente_funcion(n), n)
        rutas = FA.texto()
        for r in ('"/ot/api/<int:vid>/valorizacion-sugerida"', '"/ot/api/<int:vid>/valorizacion-aplicar"',
                  '"/ot/api/valorizacion/lote"', '"/ot/valorizar"'):
            self.assertIn(r, rutas)

    def _amb(self, funcs, tecnico=False, sa=True, extra=None):
        llamadas = []

        def nada(*a, **k):
            llamadas.append(a)
            raise _Prohibido("no debía leer nada")
        base = {"_es_rol_tecnico": lambda: tecnico, "_ot_fin_es_superadmin": lambda: sa, "jsonify": lambda d: d,
                "_ot2_err": lambda m, c, s=400, **k: ({"ok": False, "error": m, "error_codigo": c}, s),
                "_valz_una": nada, "mysql_fetchall": nada, "mysql_fetchone": nada, "mysql_execute": nada, "get_mysql": nada,
                "_valz": V, "request": types.SimpleNamespace(args={}, get_json=lambda silent=True: {}),
                "_friendly_error_page": lambda t, m, c: ("pagina_error", c), "render_template": lambda t, **k: ("render", t)}
        base.update(extra or {})
        amb = FA.cargar(funciones=funcs, extra=base)
        return amb, llamadas

    def test_un_tecnico_recibe_403_antes_de_leer_nada(self):
        for n in ("ot2_api_valorizacion_sugerida", "ot2_api_valorizacion_aplicar"):
            amb, llamadas = self._amb([n], tecnico=True)
            r = amb[n](900)
            self.assertEqual(r[1], 403, n)
            self.assertEqual(llamadas, [], n)
        amb, llamadas = self._amb(["ot_api_valorizacion_lote"], tecnico=True)
        self.assertEqual(amb["ot_api_valorizacion_lote"]()[1], 403)
        self.assertEqual(llamadas, [])

    def test_quien_no_es_superadmin_no_aplica_ni_ve_el_lote(self):
        amb, llamadas = self._amb(["ot2_api_valorizacion_aplicar", "ot_api_valorizacion_lote", "ot2_valorizar_pagina"], sa=False)
        self.assertEqual(amb["ot2_api_valorizacion_aplicar"](900)[0]["error_codigo"], "SOLO_SUPERADMIN")
        self.assertEqual(amb["ot_api_valorizacion_lote"]()[0]["error_codigo"], "SOLO_SUPERADMIN")
        self.assertEqual(amb["ot2_valorizar_pagina"](), ("pagina_error", 403))
        self.assertEqual(llamadas, [])

    def test_sugerir_no_escribe_nada(self):
        """Estático: ninguna función de lectura de la valorización tiene INSERT/UPDATE/DELETE/DDL; y la ruta de una OT, ejecutada con
        una base que revienta ante cualquier escritura, responde igual."""
        t = FA.texto()
        nombres = re.findall(r"^def (_valz_\w+)\(", t, re.M) + ["ot2_api_valorizacion_sugerida", "ot_api_valorizacion_lote"]
        self.assertGreater(len(nombres), 12)
        for n in nombres:
            if n in ("_valz_tabla_ok",):
                continue
            src = FA.fuente_funcion(n)
            self.assertNotRegex(src.upper(), r"\b(INSERT|UPDATE|DELETE|ALTER|DROP|TRUNCATE|CREATE)\s", n)
            # ni siquiera indirecto: leer nunca crea la tabla de constancias (eso es del arranque y del camino de escritura)
            self.assertNotIn("_valz_tabla_ok(", src, n)
            self.assertNotIn("_ensure_", src, n)
        # La ruta de una OT, con la sugerencia ya calculada por un doble: nunca toca mysql_execute ni get_mysql.
        sug = V.sugerir(_ot(), EQ_TROT, V.indice_mercado([_cot(i, 150000, cliente=9) for i in range(1, 4)], [], AHORA))

        def una(vid):
            return sug, {"id": vid, "numero_ot": "OT-1", "estado": "cerrada"}, {"cobertura": "garantia", "cobertura_txt": "Garantía",
                                                                                "valorizado": {"monto": None, "fuente": None}}
        amb, _ = self._amb(["ot2_api_valorizacion_sugerida", "_valz_fuente_txt"], extra={
            "_valz_una": una, "_valz_ultima_decision": lambda vids: {}})
        r = amb["ot2_api_valorizacion_sugerida"](900)
        self.assertEqual(r["monto"], 600000)
        self.assertEqual(r["ot"]["numero"], "OT-1")
        self.assertTrue(r["es_superadmin"])

    def _aplicar(self, cuerpo, sug=None, fin=None, antes=None, estado="cerrada"):
        conn, logs = _Conn(), []
        sug = sug or V.sugerir(_ot(), EQ_TROT, V.indice_mercado([_cot(i, 150000, cliente=9) for i in range(1, 4)], [], AHORA))
        fin = fin or {"cobra": False, "cobertura": "garantia"}
        antes = antes or {"id": 900, "numero_ot": "OT-2026-00900", "valorizado_clp": None, "valorizado_fuente": None}
        amb, _ = self._amb(["ot2_api_valorizacion_aplicar", "_valz_fuente_txt"], extra={
            "_valz_una": lambda vid: (sug, {"id": vid, "numero_ot": "OT-2026-00900", "estado": estado}, fin),
            "request": types.SimpleNamespace(get_json=lambda silent=True: cuerpo), "_valz_tabla_ok": lambda: True,
            "current_username": lambda: "daniel", "mysql_fetchone": lambda sql, p=None: dict(antes), "_OT_FIN_SELECT": "SELECT …",
            "json": json, "_ot_fin_rep_de": lambda vid: None, "get_mysql": lambda: conn,
            "_ot_fin_ganancia": lambda r, rep=None: -50000.0, "_VALZ_LOTE_CACHE": {"x": 1},
            "_mant_log": lambda *a: logs.append(a)})
        return amb["ot2_api_valorizacion_aplicar"](900), conn, logs, amb

    def test_aplicar_aceptada_escribe_solo_las_dos_columnas_con_la_fuente_recalculada(self):
        r, conn, logs, amb = self._aplicar({"monto": 600000, "decision": "aceptada"})
        self.assertTrue(r["ok"])
        self.assertEqual(r["valorizado"]["fuente"], "cotiz_cliente")
        sqls = [s for s, _ in conn.log]
        self.assertEqual(sqls[0], "UPDATE mant_visitas SET valorizado_clp=%s, valorizado_fuente=%s WHERE id=%s")
        self.assertEqual(conn.log[0][1], (600000, "cotiz_cliente", 900))
        self.assertTrue(sqls[1].startswith("INSERT INTO mant_ot_finanzas_correcciones"))
        antes, despues = json.loads(conn.log[1][1][4]), json.loads(conn.log[1][1][5])
        self.assertEqual(antes, {"valorizado_clp": None, "valorizado_fuente": None})
        self.assertEqual(despues, {"valorizado_clp": 600000, "valorizado_fuente": "cotiz_cliente"})
        self.assertTrue(sqls[2].startswith("INSERT INTO ot_valorizado_sugerencias"))
        p = conn.log[2][1]
        self.assertEqual((p[2], p[3], p[4], p[5], p[6]), (600000, 600000, "aceptada", "cotiz_cliente", "cotiz_cliente"))
        self.assertIn('"cotizaciones": [1, 2, 3]', p[10])
        self.assertEqual((conn.commits, conn.cerrada), (1, True))
        self.assertEqual(logs[0][2], "valorizado_sugerido_aplicado")
        self.assertIn("no se suma a lo cobrado", logs[0][3])
        self.assertEqual(amb["_VALZ_LOTE_CACHE"], {})                          # la pantalla del lote vuelve a leer

    def test_un_monto_manipulado_sin_motivo_se_rechaza_y_no_escribe(self):
        r, conn, logs, _ = self._aplicar({"monto": 950000, "decision": "aceptada"})
        self.assertEqual(r[1], 409)
        self.assertEqual(r[0]["error_codigo"], "SUGERENCIA_CAMBIO")
        self.assertEqual(r[0]["sugerido"], 600000)
        self.assertEqual((conn.log, logs), ([], []))

    def test_un_monto_distinto_con_motivo_queda_a_mano(self):
        r, conn, logs, _ = self._aplicar({"monto": 950000, "decision": "aceptada", "motivo": "precio de lista del contrato 2026"})
        self.assertTrue(r["ok"])
        self.assertEqual(conn.log[0][1], (950000, "a_mano", 900))
        self.assertEqual(conn.log[2][1][4], "editada")
        self.assertEqual(conn.log[1][1][3], "precio de lista del contrato 2026")

    def test_una_ot_que_se_cobra_no_recibe_valorizado(self):
        r, conn, _, _ = self._aplicar({"monto": 600000, "decision": "aceptada"}, fin={"cobra": True, "cobertura": "cobra"})
        self.assertEqual((r[1], r[0]["error_codigo"]), (409, "SE_COBRA"))
        self.assertEqual(conn.log, [])

    def test_descartar_solo_deja_la_constancia(self):
        r, conn, logs, _ = self._aplicar({"decision": "descartada", "motivo": "No aplica"})
        self.assertTrue(r["ok"])
        self.assertFalse(r["escribio"])
        self.assertEqual(len(conn.log), 1)
        self.assertTrue(conn.log[0][0].startswith("INSERT INTO ot_valorizado_sugerencias"))
        self.assertEqual(conn.log[0][1][4], "descartada")
        self.assertEqual(logs[0][2], "valorizado_sugerido_descartado")

    def test_ot_anulada_no_se_valoriza(self):
        r, conn, _, _ = self._aplicar({"monto": 600000, "decision": "aceptada"}, estado="anulada")
        self.assertEqual(r[0]["error_codigo"], "OT_NO_VIGENTE")
        self.assertEqual(conn.log, [])

    def test_el_codigo_de_aplicar_solo_actualiza_valorizado(self):
        src = FA.fuente_funcion("ot2_api_valorizacion_aplicar")
        updates = re.findall(r'"UPDATE [^"]+"', src)
        self.assertEqual(updates, ['"UPDATE mant_visitas SET valorizado_clp=%s, valorizado_fuente=%s WHERE id=%s"'])
        for prohibido in ("zz_monto", "zz_envio_monto", "costo_proveedor", "estado=", "valor_origen"):
            self.assertNotIn(prohibido + "=%s", src)

    def test_la_constancia_es_de_solo_insercion_y_su_tabla_nace_con_una_sola_sentencia(self):
        t = FA.texto()
        self.assertNotRegex(t, r"(?i)UPDATE\s+ot_valorizado_sugerencias|DELETE\s+FROM\s+ot_valorizado_sugerencias")
        src = FA.fuente_funcion("_ensure_ot_valorizado_sugerencias")
        self.assertEqual(src.count("CREATE TABLE IF NOT EXISTS ot_valorizado_sugerencias"), 1)
        self.assertNotIn("ALTER", src.upper())
        self.assertIn("_ensure_ot_valorizado_sugerencias()", t[t.index("# 🧮 2026-10-29 (REGLA #60): constancia de cada decisión"):][:600])

    def test_sugerir_filas_arma_la_ot_con_la_cuenta_unica(self):
        vistos = {}

        def sugerir(ot, eqs, indice, propias, tarifas, tr, no_cob):
            vistos[ot["id"]] = (ot, eqs, propias, tarifas, tr, no_cob)
            return {"hay": True, "monto": 1}
        filas = [{"id": 1, "tipo": "preventiva", "cliente_id": 9, "valz_region": "Valparaíso", "valz_comuna": "Viña del Mar"},
                 {"id": 2, "tipo": "instalacion", "cliente_id": 9, "valz_region": None, "valz_comuna": ""}]
        fins = {1: {"cobra": False, "cobertura": "garantia", "cobre": {"hay": True, "servicio": 0}},
                2: {"cobra": True, "cobertura": "cobra", "cobre": {"hay": True, "servicio": 450000}}}
        amb = FA.cargar(funciones=["_valz_sugerir_filas"], extra={
            "_valz": types.SimpleNamespace(tipo_servicio_de=V.tipo_servicio_de, sugerir=sugerir),
            "_ot_fin_lote": lambda f, repuestos=True: fins, "_ot_finanzas": lambda r: {},
            "_valz_equipos": lambda vids: {1: EQ_TROT}, "_valz_propias": lambda vids: {},
            "_valz_mercado": lambda: {"cot": {}, "ot": {}}, "_valz_no_cobrables": lambda s: set(),
            "_valz_tarifas": lambda pares: {p: {"unit": 1} for p in pares},
            "_valz_traslados": lambda comunas: {"viña del mar": 35000.0}})
        out, _f = amb["_valz_sugerir_filas"](filas)
        ot1, eqs1, _p, tar1, tr1, _n = vistos[1]
        self.assertEqual((ot1["cobertura"], ot1["region"], ot1["cobrado_servicio"]), ("garantia", "Valparaíso", None))
        self.assertEqual(tr1, {"monto": 35000.0, "comuna": "Viña del Mar", "fuente": "tabla_rutas"})
        self.assertIn(("mantencion", "trotadora"), tar1)
        ot2 = vistos[2][0]
        self.assertEqual((ot2["cobertura"], ot2["cobrado_servicio"]), ("cobra", 450000))
        self.assertIsNone(vistos[2][4])


# ═══════════════════════════════════════ ficha (JS en Node) ═══════════════════════════════════════
def _node(script, datos):
    node = shutil.which("node")
    if not node:
        raise unittest.SkipTest("Node no está instalado")
    with tempfile.TemporaryDirectory() as d:
        ruta, entrada = os.path.join(d, "t.js"), os.path.join(d, "d.json")
        with open(ruta, "w", encoding="utf-8") as fh:
            fh.write(script)
        with open(entrada, "w", encoding="utf-8") as fh:
            json.dump(datos, fh)
        r = subprocess.run([node, ruta, entrada], capture_output=True, text=True, encoding="utf-8", timeout=60)
        if r.returncode != 0:
            raise AssertionError(r.stderr)
        return json.loads(r.stdout)


class TestFichaJS(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        idx = V.indice_mercado([_cot(i, 150000, cliente=9) for i in range(1, 5)], [], AHORA)
        cls.val = V.sugerir(_ot(), EQ_TROT, idx, traslado={"monto": 35000, "comuna": "Viña del Mar"})
        cls.val["ot"] = {"valorizado": {"monto": None}}
        cls.merc = V.sugerir(_ot(cobertura="cobra", cobrado_servicio=300000), EQ_TROT, idx)
        cls.merc["ot"] = {"valorizado": {"monto": None}}
        cls.nada = V.sugerir(_ot(), [], idx)
        cls.nada["ot"] = {"valorizado": {"monto": None}}
        js = os.path.join(RAIZ, "static", "ot_valorizar.js").replace("\\", "/")
        cls.R = _node("""
const fs = require('fs'); const d = JSON.parse(fs.readFileSync(process.argv[2], 'utf8'));
const V = require(%s);
const out = {};
out.val_sa = V.htmlLinea(d.val, true, false); out.val_no_sa = V.htmlLinea(d.val, false, false);
out.merc = V.htmlLinea(d.merc, true, false);
out.nada_sa = V.htmlLinea(d.nada, true, false);
const guardado = JSON.parse(JSON.stringify(d.val)); guardado.ot.valorizado = {monto: 500000, fuente: 'a_mano', fuente_txt: 'Escrito a mano'};
out.guardado = V.htmlLinea(guardado, true, false);
const desc = JSON.parse(JSON.stringify(d.val)); desc.ultima_decision = {decision: 'descartada', usuario: 'daniel', cuando: '29/10/2026 10:00'};
out.desc = V.htmlLinea(desc, true, false);
const mercSin = JSON.parse(JSON.stringify(d.merc)); mercSin.hay = false; out.merc_sin = V.htmlLinea(mercSin, true, false);
out.det = V.htmlDetalle(d.val);
out.clp = [V.clp(600000), V.clp(-1500), V.clp(null)];
process.stdout.write(JSON.stringify(out));
""" % json.dumps(js), {"val": cls.val, "merc": cls.merc, "nada": cls.nada})

    def test_una_linea_con_confianza_fuente_y_los_tres_botones_solo_para_superadmin(self):
        h = self.R["val_sa"]["html"]
        self.assertIn("Valor sugerido ≈ $600.000", h)
        self.assertIn('class="vz-sem alta"', h)
        self.assertIn("Cotizaciones del cliente", h)
        for b in ('data-vz="usar"', 'data-vz="editar"', 'data-vz="descartar"', 'data-vz="ver"'):
            self.assertIn(b, h)
        h2 = self.R["val_no_sa"]["html"]
        for b in ('data-vz="usar"', 'data-vz="editar"', 'data-vz="descartar"'):
            self.assertNotIn(b, h2)
        self.assertIn("lo guarda el superadministrador", h2)

    def test_si_la_ot_se_cobra_solo_informa_y_avisa_en_ambar(self):
        m = self.R["merc"]
        self.assertEqual(m["clase"], "amb")
        self.assertIn("Cobramos $300.000 · el mercado sugiere ≈ $600.000 (50 % bajo)", m["html"])
        for b in ('data-vz="usar"', 'data-vz="editar"', 'data-vz="descartar"'):
            self.assertNotIn(b, m["html"])
        self.assertIsNone(self.R["merc_sin"])                                 # sin datos de mercado no se dice nada

    def test_valorizado_guardado_descartada_y_sin_datos(self):
        self.assertIn("Valorizado $500.000", self.R["guardado"]["html"])
        self.assertIn('data-vz="usar"', self.R["guardado"]["html"])          # «Usar el sugerido» (distinto del guardado)
        self.assertIn("Sugerencia descartada por daniel el 29/10/2026 10:00", self.R["desc"]["html"])
        self.assertEqual(self.R["nada_sa"]["clase"], "mudo")
        self.assertIn("Sin valor sugerido", self.R["nada_sa"]["html"])
        self.assertIn('data-vz="editar"', self.R["nada_sa"]["html"])        # escribirlo a mano, con motivo

    def test_el_detalle_trae_clases_traslado_aparte_y_que_es_solo_referencia(self):
        d = self.R["det"]
        self.assertIn("Trotadora × 4 a $150.000 c/u", d)
        self.assertIn("Viña del Mar", d)
        self.assertIn("aparte (no entra al valorizado)", d)
        self.assertIn("nunca se suma a lo cobrado", d)
        self.assertEqual(self.R["clp"], ["$600.000", "−$1.500", "—"])

    def test_sin_popups_nativos_y_con_los_helpers_ilus(self):
        js = _leer("static", "ot_valorizar.js")
        self.assertNotRegex(js, r"(?<![A-Za-z.])(alert|confirm|prompt)\(")
        for h in ("ilusConfirm", "ilusPrompt", "ilusToast"):
            self.assertIn(h, js)
        self.assertIn("minLength: 15", js)                                   # el motivo del valor editado (REGLA #42)

    def test_la_linea_vive_en_el_bloque_de_costos_fuera_del_motor_y_nunca_para_tecnicos(self):
        import jinja2
        from jinja2 import nodes
        det = _leer("templates", "ot2", "detalle.html")
        i = det.index('id="otdValSug"')
        self.assertLess(det.index('id="otdCardMotor"'), i)
        self.assertLess(i, det.index('id="otdFinFaltan"'))
        # En el árbol de la plantilla: dentro de un {% if not es_tecnico %} y NUNCA dentro de {% if puede_metadata %} (así se ve también con
        # la OT cerrada, que es donde el superadmin la valoriza).
        arbol = jinja2.Environment(extensions=["jinja2.ext.do", "jinja2.ext.loopcontrols"]).parse(det)

        def contiene(n):
            return any('id="otdValSug"' in (td.data or "") for td in n.find_all(nodes.TemplateData))

        def es(n, nombre, negado):
            t = n.test
            if negado:
                return isinstance(t, nodes.Not) and isinstance(t.node, nodes.Name) and t.node.name == nombre
            return isinstance(t, nodes.Name) and t.name == nombre
        ifs = list(arbol.find_all(nodes.If))
        self.assertTrue(any(es(n, "es_tecnico", True) and contiene(n) for n in ifs), "debe ir dentro de {% if not es_tecnico %}")
        self.assertFalse(any(es(n, "puede_metadata", False) and contiene(n) for n in ifs), "no puede depender de puede_metadata")
        self.assertIn("data-val-sug", det[i - 200:i + 300])
        self.assertIn("ot_valorizar.js", det[i - 1200:i])
        self.assertIn('id="otdFinCotiz"', det)                                # «Lo justo según el cotizador» se conserva
        self.assertIn("otdFinValUsarCotizador", det)                          # y «Valorizar con el cotizador» también (REGLA #4.2)


# ═══════════════════════════════════════ pantalla en lote (JS en Node) ═══════════════════════════════════════
_HARNESS_LOTE = r"""
const fs = require('fs');
const {js, esc} = JSON.parse(fs.readFileSync(process.argv[2], 'utf8'));
const els = {};
function El(id){
  return { id, innerHTML:'', textContent:'', style:{}, disabled:false, hidden:false, value:'', _h:{},
    addEventListener(t, f){ (this._h[t] = this._h[t] || []).push(f); },
    get lastChild(){ return this._lc = this._lc || {textContent:''}; },
    get firstElementChild(){ return this._fc = this._fc || {style:{}}; },
    async fire(t, ev){ for (const f of (this._h[t] || [])) await f.call(this, ev || {target:this}); } };
}
global.window = global;
global.document = { getElementById(id){ return els[id] = els[id] || El(id); }, querySelectorAll(){ return []; } };
const gets = [], posts = [], confirms = [], toasts = [];
let enVuelo = 0, maxVuelo = 0;
global.ilusToast = (m, o) => toasts.push(m);
global.ilusConfirm = async (o) => { confirms.push(o); return esc.confirmar !== false; };
function fila(i){ return {id: i, numero: 'OT-2026-' + String(i).padStart(5, '0'), cliente: 'Gimnasio Número ' + i + ' Sociedad Anónima Cerrada de Inversiones Deportivas',
  estado: 'cerrada', tipo: 'preventiva', tipo_txt: 'Mantención preventiva', cobertura: 'garantia', cobertura_txt: 'Garantía', fecha: '20/10/2026',
  equipos: ['Trotadora × 2'], unidades: 2, monto: 100000 * i, rango: {p25: 90000 * i, p75: 110000 * i}, cobertura_equipos: 1,
  confianza: i % 2 ? 'alta' : 'media', fuente: 'cotiz_cliente', fuente_corta: 'Cotizaciones del cliente', texto: 'Esta mantención saldría en…',
  motivos: [], puede_aplicar: i !== 4, descartada: false}; }
const TODAS = [1,2,3,4,5,6,7].map(fila);
global.fetch = async (url, opts) => {
  const metodo = ((opts && opts.method) || 'GET').toUpperCase();
  const cuerpo = opts && opts.body ? JSON.parse(opts.body) : null;
  if (metodo === 'POST') { posts.push({url, cuerpo}); enVuelo++; maxVuelo = Math.max(maxVuelo, enVuelo); await new Promise(r => setTimeout(r, 15)); enVuelo--; }
  else gets.push(url);
  let j;
  if (url.startsWith('/ot/api/valorizacion/lote?')) {
    const q = new URLSearchParams(url.split('?')[1]); const por = +q.get('por'), page = +q.get('page');
    const ini = (page - 1) * por, filas = TODAS.slice(ini, ini + por);
    j = {ok: true, filas, total: TODAS.length, paginas: Math.ceil(TODAS.length / por), page, por, desde: TODAS.length ? ini + 1 : 0,
         hasta: Math.min(ini + por, TODAS.length), n_lista: TODAS.length, periodo: 'Todo el historial',
         kpis: {dejamos_de_cobrar: 1500000, n_valorizadas: 12, internos: 300000, n_internos: 3, por_valorizar_n: 7,
                por_valorizar_con_sugerencia: 6, por_valorizar_sugerido: 2400000, por_valorizar_internos: 0},
         alta: TODAS.filter(f => f.confianza === 'alta' && f.puede_aplicar).map(f => ({id: f.id, numero: f.numero, monto: f.monto}))};
  } else if (/valorizacion-sugerida$/.test(url)) {
    const id = +url.split('/')[3]; j = {ok: true, hay: true, puede_guardar: true, monto: id === 3 ? 333333 : 100000 * id, texto: 'Revisada ' + id, confianza: 'alta'};
  } else if (/valorizacion-aplicar$/.test(url)) {
    const id = +url.split('/')[3];
    if (id === 5) return {ok: false, status: 409, json: async () => ({ok: false, error_codigo: 'SUGERENCIA_CAMBIO', sugerido: 512345, error: 'cambió'})};
    j = {ok: true, escribio: true, valorizado: {monto: cuerpo.monto, fuente: 'cotiz_cliente', fuente_txt: 'Cotizaciones del cliente'}};
  }
  if (!j) return {ok: false, status: 404, json: async () => ({ok: false})};
  return {ok: true, status: 200, json: async () => j};
};
const espera = (ms) => new Promise(r => setTimeout(r, ms || 40));
const clic = (id, btn) => els[id].fire('click', {target: {closest: () => btn}});
const foto = () => ({body: els.vzlBody.innerHTML, pie: els.vzlPie.innerHTML, sel: els.vzlSel.textContent, aplicar: els.vzlAplicar.disabled,
                     revisar: els.vzlRevisar.disabled, kpi: els.vzlKpiDejamos.textContent, alta: els.vzlAlta.lastChild.textContent,
                     aviso: els.vzlAviso.textContent, posts: posts.length, gets: gets.slice()});
(async () => {
  eval(js);
  await espera();
  const out = {inicio: foto()};
  els.vzlPor.value = '25'; await els.vzlPor.fire('change', {}); await espera();
  out.por25 = foto();
  // página 2 con 5 por página: el pie cambia y se pide al servidor
  global.VZL.S.por = 5; await global.VZL.cargar(); await espera();
  await clic('vzlPie', {getAttribute: () => '1', disabled: false}); await espera();
  out.pagina2 = foto();
  await clic('vzlPie', {getAttribute: () => '-1', disabled: false}); await espera();
  // seleccionar todo (interruptor sobre lo visible: 1,2,3,5 — la 4 no se puede aplicar)
  await els.vzlTodo.fire('click', {}); out.todo1 = foto();
  await els.vzlTodo.fire('click', {}); out.todo2 = foto();
  // marcar las de confianza alta (todas las páginas)
  await els.vzlAlta.fire('click', {}); out.alta = foto(); out.altaIds = Object.keys(global.VZL.S.sel);
  // revisar una fila: SOLO GET
  const postsAntes = posts.length;
  await clic('vzlBody', {getAttribute: (n) => ({'data-act': 'revisar', 'data-id': '3'})[n]}); await espera();
  out.revisar = foto(); out.revisarPosts = posts.length - postsAntes;
  // cancelar la confirmación: nada se escribe
  esc.confirmar = false; await els.vzlAplicar.fire('click', {}); await espera(); out.cancelado = {posts: posts.length, confirms: confirms.length};
  // aplicar de verdad: de a 3, con la confirmación que resume cuántas y el total
  esc.confirmar = true; await els.vzlAplicar.fire('click', {}); await espera(150);
  out.aplicado = foto(); out.confirm = confirms[confirms.length - 1]; out.maxVuelo = maxVuelo; out.postsCuerpos = posts.map(p => [p.url, p.cuerpo]);
  out.res = global.VZL.S.res; out.selFinal = Object.keys(global.VZL.S.sel);
  process.stdout.write(JSON.stringify(out));
})().catch(e => { console.error(e && e.stack || e); process.exit(1); });
"""


class TestPantallaLote(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.js = _leer("static", "ot_valorizar_lote.js")
        cls.R = _node(_HARNESS_LOTE, {"js": cls.js, "esc": {}})

    def test_carga_con_nada_marcado_paginada_como_etiquetas(self):
        i = self.R["inicio"]
        self.assertNotIn(" checked", i["body"])
        self.assertIn("Mostrando 1–7 de 7", i["pie"])
        self.assertIn("Página 1 de 1", i["pie"])
        self.assertTrue(i["aplicar"] and i["revisar"])                        # sin selección, no se puede aplicar ni revisar
        self.assertEqual(i["kpi"], "$1.500.000")
        self.assertEqual(i["posts"], 0)
        self.assertIn("Gimnasio Número 1 Sociedad Anónima Cerrada de Inversiones Deportivas", i["body"])   # nombre completo
        self.assertIn("por=25", self.R["por25"]["gets"][-1])
        self.assertIn("page=1", self.R["por25"]["gets"][-1])

    def test_la_pagina_siguiente_se_pide_al_servidor(self):
        p = self.R["pagina2"]
        self.assertIn("page=2", p["gets"][-1])
        self.assertIn("Mostrando 6–7 de 7", p["pie"])
        self.assertIn("Página 2 de 2", p["pie"])

    def test_seleccionar_todo_es_un_interruptor_sobre_lo_visible(self):
        self.assertIn("4 seleccionadas", self.R["todo1"]["sel"])             # 1, 2, 3 y 5 (la 4 no se puede aplicar)
        self.assertFalse(self.R["todo1"]["aplicar"])
        self.assertEqual(self.R["todo2"]["sel"], "")

    def test_marcar_las_de_confianza_alta_de_todas_las_paginas(self):
        self.assertEqual(sorted(self.R["altaIds"], key=int), ["1", "3", "5", "7"])
        self.assertIn("Marcar las de confianza alta (4)", self.R["inicio"]["alta"])

    def test_revisar_solo_lee(self):
        self.assertEqual(self.R["revisarPosts"], 0)
        self.assertTrue(any(u.endswith("/ot/api/3/valorizacion-sugerida") for u in self.R["revisar"]["gets"]))
        self.assertIn("La sugerencia cambió: ahora ≈ $333.333", self.R["revisar"]["body"])

    def test_cancelar_no_escribe_y_aplicar_va_de_a_tres_con_confirmacion(self):
        self.assertEqual(self.R["cancelado"]["posts"], 0)
        c = self.R["confirm"]
        self.assertIn("4 OT", c["title"])
        self.assertIn("en total ≈ $1.633.333", c["message"])                 # 100.000 + 333.333 (la revisada) + 500.000 + 700.000
        self.assertIn("vuelve a calcular", c["sub"])
        self.assertLessEqual(self.R["maxVuelo"], 3)
        self.assertEqual(sorted(int(u.split("/")[3]) for u, _ in self.R["postsCuerpos"]), [1, 3, 5, 7])
        for _, cuerpo in self.R["postsCuerpos"]:
            self.assertEqual(cuerpo["decision"], "aceptada")
            self.assertNotIn("fuente", cuerpo)                                # el navegador nunca manda el rótulo
        self.assertEqual(self.R["res"]["5"]["fase"], "aviso")                 # la sugerencia cambió: no se tocó
        self.assertEqual(self.R["res"]["1"]["fase"], "hecha")
        self.assertEqual(self.R["selFinal"], ["5"])                           # solo queda marcada la que no se tocó
        self.assertIn("Se guardaron 3 de 4", self.R["aplicado"]["aviso"])

    def test_plantilla_y_css_cumplen_las_reglas(self):
        t = _leer("templates", "ot2", "valorizar.html")
        css = _leer("static", "ot_valorizar.css")
        for src in (t, self.js):
            self.assertNotRegex(src, r"(?<![A-Za-z.])(alert|confirm|prompt)\(")   # REGLA #1
        self.assertIn("ilusConfirm", self.js)
        self.assertIn("Seleccionar/deseleccionar todo", t)                    # REGLA #14
        self.assertIn("Marcar las de confianza alta", t)
        for txt in ("Mostrando ", "Anterior", "Siguiente", "Página "):        # REGLA #4.3
            self.assertIn(txt, self.js)
        self.assertIn("por página", t)
        self.assertIn("min-height:44px", css)                                 # REGLA #3
        self.assertIn("font-size:16px", css)
        for src in (t, self.js, css):
            self.assertNotRegex(src, r"text-overflow\s*:\s*ellipsis|line-clamp")   # REGLA #15
        self.assertNotRegex(css, r"border-left\s*:\s*[4-9]px")               # semáforo chico, sin bordes laterales gruesos
        self.assertIn("Dejamos de cobrar", t)
        self.assertIn("Se nos escapa en internos", t)
        visible = re.sub(r"\{#.*?#\}", "", t, flags=re.S)
        self.assertNotIn("Nos costó", visible)                                # los dos números nunca se mezclan en pantalla

    def test_el_boton_esta_en_la_barra_del_superadmin(self):
        panel = _leer("templates", "ot2", "panel.html")
        self.assertIn("url_for('ot2_valorizar_pagina')", panel)
        self.assertLess(panel.index("ot2-sa-bar"), panel.index("ot2_valorizar_pagina"))
        self.assertLess(panel.index("{% if es_superadmin_ot %}"), panel.index("ot2_valorizar_pagina"))

    def test_las_plantillas_son_jinja_valido(self):
        import jinja2
        env = jinja2.Environment(extensions=["jinja2.ext.do", "jinja2.ext.loopcontrols"])
        for p in (("templates", "ot2", "valorizar.html"), ("templates", "ot2", "detalle.html"), ("templates", "ot2", "panel.html")):
            env.parse(_leer(*p))


class TestReglaDocumentada(unittest.TestCase):
    def test_regla_60_en_claude_md(self):
        md = _leer("CLAUDE.md")
        i = md.index("REGLA #60")
        bloque = md[i:i + 6000]
        for txt in ("ot_valorizar.py", "valorizacion-aplicar", "ot_valorizado_sugerencias", "a_mano", "Nos costó"):
            self.assertIn(txt, bloque)


if __name__ == "__main__":
    unittest.main()

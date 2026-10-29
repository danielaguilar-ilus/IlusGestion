"""🚚 LOGÍSTICA (WMS) DE CADA GUÍA DE DESPACHO + «lo que salió» vs «lo que gestiona la OT» + alerta (Daniel, 29-oct-2026).

Daniel: «En la guía de despacho, presiono y no hicimos la conexión con el WMS, que es la parte logística. Me gustaría
verlo, para saber QUÉ SE FUE. Y hacer una comparación de qué se fue en la guía con los productos que se gestionaron en la
OT. Eso es inteligencia y percepción. Y levantar una alerta, obviamente.»

Qué se vigila (todo con datos de mentira: nunca producción, nunca Check ni Random reales):
  A) El módulo puro (ot_guia_vs_ot.py): coincide · salió y no está en la OT · está en la OT y no salió · cantidad distinta,
     SKU con formatos distintos, servicios fuera, sin guía / sin gestionados = «sin datos», varias guías, falta de stock
     declarada, otra OT de la misma venta, equipos del cliente, lectura de Check y movimientos, hallazgos y campana.
  B) El cableado en app.py: 403 a técnicos ANTES de leer nada, Check solo GET y solo la lista blanca, Random solo SELECT,
     si Check o Random fallan la ficha sigue, la tabla solo crece (nunca DELETE), la campana sale UNA vez por hallazgo a la
     gestión de la OT y jamás al cliente, el hallazgo se resuelve solo, el ENUM tiene el tipo nuevo al final, sin montos.
  C) El JS (Node, con un DOM de mentira): chip, colores verde/ámbar/gris, clic que carga el detalle, sin nativos.
Correr con:  python -m unittest tests.test_ot_guia_logistica
"""
import ast
import datetime
import json
import os
import re
import shutil
import subprocess
import time
import unittest

import ot_doc_cadena as C
import ot_guia_vs_ot as G
import ot_saldo_servicio as S
from tests import _fuente_app as FA

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _leer(rel):
    with open(os.path.join(RAIZ, rel.replace("/", os.sep)), encoding="utf-8") as f:
        return f.read()


def _guia(num, lineas, anulada=False, desde="FCV 11745"):
    return {"numero": num, "fecha": "08/10/2026", "desde": desde, "anulada": anulada,
            "lineas": [{"sku": s, "nombre": n, "cantidad": q} for s, n, q in lineas]}


def _eq(sku, nombre, q=1, declarado=None):
    return {"sku": sku, "nombre": nombre, "cantidad": q, "declarado": declarado}


GUIA_33193 = _guia("33193", [("BB-01", "Booty Builder Cushioned Belt", 1), ("ZZENVIO", "Despacho a domicilio", 1)])
EQUIPOS_3 = [_eq("bb-01", "Booty Builder Cushioned Belt"), _eq("0X1", "Trotadora Matrix T70"), _eq("Y2", "Bicicleta Spinning S3")]


def _comparar(guias, equipos, repuestos=(), tipo="instalacion", otras=None):
    gest, fuera = G.armar_gestionados(tipo, equipos, list(repuestos))
    return G.comparar(guias, gest, otras_ot=otras, no_comparados=fuera)


# ═══════════════════════════ A) el módulo puro ═══════════════════════════
class TestSku(unittest.TestCase):
    def test_normaliza_mayusculas_espacios_y_ceros(self):
        self.assertEqual(G.norm_sku(" 00123ab "), "123AB")
        self.assertEqual(G.norm_sku("mt 100"), "MT100")
        self.assertEqual(G.norm_sku("000"), "0")
        self.assertEqual(G.norm_sku(None), "")
        self.assertEqual(G.norm_sku("BB-01"), G.norm_sku(" bb-01"))

    def test_cantidades_en_espanol(self):
        self.assertEqual(G.cant_txt(2.0), "2")
        self.assertEqual(G.cant_txt("2,5"), "2,5")
        self.assertEqual(G.cantidad("basura"), 0.0)
        self.assertEqual(G.cantidad(-3), 0.0)


class TestComparacion(unittest.TestCase):
    def test_caso_de_daniel_faltan_por_salir_dos(self):
        """«La guía GDV 33193 sacó 1 × Booty Builder Cushioned Belt; la OT gestiona 3 equipos: faltan por salir 2 (X, Y)»."""
        c = _comparar([GUIA_33193], EQUIPOS_3)
        self.assertEqual(c["estado"], "diferencias")
        self.assertEqual(c["n_diferencias"], 2)
        self.assertEqual(c["veredicto"],
                         "La guía GDV 33193 sacó 1 × Booty Builder Cushioned Belt; la OT gestiona 3 equipos: "
                         "faltan por salir 2 (Bicicleta Spinning S3 y Trotadora Matrix T70).")
        estados = {p["sku_norm"]: p["estado"] for p in c["productos"]}
        self.assertEqual(estados, {"BB-01": "coincide", "X1": "esta_en_la_OT_y_no_salio", "Y2": "esta_en_la_OT_y_no_salio"})
        self.assertEqual(c["servicios_omitidos"], 1, "el despacho (ZZENVIO) no es un producto físico")
        # la guía cuadra en lo suyo, pero la OT tiene productos que no salieron: ámbar
        self.assertEqual(c["por_guia"]["33193"]["color"], "ambar")
        self.assertEqual(c["por_guia"]["33193"]["n_dif"], 0)
        self.assertEqual([p["nombre"] for p in c["no_salio"]], ["Bicicleta Spinning S3", "Trotadora Matrix T70"])

    def test_todo_coincide_es_verde(self):
        c = _comparar([GUIA_33193], [_eq("BB-01", "Booty Builder Cushioned Belt")])
        self.assertEqual((c["estado"], c["color"], c["n_diferencias"]), ("coincide", "verde", 0))
        self.assertEqual(c["por_guia"]["33193"]["color"], "verde")
        self.assertIn("coincide con lo que gestiona la OT (1 equipo)", c["veredicto"])
        self.assertEqual(G.hallazgos(c), [])

    def test_salio_algo_que_la_ot_no_gestiona(self):
        g = _guia("40001", [("BB-01", "Booty Builder Cushioned Belt", 1), ("MAN-10", "Mancuerna hexagonal 10 kg", 2)])
        c = _comparar([g], [_eq("BB-01", "Booty Builder Cushioned Belt")])
        p = next(x for x in c["productos"] if x["sku_norm"] == "MAN-10")
        self.assertEqual(p["estado"], "salio_y_no_esta_en_la_OT")
        self.assertTrue(p["es_diferencia"])
        self.assertIn("Salió 2 en GDV 40001 y la OT no lo gestiona", p["texto"])
        self.assertIn("salió sin estar en la OT: 2 × Mancuerna hexagonal 10 kg", c["veredicto"])
        self.assertEqual(c["por_guia"]["40001"]["color"], "ambar")
        self.assertEqual(c["por_guia"]["40001"]["n_dif"], 1)

    def test_cantidad_distinta_falta_y_sobra(self):
        g = _guia("40002", [("BAN-1", "Banco ajustable", 1), ("DISC-5", "Disco olímpico 5 kg", 6)])
        c = _comparar([g], [_eq("BAN-1", "Banco ajustable", 3), _eq("DISC-5", "Disco olímpico 5 kg", 4)])
        por = {p["sku_norm"]: p for p in c["productos"]}
        self.assertEqual((por["BAN-1"]["estado"], por["BAN-1"]["falta"]), ("cantidad_distinta", 2))
        self.assertIn("faltan por salir 2", por["BAN-1"]["texto"])
        self.assertEqual((por["DISC-5"]["estado"], por["DISC-5"]["sobra"]), ("cantidad_distinta", 2))
        self.assertIn("salieron 2 de más", por["DISC-5"]["texto"])
        self.assertIn("salió de más: 2 × Disco olímpico 5 kg", c["veredicto"])

    def test_sku_con_formatos_distintos_se_emparejan(self):
        g = _guia("40003", [("  0bb-01 ", "Booty Builder Cushioned Belt", "1,0")])
        c = _comparar([g], [_eq("BB-01", "Booty Builder Cushioned Belt")])
        self.assertEqual(c["estado"], "coincide")

    def test_sin_guia_no_es_discrepancia(self):
        c = _comparar([], EQUIPOS_3)
        self.assertEqual((c["estado"], c["motivo"], c["color"]), ("sin_datos", "sin_guias", "gris"))
        self.assertIn("todavía no tiene guía de despacho", c["veredicto"])
        self.assertEqual(G.hallazgos(c), [])

    def test_solo_guias_anuladas_no_cuentan(self):
        c = _comparar([_guia("1", [("X1", "Trotadora", 1)], anulada=True)], EQUIPOS_3)
        self.assertEqual((c["estado"], c["motivo"]), ("sin_datos", "solo_anuladas"))
        self.assertEqual(c["anuladas"], ["1"])

    def test_sin_productos_gestionados_no_es_discrepancia(self):
        c = _comparar([GUIA_33193], [])
        self.assertEqual((c["estado"], c["motivo"]), ("sin_datos", "sin_gestionados"))
        self.assertEqual(G.hallazgos(c), [])
        self.assertEqual(c["por_guia"]["33193"]["color"], "gris")

    def test_mantencion_los_equipos_son_del_cliente_y_solo_cuentan_repuestos(self):
        g = _guia("40004", [("REP-77", "Correa de transmisión", 1)])
        c = _comparar([g], EQUIPOS_3, repuestos=[{"sku": "REP-77", "nombre": "Correa de transmisión", "cantidad": 1}],
                      tipo="preventiva")
        self.assertEqual(c["estado"], "coincide")
        self.assertEqual(len(c["equipos_del_cliente"]), 3)
        sin = _comparar([g], EQUIPOS_3, tipo="preventiva")
        self.assertEqual((sin["estado"], sin["motivo"]), ("sin_datos", "sin_gestionados"))
        self.assertIn("son del cliente", sin["veredicto"])

    def test_varias_guias_se_suman_y_cada_una_tiene_su_color(self):
        g1 = _guia("50001", [("X1", "Trotadora Matrix T70", 1)])
        g2 = _guia("50002", [("Y2", "Bicicleta Spinning S3", 1), ("Z9", "Remo Concept2", 1)])
        c = _comparar([g1, g2], [_eq("X1", "Trotadora Matrix T70"), _eq("Y2", "Bicicleta Spinning S3")])
        self.assertEqual(c["estado"], "diferencias")
        self.assertEqual(c["por_guia"]["50001"]["color"], "verde", "lo de esta guía cuadra y no falta nada")
        self.assertEqual(c["por_guia"]["50002"]["color"], "ambar")
        self.assertIn("Las guías GDV 50001 y GDV 50002 sacaron", c["veredicto"])
        g3 = _guia("50003", [("X1", "Trotadora Matrix T70", 1)])
        c2 = _comparar([g1, g3], [_eq("X1", "Trotadora Matrix T70", 2)])
        self.assertEqual(c2["estado"], "coincide", "1 + 1 en dos guías = 2 gestionados")

    def test_falta_de_stock_declarada_no_es_diferencia(self):
        c = _comparar([GUIA_33193], [_eq("BB-01", "Booty Builder Cushioned Belt"),
                                     _eq("X1", "Trotadora Matrix T70", declarado="falta_stock")])
        p = next(x for x in c["productos"] if x["sku_norm"] == "X1")
        self.assertEqual(p["estado"], "esta_en_la_OT_y_no_salio")
        self.assertFalse(p["es_diferencia"])
        self.assertIn("ya declaró falta de stock", p["texto"])
        self.assertEqual(c["estado"], "coincide")

    def test_lo_que_sobra_y_gestiona_otra_ot_de_la_misma_venta(self):
        g = _guia("60001", [("X1", "Trotadora Matrix T70", 3)])
        otras = [{"vid": 77, "numero_ot": "OT-2026-00377", "productos": [{"sku": "X1", "cantidad": 2}]}]
        c = _comparar([g], [_eq("X1", "Trotadora Matrix T70", 1)], otras=otras)
        p = c["productos"][0]
        self.assertEqual(p["estado"], "cantidad_distinta")
        self.assertFalse(p["es_diferencia"])
        self.assertEqual(p["explicado_por"], ["OT-2026-00377"])
        self.assertIn("Lo gestiona OT-2026-00377 (misma venta)", p["texto"])
        otras[0]["productos"][0]["cantidad"] = 1
        c2 = _comparar([g], [_eq("X1", "Trotadora Matrix T70", 1)], otras=otras)
        self.assertTrue(c2["productos"][0]["es_diferencia"], "solo una parte explicada: sigue siendo diferencia")

    def test_sin_sku_no_se_compara_pero_se_dice(self):
        c = _comparar([GUIA_33193], [_eq("BB-01", "Booty Builder Cushioned Belt"), _eq("", "Equipo sin código")])
        self.assertEqual(c["estado"], "coincide")
        self.assertEqual([x["nombre"] for x in c["sin_sku"]], ["Equipo sin código"])

    def test_nunca_hay_montos(self):
        c = _comparar([GUIA_33193], EQUIPOS_3)
        txt = json.dumps(c, ensure_ascii=False)
        self.assertNotIn("$", txt)
        for k in ("valor", "monto", "precio", "costo", "saldo"):
            self.assertNotIn('"%s' % k, txt)


class TestHallazgos(unittest.TestCase):
    def test_un_hallazgo_por_producto_con_texto_claro(self):
        hs = G.hallazgos(_comparar([GUIA_33193], EQUIPOS_3))
        self.assertEqual(sorted((h["codigo"], h["detalle"]) for h in hs),
                         [("esta_en_la_OT_y_no_salio", "X1"), ("esta_en_la_OT_y_no_salio", "Y2")])
        self.assertIn("Trotadora Matrix T70 (0X1): la OT gestiona 1 y no salió en ninguna guía.", [h["texto"] for h in hs])

    def test_plan_avisa_una_sola_vez_y_resuelve_solo(self):
        act = {("esta_en_la_OT_y_no_salio", "X1"): {"codigo": "esta_en_la_OT_y_no_salio", "detalle": "X1", "texto": "t", "guias": ""}}
        p1 = G.plan_hallazgos(act, [])
        self.assertEqual((len(p1["insertar"]), len(p1["avisar"])), (1, 1))
        guardada = [{"id": 1, "codigo": "esta_en_la_OT_y_no_salio", "detalle": "X1", "estado": "vigente", "texto": "t",
                     "guias": "", "notificada_el": datetime.datetime(2026, 10, 29)}]
        p2 = G.plan_hallazgos(act, guardada)
        self.assertEqual((p2["insertar"], p2["avisar"], p2["actualizar"]), ([], [], []))
        p3 = G.plan_hallazgos({}, guardada)
        self.assertEqual([r["id"] for r in p3["resolver"]], [1])
        guardada[0]["estado"] = "resuelta"
        p4 = G.plan_hallazgos(act, guardada)
        self.assertEqual(len(p4["reabrir"]), 1)
        self.assertEqual(p4["avisar"], [], "una sola vez por hallazgo: volver no repite la campana")
        guardada[0].update(estado="vigente", notificada_el=None)
        self.assertEqual(len(G.plan_hallazgos(act, guardada)["avisar"]), 1, "si el aviso no alcanzó a salir, se reintenta")

    def test_campana_amable_sin_montos(self):
        hs = G.hallazgos(_comparar([GUIA_33193], EQUIPOS_3))
        t, c = G.texto_campana("OT-2026-00171", "Aurum Fit Girls Spa", hs)
        self.assertEqual(t, "OT-2026-00171: 2 diferencias entre la guía de despacho y la OT")
        self.assertIn("Aurum Fit Girls Spa", c)
        self.assertIn("«Ver logística»", c)
        self.assertNotIn("..", c)
        self.assertNotIn("$", c)
        for feo in ("error", "culpa", "falla tuya", "incumpl"):
            self.assertNotIn(feo, c.lower())


def _fila_check(sku, desc, solicitado, despachado=0, pickeado=0, tipo="GDV", num="0000033193", **extra):
    f = {"tipoDocumento": tipo, "numeroDocumento": num, "codigoMaterial": sku, "material": desc,
         "solicitado": str(solicitado), "noAsignado": "0", "asignado": "0", "pickeado": str(pickeado), "revisado": "0",
         "despachado": str(despachado), "cancelado": "0", "devuelto": "0", "valor": "59990",
         "fechaEmision": "2026-10-08", "fechaEntrega": "2026-10-09"}
    f.update(extra)
    return f


class TestCheck(unittest.TestCase):
    def test_lineas_de_check_sin_valor_y_solo_del_documento(self):
        filas = [_fila_check("BB-01", "Booty Builder Cushioned Belt", 1, despachado=1),
                 _fila_check("OTRO", "De otro documento", 1, num="0000099999")]
        lin = G.lineas_check(filas, tipo="GDV", num="33193")
        self.assertEqual([l["sku"] for l in lin], ["BB-01"])
        self.assertEqual((lin[0]["pedidas"], lin[0]["despachadas"]), (1, 1))
        self.assertEqual(lin[0]["descripcion"], "Booty Builder Cushioned Belt")
        self.assertNotIn("59990", json.dumps(G.bodega(lin, "GDV 33193"), default=str))

    def test_bodega_verde_ambar_gris(self):
        verde = G.bodega(G.lineas_check([_fila_check("A", "a", 2, despachado=2)]), "GDV 1")
        self.assertEqual((verde["estado"], verde["color"]), ("ok", "verde"))
        self.assertEqual(verde["texto"], "Bodega expidió todo: 2 de 2 unidades.")
        parcial = G.bodega(G.lineas_check([_fila_check("A", "a", 3, despachado=1)]), "GDV 1")
        self.assertEqual(parcial["color"], "ambar")
        juntado = G.bodega(G.lineas_check([_fila_check("A", "a", 3, pickeado=3)]), "GDV 1")
        self.assertIn("juntó 3 de 3", juntado["texto"])
        nada = G.bodega(G.lineas_check([_fila_check("A", "a", 3)]), "GDV 1")
        self.assertEqual(nada["color"], "gris")
        self.assertEqual([e["clave"] for e in verde["etapas"]], ["pedido", "pickeado", "despachado"])

    def test_sin_datos_sin_conexion_y_sin_credenciales_no_inventan(self):
        sd = G.bodega([], "", ["GDV 33193", "FCV 11745"])
        self.assertEqual(sd["estado"], "sin_datos")
        self.assertEqual(sd["texto"], "Sin información de bodega (Check) para GDV 33193 ni para FCV 11745.")
        self.assertEqual(G.bodega(None, respondio=False)["estado"], "sin_conexion")
        self.assertIn("Check no respondió ahora", G.bodega(None, respondio=False)["texto"])
        self.assertEqual(G.bodega(None, configurado=False)["estado"], "sin_credenciales")

    def test_cruzar_lo_que_expidio_bodega_por_sku(self):
        bod = G.bodega(G.lineas_check([_fila_check("BB-01", "b", 1, despachado=1)]), "GDV 33193")
        out = G.cruzar_bodega([{"sku": "bb-01 ", "nombre": "Booty", "cantidad": 1}, {"sku": "Z", "nombre": "z", "cantidad": 1}], bod)
        self.assertEqual(out[0]["bodega"], {"pedidas": 1, "pickeadas": 1, "despachadas": 1})
        self.assertIsNone(out[1]["bodega"])

    def test_movimientos_quien_cuando_y_ua(self):
        filas = [{"doc": "GDV - 0000033193", "ot": "4567", "tipoOT": "PICKING MATERIAL", "estado": "FINALIZADA",
                  "feInicioOT": "2026-10-08T10:20:00", "fechaFin": "2026-10-08T10:45:00", "usuario": "Luis Pérez",
                  "codigo": "BB-01", "ua": "UA1012965", "ejec": "1", "sol": "1"},
                 {"doc": "FCV-0000011111", "ot": "9", "usuario": "Otra persona"}]
        m = G.movimientos(filas, [("GDV", "33193")], edad_min=4)
        self.assertEqual(m["estado"], "ok")
        o = m["ots"][0]
        self.assertEqual((o["ot"], o["tipo"], o["estado"]), ("4567", "PICKING MATERIAL", "FINALIZADA"))
        self.assertEqual(o["personas"], ["Luis Pérez"])
        self.assertEqual(o["uas"], ["UA1012965"])
        self.assertEqual(o["inicio"], "08/10/2026 10:20")
        self.assertEqual(G.movimientos(filas, [("GDV", "1")])["estado"], "sin_datos")


class TestSemaforo(unittest.TestCase):
    """«Quiero a Check en este reporte… todo bien resumido»: una línea por guía con un semáforo chico."""

    def _bod(self, *filas):
        return G.bodega(G.lineas_check(list(filas)), "GDV 33193")

    def test_verde_salio_todo_lo_gestionado(self):
        c = _comparar([GUIA_33193], [_eq("BB-01", "Booty Builder Cushioned Belt")])
        s = G.semaforo_guia(c, "33193", self._bod(_fila_check("BB-01", "b", 1, despachado=1)))
        self.assertEqual((s["color"], s["texto"]), ("verde", "Salió todo lo gestionado · bodega expidió 1 de 1"))

    def test_ambar_si_falta_algo_frente_a_la_ot_aunque_bodega_lo_expidio(self):
        c = _comparar([GUIA_33193], EQUIPOS_3)
        s = G.semaforo_guia(c, "33193", self._bod(_fila_check("BB-01", "b", 1, despachado=1)))
        self.assertEqual(s["color"], "ambar")
        self.assertEqual(s["texto"], "Faltan por salir 2 de lo que gestiona la OT · bodega expidió 1 de 1")

    def test_ambar_si_bodega_no_expidio_todo_lo_de_la_guia(self):
        c = _comparar([GUIA_33193], [_eq("BB-01", "Booty Builder Cushioned Belt")])
        s = G.semaforo_guia(c, "33193", self._bod(_fila_check("BB-01", "b", 1)))
        self.assertEqual((s["color"], s["bodega"]), ("ambar", "ambar"))
        self.assertIn("bodega expidió 0 de 1", s["texto"])

    def test_gris_sin_informacion_de_bodega(self):
        c = _comparar([GUIA_33193], [_eq("BB-01", "Booty Builder Cushioned Belt")])
        s = G.semaforo_guia(c, "33193", G.bodega([], "", ["GDV 33193"]))
        self.assertEqual(s["color"], "gris")
        self.assertEqual(s["texto"], "Salió lo que gestiona la OT · sin información de bodega (Check)")
        self.assertIn("Check no respondió ahora", G.semaforo_guia(c, "33193", G.bodega(None, respondio=False))["texto"])
        p = G.semaforo_guia(c, "33193", None, pendiente=True)
        self.assertTrue(p["pendiente"])
        self.assertIn("consultando la bodega", p["texto"])

    def test_si_check_informa_la_factura_se_mide_solo_lo_de_esta_guia(self):
        c = _comparar([GUIA_33193], [_eq("BB-01", "Booty Builder Cushioned Belt")])
        bod = G.bodega(G.lineas_check([_fila_check("BB-01", "b", 1, despachado=1, tipo="FCV", num="11745"),
                                        _fila_check("OTRO-9", "otro de la factura", 5, tipo="FCV", num="11745")]), "FCV 11745")
        self.assertEqual(bod["color"], "ambar", "la factura completa no está expedida")
        s = G.semaforo_guia(c, "33193", bod)
        self.assertEqual(s["color"], "verde", "pero lo de ESTA guía sí salió")

    def test_random_caido_es_gris(self):
        s = G.semaforo_guia({"estado": "sin_lectura"}, "33193", None)
        self.assertEqual((s["color"], s["texto"]), ("gris", "Random no respondió"))

    def test_modulo_puro(self):
        arbol = ast.parse(_leer("ot_guia_vs_ot.py"))
        mods = set()
        for n in ast.walk(arbol):
            if isinstance(n, ast.Import):
                mods |= {a.name for a in n.names}
            elif isinstance(n, ast.ImportFrom):
                mods.add(n.module)
        self.assertFalse({"flask", "pymysql", "pymssql", "requests", "app", "urllib"} & mods, mods)


# ═══════════════════════════ B) el cableado en app.py ═══════════════════════════
APP = FA.texto("app.py")
_INI = APP.index("#  🚚 2026-10-29 — LOGÍSTICA (WMS)")
BLOQUE = APP[_INI:APP.index("#  📒 2026-10-11 — LIBRO DE SALDO DE FACTURAS")]
FUNCS = ["_ot_logi_tabla", "_ot_logi_guias_lineas", "_ot_logi_gestionados", "_ot_logi_otras_ot", "_ot_logi_check_doc",
         "_ot_logi_movimientos", "_ot_logi_bodega", "_ot_logi_avisar", "_ot_logi_registrar", "_ot_logistica",
         "ot_api_documentos_logistica", "_ot_saldo_real"]
CONSTS = ["_OT_LOGI_GUIAS_CACHE", "_OT_LOGI_GUIAS_TTL", "_OT_LOGI_CHECK_CACHE", "_OT_LOGI_CHECK_TTL", "_OT_LOGI_CHECK_TTL_ERR",
          "_OT_LOGI_CHECK_TIMEOUT", "_OT_LOGI_CHECK_PRESUPUESTO", "_OT_LOGI_CHECK_MAX_DOCS", "_OT_LOGI_MAX_OTRAS",
          "_OT_LOGI_MAX_GUIAS_CHECK", "_OT_LOGI_TABLA"]
_COMPILADO = []


def _cargar(extra):
    """Como tests/_fuente_app.cargar, pero recorta y compila UNA vez (app.py pesa ~10 MB) y ejecuta en un ámbito nuevo
    cada vez: cada llamada parte con las cachés vacías."""
    if not _COMPILADO:
        for c in CONSTS:
            _COMPILADO.append(compile(ast.parse(FA.fuente_constante(c)), "<app.py:%s>" % c, "exec"))
        for f in FUNCS:
            arbol = ast.parse(FA.fuente_funcion(f))
            for nodo in arbol.body:
                if isinstance(nodo, ast.FunctionDef):
                    nodo.decorator_list = []
            _COMPILADO.append(compile(arbol, "<app.py:%s>" % f, "exec"))
    amb = dict(extra)
    for codigo in _COMPILADO:
        exec(codigo, amb)
    return amb
PERMITIDOS = set(re.findall(r'"([^"]+)"', re.search(r"_CHECKWMS_GET_PERMITIDOS\s*=\s*frozenset\(\{(.*?)\}\)", APP, re.S).group(1)))

CADENA_OT = {"cadenas": [{"leida": True, "nodos": [
    {"clave": ["VD", "10414"], "tipo": "VD", "numero": "10414", "titulo": "VD 10414", "saldo": {"servicio": {"total": 961640}}},
    {"clave": ["FCV", "11745"], "tipo": "FCV", "numero": "11745", "titulo": "FCV 11745", "usos": [{"servicio": 252101}]},
    {"clave": ["GDV", "33193"], "tipo": "GDV", "numero": "33193", "titulo": "GDV 33193", "desde": "FCV 11745", "fecha": "08/10/2026"}]}],
    "sugerencias": [], "alertas": [], "leido": True}


class Mundo(object):
    """Base, Random y Check de mentira. Registra todo lo que se le pide."""

    def __init__(self, tipo_ot="instalacion", equipos=None, random_ok=True, check=None, traza=None, tecnico=False,
                 configurado=True):
        self.tipo_ot = tipo_ot
        self.equipos = equipos if equipos is not None else [
            {"vid": 5, "mid": 1, "sku": "BB-01", "nombre": "Booty Builder Cushioned Belt", "cant": 1},
            {"vid": 5, "mid": 2, "sku": "X1", "nombre": "Trotadora Matrix T70", "cant": 1},
            {"vid": 5, "mid": 3, "sku": "Y2", "nombre": "Bicicleta Spinning S3", "cant": 1}]
        self.random_ok, self.check, self.traza, self.tecnico, self.configurado = random_ok, check, traza, tecnico, configurado
        self.alertas, self.notifs, self.logs, self.sql, self.random_sql, self.check_llamadas = [], [], [], [], [], []
        self.lecturas = 0
        self.swr = []

    # ── base ──
    def fetchone(self, sql, p=None):
        self.lecturas += 1
        self.sql.append(sql)
        if "FROM mant_visitas v LEFT JOIN mant_clientes" in sql:
            return {"id": 5, "numero_ot": "OT-2026-00171", "tipo": self.tipo_ot, "cliente_id": 9,
                    "razon_social": "Aurum Fit Girls Spa", "zz_monto": 252101, "costo": 120000}
        if "FROM mant_notificaciones" in sql:
            for n in reversed(self.notifs):
                if n["tipo"] == p[0] and n["visita_id"] == p[1] and n["uid"] == p[2] and not n["leida"]:
                    return {"id": n["id"]}
            return None
        return None

    def fetchall(self, sql, p=None):
        self.lecturas += 1
        self.sql.append(sql)
        if "FROM ot_guia_alerta" in sql:
            return [dict(a) for a in self.alertas if a["visita_id"] == p[0]]
        if "mant_visita_tareas" in sql:
            return [dict(e) for e in self.equipos]
        if "mant_ot_producto_estado" in sql or "mant_visita_repuestos" in sql or "mant_visita_documentos" in sql:
            return []
        return []

    def execute(self, sql, p=None):
        self.sql.append(sql)
        s = " ".join(sql.split())
        if s.startswith("INSERT INTO ot_guia_alerta"):
            if any(a["visita_id"] == p[0] and (a["codigo"], a["detalle"]) == (p[1], p[2]) for a in self.alertas):
                return
            self.alertas.append({"id": len(self.alertas) + 1, "visita_id": p[0], "codigo": p[1], "detalle": p[2], "texto": p[3],
                                 "guias": p[4], "estado": "vigente", "veces": 1, "notificada_el": None})
        elif s.startswith("UPDATE ot_guia_alerta SET estado='vigente'"):
            a = next(a for a in self.alertas if a["id"] == p[3])
            a.update(estado="vigente", veces=a["veces"] + 1, texto=p[0])
        elif s.startswith("UPDATE ot_guia_alerta SET estado='resuelta'"):
            next(a for a in self.alertas if a["id"] == p[2])["estado"] = "resuelta"
        elif s.startswith("UPDATE ot_guia_alerta SET texto="):
            next(a for a in self.alertas if a["id"] == p[3])["texto"] = p[0]
        elif s.startswith("UPDATE ot_guia_alerta SET notificada_el="):
            for a in self.alertas:
                if a["visita_id"] == p[1] and a["estado"] == "vigente" and a["notificada_el"] is None:
                    a["notificada_el"] = p[0]
        elif s.startswith("UPDATE mant_notificaciones"):
            next(n for n in self.notifs if n["id"] == p[3]).update(titulo=p[0], cuerpo=p[1])

    filas_random = [
        {"guia": "0000033193", "linea": "1", "sku": "BB-01", "producto": "Booty Builder Cushioned Belt", "cantidad": 1.0,
         "tidopa": "FCV", "nudopa": "0000011745", "fecha": datetime.datetime(2026, 10, 8), "esdo": ""},
        {"guia": "0000033193", "linea": "2", "sku": "ZZENVIO", "producto": "Despacho", "cantidad": 1.0,
         "tidopa": "FCV", "nudopa": "0000011745", "fecha": datetime.datetime(2026, 10, 8), "esdo": ""}]

    def random(self, sql, params=None, max_rows=500):
        self.random_sql.append((sql, params))
        if not self.random_ok:
            return None
        return [dict(f) for f in self.filas_random]

    def checkwms(self, path, params, timeout=60):
        self.check_llamadas.append((path, dict(params), timeout))
        if self.check is None:
            return None
        return self.check(params)

    def notificar(self, uid, tipo, titulo, cuerpo="", url_accion="", prioridad="media", cliente_id=None, visita_id=None):
        self.notifs.append({"id": len(self.notifs) + 1, "uid": uid, "tipo": tipo, "titulo": titulo, "cuerpo": cuerpo,
                            "url": url_accion, "visita_id": visita_id, "leida": False})
        return len(self.notifs)

    def ambiente(self):
        mundo = self
        traza = {"rows": self.traza, "ts": time.time() - 120 if self.traza is not None else 0}

        def _no(*a, **k):
            raise AssertionError("un técnico no debería llegar a leer nada")
        amb = _cargar({
            "mysql_fetchone": (_no if self.tecnico else self.fetchone), "mysql_fetchall": (_no if self.tecnico else self.fetchall),
            "mysql_execute": (_no if self.tecnico else self.execute), "_random_sql_query": (_no if self.tecnico else self.random),
            "_checkwms_get": (_no if self.tecnico else self.checkwms), "_es_rol_tecnico": lambda: mundo.tecnico,
            "_checkwms_configurado": lambda: mundo.configurado, "_CHECKWMS_TRAZA": traza, "_CHECKWMS_STALE_MAX": 6 * 3600,
            "_CHECKWMS_TRAZA_TTL": 3600, "_checkwms_swr": lambda clave, fn: mundo.swr.append(clave),
            "_checkwms_trazabilidad_rows": lambda forzar=False: None,
            "_ot_doc_cadena": (_no if self.tecnico else (lambda vid: json.loads(json.dumps(CADENA_OT)))),
            "_OT_CADENA_MAX_DOCS": 12, "_saldo": S, "_cadena": C, "_gvo": G, "time": time, "re": re,
            "datetime": datetime.datetime, "_sku_es_servicio": lambda sku, nombre="": G.es_servicio_basico(sku, nombre),
            "_hall_destinos": lambda vid, v, excluir_uid=None: [p for p in [{"id": 2, "nombre": "Víctor Soto", "por": "responsable"},
                                                                           {"id": 3, "nombre": "Aarón Díaz", "por": "creador"}]
                                                               if p["id"] != excluir_uid],
            "_hall_uid": lambda: 3, "_mant_notificar": self.notificar, "_mant_notif_cache_invalidar": lambda uid=None: None,
            "_mant_log": lambda *a: mundo.logs.append(a), "current_username": lambda: "daniel.aguilar@sphs.cl",
            "print": lambda *a, **k: None})
        return amb

    def llamar(self, guia=None, check=False):
        from flask import Flask, jsonify, request
        amb = self.ambiente()
        amb.update({"jsonify": jsonify, "request": request})
        app = Flask("prueba_logistica")
        q = "?guia=%s" % guia if guia else ("?check=1" if check else "")
        with app.test_request_context("/ot/api/5/documentos/logistica" + q):
            r = amb["ot_api_documentos_logistica"](5)
            if isinstance(r, tuple):
                return r[1], r[0].get_json()
            return r.status_code, r.get_json()


def _check_gdv(params):
    if params["tipodoc"] == "GDV" and params["numdoc"] in ("33193", "0000033193"):
        return {"estado": {"codigo": 0}, "body": {"response": [_fila_check("BB-01", "Booty Builder Cushioned Belt", 1, despachado=1)]}}
    return {"estado": {"codigo": 0}, "body": {"response": []}}


class TestRuta(unittest.TestCase):
    def test_tecnico_403_antes_de_leer_nada(self):
        m = Mundo(tecnico=True)
        st, j = m.llamar(guia="33193")
        self.assertEqual(st, 403)
        self.assertFalse(j["ok"])
        self.assertEqual((m.lecturas, m.random_sql, m.check_llamadas), (0, [], []))

    def test_decoradores_como_la_cadena(self):
        decs = FA.decoradores("ot_api_documentos_logistica")
        self.assertEqual(decs[0], '@app.route("/ot/api/<int:vid>/documentos/logistica", methods=["GET"])')
        self.assertEqual(decs[1:], ["@_mant_required", "@_no_tecnico", "@_ot_can_view"])
        src = FA.fuente_funcion("ot_api_documentos_logistica")
        self.assertLess(src.index("_es_rol_tecnico()"), src.index("_ot_logistica("))

    def test_resumen_compara_registra_y_avisa_una_vez(self):
        m = Mundo(check=_check_gdv)
        st, j = m.llamar()
        self.assertEqual(st, 200)
        comp = j["comparacion"]
        self.assertEqual((comp["estado"], comp["n_diferencias"]), ("diferencias", 2))
        self.assertIn("faltan por salir 2", comp["veredicto"])
        self.assertEqual(j["guias"][0]["numero"], "33193")
        self.assertEqual(j["guias"][0]["desde"], "FCV 11745")
        self.assertNotIn("bodega", j, "sin ?guia= no se le pregunta a Check")
        self.assertEqual(m.check_llamadas, [])
        # hallazgos persistentes + campana a la gestión (el responsable primero), sin quien lo mira (uid 3)
        self.assertEqual(sorted(a["detalle"] for a in m.alertas), ["X1", "Y2"])
        self.assertEqual([n["uid"] for n in m.notifs], [2])
        self.assertEqual(m.notifs[0]["tipo"], "ot_guia_diferencia")
        self.assertEqual(m.notifs[0]["url"], "/ot/5")
        self.assertTrue(all(a["notificada_el"] for a in m.alertas))
        self.assertEqual([l[2] for l in m.logs], ["guia_diferencia", "guia_diferencia"])
        # otra vez: nada nuevo, ni campana nueva
        m.llamar()
        self.assertEqual((len(m.alertas), len(m.notifs)), (2, 1))
        # leída la campana y vuelta a abrir: tampoco se repite (una sola vez por hallazgo)
        m.notifs[0]["leida"] = True
        m.llamar()
        self.assertEqual(len(m.notifs), 1)

    def test_el_hallazgo_se_resuelve_solo_y_nunca_se_borra(self):
        m = Mundo(check=_check_gdv)
        m.llamar()
        m.equipos = [{"vid": 5, "mid": 1, "sku": "BB-01", "nombre": "Booty Builder Cushioned Belt", "cant": 1}]
        st, j = m.llamar()
        self.assertEqual(j["comparacion"]["estado"], "coincide")
        self.assertEqual(sorted(a["estado"] for a in m.alertas), ["resuelta", "resuelta"])
        self.assertIn("guia_diferencia_resuelta", [l[2] for l in m.logs])
        self.assertFalse(any(re.match(r"\s*(DELETE|TRUNCATE)", s, re.I) for s in m.sql))

    def test_con_guia_trae_lo_que_salio_segun_check_y_solo_por_la_lista_blanca(self):
        m = Mundo(check=_check_gdv, traza=[{"doc": "GDV-0000033193", "ot": "4567", "tipoOT": "PICKING MATERIAL",
                                            "estado": "FINALIZADA", "usuario": "Luis Pérez", "ua": "UA1012965",
                                            "feInicioOT": "2026-10-08T10:20:00"}])
        st, j = m.llamar(guia="33193")
        self.assertEqual(st, 200)
        b = j["bodega"]["33193"]
        self.assertEqual((b["estado"], b["color"], b["fuente"]), ("ok", "verde", "GDV 33193"))
        self.assertTrue(b["fuente_es_guia"])
        self.assertEqual(b["guia_lineas"][0]["bodega"]["despachadas"], 1)
        self.assertEqual(b["guia_lineas"][0]["estado"], "coincide")
        self.assertEqual(b["movimientos"]["ots"][0]["personas"], ["Luis Pérez"])
        rutas = {c[0] for c in m.check_llamadas}
        self.assertTrue(rutas <= PERMITIDOS, rutas)
        self.assertEqual(rutas, {"/api/ext/GetSeguimientoDespacho"})
        self.assertTrue(all(c[2] <= 10 for c in m.check_llamadas), "timeout corto: Check no puede colgar la ficha")
        self.assertNotIn("59990", json.dumps(j), "el «valor» de Check jamás sale")
        for k in ("monto", "precio", "costo", "saldo", "zz_monto", "margen"):
            self.assertNotIn('"%s"' % k, json.dumps(j))

    def test_si_check_no_tiene_la_guia_pregunta_por_la_factura_de_origen(self):
        def solo_factura(params):
            if params["tipodoc"] == "FCV":
                return {"body": {"response": [_fila_check("BB-01", "b", 1, pickeado=1, tipo="FCV", num="0000011745")]}}
            return {"body": {"response": []}}
        m = Mundo(check=solo_factura)
        st, j = m.llamar(guia="33193")
        b = j["bodega"]["33193"]
        self.assertEqual(b["fuente"], "FCV 11745")
        self.assertFalse(b["fuente_es_guia"])
        self.assertEqual(b["consultados"], ["GDV 33193", "FCV 11745"])
        self.assertEqual(b["color"], "ambar")

    def test_si_check_no_responde_la_ficha_sigue(self):
        m = Mundo(check=None)
        st, j = m.llamar(guia="33193")
        self.assertEqual(st, 200)
        self.assertEqual(j["bodega"]["33193"]["estado"], "sin_conexion")
        self.assertEqual(len(m.check_llamadas), 1, "si Check no responde no se insiste con los demás documentos")
        self.assertEqual(j["comparacion"]["estado"], "diferencias", "la comparación (Random) sale igual")
        self.assertEqual(j["bodega"]["33193"]["movimientos"]["estado"], "cargando", "volcado frío: se pide atrás")
        self.assertEqual(m.swr, ["traza"])

    def test_sin_credenciales_de_check(self):
        m = Mundo(check=_check_gdv, configurado=False)
        st, j = m.llamar(guia="33193")
        self.assertEqual(j["bodega"]["33193"]["estado"], "sin_credenciales")
        self.assertEqual(m.check_llamadas, [])

    def test_si_random_no_responde_no_se_resuelve_a_ciegas(self):
        m = Mundo(check=_check_gdv)
        m.llamar()
        self.assertEqual(len(m.alertas), 2)
        m.random_ok = False                     # cada llamada arma su propio ámbito: la caché de guías empieza vacía
        st, j = m.llamar()
        self.assertEqual(st, 200)
        self.assertEqual(j["comparacion"]["estado"], "sin_lectura")
        self.assertEqual(sorted(a["estado"] for a in m.alertas), ["vigente", "vigente"])

    def test_resumen_rapido_deja_el_semaforo_esperando_a_check(self):
        m = Mundo(check=_check_gdv)
        st, j = m.llamar()
        self.assertTrue(j["check"])
        s = j["semaforos"]["33193"]
        self.assertTrue(s["pendiente"])
        self.assertEqual(s["color"], "ambar", "la diferencia con la OT ya se sabe sin esperar a Check")
        self.assertEqual(m.check_llamadas, [])

    def test_check_1_cierra_el_semaforo_de_cada_guia(self):
        m = Mundo(check=_check_gdv)
        st, j = m.llamar(check=True)
        s = j["semaforos"]["33193"]
        self.assertFalse(s["pendiente"])
        self.assertEqual((s["color"], s["texto"]), ("ambar", "Faltan por salir 2 de lo que gestiona la OT · bodega expidió 1 de 1"))
        self.assertEqual(j["bodega_resumen"]["33193"]["estado"], "ok")
        self.assertNotIn("bodega", j, "el detalle (movimientos, líneas) solo al hacer clic")
        self.assertEqual({c[0] for c in m.check_llamadas}, {"/api/ext/GetSeguimientoDespacho"})
        m2 = Mundo(check=_check_gdv, equipos=[{"vid": 5, "mid": 1, "sku": "BB-01", "nombre": "Booty Builder Cushioned Belt", "cant": 1}])
        s2 = m2.llamar(check=True)[1]["semaforos"]["33193"]
        self.assertEqual((s2["color"], s2["texto"]), ("verde", "Salió todo lo gestionado · bodega expidió 1 de 1"))

    def test_si_check_esta_caido_no_espera_guia_por_guia(self):
        m = Mundo(check=None, equipos=[{"vid": 5, "mid": 1, "sku": "BB-01", "nombre": "Booty", "cant": 1},
                                       {"vid": 5, "mid": 2, "sku": "X1", "nombre": "Trotadora", "cant": 1}])
        m.filas_random = Mundo.filas_random + [
            {"guia": "0000033194", "linea": "1", "sku": "X1", "producto": "Trotadora", "cantidad": 1.0, "tidopa": "FCV",
             "nudopa": "0000011745", "fecha": datetime.datetime(2026, 10, 9), "esdo": ""}]
        st, j = m.llamar(check=True)
        self.assertEqual(st, 200)
        self.assertEqual(len(m.check_llamadas), 1, "si Check no responde en la primera guía, no se le insiste")
        for n in ("33193", "33194"):
            self.assertEqual(j["semaforos"][n]["color"], "gris")
            self.assertIn("Check no respondió ahora", j["semaforos"][n]["texto"])

    def test_sin_credenciales_el_semaforo_es_gris_sin_esperar(self):
        m = Mundo(check=_check_gdv, configurado=False,
                  equipos=[{"vid": 5, "mid": 1, "sku": "BB-01", "nombre": "Booty", "cant": 1}])
        st, j = m.llamar()
        self.assertFalse(j["check"])
        s = j["semaforos"]["33193"]
        self.assertEqual((s["color"], s["pendiente"]), ("gris", False))
        self.assertIn("sin conexión con Check", s["texto"])

    def test_guia_anulada_en_random(self):
        m = Mundo(check=_check_gdv)
        m.filas_random = [dict(f, esdo="NULO") for f in Mundo.filas_random]
        st, j = m.llamar(check=True)
        self.assertEqual(j["semaforos"]["33193"]["color"], "gris")
        self.assertIn("anulada", j["semaforos"]["33193"]["texto"])
        self.assertEqual(j["comparacion"]["motivo"], "solo_anuladas")
        self.assertEqual(m.check_llamadas, [], "una guía anulada no se le pregunta a Check")

    def test_equipos_del_cliente_en_una_mantencion_no_alertan(self):
        m = Mundo(tipo_ot="preventiva", check=_check_gdv)
        st, j = m.llamar()
        self.assertEqual(j["comparacion"]["estado"], "sin_datos")
        self.assertEqual((m.alertas, m.notifs), ([], []))


class TestCableado(unittest.TestCase):
    def test_random_solo_select_y_parametrizado(self):
        sqls = re.findall(r'_random_sql_query\(\s*((?:"[^"]*"\s*)+)', BLOQUE)
        self.assertTrue(sqls)
        for sql in sqls:
            plano = "".join(re.findall(r'"([^"]*)"', sql)).strip().upper()
            self.assertTrue(plano.startswith("SELECT"), plano)
            for prohibido in ("INSERT", "UPDATE", "DELETE", "MERGE", "EXEC", " INTO ", "; "):
                self.assertNotIn(prohibido, plano)

    def test_check_solo_por_la_puerta_unica_y_solo_get_de_la_lista(self):
        self.assertNotIn("CHECKWMS_CONFIG", BLOQUE)
        self.assertNotRegex(BLOQUE, r"requests\.|_req\.|urllib")
        rutas = re.findall(r'_checkwms_get\(\s*"([^"]+)"', BLOQUE)
        self.assertEqual(set(rutas), {"/api/ext/GetSeguimientoDespacho"})
        self.assertTrue(set(rutas) <= PERMITIDOS)
        self.assertEqual(len(re.findall(r"_checkwms_get\(", BLOQUE)), len(rutas), "toda llamada a Check lleva su ruta escrita")

    def test_tabla_una_sentencia_barata_y_nunca_se_borra(self):
        src = FA.fuente_funcion("_ot_logi_tabla")
        self.assertEqual(src.count("mysql_execute("), 1)
        self.assertIn("CREATE TABLE IF NOT EXISTS ot_guia_alerta", src)
        self.assertIn("UNIQUE KEY uq_ga_ot_cod_det (visita_id, codigo, detalle)", src)
        self.assertNotIn("ALTER TABLE", src)
        self.assertIsNone(re.search(r"(DELETE\s+FROM|TRUNCATE\s+(TABLE\s+)?)ot_guia_alerta", APP, re.I))

    def test_enum_de_la_campana_tiene_el_tipo_nuevo_al_final_de_las_dos_listas(self):
        enums = re.findall(r"ALTER TABLE mant_notificaciones MODIFY COLUMN tipo \"\s*\"\s*ENUM\((.*?)\) DEFAULT 'otro'", APP, re.S)
        self.assertEqual(len(enums), 2)
        for e in enums:
            valores = re.findall(r"'([a-z_]+)'", e)
            self.assertEqual(valores[-1], G.TIPO_AVISO, "al FINAL: nada se reordena (REGLA #18)")
            self.assertEqual(valores[-2], "tk_sla")
            self.assertEqual(valores.count(G.TIPO_AVISO), 1)

    def test_sin_correo_y_nunca_al_cliente(self):
        for prohibido in ("_send_ilus_email", "_aviso_interno_correo", "_mant_send_email", "whatsapp", "contraparte"):
            self.assertNotIn(prohibido, BLOQUE)
        src = FA.fuente_funcion("_ot_logi_avisar")
        self.assertIn("_hall_destinos(vid, v, excluir_uid=_hall_uid())", src)
        self.assertIn("_gvo.TIPO_AVISO", src)

    def test_la_campana_tiene_icono_en_la_bandeja(self):
        self.assertIn("ot_guia_diferencia: { icon: 'truck'", _leer("templates/mantenciones/notificaciones.html"))

    def test_se_carga_junto_a_la_trazabilidad(self):
        parcial = _leer("templates/ot2/_fin_motor.html")
        i = parcial.index("ot_doc_cadena.js")
        self.assertLess(i, parcial.index("ot_guia_logistica.js"), "después de la cadena, que la monta")
        self.assertIn("ot_guia_logistica.css", parcial)
        cad = _leer("static/ot_doc_cadena.js")
        self.assertIn("OTGuiaLogistica.montar(sec, vid)", cad)
        self.assertIn('data-gl-ver="', cad)
        self.assertIn("data-gl-chip", cad)

    def test_no_toca_la_plata_ni_el_bloque_de_la_cadena(self):
        for prohibido in ("_ot_finanzas(", "zz_monto=", "UPDATE mant_visitas", "costo_proveedor"):
            self.assertNotIn(prohibido, BLOQUE)
        ini = APP.index("_OT_CADENA_CACHE = {}")
        fin = APP.index('@app.route("/ot/api/<int:vid>/saldo-servicio/tomar"')
        self.assertNotIn("ot_guia_alerta", APP[ini:fin], "la cadena sigue sin escribir nada")


# ═══════════════════════════ C) el JS en Node ═══════════════════════════
NODE = shutil.which("node")
JS = os.path.join(RAIZ, "static", "ot_guia_logistica.js")

_DOM_FALSO = r"""
function El(tag, attrs) {
  this.tag = tag; this.attrs = attrs || {}; this.children = []; this.parent = null; this.innerHTML = '';
  this.hidden = 'hidden' in this.attrs; var s = new Set(String(this.attrs['class'] || '').split(/\s+/).filter(Boolean));
  this.classList = { add: function(){ for (var a of arguments) s.add(a); }, remove: function(){ for (var a of arguments) s.delete(a); },
    contains: function(c){ return s.has(c); }, toString: function(){ return Array.from(s).join(' '); } };
  this._h = [];
}
El.prototype.append = function (c) { c.parent = this; this.children.push(c); return c; };
El.prototype.getAttribute = function (k) { return (k in this.attrs) ? String(this.attrs[k]) : null; };
El.prototype.setAttribute = function (k, v) { this.attrs[k] = String(v); };
El.prototype.removeAttribute = function (k) { delete this.attrs[k]; };
El.prototype.matches = function (sel) {
  var tm = sel.match(/^([a-z]+)/); if (tm && tm[1] !== this.tag) return false;
  var re = /\[([\w-]+)(?:="([^"]*)")?\]/g, m, alguno = false;
  while ((m = re.exec(sel))) { alguno = true; if (!(m[1] in this.attrs)) return false; if (m[2] !== undefined && String(this.attrs[m[1]]) !== m[2]) return false; }
  return alguno || !!tm;
};
El.prototype.querySelectorAll = function (sel) { var out = []; (function w(n){ n.children.forEach(function(c){ if (c.matches(sel)) out.push(c); w(c); }); })(this); return out; };
El.prototype.querySelector = function (sel) { return this.querySelectorAll(sel)[0] || null; };
El.prototype.closest = function (sel) { var n = this; while (n) { if (n.matches(sel)) return n; n = n.parent; } return null; };
El.prototype.contains = function (o) { while (o) { if (o === this) return true; o = o.parent; } return false; };
El.prototype.addEventListener = function (ev, fn) { this._h.push(fn); };
function clic(sec, target) { sec._h.forEach(function (h) { h({ target: target, preventDefault: function(){} }); }); }
function armar(nums) {
  var sec = new El('section', { 'class': 'dc' });
  var hd = sec.append(new El('div', { 'class': 'dc-h' }));
  hd.append(new El('span', { 'data-gl-chip': '' }));
  var ol = sec.append(new El('ol', { 'class': 'dc-cad' }));
  nums.forEach(function (n) {
    var li = ol.append(new El('li', { 'class': 'dc-n t-guia' }));
    var box = li.append(new El('div', { 'data-gl-guia': n }));
    box.append(new El('div', { 'data-gl-estado': '' }));
    box.append(new El('button', { 'data-gl-ver': n, 'aria-expanded': 'false' }));
    box.append(new El('div', { 'data-gl-cuerpo': '', 'hidden': '' }));
  });
  return sec;
}
const espera = () => new Promise(r => setTimeout(r, 5));
"""


@unittest.skipUnless(NODE, "Node no está instalado")
class TestJS(unittest.TestCase):
    RESUMEN = {"ok": True, "comparacion": {
        "estado": "diferencias", "n_diferencias": 2, "color": "ambar",
        "veredicto": "La guía GDV 33193 sacó 1 × Booty Builder Cushioned Belt; la OT gestiona 3 equipos: faltan por salir 2 (Bicicleta <S3> y Trotadora X).",
        "por_guia": {"33193": {"numero": "33193", "color": "ambar", "n_dif": 0, "texto": "",
                               "lineas": [{"sku": "BB-01", "nombre": "Booty Builder Cushioned Belt", "cantidad": 1, "estado": "coincide",
                                           "estado_txt": "Coincide", "es_diferencia": False, "texto": "Salió 1 y la OT gestiona 1."}]},
                     "33194": {"numero": "33194", "color": "verde", "n_dif": 0, "texto": "", "lineas": []}},
        "no_salio": [{"sku": "X1", "nombre": "Trotadora X", "gestiona": 1, "estado": "esta_en_la_OT_y_no_salio",
                      "estado_txt": "Está en la OT y no salió", "es_diferencia": True, "texto": "La OT gestiona 1 y no salió en ninguna guía."}],
        "anuladas": [], "sin_sku": [], "equipos_del_cliente": []}, "guias": [], "check": True,
        "semaforos": {"33193": {"color": "ambar", "texto": "Faltan por salir 1 de lo que gestiona la OT · consultando la bodega (Check)…",
                                "pendiente": True},
                      "33194": {"color": "gris", "texto": "Salió lo que gestiona la OT · consultando la bodega (Check)…", "pendiente": True}}}
    CON_CHECK = dict(RESUMEN, semaforos={
        "33193": {"color": "ambar", "texto": "Faltan por salir 1 de lo que gestiona la OT · bodega expidió 1 de 1", "pendiente": False},
        "33194": {"color": "verde", "texto": "Salió todo lo gestionado · bodega expidió 2 de 2", "pendiente": False}})

    def _correr(self, cuerpo_js, respuestas):
        script = _DOM_FALSO + r"""
        const fs = require('fs');
        const RESP = %s;
        const pedidos = [];
        global.window = {};
        global.fetch = async (u) => { pedidos.push(u); const r = RESP[u]; return { ok: !!r, json: async () => r }; };
        eval(fs.readFileSync(process.argv[1], 'utf8'));
        const L = window.OTGuiaLogistica;
        (async () => { %s })().catch(e => { console.error(e && e.stack || e); process.exit(1); });
        """ % (json.dumps(respuestas), cuerpo_js)
        out = subprocess.run([NODE, "-e", script, JS], capture_output=True, text=True, encoding="utf-8", timeout=60)
        self.assertEqual(out.returncode, 0, out.stderr)
        return json.loads(out.stdout.strip().splitlines()[-1])

    def test_chip_y_semaforo_verde_ambar_gris(self):
        r = self._correr(r"""
          const c = %s.comparacion;
          console.log(JSON.stringify({
            chip: L._htmlChip(c), verde: L._htmlChip({estado: 'coincide'}), nada: L._htmlChip({estado: 'sin_datos'}),
            sv: L._htmlSemaforo({color: 'verde', texto: 'Salió todo lo gestionado · bodega expidió 1 de 1'}),
            sa: L._htmlSemaforo({color: 'ambar', texto: 'Faltan <b>2</b>'}),
            sg: L._htmlSemaforo({color: 'gris', texto: 'Sin información de bodega (Check)', pendiente: true}),
            raro: L._htmlSemaforo({color: 'rojo', texto: 'x'}) }));
        """ % json.dumps(self.RESUMEN), {})
        self.assertIn("2 diferencias con la OT", r["chip"])
        self.assertIn('class="gl-chip ambar"', r["chip"])
        self.assertIn("data-gl-ir", r["chip"])
        self.assertIn("coincide con la OT", r["verde"])
        self.assertEqual(r["nada"], "")
        self.assertIn('class="gl-dot verde"', r["sv"])
        self.assertIn("Semáforo verde", r["sv"], "el color también se dice con palabras (lector de pantalla)")
        self.assertIn('class="gl-dot ambar"', r["sa"])
        self.assertIn("Faltan &lt;b&gt;2&lt;/b&gt;", r["sa"])
        self.assertIn('class="gl-sem pend"', r["sg"])
        self.assertIn('class="gl-dot gris"', r["raro"], "un color desconocido nunca pinta rojo")

    def test_montar_pinta_el_resumen_luego_check_y_el_clic_carga_el_detalle(self):
        detalle = json.loads(json.dumps(self.RESUMEN))
        detalle["bodega"] = {"33193": {
            "estado": "ok", "color": "verde", "texto": "Bodega expidió todo: 1 de 1 unidad.", "fuente": "GDV 33193",
            "fuente_es_guia": True, "emision": "08/10/2026", "entrega": "09/10/2026",
            "etapas": [{"clave": "pedido", "titulo": "x", "hechas": 1, "de": 1, "completa": True},
                       {"clave": "despachado", "titulo": "y", "hechas": 1, "de": 1, "completa": True}],
            "guia_lineas": [{"sku": "BB-01", "nombre": "Booty Builder Cushioned Belt", "cantidad": 1, "estado": "coincide",
                             "estado_txt": "Coincide", "es_diferencia": False, "texto": "Salió 1 y la OT gestiona 1.",
                             "bodega": {"pedidas": 1, "pickeadas": 1, "despachadas": 1}}],
            "movimientos": {"estado": "ok", "ots": [{"doc": "GDV 33193", "ot": "4567", "tipo": "PICKING MATERIAL",
                                                     "estado": "FINALIZADA", "inicio": "08/10/2026 10:20", "fin": "",
                                                     "personas": ["Luis Pérez"], "uas": ["UA1012965"]}], "edad_min": 4}}}
        detalle["semaforos"] = {"33193": dict(self.CON_CHECK["semaforos"]["33193"])}
        r = self._correr(r"""
          const sec = armar(['33193', '33194']);
          L.montar(sec, 5);
          await espera(); await espera(); await espera();
          const chip = sec.querySelector('[data-gl-chip]').innerHTML;
          const li = sec.querySelectorAll('li');
          const clases = [li[0].classList.toString(), li[1].classList.toString()];
          const sem = [sec.querySelectorAll('[data-gl-estado]')[0].innerHTML, sec.querySelectorAll('[data-gl-estado]')[1].innerHTML];
          const btn = sec.querySelector('[data-gl-ver="33193"]');
          clic(sec, btn);
          const cargando = sec.querySelector('[data-gl-cuerpo]').innerHTML;
          await espera(); await espera();
          const cuerpo = sec.querySelector('[data-gl-cuerpo]');
          const abierto = { html: cuerpo.innerHTML, hidden: cuerpo.hidden, exp: btn.getAttribute('aria-expanded'),
                            largo: sec.querySelector('ol').classList.contains('largo') };
          clic(sec, btn);
          const cerrado = { hidden: cuerpo.hidden, exp: btn.getAttribute('aria-expanded'), largo: sec.querySelector('ol').classList.contains('largo') };
          const semDespues = [sec.querySelectorAll('[data-gl-estado]')[0].innerHTML, sec.querySelectorAll('[data-gl-estado]')[1].innerHTML];
          console.log(JSON.stringify({ chip, clases, sem, cargando, abierto, cerrado, semDespues, pedidos }));
        """, {"/ot/api/5/documentos/logistica": self.RESUMEN, "/ot/api/5/documentos/logistica?check=1": self.CON_CHECK,
              "/ot/api/5/documentos/logistica?guia=33193": detalle})
        self.assertIn("2 diferencias con la OT", r["chip"])
        self.assertEqual(r["clases"], ["dc-n t-guia", "dc-n t-guia"], "colores con medida: la tarjeta no se pinta entera")
        # el semáforo de cada guía ya trae lo que dijo Check (2.ª lectura, ?check=1)
        self.assertIn('class="gl-dot ambar"', r["sem"][0])
        self.assertIn("bodega expidió 1 de 1", r["sem"][0])
        self.assertIn('class="gl-dot verde"', r["sem"][1])
        self.assertIn("Salió todo lo gestionado · bodega expidió 2 de 2", r["sem"][1])
        self.assertIn("Consultando la bodega", r["cargando"])
        h = r["abierto"]["html"]
        self.assertFalse(r["abierto"]["hidden"])
        self.assertEqual(r["abierto"]["exp"], "true")
        self.assertTrue(r["abierto"]["largo"], "la guía abierta usa el ancho completo")
        self.assertIn("Bodega expidió todo: 1 de 1 unidad.", h)
        self.assertIn("Según Check: esta guía", h)
        self.assertIn("Booty Builder Cushioned Belt", h)
        self.assertIn("bodega expidió 1 de 1", h)
        self.assertIn("La OT gestiona y no salió en ninguna guía", h)
        self.assertIn("Luis Pérez", h)
        self.assertIn("UA1012965", h)
        self.assertIn("&lt;S3&gt;", h, "todo se escapa")
        self.assertNotIn("<S3>", h)
        self.assertEqual(r["cerrado"], {"hidden": True, "exp": "false", "largo": False})
        self.assertEqual(r["semDespues"], r["sem"], "el detalle no borra lo que Check ya dijo de las otras guías")
        self.assertEqual(r["pedidos"], ["/ot/api/5/documentos/logistica", "/ot/api/5/documentos/logistica?check=1",
                                        "/ot/api/5/documentos/logistica?guia=33193"])

    def test_sin_check_configurado_no_hay_segunda_lectura(self):
        sin = dict(self.RESUMEN, check=False, semaforos={"33193": {"color": "gris", "texto": "x · sin conexión con Check", "pendiente": False}})
        r = self._correr(r"""
          const sec = armar(['33193']);
          L.montar(sec, 7);
          await espera(); await espera();
          console.log(JSON.stringify({ sem: sec.querySelector('[data-gl-estado]').innerHTML, pedidos }));
        """, {"/ot/api/7/documentos/logistica": sin})
        self.assertIn("sin conexión con Check", r["sem"])
        self.assertEqual(r["pedidos"], ["/ot/api/7/documentos/logistica"])

    def test_si_la_lectura_falla_ofrece_reintentar(self):
        r = self._correr(r"""
          const sec = armar(['33193']);
          L.montar(sec, 6);
          await espera();
          clic(sec, sec.querySelector('[data-gl-ver="33193"]'));
          await espera(); await espera();
          console.log(JSON.stringify({ html: sec.querySelector('[data-gl-cuerpo]').innerHTML,
                                       chip: sec.querySelector('[data-gl-chip]').innerHTML,
                                       sem: sec.querySelector('[data-gl-estado]').innerHTML }));
        """, {})
        self.assertIn("No pudimos leer la logística ahora", r["html"])
        self.assertIn('data-gl-reintentar="33193"', r["html"])
        self.assertEqual((r["chip"], r["sem"]), ("", ""))

    def test_css_con_medida_sin_tarjetas_pintadas(self):
        css = _leer("static/ot_guia_logistica.css")
        self.assertIsNone(re.search(r"\.dc-n[^{]*\{[^}]*background", css), "la tarjeta de la guía no se pinta entera")
        self.assertIsNone(re.search(r"border-left\s*:\s*[3-9]px", css), "sin bordes laterales gruesos de color")
        self.assertIn(".gl-dot{", css)
        self.assertRegex(css, r"\.gl-dot\{[^}]*width:10px")

    def test_sin_nativos_y_sin_montos(self):
        js = _leer("static/ot_guia_logistica.js")
        sin_coment = re.sub(r"/\*.*?\*/", "", js, flags=re.S)
        for nativo in ("alert(", "confirm(", "prompt("):
            self.assertIsNone(re.search(r"(?<![\w.])" + re.escape(nativo), sin_coment), nativo)
        self.assertIn("function esc(", js)
        self.assertNotIn("clp(", js)
        self.assertNotIn("'$'", js)


if __name__ == "__main__":
    unittest.main()

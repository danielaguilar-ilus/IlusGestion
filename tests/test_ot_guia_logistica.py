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
        self.assertIn("toca la guía de despacho", c)      # 2026-10-29: la guía es un chip; ya no hay botón «Ver logística»
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
        self.assertEqual(s["texto"], "La guía cuadra con la OT · sin información de bodega (Check)",
                         "la mitad de Random habla del papel; lo que salió lo dice Check")
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

    def test_modulo_puro_y_feriados_de_una_sola_fuente(self):
        self.assertIn("import cl_feriados as _clf", _leer("ot_guia_vs_ot.py"), "REGLA #31: feriados solo de cl_feriados")
        self.test_modulo_puro()

    def test_modulo_puro(self):
        arbol = ast.parse(_leer("ot_guia_vs_ot.py"))
        mods = set()
        for n in ast.walk(arbol):
            if isinstance(n, ast.Import):
                mods |= {a.name for a in n.names}
            elif isinstance(n, ast.ImportFrom):
                mods.add(n.module)
        self.assertFalse({"flask", "pymysql", "pymssql", "requests", "app", "urllib"} & mods, mods)


# ═══════════════ A2) Check tiene la última palabra: reglas B y C, plazo de gracia (29-oct, tarde) ═══════════════
HOY_VENCIDO = datetime.date(2026, 10, 29)      # la guía del 08/10/2026 venció su plazo el 14/10/2026
HOY_EN_PLAZO = datetime.date(2026, 10, 13)     # martes 13: segundo día hábil (el lunes 12 es feriado)
BANCO = "Banco ajustable multiposición FID 45° a 90° con rodillos para piernas (negro mate)"


def _guia10(q_guia=10):
    return _guia("33193", [("BAN-1", BANCO, q_guia), ("ZZENVIO", "Despacho", 1)])


def _bod_check(*filas, fuente="GDV 33193"):
    return G.bodega(G.lineas_check(list(filas)), fuente, [fuente])


class TestTresBandas(unittest.TestCase):
    """«Hice una OT por 10 productos, pero en realidad salieron 8: tiene que haber una alerta.» Algo POCO: tres reglas."""

    def _tb(self, comp, bod, hoy=HOY_VENCIDO, num="33193"):
        return G.tres_bandas(comp, {num: bod}, fechas={num: "08/10/2026"}, hoy=hoy)

    def test_plazo_de_gracia_en_dias_habiles_con_feriados(self):
        self.assertEqual(G.vence_el("08/10/2026"), datetime.date(2026, 10, 14), "jue 08 → vie 09, (lun 12 feriado), mar 13, mié 14")
        self.assertTrue(G.en_plazo("08/10/2026", HOY_EN_PLAZO))
        self.assertFalse(G.en_plazo("08/10/2026", datetime.date(2026, 10, 14)))
        self.assertTrue(G.en_plazo("", HOY_VENCIDO), "sin fecha no se acusa a ciegas")
        self.assertTrue(G.en_plazo("basura", HOY_VENCIDO))
        self.assertEqual(G.GRACIA_DIAS_HABILES, 3)

    def test_B_la_ot_gestiona_10_y_check_informa_8(self):
        comp = _comparar([_guia10()], [_eq("BAN-1", BANCO, 10)])
        self.assertEqual(comp["estado"], "coincide", "la guía (Random) dice 10: el papel cuadra")
        tb = self._tb(comp, _bod_check(_fila_check("BAN-1", BANCO, 10, despachado=8)))
        self.assertEqual([h["codigo"] for h in tb["hallazgos"]], ["check_expidio_menos"])
        h = tb["hallazgos"][0]
        self.assertEqual(h["detalle"], "BAN-1")
        self.assertEqual(h["texto"], BANCO + " (BAN-1): la OT gestiona 10; Check informa que salieron 8.")
        self.assertEqual(tb["productos"]["BAN-1"]["estado"], "menos")
        self.assertIn(("check_expidio_menos", "BAN-1"), tb["evaluados"])
        s = G.semaforo_guia(comp, "33193", _bod_check(_fila_check("BAN-1", BANCO, 10, despachado=8)), tb=tb)
        self.assertEqual(s["color"], "ambar", "Check manda: aunque la guía cuadre, el semáforo no es verde")
        self.assertIn("bodega expidió 8 de 10", s["texto"])

    def test_B_mientras_bodega_prepara_dentro_del_plazo_no_alerta(self):
        comp = _comparar([_guia10()], [_eq("BAN-1", BANCO, 10)])
        tb = self._tb(comp, _bod_check(_fila_check("BAN-1", BANCO, 10, despachado=8)), hoy=HOY_EN_PLAZO)
        self.assertEqual(tb["hallazgos"], [])
        self.assertEqual(tb["productos"]["BAN-1"]["estado"], "preparando")
        self.assertEqual(tb["por_guia"]["33193"]["color"], "ambar", "se ve en ámbar (falta expedir), pero no suena")

    def test_B_si_check_ya_cerro_su_pedido_en_8_alerta_sin_esperar_el_plazo(self):
        comp = _comparar([_guia10()], [_eq("BAN-1", BANCO, 10)])
        tb = self._tb(comp, _bod_check(_fila_check("BAN-1", BANCO, 8, despachado=8)), hoy=HOY_EN_PLAZO)
        self.assertEqual([h["codigo"] for h in tb["hallazgos"]], ["check_expidio_menos"], "Check no va a expedir más: ya es un descuadre")

    def test_B_con_la_guia_tambien_corta_se_dicen_las_tres_cifras_y_A_sigue(self):
        comp = _comparar([_guia10(9)], [_eq("BAN-1", BANCO, 10)])
        tb = self._tb(comp, _bod_check(_fila_check("BAN-1", BANCO, 9, despachado=8)))
        hs = G.hallazgos(comp, tb)
        self.assertEqual(sorted(h["codigo"] for h in hs), ["cantidad_distinta", "check_expidio_menos"])
        b = next(h for h in hs if h["codigo"] == "check_expidio_menos")
        self.assertIn("la OT gestiona 10 y la guía dice 9; Check informa que salieron 8.", b["texto"])

    def test_si_check_expidio_lo_de_la_guia_no_hay_B_aunque_la_guia_sea_corta(self):
        comp = _comparar([_guia10(8)], [_eq("BAN-1", BANCO, 10)])
        tb = self._tb(comp, _bod_check(_fila_check("BAN-1", BANCO, 8, despachado=8)))
        self.assertEqual(tb["hallazgos"], [], "lo que falta en la guía ya lo dice la regla A (sin duplicar el aviso)")
        self.assertEqual([h["codigo"] for h in G.hallazgos(comp, tb)], ["cantidad_distinta"])

    def test_C_hay_guia_y_check_no_registra_la_salida(self):
        comp = _comparar([GUIA_33193], [_eq("BB-01", "Booty Builder Cushioned Belt")])
        bod = _bod_check(_fila_check("BB-01", "b", 1))
        en_plazo = self._tb(comp, bod, hoy=HOY_EN_PLAZO)
        self.assertEqual(en_plazo["hallazgos"], [], "sin alerta mientras se prepara")
        self.assertEqual(en_plazo["por_guia"]["33193"]["estado"], "preparando")
        self.assertIn("todavía en plazo (hasta el 14/10/2026)", en_plazo["por_guia"]["33193"]["texto"])
        tb = self._tb(comp, bod)
        self.assertEqual([(h["codigo"], h["detalle"]) for h in tb["hallazgos"]], [("guia_sin_salida_check", "33193")])
        self.assertEqual(tb["hallazgos"][0]["texto"],
                         "Hay guía GDV 33193 del 08/10/2026 y Check no registra que haya salido (1 unidad; el plazo de 3 días "
                         "hábiles venció el 14/10/2026).")
        self.assertEqual(tb["productos"]["BB-01"]["estado"], "sin_salida", "no se acusa dos veces (B se calla si ya está C)")
        s = G.semaforo_guia(comp, "33193", bod, tb=tb)
        self.assertEqual(s["color"], "ambar")
        self.assertIn("Check no registra la salida (plazo vencido el 14/10/2026)", s["texto"])

    def test_sin_datos_de_check_nunca_alerta_ni_resuelve(self):
        comp = _comparar([_guia10()], [_eq("BAN-1", BANCO, 10)])
        for bod in (G.bodega([], "", ["GDV 33193", "FCV 11745"]), G.bodega(None, respondio=False), G.bodega(None, configurado=False),
                    _bod_check(_fila_check("OTRO", "Otro producto", 3, despachado=0))):
            tb = self._tb(comp, bod)
            self.assertEqual(tb["hallazgos"], [], bod.get("texto"))
            self.assertNotIn(("check_expidio_menos", "BAN-1"), tb["evaluados"])
            self.assertNotIn(("guia_sin_salida_check", "33193"), tb["evaluados"])
            self.assertEqual(G.semaforo_guia(comp, "33193", bod, tb=tb)["color"], "gris", "gris = no se puede saber")
        # sin guía, guía anulada o sin productos gestionados: «sin datos», nada que pedirle a Check
        for c in (_comparar([], EQUIPOS_3), _comparar([_guia("1", [("X1", "T", 1)], anulada=True)], EQUIPOS_3),
                  _comparar([GUIA_33193], [])):
            self.assertEqual(G.tres_bandas(c, {"33193": _bod_check(_fila_check("BB-01", "b", 1))}, hoy=HOY_VENCIDO)["hallazgos"], [])

    def test_falta_de_stock_declarada_no_se_le_pide_a_check(self):
        comp = _comparar([_guia10(8)], [_eq("BAN-1", BANCO, 8), _eq("BAN-1", BANCO, 2, declarado="falta_stock")])
        tb = self._tb(comp, _bod_check(_fila_check("BAN-1", BANCO, 8, despachado=8)))
        self.assertEqual(G.hallazgos(comp, tb), [], "la OT ya dijo que 2 no llegaron: Check expidió los 8 que tocaban")

    def test_por_total_cuando_check_no_informa_por_producto(self):
        comp = _comparar([_guia10()], [_eq("BAN-1", BANCO, 10)])
        fila = _fila_check("", "", 10, despachado=8)
        fila.pop("codigoMaterial")
        fila.pop("material")
        tb = self._tb(comp, _bod_check(fila))
        self.assertEqual(tb["modo"], "por_total")
        self.assertEqual([(h["codigo"], h["detalle"]) for h in tb["hallazgos"]], [("check_expidio_menos", "TOTAL")])
        self.assertEqual(tb["hallazgos"][0]["texto"], "La OT gestiona 10 productos; Check informa que salieron 8 (Check no informa "
                                                      "por producto: se comparan los totales).")

    def test_si_check_informa_por_la_factura_no_se_cuenta_dos_veces(self):
        g1 = _guia("50001", [("BAN-1", BANCO, 5)])
        g2 = _guia("50002", [("BAN-1", BANCO, 5)])
        comp = _comparar([g1, g2], [_eq("BAN-1", BANCO, 10)])
        fcv = _bod_check(_fila_check("BAN-1", BANCO, 10, despachado=10, tipo="FCV", num="11745"), fuente="FCV 11745")
        tb = G.tres_bandas(comp, {"50001": fcv, "50002": fcv}, fechas={"50001": "08/10/2026", "50002": "08/10/2026"},
                           hoy=HOY_VENCIDO)
        self.assertEqual(tb["productos"]["BAN-1"]["check"], 10, "las dos guías salieron de la misma factura: 10, no 20")
        self.assertEqual(tb["hallazgos"], [])
        corta = _bod_check(_fila_check("BAN-1", BANCO, 10, despachado=6, tipo="FCV", num="11745"), fuente="FCV 11745")
        tb2 = G.tres_bandas(comp, {"50001": corta, "50002": corta}, fechas={"50001": "08/10/2026", "50002": "08/10/2026"},
                            hoy=HOY_VENCIDO)
        self.assertIn("Check informa que salieron 6", tb2["hallazgos"][0]["texto"])

    def test_B_necesita_check_de_todas_las_guias_del_producto(self):
        g1 = _guia("50001", [("BAN-1", BANCO, 5)])
        g2 = _guia("50002", [("BAN-1", BANCO, 5)])
        comp = _comparar([g1, g2], [_eq("BAN-1", BANCO, 10)])
        solo_una = {"50001": _bod_check(_fila_check("BAN-1", BANCO, 5, despachado=5, num="50001"), fuente="GDV 50001")}
        tb = G.tres_bandas(comp, solo_una, fechas={"50001": "08/10/2026", "50002": "08/10/2026"}, hoy=HOY_VENCIDO)
        self.assertEqual(tb["hallazgos"], [], "a la otra guía no se le preguntó: contaría de menos")
        self.assertNotIn(("check_expidio_menos", "*"), tb["evaluados"])

    def test_alcance_y_plan_nunca_resuelven_lo_de_check_a_ciegas(self):
        previas = [{"id": 7, "codigo": "check_expidio_menos", "detalle": "BAN-1", "estado": "vigente", "texto": "t",
                    "notificada_el": datetime.datetime(2026, 10, 29)},
                   {"id": 8, "codigo": "guia_sin_salida_check", "detalle": "33193", "estado": "vigente", "texto": "t",
                    "notificada_el": datetime.datetime(2026, 10, 29)}]
        comp = _comparar([_guia10()], [_eq("BAN-1", BANCO, 10)])
        ev, todo = G.alcance(comp, None)                       # lectura rápida, sin Check
        self.assertEqual((ev, todo), ([], False))
        self.assertEqual(G.plan_hallazgos({}, previas, ev, todo)["resolver"], [])
        tb = self._tb(comp, _bod_check(_fila_check("BAN-1", BANCO, 10, despachado=10)))
        ev, todo = G.alcance(comp, tb)
        self.assertEqual(sorted(r["id"] for r in G.plan_hallazgos({}, previas, ev, todo)["resolver"]), [7, 8],
                         "Check ya expidió todo: B y C se resuelven solos")
        ev, todo = G.alcance(_comparar([], EQUIPOS_3), None)
        self.assertTrue(todo, "sin guía no puede quedar nada vigente")
        self.assertEqual(len(G.plan_hallazgos({}, previas, ev, todo)["resolver"]), 2)
        self.assertEqual(len(G.plan_hallazgos({}, previas, [("check_expidio_menos", "*")])["resolver"]), 1)

    def test_detalle_de_la_guia_diferencias_arriba_y_las_tres_bandas(self):
        comp = _comparar([_guia10()], [_eq("BAN-1", BANCO, 10), _eq("X1", "Trotadora Matrix T70")])
        bod = _bod_check(_fila_check("BAN-1", BANCO, 10, despachado=8))
        tb = self._tb(comp, bod)
        d = G.detalle_guia(comp, "33193", tb=tb, bod=bod)
        self.assertEqual([x["regla"] for x in d["diferencias"]], ["B", "A"], "lo de Check primero")
        self.assertIn("Check informa que salieron 8", d["diferencias"][0]["texto"])
        self.assertIn("Trotadora Matrix T70 (X1): la OT gestiona 1 y no salió en ninguna guía.", d["diferencias"][1]["texto"])
        fila = d["productos"][0]
        self.assertEqual((fila["nombre"], fila["ot"], fila["guia"], fila["check"]), (BANCO, 10, 10, 8))
        self.assertEqual(fila["check_txt"], "Según Check salieron 8 de 10")
        self.assertEqual(fila["color"], "ambar")
        fuera = d["productos"][1]
        self.assertEqual((fuera["nombre"], fuera["guia"], fuera["en_guia"]), ("Trotadora Matrix T70", 0, False))
        self.assertEqual(fuera["check_txt"], "No salió en ninguna guía")
        c_ok = _comparar([GUIA_33193], [_eq("BB-01", "Booty")])
        ok = G.detalle_guia(c_ok, "33193", bod=_bod_check(_fila_check("BB-01", "b", 1, despachado=1)))
        self.assertEqual(ok["diferencias"], [])
        self.assertEqual(ok["productos"][0]["check_txt"], "Salió de bodega (1 de 1, según Check)")
        self.assertNotIn("59990", json.dumps(d), "el «valor» de Check jamás sale")

    def test_correo_corto_con_numeros_y_sin_plata(self):
        comp = _comparar([_guia10()], [_eq("BAN-1", BANCO, 10)])
        tb = self._tb(comp, _bod_check(_fila_check("BAN-1", BANCO, 10, despachado=8)))
        t, c = G.texto_correo("OT-2026-00171", "Aurum Fit Girls Spa", G.hallazgos(comp, tb))
        self.assertEqual(t, "OT-2026-00171: 1 diferencia entre la OT y lo que salió de bodega")
        self.assertIn("Aurum Fit Girls Spa", c)
        self.assertIn("la OT gestiona 10; Check informa que salieron 8.", c)
        self.assertIn("«Trazabilidad en Random»", c)
        for feo in ("$", "monto", "costo", "precio", "proveedor", "margen", "culpa", "error"):
            self.assertNotIn(feo, c.lower())
        self.assertEqual(G.huella("a  b"), G.huella("a b"))
        self.assertNotEqual(G.huella("salieron 8"), G.huella("salieron 9"), "si la diferencia cambia, sale otro correo")
        tc, cc = G.texto_campana("OT-2026-00171", "", G.hallazgos(comp, tb))
        self.assertIn("(guía y Check)", tc)
        self.assertIn("lo que informa Check", cc)


# ═══════════════════════════ B) el cableado en app.py ═══════════════════════════
APP = FA.texto("app.py")
_INI = APP.index("#  🚚 2026-10-29 — LOGÍSTICA (WMS)")
BLOQUE = APP[_INI:APP.index("#  📒 2026-10-11 — LIBRO DE SALDO DE FACTURAS")]
FUNCS = ["_ot_logi_tabla", "_ot_logi_guias_lineas", "_ot_logi_gestionados", "_ot_logi_otras_ot", "_ot_logi_check_doc",
         "_ot_logi_movimientos", "_ot_logi_bodega", "_ot_logi_avisar", "_ot_logi_correo", "_ot_logi_registrar", "_ot_logistica",
         "ot_api_documentos_logistica", "_ot_saldo_real"]
CONSTS = ["_OT_LOGI_GUIAS_CACHE", "_OT_LOGI_GUIAS_TTL", "_OT_LOGI_CHECK_CACHE", "_OT_LOGI_CHECK_TTL", "_OT_LOGI_CHECK_TTL_ERR",
          "_OT_LOGI_CHECK_TIMEOUT", "_OT_LOGI_CHECK_PRESUPUESTO", "_OT_LOGI_CHECK_MAX_DOCS", "_OT_LOGI_MAX_OTRAS",
          "_OT_LOGI_MAX_GUIAS_CHECK", "_OT_LOGI_TABLA", "_OT_LOGI_CORREO_REINTENTO", "_OT_LOGI_CORREO_ESPERA"]
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
                 configurado=True, hoy=datetime.datetime(2026, 10, 29, 12, 0)):
        self.tipo_ot = tipo_ot
        self.equipos = equipos if equipos is not None else [
            {"vid": 5, "mid": 1, "sku": "BB-01", "nombre": "Booty Builder Cushioned Belt", "cant": 1},
            {"vid": 5, "mid": 2, "sku": "X1", "nombre": "Trotadora Matrix T70", "cant": 1},
            {"vid": 5, "mid": 3, "sku": "Y2", "nombre": "Bicicleta Spinning S3", "cant": 1}]
        self.random_ok, self.check, self.traza, self.tecnico, self.configurado = random_ok, check, traza, tecnico, configurado
        self.alertas, self.notifs, self.logs, self.sql, self.random_sql, self.check_llamadas = [], [], [], [], [], []
        self.lecturas = 0
        self.swr = []
        self.hoy = hoy
        self.correos = []                 # lo que se le entregó a _aviso_interno_correo (el correo interno)
        self.correo_modo = "ok"           # ok · apagado (la puerta lo frena: devuelve 0) · error (levanta)

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
            filas = [dict(a) for a in self.alertas if a["visita_id"] == p[0]]
            if "estado='vigente'" in sql:
                filas = [a for a in filas if a["estado"] == "vigente"]
            return filas
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
                                 "guias": p[4], "estado": "vigente", "veces": 1, "notificada_el": None,
                                 "detectada_el": p[5], "actualizado_el": p[7], "correo_huella": None, "correo_el": None})
        elif s.startswith("UPDATE ot_guia_alerta SET estado='vigente'"):
            a = next(a for a in self.alertas if a["id"] == p[3])
            a.update(estado="vigente", veces=a["veces"] + 1, texto=p[0], actualizado_el=p[2])
        elif s.startswith("UPDATE ot_guia_alerta SET estado='resuelta'"):
            next(a for a in self.alertas if a["id"] == p[2])["estado"] = "resuelta"
        elif s.startswith("UPDATE ot_guia_alerta SET texto="):
            next(a for a in self.alertas if a["id"] == p[3]).update(texto=p[0], actualizado_el=p[2])
        elif s.startswith("UPDATE ot_guia_alerta SET correo_huella=NULL"):
            next(a for a in self.alertas if a["id"] == p[0])["correo_huella"] = None
        elif s.startswith("UPDATE ot_guia_alerta SET notificada_el="):
            for a in self.alertas:
                if a["visita_id"] == p[1] and a["estado"] == "vigente" and a["notificada_el"] is None:
                    a["notificada_el"] = p[0]
        elif s.startswith("UPDATE mant_notificaciones"):
            next(n for n in self.notifs if n["id"] == p[3]).update(titulo=p[0], cuerpo=p[1])

    def execute_n(self, sql, p=None):
        """mysql_execute_returning_rowcount: el «reclamo» atómico del correo (UPDATE … WHERE huella distinta)."""
        self.sql.append(sql)
        s = " ".join(sql.split())
        if s.startswith("UPDATE ot_guia_alerta SET correo_huella=%s, correo_el=%s WHERE id=%s"):
            a = next(a for a in self.alertas if a["id"] == p[2])
            if a["correo_huella"] is None or a["correo_huella"] != p[3]:
                a.update(correo_huella=p[0], correo_el=p[1])
                return 1
            return 0
        raise AssertionError("SQL inesperado: " + s)

    def aviso_correo(self, clave, uids, titulo, cuerpo, url, etiqueta="Aviso interno", ref=None):
        if self.correo_modo == "error":
            raise RuntimeError("SMTP de mentira caído")
        self.correos.append({"clave": clave, "uids": list(uids), "titulo": titulo, "cuerpo": cuerpo, "url": url,
                             "etiqueta": etiqueta, "ref": ref})
        return 0 if self.correo_modo == "apagado" else len(uids)

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
            "mysql_execute_returning_rowcount": (_no if self.tecnico else self.execute_n),
            "_aviso_interno_correo": (_no if self.tecnico else self.aviso_correo),
            "_now_chile": lambda: mundo.hoy, "timedelta": datetime.timedelta,
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
        self.assertEqual((m.alertas, m.notifs, m.correos), ([], [], []))


def _mundo10(desp=8, ped=10, **kw):
    """La OT gestiona 10 bancos; la guía de Random dice 10; Check (bodega) informa `desp` expedidos de `ped` pedidos."""
    m = Mundo(equipos=[{"vid": 5, "mid": 1, "sku": "BAN-1", "nombre": BANCO, "cant": 10}], **kw)
    m.filas_random = [{"guia": "0000033193", "linea": "1", "sku": "BAN-1", "producto": BANCO, "cantidad": 10.0, "tidopa": "FCV",
                       "nudopa": "0000011745", "fecha": datetime.datetime(2026, 10, 8), "esdo": ""}]
    m.desp, m.ped = desp, ped
    m.check = lambda params: ({"body": {"response": [_fila_check("BAN-1", BANCO, m.ped, despachado=m.desp)]}}
                              if params["tipodoc"] == "GDV" else {"body": {"response": []}})
    return m


class TestCheckUltimaPalabra(unittest.TestCase):
    """Daniel (29-oct, tarde): «es Check quien nos tiene que dar la ÚLTIMA PALABRA… si hay diferencia, dame una ALERTA, así
    sea POR CORREO». Todo con Random, Check, base y correo de MENTIRA."""

    def test_10_vs_8_registra_B_suena_la_campana_y_sale_UN_correo_a_la_gestion(self):
        m = _mundo10()
        st, j = m.llamar(check=True)
        self.assertEqual(st, 200)
        self.assertEqual(j["comparacion"]["estado"], "coincide", "el papel (la guía) cuadra con la OT")
        self.assertEqual([h["codigo"] for h in j["check_ot"]["hallazgos"]], ["check_expidio_menos"])
        self.assertEqual(j["semaforos"]["33193"]["color"], "ambar", "Check manda: no es verde")
        self.assertEqual([(a["codigo"], a["detalle"], a["estado"]) for a in m.alertas], [("check_expidio_menos", "BAN-1", "vigente")])
        self.assertEqual([(n["uid"], n["tipo"]) for n in m.notifs], [(2, "ot_guia_diferencia")])
        self.assertEqual(len(m.correos), 1)
        c = m.correos[0]
        self.assertEqual(c["clave"], "ot_guia_diferencia")
        self.assertEqual(c["uids"], [2], "a la gestión (el responsable primero), no a quien lo está mirando")
        self.assertEqual(c["url"], "/ot/5")
        self.assertEqual(c["ref"], {"tipo": "OT", "id": 5, "codigo": "OT-2026-00171"})
        self.assertIn("la OT gestiona 10; Check informa que salieron 8.", c["cuerpo"])
        self.assertIn("Aurum Fit Girls Spa", c["cuerpo"])
        for feo in ("$", "monto", "costo", "zz_monto", "120000", "252101"):
            self.assertNotIn(feo, c["cuerpo"] + c["titulo"])
        # otra lectura igual: ni correo ni campana nuevos (UNO por hallazgo)
        m.llamar(check=True)
        m.llamar(guia="33193")
        self.assertEqual((len(m.correos), len(m.notifs), len(m.alertas)), (1, 1, 1))
        # la diferencia cambia (Check ahora informa 9): sale UN correo nuevo con lo de ahora
        m.desp = 9
        m.llamar(check=True)
        self.assertEqual(len(m.correos), 2)
        self.assertIn("Check informa que salieron 9", m.correos[1]["cuerpo"])
        self.assertEqual(len(m.alertas), 1, "el mismo hallazgo, con el texto al día")
        # bodega expide todo: se resuelve solo, sin correo
        m.desp = 10
        st, j = m.llamar(check=True)
        self.assertEqual([a["estado"] for a in m.alertas], ["resuelta"])
        self.assertEqual(len(m.correos), 2)
        self.assertEqual((j["semaforos"]["33193"]["color"], j["semaforos"]["33193"]["texto"]),
                         ("verde", "Salió todo lo gestionado · bodega expidió 10 de 10"))

    def test_la_lectura_rapida_guarda_el_correo_y_la_de_check_manda_uno_solo_con_todo(self):
        m = Mundo(check=_check_gdv)
        m.llamar()
        self.assertEqual((len(m.alertas), len(m.notifs), m.correos), (2, 1, []), "la campana suena; el correo espera a Check")
        m.llamar(check=True)
        self.assertEqual(len(m.correos), 1)
        self.assertIn("Trotadora Matrix T70 (X1)", m.correos[0]["cuerpo"])
        self.assertIn("Bicicleta Spinning S3 (Y2)", m.correos[0]["cuerpo"])
        m.llamar()
        m.llamar(check=True)
        self.assertEqual(len(m.correos), 1)

    def test_si_la_lectura_de_check_no_llega_el_correo_sale_igual_a_los_10_minutos(self):
        m = Mundo(check=_check_gdv)
        m.llamar()
        self.assertEqual(m.correos, [])
        for a in m.alertas:
            a["detectada_el"] = a["actualizado_el"] = datetime.datetime.utcnow() - datetime.timedelta(minutes=11)
        m.llamar()
        self.assertEqual(len(m.correos), 1)

    def test_sin_check_configurado_el_correo_sale_en_la_primera_lectura(self):
        m = Mundo(check=_check_gdv, configurado=False)
        m.llamar()
        self.assertEqual(len(m.correos), 1)
        self.assertEqual(m.check_llamadas, [])

    def test_una_lectura_sin_check_no_resuelve_lo_que_dijo_check(self):
        m = _mundo10()
        m.llamar(check=True)
        m.llamar()
        self.assertEqual([a["estado"] for a in m.alertas], ["vigente"])
        m.check = None                           # Check se cae: tampoco se resuelve a ciegas
        st, j = m.llamar(check=True)
        self.assertEqual(st, 200)
        self.assertEqual([a["estado"] for a in m.alertas], ["vigente"])
        self.assertEqual(j["semaforos"]["33193"]["color"], "gris")

    def test_C_hay_guia_y_check_no_registra_la_salida_pasados_3_dias_habiles(self):
        m = _mundo10(desp=0, hoy=datetime.datetime(2026, 10, 13, 10, 0))
        st, j = m.llamar(check=True)
        self.assertEqual((m.alertas, m.correos), ([], []), "dentro del plazo: se ve en ámbar, sin alerta")
        self.assertEqual(j["semaforos"]["33193"]["color"], "ambar")
        self.assertIn("todavía en plazo (hasta el 14/10/2026)", j["semaforos"]["33193"]["texto"])
        m.hoy = datetime.datetime(2026, 10, 29, 10, 0)
        st, j = m.llamar(check=True)
        self.assertEqual([(a["codigo"], a["detalle"]) for a in m.alertas], [("guia_sin_salida_check", "33193")])
        self.assertIn("Hay guía GDV 33193 del 08/10/2026 y Check no registra que haya salido", m.correos[0]["cuerpo"])
        self.assertIn("Check no registra la salida (plazo vencido el 14/10/2026)", j["semaforos"]["33193"]["texto"])

    def test_si_check_no_tiene_la_guia_no_se_alerta(self):
        m = _mundo10()
        m.check = lambda params: {"body": {"response": []}}
        st, j = m.llamar(check=True)
        self.assertEqual((m.alertas, m.correos), ([], []))
        self.assertEqual(j["semaforos"]["33193"]["color"], "gris")

    def test_correo_apagado_no_se_reintenta_y_un_error_si(self):
        m = _mundo10()
        m.correo_modo = "apagado"                # Control de correos (o la llave) lo frenó: queda en el Historial como bloqueado
        m.llamar(check=True)
        m.llamar(check=True)
        self.assertEqual(len(m.correos), 1, "frenado por un interruptor no se reintenta en cada lectura")
        m2 = _mundo10()
        m2.correo_modo = "error"
        m2.llamar(check=True)
        self.assertIsNone(m2.alertas[0]["correo_huella"], "un error (no un interruptor) suelta el reclamo")
        m2.correo_modo = "ok"
        m2.llamar(check=True)
        self.assertEqual(len(m2.correos), 1)

    def test_el_detalle_de_la_guia_trae_las_tres_bandas_y_todos_los_semaforos(self):
        m = _mundo10(traza=[{"doc": "GDV-0000033193", "ot": "4567", "tipoOT": "CONTROL DE SALIDA", "estado": "FINALIZADA",
                             "usuario": "Samuel Rojas", "feInicioOT": "2026-10-08T11:02:00"}])
        st, j = m.llamar(guia="33193")
        d = j["detalle"]
        self.assertEqual(d["diferencias"][0]["regla"], "B")
        p = d["productos"][0]
        self.assertEqual((p["nombre"], p["ot"], p["guia"], p["check"]), (BANCO, 10, 10, 8))
        self.assertEqual(j["bodega"]["33193"]["movimientos"]["ots"][0]["personas"], ["Samuel Rojas"])
        self.assertIn("33193", j["semaforos"])
        self.assertNotIn("59990", json.dumps(j), "el «valor» de Check jamás sale")

    def test_tecnico_403_y_ni_campana_ni_correo(self):
        m = _mundo10(tecnico=True)
        st, j = m.llamar(check=True)
        self.assertEqual(st, 403)
        self.assertEqual((m.correos, m.notifs, m.check_llamadas, m.random_sql), ([], [], [], []))


class TestCorreoInterno(unittest.TestCase):
    """El correo pasa por _aviso_interno_correo (el de verdad, de app.py) → _send_ilus_email con su clave del registro."""

    def test_esta_en_el_registro_nace_encendido_y_se_puede_apagar(self):
        import comm_correos_registro as R
        c = R.por_clave("ot_guia_diferencia")
        self.assertIsNotNone(c)
        self.assertEqual((c["modulo_ui"], c["llave"], c["destinatario"], c["tipo"]),
                         ("ot", "comunicacion_interna", "responsable_ot", "ilus"))
        self.assertFalse(c["protegido"])
        self.assertFalse(c["nace_apagado"], "Daniel lo pidió: nace encendido")
        self.assertTrue(R.switch_de_fila("ot_guia_diferencia", None))
        self.assertFalse(R.switch_de_fila("ot_guia_diferencia", {"activo": 0}), "Daniel lo apaga en Control de correos")
        self.assertIn("nunca al técnico ni al cliente", c["avisa_a"])

    def test_la_puerta_recibe_la_clave_y_nunca_va_a_un_tecnico(self):
        enviados = []
        usuarios = [{"id": 2, "username": "victor.soto@sphs.cl", "role": "supervisor"},
                    {"id": 4, "username": "jaizer@sphs.cl", "role": "tecnico_ejecutivo"},
                    {"id": 6, "username": "sin-correo", "role": "admin"}]

        def enviar(to, asunto, html, **kw):
            enviados.append((to, asunto, kw))
            return True
        amb = {"mysql_fetchall": lambda sql, p=None: [u for u in usuarios if u["id"] in p],
               "_ilus_email_master": lambda ctx: "<p>" + ctx["message"] + "</p>", "_public_base_url": lambda: "https://ilus.test",
               "_GESTION_CORREO_RE": re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]{2,}$"), "_brand_subject": lambda t: "ILUS · " + t,
               "_rol_familia": lambda r: "tecnico" if str(r or "").startswith("tecnico") else "gestion",
               "_send_ilus_email": enviar, "print": lambda *a, **k: None}
        exec(compile(ast.parse(FA.fuente_funcion("_aviso_interno_correo")), "<app.py:_aviso_interno_correo>", "exec"), amb)
        n = amb["_aviso_interno_correo"](G.CORREO_CLAVE, [2, 4, 6], "OT-2026-00171: 1 diferencia", "cuerpo <b>", "/ot/5",
                                         "Guía de despacho vs OT", ref={"tipo": "OT", "id": 5, "codigo": "OT-2026-00171"})
        self.assertEqual(n, 1)
        self.assertEqual([e[0] for e in enviados], ["victor.soto@sphs.cl"], "ni técnicos ni usuarios sin correo")
        kw = enviados[0][2]
        self.assertEqual((kw["correo"], kw["modulo"]), ("ot_guia_diferencia", "comunicacion_interna"))
        self.assertEqual(enviados[0][1], "ILUS · OT-2026-00171: 1 diferencia")


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

    def test_tabla_barata_una_clausula_por_sentencia_y_nunca_se_borra(self):
        """REGLA #18: la tabla nace con UNA sentencia CREATE TABLE IF NOT EXISTS y las dos columnas del correo (29-oct) se
        agregan con UNA cláusula por sentencia, por mysql_execute (que las salta sin lock si ya existen)."""
        src = FA.fuente_funcion("_ot_logi_tabla")
        self.assertEqual(src.count("mysql_execute("), 3)
        self.assertIn("CREATE TABLE IF NOT EXISTS ot_guia_alerta", src)
        self.assertIn("UNIQUE KEY uq_ga_ot_cod_det (visita_id, codigo, detalle)", src)
        alters = re.findall(r'mysql_execute\("(ALTER TABLE[^"]*)"\)', src)
        self.assertEqual(alters, ["ALTER TABLE ot_guia_alerta ADD COLUMN correo_huella VARCHAR(64) NULL",
                                  "ALTER TABLE ot_guia_alerta ADD COLUMN correo_el DATETIME NULL"])
        for a in alters:
            self.assertNotIn(",", a, "una sola cláusula por sentencia")
            self.assertNotRegex(a, r"(?i)\b(DROP|MODIFY|CHANGE)\b")
        self.assertIsNone(re.search(r"(DELETE\s+FROM|TRUNCATE\s+(TABLE\s+)?)ot_guia_alerta", APP, re.I))
        self.assertNotIn("ALTER TABLE ot_guia_alerta", APP.replace(src, ""), "solo _ot_logi_tabla toca la tabla")

    def test_enum_de_la_campana_tiene_el_tipo_nuevo_al_final_de_las_dos_listas(self):
        enums = re.findall(r"ALTER TABLE mant_notificaciones MODIFY COLUMN tipo \"\s*\"\s*ENUM\((.*?)\) DEFAULT 'otro'", APP, re.S)
        self.assertEqual(len(enums), 2)
        for e in enums:
            valores = re.findall(r"'([a-z_]+)'", e)
            self.assertEqual(valores[-1], G.TIPO_AVISO, "al FINAL: nada se reordena (REGLA #18)")
            self.assertEqual(valores[-2], "tk_sla")
            self.assertEqual(valores.count(G.TIPO_AVISO), 1)

    def test_el_correo_es_interno_por_la_puerta_unica_y_nunca_al_cliente(self):
        """2026-10-29 (Daniel: «dame una ALERTA, así sea POR CORREO»): ahora SÍ hay correo, pero solo interno: sale por
        _aviso_interno_correo (puerta única con su clave del registro) a la gestión que decide _hall_destinos. Nunca un envío
        directo, nunca al contacto/cliente, nunca WhatsApp."""
        for prohibido in ("_send_ilus_email", "_mant_send_email", "whatsapp", "contraparte", "correo_contacto", "email_cliente"):
            self.assertNotIn(prohibido, BLOQUE)
        self.assertEqual(BLOQUE.count("_aviso_interno_correo("), 1, "un solo lugar envía el correo")
        src = FA.fuente_funcion("_ot_logi_avisar")
        self.assertIn("_hall_destinos(vid, v, excluir_uid=_hall_uid())", src)
        self.assertIn("_gvo.TIPO_AVISO", src)
        cor = FA.fuente_funcion("_ot_logi_correo")
        self.assertIn("_hall_destinos(vid, v, excluir_uid=_hall_uid())", cor)
        self.assertIn("_aviso_interno_correo(_gvo.CORREO_CLAVE, uids,", cor)
        self.assertEqual(G.CORREO_CLAVE, "ot_guia_diferencia")

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
// 2026-10-29: la venta es UNA fila de chips (static/ot_doc_cadena.js) y el detalle se abre en el panel de la venta.
function armar(nums) {
  var sec = new El('section', { 'class': 'dc' });
  var hd = sec.append(new El('div', { 'class': 'dc-h' }));
  hd.append(new El('span', { 'data-gl-chip': '' }));
  var venta = sec.append(new El('div', { 'class': 'dc-venta', 'data-dc-venta': '0' }));
  var ol = venta.append(new El('ol', { 'class': 'dc-fila' }));
  ol.append(new El('li', {})).append(new El('button', { 'class': 'dc-doc t-cobro en-ot', 'data-dc-doc': '0:0', 'aria-expanded': 'false' }));
  var gs = ol.append(new El('li', { 'class': 'dc-gs' }));
  nums.forEach(function (n, i) {
    var b = gs.append(new El('button', { 'class': 'dc-doc t-guia', 'data-dc-doc': '0:' + (i + 1), 'data-gl-guia': n, 'data-gl-ver': n,
                                          'aria-expanded': 'false', 'title': 'Guía de despacho GDV ' + n + '. Ver qué salió y qué no' }));
    b.append(new El('span', { 'class': 'gl-dot gris pend', 'data-gl-dot': '' }));
  });
  venta.append(new El('p', { 'class': 'dc-res', 'data-gl-resumen': '', 'hidden': '' }));
  venta.append(new El('div', { 'class': 'dc-panel', 'data-dc-panel': '', 'hidden': '' }));
  return sec;
}
function chipDe(sec, n) { return sec.querySelector('[data-gl-ver="' + n + '"]'); }
function puntoDe(sec, n) { return chipDe(sec, n).querySelector('[data-gl-dot]').className; }
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
                      "33194": {"color": "gris", "texto": "La guía cuadra con la OT · consultando la bodega (Check)…", "pendiente": True}}}
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
        detalle["guias"] = [{"numero": "33193", "fecha": "08/10/2026", "desde": "FCV 11745"}]
        # 2026-10-29: el panel lo arma el servidor (ot_guia_vs_ot.detalle_guia): diferencias arriba y OT · guía · Check.
        detalle["detalle"] = {"guia": "33193", "check": {"estado": "completo", "color": "verde", "texto": "bodega expidió 1 de 1"},
                              "diferencias": [{"codigo": "esta_en_la_OT_y_no_salio", "regla": "A",
                                               "texto": "Trotadora <X> (X1): la OT gestiona 1 y no salió en ninguna guía."}],
                              "productos": [{"sku": "BB-01", "nombre": "Booty Builder Cushioned Belt", "ot": 1, "guia": 1,
                                             "check": 1, "estado": "coincide", "es_diferencia": False, "texto": "Salió 1 y la OT gestiona 1.",
                                             "check_txt": "Salió de bodega (1 de 1, según Check)", "check_color": "verde",
                                             "color": "verde", "en_guia": True},
                                            {"sku": "X1", "nombre": "Trotadora X", "ot": 1, "guia": 0, "check": None,
                                             "estado": "esta_en_la_OT_y_no_salio", "es_diferencia": True,
                                             "texto": "La OT gestiona 1 y no salió en ninguna guía.",
                                             "check_txt": "No salió en ninguna guía", "check_color": "gris", "color": "ambar",
                                             "en_guia": False}]}
        r = self._correr(r"""
          const sec = armar(['33193', '33194']);
          L.montar(sec, 5);
          await espera(); await espera(); await espera();
          const chip = sec.querySelector('[data-gl-chip]').innerHTML;
          const clases = [chipDe(sec, '33193').classList.toString(), chipDe(sec, '33194').classList.toString()];
          const puntos = [puntoDe(sec, '33193'), puntoDe(sec, '33194')];
          const etiquetas = [chipDe(sec, '33193').getAttribute('aria-label'), chipDe(sec, '33194').getAttribute('aria-label')];
          const res = sec.querySelector('[data-gl-resumen]');
          const resumen = { html: res.innerHTML, hidden: res.hidden };
          const btn = chipDe(sec, '33193');
          const ev = { target: btn, preventDefault: function(){} };
          sec._h.forEach(function (h) { h(ev); });
          const marcado = !!ev.__glHecho;
          const panel = sec.querySelector('[data-dc-panel]');
          const cargando = panel.innerHTML;
          await espera(); await espera();
          const abierto = { html: panel.innerHTML, hidden: panel.hidden, exp: btn.getAttribute('aria-expanded'),
                            otro: chipDe(sec, '33194').getAttribute('aria-expanded'),
                            doc: sec.querySelector('[data-dc-doc="0:0"]').getAttribute('aria-expanded') };
          clic(sec, btn);
          const cerrado = { hidden: panel.hidden, exp: btn.getAttribute('aria-expanded'), html: panel.innerHTML };
          const puntosDespues = [puntoDe(sec, '33193'), puntoDe(sec, '33194')];
          console.log(JSON.stringify({ chip, clases, puntos, etiquetas, resumen, marcado, cargando, abierto, cerrado, puntosDespues, pedidos }));
        """, {"/ot/api/5/documentos/logistica": self.RESUMEN, "/ot/api/5/documentos/logistica?check=1": self.CON_CHECK,
              "/ot/api/5/documentos/logistica?guia=33193": detalle})
        self.assertIn("2 diferencias con la OT", r["chip"])
        self.assertEqual(r["clases"], ["dc-doc t-guia", "dc-doc t-guia"], "colores con medida: el chip no se pinta entero")
        # el punto de cada guía ya trae lo que dijo Check (2.ª lectura, ?check=1)
        self.assertEqual(r["puntos"], ["gl-dot ambar", "gl-dot verde"])
        self.assertIn("Semáforo ámbar: Faltan por salir 1 de lo que gestiona la OT · bodega expidió 1 de 1", r["etiquetas"][0])
        self.assertIn("Semáforo verde: Salió todo lo gestionado · bodega expidió 2 de 2", r["etiquetas"][1])
        # bajo la fila, una línea con lo que está en ámbar (lo verde no ocupa espacio)
        self.assertFalse(r["resumen"]["hidden"])
        self.assertIn("GDV 33193: Faltan por salir 1", r["resumen"]["html"])
        self.assertNotIn("GDV 33194", r["resumen"]["html"])
        self.assertTrue(r["marcado"], "la cadena no vuelve a atender el clic de la guía")
        self.assertIn("Consultando la bodega", r["cargando"])
        self.assertIn("Guía de despacho GDV 33193", r["cargando"])
        h = r["abierto"]["html"]
        self.assertFalse(r["abierto"]["hidden"])
        self.assertEqual((r["abierto"]["exp"], r["abierto"]["otro"], r["abierto"]["doc"]), ("true", "false", "false"))
        # las diferencias con la OT, ARRIBA (lo que le interesa a Daniel)
        self.assertLess(h.index("1 diferencia con la OT"), h.index("Qué salió y qué no"))
        self.assertIn("Trotadora &lt;X&gt; (X1): la OT gestiona 1 y no salió en ninguna guía.", h, "todo se escapa")
        self.assertNotIn("<X>", h)
        # qué salió y qué no, por producto: OT · guía · Check
        for txt in ("<th scope=\"col\">OT</th>", "<th scope=\"col\">Guía</th>", "<th scope=\"col\">Check</th>",
                    "Salió de bodega (1 de 1, según Check)", "No salió en ninguna guía", "Booty Builder Cushioned Belt"):
            self.assertIn(txt, h)
        self.assertIn("08/10/2026 · salió desde FCV 11745", h)
        self.assertIn("Bodega expidió 1 de 1", h, "lo que dice Check, con la regla de las tres bandas")
        self.assertIn("Según Check: esta guía", h)
        self.assertIn("Luis Pérez", h)
        self.assertIn("UA1012965", h)
        self.assertEqual(r["cerrado"], {"hidden": True, "exp": "false", "html": ""})
        self.assertEqual(r["puntosDespues"], r["puntos"], "el detalle no borra lo que Check ya dijo de las otras guías")
        self.assertEqual(r["pedidos"], ["/ot/api/5/documentos/logistica", "/ot/api/5/documentos/logistica?check=1",
                                        "/ot/api/5/documentos/logistica?guia=33193"])

    def test_el_chip_de_la_cabecera_suma_lo_que_dijo_check(self):
        cot = {"hallazgos": [{"codigo": "check_expidio_menos", "texto": "x"}, {"codigo": "guia_sin_salida_check", "texto": "y"}]}
        r = self._correr(r"""
          console.log(JSON.stringify({ a: L._htmlChip(%s, %s), b: L._htmlChip({estado: 'coincide', n_diferencias: 0}, %s) }));
        """ % (json.dumps({"estado": "diferencias", "n_diferencias": 1}), json.dumps(cot), json.dumps(cot)), {})
        self.assertIn("3 diferencias con la OT", r["a"])
        self.assertIn("2 diferencias con la OT", r["b"], "Check manda: aunque la guía cuadre, si Check informa menos es diferencia")

    def test_sin_check_configurado_no_hay_segunda_lectura(self):
        sin = dict(self.RESUMEN, check=False, semaforos={"33193": {"color": "gris", "texto": "x · sin conexión con Check", "pendiente": False}})
        r = self._correr(r"""
          const sec = armar(['33193']);
          L.montar(sec, 7);
          await espera(); await espera();
          console.log(JSON.stringify({ sem: chipDe(sec, '33193').getAttribute('aria-label'), punto: puntoDe(sec, '33193'), pedidos }));
        """, {"/ot/api/7/documentos/logistica": sin})
        self.assertIn("sin conexión con Check", r["sem"])
        self.assertEqual(r["punto"], "gl-dot gris")
        self.assertEqual(r["pedidos"], ["/ot/api/7/documentos/logistica"])

    def test_si_la_lectura_falla_ofrece_reintentar(self):
        r = self._correr(r"""
          const sec = armar(['33193']);
          L.montar(sec, 6);
          await espera();
          clic(sec, chipDe(sec, '33193'));
          await espera(); await espera();
          console.log(JSON.stringify({ html: sec.querySelector('[data-dc-panel]').innerHTML,
                                       chip: sec.querySelector('[data-gl-chip]').innerHTML,
                                       punto: puntoDe(sec, '33193') }));
        """, {})
        self.assertIn("No pudimos leer la logística ahora", r["html"])
        self.assertIn('data-gl-reintentar="33193"', r["html"])
        self.assertEqual((r["chip"], r["punto"]), ("", "gl-dot gris"), "si falló, el punto queda gris y quieto (no «cargando»)")

    def test_si_mientras_carga_se_abre_otro_documento_la_guia_no_lo_pisa(self):
        r = self._correr(r"""
          const sec = armar(['33193']);
          L.montar(sec, 8);
          await espera(); await espera();
          clic(sec, chipDe(sec, '33193'));
          const panel = sec.querySelector('[data-dc-panel]');
          panel.removeAttribute('data-gl-abierta');    // lo que hace ot_doc_cadena.js al abrir la factura
          panel.innerHTML = 'FACTURA';
          await espera(); await espera();
          console.log(JSON.stringify({ html: panel.innerHTML }));
        """, {"/ot/api/8/documentos/logistica": self.RESUMEN, "/ot/api/8/documentos/logistica?check=1": self.CON_CHECK,
              "/ot/api/8/documentos/logistica?guia=33193": self.CON_CHECK})
        self.assertEqual(r["html"], "FACTURA")

    def test_css_con_medida_sin_tarjetas_pintadas(self):
        css = _leer("static/ot_guia_logistica.css")
        self.assertIsNone(re.search(r"\.dc-n[^{]*\{[^}]*background", css), "la tarjeta de la guía no se pinta entera")
        self.assertIsNone(re.search(r"border-left\s*:\s*[3-9]px", css), "sin bordes laterales gruesos de color")
        self.assertIn(".gl-dot{", css)
        self.assertRegex(css, r"\.gl-dot\{[^}]*width:10px")
        self.assertIn("prefers-reduced-motion", css, "el punto que late se queda quieto si la persona lo pide")
        cad = _leer("static/ot_doc_cadena.css")
        self.assertIsNone(re.search(r"border-left\s*:\s*[3-9]px", cad))
        self.assertIsNone(re.search(r"\.dc-doc\.(en-ot|t-guia|baja)\{[^}]*background", cad), "el chip no se pinta por su estado")
        self.assertRegex(cad, r"@media \(max-width:600px\)\{[^@]*\.dc-doc\{min-height:44px", "toque de 44 px en el celular")
        self.assertNotIn("text-overflow", cad + css, "nada se corta (REGLA #15)")
        self.assertNotIn("line-clamp", cad + css)


@unittest.skipUnless(NODE, "Node no está instalado")
class TestCadenaCompacta(unittest.TestCase):
    """«En la tarjeta de la guía de despacho quiero que sea PEQUEÑA; no quiero que ocupe tanto espacio la trazabilidad de
    Random.» Cada venta es UNA fila de chips; el detalle de cada documento sigue existiendo, detrás del clic (REGLA #4.2)."""
    CADENA = {"ok": True, "visita_id": 171, "sugerencias": [], "alertas": [], "cadenas": [{"leida": True, "nodos": [
        {"tipo": "VD", "numero": "10414", "titulo": "VD 10414", "tipo_txt": "Nota de venta", "fecha": "06/10/2026", "en_ot": True,
         "dada_de_baja": True, "reemplazado_por": "FCV 11745", "saldo": None, "usos": []},
        {"tipo": "FCV", "numero": "11745", "titulo": "FCV 11745", "tipo_txt": "Factura", "fecha": "06/10/2026", "en_ot": True,
         "mismo_cliente": True, "saldo": {"servicio": {"total": 961640, "saldo": 709539}},
         "usos": [{"vid": 249, "numero_ot": "OT-2026-00249", "cliente": "Physica <Ppc>", "url": "/ot/249", "servicio": 252101,
                   "medible": True, "margen_pct": 12.4, "nivel": "ambar"}]},
        {"tipo": "GDV", "numero": "33193", "titulo": "GDV 33193", "tipo_txt": "Guía de despacho", "fecha": "08/10/2026",
         "desde": "FCV 11745", "texto": "Salió el stock el 08/10/2026 (1 producto)",
         "productos": [{"sku": "BB-01", "nombre": "Booty Builder Cushioned Belt", "cantidad": 1}]},
        {"tipo": "GDV", "numero": "33194", "titulo": "GDV 33194", "tipo_txt": "Guía de despacho", "fecha": "09/10/2026",
         "desde": "FCV 11745", "productos": []}]}]}

    def test_una_fila_chica_por_venta_y_el_detalle_detras_del_clic(self):
        script = r"""
          const fs = require('fs');
          global.window = {};
          global.document = { readyState: 'complete', querySelectorAll: () => [] };
          eval(fs.readFileSync(process.argv[1], 'utf8'));
          const C = window.OTDocCadena;
          const d = %s;
          console.log(JSON.stringify({ html: C._html(d), fcv: C._htmlNodo(d.cadenas[0].nodos[1]), vd: C._htmlNodo(d.cadenas[0].nodos[0]),
                                       gdv: C._htmlNodo(d.cadenas[0].nodos[2]) }));
        """ % json.dumps(self.CADENA)
        out = subprocess.run([NODE, "-e", script, os.path.join(RAIZ, "static", "ot_doc_cadena.js")], capture_output=True,
                             text=True, encoding="utf-8", timeout=60)
        self.assertEqual(out.returncode, 0, out.stderr)
        r = json.loads(out.stdout.strip().splitlines()[-1])
        h = r["html"]
        self.assertEqual(h.count('class="dc-fila"'), 1, "una fila por venta")
        self.assertEqual(h.count('class="bi bi-arrow-right dc-fl"'), 2, "VD → FCV → [guías], con el ícono del proyecto")
        self.assertLess(h.index("10414"), h.index("11745"))
        self.assertLess(h.index("11745"), h.index("33193"))
        for n in ("33193", "33194"):
            self.assertIn('data-gl-ver="%s"' % n, h)
        self.assertEqual(h.count("data-gl-dot"), 2, "un punto de semáforo por guía")
        self.assertIn('class="dc-doc t-nota baja"', h, "la nota dada de baja va tachada")
        self.assertIn("Factura FCV 11745, en esta OT. Ver su detalle", h)
        self.assertIn('data-dc-panel role="region" aria-label="Detalle del documento" hidden', h)
        self.assertIn('id="dc-panel-171-0"', h)
        self.assertIn('aria-controls="dc-panel-171-0"', h)
        # lo grande ya no está en la fila…
        for grande in ("libre $", "También la usa", "Booty Builder", "margen"):
            self.assertNotIn(grande, h)
        # …pero sigue existiendo detrás del clic (REGLA #4.2): saldo por línea, otras OT y su margen, productos de la guía
        self.assertIn("libre $709.539 de $961.640", r["fcv"])
        self.assertIn("También la usa", r["fcv"])
        self.assertIn("margen 12,4 %", r["fcv"])
        self.assertIn("Physica &lt;Ppc&gt;", r["fcv"], "todo se escapa")
        self.assertIn("Dada de baja por FCV 11745", r["vd"])
        self.assertIn("Booty Builder Cushioned Belt", r["gdv"])

    def test_la_cadena_deja_la_guia_a_la_logistica_y_abre_lo_demas(self):
        js = _leer("static/ot_doc_cadena.js")
        self.assertIn("!e.__glHecho", js)
        self.assertIn("OTGuiaLogistica.montar(sec, vid)", js)
        self.assertIn("panel.removeAttribute('data-gl-abierta')", js)
        log = _leer("static/ot_guia_logistica.js")
        self.assertIn("e.__glHecho = true", log)

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

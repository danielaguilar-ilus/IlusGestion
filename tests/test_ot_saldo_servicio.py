"""SALDO por línea de servicio de cada documento (Daniel, 08-oct-2026):
«algo bien inteligente para evitar que dos instalaciones se paguen con el mismo saldo».

Una línea de servicio (ZZINSTALACION…) o de despacho (ZZENVIO) de una factura no puede contarse como cobro, sumando
todas las OT (no canceladas ni anuladas), por más que su monto. Qué se vigila:
  · La lógica pura (ot_saldo_servicio.py): saldo, familia nota de venta ↔ factura, reparto, acciones.
  · El candado de app.py (_ot_saldo_chequear y sus lecturas) con un mundo en memoria: dos OT con la misma factura de
    $200.000, la segunda solo toma el saldo; con saldo 0, rechazo CON salidas; la nota de venta dada de baja por su
    factura no cuenta doble.
  · Que el candado esté cableado en TODOS los caminos (crear, ligar documento, regularizar, asociar-factura, declarar
    el cobro, modal de cierre), con la acción «tomar solo el saldo» y las avisos en Regularizar y Facturación de
    proveedor, y que el motor, el asistente y el modal de cierre lo ofrezcan sin callejón sin salida.
Sin BD, sin Flask, sin ERP (solo lectura, REGLA #4.1: nada de esto escribe en Random).
Correr con:  py -m unittest tests.test_ot_saldo_servicio
"""
import ast
import os
import re
import unittest

import ot_saldo_servicio as S
from tests.test_incidencias_bajas import _codigo_y_arbol

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _leer(rel):
    with open(os.path.join(RAIZ, rel.replace("/", os.sep)), encoding="utf-8", newline="") as f:
        return f.read().replace("\r\n", "\n")


# ───────────────────────── lógica pura ─────────────────────────
class TestPuro(unittest.TestCase):
    def test_clave_unica_para_la_nota_de_venta(self):
        self.assertEqual(S.clave_doc("NVV", "VD00010667"), ("VD", "10667"))
        self.assertEqual(S.clave_doc("VD", "10667"), ("VD", "10667"))
        self.assertEqual(S.clave_doc("fcv", "0000011439"), ("FCV", "11439"))
        self.assertEqual(S.clave_doc("", ""), ("", ""))

    def test_variantes_cubren_las_formas_guardadas(self):
        v = S.variantes_sql(("VD", "10667"))
        self.assertIn(("NVV", "VD00010667"), v)
        self.assertIn(("VD", "10667"), v)
        self.assertIn(("FCV", "0000011439"), S.variantes_sql(("FCV", "11439")))

    def test_clasificacion_servicio_y_despacho_sin_retiro(self):
        cap, lin = S.clasificar_lineas([
            {"sku": "ZZINSTALACION", "monto": 200000}, {"sku": "zzenvio", "monto": 30000},
            {"sku": "ZZRETIRO", "monto": 5000}, {"sku": "BICI-1", "monto": 900000}])
        self.assertEqual(cap, {"servicio": 200000, "despacho": 30000})
        self.assertEqual([l["sku"] for l in lin["servicio"]], ["ZZINSTALACION"])

    def test_saldo_por_linea(self):
        r = S.calcular([{"sku": "ZZINSTALACION", "monto": 200000}],
                       S.consolidar_usos([{"vid": 1, "numero_ot": "OT-1", "cliente": "A", "estado": "programada", "servicio": 120000}]))
        self.assertEqual(r["servicio"]["usado"], 120000)
        self.assertEqual(r["servicio"]["saldo"], 80000)
        self.assertEqual(r["servicio"]["lineas"][0]["saldo"], 80000)
        self.assertFalse(r["despacho"]["hay_lineas"])

    def test_reparto_de_lineas_en_orden(self):
        lin = S.asignar_a_lineas([{"sku": "A", "monto": 100}, {"sku": "B", "monto": 100}], 150)
        self.assertEqual([(l["usado"], l["saldo"]) for l in lin], [(100, 0), (50, 50)])

    def test_consolidar_toma_el_mayor_aporte_por_ot_no_la_suma(self):
        u = S.consolidar_usos([
            {"vid": 1, "numero_ot": "OT-1", "estado": "programada", "servicio": 200000, "clave": ("VD", "10667")},
            {"vid": 1, "numero_ot": "OT-1", "estado": "programada", "servicio": 200000, "clave": ("FCV", "11500")}])
        self.assertEqual(len(u), 1)
        self.assertEqual(u[0]["servicio"], 200000)
        self.assertEqual(len(u[0]["documentos"]), 2)

    def test_canceladas_y_anuladas_no_cuentan(self):
        u = S.consolidar_usos([{"vid": 1, "estado": "cancelada", "servicio": 9}, {"vid": 2, "estado": "anulada", "servicio": 9},
                               {"vid": 3, "estado": "cerrada", "servicio": 7}])
        self.assertEqual([x["vid"] for x in u], [3])

    def test_sin_lineas_no_hay_nada_que_consumir(self):
        sal = S.combinar([S.calcular([], [])])
        ev = S.evaluar_pedido(sal, {"servicio": 500000, "despacho": 1})
        self.assertTrue(ev["ok"])

    def test_pedido_mayor_al_saldo(self):
        sal = S.combinar([S.calcular([{"sku": "ZZINSTALACION", "monto": 200000}],
                                     [{"vid": 1, "servicio": 150000, "despacho": 0}])])
        ev = S.evaluar_pedido(sal, {"servicio": 200000})
        self.assertFalse(ev["ok"])
        self.assertEqual(ev["excesos"][0]["exceso"], 150000)
        self.assertEqual(ev["permitido"]["servicio"], 50000)

    def test_acciones_siempre_hay_salida(self):
        con_saldo = S.acciones_exceso([{"categoria": "servicio", "pedido": 200, "saldo": 80, "exceso": 120}])
        self.assertEqual([a["tipo"] for a in con_saldo],
                         ["tomar_saldo", "ligar_factura", "pasar_garantia", "pedir_autorizacion"])
        self.assertEqual(con_saldo[0]["monto"], 80)
        sin_saldo = S.acciones_exceso([{"categoria": "servicio", "pedido": 200, "saldo": 0, "exceso": 200}])
        self.assertEqual([a["tipo"] for a in sin_saldo], ["ligar_factura", "pasar_garantia", "pedir_autorizacion"])

    def test_repartir_aportes_hasta_la_capacidad_de_cada_documento(self):
        rep = S.repartir_aportes([{"servicio": 100, "despacho": 10}, {"servicio": 100, "despacho": 0}], 150, 10)
        self.assertEqual(rep, [{"servicio": 100, "despacho": 10}, {"servicio": 50, "despacho": 0}])
        # un cobro a mano que ningún documento respalda no se reparte
        self.assertEqual(S.repartir_aportes([{"servicio": 0, "despacho": 0}], 300, 0), [{"servicio": 0, "despacho": 0}])

    def test_nota_de_venta_con_su_factura_en_la_misma_ot_no_duplica(self):
        infos = [{"clave": ["VD", "10667"], "familia": [["FCV", "11500"]], "leido": True},
                 {"clave": ["FCV", "11500"], "familia": [["VD", "10667"]], "leido": True}]
        cuentan = S.marcar_omitidas(infos)
        self.assertEqual([i["clave"] for i in cuentan], [["FCV", "11500"]])
        self.assertTrue(infos[0]["omitido"])

    def test_aviso_compartido(self):
        t = S.aviso_compartido("FCV 11439", [{"numero_ot": "OT-2026-00010"}, {"numero_ot": "OT-2026-00011"}])
        self.assertIn("FCV 11439", t)
        self.assertIn("OT-2026-00010", t)
        self.assertEqual(S.aviso_compartido("FCV 1", []), "")


# ───────────────────────── el candado de app.py, con un mundo en memoria ─────────────────────────
FUNCS = ("_ot_saldo_real", "_ot_saldo_doc", "_ot_saldo_de_docs", "_ot_saldo_chequear", "_ot_saldo_aporte_en",
         "_ot_saldo_avisos_de", "_ot_doc_real_a_usuario")
_ARBOL = None


def _arbol():
    global _ARBOL
    if _ARBOL is None:
        _ARBOL = _codigo_y_arbol()
    return _ARBOL


class Mundo:
    """Documentos del ERP + OT en memoria. Las lecturas de base/ERP de app.py se reemplazan por esto."""

    def __init__(self):
        self.erp, self.familia, self.ots, self.autorizado = {}, {}, {}, {}
        amb = {"print": lambda *a, **k: None, "_saldo": S}
        _codigo, arbol = _arbol()
        for nodo in arbol.body:
            if isinstance(nodo, ast.FunctionDef) and nodo.name in FUNCS:
                nodo.decorator_list = []
                exec(compile(ast.Module(body=[nodo], type_ignores=[]), "<app>", "exec"), amb)
        faltan = [n for n in FUNCS if n not in amb]
        assert not faltan, faltan
        amb["_erp_zz_lineas"] = self._erp_zz_lineas
        amb["_ot_saldo_familia"] = self._familia
        amb["_ot_saldo_usos"] = self._usos
        amb["_rut_analisis_comparacion"] = lambda a, b: {"match": (a or "") == (b or "")}
        amb["_ot_saldo_autorizado_hasta"] = lambda vid=None, aut_id=None, cliente_rut=None: dict(self.autorizado)
        self.amb = amb

    # ERP
    def doc(self, tido, nudo, zz, rut="11.111.111-1", total=0):
        self.erp[S.clave_doc(tido, nudo)] = {"rut": rut, "zz": zz, "total": total}

    def hijo_de(self, factura, nota):
        """La factura nace de la nota de venta (MAEDDO.TIDOPA): son el mismo cobro."""
        kf, kn = S.clave_doc(*factura), S.clave_doc(*nota)
        self.familia.setdefault(kf, set()).add(kn)
        self.familia.setdefault(kn, set()).add(kf)

    def _erp_zz_lineas(self, t, n):
        d = self.erp.get(S.clave_doc(t, n))
        if not d:
            return None, [], 0
        return {"cliente_rut": d["rut"]}, list(d["zz"]), d["total"]

    def _familia(self, t, n):
        k = S.clave_doc(t, n)
        return {k} | self.familia.get(k, set())

    # Base
    def ot(self, vid, numero, cliente="Gimnasio A", estado="programada"):
        self.ots[vid] = {"numero_ot": numero, "cliente": cliente, "estado": estado, "aportes": {}}

    def aporta(self, vid, doc, servicio=None, despacho=None):
        self.ots[vid]["aportes"][S.clave_doc(*doc)] = (servicio, despacho)

    def _usos(self, claves, excluir_vid=None, cap=None):
        filas = []
        for vid, o in self.ots.items():
            if excluir_vid and vid == excluir_vid:
                continue
            if o["estado"] in ("cancelada", "anulada"):
                continue
            for k, (s, e) in o["aportes"].items():
                if k in claves and (s is not None or e is not None):
                    filas.append({"vid": vid, "numero_ot": o["numero_ot"], "cliente": o["cliente"], "estado": o["estado"],
                                  "servicio": s, "despacho": e, "clave": k})
        return filas

    # el candado
    def chequear(self, docs, pedido, excluir=None, rut=None):
        return self.amb["_ot_saldo_chequear"](docs, pedido, excluir_vid=excluir, cliente_rut=rut,
                                              autorizado_hasta=dict(self.autorizado))


FACT = ("FCV", "11439")
INSTALACION = [{"sku": "ZZINSTALACION", "descripcion": "Instalación", "monto": 200000}]


class TestDosOtMismaFactura(unittest.TestCase):
    def setUp(self):
        self.m = Mundo()
        self.m.doc(*FACT, zz=INSTALACION + [{"sku": "ZZENVIO", "descripcion": "Despacho", "monto": 30000}], total=1230000)
        self.m.ot(1, "OT-2026-00001", "Gimnasio A")
        self.m.ot(2, "OT-2026-00002", "Gimnasio A")

    def test_la_primera_toma_todo_y_la_segunda_solo_el_saldo(self):
        self.assertIsNone(self.m.chequear([FACT], {"servicio": 200000}, excluir=1))
        self.m.aporta(1, FACT, servicio=120000)               # la primera OT cobró la instalación a medias
        err = self.m.chequear([FACT], {"servicio": 200000}, excluir=2)
        self.assertIsNotNone(err)
        self.assertEqual(err["error_codigo"], "ZZ_SALDO_CONSUMIDO")
        self.assertEqual(err["permitido"]["servicio"], 80000)
        self.assertEqual(err["excesos"][0]["saldo"], 80000)
        self.assertEqual(err["acciones"][0]["tipo"], "tomar_saldo")
        self.assertEqual(err["acciones"][0]["monto"], 80000)
        self.assertIsNone(self.m.chequear([FACT], {"servicio": 80000}, excluir=2))   # tomando solo el saldo, cabe

    def test_con_saldo_cero_rechazo_con_accion(self):
        self.m.aporta(1, FACT, servicio=200000)
        err = self.m.chequear([FACT], {"servicio": 200000}, excluir=2)
        self.assertEqual(err["error_codigo"], "ZZ_SALDO_CONSUMIDO")
        self.assertEqual(err["permitido"]["servicio"], 0)
        tipos = [a["tipo"] for a in err["acciones"]]
        self.assertNotIn("tomar_saldo", tipos)               # no hay nada que tomar...
        self.assertEqual(tipos, ["ligar_factura", "pasar_garantia", "pedir_autorizacion"])   # ...pero sí cuatro salidas menos una
        self.assertEqual(err["accion"]["tipo"], "resolver_saldo")
        self.assertIn("OT-2026-00001", err["error"])         # quién la usa
        self.assertIn("Gimnasio A", err["error"])
        self.assertTrue(err["puede_pedir_autorizacion"])

    def test_la_propia_ot_no_se_descuenta_a_si_misma(self):
        self.m.aporta(1, FACT, servicio=200000)
        self.assertIsNone(self.m.chequear([FACT], {"servicio": 200000}, excluir=1))

    def test_una_ot_cancelada_o_anulada_libera_el_saldo(self):
        self.m.aporta(1, FACT, servicio=200000)
        self.m.ots[1]["estado"] = "cancelada"
        self.assertIsNone(self.m.chequear([FACT], {"servicio": 200000}, excluir=2))
        self.m.ots[1]["estado"] = "anulada"
        self.assertIsNone(self.m.chequear([FACT], {"servicio": 200000}, excluir=2))

    def test_el_despacho_es_otra_linea(self):
        self.m.aporta(1, FACT, servicio=200000)
        self.assertIsNone(self.m.chequear([FACT], {"despacho": 30000}, excluir=2))
        self.m.aporta(1, FACT, servicio=200000, despacho=30000)
        err = self.m.chequear([FACT], {"servicio": None, "despacho": 30000}, excluir=2)
        self.assertEqual(err["excesos"][0]["categoria"], "despacho")

    def test_una_autorizacion_aprobada_permite_hasta_lo_autorizado(self):
        self.m.aporta(1, FACT, servicio=200000)
        self.m.autorizado = {"servicio": 200000, "despacho": 0}
        self.assertIsNone(self.m.chequear([FACT], {"servicio": 200000}, excluir=2))
        self.assertIsNotNone(self.m.chequear([FACT], {"servicio": 250000}, excluir=2))

    def test_varias_facturas_suman_sus_saldos(self):
        self.m.doc("FCV", "11440", zz=INSTALACION)
        self.m.aporta(1, FACT, servicio=150000)
        self.assertIsNone(self.m.chequear([FACT, ("FCV", "11440")], {"servicio": 250000}, excluir=2))   # 50.000 + 200.000
        self.assertIsNotNone(self.m.chequear([FACT, ("FCV", "11440")], {"servicio": 250001}, excluir=2))


class TestCasosBorde(unittest.TestCase):
    def setUp(self):
        self.m = Mundo()

    def test_sin_lineas_de_servicio_no_aplica(self):
        self.m.doc("FCV", "500", zz=[], total=900000)
        self.m.ot(1, "OT-1")
        self.m.aporta(1, ("FCV", "500"), servicio=100)
        self.assertIsNone(self.m.chequear([("FCV", "500")], {"servicio": 500000}, excluir=2))

    def test_erp_caido_no_bloquea(self):
        self.assertIsNone(self.m.chequear([("FCV", "999")], {"servicio": 500000}))

    def test_documento_de_otro_cliente_no_cuenta(self):
        self.m.doc("FCV", "600", zz=INSTALACION, rut="22.222.222-2")
        self.assertIsNone(self.m.chequear([("FCV", "600")], {"servicio": 999999}, rut="11.111.111-1"))

    def test_pedido_vacio_no_pide_nada(self):
        self.m.doc("FCV", "700", zz=INSTALACION)
        self.assertIsNone(self.m.chequear([("FCV", "700")], {"servicio": None, "despacho": None}))


class TestNotaDeVentaYFactura(unittest.TestCase):
    """Daniel: la nota de venta dada de baja por su factura no cuenta doble: la factura hereda lo que usó la nota."""

    def setUp(self):
        self.m = Mundo()
        self.NV, self.F = ("VD", "10667"), ("FCV", "11500")
        self.m.doc("NVV", "VD00010667", zz=INSTALACION)
        self.m.doc(*self.F, zz=INSTALACION)
        self.m.hijo_de(self.F, self.NV)
        self.m.ot(1, "OT-2026-00001")
        self.m.ot(2, "OT-2026-00002")

    def test_lo_que_uso_la_nota_lo_hereda_la_factura(self):
        self.m.aporta(1, self.NV, servicio=200000)                      # la OT 1 empezó con la nota de venta
        err = self.m.chequear([self.F], {"servicio": 200000}, excluir=2)  # la OT 2 quiere cobrar la factura que salió de esa nota
        self.assertEqual(err["error_codigo"], "ZZ_SALDO_CONSUMIDO")
        self.assertEqual(err["permitido"]["servicio"], 0)

    def test_la_nota_dada_de_baja_y_su_factura_cuentan_una_sola_vez(self):
        self.m.aporta(1, self.NV, servicio=200000)
        self.m.aporta(1, self.F, servicio=200000)    # la OT 1 ya tiene las dos (nota y factura): mismo cobro
        s = self.m.amb["_ot_saldo_doc"](*self.F, excluir_vid=2)
        self.assertEqual(s["servicio"]["usado"], 200000)   # no 400.000
        self.assertEqual(s["servicio"]["saldo"], 0)
        self.assertEqual(len(s["usos"]), 1)

    def test_si_la_ot_ya_hizo_el_cambio_solo_cuenta_la_factura(self):
        self.m.aporta(1, self.NV, servicio=None, despacho=None)   # la nota quedó dada de baja: sin aporte
        self.m.aporta(1, self.F, servicio=200000)
        s = self.m.amb["_ot_saldo_doc"](*self.NV, excluir_vid=2)
        self.assertEqual(s["servicio"]["usado"], 200000)

    def test_en_la_misma_ot_la_nota_y_su_factura_no_duplican_la_capacidad(self):
        r = self.m.amb["_ot_saldo_de_docs"]([self.NV, self.F], excluir_vid=1)
        self.assertEqual(r["combinado"]["servicio"]["total"], 200000)   # no 400.000
        self.assertIsNone(self.m.chequear([self.NV, self.F], {"servicio": 200000}, excluir=1))
        self.assertIsNotNone(self.m.chequear([self.NV, self.F], {"servicio": 200001}, excluir=1))


class TestValidadorDeCrear(unittest.TestCase):
    """_ot_validar_normalizar_finanzas (lo comparten el asistente OT 2.0, el núcleo clásico y el levantamiento)."""
    DOC = {"centro_costo": "sstt", "factura_tido": "FCV", "factura_nudo": "11439", "zz_monto": 200000,
           "valor_origen": "zz", "costo_proveedor": 0}
    TOPES = {"excluidos": [], "tope_servicio": 10 ** 9, "tope_despacho": 10 ** 9,
             "documentos": [{"tido": "FCV", "nudo": "11439", "servicio": 200000, "despacho": 30000}]}

    def _validar(self, chequear, fin=None):
        from tests import test_ot_finanzas_creacion as C
        amb = C._amb()
        amb["_saldo"] = S
        amb["_ot_zz_topes_reales"] = lambda *a, **k: self.TOPES
        amb["_ot_saldo_chequear"] = chequear
        amb["_ot_saldo_autorizado_hasta"] = lambda *a, **k: {}
        return amb["_ot_validar_normalizar_finanzas"](fin or dict(self.DOC), "instalacion", False)

    def test_con_saldo_pasa_y_anota_lo_que_aporta_cada_documento(self):
        err, c = self._validar(lambda *a, **k: None)
        self.assertIsNone(err)
        self.assertEqual(c["docs_aporte"], [{"tido": "FCV", "nudo": "11439", "servicio": 200000, "despacho": 0}])

    def test_sin_saldo_el_asistente_recibe_el_rechazo_con_sus_salidas(self):
        falso = {"ok": False, "error": "Ese cobro supera el saldo del documento", "error_codigo": "ZZ_SALDO_CONSUMIDO",
                 "codigo": "ZZ_SALDO_CONSUMIDO", "acciones": [{"tipo": "ligar_factura", "label": "x"}],
                 "excesos": [{"categoria": "servicio", "pedido": 200000, "saldo": 0, "exceso": 200000}]}
        err, c = self._validar(lambda *a, **k: dict(falso))
        self.assertIsNone(c)
        self.assertEqual(err["error_codigo"], "ZZ_SALDO_CONSUMIDO")
        self.assertEqual(err["http"], 409)
        self.assertEqual(err["extra"]["acciones"][0]["tipo"], "ligar_factura")   # llega al asistente: no se bloquea sin salida
        self.assertNotIn("error", err["extra"])

    def test_el_validador_pide_el_saldo_con_lo_que_se_quiere_cobrar(self):
        visto = {}

        def espia(docs, pedido, **k):
            visto["docs"], visto["pedido"], visto["kw"] = docs, pedido, k
            return None
        self._validar(espia, dict(self.DOC, zz_envio_monto=30000, zz_envio_codigo="ZZENVIO"))
        self.assertEqual(visto["docs"], [("FCV", "11439")])
        self.assertEqual(visto["pedido"], {"servicio": 200000, "despacho": 30000})
        self.assertIsNone(visto["kw"]["excluir_vid"])        # OT nueva: nadie se descuenta


class TestDeclararElCobro(unittest.TestCase):
    """POST /ot/api/finanzas/<vid> corrido de verdad (sin base) con el candado rechazando o dejando pasar."""
    ERR = {"ok": False, "error": "Ese cobro supera el saldo del documento", "error_codigo": "ZZ_SALDO_CONSUMIDO",
           "codigo": "ZZ_SALDO_CONSUMIDO", "permitido": {"servicio": 80000},
           "excesos": [{"categoria": "servicio", "pedido": 200000, "saldo": 80000, "exceso": 120000}],
           "acciones": [{"tipo": "tomar_saldo", "label": "Tomar solo el saldo ($80.000)", "monto": 80000},
                        {"tipo": "ligar_factura", "label": "x"}, {"tipo": "pasar_garantia", "label": "y"},
                        {"tipo": "pedir_autorizacion", "label": "z"}]}

    def _llamar(self, cuerpo, saldo_err=None):
        from tests.test_ot_finanzas_pantalla import _llamar_vivo
        return _llamar_vivo(cuerpo, fila={"costo": None, "zz_monto": None}, saldo_err=saldo_err)

    def test_si_excede_el_saldo_rechaza_con_acciones_y_no_escribe(self):
        r, updates, _fila, _ = self._llamar({"zz_monto": 200000, "valor_origen": "zz"}, saldo_err=self.ERR)
        self.assertEqual(r["error_codigo"], "ZZ_SALDO_CONSUMIDO")
        self.assertEqual(r["acciones"][0]["tipo"], "tomar_saldo")
        self.assertEqual(r["visita_id"], 249)
        self.assertEqual(updates, [], "un rechazo no escribe nada en la OT")

    def test_si_cabe_guarda_y_reparte_el_aporte_entre_sus_documentos(self):
        r, updates, fila, _ = self._llamar({"zz_monto": 80000, "valor_origen": "zz"})
        self.assertTrue(updates)
        self.assertEqual(fila["zz_monto"], 80000)

    def test_tomar_solo_el_saldo_baja_el_monto_y_sigue(self):
        r, updates, fila, _ = self._llamar({"zz_monto": 200000, "valor_origen": "zz", "tomar_saldo": True}, saldo_err=self.ERR)
        self.assertNotEqual(r.get("error_codigo"), "ZZ_SALDO_CONSUMIDO")
        self.assertEqual(fila["zz_monto"], 80000)    # el monto que quedó guardado es el saldo, no lo pedido

    def test_guardar_lo_mismo_no_se_rechaza(self):
        # una OT que ya cobraba 200.000 y se guarda por otra cosa (el costo del técnico) no vuelve a pasar por el candado
        from tests.test_ot_finanzas_pantalla import _llamar_vivo
        r, updates, _f, _ = _llamar_vivo({"zz_monto": 200000, "costo_proveedor": 50000}, fila={"zz_monto": 200000},
                                         saldo_err=self.ERR)
        self.assertNotEqual(r.get("error_codigo") if isinstance(r, dict) else None, "ZZ_SALDO_CONSUMIDO")
        self.assertTrue(updates)


# ───────────────────────── cableado en app.py, el motor, el asistente y el modal de cierre ─────────────────────────
def _fuente(nombre):
    codigo, arbol = _arbol()
    for n in arbol.body:
        if isinstance(n, ast.FunctionDef) and n.name == nombre:
            return ast.get_source_segment(codigo, n)
    raise AssertionError("no está " + nombre)


def _decoradores(nombre):
    codigo, arbol = _arbol()
    for n in arbol.body:
        if isinstance(n, ast.FunctionDef) and n.name == nombre:
            return [ast.unparse(d) for d in n.decorator_list]
    raise AssertionError("no está " + nombre)


class TestCableado(unittest.TestCase):
    def test_crear_pasa_por_el_candado_y_anota_el_aporte_por_documento(self):
        v = _fuente("_ot_validar_normalizar_finanzas")
        self.assertIn("_ot_saldo_chequear(", v)
        self.assertIn("_ot_saldo_autorizado_hasta(aut_id=_fin_aut_id, cliente_rut=cliente_rut)", v)
        self.assertIn('"docs_aporte": _fin_docs_aporte', v)
        self.assertIn("http=409", v)
        app = _leer("app.py")
        # los tres núcleos de creación anotan lo que cada documento aporta
        self.assertGreaterEqual(app.count("_ot_saldo_aporte_en("), 4)   # definición + 3 INSERT
        self.assertEqual(len(re.findall(r"zz_serv_monto, zz_envio_monto\) \"\s*\n\s*\"VALUES \(%s,'erp',1,%s,%s,%s,%s,%s,%s,%s,%s\)", app)), 3)
        # el rechazo con acciones llega a la pantalla en los tres núcleos
        self.assertEqual(app.count('_fin_err.get("extra")'), 3)

    def test_declarar_el_cobro_pasa_por_el_candado(self):
        f = _fuente("ot2_api_finanzas")
        self.assertIn("_ot_saldo_chequear(", f)
        self.assertIn('d.get("tomar_saldo")', f)
        self.assertIn("_ot_saldo_error_json(_sal_err, vid)", f)
        self.assertIn("_ot_saldo_reanotar_aportes(vid)", f)
        # solo cuando la petición CAMBIA el cobro: guardar lo mismo no se rechaza
        self.assertIn("(_zz_cambia and zz_monto is not None) or _sal_env_cambia", f)

    def test_ligar_documento_y_regularizar_pasan_por_el_candado(self):
        d = _fuente("ot2_api_documentos_agregar")
        self.assertIn("_ot_saldo_chequear(", d)
        self.assertIn('d.get("tomar_saldo")', d)
        self.assertIn("_tomo_saldo", d)
        # regularizar reutiliza el mismo camino
        reg = _fuente("ot_api_documentos_regularizar")
        self.assertIn("ot2_api_documentos_agregar", reg)

    def test_asociar_factura_pasa_por_el_candado(self):
        a = _fuente("mant_ot_asociar_factura")
        self.assertIn("_ot_saldo_chequear(", a)
        self.assertIn("_ot_saldo_tomar_core(vid)", a)
        self.assertIn('"saldo": _sal_prev', a)

    def test_el_modal_de_cierre_ofrece_salidas_y_no_toca_estado(self):
        c = _fuente("mant_ot_aprobar_cierre")
        self.assertIn("_ot_saldo_chequear(", c)
        self.assertIn('_ot_cierre_accion("ZZ_SALDO_CONSUMIDO", vid)', c)
        i = c.index("_ot_saldo_chequear(")
        # el candado corre ANTES del UPDATE que cierra la OT
        self.assertLess(i, c.index("UPDATE mant_visitas SET \"\n            \"  estado='cerrada'"))
        app = _leer("app.py")
        acc = re.search(r"_OT_CIERRE_ACCIONES = \{(.*?)\n\}", app, re.S).group(1)
        self.assertIn('"ZZ_SALDO_CONSUMIDO"', acc)
        self.assertIn("resolver_saldo", acc)

    def test_las_salidas_funcionan_en_el_estado_en_que_la_ot_llega_al_modal(self):
        t = _decoradores("ot_api_saldo_servicio_tomar")
        self.assertIn("_ot_can_finanzas_cierre", t)
        cuerpo = _fuente("ot_api_saldo_servicio_tomar")
        self.assertIn("estado NOT IN ('cerrada','cancelada','anulada')", cuerpo)   # evidencia: nunca una OT cerrada
        for prohibido in ("firma_", "cerrada_at", "estado='cerrada'", "SET estado"):
            self.assertNotIn(prohibido, cuerpo)

    def test_las_lecturas_son_solo_lectura_y_sin_tecnicos(self):
        for ruta in ("ot_api_saldo_servicio_doc", "ot_api_saldo_servicio_ot", "ot_api_panorama"):
            self.assertIn("_es_rol_tecnico()", _fuente(ruta), ruta)       # REGLA #19: los técnicos no ven montos
        for ruta in ("ot_api_saldo_servicio_doc", "ot_api_saldo_servicio_ot"):
            self.assertNotIn("mysql_execute", _fuente(ruta), ruta)        # lecturas puras
        # el ERP solo se toca con SELECT (REGLA #4.1) y por el helper permitido
        fam = _fuente("_ot_saldo_familia")
        self.assertIn("_random_sql_query(", fam)
        for palabra in ("INSERT", "UPDATE", "DELETE", "EXEC"):
            self.assertNotIn(palabra, fam)

    def test_el_panel_del_motor_cuenta_documentos_servicio_despacho_y_uso(self):
        p = _fuente("ot_api_panorama")
        for clave in ("docs_con_servicio", "docs_con_despacho", "docs_usados_en_otras", "docs_saldo_agotado", '"saldo": saldo_ot'):
            self.assertIn(clave, p)
        self.assertIn("_saldo.marcar_omitidas(infos_s)", p)

    def test_avisos_en_regularizar_y_facturacion_de_proveedor(self):
        self.assertIn("saldo_avisos", _fuente("ot_api_regularizar"))
        self.assertIn("_ot_saldo_compartidos_lote", _fuente("ot_api_regularizar"))
        fp = _fuente("_facprov_datos")
        self.assertIn("_ot_saldo_compartidos_lote", fp)
        self.assertIn('"saldo_avisos"', fp)
        self.assertIn("saldo_avisos", _leer("templates/mantenciones/facturacion_proveedores.html"))
        self.assertIn("saldo_avisos", _leer("static/ot_regularizar.js"))

    def test_autorizacion_exceder_saldo(self):
        app = _leer("app.py")
        self.assertIn('_OT_AUT_TIPOS = ("crear_sin_documento", "cobro_cero", "cerrar_sin_documento", "exceder_saldo")', app)
        self.assertGreaterEqual(app.count("'exceder_saldo'"), 2)           # CREATE TABLE y MODIFY del ENUM
        crear = _fuente("ot_aut_api_crear")
        self.assertIn('"exceder_saldo"', crear)
        self.assertIn("saldo_pedido", crear)
        aprobar = _fuente("ot_aut_api_aprobar")
        self.assertIn('tipo == "exceder_saldo"', aprobar)
        self.assertIn("saldo_servicio_autorizado", aprobar)
        # una autorización de saldo no vale como autorización de crear/cerrar/$0 sin documento
        puerta = _fuente("_ot_puerta_documento")
        self.assertNotIn("exceder_saldo", puerta)

    def test_la_lectura_de_autorizaciones_se_ata_a_la_ot_o_a_la_creacion(self):
        f = _fuente("_ot_saldo_autorizado_hasta")
        self.assertIn("tipo='exceder_saldo' AND estado='aprobada'", f)
        self.assertIn("visita_creada_id", f)    # la que ya creó una OT no se reutiliza

    def test_sql_del_saldo_va_parametrizado(self):
        f = _fuente("_ot_saldo_usos")
        self.assertNotRegex(f, r"f\"[^\"]*(SELECT|WHERE)")
        self.assertIn("estado NOT IN ('cancelada','anulada')", f)
        self.assertIn("reemplazado_por_id IS NULL", f)


class TestErpSoloLectura(unittest.TestCase):
    """REGLA #4.1: el ERP Random es de SOLO LECTURA. Las consultas del saldo pasan el validador del helper permitido."""

    def test_las_consultas_a_random_pasan_el_validador_de_solo_select(self):
        _codigo, arbol = _arbol()
        amb = {}
        for n in arbol.body:
            if isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == "_RANDOM_FORBIDDEN_TOKENS" for t in n.targets):
                exec(compile(ast.Module(body=[n], type_ignores=[]), "<app>", "exec"), amb)
            if isinstance(n, ast.FunctionDef) and n.name == "_random_sql_validate":
                exec(compile(ast.Module(body=[n], type_ignores=[]), "<app>", "exec"), amb)
        self.assertIn("_random_sql_validate", amb)
        familia = next(n for n in arbol.body if isinstance(n, ast.FunctionDef) and n.name == "_ot_saldo_familia")
        sqls = [c.value for c in ast.walk(familia) if isinstance(c, ast.Constant) and isinstance(c.value, str)
                and c.value.lstrip().upper().startswith("SELECT")]
        self.assertEqual(len(sqls), 2)
        for sql in sqls:
            amb["_random_sql_validate"](sql)      # lanza PermissionError si tocara algo prohibido
            self.assertIn("MAEDDO", sql)


class TestPantallas(unittest.TestCase):
    def test_el_motor_muestra_el_saldo_y_ofrece_las_cuatro_salidas(self):
        js = _leer("static/ot_fin_motor.js")
        for clave in ("function htmlSaldoDoc(d)", "function resolverSaldo(inst, err, reintento)", "function pedirExceder(inst, err)",
                      "docs_con_servicio", "docs_con_despacho", "docs_usados_en_otras", "docs_saldo_agotado", "resolver_saldo: 'resolverSaldo'",
                      "tomar_saldo", "'/ot/api/' + inst.vid + '/saldo-servicio/tomar'", "tipo: 'exceder_saldo'", "resolverSaldoExterno"):
            self.assertIn(clave, js)
        # las cuatro salidas
        for tipo in ("tomar_saldo", "ligar_factura", "pasar_garantia", "pedir_autorizacion"):
            self.assertIn(tipo, js)
        # ligar documento y declarar el cobro reciben el rechazo y lo resuelven en el mismo lugar
        self.assertEqual(js.count("ZZ_SALDO_CONSUMIDO"), 2)
        css = _leer("static/ot_fin_motor.css")
        for clave in (".fm-saldo", ".fm-saldo-alerta", ".fm-acc"):
            self.assertIn(clave, css)

    def test_el_asistente_de_crear_muestra_el_saldo_y_no_se_bloquea_entero(self):
        h = _leer("templates/ot2/_modal_crear.html")
        for clave in ('id="o2mFinSaldo"', "function _o2mFinCargarSaldo()", "/ot/api/saldo-servicio/", "function saldoAccion(que)",
                      "Tomar solo el saldo", "Pedir autorización a Daniel", "Ligar otra factura", "Pasar a garantía",
                      "tipo: 'exceder_saldo'", "ZZ_SALDO_CONSUMIDO", "saldoAccion:saldoAccion"):
            self.assertIn(clave, h)
        # «tomar el saldo» no pide motivo: el número sigue saliendo del documento
        self.assertIn("setFinZZ(S.fin_zz_codigo, s.servicio.saldo)", h)

    def test_la_ficha_y_el_modal_de_cierre_resuelven_el_rechazo(self):
        d = _leer("templates/ot2/detalle.html")
        self.assertGreaterEqual(d.count("resolverSaldoExterno"), 3)    # tarjeta Finanzas, Otros documentos, Asociar factura
        self.assertIn("_factSaldoHtml(d.saldo)", d)
        self.assertIn("Saldo de las líneas del documento", d)

    def test_nada_visible_para_el_cliente(self):
        # el saldo es interno: no entra a los correos ni al seguimiento público
        for rel in ("retiros_check.py", "pickups_module.py", "retiros_encuesta.py"):
            self.assertNotIn("ot_saldo_servicio", _leer(rel), rel)


if __name__ == "__main__":
    unittest.main()

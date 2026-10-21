# -*- coding: utf-8 -*-
"""🧾 El costo de la OT y el anexo del proveedor son UN solo valor (REGLA #34).

Caso real (Daniel, 20-oct-2026): la OT 239 tenía el Anexo 221 firmado por $370.000 y la ficha decía «costo no declarado»;
antes de esto el anexo y el costo de la OT solo se comparaban al cerrar y se podían declarar por separado.
Todo con datos de mentira (nunca contra producción).
"""
import json
import types
import unittest

import ot_anexo_costo as O
from tests._fuente_app import cargar, decoradores, fuente_funcion, texto


# ───────────────────────── lógica pura ─────────────────────────
class TestItemsYTotales(unittest.TestCase):
    def test_lee_el_json_del_anexo(self):
        its = O.items_de(json.dumps([{"concepto": "Instalación", "monto": 350000}, {"concepto": "Despacho", "monto": 20000}]))
        self.assertEqual(O.total(its), 370000)
        self.assertEqual(O.partir(its), (350000, 20000))

    def test_lo_ilegible_se_ignora(self):
        self.assertEqual(O.items_de("no es json"), [])
        self.assertEqual(O.items_de(None), [])
        self.assertEqual(O.items_de([1, "x", {"concepto": "A", "monto": "12.5"}]), [{"concepto": "A", "monto": 12}])

    def test_despacho_con_o_sin_tilde_y_mayusculas(self):
        for c in ("Despacho", "despacho", "DESPACHO a regiones", "  Despacho"):
            self.assertTrue(O.es_despacho(c), c)
        self.assertFalse(O.es_despacho("Instalación"))

    def test_anexo_vivo(self):
        for e in ("borrador", "enviado", "visto", "vencido", "firmado", None, ""):
            self.assertTrue(O.es_vivo(e), e)
        for e in ("rechazado", "anulado", "ANULADO"):
            self.assertFalse(O.es_vivo(e), e)

    def test_clp(self):
        self.assertEqual(O.clp(370000), "$370.000")
        self.assertEqual(O.clp(0), "$0")
        self.assertEqual(O.clp(None), "$0")


class TestAlinear(unittest.TestCase):
    def test_caso_ot_239_costo_sin_declarar(self):
        r = O.alinear(None, None, [{"concepto": "Mantención", "monto": 370000}])
        self.assertEqual((r["prov"], r["desp"], r["total"]), (370000.0, None, 370000))
        self.assertTrue(r["cambia"])
        self.assertIsNone(r["conflicto"])

    def test_el_item_despacho_manda_sobre_el_despacho_de_la_ot(self):
        r = O.alinear(100, 5, [{"concepto": "Instalación", "monto": 350000}, {"concepto": "Despacho", "monto": 20000}])
        self.assertEqual((r["prov"], r["desp"]), (350000.0, 20000.0))

    def test_sin_item_despacho_se_respeta_el_despacho_declarado(self):
        r = O.alinear(None, 20000, [{"concepto": "Servicio", "monto": 370000}])
        self.assertEqual((r["prov"], r["desp"]), (350000.0, 20000.0))

    def test_un_anexo_menor_al_despacho_es_conflicto_y_no_escribe(self):
        r = O.alinear(None, 50000, [{"concepto": "Servicio", "monto": 10000}])
        self.assertIn("menor que el despacho", r["conflicto"])
        self.assertFalse(r["cambia"])
        self.assertIsNone(r["prov"])

    def test_ya_alineado_no_cambia(self):
        r = O.alinear(370000, None, [{"concepto": "Servicio", "monto": 370000}])
        self.assertFalse(r["cambia"])
        r = O.alinear(350000, 20000, [{"concepto": "Servicio", "monto": 370000}])
        self.assertFalse(r["cambia"])

    def test_un_cero_declarado_es_un_dato(self):
        # anexo en garantía por $0: costo 0 declarado queda alineado; sin declarar se completa con 0
        self.assertFalse(O.alinear(0, None, [{"concepto": "Garantía", "monto": 0}])["cambia"])
        r = O.alinear(None, None, [{"concepto": "Garantía", "monto": 0}])
        self.assertTrue(r["cambia"])
        self.assertEqual(r["prov"], 0.0)


class TestEstadoYPermiso(unittest.TestCase):
    ITEMS = [{"concepto": "Servicio", "monto": 370000}]

    def test_estados(self):
        self.assertEqual(O.estado(None, None, self.ITEMS), "sin_declarar")
        self.assertEqual(O.estado(370000, None, self.ITEMS), "alineado")
        self.assertEqual(O.estado(350000, 20000, self.ITEMS), "alineado")
        self.assertEqual(O.estado(370000.5, None, self.ITEMS), "alineado")        # holgura de $1
        self.assertEqual(O.estado(65000, None, self.ITEMS), "difiere")

    def test_volver_a_guardar_lo_mismo_no_se_bloquea(self):
        # la tarjeta de Finanzas manda todos sus campos juntos, aunque la OT todavía difiera del anexo
        self.assertTrue(O.cambio_permitido(65000, None, 65000, None, 370000))

    def test_cambio_que_suma_lo_del_anexo(self):
        self.assertTrue(O.cambio_permitido(370000, None, None, None, 370000))        # declararlo igual al anexo
        self.assertTrue(O.cambio_permitido(350000, 20000, 370000, None, 370000))     # partir servicio y despacho
        self.assertTrue(O.cambio_permitido(None, 0, 370000, None, 370000))           # despacho en 0 conservando el total

    def test_cambio_que_no_suma_lo_del_anexo_se_rechaza(self):
        self.assertFalse(O.cambio_permitido(65000, None, None, None, 370000))
        self.assertFalse(O.cambio_permitido(None, 20000, 370000, None, 370000))      # sumaría 390.000
        self.assertFalse(O.cambio_permitido(0, None, 370000, 0, 370000))

    def test_mensaje_dice_de_donde_sale_y_que_hacer(self):
        m = O.mensaje_costo_es_del_anexo(221, "firmado", 370000)
        self.assertIn("UN solo valor", m)
        self.assertIn("Anexo N° 221", m)
        self.assertIn("$370.000", m)
        self.assertIn("emite un anexo nuevo", m)
        m2 = O.mensaje_costo_es_del_anexo(221, "enviado", 370000)
        self.assertIn("corrige el monto en el anexo", m2)


# ───────────────────────── comportamiento (base de mentira) ─────────────────────────
class TestAlinearCosto(unittest.TestCase):
    def _amb(self, fila_ot, anexo, rowcount=1):
        self.sql, self.logs, self.lote = [], [], []
        previo = {441: {"monto": 65000.0, "sugerido": 65000.0}}
        amb = {"_oac": O,
               "_ot_costo_fila": lambda vid: fila_ot,
               "_ot_anexo_vivo": lambda vid: anexo,
               "_mfp_previo_de": lambda vid: previo,
               "_mfp_seguir_costo": lambda p, quien=None, motivo="": self.lote.append((p, quien, motivo)),
               "mysql_execute_returning_rowcount": lambda sql, p=None: (self.sql.append((sql, p)) or rowcount),
               "_mant_log": lambda ent, eid, acc, det: self.logs.append((ent, eid, acc, det)),
               "current_username": lambda: "daniel",
               "print": lambda *a, **k: None}
        return cargar(funciones=("_ot_anexo_alinear_costo",), extra=amb)["_ot_anexo_alinear_costo"]

    def _anexo(self, total=370000, estado="firmado", items=None):
        its = items or [{"concepto": "Mantención", "monto": total}]
        return {"id": 9, "numero": 221, "estado": estado, "proveedor": "DAP", "items": its, "total": O.total(its)}

    def _ot(self, **kw):
        f = {"id": 441, "numero_ot": "OT-2026-00239", "estado": "creada", "costo_proveedor": None, "costo_despacho": None}
        f.update(kw)
        return f

    def test_caso_ot_239_completa_el_costo_con_el_anexo_firmado(self):
        r = self._amb(self._ot(), self._anexo())(441, origen="al abrir la OT")
        self.assertTrue(r["aplicado"])
        sql, p = self.sql[0]
        self.assertIn("UPDATE mant_visitas SET costo_proveedor=%s", sql)
        self.assertIn("NOT IN ('cerrada','cancelada','anulada')", sql)          # una OT cerrada es evidencia
        self.assertEqual(p, (370000.0, 441))
        self.assertEqual(self.logs[0][:3], ("visita", 441, "costo_alineado_anexo"))
        self.assertIn("Anexo N° 221 (firmado, $370.000)", self.logs[0][3])
        self.assertIn("sin declarar → $370.000", self.logs[0][3])
        self.assertEqual(len(self.lote), 1)                                       # el lote pendiente sigue el costo (REGLA #29)

    def test_solo_si_falta_no_pisa_un_costo_declarado_distinto(self):
        r = self._amb(self._ot(costo_proveedor=65000), self._anexo())(441, solo_si_falta=True)
        self.assertFalse(r.get("aplicado"))
        self.assertEqual(self.sql, [])

    def test_a_pedido_si_alinea_un_costo_distinto(self):
        r = self._amb(self._ot(costo_proveedor=65000), self._anexo())(441, origen="a pedido")
        self.assertTrue(r["aplicado"])
        self.assertEqual(self.sql[0][1], (370000.0, 441))
        self.assertIn("$65.000 → $370.000", self.logs[0][3])

    def test_ot_cerrada_no_se_toca(self):
        for est in ("cerrada", "cancelada", "anulada"):
            self.assertIsNone(self._amb(self._ot(estado=est), self._anexo())(441))
        self.assertEqual(self.sql, [])

    def test_sin_anexo_vivo_no_hace_nada(self):
        self.assertIsNone(self._amb(self._ot(), None)(441))
        self.assertEqual(self.sql, [])

    def test_ya_alineado_no_escribe(self):
        r = self._amb(self._ot(costo_proveedor=370000), self._anexo())(441)
        self.assertFalse(r["cambia"])
        self.assertEqual(self.sql, [])

    def test_conflicto_no_escribe(self):
        r = self._amb(self._ot(costo_despacho=500000), self._anexo(total=10000))(441)
        self.assertIsNotNone(r["conflicto"])
        self.assertEqual(self.sql, [])

    def test_con_item_despacho_actualiza_las_dos_columnas(self):
        its = [{"concepto": "Instalación", "monto": 350000}, {"concepto": "Despacho", "monto": 20000}]
        self._amb(self._ot(), self._anexo(items=its))(441)
        sql, p = self.sql[0]
        self.assertIn("costo_despacho=%s", sql)
        self.assertEqual(p, (350000.0, 20000.0, 441))

    def test_un_fallo_jamas_rompe_la_peticion(self):
        def malo(vid): raise RuntimeError("base caída")
        amb = {"_oac": O, "_ot_costo_fila": malo, "print": lambda *a, **k: None}
        f = cargar(funciones=("_ot_anexo_alinear_costo",), extra=amb)["_ot_anexo_alinear_costo"]
        self.assertIsNone(f(441))


class TestGuardia(unittest.TestCase):
    def _g(self, anexo, fila):
        jsonify = lambda d: ("JSON", d)
        amb = {"_oac": O, "jsonify": jsonify, "_ot_anexo_vivo": lambda vid: anexo, "_ot_costo_fila": lambda vid: fila}
        return cargar(funciones=("_ot_anexo_costo_guardia",), extra=amb)["_ot_anexo_costo_guardia"]

    def test_sin_anexo_o_sin_cambio_pasa(self):
        self.assertIsNone(self._g(None, {})(441, 65000, None))
        self.assertIsNone(self._g({"id": 9, "numero": 221, "estado": "firmado", "total": 370000}, {})(441, None, None))

    def test_con_anexo_un_costo_distinto_se_rechaza_con_409(self):
        ax = {"id": 9, "numero": 221, "estado": "firmado", "total": 370000}
        resp, code = self._g(ax, {"costo_proveedor": None, "costo_despacho": None})(441, 65000, None)
        self.assertEqual(code, 409)
        self.assertEqual(resp[1]["error_codigo"], "COSTO_ES_DEL_ANEXO")
        self.assertIn("UN solo valor", resp[1]["error"])
        self.assertEqual(resp[1]["anexo"]["numero"], 221)
        self.assertIn("anexo_nuevo", resp[1]["acciones"])                       # firmado: se emite uno nuevo

    def test_anexo_no_firmado_ofrece_editarlo(self):
        ax = {"id": 9, "numero": 221, "estado": "enviado", "total": 370000}
        resp, code = self._g(ax, {"costo_proveedor": None, "costo_despacho": None})(441, 65000, None)
        self.assertIn("editar_anexo", resp[1]["acciones"])

    def test_costo_igual_al_anexo_pasa(self):
        ax = {"id": 9, "numero": 221, "estado": "firmado", "total": 370000}
        self.assertIsNone(self._g(ax, {"costo_proveedor": None, "costo_despacho": None})(441, 370000, None))


# ───────────────────────── cableado en app.py ─────────────────────────
class TestCableado(unittest.TestCase):
    def test_los_tres_caminos_que_escriben_el_costo_pasan_por_la_guardia(self):
        for nombre, antes_de in (("ot_api_costo_proveedor", "cambios = {k: nv"),
                                 ("ot2_api_finanzas", "# 🆕 FASE 2 (2026-09-11)"),
                                 ("mant_visita_update", "# 💰 2026-09-10 (Daniel, caso OT-2026-00157")):
            s = fuente_funcion(nombre)
            self.assertIn("_ot_anexo_costo_guardia(", s, nombre)
            self.assertLess(s.index("_ot_anexo_costo_guardia("), s.index(antes_de), nombre)

    def test_el_anexo_alinea_el_costo_al_crearse_corregirse_y_firmarse(self):
        self.assertIn("_ot_anexo_alinear_costo(vid,", fuente_funcion("ot2_api_anexo_crear"))
        self.assertIn("_ot_anexo_alinear_costo(", fuente_funcion("ot2_api_anexo_editar"))
        self.assertIn("_ot_anexo_alinear_costo(", fuente_funcion("ot2_anexo_firma_submit"))
        # el anexo que nace con la OT (asistente) también
        self.assertIn("_ot_anexo_alinear_costo(vid, origen=f\"se creó el Anexo N° {_num_auto} junto con la OT\")", texto())

    def test_la_ficha_completa_el_costo_sin_declarar_y_muestra_el_vinculo_solo_a_gestion(self):
        s = fuente_funcion("ot2_detalle")
        i_alinear = s.index("_ot_anexo_alinear_costo(vid, origen=\"al abrir la OT")
        self.assertIn("solo_si_falta=True", s[i_alinear:i_alinear + 160])
        self.assertIn("if not _es_rol_tecnico():", s[i_alinear - 80:i_alinear])
        self.assertLess(i_alinear, s.index('"SELECT v.*, c.razon_social'))            # antes de leer la fila
        self.assertIn("anexo_vinculo = None if _es_rol_tecnico() else _ot_anexo_vinculo_vista(v)", s)
        self.assertIn("anexo_vinculo=anexo_vinculo,", s)

    def test_aprobar_cierre_completa_el_costo_antes_de_sus_candados(self):
        s = fuente_funcion("mant_ot_aprobar_cierre")
        i = s.index("_ot_anexo_alinear_costo(vid,")
        self.assertIn("solo_si_falta=True", s[i:i + 160])
        self.assertLess(i, s.index("SIN_COSTO_PROVEEDOR"))

    def test_las_rutas_nuevas_excluyen_a_los_tecnicos(self):
        for nombre in ("ot_api_costo_desde_anexo", "ot_api_anexos_vinculo_costos"):
            decs = ",".join(decoradores(nombre))
            self.assertIn("_no_tecnico", decs, nombre)
        self.assertIn("_ot_can_finanzas_cierre", ",".join(decoradores("ot_api_costo_desde_anexo")))
        self.assertIn("_ot_aut_es_superadmin()", fuente_funcion("ot_api_anexos_vinculo_costos"))

    def test_el_vinculo_no_se_le_pasa_a_un_tecnico_ni_toca_ot_cerradas(self):
        self.assertIn("ESTADOS_OT_CERRADOS", fuente_funcion("_ot_anexo_alinear_costo"))
        self.assertIn("NOT IN ('cerrada','cancelada','anulada')", fuente_funcion("_ot_anexo_alinear_costo"))

    def test_la_ficha_muestra_la_franja_y_el_boton(self):
        with open("templates/ot2/detalle.html", encoding="utf-8") as fh:
            t = fh.read()
        self.assertIn('id="otdAnexoVinculo"', t)
        self.assertIn("/costo-desde-anexo", t)
        self.assertIn("Usar el valor del anexo", t)
        self.assertNotIn("alert(", t[t.index('id="otdAnexoVinculo"'):t.index('id="otdAnexoVinculo"') + 4000])


if __name__ == "__main__":
    unittest.main()

# -*- coding: utf-8 -*-
"""🧾 ANEXO ÚNICO (REGLA #46) — Daniel, 2026-10-25: «el anexo debe ser único… describiendo el detalle de cuánto se me va a cobrar por
instalación, desinstalación y despacho, algo bien claro para evitar problemas».

Una instalación con desinstalación y el MISMO proveedor externo comparten UN solo anexo: cada partida lleva la OT a la que pertenece
(`ot`: 'entrada' = instalación, 'salida' = desinstalación; sin etiqueta = entrada, así que los anexos de antes valen igual) y cada OT se
cuadra con SU parte (REGLA #34, un solo valor). Todo con datos y base de mentira: nunca contra producción.
"""
import json
import os
import re
import shutil
import subprocess
import tempfile
import unittest

import ot_anexo_costo as O
from tests._fuente_app import cargar, fuente_funcion, texto

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _leer(rel):
    with open(os.path.join(RAIZ, rel.replace("/", os.sep)), encoding="utf-8", newline="") as f:
        return f.read().replace("\r\n", "\n")


ITEMS = [{"concepto": "Instalación", "monto": 300000, "ot": "entrada"},
         {"concepto": "Despacho (instalación)", "monto": 20000, "ot": "entrada"},
         {"concepto": "Desinstalación", "monto": 150000, "ot": "salida"},
         {"concepto": "Despacho (desinstalación)", "monto": 10000, "ot": "salida"}]


# ───────────────────────── lógica pura ─────────────────────────
class TestPartidasPorOT(unittest.TestCase):
    def test_la_etiqueta_se_conserva_y_una_invalida_se_ignora(self):
        its = O.items_de(json.dumps([{"concepto": "A", "monto": 1, "ot": "salida"}, {"concepto": "B", "monto": 2, "ot": "Entrada"},
                                     {"concepto": "C", "monto": 3, "ot": "cualquiera"}, {"concepto": "D", "monto": 4}]))
        self.assertEqual([i.get("ot") for i in its], ["salida", "entrada", None, None])
        self.assertEqual([O.rol_de(i) for i in its], ["salida", "entrada", "entrada", "entrada"])

    def test_sin_etiquetas_todo_es_de_la_entrada_y_nada_cambia(self):
        its = [{"concepto": "Instalación", "monto": 350000}, {"concepto": "Despacho", "monto": 20000}]
        self.assertFalse(O.tiene_salida(its))
        self.assertEqual(O.total(its), O.total(its, "entrada"))
        self.assertEqual(O.total(its, "salida"), 0)
        self.assertEqual(O.partir(its), O.partir(its, "entrada"))

    def test_cada_ot_tiene_su_parte(self):
        self.assertTrue(O.tiene_salida(ITEMS))
        self.assertEqual((O.total(ITEMS), O.total(ITEMS, "entrada"), O.total(ITEMS, "salida")), (480000, 320000, 160000))
        self.assertEqual(O.partir(ITEMS, "entrada"), (300000, 20000))
        self.assertEqual(O.partir(ITEMS, "salida"), (150000, 10000))

    def test_alinear_cuadra_cada_ot_con_su_parte(self):
        e = O.alinear(None, None, ITEMS, "entrada")
        s = O.alinear(None, None, ITEMS, "salida")
        self.assertEqual((e["prov"], e["desp"], e["total"]), (300000.0, 20000.0, 320000))
        self.assertEqual((s["prov"], s["desp"], s["total"]), (150000.0, 10000.0, 160000))
        # sin rol (el anexo completo) NO es lo que le toca a ninguna de las dos
        self.assertEqual(O.alinear(None, None, ITEMS)["total"], 480000)

    def test_estado_por_parte(self):
        self.assertEqual(O.estado(300000, 20000, ITEMS, "entrada"), "alineado")
        self.assertEqual(O.estado(150000, 10000, ITEMS, "salida"), "alineado")
        self.assertEqual(O.estado(300000, 20000, ITEMS, "salida"), "difiere")          # la OT de salida con el costo de la de entrada
        self.assertEqual(O.estado(480000, None, ITEMS, "entrada"), "difiere")           # el anexo completo no es su parte
        self.assertEqual(O.estado(None, None, ITEMS, "salida"), "sin_declarar")

    def test_resumen_para_las_pantallas(self):
        r = O.resumen_partidas(ITEMS)
        self.assertTrue(r["compartido"])
        self.assertEqual(r["total"], 480000)
        self.assertEqual((r["entrada"]["total"], r["entrada"]["servicio"], r["entrada"]["despacho"]), (320000, 300000, 20000))
        self.assertEqual((r["salida"]["total"], r["salida"]["servicio"], r["salida"]["despacho"]), (160000, 150000, 10000))
        self.assertFalse(O.resumen_partidas([{"concepto": "Servicio", "monto": 5}])["compartido"])

    def test_etiquetar_marca_todas_como_de_una_ot(self):
        self.assertEqual([i["ot"] for i in O.etiquetar([{"concepto": "A", "monto": 1}], "salida")], ["salida"])
        self.assertEqual([i["ot"] for i in O.etiquetar([{"concepto": "A", "monto": 1}], "x")], ["entrada"])

    def test_el_mensaje_dice_cuanto_es_su_parte_y_cuanto_el_anexo(self):
        m = O.mensaje_costo_es_del_anexo(221, "firmado", 160000, 480000)
        self.assertIn("parte de esta OT", m)
        self.assertIn("$160.000 de $480.000", m)
        self.assertIn("emite un anexo nuevo", m)
        # sin anexo compartido el texto de siempre
        self.assertIn("del Anexo N° 221 (firmado, $370.000)", O.mensaje_costo_es_del_anexo(221, "firmado", 370000))
        self.assertIn("del Anexo N° 221 (firmado, $370.000)", O.mensaje_costo_es_del_anexo(221, "firmado", 370000, 370000))


# ───────────────────────── comportamiento (base de mentira) ─────────────────────────
class _BD:
    """mant_anexos + mant_visitas de mentira. `fetchone(sql, params)` responde según el SQL que la función de app.py arma."""

    def __init__(self, anexos, visitas=None):
        self.anexos, self.visitas = anexos, visitas or {}

    def fetchone(self, sql, params=()):
        if "FROM mant_visitas" in sql and "ot_madre_id, ot_relacion" in sql:
            return self.visitas.get(int(params[0]))
        if "FROM mant_anexos" in sql:
            vid = int(params[0])
            filas = [a for a in self.anexos if a["ot_id"] == vid]
            if "estado='firmado'" in sql:
                filas = [a for a in filas if a["estado"] == "firmado"]
            else:
                filas = [a for a in filas if a["estado"] not in ("rechazado", "anulado")]
            return dict(sorted(filas, key=lambda a: a["id"])[-1]) if filas else None
        return None


def _anexo(id_=9, ot=441, estado="enviado", items=None, numero=221):
    return {"id": id_, "numero": numero, "estado": estado, "proveedor_nombre": "DAP SpA", "ot_id": ot,
            "precio_items_json": json.dumps(items if items is not None else ITEMS)}


MADRE, HIJA = 441, 442
VISITAS = {HIJA: {"ot_madre_id": MADRE, "ot_relacion": "desinstalacion"}, MADRE: {"ot_madre_id": None, "ot_relacion": None}}


class TestElAnexoDeCadaOT(unittest.TestCase):
    def _f(self, anexos, visitas=VISITAS):
        bd = _BD(anexos, visitas)
        amb = cargar(funciones=("_ot_anexo_de_la_madre", "_ot_anexo_vivo"), constantes=("_ANEXO_SEL_VIVO",),
                     extra={"_oac": O, "mysql_fetchone": bd.fetchone, "print": lambda *a, **k: None})
        return amb["_ot_anexo_vivo"]

    def test_la_instalacion_ve_solo_su_parte_pero_conoce_el_anexo_completo(self):
        ax = self._f([_anexo()])(MADRE)
        self.assertEqual((ax["rol"], ax["compartido"], ax["total"], ax["total_general"]), ("entrada", True, 320000, 480000))
        self.assertEqual(O.total(ax["items"]), 320000)
        self.assertEqual(ax["ot_id"], MADRE)

    def test_la_desinstalacion_sin_anexo_propio_lee_el_de_su_instalacion(self):
        ax = self._f([_anexo()])(HIJA)
        self.assertEqual((ax["rol"], ax["compartido"], ax["total"], ax["total_general"]), ("salida", True, 160000, 480000))
        self.assertEqual(ax["ot_id"], MADRE)
        self.assertEqual(ax["numero"], 221)
        self.assertEqual([i["concepto"] for i in ax["items"]], ["Desinstalación", "Despacho (desinstalación)"])

    def test_un_anexo_viejo_sin_etiquetas_no_cubre_a_la_desinstalacion(self):
        viejo = _anexo(items=[{"concepto": "Instalación", "monto": 350000}])
        self.assertIsNone(self._f([viejo])(HIJA))
        ax = self._f([viejo])(MADRE)
        self.assertEqual((ax["rol"], ax["compartido"], ax["total"]), (None, False, 350000))

    def test_si_la_desinstalacion_tiene_su_propio_anexo_manda_el_suyo(self):
        propio = _anexo(id_=10, ot=HIJA, numero=222, items=[{"concepto": "Desinstalación", "monto": 90000}])
        ax = self._f([_anexo(), propio])(HIJA)
        self.assertEqual((ax["numero"], ax["rol"], ax["compartido"], ax["total"]), (222, None, False, 90000))

    def test_un_anexo_anulado_no_cubre_a_nadie(self):
        self.assertIsNone(self._f([_anexo(estado="anulado")])(HIJA))
        self.assertIsNone(self._f([_anexo(estado="rechazado")])(MADRE))

    def test_una_ot_que_no_es_desinstalacion_no_hereda_nada(self):
        self.assertIsNone(self._f([_anexo()], {500: {"ot_madre_id": MADRE, "ot_relacion": "seguimiento"}})(500))

    def test_un_fallo_de_la_base_jamas_rompe(self):
        def malo(sql, p=()):
            raise RuntimeError("base caída")
        amb = cargar(funciones=("_ot_anexo_de_la_madre", "_ot_anexo_vivo"), constantes=("_ANEXO_SEL_VIVO",),
                     extra={"_oac": O, "mysql_fetchone": malo, "print": lambda *a, **k: None})
        self.assertIsNone(amb["_ot_anexo_vivo"](MADRE))


class TestLaFamiliaSeCuadraJunta(unittest.TestCase):
    def _amb(self, ax, hijas=None, resultados=None):
        self.llamadas = []

        def alinear(vid, origen="", solo_si_falta=False):
            self.llamadas.append((vid, origen, solo_si_falta))
            return (resultados or {}).get(vid)
        return cargar(funciones=("_ot_anexo_familia_ids", "_ot_anexo_alinear_familia"),
                      extra={"_ot_anexo_vivo": lambda vid: ax, "_ot_anexo_alinear_costo": alinear, "print": lambda *a, **k: None,
                             "_ot_hijas_de": lambda mid: (hijas if hijas is not None else [{"id": HIJA, "estado": "creada"}])})

    AX = {"compartido": True, "ot_id": MADRE}

    def test_la_instalacion_arrastra_a_su_desinstalacion_vigente(self):
        amb = self._amb(self.AX, hijas=[{"id": HIJA, "estado": "creada"}, {"id": 443, "estado": "cancelada"}])
        self.assertEqual(amb["_ot_anexo_familia_ids"](MADRE, self.AX), [HIJA])
        amb["_ot_anexo_alinear_familia"](MADRE, origen="x")
        self.assertEqual([c[0] for c in self.llamadas], [MADRE, HIJA])

    def test_la_desinstalacion_arrastra_a_su_instalacion(self):
        amb = self._amb(self.AX)
        self.assertEqual(amb["_ot_anexo_familia_ids"](HIJA, self.AX), [MADRE])

    def test_un_anexo_no_compartido_no_toca_a_nadie_mas(self):
        amb = self._amb({"compartido": False, "ot_id": MADRE})
        amb["_ot_anexo_alinear_familia"](MADRE, origen="x")
        self.assertEqual([c[0] for c in self.llamadas], [MADRE])

    def test_aunque_la_ot_este_cerrada_la_otra_se_cuadra(self):
        amb = self._amb(self.AX, resultados={MADRE: None})
        amb["_ot_anexo_alinear_familia"](MADRE, origen="x")
        self.assertEqual([c[0] for c in self.llamadas], [MADRE, HIJA])

    def test_el_parametro_solo_si_falta_viaja_a_las_dos(self):
        amb = self._amb(self.AX)
        amb["_ot_anexo_alinear_familia"](MADRE, origen="x", solo_si_falta=True)
        self.assertTrue(all(c[2] for c in self.llamadas))


class TestCrearYEditarConPartidasPorOT(unittest.TestCase):
    def _amb(self, hijas, anexo_propio=None):
        def fetchone(sql, params=()):
            return anexo_propio
        return cargar(funciones=("_anexo_items_norm", "_anexo_validar_salida"),
                      extra={"_oac": O, "_ot_hijas_de": lambda vid, estricto=False: hijas,
                             "_ot2_err": lambda msg, codigo, http=400, **k: (codigo, http),
                             "mysql_fetchone": fetchone, "print": lambda *a, **k: None})

    def test_las_partidas_conservan_su_ot(self):
        f = self._amb([])["_anexo_items_norm"]
        self.assertEqual(f([{"concepto": "Desinstalación", "monto": "150000", "ot": "salida"}, {"concepto": "X", "monto": 5, "ot": "raro"}]),
                         [{"concepto": "Desinstalación", "monto": 150000, "ot": "salida"}, {"concepto": "X", "monto": 5}])

    def test_sin_partidas_de_la_salida_no_hay_nada_que_validar(self):
        self.assertIsNone(self._amb([])["_anexo_validar_salida"](MADRE, [{"concepto": "A", "monto": 1, "ot": "entrada"}]))

    def test_partidas_de_salida_sin_desinstalacion_se_rechazan(self):
        v = self._amb([])["_anexo_validar_salida"]
        self.assertEqual(v(MADRE, ITEMS)[0], "SALIDA_SIN_DESINSTALACION")
        self.assertEqual(v(MADRE, [{"concepto": "x", "monto": 1, "ot": "salida"}, ])[0], "SALIDA_SIN_DESINSTALACION")
        self.assertEqual(v(None, ITEMS)[0], "SALIDA_SIN_OT")
        # una desinstalación cancelada no cuenta
        self.assertEqual(self._amb([{"id": HIJA, "estado": "cancelada"}])["_anexo_validar_salida"](MADRE, ITEMS)[0], "SALIDA_SIN_DESINSTALACION")

    def test_si_la_desinstalacion_ya_tiene_su_anexo_no_se_cobra_dos_veces(self):
        v = self._amb([{"id": HIJA, "estado": "creada"}], anexo_propio={"numero": 222})["_anexo_validar_salida"]
        self.assertEqual(v(MADRE, ITEMS), ("SALIDA_CON_ANEXO_PROPIO", 409))

    def test_con_desinstalacion_vigente_y_sin_anexo_propio_pasa(self):
        self.assertIsNone(self._amb([{"id": HIJA, "estado": "creada"}])["_anexo_validar_salida"](MADRE, ITEMS))

    def test_si_la_base_falla_no_se_deja_pasar_a_ciegas(self):
        def hijas(vid, estricto=False):
            raise RuntimeError("caída")
        amb = cargar(funciones=("_anexo_validar_salida",),
                     extra={"_oac": O, "_ot_hijas_de": hijas, "_ot2_err": lambda m, c, http=400, **k: (c, http),
                            "mysql_fetchone": lambda *a, **k: None, "print": lambda *a, **k: None})
        self.assertEqual(amb["_anexo_validar_salida"](MADRE, ITEMS), ("ERROR_INTERNO", 500))


class TestLaDesinstalacionSeSumaAlAnexo(unittest.TestCase):
    def _amb(self, fila_anexo, rowcount=1):
        self.sql, self.logs, self.alineadas = [], [], []
        return cargar(funciones=("_ot_anexo_agregar_salida",),
                      extra={"_oac": O, "json": json, "_ANEXO_EDITABLES": ("borrador", "enviado", "visto", "vencido"),
                             "mysql_fetchone": lambda sql, p=(): fila_anexo,
                             "mysql_execute_returning_rowcount": lambda sql, p=(): (self.sql.append((sql, p)) or rowcount),
                             "_mant_log": lambda *a: self.logs.append(a),
                             "_ot_anexo_alinear_familia": lambda vid, origen="": self.alineadas.append((vid, origen)),
                             "print": lambda *a, **k: None})["_ot_anexo_agregar_salida"]

    BASE = [{"concepto": "Instalación", "monto": 300000}, {"concepto": "Despacho", "monto": 20000}]

    def test_suma_las_partidas_de_la_salida_y_marca_las_de_la_entrada(self):
        f = self._amb(_anexo(estado="borrador", items=self.BASE))
        txt = f(MADRE, HIJA, 150000, 10000)
        self.assertIn("anexo único", txt)
        items = json.loads(self.sql[0][1][0])
        self.assertEqual([(i["concepto"], i["monto"], i["ot"]) for i in items],
                         [("Instalación", 300000, "entrada"), ("Despacho (instalación)", 20000, "entrada"),
                          ("Desinstalación", 150000, "salida"), ("Despacho (desinstalación)", 10000, "salida")])
        self.assertIn("estado IN ('borrador','enviado','visto','vencido')", self.sql[0][0])       # un anexo firmado nunca se toca
        self.assertEqual(self.alineadas[0][0], MADRE)                                            # y las DOS OT se cuadran
        self.assertEqual([(l[0], l[1], l[2]) for l in self.logs],                                # queda en la bitácora del anexo y de las dos OT
                         [("anexo", 9, "desinstalacion_sumada"), ("visita", MADRE, "anexo_desinstalacion_sumada"),
                          ("visita", HIJA, "anexo_desinstalacion_sumada")])
        self.assertNotIn("reenviárselo", txt)

    def test_si_ya_se_habia_enviado_avisa_que_conviene_reenviarlo(self):
        txt = self._amb(_anexo(estado="enviado", items=self.BASE))(MADRE, HIJA, 150000, 0)
        self.assertIn("reenviárselo", txt)

    def test_un_anexo_firmado_o_con_la_salida_ya_incluida_no_se_toca(self):
        for fila in (_anexo(estado="firmado", items=self.BASE), _anexo(estado="enviado", items=ITEMS), None):
            f = self._amb(fila)
            self.assertIsNone(f(MADRE, HIJA, 150000, 10000))
            self.assertEqual(self.sql, [])

    def test_sin_costos_en_la_salida_no_hay_partidas_que_sumar(self):
        f = self._amb(_anexo(estado="borrador", items=self.BASE))
        self.assertIsNone(f(MADRE, HIJA, None, 0))
        self.assertEqual(self.sql, [])

    def test_si_el_estado_cambio_entre_la_lectura_y_la_escritura_no_se_canta_exito(self):
        self.assertIsNone(self._amb(_anexo(estado="borrador", items=self.BASE), rowcount=0)(MADRE, HIJA, 150000, 10000))


class TestQuienComparteElAnexo(unittest.TestCase):
    SAME = {7: ("ficha", 3), 8: ("ficha", 3)}

    def _amb(self, llaves, firmado=False, vivo=True, madre_tec=7):
        def fetchone(sql, p=()):
            if "tecnico_user_id" in sql:
                return {"tecnico_user_id": madre_tec}
            if "estado='firmado'" in sql:
                return {"x": 1} if firmado else None
            if "NOT IN ('rechazado','anulado')" in sql:
                return {"x": 1} if vivo else None
            return None
        return cargar(funciones=("_ot_hija_comparte_anexo",),
                      extra={"mysql_fetchone": fetchone, "_ot_proveedor_ext_key": lambda uid: llaves.get(uid),
                             "print": lambda *a, **k: None})["_ot_hija_comparte_anexo"]

    def test_el_mismo_proveedor_comparte_si_el_anexo_de_la_instalacion_existe(self):
        self.assertTrue(self._amb(self.SAME)(MADRE, 8))

    def test_otro_proveedor_o_tecnico_propio_no_comparte(self):
        self.assertFalse(self._amb({7: ("ficha", 3), 8: ("ficha", 4)})(MADRE, 8))
        self.assertFalse(self._amb({7: ("ficha", 3)})(MADRE, 8))
        self.assertFalse(self._amb({})(MADRE, None))

    def test_con_el_anexo_de_la_instalacion_firmado_la_desinstalacion_lleva_el_suyo(self):
        self.assertFalse(self._amb(self.SAME, firmado=True)(MADRE, 8))
        self.assertFalse(self._amb(self.SAME, firmado=True)(MADRE, 8, en_camino=True))

    def test_si_la_instalacion_nunca_tendra_anexo_la_desinstalacion_lleva_el_suyo(self):
        # una instalación anterior a la regla (sin anexo y sin que el asistente vaya a crearlo) no deja a la desinstalación sin anexo
        self.assertFalse(self._amb(self.SAME, vivo=False)(MADRE, 8))

    def test_si_el_asistente_va_a_crear_el_anexo_de_la_instalacion_comparte(self):
        self.assertTrue(self._amb(self.SAME, vivo=False)(MADRE, 8, en_camino=True))


class TestElBloqueoDelTecnicoExterno(unittest.TestCase):
    def _f(self, madre_row, n_firmados=0):
        def fetchone(sql, p=()):
            if "estado IN ('enviado','visto')" in sql:
                return None
            if "FROM mant_visitas v" in sql:
                return {"tecnico_user_id": 8, "created_at": "2026-10-24", "rol_tec": "tecnico_externo", "n_firmados": n_firmados, "es_ext_ficha": 0}
            return None
        return cargar(funciones=("_anexo_bloquea_ot",), constantes=("_ANEXO_OBLIGATORIO_DESDE",),
                      extra={"mysql_fetchone": fetchone, "_ot_anexo_de_la_madre": lambda vid, solo_firmado=False: madre_row,
                             "print": lambda *a, **k: None})["_anexo_bloquea_ot"]

    def test_con_el_anexo_unico_firmado_la_desinstalacion_queda_libre(self):
        self.assertIsNone(self._f({"estado": "firmado", "numero": 221})(HIJA))

    def test_con_el_anexo_unico_sin_firmar_queda_bloqueada_con_su_numero(self):
        self.assertEqual(self._f({"estado": "enviado", "numero": 221})(HIJA), 221)
        self.assertEqual(self._f({"estado": "visto", "numero": 221})(HIJA), 221)

    def test_sin_anexo_que_la_cubra_sigue_como_siempre(self):
        self.assertEqual(self._f(None)(HIJA), "SIN_ANEXO")
        self.assertEqual(self._f({"estado": "borrador", "numero": 221})(HIJA), "SIN_ANEXO")      # un borrador no bloquea ni libera

    def test_un_anexo_propio_firmado_la_libera_sin_mirar_a_la_madre(self):
        self.assertIsNone(self._f({"estado": "enviado", "numero": 221}, n_firmados=1)(HIJA))


class TestLaGuardiaYLaVista(unittest.TestCase):
    def test_la_guardia_dice_cual_es_su_parte(self):
        ax = {"id": 9, "numero": 221, "estado": "firmado", "total": 160000, "total_general": 480000, "compartido": True}
        amb = cargar(funciones=("_ot_anexo_costo_guardia",),
                     extra={"_oac": O, "jsonify": lambda d: ("JSON", d), "_ot_anexo_vivo": lambda vid: ax,
                            "_ot_costo_fila": lambda vid: {"costo_proveedor": None, "costo_despacho": None}})
        resp, code = amb["_ot_anexo_costo_guardia"](HIJA, 65000, None)
        self.assertEqual(code, 409)
        self.assertIn("$160.000 de $480.000", resp[1]["error"])
        self.assertEqual((resp[1]["anexo"]["total_general"], resp[1]["anexo"]["compartido"]), (480000, True))
        # su propio costo (la parte) pasa; el del anexo completo no
        self.assertIsNone(amb["_ot_anexo_costo_guardia"](HIJA, 150000, 10000))
        self.assertEqual(amb["_ot_anexo_costo_guardia"](HIJA, 480000, None)[1], 409)

    def _vista(self, ax, v):
        return cargar(funciones=("_ot_anexo_vinculo_vista",),
                      extra={"_oac": O, "_ot_anexo_vivo": lambda vid: ax, "print": lambda *a, **k: None,
                             "mysql_fetchone": lambda sql, p=(): {"numero_ot": "OT-2026-00355"}})["_ot_anexo_vinculo_vista"](v)

    def test_la_vista_de_una_desinstalacion_dice_de_cual_instalacion_y_el_detalle(self):
        ax = {"id": 9, "numero": 221, "estado": "enviado", "items": [i for i in ITEMS if i["ot"] == "salida"], "total": 160000,
              "compartido": True, "rol": "salida", "ot_id": MADRE, "items_general": ITEMS, "total_general": 480000, "proveedor": "DAP SpA"}
        v = self._vista(ax, {"id": HIJA, "costo_proveedor": 150000, "costo_despacho": 10000, "estado": "creada"})
        self.assertEqual((v["vinculo"], v["compartido"], v["es_hija"], v["total_general_txt"]), ("alineado", True, True, "$480.000"))
        self.assertEqual(v["anexo_ot_numero"], "OT-2026-00355")
        self.assertEqual(v["partidas_txt"], "Instalación $320.000 (despacho $20.000) · Desinstalación $160.000 (despacho $10.000)")

    def test_la_vista_de_un_anexo_normal_no_cambia(self):
        ax = {"id": 9, "numero": 221, "estado": "firmado", "items": [{"concepto": "Servicio", "monto": 370000}], "total": 370000,
              "compartido": False, "rol": None, "ot_id": MADRE, "items_general": [{"concepto": "Servicio", "monto": 370000}], "total_general": 370000}
        v = self._vista(ax, {"id": MADRE, "costo_proveedor": 370000, "costo_despacho": None, "estado": "creada"})
        self.assertEqual((v["vinculo"], v["compartido"]), ("alineado", False))
        self.assertNotIn("partidas", v)


class TestLaListaDeOTQueNoCoinciden(unittest.TestCase):
    def test_cada_ot_se_compara_con_su_parte(self):
        filas = [{"anexo_id": 9, "numero": 221, "anexo_estado": "firmado", "precio_items_json": json.dumps(ITEMS), "vid": MADRE,
                  "numero_ot": "OT-1", "ot_estado": "creada", "costo_proveedor": 300000, "costo_despacho": 20000}]
        hijas = [{"id": HIJA, "numero_ot": "OT-2", "estado": "creada", "costo_proveedor": 65000, "costo_despacho": None}]

        def fetchall(sql, p=()):
            return hijas if "ot_madre_id" in sql else filas
        amb = cargar(funciones=("ot_api_anexos_vinculo_costos",),
                     extra={"_oac": O, "_ot_aut_es_superadmin": lambda: True, "jsonify": lambda d: d, "mysql_fetchall": fetchall})
        r = amb["ot_api_anexos_vinculo_costos"]()
        self.assertEqual(r["cuenta"], {"alineado": 1, "sin_declarar": 0, "difiere": 1})
        malo = r["por_corregir"][0]
        self.assertEqual((malo["ot_id"], malo["parte"], malo["total_anexo"], malo["vinculo"]), (HIJA, "salida", 160000, "difiere"))


# ───────────────────────── pantallas (JS ejecutado en Node) ─────────────────────────
def _js_funcion(texto_js, nombre):
    i = texto_js.index("function %s(" % nombre)
    prof, j = 0, texto_js.index("{", i)
    while True:
        if texto_js[j] == "{":
            prof += 1
        elif texto_js[j] == "}":
            prof -= 1
            if prof == 0:
                return texto_js[i:j + 1]
        j += 1


def _node(programa):
    f = os.path.join(tempfile.mkdtemp(), "prog.js")
    with open(f, "w", encoding="utf-8") as fh:
        fh.write(programa)
    r = subprocess.run(["node", f], capture_output=True, text=True, encoding="utf-8", timeout=60)
    assert r.returncode == 0, r.stderr
    return json.loads(r.stdout)


@unittest.skipUnless(shutil.which("node"), "sin Node")
class TestElModalDelAnexo(unittest.TestCase):
    M = _leer("templates/ot2/_modal_anexo.html")

    def _items(self, def_ot):
        prog = "\n".join(["var DEF_OT = %s; var S = null;" % json.dumps(def_ot), _js_funcion(self.M, "esCompartido"),
                          _js_funcion(self.M, "itemsDesdeOT"), "console.log(JSON.stringify(itemsDesdeOT()));"])
        return _node(prog)

    def test_con_desinstalacion_las_partidas_vienen_etiquetadas_y_con_nombre_claro(self):
        its = self._items({"tipo_label": "Instalación", "costo_proveedor": 300000, "costo_despacho": 20000,
                           "salida": {"numero_ot": "OT-2", "costo_proveedor": 150000, "costo_despacho": 10000}})
        self.assertEqual([(i["concepto"], i["monto"], i["ot"]) for i in its],
                         [("Instalación", "300000", "entrada"), ("Despacho (instalación)", "20000", "entrada"),
                          ("Desinstalación", "150000", "salida"), ("Despacho (desinstalación)", "10000", "salida")])

    def test_sin_desinstalacion_todo_queda_como_siempre(self):
        its = self._items({"tipo_label": "Mantención", "costo_proveedor": 100, "costo_despacho": 5, "salida": None})
        self.assertEqual(its, [{"concepto": "Mantención", "monto": "100"}, {"concepto": "Despacho", "monto": "5"}])
        self.assertEqual(self._items({}), [{"concepto": "", "monto": ""}])

    def test_el_payload_conserva_la_ot_de_cada_partida_y_hay_selector_por_fila(self):
        m = self.M
        self.assertIn("if (it.ot === 'entrada' || it.ot === 'salida') o.ot = it.ot;", m)
        self.assertIn("OT2ANX.setItemOt(${i}, this.value)", m)
        self.assertIn("setItemConcepto, setItemMonto, setItemOt,", m)
        self.assertIn('id="oaxSubtotales"', m)
        self.assertIn("if (it.ot === 'entrada' || it.ot === 'salida') f.ot = it.ot;", m)         # al corregir un anexo no se pierden las etiquetas


@unittest.skipUnless(shutil.which("node"), "sin Node")
class TestElAsistente(unittest.TestCase):
    W = _leer("templates/ot2/_modal_crear.html")

    def _salida(self, **s):
        base = {"tiene_hija": True, "lider": 8, "ext": {"id": 7, "es_externo": True, "empresa_nombre": "DAP"},
                "lid": {"id": 8, "es_externo": True, "empresa_nombre": "DAP"}, "costo_proveedor": "150000", "costo_despacho": "10000"}
        base.update(s)
        prog = "\n".join([
            "var B = %s; var S = {desinst: {costo_proveedor: B.costo_proveedor, costo_despacho: B.costo_despacho}};" % json.dumps(base),
            "function _o2mDiTieneHija(){ return B.tiene_hija; } function _diTecObj(){ return B.lid; } function _diLider(){ return B.lider; }",
            "function _primerTecnicoExterno(){ return B.ext; } function _diNum(v){ var n = parseInt(String(v||'').replace(/\\D/g,''),10); return isFinite(n) ? n : null; }",
            _js_funcion(self.W, "_o2mAnexoSalida"),
            "console.log(JSON.stringify(_o2mAnexoSalida()));"])
        return _node(prog)

    def test_misma_empresa_externa_comparte_el_anexo(self):
        self.assertEqual(self._salida(), {"prov": 150000, "desp": 10000})

    def test_otra_empresa_interno_o_sin_desinstalacion_no_comparte(self):
        self.assertIsNone(self._salida(lid={"id": 9, "es_externo": True, "empresa_nombre": "OTRA"}))
        self.assertIsNone(self._salida(lid={"id": 9, "es_externo": False}))
        self.assertIsNone(self._salida(tiene_hija=False))
        self.assertIsNone(self._salida(ext=None))

    def test_el_mismo_tecnico_en_las_dos_ot_comparte(self):
        self.assertIsNotNone(self._salida(lid={"id": 7, "es_externo": True, "empresa_nombre": ""}))

    def _para_servidor(self, items, comparte):
        prog = "\n".join(["var S = {anexo: {items: %s}, fin_garantia: false};" % json.dumps(items),
                          _js_funcion(self.W, "_o2mAnexoItemsValidos"), _js_funcion(self.W, "_o2mAnexoItemsParaServidor"),
                          "console.log(JSON.stringify(_o2mAnexoItemsParaServidor(%s)));" % ("true" if comparte else "false")])
        return _node(prog)

    ITEMS_W = [{"concepto": "Instalación", "monto": 300000, "ot": "entrada"}, {"concepto": "Despacho (instalación)", "monto": 20000, "ot": "entrada"},
               {"concepto": "Desinstalación", "monto": 150000, "ot": "salida"}]

    def test_si_el_servidor_dice_que_comparte_viajan_todas_con_su_ot(self):
        r = self._para_servidor(self.ITEMS_W, True)
        self.assertEqual([(i["concepto"], i["ot"]) for i in r],
                         [("Instalación", "entrada"), ("Despacho (instalación)", "entrada"), ("Desinstalación", "salida")])

    def test_si_el_servidor_dice_que_no_se_descarta_la_salida_y_se_limpia_el_nombre(self):
        r = self._para_servidor(self.ITEMS_W, False)
        self.assertEqual(r, [{"concepto": "Instalación", "monto": 300000}, {"concepto": "Despacho", "monto": 20000}])

    def test_el_servidor_es_quien_decide_y_el_asistente_le_hace_caso(self):
        w = self.W
        self.assertIn("_o2mAnexoItemsParaServidor(!!(res.j.desinstalacion && res.j.desinstalacion.comparte_anexo))", w)
        self.assertIn("body.anexo = { precio_items: _o2mAnexoItemsParaServidor(false) };", w)       # la salida aún no existe al crear la instalación
        self.assertIn("try { _o2mAnexoSeguirCostos(); } catch (e){}", w)                           # el costo de la salida mueve el anexo (si nadie lo tocó)
        self.assertIn("_o2mAnexoUnicoNota()+", w)


# ───────────────────────── cableado en app.py y la ficha ─────────────────────────
class TestCableado(unittest.TestCase):
    def test_crear_y_editar_validan_y_conservan_las_etiquetas_y_cuadran_las_dos_ot(self):
        for nombre, escribe in (("ot2_api_anexo_crear", "INSERT INTO mant_anexos"), ("ot2_api_anexo_editar", "UPDATE mant_anexos SET")):
            s = fuente_funcion(nombre)
            self.assertIn("_anexo_items_norm(items)", s, nombre)
            self.assertIn("_anexo_validar_salida(", s, nombre)
            self.assertLess(s.index("_anexo_validar_salida("), s.index(escribe), nombre)          # se valida ANTES de escribir
            self.assertLess(s.index("_anexo_validar_salida("), s.index("_anexo_total_items(items) <= 0"), nombre)
            self.assertIn("_ot_anexo_alinear_familia(", s, nombre)
        self.assertIn("_ot_anexo_alinear_familia(", fuente_funcion("ot2_anexo_firma_submit"))

    def test_el_cierre_compara_cada_ot_con_su_parte_del_anexo_firmado(self):
        s = fuente_funcion("mant_ot_aprobar_cierre")
        self.assertIn("_ot_anexo_de_la_madre(vid, solo_firmado=True)", s)
        self.assertIn("_oac.parte(_items_anx, _rol_anx_cierre)", s)
        self.assertLess(s.index("_rol_anx_cierre"), s.index('"ANEXO_DESACTUALIZADO"'))

    def test_la_desinstalacion_no_genera_anexo_propio_si_comparte_y_suma_sus_partidas(self):
        s = fuente_funcion("_ot_crear_desinstalacion")
        self.assertIn("_ot_hija_comparte_anexo(madre_vid, dd.get(\"tecnico_lider_id\"), en_camino=anexo_en_camino)", s)
        # el asistente crea el anexo de la instalación JUSTO DESPUÉS de la OT: la desinstalación lo sabe por `anexo_delegado`
        self.assertIn('anexo_en_camino=bool(d.get("anexo_delegado"))', fuente_funcion("_ot2_crear_core"))
        self.assertIn('d_hija["anexo_delegado"] = True', s)
        self.assertLess(s.index('d_hija["anexo_delegado"] = True'), s.index("_ot2_crear_core(d_hija, madre=m)"))
        self.assertIn("_ot_anexo_agregar_salida(", s)
        self.assertIn('"comparte_anexo"', s)

    def test_el_modal_de_la_ficha_recibe_lo_que_cobra_la_desinstalacion(self):
        self.assertIn('"salida": _ot_anexo_salida_defaults(vid),', fuente_funcion("ot2_detalle"))

    def test_las_funciones_nuevas_no_lanzan_ni_escriben_a_ciegas(self):
        for n in ("_ot_anexo_salida_defaults", "_ot_anexo_agregar_salida", "_ot_hija_comparte_anexo", "_ot_proveedor_ext_key",
                  "_ot_anexo_familia_ids", "_ot_anexo_alinear_familia"):
            self.assertIn("except Exception", fuente_funcion(n), n)

    def test_un_tecnico_no_recibe_nada_de_esto(self):
        s = fuente_funcion("ot2_detalle")
        self.assertIn("anexo_vinculo = None if _es_rol_tecnico() else _ot_anexo_vinculo_vista(v)", s)
        for n in ("ot2_api_anexo_crear", "ot2_api_anexo_editar"):
            self.assertIn("_es_rol_tecnico()", fuente_funcion(n), n)


class TestLaFichaYLaFranja(unittest.TestCase):
    @staticmethod
    def _macro(av):
        from jinja2 import Environment, FileSystemLoader
        env = Environment(loader=FileSystemLoader(os.path.join(RAIZ, "templates")))
        t = env.from_string("{% from 'ot2/_anexo_vinculo.html' import franja %}{{ franja(av, 'x', 442, False) }}")
        return re.sub(r"\s+", " ", t.render(av=av))

    def test_la_franja_de_una_ot_compartida_dice_su_parte_y_el_detalle(self):
        av = {"numero": 221, "firmado": False, "total_txt": "$160.000", "costo_txt": "$160.000", "vinculo": "alineado", "abierta": True,
              "compartido": True, "total_general_txt": "$480.000", "es_hija": True, "anexo_ot_numero": "OT-2026-00355",
              "partidas_txt": "Instalación $320.000 · Desinstalación $160.000"}
        h = self._macro(av)
        self.assertIn("su parte ($160.000 de $480.000) del Anexo N° 221", h)
        self.assertIn("Anexo único con el proveedor", h)
        self.assertIn("se firma desde la instalación OT-2026-00355", h)

    def test_la_franja_de_siempre_no_cambia(self):
        av = {"numero": 221, "firmado": True, "total_txt": "$370.000", "costo_txt": "$370.000", "vinculo": "alineado", "abierta": False}
        h = self._macro(av)
        self.assertIn("El costo de la OT es el Anexo N° 221 (firmado) · $370.000.", h)
        self.assertNotIn("Anexo único", h)

    def test_la_tarjeta_de_la_desinstalacion_no_ofrece_crear_otro_anexo(self):
        t = _leer("templates/ot2/detalle.html")
        i = t.index("{% if not anexo and anexo_vinculo and anexo_vinculo.compartido %}")
        j = t.index("{% elif not anexo %}", i)
        bloque = t[i:j]
        self.assertIn("anexo único", bloque)
        self.assertNotIn("OT2ANX.abrir(", bloque)
        self.assertIn("url_for('ot2_detalle', vid=anexo_vinculo.anexo_ot_id)", bloque)
        # la instalación muestra el detalle de lo que se le paga por cada cosa
        self.assertIn('id="otdAnexoUnicoDetalle"', t)

    def test_el_html_sigue_siendo_jinja_valido(self):
        from jinja2 import Environment
        env = Environment()
        for rel in ("templates/ot2/detalle.html", "templates/ot2/_anexo_vinculo.html", "templates/ot2/_modal_anexo.html"):
            env.parse(_leer(rel))


class TestLosListadosVenElAnexoDeLaInstalacion(unittest.TestCase):
    """El panel de OT y el lote de Facturación de proveedor unen mant_anexos con la OT por SQL: la desinstalación ve el anexo único de su
    instalación (el propio manda), y el lote compara a cada OT con SU parte."""

    def _join(self, nombre):
        return cargar(constantes=(nombre,), extra={"_mfp_sql_ficha_de_tecnico": lambda alias: "NULL"})[nombre]

    def _ejecutar(self, join_sql, anexos, visitas):
        import sqlite3
        cx = sqlite3.connect(":memory:")
        cx.executescript("""
            CREATE TABLE mant_visitas (id INT, ot_madre_id INT, ot_relacion TEXT);
            CREATE TABLE mant_anexos (id INT, ot_id INT, numero INT, estado TEXT, precio_items_json TEXT);
            CREATE TABLE mant_factura_proveedor_items (visita_id INT, factura_proveedor_id INT);
            CREATE TABLE mant_facturas_proveedor (id INT);""")
        cx.executemany("INSERT INTO mant_visitas VALUES (?,?,?)", visitas)
        cx.executemany("INSERT INTO mant_anexos VALUES (?,?,?,?,?)", anexos)
        # el JOIN real de app.py, tal cual (solo cambia el motor)
        filas = cx.execute("SELECT v.id, anx.numero FROM mant_visitas v " + join_sql + " ORDER BY v.id").fetchall()
        return dict(filas)

    VISITAS = [(MADRE, None, None), (HIJA, MADRE, "desinstalacion"), (500, None, None)]

    def _anexos(self, items=ITEMS, estado="enviado", propio_hija=False):
        a = [(9, MADRE, 221, estado, json.dumps(items))]
        if propio_hija:
            a.append((10, HIJA, 222, "enviado", json.dumps([{"concepto": "Desinstalación", "monto": 5}])))
        return a

    def test_la_desinstalacion_ve_el_anexo_unico_de_su_instalacion_en_los_dos_listados(self):
        for nombre in ("_OT2_JOIN_ANEXO_FAC", "_MFP_JOINS_OT"):
            sql = self._join(nombre)
            if nombre == "_MFP_JOINS_OT":
                # el lote trae joins a clientes/técnicos que esta base de mentira no tiene: se prueba el trozo del anexo, idéntico al del panel
                sql = sql[sql.index("LEFT JOIN mant_anexos anx"):sql.index("LEFT JOIN mant_factura_proveedor_items")]
            else:
                sql = sql[:sql.index("LEFT JOIN mant_factura_proveedor_items")]
            r = self._ejecutar(sql, self._anexos(), self.VISITAS)
            self.assertEqual((r[MADRE], r[HIJA], r[500]), (221, 221, None), nombre)

    def test_el_anexo_propio_de_la_desinstalacion_manda(self):
        sql = self._join("_OT2_JOIN_ANEXO_FAC")
        sql = sql[:sql.index("LEFT JOIN mant_factura_proveedor_items")]
        r = self._ejecutar(sql, self._anexos(propio_hija=True), self.VISITAS)
        self.assertEqual((r[MADRE], r[HIJA]), (221, 222))

    def test_un_anexo_viejo_sin_partidas_de_salida_no_cubre_a_la_desinstalacion(self):
        sql = self._join("_OT2_JOIN_ANEXO_FAC")
        sql = sql[:sql.index("LEFT JOIN mant_factura_proveedor_items")]
        r = self._ejecutar(sql, self._anexos(items=[{"concepto": "Instalación", "monto": 350000}]), self.VISITAS)
        self.assertEqual((r[MADRE], r[HIJA]), (221, None))

    def test_el_sql_no_lleva_porcentaje_porque_viaja_con_parametros(self):
        for nombre in ("_OT2_JOIN_ANEXO_FAC", "_MFP_JOINS_OT"):
            self.assertNotIn("%", self._join(nombre).replace("%s", ""), nombre)

    def test_el_lote_compara_a_cada_ot_con_su_parte(self):
        s = fuente_funcion("_mfp_fila_ot")
        self.assertIn("_oac.total(_its_anx, _rol_anx)", s)
        self.assertIn("_anx_de_otra", s)
        self.assertIn("no coincide con el Anexo N°", s)                      # el aviso de siempre se conserva
        self.assertIn("anx.ot_id AS anexo_ot_id", texto())


class TestLosDocumentosDelAnexo(unittest.TestCase):
    """El PDF y la página de firma del proveedor dicen cuánto es de la instalación y cuánto de la desinstalación (solo en un anexo único)."""

    @staticmethod
    def _render(fragmento, items):
        from jinja2 import Environment
        env = Environment()
        env.globals["clp"] = O.clp
        return re.sub(r"\s+", " ", env.from_string(fragmento).render(anexo={"precio_items": items}))

    def _tabla_pdf(self):
        t = _leer("templates/ot2/anexo_documento.html")
        i = t.index('{% if anexo.precio_items %}\n  <div class="sec-titulo">Precio</div>')
        return t[i:t.index("{% endif %}\n\n  <div class=\"firma-bloque\">", i) + len("{% endif %}")]

    def _tabla_firma(self):
        t = _leer("templates/ot2/anexo_firma.html")
        i = t.index('<table class="precio-tabla">')
        return t[i:t.index("</table>", i) + len("</table>")]

    def test_el_pdf_de_un_anexo_unico_trae_los_dos_subtotales(self):
        h = self._render(self._tabla_pdf(), ITEMS)
        self.assertIn("Subtotal instalación</td><td class=\"monto\">$320.000", h)
        self.assertIn("Subtotal desinstalación</td><td class=\"monto\">$160.000", h)
        self.assertIn("Total Neto</td><td class=\"monto\">$480.000", h)

    def test_la_pagina_de_firma_de_un_anexo_unico_trae_los_dos_subtotales(self):
        h = self._render(self._tabla_firma(), ITEMS)
        self.assertIn("Subtotal instalación</i></td><td class=\"monto\">$320.000", h)
        self.assertIn("Subtotal desinstalación</i></td><td class=\"monto\">$160.000", h)
        self.assertIn("Total neto</td> <td class=\"monto\">$480.000", h)

    def test_un_anexo_de_siempre_no_cambia(self):
        normal = [{"concepto": "Instalación", "monto": 350000}, {"concepto": "Despacho", "monto": 20000}]
        for tabla in (self._tabla_pdf(), self._tabla_firma()):
            h = self._render(tabla, normal)
            self.assertNotIn("Subtotal", h)
            self.assertIn("$370.000", h)


if __name__ == "__main__":
    unittest.main()

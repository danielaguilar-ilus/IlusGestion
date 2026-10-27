# -*- coding: utf-8 -*-
"""⏱️ SLA Y TRAZABILIDAD DE LOS HALLAZGOS (Daniel, 26-oct-2026).

«necesito implementar los SLA y poder tener trazabilidad y considerar varios puntos de salida: OT, Tickets, Abastecimiento y almacenamiento
de repuestos. Y el consumo de las OT» y «tablas amigables con filas informativas y una buena sensación de guía, como Retiros».

Se prueba (sin BD, sin Flask, sin correos reales: REGLA #22):
  A. el módulo puro hallazgos_sla.py: plazos en horas hábiles (graves más cortos), semáforo, recorrido de 7 pasos, «lo que toca ahora»;
  B. `_hall_trazabilidad` y `_hall_vencidos_ids` de app.py con una base de mentira: solicitudes por la gestión / por «s123» / por lote, bodega,
     ticket, consumo de la OT; todo SOLO LECTURA y que nunca levanta;
  C. el cableado: la tarjeta de gestión usa el mismo recorrido, y un hallazgo que el técnico pidió como repuesto no se da por «atendido» hasta
     que la gestión actúa sobre la solicitud.
Correr:  python -m pytest tests/test_hallazgos_sla.py -q -p no:warnings
"""
import ast
import datetime as dt
import json
import os
import re
import sys
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import gestion_circular as GC  # noqa: E402
import hallazgos_sla as HS  # noqa: E402
import ot_hallazgos as H  # noqa: E402
from tests._fuente_app import decoradores, fuente_constante, texto  # noqa: E402
from tests._fuente_app import fuente_funcion as _fuente_funcion  # noqa: E402

APP = texto("app.py")
LUN = dt.datetime(2026, 10, 26, 9, 0)        # lunes 26-oct-2026 09:00 (día hábil)
AHORA = dt.datetime(2026, 10, 26, 15, 0)     # lunes 15:00 → 6 h hábiles después de las 09:00


def fila(**kw):
    f = {"estado_ef": "abierto", "severidad": "alerta", "riesgo": 0, "detectado_en": LUN}
    f.update(kw)
    return f


# ═══════════════════════════ A) el módulo puro ═══════════════════════════
class TestPlazos(unittest.TestCase):
    def test_los_graves_tienen_plazo_mas_corto(self):
        c = HS.config({})
        self.assertLess(c["primera_grave_h"], c["primera_h"])
        self.assertLess(c["salida_grave_h"], c["salida_h"])
        self.assertEqual(c["salida_grave_h"], 2 * 9.0, "2 días hábiles de 9 h")
        for sev in ("falla", "fuera_servicio", "baja"):
            self.assertTrue(HS.es_grave(sev), sev)
        self.assertTrue(HS.es_grave("alerta", riesgo=True), "puede lastimar a alguien")
        for sev in ("alerta", "repuesto", "incidencia", None, ""):
            self.assertFalse(HS.es_grave(sev), sev)

    def test_los_plazos_se_ajustan_por_variables_de_entorno_y_lo_invalido_se_ignora(self):
        c = HS.config({"HALLAZGOS_PRIMERA_GRAVE_H": "1", "HALLAZGOS_SALIDA_DIAS": "10", "HALLAZGOS_AVISO_PCT": "50"})
        self.assertEqual((c["primera_grave_h"], c["salida_h"], c["aviso_fraccion"]), (1.0, 90.0, 0.5))
        c = HS.config({"HALLAZGOS_PRIMERA_H": "abc", "HALLAZGOS_SALIDA_DIAS": "-3", "HALLAZGOS_AVISO_PCT": "9999"})
        self.assertEqual((c["primera_h"], c["salida_h"]), (HS.PRIMERA_H, HS.SALIDA_DIAS * 9.0))
        self.assertLessEqual(c["aviso_fraccion"], 0.95)

    def test_sin_tomar_un_grave_pasadas_2_horas_habiles_es_rojo_y_un_normal_aun_no(self):
        grave = HS.reloj(fila(severidad="falla"), AHORA)
        self.assertEqual((grave["semaforo"], grave["vencida"], grave["grave"]), ("rojo", True, True))
        self.assertIn("Sin tomar hace 6 h", grave["pill"])
        self.assertEqual(grave["pct"], 100)
        normal = HS.reloj(fila(), dt.datetime(2026, 10, 26, 10, 0))
        self.assertEqual(normal["semaforo"], "verde")
        self.assertIn("Falta tomarlo", normal["pill"])
        cerca = HS.reloj(fila(), dt.datetime(2026, 10, 26, 12, 30))       # 3,5 h de 4 → 87 % del plazo
        self.assertEqual(cerca["semaforo"], "ambar")
        self.assertTrue(0 < cerca["pct"] < 100)

    def test_el_fin_de_semana_y_los_feriados_no_corren(self):
        viernes = dt.datetime(2026, 10, 23, 16, 30)
        lunes = dt.datetime(2026, 10, 26, 8, 30)
        r = HS.reloj(fila(severidad="falla", detectado_en=viernes), lunes)
        self.assertEqual(round(r["primera"]["transcurrido_h"], 2), 1.0, "viernes 16:30 → lunes 08:30 son 1 h hábil (media del viernes y media del lunes), no 64")
        self.assertEqual(r["semaforo"], "verde")
        # Un feriado en lunes tampoco corre: viernes 16:30 → martes 08:30 con el lunes feriado sigue siendo 1 h.
        fer = HS.reloj(fila(severidad="falla", detectado_en=viernes), dt.datetime(2026, 10, 27, 8, 30), feriados=lambda a: {"2026-10-26"})
        self.assertEqual(round(fer["primera"]["transcurrido_h"], 2), 1.0)
        sin_fer = HS.reloj(fila(severidad="falla", detectado_en=viernes), dt.datetime(2026, 10, 27, 8, 30))
        self.assertGreater(sin_fer["primera"]["transcurrido_h"], 9.0, "sin el feriado, el lunes completo sí cuenta")

    def test_tomarlo_cumple_el_primer_tramo_y_sigue_corriendo_la_salida(self):
        r = HS.reloj(fila(severidad="falla", tomado_en=dt.datetime(2026, 10, 26, 10, 0)), AHORA)
        self.assertTrue(r["primera"]["terminado"])
        self.assertEqual(r["semaforo"], "verde")
        self.assertIn("Elegir su salida", r["pill"])
        self.assertIn("Plazo para darle salida", r["sub"])
        vencida = HS.reloj(fila(severidad="falla", tomado_en=dt.datetime(2026, 10, 26, 10, 0)), dt.datetime(2026, 10, 29, 12, 0))
        self.assertEqual((vencida["semaforo"], vencida["vencida"]), ("rojo", True))
        self.assertIn("Sin salida hace", vencida["pill"])

    def test_cualquier_salida_cumple_los_dos_tramos(self):
        for campo in ("seguimiento_en", "ticket_en", "repuesto_en"):
            r = HS.reloj(fila(severidad="falla", **{campo: dt.datetime(2026, 10, 26, 10, 0)}), AHORA)
            self.assertTrue(r["primera"]["terminado"] and r["salida"]["terminado"], campo)
            self.assertEqual(r["semaforo"], "verde", campo)
            self.assertIn("Con salida en", r["pill"])

    def test_una_salida_tardia_deja_ambar_y_una_ot_cerrada_deja_verde_y_un_descartado_gris(self):
        tarde = HS.reloj(fila(severidad="falla", ticket_en=dt.datetime(2026, 10, 29, 12, 0)), dt.datetime(2026, 10, 29, 13, 0))
        self.assertEqual(tarde["semaforo"], "ambar", "salió, pero fuera de plazo")
        cerrada = HS.reloj(fila(severidad="falla", seguimiento_en=dt.datetime(2026, 10, 29, 12, 0), seg_cerrada=True, estado_ef="resuelto"), dt.datetime(2026, 11, 6, 13, 0))
        self.assertEqual((cerrada["semaforo"], cerrada["terminado"]), ("verde", True))
        desc = HS.reloj(fila(estado_ef="descartado", descartado_en=dt.datetime(2026, 10, 26, 11, 0)), dt.datetime(2026, 12, 1, 9, 0))
        self.assertEqual((desc["semaforo"], desc["vencida"]), ("gris", False))
        self.assertIn("Descartado en", desc["pill"])

    def test_sin_fechas_no_revienta(self):
        r = HS.reloj({"estado_ef": "abierto", "severidad": "falla"}, AHORA)
        self.assertIn(r["semaforo"], ("verde", "ambar", "rojo"))
        self.assertEqual(HS.reloj({}, AHORA)["grave"], False)


class TestSalidas(unittest.TestCase):
    def test_abastecimiento_dice_donde_va_lo_que_se_pidio(self):
        self.assertEqual(HS.abastecimiento([])["estado"], "na")
        self.assertEqual(HS.abastecimiento([{"estado": "solicitado"}])["txt"], "Por validar")
        self.assertEqual(HS.abastecimiento([{"estado": "validado"}, {"estado": "pedido"}])["txt"], "Validado: falta pedirlo", "manda el paso más atrasado")
        self.assertEqual(HS.abastecimiento([{"estado": "pedido"}])["color"], "azul")
        self.assertEqual(HS.abastecimiento([{"estado": "recibido"}, {"estado": "instalado"}])["estado"], "hecho")
        self.assertEqual(HS.abastecimiento([{"estado": "rechazado"}])["estado"], "rechazado")
        self.assertEqual(HS.abastecimiento([{"estado": "rechazado"}, {"estado": "recibido"}])["estado"], "hecho", "una rechazada no frena a las demás")

    def test_bodega_compara_lo_pedido_con_la_existencia_fisica(self):
        stock = {5: {"cantidad": 1, "ubicacion": "A-01-03"}, 6: {"cantidad": 10, "ubicacion": "B-02"}}
        sin = HS.bodega([{"estado": "validado", "cantidad": 2, "repuesto_stock_id": 5}], stock)
        self.assertEqual((sin["estado"], sin["color"], sin["txt"]), ("curso", "ambar", "Sin stock suficiente"))
        self.assertIn("Piden 2 y hay 1", sin["sub"])
        ok = HS.bodega([{"estado": "validado", "cantidad": 2, "repuesto_stock_id": 6}], stock)
        self.assertEqual((ok["estado"], ok["txt"]), ("hecho", "Hay stock en bodega"))
        self.assertIn("B-02", ok["sub"], "dice dónde está guardado")
        self.assertEqual(HS.bodega([{"estado": "validado", "cantidad": 1, "repuesto_stock_id": 99}], stock)["txt"], "Sin stock")
        self.assertEqual(HS.bodega([{"estado": "validado", "cantidad": 1}], stock)["txt"], "Repuesto sin ficha de bodega")
        self.assertEqual(HS.bodega([], stock)["estado"], "na")
        # La misma pieza pedida dos veces suma: 1 + 1 contra 1 en bodega no alcanza.
        dos = HS.bodega([{"estado": "validado", "cantidad": 1, "repuesto_stock_id": 5}, {"estado": "solicitado", "cantidad": 1, "repuesto_stock_id": 5}], stock)
        self.assertEqual(dos["txt"], "Sin stock suficiente")
        # Lo ya instalado salió de bodega: no cuenta contra la existencia de hoy.
        inst = HS.bodega([{"estado": "instalado", "cantidad": 5, "repuesto_stock_id": 5}], stock)
        self.assertEqual(inst["estado"], "hecho")

    def test_bodega_descuenta_lo_que_otras_solicitudes_ya_tienen_prometido(self):
        # Revisión 27-oct: «Hay stock» era engañoso si otras solicitudes ya tenían ese stock comprometido.
        stock = {5: {"cantidad": 3, "ubicacion": "A-01", "comprometido": 2}}
        otras = HS.bodega([{"estado": "solicitado", "cantidad": 2, "repuesto_stock_id": 5}], stock)
        self.assertEqual((otras["txt"], otras["color"]), ("Sin stock suficiente", "ambar"))
        self.assertIn("ya están prometidos a otras solicitudes", otras["sub"])
        # Si lo comprometido es del PROPIO hallazgo (validado), no se resta dos veces.
        propio = HS.bodega([{"estado": "validado", "cantidad": 2, "repuesto_stock_id": 5}], stock)
        self.assertEqual(propio["txt"], "Hay stock en bodega")
        # Sin el dato (esquemas viejos) se comporta como antes.
        self.assertEqual(HS.bodega([{"estado": "solicitado", "cantidad": 2, "repuesto_stock_id": 5}], {5: {"cantidad": 3}})["txt"], "Hay stock en bodega")

    def test_consumo_cuenta_lo_instalado_y_su_valor(self):
        self.assertEqual(HS.consumo([])["estado"], "na")
        self.assertEqual(HS.consumo([{"estado": "validado", "cantidad": 1}])["txt"], "Aún sin consumir")
        c = HS.consumo([{"estado": "instalado", "cantidad": 2, "costo_unitario": 14990}, {"estado": "pedido", "cantidad": 1}])
        self.assertEqual((c["txt"], c["valor"], c["n"], c["estado"]), ("1 de 2 instalado", 29980, 1, "curso"))
        todo = HS.consumo([{"estado": "instalado", "cantidad": 3, "costo_unitario": "1000.50"}])
        self.assertEqual((todo["estado"], todo["valor"]), ("hecho", 3002))
        sin_costo = HS.consumo([{"estado": "instalado", "cantidad": 1}])
        self.assertIsNone(sin_costo["valor"], "sin costo no se inventa un monto")
        self.assertEqual(HS.consumo([{"estado": "rechazado", "cantidad": 1}])["estado"], "na")

    def test_consumo_de_una_ot(self):
        self.assertEqual(HS.consumo_ot([]), {"n": 0.0, "valor": None, "con_costo": False})
        r = HS.consumo_ot([{"cantidad": 2, "costo_unitario": 1000}, {"cantidad": 1, "costo_unitario": None}, {"cantidad": 1.5, "costo_unitario": 100}])
        self.assertEqual((r["n"], r["valor"], r["con_costo"]), (4.5, 2150, True))
        self.assertEqual(HS.num_txt(3.0), "3")
        self.assertEqual(HS.num_txt(2.5), "2,5")


class TestGuia(unittest.TestCase):
    def ctx(self, **kw):
        c = {"estado_ef": "abierto", "tomado_por": "", "valor": "", "seg": None, "ticket": None, "sols": [], "stock": {}}
        c.update(kw)
        return c

    def test_el_recorrido_son_siempre_7_pasos_y_nada_se_esconde(self):
        r = HS.recorrido(self.ctx())
        self.assertEqual([p["clave"] for p in r], ["atencion", "valor", "ot", "ticket", "abastecimiento", "bodega", "consumo"])
        self.assertEqual([p["color"] for p in r], ["ambar", "ambar", "gris", "gris", "gris", "gris", "gris"], "lo que falta en ámbar; lo opcional en gris")
        todo = HS.recorrido(self.ctx(estado_ef="tomado", tomado_por="Aarón", valor="$80.990", seg={"numero": "OT-1", "cerrada": True},
                                     ticket={"numero": "TK-1", "cerrado": True}, sols=[{"estado": "instalado", "cantidad": 1, "repuesto_stock_id": 5}],
                                     stock={5: {"cantidad": 3}}))
        self.assertTrue(all(p["color"] == "verde" for p in todo), [(p["clave"], p["color"]) for p in todo])
        desc = HS.recorrido(self.ctx(estado_ef="descartado"))
        self.assertEqual(desc[0]["txt"], "Descartado")
        self.assertNotIn("ambar", [p["color"] for p in desc if p["clave"] != "atencion"], "un descartado no queda pidiendo pasos")

    def test_lo_que_toca_ahora_sigue_el_camino_del_repuesto(self):
        sig = lambda **kw: HS.siguiente(self.ctx(**kw))["accion"]   # noqa: E731
        self.assertEqual(sig(), "tomar")
        self.assertEqual(sig(estado_ef="tomado", tomado_por="Ana"), "valorizar")
        self.assertEqual(sig(estado_ef="tomado", tomado_por="Ana", valor="$1"), "salida")
        self.assertEqual(sig(estado_ef="descartado"), "nada")
        self.assertEqual(sig(seg={"numero": "OT-1", "cerrada": True}), "listo")
        self.assertEqual(sig(estado_ef="resuelto", seg={"numero": "OT-1", "cerrada": False}), "ot")
        self.assertEqual(sig(estado_ef="tomado", tomado_por="Ana", ticket={"numero": "TK-1"}), "ticket")
        s = lambda est, stock_cant=0: HS.siguiente(self.ctx(estado_ef="tomado", tomado_por="Ana", sols=[{"estado": est, "cantidad": 2, "repuesto_stock_id": 5}],  # noqa: E731
                                                            stock={5: {"cantidad": stock_cant}}))["accion"]
        self.assertEqual(s("solicitado"), "validar")
        self.assertEqual(s("validado", 0), "pedir", "sin stock: pedirlo al proveedor")
        self.assertEqual(s("validado", 5), "bodega", "con stock: prepararlo de bodega")
        self.assertEqual(s("pedido"), "esperar")
        self.assertEqual(s("recibido"), "instalar")
        self.assertEqual(s("instalado"), "salida")

    def test_la_guia_no_contradice_el_recorrido_ticket_cerrado_compra_y_ot(self):
        # Revisión 27-oct: «ticket abierto» con el ticket ya cerrado, «pedir al proveedor» con la compra hecha, y «cierra el hallazgo» sin esa acción.
        cerrado = HS.siguiente(self.ctx(estado_ef="tomado", tomado_por="Ana", ticket={"numero": "TK-1", "cerrado": True}))
        self.assertEqual(cerrado["accion"], "salida")
        self.assertIn("cerrado", cerrado["txt"])
        self.assertNotIn("abierto", cerrado["txt"])
        base = {"estado": "validado", "cantidad": 2, "repuesto_stock_id": None}
        sig = lambda **kw: HS.siguiente(self.ctx(estado_ef="tomado", tomado_por="Ana", sols=[dict(base, **kw)]))   # noqa: E731
        self.assertEqual(sig(compra_id=7)["accion"], "esperar", "ya está en una compra: no se vuelve a pedir")
        self.assertEqual(sig(ot_generada_id=9)["accion"], "ot")
        self.assertEqual(sig(camino="ot")["accion"], "ot", "se resuelve con una OT: no pasa por bodega")
        self.assertEqual(sig(camino="bodega", repuesto_stock_id=5)["accion"], "pedir")
        inst = HS.siguiente(self.ctx(estado_ef="tomado", tomado_por="Ana", sols=[{"estado": "instalado", "cantidad": 1}]))
        self.assertNotIn("cierra el hallazgo", inst["txt"], "no existe una acción «cerrar»: se apunta a las que sí existen")
        self.assertIn("levanta la OT", inst["txt"])
        self.assertIn("descarta", inst["txt"])

    def test_cada_frase_es_corta_y_sin_jerga(self):
        for kw in ({}, {"estado_ef": "tomado", "tomado_por": "Ana"}, {"ticket": {"numero": "TK-1"}}, {"seg": {"numero": "OT-1", "cerrada": False}}):
            t = HS.siguiente(self.ctx(**kw))["txt"]
            self.assertLessEqual(len(t), 100, t)
            self.assertNotRegex(t, r"(?i)traceback|exception|null|none|_")


# ═══════════════════════════ B) la función por lotes con una base de mentira ═══════════════════════════
_COMP = {}


def cargar(funciones=(), constantes=(), extra=None):
    amb = dict(extra or {})
    for c in constantes:
        if ("c", c) not in _COMP:
            _COMP[("c", c)] = compile(ast.parse(fuente_constante(c)), "<app:%s>" % c, "exec")
        exec(_COMP[("c", c)], amb)
    for f in funciones:
        if ("f", f) not in _COMP:
            arbol = ast.parse(_fuente_funcion(f))
            for nodo in arbol.body:
                if isinstance(nodo, ast.FunctionDef):
                    nodo.decorator_list = []
            _COMP[("f", f)] = compile(arbol, "<app:%s>" % f, "exec")
        exec(_COMP[("f", f)], amb)
    return amb


class Base:
    """Una base de mentira para `_hall_trazabilidad` (solo lectura)."""

    def __init__(self):
        self.sql, self.falla = [], None
        self.extra = {}
        self.actividad = {}
        self.sols = {}
        self.lotes = []
        self.stock = {}
        self.tickets = {}
        self.consumo = []

    def fetchall(self, sql, params=()):
        s = " ".join(sql.split())
        self.sql.append(s)
        assert s.count("%s") == len(tuple(params or ())), s[:120]
        assert s.split()[0] == "SELECT", "SOLO LECTURA: " + s[:80]
        if self.falla and self.falla in s:
            raise RuntimeError("falla simulada")
        if s.startswith("SELECT entidad_id, MIN(creado_en)"):
            return [{"entidad_id": k, "t": v} for k, v in self.actividad.items()]
        if s.startswith("SELECT id, visita_id, lote_id FROM mant_ot_repuesto_solicitudes"):
            return list(self.lotes)
        if s.startswith("SELECT id, visita_id, estado, cantidad"):
            ids = set(params)
            return [dict(v) for k, v in self.sols.items() if k in ids]
        if s.startswith("SELECT s.id, s.cantidad, u.codigo"):
            return [{"id": k, "cantidad": v[0], "codigo": v[1], "comprometido": (v[2] if len(v) > 2 else 0)} for k, v in self.stock.items() if k in set(params)]
        if s.startswith("SELECT id, numero_ticket, estado FROM tk_tickets"):
            return [{"id": k, **v} for k, v in self.tickets.items() if k in set(params)]
        if s.startswith("SELECT COALESCE(ot_generada_id, visita_id) AS visita_id, cantidad"):
            return [dict(r) for r in self.consumo if r["visita_id"] in set(params)]
        if s.startswith("SELECT visita_id, cantidad"):          # respaldo (columna ot_generada_id aún inexistente)
            return [dict(r) for r in self.consumo if r["visita_id"] in set(params)]
        raise AssertionError("consulta no prevista: " + s[:160])


def hall(i, **kw):
    f = {"id": i, "visita_id": 340, "referencia": "t%d" % i, "severidad": "falla", "riesgo": 0, "estado": "abierto", "detectado_en": dt.datetime(2026, 10, 26, 12, 0),
         "tomado_por": None, "tomado_en": None, "descartado_en": None, "seguimiento_en": None, "ot_seguimiento_id": None, "seg_numero": None, "seg_estado": None}
    f.update(kw)
    return f


class TestTrazabilidadPorLotes(unittest.TestCase):
    def amb(self, bd):
        return cargar(["_hall_trazabilidad", "_hall_in"], ["_HALL_REF_SOL", "_HALL_REF_LOTE"], {
            "re": re, "print": lambda *a, **k: None, "_oh": H, "_hs": HS, "_gc": GC, "mysql_fetchall": bd.fetchall,
            "_gc_extra_de": lambda ent, ids: {k: dict(v) for k, v in bd.extra.items() if k in set(ids)},
            "_inq_ahora": lambda: dt.datetime(2026, 10, 26, 15, 0), "_otrep_a_chile": lambda d: d, "_otrep_feriados": lambda a: set(),
            "_OTREP_ESTADOS_COMPROMETEN": ("validado", "recibido")})

    def test_la_solicitud_del_propio_tecnico_se_encuentra_por_su_clave_y_no_da_por_atendido_el_hallazgo(self):
        bd = Base()
        bd.sols[301] = {"id": 301, "visita_id": 340, "estado": "solicitado", "cantidad": 2, "repuesto_nombre": "Piola", "repuesto_stock_id": 5, "ticket_id": None,
                        "created_at": dt.datetime(2026, 10, 26, 12, 0), "validado_at": None, "pedido_at": None, "recibido_at": None, "instalado_at": None}
        bd.stock[5] = (1, "A-01-03")
        t = self.amb(bd)["_hall_trazabilidad"]([hall(1, severidad="repuesto", referencia="m9:rep:s301")])[1]
        self.assertEqual(len(t["sols"]), 1)
        self.assertEqual(t["siguiente"]["accion"], "validar", "el técnico pidió el repuesto; falta que la gestión lo valide")
        self.assertEqual(t["reloj"]["primera"]["terminado"], False, "pedirlo NO cuenta como que alguien tomó el hallazgo")
        bd.sols[301]["validado_at"] = dt.datetime(2026, 10, 26, 13, 0)
        bd.sols[301]["estado"] = "validado"
        t = self.amb(bd)["_hall_trazabilidad"]([hall(1, severidad="repuesto", referencia="m9:rep:s301")])[1]
        self.assertTrue(t["reloj"]["primera"]["terminado"], "la gestión validó la solicitud: ya respondió")
        self.assertEqual(t["siguiente"]["accion"], "pedir", "pide 2 y hay 1 en bodega")
        paso = {p["clave"]: p for p in t["recorrido"]}
        self.assertEqual((paso["bodega"]["color"], paso["abastecimiento"]["color"]), ("ambar", "azul"))

    def test_las_solicitudes_de_un_lote_se_encuentran_por_su_prefijo_en_esa_ot(self):
        bd = Base()
        bd.lotes = [{"id": 401, "visita_id": 340, "lote_id": "ab12cd34ef56aaaa"}, {"id": 402, "visita_id": 340, "lote_id": "ab12cd34ef56aaaa"}]
        for i, est in ((401, "recibido"), (402, "instalado")):
            bd.sols[i] = {"id": i, "visita_id": 340, "estado": est, "cantidad": 1, "repuesto_nombre": "X", "repuesto_stock_id": None, "ticket_id": None,
                          "created_at": None, "validado_at": dt.datetime(2026, 10, 26, 13, 0), "pedido_at": None, "recibido_at": None, "instalado_at": None}
        t = self.amb(bd)["_hall_trazabilidad"]([hall(2, severidad="repuesto", referencia="m9:rep:ab12cd34")])[2]
        self.assertEqual(sorted(s["id"] for s in t["sols"]), [401, 402])
        self.assertTrue(any("lote_id LIKE %s" in q for q in bd.sql))

    def test_la_gestion_ligo_solicitud_ticket_y_ot_cerrada_y_todo_queda_verde(self):
        bd = Base()
        bd.extra[3] = {"entidad_id": 3, "repuesto_sol_ids": "501,502", "repuesto_lote_id": "x", "repuesto_en": dt.datetime(2026, 10, 26, 13, 0),
                       "ticket_id": 9, "valor_repuestos": 29980, "valor_servicio": 0}
        bd.actividad[3] = dt.datetime(2026, 10, 26, 12, 30)
        for i in (501, 502):
            bd.sols[i] = {"id": i, "visita_id": 340, "estado": "instalado", "cantidad": 1, "repuesto_nombre": "X", "repuesto_stock_id": 5, "ticket_id": None,
                          "created_at": None, "validado_at": None, "pedido_at": None, "recibido_at": None, "instalado_at": None, "costo_unitario": 14990}
        bd.stock[5] = (4, "A-01")
        bd.tickets[9] = {"numero_ticket": "TK-2026-00099", "estado": "closed"}
        bd.consumo = [{"visita_id": 340, "cantidad": 2, "costo_unitario": 14990}, {"visita_id": 400, "cantidad": 1, "costo_unitario": 500}]
        f = hall(3, estado="resuelto", tomado_por="Aarón", ot_seguimiento_id=400, seg_numero="OT-2026-00400", seg_estado="completada",
                 seguimiento_en=dt.datetime(2026, 10, 26, 12, 40), tomado_en=dt.datetime(2026, 10, 26, 12, 20))
        t = self.amb(bd)["_hall_trazabilidad"]([f])[3]
        self.assertEqual(t["reloj"]["semaforo"], "verde")
        self.assertEqual(t["siguiente"]["accion"], "listo")
        self.assertEqual((t["ticket"]["numero"], t["ticket"]["cerrado"]), ("TK-2026-00099", True))
        self.assertEqual(t["consumo"]["valor"], 29980)
        self.assertEqual((t["consumo_ot"]["n"], t["consumo_ot"]["valor"]), (2.0, 29980), "el consumo de la OT del hallazgo")
        self.assertEqual((t["consumo_seg"]["n"], t["consumo_seg"]["valor"]), (1.0, 500), "y el de su OT de seguimiento")
        self.assertEqual(t["valor"]["total"], "$29.980")
        self.assertTrue(all(p["color"] == "verde" for p in t["recorrido"]), [(p["clave"], p["color"]) for p in t["recorrido"]])

    def test_la_ot_de_seguimiento_cancelada_deja_el_hallazgo_otra_vez_sin_gestionar(self):
        bd = Base()
        f = hall(4, estado="resuelto", ot_seguimiento_id=400, seg_numero="OT-2026-00400", seg_estado="cancelada", seguimiento_en=dt.datetime(2026, 10, 26, 12, 40))
        t = self.amb(bd)["_hall_trazabilidad"]([f])[4]
        self.assertEqual((t["estado_ef"], t["caido"], t["seg"]), ("abierto", True, None))

    def test_si_una_consulta_falla_el_resto_sigue_y_nunca_levanta(self):
        for falla in ("FROM mant_repuestos_stock", "FROM tk_tickets", "mant_ot_repuesto_solicitudes WHERE id IN", "AS visita_id, cantidad"):
            bd = Base()
            bd.sols[301] = {"id": 301, "visita_id": 340, "estado": "pedido", "cantidad": 1, "repuesto_nombre": "X", "repuesto_stock_id": 5, "ticket_id": 9,
                            "created_at": None, "validado_at": None, "pedido_at": None, "recibido_at": None, "instalado_at": None}
            bd.falla = falla
            r = self.amb(bd)["_hall_trazabilidad"]([hall(1, severidad="repuesto", referencia="m9:rep:s301")])
            self.assertIn(1, r, falla)
            self.assertEqual(len(r[1]["recorrido"]), 7, falla)

    def test_una_solicitud_rechazada_no_cuenta_como_que_la_gestion_actuo(self):
        # Revisión 27-oct: rechazar no limpia validado_at; el hallazgo quedaba «con salida» y el plazo dejaba de correr.
        bd = Base()
        bd.sols[301] = {"id": 301, "visita_id": 340, "estado": "rechazado", "cantidad": 1, "repuesto_nombre": "X", "repuesto_stock_id": None, "ticket_id": None,
                        "created_at": None, "validado_at": dt.datetime(2026, 10, 26, 13, 0), "pedido_at": None, "recibido_at": None, "instalado_at": None}
        bd.extra[1] = {"entidad_id": 1, "repuesto_sol_ids": "301", "repuesto_lote_id": "x", "repuesto_en": dt.datetime(2026, 10, 26, 13, 0)}
        t = self.amb(bd)["_hall_trazabilidad"]([hall(1, severidad="falla", detectado_en=dt.datetime(2026, 10, 22, 9, 0))])[1]
        self.assertFalse(t["reloj"]["primera"]["terminado"], "todas sus solicitudes se rechazaron: nadie actuó")
        self.assertEqual((t["reloj"]["semaforo"], t["reloj"]["vencida"]), ("rojo", True))
        # Con una viva, sí cuenta.
        bd.sols[302] = dict(bd.sols[301], id=302, estado="validado", validado_at=dt.datetime(2026, 10, 22, 10, 0))
        bd.extra[1]["repuesto_sol_ids"] = "301,302"
        t = self.amb(bd)["_hall_trazabilidad"]([hall(1, severidad="falla", detectado_en=dt.datetime(2026, 10, 22, 9, 0))])[1]
        self.assertTrue(t["reloj"]["primera"]["terminado"])

    def test_si_la_ot_de_seguimiento_cayo_el_plazo_vuelve_a_correr_desde_el_reporte(self):
        # Revisión 27-oct: seguimiento_en y tomado_en seguían puestos y el hallazgo salía verde («Con salida») para siempre.
        bd = Base()
        f = hall(5, severidad="falla", estado="resuelto", ot_seguimiento_id=400, seg_numero="OT-2026-00400", seg_estado="anulada",
                 detectado_en=dt.datetime(2026, 10, 1, 9, 0), tomado_por="Aarón", tomado_en=dt.datetime(2026, 10, 1, 10, 0),
                 seguimiento_en=dt.datetime(2026, 10, 1, 11, 0))
        t = self.amb(bd)["_hall_trazabilidad"]([f])[5]
        self.assertEqual((t["estado_ef"], t["caido"]), ("abierto", True))
        self.assertEqual((t["reloj"]["semaforo"], t["reloj"]["vencida"]), ("rojo", True))
        self.assertIn("Sin tomar", t["reloj"]["pill"])
        # Un descartado que se reabrió tampoco arrastra su «tomado_en» viejo.
        r = self.amb(bd)["_hall_trazabilidad"]([hall(6, severidad="falla", estado="abierto", tomado_en=dt.datetime(2026, 10, 1, 10, 0),
                                                     detectado_en=dt.datetime(2026, 10, 1, 9, 0))])[6]
        self.assertTrue(r["reloj"]["vencida"])

    def test_el_consumo_se_atribuye_a_la_ot_generada_como_en_el_resultado_financiero(self):
        bd = Base()
        bd.consumo = [{"visita_id": 400, "cantidad": 1, "costo_unitario": 700}]
        self.amb(bd)["_hall_trazabilidad"]([hall(7, ot_seguimiento_id=400, seg_numero="OT-2026-00400", seg_estado="en_curso", estado="resuelto")])
        q = [s for s in bd.sql if "estado='instalado'" in s][0]
        self.assertIn("COALESCE(ot_generada_id, visita_id)", q, "la misma regla de _ot_repuestos_desglose")

    def test_el_stock_trae_lo_comprometido_por_otras_solicitudes(self):
        bd = Base()
        bd.sols[301] = {"id": 301, "visita_id": 340, "estado": "solicitado", "cantidad": 2, "repuesto_nombre": "X", "repuesto_stock_id": 5, "ticket_id": None,
                        "created_at": None, "validado_at": None, "pedido_at": None, "recibido_at": None, "instalado_at": None}
        bd.stock[5] = (3, "A-01", 2)
        t = self.amb(bd)["_hall_trazabilidad"]([hall(1, severidad="repuesto", referencia="m9:rep:s301")])[1]
        paso = {p["clave"]: p for p in t["recorrido"]}
        self.assertEqual(paso["bodega"]["txt"], "Sin stock suficiente", "hay 3 físicos pero 2 ya están prometidos a otras")
        self.assertTrue(any("comprometido" in s for s in bd.sql))

    def test_sin_filas_o_sin_id_no_consulta_nada(self):
        bd = Base()
        a = self.amb(bd)["_hall_trazabilidad"]
        self.assertEqual(a([]), {})
        self.assertEqual(a([{"visita_id": 1}]), {})
        self.assertEqual(bd.sql, [])

    def test_solo_lectura_y_los_marcadores_cuadran(self):
        bd = Base()
        bd.extra[1] = {"entidad_id": 1, "repuesto_sol_ids": "7", "ticket_id": 9}
        bd.sols[7] = {"id": 7, "visita_id": 340, "estado": "pedido", "cantidad": 1, "repuesto_nombre": "X", "repuesto_stock_id": 5, "ticket_id": None,
                      "created_at": None, "validado_at": None, "pedido_at": None, "recibido_at": None, "instalado_at": None}
        bd.stock[5] = (1, None)
        self.amb(bd)["_hall_trazabilidad"]([hall(1, referencia="m9:rep:ab12cd34", severidad="repuesto"), hall(2)])
        self.assertTrue(bd.sql)       # Base.fetchall ya falla si alguna consulta no es SELECT o si los %s no cuadran


class TestVencidos(unittest.TestCase):
    def test_solo_cuenta_los_que_pasaron_su_plazo_y_lee_solo_los_en_marcha(self):
        filas = [hall(1), hall(2, severidad="alerta", detectado_en=dt.datetime(2026, 10, 26, 14, 30))]
        trazas = {1: {"reloj": {"vencida": True}}, 2: {"reloj": {"vencida": False}}}
        consultas = []

        def fetchall(sql, params=()):
            consultas.append((" ".join(sql.split()), tuple(params)))
            return [dict(f) for f in filas]
        amb = cargar(["_hall_vencidos_ids"], [], {"print": lambda *a, **k: None, "_hall_ok": lambda: True, "_oh": H, "mysql_fetchall": fetchall,
                                                  "_hall_trazabilidad": lambda f: trazas})
        self.assertEqual(amb["_hall_vencidos_ids"](), {1})
        sql, par = consultas[0]
        self.assertIn("h.estado='tomado'", sql, "los que tienen el caso tomado también corren (su salida)")
        self.assertIn("LIMIT %s", sql)
        self.assertEqual(par, (1500,))
        self.assertNotIn("descartado", sql.split("WHERE")[1].split("ORDER")[0].replace("h.estado='abierto'", ""), "los descartados no corren")

    def test_si_la_base_falla_devuelve_vacio_y_si_no_hay_tabla_ni_consulta(self):
        def rota(*a, **k):
            raise RuntimeError("x")
        amb = cargar(["_hall_vencidos_ids"], [], {"print": lambda *a, **k: None, "_hall_ok": lambda: True, "_oh": H, "mysql_fetchall": rota, "_hall_trazabilidad": lambda f: {}})
        self.assertEqual(amb["_hall_vencidos_ids"](), set())
        amb = cargar(["_hall_vencidos_ids"], [], {"print": lambda *a, **k: None, "_hall_ok": lambda: False, "_oh": H, "mysql_fetchall": rota, "_hall_trazabilidad": lambda f: {}})
        self.assertEqual(amb["_hall_vencidos_ids"](), set())


# ═══════════════════════════ C) cableado ═══════════════════════════
class TestCableado(unittest.TestCase):
    def test_la_tarjeta_de_gestion_usa_el_mismo_recorrido_y_plazo(self):
        src = _fuente_funcion("_gc_enriquecer_hallazgos")
        self.assertIn("_hall_trazabilidad(filas)", src)
        for k in ('i["recorrido"] = t_["recorrido"]', 'i["reloj"] = t_["reloj"]', 'i["siguiente"] = t_["siguiente"]'):
            self.assertIn(k, src)
        self.assertIn("_gc_enriquecer_hallazgos(vid, v, _hall_items(vid, filas_h), filas_h)", _fuente_funcion("_hall_contexto_ficha"))
        tpl = open(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "templates", "ot2", "_hallazgos.html"), encoding="utf-8").read()
        for k in ("i.reloj.pill", "i.siguiente.txt", "hz-plazo", "Ahora toca"):
            self.assertIn(k, tpl, k)

    def test_los_correos_y_avisos_no_cambian_con_esto(self):
        # Esto SOLO se muestra: no escribe, no avisa y no envía nada (REGLA #22).
        import hallazgos_sla
        self.assertNotRegex(open(hallazgos_sla.__file__, encoding="utf-8").read(), r"mysql|_send_ilus|requests\.|smtp")

    def test_el_modulo_reusa_la_jornada_de_retiros_e_inquietudes(self):
        src = open(HS.__file__, encoding="utf-8").read()
        self.assertIn("import repuestos_respuesta as _rr", src)
        self.assertIn("_iq._reloj_tramo", src, "UNA sola definición de hora hábil en todo el proyecto")


if __name__ == "__main__":
    unittest.main()

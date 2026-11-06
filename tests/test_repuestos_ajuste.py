# -*- coding: utf-8 -*-
"""🔒 6-nov-2026 — AJUSTAR LA CANTIDAD de un repuesto que ya existe, en los DOS sentidos (rebajar / aumentar), con el documento de Random
OPCIONAL y el superadmin reconocido.

Daniel (dictado mirando la pantalla de Repuestos): «Además de rebajar, para modificar las cantidades puede ser que no esté amarrado a una
factura, pero necesitan mi autorización; y cuando soy yo, reconóceme para que no me pidas autorización».

  A. La lógica pura (repuestos_rebajas.py): tipos, documento opcional, límites, constancia del kardex, textos.
  B. Las rutas REALES de app.py (extraídas con tests/_fuente_codigo.py, sin importar app.py) contra una base de mentira: pedir una rebaja o un
     aumento con y sin documento, el superadmin que se aplica en el acto, el que no lo es y queda pendiente, resolver, el saldo que cambia
     entre el pedido y la decisión, el técnico externo, la columna `tipo` que todavía no existe y el arranque barato (REGLA #18).
  C. La pantalla: textos, sin popups nativos, y en un Chromium real a 390 / 820 / 1280 px (REGLA #61: sin scroll de lado, letra ≥ 12 px,
     toques ≥ 44 px, datos completos).
Todo con datos de mentira (REGLA #22): ni un repuesto, ni un correo, ni una conexión real.
Correr:  python -m pytest tests/test_repuestos_ajuste.py -q -p no:warnings"""
import json
import os
import re
import sys
import types
import unittest
from functools import wraps

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, RAIZ)

import repuestos_rebajas as rr  # noqa: E402
from tests import _fuente_codigo as FC  # noqa: E402

ARG = "Se rompió en la instalación del cliente y quedó constatado por el técnico de turno."


def _leer(*ruta):
    with open(os.path.join(RAIZ, *ruta), encoding="utf-8") as fh:
        return fh.read().replace("\r\n", "\n")


PANE = _leer("templates", "mantenciones", "_repuestos_bodega_pane.html")
CLAUDE_MD = _leer("CLAUDE.md")


# ══════════════════════════════════════════════════════════════════════
#  A. LÓGICA PURA
# ══════════════════════════════════════════════════════════════════════
class TestTipos(unittest.TestCase):
    def test_los_dos_tipos_y_el_defecto(self):
        self.assertEqual(rr.TIPOS, ("rebaja", "aumento"))
        self.assertEqual(rr.normalizar_tipo("Aumento"), "aumento")
        self.assertEqual(rr.normalizar_tipo(" REBAJA "), "rebaja")
        # Una pantalla vieja (sin recargar) no manda tipo: es una rebaja, que es lo único que conocía.
        self.assertEqual(rr.normalizar_tipo(None), "rebaja")
        self.assertEqual(rr.normalizar_tipo(""), "rebaja")
        self.assertIsNone(rr.normalizar_tipo("regalo"))
        self.assertIsNone(rr.normalizar_tipo("", defecto=None))

    def test_el_delta_lleva_el_signo_del_tipo(self):
        self.assertEqual(rr.delta_con_signo("rebaja", 3), -3)
        self.assertEqual(rr.delta_con_signo("aumento", 3), 3)
        self.assertEqual(rr.delta_con_signo("aumento", "2,5"), 2.5)

    def test_el_kardex_distingue_salida_y_entrada(self):
        self.assertEqual(rr.MOVIMIENTO["rebaja"], ("salida", "rebaja_autorizada"))
        self.assertEqual(rr.MOVIMIENTO["aumento"], ("entrada", "ajuste_autorizado"))

    def test_etiquetas(self):
        self.assertEqual(rr.etiqueta_tipo("rebaja"), "Rebaja")
        self.assertEqual(rr.etiqueta_tipo("aumento"), "Aumento")


class TestDocumentoOpcional(unittest.TestCase):
    def test_ambos_vacios_es_sin_documento(self):
        for t, n in (("", ""), (None, None), ("  ", " "), ("", None)):
            self.assertEqual(rr.evaluar_doc(t, n), ("", "", None), (t, n))
            self.assertFalse(rr.hay_documento(t, n))

    def test_un_documento_completo_se_normaliza(self):
        self.assertEqual(rr.evaluar_doc(" gdv ", " 34190 "), ("GDV", "34190", None))
        self.assertTrue(rr.hay_documento("fcv", "88"))

    def test_si_se_escribe_algo_debe_formar_un_documento_valido(self):
        t, n, err = rr.evaluar_doc("GDV", "")
        self.assertIn("número", err)
        self.assertEqual((t, n), ("", ""))
        self.assertIn("tipo", rr.evaluar_doc("", "34190")[2])
        self.assertIn("no es válido", rr.evaluar_doc("G;DROP", "1")[2])
        self.assertIn("no es válido", rr.evaluar_doc("GDV", "34 190 !")[2])
        self.assertFalse(rr.hay_documento("GDV", ""))

    def test_etiqueta_completa(self):
        self.assertEqual(rr.etiqueta_doc_completa("GDV", "34190"), "Guía de despacho (GDV) 34190")
        self.assertEqual(rr.etiqueta_doc_completa("", ""), "Sin documento de Random")
        self.assertEqual(rr.etiqueta_doc_completa("GDV", ""), "Sin documento de Random")

    def test_validar_pedido_acepta_sin_documento_pero_no_sin_argumento(self):
        for tipo in rr.TIPOS:
            self.assertIsNone(rr.validar_pedido(5, 2, ARG, "", "", tipo))
            self.assertIn("al menos 30", rr.validar_pedido(5, 2, "corto", "", "", tipo))
            self.assertIn("al menos 30", rr.validar_pedido(5, 2, "", "GDV", "1", tipo))


class TestLimites(unittest.TestCase):
    def test_rebaja_no_pasa_el_saldo(self):
        self.assertIn("No se puede rebajar", rr.validar_pedido(5, 5.5, ARG, "", "", "rebaja"))
        self.assertIsNone(rr.validar_pedido(5, 5, ARG, "", "", "rebaja"))

    def test_aumento_tiene_tope_para_un_cero_de_mas(self):
        self.assertEqual(rr.AUMENTO_MAX, 100000)
        self.assertIsNone(rr.validar_pedido(5, 100000, ARG, "", "", "aumento"))
        err = rr.validar_pedido(5, 100001, ARG, "", "", "aumento")
        self.assertIn("100.000", err)
        self.assertIn("cero", err)

    def test_el_aumento_no_depende_del_saldo(self):
        self.assertIsNone(rr.validar_pedido(0, 40, ARG, "", "", "aumento"))
        self.assertIsNone(rr.validar_pedido(None, 40, ARG, "", "", "aumento"))

    def test_cantidad_invalida_en_los_dos_sentidos(self):
        self.assertIn("se rebajan", rr.validar_pedido(5, 0, ARG, "", "", "rebaja"))
        self.assertIn("se aumentan", rr.validar_pedido(5, -2, ARG, "", "", "aumento"))
        self.assertIn("se aumentan", rr.validar_pedido(5, "abc", ARG, "", "", "aumento"))

    def test_tipo_desconocido(self):
        self.assertIn("rebaja o un aumento", rr.validar_pedido(5, 1, ARG, "", "", "regalo"))

    def test_al_aplicar_cada_tipo_vuelve_a_mirar_lo_suyo(self):
        self.assertEqual(rr.puede_aplicarse(5, 2, "rebaja"), (True, ""))
        self.assertFalse(rr.puede_aplicarse(1, 2, "rebaja")[0])
        self.assertEqual(rr.puede_aplicarse(1, 50, "aumento"), (True, ""))      # un aumento no se frena por el saldo
        self.assertFalse(rr.puede_aplicarse(1, 100001, "aumento")[0])
        self.assertFalse(rr.puede_aplicarse(5, 0, "aumento")[0])
        self.assertFalse(rr.puede_aplicarse(5, 1, "regalo")[0])

    def test_solo_el_superadmin_se_aplica_en_el_acto(self):
        self.assertTrue(rr.aplica_en_el_acto(True))
        self.assertFalse(rr.aplica_en_el_acto(False))
        self.assertFalse(rr.aplica_en_el_acto(None))


class TestConstancia(unittest.TestCase):
    def test_nota_de_una_rebaja_con_documento(self):
        n = rr.nota_kardex(7, "Lenin", "Daniel", "GDV", "34190", ARG)
        self.assertTrue(n.startswith("Rebaja #7 pedida por Lenin, autorizada por Daniel"))
        self.assertIn("doc. GDV 34190", n)

    def test_nota_de_un_aumento_sin_documento_lo_dice_claro(self):
        n = rr.nota_kardex(8, "Lenin", "Daniel", "", "", ARG, tipo="aumento")
        self.assertTrue(n.startswith("Aumento #8 pedido por Lenin, autorizado por Daniel"))
        self.assertIn("SIN documento de Random", n)
        self.assertNotIn("doc. ", n)

    def test_nota_del_superadmin_dice_hecho_por_el_con_su_cargo(self):
        a = rr.nota_kardex(9, "Daniel", "Daniel", "", "", ARG, auto_autorizada=True, tipo="aumento")
        self.assertTrue(a.startswith("Aumento #9 hecho por Daniel (superadmin)"), a)
        r = rr.nota_kardex(10, "Daniel", "Daniel", "FCV", "5", ARG, auto_autorizada=True, tipo="rebaja")
        self.assertTrue(r.startswith("Rebaja #10 hecha por Daniel (superadmin)"), r)

    def test_la_nota_nunca_pasa_de_500(self):
        n = rr.nota_kardex(1, "A" * 120, "B" * 120, "GDV", "1", "x" * 900, tipo="aumento")
        self.assertEqual(len(n), rr.NOTA_KARDEX_MAX)
        self.assertTrue(n.endswith("…"))

    def test_resolucion_del_superadmin(self):
        self.assertEqual(rr.resolucion_auto("aumento", "Daniel", "", ""), "Aumento hecho por Daniel (superadmin), sin documento de Random.")
        self.assertEqual(rr.resolucion_auto("rebaja", "Daniel", "GDV", "1"), "Rebaja hecha por Daniel (superadmin), con documento de Random.")

    def test_aviso_al_superadmin_dice_el_tipo_y_si_hay_documento(self):
        self.assertEqual(rr.resumen_pedido("rebaja", 2, "REP-1", "gdv", "34190"), "pide rebajar 2 u. de REP-1 (GDV 34190)")
        self.assertEqual(rr.resumen_pedido("aumento", 5, "REP-1", "", ""), "pide aumentar 5 u. de REP-1 (sin documento)")


# ══════════════════════════════════════════════════════════════════════
#  B. RUTAS REALES DE app.py CONTRA UN MUNDO DE MENTIRA
# ══════════════════════════════════════════════════════════════════════
FUNCIONES = ("_repreb_fila", "_repreb_col_tipo_ok", "_repreb_ok", "_repreb_aplicar", "_repreb_notificar", "repreb_pedir", "repreb_listar",
             "repreb_resolver", "_repstock_mover", "_ensure_repuestos_rebajas")


class _Cursor:
    def __init__(self, m):
        self.m = m
        self.fila = None
        self.lastrowid = None

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def fetchone(self):
        return self.fila

    def execute(self, sql, params=()):
        s = " ".join(sql.split())
        m = self.m
        self.fila = None
        if s.startswith("SELECT * FROM mant_repuestos_rebajas WHERE id=%s FOR UPDATE"):
            self.fila = m._fila_rebaja(params[0])
        elif s.startswith("SELECT cantidad, activo FROM mant_repuestos_stock WHERE id=%s FOR UPDATE"):
            r = m.stock.get(params[0])
            self.fila = {"cantidad": r["cantidad"], "activo": r["activo"]} if r else None
        elif s.startswith("SELECT cantidad FROM mant_repuestos_stock WHERE id=%s FOR UPDATE"):
            r = m.stock.get(params[0])
            self.fila = {"cantidad": r["cantidad"]} if r else None
        elif s.startswith("UPDATE mant_repuestos_stock SET cantidad=%s WHERE id=%s"):
            m.stock[params[1]]["cantidad"] = params[0]
        elif s.startswith("INSERT INTO mant_repuestos_movimientos"):
            cols = ("repuesto_id", "tipo", "motivo_tipo", "cantidad", "saldo_resultante", "solicitud_id", "visita_id", "cliente_id", "nota", "usuario")
            m.movs.append(dict(zip(cols, params)))
        elif s.startswith("UPDATE mant_repuestos_rebajas SET estado='aprobada'"):
            autoriza, resolucion, saldo, bid = params
            r = m.rebajas[bid]
            r.update(estado="aprobada", resuelto_por=autoriza, resolucion=resolucion, saldo_resultante=saldo, resuelto_at="2026-11-06 11:00")
        elif s.startswith("INSERT INTO mant_repuestos_rebajas"):
            cols = [c.strip() for c in s[s.index("(") + 1:s.index(")")].split(",")]
            if "tipo" in cols and not m.tiene_col:
                raise RuntimeError("Unknown column 'tipo' in 'field list'")
            fila = dict(zip(cols, params))
            fila.update(id=m.sig, estado="pendiente", pedido_at="2026-11-06 10:00", resuelto_por=None, resuelto_at=None, resolucion=None,
                        saldo_resultante=None)
            if m.tiene_col:
                fila.setdefault("tipo", "rebaja")
            m.rebajas[m.sig] = fila
            self.lastrowid = m.sig
            m.sig += 1
        else:
            raise AssertionError("cursor: SQL no previsto: " + s)


class _Conn:
    def __init__(self, m):
        self.m = m

    def cursor(self):
        return _Cursor(self.m)

    def commit(self):
        self.m.commits += 1

    def rollback(self):
        self.m.rollbacks += 1


class Mundo:
    """El mundo de mentira de una sola petición: la bodega (un repuesto), las solicitudes, el kardex y las campanas."""

    def __init__(self, usuario="Lenin", superadmin=False, saldo=5, tiene_col=True, erp="ok"):
        self.usuario = usuario
        self.tiene_col = tiene_col
        self.tabla = True
        self.erp = erp
        self.stock = {7: {"id": 7, "sku": "REP-7", "descripcion": "Correa de trote de 3 mm con tensor y soporte lateral derecho", "cantidad": saldo, "activo": 1}}
        self.rebajas = {}
        self.sig = 1
        self.movs, self.logs, self.notifs, self.erp_llamadas, self.salida = [], [], [], [], []
        self.commits = self.rollbacks = 0
        self.supers = [1, 2]
        self.g = types.SimpleNamespace(permissions={"superadmin": superadmin}, user={"id": 22, "role": "supervisor"})
        self.body = {}
        self.request = types.SimpleNamespace(get_json=lambda silent=False: self.body, args={}, headers={}, path="/mantenciones/api/x", is_json=True)
        self.conn = _Conn(self)
        amb = {
            "_repreb": rr, "print": lambda *a, **k: self.salida.append(" ".join(str(x) for x in a)),
            "mysql_fetchone": self._fetchone, "mysql_fetchall": self._fetchall, "mysql_execute": self._execute,
            "mysql_execute_returning_rowcount": self._rowcount, "get_db": lambda: self.conn,
            "jsonify": lambda d: d, "request": self.request, "g": self.g, "current_username": lambda: self.usuario,
            "_mant_log": lambda ent, eid, acc, det="": self.logs.append((ent, eid, acc, det)),
            "_mant_notificar": lambda uid, tipo, titulo, cuerpo="", url="", prioridad="media": self.notifs.append(
                {"uid": uid, "tipo": tipo, "titulo": titulo, "cuerpo": cuerpo, "url": url, "prioridad": prioridad}),
            "_erp_doc_lookup": self._erp, "chile_fmt_filter": lambda v: str(v),
        }
        FC.extraer(amb, "app.py", FUNCIONES, ("_repreb_tiene_tipo",))
        self.amb = amb
        self.f = types.SimpleNamespace(**{k: amb[k] for k in FUNCIONES})

    # ── identidad y petición ──
    def como(self, usuario, superadmin):
        self.usuario = usuario
        self.g.permissions["superadmin"] = superadmin
        return self

    def llamar(self, fn, *args, body=None, **query):
        self.body = body if body is not None else {}
        self.request.args = query
        r = fn(*args)
        return (r[0], r[1]) if isinstance(r, tuple) else (r, 200)

    def pedir(self, tipo="rebaja", cantidad=2, argumento=ARG, tido="GDV", nudo="34190", rid=7, con_tipo=True):
        cuerpo = {"cantidad": cantidad, "argumento": argumento, "tido": tido, "nudo": nudo}
        if con_tipo:
            cuerpo["tipo"] = tipo
        return self.llamar(self.f.repreb_pedir, rid, body=cuerpo)

    def resolver(self, bid, accion, comentario=""):
        return self.llamar(self.f.repreb_resolver, bid, body={"accion": accion, "comentario": comentario})

    # ── SQL de mentira ──
    def _fila_rebaja(self, bid):
        r = self.rebajas.get(bid)
        if not r:
            return None
        r = dict(r)
        if not self.tiene_col:
            r.pop("tipo", None)
        return r

    def _fetchone(self, sql, params=()):
        s = " ".join(sql.split())
        if "information_schema.TABLES" in s:
            return {"x": 1} if self.tabla else None
        if "information_schema.COLUMNS" in s:
            return {"x": 1} if self.tiene_col else None
        if s.startswith("SELECT id, sku, descripcion, cantidad, activo FROM mant_repuestos_stock WHERE id=%s"):
            r = self.stock.get(params[0])
            return dict(r) if r else None
        if s.startswith("SELECT id FROM mant_repuestos_rebajas WHERE repuesto_id=%s AND estado='pendiente'"):
            return next(({"id": r["id"]} for r in self.rebajas.values() if r["repuesto_id"] == params[0] and r["estado"] == "pendiente"), None)
        if s.startswith("SELECT * FROM mant_repuestos_rebajas WHERE id=%s"):
            return self._fila_rebaja(params[0])
        if s.startswith("SELECT COUNT(*) AS n FROM mant_repuestos_rebajas WHERE estado='pendiente'"):
            filas = [r for r in self.rebajas.values() if r["estado"] == "pendiente"]
            if "AND pedido_por=%s" in s:
                filas = [r for r in filas if r["pedido_por"] == params[0]]
            return {"n": len(filas)}
        raise AssertionError("fetchone no previsto: " + s)

    def _fetchall(self, sql, params=()):
        s = " ".join(sql.split())
        if s.startswith("SELECT id FROM app_users WHERE active=1 AND role='superadmin'"):
            return [{"id": i} for i in self.supers]
        if s.startswith("SELECT * FROM mant_repuestos_rebajas"):
            p = list(params)
            filas = sorted(self.rebajas.values(), key=lambda r: -r["id"])
            if "estado=%s" in s:
                est = p.pop(0)
                filas = [r for r in filas if r["estado"] == est]
            if "pedido_por=%s" in s:
                quien = p.pop(0)
                filas = [r for r in filas if r["pedido_por"] == quien]
            out = []
            for r in filas:
                r = dict(r)
                if not self.tiene_col:
                    r.pop("tipo", None)
                out.append(r)
            return out
        raise AssertionError("fetchall no previsto: " + s)

    def _execute(self, sql, params=()):
        s = " ".join(sql.split())
        if s.startswith("UPDATE mant_repuestos_rebajas SET estado='anulada', resolucion=%s, resuelto_por=%s"):
            texto, quien, bid = params
            if self.rebajas[bid]["estado"] == "pendiente":
                self.rebajas[bid].update(estado="anulada", resolucion=texto, resuelto_por=quien)
            return
        if s.startswith(("CREATE TABLE", "ALTER TABLE")):
            raise AssertionError("las rutas no deben ejecutar DDL: " + s)
        raise AssertionError("execute no previsto: " + s)

    def _rowcount(self, sql, params=()):
        s = " ".join(sql.split())
        nuevo = re.search(r"SET estado='(\w+)'", s).group(1)
        quien, texto, bid = params
        r = self.rebajas.get(bid)
        if not r or r["estado"] != "pendiente":
            return 0
        r.update(estado=nuevo, resuelto_por=quien, resolucion=texto, resuelto_at="2026-11-06 11:00")
        return 1

    def _erp(self, tido, nudo):
        self.erp_llamadas.append((tido, nudo))
        if self.erp == "ok":
            return {"hdr": {"cliente_nombre": "Gimnasio Demo SpA"}}, "api", True, None
        if self.erp == "no_existe":
            return None, None, True, None
        return None, None, False, "timeout"

    # ── lectura rápida ──
    @property
    def saldo(self):
        return self.stock[7]["cantidad"]


class TestPedirRebajaYAumento(unittest.TestCase):
    def test_una_rebaja_con_documento_de_quien_no_es_superadmin_queda_pendiente(self):
        m = Mundo()
        p, st = m.pedir("rebaja", 2, tido="gdv", nudo="34190")
        self.assertEqual(st, 200, p)
        self.assertEqual(p["estado"], "pendiente")
        self.assertEqual(p["tipo"], "rebaja")
        self.assertFalse(p["sin_documento"])
        self.assertEqual(m.saldo, 5, "el stock no cambia hasta que el superadmin autorice")
        self.assertEqual(m.movs, [])
        self.assertEqual(m.erp_llamadas, [("GDV", "34190")], "con documento se comprueba en Random (solo lectura)")
        fila = m.rebajas[p["id"]]
        self.assertEqual((fila["tipo"], fila["doc_tido"], fila["doc_nudo"], fila["doc_cliente"]), ("rebaja", "GDV", "34190", "Gimnasio Demo SpA"))
        self.assertEqual(sorted(n["uid"] for n in m.notifs), [1, 2], "se avisa a cada superadmin")
        self.assertIn("pide rebajar 2 u. de REP-7 (GDV 34190)", m.notifs[0]["cuerpo"])
        self.assertEqual(m.notifs[0]["url"], "/repuestos?rb_ajustes=1#bodega")

    def test_un_aumento_con_documento_queda_pendiente_y_no_mueve_nada(self):
        m = Mundo()
        p, st = m.pedir("aumento", 4, tido="GRC", nudo="991")
        self.assertEqual((st, p["estado"], p["tipo"]), (200, "pendiente", "aumento"))
        self.assertEqual(m.saldo, 5)
        self.assertEqual(m.rebajas[p["id"]]["tipo"], "aumento")
        self.assertIn("pide aumentar 4 u. de REP-7 (GRC 991)", m.notifs[0]["cuerpo"])
        self.assertEqual(m.logs[0][2], "aumento_pedido")

    def test_sin_documento_se_puede_pedir_pero_queda_pendiente_y_no_consulta_random(self):
        for tipo in ("rebaja", "aumento"):
            m = Mundo()
            p, st = m.pedir(tipo, 2, tido="", nudo="")
            self.assertEqual((st, p["estado"], p["sin_documento"]), (200, "pendiente", True), (tipo, p))
            self.assertEqual(m.erp_llamadas, [], "sin documento no hay nada que comprobar en Random")
            self.assertEqual(m.saldo, 5)
            fila = m.rebajas[p["id"]]
            self.assertIsNone(fila["doc_tido"])
            self.assertIsNone(fila["doc_nudo"])
            self.assertIn("sin documento de Random", p["mensaje"])
            self.assertIn("pide", m.notifs[0]["cuerpo"])
            self.assertIn("(sin documento)", m.notifs[0]["cuerpo"])

    def test_sin_documento_sigue_exigiendo_el_argumento(self):
        m = Mundo()
        p, st = m.pedir("aumento", 2, argumento="porque sí", tido="", nudo="")
        self.assertEqual(st, 400)
        self.assertIn("al menos 30", p["error"])
        self.assertEqual(m.rebajas, {})

    def test_un_documento_a_medias_se_rechaza(self):
        m = Mundo()
        for tido, nudo in (("GDV", ""), ("", "34190"), ("G;DROP", "1")):
            p, st = m.pedir("rebaja", 1, tido=tido, nudo=nudo)
            self.assertEqual(st, 400, (tido, nudo))
            self.assertIn("documento", p["error"])
        self.assertEqual(m.rebajas, {})
        self.assertEqual(m.erp_llamadas, [])

    def test_un_documento_que_no_existe_en_random_se_rechaza_pero_sin_documento_si_pasa(self):
        m = Mundo(erp="no_existe")
        p, st = m.pedir("rebaja", 1, tido="FCV", nudo="999999")
        self.assertEqual(st, 400)
        self.assertIn("no existe en Random", p["error"])
        p, st = m.pedir("rebaja", 1, tido="", nudo="")
        self.assertEqual(st, 200)

    def test_random_caido_no_culpa_a_quien_pide_y_le_ofrece_la_otra_via(self):
        m = Mundo(erp="no_responde")
        p, st = m.pedir("rebaja", 1, tido="FCV", nudo="12")
        self.assertEqual(st, 503)
        self.assertIn("No se pudo consultar Random", p["error"])
        self.assertIn("sin documento", p["error"])
        self.assertEqual(m.rebajas, {})

    def test_limites_de_cada_sentido(self):
        m = Mundo(saldo=5)
        self.assertEqual(m.pedir("rebaja", 6, tido="", nudo="")[1], 400)
        self.assertEqual(m.pedir("rebaja", 0, tido="", nudo="")[1], 400)
        p, st = m.pedir("aumento", 100001, tido="", nudo="")
        self.assertEqual(st, 400)
        self.assertIn("100.000", p["error"])
        self.assertEqual(m.pedir("aumento", 100000, tido="", nudo="")[1], 200)
        self.assertEqual(m.rebajas[1]["cantidad"], 100000)

    def test_tipo_invalido_y_tipo_ausente_es_rebaja(self):
        m = Mundo()
        p, st = m.pedir("regalo", 1, tido="", nudo="")
        self.assertEqual(st, 400)
        self.assertIn("rebaja o un aumento", p["error"])
        p, st = m.pedir(con_tipo=False, cantidad=1, tido="", nudo="")          # una pantalla vieja sin recargar
        self.assertEqual((st, p["tipo"]), (200, "rebaja"))

    def test_una_pendiente_por_repuesto_para_quien_no_es_superadmin(self):
        m = Mundo()
        self.assertEqual(m.pedir("rebaja", 1, tido="", nudo="")[1], 200)
        p, st = m.pedir("aumento", 1, tido="", nudo="")
        self.assertEqual(st, 409)
        self.assertIn("ajuste esperando autorización", p["error"])
        self.assertEqual(len(m.rebajas), 1)

    def test_repuesto_inexistente_o_apagado(self):
        m = Mundo()
        self.assertEqual(m.pedir("rebaja", 1, tido="", nudo="", rid=99)[1], 404)
        m.stock[7]["activo"] = 0
        self.assertEqual(m.pedir("rebaja", 1, tido="", nudo="")[1], 404)

    def test_la_funcion_dormida_responde_503_en_vez_de_romper(self):
        m = Mundo()
        m.tabla = False
        p, st = m.pedir("rebaja", 1, tido="", nudo="")
        self.assertEqual(st, 503)


class TestSuperadminSeReconoce(unittest.TestCase):
    def test_rebaja_con_documento_se_aplica_en_el_acto(self):
        m = Mundo("Daniel", superadmin=True, saldo=5)
        p, st = m.pedir("rebaja", 2, tido="GDV", nudo="34190")
        self.assertEqual((st, p["estado"], p["saldo"]), (200, "aprobada", 3))
        self.assertEqual(m.saldo, 3)
        mov = m.movs[0]
        self.assertEqual((mov["tipo"], mov["motivo_tipo"], mov["cantidad"], mov["saldo_resultante"], mov["usuario"]),
                         ("salida", "rebaja_autorizada", -2, 3, "Daniel"))
        self.assertTrue(mov["nota"].startswith("Rebaja #1 hecha por Daniel (superadmin)"), mov["nota"])
        self.assertIn("doc. GDV 34190", mov["nota"])
        self.assertEqual(m.notifs, [], "al superadmin no se le pide autorización: no se avisa a nadie")
        fila = m.rebajas[1]
        self.assertEqual((fila["estado"], fila["resuelto_por"], fila["saldo_resultante"]), ("aprobada", "Daniel", 3))
        self.assertIn("Daniel (superadmin)", fila["resolucion"])
        self.assertIn("Rebaja aplicada por Daniel (superadmin)", p["mensaje"])

    def test_aumento_sin_documento_se_aplica_en_el_acto_y_la_constancia_lo_dice(self):
        m = Mundo("Daniel", superadmin=True, saldo=5)
        p, st = m.pedir("aumento", 10, tido="", nudo="")
        self.assertEqual((st, p["estado"], p["saldo"], p["sin_documento"]), (200, "aprobada", 15, True))
        self.assertEqual(m.saldo, 15)
        mov = m.movs[0]
        self.assertEqual((mov["tipo"], mov["motivo_tipo"], mov["cantidad"], mov["saldo_resultante"]), ("entrada", "ajuste_autorizado", 10, 15))
        self.assertTrue(mov["nota"].startswith("Aumento #1 hecho por Daniel (superadmin)"), mov["nota"])
        self.assertIn("SIN documento de Random", mov["nota"])
        self.assertEqual(m.erp_llamadas, [])
        self.assertIn("sin documento de Random", m.rebajas[1]["resolucion"])
        self.assertEqual(m.logs[-1][2], "aumento_aprobado")

    def test_rebaja_sin_documento_se_aplica_en_el_acto(self):
        m = Mundo("Daniel", superadmin=True, saldo=5)
        p, st = m.pedir("rebaja", 5, tido="", nudo="")
        self.assertEqual((st, p["estado"], m.saldo), (200, "aprobada", 0))
        self.assertIn("SIN documento de Random", m.movs[0]["nota"])

    def test_el_superadmin_tambien_debe_dar_argumento(self):
        m = Mundo("Daniel", superadmin=True)
        p, st = m.pedir("aumento", 3, argumento="cuenta física", tido="", nudo="")
        self.assertEqual(st, 400)
        self.assertEqual(m.saldo, 5)
        self.assertEqual(m.movs, [])

    def test_el_superadmin_sigue_validando_el_documento_si_lo_escribe(self):
        m = Mundo("Daniel", superadmin=True, erp="no_existe")
        p, st = m.pedir("rebaja", 1, tido="FCV", nudo="1")
        self.assertEqual(st, 400)
        self.assertEqual(m.saldo, 5)

    def test_la_rebaja_del_superadmin_que_ya_no_cabe_se_anula_con_el_motivo(self):
        m = Mundo("Daniel", superadmin=True, saldo=5)
        # entre la validación y la aplicación otro movimiento dejó solo 1: la aplicación vuelve a mirar y NO deja el stock en negativo.
        orig = m.amb["_repreb_aplicar"]

        def con_carrera(*a, **k):
            m.stock[7]["cantidad"] = 1
            return orig(*a, **k)
        m.amb["repreb_pedir"].__globals__["_repreb_aplicar"] = con_carrera
        p, st = m.pedir("rebaja", 4, tido="", nudo="")
        self.assertEqual(st, 409)
        self.assertIn("saldo cambió", p["error"])
        self.assertEqual(m.saldo, 1)
        self.assertEqual(m.rebajas[1]["estado"], "anulada")
        self.assertEqual(m.movs, [])

    def test_el_superadmin_no_queda_trabado_por_una_pendiente_ajena(self):
        m = Mundo("Lenin")
        m.pedir("rebaja", 1, tido="", nudo="")
        m.como("Daniel", True)
        p, st = m.pedir("aumento", 2, tido="", nudo="")
        self.assertEqual((st, p["estado"]), (200, "aprobada"))


class TestSinLaColumnaTipo(unittest.TestCase):
    """Mientras el arranque no haya creado la columna `tipo`, todo sigue como la rebaja de siempre y un aumento espera (jamás al revés)."""

    def test_la_rebaja_de_siempre_funciona_sin_la_columna(self):
        m = Mundo(tiene_col=False)
        p, st = m.pedir("rebaja", 2, tido="GDV", nudo="1")
        self.assertEqual((st, p["estado"]), (200, "pendiente"))
        self.assertNotIn("tipo", m.rebajas[1], "el INSERT no menciona la columna que aún no existe")
        m.como("Daniel", True)
        r, st = m.resolver(1, "aprobar")
        self.assertEqual((st, r["estado"], m.saldo), (200, "aprobada", 3))
        self.assertEqual(m.movs[0]["motivo_tipo"], "rebaja_autorizada")

    def test_un_aumento_se_rechaza_con_un_aviso_amable_y_no_se_guarda_nada(self):
        m = Mundo("Daniel", superadmin=True, tiene_col=False)
        p, st = m.pedir("aumento", 2, tido="", nudo="")
        self.assertEqual(st, 503)
        self.assertIn("se está preparando", p["error"])
        self.assertEqual(m.rebajas, {})
        self.assertEqual(m.saldo, 5)

    def test_el_listado_trata_toda_fila_vieja_como_rebaja(self):
        m = Mundo(tiene_col=False)
        m.pedir("rebaja", 2, tido="GDV", nudo="1")
        d, st = m.llamar(m.f.repreb_listar)
        self.assertEqual(d["rebajas"][0]["tipo"], "rebaja")
        self.assertEqual(d["rebajas"][0]["tipo_etiqueta"], "Rebaja")

    def test_la_columna_se_recuerda_una_vez_creada(self):
        m = Mundo(tiene_col=False)
        self.assertFalse(m.f._repreb_col_tipo_ok())
        m.tiene_col = True
        self.assertTrue(m.f._repreb_col_tipo_ok())
        m.tiene_col = False                                  # ya no se vuelve a preguntar en este proceso
        self.assertTrue(m.f._repreb_col_tipo_ok())


class TestResolver(unittest.TestCase):
    def _pendiente(self, tipo="rebaja", cantidad=2, tido="GDV", nudo="34190", saldo=5):
        m = Mundo("Lenin", saldo=saldo)
        p, st = m.pedir(tipo, cantidad, tido=tido, nudo=nudo)
        self.assertEqual((st, p["estado"]), (200, "pendiente"), p)
        m.notifs.clear()
        return m, p["id"]

    def test_aprobar_una_rebaja_baja_el_stock_y_deja_la_constancia(self):
        m, bid = self._pendiente("rebaja", 2)
        m.como("Daniel", True)
        r, st = m.resolver(bid, "aprobar")
        self.assertEqual((st, r["estado"], r["saldo"], r["tipo"]), (200, "aprobada", 3, "rebaja"))
        mov = m.movs[0]
        self.assertEqual((mov["tipo"], mov["motivo_tipo"], mov["cantidad"], mov["usuario"]), ("salida", "rebaja_autorizada", -2, "Daniel"))
        self.assertTrue(mov["nota"].startswith("Rebaja #1 pedida por Lenin, autorizada por Daniel"), mov["nota"])
        self.assertEqual(m.rebajas[bid]["resolucion"], "Autorizado por Daniel (superadmin).")
        self.assertEqual(m.notifs[0]["titulo"], "Rebaja de repuesto autorizada")

    def test_aprobar_un_aumento_sube_el_stock_como_entrada(self):
        m, bid = self._pendiente("aumento", 7, tido="", nudo="")
        m.como("Daniel", True)
        r, st = m.resolver(bid, "aprobar")
        self.assertEqual((st, r["saldo"], r["tipo"]), (200, 12, "aumento"))
        mov = m.movs[0]
        self.assertEqual((mov["tipo"], mov["motivo_tipo"], mov["cantidad"]), ("entrada", "ajuste_autorizado", 7))
        self.assertTrue(mov["nota"].startswith("Aumento #1 pedido por Lenin, autorizado por Daniel"), mov["nota"])
        self.assertIn("SIN documento de Random", mov["nota"])
        self.assertEqual(m.notifs[0]["titulo"], "Aumento de repuesto autorizado")
        self.assertEqual(m.logs[-1][2], "aumento_aprobado")

    def test_rechazar_exige_motivo_y_no_toca_el_stock(self):
        m, bid = self._pendiente("aumento", 7)
        m.como("Daniel", True)
        r, st = m.resolver(bid, "rechazar", "no")
        self.assertEqual(st, 400)
        self.assertIn("motivo", r["error"])
        r, st = m.resolver(bid, "rechazar", "falta la guía de recepción")
        self.assertEqual((st, r["estado"]), (200, "rechazada"))
        self.assertEqual(m.saldo, 5)
        self.assertEqual(m.movs, [])
        self.assertEqual(m.rebajas[bid]["resolucion"], "falta la guía de recepción")
        self.assertEqual(m.notifs[0]["titulo"], "Aumento de repuesto rechazado")

    def test_solo_el_superadmin_autoriza_o_rechaza(self):
        m, bid = self._pendiente()
        for accion in ("aprobar", "rechazar"):
            r, st = m.resolver(bid, accion, "no corresponde, falta la factura")
            self.assertEqual(st, 403, accion)
        self.assertEqual(m.saldo, 5)
        self.assertEqual(m.rebajas[bid]["estado"], "pendiente")

    def test_quien_pidio_puede_anular_la_suya_y_otro_no(self):
        m, bid = self._pendiente()
        m.como("Pedro", False)
        r, st = m.resolver(bid, "anular")
        self.assertEqual(st, 403)
        m.como("Lenin", False)
        r, st = m.resolver(bid, "anular")
        self.assertEqual((st, r["estado"]), (200, "anulada"))
        r, st = m.resolver(bid, "anular")
        self.assertEqual(st, 409, "una solicitud ya resuelta no se anula otra vez")
        self.assertEqual(m.saldo, 5)

    def test_aprobar_dos_veces_mueve_el_stock_una_sola_vez(self):
        m, bid = self._pendiente("rebaja", 2)
        m.como("Daniel", True)
        self.assertEqual(m.resolver(bid, "aprobar")[1], 200)
        r, st = m.resolver(bid, "aprobar")
        self.assertEqual(st, 409)
        self.assertIn("ya está aprobada", r["error"])
        self.assertEqual(m.saldo, 3)
        self.assertEqual(len(m.movs), 1)

    def test_rechazar_una_ya_aprobada_no_miente(self):
        m, bid = self._pendiente()
        m.como("Daniel", True)
        m.resolver(bid, "aprobar")
        r, st = m.resolver(bid, "rechazar", "me equivoqué de solicitud")
        self.assertEqual(st, 409)
        self.assertEqual(m.rebajas[bid]["estado"], "aprobada")

    def test_si_el_saldo_cambio_la_rebaja_no_se_aplica_pero_el_aumento_si(self):
        m, bid = self._pendiente("rebaja", 4, saldo=5)
        m.stock[7]["cantidad"] = 1                              # otra salida (una instalación) bajó el saldo mientras esperaba
        m.como("Daniel", True)
        r, st = m.resolver(bid, "aprobar")
        self.assertEqual(st, 409)
        self.assertIn("saldo cambió", r["error"])
        self.assertEqual(m.saldo, 1)
        self.assertEqual(m.movs, [])
        self.assertEqual(m.rebajas[bid]["estado"], "pendiente", "queda pendiente para rechazarla con su motivo")
        m2, bid2 = self._pendiente("aumento", 4, saldo=5)
        m2.stock[7]["cantidad"] = 1
        m2.como("Daniel", True)
        r, st = m2.resolver(bid2, "aprobar")
        self.assertEqual((st, r["saldo"]), (200, 5))

    def test_el_repuesto_apagado_no_se_ajusta(self):
        m, bid = self._pendiente("aumento", 2)
        m.stock[7]["activo"] = 0
        m.como("Daniel", True)
        r, st = m.resolver(bid, "aprobar")
        self.assertEqual(st, 409)
        self.assertIn("ya no está activo", r["error"])

    def test_solicitud_inexistente_y_accion_invalida(self):
        m, bid = self._pendiente()
        m.como("Daniel", True)
        self.assertEqual(m.resolver(99, "aprobar")[1], 404)
        r, st = m.resolver(bid, "borrar")
        self.assertEqual(st, 400)


class TestListado(unittest.TestCase):
    def test_el_superadmin_ve_todas_y_cada_fila_trae_su_tipo_y_si_va_sin_documento(self):
        m = Mundo("Lenin")
        m.pedir("rebaja", 1, tido="GDV", nudo="5")
        m.stock[7]["cantidad"] = 5
        m.rebajas[1]["estado"] = "aprobada"                     # para poder pedir otra
        m.pedir("aumento", 3, tido="", nudo="")
        m.como("Daniel", True)
        d, st = m.llamar(m.f.repreb_listar, estado="todas")
        self.assertEqual(st, 200)
        self.assertTrue(d["es_superadmin"])
        por_id = {f["id"]: f for f in d["rebajas"]}
        self.assertEqual((por_id[1]["tipo"], por_id[1]["tipo_etiqueta"], por_id[1]["sin_documento"]), ("rebaja", "Rebaja", False))
        self.assertEqual(por_id[1]["doc_etiqueta"], "Guía de despacho (GDV) 5")
        self.assertEqual((por_id[2]["tipo"], por_id[2]["tipo_etiqueta"], por_id[2]["sin_documento"]), ("aumento", "Aumento", True))
        self.assertEqual(por_id[2]["doc_etiqueta"], "Sin documento de Random")
        self.assertEqual(d["pendientes"], 1)

    def test_los_demas_solo_ven_los_suyos(self):
        m = Mundo("Lenin")
        m.pedir("rebaja", 1, tido="", nudo="")
        m.como("Pedro", False)
        d, st = m.llamar(m.f.repreb_listar)
        self.assertEqual((d["rebajas"], d["pendientes"], d["es_superadmin"]), ([], 0, False))


class TestTecnicoExterno(unittest.TestCase):
    """La puerta REAL de las tres rutas (@_no_tecnico_externo) frente a un técnico externo y a uno interno."""

    def _puerta(self, rol):
        amb = {"wraps": wraps, "request": types.SimpleNamespace(headers={}, path="/mantenciones/api/repuestos-stock/7/rebaja", is_json=True),
               "g": types.SimpleNamespace(user={"role": rol}), "jsonify": lambda d: d, "flash": lambda *a, **k: None,
               "redirect": lambda u: ("redirect", u), "url_for": lambda n: "/" + n}
        FC.extraer(amb, "app.py", ("_es_tecnico_externo", "_no_tecnico_externo"))
        llamadas = []
        vista = amb["_no_tecnico_externo"](lambda: llamadas.append(1) or {"ok": True})
        return vista(), llamadas

    def test_el_tecnico_externo_no_entra(self):
        r, llamadas = self._puerta("tecnico_externo")
        self.assertEqual(r[1], 403)
        self.assertEqual(r[0]["error_codigo"], "PROVEEDOR_SIN_ACCESO")
        self.assertEqual(llamadas, [], "la ruta ni se ejecuta")

    def test_el_tecnico_interno_y_la_gestion_si(self):
        for rol in ("tecnico", "supervisor", "superadmin"):
            r, llamadas = self._puerta(rol)
            self.assertEqual(r, {"ok": True}, rol)
            self.assertEqual(llamadas, [1])


# ══════════════════════════════════════════════════════════════════════
#  ARRANQUE BARATO (REGLA #18)
# ══════════════════════════════════════════════════════════════════════
CONST_GUARDIA = ("_DDL_RE", "_DDLG_MULTI", "_DDLG_CREATE_TBL", "_DDLG_ADD_IDX", "_DDLG_CREATE_IDX", "_DDLG_ADD_COL", "_DDLG_MOD_ENUM",
                 "_DDLG_ENUM_VAL", "_DDLG_NO_COLUMNA", "_ddl_guard_saltados")


class TestArranqueBarato(unittest.TestCase):
    def _ensure(self, falla_create=False):
        sentencias, lecturas = [], []
        amb = {"print": lambda *a, **k: None}

        def ejecutar(sql, params=None):
            sentencias.append(" ".join(sql.split()))
            if falla_create and sentencias[-1].startswith("CREATE"):
                raise RuntimeError("sin conexión")
        amb["mysql_execute"] = ejecutar
        amb["mysql_fetchone"] = lambda s, p=None: lecturas.append(s)
        FC.extraer(amb, "app.py", ("_ensure_repuestos_rebajas",))
        return amb["_ensure_repuestos_rebajas"](), sentencias, lecturas

    def test_son_dos_sentencias_de_una_clausula_y_ninguna_lectura(self):
        ok, sentencias, lecturas = self._ensure()
        self.assertTrue(ok)
        self.assertEqual(len(sentencias), 2)
        self.assertTrue(sentencias[0].startswith("CREATE TABLE IF NOT EXISTS mant_repuestos_rebajas ("))
        self.assertIn("tipo VARCHAR(10) NOT NULL DEFAULT 'rebaja'", sentencias[0], "una base nueva ya nace con la columna")
        self.assertEqual(sentencias[1], "ALTER TABLE mant_repuestos_rebajas ADD COLUMN tipo VARCHAR(10) NOT NULL DEFAULT 'rebaja' AFTER repuesto_id")
        self.assertEqual(lecturas, [], "el arranque no lee ni bloquea nada por su cuenta: la guarda de mysql_execute decide")

    def test_si_la_tabla_no_se_pudo_crear_no_se_intenta_el_alter(self):
        ok, sentencias, _ = self._ensure(falla_create=True)
        self.assertFalse(ok)
        self.assertEqual(len(sentencias), 1)

    def test_la_guarda_real_salta_lo_ya_aplicado_sin_pedir_lock(self):
        amb = {"re": re}
        FC.extraer(amb, "app.py", ("_ddl_ya_aplicado",), CONST_GUARDIA)
        guarda = amb["_ddl_ya_aplicado"]
        _, sentencias, _ = self._ensure()
        consultas = []

        def ya_existe(sql, params):
            consultas.append((" ".join(sql.split()), params))
            return {"x": 1}
        self.assertTrue(guarda(ya_existe, sentencias[0]), "el CREATE IF NOT EXISTS se salta si la tabla existe")
        self.assertTrue(guarda(ya_existe, sentencias[1]), "el ADD COLUMN se salta si la columna ya está")
        self.assertIn("information_schema.COLUMNS", consultas[1][0])
        self.assertEqual(consultas[1][1], ("mant_repuestos_rebajas", "tipo"))

        def no_existe(sql, params):
            return None
        self.assertFalse(guarda(no_existe, sentencias[1]), "si la columna falta, el ALTER corre")

    def test_la_sentencia_es_de_una_sola_clausula(self):
        amb = {"re": re}
        FC.extraer(amb, "app.py", (), CONST_GUARDIA)
        _, sentencias, _ = self._ensure()
        self.assertIsNone(amb["_DDLG_MULTI"].search(sentencias[1]), "ADD COLUMN a, ADD COLUMN b no se puede verificar (REGLA #18)")


# ══════════════════════════════════════════════════════════════════════
#  C. PANTALLA
# ══════════════════════════════════════════════════════════════════════
def _bloque_js():
    return PANE[PANE.index("const RB_AJUSTE_TIPOS_DOC"):PANE.index("function rbCantidadCambio(){")]


class TestPantallaTextos(unittest.TestCase):
    def test_solicitar_rebaja_pasa_a_ajustar_cantidad_y_no_se_quita_nada(self):
        self.assertIn(">Ajustar cantidad", PANE.replace("\n", ""))
        self.assertIn('class="dropdown-item" onclick=\'rbAjusteAbrir(', PANE)
        self.assertIn("rbmAbrir(", PANE)                       # Movimientos sigue en el menú de cada fila
        self.assertIn("rbImprimirUno(", PANE)                  # Imprimir etiqueta también
        self.assertIn("rbEliminar(", PANE)

    def test_selector_grande_rebajar_aumentar(self):
        js = _bloque_js()
        self.assertIn("<strong>Rebajar</strong>", js)
        self.assertIn("<strong>Aumentar</strong>", js)
        self.assertIn("rbAjusteTipo('rebaja')", js)
        self.assertIn("rbAjusteTipo('aumento')", js)
        self.assertRegex(PANE, r"\.rb-aj-tipo\{[^}]*min-height:56px")

    def test_el_documento_es_opcional_y_la_nota_es_clara(self):
        js = _bloque_js()
        self.assertIn('<option value="">Sin documento</option>', js)
        self.assertIn("(opcional)", js)
        self.assertIn("Sin documento también se puede pedir, pero necesita la autorización del superadmin", js)

    def test_el_superadmin_ve_que_se_le_reconoce(self):
        js = _bloque_js()
        self.assertIn("Como superadmin se aplica en el acto", js)
        self.assertIn("tu nombre queda en la constancia", js)
        self.assertIn("Aplicar ahora", js)
        self.assertIn("RB_ES_SUPERADMIN", js)
        self.assertIn("{% if permissions.superadmin %}", PANE)  # la tarjeta del modal de edición también lo dice

    def test_el_argumento_tiene_contador(self):
        js = _bloque_js()
        self.assertIn("id=\"rbAjusteContador\"", js)
        self.assertIn("caracteres mínimos", js)

    def test_nada_de_popups_nativos(self):
        js = _bloque_js()
        self.assertNotRegex(js, r"(?<![\w.])(alert|confirm|prompt)\(")
        for helper in ("ilusToast", "ilusAlert", "ilusConfirm", "ilusPrompt"):
            self.assertIn(helper, js)

    def test_la_bandeja_muestra_tipo_y_sin_documento_y_el_kardex_los_nombra(self):
        js = _bloque_js()
        self.assertIn("▲ AUMENTO", js)
        self.assertIn("▼ REBAJA", js)
        self.assertIn("SIN DOCUMENTO DE RANDOM", js)
        self.assertIn("rebaja_autorizada: 'Rebaja autorizada'", PANE)
        self.assertIn("ajuste_autorizado: 'Aumento autorizado'", PANE)

    def test_la_bandeja_se_abre_sola_desde_el_aviso_de_la_campana(self):
        self.assertIn("rb_ajustes", _bloque_js())

    def test_letra_y_botones_de_la_regla_61_en_el_css_nuevo(self):
        css = PANE[PANE.index("modal «Ajustar cantidad»"):PANE.index("@media (max-width:420px){.rb-aj-tipo")]
        for tam in re.findall(r"font-size:([\d.]+)rem", css):
            self.assertGreaterEqual(float(tam) * 16, 12, "letra menor a 12 px en " + tam)


# ──────────────── Chromium real: 390 / 820 / 1280, con datos de mentira ────────────────
try:
    from playwright.sync_api import sync_playwright
except Exception:                                   # pragma: no cover
    sync_playwright = None

BOOTSTRAP = os.path.join(os.environ.get("TEMP") or os.environ.get("TMPDIR") or "/tmp", "ilus_bootstrap_5.3.3.min.css")


def _bootstrap_css():
    if os.path.exists(BOOTSTRAP) and os.path.getsize(BOOTSTRAP) > 100000:
        return open(BOOTSTRAP, "rb").read().decode("utf-8")
    try:
        import urllib.request
        datos = urllib.request.urlopen("https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css", timeout=30).read()
        open(BOOTSTRAP, "wb").write(datos)
        return datos.decode("utf-8")
    except Exception:
        return None


LISTA_BANDEJA = {
    "ok": True, "pendientes": 3, "es_superadmin": True, "rebajas": [
        {"id": 1, "tipo": "aumento", "tipo_etiqueta": "Aumento", "estado": "pendiente", "sku": "REP-ROD-12-CON-GUIA",
         "descripcion": "Rodamiento de rueda trasera de caminadora comercial con eje reforzado y guía lateral izquierda de 12 mm",
         "cantidad": 4, "saldo_al_pedir": 1, "saldo_resultante": None, "argumento": ARG, "sin_documento": True, "doc_etiqueta": "Sin documento de Random",
         "doc_cliente": None, "pedido_por": "Lenin Urbina", "pedido_at": "06/11/2026 10:05", "resuelto_por": None, "resuelto_at": "", "resolucion": None},
        {"id": 2, "tipo": "rebaja", "tipo_etiqueta": "Rebaja", "estado": "pendiente", "sku": "REP-2", "descripcion": "Correa de trote",
         "cantidad": 2, "saldo_al_pedir": 5, "saldo_resultante": None, "argumento": ARG, "sin_documento": False,
         "doc_etiqueta": "Guía de despacho (GDV) 34190", "doc_cliente": "Gimnasio Fit Paradise Sociedad por Acciones Limitada de Las Condes",
         "pedido_por": "Dave Rojas", "pedido_at": "06/11/2026 10:12", "resuelto_por": None, "resuelto_at": "", "resolucion": None},
    ]}

STUBS = r"""
window.__calls = []; window.__toasts = []; window.__alerts = []; window.__errores = [];
window.__resp = {ok: true, estado: 'pendiente', mensaje: 'Pedido enviado. El stock no cambia hasta que el superadmin lo autorice.'};
window.__lista = %s;
window.addEventListener('error', function(e){ window.__errores.push(String(e.message)); });
window.fetch = function(u, o){
  var cuerpo = o && o.body ? JSON.parse(o.body) : null;
  window.__calls.push({u: String(u), b: cuerpo});
  var r = (!o && String(u).indexOf('repuestos-rebajas') >= 0) ? window.__lista : window.__resp;
  return Promise.resolve({ok: true, json: function(){ return Promise.resolve(r); }});
};
window.bootstrap = {Modal: function(el){ this.show = function(){ el.classList.add('show'); el.style.display = 'block'; };
                                           this.hide = function(){ el.classList.remove('show'); el.style.display = 'none'; }; }};
window.bootstrap.Modal.getOrCreateInstance = function(el){ return el.__m || (el.__m = new window.bootstrap.Modal(el)); };
window.ilusToast = function(m){ window.__toasts.push(String(m)); };
window.ilusAlert = function(o){ window.__alerts.push(o); return Promise.resolve(true); };
window.ilusConfirm = function(o){ window.__confirms = (window.__confirms || []).concat([o]); return Promise.resolve(true); };
window.ilusPrompt = function(o){ return Promise.resolve('falta la guía de despacho'); };
"""


def _pagina(superadmin):
    css = _bootstrap_css()
    regla_req = re.search(r"\.rb-req\{[^}]*\}", PANE).group(0)
    nuevo = PANE[PANE.index("/* 🔒 2026-11-06 — modal «Ajustar cantidad»"):]
    nuevo = nuevo[:nuevo.index("@media (max-width:420px){.rb-aj-tipo")]
    media = PANE[PANE.index("@media (max-width:420px){.rb-aj-tipo"):]
    media = media[:media.index("}}") + 2]
    return ("<!doctype html><html lang='es'><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'>"
            "<style>" + css + "</style><style>" + regla_req + "\n" + nuevo + "\n" + media + "</style></head><body>"
            "<div class='container-fluid p-3'><div class='d-flex gap-2 flex-wrap'>"
            "<button id='rbAjustesBtn' class='btn btn-sm btn-outline-dark' type='button' style='min-height:44px' onclick='rbAjustesAbrir()'>"
            "Ajustes <span id='rbAjustesN' class='badge bg-danger ms-1' style='display:none'>0</span></button></div></div>"
            "<input type='hidden' id='rbId' value='7'><input type='hidden' id='rbSku' value='REP-7'><input type='hidden' id='rbDescripcion' value='Correa'>"
            "<script>" + (STUBS % json.dumps(LISTA_BANDEJA)) + "</script>"
            "<script>const RB_ES_SUPERADMIN = " + ("true" if superadmin else "false") + "; let RB_CANTIDAD_ORIGINAL = 5;</script>"
            "<script>" + _bloque_js() + "</script></body></html>")


MEDIR = r"""(sel) => {
  const raiz = document.querySelector(sel), ancho = window.innerWidth;
  const out = {scroll: document.documentElement.scrollWidth - document.documentElement.clientWidth, fuera: [], chicas: [], bajos: [], cortados: [], errores: window.__errores || []};
  raiz.querySelectorAll('*').forEach(e => { const r = e.getBoundingClientRect(); if (!r.width) return;
    if (r.right > ancho + 0.5 || r.left < -0.5) out.fuera.push(e.tagName + '.' + String(e.className).slice(0, 30) + ' ' + Math.round(r.left) + '-' + Math.round(r.right)); });
  const w = document.createTreeWalker(raiz, NodeFilter.SHOW_TEXT);
  while (w.nextNode()) { const t = w.currentNode, el = t.parentElement; if (!t.textContent.trim() || !el || el.closest('.btn-close,option,script,style')) continue;
    const r = el.getBoundingClientRect(); if (!r.width || !r.height) continue;
    const fs = parseFloat(getComputedStyle(el).fontSize); if (fs < 11.99) out.chicas.push(t.textContent.trim().slice(0, 24) + '|' + fs.toFixed(1)); }
  raiz.querySelectorAll('button:not(.btn-close), select, input:not([type=hidden]), textarea').forEach(e => { const r = e.getBoundingClientRect();
    if (r.width && r.height < 43.5) out.bajos.push(e.tagName + '#' + e.id + ' ' + Math.round(r.height)); });
  raiz.querySelectorAll('#rbAjusteRes, .rb-aj-tipo span, .rb-aj-help, .rb-aj-aviso, .rb-ajl-card, .rb-ajl-card div').forEach(e => {
    if (e.scrollWidth > e.clientWidth + 1) out.cortados.push(String(e.id || e.className) + ' ' + e.scrollWidth + '>' + e.clientWidth); });
  return out; }"""


@unittest.skipIf(sync_playwright is None, "Playwright no está instalado")
class TestNavegadorReal(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if _bootstrap_css() is None:
            raise unittest.SkipTest("sin red para bajar Bootstrap")
        cls._pw = sync_playwright().start()
        try:
            cls.nav = cls._pw.chromium.launch()
        except Exception as e:
            cls._pw.stop()
            raise unittest.SkipTest("Chromium no disponible: %s" % e)
        cls.paginas = {}

    @classmethod
    def tearDownClass(cls):
        try:
            cls.nav.close()
        finally:
            cls._pw.stop()

    def abrir(self, superadmin=False, ancho=1280):
        ctx = self.nav.new_context(viewport={"width": ancho, "height": 900})
        ctx.set_default_timeout(8000)
        pg = ctx.new_page()
        errores = []
        pg.on("pageerror", lambda e: errores.append(str(e)))
        pg.set_content(_pagina(superadmin))
        self.addCleanup(ctx.close)
        pg.__errores = errores
        return pg

    def abrir_modal(self, superadmin=False, ancho=1280):
        pg = self.abrir(superadmin, ancho)
        pg.evaluate("rbAjusteAbrir(7, 'REP-7', 'Correa de trote de 3 mm con tensor y soporte lateral derecho para caminadora comercial', 5)")
        pg.wait_for_selector("#modalAjuste.show")
        return pg

    def llenar(self, pg, cant="3", arg=ARG, tido="", nudo=""):
        pg.fill("#rbAjusteCant", cant)
        pg.fill("#rbAjusteArg", arg)
        if tido:
            pg.select_option("#rbAjusteTido", tido)
            pg.fill("#rbAjusteNudo", nudo)

    def test_el_js_real_arranca_sin_errores_y_abre_el_modal(self):
        pg = self.abrir_modal()
        self.assertEqual(pg.__errores, [])
        self.assertEqual(pg.inner_text("#modalAjuste .modal-title").strip(), "Ajustar cantidad")
        self.assertIn("En bodega hoy: 5 u.", pg.inner_text("#rbAjusteRes"))

    def test_el_selector_cambia_entre_rebajar_y_aumentar(self):
        pg = self.abrir_modal()
        self.assertEqual(pg.get_attribute("#rbAjusteTipoRebaja", "aria-checked"), "true")
        self.assertIn("a rebajar", pg.inner_text("#rbAjusteCantVerbo"))
        pg.fill("#rbAjusteCant", "9")
        self.assertIn("Solo hay 5 en bodega", pg.inner_text("#rbAjusteQuedan"))
        pg.click("#rbAjusteTipoAumento")
        self.assertEqual(pg.get_attribute("#rbAjusteTipoAumento", "aria-checked"), "true")
        self.assertEqual(pg.get_attribute("#rbAjusteTipoRebaja", "aria-checked"), "false")
        self.assertIn("a aumentar", pg.inner_text("#rbAjusteCantVerbo"))
        self.assertIn("Quedarían 14 en bodega", pg.inner_text("#rbAjusteQuedan"))
        pg.fill("#rbAjusteCant", "100001")
        self.assertIn("100.000", pg.inner_text("#rbAjusteQuedan"))
        pg.click("#rbAjusteTipoRebaja")
        pg.fill("#rbAjusteCant", "2")
        self.assertIn("Quedarían 3 en bodega", pg.inner_text("#rbAjusteQuedan"))

    def test_un_repuesto_sin_stock_parte_en_aumentar(self):
        pg = self.abrir()
        pg.evaluate("rbAjusteAbrir(7, 'REP-0', 'Repuesto sin stock', 0)")
        pg.wait_for_selector("#modalAjuste.show")
        self.assertEqual(pg.get_attribute("#rbAjusteTipoAumento", "aria-checked"), "true")
        self.assertIn("a aumentar", pg.inner_text("#rbAjusteCantVerbo"))

    def test_el_documento_es_opcional_el_numero_se_habilita_al_elegir_tipo(self):
        pg = self.abrir_modal()
        self.assertTrue(pg.is_disabled("#rbAjusteNudo"))
        self.assertIn("necesita la autorización del superadmin", pg.inner_text("#rbAjusteDocAyuda"))
        pg.select_option("#rbAjusteTido", "GDV")
        self.assertFalse(pg.is_disabled("#rbAjusteNudo"))
        pg.fill("#rbAjusteNudo", "34190")
        pg.select_option("#rbAjusteTido", "")
        self.assertTrue(pg.is_disabled("#rbAjusteNudo"))
        self.assertEqual(pg.input_value("#rbAjusteNudo"), "", "al volver a «Sin documento» el número se limpia")

    def test_enviar_sin_documento_manda_el_tipo_y_el_documento_vacio(self):
        pg = self.abrir_modal()
        pg.click("#rbAjusteTipoAumento")
        self.llenar(pg, "3")
        pg.click("#rbAjusteEnviar")
        pg.wait_for_function("window.__alerts.length > 0")
        llamada = pg.evaluate("window.__calls.filter(c => c.b)[0]")
        self.assertEqual(llamada["u"], "/mantenciones/api/repuestos-stock/7/rebaja")
        self.assertEqual(llamada["b"]["tipo"], "aumento")
        self.assertEqual(llamada["b"]["cantidad"], 3)
        self.assertEqual((llamada["b"]["tido"], llamada["b"]["nudo"]), ("", ""))
        self.assertEqual(pg.evaluate("window.__alerts[0].title"), "Pedido enviado")

    def test_enviar_con_documento_lo_manda(self):
        pg = self.abrir_modal()
        self.llenar(pg, "2", tido="GDV", nudo="34190")
        pg.click("#rbAjusteEnviar")
        pg.wait_for_function("window.__alerts.length > 0")
        b = pg.evaluate("window.__calls.filter(c => c.b)[0].b")
        self.assertEqual((b["tipo"], b["cantidad"], b["tido"], b["nudo"]), ("rebaja", 2, "GDV", "34190"))

    def test_las_validaciones_avisan_sin_popups_y_no_envian(self):
        pg = self.abrir_modal()
        pg.click("#rbAjusteEnviar")                                   # sin cantidad
        pg.fill("#rbAjusteCant", "2")
        pg.click("#rbAjusteEnviar")                                   # sin argumento
        pg.fill("#rbAjusteArg", ARG)
        pg.select_option("#rbAjusteTido", "FCV")
        pg.click("#rbAjusteEnviar")                                   # tipo de documento sin número
        toasts = pg.evaluate("window.__toasts")
        self.assertEqual(len(toasts), 3, toasts)
        self.assertIn("cuántas unidades", toasts[0])
        self.assertIn("30 caracteres", toasts[1])
        self.assertIn("número del documento", toasts[2])
        self.assertEqual(pg.evaluate("window.__calls.filter(c => c.b).length"), 0)

    def test_el_superadmin_ve_que_se_aplica_en_el_acto(self):
        pg = self.abrir_modal(superadmin=True)
        self.assertIn("Como superadmin se aplica en el acto", pg.inner_text("#rbAjusteAviso"))
        self.assertIn("Aplicar ahora", pg.inner_text("#rbAjusteEnviar"))
        self.assertIn("is-super", pg.get_attribute("#rbAjusteAviso", "class"))
        self.assertIn("queda anotado así", pg.inner_text("#rbAjusteDocAyuda"))
        pg = self.abrir_modal(superadmin=False)
        self.assertIn("hasta que el superadmin lo autorice", pg.inner_text("#rbAjusteAviso"))
        self.assertIn("Enviar pedido", pg.inner_text("#rbAjusteEnviar"))

    def test_regla_61_el_modal_en_tres_anchos(self):
        for superadmin in (False, True):
            for ancho in (390, 820, 1280):
                pg = self.abrir_modal(superadmin, ancho)
                pg.click("#rbAjusteTipoAumento")
                pg.fill("#rbAjusteCant", "3")
                pg.fill("#rbAjusteArg", ARG)
                pg.select_option("#rbAjusteTido", "GRC")
                m = pg.evaluate(MEDIR, "#modalAjuste .modal-content")
                e = "a %d px (superadmin=%s)" % (ancho, superadmin)
                self.assertEqual(m["errores"], [], e)
                self.assertLessEqual(m["scroll"], 0, "scroll de lado " + e)
                self.assertEqual(m["fuera"], [], "algo se sale de la pantalla " + e)
                self.assertEqual(m["chicas"], [], "letra menor a 12 px " + e)
                self.assertEqual(m["bajos"], [], "toques menores a 44 px " + e)
                self.assertEqual(m["cortados"], [], "datos cortados " + e)
                self.assertTrue(pg.is_visible("#rbAjusteEnviar"), "el botón de enviar se ve " + e)

    def test_el_nombre_largo_del_repuesto_se_ve_completo(self):
        pg = self.abrir_modal(ancho=390)
        texto = pg.inner_text("#rbAjusteRes")
        self.assertIn("soporte lateral derecho para caminadora comercial", texto)

    def test_la_bandeja_muestra_tipo_documento_y_acciones(self):
        pg = self.abrir(superadmin=True)
        pg.click("#rbAjustesBtn")
        pg.wait_for_selector("#rbAjustesLista .rb-ajl-card")
        tarjetas = pg.inner_text("#rbAjustesLista")
        self.assertIn("▲ AUMENTO", tarjetas)
        self.assertIn("▼ REBAJA", tarjetas)
        self.assertIn("SIN DOCUMENTO DE RANDOM", tarjetas)
        self.assertIn("Guía de despacho (GDV) 34190", tarjetas)
        self.assertIn("Aumentar 4 u. (había 1)", tarjetas)
        self.assertIn("Rebajar 2 u. (había 5)", tarjetas)
        self.assertIn("Gimnasio Fit Paradise Sociedad por Acciones Limitada de Las Condes", tarjetas, "los datos completos, sin cortar")
        self.assertEqual(pg.locator("#rbAjustesLista button:has-text('Autorizar')").count(), 2)
        self.assertEqual(pg.locator("#rbAjustesN").inner_text(), "3")

    def test_la_bandeja_en_tres_anchos(self):
        for ancho in (390, 820, 1280):
            pg = self.abrir(superadmin=True, ancho=ancho)
            pg.click("#rbAjustesBtn")
            pg.wait_for_selector("#rbAjustesLista .p-3")
            m = pg.evaluate(MEDIR, "#modalAjustes .modal-content")
            e = "a %d px" % ancho
            self.assertEqual(m["errores"], [], e)
            self.assertLessEqual(m["scroll"], 0, "scroll de lado " + e)
            self.assertEqual(m["fuera"], [], "algo se sale de la pantalla " + e)
            self.assertEqual(m["chicas"], [], "letra menor a 12 px " + e)
            self.assertEqual(m["bajos"], [], "toques menores a 44 px " + e)

    def test_autorizar_un_aumento_sin_documento_advierte_al_superadmin(self):
        pg = self.abrir(superadmin=True)
        pg.click("#rbAjustesBtn")
        pg.wait_for_selector("#rbAjustesLista .p-3")
        pg.evaluate("window.__lista = {ok: true, pendientes: 0, es_superadmin: true, rebajas: []}")
        pg.click("#rbAjustesLista .p-3:first-child button:has-text('Autorizar')")
        pg.wait_for_function("window.__confirms && window.__confirms.length > 0")
        msg = pg.evaluate("window.__confirms[0].message")
        self.assertIn("Se suma al stock ahora", msg)
        self.assertIn("SIN documento de Random", msg)
        pg.wait_for_function("window.__calls.some(c => c.b && c.u.indexOf('resolver') >= 0)")
        llamada = pg.evaluate("window.__calls.filter(c => c.b && c.u.indexOf('resolver') >= 0)[0]")
        self.assertEqual(llamada["u"], "/mantenciones/api/repuestos-rebajas/1/resolver")
        self.assertEqual(llamada["b"]["accion"], "aprobar")


# ══════════════════════════════════════════════════════════════════════
#  REGLA #69 EN CLAUDE.md
# ══════════════════════════════════════════════════════════════════════
class TestReglaEnClaudeMd(unittest.TestCase):
    def test_la_regla_69_esta_y_reemplaza_la_frase_de_las_entradas(self):
        i = CLAUDE_MD.index("REGLA #69")
        regla = CLAUDE_MD[i:i + 7000]
        for texto in ("Ajustar cantidad", "aumento", "rebaja", "opcional", "superadmin", "ajuste_autorizado", "rebaja_autorizada",
                      "las entradas siguen solo por recepción"):
            self.assertIn(texto, regla, texto)


if __name__ == "__main__":
    unittest.main()

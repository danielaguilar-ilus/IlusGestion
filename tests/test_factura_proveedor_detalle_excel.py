"""Monto del lote al día + Excel del detalle + historial visible (Facturas de proveedor). Daniel, 2026-10-08.

Caso real, lote #17 de Transportes felcarm SPA: se creó con 10 OT por $8.503.000 («Solicitud de orden de compra»); a las 21:24
Daniel le agregó una OT más (OT-2026-00162, $250.000). El lote quedó con 11 OT = $8.753.000, pero «Monto de la factura» siguió
en $8.503.000 y la Diferencia en -$250.000: «debería tener 250 lucas más». Y: «perdí el detalle, no veo un Excel».

Lo que fijan estas pruebas (sin BD ni Flask: las funciones y rutas de app.py se extraen con ast y corren sobre una base falsa):
  a) Lote SIN factura real (numero_documento vacío) y pendiente: asignar / quitar una OT recalcula monto_total = suma de los items,
     deja constancia en mant_logs y avisa «porque todavía no tiene factura del proveedor».
  b) Lote CON factura real: el monto NO se toca; se avisa «La factura dice $A y las OT suman $B: diferencia $D».
  c) ajustar-monto fija la suma con antes→después y usuario, exige lote pendiente, y con factura real pide confirmación.
  d) El Excel existe, exige permiso, trae las 3 hojas, el Resumen usa fórmulas SUM sobre la hoja de OT y los totales coinciden con
     la pantalla (caso fijo del #17: 11 OT = $8.753.000, factura $8.503.000, diferencia -$250.000).
  e) Los técnicos reciben 403 en todas las rutas nuevas (REGLA #19).

Correr con:  py -m unittest tests.test_factura_proveedor_detalle_excel
"""
import ast
import copy
import datetime
import functools
import io
import os
import re
import types
import unittest

from openpyxl import load_workbook

from tests.test_incidencias_repuesto_tercera_fuente import _codigo_y_arbol, _fuente_de
from tests.test_ot_finanzas_lectores import V, _amb

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FUNCS = ("_mfp_clp_txt", "_mfp_tiene_factura_real", "_mfp_totales_lote", "_mfp_monto_lote_al_dia", "_mfp_evento_fila",
         "_mfp_eventos_total", "_mfp_eventos_lote", "_mfp_proveedor_canon", "_mfp_excel_lote", "_mfp_excel_nombre",
         "_mfp_items", "mant_factura_proveedor_asignar", "mant_factura_proveedor_desasignar",
         "mant_factura_proveedor_ajustar_monto", "mant_factura_proveedor_historial", "mant_factura_proveedor_excel",
         "_no_tecnico")
RUTAS_NUEVAS = ("mant_factura_proveedor_ajustar_monto", "mant_factura_proveedor_historial", "mant_factura_proveedor_excel")

AHORA = datetime.datetime(2026, 10, 8, 21, 50)

# Los 10 pagos del lote #17 (suman $8.503.000) y la OT que Daniel agregó a las 21:24.
PAGOS_10 = [1_200_000, 900_000, 850_000, 1_000_000, 700_000, 650_000, 1_100_000, 800_000, 903_000, 400_000]
PAGO_162 = 250_000
MARKUP = 120_000  # cobrado al cliente = pago + MARKUP, salvo la garantía (OT #4: se paga y no se cobra)


def _fila_ot(i, pago):
    """Fila de mant_visitas de la OT i (1..11) que paga `pago` al proveedor."""
    if i == 4:  # garantía: se le paga al proveedor igual, no se le cobra al cliente
        kw = dict(modalidad_cobro="garantia", costo=pago + MARKUP, zz_monto=1, zz_codigo="ZZRETIRO", valor_origen="zz",
                  costo_proveedor=pago)
    else:
        kw = dict(zz_monto=pago + MARKUP, valor_origen="zz", costo_proveedor=pago)
    if i == 9:  # una OT aún sin cerrar
        kw["estado"] = "en_proceso"
    return V(id=100 + i, numero_ot=f"OT-2026-{150 + i:05d}", **kw)


class BaseFalsa:
    """Tablas mínimas: lotes, items, logs y OT. Responde a las consultas que hacen las funciones que se prueban."""

    def __init__(self):
        self.lotes = {}
        self.items = []  # {fid, vid, monto, obs, usuario, creada}
        self.logs = []
        self.ots = {}  # vid -> fila de mant_visitas
        self.puede = True
        self.superadmin = True
        self.body = {}
        self.args = {}
        self.usuario = "daniel"
        self.ahora = AHORA

    # ── helpers de armado ────────────────────────────────────────────────────
    def lote(self, fid=17, numero_documento=None, monto=0.0, estado="pendiente", **kw):
        fila = {"id": fid, "proveedor_nombre": "Transportes felcarm SPA", "proveedor_rut": "77.230.690-4",
                "tecnico_externo_id": None, "tipo_documento": "factura", "numero_documento": numero_documento,
                "fecha": None, "monto_total": monto, "estado_pago": estado, "numero_oc": "OC-9790",
                "created_at": datetime.datetime(2026, 10, 8, 20, 43), "created_by": "daniel", "notas": None,
                "pagada_at": None, "pagada_por": None}
        fila.update(kw)
        self.lotes[fid] = fila
        return fila

    def agregar_item(self, fid, vid, monto, creada=None):
        self.items.append({"fid": fid, "vid": vid, "monto": float(monto), "obs": "", "usuario": "daniel",
                           "creada": creada or datetime.datetime(2026, 10, 8, 20, 43)})

    def suma(self, fid):
        return sum(i["monto"] for i in self.items if i["fid"] == fid)

    def n(self, fid):
        return sum(1 for i in self.items if i["fid"] == fid)

    def logs_de(self, accion):
        return [lg for lg in self.logs if lg["accion"] == accion]

    # ── dobles de la capa de datos ───────────────────────────────────────────
    def cargar(self, fid):
        return dict(self.lotes[fid]) if fid in self.lotes else None

    def fetchone(self, sql, params=None):
        params = params or ()
        if "FROM mant_factura_proveedor_items" in sql and "SUM(monto)" in sql:
            return {"suma": self.suma(params[0]), "n": self.n(params[0])}
        if "FROM mant_logs" in sql and "COUNT(*)" in sql:
            return {"n": sum(1 for lg in self.logs if lg["entidad_id"] == params[0])}
        if "numero_ot FROM mant_visitas" in sql:
            return {"numero_ot": self.ots[params[0]]["numero_ot"]}
        if "AS ok" in sql:
            return {"ok": 1}
        if "WHERE v.id=%s" in sql:
            return dict(self.ots[params[0]])
        return None

    def fetchall(self, sql, params=None):
        params = params or ()
        if "FROM mant_logs" in sql:
            fid, limite, desplazar = params
            rows = [lg for lg in self.logs if lg["entidad"] == "factura_proveedor" and lg["entidad_id"] == fid]
            rows.sort(key=lambda r: (r["created_at"], r["id"]), reverse=" DESC" in sql)
            return [dict(r) for r in rows[desplazar:desplazar + limite]]
        if "fpi.factura_proveedor_id=%s" in sql:  # _mfp_items
            out = []
            for it in sorted((i for i in self.items if i["fid"] == params[0]), key=lambda i: (i["creada"], i["vid"])):
                f = dict(self.ots[it["vid"]])
                f.update(cliente=f.get("cliente") or f"Cliente {it['vid']}", tecnico_nombre="Marcelo Pérez",
                         fac_id=it["fid"], fac_numero=self.lotes[it["fid"]]["numero_documento"],
                         fac_estado=self.lotes[it["fid"]]["estado_pago"], fac_monto=it["monto"],
                         fac_obs=it["obs"], fac_usuario=it["usuario"], fac_asignada_at=it["creada"],
                         fecha_programada=datetime.date(2026, 10, 1), cerrada_at=None)
                out.append(f)
            return out
        return []

    def rowcount(self, sql, params=None):
        params = params or ()
        if sql.startswith("UPDATE mant_facturas_proveedor SET monto_total=%s"):
            monto, fid = params
            f = self.lotes.get(fid)
            if not f:
                return 0
            if "estado_pago='pendiente'" in sql and f["estado_pago"] != "pendiente":
                return 0
            if "numero_documento IS NULL" in sql and (f["numero_documento"] or "").strip():
                return 0
            f["monto_total"] = float(monto)
            return 1
        if sql.startswith("DELETE FROM mant_factura_proveedor_items"):
            fid, vid = params
            antes = len(self.items)
            self.items = [i for i in self.items if not (i["fid"] == fid and i["vid"] == vid)]
            return antes - len(self.items)
        raise AssertionError("consulta no prevista: " + sql)

    def execute(self, sql, params=None):
        params = params or ()
        if sql.startswith("INSERT INTO mant_factura_proveedor_items"):
            fid, vid, monto, obs, usuario = params
            self.items.append({"fid": fid, "vid": vid, "monto": float(monto), "obs": obs or "", "usuario": usuario,
                               "creada": self.ahora})
            return 1
        if sql.startswith("UPDATE mant_facturas_proveedor SET notas"):
            self.lotes[params[1]]["notas"] = (self.lotes[params[1]]["notas"] or "") + params[0]
            return 1
        raise AssertionError("escritura no prevista: " + sql)

    def log(self, entidad, entidad_id, accion, detalle=""):
        self.logs.append({"id": len(self.logs) + 1, "entidad": entidad, "entidad_id": entidad_id, "accion": accion,
                          "detalle": detalle, "usuario": self.usuario, "created_at": self.ahora})


def _entorno(db):
    """Ámbito con las funciones REALES de app.py (decoradores fuera) conectadas a la base falsa."""
    amb = _amb()
    _, arbol = _codigo_y_arbol()
    peticion = types.SimpleNamespace(
        get_json=lambda silent=True: db.body, args=db.args, is_json=True, headers={}, path="/servicio-tecnico/api/x")
    env = {
        "re": re, "print": lambda *a, **k: None, "wraps": functools.wraps,
        "_mfp_fila_ot": amb["_mfp_fila_ot"], "_MFP_ESTADOS_FACTURABLES": amb["_MFP_ESTADOS_FACTURABLES"],
        "_MFP_ESTADOS_EXCLUIDOS": ("cancelada", "anulada"),
        "_MFP_SELECT_OT": "SELECT_OT ", "_MFP_JOINS_OT": " JOINS_OT ", "_MFP_SQL_OT_EXTERNA": "OT_EXTERNA",
        "_MFP_ACCION_TXT": None,
        "jsonify": lambda d: d,
        "request": peticion,
        "g": types.SimpleNamespace(permissions={"superadmin": db.superadmin}),
        "current_username": lambda: db.usuario,
        "_facprov_puede": lambda: db.puede,
        "_mfp_403": lambda: ({"ok": False, "error_codigo": "SIN_PERMISO"}, 403),
        "_mfp_cargar": db.cargar,
        "mysql_fetchone": db.fetchone, "mysql_fetchall": db.fetchall,
        "mysql_execute_returning_rowcount": db.rowcount, "mysql_execute": db.execute,
        "_mant_log": db.log,
        "chile_fmt_filter": lambda v, fmt="%d/%m/%Y %H:%M": v.strftime(fmt) if v else "",
        "rut_fmt_filter": lambda v: v, "_now_chile": lambda: db.ahora,
        "send_file": lambda buf, **kw: {"buf": buf, **kw},
        "_es_rol_tecnico": lambda: False,
        "flash": lambda *a, **k: None, "redirect": lambda u: ("redirect", u), "url_for": lambda n, **k: "/" + n,
    }
    for nodo in arbol.body:
        if isinstance(nodo, ast.Assign) and len(nodo.targets) == 1 and getattr(nodo.targets[0], "id", "") == "_MFP_ACCION_TXT":
            exec(compile(ast.Module(body=[nodo], type_ignores=[]), "<app>", "exec"), env)
        if isinstance(nodo, ast.FunctionDef) and nodo.name in FUNCS:
            # Copia: el árbol de app.py está cacheado y compartido con otras pruebas; no se le quitan los decoradores.
            nodo = copy.deepcopy(nodo)
            nodo.decorator_list = []
            exec(compile(ast.Module(body=[nodo], type_ignores=[]), "<app>", "exec"), env)
    faltan = [n for n in FUNCS if n not in env]
    assert not faltan, faltan
    return env


def _resp(r):
    """(dict, status) venga como venga la respuesta de la ruta."""
    return (r[0], r[1]) if isinstance(r, tuple) else (r, 200)


def _lote17(sin_factura=True, estado="pendiente", con_162=False):
    """El lote #17 del caso real. sin_factura=False: ya tiene la factura real del proveedor (N° 5400)."""
    db = BaseFalsa()
    db.lote(17, numero_documento=None if sin_factura else "5400", monto=sum(PAGOS_10), estado=estado)
    for i, p in enumerate(PAGOS_10, 1):
        db.ots[100 + i] = _fila_ot(i, p)
        db.agregar_item(17, 100 + i, p)
    db.ots[111] = _fila_ot(11, PAGO_162)
    db.ots[111]["numero_ot"] = "OT-2026-00162"
    db.ots[111]["cliente"] = "Brave Spa"
    if con_162:
        db.agregar_item(17, 111, PAGO_162, creada=datetime.datetime(2026, 10, 8, 21, 24))
    return db


# ════════════════════════════════════════════════════════════════════════════════════════════
class TestFuncionesPuras(unittest.TestCase):
    def setUp(self):
        self.e = _entorno(BaseFalsa())

    def test_formato_pesos_chilenos(self):
        f = self.e["_mfp_clp_txt"]
        self.assertEqual(f(8503000), "$8.503.000")
        self.assertEqual(f(-250000), "-$250.000")
        self.assertEqual(f(0), "$0")
        self.assertEqual(f(None), "$0")

    def test_lote_sin_factura_real_es_el_que_la_pantalla_llama_pendiente_de_completar(self):
        # Misma condición que s3_ok de la plantilla: numero_documento escrito.
        t = self.e["_mfp_tiene_factura_real"]
        self.assertFalse(t({"numero_documento": None}))
        self.assertFalse(t({"numero_documento": ""}))
        self.assertFalse(t({"numero_documento": "   "}))
        self.assertTrue(t({"numero_documento": "5400"}))
        plantilla = open(os.path.join(RAIZ, "templates", "mantenciones", "factura_proveedor_detalle.html"), encoding="utf-8").read()
        self.assertIn("{% set s3_ok = factura.numero_documento | default('', true) | trim != '' %}", plantilla)
        self.assertIn("Sin factura — pendiente de completar", plantilla)

    def test_totales_del_caso_17(self):
        db = _lote17(con_162=True)
        e = _entorno(db)
        items = e["_mfp_items"](17)
        self.assertEqual(len(items), 11)
        # Cada OT paga lo que dice el motor (técnico + despacho), igual que la pantalla
        self.assertEqual([i["sugerido"] for i in items], PAGOS_10 + [PAGO_162])
        tot = e["_mfp_totales_lote"](8503000, items)
        self.assertEqual(tot["total_asignado"], 8753000.0)
        self.assertEqual(tot["monto_total"], 8503000.0)
        self.assertEqual(tot["diferencia"], -250000.0)
        # cobrado = pago + $120.000 en las 10 que se cobran (la garantía de $1.000.000 no)
        self.assertEqual(tot["total_cobrado_cliente"], (8753000 - 1000000) + 10 * MARKUP)
        self.assertEqual(tot["margen_total"], tot["total_cobrado_cliente"] - 8503000)
        self.assertEqual(tot["margen_filas"], tot["total_cobrado_cliente"] - 8753000)
        self.assertEqual((tot["n_ot"], tot["n_no_cobran"], tot["n_sin_cerrar"]), (11, 1, 1))


class TestMontoAlDiaAlAsignarYQuitar(unittest.TestCase):
    """a) y b)"""

    def test_a_sin_factura_asignar_la_162_actualiza_el_monto_y_deja_log(self):
        db = _lote17(sin_factura=True)
        e = _entorno(db)
        db.body = {"visita_id": 111, "monto": 0}
        r, st = _resp(e["mant_factura_proveedor_asignar"](17))
        self.assertEqual(st, 200)
        self.assertTrue(r["ok"])
        ml = r["monto_lote"]
        self.assertTrue(ml["recalculado"])
        self.assertEqual((ml["antes"], ml["despues"]), (8503000.0, 8753000.0))
        self.assertEqual(db.lotes[17]["monto_total"], 8753000.0)
        self.assertIn("$8.753.000", ml["aviso"])
        self.assertIn("porque todavía no tiene factura del proveedor", ml["aviso"])
        lg = db.logs_de("monto_actualizado_auto")
        self.assertEqual(len(lg), 1)
        self.assertEqual(lg[0]["entidad"], "factura_proveedor")
        self.assertEqual(lg[0]["entidad_id"], 17)
        self.assertEqual(lg[0]["detalle"],
                         "monto del lote actualizado de $8.503.000 a $8.753.000: se agregó la OT-2026-00162 (sin factura aún)")
        self.assertEqual(lg[0]["usuario"], "daniel")
        # el log de la OT asignada sigue existiendo
        self.assertEqual(len(db.logs_de("ot_asignada")), 1)

    def test_a_sin_factura_quitar_la_162_vuelve_el_monto_y_deja_log(self):
        db = _lote17(sin_factura=True, con_162=True)
        db.lotes[17]["monto_total"] = 8753000.0
        e = _entorno(db)
        r, st = _resp(e["mant_factura_proveedor_desasignar"](17, 111))
        self.assertEqual(st, 200)
        self.assertTrue(r["monto_lote"]["recalculado"])
        self.assertEqual(db.lotes[17]["monto_total"], 8503000.0)
        self.assertEqual(db.logs_de("monto_actualizado_auto")[0]["detalle"],
                         "monto del lote actualizado de $8.753.000 a $8.503.000: se quitó la OT-2026-00162 (sin factura aún)")

    def test_a_si_ya_cuadra_no_escribe_nada(self):
        db = _lote17(sin_factura=True)
        e = _entorno(db)
        info = e["_mfp_monto_lote_al_dia"](17, None, ("agregó", "OT-X"))
        self.assertFalse(info["recalculado"])
        self.assertEqual(info["aviso"], "")
        self.assertEqual(db.logs, [])

    def test_b_con_factura_real_no_se_recalcula_y_se_avisa_la_diferencia(self):
        db = _lote17(sin_factura=False)
        e = _entorno(db)
        db.body = {"visita_id": 111, "monto": 0}
        r, st = _resp(e["mant_factura_proveedor_asignar"](17))
        self.assertEqual(st, 200)
        ml = r["monto_lote"]
        self.assertFalse(ml["recalculado"])
        self.assertTrue(ml["tiene_factura"])
        self.assertEqual(db.lotes[17]["monto_total"], 8503000.0, "el monto es el del documento: no se toca")
        self.assertEqual(ml["aviso"], "La factura dice $8.503.000 y las OT suman $8.753.000: diferencia -$250.000.")
        self.assertEqual(ml["nivel"], "warning")
        self.assertEqual(db.logs_de("monto_actualizado_auto"), [])

    def test_b_con_factura_real_quitar_tampoco_lo_toca(self):
        db = _lote17(sin_factura=False, con_162=True)
        e = _entorno(db)
        r, _ = _resp(e["mant_factura_proveedor_desasignar"](17, 111))
        self.assertFalse(r["monto_lote"]["recalculado"])
        self.assertEqual(db.lotes[17]["monto_total"], 8503000.0)
        self.assertEqual(r["monto_lote"]["aviso"], "")  # ya vuelve a cuadrar con la factura

    def test_lote_pagado_sin_factura_no_se_toca_aunque_el_superadmin_quite_una_ot(self):
        db = _lote17(sin_factura=True, estado="pagada", con_162=True)
        db.lotes[17]["monto_total"] = 8753000.0
        e = _entorno(db)
        db.body = {"motivo": "se cobró por error en este lote"}
        r, st = _resp(e["mant_factura_proveedor_desasignar"](17, 111))
        self.assertEqual(st, 200)
        self.assertFalse(r["monto_lote"]["recalculado"])
        self.assertEqual(db.lotes[17]["monto_total"], 8753000.0)
        self.assertIn("no se toca", r["monto_lote"]["aviso"])
        self.assertEqual(db.logs_de("monto_actualizado_auto"), [])

    def test_la_actualizacion_automatica_exige_pendiente_y_sin_documento_en_el_propio_update(self):
        # Defensa contra carreras: aunque otra pestaña le ponga el N° de factura entre la lectura y la escritura.
        src = _fuente_de("_mfp_monto_lote_al_dia")
        self.assertIn("estado_pago='pendiente'", src)
        self.assertIn("numero_documento IS NULL OR TRIM(numero_documento)=''", src)


class TestAjustarMonto(unittest.TestCase):
    """c)"""

    def test_fija_la_suma_con_antes_despues_y_usuario(self):
        db = _lote17(sin_factura=True, con_162=True)  # monto declarado quedó en $8.503.000, OT suman $8.753.000
        e = _entorno(db)
        r, st = _resp(e["mant_factura_proveedor_ajustar_monto"](17))
        self.assertEqual(st, 200)
        self.assertTrue(r["ok"] and r["cambio"])
        self.assertEqual((r["antes"], r["monto"]), (8503000.0, 8753000.0))
        self.assertEqual(db.lotes[17]["monto_total"], 8753000.0)
        lg = db.logs_de("monto_ajustado")
        self.assertEqual(len(lg), 1)
        self.assertIn("de $8.503.000 a $8.753.000", lg[0]["detalle"])
        self.assertIn("suma de 11 OT", lg[0]["detalle"])
        self.assertIn("por daniel", lg[0]["detalle"])
        self.assertEqual(lg[0]["usuario"], "daniel")

    def test_exige_estado_pendiente(self):
        for estado in ("pagada", "anulada"):
            db = _lote17(sin_factura=True, estado=estado, con_162=True)
            e = _entorno(db)
            r, st = _resp(e["mant_factura_proveedor_ajustar_monto"](17))
            self.assertEqual(st, 409, estado)
            self.assertEqual(r["error_codigo"], "FACTURA_NO_EDITABLE")
            self.assertEqual(db.lotes[17]["monto_total"], 8503000.0)
            self.assertEqual(db.logs, [])

    def test_con_factura_real_pide_confirmacion_y_sin_ella_no_cambia(self):
        db = _lote17(sin_factura=False, con_162=True)
        e = _entorno(db)
        r, st = _resp(e["mant_factura_proveedor_ajustar_monto"](17))
        self.assertEqual(st, 409)
        self.assertEqual(r["error_codigo"], "CONFIRMAR_FACTURA_REAL")
        self.assertIn("$8.503.000", r["error"])
        self.assertIn("$8.753.000", r["error"])
        self.assertEqual(db.lotes[17]["monto_total"], 8503000.0)
        self.assertEqual(db.logs_de("monto_ajustado"), [])
        # con la confirmación expresa sí, y el log recuerda lo que decía el documento
        db.body = {"confirmar_factura_real": True}
        r, st = _resp(e["mant_factura_proveedor_ajustar_monto"](17))
        self.assertEqual(st, 200)
        self.assertEqual(db.lotes[17]["monto_total"], 8753000.0)
        self.assertIn("decía $8.503.000", db.logs_de("monto_ajustado")[0]["detalle"])

    def test_si_ya_cuadra_no_cambia_ni_escribe_log(self):
        db = _lote17(sin_factura=True)
        e = _entorno(db)
        r, st = _resp(e["mant_factura_proveedor_ajustar_monto"](17))
        self.assertEqual(st, 200)
        self.assertFalse(r["cambio"])
        self.assertEqual(db.logs, [])

    def test_sin_ot_no_hay_nada_que_sumar(self):
        db = BaseFalsa()
        db.lote(5, monto=300000.0)
        e = _entorno(db)
        r, st = _resp(e["mant_factura_proveedor_ajustar_monto"](5))
        self.assertEqual((st, r["error_codigo"]), (400, "SIN_OT"))
        self.assertEqual(db.lotes[5]["monto_total"], 300000.0)

    def test_sin_permiso_o_sin_lote(self):
        db = _lote17(con_162=True)
        e = _entorno(db)
        db.puede = False
        self.assertEqual(_resp(e["mant_factura_proveedor_ajustar_monto"](17))[1], 403)
        self.assertEqual(db.lotes[17]["monto_total"], 8503000.0)
        db.puede = True
        self.assertEqual(_resp(e["mant_factura_proveedor_ajustar_monto"](999))[1], 404)


class TestHistorial(unittest.TestCase):
    def _db_con_eventos(self, n=23):
        db = _lote17(con_162=True)
        for i in range(n):
            db.ahora = datetime.datetime(2026, 10, 8, 20, 0) + datetime.timedelta(minutes=i)
            db.log("factura_proveedor", 17, "ot_asignada", f"OT-{i}")
        return db

    def test_pagina_los_mas_nuevos_primero(self):
        db = self._db_con_eventos(23)
        e = _entorno(db)
        db.args.update({"pagina": "1", "por_pagina": "10"})
        r, st = _resp(e["mant_factura_proveedor_historial"](17))
        self.assertEqual((st, r["total"], r["paginas"], r["pagina"], len(r["eventos"])), (200, 23, 3, 1, 10))
        self.assertEqual(r["eventos"][0]["detalle"], "OT-22")
        self.assertEqual(r["eventos"][0]["accion_txt"], "OT asignada")
        db.args["pagina"] = "3"
        r, _ = _resp(e["mant_factura_proveedor_historial"](17))
        self.assertEqual([x["detalle"] for x in r["eventos"]], ["OT-2", "OT-1", "OT-0"])
        db.args["pagina"] = "99"  # se acota a la última
        r, _ = _resp(e["mant_factura_proveedor_historial"](17))
        self.assertEqual(r["pagina"], 3)
        db.args.update({"pagina": "1", "por_pagina": "7"})  # tamaño no permitido -> 10
        r, _ = _resp(e["mant_factura_proveedor_historial"](17))
        self.assertEqual(r["por_pagina"], 10)

    def test_fecha_en_dia_mes_ano_con_hora(self):
        db = _lote17(con_162=True)
        db.ahora = datetime.datetime(2026, 10, 8, 21, 24)
        db.log("factura_proveedor", 17, "ot_asignada", "OT-2026-00162 · $250,000")
        e = _entorno(db)
        r, _ = _resp(e["mant_factura_proveedor_historial"](17))
        self.assertEqual(r["eventos"][0]["fecha"], "08/10/2026 21:24")

    def test_la_pantalla_trae_el_bloque_y_el_servidor_lo_alimenta(self):
        plantilla = open(os.path.join(RAIZ, "templates", "mantenciones", "factura_proveedor_detalle.html"), encoding="utf-8").read()
        for marca in ('id="fphBox"', "Historial del lote", "Mostrando ", "fphPrev", "fphNext", "fphSize",
                      "/historial?pagina="):
            self.assertIn(marca, plantilla, marca)
        src = _fuente_de("mant_factura_proveedor_detalle")
        self.assertIn("_mfp_eventos_lote(fid, limite=10)", src)
        self.assertIn("historial_total", src)


class TestExcel(unittest.TestCase):
    """d)"""

    def _excel(self, sin_factura=True):
        db = _lote17(sin_factura=sin_factura, con_162=True)
        # historial real del caso: creación a las 20:43 y la OT 162 a las 21:24
        db.ahora = datetime.datetime(2026, 10, 8, 20, 43)
        db.log("factura_proveedor", 17, "solicitud_oc_creada", "Transportes felcarm SPA · 10 OT · $8.503.000 · OC (pendiente)")
        db.ahora = datetime.datetime(2026, 10, 8, 21, 24)
        db.log("factura_proveedor", 17, "ot_asignada", "OT-2026-00162 · $250,000")
        db.ahora = AHORA
        e = _entorno(db)
        r, st = _resp(e["mant_factura_proveedor_excel"](17))
        self.assertEqual(st, 200)
        return db, r, load_workbook(io.BytesIO(r["buf"].getvalue()))

    @staticmethod
    def _evaluar(wb, hoja, celda):
        """Mini evaluador de las fórmulas del Resumen: SUM / COUNTA sobre la hoja de OT y restas de celdas."""
        v = wb[hoja][celda].value
        if not (isinstance(v, str) and v.startswith("=")):
            return v
        m = re.fullmatch(r"=(SUM|COUNTA)\('OT del lote'!([A-Z]+)(\d+):([A-Z]+)(\d+)\)", v)
        if m:
            fn, c1, r1, _c2, r2 = m.groups()
            vals = [wb["OT del lote"][f"{c1}{r}"].value for r in range(int(r1), int(r2) + 1)]
            return sum(x for x in vals if isinstance(x, (int, float))) if fn == "SUM" else len([x for x in vals if x not in (None, "")])
        m = re.fullmatch(r"=([A-Z]+\d+)-([A-Z]+\d+)", v)
        if m:
            return TestExcel._evaluar(wb, hoja, m.group(1)) - TestExcel._evaluar(wb, hoja, m.group(2))
        raise AssertionError("fórmula no prevista: " + v)

    def test_tres_hojas_y_nombre_del_archivo(self):
        db, r, wb = self._excel()
        self.assertEqual(wb.sheetnames, ["Resumen", "OT del lote", "Historial"])
        self.assertEqual(r["download_name"], "lote_17_Transportes-felcarm-SPA_08-10-2026.xlsx")
        self.assertTrue(r["as_attachment"])
        self.assertIn("spreadsheetml", r["mimetype"])

    def test_resumen_usa_formulas_y_cuadra_con_la_pantalla_caso_17(self):
        db, r, wb = self._excel()
        ws = wb["Resumen"]
        etiqueta = {ws.cell(row=i, column=1).value: i for i in range(2, ws.max_row + 1)}
        self.assertEqual(ws["B11"].value, "=SUM('OT del lote'!M2:M12)")
        self.assertEqual(ws["B12"].value, "=B10-B11")
        self.assertTrue(str(ws["B13"].value).startswith("=SUM('OT del lote'!P2:P12)"))
        self.assertEqual(etiqueta["Suma de las OT"], 11)
        self.assertEqual(etiqueta["Diferencia"], 12)
        self.assertEqual(etiqueta["Monto declarado del lote"], 10)
        # Los números son los de la pantalla (_mfp_totales_lote sobre _mfp_items, la cuenta única)
        e = _entorno(db)
        tot = e["_mfp_totales_lote"](db.lotes[17]["monto_total"], e["_mfp_items"](17))
        self.assertEqual(self._evaluar(wb, "Resumen", "B10"), 8503000)
        self.assertEqual(self._evaluar(wb, "Resumen", "B11"), 8753000)
        self.assertEqual(self._evaluar(wb, "Resumen", "B12"), -250000)
        self.assertEqual(self._evaluar(wb, "Resumen", "B11"), tot["total_asignado"])
        self.assertEqual(self._evaluar(wb, "Resumen", "B12"), tot["diferencia"])
        self.assertEqual(self._evaluar(wb, "Resumen", "B13"), tot["total_cobrado_cliente"])
        self.assertEqual(self._evaluar(wb, "Resumen", "B14"), tot["margen_total"])
        self.assertEqual(self._evaluar(wb, "Resumen", "B15"), tot["margen_filas"])
        self.assertEqual(self._evaluar(wb, "Resumen", "B16"), 11)
        self.assertEqual(ws["B17"].value, 1, "una OT no se cobra (garantía)")
        self.assertEqual(ws["B18"].value, 1, "una OT sin cerrar")
        self.assertEqual(ws["B3"].value, "Transportes felcarm SPA")
        self.assertEqual(ws["B6"].value, "OC-9790")
        self.assertIn("Sin factura aún", ws["B7"].value)
        self.assertEqual(ws["B5"].value, "Pendiente de pago")

    def test_con_factura_real_el_resumen_dice_el_documento(self):
        db, r, wb = self._excel(sin_factura=False)
        self.assertEqual(wb["Resumen"]["B7"].value, "Factura N° 5400")
        self.assertEqual(self._evaluar(wb, "Resumen", "B12"), -250000)

    def test_hoja_de_ot_una_fila_por_ot_con_totales_por_formula(self):
        db, r, wb = self._excel()
        ws = wb["OT del lote"]
        cab = [c.value for c in ws[1]]
        for col in ("N° OT", "Fecha", "Técnico", "Cliente", "Tipo de trabajo", "Cobertura", "Estado", "N° de anexo",
                    "Anexo firmado", "Pago instalación", "Pago despacho", "Total que paga esta factura",
                    "Cobrado instalación", "Cobrado despacho", "Cobrado total", "Margen", "Margen %", "Observación",
                    "Asignada por", "Asignada el"):
            self.assertIn(col, cab, col)
        self.assertEqual(ws.max_row, 13, "encabezado + 11 OT + fila de totales")
        self.assertEqual(ws["A13"].value, "TOTALES")
        self.assertEqual(ws["M13"].value, "=SUM(M2:M12)")
        self.assertEqual(ws["P13"].value, "=SUM(P2:P12)")
        self.assertEqual(ws["R13"].value, '=IF(P13>0,Q13/P13,"")')
        filas = {ws.cell(row=i, column=1).value: i for i in range(2, 13)}
        f162 = filas["OT-2026-00162"]
        self.assertEqual(ws.cell(row=f162, column=5).value, "Brave Spa")
        self.assertEqual(ws.cell(row=f162, column=13).value, 250000)
        self.assertEqual(ws.cell(row=f162, column=20).value, "daniel")
        self.assertEqual(ws.cell(row=f162, column=21).value, "08/10/2026 21:24")
        # la garantía se paga y no se cobra; la no cerrada se dice en el estado
        self.assertEqual(ws.cell(row=filas["OT-2026-00154"], column=16).value, 0)
        self.assertEqual(ws.cell(row=filas["OT-2026-00154"], column=13).value, 1000000)
        self.assertNotEqual(ws.cell(row=filas["OT-2026-00154"], column=7).value, "Se cobra")
        self.assertIn("no cerrada", ws.cell(row=filas["OT-2026-00159"], column=8).value)
        # formato de la hoja: Arial, encabezado fijo, autofiltro, pesos
        self.assertEqual(ws.freeze_panes, "A2")
        self.assertEqual(ws.auto_filter.ref, "A1:U12")
        self.assertEqual(ws["A1"].font.name, "Arial")
        self.assertEqual(ws["M2"].font.name, "Arial")
        self.assertEqual(ws["M2"].number_format, '"$"#,##0;[Red]-"$"#,##0')
        self.assertEqual(wb["Resumen"]["B10"].number_format, '"$"#,##0;[Red]-"$"#,##0')

    def test_historial_recupera_que_se_agrego_y_cuando(self):
        db, r, wb = self._excel()
        ws = wb["Historial"]
        self.assertEqual([c.value for c in ws[1]], ["Fecha y hora (Chile)", "Usuario", "Acción", "Detalle"])
        filas = [[c.value for c in row] for row in ws.iter_rows(min_row=2)]
        self.assertEqual(len(filas), 2)
        self.assertEqual(filas[0][0], "08/10/2026 20:43")  # cronológico: lo primero es la creación
        self.assertEqual(filas[0][2], "Solicitud de OC creada")
        self.assertEqual(filas[1][0], "08/10/2026 21:24")
        self.assertEqual(filas[1][1], "daniel")
        self.assertEqual(filas[1][2], "OT asignada")
        self.assertIn("OT-2026-00162", filas[1][3])

    def test_texto_que_empieza_con_igual_no_se_ejecuta_como_formula(self):
        db = _lote17(con_162=True)
        db.items[-1]["obs"] = "=HYPERLINK(\"http://x\")"
        db.ahora = datetime.datetime(2026, 10, 8, 22, 0)
        db.log("factura_proveedor", 17, "editada", "-2+3")
        e = _entorno(db)
        r, _ = _resp(e["mant_factura_proveedor_excel"](17))
        wb = load_workbook(io.BytesIO(r["buf"].getvalue()))
        celda = wb["OT del lote"].cell(row=12, column=19)
        self.assertEqual(celda.data_type, "s")
        self.assertEqual(wb["Historial"]["D2"].data_type, "s")

    def test_exige_permiso_y_lote_existente(self):
        db = _lote17(con_162=True)
        e = _entorno(db)
        db.puede = False
        r, st = _resp(e["mant_factura_proveedor_excel"](17))
        self.assertEqual((st, r["error_codigo"]), (403, "SIN_PERMISO"))
        db.puede = True
        self.assertEqual(_resp(e["mant_factura_proveedor_excel"](999))[1], 404)

    def test_nombre_de_archivo_ascii(self):
        e = _entorno(BaseFalsa())
        n = e["_mfp_excel_nombre"]
        self.assertEqual(n(3, "Logística y Transportes Milling SPA", AHORA), "lote_3_Logistica-y-Transportes-Milling-SPA_08-10-2026.xlsx")
        self.assertEqual(n(3, "", AHORA), "lote_3_proveedor_08-10-2026.xlsx")


class TestTecnicosSinAcceso(unittest.TestCase):
    """e) REGLA #19: ningún técnico ve proveedores ni lo que se les paga."""

    def _decoradores(self, nombre):
        _, arbol = _codigo_y_arbol()
        for n in ast.walk(arbol):
            if isinstance(n, ast.FunctionDef) and n.name == nombre:
                return [ast.unparse(d) for d in n.decorator_list]
        raise AssertionError(nombre)

    def test_las_rutas_nuevas_estan_detras_de_no_tecnico_y_del_permiso_de_facturacion(self):
        for nombre in RUTAS_NUEVAS:
            deco = self._decoradores(nombre)
            self.assertIn("_mant_required", deco, nombre)
            self.assertIn("_no_tecnico", deco, nombre)
            self.assertLess(deco.index("_mant_required"), deco.index("_no_tecnico"), nombre)
            self.assertIn("_facprov_puede()", _fuente_de(nombre), nombre)
            # rutas dobles /mantenciones y /servicio-tecnico
            rutas = [d for d in deco if d.startswith("app.route")]
            self.assertEqual(len(rutas), 2, nombre)
            self.assertTrue(any("/mantenciones/" in d for d in rutas) and any("/servicio-tecnico/" in d for d in rutas), nombre)

    def test_ajustar_monto_es_post_y_el_resto_get(self):
        self.assertIn("methods=['POST']", " ".join(self._decoradores("mant_factura_proveedor_ajustar_monto")))
        for nombre in ("mant_factura_proveedor_historial", "mant_factura_proveedor_excel"):
            self.assertNotIn("methods", " ".join(self._decoradores(nombre)), nombre)

    def test_un_tecnico_recibe_403_en_cada_ruta_nueva(self):
        for tecnico in ("tecnico", "tecnico_externo", "tecnico_ejecutivo"):
            db = _lote17(con_162=True)
            e = _entorno(db)
            e["_es_rol_tecnico"] = lambda: True  # lo que devuelve _es_rol_tecnico() para toda la familia técnico
            for nombre in RUTAS_NUEVAS:
                # misma cadena de decoradores que la ruta real: _no_tecnico(vista)
                vista = e["_no_tecnico"](lambda fid, _n=nombre: e[_n](fid))
                r, st = _resp(vista(17))
                self.assertEqual(st, 403, f"{tecnico}/{nombre}")
                self.assertEqual(r["error_codigo"], "TECNICO_SIN_ACCESO")
            self.assertEqual(db.lotes[17]["monto_total"], 8503000.0)
            self.assertEqual(db.logs, [])

    def test_el_excel_no_se_ofrece_fuera_del_modulo(self):
        # Los botones nuevos viven solo en las pantallas de facturación (que ya exigen el permiso).
        raiz = os.path.join(RAIZ, "templates")
        for ruta, _d, archivos in os.walk(raiz):
            for a in archivos:
                if not a.endswith(".html"):
                    continue
                p = os.path.join(ruta, a)
                if a in ("factura_proveedor_detalle.html", "facturas_proveedor.html"):
                    continue
                with open(p, encoding="utf-8", errors="ignore") as fh:
                    self.assertNotIn("mant_factura_proveedor_excel", fh.read(), p)


class TestPantallas(unittest.TestCase):
    def _leer(self, *partes):
        with open(os.path.join(RAIZ, *partes), encoding="utf-8") as fh:
            return fh.read()

    def test_detalle_tiene_boton_excel_y_boton_ajustar(self):
        t = self._leer("templates", "mantenciones", "factura_proveedor_detalle.html")
        self.assertIn("Descargar detalle (Excel)", t)
        self.assertIn("mant_factura_proveedor_excel", t)
        self.assertIn("Ajustar el monto a la suma de las OT", t)
        self.assertIn("fpdAjustarMonto()", t)
        self.assertIn("/ajustar-monto", t)
        self.assertIn("confirmar_factura_real", t)
        self.assertIn("factura del proveedor dice", t)
        self.assertIn("¿seguro?", t)
        # solo si hay diferencia y el lote está pendiente
        self.assertRegex(t, r"\{% if items and diferencia\|round\(0\) != 0 %\}")
        # usa ilusConfirm, nunca confirm() nativo (REGLA #1)
        self.assertIsNone(re.search(r"(?<![\w.])(alert|confirm|prompt)\(", t))

    def test_los_avisos_del_monto_llegan_a_la_pantalla(self):
        t = self._leer("templates", "mantenciones", "factura_proveedor_detalle.html")
        for marca in ("fpdGuardarAviso", "fpdMostrarAvisoGuardado", "d.monto_lote", "ml.aviso", 'id="fpdAvisoFlash"'):
            self.assertIn(marca, t, marca)
        self.assertIn("suma de las OT; se actualiza sola hasta que llegue la factura", t)

    def test_lista_tiene_icono_excel_por_lote_sin_tocar_las_columnas(self):
        t = self._leer("templates", "mantenciones", "facturas_proveedor.html")
        self.assertIn('class="fpv-xl"', t)
        self.assertIn("/servicio-tecnico/facturas-proveedor/{{ f.id }}/excel", t)
        self.assertEqual(t.count('<th class="rm-sortable'), 9, "las 9 columnas de la tabla siguen")
        self.assertIn('<tr class="rm-empty" id="fpvVacio" hidden><td colspan="9">', t)

    def test_la_pantalla_y_el_excel_leen_las_mismas_cifras(self):
        self.assertIn("_mfp_totales_lote(factura[\"monto_total\"], items)", _fuente_de("mant_factura_proveedor_detalle"))
        self.assertIn("_mfp_totales_lote(factura.get(\"monto_total\"), items)", _fuente_de("_mfp_excel_lote"))
        # el servidor ya no arma la suma a mano en la ruta de detalle
        self.assertNotIn('sum(i["sugerido"]', _fuente_de("mant_factura_proveedor_detalle"))

    def test_asignar_y_quitar_devuelven_el_aviso_del_monto(self):
        self.assertIn("_mfp_monto_lote_al_dia(fid, f, (\"agregó\", ot[\"numero_ot\"]))", _fuente_de("mant_factura_proveedor_asignar"))
        self.assertIn("_mfp_monto_lote_al_dia(fid, f, (\"quitó\", _num))", _fuente_de("mant_factura_proveedor_desasignar"))

    def test_espanol_latinoamericano_en_lo_nuevo(self):
        # Daniel es venezolano: «agregar», «asociar», «presionar/tocar»; nada de «asociar», «pulsar» ni «vale».
        textos = "\n".join(_fuente_de(n) for n in ("_mfp_monto_lote_al_dia", "mant_factura_proveedor_ajustar_monto",
                                                  "_mfp_excel_lote", "_mfp_evento_fila"))
        t = self._leer("templates", "mantenciones", "factura_proveedor_detalle.html")
        textos += t[t.index("async function fpdAjustarMonto"):t.index("async function fpdPagar")]
        textos += t[t.index('id="fphBox"'):t.index("{# ── Asignar OT")]
        for prohibida in (r"\bligar", r"\bpulsa", r"\bvale\b"):
            self.assertIsNone(re.search(prohibida, textos.lower()), prohibida)


if __name__ == "__main__":
    unittest.main()

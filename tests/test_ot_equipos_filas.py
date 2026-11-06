# -*- coding: utf-8 -*-
"""PASO «EQUIPOS Y SU CHECKLIST» DEL ASISTENTE «Generar OT» (Daniel, 5-nov-2026): una fila con CANTIDAD ↔ una fila POR UNIDAD (reversible), checklist
INTELIGENTE por producto y RECORDADO en el catálogo.

Daniel, con la captura de 10 filas idénticas «Rubber EPDM Flooring Corner Ramp G/B 20mm» (una línea de 10 unidades que el alta automática partió en 10
fichas): «quisiera tener la opción de REVERTIR… dame la opción de MULTIPLICAR LA FILA o dejarlo con UNA FILA CON 18. Además me cuesta mucho seleccionar el
checklist, esto parte tiene que ser INTELIGENTE. Y también GUARDARLA EN LOS CATÁLOGOS si lo seleccionan de allí».

Se prueba:
  1. El módulo puro `ot_equipos_filas.py`: cómo se sugiere contar un producto, cuándo se puede juntar o separar, la pista de un producto.
  2. Los endpoints REALES de app.py en un mundo de mentira (base en memoria con transacción): pistas, recordar el checklist, juntar y separar (nada se
     borra, todo es reversible, lo que ya tiene historia no se toca, un rechazo no cambia NADA).
  3. Cableado y reglas del proyecto: decoradores, tablas baratas (REGLA #18), arranque, el front (la lógica del asistente EJECUTADA con Node).

Sin BD, sin Flask, sin correos (REGLA #22). Correr:  python -m pytest tests/test_ot_equipos_filas.py -q -p no:warnings
"""
import copy
import datetime as dt
import json
import os
import re
import shutil
import subprocess
import tempfile
import types
import unittest

import ot_equipos_filas as F
from tests import _fuente_codigo as FC

AHORA = dt.datetime(2026, 11, 5, 15, 0, 0)


def fila(i, **kw):
    base = {"id": i, "cliente_id": 5, "sku": "RFCR-GB-20", "nombre": "Rubber EPDM Flooring Corner Ramp G/B 20mm", "serie": f"77038558-GB20-{i}",
            "doc_origen": "VD 11117", "cantidad": 1, "estado": "activo", "origen_visita_id": None,
            "created_at": AHORA - dt.timedelta(hours=1)}
    base.update(kw)
    return base


# ═══════════════════════════════════════════════════════════════════════════════════════════════════
#  1. El módulo puro
# ═══════════════════════════════════════════════════════════════════════════════════════════════════
class TestSugerirConteo(unittest.TestCase):
    def test_una_sola_unidad_no_pregunta_nada(self):
        r = F.sugerir_conteo("Trotadora ILUS", 1, {})
        self.assertEqual((r["valor"], r["origen"], r["motivo"]), ("unidad", "una_unidad", ""))
        self.assertEqual(F.sugerir_conteo("x", None)["origen"], "una_unidad")
        self.assertEqual(F.sugerir_conteo("x", "abc")["origen"], "una_unidad")

    def test_lo_que_dice_el_catalogo_manda_y_se_explica(self):
        piso = F.sugerir_conteo("Piso", 10, {"clase_nombre": "Pisos", "repetible": True})
        self.assertEqual((piso["valor"], piso["origen"]), ("cantidad", "clase"))
        self.assertIn("pisos", piso["motivo"].lower())
        self.assertIn("por cantidad", piso["motivo"])
        eq = F.sugerir_conteo("Rack", 3, {"clase_nombre": "Rack Avanzados", "repetible": False})
        self.assertEqual((eq["valor"], eq["origen"]), ("unidad", "clase"))
        self.assertIn("una fila por unidad", eq["motivo"])

    def test_sin_clasificar_el_nombre_puede_delatar_un_piso_pero_con_la_duda_va_por_unidad(self):
        r = F.sugerir_conteo("Rubber EPDM Flooring Corner Ramp G/B 20mm", 10, {})
        self.assertEqual((r["valor"], r["origen"]), ("cantidad", "nombre"))
        self.assertIn("parece un piso o accesorio", r["motivo"])
        self.assertIn("Si son máquinas", r["motivo"])                       # la salida a la sugerencia equivocada está a la vista
        r2 = F.sugerir_conteo("Dual Cable Ratio", 3, {})
        self.assertEqual((r2["valor"], r2["origen"]), ("unidad", "defecto"))
        self.assertIn("por seguridad", r2["motivo"])

    def test_nombres_de_maquinas_no_se_confunden_con_accesorios(self):
        for nombre in ("Booty Builder Platinum", "ILUS IS5 Dual Lat Pulldown / Low Row", "Trotadora Motorizada ILUS X1", "Smith Machine Counterbalance",
                       "Matrix Ultra Elíptica", "Bicicleta Indoor"):                # «Matrix» NO es «mat»
            self.assertEqual(F.sugerir_conteo(nombre, 4, {})["valor"], "unidad", nombre)

    def test_las_pistas_de_nombre_ignoran_tildes_mayusculas_y_signos(self):
        for nombre, palabra in (("COLCHONETA YOGA 10MM", "colchoneta"), ("Pelotas Bosú", "pelotas"), ("Banda-Elástica Nivel 2", None),
                                ("Bandas elásticas x10", "bandas elasticas"), ("Disco Bumper 20kg", "disco")):
            ok, p = F.parece_piso_o_accesorio(nombre)
            self.assertTrue(ok, nombre)
            if palabra:
                self.assertEqual(p, palabra, nombre)
        self.assertEqual(F.parece_piso_o_accesorio(""), (False, ""))
        self.assertEqual(F.parece_piso_o_accesorio(None), (False, ""))

    def test_normalizar_y_claves(self):
        self.assertEqual(F.normalizar("  Rampa-Esquina Ñandú  "), "rampa esquina nandu")
        self.assertEqual(F.clave_doc("FCV 0011117"), F.clave_doc("fcv-11117"))
        self.assertNotEqual(F.clave_doc("FCV 11117"), F.clave_doc("NVV 11117"))
        self.assertEqual(F.clave_sku(" rfcr-gb-20 "), "RFCR-GB-20")


class TestArmarPista(unittest.TestCase):
    def test_trae_todo_lo_que_el_asistente_necesita_de_un_producto(self):
        p = F.armar_pista("rfcr-gb-20", "Rubber EPDM Flooring Corner Ramp", 10, {"clase": "pisos", "clase_nombre": "Pisos", "repetible": True},
                          guardada={"plantilla_id": 7, "nombre": "Instalación Pisos", "por": "Aarón", "cuando": "05/11/2026 10:00"},
                          de_clase={"plantilla_id": 9, "nombre": "Pisos"})
        self.assertEqual(p["sku"], "RFCR-GB-20")
        self.assertEqual(p["conteo"]["valor"], "cantidad")
        self.assertEqual(p["guardada"]["plantilla_id"], 7)
        self.assertEqual(p["de_clase"]["plantilla_id"], 9)
        self.assertEqual((p["clase_nombre"], p["repetible"]), ("Pisos", True))

    def test_sin_nada_todo_es_nulo_y_no_se_inventa(self):
        p = F.armar_pista("X1", "Cosa", 1)
        self.assertEqual((p["guardada"], p["de_clase"], p["clase"]), (None, None, None))
        self.assertEqual(p["conteo"]["origen"], "una_unidad")


class TestValidarAgrupar(unittest.TestCase):
    def filas(self, n=4, **kw):
        return [fila(i, **kw) for i in range(1, n + 1)]

    def test_juntar_deja_la_de_menor_id_y_suma_las_cantidades(self):
        ok, cod, txt, d = F.validar_agrupar(self.filas(10), 5, ahora=AHORA)
        self.assertTrue(ok, (cod, txt))
        self.assertEqual((d["principal_id"], d["total"]), (1, 10))
        self.assertEqual(d["bajas"], list(range(2, 11)))
        ok, _c, _t, d2 = F.validar_agrupar([fila(7, cantidad=3), fila(5, cantidad=2)], 5, ahora=AHORA)
        self.assertTrue(ok)
        self.assertEqual((d2["principal_id"], d2["total"], d2["bajas"]), (5, 5, [7]))

    def test_cada_motivo_de_rechazo_tiene_su_codigo_y_su_frase(self):
        casos = (
            ("POCAS", [fila(1)], {}),
            ("CLIENTE", self.filas(2), {"cliente": 9}),
            ("BAJA", [fila(1), fila(2, estado="baja")], {}),
            ("SKU", [fila(1), fila(2, sku="OTRO-SKU")], {}),
            ("DOC", [fila(1), fila(2, doc_origen="FCV 555")], {}),
            ("USADA", [fila(1), fila(2, origen_visita_id=77)], {}),
            ("USADA", self.filas(2), {"en_uso": {2}}),
            ("ANTIGUA", [fila(1), fila(2, created_at=AHORA - dt.timedelta(days=8))], {}),
            ("MUCHAS", [fila(i) for i in range(1, F.MAX_FILAS_AGRUPAR + 2)], {}),
        )
        for esperado, filas, extra in casos:
            ok, cod, txt, d = F.validar_agrupar(filas, extra.get("cliente", 5), ahora=AHORA, en_uso=extra.get("en_uso", ()))
            self.assertFalse(ok, esperado)
            self.assertEqual(cod, esperado)
            self.assertTrue(txt and txt != esperado, esperado)

    def test_el_mismo_documento_con_o_sin_ceros_cuenta_como_el_mismo(self):
        ok, *_ = F.validar_agrupar([fila(1, doc_origen="FCV 0011117"), fila(2, doc_origen="FCV 11117")], 5, ahora=AHORA)
        self.assertTrue(ok)

    def test_sin_fecha_de_creacion_manda_la_guarda_de_que_ninguna_OT_la_usa(self):
        ok, *_ = F.validar_agrupar([fila(1, created_at=None), fila(2, created_at=None)], 5, ahora=AHORA)
        self.assertTrue(ok)


class TestValidarSeparar(unittest.TestCase):
    def test_multiplicar_una_fila_de_18_crea_17_y_la_deja_en_1(self):
        ok, cod, txt, d = F.validar_separar(fila(3, cantidad=18), 5, ahora=AHORA)
        self.assertTrue(ok, (cod, txt))
        self.assertEqual(d, {"modo": "multiplicar", "crear": 17, "cantidad_principal": 1})

    def test_si_antes_se_juntaron_filas_vuelven_las_mismas(self):
        agr = [{"hermana_id": i, "cantidad_hermana": 1, "estado_hermana": "baja"} for i in (2, 3, 4)]
        ok, _c, _t, d = F.validar_separar(fila(1, cantidad=4), 5, ahora=AHORA, agrupacion=agr)
        self.assertTrue(ok)
        self.assertEqual((d["modo"], d["restaurar"], d["cantidad_principal"]), ("revertir", [2, 3, 4], 1))

    def test_una_hermana_que_ya_no_esta_en_baja_no_se_restaura(self):
        agr = [{"hermana_id": 2, "cantidad_hermana": 1, "estado_hermana": "baja"}, {"hermana_id": 3, "cantidad_hermana": 1, "estado_hermana": "activo"}]
        _ok, _c, _t, d = F.validar_separar(fila(1, cantidad=3), 5, ahora=AHORA, agrupacion=agr)
        self.assertEqual((d["restaurar"], d["cantidad_principal"]), ([2], 2))

    def test_rechazos(self):
        casos = (
            ("NO_ENCONTRADA", None, {}),
            ("CLIENTE", fila(1, cantidad=3), {"cliente": 9}),
            ("BAJA", fila(1, cantidad=3, estado="baja"), {}),
            ("USADA", fila(1, cantidad=3, origen_visita_id=4), {}),
            ("USADA", fila(1, cantidad=3), {"en_uso": {1}}),
            ("ANTIGUA", fila(1, cantidad=3, created_at=AHORA - dt.timedelta(days=30)), {}),
            ("UNA", fila(1, cantidad=1), {}),
            ("MUCHAS_UNIDADES", fila(1, cantidad=F.MAX_UNIDADES_SEPARAR + 1), {}),
        )
        for esperado, f, extra in casos:
            ok, cod, txt, _d = F.validar_separar(f, extra.get("cliente", 5), ahora=AHORA, en_uso=extra.get("en_uso", ()))
            self.assertFalse(ok, esperado)
            self.assertEqual(cod, esperado)
            self.assertTrue(txt)

    def test_frases_para_la_persona(self):
        self.assertIn("cantidad 10", F.resumen_agrupar("Rampa", 10))
        self.assertIn("volver a separarlas", F.resumen_agrupar("Rampa", 10))
        self.assertIn("cada una con su serie", F.resumen_separar("Rampa", "multiplicar", 10))
        self.assertIn("vuelve con su serie", F.resumen_separar("Rampa", "revertir", 10))


# ═══════════════════════════════════════════════════════════════════════════════════════════════════
#  2. Los endpoints REALES de app.py en un mundo de mentira (base en memoria con transacción)
# ═══════════════════════════════════════════════════════════════════════════════════════════════════
class Resp:
    def __init__(self, data):
        self.data = data
        self.headers = {}


def _s(sql):
    return " ".join(str(sql).split())


class Mundo:
    """Las tablas que tocan los endpoints, en memoria. La conexión `get_mysql()` lleva una transacción de verdad: un rollback devuelve todo como estaba."""

    def __init__(self):
        self.maq = {}
        self.agr = []
        self.visita_equipos, self.visita_tareas = set(), set()
        self.plant = {7: {"nombre": "Instalación · Pisos", "activa": 1, "categoria_admin": "instalacion", "items": 4},
                      8: {"nombre": "Mantención · Pisos", "activa": 1, "categoria_admin": "mantencion", "items": 3},
                      9: {"nombre": "Pisos", "activa": 1, "categoria_admin": "instalacion", "items": 5},
                      10: {"nombre": "Vacía", "activa": 1, "categoria_admin": "instalacion", "items": 0},
                      11: {"nombre": "Apagada", "activa": 0, "categoria_admin": "instalacion", "items": 2}}
        self.chk = {}                    # (sku, categoria) -> fila
        self.cat_productos = {}
        self.logs, self.prints, self.sql = [], [], []
        self.usuario = "Daniel"
        self.fallar_uso = False
        self.fallar_insert = 0           # cuántos INSERT de mant_maquinas fallan con «Duplicate entry» antes de pasar
        self.siguiente = 100
        self.cuerpo = {}
        self.clases = {}
        self.tipo_categoria = {"instalacion": "instalacion", "preventiva": "mantencion"}

    def poner(self, *filas):
        for f in filas:
            self.maq[f["id"]] = dict(f)

    # ── lecturas del pool ──
    def fetchall(self, sql, params=()):
        s = _s(sql)
        self.sql.append(s)
        if "FROM mant_visita_equipos" in s:
            if self.fallar_uso:
                raise RuntimeError("tabla caída")
            return [{"m": i} for i in params if i in self.visita_equipos]
        if "FROM mant_visita_tareas" in s:
            return [{"m": i} for i in params if i in self.visita_tareas]
        if "FROM cat_producto_checklist g JOIN mant_tarea_plantillas p" in s:
            cat = params[0]
            skus = {str(x).upper() for x in params[1:]}
            out = []
            for (sku, c), r in self.chk.items():
                p = self.plant.get(r["plantilla_id"])
                if c == cat and sku.upper() in skus and p and p["activa"] and p["items"] > 0:
                    out.append({"sku": sku, "plantilla_id": r["plantilla_id"], "actualizado_por": r["actualizado_por"],
                                "actualizado_en": r["actualizado_en"], "nombre": p["nombre"]})
            return out
        if "SELECT serie FROM mant_maquinas WHERE cliente_id" in s:
            pref = params[1].rstrip("%")
            return [{"serie": m["serie"]} for m in self.maq.values() if m["cliente_id"] == params[0] and m["serie"].startswith(pref)]
        if s.startswith("SELECT id, cliente_id, sku, nombre, serie, doc_origen") and "WHERE id IN" in s:
            return [dict(self.maq[i]) for i in params if i in self.maq]
        raise AssertionError("consulta inesperada (fetchall): " + s[:160])

    def fetchone(self, sql, params=()):
        s = _s(sql)
        self.sql.append(s)
        if s.startswith("SELECT rut FROM mant_clientes"):
            return {"rut": "77.038.558-K"}
        if "FROM cat_producto_checklist g LEFT JOIN mant_tarea_plantillas p" in s:
            r = self.chk.get((params[0], params[1]))
            if not r:
                return None
            return {"plantilla_id": r["plantilla_id"], "actualizado_por": r["actualizado_por"], "actualizado_en": r["actualizado_en"],
                    "nombre": self.plant[r["plantilla_id"]]["nombre"]}
        if s.startswith("SELECT p.id, p.nombre FROM mant_tarea_plantillas p WHERE p.id=%s"):
            p = self.plant.get(params[0])
            if p and p["activa"] and p["items"] > 0 and p["categoria_admin"] in (params[1], None):
                return {"id": params[0], "nombre": p["nombre"]}
            return None
        if s.startswith("SELECT nombre FROM mant_tarea_plantillas WHERE id"):
            p = self.plant.get(params[0])
            return {"nombre": p["nombre"]} if p else None
        raise AssertionError("consulta inesperada (fetchone): " + s[:160])

    def execute(self, sql, params=()):
        s = _s(sql)
        self.sql.append(s)
        if s.startswith("INSERT INTO cat_producto_checklist"):
            sku, cat, pid, quien = params
            ahora = dt.datetime(2026, 11, 5, 15, 30)
            self.chk[(sku, cat)] = {"plantilla_id": pid, "actualizado_por": quien, "actualizado_en": ahora}
            return
        if s.startswith("DELETE FROM cat_producto_checklist"):
            self.chk.pop((params[0], params[1]), None)
            return
        if s.startswith("UPDATE mant_maquinas n JOIN mant_maquinas p"):
            return
        raise AssertionError("ejecución inesperada: " + s[:160])

    # ── la conexión con transacción ──
    def conexion(self):
        return _Conn(self)


class _Cur:
    def __init__(self, conn):
        self.c = conn
        self.m = conn.m
        self.rows = []
        self.lastrowid = None

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=()):
        s = _s(sql)
        m = self.m
        m.sql.append(s)
        self.rows = []
        if s.startswith("SELECT id, cliente_id, sku, nombre, serie, doc_origen") and "WHERE id IN" in s:
            self.rows = [dict(m.maq[i]) for i in params if i in m.maq]
        elif s.startswith("SELECT id, cliente_id, sku, nombre, serie, doc_origen") and "WHERE id=%s AND cliente_id=%s" in s:
            f = m.maq.get(params[0])
            self.rows = [dict(f)] if f and f["cliente_id"] == params[1] else []
        elif s.startswith("SELECT a.hermana_id, a.cantidad_hermana"):
            self.rows = [{"hermana_id": a["hermana_id"], "cantidad_hermana": a["cantidad_hermana"],
                          "estado_hermana": m.maq[a["hermana_id"]]["estado"]}
                         for a in m.agr if a["principal_id"] == params[0] and a["revertida_en"] is None]
        elif s.startswith("UPDATE mant_maquinas SET cantidad=%s, updated_by=%s WHERE id=%s"):
            m.maq[params[2]]["cantidad"] = params[0]
        elif s.startswith("UPDATE mant_maquinas SET estado='baja'"):
            for i in params[1:]:
                m.maq[i]["estado"] = "baja"
        elif s.startswith("UPDATE mant_maquinas SET estado='activo'"):
            for i in params[1:]:
                if m.maq[i]["estado"] == "baja":
                    m.maq[i]["estado"] = "activo"
        elif s.startswith("UPDATE mant_maquinas SET cantidad=1, updated_by=%s WHERE id=%s"):
            m.maq[params[1]]["cantidad"] = 1
        elif s.startswith("INSERT INTO mant_maquinas_agrupacion"):
            cid, principal, hermana, cant, quien = params
            m.agr.append({"cliente_id": cid, "principal_id": principal, "hermana_id": hermana, "cantidad_hermana": cant, "creada_por": quien,
                          "revertida_en": None})
        elif s.startswith("UPDATE mant_maquinas_agrupacion SET revertida_por"):
            for a in m.agr:
                if a["principal_id"] == params[1] and a["revertida_en"] is None:
                    a["revertida_en"] = "ahora"
        elif s.startswith("INSERT INTO mant_maquinas (cliente_id, sku"):
            serie, quien, origen = params
            if m.fallar_insert > 0:
                m.fallar_insert -= 1
                raise RuntimeError("(1062, \"Duplicate entry '77038558-GB20-2' for key 'uq_cliente_serie'\")")
            if any(x["serie"] == serie and x["cliente_id"] == m.maq[origen]["cliente_id"] for x in m.maq.values()):
                raise RuntimeError("(1062, \"Duplicate entry\")")
            m.siguiente += 1
            base = dict(m.maq[origen], id=m.siguiente, serie=serie, cantidad=1, estado="activo", origen_visita_id=None,
                        created_at=AHORA, created_by=quien)
            m.maq[m.siguiente] = base
            self.lastrowid = m.siguiente
        else:
            raise AssertionError("SQL inesperado en la transacción: " + s[:200])

    def fetchall(self):
        return list(self.rows)

    def fetchone(self):
        return self.rows[0] if self.rows else None


class _Conn:
    def __init__(self, mundo):
        self.m = mundo
        self.snap = copy.deepcopy((mundo.maq, mundo.agr))
        self.cerrada = False

    def cursor(self):
        return _Cur(self)

    def commit(self):
        self.snap = copy.deepcopy((self.m.maq, self.m.agr))

    def rollback(self):
        self.m.maq, self.m.agr = copy.deepcopy(self.snap)

    def close(self):
        self.cerrada = True


FUNCS = ("_oef_int", "_oef_plantillas_guardadas", "_oef_plantilla_de_clase", "ot2_api_equipos_pistas", "ot2_api_producto_checklist",
         "_maq_ids_en_uso", "_oef_fila_json", "_oef_rechazo", "ot2_api_equipos_agrupar", "ot2_api_equipos_separar", "_generar_serie_ilus",
         "_ensure_cat_producto_checklist", "_ensure_mant_maquinas_agrupacion", "mant_plantilla_sugerida_por_sku")


class Mundial(unittest.TestCase):
    def setUp(self):
        self.w = Mundo()
        w = self.w
        self.amb = {
            "request": types.SimpleNamespace(get_json=lambda silent=True: w.cuerpo, args=types.SimpleNamespace(get=lambda k, d=None: w.cuerpo.get(k, d))),
            "jsonify": lambda d: Resp(d), "datetime": dt.datetime, "_oef": F, "print": lambda *a, **k: w.prints.append(" ".join(map(str, a))),
            "current_username": lambda: w.usuario, "get_mysql": w.conexion, "mysql_fetchall": w.fetchall, "mysql_fetchone": w.fetchone,
            "mysql_execute": w.execute, "chile_fmt_filter": lambda v: v.strftime("%d/%m/%Y %H:%M") if hasattr(v, "strftime") else str(v),
            "_mant_log": lambda ent, eid, acc, det="": w.logs.append((ent, eid, acc, det)),
            "_sku_es_servicio": lambda sku, nombre=None: str(sku or "").upper().startswith("ZZ"),
            "_categoria_admin_para_tipo": lambda tipo: w.tipo_categoria.get((tipo or "").strip().lower()),
            "_inc_clasificacion_skus_batch": lambda skus: {s: w.clases[s] for s in skus if s in w.clases},
            "_plantilla_por_clasificacion_sku": lambda sku, tipo: 9 if (sku or "").upper() == "CON-CLASE" and tipo == "instalacion" else None,
            "_MAQ_COLS_SEL": "id, cliente_id, sku, nombre, serie, doc_origen, COALESCE(cantidad,1) AS cantidad, COALESCE(estado,'activo') AS estado, "
                             "origen_visita_id, created_at",
            "_OEF_MAX_PISTAS": 80, "re": re, "time": types.SimpleNamespace(time=lambda: 0),
        }
        # `_MAQ_COLS_SEL` real, leído del código (si cambia el SELECT, la prueba lo ve)
        FC.extraer(self.amb, "app.py", FUNCS, ("_MAQ_COLS_SEL", "_OEF_MAX_PISTAS"))

    def llamar(self, nombre, cuerpo):
        self.w.cuerpo = cuerpo
        r = self.amb[nombre]()
        if isinstance(r, tuple):
            return r[1], r[0].data
        return 200, r.data


class TestJuntarFilas(Mundial):
    def diez(self):
        self.w.poner(*[fila(i) for i in range(1, 11)])

    def test_diez_filas_quedan_en_una_con_cantidad_10_y_las_demas_en_baja_sin_borrar_nada(self):
        self.diez()
        st, d = self.llamar("ot2_api_equipos_agrupar", {"cliente_id": 5, "ids": list(range(1, 11))})
        self.assertEqual(st, 200, d)
        self.assertEqual((d["principal"]["id"], d["total"], d["bajas"]), (1, 10, list(range(2, 11))))
        self.assertEqual(self.w.maq[1]["cantidad"], 10)
        self.assertTrue(all(self.w.maq[i]["estado"] == "baja" for i in range(2, 11)))
        self.assertEqual(len(self.w.maq), 10)                                 # NADA se borró
        self.assertEqual(len(self.w.agr), 9)
        self.assertIn("Quedó una sola fila con cantidad 10", d["mensaje"])
        accs = [a for (_e, _i, a, _d) in self.w.logs]
        self.assertEqual(accs.count("agrupada_en"), 9)
        self.assertIn("equipos_agrupados", accs)

    def test_un_rechazo_no_cambia_nada(self):
        self.diez()
        self.w.visita_equipos.add(4)                                          # la fila 4 ya está en una OT
        antes = copy.deepcopy(self.w.maq)
        st, d = self.llamar("ot2_api_equipos_agrupar", {"cliente_id": 5, "ids": list(range(1, 11))})
        self.assertEqual((st, d["error_codigo"]), (409, "USADA"))
        self.assertEqual(self.w.maq, antes)
        self.assertEqual(self.w.agr, [])

    def test_si_no_se_puede_verificar_el_uso_no_se_sigue_a_ciegas(self):
        self.diez()
        self.w.fallar_uso = True
        st, d = self.llamar("ot2_api_equipos_agrupar", {"cliente_id": 5, "ids": [1, 2]})
        self.assertEqual((st, d["error_codigo"]), (503, "SIN_VERIFICAR"))
        self.assertEqual(self.w.maq[1]["cantidad"], 1)

    def test_pide_al_menos_dos_filas_y_valida_el_cliente(self):
        self.diez()
        st, d = self.llamar("ot2_api_equipos_agrupar", {"cliente_id": 5, "ids": [1]})
        self.assertEqual((st, d["error_codigo"]), (400, "POCAS"))
        st, d = self.llamar("ot2_api_equipos_agrupar", {"cliente_id": 9, "ids": [1, 2]})
        self.assertEqual((st, d["error_codigo"]), (409, "CLIENTE"))
        st, d = self.llamar("ot2_api_equipos_agrupar", {"cliente_id": 5, "ids": [1, 999]})
        self.assertEqual((st, d["error_codigo"]), (409, "NO_ENCONTRADA"))

    def test_las_filas_con_cantidad_se_suman(self):
        self.w.poner(fila(1, cantidad=3), fila(2, cantidad=4))
        st, d = self.llamar("ot2_api_equipos_agrupar", {"cliente_id": 5, "ids": [1, 2]})
        self.assertEqual((st, d["total"]), (200, 7))


class TestSepararFilas(Mundial):
    def test_revertir_devuelve_las_mismas_filas_con_sus_series(self):
        self.w.poner(*[fila(i) for i in range(1, 5)])
        self.llamar("ot2_api_equipos_agrupar", {"cliente_id": 5, "ids": [1, 2, 3, 4]})
        series = {i: self.w.maq[i]["serie"] for i in range(1, 5)}
        st, d = self.llamar("ot2_api_equipos_separar", {"cliente_id": 5, "id": 1})
        self.assertEqual(st, 200, d)
        self.assertEqual((d["modo"], d["restauradas"], d["nuevas"]), ("revertir", [2, 3, 4], []))
        self.assertEqual([self.w.maq[i]["estado"] for i in range(1, 5)], ["activo"] * 4)
        self.assertEqual(self.w.maq[1]["cantidad"], 1)
        self.assertEqual({i: self.w.maq[i]["serie"] for i in range(1, 5)}, series)         # las MISMAS series
        self.assertEqual(len(self.w.maq), 4)                                                 # no se creó nada
        self.assertTrue(all(a["revertida_en"] for a in self.w.agr))
        self.assertEqual(len(d["filas"]), 4)

    def test_multiplicar_una_fila_de_18_crea_17_nuevas_con_series_unicas(self):
        self.w.poner(fila(1, cantidad=18, serie="77038558-GB20-1"))
        st, d = self.llamar("ot2_api_equipos_separar", {"cliente_id": 5, "id": 1})
        self.assertEqual(st, 200, d)
        self.assertEqual(d["modo"], "multiplicar")
        self.assertEqual(len(d["nuevas"]), 17)
        self.assertEqual(self.w.maq[1]["cantidad"], 1)
        series = [m["serie"] for m in self.w.maq.values()]
        self.assertEqual(len(series), 18)
        self.assertEqual(len(set(series)), 18)                                              # ninguna repetida
        nuevas = [self.w.maq[i] for i in d["nuevas"]]
        self.assertTrue(all((m["cantidad"], m["estado"], m["origen_visita_id"]) == (1, "activo", None) for m in nuevas))
        self.assertTrue(all(m["sku"] == "RFCR-GB-20" and m["doc_origen"] == "VD 11117" for m in nuevas))      # heredan producto y documento
        self.assertIn("«Rubber EPDM Flooring Corner Ramp G/B 20mm» quedó en 18 filas", d["mensaje"])

    def test_un_choque_de_serie_se_reintenta_con_otra(self):
        self.w.poner(fila(1, cantidad=3, serie="77038558-GB20-1"))
        self.w.fallar_insert = 2
        st, d = self.llamar("ot2_api_equipos_separar", {"cliente_id": 5, "id": 1})
        self.assertEqual(st, 200, d)
        self.assertEqual(len({m["serie"] for m in self.w.maq.values()}), 3)

    def test_juntar_despues_de_multiplicar_y_volver_a_separar_cierra_el_circulo(self):
        self.w.poner(fila(1, cantidad=5, serie="77038558-GB20-1"))
        _st, d = self.llamar("ot2_api_equipos_separar", {"cliente_id": 5, "id": 1})
        ids = [1] + d["nuevas"]
        st, d2 = self.llamar("ot2_api_equipos_agrupar", {"cliente_id": 5, "ids": ids})
        self.assertEqual((st, d2["total"]), (200, 5))
        st, d3 = self.llamar("ot2_api_equipos_separar", {"cliente_id": 5, "id": 1})
        self.assertEqual((st, d3["modo"], sorted(d3["restauradas"])), (200, "revertir", sorted(d["nuevas"])))

    def test_lo_que_ya_tiene_historia_no_se_separa_y_no_cambia_nada(self):
        self.w.poner(fila(1, cantidad=4))
        self.w.visita_tareas.add(1)
        st, d = self.llamar("ot2_api_equipos_separar", {"cliente_id": 5, "id": 1})
        self.assertEqual((st, d["error_codigo"]), (409, "USADA"))
        self.assertEqual((len(self.w.maq), self.w.maq[1]["cantidad"]), (1, 4))

    def test_sobre_el_tope_se_deja_como_cantidad(self):
        self.w.poner(fila(1, cantidad=F.MAX_UNIDADES_SEPARAR + 1))
        st, d = self.llamar("ot2_api_equipos_separar", {"cliente_id": 5, "id": 1})
        self.assertEqual((st, d["error_codigo"]), (409, "MUCHAS_UNIDADES"))
        self.assertEqual(len(self.w.maq), 1)

    def test_una_fila_de_una_unidad_no_se_separa(self):
        self.w.poner(fila(1))
        st, d = self.llamar("ot2_api_equipos_separar", {"cliente_id": 5, "id": 1})
        self.assertEqual((st, d["error_codigo"]), (409, "UNA"))

    def test_faltan_datos(self):
        st, d = self.llamar("ot2_api_equipos_separar", {"cliente_id": 5})
        self.assertEqual((st, d["error_codigo"]), (400, "NO_ENCONTRADA"))
        st, d = self.llamar("ot2_api_equipos_separar", {"cliente_id": 5, "id": 77})
        self.assertEqual((st, d["error_codigo"]), (404, "NO_ENCONTRADA"))


class TestRecordarChecklist(Mundial):
    BASE = {"sku": "RFCR-GB-20", "tipo": "instalacion", "plantilla_id": 7, "nombre": "Rampa"}

    def test_guarda_con_quien_y_cuando_y_lo_deja_en_la_bitacora_del_catalogo(self):
        st, d = self.llamar("ot2_api_producto_checklist", dict(self.BASE))
        self.assertEqual((st, d["ok"], d["guardado"]), (200, True, True))
        r = self.w.chk[("RFCR-GB-20", "instalacion")]
        self.assertEqual((r["plantilla_id"], r["actualizado_por"]), (7, "Daniel"))
        self.assertEqual([a for (_e, _i, a, _d) in self.w.logs], ["checklist_producto"])
        self.assertIn("«Instalación · Pisos»", self.w.logs[0][3])

    def test_es_idempotente_y_el_mismo_usuario_puede_cambiarlo(self):
        self.llamar("ot2_api_producto_checklist", dict(self.BASE))
        st, d = self.llamar("ot2_api_producto_checklist", dict(self.BASE))
        self.assertTrue(d["sin_cambio"])
        st, d = self.llamar("ot2_api_producto_checklist", dict(self.BASE, plantilla_id=9))
        self.assertTrue(d["guardado"] and d["reemplazo"])
        self.assertEqual(self.w.chk[("RFCR-GB-20", "instalacion")]["plantilla_id"], 9)

    def test_no_pisa_en_silencio_lo_que_dejo_otra_persona(self):
        self.w.usuario = "Aarón"
        self.llamar("ot2_api_producto_checklist", dict(self.BASE))
        self.w.usuario = "Daniel"
        st, d = self.llamar("ot2_api_producto_checklist", dict(self.BASE, plantilla_id=9))
        self.assertEqual((d["guardado"], d["motivo"]), (False, "ya_guardada"))
        self.assertEqual((d["actual"]["plantilla_id"], d["actual"]["por"], d["actual"]["nombre"]), (7, "Aarón", "Instalación · Pisos"))
        self.assertEqual(self.w.chk[("RFCR-GB-20", "instalacion")]["plantilla_id"], 7)           # sigue el de Aarón
        st, d = self.llamar("ot2_api_producto_checklist", dict(self.BASE, plantilla_id=9, reemplazar=True))
        self.assertTrue(d["guardado"])
        self.assertEqual(self.w.chk[("RFCR-GB-20", "instalacion")]["actualizado_por"], "Daniel")
        self.assertIn("antes: «Instalación · Pisos» de Aarón", self.w.logs[-1][3])

    def test_un_checklist_que_no_sirve_no_se_guarda(self):
        for pid in (8, 10, 11, 999):                  # de otra categoría · vacío · apagado · inexistente
            st, d = self.llamar("ot2_api_producto_checklist", dict(self.BASE, plantilla_id=pid))
            self.assertEqual(st, 400, pid)
            self.assertFalse(d["ok"])
        self.assertEqual(self.w.chk, {})

    def test_cada_categoria_de_OT_tiene_su_propio_checklist_por_producto(self):
        self.llamar("ot2_api_producto_checklist", dict(self.BASE))
        st, d = self.llamar("ot2_api_producto_checklist", dict(self.BASE, tipo="preventiva", plantilla_id=8))
        self.assertTrue(d["guardado"])
        self.assertEqual({k[1] for k in self.w.chk}, {"instalacion", "mantencion"})

    def test_dejar_de_recordar(self):
        self.llamar("ot2_api_producto_checklist", dict(self.BASE))
        st, d = self.llamar("ot2_api_producto_checklist", dict(self.BASE, plantilla_id=0))
        self.assertEqual((st, d["olvidado"]), (200, True))
        self.assertEqual(self.w.chk, {})
        self.assertIn("checklist_producto_olvidado", [a for (_e, _i, a, _d) in self.w.logs])

    def test_faltan_datos_y_los_servicios_no_son_productos(self):
        st, d = self.llamar("ot2_api_producto_checklist", {"sku": "", "tipo": "instalacion", "plantilla_id": 7})
        self.assertEqual(st, 400)
        st, d = self.llamar("ot2_api_producto_checklist", {"sku": "X", "tipo": "tipo_raro", "plantilla_id": 7})
        self.assertEqual(st, 400)
        st, d = self.llamar("ot2_api_producto_checklist", dict(self.BASE, sku="ZZINSTALACION"))
        self.assertEqual((d["guardado"], d["motivo"]), (False, "es_servicio"))

    def test_el_producto_se_da_de_alta_en_el_catalogo_como_cotizaciones(self):
        altas = []
        self.amb["_cat_crear_o_reusar_producto_desde_erp"] = lambda sku, nombre="", **kw: altas.append((sku, nombre, kw)) or {"id": 1, "creado": True}
        st, d = self.llamar("ot2_api_producto_checklist", dict(self.BASE))
        self.assertEqual(altas, [("RFCR-GB-20", "Rampa", {"traer_foto": False})])
        self.assertTrue(d["producto_creado"])


class TestPistas(Mundial):
    def test_un_solo_viaje_para_todos_los_productos_con_su_sugerencia(self):
        self.w.clases = {"PISO-1": {"clase": "pisos", "clase_nombre": "Pisos", "repetible": True}, "RACK-1": {"clase": "racks", "clase_nombre": "Racks", "repetible": False}}
        self.w.chk[("RFCR-GB-20", "instalacion")] = {"plantilla_id": 7, "actualizado_por": "Aarón", "actualizado_en": dt.datetime(2026, 11, 5, 9, 0)}
        st, d = self.llamar("ot2_api_equipos_pistas", {"tipo": "instalacion", "items": [
            {"sku": "rfcr-gb-20", "nombre": "Rubber EPDM Flooring Corner Ramp", "cantidad": 6},
            {"sku": "RFCR-GB-20", "nombre": "", "cantidad": 4},                      # el mismo SKU en otra línea: las cantidades se suman
            {"sku": "PISO-1", "nombre": "Piso", "cantidad": 2},
            {"sku": "RACK-1", "nombre": "Rack", "cantidad": 3},
            {"sku": "CON-CLASE", "nombre": "Banco", "cantidad": 1},
            {"sku": "ZZINSTALACION", "nombre": "Servicio", "cantidad": 1}]})
        self.assertEqual(st, 200, d)
        s = d["skus"]
        self.assertEqual(set(s), {"RFCR-GB-20", "PISO-1", "RACK-1", "CON-CLASE"})          # el servicio no es un producto
        self.assertEqual(s["RFCR-GB-20"]["conteo"]["valor"], "cantidad")                    # 6 + 4 = 10 unidades, parece un piso
        self.assertEqual(s["RFCR-GB-20"]["guardada"]["plantilla_id"], 7)
        self.assertEqual(s["RFCR-GB-20"]["guardada"]["por"], "Aarón")
        self.assertEqual(s["PISO-1"]["conteo"]["origen"], "clase")
        self.assertEqual(s["RACK-1"]["conteo"]["valor"], "unidad")
        self.assertEqual(s["CON-CLASE"]["de_clase"], {"plantilla_id": 9, "nombre": "Pisos"})
        self.assertEqual(d["categoria"], "instalacion")

    def test_sin_tipo_no_hay_checklists_pero_si_conteo(self):
        st, d = self.llamar("ot2_api_equipos_pistas", {"items": [{"sku": "A", "nombre": "Tatami", "cantidad": 5}]})
        self.assertEqual(st, 200)
        self.assertEqual((d["skus"]["A"]["guardada"], d["skus"]["A"]["de_clase"], d["skus"]["A"]["conteo"]["valor"]), (None, None, "cantidad"))

    def test_basura_en_la_entrada_no_rompe(self):
        for cuerpo in ({}, {"items": "no es lista"}, {"items": [None, 5, {"nombre": "sin sku"}]}):
            st, d = self.llamar("ot2_api_equipos_pistas", cuerpo)
            self.assertEqual((st, d["skus"]), (200, {}), cuerpo)

    def test_el_endpoint_de_sugerencia_por_fila_ahora_prefiere_lo_guardado_para_el_producto(self):
        self.w.chk[("RFCR-GB-20", "instalacion")] = {"plantilla_id": 7, "actualizado_por": "Aarón", "actualizado_en": dt.datetime(2026, 11, 5, 9, 0)}
        st, d = self.llamar("mant_plantilla_sugerida_por_sku", {"sku": "RFCR-GB-20", "tipo_ot": "instalacion"})
        self.assertEqual((d["plantilla_id"], d["origen"], d["plantilla_nombre"]), (7, "producto", "Instalación · Pisos"))
        st, d = self.llamar("mant_plantilla_sugerida_por_sku", {"sku": "CON-CLASE", "tipo_ot": "instalacion"})
        self.assertEqual((d["plantilla_id"], d["origen"]), (9, "clase"))
        st, d = self.llamar("mant_plantilla_sugerida_por_sku", {"sku": "NADA", "tipo_ot": "instalacion"})
        self.assertEqual((d["plantilla_id"], d["origen"]), (None, None))


# ═══════════════════════════════════════════════════════════════════════════════════════════════════
#  3. Cableado y reglas del proyecto
# ═══════════════════════════════════════════════════════════════════════════════════════════════════
APP = FC.texto("app.py")


def _decoradores(ruta):
    i = APP.index(f'@app.route("{ruta}"')
    return APP[i:APP.index("def ", i)]


class TestCableado(unittest.TestCase):
    def test_las_cuatro_rutas_exigen_sesion_y_no_son_de_un_tecnico(self):
        for ruta in ("/ot/api/equipos/pistas", "/ot/api/producto/checklist", "/ot/api/equipos/agrupar", "/ot/api/equipos/separar"):
            dec = _decoradores(ruta)
            self.assertIn("@_mant_required", dec, ruta)
            self.assertIn("@_no_tecnico", dec, ruta)
            self.assertIn('methods=["POST"]', dec, ruta)

    def test_las_tablas_se_crean_con_un_solo_create_barato_y_al_arrancar(self):
        for fn, tabla in (("_ensure_cat_producto_checklist", "cat_producto_checklist"), ("_ensure_mant_maquinas_agrupacion", "mant_maquinas_agrupacion")):
            src = FC.funcion_top("app.py", fn)
            self.assertEqual(src.count(f"CREATE TABLE IF NOT EXISTS {tabla}"), 1, fn)
            self.assertNotIn("ALTER", src, fn)
            self.assertIn(f"        {fn}()", APP, fn)                    # se llama en el arranque (REGLA #18: barata si ya existe)

    def test_juntar_y_separar_leen_con_candado_de_fila_y_nada_se_borra(self):
        for fn in ("ot2_api_equipos_agrupar", "ot2_api_equipos_separar"):
            src = FC.funcion_top("app.py", fn)
            self.assertIn("FOR UPDATE", src, fn)
            self.assertIn("conn.rollback()", src, fn)
            self.assertNotRegex(src, r"DELETE\s+FROM\s+mant_maquinas", fn)       # nada se borra: se da de baja y se restaura (REGLA #5)
        self.assertIn("estado='baja'", FC.funcion_top("app.py", "ot2_api_equipos_agrupar"))

    def test_la_sugerencia_por_fila_sigue_aditiva(self):
        src = FC.funcion_top("app.py", "mant_plantilla_sugerida_por_sku")
        self.assertIn('"origen": "producto"', src)
        self.assertIn('"plantilla_nombre": nombre, "origen"', src)

    def test_el_alta_de_equipos_sigue_siendo_la_de_siempre(self):
        add = FC.funcion_top("app.py", "mant_maquina_add")
        self.assertIn("split_to_rows", add)
        self.assertIn("respetar_clasificacion", add)

    def test_el_modulo_puro_no_depende_de_flask_ni_de_la_base(self):
        src = FC.texto("ot_equipos_filas.py")
        self.assertNotRegex(src, r"^\s*(import|from)\s+(flask|pymysql|app)\b", re.M)


if __name__ == "__main__":
    unittest.main()

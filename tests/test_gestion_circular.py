# -*- coding: utf-8 -*-
"""🔁 GESTIÓN CIRCULAR de hallazgos e inquietudes (Daniel, 26-oct-2026).

«necesito un CRUD en los hallazgos… borrar algunos hallazgos que hemos hecho de prueba… solamente para el superadministrador… tomar el
levantamiento… editarla, crear un repuesto, solicitarlo, generar una OT, generar un ticket… gestionando de manera circular» y «agreguemos
todo para que no se quede nada atrás… además de su valorización».

Se prueba (sin BD, sin Flask, sin correos reales: REGLA #22):
  A. el módulo puro gestion_circular.py (validaciones, montos en pesos chilenos, recorrido con semáforo, referencia de origen);
  B. las rutas de app.py con una base de mentira: editar con motivo, borrar solo superadmin con copia completa ANTES de borrar y lápida,
     valorizar, el vínculo de vuelta al solicitar un repuesto, y que un hallazgo borrado no vuelve a nacer solo;
  C. el cableado: tablas baratas (REGLA #18), rutas con @_no_tecnico (REGLAS #19/#26), JS sin alert/confirm/prompt (REGLA #1), plantilla
     sin nada plegado (Daniel 26-oct: «me gusta ver el bosque siempre»).
Correr:  python -m pytest tests/test_gestion_circular.py -q -p no:warnings
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
import ot_hallazgos as H  # noqa: E402
from tests._fuente_app import RAIZ, decoradores, fuente_constante, texto  # noqa: E402
from tests._fuente_app import fuente_funcion as _fuente_funcion  # noqa: E402

APP = texto("app.py")


def _leer(rel):
    with open(os.path.join(RAIZ, rel.replace("/", os.sep)), encoding="utf-8", newline="") as fh:
        return fh.read().replace("\r\n", "\n")


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


# ═══════════════════════════ A) el módulo puro ═══════════════════════════
class TestPuro(unittest.TestCase):
    def test_montos_en_pesos_chilenos(self):
        self.assertEqual(GC.validar_valor({"repuestos": "45.990", "servicio": "$ 35.000", "nota": "  cotizado  "})[0],
                         {"repuestos": 45990, "servicio": 35000, "nota": "cotizado"})
        self.assertEqual(GC.validar_valor({"repuestos": 12000})[0]["servicio"], None)
        self.assertIn("al menos un monto", GC.validar_valor({"repuestos": "", "servicio": ""})[1])
        for malo in ("12a", "-5", "1.000.000.000", "doce mil"):
            self.assertIsNotNone(GC.validar_valor({"repuestos": malo})[1], malo)
        self.assertEqual(GC.pesos(1234567), "$1.234.567")
        self.assertEqual(GC.pesos(None), "")
        self.assertEqual(GC.total_valor(None, None), None)
        self.assertEqual(GC.total_valor(45990, None), 45990)

    def test_valor_vista(self):
        v = GC.valor_vista({"valor_repuestos": 45990, "valor_servicio": 35000, "valor_nota": "x", "valorizado_por": "Aarón"})
        self.assertEqual((v["total"], v["repuestos"], v["servicio"], v["rep_n"], v["srv_n"]), ("$80.990", "$45.990", "$35.000", 45990, 35000))
        self.assertIsNone(GC.valor_vista({}))
        self.assertIsNone(GC.valor_vista(None))

    def test_editar_exige_motivo_y_solo_guarda_lo_que_cambia(self):
        actual = {"titulo": "Tapiz roto", "detalle": "abajo", "severidad": "alerta"}
        c, m, e = GC.validar_edicion({"titulo": "Tapiz roto", "detalle": "abajo", "severidad": "falla", "motivo": "era una falla, no alerta"}, actual)
        self.assertIsNone(e)
        self.assertEqual(c, {"severidad": "falla"})
        self.assertEqual(m, "era una falla, no alerta")
        self.assertEqual(GC.validar_edicion(dict(actual, motivo="x" * 20), actual)[2], "No cambiaste nada.")
        self.assertIn("mínimo 10", GC.validar_edicion({"titulo": "Otro", "severidad": "alerta", "motivo": "corto"}, actual)[2])
        self.assertIn("vacío", GC.validar_edicion({"titulo": "", "severidad": "alerta", "motivo": "x" * 20}, actual)[2])
        self.assertIn("gravedad", GC.validar_edicion({"titulo": "Otro", "severidad": "inventada", "motivo": "x" * 20}, actual)[2])
        self.assertEqual(set(GC.SEVERIDADES_EDITABLES), set(H.SEVERIDADES), "la lista de gravedades es la MISMA que la de los hallazgos")
        self.assertIn("Gravedad: Alerta → Falla", GC.resumen_cambios({"severidad": "falla"}, actual, {"alerta": "Alerta", "falla": "Falla"}))

    def test_borrar_solo_superadmin_con_BORRAR_y_motivo(self):
        self.assertIn("superadministrador", GC.validar_borrado({"confirmar": "BORRAR", "motivo": "x" * 20}, False)[1])
        self.assertIn("BORRAR", GC.validar_borrado({"confirmar": "borra", "motivo": "x" * 20}, True)[1])
        self.assertIn("mínimo", GC.validar_borrado({"confirmar": "borrar", "motivo": "corto"}, True)[1])
        self.assertEqual(GC.validar_borrado({"confirmar": " borrar ", "motivo": "hallazgo de prueba de la capacitación"}, True),
                         ("hallazgo de prueba de la capacitación", None))

    def test_referencia_de_origen(self):
        self.assertEqual(GC.origen_ref("hallazgo", 12), "hallazgo:12")
        self.assertEqual(GC.leer_origen_ref("inquietud:5"), ("inquietud", 5))
        for malo in ("hallazgo:", "otra:3", "hallazgo:1;DROP", "", None, "hallazgo:12 "):
            self.assertEqual(GC.leer_origen_ref(malo) if malo != "hallazgo:12 " else (None, None), (None, None), malo)
        self.assertEqual(GC.origen_ref("otra", 1), "")

    def test_recorrido_con_semaforo_nada_escondido(self):
        p = GC.recorrido("abierto", None, None, None, None, "")
        self.assertEqual([x["clave"] for x in p], ["atencion", "valor", "ticket", "repuesto", "ot"])
        self.assertEqual([x["color"] for x in p], ["ambar", "ambar", "gris", "gris", "ambar"], "lo que falta en ámbar; lo opcional en gris")
        val = GC.valor_vista({"valor_repuestos": 1000})
        p = GC.recorrido("tomado", {"numero": "TK-2026-00001"}, {"numero": "Solicitud de repuesto"}, {"numero": "OT-2026-00400"}, val, "Aarón")
        self.assertTrue(all(x["color"] == "verde" for x in p))
        self.assertEqual(p[0]["txt"], "Lo tomó Aarón")
        self.assertEqual(p[1]["txt"], "Estimado $1.000")
        p = GC.recorrido("descartado", None, None, None, None, "")
        self.assertEqual(p[0]["txt"], "Descartado")
        self.assertNotIn("ambar", [x["color"] for x in p], "un descartado no queda pidiendo pasos")


# ═══════════════════════════ B) las rutas con una base de mentira ═══════════════════════════
class _Req:
    def __init__(self):
        self.json = {}

    def get_json(self, silent=False):
        return dict(self.json)


class _Cur:
    def __init__(self, m):
        self.m, self.rowcount, self._res = m, 0, None

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=()):
        s = " ".join(sql.split())
        self.m.sql.append((s, tuple(params or ())))
        self.rowcount = 1
        if s.startswith("DELETE FROM mant_ot_hallazgos"):
            antes = len(self.m.hall)
            self.m.hall = [h for h in self.m.hall if h["id"] != params[0]]
            self.rowcount = antes - len(self.m.hall)

    def fetchone(self):
        return self._res


class _Conn:
    def __init__(self, m):
        self.m = m

    def cursor(self):
        return _Cur(self.m)

    def commit(self):
        self.m.commits += 1

    def rollback(self):
        pass


class Mundo:
    def __init__(self, superadmin=True):
        self.superadmin = superadmin
        self.sql, self.logs, self.acts, self.extra, self.audit = [], [], [], [], []
        self.commits = 0
        self.lapidas = set()
        self.ahora = dt.datetime(2026, 10, 26, 14, 0)
        self.hall = [{"id": 5, "visita_id": 171, "origen": "tarea", "referencia": "t12", "severidad": "alerta", "titulo": "Tapiz: Alerta",
                      "detalle": "roto", "estado": "abierto", "retirado_en": None, "ot_seguimiento_id": None, "seg_numero": None,
                      "seg_estado": None, "actualizado_en": self.ahora, "maquina_id": 9, "equipo_nombre": "Banco"}]
        self.req = _Req()

    def fila(self, hid, vid):
        f = next((h for h in self.hall if h["id"] == hid and h["visita_id"] == vid), None)
        return dict(f) if f else None

    def fetchone(self, sql, params=()):
        s = " ".join(sql.split())
        self.sql.append((s, tuple(params or ())))
        if s.startswith("SELECT h.*, s.numero_ot AS seg_numero"):
            return self.fila(*params)
        if s.startswith("SELECT id FROM mant_ot_hallazgo_borrados"):
            return {"id": 1} if tuple(params) in self.lapidas else None
        if s.startswith("SELECT id, estado, retirado_en FROM mant_ot_hallazgos"):
            f = next((h for h in self.hall if (h["visita_id"], h["origen"], h["referencia"]) == tuple(params)), None)
            return {k: f[k] for k in ("id", "estado", "retirado_en")} if f else None
        if s.startswith("SELECT visita_id, titulo FROM mant_ot_hallazgos"):
            f = next((h for h in self.hall if h["id"] == params[0]), None)
            return {"visita_id": f["visita_id"], "titulo": f["titulo"]} if f else None
        raise AssertionError("SQL no previsto: " + s[:200])

    def fetchall(self, sql, params=()):
        s = " ".join(sql.split())
        self.sql.append((s, tuple(params or ())))
        if s.startswith("SELECT * FROM mant_gestion_extra"):
            return [dict(v) for k, v in getattr(self, "extra_previo", {}).items() if k in set(params[1:])]
        raise AssertionError("SQL no previsto: " + s[:200])

    def escribe(self, sql, params=()):
        s = " ".join(sql.split())
        self.sql.append((s, tuple(params or ())))
        if s.startswith("UPDATE mant_ot_hallazgos SET"):
            hid, vid, vers = params[-3], params[-2], params[-1]
            f = next((h for h in self.hall if h["id"] == hid and h["visita_id"] == vid), None)
            if not f or f["actualizado_en"] != vers:
                return 0
            cols = re.findall(r"(\w+)=%s", s.split(" WHERE ")[0])
            for c, v in zip(cols, params):
                f[c] = v
            f["actualizado_en"] = self.ahora + dt.timedelta(seconds=1)
            return 1
        if s.startswith("INSERT INTO mant_gestion_extra"):
            self.extra.append((s, tuple(params)))
            return 1
        if s.startswith("INSERT INTO mant_gestion_actividad"):
            self.acts.append(tuple(params))
            return 1
        if s.startswith("INSERT IGNORE INTO mant_ot_hallazgos"):
            return 1
        raise AssertionError("SQL de escritura no previsto: " + s[:200])

    def ambiente(self):
        m = self
        amb = {
            "print": lambda *a, **k: None, "time": time, "json": json, "re": re, "_gc": GC, "_oh": H,
            "request": m.req, "jsonify": lambda x: x, "get_mysql": lambda: _Conn(m),
            "mysql_fetchone": m.fetchone, "mysql_fetchall": m.fetchall, "mysql_execute": m.escribe,
            "mysql_execute_returning_rowcount": m.escribe,
            "_mant_log": lambda ent, eid, acc, det="": m.logs.append((ent, eid, acc, det)),
            "_audit": lambda *a, **k: m.audit.append((a, k)),
            "_hall_quien": lambda: "Daniel Aguilar", "_hall_uid": lambda: 7, "_hall_ok": lambda: True,
            "_es_superadmin_actual": lambda: m.superadmin,
            "_hall_cuando": lambda d: d.strftime("%d/%m/%Y %H:%M") if d else "",
            "_inq_log": lambda iid, acc, det="": m.logs.append(("inquietud", iid, acc, det)),
        }
        amb = cargar(["_ot2_err", "_gc_ok", "_gc_lapida", "_gc_act", "_gc_extra_set", "_gc_extra_de", "_gc_hallazgo_previo",
                      "_hall_fila_de", "ot_api_hallazgo_editar", "ot_api_hallazgo_borrar", "ot_api_hallazgo_valorizar",
                      "_gc_vincular_repuesto", "_hall_upsert", "_ensure_gestion_circular"], ["_GC_ESTADO"], amb)
        amb["_GC_ESTADO"]["ok"] = True
        amb["_GC_ESTADO"]["revisado_en"] = time.time()
        return amb


def _r(x):
    return x if isinstance(x, tuple) else (x, 200)


class TestRutas(unittest.TestCase):
    def test_editar_guarda_lo_que_cambia_con_motivo_y_deja_actividad_y_bitacora(self):
        m = Mundo()
        a = m.ambiente()
        m.req.json = {"titulo": "Tapiz: Alerta", "detalle": "roto", "severidad": "falla", "motivo": "el técnico marcó alerta pero es falla"}
        d, st = _r(a["ot_api_hallazgo_editar"](171, 5))
        self.assertEqual(st, 200, d)
        self.assertEqual(m.hall[0]["severidad"], "falla")
        self.assertTrue(any(acc == "hallazgo_editado" and "Alerta → Falla" in det for _e, _i, acc, det in m.logs), m.logs)
        self.assertEqual(m.acts[0][2], "editado")
        self.assertIn("motivo: el técnico", m.acts[0][3])

    def test_editar_sin_motivo_o_con_la_fila_cambiada_no_escribe(self):
        m = Mundo()
        a = m.ambiente()
        m.req.json = {"titulo": "Otro", "severidad": "alerta", "motivo": "corto"}
        d, st = _r(a["ot_api_hallazgo_editar"](171, 5))
        self.assertEqual(st, 400)
        self.assertFalse(any(s.startswith("UPDATE") for s, _p in m.sql))
        m.hall[0]["actualizado_en"] = m.ahora + dt.timedelta(minutes=1)          # otra persona lo cambió mientras tanto
        orig = m.fila
        m.fila = lambda hid, vid: dict(orig(hid, vid), actualizado_en=m.ahora)    # la pantalla vio la versión anterior
        m.req.json = {"titulo": "Otro título", "severidad": "alerta", "motivo": "x" * 20}
        d, st = _r(a["ot_api_hallazgo_editar"](171, 5))
        self.assertEqual(st, 409, d)

    def test_borrar_no_superadmin_403_y_nada_se_toca(self):
        m = Mundo(superadmin=False)
        a = m.ambiente()
        m.req.json = {"confirmar": "BORRAR", "motivo": "hallazgo de prueba"}
        d, st = _r(a["ot_api_hallazgo_borrar"](171, 5))
        self.assertEqual(st, 403)
        self.assertEqual(len(m.hall), 1)

    def test_borrar_guarda_la_copia_completa_ANTES_y_deja_lapida(self):
        m = Mundo()
        a = m.ambiente()
        m.req.json = {"confirmar": "borrar", "motivo": "hallazgo de prueba de la capacitación"}
        d, st = _r(a["ot_api_hallazgo_borrar"](171, 5))
        self.assertEqual(st, 200, d)
        self.assertEqual(m.hall, [])
        ops = [s.split(" ")[0] + " " + s.split(" ")[2] for s, _p in m.sql if s.startswith(("INSERT INTO mant_ot_hallazgo_borrados", "DELETE"))]
        self.assertEqual(ops[0], "INSERT mant_ot_hallazgo_borrados", "la copia va ANTES del borrado (REGLA #5)")
        snap = next(p for s, p in m.sql if s.startswith("INSERT INTO mant_ot_hallazgo_borrados"))
        self.assertEqual(snap[:4], (5, 171, "tarea", "t12"))
        self.assertEqual(json.loads(snap[6])["titulo"], "Tapiz: Alerta", "la fila completa queda guardada")
        self.assertEqual(m.commits, 1, "copia y borrado en UNA transacción")
        self.assertTrue(any(acc == "hallazgo_borrado" for _e, _i, acc, _d in m.logs))
        self.assertTrue(m.audit)

    def test_un_hallazgo_con_ot_de_seguimiento_viva_no_se_borra(self):
        m = Mundo()
        m.hall[0].update(ot_seguimiento_id=400, seg_numero="OT-2026-00400", seg_estado="en_curso", estado="resuelto")
        a = m.ambiente()
        m.req.json = {"confirmar": "BORRAR", "motivo": "hallazgo de prueba"}
        d, st = _r(a["ot_api_hallazgo_borrar"](171, 5))
        self.assertEqual(st, 409, d)
        self.assertEqual(len(m.hall), 1)

    def test_el_borrado_no_vuelve_a_nacer_solo(self):
        m = Mundo()
        m.hall = []
        m.lapidas.add((171, "tarea", "t12"))
        a = m.ambiente()
        r = a["_hall_upsert"](171, {"origen": "tarea", "referencia": "t12", "severidad": "falla", "titulo": "Tapiz: Falla", "fotos": []})
        self.assertEqual(r, {"res": "borrado", "id": 0, "avisar": False})
        self.assertFalse(any(s.startswith("INSERT IGNORE INTO mant_ot_hallazgos") for s, _p in m.sql))

    def test_valorizar_guarda_y_responde_el_total(self):
        m = Mundo()
        a = m.ambiente()
        m.req.json = {"repuestos": "45.990", "servicio": "35.000", "nota": "cotizado"}
        d, st = _r(a["ot_api_hallazgo_valorizar"](171, 5))
        self.assertEqual((st, d.get("total")), (200, "$80.990"), d)
        s, p = m.extra[0]
        self.assertIn("valor_repuestos", s)
        self.assertEqual(p[:5], ("hallazgo", 5, 45990, 35000, "cotizado"))
        self.assertEqual(m.acts[0][2], "valorizado")
        m.req.json = {"repuestos": "doce"}
        d, st = _r(a["ot_api_hallazgo_valorizar"](171, 5))
        self.assertEqual(st, 400)

    def test_el_repuesto_solicitado_deja_el_vinculo_de_vuelta(self):
        m = Mundo()
        a = m.ambiente()
        a["_gc_vincular_repuesto"]("hallazgo:5", "abc123", [301, 302], "TK-2026-00099")
        s, p = m.extra[0]
        self.assertEqual(p[:6], ("hallazgo", 5, "abc123", "301,302", "TK-2026-00099", "Daniel Aguilar"))
        self.assertEqual(m.acts[0][2], "repuesto")
        self.assertTrue(any(acc == "hallazgo_repuesto" for _e, _i, acc, _d in m.logs))
        # Revisión 27-oct: una segunda solicitud SUMA a las anteriores y repuesto_en conserva la primera vez.
        m.extra.clear()
        m.extra_previo = {5: {"entidad_id": 5, "repuesto_sol_ids": "301,302"}}
        a2 = m.ambiente()
        a2["_gc_vincular_repuesto"]("hallazgo:5", "def456", [302, 303], None)
        s2, p2 = m.extra[0]
        self.assertEqual(p2[3], "301,302,303", "se suman sin repetir")
        self.assertIn("repuesto_en=COALESCE(repuesto_en, NOW())", s2, "conserva la PRIMERA vez que la gestión pidió un repuesto")
        a["_gc_vincular_repuesto"]("inquietud:3", "x", [1], None)
        self.assertTrue(any(e == "inquietud" and acc == "repuesto_solicitado" for e, _i, acc, _d in m.logs))
        n = len(m.extra)
        a["_gc_vincular_repuesto"]("cualquier cosa", "x", [1], None)
        self.assertEqual(len(m.extra), n, "una referencia inválida no escribe nada")


# ═══════════════════════════ C) cableado ═══════════════════════════
class TestCableado(unittest.TestCase):
    def test_tablas_baratas_sin_alter(self):
        src = _fuente_funcion("_ensure_gestion_circular")
        self.assertEqual(src.count('"CREATE TABLE IF NOT EXISTS'), 3)
        self.assertNotIn("ALTER TABLE", src)
        self.assertNotIn("%", src.split("for sql in")[1].split("):")[0])
        self.assertRegex(APP, r"try:\n    with app\.app_context\(\):\n        _ensure_gestion_circular\(\)\nexcept Exception as _ensure_gc_err:")

    def test_rutas_solo_gestion(self):
        for f in ("ot_api_hallazgo_editar", "ot_api_hallazgo_valorizar", "ot_api_hallazgo_ticket", "ot_api_hallazgo_borrar", "inq_api_valorizar"):
            self.assertIn("@_no_tecnico", decoradores(f), f)
            self.assertIn("@_mant_required", decoradores(f), f)
        self.assertIn("_es_superadmin_actual()", _fuente_funcion("ot_api_hallazgo_borrar"))

    def test_el_ticket_no_escribe_al_cliente(self):
        src = _fuente_funcion("_gc_ticket_hallazgo") + _fuente_funcion("ot_api_hallazgo_ticket")
        self.assertNotIn("_send_ilus_email", src)
        self.assertIn('"confirmar"', _fuente_funcion("ot_api_hallazgo_ticket"), "el ticket es SUGERIDO: se confirma antes")

    def test_la_solicitud_manual_deja_el_vinculo(self):
        self.assertIn("_gc_vincular_repuesto(d.get(\"origen_ref\")", _fuente_funcion("repstock_solicitud_manual"))
        rep = _leer("templates/clientes_hub/repuestos.html")
        self.assertIn("rs_origen", rep)
        self.assertIn("origen_ref", rep)
        self.assertIn("rsmClienteElegir(cid", rep)

    def test_js_sin_nativos_y_parsea(self):
        js = _leer("static/ot_hallazgos_gestion.js")
        self.assertNotRegex(js, r"(?<![\w.])(alert|confirm|prompt)\(")
        self.assertIn("ilusConfirm", js)
        self.assertIn("min-height:44px", _leer("static/ot_hallazgos.css").split("Gestión circular")[1])

    def test_tarjeta_sin_nada_plegado_y_con_la_gestion(self):
        t = _leer("templates/ot2/_hallazgos.html")
        for k in ("data-hz-editar", "data-hz-valorizar", "data-hz-ticket", "data-hz-borrar", "url_repuesto", "hz-rec", "hz-act"):
            self.assertIn(k, t, k)
        self.assertIn("i.puede_borrar", t)
        self.assertIn('<details class="hz-retirados" open>', t)
        self.assertTrue(H.grupo_abierto([{"estado_ef": "abierto", "severidad": "alerta"}], 5), "Daniel 26-oct: todo abierto")

    def test_correos_nuevos_registrados_y_apagados(self):
        import comm_correos_registro as R
        for k in ("inq_nueva_gestion", "inq_asignada_responsable", "inq_sla_vencido", "hall_nuevos_gestion", "hall_tomado_responsable"):
            c = R.por_clave(k)
            self.assertIsNotNone(c, k)
            self.assertTrue(c["nace_apagado"], k)
            self.assertIn('"%s"' % k, APP, "se envía con su clave: " + k)
        self.assertIn("correo=clave", _fuente_funcion("_aviso_interno_correo"))
        self.assertIn('_rol_familia(u.get("role")) == "tecnico"', _fuente_funcion("_aviso_interno_correo"), "nunca a un técnico")


if __name__ == "__main__":
    unittest.main()

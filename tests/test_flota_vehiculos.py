# -*- coding: utf-8 -*-
"""🚚 FLOTA · «Solicitudes de vehículo» (Fase 1a) — Daniel, 02-nov-2026:
«lo que estamos necesitando en este minuto es poder registrar los datos de los camiones, tenerlos habilitados, poder habilitar un nuevo
camión… En el menú solamente un nombre, "solicitudes de vehículo"… Quiero solo verlo yo de momento.»

Decisiones de Daniel que se vigilan:
  1. La flota NO se fija en el código: cada vehículo se ingresa a mano (alta), se edita y se habilita / inhabilita con motivo. No se borra.
  2. Este módulo NO maneja precios ni costos.
  3. HOY solo lo ve el superadmin: ni el menú ni las rutas se abren a nadie más.
  4. Un solo ítem en el menú («Solicitudes de vehículo»), con href fijo (un url_for roto tumbaría TODAS las pantallas).
  5. Todo cambio queda en una bitácora que solo se agrega (nunca se modifica ni se borra).

Se prueba (sin BD real, sin red, sin correos):
  A. las reglas puras (flota_reglas.py);
  B. las rutas con una base de mentira: permisos, alta, duplicados, edición, habilitar / inhabilitar, historial;
  C. el cableado (registro en app.py, tablas en app_context, ítem del menú) y los candados de «sin precios» y «bitácora inmutable»;
  D. la plantilla RENDERIZADA y que todo id que usa el JS exista en ella; el JS pasa `node --check`.
Correr:  python -m pytest tests/test_flota_vehiculos.py -q -p no:warnings
"""
import datetime as dt
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import flota_reglas as R  # noqa: E402

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
HAY_NODE = shutil.which("node") is not None


def _leer(*partes):
    with open(os.path.join(RAIZ, *partes), encoding="utf-8") as f:
        return f.read()


HOY = dt.date(2026, 11, 2)


# ═══════════════════════════════════════════════════════════════════════════
#  A. Reglas puras
# ═══════════════════════════════════════════════════════════════════════════
class TestPatente(unittest.TestCase):
    def test_normaliza(self):
        self.assertEqual(R.normalizar_patente(" ab-12 34 "), "AB1234")
        self.assertEqual(R.normalizar_patente("abcd·12"), "ABCD12")
        self.assertEqual(R.normalizar_patente(None), "")

    def test_valida(self):
        for ok in ("AB1234", "ABCD12", "ABC123", "A1B2C3"):
            self.assertTrue(R.patente_valida(ok), ok)
        for mal in ("", "ABCD", "123456", "ABCDEF", "AB12", "ABCDEFGH1"):
            self.assertFalse(R.patente_valida(mal), mal)

    def test_con_guion(self):
        self.assertEqual(R.patente_con_guion("AB1234"), "AB-1234")
        self.assertEqual(R.patente_con_guion("ABCD12"), "ABCD-12")
        self.assertEqual(R.patente_con_guion("ABC123"), "ABC123")


class TestClases(unittest.TestCase):
    def test_normaliza_y_ordena(self):
        self.assertEqual(R.normalizar_clases("b, a4"), ["A4", "B"])
        self.assertEqual(R.normalizar_clases(["B", "b", "A4"]), ["A4", "B"])
        self.assertEqual(R.normalizar_clases("B/A4 zz"), ["A4", "B"])

    def test_vacio_o_basura(self):
        self.assertEqual(R.normalizar_clases(None), [])
        self.assertEqual(R.normalizar_clases(""), [])
        self.assertEqual(R.normalizar_clases("Z9, XX"), [])


class TestNumerosYFechas(unittest.TestCase):
    def test_entero_con_punto_de_miles(self):
        self.assertEqual(R._entero("12.500", 0, 3000000), 12500)
        self.assertEqual(R._entero("12500", 0, 3000000), 12500)
        self.assertEqual(R._entero(" ", 0, 10), None)
        self.assertEqual(R._entero(None, 0, 10), None)

    def test_entero_rechaza(self):
        for mal in ("12,5", "abc", "-3", "1.5", True):
            with self.assertRaises(ValueError, msg=str(mal)):
                R._entero(mal, 0, 3000000)
        with self.assertRaises(ValueError):
            R._entero(3000001, 0, 3000000)

    def test_decimal_con_coma(self):
        self.assertEqual(R._decimal("8,5", 0, 200), 8.5)
        self.assertEqual(R._decimal("8.55", 0, 200), 8.55)
        with self.assertRaises(ValueError):
            R._decimal("ocho", 0, 200)
        with self.assertRaises(ValueError):
            R._decimal("nan", 0, 200)

    def test_fecha(self):
        self.assertEqual(R.fecha_o_none("2026-12-31"), dt.date(2026, 12, 31))
        self.assertEqual(R.fecha_o_none(""), None)
        self.assertEqual(R.fecha_o_none("2026-12-31T10:00:00"), dt.date(2026, 12, 31))
        for mal in ("31/12/2026", "2026-13-01", "1999-01-01", "2101-01-01"):
            with self.assertRaises(ValueError, msg=mal):
                R.fecha_o_none(mal)


class TestVencimientos(unittest.TestCase):
    def test_estados(self):
        self.assertEqual(R.estado_vencimiento(None, HOY), None)
        self.assertEqual(R.estado_vencimiento(HOY - dt.timedelta(days=1), HOY), "vencido")
        self.assertEqual(R.estado_vencimiento(HOY, HOY), "por_vencer")
        self.assertEqual(R.estado_vencimiento(HOY + dt.timedelta(days=30), HOY), "por_vencer")
        self.assertEqual(R.estado_vencimiento(HOY + dt.timedelta(days=31), HOY), "vigente")

    def test_texto_en_lenguaje_de_persona(self):
        self.assertEqual(R.texto_dias(0), "Vence hoy")
        self.assertEqual(R.texto_dias(1), "Vence mañana")
        self.assertEqual(R.texto_dias(12), "Vence en 12 días")
        self.assertEqual(R.texto_dias(-1), "Venció ayer")
        self.assertEqual(R.texto_dias(-5), "Venció hace 5 días")

    def test_fecha_corta_nunca_iso(self):
        self.assertEqual(R.fecha_corta(dt.date(2026, 3, 9)), "09/03/2026")
        self.assertEqual(R.fecha_corta(None), "")

    def test_resumen_y_peor_estado(self):
        fila = {"revision_tecnica_vence": HOY + dt.timedelta(days=200), "permiso_circulacion_vence": HOY + dt.timedelta(days=10),
                "soap_vence": HOY - dt.timedelta(days=3), "seguro_vence": None}
        docs = R.resumen_documentos(fila, HOY)
        self.assertEqual([d["clave"] for d in docs], ["revision_tecnica", "permiso_circulacion", "soap", "seguro"])
        self.assertEqual([d["estado"] for d in docs], ["vigente", "por_vencer", "vencido", "sin_fecha"])
        self.assertEqual(docs[2]["texto"], "Venció hace 3 días")
        self.assertEqual(docs[3]["texto"], "Sin fecha registrada")
        self.assertEqual(R.peor_estado(docs), "vencido")

    def test_peor_estado_orden(self):
        mk = lambda *e: [{"estado": x} for x in e]  # noqa: E731
        self.assertEqual(R.peor_estado(mk("vigente", "vigente")), "vigente")
        self.assertEqual(R.peor_estado(mk("vigente", "sin_fecha")), "incompleto")
        self.assertEqual(R.peor_estado(mk("sin_fecha", "por_vencer")), "por_vencer")
        self.assertEqual(R.peor_estado(mk("por_vencer", "vencido")), "vencido")


class TestValidarVehiculo(unittest.TestCase):
    BUENO = {"patente": "ab-12 34", "tipo": "  Furgón ", "marca": "Hyundai", "modelo": "H1", "anio": "2019", "km_actual": "85.400",
             "capacidad_kg": "1.200", "capacidad_m3": "8,5", "licencia_clases": ["B"], "soap_vence": "2027-03-31"}

    def test_caso_bueno_normaliza_todo(self):
        d, e = R.validar_vehiculo(self.BUENO, HOY)
        self.assertEqual(e, {})
        self.assertEqual(d["patente"], "AB1234")
        self.assertEqual(d["tipo"], "Furgón")
        self.assertEqual(d["km_actual"], 85400)
        self.assertEqual(d["capacidad_kg"], 1200)
        self.assertEqual(d["capacidad_m3"], 8.5)
        self.assertEqual(d["licencia_clases"], "B")
        self.assertEqual(d["soap_vence"], dt.date(2027, 3, 31))
        self.assertIsNone(d["seguro_vence"])
        self.assertEqual(set(d), set(R.COLUMNAS))

    def test_obligatorios(self):
        d, e = R.validar_vehiculo({}, HOY)
        self.assertEqual(set(e), {"patente", "tipo", "licencia_clases"})

    def test_errores_por_campo(self):
        d, e = R.validar_vehiculo({"patente": "AB", "tipo": "x", "anio": "1900", "km_actual": "mucho", "licencia_clases": "Z",
                                   "soap_vence": "mañana", "notas": "x" * 1001}, HOY)
        self.assertEqual(set(e), {"patente", "tipo", "anio", "km_actual", "licencia_clases", "soap_vence", "notas"})

    def test_anio_no_pasa_del_proximo(self):
        _, e = R.validar_vehiculo({**self.BUENO, "anio": str(HOY.year + 2)}, HOY)
        self.assertIn("anio", e)
        _, e = R.validar_vehiculo({**self.BUENO, "anio": str(HOY.year + 1)}, HOY)
        self.assertNotIn("anio", e)

    def test_varias_clases(self):
        d, e = R.validar_vehiculo({**self.BUENO, "licencia_clases": "b, a4"}, HOY)
        self.assertEqual(e, {})
        self.assertEqual(d["licencia_clases"], "A4,B")

    def test_motivo(self):
        self.assertEqual(R.validar_motivo("  En   mantención  ")[0], "En mantención")
        self.assertIsNotNone(R.validar_motivo("no")[1])
        self.assertIsNotNone(R.validar_motivo("x" * 301)[1])
        self.assertIsNotNone(R.validar_motivo(None)[1])


class TestCambios(unittest.TestCase):
    def test_solo_lo_que_cambio(self):
        antes = {"km_actual": 12000, "capacidad_m3": dt.date, "soap_vence": dt.date(2026, 5, 1), "tipo": "Furgón", "marca": None}
        antes["capacidad_m3"] = __import__("decimal").Decimal("8.50")
        despues = {"km_actual": 12500, "capacidad_m3": 8.5, "soap_vence": dt.date(2026, 5, 1), "tipo": "Furgón", "marca": None}
        c = R.cambios(antes, despues)
        self.assertEqual(list(c), ["km_actual"])          # el DECIMAL 8.50 y el 8.5 son lo mismo
        self.assertEqual(c["km_actual"], {"antes": "12000", "despues": "12500"})

    def test_lineas_legibles(self):
        c = R.cambios({"km_actual": 12000, "marca": None}, {"km_actual": 12500, "marca": "Kia"})
        self.assertEqual(R.lineas_de_cambios(c), ["Marca: (vacío) → Kia", "Kilómetros: 12000 → 12500"])


# ═══════════════════════════════════════════════════════════════════════════
#  B. Rutas con una base de mentira
# ═══════════════════════════════════════════════════════════════════════════
class _Duplicado(Exception):
    pass


class BaseFalsa:
    """Imita, con las MISMAS sentencias que usa flota_module, las dos tablas de flota. Cualquier SQL que no conozca falla la prueba."""

    COLS = ("patente tipo marca modelo anio color vin capacidad_kg capacidad_m3 combustible estanque_l km_actual licencia_clases "
            "revision_tecnica_vence permiso_circulacion_vence soap_vence seguro_vence notas").split()

    def __init__(self):
        self.veh, self.log, self.sql, self.sig = {}, [], [], 1

    def _dup(self):
        e = _Duplicado("(1062, \"Duplicate entry 'X' for key 'uq_flota_patente'\")")
        e.args = (1062, "Duplicate entry")
        return e

    def fetchone(self, q, p=None):
        s = " ".join(q.split())
        p = p or ()
        if s == "SELECT * FROM flota_vehiculos WHERE id=%s":
            return dict(self.veh[p[0]]) if p[0] in self.veh else None
        if s == "SELECT * FROM flota_vehiculos WHERE patente=%s":
            return next((dict(v) for v in self.veh.values() if v["patente"] == p[0]), None)
        if s.startswith("SELECT COUNT(*) AS n FROM flota_solicitudes WHERE vehiculo_id=%s"):
            return {"n": 0}          # reservas por delante (fase 1b): acá no hay solicitudes
        raise AssertionError("fetchone no previsto: " + s[:90])

    def fetchall(self, q, p=None):
        s = " ".join(q.split())
        p = p or ()
        if s.startswith("SELECT * FROM flota_vehiculos ORDER BY habilitado DESC"):
            return sorted((dict(v) for v in self.veh.values()), key=lambda v: (-v["habilitado"], v["tipo"], v["patente"]))
        if s.startswith("SELECT id, accion, detalle, usuario, created_at FROM flota_vehiculo_log WHERE vehiculo_id=%s"):
            return [dict(x) for x in sorted(self.log, key=lambda x: -x["id"]) if x["vehiculo_id"] == p[0]]
        raise AssertionError("fetchall no previsto: " + s[:90])

    def execute(self, q, p=None):
        s = " ".join(q.split())
        p = p or ()
        self.sql.append(s)
        if s.startswith("CREATE TABLE IF NOT EXISTS"):
            return
        if s.startswith("INSERT INTO flota_vehiculos ("):
            cols = [c.strip() for c in s[s.index("(") + 1:s.index(")")].split(",")]
            vals = dict(zip(cols, p))
            if any(v["patente"] == vals["patente"] for v in self.veh.values()):
                raise self._dup()
            fila = {c: None for c in self.COLS}
            fila.update(habilitado=1, inhabilitado_motivo=None, inhabilitado_por=None, inhabilitado_at=None,
                        created_at=dt.datetime(2026, 11, 2, 15, 0), updated_at=dt.datetime(2026, 11, 2, 15, 0))
            fila.update(vals)
            fila["id"] = self.sig
            self.sig += 1
            self.veh[fila["id"]] = fila
            return
        if s.startswith("INSERT INTO flota_vehiculo_log"):
            self.log.append({"id": len(self.log) + 1, "vehiculo_id": p[0], "accion": p[1], "detalle": p[2], "usuario": p[3],
                             "created_at": dt.datetime(2026, 11, 2, 18, 30)})
            return
        if s.startswith("UPDATE flota_vehiculos SET patente=%s"):
            sets = re.findall(r"(\w+)=%s", s.split(" WHERE ")[0])
            nuevos = dict(zip(sets, p[:-1]))
            vid = p[-1]
            if any(v["id"] != vid and v["patente"] == nuevos["patente"] for v in self.veh.values()):
                raise self._dup()
            self.veh[vid].update(nuevos)
            return
        raise AssertionError("execute no previsto: " + s[:90])

    def rowcount(self, q, p=None):
        s = " ".join(q.split())
        p = p or ()
        self.sql.append(s)
        if s.startswith("UPDATE flota_vehiculos SET habilitado=0"):
            motivo, por, _por2, vid = p
            v = self.veh.get(vid)
            if not v or not v["habilitado"]:
                return 0
            v.update(habilitado=0, inhabilitado_motivo=motivo, inhabilitado_por=por, inhabilitado_at=dt.datetime(2026, 11, 2, 19, 0))
            return 1
        if s.startswith("UPDATE flota_vehiculos SET habilitado=1"):
            _por, vid = p
            v = self.veh.get(vid)
            if not v or v["habilitado"]:
                return 0
            v.update(habilitado=1, inhabilitado_motivo=None, inhabilitado_por=None, inhabilitado_at=None)
            return 1
        raise AssertionError("rowcount no previsto: " + s[:90])


def _app_de_prueba(base):
    from flask import Flask, g, redirect, request

    plantillas = tempfile.mkdtemp(prefix="flota_tpl_")
    os.makedirs(os.path.join(plantillas, "flota"))
    with open(os.path.join(plantillas, "base.html"), "w", encoding="utf-8") as f:
        f.write("<!doctype html><html><head>{% block head_extra %}{% endblock %}</head>"
                "<body>{% block content %}{% endblock %}{% block scripts %}{% endblock %}</body></html>")
    shutil.copy(os.path.join(RAIZ, "templates", "flota", "index.html"), os.path.join(plantillas, "flota", "index.html"))

    app = Flask(__name__, template_folder=plantillas)
    app.config["TESTING"] = True

    @app.route("/")
    def index():
        return "inicio"

    @app.before_request
    def _usuario():
        rol = request.headers.get("X-Rol")
        g.user = {"id": 1, "nombre": "Daniel Aguilar"} if rol else None
        g.permissions = {"superadmin": rol == "superadmin", "retiros": rol in ("admin", "superadmin"), "mantenciones": rol is not None}

    def login_required(view):
        from functools import wraps

        @wraps(view)
        def w(*a, **k):
            if not g.user:
                return redirect("/")
            return view(*a, **k)
        return w

    ctx = {"mysql_fetchone": base.fetchone, "mysql_fetchall": base.fetchall, "mysql_execute": base.execute,
           "mysql_execute_returning_rowcount": base.rowcount, "login_required": login_required,
           "current_username": lambda: "Daniel Aguilar"}
    import flota_module
    flota_module.register_flota_routes(app, ctx)
    return app


SUPER = {"X-Rol": "superadmin"}
OTRO = {"X-Rol": "admin"}
FUTURO = (dt.date.today() + dt.timedelta(days=400)).isoformat()
VENCIDO = (dt.date.today() - dt.timedelta(days=15)).isoformat()
POR_VENCER = (dt.date.today() + dt.timedelta(days=10)).isoformat()


def _veh(**k):
    d = {"patente": "ABCD12", "tipo": "Furgón", "marca": "Hyundai", "modelo": "H1", "anio": 2019, "licencia_clases": ["B"],
         "km_actual": 85400, "revision_tecnica_vence": FUTURO, "permiso_circulacion_vence": FUTURO, "soap_vence": FUTURO,
         "seguro_vence": FUTURO}
    d.update(k)
    return d


class TestRutas(unittest.TestCase):
    def setUp(self):
        self.base = BaseFalsa()
        self.app = _app_de_prueba(self.base)
        self.c = self.app.test_client()

    def crear(self, **k):
        r = self.c.post("/flota/api/vehiculos", json=_veh(**k), headers=SUPER)
        return r, r.get_json()

    # ── Quién entra ──
    def test_solo_superadmin_la_pagina(self):
        self.assertEqual(self.c.get("/flota", headers=SUPER).status_code, 200)
        r = self.c.get("/flota", headers=OTRO)
        self.assertEqual(r.status_code, 302)
        self.assertEqual(self.c.get("/flota").status_code, 302)       # sin sesión

    def test_solo_superadmin_la_api_y_no_escribe(self):
        for metodo, url in (("get", "/flota/api/vehiculos"), ("post", "/flota/api/vehiculos"), ("put", "/flota/api/vehiculos/1"),
                            ("post", "/flota/api/vehiculos/1/inhabilitar"), ("post", "/flota/api/vehiculos/1/habilitar"),
                            ("get", "/flota/api/vehiculos/1/historial")):
            kw = {"json": _veh()} if metodo in ("post", "put") else {}
            r = getattr(self.c, metodo)(url, headers=OTRO, **kw)
            self.assertEqual(r.status_code, 403, url)
            self.assertEqual(r.get_json()["error_codigo"], "SIN_PERMISO_FLOTA")
        self.assertEqual(self.base.veh, {})
        self.assertEqual([s for s in self.base.sql if not s.startswith("CREATE")], [])

    # ── Alta ──
    def test_alta_habilita_y_deja_bitacora(self):
        r, d = self.crear()
        self.assertEqual(r.status_code, 200)
        v = d["vehiculo"]
        self.assertTrue(v["habilitado"])
        self.assertEqual(v["patente"], "ABCD12")
        self.assertEqual(v["patente_fmt"], "ABCD-12")
        self.assertEqual(v["titulo"], "Furgón Hyundai H1 2019")
        self.assertEqual(v["licencia_texto"], "Clase B")
        self.assertEqual(v["docs_estado"], "vigente")
        self.assertEqual(self.base.log[0]["accion"], "creado")
        self.assertEqual(self.base.log[0]["usuario"], "Daniel Aguilar")

    def test_se_pueden_ingresar_mas_vehiculos(self):
        self.crear()
        self.crear(patente="WXYZ99", tipo="Camión", marca="Hino", modelo="300", anio=2022)
        d = self.c.get("/flota/api/vehiculos", headers=SUPER).get_json()
        self.assertEqual(d["resumen"]["total"], 2)
        self.assertEqual({v["tipo"] for v in d["vehiculos"]}, {"Furgón", "Camión"})

    def test_alta_patente_duplicada(self):
        self.crear()
        r, d = self.crear(patente="abcd-12")
        self.assertEqual(r.status_code, 409)
        self.assertIn("patente", d["errores"])
        self.assertEqual(len(self.base.veh), 1)

    def test_alta_invalida_no_toca_la_base(self):
        r = self.c.post("/flota/api/vehiculos", json={"patente": "x", "tipo": ""}, headers=SUPER)
        self.assertEqual(r.status_code, 400)
        self.assertTrue({"patente", "tipo", "licencia_clases"} <= set(r.get_json()["errores"]))
        self.assertEqual(self.base.veh, {})

    # ── Lista y resumen ──
    def test_lista_y_semaforo_de_documentos(self):
        self.crear(patente="AAAA11", soap_vence=VENCIDO)
        self.crear(patente="BBBB22", soap_vence=POR_VENCER)
        self.crear(patente="CCCC33")
        d = self.c.get("/flota/api/vehiculos", headers=SUPER).get_json()
        r = d["resumen"]
        self.assertEqual((r["total"], r["habilitados"], r["inhabilitados"], r["por_vencer"], r["vencidos"]), (3, 3, 0, 1, 1))
        por = {v["patente"]: v for v in d["vehiculos"]}
        self.assertEqual(por["AAAA11"]["docs_estado"], "vencido")
        self.assertEqual(por["BBBB22"]["docs_estado"], "por_vencer")
        self.assertEqual(por["CCCC33"]["docs_estado"], "vigente")
        soap = [x for x in por["AAAA11"]["documentos"] if x["clave"] == "soap"][0]
        self.assertRegex(soap["vence_fmt"], r"^\d{2}/\d{2}/\d{4}$")           # nunca ISO crudo (REGLA #6)
        self.assertIn("config", d)
        self.assertEqual(d["config"]["clase_sugerida"], "B")

    def test_vehiculo_sin_fechas_queda_incompleto(self):
        self.crear(revision_tecnica_vence=None, permiso_circulacion_vence=None, soap_vence=None, seguro_vence=None)
        v = self.c.get("/flota/api/vehiculos", headers=SUPER).get_json()["vehiculos"][0]
        self.assertEqual(v["docs_estado"], "incompleto")

    # ── Edición ──
    def test_editar_registra_lo_que_cambio(self):
        _, d = self.crear()
        vid = d["vehiculo"]["id"]
        r = self.c.put(f"/flota/api/vehiculos/{vid}", json=_veh(km_actual=90100, color="Blanco"), headers=SUPER)
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.get_json()["vehiculo"]["km_actual"], 90100)
        ev = self.c.get(f"/flota/api/vehiculos/{vid}/historial", headers=SUPER).get_json()["eventos"]
        self.assertEqual([e["accion"] for e in ev], ["editado", "creado"])
        self.assertIn("Kilómetros: 85400 → 90100", ev[0]["lineas"])
        self.assertIn("Color: (vacío) → Blanco", ev[0]["lineas"])
        self.assertRegex(ev[0]["fecha"], r"^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$")

    def test_editar_sin_cambios_no_escribe(self):
        _, d = self.crear()
        antes = len(self.base.log)
        r = self.c.put(f"/flota/api/vehiculos/{d['vehiculo']['id']}", json=_veh(), headers=SUPER)
        self.assertTrue(r.get_json()["sin_cambios"])
        self.assertEqual(len(self.base.log), antes)

    def test_editar_a_una_patente_de_otro_vehiculo(self):
        self.crear()
        _, d = self.crear(patente="WXYZ99")
        r = self.c.put(f"/flota/api/vehiculos/{d['vehiculo']['id']}", json=_veh(patente="ABCD12"), headers=SUPER)
        self.assertEqual(r.status_code, 409)

    def test_editar_inexistente(self):
        self.assertEqual(self.c.put("/flota/api/vehiculos/99", json=_veh(), headers=SUPER).status_code, 404)

    # ── Habilitar / inhabilitar ──
    def test_inhabilitar_pide_motivo(self):
        _, d = self.crear()
        vid = d["vehiculo"]["id"]
        r = self.c.post(f"/flota/api/vehiculos/{vid}/inhabilitar", json={"motivo": "no"}, headers=SUPER)
        self.assertEqual(r.status_code, 400)
        self.assertIn("motivo", r.get_json()["errores"])
        self.assertTrue(self.base.veh[vid]["habilitado"])

    def test_inhabilitar_y_habilitar(self):
        _, d = self.crear()
        vid = d["vehiculo"]["id"]
        r = self.c.post(f"/flota/api/vehiculos/{vid}/inhabilitar", json={"motivo": "En mantención en el taller"}, headers=SUPER)
        self.assertEqual(r.status_code, 200)
        v = r.get_json()["vehiculo"]
        self.assertFalse(v["habilitado"])
        self.assertEqual(v["inhabilitado_motivo"], "En mantención en el taller")
        self.assertEqual(v["inhabilitado_por"], "Daniel Aguilar")
        self.assertRegex(v["inhabilitado_fmt"], r"^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$")
        # una segunda vez no pisa el motivo de la primera
        r2 = self.c.post(f"/flota/api/vehiculos/{vid}/inhabilitar", json={"motivo": "Otro motivo distinto"}, headers=SUPER)
        self.assertEqual(r2.status_code, 409)
        self.assertEqual(self.base.veh[vid]["inhabilitado_motivo"], "En mantención en el taller")
        # habilitar
        r3 = self.c.post(f"/flota/api/vehiculos/{vid}/habilitar", json={}, headers=SUPER)
        self.assertEqual(r3.status_code, 200)
        self.assertTrue(r3.get_json()["vehiculo"]["habilitado"])
        self.assertIsNone(r3.get_json()["vehiculo"]["inhabilitado_motivo"])
        self.assertEqual(self.c.post(f"/flota/api/vehiculos/{vid}/habilitar", json={}, headers=SUPER).status_code, 409)
        ev = self.c.get(f"/flota/api/vehiculos/{vid}/historial", headers=SUPER).get_json()["eventos"]
        self.assertEqual([e["accion"] for e in ev], ["habilitado", "inhabilitado", "creado"])
        self.assertEqual(ev[1]["lineas"], ["Motivo: En mantención en el taller"])
        self.assertEqual(ev[0]["lineas"], ["Estaba inhabilitado por: En mantención en el taller"])

    def test_habilitar_avisa_pero_no_traba_con_documentos_vencidos(self):
        _, d = self.crear(soap_vence=VENCIDO, seguro_vence=POR_VENCER)
        vid = d["vehiculo"]["id"]
        self.c.post(f"/flota/api/vehiculos/{vid}/inhabilitar", json={"motivo": "Documentos al día pendientes"}, headers=SUPER)
        r = self.c.post(f"/flota/api/vehiculos/{vid}/habilitar", json={}, headers=SUPER)
        self.assertEqual(r.status_code, 200)                       # alertar, no trabar
        self.assertTrue(r.get_json()["vehiculo"]["habilitado"])
        avisos = " | ".join(r.get_json()["avisos"])
        self.assertIn("SOAP", avisos)
        self.assertIn("Seguro", avisos)

    def test_inexistentes(self):
        for suf in ("inhabilitar", "habilitar"):
            self.assertEqual(self.c.post(f"/flota/api/vehiculos/99/{suf}", json={"motivo": "motivo largo"}, headers=SUPER).status_code, 404)
        self.assertEqual(self.c.get("/flota/api/vehiculos/99/historial", headers=SUPER).status_code, 404)

    def test_no_se_puede_borrar(self):
        _, d = self.crear()
        self.assertEqual(self.c.delete(f"/flota/api/vehiculos/{d['vehiculo']['id']}", headers=SUPER).status_code, 405)


# ═══════════════════════════════════════════════════════════════════════════
#  C. Cableado y candados
# ═══════════════════════════════════════════════════════════════════════════
class TestCableado(unittest.TestCase):
    MODULO = _leer("flota_module.py")
    REGLAS = _leer("flota_reglas.py")

    def test_app_registra_el_modulo_sin_poder_tumbar_el_arranque(self):
        app = _leer("app.py")
        i = app.index("from flota_module import register_flota_routes")
        bloque = app[max(0, i - 80):i + 260]
        self.assertIn("try:", bloque)
        self.assertIn("register_flota_routes(app, globals())", bloque)
        self.assertIn("except Exception as _flota_reg_err", bloque)

    def test_tablas_se_crean_dentro_de_app_context(self):
        # gotcha: un _ensure_* sin app_context falla en silencio
        j = self.MODULO.index("with app.app_context():")
        self.assertIn("_ensure_flota_tables()", self.MODULO[j:j + 200])

    def test_arranque_barato_una_sentencia_por_ddl_y_sin_alter(self):
        self.assertNotRegex(self.MODULO, r"ALTER\s+TABLE")
        self.assertEqual(len(re.findall(r"CREATE TABLE IF NOT EXISTS flota_", self.MODULO)), 6)     # 2 de vehículos + 4 de la fase 1b

    def test_la_bitacora_solo_se_agrega(self):
        self.assertNotRegex(self.MODULO, r"UPDATE\s+flota_vehiculo_log")
        self.assertNotRegex(self.MODULO, r"DELETE\s+FROM\s+flota_vehiculo_log")

    def test_nada_se_borra(self):
        self.assertNotRegex(self.MODULO, r"DELETE\s+FROM\s+flota_vehiculos")
        self.assertNotIn('methods=["DELETE"]', self.MODULO)

    def test_sin_precios_ni_costos(self):
        # Daniel: «no vamos a manejar los precios de los costos»
        for texto in (self.MODULO, self.REGLAS, _leer("templates", "flota", "index.html"), _leer("static", "flota.js")):
            self.assertNotRegex(texto, r"(?i)\b(costo|precio|monto|valor_|factura|\bOC\b|proveedor)\b")

    def test_ningun_sql_con_fstrings_de_usuario(self):
        # REGLA #4: el SQL va con %s; lo único concatenado son nombres de columnas fijos (fr.COLUMNAS)
        for m in re.finditer(r"mysql_(?:execute|fetchone|fetchall|rowcount)\(\s*f[\"']", self.MODULO):
            self.fail("SQL con f-string: " + self.MODULO[m.start():m.start() + 80])

    def test_menu_un_solo_item_solo_superadmin_con_href_fijo(self):
        base = _leer("templates", "base.html")
        self.assertEqual(base.count("<span>Solicitudes de vehículo</span>"), 1)      # UN solo ítem en el menú
        i = base.index('href="/flota"')
        previo = base[max(0, i - 120):i]
        self.assertIn("permissions.superadmin", previo)
        self.assertNotIn("url_for('flota", base)                      # un BuildError tumbaría todas las pantallas
        self.assertEqual(base.count('href="/flota"'), 1)

    def test_la_puerta_de_acceso_es_una_sola(self):
        self.assertEqual(self.MODULO.count("def _flota_puede"), 1)
        self.assertEqual(self.MODULO.count('.get("superadmin")'), 1)


# ═══════════════════════════════════════════════════════════════════════════
#  D. Plantilla renderizada y JS
# ═══════════════════════════════════════════════════════════════════════════
class TestPantalla(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        app = _app_de_prueba(BaseFalsa())
        cls.html = app.test_client().get("/flota", headers=SUPER).get_data(as_text=True)
        cls.js = _leer("static", "flota.js")

    def test_renderiza_con_titulo_menu_y_estilos(self):
        self.assertIn("Solicitudes de vehículo", self.html)
        self.assertIn("flota.css", self.html)
        self.assertIn("flota.js", self.html)
        self.assertIn("Vista previa", self.html)          # se ve que es solo para el superadmin

    def test_todo_id_que_usa_el_js_existe(self):
        usados = set(re.findall(r"\$\('([A-Za-z0-9_]+)'\)", self.js))
        self.assertTrue(len(usados) > 15)
        for i in usados:
            self.assertIn(f'id="{i}"', self.html, f"el JS usa #{i} y la plantilla no lo tiene")

    def test_todo_campo_del_formulario_existe(self):
        for c in ("patente tipo marca modelo anio color vin capacidad_kg capacidad_m3 combustible estanque_l km_actual "
                  "revision_tecnica_vence permiso_circulacion_vence soap_vence seguro_vence notas").split():
            self.assertIn(f'name="{c}"', self.html, c)
            self.assertIn(f'data-campo="{c}"', self.html, c)
        self.assertIn('data-campo="licencia_clases"', self.html)

    def test_el_js_valida_los_mismos_campos_que_el_servidor(self):
        for c in R.COLUMNAS:
            if c in ("licencia_clases", "marca", "modelo", "color", "vin", "combustible", "notas"):
                continue                       # sin regla propia en pantalla: solo largo, que ya limita el campo
            self.assertIn(c, self.js, c)

    def test_pie_de_tabla_como_etiquetas(self):
        # REGLA #4.3
        for t in ("Mostrando", "Por página", "Anterior", "Siguiente", "Página"):
            self.assertIn(t, self.html + self.js)

    def test_sin_dialogos_nativos(self):
        # REGLA #1
        self.assertNotRegex(self.js, r"(?<![\w.])(alert|confirm|prompt)\s*\(")
        self.assertIn("ilusConfirm", self.js)
        self.assertIn("ilusPrompt", self.js)

    def test_el_js_no_usa_request_animation_frame(self):
        self.assertNotRegex(self.js, r"requestAnimationFrame\s*\(")      # el comentario que lo menciona no cuenta

    def test_las_tres_pestanas_estan_abiertas(self):
        # Fase 1b: Solicitudes, Vehículos y Choferes ya se pueden abrir (antes dos decían «Próxima fase»)
        self.assertEqual(len(re.findall(r'class="fl-tab[^"]*"[^>]*disabled', self.html)), 0)
        self.assertNotIn("Próxima fase", self.html)
        for t in ("solicitudes", "vehiculos", "choferes"):
            self.assertIn(f'data-tab="{t}"', self.html)
            self.assertIn(f'data-panel="{t}"', self.html)

    @unittest.skipUnless(HAY_NODE, "node no está instalado")
    def test_js_pasa_node_check(self):
        r = subprocess.run(["node", "--check", os.path.join(RAIZ, "static", "flota.js")], capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, r.stderr)


if __name__ == "__main__":
    unittest.main()

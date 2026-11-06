# -*- coding: utf-8 -*-
"""🚚 FLOTA · «Mis solicitudes»: un filtro para que cada persona vea las solicitudes A SU NOMBRE (Daniel, 06-nov-2026: «hay que dejar un filtro
para que el usuario vea las solicitudes a su nombre; voy a crear un perfil de chofer para que solo vean ese módulo»).

Decisiones que se vigilan:
  1. «A su nombre» = la pidió la persona O va de chofer (el chofer es un usuario de ILUS). `GET /flota/api/solicitudes?mias=1`.
  2. Sin el parámetro el listado es el de siempre (TODAS): «Nueva solicitud» lo usa para saber qué vehículos están ocupados.
  3. Los contadores de arriba siguen al filtro (lo que se cuenta es lo que se ve) y `mias=1` NUNCA devuelve todo si no hay nada propio.
  4. Quien solo pide vehículos arranca en «Mis solicitudes»; quien gestiona (entrega las llaves) arranca viendo las de todos.
  5. El perfil que solo tiene «Solicitudes de vehículo» aterriza en /flota (antes caía en «Mi cuenta» con «tu rol no tiene permisos»).
"""
import datetime as dt
import json
import os
import re
import sqlite3
import subprocess
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from test_flota_solicitudes import SUPER, Base, _leer  # noqa: E402

JS = _leer("static", "flota_uso.js")
HTML = _leer("templates", "flota", "index.html")
MODULO = _leer("flota_module.py")
APP = _leer("app.py")


class TestListado(Base):
    """El listado con `mias=1`, sobre la base de mentira (el usuario de las pruebas es el 1: Ana)."""

    def setUp(self):
        super().setUp()
        # S1: la pidió Ana (usuario 1) y va Beto de chofer           → es suya porque la pidió
        # S2: la pidió el usuario 2 (Beto) y va Ana de chofer         → es suya porque va de chofer
        # S3: la pidió el usuario 2 (Beto) y va Beto de chofer        → no es suya
        r1, d1 = self.pedir(vehiculo_id=self.furgon, chofer_id=self.beto, hora_desde="08:00", hora_hasta="10:00")
        r2, d2 = self.pedir(vehiculo_id=self.camion, chofer_id=self.ana, hora_desde="08:00", hora_hasta="10:00")
        r3, d3 = self.pedir(vehiculo_id=self.furgon, chofer_id=self.beto, hora_desde="11:00", hora_hasta="13:00")
        for r in (r1, r2, r3):
            self.assertEqual(r.status_code, 200, r.get_data(as_text=True))
        self.s1, self.s2, self.s3 = d1["solicitud"]["id"], d2["solicitud"]["id"], d3["solicitud"]["id"]
        self.base.sol[self.s2]["solicitante_id"] = 2
        self.base.sol[self.s3]["solicitante_id"] = 2

    def lista(self, query=""):
        r = self.c.get("/flota/api/solicitudes" + query, headers=SUPER)
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True))
        return r.get_json()

    def ids(self, d):
        return sorted(s["id"] for s in d["solicitudes"])

    def test_con_mias_salen_las_que_pidio_y_en_las_que_va_de_chofer(self):
        d = self.lista("?mias=1")
        self.assertEqual(self.ids(d), sorted([self.s1, self.s2]))
        self.assertTrue(d["mias"])

    def test_sin_el_parametro_salen_todas_como_siempre(self):
        for q in ("", "?mias=0", "?mias=", "?mias=si", "?mias=true"):
            d = self.lista(q)
            self.assertEqual(self.ids(d), sorted([self.s1, self.s2, self.s3]), q)
            self.assertFalse(d["mias"], q)

    def test_cada_solicitud_dice_si_va_a_nombre_de_quien_mira(self):
        d = self.lista()
        por_id = {s["id"]: s["a_mi_nombre"] for s in d["solicitudes"]}
        self.assertEqual(por_id, {self.s1: True, self.s2: True, self.s3: False})

    def test_los_contadores_siguen_al_filtro(self):
        todas = self.lista()["resumen"]
        mias = self.lista("?mias=1")["resumen"]
        self.assertEqual(todas["por_confirmar"], 3)
        self.assertEqual(mias["por_confirmar"], 2)
        self.base.sol[self.s1]["estado"] = "confirmada"
        mias = self.lista("?mias=1")["resumen"]
        self.assertEqual((mias["por_confirmar"], mias["confirmadas"]), (1, 1))
        self.assertEqual(self.lista()["resumen"]["confirmadas"], 1)

    def test_las_reservas_de_hoy_siguen_al_filtro(self):
        hoy = dt.datetime.combine(dt.date.fromisoformat(self.lista()["hoy"]), dt.time(8, 0))       # «hoy» según el servidor, no según esta máquina
        for sid in (self.s1, self.s3):
            self.base.sol[sid]["inicio"], self.base.sol[sid]["fin"] = hoy, hoy + dt.timedelta(hours=2)
        self.assertEqual(self.lista()["resumen"]["hoy"], 2)
        self.assertEqual(self.lista("?mias=1")["resumen"]["hoy"], 1)

    def test_las_devueltas_por_cerrar_cuentan_solo_las_propias(self):
        for sid in (self.s1, self.s3):
            self.base.sol[sid].update(estado="devuelta", dev_confirmada_at=None)
        self.assertEqual(self.lista()["resumen"]["por_cerrar"], 2)
        self.assertEqual(self.lista("?mias=1")["resumen"]["por_cerrar"], 1)
        # y la lista de próximas trae la devuelta propia aunque su horario ya pasó
        self.base.sol[self.s1]["fin"] = dt.datetime.now() - dt.timedelta(days=2)
        self.base.sol[self.s1]["inicio"] = dt.datetime.now() - dt.timedelta(days=2, hours=3)
        self.assertIn(self.s1, self.ids(self.lista("?mias=1")))

    def test_las_pasadas_tambien_se_filtran(self):
        for sid in (self.s1, self.s3):
            self.base.sol[sid]["fin"] = dt.datetime.now() - dt.timedelta(days=3)
            self.base.sol[sid]["inicio"] = dt.datetime.now() - dt.timedelta(days=3, hours=2)
        self.assertEqual(self.ids(self.lista("?vista=pasadas")), sorted([self.s1, self.s3]))
        self.assertEqual(self.ids(self.lista("?vista=pasadas&mias=1")), [self.s1])

    def test_sin_nada_propio_no_sale_todo(self):
        for sid in (self.s1, self.s2, self.s3):
            self.base.sol[sid]["solicitante_id"] = 2
        self.base.sol[self.s2]["chofer_id"] = self.beto
        d = self.lista("?mias=1")
        self.assertEqual(d["solicitudes"], [])
        self.assertEqual(d["resumen"]["por_confirmar"], 0)
        self.assertEqual(self.lista()["resumen"]["por_confirmar"], 3)       # sin el filtro siguen estando

    def test_el_filtro_no_cambia_lo_que_se_puede_hacer_con_cada_solicitud(self):
        # Filtrar solo MIRA distinto: los permisos de cada fila son los mismos con o sin el filtro.
        todas = {s["id"]: s["puede"] for s in self.lista()["solicitudes"]}
        mias = {s["id"]: s["puede"] for s in self.lista("?mias=1")["solicitudes"]}
        for sid, puede in mias.items():
            self.assertEqual(puede, todas[sid])


class TestSqlRealmenteFiltra(unittest.TestCase):
    """La condición SQL de verdad (la del código, no una copia) sobre una base de mentira: la misma respuesta con alias y sin alias."""

    @classmethod
    def setUpClass(cls):
        cls.s_alias = re.search(r'_SQL_MIAS_S = "([^"]+)"', MODULO).group(1)
        cls.s_plano = re.search(r'_SQL_MIAS_T = "([^"]+)"', MODULO).group(1)

    def base(self):
        db = sqlite3.connect(":memory:")
        db.executescript("""
            CREATE TABLE flota_choferes (id INTEGER PRIMARY KEY, usuario_id INTEGER);
            CREATE TABLE flota_solicitudes (id INTEGER PRIMARY KEY, solicitante_id INTEGER, chofer_id INTEGER);
            INSERT INTO flota_choferes VALUES (1, 10), (2, 20), (3, NULL);              -- el 3 es un chofer externo (sin usuario)
            INSERT INTO flota_solicitudes VALUES
              (1, 10, 2),      -- la pidió el 10, va el 20
              (2, 20, 1),      -- la pidió el 20, va el 10
              (3, 20, 2),      -- no tiene que ver con el 10
              (4, NULL, 3),    -- sin solicitante y chofer externo
              (5, 10, NULL);   -- sin chofer
        """)
        return db

    def correr(self, uid):
        db = self.base()
        q1 = ("SELECT s.id FROM flota_solicitudes s LEFT JOIN flota_choferes c ON c.id = s.chofer_id WHERE " + self.s_alias).replace("%s", "?")
        q2 = ("SELECT id FROM flota_solicitudes WHERE " + self.s_plano).replace("%s", "?")
        a = sorted(r[0] for r in db.execute(q1, (uid, uid)))
        b = sorted(r[0] for r in db.execute(q2, (uid, uid)))
        return a, b

    def test_usuario_10_ve_la_que_pidio_y_en_la_que_va(self):
        self.assertEqual(self.correr(10), ([1, 2, 5], [1, 2, 5]))

    def test_usuario_20_ve_las_suyas(self):
        self.assertEqual(self.correr(20), ([1, 2, 3], [1, 2, 3]))

    def test_un_usuario_sin_nada_no_ve_nada_ni_los_choferes_externos(self):
        self.assertEqual(self.correr(99), ([], []))
        self.assertEqual(self.correr(0), ([], []))                  # id 0: el valor que usa el código si no hay usuario


def _expresion_por_defecto():
    m = re.search(r"solMias: ([^,\n]+),", JS)
    assert m, "no encontré el valor inicial de solMias"
    return m.group(1).strip()


class TestPantalla(unittest.TestCase):
    def node(self, codigo):
        r = subprocess.run(["node", "-e", codigo], capture_output=True, text=True, timeout=60)
        self.assertEqual(r.returncode, 0, r.stderr)
        return json.loads(r.stdout.strip())

    def test_quien_solo_pide_arranca_en_mis_solicitudes_y_quien_gestiona_en_todas(self):
        expr = _expresion_por_defecto()
        casos = [({"solicitar": True}, True), ({"solicitar": True, "gestionar": True}, False), ({"solicitar": True, "admin": True}, False),
                 ({"gestionar": True}, False), ({}, True)]
        for perm, esperado in casos:
            out = self.node("var PERM = " + json.dumps(perm) + "; console.log(JSON.stringify(" + expr + "));")
            self.assertEqual(out, esperado, perm)

    def test_los_botones_marcan_la_opcion_activa(self):
        i = JS.index("function pintarQuien()")
        fn = JS[i:JS.index("function pintarSolVacio(", i)]
        codigo = """
        function mk(v) { var cls = {}, attrs = { 'data-smias': v };
          return { getAttribute: function (k) { return attrs[k]; }, setAttribute: function (k, x) { attrs[k] = x; },
                   classList: { toggle: function (c, on) { cls[c] = !!on; } }, cls: cls, attrs: attrs }; }
        var mias = mk('1'), todos = mk('0');
        var document = { querySelectorAll: function () { return [mias, todos]; } };
        var S = { solMias: true };
        """ + fn + """
        pintarQuien();
        var a = [mias.cls.on, mias.attrs['aria-pressed'], todos.cls.on, todos.attrs['aria-pressed']];
        S.solMias = false; pintarQuien();
        console.log(JSON.stringify([a, [mias.cls.on, mias.attrs['aria-pressed'], todos.cls.on, todos.attrs['aria-pressed']]]));
        """
        self.assertEqual(self.node(codigo), [[True, "true", False, "false"], [False, "false", True, "true"]])

    def test_la_carga_de_la_lista_manda_el_filtro_y_la_de_ocupacion_no(self):
        i = JS.index("function cargarSol(silencio)")
        self.assertIn("'/solicitudes?vista=' + S.solVista + '&mias=' + (S.solMias ? '1' : '0')", JS[i:i + 500])
        # «Nueva solicitud» necesita TODAS para saber qué vehículos están ocupados: esa carga no lleva el filtro
        self.assertIn("api(API + '/solicitudes?vista=proximas')", JS)

    def test_tocar_una_opcion_recarga_la_lista_desde_la_primera_pagina(self):
        self.assertIn("chips('.fl-fchip[data-smias]', 'data-smias', function (v) { S.solMias = v === '1'; S.solPag = 1; pintarQuien(); cargarSol(); });", JS)
        self.assertIn("    pintarQuien();", JS)                       # y al abrir la pantalla ya queda marcada la opción de arranque

    def test_el_botones_estan_en_los_filtros_de_la_lista_y_en_orden(self):
        i = HTML.index('id="flSolFiltros"')
        t = HTML[i:i + 1400]
        self.assertIn('data-smias="1"', t)
        self.assertIn('data-smias="0"', t)
        self.assertLess(t.index('data-smias="1"'), t.index('data-smias="0"'))
        self.assertIn("Mis solicitudes", t)
        self.assertIn("De todos", t)
        self.assertLess(t.index("De todos"), t.index('data-sest="todas"'))     # antes de los filtros por estado

    def test_la_lista_vacia_a_mi_nombre_explica_como_ver_las_demas(self):
        i = JS.index("function pintarSolLista()")
        t = JS[i:i + 1800]
        self.assertIn("if (S.solMias)", t)
        self.assertIn("No tienes solicitudes por delante a tu nombre", t)
        self.assertIn("No tienes solicitudes pasadas a tu nombre", t)
        self.assertIn("toca «De todos»", t)

    def test_viendo_las_de_todos_se_marcan_las_que_van_a_mi_nombre(self):
        self.assertIn("s.a_mi_nombre && !S.solMias", JS)
        self.assertIn(".fl-mia{", _leer("static", "flota.css"))

    def test_las_acciones_de_la_fila_no_dependen_del_filtro(self):
        i = JS.index("function botonesFila(s)")
        self.assertNotIn("solMias", JS[i:i + 1500])


def _funcion(src, nombre):
    i = src.index("def " + nombre + "(")
    j = src.index("\n\n\n", i)
    return src[i:j]


class TestAterrizaje(unittest.TestCase):
    """El perfil que SOLO tiene «Solicitudes de vehículo» entra directo a /flota."""

    def setUp(self):
        self.avisos = []
        ns = {"redirect": lambda u: ("redirige", u), "url_for": lambda ep, **k: "/" + ep,
              "flash": lambda *a, **k: self.avisos.append(a)}
        exec(_funcion(APP, "_redirect_to_first_accessible"), ns)
        self.ir = ns["_redirect_to_first_accessible"]

    def test_quien_solo_tiene_flota_aterriza_en_flota_sin_aviso_de_falta_de_permisos(self):
        for llave in ("flota_solicitar", "flota_gestionar", "flota_administrar"):
            self.assertEqual(self.ir({llave: True}), ("redirige", "/flota"), llave)
        self.assertEqual(self.avisos, [])

    def test_a_quien_ya_tiene_otro_modulo_no_le_cambia_el_aterrizaje(self):
        self.assertEqual(self.ir({"mantenciones": True, "flota_solicitar": True}), ("redirige", "/mant_index"))
        self.assertEqual(self.ir({"retiros": True, "flota_solicitar": True}), ("redirige", "/pickup_dashboard"))
        self.assertEqual(self.ir({"transporte": True, "flota_gestionar": True}), ("redirige", "/transporte_index"))
        self.assertEqual(self.ir({"cubicador": True, "flota_solicitar": True}), ("redirige", "/cubicador"))
        self.assertEqual(self.ir({"comunicaciones": True, "flota_solicitar": True}), ("redirige", "/comm_index"))

    def test_sin_ningun_modulo_sigue_yendo_a_mi_cuenta_con_el_aviso(self):
        self.assertEqual(self.ir({}), ("redirige", "/mi_cuenta"))
        self.assertEqual(self.ir({"flota_solicitar": False}), ("redirige", "/mi_cuenta"))
        self.assertEqual(len(self.avisos), 2)


if __name__ == "__main__":
    unittest.main()

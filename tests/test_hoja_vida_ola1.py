"""Ola 1 · Hoja de vida confiable de cada máquina (2026-10-05, Daniel: "dale,
parte por la Ola 1").

Prueba, extrayendo las funciones de app.py con ast (importar app.py levanta
Flask y la base), con dobles de BD:
  - la parada se abre cuando el equipo queda fuera de servicio y se cierra
    cuando vuelve a operar (y no hace nada si ya está como corresponde);
  - la garantía usa la regla más específica y NUNCA pisa una fecha puesta a mano;
  - sin reglas no se calcula ninguna garantía (es plata: lo decide Daniel);
  - los indicadores de Analytics dicen lo que miden.

Correr con:  py -m unittest tests.test_hoja_vida_ola1
"""
import ast
import json
import os
import unittest
from datetime import date, datetime, timedelta

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
APP_PY = os.path.join(RAIZ, "app.py")
_FUN = ("_hv_asegurar", "_hv_user", "_hv_fmt_dur", "_hv_evento", "_maquina_parada_sync", "_hv_sumar_meses",
        "_garantia_reglas", "_garantia_regla_para", "_garantia_aplicar")
_CONST = ("_MAQ_ESTADOS_PARADA", "_HV_TABLAS", "_HV_FAMILIAS")


_CODIGO = {}


def _codigo():
    # app.py se lee y compila UNA vez (son ~150 mil líneas).
    if "c" not in _CODIGO:
        with open(APP_PY, encoding="utf-8") as fh:
            arbol = ast.parse(fh.read())
        nodos = [n for n in arbol.body
                 if (isinstance(n, ast.FunctionDef) and n.name in _FUN)
                 or (isinstance(n, ast.Assign) and any(getattr(t, "id", "") in _CONST for t in n.targets))]
        _CODIGO["c"] = compile(ast.Module(body=nodos, type_ignores=[]), "<app>", "exec")
    return _CODIGO["c"]


def _cargar(bd):
    amb = {"datetime": datetime, "timedelta": timedelta, "json": json,
           "current_username": lambda: "tester",
           "mysql_fetchone": bd.fetchone, "mysql_fetchall": bd.fetchall,
           "mysql_execute": bd.execute, "mysql_execute_returning_rowcount": bd.rowcount}
    exec(_codigo(), amb)
    amb["_HV_TABLAS"]["ok"] = True   # sin DDL en las pruebas
    return amb


class BD:
    def __init__(self, maquina=None, abierta=None, reglas=None, maquinas=None):
        self.maquina, self.abierta = maquina, abierta
        self.reglas, self.maquinas = reglas or [], maquinas or []
        self.sql = []

    def fetchone(self, sql, params=()):
        if "FROM mant_maquinas WHERE id=%s" in sql:
            return self.maquina
        if "FROM mant_maquina_paradas WHERE maquina_id=%s AND fin_at IS NULL" in sql:
            return self.abierta
        return None

    def fetchall(self, sql, params=()):
        if "FROM mant_garantia_reglas" in sql:
            return self.reglas
        if "FROM mant_maquinas WHERE" in sql:
            return self.maquinas
        return []

    def execute(self, sql, params=()):
        self.sql.append((sql, params))

    def rowcount(self, sql, params=()):
        self.sql.append((sql, params))
        return 1


class Paradas(unittest.TestCase):
    def test_fuera_de_servicio_abre_la_parada_y_anota_el_evento(self):
        bd = BD(maquina={"id": 5, "cliente_id": 9, "estado_capturado": "fuera_servicio"})
        out = _cargar(bd)["_maquina_parada_sync"](5, "ot", "mant_visitas", 77, "correa cortada")
        self.assertEqual(out, "abierta")
        self.assertTrue(any("INSERT INTO mant_maquina_paradas" in s for s, _ in bd.sql))
        self.assertTrue(any("INSERT INTO mant_maquina_eventos" in s and "Quedó fuera de servicio" in str(p) for s, p in bd.sql))

    def test_vuelve_a_operar_cierra_la_parada_con_su_duracion(self):
        bd = BD(maquina={"id": 5, "cliente_id": 9, "estado_capturado": "operativo"},
                abierta={"id": 3, "inicio_at": datetime.utcnow() - timedelta(hours=46), "inicio_aprox": 0})
        out = _cargar(bd)["_maquina_parada_sync"](5, "repuesto_instalado")
        self.assertEqual(out, "cerrada")
        self.assertTrue(any("UPDATE mant_maquina_paradas SET fin_at=NOW()" in s for s, _ in bd.sql))
        self.assertTrue(any("Volvió a operar tras 46 h" in str(p) for _, p in bd.sql))

    def test_sin_cambios_no_escribe(self):
        for est, abierta in (("operativo", None), ("fuera_servicio", {"id": 1, "inicio_at": datetime.utcnow(), "inicio_aprox": 0})):
            bd = BD(maquina={"id": 5, "cliente_id": 9, "estado_capturado": est}, abierta=abierta)
            self.assertIsNone(_cargar(bd)["_maquina_parada_sync"](5))
            self.assertEqual(bd.sql, [])

    def test_la_baja_cierra_la_parada_como_baja(self):
        bd = BD(maquina={"id": 5, "cliente_id": 9, "estado_capturado": "dado_baja"},
                abierta={"id": 3, "inicio_at": datetime.utcnow() - timedelta(days=3), "inicio_aprox": 1})
        self.assertEqual(_cargar(bd)["_maquina_parada_sync"](5, "baja"), "cerrada")
        self.assertTrue(any("'dado_baja'" in str(p) or "Dado de baja" in str(p) for _, p in bd.sql))


class Garantia(unittest.TestCase):
    def test_sumar_meses_respeta_fin_de_mes(self):
        f = _cargar(BD())["_hv_sumar_meses"]
        self.assertEqual(f(date(2025, 1, 31), 1), date(2025, 2, 28))
        self.assertEqual(f(date(2025, 3, 14), 24), date(2027, 3, 14))

    def test_gana_la_regla_mas_especifica(self):
        f = _cargar(BD())["_garantia_regla_para"]
        reglas = [{"id": 1, "marca": None, "familia": None, "meses": 12},
                  {"id": 2, "marca": "Drax", "familia": None, "meses": 18},
                  {"id": 3, "marca": "drax", "familia": "trotadoras", "meses": 24},
                  {"id": 4, "marca": None, "familia": "trotadoras", "meses": 6}]
        self.assertEqual(f({"marca": "DRAX ", "familia_equipo": "trotadoras"}, reglas)["id"], 3)
        self.assertEqual(f({"marca": "Drax", "familia_equipo": "bancos"}, reglas)["id"], 2)
        self.assertEqual(f({"marca": "Keiser", "familia_equipo": "trotadoras"}, reglas)["id"], 4)
        self.assertEqual(f({"marca": "Keiser", "familia_equipo": "bancos"}, reglas)["id"], 1)

    def test_calcula_y_nunca_pisa_la_manual(self):
        bd = BD(reglas=[{"id": 1, "marca": None, "familia": None, "meses": 12}], maquinas=[
            {"id": 1, "marca": "Drax", "familia_equipo": "cardio", "fecha_instalacion": date(2025, 3, 14), "doc_fecha": None,
             "fecha_fin_garantia": None, "garantia_origen": None}])
        n = _cargar(bd)["_garantia_aplicar"]()
        self.assertEqual(n, 1)
        upd = [p for s, p in bd.sql if "UPDATE mant_maquinas SET fecha_fin_garantia" in s][0]
        self.assertEqual(upd[0], date(2026, 3, 14))
        self.assertEqual(upd[1], "calculada")
        # El UPDATE mismo protege la fecha manual aunque la fila se colara
        self.assertTrue(any("garantia_origen='calculada'" in s for s, _ in bd.sql))

    def test_sin_reglas_no_calcula_nada(self):
        bd = BD(reglas=[], maquinas=[{"id": 1, "marca": "Drax", "familia_equipo": "cardio", "fecha_instalacion": date(2025, 3, 14),
                                      "doc_fecha": None, "fecha_fin_garantia": None, "garantia_origen": None}])
        self.assertEqual(_cargar(bd)["_garantia_aplicar"](solo_nuevas=True), 0)
        self.assertEqual(_cargar(bd)["_garantia_aplicar"](), 0)
        self.assertFalse(any("UPDATE mant_maquinas" in s for s, _ in bd.sql))


class IndicadoresHonestos(unittest.TestCase):
    def test_analytics_ya_no_rotula_mal(self):
        with open(os.path.join(RAIZ, "templates", "mantenciones", "analytics.html"), encoding="utf-8") as fh:
            html = fh.read()
        self.assertNotIn("TMR — Tiempo Medio Reparación", html)
        self.assertNotIn(">SLA cumplido<", html)
        self.assertIn("Tiempo medio de reparación", html)
        with open(APP_PY, encoding="utf-8") as fh:
            src = fh.read()
        self.assertNotIn("COUNT(t.id) AS n_tareas", src)   # "más fallas" ya no cuenta tareas


if __name__ == "__main__":
    unittest.main()

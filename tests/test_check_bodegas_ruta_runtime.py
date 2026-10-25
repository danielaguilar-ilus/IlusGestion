# -*- coding: utf-8 -*-
"""2026-10-25 · Revisión independiente del buscador con bodegas de Check: la RUTA y la CACHÉ se ejecutan de verdad (funciones de app.py extraídas con
`ast` y corridas con dobles, sin Flask ni base, igual que tests/test_cron_token_query_string.py): ningún texto buscado en el código.

Lo que se comprueba:
  · /api/erp/check-bodegas niega a TODA la familia técnico (REGLA #19: las bodegas son de un operador externo).
  · Caché fría (instancia recién levantada) o más vieja de lo que se sirve: no deja hilos esperando los 4-45 s de la lectura; la pide en segundo plano,
    espera SOLO unos segundos (con la petición viva Cloud Run le da CPU a esa lectura) y, si no alcanzó, responde 503 «calentando».
  · Un reporte de stock VACÍO no se guarda ni pisa lo que ya había, y se responde como «Check no respondió» (no como «sin stock»).
  · Check sigue SOLO LECTURA (REGLA #4.4): la ruta nunca llama a Check por su cuenta (solo pide la lectura al mecanismo ya existente).
"""
import ast
import os
import sys
import threading
import time
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, RAIZ)

import retiros_check  # noqa: E402

with open(os.path.join(RAIZ, "app.py"), encoding="utf-8") as _fh:
    _ARBOL = ast.parse(_fh.read())


def _funcion(nombre):
    """Nodo de la función `nombre` de app.py SIN sus decoradores (@app.route, @login_required…)."""
    for nodo in _ARBOL.body:
        if isinstance(nodo, ast.FunctionDef) and nodo.name == nombre:
            nodo.decorator_list = []
            return nodo
    raise AssertionError("no se encontró %s en app.py" % nombre)


_RUTA = _funcion("api_check_bodegas")
_CACHE = _funcion("_checkwms_stock_rows")
_FRIO = _funcion("_checkwms_stock_frio")


class _Args:
    def __init__(self, skus):
        self._d = {"skus": skus}

    def get(self, k, default=None):
        return self._d.get(k, default)


class _Req:
    def __init__(self, skus):
        self.args = _Args(skus)


FILAS = [
    {"codigo": "A", "bodega": "BODEGA 02", "ubicacion": "PISOD-12", "ua": "UA1", "stFisico": 3, "disponible": 3},
    {"codigo": "B", "bodega": "BODEGA EXTERNA", "ubicacion": "RACK-3", "ua": "UA2", "stFisico": 2, "disponible": 2},
]


def _ambito_ruta(skus="A,B", tecnico=False, filas=None, ts=None, configurado=True, swr="nada", espera=0.4):
    """Dobles para correr `api_check_bodegas` y `_checkwms_stock_frio` tal cual están en app.py.

    `swr` = qué hace la lectura en segundo plano: 'nada' (se queda «en curso» y no termina), 'termina' (carga las filas a los 0,1 s) o
    'muerta' (no arranca / ya murió: la bandera queda en falso)."""
    llamadas = {"swr": [], "stock_rows": 0, "get": 0}
    stock = {"ts": (time.time() - 120) if ts is None else ts, "rows": filas, "lock": threading.Lock()}
    flag = {"stock": False, "traza": False}

    def _swr(clave, fn):
        llamadas["swr"].append((clave, fn))
        if swr == "muerta":
            return
        flag[clave] = True
        if swr == "termina":
            def _fin():
                stock.update({"ts": time.time(), "rows": FILAS})
                flag[clave] = False
            threading.Timer(0.1, _fin).start()

    def _rows(forzar=False):
        llamadas["stock_rows"] += 1
        return stock["rows"]

    def _get(*a, **k):
        llamadas["get"] += 1
        raise AssertionError("la ruta no debe hablarle a Check por su cuenta (REGLA #4.4)")

    amb = {
        "jsonify": lambda x: x, "request": _Req(skus), "time": time, "print": lambda *a, **k: None,
        "_oculta_proveedores": lambda: tecnico,
        "_CHECKWMS_STOCK": stock, "_CHECKWMS_STALE_MAX": 6 * 3600, "_CHECKWMS_ESPERA_FRIA_S": espera, "_CHECKWMS_SWR": flag,
        "_checkwms_swr": _swr, "_checkwms_stock_rows": _rows, "_checkwms_configurado": lambda: configurado, "_checkwms_get": _get,
    }
    exec(compile(ast.Module(body=[_FRIO, _RUTA], type_ignores=[]), "<app.py-extraido>", "exec"), amb)
    return amb, llamadas


class TestRutaGeneral(unittest.TestCase):
    def test_ningun_tecnico_ve_las_bodegas(self):
        for filas in (FILAS, None):
            amb, ll = _ambito_ruta(tecnico=True, filas=filas)
            payload, st = amb["api_check_bodegas"]()
            self.assertEqual(st, 403)
            self.assertFalse(payload["ok"])
            self.assertEqual(ll["stock_rows"], 0)                         # ni siquiera lee el reporte
            self.assertEqual(ll["swr"], [])

    def test_sin_skus_no_espera_a_check(self):
        amb, ll = _ambito_ruta(skus="  , ", filas=None)
        r = amb["api_check_bodegas"]()
        self.assertEqual(r, {"ok": True, "bodegas": {}, "edad_min": None})
        self.assertEqual((ll["stock_rows"], ll["swr"]), (0, []))

    def test_cache_fria_espera_un_rato_y_responde_calentando_sin_colgar_el_hilo(self):
        amb, ll = _ambito_ruta(filas=None, swr="nada", espera=0.4)
        t0 = time.time()
        payload, st = amb["api_check_bodegas"]()
        dt = time.time() - t0
        self.assertEqual(st, 503)
        self.assertTrue(payload["calentando"])
        self.assertFalse(payload["ok"])
        self.assertGreaterEqual(dt, 0.35)                                 # esperó el tope (con la petición viva, la lectura tiene CPU)…
        self.assertLess(dt, 3)                                            # …y NO los 4-45 s de la lectura completa
        self.assertEqual(ll["stock_rows"], 0)                             # la ruta nunca lee ella misma el reporte frío
        self.assertEqual([c for c, _ in ll["swr"]], ["stock"])             # pidió UNA renovación en segundo plano
        self.assertIs(ll["swr"][0][1], amb["_checkwms_stock_rows"])

    def test_si_la_lectura_termina_mientras_espera_responde_con_los_datos(self):
        amb, ll = _ambito_ruta(skus="a,b", filas=None, swr="termina", espera=3)
        t0 = time.time()
        payload, st = amb["api_check_bodegas"]()
        self.assertEqual(st, 200)
        self.assertLess(time.time() - t0, 2)                              # no esperó el tope entero
        self.assertEqual(payload["bodegas"]["A"][0]["bodega"], "BODEGA 02")
        self.assertEqual(payload["bodegas"]["B"][0]["bodega"], "BODEGA EXTERNA")
        self.assertEqual(ll["stock_rows"], 1)                             # recién ahí lee (ya tibia)

    def test_si_la_lectura_no_arranco_o_murio_no_se_espera_el_tope(self):
        amb, ll = _ambito_ruta(filas=None, swr="muerta", espera=5)
        t0 = time.time()
        payload, st = amb["api_check_bodegas"]()
        self.assertEqual(st, 503)
        self.assertTrue(payload["calentando"])                            # el modal reintenta y se pide otra lectura
        self.assertLess(time.time() - t0, 1)

    def test_un_reporte_mas_viejo_de_lo_que_se_sirve_tambien_es_frio(self):
        # Antes solo se miraba `rows is None`: con 6 h o más la caché caía al candado y colgaba el hilo hasta 45 s.
        amb, ll = _ambito_ruta(filas=FILAS, ts=time.time() - 7 * 3600, swr="nada", espera=0.2)
        payload, st = amb["api_check_bodegas"]()
        self.assertEqual(st, 503)
        self.assertTrue(payload["calentando"])
        self.assertEqual(ll["stock_rows"], 0)
        self.assertEqual([c for c, _ in ll["swr"]], ["stock"])

    def test_vencida_pero_servible_responde_al_momento(self):
        # Entre 1 h y 6 h: se sirve lo que hay (la renovación la hace _checkwms_stock_rows por su cuenta).
        amb, ll = _ambito_ruta(filas=FILAS, ts=time.time() - 3 * 3600)
        payload, st = amb["api_check_bodegas"]()
        self.assertEqual(st, 200)
        self.assertEqual(ll["swr"], [])
        self.assertEqual(payload["edad_min"], 180)

    def test_sin_credenciales_no_se_queda_calentando_para_siempre(self):
        amb, ll = _ambito_ruta(filas=None, configurado=False)
        payload, st = amb["api_check_bodegas"]()
        self.assertEqual(st, 503)
        self.assertNotIn("calentando", payload)
        self.assertIn("no está configurado", payload["error"])
        self.assertEqual(ll["swr"], [])

    def test_cache_tibia_responde_al_momento_con_las_bodegas(self):
        amb, ll = _ambito_ruta(skus="a, b ,zzz", filas=FILAS)
        payload, st = amb["api_check_bodegas"]()
        self.assertEqual(st, 200)
        self.assertEqual([b["bodega"] for b in payload["bodegas"]["A"]], ["BODEGA 02"])
        self.assertEqual(payload["bodegas"]["B"][0]["bodega"], "BODEGA EXTERNA")
        self.assertEqual(payload["bodegas"]["ZZZ"], [])
        self.assertEqual(payload["edad_min"], 2)
        self.assertEqual(ll["swr"], [])                                    # con datos no se pide nada atrás
        self.assertEqual(ll["get"], 0)

    def test_un_reporte_vacio_no_es_sin_stock_es_check_no_respondio(self):
        amb, ll = _ambito_ruta(filas=[])
        payload, st = amb["api_check_bodegas"]()
        self.assertEqual(st, 503)
        self.assertIn("Check no respondió", payload["error"])
        self.assertNotIn("bodegas", payload)


def _ambito_cache(respuesta, previo_rows=None, previo_ts=0):
    """Dobles para correr `_checkwms_stock_rows` tal cual está en app.py. `respuesta`: lo que devuelve _checkwms_get."""
    llamadas = {"get": []}

    def _get(path, params, timeout=60):
        llamadas["get"].append(path)
        return respuesta

    amb = {
        "time": time, "threading": threading, "print": lambda *a, **k: None,
        "_CHECKWMS_STOCK": {"ts": previo_ts, "rows": previo_rows, "lock": threading.Lock()},
        "_CHECKWMS_STOCK_TTL": 3600, "_CHECKWMS_STALE_MAX": 6 * 3600,
        "_checkwms_get": _get, "_checkwms_swr": lambda *a, **k: None,
    }
    exec(compile(ast.Module(body=[_CACHE], type_ignores=[]), "<app.py-extraido>", "exec"), amb)
    return amb, llamadas


class TestCacheDelReporteDeStock(unittest.TestCase):
    def test_lectura_buena_se_guarda(self):
        amb, ll = _ambito_cache({"body": {"response": FILAS}})
        self.assertEqual(amb["_checkwms_stock_rows"](), FILAS)
        self.assertEqual(amb["_CHECKWMS_STOCK"]["rows"], FILAS)
        self.assertGreater(amb["_CHECKWMS_STOCK"]["ts"], time.time() - 5)
        self.assertEqual(ll["get"], ["/api/ext/GetReporteStock"])        # el único reporte que lee, de la lista blanca

    def test_un_reporte_vacio_no_se_guarda_y_no_pisa_lo_que_habia(self):
        # Sin datos previos: sigue sin haber datos (la ruta responde «Check no respondió», no «sin stock»).
        amb, _ = _ambito_cache({"body": {"response": []}})
        self.assertIsNone(amb["_checkwms_stock_rows"]())
        self.assertIsNone(amb["_CHECKWMS_STOCK"]["rows"])
        self.assertEqual(amb["_CHECKWMS_STOCK"]["ts"], 0)
        # Con datos viejos: se conservan intactos.
        viejo_ts = time.time() - 7200
        amb, _ = _ambito_cache({"body": {"response": []}}, previo_rows=FILAS, previo_ts=viejo_ts)
        self.assertEqual(amb["_checkwms_stock_rows"](forzar=True), FILAS)
        self.assertEqual(amb["_CHECKWMS_STOCK"]["rows"], FILAS)
        self.assertEqual(amb["_CHECKWMS_STOCK"]["ts"], viejo_ts)

    def test_check_sin_respuesta_sirve_lo_viejo(self):
        amb, _ = _ambito_cache(None, previo_rows=FILAS, previo_ts=time.time() - 7200)
        self.assertEqual(amb["_checkwms_stock_rows"](forzar=True), FILAS)
        amb, _ = _ambito_cache(None)
        self.assertIsNone(amb["_checkwms_stock_rows"]())

    def test_con_la_cache_fresca_no_vuelve_a_preguntar(self):
        amb, ll = _ambito_cache({"body": {"response": [{"codigo": "X"}]}}, previo_rows=FILAS, previo_ts=time.time() - 60)
        self.assertEqual(amb["_checkwms_stock_rows"](), FILAS)
        self.assertEqual(ll["get"], [])


class TestRespuestaPuraYRetiros(unittest.TestCase):
    def test_retiros_dice_lo_mismo_con_un_reporte_vacio(self):
        # La ruta de Retiros usa la MISMA función de respuesta: `[]` es «no respondió» también ahí.
        self.assertEqual(retiros_check.respuesta_bodegas("A", [])[1], 503)
        self.assertEqual(retiros_check.respuesta_bodegas("A", FILAS)[1], 200)


if __name__ == "__main__":
    unittest.main()

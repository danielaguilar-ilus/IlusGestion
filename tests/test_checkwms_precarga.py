# -*- coding: utf-8 -*-
"""2026-10-23 · Daniel: «sí, precarga lo de Check». Los reportes de Check tardan 4-35 s y se cacheaban 1 h: la primera persona tras el vencimiento
esperaba la descarga completa (8-25 s en Retiros). Ahora: vencida la caché se sirve lo que hay AL INSTANTE y se renueva atrás (hasta 6 h de
antigüedad), y el trabajo programado de cada 10 min renueva el stock antes de que venza. Check sigue SOLO LECTURA (REGLA #4.4).
"""
import os
import threading
import time
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
with open(os.path.join(RAIZ, "app.py"), encoding="utf-8") as _fh:
    APP = _fh.read()


def _trozo(inicio, fin):
    i = APP.index(inicio)
    return APP[i:APP.index(fin, i)]


class TestRenovacionEnSegundoPlano(unittest.TestCase):
    def _swr(self):
        ns = {"threading": threading, "time": time}
        exec("_CHECKWMS_SWR = {\"stock\": False, \"traza\": False}\n" + _trozo("def _checkwms_swr(clave: str, fn) -> None:", "\n\n\ndef _checkwms_trazabilidad_rows"), ns)
        return ns

    def test_lanza_una_sola_renovacion_a_la_vez(self):
        ns = self._swr()
        llamadas, soltar = [], threading.Event()

        def fn(forzar=False):
            llamadas.append(forzar)
            soltar.wait(2)
        for _ in range(5):
            ns["_checkwms_swr"]("stock", fn)
        time.sleep(0.2)
        self.assertEqual(llamadas, [True], "5 pedidos seguidos = una sola renovación, forzada")
        soltar.set()
        time.sleep(0.2)
        self.assertFalse(ns["_CHECKWMS_SWR"]["stock"], "al terminar se puede volver a renovar")
        ns["_checkwms_swr"]("stock", fn)
        time.sleep(0.2)
        self.assertEqual(len(llamadas), 2)

    def test_un_error_no_rompe_ni_deja_trabada_la_renovacion(self):
        ns = self._swr()

        def fn(forzar=False):
            raise RuntimeError("Check caído")
        ns["_checkwms_swr"]("traza", fn)
        time.sleep(0.2)
        self.assertFalse(ns["_CHECKWMS_SWR"]["traza"])


class TestCodigo(unittest.TestCase):
    def test_vencida_se_sirve_lo_que_hay_y_se_renueva_atras(self):
        for nombre, cache in (("stock", "_CHECKWMS_STOCK"), ("traza", "_CHECKWMS_TRAZA")):
            self.assertIn(f'_checkwms_swr("{nombre}", ', APP)
            self.assertIn(f'(now - {cache}["ts"]) < _CHECKWMS_STALE_MAX', APP)
        self.assertIn("_CHECKWMS_STALE_MAX = 6 * 3600", APP)

    def test_sin_nada_en_memoria_si_se_espera(self):
        f = _trozo("def _checkwms_stock_rows(forzar: bool = False)", "\n\n\ndef _checkwms_precalentar")
        self.assertIn('with _CHECKWMS_STOCK["lock"]:', f)
        self.assertIn('_CHECKWMS_STOCK["rows"] is not None', f)

    def test_el_cron_renueva_el_stock_antes_de_que_venza_con_tope_de_tiempo(self):
        f = _trozo("def _checkwms_precalentar(max_s", "\n\n\ndef _checkwms_norm_ua")
        self.assertIn("edad_s > 1800", f)
        self.assertIn("_checkwms_stock_rows(forzar=True)", f)
        self.assertIn("_checkwms_configurado()", f)
        c = _trozo("def tr_cron_simpliroute_poll():", "def tr_simpliroute_reparar_entregados")
        self.assertIn('res["check_precarga"] = _checkwms_precalentar(', c)
        self.assertIn("if not dry and time.time() - _t_peticion < 150:", c)

    def test_sigue_siendo_solo_lectura(self):
        f = _trozo("def _checkwms_swr(clave: str, fn) -> None:", "\n\n\ndef _checkwms_norm_ua")
        for prohibido in ("requests.post", "requests.put", "requests.delete", "requests.patch", "_req.post"):
            self.assertNotIn(prohibido, f)


if __name__ == "__main__":
    unittest.main()

# -*- coding: utf-8 -*-
"""2026-10-23 · Daniel: «la aplicación se ha sentido más lenta; la velocidad es prioridad del proyecto».

Medido en producción: el uso normal es rápido (p50 de las pantallas < 0,6 s) pero cada deploy dejaba ~18-20 s de congelamiento, porque la
sonda de arranque de Cloud Run era TCP (pasa cuando gunicorn abre el puerto, antes de que los workers carguen la app). Ahora es HTTP a /ping.
Además el cron de saldo de productos (300 consultas al ERP) moría cada hora por tiempo (300 s): ahora corta a los 150 s.
"""
import os
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _leer(*p):
    with open(os.path.join(RAIZ, *p), encoding="utf-8") as fh:
        return fh.read()


DEPLOY = _leer(".github", "workflows", "deploy.yml")
APP = _leer("app.py")


class TestSondaDeArranque(unittest.TestCase):
    def test_el_deploy_usa_una_sonda_http_a_ping(self):
        self.assertIn("--startup-probe=httpGet.path=/ping,httpGet.port=8080,", DEPLOY)
        self.assertIn("failureThreshold=30", DEPLOY)

    def test_ping_no_toca_la_base_ni_pide_sesion(self):
        i = APP.index('@app.route("/ping")')
        f = APP[i:i + 500]
        self.assertIn("def _keepalive_ping():", f)
        self.assertNotIn("mysql", f)
        self.assertNotIn("login_required", f)

    def test_la_sonda_esta_en_el_comando_de_deploy(self):
        i = DEPLOY.index('gcloud run deploy "$SERVICE"')
        j = DEPLOY.index("--startup-probe=", i)
        self.assertLess(j, DEPLOY.index("--env-vars-file", i))
        self.assertIn("--concurrency 16", DEPLOY[i:j])


class TestCronConTope(unittest.TestCase):
    def test_refrescar_saldo_corta_a_los_150_segundos(self):
        i = APP.index("def tr_cron_refrescar_saldo_productos():")
        f = APP[i:i + 14000]
        self.assertIn("if time.time() - _t_cron > 150:", f)
        self.assertIn('"cortado_por_tiempo": _cortado', f)
        self.assertLess(f.index("_t_cron = time.time()"), f.index("for d in docs:"))


if __name__ == "__main__":
    unittest.main()

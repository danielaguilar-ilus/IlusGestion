# -*- coding: utf-8 -*-
"""💨 Humo: el módulo de Tickets se REGISTRA completo en una app Flask.

Por qué existe: app.py llama `register_tickets_routes(app, globals())` dentro de un try/except que solo imprime un aviso
(«[ILUS][WARN] register_tickets_routes: …»). Si algo del módulo falla al registrarse (un nombre mal escrito en un decorador, un valor por defecto
que no existe, dos rutas con el mismo nombre), la aplicación ARRANCA IGUAL y deja a TODO Tickets sin rutas, en silencio. Las pruebas que extraen
una función con `ast` no lo ven. Esta corre el registro de verdad (con un `ctx` de mentira, sin base de datos) y comprueba que las rutas que
importan quedaron puestas.
Correr:  python -m pytest tests/test_tickets_registro_humo.py -q -p no:warnings
"""
import os
import sys
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

try:
    from flask import Flask
    HAY_FLASK = True
except Exception:   # pragma: no cover - el gate de CI no instala Flask
    HAY_FLASK = False


class _CtxFalso(dict):
    """Cualquier función pedida al `ctx` devuelve algo inofensivo: nada/lista vacía en las consultas, y una función neutra para usarla de decorador."""

    @staticmethod
    def _falso(nombre):
        if nombre.endswith("fetchall"):
            return lambda *a, **k: []
        if nombre.endswith(("fetchone", "execute", "execute_returning_rowcount")):
            return lambda *a, **k: None

        def generico(*a, **k):
            if len(a) == 1 and callable(a[0]) and not k:   # @decorador
                return a[0]
            return lambda f: f                              # @decorador(...)
        return generico

    def __missing__(self, k):
        return [] if k.isupper() else self._falso(k)         # las constantes (TIPOS_DOC_CUBICADOR…) llegan vacías

    def get(self, k, default=None):
        if k in self:
            return dict.get(self, k)
        return default if k.isupper() else (default if default is not None else self._falso(k))


@unittest.skipUnless(HAY_FLASK, "Flask no está instalado")
class TestRegistroDeTickets(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if RAIZ not in sys.path:
            sys.path.insert(0, RAIZ)
        cwd = os.getcwd()
        os.chdir(RAIZ)
        try:
            import tickets_module
            cls.app = Flask(__name__, template_folder=os.path.join(RAIZ, "templates"))
            cls.app.secret_key = "prueba"
            tickets_module.register_tickets_routes(cls.app, _CtxFalso())
        finally:
            os.chdir(cwd)
        cls.rutas = {(str(r.rule), m) for r in cls.app.url_map.iter_rules() for m in r.methods - {"HEAD", "OPTIONS"}}

    def test_se_registran_las_rutas_de_tickets(self):
        self.assertGreaterEqual(len({r for r, _m in self.rutas}), 80, "se registraron muy pocas rutas de /tickets/")

    def test_estan_las_rutas_que_no_pueden_faltar(self):
        for ruta, metodo in (("/tickets/cotizaciones", "GET"), ("/tickets/cotizaciones/historico", "GET"),
                             ("/tickets/api/auditoria/correos-duplicados", "GET"),
                             ("/tickets/api/auditoria/correos-duplicados/quitar", "POST")):
            self.assertIn((ruta, metodo), self.rutas, ruta)

    def test_la_limpieza_de_duplicados_solo_acepta_post(self):
        metodos = {m for r, m in self.rutas if r == "/tickets/api/auditoria/correos-duplicados/quitar"}
        self.assertEqual(metodos, {"POST"})

if __name__ == "__main__":
    unittest.main()

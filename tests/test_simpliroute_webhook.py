"""Pruebas del webhook de SimpliRoute (2026-08-07, Daniel: "vamos con el
webhook de SimpliRoute entonces demosle").

Dos cosas se prueban aisladas, sin levantar Flask ni BD (mismo patrón AST
que tests/test_simpliroute_normalizar.py y tests/test_tickets_imap_host.py):

1. Que la ruta pública del webhook (/transporte/webhook/simpliroute) está
   exenta de CSRF -- si no lo estuviera, SimpliRoute recibiría 403 en cada
   entrega y el webhook jamás funcionaría (bug silencioso: no truena en
   local porque nadie llama la ruta con sesión/cookie).
2. Que los helpers de alta/baja/listado del webhook (_simpliroute_registrar_
   webhook / _simpliroute_listar_webhooks / _simpliroute_eliminar_webhook)
   arman la llamada HTTP correcta -- método, path y forma del payload -- sin
   depender de la red real ni de un token válido.

Correr con:  py -m unittest tests.test_simpliroute_webhook -v
"""
import ast
import unittest

APP_SRC = open("app.py", encoding="utf-8", errors="ignore").read()
_ARBOL = ast.parse(APP_SRC)


def _extraer_funcion(nombre):
    for nodo in ast.walk(_ARBOL):
        if isinstance(nodo, ast.FunctionDef) and nodo.name == nombre:
            return ast.unparse(nodo)
    raise AssertionError(f"No se encontró la función '{nombre}' en app.py")


def _extraer_asignacion(nombre):
    for nodo in ast.walk(_ARBOL):
        if isinstance(nodo, (ast.Assign, ast.AnnAssign)):
            targets = nodo.targets if isinstance(nodo, ast.Assign) else [nodo.target]
            for t in targets:
                if isinstance(t, ast.Name) and t.id == nombre:
                    return ast.unparse(nodo)
    raise AssertionError(f"No se encontró la asignación '{nombre}' en app.py")


class TestRutaWebhookExentaDeCSRF(unittest.TestCase):
    """Si esto se rompe, SimpliRoute nunca puede entregar el evento: cada
    POST del webhook llegaría sin cookie de sesión ni csrf_token y el
    before_request global lo rechazaría con 403 antes de tocar la vista."""

    def setUp(self):
        ns = {}
        exec(_extraer_asignacion("_CSRF_EXEMPT_PATHS"), ns)
        exec(_extraer_asignacion("_CSRF_EXEMPT_PREFIXES"), ns)
        exec(_extraer_funcion("_csrf_is_exempt"), ns)
        self.es_exento = ns["_csrf_is_exempt"]

    def test_ruta_receptora_exenta(self):
        self.assertTrue(self.es_exento("/transporte/webhook/simpliroute"))

    def test_cualquier_variante_bajo_el_prefijo_queda_exenta(self):
        # el prefijo cubre cualquier ruta futura bajo /transporte/webhook/
        self.assertTrue(self.es_exento("/transporte/webhook/otro-courier"))

    def test_no_exime_de_mas_de_lo_debido(self):
        # una ruta administrativa común (ej. borrar un manifiesto) NO debe
        # quedar exenta por accidente de un prefijo mal escrito.
        self.assertFalse(self.es_exento("/transporte/manifiestos/1/borrar"))


class TestHelpersRegistroWebhook(unittest.TestCase):
    """_simpliroute_registrar_webhook / _listar_ / _eliminar_ arman la
    llamada correcta a _simpliroute_request. Se stubea esa función (hace
    red real) para capturar CON QUÉ la llaman, no para probar la red."""

    def setUp(self):
        self.llamadas = []

        def _stub_request(method, path, token, *, payload=None, timeout=30):
            self.llamadas.append({
                "method": method, "path": path, "token": token,
                "payload": payload, "timeout": timeout,
            })
            return {"ok": True, "status": 200, "data": {"stub": True}}

        ns = {"_simpliroute_request": _stub_request}
        exec(_extraer_asignacion("SIMPLIROUTE_WEBHOOK_EVENTO"), ns)
        exec(_extraer_funcion("_simpliroute_listar_webhooks"), ns)
        exec(_extraer_funcion("_simpliroute_registrar_webhook"), ns)
        exec(_extraer_funcion("_simpliroute_eliminar_webhook"), ns)
        self.ns = ns

    def test_listar_hace_get_al_endpoint_correcto(self):
        self.ns["_simpliroute_listar_webhooks"]("TOKEN-FELCA")
        self.assertEqual(len(self.llamadas), 1)
        c = self.llamadas[0]
        self.assertEqual(c["method"], "GET")
        self.assertEqual(c["path"], "/v1/addons/webhooks/")
        self.assertEqual(c["token"], "TOKEN-FELCA")

    def test_registrar_hace_post_con_evento_url_y_secreto_en_headers(self):
        self.ns["_simpliroute_registrar_webhook"](
            "TOKEN-FELCA", "https://ilus-app.example/transporte/webhook/simpliroute", "secreto-123")
        c = self.llamadas[0]
        self.assertEqual(c["method"], "POST")
        self.assertEqual(c["path"], "/v1/addons/webhooks/")
        self.assertEqual(c["payload"]["webhook"], "visit_checkout_detailed")
        self.assertEqual(c["payload"]["url"],
                          "https://ilus-app.example/transporte/webhook/simpliroute")
        self.assertEqual(c["payload"]["headers"]["X-ILUS-Webhook-Secret"], "secreto-123")

    def test_registrar_permite_evento_distinto_si_se_pide(self):
        self.ns["_simpliroute_registrar_webhook"](
            "TOKEN-FELCA", "https://x/webhook", "s", evento="route_finished")
        self.assertEqual(self.llamadas[0]["payload"]["webhook"], "route_finished")

    def test_eliminar_hace_delete_con_el_evento_en_el_body(self):
        self.ns["_simpliroute_eliminar_webhook"]("TOKEN-MILLING")
        c = self.llamadas[0]
        self.assertEqual(c["method"], "DELETE")
        self.assertEqual(c["path"], "/v1/addons/webhooks/")
        self.assertEqual(c["payload"], {"webhook": "visit_checkout_detailed"})


if __name__ == "__main__":
    unittest.main(verbosity=2)

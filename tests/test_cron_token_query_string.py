"""Pruebas de _cron_extract_token() -- el helper que evita que los tokens de
los crons internos (FedEx poll, SimpliRoute poll, saldo, guías, resync,
refresco de UF) queden expuestos en texto plano en los logs de acceso de
Cloud Run.

HALLAZGO DE SEGURIDAD (2026-08-07): estos endpoints aceptaban el token
tanto por header (X-Cron-Token / X-UF-Token) como por query string
(?token=...). Cloud Run registra la URL COMPLETA de cada request en Cloud
Logging -- incluido el query string -- así que un job de Cloud Scheduler
configurado con `?token=` deja el token visible para cualquiera con acceso
de lectura a esos logs.

La corrección NO cambia el valor del token ni rompe compatibilidad: sigue
aceptando ?token= como fallback (no sabemos desde el código si Cloud
Scheduler ya está migrado a header), pero:
  1. Prioriza SIEMPRE el header sobre el query string.
  2. Loguea una advertencia cada vez que se usa la vía insegura.
  3. Permite cerrar el hueco por completo con CRON_ALLOW_QUERY_TOKEN=0
     (sin volver a tocar código ni desplegar).

No se importa app.py directo -- abre conexión a BD real al nivel de
módulo. Se extrae la función real con ast/exec y se ejecuta aislada con un
objeto `request` de prueba, mismo patrón de tests/test_tickets_imap_host.py
y tests/test_transporte_scheduler.py.

Correr con:  py -m unittest tests.test_cron_token_query_string -v
"""
import ast
import io
import os
import unittest
from contextlib import redirect_stdout

SRC = open("app.py", encoding="utf-8", errors="ignore").read()
_ARBOL = ast.parse(SRC)


def _extraer(nombre):
    for nodo in ast.walk(_ARBOL):
        if isinstance(nodo, ast.FunctionDef) and nodo.name == nombre:
            return ast.unparse(nodo)
    raise AssertionError(f"No se encontró '{nombre}' en app.py")


_CODIGO_FUNCION = _extraer("_cron_extract_token")


class _CaseInsensitiveGetter:
    """Doble mínimo de werkzeug.datastructures.Headers/MultiDict: solo el
    .get(...) que usa _cron_extract_token, case-insensitive como el header
    real de Flask."""

    def __init__(self, data):
        self._data = {k.lower(): v for k, v in (data or {}).items()}

    def get(self, key, default=None):
        return self._data.get(key.lower(), default)


class _FakeRequest:
    """Doble mínimo de flask.request: headers.get(...) y args.get(...)."""

    def __init__(self, headers=None, args=None, path="/transporte/cron/fedex-track-poll"):
        self.headers = _CaseInsensitiveGetter(headers)
        self.args = _CaseInsensitiveGetter(args)
        self.path = path


def _extraer_token(request_obj, header_name="X-Cron-Token", allow_query_env=None):
    """Ejecuta _cron_extract_token() aislada con el `request` y el
    CRON_ALLOW_QUERY_TOKEN dados. Devuelve (token, log_capturado) -- todo
    dentro del MISMO try/finally que restaura la env var, para no dejar
    "fugas" de estado entre pruebas."""
    ns = {"os": os, "request": request_obj, "print": print}
    prev = os.environ.get("CRON_ALLOW_QUERY_TOKEN")
    if allow_query_env is None:
        os.environ.pop("CRON_ALLOW_QUERY_TOKEN", None)
    else:
        os.environ["CRON_ALLOW_QUERY_TOKEN"] = allow_query_env
    try:
        exec(_CODIGO_FUNCION, ns)
        buf = io.StringIO()
        with redirect_stdout(buf):
            token = ns["_cron_extract_token"](header_name)
        return token, buf.getvalue()
    finally:
        if prev is None:
            os.environ.pop("CRON_ALLOW_QUERY_TOKEN", None)
        else:
            os.environ["CRON_ALLOW_QUERY_TOKEN"] = prev


class TestPrioridadHeaderSobreQuery(unittest.TestCase):
    def test_header_presente_gana_aunque_haya_query(self):
        tok, _ = _extraer_token(_FakeRequest(headers={"X-Cron-Token": "del-header"},
                                              args={"token": "del-query"}))
        self.assertEqual("del-header", tok)

    def test_sin_header_cae_a_query_por_compatibilidad(self):
        tok, _ = _extraer_token(_FakeRequest(args={"token": "solo-query"}))
        self.assertEqual("solo-query", tok)

    def test_sin_header_ni_query_devuelve_vacio(self):
        tok, _ = _extraer_token(_FakeRequest())
        self.assertEqual("", tok)

    def test_header_personalizado_para_uf_refresh(self):
        tok, _ = _extraer_token(_FakeRequest(headers={"X-UF-Token": "uf-secreto"}),
                                 header_name="X-UF-Token")
        self.assertEqual("uf-secreto", tok)

    def test_header_vacio_no_cuenta_como_presente(self):
        # header mandado pero vacío -> debe caer al query, no devolver ""
        tok, _ = _extraer_token(_FakeRequest(headers={"X-Cron-Token": ""},
                                              args={"token": "de-la-query"}))
        self.assertEqual("de-la-query", tok)


class TestKillSwitchCronAllowQueryToken(unittest.TestCase):
    def test_default_permite_query_string(self):
        # Sin la env var seteada, sigue funcionando igual que antes del fix
        # (no rompe los jobs de Cloud Scheduler existentes hoy).
        tok, _ = _extraer_token(_FakeRequest(args={"token": "abc"}))
        self.assertEqual("abc", tok)

    def test_env_en_0_rechaza_el_query_string(self):
        tok, _ = _extraer_token(_FakeRequest(args={"token": "abc"}), allow_query_env="0")
        self.assertEqual("", tok)

    def test_env_en_false_tambien_rechaza(self):
        tok, _ = _extraer_token(_FakeRequest(args={"token": "abc"}), allow_query_env="false")
        self.assertEqual("", tok)

    def test_env_en_1_sigue_permitiendo_explicito(self):
        tok, _ = _extraer_token(_FakeRequest(args={"token": "abc"}), allow_query_env="1")
        self.assertEqual("abc", tok)

    def test_con_header_el_kill_switch_es_irrelevante(self):
        # Si ya migraron a header, da igual el valor de la env var.
        tok, _ = _extraer_token(_FakeRequest(headers={"X-Cron-Token": "por-header"},
                                              args={"token": "por-query"}),
                                 allow_query_env="0")
        self.assertEqual("por-header", tok)


class TestLogueaLaViaInsegura(unittest.TestCase):
    def test_usar_query_string_deja_rastro_en_el_log(self):
        _, log = _extraer_token(_FakeRequest(args={"token": "abc"},
                                              path="/transporte/cron/fedex-track-poll"))
        self.assertIn("INSEGURO", log)
        self.assertIn("/transporte/cron/fedex-track-poll", log)

    def test_usar_header_no_genera_advertencia(self):
        _, log = _extraer_token(_FakeRequest(headers={"X-Cron-Token": "abc"}))
        self.assertEqual("", log)

    def test_sin_token_alguno_no_genera_advertencia(self):
        _, log = _extraer_token(_FakeRequest())
        self.assertEqual("", log)

    def test_bloqueo_por_kill_switch_tambien_se_loguea(self):
        _, log = _extraer_token(_FakeRequest(args={"token": "abc"}), allow_query_env="0")
        self.assertIn("BLOQUEADO", log)


if __name__ == "__main__":
    unittest.main(verbosity=2)

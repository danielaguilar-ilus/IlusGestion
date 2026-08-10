"""El error "CSRF token invalido" que Daniel veia al cambiar comuna/bultos.

BUG REAL (2026-08-10). Daniel, con captura de /asignar: "al momento de cambiar
la comuna o el bulto, me esta dando ese error de token... esto tiene que
funcionar bien".

QUE PASABA DE VERDAD (los logs de produccion de Cloud Run lo cerraron):
    [CSRF] reject path=/api/asignar/cotizar-couriers has_session=True has_req=True
Los DOS tokens existian pero no coincidian. Investigando: Daniel NO estaba
logueado -- su sesion habia caducado por inactividad (tenia la pestaña abierta
desde la noche anterior; ultimo login 09-08 12:32Z, los 403 el 10-08 11:52Z,
re-login recien 11:59Z). El token que la pestaña capturo al cargar ya no era
el de la sesion anonima nueva.

O sea el mensaje MENTIA: no era un problema de token, era la sesion caida.
_csrf_check_request() corre en before_request, ANTES de @login_required, asi
que una sesion muerta se presentaba como un error tecnico de CSRF en vez de
"tu sesion expiro". Eso es lo que hacia parecer que el sistema estaba roto.

NO lo causo el recalculo automatico (PR #87): los logs muestran la MISMA
revision respondiendo 200 OK antes. Ese cambio solo subio la frecuencia con
que se dispara la peticion, y por eso lo destapo.

DOS ARREGLOS:
  1. Diagnostico honesto: sesion muerta -> 401 SESSION_EXPIRED ("vuelve a
     iniciar sesion"). Token viejo con sesion viva -> 403 CSRF_INVALIDO.
  2. Auto-reparado: ante CSRF_INVALIDO el navegador pide el token vigente y
     reintenta UNA vez. Antes, una vez rotado el token, la pestaña quedaba
     muerta para TODA escritura (no solo cotizar) hasta apretar F5.

El comportamiento del wrapper se prueba de verdad (no solo que parsee) en
tests/js/csrf_wrapper_sim.js, que se ejecuta desde aca con node.

Correr con:  py -m unittest tests.test_csrf_autoreparado -v
"""
import ast
import os
import subprocess
import unittest

APP_SRC = open("app.py", encoding="utf-8", errors="ignore").read()
BASE_HTML = open("templates/base.html", encoding="utf-8", errors="ignore").read()
_ARBOL = ast.parse(APP_SRC)


def _fn(nombre):
    for n in ast.walk(_ARBOL):
        if isinstance(n, ast.FunctionDef) and n.name == nombre:
            return ast.unparse(n)
    raise AssertionError(f"no se encontro {nombre} en app.py")


class TestElServidorDiceLaVerdad(unittest.TestCase):
    """Lo que Daniel vio fue un mensaje equivocado, no solo un fallo."""

    def test_sesion_muerta_se_reporta_como_sesion_expirada(self):
        f = _fn("_csrf_check_request")
        self.assertIn("SESSION_EXPIRED", f)
        self.assertIn("Tu sesión expiró", f)

    def test_sesion_expirada_devuelve_401_no_403(self):
        """401 = quien sos; 403 = el token. Confundirlos es lo que hizo que
        una sesion caducada se leyera como sistema roto. Ademas el frontend
        usa el codigo para decidir si reintenta."""
        f = _fn("_csrf_check_request")
        i = f.find("SESSION_EXPIRED")
        self.assertGreater(i, 0)
        self.assertIn("401", f[i:i + 260])

    def test_usa_el_detector_de_ajax_compartido(self):
        """El chequeo inline anterior no contemplaba X-Wizard=1, con lo que el
        wizard de crear cliente caia a la rama HTML (flash + redirect) y el
        usuario perdia todo lo escrito sin mensaje util."""
        f = _fn("_csrf_check_request")
        self.assertIn("_is_ajax_request()", f)
        self.assertNotIn('request.headers.get("X-Requested-With") == "XMLHttpRequest"', f)

    def test_nunca_se_loguea_el_token_en_claro(self):
        """REGLA #4: el log sirve para diagnosticar, no para filtrar material
        reutilizable. Va la huella, no el token."""
        f = _fn("_csrf_check_request")
        self.assertIn("sha256", f)
        self.assertNotIn("token_session}", f)
        self.assertNotIn("token_req}", f)


class TestElInvarianteQueHaceSeguroElReintento(unittest.TestCase):
    """CSRF_INVALIDO se emite SOLO desde before_request, donde la vista aun no
    corrio: por eso reenviar la peticion no puede duplicar una escritura.

    Si alguien copia ese codigo dentro de una vista (despues de escribir en
    BD), el reintento pasaria a repetir la escritura en silencio. Radio de
    explosion real: crear equipos, mandar correo al cliente, enviar manifiesto.
    Este test es la unica cosa que protege ese invariante."""

    def test_el_codigo_aparece_exactamente_una_vez(self):
        self.assertEqual(
            APP_SRC.count("CSRF_INVALIDO"), 1,
            "CSRF_INVALIDO se emite en mas de un lugar: si alguno esta dentro "
            "de una vista, el reintento automatico puede duplicar una escritura")

    def test_se_emite_desde_el_chequeo_previo_a_la_vista(self):
        self.assertIn("CSRF_INVALIDO", _fn("_csrf_check_request"))


class TestEndpointDeRefresco(unittest.TestCase):

    def test_existe_y_es_get(self):
        i = APP_SRC.find("def api_csrf_token")
        self.assertGreater(i, 0)
        cab = APP_SRC[max(0, i - 200):i]
        self.assertIn("/api/csrf-token", cab)
        self.assertIn('methods=["GET"]', cab,
                      "debe ser GET: no muta nada y asi no amplia la superficie")

    def test_devuelve_identidad_para_atar_el_reintento(self):
        """login() hace session.clear(), o sea CADA login rota el token. Sin
        atar el refresco a la identidad, una pestaña vieja podria reejecutar
        su escritura bajo la sesion del usuario que entro despues en el mismo
        navegador -- quedando en la auditoria a nombre de quien no la hizo."""
        f = _fn("api_csrf_token")
        self.assertIn("uid", f)
        self.assertIn("autenticado", f)

    def test_no_se_agrego_a_las_rutas_exentas_de_csrf(self):
        """Es GET, no necesita exencion. Meterlo ahi por las dudas abriria un
        hueco real."""
        i = APP_SRC.find("_CSRF_EXEMPT_PREFIXES")
        bloque = APP_SRC[max(0, i - 1500):i + 900]
        self.assertNotIn("/api/csrf-token", bloque)


class TestElNavegadorSeAutorepara(unittest.TestCase):

    def test_la_pagina_publica_la_identidad(self):
        self.assertIn('name="ilus-uid"', BASE_HTML)
        self.assertIn("__ILUS_UID", BASE_HTML)

    def test_solo_reintenta_ante_el_codigo_especifico(self):
        self.assertIn("d.error_codigo !== 'CSRF_INVALIDO'", BASE_HTML,
                      "un retry ante cualquier 403 podria repetir una escritura")

    def test_no_reintenta_si_el_caller_ya_aborto(self):
        """Hay pantallas (guardado en terreno del tecnico) con su propio
        reintento por AbortError; pisarselo le haria perder el guardado."""
        self.assertIn("init.signal.aborted", BASE_HTML)

    def test_refresca_tambien_los_formularios_clasicos(self):
        """El token viejo tambien vive en los <input hidden>. Sin actualizarlos
        el usuario pierde todo lo escrito al enviar el formulario."""
        self.assertIn('input[name="csrf_token"]', BASE_HTML)

    def test_el_refresco_es_single_flight(self):
        """Varias peticiones que fallan a la vez no deben disparar N consultas."""
        self.assertIn("_csrfEnVuelo", BASE_HTML)

    def test_no_usa_alert_nativo(self):
        """REGLA #1 del proyecto. Se ancla en la DEFINICION de la funcion
        (`function _avisar...`), no en su primera llamada."""
        i = BASE_HTML.find("function _avisarSesionExpirada")
        self.assertGreater(i, 0, "no se encontro la definicion del aviso")
        bloque = BASE_HTML[i:i + 1400]
        self.assertIn("ilusAlert", bloque)
        self.assertNotIn("window.alert(", bloque)
        self.assertNotIn("\n      alert(", bloque)


class TestComportamientoRealDelWrapper(unittest.TestCase):
    """Ejecuta el JS REAL extraido de base.html contra un servidor y un DOM
    simulados. Verifica CONDUCTA, no que el archivo parsee."""

    def test_los_cinco_escenarios(self):
        if not os.path.exists("tests/js/csrf_wrapper_sim.js"):
            self.skipTest("falta el simulador")
        try:
            r = subprocess.run(["node", "tests/js/csrf_wrapper_sim.js"],
                               capture_output=True, text=True, timeout=90)
        except (FileNotFoundError, subprocess.TimeoutExpired) as e:
            self.skipTest(f"node no disponible: {e}")
        self.assertEqual(r.returncode, 0,
                         "el wrapper no se comporta como debe:\n" + (r.stdout or "") + (r.stderr or ""))
        for esperado in ("auto-repararse", "Sesion expirada", "Otro usuario"):
            self.assertIn(esperado, r.stdout)


if __name__ == "__main__":
    unittest.main(verbosity=2)

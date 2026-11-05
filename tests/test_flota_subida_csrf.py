# -*- coding: utf-8 -*-
"""🚚 FLOTA · la subida con barra (XMLHttpRequest) se REPARA SOLA cuando el token CSRF de la pestaña quedó viejo.

Daniel, 05-nov-2026, registrando un chofer con la llave por entregar en 10 minutos: «CSRF token inválido. Recarga la página» (4 intentos, 4 × 403).
El fetch de base.html ya se repara solo ante CSRF_INVALIDO; el XMLHttpRequest de flota_subida.js no: la pestaña quedaba muerta hasta un F5.
Se ejecuta el JS REAL en Node con un XMLHttpRequest y un fetch de mentira.

Correr:  python -m pytest tests/test_flota_subida_csrf.py -q -p no:warnings
"""
import json
import os
import shutil
import subprocess
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
JS = os.path.join(RAIZ, "static", "flota_subida.js")
NODE = shutil.which("node")

PRUEBA = r"""
const fs = require('fs'), vm = require('vm');
const casos = JSON.parse(process.argv[1]);
function correr(caso) {
  const llamadas = { xhr: [], fetch: [] };
  let n = 0;
  class XHR {
    constructor() { this.cab = {}; this.upload = {}; }
    open(m, u) { this.m = m; this.u = u; }
    // Como el XMLHttpRequest REAL: llamar dos veces a setRequestHeader con el mismo nombre JUNTA los valores («a, a»).
    setRequestHeader(k, v) { this.cab[k] = (k in this.cab) ? this.cab[k] + ', ' + v : v; }
    send(f) {
      const r = caso.respuestas[n++] || { status: 200, body: { ok: true } };
      llamadas.xhr.push({ metodo: this.m, url: this.u, token: this.cab['X-CSRF-Token'] || null, form: f });
      setTimeout(() => { this.status = r.status; this.responseText = JSON.stringify(r.body); this.onload(); }, 0);
    }
  }
  const win = { __CSRF_TOKEN: caso.tokenViejo, __ILUS_UID: caso.uidPagina };
  if (caso.parche !== false) {
    // Réplica del parche GLOBAL de templates/base.html (desde mayo): agrega el token a todo XMLHttpRequest al enviarlo.
    const _open = XHR.prototype.open, _send = XHR.prototype.send;
    XHR.prototype.open = function (method) { this.__csrf_method = (method || 'GET').toString().toUpperCase(); return _open.apply(this, arguments); };
    XHR.prototype.send = function (body) {
      try { if (this.__csrf_method && this.__csrf_method !== 'GET' && this.__csrf_method !== 'HEAD' && win.__CSRF_TOKEN) this.setRequestHeader('X-CSRF-Token', win.__CSRF_TOKEN); } catch (e) {}
      return _send.apply(this, arguments);
    };
  }
  const meta = { v: caso.tokenViejo, setAttribute(k, v) { this.v = v; }, getAttribute() { return this.v; } };
  const ctx = {
    window: win, XMLHttpRequest: XHR, console,
    document: { querySelector: (s) => (s.indexOf('csrf-token') >= 0 ? meta : null), createElement: () => ({}) },
    fetch: (url, opt) => { llamadas.fetch.push(url); if (caso.fetchFalla) return Promise.reject(new Error('sin red')); return Promise.resolve({ json: () => Promise.resolve(caso.csrf) }); },
    setTimeout, Promise, JSON, Object, String, Number, Math,
  };
  win.fetch = ctx.fetch;
  vm.createContext(ctx);
  vm.runInContext(fs.readFileSync(process.env.JS_SUBIDA, 'utf8'), ctx);
  return win.flotaSubida.enviar({ url: '/flota/api/choferes', method: 'POST', form: { dato: 1 } }).then((d) => ({ d, llamadas, tokenFinal: win.__CSRF_TOKEN, meta: meta.v }));
}
(async () => {
  const salida = {};
  for (const [nombre, caso] of Object.entries(casos)) salida[nombre] = await correr(caso);
  process.stdout.write(JSON.stringify(salida));
})();
"""

VIEJO_403 = {"status": 403, "body": {"ok": False, "error_codigo": "CSRF_INVALIDO", "error": "CSRF token inválido. Recarga la página."}}
OK_200 = {"status": 200, "body": {"ok": True, "chofer": {"id": 7}}}
CASOS = {
    # el token de la página es viejo, la sesión sigue viva y es la misma persona → se renueva y se reenvía una vez
    "se_repara": {"tokenViejo": "viejo", "uidPagina": "5", "respuestas": [VIEJO_403, OK_200],
                  "csrf": {"csrf_token": "nuevo", "autenticado": True, "uid": 5}},
    # la sesión murió → no se reintenta y se explica
    "sesion_muerta": {"tokenViejo": "viejo", "uidPagina": "5", "respuestas": [VIEJO_403, OK_200],
                      "csrf": {"csrf_token": "x", "autenticado": False, "uid": 0}},
    # entró OTRA persona en el mismo navegador → no se reintenta (quedaría a nombre de quien no lo hizo)
    "otra_persona": {"tokenViejo": "viejo", "uidPagina": "5", "respuestas": [VIEJO_403, OK_200],
                     "csrf": {"csrf_token": "nuevo", "autenticado": True, "uid": 9}},
    # el servidor devuelve el MISMO token: reintentar sería inútil
    "mismo_token": {"tokenViejo": "viejo", "uidPagina": "5", "respuestas": [VIEJO_403, OK_200],
                    "csrf": {"csrf_token": "viejo", "autenticado": True, "uid": 5}},
    # sigue fallando después de renovar: un solo reintento, sin bucle
    "un_solo_reintento": {"tokenViejo": "viejo", "uidPagina": "5", "respuestas": [VIEJO_403, VIEJO_403, OK_200],
                          "csrf": {"csrf_token": "nuevo", "autenticado": True, "uid": 5}},
    # un 403 que NO es del token (sin permiso) no se reintenta jamás
    "otro_403": {"tokenViejo": "viejo", "uidPagina": "5",
                 "respuestas": [{"status": 403, "body": {"ok": False, "error_codigo": "SIN_PERMISO_ADMIN_FLOTA", "error": "No tienes permiso."}}, OK_200],
                 "csrf": {"csrf_token": "nuevo", "autenticado": True, "uid": 5}},
    # un error de validación (400) tampoco
    "error_400": {"tokenViejo": "viejo", "uidPagina": "5", "respuestas": [{"status": 400, "body": {"ok": False, "error": "Revisa los campos."}}, OK_200],
                  "csrf": {"csrf_token": "nuevo", "autenticado": True, "uid": 5}},
    # el servidor no responde al pedir el token nuevo → se devuelve el error original
    "sin_red_al_renovar": {"tokenViejo": "viejo", "uidPagina": "5", "respuestas": [VIEJO_403, OK_200], "fetchFalla": True,
                           "csrf": {}},
    # una página que NO trae el parche global de base.html: la subida pone el token ella misma, una sola vez
    "sin_parche": {"tokenViejo": "bueno", "uidPagina": "5", "parche": False, "respuestas": [OK_200], "csrf": {"csrf_token": "x", "autenticado": True, "uid": 5}},
    # todo bien a la primera: ni se toca el token
    "normal": {"tokenViejo": "bueno", "uidPagina": "5", "respuestas": [OK_200], "csrf": {"csrf_token": "x", "autenticado": True, "uid": 5}},
}


@unittest.skipIf(NODE is None, "sin Node")
class TestSubidaSeRepara(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        env = dict(os.environ, JS_SUBIDA=JS)
        r = subprocess.run([NODE, "-e", PRUEBA, json.dumps(CASOS)], capture_output=True, text=True, encoding="utf-8", env=env, timeout=60)
        if r.returncode != 0:
            raise AssertionError("Node falló: " + r.stderr[-800:])
        cls.R = json.loads(r.stdout)

    def test_se_repara_y_la_subida_sale(self):
        r = self.R["se_repara"]
        self.assertTrue(r["d"]["ok"])
        self.assertEqual([x["token"] for x in r["llamadas"]["xhr"]], ["viejo", "nuevo"])      # el reenvío va con el token nuevo
        self.assertEqual(r["llamadas"]["fetch"], ["/api/csrf-token"])
        self.assertEqual((r["tokenFinal"], r["meta"]), ("nuevo", "nuevo"))                    # la pestaña queda sana para las siguientes
        self.assertEqual(r["llamadas"]["xhr"][0]["form"], r["llamadas"]["xhr"][1]["form"])    # el mismo contenido

    def test_con_la_sesion_muerta_no_reintenta_y_lo_dice(self):
        r = self.R["sesion_muerta"]
        self.assertEqual(len(r["llamadas"]["xhr"]), 1)
        self.assertFalse(r["d"]["ok"])
        self.assertIn("sesión expiró", r["d"]["error"])

    def test_si_entro_otra_persona_no_reintenta(self):
        r = self.R["otra_persona"]
        self.assertEqual(len(r["llamadas"]["xhr"]), 1)
        self.assertIn("otra persona", r["d"]["error"])
        self.assertEqual(r["tokenFinal"], "viejo")                                            # no se adopta el token de otra sesión

    def test_con_el_mismo_token_no_reintenta(self):
        self.assertEqual(len(self.R["mismo_token"]["llamadas"]["xhr"]), 1)

    def test_un_solo_reintento_nunca_un_bucle(self):
        r = self.R["un_solo_reintento"]
        self.assertEqual(len(r["llamadas"]["xhr"]), 2)
        self.assertFalse(r["d"]["ok"])
        self.assertEqual(r["d"]["error_codigo"], "CSRF_INVALIDO")

    def test_solo_el_error_del_token_se_reintenta(self):
        for caso in ("otro_403", "error_400"):
            with self.subTest(caso=caso):
                r = self.R[caso]
                self.assertEqual(len(r["llamadas"]["xhr"]), 1)
                self.assertEqual(r["llamadas"]["fetch"], [])                                  # ni siquiera pide un token nuevo

    def test_sin_red_al_renovar_devuelve_el_error_original(self):
        r = self.R["sin_red_al_renovar"]
        self.assertEqual(len(r["llamadas"]["xhr"]), 1)
        self.assertEqual(r["d"]["error_codigo"], "CSRF_INVALIDO")

    def test_el_token_viaja_UNA_sola_vez_nunca_duplicado(self):
        # Bug real del 05-nov-2026: flota_subida.js ponía el token Y el parche global de base.html lo ponía otra vez → «tok, tok» → TODA subida rechazada.
        r = self.R["normal"]
        self.assertEqual(r["llamadas"]["xhr"][0]["token"], "bueno")
        self.assertNotIn(",", r["llamadas"]["xhr"][0]["token"])
        for caso in ("se_repara", "un_solo_reintento"):
            for x in self.R[caso]["llamadas"]["xhr"]:
                self.assertNotIn(",", x["token"], caso)

    def test_sin_el_parche_global_la_subida_pone_el_token_ella_misma_una_vez(self):
        r = self.R["sin_parche"]
        self.assertEqual([x["token"] for x in r["llamadas"]["xhr"]], ["bueno"])

    def test_la_pagina_real_sigue_trayendo_el_parche_que_la_subida_detecta(self):
        base = open(os.path.join(RAIZ, "templates", "base.html"), encoding="utf-8").read()
        self.assertIn("XMLHttpRequest.prototype.send = function(body){", base)
        self.assertIn("this.__csrf_method", base)                      # la subida lo detecta por este nombre: si cambia en base.html, se cambia aquí
        with open(JS, encoding="utf-8") as f:
            self.assertIn("indexOf('__csrf_method')", f.read())

    def test_si_todo_sale_bien_no_se_toca_nada(self):
        r = self.R["normal"]
        self.assertTrue(r["d"]["ok"])
        self.assertEqual(len(r["llamadas"]["xhr"]), 1)
        self.assertEqual(r["llamadas"]["fetch"], [])
        self.assertEqual(r["tokenFinal"], "bueno")

    def test_la_api_publica_sigue_igual(self):
        with open(JS, encoding="utf-8") as f:
            src = f.read()
        self.assertIn("window.flotaSubida = { comprimir: comprimir, enviar: enviar, barra: barra, mb: mb };", src)


if __name__ == "__main__":
    unittest.main()

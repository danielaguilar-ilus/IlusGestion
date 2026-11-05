# -*- coding: utf-8 -*-
"""🔐 PASSKEYS (Face ID / huella) · la pantalla (Daniel, 05-nov-2026): «le pregunte si quiere… y dejarlo habilitado por el front… en Mi cuenta,
donde salen mis datos, necesito controlarlo».

Se vigila que el control esté en Mi cuenta (interruptor + lista de dispositivos con «Quitar»), que el login tenga su botón, que la pregunta amable salga
SOLO en el celular y una vez (no a quien ya lo activó, lo rechazó tres veces o dijo «ahora no» hace menos de 14 días), y se EJECUTA con Node el
JavaScript de ilus_passkeys.js (con un teléfono y un servidor de mentira) para comprobar lo que manda y lo que guarda.
Correr:  python -m pytest tests/test_passkeys_front.py -q -p no:warnings
"""
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
HAY_NODE = shutil.which("node") is not None


def _leer(*partes):
    with open(os.path.join(RAIZ, *partes), encoding="utf-8") as f:
        return f.read()


JS = _leer("static", "ilus_passkeys.js")
TARJETA = _leer("templates", "_passkeys_card.html")
LOGIN_PARCIAL = _leer("templates", "_passkeys_login.html")
BASE = _leer("templates", "base.html")
LOGIN = _leer("templates", "login.html")
CUENTA = _leer("templates", "mi_cuenta.html")
APP = _leer("app.py")


class TestEnganche(unittest.TestCase):
    def test_base_avisa_que_existe_y_carga_el_script_despues_de_ilus_ui(self):
        self.assertIn('<meta name="ilus-passkeys" content="1">', BASE)
        self.assertIn("{% if passkeys_activas %}", BASE)
        self.assertLess(BASE.index("filename='ilus_ui.js'"), BASE.index("filename='ilus_passkeys.js'"))       # usa ilusConfirm / ilusToast
        self.assertIn("defer", BASE[BASE.index("filename='ilus_passkeys.js'") - 40:BASE.index("filename='ilus_passkeys.js'")])

    def test_el_login_lleva_el_boton_despues_del_formulario_y_antes_de_olvidaste_tu_contraseña(self):
        i = LOGIN.index("{% include '_passkeys_login.html' ignore missing %}")
        self.assertGreater(i, LOGIN.index("</form>"))
        self.assertLess(i, LOGIN.index("forgot_password"))
        self.assertIn('data-next="{{ next_url }}"', LOGIN_PARCIAL)
        self.assertRegex(LOGIN_PARCIAL, r'id="pkLoginBtn" hidden')                      # nace oculto: el script lo muestra solo si el teléfono puede

    def test_mi_cuenta_lleva_el_control_antes_de_seguridad(self):
        i = CUENTA.index("{% include '_passkeys_card.html' ignore missing %}")
        self.assertLess(i, CUENTA.index("3. SEGURIDAD"))
        self.assertGreater(i, CUENTA.index("<h6>Datos personales</h6>"))

    def test_el_control_tiene_interruptor_lista_y_el_respaldo_de_la_contraseña(self):
        for id_ in ("pkCard", "pkSwitch", "pkEstado", "pkLista", "pkAviso"):
            self.assertIn(f'id="{id_}"', TARJETA, id_)
        self.assertIn('role="switch"', TARJETA)
        self.assertIn("Usar Face ID / huella en este dispositivo", TARJETA)
        self.assertIn("Tu contraseña sigue funcionando siempre", TARJETA)
        self.assertIn("tu cara y tu huella nunca salen del teléfono", TARJETA)
        self.assertIn("Quitar", JS)

    def test_el_interruptor_se_ve_bien_aunque_mobile_css_fuerce_casillas_de_22_px(self):
        mobile = _leer("static", "mobile.css")
        self.assertIn("width: 22px !important", mobile)                                  # la regla de la app que deformaba el interruptor
        self.assertRegex(TARJETA, r"\.form-check-input\{float:none;margin:0 !important;width:3\.1rem !important;height:1\.7rem !important")

    def test_el_acceso_del_login_es_un_enlace_sutil_pero_tactil(self):
        """Daniel, 05-nov-2026: «el botón del login del Face ID haz algo más sutil»: sin caja, sin fondo, letra chica y gris; pero con blanco táctil de 44 px."""
        css = LOGIN_PARCIAL[LOGIN_PARCIAL.index(".pk-login-btn{"):LOGIN_PARCIAL.index(".pk-login-btn[hidden]")]
        self.assertIn("border:0;background:transparent", css)
        self.assertIn("min-height:44px", css)
        self.assertIn("font-size:.85rem", css)
        self.assertNotIn("#0a0a0a", css)
        self.assertNotIn("width:100%", css)
        self.assertNotIn("pk-login-btn--ya", LOGIN_PARCIAL + JS)                      # ya no hay un «botón principal» negro
        self.assertIn("min-height:44px", TARJETA)

    def test_las_rutas_sin_sesion_estan_exentas_de_csrf_y_del_corte_por_inactividad(self):
        self.assertIn('"/passkeys/login/",', APP[APP.index("_CSRF_EXEMPT_PREFIXES"):APP.index("_CSRF_EXEMPT_PREFIXES") + 2500])
        self.assertIn("startswith(\"/passkeys/login/\")", APP[APP.index("_idle_expired', False)"):APP.index("_idle_expired', False)") + 200])
        self.assertIn("register_passkey_routes(app, globals())", APP)

    def test_solo_las_rutas_de_ingreso_son_publicas(self):
        mod = _leer("passkeys_module.py")
        publicas = re.findall(r'@app\.route\("(/passkeys/[^"]+)"[^\n]*\)\n(?:    @[^\n]+\n)*', mod)
        sin_login = [r for r in publicas if "@login_required" not in mod[mod.index(f'"{r}"'):mod.index(f'"{r}"') + 200]]
        self.assertEqual(sorted(sin_login), ["/passkeys/login/opciones", "/passkeys/login/verificar"])


class TestSinRomperNada(unittest.TestCase):
    def test_el_script_nunca_rompe_la_pagina(self):
        self.assertIn("try { marcarCierreDeSesion(); iniciarMiCuenta(); iniciarLogin(); ofrecer(); } catch (e) { /* nunca romper la página por esto */ }", JS)

    def test_usa_los_dialogos_de_ilus_y_nunca_los_nativos(self):
        for prohibido in ("alert(", "confirm(", "prompt("):
            self.assertNotRegex(JS, r"(?<![A-Za-z.])" + re.escape(prohibido))
        self.assertIn("window.ilusConfirm", JS)
        self.assertIn("window.ilusToast", JS)

    def test_pide_la_verificacion_en_el_propio_telefono_y_no_guarda_secretos(self):
        self.assertIn("isUserVerifyingPlatformAuthenticatorAvailable", JS)
        self.assertNotIn("password", JS.lower())
        self.assertNotIn("privateKey", JS)


# ── El JavaScript EJECUTADO con Node: un teléfono y un servidor de mentira ──
_PRUEBA_JS = r'''
const fs = require('fs');
const src = fs.readFileSync(process.argv[2], 'utf8');
const resultados = {};
function crearMundo(cfg) {
  cfg = cfg || {};
  const guardado = Object.assign({}, cfg.local || {}), sesion = Object.assign({}, cfg.sesion || {});
  const localStorage = { getItem: k => (k in guardado ? guardado[k] : null), setItem: (k, v) => { guardado[k] = String(v); }, removeItem: k => { delete guardado[k]; } };
  const llamadas = { fetch: [], confirm: [], toasts: [], create: [], get: [], handlers: {} };
  const elementos = Object.assign({}, cfg.elementos || {});
  const document = {
    readyState: 'complete', hidden: !!cfg.enSegundoPlano, addEventListener(ev, fn) { (llamadas.handlers[ev] = llamadas.handlers[ev] || []).push(fn); },
    querySelector: s => (s === 'meta[name="ilus-passkeys"]' ? (cfg.sinMeta ? null : { getAttribute: () => '1' }) : (s === '.modal.show' ? (cfg.modalAbierto ? {} : null) : (s === 'input[name="username"]' ? (cfg.usuario || null) : null))),
    getElementById: id => elementos[id] || null
  };
  const respuestas = cfg.respuestas || {};
  const fetch = async (url, init) => {
    llamadas.fetch.push({ url, metodo: (init && init.method) || 'GET', cuerpo: init && init.body ? JSON.parse(init.body) : null });
    const r = respuestas[url];
    const d = typeof r === 'function' ? r() : (r || { ok: true });
    return { status: d.status || 200, json: async () => d };
  };
  const navigator = { userAgent: cfg.ua || 'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1',
    standalone: !!cfg.standalone, credentials: { create: async o => { llamadas.create.push(o); if (cfg.createFalla) throw cfg.createFalla; return cfg.credencial; }, get: async o => { llamadas.get.push(o); if (cfg.getColgado) await new Promise(() => {}); if (cfg.getFalla) throw cfg.getFalla; return cfg.asercion; } } };
  const window = {
    localStorage, isSecureContext: cfg.inseguro ? false : true, setTimeout: f => { f(); return 1; }, __ILUS_UID: cfg.uid === undefined ? '0' : cfg.uid,
    matchMedia: q => ({ matches: !!(cfg.mediaCelular && /max-width: 860px/.test(q)) }),
    PublicKeyCredential: cfg.sinApi ? undefined : { isUserVerifyingPlatformAuthenticatorAvailable: async () => cfg.plataforma !== false },
    ilusConfirm: cfg.sinConfirm ? undefined : (o => { llamadas.confirm.push(o); return Promise.resolve(cfg.respondeConfirm === undefined ? false : cfg.respondeConfirm); }),
    ilusToast: (t, o) => llamadas.toasts.push([t, (o || {}).type]),
    location: { href: '' }, setInterval: () => 1,
    sessionStorage: { getItem: k => (k in sesion ? sesion[k] : null), setItem: (k, v) => { sesion[k] = String(v); }, removeItem: k => { delete sesion[k]; } }
  };
  new Function('window', 'document', 'navigator', 'fetch', 'btoa', 'atob', src)(window, document, navigator, fetch, s => Buffer.from(s, 'binary').toString('base64'), s => Buffer.from(s, 'base64').toString('binary'));
  return { w: window.ilusPasskeys, window, llamadas, guardado, sesion };
}
const buf = (...b) => new Uint8Array(b).buffer;
const b64 = ab => Buffer.from(new Uint8Array(ab)).toString('base64url');
(async () => {
  // 1. base64url ida y vuelta (con bytes altos) y contra la referencia de Node
  let m = crearMundo();
  const bytes = Uint8Array.from([0, 1, 250, 251, 252, 253, 254, 255, 62, 63]);
  resultados.b64 = { enc: m.w.aB64u(bytes.buffer), ref: Buffer.from(bytes).toString('base64url'), vuelta: Array.from(new Uint8Array(m.w.deB64u(m.w.aB64u(bytes.buffer)))), sinRelleno: !/[=+/]/.test(m.w.aB64u(bytes.buffer)) };
  // 2. etiquetas de dispositivo
  const uas = { iphoneSafari: 'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1',
    androidChrome: 'Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Mobile Safari/537.36',
    windowsEdge: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36 Edg/130.0',
    macChrome: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36' };
  resultados.etiquetas = {};
  for (const k of Object.keys(uas)) resultados.etiquetas[k] = crearMundo({ ua: uas[k] }).w.etiquetaDispositivo();
  resultados.etiquetas.iphoneInstalada = crearMundo({ ua: uas.iphoneSafari, standalone: true }).w.etiquetaDispositivo();
  // 3. mensajes de error del teléfono
  m = crearMundo();
  resultados.errores = ['NotAllowedError', 'AbortError', 'InvalidStateError', 'NotSupportedError', 'SecurityError', 'Otro'].map(n => m.w.mensajeDeError({ name: n, message: 'x' }));
  // 4. ¿soporta?
  resultados.soporta = [await crearMundo().w.soportado(), await crearMundo({ sinMeta: true }).w.soportado(), await crearMundo({ plataforma: false }).w.soportado(),
    await crearMundo({ inseguro: true }).w.soportado(), await crearMundo({ sinApi: true }).w.soportado()];
  // 5. registrar este dispositivo
  const opciones = { ok: true, publicKey: { challenge: b64(buf(1, 2, 3)), rp: { id: 'ilus.test', name: 'ILUS' }, user: { id: b64(buf(55)), name: 'a@b.c', displayName: 'Ana' },
    pubKeyCredParams: [{ type: 'public-key', alg: -7 }], excludeCredentials: [{ type: 'public-key', id: b64(buf(9, 9)) }], authenticatorSelection: { userVerification: 'required' } } };
  const respuesta = { response: { clientDataJSON: buf(10, 11), getPublicKey: () => buf(20, 21, 22), getPublicKeyAlgorithm: () => -7, getAuthenticatorData: () => buf(30, 31), getTransports: () => ['internal'] } };
  m = crearMundo({ respuestas: { '/passkeys/registro/opciones': opciones, '/passkeys/registro/verificar': { ok: true, credencial: 'CRED-1', dispositivos: [] } },
    credencial: Object.assign({ id: 'CRED-1' }, respuesta) });
  const r5 = await m.w.registrar();
  const crear = m.llamadas.create[0].publicKey, ver = m.llamadas.fetch.find(f => f.url.endsWith('/verificar')).cuerpo;
  resultados.registrar = { ok: r5.ok, tiposDeCreate: [crear.challenge.constructor.name, crear.user.id.constructor.name, crear.excludeCredentials[0].id.constructor.name],
    bytesDelDesafio: Array.from(new Uint8Array(crear.challenge)), verificar: ver, local: m.guardado };
  // 5b. cancelado / navegador viejo
  m = crearMundo({ respuestas: { '/passkeys/registro/opciones': opciones }, createFalla: { name: 'NotAllowedError' } });
  const r5b = await m.w.registrar();
  resultados.registrarCancelado = { ok: r5b.ok, cancelado: r5b.cancelado, error: r5b.error, local: m.guardado };
  m = crearMundo({ respuestas: { '/passkeys/registro/opciones': opciones }, credencial: { id: 'x', response: { clientDataJSON: buf(1) } } });
  const r5c = await m.w.registrar();
  resultados.registrarViejo = { ok: r5c.ok, error: r5c.error, posteos: m.llamadas.fetch.length };
  m = crearMundo({ respuestas: { '/passkeys/registro/opciones': { ok: false, error: 'Ya tienes 10 dispositivos' } } });
  resultados.registrarSinOpciones = (await m.w.registrar()).error;
  // 6. entrar
  const opcLogin = { ok: true, publicKey: { challenge: b64(buf(4, 5, 6)), rpId: 'ilus.test', timeout: 60000, userVerification: 'required' } };
  const aserc = { id: 'CRED-1', response: { clientDataJSON: buf(1), authenticatorData: buf(2), signature: buf(3), userHandle: buf(55) } };
  m = crearMundo({ local: { ilus_pk_cred: b64(buf(7, 7, 7)) }, respuestas: { '/passkeys/login/opciones': opcLogin, '/passkeys/login/verificar': { ok: true, redirect: '/mi-cuenta' } }, asercion: aserc });
  const r6 = await m.w.entrar('/mi-cuenta');
  const pedido = m.llamadas.get[0].publicKey, post = m.llamadas.fetch.find(f => f.url.endsWith('/login/verificar')).cuerpo;
  resultados.entrar = { ok: r6.ok, redirect: r6.redirect, permitidas: pedido.allowCredentials.length, tipoId: pedido.allowCredentials[0].id.constructor.name, post };
  m = crearMundo({ respuestas: { '/passkeys/login/opciones': opcLogin, '/passkeys/login/verificar': { ok: true, redirect: '/' } }, asercion: { id: 'C', response: { clientDataJSON: buf(1), authenticatorData: buf(2), signature: buf(3), userHandle: null } } });
  await m.w.entrar();
  resultados.entrarSinGuardada = { permitidas: m.llamadas.get[0].publicKey.allowCredentials.length, userHandle: m.llamadas.fetch.find(f => f.url.endsWith('/login/verificar')).cuerpo.userHandle, next: m.llamadas.fetch.find(f => f.url.endsWith('/login/verificar')).cuerpo.next };
  m = crearMundo({ respuestas: { '/passkeys/login/opciones': opcLogin }, getFalla: { name: 'NotAllowedError' } });
  const r6c = await m.w.entrar();
  resultados.entrarCancelado = { ok: r6c.ok, cancelado: r6c.cancelado, posteos: m.llamadas.fetch.filter(f => f.url.endsWith('/verificar')).length };
  // 7. la pregunta amable (se ejecuta sola al cargar el script)
  async function pregunta(cfg) { const mm = crearMundo(Object.assign({ mediaCelular: true, uid: '7' }, cfg)); await new Promise(r => setImmediate(r)); await new Promise(r => setImmediate(r)); return mm; }
  const casos = {
    celular: {}, escritorio: { mediaCelular: false, ua: uas.windowsEdge }, sinSesion: { uid: '0' }, yaActivado: { local: { ilus_pk_ok: '1' } },
    treintaNo: { local: { ilus_pk_no: '3' } }, hacePoco: { local: { ilus_pk_tarde: String(Date.now() + 86400000) } }, enMiCuenta: { elementos: { pkCard: {} } },
    enLogin: { elementos: { pkLoginBtn: { addEventListener() {}, getAttribute: () => '/' } } }, modalAbierto: { modalAbierto: true }, sinSoporte: { plataforma: false }, sinMeta: { sinMeta: true }
  };
  resultados.pregunta = {};
  for (const k of Object.keys(casos)) { const mm = await pregunta(casos[k]); resultados.pregunta[k] = mm.llamadas.confirm.length; }
  let mm = await pregunta({ respondeConfirm: false });
  await new Promise(r => setImmediate(r));
  resultados.ahoraNo = { no: mm.guardado.ilus_pk_no, diasDespues: Math.round((Number(mm.guardado.ilus_pk_tarde) - Date.now()) / 86400000), texto: mm.llamadas.confirm[0] && { t: mm.llamadas.confirm[0].title, ok: mm.llamadas.confirm[0].okLabel, no: mm.llamadas.confirm[0].cancelLabel } };
  mm = await pregunta({ respondeConfirm: true, respuestas: { '/passkeys/registro/opciones': { ok: false, error: 'x' } } });
  await new Promise(r => setImmediate(r)); await new Promise(r => setImmediate(r));
  resultados.activar = { pidioOpciones: mm.llamadas.fetch.some(f => f.url.endsWith('/registro/opciones')), tostadas: mm.llamadas.toasts.length,
    opcionesPedidas: mm.llamadas.fetch.filter(f => f.url.endsWith('/registro/opciones')).length };
  // 8. Safari (iPhone) pierde el gesto si hay una espera de red entre el toque y el teléfono: el desafío se pide ANTES del toque y se reutiliza
  const btnLogin = { hidden: true, disabled: false, classList: { add() {} }, addEventListener() {}, getAttribute: () => '/' }, msgLogin = { textContent: '', className: '' };
  m = crearMundo({ elementos: { pkLoginBtn: btnLogin, pkLoginMsg: msgLogin }, respuestas: { '/passkeys/login/opciones': opcLogin, '/passkeys/login/verificar': { ok: true, redirect: '/' } }, asercion: aserc });
  await new Promise(r => setImmediate(r)); await new Promise(r => setImmediate(r));
  const antes = m.llamadas.fetch.filter(f => f.url.endsWith('/login/opciones')).length;
  await m.w.entrar('/');
  const despues = m.llamadas.fetch.filter(f => f.url.endsWith('/login/opciones')).length;
  await m.w.entrar('/');
  resultados.anticipado = { botonVisible: btnLogin.hidden === false, antesDelToque: antes, trasUnIntento: despues, trasOtroIntento: m.llamadas.fetch.filter(f => f.url.endsWith('/login/opciones')).length };
  // 9. Face ID se pide SOLO al abrir donde la persona ya lo activó; el enlace queda de respaldo (Daniel: «más directo… el botón más sutil»)
  const reposo = async () => { for (let i = 0; i < 8; i++) await new Promise(r => setImmediate(r)); };
  function mundoLogin(extra) {
    const btn = { hidden: true, disabled: false, classList: { add() {} }, getAttribute: () => '/', addEventListener(ev, fn) { if (ev === 'click') this._click = fn; } };
    const msg = { textContent: '', className: '' };
    const mm = crearMundo(Object.assign({ elementos: { pkLoginBtn: btn, pkLoginMsg: msg },
      respuestas: { '/passkeys/login/opciones': opcLogin, '/passkeys/login/verificar': { ok: true, redirect: '/inicio' } }, asercion: aserc,
      local: { ilus_pk_ok: '1', ilus_pk_cred: b64(buf(7, 7, 7)) } }, extra || {}));
    return { mm, btn, msg };
  }
  let L = mundoLogin(); await reposo();
  resultados.auto = { gets: L.mm.llamadas.get.length, destino: L.mm.window.location.href, msg: L.msg.textContent, enlaceVisible: L.btn.hidden === false };
  resultados.noAuto = {};
  const noCasos = { sinActivar: { local: {} }, cerroSesion: { sesion: { ilus_pk_noauto: '1' } }, correoEscrito: { usuario: { value: 'a@b.cl' } }, enSegundoPlano: { enSegundoPlano: true }, sinSoporte: { plataforma: false } };
  for (const k of Object.keys(noCasos)) {
    const x = mundoLogin(noCasos[k]); await reposo();
    resultados.noAuto[k] = { gets: x.mm.llamadas.get.length, enlaceVisible: x.btn.hidden === false, marcaDeCierre: x.mm.sesion.ilus_pk_noauto === undefined ? 'borrada' : 'sigue' };
  }
  L = mundoLogin({ getFalla: { name: 'NotAllowedError' } }); await reposo();
  const trasAuto = { gets: L.mm.llamadas.get.length, msg: L.msg.textContent, botonActivo: !L.btn.disabled, opciones: L.mm.llamadas.fetch.filter(f => f.url.endsWith('/login/opciones')).length };
  await L.btn._click(); await reposo();
  resultados.autoCancelado = { trasAuto, trasToque: { gets: L.mm.llamadas.get.length, msg: L.msg.textContent, clase: L.msg.className } };
  L = mundoLogin({ respuestas: { '/passkeys/login/opciones': opcLogin, '/passkeys/login/verificar': { ok: false, error: 'Este dispositivo no tiene Face ID activado en ILUS.', error_codigo: 'DESCONOCIDA', status: 401 } } }); await reposo();
  resultados.desconocida = { local: L.mm.guardado, msg: L.msg.textContent, clase: L.msg.className };
  L = mundoLogin({ local: {} }); await reposo();
  await L.btn._click(); await reposo();
  resultados.toqueSinActivar = { gets: L.mm.llamadas.get.length, destino: L.mm.window.location.href };
  L = mundoLogin({ getColgado: true }); await reposo();
  await L.btn._click(); await L.btn._click(); await reposo();
  resultados.unIntentoALaVez = { gets: L.mm.llamadas.get.length, msg: L.msg.textContent, botonDeshabilitado: L.btn.disabled };
  L = mundoLogin(); await reposo();
  const manejadores = L.mm.llamadas.handlers.submit || [];
  resultados.cierreDeSesion = { hayManejador: manejadores.length > 0 };
  const m2 = crearMundo({ local: {} });
  (m2.llamadas.handlers.submit || []).forEach(h => h({ target: { getAttribute: () => '/otra-ruta' } }));
  const sinMarca = m2.sesion.ilus_pk_noauto === undefined;
  (m2.llamadas.handlers.submit || []).forEach(h => h({ target: { getAttribute: () => '/logout' } }));
  resultados.cierreDeSesion.otraRuta = sinMarca; resultados.cierreDeSesion.logout = m2.sesion.ilus_pk_noauto;
  console.log(JSON.stringify(resultados));
})();
'''


@unittest.skipUnless(HAY_NODE, "hace falta Node para ejecutar el JavaScript")
class TestEjecutado(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ruta = os.path.join(tempfile.mkdtemp(prefix="pk_front_"), "prueba.js")
        with open(ruta, "w", encoding="utf-8") as f:
            f.write(_PRUEBA_JS)
        p = subprocess.run(["node", ruta, os.path.join(RAIZ, "static", "ilus_passkeys.js")], capture_output=True, text=True, encoding="utf-8", timeout=60)
        if p.returncode != 0:
            raise AssertionError("Node falló: " + p.stderr[-2000:])
        cls.r = json.loads(p.stdout.strip().splitlines()[-1])

    def test_base64url_coincide_con_la_referencia_y_vuelve_igual(self):
        b = self.r["b64"]
        self.assertEqual(b["enc"], b["ref"])
        self.assertEqual(b["vuelta"], [0, 1, 250, 251, 252, 253, 254, 255, 62, 63])
        self.assertTrue(b["sinRelleno"])

    def test_cada_dispositivo_se_reconoce_en_la_lista(self):
        e = self.r["etiquetas"]
        self.assertEqual((e["iphoneSafari"], e["androidChrome"], e["windowsEdge"], e["macChrome"], e["iphoneInstalada"]),
                         ("iPhone · Safari", "Android · Chrome", "Windows · Edge", "Mac · Chrome", "iPhone · app instalada"))

    def test_los_errores_del_telefono_se_explican_con_calma(self):
        e = self.r["errores"]
        self.assertIn("Se canceló", e[0])
        self.assertEqual(e[0], e[1])
        self.assertIn("ya estaba activado", e[2])
        self.assertIn("no permite", e[3])
        self.assertIn("dirección oficial", e[4])
        self.assertEqual(e[5], "x")

    def test_solo_se_ofrece_donde_el_telefono_puede(self):
        self.assertEqual(self.r["soporta"], [True, False, False, False, False])

    def test_registrar_manda_los_datos_que_el_servidor_espera(self):
        r = self.r["registrar"]
        self.assertTrue(r["ok"])
        self.assertEqual(r["tiposDeCreate"], ["ArrayBuffer", "ArrayBuffer", "ArrayBuffer"])          # el teléfono recibe bytes, no texto
        self.assertEqual(r["bytesDelDesafio"], [1, 2, 3])
        v = r["verificar"]
        self.assertEqual(sorted(v), sorted(["id", "clientDataJSON", "authenticatorData", "publicKey", "publicKeyAlgorithm", "transports", "etiqueta"]))
        self.assertEqual((v["id"], v["publicKeyAlgorithm"], v["transports"], v["etiqueta"]), ("CRED-1", -7, ["internal"], "iPhone · Safari"))
        self.assertEqual((v["clientDataJSON"], v["authenticatorData"], v["publicKey"]), ("Cgs", "Hh8", "FBUW"))      # 10,11 · 30,31 · 20,21,22 en base64url
        self.assertEqual(r["local"], {"ilus_pk_cred": "CRED-1", "ilus_pk_ok": "1"})                              # recuerda ESTE dispositivo

    def test_si_cancela_o_el_navegador_es_viejo_no_se_guarda_nada(self):
        c = self.r["registrarCancelado"]
        self.assertEqual((c["ok"], c["cancelado"], c["local"]), (False, True, {}))
        self.assertIn("Se canceló", c["error"])
        v = self.r["registrarViejo"]
        self.assertFalse(v["ok"])
        self.assertIn("Actualiza tu teléfono", v["error"])
        self.assertEqual(v["posteos"], 1)                                                           # solo pidió las opciones; no mandó nada raro
        self.assertEqual(self.r["registrarSinOpciones"], "Ya tienes 10 dispositivos")

    def test_entrar_usa_la_credencial_de_este_telefono_y_manda_la_firma(self):
        e = self.r["entrar"]
        self.assertEqual((e["ok"], e["redirect"], e["permitidas"], e["tipoId"]), (True, "/mi-cuenta", 1, "ArrayBuffer"))
        self.assertEqual(sorted(e["post"]), sorted(["id", "clientDataJSON", "authenticatorData", "signature", "userHandle", "next"]))
        self.assertEqual((e["post"]["next"], e["post"]["userHandle"], e["post"]["signature"]), ("/mi-cuenta", "Nw", "Aw"))
        s = self.r["entrarSinGuardada"]
        self.assertEqual((s["permitidas"], s["userHandle"], s["next"]), (0, None, "/"))             # sin credencial guardada, el teléfono elige entre las suyas
        c = self.r["entrarCancelado"]
        self.assertEqual((c["ok"], c["cancelado"], c["posteos"]), (False, True, 0))

    def test_la_pregunta_sale_solo_en_el_celular_y_a_quien_corresponde(self):
        p = self.r["pregunta"]
        self.assertEqual(p["celular"], 1)
        for k in ("escritorio", "sinSesion", "yaActivado", "treintaNo", "hacePoco", "enMiCuenta", "enLogin", "modalAbierto", "sinSoporte", "sinMeta"):
            self.assertEqual(p[k], 0, k)

    def test_ahora_no_la_vuelve_a_preguntar_en_14_dias_y_activar_empieza_el_registro(self):
        a = self.r["ahoraNo"]
        self.assertEqual((a["no"], a["diasDespues"]), ("1", 14))
        self.assertEqual(a["texto"], {"t": "¿Entrar con Face ID?", "ok": "Activar", "no": "Ahora no"})
        self.assertTrue(self.r["activar"]["pidioOpciones"])

    def test_si_ya_lo_activo_en_este_telefono_se_pide_solo_al_abrir(self):
        a = self.r["auto"]
        self.assertEqual((a["gets"], a["destino"], a["msg"]), (1, "/inicio", "Listo. Entrando…"))
        self.assertTrue(a["enlaceVisible"])                                           # el enlace chico queda de respaldo

    def test_no_se_pide_solo_cuando_no_corresponde(self):
        for k in ("sinActivar", "cerroSesion", "correoEscrito", "enSegundoPlano"):
            n = self.r["noAuto"][k]
            self.assertEqual(n["gets"], 0, k)
            self.assertTrue(n["enlaceVisible"], k)                                    # pero el enlace sigue disponible
        self.assertFalse(self.r["noAuto"]["sinSoporte"]["enlaceVisible"])             # teléfono que no puede: ni enlace
        self.assertEqual(self.r["noAuto"]["sinSoporte"]["gets"], 0)
        self.assertEqual(self.r["noAuto"]["cerroSesion"]["marcaDeCierre"], "borrada")  # la marca vale UNA vez: la siguiente visita vuelve a pedirlo

    def test_si_lo_cierra_o_el_telefono_pide_un_toque_no_insiste_ni_reclama(self):
        c = self.r["autoCancelado"]
        self.assertEqual(c["trasAuto"], {"gets": 1, "msg": "", "botonActivo": True, "opciones": 2})   # sin mensaje; ya pidió otro desafío para el toque
        self.assertEqual(c["trasToque"]["gets"], 2)                                   # el enlace sigue sirviendo
        self.assertIn("Se canceló", c["trasToque"]["msg"])                            # y con el toque sí se explica con calma

    def test_si_lo_quitaron_en_otro_lado_deja_de_pedirse_solo(self):
        d = self.r["desconocida"]
        self.assertEqual(d["local"], {})
        self.assertIn("no tiene Face ID activado", d["msg"])
        self.assertIn("pk-msg--mal", d["clase"])
        self.assertEqual(self.r["toqueSinActivar"], {"gets": 1, "destino": "/inicio"})  # sin haberlo activado aquí, el enlace igual funciona

    def test_un_solo_intento_a_la_vez_aunque_toquen_varias_veces(self):
        u = self.r["unIntentoALaVez"]
        self.assertEqual(u["gets"], 1)                                                # el automático sigue esperando: los toques no abren otro Face ID
        self.assertEqual(u["msg"], "Verificando con Face ID…")
        self.assertTrue(u["botonDeshabilitado"])

    def test_cerrar_sesion_a_proposito_marca_para_no_pedirlo_solo(self):
        c = self.r["cierreDeSesion"]
        self.assertTrue(c["hayManejador"])
        self.assertTrue(c["otraRuta"])
        self.assertEqual(c["logout"], "1")

    def test_el_desafio_se_pide_antes_del_toque_y_se_reutiliza(self):
        """Safari (iPhone) se niega si entre el toque y la llamada al teléfono hay una espera de red: el desafío ya tiene que estar pedido."""
        a = self.r["anticipado"]
        self.assertTrue(a["botonVisible"])
        self.assertEqual((a["antesDelToque"], a["trasUnIntento"]), (1, 1))      # se pidió al mostrar el botón; el toque no volvió a pedirlo
        self.assertEqual(a["trasOtroIntento"], 2)                               # el desafío de un intento ya se usó: el siguiente pide uno nuevo
        self.assertEqual(self.r["activar"]["opcionesPedidas"], 1)               # la pregunta lo pidió mientras se leía y «Activar» lo reutilizó


if __name__ == "__main__":
    unittest.main()

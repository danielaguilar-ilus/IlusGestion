/* Simulacion del wrapper de fetch con un DOM/servidor falsos, para probar
   el COMPORTAMIENTO real (no solo que el archivo parsee). */
const fs = require('fs');

const html = fs.readFileSync('templates/base.html', 'utf8');
const i = html.indexOf('window.__CSRF_TOKEN = meta');
const ini = html.lastIndexOf('<script>', i);
const fin = html.indexOf('</script>', i);
const JS = html.slice(ini + '<script>'.length, fin);

function escenario(nombre, cfg) {
  const llamadas = [];
  let tokenServidor = cfg.tokenServidor;

  const doc = {
    querySelector: (s) => {
      if (s.includes('csrf-token')) return { getAttribute: () => 'T1', setAttribute: () => {} };
      if (s.includes('ilus-uid')) return { getAttribute: () => String(cfg.uidPagina) };
      return null;
    },
    querySelectorAll: () => [],
  };

  const origFetch = (url, init) => {
    llamadas.push({ url: String(url), method: (init && init.method) || 'GET' });
    if (String(url) === '/api/csrf-token') {
      if (cfg.refrescoCae) return Promise.reject(new Error('network'));
      return Promise.resolve({ status: 200, json: () => Promise.resolve({
        ok: true, csrf_token: tokenServidor,
        autenticado: cfg.autenticado, uid: cfg.uidServidor }) });
    }
    const enviado = init && init.headers && init.headers['X-CSRF-Token'];
    if (enviado === tokenServidor) {
      return Promise.resolve({ status: 200, clone(){ return this; },
        json: () => Promise.resolve({ ok: true }) });
    }
    const cuerpo = cfg.sesionMuerta
      ? { error_codigo: 'SESSION_EXPIRED' }
      : { error_codigo: 'CSRF_INVALIDO' };
    const st = cfg.sesionMuerta ? 401 : 403;
    return Promise.resolve({ status: st, clone(){ return this; },
      json: () => Promise.resolve(cuerpo) });
  };

  const avisos = [];
  const sandbox = {
    window: { fetch: origFetch },
    document: doc,
    location: { assign: (u) => avisos.push('NAVEGA:' + u) },
    setTimeout: (fn) => fn(),
    ilusAlert: (o) => { avisos.push('ALERT:' + o.title); return Promise.resolve(true); },
    ilusToast: (m) => avisos.push('TOAST'),
    console,
  };
  /* El navegador define Headers; sin el, `init.headers instanceof Headers`
     lanza ReferenceError y el try/catch del wrapper se traga la inyeccion. */
  sandbox.Headers = class Headers {};
  sandbox.globalThis = sandbox;
  const vm = require('vm');
  vm.createContext(sandbox);
  vm.runInContext(JS, sandbox);

  return sandbox.window.fetch('/api/asignar/cotizar-couriers',
      { method: 'POST', headers: { 'Content-Type': 'application/json' } })
    .then(r => ({ nombre, status: r.status, llamadas, avisos }))
    .catch(e => ({ nombre, error: e.message, llamadas, avisos }));
}

const casos = [
  ['Token rotado, MISMO usuario -> debe auto-repararse',
   { tokenServidor: 'T2', autenticado: true, uidPagina: 7, uidServidor: 7 },
   (r) => r.status === 200 && r.llamadas.length === 3],

  ['Sesion expirada (el caso REAL de Daniel) -> avisa, NO reintenta',
   { tokenServidor: 'T2', autenticado: false, uidPagina: 7, uidServidor: 0, sesionMuerta: true },
   (r) => r.status === 401 && r.llamadas.length === 1],

  ['Otro usuario entro en el navegador -> NO ejecuta la accion',
   { tokenServidor: 'T2', autenticado: true, uidPagina: 7, uidServidor: 99 },
   (r) => r.status === 403 && r.avisos.some(a => a.includes('Cambio de usuario'))
          && r.llamadas.length === 2],

  ['Se cae la red al refrescar -> devuelve el 403, sin inventar sesion expirada',
   { tokenServidor: 'T2', autenticado: true, uidPagina: 7, uidServidor: 7, refrescoCae: true },
   (r) => r.status === 403 && !r.avisos.some(a => a.includes('Sesion expirada'))],

  ['Todo normal (token vigente) -> una sola llamada, sin overhead',
   { tokenServidor: 'T1', autenticado: true, uidPagina: 7, uidServidor: 7 },
   (r) => r.status === 200 && r.llamadas.length === 1],
];

(async () => {
  let fallos = 0;
  for (const [nombre, cfg, ok] of casos) {
    const r = await escenario(nombre, cfg);
    const pass = ok(r);
    if (!pass) fallos++;
    console.log(`${pass ? 'OK  ' : 'FALLA'}  ${nombre}`);
    if (!pass) console.log('        ->', JSON.stringify({ status: r.status, llamadas: r.llamadas.map(c => c.url), avisos: r.avisos }));
  }
  console.log(fallos ? `\n${fallos} escenario(s) fallaron` : '\nTodos los escenarios OK');
  process.exit(fallos ? 1 : 0);
})();

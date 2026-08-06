'use strict';
/*
 * Conexión del separador de calle/número con la pantalla "Asignar y Cotizar".
 *
 * Shipit exige `street` y `number` SEPARADOS; ILUS guarda la dirección como un
 * solo texto. Quien separa es shipit_client.split_street_number() (Python, ya
 * probado). Acá se verifica la OTRA mitad: que la pantalla use ese resultado,
 * que BLOQUEE cuando no se puede separar con confianza, que el mensaje sea
 * ESPECÍFICO (Daniel: "no vayas a mandar algo genérico, algo específico") y
 * que, si el endpoint todavía no está desplegado, no se rompa nada.
 *
 * Las respuestas del fetch NO son inventadas: las calcula el propio
 * shipit_client.clasificar_direccion() y las inyecta
 * tests/test_shipit_operador_seleccion.py como archivo JSON (argv[2]). Así la
 * prueba cruza de verdad el límite Python↔JS.
 *
 * Uso:  node tests/test_shipit_direccion_ui.js <fixtures.json>
 */

const assert = require('assert');
const fs = require('fs');
const path = require('path');
const vm = require('vm');

const RAIZ = path.resolve(__dirname, '..');
const FIXTURES = JSON.parse(fs.readFileSync(process.argv[2], 'utf8'));

// ── DOM mínimo, con los elementos que el modal toca de verdad ───────────
const toasts = [];
const elementos = {};
function el(id) {
  if (!elementos[id]) {
    elementos[id] = {
      id, value: '', textContent: '', innerHTML: '',
      style: {}, dataset: {},
      classList: { add() {}, remove() {} },
      addEventListener() {}, focus() {}, select() {}, scrollIntoView() {},
      querySelector() { return null; }, querySelectorAll() { return []; },
    };
  }
  return elementos[id];
}
['cli-dir', 'modalShipitDir', 'sdTitulo', 'sdDireccionOriginal', 'sdMotivo',
 'sdCalle', 'sdNumero', 'sdAlert', 'sdPreview'].forEach(el);

const document = {
  readyState: 'complete',
  addEventListener() {},
  getElementById(id) { return elementos[id] || null; },
  querySelector() { return null; },
  querySelectorAll() { return []; },
  createElement() { return el('creado'); },
  body: { appendChild() {} },
};

// ── fetch falso: cuenta llamadas y responde según el modo ───────────────
let modoFetch = 'ok';       // 'ok' | '404' | 'html' | 'boom'
let llamadas = 0;
function fakeFetch(url, opts) {
  llamadas += 1;
  const cuerpo = JSON.parse((opts && opts.body) || '{}');
  if (modoFetch === '404') {
    return Promise.resolve({
      ok: false, status: 404,
      headers: { get: () => 'text/html; charset=utf-8' },
      json: () => Promise.resolve({}),
    });
  }
  if (modoFetch === 'html') {   // proxy/login devolvió HTML en vez de JSON
    return Promise.resolve({
      ok: true, status: 200,
      headers: { get: () => 'text/html; charset=utf-8' },
      json: () => Promise.reject(new Error('no es JSON')),
    });
  }
  if (modoFetch === 'boom') return Promise.reject(new Error('red caída'));
  const fx = FIXTURES[cuerpo.direccion];
  assert.ok(fx, 'falta fixture para: ' + cuerpo.direccion);
  return Promise.resolve({
    ok: true, status: 200,
    headers: { get: () => 'application/json' },
    json: () => Promise.resolve(Object.assign({ ok: true }, fx)),
  });
}

const window = {
  document,
  ilusToast(mensaje, opts) { toasts.push({ mensaje, opts: opts || {} }); },
  addEventListener() {},
};

const contexto = vm.createContext({
  window, document, console, JSON, Math, Date,
  setTimeout(cb) { if (typeof cb === 'function') cb(); return 0; },
  clearTimeout() {},
  fetch: fakeFetch,
  ilusToast: window.ilusToast,
  navigator: { userAgent: 'node' },
  location: { href: 'http://test/asignar' },
  MutationObserver: function () { return { observe() {}, disconnect() {}, takeRecords() { return []; } }; },
  IntersectionObserver: function () { return { observe() {}, disconnect() {}, unobserve() {} }; },
  requestAnimationFrame(cb) { if (typeof cb === 'function') cb(0); return 0; },
});

vm.runInContext(
  fs.readFileSync(path.join(RAIZ, 'static', 'cubicador_asignar.js'), 'utf8'),
  contexto, { filename: 'cubicador_asignar.js' });

const ev = (codigo) => vm.runInContext(codigo, contexto);
function set(nombre, valor) { contexto.__tmp = valor; ev(`${nombre} = __tmp;`); }
function get(nombre) { return ev(`(${nombre})`); }

// _shipitAsegurarCalleNumero() abre el modal DESPUÉS de esperar al fetch, así
// que hay que dejar correr las microtareas antes de mirar el DOM.
const tick = () => new Promise(r => setImmediate(r));

function reset(direccion) {
  toasts.length = 0;
  llamadas = 0;
  modoFetch = 'ok';
  el('cli-dir').value = direccion;
  el('sdCalle').value = '';
  el('sdNumero').value = '';
  el('modalShipitDir').style = {};
  set('_shipitDirEstado', null);
  set('_shipitDirManual', null);
  set('_shipitDirEndpointVivo', null);
  ev('_shipitDirCache.clear();');
}

(async function main() {

  // ══════════════════════════════════════════════════════════════════════
  // 1) DIRECCIÓN LIMPIA — pasa sin molestar y deja calle/número listos
  // ══════════════════════════════════════════════════════════════════════
  {
    reset('Colon 1265');
    const ok = await get('_shipitAsegurarCalleNumero')();
    assert.strictEqual(ok, true, 'una dirección limpia no debe bloquear');
    const man = get('_shipitDirManual');
    assert.strictEqual(man.calle, 'Colon');
    assert.strictEqual(man.numero, '1265');
    assert.strictEqual(el('modalShipitDir').style.display, undefined,
      'no debe abrirse el modal si no hace falta');
    console.log('  ok 1/6 · dirección limpia');
  }

  // ══════════════════════════════════════════════════════════════════════
  // 2) EL CASO PELIGROSO — "Los Aromos 145 depto 402"
  //    Tomar 402 (el depto) genera una guía VÁLIDA a la dirección
  //    EQUIVOCADA. El modal debe venir precargado con 145, nunca con 402.
  // ══════════════════════════════════════════════════════════════════════
  {
    reset('Los Aromos 145 depto 402');
    const promesa = get('_shipitAsegurarCalleNumero')();
    await tick();
    // El modal quedó abierto esperando confirmación humana.
    assert.strictEqual(el('modalShipitDir').style.display, 'flex',
      'con aviso pendiente debe pedirse confirmación');
    assert.strictEqual(el('sdNumero').value, '145',
      'debe proponer el número de la CALLE, no el del departamento');
    assert.notStrictEqual(el('sdNumero').value, '402');
    assert.strictEqual(el('sdCalle').value, 'Los Aromos');
    // El motivo es específico, no un "dirección inválida" pelado.
    assert.ok(/depto 402/.test(el('sdMotivo').innerHTML),
      'el motivo debe citar el texto sobrante real: ' + el('sdMotivo').innerHTML);

    get('confirmarModalShipitDir')();
    assert.strictEqual(await promesa, true, 'confirmar debe dejar seguir');
    assert.strictEqual(get('_shipitDirManual').numero, '145');
    console.log('  ok 2/6 · "depto 402" no se confunde con el número de la calle');
  }

  // ══════════════════════════════════════════════════════════════════════
  // 3) BLOQUEADA (s/n) — no se adivina: se pide completar, y cancelar
  //    IMPIDE asignar
  // ══════════════════════════════════════════════════════════════════════
  {
    reset('Camino a Melipilla s/n');
    const promesa = get('_shipitAsegurarCalleNumero')();
    await tick();
    assert.strictEqual(el('modalShipitDir').style.display, 'flex');
    assert.ok(/no puede leer el número/i.test(el('sdTitulo').textContent),
      'el título debe decir qué pasa: ' + el('sdTitulo').textContent);
    assert.ok(/s\/n/.test(el('sdMotivo').innerHTML),
      'el motivo debe nombrar el "s/n" concreto');
    assert.ok(/Camino a Melipilla/.test(el('sdDireccionOriginal').textContent));

    get('cerrarModalShipitDir')();
    assert.strictEqual(await promesa, false,
      'cancelar debe impedir el envío, no dejar pasar en silencio');
    assert.strictEqual(get('_shipitDirManual'), null);
    console.log('  ok 3/6 · s/n bloquea y cancelar no deja asignar');
  }

  // ══════════════════════════════════════════════════════════════════════
  // 4) VALIDACIÓN DEL FORMULARIO — no acepta campos vacíos ni un "número"
  //    sin dígitos (mandar "s/n" como número es exactamente el error que
  //    se quiere evitar)
  // ══════════════════════════════════════════════════════════════════════
  {
    reset('Camino a Melipilla s/n');
    const promesa = get('_shipitAsegurarCalleNumero')();
    await tick();
    el('sdCalle').value = 'Camino a Melipilla';
    el('sdNumero').value = '';
    get('confirmarModalShipitDir')();
    assert.strictEqual(el('sdAlert').style.display, '',
      'debe avisar que falta el número');
    assert.ok(/número/i.test(el('sdAlert').textContent));

    el('sdNumero').value = 's/n';
    get('confirmarModalShipitDir')();
    assert.ok(/dígito/i.test(el('sdAlert').textContent),
      'un número sin dígitos debe rechazarse: ' + el('sdAlert').textContent);

    el('sdNumero').value = '1450';
    get('confirmarModalShipitDir')();
    assert.strictEqual(await promesa, true);
    assert.strictEqual(get('_shipitDirManual').numero, '1450');
    assert.strictEqual(el('modalShipitDir').style.display, 'none');
    console.log('  ok 4/6 · el formulario no acepta basura');
  }

  // ══════════════════════════════════════════════════════════════════════
  // 5) BACKEND SIN EL ENDPOINT TODAVÍA — la pantalla NO se rompe
  //    (el frontend se despliega antes que el parche de app.py)
  // ══════════════════════════════════════════════════════════════════════
  {
    reset('Camino a Melipilla s/n');
    modoFetch = '404';
    const ok = await get('_shipitAsegurarCalleNumero')();
    assert.strictEqual(ok, true, 'sin endpoint no se puede bloquear el despacho');
    assert.strictEqual(el('modalShipitDir').style.display, undefined,
      'sin endpoint no se abre ningún modal');
    assert.strictEqual(get('_shipitDirEndpointVivo'), false);

    // Y no se insiste en cada cotización (una sola llamada perdida, no N).
    const antes = llamadas;
    await get('_shipitAsegurarCalleNumero')();
    await get('_shipitRevisarDireccionEnFondo')();
    assert.strictEqual(llamadas, antes,
      'tras un 404 no debe volver a preguntar');

    // Mismo trato para HTML inesperado y para caída de red.
    reset('Colon 1265'); modoFetch = 'html';
    assert.strictEqual(await get('_shipitAsegurarCalleNumero')(), true);
    reset('Colon 1265'); modoFetch = 'boom';
    assert.strictEqual(await get('_shipitAsegurarCalleNumero')(), true);
    console.log('  ok 5/6 · degrada sin romper si el backend aún no tiene el endpoint');
  }

  // ══════════════════════════════════════════════════════════════════════
  // 6) CHIP EN LA TARJETA — adelanta el problema al cotizar
  // ══════════════════════════════════════════════════════════════════════
  {
    reset('Camino a Melipilla s/n');
    set('_cotizaciones', [{
      courier_id: 77, courier_nombre: 'Shipit', tiene_cobertura: true,
      fuente: 'api_shipit', precio: 4990,
      operadores_shipit: [{ operador: 'Starken', precio: 4990, dias: 2, es_mas_barato: true }],
      trace: { advertencias: [] },
    }]);
    await get('_shipitRevisarDireccionEnFondo')();
    let chip = get('_shipitDirChipHtml')();
    assert.ok(/Falta el número/.test(chip),
      'la tarjeta debe avisar antes de llegar al botón de enviar: ' + chip);

    // Ya corregida a mano → el chip cambia a verde.
    set('_shipitDirManual', { direccion: 'Camino a Melipilla s/n', calle: 'Camino a Melipilla', numero: '1450' });
    chip = get('_shipitDirChipHtml')();
    assert.ok(/Dirección completada/.test(chip), chip);

    // Si el operador cambia la dirección, la corrección vieja NO vale.
    el('cli-dir').value = 'Otra calle 10';
    set('_shipitDirEstado', null);
    assert.strictEqual(get('_shipitDirChipHtml')(), '',
      'una corrección de otra dirección no puede seguir mostrándose como válida');
    console.log('  ok 6/6 · el chip de la tarjeta refleja el estado real');
  }

  console.log('Shipit · separador de calle/número OK');
})().catch(e => { console.error(e); process.exit(1); });

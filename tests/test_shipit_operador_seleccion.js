'use strict';
/*
 * Selección del OPERADOR de Shipit en el comparador "Asignar y Cotizar".
 *
 * Shipit es un agregador: por dentro cotiza Starken, Chilexpress, Blue
 * Express... Hasta el 2026-08-05 el backend tomaba `disponibles[0]` (el más
 * barato) y no había forma de cambiarlo. Daniel, textual: "siento que me está
 * obligando a escoger Global Tracking, solamente porque es más barato".
 *
 * Estas pruebas cargan el archivo REAL (static/cubicador_asignar.js) dentro de
 * un vm con un DOM mínimo y verifican los cuatro escenarios que importan:
 *   1. varios operadores disponibles  → se puede elegir uno distinto al barato
 *   2. un solo operador               → sigue funcionando, queda ese
 *   3. ninguno disponible             → no revienta, no queda nada elegido
 *   4. el elegido desaparece al recotizar → vuelve al más barato Y AVISA
 *
 * Lo corre tests/test_shipit_operador_seleccion.py (unittest) vía node.
 * Directo:  node tests/test_shipit_operador_seleccion.js
 */

const assert = require('assert');
const fs = require('fs');
const path = require('path');
const vm = require('vm');

const RAIZ = path.resolve(__dirname, '..');

// ── DOM mínimo ──────────────────────────────────────────────────────────
// Todo devuelve null/[] a propósito: las funciones bajo prueba son de
// cálculo, y renderCouriers() corta apenas no encuentra #courierList.
const toasts = [];
const elementos = {};

function nuevoElemento(id) {
  return {
    id,
    value: '',
    textContent: '',
    innerHTML: '',
    style: {},
    dataset: {},
    classList: { add() {}, remove() {}, toggle() {} },
    addEventListener() {},
    focus() {},
    select() {},
    scrollIntoView() {},
    querySelector() { return null; },
    querySelectorAll() { return []; },
  };
}

const document = {
  readyState: 'complete',
  addEventListener() {},
  getElementById(id) { return elementos[id] || null; },
  querySelector() { return null; },
  querySelectorAll() { return []; },
  createElement() { return nuevoElemento('creado'); },
  body: { appendChild() {} },
};

const window = {
  document,
  ilusToast(mensaje, opts) { toasts.push({ mensaje, opts: opts || {} }); },
  addEventListener() {},
};

const contexto = vm.createContext({
  window,
  document,
  console,
  JSON,
  Math,
  Date,
  setTimeout() { return 0; },
  clearTimeout() {},
  fetch() { return Promise.reject(new Error('sin red en las pruebas')); },
  ilusToast: window.ilusToast,
  navigator: { userAgent: 'node' },
  location: { href: 'http://test/asignar' },
  // El archivo registra un MutationObserver al final (stepper del flujo).
  MutationObserver: function () {
    return { observe() {}, disconnect() {}, takeRecords() { return []; } };
  },
  requestAnimationFrame(cb) { if (typeof cb === 'function') cb(0); return 0; },
  IntersectionObserver: function () {
    return { observe() {}, disconnect() {}, unobserve() {} };
  },
});

const fuente = fs.readFileSync(path.join(RAIZ, 'static', 'cubicador_asignar.js'), 'utf8');
vm.runInContext(fuente, contexto, { filename: 'cubicador_asignar.js' });

// Puente hacia los `let` del script (viven en el ámbito léxico global del
// contexto, compartido entre scripts del mismo realm).
const ev = (codigo) => vm.runInContext(codigo, contexto);
function set(nombre, valor) {
  contexto.__tmp = valor;
  ev(`${nombre} = __tmp;`);
}
function get(nombre) {
  return ev(`(${nombre})`);
}

// ── Fixtures ────────────────────────────────────────────────────────────
const ID_SHIPIT = 77;

function cotShipit(operadores) {
  const ops = operadores.map((o, i) => ({
    operador: o.nombre,
    operador_display: o.nombre + ' (vía Shipit)',
    servicio: o.servicio || 'Normal',
    precio: o.precio,
    dias: o.dias === undefined ? null : o.dias,
    es_mas_barato: i === 0,
  }));
  return {
    courier_id: ID_SHIPIT,
    courier_nombre: 'Shipit',
    tiene_cobertura: true,
    fuente: 'api_shipit',
    precio: ops.length ? ops[0].precio : null,
    servicio: 'Shipit',
    tiempo_transito: '—',
    operadores_shipit: ops,
    trace: { advertencias: [] },
  };
}

function shipitSinCobertura(mensaje) {
  return {
    courier_id: ID_SHIPIT,
    courier_nombre: 'Shipit',
    tiene_cobertura: false,
    fuente: 'no_cobertura',
    mensaje: mensaje || 'Sin cobertura para esta comuna',
    trace: { advertencias: [] },
  };
}

function reset(cotizaciones) {
  toasts.length = 0;
  set('_shipitOpSel', null);
  set('_shipitOpSelNombre', '');
  set('_courierSel', null);
  set('_cotizaciones', cotizaciones);
}

// ════════════════════════════════════════════════════════════════════════
// 1) VARIOS OPERADORES — el más barato viene marcado, pero se puede cambiar
// ════════════════════════════════════════════════════════════════════════
{
  const c = cotShipit([
    { nombre: 'Global Tracking', precio: 3990, dias: 4 },
    { nombre: 'Starken',         precio: 5490, dias: 2 },
    { nombre: 'Chilexpress',     precio: 6990, dias: 1 },
  ]);
  reset([c]);

  // Default: el más barato (lo que hacía el backend). No se rompe nada.
  assert.strictEqual(get('_shipitOpActivo')(c).operador, 'Global Tracking',
    'por defecto debe quedar preseleccionado el más barato');
  assert.strictEqual(get('_precioEfectivo')(c), 3990);

  // La franja se pinta con TODOS los operadores y deja claro que son de Shipit.
  const html = get('_shipitDesgloseHtml')(c);
  assert.ok(html.indexOf('Starken') !== -1, 'debe listar a Starken');
  assert.ok(html.indexOf('Chilexpress') !== -1, 'debe listar a Chilexpress');
  assert.ok(/de Shipit/i.test(html), 'debe decir que los operadores son de Shipit');
  assert.ok(html.indexOf('setShipitOperador(77,0)') !== -1, 'cada fila debe ser clickeable');
  assert.ok(html.indexOf('role="radio"') !== -1, 'las filas deben ser opciones, no texto');
  assert.ok(/aria-checked="true"/.test(html), 'el activo debe quedar marcado');

  // Daniel elige Starken (más caro, pero llega antes).
  get('setShipitOperador')(ID_SHIPIT, 1);
  assert.strictEqual(get('_shipitOpActivo')(c).operador, 'Starken',
    'tras elegir Starken, Starken manda');
  assert.strictEqual(get('_precioEfectivo')(c), 5490,
    'el precio efectivo debe ser el del operador elegido, no el del más barato');

  // Y eso es lo que viaja al manifiesto.
  const sel = get('_courierSel');
  assert.strictEqual(sel.operador, 'Starken');
  assert.strictEqual(sel.precio, 5490, 'costo_cotizado sale de _courierSel.precio');
  assert.strictEqual(sel.operador_precio, 5490);
  assert.strictEqual(sel.operador_dias, 2);
  assert.strictEqual(sel.operador_manual, true, 'debe constar que lo eligió una persona');

  // Volver a marcar el más barato limpia la elección manual (vuelve al default).
  get('setShipitOperador')(ID_SHIPIT, 0);
  assert.strictEqual(get('_shipitOpSel'), null,
    'volver al más barato debe restaurar el comportamiento por defecto');
  assert.strictEqual(get('_courierSel').operador_manual, false);
  assert.strictEqual(get('_courierSel').precio, 3990);

  console.log('  ok 1/4 · varios operadores: el más barato es default, pero se puede cambiar');
}

// ════════════════════════════════════════════════════════════════════════
// 2) UN SOLO OPERADOR — no se rompe, y sigue siendo elegible
// ════════════════════════════════════════════════════════════════════════
{
  const c = cotShipit([{ nombre: 'Starken', precio: 4500, dias: 3 }]);
  reset([c]);

  assert.strictEqual(get('_shipitOpActivo')(c).operador, 'Starken');
  assert.strictEqual(get('_precioEfectivo')(c), 4500);
  const html = get('_shipitDesgloseHtml')(c);
  assert.ok(/la única opción/i.test(html),
    'con un solo operador el texto no debe decir "las 1 son"');

  get('setShipitOperador')(ID_SHIPIT, 0);
  assert.strictEqual(get('_courierSel').operador, 'Starken');
  assert.strictEqual(get('_courierSel').precio, 4500);

  console.log('  ok 2/4 · un solo operador');
}

// ════════════════════════════════════════════════════════════════════════
// 3) NINGUNO DISPONIBLE — no revienta y no deja nada elegido
// ════════════════════════════════════════════════════════════════════════
{
  const c = shipitSinCobertura('Shipit no acepta más de 15 kg por bulto.');
  reset([c]);

  assert.strictEqual(get('_shipitDesgloseHtml')(c), '',
    'sin operadores no se pinta la franja');
  assert.strictEqual(get('_shipitOpActivo')(c), null);
  assert.strictEqual(get('_precioEfectivo')(c), undefined,
    'sin cobertura no hay precio efectivo que inventar');

  // Un click imposible (índice que no existe) no debe dejar basura elegida.
  get('setShipitOperador')(ID_SHIPIT, 0);
  assert.strictEqual(get('_courierSel'), null);
  assert.strictEqual(get('_shipitOpSel'), null);

  // Y sigue reconociéndose como Shipit para la reconciliación.
  assert.strictEqual(get('_esShipit')(c), true);

  console.log('  ok 3/4 · ningún operador disponible');
}

// ════════════════════════════════════════════════════════════════════════
// 4) EL ELEGIDO DESAPARECE AL RECOTIZAR — vuelve al más barato Y AVISA
//    (el caso peligroso: si no se avisa, se despacha con otro operador y
//     nadie se entera)
// ════════════════════════════════════════════════════════════════════════
{
  const antes = cotShipit([
    { nombre: 'Global Tracking', precio: 3990, dias: 4 },
    { nombre: 'Starken',         precio: 5490, dias: 2 },
  ]);
  reset([antes]);
  get('setShipitOperador')(ID_SHIPIT, 1);        // elige Starken
  assert.strictEqual(get('_courierSel').operador, 'Starken');

  // Se cambia la comuna → recotiza → Starken ya no cubre.
  const despues = cotShipit([
    { nombre: 'Global Tracking', precio: 4290, dias: 4 },
    { nombre: 'Chilexpress',     precio: 7990, dias: 1 },
  ]);
  toasts.length = 0;
  set('_cotizaciones', [despues]);
  get('_resincronizarSeleccionCourier')();

  assert.strictEqual(get('_shipitOpSel'), null,
    'el operador que ya no existe no puede quedar elegido');
  assert.strictEqual(get('_shipitOpActivo')(despues).operador, 'Global Tracking',
    'debe caer al más barato disponible');
  assert.strictEqual(get('_courierSel').precio, 4290,
    'el precio de _courierSel debe re-sincronizarse (antes quedaba congelado)');
  assert.strictEqual(get('_courierSel').operador, 'Global Tracking');
  assert.strictEqual(get('_courierSel').operador_manual, false);

  assert.strictEqual(toasts.length, 1, 'debe avisar UNA vez, no en silencio');
  assert.ok(toasts[0].mensaje.indexOf('Starken') !== -1,
    'el aviso debe nombrar al operador que se cayó: ' + toasts[0].mensaje);
  assert.ok(toasts[0].mensaje.indexOf('Global Tracking') !== -1,
    'y decir con cuál quedó: ' + toasts[0].mensaje);
  assert.strictEqual(toasts[0].opts.type, 'warning');

  // 4b) Shipit entero se cae (>15 kg) estando elegido → se limpia la selección.
  reset([cotShipit([{ nombre: 'Starken', precio: 5490, dias: 2 }])]);
  get('setShipitOperador')(ID_SHIPIT, 0);
  toasts.length = 0;
  set('_cotizaciones', [shipitSinCobertura('Este envío pesa 18 kg — Shipit no acepta más de 15 kg por bulto.')]);
  get('_resincronizarSeleccionCourier')();
  assert.strictEqual(get('_courierSel'), null,
    'un courier sin cobertura no puede quedar seleccionado para asignar');
  assert.ok(toasts.some(t => /15 kg/.test(t.mensaje)),
    'el aviso debe traer el motivo REAL, no un texto genérico: ' +
    JSON.stringify(toasts.map(t => t.mensaje)));

  console.log('  ok 4/4 · el operador elegido deja de estar disponible');
}

// ════════════════════════════════════════════════════════════════════════
// 5) CONFIDENCIALIDAD — nada de Felca/Milling ni descuentos en el HTML
//    (los precios de esos dos derivan de FedEx menos un descuento interno;
//     ningún campo puede dejar rastro de eso en pantalla)
// ════════════════════════════════════════════════════════════════════════
{
  const c = cotShipit([{ nombre: 'Starken', precio: 4500, dias: 3 }]);
  const html = get('_shipitDesgloseHtml')(c);
  [/felca/i, /milling/i, /melling/i, /descuento/i, /regla_pct/i, /es_regla/i].forEach(rx => {
    assert.ok(!rx.test(html), 'la franja de Shipit no debe mencionar ' + rx);
  });
  console.log('  ok 5/5 · sin filtraciones en el HTML de la franja');
}

console.log('Shipit · selección de operador OK');

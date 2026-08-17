'use strict';
/*
 * Desglose de stock (físico / comprometido / devengado) al cotizar o retirar.
 *
 * Daniel, 2026-08-17, con captura del comparador "Asignar y Cotizar" real
 * (dos Gymleco marcados "Sin stock bod. 02" con "1 u. saldo 1" cada uno):
 * "creo que fui yo quien te pidió que solo tomaras el stock teórico [fisico
 * - comprometido - devengado], pero yo requiero mapear la información
 * completa... si hay al menos uno físico positivo y cuántos están
 * comprometidos, para tomar la decisión -- es decir, hay 1 y está
 * comprometido, yo veré si avanzo o no".
 *
 * Antes _tkaStockBadge/_rbaStockBadge colapsaban TODO a un booleano
 * (hay_stock = fisico - comprometido > 0) y mostraban el mismo "Sin stock
 * bod. 02" tanto si el físico era 0 como si era 1 (pero ya comprometido en
 * otro lado) -- exactamente el caso de la captura. Ahora se distinguen dos
 * estados ('sin' | 'comprometido') y el segundo muestra el desglose real a
 * la vista, no solo en el tooltip.
 *
 * Este archivo prueba las DOS copias -- son espejo exacto una de otra,
 * documentado así en el código:
 *   - templates/tickets/_tka_modal.html  (_tkaStockEstado/_tkaStockBadge)
 *   - static/retiros_internal_detail.js  (_rbaStockEstado/_rbaStockBadge)
 *
 * Lo corre tests/test_stock_fisico_comprometido.py (unittest) vía node.
 * Directo:  node tests/test_stock_fisico_comprometido.js
 */

const assert = require('assert');
const fs = require('fs');
const path = require('path');
const vm = require('vm');

const RAIZ = path.resolve(__dirname, '..');

function extraerScript(rutaHtml) {
  const html = fs.readFileSync(path.join(RAIZ, rutaHtml), 'utf8');
  const inicio = html.indexOf('<script>');
  const fin = html.lastIndexOf('</script>');
  assert.ok(inicio !== -1 && fin !== -1, `No encontré <script> en ${rutaHtml}`);
  return html.slice(inicio + '<script>'.length, fin);
}

// ── DOM mínimo (las funciones bajo prueba no tocan el DOM, pero el resto
//    del archivo se define igual y no debe reventar al cargar). ──────────
function nuevoElemento() {
  return {
    value: '', textContent: '', innerHTML: '', style: {}, dataset: {},
    classList: { add() {}, remove() {}, toggle() {} },
    addEventListener() {}, focus() {}, querySelector() { return null; },
    querySelectorAll() { return []; },
  };
}
function nuevoContexto() {
  const document = {
    readyState: 'complete',
    addEventListener() {},
    getElementById() { return nuevoElemento(); },
    querySelector() { return null; },
    querySelectorAll() { return []; },
    createElement() { return nuevoElemento(); },
    body: { appendChild() {} },
  };
  const window = { document, addEventListener() {} };
  return vm.createContext({
    window, document, console, JSON, Math,
    setTimeout() { return 0; }, clearTimeout() {},
    fetch() { return Promise.reject(new Error('sin red en las pruebas')); },
    ilusToast() {}, ilusAlert() {}, ilusConfirm() {}, ilusPrompt() {},
    navigator: { userAgent: 'node' },
    MutationObserver: function () { return { observe() {}, disconnect() {} }; },
    // Solo lo usa retiros_internal_detail.js (inyectado por el template
    // real vía Jinja) -- top-level, así que tiene que existir ANTES de
    // cargar el script o revienta al leerlo. _tka_modal.html no lo usa;
    // no está de más que exista igual.
    RETIROS_DETAIL_DATA: {
      reqId: 1, reqCode: 'RET-TEST-1', customerRut: '11111111-1',
      customerNameHtml: 'Cliente de prueba', customerLabel: 'Cliente de prueba',
      pickupDashboardUrl: '/retiros', shouldMountProposeCal: false, pesoInicial: 0,
    },
  });
}

const ctxTka = nuevoContexto();
vm.runInContext(extraerScript('templates/tickets/_tka_modal.html'), ctxTka, { filename: '_tka_modal.html' });

const ctxRba = nuevoContexto();
vm.runInContext(fs.readFileSync(path.join(RAIZ, 'static', 'retiros_internal_detail.js'), 'utf8'), ctxRba, { filename: 'retiros_internal_detail.js' });

function llamar(ctx, nombre, ...args) {
  ctx.__args = args;
  return vm.runInContext(`${nombre}(...__args)`, ctx);
}

// ── Fixtures de stock (mismo shape que get_erp_stock_by_sku/skus en app.py) ─
function stock({ fisico, comprometido = 0, devengado = 0 }) {
  const disponible = fisico - comprometido;
  return {
    stock: {
      sku: 'X', nombre: 'Producto de prueba',
      fisico, comprometido, devengado, disponible,
      hay_stock: disponible > 0,
    },
  };
}

const CASOS = [
  ['tka', ctxTka, '_tkaStockEstado', '_tkaStockBadge'],
  ['rba', ctxRba, '_rbaStockEstado', '_rbaStockBadge'],
];

let pasadas = 0;
function ok(desc) { pasadas++; console.log(`  ok - ${desc}`); }

for (const [prefijo, ctx, fnEstado, fnBadge] of CASOS) {

  // ════════════════════════════════════════════════════════════════════
  // 1) Stock sano (disponible > 0) -- sin badge, sin estado.
  // ════════════════════════════════════════════════════════════════════
  {
    const l = stock({ fisico: 5, comprometido: 1 });
    assert.strictEqual(llamar(ctx, fnEstado, l), '');
    assert.strictEqual(llamar(ctx, fnBadge, l), '');
    ok(`${prefijo}: stock sano no genera badge ni estado`);
  }

  // ════════════════════════════════════════════════════════════════════
  // 2) Cero físico -- sigue siendo el aviso rojo "Sin stock bod. 02".
  // ════════════════════════════════════════════════════════════════════
  {
    const l = stock({ fisico: 0, comprometido: 0 });
    assert.strictEqual(llamar(ctx, fnEstado, l), 'sin');
    const badge = llamar(ctx, fnBadge, l);
    assert.ok(badge.includes('ln-stock-warn'), 'debe usar la clase de aviso rojo');
    assert.ok(badge.includes('Sin stock bod. 02'), 'debe decir "Sin stock bod. 02"');
    assert.ok(!badge.includes('ln-stock-info'), 'NO debe mezclar con la clase informativa');
    ok(`${prefijo}: 0 físico -> "sin", rojo, "Sin stock bod. 02"`);
  }

  // ════════════════════════════════════════════════════════════════════
  // 3) EL CASO REAL DE DANIEL: 1 físico, 1 comprometido (disponible = 0).
  //    Antes esto cala exactamente en el mismo "Sin stock bod. 02" del
  //    caso anterior -- ya NO debe hacerlo.
  // ════════════════════════════════════════════════════════════════════
  {
    const l = stock({ fisico: 1, comprometido: 1 });
    assert.strictEqual(llamar(ctx, fnEstado, l), 'comprometido');
    const badge = llamar(ctx, fnBadge, l);
    assert.ok(badge.includes('ln-stock-info'), 'debe usar la clase informativa (no la de aviso rojo)');
    assert.ok(!badge.includes('ln-stock-warn'), 'NO debe decir que no hay stock: SÍ hay 1 físico');
    assert.ok(!badge.includes('Sin stock bod. 02'), 'el texto "Sin stock" no debe aparecer en este caso');
    // El desglose real, a la vista (no solo en el tooltip):
    assert.ok(badge.includes('1 físico'), 'el físico tiene que verse en el texto, no solo en el title');
    assert.ok(badge.includes('1 comp.'), 'el comprometido tiene que verse en el texto');
    ok(`${prefijo}: 1 físico + 1 comprometido -> "comprometido", ámbar, con el desglose a la vista`);
  }

  // ════════════════════════════════════════════════════════════════════
  // 4) Físico positivo pero comprometido AÚN MÁS alto (disponible < 0):
  //    misma categoría que el caso 3, no debe caer en "sin".
  // ════════════════════════════════════════════════════════════════════
  {
    const l = stock({ fisico: 2, comprometido: 5 });
    assert.strictEqual(llamar(ctx, fnEstado, l), 'comprometido');
    ok(`${prefijo}: físico 2 / comprometido 5 (disponible negativo) sigue siendo "comprometido", no "sin"`);
  }

  // ════════════════════════════════════════════════════════════════════
  // 5) Sin objeto stock (SKU no encontrado en MAEPR) -- no revienta.
  // ════════════════════════════════════════════════════════════════════
  {
    assert.strictEqual(llamar(ctx, fnEstado, {}), '');
    assert.strictEqual(llamar(ctx, fnEstado, null), '');
    assert.strictEqual(llamar(ctx, fnBadge, {}), '');
    ok(`${prefijo}: sin dato de stock no revienta y no marca nada`);
  }

  // ════════════════════════════════════════════════════════════════════
  // 6) El tooltip conserva el desglose completo, incluido devengado.
  // ════════════════════════════════════════════════════════════════════
  {
    const l = stock({ fisico: 1, comprometido: 1, devengado: 8 });
    const badge = llamar(ctx, fnBadge, l);
    assert.ok(badge.includes('Devengado: 8'), 'el devengado tiene que seguir en el tooltip');
    assert.ok(badge.includes('title='), 'el tooltip completo debe existir');
    ok(`${prefijo}: el tooltip sigue con el desglose completo (incluido devengado)`);
  }
}

console.log(`\n${pasadas} verificaciones OK`);
console.log('stock fisico/comprometido OK');

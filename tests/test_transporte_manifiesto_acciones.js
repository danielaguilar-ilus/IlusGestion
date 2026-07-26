'use strict';

const assert = require('assert');
const fs = require('fs');
const vm = require('vm');

let captureHandler = null;
let openedData = null;
let toastMessage = null;

const trigger = {
  dataset: {
    srItem: JSON.stringify({
      id: 138762,
      item_id: 55,
      edicion_bloqueada: 1,
    }),
  },
};

const document = {
  readyState: 'complete',
  addEventListener(type, handler, capture) {
    assert.strictEqual(type, 'click');
    assert.strictEqual(capture, true);
    captureHandler = handler;
  },
};

const window = {
  document,
  abrirSimpliRouteModal(data) {
    openedData = data;
  },
  ilusToast(message) {
    toastMessage = message;
  },
};

const context = vm.createContext({
  window,
  document,
  JSON,
});
const source = fs.readFileSync(
  'static/transporte_manifiesto_acciones.js',
  'utf8'
);
vm.runInContext(source, context);

assert.ok(captureHandler, 'Debe registrar el click delegado en captura');
let prevented = false;
captureHandler({
  target: {
    closest(selector) {
      assert.strictEqual(selector, '.js-open-simpliroute-actions');
      return trigger;
    },
  },
  preventDefault() {
    prevented = true;
  },
});

assert.strictEqual(prevented, true);
assert.strictEqual(openedData.id, 138762);
assert.strictEqual(openedData.edicion_bloqueada, 1);
assert.strictEqual(toastMessage, null);
console.log('Manifest action handler OK');

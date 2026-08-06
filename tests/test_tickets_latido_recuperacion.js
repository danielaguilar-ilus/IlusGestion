'use strict';
/**
 * Pruebas del JavaScript del latido de recepción y del modal de recuperación
 * de correos (bloque inline de templates/tickets/list.html).
 *
 * Por qué existe: la pantalla es la ÚNICA forma en que alguien se entera de
 * que la recepción se cayó. Si este código revienta o pinta mal, volvemos al
 * silencio de 12 días — pero esta vez con un banner roto que nadie mira.
 *
 * Mismo patrón que tests/test_transporte_manifiesto_acciones.js: se ejecuta
 * el script en un `vm` con un DOM de mentira. No hay jsdom en el proyecto.
 *
 * Correr con:  node tests/test_tickets_latido_recuperacion.js
 */
const assert = require('assert');
const fs = require('fs');
const path = require('path');
const vm = require('vm');

// ── Extraer el bloque JS del template (no tiene sintaxis Jinja adentro) ──
const TPL = path.join(__dirname, '..', 'templates', 'tickets', 'list.html');
const html = fs.readFileSync(TPL, 'utf8');
const marca = 'LATIDO DE RECEPCIÓN + RECUPERACIÓN DE CORREOS ATRASADOS';
const iMarca = html.indexOf(marca);
assert.ok(iMarca > 0, 'no se encontró el bloque del latido en list.html');
const ini = html.lastIndexOf('<script>', iMarca) + '<script>'.length;
const fin = html.indexOf('</script>', ini);
const SOURCE = html.slice(ini, fin);
assert.ok(!/\{[%{#]/.test(SOURCE), 'el bloque JS no debe contener sintaxis Jinja');

// ── DOM de mentira ──────────────────────────────────────────────────────
class El {
  constructor(id) {
    this.id = id;
    this.innerHTML = '';
    this.textContent = '';
    this.className = '';
    this.value = '';
    this.disabled = false;
    this.style = {};
    this.attrs = {};
    this.handlers = {};
    this.hijos = {};
  }
  setAttribute(k, v) { this.attrs[k] = v; }
  getAttribute(k) { return this.attrs[k]; }
  addEventListener(t, fn) { (this.handlers[t] = this.handlers[t] || []).push(fn); }
  async fire(t, ev) {
    for (const fn of (this.handlers[t] || [])) await fn(ev || {});
  }
  querySelector(sel) {
    if (!this.hijos[sel]) this.hijos[sel] = new El(sel);
    return this.hijos[sel];
  }
  closest() { return null; }
}

function nuevoEntorno(opts) {
  opts = opts || {};
  const els = {};
  const doc = {
    handlers: {},
    getElementById(id) {
      if (opts.sinModal && id === 'tkRecModal') return null;
      if (!els[id]) els[id] = new El(id);
      return els[id];
    },
    addEventListener(t, fn) { (this.handlers[t] = this.handlers[t] || []).push(fn); },
  };
  const estado = {
    peticiones: [],
    toasts: [],
    confirms: [],
    respuestas: opts.respuestas || {},
    confirmRespuesta: opts.confirmRespuesta !== false,
  };
  const ctx = {
    document: doc,
    console,
    JSON,
    Math,
    Array,
    setTimeout: () => 0,     // no queremos temporizadores en el test
    setInterval: () => 0,
    bootstrap: { Modal: function () { this.show = () => {}; } },
    ilusToast: (m, o) => estado.toasts.push({ m, o }),
    ilusConfirm: async (o) => { estado.confirms.push(o); return estado.confirmRespuesta; },
    async fetch(url, init) {
      estado.peticiones.push({ url, init });
      const clave = (init && init.method === 'POST') ? 'rec' : 'salud';
      const cuerpo = estado.respuestas[clave];
      const payload = typeof cuerpo === 'function' ? cuerpo(estado.peticiones.length) : cuerpo;
      return { status: 200, text: async () => JSON.stringify(payload) };
    },
  };
  vm.createContext(ctx);
  vm.runInContext(SOURCE, ctx);
  return { els, doc, estado };
}

const SALUD_OK = {
  ok: true, estado: 'ok', titulo: 'Recepción de correos activa',
  detalle: 'Las respuestas de clientes están entrando a los tickets.',
  accion: '', ultimo_correo: '05/08/2026 09:14', ultimo_correo_ticket: 'TK-2026-00042',
  ultimo_ok: '05/08/2026 09:20', puede_recuperar: true,
};
const SALUD_CRITICA = {
  ok: true, estado: 'critico', titulo: 'La recepción de correos está caída',
  detalle: '[AUTHENTICATIONFAILED] Invalid credentials',
  accion: 'Los correos de clientes NO se están convirtiendo en tickets.',
  ultimo_correo: '24/07/2026 17:02', ultimo_correo_ticket: 'TK-2026-00031',
  ultimo_ok: '24/07/2026 17:05', puede_recuperar: true,
};

function filas(n, estadoFila) {
  const out = [];
  for (let i = 0; i < n; i++) {
    out.push({
      estado: estadoFila || 'nuevo',
      fecha: '28/07/2026 15:30',
      de: 'Ana Cliente ' + i,
      correo: 'ana' + i + '@gimnasiosur.cl',
      asunto: 'Re: [TK-2026-000' + (10 + i) + '] falla',
      numero: 'TK-2026-000' + (10 + i),
      nota: 'Falta en el ticket.',
    });
  }
  return out;
}

const pruebas = [];
function prueba(nombre, fn) { pruebas.push([nombre, fn]); }

// Los handlers de click no devuelven su promesa (así se escriben en el
// navegador), así que hay que dejar correr la cola de microtareas.
async function flush(n) {
  for (let i = 0; i < (n || 4); i++) await new Promise((r) => setImmediate(r));
}

// ── 1. Latido ───────────────────────────────────────────────────────────
prueba('el latido verde se pinta igual (tiene que verse SIEMPRE)', async () => {
  const { els } = nuevoEntorno({ respuestas: { salud: SALUD_OK } });
  await flush();
  const box = els.tkMailSalud;
  assert.strictEqual(box.getAttribute('data-estado'), 'ok');
  assert.ok(box.querySelector('.tkms-titulo').innerHTML.includes('activa'));
  assert.ok(els.tkmsMeta.innerHTML.includes('TK-2026-00042'));
  assert.strictEqual(els.tkmsCta.innerHTML, '', 'en verde no hace falta CTA');
});

prueba('el latido rojo muestra el error real y cómo arreglarlo', async () => {
  const { els } = nuevoEntorno({ respuestas: { salud: SALUD_CRITICA } });
  await flush();
  const box = els.tkMailSalud;
  assert.strictEqual(box.getAttribute('data-estado'), 'critico');
  assert.ok(box.querySelector('.tkms-detalle').textContent.includes('AUTHENTICATIONFAILED'));
  assert.ok(els.tkmsCta.innerHTML.includes('Revisar configuración'));
  assert.ok(els.tkmsCta.innerHTML.includes('Recuperar correos'));
});

prueba('si el endpoint falla, el latido lo dice en vez de quedarse mudo', async () => {
  const { els } = nuevoEntorno({ respuestas: { salud: { ok: false, error: 'Sin conexión' } } });
  await flush();
  assert.strictEqual(els.tkMailSalud.getAttribute('data-estado'), 'desconocido');
  assert.ok(els.tkMailSalud.querySelector('.tkms-titulo').textContent.includes('No se pudo'));
});

prueba('sin permiso de superadmin no se ofrece recuperar', async () => {
  const salud = Object.assign({}, SALUD_CRITICA, { puede_recuperar: false });
  const { els } = nuevoEntorno({ respuestas: { salud } });
  await flush();
  assert.ok(!els.tkmsCta.innerHTML.includes('Recuperar correos'));
});

prueba('sin el modal (no superadmin) el script no revienta', async () => {
  const { els } = nuevoEntorno({ sinModal: true, respuestas: { salud: SALUD_OK } });
  await flush();
  assert.strictEqual(els.tkMailSalud.getAttribute('data-estado'), 'ok');
});

// ── 2. Vista previa ─────────────────────────────────────────────────────
async function correrPreview(payload, entornoOpts) {
  const ent = nuevoEntorno(Object.assign(
    { respuestas: { salud: SALUD_OK, rec: payload } }, entornoOpts || {}));
  await flush();
  ent.els.tkRecDias.value = '20';
  ent.els.tkRecMax.value = '200';
  await ent.els.tkRecBtnPreview.fire('click');
  await flush();
  return ent;
}

prueba('la vista previa manda dry_run=true y los días elegidos', async () => {
  const ent = await correrPreview({ ok: true, dry_run: true, dias: 20, ingresados: 2,
    detalle: filas(2) });
  const post = ent.estado.peticiones.find((p) => p.init && p.init.method === 'POST');
  const body = JSON.parse(post.init.body);
  assert.strictEqual(body.dry_run, true);
  assert.strictEqual(body.dias, 20);
  assert.strictEqual(post.url, '/tickets/api/mail/recuperar');
});

prueba('la previa dice claramente que no escribió nada', async () => {
  const ent = await correrPreview({ ok: true, dry_run: true, dias: 20, ingresados: 3,
    detalle: filas(3) });
  assert.ok(ent.els.tkRecEstado.innerHTML.includes('no se escribió nada'));
  assert.ok(ent.els.tkRecFooterNota.textContent.includes('no se escribió nada'));
  assert.strictEqual(ent.els.tkRecBtnIngestar.disabled, false);
});

prueba('sin correos pendientes el botón de ingesta queda apagado', async () => {
  const ent = await correrPreview({ ok: true, dry_run: true, dias: 20, ingresados: 0,
    detalle: filas(2, 'ya_ingerido') });
  assert.strictEqual(ent.els.tkRecBtnIngestar.disabled, true);
});

prueba('los que faltan se listan primero', async () => {
  const mezcla = filas(1, 'ya_ingerido').concat(filas(1, 'propio'), filas(1, 'nuevo'));
  const ent = await correrPreview({ ok: true, dry_run: true, dias: 20, ingresados: 1,
    detalle: mezcla });
  const cuerpo = ent.els.tkRecTbody.innerHTML;
  assert.ok(cuerpo.indexOf('Se va a ingresar') < cuerpo.indexOf('Ya está en el ticket'));
});

prueba('el error del servidor se muestra con la pista de Gmail/IMAP', async () => {
  const ent = await correlPreviewError();
  assert.ok(ent.els.tkRecEstado.innerHTML.includes('IMAP'));
  assert.strictEqual(ent.els.tkRecBtnIngestar.disabled, true);
  assert.ok(ent.estado.toasts.some((t) => t.o && t.o.type === 'error'));
});
async function correlPreviewError() {
  return correrPreview({ ok: false, error: 'IMAP no disponible: credenciales' });
}

prueba('un asunto malicioso se escapa (no se inyecta HTML)', async () => {
  const fila = filas(1)[0];
  fila.asunto = '<img src=x onerror="alert(1)">';
  fila.de = '"><script>alert(2)</script>';
  const ent = await correrPreview({ ok: true, dry_run: true, dias: 20, ingresados: 1,
    detalle: [fila] });
  const cuerpo = ent.els.tkRecTbody.innerHTML;
  assert.ok(!cuerpo.includes('<img src=x'), 'el asunto tiene que ir escapado');
  assert.ok(!cuerpo.includes('<script>'), 'el remitente tiene que ir escapado');
  assert.ok(cuerpo.includes('&lt;img'));
});

prueba('avisa cuando el buzón trae más correos que el tope', async () => {
  const ent = await correrPreview({ ok: true, dry_run: true, dias: 20, ingresados: 1,
    vistos: 200, truncado: true, truncado_total: 640, detalle: filas(1) });
  assert.ok(ent.els.tkRecEstado.innerHTML.includes('640'));
});

prueba('avisa cuando se cortó por tiempo y que reintentar es seguro', async () => {
  const ent = await correrPreview({ ok: true, dry_run: true, dias: 20, ingresados: 1,
    parcial: true, detalle: filas(1) });
  assert.ok(ent.els.tkRecEstado.innerHTML.includes('no duplica nada'));
});

// ── 3. Paginación (REGLA #4.3) ──────────────────────────────────────────
prueba('la tabla se pagina como Etiquetas: Mostrando A–B de N', async () => {
  const ent = await correrPreview({ ok: true, dry_run: true, dias: 20, ingresados: 60,
    detalle: filas(60) });
  assert.strictEqual(ent.els.tkRecPagInfo.textContent, 'Mostrando 1–50 de 60');
  assert.strictEqual(ent.els.tkRecPagNum.textContent, 'Página 1 de 2');
  assert.strictEqual(ent.els.tkRecPagPrev.disabled, true);
  assert.strictEqual(ent.els.tkRecPagNext.disabled, false);
});

prueba('siguiente / anterior recorren las páginas', async () => {
  const ent = await correrPreview({ ok: true, dry_run: true, dias: 20, ingresados: 60,
    detalle: filas(60) });
  await ent.els.tkRecPagNext.fire('click');
  await flush();
  assert.strictEqual(ent.els.tkRecPagInfo.textContent, 'Mostrando 51–60 de 60');
  assert.strictEqual(ent.els.tkRecPagNext.disabled, true);
  await ent.els.tkRecPagPrev.fire('click');
  await flush();
  assert.strictEqual(ent.els.tkRecPagInfo.textContent, 'Mostrando 1–50 de 60');
});

prueba('el tamaño de página lo elige el usuario', async () => {
  const ent = await correrPreview({ ok: true, dry_run: true, dias: 20, ingresados: 60,
    detalle: filas(60) });
  ent.els.tkRecPagSize.value = '25';
  await ent.els.tkRecPagSize.fire('change');
  await flush();
  assert.strictEqual(ent.els.tkRecPagInfo.textContent, 'Mostrando 1–25 de 60');
  assert.strictEqual(ent.els.tkRecPagNum.textContent, 'Página 1 de 3');
});

// ── 4. Ingesta ──────────────────────────────────────────────────────────
prueba('la ingesta pide confirmación antes de tocar tickets reales', async () => {
  const ent = await correrPreview({ ok: true, dry_run: true, dias: 20, ingresados: 2,
    detalle: filas(2) });
  ent.estado.confirmRespuesta = false;
  const antes = ent.estado.peticiones.length;
  await ent.els.tkRecBtnIngestar.fire('click');
  await flush();
  assert.strictEqual(ent.estado.confirms.length, 1);
  assert.ok(ent.estado.confirms[0].sub.includes('No se le envía ningún correo al cliente'));
  assert.strictEqual(ent.estado.peticiones.length, antes, 'si cancela, no se llama al servidor');
});

prueba('al confirmar, manda dry_run=false', async () => {
  const ent = await correrPreview({ ok: true, dry_run: true, dias: 20, ingresados: 2,
    detalle: filas(2) });
  ent.estado.respuestas.rec = { ok: true, dry_run: false, dias: 20, ingresados: 2,
    adjuntos: 1, detalle: filas(2) };
  await ent.els.tkRecBtnIngestar.fire('click');
  await flush();
  const posts = ent.estado.peticiones.filter((p) => p.init && p.init.method === 'POST');
  assert.strictEqual(JSON.parse(posts[posts.length - 1].init.body).dry_run, false);
  assert.ok(ent.els.tkRecEstado.innerHTML.includes('No se le envió ningún correo'));
  assert.strictEqual(ent.els.tkRecBtnIngestar.disabled, true, 'no se puede ejecutar dos veces');
});

// ── Runner ──────────────────────────────────────────────────────────────
(async () => {
  let fallos = 0;
  for (const [nombre, fn] of pruebas) {
    try {
      await fn();
      console.log('  ok   ' + nombre);
    } catch (e) {
      fallos++;
      console.log('  FALL ' + nombre + '\n       ' + e.message);
    }
  }
  console.log('\n' + (pruebas.length - fallos) + '/' + pruebas.length + ' pruebas JS OK');
  process.exit(fallos ? 1 : 0);
})();

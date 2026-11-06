# -*- coding: utf-8 -*-
"""Paso «Equipos y su checklist» del asistente de OT: la PANTALLA (REGLA #63).

Daniel (5-nov-2026), viendo la línea de 10 unidades de la VD 11117 partida en 10 filas y un checklist que había que buscar fila por fila:
«dame la opción de multiplicar la fila o dejarlo con una fila con 18 · me cuesta mucho seleccionar el checklist, esto parte tiene que ser inteligente ·
guardarla en los catálogos si lo seleccionan de allí».

El servidor (juntar, separar, pistas, recordar) se prueba en tests/test_ot_equipos_filas.py. Aquí:
  1. La lógica de la pantalla EJECUTADA con Node (agrupar filas, interruptor de conteo, elegir una vez para todas, recordar en el catálogo,
     buscador de checklist), con el servidor y el DOM de mentira. Nada de datos reales (REGLA #22).
  2. El cableado de la plantilla: todo lo que el HTML llama como OT2C.xxx está exportado, CRLF intacto, sin alert/confirm/prompt nativos,
     tamaños de toque y de letra de la REGLA #3/#61.

Correr:  python -m pytest tests/test_ot_equipos_filas_front.py -q -p no:warnings
"""
import json
import os
import re
import shutil
import subprocess
import tempfile
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RUTA_HTML = os.path.join(RAIZ, "templates", "ot2", "_modal_crear.html")
HAY_NODE = shutil.which("node") is not None


def _html():
    with open(RUTA_HTML, "rb") as f:
        return f.read().decode("utf-8")


_PRUEBA_JS = r"""
const vm = require('vm'), fs = require('fs');
const src = fs.readFileSync(process.argv[2], 'utf8').replace(/\r\n/g, '\n');
function corte(a, b) {
  const i = src.indexOf(a), j = src.indexOf(b, i);
  if (i < 0 || j < 0) throw new Error('no encuentro: ' + a + ' ... ' + b);
  return src.slice(i, j);
}
const bloque = corte('  /* ═══ FILAS DE EQUIPOS · INICIO', '  /* ═══ FILAS DE EQUIPOS · FIN');
const combo = corte('  var _comboListEl = null, _comboMidActivo = null', '  /* Elegir el checklist a mano ENSEÑA al catálogo');
const sugerir = corte('  function sugerirPlantillaEquipo(mid){', '  var tBusca = null;');
const norm = src.match(/function _o2mSkuNorm\(s\)\{[^\n]*\}/)[0] + '\n' + src.match(/function _o2mDocOrigenNorm\(s\)\{[^\n]*\}/)[0];
const resultados = [];
const ok = (nombre, cond, extra) => resultados.push({nombre, ok: !!cond, extra: cond ? '' : String(extra === undefined ? '' : extra)});
const tick = () => new Promise(r => setTimeout(r, 0));
const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'}[c]));

const PLANT = [{id: 1, nombre: 'Revisión — Piso de goma'}, {id: 2, nombre: 'Instalación de Piso'}, {id: 3, nombre: 'Cinta de correr — Revisión'}, {id: 4, nombre: 'Accesorios (instalación)'}];
const fila = (id, extra) => Object.assign({id, nombre: 'Rubber Floor 20mm Negro', sku: 'PISO-20', serie: 'S-' + id, doc_origen: 'VD 11117', cantidad: 1,
                                          aplica_mantencion: 1, created_at: new Date().toUTCString(), origen_visita_id: null}, extra || {});
const diez = () => Array.from({length: 10}, (_, i) => fila(100 + i));
const marcar = (ctx, ids, plantilla, origen) => ids.forEach(id => ctx.S.equipos.push({maquina_id: id, plantilla_id: plantilla || null, plantilla_sugerida: false, plantilla_origen: origen || null}));

function montar(opts) {
  opts = opts || {};
  const reg = {fetch: [], toasts: [], alertas: [], confirms: [], filas: [], barra: 0, anclas: 0, pintar: 0, aprender: [], sug: []};
  const respuestas = (opts.confirmaciones || []).slice();
  const els = opts.els || {};
  const rutas = opts.rutas || {};
  const ctx = {
    console, Promise, Date, Object, JSON, Math, String, parseInt, isFinite, encodeURIComponent, setTimeout, Array,
    S: Object.assign({tipo: 'instalacion', cliente: {id: 7}, equipos: [], _equiposAutoCreados: []}, opts.S || {}),
    cacheEquipos: opts.equipos || [], cachePlant: opts.plantillas || PLANT,
    esc,
    document: {getElementById: id => els[id] || null, querySelectorAll: () => [], addEventListener() {}, createElement: () => ({style: {}, setAttribute() {}, classList: {toggle() {}}}), body: {appendChild() {}}},
    window: {innerWidth: 1280, innerHeight: 800},
    _o2mPlanAplica: () => false,
    _o2mEquiposVista: () => ctx.cacheEquipos,
    _o2mPlantNombre: pid => (ctx.cachePlant.find(p => p.id === pid) || {}).nombre || '',
    _o2mFilaEquipoHTML: m => '<tr id="o2mEqRow-' + m.id + '" class="o2m-eqrow"></tr>',
    _o2mRefrescarFilaEquipo: mid => reg.filas.push(mid),
    _o2mRefrescarBarraEq: () => { reg.barra++; },
    _o2mActualizarSugerenciaFila: mid => reg.sug.push(mid),
    _o2mAprenderClasificacion: (mid, pid) => reg.aprender.push([mid, pid]),
    refrescarAnclas: () => { reg.anclas++; },
    _o2mFinEstimar() {},
    pintarListaEquipos: () => { reg.pintar++; },
    ilusToast: (m, o) => reg.toasts.push(m),
    ilusAlert: o => { reg.alertas.push(o); return Promise.resolve(true); },
    ilusConfirm: o => { reg.confirms.push(o); return Promise.resolve(respuestas.length ? respuestas.shift() : true); },
    fetch: (url, o) => {
      const body = (o && o.body) ? JSON.parse(o.body) : null;
      reg.fetch.push({url, body, metodo: (o && o.method) || 'GET'});
      const clave = Object.keys(rutas).find(k => url.indexOf(k) >= 0);
      let r = {ok: false};
      if (clave) r = (typeof rutas[clave] === 'function') ? rutas[clave](body, url, reg.fetch.filter(f => f.url.indexOf(clave) >= 0).length) : rutas[clave];
      return Promise.resolve({json: () => Promise.resolve(r)});
    },
  };
  ctx.window.ilusToast = ctx.ilusToast; ctx.window.ilusAlert = ctx.ilusAlert; ctx.window.ilusConfirm = ctx.ilusConfirm;
  if (opts.tecnico) ctx.O2M_ES_TECNICO = true;
  vm.createContext(ctx);
  vm.runInContext(norm + '\n' + bloque + '\n' + combo + '\n' + sugerir, ctx);
  return {ctx, reg, els};
}

(async () => {
  // 1. Agrupar la vista
  { const m = montar({equipos: diez()}), L = m.ctx._oefGrupos(m.ctx.cacheEquipos);
    ok('10 filas del mismo producto y documento = UN grupo de unidades', L.length === 1 && L[0].tipo === 'unidades' && L[0].total === 10 && L[0].filas.length === 10);
    ok('cada fila sabe a qué grupo pertenece', m.ctx._oefGrupoDeMid[105] === L[0]); }
  { const m = montar({equipos: [fila(1, {cantidad: 18})]}), L = m.ctx._oefGrupos(m.ctx.cacheEquipos);
    ok('una fila con cantidad 18 = grupo «cantidad» de 18', L.length === 1 && L[0].tipo === 'cantidad' && L[0].total === 18); }
  { const m = montar({equipos: [fila(1), fila(2, {sku: 'OTRO'}), fila(3, {sku: '', doc_origen: ''})]}), L = m.ctx._oefGrupos(m.ctx.cacheEquipos);
    ok('filas sueltas o sin SKU se pintan como siempre', L.length === 3 && L.every(g => g.tipo === 'simple')); }
  { const m = montar({equipos: [fila(1), fila(2), fila(3, {doc_origen: 'FCV 500'}), fila(4, {doc_origen: 'FCV 500'})]}), L = m.ctx._oefGrupos(m.ctx.cacheEquipos);
    ok('el mismo SKU de OTRO documento es otro grupo', L.length === 2 && L[0].gid !== L[1].gid && L[0].filas.length === 2 && L[1].filas.length === 2); }
  { const m = montar({equipos: [fila(1, {cantidad: 3}), fila(2)]}), L = m.ctx._oefGrupos(m.ctx.cacheEquipos);
    ok('una mezcla de cantidades no se agrupa (se deja a la vista tal cual)', L.length === 2 && L.every(g => g.tipo === 'simple')); }
  { const m = montar({equipos: [fila(1), fila(2, {doc_origen: 'VD-11117'})]}), L = m.ctx._oefGrupos(m.ctx.cacheEquipos);
    ok('«VD 11117» y «VD-11117» son el mismo documento', L.length === 1 && L[0].tipo === 'unidades'); }
  { const m = montar({equipos: diez()}), a = m.ctx._oefGrupos(m.ctx.cacheEquipos)[0].gid, b = m.ctx._oefGrupos(m.ctx.cacheEquipos)[0].gid;
    ok('el id del grupo es estable entre repintados', a === b); }

  // 2. ¿Se pueden juntar/separar?
  { const m = montar({equipos: diez()}), g = m.ctx._oefGrupos(m.ctx.cacheEquipos)[0];
    ok('filas recién creadas y sin usar: sí', m.ctx._oefEditable(g) === true);
    g.filas[3].origen_visita_id = 55; ok('una fila que ya usó una OT: no', m.ctx._oefEditable(g) === false);
    g.filas[3].origen_visita_id = null; g.filas[2].created_at = new Date(Date.now() - 9 * 86400000).toUTCString();
    ok('una fila de hace más de 7 días: no', m.ctx._oefEditable(g) === false);
    m.ctx.S._equiposAutoCreados = [g.filas[2].id]; ok('...salvo que este mismo asistente la acabe de crear', m.ctx._oefEditable(g) === true); }

  // 3. El interruptor de conteo
  { const m = montar({equipos: diez()}), g = m.ctx._oefGrupos(m.ctx.cacheEquipos)[0], h = m.ctx._oefSegHtml(g);
    ok('ofrece las dos formas con la cantidad a la vista', /Una fila con cantidad 10/.test(h) && /Una fila por unidad \(10 filas\)/.test(h), h);
    ok('la forma actual está marcada y dice que lo está', /class="o2m-seg on" aria-pressed="true"[^>]*>Una fila por unidad/.test(h), h);
    ok('el grupo se anuncia como grupo de botones con el nombre del producto', /role="group" aria-label="Cómo contar Rubber Floor 20mm Negro"/.test(h));
    m.ctx._oefPistas['PISO-20|instalacion'] = {conteo: {valor: 'cantidad', origen: 'nombre', motivo: 'Sugerido: por su nombre («rubber») parece un piso o accesorio y se cuenta por cantidad.'}};
    const h2 = m.ctx._oefSegHtml(g);
    ok('marca lo sugerido y dice por qué (origen a la vista)', /Una fila con cantidad 10<small class="o2m-seg-sug"><i class="bi bi-magic"><\/i> sugerido/.test(h2) && /por su nombre/.test(h2), h2);
    g.filas[0].origen_visita_id = 9; const h3 = m.ctx._oefSegHtml(g);
    ok('si no se puede cambiar, el botón queda deshabilitado y lo explica', /disabled[^>]*title="Estas filas tienen más de 7 días/.test(h3) && /ya se usaron en una OT/.test(h3), h3); }
  { const m = montar({equipos: [fila(1, {cantidad: 150})]}), g = m.ctx._oefGrupos(m.ctx.cacheEquipos)[0], h = m.ctx._oefSegHtml(g);
    ok('más de 100 unidades no se separan desde el asistente', /disabled[^>]*title="Son más de 100 unidades/.test(h), h); }
  { const m = montar({equipos: [fila(1, {nombre: '<img src=x onerror=alert(1)>', cantidad: 4})]}), g = m.ctx._oefGrupos(m.ctx.cacheEquipos)[0];
    const h = m.ctx._oefSegHtml(g) + m.ctx._oefCabeceraHtml(g);
    ok('el nombre del producto va escapado', !/<img/.test(h) && /&lt;img/.test(h)); }

  // 4. Pistas: una consulta para todos
  { const m = montar({equipos: diez(), rutas: {'/ot/api/equipos/pistas': {ok: true, skus: {'PISO-20': {sku: 'PISO-20', conteo: {valor: 'cantidad', origen: 'nombre', motivo: 'x'}, guardada: null, de_clase: null}}}}});
    const L = m.ctx._oefGrupos(m.ctx.cacheEquipos); m.ctx._oefCargarPistas(L); m.ctx._oefCargarPistas(L); await tick(); await tick();
    const ll = m.reg.fetch.filter(f => /pistas/.test(f.url));
    ok('UNA sola consulta de pistas aunque se pida dos veces', ll.length === 1, ll.length);
    ok('lleva el producto una vez, con la cantidad total y el tipo de OT', ll[0].body.items.length === 1 && ll[0].body.items[0].cantidad === 10 && ll[0].body.tipo === 'instalacion', JSON.stringify(ll[0].body));
    ok('la pista queda guardada', m.ctx._oefPistaDe('piso-20').conteo.valor === 'cantidad');
    ok('una línea de varias unidades con pista «cantidad» se da de alta como una fila', m.ctx._oefSugiereCantidad({sku: 'PISO-20', qty: 10}) === true);
    ok('una línea de una unidad nunca', m.ctx._oefSugiereCantidad({sku: 'PISO-20', qty: 1}) === false);
    ok('un producto sin pista se parte por unidad (con la duda, por unidad)', m.ctx._oefSugiereCantidad({sku: 'OTRO', qty: 5}) === false); }
  { const m = montar({equipos: diez(), rutas: {'/ot/api/equipos/pistas': {ok: false}}});
    m.ctx._oefCargarPistas(m.ctx._oefGrupos(m.ctx.cacheEquipos)); await tick(); await tick();
    ok('si las pistas fallan todo sigue como antes', m.ctx._oefPistaDe('PISO-20') === null); }

  // 5. Sugerencia de checklist: una consulta por producto, con su origen
  { const m = montar({equipos: diez(), rutas: {'/sugerida-por-sku': {ok: true, plantilla_id: 2, origen: 'producto'}}});
    marcar(m.ctx, m.ctx.cacheEquipos.map(x => x.id));
    m.ctx.S.equipos[0].plantilla_id = 3; m.ctx.S.equipos[0].plantilla_origen = 'manual';
    m.ctx.cacheEquipos.forEach(x => m.ctx.sugerirPlantillaEquipo(x.id)); await tick(); await tick(); await tick();
    ok('una sola consulta para las 10 unidades', m.reg.fetch.filter(f => /sugerida-por-sku/.test(f.url)).length === 1, m.reg.fetch.length);
    ok('las 9 sin checklist quedan con el guardado y su origen', m.ctx.S.equipos.slice(1).every(e => e.plantilla_id === 2 && e.plantilla_origen === 'producto' && e.plantilla_sugerida === true));
    ok('lo elegido a mano no se pisa', m.ctx.S.equipos[0].plantilla_id === 3 && m.ctx.S.equipos[0].plantilla_origen === 'manual'); }
  { const m = montar({equipos: [fila(1)], rutas: {'/sugerida-por-sku': {ok: true, plantilla_id: 1}}}); marcar(m.ctx, [1]);
    m.ctx.sugerirPlantillaEquipo(1); await tick(); await tick();
    ok('sin origen «producto» es «según su clase»', m.ctx.S.equipos[0].plantilla_origen === 'clase'); }

  // 6. Juntar en UNA fila
  { const m = montar({equipos: diez(), S: {_equiposAutoCreados: [100, 101, 102, 103, 104, 105, 106, 107, 108, 109]},
        rutas: {'/ot/api/equipos/agrupar': {ok: true, principal: {id: 100, nombre: 'Rubber Floor 20mm Negro', sku: 'PISO-20', serie: 'S-100', cantidad: 10, doc_origen: 'VD 11117'},
                                             bajas: [101, 102, 103, 104, 105, 106, 107, 108, 109], total: 10, mensaje: 'Rubber Floor: 10 filas juntas en una fila con cantidad 10.'}}});
    marcar(m.ctx, [100, 101, 102], 2, 'manual');
    const g = m.ctx._oefGrupos(m.ctx.cacheEquipos)[0]; await m.ctx.eqGrupoModo(g.gid, 'cantidad');
    const q = m.reg.fetch.find(f => /agrupar/.test(f.url));
    ok('pidió confirmación con un título claro', m.reg.confirms.length === 1 && m.reg.confirms[0].okLabel === 'Juntar en una fila' && /No se borra nada/.test(m.reg.confirms[0].sub));
    ok('manda el cliente y las 10 filas', q && q.body.cliente_id === 7 && q.body.ids.length === 10, JSON.stringify(q));
    ok('la ficha local queda con UNA fila de cantidad 10', m.ctx.cacheEquipos.length === 1 && m.ctx.cacheEquipos[0].id === 100 && m.ctx.cacheEquipos[0].cantidad === 10);
    ok('la fila que queda sigue marcada y con su checklist', m.ctx.S.equipos.length === 1 && m.ctx.S.equipos[0].maquina_id === 100 && m.ctx.S.equipos[0].plantilla_id === 2);
    ok('las filas en baja salen de las que el asistente creó', m.ctx.S._equiposAutoCreados.join() === '100', m.ctx.S._equiposAutoCreados.join());
    ok('se repinta una vez y se avisa con el mensaje del servidor', m.reg.pintar === 1 && m.reg.toasts.some(t => /10 filas juntas/.test(t)), JSON.stringify(m.reg.toasts)); }
  { const m = montar({equipos: diez(), confirmaciones: [false]}); marcar(m.ctx, [100]);
    const g = m.ctx._oefGrupos(m.ctx.cacheEquipos)[0]; await m.ctx.eqGrupoModo(g.gid, 'cantidad');
    ok('si la persona dice «Mejor no», no se llama al servidor ni cambia nada', m.reg.fetch.length === 0 && m.ctx.cacheEquipos.length === 10 && m.reg.pintar === 0); }
  { const m = montar({equipos: diez(), rutas: {'/ot/api/equipos/agrupar': {ok: false, error: 'Alguna de esas filas ya se usó en una OT.'}}}); marcar(m.ctx, [100, 101], 2);
    const g = m.ctx._oefGrupos(m.ctx.cacheEquipos)[0]; await m.ctx.eqGrupoModo(g.gid, 'cantidad');
    ok('si el servidor rechaza, lo dice con sus palabras y no cambia nada', m.reg.alertas.length === 1 && /ya se usó en una OT/.test(m.reg.alertas[0].message) &&
       m.ctx.cacheEquipos.length === 10 && m.ctx.S.equipos.length === 2 && m.reg.pintar === 0, JSON.stringify(m.reg.alertas));
    ok('después de un rechazo el grupo queda libre para reintentar', !m.ctx._oefOcupado[g.gid]); }
  { const m = montar({equipos: diez()}); const g = m.ctx._oefGrupos(m.ctx.cacheEquipos)[0];
    m.ctx._oefOcupado[g.gid] = true; await m.ctx.eqGrupoModo(g.gid, 'cantidad');
    ok('mientras se cambia, otro clic se ignora', m.reg.confirms.length === 0 && m.reg.fetch.length === 0);
    delete m.ctx._oefOcupado[g.gid]; await m.ctx.eqGrupoModo(g.gid, 'unidad');
    ok('pedir la forma en la que ya está no hace nada', m.reg.confirms.length === 0 && m.reg.fetch.length === 0); }

  // 7. Separar por unidad (multiplicar o revertir)
  { const nuevas = [201, 202, 203, 204, 205, 206, 207, 208, 209];
    const m = montar({equipos: [fila(100, {cantidad: 10})], S: {_equiposAutoCreados: [100]},
        rutas: {'/ot/api/equipos/separar': {ok: true, modo: 'multiplicar', principal_id: 100, nuevas, restauradas: [],
                filas: [100].concat(nuevas).map(id => ({id, nombre: 'Rubber Floor 20mm Negro', sku: 'PISO-20', serie: 'S-' + id, cantidad: 1, doc_origen: 'VD 11117'})),
                mensaje: 'Rubber Floor: separada en 10 filas de una unidad.'}}});
    marcar(m.ctx, [100], 2, 'manual');
    const g = m.ctx._oefGrupos(m.ctx.cacheEquipos)[0]; await m.ctx.eqGrupoModo(g.gid, 'unidad');
    const q = m.reg.fetch.find(f => /separar/.test(f.url));
    ok('pidió confirmación: «Separar en 10 filas»', m.reg.confirms[0].okLabel === 'Separar en 10 filas');
    ok('manda el cliente y la fila', q && q.body.cliente_id === 7 && q.body.id === 100, JSON.stringify(q));
    ok('la ficha local queda con 10 filas de una unidad, cada una con su serie', m.ctx.cacheEquipos.length === 10 && m.ctx.cacheEquipos.every(x => x.cantidad === 1) &&
       new Set(m.ctx.cacheEquipos.map(x => x.serie)).size === 10);
    ok('las nuevas quedan marcadas con el MISMO checklist que tenía la fila', m.ctx.S.equipos.length === 10 && m.ctx.S.equipos.every(e => e.plantilla_id === 2 && e.plantilla_origen === 'manual'));
    ok('las nuevas cuentan como creadas por este asistente', nuevas.every(id => m.ctx.S._equiposAutoCreados.indexOf(id) >= 0)); }
  { const m = montar({equipos: [fila(100, {cantidad: 10})],
        rutas: {'/ot/api/equipos/separar': {ok: true, modo: 'multiplicar', principal_id: 100, nuevas: [201, 202], restauradas: [], filas: [100, 201, 202].map(id => ({id, nombre: 'X', sku: 'PISO-20', serie: 'S-' + id, cantidad: 1, doc_origen: 'VD 11117'})), mensaje: 'ok'}}});
    const g = m.ctx._oefGrupos(m.ctx.cacheEquipos)[0]; await m.ctx.eqGrupoModo(g.gid, 'unidad');
    ok('si la fila no estaba marcada, las nuevas tampoco', m.ctx.S.equipos.length === 0); }
  { const rest = [101, 102, 103];
    const m = montar({equipos: [fila(100, {cantidad: 4})],
        rutas: {'/ot/api/equipos/separar': {ok: true, modo: 'revertir', principal_id: 100, nuevas: [], restauradas: rest,
                filas: [100].concat(rest).map(id => ({id, nombre: 'Rubber Floor 20mm Negro', sku: 'PISO-20', serie: 'S-' + id, cantidad: 1, doc_origen: 'VD 11117'})), mensaje: 'revertido'}}});
    marcar(m.ctx, [100], 3, 'producto');
    const g = m.ctx._oefGrupos(m.ctx.cacheEquipos)[0]; await m.ctx.eqGrupoModo(g.gid, 'unidad');
    ok('al revertir vuelven LAS MISMAS filas con sus series', m.ctx.cacheEquipos.map(x => x.id).sort().join() === '100,101,102,103' && m.ctx.cacheEquipos.find(x => x.id === 102).serie === 'S-102');
    ok('y heredan el checklist y su origen', m.ctx.S.equipos.length === 4 && m.ctx.S.equipos.every(e => e.plantilla_id === 3 && e.plantilla_origen === 'producto')); }

  // 8. Buscador de checklist
  { const m = montar({}), C = m.ctx;
    ok('«piso» encuentra los dos y pone primero el más corto', C._oefBuscar(PLANT, 'piso').map(p => p.id).join() === '2,1', C._oefBuscar(PLANT, 'piso').map(p => p.id).join());
    ok('busca sin tildes: «revision» encuentra «Revisión»', C._oefBuscar(PLANT, 'revision').map(p => p.id).join() === '1,3', C._oefBuscar(PLANT, 'revision').map(p => p.id).join());
    ok('varias palabras, en cualquier orden', C._oefBuscar(PLANT, 'GOMA piso').map(p => p.id).join() === '1');
    ok('«instalacion accesorios» encuentra solo el que tiene las dos', C._oefBuscar(PLANT, 'instalacion accesorios').map(p => p.id).join() === '4');
    ok('sin coincidencias devuelve vacío', C._oefBuscar(PLANT, 'zzz').length === 0);
    ok('sin texto devuelve todo', C._oefBuscar(PLANT, '').length === 4);
    ok('resalta lo que coincide sin importar las tildes', C._oefResaltar('Instalación de Piso', 'instalacion') === '<mark>Instalación</mark> de Piso', C._oefResaltar('Instalación de Piso', 'instalacion'));
    ok('resalta cada palabra', C._oefResaltar('Revisión — Piso de goma', 'piso goma') === 'Revisión — <mark>Piso</mark> de <mark>goma</mark>');
    ok('lo resaltado también va escapado', C._oefResaltar('<b>x</b>', 'x') === '&lt;b&gt;<mark>x</mark>&lt;/b&gt;', C._oefResaltar('<b>x</b>', 'x')); }
  { const m = montar({equipos: diez()}), C = m.ctx, g = C._oefGrupos(C.cacheEquipos)[0];
    C._oefPistas['PISO-20|instalacion'] = {guardada: {plantilla_id: 2, nombre: 'Instalación de Piso', por: 'Lenin', cuando: '01/11/2026 10:00'}, de_clase: {plantilla_id: 1, nombre: 'Revisión — Piso de goma'}};
    marcar(C, [100], 3, 'manual');
    const ops = C._oefComboOpciones('g' + g.gid, '');
    const textos = ops.map(o => o.tipo === 'sec' ? '[' + o.txt + ']' : o.pid + (o.tag ? ':' + o.tag : ''));
    ok('sin texto: primero lo sugerido para el producto, con su origen', textos.slice(0, 3).join('|') === '[Sugeridas para este producto]|2:Guardada para este producto|1:Sugerida según su clase', textos.join('|'));
    ok('luego lo ya elegido en esta OT y después todas', textos.slice(3).join('|') === '[Ya elegidas en esta OT]|3|[Todas (1)]|4', textos.join('|'));
    const ops2 = C._oefComboOpciones('g' + g.gid, 'cinta');
    ok('con texto: solo lo que coincide, sin secciones', ops2.length === 1 && ops2[0].pid === 3 && ops2[0].resaltar === 'cinta');
    const opsFila = C._oefComboOpciones('m100', '');
    ok('en una fila con checklist ofrece quitarlo, primero', opsFila[0].quitar === true && opsFila[0].pid === 0);
    const opsB = C._oefComboOpciones('b', '');
    ok('en la barra de todos los marcados no hay sugeridas del producto', !opsB.some(o => o.txt === 'Sugeridas para este producto')); }

  // 9. Elegir una vez para todas las unidades + recordar en el catálogo
  const rutaRecordar = {'/ot/api/producto/checklist': {ok: true, guardado: true, plantilla: {id: 2, nombre: 'Instalación de Piso'}}};
  { const m = montar({equipos: diez(), rutas: rutaRecordar}), C = m.ctx, g = C._oefGrupos(C.cacheEquipos)[0];
    await C._oefAplicarGrupo(g.gid, 2); await tick(); await tick();
    ok('sin nada marcado, marca las 10 y les pone el checklist', C.S.equipos.length === 10 && C.S.equipos.every(e => e.plantilla_id === 2 && e.plantilla_origen === 'manual' && e.plantilla_sugerida === false));
    ok('avisa que las marcó', m.reg.toasts.some(t => /puesto en 10 unidades \(las marqué para esta OT\)/.test(t)), JSON.stringify(m.reg.toasts));
    ok('enseña a la clase UNA vez (no 10)', m.reg.aprender.length === 1 && m.reg.aprender[0][1] === 2);
    const rec = m.reg.fetch.filter(f => /producto\/checklist/.test(f.url));
    ok('lo recuerda en el catálogo UNA vez, con producto, tipo y checklist', rec.length === 1 && rec[0].body.sku === 'PISO-20' && rec[0].body.tipo === 'instalacion' && rec[0].body.plantilla_id === 2 && rec[0].body.reemplazar === false, JSON.stringify(rec));
    ok('queda en la pista como guardado (por ti)', C._oefPistaDe('PISO-20').guardada.plantilla_id === 2 && C._oefPistaDe('PISO-20').guardada.por === 'ti');
    ok('y lo dice', m.reg.toasts.some(t => /queda con el checklist «Instalación de Piso» en el catálogo/.test(t)), JSON.stringify(m.reg.toasts)); }
  { const m = montar({equipos: diez(), rutas: rutaRecordar}), C = m.ctx, g = C._oefGrupos(C.cacheEquipos)[0];
    C.eqRecordar({getAttribute: () => 'PISO-20', checked: false});
    await C._oefAplicarGrupo(g.gid, 2); await tick();
    ok('con «Recordar para este producto» apagado no se guarda nada en el catálogo', m.reg.fetch.filter(f => /producto\/checklist/.test(f.url)).length === 0);
    ok('pero la OT sí lleva el checklist', C.S.equipos.length === 10 && C.S.equipos.every(e => e.plantilla_id === 2)); }
  { const m = montar({equipos: diez(), S: {tipo: null}, rutas: rutaRecordar}), C = m.ctx, g = C._oefGrupos(C.cacheEquipos)[0];
    await C._oefAplicarGrupo(g.gid, 2); await tick();
    ok('sin tipo de OT no hay en qué categoría guardarlo', m.reg.fetch.filter(f => /producto\/checklist/.test(f.url)).length === 0); }
  { const m = montar({equipos: diez(), confirmaciones: [true], rutas: {'/ot/api/producto/checklist': (b, u, n) => n === 1
        ? {ok: true, guardado: false, motivo: 'ya_guardada', actual: {plantilla_id: 3, nombre: 'Cinta de correr — Revisión', por: 'Lenin', cuando: '01/11/2026 10:00'}}
        : {ok: true, guardado: true, plantilla: {id: 2, nombre: 'Instalación de Piso'}, reemplazo: true}}}), C = m.ctx, g = C._oefGrupos(C.cacheEquipos)[0];
    await C._oefAplicarGrupo(g.gid, 2); await tick(); await tick(); await tick();
    const rec = m.reg.fetch.filter(f => /producto\/checklist/.test(f.url));
    ok('si otra persona dejó otro guardado, pregunta antes de pisarlo (con quién y cuándo)', m.reg.confirms.length === 1 && /Lenin/.test(m.reg.confirms[0].message) && /01\/11\/2026/.test(m.reg.confirms[0].message) &&
       m.reg.confirms[0].okLabel === 'Cambiar el guardado', JSON.stringify(m.reg.confirms));
    ok('si acepta, lo reenvía con reemplazar', rec.length === 2 && rec[1].body.reemplazar === true, JSON.stringify(rec.map(r => r.body))); }
  { const m = montar({equipos: diez(), confirmaciones: [false], rutas: {'/ot/api/producto/checklist': {ok: true, guardado: false, motivo: 'ya_guardada', actual: {plantilla_id: 3, nombre: 'Cinta de correr — Revisión', por: 'Lenin', cuando: ''}}}}), C = m.ctx, g = C._oefGrupos(C.cacheEquipos)[0];
    await C._oefAplicarGrupo(g.gid, 2); await tick(); await tick(); await tick();
    ok('si deja el que estaba, no lo pisa y la OT usa lo elegido', m.reg.fetch.filter(f => /producto\/checklist/.test(f.url)).length === 1 && C.S.equipos.every(e => e.plantilla_id === 2));
    ok('y lo dice con claridad', m.reg.toasts.some(t => /sigue «Cinta de correr — Revisión»/.test(t)), JSON.stringify(m.reg.toasts)); }
  { const m = montar({equipos: diez(), rutas: {'/ot/api/producto/checklist': {ok: false, error: 'x'}}}), C = m.ctx, g = C._oefGrupos(C.cacheEquipos)[0];
    await C._oefAplicarGrupo(g.gid, 2); await tick(); await tick();
    ok('si no se puede guardar en el catálogo, la OT sigue igual y se avisa', C.S.equipos.length === 10 && m.reg.toasts.some(t => /No se pudo dejar guardado/.test(t)), JSON.stringify(m.reg.toasts)); }
  { const m = montar({equipos: [fila(1), fila(2, {sku: 'B', nombre: 'Otro'})], rutas: rutaRecordar}), C = m.ctx; C._oefGrupos(C.cacheEquipos); marcar(C, [1]);
    C.eqPlant(1, 2); await tick(); await tick();
    ok('elegir en una fila suelta también recuerda el checklist del producto', m.reg.fetch.filter(f => /producto\/checklist/.test(f.url)).length === 1 && C.S.equipos[0].plantilla_origen === 'manual');
    ok('y enseña a la clase como siempre', m.reg.aprender.length === 1 && m.reg.aprender[0][0] === 1); }
  { const m = montar({equipos: diez(), rutas: rutaRecordar}), C = m.ctx; C._oefGrupos(C.cacheEquipos); marcar(C, [100, 101]);
    C.eqPlant(100, 2); await tick(); await tick();
    ok('un cambio suelto dentro de un grupo de unidades NO se vuelve el checklist del producto', m.reg.fetch.filter(f => /producto\/checklist/.test(f.url)).length === 0 && C.S.equipos[0].plantilla_id === 2); }

  // 10. Para todos los marcados
  { const eq3 = [fila(1, {sku: 'A', nombre: 'A'}), fila(2, {sku: 'B', nombre: 'B'}), fila(3, {sku: 'C', nombre: 'C'})];
    const m = montar({equipos: eq3, confirmaciones: [false]}), C = m.ctx;
    marcar(C, [1], 3, 'manual'); marcar(C, [2, 3]);
    await C._oefAplicarMarcados(2);
    ok('pregunta si hay otros ya elegidos, con cuántos', m.reg.confirms.length === 1 && /1 de los 3/.test(m.reg.confirms[0].message) && m.reg.confirms[0].cancelLabel === 'Solo los que faltan');
    ok('«Solo los que faltan» deja el otro como estaba', C.S.equipos[0].plantilla_id === 3 && C.S.equipos[1].plantilla_id === 2 && C.S.equipos[2].plantilla_id === 2);
    ok('no guarda nada en el catálogo (varios productos distintos)', m.reg.fetch.length === 0); }
  { const eq3 = [fila(1, {sku: 'A', nombre: 'A'}), fila(2, {sku: 'B', nombre: 'B'})];
    const m = montar({equipos: eq3, confirmaciones: [true]}), C = m.ctx; marcar(C, [1], 3, 'manual'); marcar(C, [2]);
    await C._oefAplicarMarcados(2);
    ok('«Cambiar todos» los cambia', C.S.equipos.every(e => e.plantilla_id === 2 && e.plantilla_origen === 'manual')); }
  { const m = montar({equipos: [fila(1, {sku: 'A'}), fila(2, {sku: 'B'})]}), C = m.ctx; await C._oefAplicarMarcados(2);
    ok('sin equipos marcados no hace nada', m.reg.confirms.length === 0 && C.S.equipos.length === 0); }

  // 11. Contador «Faltan N» y origen
  { const m = montar({equipos: diez()}), C = m.ctx, g = C._oefGrupos(C.cacheEquipos)[0];
    ok('sin marcadas no hay semáforo', !/Faltan/.test(C._oefDinHtml(g)) && /0 de 10 marcadas/.test(C._oefDinHtml(g)));
    marcar(C, [100, 101], 2, 'manual'); marcar(C, [102, 103]);
    const h = C._oefDinHtml(g);
    ok('cuenta cuántos checklists faltan', /4 de 10 marcadas/.test(h) && /Faltan 2 checklists/.test(h), h);
    C.S.equipos[2].plantilla_id = 2; C.S.equipos[3].plantilla_id = 3;
    ok('si hay dos checklists distintos lo dice', /más de un checklist/.test(C._oefDinHtml(g)));
    C.S.equipos[3].plantilla_id = 2; C.S.equipos[2].plantilla_origen = 'manual'; C.S.equipos[3].plantilla_origen = 'manual';
    const h2 = C._oefDinHtml(g);
    ok('con todas listas lo dice y muestra el origen', /Todas con checklist/.test(h2) && /Instalación de Piso · Elegida a mano/.test(h2), h2);
    C.S.equipos.splice(1); ok('«Faltan 1 checklist» va en singular', (C.S.equipos[0].plantilla_id = null, /Faltan 1 checklist</.test(C._oefDinHtml(g))), C._oefDinHtml(g));
    C._oefPistas['PISO-20|instalacion'] = {guardada: {plantilla_id: 2, nombre: 'X', por: 'Lenin', cuando: 'ayer'}};
    ok('si hay un checklist guardado, se puede olvidar', /Olvidar el checklist guardado/.test(C._oefDinHtml(g))); }
  { const C = montar({}).ctx;
    ok('origen: guardada para este producto', /Guardada para este producto/.test(C._oefOrigenHtml({plantilla_id: 1, plantilla_origen: 'producto'})));
    ok('origen: elegida a mano', /Elegida a mano/.test(C._oefOrigenHtml({plantilla_id: 1, plantilla_origen: 'manual'})));
    ok('origen: sugerida según su clase', /Sugerida automáticamente según su clase/.test(C._oefOrigenHtml({plantilla_id: 1, plantilla_origen: 'clase'})));
    ok('origen de una sugerencia vieja sin origen: sugerida', /Sugerida automáticamente/.test(C._oefOrigenHtml({plantilla_id: 1, plantilla_sugerida: true})));
    ok('sin checklist no hay origen', C._oefOrigenHtml({plantilla_id: null}) === ''); }

  // 12. Marcar el grupo, abrir/cerrar, olvidar
  { const m = montar({equipos: diez(), rutas: {'/sugerida-por-sku': {ok: false}}}), C = m.ctx, g = C._oefGrupos(C.cacheEquipos)[0];
    C.eqGrupoMarcar(g.gid); ok('«Marcar las 10» marca las 10', C.S.equipos.length === 10 && m.reg.filas.length === 10);
    ok('y la sugerencia se pide UNA vez', m.reg.fetch.filter(f => /sugerida-por-sku/.test(f.url)).length === 1);
    C.eqGrupoMarcar(g.gid); ok('es un interruptor: la segunda vez las desmarca', C.S.equipos.length === 0);
    ok('el botón dice qué hará', /Marcar las 10/.test(C._oefMarcarTxt(g))); }
  { const m = montar({equipos: diez()}), C = m.ctx, g = C._oefGrupos(C.cacheEquipos)[0];
    ok('un grupo de 10 arranca cerrado', C._oefEstaAbierto(g) === false);
    const chico = montar({equipos: [fila(1), fila(2), fila(3)]}).ctx; ok('un grupo de 3 arranca abierto', chico._oefEstaAbierto(chico._oefGrupos(chico.cacheEquipos)[0]) === true);
    const clases = []; m.els['o2mEqG-' + g.gid] = {classList: {toggle: (c, on) => clases.push([c, on])}};
    C.eqGrupoAbrir(g.gid); ok('«Ver las 10 unidades» lo abre', C._oefEstaAbierto(g) === true && clases[0][0] === 'cerrado' && clases[0][1] === false);
    C.eqGrupoAbrir(g.gid); ok('y se vuelve a cerrar', C._oefEstaAbierto(g) === false); }
  { const m = montar({equipos: diez(), confirmaciones: [true], rutas: {'/ot/api/producto/checklist': {ok: true, guardado: false, olvidado: true}}}), C = m.ctx, g = C._oefGrupos(C.cacheEquipos)[0];
    C._oefPistas['PISO-20|instalacion'] = {guardada: {plantilla_id: 2, nombre: 'X', por: 'Lenin', cuando: 'ayer'}};
    await C.eqOlvidar({getAttribute: () => 'PISO-20'});
    const q = m.reg.fetch.find(f => /producto\/checklist/.test(f.url));
    ok('olvidar manda plantilla_id 0 y quita la pista guardada', q && q.body.plantilla_id === 0 && C._oefPistaDe('PISO-20').guardada === null, JSON.stringify(q)); }

  // 12b. Un técnico no ve juntar/separar, ni las pistas, ni recordar (las rutas son solo del equipo interno)
  { const m = montar({equipos: diez(), tecnico: true, rutas: rutaRecordar}), C = m.ctx, g = C._oefGrupos(C.cacheEquipos)[0];
    ok('al técnico no se le ofrece juntar ni separar', C._oefSegHtml(g) === '');
    ok('ni «Recordar para este producto»', C._oefRecordarHtml('PISO-20', '') === '');
    C._oefCargarPistas(C._oefGrupos(C.cacheEquipos)); await tick();
    ok('no consulta las pistas (evita un 403 inútil)', m.reg.fetch.filter(f => /pistas/.test(f.url)).length === 0);
    await C._oefAplicarGrupo(g.gid, 2); await tick();
    ok('puede elegir el checklist de todas las unidades, sin guardarlo en el catálogo', C.S.equipos.length === 10 && C.S.equipos.every(e => e.plantilla_id === 2) &&
       m.reg.fetch.filter(f => /producto\/checklist/.test(f.url)).length === 0);
    ok('el encabezado conserva su buscador y su contador', /o2mComboInpG-/.test(C._oefCabeceraHtml(g)) && /Todas con checklist/.test(C._oefCabeceraHtml(g))); }

  // 13. Hay que repetir el estado inicial al abrir de nuevo
  { const m = montar({equipos: diez()}), C = m.ctx; C._oefGrupos(C.cacheEquipos); C._oefPistas.x = 1; C._oefAbierto['1'] = true; C._oefReiniciar();
    ok('abrir el asistente de nuevo empieza de cero', Object.keys(C._oefPistas).length === 0 && Object.keys(C._oefAbierto).length === 0 && Object.keys(C._oefGrupoPorGid).length === 0); }

  console.log(JSON.stringify(resultados));
})().catch(e => { console.error(e && e.stack || e); process.exit(2); });
"""


@unittest.skipUnless(HAY_NODE, "Node no está instalado")
class TestPantallaDeFilasEjecutada(unittest.TestCase):
    def test_la_logica_de_la_pantalla_ejecutada(self):
        with tempfile.TemporaryDirectory() as d:
            prueba = os.path.join(d, "prueba.js")
            with open(prueba, "w", encoding="utf-8") as f:
                f.write(_PRUEBA_JS)
            r = subprocess.run(["node", prueba, RUTA_HTML], capture_output=True, text=True, timeout=90, encoding="utf-8")
        self.assertEqual(r.returncode, 0, (r.stderr or "")[-2500:])
        resultados = json.loads(r.stdout.strip().splitlines()[-1])
        self.assertGreaterEqual(len(resultados), 80)
        fallas = [x for x in resultados if not x["ok"]]
        self.assertEqual(fallas, [], json.dumps(fallas, ensure_ascii=False, indent=1))


# ═══════════════════════════════════════════════════════════════════════════
#  Cableado de la plantilla
# ═══════════════════════════════════════════════════════════════════════════
class TestCableadoDeLaPlantilla(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.crudo = open(RUTA_HTML, "rb").read()
        cls.html = cls.crudo.decode("utf-8").replace("\r\n", "\n")
        a = cls.html.index("  /* ═══ FILAS DE EQUIPOS · INICIO")
        b = cls.html.index("  /* ═══ FILAS DE EQUIPOS · FIN")
        cls.bloque = cls.html[a:b]
        c = cls.html.index("  var _comboListEl = null, _comboMidActivo = null")
        d = cls.html.index("  /* Elegir el checklist a mano ENSEÑA al catálogo")
        cls.combo = cls.html[c:d]
        cls.nuevo_js = cls.bloque + cls.combo

    def test_el_archivo_conserva_crlf(self):
        self.assertEqual(self.crudo.count(b"\n"), self.crudo.count(b"\r\n"), "hay saltos de línea LF sueltos: el archivo debe seguir en CRLF")

    def test_todo_lo_que_el_html_llama_como_OT2C_esta_exportado(self):
        exportados = self.html[self.html.index("  return {abrir:abrir, abrirWizard:abrirWizard"):]
        exportados = exportados[:exportados.index("};")]
        llamados = set(re.findall(r"OT2C\.(\w+)\(", self.nuevo_js))
        self.assertTrue(llamados, "no se encontró ninguna llamada OT2C.xxx en el código nuevo")
        faltan = sorted(n for n in llamados if not re.search(r"\b%s\s*:" % re.escape(n), exportados))
        self.assertEqual(faltan, [], "se llaman desde el HTML pero no se exportan: %s" % faltan)

    def test_sin_popups_nativos(self):
        sin_comentarios = re.sub(r"/\*.*?\*/", "", self.nuevo_js, flags=re.S)
        sin_comentarios = "\n".join(ln for ln in sin_comentarios.splitlines() if not ln.strip().startswith("//"))
        for prohibido in (r"(?<![\w.])alert\(", r"(?<![\w.])confirm\(", r"(?<![\w.])prompt\("):
            self.assertIsNone(re.search(prohibido, sin_comentarios), "REGLA #1: %s" % prohibido)

    def test_la_fila_usa_el_origen_y_el_recordar(self):
        fila = self.html[self.html.index("  function _o2mFilaEquipoHTML(m){"):self.html.index("  function pintarListaEquipos(){")]
        self.assertIn("_oefOrigenHtml(sel)", fila)
        self.assertIn("_oefRecordarFilaHtml(m)", fila)
        self.assertIn("o2m-eqrow", fila)
        self.assertIn("OT2C.comboTecla(event,", fila)
        self.assertIn("_o2mEqSalidaHtml(m.id)", fila)                                  # lo de la desinstalación sigue

    def test_la_lista_se_pinta_por_grupos_y_conserva_lo_de_siempre(self):
        lista = self.html[self.html.index("  function pintarListaEquipos(){"):self.html.index("  /* 2026-08-30 (Daniel, dictado en vivo probando el wizard")]
        self.assertIn("_oefGrupos(equiposVista)", lista)
        self.assertIn("_oefCuerpoHtml(_oefLista)", lista)
        self.assertIn("_oefBulkHtml()", lista)
        self.assertIn("_oefCargarPistas(_oefLista)", lista)
        self.assertIn("(_o2mDiTieneHija() ? 'Checklist de entrada y de salida' : 'Plantillas extra')", lista)
        self.assertIn("OT2C.diEqTodo()", lista)
        self.assertIn("OT2C.eqTogTodos()", lista)                                       # «Marcar todos» sigue (REGLA #4.2 y #14)

    def test_el_alta_automatica_pregunta_las_pistas_y_decide_por_producto(self):
        alta = self.html[self.html.index("  async function _o2mAltaAutoFaltantes(faltantes){"):self.html.index("  function _o2mAltaAutoFaltantesSiCorresponde(){")]
        self.assertIn("await _oefPistasPara(", alta)
        self.assertIn("split_to_rows: !_oefSugiereCantidad(l)", alta)
        self.assertIn("respetar_clasificacion: true", alta)                             # el candado del catálogo sigue mandando

    def test_el_encabezado_ofrece_las_dos_formas_y_el_recordar(self):
        self.assertIn("'Una fila con cantidad '", self.bloque)
        self.assertIn("'Una fila por unidad ('", self.bloque)
        self.assertIn("Recordar para este producto", self.bloque)
        self.assertIn("Checklist para todos los marcados", self.bloque)
        self.assertIn("Checklist para las ", self.bloque)
        self.assertIn("Faltan ", self.bloque)

    def test_los_tres_buscadores_tienen_roles_aria(self):
        self.assertEqual(self.bloque.count('role="combobox"'), 2)                       # grupo y barra; el de la fila vive en _o2mFilaEquipoHTML
        self.assertIn("setAttribute('role', 'listbox')", self.combo)
        self.assertIn('role="option"', self.combo)
        self.assertIn("aria-activedescendant", self.combo)

    def test_desplazar_la_propia_lista_no_la_cierra(self):
        self.assertIn("_comboListEl.contains(t)", self.combo)

    def test_tamanos_de_toque_y_de_letra(self):
        css = self.html[self.html.index("/* 2026-11-05 · FILAS DE EQUIPOS (REGLA #63)"):self.html.index("/* 2026-08-29: toggle con ícono para \"Acceso al lugar\"")]
        self.assertRegex(css, r"\.o2m-seg\{min-height:44px")
        self.assertRegex(css, r"\.o2m-gh-btn\{min-height:44px")
        self.assertIn("@media (pointer:coarse)", css)
        for tam in re.findall(r"font-size:\s*([\d.]+)(rem|px)", css):
            px = float(tam[0]) * (16 if tam[1] == "rem" else 1)
            self.assertGreaterEqual(px, 12, "REGLA #61: letra menor a 12 px (%s%s)" % tam)

    def test_los_colores_son_los_de_ilus(self):
        css = self.html[self.html.index("/* 2026-11-05 · FILAS DE EQUIPOS (REGLA #63)"):self.html.index("/* 2026-08-29: toggle con ícono para \"Acceso al lugar\"")]
        usados = set(c.lower() for c in re.findall(r"#[0-9a-fA-F]{6}\b|#[0-9a-fA-F]{3}\b", css))
        self.assertTrue({"#0a0a0a", "#16a34a"} <= usados)
        self.assertIn("var(--ilus-red,#dc2626)", css)

    def test_los_ids_de_los_buscadores_son_unicos_por_lugar(self):
        for patron in ("o2mComboInp-", "o2mComboInpG-", "o2mComboInpB"):
            self.assertIn(patron, self.html)

    def test_las_constantes_del_servidor_y_de_la_pantalla_coinciden(self):
        import ot_equipos_filas as oef
        self.assertIn("OEF_MAX_UNIDADES_SEPARAR = %d" % oef.MAX_UNIDADES_SEPARAR, self.bloque)
        self.assertIn("OEF_DIAS_ELEGIBLE = %d" % oef.MAX_DIAS_ELEGIBLE, self.bloque)
        self.assertIn("OEF_MAX_PISTAS = 80", self.bloque)

    def test_el_estado_se_reinicia_al_abrir(self):
        self.assertIn("try { _oefReiniciar(); } catch (e) {}", self.html)

    def test_la_lista_de_maquinas_trae_los_datos_para_saber_si_son_recientes(self):
        with open(os.path.join(RAIZ, "app.py"), "rb") as f:
            app = f.read().decode("utf-8")
        i = app.index("def mant_cliente_maquinas_list(cid):")
        fn = app[i:i + 3500]
        self.assertIn("created_at, origen_visita_id", fn)
        self.assertIn("except Exception as _e_cols:", fn)                              # una base sin esas columnas sigue dando la lista de siempre
        self.assertIn("estado,'activo') <> 'baja'", fn)


if __name__ == "__main__":
    unittest.main()

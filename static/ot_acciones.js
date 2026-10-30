/* ══════════════════════════════════════════════════════════════════════════════════════════════════════════════
   ot_acciones.js — Barra de acciones de cada equipo / producto de la OT y sus flujos a pantalla completa.
   2026-10-20 (Daniel, con los técnicos operando la revisión de la NVI 687 y el caso OT-2026-00171 de Aurum):
   «antes de comenzar los checklist los técnicos tengan la posibilidad de darle de baja, de dejar fuera de
   servicio, de indicar que no llegó, de solicitar piola o cualquier repuesto» · «antes de cada checklist estaba el
   menú de dar de baja, solicitar de manera inteligente repuestos y fuera de servicio. Ese menú lo requiero para
   cada equipo y poder indicar alguna incidencia» · «algo choro, algo espectacular… esta es nuestra página de orgullo».

   RENDIMIENTO: la ficha de una OT interna llega a 19 productos × 9 tareas (~1 MB de HTML). Por eso la barra NO es
   HTML repetido: cada tarjeta trae un <div class="otd-acc"> vacío y UN solo JSON (window.OTD_ACC) con lo mínimo; este
   archivo pinta la barra y crea UNA sola pantalla completa reutilizable para todos los flujos (nada de modales por
   producto). Sin proveedores ni costos (REGLA #19 y #26): solo nombres de pieza, estados y horas ya en Chile.

   Flujos: pedir repuesto (3 pasos + confirmación con seguimiento de 4 pasos) · No se puede usar · No tiene arreglo ·
   No llegó (producto) · No está / no llegó (equipo de cliente) · Informar incidencia · Mis pedidos.
   2026-10-20 (propuesta del técnico senior aprobada por Daniel): «¿Qué le pasa a este equipo?» (diagnóstico guiado de una pregunta por
   pantalla), tarjeta «Medidas» en el pedido de repuesto (qué medir de cada pieza, con dibujo y la salida «No pude medir») y foto por
   pieza clave. Las fichas y el árbol los manda el servidor (ot_piezas_medidas.py): una sola fuente de verdad.
   REGLA #1: nada de alert/confirm/prompt nativos (ilusConfirm / ilusToast / ilusPrompt).
   ══════════════════════════════════════════════════════════════════════════════════════════════════════════════ */
(function () {
  'use strict';

  var HDR = { 'X-Requested-With': 'XMLHttpRequest' };
  var MAX_FOTOS = 8;            // 2026-10-20: 1 foto general + una con huincha por cada pieza clave del pedido (antes 3 por pedido)
  var MAX_FOTO_BYTES = 8 * 1024 * 1024;
  var BORRADOR_DIAS = 7;

  /* ───────────────────────────── utilidades ───────────────────────────── */
  function cfg() { return window.OTD_ACC_CFG || {}; }
  function VID() { return cfg().vid || (typeof OTD_VID !== 'undefined' ? OTD_VID : 0); }
  function DATA() { return window.OTD_ACC || {}; }
  function LISTAS() { return window.OTD_ACC_LISTAS || {}; }
  function esc(s) {
    return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
    });
  }
  function norm(s) {
    return String(s || '').normalize('NFD').replace(/[̀-ͯ]/g, '').toLowerCase().replace(/\s+/g, ' ').trim();
  }
  function uuid() {
    var a = new Uint8Array(16);
    (window.crypto || window.msCrypto).getRandomValues(a);
    return Array.prototype.map.call(a, function (b) { return ('0' + b.toString(16)).slice(-2); }).join('');
  }
  function toast(msg, tipo) { try { if (window.ilusToast) window.ilusToast(msg, { type: tipo || 'info' }); } catch (e) { /* nada */ } }
  function sinPar(s) { return String(s || '').replace(/\s*\([^)]*\)\s*/g, ' ').replace(/\s+/g, ' ').trim(); }
  function enPar(s) { var m = /\(([^)]*)\)/.exec(String(s || '')); return m ? m[1].trim() : ''; }
  function base(d) { return '/ot/api/' + VID() + (d.k === 'i' ? '/producto/' : '/equipo/') + d.id; }

  /* ───────────────────────────── funciones PURAS del pedido de repuesto (sin DOM; probadas con Node) ─────────────────────────────
     2026-10-21 (Daniel: «práctico, ordenado, guiado, con trayectoria… algo parecido a Retiros»). Aquí viven las reglas que no dependen
     de la pantalla para poder probarlas solas: cuándo una pieza «se parece» a una baldosa, qué otros nombres viajan al servidor,
     qué ficha de medidas manda, si ya hay un pedido igual, y la trayectoria de un pedido (misma palabra para técnico y gestión).
     Las fechas SIEMPRE salen en hora Chile y en día/mes/año (REGLA #6): jamás una fecha ISO cruda. */
  var STOP = { para: 1, con: 1, del: 1, los: 1, las: 1, una: 1, uno: 1, por: 1, que: 1, sin: 1 };
  function palabras(s) {
    return norm(s).replace(/[^a-z0-9 ]+/g, ' ').split(' ').filter(function (w) { return w.length >= 3 && !STOP[w]; });
  }
  /* «poleas» y «polea» son la misma pieza; «apoya» y «apoyar» NO (antes bastaban 4 letras sueltas y salía la ficha equivocada). */
  function raizPalabra(w) {
    w = norm(w);
    if (w.length > 4 && /[^aeiou]es$/.test(w)) return w.slice(0, -2);
    if (w.length > 3 && /s$/.test(w)) return w.slice(0, -1);
    return w;
  }
  function raices(s, minLen) { return palabras(s).map(raizPalabra).filter(function (w) { return w.length >= (minLen || 3); }); }
  /* ¿Esta pieza de bodega corresponde a esta baldosa? Misma familia de dibujo, o una palabra COMPLETA (o su raíz) en común. */
  function coincidePieza(tile, desc) {
    var a = icDe(tile.full || tile.n), b = icDe(desc);
    if (a !== 'otra' && a === b) return true;
    var dw = raices(desc, 3);
    return raices(tile.n || tile.full, 4).some(function (w) { return dw.indexOf(w) >= 0; });
  }
  /* La ficha de medidas sigue a la PIEZA ELEGIDA (la que calcula el servidor desde su nombre), no a la baldosa tocada. Solo si el
     servidor no trae una ficha propia (o es la genérica «H») se usa la de la baldosa. */
  function fichaParaLinea(fichaRepuesto, fichaBaldosa, existe) {
    if (fichaRepuesto && fichaRepuesto !== 'H' && (!existe || existe(fichaRepuesto))) return fichaRepuesto;
    return fichaBaldosa || ((fichaRepuesto && (!existe || existe(fichaRepuesto))) ? fichaRepuesto : undefined);
  }
  /* Otros nombres con que el técnico conoce la pieza («perilla» = pin): lo que buscó, lo que escribió en «¿Cómo le dicen en tu empresa?»
     y lo escrito en «Otra pieza». Sin repetidos, sin el nombre canónico, máximo 6 (el servidor aprende el alias al validar). */
  function armarNombresAlt(o) {
    o = o || {};
    var canon = norm(o.canonico || ''), vistos = {}, out = [];
    function add(t) {
      var n = String(t == null ? '' : t).replace(/\s+/g, ' ').trim().slice(0, 80), k = norm(n);
      if (n.length < 2 || !k || k === canon || vistos[k] || out.length >= 6) return;
      vistos[k] = 1; out.push(n);
    }
    add(o.buscado);
    String(o.alias || '').split(/[\n;,]+/).forEach(add);
    add(o.escrito);
    return out;
  }
  /* Fechas: siempre día/mes/año hora Chile. Acepta texto ya en Chile («21/10/2026 15:03»), milisegundos o ISO CON zona horaria. */
  function fechaChile(v, soloFecha) {
    if (v == null || v === '') return '';
    if (typeof v === 'string' && /^\d{2}\/\d{2}\/\d{4}/.test(v.trim())) return soloFecha ? v.trim().slice(0, 10) : v.trim();
    var d;
    if (typeof v === 'number') d = new Date(v);
    else if (/[zZ]$|[+-]\d{2}:?\d{2}$/.test(String(v).trim())) d = new Date(String(v).trim());
    else return '';                                  // sin zona horaria no se adivina: jamás se muestra un ISO crudo
    if (isNaN(d.getTime())) return '';
    try {
      var p = {};
      new Intl.DateTimeFormat('es-CL', { timeZone: 'America/Santiago', day: '2-digit', month: '2-digit', year: 'numeric', hour: '2-digit', minute: '2-digit', hourCycle: 'h23' })
        .formatToParts(d).forEach(function (x) { p[x.type] = x.value; });
      var f = p.day + '/' + p.month + '/' + p.year;
      return soloFecha ? f : f + ' ' + p.hour + ':' + p.minute;
    } catch (e) { return ''; }
  }
  function haceTxt(ms, ahora) {
    if (typeof ms !== 'number' || !isFinite(ms)) return '';
    var m = Math.max(0, ((ahora == null ? Date.now() : ahora) - ms) / 60000);
    if (m < 1) return 'hace un momento';
    if (m < 60) return 'hace ' + Math.floor(m) + ' min';
    if (m < 1440) return 'hace ' + Math.floor(m / 60) + ' h';
    var dd = Math.floor(m / 1440);
    return 'hace ' + dd + (dd === 1 ? ' día' : ' días');
  }
  /* ¿Ya hay un pedido ABIERTO de esta pieza para este equipo? Solo sugiere (REGLA #32): la persona decide si es otra distinta. */
  function mismaPieza(a, b) {
    var na = norm(sinPar(a)), nb = norm(sinPar(b));
    if (!na || !nb) return false;
    if (na === nb) return true;
    var ia = icDe(a), ib = icDe(b);
    if (ia !== 'otra' && ia === ib) return true;
    var wb = raices(sinPar(b), 4);
    return raices(sinPar(a), 4).some(function (w) { return wb.indexOf(w) >= 0; });
  }
  function buscarDuplicado(nombre, sols) {
    var l = (sols || []).filter(function (s) { return s && s.abierta && mismaPieza(nombre, s.nombre); });
    return l.length ? l[0] : null;
  }
  var DUP_ESTADO = { solicitado: 'en revisión', validado: 'revisado por bodega', pedido: 'pedido', recibido: 'lista en bodega' };
  function dupTexto(sol, ahora) {
    var cuando = typeof sol.creada_ms === 'number' ? haceTxt(sol.creada_ms, ahora) : (fechaChile(sol.creada) ? 'el ' + fechaChile(sol.creada) : '');
    var est = DUP_ESTADO[String(sol.estado || '').toLowerCase()] || '';
    var det = [cuando, est].filter(Boolean).join(', ');
    return 'Ya hay un pedido de ' + sinPar(sol.nombre) + ' para este equipo' + (det ? ' (' + det + ')' : '') + '.';
  }
  /* Trayectoria de UN pedido: Solicitado → Revisado por bodega → Pedido (llega ~fecha) o Apartado → Lista en bodega → Instalada.
     La misma palabra para el técnico y para gestión. Las horas salen de `hitos` (por estado) si el servidor las manda; si no, no se inventan. */
  var NIVEL_ESTADO = { solicitado: 1, validado: 2, pedido: 3, recibido: 4, instalado: 5 };
  function trayectoria(x) {
    x = x || {};
    var e = String(x.estado || '').toLowerCase();
    if (e === 'rechazado') return { rechazado: true, motivo: String(x.rechazo_motivo || '').trim(), pasos: [] };
    var lvl = NIVEL_ESTADO[e] || 1, h = x.hitos || {};
    if (x.apartado && lvl < 3) lvl = 3;
    var textoConsigue = x.apartado ? 'Apartado' : ((e === 'pedido' || e === 'recibido' || e === 'instalado' || x.compra_eta) ? 'Pedido' : 'Pedido o apartado');
    var eta = (x.compra_eta && lvl < 4) ? fechaChile(x.compra_eta, true) : '';
    var def = [
      ['solicitado', 'Solicitado', h.solicitado || x.creada_ms || x.creada, ''],
      ['revisado', 'Revisado por bodega', h.validado, ''],
      ['consigue', textoConsigue, h.pedido || h.apartado, eta ? 'llega ~' + eta : ''],
      ['lista', 'Lista en bodega', h.recibido, ''],
      ['instalada', 'Instalada', h.instalado, '']
    ];
    return {
      rechazado: false, motivo: '',
      pasos: def.map(function (p, i) {
        var n = i + 1;
        return { clave: p[0], texto: p[1], estado: n <= lvl ? 'hecho' : (n === lvl + 1 ? 'actual' : 'pendiente'),
                 cuando: n <= lvl ? fechaChile(p[2]) : '', extra: p[3] };
      })
    };
  }
  function esVideoArchivo(f) { return !!f && (/^video\//.test(f.type || '') || /\.(mp4|mov|m4v|webm|3gp)$/i.test(f.name || '')); }
  var MAX_VIDEO_BYTES = 28 * 1024 * 1024;     // el servidor acepta hasta 28 MB (_OTREP_MAX_VIDEO)

  function api(method, url, body) {
    var opt = { method: method, credentials: 'same-origin', headers: Object.assign({}, HDR) };
    if (typeof FormData !== 'undefined' && body instanceof FormData) opt.body = body;
    else if (body != null) { opt.headers['Content-Type'] = 'application/json'; opt.body = JSON.stringify(body); }
    return fetch(url, opt).then(function (r) {
      return r.json().catch(function () { return {}; }).then(function (j) {
        if (!j.ok && !j.error) {
          j.error = r.status === 403 ? 'No tienes permiso para esta acción en esta OT.'
                  : r.status === 413 ? 'El archivo es demasiado grande para subirlo.'
                  : 'No se pudo completar. Intenta de nuevo.';
        }
        j._status = r.status;
        return j;
      });
    }).catch(function () { return { ok: false, error: 'Sin conexión. Revisa la señal e intenta de nuevo: lo que escribiste sigue aquí.' }; });
  }

  /* ───────────────────────────── dibujos de piezas (línea) ───────────────────────────── */
  var IC = {
    piola: '<ellipse cx="22" cy="23" rx="15" ry="10"/><ellipse cx="20" cy="22" rx="11" ry="7"/><path d="M36 27c3 3 5 7 6 11"/><circle cx="42.5" cy="40.5" r="2.6"/><path d="M8 20c-2-2-3-4-3-6"/><circle cx="5" cy="11.5" r="2.6"/>',
    pin: '<circle cx="11" cy="22" r="6.5"/><path d="M17.5 22H39"/><path d="M39 19.5l4.5 2.5L39 24.5"/><path d="M14.5 21v-2.5h3v7h-3V23"/><path d="M10 28.5c-1 6 3 11 10 11 5 0 7-3 11-3"/>',
    polea: '<circle cx="24" cy="27" r="13.5"/><circle cx="24" cy="27" r="9" stroke-dasharray="2 2.5"/><circle cx="24" cy="27" r="3"/><path d="M24 13.5V6M18 6h12"/>',
    buje: '<ellipse cx="24" cy="14" rx="13" ry="4.8"/><path d="M11 14v19c0 2.7 5.8 4.8 13 4.8s13-2.1 13-4.8V14"/><ellipse cx="24" cy="14" rx="5.5" ry="2"/><path d="M15 19v13" opacity=".5"/>',
    pad: '<rect x="6" y="12" width="36" height="24" rx="8"/><rect x="10.5" y="16.5" width="27" height="15" rx="4.5" stroke-dasharray="2 2.2"/>',
    tope: '<path d="M16 13h16l5 19H11z"/><ellipse cx="24" cy="13" rx="8" ry="2"/><path d="M24 32v9M20 41h8"/>',
    cinta: '<rect x="3" y="13" width="42" height="22" rx="11"/><rect x="10" y="19.5" width="28" height="9" rx="4.5"/><path d="M14 13.5l-1.5 3M20 13.5l-1.5 3M26 13.5l-1.5 3M32 13.5l-1.5 3M14 31.5l-1.5 3M20 31.5l-1.5 3M26 31.5l-1.5 3M32 31.5l-1.5 3" stroke-width="1.6"/>',
    tabla: '<path d="M5 29l9-12h29l-9 12z"/><path d="M5 29v4h29l9-12v-4"/><path d="M34 29v4"/>',
    motor: '<rect x="6" y="14" width="25" height="20" rx="4"/><path d="M12 14v20M18.5 14v20M25 14v20"/><path d="M31 24h5"/><circle cx="40" cy="24" r="4.2"/><path d="M11 14v-4h10v4"/>',
    consola: '<rect x="6" y="9" width="36" height="25" rx="4"/><rect x="11" y="14" width="17" height="9" rx="1.5"/><circle cx="34" cy="16.5" r="1.7"/><circle cx="34" cy="22.5" r="1.7"/><path d="M14 29h20"/><path d="M18 34v5M30 34v5"/>',
    llave: '<circle cx="13" cy="24" r="7.5"/><circle cx="13" cy="24" r="2.6"/><path d="M20.5 24c2-4.5 4 4.5 6 0s4 4.5 6 0 4 4.5 5.5 0"/><rect x="38" y="20" width="5" height="8" rx="1.2"/>',
    rodillo: '<rect x="9" y="16" width="30" height="16" rx="8"/><path d="M3 24h6M39 24h6"/><path d="M16 17v14M32 17v14" opacity=".45"/>',
    correa: '<circle cx="14" cy="24" r="8"/><circle cx="14" cy="24" r="2.5"/><circle cx="37" cy="24" r="4.5"/><path d="M14 15.5L37 19.3M14 32.5L37 28.7"/>',
    pedal: '<rect x="18" y="13" width="20" height="23" rx="4.5"/><path d="M6 24.5h12"/><path d="M22.5 19.5h11M22.5 24.5h11M22.5 29.5h11"/>',
    biela: '<circle cx="12" cy="35" r="5.5"/><circle cx="12" cy="35" r="1.8"/><circle cx="36" cy="12" r="3.8"/><path d="M16 31.5L32.8 15.2M19.5 34.5L35.5 15.8"/>',
    sillin: '<path d="M4 19c9-5 30-5 40 1-4 4.5-12 5-18 5-3 4-8 6-13 6-5 0-9-2.5-9-6 0-2.5 2-4.5 0-6z"/><path d="M23 30v10M18.5 40h9"/>',
    cuerno: '<path d="M14 40V18a6 6 0 0 1 12 0v22"/><path d="M10 40h20"/><path d="M26 22h12M26 30h12"/>',
    cable: '<path d="M6 36c8 0 6-20 14-20s6 16 14 16 6-10 8-12"/><circle cx="6" cy="36" r="2.6"/><circle cx="42" cy="20" r="2.6"/>',
    brazo: '<circle cx="11" cy="37" r="4.5"/><circle cx="36" cy="12" r="4.5"/><path d="M14 34L33 15"/><path d="M33 15l-8 1M33 15l-1 8"/>',
    disco: '<circle cx="24" cy="24" r="16"/><circle cx="24" cy="24" r="4.5"/><path d="M24 8v4M24 36v4M8 24h4M36 24h4" opacity=".5"/>',
    perno: '<path d="M16 7h16l4 7v5l-4 4H16l-4-4v-5z"/><path d="M24 23v19"/><path d="M19 29l10-3M19 35l10-3" opacity=".6"/>',
    otra: '<path d="M7 17h7.5l3-4.5h13l3 4.5H41v21H7z"/><circle cx="24" cy="26.5" r="6.5"/><path d="M36 21.5h.01"/>'
  };
  var RX_IC = [
    ['piola', /piola|guaya|chicote|cable de acero/i], ['polea', /polea|roldana/i], ['pin', /\bpin\b|perilla|pasador|clavija|seguro/i],
    ['buje', /buje|rodamiento|bocina/i], ['pad', /\bpad\b|tapiz|asiento|respaldo|cojin/i], ['tope', /tope|goma|taco|nivelador/i],
    ['cinta', /cinta|banda/i], ['tabla', /tabla|deck/i], ['motor', /motor/i], ['consola', /consola|pantalla/i],
    ['llave', /llave/i], ['rodillo', /rodillo/i], ['correa', /correa/i], ['pedal', /pedal/i], ['biela', /biela/i],
    ['sillin', /sill[ií]n/i], ['cuerno', /cuerno|porta ?disco/i], ['brazo', /brazo|manija|j-?hook/i], ['perno', /perno|tuerca|tornillo/i],
    ['disco', /\bdisco|mancuerna|collar[ií]n|\bbarra\b/i], ['cable', /cable|mosquet|agarre|manilla/i]
  ];
  function icDe(nombre) {
    var n = norm(nombre);
    for (var i = 0; i < RX_IC.length; i++) if (RX_IC[i][1].test(n)) return RX_IC[i][0];
    return 'otra';
  }
  function icSvg(k) {
    return '<svg viewBox="0 0 48 48" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">' + (IC[k] || IC.otra) + '</svg>';
  }

  /* ───────────────────────────── borradores (pedido sin terminar) ───────────────────────────── */
  function bkey(key) { return 'otdAccBor:' + VID() + ':' + key; }
  function leerBorrador(key) {
    try {
      var j = JSON.parse(localStorage.getItem(bkey(key)) || 'null');
      if (!j) return null;
      if (Date.now() - (j.t || 0) > BORRADOR_DIAS * 86400000) { localStorage.removeItem(bkey(key)); return null; }
      return j;
    } catch (e) { return null; }
  }
  function idb() {
    return new Promise(function (res) {
      try {
        var rq = indexedDB.open('ilus_ot_acciones', 1);
        rq.onupgradeneeded = function () { rq.result.createObjectStore('fotos'); };
        rq.onsuccess = function () { res(rq.result); };
        rq.onerror = function () { res(null); };
      } catch (e) { res(null); }
    });
  }
  /* 2026-10-21: el guardado local NUNCA puede frenar un envío: si IndexedDB no responde (modo privado, disco lleno…) se sigue sin él. */
  function idbOp(modo, fn) {
    return Promise.race([idbOpReal(modo, fn), new Promise(function (res) { setTimeout(function () { res(null); }, 8000); })]);
  }
  function idbOpReal(modo, fn) {
    return idb().then(function (db) {
      if (!db) return null;
      return new Promise(function (res) {
        try {
          var tx = db.transaction('fotos', modo), st = tx.objectStore('fotos'), out = fn(st);
          tx.oncomplete = function () { res(out && 'result' in out ? out.result : true); };
          tx.onerror = tx.onabort = function () { res(null); };
        } catch (e) { res(null); }
      });
    });
  }
  function borrarBorrador(key) {
    try { localStorage.removeItem(bkey(key)); } catch (e) { /* nada */ }
    var n = 0;
    return idbOp('readwrite', function (st) { for (n = 0; n < MAX_FOTOS; n++) st.delete(bkey(key) + '#' + n); st.delete(bkey(key) + '#v'); return null; });
  }
  /* 2026-10-21: si el pedido YA se envió pero faltan archivos por subir (mala señal), el borrador se conserva con `env` (lo que el servidor
     devolvió y qué archivos ya subieron) hasta que TODOS suban: así «Reintentar fotos» no pierde la foto con huincha. */
  function envMinimo(S) {
    if (!S.enviadoOk || !S.res) return null;
    var r = S.res;
    return { res: { lote_id: r.lote_id || S.loteId, numero_ticket: r.numero_ticket || '', atiende: r.atiende || '', responde_antes: r.responde_antes || '',
                    solicitudes: (r.solicitudes || []).map(function (x) { return { ref: x.ref, solicitud_id: x.solicitud_id, repuesto_nombre: x.repuesto_nombre, cantidad: x.cantidad, medida: x.medida || '' }; }) },
             ok: S.subidas || [], creada: S.creadaMs || 0 };
  }
  function guardarBorrador(S) {
    var previo = leerBorrador(S.key);
    if (previo && previo.env && !S.enviadoOk) return Promise.resolve(false);     // no pisar las fotos pendientes de un pedido anterior
    var j = { t: Date.now(), step: S.step, sel: S.sel, q: S.q, falla: S.falla, detalle: S.detalle, dejarFS: S.dejarFS, alias: S.alias || '',
              tile: S.tile, loteId: S.loteId, nFotos: S.fotos.length, refs: S.fotos.map(function (f) { return f.ref || ''; }),
              diag: S.diag || null, sug: S.sug || null, pend: S.pend || null, video: !!S.video, env: envMinimo(S) };
    try { localStorage.setItem(bkey(S.key), JSON.stringify(j)); } catch (e) { return Promise.resolve(false); }
    return idbOp('readwrite', function (st) {
      for (var i = 0; i < MAX_FOTOS; i++) {
        if (S.fotos[i]) st.put({ blob: S.fotos[i].file, name: S.fotos[i].file.name || ('foto' + i + '.jpg'), type: S.fotos[i].file.type }, bkey(S.key) + '#' + i);
        else st.delete(bkey(S.key) + '#' + i);
      }
      if (S.video) st.put({ blob: S.video.file, name: S.video.file.name || 'video.mp4', type: S.video.file.type }, bkey(S.key) + '#v');
      else st.delete(bkey(S.key) + '#v');
      return null;
    });
  }
  function cargarVideoBorrador(S) {
    return idbOp('readonly', function (st) { return st.get(bkey(S.key) + '#v'); }).then(function (r) {
      if (r && r.blob) {
        try { var f = new File([r.blob], r.name || 'video.mp4', { type: r.type || 'video/mp4' }); S.video = { file: f, url: URL.createObjectURL(f) }; } catch (e) { /* nada */ }
      }
    });
  }
  function cargarFotosBorrador(S, n, refs) {
    var tareas = [];
    for (var i = 0; i < n; i++) {
      (function (i) {
        tareas.push(idbOp('readonly', function (st) { return st.get(bkey(S.key) + '#' + i); }));
      })(i);
    }
    return Promise.all(tareas).then(function (rs) {
      rs.forEach(function (r, ix) {
        if (r && r.blob) {
          try {
            var f = new File([r.blob], r.name || 'foto.jpg', { type: r.type || 'image/jpeg' });
            S.fotos.push({ file: f, url: URL.createObjectURL(f), ref: (refs && refs[ix]) || '' });
          } catch (e) { /* nada */ }
        }
      });
    });
  }

  /* ───────────────────────────── la barra de cada tarjeta ───────────────────────────── */
  function banner(clase, titulo, icono, porque, desde, riesgo, acciones) {
    return '<div class="oa-fsb ' + clase + '" role="status"><span class="ic"><i class="bi bi-' + icono + '"></i></span><div>' +
      '<b>' + esc(titulo) + '</b>' + (porque ? '<span class="why">' + esc(porque) + '</span>' : '') +
      (desde ? '<span class="since">' + esc(desde) + '</span>' : '') +
      (riesgo ? '<span class="risk"><i class="bi bi-exclamation-triangle-fill"></i> Puede lastimar a alguien</span>' : '') +
      (acciones || '') + '</div></div>';
  }
  function btnAccion(accion, key, icono, texto, sub, clase) {
    return '<button type="button" class="oa-btn ' + (clase || 'line') + '" data-a="' + accion + '" data-key="' + esc(key) + '"><i class="bi bi-' + icono + '"></i>' +
      (sub ? '<span>' + esc(texto) + '<small>' + esc(sub) + '</small></span>' : esc(texto)) + '</button>';
  }
  function desdeTxt(cuando, por) {
    return (cuando ? 'desde ' + cuando : '') + (por ? (cuando ? ' · ' : '') + por : '');
  }

  /* 🧩 2026-10-23 (Daniel, OT-2026-00348, Isabel en terreno: «los accesorios deben solo colocar si llegó o no y el formulario chico de foto y
     descripción opcional, nada más, ya que los accesorios no se instalan»): un ACCESORIO (clase «accesorio» del Catálogo; el servidor manda
     `acc`) no lleva «Pedir repuesto», «¿Qué le pasa?» ni «Más acciones»: solo UNA pregunta, «¿Llegó?». «Sí, llegó» abre el formulario chico
     (foto y descripción, ambas opcionales); «No llegó» es la salida de siempre («No llegó · falta stock»). Una vez contestado, la barra dice qué se
     respondió y deja cambiarlo. */
  function accHTML(key, d) {
    var p = d.p || {}, ll = d.llego || null, np = d.np || null, out = [];
    if (ll) {
      out.push('<div class="oa-stbar"><span class="oa-st green"><i class="bi bi-check-circle-fill"></i> ' + esc(ll.chip || 'Llegó') + '</span></div>');
      if (ll.detalle) out.push('<div class="oa-note"><i class="bi bi-chat-left-text"></i><span>' + esc(ll.detalle) + '</span></div>');
      if (p.llego || p.noe) {
        out.push('<div class="oa-grid">' +
          (p.llego ? '<button type="button" class="oa-btn line" data-a="abrir" data-f="llego" data-key="' + esc(key) + '"><i class="bi bi-camera"></i> Agregar foto o descripción</button>' : '') +
          (p.noe ? '<button type="button" class="oa-btn ghost" data-a="accnollego" data-key="' + esc(key) + '"><i class="bi bi-box-seam"></i> Mejor marcar que no llegó</button>' : '') + '</div>');
      }
      return out.join('');
    }
    if (np) {
      var quitar = p.rev ? ' <button type="button" class="oa-lnk muted" data-a="quitarnp" data-key="' + esc(key) + '" style="min-height:30px;font-size:13.5px"><i class="bi bi-arrow-counterclockwise"></i> Quitar la marca</button>' : '';
      out.push('<div class="oa-stbar"><span class="oa-st ' + (np.estado === 'falta_stock' ? 'amber' : 'gray') + '"><i class="bi bi-box-seam"></i> ' + esc(np.chip) + '</span>' + quitar + '</div>');
      return out.join('');
    }
    if (!p.llego && !p.noe) return '<div class="oa-stbar"><span class="oa-st gray"><i class="bi bi-hourglass-split"></i> Falta indicar si llegó</span></div>';
    out.push('<div class="oa-lbl"><i class="bi bi-box-seam"></i> ¿Llegó este accesorio?</div><div class="oa-grid">' +
      (p.llego ? '<button type="button" class="oa-btn blue oa-primary" data-a="abrir" data-f="llego" data-key="' + esc(key) + '"><i class="bi bi-check-circle-fill"></i> Sí, llegó</button>' : '') +
      (p.noe ? '<button type="button" class="oa-btn line" data-a="accnollego" data-key="' + esc(key) + '"><i class="bi bi-x-circle"></i> No llegó</button>' : '') + '</div>');
    return out.join('');
  }

  function barHTML(key, d) {
    if (d.acc && d.k !== 'i') return accHTML(key, d);
    var p = d.p || {}, out = [], inter = d.k === 'i';
    var fs = false, bj = false, nlTotal = false, nl = false, np = null;

    if (inter && d.est) {
      var e = d.est;
      fs = e.estado === 'fuera_servicio'; bj = e.estado === 'baja'; nl = e.estado === 'no_llego';
      nlTotal = nl && !e.parcial;
      var rev = p.rev ? '<div class="act">' + btnAccion('revertir', key, 'arrow-counterclockwise', 'Volver a operativo', '', 'ghost') + '</div>' : '';
      if (fs) out.push(banner('', 'Fuera de servicio', 'sign-stop-fill', [e.motivo, e.detalle].filter(Boolean).join(': '), desdeTxt(e.cuando, e.por), e.riesgo_lesion, rev));
      else if (bj) out.push(banner('baja', 'Dado de baja', 'slash-circle-fill', [e.motivo, e.detalle].filter(Boolean).join(': '), desdeTxt(e.cuando, e.por), false, rev));
      else if (nl) out.push(banner('nl', 'No llegó', 'box-seam-fill', [e.resumen && e.resumen.replace(/^NO LLEGÓ( · )?/, ''), e.detalle].filter(Boolean).join(' · '), desdeTxt(e.cuando, e.por), false, rev));
    }
    if (!inter) {
      fs = !!d.fsb; bj = !!d.bj; np = d.np || null;
      if (fs) {
        var f = d.fs || {};
        var rv = p.fs ? '<div class="act">' + btnAccion('reactivar', key, 'arrow-counterclockwise', 'Volver a operativo', '', 'ghost') + '</div>' : '';
        out.push(banner('', 'Fuera de servicio', 'sign-stop-fill', f.motivo || '', desdeTxt(f.desde, f.por), f.riesgo, rv));
      }
      if (bj) out.push(banner('baja', 'Dado de baja en esta OT', 'slash-circle-fill', 'Salió del parque del cliente. Un administrador puede restaurarlo desde la ficha del cliente.', '', false, ''));
      // 2026-10-21: lo que antes avisaba el bloque «Fuera de lo normal» de abajo (se quitó por duplicado, Daniel).
      if (d.sal && !fs && !bj) out.push(banner('nl', 'No se pudo revisar', 'skip-forward-fill', d.sal, '', false, ''));
      if (d.adv && !fs && !bj && !d.sal) out.push('<div class="oa-stbar"><span class="oa-st amber"><i class="bi bi-exclamation-triangle-fill"></i> Quedó con advertencia en una visita anterior</span></div>');
      if (np) {
        var quitar = p.rev ? ' <button type="button" class="oa-lnk muted" data-a="quitarnp" data-key="' + esc(key) + '" style="min-height:30px;font-size:13.5px"><i class="bi bi-arrow-counterclockwise"></i> Quitar la marca</button>' : '';
        out.push('<div class="oa-stbar"><span class="oa-st ' + (np.estado === 'falta_stock' ? 'amber' : 'gray') + '"><i class="bi bi-box-seam"></i> ' + esc(np.chip) + '</span>' + quitar + '</div>');
        // 2026-10-21 (A9): quien marcó «No pude revisarlo» y después descubre que SÍ falta una pieza no queda sin camino: avisa a gestión con el
        // flujo de siempre (Informar incidencia, con foto). Solo se ofrece si ese flujo está permitido.
        if (p.inc) out.push('<div class="oa-stbar"><button type="button" class="oa-lnk" data-a="avisarpieza" data-key="' + esc(key) + '"><i class="bi bi-megaphone"></i> Avisar a gestión que sí hay una pieza que pedir</button></div>');
      }
    }

    var borr = leerBorrador(key);
    if (borr && borr.env && p.ped) {
      // El pedido YA se envió; solo faltan archivos por subir (mala señal): se conservan en el teléfono hasta que suban todos.
      var faltan = ((borr.env.ok || []).filter(function (x) { return !x; })).length;
      out.push('<div class="oa-draft"><div class="t"><i class="bi bi-cloud-arrow-up"></i><div>Tu pedido ya está enviado' + (faltan ? ' · faltan ' + faltan + (faltan === 1 ? ' archivo' : ' archivos') + ' por subir' : '') +
        '<small>Las fotos siguen guardadas en este teléfono.</small></div></div><div class="b">' +
        btnAccion('seguir', key, 'arrow-repeat', 'Reintentar fotos', '', 'blue') + btnAccion('borrar', key, 'trash3', 'Descartar', '', 'ghost') + '</div></div>');
    } else if (borr && p.ped) {
      var nn = (borr.sel || []).length;
      out.push('<div class="oa-draft"><div class="t"><i class="bi bi-pencil-square"></i><div>Pedido sin terminar' + (nn ? ' · ' + nn + (nn === 1 ? ' pieza' : ' piezas') : '') +
        '<small>Quedó guardado en este teléfono.</small></div></div><div class="b">' +
        btnAccion('seguir', key, 'arrow-right', 'Seguir', '', 'blue') + btnAccion('borrar', key, 'trash3', 'Borrar', '', 'ghost') + '</div></div>');
    }

    var ab = (d.sol || []).filter(function (s) { return s.abierta; });
    if (ab.length) {
      var s0 = ab[0];
      out.push('<button type="button" class="oa-trk" data-a="seg" data-key="' + esc(key) + '"><i class="bi bi-hourglass-split"></i><span>' +
        (ab.length === 1 ? 'Esperando repuesto' : ab.length + ' pedidos en curso') + ' · ' + esc(s0.nombre || '') + (ab.length > 1 ? ' y ' + (ab.length - 1) + ' más' : '') +
        '</span><span class="go">Ver <i class="bi bi-chevron-right"></i></span></button>');
    }

    var puedeCta = p.ped && !bj && !nlTotal && !np;
    if (puedeCta) {
      out.push('<button type="button" class="oa-btn blue oa-primary" data-a="abrir" data-f="pedir" data-key="' + esc(key) + '"><i class="bi bi-wrench-adjustable"></i> Pedir repuesto' +
        '<span class="oa-eqn">· ' + esc(sinPar(d.n)) + '</span></button>');
    }
    // 2026-10-21 (Daniel: «agregar poder ver el manual del equipo y las piolas… que los técnicos vean las OT como una ayuda»):
    // el manual del modelo y sus piolas (o la cinta de una trotadora) a un toque, sin tener que empezar un pedido.
    var ay = [], vivo = !bj && !nlTotal && !np;
    if (p.man) ay.push(['manual', 'file-earmark-pdf', 'Manual', inter ? 'Del producto y sus repuestos' : 'Del equipo y sus repuestos']);
    if (d.pio && p.ped && vivo) ay.push(['piola', 'link-45deg', 'Piolas', 'Las del modelo y pedir cambio']);
    if (d.cin && p.ped && vivo) ay.push(['cinta', 'arrow-left-right', 'Cinta', 'Pedir cambio de cinta']);
    // 2026-10-21 (M2): en el CELULAR «Pedir repuesto» y «¿Qué le pasa a este equipo?» quedan a la vista y lo demás (ayuda y desviaciones) se
    // pliega detrás de «Más acciones». Es el MISMO contenido: nada se quita (REGLA #4.2). En computador todo se ve como antes.
    var g = [];
    if (p.fs && !fs && !bj && !nlTotal && !np) g.push(['fs', 'sign-stop', 'No se puede usar', 'Déjalo fuera de servicio']);
    if (p.baja && !bj && !nlTotal && !np) g.push(['baja', 'slash-circle', 'No tiene arreglo', inter ? 'Dar de baja' : 'Dar de baja del cliente']);
    if (inter && p.nol && !fs && !bj && !nl) g.push(['nollego', 'box-seam', 'No llegó', 'No vino en el despacho']);
    if (!inter && p.noe && !fs && !bj && !np) g.push(['noesta', 'box-seam', 'No está / no llegó', 'En el lugar o por falta de stock']);
    if (p.inc && !bj) g.push(['incidencia', 'megaphone', 'Informar incidencia', 'Con foto, para bodega']);
    if (!inter && p.salt && !d.sal && !fs && !bj && !np) g.push(['saltar', 'skip-forward', 'No pude revisarlo', 'Queda escrito por qué', 'saltar']);
    var puedeDiag = !bj && !nlTotal && !np && (p.ped || g.length > 0);
    var plg = (puedeCta || puedeDiag) && (ay.length + g.length) > 0 ? ' oa-plg' : '';
    if (ay.length) {
      out.push('<div class="oa-lbl oa-lbl-ay' + plg + '"><i class="bi bi-life-preserver"></i> Ayuda</div><div class="oa-grid oa-ayuda' + plg + '">' + ay.map(function (x, i) {
        return '<button type="button" class="oa-btn line' + (ay.length % 2 === 1 && i === ay.length - 1 ? ' wide' : '') + '" data-a="' + x[0] + '" data-f="' + x[0] + '" data-key="' + esc(key) + '" title="' + esc(x[3]) + '">' +
          '<i class="bi bi-' + x[1] + '"></i><span>' + esc(x[2]) + '<small>' + esc(x[3]) + '</small></span></button>';
      }).join('') + '</div>');
    }
    // 2026-10-20: «¿Qué le pasa a este equipo?» va PRIMERO en «Fuera de lo normal», sin quitar ninguno de los otros botones; el pulso se
    // queda en «Pedir repuesto». En el computador es un botón compacto en la misma línea; en el celular sigue ancho y cómodo para el dedo.
    if (g.length || puedeDiag) {
      out.push('<div class="oa-lbl"><i class="bi bi-exclamation-diamond-fill"></i> Fuera de lo normal</div>' +
        (puedeDiag ? '<button type="button" class="oa-btn oa-diag" data-a="abrir" data-f="diag" data-key="' + esc(key) + '" title="Te ayudo a decidir qué hacer">' +
          '<i class="bi bi-clipboard2-pulse"></i><span>¿Qué le pasa a ' + (inter ? 'este producto' : 'este equipo') + '?<small>Te ayudo a decidir qué hacer</small></span></button>' : '') +
        (g.length ? '<div class="oa-grid' + plg + '">' + g.map(function (x, i) {
          return '<button type="button" class="oa-btn line' + (g.length % 2 === 1 && i === g.length - 1 ? ' wide' : '') + '" data-a="' + (x[4] || 'abrir') + '" data-f="' + x[0] + '" data-key="' + esc(key) + '" title="' + esc(x[3]) + '">' +
            '<i class="bi bi-' + x[1] + '"></i><span>' + esc(x[2]) + '<small>' + esc(x[3]) + '</small></span></button>';
        }).join('') + '</div>' : ''));
    }
    if (plg) {
      var abierto = !!MAS_ABIERTO[key];
      out.push('<button type="button" class="oa-btn line oa-masbtn" data-a="masacc" data-key="' + esc(key) + '" aria-expanded="' + abierto + '"><i class="bi bi-' + (abierto ? 'chevron-up' : 'three-dots') + '"></i> ' +
        (abierto ? 'Menos acciones' : 'Más acciones <span class="oa-masn">' + (ay.length + g.length) + '</span>') + '</button>');
    }
    return out.join('');
  }
  var MAS_ABIERTO = {};          // qué tarjetas tienen desplegado «Más acciones» (solo importa en el celular)
  /* El pulso de «Pedir repuesto» es solo para la PRIMERA tarjeta pendiente: si late en todas, deja de significar «empieza aquí». */
  function marcarPulso() {
    var primeros = document.querySelectorAll('.otd-acc .oa-primary'), ya = false;
    Array.prototype.forEach.call(primeros, function (b) {
      var card = b.closest('.otd-acc'), libre = !ya && card && !card.querySelector('.oa-trk');
      b.classList.toggle('oa-pulso', !!libre);
      if (libre) ya = true;
    });
  }
  function pintar(key) {
    var d = DATA()[key], el = document.getElementById('otdAcc-' + key);
    if (d && el) { el.innerHTML = barHTML(key, d); el.classList.toggle('oa-mas-open', !!MAS_ABIERTO[key]); marcarPulso(); }
  }
  function pintarTodo() { Object.keys(DATA()).forEach(pintar); marcarPulso(); }

  /* ───────────────────────────── pantalla completa ───────────────────────────── */
  var root = null, win = null, S = null, histPuesto = false, overflowPrev = '';

  function asegurarRoot() {
    if (root) return;
    root = document.createElement('div');
    root.className = 'oa-fs'; root.id = 'oaFs'; root.hidden = true;
    root.setAttribute('role', 'dialog'); root.setAttribute('aria-modal', 'true'); root.setAttribute('aria-label', 'Acciones del equipo');
    root.innerHTML = '<div class="oa-win" id="oaWin" tabindex="-1"><div id="oaTop"></div><div class="oa-body" id="oaBody"></div><div id="oaFoot"></div></div>' +
      '<input type="file" id="oaCam" accept="image/*" capture="environment" hidden>' +
      '<input type="file" id="oaGal" accept="image/*" multiple hidden>' +
      '<input type="file" id="oaVid" accept="video/*" capture="environment" hidden>';
    document.body.appendChild(root);
    win = root.querySelector('#oaWin');
    root.addEventListener('click', onClick);
    root.addEventListener('input', onInput);
    root.addEventListener('change', onChange);
    // las tarjetas de pieza son <div role="button">: Enter y Espacio las activan igual que un toque
    root.addEventListener('keydown', function (ev) {
      var t = ev.target;
      if ((ev.key === 'Enter' || ev.key === ' ') && t && t.matches && t.matches('[role="button"][data-a]')) { ev.preventDefault(); t.click(); }
    });
    document.addEventListener('keydown', function (ev) { if (ev.key === 'Escape' && root && !root.hidden && !document.querySelector('.ilus-overlay')) salir(false); });
    window.addEventListener('popstate', function () {
      if (root && !root.hidden) { histPuesto = false; salir(true); }
    });
  }
  var fondoBloqueado = false;
  function bloquear(si) {
    if (si) {
      if (!fondoBloqueado) { overflowPrev = document.documentElement.style.overflow; fondoBloqueado = true; }   // abrir dos veces seguidas no deja la página trabada
      document.documentElement.style.overflow = 'hidden';
    } else { document.documentElement.style.overflow = overflowPrev || ''; fondoBloqueado = false; }
  }
  function liberarFotos(s) { (s && s.fotos || []).forEach(function (f) { try { URL.revokeObjectURL(f.url); } catch (e) { /* nada */ } }); }

  function render(reiniciarScroll) {
    if (!S) return;
    var f = FLUJO[S.flow], h = f.vista(S);
    var body = document.getElementById('oaBody'), top = body ? body.scrollTop : 0;
    document.getElementById('oaTop').innerHTML = h.top;
    body.innerHTML = h.body;
    document.getElementById('oaFoot').innerHTML = '<div class="oa-foot">' + h.foot + '</div>';
    body.scrollTop = reiniciarScroll ? 0 : top;
  }
  function marco() {            // solo arriba y abajo (al escribir: no se pierde el foco del campo)
    if (!S) return;
    var h = FLUJO[S.flow].vista(S);
    document.getElementById('oaTop').innerHTML = h.top;
    document.getElementById('oaFoot').innerHTML = '<div class="oa-foot">' + h.foot + '</div>';
  }

  function abrir(flow, key, extra) {
    var d = DATA()[key];
    if (!d || !FLUJO[flow]) return;
    asegurarRoot();
    if (S) liberarFotos(S);
    var sem = extra && extra.semilla, ex = Object.assign({}, extra || {});
    delete ex.semilla;
    S = Object.assign({ flow: flow, key: key, d: d, step: 1, fotos: [], dirty: false, enviando: false, err: '', f: {} }, ex);
    S.sem = sem || null;
    if (FLUJO[flow].init) FLUJO[flow].init(S);
    // 2026-10-20: lo que sugirió «¿Qué le pasa?» (la pieza, el motivo…) se aplica DESPUÉS de init(): init() borra lo que trae `extra`.
    // Es una sugerencia a la vista: la persona la cambia o la confirma, nada se crea ni se marca solo (REGLA #32).
    if (sem && FLUJO[flow].semilla) FLUJO[flow].semilla(S, sem);
    root.hidden = false; bloquear(true);
    if (!histPuesto) { try { history.pushState({ oaSheet: 1 }, ''); histPuesto = true; } catch (e) { /* nada */ } }
    render(true);
    setTimeout(function () { if (win) win.focus(); }, 30);
  }
  function cerrar(vinoDeAtras) {
    if (!root || root.hidden) return;
    liberarFotos(S); S = null; root.hidden = true; bloquear(false);
    if (histPuesto && !vinoDeAtras) { histPuesto = false; try { history.back(); } catch (e) { /* nada */ } }
    else histPuesto = false;
  }
  function hayAvance(s) {
    if (s.flow === 'pedir') return (s.sel && s.sel.length) || s.fotos.length || s.falla || s.detalle;
    return s.dirty;
  }
  function salir(vinoDeAtras) {
    if (!S) return;
    var key = S.key;
    if (S.flow === 'pedir' && S.step !== 'fin' && hayAvance(S)) {
      guardarBorrador(S).then(function (ok) {
        cerrar(vinoDeAtras); pintar(key);
        if (ok === false) toast('Primero termina de subir las fotos pendientes de tu pedido anterior.', 'warning');
        else toast('Guardado en este teléfono. Lo retomas desde la tarjeta.', 'success');
      });
      return;
    }
    if (S.step !== 'fin' && S.dirty && S.flow !== 'pedir' && S.flow !== 'seg') {
      window.ilusConfirm({ title: '¿Salir sin guardar?', message: 'Lo que cargaste en esta pantalla se va a perder.', okLabel: 'Salir', cancelLabel: 'Seguir aquí', danger: true })
        .then(function (ok) {
          if (ok) cerrar(vinoDeAtras);
          else if (vinoDeAtras) { try { history.pushState({ oaSheet: 1 }, ''); histPuesto = true; } catch (e) { /* nada */ } }
        });
      return;
    }
    cerrar(vinoDeAtras);
    pintar(key);
  }
  /* Salir al modal de siempre (vista clásica / manual) sin perder lo que se llevaba escrito. */
  function irAlClasico(modo, tab) {
    var s = S, key = s.key, d = s.d, mid = d.k === 'i' ? 'i' + d.id : d.id;
    var seguir = function () { cerrar(false); pintar(key); if (window.otdRepAbrir) window.otdRepAbrir(mid, modo, tab); };
    if (s.flow === 'pedir' && s.step !== 'fin' && hayAvance(s)) guardarBorrador(s).then(seguir); else seguir();
  }
  function recargar(key) {
    try { sessionStorage.setItem('otdAccVolver', key); } catch (e) { /* nada */ }
    location.reload();
  }

  /* ───────────────────────────── piezas de HTML reutilizables ───────────────────────────── */
  function topHTML(o) {
    var d = S.d, meta = [d.serie ? 'S/N ' + d.serie : '', d.sku ? 'SKU ' + d.sku : ''].filter(Boolean).join(' · ');
    var pasos = '';
    if (o.pasos) {
      pasos = '<div class="oa-steps">' + o.pasos.map(function (t, i) {
        var n = i + 1, cl = n < o.cur ? 'done' : (n === o.cur ? 'now' : '');
        return '<div class="oa-fsst ' + cl + '"><span class="c">' + (n < o.cur ? '<i class="bi bi-check-lg"></i>' : n) + '</span>' + esc(t) + '</div>';
      }).join('') + '</div>';
    }
    var prog = '';
    if (o.prog) {
      var pc = Math.round(100 * o.prog.ok / o.prog.n);
      prog = '<div class="oa-prog"><div class="l"><span>' + o.prog.ok + ' de ' + o.prog.n + ' listo</span><span>' + (o.prog.falta ? 'Falta: ' + esc(o.prog.falta) : '¡Todo listo!') + '</span></div>' +
        '<div class="bar"><span class="' + (o.prog.ok === o.prog.n ? 'full' : '') + '" style="width:' + pc + '%"></span></div></div>';
    }
    return '<div class="oa-top"><div class="oa-r1">' +
      (o.atras ? '<button type="button" class="oa-back" data-a="atras"><i class="bi bi-arrow-left"></i> Atrás</button>' : '') +
      '<span class="oa-eb">' + esc(o.eyebrow) + '</span>' +
      '<button type="button" class="oa-exit" data-a="salir"><i class="bi bi-box-arrow-left"></i> Salir a la OT</button></div>' +
      '<div class="oa-eq"><span class="oa-ph">' + (d.img ? '<img src="' + esc(d.img) + '" alt="">' : '<i class="bi bi-image"></i>') + '</span>' +
      '<div><div class="oa-nm">' + esc(d.n) + '</div>' + (meta ? '<div class="oa-mt">' + esc(meta) + '</div>' : '') + '</div></div>' +
      pasos + prog + (o.guarda ? '<div class="oa-save"><i class="bi bi-phone"></i> Se guarda en este teléfono: puedes salir y volver.</div>' : '') + '</div>';
  }
  function num(ok, n) { return ok ? '<i class="bi bi-check-lg"></i>' : n; }
  function sc(id, ok, n, titulo, badge, cuerpo, clase) {
    return '<div class="oa-sc ' + (ok ? 'ok ' : '') + (clase || '') + '" id="' + id + '"><div class="oa-sc-h"><span class="oa-sc-c" data-n="' + n + '">' + num(ok, n) + '</span><span class="oa-sc-t">' + titulo + '</span>' +
      (badge ? '<span class="oa-badge' + (badge === 'Opcional' ? ' g' : '') + '">' + badge + '</span>' : '') + '</div>' + cuerpo + '</div>';
  }
  function errHTML() { return S.err ? '<div class="oa-urg red" role="alert"><i class="bi bi-exclamation-octagon-fill"></i><span>' + esc(S.err) + '</span></div>' : ''; }
  function faltaBtn(txt, ir) {
    return '<button type="button" class="oa-btn block lg" style="background:var(--oa-amber-bg);color:var(--oa-amber-tx);border:2px dashed var(--oa-amber)" data-a="ir" data-go="' + ir + '">' +
      '<i class="bi bi-arrow-up-circle-fill"></i> Falta ' + esc(txt) + ' · tócame</button>';
  }
  function thumbs(ref) {        // sin `ref`: las fotos generales del pedido; con `ref`: las de ESA pieza
    var vis = S.fotos.map(function (f, i) { return { f: f, i: i }; }).filter(function (x) {
      return ref == null ? (!x.f.ref || !S.propiasAct) : x.f.ref === ref;
    });
    if (!vis.length) return '';
    return '<div class="oa-thumbs">' + vis.map(function (x) {
      return '<div class="oa-thumb"><img src="' + x.f.url + '" alt="Foto ' + (x.i + 1) + '"><span class="ok"><i class="bi bi-check-lg"></i></span>' +
        '<button type="button" class="x" data-a="quitarfoto" data-i="' + x.i + '" aria-label="Quitar la foto ' + (x.i + 1) + '"><i class="bi bi-x-lg"></i></button></div>';
    }).join('') + '</div>';
  }
  function mediaCuerpo(ayuda, conVideo) {
    var lleno = S.fotos.length >= MAX_FOTOS;
    return (ayuda ? '<div class="oa-hint" style="margin:-4px 0 10px"><i class="bi bi-info-circle"></i><span>' + esc(ayuda) + '</span></div>' : '') +
      '<div class="oa-media"><button type="button" class="oa-btn blue" data-a="cam"' + (lleno ? ' disabled' : '') + '><i class="bi bi-camera-fill"></i> ' + (S.fotos.length ? 'Otra foto' : 'Tomar foto') + '</button>' +
      '<button type="button" class="oa-btn line" data-a="gal"' + (lleno ? ' disabled' : '') + '><i class="bi bi-images"></i> De la galería</button></div>' + thumbs() +
      (conVideo ? videoCuerpo() : '');
  }
  /* 2026-10-21 (A5): el video SUMA y no reemplaza a la foto (regla de Daniel: la foto es obligatoria). Opcional, hasta 28 MB (10 a 20 segundos). */
  function videoCuerpo() {
    if (S.video) {
      return '<div class="oa-vid"><video src="' + esc(S.video.url) + '" muted playsinline preload="metadata" aria-label="Video del pedido"></video><div><b><i class="bi bi-check-circle-fill"></i> Video listo</b>' +
        '<small>' + esc(S.video.file.name || 'video') + ' · ' + Math.max(1, Math.round(S.video.file.size / 1048576)) + ' MB</small>' +
        '<button type="button" class="oa-lnk muted" data-a="quitarvideo"><i class="bi bi-trash3"></i> Quitar el video</button></div></div>' +
        (S.fotos.length ? '' : '<div class="oa-av" role="status"><i class="bi bi-exclamation-triangle-fill"></i><span>El video solo no basta: saca también una foto de la pieza.</span></div>');
    }
    return '<div class="oa-vid-add"><button type="button" class="oa-btn line block" data-a="camvid"><i class="bi bi-camera-video-fill"></i> Grabar un video corto <span class="oa-opc">(opcional)</span></button>' +
      '<small>De 10 a 20 segundos y con sonido, sobre todo si hace ruido o se traba. No reemplaza la foto.</small></div>';
  }
  function chipsHTML(lista, actual, accion) {
    return '<div class="oa-chips">' + lista.map(function (m) {
      var on = actual === m[0];
      return '<button type="button" class="oa-ch' + (on ? ' on' : '') + '" aria-pressed="' + on + '" data-a="' + accion + '" data-v="' + esc(m[0]) + '">' +
        (on ? '<i class="bi bi-check-lg"></i>' : '') + esc(m[1]) + '</button>';
    }).join('') + '</div>';
  }
  function textoHTML(nombre, valor, ph, min, extra) {
    var letras = String(valor || '').replace(/\s/g, '').length;
    return '<textarea class="oa-inp" data-in="' + nombre + '" placeholder="' + esc(ph) + '" rows="3" maxlength="1000" aria-label="' + esc(ph) + '">' + esc(valor || '') + '</textarea>' +
      (min ? '<div class="oa-cnt' + (letras >= min ? ' ok' : '') + '">' + (letras >= min ? '<i class="bi bi-check-lg"></i> Bien' : letras + ' de ' + min + ' letras como mínimo') + '</div>' : '') + (extra || '');
  }
  function opcion(on, tono, icono, titulo, sub, accion, v, ancho) {
    return '<button type="button" class="oa-o2' + (on ? ' on ' + tono : '') + (ancho ? ' wide' : '') + '" aria-pressed="' + on + '" data-a="' + accion + '" data-v="' + esc(v) + '">' +
      (on ? '<span class="ck"><i class="bi bi-check-lg"></i></span>' : '') + '<i class="bi bi-' + icono + '"></i><b>' + esc(titulo) + '</b><small>' + esc(sub) + '</small></button>';
  }
  function pasosPedido(seg) {
    var txt = ['Tu pedido', 'Revisión de bodega', 'Pieza lista en bodega', 'Pieza instalada'];
    var pasos = (seg && seg.pasos) || [{ estado: 'hecho' }, { estado: 'actual' }, { estado: 'pendiente' }, { estado: 'pendiente' }];
    return '<ol class="oa-stps">' + pasos.map(function (p, i) {
      var cl = p.estado === 'hecho' ? 'done' : (p.estado === 'actual' ? 'now' : '');
      return '<li class="oa-stp ' + cl + (seg && seg.rechazado ? ' no' : '') + '">' + esc(txt[i] || p.texto) + '</li>';
    }).join('') + '</ol>';
  }
  /* Trayectoria de un pedido, con fecha y hora Chile (REGLA #6). Los datos salen de `trayectoria()`; lo que el servidor aún no manda no se inventa. */
  function trayectoriaHTML(x) {
    var t = trayectoria(x);
    if (t.rechazado) {
      return '<div class="oa-note oa-rech"><i class="bi bi-x-circle"></i><span><b>Este pedido no se va a pedir.</b>' +
        (t.motivo ? ' ' + esc(t.motivo) : '') + '</span></div>';
    }
    return '<ol class="oa-tray">' + t.pasos.map(function (p) {
      return '<li class="oa-tr ' + (p.estado === 'hecho' ? 'done' : (p.estado === 'actual' ? 'now' : '')) + '"><span class="oa-tr-t">' + esc(p.texto) +
        (p.extra ? ' <em>' + esc(p.extra) + '</em>' : '') + '</span>' +
        (p.cuando ? '<span class="oa-tr-w">' + esc(p.cuando) + '</span>' : (p.estado === 'actual' ? '<span class="oa-tr-w">ahora</span>' : '')) + '</li>';
    }).join('') + '</ol>';
  }
  /* «Lo atiende» y «Responde antes de»: SOLO si el servidor ya los manda (dueño y reloj de respuesta). Nunca se inventan. */
  function atiendeHTML(x) {
    if (!x) return '';
    var antes = x.responde_antes ? fechaChile(x.responde_antes) : '';
    if (!x.atiende && !antes) return '';
    return '<div class="oa-atn">' + (x.atiende ? '<div><i class="bi bi-person-check"></i><span>Lo atiende: <b>' + esc(x.atiende) + '</b></span></div>' : '') +
      (antes ? '<div><i class="bi bi-alarm"></i><span>Responde antes de: <b>' + esc(antes) + '</b></span></div>' : '') + '</div>';
  }
  /* Aviso de duplicado (M3): sugiere, no bloquea. Solo para las piezas de este pedido que aún no se marcaron como «otra distinta». */
  function dupHTML(s) {
    var out = '';
    (s.sel || []).forEach(function (l, i) {
      if (l.otraDistinta || !l.nombre) return;
      var sol = buscarDuplicado(l.nombre, (s.d && s.d.sol) || []);
      if (!sol) return;
      out += '<div class="oa-dup" role="status"><i class="bi bi-files"></i><div><b>' + esc(dupTexto(sol)) + '</b><span>¿Es el mismo? Mírala antes de pedirla otra vez.</span>' +
        '<div class="act"><button type="button" class="oa-btn line" data-a="dupver">Ver</button><button type="button" class="oa-btn ghost" data-a="dupotra" data-i="' + i + '">Es otra distinta</button></div></div></div>';
    });
    return out;
  }
  var TXT_NO_HAY = 'No hay en bodega: la pide gestión';   // 2026-10-21 (M10): antes «Hay que pedirla» y parecía que la compraba el técnico
  function stockChip(r) {
    if (!r) return '<span class="oa-st gray"><span class="oa-dot x"></span>Pídela con una foto</span>';
    var s = r.semaforo;
    if (s === 'verde') {
      var n = (typeof r.disponible === 'number' && !cfg().externo) ? Math.floor(r.disponible) : null;
      return '<span class="oa-st green"><span class="oa-dot g"></span>' + (n ? n + ' en bodega' : 'Hay en bodega') + '</span>';
    }
    if (s === 'ambar') return '<span class="oa-st amber"><span class="oa-dot a"></span>' + TXT_NO_HAY + '</span>';
    return '<span class="oa-st gray"><span class="oa-dot x"></span>' + TXT_NO_HAY + '</span>';
  }
  function finGenerico(o) {
    return '<div class="oa-fin"><div class="oa-done-ic ' + (o.tono || '') + '"><i class="bi bi-' + o.icono + '"></i></div><h2>' + esc(o.h) + '</h2><p class="lead">' + esc(o.lead) + '</p>' +
      '<div class="oa-sum"><div class="oa-sum-it"><span class="ph">' + (S.d.img ? '<img src="' + esc(S.d.img) + '" alt="">' : icSvg(o.dib || 'otra')) + '</span><span><b>' + esc(S.d.n) + '</b><small>' + esc(o.det || '') + '</small></span></div></div>' +
      '<div class="oa-info-l">' + (o.lineas || []).map(function (l) {
        return '<div class="' + (l[2] || '') + '"><i class="bi bi-' + l[0] + '"></i><span>' + l[1] + '</span></div>';
      }).join('') + '</div>' + pendHTML(S) + '</div>';
  }
  function irA(id) {
    var el = document.getElementById(id);
    if (!el) return;
    el.scrollIntoView({ behavior: 'smooth', block: 'center' });
    el.classList.remove('oa-flash'); void el.offsetWidth; el.classList.add('oa-flash');
  }

  /* ───────────────────────────── medidas por pieza ─────────────────────────────
     2026-10-20 (Daniel: «en las solicitudes hay que grabar datos: si es una piola, cuánto mide; si se rompió la cinta de trotadora,
     el ancho y el largo… imagínate que eres un técnico que no sabes nada y te va a llegar una solicitud: necesitas saber medidas y
     todo»). Las FICHAS (qué se mide de cada pieza, rangos, cómo se mide, qué foto sacar) las manda el servidor en `ops.medidas`:
     una sola fuente de verdad (ot_piezas_medidas.py) que el servidor usa para validar. Aquí solo se dibujan y se llenan.
     Medidas OBLIGATORIAS solo en piezas clave (piola, cinta, pad, correa, pin, brazo de seguridad), siempre con la salida
     «No pude medir» + foto con la huincha. NO hay pie de metro: los diámetros se eligen con botones y la foto los confirma. */
  function cotaH(x1, x2, y, t) {
    return '<g class="cota"><path d="M' + x1 + ' ' + y + 'H' + x2 + 'M' + (x1 + 6) + ' ' + (y - 3.5) + 'L' + x1 + ' ' + y + 'L' + (x1 + 6) + ' ' + (y + 3.5) +
      'M' + (x2 - 6) + ' ' + (y - 3.5) + 'L' + x2 + ' ' + y + 'L' + (x2 - 6) + ' ' + (y + 3.5) + '"/><text x="' + ((x1 + x2) / 2) + '" y="' + (y - 5) + '" text-anchor="middle">' + t + '</text></g>';
  }
  function cotaV(x, y1, y2, t) {
    return '<g class="cota"><path d="M' + x + ' ' + y1 + 'V' + y2 + 'M' + (x - 3.5) + ' ' + (y1 + 6) + 'L' + x + ' ' + y1 + 'L' + (x + 3.5) + ' ' + (y1 + 6) +
      'M' + (x - 3.5) + ' ' + (y2 - 6) + 'L' + x + ' ' + y2 + 'L' + (x + 3.5) + ' ' + (y2 - 6) + '"/><text x="' + (x + 7) + '" y="' + ((y1 + y2) / 2 + 4) + '">' + t + '</text></g>';
  }
  var DIB = {
    piola: '<path class="obj" d="M28 38H212" stroke-width="5"/><path d="M28 38H212" stroke="#fff" stroke-width="1.4" stroke-dasharray="3 5"/><circle class="obj" cx="20" cy="38" r="7"/><circle class="obj" cx="220" cy="38" r="7"/><circle class="obj" cx="220" cy="38" r="2.4"/><path class="thin" d="M13 52V82M227 52V82"/>' +
      cotaH(13, 227, 74, 'largo total, punta a punta') + '<g class="cota"><path d="M118 12v14M114 20l4 6 4-6"/><text x="126" y="22">Ø del cable</text></g>',
    mosqueton: '<path class="obj" d="M70 22h60a24 24 0 0 1 0 48H88a18 18 0 0 1-18-18z"/><path class="obj" d="M122 22l14 4"/>' + cotaH(70, 154, 86, 'largo total') + cotaV(180, 22, 70, 'boca'),
    agarre: '<rect class="obj" x="40" y="30" width="160" height="14" rx="7"/><circle class="obj" cx="30" cy="37" r="7"/><circle class="obj" cx="210" cy="37" r="7"/>' + cotaH(40, 200, 70, 'largo, punta a punta') + cotaV(226, 30, 44, 'Ø tubo'),
    polea: '<circle class="obj" cx="120" cy="44" r="30"/><circle class="thin" cx="120" cy="44" r="22"/><circle class="obj" cx="120" cy="44" r="8"/><path class="thin" d="M90 44V94M150 44V94"/>' + cotaH(90, 150, 92, 'Ø exterior'),
    rodamiento: '<circle class="obj" cx="90" cy="46" r="32"/><circle class="thin" cx="90" cy="46" r="22"/><circle class="obj" cx="90" cy="46" r="12"/>' + cotaH(58, 122, 92, 'Ø exterior') +
      '<g class="cota"><path d="M78 46H102"/><path d="M102 46H134"/><text x="138" y="43">Ø interior</text></g><text class="t" x="138" y="62">código grabado</text><text class="t" x="138" y="74">en el aro</text>',
    pin: '<circle class="obj" cx="34" cy="38" r="12"/><rect class="obj" x="46" y="33" width="150" height="10" rx="2"/><path class="obj" d="M196 33l10 5-10 5"/><path class="thin" d="M46 50V80M206 50V80"/>' +
      cotaH(46, 206, 72, 'largo útil: de la cabeza a la punta') + '<g class="cota"><path d="M126 10v14M122 18l4 6 4-6"/><text x="134" y="20">Ø del eje</text></g>',
    pad: '<rect class="obj" x="50" y="20" width="140" height="46" rx="12"/><rect class="thin" x="58" y="28" width="124" height="30" rx="7" stroke-dasharray="3 3"/>' + cotaH(50, 190, 84, 'largo') + cotaV(210, 20, 66, 'ancho'),
    rodillo: '<ellipse class="obj" cx="62" cy="40" rx="9" ry="22"/><path class="obj" d="M62 18H178M62 62H178"/><ellipse class="obj" cx="178" cy="40" rx="9" ry="22"/>' + cotaH(62, 178, 86, 'largo') + cotaV(204, 18, 62, 'Ø'),
    nivelador: '<path class="obj" d="M120 10v38" stroke-width="5"/><path d="M120 10v38" stroke="#fff" stroke-width="1.4" stroke-dasharray="2 4"/><ellipse class="obj" cx="120" cy="56" rx="30" ry="7"/><path class="obj" d="M90 56v6a30 7 0 0 0 60 0v-6"/>' +
      cotaV(176, 10, 48, 'vástago') + cotaH(90, 150, 88, 'Ø de la base'),
    tope: '<path class="obj" d="M94 62l8-38h36l8 38z"/><ellipse class="obj" cx="120" cy="24" rx="18" ry="4"/>' + cotaV(176, 24, 62, 'alto') + cotaH(94, 146, 84, 'Ø o lado'),
    cinta: '<rect class="obj" x="24" y="24" width="192" height="40" rx="3"/><path class="thin" d="M24 44H216" stroke-dasharray="3 4"/>' + cotaH(24, 216, 84, 'vuelta completa, estirada: de punta a punta') + cotaV(230, 24, 64, 'ancho'),
    tabla: '<rect class="obj" x="40" y="22" width="160" height="44" rx="3"/><path class="thin" d="M48 30h144M48 58h144"/>' + cotaH(40, 200, 84, 'largo') + cotaV(218, 22, 66, 'ancho'),
    placa: '<rect class="obj" x="52" y="12" width="136" height="58" rx="6"/><path class="thin" d="M64 28h112M64 40h112M64 52h80"/><text class="t" x="120" y="88" text-anchor="middle">se LEE: foto de cerca, derecha y con luz</text>',
    llave: '<circle class="obj" cx="90" cy="42" r="26"/><circle class="obj" cx="90" cy="42" r="8"/><path class="obj" d="M116 42h46"/><circle class="obj" cx="168" cy="42" r="5"/>' + cotaH(64, 116, 84, 'Ø del imán'),
    correa: '<circle class="obj" cx="60" cy="42" r="22"/><circle class="obj" cx="184" cy="42" r="12"/><path class="obj" d="M60 20L184 30M60 64L184 54"/><rect class="cota" x="96" y="36" width="54" height="14" rx="3"/><text class="t" x="123" y="47" text-anchor="middle">6PJ508</text><text class="t" x="120" y="86" text-anchor="middle">lee lo impreso en el lomo</text>',
    pedal: '<rect class="obj" x="98" y="18" width="100" height="48" rx="8"/><path class="thin" d="M112 30h72M112 42h72M112 54h72"/><path class="obj" d="M40 42h58"/><path class="obj" d="M40 36v12"/><text class="t" x="44" y="30">eje roscado</text><text class="t" x="148" y="84" text-anchor="middle">mira de qué lado va, sentado en la bici</text>',
    biela: '<circle class="obj" cx="50" cy="56" r="14"/><circle class="obj" cx="190" cy="28" r="9"/><path class="obj" d="M60 47L182 31M56 66L186 36"/><path class="thin" d="M50 70V88M190 37V88"/>' + cotaH(50, 190, 84, 'entre centros'),
    poste: '<rect class="obj" x="100" y="8" width="40" height="80" rx="2"/><circle class="obj" cx="120" cy="22" r="5"/><circle class="obj" cx="120" cy="42" r="5"/><circle class="obj" cx="120" cy="62" r="5"/>' + cotaV(164, 22, 42, 'separación') +
      '<text class="t" x="150" y="68">Ø del agujero</text>',
    perno: '<rect class="obj" x="40" y="26" width="18" height="26" rx="3"/><rect class="obj" x="58" y="33" width="140" height="12"/><path class="thin" d="M76 33v12M90 33v12M104 33v12M118 33v12M132 33v12M146 33v12M160 33v12M174 33v12M188 33v12"/>' + cotaH(58, 198, 74, 'largo bajo la cabeza') + cotaV(216, 33, 45, 'Ø'),
    cuerno: '<rect class="obj" x="40" y="44" width="20" height="32" rx="2"/><rect class="obj" x="60" y="50" width="140" height="20" rx="10"/>' + cotaH(60, 200, 40, 'largo útil') + cotaV(218, 50, 70, 'Ø'),
    disco: '<circle class="obj" cx="100" cy="46" r="36"/><circle class="obj" cx="100" cy="46" r="9"/><text class="t" x="154" y="36">peso grabado</text><text class="t" x="154" y="49">en la cara</text><text class="t" x="154" y="66">Ø del agujero</text>',
    barra: '<rect class="obj" x="20" y="40" width="200" height="8" rx="4"/><rect class="obj" x="14" y="34" width="22" height="20" rx="3"/><rect class="obj" x="204" y="34" width="22" height="20" rx="3"/>' + cotaH(14, 226, 76, 'largo total'),
    varilla: '<rect class="obj" x="114" y="8" width="12" height="82"/>' + cotaV(150, 8, 90, 'largo') + '<text class="t" x="52" y="50">Ø de la varilla</text>',
    tubo: '<rect class="obj" x="30" y="34" width="180" height="16" rx="8"/><circle class="obj" cx="44" cy="42" r="3"/><circle class="obj" cx="196" cy="42" r="3"/>' + cotaH(44, 196, 72, 'entre centros de los agujeros') + cotaV(226, 34, 50, 'Ø'),
    otra: '<path class="obj" d="M70 26h46l10 10h40v34H70z"/><text class="t" x="120" y="60" text-anchor="middle" style="font-size:22px">?</text>' + cotaH(70, 166, 86, 'largo') + cotaV(190, 36, 70, 'ancho')
  };
  function dibujo(k) {
    return '<svg class="oa-dib" viewBox="0 0 268 96" role="img" aria-label="Cómo se mide" stroke-linecap="round" stroke-linejoin="round">' + (DIB[k] || DIB.otra) + '</svg>';
  }

  function MED(s) { return (s && s.ops && s.ops.medidas) || null; }
  function fichaPorId(s, id) { var m = MED(s); return (m && id && m.fichas && m.fichas[id]) || null; }
  function fichaDeNombre(s, nombre) {
    var m = MED(s), id = m && m.ficha_de && m.ficha_de[nombre];
    return (id && fichaPorId(s, id)) ? id : (fichaPorId(s, 'H') ? 'H' : undefined);
  }
  function fichaDeLinea(s, l) { return fichaPorId(s, l.fichaId); }
  function nuevaLinea(l, fid) { l.uid = uuid().slice(0, 12); if (fid) l.fichaId = fid; if (l.q == null) l.q = (S && S.q) || ''; return l; }   // `q`: la palabra con que lo buscó (alias)
  function datosDe(l) {
    if (!l.datos) l.datos = { ficha: l.fichaId, medido_con: '', motivo_no: '', campos: {}, confirmados: {}, otro: {} };
    if (!l.datos.confirmados) l.datos.confirmados = {};
    if (!l.datos.otro) l.datos.otro = {};
    return l.datos;
  }
  function tiene(campos, k) { var v = campos[k]; return v != null && String(v).trim() !== ''; }
  function parseNum(v) {
    if (v == null || v === '') return null;
    if (typeof v === 'number') return isFinite(v) ? v : null;
    var t = String(v).replace(/\s/g, '').replace(/[^\d.,]+$/, '');      // «235cm», «5mm», «2,35 m»: se ignora lo que sobra al final
    if (!t) return null;
    if (/^\d{1,3}(\.\d{3})+(,\d+)?$/.test(t)) t = t.replace(/\./g, '').replace(',', '.'); else t = t.replace(',', '.');
    if (!/^\d*\.?\d+$/.test(t)) return null;
    var f = parseFloat(t);
    return isFinite(f) ? f : null;
  }
  function fmtNum(f) {
    if (f == null || !isFinite(f)) return '';
    if (Math.floor(f) === f) { var n = String(Math.abs(f)).replace(/\B(?=(\d{3})+(?!\d))/g, '.'); return (f < 0 ? '-' : '') + (Math.abs(f) >= 1000 ? n : String(f)); }
    return String(Math.round(f * 100) / 100).replace('.', ',');
  }
  function ecoDe(f, u) {
    if (f == null || f <= 0) return '';
    if (u === 'cm') return fmtNum(f) + ' cm = ' + fmtNum(f / 100) + ' m';
    if (u === 'mm' && f >= 1000) return fmtNum(f) + ' mm = ' + fmtNum(f / 1000) + ' m';
    if (u === 'mm' && f >= 100) return fmtNum(f) + ' mm = ' + fmtNum(f / 10) + ' cm';
    return '';
  }
  function condOk(cond, campos) {
    if (!cond) return true;
    if (cond.vacio) return !tiene(campos, cond.vacio);
    var v = campos[cond.k], ok = (cond['in'] || []).map(String).indexOf(String(v == null ? '' : v)) >= 0;
    if (!ok && cond.o) return condOk(cond.o, campos);
    return ok;
  }
  function altTxt(f) {
    var por = {}; f.campos.forEach(function (c) { por[c.k] = c.t; });
    return (f.req_alt || []).map(function (alt) { return alt.map(function (k) { return por[k] || k; }).join(' y '); }).join(' o ');
  }
  function factor(s) { var m = MED(s); return (m && m.factor_imposible) || 5; }
  /* Lo que falta, lo que solo avisa y lo imposible de UNA pieza (mismas reglas que el servidor). */
  function medCheck(s, l, f) {
    var d = datosDe(l), c = d.campos, out = { faltan: [], av: {}, imp: {}, vacio: true, completo: false };
    f.campos.forEach(function (x) {
      if (!condOk(x.cond, c) || (f.id === 'H' && x.k === 'que_es')) return;
      if (!tiene(c, x.k)) return;
      out.vacio = false;
      if (x.y === 'num' || x.y === 'opcnum') {
        var n = parseNum(c[x.k]);
        if (n == null) { if (x.y === 'opcnum' && String(c[x.k]) === 'otro') return; out.imp[x.k] = 'Escribe un número (por ejemplo 2,35).'; return; }
        if (n <= 0) out.imp[x.k] = 'Tiene que ser mayor que cero.';
        else if (x.mx != null && n > x.mx * factor(s)) out.imp[x.k] = fmtNum(n) + ' ' + (x.u || '') + ' es imposible para esta pieza. Revisa la unidad (' + (x.u || '') + ').';
        else if (x.mn != null && x.mx != null && (n < x.mn || n > x.mx) && !d.confirmados[x.k]) {
          out.av[x.k] = 'Ojo: ' + fmtNum(n) + ' ' + (x.u || '') + ' es ' + (n < x.mn ? 'muy poco' : 'mucho') + ' para esta pieza (lo normal es ' + fmtNum(x.mn) + '–' + fmtNum(x.mx) + ' ' + (x.u || '') + '). ¿Seguro? Revísalo con la huincha.';
        }
      }
    });
    if (f.clave && d.medido_con !== 'no_pude' && d.medido_con !== 'catalogo') {
      f.campos.forEach(function (x) { if (x.obl && condOk(x.cond, c) && !tiene(c, x.k)) out.faltan.push(x.t); });
      if (f.req_alt && f.req_alt.length && !f.req_alt.some(function (alt) { return alt.every(function (k) { return tiene(c, k); }); })) out.faltan.push(altTxt(f));
    }
    var sinMotivo = d.medido_con === 'no_pude' && !d.motivo_no;
    out.completo = !out.faltan.length && !Object.keys(out.imp).length && !sinMotivo;
    out.sinMotivo = sinMotivo;
    return out;
  }
  /* Estado de las medidas de TODO el pedido: ¿están completas?, ¿cuántas piezas clave?, ¿cuáles necesitan su propia foto con huincha? */
  function medEstado(s) {
    var hay = false, claves = [], okM = true, goFalta = null, textoFalta = '';
    s.sel.forEach(function (l, i) {
      var f = fichaDeLinea(s, l);
      if (!f || !f.campos || !f.campos.length) return;
      hay = true;
      if (f.clave) claves.push(i);
      var ck = medCheck(s, l, f);
      if (!ck.completo && okM) { okM = false; goFalta = 'oaMed' + i; textoFalta = ck.sinMotivo ? 'por qué no pudiste medir ' + f.corto : 'las medidas de ' + f.corto; }
    });
    return { hay: hay, okM: okM, claves: claves, propias: claves.length >= 2, propiasIdx: claves.length >= 2 ? claves : [], go: goFalta, falta: textoFalta };
  }
  /* Lo que viaja al servidor por línea (l.datos): solo lo que se ve y tiene valor. null si no se llenó nada. */
  function datosParaEnviar(s, l) {
    var f = fichaDeLinea(s, l);
    if (!f || !l.datos) return null;
    var d = l.datos, campos = {};
    f.campos.forEach(function (x) {
      if (!condOk(x.cond, d.campos) || !tiene(d.campos, x.k)) return;
      if (f.id === 'H' && x.k === 'que_es') return;
      campos[x.k] = d.campos[x.k];
      if (x.y === 'opc' && String(d.campos[x.k]) === 'otro' && tiene(d.campos, x.k + '_otro')) campos[x.k + '_otro'] = d.campos[x.k + '_otro'];
    });
    var nada = !Object.keys(campos).length && d.medido_con !== 'no_pude';
    if (nada) return null;
    var out = { ficha: f.id, medido_con: d.medido_con || (Object.keys(campos).length ? 'huincha' : ''), campos: campos };
    if (d.medido_con === 'no_pude') { out.motivo_no = d.motivo_no || ''; if (d.motivo_no === 'otro' && d.motivo_otro) out.motivo_otro = d.motivo_otro; }
    if (d.confirmados && Object.keys(d.confirmados).length) out.confirmados = d.confirmados;
    return out;
  }
  function fotosOrdenadas(s, propias) {
    if (!propias) return s.fotos.slice();
    return s.fotos.filter(function (f) { return !f.ref; }).concat(s.fotos.filter(function (f) { return f.ref; }));
  }
  function modeloMedidas(s, f) { var m = MED(s); return (m && m.modelo && m.modelo[f.id]) || []; }
  function piolasModelo(s) {
    return ((s.ops && s.ops.piolas) || []).filter(function (p) { return p.largo_m && p.diametro_mm; });
  }

  /* —— los campos de una ficha —— */
  function campoHTML(s, i, l, f, c, ck) {
    var d = datosDe(l), v = d.campos[c.k], av = ck.av[c.k], imp = ck.imp[c.k], id = 'oaM' + i + '_' + c.k;
    var req = f.clave && c.obl;
    var h = '<div class="oa-fl' + (imp ? ' bad' : '') + '" id="oaFl' + i + '_' + c.k + '"><label class="oa-fld-l" for="' + id + '">' + esc(c.t) +
      (c.u && c.y !== 'opcnum' ? ' <span class="oa-fld-x">(' + esc(c.u) + ')</span>' : '') + (req ? ' <span class="oa-req">obligatorio</span>' : '') + '</label>' +
      (c.c ? '<div class="oa-fl-c">' + esc(c.c) + '</div>' : '') + (c.ay ? '<div class="oa-fl-c">' + esc(c.ay) + '</div>' : '');
    if (c.y === 'num') {
      h += '<div class="oa-numw"><input id="' + id + '" class="oa-inp oa-num" inputmode="decimal" autocomplete="off" data-in="med" data-i="' + i + '" data-k="' + c.k + '" value="' + esc(v == null ? '' : v) + '" placeholder="' + esc(c.u || '') + '" aria-label="' + esc(c.t) + '">' +
        '<span class="oa-u">' + esc(c.u || '') + '</span></div>';
    } else if (c.y === 'txt') {
      h += '<input id="' + id + '" class="oa-inp" autocomplete="off" maxlength="80" data-in="medtxt" data-i="' + i + '" data-k="' + c.k + '" value="' + esc(v == null ? '' : v) + '" placeholder="' + esc(c.ph || '') + '" aria-label="' + esc(c.t) + '">';
    } else if (c.y === 'sino') {
      h += '<div class="oa-chips">' + [[true, 'Sí'], [false, 'No']].map(function (o) {
        var on = v === o[0];
        return '<button type="button" class="oa-ch' + (on ? ' on' : '') + '" aria-pressed="' + on + '" data-a="mopc" data-i="' + i + '" data-k="' + c.k + '" data-v="' + (o[0] ? 'si' : 'no') + '">' + (on ? '<i class="bi bi-check-lg"></i>' : '') + o[1] + '</button>';
      }).join('') + '</div>';
    } else {
      var ops = c.op || [], enOps = ops.some(function (o) { return String(o[0]) === String(v); });
      var otroNum = c.y === 'opcnum' && (d.otro[c.k] || (v != null && v !== '' && !enOps));
      h += '<div class="oa-chips">' + ops.map(function (o) {
        var on = !otroNum && String(v) === String(o[0]);
        return '<button type="button" class="oa-ch' + (on ? ' on' : '') + '" aria-pressed="' + on + '" data-a="mopc" data-i="' + i + '" data-k="' + c.k + '" data-v="' + esc(o[0]) + '">' + (on ? '<i class="bi bi-check-lg"></i>' : '') + esc(o[1]) + '</button>';
      }).join('') + (c.y === 'opcnum' ? '<button type="button" class="oa-ch' + (otroNum ? ' on' : '') + '" aria-pressed="' + !!otroNum + '" data-a="motro" data-i="' + i + '" data-k="' + c.k + '">' + (otroNum ? '<i class="bi bi-check-lg"></i>' : '') + 'Otro</button>' : '') + '</div>';
      if (otroNum) {
        h += '<div class="oa-numw" style="margin-top:8px"><input id="' + id + '" class="oa-inp oa-num" inputmode="decimal" autocomplete="off" data-in="med" data-i="' + i + '" data-k="' + c.k + '" value="' + esc(enOps ? '' : (v == null ? '' : v)) + '" placeholder="Valor en ' + esc(c.u || '') + '" aria-label="' + esc(c.t) + ' (otro valor)"><span class="oa-u">' + esc(c.u || '') + '</span></div>';
      }
      if (c.y === 'opc' && String(v) === 'otro') {
        h += '<input class="oa-inp" style="margin-top:8px" maxlength="80" autocomplete="off" data-in="medotro" data-i="' + i + '" data-k="' + c.k + '" value="' + esc(d.campos[c.k + '_otro'] || '') + '" placeholder="¿Cuál?" aria-label="' + esc(c.t) + ' (cuál)">';
      }
    }
    if (c.y === 'num' || c.y === 'opcnum') {
      var n = parseNum(v);
      h += '<div class="oa-eco" id="oaEco' + i + '_' + c.k + '">' + (n != null && n > 0 ? esc(ecoDe(n, c.u)) : '') + '</div>';
    }
    h += '<div id="oaAv' + i + '_' + c.k + '">' + avisoHTML(i, c.k, av, imp) + '</div></div>';
    return h;
  }
  function avisoHTML(i, k, av, imp) {
    if (imp) return '<div class="oa-av bad" role="alert"><i class="bi bi-x-octagon-fill"></i><span>' + esc(imp) + '</span></div>';
    if (av) return '<div class="oa-av" role="status"><i class="bi bi-exclamation-triangle-fill"></i><span>' + esc(av) + '</span><button type="button" class="oa-lnk" data-a="medok" data-i="' + i + '" data-k="' + k + '">Sí, está bien así</button></div>';
    return '';
  }

  /* —— una pieza del pedido con su ficha —— */
  function medLineaHTML(s, l, i, me) {
    var f = fichaDeLinea(s, l), d = datosDe(l), ck = medCheck(s, l, f), c = d.campos, nombre = l.nombre || f.titulo;
    var vis = [], mas = [];
    f.campos.forEach(function (x) {
      if (!condOk(x.cond, c) || (f.id === 'H' && x.k === 'que_es')) return;
      (x.mas ? mas : vis).push(x);
    });
    var h = '<div class="oa-med' + (ck.completo && !ck.vacio ? ' ok' : '') + '" id="oaMed' + i + '"><div class="oa-med-h"><span class="oa-med-ic">' + icSvg(f.ic) + '</span><div><b>' + esc(nombre) + '</b><small>' + esc(f.titulo) +
      (l.cant > 1 ? ' · ' + l.cant + ' unidades' : '') + '</small></div><span class="oa-badge' + (f.clave ? '' : ' g') + '">' + (f.clave ? 'Obligatoria' : 'Opcional') + '</span></div>';
    // cómo se mide: el dibujo y la frase, siempre visibles (no escondidas)
    h += '<div class="oa-como"><div class="oa-como-d">' + dibujo(f.dib) + '</div><div class="oa-como-t"><b><i class="bi bi-rulers"></i> Cómo se mide</b><ul>' +
      (f.como || []).map(function (t) { return '<li>' + esc(t) + '</li>'; }).join('') + '</ul></div></div>';
    if (f.seg) h += '<div class="oa-note" style="margin:0 0 10px"><i class="bi bi-shield-exclamation"></i><span><b>Seguridad:</b> ' + esc(f.seg) + '</span></div>';
    if (f.etiqueta) h += '<div class="oa-note" style="margin:0 0 10px"><i class="bi bi-camera"></i><span>Esta pieza se identifica con la foto de su etiqueta o del código grabado: sácala de cerca.</span></div>';
    // ayuda del catálogo del modelo: nunca reemplaza la medida real, la persona decide
    var mm = modeloMedidas(s, f), pm = f.id === 'A1' ? piolasModelo(s) : [];
    if ((mm.length || pm.length) && d.medido_con !== 'no_pude') {
      h += '<div class="oa-mod"><b><i class="bi bi-journal-check"></i> Según el catálogo de este modelo</b>' +
        mm.slice(0, 3).map(function (x, j) { return '<div class="oa-mod-i"><span>' + esc(x.resumen) + '</span><button type="button" class="oa-btn line" data-a="medmod" data-i="' + i + '" data-j="' + j + '">Es igual</button></div>'; }).join('') +
        pm.slice(0, 3).map(function (p, j) { return '<div class="oa-mod-i"><span>Piola ' + esc(fmtNum(p.diametro_mm)) + ' mm × ' + esc(fmtNum(p.largo_m)) + ' m</span><button type="button" class="oa-btn line" data-a="medpiola" data-i="' + i + '" data-j="' + j + '">Usar esta</button></div>'; }).join('') +
        '<small>Solo ayuda: confírmala con tu huincha.</small></div>';
    }
    if (d.medido_con === 'no_pude') {
      h += '<div class="oa-nopude"><div class="oa-nopude-t"><i class="bi bi-slash-circle"></i> Sin medida en terreno</div><div class="oa-chips-h" style="margin-top:6px">¿Por qué no pudiste medir?</div><div class="oa-chips">' +
        ((MED(s) && MED(s).motivos_no_pude) || []).map(function (m) {
          var on = d.motivo_no === m[0];
          return '<button type="button" class="oa-ch' + (on ? ' on' : '') + '" aria-pressed="' + on + '" data-a="medmot" data-i="' + i + '" data-v="' + esc(m[0]) + '">' + (on ? '<i class="bi bi-check-lg"></i>' : '') + esc(m[1]) + '</button>';
        }).join('') + '</div>' +
        '<div class="oa-note"><i class="bi bi-camera-fill"></i><span><b>Sube una foto de la pieza con la huincha al lado.</b> Sin la medida, esa foto es lo que usa bodega para encontrarla.' + (f.sin ? ' ' + esc(f.sin) : '') + '</span></div>' +
        '<button type="button" class="oa-lnk" data-a="medsi" data-i="' + i + '"><i class="bi bi-arrow-counterclockwise"></i> Sí pude medir: volver a las medidas</button></div>';
    } else {
      h += '<div class="oa-fields">' + vis.map(function (x) { return campoHTML(s, i, l, f, x, ck); }).join('') + '</div>';
      if (mas.length) {
        h += '<button type="button" class="oa-lnk oa-mas" aria-expanded="' + !!l.mas + '" data-a="medmas" data-i="' + i + '"><i class="bi bi-chevron-' + (l.mas ? 'up' : 'down') + '"></i> Más datos (opcional)</button>';
        if (l.mas) h += '<div class="oa-fields">' + mas.map(function (x) { return campoHTML(s, i, l, f, x, ck); }).join('') + '</div>';
      }
      h += '<div class="oa-faltan" id="oaFa' + i + '"' + (ck.faltan.length ? '' : ' hidden') + '><i class="bi bi-hand-index"></i><span>' + (ck.faltan.length ? 'Falta: ' + ck.faltan.map(esc).join(' · ') : '') + '</span></div>';
      if (f.clave) h += '<button type="button" class="oa-lnk muted" data-a="medno" data-i="' + i + '"><i class="bi bi-slash-circle"></i> No pude medir esta pieza</button>';
    }
    if (l.cant > 1 && !l.id) {
      h += '<div class="oa-note" style="margin-top:8px"><i class="bi bi-info-circle"></i><span>Pediste ' + l.cant + ' unidades: las medidas valen para todas. ¿No son iguales? ' +
        '<button type="button" class="oa-lnk" style="min-height:36px;display:inline" data-a="separar" data-i="' + i + '">Pedir cada una por separado</button></span></div>';
    }
    // con 2 o más piezas clave en el pedido, cada una lleva su propia foto con la huincha
    if (me.propias && f.clave) {
      var ok = s.fotos.some(function (x) { return x.ref === l.uid; });
      h += '<div class="oa-lf' + (ok ? ' ok' : '') + '" id="oaLf' + i + '"><div class="oa-lf-t"><i class="bi bi-camera-fill"></i> Foto de ESTA pieza con la huincha al lado <span class="oa-badge">Obligatoria</span></div>' +
        '<div class="oa-media"><button type="button" class="oa-btn blue" data-a="camlinea" data-i="' + i + '"' + (s.fotos.length >= MAX_FOTOS ? ' disabled' : '') + '><i class="bi bi-camera-fill"></i> ' + (ok ? 'Otra foto' : 'Tomar foto') + '</button>' +
        '<button type="button" class="oa-btn line" data-a="gallinea" data-i="' + i + '"' + (s.fotos.length >= MAX_FOTOS ? ' disabled' : '') + '><i class="bi bi-images"></i> De la galería</button></div>' + thumbs(l.uid) + '</div>';
    }
    return h + '</div>';
  }
  function medidasHTML(s, n) {
    var me = medEstado(s);
    if (!me.hay) return '';
    var cuerpo = '', primera = true;
    s.sel.forEach(function (l, i) { if (fichaDeLinea(s, l) && fichaDeLinea(s, l).campos && fichaDeLinea(s, l).campos.length) { cuerpo += medLineaHTML(s, l, i, me); primera = false; } });
    if (primera) return '';
    var intro = '<div class="oa-hint" style="margin:-4px 0 10px"><i class="bi bi-info-circle"></i><span>' +
      (me.claves.length ? 'Bodega necesita estas medidas para buscar la pieza. Mide con la huincha; si no puedes, hay una salida.' : 'Opcional, pero si las sabes, bodega encuentra la pieza más rápido.') + '</span></div>';
    return sc('oaSc-med', me.okM, n, 'Medidas de la pieza', me.claves.length ? 'Obligatorias' : 'Opcional', intro + cuerpo, me.claves.length ? '' : 'opt');
  }
  function resumenCorto(s, l) {
    var f = fichaDeLinea(s, l);
    if (!f || !l.datos) return '';
    var ck = medCheck(s, l, f);
    if (l.datos.medido_con === 'no_pude') return 'Sin medida en terreno';
    return ck.completo && !ck.vacio ? 'Medidas listas' : (ck.vacio ? '' : 'Faltan medidas');
  }

  /* ───────────────────────────── flujo: PEDIR REPUESTO ───────────────────────────── */
  var FALLAS = [['se_corto', 'Se cortó o se rompió'], ['se_solto', 'Se soltó o se salió'], ['desgaste', 'Desgaste por uso'],
                ['se_perdio', 'Se perdió o falta'], ['ruido', 'Hace ruido o se traba'], ['otro', 'Otra cosa: la cuento abajo']];
  function falla_txt(v) { for (var i = 0; i < FALLAS.length; i++) if (FALLAS[i][0] === v) return FALLAS[i][1]; return ''; }

  function tilesDe(s) {
    var sug = (s.ops && s.ops.repuestos_sugeridos) || [];
    var tiles = sug.map(function (t) { return { n: sinPar(t), dice: enPar(t), full: t, ic: icDe(t) }; });
    // lo que sugirió «¿Qué le pasa?» siempre tiene su baldosa, aunque la lista del producto sea otra
    var pz = (s.sug && s.sug.piezas) || [];
    pz.forEach(function (n) {
      if (n !== 'Otra pieza' && !tiles.some(function (t) { return t.full === n; })) tiles.unshift({ n: sinPar(n), dice: enPar(n), full: n, ic: icDe(n) });
    });
    return pz.length ? tiles.filter(function (t) { return pz.indexOf(t.full) >= 0; }).concat(tiles.filter(function (t) { return pz.indexOf(t.full) < 0; })) : tiles;
  }
  function matchTile(tile, desc) { return coincidePieza(tile, desc); }   // 2026-10-21 (M1): palabra completa o raíz, ya no 4 letras sueltas
  function compat(s) { return (s.ops && s.ops.compatibles) || []; }
  function enSel(s, id) { return s.sel.filter(function (l) { return l.id === id; })[0]; }

  function todosArchivos(s) {             // fotos en orden (la 0 viaja con el pedido) y, al final, el video opcional
    var l = s.fotos.map(function (f) { return { file: f.file, ref: f.ref || '', esVideo: false }; });
    if (s.video) l.push({ file: s.video.file, ref: '', esVideo: true });
    return l;
  }
  var pedir = {
    init: function (s) {
      s.sel = []; s.q = ''; s.ops = null; s.cargando = true; s.falla = ''; s.detalle = ''; s.dejarFS = false; s.tile = null;
      s.loteId = uuid(); s.res = null; s.sug = null; s.pend = []; s.diag = null; s.camRef = ''; s.propiasAct = false;
      s.alias = ''; s.video = null; s.enviadoOk = false; s.subidas = []; s.fotoExtraFallo = 0; s.progreso = ''; s.creadaMs = 0;
      var b = s.borrador ? leerBorrador(s.key) : null;
      if (b) {
        s.sel = b.sel || []; s.q = b.q || ''; s.falla = b.falla || ''; s.detalle = b.detalle || ''; s.dejarFS = !!b.dejarFS; s.alias = b.alias || '';
        s.tile = b.tile || null; s.loteId = b.loteId || s.loteId; s.step = b.step === 3 ? 3 : (b.step === 2 && s.tile ? 2 : 1);
        s.diag = b.diag || null; s.sug = b.sug || null; s.pend = b.pend || [];
        if (b.nFotos) cargarFotosBorrador(s, b.nFotos, b.refs).then(function () { if (S === s) render(false); });
        if (b.video) cargarVideoBorrador(s).then(function () { if (S === s) render(false); });
        if (b.env) {            // el pedido ya salió: solo faltan archivos por subir -> pantalla final con «Reintentar fotos»
          s.enviadoOk = true; s.res = b.env.res; s.subidas = b.env.ok || []; s.creadaMs = b.env.creada || 0; s.step = 'fin'; s.cargando = false;
          s.fotoExtraFallo = s.subidas.filter(function (x) { return !x; }).length;
          return;
        }
      }
      var key = s.key;
      if (s.sem && s.sem.ops) return;       // el diagnóstico ya trajo las piezas, sus fichas y el stock: no se pide dos veces
      api('GET', base(s.d) + '/repuestos-opciones').then(function (j) {
        if (S !== s) return;
        s.cargando = false;
        if (j.ok) s.ops = j; else s.opsErr = j.error;
        if (s.pre && !s.sel.length) pedir.preElegir(s);
        render(false);
      });
    },
    /* 2026-10-21 (A2): «Piolas» y «Cinta» de la tarjeta abren ESTE flujo con la baldosa ya elegida (antes abrían el modal viejo, que copiaba la
       medida del Catálogo como si fuera de terreno). Es una sugerencia a la vista: la persona cambia la pieza o la confirma (REGLA #32). */
    preElegir: function (s) {
      var t = tilesDe(s).filter(function (x) { return icDe(x.full) === s.pre; })[0];
      if (!t) { s.q = s.pre === 'cinta' ? 'cinta' : 'piola'; return; }          // el modelo no la trae como baldosa: se deja buscada
      var ms = compat(s).filter(function (r) { return matchTile(t, r.descripcion); });
      s.tile = t;
      if (ms.length) { s.step = 2; return; }
      s.sel.push(nuevaLinea({ id: null, nombre: t.n, tile: t.n, origen: 'manual', cant: 1 }, fichaDeNombre(s, t.full)));
      s.step = 3;
    },
    semilla: function (s, sem) {
      semillaBase(s, sem);
      s.diag = sem.diag || null;
      if (sem.falla) s.falla = sem.falla;                     // «¿Qué le pasó?» llega preseleccionado; la persona lo cambia
      if (sem.ops) { s.ops = sem.ops; s.cargando = false; }
      pedir.aplicarSugeridas(s);
    },
    /* Una sola pieza sugerida = como tocar su baldosa. Dos o más (media certeza) = se muestran y la persona elige. */
    aplicarSugeridas: function (s) {
      var pz = (s.sug && s.sug.piezas) || [];
      if (!pz.length || !s.ops) return;
      if (pz.length > 1) { s.step = 1; return; }
      var nombre = pz[0];
      if (nombre === 'Otra pieza') { s.sel.push(nuevaLinea({ id: null, nombre: '', origen: 'manual', cant: 1 }, fichaDeNombre(s, 'Otra pieza'))); s.step = 3; return; }
      var t = tilesDe(s).filter(function (x) { return x.full === nombre; })[0] || { n: sinPar(nombre), dice: enPar(nombre), full: nombre, ic: icDe(nombre) };
      var ms = compat(s).filter(function (r) { return matchTile(t, r.descripcion); });
      s.tile = t;
      if (ms.length) { s.step = 2; return; }
      s.sel.push(nuevaLinea({ id: null, nombre: t.n, tile: t.n, origen: 'manual', cant: 1 }, fichaDeNombre(s, t.full)));
      s.step = 3;
    },
    vista: function (s) {
      if (s.step === 'fin') return pedir.fin(s);
      var d = s.d;
      var top = topHTML({ eyebrow: 'Pedir repuesto', pasos: ['La pieza', 'Cuál es', 'Foto y envío'], cur: s.step, atras: s.step > 1, guarda: true });
      if (s.step === 1) return { top: top, body: pedir.p1(s), foot: pedir.foot1(s) };
      if (s.step === 2) return { top: top, body: pedir.p2(s), foot: pedir.foot2(s) };
      return { top: top, body: pedir.p3(s), foot: pedir.foot3(s) };
    },
    /* paso 1 — la pieza */
    p1: function (s) {
      var h = '<h2 class="oa-hq">¿Qué pieza necesita ' + esc(sinPar(s.d.n)) + '?</h2>' + sugBanner(s) + dupHTML(s);
      // 2026-10-21 (guiado): si no sabe qué pieza es, lo primero a la vista es «¿Qué le pasa?» (el diagnóstico guiado de siempre)
      if (!s.sug && !s.sel.length && permitida(s, 'pedir')) {
        h += '<button type="button" class="oa-btn oa-diag" data-a="adiag"><i class="bi bi-clipboard2-pulse"></i><span>¿Qué le pasa a ' + (s.d.k === 'i' ? 'este producto' : 'este equipo') +
          '?<small>No sé qué pieza es: te ayudo a decidir</small></span></button>';
      }
      if (s.cargando) h += '<div class="oa-hint"><i class="bi bi-arrow-repeat"></i> Buscando las piezas de este modelo…</div>';
      if (s.opsErr) h += '<div class="oa-urg"><i class="bi bi-wifi-off"></i><span>No pudimos traer las piezas de bodega. Igual puedes pedirla con una foto.</span></div>';
      h += '<div class="oa-search"><i class="bi bi-search"></i><input type="search" data-in="q" value="' + esc(s.q) + '" placeholder="Como tú le digas: perilla, cable…" aria-label="Buscar la pieza" autocomplete="off">' +
        '<button type="button" class="clr" id="oaClrQ" data-a="limpiarq" aria-label="Borrar la búsqueda"' + (s.q ? '' : ' hidden') + '><i class="bi bi-x-lg"></i></button></div>';
      return h + '<div id="oaP1R">' + pedir.p1res(s) + '</div>' + pedir.enlaces(s);
    },
    /* Enlaces pequeños de salida: el manual del modelo y la «vista clásica» (el modal de antes se conserva, REGLA #4.2). */
    enlaces: function (s) {
      var l = '';
      if (s.d.p && s.d.p.man && window.otdRepAbrir) l += '<button type="button" class="oa-lnk" data-a="manualviejo"><i class="bi bi-file-earmark-pdf"></i> Ver el manual del ' + (s.d.k === 'i' ? 'producto' : 'equipo') + '</button>';
      if (window.otdRepAbrir) l += '<button type="button" class="oa-lnk muted" data-a="clasica"><i class="bi bi-window"></i> Usar la vista clásica</button>';
      return l ? '<div class="oa-enl">' + l + '</div>' : '';
    },
    /* resultados del paso 1 (se repintan solos al escribir en el buscador, sin tocar el campo: no se cierra el teclado) */
    p1res: function (s) {
      var q = norm(s.q), tiles = tilesDe(s), cm = compat(s), h = '';
      var vis = tiles.filter(function (t) { return !q || norm(t.full).indexOf(q) >= 0 || icDe(t.full) === icDe(q); });
      var rep = q ? cm.filter(function (r) { return norm(r.descripcion + ' ' + (r.sku || '')).indexOf(q) >= 0; }).slice(0, 12) : [];
      if (tiles.length) h += '<div class="oa-hint" style="margin-bottom:10px"><i class="bi bi-hand-index"></i><span>Toca la pieza. Son las que más se piden para este tipo de equipo.</span></div>';
      h += '<div class="oa-tiles">' + vis.map(function (t, i) {
        var ms = cm.filter(function (r) { return matchTile(t, r.descripcion); });
        var hay = ms.some(function (r) { return r.semaforo === 'verde'; });
        var ya = s.sel.some(function (l) { return !l.id && l.tile === t.n; }) || ms.some(function (r) { return enSel(s, r.id); });
        var sub = !ms.length ? '<span class="oa-dot x"></span> Pídela con una foto' : (hay ? '<span class="oa-dot g"></span> Hay en bodega' : '<span class="oa-dot a"></span> ' + TXT_NO_HAY);
        var sugT = ((s.sug && s.sug.piezas) || []).indexOf(t.full) >= 0;
        return '<button type="button" class="oa-tile' + (ya ? ' on' : '') + (sugT ? ' sug' : '') + '" data-a="tile" data-i="' + tiles.indexOf(t) + '">' + (ya ? '<span class="oa-ck"><i class="bi bi-check-lg"></i></span>' : '') +
          (sugT && !ya ? '<span class="oa-tile-sug">Sugerida</span>' : '') +
          '<span class="oa-tile-ic">' + icSvg(t.ic) + '</span><span class="oa-tile-n">' + esc(t.n) + '</span>' +
          (t.dice ? '<span class="oa-tile-s">le dicen «' + esc(t.dice) + '»</span>' : '') + '<span class="oa-tile-s">' + sub + '</span></button>';
      }).join('') + (tiles.length ? '' : '<button type="button" class="oa-tile otra" data-a="otra"><span class="oa-tile-ic"><i class="bi bi-camera-fill"></i></span><span><span class="oa-tile-n" style="display:block">Otra pieza · pídela con una foto</span>' +
        '<span class="oa-tile-s">No necesitas saber el nombre: bodega la busca con tu foto.</span></span></button>') + '</div>';
      if (q && !vis.length && !rep.length) h += '<div class="oa-empty" style="margin-top:12px">No encontramos «' + esc(s.q) + '». Usa «Otra pieza» y pídela con una foto.</div>';
      if (rep.length) h += '<div class="oa-sec"><h3><i class="bi bi-box-seam"></i> En bodega para este modelo</h3></div>' + rep.map(function (r) { return pedir.repCard(s, r); }).join('');
      return h;
    },
    foot1: function (s) {
      var otra = '<button type="button" class="oa-btn line block lg" data-a="otra"><i class="bi bi-camera-fill"></i> Otra pieza · pídela con una foto</button>';
      if (s.sel.length) {
        return '<div class="oa-hint" style="margin:0 0 8px"><i class="bi bi-check2-circle"></i><span><b>' + s.sel.length + '</b> pieza' + (s.sel.length === 1 ? '' : 's') + ' en tu pedido</span></div>' +
          '<div class="row"><button type="button" class="oa-btn line lg" data-a="otra"><i class="bi bi-camera-fill"></i> Otra pieza</button>' +
          '<button type="button" class="oa-btn blue lg" data-a="a3">Foto y envío <i class="bi bi-arrow-right"></i></button></div>';
      }
      return otra;
    },
    repCard: function (s, r) {
      var l = enSel(s, r.id), on = !!l;
      return '<div class="oa-rep' + (on ? ' on' : '') + '" role="button" tabindex="0" data-a="rep" data-id="' + r.id + '"><span class="ph">' + (r.foto_url ? '<img src="' + esc(r.foto_url) + '" alt="" loading="lazy">' : icSvg(icDe(r.descripcion))) + '</span>' +
        '<span><span class="n">' + esc(r.descripcion) + '</span><span class="s">' + stockChip(r) + (r.sku ? '<span class="oa-st gray">' + esc(r.sku) + '</span>' : '') +
        (on ? '<span class="oa-st green"><i class="bi bi-check-lg"></i> Elegida</span>' : '') + '</span>' +
        (on ? '<span class="q"><span class="oa-qty" data-stop="1"><button type="button" data-a="menos" data-id="' + r.id + '" aria-label="Una menos"><i class="bi bi-dash-lg"></i></button><output>' + l.cant + '</output>' +
          '<button type="button" data-a="mas" data-id="' + r.id + '" aria-label="Una más"><i class="bi bi-plus-lg"></i></button></span><span style="color:var(--oa-soft);font-size:14px">unidades</span></span>' : '') + '</span></div>';
    },
    /* paso 2 — cuál es */
    p2: function (s) {
      var t = s.tile, ms = compat(s).filter(function (r) { return matchTile(t, r.descripcion); });
      return sugBanner(s) + dupHTML(s) + '<div class="oa-sc info" style="padding-left:14px"><div class="oa-sum-it"><span class="ph" style="display:grid;place-items:center">' + icSvg(t.ic) + '</span><span><b>' + esc(t.n) + '</b><small>' +
        (t.dice ? 'le dicen «' + esc(t.dice) + '»' : 'Pieza elegida') + '</small></span></div></div>' +
        '<h2 class="oa-hq">¿Cuál es?</h2><p class="oa-hsub">Toca la que se parece. Elegir ya es agregar al pedido.</p>' +
        ms.map(function (r) { return pedir.repCard(s, r); }).join('') +
        '<button type="button" class="oa-btn line block" style="min-height:64px;border-style:dashed" data-a="comofoto"><i class="bi bi-camera-fill"></i> No es ninguna · pídela con una foto</button>';
    },
    foot2: function (s) {
      var n = s.sel.length;
      return n ? '<button type="button" class="oa-btn blue block lg" data-a="a3">Seguir con ' + n + (n === 1 ? ' pieza' : ' piezas') + ' <i class="bi bi-arrow-right"></i></button>'
               : '<div class="oa-hint" style="min-height:48px;margin:0"><i class="bi bi-hand-index"></i><span>Toca la pieza que es, o pídela con una foto.</span></div>';
    },
    /* paso 3 — foto y envío */
    estado3: function (s) {
      s.sel.forEach(function (l) { if (!l.uid) l.uid = uuid().slice(0, 12); });          // pedidos guardados antes de esta versión
      var me = medEstado(s); s.propiasAct = me.propias;
      // con 2 o más piezas clave cada una lleva SU foto con la huincha (además de la general); con una sola basta la foto del pedido
      var okF = s.fotos.length > 0 && (!me.propias || s.fotos.some(function (f) { return !f.ref; }));
      var sinFoto = me.propias ? me.propiasIdx.filter(function (i) { return !s.fotos.some(function (f) { return f.ref === s.sel[i].uid; }); }) : [];
      var okQ = !!s.falla && (s.falla !== 'otro' || s.detalle.replace(/\s/g, '').length >= 10);
      var okA = !!String(s.alias || '').trim();
      var okL = s.sel.length > 0 && s.sel.every(function (l) { return l.id || (l.nombre || '').replace(/\s/g, '').length >= 2; });
      var falta = !me.okM ? { t: me.falta, go: me.go || 'oaSc-med' } : !okF ? { t: s.video && !s.fotos.length ? 'una foto (el video solo no basta)' : 'la foto', go: 'oaSc-foto' } :
        sinFoto.length ? { t: 'la foto de ' + (s.sel[sinFoto[0]].nombre || 'la pieza') + ' con la huincha', go: 'oaLf' + sinFoto[0] } :
        !okQ ? { t: 'qué le pasó', go: 'oaSc-falla' } : !okL ? { t: 'el nombre de la pieza', go: 'oaSc-lista' } : null;
      return { okM: me.okM, okF: okF, okQ: okQ, okL: okL, okA: okA, falta: falta, me: me };
    },
    p3: function (s) {
      var e = pedir.estado3(s), me = e.me, n = 1;
      var h = '<h2 class="oa-hq">' + (me.hay ? 'Medidas, foto y envío' : 'Foto y envío') + '</h2><p class="oa-hsub">' +
        (me.hay ? 'Lo que mide la pieza y una foto de la pieza con la huincha al lado. Bodega ve esto antes de buscarla.' : 'Una foto de la pieza y qué le pasó. Bodega ve esto antes de buscarla.') + '</p>' + errHTML() + sugBanner(s) + dupHTML(s);
      var med = medidasHTML(s, n);
      if (med) { h += med; n++; }
      var f0 = s.sel.map(function (l) { return fichaDeLinea(s, l); }).filter(function (f) { return f && f.id !== 'H'; })[0];
      h += sc('oaSc-foto', e.okF, n++, 'Foto de la pieza con la huincha al lado', 'Obligatoria',
        mediaCuerpo((f0 && f0.foto ? f0.foto + ' ' : '') + 'Sirve para que bodega reconozca la pieza.' + (me.propias ? ' Cada pieza con medida lleva además su propia foto arriba.' : ''), true));
      h += sc('oaSc-falla', e.okQ, n++, '¿Qué le pasó?', 'Obligatoria', '<div class="oa-chips-h">Toca lo que pasó.</div>' + chipsHTML(FALLAS, s.falla, 'falla') +
        '<div style="margin-top:12px">' + textoHTML('detalle', s.detalle, s.falla === 'otro' ? 'Cuéntalo con tus palabras (mínimo 10 letras)' : 'Algo más que ayude (opcional)', s.falla === 'otro' ? 10 : 0) + '</div>');
      // 2026-10-21 (A3): la palabra del técnico («perilla») viaja al servidor como otro nombre de la pieza; gestión la confirma o corrige
      h += sc('oaSc-alias', e.okA, n++, '¿Cómo le dicen en tu empresa?', 'Opcional',
        '<div class="oa-chips-h">Si la pieza tiene otro nombre en tu gimnasio o en tu taller, escríbelo. Así bodega la encuentra y el sistema aprende.</div>' +
        '<input class="oa-inp" data-in="alias" maxlength="120" autocomplete="off" value="' + esc(s.alias || '') + '" placeholder="Ej: perilla, cable de fuerza" aria-label="Cómo le dicen a la pieza en tu empresa">', 'opt');
      h += sc('oaSc-lista', e.okL, n++, 'Piezas del pedido', '', pedir.lista(s), e.okL ? '' : '');
      if (s.d.k === 'e' && !s.d.fsb) {
        h += '<button type="button" class="oa-o2 wide' + (s.dejarFS ? ' on r' : '') + '" style="margin-bottom:12px" aria-pressed="' + !!s.dejarFS + '" data-a="dejarfs">' +
          (s.dejarFS ? '<span class="ck"><i class="bi bi-check-lg"></i></span>' : '') + '<i class="bi bi-sign-stop"></i><b>Además, dejarlo fuera de servicio</b><small>Si no se puede usar mientras llega la pieza. Sale un aviso urgente.</small></button>';
      }
      return h;
    },
    lista: function (s) {
      if (!s.sel.length) return '<div class="oa-empty">No hay piezas. Vuelve atrás y elige una.</div>';
      return s.sel.map(function (l, i) {
        return '<div class="oa-rv-it"><span>' +
          (l.id || l.tile ? '<b>' + esc(l.nombre) + '</b><small>' + (l.id ? (l.sku ? esc(l.sku) + ' · ' : '') + 'de la bodega' : 'Bodega la busca con tu foto') + '</small>'
            : '<label class="oa-fld-l" for="oaN' + i + '">¿Cómo se llama? <span class="oa-fld-x">como tú le digas</span></label><input id="oaN' + i + '" class="oa-inp" data-in="nombre" data-i="' + i + '" value="' + esc(l.nombre) + '" placeholder="Ej: perilla del peso" maxlength="120">') +
          (resumenCorto(s, l) ? '<small class="oa-medtag' + (resumenCorto(s, l) === 'Faltan medidas' ? ' w' : '') + '"><i class="bi bi-rulers"></i> ' + esc(resumenCorto(s, l)) + '</small>' : '') + '</span>' +
          '<span class="oa-qty"><button type="button" data-a="menosi" data-i="' + i + '" aria-label="Una menos"><i class="bi bi-dash-lg"></i></button><output>' + l.cant + '</output>' +
          '<button type="button" data-a="masi" data-i="' + i + '" aria-label="Una más"><i class="bi bi-plus-lg"></i></button></span>' +
          '<span style="grid-column:1/-1"><button type="button" class="oa-lnk muted" style="min-height:36px;font-size:14px" data-a="quitar" data-i="' + i + '"><i class="bi bi-trash3"></i> Quitar</button></span></div>';
      }).join('') + '<button type="button" class="oa-lnk" data-a="mas-piezas" style="margin-top:6px"><i class="bi bi-plus-lg"></i> Pedir otra pieza en este mismo pedido</button>';
    },
    foot3: function (s) {
      var e = pedir.estado3(s);
      if (e.falta) return faltaBtn(e.falta.t, e.falta.go);
      return '<button type="button" class="oa-btn blue block lg" data-a="enviar"' + (s.enviando ? ' aria-busy="true"' : '') + '>' +
        (s.enviando ? '<span class="oa-spin"></span> ' + esc(s.progreso || 'Enviando…') : '<i class="bi bi-send-fill"></i> Enviar pedido a bodega') + '</button>';
    },
    /* confirmación: qué pasó, quién lo atiende (si el servidor ya lo manda) y la trayectoria con fecha y hora Chile */
    fin: function (s) {
      var r = s.res || {}, d = s.d, primera = (r.solicitudes || [])[0] || {};
      var items = (r.solicitudes || []).map(function (x) {
        return '<div class="oa-sum-it"><span class="ph">' + icSvg(icDe(x.repuesto_nombre)) + '</span><span><b>' + esc(x.repuesto_nombre) + ' <span style="white-space:nowrap">× ' + (+x.cantidad || 1) + '</span></b><small>para ' + esc(sinPar(d.n)) + (x.medida ? ' · ' + esc(x.medida) : '') + '</small></span></div>';
      }).join('');
      var pseudo = { estado: 'solicitado', creada_ms: s.creadaMs || Date.now(), atiende: r.atiende || primera.atiende || '', responde_antes: r.responde_antes || primera.responde_antes || '' };
      var promesa = (pseudo.atiende || pseudo.responde_antes) ? 'Te avisamos por la campana en cada paso. No tienes que ir a buscarla.' : 'Bodega lo revisa. Lo verás avanzar aquí, en la OT.';
      var faltan = s.fotoExtraFallo || 0;
      var body = '<div class="oa-fin"><div class="oa-done-ic"><i class="bi bi-check-lg"></i></div><h2>Listo.</h2>' +
        '<p class="lead">Bodega ya recibió tu pedido' + (r.numero_ticket ? ' <span style="white-space:nowrap">N° ' + esc(r.numero_ticket) + '</span>' : '') + '.</p><div class="oa-sum">' + items + '</div>' + atiendeHTML(pseudo) + '<div class="oa-info-l">' +
        '<div><i class="bi bi-person-check"></i><span>' + promesa + '</span></div>' +
        (r.repetido ? '<div class="w"><i class="bi bi-files"></i><span>Este pedido ya estaba enviado: no se duplicó.</span></div>' : '') +
        (s.fsAviso ? '<div class="a"><i class="bi bi-sign-stop"></i><span>El equipo quedó fuera de servicio. Avisamos como urgente.</span></div>' : '') +
        (r.avisos && r.avisos.length ? r.avisos.map(function (a) { return '<div class="w"><i class="bi bi-exclamation-triangle-fill"></i><span>' + esc(a) + '</span></div>'; }).join('') : '') +
        (faltan ? '<div class="w"><i class="bi bi-cloud-slash"></i><span>' + faltan + (faltan === 1 ? ' archivo no se pudo subir' : ' archivos no se pudieron subir') + '. El pedido sí quedó y lo que falta sigue guardado en tu teléfono: toca «Reintentar fotos».</span></div>' : '') +
        '</div>' +
        '<div class="oa-prev"><div class="oa-prev-t">Así va a avanzar</div>' + trayectoriaHTML(pseudo) + '</div>' + pendHTML(s) + '</div>';
      var foot = faltan
        ? '<div class="row"><button type="button" class="oa-btn blue lg" data-a="reintfotos"' + (s.subiendo ? ' aria-busy="true"' : '') + '>' + (s.subiendo ? '<span class="oa-spin"></span> ' + esc(s.progreso || 'Subiendo…') : '<i class="bi bi-arrow-repeat"></i> Reintentar fotos') + '</button>' +
          '<button type="button" class="oa-btn ghost lg" data-a="volver">Volver a la OT</button></div>'
        : '<div class="row"><button type="button" class="oa-btn ghost lg" data-a="otrapieza"><i class="bi bi-plus-lg"></i> Pedir otra pieza</button>' +
          '<button type="button" class="oa-btn blue lg" data-a="volver">Volver a la OT</button></div>';
      return { top: '<div class="oa-top"><div class="oa-r1"><span class="oa-eb">Pedido enviado</span></div></div>', body: body, foot: foot };
    },
    /* acciones */
    elegirRep: function (s, id) {
      var r = compat(s).filter(function (x) { return String(x.id) === String(id); })[0];
      if (!r) return;
      var l = enSel(s, r.id);
      if (l) { s.sel = s.sel.filter(function (x) { return x !== l; }); }
      else {
        // 2026-10-21 (M1): manda la ficha de la PIEZA ELEGIDA; la de la baldosa solo si el servidor no trae una propia (o es la genérica)
        var fid = fichaParaLinea(r.ficha, s.tile && fichaDeNombre(s, s.tile.full), function (id) { return !!fichaPorId(s, id); });
        s.sel.push(nuevaLinea({ id: r.id, nombre: r.descripcion, sku: r.sku || '', origen: 'compatible', cant: 1 }, fid));
      }
      s.dirty = true;
    },
    enviar: async function (s) {
      var e = pedir.estado3(s);
      if (e.falta || s.enviando) return;
      s.enviando = true; s.err = ''; s.progreso = 'Enviando…'; marco();
      s.fotos = fotosOrdenadas(s, e.me.propias);      // la foto general va primero: es la que viaja con el pedido
      var d = s.d, fd = new FormData();
      var motivo = falla_txt(s.falla) + (s.detalle.trim() ? ' — ' + s.detalle.trim() : '');
      if (s.falla === 'otro') motivo = s.detalle.trim();
      fd.append('motivo', motivo);
      fd.append('lineas', JSON.stringify(s.sel.map(function (l) {
        var o = { origen: l.id ? (l.origen || 'compatible') : 'manual', repuesto_nombre: l.nombre, cantidad: l.cant, ref: l.uid };
        if (l.id) o.repuesto_stock_id = l.id;
        if (l.sku) o.repuesto_sku = l.sku;
        var dd = datosParaEnviar(s, l);                  // las medidas viajan POR LÍNEA (l.datos)
        if (dd) { o.datos = dd; o.ficha = dd.ficha; }
        // 2026-10-21 (A3): la palabra del técnico viaja como otro nombre de la pieza («perilla» = pin); gestión lo confirma o corrige al validar
        var propia = !l.id && !l.tile;                   // «Otra pieza»: lo escrito ES el nombre
        var alt = armarNombresAlt({ canonico: propia ? '' : l.nombre, buscado: l.q || '', alias: s.alias, escrito: propia ? l.nombre : '' });
        if (alt.length) o.nombres_alt = alt;
        return o;
      })));
      if (s.diag) {                                      // el camino del «¿Qué le pasa?», para bodega y gestión
        var fz = (s.diag.piezas || []).map(function (p) { return fichaDeNombre(s, p); });       // ¿pidió una pieza de la ficha sugerida?
        fd.append('diagnostico', JSON.stringify(Object.assign({}, s.diag, { siguio: s.sel.some(function (l) { return fz.indexOf(l.fichaId) >= 0; }) })));
      }
      fd.append('lote_id', s.loteId);
      if (d.k === 'e' && s.dejarFS) fd.append('dejar_fuera_servicio', '1');
      fd.append('evidencia', s.fotos[0].file, s.fotos[0].file.name || 'foto.jpg');
      var j = await api('POST', base(d) + '/solicitar-repuestos', fd);
      if (S !== s) return;
      if (!j.ok) { s.enviando = false; s.progreso = ''; s.err = j.error || 'No se pudo enviar el pedido.'; render(false); irA('oaBody'); return; }
      // El pedido YA quedó creado. Lo que falta (fotos 2..N y el video) sube una por una y cada una queda anotada: si se corta la señal,
      // el borrador NO se borra y «Reintentar fotos» sube solo lo que falta (2026-10-21, M4).
      s.res = j; s.enviadoOk = true; s.creadaMs = Date.now();
      s.subidas = todosArchivos(s).map(function (_x, i) { return i === 0; });
      d.sol = (d.sol || []).concat((j.solicitudes || []).map(function (x) {
        return { id: x.solicitud_id, nombre: x.repuesto_nombre, cant: x.cantidad, estado: 'solicitado', label: 'Solicitado', abierta: true, seg: null, medida: x.medida || '',
                 creada_ms: s.creadaMs, atiende: x.atiende || j.atiende || '', responde_antes: x.responde_antes || j.responde_antes || '' };
      }));
      s.fsAviso = !!(d.k === 'e' && s.dejarFS);
      if (s.fsAviso) { d.fsb = true; d.fs = { motivo: motivo, riesgo: false, desde: '', por: '' }; }
      await guardarBorrador(s);                          // las fotos quedan a salvo en el teléfono mientras suben
      await pedir.subirPendientes(s);
      if (S !== s) return;
      s.enviando = false; s.step = 'fin';
      try { if (window.otdRepRefrescar) window.otdRepRefrescar(d.k === 'i' ? 'i' + d.id : d.id); } catch (e2) { /* nada */ }
      render(true);
    },
    /* Sube los archivos que faltan (fotos 2..N y el video). Cada resultado se anota y se guarda; si todos suben, recién ahí se borra el borrador. */
    subirPendientes: async function (s) {
      var d = s.d, todos = todosArchivos(s), r = s.res || {}, nFoto = 0;
      for (var i = 0; i < todos.length; i++) {
        var it = todos[i];
        if (!it.esVideo) nFoto++;
        if (s.subidas[i]) continue;
        s.progreso = it.esVideo ? 'Subiendo el video…' : 'Subiendo la foto ' + nFoto + ' de ' + todos.filter(function (x) { return !x.esVideo; }).length + '…';
        if (S === s) marco();
        var f2 = new FormData();
        f2.append('evidencia', it.file, it.file.name || (it.esVideo ? 'video.mp4' : 'foto.jpg'));
        var sidRef = null;                                // la foto de UNA pieza se asocia a SU solicitud; las generales, a todo el lote
        if (it.ref) {
          var xr = (r.solicitudes || []).filter(function (x) { return x.ref === it.ref; })[0];
          sidRef = xr ? xr.solicitud_id : null;
        }
        if (sidRef) f2.append('solicitud_id', sidRef); else f2.append('lote_id', r.lote_id || s.loteId);
        var r2 = await api('POST', base(d) + '/evidencia', f2);
        if (r2.ok) s.subidas[i] = true;
        await guardarBorrador(s);
      }
      s.fotoExtraFallo = s.subidas.filter(function (x) { return !x; }).length;
      s.progreso = '';
      if (!s.fotoExtraFallo) borrarBorrador(s.key);
    }
  };

  /* ───────────────────────────── flujo: NO SE PUEDE USAR (fuera de servicio) ───────────────────────────── */
  var fuera = {
    init: function (s) { s.motivo = ''; s.detalle = ''; s.riesgo = ''; s.dirty = false; },
    semilla: function (s, sem) { semillaBase(s, sem); if (sem.motivo) s.motivo = sem.motivo; if (sem.texto) s.detalle = sem.texto; },
    estado: function (s) {
      var min = (LISTAS().min || {}).fuera_servicio || 10;
      var okF = s.fotos.length > 0, okQ = !!s.motivo && s.detalle.replace(/\s/g, '').length >= min, okR = !!s.riesgo;
      var falta = !okF ? { t: 'la foto', go: 'oaSc-foto' } : !okQ ? { t: 'qué pasa', go: 'oaSc-que' } : !okR ? { t: 'si puede lastimar a alguien', go: 'oaSc-riesgo' } : null;
      return { okF: okF, okQ: okQ, okR: okR, falta: falta, ok: [okF, okQ, okR].filter(Boolean).length, min: min };
    },
    vista: function (s) {
      if (s.step === 'fin') return fuera.fin(s);
      var e = fuera.estado(s), L = LISTAS().fuera_servicio || [];
      var body = '<h2 class="oa-hq">Dejar fuera de servicio</h2><p class="oa-hsub">Para que nadie lo use mientras se arregla. ' +
        (s.d.k === 'e' ? 'Queda marcado en la ficha del cliente.' : 'Queda marcado en el acta de recepción.') + '</p>' + errHTML() + sugBanner(s);
      body += sc('oaSc-foto', e.okF, 1, 'Foto de lo que pasa', 'Obligatoria', mediaCuerpo('Sin foto no hay aviso: es la evidencia de por qué se detuvo.'));
      body += sc('oaSc-que', e.okQ, 2, '¿Qué pasa?', 'Obligatoria', '<div class="oa-chips-h">Elige lo que más se parece.</div>' + chipsHTML(L, s.motivo, 'motivo') +
        '<div style="margin-top:12px">' + textoHTML('detalle', s.detalle, 'Cuéntalo con tus palabras (mínimo ' + e.min + ' letras)', e.min) + '</div>');
      body += sc('oaSc-riesgo', e.okR, 3, '¿Puede lastimar a alguien?', 'Obligatoria',
        '<div class="oa-opt2">' + opcion(s.riesgo === 'si', 'r', 'exclamation-triangle-fill', 'Sí, puede lastimar', (s.sug && s.sug.riesgo === 'si' && !s.riesgo ? 'Sugerido: confírmalo tú' : 'El aviso sale urgente'), 'riesgo', 'si') +
        opcion(s.riesgo === 'no', 'g', 'shield-check', 'No, solo está malo', 'No es peligroso', 'riesgo', 'no') + '</div>' +
        (s.riesgo === 'si' ? '<div class="oa-urg" style="margin:12px 0 0"><i class="bi bi-cone-striped"></i><span><b>Pon un letrero o huincha antes de irte</b>, y desenchúfalo si tiene enchufe, para que nadie lo use.</span></div>' : ''));
      var foot = e.falta ? faltaBtn(e.falta.t, e.falta.go) :
        '<button type="button" class="oa-btn danger block lg" data-a="enviar"' + (s.enviando ? ' aria-busy="true"' : '') + '>' + (s.enviando ? '<span class="oa-spin"></span> Enviando…' : '<i class="bi bi-sign-stop"></i> Dejar fuera de servicio') + '</button>';
      return { top: topHTML({ eyebrow: 'No se puede usar', prog: { ok: e.ok, n: 3, falta: e.falta && e.falta.t } }), body: body, foot: foot };
    },
    enviar: async function (s) {
      var e = fuera.estado(s);
      if (e.falta || s.enviando) return;
      s.enviando = true; s.err = ''; marco();
      var d = s.d, fd = new FormData(), url;
      if (d.k === 'i') { fd.append('accion', 'fuera_servicio'); url = base(d) + '/estado'; } else url = base(d) + '/fuera-servicio';
      fd.append('motivo_clave', s.motivo); fd.append('detalle', s.detalle.trim()); fd.append('riesgo', s.riesgo);
      s.fotos.forEach(function (f) { fd.append('fotos', f.file, f.file.name || 'foto.jpg'); });
      var j = await api('POST', url, fd);
      if (S !== s) return;
      s.enviando = false;
      if (!j.ok) { s.err = j.error || 'No se pudo guardar.'; render(false); return; }
      s.res = j; s.step = 'fin'; render(true);
    },
    fin: function (s) {
      var d = s.d, r = s.res || {}, riesgo = s.riesgo === 'si';
      var lineas = [
        ['sign-stop', d.k === 'e' ? 'Quedó fuera de servicio en la ficha del cliente.' : 'Quedó fuera de servicio en esta OT y en el acta de recepción.', 'a'],
        ['chat-square-text', 'Le pasa: <b>' + esc(((LISTAS().fuera_servicio || []).filter(function (m) { return m[0] === s.motivo; })[0] || [0, s.motivo])[1]) + '</b>.']
      ];
      if (r.ticket) lineas.push(['ticket-perforated', 'Salió un aviso urgente: <b>' + esc(r.ticket) + '</b>.']);
      if (riesgo) lineas.push(['cone-striped', 'Pon un letrero o huincha antes de irte, para que nadie lo use.', 'w']);
      if (r.aviso) lineas.push(['exclamation-triangle-fill', esc(r.aviso), 'w']);
      return { top: '<div class="oa-top"><div class="oa-r1"><span class="oa-eb">Fuera de servicio</span></div></div>',
               body: finGenerico({ tono: 'r', icono: 'sign-stop', h: 'Listo.', lead: 'Quedó fuera de servicio.', det: 'No se puede usar', lineas: lineas }),
               foot: '<button type="button" class="oa-btn blue block lg" data-a="volverrec">Volver a la OT</button>' };
    }
  };

  /* ───────────────────────────── flujo: NO TIENE ARREGLO (dar de baja) ───────────────────────────── */
  var baja = {
    init: function (s) { s.motivo = ''; s.detalle = ''; s.dirty = false; },
    semilla: function (s, sem) { semillaBase(s, sem); if (sem.motivo) s.motivo = sem.motivo; if (sem.texto) s.detalle = sem.texto; },
    estado: function (s) {
      var min = (LISTAS().min || {}).baja || 12;
      var okF = s.fotos.length > 0, okM = !!s.motivo, okT = s.detalle.replace(/\s/g, '').length >= min;
      var falta = !okF ? { t: 'la foto', go: 'oaSc-foto' } : !okM ? { t: 'el motivo', go: 'oaSc-mot' } : !okT ? { t: 'el diagnóstico', go: 'oaSc-diag' } : null;
      return { okF: okF, okM: okM, okT: okT, falta: falta, ok: [okF, okM, okT].filter(Boolean).length, min: min };
    },
    vista: function (s) {
      if (s.step === 'fin') return baja.fin(s);
      var e = baja.estado(s), L = LISTAS().baja || [], d = s.d;
      var body = '<h2 class="oa-hq">No tiene arreglo</h2><p class="oa-hsub">Solo si no se puede recuperar. ' + (d.k === 'e' ? 'Sale del parque del cliente.' : 'Queda como «Dar de baja» en el acta.') + '</p>' + errHTML() + sugBanner(s);
      body += sc('oaSc-foto', e.okF, 1, 'Foto del ' + (d.k === 'e' ? 'equipo' : 'producto') + ' como quedó', 'Obligatoria', mediaCuerpo('Dar de baja exige al menos una foto.'));
      body += sc('oaSc-mot', e.okM, 2, '¿Por qué no tiene arreglo?', 'Obligatoria', chipsHTML(L, s.motivo, 'motivo'));
      body += sc('oaSc-diag', e.okT, 3, 'Diagnóstico', 'Obligatoria', textoHTML('detalle', s.detalle, 'Qué le pasó y por qué no se puede recuperar (mínimo ' + e.min + ' letras)', e.min) +
        '<button type="button" class="oa-dictar" data-a="dictar"><i class="bi bi-mic-fill"></i> Dictar con la voz</button>');
      body += '<div class="oa-sc info"><div class="oa-sc-h"><span class="oa-sc-c"><i class="bi bi-info-lg"></i></span><span class="oa-sc-t">Qué pasa al darlo de baja</span></div><div class="oa-info-l" style="margin:0">' +
        (d.k === 'e' ? '<div><i class="bi bi-building"></i><span>Deja de aparecer en los equipos del cliente.</span></div><div><i class="bi bi-arrow-counterclockwise"></i><span>Si fue un error, un administrador puede volver a activarlo.</span></div>'
                      : '<div><i class="bi bi-clipboard-check"></i><span>Queda registrado en la OT y en el acta, con tu nombre y la hora.</span></div><div><i class="bi bi-arrow-counterclockwise"></i><span>Si fue un error, gestión lo puede devolver a operativo.</span></div>') + '</div></div>';
      var foot = e.falta ? faltaBtn(e.falta.t, e.falta.go) :
        '<button type="button" class="oa-btn danger block lg" data-a="enviar"' + (s.enviando ? ' aria-busy="true"' : '') + '>' + (s.enviando ? '<span class="oa-spin"></span> Enviando…' : '<i class="bi bi-slash-circle"></i> Dar de baja') + '</button>';
      return { top: topHTML({ eyebrow: 'No tiene arreglo', prog: { ok: e.ok, n: 3, falta: e.falta && e.falta.t } }), body: body, foot: foot };
    },
    enviar: async function (s) {
      var e = baja.estado(s);
      if (e.falta || s.enviando) return;
      s.enviando = true; s.err = ''; marco();
      var d = s.d, fd = new FormData(), j;
      var mtxt = ((LISTAS().baja || []).filter(function (m) { return m[0] === s.motivo; })[0] || [0, s.motivo])[1];
      if (d.k === 'i') {
        fd.append('accion', 'baja'); fd.append('motivo_clave', s.motivo); fd.append('detalle', s.detalle.trim());
        s.fotos.forEach(function (f) { fd.append('fotos', f.file, f.file.name || 'foto.jpg'); });
        j = await api('POST', base(d) + '/estado', fd);
      } else {
        fd.append('motivo', mtxt + ' — ' + s.detalle.trim());
        fd.append('evidencia', s.fotos[0].file, s.fotos[0].file.name || 'foto.jpg');
        j = await api('POST', base(d) + '/dar-baja', fd);
        for (var i = 1; j.ok && i < s.fotos.length; i++) {
          var f2 = new FormData();
          f2.append('evidencia', s.fotos[i].file, s.fotos[i].file.name || 'foto.jpg'); f2.append('contexto', 'baja');
          await api('POST', base(d) + '/evidencia', f2);
        }
      }
      if (S !== s) return;
      s.enviando = false;
      if (!j.ok) { s.err = j.error || 'No se pudo dar de baja.'; render(false); return; }
      s.res = j; s.step = 'fin'; render(true);
    },
    fin: function (s) {
      var d = s.d;
      return { top: '<div class="oa-top"><div class="oa-r1"><span class="oa-eb">No tiene arreglo</span></div></div>',
               body: finGenerico({ tono: 'n', icono: 'slash-circle', h: 'Listo.', lead: 'Quedó dado de baja.', det: 'No tiene arreglo',
                 lineas: [['clipboard-check', 'Quedó registrado en la OT con tu nombre y la hora.'],
                          d.k === 'e' ? ['building', 'Salió del parque del cliente. Un administrador puede restaurarlo.', 'a'] : ['file-earmark-text', 'Va al acta de recepción como «Dar de baja».']] }),
               foot: '<button type="button" class="oa-btn blue block lg" data-a="volverrec">Volver a la OT</button>' };
    }
  };

  /* ───────────────────────────── flujo: NO LLEGÓ (producto de la revisión) ───────────────────────────── */
  // 2026-10-20 (revisión del técnico senior): «Llegó incompleto» NO es «No llegó»: el producto SÍ está y se revisa; lo que
  // falta se pide con «Pedir repuesto» o se avisa con «Informar incidencia». Antes, marcarlo así dejaba sin revisar un
  // producto que sí llegó (dispensaba su checklist).
  var QUE_NL = [['no_venia', 'No venía en el despacho'], ['otro_producto', 'Llegó otro producto']];
  var nollego = {
    init: function (s) { s.modo = (s.d.cant || 1) > 1 ? '' : 'todos'; s.cuantos = (s.d.cant || 1) > 1 ? null : 1; s.que = ''; s.quien = ''; s.quienNombre = ''; s.dirty = false; },
    semilla: function (s, sem) { semillaBase(s, sem); if (sem.que) s.que = sem.que; },
    estado: function (s) {
      var varios = (s.d.cant || 1) > 1;
      var okC = !varios || s.cuantos != null, okQ = !!s.que, okW = !!s.quien;
      var falta = !okC ? { t: 'cuántas unidades no llegaron', go: 'oaSc-cuantos' } : !okQ ? { t: 'qué pasó', go: 'oaSc-quepaso' } : !okW ? { t: 'a quién le preguntaste', go: 'oaSc-quien' } : null;
      return { okC: okC, okQ: okQ, okW: okW, falta: falta, varios: varios, ok: [varios ? okC : true, okQ, okW].filter(Boolean).length - (varios ? 0 : 1) + (varios ? 0 : 0), n: varios ? 3 : 2 };
    },
    vista: function (s) {
      if (s.step === 'fin') return nollego.fin(s);
      var e = nollego.estado(s), cant = s.d.cant || 1, L = LISTAS().consultado || [];
      var ok = (e.varios ? [e.okC] : []).concat([e.okQ, e.okW]).filter(Boolean).length;
      var body = '<h2 class="oa-hq">¿Qué pasó con este producto?</h2><p class="oa-hsub">' + (e.varios ? 'Vienen ' + cant + ' unidades. ' : '') + 'Sin foto: solo cuéntanos qué pasó y a quién le preguntaste.</p>' + errHTML() + sugBanner(s);
      var n = 1;
      if (e.varios) {
        body += sc('oaSc-cuantos', e.okC, n++, '¿Cuántas unidades no llegaron? <span class="oa-fld-x">de ' + cant + '</span>', '',
          '<div class="oa-opt3">' + opcion(s.modo === 'uno', 'a', '1-circle', 'Faltó 1 unidad', 'Llegaron ' + (cant - 1), 'nlmodo', 'uno') +
          opcion(s.modo === 'todos', 'a', 'box-seam', 'No llegó ninguna', 'Faltan las ' + cant, 'nlmodo', 'todos') +
          opcion(s.modo === 'otro', 'a', '123', 'Otro número', 'Elige cuántas', 'nlmodo', 'otro') + '</div>' +
          (s.modo === 'otro' ? '<div style="margin-top:12px;display:flex;align-items:center;gap:10px"><span class="oa-fld-l" style="margin:0">Faltan</span><span class="oa-qty"><button type="button" data-a="nlmenos" aria-label="Una menos"><i class="bi bi-dash-lg"></i></button><output>' + (s.cuantos || 1) + '</output><button type="button" data-a="nlmas" aria-label="Una más"><i class="bi bi-plus-lg"></i></button></span><b>de ' + cant + '</b></div>' : '') +
          (s.cuantos != null && s.cuantos < cant ? '<div class="oa-note"><i class="bi bi-info-circle"></i><span>Las otras ' + (cant - s.cuantos) + ' sí llegaron: se revisan con el checklist como siempre.</span></div>' : ''));
      }
      body += sc('oaSc-quepaso', e.okQ, n++, '¿Qué pasó?', '', chipsHTML(QUE_NL, s.que, 'nlque') +
        '<div class="oa-note"><i class="bi bi-info-circle"></i><span>¿Llegó, pero incompleto o dañado? Entonces sí llegó: usa «Pedir repuesto» o «Informar incidencia».</span></div>');
      body += sc('oaSc-quien', e.okW, n++, '¿A quién le preguntaste?', '', chipsHTML(L, s.quien, 'nlquien') +
        (s.quien && s.quien !== 'nadie' ? '<div style="margin-top:12px"><label class="oa-fld-l" for="oaNQ">Nombre <span class="oa-fld-x">(opcional)</span></label><input id="oaNQ" class="oa-inp" data-in="quienNombre" value="' + esc(s.quienNombre) + '" maxlength="80" placeholder="Con quién hablaste"></div>' : ''));
      body += '<div class="oa-sc info"><div class="oa-sc-h"><span class="oa-sc-c"><i class="bi bi-clipboard-check"></i></span><span class="oa-sc-t">Qué pasa después</span></div>' +
        '<div style="color:var(--oa-muted)">Queda anotado en la OT. No tienes que revisar ' + (e.varios ? 'las unidades que no llegaron' : 'este producto') + '.</div>' +
        '<div class="oa-note"><i class="bi bi-arrow-counterclockwise"></i><span>Si aparece, gestión lo puede devolver a «operativo» y lo revisas como los demás.</span></div></div>';
      var foot = e.falta ? faltaBtn(e.falta.t, e.falta.go) :
        '<button type="button" class="oa-btn blue block lg" data-a="enviar"' + (s.enviando ? ' aria-busy="true"' : '') + '>' + (s.enviando ? '<span class="oa-spin"></span> Enviando…' : '<i class="bi bi-box-seam"></i> Marcar como no llegó') + '</button>';
      return { top: topHTML({ eyebrow: 'No llegó', prog: { ok: ok, n: e.n, falta: e.falta && e.falta.t } }), body: body, foot: foot };
    },
    enviar: async function (s) {
      var e = nollego.estado(s);
      if (e.falta || s.enviando) return;
      s.enviando = true; s.err = ''; marco();
      var d = s.d, fd = new FormData();
      fd.append('accion', 'no_llego'); fd.append('cantidad', String(s.cuantos == null ? 1 : s.cuantos));
      fd.append('consultado_clave', s.quien); fd.append('consultado_detalle', s.quienNombre.trim());
      fd.append('detalle', (QUE_NL.filter(function (q) { return q[0] === s.que; })[0] || [0, ''])[1]);
      var j = await api('POST', base(d) + '/estado', fd);
      if (S !== s) return;
      s.enviando = false;
      if (!j.ok) { s.err = j.error || 'No se pudo guardar.'; render(false); return; }
      s.res = j; s.step = 'fin'; render(true);
    },
    fin: function (s) {
      return { top: '<div class="oa-top"><div class="oa-r1"><span class="oa-eb">No llegó</span></div></div>',
               body: finGenerico({ tono: 'n', icono: 'box-seam', h: 'Anotado.', lead: 'Quedó marcado como no llegó.', det: 'No llegó', dib: 'otra',
                 lineas: [['clipboard-check', 'Quedó en la OT con tu nombre y la hora.'], ['file-earmark-text', 'Va al acta de recepción como «No llegó».']] }),
               foot: '<button type="button" class="oa-btn blue block lg" data-a="volverrec">Volver a la OT</button>' };
    }
  };

  /* ───────────────────────────── flujo: NO ESTÁ / NO LLEGÓ (equipo de una OT de cliente) ───────────────────────────── */
  var noesta = {
    init: function (s) { s.op = ''; s.motivo = ''; s.detalle = ''; s.dirty = false; },
    semilla: function (s, sem) { semillaBase(s, sem); s.op = sem.opcion || ''; s.motivo = sem.motivo || ''; if (sem.texto) s.detalle = sem.texto; },
    lista: function (s) { return (LISTAS()[s.op] || []); },
    estado: function (s) {
      var okO = !!s.op, okM = !!s.motivo;
      var okD = s.motivo !== 'otro' || s.detalle.replace(/\s/g, '').length >= ((LISTAS().min || {}).equipo || 10);
      // 30-oct (Daniel: «exige también la foto en la barra de acciones»): «No está» y «Otra razón» llevan foto obligatoria; solo
      // «No llegó · falta stock» queda sin foto (no hay nada que fotografiar).
      var pideFoto = !!s.op && s.op !== 'falta_stock', okF = !pideFoto || s.fotos.length > 0;
      var falta = !okO ? { t: 'qué pasó con el equipo', go: 'oaSc-op' } : !okM ? { t: 'el motivo', go: 'oaSc-motivo' } : !okD ? { t: 'contar qué pasó', go: 'oaSc-motivo' } :
        !okF ? { t: 'la foto', go: 'oaSc-foto' } : null;
      return { okO: okO, okM: okM && okD, okF: okF, falta: falta, ok: [okO, okM && okD, okF].filter(Boolean).length, n: pideFoto ? 3 : 2 };
    },
    vista: function (s) {
      if (s.step === 'fin') return noesta.fin(s);
      var e = noesta.estado(s), min = (LISTAS().min || {}).equipo || 10;
      var body = '<h2 class="oa-hq">¿Qué pasó con este equipo?</h2><p class="oa-hsub">Así el equipo deja de bloquear la firma y el cierre, y queda escrito para quien siga con esto.</p>' + errHTML() + sugBanner(s);
      body += sc('oaSc-op', e.okO, 1, '¿Qué pasó?', '',
        '<div class="oa-opt3">' + opcion(s.op === 'no_esta', 'a', 'geo-alt', 'No está en el lugar', 'No lo encuentro', 'nop', 'no_esta') +
        opcion(s.op === 'falta_stock', 'a', 'box-seam', 'No llegó · falta stock', 'No hay stock o no llegó', 'nop', 'falta_stock') +
        opcion(s.op === 'no_revisado', 'a', 'three-dots', 'Otra razón', 'Sin acceso, en uso…', 'nop', 'no_revisado') + '</div>');
      if (s.op) {
        body += sc('oaSc-motivo', e.okM, 2, 'Motivo', 'Obligatorio', chipsHTML(noesta.lista(s), s.motivo, 'nmot') +
          '<div style="margin-top:12px">' + textoHTML('detalle', s.detalle, s.motivo === 'otro' ? 'Cuéntalo con tus palabras (mínimo ' + min + ' letras)' : 'Algo más que ayude (opcional)', s.motivo === 'otro' ? min : 0) + '</div>');
        if (s.op === 'falta_stock') {
          body += '<div class="oa-sc info"><div class="oa-sc-h"><span class="oa-sc-c"><i class="bi bi-info-lg"></i></span><span class="oa-sc-t">Sin foto</span></div><div style="color:var(--oa-muted)">«No llegó» no lleva foto: no hay nada que fotografiar. Gestión y bodega ven este equipo como pendiente por falta de stock.</div></div>';
        } else {
          body += sc('oaSc-foto', e.okF, 3, 'Foto', 'Obligatoria', mediaCuerpo('Es la evidencia: el lugar vacío, un letrero, el equipo bloqueado.'));
        }
      }
      var foot = e.falta ? faltaBtn(e.falta.t, e.falta.go) :
        '<button type="button" class="oa-btn blue block lg" data-a="enviar"' + (s.enviando ? ' aria-busy="true"' : '') + '>' + (s.enviando ? '<span class="oa-spin"></span> Enviando…' : '<i class="bi bi-check2-circle"></i> Dejarlo anotado') + '</button>';
      return { top: topHTML({ eyebrow: 'No está / no llegó', prog: { ok: e.ok, n: e.n, falta: e.falta && e.falta.t } }), body: body, foot: foot };
    },
    enviar: async function (s) {
      var e = noesta.estado(s);
      if (e.falta || s.enviando) return;
      s.enviando = true; s.err = ''; marco();
      var fd = new FormData();
      fd.append('opcion', s.op); fd.append('motivo_clave', s.motivo); fd.append('detalle', s.detalle.trim());
      if (s.op !== 'falta_stock') s.fotos.forEach(function (f) { fd.append('fotos', f.file, f.file.name || 'foto.jpg'); });
      var j = await api('POST', base(s.d) + '/no-esta', fd);
      if (S !== s) return;
      s.enviando = false;
      if (!j.ok) { s.err = j.error || 'No se pudo guardar.'; render(false); return; }
      s.res = j; s.step = 'fin'; s.d.np = j.estado; render(true);
    },
    fin: function (s) {
      var np = s.res && s.res.estado || {};
      return { top: '<div class="oa-top"><div class="oa-r1"><span class="oa-eb">No está / no llegó</span></div></div>',
               body: finGenerico({ tono: 'a', icono: 'box-seam', h: 'Anotado.', lead: np.chip || 'Quedó anotado.', det: np.chip || '',
                 lineas: [['clipboard-check', 'Quedó en la OT con tu nombre y la hora.'], ['pen', 'El equipo ya no bloquea la firma ni el cierre de la OT.'],
                          s.op === 'falta_stock' ? ['box-seam', 'Quedó como pendiente por falta de stock, para que gestión y bodega lo vean.', 'a'] : ['arrow-counterclockwise', 'Si aparece, gestión lo puede devolver a su estado normal.']] }),
               /* 2026-10-20: con «No llegó · falta stock» se ofrece seguir con «¿Qué pasó?» (static/ot_que_paso.js): los equipos pendientes pasan a una
                  OT hija sin fecha, con el mismo documento. Solo suma un botón; «Volver a la OT» sigue igual. */
               foot: (s.op === 'falta_stock' && window.OTQuePaso ? '<button type="button" class="oa-btn line block lg" data-a="quepaso" style="margin-bottom:10px"><i class="bi bi-signpost-split"></i> Dejar lo pendiente en una OT sin fecha</button>' : '') +
                     '<button type="button" class="oa-btn blue block lg" data-a="volverrec">Volver a la OT</button>' };
    }
  };

  /* ───────────────────────────── flujo: INFORMAR INCIDENCIA ───────────────────────────── */
  var incidencia = {
    init: function (s) { s.motivo = ''; s.detalle = ''; s.cant = 1; s.dirty = false; },
    semilla: function (s, sem) { semillaBase(s, sem); if (sem.texto) s.motivo = sem.texto; },
    estado: function (s) {
      var okF = s.fotos.length > 0, okQ = s.motivo.replace(/\s/g, '').length >= 10;
      var falta = !okF ? { t: 'la foto', go: 'oaSc-foto' } : !okQ ? { t: 'qué pasó', go: 'oaSc-que' } : null;
      return { okF: okF, okQ: okQ, falta: falta, ok: [okF, okQ].filter(Boolean).length };
    },
    vista: function (s) {
      if (s.step === 'fin') return incidencia.fin(s);
      var e = incidencia.estado(s), d = s.d, tope = d.cant || 1;
      var body = '<h2 class="oa-hq">Informar incidencia</h2><p class="oa-hsub">Un daño o problema que bodega y gestión tienen que ver. Queda con tu nombre y la hora.</p>' + errHTML() + sugBanner(s);
      body += sc('oaSc-foto', e.okF, 1, 'Foto del daño', 'Obligatoria', mediaCuerpo('Es la evidencia de la incidencia.'));
      body += sc('oaSc-que', e.okQ, 2, '¿Qué pasó?', 'Obligatoria', textoHTML('motivo', s.motivo, 'Cuéntalo en una frase (mínimo 10 letras)', 10) +
        '<button type="button" class="oa-dictar" data-a="dictar"><i class="bi bi-mic-fill"></i> Dictar con la voz</button>' +
        '<div style="margin-top:12px"><label class="oa-fld-l" for="oaDet">Más detalle <span class="oa-fld-x">(opcional)</span></label><textarea id="oaDet" class="oa-inp" data-in="detalle" rows="2" maxlength="1000" placeholder="Dónde, cuándo, qué lo causó">' + esc(s.detalle) + '</textarea></div>');
      if (tope > 1) {
        body += sc('oaSc-cant', true, 3, '¿Cuántas unidades están afectadas?', 'Opcional',
          '<span class="oa-qty"><button type="button" data-a="incmenos" aria-label="Una menos"><i class="bi bi-dash-lg"></i></button><output>' + s.cant + '</output><button type="button" data-a="incmas" aria-label="Una más"><i class="bi bi-plus-lg"></i></button></span> <b>de ' + tope + '</b>', 'opt');
      }
      var foot = e.falta ? faltaBtn(e.falta.t, e.falta.go) :
        '<button type="button" class="oa-btn blue block lg" data-a="enviar"' + (s.enviando ? ' aria-busy="true"' : '') + '>' + (s.enviando ? '<span class="oa-spin"></span> Enviando…' : '<i class="bi bi-send-fill"></i> Informar incidencia') + '</button>';
      return { top: topHTML({ eyebrow: 'Informar incidencia', prog: { ok: e.ok, n: 2, falta: e.falta && e.falta.t } }), body: body, foot: foot };
    },
    enviar: async function (s) {
      var e = incidencia.estado(s);
      if (e.falta || s.enviando) return;
      s.enviando = true; s.err = ''; marco();
      var fd = new FormData();
      fd.append('motivo', s.motivo.trim()); fd.append('detalle', s.detalle.trim()); fd.append('cantidad', String(s.cant));
      s.fotos.forEach(function (f) { fd.append('fotos', f.file, f.file.name || 'foto.jpg'); });
      var j = await api('POST', base(s.d) + '/incidencia', fd);
      if (S !== s) return;
      s.enviando = false;
      if (!j.ok) { s.err = j.error || 'No se pudo informar.'; render(false); return; }
      s.res = j; s.step = 'fin'; render(true);
    },
    fin: function (s) {
      var r = s.res || {};
      return { top: '<div class="oa-top"><div class="oa-r1"><span class="oa-eb">Incidencia informada</span></div></div>',
               body: finGenerico({ tono: '', icono: 'check-lg', h: 'Listo.', lead: 'Incidencia N° ' + (r.numero || r.incidencia_id || '') + ' registrada.', det: s.motivo.trim().slice(0, 80),
                 lineas: [['person-check', 'Quedó con tu nombre y la hora.'], ['megaphone', 'Gestión y bodega la ven en el módulo de Incidencias.']].concat(r.aviso ? [['exclamation-triangle-fill', esc(r.aviso), 'w']] : []) }),
               foot: '<button type="button" class="oa-btn blue block lg" data-a="volver">Volver a la OT</button>' };
    }
  };

  /* ───────────────────────────── flujo: «¿QUÉ LE PASA?» (diagnóstico guiado) ─────────────────────────────
     2026-10-20 (Daniel: «que se vea inteligente, actuando como un técnico senior… diagnosticar en una desviación»). Una pregunta por
     pantalla, máximo 3, «Otra cosa» en cada una y el atajo «Puede lastimar a alguien» siempre a la vista. El árbol lo manda el servidor
     según el sistema del equipo (ot_piezas_medidas.arbol). Termina en «Te sugiero…» con «Seguir con esto» y «Elegir otra cosa».
     REGLA #32: el sistema SUGIERE, la persona DECIDE: nada se crea ni se marca solo; «Seguir con esto» abre el flujo de siempre con la
     pieza o el motivo ya mostrados como sugerencia (parámetro `semilla`, que se aplica DESPUÉS de init()). */
  var PERM = { pedir: 'ped', fs: 'fs', baja: 'baja', incidencia: 'inc', noesta: 'noe', nollego: 'nol' };
  var ACC_IC = { pedir: ['wrench-adjustable', 'b'], fs: ['sign-stop', 'r'], baja: ['slash-circle', 'n'], incidencia: ['megaphone', ''],
                 noesta: ['geo-alt', 'a'], nollego: ['box-seam', 'a'], esperar: ['hourglass-split', 'a'] };
  var ATAJO_LOCAL = { accion: 'fs', motivo: 'pieza_rota', riesgo: 'si', texto: '',
                      por_que: 'Si algo puede lastimar a alguien, lo primero es que nadie lo use. Tú confirmas el riesgo en la pantalla que sigue.',
                      aviso: 'Pon un letrero o huincha y desenchúfalo si tiene enchufe, antes de irte.' };
  function permitida(s, accion) {
    if (accion === 'esperar') return true;
    var k = PERM[accion], d = s.d || {};
    if (!k || !d.p || !d.p[k]) return false;
    if (accion === 'fs' && (d.fsb || (d.est && d.est.estado === 'fuera_servicio'))) return false;      // ya está fuera de servicio
    if (accion === 'baja' && (d.bj || (d.est && d.est.estado === 'baja'))) return false;
    return true;
  }
  function textoPiezas(r) { return 'Hace falta: ' + (r.piezas || []).join(' o '); }
  /* Si el rol de la persona no puede hacer la acción sugerida (ej. un técnico externo no da de baja), se le ofrece lo más parecido. */
  function efectivo(s, r) {
    if (!r) return null;
    if (permitida(s, r.accion)) return r;
    if (r.accion === 'esperar') return r;
    if (!permitida(s, 'incidencia')) return null;
    var base = r.accion === 'baja' ? 'No tiene arreglo (que ILUS decida): ' + (r.texto || r.por_que || '')
             : r.accion === 'pedir' ? textoPiezas(r) + (r.texto ? ' · ' + r.texto : '') : (r.texto || r.por_que || '');
    return Object.assign({}, r, { accion: 'incidencia', piezas: undefined, texto: base.trim(),
                                  por_que: r.accion === 'baja' ? 'Tú no das de baja: avisas que no tiene arreglo y ILUS decide.' : r.por_que });
  }
  function tituloRes(r) {
    var e = { pedir: 'Pedir repuesto', fs: 'Dejarlo fuera de servicio', baja: 'Darlo de baja: no tiene arreglo', incidencia: 'Informar una incidencia',
              nollego: 'Anotar «No llegó»', esperar: 'Antes de seguir…' }[r.accion] || 'Seguir';
    if (r.accion === 'noesta') e = { no_esta: 'Anotar que no está en el lugar', falta_stock: 'Anotar «No llegó · falta stock»', no_revisado: 'Anotar que no se pudo revisar' }[r.opcion] || 'Anotar qué pasó con el equipo';
    if (r.accion === 'pedir' && r.piezas && r.piezas.length) return 'Pedir: ' + r.piezas.map(function (p) { return '<span class="oa-sug-pz">' + esc(sinPar(p)) + '</span>'; }).join('<span class="oa-sug-o"> o </span>');
    return esc(e);
  }
  function diagPayload(s, r) {
    var m = MED(s);
    return { camino: s.camino.slice(0, 6), accion: r.accion, piezas: r.piezas || [], sistema: (m && m.sistema) || '', atajo: !!s.atajo };
  }
  function semillaDe(s, r) { return Object.assign({}, r, { ops: s.ops, diag: diagPayload(s, r) }); }
  /* Lo que cada flujo hace al recibir la sugerencia: se muestra con su motivo y la persona lo cambia o lo confirma. */
  function semillaBase(s, sem) {
    s.sug = { por_que: sem.por_que || '', aviso: sem.aviso || '', riesgo: sem.riesgo || '', piezas: sem.piezas || [], titulo: sem.titulo || '' };
    s.pend = sem.ademas || [];
    s.pendCtx = { ops: sem.ops || null, diag: sem.diag || null };
    if (sem.ops && !s.ops) s.ops = sem.ops;
  }
  function sugBanner(s) {
    if (!s.sug) return '';
    return '<div class="oa-sugb"><i class="bi bi-clipboard2-pulse"></i><div><b>' + esc(s.sug.titulo || 'Sugerido por «¿Qué le pasa?»') + '</b>' + (s.sug.por_que ? '<span>' + esc(s.sug.por_que) + '</span>' : '') +
      '<span class="s">Puedes cambiarlo: tú decides.</span></div></div>' +
      (s.sug.aviso ? '<div class="oa-urg"><i class="bi bi-exclamation-triangle-fill"></i><span>' + esc(s.sug.aviso) + '</span></div>' : '');
  }
  function pendTxt(p) {
    if (p.rotulo) return p.rotulo;
    if (p.accion === 'pedir') return 'Pedir: ' + (p.piezas || []).map(sinPar).join(' o ');
    if (p.accion === 'incidencia') return 'Informar una incidencia' + (p.texto ? ': ' + p.texto : '');
    return { fs: 'Dejarlo fuera de servicio', baja: 'Dar de baja' }[p.accion] || 'Otra acción';
  }
  function pendLista(s) { return (s.pend || []).filter(function (p) { return permitida(s, p.accion); }); }
  function pendHTML(s) {
    var l = pendLista(s);
    if (!l.length) return '';
    return '<div class="oa-pend"><div class="oa-pend-t"><i class="bi bi-clipboard2-pulse"></i> También te sugerí</div>' + l.map(function (p, i) {
      return '<div class="oa-pend-i"><span>' + esc(pendTxt(p)) + '</span><button type="button" class="oa-btn line" data-a="pend" data-i="' + i + '">Hacerlo ahora</button></div>';
    }).join('') + '<small>Nada se hace solo: tú decides.</small></div>';
  }
  function peligroFoot() {
    return '<button type="button" class="oa-peligro" data-a="peligro"><i class="bi bi-cone-striped"></i><span>Puede lastimar a alguien<small>Si es peligroso, toca aquí: no se usa hasta que se revise</small></span></button>';
  }

  var diag = {
    init: function (s) {
      s.hist = []; s.camino = []; s.nodo = null; s.res = null; s.arbol = null; s.cargando = true; s.ops = null; s.atajo = false; s.dirty = false;
      api('GET', base(s.d) + '/repuestos-opciones').then(function (j) {
        if (S !== s) return;
        s.cargando = false;
        if (j.ok && j.medidas && j.medidas.arbol) { s.ops = j; s.arbol = j.medidas.arbol; s.nodo = s.arbol.raiz; }
        else s.opsErr = j.error || 'No pudimos traer las preguntas.';
        render(false);
      });
    },
    vista: function (s) {
      var top = topHTML({ eyebrow: '¿Qué le pasa?', atras: !!(s.hist.length || s.res) });
      if (s.res) return diag.resultado(s, top);
      if (s.cargando) return { top: top, body: '<h2 class="oa-hq">¿Qué le pasa a ' + esc(sinPar(s.d.n)) + '?</h2><div class="oa-hint"><i class="bi bi-arrow-repeat"></i> Preparando las preguntas…</div>', foot: peligroFoot() };
      if (s.opsErr || !s.arbol) {
        return { top: top, body: '<div class="oa-urg"><i class="bi bi-wifi-off"></i><span>No pudimos traer las preguntas. Puedes usar los otros botones de la tarjeta: siguen funcionando.</span></div>',
                 foot: '<button type="button" class="oa-btn blue block lg" data-a="salir">Volver a la OT</button>' };
      }
      return diag.pregunta(s, top);
    },
    pregunta: function (s, top) {
      var n = s.arbol.nodos[s.nodo], k = s.hist.length + 1, M = k - 1 + (((s.arbol.prof || {})[s.nodo]) || 1);
      var body = '<div class="oa-dqn"><span>Pregunta ' + k + ' de ' + M + '</span><span class="dots">' + Array.apply(null, Array(M)).map(function (_x, i) { return '<i class="' + (i < k ? 'on' : '') + '"></i>'; }).join('') + '</span></div>' +
        '<h2 class="oa-hq">' + esc(n.q) + '</h2><p class="oa-hsub">Toca lo que más se parece. Nada se crea solo: al final tú decides.</p>' +
        '<div class="oa-dq">' + n.ops.map(function (o, i) {
          return '<button type="button" class="oa-dop" data-a="dop" data-i="' + i + '"><i class="bi bi-' + esc(o.ic) + '"></i><span>' + esc(o.t) + '</span><i class="bi bi-chevron-right go"></i></button>';
        }).join('') + '</div>';
      return { top: top, body: body, foot: peligroFoot() };
    },
    resultado: function (s, top) {
      var r = efectivo(s, s.res);
      if (!r) {
        return { top: top, body: '<div class="oa-empty">Con tu rol no puedes hacer esto desde aquí. Avisa a gestión (por teléfono o por el ticket).</div>',
                 foot: '<button type="button" class="oa-btn line block lg" data-a="dotra">Elegir otra cosa</button>' };
      }
      var ic = ACC_IC[r.accion] || ACC_IC.incidencia;
      var body = '<div class="oa-sug"><span class="oa-sug-ic ' + ic[1] + '"><i class="bi bi-' + ic[0] + '"></i></span><div class="oa-sug-k">Te sugiero…</div><h2 class="oa-sug-h">' + tituloRes(r) + '</h2>' +
        (r.por_que ? '<p class="oa-sug-p">' + esc(r.por_que) + '</p>' : '') + '</div>';
      if (r.accion === 'pedir' && r.piezas && r.piezas.length > 1) body += '<div class="oa-hint" style="margin:0 0 12px"><i class="bi bi-hand-index"></i><span>Son ' + r.piezas.length + ' piezas posibles. En el paso que sigue eliges la que es.</span></div>';
      if (r.accion === 'pedir') body += '<div class="oa-hint" style="margin:0 0 12px"><i class="bi bi-rulers"></i><span>Se abre el pedido con su ficha de medidas: te dice qué medir y cómo.' + (r.video ? ' Graba también un video de 10 segundos con sonido: ayuda a bodega.' : '') + '</span></div>';
      if (r.texto && r.accion !== 'pedir' && r.accion !== 'esperar') body += '<div class="oa-hint" style="margin:0 0 12px"><i class="bi bi-chat-left-text"></i><span>Quedará escrito como punto de partida: «' + esc(r.texto) + '». Lo puedes cambiar.</span></div>';
      if (r.aviso) body += '<div class="oa-urg"><i class="bi bi-exclamation-triangle-fill"></i><span>' + esc(r.aviso) + '</span></div>';
      if (r.tips && r.tips.length) {
        body += '<div class="oa-sc info"><div class="oa-sc-h"><span class="oa-sc-c"><i class="bi bi-list-check"></i></span><span class="oa-sc-t">Prueba esto primero</span></div><ul class="oa-tips">' +
          r.tips.map(function (t) { return '<li>' + esc(t) + '</li>'; }).join('') + '</ul></div>';
      }
      var ad = (r.ademas || []).filter(function (p) { return permitida(s, p.accion); });
      if (ad.length) {
        body += '<div class="oa-sc"><div class="oa-sc-h"><span class="oa-sc-c"><i class="bi bi-plus-lg"></i></span><span class="oa-sc-t">Además te sugiero</span></div>' +
          ad.map(function (p) { return '<div class="oa-ad"><i class="bi bi-' + (ACC_IC[p.accion] || ACC_IC.incidencia)[0] + '"></i><span>' + esc(pendTxt(p)) + '</span></div>'; }).join('') +
          '<div class="oa-note"><i class="bi bi-info-circle"></i><span>No se hace solo: cuando termines lo principal te lo ofrezco con un toque.</span></div></div>';
      }
      if (s.camino.length) {
        body += '<div class="oa-cam"><div class="oa-cam-t">Lo que me contaste</div>' + s.camino.map(function (p) {
          return '<div class="oa-cam-i"><span>' + esc(p.q) + '</span><b>' + esc(p.r) + '</b></div>';
        }).join('') + '</div>';
      }
      body += '<div class="oa-nada"><i class="bi bi-shield-check"></i><span>Nada se crea ni se marca solo: en la pantalla que sigue tú confirmas todo.</span></div>';
      var foot;
      if (r.accion === 'esperar') {
        foot = (r.botones || []).map(function (b, i) { return '<button type="button" class="oa-btn ' + (i === 0 ? 'blue' : 'line') + ' block lg" style="margin-top:' + (i ? 8 : 0) + 'px" data-a="dbtn" data-i="' + i + '">' + esc(b.t) + '</button>'; }).join('');
      } else {
        foot = '<div class="row"><button type="button" class="oa-btn line lg" data-a="dotra">Elegir otra cosa</button><button type="button" class="oa-btn blue lg" data-a="dseguir">Seguir con esto <i class="bi bi-arrow-right"></i></button></div>';
      }
      return { top: top, body: body, foot: foot };
    },
    atras: function (s) {
      if (s.res) { s.res = null; s.camino.pop(); s.atajo = false; render(true); return; }
      if (s.hist.length) { s.nodo = s.hist.pop(); s.camino.pop(); render(true); }
    }
  };

  /* ───────────────────────────── pantalla: MIS PEDIDOS (seguimiento) ───────────────────────────── */
  var seg = {
    init: function (s) { s.step = 1; },
    vista: function (s) {
      var sols = s.d.sol || [];
      var body = '<h2 class="oa-hq">Mis pedidos de repuesto</h2><p class="oa-hsub">Así van los pedidos de ' + esc(sinPar(s.d.n)) + '.</p>' +
        (sols.length ? sols.slice().reverse().map(function (x) {
          var sg = x.seg || null;
          // 2026-10-21: trayectoria con fecha y hora Chile (misma palabra que ve gestión); si el pedido aún no trae su estado, el dibujo de 4 pasos de antes
          var tray = x.estado ? trayectoriaHTML(x) : (pasosPedido(sg) + (sg && sg.rechazado ? '<div class="oa-note"><i class="bi bi-x-circle"></i><span>Este pedido no se va a pedir.</span></div>' : ''));
          return '<div class="oa-pedido"><b>' + esc(x.nombre || '') + ' × ' + (+x.cant || 1) + '</b><small>' + esc(String(x.label || '').split(' · ')[0]) + '</small>' +
            (x.medida ? '<small class="oa-medtag"><i class="bi bi-rulers"></i> ' + esc(x.medida) + '</small>' : '') + atiendeHTML(x) + tray + '</div>';
        }).join('') : '<div class="oa-empty">Todavía no hay pedidos para este equipo.</div>');
      return { top: topHTML({ eyebrow: 'Seguimiento' }), body: body,
               foot: '<div class="row"><button type="button" class="oa-btn ghost lg" data-a="abrir-pedir"><i class="bi bi-plus-lg"></i> Pedir otra pieza</button><button type="button" class="oa-btn blue lg" data-a="volver">Volver a la OT</button></div>' };
    }
  };

  /* ───────────────────────────── flujo: «SÍ, LLEGÓ» (accesorio de una OT de cliente) ─────────────────────────────
     2026-10-23: el formulario chico del accesorio. Nada es obligatorio: foto y descripción son opcionales (Daniel: «foto y descripción opcional,
     nada más»). Guardar deja el accesorio como «Llegó» (cuenta como su única obligatoria para firmar). */
  var llego = {
    init: function (s) { s.detalle = (s.d.llego && s.d.llego.detalle) || ''; s.dirty = false; },
    estado: function () { return { falta: null, ok: 1 }; },
    vista: function (s) {
      if (s.step === 'fin') return llego.fin(s);
      var body = '<h2 class="oa-hq">¿Llegó este accesorio?</h2><p class="oa-hsub">Los accesorios no se instalan: solo cuéntanos que llegó. La foto y la descripción son opcionales.</p>' + errHTML();
      body += sc('oaSc-foto', s.fotos.length > 0, 1, 'Foto', 'Opcional', mediaCuerpo('Si quieres, deja una foto del accesorio.'), 'opt');
      body += sc('oaSc-desc', s.detalle.replace(/\s/g, '').length > 0, 2, 'Descripción', 'Opcional',
        textoHTML('detalle', s.detalle, 'Si quieres, cuéntanos algo: cómo llegó, cuántos, qué trae…', 0), 'opt');
      var foot = '<button type="button" class="oa-btn blue block lg" data-a="enviar"' + (s.enviando ? ' aria-busy="true"' : '') + '>' +
        (s.enviando ? '<span class="oa-spin"></span> Guardando…' : '<i class="bi bi-check-circle-fill"></i> Confirmar que llegó') + '</button>';
      return { top: topHTML({ eyebrow: 'Accesorio · ¿Llegó?' }), body: body, foot: foot };
    },
    enviar: async function (s) {
      if (s.enviando) return;
      s.enviando = true; s.err = ''; marco();
      var fd = new FormData();
      fd.append('descripcion', s.detalle.trim());
      s.fotos.forEach(function (f) { fd.append('fotos', f.file, f.file.name || 'foto.jpg'); });
      var j = await api('POST', base(s.d) + '/llego', fd);
      if (S !== s) return;
      s.enviando = false;
      if (!j.ok) { s.err = j.error || 'No se pudo guardar.'; render(false); return; }
      s.res = j; s.step = 'fin'; s.d.llego = j.estado; render(true);
    },
    fin: function (s) {
      return { top: '<div class="oa-top"><div class="oa-r1"><span class="oa-eb">Accesorio · ¿Llegó?</span></div></div>',
               body: finGenerico({ tono: '', icono: 'check-lg', h: 'Listo.', lead: 'Quedó anotado que llegó.', det: 'Llegó', dib: 'otra',
                 lineas: [['clipboard-check', 'Quedó en la OT con tu nombre y la hora.'], ['pen', 'Este accesorio ya no bloquea la firma de la OT.']] }),
               foot: '<button type="button" class="oa-btn blue block lg" data-a="volverrec">Volver a la OT</button>' };
    }
  };

  var FLUJO = { pedir: pedir, diag: diag, fs: fuera, baja: baja, nollego: nollego, noesta: noesta, incidencia: incidencia, seg: seg, llego: llego };

  /* ───────────────────────────── eventos ───────────────────────────── */
  function onInput(ev) {
    var t = ev.target;
    if (!S || !t || !t.getAttribute) return;
    var nombre = t.getAttribute('data-in');
    if (!nombre) return;
    if (nombre === 'q') {
      S.q = t.value;
      var rr = document.getElementById('oaP1R'); if (rr) rr.innerHTML = pedir.p1res(S);
      var cl = document.getElementById('oaClrQ'); if (cl) cl.hidden = !S.q;
      return;
    }
    if (nombre === 'med' || nombre === 'medtxt' || nombre === 'medotro') { medInput(t, nombre); return; }
    if (nombre === 'nombre') { var i = +t.getAttribute('data-i'); if (S.sel[i]) S.sel[i].nombre = t.value; marco(); refrescarTicks(); S.dirty = true; return; }
    S[nombre] = t.value; S.dirty = true;
    marco(); refrescarTicks();
    // el contador de letras vive junto al campo: se actualiza sin repintar el cuerpo
    var cnt = t.parentNode && t.parentNode.querySelector('.oa-cnt');
    if (cnt) {
      var min = nombre === 'motivo' ? 10 : (S.flow === 'baja' ? ((LISTAS().min || {}).baja || 12) : (S.flow === 'fs' ? ((LISTAS().min || {}).fuera_servicio || 10) : ((LISTAS().min || {}).equipo || 10)));
      var n = t.value.replace(/\s/g, '').length;
      cnt.className = 'oa-cnt' + (n >= min ? ' ok' : '');
      cnt.innerHTML = n >= min ? '<i class="bi bi-check-lg"></i> Bien' : n + ' de ' + min + ' letras como mínimo';
    }
  }
  /* Una medida mientras se escribe: se actualizan solo su eco («235 cm = 2,35 m»), su aviso y lo que falta, sin repintar el cuerpo
     (no se pierde el foco ni se cierra el teclado). */
  var _borTimer = null;
  function guardarBorradorDeb() {
    if (!S || S.flow !== 'pedir') return;
    clearTimeout(_borTimer);
    var s = S;
    _borTimer = setTimeout(function () { if (S === s && s.step !== 'fin') guardarBorrador(s); }, 700);
  }
  function medInput(t, nombre) {
    var i = +t.getAttribute('data-i'), k = t.getAttribute('data-k'), l = S.sel[i], f = l && fichaDeLinea(S, l);
    if (!l || !f) return;
    var d = datosDe(l);
    if (nombre === 'medotro') d.campos[k + '_otro'] = t.value; else d.campos[k] = t.value;
    if (nombre !== 'medotro' && t.value.trim() && (!d.medido_con || d.medido_con === 'catalogo')) d.medido_con = 'huincha';
    S.dirty = true;
    var c = f.campos.filter(function (x) { return x.k === k; })[0], ck = medCheck(S, l, f);
    var eco = document.getElementById('oaEco' + i + '_' + k);
    if (eco && c) { var n = parseNum(t.value); eco.textContent = (n != null && n > 0) ? ecoDe(n, c.u) : ''; }
    var av = document.getElementById('oaAv' + i + '_' + k); if (av) av.innerHTML = avisoHTML(i, k, ck.av[k], ck.imp[k]);
    var fl = document.getElementById('oaFl' + i + '_' + k); if (fl) fl.classList.toggle('bad', !!ck.imp[k]);
    var fa = document.getElementById('oaFa' + i);
    if (fa) { fa.hidden = !ck.faltan.length; var sp = fa.querySelector('span'); if (sp) sp.textContent = ck.faltan.length ? 'Falta: ' + ck.faltan.join(' · ') : ''; }
    var blk = document.getElementById('oaMed' + i); if (blk) blk.classList.toggle('ok', ck.completo && !ck.vacio);
    marco(); refrescarTicks(); guardarBorradorDeb();
  }
  /* El tick verde de cada tarjeta se actualiza mientras se escribe, sin repintar el cuerpo (no se pierde el foco ni el clic). */
  var TICKS = {
    pedir: { okM: 'oaSc-med', okF: 'oaSc-foto', okQ: 'oaSc-falla', okL: 'oaSc-lista', okA: 'oaSc-alias' },
    fs: { okF: 'oaSc-foto', okQ: 'oaSc-que', okR: 'oaSc-riesgo' },
    baja: { okF: 'oaSc-foto', okM: 'oaSc-mot', okT: 'oaSc-diag' },
    nollego: { okC: 'oaSc-cuantos', okQ: 'oaSc-quepaso', okW: 'oaSc-quien' },
    noesta: { okO: 'oaSc-op', okM: 'oaSc-motivo', okF: 'oaSc-foto' },
    incidencia: { okF: 'oaSc-foto', okQ: 'oaSc-que' }
  };
  function refrescarTicks() {
    if (!S || !TICKS[S.flow]) return;
    var f = FLUJO[S.flow], e = (f.estado3 ? f.estado3(S) : (f.estado ? f.estado(S) : null));
    if (!e) return;
    var mapa = TICKS[S.flow];
    Object.keys(mapa).forEach(function (k) {
      var el = document.getElementById(mapa[k]);
      if (!el || e[k] == null) return;
      el.classList.toggle('ok', !!e[k]);
      var c = el.querySelector('.oa-sc-c');
      if (c) { if (!c.hasAttribute('data-n')) c.setAttribute('data-n', c.textContent || ''); c.innerHTML = e[k] ? '<i class="bi bi-check-lg"></i>' : c.getAttribute('data-n'); }
    });
  }
  async function onChange(ev) {
    var t = ev.target;
    if (!S || !t || (t.id !== 'oaCam' && t.id !== 'oaGal' && t.id !== 'oaVid')) return;
    var files = Array.prototype.slice.call(t.files || []);
    t.value = '';
    if (t.id === 'oaVid') {                              // 2026-10-21 (A5): video opcional de 10 a 20 s, hasta 28 MB; suma a la foto, no la reemplaza
      var v = files[0];
      if (!v) return;
      if (!esVideoArchivo(v)) { toast('Eso no es un video.', 'warning'); return; }
      if (v.size > MAX_VIDEO_BYTES) { toast('El video pesa más de 28 MB. Graba uno de 10 a 20 segundos.', 'warning'); return; }
      if (S.video) { try { URL.revokeObjectURL(S.video.url); } catch (e) { /* nada */ } }
      S.video = { file: v, url: URL.createObjectURL(v) };
      S.dirty = true; S.err = '';
      if (S.flow === 'pedir') guardarBorrador(S);
      render(false);
      return;
    }
    for (var i = 0; i < files.length; i++) {
      var f = files[i];
      if (S.fotos.length >= MAX_FOTOS) { toast('Como máximo ' + MAX_FOTOS + ' fotos.', 'warning'); break; }
      if (!/^image\//.test(f.type || '') && !/\.(jpe?g|png|webp|heic|heif)$/i.test(f.name || '')) { toast('Aquí van solo fotos.', 'warning'); continue; }
      try { if (window.ilusComprimirImagen) f = await window.ilusComprimirImagen(f); } catch (e) { /* nada */ }
      if (f.size > MAX_FOTO_BYTES) { toast('Una foto pesa más de 8 MB. Toma otra.', 'warning'); continue; }
      S.fotos.push({ file: f, url: URL.createObjectURL(f), ref: S.camRef || '' });
    }
    S.camRef = '';
    S.dirty = true; S.err = '';
    if (S.flow === 'pedir') guardarBorrador(S);
    render(false);
  }
  var ACC = {
    atras: function () {
      if (S.flow === 'diag') return diag.atras(S);
      if (S.flow === 'pedir' && S.step > 1) { S.step = (S.step === 3 && S.tile && S.sel.some(function (l) { return l.id; })) ? 2 : 1; render(true); }
    },
    salir: function () { salir(false); },
    volver: function () { var k = S.key; cerrar(false); pintar(k); },
    volverrec: function () { recargar(S.key); },
    /* 2026-10-20: desde «No llegó · falta stock» a «¿Qué pasó?»: la tarjeta ya muestra «falta stock» y el modal nace con ese equipo elegido. */
    quepaso: function () { var k = S.key; cerrar(false); pintar(k); if (window.OTQuePaso) window.OTQuePaso.abrir('falta_stock'); },
    ir: function (el) { irA(el.getAttribute('data-go')); },
    cam: function () { S.camRef = ''; document.getElementById('oaCam').click(); },
    gal: function () { S.camRef = ''; document.getElementById('oaGal').click(); },
    camlinea: function (el) { var l = S.sel[+el.getAttribute('data-i')]; if (!l) return; S.camRef = l.uid; document.getElementById('oaCam').click(); },
    gallinea: function (el) { var l = S.sel[+el.getAttribute('data-i')]; if (!l) return; S.camRef = l.uid; document.getElementById('oaGal').click(); },
    camvid: function () { document.getElementById('oaVid').click(); },
    quitarvideo: function () {
      if (S.video) { try { URL.revokeObjectURL(S.video.url); } catch (e) { /* nada */ } }
      S.video = null; S.dirty = true; if (S.flow === 'pedir') guardarBorrador(S); render(false);
    },
    /* Reintentar los archivos que no subieron: el pedido ya existe, solo se sube lo que falta. */
    reintfotos: async function () {
      var s = S;
      if (!s || s.subiendo) return;
      s.subiendo = true; s.progreso = 'Subiendo…'; render(false);
      await pedir.subirPendientes(s);
      s.subiendo = false;
      if (S !== s) return;
      if (!s.fotoExtraFallo) toast('Listo: se subieron todas las fotos.', 'success');
      else toast('Todavía faltan ' + s.fotoExtraFallo + '. Revisa la señal e intenta de nuevo.', 'warning');
      render(false);
    },
    /* Aviso de duplicado (M3): «Ver» guarda el pedido sin terminar y abre los pedidos del equipo; «Es otra distinta» lo da por distinto. */
    dupver: function () { var k = S.key; guardarBorrador(S).then(function () { abrir('seg', k); }); },
    dupotra: function (el) { var l = S.sel[+el.getAttribute('data-i')]; if (!l) return; l.otraDistinta = true; S.dirty = true; guardarBorradorDeb(); render(false); },
    adiag: function () { var k = S.key; abrir('diag', k); },
    /* La «vista clásica» y el manual siguen siendo el modal de siempre (REGLA #4.2): se conserva el pedido a medias antes de salir. */
    clasica: function () { irAlClasico(S.pre === 'cinta' ? 'cinta' : (S.pre === 'piola' ? 'piola' : 'repuesto')); },
    manualviejo: function () { irAlClasico('repuesto', 'docs'); },
    quitarfoto: function (el) {
      var i = +el.getAttribute('data-i'), f = S.fotos.splice(i, 1)[0];
      if (f) try { URL.revokeObjectURL(f.url); } catch (e) { /* nada */ }
      S.dirty = true; if (S.flow === 'pedir') guardarBorrador(S); render(false);
    },
    limpiarq: function () { S.q = ''; render(false); },
    dictar: function () {
      var SR = window.SpeechRecognition || window.webkitSpeechRecognition;
      var campo = document.querySelector('#oaBody textarea[data-in="detalle"], #oaBody textarea[data-in="motivo"]');
      if (!SR || !campo) { toast('Tu navegador no deja dictar. Usa el micrófono del teclado.', 'info'); return; }
      try {
        var r = new SR(); r.lang = 'es-CL'; r.interimResults = false;
        r.onresult = function (e) {
          var txt = e.results[0][0].transcript;
          campo.value = (campo.value ? campo.value + ' ' : '') + txt;
          campo.dispatchEvent(new Event('input', { bubbles: true }));
        };
        r.onerror = function () { toast('No pude escucharte. Intenta de nuevo.', 'warning'); };
        r.start();
      } catch (e) { toast('No se pudo activar el micrófono.', 'warning'); }
    },
    /* pedir */
    tile: function (el) {
      var t = tilesDe(S)[+el.getAttribute('data-i')];
      if (!t) return;
      var ms = compat(S).filter(function (r) { return matchTile(t, r.descripcion); });
      S.tile = t;
      if (ms.length) { S.step = 2; render(true); return; }
      var ya = S.sel.filter(function (l) { return !l.id && l.tile === t.n; })[0];
      if (ya) S.sel = S.sel.filter(function (l) { return l !== ya; });
      else S.sel.push(nuevaLinea({ id: null, nombre: t.n, tile: t.n, origen: 'manual', cant: 1 }, fichaDeNombre(S, t.full)));
      S.dirty = true; render(false);
    },
    otra: function () { S.sel.push(nuevaLinea({ id: null, nombre: '', origen: 'manual', cant: 1 }, fichaDeNombre(S, 'Otra pieza'))); S.dirty = true; S.step = 3; render(true); },
    comofoto: function () {
      if (S.tile && !S.sel.some(function (l) { return !l.id && l.tile === S.tile.n; })) S.sel.push(nuevaLinea({ id: null, nombre: S.tile.n, tile: S.tile.n, origen: 'manual', cant: 1 }, fichaDeNombre(S, S.tile.full)));
      S.dirty = true; S.step = 3; render(true);
    },
    rep: function (el, ev) {
      if (ev.target.closest && ev.target.closest('[data-stop]')) return;
      pedir.elegirRep(S, el.getAttribute('data-id')); render(false);
    },
    menos: function (el) { var l = enSel(S, +el.getAttribute('data-id')); if (l && l.cant > 1) { l.cant--; render(false); } },
    mas: function (el) { var l = enSel(S, +el.getAttribute('data-id')); if (l && l.cant < 99) { l.cant++; render(false); } },
    menosi: function (el) { var l = S.sel[+el.getAttribute('data-i')]; if (l && l.cant > 1) { l.cant--; render(false); } },
    masi: function (el) { var l = S.sel[+el.getAttribute('data-i')]; if (l && l.cant < 99) { l.cant++; render(false); } },
    quitar: function (el) { S.sel.splice(+el.getAttribute('data-i'), 1); if (!S.sel.length) S.step = 1; render(true); },
    'mas-piezas': function () { S.step = 1; S.tile = null; render(true); },
    a3: function () { S.step = 3; render(true); },
    falla: function (el) { S.falla = el.getAttribute('data-v'); S.dirty = true; render(false); },
    dejarfs: function () { S.dejarFS = !S.dejarFS; render(false); },
    otrapieza: function () {
      if (S.fotoExtraFallo > 0) {          // fotos del pedido anterior sin subir: no se pierden sin avisar
        var s0 = S;
        window.ilusConfirm({ title: 'Faltan fotos por subir', message: 'Si pides otra pieza ahora, las fotos que faltan de este pedido se descartan.', okLabel: 'Descartar y seguir', cancelLabel: 'Reintentar primero', danger: true })
          .then(function (ok) { if (ok && S === s0) { borrarBorrador(s0.key).then(function () { s0.fotoExtraFallo = 0; s0.enviadoOk = false; ACC.otrapieza(); }); } });
        return;
      }
      var s = S; s.step = 1; s.alias = ''; s.video = null; s.enviadoOk = false; s.subidas = []; s.creadaMs = 0; s.fotoExtraFallo = 0; s.subiendo = false; s.progreso = ''; s.sel = []; s.tile = null; s.fotos = []; s.falla = ''; s.detalle = ''; s.dejarFS = false; s.res = null; s.fsAviso = false;
      s.sug = null; s.pend = []; s.diag = null; s.camRef = '';
      s.loteId = uuid(); s.dirty = false; render(true);
    },
    enviar: function () { FLUJO[S.flow].enviar(S); },
    /* formularios */
    motivo: function (el) { S.motivo = el.getAttribute('data-v'); S.dirty = true; render(false); },
    riesgo: function (el) { S.riesgo = el.getAttribute('data-v'); S.dirty = true; render(false); },
    nlmodo: function (el) {
      var v = el.getAttribute('data-v'), cant = S.d.cant || 1;
      S.modo = v; S.cuantos = v === 'uno' ? 1 : (v === 'todos' ? cant : (S.cuantos || 1)); S.dirty = true; render(false);
    },
    nlmenos: function () { if ((S.cuantos || 1) > 1) { S.cuantos = (S.cuantos || 1) - 1; render(false); } },
    nlmas: function () { var c = S.d.cant || 1; if ((S.cuantos || 1) < c) { S.cuantos = (S.cuantos || 1) + 1; render(false); } },
    nlque: function (el) { S.que = el.getAttribute('data-v'); S.dirty = true; render(false); },
    nlquien: function (el) { S.quien = el.getAttribute('data-v'); S.dirty = true; render(false); },
    nop: function (el) { var v = el.getAttribute('data-v'); if (S.op !== v) { S.op = v; S.motivo = ''; S.fotos.forEach(function () {}); } S.dirty = true; render(false); },
    nmot: function (el) { S.motivo = el.getAttribute('data-v'); S.dirty = true; render(false); },
    incmenos: function () { if (S.cant > 1) { S.cant--; render(false); } },
    incmas: function () { if (S.cant < (S.d.cant || 1)) { S.cant++; render(false); } },
    'abrir-pedir': function () { var k = S.key; abrir('pedir', k, leerBorrador(k) ? { borrador: true } : undefined); },
    /* medidas */
    mopc: function (el) {
      var i = +el.getAttribute('data-i'), k = el.getAttribute('data-k'), v = el.getAttribute('data-v'), l = S.sel[i], f = l && fichaDeLinea(S, l);
      if (!l || !f) return;
      var d = datosDe(l), c = f.campos.filter(function (x) { return x.k === k; })[0];
      if (c && c.y === 'sino') d.campos[k] = (v === 'si');
      else if (String(d.campos[k]) === String(v)) { delete d.campos[k]; }                 // tocarla de nuevo la desmarca
      else d.campos[k] = v;
      if (c && c.y === 'opcnum') delete d.otro[k];
      if (tiene(d.campos, k) && (!d.medido_con || d.medido_con === 'catalogo')) d.medido_con = 'huincha';
      S.dirty = true; guardarBorradorDeb(); render(false);
    },
    motro: function (el) {
      var i = +el.getAttribute('data-i'), k = el.getAttribute('data-k'), l = S.sel[i];
      if (!l) return;
      var d = datosDe(l);
      d.otro[k] = !d.otro[k];
      if (d.otro[k]) delete d.campos[k];
      S.dirty = true; render(false);
    },
    medno: function (el) { var l = S.sel[+el.getAttribute('data-i')]; if (!l) return; var d = datosDe(l); d.medido_con = 'no_pude'; S.dirty = true; guardarBorradorDeb(); render(false); },
    medsi: function (el) { var l = S.sel[+el.getAttribute('data-i')]; if (!l) return; var d = datosDe(l); d.medido_con = Object.keys(d.campos).length ? 'huincha' : ''; d.motivo_no = ''; S.dirty = true; render(false); },
    medmot: function (el) { var l = S.sel[+el.getAttribute('data-i')]; if (!l) return; datosDe(l).motivo_no = el.getAttribute('data-v'); S.dirty = true; guardarBorradorDeb(); render(false); },
    medok: function (el) { var l = S.sel[+el.getAttribute('data-i')]; if (!l) return; datosDe(l).confirmados[el.getAttribute('data-k')] = true; S.dirty = true; render(false); },
    medmas: function (el) { var l = S.sel[+el.getAttribute('data-i')]; if (!l) return; l.mas = !l.mas; render(false); },
    medmod: function (el) {
      var l = S.sel[+el.getAttribute('data-i')], f = l && fichaDeLinea(S, l), x = f && modeloMedidas(S, f)[+el.getAttribute('data-j')];
      if (!x) return;
      var d = datosDe(l); d.campos = Object.assign({}, x.campos); d.medido_con = 'catalogo'; d.motivo_no = ''; d.otro = {};
      S.dirty = true; guardarBorradorDeb(); render(false);
    },
    medpiola: function (el) {
      var l = S.sel[+el.getAttribute('data-i')], f = l && fichaDeLinea(S, l), p = piolasModelo(S)[+el.getAttribute('data-j')];
      if (!p || !f) return;
      var d = datosDe(l), dm = f.campos.filter(function (x) { return x.k === 'diametro_mm'; })[0];
      d.campos = { largo_cm: String(Math.round(p.largo_m * 1000) / 10), diametro_mm: String(p.diametro_mm) };
      d.otro = {};
      if (dm && !(dm.op || []).some(function (o) { return Number(o[0]) === Number(p.diametro_mm); })) d.otro.diametro_mm = true;
      d.medido_con = 'catalogo'; d.motivo_no = '';
      S.dirty = true; guardarBorradorDeb(); render(false);
    },
    separar: function (el) {
      var i = +el.getAttribute('data-i'), l = S.sel[i];
      if (!l || l.cant < 2) return;
      var copias = [];
      for (var q = 0; q < l.cant; q++) { var c = JSON.parse(JSON.stringify(l)); c.cant = 1; c.uid = uuid().slice(0, 12); copias.push(c); }
      S.sel.splice.apply(S.sel, [i, 1].concat(copias));
      S.dirty = true; render(false);
    },
    /* «¿Qué le pasa?» */
    dop: function (el) {
      var n = S.arbol.nodos[S.nodo], op = n.ops[+el.getAttribute('data-i')];
      if (!op) return;
      S.camino.push({ q: n.q, r: op.t });
      if (op.ir) { S.hist.push(S.nodo); S.nodo = op.ir; } else { S.res = op.res; }
      render(true);
    },
    dbtn: function (el) {
      var b = ((S.res && S.res.botones) || [])[+el.getAttribute('data-i')];
      if (!b) return;
      if (b.cerrar) return salir(false);
      if (b.ir) {
        var q = S.arbol.nodos[S.nodo].q;
        S.camino.pop(); S.camino.push({ q: q, r: b.t }); S.res = null;
        if (b.ir !== S.nodo) { S.hist.push(S.nodo); S.nodo = b.ir; }
        render(true);
      }
    },
    dotra: function () { if (!S.res) return; S.res = null; S.camino.pop(); S.atajo = false; render(true); },
    peligro: function () {
      var a = (MED(S) && MED(S).atajo) || ATAJO_LOCAL;
      S.atajo = true; S.camino.push({ q: 'Atajo de seguridad', r: 'Puede lastimar a alguien' }); S.res = Object.assign({}, a); render(true);
    },
    dseguir: function () {
      var r = efectivo(S, S.res);
      if (!r || r.accion === 'esperar') return;
      var flujo = { pedir: 'pedir', fs: 'fs', baja: 'baja', incidencia: 'incidencia', noesta: 'noesta', nollego: 'nollego' }[r.accion];
      if (flujo) abrir(flujo, S.key, { semilla: semillaDe(S, r) });
    },
    pend: function (el) {
      var l = pendLista(S), p = l[+el.getAttribute('data-i')];
      if (!p) return;
      var resto = l.filter(function (x) { return x !== p; }), ctx = S.pendCtx || {};
      var flujo = { pedir: 'pedir', fs: 'fs', baja: 'baja', incidencia: 'incidencia', noesta: 'noesta', nollego: 'nollego' }[p.accion];
      if (flujo) abrir(flujo, S.key, { semilla: Object.assign({}, p, { ademas: resto, ops: S.ops || ctx.ops, diag: S.diag || ctx.diag }) });
    }
  };
  function onClick(ev) {
    var el = ev.target.closest && ev.target.closest('[data-a]');
    if (!el || !S) return;
    var a = el.getAttribute('data-a');
    if (ACC[a]) { ev.preventDefault(); ACC[a](el, ev); }
  }

  /* acciones de la BARRA (fuera de la pantalla completa) */
  function onBarra(ev) {
    var el = ev.target.closest && ev.target.closest('.otd-acc [data-a]');
    if (!el) return;
    ev.preventDefault();
    var a = el.getAttribute('data-a'), key = el.getAttribute('data-key'), d = DATA()[key];
    if (!d) return;
    if (a === 'abrir') return abrir(el.getAttribute('data-f'), key);
    // 2026-10-23: «No llegó» de un accesorio = la salida de siempre «No llegó · falta stock», ya elegida (la persona elige el motivo).
    if (a === 'accnollego') {
      return abrir('noesta', key, { semilla: { opcion: 'falta_stock', titulo: 'Accesorio: no llegó',
        por_que: 'Un accesorio que no llegó queda como «No llegó · falta stock». Elige el motivo.', piezas: [] } });
    }
    if (a === 'seg') return abrir('seg', key);
    if (a === 'seguir') return abrir('pedir', key, { borrador: true });
    if (a === 'masacc') { MAS_ABIERTO[key] = !MAS_ABIERTO[key]; pintar(key); return; }          // «Más acciones» del celular
    var mid = d.k === 'i' ? 'i' + d.id : d.id;
    if (a === 'manual') { if (window.otdRepAbrir) window.otdRepAbrir(mid, 'repuesto', 'docs'); return; }
    // 2026-10-21 (A2): «Piolas» y «Cinta» abren el flujo NUEVO con la baldosa ya elegida; el modal viejo queda como «vista clásica» dentro del flujo
    if (a === 'piola' || a === 'cinta') {
      return abrir('pedir', key, { pre: a, semilla: { titulo: 'Empezaste desde «' + (a === 'piola' ? 'Piolas' : 'Cinta') + '»', por_que: 'Dejé elegida la pieza que tocaste.', piezas: [] } });
    }
    // 2026-10-21 (A9): «No pude revisarlo» no deja al técnico sin salida: avisa a gestión con el flujo de Informar incidencia
    if (a === 'avisarpieza') {
      return abrir('incidencia', key, { semilla: { titulo: 'Avisas a gestión', por_que: 'Marcaste que no pudiste revisar este equipo. Cuéntale a gestión qué pieza hace falta y súbele una foto.', piezas: [] } });
    }
    if (a === 'saltar') { if (typeof window.otdSaltarEquipo === 'function') window.otdSaltarEquipo(d.id); return; }
    if (a === 'borrar') {
      var yaEnviado = !!(leerBorrador(key) || {}).env;
      window.ilusConfirm({ title: yaEnviado ? '¿Descartar las fotos que faltan?' : '¿Borrar el pedido sin terminar?',
        message: yaEnviado ? 'El pedido ya quedó enviado. Se borran solo las fotos que no alcanzaron a subir.' : 'Se borran la foto y las piezas que ya habías elegido.',
        okLabel: yaEnviado ? 'Sí, descartar' : 'Sí, borrar', cancelLabel: 'No, dejarlo', danger: true })
        .then(function (ok) { if (ok) borrarBorrador(key).then(function () { pintar(key); }); });
      return;
    }
    if (a === 'reactivar') { if (window.otdFueraServicio) window.otdFueraServicio(d.id, true); return; }
    if (a === 'revertir' || a === 'quitarnp') {
      window.ilusPrompt({
        title: 'Volver a operativo', message: 'Cuenta por qué vuelve a su estado normal (queda escrito con tu nombre).', placeholder: 'Mínimo 10 caracteres', required: true,
        validate: function (v) { return (v || '').trim().length >= 10 ? { ok: true } : { ok: false, error: 'Escribe al menos 10 caracteres.' }; }
      }).then(function (motivo) {
        if (!motivo) return;
        var url = base(d) + (a === 'quitarnp' ? '/no-esta/revertir' : '/estado/revertir');
        api('POST', url, { motivo: motivo }).then(function (j) {
          if (j.ok) { toast('Listo: vuelve a operativo.', 'success'); recargar(key); } else toast(j.error || 'No se pudo.', 'error');
        });
      });
    }
  }

  function volverDeRecarga() {
    var key = null;
    try { key = sessionStorage.getItem('otdAccVolver'); sessionStorage.removeItem('otdAccVolver'); } catch (e) { /* nada */ }
    if (!key) return;
    var el = document.getElementById('otdAcc-' + key);
    if (!el) return;
    // la barra vive en la pestaña «Trabajo»: si está oculta, se abre antes de llevar a la persona a su tarjeta
    var intentos = 0;
    (function ir() {
      var tab = document.querySelector('#otdTabs [data-tab="trabajo"]');
      if (!el.offsetParent && tab) tab.click();            // los botones de pestañas de la ficha se activan un poco después de cargar
      if (el.offsetParent) { el.scrollIntoView({ block: 'center' }); return; }
      if (++intentos < 10) setTimeout(ir, 250);
    })();
  }

  /* API pública */
  window.OtAcc = { abrir: abrir, pintar: pintar, pintarTodo: pintarTodo, recargar: recargar, icSvg: icSvg };
  // Funciones puras del pedido de repuesto (sin DOM): las usan las pruebas con Node (tests/test_ot_acciones_repuestos_front.py).
  window.OtAccPuro = { coincidePieza: coincidePieza, fichaParaLinea: fichaParaLinea, armarNombresAlt: armarNombresAlt, fechaChile: fechaChile, haceTxt: haceTxt,
                       buscarDuplicado: buscarDuplicado, dupTexto: dupTexto, trayectoria: trayectoria, raizPalabra: raizPalabra, esVideoArchivo: esVideoArchivo, mismaPieza: mismaPieza };
  window.otdAccPintar = pintar;
  window.otdAccAbrir = abrir;

  function iniciar() {
    pintarTodo();
    document.addEventListener('click', onBarra);
    volverDeRecarga();
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', iniciar); else iniciar();
})();

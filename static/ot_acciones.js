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
   REGLA #1: nada de alert/confirm/prompt nativos (ilusConfirm / ilusToast / ilusPrompt).
   ══════════════════════════════════════════════════════════════════════════════════════════════════════════════ */
(function () {
  'use strict';

  var HDR = { 'X-Requested-With': 'XMLHttpRequest' };
  var MAX_FOTOS = 3;
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
    otra: '<path d="M7 17h7.5l3-4.5h13l3 4.5H41v21H7z"/><circle cx="24" cy="26.5" r="6.5"/><path d="M36 21.5h.01"/>'
  };
  var RX_IC = [
    ['piola', /piola|guaya|chicote|cable de acero/i], ['polea', /polea|roldana/i], ['pin', /\bpin\b|perilla|pasador|clavija|seguro/i],
    ['buje', /buje|rodamiento|bocina/i], ['pad', /\bpad\b|tapiz|asiento|respaldo|cojin/i], ['tope', /tope|goma|taco|nivelador/i],
    ['cinta', /cinta|banda/i], ['tabla', /tabla|deck/i], ['motor', /motor/i], ['consola', /consola|pantalla/i],
    ['llave', /llave/i], ['rodillo', /rodillo/i], ['correa', /correa/i], ['pedal', /pedal/i], ['biela', /biela/i],
    ['sillin', /sill[ií]n/i], ['cuerno', /cuerno|porta ?disco/i], ['cable', /cable|mosquet|agarre|manilla/i]
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
  function idbOp(modo, fn) {
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
    return idbOp('readwrite', function (st) { for (n = 0; n < MAX_FOTOS; n++) st.delete(bkey(key) + '#' + n); return null; });
  }
  function guardarBorrador(S) {
    var j = { t: Date.now(), step: S.step, sel: S.sel, q: S.q, falla: S.falla, detalle: S.detalle, dejarFS: S.dejarFS,
              tile: S.tile, loteId: S.loteId, nFotos: S.fotos.length };
    try { localStorage.setItem(bkey(S.key), JSON.stringify(j)); } catch (e) { return Promise.resolve(false); }
    return idbOp('readwrite', function (st) {
      for (var i = 0; i < MAX_FOTOS; i++) {
        if (S.fotos[i]) st.put({ blob: S.fotos[i].file, name: S.fotos[i].file.name || ('foto' + i + '.jpg'), type: S.fotos[i].file.type }, bkey(S.key) + '#' + i);
        else st.delete(bkey(S.key) + '#' + i);
      }
      return null;
    });
  }
  function cargarFotosBorrador(S, n) {
    var tareas = [];
    for (var i = 0; i < n; i++) {
      (function (i) {
        tareas.push(idbOp('readonly', function (st) { return st.get(bkey(S.key) + '#' + i); }));
      })(i);
    }
    return Promise.all(tareas).then(function (rs) {
      rs.forEach(function (r) {
        if (r && r.blob) {
          try {
            var f = new File([r.blob], r.name || 'foto.jpg', { type: r.type || 'image/jpeg' });
            S.fotos.push({ file: f, url: URL.createObjectURL(f) });
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

  function barHTML(key, d) {
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
      if (np) {
        var quitar = p.rev ? ' <button type="button" class="oa-lnk muted" data-a="quitarnp" data-key="' + esc(key) + '" style="min-height:30px;font-size:13.5px"><i class="bi bi-arrow-counterclockwise"></i> Quitar la marca</button>' : '';
        out.push('<div class="oa-stbar"><span class="oa-st ' + (np.estado === 'falta_stock' ? 'amber' : 'gray') + '"><i class="bi bi-box-seam"></i> ' + esc(np.chip) + '</span>' + quitar + '</div>');
      }
    }

    var borr = leerBorrador(key);
    if (borr && p.ped) {
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
    var g = [];
    if (p.fs && !fs && !bj && !nlTotal && !np) g.push(['fs', 'sign-stop', 'No se puede usar', 'Déjalo fuera de servicio']);
    if (p.baja && !bj && !nlTotal && !np) g.push(['baja', 'slash-circle', 'No tiene arreglo', inter ? 'Dar de baja' : 'Dar de baja del cliente']);
    if (inter && p.nol && !fs && !bj && !nl) g.push(['nollego', 'box-seam', 'No llegó', 'No vino en el despacho']);
    if (!inter && p.noe && !fs && !bj && !np) g.push(['noesta', 'box-seam', 'No está / no llegó', 'En el lugar o por falta de stock']);
    if (p.inc && !bj) g.push(['incidencia', 'megaphone', 'Informar incidencia', 'Con foto, para bodega']);
    if (g.length) {
      out.push('<div class="oa-lbl"><i class="bi bi-exclamation-diamond-fill"></i> Fuera de lo normal</div><div class="oa-grid">' +
        g.map(function (x, i) {
          return '<button type="button" class="oa-btn line' + (g.length % 2 === 1 && i === g.length - 1 ? ' wide' : '') + '" data-a="abrir" data-f="' + x[0] + '" data-key="' + esc(key) + '">' +
            '<i class="bi bi-' + x[1] + '"></i><span>' + esc(x[2]) + '<small>' + esc(x[3]) + '</small></span></button>';
        }).join('') + '</div>');
    }
    return out.join('');
  }
  function pintar(key) {
    var d = DATA()[key], el = document.getElementById('otdAcc-' + key);
    if (d && el) el.innerHTML = barHTML(key, d);
  }
  function pintarTodo() { Object.keys(DATA()).forEach(pintar); }

  /* ───────────────────────────── pantalla completa ───────────────────────────── */
  var root = null, win = null, S = null, histPuesto = false, overflowPrev = '';

  function asegurarRoot() {
    if (root) return;
    root = document.createElement('div');
    root.className = 'oa-fs'; root.id = 'oaFs'; root.hidden = true;
    root.setAttribute('role', 'dialog'); root.setAttribute('aria-modal', 'true'); root.setAttribute('aria-label', 'Acciones del equipo');
    root.innerHTML = '<div class="oa-win" id="oaWin" tabindex="-1"><div id="oaTop"></div><div class="oa-body" id="oaBody"></div><div id="oaFoot"></div></div>' +
      '<input type="file" id="oaCam" accept="image/*" capture="environment" hidden>' +
      '<input type="file" id="oaGal" accept="image/*" multiple hidden>';
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
    S = Object.assign({ flow: flow, key: key, d: d, step: 1, fotos: [], dirty: false, enviando: false, err: '', f: {} }, extra || {});
    if (FLUJO[flow].init) FLUJO[flow].init(S);
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
      guardarBorrador(S).then(function () { cerrar(vinoDeAtras); pintar(key); toast('Guardado en este teléfono. Lo retomas desde la tarjeta.', 'success'); });
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
  function thumbs() {
    if (!S.fotos.length) return '';
    return '<div class="oa-thumbs">' + S.fotos.map(function (f, i) {
      return '<div class="oa-thumb"><img src="' + f.url + '" alt="Foto ' + (i + 1) + '"><span class="ok"><i class="bi bi-check-lg"></i></span>' +
        '<button type="button" class="x" data-a="quitarfoto" data-i="' + i + '" aria-label="Quitar la foto ' + (i + 1) + '"><i class="bi bi-x-lg"></i></button></div>';
    }).join('') + '</div>';
  }
  function mediaCuerpo(ayuda) {
    var lleno = S.fotos.length >= MAX_FOTOS;
    return (ayuda ? '<div class="oa-hint" style="margin:-4px 0 10px"><i class="bi bi-info-circle"></i><span>' + esc(ayuda) + '</span></div>' : '') +
      '<div class="oa-media"><button type="button" class="oa-btn blue" data-a="cam"' + (lleno ? ' disabled' : '') + '><i class="bi bi-camera-fill"></i> ' + (S.fotos.length ? 'Otra foto' : 'Tomar foto') + '</button>' +
      '<button type="button" class="oa-btn line" data-a="gal"' + (lleno ? ' disabled' : '') + '><i class="bi bi-images"></i> De la galería</button></div>' + thumbs();
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
  function stockChip(r) {
    if (!r) return '<span class="oa-st gray"><span class="oa-dot x"></span>Pídela con una foto</span>';
    var s = r.semaforo;
    if (s === 'verde') {
      var n = (typeof r.disponible === 'number' && !cfg().externo) ? Math.floor(r.disponible) : null;
      return '<span class="oa-st green"><span class="oa-dot g"></span>' + (n ? n + ' en bodega' : 'Hay en bodega') + '</span>';
    }
    if (s === 'ambar') return '<span class="oa-st amber"><span class="oa-dot a"></span>Hay que pedirla</span>';
    return '<span class="oa-st gray"><span class="oa-dot x"></span>Hay que pedirla</span>';
  }
  function finGenerico(o) {
    return '<div class="oa-fin"><div class="oa-done-ic ' + (o.tono || '') + '"><i class="bi bi-' + o.icono + '"></i></div><h2>' + esc(o.h) + '</h2><p class="lead">' + esc(o.lead) + '</p>' +
      '<div class="oa-sum"><div class="oa-sum-it"><span class="ph">' + (S.d.img ? '<img src="' + esc(S.d.img) + '" alt="">' : icSvg(o.dib || 'otra')) + '</span><span><b>' + esc(S.d.n) + '</b><small>' + esc(o.det || '') + '</small></span></div></div>' +
      '<div class="oa-info-l">' + (o.lineas || []).map(function (l) {
        return '<div class="' + (l[2] || '') + '"><i class="bi bi-' + l[0] + '"></i><span>' + l[1] + '</span></div>';
      }).join('') + '</div></div>';
  }
  function irA(id) {
    var el = document.getElementById(id);
    if (!el) return;
    el.scrollIntoView({ behavior: 'smooth', block: 'center' });
    el.classList.remove('oa-flash'); void el.offsetWidth; el.classList.add('oa-flash');
  }

  /* ───────────────────────────── flujo: PEDIR REPUESTO ───────────────────────────── */
  var FALLAS = [['se_corto', 'Se cortó o se rompió'], ['se_solto', 'Se soltó o se salió'], ['desgaste', 'Desgaste por uso'],
                ['se_perdio', 'Se perdió o falta'], ['ruido', 'Hace ruido o se traba'], ['otro', 'Otra cosa: la cuento abajo']];
  function falla_txt(v) { for (var i = 0; i < FALLAS.length; i++) if (FALLAS[i][0] === v) return FALLAS[i][1]; return ''; }

  function tilesDe(s) {
    var sug = (s.ops && s.ops.repuestos_sugeridos) || [];
    return sug.map(function (t) { return { n: sinPar(t), dice: enPar(t), full: t, ic: icDe(t) }; });
  }
  function matchTile(tile, desc) {
    var a = icDe(tile.full), b = icDe(desc);
    if (a !== 'otra' && a === b) return true;
    var d = norm(desc);
    return norm(tile.n).split(' ').some(function (w) { return w.length >= 4 && d.indexOf(w) >= 0; });
  }
  function compat(s) { return (s.ops && s.ops.compatibles) || []; }
  function enSel(s, id) { return s.sel.filter(function (l) { return l.id === id; })[0]; }

  var pedir = {
    init: function (s) {
      s.sel = []; s.q = ''; s.ops = null; s.cargando = true; s.falla = ''; s.detalle = ''; s.dejarFS = false; s.tile = null;
      s.loteId = uuid(); s.res = null;
      var b = s.borrador ? leerBorrador(s.key) : null;
      if (b) {
        s.sel = b.sel || []; s.q = b.q || ''; s.falla = b.falla || ''; s.detalle = b.detalle || ''; s.dejarFS = !!b.dejarFS;
        s.tile = b.tile || null; s.loteId = b.loteId || s.loteId; s.step = b.step === 3 ? 3 : (b.step === 2 && s.tile ? 2 : 1);
        if (b.nFotos) cargarFotosBorrador(s, b.nFotos).then(function () { if (S === s) render(false); });
      }
      var key = s.key;
      api('GET', base(s.d) + '/repuestos-opciones').then(function (j) {
        if (S !== s) return;
        s.cargando = false;
        if (j.ok) s.ops = j; else s.opsErr = j.error;
        render(false);
      });
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
      var h = '<h2 class="oa-hq">¿Qué pieza necesita ' + esc(sinPar(s.d.n)) + '?</h2>';
      if (s.cargando) h += '<div class="oa-hint"><i class="bi bi-arrow-repeat"></i> Buscando las piezas de este modelo…</div>';
      if (s.opsErr) h += '<div class="oa-urg"><i class="bi bi-wifi-off"></i><span>No pudimos traer las piezas de bodega. Igual puedes pedirla con una foto.</span></div>';
      h += '<div class="oa-search"><i class="bi bi-search"></i><input type="search" data-in="q" value="' + esc(s.q) + '" placeholder="Como tú le digas: perilla, cable…" aria-label="Buscar la pieza" autocomplete="off">' +
        '<button type="button" class="clr" id="oaClrQ" data-a="limpiarq" aria-label="Borrar la búsqueda"' + (s.q ? '' : ' hidden') + '><i class="bi bi-x-lg"></i></button></div>';
      return h + '<div id="oaP1R">' + pedir.p1res(s) + '</div>';
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
        var sub = !ms.length ? '<span class="oa-dot x"></span> Pídela con una foto' : (hay ? '<span class="oa-dot g"></span> Hay en bodega' : '<span class="oa-dot a"></span> Hay que pedirla');
        return '<button type="button" class="oa-tile' + (ya ? ' on' : '') + '" data-a="tile" data-i="' + tiles.indexOf(t) + '">' + (ya ? '<span class="oa-ck"><i class="bi bi-check-lg"></i></span>' : '') +
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
      return '<div class="oa-sc info" style="padding-left:14px"><div class="oa-sum-it"><span class="ph" style="display:grid;place-items:center">' + icSvg(t.ic) + '</span><span><b>' + esc(t.n) + '</b><small>' +
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
      var okF = s.fotos.length > 0, okQ = !!s.falla && (s.falla !== 'otro' || s.detalle.replace(/\s/g, '').length >= 10);
      var okL = s.sel.length > 0 && s.sel.every(function (l) { return l.id || (l.nombre || '').replace(/\s/g, '').length >= 2; });
      var falta = !okF ? { t: 'la foto', go: 'oaSc-foto' } : !okQ ? { t: 'qué le pasó', go: 'oaSc-falla' } : !okL ? { t: 'el nombre de la pieza', go: 'oaSc-lista' } : null;
      return { okF: okF, okQ: okQ, okL: okL, falta: falta };
    },
    p3: function (s) {
      var e = pedir.estado3(s);
      var h = '<h2 class="oa-hq">Foto y envío</h2><p class="oa-hsub">Una foto de lo que está malo y qué le pasó. Bodega ve esto antes de buscar la pieza.</p>' + errHTML();
      h += sc('oaSc-foto', e.okF, 1, 'Foto de lo que está malo', 'Obligatoria', mediaCuerpo('Toma la foto con la pieza a la vista. Sirve para que bodega la reconozca.'));
      h += sc('oaSc-falla', e.okQ, 2, '¿Qué le pasó?', 'Obligatoria', '<div class="oa-chips-h">Toca lo que pasó.</div>' + chipsHTML(FALLAS, s.falla, 'falla') +
        '<div style="margin-top:12px">' + textoHTML('detalle', s.detalle, s.falla === 'otro' ? 'Cuéntalo con tus palabras (mínimo 10 letras)' : 'Algo más que ayude (opcional)', s.falla === 'otro' ? 10 : 0) + '</div>');
      h += sc('oaSc-lista', e.okL, 3, 'Piezas del pedido', '', pedir.lista(s), e.okL ? '' : '');
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
            : '<label class="oa-fld-l" for="oaN' + i + '">¿Cómo se llama? <span class="oa-fld-x">como tú le digas</span></label><input id="oaN' + i + '" class="oa-inp" data-in="nombre" data-i="' + i + '" value="' + esc(l.nombre) + '" placeholder="Ej: perilla del peso" maxlength="120">') + '</span>' +
          '<span class="oa-qty"><button type="button" data-a="menosi" data-i="' + i + '" aria-label="Una menos"><i class="bi bi-dash-lg"></i></button><output>' + l.cant + '</output>' +
          '<button type="button" data-a="masi" data-i="' + i + '" aria-label="Una más"><i class="bi bi-plus-lg"></i></button></span>' +
          '<span style="grid-column:1/-1"><button type="button" class="oa-lnk muted" style="min-height:36px;font-size:14px" data-a="quitar" data-i="' + i + '"><i class="bi bi-trash3"></i> Quitar</button></span></div>';
      }).join('') + '<button type="button" class="oa-lnk" data-a="mas-piezas" style="margin-top:6px"><i class="bi bi-plus-lg"></i> Pedir otra pieza en este mismo pedido</button>';
    },
    foot3: function (s) {
      var e = pedir.estado3(s);
      if (e.falta) return faltaBtn(e.falta.t, e.falta.go);
      return '<button type="button" class="oa-btn blue block lg" data-a="enviar"' + (s.enviando ? ' aria-busy="true"' : '') + '>' +
        (s.enviando ? '<span class="oa-spin"></span> Enviando…' : '<i class="bi bi-send-fill"></i> Enviar pedido a bodega') + '</button>';
    },
    /* confirmación */
    fin: function (s) {
      var r = s.res || {}, d = s.d;
      var items = (r.solicitudes || []).map(function (x) {
        return '<div class="oa-sum-it"><span class="ph">' + icSvg(icDe(x.repuesto_nombre)) + '</span><span><b>' + esc(x.repuesto_nombre) + ' <span style="white-space:nowrap">× ' + (+x.cantidad || 1) + '</span></b><small>para ' + esc(sinPar(d.n)) + '</small></span></div>';
      }).join('');
      var body = '<div class="oa-fin"><div class="oa-done-ic"><i class="bi bi-check-lg"></i></div><h2>Listo.</h2>' +
        '<p class="lead">Bodega ya recibió tu pedido' + (r.numero_ticket ? ' <span style="white-space:nowrap">N° ' + esc(r.numero_ticket) + '</span>' : '') + '.</p><div class="oa-sum">' + items + '</div><div class="oa-info-l">' +
        '<div><i class="bi bi-person-check"></i><span>Bodega lo revisa y te avisa. No tienes que ir a buscarla.</span></div>' +
        (r.repetido ? '<div class="w"><i class="bi bi-files"></i><span>Este pedido ya estaba enviado: no se duplicó.</span></div>' : '') +
        (s.fsAviso ? '<div class="a"><i class="bi bi-sign-stop"></i><span>El equipo quedó fuera de servicio. Avisamos como urgente.</span></div>' : '') +
        (r.avisos && r.avisos.length ? r.avisos.map(function (a) { return '<div class="w"><i class="bi bi-exclamation-triangle-fill"></i><span>' + esc(a) + '</span></div>'; }).join('') : '') +
        (s.fotoExtraFallo ? '<div class="w"><i class="bi bi-exclamation-triangle-fill"></i><span>' + s.fotoExtraFallo + ' foto(s) extra no se pudieron subir. El pedido sí quedó.</span></div>' : '') +
        '<div><i class="bi bi-eye"></i><span>Lo verás avanzar aquí, en la OT.</span></div></div>' +
        '<div class="oa-prev"><div class="oa-prev-t">Así va a avanzar</div>' + pasosPedido(null) + '</div></div>';
      return { top: '<div class="oa-top"><div class="oa-r1"><span class="oa-eb">Pedido enviado</span></div></div>', body: body,
               foot: '<div class="row"><button type="button" class="oa-btn ghost lg" data-a="otrapieza"><i class="bi bi-plus-lg"></i> Pedir otra pieza</button>' +
                     '<button type="button" class="oa-btn blue lg" data-a="volver">Volver a la OT</button></div>' };
    },
    /* acciones */
    elegirRep: function (s, id) {
      var r = compat(s).filter(function (x) { return String(x.id) === String(id); })[0];
      if (!r) return;
      var l = enSel(s, r.id);
      if (l) { s.sel = s.sel.filter(function (x) { return x !== l; }); }
      else s.sel.push({ id: r.id, nombre: r.descripcion, sku: r.sku || '', origen: 'compatible', cant: 1 });
      s.dirty = true;
    },
    enviar: async function (s) {
      var e = pedir.estado3(s);
      if (e.falta || s.enviando) return;
      s.enviando = true; s.err = ''; marco();
      var d = s.d, fd = new FormData();
      var motivo = falla_txt(s.falla) + (s.detalle.trim() ? ' — ' + s.detalle.trim() : '');
      if (s.falla === 'otro') motivo = s.detalle.trim();
      fd.append('motivo', motivo);
      fd.append('lineas', JSON.stringify(s.sel.map(function (l) {
        var o = { origen: l.id ? (l.origen || 'compatible') : 'manual', repuesto_nombre: l.nombre, cantidad: l.cant };
        if (l.id) o.repuesto_stock_id = l.id;
        if (l.sku) o.repuesto_sku = l.sku;
        return o;
      })));
      fd.append('lote_id', s.loteId);
      if (d.k === 'e' && s.dejarFS) fd.append('dejar_fuera_servicio', '1');
      fd.append('evidencia', s.fotos[0].file, s.fotos[0].file.name || 'foto.jpg');
      var j = await api('POST', base(d) + '/solicitar-repuestos', fd);
      if (S !== s) return;
      if (!j.ok) { s.enviando = false; s.err = j.error || 'No se pudo enviar el pedido.'; render(false); irA('oaBody'); return; }
      s.fotoExtraFallo = 0;
      for (var i = 1; i < s.fotos.length; i++) {
        var f2 = new FormData();
        f2.append('evidencia', s.fotos[i].file, s.fotos[i].file.name || 'foto.jpg');
        f2.append('lote_id', j.lote_id || s.loteId);
        var r2 = await api('POST', base(d) + '/evidencia', f2);
        if (!r2.ok) s.fotoExtraFallo++;
      }
      s.res = j; s.enviando = false; s.step = 'fin'; s.fsAviso = !!(d.k === 'e' && s.dejarFS);
      d.sol = (d.sol || []).concat((j.solicitudes || []).map(function (x) {
        return { id: x.solicitud_id, nombre: x.repuesto_nombre, cant: x.cantidad, estado: 'solicitado', label: 'Solicitado', abierta: true, seg: null };
      }));
      if (s.fsAviso) { d.fsb = true; d.fs = { motivo: motivo, riesgo: false, desde: '', por: '' }; }
      borrarBorrador(s.key);
      try { if (window.otdRepRefrescar) window.otdRepRefrescar(d.k === 'i' ? 'i' + d.id : d.id); } catch (e2) { /* nada */ }
      render(true);
    }
  };

  /* ───────────────────────────── flujo: NO SE PUEDE USAR (fuera de servicio) ───────────────────────────── */
  var fuera = {
    init: function (s) { s.motivo = ''; s.detalle = ''; s.riesgo = ''; s.dirty = false; },
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
        (s.d.k === 'e' ? 'Queda marcado en la ficha del cliente.' : 'Queda marcado en el acta de recepción.') + '</p>' + errHTML();
      body += sc('oaSc-foto', e.okF, 1, 'Foto de lo que pasa', 'Obligatoria', mediaCuerpo('Sin foto no hay aviso: es la evidencia de por qué se detuvo.'));
      body += sc('oaSc-que', e.okQ, 2, '¿Qué pasa?', 'Obligatoria', '<div class="oa-chips-h">Elige lo que más se parece.</div>' + chipsHTML(L, s.motivo, 'motivo') +
        '<div style="margin-top:12px">' + textoHTML('detalle', s.detalle, 'Cuéntalo con tus palabras (mínimo ' + e.min + ' letras)', e.min) + '</div>');
      body += sc('oaSc-riesgo', e.okR, 3, '¿Puede lastimar a alguien?', 'Obligatoria',
        '<div class="oa-opt2">' + opcion(s.riesgo === 'si', 'r', 'exclamation-triangle-fill', 'Sí, puede lastimar', 'El aviso sale urgente', 'riesgo', 'si') +
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
    estado: function (s) {
      var min = (LISTAS().min || {}).baja || 12;
      var okF = s.fotos.length > 0, okM = !!s.motivo, okT = s.detalle.replace(/\s/g, '').length >= min;
      var falta = !okF ? { t: 'la foto', go: 'oaSc-foto' } : !okM ? { t: 'el motivo', go: 'oaSc-mot' } : !okT ? { t: 'el diagnóstico', go: 'oaSc-diag' } : null;
      return { okF: okF, okM: okM, okT: okT, falta: falta, ok: [okF, okM, okT].filter(Boolean).length, min: min };
    },
    vista: function (s) {
      if (s.step === 'fin') return baja.fin(s);
      var e = baja.estado(s), L = LISTAS().baja || [], d = s.d;
      var body = '<h2 class="oa-hq">No tiene arreglo</h2><p class="oa-hsub">Solo si no se puede recuperar. ' + (d.k === 'e' ? 'Sale del parque del cliente.' : 'Queda como «Dar de baja» en el acta.') + '</p>' + errHTML();
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
  var QUE_NL = [['no_venia', 'No venía en el despacho'], ['otro_producto', 'Llegó otro producto'], ['incompleto', 'Llegó incompleto']];
  var nollego = {
    init: function (s) { s.modo = (s.d.cant || 1) > 1 ? '' : 'todos'; s.cuantos = (s.d.cant || 1) > 1 ? null : 1; s.que = ''; s.quien = ''; s.quienNombre = ''; s.dirty = false; },
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
      var body = '<h2 class="oa-hq">¿Qué pasó con este producto?</h2><p class="oa-hsub">' + (e.varios ? 'Vienen ' + cant + ' unidades. ' : '') + 'Sin foto: solo cuéntanos qué pasó y a quién le preguntaste.</p>' + errHTML();
      var n = 1;
      if (e.varios) {
        body += sc('oaSc-cuantos', e.okC, n++, '¿Cuántas unidades no llegaron? <span class="oa-fld-x">de ' + cant + '</span>', '',
          '<div class="oa-opt3">' + opcion(s.modo === 'uno', 'a', '1-circle', 'Faltó 1 unidad', 'Llegaron ' + (cant - 1), 'nlmodo', 'uno') +
          opcion(s.modo === 'todos', 'a', 'box-seam', 'No llegó ninguna', 'Faltan las ' + cant, 'nlmodo', 'todos') +
          opcion(s.modo === 'otro', 'a', '123', 'Otro número', 'Elige cuántas', 'nlmodo', 'otro') + '</div>' +
          (s.modo === 'otro' ? '<div style="margin-top:12px;display:flex;align-items:center;gap:10px"><span class="oa-fld-l" style="margin:0">Faltan</span><span class="oa-qty"><button type="button" data-a="nlmenos" aria-label="Una menos"><i class="bi bi-dash-lg"></i></button><output>' + (s.cuantos || 1) + '</output><button type="button" data-a="nlmas" aria-label="Una más"><i class="bi bi-plus-lg"></i></button></span><b>de ' + cant + '</b></div>' : '') +
          (s.cuantos != null && s.cuantos < cant ? '<div class="oa-note"><i class="bi bi-info-circle"></i><span>Las otras ' + (cant - s.cuantos) + ' sí llegaron: se revisan con el checklist como siempre.</span></div>' : ''));
      }
      body += sc('oaSc-quepaso', e.okQ, n++, '¿Qué pasó?', '', chipsHTML(QUE_NL, s.que, 'nlque'));
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
    lista: function (s) { return (LISTAS()[s.op] || []); },
    estado: function (s) {
      var okO = !!s.op, okM = !!s.motivo;
      var okD = s.motivo !== 'otro' || s.detalle.replace(/\s/g, '').length >= ((LISTAS().min || {}).equipo || 10);
      var falta = !okO ? { t: 'qué pasó con el equipo', go: 'oaSc-op' } : !okM ? { t: 'el motivo', go: 'oaSc-motivo' } : !okD ? { t: 'contar qué pasó', go: 'oaSc-motivo' } : null;
      return { okO: okO, okM: okM && okD, falta: falta, ok: [okO, okM && okD].filter(Boolean).length };
    },
    vista: function (s) {
      if (s.step === 'fin') return noesta.fin(s);
      var e = noesta.estado(s), min = (LISTAS().min || {}).equipo || 10;
      var body = '<h2 class="oa-hq">¿Qué pasó con este equipo?</h2><p class="oa-hsub">Así el equipo deja de bloquear la firma y el cierre, y queda escrito para quien siga con esto.</p>' + errHTML();
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
          body += sc('oaSc-foto', s.fotos.length > 0, 3, 'Foto', 'Opcional', mediaCuerpo('Si ayuda a explicarlo (el lugar vacío, un letrero, el equipo bloqueado).'), 'opt');
        }
      }
      var foot = e.falta ? faltaBtn(e.falta.t, e.falta.go) :
        '<button type="button" class="oa-btn blue block lg" data-a="enviar"' + (s.enviando ? ' aria-busy="true"' : '') + '>' + (s.enviando ? '<span class="oa-spin"></span> Enviando…' : '<i class="bi bi-check2-circle"></i> Dejarlo anotado') + '</button>';
      return { top: topHTML({ eyebrow: 'No está / no llegó', prog: { ok: e.ok, n: 2, falta: e.falta && e.falta.t } }), body: body, foot: foot };
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
               foot: '<button type="button" class="oa-btn blue block lg" data-a="volverrec">Volver a la OT</button>' };
    }
  };

  /* ───────────────────────────── flujo: INFORMAR INCIDENCIA ───────────────────────────── */
  var incidencia = {
    init: function (s) { s.motivo = ''; s.detalle = ''; s.cant = 1; s.dirty = false; },
    estado: function (s) {
      var okF = s.fotos.length > 0, okQ = s.motivo.replace(/\s/g, '').length >= 10;
      var falta = !okF ? { t: 'la foto', go: 'oaSc-foto' } : !okQ ? { t: 'qué pasó', go: 'oaSc-que' } : null;
      return { okF: okF, okQ: okQ, falta: falta, ok: [okF, okQ].filter(Boolean).length };
    },
    vista: function (s) {
      if (s.step === 'fin') return incidencia.fin(s);
      var e = incidencia.estado(s), d = s.d, tope = d.cant || 1;
      var body = '<h2 class="oa-hq">Informar incidencia</h2><p class="oa-hsub">Un daño o problema que bodega y gestión tienen que ver. Queda con tu nombre y la hora.</p>' + errHTML();
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

  /* ───────────────────────────── pantalla: MIS PEDIDOS (seguimiento) ───────────────────────────── */
  var seg = {
    init: function (s) { s.step = 1; },
    vista: function (s) {
      var sols = s.d.sol || [];
      var body = '<h2 class="oa-hq">Mis pedidos de repuesto</h2><p class="oa-hsub">Así van los pedidos de ' + esc(sinPar(s.d.n)) + '.</p>' +
        (sols.length ? sols.slice().reverse().map(function (x) {
          var sg = x.seg || null;
          return '<div class="oa-pedido"><b>' + esc(x.nombre || '') + ' × ' + (+x.cant || 1) + '</b><small>' + esc(String(x.label || '').split(' · ')[0]) + '</small>' + pasosPedido(sg) +
            (sg && sg.rechazado ? '<div class="oa-note"><i class="bi bi-x-circle"></i><span>Este pedido no se va a pedir. Bodega dejó una nota en el ticket.</span></div>' : '') + '</div>';
        }).join('') : '<div class="oa-empty">Todavía no hay pedidos para este equipo.</div>');
      return { top: topHTML({ eyebrow: 'Seguimiento' }), body: body,
               foot: '<div class="row"><button type="button" class="oa-btn ghost lg" data-a="abrir-pedir"><i class="bi bi-plus-lg"></i> Pedir otra pieza</button><button type="button" class="oa-btn blue lg" data-a="volver">Volver a la OT</button></div>' };
    }
  };

  var FLUJO = { pedir: pedir, fs: fuera, baja: baja, nollego: nollego, noesta: noesta, incidencia: incidencia, seg: seg };

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
  /* El tick verde de cada tarjeta se actualiza mientras se escribe, sin repintar el cuerpo (no se pierde el foco ni el clic). */
  var TICKS = {
    pedir: { okF: 'oaSc-foto', okQ: 'oaSc-falla', okL: 'oaSc-lista' },
    fs: { okF: 'oaSc-foto', okQ: 'oaSc-que', okR: 'oaSc-riesgo' },
    baja: { okF: 'oaSc-foto', okM: 'oaSc-mot', okT: 'oaSc-diag' },
    nollego: { okC: 'oaSc-cuantos', okQ: 'oaSc-quepaso', okW: 'oaSc-quien' },
    noesta: { okO: 'oaSc-op', okM: 'oaSc-motivo' },
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
    if (!S || !t || (t.id !== 'oaCam' && t.id !== 'oaGal')) return;
    var files = Array.prototype.slice.call(t.files || []);
    t.value = '';
    for (var i = 0; i < files.length; i++) {
      var f = files[i];
      if (S.fotos.length >= MAX_FOTOS) { toast('Como máximo ' + MAX_FOTOS + ' fotos.', 'warning'); break; }
      if (!/^image\//.test(f.type || '') && !/\.(jpe?g|png|webp|heic|heif)$/i.test(f.name || '')) { toast('Aquí van solo fotos.', 'warning'); continue; }
      try { if (window.ilusComprimirImagen) f = await window.ilusComprimirImagen(f); } catch (e) { /* nada */ }
      if (f.size > MAX_FOTO_BYTES) { toast('Una foto pesa más de 8 MB. Toma otra.', 'warning'); continue; }
      S.fotos.push({ file: f, url: URL.createObjectURL(f) });
    }
    S.dirty = true; S.err = '';
    if (S.flow === 'pedir') guardarBorrador(S);
    render(false);
  }
  var ACC = {
    atras: function () {
      if (S.flow === 'pedir' && S.step > 1) { S.step = (S.step === 3 && S.tile && S.sel.some(function (l) { return l.id; })) ? 2 : 1; render(true); }
    },
    salir: function () { salir(false); },
    volver: function () { var k = S.key; cerrar(false); pintar(k); },
    volverrec: function () { recargar(S.key); },
    ir: function (el) { irA(el.getAttribute('data-go')); },
    cam: function () { document.getElementById('oaCam').click(); },
    gal: function () { document.getElementById('oaGal').click(); },
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
      else S.sel.push({ id: null, nombre: t.n, tile: t.n, origen: 'manual', cant: 1 });
      S.dirty = true; render(false);
    },
    otra: function () { S.sel.push({ id: null, nombre: '', origen: 'manual', cant: 1 }); S.dirty = true; S.step = 3; render(true); },
    comofoto: function () {
      if (S.tile && !S.sel.some(function (l) { return !l.id && l.tile === S.tile.n; })) S.sel.push({ id: null, nombre: S.tile.n, tile: S.tile.n, origen: 'manual', cant: 1 });
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
      var s = S; s.step = 1; s.sel = []; s.tile = null; s.fotos = []; s.falla = ''; s.detalle = ''; s.dejarFS = false; s.res = null; s.fsAviso = false;
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
    'abrir-pedir': function () { abrir('pedir', S.key); }
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
    if (a === 'seg') return abrir('seg', key);
    if (a === 'seguir') return abrir('pedir', key, { borrador: true });
    if (a === 'borrar') {
      window.ilusConfirm({ title: '¿Borrar el pedido sin terminar?', message: 'Se borran la foto y las piezas que ya habías elegido.', okLabel: 'Sí, borrar', cancelLabel: 'No, dejarlo', danger: true })
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
  window.otdAccPintar = pintar;
  window.otdAccAbrir = abrir;

  function iniciar() {
    pintarTodo();
    document.addEventListener('click', onBarra);
    volverDeRecarga();
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', iniciar); else iniciar();
})();

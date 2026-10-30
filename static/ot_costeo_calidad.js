/* 🧹 Costeo de OT · Calidad de datos (Daniel 2026-10-29, FASE 2 — REGLA #59).
   «Estamos en el paso de la depuración de datos: necesito que los costos sean estructurados… siempre darle la prioridad al
   DOCUMENTO que lleva el cobro… y construyamos el dashboard también para ir depurando todos los valores.»

   El servidor (/ot/api/costeo/calidad, solo superadmin) aplica las reglas de ot_calidad.py sobre las OT del período y entrega:
     · sin `codigo`: una tarjeta por código (cuántas OT, cuánta plata toca y dónde se concentra);
     · con `codigo`: la lista PAGINADA de ese código (REGLA #4.3), con los documentos de cada OT, lo que aportó cada uno y qué otras
       OT usan el mismo documento según el libro de saldo.
   Esta pantalla SOLO LEE: «Arreglar» es un enlace al camino que ya existe (Finanzas por reparar, Regularizar, Saldo de facturas o
   la OT); aquí no hay ninguna escritura. Rojo SOLO para plata realmente perdida; lo demás ámbar o azul. Nombres completos sin cortar
   (REGLA #15), celular primero (REGLA #3), «Seleccionar/deseleccionar todo» (REGLA #14) y sin alert/confirm/prompt nativos (REGLA #1).
   El estado vive en el enlace (#calidad?…): se puede compartir. */
(function (raiz, fabrica) {
  'use strict';
  var api = fabrica();
  if (typeof module === 'object' && module.exports) { module.exports = api; return; }
  raiz.OtCosteoCalidad = api;
})(typeof window !== 'undefined' ? window : this, function () {
  'use strict';

  var SEV_TXT = { rojo: 'Plata perdida', ambar: 'Revisar', azul: 'Informativo' };
  var DIM_TXT = { tec: 'Técnico', cc: 'Centro de costo', cli: 'Cliente' };
  var POR = [10, 25, 50, 100, 200];
  var DEF = { d: '', h: '', fe: 'cierre', al: 'cerradas', c: '', p: 1, pp: 25, q: '' };
  var FECHAS = ['cierre', 'ejecucion', 'creacion', 'programada'];

  // ── Textos ─────────────────────────────────────────────────────────────────────────────────────────────────────
  function esc(s) {
    return String(s === null || s === undefined ? '' : s).replace(/[&<>"']/g, function (c) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
    });
  }
  function clp(n) {
    if (n === null || n === undefined || isNaN(n)) return '—';
    var v = Math.round(Number(n));
    var s = '$' + String(Math.abs(v)).replace(/\B(?=(\d{3})+(?!\d))/g, '.');
    return v < 0 ? '-' + s : s;
  }
  function fechaTxt(iso) {
    if (!iso || !/^\d{4}-\d{2}-\d{2}/.test(iso)) return '';
    return iso.slice(8, 10) + '/' + iso.slice(5, 7) + '/' + iso.slice(0, 4);
  }
  function plural(n, uno, varios) { return n === 1 ? uno : varios; }

  // ── Estado en el enlace (#calidad?…) ───────────────────────────────────────────────────────────────────────────
  function estadoAHash(e) {
    var q = new URLSearchParams();
    ['d', 'h', 'fe', 'al', 'c', 'q'].forEach(function (k) { if (e[k]) q.set(k, e[k]); });
    if (e.c && e.p > 1) q.set('p', String(e.p));
    if (e.c && e.pp !== DEF.pp) q.set('pp', String(e.pp));
    return '#calidad' + (q.toString() ? '?' + q.toString() : '');
  }
  function hashAEstado(hash) {
    if (!/^#calidad/.test(hash || '')) return null;
    var q = new URLSearchParams(String(hash).split('?').slice(1).join('?'));
    var e = {};
    Object.keys(DEF).forEach(function (k) { e[k] = DEF[k]; });
    ['d', 'h'].forEach(function (k) { var v = q.get(k) || ''; if (/^\d{4}-\d{2}-\d{2}$/.test(v)) e[k] = v; });
    if (FECHAS.indexOf(q.get('fe')) >= 0) e.fe = q.get('fe');
    if (q.get('al') === 'todas') e.al = 'todas';
    if (/^[A-Z0-9_]{3,40}$/.test(q.get('c') || '')) e.c = q.get('c');
    var p = parseInt(q.get('p'), 10);
    if (p > 0) e.p = p;
    var pp = parseInt(q.get('pp'), 10);
    if (POR.indexOf(pp) >= 0) e.pp = pp;
    e.q = (q.get('q') || '').slice(0, 120);
    return e;
  }

  // ── Piezas de la pantalla (funciones puras: devuelven HTML) ─────────────────────────────────────────────────────
  function clasif(M) {
    return '<span class="cx-clasif"><i class="bi bi-shield-lock-fill"></i>' + esc((M && M.clasificacion) || 'Confidencial · solo gerencia') + '</span>';
  }

  function htmlCabecera(e, M) {
    M = M || {};
    var ft = M.fechas_txt || { cierre: 'Fecha de cierre', ejecucion: 'Fecha de ejecución', creacion: 'Fecha de creación', programada: 'Fecha programada' };
    var opciones = FECHAS.map(function (k) { return '<option value="' + k + '"' + (e.fe === k ? ' selected' : '') + '>' + esc(ft[k] || k) + '</option>'; }).join('');
    return '<header class="cx-cab"><div><h2>Calidad de datos</h2><p>Lo que falta o no cuadra en el costeo de las OT del período, y por dónde se arregla. ' +
      'Manda el <b>documento que lleva el cobro</b>: varias facturas con servicio se suman, una de solo productos no aporta y un documento de otra empresa no cuenta.</p></div>' +
      clasif(M) + '</header>' +
      '<form class="cx-rango" data-cq-form="rango">' +
      '<label>Desde<input type="date" name="d" value="' + esc(e.d) + '" required></label>' +
      '<label>Hasta<input type="date" name="h" value="' + esc(e.h) + '" required></label>' +
      '<label>Fecha que manda<select name="fe">' + opciones + '</select></label>' +
      '<label class="cx-check"><input type="checkbox" name="al"' + (e.al === 'todas' ? ' checked' : '') + '><span>Incluir OT abiertas</span></label>' +
      '<button type="submit" class="cx-btn"><i class="bi bi-funnel"></i>Aplicar</button></form>';
  }

  function htmlAvisos(R) {
    var M = (R && R.meta) || {}, out = [];
    if (M.truncado) out.push('Se leyeron las ' + M.limite + ' OT más recientes del rango: acórtalo para revisarlas todas.');
    if (M.fallos && M.fallos.length) out.push('No se pudo leer: ' + M.fallos.join(', ') + '. Lo que depende de eso puede verse incompleto; usa «Actualizar».');
    if (R && R.n_sin_verificar) {
      out.push(R.n_sin_verificar + ' ' + plural(R.n_sin_verificar, 'OT se cobra', 'OT se cobran') + ' con documentos sin el aporte registrado (anteriores al 25-09-2026): ' +
        'su cobro todavía no se puede comparar con sus documentos. No se inventa una diferencia.');
    }
    return out.map(function (t) { return '<div class="cx-aviso"><i class="bi bi-info-circle"></i><span>' + esc(t) + '</span></div>'; }).join('');
  }

  function htmlTotales(R, extra) {
    var ps = R.por_severidad || {};
    var chips = ['rojo', 'ambar', 'azul'].filter(function (s) { return ps[s]; }).map(function (s) {
      return '<span class="cq-sev sev-' + s + '">' + ps[s] + ' ' + esc(s === 'rojo' ? 'con plata perdida' : s === 'ambar' ? 'por revisar' : 'con datos informativos') + '</span>';
    }).join('');
    return '<section class="cq-totales" aria-label="Resumen de la revisión"><p><b>' + R.n_ot + '</b> ' + plural(R.n_ot, 'OT revisada', 'OT revisadas') +
      ' · <b>' + R.n_con + '</b> con algo por depurar · <b>' + R.n_sin + '</b> en orden</p>' + (chips ? '<div class="cq-chips">' + chips + '</div>' : '') +
      (extra || '') + '</section>';
  }

  function htmlTarjeta(c, e) {
    var on = e.c === c.codigo;
    var dinero = c.n ? (c.monto ? '<span class="cq-card-m">' + clp(c.monto) + ' ' + esc(c.monto_txt || '') + '</span>' : '') +
      (c.n_sin_monto && c.monto_txt ? '<span class="cq-card-s">' + c.n_sin_monto + ' sin monto medible</span>' : '') : '';
    return '<li><button type="button" class="cq-card sev-' + c.severidad + (on ? ' on' : '') + '" data-cq-acc="codigo" data-val="' + esc(c.codigo) +
      '" aria-pressed="' + (on ? 'true' : 'false') + '">' +
      '<span class="cq-card-h"><i class="cq-dot" aria-hidden="true"></i><span>' + esc(SEV_TXT[c.severidad] || '') + '</span>' + (c.solo_cuenta ? '<span class="cq-solo">solo se cuenta</span>' : '') + '</span>' +
      '<b class="cq-card-t">' + esc(c.titulo) + '</b>' +
      '<span class="cq-card-n">' + c.n + ' <small>' + plural(c.n, 'OT', 'OT') + '</small></span>' + dinero + '</button></li>';
  }

  function htmlResumen(R, e, extra) {
    var cods = R.codigos || [];
    var con = cods.filter(function (c) { return c.n > 0; });
    var sin = cods.filter(function (c) { return !c.n; });
    var tarjetas = con.length ? '<ul class="cq-cards" role="list">' + con.map(function (c) { return htmlTarjeta(c, e); }).join('') + '</ul>'
      : '<div class="cq-limpio"><i class="bi bi-check-circle-fill"></i><span>Todo en orden: ninguna OT del período tiene algo por depurar.</span></div>';
    var ok = sin.length ? '<details class="cq-ok"><summary><i class="bi bi-check-circle-fill"></i>' + sin.length + ' ' + plural(sin.length, 'regla sin casos', 'reglas sin casos') + ' en el período</summary><ul>' +
      sin.map(function (c) { return '<li><b>' + esc(c.titulo) + '.</b> ' + esc(c.regla) + '</li>'; }).join('') + '</ul></details>' : '';
    return htmlTotales(R, extra) + '<section class="cq-resumen" aria-label="Tarjetas por regla de calidad">' + tarjetas + ok + '</section>';
  }

  function htmlConcentracion(conc) {
    var bloques = ['tec', 'cc', 'cli'].filter(function (d) { return (conc[d] || []).length; }).map(function (d) {
      return '<div class="cq-conc-b"><span class="cq-conc-t">' + DIM_TXT[d] + '</span><ul>' + conc[d].map(function (g) {
        return '<li' + (g.otros ? ' class="otros"' : '') + '><b>' + esc(g.nombre) + '</b><span>' + g.n + ' OT' + (g.monto ? ' · ' + clp(g.monto) : '') + '</span></li>';
      }).join('') + '</ul></div>';
    });
    return bloques.length ? '<div class="cq-conc"><h4>¿Dónde se concentra?</h4><div class="cq-conc-g">' + bloques.join('') + '</div></div>' : '';
  }

  function htmlDoc(d) {
    var partes = [esc(d.tipo_txt)];
    if (d.registrado) partes.push('aportó ' + (d.serv ? 'servicio ' + clp(d.serv) : '') + (d.serv && d.desp ? ' + ' : '') + (d.desp ? 'despacho ' + clp(d.desp) : '') + (!d.serv && !d.desp ? '$0' : ''));
    else if (d.cobro) partes.push('sin aporte registrado');
    var marcas = '';
    if (!d.cobro) marcas += '<span class="cq-tag">referencia</span>';
    if (d.reemplazado) marcas += '<span class="cq-tag">dada de baja por su factura</span>';
    if (d.heredado) marcas += '<span class="cq-tag">de la OT madre</span>';
    if (d.otra_empresa) marcas += '<span class="cq-tag t-ambar">otra empresa' + (d.justificado ? ' (justificado)' : '') + '</span>';
    var extra = '';
    if (d.saldo) {
      extra += '<small>Saldo según el libro: servicio ' + clp(d.saldo.servicio) + ' de ' + clp(d.saldo.cap_servicio) +
        (d.saldo.cap_despacho ? ' · despacho ' + clp(d.saldo.despacho) + ' de ' + clp(d.saldo.cap_despacho) : '') + (d.saldo.leido ? '' : ' · aún sin leer en Random') + '</small>';
    }
    if (d.otras_ot && d.otras_ot.length) {
      extra += '<small class="cq-otras">También lo usan: ' + d.otras_ot.map(function (u) {
        return esc(u.numero) + (u.cliente ? ' (' + esc(u.cliente) + ')' : '') + ' · ' + esc(u.categoria) + ' ' + clp(u.monto);
      }).join('; ') + '</small>';
    }
    return '<li><b>' + esc(d.titulo) + '</b> <span>' + partes.join(' · ') + '</span>' + marcas + extra + '</li>';
  }

  function htmlFila(f, sel, C) {
    var td = function (et, html, cls) { return '<td data-et="' + esc(et) + '"' + (cls ? ' class="' + cls + '"' : '') + '><span class="cx-td">' + html + '</span></td>'; };
    var docs = (f.docs || []).length ? '<ul class="cq-docs">' + f.docs.map(htmlDoc).join('') + '</ul>' : '<span class="cq-nada">Sin documentos declarados</span>';
    var otros = (f.otros || []).length ? '<small class="cq-otros">También: ' + f.otros.map(function (o) { return esc(o.titulo); }).join(' · ') + '</small>' : '';
    var arr = f.arreglo ? '<a class="cx-btn cq-arreglar" href="' + esc(f.arreglo.url) + '" title="' + esc('Te lleva a ' + f.arreglo.donde) + '"><i class="bi bi-tools"></i>' + esc(f.arreglo.texto) + '</a>'
      : '<span class="cq-nada">Solo se cuenta</span>';
    var plata = f.monto === null || f.monto === undefined ? '<span class="cq-nada">sin monto medible</span>'
      : '<b class="' + (C && C.severidad === 'rojo' ? 'cq-rojo' : '') + '">' + clp(f.monto) + '</b><small>' + esc((C && C.monto_txt) || '') + '</small>';
    return '<tr' + (sel ? ' class="sel"' : '') + '>' +
      td('Elegir', '<input type="checkbox" class="cx-chk" data-cq-cambio="sel" data-id="' + f.id + '"' + (sel ? ' checked' : '') + ' aria-label="Elegir ' + esc(f.numero) + '">') +
      td('OT', '<a href="' + esc(f.url) + '"><b>' + esc(f.numero) + '</b></a><small>' + esc(f.cliente) + '</small><small>' + esc(f.tecnico) + ' · ' + esc(f.centro) + '</small>' +
        '<small>' + esc(f.estado_txt) + (f.fecha ? ' · ' + fechaTxt(f.fecha) : '') + '</small>') +
      td('Qué pasa', esc(f.detalle) + otros) +
      td('Documentos', docs) +
      td('Plata', plata, 'num') +
      td('Arreglar', arr) + '</tr>';
  }

  function htmlLista(L, e, sel) {
    if (!L) return '';
    var C = L.codigo || {}, filas = L.filas || [];
    var nSel = filas.filter(function (f) { return sel[f.id]; }).length;
    var opciones = POR.map(function (t) { return '<option value="' + t + '"' + (t === L.por ? ' selected' : '') + '>' + t + '</option>'; }).join('');
    var cuerpo = filas.length ? '<div class="cx-tabla-wrap"><table class="cx-tabla cq-tabla"><thead><tr><th scope="col"><span class="cx-sr">Elegir</span></th>' +
      '<th scope="col">OT</th><th scope="col">Qué pasa</th><th scope="col">Documentos</th><th scope="col" class="num">Plata</th><th scope="col">Arreglar</th></tr></thead><tbody>' +
      filas.map(function (f) { return htmlFila(f, !!sel[f.id], C); }).join('') + '</tbody></table></div>'
      : '<div class="cx-vacio">' + (e.q ? 'Ninguna OT de esta regla coincide con la búsqueda.' : 'Ninguna OT del período tiene este caso.') + '</div>';
    return '<section class="cx-card cq-lista sev-' + esc(C.severidad) + '" aria-labelledby="cqListaT" id="cqLista">' +
      '<header class="cx-card-h"><div class="cq-lista-t"><span class="cq-sev sev-' + esc(C.severidad) + '">' + esc(SEV_TXT[C.severidad] || '') + '</span>' +
      '<h3 id="cqListaT">' + esc(C.titulo) + ' <small>· ' + L.total + ' OT</small></h3><p class="cq-regla">' + esc(C.regla) + '</p></div>' +
      '<button type="button" class="cx-btn cx-btn-sec" data-cq-acc="cerrar"><i class="bi bi-x-lg"></i>Cerrar la lista</button></header>' +
      '<p class="cq-como"><i class="bi bi-tools"></i><span><b>Cómo se arregla:</b> ' + esc(C.como) + '</span></p>' +
      htmlConcentracion(L.concentracion || {}) +
      '<form class="cq-buscar" data-cq-form="buscar" role="search"><label><span class="cx-sr">Buscar en esta lista</span><input type="search" name="q" value="' + esc(e.q) +
      '" placeholder="OT, cliente o técnico" aria-label="Buscar por N° de OT, cliente o técnico"></label><button type="submit" class="cx-btn cx-btn-sec"><i class="bi bi-search"></i>Buscar</button></form>' +
      '<div class="cq-acc"><button type="button" class="cx-btn cx-btn-sec" data-cq-acc="sel-todo"><i class="bi bi-check-all"></i>Seleccionar/deseleccionar todo</button>' +
      '<button type="button" class="cx-btn cx-btn-sec" data-cq-acc="copiar"' + (nSel ? '' : ' disabled') + '><i class="bi bi-clipboard"></i>' +
      (nSel ? 'Copiar los ' + nSel + ' N° de OT elegidos' : 'Copiar los N° de OT elegidos') + '</button></div>' +
      cuerpo +
      '<div class="cx-pag"><span aria-live="polite">' + (L.total ? 'Mostrando ' + L.desde + '–' + L.hasta + ' de ' + L.total : 'Sin resultados') + (nSel ? ' · ' + nSel + ' ' + plural(nSel, 'elegida', 'elegidas') : '') + '</span>' +
      '<label>Por página <select data-cq-cambio="pp" aria-label="OT por página">' + opciones + '</select></label>' +
      '<span class="cx-pag-btns"><button type="button" data-cq-acc="pag" data-val="-1"' + (L.page <= 1 ? ' disabled' : '') + '>Anterior</button>' +
      '<span>Página ' + L.page + ' de ' + L.pages + '</span>' +
      '<button type="button" data-cq-acc="pag" data-val="1"' + (L.page >= L.pages ? ' disabled' : '') + '>Siguiente</button></span></div></section>';
  }

  function htmlPie(M) {
    return '<footer class="cx-pie"><p><b>Cómo leer esta pestaña.</b> Cada tarjeta es una regla de calidad sobre las OT del período (las mismas del tablero Explorar). ' +
      'Rojo es solo plata realmente perdida; ámbar es para revisar y azul es informativo. «Arreglar» te lleva al camino que ya existe: aquí no se cambia nada.</p>' +
      '<p>Solo lectura: no escribe nada, no consulta a Random ni a Check y nunca le llega a un técnico. Leído a las ' + esc((M && M.generado) || '') +
      ' (hora de Chile); se guarda 90 segundos: usa «Actualizar» para releer. ' + clasif(M) + '</p></footer>';
  }

  // ════════════════════════════════════════════════════════════════════════════════════════════════════════════
  //  Controlador
  // ════════════════════════════════════════════════════════════════════════════════════════════════════════════
  function crear(raiz, deps) {
    deps = deps || {};
    var S = { e: null, R: null, L: null, sel: {}, cargadoR: null, cargadoL: null, cargando: false, error: null };
    var pedir = deps.fetch || function (u, o) { return window.fetch(u, o); };
    var aviso = deps.toast || function (m, t) { try { if (window.ilusToast) window.ilusToast(m, { type: t || 'info' }); } catch (x) { /* sin aviso */ } };
    var copiar = deps.copiar || function (txt) {
      try { if (navigator.clipboard && navigator.clipboard.writeText) return navigator.clipboard.writeText(txt); } catch (x) { /* sin portapapeles */ }
      return Promise.reject(new Error('sin portapapeles'));
    };
    var leerHash = deps.leerHash || function () { return (typeof location !== 'undefined' && location.hash) || ''; };
    var escribirHash = deps.escribirHash || function (h) {
      try { if (window.history && history.replaceState) history.replaceState(null, '', location.pathname + location.search + h); } catch (x) { /* nada */ }
    };

    function clave() { var e = S.e; return [e.d, e.h, e.fe, e.al].join('|'); }
    function claveL() { var e = S.e; return [clave(), e.c, e.p, e.pp, e.q].join('|'); }
    function base() {
      var q = new URLSearchParams(), e = S.e;
      if (e.d) q.set('desde', e.d);
      if (e.h) q.set('hasta', e.h);
      q.set('fecha', e.fe);
      q.set('alcance', e.al);
      return q;
    }
    function obtener(q) {
      return pedir(raiz.getAttribute('data-url') + '?' + q.toString(), {
        credentials: 'same-origin', headers: { 'Accept': 'application/json', 'X-Requested-With': 'XMLHttpRequest' }
      }).then(function (r) {
        return r.json().catch(function () { return {}; }).then(function (j) {
          if (!r.ok || !j || !j.ok) throw new Error((j && j.error) || 'No pudimos revisar la calidad de los datos. Vuelve a intentarlo.');
          return j;
        });
      });
    }

    function pintar() {
      var foco = null;
      try {
        var a = typeof document !== 'undefined' ? document.activeElement : null;
        if (a && raiz.contains && raiz.contains(a)) foco = { acc: a.getAttribute('data-cq-acc') || a.getAttribute('data-cq-cambio') || a.getAttribute('name') || '', val: a.getAttribute('data-val') || a.getAttribute('data-id') || '' };
      } catch (x) { foco = null; }
      if (S.error && !S.R) {
        raiz.innerHTML = htmlCabecera(S.e, {}) + '<div class="cx-aviso" role="alert"><i class="bi bi-exclamation-circle"></i><span>' + esc(S.error) +
          '</span> <button type="button" class="cx-btn cx-btn-sec" data-cq-acc="reintentar"><i class="bi bi-arrow-clockwise"></i>Reintentar</button></div>';
        return;
      }
      if (!S.R) {
        raiz.innerHTML = '<div class="cx-cargando" role="status"><i class="bi bi-hourglass-split"></i><span>Revisando la calidad de los datos…</span></div>';
        return;
      }
      var M = S.R.meta || {};
      var acciones = '<button type="button" class="cx-btn cx-btn-sec" data-cq-acc="actualizar"><i class="bi bi-arrow-clockwise"></i>Actualizar</button>';
      var lista = S.e.c ? (S.L && (S.cargadoL === claveL() || (S.cargando && S.L.codigo && S.L.codigo.codigo === S.e.c)) ? htmlLista(S.L, S.e, S.sel)
        : (S.error ? '<div class="cx-aviso" role="alert"><i class="bi bi-exclamation-circle"></i><span>' + esc(S.error) + '</span></div>'
          : '<div class="cx-cargando" role="status"><i class="bi bi-hourglass-split"></i><span>Cargando la lista…</span></div>'))
        : '<p class="cq-invita"><i class="bi bi-hand-index"></i>Toca una tarjeta para ver sus OT, dónde se concentra y por dónde se arregla.</p>';
      raiz.innerHTML = htmlCabecera(S.e, M) + htmlAvisos(S.R) + htmlResumen(S.R, S.e, acciones) + lista + htmlPie(M);
      raiz.classList && raiz.classList.toggle('cx-ocupado', !!S.cargando);
      if (foco && foco.acc && raiz.querySelectorAll) {
        var els = raiz.querySelectorAll('[data-cq-acc],[data-cq-cambio],input[name="q"]');
        for (var i = 0; i < els.length; i++) {
          var x = els[i];
          var acc = x.getAttribute('data-cq-acc') || x.getAttribute('data-cq-cambio') || x.getAttribute('name') || '';
          var val = x.getAttribute('data-val') || x.getAttribute('data-id') || '';
          if (acc === foco.acc && val === foco.val) { try { x.focus({ preventScroll: true }); } catch (err) { x.focus(); } break; }
        }
      }
    }

    function guardar() { escribirHash(estadoAHash(S.e)); }

    function cargarResumen(forzar) {
      var q = base();
      if (forzar) q.set('actualizar', '1');
      var pedido = clave();
      S.cargando = true;
      S.error = null;
      pintar();
      return obtener(q).then(function (j) {
        S.cargando = false;
        if (clave() !== pedido) return cargarResumen(false);
        S.R = j;
        S.e.d = (j.meta && j.meta.desde) || S.e.d;
        S.e.h = (j.meta && j.meta.hasta) || S.e.h;
        S.cargadoR = clave();
        guardar();
        if (S.e.c) return cargarLista();
        pintar();
      }).catch(function (err) {
        S.cargando = false;
        S.error = (err && err.message) || 'No pudimos revisar la calidad de los datos.';
        pintar();
      });
    }

    function cargarLista() {
      var e = S.e;
      if (!e.c) { pintar(); return Promise.resolve(); }
      var q = base();
      q.set('codigo', e.c);
      q.set('page', String(e.p));
      q.set('por', String(e.pp));
      if (e.q) q.set('q', e.q);
      var pedido = claveL();
      S.cargando = true;
      S.error = null;
      pintar();
      return obtener(q).then(function (j) {
        S.cargando = false;
        if (claveL() !== pedido) return cargarLista();
        S.L = j;
        S.e.p = j.page;
        S.cargadoL = claveL();
        guardar();
        pintar();
      }).catch(function (err) {
        S.cargando = false;
        S.error = (err && err.message) || 'No pudimos armar la lista.';
        pintar();
      });
    }

    function mostrar(escribir) {
      var e = hashAEstado(leerHash());
      if (e) S.e = e;
      else if (!S.e) { S.e = {}; Object.keys(DEF).forEach(function (k) { S.e[k] = DEF[k]; }); }
      if (escribir) guardar();
      if (!S.R || S.cargadoR !== clave()) return cargarResumen(false);
      if (S.e.c && S.cargadoL !== claveL()) return cargarLista();
      pintar();
      return Promise.resolve();
    }

    function filasVisibles() { return (S.L && S.L.filas) || []; }

    function alClic(ev) {
      var b = ev.target && ev.target.closest ? ev.target.closest('[data-cq-acc]') : null;
      if (!b || (raiz.contains && !raiz.contains(b)) || b.disabled) return;
      var acc = b.getAttribute('data-cq-acc'), e = S.e;
      if (acc === 'codigo') {
        var c = b.getAttribute('data-val');
        e.c = (e.c === c) ? '' : c;          // otro toque en la misma tarjeta la cierra
        e.p = 1; e.q = ''; S.sel = {};
        guardar();
        if (e.c) cargarLista(); else pintar();
      } else if (acc === 'cerrar') {
        e.c = ''; e.p = 1; e.q = ''; S.sel = {};
        guardar();
        pintar();
      } else if (acc === 'pag') {
        e.p = Math.max(1, (e.p || 1) + (parseInt(b.getAttribute('data-val'), 10) || 0));
        guardar();
        cargarLista();
      } else if (acc === 'sel-todo') {
        var filas = filasVisibles();
        var falta = filas.some(function (f) { return !S.sel[f.id]; });   // REGLA #14: si falta alguna, marca todas
        filas.forEach(function (f) { if (falta) S.sel[f.id] = true; else delete S.sel[f.id]; });
        pintar();
      } else if (acc === 'copiar') {
        var nums = filasVisibles().filter(function (f) { return S.sel[f.id]; }).map(function (f) { return f.numero; });
        if (!nums.length) return;
        Promise.resolve(copiar(nums.join('\n'))).then(function () {
          aviso('Copiados ' + nums.length + ' ' + plural(nums.length, 'N° de OT', 'N° de OT') + '.', 'success');
        }).catch(function () { aviso('No se pudo copiar: tu navegador no lo permite. Selecciona los números a mano.', 'warning'); });
      } else if (acc === 'actualizar') {
        S.L = null; S.cargadoL = null;
        cargarResumen(true);
      } else if (acc === 'reintentar') {
        cargarResumen(false);
      }
    }

    function alCambiar(ev) {
      var el = ev.target, c = el && el.getAttribute ? el.getAttribute('data-cq-cambio') : null;
      if (!c) return;
      if (c === 'sel') {
        var id = parseInt(el.getAttribute('data-id'), 10);
        if (el.checked) S.sel[id] = true; else delete S.sel[id];
        pintar();
      } else if (c === 'pp') {
        var pp = parseInt(el.value, 10);
        S.e.pp = POR.indexOf(pp) >= 0 ? pp : DEF.pp;
        S.e.p = 1;
        guardar();
        cargarLista();
      }
    }

    function alEscribir(ev) {
      // REGLA #4.3 (punto 4): al dejar la búsqueda VACÍA la lista se recarga sola, sin tener que apretar «Buscar».
      var el = ev.target;
      if (!el || el.name !== 'q' || el.value !== '' || !S.e.q) return;
      S.e.q = ''; S.e.p = 1;
      guardar();
      cargarLista();
    }

    function alEnviar(ev) {
      var f = ev.target && ev.target.closest ? ev.target.closest('[data-cq-form]') : null;
      if (!f) return;
      if (ev.preventDefault) ev.preventDefault();
      var e = S.e, tipo = f.getAttribute('data-cq-form');
      if (tipo === 'buscar') {
        e.q = String((f.elements.q && f.elements.q.value) || '').trim().slice(0, 120);
        e.p = 1;
        S.sel = {};
        guardar();
        cargarLista();
        return;
      }
      var d = f.elements.d.value, h = f.elements.h.value;
      if (/^\d{4}-\d{2}-\d{2}$/.test(d)) e.d = d;
      if (/^\d{4}-\d{2}-\d{2}$/.test(h)) e.h = h;
      if (FECHAS.indexOf(f.elements.fe.value) >= 0) e.fe = f.elements.fe.value;
      e.al = f.elements.al.checked ? 'todas' : 'cerradas';
      e.p = 1;
      S.sel = {};
      S.L = null; S.cargadoL = null;
      guardar();
      cargarResumen(false);
    }

    if (raiz.addEventListener) {
      raiz.addEventListener('click', alClic);
      raiz.addEventListener('change', alCambiar);
      raiz.addEventListener('input', alEscribir);
      raiz.addEventListener('submit', alEnviar);
    }
    return { mostrar: mostrar, estado: function () { return S; }, alClic: alClic, alCambiar: alCambiar, alEnviar: alEnviar, alEscribir: alEscribir };
  }

  var CTL = null;
  function mostrar(escribir) {
    if (!CTL) {
      var r = typeof document !== 'undefined' ? document.getElementById('cqApp') : null;
      if (!r) return;
      CTL = crear(r);
    }
    return CTL.mostrar(escribir);
  }

  return {
    mostrar: mostrar, crear: crear, estadoAHash: estadoAHash, hashAEstado: hashAEstado, htmlResumen: htmlResumen,
    htmlLista: htmlLista, htmlFila: htmlFila, htmlCabecera: htmlCabecera, htmlAvisos: htmlAvisos, clp: clp, esc: esc, fechaTxt: fechaTxt,
    SEV_TXT: SEV_TXT, POR: POR
  };
});

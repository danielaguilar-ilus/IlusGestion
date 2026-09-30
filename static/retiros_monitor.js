/* Monitor de Retiros: búsqueda instantánea, filtros, orden, paginación y detalle
   desplegable de la tabla (Daniel 2026-09-29).
   La lógica pura (coincide, ordenar, paginar, conteos, segmentar, aCsv) no toca el DOM
   y se prueba en Node (tests/test_retiros_monitor_js.py). El resto conecta esa lógica
   con la tabla que arma el servidor (templates/retiros/_monitor_tabla.html). */
(function (global) {
  'use strict';

  // ═════════════ Lógica pura ═════════════
  function norm(s) {
    return String(s == null ? '' : s).toLowerCase().normalize('NFD').replace(/[̀-ͯ]/g, '').trim();
  }
  function terminos(q) { return norm(q).split(/\s+/).filter(Boolean); }

  // Cada palabra debe aparecer. Se acepta también sin puntos, guiones ni "+", para
  // que "12.345.678-5" encuentre el RUT guardado como 123456785.
  function coincide(f, st) {
    var i, t, alt;
    if (st.terms && st.terms.length) {
      for (i = 0; i < st.terms.length; i++) {
        t = st.terms[i];
        if (f.search.indexOf(t) === -1) {
          alt = t.replace(/[.\-+]/g, '');
          if (!alt || f.search.indexOf(alt) === -1) return false;
        }
      }
    }
    if (st.grupo && f.grupo !== st.grupo) return false;
    if (st.alerta && f.alerta !== st.alerta) return false;
    if (st.resp) {
      if (st.resp === '__sin__') { if (f.resp) return false; }
      else if (f.resp !== st.resp) return false;
    }
    switch (st.fecha) {
      case 'hoy': if (f.req !== st.hoy && f.conf !== st.hoy) return false; break;   // igual que la tarjeta "Retiros hoy"
      case 'manana': if (f.dias !== 1) return false; break;
      case 'semana': if (!f.semana) return false; break;                             // igual que la tarjeta "esta semana"
      case 'vencidas': if (!f.vencida) return false; break;
      case 'sin_fecha': if (f.fecha) return false; break;
    }
    return true;
  }

  var CLAVES = {
    solicitud: function (f) { return f.creado; },
    cliente: function (f) { return f.cliente; },
    resp: function (f) { return f.resp; },
    fecha: function (f) { return f.fecha; },
    estado: function (f) { return f.estadoIdx; },
    cal: function (f) { return f.cal; },
    carga: function (f) { return f.bultos; }
  };

  // Estable: a igual valor se respeta el orden que trajo el servidor.
  function ordenar(filas, clave, dir) {
    var get = CLAVES[clave];
    if (!get) return filas.slice();
    var sgn = dir === 'desc' ? -1 : 1;
    return filas.slice().sort(function (a, b) {
      var va = get(a), vb = get(b);
      var ea = va === '' || va == null, eb = vb === '' || vb == null;
      if (ea && eb) return a.orden - b.orden;
      if (ea) return 1;      // sin dato queda al final, en cualquier sentido
      if (eb) return -1;
      var c = (typeof va === 'string' || typeof vb === 'string')
        ? String(va).localeCompare(String(vb), 'es') : va - vb;
      return c !== 0 ? c * sgn : a.orden - b.orden;
    });
  }

  function paginar(total, pagina, por) {
    por = Math.max(1, por | 0);
    var paginas = Math.max(1, Math.ceil(total / por));
    var p = Math.min(Math.max(1, pagina | 0), paginas);
    return { pagina: p, paginas: paginas, por: por,
             desde: total ? (p - 1) * por + 1 : 0, hasta: Math.min(total, p * por) };
  }

  // Cuántas filas quedarían en cada valor de `clave` ('grupo' | 'alerta') aplicando
  // todos los demás filtros: así los contadores de los chips dicen lo que verías al elegirlos.
  function conteos(filas, st, clave) {
    var base = {}, k;
    for (k in st) { if (Object.prototype.hasOwnProperty.call(st, k)) base[k] = st[k]; }
    base[clave] = '';
    var out = { __total__: 0 };
    filas.forEach(function (f) {
      if (!coincide(f, base)) return;
      out.__total__++;
      out[f[clave]] = (out[f[clave]] || 0) + 1;
    });
    return out;
  }

  // Parte un texto en tramos marcados / sin marcar, ignorando mayúsculas y tildes.
  function segmentar(texto, terms) {
    var t = String(texto == null ? '' : texto);
    if (!terms || !terms.length || !t) return [{ t: t, m: false }];
    var n = '', i, lc, base;
    for (i = 0; i < t.length; i++) {
      lc = t.charAt(i).toLowerCase();
      base = lc.length === 1 ? lc.normalize('NFD').replace(/[̀-ͯ]/g, '') : ' ';
      n += base.length === 1 ? base : ' ';       // misma longitud que el original
    }
    var marca = new Array(t.length), pos, k;
    terms.forEach(function (term) {
      if (!term) return;
      pos = 0;
      while ((pos = n.indexOf(term, pos)) !== -1) {
        for (k = pos; k < pos + term.length; k++) marca[k] = true;
        pos += term.length;
      }
    });
    var out = [], ini = 0, actual = !!marca[0];
    for (i = 1; i <= t.length; i++) {
      if (i === t.length || !!marca[i] !== actual) {
        out.push({ t: t.slice(ini, i), m: actual });
        ini = i; actual = !!marca[i];
      }
    }
    return out;
  }

  // CSV para Excel en español: separador ";" y BOM. Una celda que empieza con = @ -
  // podría ejecutarse como fórmula (los nombres vienen del formulario público): se
  // antepone una comilla. Un teléfono "+56 9…" pierde el "+" para no parecer fórmula.
  function celdaCsv(v) {
    var s = String(v == null ? '' : v);
    if (/^\+[\d\s]+$/.test(s)) s = s.slice(1);
    else if (/^[=@\-+\t\r]/.test(s)) s = "'" + s;
    return /[";\r\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
  }
  function aCsv(filas) {
    if (!filas || !filas.length) return '';
    var cols = Object.keys(filas[0]);
    var lineas = [cols.map(celdaCsv).join(';')];
    filas.forEach(function (r) { lineas.push(cols.map(function (c) { return celdaCsv(r[c]); }).join(';')); });
    return '﻿' + lineas.join('\r\n');
  }

  global.RetirosMonitorLogica = {
    norm: norm, terminos: terminos, coincide: coincide, ordenar: ordenar, paginar: paginar,
    conteos: conteos, segmentar: segmentar, aCsv: aCsv
  };

  // ═════════════ Conexión con la tabla ═════════════
  if (typeof document === 'undefined') return;

  var GUARDA = 'ilus.retiros.monitor.v1';
  var COLS = ['doc', 'ret', 'resp', 'cal', 'carga'];

  function $(id) { return document.getElementById(id); }
  function el(tag, cls, txt) {
    var e = document.createElement(tag);
    if (cls) e.className = cls;
    if (txt != null) e.textContent = txt;
    return e;
  }
  function toast(msg, tipo) { if (typeof global.ilusToast === 'function') global.ilusToast(msg, { type: tipo || 'info' }); }

  function leerPref() {
    try { return JSON.parse(global.localStorage.getItem(GUARDA) || '{}') || {}; } catch (e) { return {}; }
  }
  function guardarPref(p) {
    try { global.localStorage.setItem(GUARDA, JSON.stringify(p)); } catch (e) { /* modo privado: no pasa nada */ }
  }

  function iniciar() {
    var tabla = $('rmTable');
    if (!tabla) return;
    var tbody = tabla.tBodies[0];
    var raiz = $('rmMonitor');
    var vacio = $('rmEmpty');
    var DATOS = {};
    try { DATOS = JSON.parse(($('rmData') || {}).textContent || '{}') || {}; } catch (e) { DATOS = {}; }
    var pref = leerPref();
    var st = { q: '', terms: [], grupo: '', alerta: '', fecha: '', resp: '', sort: '', dir: 'asc',
               page: 1, per: [10, 25, 50, 100].indexOf(pref.per) !== -1 ? pref.per : 10, hoy: tabla.dataset.hoy || '' };
    var filas = [];

    function leerFilas() {
      filas = [].slice.call(tbody.querySelectorAll('tr.rm-row')).map(function (tr, i) {
        var d = tr.dataset;
        var prev = filas.filter(function (x) { return x.el === tr; })[0];
        return {
          el: tr, det: prev ? prev.det : null, abierto: prev ? prev.abierto : false, orden: i, id: d.rid,
          search: d.search || '', grupo: d.grupo || '', alerta: d.alerta || '', resp: d.resp || '',
          fecha: d.fecha || '', dias: d.dias === '' || d.dias == null ? null : parseInt(d.dias, 10),
          semana: d.semana === '1', vencida: d.vencida === '1', req: d.req || '', conf: d.conf || '',
          creado: parseInt(d.creado || '0', 10), cliente: d.cliente || '', bultos: parseInt(d.bultos || '0', 10),
          cal: parseInt(d.cal || '0', 10), estadoIdx: parseInt(d.estadoIdx || '99', 10)
        };
      });
    }
    leerFilas();

    // ── Detalle desplegable (se arma al abrir, con textContent: nada de HTML con datos del cliente) ──
    function linea(etq, valor, href) {
      var p = el('p'), b = el('b', null, etq + ': ');
      p.appendChild(b);
      if (href && valor) { var a = el('a', null, valor); a.href = href; p.appendChild(a); }
      else p.appendChild(document.createTextNode(valor || '—'));
      return p;
    }
    function panel(icono, titulo, hijos, ancho) {
      var s = el('section', 'rm-panel' + (ancho ? ' rm-panel-wide' : ''));
      var h = el('h4'); var i = el('i', 'bi ' + icono); h.appendChild(i); h.appendChild(document.createTextNode(titulo));
      s.appendChild(h);
      hijos.forEach(function (c) { s.appendChild(c); });
      return s;
    }
    function crearDetalle(f) {
      var d = DATOS[f.id];
      var tr = el('tr', 'rm-detail'), td = el('td');
      td.colSpan = 11;
      var g = el('div', 'rm-panels');
      if (!d) {
        g.appendChild(panel('bi-info-circle', 'Detalle', [linea('Solicitud', 'Sin datos adicionales')]));
      } else {
        var c = d.csv, x = d.x || {};
        var tel = String(c['Teléfono'] || '').replace(/[^\d+]/g, '');
        g.appendChild(panel('bi-person-lines-fill', 'Contacto', [
          linea('Nombre', c['Contacto']), linea('RUT', c['RUT cliente']),
          linea('Teléfono', c['Teléfono'], tel ? 'tel:' + tel : ''),
          linea('Correo', c['Correo'], c['Correo'] ? 'mailto:' + c['Correo'] : '')]));
        g.appendChild(panel('bi-person-badge', 'Quién retira', c['Quién retira'] ? [
          linea('Nombre', c['Quién retira']), linea('Parentesco', x.rel),
          linea('RUT', c['RUT quien retira']), linea('Teléfono', x.ptel)] : [linea('Persona', 'No indicada')]));
        var bultos = parseInt(c['Bultos'], 10) || 0;
        var carga = [linea('Bultos', bultos + (bultos === 1 ? ' bulto' : ' bultos'))];
        if (x.sp) carga.push(linea('Peso', 'Por confirmar'));
        else {
          carga.push(linea('Peso real', c['Peso kg'] + ' kg'));
          carga.push(linea('Peso volumétrico', c['Peso volumétrico'] + ' kg'));
          carga.push(linea('Volumen', c['Volumen m3'] + ' m³'));
        }
        if (x.min) carga.push(linea('Preparación estimada', x.min + ' min'));
        g.appendChild(panel('bi-box-seam', 'Documento y carga',
          [linea('Documento', c['Documento'] || 'Sin factura'), linea('Validación', x.dv || '—')].concat(carga)));
        var tl = el('ol', 'rm-tl');
        if (x.tl && x.tl.length) {
          x.tl.forEach(function (e) {
            var li = el('li'); li.appendChild(el('strong', null, e.txt));
            li.appendChild(el('div', 'rm-sub', e.cuando + (e.quien ? ' · ' + e.quien : '')));
            tl.appendChild(li);
          });
        } else { var vli = el('li'); vli.appendChild(el('div', 'rm-sub', 'Sin movimientos registrados.')); tl.appendChild(vli); }
        var resumen = el('p', 'rm-sub', 'Creada ' + c['Creada'] + ' · Canal ' + c['Canal'] + (x.por ? ' · Por ' + x.por : ''));
        g.appendChild(panel('bi-clock-history', 'Seguimiento', [tl, resumen], true));
      }
      var acc = el('div', 'rm-acciones');
      var ficha = el('a', 'rm-btn rm-btn-rojo'); ficha.href = f.el.dataset.href || '#';
      ficha.appendChild(el('i', 'bi bi-eye')); ficha.appendChild(document.createTextNode('Abrir ficha'));
      acc.appendChild(ficha);
      if (f.el.dataset.pub) {
        var pub = el('a', 'rm-btn'); pub.href = f.el.dataset.pub; pub.target = '_blank'; pub.rel = 'noopener';
        pub.appendChild(el('i', 'bi bi-link-45deg')); pub.appendChild(document.createTextNode('Link del cliente'));
        acc.appendChild(pub);
      }
      if (f.el.dataset.reag !== undefined && typeof global.abrirReagendarMonitor === 'function') {
        var re = el('button', 'rm-btn'); re.type = 'button';
        re.appendChild(el('i', 'bi bi-calendar-plus')); re.appendChild(document.createTextNode('Reagendar / proponer otra fecha'));
        re.addEventListener('click', function () { global.abrirReagendarMonitor(parseInt(f.id, 10), f.el.dataset.code || '', f.el.dataset.reag || ''); });
        acc.appendChild(re);
      }
      g.appendChild(acc);
      td.appendChild(g); tr.appendChild(td);
      f.el.parentNode.insertBefore(tr, f.el.nextSibling);
      return tr;
    }
    function abrir(f, si) {
      f.abierto = si;
      if (si && !f.det) f.det = crearDetalle(f);
      if (f.det) f.det.hidden = !si;
      var b = f.el.querySelector('.rm-exp');
      if (b) b.setAttribute('aria-expanded', si ? 'true' : 'false');
    }

    // ── Menú ⋮ de cada fila: se arma la primera vez que se abre (no viaja repetido en el HTML) ──
    function llenarMenu(btn) {
      var menu = btn.parentNode.querySelector('.dropdown-menu');
      if (!menu || menu.children.length) return;
      var d = btn.closest('tr').dataset, cli = ((DATOS[d.rid] || {}).csv || {}).Cliente || '';
      function item(icono, clase, texto) {
        var li = el('li'), it = el('a', 'dropdown-item');
        it.appendChild(el('i', 'bi ' + icono + ' me-1 ' + clase)); it.appendChild(document.createTextNode(texto));
        li.appendChild(it); menu.appendChild(li);
        return it;
      }
      item('bi-eye', 'text-primary', 'Ver ficha').href = d.href;
      var pub = item('bi-link-45deg', 'text-success', 'Link cliente'); pub.href = d.pub; pub.target = '_blank'; pub.rel = 'noopener';
      if (d.reag !== undefined) {
        var re = item('bi-calendar-plus', 'text-danger', 'Reagendar / Proponer otra fecha');
        re.setAttribute('role', 'button'); re.href = '#';
        re.addEventListener('click', function (e) {
          e.preventDefault();
          if (typeof global.abrirReagendarMonitor === 'function') global.abrirReagendarMonitor(parseInt(d.rid, 10), d.code || '', d.reag || '');
        });
      }
      if (tabla.dataset.super === '1') {
        var hr = el('li'); hr.appendChild(el('hr', 'dropdown-divider')); menu.appendChild(hr);
        var del = item('bi-trash3', '', 'Eliminar (superadmin)'); del.classList.add('text-danger'); del.href = '#';
        del.dataset.rid = d.rid; del.dataset.code = d.code || ''; del.dataset.cliente = cli;
        del.addEventListener('click', function (e) {
          e.preventDefault();
          if (typeof global.eliminarSolicitudDesdeMonitor === 'function') global.eliminarSolicitudDesdeMonitor(del);
        });
      }
    }
    // Bootstrap abre el menú en fase de captura (y la celda corta el "click"): por eso se llena en
    // eventos anteriores. show.bs.dropdown llega justo antes de posicionarlo; pointerdown y focusin
    // cubren el ratón, el dedo y el teclado. llenarMenu no hace nada si ya está armado.
    ['show.bs.dropdown', 'pointerdown', 'focusin'].forEach(function (ev) {
      tbody.addEventListener(ev, function (e) {
        var b = e.target.closest && e.target.closest('.rm-acc-btn');
        if (b) llenarMenu(b);
      });
    });

    // ── Resaltar lo buscado ──
    function resaltar(tr) {
      var els = tr.querySelectorAll('.rm-hl'), i, e, txt, segs;
      for (i = 0; i < els.length; i++) {
        e = els[i];
        if (e.dataset.orig === undefined) e.dataset.orig = e.textContent;
        txt = e.dataset.orig;
        segs = st.terms.length ? segmentar(txt, st.terms) : null;
        if (!segs || (segs.length === 1 && !segs[0].m)) {
          if (e.dataset.marcado === '1') { e.textContent = txt; e.dataset.marcado = ''; }
          continue;
        }
        e.textContent = '';
        segs.forEach(function (s) {
          if (s.m) e.appendChild(el('mark', 'rm-mark', s.t)); else e.appendChild(document.createTextNode(s.t));
        });
        e.dataset.marcado = '1';
      }
    }

    // ── Pintado ──
    function filtradas() {
      var v = filas.filter(function (f) { return coincide(f, st); });
      return st.sort ? ordenar(v, st.sort, st.dir) : v;
    }
    function hayFiltros() { return !!(st.q || st.grupo || st.alerta || st.fecha || st.resp); }

    function pintarContadores() {
      var cg = conteos(filas, st, 'grupo'), ca = conteos(filas, st, 'alerta');
      [].forEach.call(document.querySelectorAll('[data-rm-grupo]'), function (b) {
        var k = b.dataset.rmGrupo, n = k === '' ? cg.__total__ : (cg[k] || 0);
        var s = b.querySelector('.n'); if (s) s.textContent = n;
        b.classList.toggle('is-active', st.grupo === k);
        b.classList.toggle('is-zero', n === 0);
        b.setAttribute('aria-pressed', st.grupo === k ? 'true' : 'false');
      });
      [].forEach.call(document.querySelectorAll('[data-rm-alerta]'), function (b) {
        var k = b.dataset.rmAlerta, n = k === '' ? ca.__total__ : (ca[k] || 0);
        var s = b.querySelector('.n'); if (s) s.textContent = n;
        b.classList.toggle('is-active', st.alerta === k);
        b.setAttribute('aria-pressed', st.alerta === k ? 'true' : 'false');
      });
      [].forEach.call(document.querySelectorAll('[data-rm-filter]'), function (k) {
        var par = k.dataset.rmFilter.split(':'), on = st[par[0]] === par[1];
        k.classList.toggle('is-active', on);
        k.setAttribute('aria-pressed', on ? 'true' : 'false');
      });
    }

    function render() {
      var vis = filtradas();
      var pg = paginar(vis.length, st.page, st.per);
      st.page = pg.pagina;
      filas.forEach(function (f) { f.el.hidden = true; if (f.det) f.det.hidden = true; });
      vis.slice(pg.desde ? pg.desde - 1 : 0, pg.hasta).forEach(function (f) {
        tbody.appendChild(f.el);
        if (f.det) tbody.appendChild(f.det);
        f.el.hidden = false;
        if (f.det) f.det.hidden = !f.abierto;
        resaltar(f.el);
      });
      if (vacio) vacio.hidden = vis.length > 0 || !filas.length;
      // La tabla trae los últimos 250 retiros: si lo buscado no está, se ofrece
      // buscarlo en TODO el historial (búsqueda del servidor, ?q=) — 2026-09-29.
      var hist = $('rmEmptyHist');
      if (hist) {
        hist.hidden = !(st.q && vis.length === 0);
        if (!hist.hidden) hist.href = '?q=' + encodeURIComponent(st.q);
      }
      pintarContadores();
      var cnt = $('rmCount');
      if (cnt) {
        // El sustantivo va en su propio <span>: en celular se oculta y queda solo el número (CSS).
        cnt.textContent = '';
        cnt.appendChild(document.createTextNode(hayFiltros() ? vis.length + ' de ' + filas.length : String(filas.length)));
        if (!hayFiltros()) cnt.appendChild(el('span', 'rm-count-txt', filas.length === 1 ? ' solicitud' : ' solicitudes'));
      }
      var pie = $('rmFootCount');
      if (pie) {
        pie.textContent = '';
        if (vis.length) {
          pie.appendChild(document.createTextNode('Mostrando '));
          pie.appendChild(el('strong', null, pg.desde + '–' + pg.hasta));
          pie.appendChild(document.createTextNode(' de '));
          pie.appendChild(el('strong', null, String(vis.length)));
          if (hayFiltros()) pie.appendChild(document.createTextNode(' (filtrado de ' + filas.length + ')'));
        } else pie.appendChild(document.createTextNode('Sin resultados'));
      }
      var info = $('rmPageInfo'); if (info) info.textContent = 'Página ' + pg.pagina + ' de ' + pg.paginas;
      var ant = $('rmPrev'), sig = $('rmNext');
      if (ant) ant.disabled = pg.pagina <= 1;
      if (sig) sig.disabled = pg.pagina >= pg.paginas;
      var lim = $('rmClear'); if (lim) lim.hidden = !hayFiltros();
      var x = $('rmSearchClear'); if (x) x.hidden = !st.q;
      [].forEach.call(tabla.querySelectorAll('th.rm-sortable'), function (th) {
        var on = st.sort === th.dataset.sort, ic = th.querySelector('.bi');
        th.classList.toggle('is-asc', on && st.dir === 'asc');
        th.classList.toggle('is-desc', on && st.dir === 'desc');
        th.setAttribute('aria-sort', on ? (st.dir === 'asc' ? 'ascending' : 'descending') : 'none');
        if (ic) ic.className = 'bi ' + (on ? (st.dir === 'asc' ? 'bi-arrow-up' : 'bi-arrow-down') : 'bi-arrow-down-up');
      });
    }

    function fijar(clave, valor) { st[clave] = valor; st.page = 1; render(); }

    // ── Eventos ──
    var caja = $('rmSearch'), t0 = null;
    // En celular el texto guía largo del escritorio se corta ("Solicitud, cliente, l…"): uno corto.
    if (caja && global.matchMedia && global.matchMedia('(max-width:767px)').matches) caja.placeholder = 'Cliente, RUT o solicitud…';
    if (caja) {
      caja.addEventListener('input', function () {
        clearTimeout(t0);
        t0 = setTimeout(function () { st.q = caja.value; st.terms = terminos(caja.value); st.page = 1; render(); }, 60);
      });
      caja.addEventListener('keydown', function (e) {
        if (e.key === 'Escape') { if (caja.value) { caja.value = ''; st.q = ''; st.terms = []; st.page = 1; render(); } else caja.blur(); }
      });
    }
    var x = $('rmSearchClear');
    if (x) x.addEventListener('click', function () { caja.value = ''; st.q = ''; st.terms = []; st.page = 1; render(); caja.focus(); });
    [].forEach.call(document.querySelectorAll('[data-rm-grupo]'), function (b) {
      b.addEventListener('click', function () { fijar('grupo', st.grupo === b.dataset.rmGrupo ? '' : b.dataset.rmGrupo); });
    });
    [].forEach.call(document.querySelectorAll('[data-rm-alerta]'), function (b) {
      b.addEventListener('click', function () { fijar('alerta', st.alerta === b.dataset.rmAlerta ? '' : b.dataset.rmAlerta); });
    });
    var sf = $('rmFecha'), sr = $('rmResp');
    if (sf) sf.addEventListener('change', function () { fijar('fecha', sf.value); });
    if (sr) sr.addEventListener('change', function () { fijar('resp', sr.value); });
    [].forEach.call(document.querySelectorAll('[data-rm-filter]'), function (k) {
      function accionar() {
        var par = k.dataset.rmFilter.split(':');
        fijar(par[0], st[par[0]] === par[1] ? '' : par[1]);
        if (sf && par[0] === 'fecha') sf.value = st.fecha;
        if (raiz && st[par[0]]) { try { raiz.scrollIntoView({ behavior: 'smooth', block: 'start' }); } catch (e) { /* navegador viejo */ } }
      }
      k.addEventListener('click', accionar);
      k.addEventListener('keydown', function (e) { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); accionar(); } });
    });
    var lim = $('rmClear');
    if (lim) lim.addEventListener('click', function () {
      st.q = ''; st.terms = []; st.grupo = ''; st.alerta = ''; st.fecha = ''; st.resp = ''; st.sort = ''; st.dir = 'asc'; st.page = 1;
      if (caja) caja.value = ''; if (sf) sf.value = ''; if (sr) sr.value = '';
      render();
    });
    var vaciar = $('rmEmptyClear'); if (vaciar && lim) vaciar.addEventListener('click', function () { lim.click(); });
    [].forEach.call(tabla.querySelectorAll('th.rm-sortable'), function (th) {
      th.tabIndex = 0;
      function ciclo() {
        var c = th.dataset.sort;
        if (st.sort !== c) { st.sort = c; st.dir = 'asc'; }
        else if (st.dir === 'asc') st.dir = 'desc';
        else { st.sort = ''; st.dir = 'asc'; }
        st.page = 1; render();
      }
      th.addEventListener('click', ciclo);
      th.addEventListener('keydown', function (e) { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); ciclo(); } });
    });
    var per = $('rmPer');
    if (per) {
      per.value = String(st.per);
      per.addEventListener('change', function () {
        st.per = parseInt(per.value, 10) || 10; st.page = 1;
        pref.per = st.per; guardarPref(pref); render();
      });
    }
    var ant = $('rmPrev'), sig = $('rmNext');
    if (ant) ant.addEventListener('click', function () { st.page--; render(); });
    if (sig) sig.addEventListener('click', function () { st.page++; render(); });
    tbody.addEventListener('click', function (e) {
      var b = e.target.closest('.rm-exp');
      if (!b) return;
      e.stopPropagation();
      var f = filas.filter(function (x) { return x.el === b.closest('tr'); })[0];
      if (f) abrir(f, !f.abierto);
    });
    var todo = $('rmExpandAll');
    if (todo) todo.addEventListener('click', function () {
      var vis = filas.filter(function (f) { return !f.el.hidden; });
      var abrirTodo = vis.some(function (f) { return !f.abierto; });
      vis.forEach(function (f) { abrir(f, abrirTodo); });
    });

    // ── Columnas visibles y densidad ──
    function aplicarColumnas() {
      COLS.forEach(function (c) {
        // "Calidad" nace oculta (2026-09-29): solo se ve si el usuario la activó en "Columnas".
        var oculta = c === 'cal' ? !(pref.cols && pref.cols.cal === true)
                                 : !!(pref.cols && pref.cols[c] === false);   // boolean: toggle(clase, undefined) alterna en vez de fijar
        tabla.classList.toggle('rm-hide-' + c, oculta);
        var cb = document.querySelector('input[data-rm-col="' + c + '"]'); if (cb) cb.checked = !oculta;
      });
      if (raiz) raiz.classList.toggle('rm-compacta', !!pref.compacta);
      var d = $('rmDens'); if (d) d.checked = !!pref.compacta;
    }
    [].forEach.call(document.querySelectorAll('input[data-rm-col]'), function (cb) {
      cb.addEventListener('change', function () {
        pref.cols = pref.cols || {}; pref.cols[cb.dataset.rmCol] = cb.checked; guardarPref(pref); aplicarColumnas();
      });
    });
    var dens = $('rmDens');
    if (dens) dens.addEventListener('change', function () { pref.compacta = dens.checked; guardarPref(pref); aplicarColumnas(); });
    var panelCols = $('rmCols');
    document.addEventListener('click', function (e) { if (panelCols && panelCols.open && !panelCols.contains(e.target)) panelCols.open = false; });

    // ── Exportar lo que se está viendo (todas las páginas) ──
    var exp = $('rmExport');
    if (exp) exp.addEventListener('click', function () {
      var datos = filtradas().map(function (f) { return (DATOS[f.id] || {}).csv; }).filter(Boolean);
      if (!datos.length) { toast('No hay filas para exportar', 'warning'); return; }
      var url = URL.createObjectURL(new Blob([aCsv(datos)], { type: 'text/csv;charset=utf-8' }));
      var a = el('a'); a.href = url; a.download = 'retiros_' + (st.hoy || 'monitor') + '.csv';
      document.body.appendChild(a); a.click(); a.remove();
      setTimeout(function () { URL.revokeObjectURL(url); }, 2000);
      toast('Exportadas ' + datos.length + (datos.length === 1 ? ' fila' : ' filas') + ' a Excel (CSV)', 'success');
    });

    // "/" lleva el cursor a la búsqueda
    document.addEventListener('keydown', function (e) {
      if (e.key !== '/' || e.ctrlKey || e.metaKey || e.altKey || !caja) return;
      var t = e.target, tag = t && t.tagName;
      if (tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT' || (t && t.isContentEditable)) return;
      e.preventDefault(); caja.focus(); caja.select();
    });

    aplicarColumnas();
    render();
    iniciarRelojes(tabla);
    // Para revisar la pantalla con más filas sin tocar la base (consola del navegador).
    global.RetirosMonitor = { estado: st, datos: DATOS, render: render, reindexar: function () { leerFilas(); render(); } };
  }

  // ── Reloj en vivo de "Sin responder" (Daniel 2026-09-29: "que tenga un reloj o al menos que avise") ──
  // El servidor entrega el tiempo hábil ya transcurrido y la hora límite (retiros_monitor.py,
  // m_reloj). Aquí avanza cada segundo, pero solo en horario hábil (lun-vie 09-18 hora Chile y
  // día no feriado, igual que el SLA del Centro de control): fuera de horario queda en pausa.
  // Al pasar a rojo con el Monitor abierto: aviso en pantalla y la fila parpadea.
  function horaChile() {
    try {
      var p = {};
      new Intl.DateTimeFormat('en-US', { timeZone: 'America/Santiago', weekday: 'short', hour: 'numeric', hour12: false })
        .formatToParts(new Date()).forEach(function (x) { p[x.type] = x.value; });
      return { dia: p.weekday, hora: parseInt(p.hour, 10) % 24 };
    } catch (e) { return null; }
  }
  function durTxt(s) {
    s = Math.max(0, Math.floor(s));
    var h = Math.floor(s / 3600), m = Math.floor(s % 3600 / 60), x = s % 60;
    return (h < 10 ? '0' : '') + h + ':' + (m < 10 ? '0' : '') + m + ':' + (x < 10 ? '0' : '') + x;
  }
  function restaTxt(s) {
    var min = Math.ceil(s / 60), h = Math.floor(min / 60), m = min % 60;
    return h ? (m ? h + ' h ' + m + ' min' : h + ' h') : m + ' min';
  }
  function iniciarRelojes(tabla) {
    var relojes = [].slice.call(tabla.querySelectorAll('.rm-reloj[data-espera]')).map(function (el) {
      var tr = el.closest('tr'), cod = tr && tr.querySelector('.rm-code');
      return { el: el, base: +el.dataset.espera || 0, ambar: +tabla.dataset.slaAmbar || 7200, rojo: +tabla.dataset.slaRojo || 14400,
               plazo: el.dataset.plazo || '', code: cod ? cod.textContent.trim() : 'Un retiro', nivel: '' };
    });
    if (!relojes.length) return;
    var hoyHabil = tabla.dataset.hoyHabil !== '0';
    var extra = 0, ultimo = Date.now();
    function tic() {
      var ahora = Date.now(), hc = horaChile();
      var enHorario = hoyHabil && hc && hc.dia !== 'Sat' && hc.dia !== 'Sun' && hc.hora >= 9 && hc.hora < 18;
      if (enHorario) extra += (ahora - ultimo) / 1000;
      ultimo = ahora;
      relojes.forEach(function (r) {
        var s = r.base + extra;
        var nivel = s >= r.rojo ? 'rojo' : (s >= r.ambar ? 'ambar' : 'verde');
        var pill = r.el.querySelector('.rm-pill'), t = r.el.querySelector('.rm-reloj-t');
        var barra = r.el.querySelector('.rm-reloj-barra i'), plazo = r.el.querySelector('.rm-reloj-plazo');
        if (t) t.textContent = 'Sin responder · ' + durTxt(s) + (enHorario ? '' : ' · en pausa');
        if (barra) barra.style.width = Math.min(100, s / r.rojo * 100) + '%';
        if (plazo) plazo.textContent = s >= r.rojo
          ? 'Plazo vencido' + (r.plazo ? ' (' + r.plazo + ')' : '') + ': responder ya'
          : 'Quedan ' + restaTxt(r.rojo - s) + ' hábiles' + (r.plazo ? ' · antes de ' + r.plazo : '');
        if (nivel !== r.nivel) {
          ['verde', 'ambar', 'rojo'].forEach(function (n) {
            r.el.classList.toggle(n, n === nivel);
            if (pill) pill.classList.toggle(n, n === nivel);
          });
          var tr = r.el.closest('tr');
          if (tr) tr.dataset.alerta = nivel;
          // Solo avisa al CRUZAR a rojo con la pantalla abierta (no al cargar).
          if (nivel === 'rojo' && r.nivel && r.nivel !== 'rojo') {
            r.el.classList.add('rm-reloj-alerta');
            toast('⏱ ' + r.code + ' superó el plazo de respuesta (' + Math.round(r.rojo / 3600) + ' h hábiles sin responder)', 'error');
          }
          r.nivel = nivel;
        }
      });
    }
    tic();
    setInterval(tic, 1000);
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', iniciar);
  else iniciar();
})(typeof window !== 'undefined' ? window : globalThis);

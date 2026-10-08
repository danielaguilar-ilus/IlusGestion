/* Bandeja «Regularizar» (Daniel 2026-10-07): las OT que no pasan la puerta del documento. Paginada en el servidor
   (REGLA #4.3: Mostrando A–B de N, N por página, Anterior / Página X de Y / Siguiente). Cada fila: sus documentos
   completos, el semáforo (sin documento / $0 sin autorizar / nota de venta sin factura) y las dos salidas:
   ligar el documento (mismo diálogo del motor, POST /ot/api/<vid>/documentos) o pedir la autorización de Daniel.
   En OT cerradas solo se regulariza el documento / la plata: nunca estado, firmas ni fechas (OT = evidencia).
   Datos: GET /ot/api/regularizar. Requiere static/ot_fin_motor.js (los diálogos). */
(function () {
  'use strict';
  var raiz = document.getElementById('ppReg');
  if (!raiz) return;
  var esSA = raiz.getAttribute('data-sa') === '1';
  var CENTROS = [];
  try { CENTROS = JSON.parse(raiz.getAttribute('data-centros') || '[]'); } catch (e) { CENTROS = []; }
  var f = { estado: '', falta: '', cliente_q: '', creador: '', mes: '' }, page = 1, porPag = 50;

  function esc(s) {
    return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
    });
  }
  function qs() {
    var p = ['page=' + page, 'per_page=' + porPag];
    Object.keys(f).forEach(function (k) { if (f[k]) p.push(k + '=' + encodeURIComponent(f[k])); });
    return p.join('&');
  }
  function api(url) {
    return fetch(url, { credentials: 'same-origin' }).then(function (r) {
      return r.json().catch(function () { return null; }).then(function (j) { return { ok: r.ok && !(j && j.ok === false), j: j || {} }; });
    }).catch(function () { return { ok: false, j: { error: 'No hay conexión con el servidor.' } }; });
  }

  function docs(it) {
    if (!it.documentos.length) return '<div class="pp-docs"><span class="pp-doc ninguno">Sin documento de Random</span></div>';
    return '<div class="pp-docs">' + it.documentos.map(function (d) {
      var nv = ['NVV', 'NVI', 'VD', 'WEB'].indexOf(d.tido) >= 0;
      return '<span class="pp-doc ' + (nv ? 'nv' : 'fv') + '">' + esc(d.tido) + ' ' + esc(d.nudo) + (nv ? ' · falta la factura' : '') + '</span>';
    }).join('') + '</div>';
  }
  function fila(it) {
    var pend = it.solicitud_pendiente;
    var acc = '<button type="button" class="pp-btn pri" data-lig="' + it.id + '"><i class="bi bi-link-45deg"></i> Ligar documento</button>';
    if (pend) {
      acc += '<a class="pp-btn" href="/ot/autorizaciones/' + pend.id + '"><i class="bi bi-hourglass-split"></i> Esperando a Daniel</a>';
    } else {
      acc += '<button type="button" class="pp-btn" data-aut="' + it.id + '|cerrar_sin_documento"><i class="bi bi-shield-lock"></i> ' + (esSA ? 'Autorizar sin documento' : 'Pedir autorización') + '</button>' +
        '<button type="button" class="pp-btn" data-aut="' + it.id + '|cobro_cero"><i class="bi bi-shield-check"></i> Es un $0</button>';
    }
    return '<tr><td data-k="OT"><a class="ot" href="' + esc(it.url) + '">' + esc(it.numero_ot) + '</a>' +
      '<small>' + esc(it.fecha || 'Sin fecha') + (it.cerrada ? ' · cerrada: solo se regulariza la plata y el documento' : '') + '</small></td>' +
      '<td data-k="Cliente">' + esc(it.cliente) + '<small>Creada por ' + esc(it.creado_por || '—') + (it.creada_el ? ' · ' + esc(it.creada_el) : '') + '</small></td>' +
      '<td data-k="Estado">' + esc(it.estado_txt) + '<small>' + esc(it.cobertura || '') + '</small></td>' +
      '<td data-k="Qué falta"><span class="pp-sem ' + esc(it.falta) + '">' + esc(it.falta_txt) + '</span><small>' + esc(it.mensaje) + '</small></td>' +
      '<td data-k="Documentos">' + docs(it) + '</td>' +
      '<td data-k="Acciones"><div class="acc">' + acc + '</div></td></tr>';
  }

  function pintar(j) {
    var r = j.resumen || {};
    var chips = '<button type="button" class="pp-chip' + (!f.falta ? ' on' : '') + '" data-falta="">Todas <b>' + (j.total_global != null ? j.total_global : j.total) + '</b></button>' +
      Object.keys(j.faltas || {}).map(function (k) {
        return '<button type="button" class="pp-chip' + (f.falta === k ? ' on' : '') + '" data-falta="' + k + '">' + esc(j.faltas[k]) + ' <b>' + (r[k] || 0) + '</b></button>';
      }).join('');
    var estados = '<option value="">Todos los estados</option>' + Object.keys(j.estados || {}).map(function (k) {
      return '<option value="' + k + '"' + (f.estado === k ? ' selected' : '') + '>' + esc(j.estados[k]) + '</option>';
    }).join('');
    var barra = '<div class="pp-bar">' +
      '<input id="ppCli" type="search" placeholder="Cliente…" value="' + esc(f.cliente_q) + '" aria-label="Cliente">' +
      '<input id="ppCre" type="search" placeholder="Creada por…" value="' + esc(f.creador) + '" aria-label="Creada por">' +
      '<input id="ppMes" type="month" value="' + esc(f.mes) + '" aria-label="Mes">' +
      '<select id="ppEst" aria-label="Estado">' + estados + '</select>' +
      '<button type="button" class="pp-btn pri" data-buscar="1"><i class="bi bi-search"></i> Filtrar</button>' +
      '<button type="button" class="pp-btn" data-limpiar="1">Limpiar</button></div>';
    var cuerpo = j.items.length
      ? '<table class="pp-rt"><thead><tr><th>OT</th><th>Cliente</th><th>Estado</th><th>Qué falta</th><th>Documentos</th><th>Acciones</th></tr></thead><tbody>' + j.items.map(fila).join('') + '</tbody></table>'
      : '<div class="pp-vacio"><i class="bi bi-check2-circle" style="font-size:1.8rem;color:#16a34a"></i><br>No hay OT por regularizar con este filtro.</div>';
    var pie = '<div class="pp-pie"><span class="t">Mostrando ' + j.desde + '–' + j.hasta + ' de ' + j.total + (j.truncado ? ' (se muestran las 2.000 más recientes)' : '') + '</span>' +
      '<label class="t">Por página <select id="ppPor">' + [25, 50, 100, 200].map(function (n) { return '<option' + (n === porPag ? ' selected' : '') + '>' + n + '</option>'; }).join('') + '</select></label>' +
      '<span><button type="button" class="pp-btn" data-pag="' + (j.page - 1) + '"' + (j.page <= 1 ? ' disabled' : '') + '>Anterior</button> ' +
      '<span class="t">Página ' + j.page + ' de ' + j.pages + '</span> ' +
      '<button type="button" class="pp-btn" data-pag="' + (j.page + 1) + '"' + (j.page >= j.pages ? ' disabled' : '') + '>Siguiente</button></span></div>';
    raiz.innerHTML = '<div class="pp-chips">' + chips + '</div>' + barra + cuerpo + pie;
  }

  var totalGlobal = null;
  function cargar() {
    raiz.classList.add('cargando');
    return api('/ot/api/regularizar?' + qs()).then(function (r) {
      raiz.classList.remove('cargando');
      if (!r.ok) { raiz.innerHTML = '<div class="pp-error">' + esc(r.j.error || 'No se pudo leer la bandeja.') + '</div>'; return; }
      if (!f.falta && !f.estado && !f.cliente_q && !f.creador && !f.mes) totalGlobal = r.j.total;
      r.j.total_global = totalGlobal;
      pintar(r.j);
    });
  }
  function leerFiltros() {
    var g = function (id) { var e = document.getElementById(id); return e ? e.value.trim() : ''; };
    f.cliente_q = g('ppCli'); f.creador = g('ppCre'); f.mes = g('ppMes'); f.estado = g('ppEst');
  }

  raiz.addEventListener('click', function (e) {
    var t;
    if ((t = e.target.closest('[data-falta]'))) { leerFiltros(); f.falta = t.getAttribute('data-falta'); page = 1; cargar(); }
    else if ((t = e.target.closest('[data-pag]'))) { page = parseInt(t.getAttribute('data-pag'), 10) || 1; cargar(); }
    else if (e.target.closest('[data-buscar]')) { leerFiltros(); page = 1; cargar(); }
    else if (e.target.closest('[data-limpiar]')) { f = { estado: '', falta: '', cliente_q: '', creador: '', mes: '' }; page = 1; cargar(); }
    else if ((t = e.target.closest('[data-lig]'))) {
      window.OTFinMotor.ligarDocumento(parseInt(t.getAttribute('data-lig'), 10), { hecho: cargar });
    } else if ((t = e.target.closest('[data-aut]'))) {
      var p = t.getAttribute('data-aut').split('|');
      window.OTFinMotor.pedirAutorizacion(parseInt(p[0], 10), p[1], { superadmin: esSA, opciones: CENTROS, hecho: cargar });
    }
  });
  raiz.addEventListener('change', function (e) {
    if (e.target.id === 'ppPor') { porPag = parseInt(e.target.value, 10) || 50; page = 1; leerFiltros(); cargar(); }
  });
  raiz.addEventListener('keydown', function (e) {
    if (e.key === 'Enter' && e.target.closest('.pp-bar')) { leerFiltros(); page = 1; cargar(); }
  });
  cargar();
})();

/* Guía del retiro (Daniel 2026-10-02) — ver templates/retiros/_guia.html y retiros_guia.py.
   Dibuja los 6 pasos con lo hecho, lo que te toca (en ROJO) y lo que falta; marca en rojo las
   tarjetas y botones pendientes de la ficha; avisa EXACTAMENTE qué falta antes de enviar algo que
   le escribe al cliente; muestra lo que Check (bodega) dice de la preparación.
   Seguridad: «Confirmo» solo deja un registro interno; nada de aquí escribe al cliente. Check solo se consulta. */
(function () {
  'use strict';
  var panel = document.getElementById('gpPanel');
  var dataEl = document.getElementById('gpData');
  if (!panel || !dataEl) return;
  var RID = panel.dataset.rid;
  var STATUS = panel.dataset.status || '';
  var G;
  try { G = JSON.parse(dataEl.textContent); } catch (e) { return; }
  var CHECK = null;          // última respuesta de /check-preparacion
  var CHECK_TS = null;
  var CHECK_CARGANDO = false;
  var ACT = null;            // última respuesta de /check-actividad (OT de Check: quién, cuándo, asignación)
  var ACT_CARGANDO = false;
  var ACT_REINTENTOS = 0;
  var AVISO_ABIERTO = false;
  var ULTIMO_DIBUJO = '';

  var CARDS = { 1: '#paso-2', 2: '#paso-resp', 3: '#paso-3', 4: '#paso-4', 5: '#paso-confirmacion', 6: '#paso-confirmacion' };
  var BTNS = { 2: '#btnTomarRetiro', 4: '#btnAbrirProponerFecha', 5: '#formEnviarPreparacion .cierre-next-btn', 6: '.cierre-next-btn.is-green' };
  var GUARDS = [
    { sel: '#btnAbrirProponerFecha', n: 4 },
    { sel: '#formEnviarPreparacion .cierre-next-btn', n: 5 },
    { sel: '.cierre-next-btn.is-green', n: 6 }
  ];
  var ESTADO_TXT = {
    hecho: ['b-hecho', 'bi-check-circle-fill', 'Hecho'],
    actual: ['b-actual', 'bi-exclamation-circle-fill', 'Te toca'],
    espera: ['b-espera', 'bi-hourglass-split', 'Esperando'],
    pendiente: ['b-pendiente', 'bi-lock-fill', 'Aún no toca'],
    bloqueado: ['b-bloqueado', 'bi-slash-circle-fill', 'No se puede todavía']
  };

  var MSG_RESP = 'Primero declara quién se hace cargo de este retiro (paso 2). Sin responsable no se avanza ni se agenda o libera el calendario.';

  function esc(s) {
    return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
    });
  }
  function $(sel, root) { return (root || document).querySelector(sel); }
  function toast(msg, tipo) { if (typeof window.ilusToast === 'function') window.ilusToast(msg, { type: tipo || 'info' }); }
  function visible(el) { return !!(el && (el.offsetWidth || el.offsetHeight || el.getClientRects().length)); }

  // ── Qué falta antes de un paso ──────────────────────────────────────────
  function faltantesAntes(n) {
    var lista = (G.previos && G.previos[String(n)]) || [];
    return lista.map(function (par) {
      var p = G.pasos[par[0] - 1];
      return { n: par[0], titulo: p.titulo, texto: (par[1] && par[1][0]) || ('Falta terminar el paso ' + par[0] + '.'), estado: p.estado };
    });
  }
  function anclaDe(p) {
    if (p.ancla && $(p.ancla)) return p.ancla;
    return CARDS[p.n] || p.ancla;
  }
  function irA(sel, resaltar) {
    var el = $(sel);
    if (!el) return false;
    el.scrollIntoView({ behavior: 'smooth', block: 'start' });
    if (resaltar !== false) {
      el.classList.remove('gp-resalta'); void el.offsetWidth; el.classList.add('gp-resalta');
      setTimeout(function () { el.classList.remove('gp-resalta'); }, 1800);
    }
    return true;
  }
  // Navegar a un paso: desplaza y resalta; si aún no existe esa tarjeta, lo dice (nada de clics mudos).
  function irAlPaso(n, conAviso) {
    var p = G.pasos[n - 1];
    var ant = conAviso ? faltantesAntes(n) : [];
    if (!irA(anclaDe(p))) {
      var motivo = (p.faltan && p.faltan[0]) || 'Esa parte de la ficha aparece cuando se termina el paso anterior.';
      toast('Todavía no está disponible: ' + motivo, 'warning');
      if (ant.length) irA(anclaDe(G.pasos[ant[0].n - 1]));
      return;
    }
    if (ant.length && p.estado !== 'hecho') {
      toast('Ojo, antes falta el paso ' + ant[0].n + ' (' + ant[0].titulo + '): ' + ant[0].texto, 'warning');
    }
  }

  // ── Aviso «antes falta…» (solo para los botones que le escriben al cliente) ──────────────
  function avisoFalta(n, items, duro, sinPropios) {
    return new Promise(function (resolve) {
      if (AVISO_ABIERTO) { resolve(null); return; }
      AVISO_ABIERTO = true;
      var p = G.pasos[n - 1];
      var el = document.createElement('div');
      el.className = 'modal fade gp-modal';
      el.tabIndex = -1;
      el.setAttribute('aria-hidden', 'true');
      var propios = (duro && !sinPropios) ? p.faltan : [];
      var lista = items.map(function (it) {
        return '<li><span class="n">' + it.n + '</span><div><b>Paso ' + it.n + ' · ' + esc(it.titulo) + '</b><span>' + esc(it.texto) + '</span></div></li>';
      }).join('') + propios.map(function (t) {
        return '<li><span class="n"><i class="bi bi-x-lg"></i></span><div><b>Paso ' + n + ' · ' + esc(p.titulo) + '</b><span>' + esc(t) + '</span></div></li>';
      }).join('');
      var primero = items.length ? items[0].n : n;
      el.innerHTML =
        '<div class="modal-dialog modal-dialog-centered"><div class="modal-content">' +
        '<div class="modal-header"><h5 class="modal-title"><i class="bi bi-exclamation-octagon-fill text-danger me-2"></i>' +
        (duro ? 'Todavía no se puede' : 'Antes del paso ' + n + ' falta:') + '</h5>' +
        '<button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Cerrar"></button></div>' +
        '<div class="modal-body"><p class="mb-3" style="font-size:1.05rem">' +
        (duro ? 'Este paso no se puede hacer hasta resolver:' : 'Para que el retiro salga ordenado, primero termina esto:') +
        '</p><ol class="gp-modal-lista">' + lista + '</ol></div>' +
        '<div class="modal-footer">' +
        '<button type="button" class="gp-btn" data-r="ir"><i class="bi bi-arrow-left-circle-fill"></i>' +
        (items.length ? 'Ir al paso ' + primero : 'Entendido') + '</button>' +
        (duro ? '' : '<button type="button" class="gp-btn sec" data-r="igual">Continuar de todos modos</button>') +
        '</div></div></div>';
      document.body.appendChild(el);
      var res = null;
      var m = window.bootstrap ? window.bootstrap.Modal.getOrCreateInstance(el) : null;
      var cerrar = function () { AVISO_ABIERTO = false; el.remove(); resolve(res); };
      el.addEventListener('click', function (ev) {
        var b = ev.target.closest && ev.target.closest('[data-r]');
        if (!b) return;
        res = b.dataset.r;
        if (m) m.hide(); else cerrar();
      });
      el.addEventListener('hidden.bs.modal', cerrar);
      if (m) m.show(); else { el.style.display = 'block'; el.classList.add('show'); }
    });
  }
  // Sin responsable declarado no se avanza: aviso DURO (sin «continuar de todos modos») que lleva directo a «Me hago cargo»
  function avisoResponsable(n) {
    var p2 = G.pasos[1];
    avisoFalta(n, [{ n: 2, titulo: p2.titulo, texto: (p2.faltan && p2.faltan[0]) || 'Declara quién se hace cargo de este retiro.', estado: 'actual' }], true, true)
      .then(function (r) { if (r === 'ir') irAlPaso(2, false); });
  }
  function guardar(n, hacer) {
    var p = G.pasos[n - 1];
    var ant = faltantesAntes(n).filter(function (it) { return it.n !== n; });
    var duro = !!(p.bloquea && p.faltan.length);
    if (!ant.length && !duro) { hacer(); return; }
    avisoFalta(n, ant, duro).then(function (r) {
      if (r === 'igual' && !duro) hacer();
      else if (r === 'ir' && ant.length) irAlPaso(ant[0].n, false);
    });
  }
  // Intercepta (en captura) los botones que más importan para avisar antes, sin tocar sus funciones.
  window.addEventListener('click', function (ev) {
    if (!ev.target || !ev.target.closest) return;
    for (var i = 0; i < GUARDS.length; i++) {
      var g = GUARDS[i];
      var b = ev.target.closest(g.sel);
      if (!b) continue;
      if (b.dataset.gpOk === '1') return;
      if (G.sin_responsable) {            // Daniel 2026-10-02: «para avanzar debe declarar el responsable, y para agendar o liberar el calendario»
        ev.preventDefault();
        ev.stopImmediatePropagation();
        avisoResponsable(g.n);
        return;
      }
      var p = G.pasos[g.n - 1];
      var ant = faltantesAntes(g.n);
      if (!ant.length && !(p.bloquea && p.faltan.length)) return;
      ev.preventDefault();
      ev.stopImmediatePropagation();
      guardar(g.n, function () {
        b.dataset.gpOk = '1';
        b.click();
        setTimeout(function () { delete b.dataset.gpOk; }, 800);
      });
      return;
    }
  }, true);

  // ── Dibujo ───────────────────────────────────────────────────────────────
  // La barra es corta a propósito (como la cabecera del modal «Nuevo retiro interno»): progreso, los 6 pasos y
  // el siguiente. El detalle de cada paso vive en SU tarjeta de la ficha, bordeada en rojo si te toca.
  function botones(p) {
    if (!p.accion) return '';
    var a = p.accion;
    var cls = 'gp-btn' + (p.estado === 'actual' ? '' : ' sec') + (a.tipo === 'retirar' && p.estado === 'actual' ? ' verde' : '');
    var dis = a.deshabilitada ? ' disabled title="' + esc(a.motivo || 'Primero confirma las facturas (paso 1)') + '"' : '';
    var icono = { confirmar_docs: 'bi-patch-check-fill', confirmar_productos: 'bi-patch-check-fill', tomar: 'bi-person-raised-hand',
      proponer: 'bi-calendar2-plus', preparacion: 'bi-box-seam-fill', retirar: 'bi-check-circle-fill', ir: 'bi-arrow-right-circle-fill' }[a.tipo] || 'bi-arrow-right-circle-fill';
    return '<button type="button" class="' + cls + '" data-gp-acc="' + p.n + '"' + dis + '><i class="bi ' + icono + '"></i>' + esc(a.texto) + '</button>';
  }
  // Un paso del recorrido: círculo conectado al anterior (verde = hecho, rojo que late = te toca, ámbar = esperando, gris = aún no)
  function nodoHtml(p, i) {
    var st = ESTADO_TXT[p.estado] || ESTADO_TXT.pendiente;
    var ant = i > 0 ? G.pasos[i - 1] : null;
    var cont = p.estado === 'hecho' ? '<i class="bi bi-check-lg"></i>' : (p.estado === 'espera' ? '<i class="bi bi-hourglass-split"></i>' :
      (p.estado === 'bloqueado' ? '<i class="bi bi-lock-fill"></i>' : String(p.n)));
    return '<li class="gp-n is-' + p.estado + (ant && ant.estado === 'hecho' ? ' tras-hecho' : '') + (p.n === G.siguiente ? ' es-sig' : '') + '">' +
      '<button type="button" class="gp-node" data-gp-ver="' + p.n + '" title="' + esc(p.pregunta) + '" aria-label="Paso ' + p.n + ', ' + esc(p.titulo) + ': ' + st[2] + '">' +
      '<span class="gp-nc">' + cont + '</span><span class="gp-nt">' + esc(p.titulo) + '<small>' + st[2] + '</small></span></button></li>';
  }
  // La tarea que toca, con su botón, SIEMPRE a la vista (Daniel 2026-10-02: «que avisara arriba… y tuviera la acción, un botón»)
  function stripHtml() {
    if (G.terminal) {
      return '<div class="gp-strip is-pendiente" aria-live="polite"><span class="gp-strip-ico"><i class="bi bi-flag-fill"></i></span>' +
        '<div class="gp-strip-t"><span class="gp-strip-k">Retiro terminado</span><b>' + esc(G.terminal) + ' No queda nada por hacer.</b></div></div>';
    }
    if (!G.siguiente) {
      return '<div class="gp-strip is-fin" aria-live="polite"><span class="gp-strip-ico"><i class="bi bi-trophy-fill"></i></span>' +
        '<div class="gp-strip-t"><span class="gp-strip-k">Todo listo</span><b>¡Todos los pasos están hechos! El cliente ya se llevó su pedido.</b></div></div>';
    }
    var p = G.pasos[G.siguiente - 1];
    var k = { actual: ['Te toca', 'bi-exclamation-lg'], espera: ['Ahora toca esperar', 'bi-hourglass-split'],
      pendiente: ['Siguiente (aún no toca)', 'bi-lock-fill'], bloqueado: ['No se puede avanzar todavía', 'bi-slash-circle-fill'] }[p.estado] || ['Siguiente paso', 'bi-signpost-2-fill'];
    var h = '<div class="gp-strip is-' + p.estado + '" aria-live="polite"><span class="gp-strip-ico"><i class="bi ' + k[1] + '"></i></span>' +
      '<div class="gp-strip-t"><span class="gp-strip-k">' + k[0] + ' · Paso ' + p.n + ' · ' + esc(p.titulo) + '</span><b>' + esc(p.faltan[0] || p.pregunta) + '</b>';
    if (p.avisos && p.avisos[0]) h += '<span class="gp-strip-av"><i class="bi bi-exclamation-triangle-fill"></i> ' + esc(p.avisos[0]) + '</span>';
    h += '</div>';
    if (p.correo && p.estado === 'actual') h += '<span class="gp-correo"><i class="bi bi-envelope-fill"></i>Le llega un correo al cliente</span>';
    var b = botones(p);
    if (!b && p.estado !== 'hecho') b = '<button type="button" class="gp-btn sec" data-gp-ver="' + p.n + '"><i class="bi bi-arrow-right-circle"></i>Ir a este paso</button>';
    if (b) h += '<div class="gp-strip-acc">' + b + '</div>';
    return h + '<span class="gp-tecla" title="Te lleva al siguiente pendiente, por orden de tarjeta"><kbd>Enter ↵</kbd><span>siguiente pendiente</span></span></div>';
  }
  // ── Multi-documento: un semáforo por factura o boleta del retiro ────────────────────────────────────────────
  function claveDoc(r) {              // 'BLV 0000023732' y 'BLV 23732' son el mismo documento
    var m = /^\s*([A-Za-z]{2,5})[\s\-_.\/:]*0*([0-9]+)/.exec(String(r || ''));
    return m ? m[1].toUpperCase() + ' ' + m[2] : String(r || '').replace(/\s+/g, ' ').trim().toUpperCase();
  }
  function articuloDoc(clave) {       // la tabla de productos de ESA factura
    var arts = document.querySelectorAll('#paso-3 .pd-doc');
    for (var i = 0; i < arts.length; i++) { if (claveDoc(textoDe($('.pd-doc-num', arts[i]))) === clave) return arts[i]; }
    return null;
  }
  function filaDoc(clave) {           // su fila en la tabla de facturas del paso 1
    var filas = document.querySelectorAll('#tabDocsAsociados tbody tr[data-doc-id]');
    for (var i = 0; i < filas.length; i++) {
      if (claveDoc(textoDe($('.td-pill-dark', filas[i])) + ' ' + textoDe($('td.mono', filas[i]))) === clave) return filas[i];
    }
    return null;
  }
  var ORDEN_NIVEL = { neutro: 0, ok: 1, aviso: 2, mal: 3 };
  function estadoDoc(d) {
    var clave = claveDoc(d.rotulo), nivel = 'neutro', mal = [], aviso = [], info = [];
    function subir(n) { if (ORDEN_NIVEL[n] > ORDEN_NIVEL[nivel]) nivel = n; }
    if (d.con_saldo === 1) { subir('ok'); info.push('con saldo'); }
    else if (d.con_saldo === 0) { subir('aviso'); aviso.push('sin saldo'); }
    else info.push('saldo sin verificar');
    if (d.otro_rut) { subir('aviso'); aviso.push('otro RUT'); }
    var art = articuloDoc(clave);
    if (art) {
      var rojos = art.querySelectorAll('.pd-row.t-rojo').length, ambar = art.querySelectorAll('.pd-row.t-ambar').length;
      if (rojos) { subir('mal'); mal.push(rojos + (rojos === 1 ? ' producto con problema' : ' productos con problema')); }
      if (ambar) { subir('aviso'); aviso.push(ambar + ' por revisar'); }
    }
    var cd = null;
    if (CHECK && CHECK.documentos && (STATUS === 'agenda_confirmada' || STATUS === 'en_preparacion')) {
      CHECK.documentos.forEach(function (x) { if (claveDoc(x.rotulo) === clave) cd = x; });
    }
    if (cd) info.push(cd.estado === 'listo' ? 'Check: listo' : (cd.estado === 'sin_datos' ? 'Check aún no lo tiene' : 'Check: en proceso'));
    return { clave: clave, nivel: nivel, partes: mal.concat(aviso, info) };
  }
  function docsHtml() {
    var docs = G.docs || [];
    if (docs.length < 2) return '';
    var MAX = 6;
    var h = '<div class="gp-docs" role="list" aria-label="Documentos de este retiro"><span class="gp-docs-k"><i class="bi bi-files"></i>' + docs.length + ' documentos</span>';
    docs.slice(0, MAX).forEach(function (d) {
      var e = estadoDoc(d);
      var icono = { ok: 'bi-check-circle-fill', aviso: 'bi-exclamation-triangle-fill', mal: 'bi-x-octagon-fill', neutro: 'bi-dash-circle' }[e.nivel];
      h += '<button type="button" role="listitem" class="gp-doc is-' + e.nivel + '" data-gp-doc="' + esc(e.clave) + '" title="' + esc(e.clave + (e.partes.length ? ': ' + e.partes.join(' · ') : '')) +
        '"><i class="bi ' + icono + '"></i><b>' + esc(e.clave) + '</b>' + (e.partes.length ? '<small>' + esc(e.partes[0]) + '</small>' : '') + '</button>';
    });
    if (docs.length > MAX) h += '<button type="button" class="gp-doc is-neutro" data-gp-doc="*" title="Ver todos los documentos"><b>+' + (docs.length - MAX) + ' más</b></button>';
    return h + '</div>';
  }
  function irADoc(clave) {
    var el = clave === '*' ? $('#tabDocsAsociados') : (articuloDoc(clave) || filaDoc(clave) || $('#paso-3'));
    if (!el) return;
    el.scrollIntoView({ behavior: 'smooth', block: 'center' });
    el.classList.remove('gp-resalta'); void el.offsetWidth; el.classList.add('gp-resalta');
    setTimeout(function () { el.classList.remove('gp-resalta'); }, 1800);
    el.setAttribute('tabindex', '-1');
    try { el.focus({ preventScroll: true }); } catch (e) { /* queda resaltado igual */ }
  }
  // «hace 12 s» / «hace 3 min» / «hace 2 h»
  function haceTxt(s) {
    s = Math.max(0, parseInt(s, 10) || 0);
    if (s < 60) return s + ' s';
    if (s < 3600) return Math.round(s / 60) + ' min';
    return Math.round(s / 3600) + ' h';
  }
  // ¿Funciona la conexión con Check? (Daniel 2026-10-02: «que me diga si está funcionando la conexión o no»)
  function conexionHtml() {
    var c = CHECK && CHECK.conexion;
    if (!c) return '';
    var t = {
      ok: ['ok', 'Conectado a Check', 'Conexión con Check funcionando'],
      parcial: ['aviso', 'Check responde a medias', 'Check responde solo por ' + c.docs_ok + ' de ' + c.docs_total + ' documentos'],
      sin_conexion: ['mal', 'Sin conexión con Check', 'Check no respondió'],
      sin_credenciales: ['mal', 'Check no está configurado', 'Este sistema no tiene las credenciales de Check'],
      sin_documentos: ['neutro', 'Sin documentos', 'No hay documentos que consultar en Check']
    }[c.estado] || ['neutro', 'Estado desconocido', 'No se pudo saber el estado de la conexión'];
    var hace = ((c.estado === 'ok' || c.estado === 'parcial') && c.leido_hace_s != null) ? '<small>leído hace ' + esc(haceTxt(c.leido_hace_s)) + '</small>' : '';
    return '<span class="gp-conex is-' + t[0] + '" role="status" title="' + esc(t[2]) + '"><i class="gp-conex-dot"></i><b>' + esc(t[1]) + '</b>' + hace + '</span>';
  }
  // Iniciales y color estable para el avatar de cada persona que nombra Check
  function iniciales(n) {
    var p = String(n || '').replace(/[^A-Za-zÁÉÍÓÚÑáéíóúñ0-9 ._-]/g, ' ').split(/[\s._-]+/).filter(Boolean);
    if (!p.length) return '?';
    return (p.length === 1 ? p[0].slice(0, 2) : p[0].charAt(0) + p[1].charAt(0)).toUpperCase();
  }
  var COLORES_AV = ['#0a0a0a', '#dc2626', '#2563eb', '#15803d', '#b45309'];
  function colorAv(n) {
    var h = 0, s = String(n || '');
    for (var i = 0; i < s.length; i++) h = (h * 31 + s.charCodeAt(i)) >>> 0;
    return COLORES_AV[h % COLORES_AV.length];
  }
  function partesMomento(v) {            // 'dd/mm/aaaa hh:mm' → fecha y hora por separado
    var m = /^(\d{2}\/\d{2}\/\d{4})(?:\s+(\d{2}:\d{2}))?$/.exec(String(v || '').trim());
    return m ? { f: m[1], h: m[2] || '' } : { f: String(v || ''), h: '' };
  }
  // Una OT de Check en una tarjeta COMPACTA (Daniel 2026-10-06: «tarjetas más chicas, con menos scroll, mejor distribuidas… resaltar el número de OT
  // para que se entienda qué interesa a la hora de buscar información en Check»): N° de OT grande y copiable, estado, tipo, quién y cuándo en una línea.
  function cuandoTxt(momentos) {
    var ms = (momentos || []).map(function (m) { var pm = partesMomento(m.valor); return { e: m.etiqueta, f: pm.f, h: pm.h }; });
    if (!ms.length) return '';
    var unaFecha = ms.every(function (m) { return m.f === ms[0].f && m.h; });
    if (unaFecha) {
      return '<span class="ck-otc-f">' + esc(ms[0].f) + '</span>' +
        ms.map(function (m) { return '<span>' + esc(m.e) + ' <b>' + esc(m.h) + '</b></span>'; }).join('<i class="bi bi-arrow-right"></i>');
    }
    return ms.map(function (m) { return '<span>' + esc(m.e) + ' <b>' + esc(m.f + (m.h ? ' ' + m.h : '')) + '</b></span>'; }).join('<i class="bi bi-arrow-right"></i>');
  }
  function otHtml(o, clave) {
    var est = String(o.estado || '').toLowerCase();
    var cls = /termin|final|cerr|complet|ejecut|ok/.test(est) ? 'ok' : (/anul|cancel|error|rechaz/.test(est) ? 'mal' : 'en');
    var h = '<article class="ck-otc is-' + cls + '"><div class="ck-otc-top">' +
      '<span class="ck-otn" title="Número de la OT: con él se busca en Check"><small>N° OT</small><b>' + esc(o.ot || 's/n') + '</b>' +
      (o.ot ? '<button type="button" class="ck-copiar" data-copiar="' + esc(o.ot) + '" title="Copiar el N° de OT para buscarlo en Check" aria-label="Copiar N° de OT ' + esc(o.ot) + '"><i class="bi bi-copy"></i></button>' : '') +
      '</span>' + (o.estado ? '<span class="ck-est is-' + cls + '">' + esc(o.estado) + '</span>' : '') + '</div>' +
      '<div class="ck-otc-meta">' + (o.tipo ? '<b>' + esc(o.tipo) + '</b> · ' : '') + o.n_lineas + (o.n_lineas === 1 ? ' línea' : ' líneas') +
      (o.unidades ? ' · ' + o.unidades + (o.unidades === 1 ? ' unidad' : ' unidades') : '') + '</div>';
    if ((o.personas || []).length) {
      h += '<div class="ck-otc-q">' + o.personas.map(function (p) {
        return '<span class="ck-otc-per"><span class="ck-av" style="background:' + colorAv(p.valor) + '" aria-hidden="true">' + esc(iniciales(p.valor)) + '</span>' +
          '<b>' + esc(p.valor) + '</b><small>' + esc(p.etiqueta) + '</small></span>';
      }).join('') + '</div>';
    } else {
      h += '<div class="ck-otc-q ck-otc-nada"><i class="bi bi-info-circle"></i>Check no informa quién</div>';
    }
    var c = cuandoTxt(o.momentos);
    h += '<div class="ck-otc-c"><i class="bi bi-clock-history"></i>' + (c || 'Check no informa fecha ni hora') + '</div>';
    if (o.datos && o.datos.length) {
      h += '<details class="ck-raw" data-k="' + clave + '"><summary><i class="bi bi-braces"></i>Todos los datos de Check (' + o.n_datos + ')</summary><dl>' +
        o.datos.map(function (d) { return '<dt>' + esc(d.etiqueta) + '</dt><dd>' + esc(d.valor) + '</dd>'; }).join('') + '</dl></details>';
    }
    return h + '</article>';
  }
  // Quién pickeó / responsable / cuándo / a quién se asignó, según los movimientos (OT) de Check
  function actHtml() {
    var h = '<section class="ck-act"><div class="ck-act-h"><i class="bi bi-person-badge-fill"></i><div><b>Quién y cuándo</b><small>Movimientos (OT) que Check registra de este pedido</small></div></div>';
    var msg = function (icono, txt, spin) {
      return h + '<p class="ck-vacio">' + (spin ? '<span class="spinner-border spinner-border-sm"></span>' : '<i class="bi ' + icono + '"></i>') + txt + '</p></section>';
    };
    if (!ACT) return msg('', 'Buscando las OT de este pedido en Check…', true);
    if (ACT.estado === 'cargando') return msg('', 'Check está entregando sus movimientos; la primera vez puede tardar hasta 40 segundos…', true);
    if (ACT.estado === 'sin_credenciales') return msg('bi-key-fill', 'Este sistema no tiene las credenciales de Check cargadas.');
    if (ACT.estado !== 'listo') return msg('bi-exclamation-triangle-fill', 'Check no entregó los movimientos en este momento. Se reintenta solo.');
    var docs = ACT.documentos || [];
    if (!docs.length) return msg('bi-file-earmark-x', 'Este retiro no tiene documentos para buscar en Check.');
    docs.forEach(function (d, i) {
      h += '<div class="ck-doc"><div class="ck-doc-h"><span class="ck-doc-t">' + esc(d.rotulo) + '</span><span class="ck-doc-n">' +
        (d.ots.length ? d.n_ot + (d.n_ot === 1 ? ' OT' : ' OT') : 'sin OT todavía') + '</span></div>';
      if (!d.ots.length) h += '<p class="ck-vacio"><i class="bi bi-hourglass-split"></i>Check todavía no muestra una OT (movimiento de bodega) para este documento.</p>';
      if (d.ots.length) h += '<div class="ck-otgrid">' + d.ots.map(function (o, j) { return otHtml(o, 'd' + i + 'o' + j); }).join('') + '</div>';
      if (d.mas) h += '<p class="ck-vacio">…y ' + d.mas + ' OT más.</p>';
      h += '</div>';
    });
    if (ACT.desde_registro) {
      return h + '<p class="ck-pie">Registro guardado en ILUS el ' + esc(ACT.registrado || '') + ': lo que Check informó mientras se preparaba el pedido, con la hora que entrega Check. Check solo se consulta, nunca se modifica.</p></section>';
    }
    return h + '<p class="ck-pie">Datos tal como los informa Check (solo lectura), con la hora que entrega Check · última lectura hace ' + esc(haceTxt(ACT.edad_s)) +
      (ACT.refrescando ? ' · actualizando…' : '') + '.</p></section>';
  }
  // Retiro completado: solo el registro de bodega (quién y cuándo), sin conexión en vivo ni botones
  function registroCheckHtml() {
    if (!G.pasos[0].detalle || !G.pasos[0].detalle.length || G.terminal) return '';
    var guardado = ACT && ACT.desde_registro;
    return '<div class="gp-check ck is-ok" data-gp-noclick="1"><div class="ck-head"><div class="ck-tit"><span class="ck-ico"><i class="bi bi-box-seam-fill"></i></span>' +
      '<div><b>Check · Bodega</b><small>Registro de la preparación de este retiro</small></div></div>' +
      (guardado ? '<span class="gp-conex is-neutro" role="status" title="Lo que Check informó mientras se preparaba el pedido, guardado en ILUS"><i class="gp-conex-dot"></i><b>Registro guardado</b><small>' + esc(ACT.registrado || '') + '</small></span>' : '') +
      '</div><div class="ck-cuerpo">' + actHtml() + '</div></div>';
  }
  function checkHtml() {
    if (esRetirado()) return registroCheckHtml();
    if (STATUS !== 'agenda_confirmada' && STATUS !== 'en_preparacion') return '';
    if (!G.pasos[0].detalle || !G.pasos[0].detalle.length) return '';
    var cx = CHECK && CHECK.conexion ? CHECK.conexion.estado : '';
    var ev = (CHECK && CHECK.evaluacion) || {};
    var tono = (cx === 'sin_conexion' || cx === 'sin_credenciales') ? 'mal' : (ev.listo ? 'ok' : (CHECK ? 'en' : 'neutro'));
    var h = '<div class="gp-check ck is-' + tono + '" data-gp-noclick="1"><div class="ck-head"><div class="ck-tit"><span class="ck-ico"><i class="bi bi-box-seam-fill"></i></span>' +
      '<div><b>Check · Bodega</b><small>Preparación del pedido en tiempo real · solo lectura</small></div></div>' + conexionHtml() +
      '<button type="button" class="ck-btn" data-gp-check-refresh="1"' + (CHECK_CARGANDO ? ' disabled' : '') + '><i class="bi bi-arrow-clockwise' + (CHECK_CARGANDO ? ' ck-gira' : '') + '"></i>' + (CHECK_CARGANDO ? 'Consultando…' : 'Actualizar') + '</button></div><div class="ck-cuerpo">';
    if (!CHECK) {
      return h + '<p class="ck-banner is-neutro"><span class="spinner-border spinner-border-sm"></span><b>Consultando a Check qué tan avanzada va la preparación…</b></p>' + actHtml() + '</div></div>';
    }
    var icono = { ok: 'bi-check-circle-fill', en: 'bi-hourglass-split', mal: 'bi-wifi-off', neutro: 'bi-info-circle-fill' }[tono];
    h += '<p class="ck-banner is-' + tono + '"><i class="bi ' + icono + '"></i><b>' + esc(ev.frase || '') + '</b></p>';
    if (ev.alerta) h += '<p class="ck-alerta"><i class="bi bi-exclamation-triangle-fill"></i>' + esc(ev.alerta) + '</p>';
    if (window.RETIROS_CHECK_LISTO) {          // Check ya terminó y el retiro sigue en «Cita confirmada»: se dice y se ofrece lo que toca
      var dsp = window.RETIROS_CHECK_LISTO.despachado;
      h += '<div class="ck-fin"><div class="ck-fin-t"><i class="bi bi-patch-check-fill"></i><div><b>' + (dsp ? 'Check ya preparó y despachó este pedido' : 'Check ya preparó este pedido') + '</b><span>' +
        (dsp ? 'En ILUS sigue en «Cita confirmada». Si el cliente ya se lo llevó, márcalo como RETIRADO y anota quién lo retiró. No lo envíes a preparación: le llegaría el aviso «estamos preparando» con el pedido ya entregado.'
             : 'Cuando el cliente venga y se lo lleve, márcalo como RETIRADO y anota quién lo retiró. Enviarlo a preparación ya no hace falta.') +
        '</span></div></div><button type="button" class="gp-btn verde" data-gp-retirar="1"><i class="bi bi-check-circle-fill"></i>Marcar como RETIRADO</button></div>';
    }
    if (ev.etapas && ev.etapas.length) {
      h += '<ul class="ck-et">' + ev.etapas.map(function (e) {
        var pct = e.de ? Math.min(100, Math.round(100 * e.hechas / e.de)) : 0;
        var extra = (e.clave === 'revisado' || e.clave === 'despachado');
        return '<li class="' + (e.completa ? 'is-ok ' : '') + (e.clave === 'pickeado' ? 'is-clave ' : '') + (extra ? 'is-extra' : '') + '">' +
          '<div class="ck-et-top"><span class="ck-et-n">' + (e.completa ? '<i class="bi bi-check-lg"></i>' : '<i class="bi bi-circle"></i>') + '</span>' +
          '<span class="ck-et-c"><b>' + e.hechas + '</b> de ' + e.de + '</span></div>' +
          '<div class="ck-et-t">' + esc(e.titulo) + '</div><div class="ck-et-barra"><i style="width:' + pct + '%"></i></div>' +
          '<small>' + esc(e.texto) + (e.clave === 'pickeado' ? ' <b>Marca «preparado».</b>' : '') + (extra ? ' No hace falta para «preparado».' : '') + '</small></li>';
      }).join('') + '</ul>';
    }
    if (CHECK.documentos && CHECK.documentos.length) {
      h += '<div class="ck-docs">' + CHECK.documentos.map(function (d) {
        var t = d.estado === 'listo' ? ['ok', 'listo'] : (d.estado === 'sin_datos' ? ['neutro', 'Check aún no lo tiene'] : ['en', 'en proceso']);
        return '<span class="ck-docpill is-' + t[0] + '"><b>' + esc(d.rotulo) + '</b>' + esc(t[1]) + '</span>';
      }).join('') + '</div>';
    }
    h += actHtml();
    var nota;
    if (STATUS !== 'en_preparacion') nota = (CHECK.prep_auto_activo === false)
      ? 'Cuando envíes el pedido a preparación, ILUS empezará a marcar solo el «listo» según Check. El envío automático está apagado: por ahora solo se consulta.'
      : 'Cuando Check vea que bodega empezó a juntar el pedido, y la cita sea de hoy o del próximo día hábil, ILUS lo pasa solo a «En preparación» en horario de bodega (queda en la bitácora como automático, con la hora, la OT y el usuario que informe Check). Si quieres adelantarte, usa el botón.';
    else if (CHECK.auto_activo === false) nota = 'El marcado automático está apagado: Check solo informa. Marca la lista de abajo a mano.';
    else nota = 'Cuando Check tenga todo pickeado (confirmado en dos revisiones seguidas), ILUS marca solo «Pedido listo para entregar». No se le envía ningún correo al cliente. Check solo se consulta, nunca se modifica.';
    return h + '<p class="ck-nota"><i class="bi bi-shield-lock-fill"></i><span>' + nota + (CHECK_TS ? ' Actualizado a las ' + CHECK_TS + '.' : '') + '</span></p></div></div>';
  }
  // El panel de Check vive DENTRO de la tarjeta del paso 5 (donde lo busca quien prepara), no en la barra.
  function renderCheck() {
    var html = checkHtml();
    var slot = document.getElementById('gpCheckSlot');
    if (!html) { if (slot) slot.remove(); return; }
    if (!slot) {
      var cont = $('#paso5Content');
      if (!cont) return;
      slot = document.createElement('div');
      slot.id = 'gpCheckSlot';
      cont.insertBefore(slot, cont.firstChild);
    }
    if (slot.dataset.h !== html) {
      // «Ver todos los datos» abierto: se vuelve a abrir tras repintar (si no, se cerraría solo cada minuto)
      var abiertos = [].map.call(slot.querySelectorAll('details[open][data-k]'), function (d) { return d.getAttribute('data-k'); });
      slot.innerHTML = html; slot.dataset.h = html;
      abiertos.forEach(function (k) { var d = slot.querySelector('details[data-k="' + k + '"]'); if (d) d.open = true; });
    }
  }
  // ── Check ya lo terminó pero el retiro sigue en «Cita confirmada» ───────────────────────────────────────────
  // Daniel 2026-10-05 (retiro real): «si Check se completó, ¿por qué no avisó que está preparado? Además falta marcar como entregado el pedido».
  // El servidor no le habla a Check al armar la guía, así que aquí se corrige lo que se ve: el paso 5 queda «preparado según Check» y lo que
  // toca es el 6, «Marcar como RETIRADO». Nunca se empuja «Enviar a preparación»: al cliente le llegaría «estamos preparando» con el pedido
  // ya preparado o entregado. Se reaplica en cada render (la guía del servidor se vuelve a leer cada minuto).
  function ajustarPorCheck() {
    var ev = CHECK && CHECK.evaluacion;
    var activo = !!(STATUS === 'agenda_confirmada' && !G.terminal && ev && ev.listo);
    var desp = !!(activo && (ev.despachadas || 0) > 0);
    window.RETIROS_CHECK_LISTO = activo ? { despachado: desp } : null;      // lo lee _confirmarEnviarPreparacion (retiros_internal_detail.js)
    document.body.classList.toggle('gp-check-listo', activo);
    if (!activo || G.pasos[4].estado === 'hecho') return;
    var p5 = G.pasos[4], p6 = G.pasos[5];
    p5.estado = 'hecho'; p5.resumen = desp ? 'Check: preparado y despachado' : 'Check: pedido preparado';
    p5.faltan = []; p5.avisos = []; p5.accion = null; p5.correo = false; p5.bloquea = false;
    p6.estado = 'actual'; p6.bloquea = false; p6.correo = true; p6.avisos = [];
    p6.faltan = [desp
      ? 'Check ya preparó y despachó el pedido. Si el cliente ya se lo llevó: «Marcar como RETIRADO» y anota quién lo retiró. No lo envíes a preparación.'
      : 'Check ya preparó el pedido. Cuando el cliente venga y se lo lleve: «Marcar como RETIRADO» y anota quién lo retiró.'];
    p6.accion = G.sin_responsable ? { tipo: 'retirar', texto: 'Marcar como RETIRADO', deshabilitada: true, motivo: MSG_RESP } : { tipo: 'retirar', texto: 'Marcar como RETIRADO' };
    if (!G.sin_responsable) G.siguiente = 6;
  }
  // «Marcar como RETIRADO» desde el panel de Check (el mismo modal de siempre: quién retiró + RUT + foto)
  function retirarDesdeCheck() {
    if (G.sin_responsable) { avisoResponsable(6); return; }
    if (typeof window.abrirModalRetirar === 'function' && document.getElementById('modalRetirar')) window.abrirModalRetirar();
    else toast('No pude abrir la ventana de «Marcar como retirado». Actualiza la página (F5) e inténtalo de nuevo.', 'warning');
  }
  function render() {
    ajustarPorCheck();
    var total = G.pasos.length;
    var hechos = G.pasos.filter(function (p) { return p.estado === 'hecho'; }).length;
    var pct = Math.round(100 * hechos / total);
    var anillo = '<div class="gp-ring' + (hechos === total ? ' is-lleno' : '') + '" role="img" aria-label="' + hechos + ' de ' + total + ' pasos listos"><svg viewBox="0 0 36 36" aria-hidden="true">' +
      '<circle class="gp-ring-bg" cx="18" cy="18" r="15.9155" pathLength="100"/><circle class="gp-ring-fg" cx="18" cy="18" r="15.9155" pathLength="100" stroke-dasharray="' + pct + ' 100"/></svg>' +
      '<b>' + hechos + '<small>/' + total + '</small></b></div>';
    var h = '<div class="gp-top">' + anillo + '<ol class="gp-track">' + G.pasos.map(nodoHtml).join('') + '</ol>' + docsHtml() + '</div>' + stripHtml();
    if (h !== ULTIMO_DIBUJO) {
      var foco = document.activeElement;
      var clave = '';
      if (foco && panel.contains(foco)) {
        if (foco.getAttribute('data-gp-acc')) clave = '[data-gp-acc="' + foco.getAttribute('data-gp-acc') + '"]';
        else if (foco.getAttribute('data-gp-ver')) clave = '[data-gp-ver="' + foco.getAttribute('data-gp-ver') + '"]';
        else if (foco.getAttribute('data-gp-doc')) clave = '[data-gp-doc="' + foco.getAttribute('data-gp-doc') + '"]';
      }
      panel.innerHTML = h;
      ULTIMO_DIBUJO = h;
      if (clave) { var nuevo = $(clave, panel); if (nuevo && !nuevo.disabled) nuevo.focus({ preventScroll: true }); }
    }
    renderCheck();
  }

  // ── Marcas sobre las tarjetas de la ficha ───────────────────────────────
  function decorar() {
    [].forEach.call(document.querySelectorAll('.step-section'), function (e) {
      e.classList.remove('gp-roja', 'gp-ambar', 'gp-e-hecho', 'gp-e-actual', 'gp-e-espera', 'gp-e-pendiente', 'gp-e-bloqueado');
    });
    [].forEach.call(document.querySelectorAll('.gp-tag'), function (e) { e.remove(); });
    [].forEach.call(document.querySelectorAll('.gp-btn-roja'), function (e) { e.classList.remove('gp-btn-roja'); });
    if (G.terminal) return;
    var usadas = {};
    G.pasos.forEach(function (p) {
      var card = $(anclaDe(p));
      if (!card || usadas[card.id]) return;
      // En la tarjeta compartida (preparación y entrega) manda el primer paso que no esté hecho.
      if (p.n === 6 && G.pasos[4].estado !== 'hecho') return;
      usadas[card.id] = true;
      card.classList.add('gp-e-' + p.estado);
      if (p.estado !== 'actual' && p.estado !== 'espera') return;
      card.classList.add(p.estado === 'actual' ? 'gp-roja' : 'gp-ambar');
      var tag = document.createElement('div');
      tag.className = 'gp-tag' + (p.estado === 'espera' ? ' is-espera' : '');
      var acc = p.accion && (p.accion.tipo === 'confirmar_docs' || p.accion.tipo === 'confirmar_productos') && p.estado === 'actual' && !p.accion.deshabilitada;
      var bloq = p.estado === 'actual' && p.accion && p.accion.deshabilitada && p.accion.motivo;
      tag.innerHTML = '<span><b>PASO ' + p.n + ' · ' + (p.estado === 'actual' ? 'TE TOCA' : 'ESPERANDO') + ':</b> ' + esc(p.faltan[0] || p.titulo) + (p.correo && p.estado === 'actual' ? ' <em class="gp-tag-mail"><i class="bi bi-envelope-fill"></i> Le llega un correo al cliente</em>' : '') +
        (bloq ? ' <em class="gp-tag-mail"><i class="bi bi-lock-fill"></i> ' + esc(p.accion.motivo) + '</em>' : '') + '</span>' +
        (acc ? '<button type="button" class="gp-btn" data-gp-acc="' + p.n + '"><i class="bi bi-patch-check-fill"></i>' + esc(p.accion.texto) + '</button>' : '');
      card.insertBefore(tag, card.firstChild);
      if (p.estado === 'actual' && BTNS[p.n]) {
        var b = $(BTNS[p.n]);
        if (b && visible(b)) b.classList.add('gp-btn-roja');
      }
    });
  }

  // ── Acciones ─────────────────────────────────────────────────────────────
  function aplicar(d) {
    G = d;
    if (d.status) { STATUS = d.status; panel.dataset.status = d.status; }
    render(); decorar();
  }
  function postear(ruta, btn, firma, okMsg) {
    if (btn) btn.disabled = true;
    return fetch('/retiros/' + RID + '/' + ruta, {
      method: 'POST', credentials: 'same-origin',
      headers: { 'Accept': 'application/json', 'Content-Type': 'application/json', 'X-Requested-With': 'XMLHttpRequest' },
      body: JSON.stringify({ firma: firma || null })
    }).then(function (r) { return r.json().then(function (d) { return { ok: r.ok && d && d.ok, d: d }; }); })
      .then(function (x) {
        if (!x.ok) {
          toast((x.d && x.d.error) || 'No se pudo guardar. Reintenta.', 'error');
          if (btn) btn.disabled = false;
          refrescar();                          // por si la lista cambió: se muestra la vigente
          return;
        }
        aplicar(x.d);
        toast(okMsg, 'success');
      })
      .catch(function (e) { toast('Sin conexión: ' + e.message, 'error'); if (btn) btn.disabled = false; });
  }
  function accionar(n, btn) {
    var p = G.pasos[n - 1];
    var a = p.accion;
    if (!a) return;
    if (a.tipo === 'confirmar_docs') return postear('confirmar-docs', btn, G.firma_docs, '✓ Facturas confirmadas. Queda registrado quién y cuándo. No se envía nada al cliente.');
    if (a.tipo === 'confirmar_productos') return postear('confirmar-productos', btn, G.firma_prod, '✓ Productos confirmados. Queda registrado quién y cuándo. No se envía nada al cliente.');
    if (a.tipo === 'tomar') {
      var t = $('#btnTomarRetiro');
      if (!t) { irAlPaso(2, false); return; }
      irA('#paso-resp');
      if (typeof window.tomarRetiro === 'function') window.tomarRetiro(t);
      return;
    }
    var real = BTNS[n] && $(BTNS[n]);
    var activar = function (ancla) {
      if (!irA(ancla)) { irAlPaso(n, false); return; }
      if (real && visible(real)) real.click();
      else toast('Ese botón todavía no está disponible: ' + (p.faltan[0] || 'falta terminar un paso anterior.'), 'warning');
    };
    if (a.tipo === 'proponer') return activar('#paso-4');
    if (a.tipo === 'preparacion') return activar('#paso-confirmacion');
    if (a.tipo === 'retirar') {
      irA('#paso-confirmacion');
      if (real && visible(real)) real.click();
      else if (typeof window.abrirModalRetirar === 'function') window.abrirModalRetirar();
      return;
    }
    irAlPaso(n, false);
  }
  panel.addEventListener('click', function (ev) {
    var t = ev.target;
    if (!t.closest) return;
    var b = t.closest('[data-gp-acc]');
    if (b) { ev.stopPropagation(); accionar(parseInt(b.dataset.gpAcc, 10), b); return; }
    var v = t.closest('[data-gp-ver]');
    if (v) { irAlPaso(parseInt(v.dataset.gpVer, 10), true); return; }
    var dd = t.closest('[data-gp-doc]');
    if (dd) { irADoc(dd.getAttribute('data-gp-doc')); return; }
    if (t.closest('.gp-strip') && !t.closest('button,a')) irAlSiguientePendiente(false);
  });
  // Botones que viven en las tarjetas de la ficha: «Confirmo» de la franja roja y «Actualizar» del panel de Check
  document.addEventListener('click', function (ev) {
    var t = ev.target;
    if (!t.closest) return;
    var b = t.closest('.gp-tag [data-gp-acc]');
    if (b) { accionar(parseInt(b.dataset.gpAcc, 10), b); return; }
    if (t.closest('#gpCheckSlot [data-gp-check-refresh]')) { cargarCheck(true); ACT_REINTENTOS = 0; cargarActividad(); }
    if (t.closest('[data-gp-retirar]')) { ev.preventDefault(); retirarDesdeCheck(); }
    var cp = t.closest('[data-copiar]');
    if (cp) {
      ev.preventDefault();
      var txt = cp.getAttribute('data-copiar');
      var a_mano = function () { toast('Copia a mano el N° de OT: ' + txt, 'info'); };
      try { navigator.clipboard.writeText(txt).then(function () { toast('✓ N° de OT ' + txt + ' copiado: pégalo en Check para buscarlo.', 'success'); }, a_mano); }
      catch (e) { a_mano(); }
    }
  });

  // ── Enter = ir al siguiente pendiente, por prioridad de tarjeta ─────────────────────────────────────────
  // Daniel 2026-10-02: «si algo está mal y le das Enter, que te lleve a donde falta el detalle, por orden de prioridad de
  // tarjeta». La lista sale de la guía (pasos en rojo) y de lo que se ve en pantalla (productos con problema, facturas sin
  // saldo o de otro RUT, el RUT que no coincide); va de la tarjeta 1 a la 6 y cada Enter pasa al siguiente (Mayús+Enter vuelve al
  // anterior). Con varios documentos, cada pendiente dice de CUÁL es. Sin responsable declarado solo hay un pendiente: «Me hago cargo».
  var PEND_I = -1, PEND_EL = null;
  var VENTANAS = '.modal.show,.ilus-overlay,.ilus-sheet-overlay,.rba-modal.is-open,#opChatPanel.open,[aria-modal="true"]:not([aria-hidden="true"])';
  function hayVentanaAbierta() {      // una ventana propia abierta: el Enter es suyo, no de la guía
    var l = document.querySelectorAll(VENTANAS);
    for (var i = 0; i < l.length; i++) { if (visible(l[i])) return true; }
    return false;
  }
  function textoDe(el) { return el ? String(el.textContent || '').replace(/\s+/g, ' ').trim() : ''; }
  function docDeFila(row) {           // «BLV 23732 · » delante del pendiente cuando el retiro tiene varios documentos
    var art = row.closest && row.closest('.pd-doc');
    return ((G.docs || []).length > 1 && art) ? claveDoc(textoDe($('.pd-doc-num', art))) + ' · ' : '';
  }
  function pendientes() {
    var out = [];
    if (G.terminal) return out;
    function poner(n, nivel, texto, el) { if (el) out.push({ n: n, nivel: nivel, texto: texto, el: el }); }
    if (G.sin_responsable) {          // lo demás está bloqueado hasta que alguien se haga cargo
      var tomar0 = $('#btnTomarRetiro'), card0 = $(CARDS[2]);
      poner(2, 'rojo', (G.pasos[1].faltan && G.pasos[1].faltan[0]) || G.pasos[1].titulo, visible(tomar0) ? tomar0 : card0);
      return out;
    }
    G.pasos.forEach(function (p) {
      var card = $(anclaDe(p));
      var boton = card ? $('.gp-tag [data-gp-acc]', card) : null;       // el «Confirmo» de la franja roja
      if (p.n === 1) {
        if (p.estado === 'actual') {
          var abrir = $('.btn-asociar-compact');
          var hayDocs = G.pasos[0].detalle && G.pasos[0].detalle.length;
          poner(1, 'rojo', p.faltan[0] || p.titulo, hayDocs ? (boton || card) : (visible(abrir) ? abrir : card));
        }
        var alertaRut = $('.fv8-alerta');
        if (alertaRut && visible(alertaRut)) poner(1, 'rojo', textoDe($('strong', alertaRut)) || 'El RUT del documento no coincide con el de quien retira.', alertaRut);
        [].forEach.call(document.querySelectorAll('#tabDocsAsociados tbody tr[data-doc-id]'), function (tr) {
          var sinSaldo = $('.td-pill-warn', tr), otroRut = $('.otro-rut-badge', tr);
          if (sinSaldo || otroRut) {
            poner(1, 'ambar', 'Revisar ' + textoDe($('.td-pill-dark', tr)) + ' ' + textoDe($('td.mono', tr)) + ': ' + (sinSaldo ? textoDe(sinSaldo).toLowerCase() : 'es de otro RUT'), tr);
          }
        });
      } else if (p.n === 2) {
        if (p.estado === 'actual' || (p.estado === 'pendiente' && p.secundario)) {
          var tomar = $('#btnTomarRetiro');
          poner(2, p.estado === 'actual' ? 'rojo' : 'ambar', p.faltan[0] || p.titulo, visible(tomar) ? tomar : card);
        }
      } else if (p.n === 3) {
        [].forEach.call(document.querySelectorAll('#paso-3 .pd-row.t-rojo'), function (row) {
          poner(3, 'rojo', docDeFila(row) + 'Producto con problema: ' + textoDe($('.pd-nombre', row)) + ' — ' + textoDe($('.pd-badge', row)), row);
        });
        [].forEach.call(document.querySelectorAll('#paso-3 .pd-row.t-ambar'), function (row) {
          poner(3, 'ambar', docDeFila(row) + 'Revisar: ' + textoDe($('.pd-nombre', row)) + ' — ' + textoDe($('.pd-badge', row)), row);
        });
        if (p.estado === 'actual') poner(3, 'rojo', p.faltan[0] || p.titulo, boton || card);
      } else if (p.estado === 'actual') {
        var bt = BTNS[p.n] ? $(BTNS[p.n]) : null;
        poner(p.n, 'rojo', p.faltan[0] || p.titulo, visible(bt) ? bt : card);
      }
    });
    return out;
  }
  function irAlSiguientePendiente(atras) {
    if (G.terminal) { toast(G.terminal + ' No queda nada por hacer.', 'info'); return; }
    var lista = pendientes();
    if (!lista.length) {
      var sig = G.siguiente ? G.pasos[G.siguiente - 1] : null;
      toast(sig && sig.estado === 'espera' ? 'Todo está al día. Ahora toca esperar: ' + (sig.faltan[0] || sig.titulo) : 'No hay nada pendiente: todo está en verde ✔', 'success');
      return;
    }
    // Dónde quedó la persona: se busca por la CARTA (no por el número de orden): si resolvió un pendiente anterior, el Enter no salta uno
    var pos = -1;
    for (var k = 0; k < lista.length; k++) { if (PEND_EL && lista[k].el === PEND_EL) { pos = k; break; } }
    var i;
    if (pos >= 0) i = pos + (atras ? -1 : 1);
    else if (PEND_I >= 0) i = atras ? PEND_I - 1 : PEND_I;          // el que estaba ya se resolvió: el siguiente ocupó su lugar
    else i = atras ? lista.length - 1 : 0;
    i = ((i % lista.length) + lista.length) % lista.length;
    PEND_I = i;
    var it = lista[i];
    PEND_EL = it.el;
    it.el.scrollIntoView({ behavior: 'smooth', block: 'center' });
    var marco = (it.el.closest && it.el.closest('.pd-row,tr,.step-section')) || it.el;
    [marco, it.el].forEach(function (x) {
      x.classList.remove('gp-resalta'); void x.offsetWidth; x.classList.add('gp-resalta');
      setTimeout(function () { x.classList.remove('gp-resalta'); }, 1800);
    });
    // El foco se queda en el CONTENEDOR (no en un botón): así otro Enter sigue al siguiente pendiente en vez de apretar
    // sin querer «Enviar a preparación» o «Quitar»; con Tab se entra a los controles de esa fila o tarjeta.
    marco.setAttribute('tabindex', '-1');
    try { marco.focus({ preventScroll: true }); } catch (e) { /* queda resaltado igual */ }
    toast('Pendiente ' + (i + 1) + ' de ' + lista.length + ' · Paso ' + it.n + ': ' + it.texto, it.nivel === 'rojo' ? 'warning' : 'info');
  }
  document.addEventListener('keydown', function (ev) {
    if (ev.key !== 'Enter' || ev.defaultPrevented || ev.ctrlKey || ev.metaKey || ev.altKey) return;
    var t = ev.target;
    // Enter dentro de un campo, botón, enlace o ventana abierta sigue haciendo lo suyo
    if (t && t.closest && t.closest('input,textarea,select,button,a,summary,[contenteditable="true"],[role="button"]')) return;
    if (AVISO_ABIERTO || hayVentanaAbierta()) return;
    if (ev.repeat) return;            // con la tecla mantenida no se recorre toda la lista de golpe (ni 12 avisos)
    ev.preventDefault();
    irAlSiguientePendiente(ev.shiftKey);
  });

  // ── Refrescos ────────────────────────────────────────────────────────────
  function refrescar() {
    fetch('/retiros/' + RID + '/guia', { credentials: 'same-origin', cache: 'no-store' })
      .then(function (r) { return r.json(); })
      .then(function (d) { if (d && d.ok) aplicar(d); })
      .catch(function () { /* sin red: se queda como está */ });
  }
  // Retiro completado: la tarjeta de preparación sigue mostrando lo que Check informó (OT, quién, cuándo), guardado en ILUS (Daniel 2026-10-06)
  function esRetirado() { return STATUS === 'retirada' || STATUS === 'cerrada'; }
  function relevanteActividad() {
    if (esRetirado()) return !!(G.pasos[0].detalle && G.pasos[0].detalle.length && !G.terminal);
    return relevanteCheck();
  }
  function relevanteCheck() {
    return (STATUS === 'agenda_confirmada' || STATUS === 'en_preparacion') && G.pasos[0].detalle && G.pasos[0].detalle.length && !G.terminal;
  }
  function msgCheckCaido() {
    return { evaluacion: { frase: 'No pudimos consultar Check ahora. Usa la lista manual de abajo.', etapas: [] }, documentos: [],
      conexion: { estado: 'sin_conexion', docs_ok: 0, docs_total: 0 } };
  }
  // Quién pickeó / responsable / cuándo / asignado, según las OT de Check. La primera lectura puede tardar (Check entrega
  // un volcado grande): el servidor responde «cargando» y aquí se reintenta cada 6 s (hasta ~1 minuto).
  function cargarActividad() {
    if (!relevanteActividad() || ACT_CARGANDO) return;
    if (esRetirado() && ACT && ACT.estado === 'listo') return;          // ya está guardado: no hace falta volver a pedirlo
    ACT_CARGANDO = true;
    var ctrl = window.AbortController ? new AbortController() : null;
    var corte = setTimeout(function () { if (ctrl) ctrl.abort(); }, 25000);
    fetch('/retiros/' + RID + '/check-actividad', { credentials: 'same-origin', cache: 'no-store', signal: ctrl ? ctrl.signal : undefined })
      .then(function (r) { return r.json(); })
      .then(function (d) {
        clearTimeout(corte);
        ACT_CARGANDO = false;
        ACT = (d && d.ok) ? d : { estado: 'error' };
        renderCheck();
        if (ACT.estado === 'cargando' && ACT_REINTENTOS < 10) { ACT_REINTENTOS++; setTimeout(cargarActividad, 6000); }
        else if (ACT.estado === 'listo') ACT_REINTENTOS = 0;
      })
      .catch(function () {
        clearTimeout(corte);
        ACT_CARGANDO = false;
        ACT = { estado: 'error' };
        renderCheck();
      });
  }
  function cargarCheck(manual) {
    if (!relevanteCheck()) return;
    if (CHECK_CARGANDO) { if (manual) toast('Ya estoy consultando a Check, un momento…', 'info'); return; }
    CHECK_CARGANDO = true;
    if (manual) render();
    var ctrl = window.AbortController ? new AbortController() : null;
    var corte = setTimeout(function () { if (ctrl) ctrl.abort(); }, 30000);
    fetch('/retiros/' + RID + '/check-preparacion' + ((STATUS === 'en_preparacion' || STATUS === 'agenda_confirmada') ? '' : '?solo_lectura=1'),
      { credentials: 'same-origin', cache: 'no-store', signal: ctrl ? ctrl.signal : undefined })
      .then(function (r) { return r.json(); })
      .then(function (d) {
        clearTimeout(corte);
        CHECK_CARGANDO = false;
        if (!d || !d.ok) CHECK = msgCheckCaido();
        else { CHECK = d; CHECK_TS = new Date().toLocaleTimeString('es-CL', { hour: '2-digit', minute: '2-digit' }); }
        if (d && d.prep_auto_ahora) {
          // Daniel 2026-10-02: «envíes a preparación en automático». El estado y los botones los pinta el servidor: se recarga la
          // página, salvo que la persona esté escribiendo en un campo.
          toast('✓ Check detectó que bodega ya empezó a juntar el pedido: el retiro pasó solo a EN PREPARACIÓN.', 'success');
          setTimeout(function () {
            var foco = document.activeElement;
            var ocupada = hayVentanaAbierta() || (foco && foco.closest && foco.closest('input,textarea,select,[contenteditable="true"]'));
            if (!ocupada) { window.location.reload(); return; }
            // Está haciendo algo (una ventana abierta o escribiendo): no se le pisa; se le deja un aviso fijo para que actualice cuando termine
            if (typeof window.ilusToast === 'function') window.ilusToast('El retiro cambió de estado. Cuando termines lo que estás haciendo, actualiza la página (F5).', { type: 'warning', duration: 0 });
          }, 2500);
        }
        if (d && d.aplicado_ahora) {
          toast('✓ Check confirmó que bodega ya juntó todo: pedido LISTO para entregar.', 'success');
          if (typeof window._wmsCargar === 'function') { try { window._wmsCargar(); } catch (e) { /* la lista se actualiza al recargar */ } }
        }
        render(); decorar();
        if (d && d.ok && (d.aplicado_ahora || (d.preparado && STATUS === 'en_preparacion' && G.pasos[4].estado !== 'hecho'))) refrescar();
      })
      .catch(function () {
        clearTimeout(corte);
        CHECK_CARGANDO = false;
        CHECK = msgCheckCaido();
        render(); decorar();
      });
  }

  // Cuando cualquier acción de la ficha cambia algo de ESTE retiro, la guía se actualiza sola.
  var _fetch = window.fetch;
  window.fetch = function (input, init) {
    var p = _fetch.apply(this, arguments);
    try {
      var url = typeof input === 'string' ? input : ((input && input.url) || '');
      var metodo = ((init && init.method) || (input && input.method) || 'GET').toUpperCase();
      if (metodo !== 'GET' && url.indexOf('/retiros/' + RID + '/') >= 0 && !/\/(guia|check-preparacion|confirmar-)/.test(url)) {
        p.then(function () { setTimeout(refrescar, 700); }, function () { });
      }
    } catch (e) { /* nunca romper el fetch original */ }
    return p;
  };

  // Barra fija compacta (Daniel: «no quiero tanto scroll»): al bajar por la ficha solo quedan a la vista el progreso y los 6 pasos
  // (el «siguiente paso» completo, con su botón, se ve al subir). Solo en escritorio, donde la barra es fija.
  try {
    if ('IntersectionObserver' in window && panel.parentNode) {
      var sentinela = document.createElement('div');
      sentinela.setAttribute('aria-hidden', 'true');
      sentinela.style.cssText = 'height:1px;margin:0;padding:0;pointer-events:none';
      panel.parentNode.insertBefore(sentinela, panel);
      new IntersectionObserver(function (es) {
        var e = es[es.length - 1];
        var fija = !e.isIntersecting && e.boundingClientRect.top < 0 && window.matchMedia('(min-width:992px)').matches;
        panel.classList.toggle('is-fija', fija);
      }).observe(sentinela);
    }
  } catch (e) { /* sin barra compacta: todo sigue funcionando igual */ }

  // Sin bucles: render() solo escribe dentro del panel (y su dibujo se compara antes de tocar el DOM)
  try {
    var contProductos = $('#paso-3'), esperaDocs = null;
    if (contProductos && window.MutationObserver) {
      new MutationObserver(function () { clearTimeout(esperaDocs); esperaDocs = setTimeout(render, 250); }).observe(contProductos, { childList: true, subtree: true });
    }
  } catch (e) { /* sin seguimiento en vivo: se actualiza con cada refresco */ }

  render();
  decorar();
  cargarCheck(false);
  cargarActividad();
  setInterval(function () {
    if (document.visibilityState !== 'visible') return;
    refrescar();
    cargarCheck(false);
    ACT_REINTENTOS = 0;
    cargarActividad();
  }, 60000);
  document.addEventListener('visibilitychange', function () {
    if (document.visibilityState === 'visible') { refrescar(); cargarCheck(false); ACT_REINTENTOS = 0; cargarActividad(); }
  });
  window.gpRefrescar = refrescar;
})();

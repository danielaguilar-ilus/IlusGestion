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
  // ── OT de Check en UNA línea + tiempos de preparación (Daniel 2026-10-06: «insisto, eso debe ser realmente pequeño» y «calcular los tiempos
  //    de preparación… no sabes cuánto valor le da eso en tiempo real»). Las horas son las que entrega Check; si una OT sigue en curso, corre en vivo.
  function aFecha(v) {                     // 'dd/mm/aaaa hh:mm' → Date (hora local, la que entrega Check)
    var m = /^(\d{2})\/(\d{2})\/(\d{4})(?:\s+(\d{2}):(\d{2}))?/.exec(String(v || '').trim());
    return m ? new Date(+m[3], +m[2] - 1, +m[1], +(m[4] || 0), +(m[5] || 0)) : null;
  }
  function horaDe(d) { return d ? ('0' + d.getHours()).slice(-2) + ':' + ('0' + d.getMinutes()).slice(-2) : ''; }
  function durTxt(min) {
    min = Math.max(0, Math.round(min));
    if (min < 1) return 'menos de 1 min';
    if (min < 60) return min + ' min';
    return Math.floor(min / 60) + ' h' + (min % 60 ? ' ' + (min % 60) + ' min' : '');
  }
  function otFinal(o) { return /termin|final|cerr|complet|ejecut|ok/.test(String(o.estado || '').toLowerCase()); }
  function otInicio(o) { return aFecha(o.inicio) || aFecha(((o.momentos || [])[0] || {}).valor); }
  function otFin(o) {
    var f = aFecha(o.fin);
    if (f) return f;
    var ms = o.momentos || [];
    return (ms.length > 1 && otFinal(o)) ? aFecha(ms[ms.length - 1].valor) : null;
  }
  function otAsignada(o) {
    var m = (o.momentos || []).filter(function (x) { return /asign/i.test(String(x.campo || x.etiqueta || '')); })[0];
    return m ? aFecha(m.valor) : null;
  }
  function otClase(o) {
    var t = String(o.tipo || '').toLowerCase();
    if (/salida|despach|expedi|entrega/.test(t)) return 'salida';
    if (/pick/.test(t)) return 'picking';
    if (/revis|chequeo|control/.test(t)) return 'revision';
    return 'otra';
  }
  // Separación inteligente (Daniel 2026-10-06: «la puedo asignar en la tarde y el operario la termina mañana»): si una OT o un tramo pasa de un
  // día a otro, la noche y el fin de semana NO se cuentan; solo la jornada de bodega (07:30–20:00, lunes a viernes). Dentro del mismo día se cuenta
  // el reloj tal cual (aunque bodega trabaje un poco antes o después de la jornada).
  var JORNADA = (function () {
    var m = /^(\d{1,2}):(\d{2})-(\d{1,2}):(\d{2})$/.exec((panel.dataset.jornada || '').trim());
    return m ? [+m[1] * 60 + +m[2], +m[3] * 60 + +m[4]] : [7 * 60 + 30, 20 * 60];
  })();
  function mismoDia(a, b) { return a.getFullYear() === b.getFullYear() && a.getMonth() === b.getMonth() && a.getDate() === b.getDate(); }
  function minJornada(a, b) {
    var tot = 0, d = new Date(a.getFullYear(), a.getMonth(), a.getDate());
    for (var n = 0; d < b && n < 400; n++) {
      if (d.getDay() !== 0 && d.getDay() !== 6) {
        var j0 = new Date(d.getTime() + JORNADA[0] * 60000), j1 = new Date(d.getTime() + JORNADA[1] * 60000);
        var x = a > j0 ? a : j0, y = b < j1 ? b : j1;
        if (y > x) tot += y - x;
      }
      d = new Date(d.getFullYear(), d.getMonth(), d.getDate() + 1);
    }
    return tot / 60000;
  }
  // {min: minutos que cuentan, reloj: minutos de reloj, pausa: true si se descontó la noche / fin de semana}
  function durReal(a, b) {
    if (!a || !b || b <= a) return { min: 0, reloj: 0, pausa: false };
    var reloj = (b - a) / 60000;
    if (mismoDia(a, b)) return { min: reloj, reloj: reloj, pausa: false };
    var min = minJornada(a, b);
    return { min: min, reloj: reloj, pausa: reloj - min >= 30 };
  }
  function pausaTxt(d) { return 'sin contar noche ni fin de semana · reloj ' + durTxt(d.reloj); }
  var HAY_EN_CURSO = false;               // si una OT sigue abierta, el panel se vuelve a dibujar cada 30 s
  var DATOS_OT = {};                      // clave → OT, para abrir sus datos completos en el modal
  function fechaCorta(d) { return ('0' + d.getDate()).slice(-2) + '/' + ('0' + (d.getMonth() + 1)).slice(-2); }
  function pieza(cls, k, v, em, title) {
    return '<span class="ck-t' + (cls ? ' ' + cls : '') + '"' + (title ? ' title="' + esc(title) + '"' : '') + '><small>' + k + '</small><b>' + v + '</b>' +
      (em ? '<em>' + em + '</em>' : '') + '</span>';
  }
  function tiemposHtml(ots) {
    var ahora = new Date(), lista = [];
    (ots || []).forEach(function (o) {
      var ini = otInicio(o);
      if (!ini) return;
      var fin = otFin(o), curso = !fin && !otFinal(o);
      if (curso) HAY_EN_CURSO = true;
      lista.push({ c: otClase(o), ini: ini, fin: fin || (curso ? ahora : ini), curso: curso, asig: otAsignada(o) });
    });
    if (!lista.length) return '';
    lista.sort(function (a, b) { return a.ini - b.ini; });
    var enCurso = lista.some(function (x) { return x.curso; });
    var piezas = [];
    // Misma lógica que retiros_tiempos.py (lo que queda guardado como evidencia): trabajo = suma de cada OT; preparación efectiva = tiempo con al
    // menos una OT abierta (las que se pisan se juntan); pausas = principio a fin − efectiva (la intermitencia: nadie tenía una OT abierta).
    var suma = 0, sumaPausa = false;
    lista.forEach(function (x) { var d = durReal(x.ini, x.fin); suma += d.min; sumaPausa = sumaPausa || d.pausa; });
    var juntos = [];
    lista.forEach(function (x) {
      var u = juntos[juntos.length - 1];
      if (u && x.ini <= u[1]) { if (x.fin > u[1]) u[1] = x.fin; } else juntos.push([x.ini, x.fin]);
    });
    var efectivo = juntos.reduce(function (m, j) { return m + durReal(j[0], j[1]).min; }, 0);
    var ultFin = lista.reduce(function (m, x) { return x.fin > m ? x.fin : m; }, lista[0].fin);
    var pfr = durReal(lista[0].ini, ultFin);
    var pausasMin = Math.max(0, Math.round(pfr.min - efectivo));
    piezas.push(pieza(enCurso ? 'is-curso' : 'is-trabajo', 'Trabajo (' + lista.length + ' OT)', durTxt(suma),
      enCurso ? 'en curso' : (Math.round(suma) - Math.round(efectivo) >= 2 ? 'en paralelo · efectivo ' + durTxt(efectivo) : (sumaPausa ? 'sin noches' : '')),
      'Suma de lo que duró cada OT en Check' + (sumaPausa ? ' (sin contar noche ni fin de semana)' : '') +
      (Math.round(suma) - Math.round(efectivo) >= 2 ? '. Hubo OT al mismo tiempo: la preparación efectiva fue ' + durTxt(efectivo) : '')));
    // Pausas (intermitencia) y, si se sabe, cuánto quedó listo esperando la salida
    var pk = lista.filter(function (x) { return x.c === 'picking'; });
    var sal = lista.filter(function (x) { return x.c === 'salida'; });
    var listoTxt = '';
    if (pk.length && sal.length && !pk.some(function (x) { return x.curso; })) {
      var pFin = pk.reduce(function (m, x) { return x.fin > m ? x.fin : m; }, pk[0].fin);
      if (sal[0].ini >= pFin) listoTxt = 'listo esperando ' + durTxt(durReal(pFin, sal[0].ini).min);
    }
    if (pausasMin >= 1 || listoTxt) {
      var detalle = [];
      for (var q = 1; q < juntos.length; q++) {
        var dq = durReal(juntos[q - 1][1], juntos[q][0]);
        if (dq.min >= 1) detalle.push(horaDe(juntos[q - 1][1]) + '–' + horaDe(juntos[q][0]) + ': ' + durTxt(dq.min) + (dq.pausa ? ' (sin la noche)' : ''));
      }
      piezas.push(pieza('', 'Pausas', durTxt(pausasMin), listoTxt,
        'Tiempo entre OT en que nadie tenía una OT abierta' + (detalle.length ? ' · ' + detalle.join(' · ') : '')));
    }
    // Espera de asignación: desde que se asignó la primera OT hasta que se empezó
    var asig = lista[0].asig;
    if (asig && asig <= lista[0].ini) {
      var da = durReal(asig, lista[0].ini);
      if (da.min >= 1) piezas.push(pieza('', 'Asignada → inicio', durTxt(da.min), horaDe(asig) + (mismoDia(asig, lista[0].ini) ? '' : ' ' + fechaCorta(asig)),
        'Desde que se asignó la OT hasta que el operario la empezó' + (da.pausa ? ' (sin contar noche ni fin de semana)' : '')));
    }
    // 3) De principio a fin (primera OT → última), con la misma separación inteligente
    var ult = ultFin, pf = pfr;
    piezas.push(pieza(enCurso ? 'is-curso' : 'is-total', 'Principio a fin', durTxt(pf.min),
      (pf.pausa ? 'sin noches · ' : '') + horaDe(lista[0].ini) + (mismoDia(lista[0].ini, ult) ? '' : ' ' + fechaCorta(lista[0].ini)) + '–' + (enCurso ? 'ahora' : horaDe(ult)),
      pf.pausa ? pausaTxt(pf) : 'Desde que empezó la primera OT hasta que terminó la última'));
    // 4) Salida (expedición) y cómo quedó contra la cita
    if (sal.length) {
      piezas.push(pieza('is-salida', 'Salida', horaDe(sal[0].ini), fechaCorta(sal[0].ini), 'Hora en que Check registró la salida (expedición)'));
      var mc = /^(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2})/.exec((panel.dataset.cita || '').trim());
      if (mc) {
        var dc = new Date(+mc[1], +mc[2] - 1, +mc[3], +mc[4], +mc[5]);
        var dif = (dc - sal[0].ini) / 60000;
        piezas.push(pieza(dif > 15 ? 'is-antes' : (dif < -15 ? 'is-despues' : 'is-justo'), 'Vs cita ' + horaDe(dc),
          Math.abs(dif) <= 15 ? 'a la hora' : durTxt(Math.abs(dif)) + (dif > 0 ? ' antes' : ' después'), '', 'Salida comparada con la hora de la cita del cliente'));
      }
    }
    return '<div class="ck-tiempos" aria-label="Tiempos de preparación"><span class="ck-tiempos-k"><i class="bi bi-stopwatch"></i>Tiempos</span>' + piezas.join('') + '</div>';
  }
  // Una OT en UNA línea; sus datos completos se ven en un modal (botón «Datos»)
  // Inicio → fin con color (Daniel 2026-10-06: «el inicio y el fin ponlo más animadito, con colores»): verde el inicio, rojo el fin, la barra
  // entre ambos y la duración REAL de la OT (08:11 → 08:26 = 15 min: el número de arriba sale de aquí, no es inventado).
  function tlHtml(ini, fin, curso, d) {
    if (!ini) return '<span class="ck-tl is-nada"><i class="bi bi-clock"></i>Check no informa la hora</span>';
    var otroDia = fin && !mismoDia(ini, fin);
    return '<span class="ck-tl' + (curso ? ' is-curso' : '') + '">' +
      '<span class="ck-tl-p is-ini"><small>Inicio ' + fechaCorta(ini) + '</small><b>' + horaDe(ini) + '</b></span>' +
      '<span class="ck-tl-bar" aria-hidden="true"></span>' +
      (curso ? '<span class="ck-tl-p is-ahora"><small>En curso</small><b>ahora</b></span>'
             : '<span class="ck-tl-p is-fin"><small>Fin' + (otroDia ? ' ' + fechaCorta(fin) : '') + '</small><b>' + horaDe(fin || ini) + '</b></span>') +
      '<span class="ck-tl-d"' + (d && d.pausa ? ' title="' + esc(pausaTxt(d)) + '"' : '') + '>' + (d ? (d.min < 1 ? '<1 min' : durTxt(d.min)) : '') +
        (d && d.pausa ? ' <i class="bi bi-moon-stars" aria-label="' + esc(pausaTxt(d)) + '"></i>' : '') + '</span></span>';
  }
  function cantTxt(o) {
    var n = o.n_lineas || 0, u = o.unidades || 0;
    return n + (n === 1 ? ' línea' : ' líneas') + (u ? ' · ' + (Math.round(u * 100) / 100) + (u === 1 ? ' unidad' : ' unidades') : '');
  }
  // Una OT en UNA fila con TODA la información de la tarjeta (tipo, líneas y unidades, quién y en qué rol, inicio → fin y duración, estado);
  // el resto de los campos de Check en el modal (botón «Datos»).
  function otHtml(o, clave) {
    var est = String(o.estado || '').toLowerCase();
    var cls = /termin|final|cerr|complet|ejecut|ok/.test(est) ? 'ok' : (/anul|cancel|error|rechaz/.test(est) ? 'mal' : 'en');
    var ini = otInicio(o), fin = otFin(o), curso = !fin && !otFinal(o) && !!ini;
    var d = ini ? durReal(ini, fin || new Date()) : null;
    DATOS_OT[clave] = o;
    var quien = (o.personas || []).length ? o.personas.map(function (p) {
      return '<span class="ck-otl-per"><span class="ck-av" style="background:' + colorAv(p.valor) + '" aria-hidden="true">' + esc(iniciales(p.valor)) + '</span>' +
        '<b>' + esc(p.valor) + '</b><small>' + esc(p.etiqueta) + '</small></span>';
    }).join('') : '<span class="ck-otl-nada"><i class="bi bi-person"></i> Check no informa quién</span>';
    return '<div class="ck-otl is-' + cls + '">' +
      '<span class="ck-otn"><b>' + esc(o.ot || 's/n') + '</b>' +
      (o.ot ? '<button type="button" class="ck-copiar" data-copiar="' + esc(o.ot) + '" title="Copiar el N° de OT para buscarlo en Check" aria-label="Copiar N° de OT ' + esc(o.ot) + '"><i class="bi bi-copy"></i></button>' : '') + '</span>' +
      '<span class="ck-otl-tipo"><b>' + esc(o.tipo || 'OT') + '</b><small>' + cantTxt(o) + ' <span class="ck-est is-' + cls + '">' + esc(o.estado || '—') + '</span></small></span>' +
      '<span class="ck-otl-q">' + quien + '</span>' +
      tlHtml(ini, fin, curso, d) +
      ((o.datos || []).length ? '<button type="button" class="ck-otl-mas" data-ck-datos="' + clave + '" title="Ver y copiar todos los datos que Check entrega de esta OT">' +
        '<i class="bi bi-list-ul"></i>Datos <span>' + o.datos.length + '</span></button>' : '<span></span>') +
      '</div>';
  }
  // Todo lo de una OT como texto, para copiar y pegar (correo, WhatsApp, planilla)
  function textoOT(o) {
    var ini = otInicio(o), fin = otFin(o), d = ini ? durReal(ini, fin || new Date()) : null;
    var t = ['OT ' + (o.ot || 's/n') + ' · ' + (o.tipo || 'OT') + ' · ' + (o.estado || ''),
      'Quién: ' + ((o.personas || []).map(function (p) { return p.valor + ' (' + p.etiqueta + ')'; }).join(', ') || 'sin dato'),
      'Inicio: ' + (ini ? fechaCorta(ini) + ' ' + horaDe(ini) : '—') + ' · Fin: ' + (fin ? fechaCorta(fin) + ' ' + horaDe(fin) : (ini && !otFinal(o) ? 'en curso' : '—')) +
        (d ? ' · Duración: ' + durTxt(d.min) + (d.pausa ? ' (' + pausaTxt(d) + ')' : '') : ''),
      cantTxt(o), ''];
    (o.lineas || []).forEach(function (l) {
      t.push('· ' + [l.sku, l.descripcion, l.ua ? 'UA ' + l.ua : '', (l.ejecutado != null ? l.ejecutado : '?') + ' de ' + (l.solicitado != null ? l.solicitado : '?'),
        (l.origen || l.destino) ? (l.origen || '?') + ' → ' + (l.destino || '?') : ''].filter(Boolean).join(' · '));
    });
    if ((o.lineas || []).length) t.push('');
    (o.datos || []).forEach(function (x) { t.push(x.etiqueta + ': ' + x.valor); });
    return t.join('\n');
  }
  // Modal con TODOS los datos de una OT (Daniel 2026-10-06: «los datos de Check en un modal, sin scroll, y poder copiar y pegar todo»).
  function abrirDatosOT(clave) {
    var o = DATOS_OT[clave];
    if (!o) return;
    var ini = otInicio(o), fin = otFin(o), curso = !fin && !otFinal(o) && !!ini, d = ini ? durReal(ini, fin || new Date()) : null;
    var cab = '<div class="ck-dm-cab"><span class="ck-otn"><small>N° OT</small><b>' + esc(o.ot || 's/n') + '</b>' +
      (o.ot ? '<button type="button" class="ck-copiar" data-copiar="' + esc(o.ot) + '" aria-label="Copiar N° de OT ' + esc(o.ot) + '"><i class="bi bi-copy"></i></button>' : '') + '</span>' +
      '<span class="ck-dm-res"><b>' + esc(o.tipo || 'OT') + '</b> · ' + esc(o.estado || '') + ' · ' + cantTxt(o) +
      ((o.personas || []).length ? ' · ' + o.personas.map(function (p) { return '<b>' + esc(p.valor) + '</b> <small>(' + esc(p.etiqueta) + ')</small>'; }).join(', ') : '') + '</span>' +
      tlHtml(ini, fin, curso, d) + '</div>';
    if ((o.lineas || []).length) {
      cab += '<div class="ck-dm-lin"><table><thead><tr><th>SKU</th><th>Descripción</th><th>UA</th><th>Ejecutado / solicitado</th><th>Origen → destino</th></tr></thead><tbody>' +
        o.lineas.map(function (l) {
          return '<tr><td>' + esc(l.sku || '—') + '</td><td>' + esc(l.descripcion || '—') + '</td><td>' + esc(l.ua || '—') + '</td><td>' +
            esc(l.ejecutado != null ? l.ejecutado : '?') + ' / ' + esc(l.solicitado != null ? l.solicitado : '?') + '</td><td>' +
            esc((l.origen || l.destino) ? (l.origen || '?') + ' → ' + (l.destino || '?') : '—') + '</td></tr>';
        }).join('') + '</tbody></table></div>';
    }
    var cuerpo = cab + '<dl class="ck-dm-datos">' + (o.datos || []).map(function (x) { return '<dt>' + esc(x.etiqueta) + '</dt><dd>' + esc(x.valor) + '</dd>'; }).join('') + '</dl>';
    var titulo = 'Datos de Check · OT ' + (o.ot || 's/n');
    if (!(window.bootstrap && window.bootstrap.Modal)) {
      if (typeof window.ilusAlert === 'function') window.ilusAlert({ title: titulo, message: '', sub: cuerpo, subHtml: true });
      return;
    }
    var m = document.getElementById('ckDatosModal');
    if (!m) {
      m = document.createElement('div');
      m.className = 'modal fade';
      m.id = 'ckDatosModal';
      m.tabIndex = -1;
      m.setAttribute('aria-labelledby', 'ckDatosTit');
      m.setAttribute('aria-hidden', 'true');
      m.innerHTML = '<div class="modal-dialog modal-lg modal-dialog-centered modal-dialog-scrollable"><div class="modal-content">' +
        '<div class="modal-header py-2"><h5 class="modal-title" id="ckDatosTit"></h5><button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Cerrar"></button></div>' +
        '<div class="modal-body" id="ckDatosBody"></div>' +
        '<div class="modal-footer py-2"><span class="me-auto small text-muted"><i class="bi bi-lock-fill me-1"></i>Tal como lo entrega Check (solo lectura). Esc o clic afuera para cerrar.</span>' +
        '<button type="button" class="btn btn-outline-dark" id="ckDatosCopiar"><i class="bi bi-clipboard-check me-1"></i>Copiar todo</button>' +
        '<button type="button" class="btn btn-dark" data-bs-dismiss="modal">Cerrar</button></div></div></div>';
      document.body.appendChild(m);
    }
    m.querySelector('#ckDatosTit').textContent = titulo;
    m.querySelector('#ckDatosBody').innerHTML = cuerpo;
    m.querySelector('#ckDatosCopiar').setAttribute('data-copiar-todo', clave);
    window.bootstrap.Modal.getOrCreateInstance(m).show();
  }
  // Quién pickeó / responsable / cuándo / a quién se asignó, según los movimientos (OT) de Check
  function actHtml() {
    HAY_EN_CURSO = false;
    DATOS_OT = {};
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
      if (d.ots.length) {
        var crono = d.ots.slice().sort(function (a, b) { return (otInicio(a) || 0) - (otInicio(b) || 0); });      // en el orden en que pasaron
        h += tiemposHtml(crono) + '<div class="ck-otlista">' + crono.map(function (o, j) { return otHtml(o, 'd' + i + 'o' + j); }).join('') + '</div>';
      }
      if (d.mas) h += '<p class="ck-vacio">…y ' + d.mas + ' OT más.</p>';
      h += '</div>';
    });
    var T = ACT.tiempos;
    if (T && T.calculado && !T.en_curso) {
      h += '<p class="ck-evid" title="' + esc(T.criterio || '') + '"><i class="bi bi-shield-check"></i><span><b>Tiempos guardados como evidencia</b> el ' + esc(T.calculado) +
        ': trabajo ' + durTxt(T.trabajo_min || 0) + ' en ' + (T.n_ot || 0) + ' OT · preparación efectiva ' + durTxt(T.efectivo_min || 0) +
        (T.pausas_min ? ' · pausas ' + durTxt(T.pausas_min) : '') +
        (T.min_por_unidad != null ? ' · picking ' + String(T.min_por_unidad).replace('.', ',') + ' min por unidad' : '') + '.</span></p>';
    }
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
    var vencida = !!(p5.accion && p5.accion.tipo === 'que_paso');    // la cita ya pasó (servidor): se pregunta qué pasó, sin empujar un correo
    p5.estado = 'hecho'; p5.resumen = desp ? 'Check: preparado y despachado' : 'Check: pedido preparado';
    p5.faltan = []; p5.avisos = []; p5.accion = null; p5.correo = false; p5.bloquea = false;
    p6.estado = 'actual'; p6.bloquea = false; p6.correo = true; p6.avisos = [];
    p6.faltan = [desp
      ? 'Check ya preparó y despachó el pedido. Si el cliente ya se lo llevó: «Marcar como RETIRADO» y anota quién lo retiró. No lo envíes a preparación.'
      : 'Check ya preparó el pedido. Cuando el cliente venga y se lo lleve: «Marcar como RETIRADO» y anota quién lo retiró.'];
    p6.accion = G.sin_responsable ? { tipo: 'retirar', texto: 'Marcar como RETIRADO', deshabilitada: true, motivo: MSG_RESP } : { tipo: 'retirar', texto: 'Marcar como RETIRADO' };
    if (vencida) {
      p6.correo = false;
      p6.faltan = ['Check ya preparó el pedido y la cita ya pasó. Registra qué pasó: si el cliente retiró, márcalo como RETIRADO; si no vino, reagenda o ciérralo.'];
      p6.accion = { tipo: 'que_paso', texto: '¿Qué pasó?' };
      G.siguiente = 6;
      return;
    }
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
    if (a.tipo === 'que_paso') {                       // cita vencida: registrar qué pasó (no vino, reagendar, cancelar…)
      if (typeof window.abrirQuePaso === 'function') window.abrirQuePaso('');
      else toast('Recarga la página para registrar qué pasó.', 'warning');
      return;
    }
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
    var ct = t.closest('[data-copiar-todo]');
    if (ct) {
      ev.preventDefault();
      var oc = DATOS_OT[ct.getAttribute('data-copiar-todo')];
      if (!oc) return;
      var falla = function () { toast('No se pudo copiar solo: selecciona el texto del modal y cópialo a mano.', 'warning'); };
      try { navigator.clipboard.writeText(textoOT(oc)).then(function () { toast('✓ Datos de la OT ' + (oc.ot || '') + ' copiados: pégalos donde los necesites.', 'success'); }, falla); }
      catch (e2) { falla(); }
      return;
    }
    var dm = t.closest('[data-ck-datos]');
    if (dm) { ev.preventDefault(); abrirDatosOT(dm.getAttribute('data-ck-datos')); return; }
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
    // ya está guardado: no hace falta volver a pedirlo (salvo que el servidor esté trayendo de Check la OT de control de salida que le falta)
    if (esRetirado() && ACT && ACT.estado === 'listo' && !ACT.refrescando) return;
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
        else if (ACT.estado === 'listo' && ACT.desde_registro && ACT.refrescando && ACT_REINTENTOS < 8) { ACT_REINTENTOS++; setTimeout(cargarActividad, 8000); }
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

  // OT en curso: el tiempo corre solo (sin volver a preguntarle a Check)
  setInterval(function () { if (HAY_EN_CURSO && document.visibilityState === 'visible') renderCheck(); }, 30000);

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

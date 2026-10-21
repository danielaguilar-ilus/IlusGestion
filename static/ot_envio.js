/* ══════════════════════════════════════════════════════════════════════════════════════════════════════════════
   ot_envio.js — Comunicación de la orden de trabajo (REGLA #37).
   2026-10-21 (Daniel): «necesito que esto sea controlable, que avises cuándo lo tienes que enviar y además habilita, así como las cotizaciones,
   un envío por mail directo de las órdenes de trabajo… y quiero que avise a quién se le está enviando: al momento de crearse y enviarse los
   correos se vaya educando el personal de cuándo genera comunicación tanto con los técnicos como con los clientes».
   Tres piezas:
     1. AVISO PREVIO: «Al aprobar el cierre se enviará esta OT a: …» dentro del modal de firma (el técnico) y del modal «Firmar y cerrar»
        (la gestión), con el rol de cada destinatario y lo que esté apagado. GET /ot/api/<id>/correo/destinatarios.
     2. «ENVIAR OT POR CORREO» (solo gestión, OT cerrada): destinatarios prellenados y editables, asunto y mensaje opcionales, resumen explícito,
        vista previa y, después de enviar, la verdad por destinatario. GET/POST /ot/api/<id>/correo/enviar · POST …/correo/vista-previa.
     3. Los avisos de la creación (un recuadro por 15 minutos que se puede cerrar).
   Sin alert/confirm/prompt nativos (REGLA #1: ilusToast). Botones de 44 px y pantalla completa en el celular (REGLA #3). Selección múltiple con
   «Marcar/desmarcar todos» (REGLA #14). Un técnico nunca ve plata ni proveedores (REGLAS #19/#26): el servidor no se los manda.
   ══════════════════════════════════════════════════════════════════════════════════════════════════════════════ */
(function () {
  'use strict';
  var CFG = window.OTD_OE_CFG || {};
  var VID = CFG.vid;
  if (!VID) return;
  var RX_CORREO = /^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$/;

  function esc(s) {
    return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
    });
  }
  function toast(m, t, d) { try { if (window.ilusToast) window.ilusToast(m, { type: t || 'info', duration: d || 6000 }); } catch (e) { /* nada */ } }
  function api(metodo, ruta, cuerpo) {
    var init = { credentials: 'same-origin', method: metodo, headers: { 'X-Requested-With': 'XMLHttpRequest', 'Accept': 'application/json' } };
    if (cuerpo !== undefined) { init.headers['Content-Type'] = 'application/json'; init.body = JSON.stringify(cuerpo); }
    return fetch('/ot/api/' + VID + ruta, init).then(function (r) {
      return r.json().catch(function () { return {}; }).then(function (j) {
        if (!j.ok && !j.error) j.error = r.status === 403 ? 'No tienes permiso para esta acción en esta OT.' : 'No se pudo completar. Intenta de nuevo.';
        j._http = r.status;
        return j;
      });
    }).catch(function () { return { ok: false, error: 'Sin conexión. Revisa la señal e intenta de nuevo: lo que escribiste sigue aquí.' }; });
  }

  /* ═══════════════ 1) AVISO PREVIO: «Al aprobar el cierre se enviará esta OT a…» ═══════════════ */
  var avisoCache = { t: 0, d: null };

  function pintarAviso(el, d) {
    if (!d || !d.ok || !d.aplica) { el.hidden = true; el.innerHTML = ''; return; }
    var items = d.items || [];
    var conAlerta = items.some(function (i) { return !i.saldra; });
    var h = '<div class="oe-aviso-cab"><i class="bi bi-envelope-paper-fill" aria-hidden="true"></i><span>' +
      (items.some(function (i) { return i.saldra; }) ? 'Al aprobar el cierre se enviará esta OT a:' : 'Al aprobar el cierre no se enviará ningún correo.') + '</span></div>';
    if (items.length) {
      h += '<ul class="oe-aviso-lista">';
      items.forEach(function (i) {
        var quien = (i.nombre || i.correo || '') + (i.rol_txt ? ' (' + i.rol_txt + ')' : '');
        var det = [];
        if (i.correo) det.push(esc(i.correo));
        if (i.saldra && i.nota) det.push(esc(i.nota));
        if (!i.saldra && i.motivo) det.push(esc(i.motivo));
        h += '<li class="' + (i.saldra ? 'oe-ok' : 'oe-no') + '"><i class="bi ' + (i.saldra ? 'bi-check-circle-fill' : 'bi-exclamation-triangle-fill') + '" aria-hidden="true"></i>' +
          '<span><b>' + esc(quien) + '</b>' + (det.length ? '<small>' + det.join(' · ') + '</small>' : '') + '</span></li>';
      });
      h += '</ul>';
    }
    h += '<p class="oe-aviso-pie">Así sabes qué comunicación genera esta OT y a quién.' +
      (CFG.gestion ? ' Si alguien más debe recibirla, después de cerrar usa «Enviar OT por correo».' : '') + '</p>';
    el.className = 'oe-aviso' + (conAlerta ? ' oe-con-alerta' : '');
    el.innerHTML = h;
    el.hidden = false;
  }

  function cargarAviso(el) {
    if (avisoCache.d && (Date.now() - avisoCache.t) < 20000) { pintarAviso(el, avisoCache.d); return; }
    el.hidden = false;
    el.className = 'oe-aviso';
    el.innerHTML = '<div class="oe-aviso-cab"><span class="spinner-border spinner-border-sm" aria-hidden="true"></span><span>Revisando a quién se le avisará…</span></div>';
    api('GET', '/correo/destinatarios').then(function (d) {
      if (d && d.ok) avisoCache = { t: Date.now(), d: d };
      pintarAviso(el, d);
    });
  }

  function engancharAvisos() {
    document.querySelectorAll('[data-oe-aviso]').forEach(function (el) {
      var m = el.closest('.modal');
      if (!m || m.getAttribute('data-oe-enganchado')) return;
      m.setAttribute('data-oe-enganchado', '1');
      m.addEventListener('show.bs.modal', function () { cargarAviso(el); });
    });
  }

  /* ═══════════════ 2) «ENVIAR OT POR CORREO» ═══════════════ */
  var D = null;                                   // lo que mandó el servidor
  var S = nuevoEstado();
  var el = null, modal = null;

  function nuevoEstado() { return { dest: [], asunto: '', mensaje: '', ocupado: false, res: null, vista: false, vistaHtml: '', vistaCargando: false }; }

  function asegurar() {
    if (el) return;
    el = document.createElement('div');
    el.className = 'modal fade oe-modal';
    el.id = 'otdEnviarOtModal';
    el.tabIndex = -1;
    el.setAttribute('aria-hidden', 'true');
    el.setAttribute('aria-labelledby', 'oeTitulo');
    el.innerHTML = '<div class="modal-dialog modal-lg modal-dialog-centered modal-dialog-scrollable modal-fullscreen-sm-down"><div class="modal-content">' +
      '<div class="modal-header oe-mh"><div><h5 class="modal-title" id="oeTitulo"><i class="bi bi-envelope-paper-fill" aria-hidden="true"></i> Enviar OT por correo</h5>' +
      '<div class="oe-sub" id="oeSub"></div></div><button type="button" class="btn-close btn-close-white" data-bs-dismiss="modal" aria-label="Cerrar"></button></div>' +
      '<div class="modal-body" id="oeCuerpo"></div>' +
      '<div class="modal-footer" id="oePie"></div></div></div>';
    document.body.appendChild(el);
    modal = new bootstrap.Modal(el);
    el.addEventListener('click', onClick);
    el.addEventListener('input', onInput);
    el.addEventListener('keydown', function (ev) {
      if (ev.key === 'Enter' && ev.target && ev.target.id === 'oeNuevo') { ev.preventDefault(); agregarManual(); }
    });
    el.addEventListener('show.bs.modal', function () { document.body.classList.add('oe-abierto'); });
    el.addEventListener('hidden.bs.modal', function () {
      document.body.classList.remove('oe-abierto');
      if (S.res && S.res.enviados) setTimeout(function () { location.reload(); }, 150);       // la ficha se repinta: el recordatorio ya no corresponde
    });
  }
  function cuerpoEl() { return el.querySelector('#oeCuerpo'); }
  function pieEl() { return el.querySelector('#oePie'); }

  function seleccionados() { return S.dest.filter(function (d) { return d.on; }); }
  function quienTxt(d) { var n = (d.nombre || d.correo || ''); return n + (d.rol_txt ? ' (' + d.rol_txt + ')' : ''); }
  function sinMarcar() { return S.dest.some(function (d) { return !d.on; }); }

  function paso(n, ok, titulo, ayuda, cuerpo) {
    return '<section class="oe-paso' + (ok ? ' is-ok' : '') + '"><div class="oe-paso-cab"><span class="oe-circ">' + (ok ? '<i class="bi bi-check-lg"></i>' : n) + '</span>' +
      '<div><h6>' + esc(titulo) + '</h6>' + (ayuda ? '<small>' + esc(ayuda) + '</small>' : '') + '</div></div>' + cuerpo + '</section>';
  }

  function htmlDestinatarios() {
    var h = '';
    if (S.dest.length > 1) {
      h += '<div class="oe-toolbar"><button type="button" class="oe-todo" data-oe-todo><i class="bi bi-check-all"></i> ' +
        (sinMarcar() ? 'Marcar todos' : 'Desmarcar todos') + '</button><span class="oe-cuenta">' + seleccionados().length + ' de ' + S.dest.length + ' elegidos</span></div>';
    }
    if (!S.dest.length) {
      h += '<div class="oe-vacio"><i class="bi bi-info-circle"></i> No hay correos propuestos para esta OT: escribe uno abajo.</div>';
    }
    h += '<div class="oe-dests">';
    S.dest.forEach(function (d, i) {
      h += '<div class="oe-fila">' +
        '<button type="button" class="oe-dest' + (d.on ? ' on' : '') + '" data-oe-dest="' + i + '" aria-pressed="' + (d.on ? 'true' : 'false') + '">' +
        '<span class="oe-chk"><i class="bi ' + (d.on ? 'bi-check-square-fill' : 'bi-square') + '" aria-hidden="true"></i></span>' +
        '<span class="oe-dest-tx"><b>' + esc(d.nombre || d.correo) + '</b>' + (d.nombre ? '<small>' + esc(d.correo) + '</small>' : '') +
        '<span class="oe-rol ' + esc(d.rol) + '">' + esc(d.rol_txt || 'Otro destinatario') + '</span></span></button>' +
        (d.extra ? '<button type="button" class="oe-quitar" data-oe-quitar="' + i + '" aria-label="Quitar ' + esc(d.correo) + '" title="Quitar"><i class="bi bi-x-lg"></i></button>' : '') +
        '</div>';
    });
    h += '</div>';
    h += '<div class="oe-agregar"><input type="email" id="oeNuevo" placeholder="Agregar otro correo" aria-label="Agregar otro correo" autocomplete="off" inputmode="email">' +
      '<button type="button" class="oe-btn sec" data-oe-agregar><i class="bi bi-plus-lg"></i> Agregar</button></div>';
    h += '<div class="oe-nota">Escribe un correo y presiona Enter o «Agregar». Máximo ' + esc(D.maximo) + ' destinatarios. Se manda un correo por persona, con el mismo documento firmado (sin montos).</div>';
    return h;
  }

  function htmlHistorial() {
    var ant = D.anteriores || [];
    if (!ant.length) return '';
    var h = '<section class="oe-paso"><div class="oe-paso-cab"><span class="oe-circ oe-gris"><i class="bi bi-clock-history"></i></span><div><h6>Lo que ya se envió de esta OT</h6></div></div><ul class="oe-hist">';
    ant.forEach(function (a) {
      h += '<li class="t-' + esc(a.tipo) + '"><b>' + esc(a.titulo) + '</b><br><small>' + esc(a.cuando) + (a.quien ? ' · ' + esc(a.quien) : '') + '</small>' +
        ((a.lineas || []).length ? '<ul>' + a.lineas.map(function (l) { return '<li>' + esc(l) + '</li>'; }).join('') + '</ul>' : '') + '</li>';
    });
    return h + '</ul></section>';
  }

  function resumenTxt() {
    var sel = seleccionados();
    if (!sel.length) return '<div class="oe-resumen vacio">Elige al menos un destinatario.</div>';
    return '<div class="oe-resumen"><i class="bi bi-envelope-check" aria-hidden="true"></i> Se enviará a: ' + sel.map(function (d) { return esc(quienTxt(d)); }).join(' · ') + '.</div>';
  }

  function pintarResultado() {
    var r = S.res;
    var ok = r.enviados || 0, tot = (r.resultados || []).length;
    var cls = ok === tot ? '' : (ok ? ' ambar' : ' rojo');
    var h = '<div class="oe-res-cab"><span class="oe-res-ic' + cls + '"><i class="bi ' + (ok ? (ok === tot ? 'bi-check-lg' : 'bi-exclamation-lg') : 'bi-x-lg') + '"></i></span>' +
      '<h5>' + (ok === tot ? 'Listo, se envió' : (ok ? 'Se envió a ' + ok + ' de ' + tot : 'No salió ningún correo')) + '</h5>' +
      '<p style="color:#374151;margin:4px 0">Esto es lo que pasó con cada destinatario. Queda en la bitácora de la OT y en el Historial de Comunicaciones.</p></div><ul class="oe-res-lista">';
    (r.resultados || []).forEach(function (x, i) {
      var t = (r.textos || [])[i] || { texto: x.correo, ok: x.resultado === 'enviado' };
      h += '<li class="' + (t.ok ? 'ok' : 'no') + '"><i class="bi ' + (t.ok ? 'bi-check-circle-fill' : 'bi-exclamation-triangle-fill') + '" aria-hidden="true"></i><span>' + esc(t.texto) + '</span></li>';
    });
    h += '</ul>';
    if (!ok && r.error) h += '<div class="oe-alerta rojo" style="margin-top:12px"><i class="bi bi-info-circle"></i><span>' + esc(r.error) + '</span></div>';
    cuerpoEl().innerHTML = h;
    cuerpoEl().scrollTop = 0;
    pie();
  }

  function pintar() {
    var c = cuerpoEl();
    el.querySelector('#oeSub').textContent = (D.ot.numero || '') + (D.ot.cliente ? ' · ' + D.ot.cliente : '');
    if (S.res) { pintarResultado(); return; }
    if (!D.puede) {
      c.innerHTML = '<div class="oe-alerta"><i class="bi bi-lock-fill"></i><span>' + esc(D.razon || 'No se puede enviar esta OT todavía.') + '</span></div>' + htmlHistorial();
      pie();
      return;
    }
    var h = paso(1, seleccionados().length > 0, '¿A quién?', 'Prellenado con el cliente, quien creó la OT y los técnicos. Quita o agrega a quien quieras.', htmlDestinatarios());
    h += paso(2, true, 'Asunto y mensaje', 'Opcionales. Si no escribes un mensaje, el correo lleva solo el documento.',
      '<label class="oe-label" for="oeAsunto">Asunto</label><input id="oeAsunto" class="oe-in" maxlength="' + esc(D.max_asunto) + '" value="' + esc(S.asunto) + '">' +
      '<label class="oe-label" for="oeMensaje">Mensaje (opcional)</label><textarea id="oeMensaje" maxlength="' + esc(D.max_mensaje) + '" placeholder="Ej.: Te comparto la orden de trabajo ya firmada. Quedamos atentos.">' + esc(S.mensaje) + '</textarea>' +
      '<div class="oe-nota"><i class="bi bi-shield-lock-fill"></i> Los técnicos reciben el mismo correo, pero de tu mensaje se les quitan las líneas que hablen de plata, proveedores u OC. Ningún correo lleva montos.</div>');
    h += paso(3, seleccionados().length > 0, 'Revisa y envía', 'Aquí ves exactamente a quién le llegará.',
      '<div id="oeResumen">' + resumenTxt() + '</div>' +
      '<div style="margin-top:10px"><button type="button" class="oe-btn sec" data-oe-vista><i class="bi bi-eye"></i> ' + (S.vista ? 'Ocultar la vista previa' : 'Ver cómo queda') + '</button></div>' +
      (S.vista ? (S.vistaCargando ? '<div class="oe-vacio"><span class="spinner-border spinner-border-sm me-2"></span>Preparando la vista previa…</div>'
        : '<iframe class="oe-preview" id="oePreview" sandbox="" title="Vista previa del correo"></iframe>') : ''));
    h += htmlHistorial();
    c.innerHTML = h;
    if (S.vista && !S.vistaCargando) {
      var fr = c.querySelector('#oePreview');
      if (fr) fr.srcdoc = S.vistaHtml || '<p style="font-family:Arial;padding:16px">No se pudo preparar la vista previa.</p>';
    }
    pie();
  }

  function pie() {
    var p = pieEl();
    if (!p) return;
    if (S.res || !D || !D.puede) {
      p.innerHTML = '<button type="button" class="oe-btn sec" data-bs-dismiss="modal">Cerrar</button>';
      return;
    }
    var n = seleccionados().length;
    p.innerHTML = '<button type="button" class="oe-btn sec" data-bs-dismiss="modal">Cancelar</button>' +
      '<button type="button" class="oe-btn rojo" data-oe-enviar' + (n && !S.ocupado ? '' : ' disabled') + '>' +
      (S.ocupado ? '<span class="spinner-border spinner-border-sm"></span> Enviando…' : '<i class="bi bi-send-fill"></i> Enviar (' + n + ')') + '</button>';
  }

  function actualizarResumen() {
    var r = el.querySelector('#oeResumen');
    if (r) r.innerHTML = resumenTxt();
    pie();
  }

  function agregarManual() {
    var inp = el.querySelector('#oeNuevo');
    if (!inp) return;
    var v = (inp.value || '').trim().toLowerCase();
    if (!v) return;
    if (!RX_CORREO.test(v) || v.indexOf('..') >= 0) { toast('Ese correo no es válido', 'warning', 4000); return; }
    var ya = S.dest.filter(function (d) { return d.correo === v; })[0];
    if (ya) { ya.on = true; toast('Ese correo ya estaba en la lista: quedó marcado', 'info', 4000); pintar(); return; }
    if (S.dest.length >= D.maximo) { toast('Como máximo ' + D.maximo + ' destinatarios por envío.', 'warning', 5000); return; }
    S.dest.push({ correo: v, nombre: '', rol: 'otro', rol_txt: 'Otro destinatario', on: true, extra: true });
    pintar();
    var nuevo = el.querySelector('#oeNuevo');
    if (nuevo) nuevo.focus();
  }

  function onInput(ev) {
    var t = ev.target;
    if (!t) return;
    if (t.id === 'oeAsunto') { S.asunto = t.value; S.vistaHtml = ''; }
    else if (t.id === 'oeMensaje') { S.mensaje = t.value; S.vistaHtml = ''; }
  }

  function onClick(ev) {
    var t = ev.target;
    if (!t || !t.closest) return;
    var b = t.closest('[data-oe-dest]');
    if (b) { var d = S.dest[+b.getAttribute('data-oe-dest')]; if (d) { d.on = !d.on; pintar(); } return; }
    b = t.closest('[data-oe-quitar]');
    if (b) { S.dest.splice(+b.getAttribute('data-oe-quitar'), 1); pintar(); return; }
    b = t.closest('[data-oe-todo]');
    if (b) { var marcar = sinMarcar(); S.dest.forEach(function (x) { x.on = marcar; }); pintar(); return; }
    b = t.closest('[data-oe-agregar]');
    if (b) { agregarManual(); return; }
    b = t.closest('[data-oe-vista]');
    if (b) { vistaPrevia(); return; }
    b = t.closest('[data-oe-enviar]');
    if (b) { enviar(); return; }
  }

  function vistaPrevia() {
    if (S.vista) { S.vista = false; pintar(); return; }
    S.vista = true; S.vistaCargando = true; pintar();
    api('POST', '/correo/vista-previa', { asunto: S.asunto, mensaje: S.mensaje }).then(function (r) {
      S.vistaCargando = false;
      if (!r.ok) { S.vista = false; toast(r.error || 'No se pudo preparar la vista previa.', 'error'); pintar(); return; }
      S.vistaHtml = r.html || '';
      pintar();
    });
  }

  function enviar() {
    var sel = seleccionados();
    if (!sel.length || S.ocupado) return;
    S.ocupado = true; pie();
    api('POST', '/correo/enviar', { destinatarios: sel.map(function (d) { return d.correo; }), asunto: S.asunto, mensaje: S.mensaje }).then(function (r) {
      S.ocupado = false;
      if (r.resultados) { S.res = r; pintar(); return; }      // 200, 409 y 502 traen el detalle por destinatario
      toast(r.error || 'No se pudo enviar. Intenta de nuevo.', 'error');
      pie();
    });
  }

  function abrir() {
    asegurar();
    S = nuevoEstado();
    cuerpoEl().innerHTML = '<div class="oe-vacio"><span class="spinner-border spinner-border-sm me-2"></span>Cargando…</div>';
    pieEl().innerHTML = '';
    el.querySelector('#oeSub').textContent = '';
    modal.show();
    api('GET', '/correo/enviar').then(function (d) {
      if (!d.ok) { cuerpoEl().innerHTML = '<div class="oe-alerta rojo"><i class="bi bi-exclamation-triangle-fill"></i><span>' + esc(d.error || 'No se pudo cargar.') + '</span></div>'; pie(); return; }
      D = d;
      S.asunto = d.asunto || '';
      S.dest = (d.destinatarios || []).map(function (x) { return { correo: x.correo, nombre: x.nombre, rol: x.rol, rol_txt: x.rol_txt, on: true, extra: false }; });
      pintar();
    });
  }

  /* ═══════════════ 3) avisos de la creación (se pueden cerrar) ═══════════════ */
  function engancharCreacion() {
    var box = document.getElementById('oeCreacion');
    if (!box) return;
    var id = box.getAttribute('data-oe-id');
    var clave = 'oeCreacionVista:' + id;
    try { if (window.sessionStorage && sessionStorage.getItem(clave)) { box.hidden = true; return; } } catch (e) { /* sin almacenamiento: se muestra igual */ }
    box.addEventListener('click', function (ev) {
      if (ev.target && ev.target.closest && ev.target.closest('[data-oe-cerrar]')) {
        box.hidden = true;
        try { if (window.sessionStorage) sessionStorage.setItem(clave, '1'); } catch (e) { /* nada */ }
      }
    });
  }

  window.OTEnvio = { abrir: abrir };
  document.addEventListener('click', function (ev) {
    var b = ev.target && ev.target.closest ? ev.target.closest('[data-oe-abrir]') : null;
    if (b) { ev.preventDefault(); abrir(); }
  });
  function iniciar() { engancharAvisos(); engancharCreacion(); }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', iniciar); else iniciar();
})();

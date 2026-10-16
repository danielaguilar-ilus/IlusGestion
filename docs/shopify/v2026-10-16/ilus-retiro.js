/* ILUS Fitness · Formulario «Agenda tu retiro» (tienda) · versión 2026-10-12
   Cambios: hasta 5 documentos por solicitud (botón «Agregar documento») y la anticipación mínima que informa ILUS
   (Retiros → Horarios y alertas; se cuenta en días hábiles). Envía a la misma API de siempre; ILUS ya acepta varios documentos. */
(function () {
  var API = 'https://ilus-app-469212710544.southamerica-west1.run.app';
  var root = document.getElementById('ilus-rf');
  if (!root) return;
  var $ = function (id) { return document.getElementById(id); };
  var form = $('irfForm');
  var DIAS = ['dom', 'lun', 'mar', 'mié', 'jue', 'vie', 'sáb'];
  var MESES = ['enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio', 'julio', 'agosto', 'septiembre', 'octubre', 'noviembre', 'diciembre'];
  var dispo = null, cursor = null, fechaSel = '', slotSel = null, t0Servidor = 0, enviando = false;

  function esc(s) {
    return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) {
      return ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[c];
    });
  }
  function isoDia(iso) { var p = iso.split('-'); return new Date(Date.UTC(+p[0], +p[1] - 1, +p[2])); }
  function fechaTxt(iso) { var d = isoDia(iso); return DIAS[d.getUTCDay()] + ' ' + iso.slice(8, 10) + '/' + iso.slice(5, 7) + '/' + iso.slice(0, 4); }

  /* ── RUT, correo y celular ─────────────────────────────── */
  function rutLimpio(v) { return String(v || '').replace(/[^0-9kK]/g, '').toUpperCase(); }
  function rutValido(v) {
    var r = rutLimpio(v); if (r.length < 2) return false;
    var cuerpo = r.slice(0, -1), dv = r.slice(-1), suma = 0, mul = 2;
    if (!/^\d+$/.test(cuerpo)) return false;
    for (var i = cuerpo.length - 1; i >= 0; i--) { suma += (+cuerpo[i]) * mul; mul = mul === 7 ? 2 : mul + 1; }
    var res = 11 - (suma % 11), esp = res === 11 ? '0' : res === 10 ? 'K' : String(res);
    return esp === dv;
  }
  function rutFormato(v) {
    var r = rutLimpio(v); if (r.length < 2) return v;
    var cuerpo = r.slice(0, -1).replace(/\B(?=(\d{3})+(?!\d))/g, '.');
    return cuerpo + '-' + r.slice(-1);
  }
  function emailValido(v) { return /^[^\s@]+@[^\s@]+\.[a-zA-Z]{2,}$/.test(String(v || '').trim()); }
  var TYPOS = { 'gmial.com': 'gmail.com', 'gmai.com': 'gmail.com', 'gmail.co': 'gmail.com', 'gmail.cl': 'gmail.com', 'gamil.com': 'gmail.com', 'hotmial.com': 'hotmail.com', 'hotmail.co': 'hotmail.com', 'hotmal.com': 'hotmail.com', 'outlok.com': 'outlook.com', 'yaho.com': 'yahoo.com' };
  function telDigitos(v) {
    var d = String(v || '').replace(/\D/g, '');
    if (d.indexOf('56') === 0 && d.length === 11) d = d.slice(2);
    return d;
  }
  function telValido(v) { return /^9\d{8}$/.test(telDigitos(v)); }
  function telFormato(v) { var d = telDigitos(v); return /^9\d{8}$/.test(d) ? '+56 ' + d[0] + ' ' + d.slice(1, 5) + ' ' + d.slice(5) : v; }

  function marcar(inp, ok, errId) {
    inp.classList.toggle('is-bad', ok === false);
    inp.classList.toggle('is-good', ok === true);
    if (errId) $(errId).hidden = ok !== false;
  }
  function validarCampo(id) {
    var inp = $(id), v = inp.value.trim();
    if (id === 'irf_rut' || id === 'irf_prut') {
      if (!v) { marcar(inp, null, id + '_err'); return false; }
      var okR = rutValido(v); if (okR) inp.value = rutFormato(v);
      marcar(inp, okR, id + '_err'); return okR;
    }
    if (id === 'irf_email') {
      if (!v) { marcar(inp, null, 'irf_email_err'); return false; }
      var okE = emailValido(v); marcar(inp, okE, 'irf_email_err');
      var dom = v.split('@')[1] || '', sug = $('irf_email_sug');
      if (TYPOS[dom.toLowerCase()]) { sug.textContent = '¿Quisiste decir ' + v.split('@')[0] + '@' + TYPOS[dom.toLowerCase()] + '?'; sug.hidden = false; }
      else sug.hidden = true;
      return okE;
    }
    if (id === 'irf_tel' || id === 'irf_ptel') {
      if (!v) { marcar(inp, null, id + '_err'); return id === 'irf_ptel'; }
      var okT = telValido(v); if (okT) inp.value = telFormato(v);
      marcar(inp, okT, id + '_err'); return okT;
    }
    return !!v;
  }
  ['irf_rut', 'irf_prut', 'irf_email', 'irf_tel', 'irf_ptel'].forEach(function (id) {
    $(id).addEventListener('blur', function () { validarCampo(id); actualizar(); });
  });
  $('irf_email_sug').addEventListener('click', function () {
    var v = $('irf_email').value.trim(), dom = (v.split('@')[1] || '').toLowerCase();
    if (TYPOS[dom]) { $('irf_email').value = v.split('@')[0] + '@' + TYPOS[dom]; validarCampo('irf_email'); actualizar(); }
  });
  $('irf_tipo').addEventListener('change', function () {
    $('irf_num_help').textContent = this.value === 'pedido'
      ? 'Está en el correo de confirmación de tu pedido (ej: #1136 → escribe 1136).'
      : 'Está en la parte superior de tu factura o boleta.';
  });

  /* ── Más documentos: hasta 5 por solicitud (ILUS 2026-10-12) ── */
  var MAX_DOCS = 5, seqDoc = 1, OPCIONES_DOC = $('irf_tipo').innerHTML;
  function filasDoc() { return [].slice.call($('irf_docs_extra').querySelectorAll('[data-docrow]')); }
  function renumerarDocs() {
    filasDoc().forEach(function (f, i) {
      var n = i + 2;
      f.querySelector('[data-lbl-tipo]').firstChild.nodeValue = 'Tipo del documento ' + n + ' ';
      f.querySelector('[data-lbl-num]').firstChild.nodeValue = 'N° del documento ' + n + ' ';
      f.querySelector('[data-doc-del]').setAttribute('aria-label', 'Quitar el documento ' + n);
    });
    var total = 1 + filasDoc().length;
    $('irf_docadd').hidden = total >= MAX_DOCS;
    $('irf_docadd_help').textContent = total >= MAX_DOCS
      ? 'Llegaste al máximo de ' + MAX_DOCS + ' documentos por solicitud.'
      : '¿Vienes a retirar más de una compra? Agrega hasta ' + MAX_DOCS + ' documentos en esta misma solicitud.';
  }
  $('irf_docadd').addEventListener('click', function () {
    if (1 + filasDoc().length >= MAX_DOCS) return;
    seqDoc++;
    var f = document.createElement('div');
    f.className = 'irf-grid irf-docrow';
    f.setAttribute('data-docrow', '');
    f.innerHTML =
      '<div class="irf-c5"><label class="irf-lbl" data-lbl-tipo for="irf_tipo_' + seqDoc + '">Tipo del documento <span class="irf-req">*</span></label>' +
      '<select class="irf-in" id="irf_tipo_' + seqDoc + '" data-doc-tipo>' + OPCIONES_DOC + '</select></div>' +
      '<div class="irf-c7"><label class="irf-lbl" data-lbl-num for="irf_num_' + seqDoc + '">N° del documento <span class="irf-req">*</span></label>' +
      '<div class="irf-docnum"><input class="irf-in" id="irf_num_' + seqDoc + '" data-doc-num maxlength="60" inputmode="numeric" placeholder="Ej: 1137" autocomplete="off">' +
      '<button type="button" class="irf-docdel" data-doc-del>×</button></div></div>';
    f.querySelector('[data-doc-tipo]').value = $('irf_tipo').value;     // por defecto, el mismo tipo del primero
    $('irf_docs_extra').appendChild(f);
    renumerarDocs();
    try { f.querySelector('[data-doc-num]').focus(); } catch (x) { }
    actualizar();
  });
  $('irf_docs_extra').addEventListener('click', function (e) {
    var b = e.target && e.target.closest ? e.target.closest('[data-doc-del]') : null;
    if (!b) return;
    b.closest('[data-docrow]').remove();
    renumerarDocs();
    actualizar();
    try { $('irf_docadd').hidden ? $('irf_num').focus() : $('irf_docadd').focus(); } catch (x) { }
  });
  renumerarDocs();

  /* ── Tercero que retira ────────────────────────────────── */
  $('irf_auth').addEventListener('change', function () { $('irf_auth_box').hidden = !this.checked; declaracion(); actualizar(); });
  function declaracion() {
    var on = $('irf_auth').checked, rc = rutLimpio($('irf_rut').value), rp = rutLimpio($('irf_prut').value);
    var mostrar = on && rc && rp && rc !== rp && rutValido(rc) && rutValido(rp);
    $('irf_decl').hidden = !mostrar;
    if (mostrar) {
      $('irf_decl_cli').textContent = ($('irf_nombre').value.trim() || 'el cliente') + ' (' + rutFormato(rc) + ')';
      $('irf_decl_ter').textContent = ($('irf_pnom').value.trim() || 'otra persona') + ' (' + rutFormato(rp) + ')';
    }
    return mostrar;
  }
  ['irf_nombre', 'irf_pnom', 'irf_prut', 'irf_rut'].forEach(function (id) { $(id).addEventListener('input', declaracion); });

  /* ── Semáforo de pasos ─────────────────────────────────── */
  function pasoOk(n) {
    if (n === 1) return !!$('irf_num').value.trim() && filasDoc().every(function (f) { return !!f.querySelector('[data-doc-num]').value.trim(); });
    if (n === 2) return !!$('irf_nombre').value.trim() && rutValido($('irf_rut').value) && emailValido($('irf_email').value) && telValido($('irf_tel').value);
    if (n === 3) {
      if (!$('irf_auth').checked) return true;
      var ok = !!$('irf_pnom').value.trim() && rutValido($('irf_prut').value) && (!$('irf_ptel').value.trim() || telValido($('irf_ptel').value));
      return ok && (!declaracion() || $('irf_decl_ok').checked);
    }
    if (n === 4) return !!(fechaSel && slotSel);
    return true;
  }
  function actualizar() {
    var listos = 0;
    [1, 2, 3, 4, 5].forEach(function (n) {
      var sec = form.querySelector('[data-paso="' + n + '"]'), ok = pasoOk(n);
      var marcado = (n === 3) ? $('irf_auth').checked && ok : (n === 5) ? !!$('irf_obs').value.trim() : ok;
      sec.classList.toggle('is-ok', marcado);
      if (n !== 3 && n !== 5 && ok) listos++;
    });
    // 4 pasos obligatorios: documento, cliente, día/hora y la declaración
    if ($('irf_terms').checked) listos++;
    $('irf_terms_box').classList.toggle('is-ok', $('irf_terms').checked);
    $('irfProgBar').style.width = (listos / 4 * 100) + '%';
    $('irfProg').classList.toggle('is-ok', listos === 4);
    $('irfProgTxt').textContent = listos + ' de 4 pasos listos';
  }
  // Al corregir un campo se le quita el rojo, y la lista de avisos se oculta
  // (vuelve a revisarse al enviar)
  function alEditar(e) {
    var t = e.target;
    if (t && t.classList && t.classList.contains('irf-in') && String(t.value || '').trim()) t.classList.remove('is-bad');
    $('irf_alert').hidden = true;
    actualizar();
  }
  form.addEventListener('input', alEditar);
  form.addEventListener('change', alEditar);

  /* ── Agenda ────────────────────────────────────────────── */
  function cargarAgenda(mantener) {
    $('irf_cal_err').hidden = true; $('irf_cal').hidden = false;
    return fetch(API + '/retiros/api/disponibilidad-publica', { credentials: 'omit' })
      .then(function (r) { if (!r.ok) throw new Error('http ' + r.status); return r.json(); })
      .then(function (d) {
        if (!d || !d.dias || !d.from) throw new Error('sin datos');
        dispo = d;
        if (d.min_notice_hours && $('irf_antic')) $('irf_antic').textContent = String(d.min_notice_hours);
        if (d.ahora_ts && !t0Servidor) t0Servidor = d.ahora_ts;
        if (!cursor) cursor = { y: +d.from.slice(0, 4), m: +d.from.slice(5, 7) - 1 };
        avisos(); bodega(d);
        if (!mantener) { fechaSel = ''; slotSel = null; }
        pintarCal(); pintarSlots(); actualizar();
      })
      .catch(function () { $('irf_cal').hidden = true; $('irf_cal_err').hidden = false; });
  }
  $('irf_retry').addEventListener('click', function () { cargarAgenda(false); });

  function bodega(d) {
    if (!d.warehouse_addr) return;
    $('irf_bod_nom').textContent = d.warehouse_name || 'Bodega ILUS';
    $('irf_bod_dir').textContent = d.warehouse_addr;
    if (d.maps_url) $('irf_bod_map').href = d.maps_url; else $('irf_bod_map').hidden = true;
    $('irf_bodega').hidden = false;
  }
  function avisos() {
    var b = (dispo.bloqueos || []).slice(0, 6), box = $('irf_avisos');
    if (!b.length) { box.hidden = true; return; }
    box.innerHTML = '<strong>Días y horarios sin atención en bodega</strong><ul>' + b.map(function (x) {
      return '<li>' + esc(fechaTxt(x.fecha)) + (x.dia_completo ? ' · todo el día' : ' · ' + esc(x.desde) + '–' + esc(x.hasta)) + (x.motivo ? ' — ' + esc(x.motivo) : '') + '</li>';
    }).join('') + '</ul>';
    box.hidden = false;
  }
  function clave(y, m) { return y * 12 + m; }
  function pintarCal() {
    var y = cursor.y, m = cursor.m, desde = dispo.from, hasta = dispo.to;
    $('irf_mes').textContent = MESES[m] + ' ' + y;
    $('irf_prev').disabled = clave(y, m) <= clave(+desde.slice(0, 4), +desde.slice(5, 7) - 1);
    $('irf_next').disabled = clave(y, m) >= clave(+hasta.slice(0, 4), +hasta.slice(5, 7) - 1);
    var nd = new Date(Date.UTC(y, m + 1, 0)).getUTCDate(), off = (new Date(Date.UTC(y, m, 1)).getUTCDay() + 6) % 7, html = '';
    for (var i = 0; i < off; i++) html += '<span></span>';
    for (var d = 1; d <= nd; d++) {
      var iso = y + '-' + String(m + 1).padStart(2, '0') + '-' + String(d).padStart(2, '0');
      var dia = (iso >= desde && iso <= hasta) ? dispo.dias[iso] : null;
      if (!dia) { html += '<button type="button" class="irf-day is-out" disabled>' + d + '</button>'; continue; }
      // Día hábil pero sin ninguna hora que se pueda elegir (ej. mañana cuando
      // ya no se alcanza la anticipación mínima): se muestra como no disponible
      var libres = (dia.slots || []).filter(function (s) { return s.estado === 'disponible' || s.estado === 'ocupado'; }).length;
      var razon = !dia.disponible ? (dia.razon || 'Día no disponible') : (!libres ? 'Ya no quedan horas libres este día' : '');
      if (razon) {
        html += '<button type="button" class="irf-day is-off" data-off="' + esc(razon) + '" data-iso="' + iso + '" aria-label="' + d + ': ' + esc(razon) + '">' + d + '</button>';
        continue;
      }
      var pct = +dia.ocupacion_pct || 0, col = pct >= 80 ? '#dc2626' : pct >= 40 ? '#f59e0b' : '#16a34a';
      html += '<button type="button" class="irf-day' + (iso === fechaSel ? ' is-sel' : '') + '" data-iso="' + iso + '" aria-label="' + esc(fechaTxt(iso)) + '">' + d + '<i style="background:' + col + '"></i></button>';
    }
    $('irf_days').innerHTML = html;
  }
  $('irf_prev').addEventListener('click', function () { if (this.disabled) return; cursor.m--; if (cursor.m < 0) { cursor.m = 11; cursor.y--; } pintarCal(); });
  $('irf_next').addEventListener('click', function () { if (this.disabled) return; cursor.m++; if (cursor.m > 11) { cursor.m = 0; cursor.y++; } pintarCal(); });
  $('irf_days').addEventListener('click', function (e) {
    var b = e.target.closest('.irf-day'); if (!b || !b.getAttribute('data-iso')) return;
    if (b.getAttribute('data-off')) {
      fechaSel = ''; slotSel = null; pintarCal();
      $('irf_slots').innerHTML = '<div class="irf-note is-bad"><strong>' + esc(fechaTxt(b.getAttribute('data-iso'))) + ':</strong> ' + esc(b.getAttribute('data-off')) + '. Elige otro día.</div>';
      resumen(); actualizar(); return;
    }
    fechaSel = b.getAttribute('data-iso'); slotSel = null;
    pintarCal(); pintarSlots(); actualizar();
  });

  function pintarSlots() {
    var box = $('irf_slots');
    if (!fechaSel || !dispo || !dispo.dias[fechaSel]) { box.innerHTML = '<div class="irf-note">Elige primero un día en el calendario.</div>'; resumen(); return; }
    var slots = (dispo.dias[fechaSel].slots || []).filter(function (s) { return s.estado !== 'colacion'; });
    var corte = dispo.lunch_start || '12:30', man = [], tar = [];
    slots.forEach(function (s) { (s.time_from < corte ? man : tar).push(s); });
    function chips(lista) {
      return '<div class="irf-sgrid">' + lista.map(function (s) {
        var st = s.estado, cls = 'irf-slot', dis = '', tit = 'Disponible';
        if (st === 'ocupado') { cls += ' is-half'; tit = 'Medio cupo: queda 1 lugar'; }
        else if (st === 'bloqueado') { cls += ' is-block'; dis = ' disabled'; tit = 'Bloqueado por ILUS' + (s.razon ? ': ' + s.razon : ''); }
        else if (st !== 'disponible') { dis = ' disabled'; tit = st === 'completo' ? 'Sin cupos' : 'No disponible'; }
        if (slotSel && slotSel.time_from === s.time_from) cls += ' is-sel';
        return '<button type="button" class="' + cls + '" data-t="' + esc(s.time_from) + '" title="' + esc(tit) + '" aria-label="' + esc(s.time_from + ' — ' + tit) + '"' + dis + '>' + esc(s.time_from) + '</button>';
      }).join('') + '</div>';
    }
    var libres = slots.filter(function (s) { return s.estado === 'disponible' || s.estado === 'ocupado'; }).length;
    box.innerHTML = libres ? ((man.length ? '<div class="irf-sect">Mañana</div>' + chips(man) : '') + (tar.length ? '<div class="irf-sect">Tarde</div>' + chips(tar) : ''))
      : '<div class="irf-note is-bad">Este día ya no tiene horas libres. Elige otro día.</div>';
    resumen();
  }
  $('irf_slots').addEventListener('click', function (e) {
    var b = e.target.closest('.irf-slot'); if (!b || b.disabled) return;
    var t = b.getAttribute('data-t'), s = (dispo.dias[fechaSel].slots || []).filter(function (x) { return x.time_from === t; })[0];
    slotSel = (slotSel && slotSel.time_from === t) ? null : s;
    pintarSlots(); actualizar();
  });
  function resumen() {
    var el = $('irf_sum');
    if (fechaSel && slotSel) { el.innerHTML = 'Llegas el <b>' + esc(fechaTxt(fechaSel)) + '</b> a las <b>' + esc(slotSel.time_from) + '</b>'; el.classList.add('is-ok'); }
    else { el.textContent = fechaSel ? 'Ahora elige tu hora de llegada.' : 'Aún no eliges día y hora.'; el.classList.remove('is-ok'); }
  }

  /* ── Datos que vienen en el link de la página ─────────── */
  try {
    var qs = new URLSearchParams(window.location.search);
    var tipo = (qs.get('tipo') || '').toLowerCase();
    if (['pedido', 'factura', 'boleta'].indexOf(tipo) !== -1) { $('irf_tipo').value = tipo; $('irf_tipo').dispatchEvent(new Event('change')); }
    var pon = function (id, v) { if (v && !$(id).value) $(id).value = String(v).trim().slice(0, 160); };
    pon('irf_num', (qs.get('numero') || qs.get('pedido') || '').replace(/^#/, ''));
    pon('irf_nombre', qs.get('nombre')); pon('irf_email', qs.get('email'));
    pon('irf_tel', qs.get('telefono')); pon('irf_rut', qs.get('rut'));
    ['irf_rut', 'irf_email', 'irf_tel'].forEach(function (id) { if ($(id).value) validarCampo(id); });
  } catch (e) { }

  /* ── Envío ─────────────────────────────────────────────── */
  function mostrarErrores(lista) {
    var a = $('irf_alert');
    a.innerHTML = 'Revisa esto antes de enviar:<ul>' + lista.map(function (x) { return '<li>' + esc(x) + '</li>'; }).join('') + '</ul>';
    a.hidden = false;
  }
  function validarTodo() {
    var errores = [], primero = null;
    function falta(msg, el) { errores.push(msg); if (!primero) primero = el; }
    if (!$('irf_num').value.trim()) { falta('Escribe el N° del documento.', $('irf_num')); marcar($('irf_num'), false); }
    filasDoc().forEach(function (f, i) {
      var inp = f.querySelector('[data-doc-num]');
      if (!inp.value.trim()) { falta('Escribe el N° del documento ' + (i + 2) + ' o quítalo.', inp); marcar(inp, false); }
    });
    if (!$('irf_nombre').value.trim()) { falta('Escribe el nombre completo o razón social.', $('irf_nombre')); marcar($('irf_nombre'), false); }
    if (!validarCampo('irf_rut')) { falta('Revisa el RUT del cliente.', $('irf_rut')); marcar($('irf_rut'), false, 'irf_rut_err'); }
    if (!validarCampo('irf_email')) { falta('Revisa el correo.', $('irf_email')); marcar($('irf_email'), false, 'irf_email_err'); }
    if (!validarCampo('irf_tel')) { falta('Revisa el celular (9 XXXX XXXX).', $('irf_tel')); marcar($('irf_tel'), false, 'irf_tel_err'); }
    if ($('irf_auth').checked) {
      if (!$('irf_pnom').value.trim()) { falta('Escribe el nombre de quien retira.', $('irf_pnom')); marcar($('irf_pnom'), false); }
      if (!validarCampo('irf_prut')) { falta('Revisa el RUT de quien retira.', $('irf_prut')); marcar($('irf_prut'), false, 'irf_prut_err'); }
      if (!validarCampo('irf_ptel')) falta('Revisa el celular de quien retira.', $('irf_ptel'));
      if (declaracion() && !$('irf_decl_ok').checked) falta('Confirma que el cliente autoriza a la persona que retira.', $('irf_decl'));
    }
    if (!fechaSel || !slotSel) falta('Elige el día y tu hora de llegada.', $('irf_paso4'));
    if (!$('irf_terms').checked) falta('Marca la declaración de responsabilidad.', $('irf_terms_box'));
    return { errores: errores, primero: primero };
  }

  form.addEventListener('submit', function (e) {
    e.preventDefault();
    if (enviando) return;
    $('irf_alert').hidden = true;
    var v = validarTodo();
    if (v.errores.length) {
      mostrarErrores(v.errores);
      if (v.primero) { v.primero.scrollIntoView({ behavior: 'smooth', block: 'center' }); if (v.primero.focus) try { v.primero.focus({ preventScroll: true }); } catch (x) { } }
      return;
    }
    var fd = new FormData();
    var nombre = $('irf_nombre').value.trim(), auth = $('irf_auth').checked;
    fd.append('document_type', $('irf_tipo').value);
    fd.append('document_number', $('irf_num').value.trim());
    filasDoc().forEach(function (f) {
      var num = f.querySelector('[data-doc-num]').value.trim();
      if (num) { fd.append('document_type', f.querySelector('[data-doc-tipo]').value); fd.append('document_number', num); }
    });
    fd.append('invoice_total_amount', '0');
    fd.append('customer_name', nombre);
    fd.append('customer_rut', $('irf_rut').value.trim());
    fd.append('contact_email', $('irf_email').value.trim().toLowerCase());
    fd.append('contact_phone', $('irf_tel').value.trim());
    fd.append('auth_active', auth ? '1' : '0');
    fd.append('contact_name', auth ? $('irf_pnom').value.trim() : nombre);
    if (auth) {
      fd.append('pickup_person_name', $('irf_pnom').value.trim());
      fd.append('pickup_person_rut', $('irf_prut').value.trim());
      fd.append('pickup_person_phone', $('irf_ptel').value.trim());
      fd.append('pickup_person_relation', $('irf_prel').value);
      if ($('irf_decl_ok').checked) fd.append('acepta_tercero', '1');
    }
    fd.append('requested_date', fechaSel);
    fd.append('requested_time_from', slotSel.time_from);
    fd.append('requested_time_to', slotSel.time_to);
    fd.append('total_packages', '0');
    fd.append('observations', $('irf_obs').value.trim());
    fd.append('accept_terms', 'on');
    fd.append('hp_ilus_extra', $('irf_hp').value);
    if (t0Servidor) fd.append('ilus_t0', String(t0Servidor));

    enviando = true; $('irf_send').disabled = true; $('irf_send_txt').textContent = 'Enviando…';
    var ctl = window.AbortController ? new AbortController() : null;
    var reloj = ctl ? setTimeout(function () { ctl.abort(); }, 45000) : null;
    fetch(API + '/retiros/solicitar', { method: 'POST', body: fd, headers: { 'Accept': 'application/json' }, credentials: 'omit', signal: ctl ? ctl.signal : undefined })
      .then(function (r) {
        return r.json().catch(function () { return null; }).then(function (d) { return { st: r.status, d: d }; });
      })
      .then(function (res) {
        var d = res.d;
        if (d && d.ok && d.code) { exito(d); return; }
        if (res.st === 429) { mostrarErrores(['Hiciste muchos intentos seguidos. Espera unos minutos y vuelve a enviar.']); return; }
        var errs = (d && d.errors && d.errors.length) ? d.errors : [(d && d.error) || 'No pudimos registrar tu solicitud. Inténtalo de nuevo.'];
        mostrarErrores(errs);
        if (/cupo|lleno|slot|horario/i.test(errs.join(' '))) {
          cargarAgenda(true).then(function () { slotSel = null; pintarSlots(); actualizar(); });
          $('irf_paso4').scrollIntoView({ behavior: 'smooth', block: 'start' });
        } else {
          $('irf_alert').scrollIntoView({ behavior: 'smooth', block: 'center' });
        }
      })
      .catch(function (err) {
        mostrarErrores([err && err.name === 'AbortError'
          ? 'La conexión está muy lenta. Vuelve a enviar: si tu solicitud ya había quedado registrada, no se duplica.'
          : 'Sin conexión con ILUS. Revisa tu internet y vuelve a enviar (no se duplica).']);
        $('irf_alert').scrollIntoView({ behavior: 'smooth', block: 'center' });
      })
      .then(function () {
        if (reloj) clearTimeout(reloj);
        enviando = false; $('irf_send').disabled = false; $('irf_send_txt').textContent = 'Enviar solicitud de retiro';
      });
  });

  function exito(d) {
    form.hidden = true;
    var code = String(d.code).replace(/[^A-Za-z0-9-]/g, '');
    $('irf_code').textContent = code;
    $('irf_ok_txt').textContent = 'Te enviamos un correo a ' + $('irf_email').value.trim().toLowerCase() + ' con el enlace de seguimiento. Te confirmaremos la fecha en menos de 24 horas.';
    var url = String(d.tracking_url || '');
    if (url.charAt(0) === '/') url = API + url;
    if (/^https:\/\//.test(url)) $('irf_track').href = url; else $('irf_track').hidden = true;
    $('irf_ok').hidden = false;
    $('irfProgBar').style.width = '100%'; $('irfProg').classList.add('is-ok'); $('irfProgTxt').textContent = 'Solicitud enviada';
    $('irf_ok').scrollIntoView({ behavior: 'smooth', block: 'center' });
    try { $('irf_ok').focus({ preventScroll: true }); } catch (x) { }
  }
  $('irf_copy').addEventListener('click', function () {
    var code = $('irf_code').textContent, btn = this;
    var listo = function () { btn.textContent = 'Código copiado'; };
    if (navigator.clipboard && navigator.clipboard.writeText) navigator.clipboard.writeText(code).then(listo, function () { btn.textContent = code; });
    else btn.textContent = code;
  });

  cargarAgenda(false);
  actualizar();
})();

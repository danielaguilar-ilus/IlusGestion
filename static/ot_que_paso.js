/* ══════════════════════════════════════════════════════════════════════════════════════════════════════════════
   ot_que_paso.js — «¿Qué pasó?» de la orden de trabajo (desviaciones, al estilo de Retiros).
   2026-10-20 (Daniel, caso real Aurum Fit Girls Spa, OT-2026-00171: 2 accesorios no llegaron por falta de stock y la OT no se
   podía completar): «así como planteamos en Retiros… ¿pasó algo?».
   Cuando falta stock: se cierra lo que sí se hizo y los equipos pendientes pasan a una OT HIJA «pendiente», sin fecha, con el MISMO
   documento (no se cobra de nuevo). La usa el técnico asignado (también el externo) y la gestión. Avisos: gestión por la campana y
   bodega por correo INTERNO; al cliente nunca se le escribe. Queda quién, cuándo, motivo y detalle.
   El sistema SUGIERE (equipos ya marcados «falta stock») y la persona DECIDE: lo que se ve elegido es lo que viaja (REGLA #32).
   Sin alert/confirm/prompt nativos (REGLA #1: ilusConfirm / ilusToast). Botones de 44 px, pantalla completa en el celular.
   Un técnico nunca ve plata, documentos ni cuentas (REGLAS #19/#26): el servidor no se los manda.
   API: GET/POST /ot/api/<vid>/que-paso   ·   window.OTQuePaso.abrir(tipoInicial)
   ══════════════════════════════════════════════════════════════════════════════════════════════════════════════ */
(function () {
  'use strict';
  var CFG = window.OTD_QP_CFG || {};
  var VID = CFG.vid;
  if (!VID) return;

  function esc(s) {
    return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
    });
  }
  function toast(m, t, d) { try { if (window.ilusToast) window.ilusToast(m, { type: t || 'info', duration: d || 6000 }); } catch (e) { /* nada */ } }
  function api(method, cuerpo) {
    var init = { credentials: 'same-origin', method: method, headers: { 'X-Requested-With': 'XMLHttpRequest', 'Accept': 'application/json' } };
    if (cuerpo !== undefined) { init.headers['Content-Type'] = 'application/json'; init.body = JSON.stringify(cuerpo); }
    return fetch('/ot/api/' + VID + '/que-paso', init).then(function (r) {
      return r.json().catch(function () { return {}; }).then(function (j) {
        if (!j.ok && !j.error) j.error = r.status === 403 ? 'No tienes permiso para esta acción en esta OT.' : 'No se pudo completar. Intenta de nuevo.';
        j._http = r.status;
        return j;
      });
    }).catch(function () { return { ok: false, error: 'Sin conexión. Revisa la señal e intenta de nuevo: lo que escribiste sigue aquí.' }; });
  }

  var D = null;                                   // lo que mandó el servidor
  var S = nuevoEstado();
  var ocupado = false;
  var el = null, modal = null;

  function nuevoEstado() { return { tipo: '', motivo: '', accion: '', detalle: '', equipos: {}, bodega: true, listo: null, sinRecargar: false }; }

  /* ───────────────────────── el modal (se crea una vez y se vuelve a pintar) ───────────────────────── */
  function asegurar() {
    if (el) return;
    el = document.createElement('div');
    el.className = 'modal fade qp-modal';
    el.id = 'otdQuePasoModal';
    el.tabIndex = -1;
    el.setAttribute('aria-hidden', 'true');
    el.setAttribute('aria-labelledby', 'qpTitulo');
    el.innerHTML = '<div class="modal-dialog modal-lg modal-dialog-centered modal-dialog-scrollable modal-fullscreen-sm-down"><div class="modal-content">' +
      '<div class="modal-header qp-mh"><div><h5 class="modal-title" id="qpTitulo"><i class="bi bi-signpost-split"></i> ¿Qué pasó con esta orden?</h5>' +
      '<div class="qp-sub" id="qpSub"></div></div><button type="button" class="btn-close btn-close-white" data-bs-dismiss="modal" aria-label="Cerrar"></button></div>' +
      '<div class="modal-body" id="qpCuerpo"></div>' +
      '<div class="modal-footer" id="qpPie"></div></div></div>';
    document.body.appendChild(el);
    modal = new bootstrap.Modal(el);
    el.addEventListener('click', onClick);
    el.addEventListener('input', function (ev) { if (ev.target.id === 'qpDetalle') { S.detalle = ev.target.value; pie(); } });
    el.addEventListener('change', function (ev) { if (ev.target.id === 'qpBodega') S.bodega = ev.target.checked; });
    el.addEventListener('show.bs.modal', function () { document.body.classList.add('qp-abierto'); });
    el.addEventListener('hidden.bs.modal', function () {
      document.body.classList.remove('qp-abierto');
      if (S.listo && !S.sinRecargar) setTimeout(function () { location.reload(); }, 150);     // la ficha se repinta con lo nuevo
    });
  }
  function cuerpoEl() { return el.querySelector('#qpCuerpo'); }
  function pieEl() { return el.querySelector('#qpPie'); }

  /* ───────────────────────── helpers de estado ───────────────────────── */
  function acciones() { return (D.catalogo.acciones_por_tipo[S.tipo] || []); }
  function accion() { return S.accion; }
  function creaPendiente() { var a = D.catalogo.acciones[S.accion]; return !!(a && a.crea_pendiente); }
  function elegidos() { return Object.keys(S.equipos).filter(function (k) { return S.equipos[k]; }).map(Number); }
  function elegibles() { return (D.equipos || []).filter(function (e) { return e.elegible; }); }
  function sinElegir() { var e = elegibles(); return e.some(function (x) { return !S.equipos[x.maquina_id]; }); }

  function listo() {
    if (!S.tipo || !S.motivo || !S.accion) return false;
    if (S.motivo === 'otro' && (S.detalle || '').trim().length < (D.catalogo.detalle_min_otro || 5)) return false;
    if (creaPendiente() && !elegidos().length) return false;
    return true;
  }

  /* ───────────────────────── dibujo ───────────────────────── */
  function paso(n, ok, titulo, ayuda, cuerpo) {
    return '<section class="qp-paso' + (ok ? ' is-ok' : '') + '"><div class="qp-paso-cab"><span class="qp-circ">' + (ok ? '<i class="bi bi-check-lg"></i>' : n) + '</span>' +
      '<div><h6>' + esc(titulo) + '</h6>' + (ayuda ? '<small>' + esc(ayuda) + '</small>' : '') + '</div></div>' + cuerpo + '</section>';
  }
  function colorCls(c) { return { rojo: 'c-rojo', ambar: 'c-ambar', azul: 'c-azul', verde: 'c-verde' }[c] || ''; }

  function htmlEquipos() {
    var h = '';
    var el_ = elegibles();
    if (!(D.equipos || []).length) {
      return '<div class="qp-vacio"><i class="bi bi-info-circle"></i> Esta orden no tiene equipos con tareas sin hacer que se puedan dejar pendientes.</div>';
    }
    if (el_.length > 1) {
      h += '<div class="qp-toolbar"><button type="button" class="qp-todo" data-qp-todo><i class="bi bi-check-all"></i> ' +
        (sinElegir() ? 'Marcar todo' : 'Desmarcar todo') + '</button><span class="qp-cuenta">' + elegidos().length + ' de ' + el_.length + ' elegidos</span></div>';
    }
    h += '<div class="qp-eqs">';
    D.equipos.forEach(function (e) {
      var on = !!S.equipos[e.maquina_id];
      var sub = [];
      if (e.sku) sub.push('SKU ' + e.sku);
      if (e.serie) sub.push('S/N ' + e.serie);
      var tar = e.pendientes + (e.pendientes === 1 ? ' tarea sin hacer' : ' tareas sin hacer') + (e.total ? ' de ' + e.total : '');
      h += '<button type="button" class="qp-eq' + (on ? ' on' : '') + (e.elegible ? '' : ' off') + '" data-qp-eq="' + e.maquina_id + '" aria-pressed="' + on + '"' + (e.elegible ? '' : ' disabled') + '>' +
        '<span class="qp-chk"><i class="bi ' + (on ? 'bi-check-square-fill' : 'bi-square') + '"></i></span>' +
        '<span class="qp-eq-tx"><b>' + esc(e.nombre) + '</b>' +
        (sub.length ? '<small>' + esc(sub.join(' · ')) + '</small>' : '') +
        '<small>' + esc(e.elegible ? tar : e.motivo_no) + '</small>' +
        (e.estado_pe === 'falta_stock' && e.elegible ? '<span class="qp-tag"><i class="bi bi-box-seam"></i> Ya marcado «falta stock»</span>' : '') +
        '</span></button>';
    });
    return h + '</div>';
  }

  function htmlHistorial() {
    var h = '';
    var pen = D.pendientes || [];
    if (pen.length) {
      h += '<div class="qp-pen"><b><i class="bi bi-hourglass-split"></i> OT pendientes por falta de stock</b><ul>';
      pen.forEach(function (p) {
        h += '<li>' + (p.url ? '<a href="' + esc(p.url) + '"><b>' + esc(p.numero_ot) + '</b></a>' : '<b>' + esc(p.numero_ot) + '</b>') +
          (p.sin_fecha ? ' · <span class="qp-chip-ambar">Sin fecha: espera el stock</span>' : '') + '</li>';
      });
      h += '</ul></div>';
    }
    var reg = D.registradas || [];
    if (reg.length) {
      h += '<section class="qp-paso"><div class="qp-paso-cab"><span class="qp-circ qp-gris"><i class="bi bi-clock-history"></i></span><div><h6>Ya registrado en esta orden</h6></div></div><ul class="qp-hist">';
      reg.forEach(function (r) {
        var eq = (r.equipos || []).map(function (e) {
          var det = [e.sku ? 'SKU ' + e.sku : '', e.serie ? 'S/N ' + e.serie : ''].filter(Boolean).join(' · ');
          return e.nombre + (det ? ' (' + det + ')' : '');
        }).join('; ');
        h += '<li class="t-' + esc(r.tipo) + '"><b>' + esc(r.titulo) + '</b> · ' + esc(r.motivo) + (r.detalle ? ' — ' + esc(r.detalle) : '') +
          (eq ? '<br><span class="qp-eqs-txt">' + esc(eq) + '</span>' : '') +
          (r.hija_numero ? '<br>OT pendiente: ' + (r.hija_url ? '<a href="' + esc(r.hija_url) + '"><b>' + esc(r.hija_numero) + '</b></a>' : '<b>' + esc(r.hija_numero) + '</b>') +
            (r.hija_sin_fecha ? ' · <span class="qp-chip-ambar">sin fecha</span>' : '') : '') +
          '<br><small>' + esc(r.accion) + ' · ' + esc(r.quien) + ' · ' + esc(r.cuando) +
          (r.aviso_bodega === 'enviado' ? ' · se avisó a bodega' : (r.aviso_bodega === 'bloqueado' ? ' · el correo a bodega no salió' : '')) + '</small></li>';
      });
      h += '</ul></section>';
    }
    return h;
  }

  function pintar() {
    var c = cuerpoEl();
    var sub = el.querySelector('#qpSub');
    sub.textContent = (D.ot.numero_ot || '') + (D.ot.estado ? ' · ' + String(D.ot.estado).replace(/_/g, ' ') : '');
    if (S.listo) { c.innerHTML = S.listo; pie(); return; }
    var h = '';
    if (!D.puede) {
      h += '<div class="qp-alerta"><i class="bi bi-lock-fill"></i><span>' + (D.terminada
        ? 'La orden ya está terminada o esperando su aprobación: queda en solo lectura. Aquí puedes ver lo que se registró.'
        : 'No tienes permiso para registrar sobre esta orden. Aquí puedes ver lo que se registró.') + '</span></div>';
      h += htmlHistorial() || '<div class="qp-vacio"><i class="bi bi-info-circle"></i> Todavía no se registró nada en esta orden.</div>';
      c.innerHTML = h; pie(); return;
    }
    /* Paso 1 · qué pasó */
    h += paso(1, !!S.tipo, '¿Qué pasó?', 'Elige la situación.', '<div class="qp-grid">' + D.catalogo.tipos.map(function (t) {
      return '<button type="button" class="qp-card ' + colorCls(t.color) + '" data-qp-tipo="' + esc(t.clave) + '" aria-pressed="' + (S.tipo === t.clave) + '">' +
        '<i class="bi ' + esc(t.icono) + '" aria-hidden="true"></i><span><b>' + esc(t.titulo) + '</b><small>' + esc(t.ayuda) + '</small></span></button>';
    }).join('') + '</div>');
    if (S.tipo) {
      /* Paso 2 · motivo (obligatorio, de una lista) */
      h += paso(2, !!S.motivo && !(S.motivo === 'otro' && (S.detalle || '').trim().length < 5), 'Motivo', 'Obligatorio. Con «Otro motivo», cuéntalo en el detalle.',
        '<div class="qp-motivos">' + (D.catalogo.motivos[S.tipo] || []).map(function (m) {
          return '<button type="button" class="qp-chip" data-qp-motivo="' + esc(m.clave) + '" aria-pressed="' + (S.motivo === m.clave) + '">' + esc(m.texto) + '</button>';
        }).join('') + '</div><label class="qp-label" for="qpDetalle">Detalle ' + (S.motivo === 'otro' ? '(obligatorio)' : '(opcional)') + '</label>' +
        '<textarea id="qpDetalle" maxlength="' + D.catalogo.detalle_max + '" placeholder="' + (S.tipo === 'falta_stock' ? 'Ej.: bodega avisó que el stock llega la próxima semana.' : 'Ej.: el cliente no estaba y no contestó el teléfono.') + '">' + esc(S.detalle) + '</textarea>');
      /* Paso 3 · qué hacemos */
      h += paso(3, !!S.accion, '¿Qué hacemos?', 'Decide quien está a cargo de la orden.', '<div class="qp-grid">' + acciones().map(function (k) {
        var a = D.catalogo.acciones[k];
        var cls = a.crea_pendiente ? 'c-ambar' : (k === 'reagendar' ? 'c-azul' : 'c-verde');
        var ic = a.crea_pendiente ? 'bi-box-seam' : (k === 'reagendar' ? 'bi-calendar2-week' : 'bi-journal-text');
        return '<button type="button" class="qp-card ' + cls + '" data-qp-accion="' + esc(k) + '" aria-pressed="' + (S.accion === k) + '">' +
          '<i class="bi ' + ic + '" aria-hidden="true"></i><span><b>' + esc(a.titulo) + '</b><small>' + esc(a.ayuda) + '</small></span></button>';
      }).join('') + '</div>');
      /* Paso 4 · qué quedó pendiente */
      var n = 4;
      if (creaPendiente()) {
        h += paso(n, elegidos().length > 0, '¿Qué quedó pendiente?', 'Elige los equipos que no se pudieron terminar. Solo pasan sus tareas sin hacer; lo hecho se queda en esta orden.', htmlEquipos());
        n++;
      }
      /* Paso 5 · avisos */
      if (S.accion) {
        var bod = D.bodega || {};
        var avisoB = creaPendiente()
          ? '<label class="qp-aviso' + (bod.hay ? '' : ' is-off') + '"><input type="checkbox" id="qpBodega"' + (S.bodega && bod.hay ? ' checked' : '') + (bod.hay ? '' : ' disabled') + '>' +
            '<span><b>Avisar a bodega</b><small>' + (bod.hay
              ? 'Correo interno con los productos que faltan' + ((bod.correos || []).length ? ' a ' + esc(bod.correos.join(', ')) : '') + '.'
              : 'Bodega no tiene correo configurado todavía (Retiros → Configuración → Equipo y alertas): la gestión recibe el aviso por la campana.') + '</small></span></label>'
          : '';
        h += paso(n, true, 'Avisos', 'Nada le llega al cliente.',
          '<div class="qp-aviso fijo"><i class="bi bi-bell-fill"></i><span><b>Aviso a la gestión</b><small>Campana para quien creó la orden, los supervisores y los ejecutivos de servicio técnico.</small></span></div>' +
          avisoB + '<div class="qp-aviso fijo no"><i class="bi bi-person-slash"></i><span><b>Al cliente no se le envía nada</b><small>Ni correo ni mensaje: esto es solo interno.</small></span></div>');
      }
    }
    h += htmlHistorial();
    c.innerHTML = h;
    pie();
  }

  function pie() {
    var p = pieEl();
    if (!p) return;
    if (S.listo || !D || !D.puede) {
      p.innerHTML = '<button type="button" class="btn btn-outline-secondary qp-btn" data-bs-dismiss="modal">Cerrar</button>';
      return;
    }
    var txt = { cerrar_y_pendiente: 'Dejar pendiente y avisar', reagendar: 'Registrar y reagendar', solo_registrar: 'Registrar' }[S.accion] || 'Registrar';
    p.innerHTML = '<button type="button" class="btn btn-outline-secondary qp-btn" data-bs-dismiss="modal">Volver</button>' +
      '<button type="button" class="btn btn-danger qp-btn" data-qp-guardar' + (listo() && !ocupado ? '' : ' disabled') + '>' + esc(txt) + '</button>';
  }

  /* ───────────────────────── eventos ───────────────────────── */
  function preseleccionar() {
    S.equipos = {};
    (D.equipos || []).forEach(function (e) { if (e.preseleccionado) S.equipos[e.maquina_id] = true; });
  }
  function onClick(ev) {
    var t = ev.target;
    if (!t || !t.closest) return;
    var b = t.closest('[data-qp-tipo]');
    if (b) {
      if (S.tipo !== b.dataset.qpTipo) {
        S.tipo = b.dataset.qpTipo; S.motivo = ''; S.accion = '';
        var a = acciones();
        if (a.length) S.accion = a[0];               // sugiere la primera; la persona puede cambiarla
        if (creaPendiente()) preseleccionar();
        S.bodega = true;
      }
      pintar(); return;
    }
    b = t.closest('[data-qp-motivo]');
    if (b) { S.motivo = b.dataset.qpMotivo; pintar(); if (S.motivo === 'otro') { var ta = el.querySelector('#qpDetalle'); if (ta) ta.focus(); } return; }
    b = t.closest('[data-qp-accion]');
    if (b) { S.accion = b.dataset.qpAccion; if (creaPendiente() && !elegidos().length) preseleccionar(); pintar(); return; }
    b = t.closest('[data-qp-eq]');
    if (b && !b.disabled) { var id = b.dataset.qpEq; S.equipos[id] = !S.equipos[id]; pintar(); return; }
    b = t.closest('[data-qp-todo]');
    if (b) { var marcar = sinElegir(); elegibles().forEach(function (e) { S.equipos[e.maquina_id] = marcar; }); pintar(); return; }
    b = t.closest('[data-qp-guardar]');
    if (b) { guardar(); return; }
    b = t.closest('[data-qp-reagendar]');
    if (b) { S.sinRecargar = true; modal.hide(); setTimeout(irAReagendar, 350); return; }
  }

  function irAReagendar() {
    if (typeof window.otdAbrirReagendar === 'function' && document.getElementById('otdReagendarBox')) window.otdAbrirReagendar();
    else toast('Pídele a la gestión que reagende esta orden: tú no tienes el botón de Reagendar.', 'warning');
  }

  function exito(r) {
    var h = '<div class="qp-ok"><span class="qp-ok-ic"><i class="bi bi-check-lg"></i></span><h5>Listo, quedó registrado</h5>';
    if (r.pendiente) {
      h += '<p>' + (r.pendiente.nueva ? 'Se creó la OT pendiente ' : 'Se sumaron los equipos a la OT pendiente ') + '<b>' + esc(r.pendiente.numero_ot) + '</b>, <b>sin fecha</b> hasta que llegue el stock. ' +
        'Tiene el mismo documento y <b>no se cobra de nuevo</b>.</p>';
      if ((r.equipos || []).length) {
        h += '<ul class="qp-ok-lista">' + r.equipos.map(function (e) {
          var det = [e.sku ? 'SKU ' + e.sku : '', e.serie ? 'S/N ' + e.serie : ''].filter(Boolean).join(' · ');
          return '<li>' + esc(e.nombre) + (det ? ' <small>(' + esc(det) + ')</small>' : '') + '</li>';
        }).join('') + '</ul>';
      }
      h += '<div class="qp-aviso fijo"><i class="bi bi-arrow-right-circle-fill"></i><span><b>Lo que sigue</b><small>Termina y firma esta orden con lo que sí se hizo: los equipos pendientes ya no frenan la firma.</small></span></div>';
      var av = r.avisos || {};
      h += '<p class="qp-ok-av"><i class="bi bi-bell-fill"></i> ' + (av.gestion ? 'Se avisó a la gestión (' + av.gestion + ').' : 'La gestión verá el registro en la bitácora.') +
        (av.bodega === 'enviado' ? ' Se avisó a bodega por correo.' : (av.bodega === 'bloqueado' ? ' El correo a bodega no salió (revisa Comunicaciones).' : (av.bodega === 'sin_correos' ? ' Bodega no tiene correo configurado.' : ''))) + '</p>';
      if (r.pendiente.url) h += '<a class="btn btn-outline-dark qp-btn" href="' + esc(r.pendiente.url) + '"><i class="bi bi-box-arrow-up-right"></i> Abrir ' + esc(r.pendiente.numero_ot) + '</a>';
    } else if (r.siguiente === 'reagendar') {
      h += '<p>Quedó escrito qué pasó. Ahora elige la fecha nueva.</p><button type="button" class="btn btn-danger qp-btn" data-qp-reagendar><i class="bi bi-calendar-event"></i> Reagendar</button>';
    } else {
      h += '<p>Quedó en la bitácora como evidencia; la orden sigue igual.</p>';
    }
    h += '</div>';
    S.listo = h;
  }

  function guardar() {
    if (!listo() || ocupado) return;
    ocupado = true; pie();
    var cuerpo = { tipo: S.tipo, motivo: S.motivo, detalle: S.detalle, accion: S.accion, equipos: creaPendiente() ? elegidos() : [], avisar_bodega: !!(S.bodega && D.bodega && D.bodega.hay) };
    var pregunta = creaPendiente()
      ? window.ilusConfirm({ title: '¿Dejar estos equipos en una OT pendiente?',
          message: (cuerpo.equipos.length === 1 ? 'El equipo elegido queda' : 'Los ' + cuerpo.equipos.length + ' equipos elegidos quedan') + ' «falta stock» y deja' + (cuerpo.equipos.length === 1 ? '' : 'n') + ' de frenar la firma de esta orden. Se crea una OT hija sin fecha, con el mismo documento: no se cobra de nuevo.',
          sub: 'Al cliente <strong>no</strong> se le envía ningún correo.', subHtml: true, okLabel: 'Sí, dejar pendiente', cancelLabel: 'Volver', type: 'question' })
      : Promise.resolve(true);
    Promise.resolve(pregunta).then(function (ok) {
      if (!ok) { ocupado = false; pie(); return null; }
      return api('POST', cuerpo).then(function (r) {
        ocupado = false;
        if (!r.ok) { toast(r.error || 'No se pudo registrar. Reintenta.', 'error'); pie(); return; }
        exito(r);
        pintar();
        if (r.siguiente === 'reagendar') { /* el botón del éxito lo lleva */ }
        D = D || {};
        api('GET').then(function (d) { if (d && d.ok) { D = d; } });
      });
    });
  }

  /* ───────────────────────── entrada ───────────────────────── */
  function abrir(tipoInicial) {
    asegurar();
    S = nuevoEstado();
    ocupado = false;
    cuerpoEl().innerHTML = '<div class="qp-vacio"><span class="spinner-border spinner-border-sm me-2"></span>Cargando…</div>';
    pieEl().innerHTML = '';
    modal.show();
    api('GET').then(function (d) {
      if (!d.ok) { cuerpoEl().innerHTML = '<div class="qp-alerta"><i class="bi bi-exclamation-triangle-fill"></i><span>' + esc(d.error || 'No se pudo cargar.') + '</span></div>'; return; }
      D = d;
      if (tipoInicial && d.puede && d.catalogo.tipos.some(function (t) { return t.clave === tipoInicial; })) {
        S.tipo = tipoInicial;
        var a = acciones();
        if (a.length) S.accion = a[0];
        if (creaPendiente()) preseleccionar();
      }
      pintar();
    });
  }

  window.OTQuePaso = { abrir: abrir };
  document.addEventListener('click', function (ev) {
    var b = ev.target && ev.target.closest ? ev.target.closest('[data-qp-abrir]') : null;
    if (b) abrir(b.getAttribute('data-qp-abrir') || '');
  });
})();

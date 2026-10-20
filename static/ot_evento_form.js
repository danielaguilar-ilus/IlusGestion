/* 🧩 2026-10-20 — Formulario «Evento» (crear / editar / cancelar / reactivar), una sola pieza para el asistente de crear OT
   y las pantallas /ot/eventos (Daniel, 19-oct: «clasificar las OT por EVENTO»). Un evento es una CARPETA (nombre, lugar,
   fechas) que agrupa OT; no es un documento: su «documento de referencia» es solo informativo (REGLA #24).
   Se abre como ventana apilada propia (nada de alert/confirm/prompt nativos, REGLA #1). Lugar con Google Places (el mismo
   ilusPlacesAutocomplete del resto del sistema). API: POST /ot/api/eventos · PUT /ot/api/eventos/<id> (editar, cancelar con
   motivo ≥ 10, reactivar). Solo gestión: el servidor responde 403 a un técnico (REGLA #26) y este formulario nunca se
   ofrece a uno.
   Uso: OTEventoForm.abrir({evento?, cliente?, host?, onGuardado(ev)}). */
(function () {
  'use strict';
  function esc(s) {
    return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
    });
  }
  function api(url, metodo, cuerpo) {
    var init = { credentials: 'same-origin', method: metodo || 'GET', headers: {} };
    if (cuerpo !== undefined) { init.headers['Content-Type'] = 'application/json'; init.body = JSON.stringify(cuerpo); }
    return fetch(url, init).then(function (r) {
      return r.json().catch(function () { return null; }).then(function (j) {
        return { http: r.status, ok: r.ok && !(j && j.ok === false), j: j || {} };
      });
    }).catch(function () { return { http: 0, ok: false, j: { error: 'No hay conexión con el servidor. Reintenta.' } }; });
  }
  function toast(m, t) { if (window.ilusToast) window.ilusToast(m, { type: t || 'info' }); }
  function pick(comps, tipos) {
    for (var i = 0; i < tipos.length; i++) {
      var c = (comps || []).find(function (x) { return (x.types || []).indexOf(tipos[i]) >= 0; });
      if (c) return c.long_name;
    }
    return '';
  }

  function abrir(opts) {
    opts = opts || {};
    var ev = opts.evento || null;
    var host = opts.host || document.body;
    var viejo = document.getElementById('evFormOv');
    if (viejo) viejo.remove();
    /* Lo que ya está confirmado con Google (lat/lng) viaja intacto mientras el texto del lugar no cambie. */
    var lugar = { direccion: (ev && ev.direccion) || '', lat: ev ? ev.lat : null, lng: ev ? ev.lng : null,
      place_id: (ev && ev.place_id) || '', comuna: (ev && ev.comuna) || '', region: (ev && ev.region) || '' };
    var cliente = (ev && ev.cliente_id) ? { id: ev.cliente_id, nombre: ev.cliente } : (opts.cliente || null);
    var cancelado = !!(ev && ev.estado_db === 'cancelado');

    var ov = document.createElement('div');
    ov.id = 'evFormOv';
    ov.className = 'ev-ov';
    ov.setAttribute('role', 'dialog');
    ov.setAttribute('aria-modal', 'true');
    ov.setAttribute('aria-label', ev ? 'Editar evento' : 'Nuevo evento');
    ov.innerHTML =
      '<div class="ev-ov-box">' +
        '<div class="ev-ov-h"><b><i class="bi bi-calendar-event"></i>' + (ev ? 'Editar evento' : 'Crear evento') + '</b>' +
          '<button type="button" class="ev-ov-x" aria-label="Cerrar" data-x>&times;</button></div>' +
        '<div class="ev-ov-b">' +
          '<div class="full"><label class="ev-lbl" for="evfNombre">Nombre del evento *</label>' +
            '<input class="ev-in" id="evfNombre" maxlength="160" autocomplete="off" placeholder="Ej: Mall Plaza Egaña · Fitness Week" value="' + esc(ev ? ev.nombre : '') + '">' +
            '<div class="ev-ayuda">Un evento agrupa las OT de un mismo lugar y fechas (por ejemplo, la instalación y la desinstalación de un mall).</div></div>' +
          '<div class="full"><label class="ev-lbl" for="evfLugar">Lugar</label>' +
            '<input class="ev-in" id="evfLugar" maxlength="400" autocomplete="off" placeholder="Empieza a escribir y elige la sugerencia" value="' + esc(lugar.direccion) + '">' +
            '<div class="ev-ayuda" id="evfLugarEst"></div></div>' +
          '<div class="full"><label class="ev-lbl" for="evfDetalle">Edificio, local u oficina</label>' +
            '<input class="ev-in" id="evfDetalle" maxlength="200" autocomplete="off" placeholder="Ej: Local 12, segundo piso" value="' + esc(ev ? ev.detalle : '') + '"></div>' +
          '<div><label class="ev-lbl" for="evfIni">Fecha de inicio</label><input class="ev-in" type="date" id="evfIni" value="' + esc(ev ? ev.fecha_ini_iso : '') + '"></div>' +
          '<div><label class="ev-lbl" for="evfFin">Fecha de término</label><input class="ev-in" type="date" id="evfFin" value="' + esc(ev ? ev.fecha_fin_iso : '') + '"></div>' +
          '<div class="full"><label class="ev-lbl" for="evfDoc">Documento de referencia (opcional)</label>' +
            '<input class="ev-in" id="evfDoc" maxlength="80" autocomplete="off" placeholder="Ej: BD10961" value="' + esc(ev ? (ev.doc_ref || '') : '') + '">' +
            '<div class="ev-ayuda">Solo sirve para encontrar las OT del evento. No cuenta como documento de ninguna OT.</div></div>' +
          '<div class="full"><label class="ev-lbl" for="evfNotas">Notas (opcional)</label>' +
            '<textarea class="ev-in" id="evfNotas" maxlength="1000" placeholder="Lo que el equipo debe saber de este evento">' + esc(ev ? (ev.notas || '') : '') + '</textarea></div>' +
          (cliente ? '<div class="full ev-ayuda"><i class="bi bi-building me-1"></i>Cliente: <b>' + esc(cliente.nombre || ('#' + cliente.id)) + '</b></div>' : '') +
          '<div class="full" id="evfErr"></div>' +
        '</div>' +
        '<div class="ev-ov-f">' +
          (ev && !cancelado ? '<button type="button" class="ev-btn mal" data-cancelar style="margin-right:auto"><i class="bi bi-slash-circle"></i>Cancelar evento</button>' : '') +
          (ev && cancelado ? '<button type="button" class="ev-btn" data-reactivar style="margin-right:auto"><i class="bi bi-arrow-counterclockwise"></i>Reactivar evento</button>' : '') +
          '<button type="button" class="ev-btn" data-x>Cerrar</button>' +
          '<button type="button" class="ev-btn pri" data-guardar><i class="bi bi-check-lg"></i>' + (ev ? 'Guardar cambios' : 'Crear evento') + '</button>' +
        '</div>' +
      '</div>';
    host.appendChild(ov);

    function $(id) { return document.getElementById(id); }
    function cerrar() { if (ov.parentNode) ov.remove(); }
    function mostrarError(msg, extra) {
      $('evfErr').innerHTML = '<div class="ev-err"><i class="bi bi-exclamation-triangle-fill me-1"></i>' + esc(msg) + (extra || '') + '</div>';
    }
    function pintarLugar() {
      var e = $('evfLugarEst');
      if (!e) return;
      e.innerHTML = lugar.lat != null && lugar.lng != null
        ? '<span style="color:#16a34a"><i class="bi bi-geo-alt-fill me-1"></i>Ubicación confirmada.</span>'
        : (($('evfLugar').value || '').trim()
          ? '<span style="color:#b45309"><i class="bi bi-exclamation-triangle me-1"></i>Elige una sugerencia para confirmar la ubicación (sin esto el técnico no tiene punto exacto).</span>'
          : '');
    }
    pintarLugar();

    ov.addEventListener('click', function (e) { if (e.target === ov) cerrar(); });
    ov.addEventListener('keydown', function (e) { if (e.key === 'Escape') { e.stopPropagation(); cerrar(); } });
    Array.prototype.forEach.call(ov.querySelectorAll('[data-x]'), function (b) { b.addEventListener('click', cerrar); });

    /* Escribir a mano invalida la confirmación de Google: el lat/lng guardado ya no correspondería al texto. */
    $('evfLugar').addEventListener('input', function () {
      if (this.value !== lugar.direccion) { lugar.lat = null; lugar.lng = null; lugar.place_id = ''; lugar.comuna = ''; lugar.region = ''; }
      lugar.direccion = this.value;
      pintarLugar();
    });
    setTimeout(function () {
      var inp = $('evfLugar');
      if (!inp || inp.dataset.placesInit === '1' || typeof window.ilusPlacesAutocomplete !== 'function') return;
      window.ilusPlacesAutocomplete('evfLugar', {
        country: 'cl', types: ['geocode', 'establishment'],
        onPlaceSelected: function (p) {
          lugar.direccion = p.direccion || inp.value; lugar.lat = p.lat; lugar.lng = p.lng; lugar.place_id = p.place_id || '';
          lugar.region = pick(p.componentes, ['administrative_area_level_1']);
          lugar.comuna = pick(p.componentes, ['administrative_area_level_3', 'locality', 'sublocality_level_1']);
          pintarLugar();
        }
      });
    }, 60);

    function cuerpo() {
      return {
        nombre: ($('evfNombre').value || '').trim(), cliente_id: cliente ? cliente.id : null,
        direccion: ($('evfLugar').value || '').trim(), detalle: ($('evfDetalle').value || '').trim(),
        lat: lugar.lat, lng: lugar.lng, place_id: lugar.place_id, comuna: lugar.comuna, region: lugar.region,
        fecha_ini: $('evfIni').value || null, fecha_fin: $('evfFin').value || null,
        doc_ref: ($('evfDoc').value || '').trim() || null, notas: ($('evfNotas').value || '').trim() || null
      };
    }
    function guardar() {
      var b = cuerpo();
      if (b.nombre.length < 3) { mostrarError('Escribe el nombre del evento (mínimo 3 letras).'); $('evfNombre').focus(); return; }
      if (b.fecha_ini && b.fecha_fin && b.fecha_fin < b.fecha_ini) { mostrarError('La fecha de término no puede ser anterior a la de inicio.'); return; }
      var btn = ov.querySelector('[data-guardar]');
      btn.disabled = true;
      var antes = btn.innerHTML;
      btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Guardando…';
      api(ev ? '/ot/api/eventos/' + ev.id : '/ot/api/eventos', ev ? 'PUT' : 'POST', b).then(function (r) {
        btn.disabled = false; btn.innerHTML = antes;
        if (!r.ok) { mostrarError(r.j.error || 'No se pudo guardar el evento.'); return; }
        cerrar();
        toast(r.j.ya_existia ? 'Ese evento ya estaba creado: lo dejé seleccionado.' : (ev ? '✓ Evento guardado' : '✓ Evento «' + b.nombre + '» creado'), 'success');
        if (typeof opts.onGuardado === 'function') opts.onGuardado(r.j.evento || null);
      });
    }
    ov.querySelector('[data-guardar]').addEventListener('click', guardar);
    $('evfNombre').addEventListener('keydown', function (e) { if (e.key === 'Enter') { e.preventDefault(); guardar(); } });

    var bc = ov.querySelector('[data-cancelar]');
    if (bc) bc.addEventListener('click', function () {
      window.ilusPrompt({
        title: 'Cancelar el evento', message: 'Explica por qué se cancela: queda guardado con tu nombre.',
        sub: 'Un evento nunca se borra. Solo se puede cancelar si todas sus OT están cerradas o canceladas.',
        placeholder: 'Ej: el mall suspendió la actividad', required: true, multiline: true, okLabel: 'Cancelar el evento', cancelLabel: 'Volver'
      }).then(function (motivo) {
        if (!motivo) return;
        api('/ot/api/eventos/' + ev.id, 'PUT', { cancelar: true, motivo: motivo }).then(function (r) {
          if (!r.ok) {
            var lista = '';
            if (r.j.ots && r.j.ots.length) {
              lista = '<ul class="mb-0 mt-1">' + r.j.ots.slice(0, 8).map(function (o) {
                return '<li><a href="/ot/' + o.id + '">' + esc(o.numero_ot || ('OT ' + o.id)) + '</a> · ' + esc(o.estado_label || o.estado || '') + '</li>';
              }).join('') + '</ul>';
            }
            mostrarError(r.j.error || 'No se pudo cancelar el evento.', lista);
            return;
          }
          cerrar();
          toast('Evento cancelado.', 'success');
          if (typeof opts.onGuardado === 'function') opts.onGuardado(r.j.evento || null);
        });
      });
    });
    var br = ov.querySelector('[data-reactivar]');
    if (br) br.addEventListener('click', function () {
      api('/ot/api/eventos/' + ev.id, 'PUT', { reactivar: true }).then(function (r) {
        if (!r.ok) { mostrarError(r.j.error || 'No se pudo reactivar el evento.'); return; }
        cerrar();
        toast('Evento reactivado.', 'success');
        if (typeof opts.onGuardado === 'function') opts.onGuardado(r.j.evento || null);
      });
    });
    setTimeout(function () { var n = $('evfNombre'); if (n) n.focus(); }, 80);
  }

  window.OTEventoForm = { abrir: abrir };
})();

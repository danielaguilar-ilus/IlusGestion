/* ══════════════════════════════════════════════════════════════════════════════════════════════════════════════
   ot_hallazgos.js — «Hallazgos de esta OT» (REGLA #38). Daniel, 2026-10-20: «que la OT avise para que se tome el caso y se levante una
   acción que debería ser otra OT».
   La tarjeta la dibuja el servidor (templates/ot2/_hallazgos.html); este archivo solo le da vida:
     · «Tomar el caso»            → POST /ot/api/<vid>/hallazgos/<id>/tomar
     · «Levantar OT de seguimiento» → abre el asistente «Nueva OT» PRELLENADO (/ot/?crear=1&seguimiento=<ids>&desde=<vid>); el asistente
                                    vuelve a pedir al servidor lo prellenado y lo valida de nuevo al crear (REGLA #32).
     · «Descartar»                → motivo OBLIGATORIO (≥ 10 caracteres), POST …/descartar
     · «Reabrir»                  → POST …/reabrir
     · «Seleccionar todo» (REGLA #14): un solo botón que marca o desmarca todos los que se pueden elegir.
     · Las fotos abren el visor global (ilusLightbox).
   Sin alert/confirm/prompt nativos (REGLA #1: ilusPrompt / ilusConfirm / ilusToast). Botones de 44 px (REGLA #3). Un técnico nunca llega
   aquí: el servidor no le dibuja la tarjeta (REGLAS #19/#26).
   2026-10-22 (auditoría Impeccable, fase 1):
     · H-27  mientras guarda, el botón tocado dice «Guardando…» (y los demás quedan deshabilitados); si algo falla, todo vuelve a su lugar.
             La página se recarga como antes (así los contadores salen con lo nuevo), pero la persona vuelve EXACTAMENTE donde estaba: el
             scroll y qué equipos tenía abiertos se guardan en sessionStorage antes de recargar.
     · H-20  los equipos son <details> plegables: un botón abre o cierra todos, y «Seleccionar todo» los abre para que se vea qué se eligió.
     · H-16  el pie de las fotos es el título COMPLETO (data-hz-cap), aunque en pantalla el hallazgo diga solo lo que pasó.
   ══════════════════════════════════════════════════════════════════════════════════════════════════════════════ */
(function () {
  'use strict';
  var CFG = window.OTD_HZ_CFG || {};
  var VID = CFG.vid;
  if (!VID) return;

  var MIN_DESCARTE = 10;
  var ocupado = false;
  var CLAVE_VISTA = 'ilus_hz_vista';      // sessionStorage: dónde estaba la persona justo antes de recargar la página
  var liberarPendiente = null;            // cómo volver a habilitar los botones si la persona regresa a esta página (caché del navegador)

  function toast(m, t, d) { try { if (window.ilusToast) window.ilusToast(m, { type: t || 'info', duration: d || 5000 }); } catch (e) { /* nada */ } }

  function api(ruta, cuerpo) {
    var init = { credentials: 'same-origin', method: 'POST', headers: { 'X-Requested-With': 'XMLHttpRequest', 'Accept': 'application/json' } };
    if (cuerpo !== undefined) { init.headers['Content-Type'] = 'application/json'; init.body = JSON.stringify(cuerpo); }
    return fetch('/ot/api/' + VID + '/hallazgos/' + ruta, init).then(function (r) {
      return r.json().catch(function () { return {}; }).then(function (j) {
        /* H-28: qué hacer, sin jerga. Cuando el servidor trae su propio motivo («Este caso ya lo tomó…») se dice tal cual. */
        if (!j.ok && !j.error) j.error = r.status === 403 ? 'No tienes permiso para gestionar esta OT.' : 'No se pudo guardar. Recarga la página e intenta de nuevo; si sigue igual, avisa a soporte.';
        return j;
      });
    }).catch(function () { return { ok: false, error: 'Sin conexión. Revisa la señal e intenta de nuevo.' }; });
  }

  /* ── Dónde estaba la persona (scroll y equipos abiertos), para volver ahí después de recargar (H-27) ── */
  function guardar(clave, valor) { try { window.sessionStorage.setItem(clave, valor); } catch (e) { /* sin almacenamiento: la recarga sigue igual */ } }
  function leer(clave) { try { return window.sessionStorage.getItem(clave); } catch (e) { return null; } }
  function borrar(clave) { try { window.sessionStorage.removeItem(clave); } catch (e) { /* nada */ } }
  function rutaActual() { return location.pathname + location.search; }
  function grupos() { return Array.prototype.slice.call(document.querySelectorAll('details[data-hz-grupo]')); }

  /* La página se vuelve a pedir al servidor para que la tarjeta y los contadores salgan con lo nuevo; la persona vuelve donde estaba. */
  function recargar() {
    var vista = { y: 0, grupos: {}, t: Date.now(), ruta: rutaActual() };
    try { vista.y = window.pageYOffset || (document.documentElement && document.documentElement.scrollTop) || 0; } catch (e) { vista.y = 0; }
    grupos().forEach(function (d) { vista.grupos[d.getAttribute('data-hz-grupo')] = !!d.open; });
    guardar(CLAVE_VISTA, JSON.stringify(vista));
    try { history.replaceState(null, '', location.pathname + location.search + '#otdHallazgos'); } catch (e) { /* nada */ }
    setTimeout(function () { location.reload(); }, 450);
  }

  function restaurarVista() {
    var crudo = leer(CLAVE_VISTA);
    if (!crudo) return;
    borrar(CLAVE_VISTA);
    var v = null;
    try { v = JSON.parse(crudo); } catch (e) { v = null; }
    if (!v || v.ruta !== rutaActual() || (Date.now() - (v.t || 0)) > 120000) return;      // solo la recarga que acabamos de pedir
    grupos().forEach(function (d) {
      var k = d.getAttribute('data-hz-grupo');
      if (v.grupos && Object.prototype.hasOwnProperty.call(v.grupos, k)) d.open = !!v.grupos[k];
    });
    var y = parseInt(v.y, 10) || 0;
    function ir() { try { window.scrollTo(0, y); } catch (e) { /* nada */ } }
    ir();
    if (window.requestAnimationFrame) window.requestAnimationFrame(ir);
  }

  /* ── Botones con estado de carga (H-27): el tocado dice «Guardando…», todos quedan deshabilitados y `liberar()` lo deja como estaba ── */
  function bloquear(btn, texto) {
    var tocados = [], original = null, rotulo = null;
    try {
      var todos = document.querySelectorAll('.hz-btn');
      for (var i = 0; i < todos.length; i++) {
        if (!todos[i].disabled) { todos[i].disabled = true; tocados.push(todos[i]); }
      }
      if (btn && btn.querySelector) {
        rotulo = btn.querySelector('.hz-btn-tx');
        if (rotulo) { original = rotulo.textContent; rotulo.textContent = texto; }
        btn.setAttribute('aria-busy', 'true');
      }
    } catch (e) { /* si algo de esto falla igual se sigue: la bandera `ocupado` ya evita mandar dos veces */ }
    return function liberar() {
      try {
        tocados.forEach(function (b) { b.disabled = false; });
        if (btn && btn.removeAttribute) btn.removeAttribute('aria-busy');
        if (rotulo && original !== null) rotulo.textContent = original;
        pintarSeleccion();            // el botón «Levantar una OT con lo elegido» vuelve a su estado según lo elegido
      } catch (e) { /* nada */ }
    };
  }

  function itemDe(el) { return el.closest ? el.closest('.hz-item') : null; }

  /* ── Elegir varios (REGLA #14) ── */
  function seleccionables() { return Array.prototype.slice.call(document.querySelectorAll('[data-hz-sel]:not(:disabled)')); }
  function elegidos() { return seleccionables().filter(function (c) { return c.checked; }).map(function (c) { return parseInt(c.value, 10); }); }
  function pintarSeleccion() {
    var todos = seleccionables(), n = elegidos().length;
    var cuenta = document.querySelector('[data-hz-cuenta]');
    if (cuenta) cuenta.textContent = n + ' de ' + todos.length + ' elegido' + (n === 1 ? '' : 's');
    var btn = document.querySelector('[data-hz-levantar-sel]');
    if (btn) {
      btn.disabled = n === 0;
      var sp = btn.querySelector('span');
      if (sp) sp.textContent = n > 1 ? 'Levantar una OT con los ' + n + ' elegidos' : (n === 1 ? 'Levantar una OT con el elegido' : 'Levantar una OT con lo elegido');
    }
    var tg = document.querySelector('[data-hz-todo] span');
    if (tg) tg.textContent = (todos.length && n === todos.length) ? 'Quitar la selección' : 'Seleccionar todo';
  }
  function alternarTodo() {
    var todos = seleccionables();
    var faltan = todos.some(function (c) { return !c.checked; });        // si alguno está sin marcar, marca todos; si ya están todos, los desmarca
    if (faltan) grupos().forEach(function (d) { d.open = true; });       // H-20: los equipos plegados se abren para que se vea qué quedó elegido
    todos.forEach(function (c) { c.checked = faltan; });
    pintarSeleccion();
  }

  /* ── Abrir o cerrar todos los equipos (H-20): un solo botón-interruptor, como «Seleccionar todo» ── */
  function pintarPlegado() {
    var btn = document.querySelector('[data-hz-plegar-todo]');
    if (!btn) return;
    var ds = grupos();
    var todosAbiertos = ds.length > 0 && ds.every(function (d) { return d.open; });
    var sp = btn.querySelector('.hz-btn-tx');
    if (sp) sp.textContent = todosAbiertos ? 'Cerrar todos los equipos' : 'Abrir todos los equipos';
    btn.setAttribute('aria-expanded', todosAbiertos ? 'true' : 'false');
  }
  function alternarPlegado() {
    var ds = grupos();
    var abrir = ds.some(function (d) { return !d.open; });               // si alguno está cerrado, abre todos; si ya están todos abiertos, los cierra
    ds.forEach(function (d) { d.open = abrir; });
    pintarPlegado();
  }

  /* ── Levantar la OT de seguimiento: el asistente «Nueva OT» llega prellenado ── */
  function levantar(ids, btn) {
    ids = (ids || []).filter(Boolean);
    if (!ids.length) { toast('Elige al menos un hallazgo para la OT de seguimiento.', 'warning'); return; }
    liberarPendiente = bloquear(btn, 'Abriendo…');
    location.href = '/ot/?crear=1&seguimiento=' + ids.join(',') + '&desde=' + VID;
  }

  /* ── Tomar el caso ── */
  function tomar(id, btn) {
    if (ocupado) return;
    ocupado = true;
    var liberar = bloquear(btn, 'Guardando…');
    api(id + '/tomar').then(function (j) {
      ocupado = false;
      if (!j.ok) { liberar(); toast(j.error, 'error', 8000); return; }
      toast('✓ Tomaste el caso. Queda a tu nombre.', 'success');
      recargar();
    });
  }

  /* ── Descartar: el motivo es obligatorio y queda escrito ── */
  function descartar(id, titulo, btn) {
    if (ocupado || !window.ilusPrompt) return;
    window.ilusPrompt({
      title: 'Descartar este hallazgo',
      message: titulo || 'Hallazgo de la OT',
      sub: 'Queda escrito en la bitácora de la OT con tu nombre. No se borra nada y se puede reabrir.',
      placeholder: 'Por qué se descarta (mínimo ' + MIN_DESCARTE + ' caracteres)…',
      multiline: true, required: true, okLabel: 'Descartar', cancelLabel: 'Cancelar', type: 'warning',
      validate: function (v) {
        var t = String(v || '').replace(/\s+/g, ' ').trim();
        if (t.length < MIN_DESCARTE) return { ok: false, error: 'Cuenta por qué se descarta (mínimo ' + MIN_DESCARTE + ' caracteres).' };
        return { ok: true, valor: t };
      }
    }).then(function (motivo) {
      if (!motivo) return;
      ocupado = true;
      var liberar = bloquear(btn, 'Guardando…');
      api(id + '/descartar', { motivo: motivo }).then(function (j) {
        ocupado = false;
        if (!j.ok) { liberar(); toast(j.error, 'error', 8000); return; }
        toast('✓ Hallazgo descartado. Quedó escrito el motivo.', 'success');
        recargar();
      });
    });
  }

  function reabrir(id, btn) {
    if (ocupado || !window.ilusConfirm) return;
    window.ilusConfirm({
      title: 'Reabrir el hallazgo', message: '¿Volver a dejarlo «sin gestionar»?',
      sub: 'El descarte anterior queda escrito en la bitácora de la OT.', okLabel: 'Reabrir', cancelLabel: 'Cancelar', type: 'question'
    }).then(function (ok) {
      if (!ok) return;
      ocupado = true;
      var liberar = bloquear(btn, 'Guardando…');
      api(id + '/reabrir').then(function (j) {
        ocupado = false;
        if (!j.ok) { liberar(); toast(j.error, 'error', 8000); return; }
        toast('✓ Hallazgo reabierto.', 'success');
        recargar();
      });
    });
  }

  /* ── Las fotos son la evidencia: se abren en el visor global ── */
  function verFoto(btn) {
    var cont = btn.closest('[data-hz-fotos]');
    var urls = [];
    try { urls = JSON.parse(cont.getAttribute('data-hz-fotos') || '[]'); } catch (e) { urls = []; }
    var idx = parseInt(btn.getAttribute('data-hz-foto'), 10) || 0;
    if (!urls.length) return;
    var it = itemDe(btn);
    var cap = 'Hallazgo';
    try {
      /* El pie es el título COMPLETO (con el equipo): en pantalla el hallazgo dice solo lo que pasó, porque el equipo ya va en el encabezado. */
      if (it && it.getAttribute && it.getAttribute('data-hz-cap')) cap = it.getAttribute('data-hz-cap');
      else if (it && it.querySelector && it.querySelector('.hz-tit')) cap = it.querySelector('.hz-tit').textContent.trim();
    } catch (e) { /* nada */ }
    if (typeof window.ilusLightbox === 'function') {
      window.ilusLightbox(urls.map(function (u) { return { url: u, caption: cap }; }), idx, 'Hallazgo');
    } else {
      window.open(urls[idx], '_blank', 'noopener');
    }
  }

  /* ── Desde la franja ámbar del cierre: cierra el modal y lleva a la tarjeta (nunca bloquea nada) ── */
  function irALaTarjeta() {
    try {
      document.querySelectorAll('.modal.show').forEach(function (m) {
        var inst = window.bootstrap && window.bootstrap.Modal ? window.bootstrap.Modal.getInstance(m) : null;
        if (inst) inst.hide();
      });
    } catch (e) { /* nada */ }
    setTimeout(function () {
      var t = document.getElementById('otdHallazgos');
      if (!t) return;
      t.scrollIntoView({ behavior: 'smooth', block: 'start' });
      t.classList.add('hz-resaltado');
      setTimeout(function () { t.classList.remove('hz-resaltado'); }, 2200);
    }, 350);
  }

  document.addEventListener('click', function (ev) {
    var b;
    if ((b = ev.target.closest('[data-hz-tomar]'))) { ev.preventDefault(); tomar(parseInt(b.getAttribute('data-hz-tomar'), 10), b); return; }
    if ((b = ev.target.closest('[data-hz-levantar-sel]'))) { ev.preventDefault(); levantar(elegidos(), b); return; }
    if ((b = ev.target.closest('[data-hz-levantar]'))) { ev.preventDefault(); levantar([parseInt(b.getAttribute('data-hz-levantar'), 10)], b); return; }
    if ((b = ev.target.closest('[data-hz-descartar]'))) { ev.preventDefault(); descartar(parseInt(b.getAttribute('data-hz-descartar'), 10), b.getAttribute('data-hz-titulo'), b); return; }
    if ((b = ev.target.closest('[data-hz-reabrir]'))) { ev.preventDefault(); reabrir(parseInt(b.getAttribute('data-hz-reabrir'), 10), b); return; }
    if ((b = ev.target.closest('[data-hz-todo]'))) { ev.preventDefault(); alternarTodo(); return; }
    if ((b = ev.target.closest('[data-hz-plegar-todo]'))) { ev.preventDefault(); alternarPlegado(); return; }
    if ((b = ev.target.closest('[data-hz-foto]'))) { ev.preventDefault(); verFoto(b); return; }
    if ((b = ev.target.closest('[data-hz-ir]'))) { ev.preventDefault(); irALaTarjeta(); return; }
  });
  document.addEventListener('change', function (ev) { if (ev.target && ev.target.matches && ev.target.matches('[data-hz-sel]')) pintarSeleccion(); });
  /* `toggle` no sube por el árbol: se escucha en la captura. Así el rótulo de «Abrir/Cerrar todos» sigue a cada equipo que la persona abre o cierra. */
  document.addEventListener('toggle', function (ev) {
    if (ev.target && ev.target.matches && ev.target.matches('details[data-hz-grupo]')) pintarPlegado();
  }, true);
  /* Si la persona vuelve a esta página con «Atrás» (el navegador la guardó tal cual), los botones no pueden quedar con «Abriendo…». */
  if (window.addEventListener) {
    window.addEventListener('pageshow', function (ev) {
      if (ev && ev.persisted && liberarPendiente) { liberarPendiente(); liberarPendiente = null; }
    });
  }

  restaurarVista();
  pintarSeleccion();
  pintarPlegado();
  window.OTHallazgos = { levantar: levantar, tomar: tomar, descartar: descartar, reabrir: reabrir, elegidos: elegidos };
})();

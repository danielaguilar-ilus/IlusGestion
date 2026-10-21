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
   ══════════════════════════════════════════════════════════════════════════════════════════════════════════════ */
(function () {
  'use strict';
  var CFG = window.OTD_HZ_CFG || {};
  var VID = CFG.vid;
  if (!VID) return;

  var MIN_DESCARTE = 10;
  var ocupado = false;

  function toast(m, t, d) { try { if (window.ilusToast) window.ilusToast(m, { type: t || 'info', duration: d || 5000 }); } catch (e) { /* nada */ } }

  function api(ruta, cuerpo) {
    var init = { credentials: 'same-origin', method: 'POST', headers: { 'X-Requested-With': 'XMLHttpRequest', 'Accept': 'application/json' } };
    if (cuerpo !== undefined) { init.headers['Content-Type'] = 'application/json'; init.body = JSON.stringify(cuerpo); }
    return fetch('/ot/api/' + VID + '/hallazgos/' + ruta, init).then(function (r) {
      return r.json().catch(function () { return {}; }).then(function (j) {
        if (!j.ok && !j.error) j.error = r.status === 403 ? 'No tienes permiso para gestionar esta OT.' : 'No se pudo completar. Intenta de nuevo.';
        return j;
      });
    }).catch(function () { return { ok: false, error: 'Sin conexión. Revisa la señal e intenta de nuevo.' }; });
  }

  /* La página se vuelve a pedir al servidor para que la tarjeta y los contadores salgan con lo nuevo; el ancla deja a la persona donde estaba. */
  function recargar() {
    try { history.replaceState(null, '', location.pathname + location.search + '#otdHallazgos'); } catch (e) { /* nada */ }
    setTimeout(function () { location.reload(); }, 450);
  }

  function itemDe(el) { return el.closest ? el.closest('.hz-item') : null; }
  function idDe(el) { var it = itemDe(el); return it ? parseInt(it.getAttribute('data-hz-id'), 10) : 0; }

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
    todos.forEach(function (c) { c.checked = faltan; });
    pintarSeleccion();
  }

  /* ── Levantar la OT de seguimiento: el asistente «Nueva OT» llega prellenado ── */
  function levantar(ids) {
    ids = (ids || []).filter(Boolean);
    if (!ids.length) { toast('Elige al menos un hallazgo para la OT de seguimiento.', 'warning'); return; }
    location.href = '/ot/?crear=1&seguimiento=' + ids.join(',') + '&desde=' + VID;
  }

  /* ── Tomar el caso ── */
  function tomar(id) {
    if (ocupado) return;
    ocupado = true;
    api(id + '/tomar').then(function (j) {
      ocupado = false;
      if (!j.ok) { toast(j.error, 'error', 8000); return; }
      toast('✓ Tomaste el caso. Queda a tu nombre.', 'success');
      recargar();
    });
  }

  /* ── Descartar: el motivo es obligatorio y queda escrito ── */
  function descartar(id, titulo) {
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
      api(id + '/descartar', { motivo: motivo }).then(function (j) {
        ocupado = false;
        if (!j.ok) { toast(j.error, 'error', 8000); return; }
        toast('✓ Hallazgo descartado. Quedó escrito el motivo.', 'success');
        recargar();
      });
    });
  }

  function reabrir(id) {
    if (ocupado || !window.ilusConfirm) return;
    window.ilusConfirm({
      title: 'Reabrir el hallazgo', message: '¿Volver a dejarlo «sin gestionar»?',
      sub: 'El descarte anterior queda escrito en la bitácora de la OT.', okLabel: 'Reabrir', cancelLabel: 'Cancelar', type: 'question'
    }).then(function (ok) {
      if (!ok) return;
      ocupado = true;
      api(id + '/reabrir').then(function (j) {
        ocupado = false;
        if (!j.ok) { toast(j.error, 'error', 8000); return; }
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
    var cap = it && it.querySelector('.hz-tit') ? it.querySelector('.hz-tit').textContent.trim() : 'Hallazgo';
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
    if ((b = ev.target.closest('[data-hz-tomar]'))) { ev.preventDefault(); tomar(parseInt(b.getAttribute('data-hz-tomar'), 10)); return; }
    if ((b = ev.target.closest('[data-hz-levantar-sel]'))) { ev.preventDefault(); levantar(elegidos()); return; }
    if ((b = ev.target.closest('[data-hz-levantar]'))) { ev.preventDefault(); levantar([parseInt(b.getAttribute('data-hz-levantar'), 10)]); return; }
    if ((b = ev.target.closest('[data-hz-descartar]'))) { ev.preventDefault(); descartar(parseInt(b.getAttribute('data-hz-descartar'), 10), b.getAttribute('data-hz-titulo')); return; }
    if ((b = ev.target.closest('[data-hz-reabrir]'))) { ev.preventDefault(); reabrir(parseInt(b.getAttribute('data-hz-reabrir'), 10)); return; }
    if ((b = ev.target.closest('[data-hz-todo]'))) { ev.preventDefault(); alternarTodo(); return; }
    if ((b = ev.target.closest('[data-hz-foto]'))) { ev.preventDefault(); verFoto(b); return; }
    if ((b = ev.target.closest('[data-hz-ir]'))) { ev.preventDefault(); irALaTarjeta(); return; }
  });
  document.addEventListener('change', function (ev) { if (ev.target && ev.target.matches && ev.target.matches('[data-hz-sel]')) pintarSeleccion(); });

  pintarSeleccion();
  window.OTHallazgos = { levantar: levantar, tomar: tomar, descartar: descartar, reabrir: reabrir, elegidos: elegidos };
})();

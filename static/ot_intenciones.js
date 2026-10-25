/* 🧭 Clasificaciones del costeo (REGLA #47): /ot/intenciones — SOLO el superadministrador.
   Crear, editar, activar y desactivar las clasificaciones («¿Por qué?») que se eligen al declarar un $0 o pedir una autorización. Nunca se borran.
   Sin alert/confirm/prompt nativos (REGLA #1): diálogo propio + ilusToast + ilusConfirm. «Marcar/desmarcar todo» en la lista de usos (REGLA #14).
   API: GET /ot/api/intenciones/gestion · POST /ot/api/intenciones · POST /ot/api/intenciones/<clave> · POST /ot/api/intenciones/<clave>/estado. */
(function () {
  'use strict';
  var raiz = document.getElementById('ppInten');
  if (!raiz) return;
  var datos = { items: [], usos: [], familias: [], centros: [] };

  function esc(s) {
    return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
    });
  }
  function api(url, body) {
    var init = { credentials: 'same-origin', method: body === undefined ? 'GET' : 'POST', headers: {} };
    if (body !== undefined) { init.headers['Content-Type'] = 'application/json'; init.body = JSON.stringify(body); }
    return fetch(url, init).then(function (r) {
      return r.json().catch(function () { return null; }).then(function (j) { return { ok: r.ok && !(j && j.ok === false), j: j || {} }; });
    }).catch(function () { return { ok: false, j: { error: 'No hay conexión con el servidor.' } }; });
  }
  function toast(m, t) { if (window.ilusToast) window.ilusToast(m, { type: t || 'info' }); }
  function nombreUso(v) { var u = datos.usos.filter(function (x) { return x.v === v; })[0]; return u ? u.n : v; }
  function nombreCentro(v) { var c = datos.centros.filter(function (x) { return x.v === v; })[0]; return c ? c.n : (v || ''); }
  function nombreFamilia(v) { var f = datos.familias.filter(function (x) { return x.v === v; })[0]; return f ? f.n : ''; }

  function tarjeta(i) {
    var chips = (i.aplica_a || []).map(function (u) { return '<span class="oi-chip uso">' + esc(nombreUso(u)) + '</span>'; }).join('');
    var extra = (i.sistema ? '<span class="oi-chip sis">Del sistema</span>' : '') +
      (i.solo_admin ? '<span class="oi-chip adm">Solo administrador</span>' : '') +
      (i.familia_cero ? '<span class="oi-chip fam">Un $0 · ' + esc(nombreFamilia(i.familia_cero)) + '</span>' : '') +
      (!i.activa ? '<span class="oi-chip off">Desactivada</span>' : '');
    var traza = [];
    if (i.creada_por) traza.push('Creada por ' + esc(i.creada_por) + (i.creada_at ? ' el ' + esc(i.creada_at) : ''));
    if (i.actualizada_por) traza.push('Última edición de ' + esc(i.actualizada_por) + (i.actualizada_at ? ' el ' + esc(i.actualizada_at) : ''));
    var h = '<article class="pp-card oi-card ' + (i.activa ? 'aprobada' : 'inactiva') + '" data-clave="' + esc(i.clave) + '">' +
      '<span class="pp-circ"><i class="bi ' + (i.activa ? 'bi-check-lg' : 'bi-slash-circle') + '"></i></span>' +
      '<h3>' + esc(i.nombre) + '<small>' + esc(i.clave) + '</small></h3>' +
      (i.descripcion ? '<div class="oi-desc">' + esc(i.descripcion) + '</div>' : '') +
      '<dl class="pp-datos"><div><dt>Quién lo asume</dt><dd>' + esc(nombreCentro(i.asume_centro) || 'Sin definir') + '</dd></div></dl>' +
      '<div class="oi-chips">' + chips + extra + '</div>' +
      (traza.length ? '<div class="oi-traza">' + traza.join(' · ') + '</div>' : '') +
      '<div class="pp-acc"><button type="button" class="pp-btn" data-editar="' + esc(i.clave) + '"><i class="bi bi-pencil"></i> Editar</button>';
    if (!i.sistema) {
      h += '<button type="button" class="pp-btn ' + (i.activa ? 'mal' : 'ok') + '" data-estado="' + esc(i.clave) + '" data-activa="' + (i.activa ? '0' : '1') + '">' +
        (i.activa ? '<i class="bi bi-slash-circle"></i> Desactivar' : '<i class="bi bi-check-lg"></i> Activar') + '</button>';
    }
    return h + '</div></article>';
  }

  function pintar() {
    var act = datos.items.filter(function (i) { return i.activa; }), off = datos.items.filter(function (i) { return !i.activa; });
    raiz.innerHTML =
      '<div class="pp-chips"><span class="pp-chip on">Activas <b>' + act.length + '</b></span>' +
      '<span class="pp-chip">Desactivadas <b>' + off.length + '</b></span></div>' +
      '<div class="oi-lista">' + act.concat(off).map(tarjeta).join('') + '</div>' +
      '<p class="pp-sub" style="margin-top:14px">Una clasificación nunca se borra: si ya no se usa, se desactiva y lo que ya quedó clasificado sigue mostrándola. ' +
      'Las «solo administrador» únicamente las puede elegir un administrador; las del sistema (Garantía, Regalía, Arriendo o leasing y Otro motivo) no se desactivan.</p>';
  }

  function cargar() {
    return api('/ot/api/intenciones/gestion').then(function (r) {
      if (!r.ok) { raiz.innerHTML = '<div class="pp-error">' + esc(r.j.error || 'No se pudo leer el catálogo.') + '</div>'; return; }
      datos = r.j; pintar();
    });
  }

  /* ── diálogo de crear / editar ───────────────────────────────────────────────────────────────────────── */
  function dialogo(i) {
    var nuevo = !i;
    i = i || { nombre: '', descripcion: '', familia_cero: '', aplica_a: [], asume_centro: '', solo_admin: false, orden: 500 };
    var ov = document.createElement('div');
    ov.className = 'oi-ov';
    ov.setAttribute('role', 'dialog'); ov.setAttribute('aria-modal', 'true');
    var usos = datos.usos.map(function (u) {
      var on = (i.aplica_a || []).indexOf(u.v) >= 0;
      return '<label class="' + (on ? 'on' : '') + '"><input type="checkbox" data-uso="' + esc(u.v) + '"' + (on ? ' checked' : '') + '> <span>' + esc(u.n) + '</span></label>';
    }).join('');
    var fams = '<option value="">— (no es un $0)</option>' + datos.familias.map(function (f) {
      return '<option value="' + esc(f.v) + '"' + (i.familia_cero === f.v ? ' selected' : '') + '>' + esc(f.n) + '</option>';
    }).join('');
    var cents = '<option value="">— (sin definir)</option>' + datos.centros.map(function (c) {
      return '<option value="' + esc(c.v) + '"' + (i.asume_centro === c.v ? ' selected' : '') + '>' + esc(c.n) + '</option>';
    }).join('');
    ov.innerHTML = '<div class="oi-dlg"><div class="oi-dlg-h"><b>' + (nuevo ? 'Nueva clasificación' : 'Editar «' + esc(i.nombre) + '»') + '</b><button type="button" class="oi-x" aria-label="Cerrar">&times;</button></div>' +
      '<div class="oi-dlg-b">' +
      '<div class="oi-fld"><label for="oiNombre">Nombre *</label><input type="text" id="oiNombre" maxlength="80" value="' + esc(i.nombre) + '" placeholder="Ej: Costo asumido por Marketing"></div>' +
      '<div class="oi-fld"><label for="oiDesc">Qué significa</label><textarea id="oiDesc" maxlength="300" placeholder="Una frase para que quien la elige entienda cuándo usarla">' + esc(i.descripcion) + '</textarea></div>' +
      '<div class="oi-fld"><span class="oi-lbl">Se puede elegir cuando… *</span><div class="oi-usos" id="oiUsos">' + usos + '</div>' +
      '<button type="button" class="oi-todo" id="oiTodo">Marcar / desmarcar todo</button></div>' +
      '<div class="oi-fld"><label for="oiFam">Si es un $0, ¿a qué familia contable pertenece?</label>' +
      '<select id="oiFam"' + (i.sistema ? ' disabled' : '') + '>' + fams + '</select>' +
      '<div class="oi-help">La familia (Garantía, Regalía o Arriendo/leasing) define cómo se cuenta el $0 en el costeo. Si no eliges una y se usa en «Un $0», cuenta como Regalía; nunca toca una Garantía.</div></div>' +
      '<div class="oi-fld"><label for="oiCentro">¿Qué centro de costo lo asume?</label><select id="oiCentro">' + cents + '</select>' +
      '<div class="oi-help">Es lo que el sistema sugiere (con su origen a la vista); quien declara la OT decide.</div></div>' +
      '<div class="oi-fld"><label style="display:flex;align-items:center;gap:10px;min-height:44px;cursor:pointer;margin:0"><input type="checkbox" id="oiAdmin"' + (i.solo_admin ? ' checked' : '') +
      ' style="width:20px;height:20px"> <span>Solo un administrador puede elegirla</span></label></div>' +
      '<div class="oi-fld"><label for="oiOrden">Orden en la lista (menor primero)</label><input type="number" id="oiOrden" min="0" max="999" value="' + esc(i.orden) + '"></div>' +
      '<div class="oi-err" id="oiErr" role="alert"></div></div>' +
      '<div class="oi-dlg-f"><button type="button" class="pp-btn" data-r="no">Cancelar</button><button type="button" class="pp-btn pri" data-r="si">' + (nuevo ? 'Crear' : 'Guardar') + '</button></div></div>';
    document.body.appendChild(ov);
    function cerrar() { document.removeEventListener('keydown', tecla, true); if (ov.parentNode) ov.parentNode.removeChild(ov); }
    function tecla(e) { if (e.key === 'Escape') { e.stopPropagation(); cerrar(); } }
    document.addEventListener('keydown', tecla, true);
    ov.addEventListener('change', function (e) {
      if (e.target.matches && e.target.matches('#oiUsos input')) e.target.closest('label').classList.toggle('on', e.target.checked);
    });
    ov.addEventListener('click', function (e) {
      if (e.target === ov || e.target.closest('.oi-x') || e.target.closest('[data-r="no"]')) return cerrar();
      if (e.target.closest('#oiTodo')) {
        var cbs = ov.querySelectorAll('#oiUsos input'), alguno = false;
        for (var k = 0; k < cbs.length; k++) if (!cbs[k].checked) alguno = true;
        for (var m = 0; m < cbs.length; m++) { cbs[m].checked = alguno; cbs[m].closest('label').classList.toggle('on', alguno); }
        return;
      }
      if (!e.target.closest('[data-r="si"]')) return;
      var usosSel = [].slice.call(ov.querySelectorAll('#oiUsos input:checked')).map(function (c) { return c.getAttribute('data-uso'); });
      var body = { nombre: ov.querySelector('#oiNombre').value.trim(), descripcion: ov.querySelector('#oiDesc').value.trim(),
        aplica_a: usosSel, familia_cero: ov.querySelector('#oiFam').value, asume_centro: ov.querySelector('#oiCentro').value,
        solo_admin: ov.querySelector('#oiAdmin').checked, orden: ov.querySelector('#oiOrden').value };
      var eb = ov.querySelector('#oiErr');
      if (body.nombre.length < 3) { eb.textContent = 'Escribe el nombre (mínimo 3 letras).'; eb.className = 'oi-err on'; return; }
      if (!usosSel.length) { eb.textContent = 'Elige al menos una situación donde se pueda usar.'; eb.className = 'oi-err on'; return; }
      var url = nuevo ? '/ot/api/intenciones' : '/ot/api/intenciones/' + encodeURIComponent(i.clave);
      api(url, body).then(function (r) {
        if (!r.ok) { eb.textContent = r.j.error || 'No se pudo guardar.'; eb.className = 'oi-err on'; return; }
        cerrar();
        toast(nuevo ? 'Clasificación creada.' : 'Cambios guardados.', 'success');
        cargar();
      });
    });
    var primero = ov.querySelector('#oiNombre'); if (primero) setTimeout(function () { try { primero.focus(); } catch (e) { } }, 60);
  }

  function cambiarEstado(clave, activa) {
    var it = datos.items.filter(function (x) { return x.clave === clave; })[0];
    if (!it) return;
    var pregunta = activa ? Promise.resolve(true) : window.ilusConfirm({ title: 'Desactivar «' + it.nombre + '»',
      message: 'Ya no se podrá elegir en las solicitudes nuevas.', sub: 'Lo que ya quedó clasificado con ella sigue mostrándola. Puedes volver a activarla cuando quieras.',
      okLabel: 'Sí, desactivar', cancelLabel: 'Cancelar', type: 'question' });
    return pregunta.then(function (ok) {
      if (!ok) return;
      return api('/ot/api/intenciones/' + encodeURIComponent(clave) + '/estado', { activa: activa }).then(function (r) {
        if (!r.ok) { toast(r.j.error || 'No se pudo cambiar.', 'error'); return; }
        toast(activa ? 'Activada.' : 'Desactivada.', 'success');
        cargar();
      });
    });
  }

  raiz.addEventListener('click', function (e) {
    var ed = e.target.closest('[data-editar]'), es = e.target.closest('[data-estado]');
    if (ed) {
      var it = datos.items.filter(function (x) { return x.clave === ed.getAttribute('data-editar'); })[0];
      if (it) dialogo(it);
    } else if (es) {
      cambiarEstado(es.getAttribute('data-estado'), es.getAttribute('data-activa') === '1');
    }
  });
  var bNueva = document.getElementById('oiNueva');
  if (bNueva) bNueva.addEventListener('click', function () { dialogo(null); });
  cargar();
})();

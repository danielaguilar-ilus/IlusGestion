/* ══════════════════════════════════════════════════════════════════════════════════════════════════════════════
   ot_hallazgos_gestion.js — Gestión CIRCULAR de cada hallazgo (Daniel, 26-oct-2026: «tomar el levantamiento… editarla, crear un
   repuesto, solicitarlo, generar una OT, generar un ticket… gestionando de manera circular… además de su valorización»).
     · «Editar»              → título, detalle y gravedad, con MOTIVO obligatorio        POST /ot/api/<vid>/hallazgos/<id>/editar
     · «Valorizar»           → repuestos + servicio (pesos) y una nota                     POST …/valorizar
     · «Levantar ticket»     → SUGERIDO, pregunta antes; interno (sin correo al cliente)  POST …/ticket
     · «Solicitar repuesto»  → es un enlace a Repuestos con el cliente, el equipo y el motivo ya escritos (lo arma el servidor)
     · «Borrar»              → solo superadministrador: escribe BORRAR + motivo           POST …/borrar
   Reutiliza api / bloquear / recargar de ot_hallazgos.js (window.OTHallazgos). También lo carga la ficha de una INQUIETUD para «Valorizar»
   (botón con data-gc-url: la ruta propia de la inquietud). Sin alert/confirm/prompt nativos (REGLA #1); diálogo
   propio para los formularios de más de 3 campos (REGLA #8), con objetivos de 44 px y pantalla completa en el celular (REGLA #3).
   ══════════════════════════════════════════════════════════════════════════════════════════════════════════════ */
(function () {
  'use strict';
  var H = (window.OTHallazgos && window.OTHallazgos.api) ? window.OTHallazgos : null;   // null en la ficha de una inquietud
  var ocupado = false;
  var SEVERIDADES = [['falla', 'Falla'], ['alerta', 'Alerta'], ['fuera_servicio', 'Fuera de servicio'], ['baja', 'Dar de baja'],
                     ['repuesto', 'Repuesto'], ['incidencia', 'Incidencia']];

  function toast(m, t, d) { try { if (window.ilusToast) window.ilusToast(m, { type: t || 'info', duration: d || 5000 }); } catch (e) { /* nada */ } }
  function esc(s) { return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) { return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]; }); }
  /* POST a una ruta completa (la ficha de una inquietud no tiene ot_hallazgos.js). */
  function postar(url, cuerpo) {
    return fetch(url, { credentials: 'same-origin', method: 'POST',
      headers: { 'X-Requested-With': 'XMLHttpRequest', 'Accept': 'application/json', 'Content-Type': 'application/json' },
      body: JSON.stringify(cuerpo || {}) }).then(function (r) {
      return r.json().catch(function () { return {}; }).then(function (j) {
        if (!j.ok && !j.error) j.error = r.status === 403 ? 'No tienes permiso para esto.' : 'No se pudo guardar. Recarga la página e intenta de nuevo.';
        return j;
      });
    }).catch(function () { return { ok: false, error: 'Sin conexión. Revisa la señal e intenta de nuevo.' }; });
  }
  function recargar() { if (H && H.recargar) H.recargar(); else setTimeout(function () { location.reload(); }, 400); }
  function pesos(n) { return '$' + String(Math.round(n || 0)).replace(/\B(?=(\d{3})+(?!\d))/g, '.'); }
  function monto(v) {
    var s = String(v == null ? '' : v).replace(/[$\s.]/g, '').replace(/,/g, '');
    if (!s) return null;
    if (!/^\d+$/.test(s)) return NaN;
    return parseInt(s, 10);
  }

  /* Diálogo propio (sin Bootstrap): se arma, se muestra y se destruye. `guardar(form)` devuelve una promesa {ok, error}. */
  function dialogo(opts) {
    var previo = document.activeElement;
    var velo = document.createElement('div');
    velo.className = 'hzg-velo';
    velo.innerHTML = '<div class="hzg-caja" role="dialog" aria-modal="true" aria-labelledby="hzgTit">'
      + '<div class="hzg-cab"><div><h4 id="hzgTit">' + esc(opts.titulo) + '</h4>' + (opts.sub ? '<p>' + esc(opts.sub) + '</p>' : '') + '</div></div>'
      + '<form class="hzg-cuerpo" novalidate>' + opts.cuerpo + '<div class="hzg-err" role="alert" aria-live="assertive"></div></form>'
      + '<div class="hzg-pie"><button type="button" class="hz-btn sec" data-hzg-cancelar>Cancelar</button>'
      + '<button type="button" class="hz-btn ' + (opts.peligro ? 'peligro' : 'pri') + '" data-hzg-ok><span class="hz-btn-tx">' + esc(opts.ok || 'Guardar') + '</span></button></div></div>';
    document.body.appendChild(velo);
    var form = velo.querySelector('form'), err = velo.querySelector('.hzg-err'), btnOk = velo.querySelector('[data-hzg-ok]');
    function cerrar() { document.removeEventListener('keydown', teclas, true); velo.remove(); try { if (previo && previo.focus) previo.focus(); } catch (e) { /* nada */ } }
    function teclas(ev) { if (ev.key === 'Escape') { ev.preventDefault(); cerrar(); } }
    document.addEventListener('keydown', teclas, true);
    velo.addEventListener('click', function (ev) { if (ev.target === velo) cerrar(); });
    velo.querySelector('[data-hzg-cancelar]').addEventListener('click', cerrar);
    if (opts.alCambiar) form.addEventListener('input', function () { opts.alCambiar(form); });
    function enviar() {
      if (ocupado) return;
      err.textContent = '';
      var pre = opts.validar ? opts.validar(form) : null;
      if (pre) { err.textContent = pre; return; }
      ocupado = true;
      btnOk.disabled = true;
      var tx = btnOk.querySelector('.hz-btn-tx'), original = tx.textContent;
      tx.textContent = 'Guardando…';
      btnOk.setAttribute('aria-busy', 'true');
      opts.guardar(form).then(function (j) {
        ocupado = false;
        if (!j || !j.ok) {
          btnOk.disabled = false; tx.textContent = original; btnOk.removeAttribute('aria-busy');
          err.textContent = (j && j.error) || 'No se pudo guardar.';
          return;
        }
        cerrar();
        if (opts.listo) opts.listo(j);
      });
    }
    btnOk.addEventListener('click', enviar);
    form.addEventListener('submit', function (ev) { ev.preventDefault(); enviar(); });
    if (opts.alCambiar) opts.alCambiar(form);
    setTimeout(function () { var f = form.querySelector('input,textarea,select'); if (f) f.focus(); }, 30);
  }

  function editar(btn) {
    var id = parseInt(btn.getAttribute('data-hz-editar'), 10), d = {};
    try { d = JSON.parse(btn.getAttribute('data-hz-ed') || '{}'); } catch (e) { d = {}; }
    var ops = SEVERIDADES.map(function (s) { return '<option value="' + s[0] + '"' + (s[0] === d.severidad ? ' selected' : '') + '>' + s[1] + '</option>'; }).join('');
    dialogo({
      titulo: 'Editar el hallazgo', sub: 'Corrige cómo se lee el hallazgo. La respuesta del técnico en la OT no se toca. Queda escrito con tu nombre.',
      cuerpo: '<label>Gravedad<select name="severidad">' + ops + '</select></label>'
        + '<label>Título<textarea name="titulo" maxlength="300" rows="2" style="min-height:64px">' + esc(d.titulo) + '</textarea></label>'
        + '<label>Detalle <small>(opcional)</small><textarea name="detalle" maxlength="1500">' + esc(d.detalle) + '</textarea></label>'
        + '<label>Motivo del cambio <small>(obligatorio, mínimo 10 caracteres)</small><textarea name="motivo" maxlength="400" placeholder="Por qué se corrige…"></textarea></label>',
      ok: 'Guardar cambios',
      validar: function (f) {
        if (String(f.titulo.value).trim().length < 3) return 'El título no puede quedar vacío.';
        if (String(f.motivo.value).replace(/\s+/g, ' ').trim().length < 10) return 'Cuenta por qué se corrige (mínimo 10 caracteres).';
        return null;
      },
      guardar: function (f) { return H.api(id + '/editar', { titulo: f.titulo.value, detalle: f.detalle.value, severidad: f.severidad.value, motivo: f.motivo.value }); },
      listo: function () { toast('✓ Hallazgo actualizado. Quedó en su registro de actividad.', 'success'); H.recargar(); }
    });
  }

  function valorizar(btn) {
    var id = parseInt(btn.getAttribute('data-hz-valorizar'), 10), url = btn.getAttribute('data-gc-url');
    var rep = btn.getAttribute('data-hz-rep') || '', srv = btn.getAttribute('data-hz-srv') || '';
    dialogo({
      titulo: 'Valorizar el hallazgo', sub: btn.getAttribute('data-hz-titulo') || '',
      cuerpo: '<p style="margin:0;color:#4b5563;font-size:.86rem">Cuánto estimas que cuesta resolverlo. Es información interna: los técnicos y el cliente no la ven.</p>'
        + '<div class="hzg-fila"><label>Repuestos (pesos)<input name="repuestos" inputmode="numeric" autocomplete="off" placeholder="Ej: 45.990" value="' + esc(rep ? pesos(rep).slice(1) : '') + '"></label>'
        + '<label>Servicio / mano de obra (pesos)<input name="servicio" inputmode="numeric" autocomplete="off" placeholder="Ej: 35.000" value="' + esc(srv ? pesos(srv).slice(1) : '') + '"></label></div>'
        + '<div class="hzg-total" data-hzg-total>Total estimado: $0</div>'
        + '<label>Nota <small>(opcional)</small><input name="nota" maxlength="300" value="' + esc(btn.getAttribute('data-hz-nota') || '') + '" placeholder="Ej: cotizado al proveedor, falta confirmar flete"></label>',
      ok: 'Guardar valorización',
      alCambiar: function (f) {
        var r = monto(f.repuestos.value), s = monto(f.servicio.value);
        var t = f.querySelector('[data-hzg-total]');
        t.textContent = (isNaN(r) || isNaN(s)) ? 'Escribe solo números (por ejemplo 45.990).' : 'Total estimado: ' + pesos((r || 0) + (s || 0));
      },
      validar: function (f) {
        var r = monto(f.repuestos.value), s = monto(f.servicio.value);
        if (isNaN(r) || isNaN(s)) return 'Escribe los montos en pesos, solo números (por ejemplo 45.990).';
        if (r === null && s === null) return 'Escribe al menos un monto: repuestos o servicio.';
        return null;
      },
      guardar: function (f) {
        var cuerpo = { repuestos: f.repuestos.value, servicio: f.servicio.value, nota: f.nota.value };
        return url ? postar(url, cuerpo) : H.api(id + '/valorizar', cuerpo);
      },
      listo: function (j) { toast('✓ Valorizado en ' + (j.total || '') + '.', 'success'); recargar(); }
    });
  }

  function ticket(btn) {
    if (ocupado || !window.ilusConfirm) return;
    var id = parseInt(btn.getAttribute('data-hz-ticket'), 10);
    window.ilusConfirm({
      title: 'Levantar un ticket', message: btn.getAttribute('data-hz-titulo') || 'Hallazgo de la OT',
      sub: 'Se crea un ticket INTERNO con el equipo y lo que reportó el técnico. Al cliente no se le envía nada. Si nadie había tomado el caso, queda a tu nombre.',
      okLabel: 'Levantar ticket', cancelLabel: 'Cancelar', type: 'question'
    }).then(function (ok) {
      if (!ok) return;
      ocupado = true;
      var liberar = H.bloquear(btn, 'Creando…');
      H.api(id + '/ticket', { confirmar: true }).then(function (j) {
        ocupado = false;
        if (!j.ok) { liberar(); toast(j.error, 'error', 8000); return; }
        toast('✓ Ticket ' + (j.numero || '') + ' creado. Quedó ligado al hallazgo.', 'success', 7000);
        H.recargar();
      });
    });
  }

  function borrar(btn) {
    var id = parseInt(btn.getAttribute('data-hz-borrar'), 10);
    dialogo({
      titulo: 'Borrar el hallazgo', sub: btn.getAttribute('data-hz-titulo') || '', peligro: true, ok: 'Borrar definitivamente',
      cuerpo: '<p style="margin:0;color:#991b1b;font-weight:700">Solo para limpiar hallazgos de prueba. Antes de borrar queda una copia completa en el registro, y el mismo hallazgo no volverá a aparecer solo.</p>'
        + '<label>Motivo <small>(obligatorio, mínimo 10 caracteres)</small><textarea name="motivo" maxlength="400" placeholder="Ej: hallazgo de prueba de la OT de capacitación"></textarea></label>'
        + '<label>Escribe BORRAR para confirmar<input name="confirmar" autocomplete="off" autocapitalize="characters"></label>',
      validar: function (f) {
        if (String(f.motivo.value).replace(/\s+/g, ' ').trim().length < 10) return 'Cuenta por qué se borra (mínimo 10 caracteres).';
        if (String(f.confirmar.value).trim().toUpperCase() !== 'BORRAR') return 'Escribe BORRAR para confirmar.';
        return null;
      },
      guardar: function (f) { return H.api(id + '/borrar', { motivo: f.motivo.value, confirmar: f.confirmar.value }); },
      listo: function () { toast('✓ Hallazgo borrado. Quedó la copia en el registro.', 'success'); H.recargar(); }
    });
  }

  document.addEventListener('click', function (ev) {
    var b;
    if ((b = ev.target.closest('[data-hz-valorizar]'))) { ev.preventDefault(); valorizar(b); return; }
    if (!H) return;                                  // lo demás es solo de la tarjeta de hallazgos
    if ((b = ev.target.closest('[data-hz-editar]'))) { ev.preventDefault(); editar(b); return; }
    if ((b = ev.target.closest('[data-hz-ticket]'))) { ev.preventDefault(); ticket(b); return; }
    if ((b = ev.target.closest('[data-hz-borrar]'))) { ev.preventDefault(); borrar(b); return; }
  });
  window.OTHallazgosGestion = { editar: editar, valorizar: valorizar, ticket: ticket, borrar: borrar, monto: monto, pesos: pesos };
})();

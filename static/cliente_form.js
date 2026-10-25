/* 🧾 Editar cliente — semáforo, pasos y validaciones en vivo · 2026-10-24
 * Solo lee y marca los campos del modal #modalEditarCliente (ids ec_*): NO cambia lo que se guarda (guardarCliente() de mant_ficha.js sigue
 * igual) ni bloquea el guardado. Cada campo pasa de ROJO (falta o está mal) a VERDE (válido); la dirección solo es «verificada» si se eligió
 * una sugerencia de Google Places (place_id) y el texto no se cambió después. Cada tarjeta de paso: rojo → verde con check; opcional vacío → ámbar.
 */
(function () {
  'use strict';
  var $ = function (id) { return document.getElementById(id); };
  function val(id) { var e = $(id); return e ? String(e.value || '').trim() : ''; }

  // RUT chileno: módulo 11 (mismo criterio que el servidor).
  function rutOk(s) {
    var c = String(s || '').replace(/[^0-9kK]/g, '').toUpperCase();
    if (c.length < 2) return false;
    var cuerpo = c.slice(0, -1), dv = c.slice(-1), suma = 0, m = 2;
    for (var i = cuerpo.length - 1; i >= 0; i--) { suma += parseInt(cuerpo.charAt(i), 10) * m; m = (m === 7) ? 2 : m + 1; }
    var r = 11 - (suma % 11), esp = (r === 11) ? '0' : (r === 10 ? 'K' : String(r));
    return dv === esp;
  }
  function emailOk(s) { return /^[^\s@]+@[^\s@]+\.[^\s@]{2,}$/.test(String(s || '')); }
  function telOk(s) {
    var d = String(s || '').replace(/\D/g, '');
    if (d.indexOf('56') === 0 && d.length >= 11) d = d.slice(2);
    return d.length === 9 || d.length === 8;
  }

  var verificada = '';        // texto de la dirección tal como Google la devolvió (o la que ya venía verificada al abrir)

  function marca(id, estado, msg) {
    var e = $(id); if (!e) return;
    e.classList.remove('ecf-ok', 'ecf-bad', 'ecf-warn');
    if (estado) e.classList.add('ecf-' + estado);
    var m = document.querySelector('[data-ecf-msg="' + id + '"]');
    if (m) { m.textContent = msg || ''; m.className = 'ecf-msg' + (estado ? ' ' + estado : ''); }
  }
  // Campo opcional con formato: vacío = sin marca; válido = verde; inválido = rojo.
  function opcional(id, okFn, msgOk, msgMal) {
    var v = val(id);
    if (!v) { marca(id, '', ''); return null; }
    if (okFn(v)) { marca(id, 'ok', msgOk || ''); return true; }
    marca(id, 'bad', msgMal || 'Revisa este dato.'); return false;
  }
  function paso(n, estado) {   // estado: 'complete' | 'pending' | 'neutral'
    var s = $('ecfSec' + n); if (!s) return;
    s.classList.toggle('is-complete', estado === 'complete');
    s.classList.toggle('is-neutral', estado === 'neutral');
  }

  function evaluar() {
    if (!$('modalEditarCliente')) return;
    // ── Paso 1 · Empresa ───────────────────────────────────────────
    var razonOk = val('ec_razon').length >= 3;
    marca('ec_razon', razonOk ? 'ok' : 'bad', razonOk ? '' : (val('ec_razon') ? 'Escribe el nombre completo de la empresa.' : 'La razón social es obligatoria.'));
    var rut = val('ec_rut'), rutValido = !!rut && rutOk(rut);
    if (!rut) marca('ec_rut', 'warn', 'Recomendado: con el RUT se cruza con el ERP y los documentos.');
    else marca('ec_rut', rutValido ? 'ok' : 'bad', rutValido ? 'RUT válido.' : 'El dígito verificador no calza.');
    var dia = val('ec_dia_pref');
    if (!dia) marca('ec_dia_pref', '', '');
    else { var dn = parseInt(dia, 10); marca('ec_dia_pref', (dn >= 1 && dn <= 28) ? 'ok' : 'bad', (dn >= 1 && dn <= 28) ? '' : 'Debe ser un día entre 1 y 28.'); }
    opcional('ec_email_empresa', emailOk, 'Correo válido.', 'Le falta la @ o el dominio.');
    opcional('ec_tel_empresa', telOk, 'Teléfono válido.', 'Teléfono incompleto (ej: +56 2 2345 6789).');
    var s1 = razonOk && rutValido;
    paso(1, s1 ? 'complete' : 'pending');

    // ── Paso 2 · Ubicación (Google Places) ─────────────────────────
    var dir = val('ec_direccion'), pid = val('ec_direccion_place_id');
    var verif = !!dir && !!pid && (dir === verificada);
    var caja = $('ecfVerif');
    if (caja) {
      if (!dir) { caja.className = 'ecf-verif bad'; caja.innerHTML = '<i class="bi bi-geo-alt"></i><span>Falta la dirección. Escríbela y elige una sugerencia de Google.</span>'; }
      else if (verif) { caja.className = 'ecf-verif ok'; caja.innerHTML = '<i class="bi bi-check-circle-fill"></i><span><b>Dirección verificada con Google.</b> Comuna, ciudad y región se completan solas.</span>'; }
      else if (pid) { caja.className = 'ecf-verif warn'; caja.innerHTML = '<i class="bi bi-exclamation-triangle-fill"></i><span><b>Cambiaste la dirección.</b> Elige otra sugerencia del menú para volver a verificarla.</span>'; }
      else { caja.className = 'ecf-verif warn'; caja.innerHTML = '<i class="bi bi-exclamation-triangle-fill"></i><span><b>Sin verificar.</b> Selecciona una sugerencia del menú de Google para validarla.</span>'; }
    }
    marca('ec_direccion', !dir ? 'bad' : (verif ? 'ok' : 'warn'), '');
    var reg = val('ec_region'), com = val('ec_comuna'), ciu = val('ec_ciudad');
    marca('ec_region', reg ? 'ok' : '', ''); marca('ec_comuna', com ? 'ok' : '', ''); marca('ec_ciudad', ciu ? 'ok' : '', '');
    var s2 = verif && !!reg && !!com;
    paso(2, s2 ? 'complete' : 'pending');

    // ── Paso 3 · Contacto principal ────────────────────────────────
    var nom = val('ec_contacto_nombre').length >= 3;
    marca('ec_contacto_nombre', nom ? 'ok' : 'bad', nom ? '' : 'Indica con quién hablar.');
    marca('ec_contacto_cargo', val('ec_contacto_cargo') ? 'ok' : '', '');
    ['ec_contacto_cargo', 'ec_contacto2_cargo'].forEach(function (id) { var c = $(id); if (c) c.title = c.value; });   // el cargo completo, aunque no quepa en el campo
    var tel = opcional('ec_contacto_tel', telOk, 'Teléfono válido.', 'Teléfono incompleto (ej: +56 9 1234 5678).');
    var mail = opcional('ec_contacto_email', emailOk, 'Correo válido.', 'Le falta la @ o el dominio.');
    var viaOk = (tel === true) || (mail === true);
    var s3 = nom && viaOk && tel !== false && mail !== false;
    if (nom && !viaOk) {
      var m = document.querySelector('[data-ecf-msg="ec_contacto_tel"]');
      if (m && !m.textContent) { m.textContent = 'Falta un teléfono o un correo para contactarlo.'; m.className = 'ecf-msg warn'; }
    }
    paso(3, s3 ? 'complete' : 'pending');

    // ── Paso 4 · Ejecutivo ─────────────────────────────────────────
    var ej = val('ec_ejecutivo_user_id');
    marca('ec_ejecutivo_user_id', ej ? 'ok' : 'warn', ej ? '' : 'Sin responsable asignado: nadie se encarga de comunicarse con este cliente.');
    paso(4, ej ? 'complete' : 'pending');

    // ── Paso 5 · Contacto secundario (opcional) ────────────────────
    var n2 = val('ec_contacto2_nombre'), t2 = val('ec_contacto2_tel'), e2 = val('ec_contacto2_email'), c2 = val('ec_contacto2_cargo');
    var alguno = !!(n2 || t2 || e2 || c2);
    var t2ok = opcional('ec_contacto2_tel', telOk, 'Teléfono válido.', 'Teléfono incompleto.');
    var e2ok = opcional('ec_contacto2_email', emailOk, 'Correo válido.', 'Le falta la @ o el dominio.');
    marca('ec_contacto2_nombre', n2 ? (n2.length >= 3 ? 'ok' : 'bad') : '', '');
    marca('ec_contacto2_cargo', c2 ? 'ok' : '', '');
    var s5 = n2.length >= 3 && (t2ok === true || e2ok === true) && t2ok !== false && e2ok !== false;
    paso(5, !alguno ? 'neutral' : (s5 ? 'complete' : 'pending'));

    // ── Paso 6 · Notas (opcional) ──────────────────────────────────
    paso(6, val('ec_notas') ? 'complete' : 'neutral');

    // Progreso de la cabecera: los 4 pasos que importan.
    var listos = [s1, s2, s3, !!ej].filter(Boolean).length;
    var t = $('ecfProgTxt'), b = $('ecfProgBar'), p = document.querySelector('.ecf-prog');
    if (t) t.textContent = listos + ' de 4 pasos listos';
    if (b) b.style.width = (listos * 25) + '%';
    if (p) p.classList.toggle('ok', listos === 4);
  }

  var _tm = 0;   // setTimeout y no requestAnimationFrame: este último no corre con la pestaña en segundo plano
  function evaluarPronto() { if (_tm) return; _tm = setTimeout(function () { _tm = 0; try { evaluar(); } catch (e) { if (window.console) console.warn('[ecf]', e); } }, 30); }

  function iniciar() {
    var m = $('modalEditarCliente'); if (!m || m.dataset.ecf === '1') return;
    m.dataset.ecf = '1';
    m.addEventListener('input', evaluarPronto);
    m.addEventListener('change', evaluarPronto);
    m.addEventListener('blur', evaluarPronto, true);
    m.addEventListener('show.bs.modal', function () {
      verificada = (val('ec_direccion_place_id') ? val('ec_direccion') : '');
      evaluar();
    });
    m.addEventListener('shown.bs.modal', function () { evaluar(); setTimeout(evaluar, 600); });   // el selector de ejecutivos se llena por fetch
    // Al cambiar el texto de la dirección a mano, deja de estar «verificada» (el place_id no se borra: igual que antes al guardar).
    var d = $('ec_direccion');
    if (d) d.addEventListener('input', evaluarPronto);
  }

  // Lo llama mant_ficha.js cuando Google Places entrega una sugerencia: esa dirección queda verificada.
  window.ecfDireccionVerificada = function (texto) { verificada = String(texto || val('ec_direccion')); evaluar(); };
  window.ecfEvaluar = evaluar;

  // Datos ESCRITOS pero mal (rojo con valor): guardarCliente() los muestra antes de guardar y la persona decide («alertar, no trabar»).
  // Un campo vacío no entra: faltar un dato es de la tarjeta de paso (rojo/ámbar), no un error de escritura.
  var ETIQUETAS = {
    ec_razon: 'Razón social', ec_rut: 'RUT', ec_dia_pref: 'Día de mantención',
    ec_email_empresa: 'Email de la empresa', ec_tel_empresa: 'Teléfono de la empresa',
    ec_contacto_tel: 'Contacto principal · teléfono', ec_contacto_email: 'Contacto principal · email',
    ec_contacto2_nombre: 'Contacto secundario · nombre', ec_contacto2_tel: 'Contacto secundario · teléfono',
    ec_contacto2_email: 'Contacto secundario · email'
  };
  window.ecfProblemas = function () {
    evaluar();
    var out = [];
    Object.keys(ETIQUETAS).forEach(function (id) {
      var e = $(id);
      if (!e || !val(id) || !e.classList.contains('ecf-bad')) return;
      var m = document.querySelector('[data-ecf-msg="' + id + '"]');
      out.push({ id: id, etiqueta: ETIQUETAS[id], msg: (m && m.textContent) || 'Revisa este dato.' });
    });
    return out;
  };

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', iniciar); else iniciar();
})();

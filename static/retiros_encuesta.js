/* ILUS Fitness · Encuesta de satisfacción de Retiros — comportamiento del formulario público (dinámico).
   Sin dependencias. El formulario funciona sin JS (POST normal); esto solo agrega: progreso, tarjetas que se
   ponen verdes al completarse, contadores de caracteres y la guía hacia lo primero que falta al intentar enviar. */
(function () {
  'use strict';
  var form = document.getElementById('encForm');
  if (!form) return;

  var TOTAL = parseInt(form.getAttribute('data-total') || '0', 10);
  var txtProg = document.getElementById('encProgresoTxt');
  var hintProg = document.getElementById('encProgresoHint');
  var barra = document.getElementById('encBarra');
  var barraRol = form.querySelector('[role="progressbar"]');
  var faltan = document.getElementById('encFaltan');
  var consent = document.getElementById('consentimiento');
  var pasos = Array.prototype.slice.call(form.querySelectorAll('.step-section[data-pid]'));

  function respondida(sec) {
    var tipo = sec.getAttribute('data-tipo');
    if (tipo === 'texto') {
      var ta = sec.querySelector('textarea');
      return !!(ta && ta.value.trim().length);
    }
    return !!sec.querySelector('input[type="radio"]:checked');
  }
  function requerida(sec) { return sec.getAttribute('data-obligatoria') === '1'; }

  function refrescar() {
    var hechas = 0;
    pasos.forEach(function (sec) {
      var ok = respondida(sec);
      sec.classList.toggle('is-complete', ok);
      if (ok) sec.classList.remove('has-error');
      if (ok && requerida(sec)) hechas++;
    });
    if (txtProg) txtProg.textContent = hechas + ' de ' + TOTAL + ' respondidas';
    if (barra) barra.style.width = (TOTAL ? Math.round(100 * hechas / TOTAL) : 100) + '%';
    if (barraRol) barraRol.setAttribute('aria-valuenow', String(hechas));
    var listo = hechas === TOTAL && (!consent || consent.checked);
    form.classList.toggle('is-done', listo);
    if (hintProg) {
      hintProg.textContent = hechas === TOTAL ? (listo ? '¡Listo para enviar!' : 'Falta aceptar el aviso') :
        (hechas === 0 ? 'Tu opinión cuenta' : 'Vas muy bien');
    }
    if (listo && faltan) faltan.textContent = '';
    var cons = document.getElementById('encConsent');
    if (cons && consent && consent.checked) cons.classList.remove('has-error');
  }

  function contador(ta) {
    var id = ta.getAttribute('data-contador');
    var el = id && document.getElementById(id);
    if (el) el.textContent = String(ta.value.length);
  }

  form.addEventListener('change', refrescar);
  form.addEventListener('input', function (e) {
    if (e.target && e.target.tagName === 'TEXTAREA') { contador(e.target); refrescar(); }
  });
  Array.prototype.forEach.call(form.querySelectorAll('textarea[data-contador]'), contador);

  form.addEventListener('submit', function (e) {
    var pendientes = pasos.filter(function (s) { return requerida(s) && !respondida(s); });
    var sinConsent = consent && !consent.checked;
    if (!pendientes.length && !sinConsent) return;       // todo en orden: se envía normal
    e.preventDefault();
    pasos.forEach(function (s) { s.classList.toggle('has-error', requerida(s) && !respondida(s)); });
    var cons = document.getElementById('encConsent');
    if (cons) cons.classList.toggle('has-error', !!sinConsent);
    if (faltan) {
      var partes = [];
      if (pendientes.length) partes.push(pendientes.length === 1 ? 'Te falta 1 respuesta' : 'Te faltan ' + pendientes.length + ' respuestas');
      if (sinConsent) partes.push('acepta el aviso de privacidad');
      faltan.textContent = partes.join(' y ') + '.';
    }
    var destino = pendientes.length ? pendientes[0] : cons;
    var campo = destino && destino.querySelector('input, textarea');
    var reduce = window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches;
    if (destino) destino.scrollIntoView({ behavior: reduce ? 'auto' : 'smooth', block: 'center' });
    if (campo) { try { campo.focus({ preventScroll: true }); } catch (_) { campo.focus(); } }
  });

  refrescar();
})();

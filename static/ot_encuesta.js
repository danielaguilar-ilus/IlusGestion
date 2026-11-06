/* ILUS Fitness · Encuesta corta de la OT (página pública). El formulario funciona sin JavaScript (POST normal); esto solo agrega:
   la etiqueta de la nota elegida, el contador del comentario y la guía si falta la nota. Sin dependencias. REGLA #68. */
(function () {
  'use strict';
  var form = document.getElementById('oeForm');
  if (!form) return;

  var elegida = document.getElementById('oeElegida');
  var faltan = document.getElementById('oeFaltan');
  var paso = document.getElementById('oePaso1');
  var boton = document.getElementById('oeEnviar');
  var ta = document.getElementById('oeComentario');
  var cuenta = document.getElementById('oeCuenta');

  function marcada() { return form.querySelector('input[name="n"]:checked'); }

  function refrescar() {
    var r = marcada();
    form.classList.toggle('is-done', !!r);
    if (r) {
      if (paso) paso.classList.remove('has-error');
      if (faltan) faltan.textContent = '';
      var lab = form.querySelector('label[for="' + r.id + '"]');
      var etiqueta = lab ? lab.getAttribute('data-etiqueta') : '';
      if (elegida) {
        elegida.textContent = '';
        var txt = document.createTextNode('Tu nota: ');
        var fuerte = document.createElement('strong');
        fuerte.textContent = r.value + (etiqueta ? ' · ' + etiqueta : '');
        elegida.appendChild(txt);
        elegida.appendChild(fuerte);
      }
    }
  }

  function contar() { if (ta && cuenta) cuenta.textContent = String(ta.value.length); }

  form.addEventListener('change', refrescar);
  if (ta) ta.addEventListener('input', contar);
  contar();
  refrescar();
  // volver con «Atrás» no deja el botón trabado en «Enviando…»
  window.addEventListener('pageshow', function () { if (boton) { boton.disabled = false; boton.textContent = 'Enviar'; } });

  form.addEventListener('submit', function (e) {
    if (!marcada()) {
      e.preventDefault();
      if (paso) paso.classList.add('has-error');
      if (faltan) faltan.textContent = 'Elige una nota del 1 al 5.';
      var primero = form.querySelector('input[name="n"]');
      var reduce = window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches;
      if (paso) paso.scrollIntoView({ behavior: reduce ? 'auto' : 'smooth', block: 'center' });
      if (primero) { try { primero.focus({ preventScroll: true }); } catch (_) { primero.focus(); } }
      return;
    }
    // un solo envío: el segundo toque no repite la petición (el servidor igual guarda una sola vez)
    if (boton) { boton.disabled = true; boton.textContent = 'Enviando…'; }
  });
})();

/* Ficha del retiro v8 (Daniel 2026-10-02): «el detalle del cliente no necesito que esté en una tarjeta: arriba tiene un header…
   pero sí la opción de editar si alguien adicional va a buscar el pedido».
   Los datos del cliente viven en el encabezado y se editan en el modal «Datos del cliente y de quién retira» (#modalFichaEditar):
   mismos campos data-inline-edit de siempre, que se guardan solos (retiros_internal_detail.js). Aquí solo se mantiene el
   encabezado al día mientras se edita y se enfoca «Persona que retira» cuando se abre desde ese botón. No escribe nada por sí solo. */
(function () {
  'use strict';
  var modal = document.getElementById('modalFichaEditar');
  if (!modal) return;

  function dato(campo) {
    var el = modal.querySelector('[data-inline-edit="' + campo + '"]');
    return el ? el.textContent.replace(/\s+/g, ' ').trim() : '';
  }
  function rutFmt(r) {                       // 184338726 → 18.433.872-6
    var c = String(r || '').replace(/[^0-9kK]/g, '').toUpperCase();
    if (c.length < 2) return c;
    return c.slice(0, -1).replace(/\B(?=(\d{3})+(?!\d))/g, '.') + '-' + c.slice(-1);
  }
  function waNumero(tel) {
    var d = String(tel || '').replace(/\D/g, '');
    return d.length === 9 ? '56' + d : d;
  }
  function fijar(id, texto, oculto) {
    var el = document.getElementById(id);
    if (!el) return;
    if (texto != null) el.textContent = texto;
    if (oculto != null) el.hidden = !!oculto;
  }

  function sincronizarEncabezado() {
    var nombre = dato('customer_name'), rut = dato('customer_rut'), tel = dato('contact_phone'), mail = dato('contact_email'),
        cnom = dato('contact_name'), rnom = dato('pickup_person_name');
    var h1 = document.querySelector('.rh-cli');
    if (h1 && nombre) h1.textContent = nombre;
    fijar('rhRut', rut ? 'RUT ' + rutFmt(rut) : '', !rut);
    fijar('rhTelTxt', tel, null); fijar('rhTel', null, !tel);
    var contacto = (cnom || '') + (cnom && mail ? ' · ' : '') + (mail || '');
    fijar('rhContactoTxt', contacto, null); fijar('rhContacto', null, !contacto);
    var mismo = rnom && nombre && rnom.toLowerCase() === nombre.toLowerCase();
    fijar('heroRetiraTxt', mismo ? 'El mismo cliente' : (rnom || '—'), null);
    var llamar = document.querySelector('a.rh-btn.call'), wa = document.querySelector('a.rh-btn.wa');
    if (llamar && tel) llamar.href = 'tel:' + tel.replace(/\s+/g, '');
    if (wa && tel) wa.href = 'https://wa.me/' + waNumero(tel);
  }

  // El encabezado sigue lo que se escribe (si el guardado falla, el campo vuelve a su valor y el encabezado también)
  var pendiente = null;
  function programar() {
    clearTimeout(pendiente);
    pendiente = setTimeout(sincronizarEncabezado, 250);
  }
  if (window.MutationObserver) {
    new MutationObserver(programar).observe(modal, { subtree: true, childList: true, characterData: true });
  }
  modal.addEventListener('hidden.bs.modal', sincronizarEncabezado);

  // Desde «Quién retira» o «Corregir quién retira»: el cursor cae directo en la persona que retira
  modal.addEventListener('shown.bs.modal', function (ev) {
    var origen = ev.relatedTarget;
    if (!origen || !origen.closest) return;
    if (origen.closest('#heroRetiraTile') || origen.closest('.fv8-corregir')) {
      var campo = modal.querySelector('[data-inline-edit="pickup_person_name"]');
      var tarjeta = modal.querySelector('.fv5-ret');
      if (tarjeta) { tarjeta.classList.add('fv8-foco'); setTimeout(function () { tarjeta.classList.remove('fv8-foco'); }, 2200); }
      if (campo) campo.focus();
    }
  });
})();

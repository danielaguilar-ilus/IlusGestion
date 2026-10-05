/* ══════════════════════════════════════════════════════════════════
   Franja del prospecto: tarjeta de Clientes y ficha del cliente.
   Daniel 2026-10-05: «ofrecer en ticket nuevo… para que la conversación
   sea fresca» y «solo el paso que toca». Todo queda en el ticket de la
   oferta. Nada sale al cliente solo: el WhatsApp lo envía la persona
   desde su teléfono y el correo sale desde el modal de la ficha.
   HTML de la franja: templates/mantenciones/_prosp_strip.html.
   ══════════════════════════════════════════════════════════════════ */
(function () {
  'use strict';

  function telWa(tel) {
    const d = String(tel || '').replace(/\D/g, '');
    if (d.startsWith('56') && d.length >= 11) return d;
    if (d.length === 9) return '56' + d;
    if (d.length === 8) return '569' + d;
    return '';
  }

  async function post(url, body) {
    const r = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' },
                                 body: JSON.stringify(body || {}) });
    let d = {};
    try { d = await r.json(); } catch (e) { /* respuesta sin JSON */ }
    if (!r.ok || !d.ok) throw new Error(d.error || 'No se pudo completar la acción. Intenta de nuevo.');
    return d;
  }

  // Vuelve a pintar la franja con el paso nuevo (en la ficha se recarga: hay más partes que cambian).
  async function refrescar(cid) {
    if (window.PROSP_EN_FICHA) { setTimeout(function () { location.reload(); }, 700); return; }
    const el = document.querySelector('.cc2-prosp[data-prosp-cid="' + cid + '"]');
    if (!el) return;
    try {
      const r = await fetch('/mantenciones/api/clientes/' + cid + '/prospecto/tarjeta');
      const d = await r.json();
      if (!d.ok || !d.html) return;
      const tmp = document.createElement('div');
      tmp.innerHTML = d.html.trim();
      const nuevo = tmp.firstElementChild;
      if (nuevo) { el.replaceWith(nuevo); nuevo.classList.add('pflash'); }
    } catch (e) { /* si falla, la próxima carga de la página la muestra al día */ }
  }

  // Mensaje de WhatsApp del cuadro de mando + link de la propuesta (para que responda con un clic).
  function armarMensaje(btn) {
    const nom = (btn.dataset.nom || '').trim() || 'equipo';
    let msg = String(btn.dataset.msg || '').split('{{contacto}}').join(nom).split('{{cliente}}').join(btn.dataset.cli || '');
    const link = btn.dataset.link || '';
    if (msg.indexOf('{{link}}') >= 0) msg = msg.split('{{link}}').join(link);
    else if (link) msg = msg.trim() + '\n\nPuedes responder con un clic aquí: ' + link;
    return msg.trim();
  }

  window.prospOfrecer = async function (ev, btn) {
    ev.stopPropagation();
    const cid = btn.dataset.cid;
    const re = btn.dataset.re === '1';
    const ok = await ilusConfirm({
      title: re ? 'Volver a ofrecer mantención' : 'Ofrecer mantención',
      message: 'Se abre un ticket nuevo para la oferta a ' + (btn.dataset.cli || 'este cliente') + '.',
      sub: 'Toda la conversación de la oferta queda en ese ticket. No se envía nada al cliente: después eliges cómo contactarlo.',
      okLabel: 'Abrir ticket de la oferta', cancelLabel: 'Cancelar', type: 'question',
    });
    if (!ok) return;
    btn.disabled = true;
    try {
      const d = await post('/mantenciones/api/clientes/' + cid + '/prospecto/iniciar');
      ilusToast('✓ ' + d.mensaje, { type: 'success' });
      await refrescar(cid);
    } catch (e) { ilusToast(e.message, { type: 'error' }); btn.disabled = false; }
  };

  window.prospContactar = async function (ev, btn, canal) {
    ev.stopPropagation();
    const cid = btn.dataset.cid;
    let mensaje = '';
    if (canal === 'whatsapp') {
      const num = telWa(btn.dataset.tel);
      if (!num) { ilusToast('El teléfono de este cliente no es válido para WhatsApp. Revísalo en su ficha.', { type: 'warning' }); return; }
      mensaje = armarMensaje(btn);
      window.open('https://wa.me/' + num + '?text=' + encodeURIComponent(mensaje), '_blank');
    } else if (canal === 'llamada') {
      window.location.href = 'tel:' + String(btn.dataset.tel || '').replace(/[^\d+]/g, '');
    }
    try {
      const d = await post('/mantenciones/api/clientes/' + cid + '/prospecto/contacto',
                           { canal: canal, mensaje: mensaje, tel: btn.dataset.tel || '' });
      ilusToast('✓ Anotado en el ticket ' + ((d.ticket && d.ticket.numero) || 'de la oferta') +
                ' · próxima gestión ' + d.proxima_gestion, { type: 'success' });
      await refrescar(cid);
    } catch (e) { ilusToast(e.message, { type: 'error' }); }
  };

  const RESPUESTAS = {
    si: { title: 'El cliente quiere la cotización', okLabel: 'Sí, registrar', type: 'success',
          sub: 'Queda anotado en el ticket de la oferta. Siguiente paso: armar la cotización con sus equipos.' },
    acepta: { title: 'El cliente aceptó la cotización', okLabel: 'Registrar aceptación', type: 'success',
              sub: 'Úsalo cuando aceptó por teléfono, WhatsApp o correo. Siguiente paso: el contrato.' },
    no: { title: '«No por ahora»', okLabel: 'Registrar y cerrar la oferta', type: 'warning', danger: true,
          sub: 'El ticket de la oferta se cierra. Más adelante puedes volver a ofrecer: se abre otro ticket.' },
  };

  window.prospRespuesta = async function (ev, btn, resp) {
    ev.stopPropagation();
    const cid = btn.dataset.cid;
    const t = RESPUESTAS[resp];
    if (!t) return;
    const ok = await ilusConfirm({ title: t.title, message: btn.dataset.cli || '', sub: t.sub,
                                   okLabel: t.okLabel, cancelLabel: 'Cancelar', type: t.type, danger: !!t.danger });
    if (!ok) return;
    btn.disabled = true;
    try {
      const d = await post('/mantenciones/api/clientes/' + cid + '/prospecto/respuesta', { respuesta: resp });
      ilusToast('✓ ' + d.mensaje, { type: 'success' });
      await refrescar(cid);
    } catch (e) { ilusToast(e.message, { type: 'error' }); btn.disabled = false; }
  };
})();

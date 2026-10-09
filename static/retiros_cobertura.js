/* Horario de cobertura de Retiros (Daniel 2026-10-06): «la cobertura día a día de 8 de la mañana… y no mostrar este mensaje a cinco de la tarde,
   con descanso a la una de la tarde». Fuera de ese horario nadie puede responderle al cliente: antes de gestionar un retiro a mano se AVISA.
   Es solo un aviso: no bloquea ni cambia nada. El horario lo calcula el servidor (/retiros/api/cobertura) en hora Chile, con feriados y cierres. */
(function () {
  'use strict';
  var memo = { t: 0, d: null };
  window.retirosCobertura = async function () {
    if (memo.d && Date.now() - memo.t < 45000) return memo.d;
    try {
      var r = await fetch('/retiros/api/cobertura', { credentials: 'same-origin', cache: 'no-store' });
      var d = await r.json();
      if (d && d.ok) { memo = { t: Date.now(), d: d }; return d; }
    } catch (e) { /* sin red: no se avisa nada, nunca se bloquea */ }
    return null;
  };
  // Qué le llega al cliente al pasar a cada estado (los mismos de kind_map en pickup_update_status)
  window.RETIROS_CORREO_POR_ESTADO = {
    agenda_confirmada: 'la confirmación de su cita',
    reagendada: 'el aviso de que su retiro se reagendó',
    rechazada: 'el aviso de que su retiro fue rechazado',
    fallida: 'el aviso de que no se pudo completar su retiro',
    informacion_incompleta: 'un correo pidiéndole la información que falta',
    esperando_cliente: 'un correo pidiéndole la información que falta',
    en_preparacion: 'el correo «estamos preparando tu pedido»',
    retirada: 'el correo «retiro completado»'
  };
  // Aviso CÓMODO antes de un paso que le escribe al cliente (Daniel 2026-10-06: «solo una alerta de que van a generar un correo y se pueden
  // retractar»): Enter = continuar · Esc o clic afuera = volver sin enviar nada. Devuelve true/false. Si no hay ilusConfirm, no bloquea.
  window.retirosAvisoCorreo = async function (o) {
    o = o || {};
    if (typeof window.ilusConfirm !== 'function') return true;
    var fuera = await window.retirosAvisoCobertura();
    return window.ilusConfirm({
      title: o.titulo || '📧 Esto le envía un correo al cliente',
      message: o.mensaje || ('Al cliente le llega ' + (o.que || 'un aviso') + '.'),
      sub: (fuera ? '⏰ ' + fuera + ' ' : '') + 'Enter para continuar · Esc o clic afuera para volver sin enviar nada.',
      okLabel: o.ok || 'Continuar', cancelLabel: 'Volver',
      type: fuera ? 'warning' : 'info'
    });
  };
  // Texto del aviso, o '' si hay cobertura (o no se pudo saber)
  window.retirosAvisoCobertura = async function () {
    var d = await window.retirosCobertura();
    if (!d || d.abierta !== false || !d.motivo) return '';          // solo con un «fuera de horario» explícito del servidor
    return d.motivo + '. Cobertura: lunes a viernes hábiles ' + d.horario + '. El cliente recibirá el aviso ahora, pero nadie podrá responderle' +
      (d.vuelve ? ' hasta ' + d.vuelve : '') + '.';
  };
})();

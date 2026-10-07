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
  // Texto del aviso, o '' si hay cobertura (o no se pudo saber)
  window.retirosAvisoCobertura = async function () {
    var d = await window.retirosCobertura();
    if (!d || d.abierta) return '';
    return d.motivo + '. Cobertura: lunes a viernes hábiles ' + d.horario + '. El cliente recibirá el aviso ahora, pero nadie podrá responderle' +
      (d.vuelve ? ' hasta ' + d.vuelve : '') + '.';
  };
})();

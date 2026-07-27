(function (global) {
  'use strict';

  function notifyError(message) {
    if (typeof global.ilusToast === 'function') {
      global.ilusToast(message, { type: 'error' });
    }
  }

  function openSimpliRouteActions(trigger) {
    var rawData = trigger.dataset.srItem || '';
    var data;
    try {
      data = JSON.parse(rawData);
    } catch (error) {
      notifyError('No se pudieron leer los datos de seguimiento. Recarga la pagina.');
      return;
    }

    if (typeof global.abrirSimpliRouteModal !== 'function') {
      notifyError('No se pudo abrir el seguimiento. Recarga la pagina.');
      return;
    }
    global.abrirSimpliRouteModal(data);
  }

  function bind() {
    if (global.__ilusManifestActionsBound) return;
    global.__ilusManifestActionsBound = true;

    // La celda de acciones detiene el bubbling para no desplegar la factura.
    document.addEventListener('click', function (event) {
      var trigger = event.target.closest('.js-open-simpliroute-actions');
      if (!trigger) return;
      event.preventDefault();
      openSimpliRouteActions(trigger);
    }, true);
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', bind);
  } else {
    bind();
  }
})(window);

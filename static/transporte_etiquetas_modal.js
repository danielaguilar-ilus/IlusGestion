(function (global) {
  'use strict';

  var modalInstance = null;
  var lastTrigger = null;

  function elements() {
    return {
      modal: document.getElementById('transportLabelsModal'),
      frame: document.getElementById('transportLabelsFrame'),
      title: document.getElementById('transportLabelsModalTitleText'),
      loading: document.getElementById('transportLabelsModalLoading')
    };
  }

  function embeddedUrl(rawUrl) {
    var url = new URL(rawUrl, global.location.origin);
    url.searchParams.set('embed', '1');
    return url.toString();
  }

  function open(rawUrl, title, trigger) {
    var el = elements();
    if (!el.modal || !el.frame || !global.bootstrap) {
      if (typeof global.ilusToast === 'function') {
        global.ilusToast('No se pudo abrir el editor de etiquetas. Recarga la pagina.', {
          type: 'error'
        });
      }
      return;
    }

    lastTrigger = trigger || document.activeElement;
    el.title.textContent = title || 'Etiquetas del manifiesto';
    el.loading.hidden = false;
    el.frame.src = embeddedUrl(rawUrl);
    modalInstance = modalInstance || global.bootstrap.Modal.getOrCreateInstance(el.modal);
    modalInstance.show();
  }

  function bind() {
    var el = elements();
    if (!el.modal || !el.frame) return;

    document.addEventListener('click', function (event) {
      var trigger = event.target.closest('.js-open-etiquetas-modal');
      if (!trigger) return;
      event.preventDefault();
      open(
        trigger.getAttribute('href') || trigger.dataset.etiquetasUrl,
        trigger.dataset.etiquetasTitle,
        trigger
      );
    });

    el.frame.addEventListener('load', function () {
      if (el.frame.src !== 'about:blank') el.loading.hidden = true;
    });

    el.modal.addEventListener('hidden.bs.modal', function () {
      el.frame.src = 'about:blank';
      el.loading.hidden = false;
      if (lastTrigger && typeof lastTrigger.focus === 'function') lastTrigger.focus();
      lastTrigger = null;
    });
  }

  global.ilusEtiquetasModal = { open: open };
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', bind);
  } else {
    bind();
  }
})(window);

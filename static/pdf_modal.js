/* ════════════════════════════════════════════════════════════════════
   pdf_modal.js — Modal global de vista previa de PDF (ILUS)
   ────────────────────────────────────────────────────────────────────
   Extraído desde print_labels.html (Etiquetas, único lugar donde existía
   antes) y generalizado para TODO el proyecto: cotizaciones de Servicio
   Técnico y de Transporte, PDF de OT en Mantenciones, y las propias
   Etiquetas. El HTML del modal vive en templates/_partials/pdf_modal.html,
   incluido una sola vez desde base.html.

   Uso:
     openPdf(url)
     openPdf(url, {
       openUrl:        '...',   // "Abrir en pestaña nueva" (default: url)
       downloadUrl:    '...',   // "Descargar" (default: url + "download=1")
       downloadName:   '...',   // nombre sugerido del archivo descargado
       title:          '...',   // título del modal
       loadingMessage: '...',   // texto bajo la barra de progreso
       safetyTimeout:  15000,   // ms antes de ocultar la barra si el
                                 // iframe nunca dispara "load"
     })
     closePdf()
     printPdf()   // dispara el diálogo de impresión del PDF cargado

   Compat legacy (Etiquetas): cualquier elemento con
     class="js-preview-pdf" data-pdf-url="..."
   se cablea solo por delegación de eventos — no hace falta llamar
   openPdf() a mano para esos triggers.
   ════════════════════════════════════════════════════════════════════ */

(function (global) {
  'use strict';

  const DEFAULT_TITLE = 'Vista previa del documento';
  const DEFAULT_LOADING_MSG = 'Generando documento…';
  const SAFETY_TIMEOUT_MS = 15000;

  let _safetyTimer = null;

  function _els() {
    return {
      modal: document.getElementById('pdfModal'),
      frame: document.getElementById('pdfFrame'),
      openLink: document.getElementById('pdfOpen'),
      downloadLink: document.getElementById('pdfDownload'),
      printBtn: document.getElementById('pdfPrint'),
      titleEl: document.getElementById('pdfTitle'),
      loading: document.getElementById('pdfLoading'),
      loadingMsg: document.getElementById('pdfLoadingMsg'),
    };
  }

  function _withParam(url, param) {
    return url + (url.indexOf('?') === -1 ? '?' : '&') + param;
  }

  function _clearSafetyTimer() {
    if (_safetyTimer) {
      clearTimeout(_safetyTimer);
      _safetyTimer = null;
    }
  }

  function _showLoading(els, msg) {
    if (!els.loading) return;
    if (els.loadingMsg) els.loadingMsg.textContent = msg || DEFAULT_LOADING_MSG;
    els.loading.classList.add('show');
    els.loading.setAttribute('aria-hidden', 'false');
  }

  function _hideLoading(els) {
    if (!els.loading) return;
    els.loading.classList.remove('show');
    els.loading.setAttribute('aria-hidden', 'true');
  }

  function openPdf(url, opts) {
    if (!url) return;
    opts = opts || {};
    const els = _els();

    // Si la página no incluyó el partial (no debería pasar — base.html
    // lo incluye siempre) no rompemos el click: caemos al comportamiento
    // previo a este modal.
    if (!els.modal || !els.frame) {
      global.open(url, '_blank', 'noopener');
      return;
    }

    _clearSafetyTimer();

    const iframeSrc = _withParam(url, '_ts=' + Date.now());
    const openHref = opts.openUrl || iframeSrc;
    const downloadHref = opts.downloadUrl || _withParam(url, 'download=1');
    const title = opts.title || DEFAULT_TITLE;

    if (els.titleEl) els.titleEl.textContent = title;
    els.frame.setAttribute('title', title);
    if (els.openLink) els.openLink.href = openHref;
    if (els.downloadLink) {
      els.downloadLink.href = downloadHref;
      // Fuerza "Guardar como" en el navegador aunque el endpoint responda
      // inline (ej: PDF de OT vía Playwright, sin
      // Content-Disposition:attachment). Siempre same-origin en ILUS, así
      // que el atributo download es seguro y no rompe el nombre de
      // archivo que ya sugiera el servidor.
      els.downloadLink.setAttribute('download', opts.downloadName || '');
    }

    _showLoading(els, opts.loadingMessage);
    els.frame.src = iframeSrc;

    els.modal.classList.add('is-open');
    els.modal.setAttribute('aria-hidden', 'false');

    // Timeout de seguridad: algunos PDF/iframes no disparan "load" de
    // forma confiable. Sin esto, la barra de progreso quedaría pegada
    // para siempre en ese caso.
    _safetyTimer = setTimeout(function () {
      _hideLoading(_els());
      _safetyTimer = null;
    }, opts.safetyTimeout || SAFETY_TIMEOUT_MS);
  }

  function closePdf() {
    const els = _els();
    if (!els.modal) return;
    els.modal.classList.remove('is-open');
    els.modal.setAttribute('aria-hidden', 'true');
    if (els.frame) els.frame.src = 'about:blank';
    _clearSafetyTimer();
    _hideLoading(els);
  }

  function printPdf() {
    const els = _els();
    if (!els.frame) return;
    try {
      els.frame.contentWindow.focus();
      els.frame.contentWindow.print();
    } catch (err) {
      // Bloqueo del navegador (raro, mismo origen siempre en ILUS):
      // fallback a pestaña nueva, donde el visor nativo de PDF trae su
      // propio botón de impresión.
      console.warn('[pdf_modal] No se pudo imprimir el iframe directamente:', err);
      if (els.openLink && els.openLink.href) {
        global.open(els.openLink.href, '_blank', 'noopener');
      }
    }
  }

  document.addEventListener('DOMContentLoaded', function () {
    const els = _els();

    if (els.frame) {
      els.frame.addEventListener('load', function () {
        _clearSafetyTimer();
        _hideLoading(_els());
      });
    }

    document.querySelectorAll('.js-close-pdf').forEach(function (btn) {
      btn.addEventListener('click', closePdf);
    });

    if (els.modal) {
      els.modal.addEventListener('click', function (ev) {
        if (ev.target === els.modal) closePdf();
      });
    }

    document.addEventListener('keydown', function (ev) {
      const m = document.getElementById('pdfModal');
      if (ev.key === 'Escape' && m && m.classList.contains('is-open')) closePdf();
    });

    if (els.printBtn) els.printBtn.addEventListener('click', printPdf);

    // Delegado (compat legacy Etiquetas + cualquier trigger futuro,
    // incluso si se agrega al DOM después de este DOMContentLoaded).
    document.addEventListener('click', function (ev) {
      const trigger = ev.target.closest && ev.target.closest('.js-preview-pdf[data-pdf-url]');
      if (trigger) openPdf(trigger.dataset.pdfUrl);
    });
  });

  global.openPdf = openPdf;
  global.closePdf = closePdf;
  global.printPdf = printPdf;

})(window);

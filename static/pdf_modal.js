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

  // 2026-08-27 (Daniel): "dejar ese logo como un logo estandarizado de
  // espera" -- el anillo rojo ILUS (window.ilusLoader, static/ilus_ui.js)
  // ya es el loader que usa Asignar y Cotizar al buscar en el ERP. Este
  // modal tenía su PROPIA barra negra con logo estático: dos loaders
  // distintos para la misma acción de "esperar". Si ilusLoader está
  // disponible (siempre, se carga en base.html) se usa ese; el markup
  // propio de pdf_modal.html queda como fallback si algún día no cargara.
  function _showLoading(els, msg) {
    if (window.ilusLoader) {
      window.ilusLoader.show({ text: msg || DEFAULT_LOADING_MSG });
      return;
    }
    if (!els.loading) return;
    if (els.loadingMsg) els.loadingMsg.textContent = msg || DEFAULT_LOADING_MSG;
    els.loading.classList.add('show');
    els.loading.setAttribute('aria-hidden', 'false');
  }

  function _hideLoading(els) {
    if (window.ilusLoader) {
      window.ilusLoader.hide();
    }
    if (!els.loading) return;
    els.loading.classList.remove('show');
    els.loading.setAttribute('aria-hidden', 'true');
  }

  // ────────────────────────────────────────────────────────────────────
  // 2026-08-27 (Daniel): "cuando presionemos Descargar, poner una barra
  // de progreso... con un look bien profesional" + reportó que la
  // descarga de una OT grande (60 equipos/637 tareas) directamente daba
  // error. Antes "Descargar" era un <a href download> puro: el navegador
  // navegaba a la URL sin que este JS se enterara de nada -- si el
  // servidor tardaba, fallaba o daba timeout, no había forma de avisar
  // (por eso "no sé si se descargó" era literal: no había ninguna señal).
  // Ahora se intercepta el click, se hace fetch() al mismo endpoint, y:
  //   - mientras espera, anima una barra de progreso ASINTÓTICA (sube
  //     rápido al principio y frena acercándose al 90%, sin llegar nunca
  //     ahí hasta que la respuesta real llegue) -- mismo truco que usa
  //     cualquier barra "de mentira" cuando no hay forma real de saber
  //     el % de un PDF generándose server-side (no hay Content-Length
  //     útil: el tiempo lo domina el render en el servidor, no la
  //     transferencia).
  //   - si la respuesta fallara (timeout, 500, etc.) se avisa con un
  //     error CLARO en vez de que el usuario se quede sin saber qué pasó.
  //   - si funciona, se arma un Blob y se dispara la descarga a mano --
  //     más confiable que el atributo `download` solo (que en algunos
  //     navegadores no fuerza "Guardar como" si la respuesta no trae
  //     Content-Disposition:attachment).
  // ────────────────────────────────────────────────────────────────────
  const PROGRESS_STAGES = [
    { atMs: 0,     texto: 'Generando el documento…' },
    { atMs: 2500,  texto: 'Incrustando fotos y firmas…' },
    { atMs: 9000,  texto: 'Maquetando páginas…' },
    { atMs: 22000, texto: 'Esto puede tardar un poco más (documento grande)…' },
  ];

  function _descargarConProgreso(url, downloadName) {
    if (!window.ilusLoader) {
      // Sin ilusLoader no hay forma de mostrar progreso -- cae al
      // comportamiento simple de siempre (abrir la URL tal cual).
      global.open(url, '_blank', 'noopener');
      return;
    }
    const t0 = Date.now();
    let pct = 6;
    let ultimaEtapa = -1;
    window.ilusLoader.show({ text: PROGRESS_STAGES[0].texto, progress: pct });

    const tick = setInterval(function () {
      const transcurrido = Date.now() - t0;
      // Sube rápido al inicio, frena cerca del 90% -- nunca lo alcanza
      // hasta que la descarga real termine (ver más abajo).
      pct += (90 - pct) * 0.07;
      window.ilusLoader.progress(pct);
      for (let i = PROGRESS_STAGES.length - 1; i >= 0; i--) {
        if (transcurrido >= PROGRESS_STAGES[i].atMs && i !== ultimaEtapa) {
          window.ilusLoader.text(PROGRESS_STAGES[i].texto);
          ultimaEtapa = i;
          break;
        }
      }
    }, 400);

    fetch(url)
      .then(function (r) {
        clearInterval(tick);
        if (!r.ok) {
          return r.text().catch(function () { return ''; }).then(function (body) {
            throw new Error('HTTP ' + r.status + (body ? ': ' + body.slice(0, 200) : ''));
          });
        }
        return r.blob();
      })
      .then(function (blob) {
        window.ilusLoader.progress(100);
        window.ilusLoader.text('¡Listo!');
        const blobUrl = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = blobUrl;
        a.download = downloadName || 'documento.pdf';
        document.body.appendChild(a);
        a.click();
        a.remove();
        setTimeout(function () { URL.revokeObjectURL(blobUrl); }, 4000);
        setTimeout(function () { window.ilusLoader.hide(); }, 500);
      })
      .catch(function (err) {
        clearInterval(tick);
        window.ilusLoader.hide();
        console.error('[pdf_modal] descarga falló:', err);
        if (typeof ilusAlert === 'function') {
          ilusAlert({
            title: 'No se pudo descargar',
            message: 'El documento no se generó a tiempo o el servidor devolvió un error.',
            sub: 'Si el documento es muy grande (muchos equipos/fotos), puede necesitar más tiempo. Intenta de nuevo o avisa a soporte si se repite.',
            type: 'error',
          });
        } else {
          alert('No se pudo descargar el documento.');
        }
      });
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
      // El href/download nativos quedan como fallback (clic derecho →
      // "Guardar enlace como", o si el usuario abre en pestaña nueva).
      // El click normal lo intercepta _descargarConProgreso vía fetch+blob
      // (ver más abajo) para poder mostrar la barra de progreso y avisar
      // si falla -- un <a download> plano no da ninguna señal de ninguna
      // de las dos cosas.
      els.downloadLink.setAttribute('download', opts.downloadName || '');
      els.downloadLink.dataset.downloadName = opts.downloadName || '';
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

    if (els.downloadLink) {
      els.downloadLink.addEventListener('click', function (ev) {
        // Ctrl/Cmd/middle-click: dejar el comportamiento nativo del navegador
        // (abrir en pestaña nueva, etc.) -- solo se intercepta el clic normal.
        if (ev.button !== 0 || ev.ctrlKey || ev.metaKey || ev.shiftKey) return;
        ev.preventDefault();
        _descargarConProgreso(this.href, this.dataset.downloadName);
      });
    }

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

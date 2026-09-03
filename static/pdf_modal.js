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

  // 🔧 FIX 2026-09-02 (OT-2026-00058, Daniel: "no deja descargar bien las
  // OT"): un documento pesado (60 equipos, 127 fotos) tarda 25-33s en
  // generarse. Sin este freno, un usuario impaciente reintentaba el click
  // de "Descargar" mientras la primera descarga todavía estaba en curso --
  // dos generaciones del MISMO PDF pesado corriendo a la vez es justo lo
  // que agotaba la memoria del servidor y hacía caer la instancia (ver
  // _pdf_grande_lock en app.py). Este flag ignora un segundo click mientras
  // el primero sigue en vuelo, en vez de dejar que dispare otra descarga.
  let _descargaEnCurso = false;

  function _descargarConProgreso(url, downloadName) {
    if (_descargaEnCurso) {
      if (typeof ilusToast === 'function') {
        ilusToast('Ya se está generando el documento, espera un momento…', { type: 'info' });
      }
      return;
    }
    if (!window.ilusLoader) {
      // Sin ilusLoader no hay forma de mostrar progreso -- cae al
      // comportamiento simple de siempre (abrir la URL tal cual).
      global.open(url, '_blank', 'noopener');
      return;
    }
    _descargaEnCurso = true;
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
        // 2026-08-27: cuando el servidor no logra generar el PDF a tiempo
        // (documento muy grande) o Chromium no está disponible, algunos
        // endpoints (ej. mant_visita_pdf) responden 200 con el HTML
        // imprimible de respaldo en vez de un error -- a propósito, para
        // que la OT "siempre se pueda ver". Si se descarga tal cual como
        // ".pdf" el archivo queda inservible (es HTML con otra extensión).
        // Se detecta por Content-Type y se trata como fallo de descarga,
        // pero sin dejar al usuario con las manos vacías: se abre igual
        // en pestaña nueva (ahí SÍ puede verlo/imprimirlo con Ctrl+P).
        const ct = (r.headers.get('content-type') || '').toLowerCase();
        if (ct.indexOf('application/pdf') === -1) {
          const err = new Error('El servidor devolvió el documento en HTML, no pudo generar el PDF a tiempo.');
          err.fallbackNoPdf = true;
          throw err;
        }
        return r.blob();
      })
      .then(function (blob) {
        _descargaEnCurso = false;
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
        _descargaEnCurso = false;
        clearInterval(tick);
        window.ilusLoader.hide();
        console.error('[pdf_modal] descarga falló:', err);
        if (err && err.fallbackNoPdf) {
          global.open(url, '_blank', 'noopener');
          if (typeof ilusAlert === 'function') {
            ilusAlert({
              title: 'El PDF no se pudo generar a tiempo',
              message: 'Te abrimos el documento en una pestaña nueva -- desde ahí puedes verlo e imprimirlo (Ctrl/Cmd+P → Guardar como PDF).',
              sub: 'Es el mismo contenido; el generador automático no alcanzó a terminar. Reintenta más tarde si necesitas el PDF directo.',
              type: 'warning',
            });
          }
          return;
        }
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

    /* 🖨️ 2026-09-03 (Daniel: "necesito que la página se autoajuste a las
       medidas que declaramos de la etiqueta 100x150 y 100x50... deja eso
       automático porque estoy perdiendo tiempo con la orientación y las
       hojas").
       Imprimir la PÁGINA HTML deja el tamaño de papel en manos del
       diálogo de Chrome, que recuerda lo último que usó el usuario e
       ignora el `@page size` de la hoja: por eso salía en carta, con la
       etiqueta chiquita al centro y con encabezado y pie del navegador.
       El PDF, en cambio, lleva el tamaño DENTRO del archivo (medido:
       100,2 x 50,1 mm), así que al imprimirlo Chrome usa esa medida sola.
       Con `autoPrint` el visor abre el PDF y lanza la impresión apenas
       carga -- sin tocar orientación ni tamaño de hoja. */
    _autoPrintPend = !!opts.autoPrint;

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

  // Bandera de "imprimir apenas cargue" (ver opts.autoPrint en openPdf).
  let _autoPrintPend = false;

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
        if (_autoPrintPend) {
          _autoPrintPend = false;
          // Un respiro para que el visor nativo termine de montarse: sin
          // esto, print() a veces sale sobre un documento todavía vacío.
          setTimeout(printPdf, 350);
        }
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

/* ══════════════════════════════════════════════════════════════════════
   ilus_lightbox.js — Visor de imágenes global (ILUS)
   ──────────────────────────────────────────────────────────────────────
   Extraído desde static/transporte_manifiesto_detalle.js (único lugar
   donde existía) y generalizado para TODO el proyecto — mismo camino que
   ya recorrió pdf_modal.js con el modal de PDF.

   Daniel, 2026-09-04 (viendo las fotos de la OT 2.0): "quiero que me
   agregues un visor de imágenes para que las fotos de la OT las pueda
   ver de manera rápida... con buena calidad, mover las fotos, si es
   necesario rotarla" — y al preguntarle a cuál modal se refería:
   "cuando abrimos una foto, que se mueve para allá, la puedes voltear,
   la puedes girar". Ese visor ya existía en Transporte (evidencia de
   entrega); acá pasa a ser de todos.

   Uso:
     ilusLightbox('/f/abc123')                       // una sola imagen
     ilusLightbox([url1, url2, url3], 1, 'Evidencia') // galería, abre en la 2a
     ilusLightbox([{url, caption, id}], 0, 'Foto', {  // con "Guardar giro"
       onGuardarGiro: async (item, grados) => nuevaUrl,
     })

   Qué trae: navegación (flechas del teclado o botones), zoom (botones,
   rueda del mouse, doble click), girar 90° a la izquierda o a la derecha,
   restablecer, y Escape para cerrar. El CSS se inyecta solo la primera vez
   que se abre, así que basta con cargar este archivo — no hay que
   acordarse de un <link>.

   🔄 2026-09-25 (Daniel: "las fotos de todas las OT están torcidas;
   quisiera que las endereces y que dejes la opción de poder editar y que
   se puedan guardar si es que están mal"). El giro de este visor era solo
   de pantalla: al cambiar de foto o cerrar, se perdía. Ahora, si quien lo
   abre pasa el 4° parámetro `opts.onGuardarGiro` y la foto trae `id`,
   aparece "Guardar giro": el servidor gira el archivo de verdad (guarda
   una copia nueva, el original NO se toca) y el visor pasa a mostrar la
   URL nueva. Sin `opts` todo funciona exactamente igual que antes — los
   llamadores viejos no cambian.
   ══════════════════════════════════════════════════════════════════════ */

(function (global) {
  'use strict';

  var CSS_ID = 'ilus-lightbox-css';

  function _asegurarCss() {
    if (document.getElementById(CSS_ID)) return;
    var st = document.createElement('style');
    st.id = CSS_ID;
    st.textContent = [
      '.ilus-lightbox-ov{position:fixed;inset:0;z-index:2000;background:rgba(10,10,10,.92);',
      '  display:flex;align-items:center;justify-content:center;padding:2rem}',
      '.ilus-lightbox-close{position:absolute;top:1rem;right:1.2rem;background:none;border:none;',
      '  color:#fff;font-size:2.2rem;line-height:1;cursor:pointer;opacity:.85;z-index:2}',
      '.ilus-lightbox-close:hover{opacity:1}',
      '.ilus-lb-stage{width:100%;height:100%;display:flex;align-items:center;',
      '  justify-content:center;overflow:hidden}',
      '.ilus-lb-img{max-width:min(88vw,900px);max-height:80vh;object-fit:contain;',
      '  border-radius:8px;box-shadow:0 20px 60px rgba(0,0,0,.5);',
      '  transform-origin:center center;transition:transform .15s ease;',
      '  cursor:zoom-in;user-select:none;-webkit-user-drag:none}',
      '.ilus-lb-img.is-drag{cursor:grabbing;transition:none}',
      '.ilus-lb-nav{position:absolute;top:50%;transform:translateY(-50%);',
      '  background:rgba(255,255,255,.12);border:none;color:#fff;width:46px;height:46px;',
      '  border-radius:50%;font-size:1.3rem;cursor:pointer;z-index:2;display:flex;',
      '  align-items:center;justify-content:center;transition:background .15s}',
      '.ilus-lb-nav:hover{background:rgba(255,255,255,.28)}',
      '.ilus-lb-prev{left:.8rem}.ilus-lb-next{right:.8rem}',
      '.ilus-lb-counter{position:absolute;top:1.1rem;left:50%;transform:translateX(-50%);',
      '  color:#fff;font-size:.8rem;font-weight:700;background:rgba(255,255,255,.14);',
      '  padding:.3rem .8rem;border-radius:999px;z-index:2}',
      '.ilus-lb-cap{position:absolute;top:3.1rem;left:50%;transform:translateX(-50%);',
      '  color:#e5e7eb;font-size:.76rem;max-width:min(88vw,900px);text-align:center;',
      '  z-index:2;text-shadow:0 1px 3px rgba(0,0,0,.8)}',
      '.ilus-lb-toolbar{position:absolute;bottom:1.2rem;left:50%;transform:translateX(-50%);',
      '  display:flex;align-items:center;gap:.35rem;background:rgba(255,255,255,.12);',
      '  backdrop-filter:blur(6px);padding:.4rem .55rem;border-radius:999px;z-index:2}',
      '.ilus-lb-tool{background:none;border:none;color:#fff;font-size:1.05rem;width:34px;',
      '  height:34px;border-radius:50%;cursor:pointer;display:flex;align-items:center;',
      '  justify-content:center;transition:background .15s;text-decoration:none}',
      '.ilus-lb-tool:hover{background:rgba(255,255,255,.25);color:#fff}',
      '.ilus-lb-zoom-pct{color:#fff;font-size:.72rem;font-weight:700;min-width:36px;text-align:center}',
      /* 2026-09-25: "Guardar giro" -- solo aparece con un giro pendiente. */
      '.ilus-lb-save{display:none;align-items:center;gap:6px;background:#dc2626;color:#fff;',
      '  border:none;border-radius:999px;min-height:34px;padding:0 14px;font-size:.78rem;',
      '  font-weight:800;cursor:pointer;white-space:nowrap;box-shadow:0 3px 10px rgba(220,38,38,.35)}',
      '.ilus-lb-save.is-on{display:inline-flex}',
      '.ilus-lb-save:hover{background:#b91c1c}',
      '.ilus-lb-save:disabled{opacity:.6;cursor:wait}',
      /* Móvil: botones de 44px (REGLA #3) y la barra puede pasar a 2 filas
         en vez de salirse de la pantalla. */
      '@media (max-width:576px){',
      '  .ilus-lightbox-ov{padding:1rem .5rem}',
      '  .ilus-lb-nav{width:44px;height:44px;font-size:1.1rem}',
      '  .ilus-lb-prev{left:.3rem}.ilus-lb-next{right:.3rem}',
      /* left+right+margin auto: centrada y con TODO el ancho disponible
         (con left:50% solo tenía la mitad y se partía en 3 filas). */
      '  .ilus-lb-toolbar{gap:.1rem;padding:.25rem .35rem;left:8px;right:8px;transform:none;',
      '    width:max-content;max-width:calc(100% - 16px);margin:0 auto;',
      '    flex-wrap:wrap;justify-content:center;border-radius:24px;bottom:.6rem}',
      '  .ilus-lb-tool{width:44px;height:44px;font-size:1rem}',
      '  .ilus-lb-save{min-height:44px;padding:0 16px}}',
    ].join('');
    document.head.appendChild(st);
  }

  function _toast(msg, type) {
    if (typeof global.ilusToast === 'function') global.ilusToast(msg, { type: type || 'info' });
  }

  /* images: string | [string] | [{url, caption, id}] — el objeto permite
     mostrar de quién/cuándo es la foto sin obligar a nadie a usarlo.
     opts (opcional, 2026-09-25):
       onGuardarGiro(item, grados) -> Promise<nuevaUrl>
         grados = giro pendiente a la DERECHA respecto de cómo se ve ahora
         (90 | 180 | -90). Si la promesa falla, su mensaje se muestra en un
         toast y el giro queda pendiente para reintentar. */
  function ilusLightbox(images, startIdx, altBase, opts) {
    if (typeof images === 'string') images = [images];
    images = (images || []).filter(Boolean).map(function (it) {
      return (typeof it === 'string') ? { url: it, caption: '' } : it;
    }).filter(function (it) { return it && it.url; });
    if (!images.length) return;
    opts = opts || {};

    _asegurarCss();

    var idx = Math.max(0, Math.min(startIdx || 0, images.length - 1));
    var zoom = 1, rot = 0, panX = 0, panY = 0;
    var multi = images.length > 1;
    var guardando = false;

    // ¿La foto actual se puede guardar girada? Necesita id + callback.
    function puedeGuardar(it) {
      return !!(it && it.id && typeof opts.onGuardarGiro === 'function');
    }

    var ov = document.createElement('div');
    ov.className = 'ilus-lightbox-ov';
    ov.innerHTML =
      '<button type="button" class="ilus-lightbox-close" aria-label="Cerrar">&times;</button>' +
      (multi ? '<button type="button" class="ilus-lb-nav ilus-lb-prev" aria-label="Anterior"><i class="bi bi-chevron-left"></i></button>' : '') +
      (multi ? '<button type="button" class="ilus-lb-nav ilus-lb-next" aria-label="Siguiente"><i class="bi bi-chevron-right"></i></button>' : '') +
      (multi ? '<div class="ilus-lb-counter"></div>' : '') +
      '<div class="ilus-lb-cap"></div>' +
      '<div class="ilus-lb-stage"><img class="ilus-lb-img" src="" alt=""></div>' +
      '<div class="ilus-lb-toolbar">' +
        '<button type="button" class="ilus-lb-tool" data-act="zoom-out" title="Alejar"><i class="bi bi-zoom-out"></i></button>' +
        '<span class="ilus-lb-zoom-pct">100%</span>' +
        '<button type="button" class="ilus-lb-tool" data-act="zoom-in" title="Acercar"><i class="bi bi-zoom-in"></i></button>' +
        '<button type="button" class="ilus-lb-tool" data-act="rotate-left" title="Girar 90° a la izquierda" aria-label="Girar 90° a la izquierda"><i class="bi bi-arrow-counterclockwise"></i></button>' +
        '<button type="button" class="ilus-lb-tool" data-act="rotate" title="Girar 90° a la derecha" aria-label="Girar 90° a la derecha"><i class="bi bi-arrow-clockwise"></i></button>' +
        '<button type="button" class="ilus-lb-tool" data-act="reset" title="Restablecer"><i class="bi bi-aspect-ratio"></i></button>' +
        '<a class="ilus-lb-tool" data-act="abrir" target="_blank" rel="noopener" title="Abrir original en pestaña nueva"><i class="bi bi-box-arrow-up-right"></i></a>' +
        '<button type="button" class="ilus-lb-save" data-act="guardar" title="Guardar la foto girada"><i class="bi bi-check2-circle"></i> <span>Guardar giro</span></button>' +
      '</div>';

    var img     = ov.querySelector('.ilus-lb-img');
    var counter = ov.querySelector('.ilus-lb-counter');
    var capEl   = ov.querySelector('.ilus-lb-cap');
    var zoomPct = ov.querySelector('.ilus-lb-zoom-pct');
    var abrirEl = ov.querySelector('[data-act="abrir"]');
    var saveBtn = ov.querySelector('[data-act="guardar"]');
    var saveTxt = saveBtn ? saveBtn.querySelector('span') : null;
    var toolsGiro = Array.prototype.slice.call(
      ov.querySelectorAll('[data-act="rotate"],[data-act="rotate-left"],[data-act="reset"]'));

    function applyTransform() {
      img.style.transform =
        'translate(' + panX + 'px,' + panY + 'px) scale(' + zoom + ') rotate(' + rot + 'deg)';
      // Girada de lado (90°/270°), el ancho visible es el ALTO de la imagen:
      // se intercambian los topes para que una foto apaisada no se salga
      // de la pantalla por arriba y por abajo.
      var deLado = (rot === 90 || rot === 270);
      img.style.maxWidth  = deLado ? '80vh' : '';
      img.style.maxHeight = deLado ? 'min(88vw,900px)' : '';
      zoomPct.textContent = Math.round(zoom * 100) + '%';
      if (saveBtn) {
        var on = rot !== 0 && puedeGuardar(images[idx]);
        saveBtn.classList.toggle('is-on', on);
        saveBtn.disabled = guardando;
        if (saveTxt) saveTxt.textContent = guardando ? 'Guardando…' : 'Guardar giro';
      }
      // Mientras se guarda, el giro queda congelado: un giro extra se
      // perdería en silencio al terminar (render() vuelve rot a 0).
      toolsGiro.forEach(function (b) { b.disabled = guardando; });
    }
    function render() {
      var it = images[idx];
      img.src = it.url;
      img.alt = (altBase || 'Imagen') + (multi ? ' ' + (idx + 1) : '');
      if (abrirEl) abrirEl.href = it.url;
      if (capEl) capEl.textContent = it.caption || '';
      zoom = 1; rot = 0; panX = 0; panY = 0;
      applyTransform();
      if (counter) counter.textContent = (idx + 1) + ' / ' + images.length;
    }
    // Un giro que se pudo guardar y no se guardó se pierde al cambiar de
    // foto o cerrar: se avisa para que no parezca que quedó guardado.
    function avisarGiroSinGuardar() {
      if (rot !== 0 && !guardando && puedeGuardar(images[idx])) {
        _toast('El giro de esa foto no se guardó. Para dejarla derecha, gírala y toca "Guardar giro".', 'warning');
      }
    }
    function go(delta) {
      avisarGiroSinGuardar();
      idx = (idx + delta + images.length) % images.length;
      render();
    }
    function cerrar() {
      avisarGiroSinGuardar();
      ov.remove();
      document.removeEventListener('keydown', onKey);
    }
    function guardarGiro() {
      var it = images[idx];
      if (guardando || rot === 0 || !puedeGuardar(it)) return;
      var i0 = idx;
      // 270 a la derecha = 90 a la izquierda: se manda el giro más corto.
      var grados = (rot === 270) ? -90 : rot;
      guardando = true;
      applyTransform();
      Promise.resolve()
        .then(function () { return opts.onGuardarGiro(it, grados); })
        .then(function (res) {
          // El callback devuelve la URL nueva, o {url, mensaje, tipo} cuando
          // hay algo que avisar (p. ej. la foto ya había cambiado).
          var nuevaUrl = (res && typeof res === 'object') ? res.url : res;
          guardando = false;
          if (nuevaUrl) it.url = nuevaUrl;
          if (res && typeof res === 'object' && res.mensaje) _toast(res.mensaje, res.tipo || 'info');
          else _toast('✓ Foto guardada con el giro', 'success');
          // Si sigue a la vista, se recarga ya derecha y sin giro pendiente.
          if (idx === i0 && document.body.contains(ov)) { render(); }
          else { applyTransform(); }
        })
        .catch(function (err) {
          guardando = false;
          applyTransform();
          _toast((err && err.message) || 'No se pudo guardar el giro. Intenta de nuevo.', 'error');
        });
    }
    function onKey(e) {
      if (e.key === 'Escape') cerrar();
      else if (multi && e.key === 'ArrowLeft')  go(-1);
      else if (multi && e.key === 'ArrowRight') go(1);
      else if (e.key === '+') { zoom = Math.min(zoom * 1.25, 5);  applyTransform(); }
      else if (e.key === '-') { zoom = Math.max(zoom / 1.25, .3); applyTransform(); }
      else if (!guardando && (e.key === 'r' || e.key === 'R')) { rot = (rot + 90) % 360; applyTransform(); }
      else if (!guardando && (e.key === 'l' || e.key === 'L')) { rot = (rot + 270) % 360; applyTransform(); }
    }

    ov.addEventListener('click', function (e) {
      if (e.target === ov || e.target.classList.contains('ilus-lightbox-close') ||
          e.target.classList.contains('ilus-lb-stage')) { cerrar(); return; }
      var nav = e.target.closest('.ilus-lb-nav');
      if (nav) { go(nav.classList.contains('ilus-lb-prev') ? -1 : 1); return; }
      if (e.target.closest('.ilus-lb-save')) { guardarGiro(); return; }
      var tool = e.target.closest('.ilus-lb-tool');
      if (tool) {
        var act = tool.dataset.act;
        if (act === 'abrir') return;             // <a>: deja que el navegador lo abra
        if (guardando && (act === 'rotate' || act === 'rotate-left' || act === 'reset')) return;
        if (act === 'zoom-in')       zoom = Math.min(zoom * 1.25, 5);
        else if (act === 'zoom-out') { zoom = Math.max(zoom / 1.25, .3); if (zoom === 1) { panX = 0; panY = 0; } }
        else if (act === 'rotate')   rot  = (rot + 90) % 360;
        else if (act === 'rotate-left') rot = (rot + 270) % 360;
        else if (act === 'reset')    { zoom = 1; rot = 0; panX = 0; panY = 0; }
        applyTransform();
      }
    });

    img.addEventListener('dblclick', function () {
      zoom = (zoom === 1) ? 2 : 1;
      if (zoom === 1) { panX = 0; panY = 0; }
      applyTransform();
    });

    ov.addEventListener('wheel', function (e) {
      e.preventDefault();
      zoom = Math.max(.3, Math.min(5, zoom * (e.deltaY < 0 ? 1.1 : .9)));
      if (zoom === 1) { panX = 0; panY = 0; }
      applyTransform();
    }, { passive: false });

    /* Arrastrar la foto (Daniel: "mover las fotos"). Solo tiene sentido con
       zoom aplicado -- sin zoom la imagen entra completa y moverla sería
       perderla de vista. */
    var drag = null;
    img.addEventListener('pointerdown', function (e) {
      if (zoom <= 1) return;
      drag = { x: e.clientX - panX, y: e.clientY - panY };
      img.classList.add('is-drag');
      try { img.setPointerCapture(e.pointerId); } catch (_) {}
    });
    img.addEventListener('pointermove', function (e) {
      if (!drag) return;
      panX = e.clientX - drag.x;
      panY = e.clientY - drag.y;
      applyTransform();
    });
    ['pointerup', 'pointercancel'].forEach(function (ev) {
      img.addEventListener(ev, function () { drag = null; img.classList.remove('is-drag'); });
    });

    document.addEventListener('keydown', onKey);
    render();
    // Si hay un modal Bootstrap abierto, el visor se cuelga DENTRO para
    // quedar por encima de su backdrop; si no, va al body.
    (document.querySelector('.modal.show') || document.body).appendChild(ov);
  }

  global.ilusLightbox = ilusLightbox;
})(window);

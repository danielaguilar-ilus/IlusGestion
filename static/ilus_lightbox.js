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

   Qué trae: navegación (flechas del teclado o botones), zoom (botones,
   rueda del mouse, doble click), girar 90°, restablecer, y Escape para
   cerrar. El CSS se inyecta solo la primera vez que se abre, así que
   basta con cargar este archivo — no hay que acordarse de un <link>.
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
      '@media (max-width:576px){',
      '  .ilus-lb-nav{width:38px;height:38px;font-size:1.1rem}',
      '  .ilus-lb-toolbar{gap:.15rem;padding:.3rem .4rem}',
      '  .ilus-lb-tool{width:30px;height:30px;font-size:.92rem}}',
    ].join('');
    document.head.appendChild(st);
  }

  /* images: string | [string] | [{url, caption}] — el objeto permite
     mostrar de quién/cuándo es la foto sin obligar a nadie a usarlo. */
  function ilusLightbox(images, startIdx, altBase) {
    if (typeof images === 'string') images = [images];
    images = (images || []).filter(Boolean).map(function (it) {
      return (typeof it === 'string') ? { url: it, caption: '' } : it;
    }).filter(function (it) { return it && it.url; });
    if (!images.length) return;

    _asegurarCss();

    var idx = Math.max(0, Math.min(startIdx || 0, images.length - 1));
    var zoom = 1, rot = 0, panX = 0, panY = 0;
    var multi = images.length > 1;

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
        '<button type="button" class="ilus-lb-tool" data-act="rotate" title="Girar 90°"><i class="bi bi-arrow-clockwise"></i></button>' +
        '<button type="button" class="ilus-lb-tool" data-act="reset" title="Restablecer"><i class="bi bi-aspect-ratio"></i></button>' +
        '<a class="ilus-lb-tool" data-act="abrir" target="_blank" rel="noopener" title="Abrir original en pestaña nueva"><i class="bi bi-box-arrow-up-right"></i></a>' +
      '</div>';

    var img     = ov.querySelector('.ilus-lb-img');
    var counter = ov.querySelector('.ilus-lb-counter');
    var capEl   = ov.querySelector('.ilus-lb-cap');
    var zoomPct = ov.querySelector('.ilus-lb-zoom-pct');
    var abrirEl = ov.querySelector('[data-act="abrir"]');

    function applyTransform() {
      img.style.transform =
        'translate(' + panX + 'px,' + panY + 'px) scale(' + zoom + ') rotate(' + rot + 'deg)';
      zoomPct.textContent = Math.round(zoom * 100) + '%';
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
    function go(delta) { idx = (idx + delta + images.length) % images.length; render(); }
    function cerrar() { ov.remove(); document.removeEventListener('keydown', onKey); }
    function onKey(e) {
      if (e.key === 'Escape') cerrar();
      else if (multi && e.key === 'ArrowLeft')  go(-1);
      else if (multi && e.key === 'ArrowRight') go(1);
      else if (e.key === '+') { zoom = Math.min(zoom * 1.25, 5);  applyTransform(); }
      else if (e.key === '-') { zoom = Math.max(zoom / 1.25, .3); applyTransform(); }
      else if (e.key === 'r' || e.key === 'R') { rot = (rot + 90) % 360; applyTransform(); }
    }

    ov.addEventListener('click', function (e) {
      if (e.target === ov || e.target.classList.contains('ilus-lightbox-close') ||
          e.target.classList.contains('ilus-lb-stage')) { cerrar(); return; }
      var nav = e.target.closest('.ilus-lb-nav');
      if (nav) { go(nav.classList.contains('ilus-lb-prev') ? -1 : 1); return; }
      var tool = e.target.closest('.ilus-lb-tool');
      if (tool) {
        var act = tool.dataset.act;
        if (act === 'abrir') return;             // <a>: deja que el navegador lo abra
        if (act === 'zoom-in')       zoom = Math.min(zoom * 1.25, 5);
        else if (act === 'zoom-out') { zoom = Math.max(zoom / 1.25, .3); if (zoom === 1) { panX = 0; panY = 0; } }
        else if (act === 'rotate')   rot  = (rot + 90) % 360;
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

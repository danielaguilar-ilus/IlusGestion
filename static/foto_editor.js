/* ═══════════════════════════════════════════════════════════════════════
   ILUS Foto Editor — visor + editor profesional de fotos de producto
   ─────────────────────────────────────────────────────────────────────────
   · Subida AJAX sin recargar la página, con compresión EN EL NAVEGADOR
     (máx 1600px JPEG q0.85) → de ~6 MB de cámara a ~300 KB antes de viajar.
   · Visor fullscreen oscuro con zoom (rueda / pinch / doble tap) y arrastre.
   · Editor: rotar 90°, voltear H/V, recortar, brillo/contraste, descargar.
   · Guardar → POST /products/<pid>/photos/<id>/replace (key nueva en GCS).
   · Eliminar con confirmación de SKU (ilusPrompt — REGLA #1, sin prompts
     nativos). Requiere ilus_ui.js cargado (base.html ya lo incluye).
   Uso:  ilusFotos.init({ mount, productId, sku, csrf, canEdit, canDelete,
                          maxPhotos, photos:[{id,url}], urls:{upload,del,replace} })
   ═══════════════════════════════════════════════════════════════════════ */
(function () {
  'use strict';

  /* ───────────────────────── CSS embebido ───────────────────────── */
  var CSS = [
    '.ife-backdrop{position:fixed;inset:0;z-index:20000;background:rgba(4,4,6,.96);',
    ' display:flex;flex-direction:column;animation:ifeIn .18s ease}',
    '@keyframes ifeIn{from{opacity:0}to{opacity:1}}',
    '.ife-head{display:flex;align-items:center;gap:10px;padding:10px 14px;',
    ' padding-top:calc(10px + env(safe-area-inset-top,0px));color:#fff;flex-shrink:0}',
    '.ife-head .ife-title{font-weight:700;font-size:.95rem;letter-spacing:.3px;',
    ' overflow:hidden;text-overflow:ellipsis;white-space:nowrap;flex:1}',
    '.ife-head .ife-title small{color:#999;font-weight:500}',
    '.ife-iconbtn{background:rgba(255,255,255,.08);border:none;color:#fff;',
    ' width:44px;height:44px;border-radius:10px;display:inline-flex;align-items:center;',
    ' justify-content:center;font-size:1.15rem;cursor:pointer;flex-shrink:0;',
    ' transition:background .15s}',
    '.ife-iconbtn:hover{background:rgba(255,255,255,.18)}',
    '.ife-iconbtn:disabled{opacity:.35;cursor:default}',
    '.ife-iconbtn.active{background:#dc2626}',
    '.ife-stage{flex:1;position:relative;overflow:hidden;touch-action:none;',
    ' display:flex;align-items:center;justify-content:center;min-height:0}',
    '.ife-stage canvas{position:absolute;left:0;top:0}',
    '.ife-spin{position:absolute;inset:0;display:flex;align-items:center;',
    ' justify-content:center;color:#dc2626;font-size:2rem}',
    /* crop overlay */
    '.ife-crop{position:absolute;border:2px solid #dc2626;',
    ' box-shadow:0 0 0 9999px rgba(0,0,0,.55);cursor:move;touch-action:none}',
    '.ife-crop .ife-h{position:absolute;width:26px;height:26px;background:#dc2626;',
    ' border:3px solid #fff;border-radius:50%;touch-action:none}',
    '.ife-crop .ife-h.nw{left:-14px;top:-14px;cursor:nwse-resize}',
    '.ife-crop .ife-h.ne{right:-14px;top:-14px;cursor:nesw-resize}',
    '.ife-crop .ife-h.sw{left:-14px;bottom:-14px;cursor:nesw-resize}',
    '.ife-crop .ife-h.se{right:-14px;bottom:-14px;cursor:nwse-resize}',
    /* toolbar */
    '.ife-tools{display:flex;gap:8px;align-items:center;padding:10px 12px;',
    ' padding-bottom:calc(10px + env(safe-area-inset-bottom,0px));',
    ' overflow-x:auto;flex-shrink:0;scrollbar-width:none}',
    '.ife-tools::-webkit-scrollbar{display:none}',
    '.ife-tools .ife-sep{width:1px;height:28px;background:rgba(255,255,255,.15);',
    ' flex-shrink:0;margin:0 2px}',
    '.ife-tools .ife-spacer{flex:1}',
    '.ife-save{background:#dc2626;border:none;color:#fff;font-weight:700;',
    ' min-height:44px;padding:0 20px;border-radius:10px;font-size:.9rem;',
    ' display:inline-flex;align-items:center;gap:8px;cursor:pointer;flex-shrink:0}',
    '.ife-save:disabled{opacity:.35;cursor:default}',
    /* panel ajustes */
    '.ife-adjust{padding:8px 16px;color:#ddd;flex-shrink:0;display:none}',
    '.ife-adjust.open{display:block}',
    '.ife-adjust .row-adj{display:flex;align-items:center;gap:12px;margin:6px 0}',
    '.ife-adjust label{font-size:.78rem;width:78px;flex-shrink:0;color:#bbb}',
    '.ife-adjust input[type=range]{flex:1;accent-color:#dc2626;min-height:28px}',
    '.ife-adjust .val{width:44px;text-align:right;font-size:.78rem;',
    ' font-variant-numeric:tabular-nums;color:#fff}',
    /* barra crop */
    '.ife-cropbar{display:none;gap:10px;justify-content:center;padding:8px;flex-shrink:0}',
    '.ife-cropbar.open{display:flex}',
    '.ife-cropbar button{min-height:44px;border-radius:10px;border:none;',
    ' padding:0 22px;font-weight:700;cursor:pointer;font-size:.88rem}',
    '.ife-cropbar .ok{background:#dc2626;color:#fff}',
    '.ife-cropbar .no{background:rgba(255,255,255,.12);color:#fff}',
    /* cards del grid (complementa .photo-card existente) */
    '.ife-card-actions{position:absolute;right:6px;bottom:6px;display:flex;gap:6px}',
    '.ife-card-actions button{width:34px;height:34px;border-radius:8px;border:none;',
    ' background:rgba(10,10,10,.72);color:#fff;font-size:.9rem;cursor:pointer;',
    ' display:inline-flex;align-items:center;justify-content:center;backdrop-filter:blur(2px)}',
    '.ife-card-actions button:hover{background:#dc2626}',
    '.ife-upcard{position:relative}',
    '.ife-upcard .ife-upov{position:absolute;inset:0;background:rgba(10,10,10,.55);',
    ' display:flex;flex-direction:column;align-items:center;justify-content:center;',
    ' gap:8px;color:#fff;border-radius:6px}',
    '.ife-upov .bar{width:70%;height:6px;background:rgba(255,255,255,.25);',
    ' border-radius:3px;overflow:hidden}',
    '.ife-upov .bar>div{height:100%;width:0;background:#dc2626;transition:width .2s}',
    '.ife-upov .pct{font-size:.75rem;font-weight:700}',
    '@media (max-width:768px){',
    ' .ife-backdrop{height:100dvh}',
    ' .ife-tools{gap:6px;padding:8px 8px calc(8px + env(safe-area-inset-bottom,0px))}',
    ' .ife-iconbtn{width:42px;height:42px}',
    '}'
  ].join('');

  var cssDone = false;
  function injectCss() {
    if (cssDone) return;
    var s = document.createElement('style');
    s.id = 'ife-css';
    s.textContent = CSS;
    document.head.appendChild(s);
    cssDone = true;
  }

  /* ───────────────────────── helpers ───────────────────────── */
  function toast(msg, type) {
    if (window.ilusToast) { window.ilusToast(msg, { type: type || 'info' }); }
  }

  function loadImg(src, tryCross) {
    return new Promise(function (res, rej) {
      var im = new Image();
      if (tryCross && /^https?:/i.test(src)) im.crossOrigin = 'anonymous';
      im.onload = function () { res(im); };
      im.onerror = function () {
        if (im.crossOrigin) {           // reintento sin CORS (queda "tainted")
          var im2 = new Image();
          im2.onload = function () { res(im2); };
          im2.onerror = rej;
          im2.src = src;
        } else { rej(new Error('img load')); }
      };
      im.src = src;
    });
  }

  /* Comprime en el navegador ANTES de subir: lado mayor 1600px, JPEG 0.85.
     Si no conviene (GIF, no-imagen, o queda más pesada) sube el original. */
  function compressForUpload(file) {
    if (!/^image\//.test(file.type) || file.type === 'image/gif') {
      return Promise.resolve({ blob: file, name: file.name });
    }
    var url = URL.createObjectURL(file);
    return loadImg(url).then(function (img) {
      var MAX = 1600;
      var w = img.naturalWidth, h = img.naturalHeight;
      var sc = Math.min(1, MAX / Math.max(w, h));
      var cw = Math.max(1, Math.round(w * sc)), ch = Math.max(1, Math.round(h * sc));
      var cv = document.createElement('canvas');
      cv.width = cw; cv.height = ch;
      var cctx = cv.getContext('2d');
      cctx.fillStyle = '#fff';           // PNG transparente -> fondo blanco
      cctx.fillRect(0, 0, cw, ch);
      cctx.drawImage(img, 0, 0, cw, ch);
      return new Promise(function (res) {
        cv.toBlob(function (blob) {
          URL.revokeObjectURL(url);
          if (!blob || blob.size >= file.size) { res({ blob: file, name: file.name }); }
          else { res({ blob: blob, name: 'foto.jpg' }); }
        }, 'image/jpeg', 0.85);
      });
    }).catch(function () {
      URL.revokeObjectURL(url);
      return { blob: file, name: file.name };
    });
  }

  /* XHR con progreso (fetch no reporta progreso de subida) */
  function xhrUpload(url, formData, onProgress) {
    return new Promise(function (res, rej) {
      var x = new XMLHttpRequest();
      x.open('POST', url, true);
      x.setRequestHeader('X-Requested-With', 'XMLHttpRequest');
      if (x.upload && onProgress) {
        x.upload.onprogress = function (e) {
          if (e.lengthComputable) onProgress(Math.round(e.loaded * 100 / e.total));
        };
      }
      x.onload = function () {
        var j = null;
        try { j = JSON.parse(x.responseText); } catch (e) { /* no-json */ }
        if (x.status >= 200 && x.status < 300 && j && j.ok) res(j);
        else rej((j && j.error) || ('Error HTTP ' + x.status));
      };
      x.onerror = function () { rej('Sin conexión con el servidor.'); };
      x.send(formData);
    });
  }

  function filtersSupported() {
    /* OJO: NO detectar asignando ctx.filter y releyéndolo — en navegadores
       sin soporte la asignación crea una propiedad expando que devuelve el
       mismo string y da falso positivo (Safari guardaba sin filtros). */
    return ('filter' in CanvasRenderingContext2D.prototype);
  }

  /* Aplica brillo/contraste píxel a píxel (fallback Safari sin ctx.filter) */
  function bakeFilterManual(canvas, bright, contrast) {
    var ctx = canvas.getContext('2d');
    var d = ctx.getImageData(0, 0, canvas.width, canvas.height);
    var px = d.data;
    var b = bright / 100;
    var c = contrast / 100;
    var i, v, ch;
    for (i = 0; i < px.length; i += 4) {
      for (ch = 0; ch < 3; ch++) {
        v = px[i + ch] * b;                       // brillo
        v = (v - 128) * c + 128;                  // contraste
        px[i + ch] = v < 0 ? 0 : v > 255 ? 255 : v;
      }
    }
    ctx.putImageData(d, 0, 0);
  }

  /* ═════════════════════════ EDITOR / VISOR ═════════════════════════ */
  function createEditor(opts) {
    injectCss();
    var root = null, stage = null, canvas = null, ctx = null;
    var cropBox = null, adjustPanel = null, cropBar = null;
    var btnSave = null, btnCrop = null, btnAdjust = null, spinner = null, titleEl = null;

    var st = null;      // estado por foto abierta
    var openGen = 0;    // token de generación: invalida cargas/respuestas tardías

    function build() {
      if (root) return;
      root = document.createElement('div');
      root.className = 'ife-backdrop';
      root.style.display = 'none';
      root.innerHTML =
        '<div class="ife-head">' +
        '  <div class="ife-title"></div>' +
        '  <button type="button" class="ife-iconbtn" data-a="download" title="Descargar"><i class="bi bi-download"></i></button>' +
        (opts.canDelete ? '  <button type="button" class="ife-iconbtn" data-a="delete" title="Eliminar foto"><i class="bi bi-trash"></i></button>' : '') +
        '  <button type="button" class="ife-iconbtn" data-a="close" title="Cerrar"><i class="bi bi-x-lg"></i></button>' +
        '</div>' +
        '<div class="ife-stage">' +
        '  <canvas></canvas>' +
        '  <div class="ife-spin" style="display:none"><span class="spinner-border"></span></div>' +
        '</div>' +
        '<div class="ife-adjust">' +
        '  <div class="row-adj"><label>Brillo</label><input type="range" min="50" max="150" value="100" data-adj="bright"><span class="val" data-v="bright">100%</span></div>' +
        '  <div class="row-adj"><label>Contraste</label><input type="range" min="50" max="150" value="100" data-adj="contrast"><span class="val" data-v="contrast">100%</span></div>' +
        '</div>' +
        '<div class="ife-cropbar">' +
        '  <button type="button" class="no" data-a="crop-cancel"><i class="bi bi-x-lg me-1"></i>Cancelar</button>' +
        '  <button type="button" class="ok" data-a="crop-apply"><i class="bi bi-check-lg me-1"></i>Aplicar recorte</button>' +
        '</div>' +
        '<div class="ife-tools">' +
        (opts.canEdit ?
        '  <button type="button" class="ife-iconbtn" data-a="rotl" title="Rotar a la izquierda"><i class="bi bi-arrow-counterclockwise"></i></button>' +
        '  <button type="button" class="ife-iconbtn" data-a="rotr" title="Rotar a la derecha"><i class="bi bi-arrow-clockwise"></i></button>' +
        '  <button type="button" class="ife-iconbtn" data-a="fliph" title="Voltear horizontal"><i class="bi bi-symmetry-vertical"></i></button>' +
        '  <button type="button" class="ife-iconbtn" data-a="flipv" title="Voltear vertical"><i class="bi bi-symmetry-horizontal"></i></button>' +
        '  <button type="button" class="ife-iconbtn" data-a="crop" title="Recortar"><i class="bi bi-crop"></i></button>' +
        '  <button type="button" class="ife-iconbtn" data-a="adjust" title="Brillo y contraste"><i class="bi bi-sliders2"></i></button>' +
        '  <span class="ife-sep"></span>' : '') +
        '  <button type="button" class="ife-iconbtn" data-a="zoomout" title="Alejar"><i class="bi bi-zoom-out"></i></button>' +
        '  <button type="button" class="ife-iconbtn" data-a="zoomin" title="Acercar"><i class="bi bi-zoom-in"></i></button>' +
        (opts.canEdit ?
        '  <button type="button" class="ife-iconbtn" data-a="reset" title="Restablecer original"><i class="bi bi-arrow-repeat"></i></button>' : '') +
        '  <span class="ife-spacer"></span>' +
        (opts.canEdit ?
        '  <button type="button" class="ife-save" data-a="save" disabled><i class="bi bi-floppy2"></i>Guardar</button>' : '') +
        '</div>';
      document.body.appendChild(root);

      stage       = root.querySelector('.ife-stage');
      canvas      = stage.querySelector('canvas');
      ctx         = canvas.getContext('2d');
      spinner     = stage.querySelector('.ife-spin');
      adjustPanel = root.querySelector('.ife-adjust');
      cropBar     = root.querySelector('.ife-cropbar');
      btnSave     = root.querySelector('[data-a="save"]');
      btnCrop     = root.querySelector('[data-a="crop"]');
      btnAdjust   = root.querySelector('[data-a="adjust"]');
      titleEl     = root.querySelector('.ife-title');

      root.addEventListener('click', onAction);
      adjustPanel.querySelectorAll('input[type=range]').forEach(function (r) {
        r.addEventListener('input', onSlider);
      });
      bindStageGestures();
      window.addEventListener('resize', function () { if (isOpen()) draw(); });
      document.addEventListener('keydown', function (e) {
        if (e.key === 'Escape' && isOpen()) requestClose();
      });
    }

    function isOpen() { return root && root.style.display !== 'none'; }

    /* ── apertura ─────────────────────────────────────────────── */
    function open(photo) {
      build();
      var gen = ++openGen;   // invalida cualquier carga anterior en vuelo
      st = {
        photo: photo, img: null, work: null,
        bright: 100, contrast: 100, geomDirty: false, editSeq: 0,
        tainted: false, zoom: 1, panX: 0, panY: 0, cropMode: false, saving: false
      };
      titleEl.innerHTML = '<i class="bi bi-image me-2"></i>' + opts.sku +
                          ' <small>· foto del producto</small>';
      adjustPanel.classList.remove('open');
      cropBar.classList.remove('open');
      if (btnAdjust) btnAdjust.classList.remove('active');
      if (btnSave) btnSave.disabled = true;
      resetSliders();
      root.style.display = 'flex';
      document.body.style.overflow = 'hidden';
      spinner.style.display = 'flex';
      canvas.style.display = 'none';
      loadImg(photo.url, true).then(function (img) {
        if (gen !== openGen || !st || st.photo !== photo) return;  // llegó tarde
        st.img = img;
        st.work = document.createElement('canvas');
        st.work.width = img.naturalWidth; st.work.height = img.naturalHeight;
        st.work.getContext('2d').drawImage(img, 0, 0);
        try { st.work.getContext('2d').getImageData(0, 0, 1, 1); }
        catch (e) { st.tainted = true; }
        spinner.style.display = 'none';
        canvas.style.display = '';
        draw();
        if (st.tainted && opts.canEdit) {
          toast('Esta foto antigua no permite edición (solo vista).', 'warning');
        }
      }).catch(function () {
        if (gen !== openGen || !st) return;   // ya se cerró o abrió otra
        spinner.style.display = 'none';
        toast('No se pudo cargar la imagen.', 'error');
        close();
      });
    }

    function isDirty() {
      return !!st && (st.geomDirty || st.bright !== 100 || st.contrast !== 100);
    }

    function close() {
      /* limpiar overlays para que la próxima apertura arranque limpia */
      if (cropBox) cropBox.style.display = 'none';
      cropBar.classList.remove('open');
      if (btnCrop) btnCrop.classList.remove('active');
      adjustPanel.classList.remove('open');
      if (btnAdjust) btnAdjust.classList.remove('active');
      root.style.display = 'none';
      document.body.style.overflow = '';
      st = null;
    }

    function requestClose() {
      if (st && isDirty() && opts.canEdit && !st.tainted) {
        var ask = window.ilusConfirm
          ? window.ilusConfirm({
              title: 'Descartar cambios',
              message: 'La foto tiene cambios sin guardar.',
              sub: 'Si sales ahora, se perderán.',
              okLabel: 'Salir sin guardar', cancelLabel: 'Seguir editando',
              danger: true, type: 'warning'
            })
          : Promise.resolve(true);
        ask.then(function (ok) { if (ok) close(); });
      } else { close(); }
    }

    /* ── dibujo ───────────────────────────────────────────────── */
    function fitScale() {
      var r = stage.getBoundingClientRect();
      return Math.min(r.width / st.work.width, r.height / st.work.height) * 0.96;
    }

    function draw() {
      if (!st || !st.work) return;
      var r = stage.getBoundingClientRect();
      var dpr = window.devicePixelRatio || 1;
      canvas.width = Math.round(r.width * dpr);
      canvas.height = Math.round(r.height * dpr);
      canvas.style.width = r.width + 'px';
      canvas.style.height = r.height + 'px';
      var sc = fitScale() * st.zoom;
      var dw = st.work.width * sc, dh = st.work.height * sc;
      var dx = (r.width - dw) / 2 + st.panX;
      var dy = (r.height - dh) / 2 + st.panY;
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      ctx.clearRect(0, 0, r.width, r.height);
      ctx.imageSmoothingQuality = 'high';
      ctx.drawImage(st.work, dx, dy, dw, dh);
      canvas.style.filter = (st.bright !== 100 || st.contrast !== 100)
        ? 'brightness(' + st.bright + '%) contrast(' + st.contrast + '%)' : '';
      st._geom = { dx: dx, dy: dy, dw: dw, dh: dh, sc: sc };
      if (st.cropMode) positionCropBox();
    }

    function updateSaveBtn() {
      if (btnSave) btnSave.disabled = !st || st.tainted || st.saving || !isDirty();
    }

    /* Operación geométrica (rotar/voltear/recortar): siempre ensucia */
    function markGeomDirty() {
      st.geomDirty = true;
      st.editSeq++;
      updateSaveBtn();
    }

    /* ── operaciones ──────────────────────────────────────────── */
    function rotate(dir) {
      var w = st.work.width, h = st.work.height;
      var nc = document.createElement('canvas');
      nc.width = h; nc.height = w;
      var c = nc.getContext('2d');
      c.translate(h / 2, w / 2);
      c.rotate(dir * Math.PI / 2);
      c.drawImage(st.work, -w / 2, -h / 2);
      st.work = nc;
      resetView(); markGeomDirty(); draw();
    }

    function flip(horizontal) {
      var w = st.work.width, h = st.work.height;
      var nc = document.createElement('canvas');
      nc.width = w; nc.height = h;
      var c = nc.getContext('2d');
      if (horizontal) { c.translate(w, 0); c.scale(-1, 1); }
      else            { c.translate(0, h); c.scale(1, -1); }
      c.drawImage(st.work, 0, 0);
      st.work = nc;
      markGeomDirty(); draw();
    }

    function resetAll() {
      var img = st.img;
      st.work = document.createElement('canvas');
      st.work.width = img.naturalWidth; st.work.height = img.naturalHeight;
      st.work.getContext('2d').drawImage(img, 0, 0);
      st.bright = 100; st.contrast = 100;
      st.geomDirty = false;
      st.editSeq++;
      updateSaveBtn();
      resetSliders(); exitCrop(); resetView(); draw();
      toast('Foto restablecida al original.', 'info');
    }

    function resetView() { st.zoom = 1; st.panX = 0; st.panY = 0; }

    function resetSliders() {
      if (!adjustPanel) return;
      adjustPanel.querySelectorAll('input[type=range]').forEach(function (r) {
        r.value = 100;
      });
      adjustPanel.querySelectorAll('.val').forEach(function (v) { v.textContent = '100%'; });
    }

    function onSlider(e) {
      var k = e.target.getAttribute('data-adj');
      st[k] = parseInt(e.target.value, 10);
      adjustPanel.querySelector('[data-v="' + k + '"]').textContent = e.target.value + '%';
      st.editSeq++;
      updateSaveBtn();
      draw();
    }

    /* ── crop ─────────────────────────────────────────────────── */
    function enterCrop() {
      if (st.tainted) return;
      exitAdjust();
      st.cropMode = true;
      resetView(); draw();
      var g = st._geom;
      st.crop = { x: g.dx + g.dw * 0.1, y: g.dy + g.dh * 0.1, w: g.dw * 0.8, h: g.dh * 0.8 };
      if (!cropBox) {
        cropBox = document.createElement('div');
        cropBox.className = 'ife-crop';
        cropBox.innerHTML = '<div class="ife-h nw"></div><div class="ife-h ne"></div>' +
                            '<div class="ife-h sw"></div><div class="ife-h se"></div>';
        stage.appendChild(cropBox);
        bindCropGestures();
      }
      cropBox.style.display = 'block';
      positionCropBox();
      cropBar.classList.add('open');
      btnCrop.classList.add('active');
    }

    function exitCrop() {
      st.cropMode = false;
      if (cropBox) cropBox.style.display = 'none';
      cropBar.classList.remove('open');
      if (btnCrop) btnCrop.classList.remove('active');
    }

    function positionCropBox() {
      if (!cropBox || !st.crop) return;
      var g = st._geom;
      // mantener dentro de la imagen visible
      st.crop.x = Math.max(g.dx, Math.min(st.crop.x, g.dx + g.dw - st.crop.w));
      st.crop.y = Math.max(g.dy, Math.min(st.crop.y, g.dy + g.dh - st.crop.h));
      cropBox.style.left = st.crop.x + 'px';
      cropBox.style.top = st.crop.y + 'px';
      cropBox.style.width = st.crop.w + 'px';
      cropBox.style.height = st.crop.h + 'px';
    }

    function applyCrop() {
      var g = st._geom, c = st.crop;
      var sx = (c.x - g.dx) / g.sc, sy = (c.y - g.dy) / g.sc;
      var sw = c.w / g.sc, sh = c.h / g.sc;
      sx = Math.max(0, sx); sy = Math.max(0, sy);
      sw = Math.min(sw, st.work.width - sx); sh = Math.min(sh, st.work.height - sy);
      if (sw < 20 || sh < 20) { toast('El recorte es demasiado pequeño.', 'warning'); return; }
      var nc = document.createElement('canvas');
      nc.width = Math.round(sw); nc.height = Math.round(sh);
      nc.getContext('2d').drawImage(st.work, sx, sy, sw, sh, 0, 0, nc.width, nc.height);
      st.work = nc;
      exitCrop(); markGeomDirty(); resetView(); draw();
    }

    function bindCropGestures() {
      var mode = null, startX = 0, startY = 0, orig = null, dragId = null;
      function down(e) {
        if (!st || !st.cropMode) return;
        if (mode) return;                 // ya hay un dedo arrastrando
        e.preventDefault(); e.stopPropagation();
        dragId = e.pointerId;
        var t = e.target;
        mode = t.classList.contains('ife-h')
          ? (t.classList.contains('nw') ? 'nw' : t.classList.contains('ne') ? 'ne'
             : t.classList.contains('sw') ? 'sw' : 'se')
          : 'move';
        startX = e.clientX; startY = e.clientY;
        orig = { x: st.crop.x, y: st.crop.y, w: st.crop.w, h: st.crop.h };
        cropBox.setPointerCapture(e.pointerId);
      }
      function move(e) {
        if (!mode || !st || !st.cropMode) return;
        if (e.pointerId !== dragId) return;   // ignorar segundo dedo
        var dx = e.clientX - startX, dy = e.clientY - startY;
        var g = st._geom, MIN = 40;
        var c = st.crop;
        if (mode === 'move') {
          c.x = orig.x + dx; c.y = orig.y + dy;
        } else {
          if (mode.indexOf('w') >= 0) { c.x = orig.x + dx; c.w = orig.w - dx; }
          if (mode.indexOf('e') >= 0) { c.w = orig.w + dx; }
          if (mode.indexOf('n') >= 0) { c.y = orig.y + dy; c.h = orig.h - dy; }
          if (mode.indexOf('s') >= 0) { c.h = orig.h + dy; }
          if (c.w < MIN) { if (mode.indexOf('w') >= 0) c.x = orig.x + orig.w - MIN; c.w = MIN; }
          if (c.h < MIN) { if (mode.indexOf('n') >= 0) c.y = orig.y + orig.h - MIN; c.h = MIN; }
          c.w = Math.min(c.w, g.dx + g.dw - c.x);
          c.h = Math.min(c.h, g.dy + g.dh - c.y);
        }
        positionCropBox();
      }
      function up(e) { if (e.pointerId === dragId) { mode = null; dragId = null; } }
      cropBox.addEventListener('pointerdown', down);
      cropBox.addEventListener('pointermove', move);
      cropBox.addEventListener('pointerup', up);
      cropBox.addEventListener('pointercancel', up);
    }

    /* ── zoom / pan / pinch ───────────────────────────────────── */
    function bindStageGestures() {
      var pointers = {}, lastDist = 0, panStart = null;
      stage.addEventListener('wheel', function (e) {
        if (!st || st.cropMode) return;
        e.preventDefault();
        setZoom(st.zoom * (e.deltaY < 0 ? 1.15 : 0.87));
      }, { passive: false });
      stage.addEventListener('pointerdown', function (e) {
        if (!st || st.cropMode) return;
        pointers[e.pointerId] = e;
        if (Object.keys(pointers).length === 1 && st.zoom > 1) {
          panStart = { x: e.clientX - st.panX, y: e.clientY - st.panY };
        }
      });
      stage.addEventListener('pointermove', function (e) {
        if (!st || st.cropMode) return;
        if (pointers[e.pointerId]) pointers[e.pointerId] = e;
        var ids = Object.keys(pointers);
        if (ids.length === 2) {
          var a = pointers[ids[0]], b = pointers[ids[1]];
          var d = Math.hypot(a.clientX - b.clientX, a.clientY - b.clientY);
          if (lastDist) setZoom(st.zoom * (d / lastDist));
          lastDist = d;
        } else if (ids.length === 1 && panStart && st.zoom > 1) {
          if (e.pointerType === 'mouse' && e.buttons === 0) {
            /* solto el boton fuera del stage: cortar el pan fantasma */
            delete pointers[e.pointerId]; panStart = null; return;
          }
          st.panX = e.clientX - panStart.x;
          st.panY = e.clientY - panStart.y;
          draw();
        }
      });
      function lift(e) {
        delete pointers[e.pointerId];
        if (Object.keys(pointers).length < 2) lastDist = 0;
        if (!Object.keys(pointers).length) panStart = null;
      }
      stage.addEventListener('pointerup', lift);
      stage.addEventListener('pointercancel', lift);
      stage.addEventListener('dblclick', function () {
        if (!st || st.cropMode) return;
        setZoom(st.zoom > 1 ? 1 : 2);
      });
    }

    function setZoom(z) {
      st.zoom = Math.max(1, Math.min(5, z));
      if (st.zoom === 1) { st.panX = 0; st.panY = 0; }
      draw();
    }

    /* ── panel ajustes ────────────────────────────────────────── */
    function toggleAdjust() {
      if (st.tainted) return;
      exitCrop();
      var open = adjustPanel.classList.toggle('open');
      btnAdjust.classList.toggle('active', open);
    }
    function exitAdjust() {
      adjustPanel.classList.remove('open');
      if (btnAdjust) btnAdjust.classList.remove('active');
    }

    /* ── exportar (aplica filtros a los píxeles) ─────────────── */
    function bake() {
      var out = document.createElement('canvas');
      out.width = st.work.width; out.height = st.work.height;
      var c = out.getContext('2d');
      c.fillStyle = '#fff';                       // JPEG no tiene alfa: fondo
      c.fillRect(0, 0, out.width, out.height);    // blanco, no negro
      if (st.bright !== 100 || st.contrast !== 100) {
        if (filtersSupported()) {
          c.filter = 'brightness(' + st.bright + '%) contrast(' + st.contrast + '%)';
          c.drawImage(st.work, 0, 0);
        } else {
          c.drawImage(st.work, 0, 0);
          bakeFilterManual(out, st.bright, st.contrast);
        }
      } else {
        c.drawImage(st.work, 0, 0);
      }
      return out;
    }

    function download() {
      if (st.tainted) { toast('Esta foto antigua no permite descarga directa. Ábrela en otra pestaña.', 'warning'); return; }
      bake().toBlob(function (blob) {
        if (!blob) { toast('No se pudo generar la imagen.', 'error'); return; }
        var a = document.createElement('a');
        a.href = URL.createObjectURL(blob);
        a.download = opts.sku + '_foto.jpg';
        document.body.appendChild(a); a.click(); a.remove();
        setTimeout(function () { URL.revokeObjectURL(a.href); }, 5000);
      }, 'image/jpeg', 0.92);
    }

    function save() {
      if (!st || !isDirty() || st.saving || st.tainted) return;
      /* Capturar el estado LOCAL: si el usuario cierra el editor o abre otra
         foto mientras el XHR vuela, la respuesta no debe tocar el estado nuevo */
      var mySt = st;
      var seqAtSave = mySt.editSeq;
      mySt.saving = true;
      btnSave.disabled = true;
      btnSave.innerHTML = '<span class="spinner-border spinner-border-sm"></span>Guardando…';
      var baked = bake();
      baked.toBlob(function (blob) {
        if (!blob) { saveDone(mySt, false, 'No se pudo generar la imagen.'); return; }
        var fd = new FormData();
        fd.append('photo', new File([blob], 'foto.jpg', { type: 'image/jpeg' }));
        fd.append('csrf_token', opts.csrf);
        var url = opts.urls.replace.replace('{id}', mySt.photo.id) + '?ajax=1';
        xhrUpload(url, fd).then(function (j) {
          mySt.photo.url = j.url;
          if (opts.onReplaced) opts.onReplaced(mySt.photo.id, j.url);  // grid SIEMPRE
          if (mySt.editSeq === seqAtSave) {
            /* nada cambió durante el guardado → re-basar: lo guardado (con
               filtros ya horneados) pasa a ser el nuevo original visible */
            mySt.work = baked;
            mySt.geomDirty = false;
            mySt.bright = 100; mySt.contrast = 100;
            if (st === mySt) { resetSliders(); draw(); }
          }
          /* si editSeq avanzó, los cambios nuevos siguen pendientes (dirty) */
          saveDone(mySt, true);
        }).catch(function (err) { saveDone(mySt, false, err); });
      }, 'image/jpeg', 0.9);
    }

    function saveDone(mySt, ok, err) {
      mySt.saving = false;
      if (st === mySt) {   // solo tocar la UI si el editor sigue en esta foto
        btnSave.innerHTML = '<i class="bi bi-floppy2"></i>Guardar';
        updateSaveBtn();
      }
      if (ok) toast('✓ Foto guardada con los cambios.', 'success');
      else toast(String(err || 'Error al guardar.'), 'error');
    }

    function removePhoto() {
      if (!st) return;
      var mySt = st;   // sobrevive a cierres/cambios de foto durante el XHR
      var doDelete = function (confirmSku) {
        var fd = new FormData();
        fd.append('confirm_sku', confirmSku);
        fd.append('csrf_token', opts.csrf);
        var url = opts.urls.del.replace('{id}', mySt.photo.id) + '?ajax=1';
        xhrUpload(url, fd).then(function () {
          toast('Foto eliminada.', 'success');
          if (st === mySt) close();
          if (opts.onDeleted) opts.onDeleted(mySt.photo.id);
        }).catch(function (err) { toast(String(err), 'error'); });
      };
      if (window.ilusPrompt) {
        window.ilusPrompt({
          title: 'Eliminar foto',
          message: 'Esta acción es permanente. Escribe el SKU para confirmar:',
          sub: 'SKU del producto: ' + opts.sku,
          placeholder: opts.sku,
          required: true,
          type: 'danger'
        }).then(function (val) {
          if (val === null || val === undefined) return;
          if (String(val).trim().toUpperCase() !== opts.sku) {
            toast('El SKU no coincide. La foto NO fue eliminada.', 'warning');
            return;
          }
          doDelete(String(val).trim().toUpperCase());
        });
      }
    }

    function onAction(e) {
      var b = e.target.closest('[data-a]');
      if (!b || !st) return;
      var a = b.getAttribute('data-a');
      /* mientras la imagen carga (st.work=null) solo permitir cerrar/eliminar */
      if (!st.work && a !== 'close' && a !== 'delete') return;
      switch (a) {
        case 'close':       requestClose(); break;
        case 'download':    download(); break;
        case 'delete':      removePhoto(); break;
        case 'rotl':        if (!st.tainted) { exitCrop(); rotate(-1); } break;
        case 'rotr':        if (!st.tainted) { exitCrop(); rotate(1); } break;
        case 'fliph':       if (!st.tainted) { exitCrop(); flip(true); } break;
        case 'flipv':       if (!st.tainted) { exitCrop(); flip(false); } break;
        case 'crop':        st.cropMode ? exitCrop() : enterCrop(); break;
        case 'crop-apply':  applyCrop(); break;
        case 'crop-cancel': exitCrop(); break;
        case 'adjust':      toggleAdjust(); break;
        case 'zoomin':      if (!st.cropMode) setZoom(st.zoom * 1.25); break;
        case 'zoomout':     if (!st.cropMode) setZoom(st.zoom * 0.8); break;
        case 'reset':       if (!st.tainted) resetAll(); break;
        case 'save':        save(); break;
      }
    }

    return { open: open };
  }

  /* ═════════════════════════ GRID + UPLOAD ═════════════════════════ */
  function init(cfg) {
    injectCss();
    var zone = document.querySelector(cfg.mount);
    if (!zone) return;
    var photos = (cfg.photos || []).slice();
    var editor = createEditor({
      sku: cfg.sku,
      csrf: cfg.csrf,
      canEdit: !!cfg.canEdit,
      canDelete: !!cfg.canDelete,
      urls: cfg.urls,
      onReplaced: function (id, url) {
        var p = photos.find(function (x) { return x.id === id; });
        if (p) p.url = url;
        var img = zone.querySelector('[data-photo-id="' + id + '"] img');
        if (img) img.src = url;
      },
      onDeleted: function (id) {
        photos = photos.filter(function (x) { return x.id !== id; });
        render();
      }
    });

    function updateBadge() {
      if (!cfg.badge) return;
      var b = document.querySelector(cfg.badge);
      if (b) b.textContent = photos.length + '/' + cfg.maxPhotos;
    }

    function render() {
      /* no destruir tarjetas de subidas EN VUELO (subidas concurrentes) */
      var pending = Array.prototype.slice.call(zone.querySelectorAll('.ife-upcard'));
      zone.innerHTML = '';
      photos.forEach(function (p) { zone.appendChild(card(p)); });
      pending.forEach(function (c) { zone.appendChild(c); });
      if (cfg.canEdit && photos.length + pending.length < cfg.maxPhotos) {
        zone.appendChild(uploadZoneDesktop());
        zone.appendChild(uploadZoneMobile());
      } else if (cfg.canEdit && photos.length >= cfg.maxPhotos) {
        var full = document.createElement('div');
        full.className = 'd-flex flex-column align-items-center justify-content-center';
        full.style.cssText = 'min-height:120px;border:2px dashed #ddd;border-radius:6px;background:#fafafa';
        full.innerHTML = '<i class="bi bi-check-circle text-success fs-4 mb-1"></i>' +
                         '<div class="small text-muted">Límite de ' + cfg.maxPhotos + ' fotos alcanzado</div>';
        zone.appendChild(full);
      }
      updateBadge();
    }

    function card(p) {
      var d = document.createElement('div');
      d.className = 'photo-card';
      d.setAttribute('data-photo-id', p.id);
      d.innerHTML =
        '<button type="button" class="photo-preview-btn">' +
        '  <img src="' + p.url + '" alt="Foto del producto" loading="lazy" decoding="async">' +
        '</button>' +
        '<div class="ife-card-actions">' +
        '  <button type="button" data-open="1" title="Ver y editar"><i class="bi bi-arrows-fullscreen"></i></button>' +
        '</div>';
      d.querySelector('.photo-preview-btn').addEventListener('click', function () { editor.open(p); });
      d.querySelector('[data-open]').addEventListener('click', function () { editor.open(p); });
      return d;
    }

    /* zona desktop: click + drag&drop */
    function uploadZoneDesktop() {
      var f = document.createElement('div');
      f.className = 'd-none d-md-block';
      f.innerHTML =
        '<label class="photo-upload-zone d-flex flex-column align-items-center justify-content-center h-100" style="min-height:120px;cursor:pointer">' +
        '  <div class="up-icon"><i class="bi bi-camera-plus"></i></div>' +
        '  <div class="up-text">Agregar foto<br><span style="color:#bbb;font-size:.7rem">Clic o arrastra · JPG, PNG, WEBP</span></div>' +
        '  <input type="file" accept="image/*" style="display:none">' +
        '</label>';
      var input = f.querySelector('input');
      var label = f.querySelector('label');
      input.addEventListener('change', function () {
        if (input.files && input.files[0]) uploadFile(input.files[0]);
        input.value = '';
      });
      ['dragover', 'dragenter'].forEach(function (ev) {
        label.addEventListener(ev, function (e) {
          e.preventDefault();
          label.style.borderColor = '#dc2626'; label.style.background = '#fff5f5';
        });
      });
      ['dragleave', 'drop'].forEach(function (ev) {
        label.addEventListener(ev, function (e) {
          e.preventDefault();
          label.style.borderColor = ''; label.style.background = '';
        });
      });
      label.addEventListener('drop', function (e) {
        var file = e.dataTransfer && e.dataTransfer.files && e.dataTransfer.files[0];
        if (file) uploadFile(file);
      });
      return f;
    }

    /* zona mobile: Tomar foto (cámara) / Desde galería */
    function uploadZoneMobile() {
      var d = document.createElement('div');
      d.className = 'd-flex d-md-none flex-column gap-2 p-2';
      d.style.minWidth = '140px';
      d.innerHTML =
        '<label class="btn btn-dark-ilus w-100 d-flex align-items-center justify-content-center gap-2 py-3" style="cursor:pointer;border-radius:8px;font-size:.9rem">' +
        '  <i class="bi bi-camera-fill fs-5"></i><span>Tomar foto</span>' +
        '  <input type="file" accept="image/*" capture="environment" style="display:none">' +
        '</label>' +
        '<label class="btn btn-outline-secondary w-100 d-flex align-items-center justify-content-center gap-2 py-3" style="cursor:pointer;border-radius:8px;font-size:.9rem">' +
        '  <i class="bi bi-images fs-5"></i><span>Desde galería</span>' +
        '  <input type="file" accept="image/*" style="display:none">' +
        '</label>';
      d.querySelectorAll('input').forEach(function (input) {
        input.addEventListener('change', function () {
          if (input.files && input.files[0]) uploadFile(input.files[0]);
          input.value = '';
        });
      });
      return d;
    }

    function uploadFile(file) {
      if (photos.length >= cfg.maxPhotos) {
        toast('Máximo ' + cfg.maxPhotos + ' fotos por producto.', 'warning');
        return;
      }
      if (!/^image\//.test(file.type)) {
        toast('El archivo no es una imagen.', 'warning');
        return;
      }
      /* tarjeta optimista con preview local inmediato */
      var previewUrl = URL.createObjectURL(file);
      var up = document.createElement('div');
      up.className = 'photo-card ife-upcard';
      up.innerHTML =
        '<button type="button" class="photo-preview-btn"><img src="' + previewUrl + '" alt="Subiendo…"></button>' +
        '<div class="ife-upov">' +
        '  <div class="pct">Optimizando…</div>' +
        '  <div class="bar"><div></div></div>' +
        '</div>';
      var firstZone = zone.querySelector('.d-none.d-md-block, .d-flex.d-md-none');
      zone.insertBefore(up, firstZone || null);
      var bar = up.querySelector('.bar>div');
      var pct = up.querySelector('.pct');

      compressForUpload(file).then(function (r) {
        pct.textContent = 'Subiendo… 0%';
        var fd = new FormData();
        fd.append('photo', new File([r.blob], r.name, { type: r.blob.type || 'image/jpeg' }));
        fd.append('csrf_token', cfg.csrf);
        return xhrUpload(cfg.urls.upload + '?ajax=1', fd, function (p) {
          bar.style.width = p + '%';
          pct.textContent = 'Subiendo… ' + p + '%';
        });
      }).then(function (j) {
        URL.revokeObjectURL(previewUrl);
        photos.push({ id: j.id, url: j.url });
        render();
        toast('✓ Foto agregada.', 'success');
      }).catch(function (err) {
        URL.revokeObjectURL(previewUrl);
        up.remove();
        toast(String(err || 'Error al subir la foto.'), 'error');
      });
    }

    render();
  }

  /* ═══════════════ VISOR STANDALONE (modal rápido del catálogo) ═══════════
     Abre el visor/editor sobre una foto sin necesidad del grid.
     cfg: { key, sku, csrf, canEdit, canDelete, urls:{del,replace},
            photo:{id,url}, onReplaced(id,url), onDeleted(id) }        */
  var _viewers = {};
  function view(cfg) {
    injectCss();
    var key = cfg.key || cfg.sku;
    var v = _viewers[key];
    if (!v) {
      v = _viewers[key] = { cbs: {}, ed: null };
      v.ed = createEditor({
        sku: cfg.sku,
        csrf: cfg.csrf,
        canEdit: !!cfg.canEdit,
        canDelete: !!cfg.canDelete,
        urls: cfg.urls,
        onReplaced: function (id, url) { if (v.cbs.onReplaced) v.cbs.onReplaced(id, url); },
        onDeleted:  function (id)      { if (v.cbs.onDeleted)  v.cbs.onDeleted(id); }
      });
    }
    v.cbs.onReplaced = cfg.onReplaced;
    v.cbs.onDeleted  = cfg.onDeleted;
    v.ed.open(cfg.photo);
  }

  window.ilusFotos = { init: init, view: view };
})();

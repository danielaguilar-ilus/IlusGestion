/* Flota · subida de fotos y archivos con barra de progreso (05-nov-2026, Daniel: «tarda mucho subiendo las fotos, colócale una barra de proceso»).
   1) comprimir(archivo): reduce la foto EN EL CELULAR antes de subirla (lado mayor 1600 px, JPG al 82 %): cuatro fotos de 4 a 8 MB pasan a ~0,4 MB
      cada una. El servidor igual las reduce a 1600 px, así que no se pierde nada de lo que se guarda. Un PDF o una imagen ya chica no se tocan.
   2) enviar({url, method, form, onProgreso, onSubido}): XMLHttpRequest con el avance de la subida (fetch no lo da) y el token CSRF.
   3) barra(contenedor): la barra («Preparando… / Subiendo… / Guardando…») con su porcentaje, legible en el celular. */
(function () {
  'use strict';
  var MAX_LADO = 1600, CALIDAD = 0.82, MIN_BYTES = 400 * 1024;

  function esImagen(f) { return /^image\/(jpeg|png|webp)$/.test(f.type || '') || /\.(jpe?g|png|webp)$/i.test(f.name || ''); }

  function comprimir(archivo) {
    return new Promise(function (resolver) {
      try {
        var cv0 = document.createElement('canvas');
        if (!archivo || !esImagen(archivo) || archivo.size <= MIN_BYTES || !window.URL || !cv0.toBlob) { resolver(archivo); return; }
        var url = URL.createObjectURL(archivo), img = new Image();
        var listo = function (r) { try { URL.revokeObjectURL(url); } catch (e) { /* nada */ } resolver(r); };
        img.onload = function () {
          try {
            var w = img.naturalWidth, h = img.naturalHeight, k = Math.min(1, MAX_LADO / Math.max(w, h));
            var cw = Math.max(1, Math.round(w * k)), ch = Math.max(1, Math.round(h * k));
            var cv = document.createElement('canvas');
            cv.width = cw; cv.height = ch;
            var cx = cv.getContext('2d');
            cx.fillStyle = '#fff'; cx.fillRect(0, 0, cw, ch);      // un PNG con transparencia no queda negro
            cx.drawImage(img, 0, 0, cw, ch);                       // el navegador la endereza según la orientación de la cámara
            cv.toBlob(function (blob) {
              if (!blob || blob.size >= archivo.size) { listo(archivo); return; }
              var nombre = String(archivo.name || 'foto').replace(/\.[^.]+$/, '') + '.jpg';
              listo(typeof File === 'function' ? new File([blob], nombre, { type: 'image/jpeg', lastModified: Date.now() }) : blob);
            }, 'image/jpeg', CALIDAD);
          } catch (e) { listo(archivo); }
        };
        img.onerror = function () { listo(archivo); };
        img.src = url;
      } catch (e) { resolver(archivo); }
    });
  }

  function enviar(o) {
    return new Promise(function (resolver) {
      var x = new XMLHttpRequest();
      var fin = function (d) { resolver(d); };
      try {
        x.open(o.method || 'POST', o.url, true);
        x.withCredentials = true;
        x.timeout = o.timeoutMs || 180000;
        x.setRequestHeader('Accept', 'application/json');
        x.setRequestHeader('X-Requested-With', 'XMLHttpRequest');
        if (window.__CSRF_TOKEN) x.setRequestHeader('X-CSRF-Token', window.__CSRF_TOKEN);
        if (x.upload) {
          x.upload.onprogress = function (e) { if (e.lengthComputable && o.onProgreso) o.onProgreso(e.loaded / e.total, e.loaded, e.total); };
          x.upload.onload = function () { if (o.onSubido) o.onSubido(); };
        }
        x.onload = function () {
          var d;
          try { d = JSON.parse(x.responseText); } catch (e) { d = { ok: false, error: 'El servidor respondió algo inesperado.' }; }
          d.status = x.status;
          fin(d);
        };
        x.onerror = function () { fin({ ok: false, status: 0, error: 'No hay conexión con el servidor. Revisa tu internet e intenta de nuevo.' }); };
        x.ontimeout = function () { fin({ ok: false, status: 0, error: 'La subida tardó demasiado. Revisa tu internet e intenta de nuevo.' }); };
        x.onabort = function () { fin({ ok: false, status: 0, error: 'Se canceló la subida.' }); };
        x.send(o.form);
      } catch (e) {
        fin({ ok: false, status: 0, error: 'No se pudo iniciar la subida. Intenta de nuevo.' });
      }
    });
  }

  function mb(n) { return (Number(n || 0) / 1048576).toFixed(1).replace('.', ','); }

  /* La barra: texto de la etapa + porcentaje + riel. `mostrar(texto, fraccion)`; `fraccion` null = sin porcentaje (animada). */
  function barra(contenedor) {
    contenedor.innerHTML = '<div class="fl-prog" hidden><div class="fl-prog-t"><span class="fl-prog-txt" role="status"></span><b class="fl-prog-pct"></b></div>' +
      '<div class="fl-prog-bar" role="progressbar" aria-valuemin="0" aria-valuemax="100" aria-label="Avance de la subida"><i></i></div></div>';
    var raiz = contenedor.firstChild, txt = raiz.querySelector('.fl-prog-txt'), pct = raiz.querySelector('.fl-prog-pct'),
      riel = raiz.querySelector('.fl-prog-bar'), relleno = riel.firstChild;
    return {
      mostrar: function (texto, fraccion) {
        raiz.hidden = false;
        txt.textContent = texto || '';
        if (fraccion == null) {
          riel.classList.add('ind'); relleno.style.width = ''; pct.textContent = ''; riel.removeAttribute('aria-valuenow');
        } else {
          var p = Math.max(0, Math.min(100, Math.round(fraccion * 100)));
          riel.classList.remove('ind'); relleno.style.width = p + '%'; pct.textContent = p + ' %'; riel.setAttribute('aria-valuenow', String(p));
        }
      },
      ocultar: function () { raiz.hidden = true; }
    };
  }

  window.flotaSubida = { comprimir: comprimir, enviar: enviar, barra: barra, mb: mb };
})();

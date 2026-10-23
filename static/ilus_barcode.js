/* ILUS Fitness — escáner de código de barras por cámara (motor único).
 *
 * Pedido de Daniel (2026-10-23): "Lenin me indica que en los repuestos el
 * buscador del código de barra por cámara de Android no le funciona y
 * cuesta muchísimo". Antes cada botón de Repuestos traía su propia copia
 * del escáner: video chico a 1280x720, sin enfoque continuo, sin guía,
 * decodificando el cuadro completo y sin plan B si el detector nativo de
 * Android fallaba en silencio. Aquí queda UNA sola implementación:
 *
 *   const codigo = await IlusBarcode.escanear({titulo, ayuda});
 *   // string con el código leído, o null si canceló / no hubo cámara
 *
 * Qué hace distinto (todo pensado para Android Chrome):
 *  - Pantalla completa (100dvh) con marco guía ancho para códigos 1D.
 *  - Cámara trasera a 1920x1080 y enfoque CONTINUO; zoom inicial ~2x
 *    cuando el teléfono lo permite (las etiquetas chicas se leen sin
 *    acercar tanto el teléfono, que es lo que hace perder el foco).
 *  - Botones de linterna, zoom +/− y cambio de lente (si hay más de una
 *    cámara trasera, p. ej. gran angular vs principal).
 *  - Decodifica primero la franja del marco (más rápido y menos ruido) y
 *    cada tercer intento el cuadro completo.
 *  - Motor de DOS capas: BarcodeDetector nativo y, si no lo trae o falla
 *    de forma repetida, polyfill WASM (zxing-cpp) desde CDN.
 *  - Lecturas poco confiables (ITF, Code 39, UPC-E) se confirman con una
 *    segunda lectura igual; Code 128 y EAN traen dígito verificador.
 *  - Pitido + vibración al leer, tecla Atrás del celular cierra el
 *    escáner (no saca a la persona de la página) y la cámara se apaga
 *    siempre al salir.
 *  - Mensajes claros: permiso denegado, cámara en uso, sin cámara, página
 *    sin HTTPS. Sin alert/confirm/prompt nativos (REGLA #1): usa ilusToast.
 *
 * La búsqueda por texto nunca se toca: este archivo solo ofrece un camino
 * más para llegar al mismo campo. Sin dependencias propias (Bootstrap
 * Icons ya viene en base.html; si falta, los botones igual llevan texto).
 */
(function (global) {
  'use strict';

  var FORMATOS = ['code_128', 'code_39', 'ean_13', 'ean_8', 'upc_a', 'upc_e', 'itf'];
  /* Formatos sin dígito verificador fuerte: se piden dos lecturas iguales. */
  var CONFIRMAR_DOS = { itf: 1, code_39: 1, upc_e: 1 };
  var POLYFILL_URLS = [
    'https://cdn.jsdelivr.net/npm/barcode-detector@3/dist/iife/index.js',
    'https://unpkg.com/barcode-detector@3/dist/iife/index.js'
  ];
  var ANCHO_MARCO = 0.88;   /* fracción del ancho visible */
  var ALTO_MARCO = 0.30;    /* fracción del alto visible */
  var PAUSA_MS = 70;        /* entre intentos de lectura */
  var FALLAS_PARA_CAMBIAR = 12;  /* errores seguidos del nativo → polyfill */
  var AVISO_SIN_LECTURA_MS = 7000;

  var abierto = false;
  var polyPromise = null;

  function aviso(msg, tipo) {
    if (typeof global.ilusToast === 'function') {
      global.ilusToast(msg, { type: tipo || 'info' });
    }
  }

  /* ── Motor de detección ───────────────────────────────────────────── */
  function cargarPolyfill() {
    if (global.BarcodeDetectionAPI) return Promise.resolve();
    if (polyPromise) return polyPromise;
    polyPromise = new Promise(function (res, rej) {
      var i = 0;
      (function siguiente() {
        if (i >= POLYFILL_URLS.length) { rej(new Error('polyfill')); return; }
        var s = document.createElement('script');
        s.src = POLYFILL_URLS[i++];
        s.async = true;
        s.onload = function () { res(); };
        s.onerror = function () { s.remove(); siguiente(); };
        document.head.appendChild(s);
      })();
    }).catch(function (e) { polyPromise = null; throw e; });
    return polyPromise;
  }

  function clasePolyfill() {
    return (global.BarcodeDetectionAPI && global.BarcodeDetectionAPI.BarcodeDetector) ||
           global.BarcodeDetector || null;
  }

  /* Devuelve {detect(src)->Promise<[{rawValue,format}]>, tipo} o null. */
  function crearMotor(forzarPolyfill) {
    var nativoPosible = !forzarPolyfill && typeof global.BarcodeDetector === 'function' && !global.BarcodeDetectionAPI;
    var probarNativo = nativoPosible
      ? global.BarcodeDetector.getSupportedFormats()
          .then(function (f) { return f.indexOf('code_128') >= 0; })
          .catch(function () { return false; })
      : Promise.resolve(false);
    return probarNativo.then(function (ok) {
      if (ok) {
        var det;
        try { det = new global.BarcodeDetector({ formats: FORMATOS }); }
        catch (e) { det = null; }
        if (det) return { tipo: 'nativo', detect: function (src) { return det.detect(src); } };
      }
      return cargarPolyfill().then(function () {
        var C = clasePolyfill();
        if (!C) return null;
        var det2 = new C({ formats: FORMATOS });
        return { tipo: 'polyfill', detect: function (src) { return det2.detect(src); } };
      }).catch(function () { return null; });
    });
  }

  /* ── Utilidades ───────────────────────────────────────────────────── */
  function pitar() {
    try {
      var C = global.AudioContext || global.webkitAudioContext;
      if (!C) return;
      var a = new C(), o = a.createOscillator(), g = a.createGain();
      o.connect(g); g.connect(a.destination);
      o.frequency.value = 1400; g.gain.value = 0.12;
      o.start();
      setTimeout(function () { try { o.stop(); a.close(); } catch (e) {} }, 120);
    } catch (e) { /* sin audio no pasa nada */ }
  }

  function mensajeErrorCamara(e) {
    var n = e && e.name;
    if (!global.isSecureContext) {
      return 'La cámara solo funciona en una página segura (https).';
    }
    if (n === 'NotAllowedError' || n === 'SecurityError') {
      return 'Sin permiso para la cámara. Tócalo en el candado de la barra de direcciones y elige "Permitir".';
    }
    if (n === 'NotFoundError' || n === 'OverconstrainedError') {
      return 'Este equipo no tiene una cámara disponible.';
    }
    if (n === 'NotReadableError' || n === 'AbortError') {
      return 'La cámara está en uso por otra aplicación. Ciérrala e inténtalo de nuevo.';
    }
    return 'No se pudo acceder a la cámara — revisa los permisos.';
  }

  function inyectarEstilos() {
    if (document.getElementById('ilusBarcodeCss')) return;
    var st = document.createElement('style');
    st.id = 'ilusBarcodeCss';
    st.textContent =
      '#ilusBcOverlay{position:fixed;inset:0;height:100vh;height:100dvh;background:#000;z-index:2000;' +
      'color:#fff;overflow:hidden;font-family:inherit;touch-action:manipulation}' +
      '#ilusBcOverlay video{position:absolute;inset:0;width:100%;height:100%;object-fit:cover;background:#000}' +
      '#ilusBcOverlay .bc-top{position:absolute;left:0;right:0;top:0;display:flex;align-items:center;gap:8px;' +
      'padding:calc(env(safe-area-inset-top,0px) + 10px) 12px 10px;' +
      'background:linear-gradient(rgba(0,0,0,.65),rgba(0,0,0,0))}' +
      '#ilusBcOverlay .bc-titulo{flex:1;font-weight:700;font-size:1rem;line-height:1.25}' +
      '#ilusBcOverlay .bc-marco{position:absolute;border:3px solid #dc2626;border-radius:12px;pointer-events:none;' +
      'box-shadow:0 0 0 100vmax rgba(0,0,0,.45);transition:border-color .15s}' +
      '#ilusBcOverlay .bc-marco.bc-ok{border-color:#16a34a}' +
      '#ilusBcOverlay .bc-linea{position:absolute;left:8px;right:8px;top:50%;height:2px;background:#dc2626;' +
      'box-shadow:0 0 10px rgba(220,38,38,.9);animation:ilusBcLinea 1.6s ease-in-out infinite alternate}' +
      '@keyframes ilusBcLinea{from{top:12%}to{top:88%}}' +
      '@media (prefers-reduced-motion:reduce){#ilusBcOverlay .bc-linea{animation:none}}' +
      '#ilusBcOverlay .bc-bottom{position:absolute;left:0;right:0;bottom:0;display:flex;flex-direction:column;' +
      'align-items:center;gap:10px;padding:14px 12px calc(env(safe-area-inset-bottom,0px) + 14px);' +
      'background:linear-gradient(rgba(0,0,0,0),rgba(0,0,0,.75))}' +
      '#ilusBcOverlay .bc-estado{text-align:center;font-size:.92rem;max-width:480px;min-height:2.6em}' +
      '#ilusBcOverlay .bc-fila{display:flex;flex-wrap:wrap;gap:8px;justify-content:center}' +
      '#ilusBcOverlay .bc-btn{min-height:44px;min-width:44px;padding:8px 16px;border-radius:10px;font-weight:600;' +
      'border:1px solid rgba(255,255,255,.55);background:rgba(0,0,0,.45);color:#fff;font-size:.95rem}' +
      '#ilusBcOverlay .bc-btn.bc-activo{background:#fff;color:#0a0a0a}' +
      '#ilusBcOverlay .bc-btn.bc-cancelar{background:#fff;color:#0a0a0a;border-color:#fff}' +
      '#ilusBcOverlay .bc-btn:focus-visible{outline:3px solid #fff;outline-offset:2px}';
    document.head.appendChild(st);
  }

  /* ── Escáner ──────────────────────────────────────────────────────── */
  function escanear(opts) {
    opts = opts || {};
    if (abierto) return Promise.resolve(null);
    abierto = true;
    inyectarEstilos();

    return new Promise(function (resolve) {
      var stream = null, track = null, motor = null;
      var activo = true, cerrado = false, motorListo = false;
      var temporizador = null, ultimaLectura = null, errSeguidos = 0, intento = 0;
      var linterna = false, zoomActual = 1, zoomCaps = null;
      var lentes = [], lenteIdx = 0;
      var inicioLectura = 0, avisado = false;
      var empujoHistoria = false;
      var canvas = document.createElement('canvas');
      var ctx = canvas.getContext('2d');

      var overlay = document.createElement('div');
      overlay.id = 'ilusBcOverlay';
      overlay.setAttribute('role', 'dialog');
      overlay.setAttribute('aria-modal', 'true');
      overlay.setAttribute('aria-label', opts.titulo || 'Escanear código de barras');
      overlay.innerHTML =
        '<video autoplay playsinline muted></video>' +
        '<div class="bc-top"><div class="bc-titulo"></div>' +
        '<button type="button" class="bc-btn bc-x" aria-label="Cerrar escáner"><i class="bi bi-x-lg"></i></button></div>' +
        '<div class="bc-marco"><div class="bc-linea"></div></div>' +
        '<div class="bc-bottom"><div class="bc-estado" aria-live="polite"></div>' +
        '<div class="bc-fila">' +
        '<button type="button" class="bc-btn bc-luz" hidden><i class="bi bi-lightning-charge-fill me-1"></i>Linterna</button>' +
        '<button type="button" class="bc-btn bc-zmenos" hidden aria-label="Alejar">&minus;</button>' +
        '<button type="button" class="bc-btn bc-zmas" hidden aria-label="Acercar">+</button>' +
        '<button type="button" class="bc-btn bc-lente" hidden><i class="bi bi-camera me-1"></i>Cambiar cámara</button>' +
        '<button type="button" class="bc-btn bc-cancelar"><i class="bi bi-x-lg me-1"></i>Cancelar</button>' +
        '</div></div>';
      overlay.querySelector('.bc-titulo').textContent = opts.titulo || 'Escanear código de barras';
      document.body.appendChild(overlay);

      var video = overlay.querySelector('video');
      var marco = overlay.querySelector('.bc-marco');
      var estado = overlay.querySelector('.bc-estado');
      var btnLuz = overlay.querySelector('.bc-luz');
      var btnZm = overlay.querySelector('.bc-zmenos');
      var btnZp = overlay.querySelector('.bc-zmas');
      var btnLente = overlay.querySelector('.bc-lente');

      function decir(html) { estado.innerHTML = html; }
      function ayuda() {
        decir(opts.ayuda ? escapar(opts.ayuda) :
          'Apunta al código de barras y mantén el teléfono quieto');
      }
      function escapar(s) {
        return String(s).replace(/[&<>"]/g, function (c) {
          return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c];
        });
      }

      function posicionarMarco() {
        var w = overlay.clientWidth, h = overlay.clientHeight;
        var mw = Math.round(w * ANCHO_MARCO), mh = Math.round(Math.min(h * ALTO_MARCO, mw * 0.55));
        marco.style.width = mw + 'px';
        marco.style.height = mh + 'px';
        marco.style.left = Math.round((w - mw) / 2) + 'px';
        marco.style.top = Math.round((h - mh) / 2.4) + 'px';
      }

      function cerrar(valor) {
        if (cerrado) return;
        cerrado = true; activo = false; abierto = false;
        if (temporizador) { clearTimeout(temporizador); temporizador = null; }
        try { if (track && linterna) track.applyConstraints({ advanced: [{ torch: false }] }); } catch (e) {}
        if (stream) {
          stream.getTracks().forEach(function (t) { try { t.stop(); } catch (e) {} });
          stream = null; track = null;
        }
        try { video.srcObject = null; } catch (e) {}
        overlay.remove();
        global.removeEventListener('popstate', alAtras);
        global.removeEventListener('pagehide', alSalir);
        document.removeEventListener('keydown', alTecla);
        global.removeEventListener('resize', posicionarMarco);
        var res = valor == null ? null : valor;
        if (empujoHistoria && global.history.state && global.history.state.ilusBc) {
          /* Se retira la entrada de historial que abrió el escáner y SOLO
             después se entrega el código: quien lo recibe puede enviar un
             formulario (navegar) sin que el "atrás" pendiente lo pise. */
          empujoHistoria = false;
          var hecho = false;
          var fin = function () {
            if (hecho) return;
            hecho = true;
            global.removeEventListener('popstate', fin);
            resolve(res);
          };
          global.addEventListener('popstate', fin);
          setTimeout(fin, 600);
          try { global.history.back(); } catch (e) { fin(); }
        } else {
          resolve(res);
        }
      }
      function alAtras() { empujoHistoria = false; cerrar(null); }
      function alSalir() { empujoHistoria = false; cerrar(null); }
      function alTecla(ev) { if (ev.key === 'Escape') cerrar(null); }

      /* ── Cámara ── */
      function restriccionesBase(deviceId) {
        var v = { width: { ideal: 1920 }, height: { ideal: 1080 }, frameRate: { ideal: 30 } };
        if (deviceId) v.deviceId = { exact: deviceId };
        else v.facingMode = { ideal: 'environment' };
        return { video: v, audio: false };
      }

      function obtenerStream(deviceId) {
        return navigator.mediaDevices.getUserMedia(restriccionesBase(deviceId)).catch(function (e) {
          /* Algunos equipos rechazan 1080p o el deviceId: reintenta sencillo. */
          if (e && (e.name === 'OverconstrainedError' || e.name === 'ConstraintNotSatisfiedError')) {
            return navigator.mediaDevices.getUserMedia({ video: { facingMode: 'environment' }, audio: false });
          }
          throw e;
        });
      }

      function descubrirLentes() {
        if (!navigator.mediaDevices.enumerateDevices) return Promise.resolve();
        return navigator.mediaDevices.enumerateDevices().then(function (ds) {
          var traseras = ds.filter(function (d) {
            return d.kind === 'videoinput' && /back|rear|trasera|environment/i.test(d.label || '');
          });
          /* Fuera lentes que no sirven para códigos: gran angular, tele, profundidad, macro de baja res. */
          var utiles = traseras.filter(function (d) { return !/ultra|depth|tele|macro|infrared/i.test(d.label); });
          lentes = (utiles.length ? utiles : traseras);
          var actual = track && track.getSettings ? track.getSettings().deviceId : null;
          var i = lentes.findIndex(function (d) { return d.deviceId === actual; });
          lenteIdx = i >= 0 ? i : 0;
          btnLente.hidden = lentes.length < 2;
        }).catch(function () {});
      }

      function configurarPista() {
        track = stream.getVideoTracks()[0];
        var caps = {};
        try { caps = track.getCapabilities ? track.getCapabilities() : {}; } catch (e) {}
        var avanzado = [];
        if (caps.focusMode && caps.focusMode.indexOf('continuous') >= 0) avanzado.push({ focusMode: 'continuous' });
        if (caps.exposureMode && caps.exposureMode.indexOf('continuous') >= 0) avanzado.push({ exposureMode: 'continuous' });
        if (caps.whiteBalanceMode && caps.whiteBalanceMode.indexOf('continuous') >= 0) avanzado.push({ whiteBalanceMode: 'continuous' });
        if (caps.zoom && caps.zoom.max > caps.zoom.min) {
          zoomCaps = caps.zoom;
          zoomActual = Math.min(zoomCaps.max, Math.max(zoomCaps.min, 2));
          avanzado.push({ zoom: zoomActual });
          btnZm.hidden = false; btnZp.hidden = false;
        } else { zoomCaps = null; btnZm.hidden = btnZp.hidden = true; }
        btnLuz.hidden = !caps.torch;
        linterna = false; btnLuz.classList.remove('bc-activo');
        if (avanzado.length) {
          track.applyConstraints({ advanced: avanzado }).catch(function () {
            /* Si el conjunto falla, prueba cada uno por separado (hay equipos que aceptan solo algunos). */
            avanzado.forEach(function (c) { track.applyConstraints({ advanced: [c] }).catch(function () {}); });
          });
        }
      }

      function ajustarZoom(delta) {
        if (!track || !zoomCaps) return;
        var paso = Math.max((zoomCaps.max - zoomCaps.min) / 8, 0.25);
        zoomActual = Math.min(zoomCaps.max, Math.max(zoomCaps.min, zoomActual + delta * paso));
        track.applyConstraints({ advanced: [{ zoom: zoomActual }] }).catch(function () {});
      }

      function alternarLinterna() {
        if (!track) return;
        linterna = !linterna;
        track.applyConstraints({ advanced: [{ torch: linterna }] }).then(function () {
          btnLuz.classList.toggle('bc-activo', linterna);
        }).catch(function () {
          linterna = false;
          btnLuz.hidden = true;
          aviso('Esta cámara no permite encender la linterna desde aquí.', 'warning');
        });
      }

      function arrancarCamara(deviceId) {
        return obtenerStream(deviceId).then(function (s) {
          if (cerrado) { s.getTracks().forEach(function (t) { try { t.stop(); } catch (e) {} }); return null; }
          stream = s;
          video.srcObject = s;
          configurarPista();
          return video.play().catch(function () {});
        });
      }

      function cambiarLente() {
        if (lentes.length < 2) return;
        lenteIdx = (lenteIdx + 1) % lentes.length;
        var viejo = stream;
        activo = false;
        if (viejo) viejo.getTracks().forEach(function (t) { try { t.stop(); } catch (e) {} });
        stream = null; track = null;
        decir('Cambiando de cámara…');
        arrancarCamara(lentes[lenteIdx].deviceId).then(function () {
          if (cerrado) return;
          activo = true; inicioLectura = Date.now(); avisado = false; ayuda(); programar(0);
        }).catch(function (e) { aviso(mensajeErrorCamara(e), 'error'); cerrar(null); });
      }

      /* ── Lectura ── */
      function geometriaMarco() {
        /* Rectángulo del marco en píxeles del VIDEO (object-fit:cover). */
        var vw = video.videoWidth, vh = video.videoHeight;
        var bw = overlay.clientWidth, bh = overlay.clientHeight;
        if (!vw || !vh || !bw || !bh) return null;
        var esc = Math.max(bw / vw, bh / vh);
        var offX = (bw - vw * esc) / 2, offY = (bh - vh * esc) / 2;
        var r = marco.getBoundingClientRect();
        var o = overlay.getBoundingClientRect();
        var padX = r.width * 0.06, padY = r.height * 0.25;
        var x = (r.left - o.left - padX - offX) / esc;
        var y = (r.top - o.top - padY - offY) / esc;
        var w = (r.width + padX * 2) / esc;
        var h = (r.height + padY * 2) / esc;
        x = Math.max(0, x); y = Math.max(0, y);
        w = Math.min(vw - x, w); h = Math.min(vh - y, h);
        if (w < 40 || h < 40) return null;
        return { x: x, y: y, w: w, h: h };
      }

      function fuenteDeteccion() {
        intento++;
        /* Cada tercer intento (o si no hay geometría) se mira el cuadro completo. */
        if (intento % 3 === 0) return video;
        var g = geometriaMarco();
        if (!g) return video;
        canvas.width = Math.round(g.w); canvas.height = Math.round(g.h);
        ctx.drawImage(video, g.x, g.y, g.w, g.h, 0, 0, canvas.width, canvas.height);
        return canvas;
      }

      function programar(ms) {
        if (temporizador) clearTimeout(temporizador);
        temporizador = setTimeout(tick, ms == null ? PAUSA_MS : ms);
      }

      function leido(cod) {
        var valor = (cod.rawValue || '').trim();
        if (!valor) return false;
        var fmt = cod.format || '';
        if (CONFIRMAR_DOS[fmt]) {
          var ahora = Date.now();
          if (!ultimaLectura || ultimaLectura.valor !== valor || ahora - ultimaLectura.t > 1500) {
            ultimaLectura = { valor: valor, t: ahora };
            return false;
          }
        }
        activo = false;
        marco.classList.add('bc-ok');
        pitar();
        if (navigator.vibrate) { try { navigator.vibrate(60); } catch (e) {} }
        decir('<i class="bi bi-check-circle-fill me-1"></i><strong>' + escapar(valor) + '</strong>');
        setTimeout(function () { cerrar(valor); }, 180);
        return true;
      }

      function tick() {
        temporizador = null;
        if (!activo || cerrado || !motorListo) return;
        if (video.readyState < 2 || !video.videoWidth) { programar(120); return; }
        if (!avisado && Date.now() - inicioLectura > AVISO_SIN_LECTURA_MS) {
          avisado = true;
          decir('¿Cuesta leerlo? Prueba con más luz (Linterna), aléjate un poco hasta que se vea nítido o ' +
                'usa el zoom. También puedes cerrar y escribir el código.');
        }
        var src;
        try { src = fuenteDeteccion(); } catch (e) { src = video; }
        motor.detect(src).then(function (codigos) {
          errSeguidos = 0;
          if (!activo || cerrado) return;
          if (codigos && codigos.length) {
            for (var i = 0; i < codigos.length; i++) { if (leido(codigos[i])) return; }
          }
          programar();
        }).catch(function () {
          errSeguidos++;
          if (motor.tipo === 'nativo' && errSeguidos >= FALLAS_PARA_CAMBIAR) {
            /* El detector del teléfono no responde (servicio sin descargar, etc.): pasa al polyfill. */
            errSeguidos = 0;
            decir('Activando lector alternativo…');
            crearMotor(true).then(function (m) {
              if (cerrado) return;
              if (!m) { aviso('No se pudo activar el lector de códigos. Usa la búsqueda por texto.', 'warning'); cerrar(null); return; }
              motor = m; ayuda(); programar(0);
            });
            return;
          }
          programar(errSeguidos > 3 ? 250 : PAUSA_MS);
        });
      }

      /* ── Arranque ── */
      overlay.querySelector('.bc-x').onclick = function () { cerrar(null); };
      overlay.querySelector('.bc-cancelar').onclick = function () { cerrar(null); };
      btnLuz.onclick = alternarLinterna;
      btnZm.onclick = function () { ajustarZoom(-1); };
      btnZp.onclick = function () { ajustarZoom(1); };
      btnLente.onclick = cambiarLente;
      global.addEventListener('popstate', alAtras);
      global.addEventListener('pagehide', alSalir);
      document.addEventListener('keydown', alTecla);
      global.addEventListener('resize', posicionarMarco);
      try {
        global.history.pushState({ ilusBc: 1 }, '');
        empujoHistoria = true;
      } catch (e) {}
      posicionarMarco();
      decir('<span class="spinner-border spinner-border-sm me-2"></span>Iniciando cámara…');

      if (!navigator.mediaDevices || !navigator.mediaDevices.getUserMedia) {
        aviso(mensajeErrorCamara({ name: 'SecurityError' }), 'error');
        cerrar(null);
        return;
      }

      /* La cámara y el motor arrancan en paralelo: el permiso del teléfono
         se pide ya mientras se prepara (o descarga) el detector. */
      var pCamara = arrancarCamara(null);
      var pMotor = Promise.resolve().then(function () { return crearMotor(false); }).catch(function () { return null; });
      pCamara.then(function () {
        if (cerrado) return null;
        return descubrirLentes();
      }).then(function () { return pMotor; }).then(function (m) {
        if (cerrado) return;
        if (!m) {
          aviso('No se pudo activar el escáner (sin internet o navegador muy antiguo) — usa la búsqueda por texto', 'warning');
          cerrar(null);
          return;
        }
        motor = m; motorListo = true;
        inicioLectura = Date.now();
        ayuda();
        programar(0);
      }).catch(function (e) {
        if (cerrado) return;
        aviso(mensajeErrorCamara(e), 'error');
        cerrar(null);
      });
    });
  }

  /* Descarga el polyfill en tiempo muerto si el navegador no trae el nativo,
     para que el primer escaneo real no espere. */
  function precargar() {
    (global.requestIdleCallback || function (f) { return setTimeout(f, 2500); })(function () {
      if (typeof global.BarcodeDetector === 'function') return;
      cargarPolyfill().catch(function () {});
    });
  }

  global.IlusBarcode = { escanear: escanear, precargar: precargar, formatos: FORMATOS.slice() };
})(window);

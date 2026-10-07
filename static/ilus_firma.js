/* ILUS Fitness · Componente ÚNICO de firma manuscrita (canvas).
   Uso:  var f = window.IlusFirma.crear(document.getElementById('miCanvas'), { onCambio: function (firmado) { ... } });
         f.firmado()  → true si hay una firma real (largo de trazo >= minLargo)
         f.aDataUrl() → 'data:image/png;base64,...' con FONDO BLANCO (o '' si no se dibujó nada)
         f.limpiar() · f.deshacer() · f.setColor('#1e40af') · f.redibujar() · f.destruir()

   Por qué existe (Daniel 2026-10-07: «unifica el componente de firma»): había 7 copias con bugs distintos
   (firma EN BLANCO tras un resize, trazos desfasados al rotar, firma deformada, borrosa en retina, un
   segundo dedo que saltaba el trazo, PNG transparente invisible sobre fondo oscuro).

   Cómo lo resuelve:
   · Pointer Events + setPointerCapture; se IGNORA un segundo puntero mientras hay un trazo en curso.
   · Los trazos se guardan como VECTORES en coordenadas CSS de referencia y se redibujan ante resize,
     rotación, ResizeObserver o reapertura de un modal. Si el recuadro cambia de proporción se escala
     uniforme (min(w/refW, h/refH)) y se centra: la firma nunca se pierde ni se deforma.
   · Escala por devicePixelRatio con tope 2 (nítido en retina sin inflar el PNG).
   · Suavizado con quadraticCurveTo por punto medio.
   · Detección de firma real por largo mínimo del trazo (opciones.minLargo, en px CSS).
   · aDataUrl(): re-dibuja los vectores sobre un canvas aparte con fondo blanco, ancho máx. configurable
     (1200 px por defecto) y baja el tamaño hasta quedar bajo ~300 KB.
   Sin dependencias. Sin alert/confirm. Un toque sin arrastre se dibuja como punto pero NO cuenta como firma
   (salvo minLargo:0). */
(function (global) {
  'use strict';

  var DEFECTO = {
    color: '#0a0a0a',        // color del trazo (se puede cambiar con setColor, afecta a los trazos NUEVOS)
    grosor: 2.6,             // grosor en px CSS
    minLargo: 20,            // largo total mínimo de trazo (px CSS) para considerar que hay firma
    maxAncho: 1200,          // ancho máximo del PNG exportado
    maxBytes: 300 * 1024,    // tope aproximado del PNG exportado
    dprMax: 2,               // tope del devicePixelRatio
    fondo: '#ffffff',        // fondo del PNG exportado
    fondoPantalla: false,    // true = también pinta el fondo dentro del canvas en pantalla
    lineaBase: false,        // true = dibuja una línea base en pantalla (no sale en el PNG)
    marcaX: false,           // true = dibuja una «X» al inicio de la línea base (solo pantalla)
    guia: null,              // elemento (o id) con el texto guía: se oculta al firmar
    ariaLabel: 'Área para dibujar la firma manuscrita',
    permitir: null,          // function () → false impide empezar un trazo (candados propios de cada pantalla)
    onCambio: null           // function (firmado, info) tras cada trazo, deshacer o limpiar
  };

  // ── Cálculos puros (se prueban también con Node) ──
  function largoTrazo(pts) {
    var l = 0;
    for (var i = 1; i < pts.length; i++) {
      var dx = pts[i].x - pts[i - 1].x, dy = pts[i].y - pts[i - 1].y;
      l += Math.sqrt(dx * dx + dy * dy);
    }
    return l;
  }
  function largoTotal(trazos) {
    var l = 0;
    for (var i = 0; i < trazos.length; i++) l += largoTrazo(trazos[i].pts);
    return l;
  }
  function hayFirma(trazos, minLargo) {
    if (!trazos.length) return false;
    return largoTotal(trazos) >= minLargo && (minLargo > 0 || trazos.length > 0);
  }
  // Escala uniforme + centrado de la caja de referencia dentro de la caja actual.
  function transformacion(refW, refH, w, h) {
    if (!refW || !refH || !w || !h) return { s: 1, ox: 0, oy: 0 };
    var s = Math.min(w / refW, h / refH);
    return { s: s, ox: (w - refW * s) / 2, oy: (h - refH * s) / 2 };
  }

  function trazarVector(ctx, t) {
    var pts = t.pts;
    ctx.strokeStyle = t.color; ctx.fillStyle = t.color; ctx.lineWidth = t.grosor;
    ctx.lineCap = 'round'; ctx.lineJoin = 'round';
    if (!pts.length) return;
    if (pts.length === 1) {                     // toque sin arrastre: un punto visible
      ctx.beginPath(); ctx.arc(pts[0].x, pts[0].y, t.grosor * 0.6, 0, Math.PI * 2); ctx.fill();
      return;
    }
    if (pts.length === 2) {
      ctx.beginPath(); ctx.moveTo(pts[0].x, pts[0].y); ctx.lineTo(pts[1].x, pts[1].y); ctx.stroke();
      return;
    }
    ctx.beginPath(); ctx.moveTo(pts[0].x, pts[0].y);
    for (var i = 1; i < pts.length - 1; i++) {   // suavizado por punto medio
      var mx = (pts[i].x + pts[i + 1].x) / 2, my = (pts[i].y + pts[i + 1].y) / 2;
      ctx.quadraticCurveTo(pts[i].x, pts[i].y, mx, my);
    }
    ctx.lineTo(pts[pts.length - 1].x, pts[pts.length - 1].y);
    ctx.stroke();
  }

  function crear(canvas, opciones) {
    if (!canvas || !canvas.getContext) throw new Error('IlusFirma: falta el canvas');
    if (canvas._ilusFirma) return canvas._ilusFirma;      // idempotente: un modal que se reabre no duplica listeners
    var op = {}, k;
    for (k in DEFECTO) op[k] = DEFECTO[k];
    for (k in (opciones || {})) if (opciones[k] !== undefined) op[k] = opciones[k];

    var ctx = canvas.getContext('2d');
    var trazos = [];            // [{color, grosor, pts:[{x,y}]}] en coordenadas de la caja de referencia
    var actual = null;          // trazo en curso
    var punteroActivo = null;   // id del único puntero que dibuja
    var refW = 0, refH = 0;     // caja de referencia (tamaño CSS cuando se empezó a firmar)
    var cssW = 0, cssH = 0, dprUsado = 0;
    var color = op.color;
    var guia = typeof op.guia === 'string' ? document.getElementById(op.guia) : op.guia;
    var ultimoFirmado = null, ro = null, rafPend = false, vivo = true;

    canvas.classList.add('ilus-firma');
    canvas.style.touchAction = 'none';
    if (!canvas.getAttribute('aria-label') && !canvas.getAttribute('aria-labelledby')) canvas.setAttribute('aria-label', op.ariaLabel);

    function dpr() { return Math.min(Math.max(1, global.devicePixelRatio || 1), op.dprMax); }

    // Ajusta el mapa de bits al tamaño CSS actual. Devuelve false si el canvas está oculto (modal cerrado).
    function ajustar() {
      var w = canvas.clientWidth, h = canvas.clientHeight;
      if (!w || !h) { var r0 = canvas.getBoundingClientRect(); w = r0.width; h = r0.height; }
      if (!w || !h) return false;
      var d = dpr(), bw = Math.round(w * d), bh = Math.round(h * d);
      if (Math.abs(w - cssW) < 0.5 && Math.abs(h - cssH) < 0.5 && d === dprUsado && canvas.width === bw && canvas.height === bh) return true;
      cssW = w; cssH = h; dprUsado = d;
      if (canvas.width !== bw) canvas.width = bw;       // asignar width/height borra el mapa de bits: se redibuja enseguida
      if (canvas.height !== bh) canvas.height = bh;
      if (!trazos.length && !actual) { refW = w; refH = h; }
      if (!refW || !refH) { refW = w; refH = h; }
      pintar();
      return true;
    }

    function tf() { return transformacion(refW, refH, cssW, cssH); }

    function pintar() {
      if (!cssW || !cssH) return;
      var kx = canvas.width / cssW, ky = canvas.height / cssH;
      ctx.setTransform(1, 0, 0, 1, 0, 0);
      ctx.clearRect(0, 0, canvas.width, canvas.height);
      ctx.setTransform(kx, 0, 0, ky, 0, 0);
      if (op.fondoPantalla) { ctx.fillStyle = op.fondo; ctx.fillRect(0, 0, cssW, cssH); }
      if (op.lineaBase) {
        var yb = cssH * 0.78;
        ctx.strokeStyle = '#cbd5e1'; ctx.lineWidth = 1; ctx.beginPath();
        ctx.moveTo(cssW * 0.06, yb); ctx.lineTo(cssW * 0.94, yb); ctx.stroke();
        if (op.marcaX) { ctx.fillStyle = '#9ca3af'; ctx.font = '600 14px sans-serif'; ctx.textBaseline = 'alphabetic'; ctx.fillText('✕', cssW * 0.06, yb - 6); }
      }
      var t = tf();
      ctx.setTransform(kx * t.s, 0, 0, ky * t.s, kx * t.ox, ky * t.oy);
      for (var i = 0; i < trazos.length; i++) trazarVector(ctx, trazos[i]);
      if (actual) trazarVector(ctx, actual);
      ctx.setTransform(1, 0, 0, 1, 0, 0);
    }

    // Punto del evento → coordenadas de la caja de referencia (deshace escala/centrado actuales).
    function punto(ev) {
      var r = canvas.getBoundingClientRect();
      var rw = r.width || cssW || 1, rh = r.height || cssH || 1;
      var x = (ev.clientX - r.left) * (cssW / rw), y = (ev.clientY - r.top) * (cssH / rh);
      var t = tf();
      return { x: (x - t.ox) / t.s, y: (y - t.oy) / t.s };
    }

    function info() { return { trazos: trazos.length, largo: largoTotal(trazos) }; }
    function firmado() { return hayFirma(trazos, op.minLargo); }
    function actualizarGuia() {
      if (!guia) return;
      guia.style.display = (trazos.length || actual) ? 'none' : '';
    }
    function avisar() {
      actualizarGuia();
      var f = firmado();
      if (typeof op.onCambio === 'function') { try { op.onCambio(f, info()); } catch (e) { if (global.console) console.error(e); } }
      ultimoFirmado = f;
    }

    function agregar(ev) {
      var p = punto(ev), u = actual.pts[actual.pts.length - 1];
      if (u && Math.abs(p.x - u.x) < 0.5 && Math.abs(p.y - u.y) < 0.5) return;   // ruido: punto casi idéntico
      actual.pts.push(p);
    }

    function alBajar(ev) {
      if (punteroActivo !== null) return;                       // segundo dedo: se ignora, el trazo sigue donde estaba
      if (ev.pointerType === 'mouse' && ev.button !== 0) return;
      if (typeof op.permitir === 'function' && !op.permitir()) return;
      if (!ajustar() && !cssW) return;
      ev.preventDefault();
      try { canvas.setPointerCapture(ev.pointerId); } catch (e) { /* sin captura: sigue funcionando */ }
      punteroActivo = ev.pointerId;
      if (!trazos.length) { refW = cssW; refH = cssH; }
      actual = { color: color, grosor: op.grosor, pts: [punto(ev)] };
      actualizarGuia();
      pintar();
    }
    function alMover(ev) {
      if (punteroActivo === null || ev.pointerId !== punteroActivo || !actual) return;
      ev.preventDefault();
      var lista = (typeof ev.getCoalescedEvents === 'function') ? ev.getCoalescedEvents() : null;
      if (lista && lista.length) { for (var i = 0; i < lista.length; i++) agregar(lista[i]); } else agregar(ev);
      pintar();
    }
    function alSoltar(ev) {
      if (punteroActivo === null || (ev && ev.pointerId !== undefined && ev.pointerId !== punteroActivo)) return;
      try { canvas.releasePointerCapture(punteroActivo); } catch (e) { /* ya liberado */ }
      punteroActivo = null;
      if (actual) { trazos.push(actual); actual = null; }
      pintar();
      avisar();
    }
    function sinMenu(ev) { ev.preventDefault(); }

    canvas.addEventListener('pointerdown', alBajar);
    canvas.addEventListener('pointermove', alMover);
    canvas.addEventListener('pointerup', alSoltar);
    canvas.addEventListener('pointercancel', alSoltar);
    canvas.addEventListener('lostpointercapture', alSoltar);
    canvas.addEventListener('contextmenu', sinMenu);

    // Resize / rotación / modal que se abre: se re-ajusta sin perder los vectores.
    function programarAjuste() {
      if (rafPend || !vivo) return;
      rafPend = true;
      var correr = function () { rafPend = false; if (vivo) ajustar(); };
      if (global.requestAnimationFrame) global.requestAnimationFrame(correr); else setTimeout(correr, 16);
    }
    if (typeof global.ResizeObserver === 'function') { ro = new global.ResizeObserver(programarAjuste); ro.observe(canvas); }
    global.addEventListener('resize', programarAjuste);
    global.addEventListener('orientationchange', programarAjuste);

    function limpiar() {
      trazos = []; actual = null;
      if (punteroActivo !== null) { try { canvas.releasePointerCapture(punteroActivo); } catch (e) {} punteroActivo = null; }
      if (cssW && cssH) { refW = cssW; refH = cssH; }
      pintar(); avisar();
    }
    function deshacer() {
      if (!trazos.length) return;
      trazos.pop();
      if (!trazos.length && cssW && cssH) { refW = cssW; refH = cssH; }
      pintar(); avisar();
    }
    function setColor(c) { color = c; }

    // PNG con fondo blanco, redibujado desde los vectores (no copia el canvas de pantalla).
    function aDataUrl(extra) {
      var o = extra || {};
      var maxAncho = o.maxAncho || op.maxAncho, maxBytes = o.maxBytes || op.maxBytes;
      ajustar();
      if (!trazos.length || !cssW || !cssH) return '';
      var t = tf();
      var ancho = Math.max(2, Math.min(maxAncho, Math.round(cssW * dpr())));
      var url = '';
      for (var intento = 0; intento < 6; intento++) {
        var alto = Math.max(2, Math.round(ancho * cssH / cssW));
        var c2 = document.createElement('canvas'); c2.width = ancho; c2.height = alto;
        var x2 = c2.getContext('2d');
        x2.fillStyle = o.fondo || op.fondo; x2.fillRect(0, 0, ancho, alto);
        var e = ancho / cssW;
        x2.setTransform(e * t.s, 0, 0, e * t.s, e * t.ox, e * t.oy);
        for (var i = 0; i < trazos.length; i++) trazarVector(x2, trazos[i]);
        url = c2.toDataURL('image/png');
        if ((url.length - 22) * 0.75 <= maxBytes || ancho <= 320) break;
        ancho = Math.round(ancho * 0.75);          // pasó del tope: se reduce y se reintenta
      }
      return url;
    }

    function destruir() {
      vivo = false;
      canvas.removeEventListener('pointerdown', alBajar);
      canvas.removeEventListener('pointermove', alMover);
      canvas.removeEventListener('pointerup', alSoltar);
      canvas.removeEventListener('pointercancel', alSoltar);
      canvas.removeEventListener('lostpointercapture', alSoltar);
      canvas.removeEventListener('contextmenu', sinMenu);
      global.removeEventListener('resize', programarAjuste);
      global.removeEventListener('orientationchange', programarAjuste);
      if (ro) ro.disconnect();
      delete canvas._ilusFirma;
    }

    var api = {
      limpiar: limpiar, deshacer: deshacer, setColor: setColor, redibujar: function () { ajustar(); pintar(); },
      firmado: firmado, aDataUrl: aDataUrl, destruir: destruir,
      numTrazos: function () { return trazos.length; },
      largo: function () { return largoTotal(trazos); },
      vacio: function () { return trazos.length === 0; },
      canvas: canvas
    };
    canvas._ilusFirma = api;
    ajustar();
    actualizarGuia();
    return api;
  }

  var IlusFirma = { crear: crear, _puro: { largoTrazo: largoTrazo, largoTotal: largoTotal, hayFirma: hayFirma, transformacion: transformacion } };
  global.IlusFirma = IlusFirma;
  if (typeof module !== 'undefined' && module.exports) module.exports = IlusFirma;
})(typeof window !== 'undefined' ? window : globalThis);

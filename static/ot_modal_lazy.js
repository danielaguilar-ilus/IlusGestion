/* 🚀 2026-10-23 — Cargador diferido del asistente «Generar OT» (OT2C).
 *
 * Daniel: «mejorar el performance de este modal… tecnología del 2028». El asistente pesa ~765 KB (HTML, estilos y ~12 mil líneas de JS)
 * y antes viajaba DENTRO de tres páginas (Órdenes de trabajo, ficha de ticket y Repuestos) en cada carga. Ahora esas páginas traen solo
 * este cargador (~3 KB) y el asistente se descarga desde /ot/modal-crear:
 *   · en un momento ocioso tras cargar la página (solo lo baja al caché del navegador: no ejecuta nada),
 *   · al acercar el mouse o el dedo a un botón que lo abre, y
 *   · al instante si la URL trae ?crear=1.
 * Mientras no está cargado, `window.OT2C` es un cascarón con los mismos métodos públicos: la llamada espera a que cargue y se ejecuta.
 * Si la descarga falla se avisa y se puede reintentar (nunca queda el botón muerto).
 */
(function () {
  'use strict';
  if (window.OT2C && window.OT2C.__real) return;

  var URL_FRAGMENTO = '/ot/modal-crear';
  var METODOS = ['abrir', 'abrirWizard', 'abrirDesdeTicket', 'abrirDesdeHallazgos', 'cerrar', 'irAncla', 'avisarBloqueado'];
  var promesa = null;     // carga en curso o terminada
  var html = null;        // fragmento ya descargado (todavía sin ejecutar)

  function aviso(msg) {
    try {
      if (window.ilusToast) window.ilusToast(msg, { type: 'error' });
      else console.error(msg);
    } catch (e) {}
  }

  function descargar() {
    if (html !== null) return Promise.resolve(html);
    return fetch(URL_FRAGMENTO, { credentials: 'same-origin' }).then(function (r) {
      if (!r.ok) throw new Error('HTTP ' + r.status);
      return r.text();
    }).then(function (t) { html = t; return t; });
  }

  // Ejecuta los <script> del fragmento EN ORDEN (los externos esperan su carga): al insertar HTML con innerHTML no corren solos.
  function ejecutar(scripts, i, ok, fallo) {
    if (i >= scripts.length) { ok(); return; }
    var viejo = scripts[i], nuevo = document.createElement('script');
    Array.prototype.forEach.call(viejo.attributes, function (a) { nuevo.setAttribute(a.name, a.value); });
    if (viejo.src) {
      nuevo.async = false;
      nuevo.onload = function () { ejecutar(scripts, i + 1, ok, fallo); };
      nuevo.onerror = function () { fallo(new Error('script ' + viejo.src)); };
      viejo.parentNode.replaceChild(nuevo, viejo);
    } else {
      nuevo.text = viejo.textContent;
      viejo.parentNode.replaceChild(nuevo, viejo);   // corre al insertarse
      ejecutar(scripts, i + 1, ok, fallo);
    }
  }

  function montar() {
    if (promesa) return promesa;
    promesa = descargar().then(function (t) {
      return new Promise(function (ok, fallo) {
        var caja = document.createElement('div');
        caja.id = 'ot2-modal-crear-raiz';
        caja.innerHTML = t;
        var scripts = Array.prototype.slice.call(caja.querySelectorAll('script'));
        // Quita el cascarón ANTES de ejecutar: el fragmento define el OT2C de verdad.
        var cascaron = window.OT2C;
        try { delete window.OT2C; } catch (e) { window.OT2C = undefined; }
        document.body.appendChild(caja);
        ejecutar(scripts, 0, function () {
          if (!window.OT2C || !window.OT2C.abrir) { window.OT2C = cascaron; fallo(new Error('OT2C no quedó definido')); return; }
          window.OT2C.__real = true;
          ok(window.OT2C);
        }, function (e) { window.OT2C = cascaron; fallo(e); });
      });
    }).catch(function (e) {
      promesa = null;      // permite reintentar
      html = null;
      aviso('No se pudo cargar el formulario de OT. Revisa tu conexión e intenta de nuevo.');
      throw e;
    });
    return promesa;
  }

  var cascaron = { __real: false, __precargar: function () { return descargar().catch(function () {}); } };
  METODOS.forEach(function (m) {
    cascaron[m] = function () {
      var args = arguments;
      if (m === 'cerrar' && !promesa) return;                       // nada abierto: nada que cerrar
      var btn = document.activeElement && document.activeElement.tagName === 'BUTTON' ? document.activeElement : null;
      if (btn) btn.setAttribute('aria-busy', 'true');
      return montar().then(function (real) {
        if (btn) btn.removeAttribute('aria-busy');
        return real[m].apply(real, args);
      }, function () { if (btn) btn.removeAttribute('aria-busy'); });
    };
  });
  window.OT2C = cascaron;

  // 1) Momento ocioso: solo descarga al caché (no ejecuta 765 KB de JS mientras la persona está leyendo la página).
  function ocioso() {
    var go = function () { cascaron.__precargar(); };
    if ('requestIdleCallback' in window) window.requestIdleCallback(go, { timeout: 6000 });
    else setTimeout(go, 3000);
  }
  if (document.readyState === 'complete') ocioso();
  else window.addEventListener('load', ocioso, { once: true });

  // 2) Intención: mouse o dedo cerca de un botón que abre el asistente → se monta antes del clic.
  function intencion(ev) {
    var el = ev.target && ev.target.closest ? ev.target.closest('[onclick*="OT2C."]') : null;
    if (el && !promesa) montar().catch(function () {});
  }
  document.addEventListener('pointerover', intencion, { passive: true });
  document.addEventListener('touchstart', intencion, { passive: true });
  document.addEventListener('focusin', intencion);

  // 3) ?crear=1 → el propio asistente abre lo que corresponde al terminar de cargar (ver _o2mInicio).
  try {
    if (new URLSearchParams(location.search).get('crear') === '1') montar().catch(function () {});
  } catch (e) {}
})();

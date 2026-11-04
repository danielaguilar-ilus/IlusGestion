/* ⏱️ Sesiones de trabajo por equipo en la OT (Daniel, 2026-11-04).
   «Si el técnico selecciona otro equipo debe contraer el equipo seleccionado y pausar sus tiempos para que continúe con el otro,
   reconociendo si hay otro técnico trabajando un equipo.»
   · ACORDEÓN: abrir un equipo (con el dedo o con «Siguiente equipo») contrae el que estaba abierto. Antes solo el botón lo hacía.
   · PAUSA: cada equipo abierto es una sesión en el servidor (POST /ot/api/<id>/equipo-trabajo). Abrir otro, contraer, ocultar la
     pantalla o salir de la OT la pausa; mientras sigue abierto y visible manda un latido cada 30 s. Si el teléfono se duerme, el
     servidor corta el tramo en el último latido: esa pausa no se cuenta como trabajo.
   · AVISO: si OTRO técnico tiene un equipo abierto, la tarjeta dice «Fulano está trabajando aquí» (ámbar). Nunca bloquea.
   Solo registra el técnico con la OT en ejecución (window.OTD_EQS_CFG.puede); la gestión que mira la ficha no suma ni aparece.
   Todo es best-effort: si el servidor no responde, el técnico sigue trabajando igual. Reglas de tiempo: ot_equipo_sesiones.py. */
(function () {
  'use strict';
  var CFG = window.OTD_EQS_CFG || {};
  if (!CFG.vid) return;
  var URL = '/ot/api/' + CFG.vid + '/equipo-trabajo';
  var LATIDO_MS = (CFG.latidoSeg || 30) * 1000;
  var abierta = null;        // clave (e<id> / i<id>) del equipo que ESTE técnico tiene abierto
  var pausadaOculta = false; // la pantalla se ocultó con un equipo abierto: al volver se reanuda
  var timer = null;

  function items() { return Array.prototype.slice.call(document.querySelectorAll('#otdWkCard .otd-wk-item')); }
  function esItem(el) { return !!(el && el.matches && el.matches('#otdWkCard .otd-wk-item')); }

  /* Pinta (o esconde) el aviso «Fulano está trabajando aquí» de cada tarjeta. Solo textContent: los nombres son datos, no HTML. */
  function pintar(otros) {
    otros = otros || {};
    Array.prototype.forEach.call(document.querySelectorAll('[data-pres]'), function (s) {
      var o = otros[s.getAttribute('data-pres')];
      if (o && o.texto) { s.textContent = o.texto; s.hidden = false; }
      else { s.textContent = ''; s.hidden = true; }
    });
  }

  function recibir(j) { if (j && j.otros) pintar(j.otros); return j; }

  function enviar(accion, clave, motivo) {
    if (!CFG.puede) return Promise.resolve(null);
    try {
      return fetch(URL, {
        method: 'POST', credentials: 'same-origin', keepalive: true,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ accion: accion, clave: clave || '', motivo: motivo || '' })
      }).then(function (r) { return r.ok ? r.json() : null; }).then(recibir).catch(function () { return null; });
    } catch (e) { return Promise.resolve(null); }
  }

  function leer() {
    try {
      return fetch(URL, { credentials: 'same-origin', cache: 'no-store' })
        .then(function (r) { return r.ok ? r.json() : null; }).then(recibir).catch(function () { return null; });
    } catch (e) { return Promise.resolve(null); }
  }

  function tic() {
    if (document.hidden) return;   // en segundo plano no se manda nada: gasta batería y la sesión ya quedó pausada
    if (abierta) enviar('latido', abierta); else leer();
  }

  function arrancar() {
    if (timer || !CFG.puede) return;
    timer = setInterval(tic, LATIDO_MS);
  }

  /* Acordeón + sesión. El evento «toggle» no burbujea, por eso se escucha en captura sobre el documento. */
  function alAbrir(el) {
    items().forEach(function (o) { if (o !== el && o.open) o.open = false; });
    if (el.dataset.prog) { delete el.dataset.prog; }   // lo abrió «Siguiente equipo», que ya se encarga de llevar la pantalla
    else if (el.querySelector) {
      var cab = el.querySelector('summary') || el;
      if (cab.scrollIntoView) cab.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }
    var clave = el.getAttribute('data-clave');
    if (!clave || !CFG.puede) return;
    abierta = clave;
    pausadaOculta = false;
    enviar('iniciar', clave);
    arrancar();
  }

  function alCerrar(el) {
    var clave = el.getAttribute('data-clave');
    // Si lo cierra porque abrió OTRO, `abierta` ya apunta al nuevo y no se pausa: el servidor pausa el anterior al iniciar el nuevo.
    if (clave && CFG.puede && abierta === clave) {
      abierta = null;
      enviar('pausar', clave, 'contrajo');
    }
  }

  document.addEventListener('toggle', function (ev) {
    var el = ev.target;
    if (!esItem(el)) return;
    if (el.open) alAbrir(el); else alCerrar(el);
  }, true);

  document.addEventListener('visibilitychange', function () {
    if (!CFG.puede || !abierta) return;
    if (document.hidden) { pausadaOculta = true; enviar('pausar', abierta, 'salio'); }
    else if (pausadaOculta) { pausadaOculta = false; enviar('iniciar', abierta); }
  });

  window.addEventListener('pagehide', function () {
    if (CFG.puede && abierta) enviar('pausar', abierta, 'salio');
  });

  function inicio() {
    if (!CFG.puede) return;
    leer();       // pinta de entrada quién más está trabajando
    arrancar();   // y lo mantiene al día cada 30 s, también sin equipo abierto
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', inicio); else inicio();

  /* Para las pruebas y para que «Siguiente equipo» pueda avisar que abrió por programa. */
  window.OTD_EQS = {
    abierta: function () { return abierta; },
    pintar: pintar
  };
})();

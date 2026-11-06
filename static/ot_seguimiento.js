/* ILUS Fitness · Seguimiento en vivo de la OT para el cliente (REGLA #67, 2026-11-06).
   La página ya viene dibujada por el servidor; este archivo solo la REFRESCA con /estado cada 30 s mientras la pestaña está
   visible (sin hilos ni conexiones largas: Cloud Run) y anima lo que cambia: la línea se llena, el paso actual pulsa y el ✓
   aparece al completarse. Nada de alert/confirm/prompt (REGLA #1). Sin librerías. */
(function () {
  'use strict';
  var CADA_MS = 30000;
  var url = location.pathname.replace(/\/+$/, '') + '/estado';
  var ultimo = null;

  function $(id) { return document.getElementById(id); }

  function texto(id, valor) {
    var e = $(id);
    if (e && e.textContent !== String(valor)) e.textContent = String(valor);
  }

  function mostrar(el, si) {
    if (!el) return;
    if (si) el.removeAttribute('hidden'); else el.setAttribute('hidden', '');
  }

  function clase(el, prefijo, valor) {
    if (!el) return;
    var nuevas = [];
    el.className.split(/\s+/).forEach(function (c) { if (c && c.indexOf(prefijo) !== 0) nuevas.push(c); });
    nuevas.push(prefijo + valor);
    el.className = nuevas.join(' ');
  }

  function lista(ul, items) {
    if (!ul) return;
    while (ul.firstChild) ul.removeChild(ul.firstChild);
    (items || []).forEach(function (t) {
      var li = document.createElement('li');
      li.textContent = t;                      // siempre texto: nunca HTML de la respuesta
      ul.appendChild(li);
    });
  }

  function pintar(s) {
    if (!s) return;
    document.documentElement.setAttribute('data-tono', s.tono || 'gris');
    texto('segNum', s.numero);
    texto('segTipo', s.tipo);
    texto('segDia', s.dia_txt || 'Fecha por confirmar');
    texto('segHora', s.horario_txt);
    if (s.cliente) texto('segCli', s.cliente);

    var estado = $('segEstado');
    clase(estado, 'tono-', s.tono || 'gris');
    var uso = $('segIcoUse');
    if (uso) {
      var ref = '#ico-' + (s.icono || 'calendario');
      if (uso.getAttribute('href') !== ref) { uso.setAttribute('href', ref); uso.setAttribute('xlink:href', ref); }
    }
    texto('segTitulo', s.titulo);
    texto('segMensaje', s.mensaje);
    mostrar($('segSigue'), !!s.que_sigue);
    texto('segSigueTxt', s.que_sigue || '');

    var steps = $('segSteps');
    if (steps) steps.classList.toggle('is-terminado', s.situacion === 'terminada');
    var fill = $('segFill');
    if (fill) fill.style.width = (s.linea || 0) + '%';
    (s.pasos || []).forEach(function (p) {
      var li = document.querySelector('.seg-paso[data-clave="' + p.clave + '"]');
      if (!li) return;
      var antes = li.className;
      clase(li, 'is-', p.estado);
      var det = li.querySelector('.seg-paso-det');
      if (det && det.textContent !== (p.detalle || '')) det.textContent = p.detalle || '';
      var sr = li.querySelector('.seg-sr');
      if (sr) sr.textContent = p.estado === 'hecho' ? '(listo)' : (p.estado === 'actual' ? '(paso actual)' : '');
      if (antes.indexOf('is-hecho') < 0 && p.estado === 'hecho') {
        // acaba de completarse: el ✓ vuelve a entrar con su pop
        var ok = li.querySelector('.seg-circ-ok');
        if (ok) { ok.style.animation = 'none'; void ok.offsetWidth; ok.style.animation = ''; }
      }
    });

    var firmas = $('segFirmas');
    mostrar(firmas, !!(s.firmas && s.firmas.length));
    (s.firmas || []).forEach(function (f) {
      var chip = document.querySelector('.seg-firma[data-clave="' + f.clave + '"]');
      if (chip) chip.classList.toggle('ok', !!f.ok);
    });

    texto('segTec', (s.tecnicos && s.tecnicos.length) ? s.tecnicos.join(', ') : 'Por confirmar');
    texto('segEq', s.equipos_txt || '—');
    texto('segFecha', s.fecha_txt || 'Por confirmar');
    texto('segHorario', s.horario_txt || 'por confirmar');
    var det = $('segDet');
    mostrar(det, !!(s.equipos && s.equipos.length));
    if (!ultimo || JSON.stringify(ultimo.equipos || []) !== JSON.stringify(s.equipos || [])) lista($('segEqLista'), s.equipos);

    var btn = $('segBtnDoc');
    if (btn) {
      var link = s.link_firmada || '';
      mostrar(btn, !!link);
      if (link && btn.getAttribute('href') !== link) btn.setAttribute('href', link);
    }
    texto('segAct', s.actualizado || '');
    ultimo = s;
  }

  function enVivo(ok) {
    var live = $('segLive');
    if (!live) return;
    live.classList.toggle('is-off', !ok);
    live.setAttribute('title', ok ? 'Esta página se actualiza sola cada 30 segundos' : 'Sin conexión por ahora: se reintenta sola');
  }

  function cargar() {
    if (document.visibilityState !== 'visible') return;
    if (typeof fetch !== 'function') return;
    fetch(url, { headers: { Accept: 'application/json' }, cache: 'no-store', credentials: 'same-origin' })
      .then(function (r) {
        if (r.status === 404) { location.reload(); return null; }     // el enlace venció: la página lo dice con calma
        if (!r.ok) throw new Error('estado ' + r.status);
        return r.json();
      })
      .then(function (s) { if (s && s.ok) { pintar(s); enVivo(true); } })
      .catch(function () { enVivo(false); });
  }

  // Arranque: la línea de progreso entra desde 0 hasta donde va el servicio.
  var fill0 = $('segFill');
  if (fill0) {
    var ancho = fill0.style.width;
    fill0.style.width = '0%';
    requestAnimationFrame(function () { requestAnimationFrame(function () { fill0.style.width = ancho; }); });
  }
  setInterval(cargar, CADA_MS);
  document.addEventListener('visibilitychange', function () { if (document.visibilityState === 'visible') cargar(); });

  window.ILUS_SEG = { pintar: pintar, cargar: cargar, url: url };   // para las pruebas en navegador
})();

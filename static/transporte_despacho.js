/* ══════════════════════════════════════════════════════════════════════
   Pantalla "Despacho al courier" — pasos numerados con semáforo.
   2026-08-22.

   Dos pasadas a propósito:
     1) live=0 → pinta al instante con lo que ya está en MySQL. El paso 5
        queda "sin comprobar" (honesto: todavía no le preguntamos al courier).
     2) live=1 → consulta las visitas al courier y resuelve el paso 5.

   REGLA DE ORO: nada se pinta verde sin un dato que lo demuestre. Cuando no
   se pudo comprobar, se dice "sin comprobar" -- nunca verde, nunca silencio.

   Fechas: el backend manda SIEMPRE el par (iso, iso_label). Acá solo se
   imprime el _label ya formateado en hora Chile (REGLA #6). No se construye
   ningún Date() en el navegador: new Date('2026-08-18') se interpreta como
   UTC y en Chile puede mostrar el día anterior.
   ══════════════════════════════════════════════════════════════════════ */
(function () {
  'use strict';

  var page = document.querySelector('.dsp-page');
  if (!page) return;
  var MID = page.getAttribute('data-mid');

  // Estado -> clase CSS. Un único diccionario para que el riel, la tarjeta
  // de paso y los puntitos del pedido no puedan desincronizarse nunca.
  var CLASE = {
    ok:            'is-ok',
    esperando:     'is-wait',
    trabado:       'is-fail',
    ahora:         'is-now',
    sin_comprobar: 'is-unknown',
    pendiente:     'is-todo'
  };

  var GLIFO = {
    ok:            '✓',   // ✓
    esperando:     '●',   // ●
    trabado:       '✕',   // ✕
    ahora:         '',         // lleva el número del paso
    sin_comprobar: '?',
    pendiente:     ''
  };

  var ICONO_VEREDICTO = {
    ok:            'bi-check-circle-fill',
    esperando:     'bi-clock-fill',
    trabado:       'bi-x-circle-fill',
    ahora:         'bi-exclamation-circle-fill',
    sin_comprobar: 'bi-question-circle-fill',
    pendiente:     'bi-dash-circle'
  };

  var ultimo = null;
  var filtro = null;

  function cls(estado) { return CLASE[estado] || 'is-todo'; }

  // Escapar SIEMPRE lo que viene del servidor: nombres de cliente y mensajes
  // de error del courier son texto libre y pueden traer < o &.
  function esc(s) {
    return String(s == null ? '' : s)
      .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
  }

  function pedir(live) {
    var btn = document.getElementById('dspBtnComprobar');
    if (live && btn) {
      btn.disabled = true;
      btn.innerHTML = '<i class="bi bi-hourglass-split"></i> Comprobando…';
    }
    return fetch('/transporte/api/manifiestos/' + MID + '/despacho/estado?live=' + (live ? '1' : '0'), {
      headers: { 'Accept': 'application/json' }
    })
      .then(function (r) { return r.json(); })
      .then(function (d) {
        if (!d || !d.ok) throw new Error((d && d.error) || 'No se pudo leer el estado.');
        ultimo = d;
        pintar(d);
        return d;
      })
      .catch(function (e) {
        var v = document.getElementById('dspVerdict');
        if (v) {
          v.className = 'dsp-verdict is-unknown';
          v.innerHTML = '<i class="bi bi-question-circle-fill"></i> <span>No se pudo leer el estado del despacho. ' +
                        esc(e.message || '') + '</span>';
        }
      })
      .then(function () {
        if (btn) {
          btn.disabled = false;
          btn.innerHTML = '<i class="bi bi-arrow-clockwise"></i> Comprobar ahora';
        }
      });
  }

  function pintar(d) {
    pintarSubtitulo(d);
    pintarSemaforo(d);
    pintarRail(d);
    pintarPasos(d);
    pintarChips(d);
    pintarPedidos(d);
  }

  function pintarSubtitulo(d) {
    var el = document.getElementById('dspSubtitulo');
    if (!el) return;
    var m = d.manifiesto || {};
    el.textContent = [
      m.correlativo,
      (d.courier && d.courier.nombre) || '—',
      m.fecha_label,
      (m.n_items || 0) + ' factura' + (m.n_items === 1 ? '' : 's'),
      (m.n_bultos || 0) + ' bulto' + (m.n_bultos === 1 ? '' : 's')
    ].filter(Boolean).join(' · ');
  }

  function pintarSemaforo(d) {
    var r = d.resumen || {};
    var cont = document.getElementById('dspCounts');
    if (cont) {
      cont.innerHTML =
        chip('is-ok', 'bi-check-circle-fill', r.ok || 0, 'listas') +
        chip('is-wait', 'bi-clock-fill', r.esperando || 0, 'esperando') +
        chip('is-fail', 'bi-x-circle-fill', r.trabados || 0, 'trabadas') +
        chip('is-unknown', 'bi-question-circle-fill', r.sin_comprobar || 0, 'sin comprobar');
    }

    var total = (d.pedidos || []).length;
    var bar = document.getElementById('dspBar');
    if (bar) {
      if (!total) {
        bar.innerHTML = '<span class="seg-todo" style="width:100%"></span>';
      } else {
        var pend = r.pendientes || 0;
        bar.innerHTML =
          seg('seg-ok', r.ok || 0, total) +
          seg('seg-wait', r.esperando || 0, total) +
          seg('seg-fail', r.trabados || 0, total) +
          seg('seg-unknown', r.sin_comprobar || 0, total) +
          seg('seg-todo', pend, total);
      }
    }

    var ver = d.veredicto || {};
    var v = document.getElementById('dspVerdict');
    if (v) {
      v.className = 'dsp-verdict ' + cls(ver.estado);
      v.innerHTML = '<i class="bi ' + (ICONO_VEREDICTO[ver.estado] || 'bi-dash-circle') + '"></i> ' +
                    '<span>' + esc(ver.texto || '') + '</span>';
    }
  }

  function chip(k, ico, n, txt) {
    return '<span class="dsp-count ' + k + '"><i class="bi ' + ico + '"></i> <b>' + n + '</b> ' + txt + '</span>';
  }

  function seg(k, n, total) {
    if (!n) return '';
    return '<span class="' + k + '" style="width:' + ((n / total) * 100) + '%"></span>';
  }

  function pintarRail(d) {
    var el = document.getElementById('dspRail');
    if (!el) return;
    el.innerHTML = (d.pasos || []).map(function (p) {
      var c = cls(p.estado);
      var glifo = p.estado === 'ahora' || p.estado === 'pendiente'
        ? String(p.n)
        : (GLIFO[p.estado] || String(p.n));
      // Contador "4 de 7" solo si el paso lo declara en su badge.
      return '' +
        '<div class="dsp-rail-step">' +
          '<div class="dsp-rail-num">Paso ' + p.n + '</div>' +
          '<div class="dsp-circle ' + c + '" title="' + esc(p.titulo) + '">' + glifo + '</div>' +
          '<div class="dsp-rail-name">' + esc(p.titulo) + '</div>' +
          '<div class="dsp-rail-count">' + esc(p.badge || '') + '</div>' +
        '</div>';
    }).join('');
  }

  function pintarPasos(d) {
    var el = document.getElementById('dspSteps');
    if (!el) return;
    el.innerHTML = (d.pasos || []).map(function (p) {
      var c = cls(p.estado);
      var glifo = p.estado === 'ahora' || p.estado === 'pendiente'
        ? String(p.n)
        : (GLIFO[p.estado] || String(p.n));
      return '' +
        '<div class="dsp-step ' + c + '">' +
          '<div class="dsp-circle ' + c + '">' + glifo + '</div>' +
          '<div class="dsp-step-body">' +
            '<div class="dsp-step-top">' +
              '<span class="dsp-step-title">' + esc(p.titulo) + '</span>' +
              '<span class="dsp-badge ' + c + '">' + esc(p.badge || '') + '</span>' +
            '</div>' +
            '<div class="dsp-step-sub">' + esc(p.subtitulo || '') + '</div>' +
            '<div class="dsp-step-detail">' + esc(p.detalle || '') + '</div>' +
          '</div>' +
        '</div>';
    }).join('');
  }

  function pintarChips(d) {
    var el = document.getElementById('dspChips');
    if (!el) return;
    var r = d.resumen || {};
    var total = (d.pedidos || []).length;
    var defs = [
      { k: null,            t: 'Todos',         n: total },
      { k: 'trabado',       t: 'Trabados',      n: r.trabados || 0 },
      { k: 'esperando',     t: 'Esperando',     n: r.esperando || 0 },
      { k: 'ok',            t: 'Listos',        n: r.ok || 0 },
      { k: 'sin_comprobar', t: 'Sin comprobar', n: r.sin_comprobar || 0 }
    ];
    el.innerHTML = defs.map(function (c) {
      var act = (filtro === c.k) ? ' active' : '';
      return '<button type="button" class="dsp-chip' + act + '" data-f="' + (c.k || '') + '">' +
             esc(c.t) + ' (' + c.n + ')</button>';
    }).join('');

    Array.prototype.forEach.call(el.querySelectorAll('.dsp-chip'), function (b) {
      b.addEventListener('click', function () {
        var k = b.getAttribute('data-f') || null;
        // REGLA #4.3: volver a hacer clic en el chip activo LIMPIA el filtro
        // y re-renderiza la lista completa. Bug real que Daniel reportó el
        // 01-08: quitaba el filtro y la tabla se quedaba con el resultado
        // anterior.
        filtro = (filtro === k) ? null : k;
        pintarChips(ultimo);
        pintarPedidos(ultimo);
      });
    });
  }

  function pintarPedidos(d) {
    var el = document.getElementById('dspPedidos');
    if (!el) return;
    var lista = (d.pedidos || []).filter(function (p) {
      return !filtro || p.estado === filtro;
    });

    if (!lista.length) {
      el.innerHTML = '<p class="dsp-empty">' +
        (filtro ? 'Ningún pedido en este estado.' : 'Este manifiesto todavía no tiene facturas.') +
        '</p>';
      return;
    }

    el.innerHTML = lista.map(function (p) {
      var c = cls(p.estado);
      var dots = [1, 2, 3, 4, 5, 6].map(function (n) {
        var e = (p.pasos && p.pasos[String(n)]) || 'pendiente';
        return '<span class="dsp-dot ' + cls(e) + '" title="Paso ' + n + ': ' + esc(e) + '">' +
               (GLIFO[e] || '') + '</span>';
      }).join('');

      var visita = '';
      if (p.visita && p.visita.consultada_ok) {
        var v = p.visita;
        var partes = [];
        if (v.estado_courier) partes.push(esc(v.estado_courier));
        if (v.chofer) partes.push('chofer ' + esc(v.chofer));
        if (v.planned_date_label) partes.push('ruta del ' + esc(v.planned_date_label));
        if (partes.length) visita = '<div class="dsp-ped-det">' + partes.join(' · ') + '</div>';
      }

      return '' +
        '<div class="dsp-pedido ' + c + '">' +
          '<div class="dsp-ped-main">' +
            '<div class="dsp-ped-doc">' + esc(p.doc) + '</div>' +
            '<div class="dsp-ped-cli">' + esc(p.cliente) + ' · ' + esc(p.comuna) + '</div>' +
            '<div class="dsp-ped-est ' + c + '">' + esc(p.titular || '') + '</div>' +
            '<div class="dsp-ped-det">' + esc(p.detalle || '') + '</div>' +
            visita +
          '</div>' +
          '<div class="dsp-dots" title="Los 6 pasos de este pedido">' + dots + '</div>' +
        '</div>';
    }).join('');
  }

  var btn = document.getElementById('dspBtnComprobar');
  if (btn) btn.addEventListener('click', function () { pedir(true); });

  // Primer pintado rápido (MySQL) y, apenas está, la comprobación en vivo.
  pedir(false).then(function (d) {
    if (d && d.courier && d.courier.token_ok) {
      setTimeout(function () { pedir(true); }, 300);
    }
  });
})();

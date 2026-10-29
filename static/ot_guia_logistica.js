/* 🚚 Logística (WMS) de cada guía de despacho dentro de «Trazabilidad en Random» (Daniel, 29-oct-2026).

   «En la guía de despacho, presiono y no hicimos la conexión con el WMS, que es la parte logística. Me gustaría verlo, para
   saber QUÉ SE FUE. Y hacer una comparación de qué se fue en la guía con los productos que se gestionaron en la OT. Eso es
   inteligencia y percepción. Y levantar una alerta, obviamente.» Y después: «quiero a Check en este reporte… todo bien
   resumido, no tarjetas tan grandes».

   static/ot_doc_cadena.js la monta con OTGuiaLogistica.montar(sec, vid). Tres lecturas de GET /ot/api/<vid>/documentos/logistica:
     1) sin parámetros (rápida, solo Random): el chip «N diferencias con la OT» de la cabecera y un semáforo provisorio;
     2) ?check=1: Check (bodega) por cada guía → UNA línea por guía con un semáforo chico
        (verde = salió todo lo gestionado · ámbar = falta o sobra algo · gris = sin información de bodega);
     3) ?guia=<número>, al hacer clic en «Ver logística»: el detalle (qué informa Check, línea por línea contra la OT, y los
        movimientos de bodega: OT de Check, operador, UA).
   Colores con medida: semáforo y chips, nunca la tarjeta entera pintada. Nombres completos (REGLA #15), celular primero
   (REGLA #3), sin alert/confirm/prompt (REGLA #1), sin montos. El servidor nunca se lo entrega a un técnico (REGLAS
   #19/#26). Todo el texto se escapa. */
(function (global) {
  'use strict';

  var resumenes = {};   // vid → última respuesta con la comparación (null si falló)
  var semaforos = {};   // vid → {num: semáforo} (lo definitivo no lo pisa uno provisorio)
  var pidiendo = {};    // url → promesa en curso
  var detalles = {};    // 'vid|num' → respuesta con el detalle de esa guía

  function has(o, k) { return Object.prototype.hasOwnProperty.call(o, k); }
  function esc(s) {
    return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
    });
  }
  function cant(x) {
    var n = Number(x) || 0;
    return (Math.round(n * 100) / 100).toString().replace('.', ',');
  }
  function plural(n, uno, varios) { return Math.abs((Number(n) || 0) - 1) < 0.001 ? uno : varios; }

  function url(vid, q) { return '/ot/api/' + vid + '/documentos/logistica' + (q ? '?' + q : ''); }
  function pedir(u) {
    if (pidiendo[u]) return pidiendo[u];
    pidiendo[u] = fetch(u, { credentials: 'same-origin', headers: { 'Accept': 'application/json' } })
      .then(function (r) { return r.ok ? r.json() : null; })
      .catch(function () { return null; })
      .then(function (j) { pidiendo[u] = null; return (j && j.ok) ? j : null; });
    return pidiendo[u];
  }
  function guardar(vid, j) {
    if (!j) return;
    resumenes[vid] = j;
    var s = semaforos[vid] || (semaforos[vid] = {});
    var nuevos = j.semaforos || {};
    Object.keys(nuevos).forEach(function (n) {
      if (!nuevos[n]) return;
      if (nuevos[n].pendiente && s[n] && !s[n].pendiente) return;      // lo provisorio no pisa lo que ya dijo Check
      s[n] = nuevos[n];
    });
  }
  function cargarResumen(vid) {
    if (has(resumenes, vid)) return Promise.resolve(resumenes[vid]);
    return pedir(url(vid)).then(function (j) { if (j) guardar(vid, j); else resumenes[vid] = null; return j; });
  }
  function cargarCheck(vid) {
    return pedir(url(vid, 'check=1')).then(function (j) { guardar(vid, j); return j; });
  }
  function cargarGuia(vid, num, forzar) {
    var k = vid + '|' + num;
    if (!forzar && detalles[k]) return Promise.resolve(detalles[k]);
    return pedir(url(vid, 'guia=' + encodeURIComponent(num))).then(function (j) {
      if (j) { detalles[k] = j; guardar(vid, j); }
      return j;
    });
  }

  /* ── Pintado (funciones puras: devuelven HTML) ─────────────────────────── */
  var COLOR_TXT = { verde: 'verde', ambar: 'ámbar', gris: 'gris' };

  function semaforoDe(vid, num) {
    var s = (semaforos[vid] || {})[num];
    if (s) return s;
    var comp = (resumenes[vid] || {}).comparacion;
    if (!comp) return null;
    return { color: 'gris', texto: comp.estado === 'sin_lectura' ? 'Random no respondió' : 'Sin datos para comparar con la OT' };
  }

  function dot(color) {
    var c = COLOR_TXT[color] ? color : 'gris';
    return '<span class="gl-dot ' + c + '" aria-hidden="true"></span><span class="gl-sr">Semáforo ' + COLOR_TXT[c] + ': </span>';
  }

  function htmlSemaforo(s) {
    if (!s) return '';
    return '<span class="gl-sem' + (s.pendiente ? ' pend' : '') + '">' + dot(s.color) + '<span class="gl-sem-t">' + esc(s.texto) + '</span></span>';
  }

  function htmlChip(comp) {
    if (!comp) return '';
    var n = Number(comp.n_diferencias) || 0;
    if (comp.estado === 'diferencias' && n > 0) {
      return '<button type="button" class="gl-chip ambar" data-gl-ir aria-label="' + esc(n + plural(n, ' diferencia', ' diferencias') +
        ' entre lo que salió en la guía y lo que gestiona la OT. Abrir el detalle') + '"><i class="bi bi-exclamation-triangle-fill" aria-hidden="true"></i> ' +
        n + plural(n, ' diferencia', ' diferencias') + ' con la OT</button>';
    }
    if (comp.estado === 'coincide') {
      return '<span class="gl-chip verde"><i class="bi bi-check2" aria-hidden="true"></i> Lo que salió coincide con la OT</span>';
    }
    return '';
  }

  var ETAPA_TXT = { pedido: 'Pedido', pickeado: 'Juntado', despachado: 'Expedido' };

  function htmlBodega(bod) {
    if (!bod) return '';
    var h = '<div class="gl-sec"><h5><i class="bi bi-box-seam" aria-hidden="true"></i> Bodega (Check)</h5>' +
      '<p class="gl-fr">' + dot(bod.color) + esc(bod.texto) + '</p>';
    if (bod.estado === 'ok') {
      var donde = bod.fuente_es_guia ? 'esta guía' : (esc(bod.fuente) + ' (el documento de la venta)');
      var etapas = (bod.etapas || []).map(function (e) {
        return esc(ETAPA_TXT[e.clave] || e.titulo) + ' ' + esc(cant(e.hechas)) + '/' + esc(cant(e.de));
      });
      h += '<p class="gl-meta">Según Check: ' + donde + (bod.emision ? ' · emitido ' + esc(bod.emision) : '') +
        (bod.entrega ? ' · entrega ' + esc(bod.entrega) : '') + (etapas.length ? ' · ' + etapas.join(' · ') : '') + '</p>';
    }
    return h + '</div>';
  }

  function textoBodegaLinea(b, bod) {
    if (!bod || bod.estado !== 'ok') return '';
    if (!b) return 'bodega: Check no informa este producto';
    if (b.despachadas > 0) return 'bodega expidió ' + cant(b.despachadas) + ' de ' + cant(b.pedidas);
    if (b.pickeadas > 0) return 'bodega juntó ' + cant(b.pickeadas) + ' de ' + cant(b.pedidas) + ', sin expedir';
    return 'bodega: todavía sin preparar';
  }

  function colorLinea(l) {
    if (l.es_diferencia) return 'ambar';
    return l.estado === 'coincide' ? 'verde' : 'gris';
  }

  function htmlLinea(l, bod) {
    var tb = textoBodegaLinea(l.bodega, bod);
    var detalle = [esc(l.estado_txt || '')];
    if (tb) detalle.push(esc(tb));
    return '<li>' + dot(colorLinea(l)) + '<span class="gl-l"><span class="gl-q">' + esc(cant(l.cantidad != null ? l.cantidad : l.gestiona)) +
      ' ×</span> ' + esc(l.nombre || l.sku || 'Producto') + (l.sku && l.nombre ? ' <small>' + esc(l.sku) + '</small>' : '') +
      '<span class="gl-meta"> — ' + detalle.join(' · ') + '</span>' +
      ((l.es_diferencia || (l.estado !== 'coincide' && l.texto)) && l.texto ? '<span class="gl-meta gl-l-x">' + esc(l.texto) + '</span>' : '') +
      '</span></li>';
  }

  function htmlComparacion(comp, num, bod) {
    if (!comp) return '';
    var pg = (comp.por_guia || {})[num] || {};
    var lineas = (bod && bod.guia_lineas) || pg.lineas || [];
    var h = '<div class="gl-sec"><h5><i class="bi bi-arrow-left-right" aria-hidden="true"></i> Guía vs. OT</h5>' +
      '<p class="gl-fr">' + esc(comp.veredicto || '') + '</p>';
    if (lineas.length) h += '<ul class="gl-lineas" aria-label="Productos de esta guía">' + lineas.map(function (l) { return htmlLinea(l, bod); }).join('') + '</ul>';
    var noSalio = (comp.no_salio || []);
    if (noSalio.length) {
      h += '<p class="gl-sub">La OT gestiona y no salió en ninguna guía:</p><ul class="gl-lineas">' + noSalio.map(function (p) {
        return htmlLinea({ cantidad: p.gestiona, nombre: p.nombre, sku: p.sku, estado: p.estado, estado_txt: p.estado_txt,
          es_diferencia: p.es_diferencia, texto: p.texto }, null);
      }).join('') + '</ul>';
    }
    var notas = [];
    if ((comp.equipos_del_cliente || []).length) {
      var ne = comp.equipos_del_cliente.length;
      notas.push(ne + plural(ne, ' equipo es del cliente', ' equipos son del cliente') + ': no se espera que salgan en una guía (solo cuentan sus repuestos)');
    }
    if ((comp.sin_sku || []).length) notas.push('sin SKU, no se comparan: ' + comp.sin_sku.map(function (x) { return x.nombre || 'producto'; }).join(', '));
    if ((comp.anuladas || []).length) notas.push('guías anuladas en Random (no cuentan): ' + comp.anuladas.map(function (n) { return 'GDV ' + n; }).join(', '));
    if (notas.length) h += '<p class="gl-meta">' + esc(notas.join(' · ')) + '.</p>';
    return h + '</div>';
  }

  function htmlMovimientos(bod) {
    if (!bod) return '';
    var m = bod.movimientos || {};
    var h = '<div class="gl-sec"><h5><i class="bi bi-person-badge" aria-hidden="true"></i> Movimientos en bodega</h5>';
    if (m.estado !== 'ok') return h + '<p class="gl-meta">' + esc(m.texto || 'Sin movimientos de bodega para mostrar.') + '</p></div>';
    h += '<ul class="gl-mov">' + (m.ots || []).map(function (o) {
      var partes = [];
      var cuando = (o.inicio || '') + (o.fin ? ' → ' + o.fin : '');
      if (cuando) partes.push(esc(cuando));
      if ((o.personas || []).length) partes.push(esc(o.personas.join(', ')));
      if ((o.uas || []).length) partes.push('UA ' + esc(o.uas.join(', ')));
      return '<li><b>OT ' + esc(o.ot || 's/n') + '</b>' + (o.tipo ? ' · ' + esc(o.tipo) : '') + (o.estado ? ' · ' + esc(o.estado) : '') +
        ' <small>(' + esc(o.doc) + ')</small>' + (partes.length ? '<span class="gl-meta"> — ' + partes.join(' · ') + '</span>' : '') + '</li>';
    }).join('') + '</ul>';
    var pie = [];
    if (m.mas) pie.push('y ' + esc(m.mas) + ' movimientos más');
    if (m.edad_min != null) pie.push('reporte de Check leído hace ' + esc(m.edad_min) + ' min');
    if (pie.length) h += '<p class="gl-meta">' + pie.join(' · ') + '.</p>';
    return h + '</div>';
  }

  function htmlDetalle(r, num) {
    if (!r) {
      return '<div class="gl-err" role="note"><span>No pudimos leer la logística ahora. La ficha sigue igual.</span>' +
        '<button type="button" class="gl-btn" data-gl-reintentar="' + esc(num) + '"><i class="bi bi-arrow-clockwise" aria-hidden="true"></i> Reintentar</button></div>';
    }
    var bod = (r.bodega || {})[num] || null;
    return '<div class="gl-det">' + htmlBodega(bod) + htmlComparacion(r.comparacion, num, bod) + htmlMovimientos(bod) + '</div>';
  }

  /* ── Montaje y clics ─────────────────────────────────────────────────── */
  function cajas(sec) { return sec.querySelectorAll('[data-gl-guia]'); }
  function caja(sec, num) {
    var cs = cajas(sec);
    for (var i = 0; i < cs.length; i++) { if (cs[i].getAttribute('data-gl-guia') === String(num)) return cs[i]; }
    return null;
  }

  function pintar(sec, vid) {
    var d = resumenes[vid];
    var chip = sec.querySelector('[data-gl-chip]');
    if (chip) chip.innerHTML = htmlChip(d ? d.comparacion : null);
    var cs = cajas(sec);
    for (var i = 0; i < cs.length; i++) {
      var est = cs[i].querySelector('[data-gl-estado]');
      if (est) est.innerHTML = d ? htmlSemaforo(semaforoDe(vid, cs[i].getAttribute('data-gl-guia'))) : '';
    }
  }

  function hayPendientes(vid) {
    var s = semaforos[vid] || {};
    return Object.keys(s).some(function (n) { return s[n] && s[n].pendiente; });
  }

  function abrir(sec, vid, num, forzar) {
    var box = caja(sec, num);
    if (!box) return Promise.resolve(null);
    var btn = box.querySelector('[data-gl-ver]'), cuerpo = box.querySelector('[data-gl-cuerpo]');
    if (!btn || !cuerpo) return Promise.resolve(null);
    cuerpo.hidden = false;
    btn.setAttribute('aria-expanded', 'true');
    btn.innerHTML = '<i class="bi bi-chevron-up" aria-hidden="true"></i> Ocultar logística';
    // La guía abierta necesita el ancho completo: la cadena pasa a columna (la misma vista de 4+ documentos).
    var ol = box.closest ? box.closest('ol') : null;
    if (ol && !ol.classList.contains('largo')) { ol.classList.add('largo'); ol.setAttribute('data-gl-largo', '1'); }
    cuerpo.innerHTML = '<p class="gl-meta"><i class="bi bi-hourglass-split" aria-hidden="true"></i> Consultando la bodega (Check) y Random…</p>';
    return cargarGuia(vid, num, forzar).then(function (r) {
      cuerpo.innerHTML = htmlDetalle(r, num);
      pintar(sec, vid);
      return r;
    });
  }

  function cerrar(sec, num) {
    var box = caja(sec, num);
    if (!box) return;
    var btn = box.querySelector('[data-gl-ver]'), cuerpo = box.querySelector('[data-gl-cuerpo]');
    if (cuerpo) cuerpo.hidden = true;
    if (btn) {
      btn.setAttribute('aria-expanded', 'false');
      btn.innerHTML = '<i class="bi bi-truck" aria-hidden="true"></i> Ver logística';
    }
    var ol = box.closest ? box.closest('ol') : null;
    if (ol && ol.getAttribute('data-gl-largo') === '1' && !ol.querySelectorAll('[data-gl-ver][aria-expanded="true"]').length) {
      ol.classList.remove('largo');
      ol.removeAttribute('data-gl-largo');
    }
  }

  function primeraAmbar(sec, vid) {
    var cs = cajas(sec);
    for (var i = 0; i < cs.length; i++) {
      var num = cs[i].getAttribute('data-gl-guia');
      var s = semaforoDe(vid, num);
      if (s && s.color === 'ambar') return num;
    }
    return cs.length ? cs[0].getAttribute('data-gl-guia') : null;
  }

  function montar(sec, vid) {
    vid = parseInt(vid, 10);
    if (!sec || !vid || sec.__glMontado) return;
    if (!cajas(sec).length) return;                 // sin guías de despacho: nada que comparar
    sec.__glMontado = true;
    sec.addEventListener('click', function (e) {
      var t = e.target;
      if (!t || !t.closest) return;
      var b = t.closest('[data-gl-ver]');
      if (b && sec.contains(b)) {
        e.preventDefault();
        var num = b.getAttribute('data-gl-ver');
        if (b.getAttribute('aria-expanded') === 'true') cerrar(sec, num); else abrir(sec, vid, num);
        return;
      }
      var re = t.closest('[data-gl-reintentar]');
      if (re && sec.contains(re)) { e.preventDefault(); abrir(sec, vid, re.getAttribute('data-gl-reintentar'), true); return; }
      var ir = t.closest('[data-gl-ir]');
      if (ir && sec.contains(ir)) {
        e.preventDefault();
        var n = primeraAmbar(sec, vid);
        var box = n ? caja(sec, n) : null;
        var btn = box && box.querySelector('[data-gl-ver]');
        if (btn && btn.getAttribute('aria-expanded') !== 'true') abrir(sec, vid, n);
        if (box && box.scrollIntoView) box.scrollIntoView({ behavior: 'smooth', block: 'center' });
      }
    });
    cargarResumen(vid).then(function (d) {
      pintar(sec, vid);
      if (d && d.check && hayPendientes(vid)) return cargarCheck(vid).then(function () { pintar(sec, vid); });
    });
  }

  function iniciar() {
    var els = document.querySelectorAll('[data-fin-motor]');
    for (var i = 0; i < els.length; i++) {
      var hijos = els[i].children || [];
      for (var j = 0; j < hijos.length; j++) {
        if (hijos[j].classList && hijos[j].classList.contains('dc')) montar(hijos[j], els[i].getAttribute('data-vid'));
      }
    }
  }

  global.OTGuiaLogistica = {
    montar: montar, iniciar: iniciar, abrir: abrir, cerrar: cerrar,
    _htmlChip: htmlChip, _htmlSemaforo: htmlSemaforo, _htmlDetalle: htmlDetalle,
    _limpiar: function () { resumenes = {}; semaforos = {}; pidiendo = {}; detalles = {}; }
  };
  if (typeof document !== 'undefined') {
    if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', iniciar);
    else iniciar();
  }
})(window);

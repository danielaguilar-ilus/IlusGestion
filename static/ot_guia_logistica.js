/* 🚚 Logística (WMS) de cada guía de despacho dentro de «Trazabilidad en Random» (Daniel, 29-oct-2026).

   «En la guía de despacho, presiono y no hicimos la conexión con el WMS, que es la parte logística. Me gustaría verlo, para
   saber QUÉ SE FUE. Y hacer una comparación de qué se fue en la guía con los productos que se gestionaron en la OT. Eso es
   inteligencia y percepción. Y levantar una alerta, obviamente.» Y después: «quiero a Check en este reporte… todo bien
   resumido, no tarjetas tan grandes».
   2026-10-29 (tarde): «En la tarjeta de la guía de despacho quiero que sea PEQUEÑA… cuando le doy clic quiero que me diga
   TODOS LOS MOVIMIENTOS: qué se entregó y qué no se entregó, y si hay DIFERENCIA con la orden de trabajo». Y «es Check quien
   nos tiene que dar la ÚLTIMA PALABRA… algo POCO».

   static/ot_doc_cadena.js pinta cada venta como UNA fila de chips; cada guía es un chip con un punto de semáforo
   ([data-gl-dot]) y esta pieza lo maneja (OTGuiaLogistica.montar(sec, vid)). Tres lecturas de
   GET /ot/api/<vid>/documentos/logistica:
     1) sin parámetros (rápida, solo Random): el chip «N diferencias con la OT» de la cabecera y un semáforo provisorio;
     2) ?check=1: Check (bodega) por cada guía → el punto de cada guía (verde = salió todo lo gestionado y Check lo expidió ·
        ámbar = hay diferencia o falta expedir · gris = Check no tiene información) y una línea bajo la fila con lo ámbar;
     3) ?guia=<número>, al tocar el chip de la guía: el panel con las DIFERENCIAS CON LA OT arriba, «qué salió y qué no» por
        producto (OT · guía · Check), lo que informa Check y los movimientos de bodega (OT de Check, operador, UA).
   Colores con medida: el punto y los chips, nunca la tarjeta entera pintada. Nombres completos (REGLA #15), celular primero
   (REGLA #3), sin alert/confirm/prompt (REGLA #1), sin montos. El servidor nunca se lo entrega a un técnico (REGLAS
   #19/#26). Todo el texto se escapa. */
(function (global) {
  'use strict';

  var resumenes = {};   // vid → última respuesta con la comparación (null si falló)
  var semaforos = {};   // vid → {num: semáforo} (lo definitivo no lo pisa uno provisorio)
  var checkOT = {};     // vid → lo que Check agregó (reglas B y C) en la última lectura con Check
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
    if (j.check_ot) checkOT[vid] = j.check_ot;
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
  function col(c) { return COLOR_TXT[c] ? c : 'gris'; }

  function semaforoDe(vid, num) {
    var s = (semaforos[vid] || {})[num];
    if (s) return s;
    var comp = (resumenes[vid] || {}).comparacion;
    if (!comp) return null;
    return { color: 'gris', texto: comp.estado === 'sin_lectura' ? 'Random no respondió' : 'Sin datos para comparar con la OT' };
  }

  function dot(color) {
    var c = col(color);
    return '<span class="gl-dot ' + c + '" aria-hidden="true"></span><span class="gl-sr">Semáforo ' + COLOR_TXT[c] + ': </span>';
  }

  function htmlSemaforo(s) {
    if (!s) return '';
    return '<span class="gl-sem' + (s.pendiente ? ' pend' : '') + '">' + dot(s.color) + '<span class="gl-sem-t">' + esc(s.texto) + '</span></span>';
  }

  function nDiferencias(comp, cot) {
    var n = (comp && comp.estado === 'diferencias') ? (Number(comp.n_diferencias) || 0) : 0;
    return n + ((cot && cot.hallazgos) || []).length;
  }

  function htmlChip(comp, cot) {
    if (!comp) return '';
    var n = nDiferencias(comp, cot);
    if (n > 0) {
      return '<button type="button" class="gl-chip ambar" data-gl-ir aria-label="' + esc(n + plural(n, ' diferencia', ' diferencias') +
        ' entre la OT y lo que salió (guía de despacho y Check). Abrir el detalle') + '"><i class="bi bi-exclamation-triangle-fill" aria-hidden="true"></i> ' +
        n + plural(n, ' diferencia', ' diferencias') + ' con la OT</button>';
    }
    if (comp.estado === 'coincide') {
      return '<span class="gl-chip verde"><i class="bi bi-check2" aria-hidden="true"></i> Lo que salió coincide con la OT</span>';
    }
    return '';
  }

  var ETAPA_TXT = { pedido: 'Pedido', pickeado: 'Juntado', despachado: 'Expedido' };

  function htmlBodega(bod, chk) {
    if (!bod) return '';
    var color = (chk && chk.color) || bod.color, texto = (chk && chk.texto) ? chk.texto : bod.texto;
    texto = String(texto || '');
    var h = '<section class="gl-sec"><h5><i class="bi bi-box-seam" aria-hidden="true"></i> Bodega (Check)</h5>' +
      '<p class="gl-fr">' + dot(color) + esc(texto.charAt(0).toUpperCase() + texto.slice(1)) + '</p>';
    if (bod.estado === 'ok') {
      var donde = bod.fuente_es_guia ? 'esta guía' : (esc(bod.fuente) + ' (el documento de la venta)');
      var etapas = (bod.etapas || []).map(function (e) {
        return esc(ETAPA_TXT[e.clave] || e.titulo) + ' ' + esc(cant(e.hechas)) + '/' + esc(cant(e.de));
      });
      h += '<p class="gl-meta">Según Check: ' + donde + (bod.emision ? ' · emitido ' + esc(bod.emision) : '') +
        (bod.entrega ? ' · entrega ' + esc(bod.entrega) : '') + (etapas.length ? ' · ' + etapas.join(' · ') : '') + '</p>';
    }
    return h + '</section>';
  }

  /* Las DIFERENCIAS CON LA OT van arriba: es lo que le interesa a Daniel. */
  function htmlDiferencias(det, comp) {
    var difs = (det && det.diferencias) || [];
    if (difs.length) {
      return '<section class="gl-sec gl-difs" aria-label="Diferencias con la OT"><h5><i class="bi bi-exclamation-triangle-fill" aria-hidden="true"></i> ' +
        difs.length + plural(difs.length, ' diferencia', ' diferencias') + ' con la OT</h5><ul class="gl-lista">' +
        difs.map(function (d) { return '<li>' + dot('ambar') + '<span>' + esc(d.texto) + '</span></li>'; }).join('') + '</ul></section>';
    }
    if (comp && comp.estado === 'sin_datos') {
      return '<section class="gl-sec gl-difs"><p class="gl-fr">' + dot('gris') + esc(comp.veredicto || 'Sin datos para comparar con la OT.') + '</p></section>';
    }
    return '<section class="gl-sec gl-difs"><p class="gl-fr gl-ok">' + dot('verde') + 'Sin diferencias con la OT en esta guía.</p></section>';
  }

  function num0(x) { return x == null ? '—' : esc(cant(x)); }

  /* «Qué salió y qué no»: una fila por producto, con lo que dice cada banda (OT · guía · Check) y qué pasó en bodega. */
  function htmlProductos(det) {
    var filas = (det && det.productos) || [];
    if (!filas.length) return '';
    return '<section class="gl-sec gl-prods"><h5><i class="bi bi-list-check" aria-hidden="true"></i> Qué salió y qué no</h5>' +
      '<table class="gl-tab"><thead><tr><th scope="col">Producto</th><th scope="col">OT</th><th scope="col">Guía</th>' +
      '<th scope="col">Check</th><th scope="col">En bodega</th></tr></thead><tbody>' +
      filas.map(function (f) {
        var extra = (f.es_diferencia || (f.estado && f.estado !== 'coincide')) && f.texto ? '<span class="gl-meta gl-l-x">' + esc(f.texto) + '</span>' : '';
        return '<tr class="' + (f.en_guia ? '' : 'fuera') + '"><td class="gl-td-p">' + dot(f.color) + '<span class="gl-l">' + esc(f.nombre || f.sku || 'Producto') +
          (f.sku && f.nombre ? ' <small>' + esc(f.sku) + '</small>' : '') + extra + '</span></td>' +
          '<td data-l="OT">' + num0(f.ot) + '</td><td data-l="Guía">' + num0(f.guia) + '</td><td data-l="Check">' + num0(f.check) + '</td>' +
          '<td data-l="En bodega" class="gl-td-b ' + col(f.check_color) + '">' + esc(f.check_txt || '') + '</td></tr>';
      }).join('') + '</tbody></table></section>';
  }

  function htmlMovimientos(bod) {
    if (!bod) return '';
    var m = bod.movimientos || {};
    var h = '<section class="gl-sec"><h5><i class="bi bi-person-badge" aria-hidden="true"></i> Movimientos en bodega</h5>';
    if (m.estado !== 'ok') return h + '<p class="gl-meta">' + esc(m.texto || 'Sin movimientos de bodega para mostrar.') + '</p></section>';
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
    return h + '</section>';
  }

  function htmlNotas(comp) {
    if (!comp) return '';
    var notas = [];
    if ((comp.equipos_del_cliente || []).length) {
      var ne = comp.equipos_del_cliente.length;
      notas.push(ne + plural(ne, ' equipo es del cliente', ' equipos son del cliente') + ': no se espera que salgan en una guía (solo cuentan sus repuestos)');
    }
    if ((comp.sin_sku || []).length) notas.push('sin SKU, no se comparan: ' + comp.sin_sku.map(function (x) { return x.nombre || 'producto'; }).join(', '));
    if ((comp.anuladas || []).length) notas.push('guías anuladas en Random (no cuentan): ' + comp.anuladas.map(function (n) { return 'GDV ' + n; }).join(', '));
    return notas.length ? '<p class="gl-meta gl-notas">' + esc(notas.join(' · ')) + '.</p>' : '';
  }

  function cabecera(r, num) {
    var g = ((r && r.guias) || []).filter(function (x) { return String(x.numero) === String(num); })[0] || {};
    return '<p class="gl-cab"><b>Guía de despacho GDV ' + esc(num) + '</b>' + (g.fecha ? ' · ' + esc(g.fecha) : '') +
      (g.desde ? ' · salió desde ' + esc(g.desde) : '') + '</p>';
  }

  function htmlDetalle(r, num) {
    if (!r) {
      return '<div class="gl-err" role="note"><span>No pudimos leer la logística ahora. La ficha sigue igual.</span>' +
        '<button type="button" class="gl-btn" data-gl-reintentar="' + esc(num) + '"><i class="bi bi-arrow-clockwise" aria-hidden="true"></i> Reintentar</button></div>';
    }
    var bod = (r.bodega || {})[num] || null;
    var det = r.detalle || null;
    return '<div class="gl-det">' + cabecera(r, num) + htmlDiferencias(det, r.comparacion) + htmlProductos(det) +
      htmlBodega(bod, det && det.check) + htmlMovimientos(bod) + htmlNotas(r.comparacion) + '</div>';
  }

  /* ── Montaje y clics ─────────────────────────────────────────────────── */
  function chips(sec) { return sec.querySelectorAll('[data-gl-ver]'); }
  function chipDe(sec, num) {
    var cs = chips(sec);
    for (var i = 0; i < cs.length; i++) { if (cs[i].getAttribute('data-gl-ver') === String(num)) return cs[i]; }
    return null;
  }
  function ventaDe(chip) { return chip && chip.closest ? chip.closest('[data-dc-venta]') : null; }
  function panelDe(chip) { var v = ventaDe(chip); return v ? v.querySelector('[data-dc-panel]') : null; }
  function marcar(venta, chip) {
    var bs = venta.querySelectorAll('[aria-expanded]');
    for (var i = 0; i < bs.length; i++) bs[i].setAttribute('aria-expanded', bs[i] === chip ? 'true' : 'false');
  }

  function pintar(sec, vid) {
    var d = resumenes[vid];
    var chip = sec.querySelector('[data-gl-chip]');
    if (chip) chip.innerHTML = htmlChip(d ? d.comparacion : null, checkOT[vid]);
    var cs = chips(sec), lineas = {};
    for (var i = 0; i < cs.length; i++) {
      var num = cs[i].getAttribute('data-gl-ver');
      var s = d ? semaforoDe(vid, num) : null;
      var p = cs[i].querySelector('[data-gl-dot]');
      if (p) p.className = 'gl-dot ' + col(s && s.color) + ((s && s.pendiente) ? ' pend' : '');   // falló la lectura: gris quieto
      if (s) {
        var base = String(cs[i].getAttribute('title') || '').split(' · Semáforo')[0];
        var t = base + ' · Semáforo ' + COLOR_TXT[col(s.color)] + ': ' + s.texto;
        cs[i].setAttribute('title', t);
        cs[i].setAttribute('aria-label', t);
      }
      // La línea bajo la fila: solo lo que está en ámbar (lo verde y lo gris se ven en el punto y al tocar).
      var venta = ventaDe(cs[i]);
      var k = venta ? venta.getAttribute('data-dc-venta') : '';
      if (!lineas[k]) lineas[k] = { venta: venta, txt: [] };
      if (s && s.color === 'ambar') lineas[k].txt.push('GDV ' + num + ': ' + s.texto);
    }
    Object.keys(lineas).forEach(function (k) {
      var v = lineas[k].venta, res = v && v.querySelector('[data-gl-resumen]');
      if (!res) return;
      res.innerHTML = lineas[k].txt.map(function (t) { return '<span>' + dot('ambar') + esc(t) + '</span>'; }).join('');
      res.hidden = !lineas[k].txt.length;
    });
  }

  function hayPendientes(vid) {
    var s = semaforos[vid] || {};
    return Object.keys(s).some(function (n) { return s[n] && s[n].pendiente; });
  }

  function abrir(sec, vid, num, forzar) {
    var chip = chipDe(sec, num), panel = panelDe(chip);
    if (!chip || !panel) return Promise.resolve(null);
    marcar(ventaDe(chip), chip);
    panel.hidden = false;
    panel.setAttribute('data-gl-abierta', String(num));
    panel.innerHTML = cabecera(resumenes[vid], num) +
      '<p class="gl-meta"><i class="bi bi-hourglass-split" aria-hidden="true"></i> Consultando la bodega (Check) y Random…</p>';
    return cargarGuia(vid, num, forzar).then(function (r) {
      if (panel.getAttribute('data-gl-abierta') !== String(num)) return r;   // mientras tanto se abrió otro documento
      panel.innerHTML = htmlDetalle(r, num);
      pintar(sec, vid);
      return r;
    });
  }

  function cerrar(sec, num) {
    var chip = chipDe(sec, num), panel = panelDe(chip);
    if (!chip) return;
    chip.setAttribute('aria-expanded', 'false');
    if (panel && panel.getAttribute('data-gl-abierta') === String(num)) {
      panel.hidden = true;
      panel.innerHTML = '';
      panel.removeAttribute('data-gl-abierta');
    }
  }

  function primeraAmbar(sec, vid) {
    var cs = chips(sec);
    for (var i = 0; i < cs.length; i++) {
      var num = cs[i].getAttribute('data-gl-ver');
      var s = semaforoDe(vid, num);
      if (s && s.color === 'ambar') return num;
    }
    return cs.length ? cs[0].getAttribute('data-gl-ver') : null;
  }

  function montar(sec, vid) {
    vid = parseInt(vid, 10);
    if (!sec || !vid || sec.__glMontado) return;
    if (!chips(sec).length) return;                 // sin guías de despacho: nada que comparar
    sec.__glMontado = true;
    sec.addEventListener('click', function (e) {
      var t = e.target;
      if (!t || !t.closest) return;
      var b = t.closest('[data-gl-ver]');
      if (b && sec.contains(b)) {
        e.preventDefault();
        e.__glHecho = true;                         // ot_doc_cadena.js no lo atiende de nuevo
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
        var chip = n ? chipDe(sec, n) : null;
        if (chip && chip.getAttribute('aria-expanded') !== 'true') abrir(sec, vid, n);
        var v = ventaDe(chip);
        if (v && v.scrollIntoView) v.scrollIntoView({ behavior: 'smooth', block: 'center' });
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
    _limpiar: function () { resumenes = {}; semaforos = {}; checkOT = {}; pidiendo = {}; detalles = {}; }
  };
  if (typeof document !== 'undefined') {
    if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', iniciar);
    else iniciar();
  }
})(window);

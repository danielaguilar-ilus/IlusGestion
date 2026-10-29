/* 🔗 «Trazabilidad en Random» — la cadena documental de la OT (Daniel, 25-oct-2026).

   «Es para agregar inteligencia a los cobros, ya que se les olvida que los servicios se facturan… sugieras el documento
   en caso que lo tenga: FCV o BLV es lo que cierra una OT»; «si la ocupé en otra orden de trabajo, me debe arrojar el
   saldo y el margen de esa OT… que no te metan gato por liebre… nació una nota de venta, se volvió factura o boleta y
   después salió el stock con una guía de despacho».

   Se monta solo dentro del bloque «Costos y documentación» (static/ot_fin_motor.js, [data-fin-motor]) y se vuelve a
   poner cada vez que el motor se repinta. Lee GET /ot/api/<vid>/documentos/cadena (solo lectura, sin técnicos) y:
     · sugiere la factura o boleta que Random ya tiene para la nota de venta de la OT, con el MISMO diálogo de siempre
       («Agregar un documento a la OT», OTFinMotor.ligarDocumento) ya rellenado: la persona decide (REGLA #32);
     · muestra la cadena nota de venta → factura o boleta → guía de despacho, el saldo de cada documento y qué otras OT
       lo usan, con su margen;
     · alerta en ámbar lo que no cuadra (otra empresa, una factura que no viene de la nota de venta). Nunca bloquea.
   Sin alert/confirm/prompt nativos (REGLA #1). Todo el texto se escapa. */
(function (global) {
  'use strict';

  var datos = {};      // vid → respuesta del servidor (null si no hay nada que mostrar)
  var pidiendo = {};   // vid → promesa en curso

  function esc(s) {
    return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
    });
  }
  function clp(n) {
    var v = Math.round(Number(n) || 0);
    return (v < 0 ? '-$' : '$') + Math.abs(v).toString().replace(/\B(?=(\d{3})+(?!\d))/g, '.');
  }
  function pct(p) { return (Math.round(Number(p) * 10) / 10).toString().replace('.', ',') + ' %'; }

  function cargar(vid) {
    if (Object.prototype.hasOwnProperty.call(datos, vid)) return Promise.resolve(datos[vid]);
    if (pidiendo[vid]) return pidiendo[vid];
    pidiendo[vid] = fetch('/ot/api/' + vid + '/documentos/cadena', { credentials: 'same-origin', headers: { 'Accept': 'application/json' } })
      .then(function (r) { return r.ok ? r.json() : null; })
      .catch(function () { return null; })
      .then(function (j) {
        datos[vid] = (j && j.ok && !j.interna) ? j : null;
        pidiendo[vid] = null;
        return datos[vid];
      });
    return pidiendo[vid];
  }

  function hayAlgo(d) {
    if (!d) return false;
    var hayCadena = (d.cadenas || []).some(function (c) { return (c.nodos || []).length > 0; });
    return hayCadena || (d.sugerencias || []).length > 0 || (d.alertas || []).length > 0;
  }

  /* ── Pintado ─────────────────────────────────────────────────────────── */
  function htmlSaldo(n) {
    if (!n.saldo) {
      if (n.tipo === 'GDV') return '';
      return n.leido === false ? '<p class="dc-sal dc-gris">Random no respondió: no se pudo leer su saldo.</p>'
        : '<p class="dc-sal dc-gris">Sin líneas de servicio ni de despacho (solo productos).</p>';
    }
    var partes = [];
    [['servicio', 'Servicio'], ['despacho', 'Despacho']].forEach(function (c) {
      var x = n.saldo[c[0]];
      if (!x) return;
      var libre = Math.max(0, x.saldo);
      partes.push('<span class="dc-sal-i ' + (libre <= 0 ? 'cons' : libre < x.total ? 'parc' : 'libre') + '">' + c[1] + ': ' +
        (libre >= x.total ? 'libre ' + clp(x.total) : libre <= 0 ? 'usado completo (' + clp(x.total) + ')' : 'libre ' + clp(libre) + ' de ' + clp(x.total)) + '</span>');
    });
    return '<p class="dc-sal">' + partes.join('') + '</p>';
  }

  function htmlUsos(n) {
    if (!(n.usos || []).length) return '';
    return '<ul class="dc-usos" aria-label="Otras OT que usan este documento">' + n.usos.map(function (u) {
      var tomo = [];
      if (u.servicio) tomo.push(clp(u.servicio) + ' de servicio');
      if (u.despacho) tomo.push(clp(u.despacho) + ' de despacho');
      var ot = u.url ? '<a href="' + esc(u.url) + '">' + esc(u.numero_ot || ('OT ' + u.vid)) + '</a>' : esc(u.numero_ot || ('OT ' + u.vid));
      var margen = u.medible && u.margen_pct != null
        ? '<span class="dc-mg ' + esc(u.nivel || '') + '">margen ' + pct(u.margen_pct) + '</span>'
        : '<span class="dc-mg gris">margen sin medir todavía</span>';
      return '<li><i class="bi bi-arrow-return-right" aria-hidden="true"></i> También la usa ' + ot +
        (u.cliente ? ' · ' + esc(u.cliente) : '') + (tomo.length ? ' · tomó ' + tomo.join(' + ') : '') + ' · ' + margen + '</li>';
    }).join('') + '</ul>';
  }

  function htmlNodo(n) {
    var cls = 'dc-n t-' + (n.tipo === 'GDV' ? 'guia' : /^(FCV|FCE|BLV|BLE)$/.test(n.tipo) ? 'cobro' : 'nota');
    if (n.en_ot && !n.dada_de_baja) cls += ' en-ot';
    if (n.dada_de_baja) cls += ' baja';
    var chips = [];
    if (n.tipo !== 'GDV') {
      if (n.dada_de_baja) chips.push('<span class="dc-chip gris">Dada de baja por ' + esc(n.reemplazado_por || 'la factura') + '</span>');
      else if (n.heredado) chips.push('<span class="dc-chip gris">Copia de la OT madre</span>');
      else if (n.en_ot) chips.push('<span class="dc-chip verde"><i class="bi bi-check2" aria-hidden="true"></i> En esta OT</span>');
      else chips.push('<span class="dc-chip azul">No está en esta OT</span>');
      if (n.mismo_cliente === false) chips.push('<span class="dc-chip ambar">Otra empresa</span>');
    }
    var h = '<li class="' + cls + '"><div class="dc-n-h"><span class="dc-tipo">' + esc(n.tipo_txt) + '</span> <b>' + esc(n.titulo) + '</b>' +
      (n.fecha ? ' <span class="dc-f">' + esc(n.fecha) + '</span>' : '') + '</div>';
    if (chips.length) h += '<div class="dc-chips">' + chips.join('') + '</div>';
    if (n.tipo === 'GDV') {
      h += '<p class="dc-sal">' + esc(n.texto || 'Salió el stock') + (n.desde ? ' · desde ' + esc(n.desde) : '') + '</p>';
      if ((n.productos || []).length) {
        h += '<ul class="dc-prods">' + n.productos.map(function (p) {
          return '<li>' + esc(p.cantidad) + ' × ' + esc(p.nombre || p.sku) + (p.sku && p.nombre ? ' <small>' + esc(p.sku) + '</small>' : '') + '</li>';
        }).join('') + '</ul>';
      }
      /* 🚚 2026-10-29 (Daniel: «en la guía de despacho, presiono y no hicimos la conexión con el WMS… para saber QUÉ SE FUE»):
         «Ver logística» lo maneja static/ot_guia_logistica.js (bodega según Check y «lo que salió» vs «lo que gestiona la OT»). */
      h += '<div class="gl" data-gl-guia="' + esc(n.numero) + '"><div class="gl-estado" data-gl-estado aria-live="polite"></div>' +
        '<button type="button" class="gl-btn" data-gl-ver="' + esc(n.numero) + '" aria-expanded="false">' +
        '<i class="bi bi-truck" aria-hidden="true"></i> Ver logística</button>' +
        '<div class="gl-cuerpo" data-gl-cuerpo hidden></div></div>';
    } else {
      if (n.mismo_cliente === false && (n.rut || n.razon_social)) {
        h += '<p class="dc-sal dc-ambar">Emitida a ' + esc(n.razon_social || 'otra empresa') + (n.rut ? ' (RUT ' + esc(n.rut) + ')' : '') + '</p>';
      }
      h += htmlSaldo(n) + htmlUsos(n);
    }
    return h + '</li>';
  }

  function htmlSugerencia(s, i) {
    var otra = s.mismo_cliente === false;
    return '<div class="dc-sug' + (otra ? ' otra' : '') + '" role="note"><i class="bi ' + (otra ? 'bi-exclamation-triangle-fill' : 'bi-lightbulb-fill') + '" aria-hidden="true"></i>' +
      '<div class="dc-sug-t"><b>' + (otra ? 'Factura encontrada, de otra empresa' : 'Esta nota de venta ya se facturó') + '</b>' +
      '<p>' + esc(s.texto) + '.</p>' +
      (s.aviso_otra_empresa ? '<p class="dc-av">' + esc(s.aviso_otra_empresa) + '</p>' : '') +
      (s.aviso_consumido ? '<p class="dc-av">' + esc(s.aviso_consumido) + '</p>' : '') +
      '<p class="dc-help">La factura o la boleta es lo que cierra la OT. Al agregarla, la nota de venta queda dada de baja y las dos se siguen viendo.</p></div>' +
      '<button type="button" class="dc-btn" data-dc-add="' + i + '" aria-label="Agregar la ' + esc(s.titulo) + ' a esta OT"><i class="bi bi-plus-circle" aria-hidden="true"></i> Agregar la ' + esc(s.titulo) + '</button></div>';
  }

  function html(d) {
    var h = '<div class="dc-h"><h3><i class="bi bi-diagram-3-fill" aria-hidden="true"></i> Trazabilidad en Random</h3>' +
      '<small>Nota de venta → factura o boleta → guía de despacho · solo lectura</small>' +
      '<span class="gl-chip-slot" data-gl-chip></span></div>';
    (d.sugerencias || []).forEach(function (s, i) { h += htmlSugerencia(s, i); });
    (d.alertas || []).forEach(function (a) {
      h += '<div class="dc-al" role="note"><i class="bi bi-exclamation-triangle-fill" aria-hidden="true"></i><p>' + esc(a.texto) + '</p></div>';
    });
    (d.cadenas || []).forEach(function (c) {
      if (!(c.nodos || []).length) return;
      h += '<ol class="dc-cad' + (c.nodos.length > 3 ? ' largo' : '') + '" aria-label="Cadena de documentos de una venta">' + c.nodos.map(htmlNodo).join('') + '</ol>';
      if (c.leida === false) h += '<p class="dc-help">Random no respondió del todo: esta cadena puede estar incompleta.</p>';
    });
    return h;
  }

  /* ── Montaje dentro del motor ───────────────────────────────────────── */
  function pintar(el) {
    var vid = parseInt(el.getAttribute('data-vid'), 10);
    if (!vid || el.querySelector('.fm-cargando')) return;     // el motor aún no pinta
    var d = datos[vid];
    var viejo = null;
    for (var i = 0; i < el.children.length; i++) { if (el.children[i].classList.contains('dc')) { viejo = el.children[i]; break; } }
    if (!hayAlgo(d)) { if (viejo) viejo.remove(); return; }
    if (viejo) return;
    var sec = document.createElement('section');
    sec.className = 'dc';
    sec.setAttribute('aria-label', 'Trazabilidad en Random de los documentos de esta OT');
    sec.innerHTML = html(d);
    var top = null;
    for (var j = 0; j < el.children.length; j++) { if (el.children[j].classList.contains('fm-top')) { top = el.children[j]; break; } }
    if (top && top.nextSibling) el.insertBefore(sec, top.nextSibling);
    else el.appendChild(sec);
    // 🚚 2026-10-29: la logística de las guías (static/ot_guia_logistica.js) se monta sobre la sección recién pintada.
    if (global.OTGuiaLogistica && typeof global.OTGuiaLogistica.montar === 'function') global.OTGuiaLogistica.montar(sec, vid);
  }

  function agregar(vid, s) {
    if (!global.OTFinMotor || typeof global.OTFinMotor.ligarDocumento !== 'function') {
      if (typeof global.ilusToast === 'function') global.ilusToast('Usa «Agregar factura, boleta o nota de venta» con la ' + s.titulo + '.', { type: 'info' });
      return;
    }
    global.OTFinMotor.ligarDocumento(vid, {
      pre: { tipo: s.tipo, numero: s.numero, etiqueta: s.etiqueta || '' },
      hecho: function () { delete datos[vid]; setTimeout(function () { location.reload(); }, 700); }
    });
  }

  function montar(el) {
    if (el.__dcMontado) return;
    el.__dcMontado = true;
    var vid = parseInt(el.getAttribute('data-vid'), 10);
    if (!vid) return;
    el.addEventListener('click', function (e) {
      var b = e.target.closest('[data-dc-add]');
      if (!b || !el.contains(b)) return;
      e.preventDefault();
      var s = ((datos[vid] || {}).sugerencias || [])[parseInt(b.getAttribute('data-dc-add'), 10)];
      if (s) agregar(vid, s);
    });
    var t = null;
    var obs = new MutationObserver(function () {
      clearTimeout(t);
      t = setTimeout(function () { pintar(el); }, 60);
    });
    obs.observe(el, { childList: true });
    cargar(vid).then(function () { pintar(el); });
  }

  function iniciar() {
    var els = document.querySelectorAll('[data-fin-motor]');
    for (var i = 0; i < els.length; i++) montar(els[i]);
  }

  global.OTDocCadena = { iniciar: iniciar, _html: html };
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', iniciar);
  else iniciar();
})(window);

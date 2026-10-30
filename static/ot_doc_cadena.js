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
   2026-10-29 (Daniel: «quiero que sea PEQUEÑA; no quiero que ocupe tanto espacio la trazabilidad de Random. Pero cuando le
   doy clic… que me diga TODOS LOS MOVIMIENTOS»): cada venta es UNA fila fina de chips (nota de venta → factura → guías, con un
   punto de semáforo por guía) y el detalle de cada documento (saldo por línea, otras OT que lo usan y su margen, productos,
   «otra empresa», dada de baja) se abre al tocar su chip, en un panel debajo de la fila. Nada se perdió (REGLA #4.2): es lo
   mismo de antes, detrás del clic. El panel de una GUÍA lo pinta static/ot_guia_logistica.js (qué salió y qué no, Check).
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

  function tipoCls(n) { return n.tipo === 'GDV' ? 'guia' : /^(FCV|FCE|BLV|BLE)$/.test(n.tipo) ? 'cobro' : 'nota'; }

  /* El DETALLE de un documento (lo que antes era su tarjeta grande): se pinta en el panel de la venta al tocar su chip. */
  function htmlNodo(n) {
    var chips = [];
    if (n.tipo !== 'GDV') {
      if (n.dada_de_baja) chips.push('<span class="dc-chip gris">Dada de baja por ' + esc(n.reemplazado_por || 'la factura') + '</span>');
      else if (n.heredado) chips.push('<span class="dc-chip gris">Copia de la OT madre</span>');
      else if (n.en_ot) chips.push('<span class="dc-chip verde"><i class="bi bi-check2" aria-hidden="true"></i> En esta OT</span>');
      else chips.push('<span class="dc-chip azul">No está en esta OT</span>');
      if (n.mismo_cliente === false) chips.push('<span class="dc-chip ambar">Otra empresa</span>');
    }
    var h = '<div class="dc-n t-' + tipoCls(n) + '"><div class="dc-n-h"><span class="dc-tipo">' + esc(n.tipo_txt) + '</span> <b>' + esc(n.titulo) + '</b>' +
      (n.fecha ? ' <span class="dc-f">' + esc(n.fecha) + '</span>' : '') + '</div>';
    if (chips.length) h += '<div class="dc-chips">' + chips.join('') + '</div>';
    if (n.tipo === 'GDV') {
      /* Respaldo: si static/ot_guia_logistica.js no está, la guía muestra lo de Random (sin Check). */
      h += '<p class="dc-sal">' + esc(n.texto || 'Salió el stock') + (n.desde ? ' · desde ' + esc(n.desde) : '') + '</p>';
      if ((n.productos || []).length) {
        h += '<ul class="dc-prods">' + n.productos.map(function (p) {
          return '<li>' + esc(p.cantidad) + ' × ' + esc(p.nombre || p.sku) + (p.sku && p.nombre ? ' <small>' + esc(p.sku) + '</small>' : '') + '</li>';
        }).join('') + '</ul>';
      }
    } else {
      if (n.mismo_cliente === false && (n.rut || n.razon_social)) {
        h += '<p class="dc-sal dc-ambar">Emitida a ' + esc(n.razon_social || 'otra empresa') + (n.rut ? ' (RUT ' + esc(n.rut) + ')' : '') + '</p>';
      }
      h += htmlSaldo(n) + htmlUsos(n);
    }
    return h + '</div>';
  }

  /* El CHIP de un documento en la fila de la venta: tipo y número, y una marca chica (✓ en esta OT, ⚠ otra empresa,
     tachado si quedó dada de baja). Las guías llevan además su punto de semáforo (lo pinta ot_guia_logistica.js). */
  function chipDoc(n, vi, ni, panelId) {
    var guia = n.tipo === 'GDV';
    var cls = 'dc-doc t-' + tipoCls(n) + (n.en_ot && !n.dada_de_baja ? ' en-ot' : '') + (n.dada_de_baja ? ' baja' : '');
    var estado = [];
    if (!guia) {
      if (n.dada_de_baja) estado.push('dada de baja por ' + (n.reemplazado_por || 'la factura'));
      else if (n.heredado) estado.push('copia de la OT madre');
      else if (n.en_ot) estado.push('en esta OT');
      else estado.push('no está en esta OT');
      if (n.mismo_cliente === false) estado.push('emitida a otra empresa');
    } else if (n.fecha) estado.push('del ' + n.fecha);
    var marca = '';
    if (!guia && n.en_ot && !n.dada_de_baja && !n.heredado) marca += '<i class="bi bi-check2 dc-ok" aria-hidden="true"></i>';
    if (!guia && n.mismo_cliente === false) marca += '<i class="bi bi-exclamation-triangle-fill dc-warn" aria-hidden="true"></i>';
    var etiqueta = (n.tipo_txt || n.tipo) + ' ' + (n.titulo || '') + (estado.length ? ', ' + estado.join(', ') : '') +
      (guia ? '. Ver qué salió y qué no' : '. Ver su detalle');
    return '<button type="button" class="' + cls + '" data-dc-doc="' + vi + ':' + ni + '"' +
      (guia ? ' data-gl-guia="' + esc(n.numero) + '" data-gl-ver="' + esc(n.numero) + '"' : '') +
      ' aria-expanded="false" aria-controls="' + esc(panelId) + '" aria-label="' + esc(etiqueta) + '" title="' + esc(etiqueta) + '">' +
      (guia ? '<span class="gl-dot gris pend" data-gl-dot aria-hidden="true"></span>' : '') +
      '<span class="dc-doc-t">' + esc(n.tipo) + '</span> <b>' + esc(n.numero) + '</b>' + marca + '</button>';
  }

  function htmlVenta(c, vi, pref) {
    var docs = [], guias = [], id = 'dc-panel-' + pref + '-' + vi;
    (c.nodos || []).forEach(function (n, ni) { (n.tipo === 'GDV' ? guias : docs).push(chipDoc(n, vi, ni, id)); });
    // La flecha va pegada al documento que sigue: al partirse la fila en el celular nunca queda suelta al final.
    var flecha = '<i class="bi bi-arrow-right dc-fl" aria-hidden="true"></i>';
    var items = docs.map(function (d, i) { return '<li>' + (i ? flecha : '') + d + '</li>'; });
    if (guias.length) items.push('<li class="dc-gs">' + (docs.length ? flecha : '') + guias.join('') + '</li>');
    return '<div class="dc-venta" data-dc-venta="' + vi + '"><ol class="dc-fila" aria-label="Cadena de documentos de una venta">' +
      items.join('') + '</ol>' +
      '<p class="dc-res" data-gl-resumen hidden></p>' +
      '<div class="dc-panel" id="' + id + '" data-dc-panel role="region" aria-label="Detalle del documento" hidden></div>' +
      (c.leida === false ? '<p class="dc-help">Random no respondió del todo: esta cadena puede estar incompleta.</p>' : '') + '</div>';
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
      '<small>toca un documento para ver su detalle</small>' +
      '<span class="gl-chip-slot" data-gl-chip></span></div>';
    (d.sugerencias || []).forEach(function (s, i) { h += htmlSugerencia(s, i); });
    (d.alertas || []).forEach(function (a) {
      h += '<div class="dc-al" role="note"><i class="bi bi-exclamation-triangle-fill" aria-hidden="true"></i><p>' + esc(a.texto) + '</p></div>';
    });
    var pref = String(d.visita_id || '').replace(/\D/g, '') || 'x';
    (d.cadenas || []).forEach(function (c, vi) {
      if (!(c.nodos || []).length) return;
      h += htmlVenta(c, vi, pref);
    });
    return h;
  }

  /* ── La VENTANA del detalle (2026-10-29, Daniel: «si quiero ver el detalle, no quiero ver eso ahí en la pantalla. Quiero darle
        clic y verlo, así como en Retiros»). Una sola ventana a la vez; Esc, la X o tocar afuera la cierran; en el celular
        ocupa toda la pantalla. La usa también static/ot_guia_logistica.js (OTDocCadena.modal). ─────────────────────────── */
  var modalAbierto = null;
  function cerrarModal() {
    var m = modalAbierto;
    if (!m) return;
    modalAbierto = null;
    document.removeEventListener('keydown', m.tecla, true);
    if (m.el.parentNode) m.el.parentNode.removeChild(m.el);
    document.documentElement.classList.remove('dc-mo-abierto');
    if (m.abridor && typeof m.abridor.focus === 'function') { try { m.abridor.focus(); } catch (e) { /* el botón ya no existe */ } }
  }
  function modal(o) {
    o = o || {};
    cerrarModal();
    var el = document.createElement('div');
    el.className = 'dc-mo';
    el.setAttribute('data-dc-modal', '');
    el.innerHTML = '<div class="dc-mo-c" role="dialog" aria-modal="true" aria-labelledby="dc-mo-t" tabindex="-1">' +
      '<div class="dc-mo-h"><h3 id="dc-mo-t">' + esc(o.titulo || '') + '</h3>' +
      '<button type="button" class="dc-mo-x" data-dc-mo-x aria-label="Cerrar"><i class="bi bi-x-lg" aria-hidden="true"></i></button></div>' +
      '<div class="dc-mo-b" data-dc-mo-body></div></div>';
    var cuerpo = el.querySelector('[data-dc-mo-body]');
    cuerpo.innerHTML = o.html || '';
    var m = { el: el, abridor: o.abridor, tecla: function (e) { if (e.key === 'Escape') { e.stopPropagation(); cerrarModal(); } } };
    el.addEventListener('click', function (e) {
      var t = e.target;
      if (t === el || (t && t.closest && t.closest('[data-dc-mo-x]'))) { e.preventDefault(); cerrarModal(); }
    });
    document.addEventListener('keydown', m.tecla, true);
    document.body.appendChild(el);
    document.documentElement.classList.add('dc-mo-abierto');
    modalAbierto = m;
    var x = el.querySelector('[data-dc-mo-x]');
    if (x && typeof x.focus === 'function') x.focus();
    return {
      cuerpo: cuerpo,
      poner: function (h) { cuerpo.innerHTML = h; cuerpo.scrollTop = 0; },
      cerrar: cerrarModal,
      activo: function () { return modalAbierto === m; }
    };
  }

  /* Los documentos de la venta donde está esa guía (nota de venta → factura → guías), para armar su línea de tiempo. */
  function cadenaDe(vid, num) {
    var cs = (datos[vid] || {}).cadenas || [];
    for (var i = 0; i < cs.length; i++) {
      var ns = cs[i].nodos || [];
      for (var j = 0; j < ns.length; j++) { if (ns[j].tipo === 'GDV' && String(ns[j].numero) === String(num)) return ns; }
    }
    return [];
  }

  /* ── El panel de una venta (respaldo si no hay ventana): un documento abierto a la vez ─────────────── */
  function marcar(venta, chip) {
    var bs = venta.querySelectorAll('[data-dc-doc]');
    for (var i = 0; i < bs.length; i++) bs[i].setAttribute('aria-expanded', bs[i] === chip ? 'true' : 'false');
  }
  function abrirDoc(chip, vid) {
    var venta = chip.closest('[data-dc-venta]');
    var panel = venta && venta.querySelector('[data-dc-panel]');
    if (!panel) return;
    if (chip.getAttribute('aria-expanded') === 'true') { cerrarPanel(venta); return; }
    var pos = String(chip.getAttribute('data-dc-doc') || '').split(':');
    var c = ((datos[vid] || {}).cadenas || [])[parseInt(pos[0], 10)] || {};
    var n = (c.nodos || [])[parseInt(pos[1], 10)];
    if (!n) return;
    if (document.body && typeof document.body.appendChild === 'function') {
      modal({ titulo: (n.tipo_txt || n.tipo || 'Documento') + ' ' + (n.numero || ''), html: htmlNodo(n), abridor: chip });
      return;
    }
    marcar(venta, chip);
    panel.removeAttribute('data-gl-abierta');      // si una guía seguía cargando, ya no pinta encima
    panel.innerHTML = htmlNodo(n);
    panel.hidden = false;
  }
  function cerrarPanel(venta) {
    var panel = venta.querySelector('[data-dc-panel]');
    if (panel) { panel.hidden = true; panel.innerHTML = ''; panel.removeAttribute('data-gl-abierta'); }
    marcar(venta, null);
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
      if (!e.target || !e.target.closest) return;
      var b = e.target.closest('[data-dc-add]');
      if (b && el.contains(b)) {
        e.preventDefault();
        var s = ((datos[vid] || {}).sugerencias || [])[parseInt(b.getAttribute('data-dc-add'), 10)];
        if (s) agregar(vid, s);
        return;
      }
      // Un chip de documento abre su detalle. El de una GUÍA lo atiende ot_guia_logistica.js (marca e.__glHecho); si ese
      // archivo no cargó, se muestra lo de Random como respaldo.
      var chip = e.target.closest('[data-dc-doc]');
      if (chip && el.contains(chip) && !e.__glHecho) {
        e.preventDefault();
        abrirDoc(chip, vid);
      }
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

  global.OTDocCadena = { iniciar: iniciar, modal: modal, cerrarModal: cerrarModal, cadenaDe: cadenaDe, _html: html, _htmlNodo: htmlNodo };
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', iniciar);
  else iniciar();
})(window);

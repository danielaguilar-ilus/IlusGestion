/* Tickets 2.0 · ficha en formato Retiros (2026-10-27).
   tickets_ficha.js llama window.tkV2AlCargar(t, d) al terminar cada carga.
   Aquí solo se LEE lo que ya llegó del servidor: el semáforo de los pasos
   1-5 y la tarjeta «Contactos». No escribe nada ni cambia otro comportamiento. */
(function(){
  'use strict';

  function e(s){
    return String(s == null ? '' : s).replace(/[&<>"']/g, function(c){
      return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c];
    });
  }
  function lleno(v){ v = (v == null ? '' : String(v)).trim(); return v !== '' && v !== '—'; }
  function plural(n, uno, varios){ return n + ' ' + (n === 1 ? uno : varios); }

  // Estado previo de cada paso: el pulso verde solo se ve cuando un paso
  // RECIÉN se completa, no en cada recarga del latido.
  var _previo = {};

  function pintarPaso(id, estado, sub){
    var el = document.getElementById(id);
    if (!el) return;
    el.classList.remove('is-ok', 'is-warn', 'is-opt', 'tk-paso-recien');
    if (estado) el.classList.add('is-' + estado);
    if (estado === 'ok' && _previo[id] && _previo[id] !== 'ok') el.classList.add('tk-paso-recien');
    _previo[id] = estado || 'pend';
    var s = el.querySelector('[data-paso-sub]');
    if (s) s.textContent = sub || '';
  }

  // Número chileno → wa.me (solo dígitos, con 56 delante).
  function waNumero(tel){
    var d = String(tel || '').replace(/\D/g, '');
    if (d.length === 9 && d.charAt(0) === '9') d = '56' + d;
    if (d.length === 8) d = '569' + d;
    return d.length >= 11 ? d : '';
  }
  function iniciales(nom){
    var p = String(nom || '').trim().split(/\s+/).filter(Boolean);
    if (!p.length) return '?';
    return (p[0].charAt(0) + (p.length > 1 ? p[p.length - 1].charAt(0) : '')).toUpperCase();
  }

  function pintarContactos(d){
    var cont = document.getElementById('listaContactos');
    if (!cont) return;
    var lista = d.contactos || [];
    var link = document.getElementById('contactosFichaCliente');
    if (link) {
      if (d.cliente_ficha && d.cliente_ficha.url) {
        link.href = d.cliente_ficha.url;
        link.title = 'Abrir la ficha de ' + (d.cliente_ficha.razon_social || 'el cliente');
        link.style.display = '';
      } else {
        link.style.display = 'none';
      }
    }
    if (!lista.length) {
      cont.innerHTML = '<div class="tk-ct-vacio"><i class="bi bi-person-plus me-1"></i>'
        + 'Este ticket no trae a quién contactar. Agrega el nombre, teléfono o correo en '
        + '<strong>Identificación del cliente</strong>.</div>';
      return;
    }
    cont.innerHTML = lista.map(function(c){
      var esTicket = c.origen === 'ticket';
      var nombre = c.nombre || c.email || c.tel || 'Sin nombre';
      var meta = '';
      if (c.cargo) meta += '<span><i class="bi bi-briefcase"></i>' + e(c.cargo) + '</span>';
      if (c.tel) meta += '<span><i class="bi bi-telephone"></i>' + e(c.tel) + '</span>';
      if (c.email) meta += '<span><i class="bi bi-envelope"></i>' + e(c.email) + '</span>';
      var acc = '';
      if (c.tel) {
        acc += '<a class="tk-ct-btn tel" href="tel:' + e(String(c.tel).replace(/[^\d+]/g, '')) + '" '
          + 'title="Llamar a ' + e(nombre) + '" aria-label="Llamar a ' + e(nombre) + '"><i class="bi bi-telephone-fill"></i></a>';
        var wa = waNumero(c.tel);
        if (wa) acc += '<a class="tk-ct-btn wa" href="https://wa.me/' + wa + '" target="_blank" rel="noopener" '
          + 'title="WhatsApp a ' + e(nombre) + '" aria-label="WhatsApp a ' + e(nombre) + '"><i class="bi bi-whatsapp"></i></a>';
      }
      if (c.email) acc += '<a class="tk-ct-btn mail" href="mailto:' + e(c.email) + '" '
        + 'title="Escribir a ' + e(c.email) + '" aria-label="Escribir a ' + e(nombre) + '"><i class="bi bi-envelope-fill"></i></a>';
      return '<div class="tk-ct">'
        + '<div class="tk-ct-av' + (esTicket ? ' ticket' : '') + '" aria-hidden="true">' + e(iniciales(nombre)) + '</div>'
        + '<div><div class="tk-ct-nom">' + e(nombre) + '</div>'
        + (meta ? '<div class="tk-ct-meta">' + meta + '</div>' : '')
        + '<span class="tk-ct-chip' + (esTicket ? ' ticket' : '') + '">' + e(c.label || '') + '</span></div>'
        + (acc ? '<div class="tk-ct-acc">' + acc + '</div>' : '<div></div>')
        + '</div>';
    }).join('');
  }

  window.tkV2AlCargar = function(t, d){
    t = t || {}; d = d || {};

    // 1 · Cliente
    var faltan = [];
    if (!lleno(t.empresa) && !lleno(t.nombre_contacto)) faltan.push('el nombre');
    if (!lleno(t.rut)) faltan.push('el RUT');
    pintarPaso('pasoCliente', faltan.length ? 'warn' : 'ok',
      faltan.length ? 'Falta ' + faltan.join(' y ') : (t.empresa || t.nombre_contacto || ''));

    // 2 · Contactos
    var cts = d.contactos || [];
    var conVia = cts.filter(function(c){ return lleno(c.tel) || lleno(c.email); }).length;
    pintarPaso('pasoContactos', conVia ? 'ok' : 'warn',
      conVia ? plural(cts.length, 'contacto', 'contactos') + (d.cliente_ficha ? ' · con ficha de cliente' : '')
             : 'Sin teléfono ni correo para contactar');
    pintarContactos(d);

    // 3 · Problema
    var hayProblema = lleno(t.titulo) || lleno(t.descripcion);
    pintarPaso('pasoProblema', hayProblema ? 'ok' : 'warn',
      hayProblema ? '' : 'Falta describir qué le pasa al cliente');

    // 4 · Equipos
    var eq = d.equipos || [];
    var conDoc = eq.filter(function(x){ return lleno(x.documento_garantia); }).length;
    pintarPaso('pasoEquipos', eq.length ? 'ok' : 'warn',
      eq.length ? plural(eq.length, 'equipo', 'equipos') + (conDoc ? ' · ' + conDoc + ' con documento' : '')
                : 'Aún no hay equipos asociados');

    // 5 · Fotos y documentos (opcional: gris si no hay)
    var adj = d.adjuntos || [];
    var fotos = adj.filter(function(a){ return /^image\//.test(a.mime_type || ''); }).length;
    var otros = adj.length - fotos;
    var partes = [];
    if (fotos) partes.push(plural(fotos, 'foto', 'fotos'));
    if (otros) partes.push(plural(otros, 'documento', 'documentos'));
    pintarPaso('pasoAdjuntos', adj.length ? 'ok' : 'opt',
      adj.length ? partes.join(' · ') : 'Opcional: fotos o documentos del caso');
  };
})();

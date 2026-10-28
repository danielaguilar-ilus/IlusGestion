/* Tickets 2.0 · ficha en formato Retiros (2026-10-27).
   tickets_ficha.js llama window.tkV2AlCargar(t, d) al terminar cada carga.
   Aquí se LEE lo que ya llegó del servidor y se dibuja:
     · el semáforo de los pasos 1-7 y la tarjeta «Contactos»;
     · la GUÍA del ticket (d.guia, calculada en tickets_guia.py): progreso,
       fichas por paso y «Siguiente: …» con sus botones — el sistema sugiere,
       la persona decide (REGLA #32);
     · la cabecera tipo Retiros (RUT · teléfono · contacto · SLA · OT · motivo);
     · motivo principal y diagnóstico de solución (PATCH de siempre);
     · «Me hago cargo», «Asociar OT existente» y «Generar cotización» en modal.
   No cambia ningún comportamiento de tickets_ficha.js (REGLA #4.2). */
(function(){
  'use strict';

  function e(s){
    return String(s == null ? '' : s).replace(/[&<>"']/g, function(c){
      return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c];
    });
  }
  function lleno(v){ v = (v == null ? '' : String(v)).trim(); return v !== '' && v !== '—'; }
  function plural(n, uno, varios){ return n + ' ' + (n === 1 ? uno : varios); }
  function $(id){ return document.getElementById(id); }
  function toast(msg, tipo){ if (typeof window.ilusToast === 'function') window.ilusToast(msg, {type: tipo || 'info'}); }
  function recargar(){ if (typeof window.cargar === 'function') window.cargar(); }

  var T = null, D = null;   // último ticket / respuesta pintados

  // Estado previo de cada paso: el pulso verde solo se ve cuando un paso
  // RECIÉN se completa, no en cada recarga del latido.
  var _previo = {};

  function pintarPaso(id, estado, sub){
    var el = $(id);
    if (!el) return;
    el.classList.remove('is-ok', 'is-warn', 'is-opt', 'is-actual', 'tk-paso-recien');
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
  function rutFmt(r){
    var s = String(r || '').replace(/[^0-9kK]/g, '').toUpperCase();
    if (s.length < 2) return r || '';
    var cuerpo = s.slice(0, -1), dv = s.slice(-1);
    return cuerpo.replace(/\B(?=(\d{3})+(?!\d))/g, '.') + '-' + dv;
  }

  function pintarContactos(d){
    var cont = $('listaContactos');
    if (!cont) return;
    var lista = d.contactos || [];
    var link = $('contactosFichaCliente');
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

  // ── Cabecera tipo Retiros ───────────────────────────────────────────────
  function fila(idRow, idTxt, valor){
    var row = $(idRow), txt = $(idTxt);
    if (!row || !txt) return;
    if (lleno(valor)) { txt.textContent = valor; row.style.display = 'inline-flex'; }
    else row.style.display = 'none';
  }
  function tile(id, v, k, clase){
    var el = $(id); if (!el) return;
    var ev = el.querySelector('.v'), ek = el.querySelector('.k');
    if (ev) ev.textContent = v;
    if (ek && k != null) ek.textContent = k;
    el.classList.remove('is-rojo', 'is-ambar', 'is-verde', 'is-gris', 'is-falta');
    if (clase) el.classList.add(clase);
  }
  function pintarHero(t, d){
    fila('hRutRow', 'hRut', t.rut ? 'RUT ' + rutFmt(t.rut) : '');
    fila('hTelRow', 'hTel', t.phone || '');
    var contacto = [t.nombre_contacto, t.email].filter(lleno).join(' · ');
    fila('hContactoRow', 'hContacto', contacto);
    var sla = t.sla_resp || null;
    if (sla) {
      var cl = {rojo: 'is-rojo', ambar: 'is-ambar', verde: 'is-verde', gris: 'is-gris'}[sla.semaforo] || 'is-gris';
      tile('tileSla', sla.pill || sla.etiqueta || '—', 'SLA de respuesta' + (sla.sub ? ' · ' + sla.sub : ''), cl);
    } else {
      tile('tileSla', '—', 'SLA de respuesta', 'is-gris');
    }
    var resp = (t.asignado_a || '').trim();
    tile('tileResp', resp || 'Sin asignar', 'Responsable', resp ? '' : 'is-falta');
    if (t.visita_id) {
      tile('tileOT', t.visita_numero_ot || ('OT #' + t.visita_id),
           'Orden de trabajo' + (t.visita_estado_label ? ' · ' + t.visita_estado_label : ''), 'is-verde');
    } else {
      tile('tileOT', 'Sin OT', 'Orden de trabajo', '');
    }
    var motivo = (t.motivo_principal || '').trim();
    tile('tileMotivo', motivo || 'Sin definir', 'Motivo principal', motivo ? '' : 'is-falta');
    var cf = d.cliente_ficha;
    tile('tileCliente', cf ? (cf.razon_social || 'Cliente con ficha') : (t.empresa || t.nombre_contacto || 'Sin ficha'),
         cf ? 'Cliente · ficha en ILUS' : 'Cliente · sin ficha en ILUS', cf ? '' : 'is-falta');
    var tc = $('tileCliente');
    if (tc) { tc.dataset.href = cf && cf.url ? cf.url : ''; }
  }

  // ── Motivo principal / diagnóstico de solución ──────────────────────────
  function pintarMotivoDiag(t){
    var mi = $('motivoInput'), di = $('diagTextarea');
    if (mi && document.activeElement !== mi) mi.value = t.motivo_principal || '';
    if (di && document.activeElement !== di) di.value = t.diagnostico_solucion || '';
    var motivo = (t.motivo_principal || '').trim();
    pintarPaso('pasoMotivo', motivo ? 'ok' : 'warn', motivo || 'Falta el motivo principal');
    var diag = (t.diagnostico_solucion || '').trim();
    var cerrado = ['resolved', 'closed', 'cancelado'].indexOf(t.estado) >= 0;
    pintarPaso('pasoDiagnostico', diag ? 'ok' : (cerrado ? 'warn' : 'opt'),
      diag ? 'Registrado' : (cerrado ? 'Ticket terminado sin diagnóstico' : 'Se registra cuando termina la gestión'));
    var mm = $('motivoMeta'); if (mm) mm.textContent = motivo ? '' : 'Ordena la bandeja y los reportes por causa.';
    var dm = $('diagMeta'); if (dm) dm.textContent = '';
  }
  function guardarCampo(campo, valor, btn, ok){
    if (btn) btn.disabled = true;
    var body = {}; body[campo] = valor;
    fetch('/tickets/api/tickets/' + TID, {method: 'PATCH', headers: {'Content-Type': 'application/json'},
      credentials: 'same-origin', body: JSON.stringify(body)})
      .then(function(r){ return r.json(); })
      .then(function(j){
        if (btn) btn.disabled = false;
        if (!j.ok) { toast(j.error || 'No se pudo guardar', 'error'); return; }
        toast(ok, 'success');
        recargar();
      })
      .catch(function(){ if (btn) btn.disabled = false; toast('Error de red', 'error'); });
  }

  // ── OT: estado en la tarjeta lateral ────────────────────────────────────
  function pintarOT(t){
    var box = $('otSideEstado');
    if (!box) return;
    if (!t.visita_id) { box.innerHTML = ''; return; }
    var partes = [];
    if (t.visita_estado_label) partes.push('<span class="tk-ot-chip"><i class="bi bi-activity"></i>' + e(t.visita_estado_label) + '</span>');
    if (t.visita_fecha) partes.push('<span class="tk-ot-chip"><i class="bi bi-calendar-event"></i>' + e(t.visita_fecha) + '</span>');
    if (t.visita_tecnico) partes.push('<span class="tk-ot-chip"><i class="bi bi-person-gear"></i>' + e(t.visita_tecnico) + '</span>');
    box.innerHTML = partes.join('');
  }

  // ── Guía del ticket ─────────────────────────────────────────────────────
  var ANCLA = { cliente: 'pasoCliente', contactos: 'pasoContactos', problema: 'pasoProblema', equipos: 'pasoEquipos',
                adjuntos: 'pasoAdjuntos', responsable: 'cardEstado', respuesta: null, motivo: 'pasoMotivo',
                gestion: 'cardOT', diagnostico: 'pasoDiagnostico', cierre: 'cardEstado' };
  var ESTADO_TXT = { hecho: ['Hecho', 'bi-check-circle-fill'], actual: ['Te toca', 'bi-exclamation-circle-fill'],
                     espera: ['Esperando', 'bi-hourglass-split'], pendiente: ['Pendiente', 'bi-circle'],
                     opcional: ['Opcional', 'bi-dash-circle'] };
  var ACCIONES = {
    tomar: ['Me hago cargo', 'bi-hand-index-thumb', 'rojo'],
    responder: ['Responder al cliente', 'bi-reply-fill', 'rojo'],
    editar_cliente: ['Completar cliente', 'bi-pencil-square', 'sec'],
    buscar_productos: ['Buscar productos', 'bi-search', 'sec'],
    motivo: ['Escribir el motivo', 'bi-bullseye', 'rojo'],
    diagnostico: ['Escribir el diagnóstico', 'bi-clipboard2-pulse', 'rojo'],
    generar_ot: ['Generar OT', 'bi-clipboard2-plus', 'rojo'],
    asociar_ot: ['Asociar OT existente', 'bi-link-45deg', 'sec'],
    cotizar: ['Generar cotización', 'bi-file-earmark-plus', 'sec'],
    repuesto: ['Solicitar repuesto', 'bi-tools', 'sec'],
    ver_ot: ['Ver la OT', 'bi-box-arrow-up-right', 'rojo'],
    resolver: ['Marcar como resuelto', 'bi-check2-circle', 'verde'],
    cerrar: ['Cerrar el ticket', 'bi-archive-fill', 'verde']
  };

  function irA(id, resaltar){
    var el = id && $(id);
    if (!el) return false;
    el.scrollIntoView({behavior: 'smooth', block: 'start'});
    if (resaltar !== false) {
      el.classList.remove('tk-resalta'); void el.offsetWidth; el.classList.add('tk-resalta');
      setTimeout(function(){ el.classList.remove('tk-resalta'); }, 1800);
    }
    return true;
  }
  function irARespuestas(){
    var tab = document.querySelector('.tk-tab[data-pane="conv"]');
    if (tab) tab.click();
    setTimeout(function(){
      var ed = document.querySelector('#rpEditor .ql-editor');
      if (ed) { ed.focus(); ed.scrollIntoView({behavior: 'smooth', block: 'center'}); }
    }, 150);
  }
  function irAPaso(clave){
    if (clave === 'respuesta') { irARespuestas(); return; }
    var id = ANCLA[clave];
    if (!id) return;
    if (id === 'cardEstado' || id === 'cardOT') {
      var pane = document.querySelector('.tk-tab[data-pane="info"]');
      if (pane && !pane.classList.contains('active')) pane.click();
    }
    irA(id);
  }

  function cambiarEstado(nuevo, titulo, msg){
    var p = (typeof window.ilusConfirm === 'function')
      ? window.ilusConfirm({title: titulo, message: msg, okLabel: titulo, cancelLabel: 'Cancelar'})
      : Promise.resolve(true);
    p.then(function(ok){
      if (!ok) return;
      guardarCampo('estado', nuevo, null, '✓ Estado actualizado');
    });
  }

  function ejecutar(accion){
    var t = T || {};
    switch (accion) {
      case 'tomar': tomar(); break;
      case 'responder': irARespuestas(); break;
      case 'editar_cliente': irA('pasoCliente'); break;
      case 'buscar_productos': { var b = $('btnEqDesdeDoc'); if (b) b.click(); else irA('pasoEquipos'); break; }
      case 'motivo': irA('pasoMotivo'); setTimeout(function(){ var i = $('motivoInput'); if (i) i.focus(); }, 400); break;
      case 'diagnostico': irA('pasoDiagnostico'); setTimeout(function(){ var i = $('diagTextarea'); if (i) i.focus(); }, 400); break;
      case 'generar_ot': if (typeof window.tkAbrirGenerarOT2 === 'function') window.tkAbrirGenerarOT2(); break;
      case 'asociar_ot': abrirAsociarOT(); break;
      case 'cotizar': abrirCotizacion(); break;
      case 'repuesto': if (window.TKREP && typeof window.TKREP.abrir === 'function') window.TKREP.abrir(); break;
      case 'ver_ot': if (t.visita_id) window.open('/ot/' + t.visita_id, '_blank', 'noopener'); break;
      case 'resolver': cambiarEstado('resolved', 'Marcar como resuelto',
        '¿El caso quedó resuelto para el cliente? El ticket pasa a «Resuelto» y el cliente recibe el aviso de siempre.'); break;
      case 'cerrar': cambiarEstado('closed', 'Cerrar el ticket',
        '¿Cerrar definitivamente este ticket? Queda como cerrado en la bandeja.'); break;
    }
  }

  function botonesHtml(acciones, chico){
    return (acciones || []).filter(function(a){ return ACCIONES[a]; })
      .filter(function(a){ return !(window._TK_ES_TECNICO && (a === 'cotizar' || a === 'asociar_ot')); })
      .map(function(a){
        var def = ACCIONES[a];
        return '<button type="button" class="tk-gbtn ' + def[2] + (chico ? ' chico' : '') + '" data-accion="' + a + '">'
          + '<i class="bi ' + def[1] + '"></i>' + e(def[0]) + '</button>';
      }).join('');
  }

  var _guiaDibujada = '';
  function pintarGuia(g, t){
    var panel = $('tkGuia');
    if (!panel) return;
    if (!g || !g.pasos) { panel.innerHTML = ''; panel.style.display = 'none'; return; }
    panel.style.display = '';
    var firma = JSON.stringify(g);
    if (firma === _guiaDibujada) return;   // el latido no redibuja si nada cambió
    _guiaDibujada = firma;
    var pr = g.progreso || {hechos: 0, total: 0, pct: 0};
    var sig = g.siguiente || {};
    var chips = g.pasos.map(function(p, i){
      var et = ESTADO_TXT[p.estado] || ESTADO_TXT.pendiente;
      return '<li><button type="button" class="tk-gchip is-' + p.estado + '" data-paso="' + e(p.clave) + '" '
        + 'title="' + e(p.titulo + (p.resumen ? ' · ' + p.resumen : '') + (p.faltan && p.faltan.length ? ' · ' + p.faltan.join(' ') : '')) + '">'
        + '<span class="tk-gchip-n">' + (p.estado === 'hecho' ? '<i class="bi bi-check-lg"></i>' : (i + 1)) + '</span>'
        + '<span class="tk-gchip-t">' + e(p.titulo) + '<small><i class="bi ' + et[1] + '"></i>' + e(p.resumen || et[0]) + '</small></span>'
        + '</button></li>';
    }).join('');
    var tono = sig.tono || 'actual';
    panel.innerHTML =
      '<div class="tk-gbar">'
      + '<div class="tk-gbar-t"><i class="bi bi-signpost-split-fill"></i><b>Guía del ticket</b></div>'
      + '<div class="tk-gbar-prog"><span>' + pr.hechos + ' de ' + pr.total + ' pasos listos</span>'
      + '<div class="tk-gprog"><i style="width:' + Math.max(0, Math.min(100, pr.pct)) + '%"></i></div><b>' + pr.pct + '%</b></div>'
      // Versión compacta cuando la barra queda fija arriba al hacer scroll (como la guía de Retiros)
      + '<button type="button" class="tk-gbar-sig" data-accion-sig="' + e((sig.acciones || [])[0] || '') + '" title="Ir a lo que toca">'
      + '<i class="bi bi-arrow-right-circle-fill"></i><span>' + (tono === 'fin' ? 'Listo' : 'Siguiente') + ': <b>' + e(sig.titulo || '') + '</b></span></button>'
      + '</div>'
      + '<ul class="tk-gchips">' + chips + '</ul>'
      + '<div class="tk-gsig is-' + e(tono) + '">'
      + '<div class="tk-gsig-k"><i class="bi ' + (tono === 'fin' ? 'bi-check-circle-fill' : (tono === 'espera' ? 'bi-hourglass-split' : 'bi-arrow-right-circle-fill')) + '"></i>'
      + (tono === 'fin' ? 'Listo' : (tono === 'espera' ? 'Esperando' : (tono === 'sugerencia' ? 'Sugerencia' : 'Siguiente'))) + '</div>'
      + '<h4>' + e(sig.titulo || '') + '</h4>'
      + (sig.texto ? '<p>' + e(sig.texto) + '</p>' : '')
      + (sig.acciones && sig.acciones.length ? '<div class="tk-gacc">' + botonesHtml(sig.acciones) + '</div>' : '')
      + '</div>';
    panel.querySelectorAll('.tk-gchip').forEach(function(b){
      b.addEventListener('click', function(){ irAPaso(b.dataset.paso); });
    });
    panel.querySelectorAll('.tk-gbtn').forEach(function(b){
      b.addEventListener('click', function(){ ejecutar(b.dataset.accion); });
    });
    var bs = panel.querySelector('.tk-gbar-sig');
    if (bs) bs.addEventListener('click', function(){
      if (sig.clave) irAPaso(sig.clave); else window.scrollTo({top: 0, behavior: 'smooth'});
    });
    observarFija(panel);
  }

  // La guía es sticky en escritorio: cuando queda pegada arriba se compacta
  // (solo la barra con «Siguiente: …»), para no tapar media pantalla.
  var _obs = null;
  function observarFija(panel){
    if (_obs || !('IntersectionObserver' in window)) return;
    var sent = document.createElement('div');
    sent.className = 'tk-guia-sentinel';
    panel.parentNode.insertBefore(sent, panel);
    _obs = new IntersectionObserver(function(en){
      panel.classList.toggle('is-fija', !en[0].isIntersecting && window.innerWidth >= 992);
    }, {threshold: 0});
    _obs.observe(sent);
  }

  // ── «Me hago cargo» ────────────────────────────────────────────────────
  function tomar(){
    var yo = (window._TK_YO_NOMBRE || '').trim();
    if (!yo) { toast('No pude reconocer tu nombre: elige tu nombre en «Ejecutivo asignado» y guarda.', 'warning'); irA('cardEstado'); return; }
    var p = (typeof window.ilusConfirm === 'function')
      ? window.ilusConfirm({title: 'Me hago cargo', message: 'El ticket queda a tu nombre: ' + yo + '.',
                            sub: 'El SLA de respuesta y los avisos llegan a ti. Puedes reasignarlo después.', okLabel: 'Sí, me hago cargo', cancelLabel: 'Cancelar'})
      : Promise.resolve(true);
    p.then(function(ok){ if (ok) guardarCampo('asignado_a', yo, $('btnTomarTicket'), '✓ Ticket a tu nombre'); });
  }
  function pintarTomar(t){
    var b = $('btnTomarTicket');
    if (!b) return;
    var cerrado = ['resolved', 'closed', 'cancelado'].indexOf(t.estado) >= 0;
    b.style.display = (!lleno(t.asignado_a) && !cerrado && (window._TK_YO_NOMBRE || '').trim()) ? '' : 'none';
  }

  // ── Asociar OT existente ───────────────────────────────────────────────
  var _otTimer = null;
  function abrirAsociarOT(){
    var m = $('tkAsocOtModal');
    if (!m || !window.bootstrap) return;
    var q = $('tkAsocOtQ'); if (q) q.value = '';
    buscarOT('');
    window.bootstrap.Modal.getOrCreateInstance(m).show();
    setTimeout(function(){ if (q) q.focus(); }, 300);
  }
  function buscarOT(q){
    var cont = $('tkAsocOtResultados');
    if (!cont) return;
    cont.innerHTML = '<div class="tk-asoc-vacio"><span class="spinner-border spinner-border-sm me-2"></span>Buscando OT…</div>';
    fetch('/tickets/api/tickets/' + TID + '/ot-buscar?q=' + encodeURIComponent(q || ''), {credentials: 'same-origin'})
      .then(function(r){ return r.json(); })
      .then(function(j){
        if (!j.ok) { cont.innerHTML = '<div class="tk-asoc-vacio">' + e(j.error || 'No se pudo buscar.') + '</div>'; return; }
        var hint = $('tkAsocOtHint');
        if (hint && j.hint) hint.textContent = j.hint;
        if (!j.ots.length) {
          cont.innerHTML = '<div class="tk-asoc-vacio"><i class="bi bi-clipboard2-x me-1"></i>'
            + (q ? 'Ninguna OT coincide con «' + e(q) + '».' : (j.sin_rut ? 'Este ticket no tiene RUT: escribe el número de la OT o el cliente.' : 'Este cliente no tiene OT todavía. Escribe un número para buscar en todas.')) + '</div>';
          return;
        }
        cont.innerHTML = j.ots.map(function(o){
          var sem = o.terminada ? 'gris' : (o.otro_ticket ? 'rojo' : (o.mismo_cliente ? 'verde' : 'ambar'));
          var aviso = o.otro_ticket ? 'Ya vinculada al ticket ' + o.otro_ticket
            : (o.terminada ? 'OT terminada (' + o.estado_label + ')' : (o.mismo_cliente ? 'Mismo cliente del ticket' : 'Otro cliente: pedirá confirmación'));
          return '<div class="tk-asoc-item t-' + sem + '">'
            + '<div class="tk-asoc-main"><div class="tk-asoc-num">' + e(o.numero_ot) + ' <span class="tk-asoc-est">' + e(o.estado_label || o.estado) + '</span></div>'
            + '<div class="tk-asoc-cli">' + e(o.cliente || 'Sin cliente') + (o.cliente_rut ? ' · ' + e(rutFmt(o.cliente_rut)) : '') + '</div>'
            + (o.titulo ? '<div class="tk-asoc-tit">' + e(o.titulo) + '</div>' : '')
            + '<div class="tk-asoc-meta">' + (o.fecha ? '<span><i class="bi bi-calendar-event"></i>' + e(o.fecha) + '</span>' : '')
            + (o.tecnico ? '<span><i class="bi bi-person-gear"></i>' + e(o.tecnico) + '</span>' : '')
            + '<span class="tk-asoc-aviso"><i class="bi bi-info-circle"></i>' + e(aviso) + '</span></div></div>'
            + '<div class="tk-asoc-acc">'
            + '<a class="tk-gbtn sec chico" href="' + e(o.url) + '" target="_blank" rel="noopener"><i class="bi bi-eye"></i>Ver</a>'
            + (o.otro_ticket ? '' : '<button type="button" class="tk-gbtn rojo chico" data-vid="' + o.id + '" data-num="' + e(o.numero_ot) + '"><i class="bi bi-link-45deg"></i>Asociar</button>')
            + '</div></div>';
        }).join('');
        cont.querySelectorAll('button[data-vid]').forEach(function(b){
          b.addEventListener('click', function(){ asociarOT(parseInt(b.dataset.vid, 10), b.dataset.num, false, ''); });
        });
      })
      .catch(function(){ cont.innerHTML = '<div class="tk-asoc-vacio">Error de red.</div>'; });
  }
  function asociarOT(vid, num, forzar, motivo){
    fetch('/tickets/api/tickets/' + TID + '/asociar-ot', {method: 'POST', credentials: 'same-origin',
      headers: {'Content-Type': 'application/json'}, body: JSON.stringify({visita_id: vid, forzar: !!forzar, motivo: motivo || ''})})
      .then(function(r){ return r.json(); })
      .then(function(j){
        if (j.ok) {
          var m = $('tkAsocOtModal'); if (m && window.bootstrap) window.bootstrap.Modal.getOrCreateInstance(m).hide();
          toast('✓ ' + (j.numero_ot || 'OT') + ' asociada al ticket', 'success');
          recargar();
          return;
        }
        if (j.error_codigo === 'CLIENTE_DISTINTO' && typeof window.ilusPrompt === 'function') {
          window.ilusPrompt({title: 'La OT es de otro cliente', message: j.error,
            placeholder: 'Motivo (mínimo 10 caracteres)', required: true})
            .then(function(mot){ if (mot && mot.trim().length >= 10) asociarOT(vid, num, true, mot.trim()); else if (mot != null) toast('El motivo necesita al menos 10 caracteres.', 'warning'); });
          return;
        }
        toast(j.error || 'No se pudo asociar la OT', 'error');
      })
      .catch(function(){ toast('Error de red', 'error'); });
  }
  function quitarOT(){
    if (typeof window.ilusPrompt !== 'function') return;
    window.ilusPrompt({title: 'Quitar el vínculo con la OT', message: 'La OT seguirá existiendo; solo deja de estar ligada a este ticket. Queda registrado en la actividad.',
      placeholder: 'Motivo (mínimo 10 caracteres)', required: true})
      .then(function(mot){
        if (!mot) return;
        if (mot.trim().length < 10) { toast('El motivo necesita al menos 10 caracteres.', 'warning'); return; }
        fetch('/tickets/api/tickets/' + TID + '/asociar-ot', {method: 'DELETE', credentials: 'same-origin',
          headers: {'Content-Type': 'application/json'}, body: JSON.stringify({motivo: mot.trim()})})
          .then(function(r){ return r.json(); })
          .then(function(j){ if (j.ok) { toast('Vínculo quitado', 'success'); recargar(); } else toast(j.error || 'No se pudo', 'error'); })
          .catch(function(){ toast('Error de red', 'error'); });
      });
  }

  // ── Generar cotización en un modal (el MISMO asistente, en iframe) ─────
  function abrirCotizacion(){
    var m = $('tkCotModal'), fr = $('tkCotFrame');
    var url = '/tickets/cotizaciones?desde_ticket=' + TID;
    if (!m || !fr || !window.bootstrap || window._TK_ES_TECNICO) { window.open(url, '_blank'); return; }
    var ap = $('tkCotAbrirAparte'); if (ap) ap.href = url;
    fr.src = url + '&embed=1';
    window.bootstrap.Modal.getOrCreateInstance(m).show();
  }

  function enlazarUnaVez(){
    if (enlazarUnaVez.hecho) return;
    enlazarUnaVez.hecho = true;
    var bm = $('btnGuardarMotivo');
    if (bm) bm.addEventListener('click', function(){ guardarCampo('motivo_principal', ($('motivoInput').value || '').trim(), bm, '✓ Motivo guardado'); });
    var mi = $('motivoInput');
    if (mi) mi.addEventListener('keydown', function(ev){ if (ev.key === 'Enter') { ev.preventDefault(); if (bm) bm.click(); } });
    var bd = $('btnGuardarDiag');
    if (bd) bd.addEventListener('click', function(){ guardarCampo('diagnostico_solucion', ($('diagTextarea').value || '').trim(), bd, '✓ Diagnóstico guardado'); });
    var bt = $('btnTomarTicket');
    if (bt) bt.addEventListener('click', tomar);
    var ba = $('btnAsociarOT');
    if (ba) ba.addEventListener('click', abrirAsociarOT);
    var bq = $('btnQuitarOT');
    if (bq) bq.addEventListener('click', quitarOT);
    var q = $('tkAsocOtQ');
    if (q) q.addEventListener('input', function(){ clearTimeout(_otTimer); _otTimer = setTimeout(function(){ buscarOT(q.value.trim()); }, 300); });
    // «Generar cotización» de la tarjeta lateral: ahora en modal (gestión);
    // un técnico conserva el comportamiento de siempre (pestaña aparte).
    var bc = $('btnGenerarCotizacionSide');
    if (bc && !window._TK_ES_TECNICO) {
      var nuevo = bc.cloneNode(true);          // quita el listener viejo (window.open) sin tocar tickets_ficha.js
      bc.parentNode.replaceChild(nuevo, bc);
      nuevo.addEventListener('click', abrirCotizacion);
    }
    var cm = $('tkCotModal');
    if (cm) cm.addEventListener('hidden.bs.modal', function(){ var fr = $('tkCotFrame'); if (fr) fr.src = 'about:blank'; recargar(); });
    document.querySelectorAll('.tk-tile[data-ir]').forEach(function(b){
      b.addEventListener('click', function(){
        if (b.id === 'tileCliente' && b.dataset.href) { window.open(b.dataset.href, '_blank', 'noopener'); return; }
        irAPaso(b.dataset.ir);
      });
    });
  }

  window.tkV2AlCargar = function(t, d){
    t = t || {}; d = d || {};
    T = t; D = d;
    enlazarUnaVez();

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

    // 6 · 7 · Motivo y diagnóstico, cabecera, OT, guía
    pintarMotivoDiag(t);
    pintarHero(t, d);
    pintarOT(t);
    pintarTomar(t);
    pintarGuia(d.guia, t);
  };
})();

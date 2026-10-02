/* Guía del retiro (Daniel 2026-10-02) — ver templates/retiros/_guia.html y retiros_guia.py.
   Dibuja los 6 pasos con lo hecho, lo que te toca (en ROJO) y lo que falta; marca en rojo las
   tarjetas y botones pendientes de la ficha; avisa EXACTAMENTE qué falta antes de enviar algo que
   le escribe al cliente; muestra lo que Check (bodega) dice de la preparación.
   Seguridad: «Confirmo» solo deja un registro interno; nada de aquí escribe al cliente. Check solo se consulta. */
(function () {
  'use strict';
  var panel = document.getElementById('gpPanel');
  var dataEl = document.getElementById('gpData');
  if (!panel || !dataEl) return;
  var RID = panel.dataset.rid;
  var STATUS = panel.dataset.status || '';
  var G;
  try { G = JSON.parse(dataEl.textContent); } catch (e) { return; }
  var CHECK = null;          // última respuesta de /check-preparacion
  var CHECK_TS = null;
  var CHECK_CARGANDO = false;
  var AVISO_ABIERTO = false;
  var ULTIMO_DIBUJO = '';

  var CARDS = { 1: '#paso-2', 2: '#paso-resp', 3: '#paso-3', 4: '#paso-4', 5: '#paso-confirmacion', 6: '#paso-confirmacion' };
  var BTNS = { 2: '#btnTomarRetiro', 4: '#btnAbrirProponerFecha', 5: '#formEnviarPreparacion .cierre-next-btn', 6: '.cierre-next-btn.is-green' };
  var GUARDS = [
    { sel: '#btnAbrirProponerFecha', n: 4 },
    { sel: '#formEnviarPreparacion .cierre-next-btn', n: 5 },
    { sel: '.cierre-next-btn.is-green', n: 6 }
  ];
  var ESTADO_TXT = {
    hecho: ['b-hecho', 'bi-check-circle-fill', 'Hecho'],
    actual: ['b-actual', 'bi-exclamation-circle-fill', 'Te toca'],
    espera: ['b-espera', 'bi-hourglass-split', 'Esperando'],
    pendiente: ['b-pendiente', 'bi-lock-fill', 'Aún no toca'],
    bloqueado: ['b-bloqueado', 'bi-slash-circle-fill', 'No se puede todavía']
  };

  function esc(s) {
    return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
    });
  }
  function $(sel, root) { return (root || document).querySelector(sel); }
  function toast(msg, tipo) { if (typeof window.ilusToast === 'function') window.ilusToast(msg, { type: tipo || 'info' }); }
  function visible(el) { return !!(el && (el.offsetWidth || el.offsetHeight || el.getClientRects().length)); }

  // ── Qué falta antes de un paso ──────────────────────────────────────────
  function faltantesAntes(n) {
    var lista = (G.previos && G.previos[String(n)]) || [];
    return lista.map(function (par) {
      var p = G.pasos[par[0] - 1];
      return { n: par[0], titulo: p.titulo, texto: (par[1] && par[1][0]) || ('Falta terminar el paso ' + par[0] + '.'), estado: p.estado };
    });
  }
  function anclaDe(p) {
    if (p.ancla && $(p.ancla)) return p.ancla;
    return CARDS[p.n] || p.ancla;
  }
  function irA(sel, resaltar) {
    var el = $(sel);
    if (!el) return false;
    el.scrollIntoView({ behavior: 'smooth', block: 'start' });
    if (resaltar !== false) {
      el.classList.remove('gp-resalta'); void el.offsetWidth; el.classList.add('gp-resalta');
      setTimeout(function () { el.classList.remove('gp-resalta'); }, 1800);
    }
    return true;
  }
  // Navegar a un paso: desplaza y resalta; si aún no existe esa tarjeta, lo dice (nada de clics mudos).
  function irAlPaso(n, conAviso) {
    var p = G.pasos[n - 1];
    var ant = conAviso ? faltantesAntes(n) : [];
    if (!irA(anclaDe(p))) {
      var motivo = (p.faltan && p.faltan[0]) || 'Esa parte de la ficha aparece cuando se termina el paso anterior.';
      toast('Todavía no está disponible: ' + motivo, 'warning');
      if (ant.length) irA(anclaDe(G.pasos[ant[0].n - 1]));
      return;
    }
    if (ant.length && p.estado !== 'hecho') {
      toast('Ojo, antes falta el paso ' + ant[0].n + ' (' + ant[0].titulo + '): ' + ant[0].texto, 'warning');
    }
  }

  // ── Aviso «antes falta…» (solo para los botones que le escriben al cliente) ──────────────
  function avisoFalta(n, items, duro) {
    return new Promise(function (resolve) {
      if (AVISO_ABIERTO) { resolve(null); return; }
      AVISO_ABIERTO = true;
      var p = G.pasos[n - 1];
      var el = document.createElement('div');
      el.className = 'modal fade gp-modal';
      el.tabIndex = -1;
      el.setAttribute('aria-hidden', 'true');
      var propios = duro ? p.faltan : [];
      var lista = items.map(function (it) {
        return '<li><span class="n">' + it.n + '</span><div><b>Paso ' + it.n + ' · ' + esc(it.titulo) + '</b><span>' + esc(it.texto) + '</span></div></li>';
      }).join('') + propios.map(function (t) {
        return '<li><span class="n"><i class="bi bi-x-lg"></i></span><div><b>Paso ' + n + ' · ' + esc(p.titulo) + '</b><span>' + esc(t) + '</span></div></li>';
      }).join('');
      var primero = items.length ? items[0].n : n;
      el.innerHTML =
        '<div class="modal-dialog modal-dialog-centered"><div class="modal-content">' +
        '<div class="modal-header"><h5 class="modal-title"><i class="bi bi-exclamation-octagon-fill text-danger me-2"></i>' +
        (duro ? 'Todavía no se puede' : 'Antes del paso ' + n + ' falta:') + '</h5>' +
        '<button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Cerrar"></button></div>' +
        '<div class="modal-body"><p class="mb-3" style="font-size:1.05rem">' +
        (duro ? 'Este paso no se puede hacer hasta resolver:' : 'Para que el retiro salga ordenado, primero termina esto:') +
        '</p><ol class="gp-modal-lista">' + lista + '</ol></div>' +
        '<div class="modal-footer">' +
        '<button type="button" class="gp-btn" data-r="ir"><i class="bi bi-arrow-left-circle-fill"></i>' +
        (items.length ? 'Ir al paso ' + primero : 'Entendido') + '</button>' +
        (duro ? '' : '<button type="button" class="gp-btn sec" data-r="igual">Continuar de todos modos</button>') +
        '</div></div></div>';
      document.body.appendChild(el);
      var res = null;
      var m = window.bootstrap ? window.bootstrap.Modal.getOrCreateInstance(el) : null;
      var cerrar = function () { AVISO_ABIERTO = false; el.remove(); resolve(res); };
      el.addEventListener('click', function (ev) {
        var b = ev.target.closest && ev.target.closest('[data-r]');
        if (!b) return;
        res = b.dataset.r;
        if (m) m.hide(); else cerrar();
      });
      el.addEventListener('hidden.bs.modal', cerrar);
      if (m) m.show(); else { el.style.display = 'block'; el.classList.add('show'); }
    });
  }
  function guardar(n, hacer) {
    var p = G.pasos[n - 1];
    var ant = faltantesAntes(n).filter(function (it) { return it.n !== n; });
    var duro = !!(p.bloquea && p.faltan.length);
    if (!ant.length && !duro) { hacer(); return; }
    avisoFalta(n, ant, duro).then(function (r) {
      if (r === 'igual' && !duro) hacer();
      else if (r === 'ir' && ant.length) irAlPaso(ant[0].n, false);
    });
  }
  // Intercepta (en captura) los botones que más importan para avisar antes, sin tocar sus funciones.
  window.addEventListener('click', function (ev) {
    if (!ev.target || !ev.target.closest) return;
    for (var i = 0; i < GUARDS.length; i++) {
      var g = GUARDS[i];
      var b = ev.target.closest(g.sel);
      if (!b) continue;
      if (b.dataset.gpOk === '1') return;
      var p = G.pasos[g.n - 1];
      var ant = faltantesAntes(g.n);
      if (!ant.length && !(p.bloquea && p.faltan.length)) return;
      ev.preventDefault();
      ev.stopImmediatePropagation();
      guardar(g.n, function () {
        b.dataset.gpOk = '1';
        b.click();
        setTimeout(function () { delete b.dataset.gpOk; }, 800);
      });
      return;
    }
  }, true);

  // ── Dibujo ───────────────────────────────────────────────────────────────
  function listaFaltan(p) {
    if (!p.faltan.length) return '';
    return '<ul class="gp-faltan">' + p.faltan.map(function (t) {
      var ic = p.estado === 'espera' ? 'bi-hourglass-split' : (p.estado === 'actual' ? 'bi-arrow-right-circle-fill' : 'bi-info-circle-fill');
      return '<li><i class="bi ' + ic + '"></i><span>' + esc(t) + '</span></li>';
    }).join('') + '</ul>';
  }
  function detalleHtml(p) {
    if (!p.detalle || !p.detalle.length || p.estado === 'hecho' || (p.n !== 1 && p.n !== 3)) return '';
    return '<div class="gp-detalle"><b>' + (p.n === 1 ? 'Documentos que vas a confirmar' : 'Productos que vas a confirmar') + ' (' + p.detalle.length + ')</b><ul>' +
      p.detalle.map(function (t) { return '<li>' + esc(t) + '</li>'; }).join('') + '</ul></div>';
  }
  function botones(p) {
    if (!p.accion) return '';
    var a = p.accion;
    var cls = 'gp-btn' + (p.estado === 'actual' ? '' : ' sec') + (a.tipo === 'retirar' && p.estado === 'actual' ? ' verde' : '');
    var dis = a.deshabilitada ? ' disabled title="Primero confirma las facturas (paso 1)"' : '';
    var icono = { confirmar_docs: 'bi-patch-check-fill', confirmar_productos: 'bi-patch-check-fill', tomar: 'bi-person-raised-hand',
      proponer: 'bi-calendar2-plus', preparacion: 'bi-box-seam-fill', retirar: 'bi-check-circle-fill', ir: 'bi-arrow-right-circle-fill' }[a.tipo] || 'bi-arrow-right-circle-fill';
    return '<div class="gp-acc"><button type="button" class="' + cls + '" data-gp-acc="' + p.n + '"' + dis + '><i class="bi ' + icono + '"></i>' + esc(a.texto) + '</button></div>';
  }
  function pasoHtml(p) {
    var st = ESTADO_TXT[p.estado] || ESTADO_TXT.pendiente;
    var esSig = p.n === G.siguiente && p.estado === 'actual';   // ya está explicado arriba, en «Tu siguiente paso»
    var h = '<li class="gp-paso is-' + p.estado + '" data-n="' + p.n + '">';
    h += '<span class="gp-circ">' + (p.estado === 'hecho' ? '<i class="bi bi-check-lg"></i>' : p.n) + '</span><div>';
    h += '<div class="gp-ptop"><h5 class="gp-ptit">Paso ' + p.n + ' · ' + esc(p.titulo) + '</h5>' +
      '<span class="gp-badge ' + st[0] + '"><i class="bi ' + st[1] + '"></i>' + st[2] + '</span></div>';
    if (p.estado !== 'hecho' && !esSig) h += '<p class="gp-pq">' + esc(p.pregunta) + '</p>';
    if (p.resumen) h += '<p class="gp-res"><i class="bi bi-check2-circle"></i> ' + esc(p.resumen) + '</p>';
    if (!esSig) h += listaFaltan(p);
    if (p.avisos && p.avisos.length && p.estado !== 'hecho') {
      h += '<ul class="gp-avisos">' + p.avisos.map(function (t) { return '<li><i class="bi bi-exclamation-triangle-fill"></i><span>' + esc(t) + '</span></li>'; }).join('') + '</ul>';
    }
    if (!esSig) h += detalleHtml(p);
    if (p.correo && p.estado !== 'hecho' && !esSig) h += '<span class="gp-correo"><i class="bi bi-envelope-fill"></i>Esta acción le envía un correo al cliente</span>';
    if (!esSig) {
      var b = botones(p);
      if (!b && p.estado !== 'hecho') b = '<div class="gp-acc"><button type="button" class="gp-btn sec" data-gp-ver="' + p.n + '"><i class="bi bi-arrow-right-circle"></i>Ir a este paso</button></div>';
      h += b;
    }
    if (p.n === 5) h += checkHtml(p);
    h += '</div></li>';
    return h;
  }
  function sigHtml() {
    if (G.terminal) {
      return '<div class="gp-sig is-pendiente"><div class="gp-sig-k"><i class="bi bi-flag-fill"></i>Retiro terminado</div><h4>' + esc(G.terminal) + '</h4><p class="gp-preg mb-0">No queda nada por hacer en este retiro.</p></div>';
    }
    if (!G.siguiente) {
      return '<div class="gp-sig is-fin"><div class="gp-sig-k"><i class="bi bi-trophy-fill"></i>Retiro completado</div><h4>¡Todos los pasos están hechos!</h4><p class="gp-preg mb-0">El cliente ya se llevó su pedido.</p></div>';
    }
    var p = G.pasos[G.siguiente - 1];
    var k = { actual: ['Tu siguiente paso', 'bi-signpost-2-fill'], espera: ['Ahora toca esperar', 'bi-hourglass-split'],
      pendiente: ['Siguiente paso (aún no toca)', 'bi-lock-fill'], bloqueado: ['No se puede avanzar todavía', 'bi-slash-circle-fill'] }[p.estado] || ['Siguiente paso', 'bi-signpost-2-fill'];
    var h = '<div class="gp-sig is-' + p.estado + '" aria-live="polite"><div class="gp-sig-k"><i class="bi ' + k[1] + '"></i>' + k[0] + '</div>';
    h += '<h4>Paso ' + p.n + ' · ' + esc(p.titulo) + '</h4><p class="gp-preg">' + esc(p.pregunta) + '</p>';
    h += listaFaltan(p);
    if (p.avisos && p.avisos.length) {
      h += '<ul class="gp-avisos">' + p.avisos.map(function (t) { return '<li><i class="bi bi-exclamation-triangle-fill"></i><span>' + esc(t) + '</span></li>'; }).join('') + '</ul>';
    }
    h += detalleHtml(p);                       // lo que se va a confirmar, ANTES del botón «Confirmo»
    if (p.correo && p.estado === 'actual') h += '<span class="gp-correo"><i class="bi bi-envelope-fill"></i>Esta acción le envía un correo al cliente</span>';
    var b = botones(p);
    if (!b && p.estado !== 'hecho') b = '<div class="gp-acc"><button type="button" class="gp-btn sec" data-gp-ver="' + p.n + '"><i class="bi bi-arrow-right-circle"></i>Ir a este paso</button></div>';
    return h + b + '</div>';
  }
  function checkHtml(p5) {
    if (STATUS !== 'agenda_confirmada' && STATUS !== 'en_preparacion') return '';
    if (!G.pasos[0].detalle || !G.pasos[0].detalle.length) return '';
    var h = '<div class="gp-check" data-gp-noclick="1"><div class="gp-check-h"><i class="bi bi-box-seam-fill"></i><b>Preparación en Check (bodega)</b>' +
      '<button type="button" class="gp-btn sec" data-gp-check-refresh="1"' + (CHECK_CARGANDO ? ' disabled' : '') + '><i class="bi bi-arrow-clockwise"></i>' + (CHECK_CARGANDO ? 'Consultando…' : 'Actualizar') + '</button></div>';
    if (!CHECK) {
      return h + '<p class="gp-check-frase"><span class="spinner-border spinner-border-sm me-2"></span>Consultando a Check qué tan avanzada va la preparación…</p></div>';
    }
    var ev = CHECK.evaluacion || {};
    h += '<p class="gp-check-frase ' + (ev.listo ? 'ok' : '') + '">' + (ev.listo ? '<i class="bi bi-check-circle-fill"></i> ' : '') + esc(ev.frase || '') + '</p>';
    if (ev.alerta) h += '<p class="gp-check-alerta"><i class="bi bi-exclamation-triangle-fill"></i> ' + esc(ev.alerta) + '</p>';
    if (ev.etapas && ev.etapas.length) {
      h += '<ul class="gp-check-et">' + ev.etapas.map(function (e) {
        var pct = e.de ? Math.min(100, Math.round(100 * e.hechas / e.de)) : 0;
        var extra = (e.clave === 'revisado' || e.clave === 'despachado');
        return '<li class="' + (e.completa ? 'is-ok ' : '') + (e.clave === 'pickeado' ? 'is-clave ' : '') + (extra ? 'is-extra' : '') + '"><span class="gp-et-n">' +
          (e.completa ? '<i class="bi bi-check-lg"></i>' : '<i class="bi bi-circle"></i>') + '</span>' +
          '<div class="gp-et-t">' + esc(e.titulo) + '<small>' + esc(e.texto) + (e.clave === 'pickeado' ? ' <b>Esta etapa es la que marca «preparado».</b>' : '') +
          (extra ? ' No hace falta para marcar preparado.' : '') + '</small></div>' +
          '<span class="gp-et-c">' + e.hechas + ' de ' + e.de + '</span><div class="gp-check-bar"><i style="width:' + pct + '%"></i></div></li>';
      }).join('') + '</ul>';
    }
    if (CHECK.documentos && CHECK.documentos.length) {
      h += '<p class="gp-check-docs"><b>Documentos:</b> ' + CHECK.documentos.map(function (d) {
        var t = d.estado === 'listo' ? 'listo' : (d.estado === 'sin_datos' ? 'Check aún no lo tiene' : 'en proceso');
        return esc(d.rotulo) + ' (' + t + ')';
      }).join(' · ') + '</p>';
    }
    var nota;
    if (STATUS !== 'en_preparacion') nota = 'Cuando envíes el pedido a preparación, ILUS empezará a marcar solo el «listo» según Check. Por ahora solo se consulta.';
    else if (CHECK.auto_activo === false) nota = 'El marcado automático está apagado: Check solo informa. Marca la lista de abajo a mano.';
    else nota = 'Cuando Check tenga todo pickeado (confirmado en dos revisiones seguidas), ILUS marca solo «Pedido listo para entregar». No se le envía ningún correo al cliente. Check solo se consulta, nunca se modifica.';
    h += '<p class="gp-check-nota">' + nota + (CHECK_TS ? ' Actualizado a las ' + CHECK_TS + '.' : '') + '</p></div>';
    return h;
  }
  function render() {
    var hechos = G.pasos.filter(function (p) { return p.estado === 'hecho'; }).length;
    var pct = Math.round(100 * hechos / G.pasos.length);
    var h = '<div class="gp-head"><div class="gp-head-t"><i class="bi bi-signpost-split-fill"></i><div><h3>Guía del retiro</h3>' +
      '<p>Sigue los 6 pasos en orden. <b>En rojo está lo que te toca hacer ahora.</b></p></div></div>' +
      '<div class="gp-prog"><b>' + hechos + ' de ' + G.pasos.length + ' pasos listos</b><div class="gp-prog-bar"><i style="width:' + pct + '%"></i></div></div></div>';
    h += sigHtml();
    h += '<ol class="gp-lista">' + G.pasos.map(pasoHtml).join('') + '</ol>';
    if (h === ULTIMO_DIBUJO) return;           // nada cambió: no se redibuja (así no se pierde el foco)
    var foco = document.activeElement;
    var clave = '';
    if (foco && panel.contains(foco)) {
      if (foco.getAttribute('data-gp-acc')) clave = '[data-gp-acc="' + foco.getAttribute('data-gp-acc') + '"]';
      else if (foco.getAttribute('data-gp-ver')) clave = '[data-gp-ver="' + foco.getAttribute('data-gp-ver') + '"]';
      else if (foco.hasAttribute('data-gp-check-refresh')) clave = '[data-gp-check-refresh]';
    }
    panel.innerHTML = h;
    ULTIMO_DIBUJO = h;
    if (clave) { var nuevo = $(clave, panel); if (nuevo && !nuevo.disabled) nuevo.focus({ preventScroll: true }); }
  }

  // ── Marcas sobre las tarjetas de la ficha ───────────────────────────────
  function decorar() {
    [].forEach.call(document.querySelectorAll('.step-section'), function (e) {
      e.classList.remove('gp-roja', 'gp-ambar', 'gp-e-hecho', 'gp-e-actual', 'gp-e-espera', 'gp-e-pendiente', 'gp-e-bloqueado');
    });
    [].forEach.call(document.querySelectorAll('.gp-tag'), function (e) { e.remove(); });
    [].forEach.call(document.querySelectorAll('.gp-btn-roja'), function (e) { e.classList.remove('gp-btn-roja'); });
    if (G.terminal) return;
    var usadas = {};
    G.pasos.forEach(function (p) {
      var card = $(anclaDe(p));
      if (!card || usadas[card.id]) return;
      // En la tarjeta compartida (preparación y entrega) manda el primer paso que no esté hecho.
      if (p.n === 6 && G.pasos[4].estado !== 'hecho') return;
      usadas[card.id] = true;
      card.classList.add('gp-e-' + p.estado);
      if (p.estado !== 'actual' && p.estado !== 'espera') return;
      card.classList.add(p.estado === 'actual' ? 'gp-roja' : 'gp-ambar');
      var tag = document.createElement('div');
      tag.className = 'gp-tag' + (p.estado === 'espera' ? ' is-espera' : '');
      var acc = p.accion && (p.accion.tipo === 'confirmar_docs' || p.accion.tipo === 'confirmar_productos') && p.estado === 'actual' && !p.accion.deshabilitada;
      tag.innerHTML = '<span><b>PASO ' + p.n + ' · ' + (p.estado === 'actual' ? 'TE TOCA' : 'ESPERANDO') + ':</b> ' + esc(p.faltan[0] || p.titulo) + '</span>' +
        (acc ? '<button type="button" class="gp-btn" data-gp-acc="' + p.n + '"><i class="bi bi-patch-check-fill"></i>' + esc(p.accion.texto) + '</button>' : '');
      card.insertBefore(tag, card.firstChild);
      if (p.estado === 'actual' && BTNS[p.n]) {
        var b = $(BTNS[p.n]);
        if (b && visible(b)) b.classList.add('gp-btn-roja');
      }
    });
  }

  // ── Acciones ─────────────────────────────────────────────────────────────
  function aplicar(d) {
    G = d;
    if (d.status) { STATUS = d.status; panel.dataset.status = d.status; }
    render(); decorar();
  }
  function postear(ruta, btn, firma, okMsg) {
    if (btn) btn.disabled = true;
    return fetch('/retiros/' + RID + '/' + ruta, {
      method: 'POST', credentials: 'same-origin',
      headers: { 'Accept': 'application/json', 'Content-Type': 'application/json', 'X-Requested-With': 'XMLHttpRequest' },
      body: JSON.stringify({ firma: firma || null })
    }).then(function (r) { return r.json().then(function (d) { return { ok: r.ok && d && d.ok, d: d }; }); })
      .then(function (x) {
        if (!x.ok) {
          toast((x.d && x.d.error) || 'No se pudo guardar. Reintenta.', 'error');
          if (btn) btn.disabled = false;
          refrescar();                          // por si la lista cambió: se muestra la vigente
          return;
        }
        aplicar(x.d);
        toast(okMsg, 'success');
      })
      .catch(function (e) { toast('Sin conexión: ' + e.message, 'error'); if (btn) btn.disabled = false; });
  }
  function accionar(n, btn) {
    var p = G.pasos[n - 1];
    var a = p.accion;
    if (!a) return;
    if (a.tipo === 'confirmar_docs') return postear('confirmar-docs', btn, G.firma_docs, '✓ Facturas confirmadas. Queda registrado quién y cuándo. No se envía nada al cliente.');
    if (a.tipo === 'confirmar_productos') return postear('confirmar-productos', btn, G.firma_prod, '✓ Productos confirmados. Queda registrado quién y cuándo. No se envía nada al cliente.');
    if (a.tipo === 'tomar') {
      var t = $('#btnTomarRetiro');
      if (!t) { irAlPaso(2, false); return; }
      irA('#paso-resp');
      if (typeof window.tomarRetiro === 'function') window.tomarRetiro(t);
      return;
    }
    var real = BTNS[n] && $(BTNS[n]);
    var activar = function (ancla) {
      if (!irA(ancla)) { irAlPaso(n, false); return; }
      if (real && visible(real)) real.click();
      else toast('Ese botón todavía no está disponible: ' + (p.faltan[0] || 'falta terminar un paso anterior.'), 'warning');
    };
    if (a.tipo === 'proponer') return activar('#paso-4');
    if (a.tipo === 'preparacion') return activar('#paso-confirmacion');
    if (a.tipo === 'retirar') {
      irA('#paso-confirmacion');
      if (real && visible(real)) real.click();
      else if (typeof window.abrirModalRetirar === 'function') window.abrirModalRetirar();
      return;
    }
    irAlPaso(n, false);
  }
  panel.addEventListener('click', function (ev) {
    var t = ev.target;
    if (!t.closest) return;
    var chk = t.closest('[data-gp-check-refresh]');
    if (chk) { cargarCheck(true); return; }
    var b = t.closest('[data-gp-acc]');
    if (b) { ev.stopPropagation(); accionar(parseInt(b.dataset.gpAcc, 10), b); return; }
    var v = t.closest('[data-gp-ver]');
    if (v) { irAlPaso(parseInt(v.dataset.gpVer, 10), true); return; }
    if (t.closest('[data-gp-noclick]')) return;
    var li = t.closest('.gp-paso');
    if (li) irAlPaso(parseInt(li.dataset.n, 10), true);
  });
  // Los botones «Confirmo» que viven en la franja roja de cada tarjeta
  document.addEventListener('click', function (ev) {
    var b = ev.target.closest && ev.target.closest('.gp-tag [data-gp-acc]');
    if (b) accionar(parseInt(b.dataset.gpAcc, 10), b);
  });

  // ── Refrescos ────────────────────────────────────────────────────────────
  function refrescar() {
    fetch('/retiros/' + RID + '/guia', { credentials: 'same-origin', cache: 'no-store' })
      .then(function (r) { return r.json(); })
      .then(function (d) { if (d && d.ok) aplicar(d); })
      .catch(function () { /* sin red: se queda como está */ });
  }
  function relevanteCheck() {
    return (STATUS === 'agenda_confirmada' || STATUS === 'en_preparacion') && G.pasos[0].detalle && G.pasos[0].detalle.length && !G.terminal;
  }
  function msgCheckCaido() {
    return { evaluacion: { frase: 'No pudimos consultar Check ahora. Usa la lista manual de abajo.', etapas: [] }, documentos: [] };
  }
  function cargarCheck(manual) {
    if (!relevanteCheck()) return;
    if (CHECK_CARGANDO) { if (manual) toast('Ya estoy consultando a Check, un momento…', 'info'); return; }
    CHECK_CARGANDO = true;
    if (manual) render();
    var ctrl = window.AbortController ? new AbortController() : null;
    var corte = setTimeout(function () { if (ctrl) ctrl.abort(); }, 30000);
    fetch('/retiros/' + RID + '/check-preparacion' + (STATUS === 'en_preparacion' ? '' : '?solo_lectura=1'),
      { credentials: 'same-origin', cache: 'no-store', signal: ctrl ? ctrl.signal : undefined })
      .then(function (r) { return r.json(); })
      .then(function (d) {
        clearTimeout(corte);
        CHECK_CARGANDO = false;
        if (!d || !d.ok) CHECK = msgCheckCaido();
        else { CHECK = d; CHECK_TS = new Date().toLocaleTimeString('es-CL', { hour: '2-digit', minute: '2-digit' }); }
        if (d && d.aplicado_ahora) {
          toast('✓ Check confirmó que bodega ya juntó todo: pedido LISTO para entregar.', 'success');
          if (typeof window._wmsCargar === 'function') { try { window._wmsCargar(); } catch (e) { /* la lista se actualiza al recargar */ } }
        }
        render(); decorar();
        if (d && d.ok && (d.aplicado_ahora || (d.preparado && STATUS === 'en_preparacion' && G.pasos[4].estado !== 'hecho'))) refrescar();
      })
      .catch(function () {
        clearTimeout(corte);
        CHECK_CARGANDO = false;
        CHECK = msgCheckCaido();
        render(); decorar();
      });
  }

  // Cuando cualquier acción de la ficha cambia algo de ESTE retiro, la guía se actualiza sola.
  var _fetch = window.fetch;
  window.fetch = function (input, init) {
    var p = _fetch.apply(this, arguments);
    try {
      var url = typeof input === 'string' ? input : ((input && input.url) || '');
      var metodo = ((init && init.method) || (input && input.method) || 'GET').toUpperCase();
      if (metodo !== 'GET' && url.indexOf('/retiros/' + RID + '/') >= 0 && !/\/(guia|check-preparacion|confirmar-)/.test(url)) {
        p.then(function () { setTimeout(refrescar, 700); }, function () { });
      }
    } catch (e) { /* nunca romper el fetch original */ }
    return p;
  };

  render();
  decorar();
  cargarCheck(false);
  setInterval(function () {
    if (document.visibilityState !== 'visible') return;
    refrescar();
    cargarCheck(false);
  }, 60000);
  document.addEventListener('visibilitychange', function () {
    if (document.visibilityState === 'visible') { refrescar(); cargarCheck(false); }
  });
  window.gpRefrescar = refrescar;
})();

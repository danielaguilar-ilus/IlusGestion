/* ══════════════════════════════════════════════════════════════════════════════════════════════════════════════
   inquietudes.js — Monitor y ficha de «Inquietudes de clientes» (solo gestión).
   2026-10-26. Modo «monitor»: el botón «Nueva inquietud» (cliente → equipo con foto del catálogo → texto → fotos opcionales).
   Modo «ficha»: las acciones (adjudicarme, asignar, clasificar, editar, cerrar, anular, reabrir, borrar —solo superadmin—, nota, levantar
   ticket, ligar ticket/OT, marcar pasos). TODA acción pregunta antes con los modales de ILUS (REGLA #1: nada de alert/confirm/prompt nativos)
   y el servidor vuelve a validar. Nada se le escribe al cliente (REGLA #22). API: /ot/inquietudes/api/…
   ══════════════════════════════════════════════════════════════════════════════════════════════════════════════ */
(function () {
  'use strict';
  var CFG = window.INQ_CFG || {};
  var BASE = '/ot/inquietudes/api/';

  function esc(s) {
    return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) { return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]; });
  }
  function $(id) { return document.getElementById(id); }
  function toast(m, t, d) { try { if (window.ilusToast) window.ilusToast(m, { type: t || 'info', duration: d || 5000 }); } catch (e) { /* nada */ } }
  function post(url, cuerpo) {
    var init = { method: 'POST', credentials: 'same-origin', headers: { 'X-Requested-With': 'XMLHttpRequest', 'Accept': 'application/json' } };
    if (cuerpo instanceof FormData) init.body = cuerpo;
    else { init.headers['Content-Type'] = 'application/json'; init.body = JSON.stringify(cuerpo || {}); }
    return fetch(url, init).then(function (r) {
      return r.json().catch(function () { return {}; }).then(function (j) {
        if (!j.ok && !j.error) j.error = r.status === 403 ? 'No tienes permiso para esta acción.' : 'No se pudo completar. Intenta de nuevo.';
        return j;
      });
    }).catch(function () { return { ok: false, error: 'Sin conexión. Revisa la señal e intenta de nuevo.' }; });
  }
  function get(url) {
    return fetch(url, { credentials: 'same-origin', headers: { 'Accept': 'application/json' } }).then(function (r) { return r.json().catch(function () { return {}; }); })
      .catch(function () { return { ok: false, error: 'Sin conexión.' }; });
  }
  function aviso(titulo, mensaje) {
    if (window.ilusAlert) return window.ilusAlert({ title: titulo, message: mensaje, type: 'warning' });
    toast(mensaje, 'warning', 8000);
    return Promise.resolve(true);
  }
  function recargar() { window.location.reload(); }

  /* ══════════════ FICHA ══════════════ */
  function ficha() {
    var ID = CFG.id;
    var U = BASE + ID + '/';

    function correr(url, cuerpo, ok) {
      return post(url, cuerpo).then(function (j) {
        if (!j || !j.ok) { aviso('No se pudo hacer', (j && j.error) || 'Intenta de nuevo.'); return j; }
        if (j.volver) { toast(j.mensaje || 'Listo', 'success'); window.location.href = j.volver; return j; }
        toast((ok || j.mensaje || 'Listo'), 'success');
        setTimeout(recargar, 450);
        return j;
      });
    }
    function pedirTexto(opts) {
      return window.ilusPrompt(Object.assign({ required: true, multiline: true, minLength: 10, okLabel: 'Guardar', cancelLabel: 'Cancelar' }, opts));
    }

    var acciones = {
      adjudicar: function () {
        window.ilusConfirm({ title: 'Adjudicarme la inquietud', message: '¿Quieres hacerte cargo de ' + CFG.numero + '?',
          sub: 'Queda a tu nombre y cumple la primera respuesta del plazo.', okLabel: 'Sí, adjudicármela', cancelLabel: 'Cancelar', type: 'question' })
          .then(function (ok) { if (ok) correr(U + 'adjudicar', {}); });
      },
      asignar: function () {
        var sel = $('iqfResp'), uid = sel && sel.value;
        if (!uid) { aviso('Elige a una persona', 'Escoge de la lista a quien quedará a cargo.'); return; }
        var nombre = sel.options[sel.selectedIndex].text;
        window.ilusConfirm({ title: 'Cambiar el responsable', message: '¿Dejar a ' + nombre + ' a cargo de ' + CFG.numero + '?',
          sub: 'Le llega un aviso por la campana. Al cliente no se le avisa nada.', okLabel: 'Sí, asignar', cancelLabel: 'Cancelar', type: 'question' })
          .then(function (ok) { if (ok) correr(U + 'asignar', { user_id: parseInt(uid, 10) }); });
      },
      clasificar: function () {
        var sel = $('iqfTipo');
        correr(U + 'clasificar', { tipo: sel.value }, 'Guardado: ahora el recorrido es el de «' + sel.options[sel.selectedIndex].text + '».');
      },
      editar: function () {
        var m = $('iqfEditar');
        if (!m) return;
        $('iqfEdErr').hidden = true;
        var bs = bootstrap.Modal.getOrCreateInstance(m);
        bs.show();
        $('iqfEdOk').onclick = function () {
          var motivo = ($('iqfEdMot').value || '').trim();
          if (motivo.length < 10) { var e = $('iqfEdErr'); e.textContent = 'Cuenta por qué lo corriges (mínimo 10 caracteres).'; e.hidden = false; return; }
          post(U + 'editar', { inquietud: $('iqfEdInq').value, diagnostico: $('iqfEdDia').value, motivo: motivo }).then(function (j) {
            if (!j.ok) { var e2 = $('iqfEdErr'); e2.textContent = j.error || 'No se pudo guardar.'; e2.hidden = false; return; }
            bs.hide(); toast('Cambios guardados', 'success'); setTimeout(recargar, 400);
          });
        };
      },
      cerrar: function () {
        pedirTexto({ title: 'Cerrar ' + CFG.numero, message: 'Escribe la resolución: qué se hizo o se decidió.', placeholder: 'Ej: Se cambió el rodamiento y el equipo quedó funcionando.', okLabel: 'Cerrar' })
          .then(function (t) { if (t) correr(U + 'cerrar', { resolucion: t }); });
      },
      anular: function () {
        pedirTexto({ title: 'Anular ' + CFG.numero, message: '¿Por qué no procede? Queda escrito en el registro.', placeholder: 'Ej: Era un comentario sobre otro cliente.', okLabel: 'Anular' })
          .then(function (t) { if (t) correr(U + 'anular', { motivo: t }); });
      },
      reabrir: function () {
        pedirTexto({ title: 'Reabrir ' + CFG.numero, message: '¿Por qué la reabres? El plazo de resolución parte de nuevo.', placeholder: 'Ej: El cliente volvió a llamar por lo mismo.', okLabel: 'Reabrir' })
          .then(function (t) { if (t) correr(U + 'reabrir', { motivo: t }); });
      },
      borrar: function () {
        window.ilusConfirm({ title: 'Borrar la inquietud', message: 'Solo el superadministrador puede borrar. ¿Seguro que quieres borrar ' + CFG.numero + '?',
          sub: 'Desaparece del monitor, pero el borrado queda escrito en el registro con tu nombre y el motivo.', okLabel: 'Sí, borrar', cancelLabel: 'No', danger: true, type: 'danger' })
          .then(function (ok) {
            if (!ok) return;
            pedirTexto({ title: 'Motivo del borrado', message: 'Escribe por qué la borras (obligatorio).', placeholder: 'Ej: Duplicada de INQ-00012.', okLabel: 'Borrar', danger: true })
              .then(function (t) { if (t) correr(U + 'borrar', { motivo: t }); });
          });
      },
      nota: function () {
        window.ilusPrompt({ title: 'Agregar una nota', message: 'Queda en el registro de actividad con tu nombre y la hora.', required: true, multiline: true, minLength: 3, okLabel: 'Guardar nota' })
          .then(function (t) { if (t) correr(U + 'nota', { texto: t }, 'Nota guardada'); });
      },
      ticket: function () {
        window.ilusConfirm({ title: 'Levantar un ticket', message: '¿Quieres levantar un ticket con esta inquietud?',
          sub: 'Es un ticket interno: NO se le escribe al cliente. La inquietud queda «Escalada» y a tu cargo si nadie la tenía.', okLabel: 'Sí, levantar ticket', cancelLabel: 'Todavía no', type: 'question' })
          .then(function (ok) { if (ok) correr(U + 'ticket', { confirmar: true }, 'Ticket levantado'); });
      },
      vincular: function (b) {
        var que = b.getAttribute('data-que');
        window.ilusPrompt({ title: que === 'ot' ? 'Ligar una OT' : 'Ligar un ticket', message: 'Escribe el número (' + (que === 'ot' ? 'OT-2026-00123' : 'TK-2026-00123') + '). Debe ser del mismo cliente.',
          required: true, placeholder: que === 'ot' ? 'OT-2026-00123' : 'TK-2026-00123', okLabel: 'Ligar' })
          .then(function (t) { if (t) correr(U + 'vincular', { que: que, referencia: t }, 'Ligada'); });
      },
      paso: function (b) {
        correr(U + 'paso', { clave: b.getAttribute('data-clave'), hecho: b.getAttribute('data-hecho') === '1' }, 'Paso actualizado');
      }
    };

    document.addEventListener('click', function (e) {
      var b = e.target.closest && e.target.closest('[data-inq]');
      if (!b) return;
      var f = acciones[b.getAttribute('data-inq')];
      if (f) { e.preventDefault(); f(b); }
    });
  }

  /* ══════════════ MONITOR: nueva inquietud ══════════════ */
  function monitor() {
    var btn = $('inqNueva');
    if (!btn) return;
    var S = { cliente: null, equipo: null, libre: false, fotos: [], tq: null, te: null, enviando: false };
    var el = null, modal = null;

    function asegurar() {
      if (el) return;
      el = document.createElement('div');
      el.className = 'modal fade iqf-modal';
      el.tabIndex = -1;
      el.setAttribute('aria-hidden', 'true');
      el.setAttribute('aria-labelledby', 'inqNTit');
      el.innerHTML = '<div class="modal-dialog modal-lg modal-dialog-centered modal-dialog-scrollable modal-fullscreen-sm-down"><div class="modal-content">' +
        '<div class="modal-header"><h5 class="modal-title" id="inqNTit"><i class="bi bi-chat-quote-fill me-2"></i>Nueva inquietud de un cliente</h5><button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Cerrar"></button></div>' +
        '<div class="modal-body">' +
        '<div class="fw-bold mb-1">1. ¿De qué cliente?</div><div id="inqNCli"></div>' +
        '<div id="inqNCliBus"><input id="inqNQ" type="search" class="form-control" placeholder="Nombre o RUT del cliente" autocomplete="off"><div id="inqNCliRes" class="iqf-lista-sel"></div></div>' +
        '<div id="inqNEqW" hidden class="mt-3"><div class="fw-bold mb-1">2. ¿De qué equipo?</div><div id="inqNEq"></div>' +
        '<div id="inqNEqBus"><input id="inqNQe" type="search" class="form-control" placeholder="Nombre, SKU o N° de serie" autocomplete="off"><div id="inqNEqRes" class="iqf-lista-sel"></div>' +
        '<button type="button" class="btn btn-link p-0 mt-1" id="inqNLibre" style="min-height:44px">El equipo no está en la lista: escribirlo a mano</button></div>' +
        '<input id="inqNLibreTxt" type="text" class="form-control" placeholder="¿Cuál es el equipo?" maxlength="200" hidden></div>' +
        '<div class="fw-bold mt-3 mb-1">3. ¿Qué le preocupa al cliente?</div><textarea id="inqNInq" class="form-control" rows="3" maxlength="700" placeholder="Con sus palabras."></textarea>' +
        '<div class="fw-bold mt-3 mb-1">Diagnóstico adicional <small class="text-muted fw-normal">(opcional aquí)</small></div><textarea id="inqNDia" class="form-control" rows="2" maxlength="700"></textarea>' +
        '<div class="fw-bold mt-3 mb-1">Fotos <small class="text-muted fw-normal">(opcional, hasta 3)</small></div>' +
        '<input id="inqNFile" type="file" accept="image/*" multiple hidden><button type="button" class="btn btn-outline-dark iqf-btn" id="inqNFotoBtn"><i class="bi bi-camera-fill"></i>Agregar foto</button>' +
        '<div id="inqNFotos" class="iqf-fotos mt-2"></div>' +
        '<div class="text-danger fw-bold mt-3" id="inqNErr" role="alert" hidden></div></div>' +
        '<div class="modal-footer"><button type="button" class="btn btn-outline-secondary iqf-btn" data-bs-dismiss="modal">Cancelar</button>' +
        '<button type="button" class="btn btn-dark iqf-btn" id="inqNOk"><i class="bi bi-send-fill"></i>Guardar inquietud</button></div></div></div>';
      document.body.appendChild(el);
      modal = new bootstrap.Modal(el);
      $('inqNQ').addEventListener('input', function () { clearTimeout(S.tq); S.tq = setTimeout(buscarCli, 250); });
      $('inqNQe').addEventListener('input', function () { clearTimeout(S.te); S.te = setTimeout(buscarEq, 250); });
      $('inqNLibre').addEventListener('click', function () { S.libre = true; S.equipo = null; pintarEq(); });
      $('inqNFotoBtn').addEventListener('click', function () { $('inqNFile').click(); });
      $('inqNFile').addEventListener('change', fotos);
      $('inqNOk').addEventListener('click', guardar);
      el.addEventListener('click', function (e) {
        var c = e.target.closest && e.target.closest('[data-cli]');
        if (c) { S.cliente = JSON.parse(c.getAttribute('data-cli')); S.equipo = null; S.libre = false; pintarCli(); buscarEq(); return; }
        var q = e.target.closest && e.target.closest('[data-eq]');
        if (q) { S.equipo = JSON.parse(q.getAttribute('data-eq')); S.libre = false; pintarEq(); return; }
        if (e.target.closest && e.target.closest('[data-cambiar]')) {
          if (e.target.closest('[data-cambiar]').getAttribute('data-cambiar') === 'cli') { S.cliente = null; S.equipo = null; pintarCli(); } else { S.equipo = null; S.libre = false; pintarEq(); }
        }
      });
    }
    function fotoEq(u) { return u ? '<img src="' + esc(u) + '" alt="Foto del producto en el catálogo">' : '<span class="sin"><i class="bi bi-image"></i></span>'; }
    function buscarCli() {
      var q = ($('inqNQ').value || '').trim();
      if (q.length < 2) { $('inqNCliRes').innerHTML = ''; return; }
      get(BASE + 'clientes?q=' + encodeURIComponent(q)).then(function (j) {
        $('inqNCliRes').innerHTML = (j.clientes || []).map(function (c) {
          return '<button type="button" class="iqf-op" data-cli=\'' + esc(JSON.stringify(c)) + '\'><span><b>' + esc(c.nombre) + '</b><small>' + esc(c.rut) + '</small></span></button>';
        }).join('') || '<div class="text-muted p-2">No encontramos clientes con eso.</div>';
      });
    }
    function buscarEq() {
      if (!S.cliente) return;
      get(BASE + 'equipos?cliente_id=' + S.cliente.id + '&q=' + encodeURIComponent(($('inqNQe').value || '').trim())).then(function (j) {
        $('inqNEqRes').innerHTML = (j.equipos || []).map(function (e) {
          return '<button type="button" class="iqf-op" data-eq=\'' + esc(JSON.stringify(e)) + '\'>' + fotoEq(e.foto) + '<span><b>' + esc(e.nombre) + '</b><small>' + (e.sku ? 'SKU ' + esc(e.sku) : 'Sin SKU') + (e.serie ? ' · S/N ' + esc(e.serie) : '') + '</small></span></button>';
        }).join('') || '<div class="text-muted p-2">No hay equipos con eso en la ficha del cliente.</div>';
      });
    }
    function pintarCli() {
      $('inqNCli').innerHTML = S.cliente ? '<div class="iqf-op"><span><b>' + esc(S.cliente.nombre) + '</b><small>' + esc(S.cliente.rut) + '</small></span><button type="button" class="btn btn-link ms-auto" data-cambiar="cli">Cambiar</button></div>' : '';
      $('inqNCliBus').hidden = !!S.cliente;
      $('inqNEqW').hidden = !S.cliente;
      pintarEq();
    }
    function pintarEq() {
      $('inqNEq').innerHTML = S.equipo ? '<div class="iqf-op">' + fotoEq(S.equipo.foto) + '<span><b>' + esc(S.equipo.nombre) + '</b><small>' + (S.equipo.sku ? 'SKU ' + esc(S.equipo.sku) : 'Sin SKU') + (S.equipo.serie ? ' · S/N ' + esc(S.equipo.serie) : '') + '</small></span><button type="button" class="btn btn-link ms-auto" data-cambiar="eq">Cambiar</button></div>' : '';
      $('inqNEqBus').hidden = !!S.equipo || S.libre;
      $('inqNLibreTxt').hidden = !S.libre;
    }
    function pintarFotos() {
      $('inqNFotos').innerHTML = S.fotos.map(function (f, i) { return '<a href="#" data-q="' + i + '" title="Quitar" onclick="return false"><img src="' + esc(f.url) + '" alt="Foto ' + (i + 1) + '"></a>'; }).join('');
      $('inqNFotoBtn').hidden = S.fotos.length >= 3;
    }
    function fotos() {
      var inp = $('inqNFile'), lista = Array.prototype.slice.call(inp.files || []);
      inp.value = '';
      lista.slice(0, Math.max(0, 3 - S.fotos.length)).forEach(function (f) { S.fotos.push({ file: f, url: URL.createObjectURL(f) }); });
      pintarFotos();
    }
    function err(m) { var e = $('inqNErr'); e.textContent = m || ''; e.hidden = !m; }
    function guardar() {
      if (S.enviando) return;
      err('');
      if (!S.cliente) { err('Elige el cliente.'); return; }
      var libre = ($('inqNLibreTxt').value || '').trim();
      if (!S.equipo && !(S.libre && libre.length >= 3)) { err('Elige el equipo o escribe cuál es.'); return; }
      if (($('inqNInq').value || '').trim().length < 10) { err('Cuenta qué le preocupa al cliente (mínimo 10 caracteres).'); return; }
      var fd = new FormData();
      fd.append('cliente_id', S.cliente.id);
      if (S.equipo) fd.append('maquina_id', S.equipo.id); else fd.append('producto_texto', libre);
      fd.append('inquietud', $('inqNInq').value); fd.append('diagnostico', $('inqNDia').value);
      S.fotos.forEach(function (f) { fd.append('fotos', f.file, f.file.name || 'foto.jpg'); });
      S.enviando = true;
      post(BASE + 'crear', fd).then(function (j) {
        S.enviando = false;
        if (!j.ok) { err(j.error || 'No pudimos guardarla.'); return; }
        toast('✓ ' + j.numero + ' guardada', 'success');
        window.location.href = j.url;
      });
    }
    btn.addEventListener('click', function () { asegurar(); S.cliente = null; S.equipo = null; S.libre = false; S.fotos = []; pintarCli(); pintarFotos(); err(''); modal.show(); });
  }

  // Fila de la tabla = enlace a la ficha (como el Monitor de Retiros); los controles de dentro siguen funcionando.
  document.addEventListener('click', function (e) {
    var tr = e.target.closest && e.target.closest('tr.rm-row[data-href]');
    if (!tr || e.target.closest('a,button,input,select,label')) return;
    window.location.href = tr.getAttribute('data-href');
  });

  if (CFG.modo === 'ficha') ficha();
  else if (CFG.modo === 'monitor') monitor();
})();

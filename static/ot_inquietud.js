/* ══════════════════════════════════════════════════════════════════════════════════════════════════════════════
   ot_inquietud.js — «El cliente me comentó…» (franja de la OT + modal del técnico).
   2026-10-26 (Daniel: las inquietudes del cliente que levantan los técnicos que manipulan OT).
   El técnico (también el externo) busca CUALQUIER equipo de la ficha del cliente (nombre, SKU o N° de serie; con su foto del catálogo para
   reconocerlo) o escribe el equipo a mano, cuenta qué le preocupa al cliente y deja su diagnóstico (los dos obligatorios, mínimo 10 caracteres),
   con hasta 3 fotos opcionales. La gestión lo ve en /ot/inquietudes; aquí no se muestra plata, proveedores ni el monitor (REGLAS #19/#26).
   Al cliente no se le avisa nada (REGLA #22). Sin alert/confirm/prompt nativos (REGLA #1). Botones de 44 px (REGLA #3).
   API: GET /ot/api/<vid>/equipos-cliente?q= · POST /ot/api/<vid>/inquietud-cliente (multipart) · GET /ot/api/<vid>/inquietudes
   ══════════════════════════════════════════════════════════════════════════════════════════════════════════════ */
(function () {
  'use strict';
  var CFG = window.OTD_IQ_CFG || {};
  var VID = CFG.vid;
  if (!VID) return;
  var MIN = 10, MAX_FOTOS = 3;

  function esc(s) {
    return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
    });
  }
  function toast(m, t, d) { try { if (window.ilusToast) window.ilusToast(m, { type: t || 'info', duration: d || 6000 }); } catch (e) { /* nada */ } }
  function $(id) { return document.getElementById(id); }

  var S = { equipo: null, libre: false, fotos: [], buscando: 0, enviando: false };
  var el = null, modal = null, timer = null;

  /* ── La lista de lo ya anotado desde esta OT (solo número, equipo y estado) ── */
  function pintarLista() {
    var cont = $('iqLista');
    if (!cont) return;
    fetch('/ot/api/' + VID + '/inquietudes', { credentials: 'same-origin', headers: { 'Accept': 'application/json' } })
      .then(function (r) { return r.json(); })
      .then(function (j) {
        if (!j || !j.ok || !(j.items || []).length) { cont.innerHTML = ''; return; }
        cont.innerHTML = j.items.map(function (i) {
          return '<span class="iq-chip"><b>' + esc(i.numero) + '</b> ' + esc(i.equipo) + ' · ' + esc(i.estado) + '</span>';
        }).join('');
      }).catch(function () { /* la franja funciona sin la lista */ });
  }

  /* ── El modal ── */
  function asegurar() {
    if (el) return;
    el = document.createElement('div');
    el.className = 'modal fade iq-modal';
    el.id = 'otdInquietudModal';
    el.tabIndex = -1;
    el.setAttribute('aria-hidden', 'true');
    el.setAttribute('aria-labelledby', 'iqTitulo');
    el.innerHTML =
      '<div class="modal-dialog modal-lg modal-dialog-centered modal-dialog-scrollable modal-fullscreen-sm-down"><div class="modal-content">' +
      '<div class="modal-header iq-mh"><div><h5 class="modal-title" id="iqTitulo"><i class="bi bi-chat-quote-fill"></i> El cliente me comentó…</h5>' +
      '<div class="iq-sub">Queda registrado con tu nombre. La gestión decide qué hacer; al cliente no se le avisa nada.</div></div>' +
      '<button type="button" class="btn-close btn-close-white" data-bs-dismiss="modal" aria-label="Cerrar"></button></div>' +
      '<div class="modal-body">' +
      '<section class="iq-paso"><div class="iq-paso-cab"><span class="iq-circ" id="iqC1">1</span><b>¿De qué equipo habla?</b></div>' +
      '<div id="iqEquipoSel"></div>' +
      '<div id="iqBuscador"><label class="iq-lbl" for="iqQ">Busca por nombre, SKU o N° de serie</label>' +
      '<input id="iqQ" type="search" class="iq-input" autocomplete="off" placeholder="Ej: trotadora, ABC123, 65206047…">' +
      '<div id="iqResultados" class="iq-resultados" role="listbox" aria-label="Equipos del cliente"></div>' +
      '<button type="button" class="iq-link" id="iqLibreBtn"><i class="bi bi-pencil-square"></i> El equipo no está en la lista: escribirlo a mano</button></div>' +
      '<div id="iqLibre" hidden><label class="iq-lbl" for="iqLibreTxt">¿Cuál es el equipo?</label>' +
      '<input id="iqLibreTxt" type="text" class="iq-input" maxlength="200" placeholder="Ej: Trotadora Life Fitness del segundo piso">' +
      '<button type="button" class="iq-link" id="iqVolverBusca"><i class="bi bi-search"></i> Mejor buscarlo en la ficha del cliente</button></div></section>' +
      '<section class="iq-paso"><div class="iq-paso-cab"><span class="iq-circ" id="iqC2">2</span><b>¿Qué le preocupa al cliente?</b></div>' +
      '<textarea id="iqInq" class="iq-input" rows="3" maxlength="700" placeholder="Con sus palabras: qué nota raro, qué le preocupa, qué pide."></textarea>' +
      '<div class="iq-cont" id="iqInqCont"></div></section>' +
      '<section class="iq-paso"><div class="iq-paso-cab"><span class="iq-circ" id="iqC3">3</span><b>Tu diagnóstico adicional</b></div>' +
      '<textarea id="iqDia" class="iq-input" rows="3" maxlength="700" placeholder="Lo que viste tú: qué tiene el equipo, qué se necesitaría."></textarea>' +
      '<div class="iq-cont" id="iqDiaCont"></div></section>' +
      '<section class="iq-paso"><div class="iq-paso-cab"><span class="iq-circ iq-gris" id="iqC4">4</span><b>Fotos <small>(opcional, hasta ' + MAX_FOTOS + ')</small></b></div>' +
      '<input id="iqFile" type="file" accept="image/*" multiple hidden>' +
      '<button type="button" class="iq-btn iq-btn-sec" id="iqFotoBtn"><i class="bi bi-camera-fill"></i> Agregar foto</button>' +
      '<div id="iqFotos" class="iq-fotos"></div></section>' +
      '<div class="iq-error" id="iqError" role="alert" hidden></div>' +
      '</div>' +
      '<div class="modal-footer"><button type="button" class="iq-btn iq-btn-sec" data-bs-dismiss="modal">Cancelar</button>' +
      '<button type="button" class="iq-btn iq-btn-pri" id="iqGuardar"><i class="bi bi-send-fill"></i> Guardar inquietud</button></div>' +
      '</div></div>';
    document.body.appendChild(el);
    modal = new bootstrap.Modal(el);
    $('iqQ').addEventListener('input', function () { clearTimeout(timer); timer = setTimeout(buscar, 250); });
    $('iqLibreBtn').addEventListener('click', function () { S.libre = true; S.equipo = null; pintarEquipo(); $('iqLibreTxt').focus(); });
    $('iqVolverBusca').addEventListener('click', function () { S.libre = false; pintarEquipo(); $('iqQ').focus(); });
    $('iqLibreTxt').addEventListener('input', progreso);
    $('iqInq').addEventListener('input', progreso);
    $('iqDia').addEventListener('input', progreso);
    $('iqFotoBtn').addEventListener('click', function () { $('iqFile').click(); });
    $('iqFile').addEventListener('change', agregarFotos);
    $('iqGuardar').addEventListener('click', guardar);
    el.addEventListener('click', function (e) {
      var b = e.target.closest && e.target.closest('[data-iq-equipo]');
      if (b) { elegir(JSON.parse(b.getAttribute('data-iq-equipo'))); return; }
      var q = e.target.closest && e.target.closest('[data-iq-quitar]');
      if (q) { S.fotos.splice(parseInt(q.getAttribute('data-iq-quitar'), 10), 1); pintarFotos(); return; }
      if (e.target.closest && e.target.closest('#iqCambiarEq')) { S.equipo = null; pintarEquipo(); $('iqQ').focus(); }
    });
    el.addEventListener('hidden.bs.modal', function () { S.enviando = false; });
  }

  function foto(src) {
    return src ? '<img class="iq-eq-foto" src="' + esc(src) + '" alt="Foto del producto en el catálogo" loading="lazy">'
               : '<span class="iq-eq-foto iq-eq-sin" aria-hidden="true"><i class="bi bi-image"></i></span>';
  }
  function datosEq(e) {
    return '<span class="iq-eq-datos"><b>' + esc(e.nombre) + '</b>' +
      '<small>' + (e.sku ? 'SKU ' + esc(e.sku) : 'Sin SKU') + (e.serie ? ' · S/N ' + esc(e.serie) : '') + '</small></span>';
  }

  function buscar() {
    var q = ($('iqQ').value || '').trim();
    var n = ++S.buscando;
    var cont = $('iqResultados');
    cont.innerHTML = '<div class="iq-vacio"><i class="bi bi-hourglass-split"></i> Buscando…</div>';
    fetch('/ot/api/' + VID + '/equipos-cliente?q=' + encodeURIComponent(q), { credentials: 'same-origin', headers: { 'Accept': 'application/json' } })
      .then(function (r) { return r.json().catch(function () { return {}; }); })
      .then(function (j) {
        if (n !== S.buscando) return;                      // llegó una respuesta vieja
        if (!j || !j.ok) { cont.innerHTML = '<div class="iq-vacio">' + esc((j && j.error) || 'No pudimos buscar ahora.') + '</div>'; return; }
        if (!(j.equipos || []).length) { cont.innerHTML = '<div class="iq-vacio">No encontramos equipos con eso en la ficha del cliente.</div>'; return; }
        cont.innerHTML = j.equipos.map(function (e) {
          return '<button type="button" class="iq-eq" role="option" data-iq-equipo=\'' + esc(JSON.stringify(e)).replace(/'/g, '&#39;') + '\'>' + foto(e.foto) + datosEq(e) + '</button>';
        }).join('');
      })
      .catch(function () { if (n === S.buscando) cont.innerHTML = '<div class="iq-vacio">Sin conexión. Revisa la señal e intenta de nuevo.</div>'; });
  }

  function elegir(e) { S.equipo = e; S.libre = false; pintarEquipo(); }

  function pintarEquipo() {
    $('iqEquipoSel').innerHTML = S.equipo
      ? '<div class="iq-eq-sel">' + foto(S.equipo.foto) + datosEq(S.equipo) + '<button type="button" class="iq-link" id="iqCambiarEq">Cambiar</button></div>' : '';
    $('iqBuscador').hidden = !!S.equipo || S.libre;
    $('iqLibre').hidden = !S.libre;
    progreso();
  }

  function pintarFotos() {
    $('iqFotos').innerHTML = S.fotos.map(function (f, i) {
      return '<div class="iq-foto"><img src="' + esc(f.url) + '" alt="Foto ' + (i + 1) + ' de la inquietud"><button type="button" class="iq-foto-x" data-iq-quitar="' + i +
        '" aria-label="Quitar la foto ' + (i + 1) + '"><i class="bi bi-x-lg"></i></button></div>';
    }).join('');
    $('iqFotoBtn').hidden = S.fotos.length >= MAX_FOTOS;
  }

  function agregarFotos() {
    var inp = $('iqFile');
    var lista = Array.prototype.slice.call(inp.files || []);
    inp.value = '';
    var cupo = MAX_FOTOS - S.fotos.length;
    if (lista.length > cupo) toast('Solo caben ' + MAX_FOTOS + ' fotos: se agregaron las primeras.', 'warning');
    lista.slice(0, Math.max(0, cupo)).reduce(function (p, f) {
      return p.then(function () {
        return (window.ilusComprimirImagen ? Promise.resolve(window.ilusComprimirImagen(f)).catch(function () { return f; }) : Promise.resolve(f))
          .then(function (blob) {
            var archivo = blob || f;
            S.fotos.push({ file: archivo, name: f.name || 'foto.jpg', url: URL.createObjectURL(archivo) });
            pintarFotos();
          });
      });
    }, Promise.resolve());
  }

  function largo(id) { return (($(id).value || '').replace(/\s+/g, ' ').trim()).length; }
  function progreso() {
    var okEq = !!S.equipo || (S.libre && ($('iqLibreTxt').value || '').trim().length >= 3);
    var l2 = largo('iqInq'), l3 = largo('iqDia');
    $('iqC1').className = 'iq-circ' + (okEq ? ' ok' : '');
    $('iqC2').className = 'iq-circ' + (l2 >= MIN ? ' ok' : '');
    $('iqC3').className = 'iq-circ' + (l3 >= MIN ? ' ok' : '');
    $('iqInqCont').textContent = l2 >= MIN ? '' : 'Faltan ' + (MIN - l2) + ' caracteres como mínimo.';
    $('iqDiaCont').textContent = l3 >= MIN ? '' : 'Faltan ' + (MIN - l3) + ' caracteres como mínimo.';
    return okEq && l2 >= MIN && l3 >= MIN;
  }

  function error(m) { var e = $('iqError'); e.textContent = m || ''; e.hidden = !m; }

  function guardar() {
    if (S.enviando) return;
    error('');
    if (!progreso()) {
      error(!S.equipo && !(S.libre && ($('iqLibreTxt').value || '').trim().length >= 3) ? 'Elige el equipo en el buscador o escribe cuál es.'
        : (largo('iqInq') < MIN ? 'Cuenta qué le preocupa al cliente (mínimo ' + MIN + ' caracteres).' : 'Escribe tu diagnóstico del equipo (mínimo ' + MIN + ' caracteres).'));
      return;
    }
    var fd = new FormData();
    if (S.equipo) fd.append('maquina_id', S.equipo.id); else fd.append('producto_texto', ($('iqLibreTxt').value || '').trim());
    fd.append('inquietud', $('iqInq').value);
    fd.append('diagnostico', $('iqDia').value);
    S.fotos.forEach(function (f) { fd.append('fotos', f.file, f.name); });
    S.enviando = true;
    var b = $('iqGuardar'); b.disabled = true; b.innerHTML = '<i class="bi bi-hourglass-split"></i> Guardando…';
    fetch('/ot/api/' + VID + '/inquietud-cliente', { method: 'POST', credentials: 'same-origin', body: fd, headers: { 'X-Requested-With': 'XMLHttpRequest', 'Accept': 'application/json' } })
      .then(function (r) { return r.json().catch(function () { return {}; }); })
      .then(function (j) {
        S.enviando = false; b.disabled = false; b.innerHTML = '<i class="bi bi-send-fill"></i> Guardar inquietud';
        if (!j || !j.ok) { error((j && j.error) || 'No pudimos guardarla. Lo que escribiste sigue aquí: intenta de nuevo.'); return; }
        modal.hide();
        toast('✓ ' + (j.numero || 'Inquietud') + ' guardada. ' + (j.mensaje || ''), 'success', 7000);
        reiniciar();
        pintarLista();
      })
      .catch(function () {
        S.enviando = false; b.disabled = false; b.innerHTML = '<i class="bi bi-send-fill"></i> Guardar inquietud';
        error('Sin conexión. Lo que escribiste sigue aquí: revisa la señal e intenta de nuevo.');
      });
  }

  function reiniciar() {
    S.equipo = null; S.libre = false; S.fotos = [];
    ['iqQ', 'iqLibreTxt', 'iqInq', 'iqDia'].forEach(function (id) { if ($(id)) $(id).value = ''; });
    if ($('iqResultados')) $('iqResultados').innerHTML = '';
    pintarFotos(); pintarEquipo(); error('');
  }

  function abrir() {
    asegurar();
    pintarFotos(); pintarEquipo(); error('');
    modal.show();
    buscar();
  }

  var b = $('iqAbrir');
  if (b) b.addEventListener('click', abrir);
  pintarLista();
  window.OTInquietud = { abrir: abrir };
})();

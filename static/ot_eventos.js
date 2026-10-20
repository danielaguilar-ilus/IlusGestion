/* 🧩 2026-10-20 — Pantallas de eventos (Daniel, 19-oct: «clasificar las OT por EVENTO»). Un mismo archivo para las dos:
     · /ot/eventos       → lista paginada de eventos (REGLA #4.3): <div id="evRaiz" data-modo="lista">
     · /ot/eventos/<id>  → un evento: avance por fase, recorrido instalación → desinstalación, una línea por día, tabla de sus OT
                           y «Asociar OT existentes» (casillas con marcar/desmarcar todo, REGLA #14): data-modo="detalle" data-id
   Sin alert/confirm/prompt nativos (REGLA #1). Fechas dd/mm/aaaa (el servidor ya las manda así, REGLA #6). Sin montos
   (REGLA #26: es avance, no plata). API: GET/POST /ot/api/eventos, GET /ot/api/eventos/<id>, GET …/candidatas, POST …/asociar. */
(function () {
  'use strict';
  var raiz = document.getElementById('evRaiz');
  if (!raiz) return;
  var modo = raiz.getAttribute('data-modo');
  var esTecnico = raiz.getAttribute('data-tecnico') === '1';

  function esc(s) {
    return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
    });
  }
  function api(url, cuerpo) {
    var init = { credentials: 'same-origin', method: cuerpo === undefined ? 'GET' : 'POST', headers: {} };
    if (cuerpo !== undefined) { init.headers['Content-Type'] = 'application/json'; init.body = JSON.stringify(cuerpo); }
    return fetch(url, init).then(function (r) {
      return r.json().catch(function () { return null; }).then(function (j) { return { ok: r.ok && !(j && j.ok === false), http: r.status, j: j || {} }; });
    }).catch(function () { return { ok: false, http: 0, j: { error: 'No hay conexión con el servidor.' } }; });
  }
  function toast(m, t) { if (window.ilusToast) window.ilusToast(m, { type: t || 'info', duration: 6000 }); }
  /* REGLA #6: nunca una fecha ISO cruda en pantalla (el avance trae la próxima fecha como aaaa-mm-dd). */
  function bonita(iso) { var m = /^(\d{4})-(\d{2})-(\d{2})/.exec(String(iso || '')); return m ? m[3] + '/' + m[2] + '/' + m[1] : String(iso || ''); }
  function qs() { try { return new URLSearchParams(location.search); } catch (e) { return { get: function () { return null; } }; } }
  function guardarUrl(p) {
    try {
      var u = new URLSearchParams();
      Object.keys(p).forEach(function (k) { if (p[k] !== '' && p[k] != null) u.set(k, p[k]); });
      history.replaceState(null, '', location.pathname + (u.toString() ? '?' + u.toString() : ''));
    } catch (e) { /* la URL es una comodidad: nunca puede romper la pantalla */ }
  }
  var FASES = ['instalacion', 'desinstalacion', 'revision', 'otras'];
  var FASE_TXT = { instalacion: 'Instalación', desinstalacion: 'Desinstalación', revision: 'Revisión interna', otras: 'Otras OT' };
  var FILTROS = [['activos', 'Activos'], ['proximos', 'Próximos'], ['terminados', 'Terminados'], ['cancelados', 'Cancelados'], ['todos', 'Todos']];
  var ICO_SEM = { verde: 'bi-check-lg', ambar: 'bi-hourglass-split', rojo: 'bi-exclamation-lg', gris: 'bi-dash-lg' };
  var NIVEL_ICO = { gris: 'bi-dash-circle', ambar: 'bi-hourglass-split', azul: 'bi-calendar-check', verde: 'bi-check-circle-fill', rojo: 'bi-exclamation-triangle-fill' };

  /* ── Pie de tabla con el patrón de Etiquetas (REGLA #4.3) ───────────────────────────────────────────────────── */
  function pie(d, opciones, idBase) {
    var h = '<div class="ev-pie"><span class="t">Mostrando <b>' + d.desde + '–' + d.hasta + '</b> de <b>' + d.total + '</b></span>' +
      '<span class="ev-pp" aria-label="Filas por página">';
    opciones.forEach(function (n) {
      h += '<button type="button" class="' + (n === d.per_page ? 'on' : '') + '" data-pp="' + n + '" aria-label="' + n + ' por página">' + n + '</button>';
    });
    h += '</span><span class="ev-pp">' +
      '<button type="button" class="nav" data-pg="' + (d.page - 1) + '"' + (d.page <= 1 ? ' disabled' : '') + '><i class="bi bi-chevron-left me-1"></i>Anterior</button>' +
      '<span style="font-weight:800;padding:0 6px">Página <b>' + d.page + '</b> de ' + d.pages + '</span>' +
      '<button type="button" class="nav" data-pg="' + (d.page + 1) + '"' + (d.page >= d.pages ? ' disabled' : '') + '>Siguiente<i class="bi bi-chevron-right ms-1"></i></button></span></div>';
    return h;
  }

  /* ══════════════════════════ LISTA ══════════════════════════════════════════════════════════════════════════ */
  function lista() {
    var p0 = qs();
    var st = { estado: p0.get('estado') || 'activos', q: p0.get('q') || '', page: parseInt(p0.get('page') || '1', 10) || 1,
      porPag: parseInt(p0.get('per_page') || '20', 10) || 20 };
    if (FILTROS.map(function (f) { return f[0]; }).indexOf(st.estado) < 0) st.estado = 'activos';
    var enGestion = !esTecnico;
    var btnNuevo = document.getElementById('evNuevo');
    if (btnNuevo) btnNuevo.addEventListener('click', function () {
      window.OTEventoForm.abrir({ onGuardado: function (ev) { if (ev && ev.id) location.href = '/ot/eventos/' + ev.id; else cargar(); } });
    });

    function tarjeta(e) {
      var sem = e.semaforo || 'gris';
      var fases = FASES.filter(function (f) { return e.por_fase && e.por_fase[f] && e.por_fase[f].total > 0; });
      var h = '<article class="ev-card ' + esc(sem) + '"><span class="ev-circ" aria-hidden="true"><i class="bi ' + (ICO_SEM[sem] || 'bi-dash-lg') + '"></i></span>' +
        '<h3><a href="' + esc(e.url) + '">' + esc(e.nombre) + '</a><span class="ev-est ' + esc(e.estado) + '">' + esc(e.estado_txt) + '</span></h3>' +
        '<div class="ev-sub">' + [e.cliente, e.lugar].filter(Boolean).map(esc).join(' · ') + '</div>' +
        '<dl class="ev-datos">' +
        '<div><dt>Fechas</dt><dd>' + esc(e.fechas_txt || 'Sin fechas definidas') + '</dd></div>' +
        '<div><dt>Órdenes de trabajo</dt><dd>' + e.n_ots + (e.n_ots ? ' · ' + e.pct + '% cerradas' : '') + '</dd></div>' +
        '<div><dt>Próxima fecha</dt><dd>' + esc(e.proxima_fecha || '—') + '</dd></div>' +
        '<div><dt>Situación</dt><dd>' + esc(e.semaforo_txt || '') + '</dd></div></dl>';
      if (fases.length) {
        h += '<dl class="ev-fases">' + fases.map(function (f) {
          var x = e.por_fase[f], pc = x.total ? Math.round(100 * x.cerradas / x.total) : 0;
          return '<div class="ev-fase"><dt>' + FASE_TXT[f] + '</dt><dd>' + x.cerradas + ' de ' + x.total + ' cerrada' + (x.total === 1 ? '' : 's') +
            '</dd><div class="ev-barrita" role="img" aria-label="' + pc + '%"><i style="width:' + pc + '%"></i></div></div>';
        }).join('') + '</dl>';
      } else {
        h += '<div class="ev-sub">Todavía no tiene órdenes de trabajo.</div>';
      }
      var avisos = '';
      if (e.desinst_vencidas) avisos += '<span class="ev-chip rojo"><i class="bi bi-exclamation-triangle-fill"></i>' + e.desinst_vencidas + ' desinstalación' + (e.desinst_vencidas === 1 ? '' : 'es') + ' vencida' + (e.desinst_vencidas === 1 ? '' : 's') + '</span> ';
      if (e.desinst_pendientes) avisos += '<span class="ev-chip ambar"><i class="bi bi-hourglass-split"></i>' + e.desinst_pendientes + ' desinstalación' + (e.desinst_pendientes === 1 ? '' : 'es') + ' sin agendar</span>';
      if (avisos) h += '<div class="ev-sub" style="display:flex;flex-wrap:wrap;gap:6px">' + avisos + '</div>';
      h += '<div class="ev-acc"><a class="ev-btn pri" href="' + esc(e.url) + '"><i class="bi bi-eye"></i>Abrir</a>';
      if (enGestion) {
        h += '<a class="ev-btn" href="/ot/?crear=1&evento=' + e.id + '"' + (e.estado === 'cancelado' ? ' aria-disabled="true" style="pointer-events:none;opacity:.45"' : '') + '><i class="bi bi-plus-lg"></i>Agregar OT al evento</a>' +
          '<button type="button" class="ev-btn" data-editar="' + e.id + '"><i class="bi bi-pencil"></i>Editar</button>';
      }
      return h + '</div></article>';
    }

    function pintar(d) {
      var h = '<div class="ev-chips" role="group" aria-label="Estado de los eventos">' + FILTROS.map(function (f) {
        var n = d.conteos ? d.conteos[f[0]] : 0;
        return '<button type="button" class="ev-fch' + (st.estado === f[0] ? ' on' : '') + '" data-estado="' + f[0] + '" aria-pressed="' + (st.estado === f[0]) + '">' + f[1] + ' <b>' + (n || 0) + '</b></button>';
      }).join('') + '</div>' +
        '<form class="ev-barra" id="evBuscar" role="search"><input class="ev-in" type="search" id="evQ" value="' + esc(st.q) + '" placeholder="Buscar por nombre, lugar o cliente…" aria-label="Buscar eventos">' +
        '<button class="ev-btn pri" type="submit"><i class="bi bi-search"></i>Buscar</button>' +
        (st.q ? '<button class="ev-btn" type="button" id="evLimpiar"><i class="bi bi-x-lg"></i>Limpiar</button>' : '') + '</form>';
      if (!d.eventos.length) {
        h += '<div class="ev-vacio"><i class="bi bi-calendar-event" style="font-size:1.6rem;display:block;margin-bottom:6px"></i>' +
          (st.q ? 'No hay eventos que coincidan con «' + esc(st.q) + '».' : (st.estado === 'activos' ? 'No hay eventos activos todavía.' : 'No hay eventos en esta lista.')) +
          (enGestion && !st.q ? '<br><small>Usa «Nuevo evento» o créalo al generar una OT.</small>' : '') + '</div>';
      } else {
        h += '<div class="ev-lista">' + d.eventos.map(tarjeta).join('') + '</div>' + pie(d, [20, 50, 100]);
      }
      raiz.innerHTML = h;
      enlazar();
    }
    function enlazar() {
      Array.prototype.forEach.call(raiz.querySelectorAll('[data-estado]'), function (b) {
        b.addEventListener('click', function () { st.estado = b.getAttribute('data-estado'); st.page = 1; cargar(); });
      });
      var f = document.getElementById('evBuscar');
      if (f) f.addEventListener('submit', function (ev) { ev.preventDefault(); st.q = (document.getElementById('evQ').value || '').trim(); st.page = 1; cargar(); });
      var l = document.getElementById('evLimpiar');
      if (l) l.addEventListener('click', function () { st.q = ''; st.page = 1; cargar(); });
      Array.prototype.forEach.call(raiz.querySelectorAll('[data-pp]'), function (b) {
        b.addEventListener('click', function () { st.porPag = parseInt(b.getAttribute('data-pp'), 10); st.page = 1; cargar(); });
      });
      Array.prototype.forEach.call(raiz.querySelectorAll('[data-pg]'), function (b) {
        b.addEventListener('click', function () { if (b.disabled) return; st.page = parseInt(b.getAttribute('data-pg'), 10); cargar(); window.scrollTo({ top: 0, behavior: 'smooth' }); });
      });
      Array.prototype.forEach.call(raiz.querySelectorAll('[data-editar]'), function (b) {
        b.addEventListener('click', function () {
          var id = parseInt(b.getAttribute('data-editar'), 10);
          api('/ot/api/eventos/' + id + '?per_page=1').then(function (r) {
            if (!r.ok) { toast(r.j.error || 'No se pudo abrir el evento.', 'error'); return; }
            window.OTEventoForm.abrir({ evento: r.j.evento, onGuardado: function () { cargar(); } });
          });
        });
      });
    }
    function cargar() {
      guardarUrl({ estado: st.estado === 'activos' ? '' : st.estado, q: st.q, page: st.page > 1 ? st.page : '', per_page: st.porPag !== 20 ? st.porPag : '' });
      api('/ot/api/eventos?estado=' + encodeURIComponent(st.estado) + '&q=' + encodeURIComponent(st.q) + '&page=' + st.page + '&per_page=' + st.porPag).then(function (r) {
        if (!r.ok) { raiz.innerHTML = '<div class="ev-error">' + esc(r.j.error || 'No se pudo leer la lista de eventos.') + '</div>'; return; }
        if (r.j.page && r.j.page !== st.page) st.page = r.j.page;
        pintar(r.j);
      });
    }
    cargar();
  }

  /* ══════════════════════════ DETALLE ════════════════════════════════════════════════════════════════════════ */
  function detalle() {
    var id = parseInt(raiz.getAttribute('data-id'), 10);
    var p0 = qs();
    var st = { page: parseInt(p0.get('page') || '1', 10) || 1, porPag: parseInt(p0.get('per_page') || '25', 10) || 25, asocAbierto: false };
    var D = null;

    function kpiFase(f, av) {
      var x = (av.por_fase || {})[f] || { total: 0, cerradas: 0, por_cerrar: 0, en_curso: 0, pendientes: 0 };
      var pc = x.total ? Math.round(100 * x.cerradas / x.total) : 0;
      var partes = [];
      if (x.en_curso) partes.push(x.en_curso + ' en curso');
      if (x.por_cerrar) partes.push(x.por_cerrar + ' por cerrar');
      if (x.pendientes) partes.push(x.pendientes + ' sin empezar');
      return '<div class="ev-kpi"><div class="n">' + x.cerradas + ' <small>de ' + x.total + '</small></div><div class="l">' + FASE_TXT[f] + '</div>' +
        '<div class="ev-barrita"><i style="width:' + pc + '%"></i></div>' +
        '<div class="s">' + (x.total ? (partes.length ? esc(partes.join(' · ')) : (x.cerradas === x.total ? 'Todo cerrado' : '—')) : 'Sin órdenes') + '</div></div>';
    }
    function par(p) {
      var m = p.madre, h = p.hija, e = p.estado || {};
      var nivel = e.nivel || 'gris';
      return '<div class="ev-par">' +
        '<div class="ev-par-ot"><small>Instalación</small><b><a href="/ot/' + m.id + '">' + esc(m.numero_ot) + '</a></b>' +
        '<small>' + esc(m.cliente || '') + '</small><small>' + esc(m.fecha || 'Sin fecha') + ' · ' + esc((m.estado || '').replace(/_/g, ' ')) + '</small></div>' +
        '<div class="ev-par-flecha" aria-hidden="true"><i class="bi bi-arrow-right"></i></div>' +
        (h ? '<div class="ev-par-ot ' + esc(nivel) + '"><small>Desinstalación</small><b><a href="/ot/' + h.id + '">' + esc(h.numero_ot) + '</a></b>' +
              '<small>' + esc(h.fecha || 'Sin fecha') + ' · ' + esc((h.estado || '').replace(/_/g, ' ')) + '</small>' +
              '<span class="ev-chip ' + esc(nivel) + '" style="margin-top:6px"><i class="bi ' + (NIVEL_ICO[nivel] || 'bi-info-circle') + '"></i>' + esc(e.texto || '') + '</span></div>'
           : '<div class="ev-par-ot vacio ' + esc(nivel) + '"><small>Desinstalación</small><b>' + (e.codigo === 'ninguna' ? 'No lleva desinstalación' : 'Sin agendar') + '</b>' +
             '<span class="ev-chip ' + esc(nivel) + '" style="margin-top:6px"><i class="bi ' + (NIVEL_ICO[nivel] || 'bi-info-circle') + '"></i>' + esc(e.texto || '') + '</span></div>') +
        '</div>';
    }
    function tabla(d) {
      if (!d.ots.length) return '<div class="ev-vacio">Este evento todavía no tiene órdenes de trabajo.</div>';
      return '<div class="ev-scroll"><table class="ev-tabla"><thead><tr><th>N° OT</th><th>Cliente</th><th>Fase</th><th>Estado</th><th>Fecha</th><th>Técnico</th></tr></thead><tbody>' +
        d.ots.map(function (o) {
          return '<tr><td data-k="N° OT"><a class="ot" href="' + esc(o.url) + '">' + esc(o.numero_ot) + '</a>' +
            (o.madre_numero ? '<small>Desinstalación de ' + esc(o.madre_numero) + '</small>' : '') +
            (o.desinstalacion_prevista && o.fase === 'instalacion' ? '<small>Desinstalación prevista ' + esc(o.desinstalacion_prevista) + '</small>' : '') + '</td>' +
            '<td data-k="Cliente">' + esc(o.cliente) + '</td>' +
            '<td data-k="Fase">' + esc(o.fase_txt) + '<small>' + esc(o.tipo_label) + '</small></td>' +
            '<td data-k="Estado">' + esc(o.estado_label) + '</td>' +
            '<td data-k="Fecha">' + esc(o.fecha || 'Sin fecha') + (o.hora ? '<small>' + esc(o.hora) + ' h</small>' : '') + '</td>' +
            '<td data-k="Técnico">' + esc(o.tecnico || 'Sin asignar') + '</td></tr>';
        }).join('') + '</tbody></table></div>' + pie(d, [25, 50, 100]);
    }

    function pintar(d) {
      D = d;
      var e = d.evento, av = d.avance || {};
      var cancelado = e.estado === 'cancelado';
      var puedeEditar = d.puede_editar && !esTecnico;
      document.title = e.nombre + ' · Evento · ILUS Fitness';
      var h = '<div class="ev-cab"><h2>' + esc(e.nombre) + '<span class="ev-est ' + esc(e.estado) + '">' + esc(e.estado_txt) + '</span></h2>' +
        '<div class="ev-sub">' + [e.cliente, e.lugar].filter(Boolean).map(esc).join(' · ') + '</div>' +
        '<div style="display:flex;flex-wrap:wrap;gap:8px">' +
        '<span class="ev-chip"><i class="bi bi-calendar-event"></i>' + esc(e.fechas_txt || 'Sin fechas definidas') + '</span>' +
        '<span class="ev-chip"><i class="bi bi-clipboard2-pulse"></i>' + e.n_ots + (e.n_ots === 1 ? ' orden' : ' órdenes') + ' de trabajo</span>' +
        (e.semaforo_txt ? '<span class="ev-chip ' + (e.semaforo === 'rojo' ? 'rojo' : (e.semaforo === 'ambar' ? 'ambar' : (e.semaforo === 'verde' ? 'verde' : ''))) + '"><i class="bi ' + (ICO_SEM[e.semaforo] || 'bi-dash-lg') + '"></i>' + esc(e.semaforo_txt) + '</span>' : '') +
        (e.doc_ref ? '<span class="ev-chip"><i class="bi bi-file-earmark-text"></i>Referencia ' + esc(e.doc_ref) + '</span>' : '') + '</div>' +
        (cancelado && e.cancelado_motivo ? '<div class="ev-aviso rojo" style="margin:10px 0 0"><i class="bi bi-slash-circle"></i><span>Evento cancelado' + (e.cancelado_por ? ' por ' + esc(e.cancelado_por) : '') + ': ' + esc(e.cancelado_motivo) + '</span></div>' : '') +
        (e.notas ? '<div class="ev-sub" style="margin-top:10px;color:#e5e7eb"><i class="bi bi-sticky me-1"></i>' + esc(e.notas) + '</div>' : '') +
        '<div class="ev-cab-acc">' +
        (puedeEditar ? '<a class="ev-btn rojo" href="/ot/?crear=1&evento=' + e.id + '"' + (cancelado ? ' aria-disabled="true" style="pointer-events:none;opacity:.45"' : '') + '><i class="bi bi-plus-lg"></i>Agregar OT al evento</a>' +
          '<button type="button" class="ev-btn" id="evAsocBtn" aria-expanded="' + st.asocAbierto + '"' + (cancelado ? ' disabled' : '') + '><i class="bi bi-link-45deg"></i>Asociar OT existentes</button>' +
          '<button type="button" class="ev-btn" id="evEditarBtn"><i class="bi bi-pencil"></i>Editar evento</button>' : '') +
        '<a class="ev-btn" href="/ot/?evento=' + e.id + '"><i class="bi bi-table"></i>Ver en la tabla de OT</a></div></div>';

      if (av.desinst_vencidas) h += '<div class="ev-aviso rojo"><i class="bi bi-exclamation-triangle-fill"></i><span><b>' + av.desinst_vencidas + ' desinstalación' + (av.desinst_vencidas === 1 ? '' : 'es') + ' vencida' + (av.desinst_vencidas === 1 ? '' : 's') + '</b>: la fecha ya pasó y la OT no se ha hecho.</span></div>';
      if (av.desinst_pendientes) h += '<div class="ev-aviso"><i class="bi bi-hourglass-split"></i><span><b>' + av.desinst_pendientes + ' instalación' + (av.desinst_pendientes === 1 ? '' : 'es') + '</b> con la desinstalación sin agendar. Ábrela y usa «Programar desinstalación».</span></div>';

      h += '<div class="ev-kpis"><div class="ev-kpi"><div class="n">' + (av.pct || 0) + '<small>%</small></div><div class="l">Avance del evento</div>' +
        '<div class="ev-barrita"><i style="width:' + (av.pct || 0) + '%"></i></div><div class="s">' + (av.cerradas || 0) + ' de ' + (av.total || 0) + ' cerradas' + (av.proxima_fecha ? ' · próxima ' + esc(bonita(av.proxima_fecha)) : '') + '</div></div>' +
        FASES.filter(function (f) { return f === 'instalacion' || f === 'desinstalacion' || (av.por_fase[f] && av.por_fase[f].total > 0); }).map(function (f) { return kpiFase(f, av); }).join('') + '</div>';

      if (d.pares && d.pares.length) {
        h += '<section class="ev-sec"><h4><i class="bi bi-arrow-left-right"></i>Instalación y desinstalación</h4>' + d.pares.map(par).join('') + '</section>';
      }
      if (d.dias && d.dias.length) {
        h += '<section class="ev-sec"><h4><i class="bi bi-calendar3"></i>Una línea por día</h4>' + d.dias.map(function (x) {
          return '<div class="ev-dia"><div class="f">' + esc(x.fecha) + '<small>' + esc(x.dow) + '</small></div><div class="o">' +
            x.ots.map(function (o) {
              return '<a class="ev-chip ' + (o.fase === 'desinstalacion' ? 'azul' : '') + '" href="/ot/' + o.id + '"><i class="bi ' + (o.fase === 'desinstalacion' ? 'bi-box-arrow-up' : 'bi-tools') + '"></i>' + esc(o.numero_ot) + ' · ' + esc(FASE_TXT[o.fase] || o.fase) + '</a>';
            }).join('') + '</div></div>';
        }).join('') + '</section>';
      }
      h += '<section class="ev-sec" id="evAsoc"' + (st.asocAbierto && puedeEditar ? '' : ' hidden') + '></section>';
      h += '<section class="ev-sec"><h4><i class="bi bi-list-check"></i>Órdenes de trabajo del evento</h4>' + tabla(d) + '</section>';
      raiz.innerHTML = h;
      enlazar(puedeEditar);
      if (st.asocAbierto && puedeEditar) asociar.abrir();
    }

    function enlazar(puedeEditar) {
      var ed = document.getElementById('evEditarBtn');
      if (ed) ed.addEventListener('click', function () {
        window.OTEventoForm.abrir({ evento: D.evento, onGuardado: function () { cargar(); } });
      });
      var ab = document.getElementById('evAsocBtn');
      if (ab) ab.addEventListener('click', function () {
        st.asocAbierto = !st.asocAbierto;
        ab.setAttribute('aria-expanded', st.asocAbierto);
        var sec = document.getElementById('evAsoc');
        if (st.asocAbierto) { sec.hidden = false; asociar.abrir(); sec.scrollIntoView({ behavior: 'smooth', block: 'start' }); } else { sec.hidden = true; }
      });
      Array.prototype.forEach.call(raiz.querySelectorAll('[data-pp]'), function (b) {
        b.addEventListener('click', function () { st.porPag = parseInt(b.getAttribute('data-pp'), 10); st.page = 1; cargar(); });
      });
      Array.prototype.forEach.call(raiz.querySelectorAll('[data-pg]'), function (b) {
        b.addEventListener('click', function () { if (b.disabled) return; st.page = parseInt(b.getAttribute('data-pg'), 10); cargar(); });
      });
    }

    /* ── Asociar OT existentes (REGLA #14: un solo control marca / desmarca TODAS las visibles) ──────────────── */
    var asociar = (function () {
      var s = { q: '', page: 1, porPag: 20, sel: {}, datos: null };
      function pintarLista() {
        var sec = document.getElementById('evAsoc');
        if (!sec) return;
        var d = s.datos;
        var h = '<h4><i class="bi bi-link-45deg"></i>Asociar órdenes de trabajo que ya existen</h4>' +
          '<p class="ev-ayuda" style="margin:-4px 0 10px">Busca por N° de OT, cliente o documento. Las que traen el documento de referencia del evento, el mismo cliente o una fecha dentro del evento aparecen sugeridas. Una OT que ya está en otro evento se mueve a este.</p>' +
          '<form class="ev-barra" id="evAsocForm" role="search"><input class="ev-in" type="search" id="evAsocQ" value="' + esc(s.q) + '" placeholder="N° de OT, cliente o documento…" aria-label="Buscar OT para asociar">' +
          '<button class="ev-btn pri" type="submit"><i class="bi bi-search"></i>Buscar</button></form>';
        if (!d) { sec.innerHTML = h + '<div class="ev-vacio">Buscando…</div>'; enlazarAsoc(); return; }
        if (d.mensaje) h += '<div class="ev-aviso azul"><i class="bi bi-info-circle-fill"></i><span>' + esc(d.mensaje) + '</span></div>';
        if (!d.ots.length) {
          h += '<div class="ev-vacio">No hay órdenes para asociar con esta búsqueda.</div>';
        } else {
          var marcadas = d.ots.filter(function (o) { return s.sel[o.id]; }).length;
          var total = Object.keys(s.sel).filter(function (k) { return s.sel[k]; }).length;
          h += '<div class="ev-asoc"><div class="ev-asoc-top"><button type="button" class="ev-btn" id="evAsocTodo"><i class="bi bi-check-all"></i>Seleccionar/deseleccionar todo</button>' +
            '<span class="ev-ayuda" style="margin:0">' + total + ' seleccionada' + (total === 1 ? '' : 's') + '</span>' +
            '<button type="button" class="ev-btn ok" id="evAsocGo" style="margin-left:auto"' + (total ? '' : ' disabled') + '><i class="bi bi-link-45deg"></i>Asociar ' + (total || '') + ' OT</button></div>' +
            d.ots.map(function (o) {
              return '<label class="ev-asoc-fila' + (o.sugerida ? ' sug' : '') + '"><input type="checkbox" data-id="' + o.id + '"' + (s.sel[o.id] ? ' checked' : '') + '>' +
                '<span class="d"><b>' + esc(o.numero_ot) + ' · ' + esc(o.cliente) + '</b>' +
                '<small>' + esc(o.tipo_label || '') + ' · ' + esc((o.estado || '').replace(/_/g, ' ')) + ' · ' + esc(o.fecha || 'Sin fecha') + '</small>' +
                (o.documentos ? '<small>Documentos: ' + esc(o.documentos) + '</small>' : '') +
                (o.sugerida ? '<small style="color:#92400e;font-weight:800"><i class="bi bi-stars me-1"></i>Sugerida</small>' : '') +
                (o.evento_actual_id && o.evento_actual_id !== id ? '<small style="color:#1e40af">Hoy está en otro evento</small>' : '') + '</span></label>';
            }).join('') + '</div>' + pie(d, [20, 50, 100]);
        }
        sec.innerHTML = h;
        enlazarAsoc();
      }
      function enlazarAsoc() {
        var sec = document.getElementById('evAsoc');
        var f = document.getElementById('evAsocForm');
        if (f) f.addEventListener('submit', function (e) { e.preventDefault(); s.q = (document.getElementById('evAsocQ').value || '').trim(); s.page = 1; cargarCand(); });
        Array.prototype.forEach.call(sec.querySelectorAll('input[type=checkbox][data-id]'), function (c) {
          c.addEventListener('change', function () { s.sel[c.getAttribute('data-id')] = c.checked; pintarLista(); });
        });
        var todo = document.getElementById('evAsocTodo');
        if (todo) todo.addEventListener('click', function () {
          /* Un solo toggle: si alguna visible está sin marcar, marca todas; si ya están todas, las desmarca. */
          var visibles = s.datos.ots.map(function (o) { return o.id; });
          var algunaSin = visibles.some(function (v) { return !s.sel[v]; });
          visibles.forEach(function (v) { s.sel[v] = algunaSin; });
          pintarLista();
        });
        var go = document.getElementById('evAsocGo');
        if (go) go.addEventListener('click', confirmar);
        Array.prototype.forEach.call(sec.querySelectorAll('[data-pp]'), function (b) {
          b.addEventListener('click', function () { s.porPag = parseInt(b.getAttribute('data-pp'), 10); s.page = 1; cargarCand(); });
        });
        Array.prototype.forEach.call(sec.querySelectorAll('[data-pg]'), function (b) {
          b.addEventListener('click', function () { if (b.disabled) return; s.page = parseInt(b.getAttribute('data-pg'), 10); cargarCand(); });
        });
      }
      function confirmar() {
        var vids = Object.keys(s.sel).filter(function (k) { return s.sel[k]; }).map(function (k) { return parseInt(k, 10); });
        if (!vids.length) return;
        window.ilusConfirm({
          title: 'Asociar al evento', message: '¿Asociar ' + vids.length + ' orden' + (vids.length === 1 ? '' : 'es') + ' de trabajo a «' + D.evento.nombre + '»?',
          sub: 'Las desinstalaciones se mueven solas con su instalación. Puedes cambiarlo después desde la ficha de cada OT.',
          okLabel: 'Sí, asociar', cancelLabel: 'Todavía no', type: 'question'
        }).then(function (ok) {
          if (!ok) return;
          api('/ot/api/eventos/' + id + '/asociar', { vids: vids }).then(function (r) {
            var j = r.j || {};
            var oms = j.omitidas || [];
            if (!r.ok && !(j.asociadas && j.asociadas.length)) {
              toast(j.error || (oms.length ? 'No se pudo asociar ninguna OT: ' + (oms[0].motivo || '') : 'No se pudo asociar.'), 'error');
              if (oms.length) mostrarOmitidas(oms);
              return;
            }
            toast('✓ ' + (j.asociadas || []).length + ' OT asociada' + ((j.asociadas || []).length === 1 ? '' : 's') + ' al evento', 'success');
            s.sel = {};
            cargar(function () {
              st.asocAbierto = true;
              if (oms.length) mostrarOmitidas(oms);
            });
          });
        });
      }
      function mostrarOmitidas(oms) {
        var sec = document.getElementById('evAsoc');
        if (!sec) return;
        sec.hidden = false;
        var div = document.createElement('div');
        div.className = 'ev-aviso';
        div.innerHTML = '<i class="bi bi-exclamation-triangle-fill"></i><span><b>No se asociaron ' + oms.length + ':</b><br>' + oms.map(function (o) {
          return 'OT ' + esc(o.id) + ' — ' + esc(o.motivo || o.error_codigo || '');
        }).join('<br>') + '</span>';
        sec.insertBefore(div, sec.children[1] || null);
      }
      function cargarCand() {
        api('/ot/api/eventos/' + id + '/candidatas?q=' + encodeURIComponent(s.q) + '&page=' + s.page + '&per_page=' + s.porPag).then(function (r) {
          if (!r.ok) { s.datos = { ots: [], mensaje: r.j.error || 'No se pudo buscar.', total: 0, page: 1, pages: 1, per_page: s.porPag, desde: 0, hasta: 0 }; } else { s.datos = r.j; }
          pintarLista();
        });
      }
      return { abrir: function () { s.datos = null; pintarLista(); cargarCand(); } };
    })();

    function cargar(despues) {
      guardarUrl({ page: st.page > 1 ? st.page : '', per_page: st.porPag !== 25 ? st.porPag : '' });
      api('/ot/api/eventos/' + id + '?page=' + st.page + '&per_page=' + st.porPag).then(function (r) {
        if (!r.ok) {
          raiz.innerHTML = '<div class="ev-error">' + esc(r.j.error || 'No se pudo leer el evento.') +
            '<br><a class="ev-btn" style="margin-top:10px" href="/ot/eventos"><i class="bi bi-arrow-left"></i>Volver a los eventos</a></div>';
          return;
        }
        if (r.j.page && r.j.page !== st.page) st.page = r.j.page;
        pintar(r.j);
        if (typeof despues === 'function') despues();
      });
    }
    cargar();
  }

  if (modo === 'detalle') detalle(); else lista();
})();

/* ══════════════════════════════════════════════════════════════════════════════════════════════════════════════
   maquina_historia.js — «Historia del equipo» (hoja de vida unificada) en la ficha del equipo.

   Daniel (2026-10-21): «Si entro a determinado producto, una trotadora de determinada cuenta, me va a decir todos los diagnósticos
   que ha tenido, visitas, solicitudes de repuesto, todo bien ordenadito.» Esta pantalla solo DIBUJA lo que arma el servidor en
   GET /mantenciones/api/maquinas/<id>/historia (ya unificado, sin duplicados y recortado para el rol de quien mira): la línea de
   tiempo en tarjetas con semáforo, las pastillas por tipo con su contador, el rango de fechas y el pie «Mostrando A–B de N».

   · Paginación real (REGLA #4.3): cada filtro, página o tamaño vuelve a pedir al servidor; vaciar un filtro también recarga.
   · Nada de alert/confirm/prompt nativos (REGLA #1): los avisos son bloques en pantalla (y ilusToast si existe).
   · TODO texto que viene del servidor se escapa (esc) antes de entrar a un HTML; los enlaces solo aceptan «/…» o https.
   · Las fechas llegan ya en hora de Chile (REGLA #6): aquí no se formatea ni se convierte nada.
   ══════════════════════════════════════════════════════════════════════════════════════════════════════════════ */
(function (global) {
  'use strict';

  // ── utilidades ─────────────────────────────────────────────────────────────────────────────────────────────
  function esc(s) {
    return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
    });
  }
  function urlOk(u) {
    u = String(u == null ? '' : u).trim();
    return (u.charAt(0) === '/' && u.charAt(1) !== '/') || u.indexOf('https://') === 0 ? u : '';
  }
  function colorOk(c) { return /^(rojo|ambar|azul|verde|gris|negro)$/.test(c || '') ? c : 'gris'; }
  function icoOk(i) { return /^bi-[a-z0-9-]+$/.test(i || '') ? i : 'bi-circle'; }
  function sevOk(s) { return /^(ok|aviso|falla|info)$/.test(s || '') ? s : 'info'; }
  function plural(n, uno, varios) { return n + ' ' + (n === 1 ? uno : varios); }
  function chip(texto, color, icono) {
    return '<span class="mh-chip c-' + colorOk(color) + '">' + (icono ? '<i class="bi ' + icoOk(icono) + '" aria-hidden="true"></i> ' : '') + esc(texto) + '</span>';
  }

  // Etiqueta corta del subtipo (solo donde el título no lo dice ya).
  var SUBTIPOS = {
    hallazgo: { falla: 'Falla', alerta: 'Alerta', fuera_servicio: 'Fuera de servicio', baja: 'Baja', repuesto: 'Repuesto', incidencia: 'Incidencia' },
    estado: { fuera_servicio: 'Fuera de servicio', en_reparacion: 'En reparación', baja: 'Baja', cambio: 'Cambio de estado', restaurado: 'Restaurado' },
    ficha: { serie: 'N° de serie', ubicacion: 'Ubicación', dato: 'Dato corregido', alta: 'Alta' },
    repuesto: { piola: 'Piola', cinta: 'Cinta' }
  };

  // ── piezas de la tarjeta ───────────────────────────────────────────────────────────────────────────────────
  function lineasHtml(ls) {
    if (!ls || !ls.length) return '';
    return '<ul class="mh-lineas">' + ls.map(function (l) {
      return '<li><i class="bi ' + icoOk(l.icono) + '" aria-hidden="true"></i><span>' + esc(l.texto) + '</span></li>';
    }).join('') + '</ul>';
  }
  function pasosHtml(ps) {
    if (!ps || ps.length < 2) return '';
    return '<ol class="mh-pasos">' + ps.map(function (p) {
      return '<li class="' + (p.hecho ? 'hecho' : 'pend') + '"><b>' + esc(p.texto) + '</b>' + (p.fecha ? '<small>' + esc(p.fecha) + '</small>' : '') + '</li>';
    }).join('') + '</ol>';
  }
  function gestionHtml(g) {
    if (!g) return '';
    var seg = '';
    if (g.seguimiento && g.seguimiento.numero) {
      var u = urlOk(g.seguimiento.url);
      seg = ' · OT de seguimiento ' + (u ? '<a href="' + esc(u) + '">' + esc(g.seguimiento.numero) + '</a>' : esc(g.seguimiento.numero));
    }
    return '<div class="mh-gest c-' + colorOk(g.color) + '"><i class="bi bi-exclamation-diamond-fill" aria-hidden="true"></i><span>' + esc(g.texto) + seg + '</span></div>';
  }
  function fotosHtml(e) {
    var fs = e.fotos || [];
    if (!fs.length) return '';
    return '<div class="mh-fotos">' + fs.map(function (u, i) {
      var url = urlOk(u);
      return url ? '<button type="button" class="mh-foto" data-foto="' + i + '" aria-label="Ver la foto ' + (i + 1) + ' de ' + fs.length + '"><img src="' + esc(url) + '" alt="Foto ' + (i + 1) + '" loading="lazy"></button>' : '';
    }).join('') + '</div>';
  }
  function enlacesHtml(e) {
    var items = [];
    (e.refs || []).forEach(function (r) { items.push(r); });
    if (!items.length) return '';
    return '<div class="mh-enlaces">' + items.map(function (r) {
      var u = urlOk(r.url);
      return u
        ? '<a class="mh-enl" href="' + esc(u) + '"><i class="bi bi-box-arrow-up-right" aria-hidden="true"></i> ' + esc(r.texto) + '</a>'
        : '<span class="mh-enl sin"><i class="bi bi-lock-fill" aria-hidden="true"></i> ' + esc(r.numero || r.texto) + '</span>';
    }).join('') + '</div>';
  }
  // Las cuatro respuestas, compactas: QUÉ es el título de la tarjeta; aquí van POR QUÉ, CUÁNDO y la OT (número, estado y enlace).
  function otHtml(e) {
    var r = e.ref;
    if (!r) return e.pide_motivo ? '<span class="mh-sin">sin OT ligada</span>' : '';
    var u = urlOk(r.url);
    var est = r.estado ? ' <span class="mh-chip ' + (r.en_curso ? 'c-azul' : 'c-gris') + '">' + esc(r.estado) + '</span>' : '';
    return (u ? '<a class="mh-enl" href="' + esc(u) + '"><i class="bi bi-box-arrow-up-right" aria-hidden="true"></i> ' + esc(r.texto) + '</a>'
              : '<span class="mh-enl sin"><i class="bi bi-lock-fill" aria-hidden="true"></i> ' + esc(r.numero || r.texto) + '</span>') + est;
  }
  function cuatroHtml(e) {
    var porque = e.pide_motivo ? (e.por_que ? esc(e.por_que) : '<span class="mh-sin">sin motivo registrado</span>') : '';
    var cuando = esc(e.fecha) + (e.fecha_fin ? ' <span class="mh-hasta">→ ' + esc(e.fecha_fin) + '</span>' : '');
    var ot = otHtml(e);
    return '<dl class="mh-4w">' + (porque ? '<div class="mh-por"><dt>Por qué</dt><dd>' + porque + '</dd></div>' : '') +
      '<div><dt>Cuándo</dt><dd><time>' + cuando + '</time></dd></div>' + (ot ? '<div><dt>OT</dt><dd>' + ot + '</dd></div>' : '') + '</dl>';
  }
  function tarjetaHtml(e) {
    var sub = (SUBTIPOS[e.tipo] || {})[e.subtipo];
    var h = '<li class="mh-card s-' + sevOk(e.severidad) + (e.abierto ? ' abierto' : '') + (e.futuro ? ' futuro' : '') + '" data-id="' + esc(e.id) +
      '" id="mh-' + esc(String(e.id).replace(/[^A-Za-z0-9_-]/g, '-')) + '">';
    h += '<span class="mh-nodo" aria-hidden="true"><i class="bi ' + icoOk(e.icono) + '"></i></span>';
    h += '<div class="mh-cuerpo"><div class="mh-fila1">';
    h += '<span class="mh-chip tipo"><i class="bi ' + icoOk(e.icono) + '" aria-hidden="true"></i> ' + esc(e.tipo_texto) + '</span>';
    if (sub) h += '<span class="mh-chip sub">' + esc(sub) + '</span>';
    if (e.estado_txt) h += '<span class="mh-chip c-' + colorOk(e.estado_color) + '">' + esc(e.estado_txt) + '</span>';
    if (e.riesgo) h += '<span class="mh-chip c-rojo"><i class="bi bi-person-exclamation" aria-hidden="true"></i> Puede lastimar a alguien</span>';
    if (e.ligada_ot) h += '<span class="mh-chip sub"><i class="bi bi-clipboard2-pulse" aria-hidden="true"></i> Ligado a OT</span>';
    h += '</div>';
    h += '<h4 class="mh-tit">' + esc(e.titulo) + '</h4>';
    h += cuatroHtml(e);
    if (e.detalle && e.detalle !== e.por_que) h += '<p class="mh-det">' + esc(e.detalle) + '</p>';
    h += lineasHtml(e.lineas) + pasosHtml(e.pasos) + gestionHtml(e.gestion) + fotosHtml(e);
    if (e.actor) h += '<div class="mh-meta"><i class="bi bi-person-fill" aria-hidden="true"></i> Registró ' + esc(e.actor) + '</div>';
    h += enlacesHtml(e) + '</div></li>';
    return h;
  }
  function listaHtml(entradas) {
    var mes = null, h = '';
    (entradas || []).forEach(function (e) {
      if (e.mes && e.mes !== mes) {
        mes = e.mes;
        h += '<li class="mh-mes" role="presentation"><span>' + esc(e.mes_txt || e.mes) + '</span></li>';
      }
      h += tarjetaHtml(e);
    });
    return h;
  }

  // ── cabecera, pendiente, pastillas y pie ───────────────────────────────────────────────────────────────────
  function pendienteHtml(r) {
    r = r || {};
    var cs = [];
    if (r.fuera_servicio) cs.push(chip('Fuera de servicio desde ' + r.fuera_servicio.desde + (r.fuera_servicio.hace ? ' (' + r.fuera_servicio.hace + ')' : ''), 'rojo', 'bi-sign-stop-fill'));
    if (r.hallazgos_abiertos) cs.push(chip(plural(r.hallazgos_abiertos, 'hallazgo sin gestionar', 'hallazgos sin gestionar'), 'ambar', 'bi-exclamation-diamond-fill'));
    if (r.repuestos_abiertos) cs.push(chip(plural(r.repuestos_abiertos, 'repuesto en curso', 'repuestos en curso'), 'azul', 'bi-tools'));
    if (r.tickets_abiertos) cs.push(chip(plural(r.tickets_abiertos, 'ticket abierto', 'tickets abiertos'), 'ambar', 'bi-ticket-detailed-fill'));
    if (!cs.length) cs.push(chip('Sin pendientes', 'verde', 'bi-check-circle-fill'));
    if (r.ultima_visita) cs.push(chip('Última visita: ' + r.ultima_visita.fecha, 'gris', 'bi-clipboard2-pulse-fill'));
    return '<div class="mh-pend"><span class="mh-pend-t">Pendiente ahora</span>' + cs.join('') + '</div>';
  }
  function fraseHtml(d) {
    var f = d.frase;
    if (!f) return '';
    var mapa = d.gps && urlOk(d.gps.url) ? ' <a class="mh-enl" href="' + esc(d.gps.url) + '" target="_blank" rel="noopener"><i class="bi bi-geo-alt-fill" aria-hidden="true"></i> Ver en el mapa</a>' : '';
    return '<div class="mh-frase" id="mhFrase"><p><i class="bi bi-geo-alt-fill" aria-hidden="true"></i><span>' + esc(f.donde) + mapa + '</span></p>' +
      '<p><i class="bi bi-clipboard2-pulse-fill" aria-hidden="true"></i><span>' + esc(f.ultima) + '</span></p>' +
      '<p><i class="bi bi-activity" aria-hidden="true"></i><span>' + esc(f.hoy) + '</span></p></div>';
  }
  function cabeceraHtml(d) {
    var q = d.equipo || {}, est = q.estado || {}, gar = q.garantia || {};
    var dato = function (rot, val) { return '<div><dt>' + esc(rot) + '</dt><dd>' + val + '</dd></div>'; };
    return '<section class="mh-cab e-' + colorOk(est.color) + '" id="mhCab" aria-labelledby="mhTitulo">' +
      '<div class="mh-cab-fila"><span class="mh-ic" aria-hidden="true"><i class="bi bi-journal-medical"></i></span>' +
      '<div class="mh-cab-tx"><h3 id="mhTitulo">Historia de ' + esc(q.nombre) + '</h3>' +
      '<p>Todo lo que le ha pasado a este equipo, en orden y sin repetir nada. Cada hecho de terreno dice qué se hizo, por qué, cuándo y en qué OT.</p></div>' +
      chip(est.texto || 'Operativo', est.color, 'bi-activity') + '</div>' + fraseHtml(d) +
      '<dl class="mh-datos">' +
      dato('N° de serie', q.serie ? '<span class="mh-sn">' + esc(q.serie) + '</span>' : '—') +
      dato('SKU', q.sku ? esc(q.sku) : '—') +
      dato('Cliente', q.cliente ? esc(q.cliente) : '—') +
      (q.marca || q.modelo ? dato('Marca y modelo', esc([q.marca, q.modelo].filter(Boolean).join(' · '))) : '') +
      (q.ubicacion ? dato('Ubicación', esc(q.ubicacion)) : '') +
      dato('Garantía', chip(gar.texto || 'Sin garantía registrada', gar.color, 'bi-shield-check')) +
      '</dl>' + pendienteHtml(d.resumen) + '</section>';
  }
  function pillsHtml(d, tipoActivo, soloOt) {
    var h = '<button type="button" class="mh-pill' + (tipoActivo ? '' : ' on') + '" data-tipo="" aria-pressed="' + (tipoActivo ? 'false' : 'true') +
      '"><i class="bi bi-collection" aria-hidden="true"></i> Todos <b>' + (d.todos || 0) + '</b></button>';
    (d.tipos || []).forEach(function (t) {
      if (!t.n && t.clave !== tipoActivo) return;
      h += '<button type="button" class="mh-pill' + (t.clave === tipoActivo ? ' on' : '') + '" data-tipo="' + esc(t.clave) + '" aria-pressed="' +
        (t.clave === tipoActivo ? 'true' : 'false') + '"><i class="bi ' + icoOk(t.icono) + '" aria-hidden="true"></i> ' + esc(t.texto) + ' <b>' + (t.n || 0) + '</b></button>';
    });
    h += '<button type="button" class="mh-pill ot' + (soloOt ? ' on' : '') + '" data-solo-ot="1" aria-pressed="' + (soloOt ? 'true' : 'false') +
      '"><i class="bi bi-clipboard2-pulse" aria-hidden="true"></i> Solo lo ligado a una OT <b>' + (d.n_ot || 0) + '</b></button>';
    return h;
  }
  function pieHtml(m) {
    m = m || {};
    var opc = [10, 20, 50, 100].map(function (n) {
      return '<option value="' + n + '"' + (n === m.por_pagina ? ' selected' : '') + '>' + n + ' por página</option>';
    }).join('');
    return '<div class="mh-pie-t" aria-live="polite"><i class="bi bi-collection" aria-hidden="true"></i>' +
      (m.total ? '<span>Mostrando <b>' + m.desde + '–' + m.hasta + '</b> de <b>' + m.total + '</b></span>' : '<span>Sin resultados</span>') + '</div>' +
      '<div class="mh-pie-c"><label><span class="vh">Registros por página</span><select id="mhPP">' + opc + '</select></label>' +
      '<button type="button" class="mh-btn sec" data-accion="ant"' + (m.pagina > 1 ? '' : ' disabled') + '><i class="bi bi-chevron-left" aria-hidden="true"></i> Anterior</button>' +
      '<span class="mh-pag-n">Página ' + (m.pagina || 1) + ' de ' + (m.paginas || 1) + '</span>' +
      '<button type="button" class="mh-btn pri" data-accion="sig"' + (m.pagina < m.paginas ? '' : ' disabled') + '>Siguiente <i class="bi bi-chevron-right" aria-hidden="true"></i></button></div>';
  }
  function vacioHtml(hayFiltros) {
    return hayFiltros
      ? '<div class="mh-estado"><i class="bi bi-funnel" aria-hidden="true"></i><b>No hay registros con estos filtros</b><p>Prueba con otro tipo o con otro rango de fechas.</p>' +
        '<button type="button" class="mh-btn" data-accion="quitar"><i class="bi bi-x-circle" aria-hidden="true"></i> Quitar filtros</button></div>'
      : '<div class="mh-estado"><i class="bi bi-journal-medical" aria-hidden="true"></i><b>Este equipo todavía no tiene historia registrada</b>' +
        '<p>Cuando tenga visitas, diagnósticos, repuestos o cambios, van a aparecer aquí, uno bajo el otro, del más nuevo al más antiguo.</p></div>';
  }
  function errorHtml(msg) {
    return '<div class="mh-estado err" role="alert"><i class="bi bi-exclamation-triangle-fill" aria-hidden="true"></i><b>No pudimos cargar la historia del equipo</b><p>' +
      esc(msg || 'Revisa tu conexión e inténtalo de nuevo.') + '</p>' +
      '<button type="button" class="mh-btn" data-accion="reintentar"><i class="bi bi-arrow-clockwise" aria-hidden="true"></i> Reintentar</button></div>';
  }
  function esqueletoHtml() { return '<div class="mh-esq"></div><div class="mh-esq"></div><div class="mh-esq"></div>'; }

  // ── el controlador (solo se arma en la ficha del equipo) ───────────────────────────────────────────────────
  function guardar(clave, valor) { try { global.localStorage.setItem(clave, valor); } catch (e) { /* sin almacenamiento: no pasa nada */ } }
  function leerGuardado(clave) { try { return global.localStorage.getItem(clave); } catch (e) { return null; } }

  function iniciar(raiz) {
    var mid = raiz.getAttribute('data-mid');
    var base = '/mantenciones/api/maquinas/' + encodeURIComponent(mid) + '/historia';
    var q = new URLSearchParams(global.location.search);
    var pp = parseInt(q.get('pp') || leerGuardado('ilus_mh_pp') || '20', 10);
    var st = { pagina: Math.max(1, parseInt(q.get('pagina') || '1', 10) || 1), pp: [10, 20, 50, 100].indexOf(pp) >= 0 ? pp : 20,
               tipo: (q.get('tipos') || '').split(',')[0] || '', desde: q.get('desde') || '', hasta: q.get('hasta') || '', soloOt: q.get('solo_ot') === '1' };
    var token = 0, armado = false, ultimo = null, fotosPorId = {};
    var el = {};

    function armar() {
      raiz.innerHTML = '<div id="mhCabecera"></div>' +
        '<section class="mh-filtros" aria-label="Filtros de la historia"><div class="mh-pills" id="mhPills" role="group" aria-label="Filtrar por tipo"></div>' +
        '<div class="mh-fechas"><label>Desde<input type="date" id="mhDesde"></label><label>Hasta<input type="date" id="mhHasta"></label>' +
        '<button type="button" class="mh-btn sec" data-accion="quitar" id="mhQuitar" hidden><i class="bi bi-x-circle" aria-hidden="true"></i> Quitar filtros</button></div></section>' +
        '<div id="mhAvisos"></div><ol class="mh-lista" id="mhLista"></ol><div id="mhEstado"></div><footer class="mh-pie" id="mhPie" hidden></footer>';
      ['mhCabecera', 'mhPills', 'mhDesde', 'mhHasta', 'mhQuitar', 'mhAvisos', 'mhLista', 'mhEstado', 'mhPie'].forEach(function (id) { el[id] = raiz.querySelector('#' + id); });
      el.mhDesde.value = st.desde; el.mhHasta.value = st.hasta;
      armado = true;
    }
    function hayFiltros() { return !!(st.tipo || st.desde || st.hasta || st.soloOt); }
    function sincronizarUrl() {
      try {
        var p = new URLSearchParams(global.location.search);
        p.set('tab', 'historia');
        [['tipos', st.tipo], ['desde', st.desde], ['hasta', st.hasta], ['solo_ot', st.soloOt ? '1' : '']].forEach(function (kv) { if (kv[1]) p.set(kv[0], kv[1]); else p.delete(kv[0]); });
        if (st.pagina > 1) p.set('pagina', st.pagina); else p.delete('pagina');
        if (st.pp !== 20) p.set('pp', st.pp); else p.delete('pp');
        global.history.replaceState(null, '', global.location.pathname + '?' + p.toString());
      } catch (e) { /* la URL es solo comodidad */ }
    }
    function avisos(d) {
      var h = '';
      if (d.fuentes_no_disponibles && d.fuentes_no_disponibles.length) h += '<div class="mh-aviso"><i class="bi bi-info-circle" aria-hidden="true"></i><span>Una parte de la historia no se pudo leer en este momento, así que puede faltar algo. Vuelve a intentar en un minuto.</span></div>';
      if (d.fuentes_topadas && d.fuentes_topadas.length) h += '<div class="mh-aviso"><i class="bi bi-info-circle" aria-hidden="true"></i><span>Este equipo tiene muchísimos registros: de cada tipo se muestran los más recientes.</span></div>';
      el.mhAvisos.innerHTML = h;
    }
    function pintar(d) {
      ultimo = d;
      el.mhCabecera.innerHTML = cabeceraHtml(d);
      el.mhPills.innerHTML = pillsHtml(d, st.tipo, st.soloOt);
      el.mhDesde.value = d.filtros && d.filtros.desde || '';
      el.mhHasta.value = d.filtros && d.filtros.hasta || '';
      st.desde = el.mhDesde.value; st.hasta = el.mhHasta.value;
      el.mhQuitar.hidden = !hayFiltros();
      avisos(d);
      st.pagina = d.meta.pagina;
      fotosPorId = {};
      (d.entradas || []).forEach(function (e) { fotosPorId[e.id] = e.fotos || []; });
      if (d.entradas && d.entradas.length) {
        el.mhLista.hidden = false;
        el.mhLista.innerHTML = listaHtml(d.entradas);
        el.mhEstado.innerHTML = '';
      } else {
        el.mhLista.hidden = true;
        el.mhLista.innerHTML = '';
        el.mhEstado.innerHTML = vacioHtml(hayFiltros());
      }
      el.mhPie.hidden = !(d.meta && d.meta.total);
      el.mhPie.innerHTML = pieHtml(d.meta);
      sincronizarUrl();
    }
    function cargar(irArriba) {
      if (!armado) armar();
      var mi = ++token;
      el.mhLista.classList.add('cargando');
      if (!ultimo) { el.mhEstado.innerHTML = esqueletoHtml(); el.mhLista.hidden = true; }
      var p = new URLSearchParams();
      p.set('pagina', st.pagina); p.set('por_pagina', st.pp);
      if (st.tipo) p.set('tipos', st.tipo);
      if (st.desde) p.set('desde', st.desde);
      if (st.hasta) p.set('hasta', st.hasta);
      if (st.soloOt) p.set('solo_ot', '1');
      global.fetch(base + '?' + p.toString(), { credentials: 'same-origin', headers: { 'Accept': 'application/json' } })
        .then(function (r) { return r.json().catch(function () { return { ok: false }; }).then(function (j) { return { http: r.status, j: j }; }); })
        .then(function (x) {
          if (mi !== token) return;                                   // llegó una respuesta más nueva: esta se descarta
          el.mhLista.classList.remove('cargando');
          if (!x.j || !x.j.ok) { fallo(x.j && x.j.error); return; }
          pintar(x.j);
          if (irArriba) { var c = raiz.querySelector('#mhCab'); if (c && c.scrollIntoView) c.scrollIntoView({ behavior: 'smooth', block: 'start' }); }
        })
        .catch(function () { if (mi !== token) return; el.mhLista.classList.remove('cargando'); fallo(); });
    }
    function fallo(msg) {
      el.mhLista.hidden = true;
      el.mhPie.hidden = true;
      el.mhEstado.innerHTML = errorHtml(msg);
      if (typeof global.ilusToast === 'function') global.ilusToast('No pudimos cargar la historia del equipo', { type: 'error' });
    }

    raiz.addEventListener('click', function (ev) {
      var t = ev.target && ev.target.closest ? ev.target : null;
      if (!t) return;
      var pill = t.closest('.mh-pill');
      if (pill && pill.hasAttribute('data-solo-ot')) { st.soloOt = !st.soloOt; st.pagina = 1; cargar(false); return; }
      if (pill) { st.tipo = pill.getAttribute('data-tipo') === st.tipo ? '' : (pill.getAttribute('data-tipo') || ''); st.pagina = 1; cargar(false); return; }
      var foto = t.closest('.mh-foto');
      if (foto) {
        var card = foto.closest('.mh-card'), fs = (card && fotosPorId[card.getAttribute('data-id')]) || [];
        var i = parseInt(foto.getAttribute('data-foto'), 10) || 0;
        if (typeof global.ilusLightbox === 'function') global.ilusLightbox(fs, i, 'Fotos de la historia del equipo');
        else if (fs[i]) global.open(fs[i], '_blank', 'noopener');
        return;
      }
      var acc = t.closest('[data-accion]');
      if (!acc) return;
      var a = acc.getAttribute('data-accion');
      if (a === 'quitar') { st.tipo = ''; st.soloOt = false; st.desde = ''; st.hasta = ''; el.mhDesde.value = ''; el.mhHasta.value = ''; st.pagina = 1; cargar(false); }
      else if (a === 'ant' && st.pagina > 1) { st.pagina -= 1; cargar(true); }
      else if (a === 'sig' && ultimo && st.pagina < ultimo.meta.paginas) { st.pagina += 1; cargar(true); }
      else if (a === 'reintentar') { ultimo = null; cargar(false); }
    });
    raiz.addEventListener('change', function (ev) {
      var t = ev.target;
      if (!t || !t.id) return;
      if (t.id === 'mhDesde') { st.desde = t.value || ''; st.pagina = 1; cargar(false); }        // vaciar el campo también recarga (REGLA #4.3)
      else if (t.id === 'mhHasta') { st.hasta = t.value || ''; st.pagina = 1; cargar(false); }
      else if (t.id === 'mhPP') { st.pp = parseInt(t.value, 10) || 20; guardar('ilus_mh_pp', String(st.pp)); st.pagina = 1; cargar(true); }
    });

    return { activar: function () { if (!armado) cargar(false); }, recargar: function () { cargar(false); } };
  }

  var instancia = null;
  function activar() {
    if (!instancia) {
      var raiz = global.document && global.document.getElementById('mhRaiz');
      if (!raiz) return;
      instancia = iniciar(raiz);
    }
    instancia.activar();
  }

  global.MaquinaHistoria = {
    activar: activar,
    _test: { esc: esc, urlOk: urlOk, tarjetaHtml: tarjetaHtml, listaHtml: listaHtml, pasosHtml: pasosHtml, pendienteHtml: pendienteHtml,
             cabeceraHtml: cabeceraHtml, pillsHtml: pillsHtml, cuatroHtml: cuatroHtml, fraseHtml: fraseHtml, pieHtml: pieHtml, vacioHtml: vacioHtml, errorHtml: errorHtml, enlacesHtml: enlacesHtml,
             gestionHtml: gestionHtml, fotosHtml: fotosHtml }
  };
})(typeof window !== 'undefined' ? window : this);

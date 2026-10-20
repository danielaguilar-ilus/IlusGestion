/* 📒 Saldo de facturas (Daniel 2026-10-11, antes de la auditoría del martes).
   «Si ya utilicé una factura y se ocupó el saldo en otra orden de trabajo, ese saldo tiene que quedar bloqueado…
   necesito decirle [al auditor] que mi sistema se encarga de eso».
   · Consulta «¿Puedo usar esta factura?»: GET /ot/api/saldo-facturas/doc/<tido>/<nudo> (saldo en vivo, el MISMO
     motor del candado). Barras por categoría: cada OT es un tramo, lo libre va en verde, el exceso en rojo.
   · Listado paginado con semáforo (REGLA #4.3): GET /ot/api/saldo-facturas.
   · Revisión completa del libro (también la carga inicial): POST /ot/api/saldo-facturas/revisar, por tramos.
   Nada de alert/confirm nativos (REGLA #1): ilusToast. */
(function () {
  'use strict';
  var app = document.getElementById('sfApp');
  if (!app) return;

  var ICONO = { libre: 'bi-circle', con_saldo: 'bi-pie-chart-fill', agotada: 'bi-lock-fill', excedida: 'bi-exclamation-octagon-fill',
    sin_lineas: 'bi-dash-circle', sin_leer: 'bi-cloud-slash' };
  var ORDEN = ['excedida', 'agotada', 'con_saldo', 'libre', 'sin_lineas', 'sin_leer'];
  var TIPO_LARGO = { FCV: 'Factura', FCE: 'Factura exenta', BLV: 'Boleta', BLE: 'Boleta exenta', NVV: 'Nota de venta', NVI: 'Nota de venta interna', VD: 'Nota de venta', WEB: 'Nota de venta web' };
  var CAT = { servicio: 'Instalación o servicio', despacho: 'Despacho' };
  var st = { estado: '', q: '', page: 1, per: 50, estados: {} };
  /* Revisión 2026-10-11: una respuesta vieja nunca pisa a la nueva (REGLA #4.3: limpiar el filtro recarga de verdad). */
  var seqLista = 0, seqDoc = 0;
  var LS_REV = 'sf_rev_desde';

  function esc(s) { return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) { return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]; }); }
  function clp(n) { var v = Math.round(Number(n) || 0); return (v < 0 ? '-$' : '$') + Math.abs(v).toLocaleString('es-CL'); }
  function toast(m, t) { if (window.ilusToast) window.ilusToast(m, { type: t || 'info' }); }
  function api(url, opt) {
    opt = opt || {};
    var init = { method: opt.method || 'GET', credentials: 'same-origin', headers: { 'Accept': 'application/json' } };
    if (opt.body) { init.headers['Content-Type'] = 'application/json'; init.body = JSON.stringify(opt.body); }
    return fetch(url, init).then(function (r) {
      return r.json().then(function (j) { return { ok: r.ok && j && j.ok !== false, status: r.status, j: j || {} }; },
        function () { return { ok: false, status: r.status, j: { error: 'Respuesta inesperada del servidor (' + r.status + ').' } }; });
    }, function () { return { ok: false, status: 0, j: { error: 'Sin conexión: revisa internet y reintenta.' } }; });
  }
  function lsGet(k) { try { return window.localStorage.getItem(k); } catch (e) { return null; } }
  function lsSet(k, v) { try { if (v == null) window.localStorage.removeItem(k); else window.localStorage.setItem(k, v); } catch (e) { } }
  function pct(a, b) { return b > 0 ? Math.max(0, Math.min(100, a * 100 / b)) : 0; }
  function numDoc(n) { return String(n || '').replace(/^(VD|WEB)?0+(?=\d)/, '$1'); }

  /* ── Consulta ──────────────────────────────────────────────────────────────────────────────────────────── */
  var fTipo = document.getElementById('sfTipo'), fNum = document.getElementById('sfNum'), fForm = document.getElementById('sfForm');
  var res = document.getElementById('sfRes');

  /* «FCV 11439», «fcv11439», «VD 10.666» o «WEB 532» pegados en el número: se separa el tipo solo. Las notas de venta
     de ILUS viven en Random como «VD…» (y las web como «WEB…»): una «NVV» con número suelto se consulta como VD. */
  function leerEntrada() {
    var raw = (fNum.value || '').trim().toUpperCase().replace(/\./g, '');
    var m = raw.match(/^(FCV|FCE|BLV|BLE|NVV|NVI|VD|WEB)\s*[-#N°º:]*\s*(\d+)$/);
    var tipo = fTipo.value, num = raw.replace(/[^0-9]/g, '');
    if (m) {
      tipo = m[1] === 'NVV' ? 'VD' : m[1];
      num = m[2];
      if (fTipo.querySelector('option[value="' + tipo + '"]')) fTipo.value = tipo;
      fNum.value = num;
    }
    return { tipo: tipo, num: num };
  }

  fForm.addEventListener('submit', function (e) {
    e.preventDefault();
    var x = leerEntrada();
    if (!x.num) { toast('Escribe el número del documento.', 'warning'); fNum.focus(); return; }
    consultar(x.tipo, x.num, true);
  });

  function consultar(tido, nudo, mover) {
    var btn = fForm.querySelector('.sf-go');
    var seq = ++seqDoc;
    btn.disabled = true;
    res.hidden = false;
    res.innerHTML = '<div class="pp-vacio">Leyendo ' + esc(tido + ' ' + nudo) + ' en Random y en las OT…</div>';
    if (mover) res.scrollIntoView({ behavior: 'smooth', block: 'start' });
    return api('/ot/api/saldo-facturas/doc/' + encodeURIComponent(tido) + '/' + encodeURIComponent(nudo)).then(function (r) {
      if (seq !== seqDoc) return;   // llegó tarde: ya se pidió otro documento
      btn.disabled = false;
      if (!r.ok) {
        res.innerHTML = '<div class="pp-error"><i class="bi bi-exclamation-triangle-fill"></i> ' + esc(r.j.error || 'No se pudo leer el documento.') + '</div>';
        return;
      }
      pintarDoc(r.j);
      try { var u = new URL(location.href); u.searchParams.set('doc', tido + '-' + nudo); history.replaceState(null, '', u.toString()); } catch (e) { }
    });
  }

  function medidor(cat, s, usos) {
    s = s || {};
    var total = Number(s.total) || 0, usado = Number(s.usado) || 0;
    if (total <= 0 && usado <= 0) {
      return '<div class="sf-med nada"><div class="sf-med-h"><b>' + esc(CAT[cat]) + '</b></div>Esta factura no trae línea de ' +
        (cat === 'despacho' ? 'despacho (ZZENVIO)' : 'instalación ni mantención (ZZ)') + ': no hay saldo que controlar aquí.</div>';
    }
    var escala = Math.max(total, usado), saldo = Math.max(0, total - usado), exceso = Math.max(0, usado - total);
    var segs = '', i = 0;
    (usos || []).forEach(function (u) {
      var m = Number(u[cat]) || 0; if (!m) return;
      var w = pct(m, escala);
      var et = (u.numero_ot || ('OT ' + u.vid)) + ' · ' + clp(m);
      segs += '<a class="sf-seg u' + (i % 4) + '" href="/ot/' + esc(u.vid) + '" style="flex:0 0 ' + w.toFixed(2) + '%" title="' + esc(et) + '" aria-label="' + esc(et) + '">' +
        (w >= 14 ? esc(u.numero_ot || '') : '') + '</a>';
      i++;
    });
    if (exceso > 0) {
      segs += '<span class="sf-seg exceso" style="flex:0 0 ' + pct(exceso, escala).toFixed(2) + '%" title="Cobrado de más: ' + clp(exceso) + '">' +
        (pct(exceso, escala) >= 14 ? 'Exceso' : '') + '</span>';
    } else if (saldo > 0) {
      segs += '<span class="sf-seg libre" style="flex:1 1 auto" title="Libre: ' + clp(saldo) + '">' + (pct(saldo, escala) >= 12 ? 'Libre' : '') + '</span>';
    }
    var q = exceso > 0 ? '<strong class="neg">' + clp(-exceso) + '</strong> (cobrado de más)'
      : 'Queda <strong class="' + (saldo ? '' : 'cero') + '">' + clp(saldo) + '</strong> de ' + clp(total);
    return '<div class="sf-med"><div class="sf-med-h"><b>' + esc(CAT[cat]) + '</b><span class="q">' + q + '</span></div>' +
      '<div class="sf-barra" role="img" aria-label="' + esc(CAT[cat] + ': monto ' + clp(total) + ', usado ' + clp(usado) + ', queda ' + clp(saldo)) + '">' + segs + '</div>' +
      '<div class="sf-med-pie"><span>Monto <b>' + clp(total) + '</b></span><span>Usado <b>' + clp(usado) + '</b> (' + Math.round(pct(usado, total || 1)) + ' %)</span>' +
      '<span>Queda <b>' + clp(saldo) + '</b></span></div></div>';
  }

  function cuentaOt(f) {
    if (!f) return '<span class="sf-cuenta">Sin datos de la cuenta.</span>';
    if (!f.cobra) return '<span class="sf-cuenta">' + esc(f.cobertura_txt || 'No se cobra') + ' · nos costó <b>' + clp(f.me_cobraron) + '</b></span>';
    var q = Number(f.queda) || 0;
    return '<span class="sf-cuenta">Cobramos <b>' + clp(f.cobre) + '</b> − nos cobraron <b>' + clp(f.me_cobraron) + '</b> = ' +
      (q < 0 ? '<span class="mal">se pierden ' + clp(-q) + '</span>' : '<span class="ok">quedan ' + clp(q) + '</span>') + '</span>';
  }

  function pintarDoc(d) {
    var doc = d.doc || {}, est = d.estado || {}, ver = d.veredicto || {};
    var tipoL = TIPO_LARGO[doc.tido] || doc.tido || 'Documento';
    var datos = [doc.cliente, doc.cliente_rut ? 'RUT ' + doc.cliente_rut : '', doc.fecha ? 'Emitida el ' + doc.fecha : '',
      doc.total_doc ? 'Total del documento ' + clp(doc.total_doc) : ''].filter(Boolean).join(' · ');
    var h = '<article class="sf-doc"><header class="sf-doc-h"><div><span class="sf-eyebrow">' + esc(tipoL) + '</span>' +
      '<h2>' + esc(doc.titulo || '') + '</h2><p>' + esc(datos || 'Sin datos de cabecera en Random') + '</p></div>' +
      '<span class="sf-pill ' + esc(est.estado) + '"><i class="bi ' + (ICONO[est.estado] || 'bi-circle') + '"></i>' + esc(est.txt || '') + '</span></header>' +
      '<div class="sf-doc-b">';
    if (d.aviso) h += '<div class="sf-ver gris"><i class="bi bi-cloud-slash"></i><div><b>Random no respondió</b>' + esc(d.aviso) + '</div></div>';
    /* Veredicto: lo que se puede hacer con esta factura, en una frase. Doble cobro manda sobre todo. */
    if (est.estado === 'excedida') {
      h += '<div class="sf-ver dos"><i class="bi bi-exclamation-octagon-fill"></i><div><b>Doble cobro detectado</b>' +
        'Las OT suman ' + clp(est.exceso) + ' más de lo que trae la factura. Revisa cuál cobra de más: abajo está cada OT con lo que toma.</div></div>';
    } else {
      h += '<div class="sf-ver ' + esc(ver.clase || 'gris') + '"><i class="bi ' + (ver.ok ? (ver.clase === 'gris' ? 'bi-info-circle-fill' : 'bi-check-circle-fill') : 'bi-lock-fill') + '"></i>' +
        '<div><b>' + esc(ver.titulo || '') + '</b>' + esc(ver.texto || '') + '</div></div>';
    }
    h += '<div class="sf-meds">' + medidor('servicio', d.servicio, d.usos) + medidor('despacho', d.despacho, d.usos) + '</div>';
    if ((d.usos || []).length) {
      h += '<div class="sf-leyenda"><span><i style="background:#0a0a0a"></i>Lo que toma cada OT (toca un tramo para abrirla)</span>' +
        '<span><i style="background:#bbf7d0"></i>Saldo libre</span><span><i style="background:#dc2626"></i>Cobrado de más</span></div>';
    }
    /* Quién la usó */
    var usos = d.usos || [];
    h += '<div class="sf-sec"><h4><i class="bi bi-clipboard2-pulse"></i>Qué OT la usan <small>' + (usos.length ? usos.length + ' OT' : 'ninguna todavía') + '</small></h4>';
    if (!usos.length) {
      h += '<div class="sf-muted">Ninguna OT toma plata de esta factura. Todo su saldo está disponible.</div>';
    } else {
      h += '<div class="sf-usos">' + usos.map(function (u) {
        var f = u.fin || {};
        var perdida = f.cobra && Number(f.queda) < 0;
        return '<div class="sf-uso' + (perdida ? ' perdida' : '') + '"><div><a class="ot" href="/ot/' + esc(u.vid) + '">' + esc(u.numero_ot || ('OT ' + u.vid)) + '</a>' +
          '<div class="cli">' + esc(u.cliente || '') + (f.estado_txt ? ' · ' + esc(f.estado_txt) : '') + (f.fecha ? ' · ' + esc(f.fecha) : '') + '</div></div>' +
          '<div class="toma">' + (u.servicio ? '<span class="sf-chip ser">Servicio ' + clp(u.servicio) + '</span>' : '') +
          (u.despacho ? '<span class="sf-chip des">Despacho ' + clp(u.despacho) + '</span>' : '') + '</div>' +
          '<div>' + cuentaOt(f) + (perdida ? ' <span class="sf-pill excedida"><i class="bi bi-shield-exclamation"></i>Pérdida: cierra solo con autorización de Daniel</span>' : '') + '</div>' +
          /* 🧩 2026-10-20: la desinstalación cubierta aparece BAJO la OT de la instalación con $0: el auditor va a preguntar. */
          (u.desinstalaciones || []).map(function (x) {
            return '<div class="sf-desinst"><i class="bi bi-box-arrow-up"></i> <a class="ot" href="' + esc(x.url || ('/ot/' + x.vid)) + '">' + esc(x.numero_ot || ('OT ' + x.vid)) + '</a>' +
              ' · ' + esc(x.relacion || 'Desinstalación cubierta por la instalación') + ' · <b>' + clp(x.servicio || 0) + '</b>' + ((x.despacho || 0) ? ' + despacho ' + clp(x.despacho) : '') +
              (x.estado ? ' · ' + esc(String(x.estado).replace(/_/g, ' ')) : '') + '</div>';
          }).join('') + '</div>';
      }).join('') + '</div>';
    }
    h += '</div>';
    /* Dónde viaja la factura (herencia) */
    var her = d.herencia || {}, fam = d.familia || [];
    var chips = fam.map(function (f) { return '<span class="h"><i class="bi bi-diagram-2"></i>' + esc(f.rol) + ': <b>' + esc(f.titulo) + '</b></span>'; })
      .concat((her.tickets || []).map(function (t) {
        return '<a href="' + esc(t.url) + '"><i class="bi bi-ticket-detailed"></i>Ticket ' + esc(t.numero) + (t.titulo ? ' · ' + esc(t.titulo) : '') + '</a>';
      }))
      .concat((her.cotizaciones || []).map(function (c) { return '<a href="' + esc(c.url) + '"><i class="bi bi-receipt"></i>Cotización ' + esc(c.numero) + '</a>'; }));
    h += '<div class="sf-sec"><h4><i class="bi bi-signpost-split"></i>Dónde viaja esta factura <small>la heredan, no la consumen</small></h4><div class="sf-hereda">' +
      (chips.length ? chips.join('') : '<span class="nada">Ningún ticket ni cotización la trae.</span>') + '</div></div>';
    /* Libro */
    var movs = d.movimientos || [];
    h += '<div class="sf-sec"><h4><i class="bi bi-journal-text"></i>Libro de movimientos <small>no se borran ni se editan</small></h4>';
    if (!movs.length) {
      h += '<div class="sf-muted">Todavía no hay movimientos registrados de esta factura.</div>';
    } else {
      h += '<ol class="sf-libro">' + movs.slice(0, 12).map(movHtml).join('') + '</ol>';
      if (movs.length > 12) h += '<button type="button" class="sf-mas" data-sf-mas>Ver los ' + movs.length + ' movimientos</button>';
    }
    h += '</div></div></article>';
    res.innerHTML = h;
    var mas = res.querySelector('[data-sf-mas]');
    if (mas) mas.addEventListener('click', function () { res.querySelector('.sf-libro').innerHTML = movs.map(movHtml).join(''); mas.remove(); });
  }

  function movHtml(m) {
    var signo = m.monto > 0 ? '+' : '−';
    return '<li class="' + esc(m.movimiento) + '"><div class="l1"><span class="m">' + signo + clp(Math.abs(m.monto)).replace('-', '') + '</span>' +
      '<span>' + (m.movimiento === 'consumo' ? 'Consume' : 'Libera') + ' ' + esc((m.categoria_txt || '').toLowerCase()) + ' de ' + esc(m.documento) + '</span>' +
      '<span class="f">' + esc(m.fecha) + '</span></div>' +
      '<div class="l2"><a href="' + esc(m.url) + '">' + esc(m.numero_ot) + '</a>' + (m.cliente ? ' · ' + esc(m.cliente) : '') +
      ' · la OT queda tomando ' + clp(m.acumulado) + '</div>' +
      '<div class="l3">' + esc(m.origen_txt) + ' · ' + esc(m.usuario) + (m.detalle && m.origen === 'inicial' ? ' · ' + esc(m.detalle) : '') + '</div></li>';
  }

  /* ── Listado ───────────────────────────────────────────────────────────────────────────────────────────── */
  var tabla = document.getElementById('sfTabla'), pie = document.getElementById('sfPie'), chipsEl = document.getElementById('sfChips');
  var qEl = document.getElementById('sfQ'), perEl = document.getElementById('sfPer');
  var kpis = document.getElementById('sfKpis'), rev = document.getElementById('sfRev'), sub = document.getElementById('sfListaSub');

  function mini(s, etq) {
    s = s || {};
    var tot = Number(s.total) || 0, us = Number(s.usado) || 0;
    if (tot <= 0) return '<div class="sf-mini sin"><span class="l">' + etq + '</span><div class="b"></div><span class="t">Sin línea</span></div>';
    var esc_ = Math.max(tot, us), exc = us > tot;
    return '<div class="sf-mini"><span class="l">' + etq + '</span><div class="b"><i style="width:' + pct(Math.min(us, tot), esc_).toFixed(1) + '%"></i>' +
      (exc ? '<i class="x" style="width:' + pct(us - tot, esc_).toFixed(1) + '%"></i>' : '') + '</div>' +
      '<span class="t">' + (exc ? '<b style="color:#dc2626">' + clp(tot - us) + '</b> cobrado de más' : 'Queda <b' + (tot - us <= 0 ? ' class="z"' : '') + '>' + clp(tot - us) + '</b> de ' + clp(tot)) + '</span></div>';
  }

  function cargarLista() {
    var u = '/ot/api/saldo-facturas?page=' + st.page + '&per_page=' + st.per + (st.estado ? '&estado=' + encodeURIComponent(st.estado) : '') +
      (st.q ? '&q=' + encodeURIComponent(st.q) : '');
    tabla.setAttribute('aria-busy', 'true');
    var seq = ++seqLista;
    return api(u).then(function (r) {
      if (seq !== seqLista) return;   // llegó tarde: hay una búsqueda más nueva
      tabla.removeAttribute('aria-busy');
      if (!r.ok) { tabla.innerHTML = '<div class="pp-error">' + esc(r.j.error || 'No se pudo leer el listado.') + '</div>'; pie.innerHTML = ''; return; }
      var j = r.j;
      st.estados = j.estados || st.estados;
      pintarKpis(j); pintarRev(j.revision || {}); pintarChips(j.conteo || {}, j.totales || {});
      sub.textContent = j.total ? (j.total + ' factura' + (j.total === 1 ? '' : 's') + (st.estado || st.q ? ' con este filtro' : '')) : '';
      if (!j.items.length) {
        tabla.innerHTML = '<div class="pp-vacio">' + ((j.totales || {}).facturas ? 'Ninguna factura con este filtro.' :
          'Todavía no hay facturas en el libro. Se cargan solas cada 10 minutos, o con «Revisar todas las OT ahora».') + '</div>';
      } else {
        tabla.innerHTML = '<div class="sf-th"><span>Documento</span><span>Servicio</span><span>Despacho</span><span>OT que la usan</span><span>Estado</span></div>' +
          j.items.map(function (f) {
            var e = f.estado || {};
            return '<div class="sf-fila ' + esc(e.estado) + '"><div class="doc"><b>' + esc(f.titulo) + '</b><span>' + esc(f.cliente || 'Cliente sin leer en Random') +
              (f.fecha ? ' · ' + esc(f.fecha) : '') + '</span></div>' + mini(f.servicio, 'Servicio') + mini(f.despacho, 'Despacho') +
              '<div class="ots">' + (f.n_ot ? esc(f.ots) : '<span class="sf-muted">Ninguna</span>') + '</div>' +
              '<div class="acc"><span class="sf-pill ' + esc(e.estado) + '"><i class="bi ' + (ICONO[e.estado] || 'bi-circle') + '"></i>' + esc(e.txt) + '</span>' +
              '<button type="button" class="sf-ver-btn" data-sf-ver="' + esc(f.tido) + '|' + esc(f.nudo) + '"><i class="bi bi-eye"></i>Ver</button></div></div>';
          }).join('');
      }
      pie.innerHTML = j.total ? '<span class="t">Mostrando ' + j.desde + '–' + j.hasta + ' de ' + j.total + '</span>' +
        '<span class="d-flex gap-2 align-items-center"><button type="button" class="pp-btn" data-pag="-1"' + (j.page <= 1 ? ' disabled' : '') + '>Anterior</button>' +
        '<span class="t">Página ' + j.page + ' de ' + j.pages + '</span>' +
        '<button type="button" class="pp-btn" data-pag="1"' + (j.page >= j.pages ? ' disabled' : '') + '>Siguiente</button></span>' : '';
      st.page = j.page;
    });
  }

  function pintarKpis(j) {
    var t = j.totales || {}, c = j.conteo || {};
    kpis.innerHTML = '<div class="sf-kpi"><small>Facturas controladas</small><b>' + (t.facturas || 0) + '</b></div>' +
      '<div class="sf-kpi"><small>Saldo de servicio y despacho</small><b>' + clp(t.capacidad) + '</b></div>' +
      '<div class="sf-kpi"><small>Usado por las OT</small><b>' + clp(t.usado) + '</b></div>' +
      '<div class="sf-kpi v"><small>Disponible</small><b>' + clp(t.saldo) + '</b></div>' +
      '<div class="sf-kpi' + ((c.excedida || 0) ? ' r' : ' v') + '"><small>Doble cobro</small><b>' + (c.excedida || 0) + '</b></div>';
  }

  function pintarChips(c) {
    var tot = 0; Object.keys(c).forEach(function (k) { tot += c[k]; });
    var html = '<button type="button" class="pp-chip' + (st.estado ? '' : ' on') + '" data-est="">Todas <b>' + tot + '</b></button>';
    ORDEN.forEach(function (k) {
      if (!c[k] && st.estado !== k) return;
      var e = st.estados[k] || {};
      html += '<button type="button" class="pp-chip' + (st.estado === k ? ' on' : '') + '" data-est="' + k + '" title="' + esc(e.frase || '') + '">' +
        '<i class="bi ' + ICONO[k] + '"></i>' + esc(e.txt || k) + ' <b>' + (c[k] || 0) + '</b></button>';
    });
    chipsEl.innerHTML = html;
  }

  var revisando = false;
  function pintarRev(r) {
    if (revisando) return;
    var n = r.ots_revisadas || 0;
    rev.className = 'sf-rev' + (n ? '' : ' pendiente');
    rev.innerHTML = '<div class="t"><i class="bi ' + (n ? 'bi-shield-check' : 'bi-hourglass-split') + '"></i><div>' +
      (n ? '<b>El sistema vigila ' + n + ' OT con documento.</b> Última revisión: ' + esc(r.ultima || '—') + '. Se revisa solo cada 10 minutos y después de cada cambio en una OT.'
        : '<b>El libro todavía no tiene la carga inicial.</b> Se carga sola cada 10 minutos; para tenerla ahora, revisa todas las OT.') + '</div></div>' +
      '<button type="button" class="pp-btn' + (n ? '' : ' pri') + '" data-sf-revisar><i class="bi bi-arrow-repeat"></i> Revisar todas las OT ahora</button>';
  }

  function revisarTodo() {
    if (revisando) return;
    revisando = true;
    var desde = parseInt(lsGet(LS_REV) || '0', 10) || 0;
    rev.className = 'sf-rev pendiente';
    function pintarProg(h, t) {
      rev.innerHTML = '<div class="t"><i class="bi bi-arrow-repeat"></i><div><b>Revisando las OT…</b> ' + h + ' de ' + t +
        '. Puedes seguir usando la página; si la cierras, sigue donde quedó.</div></div><div class="sf-prog"><span style="width:' + pct(h, t || 1).toFixed(1) + '%"></span></div>';
    }
    pintarProg(0, 0);
    (function paso() {
      api('/ot/api/saldo-facturas/revisar', { method: 'POST', body: { desde: desde } }).then(function (r) {
        if (!r.ok) {
          revisando = false;
          toast(r.j.error || 'La revisión se detuvo. Vuelve a intentarlo: sigue donde quedó.', 'error');
          cargarLista();
          return;
        }
        desde = r.j.siguiente; lsSet(LS_REV, String(desde));
        pintarProg(r.j.hechas, r.j.total);
        if (r.j.fin) {
          revisando = false; lsSet(LS_REV, null);
          toast('Listo: el libro quedó al día con todas las OT.', 'success');
          cargarLista();
          return;
        }
        paso();
      });
    })();
  }

  /* ── Eventos ───────────────────────────────────────────────────────────────────────────────────────────── */
  app.addEventListener('click', function (e) {
    var b = e.target.closest('[data-est]');
    if (b && chipsEl.contains(b)) { st.estado = b.getAttribute('data-est'); st.page = 1; cargarLista(); return; }
    var p = e.target.closest('[data-pag]');
    if (p && !p.disabled) { st.page += parseInt(p.getAttribute('data-pag'), 10); cargarLista(); tabla.scrollIntoView({ block: 'start' }); return; }
    var v = e.target.closest('[data-sf-ver]');
    if (v) {
      var par = v.getAttribute('data-sf-ver').split('|');
      fTipo.value = fTipo.querySelector('option[value="' + par[0] + '"]') ? par[0] : fTipo.value;
      fNum.value = numDoc(par[1]);
      consultar(par[0], par[1], true);
      return;
    }
    if (e.target.closest('[data-sf-revisar]')) revisarTodo();
  });
  var tq = null;
  qEl.addEventListener('input', function () {
    clearTimeout(tq);
    /* REGLA #4.3: al limpiar el filtro, la tabla se recarga. */
    tq = setTimeout(function () { st.q = qEl.value.trim(); st.page = 1; cargarLista(); }, 350);
  });
  perEl.addEventListener('change', function () { st.per = parseInt(perEl.value, 10) || 50; st.page = 1; lsSet('sf_per', String(st.per)); cargarLista(); });

  var perGuardado = parseInt(lsGet('sf_per') || '0', 10);
  if (perGuardado && [25, 50, 100, 200].indexOf(perGuardado) >= 0) { st.per = perGuardado; perEl.value = String(perGuardado); }
  cargarLista().then(function () { if (lsGet(LS_REV)) revisarTodo(); });
  try {
    var dq = new URL(location.href).searchParams.get('doc');
    var mm = dq && dq.match(/^([A-Z]{2,4})-(.+)$/i);
    if (mm) { var t = mm[1].toUpperCase(); if (fTipo.querySelector('option[value="' + t + '"]')) fTipo.value = t; fNum.value = numDoc(mm[2]); consultar(t, mm[2], false); }
  } catch (e) { }
})();

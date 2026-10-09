/* Tabla de verdad de "Facturas registradas" (Facturas de proveedor, Servicio Técnico).
   Daniel 2026-10-08: "los lotes ordenados por número... tabla de verdad, como la de Retiros, que se
   pueda FILTRAR LAS COLUMNAS". Las filas llegan renderizadas por el servidor (ya en orden de lote
   descendente) y acá se filtra, se ordena y se pagina en el navegador (REGLA #4.3).

   Las funciones de arriba son PURAS (sin DOM): las prueba tests/test_facturas_proveedor_tabla.py con
   node. El bloque de abajo solo las conecta con la página. Si cambias el criterio de orden o de
   filtro, cambia también esa prueba. */
(function (raiz) {
  'use strict';

  var ESTADO_RANGO = { pendiente: 0, pagada: 1, anulada: 2 };
  var ESTADO_TXT = { pendiente: 'pendiente de pago', pagada: 'pagada', anulada: 'anulada' };

  // minúsculas y sin tildes, para que "ópera" y "OPERA" calcen
  function norm(s) {
    var t = String(s == null ? '' : s).toLowerCase();
    try { t = t.normalize('NFD').replace(/[̀-ͯ]/g, ''); } catch (e) { /* navegador viejo */ }
    return t.replace(/\s+/g, ' ').trim();
  }
  // sin puntos, guiones ni espacios: "76.123.456-7" calza con "761234567"
  function compacto(s) { return norm(s).replace(/[.\-\s#]/g, ''); }

  // "1.085.000" / "1085000" / "1085000,5" -> número; vacío o basura -> null
  function numero(v) {
    if (v === '' || v == null) return null;
    var n = Number(String(v).replace(/[\s$]/g, '').replace(/\./g, '').replace(',', '.'));
    return isFinite(n) ? n : null;
  }

  function clp(n) {
    var x = Math.round(Number(n) || 0);
    var neg = x < 0;
    var t = String(Math.abs(x)).replace(/\B(?=(\d{3})+(?!\d))/g, '.');
    return (neg ? '-$' : '$') + t;
  }

  // Cada fila: {id, nombre, prov, oc, fact, fecha, ots, nOt, monto, estado, etapa}
  function prepararFila(d) {
    var f = {
      id: Number(d.id) || 0,
      nombre: String(d.nombre || ''),
      prov: String(d.prov || ''),
      oc: String(d.oc || ''),
      fact: String(d.fact || ''),
      fecha: String(d.fecha || ''),
      fechaLbl: String(d.fechaLbl || ''),
      ots: String(d.ots || ''),
      nOt: Number(d.nOt) || 0,
      monto: Number(d.monto) || 0,
      estado: String(d.estado || 'pendiente'),
      etapa: Number(d.etapa) || 0
    };
    f._prov = norm(f.prov); f._provC = compacto(f.prov);
    f._oc = norm(f.oc); f._ocC = compacto(f.oc);
    f._fact = norm(f.fact); f._factC = compacto(f.fact);
    f._ots = norm(f.ots); f._otsC = compacto(f.ots);
    f._q = norm([f.id, f.prov, f.oc, f.fact, f.fechaLbl, f.ots, ESTADO_TXT[f.estado] || f.estado].join(' '));
    f._qC = compacto([f.id, f.prov, f.oc, f.fact, f.fechaLbl, f.ots].join(' '));
    return f;
  }

  function calza(texto, textoC, buscado) {
    var b = norm(buscado);
    if (!b) return true;
    if (texto.indexOf(b) !== -1) return true;
    var bc = compacto(buscado);
    return !!bc && textoC.indexOf(bc) !== -1;
  }

  // "15", "#15", "15, 17" -> el N° de lote EMPIEZA con alguno de esos números ("1" muestra el 1 y del 10 al 19)
  function calzaLote(id, buscado) {
    var toks = String(buscado || '').split(/[\s,;]+/).map(function (t) { return t.replace(/^#/, ''); })
      .filter(Boolean);
    if (!toks.length) return true;
    var s = String(id);
    return toks.some(function (t) { return s.indexOf(t) === 0; });
  }

  function etapaCalza(f, v) {
    switch (v) {
      case 'e0': return f.etapa === 0;
      case 'e1': return f.etapa === 1;
      case 'e2': return f.etapa === 2;
      case 'e3': return f.etapa === 3;
      case 'e4': return f.etapa === 4;
      case 'f_oc': return f.estado !== 'anulada' && !f.oc;
      case 'f_fact': return f.estado !== 'anulada' && !f.fact;
      case 'f_pago': return f.estado === 'pendiente';
      default: return true;
    }
  }

  function filtrar(filas, fl) {
    fl = fl || {};
    var mMin = numero(fl.montoMin), mMax = numero(fl.montoMax);
    var desde = fl.desde || '', hasta = fl.hasta || '';
    return filas.filter(function (f) {
      if (fl.q && !calza(f._q, f._qC, fl.q)) return false;
      if (fl.lote && !calzaLote(f.id, fl.lote)) return false;
      if (fl.prov && !calza(f._prov, f._provC, fl.prov)) return false;
      if (fl.oc && !calza(f._oc, f._ocC, fl.oc)) return false;
      if (fl.fact && !calza(f._fact, f._factC, fl.fact)) return false;
      if (fl.ot && !calza(f._ots, f._otsC, fl.ot)) return false;
      if (fl.estado && f.estado !== fl.estado) return false;
      if (fl.etapa && !etapaCalza(f, fl.etapa)) return false;
      if (mMin !== null && f.monto < mMin) return false;
      if (mMax !== null && f.monto > mMax) return false;
      if (desde && (!f.fecha || f.fecha < desde)) return false;
      if (hasta && (!f.fecha || f.fecha > hasta)) return false;
      return true;
    });
  }

  function hayFiltros(fl) {
    fl = fl || {};
    return ['q', 'lote', 'prov', 'oc', 'fact', 'ot', 'estado', 'etapa', 'montoMin', 'montoMax', 'desde', 'hasta']
      .some(function (k) { return String(fl[k] == null ? '' : fl[k]).trim() !== ''; });
  }

  // Los vacíos (sin OC, sin factura, sin fecha) van siempre al final, suba o baje el orden.
  function cmpTexto(a, b, dir) {
    if (!a && !b) return 0;
    if (!a) return 1;
    if (!b) return -1;
    var r = a < b ? -1 : (a > b ? 1 : 0);
    return dir === 'asc' ? r : -r;
  }

  function ordenar(filas, campo, dir) {
    dir = dir === 'asc' ? 'asc' : 'desc';
    var signo = dir === 'asc' ? 1 : -1;
    var copia = filas.slice();
    copia.sort(function (a, b) {
      var r = 0;
      switch (campo) {
        case 'lote': r = (a.id - b.id) * signo; break;
        case 'prov': r = cmpTexto(norm(a.nombre), norm(b.nombre), dir); break;
        case 'tracking': r = (a.etapa - b.etapa) * signo; break;
        case 'oc': r = cmpTexto(a._oc, b._oc, dir); break;
        case 'fact': r = cmpTexto(a._fact, b._fact, dir); break;
        case 'fecha': r = cmpTexto(a.fecha, b.fecha, dir); break;
        case 'ot': r = (a.nOt - b.nOt) * signo; break;
        case 'monto': r = (a.monto - b.monto) * signo; break;
        case 'estado': r = ((ESTADO_RANGO[a.estado] || 0) - (ESTADO_RANGO[b.estado] || 0)) * signo; break;
        default: r = (a.id - b.id) * signo;
      }
      // empate: siempre el lote más nuevo primero
      return r !== 0 ? r : (b.id - a.id);
    });
    return copia;
  }

  function paginar(total, pagina, porPagina) {
    porPagina = Math.max(1, Number(porPagina) || 25);
    var paginas = Math.max(1, Math.ceil(total / porPagina));
    pagina = Math.min(Math.max(1, Number(pagina) || 1), paginas);
    var ini = (pagina - 1) * porPagina;
    var fin = Math.min(ini + porPagina, total);
    return { pagina: pagina, paginas: paginas, porPagina: porPagina, ini: ini, fin: fin,
             desde: total ? ini + 1 : 0, hasta: fin };
  }

  // Mismos totales que la cabecera de la sección en el servidor: las anuladas no suman, salvo que se
  // esté mirando justo las anuladas.
  function totales(filas, incluirAnuladas) {
    var t = { n: 0, monto: 0, nPend: 0, montoPend: 0, nPag: 0, montoPag: 0 };
    filas.forEach(function (f) {
      if (f.estado === 'anulada' && !incluirAnuladas) return;
      t.n++; t.monto += f.monto;
      if (f.estado === 'pendiente') { t.nPend++; t.montoPend += f.monto; }
      if (f.estado === 'pagada') { t.nPag++; t.montoPag += f.monto; }
    });
    return t;
  }

  var API = { norm: norm, compacto: compacto, numero: numero, clp: clp, prepararFila: prepararFila,
              filtrar: filtrar, hayFiltros: hayFiltros, ordenar: ordenar, paginar: paginar, totales: totales };
  if (typeof module !== 'undefined' && module.exports) { module.exports = API; }
  raiz.FPT = API;

  // ───────────────────────── Conexión con la página ─────────────────────────
  if (typeof document === 'undefined') return;

  function iniciar() {
    var tabla = document.getElementById('fpvTabla');
    if (!tabla) return;
    var tbody = tabla.tBodies[0];
    var cfg = tabla.dataset;
    var trs = Array.prototype.slice.call(tbody.querySelectorAll('tr.rm-row'));
    var filas = trs.map(function (tr) {
      var d = tr.dataset;
      var f = prepararFila({ id: d.id, nombre: d.nombre, prov: d.prov, oc: d.oc, fact: d.fact, fecha: d.fecha,
                             fechaLbl: d.fechaLbl, ots: d.ots, nOt: d.not, monto: d.monto, estado: d.estado,
                             etapa: d.etapa });
      f.el = tr;
      return f;
    });
    var vacio = document.getElementById('fpvVacio');
    var estadoServidor = cfg.estadoServidor || '';

    var st = { orden: 'lote', dir: 'desc', pagina: 1, porPagina: Number(cfg.perPage) || 25 };
    var campos = {};
    Array.prototype.forEach.call(document.querySelectorAll('[data-fpt-f]'), function (el) {
      campos[el.getAttribute('data-fpt-f')] = el;
    });

    function leerFiltros() {
      var fl = {};
      Object.keys(campos).forEach(function (k) { fl[k] = campos[k].value; });
      return fl;
    }

    function $(id) { return document.getElementById(id); }

    function pintarTotales(todas, filtradas, activo) {
      var el = $('fpvTot');
      if (!el) return;
      var t = totales(filtradas, !!estadoServidor || (campos.estado && campos.estado.value === 'anulada'));
      var h = '';
      if (t.n) {
        h = clp(t.monto) + ' en ' + t.n + ' factura' + (t.n === 1 ? '' : 's');
        if (t.nPend) h += ' · <b class="pend">por pagar ' + clp(t.montoPend) + ' (' + t.nPend + ')</b>';
        if (t.nPag) h += ' · <b class="ok">pagadas ' + clp(t.montoPag) + ' (' + t.nPag + ')</b>';
      } else {
        h = 'Sin facturas con esos filtros';
      }
      if (activo) {
        var g = totales(todas, !!estadoServidor);
        h += ' <span class="fpv-tot-gen">· total general ' + clp(g.monto) + ' en ' + g.n + '</span>';
      }
      el.innerHTML = h;
      var sn = $('fpvSecN');
      if (sn) sn.textContent = String(filtradas.length);
    }

    function aplicar() {
      var fl = leerFiltros();
      var activo = hayFiltros(fl);
      var filtradas = ordenar(filtrar(filas, fl), st.orden, st.dir);
      var pg = paginar(filtradas.length, st.pagina, st.porPagina);
      st.pagina = pg.pagina;
      var visibles = {};
      filtradas.slice(pg.ini, pg.fin).forEach(function (f) { visibles[f.id] = true; });
      // se reordena el DOM según el orden actual; los que no están en la página quedan ocultos
      var frag = document.createDocumentFragment();
      filtradas.forEach(function (f) { f.el.hidden = !visibles[f.id]; frag.appendChild(f.el); });
      filas.forEach(function (f) { if (filtradas.indexOf(f) === -1) { f.el.hidden = true; frag.appendChild(f.el); } });
      if (vacio) frag.appendChild(vacio);
      tbody.appendChild(frag);
      if (vacio) {
        vacio.hidden = filtradas.length > 0;
        var vm = $('fpvVacioMsg');
        if (vm) vm.textContent = activo ? 'Ninguna factura coincide con esos filtros.' : 'No hay facturas.';
      }

      var cnt = $('fpvCount');
      if (cnt) cnt.textContent = filtradas.length + ' de ' + filas.length + ' factura' + (filas.length === 1 ? '' : 's');
      var info = $('fpvPageInfo');
      if (info) info.textContent = 'Página ' + pg.pagina + ' de ' + pg.paginas;
      var rango = $('fpvRango');
      if (rango) rango.innerHTML = 'Mostrando <strong>' + pg.desde + '–' + pg.hasta + '</strong> de <strong>' +
        filtradas.length + '</strong>' + (activo ? ' (filtradas de ' + filas.length + ')' : '');
      var prev = $('fpvPrev'), next = $('fpvNext');
      if (prev) prev.disabled = pg.pagina <= 1;
      if (next) next.disabled = pg.pagina >= pg.paginas;
      var lim = $('fpvLimpiar');
      if (lim) lim.disabled = !activo;
      var tq = $('fpvTq');
      var x = $('fpvTqX');
      if (x && tq) x.hidden = !tq.value;

      Array.prototype.forEach.call(tabla.querySelectorAll('th[data-sort]'), function (th) {
        var on = th.getAttribute('data-sort') === st.orden;
        th.classList.toggle('is-asc', on && st.dir === 'asc');
        th.classList.toggle('is-desc', on && st.dir === 'desc');
        th.setAttribute('aria-sort', on ? (st.dir === 'asc' ? 'ascending' : 'descending') : 'none');
        var ic = th.querySelector('.bi');
        if (ic) ic.className = 'bi ' + (on ? (st.dir === 'asc' ? 'bi-sort-up' : 'bi-sort-down') : 'bi-arrow-down-up');
      });
      var om = $('fpvOrdMov'), od = $('fpvOrdDir');
      if (om) om.value = st.orden;
      if (od) {
        od.setAttribute('aria-label', st.dir === 'asc' ? 'Orden ascendente' : 'Orden descendente');
        od.innerHTML = '<i class="bi bi-sort-' + (st.dir === 'asc' ? 'up' : 'down') + '"></i>';
      }
      var fb = $('fpvFiltrosBtn');
      if (fb) fb.classList.toggle('on', activo);
      pintarTotales(filas, filtradas, activo);
    }

    // escribir en un filtro vuelve a la página 1 y recalcula, incluso al dejarlo vacío (REGLA #4.3)
    var t = null;
    function cambioFiltro() {
      st.pagina = 1;
      clearTimeout(t);
      t = setTimeout(aplicar, 90);
    }
    Object.keys(campos).forEach(function (k) {
      var el = campos[k];
      el.addEventListener('input', cambioFiltro);
      el.addEventListener('change', function () { st.pagina = 1; aplicar(); });
    });

    Array.prototype.forEach.call(tabla.querySelectorAll('th[data-sort]'), function (th) {
      function alternar() {
        var k = th.getAttribute('data-sort');
        if (st.orden === k) { st.dir = st.dir === 'asc' ? 'desc' : 'asc'; }
        else { st.orden = k; st.dir = (k === 'prov' || k === 'oc' || k === 'fact' || k === 'estado') ? 'asc' : 'desc'; }
        st.pagina = 1;
        aplicar();
      }
      th.addEventListener('click', alternar);
      th.addEventListener('keydown', function (ev) {
        if (ev.key === 'Enter' || ev.key === ' ') { ev.preventDefault(); alternar(); }
      });
    });
    var om = $('fpvOrdMov'), od = $('fpvOrdDir');
    if (om) om.addEventListener('change', function () { st.orden = om.value; st.pagina = 1; aplicar(); });
    if (od) od.addEventListener('click', function () { st.dir = st.dir === 'asc' ? 'desc' : 'asc'; st.pagina = 1; aplicar(); });

    function limpiar() {
      Object.keys(campos).forEach(function (k) { campos[k].value = ''; });
      st.pagina = 1;
      st.orden = 'lote'; st.dir = 'desc';
      aplicar();
    }
    var bl = $('fpvLimpiar');
    if (bl) bl.addEventListener('click', limpiar);
    var tqx = $('fpvTqX');
    if (tqx) tqx.addEventListener('click', function () { var q = $('fpvTq'); if (q) { q.value = ''; q.focus(); } st.pagina = 1; aplicar(); });

    var sel = $('fpvSize');
    if (sel) {
      sel.value = String(st.porPagina);
      sel.addEventListener('change', function () {
        st.porPagina = Number(sel.value) || 25; st.pagina = 1;
        try {
          var u = new URL(window.location.href);
          u.searchParams.set('per_page', String(st.porPagina));
          window.history.replaceState(null, '', u.toString());
        } catch (e) { /* sin historial: no pasa nada */ }
        aplicar();
      });
    }
    var prev = $('fpvPrev'), next = $('fpvNext');
    if (prev) prev.addEventListener('click', function () { st.pagina--; aplicar(); tabla.scrollIntoView({ block: 'nearest' }); });
    if (next) next.addEventListener('click', function () { st.pagina++; aplicar(); tabla.scrollIntoView({ block: 'nearest' }); });

    // celular: los filtros por columna se abren/cierran con un botón
    var fb = $('fpvFiltrosBtn');
    if (fb) fb.addEventListener('click', function () {
      var abierto = tabla.classList.toggle('fpv-fl-open');
      fb.setAttribute('aria-expanded', abierto ? 'true' : 'false');
    });

    // toda la fila abre el detalle (como antes); los enlaces y botones de adentro hacen lo suyo
    tbody.addEventListener('click', function (ev) {
      var tr = ev.target.closest && ev.target.closest('tr.rm-row');
      if (!tr || ev.target.closest('a,button,input,select,label')) return;
      if (tr.dataset.href) window.location.href = tr.dataset.href;
    });
    tbody.addEventListener('keydown', function (ev) {
      var tr = ev.target.closest && ev.target.closest('tr.rm-row');
      if (tr && ev.target === tr && ev.key === 'Enter' && tr.dataset.href) window.location.href = tr.dataset.href;
    });

    aplicar();
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', iniciar);
  else iniciar();
})(typeof window !== 'undefined' ? window : (typeof globalThis !== 'undefined' ? globalThis : this));

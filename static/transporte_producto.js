// ═══════════════════════════════════════════════════════════════════════
// Transporte · Trazabilidad por producto (Fase 1 — MVP, 2026-07-29)
// Daniel: "trazabilidad épica de producto" — filtrar por SKU y ver cuántas
// veces se ha despachado, con documentos/clientes/cantidades/estado.
//
// Simplificación deliberada de Fase 1: cada fila de documento es un LINK
// que navega a /transporte/manifiestos/<id>#row-<item_id> (la ficha del
// manifiesto donde ya vive la trazabilidad rica por courier). No se abre
// ningún modal de seguimiento genérico desde acá — eso queda para una
// fase futura (evita duplicar la lógica de los 3 couriers).
// ═══════════════════════════════════════════════════════════════════════

(function () {
  'use strict';

  var _searchTimer = null;
  var _results = [];
  var _activeIdx = -1;
  var _skuActual = null;

  function $(id) { return document.getElementById(id); }

  function escHtml(s) {
    return String(s == null ? '' : s)
      .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
  }

  // ── Chip de cumplimiento pedido/despachado/saldo — MISMA lógica que
  // _ilusQtyFillChips() en transporte_manifiesto_detalle.js (replicada acá
  // a propósito: son archivos JS distintos cargados en páginas distintas,
  // no hay forma limpia de compartir función sin un módulo nuevo — ver
  // nota al equipo en el resumen de la tarea). Misma clase CSS .sr-qty-chip
  // (viene de transporte_manifiesto_detalle.css, enlazado en producto.html).
  function qtyFillChips(cantidad, despachada, saldo) {
    var qty = Number(cantidad) || 0;
    var desp = Number(despachada) || 0;
    var sal = (saldo === null || saldo === undefined) ? Math.max(qty - desp, 0) : Number(saldo);
    var cls = 'pend', label = 'Pendiente';
    if (qty > 0 && desp >= qty) { cls = 'ok'; label = 'Completo'; }
    else if (desp > 0) { cls = 'parcial'; label = 'Parcial'; }
    var html = '<span class="sr-qty-chip qty-fill ' + cls + '" title="Despachado ' + desp + ' de ' + qty + ' solicitados">'
      + desp + '/' + qty + ' <span class="qf-label">' + label + '</span></span>';
    if (sal > 0) {
      html += '<span class="sr-qty-chip saldo" title="Saldo pendiente en el ERP">saldo ' + sal + '</span>';
    }
    return html;
  }

  function entregaBadge(estado) {
    var cls = '', ico = 'bi-clipboard-check';
    if (!estado) {
      return '<span class="pr-entrega"><i class="bi bi-dash-circle"></i>Sin manifiesto</span>';
    }
    if (estado === 'Entregado') { cls = 'ok'; ico = 'bi-check-circle-fill'; }
    else if (estado === 'En ruta' || estado === 'Entregado a transporte') { cls = 'ruta'; ico = 'bi-truck'; }
    else if (estado === 'Problema' || estado === 'Entrega fallida' || estado === 'Devolución') { cls = 'bad'; ico = 'bi-x-octagon-fill'; }
    return '<span class="pr-entrega ' + cls + '"><i class="bi ' + ico + '"></i>' + escHtml(estado) + '</span>';
  }

  function fmtNum(v) {
    var n = Number(v) || 0;
    // Sin decimales si es entero (cantidades suelen serlo), 1 decimal si no.
    return (Math.round(n) === n) ? String(Math.round(n)) : n.toFixed(1);
  }

  // ═══════════════════════════════════════════════════════════════════
  // Typeahead de SKU (mismo patrón que el buscador ERP de "Agregar equipo
  // manual" en mant_ficha.js: debounce 300ms + dropdown + teclado).
  // ═══════════════════════════════════════════════════════════════════
  function onSearchInput() {
    clearTimeout(_searchTimer);
    var q = $('prSearchInput').value.trim();
    var drop = $('prDrop');
    if (q.length < 2) { drop.style.display = 'none'; return; }
    drop.innerHTML = '<div class="pr-drop-msg"><span class="spinner-border spinner-border-sm me-1"></span>Buscando…</div>';
    drop.style.display = 'block';
    _searchTimer = setTimeout(function () { fetchSuggestions(q); }, 300);
  }

  function fetchSuggestions(q) {
    fetch('/transporte/api/productos/buscar?q=' + encodeURIComponent(q))
      .then(function (r) { return r.json(); })
      .then(function (data) {
        _results = Array.isArray(data) ? data : [];
        _activeIdx = -1;
        renderSuggestions();
      })
      .catch(function () {
        $('prDrop').innerHTML = '<div class="pr-drop-msg">Error al buscar</div>';
      });
  }

  function renderSuggestions() {
    var drop = $('prDrop');
    if (!_results.length) {
      drop.innerHTML = '<div class="pr-drop-msg">Sin resultados para ese texto</div>';
      return;
    }
    drop.innerHTML = _results.map(function (p, i) {
      return '<div class="pr-item' + (i === _activeIdx ? ' active' : '') + '" data-idx="' + i + '">'
        + '<span class="pr-item-sku">' + escHtml(p.sku) + '</span>'
        + '<span class="pr-item-nombre">' + escHtml(p.nombre || '—') + '</span>'
        + '</div>';
    }).join('');
    drop.style.display = 'block';
    Array.prototype.forEach.call(drop.querySelectorAll('.pr-item'), function (el) {
      el.addEventListener('click', function () {
        var idx = parseInt(el.getAttribute('data-idx'), 10);
        seleccionarSugerencia(idx);
      });
    });
  }

  function seleccionarSugerencia(i) {
    var p = _results[i];
    if (!p) return;
    $('prSearchInput').value = p.sku;
    $('prDrop').style.display = 'none';
    buscarTrazabilidad(p.sku);
  }

  function onSearchKeydown(e) {
    var drop = $('prDrop');
    var items = drop.querySelectorAll('.pr-item');
    if (e.key === 'ArrowDown') {
      if (items.length) { _activeIdx = Math.min(_activeIdx + 1, items.length - 1); renderSuggestions(); }
      e.preventDefault();
    } else if (e.key === 'ArrowUp') {
      if (items.length) { _activeIdx = Math.max(_activeIdx - 1, -1); renderSuggestions(); }
      e.preventDefault();
    } else if (e.key === 'Enter') {
      e.preventDefault();
      if (_activeIdx >= 0 && _results[_activeIdx]) {
        seleccionarSugerencia(_activeIdx);
      } else {
        var q = $('prSearchInput').value.trim();
        if (q) buscarTrazabilidad(q);
      }
    } else if (e.key === 'Escape') {
      drop.style.display = 'none';
    }
  }

  document.addEventListener('click', function (e) {
    var wrap = document.querySelector('.pr-search-wrap');
    if (wrap && !wrap.contains(e.target)) {
      var dd = $('prDrop');
      if (dd) dd.style.display = 'none';
    }
  });

  // ═══════════════════════════════════════════════════════════════════
  // Trazabilidad: fetch + render de KPIs, stock ERP y tabla de documentos
  // ═══════════════════════════════════════════════════════════════════
  function buscarTrazabilidad(sku) {
    sku = (sku || '').trim().toUpperCase();
    if (!sku) return;
    _skuActual = sku;
    $('prBtnFiltrar').disabled = false;

    // Deep-link: refrescar la URL sin recargar la página, para que se
    // pueda compartir/recargar directo sobre este SKU.
    try {
      var url = new URL(window.location.href);
      url.searchParams.set('sku', sku);
      window.history.replaceState({}, '', url.toString());
    } catch (e) { /* no-op en navegadores raros */ }

    var cont = $('prResultado');
    cont.innerHTML = '<section class="trx-card"><div class="trx-card-body">'
      + '<div class="pr-loading"><span class="spinner-border me-2"></span>Cargando trazabilidad de ' + escHtml(sku) + '…</div>'
      + '</div></section>';

    var params = new URLSearchParams();
    var desde = $('prDesde').value, hasta = $('prHasta').value;
    if (desde) params.set('desde', desde);
    if (hasta) params.set('hasta', hasta);
    var qs = params.toString();

    fetch('/transporte/api/producto/' + encodeURIComponent(sku) + '/trazabilidad' + (qs ? '?' + qs : ''))
      .then(function (r) { return r.json().then(function (data) { return { ok: r.ok, data: data }; }); })
      .then(function (res) {
        if (!res.ok || !res.data.ok) {
          renderError(res.data && res.data.error ? res.data.error : 'No se pudo cargar la trazabilidad.');
          return;
        }
        renderResultado(res.data);
      })
      .catch(function () {
        renderError('Error de red al consultar la trazabilidad.');
      });
  }

  function renderError(msg) {
    $('prResultado').innerHTML = '<section class="trx-card"><div class="trx-card-body">'
      + '<div class="trx-empty"><div class="trx-empty-ico"><i class="bi bi-exclamation-triangle"></i></div>'
      + '<div class="trx-empty-t">No se pudo cargar</div>'
      + '<div class="trx-empty-d">' + escHtml(msg) + '</div></div>'
      + '</div></section>';
    if (window.ilusToast) ilusToast(msg, { type: 'error' });
  }

  function renderResultado(data) {
    var r = data.resumen || {};
    var stock = data.stock_erp;
    var docs = data.documentos || [];

    var stockHtml = '';
    if (stock) {
      stockHtml = '<div class="pr-stock-row">'
        + '<div class="pr-stock-chip"><span class="pr-stock-k">Físico ERP</span><span class="pr-stock-v">' + fmtNum(stock.fisico) + '</span></div>'
        + '<div class="pr-stock-chip"><span class="pr-stock-k">Comprometido</span><span class="pr-stock-v">' + fmtNum(stock.comprometido) + '</span></div>'
        + '<div class="pr-stock-chip ' + (stock.disponible > 0 ? 'ok' : 'bad') + '"><span class="pr-stock-k">Disponible</span><span class="pr-stock-v">' + fmtNum(stock.disponible) + '</span></div>'
        + '<div class="pr-stock-chip"><span class="pr-stock-k">En camino</span><span class="pr-stock-v">' + fmtNum(stock.devengado) + '</span></div>'
        + '</div>';
    } else {
      stockHtml = '<div class="pr-rango mb-3"><i class="bi bi-info-circle me-1"></i>Sin datos de stock ERP para este SKU.</div>';
    }

    var rangoTxt = (r.primera_vez && r.ultima_vez)
      ? ('Desde ' + r.primera_vez + ' hasta ' + r.ultima_vez)
      : 'Sin despachos registrados en el rango';

    var filasHtml = '';
    if (docs.length) {
      filasHtml = docs.map(function (d) {
        var docCell;
        if (d.manifest_id) {
          docCell = '<a class="pr-doc-link" href="/transporte/manifiestos/' + d.manifest_id + '#row-' + d.item_id + '">'
            + escHtml(d.commitment_id ? ('Doc #' + d.commitment_id) : '—') + '</a>';
        } else {
          docCell = '<span class="pr-doc-sin" title="Aún no asignado a un manifiesto">Doc #' + escHtml(d.commitment_id) + ' · sin manifiesto</span>';
        }
        var courierCell = d.courier
          ? (escHtml(d.courier) + (d.correlativo ? ' <span class="text-muted small">(' + escHtml(d.correlativo) + ')</span>' : ''))
          : '<span class="text-muted">—</span>';
        return '<tr>'
          + '<td>' + docCell + '</td>'
          + '<td>' + escHtml(d.fecha_emision || '—') + '</td>'
          + '<td>' + escHtml(d.cliente_nombre || '—') + '</td>'
          + '<td>' + escHtml(d.comuna || '—') + '</td>'
          + '<td>' + qtyFillChips(d.cantidad, d.cant_despachada, d.saldo) + '</td>'
          + '<td>' + entregaBadge(d.estado_entrega) + '</td>'
          + '<td>' + courierCell + '</td>'
          + '</tr>';
      }).join('');
    }

    var tablaHtml = docs.length
      ? ('<div class="trx-table-wrap"><div style="overflow-x:auto"><table class="trx-table"><thead><tr>'
        + '<th>Documento</th><th>Fecha</th><th>Cliente</th><th>Comuna</th>'
        + '<th>Pedido/Despachado</th><th>Entrega</th><th>Courier</th>'
        + '</tr></thead><tbody>' + filasHtml + '</tbody></table></div></div>'
        + (docs.length >= 50 ? '<div class="pr-rango mt-2"><i class="bi bi-info-circle me-1"></i>Se muestran los 50 despachos más recientes del rango — ajusta las fechas para ver el resto.</div>' : ''))
      : ('<div class="trx-empty"><div class="trx-empty-ico"><i class="bi bi-inbox"></i></div>'
        + '<div class="trx-empty-t">Sin despachos para este SKU en el rango</div>'
        + '<div class="trx-empty-d">Prueba ampliando el rango de fechas, o verifica que el SKU sea el correcto.</div></div>');

    $('prResultado').innerHTML =
      '<section class="trx-card">'
      + '<div class="trx-card-body">'
      + '<div class="pr-sku-head">'
      + '<span class="pr-sku-badge">' + escHtml(data.sku) + '</span>'
      + '</div>'
      + '<div class="pr-rango mb-3">' + escHtml(rangoTxt) + '</div>'
      + stockHtml
      + '<div class="trx-kpis">'
      + '<div class="trx-kpi"><div class="trx-kpi-k"><i class="bi bi-file-earmark-text"></i>Documentos</div><div class="trx-kpi-v">' + (r.n_documentos || 0) + '</div><div class="trx-kpi-d">Veces despachado</div></div>'
      + '<div class="trx-kpi"><div class="trx-kpi-k"><i class="bi bi-people"></i>Clientes</div><div class="trx-kpi-v">' + (r.n_clientes || 0) + '</div><div class="trx-kpi-d">Clientes distintos</div></div>'
      + '<div class="trx-kpi trx-kpi-ok"><div class="trx-kpi-k"><i class="bi bi-check-circle-fill"></i>Entregados</div><div class="trx-kpi-v">' + (r.n_entregados || 0) + '</div><div class="trx-kpi-d">Ítems entregados</div></div>'
      + '<div class="trx-kpi' + ((r.n_con_preventa || 0) > 0 ? ' trx-kpi-warn' : '') + '"><div class="trx-kpi-k"><i class="bi bi-exclamation-triangle"></i>Con preventa</div><div class="trx-kpi-v">' + (r.n_con_preventa || 0) + '</div><div class="trx-kpi-d">Docs sin stock al vender</div></div>'
      + '<div class="trx-kpi"><div class="trx-kpi-k"><i class="bi bi-box-arrow-in-down"></i>Pedido total</div><div class="trx-kpi-v">' + fmtNum(r.total_pedido) + '</div><div class="trx-kpi-d">Suma de líneas</div></div>'
      + '<div class="trx-kpi trx-kpi-ok"><div class="trx-kpi-k"><i class="bi bi-box-seam"></i>Despachado</div><div class="trx-kpi-v">' + fmtNum(r.total_despachado) + '</div><div class="trx-kpi-d">Suma de líneas</div></div>'
      + '<div class="trx-kpi' + ((r.total_pendiente || 0) > 0 ? ' trx-kpi-warn' : '') + '"><div class="trx-kpi-k"><i class="bi bi-hourglass-split"></i>Pendiente</div><div class="trx-kpi-v">' + fmtNum(r.total_pendiente) + '</div><div class="trx-kpi-d">Saldo por despachar</div></div>'
      + '</div>'
      + '<h3 class="mt-2" style="font-weight:800;font-size:.92rem;color:var(--trx-black)"><i class="bi bi-list-ul me-1"></i>Detalle histórico</h3>'
      + tablaHtml
      + '</div>'
      + '</section>';
  }

  // ═══════════════════════════════════════════════════════════════════
  // Init
  // ═══════════════════════════════════════════════════════════════════
  document.addEventListener('DOMContentLoaded', function () {
    var input = $('prSearchInput');
    if (!input) return;
    input.addEventListener('input', onSearchInput);
    input.addEventListener('keydown', onSearchKeydown);
    $('prBtnFiltrar').addEventListener('click', function () {
      if (_skuActual) buscarTrazabilidad(_skuActual);
    });

    var skuInicial = (window.__PR_SKU_INICIAL || '').trim();
    if (skuInicial) {
      buscarTrazabilidad(skuInicial);
    }
  });
})();

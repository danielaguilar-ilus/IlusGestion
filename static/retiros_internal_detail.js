// Asegura class js-reveal en body para fade-in suave
if ('IntersectionObserver' in window){
  document.body.classList.add('js-reveal');
}
// ════════════════════════════════════════════════════════════════════
//  INFORME MENSUAL
// ════════════════════════════════════════════════════════════════════
async function abrirDescargaInforme(rutCliente){
  const mesDefault = new Date().toISOString().slice(0,7);
  const mes = await ilusPrompt({
    title: 'Informe mensual de retiros',
    message: 'Selecciona el mes a exportar (YYYY-MM):',
    sub: 'Genera Excel con todos los retiros del mes.',
    placeholder: 'Ej: ' + mesDefault,
    defaultValue: mesDefault,
    required: true,
  });
  if (!mes) return;
  if (!/^\d{4}-\d{2}$/.test(mes)){ ilusToast('Formato inválido. Usa YYYY-MM.', { type:'warning' }); return; }
  const soloCliente = await ilusConfirm({
    title: 'Filtrar por este cliente',
    message: `¿Limitar el informe SOLO al RUT ${rutCliente || ''}?`,
    sub: 'Cancela para descargar TODOS los retiros del mes.',
    okLabel: 'Sí, solo este cliente', cancelLabel: 'No, todos',
  });
  let url = `/retiros/api/informe-mes.xlsx?mes=${encodeURIComponent(mes)}`;
  if (soloCliente && rutCliente) url += `&cliente_rut=${encodeURIComponent(rutCliente)}`;
  window.location.href = url;
}

// ════════════════════════════════════════════════════════════════════
//  BORRADO TOTAL DE LA SOLICITUD — SOLO SUPERADMIN
//  Daniel 2026-05-23: doble confirmación (ilusConfirm + ilusPrompt con
//  texto literal "BORRAR") para evitar fat-finger en celular.
// ════════════════════════════════════════════════════════════════════
async function superadminEliminarSolicitud(){
  const code = RETIROS_DETAIL_DATA.reqCode;
  const cliente = RETIROS_DETAIL_DATA.customerNameHtml;

  // 1) Confirmación general
  const sigue = await ilusConfirm({
    title: 'Eliminar solicitud completa',
    message: `¿Seguro que quieres eliminar la solicitud ${code}?`,
    sub: `Cliente: <strong>${cliente}</strong><br>` +
         'Esto borra <strong>permanentemente</strong>: docs asociados, fotos, ' +
         'firmas, propuestas, mensajes y todo el historial.<br>' +
         '<strong style="color:#dc2626">Esta acción no se puede deshacer.</strong>',
    subHtml: true,
    okLabel: 'Continuar', cancelLabel: 'Cancelar',
    danger: true,
  });
  if (!sigue) return;

  // 2) Confirmación con texto literal — bloquea fat-finger
  const conf = await ilusPrompt({
    title: 'Confirmación final',
    message: 'Para confirmar, escribe en mayúsculas:',
    sub: '<strong style="color:#dc2626;font-size:1.1rem">BORRAR</strong>',
    subHtml: true,
    placeholder: 'BORRAR',
    required: true,
  });
  if (!conf) return;
  if (conf.trim().toUpperCase() !== 'BORRAR'){
    ilusToast('Confirmación incorrecta — debes escribir BORRAR', { type:'warning' });
    return;
  }

  // 3) Llamar endpoint con feedback visual
  const btn = document.getElementById('btnSuperadminEliminar');
  const original = btn.innerHTML;
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Eliminando…';

  try {
    const r = await fetch(`/retiros/${RETIROS_DETAIL_DATA.reqId}`, {
      method: 'DELETE',
      headers: {
        'Content-Type': 'application/json',
        'X-Confirm-Delete': 'BORRAR',
        'X-Requested-With': 'XMLHttpRequest',
        'Accept': 'application/json',
      },
      body: JSON.stringify({ confirm: 'BORRAR' }),
    });
    let d;
    try { d = await r.json(); }
    catch(parseErr){
      btn.disabled = false;
      btn.innerHTML = original;
      ilusAlert({
        type: 'error',
        title: 'Error inesperado',
        message: `El servidor respondió HTML en lugar de JSON (status ${r.status}). ¿Sesión expirada?`,
      });
      return;
    }
    if (!d.ok){
      btn.disabled = false;
      btn.innerHTML = original;
      ilusAlert({
        type: 'error',
        title: 'No se pudo eliminar',
        message: d.error || `Error HTTP ${r.status}`,
      });
      return;
    }
    // Éxito → toast + redirect al dashboard
    ilusToast(`✓ ${d.message || 'Solicitud eliminada'}`, { type:'success', duration:3000 });
    setTimeout(()=> { window.location.href = RETIROS_DETAIL_DATA.pickupDashboardUrl; }, 800);
  } catch(e){
    btn.disabled = false;
    btn.innerHTML = original;
    ilusAlert({
      type: 'error',
      title: 'Sin conexión',
      message: 'No se pudo contactar al servidor: ' + (e.message || 'error de red'),
    });
  }
}

// ════════════════════════════════════════════════════════════════════
//  TABS DE INFO
// ════════════════════════════════════════════════════════════════════
function cambiarTabInfo(name){
  document.querySelectorAll('.info-tabs-nav button').forEach(b => b.classList.remove('is-active'));
  document.querySelectorAll('.info-tabs-pane').forEach(p => p.classList.remove('is-active'));
  const btn = document.querySelector(`.info-tabs-nav button[data-tab="${name}"]`);
  const pane = document.getElementById('tab-' + name);
  if (btn) btn.classList.add('is-active');
  if (pane) pane.classList.add('is-active');
}

// ════════════════════════════════════════════════════════════════════
//  STEP REVEAL (fade-in al entrar en viewport)
// ════════════════════════════════════════════════════════════════════
(function _initStepReveal(){
  const steps = document.querySelectorAll('.step-section');
  if (!('IntersectionObserver' in window)){
    steps.forEach(s => s.classList.add('in-view'));
    return;
  }
  const obs = new IntersectionObserver((entries) => {
    entries.forEach(entry => {
      if (entry.isIntersecting){
        entry.target.classList.add('in-view');
        obs.unobserve(entry.target);
      }
    });
  }, { threshold: 0.08, rootMargin: '0px 0px -20px 0px' });
  steps.forEach((s, i) => {
    s.style.transitionDelay = (i * 60) + 'ms';
    obs.observe(s);
  });
  setTimeout(() => {
    steps.forEach(s => {
      if (!s.classList.contains('in-view')){
        const r = s.getBoundingClientRect();
        if (r.top < window.innerHeight) s.classList.add('in-view');
      }
    });
  }, 800);
})();
// ════════════════════════════════════════════════════════════════════
//  ESTADO COMPARTIDO + UTILS
// ════════════════════════════════════════════════════════════════════
const _RID = RETIROS_DETAIL_DATA.reqId;
const _RUT_CLI = RETIROS_DETAIL_DATA.customerRut;
let _erpLineas = [];

function _esc(s){ return String(s == null ? '' : s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); }

/* ════════════════════════════════════════════════════════════════════
   _fetchJsonSafe(url, opts)
   ────────────────────────────────────────────────────────────────────
   Fix Daniel 2026-05-23 (CRÍTICO): cuando el backend devuelve HTML
   (sesión expirada → login, 500 sin handler, breaker 503 con HTML, etc.),
   `r.json()` crashea con "Unexpected token '<', '<!doctype...".
   Este helper:
     1. Marca request como AJAX (header X-Requested-With) para que el
        backend devuelva JSON 401/403 en lugar de redirect HTML.
     2. Solicita Accept: application/json explícito.
     3. Si el response NO es JSON, devuelve un objeto sintético
        {ok:false, error:'...', _http_status:N, _not_json:true}
        con mensaje legible según el status.
   Uso:
     const d = await _fetchJsonSafe('/foo', {method:'POST', body:...});
     if (!d.ok) { mostrar(d.error); return; }
   ════════════════════════════════════════════════════════════════════ */
async function _fetchJsonSafe(url, opts){
  opts = opts || {};
  opts.headers = opts.headers || {};
  // Garantizar AJAX/JSON detection en el backend
  if (!opts.headers['X-Requested-With'] && !(opts.headers instanceof Headers)){
    opts.headers['X-Requested-With'] = 'XMLHttpRequest';
  }
  if (!opts.headers['Accept'] && !(opts.headers instanceof Headers)){
    opts.headers['Accept'] = 'application/json';
  }
  let r;
  try {
    r = await fetch(url, opts);
  } catch(netErr){
    return { ok:false, error:'Sin conexión: ' + (netErr.message||'error de red'),
             _not_json:true, _http_status:0 };
  }
  const ct = (r.headers.get('content-type') || '').toLowerCase();
  if (ct.includes('application/json') || ct.includes('+json')){
    try { const d = await r.json(); d._http_status = r.status; return d; }
    catch(je){ /* fallthrough */ }
  }
  // Respuesta NO JSON — derivar mensaje según status
  let msg;
  if (r.status === 401)      msg = 'Tu sesión expiró. Recarga la página e inicia sesión nuevamente.';
  else if (r.status === 403) msg = 'No tienes permiso para esta acción. Pide al admin que active el permiso.';
  else if (r.status === 404) msg = 'Recurso no encontrado en el servidor.';
  else if (r.status === 502 || r.status === 503 || r.status === 504)
    msg = 'El servidor está sobrecargado o el ERP no responde. Reintenta en 30 segundos.';
  else if (r.status >= 500)  msg = 'Error interno del servidor. Reintenta o contacta soporte.';
  else                       msg = 'Respuesta inesperada del servidor (HTTP ' + r.status + ').';
  // Log discreto para diagnóstico
  try { console.warn('[_fetchJsonSafe] Non-JSON response from', url, '→', r.status, ct); } catch(_e){}
  return { ok:false, error: msg, _not_json:true, _http_status: r.status };
}

// ════════════════════════════════════════════════════════════════════
//  SUGERENCIAS — Documentos con saldo del cliente (solo_con_saldo=1)
// ════════════════════════════════════════════════════════════════════
async function cargarSaldoCliente(rid){
  const cont = document.getElementById('saldoClienteContenido');
  const rut = _RUT_CLI;
  if (!rut){
    cont.innerHTML = '<div class="text-muted small">Sin RUT de cliente.</div>';
    return;
  }
  const dias = document.getElementById('saldoDias').value || '30';
  cont.innerHTML = '<div class="text-muted small py-2"><i class="bi bi-hourglass-split me-1"></i>Consultando documentos con saldo...</div>';
  try {
    const r = await fetch(`/retiros/api/cliente/${encodeURIComponent(rut)}/saldo-pendiente?dias=${dias}&solo_con_saldo=1`);
    const d = await r.json();

    // Hint inteligente que ENSEÑA al usuario
    const hintEl = document.getElementById('hintSaldo');
    const hintMsg = document.getElementById('hintSaldoMsg');
    if (d.hint){
      hintMsg.innerHTML = _esc(d.hint);
      hintEl.style.display = 'flex';
      hintEl.className = 'smart-hint ' + (d.resumen && d.resumen.con_saldo > 0 ? '' : 'is-warn');
    } else if (d.docs && d.docs.length === 0 && d.resumen && d.resumen.sin_saldo > 0){
      hintMsg.innerHTML = `Este cliente tiene <strong>${d.resumen.sin_saldo}</strong> documento(s) emitidos pero todos están ya despachados. ¿Estás seguro que viene a retirar algo?`;
      hintEl.style.display = 'flex';
      hintEl.className = 'smart-hint is-warn';
    } else if (d.resumen && d.resumen.con_saldo === 1){
      hintMsg.innerHTML = `Solo hay <strong>1 documento con saldo</strong> pendiente — lo más probable es que sea ese. Asócialo y avanza al paso 3.`;
      hintEl.style.display = 'flex';
      hintEl.className = 'smart-hint';
    } else {
      hintEl.style.display = 'none';
    }

    if (d.error && (!d.docs || d.docs.length === 0)){
      cont.innerHTML = `<div class="text-muted small py-2"><i class="bi bi-info-circle me-1"></i>${_esc(d.error)}</div>`;
      return;
    }
    if (!d.docs || d.docs.length === 0){
      cont.innerHTML = `<div class="text-muted small py-2"><i class="bi bi-info-circle me-1"></i>Sin documentos con saldo pendiente en últimos ${dias} días. Usa el buscador manual de abajo si tienes el N° del documento.</div>`;
      return;
    }

    // Render cards SIEMPRE ABIERTAS por default (Daniel 2026-05-23: "déjala
    // siempre abierta todo el rato posible, todo lo que pueda a través del
    // cliente"). Cargamos las líneas EN PARALELO al render.
    cont.innerHTML = d.docs.map((doc, idx) => {
      const yaAso = doc.ya_tiene_retiro;
      const fecha = doc.fecha || doc.fecha_iso || '';
      return `<div class="erp-doc-card is-open ${yaAso ? 'is-already' : ''}" data-tido="${_esc(doc.tido_display)}" data-nudo="${_esc(doc.nudo_display)}" data-idx="${idx}">
        <div class="erp-doc-card-head" onclick="toggleErpDoc(${idx})">
          <div class="erp-doc-num">${_esc(doc.tido_display)} ${_esc(doc.nudo_display)}</div>
          <div class="erp-doc-meta">
            <span class="ddate">${_esc(fecha)}</span>
            <span class="dcli">${_esc(doc.cliente || '')}</span>
          </div>
          <div class="erp-doc-totals">
            <span class="badge-pill ok"><i class="bi bi-check-circle"></i> Con saldo</span>
            <span class="badge-pill">${doc.n_lineas || 0} líneas</span>
            <span class="badge-pill" title="Total bruto">${Math.round(doc.total||0).toLocaleString('es-CL')} $</span>
          </div>
          ${yaAso
            ? '<button class="erp-doc-add-btn is-done" disabled><i class="bi bi-check"></i>Ya en este retiro</button>'
            : `<button class="erp-doc-add-btn" onclick="event.stopPropagation();agregarDocDirecto(${idx})"><i class="bi bi-plus-lg"></i>Agregar al retiro</button>`}
          <button class="erp-doc-toggle" onclick="event.stopPropagation();toggleErpDoc(${idx})" title="Colapsar / expandir"><i class="bi bi-chevron-up"></i></button>
        </div>
        <div class="erp-doc-body" id="erpDocBody-${idx}">
          <div class="text-muted small py-2"><i class="bi bi-hourglass-split me-1"></i>Cargando líneas del documento...</div>
        </div>
      </div>`;
    }).join('');

    // Guardar el array para uso posterior
    window._erpSugDocs = d.docs;

    // Auto-cargar TODAS las líneas en paralelo (Daniel: ver todo al instante).
    // ⚡ PERF (Daniel 2026-05-24): bajamos de 6 → 3 concurrentes para evitar
    // contention sobre la pool de pymssql (~5 conns). Con 3 el ERP responde
    // con baja latencia consistente; con 6 hay queue que aumenta p95.
    // El cache server-side de /api/erp/documento (5min) hace el resto.
    (async () => {
      const CHUNK = 3;
      for (let i = 0; i < d.docs.length; i += CHUNK){
        const chunk = d.docs.slice(i, i + CHUNK);
        await Promise.all(chunk.map((_, j) => _loadErpDocLines(i + j)));
      }
    })();
  } catch(e){
    cont.innerHTML = `<div class="text-muted small py-2"><i class="bi bi-x-circle me-1"></i>No se pudo consultar el ERP: ${_esc(e.message)}</div>`;
  }
}

// Carga las líneas de un doc específico (lazy o eager — mismo código).
// Extraído de toggleErpDoc para poder llamarlo desde render inicial.
// ⚡ PERF (Daniel 2026-05-24): memoización por (tido, nudo) para que si el
// operador colapsa/expande no haga otro fetch — el resultado vive en window.
window._erpDocCache = window._erpDocCache || new Map();
window._erpDocInflight = window._erpDocInflight || new Map();

async function _loadErpDocLines(idx){
  const body = document.getElementById('erpDocBody-' + idx);
  if (!body || body.dataset.loaded === '1') return;
  const doc = (window._erpSugDocs || [])[idx];
  if (!doc) return;
  const cacheKey = (doc.tido_display || '') + '|' + (doc.nudo_display || '');
  try {
    let dd;
    // Hit cache cliente
    if (window._erpDocCache.has(cacheKey)){
      dd = window._erpDocCache.get(cacheKey);
    } else {
      // De-duplicación: si ya hay una request en vuelo para este doc, esperamos
      let p = window._erpDocInflight.get(cacheKey);
      if (!p){
        p = fetch('/api/erp/documento', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ tido: doc.tido_display, nudo: doc.nudo_display })
        }).then(async (r) => {
          const ddJson = await r.json();
          if (r.ok && !ddJson.error){
            window._erpDocCache.set(cacheKey, ddJson);
          }
          return ddJson;
        }).finally(() => {
          window._erpDocInflight.delete(cacheKey);
        });
        window._erpDocInflight.set(cacheKey, p);
      }
      dd = await p;
    }
    if (dd.error){
      body.innerHTML = `<div class="text-muted small py-2"><i class="bi bi-x-circle me-1"></i>${_esc(dd.error || 'Error consultando ERP')}</div>`;
      return;
    }
    const lineas = (dd.lineas || []).filter(l => !l.es_zz);
    if (!lineas.length){
      body.innerHTML = '<div class="text-muted small py-2"><i class="bi bi-info-circle me-1"></i>Sin líneas de productos (solo servicios).</div>';
      body.dataset.loaded = '1';
      return;
    }
    body.innerHTML = `<div style="font-size:.72rem;font-weight:800;letter-spacing:.06em;text-transform:uppercase;color:#0a0a0a;margin-bottom:6px">Productos pendientes (${lineas.length})</div>` +
      lineas.map(l => {
        const qty = parseFloat(l.cantidad || 0);
        const saldoQty = qty;
        return `<div class="erp-line">
          <span class="ln-sku">${_esc(l.sku || '')}</span>
          <span class="ln-desc">${_esc(l.descripcion_erp || l.nombre_app || '')}</span>
          <span class="ln-qty">${qty} u. <span class="ln-qty-saldo">· saldo ${saldoQty} u.</span></span>
        </div>`;
      }).join('');
    body.dataset.loaded = '1';
  } catch(e){
    body.innerHTML = `<div class="text-muted small py-2"><i class="bi bi-x-circle me-1"></i>Error de red: ${_esc(e.message)}</div>`;
  }
}

// Toggle expandir/colapsar doc. Las líneas se cargan via _loadErpDocLines (en
// render inicial Y aquí si por alguna razón faltó).
async function toggleErpDoc(idx){
  const card = document.querySelector(`.erp-doc-card[data-idx="${idx}"]`);
  if (!card) return;
  const isOpen = card.classList.toggle('is-open');
  // Cambiar icono del chevron
  const chev = card.querySelector('.erp-doc-toggle i');
  if (chev) chev.className = isOpen ? 'bi bi-chevron-up' : 'bi bi-chevron-down';
  if (!isOpen) return;
  await _loadErpDocLines(idx);
}

// Agregar un doc al retiro desde las sugerencias
async function agregarDocDirecto(idx){
  const doc = (window._erpSugDocs || [])[idx];
  if (!doc) return;
  const card = document.querySelector(`.erp-doc-card[data-idx="${idx}"]`);
  const btn = card ? card.querySelector('.erp-doc-add-btn') : null;
  if (btn){ btn.disabled = true; btn.innerHTML = '<span class="spinner-border spinner-border-sm"></span> Agregando...'; }
  try {
    const r = await fetch(`/retiros/${_RID}/docs/agregar`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ document_type: doc.tido_display, document_number: doc.nudo_display }),
    });
    const d = await r.json();
    if (r.status === 409){
      ilusToast(`El doc ${doc.tido_display} ${doc.nudo_display} ya está asociado a este retiro.`, { type:'warning' });
      if (btn){ btn.disabled = true; btn.className = 'erp-doc-add-btn is-done'; btn.innerHTML = '<i class="bi bi-check"></i>Ya está'; }
      return;
    }
    if (!d.ok){
      ilusToast('Error: ' + (d.error || 'No se pudo agregar'), { type:'error' });
      if (btn){ btn.disabled = false; btn.innerHTML = '<i class="bi bi-plus-lg"></i>Agregar al retiro'; }
      return;
    }
    if (d.warning_otro_retiro){
      ilusToast(`⚠ También figura en otro retiro: ${d.warning_otro_retiro.code}`, { type:'warning', duration: 5000 });
    } else {
      ilusToast(`✓ ${doc.tido_display} ${doc.nudo_display} agregado`, { type:'success' });
    }
    if (btn){ btn.className = 'erp-doc-add-btn is-done'; btn.innerHTML = '<i class="bi bi-check"></i>Agregado'; }
    if (card) card.classList.add('is-already');
    await refrescarDocsAsociados(_RID);
  } catch(e){
    ilusToast('Error de red: ' + e.message, { type:'error' });
    if (btn){ btn.disabled = false; btn.innerHTML = '<i class="bi bi-plus-lg"></i>Agregar al retiro'; }
  }
}

// ════════════════════════════════════════════════════════════════════
//  BUSCADOR MANUAL — buscar en ERP + agregar al retiro en un solo click
// ════════════════════════════════════════════════════════════════════
async function buscarYAgregarERP(rid){
  const tido = (document.getElementById('val_tido').value || '').toUpperCase();
  const nudo = (document.getElementById('val_nudo').value || '').trim();
  if (!tido || !nudo){ ilusToast('Ingresa tipo y N° del documento', { type:'warning' }); return; }
  const resDiv = document.getElementById('erpResult');
  const btn = document.getElementById('btnBuscarManual');
  if (btn){ btn.disabled = true; btn.innerHTML = '<span class="spinner-border spinner-border-sm"></span><span>Buscando...</span>'; }
  resDiv.innerHTML = '<div class="text-muted small py-2"><i class="bi bi-hourglass-split me-1"></i>Consultando ERP Random...</div>';
  try {
    const r = await fetch(`/retiros/${rid}/docs/agregar`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Requested-With': 'XMLHttpRequest',
        'Accept': 'application/json',
      },
      body: JSON.stringify({ document_type: tido, document_number: nudo }),
    });
    // FIX Daniel 2026-05-23: parse defensivo del JSON. Si el backend devuelve
    // HTML (error 500 default de Flask), no rompemos con "Unexpected token <".
    let d;
    try { d = await r.json(); }
    catch(parseErr){
      resDiv.innerHTML = `<div class="smart-hint is-danger" style="margin:0">
        <i class="bi bi-x-circle"></i>
        <div><strong>Backend respondió HTML (status ${r.status}).</strong><br>
          <small>Probable sesión expirada. Recarga la página y vuelve a iniciar sesión.</small>
        </div></div>`;
      return;
    }
    if (r.status === 409){
      resDiv.innerHTML = `<div class="smart-hint is-warn" style="margin:0"><i class="bi bi-exclamation-triangle-fill"></i><div>El documento ${_esc(tido)} ${_esc(nudo)} ya está asociado a este retiro.</div></div>`;
      return;
    }
    if (!d.ok){
      // FIX Daniel 2026-05-23: mostrar detalle REAL del backend, no genérico.
      // El backend ahora devuelve {error, detalle, tipo_error, trace?} con
      // info útil para diagnóstico inmediato.
      const errMain = d.error || 'No se pudo agregar';
      const tipoErr = d.tipo_error ? `<span style="opacity:.7">[${_esc(d.tipo_error)}]</span> ` : '';
      const traceHtml = d.trace ?
        `<details style="margin-top:8px"><summary style="cursor:pointer;font-size:.78rem;color:#475569">Ver traceback técnico</summary>
          <pre style="background:#1e293b;color:#e2e8f0;padding:8px;border-radius:6px;font-size:.7rem;overflow:auto;max-height:200px;margin:6px 0 0">${_esc(d.trace)}</pre>
        </details>` : '';
      resDiv.innerHTML = `<div class="smart-hint is-danger" style="margin:0">
        <i class="bi bi-x-circle"></i>
        <div>${tipoErr}${_esc(errMain)}${traceHtml}</div>
      </div>`;
      return;
    }
    let warnHtml = '';
    if (d.warning_otro_retiro){
      warnHtml = `<div class="smart-hint is-warn" style="margin:8px 0 0"><i class="bi bi-exclamation-triangle-fill"></i><div>También figura en otro retiro: <strong>${_esc(d.warning_otro_retiro.code)}</strong>. Verifica que no esté duplicado.</div></div>`;
    }
    // Si fue fallback minimal (sin enrichment), aviso al operador
    let minHtml = '';
    if (d.doc && d.doc.minimal_source){
      minHtml = `<div class="smart-hint is-info" style="margin:8px 0 0"><i class="bi bi-info-circle"></i><div>Doc asociado con info mínima (el motor de enrichment no respondió). Peso/volumen se calcularán al hacer click en "Revisar carga".</div></div>`;
    }
    resDiv.innerHTML = `<div class="smart-hint is-success" style="margin:0"><i class="bi bi-check-circle"></i><div>Documento <strong>${_esc(tido)} ${_esc(nudo)}</strong> agregado al retiro.</div></div>${warnHtml}${minHtml}`;
    ilusToast(`✓ ${tido} ${nudo} agregado`, { type:'success' });
    await refrescarDocsAsociados(rid);
  } catch(e){
    resDiv.innerHTML = `<div class="smart-hint is-danger" style="margin:0"><i class="bi bi-x-circle"></i><div>Error de red: ${_esc(e.message)}</div></div>`;
  } finally {
    if (btn){ btn.disabled = false; btn.innerHTML = '<i class="bi bi-plus-circle"></i><span>Buscar y agregar</span>'; }
  }
}

// Compat: nombre antiguo usado por código heredado/integraciones
window.agregarDocAlRetiro = buscarYAgregarERP;
window.buscarERP = buscarYAgregarERP;

// ════════════════════════════════════════════════════════════════════
//  QUITAR DOC ASOCIADO
// ════════════════════════════════════════════════════════════════════
async function quitarDoc(rid, docId, labelDoc){
  const ok = await ilusConfirm({
    title: 'Quitar documento',
    message: `¿Quitar ${labelDoc} de este retiro?`,
    sub: 'Esto NO elimina el documento del ERP, solo lo saca de este retiro.',
    okLabel: 'Sí, quitar', cancelLabel: 'Cancelar', danger: true,
  });
  if (!ok) return;
  try {
    const r = await fetch(`/retiros/${rid}/docs/${docId}`, { method: 'DELETE' });
    const d = await r.json();
    if (!d.ok){ ilusToast('Error: ' + (d.error || 'No se pudo quitar'), { type:'error' }); return; }
    ilusToast('✓ Documento quitado', { type:'success' });
    await refrescarDocsAsociados(rid);
    // Si quitamos uno y aparece en sugerencias, habilitarlo nuevamente
    cargarSaldoCliente(rid);
  } catch(e){
    ilusToast('Error de red: ' + e.message, { type:'error' });
  }
}

// ════════════════════════════════════════════════════════════════════
//  TABLAS ILUS — Daniel 2026-05-24
//  Tabla 1: documentos asociados al retiro (vista tipo "carrito")
//  Tabla 2: productos consolidados (líneas incluidas de todos los docs)
// ════════════════════════════════════════════════════════════════════
function _renderTablaDocsAsociados(docs){
  const body  = document.getElementById('tabDocsAsociadosBody');
  const badge = document.getElementById('tabDocsBadge');
  if (!body) return;
  if (badge) badge.textContent = docs.length;
  if (!docs.length){
    body.innerHTML = `<tr class="ilus-tabla-empty-row" id="rowDocsVacio">
      <td colspan="8">
        <div class="ilus-tabla-empty">
          <i class="bi bi-inbox"></i>
          <strong>Aún no hay documentos asociados</strong>
          <small>Haz click en el botón rojo de arriba para buscar y agregar facturas/boletas.</small>
        </div>
      </td>
    </tr>`;
    return;
  }
  body.innerHTML = docs.map((d, idx) => {
    let saldoPill;
    if (d.con_saldo === 1) saldoPill = '<span class="td-pill td-pill-ok"><i class="bi bi-check-circle"></i>Con saldo</span>';
    else if (d.con_saldo === 0) saldoPill = '<span class="td-pill td-pill-warn"><i class="bi bi-exclamation-triangle"></i>Sin saldo</span>';
    else saldoPill = '<span class="td-pill"><i class="bi bi-question-circle"></i>No verif.</span>';
    const tipoUp = String(d.document_type || '').toUpperCase();
    const numero = d.document_number || '';
    // 🔧 FIX Daniel 2026-05-24: mostrar "X / Y" cuando hay selección parcial
    // para que el operador vea cuántas líneas REALMENTE se asociaron.
    const _totalLn = d.n_lineas || 0;
    const _selLn   = (d.n_lineas_seleccionadas != null) ? d.n_lineas_seleccionadas : null;
    const _lineasCell = (d.has_seleccion_lineas && _selLn !== null)
      ? `<strong style="color:#92400e">${_selLn}</strong><small style="color:#9ca3af"> / ${_totalLn}</small>`
      : `<strong>${_totalLn}</strong>`;
    return `<tr data-doc-id="${d.id}">
      <td data-label="#">${idx + 1}</td>
      <td data-label="Tipo"><span class="td-pill td-pill-dark">${_esc(tipoUp)}</span></td>
      <td data-label="Nº" class="mono">${_esc(numero)}</td>
      <td data-label="Cliente">
        <div class="td-cli">
          <strong>${_esc(d.cliente_nombre || '—')}</strong>
          <small class="mono">${_esc(d.cliente_rut || '—')}</small>
        </div>
      </td>
      <td data-label="Líneas" class="num" title="${d.has_seleccion_lineas?'Líneas seleccionadas / total del documento':'Total de líneas'}">${_lineasCell}</td>
      <td data-label="Peso" class="num">${parseFloat(d.peso_real_kg||0).toFixed(1)} kg</td>
      <td data-label="Saldo">${saldoPill}</td>
      <td data-label="Acciones" class="acciones">
        <button type="button" class="td-btn td-btn-prods"
                onclick="abrirSeleccionProductos(${d.id}, '${_esc(tipoUp)}', '${_esc(numero)}')"
                title="Ver y elegir qué productos retirar de esta factura">
          <i class="bi bi-list-check"></i><span>Productos</span>
        </button>
        <button type="button" class="td-btn td-btn-quitar"
                onclick="quitarDoc(${_RID}, ${d.id}, '${_esc(tipoUp)} ${_esc(numero)}')"
                title="Quitar del retiro (no afecta al ERP)">
          <i class="bi bi-x-lg"></i><span>Quitar</span>
        </button>
      </td>
    </tr>`;
  }).join('');
}

async function refrescarTablaProductos(){
  const body   = document.getElementById('tabProductosBody');
  const foot   = document.getElementById('tabProductosFoot');
  const badge  = document.getElementById('tabProdsBadge');
  const btnRef = document.querySelector('.ilus-tabla-refresh');
  if (!body) return;
  if (btnRef) btnRef.classList.add('is-loading');
  try {
    const d = await _fetchJsonSafe(`/retiros/${_RID}/lineas-resumen`);
    if (!d.ok){
      body.innerHTML = `<tr class="ilus-tabla-empty-row"><td colspan="8">
        <div class="ilus-tabla-empty">
          <i class="bi bi-exclamation-triangle" style="color:#dc2626"></i>
          <strong>No se pudo cargar la lista de productos</strong>
          <small>${_esc(d.error || 'Error desconocido')}</small>
        </div></td></tr>`;
      if (foot) foot.style.display = 'none';
      if (badge) badge.textContent = '0';
      return;
    }
    const lineas = d.lineas || [];
    const tot = d.totales || { n_lineas: 0, peso_total_kg: 0, vol_total_m3: 0 };
    if (badge) badge.textContent = tot.n_lineas || lineas.length;
    if (!lineas.length){
      body.innerHTML = `<tr class="ilus-tabla-empty-row"><td colspan="8">
        <div class="ilus-tabla-empty">
          <i class="bi bi-box"></i>
          <strong>Aún no hay productos seleccionados</strong>
          <small>Asocia una factura/boleta arriba y luego haz click en <strong>"Productos"</strong> en cada fila para elegir qué se va a retirar. Si no eliges nada, se incluye todo lo del documento.</small>
        </div></td></tr>`;
      if (foot) foot.style.display = 'none';
      return;
    }
    body.innerHTML = lineas.map(ln => {
      const docTipo = String(ln.doc_tipo || '').toUpperCase();
      // 🆕 Daniel 2026-05-24: badge ámbar para líneas marcadas SIN saldo en ERP
      // (el operador las incluyó manualmente aunque Random las reporte como ya
      // entregadas). Da aviso visual sin bloquear la operación.
      const sinSaldoBadge = ln.marcada_sin_saldo
        ? '<span class="td-sin-saldo-badge" title="Esta línea ya estaba rebajada en el ERP Random (figura como entregada). El operador la incluyó igualmente porque el cliente viene a retirarla.">'
          + '<i class="bi bi-exclamation-triangle-fill"></i>'
          + '<span>Ya rebajado en ERP</span>'
          + '</span>'
        : '';
      const rowCls = ln.marcada_sin_saldo ? ' class="td-row-sin-saldo"' : '';
      return `<tr${rowCls}>
        <td data-label="SKU" class="mono">${_esc(ln.sku || '—')}</td>
        <td data-label="Descripción">
          <div class="td-prod-desc">${_esc(ln.descripcion || '(sin descripción)')}</div>
          ${sinSaldoBadge}
        </td>
        <td data-label="Doc origen">
          <span class="td-doc-origen">
            <span class="tdo-tipo">${_esc(docTipo)}</span>
            <span>${_esc(ln.doc_numero || '')}</span>
          </span>
        </td>
        <td data-label="Cantidad" class="num"><strong>${_fmtNum(ln.cantidad, 2)}</strong></td>
        <td data-label="Peso un." class="num">${_fmtNum(ln.peso_unit_kg, 2)} kg</td>
        <td data-label="Vol. un." class="num">${_fmtNum(ln.vol_unit_m3, 3)} m³</td>
        <td data-label="Total kg" class="num"><strong>${_fmtNum(ln.peso_total, 1)} kg</strong></td>
        <td data-label="Total m³" class="num"><strong>${_fmtNum(ln.vol_total, 3)} m³</strong></td>
      </tr>`;
    }).join('');
    if (foot){
      foot.style.display = '';
      const totKg = document.getElementById('tabProdsTotKg');
      const totM3 = document.getElementById('tabProdsTotM3');
      const totLn = document.getElementById('tabProdsTotLineas');
      if (totKg) totKg.textContent = _fmtNum(tot.peso_total_kg, 1) + ' kg';
      if (totM3) totM3.textContent = _fmtNum(tot.vol_total_m3, 3) + ' m³';
      if (totLn) totLn.textContent = tot.n_lineas || lineas.length;
    }
  } catch(e){
    body.innerHTML = `<tr class="ilus-tabla-empty-row"><td colspan="8">
      <div class="ilus-tabla-empty">
        <i class="bi bi-wifi-off" style="color:#dc2626"></i>
        <strong>Error de red</strong>
        <small>${_esc(e.message || 'No se pudo contactar al servidor')}</small>
      </div></td></tr>`;
  } finally {
    if (btnRef) btnRef.classList.remove('is-loading');
  }
}

function _fmtNum(n, dec){
  const v = parseFloat(n || 0);
  if (!isFinite(v)) return '0';
  return v.toFixed(dec == null ? 2 : dec);
}

function _renderDocAsoCard(d){
  let cls = 'no-verif', badge = '<span class="badge-pill"><i class="bi bi-question-circle me-1"></i>No verificado</span>';
  if (d.con_saldo === 1){
    cls = '';
    badge = '<span class="badge-pill" style="background:#dcfce7;color:#166534;border-color:#86efac"><i class="bi bi-check-circle me-1"></i>Con saldo</span>';
  } else if (d.con_saldo === 0){
    cls = 'no-saldo';
    badge = '<span class="badge-pill" style="background:#fef3c7;color:#92400e;border-color:#fde68a"><i class="bi bi-exclamation-triangle me-1"></i>Sin saldo</span>';
  }
  // Daniel 2026-05-23: badge si tiene selección granular
  const hasSel = d.has_seleccion_lineas ? `<span class="badge-pill" style="background:#fef3c7;color:#92400e;border-color:#fde68a" title="Solo retira líneas seleccionadas"><i class="bi bi-funnel-fill me-1"></i>Selección parcial</span>` : '';
  // 🔧 FIX Daniel 2026-05-24: mostrar "X / Y líneas" cuando hay selección
  // granular. Antes mostraba "10 líneas" siempre → el operador creía que
  // se asociaba el doc completo. Ahora se ve claramente "2 / 10".
  const _tot = d.n_lineas || 0;
  const _sel = (d.n_lineas_seleccionadas != null) ? d.n_lineas_seleccionadas : null;
  const _lineasBadge = (d.has_seleccion_lineas && _sel !== null)
    ? `<span class="badge-pill" title="Solo se retira la selección marcada" style="background:#fef3c7;color:#92400e;border-color:#fde68a;font-weight:700">${_sel} / ${_tot} líneas</span>`
    : `<span class="badge-pill" title="Total de líneas del documento">${_tot} líneas</span>`;
  return `<div class="doc-aso ${cls}" data-doc-id="${d.id}">
    <div class="da-num">${_esc(d.document_type)} ${_esc(d.document_number)}</div>
    <div class="da-cli">
      <div class="dnom">${_esc(d.cliente_nombre || '—')}</div>
      <div class="drut">${_esc(d.cliente_rut || '—')}</div>
    </div>
    <div class="da-totals">
      ${badge}
      ${hasSel}
      <span class="badge-pill weight">${parseFloat(d.peso_real_kg||0).toFixed(1)} kg</span>
      ${_lineasBadge}
    </div>
    <div class="da-actions">
      <button type="button" class="da-prods" onclick="abrirSeleccionProductos(${d.id}, '${_esc(d.document_type)}', '${_esc(d.document_number)}')" title="Seleccionar productos específicos a retirar">
        <i class="bi bi-funnel"></i>Productos
      </button>
      <button type="button" class="da-quitar" onclick="quitarDoc(${_RID}, ${d.id}, '${_esc(d.document_type)} ${_esc(d.document_number)}')">
        <i class="bi bi-x-lg"></i>Quitar
      </button>
    </div>
  </div>`;
}

// ════════════════════════════════════════════════════════════════════
//  SELECCIÓN GRANULAR DE PRODUCTOS POR DOC (Daniel 2026-05-23)
//  "de dos documentos que tengan 10 y 10 productos, podría seleccionar
//  2 y 3 productos de cada factura"
// ════════════════════════════════════════════════════════════════════
async function abrirSeleccionProductos(docId, tipo, numero){
  let modal = document.getElementById('selProdModal');
  if (!modal){
    modal = document.createElement('div');
    modal.id = 'selProdModal';
    modal.className = 'sel-prod-overlay';
    modal.innerHTML = `
      <div class="sel-prod-card" onclick="event.stopPropagation()">
        <div class="sel-prod-head">
          <div>
            <h5 id="selProdTitle" style="margin:0;font-weight:800">Productos del documento</h5>
            <div id="selProdSub" style="font-size:.78rem;color:#94a3b8;margin-top:2px"></div>
          </div>
          <button type="button" class="sel-prod-close" onclick="cerrarSeleccionProductos()" title="Cerrar">
            <i class="bi bi-x-lg"></i>
          </button>
        </div>
        <div id="selProdBody" class="sel-prod-body">
          <div class="text-center py-4 text-muted"><i class="bi bi-hourglass-split"></i> Cargando líneas...</div>
        </div>
        <div class="sel-prod-foot">
          <div id="selProdResumen" class="sel-prod-resumen"></div>
          <div style="display:flex;gap:8px">
            <button type="button" class="btn-2027" onclick="cerrarSeleccionProductos()" style="background:#fff;color:#0a0a0a">Cancelar</button>
            <button type="button" class="btn-2027" id="selProdGuardar" onclick="guardarSeleccionProductos()" style="background:var(--ilus-red);color:#fff">
              <i class="bi bi-check-circle me-1"></i>Guardar selección
            </button>
          </div>
        </div>
      </div>`;
    modal.addEventListener('click', e => { if (e.target === modal) cerrarSeleccionProductos(); });
    document.body.appendChild(modal);
  }
  document.getElementById('selProdTitle').textContent = `Productos de ${tipo} ${numero}`;
  document.getElementById('selProdSub').textContent = 'Marca solo los productos que el cliente va a retirar';
  document.getElementById('selProdBody').innerHTML = '<div class="text-center py-4 text-muted"><i class="bi bi-hourglass-split"></i> Cargando líneas...</div>';
  modal.style.display = 'flex';
  modal.dataset.docId = docId;
  try {
    const r = await fetch(`/retiros/${_RID}/docs/${docId}/lineas`);
    const d = await r.json();
    if (!d.ok){
      document.getElementById('selProdBody').innerHTML = `<div class="text-danger py-3">${_esc(d.error || 'Error al cargar')}</div>`;
      return;
    }
    _renderSeleccionLineas(d.lineas || []);
  } catch(e){
    document.getElementById('selProdBody').innerHTML = `<div class="text-danger py-3">Error de red: ${_esc(e.message)}</div>`;
  }
}

function cerrarSeleccionProductos(){
  const m = document.getElementById('selProdModal');
  if (m) m.style.display = 'none';
}

function _renderSeleccionLineas(lineas){
  const body = document.getElementById('selProdBody');
  if (!lineas.length){
    body.innerHTML = '<div class="text-muted py-3 text-center">Sin productos (solo servicios).</div>';
    document.getElementById('selProdGuardar').disabled = true;
    return;
  }
  // Bulk actions
  const head = `<div class="sel-prod-bulk">
    <button type="button" class="btn-bulk" onclick="_selProdToggleAll(true)"><i class="bi bi-check-all"></i> Marcar todas</button>
    <button type="button" class="btn-bulk" onclick="_selProdToggleAll(false)"><i class="bi bi-square"></i> Desmarcar todas</button>
    <span style="flex:1"></span>
    <span style="font-size:.74rem;color:#94a3b8">Editá la cantidad si quieres retirar menos que lo que está en el doc.</span>
  </div>`;
  const rows = lineas.map((ln, idx) => `
    <div class="sel-prod-row" data-sku="${_esc(ln.sku)}" data-cantidad-doc="${ln.cantidad_doc}">
      <label class="sel-prod-check">
        <input type="checkbox" ${ln.incluida ? 'checked' : ''} onchange="_selProdRecalc()">
      </label>
      <div class="sel-prod-info">
        <div class="sel-prod-sku">${_esc(ln.sku)}</div>
        <div class="sel-prod-desc">${_esc(ln.descripcion || '(sin descripción)')}</div>
      </div>
      <div class="sel-prod-qty">
        <label style="font-size:.66rem;color:#94a3b8;text-transform:uppercase;letter-spacing:.04em;font-weight:700">Cantidad</label>
        <div class="sel-prod-qty-input">
          <input type="number" min="0" max="${ln.cantidad_doc}" step="0.01"
                 value="${ln.cantidad_seleccionada}"
                 data-qty-input="${idx}"
                 onchange="_selProdRecalc()"
                 oninput="_selProdRecalc()">
          <span style="color:#94a3b8;font-size:.74rem">/ ${ln.cantidad_doc}</span>
        </div>
      </div>
      <div class="sel-prod-totals" data-uxv="${ln.unidades_por_venta || 1}">
        <div><i class="bi bi-box"></i> <span data-peso="${ln.peso_unit_kg}">0.0</span> kg</div>
        <div><i class="bi bi-bounding-box"></i> <span data-vol="${ln.vol_unit_m3}">0.000</span> m³</div>
      </div>
    </div>
  `).join('');
  body.innerHTML = head + rows;
  _selProdRecalc();
  document.getElementById('selProdGuardar').disabled = false;
}

function _selProdToggleAll(checked){
  document.querySelectorAll('.sel-prod-row input[type="checkbox"]').forEach(cb => cb.checked = checked);
  _selProdRecalc();
}

function _selProdRecalc(){
  let totPeso = 0, totVol = 0, nIncluidas = 0, nTotal = 0;
  document.querySelectorAll('.sel-prod-row').forEach(row => {
    nTotal++;
    const cb = row.querySelector('input[type="checkbox"]');
    const qtyInp = row.querySelector('input[type="number"]');
    const qty = parseFloat(qtyInp.value || 0);
    const incluida = cb.checked && qty > 0;
    const pesoSpan = row.querySelector('[data-peso]');
    const volSpan  = row.querySelector('[data-vol]');
    const pesoUnit = parseFloat(pesoSpan.dataset.peso || 0);
    const volUnit  = parseFloat(volSpan.dataset.vol || 0);
    /* Productos con unidad secundaria (discos, mancuernas): la ficha describe
       el PAR y el ERP cuenta piezas sueltas, así que 4 piezas son 2 empaques.
       Sin esto, el peso del retiro salía al doble (2026-08-07, caso FCV 11225). */
    const uxv = Math.max(parseFloat(pesoSpan.closest('[data-uxv]')?.dataset.uxv) || 1, 1);
    const equiv = uxv > 1 ? (qty / uxv) : qty;
    const pesoTot = incluida ? pesoUnit * equiv : 0;
    const volTot  = incluida ? volUnit * equiv : 0;
    pesoSpan.textContent = pesoTot.toFixed(1);
    volSpan.textContent  = volTot.toFixed(3);
    if (incluida){
      totPeso += pesoTot;
      totVol  += volTot;
      nIncluidas++;
    }
    row.classList.toggle('is-excluded', !incluida);
  });
  const res = document.getElementById('selProdResumen');
  res.innerHTML = `<strong>${nIncluidas}/${nTotal}</strong> incluidas · <strong>${totPeso.toFixed(1)} kg</strong> · <strong>${totVol.toFixed(3)} m³</strong>`;
}

async function guardarSeleccionProductos(){
  const modal = document.getElementById('selProdModal');
  const docId = modal.dataset.docId;
  if (!docId) return;
  const lineas = [];
  document.querySelectorAll('.sel-prod-row').forEach(row => {
    const sku = row.dataset.sku;
    const cb = row.querySelector('input[type="checkbox"]');
    const qtyInp = row.querySelector('input[type="number"]');
    const qty = parseFloat(qtyInp.value || 0);
    lineas.push({ sku, incluida: !!cb.checked, cantidad_seleccionada: qty });
  });
  const btn = document.getElementById('selProdGuardar');
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner-border spinner-border-sm"></span> Guardando...';
  try {
    const r = await fetch(`/retiros/${_RID}/docs/${docId}/lineas`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' },
      body: JSON.stringify({ lineas }),
    });
    const d = await r.json();
    if (!d.ok){
      ilusAlert({ type:'error', title:'No se pudo guardar', message: d.error || `HTTP ${r.status}` });
      return;
    }
    ilusToast(`✓ ${d.lineas_guardadas} líneas guardadas — totales actualizados`, { type:'success' });
    cerrarSeleccionProductos();
    await refrescarDocsAsociados(_RID);
    // refrescarDocsAsociados ya dispara refrescarTablaProductos() internamente.
  } catch(e){
    ilusAlert({ type:'error', title:'Sin conexión', message: e.message });
  } finally {
    btn.disabled = false;
    btn.innerHTML = '<i class="bi bi-check-circle me-1"></i>Guardar selección';
  }
}

async function refrescarDocsAsociados(rid){
  try {
    const r = await fetch(`/retiros/${rid}/docs`);
    const d = await r.json();
    if (!d.ok) return;
    const lista = document.getElementById('docsAsociadosLista');
    const badge = document.getElementById('badgeNDocs');
    const ndocs = (d.docs || []).length;
    const ncons = (d.saldo_summary && d.saldo_summary.con_saldo) || 0;
    if (badge) badge.textContent = ndocs;
    if (!d.docs || d.docs.length === 0){
      lista.innerHTML = `<div style="padding:16px 18px;text-align:center;color:#9ca3af;font-size:.85rem;background:#fafafa;border:1.5px dashed var(--gray-2);border-radius:10px"><i class="bi bi-inbox me-1"></i>Aún no hay documentos asociados.</div>`;
    } else {
      lista.innerHTML = d.docs.map(doc => _renderDocAsoCard(doc)).join('');
    }
    // Daniel 2026-05-24: refrescar tabla 1 (docs asociados) + tabla 2 (productos)
    _renderTablaDocsAsociados(d.docs || []);
    refrescarTablaProductos();
    // Refrescar Paso 3 — carga total
    const t = d.totales || {};
    const $1 = document.getElementById('cargaNDocs'); if ($1) $1.textContent = ndocs;
    const $2 = document.getElementById('cargaPeso'); if ($2) $2.textContent = `${parseFloat(t.peso_real_kg||0).toFixed(1)} kg`;
    const $3 = document.getElementById('cargaVol'); if ($3) $3.textContent = `${parseFloat(t.volumen_m3||0).toFixed(3)} m³`;
    const $4 = document.getElementById('cargaTiempo'); if ($4) $4.textContent = `${t.tiempo_estimado_min || '—'} min`;
    // Refrescar Paso 5 — resumen
    const $5 = document.getElementById('resumenDocs'); if ($5) $5.textContent = `${ndocs} doc${ndocs===1?'':'s'} · ${ncons} con saldo`;
    const $6 = document.getElementById('resumenCarga'); if ($6) $6.textContent = `${parseFloat(t.peso_real_kg||0).toFixed(1)} kg · ${parseFloat(t.volumen_m3||0).toFixed(3)} m³ · ${t.tiempo_estimado_min || '—'} min`;
    // Reflejar en inputs hidden (compat con form validación)
    ['val_peso_real','val_peso_vol','val_m3','val_tiempo'].forEach((id, i) => {
      const el = document.getElementById(id);
      if (!el) return;
      const vals = [
        parseFloat(t.peso_real_kg||0).toFixed(2),
        parseFloat(t.peso_vol_kg||0).toFixed(2),
        parseFloat(t.volumen_m3||0).toFixed(3),
        t.tiempo_estimado_min || 30,
      ];
      el.value = vals[i];
    });
    // Hint peso > 100kg
    const hintPeso = document.getElementById('hintPeso');
    if (hintPeso){
      hintPeso.style.display = parseFloat(t.peso_real_kg || 0) > 100 ? 'flex' : 'none';
    }
    // Actualizar estado de cada paso visualmente
    _refrescarEstadoPasos(ndocs, ncons, d.request_state || {});
  } catch(e){
    console.error('refrescarDocsAsociados', e);
  }
}

// Estado de pasos (verde/rojo/gris) según docs y propuesta.
// Daniel 2026-05-24: el paso 4 SOLO se bloquea cuando ndocs===0.
// Antes exigía ncons>0 (docs con saldo verificado por ERP) y dejaba
// al operador atrapado cuando el ERP no podía verificar el saldo ZZ
// (boletas sin línea ZZ, timeout, etc.). El warning de "sin saldo
// verificado" se muestra dentro del paso pero NO bloquea.
function _refrescarEstadoPasos(ndocs, ncons, requestState){
  const p2 = document.getElementById('paso-2');
  const p3 = document.getElementById('paso-3');
  const p4 = document.getElementById('paso-4');
  const p5 = document.getElementById('paso-5');
  if (p2){ p2.classList.toggle('is-complete', ndocs > 0); }
  if (p3){
    p3.classList.toggle('is-blocked', ndocs === 0);
    p3.classList.toggle('is-complete', ndocs > 0);  // si hay docs, totales se calcularon
  }
  if (p4){
    p4.classList.toggle('is-blocked', ndocs === 0);
    p4.classList.toggle('is-complete', !!requestState.step4_done);
    // Mostrar/ocultar form: basta con tener al menos UN doc asociado
    const form = document.getElementById('iwProposeForm');
    if (form){
      form.style.display = (ndocs > 0 && !requestState.step4_done) ? '' : 'none';
      if (ndocs > 0 && !requestState.step4_done && !window._calMounted){
        _mountProposeCalendar();
        window._calMounted = true;
      }
    }
    // Toggle hints inteligentes en el paso 4
    const hintBlock = document.getElementById('paso4HintBloqueo');
    if (hintBlock) hintBlock.style.display = (ndocs === 0) ? '' : 'none';
    const hintNoSaldo = document.getElementById('paso4HintSinSaldo');
    if (hintNoSaldo) hintNoSaldo.style.display = (ndocs > 0 && ncons === 0 && !requestState.step4_done) ? '' : 'none';
  }
  if (p5){
    // Paso 5 NUNCA se bloquea: el operador debe poder enviar siempre.
    // Solo se marca completo cuando el cliente acepta (step5_done).
    p5.classList.remove('is-blocked');
    p5.classList.toggle('is-complete', !!requestState.step5_done);
  }
}

// ════════════════════════════════════════════════════════════════════
//  CALENDARIO INTERNO (mismo widget que el público)
// ════════════════════════════════════════════════════════════════════
(function(){
  'use strict';
  const REQ_ID  = _RID;
  const DATE_IN = document.getElementById('icdCalDate');
  const GRID    = document.getElementById('icdSlotGrid');
  const SUMMARY = document.getElementById('icdCalSummary');
  const MODAL_EL = document.getElementById('icdOwnersModal');
  const MODAL_BODY = document.getElementById('icdOwnersModalBody');
  const MODAL_TITLE = document.getElementById('icdOwnersModalTitle');
  let _modalInstance = null;

  function _hmToMin(s){
    const p = String(s||'').split(':').map(Number);
    return (p[0]||0)*60 + (p[1]||0);
  }
  function _addDays(iso, n){
    const d = new Date(iso + 'T00:00:00');
    d.setDate(d.getDate() + n);
    return d.toISOString().slice(0,10);
  }
  function _renderSkel(){
    let html = '<div class="icd-cal-skel" aria-hidden="true">';
    for (let i=0;i<10;i++) html += '<div></div>';
    html += '</div>';
    GRID.innerHTML = html;
    SUMMARY.textContent = 'Cargando…';
  }
  function _renderEmpty(msg){
    GRID.innerHTML = '<div class="icd-cal-empty"><i class="bi bi-calendar-x"></i>' + _esc(msg) + '</div>';
    SUMMARY.textContent = '';
  }
  function _isThisRequestSlot(slot){
    const owners = Array.isArray(slot.owners) ? slot.owners : [];
    return owners.some(o => Number(o.request_id) === REQ_ID);
  }
  function _slotClass(slot){
    const cls = ['icd-slot'];
    const estado = slot.estado || (slot.lunch ? 'colacion' : (!slot.disponible ? 'completo' : 'disponible'));
    if (estado === 'colacion') cls.push('is-lunch');
    else if (estado === 'completo') cls.push('is-full');
    else if (estado === 'ocupado') cls.push('is-busy');
    else if (estado === 'bloqueado') cls.push('is-blocked');
    return { cls, estado };
  }
  function _renderDia(payload, fecha){
    const dia = (payload.dias || {})[fecha];
    if (!dia){ _renderEmpty('Sin datos para ' + fecha + '.'); return; }
    if (!dia.disponible){ _renderEmpty((dia.razon || 'Día no operativo') + ' — ' + fecha); return; }
    const slots = dia.slots || [];
    if (!slots.length){ _renderEmpty('Sin bloques configurados.'); return; }
    const lunchStart = (payload.lunch_start) || '12:30';
    const lunchStartMin = _hmToMin(lunchStart);
    let postLunchIdx = -1;
    for (let i = 0; i < slots.length; i++){
      const s = slots[i];
      const startMin = _hmToMin(s.time_from || s.hora || '00:00');
      const isLunch = (s.estado === 'colacion') || s.lunch;
      if (!isLunch && startMin >= lunchStartMin){ postLunchIdx = i; break; }
    }
    const hasMorning = slots.some((s, i) => postLunchIdx === -1 ? true : i < postLunchIdx);
    const hasAfternoon = postLunchIdx !== -1;
    const slotHtml = (s, i) => {
      const { cls, estado } = _slotClass(s);
      const owners = Array.isArray(s.owners) ? s.owners : [];
      const isCurrent = _isThisRequestSlot(s);
      if (isCurrent) cls.push('is-current');
      if (owners.length) cls.push('has-owners');
      const oc = s.ocupacion_actual != null ? s.ocupacion_actual : (s.ocupados || 0);
      const mx = s.capacidad_max    != null ? s.capacidad_max    : (s.max || 2);
      let label = '';
      if (estado === 'colacion') label = 'Colación';
      else if (estado === 'bloqueado') label = 'Bloqueado';
      else label = oc + '/' + mx;
      let ownersLine = '';
      if (owners.length === 1){
        ownersLine = `<div class="icd-slot-owners" title="${_esc(owners[0].code)} · ${_esc(owners[0].customer_name)}">${_esc(owners[0].code)}</div>`;
      } else if (owners.length > 1){
        ownersLine = `<div class="icd-slot-owners" title="${owners.length} retiros">${owners.length} retiros</div>`;
      }
      const hora = s.time_from || s.hora || '';
      return `<div class="${cls.join(' ')}" data-idx="${i}" role="button" tabindex="${owners.length ? 0 : -1}">
        <div class="icd-slot-hora">${_esc(hora)}</div>
        <div class="icd-slot-meta">${_esc(label)}</div>
        ${ownersLine}
      </div>`;
    };
    let html = '';
    if (hasMorning && hasAfternoon){
      html += '<div class="icd-slot-section-title">☀️ Mañana</div>';
      html += '<div class="icd-slot-grid-inner">' + slots.slice(0, postLunchIdx).map((s, i) => slotHtml(s, i)).join('') + '</div>';
      html += '<div class="icd-slot-section-title">🌤 Tarde</div>';
      html += '<div class="icd-slot-grid-inner">' + slots.slice(postLunchIdx).map((s, j) => slotHtml(s, postLunchIdx + j)).join('') + '</div>';
    } else {
      html = '<div class="icd-slot-grid-inner">' + slots.map((s, i) => slotHtml(s, i)).join('') + '</div>';
    }
    GRID.innerHTML = html;
    let lib=0, par=0, lle=0, lun=0, blk=0;
    slots.forEach(s => {
      const e = s.estado;
      if (e === 'disponible') lib++;
      else if (e === 'ocupado') par++;
      else if (e === 'completo') lle++;
      else if (e === 'colacion') lun++;
      else if (e === 'bloqueado') blk++;
    });
    const parts = [];
    if (lib) parts.push(`<span style="color:#16a34a"><strong>${lib}</strong> libres</span>`);
    if (par) parts.push(`<span style="color:#c2410c"><strong>${par}</strong> parciales</span>`);
    if (lle) parts.push(`<span style="color:#7f1d1d"><strong>${lle}</strong> llenos</span>`);
    if (blk) parts.push(`<span style="color:#6b7280"><strong>${blk}</strong> bloqueados</span>`);
    SUMMARY.innerHTML = parts.join(' · ');
    GRID.querySelectorAll('.icd-slot.has-owners').forEach(el => {
      el.addEventListener('click', () => {
        const i = parseInt(el.dataset.idx, 10);
        const slot = slots[i];
        if (slot) _openOwnersModal(fecha, slot);
      });
      el.addEventListener('keydown', (e) => {
        if (e.key === 'Enter' || e.key === ' '){ e.preventDefault(); el.click(); }
      });
    });
  }
  function _openOwnersModal(fecha, slot){
    const owners = Array.isArray(slot.owners) ? slot.owners : [];
    const hora = (slot.time_from || slot.hora || '') + (slot.time_to ? ' – ' + slot.time_to : '');
    MODAL_TITLE.innerHTML = `<i class="bi bi-people-fill me-1"></i>${_esc(fecha)} · ${_esc(hora)}`;
    if (!owners.length){
      MODAL_BODY.innerHTML = '<div class="text-center text-muted py-3">Este bloque no tiene retiros asignados.</div>';
    } else {
      MODAL_BODY.innerHTML = owners.map(o => {
        const isCur = Number(o.request_id) === REQ_ID;
        return `<div class="icd-owner-row ${isCur ? 'is-current' : ''}">
          <div class="icd-owner-info">
            <div class="icd-owner-code">${_esc(o.code)}${isCur ? ' <span class="badge bg-danger ms-1" style="font-size:.6rem">ESTE</span>' : ''}</div>
            <div class="icd-owner-name">${_esc(o.customer_name)}</div>
            <div class="icd-owner-status">${_esc(o.status_label || o.status || '—')}</div>
          </div>
          ${isCur
            ? '<span class="icd-owner-go" style="background:#9ca3af;cursor:default">Estás aquí</span>'
            : `<a class="icd-owner-go" href="${_esc(o.detail_url)}"><i class="bi bi-box-arrow-up-right"></i>Abrir</a>`}
        </div>`;
      }).join('');
    }
    if (!_modalInstance && window.bootstrap){
      _modalInstance = new bootstrap.Modal(MODAL_EL);
    }
    if (_modalInstance) _modalInstance.show();
  }
  async function _loadDia(fecha){
    if (!fecha){ _renderEmpty('Elige una fecha.'); return; }
    _renderSkel();
    try {
      const r = await fetch('/retiros/api/disponibilidad-publica?include_owners=1&date=' + encodeURIComponent(fecha), {
        headers: {'X-Requested-With': 'XMLHttpRequest'}, credentials: 'same-origin', cache: 'no-store',
      });
      if (!r.ok) throw new Error('HTTP ' + r.status);
      const payload = await r.json();
      _renderDia(payload, fecha);
    } catch (e){
      _renderEmpty('No se pudo cargar el calendario. ' + (e.message || ''));
    }
  }
  function _setDateBounds(){
    const max = new Date(Date.now() + 31*24*3600*1000).toISOString().slice(0,10);
    DATE_IN.max = max;
  }
  DATE_IN.addEventListener('change', () => _loadDia(DATE_IN.value));
  document.getElementById('icdCalPrev').addEventListener('click', () => {
    if (!DATE_IN.value) return;
    DATE_IN.value = _addDays(DATE_IN.value, -1);
    _loadDia(DATE_IN.value);
  });
  document.getElementById('icdCalNext').addEventListener('click', () => {
    if (!DATE_IN.value) return;
    DATE_IN.value = _addDays(DATE_IN.value, 1);
    _loadDia(DATE_IN.value);
  });
  document.getElementById('icdCalToday').addEventListener('click', () => {
    const tomorrow = new Date(Date.now() + 24*3600*1000).toISOString().slice(0,10);
    DATE_IN.value = tomorrow;
    _loadDia(tomorrow);
  });
  document.getElementById('icdCalRefresh').addEventListener('click', () => {
    if (DATE_IN.value) _loadDia(DATE_IN.value);
  });
  _setDateBounds();
  const initial = DATE_IN.value || (new Date(Date.now() + 24*3600*1000)).toISOString().slice(0,10);
  DATE_IN.value = initial;
  _loadDia(initial);
})();

// ════════════════════════════════════════════════════════════════════
//  PASO 4 — CALENDARIO INTELIGENTE PARA PROPUESTA
// ════════════════════════════════════════════════════════════════════

// Daniel 2026-05-24: si el día actual está sin cupos libres, busca los
// próximos 3 días futuros con al menos 4 slots libres y muestra chips
// clickeables. Se llama desde onChange del calendario + en el init.
function _refreshSuggestedDays(){
  try {
    const wrap   = document.getElementById('iwSuggestedDays');
    const chips  = document.getElementById('iwSuggestedDaysChips');
    const dateIn = document.getElementById('iwProposeDate');
    const inst   = window._proposeCalInstance;
    if (!wrap || !chips || !dateIn || !inst) return;
    const st = inst._state ? inst._state() : null;
    const payload = st && st.payload;
    if (!payload || !payload.dias){
      wrap.style.display = 'none';
      return;
    }
    const fecha = dateIn.value;
    if (!fecha){
      wrap.style.display = 'none';
      return;
    }
    const dia = payload.dias[fecha];
    if (!dia){
      wrap.style.display = 'none';
      return;
    }
    // Contar slots LIBRES (no colación / no completo / no bloqueado) en el día actual
    const slotsLibres = (dia.slots || []).filter(s => {
      const st = s.estado || (s.lunch ? 'colacion' : (!s.disponible ? 'completo' : 'disponible'));
      return st === 'disponible' || st === 'ocupado';
    }).length;
    if (slotsLibres > 0){
      wrap.style.display = 'none';
      return;
    }
    // Día seleccionado SIN cupos → buscar próximos 3 días con ≥4 slots libres
    const todasFechas = Object.keys(payload.dias).sort();
    const sugeridos = [];
    for (const f of todasFechas){
      if (f <= fecha) continue;
      const d = payload.dias[f];
      if (!d || !d.disponible) continue;
      const libres = (d.slots || []).filter(s => {
        const st = s.estado || (s.lunch ? 'colacion' : (!s.disponible ? 'completo' : 'disponible'));
        return st === 'disponible' || st === 'ocupado';
      }).length;
      if (libres >= 4){
        sugeridos.push({ fecha: f, libres });
        if (sugeridos.length >= 3) break;
      }
    }
    if (sugeridos.length === 0){
      // No hay sugerencias en 30 días — mensaje empático
      chips.innerHTML = '<span style="color:#6b7280;font-size:.82rem">Sin disponibilidad en los próximos 30 días. Revisa con bodega.</span>';
      wrap.style.display = 'flex';
      return;
    }
    // Renderizar chips clickeables (paleta ILUS, pill blanco/rojo)
    chips.innerHTML = sugeridos.map(s => {
      const [yy, mm, dd] = s.fecha.split('-');
      const label = `${dd}/${mm}`;
      return `<button type="button" class="iw-suggest-chip" data-fecha="${s.fecha}" title="${s.fecha} — ${s.libres} bloques libres">
        <i class="bi bi-calendar-event"></i>${label}
        <span class="iw-suggest-libres">${s.libres} libres</span>
      </button>`;
    }).join('');
    wrap.style.display = 'flex';
    // Bind clicks
    chips.querySelectorAll('.iw-suggest-chip').forEach(btn => {
      btn.addEventListener('click', () => {
        const f = btn.dataset.fecha;
        if (!f) return;
        dateIn.value = f;
        // Trigger change para que el calendario recargue el día
        dateIn.dispatchEvent(new Event('change'));
        if (typeof ilusToast === 'function'){
          ilusToast(`Día cambiado a ${f}`, { type: 'success' });
        }
      });
    });
  } catch(_){ /* defensivo */ }
}

function _mountProposeCalendar(){
  if (window._proposeCalInstance) return;
  if (typeof window.IlusRetirosCalendar === 'undefined') return;
  const dateInp = document.getElementById('iwProposeDate');
  // Daniel 2026-05-24: por default arrancamos en LA FECHA QUE PIDIÓ EL
  // CLIENTE (si tiene). Así el operador ve sus banderas 🔵 al instante.
  // Si no, hoy mismo.
  const cardEl = document.getElementById('iwClientCard');
  const cardData = cardEl ? cardEl.dataset : null;
  if (dateInp && !dateInp.value){
    if (cardData && cardData.reqDate){
      dateInp.value = cardData.reqDate;
    } else {
      const today = new Date();
      dateInp.value = today.toISOString().slice(0,10);
    }
  }
  // Detectamos rango pedido por cliente (si la card está renderizada)
  const requestedRange = cardData ? {
    date: cardData.reqDate || '',
    time_from: cardData.reqTf || '',
    time_to: cardData.reqTt || '',
  } : null;

  window._proposeCalInstance = window.IlusRetirosCalendar.mount({
    container:      '#iwProposeGrid',
    dateInput:      '#iwProposeDate',
    summaryEl:      '#iwProposeSummary',
    quickActionsEl: '#iwProposeQuick',
    hiddenDate:     '#iwProposeHidDate',
    hiddenTimeFrom: '#iwProposeHidTf',
    hiddenTimeTo:   '#iwProposeHidTt',
    includeOwners:  true,
    // Daniel 2026-05-24: el OPERADOR manda — puede agendar hoy mismo y
    // cruzar colación si la factura es grande. El cliente público NO
    // pasa estas flags (mantiene su 24h y bloque de colación).
    allowToday:      true,
    allowCrossLunch: true,
    // Daniel 2026-05-24: bandera ★ en el slot del CURRENT request +
    // 🔵 en el rango que pidió el cliente + drag para estirar.
    currentRequestId: RETIROS_DETAIL_DATA.reqId,
    requestedRange:   requestedRange,
    // Daniel 2026-06-15: SIEMPRE un solo bloque de 30 min. Desactivamos el
    // drag y los rangos multi-bloque (shift+click / botones de duración). El
    // operador escoge UNA sola media hora — a la que el cliente llegará.
    // No auto-seleccionamos por tiempo estimado (suggestedDurationMin:30 ⇒ 1
    // bloque, sin pre-selección sorpresa).
    enableDragSelect: false,
    enableMultiBlock: false,
    suggestedDurationMin: 30,
    onChange: (sel) => {
      const quick = document.getElementById('iwProposeQuick');
      if (quick) quick.style.display = sel || (document.getElementById('iwProposeDate') || {}).value ? 'flex' : 'none';
      // Reflejar en resumen paso 5
      const fIn = document.getElementById('iwProposeHidDate');
      const tfIn = document.getElementById('iwProposeHidTf');
      const ttIn = document.getElementById('iwProposeHidTt');
      const fechaRes = document.getElementById('resumenFecha');
      if (fechaRes && fIn && fIn.value && tfIn && tfIn.value && ttIn && ttIn.value){
        fechaRes.textContent = `${fIn.value} · ${tfIn.value}–${ttIn.value}`;
      }
      // Daniel 2026-05-24: chequear sugerencias de días con cupos
      _refreshSuggestedDays();
      // Refrescar preview del mensaje (interpola fecha y hora)
      if (typeof _iwRefreshPreview === 'function') _iwRefreshPreview();
    },
  });
}

// ════════════════════════════════════════════════════════════════════
//  PASO 4 — Card "El cliente pidió" + plantillas mensaje + preview
//  Daniel 2026-05-24: el operador acepta tal cual o modifica.
// ════════════════════════════════════════════════════════════════════
const _IW_TPLS = {
  amistoso: 'Hola {nombre}, te confirmamos disponibilidad para tu retiro el {fecha} entre {hora_inicio} y {hora_fin}. Por favor confirma tocando el botón en el email. ¡Te esperamos!',
  reagendado: 'Hola {nombre}, te proponemos REAGENDAR tu retiro al {fecha} entre {hora_inicio} y {hora_fin}. Por favor confirma con un click — si no calza, contraprópone otra hora.',
  formal: 'Estimado/a {nombre}, le informamos que su retiro queda agendado para el {fecha}, entre {hora_inicio} y {hora_fin} horas. Le solicitamos confirmar disponibilidad. Saludos cordiales.',
};
const _IW_CUSTOMER_NAME = RETIROS_DETAIL_DATA.customerLabel;

function _fmtFechaCorta(iso){
  // 2026-05-25 → "lunes 25 de mayo" en español
  if (!iso) return '';
  try {
    const d = new Date(iso + 'T12:00:00');
    const dia = d.toLocaleDateString('es-CL', { weekday: 'long', day: 'numeric', month: 'long' });
    return dia;
  } catch(_){ return iso; }
}
function _iwInterpolate(tpl){
  const fIn = document.getElementById('iwProposeHidDate');
  const tfIn = document.getElementById('iwProposeHidTf');
  const ttIn = document.getElementById('iwProposeHidTt');
  const fecha = fIn && fIn.value ? _fmtFechaCorta(fIn.value) : 'el día acordado';
  const hf = tfIn && tfIn.value ? tfIn.value : '—';
  const ht = ttIn && ttIn.value ? ttIn.value : '—';
  return String(tpl || '')
    .replace(/\{nombre\}/g, _IW_CUSTOMER_NAME)
    .replace(/\{fecha\}/g, fecha)
    .replace(/\{hora_inicio\}/g, hf)
    .replace(/\{hora_fin\}/g, ht);
}
function _iwRefreshPreview(){
  const ta = document.getElementById('iwProposeMessage');
  const pv = document.getElementById('iwMsgPreview');
  if (!ta || !pv) return;
  const raw = ta.value || '';
  if (!raw.trim()){
    pv.style.display = 'none';
    return;
  }
  const interp = _iwInterpolate(raw);
  // Escape básico para HTML (defensa simple)
  const esc = (s) => String(s).replace(/[&<>"']/g, c => ({
    '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'
  }[c]));
  // Resaltar nombre y hora con clases
  let html = esc(interp);
  if (_IW_CUSTOMER_NAME){
    const escName = esc(_IW_CUSTOMER_NAME);
    html = html.split(escName).join(`<span class="iw-msg-preview-name">${escName}</span>`);
  }
  const tfIn = document.getElementById('iwProposeHidTf');
  const ttIn = document.getElementById('iwProposeHidTt');
  if (tfIn && tfIn.value && ttIn && ttIn.value){
    const t1 = esc(tfIn.value);
    const t2 = esc(ttIn.value);
    html = html.split(t1).join(`<span class="iw-msg-preview-time">${t1}</span>`);
    html = html.split(t2).join(`<span class="iw-msg-preview-time">${t2}</span>`);
  }
  pv.innerHTML = html;
  pv.style.display = 'block';
}
// Binds para plantillas y textarea
(function(){
  const ta = document.getElementById('iwProposeMessage');
  if (ta){
    ta.addEventListener('input', _iwRefreshPreview);
  }
  document.querySelectorAll('.iw-msg-template-chip').forEach(chip => {
    chip.addEventListener('click', () => {
      const key = chip.dataset.iwTpl || '';
      // Marcar activo (visual)
      document.querySelectorAll('.iw-msg-template-chip').forEach(c => c.classList.remove('is-active'));
      chip.classList.add('is-active');
      const tpl = _IW_TPLS[key] || '';
      if (ta){
        ta.value = tpl;
        _iwRefreshPreview();
      }
    });
  });
  // Cambio de fecha → refrescar preview (interpolación de {fecha})
  const dateInp = document.getElementById('iwProposeDate');
  if (dateInp){
    dateInp.addEventListener('change', () => { try { _iwRefreshPreview(); } catch(_){} });
  }
})();

// Binds para card "El cliente pidió"
(function(){
  const card = document.getElementById('iwClientCard');
  if (!card) return;
  const btnAccept = document.getElementById('iwBtnAcceptClient');
  const btnModify = document.getElementById('iwBtnModifyClient');
  // Daniel 2026-06-15: "2026-06-17" (ISO/inglés) → "17/06/2026" (día/mes/año).
  const _iwFechaDMY = (iso) => {
    const m = /^(\d{4})-(\d{2})-(\d{2})/.exec(String(iso || ''));
    return m ? (m[3] + '/' + m[2] + '/' + m[1]) : String(iso || '');
  };
  if (btnAccept){
    // Daniel 2026-06-15: "Aceptar como propuesta" envía DIRECTO la hora exacta
    // que pidió el cliente, SIN abrir el calendario. Un clic (con confirmación)
    // y el cliente recibe el correo.
    btnAccept.addEventListener('click', async () => {
      const reqDate = card.dataset.reqDate || '';
      const reqTf = card.dataset.reqTf || '';
      const reqTt = card.dataset.reqTt || '';
      if (!reqDate || !reqTf || !reqTt){
        if (typeof ilusToast === 'function') ilusToast('El cliente no envió una hora exacta. Usa "Modificar" para elegir.', { type:'warning' });
        return;
      }
      // Enviar un correo al cliente es una acción hacia afuera → confirmamos.
      // Deshabilitamos el botón durante el confirm para evitar doble-clic.
      btnAccept.disabled = true;
      let ok = true;
      if (typeof ilusConfirm === 'function'){
        ok = await ilusConfirm({
          title: 'Enviar propuesta al cliente',
          message: '¿Proponer al cliente exactamente la fecha y hora que pidió?',
          sub: _iwFechaDMY(reqDate) + ' · ' + reqTf + '–' + reqTt,
          okLabel: 'Sí, enviar propuesta', cancelLabel: 'Cancelar',
        });
      }
      if (!ok){ btnAccept.disabled = false; return; }
      // Poblar los hidden inputs con la hora EXACTA del cliente, sin tocar el
      // calendario. enviarPropuestaWizard() los lee directamente.
      const hd = document.getElementById('iwProposeHidDate');
      const ht = document.getElementById('iwProposeHidTf');
      const he = document.getElementById('iwProposeHidTt');
      const dInp = document.getElementById('iwProposeDate');
      if (hd) hd.value = reqDate;
      if (ht) ht.value = reqTf;
      if (he) he.value = reqTt;
      if (dInp) dInp.value = reqDate;
      enviarPropuestaWizard();
    });
  }
  if (btnModify){
    // Daniel 2026-06-15: "Modificar" REVELA el calendario (oculto por defecto)
    // para que el operador escoja otro bloque único. Montaje lazy.
    btnModify.addEventListener('click', () => {
      const picker = document.getElementById('iwManualPicker');
      if (picker) picker.style.display = '';
      if (typeof _mountProposeCalendar === 'function' && !window._calMounted){
        try { _mountProposeCalendar(); window._calMounted = true; } catch(_e){}
      }
      card.style.opacity = '.7';
      card.style.borderStyle = 'dashed';
      if (picker && picker.scrollIntoView){ picker.scrollIntoView({ behavior:'smooth', block:'center' }); }
      if (typeof ilusToast === 'function') ilusToast('Elige el bloque que prefieras abajo y envía la propuesta.', { type:'info' });
    });
  }
})();

// Inicial: mount si el form está visible y aún no se propuso.
// Daniel 2026-05-24: ahora basta con tener al menos UN doc asociado
// (no requiere con_saldo verificado por ERP, que a veces falla).
// ⚡ PERF (Juan Daniel 2026-06-03): retiros_calendar.js ahora carga con
// `defer` para no bloquear el parseo de la ficha (34.6KB). En este punto
// del parseo la librería todavía NO cargó, así que NO montamos aquí —
// solo fijamos la intención (booleano evaluado por Jinja). El montaje
// real ocurre en DOMContentLoaded, cuando el script diferido ya existe.
window._shouldMountProposeCal = RETIROS_DETAIL_DATA.shouldMountProposeCal;

// Re-proponer
const _reproBtn = document.getElementById('iwShowReproposeBtn');
if (_reproBtn){
  _reproBtn.addEventListener('click', async () => {
    const ok = await ilusConfirm({
      title: 'Re-proponer fecha',
      message: '¿Enviar una nueva propuesta? La anterior quedará en historial.',
      sub: 'El cliente recibirá otro email con la nueva fecha.',
      okLabel: 'Sí, re-proponer',
    });
    if (!ok) return;
    const form = document.getElementById('iwProposeForm');
    if (form) form.style.display = '';
    const summary = document.getElementById('iwStep4Summary');
    if (summary) summary.style.display = 'none';
    // Re-proponer = elegir nueva fecha → revelar el picker manual y montar.
    const picker = document.getElementById('iwManualPicker');
    if (picker) picker.style.display = '';
    _mountProposeCalendar();
    window._calMounted = true;
    if (form && form.scrollIntoView){ form.scrollIntoView({behavior:'smooth', block:'center'}); }
  });
}

// ════════════════════════════════════════════════════════════════════
//  ACEPTAR CONTRAPROPUESTA DEL CLIENTE (responsive_retiros 2026-06-09)
//  Visible solo cuando hay propuesta pending con proposed_by='cliente'.
//  POST /retiros/<rid>/aceptar-contrapropuesta → {ok,message,confirmed,redirect_url}
// ════════════════════════════════════════════════════════════════════
async function aceptarContrapropuesta(btn){
  const ok = await ilusConfirm({
    title: 'Aceptar contrapropuesta',
    message: '¿Aceptar la fecha y hora que propuso el cliente?',
    sub: 'El retiro quedará confirmado y el cliente recibirá aviso por email.',
    okLabel: 'Sí, aceptar', cancelLabel: 'Cancelar',
  });
  if (!ok) return;
  const _orig = btn ? btn.innerHTML : '';
  if (btn){
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Aceptando…';
  }
  try {
    const fd = new FormData();
    const tok = document.querySelector('#iwProposeFormEl input[name=csrf_token]') ||
                document.querySelector('input[name=csrf_token]');
    if (tok) fd.append('csrf_token', tok.value);
    const r = await fetch(`/retiros/${_RID}/aceptar-contrapropuesta`, {
      method: 'POST',
      headers: { 'X-Requested-With': 'XMLHttpRequest', 'Accept': 'application/json' },
      body: fd, credentials: 'same-origin',
    });
    const d = await r.json().catch(() => ({ ok:false, error:'Respuesta inválida del servidor.' }));
    if (!r.ok || !d.ok){
      await ilusAlert({ title:'No se pudo aceptar', message: (d && d.error) || ('Error HTTP ' + r.status), type:'error' });
      return;
    }
    ilusToast('✓ ' + (d.message || 'Contrapropuesta aceptada — retiro confirmado'), { type:'success' });
    setTimeout(() => {
      if (d.redirect_url) window.location.href = d.redirect_url;
      else window.location.reload();
    }, 900);
  } catch(err){
    await ilusAlert({ title:'Error de red', message: err.message || 'No se pudo contactar al servidor.', type:'error' });
  } finally {
    if (btn){ btn.disabled = false; btn.innerHTML = _orig; }
  }
}

// ════════════════════════════════════════════════════════════════════
//  PASO 5 — ENVIAR PROPUESTA AL CLIENTE
// ════════════════════════════════════════════════════════════════════
async function enviarPropuestaWizard(){
  // Daniel 2026-06-15: guard de re-entrada (anti doble-envío). El correo es
  // SÍNCRONO ahora → la ventana de doble-clic es mayor; sin esto el cliente
  // podría recibir DOS correos de propuesta (dos POST a /proposal).
  if (window._iwSendingProposal) return;
  const date = (document.getElementById('iwProposeHidDate') || {}).value || '';
  const tf   = (document.getElementById('iwProposeHidTf')   || {}).value || '';
  const tt   = (document.getElementById('iwProposeHidTt')   || {}).value || '';
  const reason  = (document.getElementById('iwProposeReason') || {}).value || '';
  const message = (document.getElementById('iwProposeMessage') || {}).value || '';
  const btn = document.getElementById('btnEnviarPropuesta');
  const fb = document.getElementById('iwProposeFeedback');

  if (!date || !tf || !tt){
    ilusToast('Selecciona día y bloque horario en el paso 4 antes de enviar.', { type:'warning' });
    document.getElementById('paso-4').scrollIntoView({behavior:'smooth', block:'center'});
    return;
  }

  window._iwSendingProposal = true;
  // UI loading
  btn.disabled = true;
  btn.classList.add('is-loading');
  const _orig = btn.innerHTML;
  btn.innerHTML = '<i class="bi bi-send-check"></i><span>Enviando...</span>';
  if (fb) fb.innerHTML = '';

  const t0 = performance.now();
  try {
    const fd = new FormData();
    fd.append('csrf_token', document.querySelector('#iwProposeFormEl input[name=csrf_token]').value);
    fd.append('date', date);
    fd.append('time_from', tf);
    fd.append('time_to', tt);
    fd.append('reason', reason);
    fd.append('message', message);
    const r = await fetch(`/retiros/${_RID}/proposal`, {
      method: 'POST',
      headers: { 'X-Requested-With': 'XMLHttpRequest', 'Accept': 'application/json' },
      body: fd, credentials: 'same-origin',
    });
    const d = await r.json();
    const ms = Math.round(performance.now() - t0);
    if (!r.ok || !d.ok){
      const msg = (d && d.error) || ('Error HTTP ' + r.status);
      if (fb) fb.innerHTML = `<div class="smart-hint is-danger" style="margin:0"><i class="bi bi-x-circle"></i><div>${_esc(msg)}</div></div>`;
      ilusToast(msg, { type:'error' });
      return;
    }
    // Daniel 2026-06-15: reflejar el resultado REAL del correo (el backend lo
    // envía síncrono y devuelve email_enviado). Si el correo no salió, avisamos
    // claro en vez de prometer un email que nunca llegó.
    const _emailOk = (d.email_enviado !== false);
    const _msgTxt = d.message || (_emailOk
      ? 'Propuesta enviada. El cliente recibió el correo.'
      : 'Propuesta registrada, pero el correo al cliente no salió. Revisa la llave de correo de Retiros.');
    if (fb) fb.innerHTML = `<div class="smart-hint ${_emailOk ? 'is-success' : 'is-warn'}" style="margin:0"><i class="bi bi-${_emailOk ? 'check-circle' : 'exclamation-triangle'}"></i><div>${_esc(_msgTxt)}</div></div>`;
    ilusToast((_emailOk ? '✓ ' : '⚠ ') + _msgTxt, { type: _emailOk ? 'success' : 'warning' });
    setTimeout(() => { window.location.reload(); }, _emailOk ? 1200 : 2800);
  } catch(err){
    if (fb) fb.innerHTML = `<div class="smart-hint is-danger" style="margin:0"><i class="bi bi-x-circle"></i><div>Error de red: ${_esc(err.message)}</div></div>`;
    ilusToast('Error de red: ' + err.message, { type:'error' });
  } finally {
    window._iwSendingProposal = false;
    btn.disabled = false;
    btn.classList.remove('is-loading');
    btn.innerHTML = _orig;
    // Reactivar "Aceptar como propuesta" si quedó deshabilitado (en fallo; en
    // éxito la página recarga de todos modos).
    var _ba = document.getElementById('iwBtnAcceptClient');
    if (_ba) _ba.disabled = false;
  }
}

// ════════════════════════════════════════════════════════════════════
//  PASO 1 — EDICIÓN DE NOMBRE CLIENTE + USAR COMO CONTACTO
//  Daniel 2026-05-23: "el nombre del cliente o la razón social cambie
//  con la asignación del producto. Entonces, no te dejes llevar mucho
//  por bloquear lo que dice el cliente"
// ════════════════════════════════════════════════════════════════════
function pa1EditNombre(){
  document.getElementById('pa1NombreDisplay').style.display = 'none';
  document.getElementById('pa1NombreInput').style.display   = '';
  document.getElementById('pa1EditBtn').style.display       = 'none';
  document.getElementById('pa1SaveBtn').style.display       = '';
  document.getElementById('pa1CancelBtn').style.display     = '';
  setTimeout(()=>{document.getElementById('pa1NombreInput').focus()}, 50);
}
function pa1CancelarEdicion(){
  const orig = document.getElementById('pa1NombreDisplay').textContent.trim();
  document.getElementById('pa1NombreInput').value = orig;
  document.getElementById('pa1NombreDisplay').style.display = '';
  document.getElementById('pa1NombreInput').style.display   = 'none';
  document.getElementById('pa1EditBtn').style.display       = '';
  document.getElementById('pa1SaveBtn').style.display       = 'none';
  document.getElementById('pa1CancelBtn').style.display     = 'none';
}
async function pa1GuardarNombre(){
  const inp = document.getElementById('pa1NombreInput');
  const nuevo = (inp.value || '').trim();
  if (!nuevo){
    ilusToast('El nombre no puede quedar vacío', { type:'warning' });
    return;
  }
  const orig = document.getElementById('pa1NombreDisplay').textContent.trim();
  if (nuevo === orig){
    pa1CancelarEdicion();
    return;
  }
  const btn = document.getElementById('pa1SaveBtn');
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner-border spinner-border-sm"></span>';
  try {
    const d = await _fetchJsonSafe(`/retiros/${_RID}/customer`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ customer_name: nuevo }),
    });
    if (!d.ok){
      ilusToast('Error: ' + (d.error || 'No se pudo guardar'), { type:'error' });
      btn.disabled = false;
      btn.innerHTML = '<i class="bi bi-check-lg"></i><span>Guardar</span>';
      return;
    }
    // Actualizar display + esconder inputs
    document.getElementById('pa1NombreDisplay').textContent = d.customer_name || nuevo;
    document.getElementById('pa1NombreDisplay').style.display = '';
    document.getElementById('pa1NombreInput').style.display   = 'none';
    document.getElementById('pa1EditBtn').style.display       = '';
    btn.style.display = 'none';
    document.getElementById('pa1CancelBtn').style.display = 'none';
    ilusToast('Razón social actualizada', { type:'success' });
  } catch(e){
    ilusToast('Error de red: ' + e.message, { type:'error' });
  } finally {
    btn.disabled = false;
    btn.innerHTML = '<i class="bi bi-check-lg"></i><span>Guardar</span>';
  }
}
async function pa1UsarClienteComoContacto(checked){
  if (!checked) return;
  const ok = await ilusConfirm({
    title: 'Usar datos del cliente como contacto',
    message: '¿Reemplazar la persona que retira con los datos del cliente?',
    sub: 'El dueño del documento queda como contacto oficial. Esto se registra en el historial.',
    okLabel: 'Sí, copiar', cancelLabel: 'No, cancelar',
  });
  if (!ok){
    document.getElementById('pa1UsarComoContacto').checked = false;
    return;
  }
  try {
    const d = await _fetchJsonSafe(`/retiros/${_RID}/customer`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ usar_cliente_como_contacto: true }),
    });
    if (!d.ok){
      ilusToast('Error: ' + (d.error || 'No se pudo actualizar'), { type:'error' });
      document.getElementById('pa1UsarComoContacto').checked = false;
      return;
    }
    // Refrescar UI con los nuevos datos del contacto
    const nm = document.getElementById('pa1RetiraNombre');
    const rr = document.getElementById('pa1RetiraRut');
    if (nm) nm.textContent = d.pickup_person_name || '';
    if (rr){
      const oldText = rr.textContent || '';
      const phoneMatch = oldText.split('·')[1] || '';
      rr.textContent = (d.pickup_person_rut || '') + ' · ' + (phoneMatch.trim());
    }
    ilusToast('Datos del cliente aplicados como contacto', { type:'success' });
  } catch(e){
    ilusToast('Error de red: ' + e.message, { type:'error' });
    document.getElementById('pa1UsarComoContacto').checked = false;
  }
}

// ════════════════════════════════════════════════════════════════════
//  ✦✦ MODAL BÚSQUEDA AVANZADA — 2 motores estilo mantenciones ✦✦
//  Daniel 2026-05-23
//  Reusa el endpoint /retiros/api/buscar-erp que detecta auto el modo
//  (RUT, número doc, nombre). Para "Por documento" usamos /api/erp/documento
//  para traer las líneas del header.
// ════════════════════════════════════════════════════════════════════
const _RBA = {
  open: false,
  tab:  'doc',          // 'doc' | 'cli'
  selDoc: new Map(),    // sku -> {sku, nombre, qty, saldo, doc_tido, doc_nudo}
  selCli: new Map(),    // key=`${tipo}|${nudo}` -> {tido, nudo, sku, nombre, qty, saldo}
  loaded: { docHeader: null, cliQuery: null },
  _liveTimer: null,
};

// 🔧 BUG FIX Daniel 2026-05-24: helper que deja el modal en estado LIMPIO.
// Se llama tanto al abrir (rbaOpen) como al cerrar (rbaClose). Antes el
// modal heredaba la data del documento anterior: número de doc tipeado,
// selección de cliente, cards expandidas, contador desactualizado, etc.
// Ahora cada apertura es 100% fresca.
function _rbaResetEstado(){
  // 1. Estado interno
  _RBA.selDoc.clear();
  _RBA.selCli.clear();
  _RBA.loaded.docHeader = null;
  _RBA.loaded.cliQuery = null;
  _RBA.tab = 'doc';  // siempre arrancar en tab "Por documento"

  // 2. Inputs
  const inpNudo = document.getElementById('rbaDocNudo');
  if (inpNudo) inpNudo.value = '';
  const inpCli  = document.getElementById('rbaCliQ');
  if (inpCli)   inpCli.value = '';
  // Reset al primer tipo de doc (FCV por default)
  const selTido = document.getElementById('rbaDocTido');
  if (selTido)  selTido.selectedIndex = 0;

  // 3. Resultados — empty states por default
  const docRes = document.getElementById('rbaDocResult');
  if (docRes){
    docRes.innerHTML = `<div class="rba-empty">
      <i class="bi bi-receipt rba-empty-icon"></i>
      <h6>Buscar productos de un documento</h6>
      <p>Ingresa tipo + número arriba y te mostramos sus líneas con saldo disponible.</p>
    </div>`;
  }
  const cliRes = document.getElementById('rbaCliResult');
  if (cliRes){
    cliRes.innerHTML = `<div class="rba-empty">
      <i class="bi bi-person-vcard rba-empty-icon"></i>
      <h6>Buscar todos los documentos de un cliente</h6>
      <p>Útil cuando el cliente trae varias facturas a la vez. Mostramos todos sus docs ERP en una sola lista.</p>
    </div>`;
  }

  // 4. Tabs: forzar volver al tab "doc" visualmente
  const tabDoc = document.getElementById('rbaTabDoc');
  const tabCli = document.getElementById('rbaTabCli');
  const panelDoc = document.getElementById('rbaPanelDoc');
  const panelCli = document.getElementById('rbaPanelCli');
  if (tabDoc)   tabDoc.classList.add('is-active');
  if (tabCli)   tabCli.classList.remove('is-active');
  if (panelDoc) panelDoc.style.display = '';
  if (panelCli) panelCli.style.display = 'none';

  // 5. Cancelar timer de loading text si quedó andando
  if (_RBA._liveTimer){
    clearInterval(_RBA._liveTimer);
    _RBA._liveTimer = null;
  }

  // 6. Contador + botón asociar
  try { rbaUpdateCounter(); } catch(_){}
}

function rbaOpen(){
  // 🔧 BUG FIX Daniel 2026-05-24: SIEMPRE resetear estado ANTES de abrir.
  // Antes el segundo "abrir" mostraba: número del doc anterior, selección
  // anterior persistente, cards expandidas del cliente anterior, etc.
  // Daniel: "lo abro, uso un documento y lo vuelvo a abrir y está sucio".
  _rbaResetEstado();

  _RBA.open = true;
  const bd = document.getElementById('rbaBackdrop');
  const md = document.getElementById('rbaModal');
  bd.classList.add('is-open');
  md.classList.add('is-open');
  bd.setAttribute('aria-hidden', 'false');
  md.setAttribute('aria-hidden', 'false');
  document.body.style.overflow = 'hidden';

  // Focus input según tab activo + pre-poblar RUT sin DV
  // FIX Daniel 2026-05-23: el RUT del cliente actual (ej. "25.547.065-5")
  // se pre-pobla en el tab "Por RUT/nombre" SIN dígito verificador ni puntos
  // (ej. "25547065"), editable, para arrancar la búsqueda más rápido.
  setTimeout(()=>{
    if (_RBA.tab === 'doc'){
      const i = document.getElementById('rbaDocNudo');
      if (i) i.focus();
    } else {
      const i = document.getElementById('rbaCliQ');
      if (i){
        // Pre-poblar SOLO si está vacío (no sobrescribir si el operador ya escribió)
        if (!i.value && _RUT_CLI){
          const clean = String(_RUT_CLI).replace(/[.\-\s]/g, '');
          // Si >= 8 chars asumimos que el último es DV y lo quitamos
          const rutBase = clean.length >= 8 ? clean.slice(0, -1) : clean;
          i.value = rutBase;
          // Auto-buscar al abrir (sin esperar Enter)
          setTimeout(()=> rbaBuscarPorCliente(), 100);
        }
        i.focus();
        i.select();
      }
    }
  }, 350);
}
function rbaClose(){
  _RBA.open = false;
  // 🔧 BUG FIX Daniel 2026-05-24: forzar blur del elemento activo ANTES de
  // ocultar el modal. Si el operador tenía abierto el <select> nativo de
  // tipo de doc (FCV/BLV/...) y cierra con backdrop, el menú nativo del
  // navegador podía quedar "huérfano" flotando sobre el resto de la página.
  try {
    if (document.activeElement && typeof document.activeElement.blur === 'function'){
      document.activeElement.blur();
    }
  } catch(_){}
  document.getElementById('rbaBackdrop').classList.remove('is-open');
  document.getElementById('rbaModal').classList.remove('is-open');
  document.getElementById('rbaBackdrop').setAttribute('aria-hidden', 'true');
  document.getElementById('rbaModal').setAttribute('aria-hidden', 'true');
  document.body.style.overflow = '';
  // Limpiar timer de loading text si quedó andando (defensa)
  if (_RBA._liveTimer){
    clearInterval(_RBA._liveTimer);
    _RBA._liveTimer = null;
  }
}
// ESC para cerrar
document.addEventListener('keydown', e => {
  if (e.key === 'Escape' && _RBA.open) rbaClose();
});

function rbaSetTab(tab){
  _RBA.tab = tab;
  document.getElementById('rbaTabDoc').classList.toggle('is-active', tab === 'doc');
  document.getElementById('rbaTabCli').classList.toggle('is-active', tab === 'cli');
  document.getElementById('rbaPanelDoc').style.display = tab === 'doc' ? '' : 'none';
  document.getElementById('rbaPanelCli').style.display = tab === 'cli' ? '' : 'none';
}

// Skeleton premium para resultados
function _rbaSkel(){
  return `<div class="rba-skel">
    <div class="rba-skel-row"></div>
    <div class="rba-skel-row"></div>
    <div class="rba-skel-row"></div>
    <div class="rba-skel-row"></div>
  </div>`;
}
// Loading text con texto cambiante (3 fases)
function _rbaLoadingText(){
  return `<div class="rba-loading-text">
    <span id="rbaLoadingMsg">Consultando ERP Random</span>
    <span class="dot1"></span><span class="dot2"></span><span class="dot3"></span>
  </div>`;
}
function _rbaCycleLoadingMsg(){
  const msgs = ['Consultando ERP Random', 'Calculando saldos por línea', 'Verificando documentos ya asociados'];
  let i = 0;
  if (_RBA._liveTimer) clearInterval(_RBA._liveTimer);
  _RBA._liveTimer = setInterval(()=>{
    const el = document.getElementById('rbaLoadingMsg');
    if (!el || !_RBA.open){
      clearInterval(_RBA._liveTimer);
      _RBA._liveTimer = null;
      return;
    }
    i = (i + 1) % msgs.length;
    el.style.opacity = 0;
    setTimeout(()=>{
      if (el) {el.textContent = msgs[i]; el.style.opacity = 1;}
    }, 200);
  }, 1400);
}
function _rbaStopLoadingMsg(){
  if (_RBA._liveTimer){clearInterval(_RBA._liveTimer); _RBA._liveTimer = null;}
}

// ──────────────────────────────────────────────────────────────
// TAB "POR DOCUMENTO": busca líneas de un TIDO+NUDO específico
// ──────────────────────────────────────────────────────────────
async function rbaBuscarPorDoc(){
  const tido = (document.getElementById('rbaDocTido').value || '').toUpperCase();
  const nudo = (document.getElementById('rbaDocNudo').value || '').trim();
  if (!tido || !nudo){
    ilusToast('Ingresa tipo y número', { type:'warning' });
    return;
  }
  const cont = document.getElementById('rbaDocResult');
  const btn  = document.getElementById('rbaDocBtn');
  if (btn){ btn.disabled = true; btn.innerHTML = '<span class="spinner-border spinner-border-sm"></span><span>Buscando…</span>'; }
  cont.innerHTML = _rbaSkel() + _rbaLoadingText();
  _rbaCycleLoadingMsg();
  // FIX Daniel 2026-05-24: limpiar selección del tab "Por documento" al re-buscar
  // (antes el Map arrastraba basura de búsquedas previas → contador desincronizado).
  _RBA.selDoc.clear();
  rbaUpdateCounter();
  console.log('[rba] buscar por doc:', tido, nudo, '— selDoc limpiado');
  const t0 = performance.now();
  try {
    const r = await fetch('/api/erp/documento', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ tido, nudo })
    });
    const d = await r.json();
    if (!r.ok || d.error){
      cont.innerHTML = `<div class="rba-empty">
        <i class="bi bi-exclamation-triangle rba-empty-icon" style="color:#f59e0b"></i>
        <h6>${_esc(d.error || 'No se encontró el documento')}</h6>
        <p>Verifica que el tipo y número sean correctos.</p>
      </div>`;
      return;
    }
    const lineas = (d.lineas || []).filter(l => !l.es_zz);
    if (!lineas.length){
      cont.innerHTML = `<div class="rba-empty">
        <i class="bi bi-info-circle rba-empty-icon"></i>
        <h6>Documento sin productos</h6>
        <p>El documento solo tiene servicios o fletes — no hay líneas físicas que retirar.</p>
      </div>`;
      return;
    }
    _RBA.loaded.docHeader = {
      tido: d.tido || tido,
      nudo: d.nudo || nudo,
      cliente: d.cliente || d.razon_social || '',
      rut: d.rut || '',
      fecha: d.fecha || ''
    };
    const t1 = Math.round(performance.now() - t0);
    rbaRenderDocLineas(lineas, t1);
  } catch(e){
    cont.innerHTML = `<div class="rba-empty">
      <i class="bi bi-x-circle rba-empty-icon" style="color:#dc2626"></i>
      <h6>Error de red</h6>
      <p>${_esc(e.message)}</p>
    </div>`;
  } finally {
    if (btn){ btn.disabled = false; btn.innerHTML = '<i class="bi bi-search"></i><span>Buscar líneas</span>'; }
    _rbaStopLoadingMsg();
  }
}
function rbaRenderDocLineas(lineas, fetchMs){
  const cont = document.getElementById('rbaDocResult');
  const h = _RBA.loaded.docHeader || {};
  let html = `<div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px;margin-bottom:8px;font-size:.82rem">
    <div>
      <strong style="font-family:monospace;color:var(--ilus-red)">${_esc(h.tido)} ${_esc(h.nudo)}</strong>
      <span style="color:#6b7280;margin-left:8px">${_esc(h.cliente || '—')}</span>
      <span style="color:#9ca3af;margin-left:6px;font-size:.78rem">${_esc(h.fecha || '')}</span>
    </div>
    <span style="color:#9ca3af;font-size:.72rem">${lineas.length} línea${lineas.length===1?'':'s'} · ${fetchMs} ms</span>
  </div>`;
  // Master selector
  html += `<div class="rba-master-bar">
    <label>
      <span class="rba-chk" id="rbaMasterChkDoc"><input type="checkbox" onchange="rbaToggleAllDoc(this.checked)"><span class="rba-chk-mark">✓</span></span>
      Seleccionar todas las líneas con saldo
    </label>
    <span class="rba-master-info">${lineas.filter(l => _rbaSaldoLinea(l) > 0).length} con saldo · ${lineas.filter(l => _rbaSaldoLinea(l) === 0).length} sin saldo</span>
  </div>`;
  html += '<div>';
  lineas.forEach((l, i) => {
    const sku = (l.sku || '').toUpperCase();
    const nom = l.descripcion_erp || l.nombre_app || l.nombre || '';
    const qty = parseFloat(l.cantidad) || 0;
    const saldo = _rbaSaldoLinea(l);
    const isZero = saldo <= 0;
    // FIX 2026-08-03: no pre-marcar tampoco las que no tienen stock en la
    // bodega 02 -- mismo criterio que ya aplica para "sin saldo". No se
    // deshabilita el checkbox: el operador puede marcarla a mano si de
    // verdad va a retirar de otra bodega.
    // 2026-08-17: mismo criterio que _rbaStockEstado -- rojo solo si de
    // verdad no hay nada físico, ámbar si hay algo físico pero comprometido.
    // La selección sigue igual de cautelosa que antes en los dos casos
    // (decisión manual a propósito, ninguno viene pre-marcado).
    const stockEstado = _rbaStockEstado(l);
    const sinStockBod = stockEstado !== '';
    const rowStockClass = stockEstado === 'sin' ? 'is-no-stock'
                         : stockEstado === 'comprometido' ? 'is-stock-comprometido' : '';
    const lineKey = `DOC|${sku}|${i}`;
    // Daniel 2026-05-24: PRE-MARCAR las líneas con saldo automáticamente
    // + permitir marcar manualmente las sin saldo (con aviso amable).
    // El checkbox ya NO está disabled — el modal de aviso se dispara
    // en el toggle handler si el saldo es cero.
    const initialMax = isZero ? qty : saldo;   // permitir cantidad histórica si no hay saldo
    const initialQty = isZero ? 0 : saldo;     // por default no llevarse nada de las sin saldo
    const preChecked = !isZero && !sinStockBod; // marcadas: solo las con saldo Y con stock en bod. 02
    html += `<div class="rba-line ${isZero?'is-zero':''} ${rowStockClass} ${preChecked?'is-selected':''}" data-key="${_esc(lineKey)}" data-sku="${_esc(sku)}" style="animation:rbaDocIn .35s var(--ease-spring) ${i*30}ms both">
      <label class="rba-chk ${preChecked?'is-checked':''}" data-sku="${_esc(sku)}" data-saldo="${saldo}">
        <input type="checkbox" ${preChecked?'checked':''} data-line-key="${_esc(lineKey)}" data-sku="${_esc(sku)}" data-nombre="${_esc(nom)}" data-qty="${saldo}" data-cantidad-doc="${qty}" data-is-zero="${isZero?1:0}" data-sin-stock="${sinStockBod?1:0}" onchange="rbaToggleLineaDoc(this)">
        <span class="rba-chk-mark">✓</span>
      </label>
      <div class="ln-sku">${_esc(sku) || '—'}</div>
      <div class="ln-desc">
        ${_esc(nom)}
        ${isZero ? '<span class="rba-line-tag-zero" title="Esta línea ya fue rebajada del sistema (ya entregada). Puedes marcarla igualmente si necesitas, pero por defecto no se asocia."><i class="bi bi-check-circle-fill"></i> entregado</span>' : ''}
      </div>
      <div class="ln-qty">
        <span>${qty} u.</span>
        <span class="${isZero?'ln-qty-saldo-zero':'ln-qty-saldo'}">${isZero?'sin saldo':`saldo ${saldo}`}</span>
        ${_rbaStockBadge(l)}
      </div>
      <input class="ln-qty-input" type="number" min="0" max="${initialMax}" value="${initialQty}" data-line-key="${_esc(lineKey)}" data-max-saldo="${saldo}" data-max-doc="${qty}" onchange="rbaCambiarQtyDoc(this)">
    </div>`;
  });
  html += '</div>';
  cont.innerHTML = html;
  // Daniel 2026-05-24: auto-poblar _RBA.selDoc con las líneas pre-marcadas
  // (las que tienen saldo) para que el contador y "Asociar al retiro"
  // funcionen sin requerir click manual del operador.
  // IMPORTANTE: silent=true para NO disparar toast de "ya fue rebajado"
  // (esos toasts solo deben salir cuando el operador marca manualmente).
  setTimeout(() => {
    document.querySelectorAll('#rbaDocResult input[type="checkbox"][data-line-key]:checked').forEach(cb => {
      rbaToggleLineaDoc(cb, /*silent=*/true);
    });
    console.log('[rba] auto-poblado: selDoc.size =', _RBA.selDoc.size);
  }, 50);
}

// Helper: extrae el saldo REAL de una línea del ERP.
// El backend ya calcula saldo = CAPRCO1 - CAPRAD1 y lo expone como
// `l.saldo`. Si por algún motivo no viene (motor REST viejo, etc.)
// caemos a cantidad - cantidad_despachada, y como última opción
// asumimos saldo = cantidad (compat con docs sin guía asociada).
function _rbaSaldoLinea(l){
  if (l == null) return 0;
  const saldoBackend = parseFloat(l.saldo);
  if (!isNaN(saldoBackend)) return Math.max(0, saldoBackend);
  const qty   = parseFloat(l.cantidad) || 0;
  const desp  = parseFloat(l.cantidad_despachada) || 0;
  return Math.max(0, qty - desp);
}

// FIX 2026-08-03 (Daniel: "el stock principal es el de la bodega 02" +
// "dale una vuelta al módulo de retiros por esa parte" -- espejo exacto de
// _tkaStockBadge en tickets/_tka_modal.html). /api/erp/documento YA trae
// `l.stock` (get_erp_stock_by_skus, físico acotado a la bodega principal --
// ver app.py) -- acá nunca se mostraba. "saldo" es cuánto falta por
// despachar de ESE documento, no si hay stock físico para retirarlo. No
// bloquea la selección -- eso lo sigue decidiendo el saldo -- solo avisa.
// FIX 2026-08-17 (Daniel, mismo pedido que en _tkaStockBadge de
// tickets/_tka_modal.html -- espejo exacto, mismo arreglo): "requiero
// mapear la información completa... si hay al menos uno físico positivo y
// cuántos están comprometidos, para tomar la decisión... yo veré si avanzo
// o no". Antes esta función colapsaba a un booleano y mostraba "Sin stock
// bod. 02" tanto si el físico era 0 como si era 1 (pero comprometido) --
// dos situaciones muy distintas para quien retira. Ahora se distingue: cero
// físico sigue en rojo ("no hay nada"); físico positivo pero comprometido
// muestra el desglose real en ámbar informativo, para decidir con el dato
// completo en vez de un "sin stock" que suena a que no hay absolutamente nada.
// '' (OK) | 'sin' (0 físico) | 'comprometido' (algo físico, ya reservado).
// Espejo exacto de _tkaStockEstado en tickets/_tka_modal.html -- único
// lugar donde se decide la distinción; el badge y las 2 filas de tabla
// (abajo) solo consultan esto.
function _rbaStockEstado(l){
  const st = l && l.stock;
  if (!st || st.hay_stock !== false) return '';
  return (parseFloat(st.fisico) || 0) <= 0 ? 'sin' : 'comprometido';
}

function _rbaStockBadge(l){
  const estado = _rbaStockEstado(l);
  if (!estado) return '';
  const st = l.stock;
  const f = (n) => (Math.round((parseFloat(n) || 0) * 10) / 10).toLocaleString('es-CL');
  const tip = 'Físico bodega 02: ' + f(st.fisico) + ' · Comprometido: ' + f(st.comprometido)
    + ' · Devengado: ' + f(st.devengado) + ' · Disponible: ' + f(st.disponible);

  if (estado === 'sin'){
    return '<span class="ln-stock-warn" title="' + _esc(tip) + '">'
      + '<i class="bi bi-exclamation-triangle-fill"></i>Sin stock bod. 02</span>';
  }

  let texto = f(st.fisico) + ' físico';
  if ((parseFloat(st.comprometido) || 0) > 0) texto += ' · ' + f(st.comprometido) + ' comp.';
  return '<span class="ln-stock-info" title="' + _esc(tip) + '">'
    + '<i class="bi bi-info-circle-fill"></i>' + _esc(texto) + '</span>';
}

function rbaToggleAllDoc(checked){
  // FIX Daniel 2026-05-24: el label dice "Seleccionar todas las líneas con
  // saldo" → debemos respetarlo. Antes marcaba TODAS (incluso las sin saldo)
  // y disparaba 5+ toasts en cascada de "ya fue rebajado". Ahora:
  //   - Al marcar (checked=true): solo toca las con saldo (isZero=0).
  //   - Al desmarcar (checked=false): desmarca TODAS (consistente con UX).
  const inputs = document.querySelectorAll('#rbaDocResult input[type="checkbox"][data-line-key]');
  let nAffected = 0;
  inputs.forEach(inp => {
    if (inp.disabled) return;
    const isZero = inp.dataset.isZero === '1';
    // FIX 2026-08-03: tampoco marcar por encima una línea sin stock en
    // bodega 02 al usar "seleccionar todas" -- mismo criterio que "sin saldo".
    const sinStockBod = inp.dataset.sinStock === '1';
    if (checked && (isZero || sinStockBod)) return;   // ← no marcar las sin saldo/sin stock al usar "todas"
    if (inp.checked === checked) return;  // ya está como queremos
    inp.checked = checked;
    rbaToggleLineaDoc(inp, /*silent=*/true);  // silent: este es un cambio masivo, no manual
    nAffected++;
  });
  console.log('[rba] toggleAll:', checked, '— líneas afectadas:', nAffected);
}
function rbaToggleLineaDoc(inp, silent){
  const chkBox = inp.closest('.rba-chk');
  const row = inp.closest('.rba-line');
  const key = inp.dataset.lineKey;
  const sku = inp.dataset.sku;
  const nombre = inp.dataset.nombre;
  const isZero = inp.dataset.isZero === '1';
  const qtyInp = document.querySelector(`#rbaDocResult input.ln-qty-input[data-line-key="${key}"]`);
  // Daniel 2026-05-24: si la línea NO tiene saldo y el operador la marca,
  // mostrar aviso amable y dejar marcado (no bloquear).
  // silent=true desactiva el toast (usado por auto-poblado post-render y toggleAll).
  if (inp.checked && isZero && !silent){
    ilusToast(`ℹ "${nombre}" ya fue rebajado del sistema (entregado). Lo agregamos igualmente.`, {
      type: 'info', duration: 4500
    });
    if (qtyInp && parseFloat(qtyInp.value || 0) === 0){
      qtyInp.value = 1;
    }
  }
  if (chkBox) chkBox.classList.toggle('is-checked', inp.checked);
  if (row) row.classList.toggle('is-selected', inp.checked);
  const qty = parseFloat((qtyInp ? qtyInp.value : inp.dataset.qty) || 0) || 0;
  const h = _RBA.loaded.docHeader || {};
  if (inp.checked){
    _RBA.selDoc.set(key, {
      sku, nombre, qty,
      saldo: parseFloat(inp.dataset.qty) || qty,
      doc_tido: h.tido, doc_nudo: h.nudo,
      // 🆕 Daniel 2026-05-24: persistir "ya rebajado en ERP" para badge
      // ámbar en tabla externa después de asociar.
      marcada_sin_saldo: isZero,
    });
    if (!silent) console.log('[rba] línea marcada: SKU=', sku, 'qty=', qty, isZero?'(sin saldo ERP)':'');
  } else {
    _RBA.selDoc.delete(key);
    if (!silent) console.log('[rba] línea desmarcada: SKU=', sku);
  }
  rbaUpdateCounter();
}
function rbaCambiarQtyDoc(inp){
  const key = inp.dataset.lineKey;
  let v = parseFloat(inp.value || 0) || 0;
  const maxDoc = parseFloat(inp.dataset.maxDoc || 0) || 0;
  const maxSaldo = parseFloat(inp.dataset.maxSaldo || 0) || 0;
  const max = Math.max(maxDoc, maxSaldo);  // permite cantidad histórica si saldo=0 pero el operador la marcó
  if (v < 0) v = 0;
  if (max > 0 && v > max) v = max;
  inp.value = v;
  const item = _RBA.selDoc.get(key);
  if (item){
    item.qty = v;
    _RBA.selDoc.set(key, item);
  }
  rbaUpdateCounter();
}

// ──────────────────────────────────────────────────────────────
// TAB "POR CLIENTE": busca docs por RUT/nombre/número
// ──────────────────────────────────────────────────────────────
async function rbaBuscarPorCliente(){
  const q = (document.getElementById('rbaCliQ').value || '').trim();
  if (q.length < 3){
    ilusToast('Mínimo 3 caracteres', { type:'warning' });
    return;
  }
  const cont = document.getElementById('rbaCliResult');
  const btn  = document.getElementById('rbaCliBtn');
  if (btn){ btn.disabled = true; btn.innerHTML = '<span class="spinner-border spinner-border-sm"></span><span>Buscando…</span>'; }
  cont.innerHTML = _rbaSkel() + _rbaLoadingText();
  _rbaCycleLoadingMsg();
  const t0 = performance.now();
  try {
    const d = await _fetchJsonSafe('/retiros/api/buscar-erp', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ q })
    });
    if (d.sin_conexion){
      cont.innerHTML = `<div class="rba-empty">
        <i class="bi bi-plug rba-empty-icon" style="color:#f59e0b"></i>
        <h6>ERP no conectado</h6>
        <p>${_esc(d.error || 'Pídele al admin que setee RANDOM_SQL_* en Google Cloud.')}</p>
      </div>`;
      return;
    }
    if (!d.ok || (d._not_json)){
      cont.innerHTML = `<div class="rba-empty">
        <i class="bi bi-x-circle rba-empty-icon" style="color:#dc2626"></i>
        <h6>Error de búsqueda</h6>
        <p>${_esc(d.error || 'No se pudo consultar el ERP')}</p>
      </div>`;
      return;
    }
    const docs = d.documentos || [];
    if (!docs.length){
      cont.innerHTML = `<div class="rba-empty">
        <i class="bi bi-search rba-empty-icon"></i>
        <h6>Sin resultados para "${_esc(q)}"</h6>
        <p>Prueba con un RUT distinto o un fragmento del nombre del cliente.</p>
      </div>`;
      return;
    }
    _RBA.loaded.cliQuery = { q, modo: d.modo, docs };
    const t1 = Math.round(performance.now() - t0);
    rbaRenderCliDocs(docs, d.modo, t1);
    // ⚡ PERF Daniel 2026-05-24: pre-warmup paralelo de los TOP 5 docs con
    // saldo (los más probables de expandir). El backend cachea cada doc
    // 5min en _ERP_DOC_CACHE → expandir es instantáneo y "Asociar" también.
    // Sin esto: cada expansión + asociación = 800-1500ms cada una.
    try { _rbaPrewarmDocs(docs); } catch(_){}
  } catch(e){
    cont.innerHTML = `<div class="rba-empty">
      <i class="bi bi-x-circle rba-empty-icon" style="color:#dc2626"></i>
      <h6>Error de red</h6>
      <p>${_esc(e.message)}</p>
    </div>`;
  } finally {
    if (btn){ btn.disabled = false; btn.innerHTML = '<i class="bi bi-search"></i><span>Buscar cliente</span>'; }
    _rbaStopLoadingMsg();
  }
}
// 🔧 PERF Daniel 2026-05-24: pre-warmup paralelo del cache de docs en
// el backend. Para los TOP 5 docs con saldo, dispara /api/erp/documento
// en paralelo (fire-and-forget). El backend cachea cada doc 5min → al
// expandir o asociar, la respuesta es <10ms en vez de ~1200ms.
//
// Por qué solo 5: si el RUT tiene 80 docs, prewarmear todos saturaría
// el ERP de Random. 5 es el promedio razonable de expansión por sesión.
function _rbaPrewarmDocs(docs){
  if (!Array.isArray(docs) || docs.length === 0) return;
  // Priorizar docs con saldo (más probables de expandir y asociar)
  const top = docs
    .filter(d => d.tiene_saldo && !d.ya_tiene_retiro)
    .slice(0, 5);
  if (!top.length) return;
  // Concurrent prewarm — fire & forget (no esperamos respuesta)
  top.forEach(doc => {
    fetch('/api/erp/documento', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ tido: doc.tido_display, nudo: doc.nudo_display })
    }).catch(()=>{ /* silencioso — solo prewarm */ });
  });
}

function rbaRenderCliDocs(docs, modo, fetchMs){
  const cont = document.getElementById('rbaCliResult');
  const modoTxt = { rut: 'RUT', numero: 'N° doc', nombre: 'Nombre' }[modo] || modo;
  const _conSaldo = docs.filter(d => d.tiene_saldo && !d.ya_tiene_retiro).length;
  let html = `<div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px;margin-bottom:10px;font-size:.82rem">
    <div><strong>${docs.length}</strong> documento${docs.length===1?'':'s'} por <strong>${_esc(modoTxt)}</strong></div>
    <span style="color:#9ca3af;font-size:.72rem">${fetchMs} ms</span>
  </div>`;
  // Daniel 2026-06-16: un click para asociar TODAS las facturas con saldo del
  // cliente al retiro (omite las que ya están en otro retiro).
  if (_conSaldo > 0){
    html += `<div style="margin-bottom:10px">
      <button type="button" id="rbaAsocTodasBtn" class="btn btn-sm fw-bold"
        style="background:#16a34a;color:#fff;border:none;border-radius:10px;padding:8px 14px"
        onclick="rbaAsociarTodasConSaldo()">
        <i class="bi bi-lightning-charge-fill me-1"></i>Asociar las ${_conSaldo} factura${_conSaldo===1?'':'s'} con saldo a este retiro
      </button>
    </div>`;
  }
  html += '<div class="rba-results">';
  docs.forEach((doc, i) => {
    const clsArr = [];
    if (doc.ya_tiene_retiro) clsArr.push('is-already');
    if (!doc.tiene_saldo) clsArr.push('is-no-saldo');
    const fecha = doc.fecha || '';
    const total = doc.valor_total ? '$' + Math.round(doc.valor_total).toLocaleString('es-CL') : '—';
    const saldoBadge = doc.tiene_saldo
      ? '<span class="badge-pill ok"><i class="bi bi-check-circle"></i>Con saldo</span>'
      : '<span class="badge-pill warn"><i class="bi bi-exclamation-triangle"></i>Sin saldo</span>';
    const alreadyBadge = doc.ya_tiene_retiro
      ? '<span class="badge-pill lock"><i class="bi bi-shield-lock"></i>En otro retiro</span>'
      : '';
    html += `<div class="rba-doc-card ${clsArr.join(' ')}" data-cli-idx="${i}" style="animation-delay:${i*40}ms">
      <div class="rba-doc-head" onclick="rbaToggleDocCli(${i})">
        <div class="rba-doc-num">
          <span class="badge-tipo">${_esc(doc.tido_display)}</span>${_esc(doc.nudo_display)}
        </div>
        <div class="rba-doc-info">
          <div class="rd-cli">${_esc(doc.razon_social || '—')}</div>
          <div class="rd-meta">
            <span class="rd-rut">${_esc(doc.rut || '—')}</span>
            ${fecha ? `<span><i class="bi bi-calendar3 me-1"></i>${_esc(fecha)}</span>` : ''}
          </div>
        </div>
        <div class="rba-doc-totals">
          ${saldoBadge}
          ${alreadyBadge}
          <span class="badge-pill">${doc.n_lineas || 0} líneas</span>
          <span class="badge-pill" title="Total bruto">${total}</span>
        </div>
        <div class="rba-doc-actions">
          <button class="rba-doc-toggle btn-2027" onclick="event.stopPropagation();rbaToggleDocCli(${i})" aria-label="Ver productos">
            <i class="bi bi-chevron-down"></i>
          </button>
        </div>
      </div>
      <div class="rba-doc-body" id="rbaCliBody-${i}">
        <div class="rba-loading-text">Cargando líneas <span class="dot1"></span><span class="dot2"></span><span class="dot3"></span></div>
      </div>
    </div>`;
  });
  html += '</div>';
  cont.innerHTML = html;
}
async function rbaToggleDocCli(idx){
  const card = document.querySelector(`#rbaCliResult .rba-doc-card[data-cli-idx="${idx}"]`);
  if (!card) return;
  const isOpen = card.classList.toggle('is-open');
  if (!isOpen) return;
  const body = document.getElementById('rbaCliBody-' + idx);
  if (body.dataset.loaded === '1') return;
  const doc = ((_RBA.loaded.cliQuery && _RBA.loaded.cliQuery.docs) || [])[idx];
  if (!doc){ body.innerHTML = '<div class="text-muted small">No se encontró el documento.</div>'; return; }
  body.innerHTML = _rbaSkel();
  try {
    const r = await fetch('/api/erp/documento', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ tido: doc.tido_display, nudo: doc.nudo_display })
    });
    const dd = await r.json();
    if (!r.ok || dd.error){
      body.innerHTML = `<div class="text-muted small py-2"><i class="bi bi-x-circle me-1"></i>${_esc(dd.error || 'Error consultando ERP')}</div>`;
      return;
    }
    const lineas = (dd.lineas || []).filter(l => !l.es_zz);
    if (!lineas.length){
      body.innerHTML = '<div class="text-muted small py-2"><i class="bi bi-info-circle me-1"></i>Sin productos (solo servicios).</div>';
      body.dataset.loaded = '1';
      return;
    }
    // Botón para asociar el DOC ENTERO directo (atajo)
    let html = `<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;font-size:.78rem;color:#6b7280">
      <span>${lineas.length} producto${lineas.length===1?'':'s'} disponibles · selecciona los que vienen a retirar</span>
      ${doc.ya_tiene_retiro
        ? '<span class="badge-pill lock"><i class="bi bi-shield-lock me-1"></i>Doc en otro retiro</span>'
        : `<button class="rba-doc-add-btn btn-2027" onclick="rbaAsociarDocCompleto(${idx})">
            <i class="bi bi-plus-lg"></i>Asociar doc completo
          </button>`}
    </div>`;
    lineas.forEach((l, j) => {
      const sku = (l.sku || '').toUpperCase();
      const nom = l.descripcion_erp || l.nombre_app || l.nombre || '';
      const qty = parseFloat(l.cantidad) || 0;
      const saldo = _rbaSaldoLinea(l);
      const isZero = saldo <= 0;
      const lineKey = `CLI|${idx}|${sku}|${j}`;
      // Ver comentario equivalente en rbaRenderDocLineas (mismo criterio,
      // mismo motivo: Daniel, "dale una vuelta al módulo de retiros").
      const stockEstado = _rbaStockEstado(l);
      const sinStockBod = stockEstado !== '';
      const rowStockClass = stockEstado === 'sin' ? 'is-no-stock'
                           : stockEstado === 'comprometido' ? 'is-stock-comprometido' : '';
      // Daniel 2026-05-24: pre-marcar las que tienen saldo, permitir
      // marcar las sin saldo (con aviso amable).
      const initialMax = isZero ? qty : saldo;
      const initialQty = isZero ? 0 : saldo;
      const preChecked = !isZero && !sinStockBod;
      html += `<div class="rba-line ${isZero?'is-zero':''} ${rowStockClass} ${preChecked?'is-selected':''}" data-key="${_esc(lineKey)}">
        <label class="rba-chk ${preChecked?'is-checked':''}">
          <input type="checkbox" ${preChecked?'checked':''} data-line-key="${_esc(lineKey)}" data-doc-tido="${_esc(doc.tido_display)}" data-doc-nudo="${_esc(doc.nudo_display)}" data-sku="${_esc(sku)}" data-nombre="${_esc(nom)}" data-qty="${saldo}" data-cantidad-doc="${qty}" data-is-zero="${isZero?1:0}" data-sin-stock="${sinStockBod?1:0}" onchange="rbaToggleLineaCli(this)">
          <span class="rba-chk-mark">✓</span>
        </label>
        <div class="ln-sku">${_esc(sku) || '—'}</div>
        <div class="ln-desc">
          ${_esc(nom)}
          ${isZero ? '<span class="rba-line-tag-zero" title="Esta línea ya fue rebajada del sistema (entregada). Puedes marcarla igualmente si necesitas."><i class="bi bi-check-circle-fill"></i> entregado</span>' : ''}
        </div>
        <div class="ln-qty">
          <span>${qty} u.</span>
          <span class="${isZero?'ln-qty-saldo-zero':'ln-qty-saldo'}">${isZero?'sin saldo':`saldo ${saldo}`}</span>
          ${_rbaStockBadge(l)}
        </div>
        <input class="ln-qty-input" type="number" min="0" max="${initialMax}" value="${initialQty}" data-line-key="${_esc(lineKey)}" data-max-saldo="${saldo}" data-max-doc="${qty}" onchange="rbaCambiarQtyCli(this)">
      </div>`;
    });
    body.innerHTML = html;
    body.dataset.loaded = '1';
    // Auto-poblar _RBA.selCli con las líneas pre-marcadas
    // silent=true para evitar avalancha de toasts en docs con saldo cero
    setTimeout(() => {
      body.querySelectorAll('input[type="checkbox"][data-line-key]:checked').forEach(cb => {
        rbaToggleLineaCli(cb, /*silent=*/true);
      });
    }, 50);
  } catch(e){
    body.innerHTML = `<div class="text-muted small py-2"><i class="bi bi-x-circle me-1"></i>Error: ${_esc(e.message)}</div>`;
  }
}
function rbaToggleLineaCli(inp, silent){
  const chkBox = inp.closest('.rba-chk');
  if (chkBox) chkBox.classList.toggle('is-checked', inp.checked);
  const row = inp.closest('.rba-line');
  if (row) row.classList.toggle('is-selected', inp.checked);
  const key = inp.dataset.lineKey;
  const isZero = inp.dataset.isZero === '1';
  const nombre = inp.dataset.nombre;
  // Daniel 2026-05-24: aviso amable si marca una línea sin saldo (entregada)
  // silent=true desactiva el toast (usado por auto-poblado post-render)
  if (inp.checked && isZero && !silent){
    ilusToast(`ℹ "${nombre}" ya fue rebajado del sistema (entregado). Lo agregamos igualmente.`, {
      type: 'info', duration: 4500
    });
    const qtyInpZ = document.querySelector(`input.ln-qty-input[data-line-key="${key}"]`);
    if (qtyInpZ && parseFloat(qtyInpZ.value || 0) === 0) qtyInpZ.value = 1;
  }
  // qty actual: tomar del input si existe (puede haberse editado)
  const qtyInp = document.querySelector(`input.ln-qty-input[data-line-key="${key}"]`);
  const qtyEff = qtyInp ? (parseFloat(qtyInp.value || 0) || 0) : (parseFloat(inp.dataset.qty) || 0);
  if (inp.checked){
    _RBA.selCli.set(key, {
      tido:   inp.dataset.docTido,
      nudo:   inp.dataset.docNudo,
      sku:    inp.dataset.sku,
      nombre: nombre,
      qty:    qtyEff,
      saldo:  parseFloat(inp.dataset.qty) || qtyEff,
      // 🆕 Daniel 2026-05-24: persistir "ya rebajado en ERP" para badge
      // ámbar en tabla externa después de asociar.
      marcada_sin_saldo: isZero,
    });
  } else {
    _RBA.selCli.delete(key);
  }
  rbaUpdateCounter();
}
function rbaCambiarQtyCli(inp){
  const key = inp.dataset.lineKey;
  const item = _RBA.selCli.get(key);
  if (!item) return;
  let v = parseFloat(inp.value || 0) || 0;
  if (v < 0) v = 0;
  if (v > item.saldo) v = item.saldo;
  inp.value = v;
  item.qty = v;
  _RBA.selCli.set(key, item);
  rbaUpdateCounter();
}
async function rbaAsociarDocCompleto(idx){
  const doc = ((_RBA.loaded.cliQuery && _RBA.loaded.cliQuery.docs) || [])[idx];
  if (!doc) return;
  // Atajo: usar el endpoint existente que asocia el doc entero
  try {
    const d = await _fetchJsonSafe(`/retiros/${_RID}/docs/agregar`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ document_type: doc.tido_display, document_number: doc.nudo_display })
    });
    if (d._http_status === 409){
      ilusToast(`Ya estaba en este retiro`, { type:'warning' });
      return;
    }
    if (!d.ok){
      ilusToast('Error: ' + (d.error || 'No se pudo agregar'), { type:'error' });
      return;
    }
    ilusToast(`✓ ${doc.tido_display} ${doc.nudo_display} agregado`, { type:'success' });
    await refrescarDocsAsociados(_RID);
    // Refrescar también sugerencias para mostrar nuevo estado
    cargarSaldoCliente(_RID);
  } catch(e){
    ilusToast('Error: ' + e.message, { type:'error' });
  }
}

// Daniel 2026-06-16: asocia de un click TODAS las facturas con saldo del cliente
// al retiro. Omite las que ya están en otro retiro (409). Muestra progreso.
async function rbaAsociarTodasConSaldo(){
  const docs = (_RBA.loaded.cliQuery && _RBA.loaded.cliQuery.docs) || [];
  const objetivo = docs.filter(d => d.tiene_saldo && !d.ya_tiene_retiro);
  if (!objetivo.length){ ilusToast('No hay facturas con saldo para asociar.', { type:'info' }); return; }
  const ok = await ilusConfirm({
    title: 'Asociar facturas con saldo',
    message: `¿Asociar las ${objetivo.length} factura(s) con saldo de este cliente a este retiro?`,
    sub: 'Las que ya están en otro retiro se omiten automáticamente.',
    okLabel: 'Sí, asociar todas', cancelLabel: 'Cancelar',
  });
  if (!ok) return;
  const btn = document.getElementById('rbaAsocTodasBtn');
  const _orig = btn ? btn.innerHTML : '';
  if (btn) btn.disabled = true;
  let okN = 0, dupN = 0, errN = 0, i = 0;
  for (const doc of objetivo){
    i++;
    if (btn) btn.innerHTML = `<span class="spinner-border spinner-border-sm me-1"></span>Asociando ${i}/${objetivo.length}…`;
    try {
      const d = await _fetchJsonSafe(`/retiros/${_RID}/docs/agregar`, {
        method:'POST', headers:{'Content-Type':'application/json'},
        body: JSON.stringify({ document_type: doc.tido_display, document_number: doc.nudo_display })
      });
      if (d._http_status === 409) dupN++;
      else if (d.ok) okN++;
      else errN++;
    } catch(_){ errN++; }
  }
  await refrescarDocsAsociados(_RID);
  cargarSaldoCliente(_RID);
  if (btn){ btn.disabled = false; btn.innerHTML = _orig; }
  let msg = `✓ ${okN} factura(s) asociada(s)`;
  if (dupN) msg += ` · ${dupN} ya estaban`;
  if (errN) msg += ` · ${errN} con error`;
  ilusToast(msg, { type: errN ? 'warning' : 'success' });
}

// ──────────────────────────────────────────────────────────────
// Contador en vivo + bump animation
// ──────────────────────────────────────────────────────────────
function rbaUpdateCounter(){
  // FIX Daniel 2026-05-24: contar SOLO líneas con qty>0 (no las marcadas en cero).
  // Antes el footer decía "0 productos seleccionados" siempre como texto fijo
  // y el botón no se desactivaba correctamente si una línea estaba en qty=0.
  const allSel = [..._RBA.selDoc.values(), ..._RBA.selCli.values()]
    .filter(it => (it.qty || 0) > 0);
  const n = allSel.length;
  const pill = document.getElementById('rbaCounterPill');
  if (pill){
    pill.textContent = n;
    pill.classList.remove('is-bump');
    void pill.offsetWidth;       // reflow para reiniciar animación
    pill.classList.add('is-bump');
  }
  const label = document.getElementById('rbaCounterLabel');
  if (label) label.textContent = (n === 1) ? 'producto seleccionado' : 'productos seleccionados';
  const totalUnits = allSel.reduce((s, it) => s + (it.qty || 0), 0);
  const kg = document.getElementById('rbaCounterKg');
  const m3 = document.getElementById('rbaCounterM3');
  if (kg) kg.textContent = `${totalUnits.toFixed(0)} u.`;
  if (m3) m3.textContent = `${n} línea${n===1?'':'s'}`;
  const btn = document.getElementById('rbaAssocBtn');
  if (btn) btn.disabled = n === 0;
}

// ──────────────────────────────────────────────────────────────
// Asociar selección al retiro: agrupa por (tido, nudo) y agrega cada doc
// ──────────────────────────────────────────────────────────────
async function rbaAsociarSeleccion(){
  // Daniel 2026-05-24 — REWRITE PARA RESPETAR LÍNEAS SELECCIONADAS:
  // Antes: solo agregaba el documento entero (sin importar qué líneas marcó).
  // Ahora: agrupa por doc + posta líneas granulares vía /docs/<id>/lineas.
  //
  // Estructura: docsToAdd[key] = {tido, nudo, lineas: [{sku, qty, nombre}]}

  const docsToAdd = new Map();

  // Tab "Por documento": una sola key (el doc header buscado)
  if (_RBA.selDoc.size > 0 && _RBA.loaded.docHeader){
    const h = _RBA.loaded.docHeader;
    const key = `${h.tido}|${h.nudo}`;
    const lineas = [];
    _RBA.selDoc.forEach(it => {
      // Solo agregar líneas con qty > 0 (defensivo)
      if ((it.qty || 0) > 0){
        lineas.push({
          sku: it.sku, nombre: it.nombre, qty: it.qty, saldo: it.saldo,
          marcada_sin_saldo: !!it.marcada_sin_saldo,
        });
      }
    });
    if (lineas.length > 0){
      docsToAdd.set(key, { tido: h.tido, nudo: h.nudo, lineas });
    }
  }

  // Tab "Por cliente": múltiples docs posibles, cada línea sabe a qué doc va
  _RBA.selCli.forEach(it => {
    if (!it.tido || !it.nudo) return;
    if ((it.qty || 0) <= 0) return;  // defensivo
    const key = `${it.tido}|${it.nudo}`;
    if (!docsToAdd.has(key)){
      docsToAdd.set(key, { tido: it.tido, nudo: it.nudo, lineas: [] });
    }
    docsToAdd.get(key).lineas.push({
      sku: it.sku, nombre: it.nombre, qty: it.qty, saldo: it.saldo,
      marcada_sin_saldo: !!it.marcada_sin_saldo,
    });
  });

  console.log('[rba] asociando', docsToAdd.size, 'doc(s) — total líneas seleccionadas:',
    [...docsToAdd.values()].reduce((s, d) => s + d.lineas.length, 0));

  if (docsToAdd.size === 0){
    ilusToast('Selecciona al menos un producto con cantidad mayor a 0', { type:'warning' });
    return;
  }

  const btn = document.getElementById('rbaAssocBtn');
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner-border spinner-border-sm"></span><span>Asociando…</span>';

  const okList = [];     // docs asociados OK
  const dupList = [];    // docs ya asociados (409 DUPLICATE)
  const errList = [];    // docs con error real (con detalle)
  const otrosN = [];

  for (const [, info] of docsToAdd){
    const label = `${info.tido} ${info.nudo}`;
    try {
      // ⚡ PERF Daniel 2026-05-24 (8s → <1.5s): asociar + selección granular
      // en UN SOLO POST. Antes hacíamos 2 round-trips secuenciales
      // (agregar doc + guardar líneas). El backend ahora acepta `lineas`
      // OPCIONAL en el body de /docs/agregar y procesa todo en la misma
      // transacción + 1 sólo recalc_totales.
      const lineas_payload = (info.lineas || [])
        .filter(ln => (ln.qty || 0) > 0)
        .map(ln => ({
          sku: ln.sku,
          incluida: true,
          cantidad_seleccionada: ln.qty,
          // 🆕 Daniel 2026-05-24: el backend guarda esta flag por línea para
          // mostrar badge ámbar "ya rebajado en ERP" en la tabla externa.
          marcada_sin_saldo: !!ln.marcada_sin_saldo,
        }));
      const r = await fetch(`/retiros/${_RID}/docs/agregar`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Requested-With': 'XMLHttpRequest',
          'Accept': 'application/json'
        },
        body: JSON.stringify({
          document_type: info.tido,
          document_number: info.nudo,
          lineas: lineas_payload,  // 🆕 selección granular en mismo POST
        })
      });
      let d;
      try { d = await r.json(); }
      catch(_){
        errList.push({ label, motivo: `Backend respondió HTML (status ${r.status}). ¿Sesión expirada?` });
        continue;
      }
      if (r.status === 409 || d.code === 'DUPLICATE'){
        // Caso DUPLICATE sin líneas → 409 estándar (mantiene compat).
        // Si vinieron líneas, el backend lo procesa como UPDATE silencioso
        // y devuelve 200 con duplicate_updated=true (no llega acá).
        dupList.push(label);
        continue;
      }
      if (!d.ok){
        const motivo = (d.error || `HTTP ${r.status}`).toString().substring(0, 140);
        errList.push({ label, motivo });
        continue;
      }
      okList.push(label);
      if (d.duplicate_updated){
        console.log(`[rba] doc ${label} ya estaba asociado → selección actualizada (${d.lineas_guardadas} líneas)`);
      } else {
        console.log(`[rba] ✓ doc ${label} asociado (perf=${d._perf_ms || '?'}ms · ${d.lineas_guardadas || 0} líneas)`);
      }
      if (d.warning_otro_retiro){
        otrosN.push(d.warning_otro_retiro.code);
      }
    } catch(e){
      errList.push({ label, motivo: 'Sin conexión: ' + (e.message || 'error de red') });
    }
  }
  btn.disabled = false;
  btn.innerHTML = '<i class="bi bi-box-arrow-in-down"></i><span>Asociar al retiro</span>';

  const okN = okList.length;
  const dupN = dupList.length;
  const errN = errList.length;

  if (okN){
    let msg = `✓ ${okN} documento${okN===1?'':'s'} asociado${okN===1?'':'s'}`;
    if (dupN) msg += ` · ${dupN} ya estaban`;
    if (errN) msg += ` · ${errN} con error`;
    ilusToast(msg, { type: 'success', duration: 5000 });
    if (otrosN.length){
      ilusToast(`⚠ Algunos figuran en otros retiros: ${otrosN.join(', ')}`, { type:'warning', duration:6000 });
    }
    // Si hubo errores parciales, los mostramos también con detalle
    if (errN){
      const detalle = errList.map(e => `${e.label}: ${e.motivo}`).join('\n');
      ilusAlert({
        type: 'warning',
        title: `${errN} no se pudieron asociar`,
        message: 'Algunos productos sí se asociaron, otros fallaron:',
        sub: detalle,
        subHtml: false,
      });
    }
    await refrescarDocsAsociados(_RID);
    cargarSaldoCliente(_RID);
    rbaClose();
  } else if (dupN && !errN){
    ilusToast(`Los ${dupN} documento${dupN===1?'':'s'} ya estaban asociado${dupN===1?'':'s'}: ${dupList.join(', ')}`, {
      type:'warning', duration: 6000
    });
  } else {
    // 100% error — mostrar TODOS los motivos
    const detalle = errList.map(e => `${e.label}: ${e.motivo}`).join('\n');
    ilusAlert({
      type: 'error',
      title: `No se pudo asociar (${errN} error${errN===1?'':'es'})`,
      message: 'Estos son los motivos exactos:',
      sub: detalle,
      subHtml: false,
    });
  }
}

// Enter en inputs del modal dispara búsqueda
document.addEventListener('keydown', e => {
  if (!_RBA.open) return;
  if (e.key === 'Enter'){
    if (document.activeElement && document.activeElement.id === 'rbaDocNudo') rbaBuscarPorDoc();
    if (document.activeElement && document.activeElement.id === 'rbaCliQ')    rbaBuscarPorCliente();
  }
});

// ════════════════════════════════════════════════════════════════════
//  INLINE EDIT — Auto-save de campos de la ficha (Daniel 2026-05-23)
//  Marca cualquier elemento con `data-inline-edit="nombre_campo"` para
//  hacerlo editable haciendo click. Debounce 800ms, indicador visual
//  "guardando..." / "✓ guardado HH:MM:SS". Enter guarda, Esc cancela.
//
//  Uso en template:
//    <span data-inline-edit="contact_email">valor_del_campo</span>
//
//  El backend valida vs whitelist _PICKUP_INLINE_FIELDS.
// ════════════════════════════════════════════════════════════════════
const _INLINE_EDIT_DEBOUNCE_MS = 800;
const _INLINE_EDIT_INDICATORS = new WeakMap();

function _ensureInlineIndicator(el){
  let ind = _INLINE_EDIT_INDICATORS.get(el);
  if (ind) return ind;
  ind = document.createElement('span');
  ind.className = 'inline-edit-indicator';
  ind.style.cssText = 'display:inline-block;margin-left:8px;font-size:.72rem;font-weight:600;color:#6b7280;transition:color .2s;vertical-align:middle';
  if (el.parentNode){
    el.parentNode.insertBefore(ind, el.nextSibling);
  }
  _INLINE_EDIT_INDICATORS.set(el, ind);
  return ind;
}

async function _saveInlineField(el){
  const field = el.dataset.inlineEdit;
  const newValue = el.textContent.replace(/\s+/g, ' ').trim();
  const originalValue = el.dataset.inlineOriginal || '';
  if (newValue === originalValue){
    delete el.dataset.inlineDirty;
    return;
  }
  const indicator = _ensureInlineIndicator(el);
  indicator.innerHTML = '<i class="bi bi-arrow-repeat" style="display:inline-block;animation:spin 1s linear infinite"></i> guardando…';
  indicator.style.color = '#6b7280';
  try {
    const r = await fetch(`/retiros/${_RID}/field`, {
      method: 'PATCH',
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'X-Requested-With': 'XMLHttpRequest',
      },
      body: JSON.stringify({ field, value: newValue }),
    });
    let d;
    try { d = await r.json(); }
    catch(_){
      indicator.innerHTML = '<i class="bi bi-exclamation-triangle-fill"></i> sesión expirada';
      indicator.style.color = '#dc2626';
      el.textContent = originalValue;
      return;
    }
    if (!d.ok){
      indicator.innerHTML = `<i class="bi bi-x-circle-fill"></i> ${d.error || 'error'}`;
      indicator.style.color = '#dc2626';
      el.textContent = originalValue;
      return;
    }
    el.textContent = d.value;
    el.dataset.inlineOriginal = d.value;
    delete el.dataset.inlineDirty;
    indicator.innerHTML = `<i class="bi bi-check-circle-fill"></i> guardado ${d.saved_at}`;
    indicator.style.color = '#16a34a';
    setTimeout(()=> {
      if (indicator.innerHTML.includes('check-circle')){
        indicator.style.opacity = '0';
        setTimeout(()=> { indicator.innerHTML = ''; indicator.style.opacity = '1'; }, 400);
      }
    }, 2500);
  } catch(err){
    indicator.innerHTML = '<i class="bi bi-wifi-off"></i> sin conexión';
    indicator.style.color = '#dc2626';
  }
}

function setupInlineEdit(root){
  const scope = root || document;
  scope.querySelectorAll('[data-inline-edit]:not([data-inline-ready])').forEach(el => {
    el.dataset.inlineReady = '1';
    el.contentEditable = 'true';
    el.spellcheck = false;
    el.classList.add('inline-edit-field');
    el.dataset.inlineOriginal = el.textContent.replace(/\s+/g, ' ').trim();

    let debounceTimer = null;
    el.addEventListener('input', () => {
      el.dataset.inlineDirty = '1';
      const ind = _ensureInlineIndicator(el);
      ind.innerHTML = '<i class="bi bi-pencil-fill"></i> escribiendo…';
      ind.style.color = '#6b7280';
      clearTimeout(debounceTimer);
      debounceTimer = setTimeout(() => _saveInlineField(el), _INLINE_EDIT_DEBOUNCE_MS);
    });
    el.addEventListener('blur', () => {
      if (el.dataset.inlineDirty === '1'){
        clearTimeout(debounceTimer);
        _saveInlineField(el);
      }
    });
    el.addEventListener('keydown', (e) => {
      if (e.key === 'Enter'){
        e.preventDefault();
        el.blur();
      } else if (e.key === 'Escape'){
        el.textContent = el.dataset.inlineOriginal || '';
        delete el.dataset.inlineDirty;
        el.blur();
      }
    });
    // Paste como texto plano (evita HTML)
    el.addEventListener('paste', (e) => {
      e.preventDefault();
      const text = (e.clipboardData || window.clipboardData).getData('text/plain');
      document.execCommand('insertText', false, text.replace(/\s+/g, ' ').trim());
    });
  });
}

// ════════════════════════════════════════════════════════════════════
//  EMAIL MULTI-DESTINO — sugerencias de docs ERP
//  Daniel 2026-05-23: si un doc ERP trae email del cliente, sugerimos
//  agregarlo a extra_emails con un click. Mientras más correos, mejor cobertura.
// ════════════════════════════════════════════════════════════════════
async function cargarSugerenciasEmail(){
  const cont = document.getElementById('emailErpSug');
  if (!cont) return;
  try {
    const r = await fetch(`/retiros/${_RID}/emails-info`);
    if (!r.ok) return;
    const d = await r.json();
    if (!d.ok || !d.sugerencias || !d.sugerencias.length){
      // Si no hay sugerencias pero hay total, mostramos resumen
      if (d.count > 1){
        cont.innerHTML = `<div style="font-size:.74rem;color:#16a34a;margin-top:4px">
          <i class="bi bi-check-circle-fill"></i> ${d.count} destinatarios configurados</div>`;
      }
      return;
    }
    cont.innerHTML = '<div style="font-size:.72rem;color:#64748b;margin:6px 0 4px"><i class="bi bi-lightbulb"></i> Detectados en ERP — click para agregar:</div>' +
      d.sugerencias.map(em => `<button type="button" class="sug-email-chip" onclick="agregarEmailSugerencia('${_esc(em).replace(/'/g, "\\'")}')">
        <i class="bi bi-plus-circle"></i> ${_esc(em)}
      </button>`).join('');
  } catch(e){ /* silencioso */ }
}

async function agregarEmailSugerencia(email){
  const field = document.querySelector('[data-inline-edit="extra_emails"]');
  if (!field) return;
  const current = (field.textContent || '').trim();
  const nuevos = current ? `${current}, ${email}` : email;
  // Actualizar visualmente y disparar guardado
  field.textContent = nuevos;
  field.dataset.inlineDirty = '1';
  field.dataset.inlineOriginal = current;  // para que detecte cambio
  // Trigger save via blur (debounce normal)
  field.dispatchEvent(new Event('input', { bubbles: true }));
  field.dispatchEvent(new Event('blur', { bubbles: true }));
  // Refrescar sugerencias después
  setTimeout(()=> cargarSugerenciasEmail(), 1200);
}

// ════════════════════════════════════════════════════════════════════
//  INIT
// ════════════════════════════════════════════════════════════════════
document.addEventListener('DOMContentLoaded', function(){
  // ⚡ PERF (Juan Daniel 2026-06-03): abrir una solicitud en <2s.
  // La ficha PINTA primero (los datos del retiro y los documentos ya
  // asociados —con su saldo guardado— vienen en el HTML, se ven al
  // instante). Las 3 cargas pesadas se DIFIEREN a después del primer
  // paint con requestIdleCallback, ordenadas de la más rápida a la más
  // lenta. La consulta al ERP Random (la más lenta) va de última y nunca
  // bloquea lo que el operador ve.
  const _deferLoad = (fn, timeout) => {
    const run = () => { try { fn(); } catch(_e){} };
    if (typeof window.requestIdleCallback === 'function') {
      window.requestIdleCallback(run, { timeout: timeout });
    } else {
      setTimeout(run, 60);   // fallback Safari < 17
    }
  };
  // 1) Tabla de productos del retiro (consulta local, rápida).
  _deferLoad(() => refrescarTablaProductos(), 800);
  // 2) Sugerencias de email (ligera).
  _deferLoad(() => cargarSugerenciasEmail(), 1200);
  // 3) Documentos con saldo en el ERP (lo más lento) — al final, en
  //    segundo plano. El operador ya tiene la ficha completa a la vista.
  _deferLoad(() => cargarSaldoCliente(_RID), 2000);

  // 4) Calendario de propuesta, solo si la solicitud está en ese paso.
  //    retiros_calendar.js carga con `defer`, por lo que para este punto
  //    (DOMContentLoaded) ya está disponible. Antes esto se montaba en
  //    pleno parseo y bloqueaba; ahora va después del primer paint.
  if (window._shouldMountProposeCal && !window._calMounted){
    // Daniel 2026-06-15: si hay card "El cliente pidió", el calendario arranca
    // OCULTO y se monta al pulsar "Modificar" (lazy). Si NO hay card (cliente
    // sin hora exacta), el picker ya está visible → montamos de inmediato.
    var _hasClientCard = !!document.getElementById('iwClientCard');
    if (!_hasClientCard){
      try { _mountProposeCalendar(); window._calMounted = true; } catch(_e){}
    }
  }

  // Hint de peso (instantáneo, sin red).
  const pesoInicial = RETIROS_DETAIL_DATA.pesoInicial;
  if (pesoInicial > 100){
    const hp = document.getElementById('hintPeso');
    if (hp) hp.style.display = 'flex';
  }
  // Activar edición inline en todos los elementos marcados.
  setupInlineEdit();
});
  // ── Check WMS + modal retirar (Daniel 2026-06-20) ──
  const _WMS_RID = RETIROS_DETAIL_DATA.reqId;
  let _wmsEstado = null;
  const _wmsEsc = s => String(s==null?'':s).replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));

  async function _wmsCargar(){
    const cont = document.getElementById('wmsItems');
    if (!cont) return;
    try {
      const r = await fetch(`/retiros/${_WMS_RID}/picking`, {credentials:'same-origin', cache:'no-store'});
      const d = await r.json();
      if (!d || !d.ok) { cont.innerHTML = '<div class="text-muted small">No se pudo cargar el checklist.</div>'; return; }
      _wmsEstado = d;
      _wmsRender(d);
    } catch(e) { cont.innerHTML = '<div class="text-muted small">Error de red al cargar el checklist.</div>'; }
  }
  function _wmsRender(d){
    const cont = document.getElementById('wmsItems');
    const bar = document.getElementById('wmsProgBar');
    const txt = document.getElementById('wmsProgTxt');
    if (!cont) return;
    if (!d.items.length){
      cont.innerHTML = '<div class="text-muted small">Este retiro no tiene productos con detalle — puedes cerrar directo con "Marcar como RETIRADO".</div>';
      if (txt) txt.textContent = 'Sin ítems';
      return;
    }
    const pct = d.total ? Math.round(d.hechos * 100 / d.total) : 0;
    if (bar) bar.style.width = pct + '%';
    if (txt) txt.textContent = `${d.hechos}/${d.total} · ${pct}%`;
    cont.innerHTML = d.items.map(it => `
      <label class="d-flex align-items-center gap-2 py-2 px-2 rounded" style="cursor:pointer;border-bottom:1px solid #f3f4f6;${it.picked ? 'background:#f0fdf4' : ''}">
        <input type="checkbox" class="form-check-input mt-0" style="width:1.25em;height:1.25em" ${it.picked ? 'checked' : ''}
               onchange="_wmsToggle(${it.id}, this.checked, this)">
        <span class="flex-grow-1 ${it.picked ? 'text-decoration-line-through text-muted' : ''}">
          ${_wmsEsc(it.descripcion)}
          <span class="text-muted small">· SKU ${_wmsEsc(it.sku)} · x${it.cantidad}</span>
        </span>
        ${it.picked && it.picked_by ? `<span class="badge bg-success-subtle text-success" style="font-size:.66rem">✓ ${_wmsEsc(it.picked_by)}</span>` : ''}
      </label>`).join('');
    if (d.completo){
      cont.insertAdjacentHTML('beforeend',
        '<div class="alert alert-success py-2 px-3 mt-2 mb-0 small"><i class="bi bi-check2-all me-1"></i><strong>Pedido LISTO para entrega</strong> — checklist completo.</div>');
    }
  }
  async function _wmsToggle(itemId, picked, cb){
    cb.disabled = true;
    try {
      const r = await fetch(`/retiros/${_WMS_RID}/picking/toggle`, {
        method:'POST', headers:{'Content-Type':'application/json'}, credentials:'same-origin',
        body: JSON.stringify({item_id: itemId, picked: picked})});
      const d = await r.json();
      if (d && d.ok){ _wmsEstado = d; _wmsRender(d); }
      else { cb.checked = !picked; ilusToast(d.error || 'No se pudo guardar', {type:'error'}); }
    } catch(e){ cb.checked = !picked; ilusToast('Error de red', {type:'error'}); }
    cb.disabled = false;
  }
  function abrirModalRetirar(){
    // Proceso EXIGENTE: si el checklist existe y está incompleto, advertimos.
    const warn = document.getElementById('retWmsWarn');
    const wtxt = document.getElementById('retWmsWarnTxt');
    if (warn && wtxt && _wmsEstado && _wmsEstado.total > 0 && !_wmsEstado.completo){
      wtxt.textContent = `Faltan ${_wmsEstado.total - _wmsEstado.hechos} de ${_wmsEstado.total} ítems del checklist por chequear`;
      warn.style.display = '';
    } else if (warn) { warn.style.display = 'none'; }
    bootstrap.Modal.getOrCreateInstance(document.getElementById('modalRetirar')).show();
  }
  document.addEventListener('DOMContentLoaded', _wmsCargar);
(function(){
  'use strict';
  const RID = RETIROS_DETAIL_DATA.reqId;
  const btn=document.getElementById('opChatBtn'), panel=document.getElementById('opChatPanel');
  const body=document.getElementById('opChatBody'), input=document.getElementById('opChatInput');
  const sendBtn=document.getElementById('opChatSend'), dot=document.getElementById('opChatDot'), closeBtn=document.getElementById('opChatClose');
  let open=false, seenCl=0, pollT=null;
  const esc=s=>String(s==null?'':s).replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
  function render(msgs){
    if(!msgs.length){ body.innerHTML='<div class="opc-empty">Aún no hay mensajes con el cliente.</div>'; return; }
    body.innerHTML=msgs.map(m=>`<div class="opc-msg ${m.sender==='operador'?'op':'cl'}">${esc(m.cuerpo)}<div class="h">${esc(m.autor)} · ${esc(m.hora)}</div></div>`).join('');
    body.scrollTop=body.scrollHeight;
  }
  async function load(){
    try{
      const r=await fetch(`/retiros/${RID}/mensajes`,{cache:'no-store',credentials:'same-origin'});
      const d=await r.json(); if(!d||!d.ok) return;
      const cl=d.mensajes.filter(m=>m.sender==='cliente').length;
      if(open){ render(d.mensajes); seenCl=cl; dot.style.display='none'; }
      else { const nuevos=cl-seenCl; if(nuevos>0){ dot.textContent=nuevos; dot.style.display='flex'; } }
    }catch(e){}
  }
  async function send(){
    const t=input.value.trim(); if(!t) return; input.value=''; sendBtn.disabled=true;
    if(body.querySelector('.opc-empty')) body.innerHTML='';
    const tmp=document.createElement('div'); tmp.className='opc-msg op'; tmp.innerHTML=esc(t)+'<div class="h">Tú · enviando…</div>';
    body.appendChild(tmp); body.scrollTop=body.scrollHeight;
    try{
      const r=await fetch(`/retiros/${RID}/mensaje`,{method:'POST',headers:{'Content-Type':'application/json'},credentials:'same-origin',body:JSON.stringify({mensaje:t})});
      const d=await r.json(); if(d&&d.ok){ await load(); } else { tmp.querySelector('.h').textContent='No se pudo enviar'; }
    }catch(e){ tmp.querySelector('.h').textContent='Error de red'; }
    sendBtn.disabled=false; input.focus();
  }
  function openP(){ open=true; panel.classList.add('open'); dot.style.display='none'; load(); input.focus(); if(!pollT) pollT=setInterval(()=>{if(open)load();},9000); }
  function closeP(){ open=false; panel.classList.remove('open'); }
  btn.addEventListener('click',()=>open?closeP():openP());
  closeBtn.addEventListener('click',closeP);
  sendBtn.addEventListener('click',send);
  input.addEventListener('keydown',e=>{ if(e.key==='Enter'){e.preventDefault();send();} });
  setTimeout(()=>{ load(); setInterval(()=>{if(!open)load();},20000); },2000);
})();

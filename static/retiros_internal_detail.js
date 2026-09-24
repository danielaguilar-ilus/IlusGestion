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
//  ZONA SECUNDARIA — expandir + ir a una tarjeta (Comunicación, Adjuntos,
//  Propuestas, Historial, Cambiar estado)
//  REESTRUCTURACIÓN 2026-09-22: antes esto cambiaba la pestaña activa
//  dentro de un tab-strip único (#infoTabs .info-tabs-nav). Ahora cada
//  una es su propia tarjeta .step-section.is-secondary colapsable — la
//  función expande esa tarjeta y hace scroll hasta ella. Mismos ids
//  #tab-<name> que ya usaba el tab-strip (comunicacion/adjuntos/
//  propuestas/historial/acciones), así que TODOS los llamadores
//  existentes (botón "Mensajes/Adjuntos" del header, "Enviarle
//  mensaje"/"Enviar recordatorio") siguen funcionando sin cambios.
// ════════════════════════════════════════════════════════════════════
// Herramientas del retiro (2026-09-24): una tarjeta con pestañas en vez de 5
// tarjetas apiladas. Solo un panel abierto a la vez; tocar la pestaña activa
// lo cierra. cambiarTabInfo(name) la abre desde los botones de más arriba.
function _rdMoreAbrir(name, toggle){
  const pane = document.getElementById('tab-' + name);
  if (!pane) return null;
  const card = document.getElementById('rdMore');
  const yaAbierto = !pane.hidden;
  if (card){
    card.querySelectorAll('.rd-more-pane').forEach(p => { p.hidden = true; });
    card.querySelectorAll('.rd-more-tab').forEach(t => t.setAttribute('aria-selected', 'false'));
    card.classList.remove('is-open');
  }
  if (toggle && yaAbierto) return null;
  pane.hidden = false;
  if (card){
    card.classList.add('is-open');
    const tab = card.querySelector('.rd-more-tab[data-pane="' + name + '"]');
    if (tab) tab.setAttribute('aria-selected', 'true');
  }
  return pane;
}
function cambiarTabInfo(name){
  const pane = _rdMoreAbrir(name, false);
  if (!pane) return;
  (document.getElementById('rdMore') || pane).scrollIntoView({ behavior: 'smooth', block: 'start' });
}
document.addEventListener('DOMContentLoaded', () => {
  document.querySelectorAll('#rdMore .rd-more-tab').forEach(tab => {
    tab.addEventListener('click', () => _rdMoreAbrir(tab.dataset.pane, true));
  });
});

// ════════════════════════════════════════════════════════════════════
//  COLAPSAR/EXPANDIR cada "Paso" y cada tarjeta de zona secundaria
//  REESTRUCTURACIÓN 2026-09-22 — mismo patrón visual que
//  .erp-doc-toggle/.erp-doc-card.is-open (arriba, lista de docs ERP):
//  el toggle vive en el header de la tarjeta, .is-collapsed en el
//  <section class="step-section"> controla qué se ve.
// ════════════════════════════════════════════════════════════════════
function toggleStepCollapse(toggleBtn){
  const card = toggleBtn.closest('.step-section');
  if (!card) return;
  card.classList.toggle('is-collapsed');
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
  // threshold 0 (antes 0.08): Chrome solo marca "visible" cuando el 8 % de la
  // sección está en pantalla → una sección de más de ~12 pantallas (Paso 3 con
  // muchos productos) NUNCA aparecía: pantallas grises al bajar (2026-09-24).
  }, { threshold: 0, rootMargin: '0px 0px -20px 0px' });
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
  // Red de seguridad: pestaña en segundo plano / observador que no dispara →
  // ninguna sección puede quedar invisible para siempre.
  setTimeout(() => { steps.forEach(s => s.classList.add('in-view')); }, 3000);
})();
// ════════════════════════════════════════════════════════════════════
//  ESTADO COMPARTIDO + UTILS
// ════════════════════════════════════════════════════════════════════
const _RID = RETIROS_DETAIL_DATA.reqId;
const _RUT_CLI = RETIROS_DETAIL_DATA.customerRut;
let _erpLineas = [];

function _esc(s){ return String(s == null ? '' : s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); }

// ════════════════════════════════════════════════════════════════════
//  Daniel 2026-09-16 — helpers para la tabla extendida "Documentos
//  asociados" del Paso 2 (columnas Emisión/Total/Asociado por, orden
//  por columna, buscador y selección múltiple).
// ════════════════════════════════════════════════════════════════════

// Convierte un timestamp 'YYYY-MM-DD HH:MM:SS' tal como lo devuelve MySQL
// (naive, guardado en UTC vía NOW() — REGLA #6) a hora de Chile. El filtro
// Jinja `chile_fmt` hace esto server-side con zoneinfo; acá usamos Intl con
// timeZone America/Santiago, que también resuelve el horario de verano
// automáticamente (igual criterio, sin duplicar tablas de DST a mano).
function _chileFmtStr(raw){
  if (!raw) return '—';
  try {
    const s = String(raw).trim();
    const iso = s.includes('T') ? s : s.replace(' ', 'T');
    const d = new Date(iso.endsWith('Z') ? iso : iso + 'Z');
    if (isNaN(d.getTime())) return _esc(s);
    return new Intl.DateTimeFormat('es-CL', {
      timeZone: 'America/Santiago', day: '2-digit', month: '2-digit', year: 'numeric',
      hour: '2-digit', minute: '2-digit', hour12: false,
    }).format(d).replace(',', '');
  } catch(e){ return _esc(String(raw)); }
}

// Clave ordenable AAAAMMDD para la columna "Emisión". fecha_emision puede
// venir 'dd-mm-aaaa' (formato típico de doc.fecha del ERP, ver rbaRenderCliDocs)
// o 'aaaa-mm-dd' (ISO). Si no matchea ninguno, se ordena por el texto tal cual
// (best-effort, nunca rompe el sort).
function _dateSortVal(raw){
  if (!raw) return '';
  const s = String(raw).trim();
  // dd-mm-aaaa (chile_fmt, propuesta/confirmada/added_at ya formateadas)
  let m = s.match(/^(\d{2})-(\d{2})-(\d{4})/);
  if (m) return m[3] + m[1] + m[2];
  // aaaa-mm-dd (ISO, por si algo llega crudo)
  m = s.match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (m) return m[1] + m[2] + m[3];
  // FIX revisor 2026-09-16: fecha_emision viene del ERP como "dd/mm/aaaa"
  // (con barras, _cubicador_fetch) — sin este patrón, ordenar por "Emisión"
  // caía al fallback de texto crudo y agrupaba por día del mes, no por fecha.
  m = s.match(/^(\d{2})\/(\d{2})\/(\d{4})/);
  if (m) return m[3] + m[2] + m[1];
  return s;
}

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
      <td colspan="12">
        <div class="ilus-tabla-empty">
          <i class="bi bi-inbox"></i>
          <strong>Aún no hay documentos asociados</strong>
          <small>Haz click en "Agregar factura o boleta" arriba para buscarla en el ERP.</small>
        </div>
      </td>
    </tr>`;
    _docsTablaUpdateSeleccion();
    return;
  }
  body.innerHTML = docs.map((d, idx) => {
    let saldoPill, saldoSort;
    if (d.con_saldo === 1){ saldoPill = '<span class="td-pill td-pill-ok"><i class="bi bi-check-circle"></i>Con saldo</span>'; saldoSort = 'Con saldo'; }
    else if (d.con_saldo === 0){ saldoPill = '<span class="td-pill td-pill-warn"><i class="bi bi-exclamation-triangle"></i>Sin saldo</span>'; saldoSort = 'Sin saldo'; }
    else { saldoPill = '<span class="td-pill"><i class="bi bi-question-circle"></i>No verif.</span>'; saldoSort = 'No verificado'; }
    const tipoUp = String(d.document_type || '').toUpperCase();
    const numero = d.document_number || '';
    const numeroSort = parseInt(numero, 10) || 0;
    // 🔧 FIX Daniel 2026-05-24: mostrar "X / Y" cuando hay selección parcial
    // para que el operador vea cuántas líneas REALMENTE se asociaron.
    const _totalLn = d.n_lineas || 0;
    const _selLn   = (d.n_lineas_seleccionadas != null) ? d.n_lineas_seleccionadas : null;
    const _lineasCell = (d.has_seleccion_lineas && _selLn !== null)
      ? `<strong style="color:#92400e">${_selLn}</strong><small style="color:#9ca3af"> / ${_totalLn}</small>`
      : `<strong>${_totalLn}</strong>`;
    // 🆕 Daniel 2026-09-16: Emisión/Total — mismo criterio ya usado en
    // rbaRenderCliDocs (doc.fecha, doc.valor_total del ERP): se muestran
    // TAL CUAL vienen, sin reparsear la fecha (el ERP ya la formatea).
    // OJO: si GET /retiros/<rid>/docs aún no devuelve fecha_emision/
    // valor_total, quedan en blanco sin romper el render (fallback '—').
    const fechaRaw  = d.fecha_emision || '';
    const fechaDisp = fechaRaw ? _esc(String(fechaRaw)) : '—';
    const valorNum  = (d.valor_total !== null && d.valor_total !== undefined && d.valor_total !== '') ? parseFloat(d.valor_total) : NaN;
    const totalDisp = isFinite(valorNum) ? '$' + Math.round(valorNum).toLocaleString('es-CL') : '—';
    const addedByDisp = d.added_by ? _esc(d.added_by) : '—';
    const addedAtDisp = d.added_at ? _chileFmtStr(d.added_at) : '—';
    const otroRutBadge = d.motivo_otro_rut ? `<details class="otro-rut-badge">
          <summary><i class="bi bi-exclamation-triangle-fill"></i> Otro RUT</summary>
          <div class="otro-rut-detail">
            <strong>Motivo:</strong> ${_esc(d.motivo_otro_rut)}<br>
            <strong>Justificado por:</strong> ${_esc(d.otro_rut_por || '—')}${d.otro_rut_en ? ' · ' + _chileFmtStr(d.otro_rut_en) : ''}
          </div>
        </details>` : '';
    // Daniel 2026-09-23: el botón "Productos" de esta fila se quitó a
    // propósito — la tabla de "Productos a retirar" (#prodDiagBody) es el
    // editor ahora, sin salir de la ficha.
    return `<tr data-doc-id="${d.id}">
      <td data-label="" class="td-check"><input type="checkbox" class="doc-row-check" value="${d.id}" onchange="_docsTablaUpdateSeleccion()"></td>
      <td data-label="#">${idx + 1}</td>
      <td data-label="Tipo" data-sort-key="tipo" data-sort-value="${_esc(tipoUp)}"><span class="td-pill td-pill-dark">${_esc(tipoUp)}</span></td>
      <td data-label="Nº" class="mono" data-sort-key="numero" data-sort-value="${numeroSort}">${_esc(numero)}</td>
      <td data-label="Emisión" data-sort-key="fecha" data-sort-value="${_esc(String(fechaRaw))}">${fechaDisp}</td>
      <td data-label="Cliente">
        <div class="td-cli">
          <strong>${_esc(d.cliente_nombre || '—')}</strong>
          <small class="mono">${_esc(d.cliente_rut || '—')}</small>
        </div>
      </td>
      <td data-label="Líneas" class="num" data-sort-key="lineas" data-sort-value="${_totalLn}" title="${d.has_seleccion_lineas?'Líneas seleccionadas / total del documento':'Total de líneas'}">${_lineasCell}</td>
      <td data-label="Peso" class="num" data-sort-key="peso" data-sort-value="${parseFloat(d.peso_real_kg||0)}">${parseFloat(d.peso_real_kg||0).toFixed(1)} kg</td>
      <td data-label="Total" class="num" data-sort-key="total" data-sort-value="${isFinite(valorNum)?valorNum:0}">${totalDisp}</td>
      <td data-label="Saldo" data-sort-key="saldo" data-sort-value="${saldoSort}">${saldoPill}</td>
      <td data-label="Asociado por" data-sort-key="asociado" data-sort-value="${_esc(d.added_by||'')}">
        <div class="td-asoc">
          <strong>${addedByDisp}</strong>
          <small>${addedAtDisp}</small>
          ${otroRutBadge}
        </div>
      </td>
      <td data-label="Acciones" class="acciones">
        <button type="button" class="td-btn td-btn-quitar"
                onclick="quitarDoc(${_RID}, ${d.id}, '${_esc(tipoUp)} ${_esc(numero)}')"
                title="Quitar del retiro (no afecta al ERP)">
          <i class="bi bi-x-lg"></i><span>Quitar</span>
        </button>
      </td>
    </tr>`;
  }).join('');
  _docsTablaApplySort();
  _docsTablaUpdateSeleccion();
  const searchInp = document.getElementById('tabDocsBuscar');
  if (searchInp && searchInp.value) _docsTablaFiltrar(searchInp.value);
}

// ════════════════════════════════════════════════════════════════════
//  Daniel 2026-09-16 — orden por columna, buscador y selección múltiple
//  de la tabla "Documentos asociados". Aplica tanto al render inicial
//  (Jinja, en el HTML servido) como a cada re-render vía
//  _renderTablaDocsAsociados (llamado desde refrescarDocsAsociados).
// ════════════════════════════════════════════════════════════════════
let _docsSortState = { key: null, dir: 1, type: 'str' };

function _docsTablaSort(key, type){
  if (_docsSortState.key === key) _docsSortState.dir *= -1;
  else { _docsSortState.key = key; _docsSortState.dir = 1; _docsSortState.type = type; }
  _docsTablaApplySort();
}

function _docsTablaApplySort(){
  const { key, dir, type } = _docsSortState;
  if (!key) return;
  const tbody = document.getElementById('tabDocsAsociadosBody');
  if (!tbody) return;
  const rows = Array.from(tbody.querySelectorAll('tr[data-doc-id]'));
  if (!rows.length) return;
  rows.sort((a, b) => {
    const va = _docsSortCellVal(a, key, type);
    const vb = _docsSortCellVal(b, key, type);
    if (type === 'num') return (va - vb) * dir;
    return String(va).localeCompare(String(vb), 'es', { numeric: true, sensitivity: 'base' }) * dir;
  });
  rows.forEach(r => tbody.appendChild(r));
  _docsTablaUpdateSortIcons();
}

function _docsSortCellVal(row, key, type){
  const cell = row.querySelector(`td[data-sort-key="${key}"]`);
  if (!cell) return type === 'num' ? 0 : '';
  let raw = cell.getAttribute('data-sort-value');
  if (raw === null) raw = cell.textContent.trim();
  // La columna Emisión puede traer 'dd-mm-aaaa' (ERP) o 'aaaa-mm-dd' (ISO) —
  // se normaliza a clave AAAAMMDD recién acá, tanto para filas Jinja (SSR)
  // como para las que arma _renderTablaDocsAsociados.
  if (key === 'fecha') return _dateSortVal(raw);
  return type === 'num' ? (parseFloat(raw) || 0) : raw;
}

function _docsTablaUpdateSortIcons(){
  document.querySelectorAll('#tabDocsAsociados thead th[data-sort-key]').forEach(th => {
    const ico = th.querySelector('.th-sort-ico');
    if (!ico) return;
    if (th.dataset.sortKey === _docsSortState.key){
      ico.textContent = _docsSortState.dir === 1 ? '▲' : '▼';
      ico.classList.add('is-active');
    } else {
      ico.textContent = '↕';
      ico.classList.remove('is-active');
    }
  });
}

// Buscador rápido — filtra filas por texto visible (tipo, número, cliente,
// asociado por, etc.), sin recargar. REGLA #4.3: al vaciar el campo, la
// tabla se re-muestra completa (no queda pegada al último filtro).
function _docsTablaFiltrar(q){
  const term = (q || '').trim().toLowerCase();
  const tbody = document.getElementById('tabDocsAsociadosBody');
  if (!tbody) return;
  const rows = Array.from(tbody.querySelectorAll('tr[data-doc-id]'));
  let visibles = 0;
  rows.forEach(r => {
    const match = !term || r.textContent.toLowerCase().includes(term);
    r.style.display = match ? '' : 'none';
    if (match) visibles++;
  });
  let emptyRow = document.getElementById('rowDocsFiltroVacio');
  if (rows.length && term && visibles === 0){
    if (!emptyRow){
      emptyRow = document.createElement('tr');
      emptyRow.id = 'rowDocsFiltroVacio';
      emptyRow.className = 'ilus-tabla-empty-row';
      tbody.appendChild(emptyRow);
    }
    emptyRow.innerHTML = `<td colspan="12"><div class="ilus-tabla-empty">
      <i class="bi bi-search"></i>
      <strong>Sin resultados para "${_esc(q)}"</strong>
      <small>Prueba con otro tipo, número o nombre de cliente.</small>
    </div></td>`;
  } else if (emptyRow){
    emptyRow.remove();
  }
  _docsTablaUpdateSeleccion();
}

// "Seleccionar/deseleccionar todo" — REGLA #14. Header checkbox, mismo
// patrón que tkaToggleAllDoc (templates/tickets/_tka_modal.html): el
// nuevo estado del checkbox del header decide si se marcan o desmarcan
// TODAS las filas visibles (respeta el filtro de búsqueda activo).
function _docsTablaToggleAll(checked){
  document.querySelectorAll('#tabDocsAsociadosBody .doc-row-check').forEach(cb => {
    const tr = cb.closest('tr');
    if (tr && tr.style.display === 'none') return;
    cb.checked = checked;
  });
  _docsTablaUpdateSeleccion();
}

function _docsTablaUpdateSeleccion(){
  const all = Array.from(document.querySelectorAll('#tabDocsAsociadosBody .doc-row-check'));
  const visibles = all.filter(cb => { const tr = cb.closest('tr'); return !tr || tr.style.display !== 'none'; });
  const checked = all.filter(cb => cb.checked);
  const nSpan = document.getElementById('nSeleccionados');
  const btn = document.getElementById('btnQuitarSeleccionados');
  if (nSpan) nSpan.textContent = `(${checked.length})`;
  if (btn) btn.disabled = checked.length === 0;
  const headerChk = document.getElementById('tabDocsCheckAll');
  if (headerChk){
    const visiblesChecked = visibles.filter(cb => cb.checked);
    headerChk.checked = visibles.length > 0 && visiblesChecked.length === visibles.length;
    headerChk.indeterminate = visiblesChecked.length > 0 && visiblesChecked.length < visibles.length;
  }
}

// "Quitar seleccionados" — reutiliza el mismo DELETE que ya usa quitarDoc
// fila por fila, con UN solo ilusConfirm de resumen antes (REGLA #1).
async function _docsTablaQuitarSeleccionados(){
  const checks = Array.from(document.querySelectorAll('#tabDocsAsociadosBody .doc-row-check:checked'));
  if (!checks.length) return;
  const n = checks.length;
  const ok = await ilusConfirm({
    title: 'Quitar documentos',
    message: `¿Quitar ${n} documento${n===1?'':'s'} de este retiro?`,
    sub: 'Esto NO elimina los documentos del ERP, solo los saca de este retiro.',
    okLabel: 'Sí, quitar', cancelLabel: 'Cancelar', danger: true,
  });
  if (!ok) return;
  const btn = document.getElementById('btnQuitarSeleccionados');
  const _orig = btn ? btn.innerHTML : '';
  if (btn){ btn.disabled = true; btn.innerHTML = '<span class="spinner-border spinner-border-sm"></span> Quitando...'; }
  let okN = 0, errN = 0;
  for (const cb of checks){
    try {
      const r = await fetch(`/retiros/${_RID}/docs/${cb.value}`, { method: 'DELETE' });
      const d = await r.json();
      if (d.ok) okN++; else errN++;
    } catch(e){ errN++; }
  }
  if (okN) ilusToast(`✓ ${okN} documento${okN===1?'':'s'} quitado${okN===1?'':'s'}`, { type:'success' });
  if (errN) ilusToast(`⚠ ${errN} no se pudo${errN===1?'':'ieron'} quitar`, { type:'warning' });
  await refrescarDocsAsociados(_RID);
  cargarSaldoCliente(_RID);
  if (btn){ btn.disabled = false; btn.innerHTML = _orig || '<i class="bi bi-trash3"></i>Quitar seleccionados <span id="nSeleccionados">(0)</span>'; }
}

// ════════════════════════════════════════════════════════════════════
//  PRODUCTOS A RETIRAR — CON DIAGNÓSTICO (ficha v4, Daniel 2026-09-23)
//  "lo que más quiero vender es qué productos van a retirar y cuánto
//  pesa... que me muestre si está comprometido, cuánto hay de stock, si
//  puedo avanzar o no, si está entregado con saldo o no".
//
//  Reemplaza la tabla plana de antes Y el modal de selección por doc
//  (abrirSeleccionProductos/guardarSeleccionProductos, REGLA #4.2: Daniel
//  pidió explícitamente quitar el botón "Productos" — su función de
//  elegir qué se retira sigue viva, ahora inline en esta misma tabla).
//  Fuente: GET /retiros/<rid>/productos-erp (pickups_module.py), que
//  cruza saldo/guía/stock del ERP con lo que ILUS ya sabe. Los cambios
//  (casilla, cantidad, "Entregar igual…") se guardan con
//  POST /docs/<doc_id>/lineas (debounce ~600ms) y recargan esta tabla.
// ════════════════════════════════════════════════════════════════════
function _fmtNum(n, dec){
  const v = parseFloat(n || 0);
  if (!isFinite(v)) return '0';
  return v.toFixed(dec == null ? 2 : dec);
}

const _PD_VEREDICTO = {
  ok:        { cls: 'is-ok',        ico: 'bi-check-circle-fill' },
  revisar:   { cls: 'is-revisar',   ico: 'bi-exclamation-triangle-fill' },
  bloqueado: { cls: 'is-bloqueado', ico: 'bi-x-octagon-fill' },
  vacio:     { cls: 'is-vacio',     ico: 'bi-inbox' },
};
let _pdUltimoMotivo = '';   // reusar motivo entre líneas del mismo lote (mismo patrón que el tka)
let _pdSaveTimers = {};     // debounce de guardado, uno por documento

async function refrescarProductosDiag(){
  const banner = document.getElementById('prodDiagBanner');
  const body   = document.getElementById('prodDiagBody');
  if (!banner || !body) return;
  try {
    const d = await _fetchJsonSafe(`/retiros/${_RID}/productos-erp`);
    if (!d.ok){
      banner.className = 'prod-diag-banner is-bloqueado';
      banner.innerHTML = `<i class="bi bi-exclamation-triangle-fill"></i>
        <div class="pdb-txt"><strong>No se pudo cargar el diagnóstico</strong><span>${_esc(d.error || 'Error desconocido')}</span></div>`;
      return;
    }
    const v = d.veredicto || { estado: 'vacio', titulo: '', detalle: '' };
    const vc = _PD_VEREDICTO[v.estado] || _PD_VEREDICTO.vacio;
    const c = d.contadores || {};
    const chips = [
      c.listos       ? `<span class="pd-chip t-ok">${c.listos} listo${c.listos===1?'':'s'}</span>` : '',
      c.bloqueados   ? `<span class="pd-chip t-rojo">${c.bloqueados} con problema${c.bloqueados===1?'':'s'}</span>` : '',
      c.revisar      ? `<span class="pd-chip t-ambar">${c.revisar} para revisar</span>` : '',
      c.ya_entregados? `<span class="pd-chip t-gris">${c.ya_entregados} ya entregado${c.ya_entregados===1?'':'s'}</span>` : '',
      c.sin_ficha    ? `<span class="pd-chip t-ambar">${c.sin_ficha} sin ficha logística</span>` : '',
    ].filter(Boolean).join('');
    banner.className = 'prod-diag-banner ' + vc.cls;
    banner.innerHTML = `<i class="bi ${vc.ico}"></i>
      <div class="pdb-txt"><strong>${_esc(v.titulo)}</strong><span>${_esc(v.detalle)}</span></div>
      <div class="pdb-chips">${chips}</div>
      <button type="button" class="pdb-refresh" onclick="refrescarProductosDiag()" title="Volver a verificar en el ERP">
        <i class="bi bi-arrow-clockwise"></i>
      </button>`;
    if (v.erp_caido){
      banner.innerHTML += `<div class="pdb-erp-caido"><i class="bi bi-wifi-off"></i> El ERP no respondió para algún documento — no se inventó saldo ni guía, revisa cuando vuelva a responder.</div>`;
    }
    const docs = d.docs || [];
    if (!docs.length){
      body.innerHTML = '';
      return;
    }
    body.innerHTML = docs.map(doc => _pdRenderDoc(doc)).join('')
      + _pdRenderTotales(d.totales || {}, c);
  } catch(e){
    banner.className = 'prod-diag-banner is-bloqueado';
    banner.innerHTML = `<i class="bi bi-wifi-off"></i>
      <div class="pdb-txt"><strong>Error de red</strong><span>${_esc(e.message || 'No se pudo contactar al servidor')}</span></div>`;
  }
}

function _pdRenderDoc(doc){
  const otros = (doc.otros_retiros || []).filter(o => !o.cerrado);
  const otrosHtml = otros.length ? `<div class="pd-aviso-doc">
      <i class="bi bi-exclamation-triangle-fill"></i>
      También está en ${otros.map(o => `<a href="/retiros/${o.id}" target="_blank" rel="noopener">${_esc(o.code)}</a> (${_esc(o.estado)})`).join(', ')} — revisa que no se retire dos veces.
    </div>` : '';
  const erpErrHtml = !doc.erp_ok ? `<div class="pd-aviso-doc t-gris">
      <i class="bi bi-wifi-off"></i> ${_esc(doc.erp_error || 'No se pudo verificar este documento en el ERP')} — se muestra lo que ILUS guardó, sin saldo ni stock.
    </div>` : '';
  const lineas = (doc.lineas || []).filter(l => (l.sku || '').trim());
  const rows = lineas.map(l => _pdRenderLinea(doc.doc_id, l)).join('');
  const servicios = (doc.servicios || []).map(s => {
    const cons = (s.consumos || []).map(c => `${_esc(c.label)} ${_esc(c.numero)} (${_esc(c.fecha)})`).join(', ');
    return `${_esc(s.nombre || s.sku)}${cons ? ' — ' + cons : ''}`;
  });
  const servHtml = servicios.length
    ? `<div class="pd-servicios"><i class="bi bi-info-circle"></i> ${servicios.map(_esc).join(' · ')}: línea de servicio, no es producto físico.</div>`
    : '';
  return `<article class="pd-doc" data-doc-id="${doc.doc_id}">
    <div class="pd-doc-head">
      <span class="pd-doc-pill">${_esc(doc.tipo)}</span>
      <div class="pd-doc-meta">
        <span class="pd-doc-num">${_esc(doc.tipo)} ${_esc(doc.numero)}</span>
        <span class="pd-doc-fecha">${doc.fecha ? 'Emitida ' + _esc(doc.fecha) : ''}</span>
      </div>
      <button type="button" class="pd-toggle-all" onclick="_pdToggleAll(this, ${doc.doc_id})" title="Marcar o desmarcar todas las líneas de este documento">
        <i class="bi bi-check-all"></i>Seleccionar/deseleccionar todo
      </button>
    </div>
    ${otrosHtml}${erpErrHtml}
    ${rows || '<div class="pd-vacio">Sin productos en este documento (solo servicios).</div>'}
    ${servHtml}
  </article>`;
}

function _pdRenderLinea(docId, l){
  const disabled = !(l.estado === 'ok' || l.estado === 'revisar' || l.estado === 'autorizado');
  const checked = !!l.incluida;
  const nulido = l.nulido ? `línea ${l.nulido} · ` : '';
  const consumos = (l.consumos || []).map(c =>
    `<span class="pd-guia">${_esc(c.label)} ${_esc(c.numero)} · ${_esc(c.fecha)}</span>`).join('');
  const st = l.stock;
  const stockHtml = st === null
    ? '<span class="pd-stock-na">Sin verificar</span>'
    : `<span class="pd-stock-chip ${st.fisico <= 0 ? 't-rojo' : (st.libre < 0 ? 't-ambar' : 't-ok')}">${_fmtNum(st.fisico, 0)} físico</span>
       ${st.comprometido > 0 ? `<span class="pd-stock-chip t-ambar">${_fmtNum(st.comprometido, 0)} comp.</span>` : ''}
       ${st.devengado > 0 ? `<span class="pd-stock-chip t-ambar">${_fmtNum(st.devengado, 0)} deveng.</span>` : ''}`;
  const saldoTxt = l.saldo === null ? '—' : _fmtNum(l.saldo, 2);
  const facturadoTxt = l.facturado === null ? '—' : _fmtNum(l.facturado, 2);
  const avisos = (l.avisos || []).map(a => `<div class="pd-aviso-linea">${_esc(a)}</div>`).join('');
  const puedeAutorizar = l.estado === 'sin_saldo' || l.estado === 'excede_saldo';
  const autorizarBtn = puedeAutorizar
    ? `<button type="button" class="pd-btn-autorizar" onclick="_pdEntregarIgual(this, ${docId}, '${_escAttr(l.sku)}', '${_escAttr(l.nombre)}')">Entregar igual…</button>`
    : (l.estado === 'autorizado'
        ? `<button type="button" class="pd-btn-quitar-autorizacion" onclick="_pdQuitarAutorizacion(this, ${docId}, '${_escAttr(l.sku)}')" title="Quitar autorización">Quitar autorización</button>`
        : '');
  const pesoTxt = (l.peso_total != null && l.a_retirar > 0)
    ? `${_fmtNum(l.peso_unit, 2)} × ${_fmtNum(l.a_retirar, 2)} = ${_fmtNum(l.peso_total, 1)} kg`
    : '—';
  const volTxt = (l.vol_total != null && l.a_retirar > 0) ? `${_fmtNum(l.vol_total, 3)} m³` : '—';
  return `<div class="pd-row t-${l.tono}" data-doc-id="${docId}" data-sku="${_escAttr(l.sku)}">
    <div class="pd-check">
      <input type="checkbox" ${checked ? 'checked' : ''} ${disabled ? 'disabled' : ''}
             aria-label="Retirar ${_escAttr(l.nombre)}"
             onchange="_pdOnChange(this, ${docId})">
      <div class="pd-qty">
        <button type="button" tabindex="-1" onclick="_pdStep(this, -1)" ${disabled ? 'disabled' : ''} aria-label="Una menos">−</button>
        <input type="number" min="0" step="0.01" value="${_fmtNum(l.a_retirar, 2)}" ${disabled ? 'disabled' : ''}
               oninput="_pdOnChange(this, ${docId})">
        <button type="button" tabindex="-1" onclick="_pdStep(this, 1)" ${disabled ? 'disabled' : ''} aria-label="Una más">+</button>
      </div>
    </div>
    <div class="pd-info">
      <span class="pd-nombre">${_esc(l.nombre || l.sku)}</span>
      <span class="pd-sub">SKU ${_esc(l.sku)} · ${nulido}bodega ${_esc(l.bodega || '02')}</span>
      ${consumos ? `<div class="pd-guias">${consumos}</div>` : ''}
    </div>
    <div class="pd-nums">
      <span><b>${facturadoTxt}</b> facturado</span>
      <span class="${l.saldo !== null && l.saldo <= 0 ? 't-gris' : 't-ok'}"><b>${saldoTxt}</b> saldo</span>
    </div>
    <div class="pd-stock">${stockHtml}</div>
    <div class="pd-peso">
      <span>${pesoTxt}</span>
      <span class="pd-vol">${volTxt}</span>
    </div>
    <div class="pd-estado">
      <span class="pd-badge t-${l.tono}"><i class="bi ${_PD_ICO[l.estado] || 'bi-dash-circle'}"></i>${_esc(l.estado_txt)}</span>
      ${autorizarBtn}
    </div>
    ${avisos}
  </div>`;
}

const _PD_ICO = {
  ok: 'bi-check-lg', revisar: 'bi-exclamation-triangle-fill', autorizado: 'bi-shield-check',
  sin_saldo: 'bi-x-lg', excede_saldo: 'bi-x-lg', sin_stock: 'bi-x-lg', stock_insuf: 'bi-x-lg',
  entregado: 'bi-check2-circle', no_incluido: 'bi-dash-circle',
};

function _pdRenderTotales(tot, c){
  return `<div class="pd-totales">
    <span>Se retiran <b>${_fmtNum(tot.unidades, 0)} unidades</b> de este retiro</span>
    <span><b>${_fmtNum(tot.peso_total_kg, 1)} kg</b><small>peso real</small></span>
    <span><b>${_fmtNum(tot.vol_total_m3, 3)} m³</b><small>volumen</small></span>
    <span><b>${_fmtNum(tot.peso_vol_total_kg, 1)} kg</b><small>peso volumétrico</small></span>
  </div>`;
}

function _escAttr(s){ return _esc(s).replace(/'/g, '&#39;'); }

function _pdStep(btn, delta){
  const inp = btn.parentElement.querySelector('input[type="number"]');
  if (!inp || inp.disabled) return;
  const v = Math.max(0, (parseFloat(inp.value || 0) + delta));
  inp.value = v % 1 === 0 ? v : v.toFixed(2);
  inp.dispatchEvent(new Event('input', { bubbles: true }));
}

function _pdOnChange(el, docId){
  const row = el.closest('.pd-row');
  if (row){
    const cb = row.querySelector('input[type="checkbox"]');
    const qty = row.querySelector('input[type="number"]');
    // Marcar la casilla sola al tocar la cantidad (misma UX que el buscador ERP)
    if (el === qty && parseFloat(qty.value || 0) > 0 && !cb.disabled) cb.checked = true;
    if (el === cb && !cb.checked) qty.value = '0';
  }
  _pdSaveDoc(docId);
}

// Marcar/desmarcar todo (REGLA #14): toggle según si ALGUNA línea editable
// del documento está sin marcar. Reusa el guard individual de cada fila.
function _pdToggleAll(btn, docId){
  const art = btn.closest('.pd-doc');
  const boxes = [...art.querySelectorAll('.pd-row input[type="checkbox"]:not([disabled])')];
  if (!boxes.length) return;
  const marcarTodo = boxes.some(cb => !cb.checked);
  boxes.forEach(cb => {
    if (cb.checked === marcarTodo) return;
    cb.checked = marcarTodo;
    const row = cb.closest('.pd-row');
    const qty = row.querySelector('input[type="number"]');
    if (marcarTodo && parseFloat(qty.value || 0) <= 0){
      // sin cantidad guardada aún: usar lo facturado como default (mismo criterio que "sin selección" del backend)
      const facturadoTxt = row.querySelector('.pd-nums b')?.textContent;
      qty.value = facturadoTxt && facturadoTxt !== '—' ? facturadoTxt : qty.value;
    }
    if (!marcarTodo) qty.value = '0';
  });
  _pdSaveDoc(docId);
}

async function _pdEntregarIgual(btn, docId, sku, nombre){
  let motivo;
  if (_pdUltimoMotivo){
    const reusar = await ilusConfirm({
      title: 'Producto sin saldo disponible',
      message: `"${nombre}" tampoco tiene saldo. ¿Usar el mismo motivo del producto anterior?`,
      sub: `"${_esc(_pdUltimoMotivo)}"`, subHtml: true,
      okLabel: 'Usar el mismo motivo', cancelLabel: 'Escribir uno distinto',
      type: 'warning',
    });
    motivo = reusar ? _pdUltimoMotivo : await ilusPrompt({
      title: 'Producto sin saldo disponible',
      message: `"${nombre}" ya fue rebajado en el ERP (aparece como entregado o facturado de más). Explica por qué se debe entregar igual:`,
      placeholder: 'Ej: cliente reporta que nunca lo recibió, se reemplaza por garantía…',
      required: true, type: 'warning',
    });
  } else {
    motivo = await ilusPrompt({
      title: 'Producto sin saldo disponible',
      message: `"${nombre}" ya fue rebajado en el ERP (aparece como entregado o facturado de más). Explica por qué se debe entregar igual:`,
      placeholder: 'Ej: cliente reporta que nunca lo recibió, se reemplaza por garantía…',
      required: true, type: 'warning',
    });
  }
  if (!motivo) return;
  _pdUltimoMotivo = motivo;
  const row = btn.closest('.pd-row');
  row.dataset.marcadaSinSaldo = '1';
  row.dataset.motivoSinSaldo = motivo;
  const cb = row.querySelector('input[type="checkbox"]');
  cb.disabled = false;
  cb.checked = true;
  const qty = row.querySelector('input[type="number"]');
  qty.disabled = false;
  if (parseFloat(qty.value || 0) <= 0) qty.value = '1';
  await _pdSaveDoc(docId, true);
}

async function _pdQuitarAutorizacion(btn, docId, sku){
  const ok = await ilusConfirm({
    title: 'Quitar autorización', message: `"${sku}" vuelve a bloquearse hasta que se autorice de nuevo.`,
    okLabel: 'Quitar', cancelLabel: 'Cancelar',
  });
  if (!ok) return;
  const row = btn.closest('.pd-row');
  row.dataset.marcadaSinSaldo = '0';
  row.dataset.motivoSinSaldo = '';
  await _pdSaveDoc(docId, true);
}

// Guardado con debounce: junta el estado actual de TODAS las filas del
// documento (checkbox + cantidad + autorización) y hace un solo POST.
// `inmediato=true` salta el debounce (usado tras "Entregar igual…").
function _pdSaveDoc(docId, inmediato){
  clearTimeout(_pdSaveTimers[docId]);
  const run = async () => {
    const art = document.querySelector(`.pd-doc[data-doc-id="${docId}"]`);
    if (!art) return;
    const lineas = [...art.querySelectorAll('.pd-row')].map(row => {
      const sku = row.dataset.sku;
      const cb = row.querySelector('input[type="checkbox"]');
      const qty = row.querySelector('input[type="number"]');
      const out = { sku, incluida: !!(cb && cb.checked), cantidad_seleccionada: parseFloat((qty && qty.value) || 0) };
      if (row.dataset.marcadaSinSaldo !== undefined){
        out.marcada_sin_saldo = row.dataset.marcadaSinSaldo === '1';
        out.motivo_sin_saldo = row.dataset.motivoSinSaldo || '';
      }
      return out;
    });
    if (!lineas.length) return;
    try {
      const r = await fetch(`/retiros/${_RID}/docs/${docId}/lineas`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' },
        body: JSON.stringify({ lineas }),
      });
      const d = await r.json();
      if (!d.ok){
        ilusToast('⚠ No se pudo guardar: ' + (d.error || `HTTP ${r.status}`), { type: 'error' });
        return;
      }
      await refrescarDocsAsociados(_RID);
      // refrescarDocsAsociados dispara refrescarProductosDiag() internamente.
    } catch(e){
      ilusToast('⚠ Sin conexión: ' + e.message, { type: 'error' });
    }
  };
  if (inmediato) { run(); return; }
  _pdSaveTimers[docId] = setTimeout(run, 600);
}

async function refrescarDocsAsociados(rid){
  try {
    const r = await fetch(`/retiros/${rid}/docs`);
    const d = await r.json();
    if (!d.ok) return;
    const ndocs = (d.docs || []).length;
    const ncons = (d.saldo_summary && d.saldo_summary.con_saldo) || 0;
    // Daniel 2026-09-16: nOtroRut alimenta el semáforo ámbar del paso 2
    // (_refrescarEstadoPasos) — docs asociados desde un RUT distinto al
    // cliente del retiro, con motivo justificado pero pendientes de ojo.
    const nOtroRut = (d.docs || []).filter(doc => !!doc.motivo_otro_rut).length;
    // Daniel 2026-05-24: refrescar tabla 1 (docs asociados) + tabla 2 (productos)
    // 🔧 2026-09-16: la lista de chips #docsAsociadosLista se eliminó (Daniel
    // autorizó fusionarla con la tabla) — la tabla es la única fuente de verdad.
    _renderTablaDocsAsociados(d.docs || []);
    _pintarFichaV3(d);
    refrescarProductosDiag();
    // Refrescar Paso "Carga" — carga total. REESTRUCTURACIÓN 2026-09-22:
    // el strip vive UNA sola vez de forma canónica (#cargaNDocs/#cargaPeso/
    // #cargaVol/#cargaTiempo, sección "Carga") pero se referencia además
    // dentro del modal "Proponer fecha" (#iwCarga*) y del resumen previo
    // a enviar, dentro del Paso "Agenda" (#resumenDocs/#resumenCarga) —
    // se actualizan los 3 juegos de ids para que ninguno quede desfasado.
    const t = d.totales || {};
    const _pesoTxt = `${parseFloat(t.peso_real_kg||0).toFixed(1)} kg`;
    const _volTxt  = `${parseFloat(t.volumen_m3||0).toFixed(3)} m³`;
    const _tiempoTxt = `${t.tiempo_estimado_min || '—'} min`;
    const $1 = document.getElementById('cargaNDocs'); if ($1) $1.textContent = ndocs;
    const $2 = document.getElementById('cargaPeso'); if ($2) $2.textContent = _pesoTxt;
    const $3 = document.getElementById('cargaVol'); if ($3) $3.textContent = _volTxt;
    const $4 = document.getElementById('cargaTiempo'); if ($4) $4.textContent = _tiempoTxt;
    const $1b = document.getElementById('iwCargaNDocs'); if ($1b) $1b.textContent = ndocs;
    const $2b = document.getElementById('iwCargaPeso'); if ($2b) $2b.textContent = _pesoTxt;
    const $3b = document.getElementById('iwCargaVol'); if ($3b) $3b.textContent = _volTxt;
    const $4b = document.getElementById('iwCargaTiempo'); if ($4b) $4b.textContent = _tiempoTxt;
    // Línea-resumen de la tarjeta "Carga" cuando está colapsada
    const $cargaLine = document.getElementById('paso3CollapsedLine');
    if ($cargaLine) $cargaLine.textContent = `⚖️ ${_pesoTxt} · ${_volTxt} · ${_tiempoTxt}`;
    // Refrescar resumen previo a enviar (dentro del Paso "Agenda")
    const $5 = document.getElementById('resumenDocs'); if ($5) $5.textContent = `${ndocs} doc${ndocs===1?'':'s'} · ${ncons} con saldo`;
    const $6 = document.getElementById('resumenCarga'); if ($6) $6.textContent = `${_pesoTxt} · ${_volTxt} · ${_tiempoTxt}`;
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
    _refrescarEstadoPasos(ndocs, ncons, d.request_state || {}, nOtroRut);
  } catch(e){
    console.error('refrescarDocsAsociados', e);
  }
}

// FICHA v3 (Daniel 2026-09-23): al agregar o quitar facturas sin recargar,
// la cabecera negra (documento, valor, peso, volumen, peso vol.) y el bloque
// "documentos valorizados" (#docsValorWrap) se repintan con lo mismo que
// arma la plantilla. Cada id se busca con guard: si la ficha no lo tiene,
// no rompe nada del resto del JS.
function _fmtClp(v){
  if (v === null || v === undefined || v === '' || isNaN(Number(v))) return '—';
  return '$' + Math.round(Number(v)).toLocaleString('es-CL');
}
function _pintarFichaV3(d){
  try {
    const docs = d.docs || [];
    const n = docs.length;
    const t = d.totales || {};
    const v = d.valores || {};
    const _num = (x, dec) => parseFloat(x || 0).toFixed(dec).replace('.', ',');
    const set = (id, txt) => { const el = document.getElementById(id); if (el) el.textContent = txt; };
    set('heroPeso', `${_num(t.peso_real_kg, 1)} kg`);
    set('heroVol', `${_num(t.volumen_m3, 3)} m³`);
    set('heroPesoVol', `${_num(t.peso_vol_kg, 1)} kg`);
    // Valor: "—" si no hay documentos o ninguno trae valor en el ERP
    const hayValor = (v.n_docs || 0) > 0 && (v.n_sin_dato || 0) < (v.n_docs || 0);
    set('heroValorBruto', hayValor ? _fmtClp(v.bruto) : '—');
    set('heroValorNeto', hayValor ? _fmtClp(v.neto) : '—');
    // Documento principal + cuántos más
    const tile = document.getElementById('heroDocTile');
    const docTxt = document.getElementById('heroDocTxt');
    if (n > 0){
      const d0 = docs[0];
      if (docTxt) docTxt.textContent = `${String(d0.document_type || '').toUpperCase()} ${d0.document_number || ''}`.trim() + (n > 1 ? ` +${n - 1}` : '');
      if (tile) tile.classList.remove('is-falta');
    } else {
      // Sin documentos asociados: siempre en rojo; el número que escribió el
      // cliente se muestra solo como referencia (igual que la plantilla).
      const decl = tile && tile.dataset.declarado;
      const declLbl = (tile && tile.dataset.declaradoLbl) || 'Declaró:';
      if (docTxt) docTxt.textContent = decl ? `${declLbl} ${decl}` : 'Sin documento';
      if (tile) tile.classList.add('is-falta');
    }
    // Sello de la cabecera: solo cuando su estado depende de los documentos
    // (sin propuesta enviada ni agenda). Los demás estados no se tocan.
    const hero = document.getElementById('rhHero');
    const rs = d.request_state || {};
    if (hero && hero.dataset.selloDocs === '1' && !rs.proposed_date && !rs.confirmed_date){
      const sinDocs = n === 0;
      hero.classList.toggle('rh-fail', sinDocs);
      hero.classList.toggle('rh-pend', !sinDocs);
      const ico = document.getElementById('rhSelloIco');
      if (ico) ico.className = 'bi ' + (sinDocs ? 'bi-file-earmark-plus' : 'bi-send');
      set('rhSelloSig', sinDocs ? 'Siguiente: agregar la factura o boleta' : 'Siguiente: proponer fecha al cliente');
    }
    // Bloque de documentos valorizados
    const wrap = document.getElementById('docsValorWrap');
    const list = document.getElementById('docsValorList');
    const tot = document.getElementById('docsValorTot');
    if (wrap) wrap.hidden = n === 0;
    if (list){
      list.innerHTML = docs.map(doc => {
        const sub = [doc.fecha_emision, doc.cliente_nombre].filter(Boolean).map(_esc).join(' · ');
        return `<div class="rd-dv">
          <div class="rd-dv-doc">
            <span class="num">${_esc(String(doc.document_type || '').toUpperCase())} ${_esc(doc.document_number || '')}</span>
            <span class="sub">${sub}</span>
          </div>
          <div class="rd-dv-vals">
            <span class="val"><span class="k">Neto</span><span class="v">${_fmtClp(doc.valor_neto)}</span></span>
            <span class="val"><span class="k">IVA</span><span class="v">${_fmtClp(doc.valor_iva)}</span></span>
            <span class="val bruto"><span class="k">Bruto</span><span class="v">${_fmtClp(doc.valor_bruto)}</span></span>
          </div>
        </div>`;
      }).join('');
    }
    if (tot){
      if (!(v.n_docs > 0)){ tot.innerHTML = ''; }
      else {
        const ns = v.n_sin_dato || 0;
        tot.innerHTML = `<span class="lbl">Total del retiro</span>
          <span class="val"><span class="k">Neto</span><span class="v">${_fmtClp(v.neto)}</span></span>
          <span class="val"><span class="k">IVA</span><span class="v">${_fmtClp(v.iva)}</span></span>
          <span class="val bruto"><span class="k">Bruto</span><span class="v">${_fmtClp(v.bruto)}</span></span>`
          + (ns ? `<span class="aviso"><i class="bi bi-exclamation-triangle-fill"></i>${ns} documento${ns === 1 ? '' : 's'} sin valor en el ERP</span>` : '');
      }
    }
  } catch(e){
    console.error('_pintarFichaV3', e);
  }
}

// Estado de pasos (verde/rojo/gris) según docs y propuesta.
// Daniel 2026-05-24: el paso 4 SOLO se bloquea cuando ndocs===0.
// Antes exigía ncons>0 (docs con saldo verificado por ERP) y dejaba
// al operador atrapado cuando el ERP no podía verificar el saldo ZZ
// (boletas sin línea ZZ, timeout, etc.). El warning de "sin saldo
// verificado" se muestra dentro del paso pero NO bloquea.
let _p3NdocsPrev = null;   // nº de docs del último repintado (null = aún no se pinta)
function _refrescarEstadoPasos(ndocs, ncons, requestState, nOtroRut){
  // Ficha v4: última fuente de verdad del estado de agenda, para
  // _icdCambiarAgenda() (calendario potenciado) sin tener que repetir
  // el fetch — se refresca cada vez que se asocia/quita un documento.
  window._RETIROS_REQUEST_STATE = requestState || {};
  if (_p3NdocsPrev === null){
    const _p3i = document.getElementById('paso-3');
    _p3NdocsPrev = (_p3i && !_p3i.classList.contains('is-blocked')) ? ndocs : 0;
  }
  nOtroRut = nOtroRut || 0;
  const p2 = document.getElementById('paso-2');
  const p3 = document.getElementById('paso-3');
  const p4 = document.getElementById('paso-4');
  if (p2){
    // Daniel 2026-09-16: semáforo de 3 estados — ámbar (is-warn) cuando hay
    // docs pero NINGUNO con saldo confirmado, o cuando alguno viene de un
    // RUT distinto (motivo_otro_rut) pendiente de revisar; verde cuando al
    // menos uno tiene saldo y no hay "otro RUT" pendiente; gris si no hay docs.
    const isWarn = ndocs > 0 && (ncons === 0 || nOtroRut > 0);
    p2.classList.toggle('is-warn', isWarn);
    p2.classList.toggle('is-complete', ndocs > 0 && !isWarn);
    const statusLine = document.getElementById('paso2StatusLine');
    if (statusLine){
      statusLine.classList.toggle('is-warn', isWarn);
      statusLine.classList.toggle('is-ok', ndocs > 0 && !isWarn);
      if (ndocs === 0){
        statusLine.textContent = 'Aún no hay documentos asociados.';
      } else if (nOtroRut > 0){
        statusLine.textContent = `${ndocs} documento${ndocs===1?'':'s'} · atención: ${nOtroRut} de otro RUT`;
      } else {
        statusLine.textContent = `${ndocs} documento${ndocs===1?'':'s'} · ${ncons} con saldo`;
      }
    }
    // Línea-resumen de la tarjeta "Documentos" cuando está colapsada
    // (REESTRUCTURACIÓN 2026-09-22 — mismo texto que statusLine, con emoji)
    const collapsedLine = document.getElementById('paso2CollapsedLine');
    if (collapsedLine){
      const _cliente = (RETIROS_DETAIL_DATA.customerLabel || 'cliente');
      if (ndocs === 0){
        collapsedLine.textContent = '📄 Aún no hay documentos asociados.';
      } else if (nOtroRut > 0){
        collapsedLine.textContent = `📄 ${_cliente} · ${ndocs} documento${ndocs===1?'':'s'} · atención: ${nOtroRut} de otro RUT`;
      } else {
        collapsedLine.textContent = `📄 ${_cliente} · ${ndocs} documento${ndocs===1?'':'s'} · ${ncons} con saldo`;
      }
    }
    // Badge del header del paso (step-badge-ok / step-badge-warn)
    const badgeOk = p2.querySelector('.step-badge-ok');
    const badgeWarn = p2.querySelector('.step-badge-warn');
    const badgeTxt = `${ndocs} asociado${ndocs===1?'':'s'}`;
    if (ndocs === 0){
      if (badgeOk) badgeOk.remove();
      if (badgeWarn) badgeWarn.remove();
    } else if (isWarn){
      if (badgeOk) badgeOk.remove();
      const actions = p2.querySelector('.paso2-head-actions');
      let b = p2.querySelector('.step-badge-warn');
      if (!b && actions){
        b = document.createElement('span');
        b.className = 'step-badge-warn';
        actions.insertBefore(b, actions.firstChild);
      }
      if (b) b.innerHTML = `<i class="bi bi-exclamation-triangle"></i>${badgeTxt}`;
    } else {
      if (badgeWarn) badgeWarn.remove();
      const actions = p2.querySelector('.paso2-head-actions');
      let b = p2.querySelector('.step-badge-ok');
      if (!b && actions){
        b = document.createElement('span');
        b.className = 'step-badge-ok';
        actions.insertBefore(b, actions.firstChild);
      }
      if (b) b.innerHTML = `<i class="bi bi-files"></i>${badgeTxt}`;
    }
  }
  if (p3){
    // Ficha v3: igual que el render del servidor (_paso3_collapsed = sin docs).
    // Se abre solo al pasar de 0 a 1+ documentos: si la operadora la plegó
    // a mano después, no se vuelve a abrir sola.
    if (ndocs > 0 && !(_p3NdocsPrev > 0)) p3.classList.remove('is-collapsed');
    _p3NdocsPrev = ndocs;
    p3.classList.toggle('is-blocked', ndocs === 0);
    p3.classList.toggle('is-complete', ndocs > 0);  // si hay docs, totales se calcularon
    const hint = document.getElementById('paso3HintSinDocs');
    if (hint) hint.style.display = ndocs === 0 ? '' : 'none';
  }
  if (p4){
    p4.classList.toggle('is-blocked', ndocs === 0);
    p4.classList.toggle('is-complete', !!requestState.step4_done);
    // Mostrar/ocultar el botón que abre el modal de propuesta (2026-09-14:
    // antes esto mostraba/ocultaba el #iwProposeForm inline; ese form ahora
    // vive DENTRO de #modalProponerFecha, así que solo togglea el CTA que
    // lo abre — basta con tener al menos UN doc asociado).
    const cta = document.getElementById('iwProposeCta');
    if (cta){
      cta.style.display = (ndocs > 0 && !requestState.step4_done) ? '' : 'none';
    }
    // Montar el calendario SOLO si el modal ya está abierto en pantalla. Si
    // está cerrado, el listener shown.bs.modal de #modalProponerFecha se
    // encarga al abrirlo — montar el widget con el modal oculto (display:none
    // nativo de Bootstrap hasta que se muestra) mide mal el grid.
    const modalPF = document.getElementById('modalProponerFecha');
    const modalPFVisible = !!(modalPF && modalPF.classList.contains('show'));
    if (modalPFVisible && ndocs > 0 && !requestState.step4_done && !window._calMounted){
      _mountProposeCalendar();
      window._calMounted = true;
    }
    // Toggle hints inteligentes en el paso 4
    const hintBlock = document.getElementById('paso4HintBloqueo');
    if (hintBlock) hintBlock.style.display = (ndocs === 0) ? '' : 'none';
    const hintNoSaldo = document.getElementById('paso4HintSinSaldo');
    if (hintNoSaldo) hintNoSaldo.style.display = (ndocs > 0 && ncons === 0 && !requestState.step4_done) ? '' : 'none';
  }
  // FIX 2026-09-22 (reestructuración de la ficha): id="paso-5" pasó a ser
  // solo el div-ancla que agrupa las secciones 8/9 (#paso-esperando /
  // #paso-confirmacion), ya no lleva class="step-section" — togglear
  // is-complete/is-blocked ahí no pinta nada (esas clases están scopeadas a
  // .step-section en el CSS). La tarjeta real que se marca completa cuando
  // el cliente acepta (step5_done) es #paso-confirmacion.
  const pConfirmacion = document.getElementById('paso-confirmacion');
  if (pConfirmacion){
    // Paso 5 NUNCA se bloquea: el operador debe poder enviar siempre.
    pConfirmacion.classList.remove('is-blocked');
    pConfirmacion.classList.toggle('is-complete', !!requestState.step5_done);
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
  // Ficha v4: si falta alguno de estos ids (ej. retiro en estado terminal,
  // #sec-calendario ni se renderiza), no seguir — evita cortar el resto
  // del archivo, del que dependen ~30 funciones más abajo.
  if (!DATE_IN || !GRID || !SUMMARY) return;
  let _modalInstance = null;
  // Ficha v4 (Daniel 2026-09-23): "toca un bloque libre para mover este
  // retiro ahí". Solo si el operador tiene permiso 'retiros' y el retiro
  // no está en un estado terminal (mismo criterio que el backend en
  // POST /proposal — pickups_module.py).
  const CAN_CHANGE = !!(window.RETIROS_DETAIL_DATA && RETIROS_DETAIL_DATA.canChangeAgenda);

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
      const hayCupo = (estado === 'disponible' || estado === 'ocupado') && oc < mx;
      // Ficha v4: bloque LIBRE (sin dueños) con cupo → clic directo mueve
      // el retiro aquí. Con dueños pero con cupo (ocupado), se ofrece
      // "Mover este retiro aquí" dentro del modal de dueños (ver abajo) —
      // así el operador ve primero quién más hay agendado a esa hora.
      const pickable = CAN_CHANGE && !isCurrent && hayCupo && !owners.length;
      if (pickable) cls.push('is-pickable');
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
      const pickTag = pickable ? '<div class="icd-slot-pick">Cambiar aquí</div>' : '';
      return `<div class="${cls.join(' ')}" data-idx="${i}" role="button" tabindex="${(owners.length || pickable) ? 0 : -1}">
        <div class="icd-slot-hora">${_esc(hora)}</div>
        <div class="icd-slot-meta">${_esc(label)}</div>
        ${ownersLine}${pickTag}
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
    // Ficha v4: bloques libres "pickables" (sin dueños, con cupo) — clic
    // directo abre la confirmación de cambio de agenda.
    GRID.querySelectorAll('.icd-slot.is-pickable').forEach(el => {
      el.addEventListener('click', () => {
        const i = parseInt(el.dataset.idx, 10);
        const slot = slots[i];
        if (slot && typeof _icdCambiarAgenda === 'function') _icdCambiarAgenda(fecha, slot);
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
    // Ficha v4: bloque con dueños pero TODAVÍA con cupo → ofrecer mover
    // este retiro aquí sin cerrar el modal de "quién hay" (el operador
    // ve primero con quién compartiría el bloque).
    const oc = slot.ocupacion_actual != null ? slot.ocupacion_actual : (slot.ocupados || 0);
    const mx = slot.capacidad_max    != null ? slot.capacidad_max    : (slot.max || 2);
    const yaEsEste = _isThisRequestSlot(slot);
    if (CAN_CHANGE && !yaEsEste && oc < mx && slot.estado !== 'bloqueado' && slot.estado !== 'colacion'){
      MODAL_BODY.innerHTML += `<button type="button" class="icd-owner-mover" id="icdBtnMoverAqui">
        <i class="bi bi-arrow-repeat"></i>Mover este retiro a este bloque (${mx - oc} cupo${mx - oc === 1 ? '' : 's'} libre${mx - oc === 1 ? '' : 's'})
      </button>`;
      const btnMover = document.getElementById('icdBtnMoverAqui');
      if (btnMover) btnMover.addEventListener('click', () => {
        if (_modalInstance) _modalInstance.hide();
        _icdCambiarAgenda(fecha, slot);
      });
    }
    if (!_modalInstance && window.bootstrap){
      _modalInstance = new bootstrap.Modal(MODAL_EL);
    }
    if (_modalInstance) _modalInstance.show();
  }
  // Ficha v4 (Daniel 2026-09-23): confirma las consecuencias y mueve la
  // agenda de ESTE retiro al bloque tocado. Reusa enviarPropuestaWizard()
  // (mismo mecanismo que "Aceptar como propuesta") — mismo correo real,
  // mismo candado anti-doble-envío, mismo manejo de errores 409.
  window._icdCambiarAgenda = async function(fecha, slot){
    if (!CAN_CHANGE) return;
    // window._RETIROS_REQUEST_STATE se refresca en cada _refrescarEstadoPasos
    // (más al día); antes del primer refresco se usa lo que trajo el
    // servidor al cargar la página.
    const rs = window._RETIROS_REQUEST_STATE || {
      step4_done: !!(window.RETIROS_DETAIL_DATA && RETIROS_DETAIL_DATA.step4Done),
      step5_done: !!(window.RETIROS_DETAIL_DATA && RETIROS_DETAIL_DATA.step5Done),
    };
    const tf = slot.time_from || slot.hora || '';
    const tt = slot.time_to || '';
    const [y, m, d] = fecha.split('-');
    const fechaDMY = `${d}/${m}/${y}`;
    let consecuencias;
    if (rs.step5_done){
      consecuencias = 'La cita confirmada actual queda sin efecto. El retiro vuelve a esperar que el cliente acepte esta nueva fecha, y se libera el cupo anterior.';
    } else if (rs.step4_done){
      consecuencias = 'La propuesta enviada anteriormente deja de servir (el botón del correo viejo ya no confirma). Se libera ese cupo y se manda una propuesta nueva.';
    } else {
      consecuencias = 'Se le manda al cliente una propuesta de fecha para que la acepte o proponga otra — nunca queda confirmado solo.';
    }
    const email = (window.RETIROS_DETAIL_DATA && RETIROS_DETAIL_DATA.contactEmail) || '';
    if (!email){
      if (typeof ilusAlert === 'function'){
        await ilusAlert({ type:'warning', title:'Falta el correo del cliente',
          message: 'Agrega un correo en la Ficha antes de proponer una fecha — si no, el cliente nunca se entera.' });
      }
      return;
    }
    const ok = typeof ilusConfirm === 'function' ? await ilusConfirm({
      title: '¿Cambiar la agenda de este retiro?',
      message: `Mover a ${fechaDMY} · ${tf}${tt ? '–' + tt : ''}.`,
      sub: `📧 Le llega un correo a <strong>${_esc(email)}</strong> con la nueva fecha.<br>${_esc(consecuencias)}`,
      subHtml: true,
      okLabel: 'Sí, cambiar y avisar al cliente', cancelLabel: 'Cancelar',
      danger: !!rs.step5_done, type: rs.step5_done ? 'warning' : 'question',
    }) : true;
    if (!ok) return;
    const hd = document.getElementById('iwProposeHidDate');
    const ht = document.getElementById('iwProposeHidTf');
    const he = document.getElementById('iwProposeHidTt');
    const reasonInp = document.getElementById('iwProposeReason');
    if (hd) hd.value = fecha;
    if (ht) ht.value = tf;
    if (he) he.value = tt;
    if (reasonInp) reasonInp.value = 'Cambio de horario desde el calendario del día';
    if (typeof enviarPropuestaWizard === 'function') enviarPropuestaWizard();
  };
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
  // Daniel 2026-09-14: el mini-calendario mensual (widget compartido, ver
  // más abajo) sigue a los botones ‹ › Hoy — le avisamos a mano porque
  // estos handlers cambian el input por JS sin emitir 'change'.
  const _syncMonth = () => { try { if (window._icdMonthCal) window._icdMonthCal.setDate(DATE_IN.value); } catch(_){} };
  document.getElementById('icdCalPrev').addEventListener('click', () => {
    if (!DATE_IN.value) return;
    DATE_IN.value = _addDays(DATE_IN.value, -1);
    _loadDia(DATE_IN.value);
    _syncMonth();
  });
  document.getElementById('icdCalNext').addEventListener('click', () => {
    if (!DATE_IN.value) return;
    DATE_IN.value = _addDays(DATE_IN.value, 1);
    _loadDia(DATE_IN.value);
    _syncMonth();
  });
  document.getElementById('icdCalToday').addEventListener('click', () => {
    const tomorrow = new Date(Date.now() + 24*3600*1000).toISOString().slice(0,10);
    DATE_IN.value = tomorrow;
    _loadDia(tomorrow);
    _syncMonth();
  });
  document.getElementById('icdCalRefresh').addEventListener('click', () => {
    if (DATE_IN.value) _loadDia(DATE_IN.value);
  });
  _setDateBounds();
  const initial = DATE_IN.value || (new Date(Date.now() + 24*3600*1000)).toISOString().slice(0,10);
  DATE_IN.value = initial;
  _loadDia(initial);

  // Mini-calendario mensual a la izquierda de los bloques (mismo diseño que
  // /retiros/solicitar). Solo NAVEGACIÓN: clic en un día → setDate() del
  // widget → emite 'change' en #icdCalDate → _loadDia() pinta la grilla icd
  // de siempre a la derecha. La grilla propia del widget va oculta
  // (#icdMonthGrid). retiros_calendar.js carga con defer → DOMContentLoaded.
  document.addEventListener('DOMContentLoaded', function(){
    if (window._icdMonthCal || !window.IlusRetirosCalendar) return;
    if (!document.getElementById('icdMonth')) return;
    try {
      window._icdMonthCal = window.IlusRetirosCalendar.mount({
        container:      '#icdMonthGrid',
        dateInput:      '#icdCalDate',
        monthContainer: '#icdMonth',
        monthHelp:      'Clic en un día para ver sus bloques. El punto muestra la carga del día.',
        includeOwners:  true,
        allowToday:     true,
        allowCrossLunch: true,
        currentRequestId: RETIROS_DETAIL_DATA.reqId,
        enableDragSelect: false,
        enableMultiBlock: false,
        suggestedDurationMin: 30,
      });
    } catch(e){ console.warn('icd month mount', e); }
  });
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
    monthContainer: '#iwProposeMonth', // Daniel 2026-09-14: mini-calendario mensual con densidad de cupos (modal "Proponer fecha")
    monthHelp: 'Como operador puedes elegir cualquier fecha desde hoy. Si necesitas, puedes cruzar la colación (13:00–14:00) para facturas grandes.',
    // Si el API falla, mostramos el input date nativo como respaldo
    // (mismo patrón que #calFallback en el formulario público).
    onLoadFail: function(){ const fb = document.getElementById('iwProposeDateFallback'); if (fb) fb.style.display = ''; },
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
        // Bug Daniel 2026-09-14: el input oculto trae ISO (YYYY-MM-DD) y se
        // mostraba tal cual al usuario — debe verse día-mes-año.
        const _dmy = fIn.value.replace(/^(\d{4})-(\d{2})-(\d{2})$/, '$3-$2-$1');
        fechaRes.textContent = `${_dmy} · ${tfIn.value}–${ttIn.value}`;
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

// Intención de montar el calendario en cuanto se pueda (booleano evaluado
// por Jinja: hay doc asociado y aún no se propuso). Daniel 2026-05-24.
// 2026-09-14: el form (y su calendario) ahora vive DENTRO de
// #modalProponerFecha, oculto por Bootstrap hasta que se abre — así que ya
// NO montamos en DOMContentLoaded (mediría mal el grid con el modal oculto).
// El montaje real ocurre en el listener shown.bs.modal de más abajo.
window._shouldMountProposeCal = RETIROS_DETAIL_DATA.shouldMountProposeCal;

// ════════════════════════════════════════════════════════════════════
//  FIX 2026-09-22 (Daniel, captura: el modal aparece descentrado, cortado
//  y flotando encima de los pasos en vez de como overlay normal) —
//  #modalProponerFecha queda anidado dentro de .form-card (el card
//  glassmorphism que envuelve TODO el wizard "Gestionar retiro" — tiene
//  backdrop-filter:blur(20px) en su CSS). backdrop-filter (igual que
//  transform/filter/perspective) crea un containing block nuevo para
//  position:fixed: el modal de Bootstrap deja de fijarse contra el
//  viewport y pasa a fijarse contra .form-card, que mide tan alto como
//  TODO el wizard (~2300px) — por eso se veía centrado respecto a ESE
//  alto en vez del viewport real, cortando el header y el calendario.
//  Se originó el 2026-09-14 al convertir el form inline (que SÍ vivía
//  bien dentro de .form-card) en modal, sin sacarlo de ahí.
//  Fix: reparentar el modal a <body> apenas el script corre (defer, DOM
//  ya listo) — antes de que Bootstrap lo muestre por primera vez.
// ════════════════════════════════════════════════════════════════════
(function _fixModalProponerFechaContainingBlock(){
  const modalPF = document.getElementById('modalProponerFecha');
  if (modalPF && modalPF.parentElement !== document.body){
    document.body.appendChild(modalPF);
  }
})();

// ════════════════════════════════════════════════════════════════════
//  MODAL "Proponer fecha y hora" (2026-09-14) — montaje del calendario
//  ────────────────────────────────────────────────────────────────────
//  Mismo patrón que #modalNuevoRetiroInterno en internal_dashboard.html
//  (commit 6cd95ea1): el widget IlusRetirosCalendar se monta SOLO cuando
//  el modal ya está visible (shown.bs.modal), nunca antes — mide mal el
//  grid si el contenedor está en display:none. Si ya estaba montado
//  (reabrir el modal), solo recarga cupos con .reload().
// ════════════════════════════════════════════════════════════════════
(function(){
  const modalPF = document.getElementById('modalProponerFecha');
  if (!modalPF) return;
  modalPF.addEventListener('shown.bs.modal', function(){
    try {
      const hasClientCard = !!document.getElementById('iwClientCard');
      const forceManual = !!window._iwForceManualOnOpen;
      window._iwForceManualOnOpen = false;
      if (!window._calMounted){
        // Sin card "el cliente pidió" → el picker manual ya está visible de
        // entrada, se monta de inmediato (mismo criterio que antes tenía
        // el DOMContentLoaded). Con card, el montaje sigue siendo lazy (al
        // pulsar "Modificar" — ver más arriba) salvo que "Re-proponer" haya
        // forzado ir directo al picker manual.
        if (!hasClientCard || forceManual){
          _mountProposeCalendar();
          window._calMounted = true;
        }
      } else if (window._proposeCalInstance && window._proposeCalInstance.reload){
        window._proposeCalInstance.reload();  // refrescar cupos al reabrir
      }
    } catch(e){ console.warn('propose calendar mount', e); }
  });
})();

// Re-proponer — Daniel 2026-09-14: ahora ABRE EL MODAL (antes revelaba el
// form inline en la página). El resumen "Propuesta enviada" (#iwStep4Summary)
// se queda visible en la página detrás del modal — no hace falta ocultarlo,
// la página se recarga sola al enviar con éxito (enviarPropuestaWizard).
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
    // Re-proponer = elegir nueva fecha → revelar el picker manual (bypassa
    // la card "el cliente pidió", si la hay) y forzar el montaje al abrir.
    const picker = document.getElementById('iwManualPicker');
    if (picker) picker.style.display = '';
    window._iwForceManualOnOpen = true;
    const modalPF = document.getElementById('modalProponerFecha');
    if (modalPF && window.bootstrap){
      bootstrap.Modal.getOrCreateInstance(modalPF).show();
    }
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
//  MARCAR PROPUESTA COMO ACEPTADA MANUALMENTE (Daniel 2026-09-23)
//  Atajo para cuando el cliente confirma por un canal que NO es el link
//  del correo (llamada, WhatsApp, presencial). Motivo obligatorio — mismo
//  patrón que motivo_otro_rut / motivo_sin_saldo: queda con nombre y hora.
//  POST /retiros/<rid>/marcar-aceptada-manual → {ok,message,actor,en,motivo,confirmed,redirect_url}
// ════════════════════════════════════════════════════════════════════
async function marcarAceptadaManual(btn){
  const motivo = await ilusPrompt({
    title: 'Marcar como aceptada',
    message: 'Explica cómo confirmó el cliente (ej. "llamó y confirmó por teléfono").',
    sub: 'Queda registrado con tu nombre y la hora — el cliente no hizo click en el correo.',
    placeholder: 'Ej: cliente llamó y confirmó verbalmente',
    required: true,
  });
  if (!motivo) return;
  const _orig = btn ? btn.innerHTML : '';
  if (btn){
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Registrando…';
  }
  try {
    const fd = new FormData();
    fd.append('motivo', motivo);
    const tok = document.querySelector('#iwProposeFormEl input[name=csrf_token]') ||
                document.querySelector('input[name=csrf_token]');
    if (tok) fd.append('csrf_token', tok.value);
    const r = await fetch(`/retiros/${_RID}/marcar-aceptada-manual`, {
      method: 'POST',
      headers: { 'X-Requested-With': 'XMLHttpRequest', 'Accept': 'application/json' },
      body: fd, credentials: 'same-origin',
    });
    const d = await r.json().catch(() => ({ ok:false, error:'Respuesta inválida del servidor.' }));
    if (!r.ok || !d.ok){
      if (d && d.code === 'MOTIVO_REQUERIDO'){
        await ilusAlert({ title:'Falta el motivo', message: d.detalle || d.error, type:'warning' });
      } else {
        await ilusAlert({ title:'No se pudo registrar', message: (d && d.error) || ('Error HTTP ' + r.status), type:'error' });
      }
      return;
    }
    ilusToast('✓ ' + (d.message || 'Marcado como aceptado'), { type:'success' });
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
    // 2026-09-14: el calendario ahora vive DENTRO del modal — scrollear
    // #paso-4 (la página, detrás del modal) ya no sirve de nada. Scrolleamos
    // al calendario dentro del propio modal-dialog-scrollable.
    ilusToast('Selecciona día y bloque horario en el calendario antes de enviar.', { type:'warning' });
    const _calEl = document.getElementById('iwProposeGrid');
    if (_calEl && _calEl.scrollIntoView) _calEl.scrollIntoView({behavior:'smooth', block:'center'});
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
    // 2026-09-14: el form vive dentro de #modalProponerFecha — cerrarlo tras
    // un envío exitoso (registrado en ERP, salga o no el correo) para que el
    // operador no se quede mirando el modal mientras la página recarga
    // detrás. La recarga (abajo) igual refleja el estado nuevo.
    try {
      const _modalPF = document.getElementById('modalProponerFecha');
      const _modalPFInst = _modalPF && window.bootstrap ? bootstrap.Modal.getInstance(_modalPF) : null;
      if (_modalPFInst) _modalPFInst.hide();
    } catch(_){}
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
//  FICHA DEL RETIRO — "USAR DATOS DEL CLIENTE COMO CONTACTO DE RETIRO"
//  Daniel 2026-05-23: "el nombre del cliente o la razón social cambie
//  con la asignación del producto. Entonces, no te dejes llevar mucho
//  por bloquear lo que dice el cliente"
//
//  REESTRUCTURACIÓN 2026-09-22 (Daniel, "Sí, constrúyela tal cual"):
//  antes existían DOS editores de razón social — el botonera propia del
//  viejo "Paso 1" (pa1EditNombre/pa1GuardarNombre, POST /customer) y el
//  campo autoguardado de la Ficha del retiro (data-inline-edit=
//  "customer_name", PATCH /field, whitelist _PICKUP_INLINE_FIELDS en
//  pickups_module.py — ya soportaba customer_name). Quedó UNA sola
//  instancia editable: la de la Ficha (persistente, nunca se colapsa).
//  Se eliminaron pa1EditNombre/pa1CancelarEdicion/pa1GuardarNombre (ya
//  no hay markup #pa1NombreDisplay/#pa1NombreInput/#pa1*Btn que los
//  use). La tarjeta de solo-lectura "Quien retira" del viejo Paso 1
//  (#pa1RetiraNombre/#pa1RetiraRut) también se fusionó en la Ficha —
//  esta función ahora actualiza los campos data-inline-edit de la
//  Ficha directamente (pickup_person_name/pickup_person_rut; el
//  backend /customer con usar_cliente_como_contacto=true NO copia
//  teléfono, solo nombre+RUT — ver pickup_actualizar_cliente).
// ════════════════════════════════════════════════════════════════════
async function pa1UsarClienteComoContacto(checked){
  if (!checked) return;
  const ok = await ilusConfirm({
    title: 'Usar datos del cliente como contacto',
    message: '¿Reemplazar la persona que retira con los datos del cliente?',
    sub: 'El dueño del documento queda como contacto oficial. Esto se registra en el historial.',
    okLabel: 'Sí, copiar', cancelLabel: 'No, cancelar',
  });
  if (!ok){
    const chk = document.getElementById('pa1UsarComoContacto');
    if (chk) chk.checked = false;
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
      const chk = document.getElementById('pa1UsarComoContacto');
      if (chk) chk.checked = false;
      return;
    }
    // Refrescar la Ficha (única fuente editable) con los nuevos datos.
    // Se actualiza también dataset.inlineOriginal para que setupInlineEdit
    // no lo trate como "cambio sin guardar" del operador.
    const _map = {
      pickup_person_name: d.pickup_person_name || '',
      pickup_person_rut:  d.pickup_person_rut  || '',
    };
    Object.keys(_map).forEach(field => {
      const el = document.querySelector(`[data-inline-edit="${field}"]`);
      if (!el) return;
      el.textContent = _map[field];
      el.dataset.inlineOriginal = _map[field];
      delete el.dataset.inlineDirty;
    });
    ilusToast('Datos del cliente aplicados como contacto', { type:'success' });
  } catch(e){
    ilusToast('Error de red: ' + e.message, { type:'error' });
    const chk = document.getElementById('pa1UsarComoContacto');
    if (chk) chk.checked = false;
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
  // 🆕 Daniel 2026-09-16: último motivo declarado para una línea sin saldo,
  // ofrecido para reutilizar en el resto del lote (mismo patrón que _TKA en
  // el modal de Tickets, _tkaPedirMotivoSinSaldo).
  ultimoMotivoSinSaldo: '',
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
  _RBA.ultimoMotivoSinSaldo = '';

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

  // Focus input según tab activo. FIX Daniel 2026-05-23 (y 2026-09-16, tras
  // reportar el campo vacío): el RUT del cliente actual (ej. "25.547.065-5")
  // se pre-pobla en el tab "Por RUT/nombre" SIN dígito verificador ni puntos
  // (ej. "25547065" — "despreocúpate por el código verificador", el
  // backend igual lo trata como cuerpo). El bug: esta pre-carga SOLO vivía
  // acá, gateada a `_RBA.tab !== 'doc'` — pero _rbaResetEstado() siempre
  // deja _RBA.tab='doc' al abrir, así que el operador SIEMPRE llega con la
  // pestaña "Por documento" activa y nunca disparaba este branch; recién se
  // notaba vacío al cambiar de pestaña a mano con rbaSetTab('cli'), que no
  // tenía esta lógica. Ahora es una función propia, llamada desde AMBOS
  // lugares (acá, por si algún día se abre directo en 'cli', y desde
  // rbaSetTab).
  setTimeout(()=>{
    if (_RBA.tab === 'doc'){
      const i = document.getElementById('rbaDocNudo');
      if (i) i.focus();
    } else {
      _rbaPrefillCliRut();
    }
  }, 350);
}
function _rbaPrefillCliRut(){
  const i = document.getElementById('rbaCliQ');
  if (!i) return;
  // Pre-poblar SOLO si está vacío (no sobrescribir si el operador ya escribió)
  if (!i.value && _RUT_CLI){
    const clean = String(_RUT_CLI).replace(/[.\-\s]/g, '');
    // Si >= 8 chars asumimos que el último es DV y lo quitamos
    const rutBase = clean.length >= 8 ? clean.slice(0, -1) : clean;
    i.value = rutBase;
    // Auto-buscar al llegar al tab (sin esperar Enter)
    setTimeout(()=> rbaBuscarPorCliente(), 100);
  }
  i.focus();
  i.select();
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
  // FIX Daniel 2026-09-16: al cambiar A "Por RUT/nombre" con un clic (el
  // camino real, ya que el modal siempre abre en "Por documento"),
  // adelantarse con el RUT del cliente del retiro — mismo criterio que
  // rbaOpen()/_rbaPrefillCliRut: sin DV, solo si el campo sigue vacío.
  if (tab === 'cli') _rbaPrefillCliRut();
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
    // FIX 2026-08-17-b (Daniel, espejo exacto del arreglo en
    // tickets/_tka_modal.html): "si está cargado positivamente quiero que
    // lo seleccione para avanzar... si hay 1 en stock y está comprometido,
    // puede ser por el mismo documento que estoy cotizando... yo veré si
    // avanzo o no -- solo selecciónalo si hay al menos 1 y notifica que
    // está comprometido". SOLO 'sin' (0 físico) deja la línea sin marcar;
    // 'comprometido' (físico >= 1) SÍ se pre-selecciona, con el aviso ámbar
    // siempre visible. Random no expone a qué documento pertenece el
    // "comprometido" (STOCNV1 es un contador global), así que no se puede
    // confirmar en código si es el mismo documento -- se avisa igual.
    const stockEstado = _rbaStockEstado(l);
    const sinStockBod = stockEstado === 'sin';
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

// 🆕 Daniel 2026-09-16: "es peligroso... si una persona está generando algo
// que ya tiene una guía y que se entregó" -- justificación obligatoria con
// trazabilidad (quién/cuándo/por qué) al incluir una línea sin saldo ERP.
// Espejo exacto de _tkaPedirMotivoSinSaldo (templates/tickets/_tka_modal.html):
// autoReuse=true (usado solo desde el flujo de selección masiva) ofrece
// reutilizar el motivo del producto anterior sin volver a preguntar "¿reusar?"
// -- fuera de ese flujo (click manual línea por línea) SIEMPRE se confirma.
async function _rbaPedirMotivoSinSaldo(nombre, autoReuse){
  if (autoReuse && _RBA.ultimoMotivoSinSaldo){
    return _RBA.ultimoMotivoSinSaldo;
  }
  if (_RBA.ultimoMotivoSinSaldo){
    const reusar = await ilusConfirm({
      title: 'Producto sin saldo disponible',
      message: `"${nombre}" tampoco tiene saldo. ¿Usar el mismo motivo declarado para los equipos anteriores?`,
      sub: `"${_esc(_RBA.ultimoMotivoSinSaldo)}"`,
      subHtml: true,
      okLabel: 'Usar el mismo motivo', cancelLabel: 'Escribir uno distinto',
      type: 'warning',
    });
    if (reusar) return _RBA.ultimoMotivoSinSaldo;
  }
  const motivo = await ilusPrompt({
    title: 'Producto sin saldo disponible',
    message: `"${nombre}" ya fue rebajado del sistema (aparentemente ya tiene guía/entrega). Explica por qué se debe incluir igual:`,
    placeholder: 'Ej: cliente reporta que nunca llegó, se reemplaza por garantía…',
    required: true, type: 'warning',
  });
  if (motivo) _RBA.ultimoMotivoSinSaldo = motivo;
  return motivo;
}
// 2026-09-16 (Daniel): antes este botón saltaba a propósito las líneas sin
// saldo -- ahora las incluye pidiendo motivo (secuencial, no en paralelo,
// para que "pedir motivo, ofrecer reutilizarlo en las siguientes" funcione
// igual que si el usuario las marcara una por una). Sin stock en bodega 02
// sigue excluido del "todas" -- es un problema de bodega, no de saldo ERP.
async function rbaToggleAllDoc(checked){
  const inputs = Array.from(document.querySelectorAll('#rbaDocResult input[type="checkbox"][data-line-key]'));
  let nAffected = 0;
  for (const inp of inputs){
    if (inp.disabled) continue;
    const sinStockBod = inp.dataset.sinStock === '1';
    if (checked && sinStockBod) continue;
    if (inp.checked === checked) continue;  // ya está como queremos
    inp.checked = checked;
    await rbaToggleLineaDoc(inp, /*silent=*/false, /*autoReuse=*/true);
    nAffected++;
  }
  console.log('[rba] toggleAll:', checked, '— líneas afectadas:', nAffected);
}
async function rbaToggleLineaDoc(inp, silent, autoReuse){
  const chkBox = inp.closest('.rba-chk');
  const row = inp.closest('.rba-line');
  const key = inp.dataset.lineKey;
  const sku = inp.dataset.sku;
  const nombre = inp.dataset.nombre;
  const isZero = inp.dataset.isZero === '1';
  const qtyInp = document.querySelector(`#rbaDocResult input.ln-qty-input[data-line-key="${key}"]`);
  let motivo = (_RBA.selDoc.get(key) || {}).motivo_sin_saldo || '';
  // Daniel 2026-05-24 → 2026-09-16: si la línea NO tiene saldo, ya no basta
  // el aviso amable -- se pide motivo obligatorio (con trazabilidad) antes
  // de dejarla marcada. silent=true (auto-poblado post-render) no pregunta.
  if (inp.checked && isZero && !silent){
    motivo = await _rbaPedirMotivoSinSaldo(nombre, autoReuse);
    if (!motivo){ inp.checked = false; return; }
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
      // 🆕 Daniel 2026-09-16: motivo obligatorio + trazabilidad (backend
      // agrega quién/cuándo) -- ver _apply_lineas_seleccion_inline.
      motivo_sin_saldo: isZero ? motivo : '',
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
      // 2026-08-17-b: solo 'sin' (0 físico) deja la línea sin marcar.
      const stockEstado = _rbaStockEstado(l);
      const sinStockBod = stockEstado === 'sin';
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
async function rbaToggleLineaCli(inp, silent, autoReuse){
  const key = inp.dataset.lineKey;
  const isZero = inp.dataset.isZero === '1';
  const nombre = inp.dataset.nombre;
  let motivo = (_RBA.selCli.get(key) || {}).motivo_sin_saldo || '';
  // Daniel 2026-05-24 → 2026-09-16: motivo obligatorio (mismo criterio que
  // rbaToggleLineaDoc, ver _rbaPedirMotivoSinSaldo) al marcar una línea sin
  // saldo. silent=true desactiva el prompt (usado por auto-poblado post-render).
  if (inp.checked && isZero && !silent){
    motivo = await _rbaPedirMotivoSinSaldo(nombre, autoReuse);
    if (!motivo){ inp.checked = false; return; }
    const qtyInpZ = document.querySelector(`input.ln-qty-input[data-line-key="${key}"]`);
    if (qtyInpZ && parseFloat(qtyInpZ.value || 0) === 0) qtyInpZ.value = 1;
  }
  const chkBox = inp.closest('.rba-chk');
  if (chkBox) chkBox.classList.toggle('is-checked', inp.checked);
  const row = inp.closest('.rba-line');
  if (row) row.classList.toggle('is-selected', inp.checked);
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
      // 🆕 Daniel 2026-09-16: motivo obligatorio + trazabilidad.
      motivo_sin_saldo: isZero ? motivo : '',
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
// ════════════════════════════════════════════════════════════════════
//  Daniel 2026-09-16 — motivo obligatorio para asociar un documento de
//  un RUT distinto al cliente del retiro.
//  ─────────────────────────────────────────────────────────────────
//  Único punto de entrada para POST /retiros/<rid>/docs/agregar desde los
//  3 call sites del modal RBA (rbaAsociarDocCompleto, rbaAsociarTodasConSaldo,
//  rbaAsociarSeleccion). Si el backend responde 409 {code:"MOTIVO_REQUERIDO"}
//  (documento a nombre de un RUT distinto al del cliente de este retiro),
//  pide el motivo con ilusPrompt (REGLA #1 — nunca prompt() nativo) y
//  reintenta el MISMO POST agregando motivo_otro_rut. Si el operador
//  cancela el prompt, devuelve {ok:false, cancelled:true} SIN reintentar —
//  el caller debe tratarlo como "no hacer nada" (sin toast de error).
// ════════════════════════════════════════════════════════════════════
async function _rbaPostDocsAgregar(body){
  const doPost = (payload) => _fetchJsonSafe(`/retiros/${_RID}/docs/agregar`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
  let d = await doPost(body);
  if (d._http_status === 409 && d.code === 'MOTIVO_REQUERIDO'){
    const docLabel = `${_esc(d.doc_cliente_nombre || 'el documento')} (${_esc(d.doc_cliente_rut || '—')})`;
    const reqLabel = `${_esc(d.req_cliente_nombre || 'el cliente de este retiro')} (${_esc(d.req_cliente_rut || '—')})`;
    const motivo = await ilusPrompt({
      title: 'Documento de otro cliente',
      message: `Este documento figura a nombre de ${docLabel}, distinto de ${reqLabel}. Indica el motivo para asociarlo igual:`,
      placeholder: 'Ej: el cliente autorizó el retiro conjunto...',
      required: true,
    });
    if (!motivo){
      return { ok: false, cancelled: true };
    }
    d = await doPost(Object.assign({}, body, { motivo_otro_rut: motivo }));
  }
  return d;
}

async function rbaAsociarDocCompleto(idx){
  const doc = ((_RBA.loaded.cliQuery && _RBA.loaded.cliQuery.docs) || [])[idx];
  if (!doc) return;
  // Atajo: usar el endpoint existente que asocia el doc entero
  try {
    const d = await _rbaPostDocsAgregar({ document_type: doc.tido_display, document_number: doc.nudo_display });
    if (d.cancelled) return;
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
  // Daniel 2026-09-16: cancelN cuenta los docs que el operador saltó porque
  // canceló el prompt de motivo (RUT distinto) — el lote sigue con el resto,
  // no se aborta (REGLA de negocio: "seguir con los demás sin abortar todo").
  let okN = 0, dupN = 0, errN = 0, cancelN = 0, i = 0;
  for (const doc of objetivo){
    i++;
    if (btn) btn.innerHTML = `<span class="spinner-border spinner-border-sm me-1"></span>Asociando ${i}/${objetivo.length}…`;
    try {
      const d = await _rbaPostDocsAgregar({ document_type: doc.tido_display, document_number: doc.nudo_display });
      if (d.cancelled){ cancelN++; continue; }
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
  if (cancelN) msg += ` · ${cancelN} omitida(s) (sin motivo)`;
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
          motivo_sin_saldo: it.motivo_sin_saldo || '',
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
      motivo_sin_saldo: it.motivo_sin_saldo || '',
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
  const cancelList = []; // docs omitidos: operador canceló el motivo de "otro RUT"
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
          // 🆕 Daniel 2026-09-16: motivo obligatorio + trazabilidad (el
          // backend agrega quién/cuándo — ver _apply_lineas_seleccion_inline).
          motivo_sin_saldo: ln.motivo_sin_saldo || '',
        }));
      // 🆕 Daniel 2026-09-16: pasa por _rbaPostDocsAgregar — si el doc es de
      // un RUT distinto al del cliente del retiro, pide el motivo con
      // ilusPrompt y reintenta sola con motivo_otro_rut adentro.
      const d = await _rbaPostDocsAgregar({
        document_type: info.tido,
        document_number: info.nudo,
        lineas: lineas_payload,  // 🆕 selección granular en mismo POST
      });
      if (d.cancelled){
        cancelList.push(label);
        continue;
      }
      if (d._not_json){
        errList.push({ label, motivo: `Backend respondió HTML (status ${d._http_status}). ¿Sesión expirada?` });
        continue;
      }
      if (d._http_status === 409 || d.code === 'DUPLICATE'){
        // Caso DUPLICATE sin líneas → 409 estándar (mantiene compat).
        // Si vinieron líneas, el backend lo procesa como UPDATE silencioso
        // y devuelve 200 con duplicate_updated=true (no llega acá).
        dupList.push(label);
        continue;
      }
      if (!d.ok){
        const motivo = (d.error || `HTTP ${d._http_status}`).toString().substring(0, 140);
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
  const cancelN = cancelList.length;

  if (okN){
    let msg = `✓ ${okN} documento${okN===1?'':'s'} asociado${okN===1?'':'s'}`;
    if (dupN) msg += ` · ${dupN} ya estaban`;
    if (cancelN) msg += ` · ${cancelN} omitido${cancelN===1?'':'s'} (sin motivo)`;
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
  } else if (cancelN && !errN && !dupN){
    // El operador canceló el/los prompt(s) de motivo — no hay error real que mostrar.
    ilusToast(`Operación cancelada: no se ingresó motivo para ${cancelN} documento${cancelN===1?'':'s'} de otro RUT.`, {
      type:'info', duration: 5000
    });
  } else if (errN){
    // Hubo al menos un error real — mostrar TODOS los motivos
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
    // El mismo campo aparece en más de un lugar (el correo, dos veces) y el
    // diálogo "Cambiar aquí" leía el correo de cuando cargó la página: todo se
    // sincroniza con lo recién guardado (2026-09-24).
    document.querySelectorAll('[data-inline-edit="' + field + '"]').forEach(o => {
      if (o !== el && o.dataset.inlineDirty !== '1'){
        o.textContent = d.value;
        o.dataset.inlineOriginal = d.value;
      }
    });
    if (field === 'contact_email'){
      if (window.RETIROS_DETAIL_DATA) window.RETIROS_DETAIL_DATA.contactEmail = d.value;
      document.querySelectorAll('a.rh-btn.mail, a.btn-2027[href^="mailto:"]').forEach(a => {
        a.href = 'mailto:' + d.value;
      });
    }
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
    // Correos: se guardan SOLO al salir del campo o con Enter. Con el guardado
    // por pausa, "maria@hotmail" a medio escribir iba al servidor, volvía 400 y
    // el valor viejo se reponía mientras el operador seguía tecleando (2026-09-24).
    const _soloAlSalir = ['contact_email', 'extra_emails'].indexOf(el.dataset.inlineEdit) !== -1;
    el.addEventListener('input', () => {
      el.dataset.inlineDirty = '1';
      const ind = _ensureInlineIndicator(el);
      ind.innerHTML = _soloAlSalir
        ? '<i class="bi bi-pencil-fill"></i> escribiendo… (se guarda al presionar Enter o salir)'
        : '<i class="bi bi-pencil-fill"></i> escribiendo…';
      ind.style.color = '#6b7280';
      clearTimeout(debounceTimer);
      if (!_soloAlSalir){
        debounceTimer = setTimeout(() => _saveInlineField(el), _INLINE_EDIT_DEBOUNCE_MS);
      }
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
  // 1) Diagnóstico de productos del retiro (consulta al ERP: saldo, guía, stock).
  _deferLoad(() => refrescarProductosDiag(), 800);
  // 2) Sugerencias de email (ligera).
  _deferLoad(() => cargarSugerenciasEmail(), 1200);
  // 3) Documentos con saldo en el ERP (lo más lento) — al final, en
  //    segundo plano. El operador ya tiene la ficha completa a la vista.
  _deferLoad(() => cargarSaldoCliente(_RID), 2000);

  // 4) Calendario de propuesta — 2026-09-14: el form (y su calendario) ahora
  //    vive DENTRO de #modalProponerFecha, oculto por Bootstrap hasta que se
  //    abre. Montarlo aquí (con el modal todavía en display:none) mediría
  //    mal el grid, así que el montaje se movió al listener shown.bs.modal
  //    de #modalProponerFecha (ver bloque "MODAL 'Proponer fecha y hora'"
  //    más arriba en este archivo). Nada que hacer acá.

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

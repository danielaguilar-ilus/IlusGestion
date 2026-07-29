/* transporte_courier_ficha.js
 * ---------------------------------------------------------------
 * Extraido tal cual desde templates/transporte/courier_ficha.html
 * (bloque <script> inline). Se movio a /static para que el navegador
 * lo cachee: la pagina se sirve con cache-control: no-store, asi que
 * antes se re-descargaban ~27KB de JS en cada clic.
 *
 * La configuracion que dependia de Jinja viaja en window.CFICHA,
 * definida inline en el template ANTES de cargar este archivo:
 *   window.CFICHA = { courierId: <int>, contratos: [...] };
 *
 * Es un MOVIMIENTO, no una refactorizacion: mismo codigo, mismos
 * nombres y las mismas funciones globales (los onclick="" del HTML
 * las siguen resolviendo desde window).
 * --------------------------------------------------------------- */
const COURIER_ID = CFICHA.courierId;

/* ════════════════════════
   Init modals
════════════════════════ */
let _modalContrato, _modalImportTar;
document.addEventListener('DOMContentLoaded', () => {
  _modalContrato  = new bootstrap.Modal(document.getElementById('modalContrato'));
  _modalImportTar = new bootstrap.Modal(document.getElementById('modalImportTar'));

  // Activate tab from URL hash
  const hash = location.hash;
  if (hash) {
    const tab = document.querySelector(`[data-bs-target="${hash}"]`);
    if (tab) bootstrap.Tab.getOrCreateInstance(tab).show();
  }

  // Load contratos & tarifas when tab shown
  document.querySelectorAll('[data-bs-toggle="tab"]').forEach(btn => {
    btn.addEventListener('shown.bs.tab', e => {
      const target = e.target.getAttribute('data-bs-target');
      if (target === '#tab-contratos') cargarContratos();
      if (target === '#tab-tarifas')   cargarTarifas(1);
    });
  });

  // Initial load if on contratos tab
  if (hash === '#tab-contratos') cargarContratos();
  if (hash === '#tab-tarifas')   cargarTarifas(1);
});

/* ════════════════════════
   FICHA: save
════════════════════════ */
async function guardarFicha() {
  const body = {
    nombre        : gval('f_nombre'),
    nombre_fantasia: gval('f_nfantasia'),
    rut           : gval('f_rut'),
    giro          : gval('f_giro'),
    tipo          : gval('f_tipo'),
    direccion     : gval('f_direccion'),
    telefono      : gval('f_telefono'),
    email         : gval('f_email'),
    website       : gval('f_website'),
    contacto      : gval('f_contacto'),
    contacto_cargo: gval('f_cargo'),
    factor_vol    : gval('f_factor'),
    peso_max_bulto: gval('f_pmax_bulto'),
    peso_max_guia : gval('f_pmax_guia'),
    notas         : gval('f_notas'),
    activo        : document.getElementById('f_activo')?.checked,
  };
  if (!body.nombre.trim()) { showAlert('fichaAlert', 'El nombre es obligatorio.', 'danger'); return; }
  const r = await fetch(`/transporte/couriers/${COURIER_ID}`, {
    method: 'PUT', headers: {'Content-Type':'application/json'},
    body: JSON.stringify(body)
  });
  if (r.ok) {
    showAlert('fichaAlert', '<i class="bi bi-check-circle me-2"></i>Cambios guardados correctamente.', 'success');
    // Update header name
    document.querySelector('h3.fw-bold').textContent = body.nombre;
  } else {
    showAlert('fichaAlert', 'Error al guardar. Intenta de nuevo.', 'danger');
  }
}

/* ════════════════════════
   ACTIVE TOGGLE (header)
════════════════════════ */
async function cambiarActivo(val) {
  const r0 = await fetch(`/transporte/couriers/${COURIER_ID}/api`);
  const c  = await r0.json();
  await fetch(`/transporte/couriers/${COURIER_ID}`, {
    method: 'PUT', headers: {'Content-Type':'application/json'},
    body: JSON.stringify({...c, activo: val})
  });
  const lbl = document.querySelector('label[for="toggleActivo"]');
  if (lbl) lbl.textContent = val ? 'Courier activo' : 'Courier inactivo';
}

/* ════════════════════════
   CONTRATOS
════════════════════════ */
let _contratoEditId = null;

async function cargarContratos() {
  const el = document.getElementById('listaContratos');
  el.innerHTML = '<div class="text-center py-4"><div class="spinner-border spinner-border-sm text-danger"></div></div>';

  const r = await fetch(`/transporte/couriers/${COURIER_ID}/api`); // reuse for courier info
  // Actually we need contracts from a separate endpoint — use the page's Jinja-rendered list for initial
  // but reload via a meta endpoint if needed. For now, build from template data + AJAX refresh.
  renderContratos(CFICHA.contratos);
}

function renderContratos(list) {
  const el = document.getElementById('listaContratos');
  if (!list || !list.length) {
    el.innerHTML = `
      <div class="text-center py-5 text-muted">
        <i class="bi bi-file-earmark-x display-4 d-block mb-3 opacity-25"></i>
        <div class="fw-semibold mb-1">Sin contratos registrados</div>
        <div class="small mb-3">Registra el contrato o tarifario vigente con el botón "Nuevo contrato".</div>
        <button class="btn btn-danger btn-sm" data-bs-toggle="modal" data-bs-target="#modalContrato"
                onclick="abrirModalContrato()">
          <i class="bi bi-plus me-1"></i>Agregar contrato
        </button>
      </div>`;
    return;
  }

  const fmt = s => s ? new Date(s).toLocaleDateString('es-CL', {day:'2-digit',month:'2-digit',year:'numeric'}) : '—';
  const tipoLabel = {contrato:'Contrato',tarifario:'Tarifario',acuerdo:'Acuerdo',otro:'Otro'};

  let html = '<div class="d-flex flex-column gap-2">';
  list.forEach(ct => {
    const now  = new Date();
    const fin  = ct.fecha_fin ? new Date(ct.fecha_fin) : null;
    const dias = fin ? Math.ceil((fin - now) / 86400000) : null;
    const isVig = ct.vigente;
    const isExp = fin && fin < now;

    let expiryBadge = '';
    if (isVig && fin) {
      if (isExp) {
        expiryBadge = `<span class="badge bg-danger ms-1">Vencido</span>`;
      } else if (dias <= 30) {
        expiryBadge = `<span class="badge bg-warning text-dark ms-1">Vence en ${dias}d</span>`;
      } else {
        expiryBadge = `<span class="badge bg-success ms-1">Vigente · ${dias}d restantes</span>`;
      }
    }

    html += `
    <div class="contrato-card ${isVig && !isExp ? 'vigente-card' : isExp ? 'vencido-card' : ''}">
      <div class="d-flex align-items-start justify-content-between gap-2">
        <div class="flex-grow-1">
          <div class="d-flex align-items-center gap-2 flex-wrap">
            <span class="fw-semibold">${esc(ct.nombre)}</span>
            <span class="badge bg-secondary small">${esc(tipoLabel[ct.tipo] || ct.tipo)}</span>
            ${isVig ? '<span class="badge bg-success small">Vigente</span>' : '<span class="badge bg-light text-dark border small">Inactivo</span>'}
            ${expiryBadge}
          </div>
          <div class="text-muted small mt-1">
            <span class="me-3"><i class="bi bi-calendar me-1"></i>${fmt(ct.fecha_inicio)} → ${fmt(ct.fecha_fin)}</span>
            ${ct.subido_por ? `<span><i class="bi bi-person me-1"></i>${esc(ct.subido_por)}</span>` : ''}
          </div>
          ${ct.descripcion ? `<div class="text-muted small mt-1">${esc(ct.descripcion)}</div>` : ''}
          ${ct.archivo_url ? `<div class="mt-1"><a href="${esc(ct.archivo_url)}" target="_blank" class="btn btn-xs btn-outline-secondary btn-sm" style="font-size:.75rem;padding:2px 8px;"><i class="bi bi-file-earmark-arrow-down me-1"></i>Ver archivo</a></div>` : ''}
        </div>
        <div class="d-flex gap-1 flex-shrink-0">
          <button class="btn btn-sm btn-outline-secondary" onclick="editarContrato(${JSON.stringify(ct)})" title="Editar">
            <i class="bi bi-pencil"></i>
          </button>
          <button class="btn btn-sm btn-outline-danger" onclick="eliminarContrato(${ct.id})" title="Eliminar">
            <i class="bi bi-trash"></i>
          </button>
        </div>
      </div>
    </div>`;
  });
  html += '</div>';
  el.innerHTML = html;
}

let _contratosCache = CFICHA.contratos;

function abrirModalContrato() {
  _contratoEditId = null;
  document.getElementById('contratoModalTitulo').innerHTML =
    '<i class="bi bi-file-earmark-plus me-2 text-danger"></i>Nuevo contrato';
  document.getElementById('ct_nombre').value  = '';
  document.getElementById('ct_tipo').value    = 'contrato';
  document.getElementById('ct_vigente').checked = true;
  document.getElementById('ct_inicio').value  = '';
  document.getElementById('ct_fin').value     = '';
  document.getElementById('ct_url').value     = '';
  document.getElementById('ct_desc').value    = '';
}

function editarContrato(ct) {
  _contratoEditId = ct.id;
  document.getElementById('contratoModalTitulo').innerHTML =
    '<i class="bi bi-pencil me-2 text-warning"></i>Editar contrato';
  document.getElementById('ct_nombre').value    = ct.nombre || '';
  document.getElementById('ct_tipo').value      = ct.tipo || 'contrato';
  document.getElementById('ct_vigente').checked = !!ct.vigente;
  document.getElementById('ct_inicio').value    = ct.fecha_inicio ? ct.fecha_inicio.slice(0,10) : '';
  document.getElementById('ct_fin').value       = ct.fecha_fin ? ct.fecha_fin.slice(0,10) : '';
  document.getElementById('ct_url').value       = ct.archivo_url || '';
  document.getElementById('ct_desc').value      = ct.descripcion || '';
  _modalContrato.show();
}

async function guardarContrato() {
  const body = {
    nombre     : gval('ct_nombre'),
    tipo       : gval('ct_tipo'),
    vigente    : document.getElementById('ct_vigente').checked,
    fecha_inicio: gval('ct_inicio') || null,
    fecha_fin  : gval('ct_fin') || null,
    archivo_url: gval('ct_url'),
    descripcion: gval('ct_desc'),
  };
  if (!body.nombre.trim()) { alert('El nombre es obligatorio'); return; }

  const url    = _contratoEditId
    ? `/transporte/couriers/${COURIER_ID}/contratos/${_contratoEditId}`
    : `/transporte/couriers/${COURIER_ID}/contratos`;
  const method = _contratoEditId ? 'PUT' : 'POST';

  const r = await fetch(url, {
    method, headers: {'Content-Type':'application/json'}, body: JSON.stringify(body)
  });
  if (r.ok) {
    _modalContrato.hide();
    // Refresh list
    const r2 = await fetch(`/transporte/couriers/${COURIER_ID}/api`);
    // We don't have a contratos endpoint returning the list, so reload the page contratos from server
    location.reload();
  } else {
    alert('Error al guardar contrato');
  }
}

async function eliminarContrato(kid) {
  const ok = await ilusConfirm({
    title: 'Eliminar contrato',
    message: '¿Eliminar este contrato del courier?',
    sub: 'Esta acción no se puede deshacer.',
    okLabel: 'Eliminar', danger: true,
  });
  if (!ok) return;
  const r = await fetch(`/transporte/couriers/${COURIER_ID}/contratos/${kid}`, {method:'DELETE'});
  if (r.ok) {
    _contratosCache = _contratosCache.filter(c => c.id !== kid);
    renderContratos(_contratosCache);
  } else {
    if (typeof ilusToast === 'function') ilusToast('Error al eliminar el contrato', { type:'error' });
    else alert('Error al eliminar');
  }
}

/* ════════════════════════
   TARIFAS
════════════════════════ */
let _tarPage  = 1;
let _tarTimer = null;

function debounceCargar() {
  clearTimeout(_tarTimer);
  _tarTimer = setTimeout(() => cargarTarifas(1), 350);
}

function limpiarFiltrosTar() {
  document.getElementById('tarSearch').value = '';
  document.getElementById('tarRegion').value = '';
  document.getElementById('tarZona').value   = '';
  cargarTarifas(1);
}

async function cargarTarifas(page) {
  _tarPage = page || 1;
  const q      = document.getElementById('tarSearch')?.value.trim() || '';
  const region = document.getElementById('tarRegion')?.value || '';
  const zona   = document.getElementById('tarZona')?.value   || '';

  const params = new URLSearchParams({ page: _tarPage });
  if (q)      params.set('q', q);
  if (region) params.set('region', region);
  if (zona)   params.set('zona', zona);

  const body = document.getElementById('tablaTarifasBody');
  body.innerHTML = `<tr><td colspan="10" class="text-center py-4">
    <div class="spinner-border spinner-border-sm text-danger"></div>
  </td></tr>`;

  const r = await fetch(`/transporte/couriers/${COURIER_ID}/comunas/paginated?${params}`);
  if (!r.ok) {
    body.innerHTML = `<tr><td colspan="10" class="text-center text-danger py-4">Error cargando tarifas.</td></tr>`;
    return;
  }
  const d = await r.json();
  renderTarifas(d);
}

function renderTarifas(d) {
  const head = document.getElementById('tablaTarifasHead');
  const body = document.getElementById('tablaTarifasBody');
  const pg   = document.getElementById('tarPagination');
  const stats= document.getElementById('tarStats');

  if (!d.rows.length) {
    head.innerHTML = '';
    body.innerHTML = `<tr><td colspan="8" class="text-center py-5 text-muted">
      <i class="bi bi-map display-5 d-block mb-2 opacity-25"></i>
      <div class="fw-semibold mb-1">Sin tarifas</div>
      <div class="small">Importa un Excel de tarifas para ver los precios por comuna.</div>
    </td></tr>`;
    pg.innerHTML = '';
    stats.textContent = '0 comunas';
    return;
  }

  // Collect all weight keys across rows (smart selection: prefer 1, 5, 10, 30, 100 kg if present)
  const allKeys = [];
  d.rows.forEach(row => {
    if (row.precios) Object.keys(row.precios).forEach(k => { if (!allKeys.includes(k)) allKeys.push(k); });
  });

  const PREFERRED = ['1','1.0','5','5.0','10','10.0','30','30.0','100','100.0'];
  let showKeys = allKeys.filter(k => PREFERRED.includes(k));
  if (showKeys.length < 3) showKeys = allKeys.slice(0, 6);

  // Header
  head.innerHTML = `<th>Código</th><th>Sucursal</th><th>Comuna</th><th>Zona</th>
    <th>Región</th><th>Días</th>
    ${showKeys.map(k => `<th class="text-end">${esc(k)} kg</th>`).join('')}
    <th class="text-center" style="width:90px">Acciones</th>`;

  // Rows con botones de validar/editar tarifa
  const fmt = n => Math.round(n).toLocaleString('es-CL');
  body.innerHTML = d.rows.map(row => `
    <tr>
      <td class="text-muted small">${esc(row.codigo||'—')}</td>
      <td class="text-muted small">${esc(row.sucursal||'—')}</td>
      <td class="fw-semibold">${esc(row.comuna)}</td>
      <td><span class="badge bg-secondary small">${esc(row.zona||'—')}</span></td>
      <td class="text-muted small">${esc(row.region||'—')}</td>
      <td class="text-center small">${esc(row.dias_transito||'—')}</td>
      ${showKeys.map(k => {
        const v = row.precios?.[k];
        return `<td class="text-end fw-semibold small" data-comuna="${esc(row.comuna)}" data-bracket="${esc(k)}">
          ${v != null
            ? `<a href="#" onclick="event.preventDefault();editarPrecio('${esc(row.comuna)}','${esc(k)}',${v})"
                 style="color:#0f172a;text-decoration:none;border-bottom:1px dashed #cbd5e1"
                 title="Click para editar este precio">$${fmt(v)}</a>`
            : `<a href="#" onclick="event.preventDefault();editarPrecio('${esc(row.comuna)}','${esc(k)}',0)"
                 style="color:#9ca3af;text-decoration:none" title="Crear este bracket">+</a>`}
        </td>`;
      }).join('')}
      <td class="text-center">
        <button class="btn btn-outline-success btn-sm py-0 px-2" style="font-size:.66rem"
                onclick="validarComuna('${esc(row.comuna)}')" title="Marcar tarifa como validada">
          <i class="bi bi-shield-check"></i>
        </button>
      </td>
    </tr>`).join('');

  // Stats
  stats.textContent = `${d.total} comunas`;

  // Pagination
  if (d.pages <= 1) { pg.innerHTML = ''; return; }
  let pgHtml = `<span class="text-muted small">Pág. ${d.page} de ${d.pages} (${d.total} total)</span>`;
  pgHtml += `<div class="d-flex gap-1">`;
  pgHtml += `<button class="btn btn-sm btn-outline-secondary" ${d.page<=1?'disabled':''} onclick="cargarTarifas(${d.page-1})"><i class="bi bi-chevron-left"></i></button>`;

  const start = Math.max(1, d.page-2);
  const end   = Math.min(d.pages, d.page+2);
  for (let i = start; i <= end; i++) {
    pgHtml += `<button class="btn btn-sm ${i===d.page?'btn-danger':'btn-outline-secondary'}" onclick="cargarTarifas(${i})">${i}</button>`;
  }
  pgHtml += `<button class="btn btn-sm btn-outline-secondary" ${d.page>=d.pages?'disabled':''} onclick="cargarTarifas(${d.page+1})"><i class="bi bi-chevron-right"></i></button>`;
  pgHtml += `</div>`;
  pg.innerHTML = pgHtml;
}

/* ════════════════════════
   PRICE CALCULATOR
════════════════════════ */
async function calcularPrecio() {
  const comuna = document.getElementById('calcComuna').value.trim();
  const peso   = parseFloat(document.getElementById('calcPeso').value) || 1;
  const el     = document.getElementById('calcResult');
  if (!comuna) { el.innerHTML = '<small class="text-warning">Ingresa una comuna.</small>'; return; }
  el.innerHTML = '<div class="spinner-border spinner-border-sm text-danger"></div>';
  const r = await fetch(`/transporte/couriers/lookup?courier_id=${COURIER_ID}&comuna=${encodeURIComponent(comuna)}&peso=${peso}`);
  const d = await r.json();
  if (d.ok) {
    const fmt = n => Math.round(n).toLocaleString('es-CL');
    el.innerHTML = `
      <div class="p-2 rounded" style="background:#d4edda;border:1px solid #28a745;">
        <div class="fw-bold text-success fs-5">$${fmt(d.precio)}</div>
        <div class="text-muted small">${peso} kg → ${esc(d.comuna_matched || comuna)}</div>
        ${d.partial_match ? `<small class="text-muted">Coincidencia parcial</small>` : ''}
      </div>`;
  } else {
    el.innerHTML = `<small class="text-danger">${esc(d.error)}</small>`;
  }
}

/* ════════════════════════
   CHOFERES (roster del courier, 2026-07-26)
   Usado para autocompletar la captura de retiro de bodega. Encadena
   ilusPrompt (nombre → RUT → teléfono opcional → patente opcional) porque
   son solo 4 campos cortos — no amerita un modal Bootstrap completo.
════════════════════════ */
async function agregarChoferCourier() {
  const nombre = await ilusPrompt({
    title: 'Agregar chofer', message: 'Nombre completo del chofer:',
    placeholder: 'Ej: Juan Pérez Soto', required: true,
  });
  if (!nombre) return;
  const rut = await ilusPrompt({
    title: 'Agregar chofer', message: 'RUT del chofer:',
    placeholder: 'Ej: 12.345.678-9', required: true,
  });
  if (!rut) return;
  const telefono = await ilusPrompt({
    title: 'Agregar chofer', message: 'Teléfono (opcional):',
    placeholder: 'Ej: +56 9 1234 5678', required: false,
  });
  const patente = await ilusPrompt({
    title: 'Agregar chofer', message: 'Patente (opcional):',
    placeholder: 'Ej: ABCD12', required: false,
  });
  // FIX 2026-07-27 (Daniel: "comparativa con el camión, para restringir el
  // manifiesto según su límite de peso/volumen"): capacidad del camión,
  // guardada en el perfil del chofer (patente = 1 camión fijo).
  const pesoMax = await ilusPrompt({
    title: 'Agregar chofer', message: 'Capacidad máxima de carga del camión, en KG (opcional):',
    placeholder: 'Ej: 2000', required: false, inputType: 'number',
    sub: 'Déjalo en blanco si no quieres restringir por peso.',
  });
  const volMax = await ilusPrompt({
    title: 'Agregar chofer', message: 'Capacidad máxima de carga del camión, en M³ (opcional):',
    placeholder: 'Ej: 12', required: false, inputType: 'number',
    sub: 'Déjalo en blanco si no quieres restringir por volumen.',
  });
  try {
    const r = await fetch(`/transporte/couriers/${COURIER_ID}/choferes`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({
        nombre, rut, telefono: telefono || '', patente: patente || '',
        peso_max_kg: pesoMax || '', volumen_max_m3: volMax || '',
      }),
    });
    const d = await r.json();
    if (d.ok) {
      ilusToast('✓ Chofer agregado', { type: 'success' });
      location.reload();
    } else {
      await ilusAlert({ title: 'No se pudo agregar', message: d.error || 'Error desconocido', type: 'error' });
    }
  } catch (e) {
    ilusToast('Error de conexión', { type: 'error' });
  }
}

async function eliminarChoferCourier(choferId) {
  const ok = await ilusConfirm({
    title: 'Quitar chofer',
    message: '¿Quitar este chofer del roster del courier?',
    sub: 'No se borra su historial de retiros, solo deja de aparecer como sugerencia.',
    okLabel: 'Quitar', cancelLabel: 'Cancelar', danger: true,
  });
  if (!ok) return;
  try {
    const r = await fetch(`/transporte/couriers/${COURIER_ID}/choferes/${choferId}`, { method: 'DELETE' });
    const d = await r.json();
    if (d.ok) {
      const row = document.getElementById('chofer-row-' + choferId);
      if (row) row.remove();
      ilusToast('✓ Chofer quitado', { type: 'success' });
    } else {
      await ilusAlert({ title: 'No se pudo quitar', message: d.error || 'Error desconocido', type: 'error' });
    }
  } catch (e) {
    ilusToast('Error de conexión', { type: 'error' });
  }
}

/* ════════════════════════
   EDICIÓN/VALIDACIÓN DE TARIFAS (con audit log)
════════════════════════ */
async function editarPrecio(comuna, bracket, precioActual) {
  const raw = await ilusPrompt({
    title: `Editar tarifa · ${comuna} · bracket ${bracket}`,
    message: 'Nuevo precio (CLP entero):',
    placeholder: 'Ej: 105237',
    defaultValue: String(precioActual || ''),
    required: true,
  });
  if (raw === null) return;
  const precio = parseInt(String(raw).replace(/[.,$\s]/g,''), 10);
  if (isNaN(precio) || precio <= 0) {
    await ilusAlert({title:'Precio inválido', message:'Debe ser un número entero positivo.', type:'warning'});
    return;
  }
  const motivo = await ilusPrompt({
    title: 'Motivo del cambio',
    message: '¿Por qué se está editando? (queda en audit log)',
    placeholder: 'Ej: actualización tarifa nov-2026',
    required: true,
  });
  if (motivo === null) return;

  try {
    const r = await fetch(`/api/transporte/couriers/${COURIER_ID}/tarifa-editar`, {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({ comuna, bracket, precio_nuevo: precio, motivo }),
    });
    const d = await r.json();
    if (!r.ok || d.error) throw new Error(d.error || 'HTTP '+r.status);
    ilusToast(`✓ Tarifa actualizada (audit #${d.audit_id})`, {type:'success'});
    cargarTarifas(_tarPage);
  } catch (e) {
    await ilusAlert({title:'Error', message:e.message, type:'error'});
  }
}

async function validarComuna(comuna) {
  const bracket = await ilusPrompt({
    title: `Validar tarifa · ${comuna}`,
    message: 'Bracket a validar (ej: 100+, 50, 25):',
    placeholder: '100+',
    required: true,
  });
  if (bracket === null) return;
  const raw = await ilusPrompt({
    title: `Precio correcto · ${comuna} · ${bracket}`,
    message: 'Precio validado contra Excel maestro (CLP):',
    placeholder: 'Ej: 105237',
    required: true,
  });
  if (raw === null) return;
  const precio = parseInt(String(raw).replace(/[.,$\s]/g,''), 10);
  if (isNaN(precio) || precio <= 0) {
    await ilusAlert({title:'Precio inválido', type:'warning', message:'Debe ser un número positivo.'});
    return;
  }
  const notas = await ilusPrompt({
    title: 'Notas (opcional)',
    message: 'Fuente o referencia:',
    placeholder: 'Excel maestro 21/05/2026',
    required: false,
  });
  if (notas === null) return;

  try {
    const r = await fetch(`/api/transporte/couriers/${COURIER_ID}/tarifa-validar`, {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({ comuna, bracket, precio_correcto: precio, notas: notas || '' })
    });
    const d = await r.json();
    if (!r.ok || d.error) throw new Error(d.error || 'HTTP '+r.status);
    ilusToast(`✓ Tarifa VALIDADA: ${comuna} · ${bracket} = $${precio.toLocaleString('es-CL')}`, {type:'success'});
    cargarTarifas(_tarPage);
  } catch (e) {
    await ilusAlert({title:'Error', message:e.message, type:'error'});
  }
}

/* ════════════════════════
   LOGOS
════════════════════════ */
async function guardarLogo(type) {
  const inputMap   = {principal:'urlPrincipal', square:'urlSquare', label:'urlLabel'};
  const alertMap   = {principal:'alertPrincipal', square:'alertSquare', label:'alertLabel'};
  const previewMap = {principal:'previewPrincipal', square:'previewSquare', label:'previewLabel'};

  const logo_url = document.getElementById(inputMap[type])?.value.trim() || '';
  const alertEl  = document.getElementById(alertMap[type]);
  const prevEl   = document.getElementById(previewMap[type]);

  const r = await fetch(`/transporte/couriers/${COURIER_ID}/logo`, {
    method: 'POST', headers: {'Content-Type':'application/json'},
    body: JSON.stringify({logo_url, logo_type: type})
  });
  if (r.ok) {
    alertEl.style.display = '';
    alertEl.innerHTML = `<div class="alert alert-success alert-sm small py-1">Guardado correctamente.</div>`;
    if (logo_url && prevEl) {
      if (prevEl.tagName === 'IMG') {
        prevEl.src = logo_url;
      } else {
        prevEl.insertAdjacentHTML('afterend', `<img src="${esc(logo_url)}" id="${previewMap[type]}" style="max-width:160px;max-height:80px;object-fit:contain;">`);
        prevEl.remove();
      }
      // Update header logo if principal
      if (type === 'principal') {
        const hl = document.getElementById('headerLogo');
        if (hl) {
          if (hl.tagName === 'IMG') hl.src = logo_url;
          else hl.insertAdjacentHTML('afterend', `<img src="${esc(logo_url)}" id="headerLogo" style="width:80px;height:80px;object-fit:contain;background:#fff;border-radius:10px;padding:8px;">`);
        }
      }
    }
    setTimeout(() => { alertEl.style.display = 'none'; }, 3000);
  } else {
    alertEl.style.display = '';
    alertEl.innerHTML = `<div class="alert alert-danger alert-sm small py-1">Error al guardar.</div>`;
  }
}

/* ════════════════════════
   IMPORT EXCEL (tarifas)
════════════════════════ */
function abrirImportTarifas() {
  document.getElementById('importResultsTar').style.display  = 'none';
  document.getElementById('importProgressTar').style.display = 'none';
  document.getElementById('importFileTar').value = '';
  _modalImportTar.show();
}

function onDropTar(e) {
  e.preventDefault();
  document.getElementById('dropZoneTar').style.borderColor = '';
  const f = e.dataTransfer.files[0];
  if (f) subirExcelTar(f);
}

async function subirExcelTar(file) {
  if (!file) return;
  document.getElementById('importProgressTar').style.display = '';
  document.getElementById('importResultsTar').style.display  = 'none';
  const fd = new FormData();
  fd.append('file', file);
  try {
    const r = await fetch('/transporte/couriers/import', { method: 'POST', body: fd });
    const d = await r.json();
    document.getElementById('importProgressTar').style.display = 'none';
    const el = document.getElementById('importResultsTar');
    el.style.display = '';
    if (!d.ok) {
      el.innerHTML = `<div class="alert alert-danger small">${esc(d.error)}</div>`;
    } else {
      el.innerHTML = `
        <div class="alert alert-success small mb-2">
          <i class="bi bi-check-circle me-1"></i>
          <strong>${d.total}</strong> comunas importadas.
        </div>
        <button class="btn btn-sm btn-outline-secondary" onclick="location.reload()">
          <i class="bi bi-arrow-clockwise me-1"></i>Recargar
        </button>`;
    }
  } catch (err) {
    document.getElementById('importProgressTar').style.display = 'none';
    document.getElementById('importResultsTar').style.display  = '';
    document.getElementById('importResultsTar').innerHTML = `<div class="alert alert-danger small">${esc(String(err))}</div>`;
  }
}

/* ════════════════════════
   HELPERS
════════════════════════ */
function esc(s){ return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;'); }
function gval(id){ return (document.getElementById(id)||{}).value||''; }
function showAlert(id, msg, type) {
  const el = document.getElementById(id);
  el.style.display = '';
  el.innerHTML = `<div class="alert alert-${type} small">${msg}</div>`;
  if (type === 'success') setTimeout(() => { el.style.display = 'none'; }, 4000);
}

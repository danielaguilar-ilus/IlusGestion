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
    // Update header name (id del <h1> del .trx-hero, ver courier_ficha.html)
    const _hdrNombre = document.getElementById('courierNombreHeader');
    if (_hdrNombre) _hdrNombre.textContent = body.nombre;
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
    // Shipit es un agregador: su precio depende del operador que responda en
    // el momento, así que NO hay Excel que importar. Decirle a Daniel que
    // importe uno lo mandaba a buscar algo que no existe.
    body.innerHTML = CFICHA.cotizaEnVivo
      ? `<tr><td colspan="8" class="text-center py-5">
          <i class="bi bi-broadcast display-5 d-block mb-2" style="color:#dc2626;opacity:.5"></i>
          <div class="fw-semibold mb-1">Este courier cotiza en vivo</div>
          <div class="small text-muted" style="max-width:520px;margin:0 auto">
            Shipit no trabaja con una tabla de precios fija: compara varios
            operadores (Chilexpress, Starken, Bluexpress y otros) y el precio
            depende de cuál responda más barato en ese momento.
            <br><br>
            Usa la <strong>calculadora de precio</strong> de arriba para ver
            cuánto cuesta una comuna y un peso concretos, con el detalle de
            cada operador.
          </div>
        </td></tr>`
      : `<tr><td colspan="8" class="text-center py-5 text-muted">
          <i class="bi bi-map display-5 d-block mb-2 opacity-25"></i>
          <div class="fw-semibold mb-1">Sin tarifas</div>
          <div class="small">Importa un Excel de tarifas para ver los precios por comuna.</div>
        </td></tr>`;
    pg.innerHTML = '';
    stats.textContent = CFICHA.cotizaEnVivo ? 'Cotiza en vivo' : '0 comunas';
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
    // Shipit responde EN VIVO y con varios operadores: se muestran todos para
    // que se vea de dónde sale el precio (2026-08-05).
    const desglose = (d.en_vivo && Array.isArray(d.operadores) && d.operadores.length)
      ? `<div class="mt-2 pt-2" style="border-top:1px dashed #28a745">
           <div class="small text-muted mb-1">Operadores disponibles vía Shipit:</div>
           ${d.operadores.map((op, i) => `
             <div class="d-flex justify-content-between small ${i === 0 ? 'fw-bold text-success' : 'text-muted'}">
               <span>${esc(op.operador)}${op.dias ? ` · ${op.dias} día${op.dias === 1 ? '' : 's'}` : ''}</span>
               <span>$${fmt(op.precio)}</span>
             </div>`).join('')}
         </div>`
      : '';
    el.innerHTML = `
      <div class="p-2 rounded" style="background:#d4edda;border:1px solid #28a745;">
        <div class="fw-bold text-success fs-5">$${fmt(d.precio)}</div>
        <div class="text-muted small">${peso} kg → ${esc(d.comuna_matched || comuna)}</div>
        ${d.en_vivo ? '<span class="badge bg-primary" style="font-size:.6rem">Cotizado en vivo</span>' : ''}
        ${d.partial_match ? `<small class="text-muted">Coincidencia parcial</small>` : ''}
        ${desglose}
      </div>`;
  } else {
    el.innerHTML = `<small class="text-danger">${esc(d.error)}</small>`;
  }
}

/* ════════════════════════
   CHOFERES (roster del courier — 2026-08-06)
   Gestión completa dentro de la ficha del courier (reemplaza el menú admin
   "Choferes" del sidebar, pedido explícito de Daniel): foto, licencia con
   clase + filtro inteligente, seguro de carga, además de lo que ya había
   (contacto + capacidad del camión, usado por la captura de retiro).
   Las 3 acciones del viejo menú admin (asignar courier / resetear PIN /
   activar-desactivar) reusan LOS MISMOS endpoints de siempre
   (/transporte/choferes/<id>/courier|pin|toggle) — no se duplicó lógica.

   NO hay contrato de chofer acá: Daniel aclaró (mismo día) que "el
   contrato de prestación de servicio" es del courier (empresa), no de cada
   chofer -- vive en la pestaña Contratos de la ficha, enlazada con un
   banner arriba de esta tabla en vez de duplicarse por chofer.
════════════════════════ */
const CH_CLASES = (window.CFICHA && CFICHA.licenciaClases) || {};
const CH_CAMION_PEQUENO = new Set((window.CFICHA && CFICHA.licenciaCamionPequeno) || []);
let _chModal = null;
let _chSelected = new Set();   // clases de licencia elegidas en el modal

function _chFindPorId(id) {
  return (CFICHA.choferes || []).find(c => String(c.id) === String(id));
}

function chRenderClasesPicker() {
  const wrap = document.getElementById('ch_clases_picker');
  wrap.innerHTML = Object.keys(CH_CLASES).map(clase => {
    const activo = _chSelected.has(clase);
    const chico = CH_CAMION_PEQUENO.has(clase);
    return `<button type="button" class="btn btn-sm ${activo ? 'btn-danger' : (chico ? 'btn-outline-success' : 'btn-outline-secondary')}"
              style="min-height:44px" title="${esc(CH_CLASES[clase])}"
              onclick="chToggleClase('${clase}')">${clase}</button>`;
  }).join('');
}

function chToggleClase(clase) {
  if (_chSelected.has(clase)) _chSelected.delete(clase); else _chSelected.add(clase);
  chRenderClasesPicker();
}

// Zona de subida de un documento: antes de guardar el chofer no hay id para
// nombrar el archivo en GCS (mismo límite que el logo del courier), así que
// se deshabilita con una nota hasta que exista el registro.
function _chDocZonaHtml(tipo, urlActual) {
  const id = document.getElementById('ch_id').value;
  if (!id) {
    return '<div class="small text-muted fst-italic">Se habilita al guardar</div>';
  }
  const verLink = urlActual
    ? `<a href="${esc(urlActual)}" target="_blank" rel="noopener" class="small d-block mb-1"><i class="bi bi-eye me-1"></i>Ver actual</a>`
    : '';
  return `${verLink}<input type="file" class="form-control form-control-sm bg-dark text-light border-secondary"
            accept=".pdf,.jpg,.jpeg,.png,.webp" onchange="subirDocumentoChofer('${tipo}', this)">`;
}

// Foto de perfil (2026-08-06, pedido de Daniel: "una fotito" del chofer).
// Mismo patrón EXACTO que el logo del courier: pegar (Ctrl+V) / arrastrar /
// elegir archivo, subida a GCS vía _cloud_upload (nunca Cloudinary). Igual
// que los documentos, hace falta un id ya guardado para nombrar el archivo.
function _chFotoZonaHtml(fotoUrl) {
  const id = document.getElementById('ch_id').value;
  if (!id) {
    return '<div class="ch-foto-zone ch-foto-disabled"><i class="bi bi-person-fill"></i></div>'
         + '<div class="small text-muted fst-italic mt-1">Se habilita al guardar</div>';
  }
  const contenido = fotoUrl
    ? `<img class="ch-foto-img" src="${esc(fotoUrl)}" alt="">`
    : '<i class="bi bi-person-fill"></i>';
  return `<div class="ch-foto-zone" tabindex="0" title="Arrastra, pega (Ctrl+V) o haz click para elegir"
            ondragover="event.preventDefault()" ondrop="chFotoDrop(event)" onpaste="chFotoPaste(event)"
            onclick="document.getElementById('ch_foto_input').click()">${contenido}</div>
          <input type="file" id="ch_foto_input" accept="image/*" style="display:none" onchange="chFotoInputChange(this)">`;
}

function chFotoDrop(e) {
  e.preventDefault();
  const f = e.dataTransfer && e.dataTransfer.files && e.dataTransfer.files[0];
  if (f) _chSubirFoto(f);
}

function chFotoPaste(e) {
  const items = ((e.clipboardData || window.clipboardData) || {}).items || [];
  for (const it of items) {
    if (it.type && it.type.indexOf('image') > -1) {
      const f = it.getAsFile();
      if (f) { _chSubirFoto(f); break; }
    }
  }
}

function chFotoInputChange(inputEl) {
  const f = inputEl.files[0];
  if (f) _chSubirFoto(f);
}

async function _chSubirFoto(file) {
  const id = document.getElementById('ch_id').value;
  if (!id) return;
  if (!file.type || file.type.indexOf('image') === -1) {
    ilusToast('Ese archivo no es una imagen.', { type: 'warning' });
    return;
  }
  const fd = new FormData();
  fd.append('foto', file);
  try {
    const r = await fetch(`/transporte/couriers/${COURIER_ID}/choferes/${id}/foto`, { method: 'POST', body: fd });
    const d = await r.json();
    if (d.ok) {
      ilusToast('✓ Foto actualizada', { type: 'success' });
      location.reload();
    } else {
      await ilusAlert({ title: 'No se pudo subir la foto', message: d.error || 'Error desconocido', type: 'error' });
    }
  } catch (e) {
    ilusToast('Error de conexión subiendo la foto', { type: 'error' });
  }
}

function abrirModalChofer(id) {
  if (!_chModal) _chModal = new bootstrap.Modal(document.getElementById('modalChofer'));
  const ch = id ? _chFindPorId(id) : null;

  document.getElementById('ch_id').value = ch ? ch.id : '';
  document.getElementById('choferModalTitulo').innerHTML = ch
    ? '<i class="bi bi-person-badge-fill me-2 text-danger"></i>Editar chofer'
    : '<i class="bi bi-person-badge-fill me-2 text-danger"></i>Nuevo chofer';
  document.getElementById('ch_nombre').value = ch ? ch.nombre : '';
  document.getElementById('ch_rut').value = ch ? ch.rut : '';
  document.getElementById('ch_telefono').value = ch ? ch.telefono : '';
  document.getElementById('ch_patente').value = ch ? ch.patente : '';
  document.getElementById('ch_peso_max').value = ch && ch.peso_max_kg ? ch.peso_max_kg : '';
  document.getElementById('ch_vol_max').value = ch && ch.volumen_max_m3 ? ch.volumen_max_m3 : '';
  document.getElementById('ch_licencia_venc').value = ch ? ch.licencia_vencimiento : '';
  document.getElementById('ch_seguro_vigente').checked = !!(ch && ch.seguro_carga_vigente);
  document.getElementById('ch_seguro_venc').value = ch ? ch.seguro_carga_vencimiento : '';

  _chSelected = new Set(ch && ch.licencia_clase
    ? ch.licencia_clase.split(/[,/\s]+/).filter(c => CH_CLASES[c]) : []);
  chRenderClasesPicker();

  document.getElementById('ch_foto_wrap').innerHTML = _chFotoZonaHtml(ch && ch.foto_url);
  document.getElementById('ch_licencia_doc_wrap').innerHTML = _chDocZonaHtml('licencia', ch && ch.licencia_doc_url);
  document.getElementById('ch_seguro_doc_wrap').innerHTML = _chDocZonaHtml('seguro', ch && ch.seguro_carga_doc_url);
  document.getElementById('chDocsHint').style.display = ch ? 'none' : '';

  _chModal.show();
}

async function guardarChofer() {
  const id = document.getElementById('ch_id').value;
  const nombre = document.getElementById('ch_nombre').value.trim();
  const rut = document.getElementById('ch_rut').value.trim();
  if (!nombre || !rut) {
    ilusToast('Nombre y RUT son obligatorios', { type: 'warning' });
    return;
  }
  // licencia_clase es VARCHAR(10) en BD -- alcanza para 1-2 clases (ej.
  // "B,A4" = 5 caracteres) pero no para 3+. Se avisa acá para no esperar
  // el viaje al servidor (que igual valida lo mismo).
  const claseStr = Array.from(_chSelected).join(',');
  if (claseStr.length > 10) {
    ilusToast('Demasiadas clases de licencia combinadas (máx. 10 caracteres, ej. "B,A4"). Elige menos clases.', { type: 'warning' });
    return;
  }
  const payload = {
    nombre, rut,
    telefono: document.getElementById('ch_telefono').value.trim(),
    patente: document.getElementById('ch_patente').value.trim(),
    peso_max_kg: document.getElementById('ch_peso_max').value,
    volumen_max_m3: document.getElementById('ch_vol_max').value,
    licencia_clase: claseStr,
    licencia_vencimiento: document.getElementById('ch_licencia_venc').value,
    seguro_carga_vigente: document.getElementById('ch_seguro_vigente').checked,
    seguro_carga_vencimiento: document.getElementById('ch_seguro_venc').value,
  };
  const btn = document.getElementById('chGuardarBtn');
  btn.disabled = true;
  try {
    const url = id ? `/transporte/couriers/${COURIER_ID}/choferes/${id}` : `/transporte/couriers/${COURIER_ID}/choferes`;
    const r = await fetch(url, {
      method: id ? 'PUT' : 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(payload),
    });
    const d = await r.json();
    if (d.ok) {
      ilusToast(id ? '✓ Chofer actualizado' : '✓ Chofer agregado', { type: 'success' });
      if (!id) {
        ilusToast('Ahora puedes editarlo para subir licencia, póliza y contrato', { type: 'info' });
      }
      location.reload();
    } else {
      await ilusAlert({ title: 'No se pudo guardar', message: d.error || 'Error desconocido', type: 'error' });
      btn.disabled = false;
    }
  } catch (e) {
    ilusToast('Error de conexión', { type: 'error' });
    btn.disabled = false;
  }
}

async function subirDocumentoChofer(tipo, inputEl) {
  const id = document.getElementById('ch_id').value;
  const file = inputEl.files[0];
  if (!id || !file) return;
  const fd = new FormData();
  fd.append('tipo', tipo);
  fd.append('archivo', file);
  try {
    const r = await fetch(`/transporte/couriers/${COURIER_ID}/choferes/${id}/documento`, { method: 'POST', body: fd });
    const d = await r.json();
    if (d.ok) {
      ilusToast('✓ Documento subido', { type: 'success' });
      location.reload();
    } else {
      await ilusAlert({ title: 'No se pudo subir', message: d.error || 'Error desconocido', type: 'error' });
    }
  } catch (e) {
    ilusToast('Error de conexión subiendo el documento', { type: 'error' });
  }
}

// ── Acciones heredadas del viejo menú admin "Choferes" ──
// Mismos endpoints de siempre (/transporte/choferes/<id>/...), solo cambia
// DESDE DÓNDE se disparan: antes una tabla aparte, ahora el menú "..." de
// cada fila en la ficha del courier.
async function resetearPinChofer(id, nombre) {
  const pin = await ilusPrompt({
    title: 'Resetear PIN', message: 'Nuevo PIN para ' + nombre + ' (4 a 8 dígitos)',
    placeholder: '1234', required: true,
  });
  if (!pin) return;
  try {
    const r = await fetch(`/transporte/choferes/${id}/pin`, {
      method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({ pin }),
    });
    const d = await r.json();
    if (d.ok) ilusToast('✓ PIN actualizado', { type: 'success' });
    else await ilusAlert({ title: 'No se pudo actualizar', message: d.error || 'Error desconocido', type: 'error' });
  } catch (e) {
    ilusToast('Error de conexión', { type: 'error' });
  }
}

async function cambiarCourierChofer(id, courierId) {
  try {
    const r = await fetch(`/transporte/choferes/${id}/courier`, {
      method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({ courier_id: courierId }),
    });
    const d = await r.json();
    if (d.ok) {
      ilusToast('✓ Chofer movido de transporte', { type: 'success' });
      location.reload();
    } else {
      await ilusAlert({ title: 'No se pudo mover', message: d.error || 'Error desconocido', type: 'error' });
    }
  } catch (e) {
    ilusToast('Error de conexión', { type: 'error' });
  }
}

async function eliminarChoferCourier(choferId) {
  const ok = await ilusConfirm({
    title: 'Desactivar chofer',
    message: '¿Desactivar este chofer del roster del courier?',
    sub: 'No se borra su historial de retiros ni sus documentos — deja de aparecer como sugerencia en captura de retiro y se puede reactivar cuando quieras.',
    okLabel: 'Desactivar', cancelLabel: 'Cancelar', danger: true,
  });
  if (!ok) return;
  try {
    const r = await fetch(`/transporte/couriers/${COURIER_ID}/choferes/${choferId}`, { method: 'DELETE' });
    const d = await r.json();
    if (d.ok) {
      ilusToast('✓ Chofer desactivado', { type: 'success' });
      location.reload();
    } else {
      await ilusAlert({ title: 'No se pudo desactivar', message: d.error || 'Error desconocido', type: 'error' });
    }
  } catch (e) {
    ilusToast('Error de conexión', { type: 'error' });
  }
}

async function reactivarChoferCourier(choferId) {
  try {
    const r = await fetch(`/transporte/choferes/${choferId}/toggle`, { method: 'POST' });
    const d = await r.json();
    if (d.ok) {
      ilusToast('✓ Chofer reactivado', { type: 'success' });
      location.reload();
    } else {
      await ilusAlert({ title: 'No se pudo reactivar', message: d.error || 'Error desconocido', type: 'error' });
    }
  } catch (e) {
    ilusToast('Error de conexión', { type: 'error' });
  }
}

// ── Filtro inteligente por clase de licencia ──
// "Camión pequeño" es el atajo a B/A4 (Daniel, 2026-08-05: "clase A o B
// para camiones pequeños"); las demás clases se filtran una por una.
function chFiltrarClase(filtro, btnEl) {
  document.querySelectorAll('#chFiltroClases .cr-filtro-chip').forEach(b => b.classList.remove('active'));
  btnEl.classList.add('active');
  document.getElementById('tablaChoferesBody').dataset.filtro = filtro;
  chAplicarFiltro();
}

function chAplicarFiltro() {
  const filtro = document.getElementById('tablaChoferesBody').dataset.filtro || 'ALL';
  const mostrarInactivos = document.getElementById('chMostrarInactivos').checked;
  document.querySelectorAll('#tablaChoferesBody tr[data-clases]').forEach(tr => {
    const clases = (tr.dataset.clases || '').split(',').filter(Boolean);
    const activo = tr.dataset.activo === '1';
    const pasaClase = filtro === 'ALL' ? true
      : filtro === 'CAMION_PEQUENO' ? clases.some(c => CH_CAMION_PEQUENO.has(c))
      : clases.includes(filtro);
    const pasaActivo = activo || mostrarInactivos;
    tr.style.display = (pasaClase && pasaActivo) ? '' : 'none';
  });
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

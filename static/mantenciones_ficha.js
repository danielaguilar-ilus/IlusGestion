/* ==== from ficha.html lines 668-712 ==== */
(function(){
  // Carga inline-async: no bloquea el render del resto de la ficha.
  // CID se define más abajo en mant_ficha.js — usamos cliente.id directo.
  const _cid = window.__FICHA_DATA.cid;
  fetch(`/mantenciones/api/clientes/${_cid}/garantia-alertas?dias=30`)
    .then(r => r.ok ? r.json() : null)
    .then(d => {
      if (!d || !d.ok || !d.alertas || !d.alertas.length) return;
      const wrap = document.getElementById('garantiaAlertWrap');
      if (!wrap) return;
      // Si el usuario ya descartó este banner en esta sesión, no mostrar
      try {
        if (sessionStorage.getItem('ilus_gar_dismissed_' + _cid) === '1') return;
      } catch(e){}
      const items = d.alertas.map(a => {
        const eq = [a.nombre, a.marca, a.modelo].filter(Boolean).join(' ').trim() || 'Equipo';
        const ser = a.serie ? ` (serie ${a.serie})` : '';
        const dt  = a.fecha_fin_garantia
                    ? new Date(a.fecha_fin_garantia + 'T00:00:00').toLocaleDateString('es-CL')
                    : '';
        const dias = a.dias_restantes;
        const dStr = dias <= 0 ? 'hoy' : (dias === 1 ? 'mañana' : `en ${dias} días`);
        return `<li style="margin:2px 0"><strong>${eq}</strong>${ser} — sale de garantía <strong>${dStr}</strong>${dt ? ' (' + dt + ')' : ''}.</li>`;
      }).join('');
      const titulo = d.alertas.length === 1
        ? 'Garantía próxima a vencer'
        : `${d.alertas.length} equipos con garantía próxima a vencer`;
      wrap.innerHTML = `
        <div class="alert d-flex align-items-start gap-2 mb-0" role="alert"
             style="background:#fef9c3;border:1px solid #facc15;color:#713f12;border-radius:10px;padding:12px 16px">
          <i class="bi bi-shield-exclamation" style="font-size:1.3rem;color:#b45309;margin-top:2px"></i>
          <div style="flex:1;min-width:0">
            <div class="fw-bold" style="font-size:.9rem;color:#78350f">${titulo}</div>
            <ul class="mb-1 mt-1" style="font-size:.82rem;padding-left:1.1rem">${items}</ul>
            <div class="small" style="color:#92400e">Considera renegociar el contrato antes del vencimiento.</div>
          </div>
          <button type="button" class="btn-close ms-2" aria-label="Cerrar"
                  onclick="(function(){ try{sessionStorage.setItem('ilus_gar_dismissed_${_cid}','1');}catch(e){}; document.getElementById('garantiaAlertWrap').style.display='none'; })()"></button>
        </div>`;
      wrap.style.display = '';
    })
    .catch(()=>{});
})();

/* ==== from ficha.html lines 2361-3032 ==== */
// ════════════════════════════════════════════════════════════════════
// FILTROS DE LA TABLA DE EQUIPOS (combinables) — 2026-06-10 (Daniel)
// Estado (tab) + búsqueda + plan de mantención + mostrar bajas se
// evalúan juntos en _eqAplicarFiltros(), no se pisan entre sí.
// ════════════════════════════════════════════════════════════════════
const _eqFiltros = { estado: 'todos', buscar: '', plan: 'todos', mostrarBajas: false };

function _eqAplicarFiltros() {
  const rows = document.querySelectorAll('#maqListado tr');
  let visible = 0;
  rows.forEach(tr => {
    const est = tr.dataset.estado || 'activo';
    const apl = tr.dataset.aplica === '1';
    const name = tr.dataset.name || '';
    const esBaja = (est === 'baja');
    let show = true;
    // 1) Tab de estado
    if (_eqFiltros.estado !== 'todos' && est !== _eqFiltros.estado) show = false;
    // 2) Plan de mantención
    if (_eqFiltros.plan === 'si' && !apl) show = false;
    if (_eqFiltros.plan === 'no' && apl) show = false;
    // 3) Búsqueda por texto
    if (_eqFiltros.buscar && !name.includes(_eqFiltros.buscar)) show = false;
    // 4) Bajas ocultas por default (salvo que el tab pida explícitamente "baja")
    if (esBaja && !_eqFiltros.mostrarBajas && _eqFiltros.estado !== 'baja') show = false;
    tr.style.display = show ? '' : 'none';
    if (show) visible++;
  });
  const countEl = document.getElementById('eqCount');
  if (countEl) countEl.textContent = `Mostrando ${visible} equipo${visible!==1?'s':''}`;
}

// ── Filtro tab (Todos/Activos/Baja) ─────────────────────────
function eqFiltrar(estado, btn) {
  document.querySelectorAll('.eq-tab-btn').forEach(b => b.classList.remove('active'));
  if (btn) btn.classList.add('active');
  _eqFiltros.estado = estado;
  _eqAplicarFiltros();
}

// ── Filtro búsqueda ────────────────────────────────────────
function eqBuscar(q) {
  _eqFiltros.buscar = (q || '').toLowerCase().trim();
  _eqAplicarFiltros();
}
// Alias para el oninput viejo
function filtrarEquipos(q) { eqBuscar(q); }

// ── Filtro plan de mantención (chips Todos / Solo en plan / Sin mantención) ──
function eqFiltrarPlan(plan, btn) {
  document.querySelectorAll('.eq-plan-btn').forEach(b => b.classList.remove('active'));
  if (btn) btn.classList.add('active');
  _eqFiltros.plan = plan;
  _eqAplicarFiltros();
}

// ════════════════════════════════════════════════════════════════════
// SELECCIÓN MÚLTIPLE DE EQUIPOS + barra flotante + visita multi
// ════════════════════════════════════════════════════════════════════
function eqToggleAll(checked) {
  document.querySelectorAll('.eq-row-chk').forEach(c => {
    if (!c.disabled) c.checked = checked;
  });
  eqUpdateBarra();
}

function eqUpdateBarra() {
  const seleccionados = document.querySelectorAll('.eq-row-chk:checked');
  const n = seleccionados.length;
  const barra = document.getElementById('eqBarraSel');
  const btnCount = document.getElementById('eqBarraCount');
  if (n === 0) {
    if (barra) barra.style.display = 'none';
  } else {
    if (barra) barra.style.display = 'flex';
    if (btnCount) btnCount.textContent = n;
  }
  // Actualizar el master checkbox
  const total = document.querySelectorAll('.eq-row-chk').length;
  const master = document.getElementById('eqChkAll');
  if (master) {
    master.checked = (n > 0 && n === total);
    master.indeterminate = (n > 0 && n < total);
  }
}

// ── Plan de mantención: refresco optimista del chip + atajos/masivo (2026-06-10) ──
// Sincroniza el chip "En plan" / "Sin mantención" de una fila tras un cambio.
function _eqRefrescarChipPlan(mid, aplica) {
  const apl = aplica ? 1 : 0;
  const sw = document.getElementById('swPlan-' + mid);
  if (sw) {
    sw.checked = Boolean(apl);
    sw.title = apl ? 'En plan — click para excluir' : 'Sin mantención — click para incluir';
  }
  const tr = document.getElementById('maq-' + mid);
  if (tr) tr.dataset.aplica = String(apl);
}

// Atajo: marca solo las filas VISIBLES que están en plan de mantención.
function eqSeleccionarEnPlan() {
  let marcados = 0;
  document.querySelectorAll('#maqListado tr').forEach(tr => {
    const chk = tr.querySelector('.eq-row-chk');
    if (!chk || chk.disabled) return;
    const visible = tr.style.display !== 'none';
    const enPlan = tr.dataset.aplica === '1';
    chk.checked = (visible && enPlan);
    if (chk.checked) marcados++;
  });
  eqUpdateBarra();
  if (marcados === 0) {
    ilusToast('No hay equipos en plan visibles para seleccionar', { type: 'warning' });
  } else {
    ilusToast(`✓ ${marcados} equipo(s) en plan seleccionado(s)`, { type: 'success' });
  }
}

// Acción masiva: pone (aplica=1) o quita (aplica=0) del plan los seleccionados.
async function eqPlanMasivo(aplica) {
  const seleccionados = Array.from(document.querySelectorAll('.eq-row-chk:checked'));
  const ids = seleccionados.map(c => parseInt(c.dataset.id)).filter(Boolean);
  if (!ids.length) { ilusToast('Selecciona al menos un equipo', { type: 'warning' }); return; }
  if (ids.length > 500) { ilusToast('Máximo 500 equipos por acción', { type: 'warning' }); return; }

  // "Quitar del plan" pide confirmación (reversible → no danger fuerte).
  if (!aplica) {
    const ok = await ilusConfirm({
      title: 'Quitar del plan de mantención',
      message: `¿Excluir ${ids.length} equipo(s) del plan de mantención?`,
      sub: 'No se contarán en el plan, la valorización ni los levantamientos. Puedes revertirlo en cualquier momento.',
      okLabel: 'Quitar del plan', cancelLabel: 'Cancelar',
    });
    if (!ok) return;
  }

  try {
    const r = await fetch(`/mantenciones/api/clientes/${CID}/equipos/aplica-mantencion-seleccion`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ids, aplica: aplica ? 1 : 0 }),
    });
    const d = await r.json().catch(() => ({}));
    if (!r.ok || !d.ok) {
      ilusToast('No se pudo: ' + (d.error || 'error'), { type: 'error' });
      return;
    }
    // Optimista: actualizar los chips de las filas afectadas.
    ids.forEach(id => _eqRefrescarChipPlan(id, aplica));
    const n = (typeof d.afectados === 'number') ? d.afectados : ids.length;
    ilusToast(aplica ? `✓ ${n} equipo(s) puesto(s) en plan`
                     : `${n} equipo(s) quitado(s) del plan`,
              { type: aplica ? 'success' : 'info' });
    // Reaplicar filtros por si el filtro "plan" está activo (la fila puede ocultarse).
    _eqAplicarFiltros();
  } catch (e) {
    ilusToast('Error de red: ' + (e.message || ''), { type: 'error' });
  }
}

// ════════════════════════════════════════════════════════════════════
// FILTRO "MOSTRAR BAJAS" + BAJA MASIVA SELECTIVA (Daniel 2026-05-26)
// ════════════════════════════════════════════════════════════════════
function eqAplicarFiltroBajas(mostrar) {
  _eqFiltros.mostrarBajas = !!mostrar;
  _eqAplicarFiltros();
}

// Cuenta automática de bajas al cargar (para badge)
(function _eqContarBajas(){
  document.addEventListener('DOMContentLoaded', () => {
    const n = document.querySelectorAll('tr.eq-row-baja').length;
    const el = document.getElementById('eqBajasCount');
    if (el) el.textContent = n;
  });
})();

async function bajaMasivaSeleccionados() {
  const seleccionados = Array.from(document.querySelectorAll('.eq-row-chk:checked'));
  const ids = seleccionados.map(c => parseInt(c.dataset.id));
  if (!ids.length) {
    ilusToast('Selecciona al menos un equipo', { type: 'warning' });
    return;
  }
  // Mostrar listado breve en confirmación
  const nombres = seleccionados.slice(0, 5).map(c => '• ' + c.dataset.nombre).join('\n');
  const sufijo = seleccionados.length > 5 ? `\n...y ${seleccionados.length - 5} más` : '';
  const confirmText = await ilusPrompt({
    title: `Dar de baja ${ids.length} equipo(s)`,
    message: 'Los equipos seleccionados quedarán marcados como <strong>BAJA</strong> (papelera, no se eliminan).',
    sub: nombres + sufijo + '\n\nEscribe <strong>BAJA</strong> para confirmar.',
    subHtml: true,
    placeholder: 'BAJA',
    okLabel: 'Dar de baja',
    cancelLabel: 'Cancelar',
  });
  if (!confirmText) return;
  if (confirmText.trim().toUpperCase() !== 'BAJA') {
    await ilusAlert({
      title: 'Confirmación incorrecta',
      message: 'Debes escribir exactamente <strong>BAJA</strong> para confirmar.',
      subHtml: true,
      type: 'warning',
    });
    return;
  }
  ilusToast('Procesando baja…', { type: 'info' });
  try {
    const r = await fetch(`/mantenciones/api/clientes/${CID}/equipos/baja-seleccion`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ids }),
    });
    const d = await r.json();
    if (d.ok) {
      await ilusAlert({
        title: 'Baja completada',
        message: `Se dieron de baja <strong>${d.n}</strong> equipo(s). La página se recargará.`,
        subHtml: true,
        type: 'success',
      });
      location.reload();
    } else {
      await ilusAlert({ title: 'Error', message: d.error || 'No se pudo completar la baja.', type: 'error' });
    }
  } catch (e) {
    await ilusAlert({ title: 'Error de red', message: 'No se pudo conectar con el servidor.', type: 'error' });
  }
}

function abrirVisitaMulti() {
  const seleccionados = Array.from(document.querySelectorAll('.eq-row-chk:checked'))
    .map(c => ({
      id: c.dataset.id,
      nombre: c.dataset.nombre,
      sku: c.dataset.sku || '',
      serie: c.dataset.serie || '',
      cantidad: c.dataset.cantidad || 1,
    }));
  if (!seleccionados.length) { ilusToast('Selecciona al menos un equipo', { type: 'warning' }); return; }

  // Llenar el modal con los equipos seleccionados
  const ul = document.getElementById('vm_lista');
  ul.innerHTML = seleccionados.map(eq => `
    <li class="d-flex align-items-center gap-2 py-1" style="font-size:.83rem;border-bottom:1px solid #f3f4f6">
      <i class="bi bi-check2-square text-success"></i>
      <div style="flex:1;min-width:0">
        <div class="fw-semibold text-truncate">${escHtml(eq.nombre)}</div>
        <div class="small text-muted">
          ${eq.sku ? '<span class="font-monospace">'+escHtml(eq.sku)+'</span>' : ''}
          ${eq.serie ? ' · serie '+escHtml(eq.serie) : ''}
        </div>
      </div>
    </li>
  `).join('');
  document.getElementById('vm_count').textContent = seleccionados.length;

  // Guardar IDs en variable global
  window._vmIds = seleccionados.map(e => parseInt(e.id));

  // Defaults
  document.getElementById('vm_tipo').value = 'preventiva';
  document.getElementById('vm_estado').value = 'operativo';
  document.getElementById('vm_motivo').value = '';
  document.getElementById('vm_titulo').value = '';
  document.getElementById('vm_observaciones').value = '';
  document.getElementById('vm_hora_inicio').value = '';
  document.getElementById('vm_hora_fin').value = '';
  document.getElementById('vm_costo').value = '';
  // Garantía: default "No aplica" (servicio pagado)
  document.getElementById('vm_gar_no').checked = true;
  document.getElementById('vm_gar_si').checked = false;
  vmGarToggleNota();
  // Resumen de cantidad para el alert al pie
  const resumen = document.getElementById('vm_count_resumen');
  if (resumen) resumen.textContent = seleccionados.length + ' ';
  // Reset multi-técnico (chips + buscador)
  window._vmTecnicosSel = [];
  vmCargarTecnicos().then(() => { vmRenderChips(); vmSetupTecSearch(); });
  document.getElementById('vm_tec_search').value = '';
  document.getElementById('vm_tec_dropdown').style.display = 'none';
  // Reset repuestos
  window._vmRepuestos = [];
  vmRenderRepuestos();
  document.getElementById('vm_rep_search_wrap').style.display = 'none';
  vmActualizarFechaPorTipo();

  if (!window._modalVisitaMulti) {
    window._modalVisitaMulti = new bootstrap.Modal(document.getElementById('modalVisitaMulti'));
  }
  window._modalVisitaMulti.show();
}

// Fecha por defecto: hoy + 48 horas (independiente del tipo de visita).
// El estado sí se sugiere según el tipo (garantía → crítico, etc.)
function vmActualizarFechaPorTipo() {
  const tipo = document.getElementById('vm_tipo').value;
  // Fecha siempre +48h (solo se modifica si la fecha está vacía o si es superadmin)
  const fechaInput = document.getElementById('vm_fecha');
  const isSuper = window.__FICHA_DATA.is_superadmin;
  if (!fechaInput.value || isSuper === false) {
    const f = new Date(); f.setDate(f.getDate() + 2);
    fechaInput.value = f.toISOString().slice(0,10);
  }
  // Sugerir estado por tipo
  const estadoSug = {garantia:'critico', correctiva:'en_mantencion', preventiva:'operativo', inspeccion:'operativo'};
  document.getElementById('vm_estado').value = estadoSug[tipo] || 'operativo';
  // Resumen
  const labels = {garantia:'Cambio/Garantía', correctiva:'Correctiva', preventiva:'Mantención preventiva', inspeccion:'Inspección/Levantamiento'};
  document.getElementById('vm_tipo_label').textContent = labels[tipo] || tipo;
}

// ── Multi-técnico: cache, buscador, chips, costo auto ─────────────────
window._vmTecnicosAll = [];      // Catálogo completo cacheado
window._vmTecnicosSel = [];      // Técnicos seleccionados [{id,nombre,tarifa_visita}]

async function vmCargarTecnicos() {
  if (window._vmTecnicosAll.length) return;
  try {
    const r = await fetch('/mantenciones/api/tecnicos');
    window._vmTecnicosAll = await r.json();
  } catch(e) {
    window._vmTecnicosAll = [];
  }
}

function vmRenderChips(){
  const wrap = document.getElementById('vm_tec_chips');
  if (!wrap) return;
  if (!window._vmTecnicosSel.length){
    wrap.innerHTML = '<span class="small text-muted">Ningún técnico asignado todavía</span>';
  } else {
    wrap.innerHTML = window._vmTecnicosSel.map(t => `
      <span class="badge d-inline-flex align-items-center gap-1"
            style="background:#0f172a;color:#fff;font-weight:600;padding:6px 10px;font-size:.78rem">
        <i class="bi bi-person-badge"></i>${escHtml(t.nombre)}
        <button type="button" class="btn-close btn-close-white ms-1"
                style="font-size:.55rem" onclick="vmRemoveTec(${t.id})"></button>
      </span>
    `).join('');
  }
  vmRecalcularCosto();
}

function vmAddTec(id){
  if (window._vmTecnicosSel.length >= 10) {
    ilusToast('Máximo 10 técnicos por visita', { type:'warning' }); return;
  }
  if (window._vmTecnicosSel.some(t => t.id === id)) return;
  const t = window._vmTecnicosAll.find(x => x.id === id);
  if (!t) return;
  window._vmTecnicosSel.push(t);
  vmRenderChips();
  document.getElementById('vm_tec_search').value = '';
  document.getElementById('vm_tec_dropdown').style.display = 'none';
}

function vmRemoveTec(id){
  window._vmTecnicosSel = window._vmTecnicosSel.filter(t => t.id !== id);
  vmRenderChips();
}

function vmRecalcularCosto(){
  // Costo automático = sum(tarifa_visita × cantidad) — default 50.000 si no tiene tarifa
  let total = 0;
  for (const t of window._vmTecnicosSel){
    total += parseFloat(t.tarifa_visita || 50000);
  }
  document.getElementById('vm_costo').value = total ? Math.round(total) : '';
  const bd = document.getElementById('vm_costo_breakdown');
  if (bd && window._vmTecnicosSel.length){
    const detalle = window._vmTecnicosSel.map(t =>
      `${t.nombre.split(' ')[0]}: $${(parseFloat(t.tarifa_visita || 50000)).toLocaleString('es-CL')}`
    ).join(' + ');
    bd.innerHTML = `<strong>Auto:</strong> ${detalle} = <strong>$${total.toLocaleString('es-CL')}</strong>`;
  } else if (bd){
    bd.innerHTML = 'Default: $50.000 × cantidad de técnicos. Repuestos se registran aparte.';
  }
}

// Buscador de técnicos en el modal
function vmSetupTecSearch(){
  const inp = document.getElementById('vm_tec_search');
  const dd  = document.getElementById('vm_tec_dropdown');
  if (!inp || inp.dataset.bound === '1') return;
  inp.dataset.bound = '1';
  inp.addEventListener('input', () => {
    const q = inp.value.trim().toLowerCase();
    if (q.length < 1) { dd.style.display='none'; return; }
    const list = window._vmTecnicosAll.filter(t =>
      !window._vmTecnicosSel.some(s => s.id === t.id) &&
      ((t.nombre || '').toLowerCase().includes(q) ||
       (t.especialidad || '').toLowerCase().includes(q))
    ).slice(0, 8);
    if (!list.length){
      dd.innerHTML = '<div class="px-3 py-2 small text-muted">Sin coincidencias</div>';
    } else {
      dd.innerHTML = list.map(t => {
        const tar = t.tarifa_visita ? `$${parseInt(t.tarifa_visita).toLocaleString('es-CL')}` : '$50.000';
        const ext = t.es_externo ? '<span class="badge bg-warning text-dark ms-1" style="font-size:.6rem">externo</span>' : '';
        return `<div class="px-3 py-2 vm-tec-item" style="cursor:pointer;border-bottom:1px solid #f3f4f6"
                     data-id="${t.id}">
          <div class="fw-semibold small">${escHtml(t.nombre)}${ext}</div>
          <div class="small text-muted">
            ${t.especialidad ? '<i class=\"bi bi-tools me-1\"></i>'+escHtml(t.especialidad)+' · ' : ''}
            <i class="bi bi-cash"></i> ${tar} / día
          </div>
        </div>`;
      }).join('');
    }
    dd.style.display = 'block';
    dd.querySelectorAll('.vm-tec-item').forEach(it => {
      it.addEventListener('click', () => vmAddTec(parseInt(it.dataset.id, 10)));
    });
  });
  document.addEventListener('click', e => {
    if (!inp.contains(e.target) && !dd.contains(e.target)) dd.style.display='none';
  });
}

function escHtml(s){
  return String(s||'').replace(/[&<>"']/g, c =>
    ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
}

// ─── Auto-completar horas: '8' → '08:00', '12' → '12:00', '8:30' → '08:30' ──
function normalizeHoraInput(input){
  let v = (input.value || '').trim();
  if (!v) return;
  // Solo dígitos: hora sin minutos
  if (/^\d{1,2}$/.test(v)){
    const h = parseInt(v, 10);
    if (h >= 0 && h <= 23){
      input.value = String(h).padStart(2, '0') + ':00';
    }
    return;
  }
  // Con ':' — completar minutos si faltan
  const m = v.match(/^(\d{1,2}):(\d{0,2})$/);
  if (m){
    const h  = parseInt(m[1], 10);
    const mm = m[2] ? parseInt(m[2], 10) : 0;
    if (h >= 0 && h <= 23 && mm >= 0 && mm <= 59){
      input.value = String(h).padStart(2,'0') + ':' + String(mm).padStart(2,'0');
    }
  }
}

// ─── REPUESTOS ──────────────────────────────────────────────────────
window._vmRepuestos = [];

function vmRenderRepuestos(){
  const wrap  = document.getElementById('vm_repuestos_list');
  const empty = document.getElementById('vm_rep_empty');
  const totalLbl = document.getElementById('vm_rep_total');
  const lista = window._vmRepuestos || [];
  let total = 0;
  if (!lista.length){
    wrap.innerHTML = '<div class="text-muted small fst-italic" id="vm_rep_empty">Sin repuestos asociados</div>';
    totalLbl.textContent = '$0';
    return;
  }
  wrap.innerHTML = lista.map((rp, i) => {
    const ctot = (parseFloat(rp.cantidad||1) * parseFloat(rp.costo_unitario||0)) || 0;
    total += ctot;
    return `<div class="d-flex align-items-center gap-2 p-2 bg-white rounded border" data-i="${i}">
      <span class="badge ${rp.origen==='manual' ? 'bg-secondary' : 'bg-info'}" style="font-size:.6rem;text-transform:uppercase">
        ${rp.origen==='manual' ? '✎' : (rp.sku || 'erp')}
      </span>
      <input type="text" class="form-control form-control-sm" value="${escHtml(rp.descripcion)}"
             onchange="window._vmRepuestos[${i}].descripcion = this.value;vmRenderRepuestos()"
             style="font-size:.82rem;flex:2">
      <input type="number" class="form-control form-control-sm" value="${rp.cantidad}" min="0" step="0.5"
             onchange="window._vmRepuestos[${i}].cantidad = parseFloat(this.value)||1;vmRenderRepuestos()"
             style="font-size:.82rem;width:65px" title="Cantidad">
      <span class="small text-muted">×</span>
      <input type="number" class="form-control form-control-sm" value="${rp.costo_unitario}" min="0" step="100"
             onchange="window._vmRepuestos[${i}].costo_unitario = parseFloat(this.value)||0;vmRenderRepuestos()"
             style="font-size:.82rem;width:90px" title="Costo unitario">
      <span class="small fw-bold" style="min-width:80px;text-align:right;color:#0284c7">
        $${ctot.toLocaleString('es-CL')}
      </span>
      <button type="button" class="btn-close" onclick="vmRemoveRep(${i})"></button>
    </div>`;
  }).join('');
  totalLbl.textContent = '$' + total.toLocaleString('es-CL');
}

function vmAddRepManual(){
  window._vmRepuestos.push({
    sku: null, producto_id: null, descripcion: 'Nuevo repuesto',
    cantidad: 1, costo_unitario: 0, origen: 'manual'
  });
  vmRenderRepuestos();
}

function vmRemoveRep(i){
  window._vmRepuestos.splice(i, 1);
  vmRenderRepuestos();
}

function vmAbrirERPSearch(){
  const wrap = document.getElementById('vm_rep_search_wrap');
  wrap.style.display = wrap.style.display === 'none' ? 'block' : 'none';
  if (wrap.style.display === 'block') {
    document.getElementById('vm_rep_search').focus();
  }
}

let _vmRepTimer = null;
document.addEventListener('DOMContentLoaded', () => {
  const inp = document.getElementById('vm_rep_search');
  const dd  = document.getElementById('vm_rep_search_dd');
  if (!inp) return;
  inp.addEventListener('input', () => {
    clearTimeout(_vmRepTimer);
    const q = inp.value.trim();
    if (q.length < 2) { dd.style.display='none'; return; }
    _vmRepTimer = setTimeout(async () => {
      try {
        const r = await fetch(`/mantenciones/api/productos-search?q=${encodeURIComponent(q)}`);
        const list = await r.json();
        if (!Array.isArray(list) || !list.length){
          dd.innerHTML = '<div class="px-3 py-2 small text-muted">Sin resultados — usa "Manual"</div>';
          dd.style.display = 'block';
          return;
        }
        dd.innerHTML = list.map((p, idx) => `
          <div class="px-3 py-2 vm-prod-item" style="cursor:pointer;border-bottom:1px solid #f3f4f6"
               data-i="${idx}">
            <div class="d-flex justify-content-between align-items-center">
              <div style="flex:1;min-width:0">
                <div class="fw-semibold small text-truncate">${escHtml(p.nombre)}</div>
                <div class="small text-muted">
                  <span class="font-monospace">${escHtml(p.sku||'—')}</span>
                  ${p.marca ? ' · ' + escHtml(p.marca) : ''}
                </div>
              </div>
              <span class="small fw-bold text-info" style="white-space:nowrap">
                $${parseInt(p.precio_venta||0).toLocaleString('es-CL')}
              </span>
            </div>
          </div>
        `).join('');
        dd.style.display = 'block';
        dd.querySelectorAll('.vm-prod-item').forEach(it => {
          it.addEventListener('click', () => {
            const p = list[parseInt(it.dataset.i, 10)];
            window._vmRepuestos.push({
              sku: p.sku, producto_id: null,
              descripcion: p.nombre,
              cantidad: 1,
              costo_unitario: parseFloat(p.precio_venta||0),
              origen: 'erp'
            });
            inp.value = '';
            dd.style.display = 'none';
            document.getElementById('vm_rep_search_wrap').style.display = 'none';
            vmRenderRepuestos();
          });
        });
      } catch(e){
        dd.innerHTML = `<div class="px-3 py-2 small text-danger">Error: ${e.message}</div>`;
        dd.style.display = 'block';
      }
    }, 300);
  });
  document.addEventListener('click', e => {
    if (!inp.contains(e.target) && !dd.contains(e.target)) dd.style.display='none';
  });
});

// Muestra/oculta la nota "pendiente de facturar" según el toggle de garantía.
// La nota aparece cuando NO aplica (servicio pago que exige factura para cerrar).
function vmGarToggleNota() {
  const noAplica = document.getElementById('vm_gar_no')?.checked;
  document.getElementById('vm_gar_nota')?.classList.toggle('show', !!noAplica);
}
function viGarToggleNota() {
  const noAplica = document.getElementById('vi_gar_no')?.checked;
  document.getElementById('vi_gar_nota')?.classList.toggle('show', !!noAplica);
}

async function confirmarVisitaMulti() {
  const ids = window._vmIds || [];
  if (!ids.length) return;
  const motivo = document.getElementById('vm_motivo').value.trim();
  if (motivo.length < 8) { ilusToast('El motivo debe tener al menos 8 caracteres', { type:'warning' }); return; }
  const fecha = document.getElementById('vm_fecha').value;
  if (!fecha) { ilusToast('La fecha es obligatoria', { type:'warning' }); return; }

  // Auto-completar y validar horas: '8' → '08:00', '12' → '12:00'
  normalizeHoraInput(document.getElementById('vm_hora_inicio'));
  normalizeHoraInput(document.getElementById('vm_hora_fin'));
  const hi = document.getElementById('vm_hora_inicio').value;
  const hf = document.getElementById('vm_hora_fin').value;
  if (hi && !/^\d{2}:\d{2}$/.test(hi)) { ilusToast('Hora inicio inválida (usa formato HH:MM o solo número)', { type:'warning' }); return; }
  if (hf && !/^\d{2}:\d{2}$/.test(hf)) { ilusToast('Hora fin inválida (usa formato HH:MM o solo número)', { type:'warning' }); return; }
  if (hi && hf && hf <= hi) { ilusToast('La hora de fin debe ser posterior a la de inicio', { type:'warning' }); return; }

  const btn = document.getElementById('vm_btn');
  const orig = btn.innerHTML;
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Creando visita…';

  try {
    const r = await fetch(`/mantenciones/api/clientes/${CID}/visita-multi`, {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({
        maquina_ids: ids,
        tipo_visita: document.getElementById('vm_tipo').value,
        garantia_aplica: document.getElementById('vm_gar_si').checked,
        fecha_programada: fecha,
        hora_inicio: hi || null,
        hora_fin:    hf || null,
        costo:       document.getElementById('vm_costo').value || null,
        motivo,
        observaciones: document.getElementById('vm_observaciones').value.trim(),
        estado_nuevo: document.getElementById('vm_estado').value,
        tecnico_ids:  (window._vmTecnicosSel || []).map(t => t.id),
        titulo:       document.getElementById('vm_titulo').value.trim(),
        repuestos:    window._vmRepuestos || [],
      })
    });
    const d = await r.json().catch(() => ({error: 'respuesta no válida'}));
    if (!r.ok || !d.ok) {
      ilusToast('Error: ' + (d.error || 'no se pudo crear la visita'), { type:'error' });
      btn.disabled = false; btn.innerHTML = orig;
      return;
    }
    btn.innerHTML = `<i class="bi bi-check-circle-fill me-1"></i>${d.numero_ot || 'Visita'} creada`;
    const tecCount = d.tecnicos_asignados || 0;
    const costoTxt = d.costo_calculado ? ` · $${Math.round(d.costo_calculado).toLocaleString('es-CL')}` : '';
    const repTxt   = d.repuestos_count ? ` · ${d.repuestos_count} repuesto(s)` : '';
    ilusToast(
      `${d.numero_ot || ('Visita #' + d.visita_id)} creada · ${d.equipos_afectados} equipo(s) · ${tecCount} técnico(s)${repTxt}${costoTxt}`,
      { type:'success', duration: 4500 }
    );

    // Preguntar al usuario si desea enviar email al cliente
    setTimeout(async () => {
      const enviar = await ilusConfirm({
        title: '¿Enviar email al cliente?',
        message: `Notificar al cliente que se agendó la visita ${d.numero_ot || ''}`,
        sub: 'Se enviará un correo con el detalle de la visita, técnicos asignados, fecha y costo.',
        okLabel: '📧 Enviar email',
        cancelLabel: 'Ahora no',
        type: 'info',
      });
      if (enviar) {
        try {
          const r2 = await fetch(`/mantenciones/api/visitas/${d.visita_id}/enviar-email`, {
            method:'POST', headers:{'Content-Type':'application/json'},
            body: JSON.stringify({})
          });
          const d2 = await r2.json().catch(() => ({}));
          if (r2.ok && d2.ok) {
            ilusToast(`Email enviado a ${d2.destinatario}`, { type:'success' });
          } else {
            ilusToast('No se pudo enviar: ' + (d2.error || 'error desconocido'), { type:'error', duration: 6000 });
          }
        } catch(e){ ilusToast('Error de red: '+e.message, { type:'error' }); }
      }
      setTimeout(() => location.reload(), 700);
    }, 800);
  } catch(e) {
    ilusToast('Error de red: ' + e.message, { type:'error' });
    btn.disabled = false; btn.innerHTML = orig;
  }
}

/* ==== from ficha.html lines 4400-4654 ==== */
let _repCache = [];

async function cargarRepuestos() {
  try {
    const r = await fetch(`/mantenciones/api/clientes/${CID}/repuestos`);
    if (!r.ok) throw new Error('HTTP ' + r.status);
    const data = await r.json();
    _repCache = data.repuestos || [];
    repRenderTabla(_repCache);
    repRenderKpis(data.totales || {});
    const badge = document.getElementById('repCountBadge');
    if (badge) {
      if (_repCache.length) { badge.textContent = _repCache.length; badge.style.display = ''; }
      else badge.style.display = 'none';
    }
  } catch (e) {
    document.getElementById('repTbody').innerHTML =
      `<tr><td colspan="9" class="text-center text-danger py-3">Error: ${e.message}</td></tr>`;
  }
}

function repFmt(n) {
  if (n === null || n === undefined) return '—';
  return new Intl.NumberFormat('es-CL').format(Math.round(n));
}

function repRenderKpis(t) {
  const $ = id => document.getElementById(id);
  $('rep-k-count').textContent     = t.count || 0;
  $('rep-k-venta').textContent     = t.venta_count || 0;
  $('rep-k-garantia').textContent  = t.garantia_count || 0;
  $('rep-k-venta-tot').textContent = '$' + repFmt(t.venta_total);
  $('rep-k-margen').textContent    = '$' + repFmt(t.margen_total);
}

function repRenderTabla(arr) {
  const tbody = document.getElementById('repTbody');
  if (!arr.length) {
    tbody.innerHTML = `
      <tr><td colspan="9" class="text-center text-muted py-5">
        <i class="bi bi-box-seam" style="font-size:2rem;opacity:.3"></i>
        <div class="fw-semibold mt-2" style="font-size:.9rem;color:#374151">Sin repuestos registrados</div>
        <div style="font-size:.78rem">Agrega el primero con el botón "Agregar repuesto"</div>
      </td></tr>`;
    return;
  }
  tbody.innerHTML = arr.map(r => `
    <tr id="rep-row-${r.id}">
      <td>
        <div class="fw-bold" style="font-size:.84rem;color:#0f172a">${r.nombre || '—'}</div>
        <div style="font-size:.68rem;color:#9ca3af;font-family:monospace">${r.sku || '—'}</div>
      </td>
      <td><span class="rep-tipo-${r.tipo}">${r.tipo}</span></td>
      <td>${r.cantidad}</td>
      <td>$${repFmt(r.costo_unitario)}</td>
      <td>$${repFmt(r.precio_venta)}</td>
      <td style="color:${r.margen >= 0 ? '#16a34a' : '#dc2626'};font-weight:600">
        $${repFmt(r.margen)}${r.margen_pct !== null ? ` <small style="color:#9ca3af">(${r.margen_pct}%)</small>` : ''}
      </td>
      <td><span class="rep-estado rep-estado-${r.estado}">${r.estado}</span></td>
      <td style="font-size:.75rem;color:#6b7280">${r.fecha || '—'}</td>
      <td>
        <div class="d-flex gap-1 justify-content-end">
          <button class="btn btn-xs btn-outline-secondary" onclick="editarRepuesto(${r.id})" title="Editar">
            <i class="bi bi-pencil"></i>
          </button>
          <button class="btn btn-xs btn-outline-danger" onclick="eliminarRepuesto(${r.id})" title="Eliminar">
            <i class="bi bi-trash"></i>
          </button>
        </div>
      </td>
    </tr>
  `).join('');
}

function abrirRepuestoModal() {
  document.getElementById('rep_id').value = '';
  document.getElementById('repModalTitulo').innerHTML = '<i class="bi bi-gear-wide-connected me-2"></i>Nuevo repuesto';
  ['rep_sku','rep_nombre','rep_descripcion','rep_documento','rep_proveedor','rep_observacion'].forEach(id => document.getElementById(id).value = '');
  document.getElementById('rep_cantidad').value = 1;
  document.getElementById('rep_costo').value = 0;
  document.getElementById('rep_precio').value = 0;
  document.getElementById('rep_tipo').value = 'venta';
  document.getElementById('rep_estado').value = 'cotizado';
  document.getElementById('rep_fecha').value = new Date().toISOString().slice(0,10);
  document.getElementById('rep_maquina').value = '';
  new bootstrap.Modal(document.getElementById('modalRepuesto')).show();
}

function editarRepuesto(rid) {
  const r = _repCache.find(x => x.id === rid);
  if (!r) return;
  document.getElementById('rep_id').value = r.id;
  document.getElementById('repModalTitulo').innerHTML = '<i class="bi bi-pencil-square me-2"></i>Editar repuesto';
  document.getElementById('rep_sku').value = r.sku || '';
  document.getElementById('rep_nombre').value = r.nombre || '';
  document.getElementById('rep_descripcion').value = r.descripcion || '';
  document.getElementById('rep_documento').value = r.documento || '';
  document.getElementById('rep_proveedor').value = r.proveedor || '';
  document.getElementById('rep_observacion').value = r.observacion || '';
  document.getElementById('rep_cantidad').value = r.cantidad;
  document.getElementById('rep_costo').value = r.costo_unitario;
  document.getElementById('rep_precio').value = r.precio_venta;
  document.getElementById('rep_tipo').value = r.tipo;
  document.getElementById('rep_estado').value = r.estado;
  document.getElementById('rep_fecha').value = r.fecha || '';
  document.getElementById('rep_maquina').value = r.maquina_id || '';
  new bootstrap.Modal(document.getElementById('modalRepuesto')).show();
}

async function guardarRepuesto() {
  const rid = document.getElementById('rep_id').value;
  const payload = {
    sku:         document.getElementById('rep_sku').value.trim(),
    nombre:      document.getElementById('rep_nombre').value.trim(),
    descripcion: document.getElementById('rep_descripcion').value.trim(),
    cantidad:    parseFloat(document.getElementById('rep_cantidad').value) || 1,
    costo_unitario: parseFloat(document.getElementById('rep_costo').value) || 0,
    precio_venta:parseFloat(document.getElementById('rep_precio').value) || 0,
    tipo:        document.getElementById('rep_tipo').value,
    estado:      document.getElementById('rep_estado').value,
    fecha:       document.getElementById('rep_fecha').value || null,
    documento:   document.getElementById('rep_documento').value.trim(),
    proveedor:   document.getElementById('rep_proveedor').value.trim(),
    observacion: document.getElementById('rep_observacion').value.trim(),
    maquina_id:  document.getElementById('rep_maquina').value || null,
  };
  if (!payload.nombre) { alert('Nombre del repuesto es obligatorio'); return; }
  const url = rid ? `/mantenciones/api/repuestos/${rid}` : `/mantenciones/api/clientes/${CID}/repuestos`;
  const method = rid ? 'PUT' : 'POST';
  const r = await fetch(url, {
    method, headers: {'Content-Type':'application/json'},
    body: JSON.stringify(payload)
  });
  if (!r.ok) { alert('Error al guardar'); return; }
  bootstrap.Modal.getInstance(document.getElementById('modalRepuesto')).hide();
  cargarRepuestos();
}

async function eliminarRepuesto(rid) {
  const ok = await ilusConfirm({
    title: 'Eliminar repuesto',
    message: '¿Eliminar este repuesto?',
    okLabel: 'Eliminar', danger: true,
  });
  if (!ok) return;
  const r = await fetch(`/mantenciones/api/repuestos/${rid}`, {method:'DELETE'});
  if (r.ok) cargarRepuestos();
  else ilusToast('Error al eliminar', { type:'error' });
}

// ── Importar repuestos desde ERP ────────────────────────────
let _repErpLineas = [];
function abrirImportarRepErp() {
  document.getElementById('repErp_nudo').value = '';
  document.getElementById('repErpResultado').innerHTML = '';
  document.getElementById('btnRepErpImportar').disabled = true;
  _repErpLineas = [];
  new bootstrap.Modal(document.getElementById('modalImportarRepErp')).show();
}

async function repErpBuscar() {
  const tido = document.getElementById('repErp_tido').value;
  const nudo = document.getElementById('repErp_nudo').value.trim();
  if (!nudo) { alert('Ingresa el número de documento'); return; }
  const cont = document.getElementById('repErpResultado');
  cont.innerHTML = '<div class="text-center py-3"><div class="spinner-border text-primary"></div><div class="text-muted mt-2" style="font-size:.82rem">Consultando ERP…</div></div>';
  try {
    const r = await fetch(`/api/erp/documento?tido=${encodeURIComponent(tido)}&nudo=${encodeURIComponent(nudo)}`);
    if (!r.ok) {
      const e = await r.json();
      cont.innerHTML = `<div class="alert alert-warning"><i class="bi bi-exclamation-triangle me-1"></i>${e.error || 'Documento no encontrado'}</div>`;
      return;
    }
    const data = await r.json();
    _repErpLineas = data.lineas || [];
    if (!_repErpLineas.length) {
      cont.innerHTML = '<div class="alert alert-info">El documento no tiene líneas.</div>';
      return;
    }
    cont.innerHTML = `
      <div class="alert alert-info py-2 mb-3" style="font-size:.78rem">
        <strong>${data.hdr.razon_social || '—'}</strong>
        ${data.hdr.rut?` · RUT ${data.hdr.rut}`:''}
        ${data.hdr.fecha?` · ${data.hdr.fecha}`:''}
      </div>
      <div class="d-flex justify-content-between align-items-center mb-2">
        <span style="font-size:.82rem;color:#374151"><strong>${_repErpLineas.length}</strong> líneas en el documento</span>
        <div>
          <label class="small me-2"><input type="checkbox" id="rep_erp_all" checked onchange="document.querySelectorAll('.rep-erp-chk').forEach(c=>c.checked=this.checked)"> Marcar todas</label>
        </div>
      </div>
      <div class="table-responsive" style="max-height:300px;overflow-y:auto">
        <table class="table table-sm" style="font-size:.78rem">
          <thead><tr><th></th><th>SKU</th><th>Descripción</th><th>Cant.</th><th>P. unit.</th></tr></thead>
          <tbody>
            ${_repErpLineas.map((ln, i) => `
              <tr>
                <td><input type="checkbox" class="rep-erp-chk" data-idx="${i}" checked></td>
                <td class="font-monospace">${ln.sku || '—'}</td>
                <td>${ln.nombre}</td>
                <td>${ln.cantidad}</td>
                <td>$${new Intl.NumberFormat('es-CL').format(ln.precio_unit||0)}</td>
              </tr>`).join('')}
          </tbody>
        </table>
      </div>`;
    document.getElementById('btnRepErpImportar').disabled = false;
  } catch (e) {
    cont.innerHTML = `<div class="alert alert-danger">${e.message}</div>`;
  }
}

async function repErpImportar() {
  const seleccionados = Array.from(document.querySelectorAll('.rep-erp-chk:checked'))
    .map(c => parseInt(c.dataset.idx)).filter(n => !isNaN(n));
  if (!seleccionados.length) { alert('Selecciona al menos una línea'); return; }
  const tido = document.getElementById('repErp_tido').value;
  const nudo = document.getElementById('repErp_nudo').value.trim();
  const btn = document.getElementById('btnRepErpImportar');
  btn.disabled = true; btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Importando…';
  let ok = 0, err = 0;
  for (const idx of seleccionados) {
    const ln = _repErpLineas[idx];
    const payload = {
      sku: ln.sku, nombre: ln.nombre,
      cantidad: ln.cantidad, precio_venta: ln.precio_unit,
      tipo: 'venta', estado: 'cotizado',
      documento: `${tido} ${nudo}`,
      fecha: new Date().toISOString().slice(0,10),
    };
    try {
      const r = await fetch(`/mantenciones/api/clientes/${CID}/repuestos`, {
        method:'POST', headers:{'Content-Type':'application/json'},
        body: JSON.stringify(payload)
      });
      if (r.ok) ok++; else err++;
    } catch { err++; }
  }
  bootstrap.Modal.getInstance(document.getElementById('modalImportarRepErp')).hide();
  alert(`✓ Importados ${ok} repuestos${err?` (${err} errores)`:''}`);
  cargarRepuestos();
}

// Auto-cargar al abrir el tab por primera vez
document.addEventListener('DOMContentLoaded', () => {
  document.querySelectorAll('.ftab-btn[data-tab="repuestos"]').forEach(btn => {
    btn.addEventListener('click', () => { if (!_repCache.length) cargarRepuestos(); });
  });
  // PERF: NO cargar repuestos al inicio. Lazy load al primer click del tab.
  // (Antes se cargaba siempre, retrasando el TTI ~500ms aunque el usuario
  // no abriera nunca la pestaña.)
});

/* ==== from ficha.html lines 4819-5072 ==== */
// Estado cacheado de la última carga, usado para exportar y para acciones de ligar
window._finServicios = [];
window._finTotales = {};

async function cargarFinanzas() {
  const body = document.getElementById('finTblBody');
  const desde = document.getElementById('fin_desde').value;
  const hasta = document.getElementById('fin_hasta').value;
  const cubierto = document.getElementById('fin_cubierto').value;
  const estado = document.getElementById('fin_estado').value;
  const qs = new URLSearchParams();
  if (desde) qs.set('desde', desde);
  if (hasta) qs.set('hasta', hasta);
  if (cubierto) qs.set('cubierto', cubierto);
  if (estado) qs.set('estado', estado);

  body.innerHTML = `<tr><td colspan="11">
    <div class="fin-skel" style="width:60%"></div>
    <div class="fin-skel" style="width:80%"></div>
    <div class="fin-skel" style="width:70%"></div>
  </td></tr>`;

  try {
    const r = await fetch(`/mantenciones/api/clientes/${CID}/finanzas-servicios?` + qs.toString());
    if (!r.ok) throw new Error('HTTP ' + r.status);
    const d = await r.json();
    if (!d.ok) throw new Error(d.error || 'Error desconocido');
    window._finServicios = d.servicios || [];
    window._finTotales = d.totales || {};
    finRender(d);
  } catch (e) {
    body.innerHTML = `<tr><td colspan="11"><div class="alert alert-danger mb-0" style="font-size:.82rem">
      <i class="bi bi-x-circle me-1"></i>Error cargando servicios: ${e.message}
    </div></td></tr>`;
  }
}

function _fmtMoney(n){ return '$' + new Intl.NumberFormat('es-CL').format(Math.round(n||0)); }
function _escH(s){ return String(s||'').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'})[c]); }
function _fmtFecha(iso){
  if (!iso) return '—';
  try {
    return new Date(iso + 'T00:00:00').toLocaleDateString('es-CL', {day:'2-digit',month:'2-digit',year:'2-digit'});
  } catch(e){ return iso; }
}

function _tipoBadge(t){
  const map = {
    preventiva:['#dcfce7','#166534','Prev'],
    correctiva:['#fee2e2','#991b1b','Corr'],
    garantia:['#dbeafe','#1e40af','Gar'],
    inspeccion:['#fef3c7','#92400e','Insp'],
    retroactiva:['#f3f4f6','#475569','Ret'],
    instalacion:['#e0e7ff','#3730a3','Inst'],
    levantamiento:['#fce7f3','#9f1239','Lev'],
  };
  const [bg,fg,lbl] = map[t] || ['#f3f4f6','#475569', (t || '—').substring(0,4)];
  return `<span style="background:${bg};color:${fg};padding:2px 7px;border-radius:50px;font-size:.65rem;font-weight:700">${lbl}</span>`;
}

function _estadoBadge(e, dias, s){
  // Garantía que aplica → no se cobra: no hay nada que facturar.
  if (e === 'no_aplica' || (s && s.cubierto_por === 'garantia'))
    return `<span class="fin-est fin-est-fact" style="background:#dcfce7;color:#166534">🛡 Cubierto</span>`;
  if (e === 'facturado') return `<span class="fin-est fin-est-fact">✓ Facturado</span>`;
  if (e === 'con_oc')    return `<span class="fin-est fin-est-oc">🟠 Con OC${dias>7 ? ' · '+dias+'d' : ''}</span>`;
  if (e === 'cotizado')  return `<span class="fin-est fin-est-cot">🟡 Cotizado${dias>7 ? ' · '+dias+'d' : ''}</span>`;
  // Servicio PAGO (cliente/mixto) sin factura → PENDIENTE DE FACTURAR (rojo).
  const esPago = s && (s.cubierto_por === 'cliente' || s.cubierto_por === 'mixto');
  if (esPago && !(s && s.factura))
    return `<span class="fin-est fin-est-sin">🚨 Pendiente de facturar${dias>0 ? ' · '+dias+'d' : ''}</span>`;
  return `<span class="fin-est fin-est-sin">🚨 Sin cotizar${dias>0 ? ' · '+dias+'d' : ''}</span>`;
}

function _coverBadge(c, garantiaFlag){
  const labels = {contrato:'Contrato', cliente:'Cliente', garantia:'🛡 Garantía', mixto:'Mixto'};
  const cls = `fin-cover fin-cover-${c}`;
  let extra = '';
  if (garantiaFlag && c !== 'garantia') {
    extra = ` <span class="fin-cover fin-cover-garantia" title="Equipo en garantía">🛡</span>`;
  }
  return `<span class="${cls}">${labels[c] || c}</span>${extra}`;
}

function finRender(d){
  const servicios = d.servicios || [];
  const cli = d.cliente || {};
  const cnt = document.getElementById('fin_count');
  cnt.textContent = `${servicios.length} servicio${servicios.length===1?'':'s'}` +
                    (d.diagnostics ? ` · ${d.diagnostics.tiempo_ms}ms` : '');

  if (!servicios.length){
    document.getElementById('finTblBody').innerHTML = `
      <tr><td colspan="11">
        <div class="fin-empty">
          <i class="bi bi-cash-stack"></i>
          <div class="fw-semibold" style="font-size:.95rem;color:#374151">Sin servicios registrados todavía</div>
          <div style="margin-top:6px">Carga la primera visita histórica para empezar a poblar el flujo financiero.</div>
          <button class="btn btn-sm btn-ilus mt-3" onclick="abrirVisitaHistorica()">
            <i class="bi bi-clock-history me-1"></i>Cargar primera visita histórica
          </button>
        </div>
      </td></tr>`;
  } else {
    document.getElementById('finTblBody').innerHTML = servicios.map(s => {
      const cot = s.cotizacion
        ? `<span class="fin-chip fin-chip-cov" onclick="finVerDoc('${s.cotizacion.split(' ')[0]}','${s.cotizacion.split(' ')[1]||''}')" title="Ver cotización en Random">${_escH(s.cotizacion)}</span>`
        : `<span class="fin-chip fin-chip-empty" onclick="finLigar(${s.id}, 'cotizacion')">+ ligar</span>`;
      const oc = s.oc_numero
        ? `<span class="fin-chip fin-chip-oc" title="OC del cliente">${_escH(s.oc_numero)}</span>`
        : `<span class="fin-chip fin-chip-empty" onclick="finLigar(${s.id}, 'oc')">+ ligar</span>`;
      const fac = s.factura
        ? `<span class="fin-chip fin-chip-fac" onclick="finVerDoc('${s.factura.split(' ')[0]}','${s.factura.split(' ')[1]||''}')" title="Ver factura en Random">${_escH(s.factura)}</span>`
        : `<span class="fin-chip fin-chip-empty" onclick="finLigar(${s.id}, 'factura')">+ ligar</span>`;
      const desc = (s.titulo || '—') + (s.es_retroactiva ? ' <span title="Visita histórica" style="font-size:.7rem">📜</span>' : '');
      const garFlag = (s.cubierto_por === 'garantia');
      return `<tr>
        <td class="font-monospace text-nowrap">${_fmtFecha(s.fecha)}</td>
        <td>${_tipoBadge(s.tipo_visita)}</td>
        <td style="max-width:280px">
          <div style="white-space:nowrap;overflow:hidden;text-overflow:ellipsis;font-weight:600;color:#0f172a">${_escH(desc)}</div>
          ${s.nota_libre ? `<div class="small text-muted" style="white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:260px">${_escH(s.nota_libre.substring(0,80))}</div>` : ''}
        </td>
        <td>${cot}</td>
        <td>${oc}</td>
        <td>${fac}</td>
        <td class="text-end font-monospace">${s.monto_base ? _fmtMoney(s.monto_base) : '<span class="text-muted">—</span>'}</td>
        <td class="text-end font-monospace">${s.monto_repuestos ? _fmtMoney(s.monto_repuestos) : '<span class="text-muted">—</span>'}</td>
        <td>${_coverBadge(s.cubierto_por, garFlag)}</td>
        <td class="text-end font-monospace fw-bold">${s.monto_total ? _fmtMoney(s.monto_total) : '<span class="text-muted">—</span>'}</td>
        <td>${_estadoBadge(s.estado_facturacion, s.dias_sin_facturar, s)}</td>
      </tr>`;
    }).join('');
  }

  // Footer totales
  const t = d.totales || {};
  document.getElementById('fin_t_anio').textContent = _fmtMoney(t.anio);
  document.getElementById('fin_t_mes').textContent  = _fmtMoney(t.mes);
  document.getElementById('fin_t_contrato').textContent = _fmtMoney(t.total_acumulado_contrato);
  document.getElementById('fin_t_garantia').textContent = _fmtMoney(t.cubierto_garantia);
  document.getElementById('fin_t_pendiente').textContent = _fmtMoney(t.pendiente_facturar);
}

async function finVerDoc(tido, nudo){
  if (!tido || !nudo) return;
  try {
    const r = await fetch(`/api/erp/documento?tido=${encodeURIComponent(tido)}&nudo=${encodeURIComponent(nudo)}`);
    const d = await r.json();
    if (r.status !== 200 || d.error) throw new Error(d.error || ('HTTP '+r.status));
    const head = d.hdr || d.header || d.documento || {};
    const lineas = d.lineas || d.lines || [];
    let body = '';
    if (head.nrazon)  body += `<div class="small text-muted">${_escH(head.nrazon)}</div>`;
    if (head.fecha)   body += `<div class="small">Fecha: ${_escH(head.fecha)}</div>`;
    if (head.valor_neto)  body += `<div class="small">Neto: ${_fmtMoney(head.valor_neto)}</div>`;
    if (head.valor_bruto) body += `<div class="small">Bruto: ${_fmtMoney(head.valor_bruto)}</div>`;
    if (lineas.length){
      body += `<hr><div class="small fw-bold">${lineas.length} línea(s):</div>`;
      body += '<ul class="small mb-0" style="max-height:180px;overflow-y:auto;padding-left:18px">';
      body += lineas.slice(0,30).map(l => `<li>${_escH(l.nombre || l.descripcion || l.glosa || l.sku || '—')}${l.cantidad ? ' × '+l.cantidad : ''}</li>`).join('');
      body += '</ul>';
    }
    await ilusAlert({title:`Documento ${tido} ${nudo}`, message:'Detalle del documento ERP', sub: body, subHtml:true, type:'info'});
  } catch(e){
    if (typeof ilusAlert === 'function')
      await ilusAlert({title:'Error', message:`No se pudo cargar ${tido} ${nudo}: ${e.message}`, type:'error'});
    else
      alert(`Error: ${e.message}`);
  }
}

async function finLigar(vid, tipo){
  if (tipo === 'oc'){
    const numero = await ilusPrompt({title:'Ligar OC', message:'Número de OC del cliente', placeholder:'OC-77821', required:true});
    if (!numero) return;
    try {
      const r = await fetch(`/mantenciones/api/visitas/${vid}/oc`, {
        method:'POST', headers:{'Content-Type':'application/json'},
        body: JSON.stringify({numero: numero.trim()})
      });
      const d = await r.json();
      if (!d.ok) throw new Error(d.error || 'Error');
      ilusToast('✓ OC ligada', {type:'success'});
      cargarFinanzas();
    } catch(e){ ilusToast('Error: '+e.message, {type:'error'}); }
    return;
  }
  // Cotización o Factura: TIDO + NUDO
  const isFac = (tipo === 'factura');
  const opciones = isFac ? ['FCV (factura)','BLV (boleta)'] : ['COV (cotización)','NVV (nota venta)'];
  const codes    = isFac ? ['FCV','BLV'] : ['COV','NVV'];
  const tidoStr = await ilusPrompt({
    title: 'Ligar ' + (isFac ? 'factura' : 'cotización'),
    message: 'TIDO (' + opciones.join(' / ') + ')',
    placeholder: codes[0],
    required: true,
  });
  if (!tidoStr) return;
  const t = tidoStr.trim().toUpperCase();
  if (!codes.includes(t)){
    await ilusAlert({title:'TIDO inválido', message:`Debe ser ${codes.join(' o ')}`, type:'warning'});
    return;
  }
  const nudo = await ilusPrompt({title:'N° de documento', message:`Número de ${t}`, placeholder:'10499', required:true});
  if (!nudo) return;
  try {
    const url = isFac
      ? `/mantenciones/api/visitas/${vid}/factura`
      : `/mantenciones/api/visitas/${vid}/cotizacion`;
    const r = await fetch(url, {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({tido:t, nudo:nudo.trim()})
    });
    const d = await r.json();
    if (!d.ok) throw new Error(d.error || 'Error');
    ilusToast('✓ ' + (isFac?'Factura':'Cotización') + ' ligada', {type:'success'});
    cargarFinanzas();
  } catch(e){
    ilusToast('Error: '+e.message, {type:'error'});
  }
}

function finExportCSV(){
  const rows = window._finServicios || [];
  if (!rows.length){ ilusToast('Sin servicios para exportar', {type:'warning'}); return; }
  const headers = ['Fecha','Tipo','Descripción','Cotización','OC','Factura','Monto base','Repuestos','Cubierto por','Total','Estado facturación','Días sin facturar'];
  const esc = s => '"' + String(s||'').replace(/"/g,'""') + '"';
  const lines = [headers.map(esc).join(',')];
  rows.forEach(s => {
    lines.push([
      s.fecha || '', s.tipo_visita || '', s.titulo || '',
      s.cotizacion || '', s.oc_numero || '', s.factura || '',
      s.monto_base || 0, s.monto_repuestos || 0,
      s.cubierto_por || '', s.monto_total || 0,
      s.estado_facturacion || '', s.dias_sin_facturar || 0
    ].map(esc).join(','));
  });
  const t = window._finTotales || {};
  lines.push('');
  lines.push([esc('TOTAL AÑO'), '', '', '', '', '', '', '', '', esc(t.anio||0), '', ''].join(','));
  lines.push([esc('TOTAL MES'), '', '', '', '', '', '', '', '', esc(t.mes||0), '', ''].join(','));
  lines.push([esc('CUBIERTO POR GARANTÍA'), '', '', '', '', '', '', '', '', esc(t.cubierto_garantia||0), '', ''].join(','));
  lines.push([esc('PENDIENTE DE FACTURAR'), '', '', '', '', '', '', '', '', esc(t.pendiente_facturar||0), '', ''].join(','));
  const blob = new Blob(['﻿' + lines.join('\r\n')], {type:'text/csv;charset=utf-8;'});
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url; a.download = `finanzas-cliente-${CID}-${new Date().toISOString().slice(0,10)}.csv`;
  document.body.appendChild(a); a.click(); document.body.removeChild(a);
  setTimeout(()=>URL.revokeObjectURL(url), 1000);
  ilusToast('✓ CSV descargado', {type:'success'});
}

/* ==== from ficha.html lines 5254-5551 ==== */
let _docCache = [];
async function cargarDocumentos() {
  try {
    const r = await fetch(`/mantenciones/api/clientes/${CID}/documentos`);
    if (!r.ok) throw new Error('HTTP ' + r.status);
    _docCache = await r.json();
    docRender(_docCache);
  } catch (e) {
    document.getElementById('docListado').innerHTML = `<div class="alert alert-danger">${e.message}</div>`;
  }
}
function docRender(arr) {
  const cont = document.getElementById('docListado');
  if (!arr.length) {
    cont.innerHTML = `<div class="text-center py-5 text-muted">
      <i class="bi bi-folder-x" style="font-size:2.5rem;opacity:.25"></i>
      <div class="fw-semibold mt-2">Sin documentos</div>
      <div style="font-size:.78rem">Sube el primero con el botón "Subir documento"</div></div>`;
    return;
  }
  const ico = {
    contrato:'bi-file-earmark-pdf', anexo:'bi-paperclip',
    externo:'bi-globe', imagen:'bi-image',
    reporte:'bi-file-earmark-richtext', otro:'bi-file-earmark'
  };
  cont.innerHTML = arr.map(d => {
    // Si es contrato sin archivo, mostramos badge especial y enlace al tab Contratos
    const sinArchivo = !!d.sin_archivo;
    const badgeEstado = sinArchivo
      ? '<span class="badge bg-warning text-dark" style="font-size:.62rem;font-weight:600" title="Contrato registrado pero sin archivo subido">📋 metadata</span>'
      : (d.persistente
          ? '<span class="doc-badge-persist" title="Persistente en Cloudinary">☁️ persistente</span>'
          : '<span class="doc-badge-volatil" title="Solo en disco — se pierde con deploy">⚠ volátil</span>');
    const acciones = sinArchivo
      ? `<button type="button" class="btn btn-xs btn-outline-warning"
                 onclick="switchTab('contrato')"
                 title="Este contrato no tiene archivo. Ir al tab Contratos para subirlo.">
           <i class="bi bi-arrow-up-right-square"></i> Subir archivo
         </button>`
      // SEGURIDAD 2026-05-26: botones Descargar / Eliminar SOLO si superadmin.
      // El visor pasa por el endpoint que valida server-side los permisos.
      : `<button type="button" class="btn btn-xs btn-outline-primary"
                 onclick='docVisor(${JSON.stringify(d).replace(/'/g,"&#39;")})'
                 title="Ver dentro del sistema">
           <i class="bi bi-eye"></i>
         </button>
         ${d.downloadable ? `<a href="${d.url}?download=1" download class="btn btn-xs btn-outline-secondary" title="Descargar (solo superadmin)"><i class="bi bi-download"></i></a>` : ''}
         ${d.deletable ? `<button class="btn btn-xs btn-outline-danger" onclick="docEliminar('${d.kind}','${d.id}')" title="Eliminar (solo superadmin)"><i class="bi bi-trash"></i></button>` : ''}`;
    return `
    <div class="doc-card">
      <div class="doc-icon doc-tipo-${d.tipo||'otro'}">
        <i class="bi ${ico[d.tipo]||'bi-file-earmark'}"></i>
      </div>
      <div style="flex:1;min-width:0">
        <div class="doc-name text-truncate">${d.nombre || 'Sin nombre'}
          ${badgeEstado}
        </div>
        <div class="doc-meta">
          <span class="me-2">${d.fuente || d.tipo}</span>
          <span class="me-2">${d.created_at || ''}</span>
          ${d.created_by ? `<span class="me-2"><i class="bi bi-person me-1"></i>${d.created_by}</span>` : ''}
          ${d.size_kb ? `<span>${d.size_kb} KB</span>` : ''}
        </div>
      </div>
      <div class="d-flex gap-1">
        ${acciones}
      </div>
    </div>`;
  }).join('');
}
function docFiltrar(tipo, btn) {
  document.querySelectorAll('.doc-filter-btn').forEach(b => b.classList.remove('active'));
  if (btn) btn.classList.add('active');
  if (tipo === 'todos') docRender(_docCache);
  else docRender(_docCache.filter(d => d.tipo === tipo));
}
function docAbrirModal() {
  document.getElementById('docModalNombre').value = '';
  document.getElementById('docModalArchivo').value = '';
  docModalToggleSubtipo();
  new bootstrap.Modal(document.getElementById('modalDocSubir')).show();
}
function docModalToggleSubtipo() {
  const tipo = document.getElementById('docModalTipo').value;
  document.getElementById('docModalSubtipoWrap').style.display = (tipo === 'externo') ? '' : 'none';
}
async function docModalEnviar() {
  const tipo = document.getElementById('docModalTipo').value;
  const subtipo = (tipo === 'externo') ? document.getElementById('docModalSubtipo').value : '';
  const nombre = document.getElementById('docModalNombre').value.trim();
  const fileInput = document.getElementById('docModalArchivo');
  if (!fileInput.files || !fileInput.files[0]) {
    if (typeof ilusToast === 'function') ilusToast('Selecciona un archivo', { type:'warning' });
    else alert('Selecciona un archivo');
    return;
  }
  const btn = document.getElementById('docModalGuardar');
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Subiendo…';
  try {
    const fd = new FormData();
    fd.append('archivo', fileInput.files[0]);
    fd.append('tipo', tipo);
    if (subtipo) fd.append('subtipo_externo', subtipo);
    fd.append('nombre', nombre || fileInput.files[0].name);
    const r = await fetch(`/mantenciones/api/clientes/${CID}/documentos`, { method:'POST', body:fd });
    const d = await r.json();
    if (!d.ok) throw new Error(d.error || 'Error desconocido');
    bootstrap.Modal.getInstance(document.getElementById('modalDocSubir')).hide();
    if (typeof ilusToast === 'function') {
      ilusToast(
        d.persistente
          ? `✓ Subido a Cloudinary (persistente)`
          : `✓ Subido (filesystem — se pierde en deploys)`,
        { type: d.persistente ? 'success' : 'warning' }
      );
    }
    cargarDocumentos();
  } catch(e) {
    if (typeof ilusToast === 'function') ilusToast('Error: '+e.message, { type:'error' });
    else alert('Error: '+e.message);
  } finally {
    btn.disabled = false;
    btn.innerHTML = '<i class="bi bi-upload me-1"></i>Subir';
  }
}
async function docEliminar(kind, id) {
  const ok = await ilusConfirm({
    title: 'Eliminar documento', message: '¿Eliminar este documento?',
    okLabel: 'Eliminar', danger: true,
  });
  if (!ok) return;
  await fetch(`/mantenciones/api/documentos/${kind}/${id}`, {method:'DELETE'});
  cargarDocumentos();
}

// ════════════════════════════════════════════════════════════════════
//  PREVIEW UNIVERSAL DE DOCUMENTOS
//  Detecta el tipo por extensión/mime y renderiza el visor adecuado:
//    PDF      → iframe nativo (todos los navegadores soportan)
//    Imagen   → <img> centrada
//    Office   → Microsoft Office Online Viewer (gratis, oficial)
//    Texto    → fetch + <pre>
//    Otros    → fallback con botón descargar
// ════════════════════════════════════════════════════════════════════
// Determina el tipo de archivo (pdf/imagen/office/texto/otro) priorizando:
//   1) archivo_tipo hint del backend (es la fuente de verdad de mant_contratos.archivo_tipo)
//   2) mime_type explícito si llega
//   3) extensión de la URL como fallback
// Esto evita el bug donde URLs tipo /api/contratos/X/archivo (sin extensión visible)
// hacían que el viewer cayera siempre en "otro" → "Formato no previsualizable".
function docTipoArchivo(url, mime, archivoTipoHint){
  // Hint del backend (más confiable que parsear URL del proxy)
  const hint = (archivoTipoHint || '').toLowerCase();
  if (hint === 'pdf') return 'pdf';
  if (['word','doc','docx','xls','xlsx','xlsm','ppt','pptx','office'].includes(hint)) return 'office';
  if (['imagen','jpg','jpeg','png','gif','webp','bmp','svg'].includes(hint)) return 'imagen';
  if (['texto','txt','csv','md'].includes(hint)) return 'texto';

  const u = (url || '').toLowerCase().split('?')[0];
  const m = (mime || '').toLowerCase();
  const ext = u.substring(u.lastIndexOf('.') + 1);
  if (m === 'application/pdf' || ext === 'pdf') return 'pdf';
  if (m.startsWith('image/') || ['jpg','jpeg','png','gif','webp','bmp','svg'].includes(ext)) return 'imagen';
  if (['xls','xlsx','xlsm','doc','docx','ppt','pptx'].includes(ext)
      || m.includes('officedocument') || m.includes('msword') || m.includes('ms-excel')) return 'office';
  if (['txt','csv','log','md','json','xml','html'].includes(ext) || m.startsWith('text/')) return 'texto';
  return 'otro';
}

function _docPrevEsc(s){
  return String(s||'').replace(/[&<>"']/g, c =>
    ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
}

function _docPrevFallback(item, msg){
  return `<div class="doc-prev-empty">
    <i class="bi bi-file-earmark-arrow-down"></i>
    <div class="fname">${_docPrevEsc(item.nombre||'Documento')}</div>
    <div class="fmeta">${_docPrevEsc(msg||'')}${item.size_kb?` · ${item.size_kb} KB`:''}</div>
    <a href="${_docPrevEsc(item.url)}" download class="btn btn-primary btn-sm">
      <i class="bi bi-download me-1"></i>Descargar archivo
    </a>
  </div>`;
}

// NUEVO 2026-05-26: el visor unificado para CUALQUIER documento del cliente.
// Usa el mismo UniversalDocumentViewer del tab Contratos para que TODO se vea
// igual y con la misma robustez (proxy inline, PDF.js fallback, mensajes
// claros, "Este contrato debe re-subirse" cuando corresponda).
function docVisor(item){
  if (!item || !item.url){
    if (typeof ilusToast === 'function') ilusToast('Documento sin archivo', { type:'warning' });
    return;
  }
  // El backend ya nos dio archivo_tipo + mime_type + has_cloud
  const tipoHint = item.archivo_tipo || _udvTipoDesdeMime(item.mime_type || '') || '';
  // Solo contratos principales pueden re-subirse desde el visor; los adjuntos
  // se re-suben desde su propio flujo (Subir documento).
  const esContrato = (item.kind === 'contrato_principal');
  verArchivoUDV({
    baseUrl:       item.url,
    ctid:          esContrato ? item.id : null,
    nombre:        item.nombre || 'Documento',
    tipo:          tipoHint,
    hasCloud:      !!item.has_cloud,
    allowDownload: !!item.downloadable,
    allowResubir:  esContrato,
  });
}
// Helper: detecta tipo desde el mime type (fallback cuando no hay archivo_tipo)
function _udvTipoDesdeMime(mime){
  const m = (mime||'').toLowerCase();
  if (m.includes('pdf')) return 'pdf';
  if (m.startsWith('image/')) return 'imagen';
  if (m.includes('msword') || m.includes('wordprocessing')) return 'docx';
  if (m.includes('spreadsheet') || m.includes('excel')) return 'xlsx';
  if (m.includes('presentation') || m.includes('powerpoint')) return 'pptx';
  return '';
}

async function docPreview(item){
  if (!item || !item.url){
    if (typeof ilusToast === 'function') ilusToast('Documento sin URL', { type:'warning' });
    return;
  }
  // Tipo de archivo: priorizar hint del backend (archivo_tipo) sobre URL
  const tipo = docTipoArchivo(item.url, item.mime_type || item.mime, item.archivo_tipo);
  const nombre = item.nombre || 'Documento';
  const body = document.getElementById('docPrevBody');

  // Header
  document.getElementById('docPrevTitle').textContent = nombre;
  document.getElementById('docPrevTitle').title = nombre;
  document.getElementById('docPrevDownload').href = item.url;
  document.getElementById('docPrevDownload').setAttribute('download', nombre);
  document.getElementById('docPrevExternal').href = item.url;

  // Badges
  document.getElementById('docPrevBadgeTipo').textContent = tipo.toUpperCase();
  const persist = document.getElementById('docPrevBadgePersist');
  if (item.persistente){ persist.textContent = '☁️ persistente'; persist.className='badge bg-success'; }
  else { persist.textContent = '⚠ volátil'; persist.className='badge bg-warning text-dark'; }

  // Meta
  document.getElementById('docPrevMeta').textContent = [
    item.fuente || item.tipo,
    item.created_at || '',
    item.size_kb ? `${item.size_kb} KB` : '',
    item.created_by ? `por ${item.created_by}` : ''
  ].filter(Boolean).join(' · ') || '—';

  // Icono por tipo
  const icoMap = {pdf:'bi-file-earmark-pdf', imagen:'bi-file-earmark-image',
                  office:'bi-file-earmark-spreadsheet', texto:'bi-file-earmark-text', otro:'bi-file-earmark'};
  document.getElementById('docPrevIcon').className = 'bi ' + (icoMap[tipo] || 'bi-file-earmark');

  // Mostrar loader y abrir modal
  body.innerHTML = `<div class="doc-prev-loader"><div class="spinner-border text-light"></div><div class="mt-2 small">Cargando…</div></div>`;
  new bootstrap.Modal(document.getElementById('modalDocPreview')).show();

  // Render según tipo
  try {
    if (tipo === 'pdf'){
      body.innerHTML = `<iframe src="${_docPrevEsc(item.url)}#toolbar=1&view=FitH" title="${_docPrevEsc(nombre)}"></iframe>`;
    }
    else if (tipo === 'imagen'){
      body.innerHTML = `<div style="display:flex;height:100%;align-items:center;justify-content:center;background:#000">
        <img class="doc-prev-img" src="${_docPrevEsc(item.url)}" alt="${_docPrevEsc(nombre)}"
             onerror="document.getElementById('docPrevBody').innerHTML='<div class=&quot;doc-prev-empty&quot;><i class=&quot;bi bi-exclamation-triangle&quot;></i><div class=&quot;fname&quot;>No se pudo cargar la imagen</div></div>'">
      </div>`;
    }
    else if (tipo === 'office'){
      // Office Online Viewer requiere URL pública HTTPS (Cloudinary cumple)
      const isHttps = /^https:\/\//i.test(item.url);
      if (!isHttps){
        body.innerHTML = _docPrevFallback(item, 'Office Viewer requiere URL pública HTTPS (Cloudinary). Este archivo está en filesystem local.');
        return;
      }
      const viewer = `https://view.officeapps.live.com/op/embed.aspx?src=${encodeURIComponent(item.url)}`;
      body.innerHTML = `<iframe src="${viewer}" title="${_docPrevEsc(nombre)}" allowfullscreen></iframe>`;
    }
    else if (tipo === 'texto'){
      const r = await fetch(item.url);
      if (!r.ok) throw new Error('HTTP ' + r.status);
      const txt = await r.text();
      const truncado = txt.length > 200000;
      body.innerHTML = `<pre class="doc-prev-text">${_docPrevEsc(txt.slice(0, 200000))}${truncado?'\n\n… (truncado a 200 KB)':''}</pre>`;
    }
    else {
      body.innerHTML = _docPrevFallback(item, 'Formato no previsualizable en línea.');
    }
  } catch(e){
    body.innerHTML = _docPrevFallback(item, 'Error al cargar: ' + e.message);
  }
}

/* ==== from ficha.html lines 5654-5778 ==== */
// PERF 2026-05-22 — Helper Cloudinary transforms (mismo perfil que app.py).
// Reduce 80-95% el peso de las fotos: original 4-8MB iPhone → ~50-300KB.
const _EV_CLOUD_PROFILES = {
  thumb:   'f_auto,q_auto:eco,w_120,c_limit,dpr_auto',
  card:    'f_auto,q_auto,w_400,c_limit,dpr_auto',
  gallery: 'f_auto,q_auto,w_800,c_limit,dpr_auto',
  full:    'f_auto,q_auto'
};
const _EV_CLOUD_RE = /^(https?:\/\/res\.cloudinary\.com\/[^/]+\/(?:image|video|raw)\/upload\/)(.+)$/i;
const _EV_CLOUD_TX_TOKEN_RE = /(?:^|,)[a-z]_[^,/]+/;
function cloudTx(url, kind){
  if (!url || typeof url !== 'string') return url || '';
  const m = url.match(_EV_CLOUD_RE);
  if (!m) return url;
  const firstSeg = m[2].split('/')[0];
  if (firstSeg && _EV_CLOUD_TX_TOKEN_RE.test(',' + firstSeg)) return url;
  return m[1] + (_EV_CLOUD_PROFILES[kind] || _EV_CLOUD_PROFILES.card) + '/' + m[2];
}

let _evFotos = [];
let _evFiltroActual = '';

async function cargarEvidencias() {
  const cont = document.getElementById('evGrid');
  // PERF 2026-05-22 — Skeleton shimmer (8 placeholders) en vez de spinner.
  // Mejor percepción de carga + evita layout shift cuando llegan las imgs.
  cont.innerHTML = Array(8).fill(0).map(() => `
    <div style="background:#fff;border:1px solid #e5e7eb;border-radius:10px;overflow:hidden">
      <div class="ev-skeleton-img"></div>
      <div style="padding:10px">
        <div class="ev-skeleton-line" style="width:75%"></div>
        <div class="ev-skeleton-line" style="width:50%"></div>
      </div>
    </div>
  `).join('');
  try {
    const r = await fetch(`/mantenciones/api/clientes/${CID}/evidencias`);
    const d = await r.json();
    if (!d.ok) {
      cont.innerHTML = `<div class="alert alert-danger">Error: ${d.error || 'desconocido'}</div>`;
      return;
    }
    _evFotos = d.fotos || [];
    // Actualizar contadores
    document.getElementById('evTotalBadge').textContent = d.total;
    document.getElementById('evCntTodos').textContent = `(${d.total})`;
    document.getElementById('evCntOt').textContent = `(${d.por_origen.ot || 0})`;
    document.getElementById('evCntLev').textContent = `(${d.por_origen.levantamiento || 0})`;
    document.getElementById('evCntGal').textContent = `(${d.por_origen.galeria || 0})`;
    renderEvidencias();
  } catch (e) {
    cont.innerHTML = `<div class="alert alert-danger">Error de red: ${e.message}</div>`;
  }
}

function filtrarEvidencias(chip, origen) {
  _evFiltroActual = origen;
  document.querySelectorAll('.ev-chip').forEach(c => c.classList.remove('active'));
  chip.classList.add('active');
  renderEvidencias();
}

function renderEvidencias() {
  const cont = document.getElementById('evGrid');
  const fotos = _evFiltroActual
    ? _evFotos.filter(f => f.origen === _evFiltroActual)
    : _evFotos;
  if (!fotos.length) {
    const msg = _evFiltroActual
      ? `No hay fotos del tipo "${_evFiltroActual}"`
      : 'Aún no hay evidencias fotográficas para este cliente';
    cont.innerHTML = `
      <div class="ev-empty">
        <i class="bi bi-camera"></i>
        <div class="fw-semibold mt-1">${msg}</div>
        <div class="small mt-1">Las fotos de OTs, levantamientos y galería de equipos aparecerán aquí.</div>
      </div>`;
    return;
  }
  const ico = { ot:'bi-clipboard2-pulse', levantamiento:'bi-camera-fill', galeria:'bi-images' };
  cont.innerHTML = fotos.map((f, idx) => `
    <div class="ev-card" onclick="abrirEvLightbox(${idx})">
      <img class="ev-img" src="${_evEsc(cloudTx(f.url, 'card'))}" alt="${_evEsc(f.descripcion||'')}" loading="lazy" decoding="async"
           loading="lazy" decoding="async" onerror="this.style.display='none'">
      <div class="ev-meta">
        <div class="tt" title="${_evEsc(f.maquina_nombre||f.descripcion||'')}">
          ${_evEsc(f.maquina_nombre || f.descripcion || 'Sin descripción')}
        </div>
        <div class="ss">
          ${_evEsc(f.fecha)} · ${_evEsc(f.tomada_por || '—')}
        </div>
        <span class="badge-orig ev-orig-${f.origen}">
          <i class="bi ${ico[f.origen]||'bi-image'} me-1"></i>${_evEsc(f.origen_label)}
        </span>
      </div>
    </div>
  `).join('');
}

function _evEsc(s) {
  return String(s||'').replace(/[&<>"']/g, c =>
    ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
}

function abrirEvLightbox(idx) {
  const fotos = _evFiltroActual ? _evFotos.filter(f => f.origen === _evFiltroActual) : _evFotos;
  const f = fotos[idx];
  if (!f) return;
  // PERF 2026-05-22: lightbox = 'full' (f_auto + q_auto, sin resize) —
  // sirve la mejor calidad en formato moderno (WebP/AVIF) pero respetando
  // el tamaño original. Reduce 30-50% vs el JPG original sin perdidas
  // perceptibles.
  document.getElementById('evLightboxImg').src = cloudTx(f.url, 'full');
  document.getElementById('evLightboxMeta').innerHTML =
    `<div class="fw-bold">${_evEsc(f.maquina_nombre || 'Sin equipo asociado')}</div>
     <div class="small mt-1">${_evEsc(f.descripcion || '')}</div>
     <div class="small mt-1" style="opacity:.7">${_evEsc(f.origen_label)} · ${_evEsc(f.fecha)} · por ${_evEsc(f.tomada_por||'—')}</div>`;
  document.getElementById('evLightbox').style.display = 'flex';
}

function cerrarEvLightbox() {
  document.getElementById('evLightbox').style.display = 'none';
}

/* ==== from ficha.html lines 5822-5924 ==== */
async function cargarComunicaciones() {
  try {
    const r = await fetch(`/mantenciones/api/notificaciones?cliente_id=${CID}`);
    const arr = await r.json();
    const cont = document.getElementById('comListado');
    if (!arr.length) {
      cont.innerHTML = `<div class="text-center py-5 text-muted">
        <i class="bi bi-envelope-x" style="font-size:2.5rem;opacity:.25"></i>
        <div class="fw-semibold mt-2">Sin comunicaciones registradas</div>
        <div style="font-size:.78rem">Las notificaciones y emails enviados aparecerán aquí</div></div>`;
      return;
    }
    cont.innerHTML = arr.map(n => `
      <div class="com-card">
        <div class="d-flex align-items-start gap-3 flex-wrap">
          <div style="width:38px;height:38px;border-radius:9px;display:flex;align-items:center;justify-content:center;background:${n.canal==='email'?'#dbeafe':n.canal==='whatsapp'?'#dcfce7':'#f3f4f6'};color:${n.canal==='email'?'#1e40af':n.canal==='whatsapp'?'#166534':'#6b7280'};flex-shrink:0">
            <i class="bi ${n.canal==='email'?'bi-envelope-fill':n.canal==='whatsapp'?'bi-whatsapp':'bi-bell-fill'}"></i>
          </div>
          <div style="flex:1;min-width:0">
            <div class="d-flex align-items-center gap-2 flex-wrap mb-1">
              <span class="fw-bold" style="font-size:.85rem">${n.titulo}</span>
              <span class="com-canal com-canal-${n.canal}">${n.canal}</span>
              <span class="com-est com-est-${n.estado}">${n.estado}</span>
            </div>
            <div style="font-size:.77rem;color:#374151;line-height:1.45">${n.mensaje||''}</div>
            <div style="font-size:.66rem;color:#9ca3af;margin-top:3px">
              ${n.created_at||''}${n.destinatario?' · '+n.destinatario:''}${n.fecha_envio?' · enviado '+n.fecha_envio:''}
            </div>
          </div>
        </div>
      </div>`).join('');
  } catch (e) {
    document.getElementById('comListado').innerHTML = `<div class="alert alert-danger">${e.message}</div>`;
  }
}

function abrirEmailManual() {
  const cliente_email = window.__FICHA_DATA.cliente_contacto_email;
  const cliente_nombre = window.__FICHA_DATA.cliente_contacto_nombre;
  const html = `
    <div class="modal fade" id="modalEmailManual" tabindex="-1">
      <div class="modal-dialog modal-lg">
        <div class="modal-content">
          <div class="modal-header" style="background:#0f172a;color:#fff">
            <h5 class="modal-title"><i class="bi bi-envelope-paper me-2"></i>Enviar email manual</h5>
            <button type="button" class="btn-close btn-close-white" data-bs-dismiss="modal"></button>
          </div>
          <div class="modal-body">
            <div class="mb-2">
              <label class="form-label small fw-semibold">Destinatario</label>
              <input type="email" id="em_to" class="form-control form-control-sm" value="${cliente_email}">
            </div>
            <div class="mb-2">
              <label class="form-label small fw-semibold">Asunto</label>
              <input type="text" id="em_subj" class="form-control form-control-sm">
            </div>
            <div class="mb-0">
              <label class="form-label small fw-semibold">Mensaje</label>
              <textarea id="em_body" class="form-control form-control-sm" rows="8"
                        placeholder="Estimado/a ${cliente_nombre}, ..."></textarea>
            </div>
          </div>
          <div class="modal-footer">
            <button class="btn btn-sm btn-outline-secondary" data-bs-dismiss="modal">Cancelar</button>
            <button class="btn btn-sm btn-ilus" id="btn_em_enviar">
              <i class="bi bi-send me-1"></i>Enviar
            </button>
          </div>
        </div>
      </div>
    </div>`;
  const old = document.getElementById('modalEmailManual'); if (old) old.remove();
  document.body.insertAdjacentHTML('beforeend', html);
  const m = new bootstrap.Modal(document.getElementById('modalEmailManual'));
  m.show();
  document.getElementById('btn_em_enviar').onclick = async () => {
    const btn = document.getElementById('btn_em_enviar');
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Enviando...';
    const payload = {
      destinatario: document.getElementById('em_to').value.trim(),
      asunto:       document.getElementById('em_subj').value.trim(),
      mensaje:      document.getElementById('em_body').value.trim(),
    };
    if (!payload.destinatario || !payload.asunto || !payload.mensaje) {
      alert('Completa destinatario, asunto y mensaje.'); btn.disabled=false; btn.innerHTML='<i class="bi bi-send me-1"></i>Enviar'; return;
    }
    const r = await fetch(`/mantenciones/api/clientes/${CID}/email-manual`, {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify(payload)
    });
    const data = await r.json();
    if (data.ok) {
      m.hide();
      cargarComunicaciones();
    } else {
      alert('Error: ' + (data.error || 'No se pudo enviar'));
      btn.disabled=false; btn.innerHTML='<i class="bi bi-send me-1"></i>Enviar';
    }
  };
}

/* ==== from ficha.html lines 6547-7296 ==== */
  /* ──────────────────────────────────────────────────────────────
     Visitas: toggle Lista / Timeline (Plan 2026-05-21 Capa 10)
     ────────────────────────────────────────────────────────────── */
  // Equipos del cliente (inyectados desde Jinja)
  window._visTlMaquinas = window.__FICHA_DATA.vis_maquinas_ids;
  window._visTlMaquinasMap = window.__FICHA_DATA.vis_maquinas_map;
  window._visTlVisitas = [];   // cache de visitas (formato API)
  window._visTlLoaded = false;

  const _VIS_TIPO_COLOR = {
    preventiva:'#16a34a', correctiva:'#f59e0b', garantia:'#3b82f6',
    retroactiva:'#6b7280', instalacion:'#7c3aed', levantamiento:'#9f1239',
    inspeccion:'#0891b2'
  };
  const _VIS_TIPO_ICON = {
    preventiva:'P', correctiva:'C', garantia:'G',
    retroactiva:'R', instalacion:'I', levantamiento:'L', inspeccion:'In'
  };

  /* ══════════════════════════════════════════════════════════════════
     Resumen ejecutivo de visitas — Plan 2026-05-21
     Carga UNA sola vez la lista completa de visitas del cliente y
     renderiza tres bloques: próximas, últimas y ritmo de 12 meses.
     Reusa la misma cache (_visTlVisitas) que la vista Timeline,
     así si el usuario ya abrió Timeline antes, no se pide de nuevo.
     ══════════════════════════════════════════════════════════════════ */
  const CT_VIGENTE_ID = (window.__FICHA_DATA || {}).contrato_vigente_id || null;
  // 2026-06-10 — flag para acciones comerciales (proponer plan): solo no-técnicos
  const VIS_NO_TECNICO = window.__FICHA_DATA.vis_no_tecnico;
  window._visResumenLoaded = false;

  // Botón "Proponer plan al cliente" — la función la implementa
  // mant_ficha.js (mantProponerPlanEmail). Si no existe (JS viejo en
  // cache) o el usuario es técnico, NO se muestra el botón.
  function _visBtnProponerPlan(){
    if (!VIS_NO_TECNICO || typeof window.mantProponerPlanEmail !== 'function') return '';
    return `<button class="btn btn-sm btn-outline-dark" style="font-size:.72rem" onclick="window.mantProponerPlanEmail()">
              <i class="bi bi-envelope-paper me-1"></i>Proponer plan al cliente
            </button>`;
  }

  // Toggle de la sección colapsada de canceladas/anuladas (lista de OTs)
  function visToggleCanceladas(){
    const w = document.getElementById('visCancWrap');
    const b = document.getElementById('visCancToggleBtn');
    if (!w || !b) return;
    const show = w.style.display === 'none';
    w.style.display = show ? '' : 'none';
    b.innerHTML = show
      ? '<i class="bi bi-eye-slash me-1"></i>Ocultar canceladas'
      : '<i class="bi bi-eye me-1"></i>' + (b.dataset.lblVer || 'Ver canceladas');
  }

  async function _visEnsureVisitasLoaded(){
    if (window._visResumenLoaded || window._visTlLoaded) return;
    try {
      const r = await fetch(`/mantenciones/api/visitas?cliente_id=${CID}&start=1900-01-01&end=2100-12-31`);
      const arr = await r.json();
      window._visTlVisitas = Array.isArray(arr) ? arr : [];
      window._visResumenLoaded = true;
    } catch(e){
      window._visTlVisitas = [];
    }
  }

  function _visFechaRel(d){
    if (!d) return '';
    const diff = Math.round((d - new Date()) / 86400000);
    if (diff === 0) return 'hoy';
    if (diff === 1) return 'mañana';
    if (diff === -1) return 'ayer';
    if (diff > 0 && diff <= 30) return `en ${diff} días`;
    if (diff > 30) return `en ${Math.round(diff/30)} mes${Math.round(diff/30)>1?'es':''}`;
    if (diff < 0 && diff >= -30) return `hace ${-diff} días`;
    if (diff < -30) return `hace ${Math.round(-diff/30)} mes${Math.round(-diff/30)>1?'es':''}`;
    return '';
  }

  function _visGetDate(v){
    const s = v.fecha_realizada || v.fecha || v.start || v.fecha_programada || '';
    if (!s) return null;
    try {
      const d = new Date(String(s).substring(0,10) + 'T00:00:00');
      return isNaN(d) ? null : d;
    } catch(e){ return null; }
  }

  function _visEscHtml(s){
    return String(s||'').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'})[c]);
  }

  async function renderVisResumen(){
    await _visEnsureVisitasLoaded();
    _renderProximas();
    _renderUltimas();
    _renderRitmo();
  }

  function _renderProximas(){
    const cont = document.getElementById('visProximasBody');
    if (!cont) return;
    const hoy = new Date(); hoy.setHours(0,0,0,0);
    const visitas = window._visTlVisitas || [];
    const futurasOActivas = visitas
      .filter(v => {
        const d = _visGetDate(v);
        if (!d) return false;
        const est = (v.estado || v.status || '').toLowerCase();
        if (['completada','cancelada','anulada'].includes(est)) return false;
        return d >= hoy || est === 'programada' || est === 'reagendada' || est === 'en_curso';
      })
      .sort((a,b) => {
        const da = _visGetDate(a), db = _visGetDate(b);
        return (da?.getTime()||0) - (db?.getTime()||0);
      });

    if (!futurasOActivas.length){
      cont.innerHTML = `
        <div class="vis-prox-empty">
          <i class="bi bi-calendar-x" style="font-size:1.6rem;opacity:.5;display:block;margin-bottom:8px"></i>
          <div class="mb-2">No hay mantenciones programadas</div>
          ${CT_VIGENTE_ID ? `
            <div class="d-flex gap-2 justify-content-center flex-wrap">
              <button class="btn btn-sm btn-ilus" onclick="visCalendarizarProximas()">
                <i class="bi bi-calendar-plus me-1"></i>Calendarizar próximas
              </button>${_visBtnProponerPlan()}
            </div>` : `
            <div style="font-size:.7rem;color:#9ca3af">Sin contrato vigente con frecuencia definida</div>
          `}
        </div>`;
      return;
    }

    const prox = futurasOActivas[0];
    const proxDate = _visGetDate(prox);
    const proxDias = Math.round((proxDate - hoy) / 86400000);
    const atrasada = proxDias < 0;
    const urgente  = proxDias >= 0 && proxDias < 7;
    const chip = atrasada
      ? `<span class="vis-prox-chip-urg">Atrasada ${-proxDias}d</span>`
      : (urgente ? `<span class="vis-prox-chip-urg">Urgente</span>` :
         (proxDias <= 30 ? `<span class="vis-prox-chip-warn">Próxima</span>` : ''));

    const fechaStr = proxDate.toLocaleDateString('es-CL', {day:'2-digit', month:'2-digit', year:'numeric'});
    const tecnico  = prox.tecnico || (prox.tecnicos && prox.tecnicos[0]?.nombre) || 'Sin asignar';
    const tipo     = (prox.tipo || 'preventiva').replace(/_/g, ' ');

    let html = `
      <div class="vis-prox-next ${atrasada ? 'atrasada' : urgente ? 'urgente' : ''}">
        <div class="d-flex align-items-start justify-content-between gap-2 mb-2">
          <div>
            <div class="vis-prox-fecha ${atrasada ? 'atrasada' : urgente ? 'urgente' : ''}">${fechaStr}</div>
            <div class="vis-prox-rel ${atrasada ? 'atrasada' : urgente ? 'urgente' : ''}">${_visFechaRel(proxDate)}</div>
          </div>
          ${chip}
        </div>
        <div style="font-size:.85rem;font-weight:700;text-transform:capitalize;color:${atrasada?'#fff':'#0f172a'}">
          ${_visEscHtml(prox.title || prox.titulo || tipo)}
        </div>
        <div style="font-size:.75rem;color:${atrasada?'#fee2e2':'#6b7280'};margin-top:2px">
          <i class="bi bi-person-badge me-1"></i>${_visEscHtml(tecnico)}
          ${prox.numero_ot ? `· <span class="font-monospace" style="font-weight:600">${prox.numero_ot}</span>` : ''}
        </div>
        <div class="d-flex gap-2 mt-2 flex-wrap">
          <a href="/mantenciones/ot/${prox.id}" class="btn btn-sm ${atrasada?'btn-light':'btn-outline-secondary'}" style="font-size:.72rem">
            <i class="bi bi-arrow-right me-1"></i>Abrir OT
          </a>
          ${atrasada ? `
            <button class="btn btn-sm btn-light" style="font-size:.72rem" onclick="visAbrirReagendar(${prox.id})">
              <i class="bi bi-calendar2-event me-1"></i>Reagendar
            </button>` : ''}
        </div>
      </div>`;

    // Siguientes 3 (slice 1..4)
    const siguientes = futurasOActivas.slice(1, 4);
    if (siguientes.length){
      html += `<div style="font-size:.66rem;text-transform:uppercase;letter-spacing:.4px;font-weight:800;color:#9ca3af;margin:6px 4px 4px">Siguientes</div>`;
      html += `<div class="vis-prox-sig">`;
      siguientes.forEach(s => {
        const sd = _visGetDate(s);
        const sStr = sd ? sd.toLocaleDateString('es-CL', {day:'2-digit', month:'short', year:'2-digit'}) : '—';
        const tipoS = (s.tipo || 'preventiva').replace(/_/g, ' ');
        const colorDot = _VIS_TIPO_COLOR[s.tipo] || '#9ca3af';
        html += `<div class="vis-prox-sig-row" onclick="window.location.href='/mantenciones/ot/${s.id}'">
          <span class="vis-prox-sig-dot" style="background:${colorDot}"></span>
          <span style="font-weight:700;color:#374151;min-width:90px">${sStr}</span>
          <span style="flex:1;text-transform:capitalize">${_visEscHtml(tipoS)}</span>
          <i class="bi bi-chevron-right" style="color:#9ca3af"></i>
        </div>`;
      });
      html += `</div>`;
    } else if (CT_VIGENTE_ID){
      html += `
        <div style="font-size:.7rem;color:#9ca3af;text-align:center;padding:8px 4px" class="d-flex gap-2 justify-content-center flex-wrap">
          <button class="btn btn-xs btn-outline-secondary" onclick="visCalendarizarProximas()" style="font-size:.7rem">
            <i class="bi bi-calendar-plus me-1"></i>Calendarizar más visitas
          </button>${_visBtnProponerPlan()}
        </div>`;
    }
    cont.innerHTML = html;
  }

  function _renderUltimas(){
    const cont = document.getElementById('visUltimasBody');
    if (!cont) return;
    const visitas = window._visTlVisitas || [];
    // FIX 2026-06-10: 'cerrada' también es realizada (antes solo 'completada'
    // y las OTs cerradas desaparecían de "Últimas realizadas").
    const realizadas = visitas
      .filter(v => ['completada','cerrada'].includes((v.estado || '').toLowerCase()))
      .sort((a,b) => (_visGetDate(b)?.getTime()||0) - (_visGetDate(a)?.getTime()||0))
      .slice(0, 5);

    if (!realizadas.length){
      cont.innerHTML = `
        <div class="vis-prox-empty">
          <i class="bi bi-calendar-check" style="font-size:1.6rem;opacity:.5;display:block;margin-bottom:8px"></i>
          <div>Aún no hay mantenciones realizadas</div>
        </div>`;
      return;
    }

    let html = '';
    realizadas.forEach(v => {
      const d = _visGetDate(v);
      const rel = d ? _visFechaRel(d) : '';
      const dStr = d ? d.toLocaleDateString('es-CL', {day:'2-digit', month:'short', year:'2-digit'}) : '—';
      const col = _VIS_TIPO_COLOR[v.tipo] || '#475569';
      const tipo = (v.tipo || '').replace(/_/g, ' ');
      const fact = (v.estado_facturacion || '').toLowerCase();
      // FIX 2026-06-10: cubierto_por REAL del API. Antes había un fallback
      // ||'contrato' que pintaba el badge "Contrato" aunque el dato no
      // existiera. Si viene null → sin badge de cobertura.
      const cub = (v.cubierto_por || '').toLowerCase();
      let factBadge = '';
      if (cub === 'garantia') factBadge = `<span class="vis-ult-fact gar">Garantía</span>`;
      else if (fact === 'facturado') factBadge = `<span class="vis-ult-fact fact">Facturado</span>`;
      else if (cub === 'contrato') factBadge = `<span class="vis-ult-fact gar" style="background:#e0e7ff;color:#3730a3">Contrato</span>`;
      else if (cub === 'cliente') factBadge = `<span class="vis-ult-fact no">Facturable</span>`;
      // Históricas (es_retroactiva=1): atenuadas + badge gris
      const esHist = !!v.es_retroactiva;
      const histBadge = esHist
        ? `<span class="badge-historica" title="Registrada retroactivamente; realizada antes de que existiera el sistema">Histórica</span>`
        : '';
      html += `<div class="vis-ult-item" ${esHist ? 'style="opacity:.72"' : ''} onclick="window.location.href='/mantenciones/ot/${v.id}'">
        <span class="vis-ult-dot" style="background:${col}"></span>
        <div class="vis-ult-body">
          <div class="vis-ult-titulo">${_visEscHtml(v.title || v.titulo || tipo)}</div>
          <div class="vis-ult-meta">
            <span style="font-weight:700;color:#374151">${dStr}</span>
            ${rel ? ` · <span>${rel}</span>` : ''}
            ${v.tecnico ? ` · <i class="bi bi-person-badge"></i> ${_visEscHtml(v.tecnico)}` : ''}
          </div>
        </div>
        <div style="display:flex;flex-direction:column;align-items:flex-end;gap:4px">${factBadge}${histBadge}</div>
      </div>`;
    });
    cont.innerHTML = html;
  }

  /* ══════════════════════════════════════════════════════════════
     RITMO v2 (2026-06-10) — RECOMENDADO vs REALIZADO.
     Consume GET /mantenciones/api/clientes/<cid>/ritmo-mantencion:
       meses[12] = {ym,label,recomendadas,realizadas,historicas,
                    agendadas,atrasadas}  (orden ASC, mes actual último)
       cumplimiento = {esperadas,realizadas,pct,estimado,
                       frecuencia_meses,frecuencia_origen}
     Si el endpoint falla, cae al render simple client-side de antes
     (_renderRitmoFallback) para no romper la pestaña.
     ══════════════════════════════════════════════════════════════ */
  async function _renderRitmo(){
    const cont = document.getElementById('visRitmoTrack');
    if (!cont) return;
    try {
      const r = await fetch(`/mantenciones/api/clientes/${CID}/ritmo-mantencion`);
      if (!r.ok) throw new Error('HTTP ' + r.status);
      const d = await r.json();
      if (!d || !d.ok || !Array.isArray(d.meses) || !d.meses.length){
        throw new Error((d && d.error) || 'sin datos');
      }
      _renderRitmoPro(d);
    } catch(e){
      _renderRitmoFallback();
    }
  }

  function _renderRitmoPro(d){
    const cont = document.getElementById('visRitmoTrack');
    const lbl  = document.getElementById('visRitmoCount');
    const cump = document.getElementById('visRitmoCumpl');
    const leg  = document.getElementById('visRitmoLeg');
    if (!cont) return;
    const now = new Date();
    const ymNow = `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}`;

    // ── Banner CUMPLIMIENTO ──
    const c = d.cumplimiento || {};
    if (cump){
      const pct = Math.max(0, Math.min(100, Math.round(c.pct || 0)));
      const realiz = c.realizadas || 0;
      if (!c.esperadas || c.frecuencia_origen === 'sin_datos'){
        cump.innerHTML = `<span class="vrc-txt">${realiz} realizada${realiz !== 1 ? 's' : ''} en 12 meses · sin frecuencia conocida para proyectar el plan</span>`;
      } else {
        const cls = pct >= 80 ? 'good' : (pct >= 50 ? 'mid' : 'bad');
        const est = c.estimado ? ' <span style="font-size:.72rem;font-weight:700;color:#9ca3af">(estimado)</span>' : '';
        const origen = ({
          contrato:      'según contrato',
          sugerida:      'según frecuencia sugerida',
          regla_default: 'según regla general',
        })[c.frecuencia_origen] || 'según contrato';
        const cada = c.frecuencia_meses ? ` (cada ${c.frecuencia_meses} ${c.frecuencia_meses === 1 ? 'mes' : 'meses'})` : '';
        cump.innerHTML = `<span class="vrc-pct ${cls}">CUMPLIMIENTO: ${pct}%${est}</span>` +
          `<span class="vrc-txt">${realiz} realizada${realiz !== 1 ? 's' : ''} de ${c.esperadas} esperada${c.esperadas !== 1 ? 's' : ''} ${origen}${cada}</span>`;
      }
      cump.style.display = '';
    }
    if (lbl){
      const totR = d.meses.reduce((a,m) => a + (m.realizadas || 0), 0);
      lbl.textContent = `${totR} realizada${totR !== 1 ? 's' : ''} en 12 meses`;
    }

    // ── Track mensual: anillo punteado = recomendado · relleno = real ──
    let html = '';
    d.meses.forEach(m => {
      const esActual = (m.ym === ymNow);
      const futuro   = (m.ym > ymNow);
      const rec = m.recomendadas || 0, rea = m.realizadas || 0,
            his = m.historicas  || 0, age = m.agendadas  || 0,
            atr = m.atrasadas   || 0;
      let cls = 'vr2-empty', inner = '';
      if (rea > 0){
        if (his >= rea){ cls = 'vr2-hist'; inner = '<i class="bi bi-clock-history"></i>'; }
        else { cls = 'vr2-ok'; }
      } else if (age > 0){
        cls = (atr > 0) ? 'vr2-atrasada' : 'vr2-agendada';
      } else if (rec > 0 && !futuro && !esActual){
        cls = 'vr2-miss';   // recomendada, mes pasado, nadie fue
      }
      const recCls = rec > 0 ? ' vr2-rec' : '';
      const curCls = esActual ? ' current-month' : '';
      const parts = [];
      if (rec || rea || age){
        parts.push(`Recomendadas: ${rec}`);
        parts.push(`Realizadas: ${rea}${his ? ` (${his} histórica${his > 1 ? 's' : ''})` : ''}`);
        if (age) parts.push(`Agendadas: ${age}${atr ? ` (${atr} atrasada${atr > 1 ? 's' : ''})` : ''}`);
      } else {
        parts.push('Sin actividad');
      }
      const tip = `${m.label || m.ym} — ${parts.join(' · ')}`;
      const lblTxt = String(m.label || m.ym).split(' ')[0];
      html += `<div class="vis-ritmo-month">
        <div class="vis-ritmo-dot ${cls}${recCls}${curCls}">${inner}</div>
        <div class="vis-ritmo-lbl ${esActual ? 'current' : ''}">${_visEscHtml(lblTxt)}</div>
        <div class="vis-ritmo-tooltip">${_visEscHtml(tip)}</div>
      </div>`;
    });
    cont.innerHTML = html;

    // ── Leyenda actualizada ──
    if (leg){
      leg.innerHTML = `
        <span><span class="vis-ritmo-leg-dot" style="background:#fff;outline:2px dashed #cbd5e1;outline-offset:1px"></span>Recomendada (plan)</span>
        <span><span class="vis-ritmo-leg-dot" style="background:#16a34a"></span>Realizada</span>
        <span><span class="vis-ritmo-leg-dot" style="background:#86b89a"></span>Histórica (pre-sistema)</span>
        <span><span class="vis-ritmo-leg-dot" style="background:#fca5a5"></span>Recomendada no realizada</span>
        <span><span class="vis-ritmo-leg-dot" style="background:#60a5fa"></span>Agendada</span>
        <span><span class="vis-ritmo-leg-dot" style="background:#f59e0b"></span>Agendada atrasada</span>
        <span><span class="vis-ritmo-leg-dot" style="background:#e5e7eb"></span>Sin actividad</span>
        <span style="margin-left:6px"><span class="vis-ritmo-leg-dot" style="background:#fff;box-shadow:0 0 0 2px #dc2626"></span>Mes actual</span>`;
    }
  }

  /* Fallback (código original pre-2026-06-10): círculos por mes con las
     completadas del cache client-side. Solo se usa si el endpoint falla. */
  function _renderRitmoFallback(){
    const cont = document.getElementById('visRitmoTrack');
    const lbl  = document.getElementById('visRitmoCount');
    if (!cont) return;
    const cump = document.getElementById('visRitmoCumpl');
    if (cump) cump.style.display = 'none';
    const visitas = window._visTlVisitas || [];
    const hoy = new Date(); hoy.setDate(1); hoy.setHours(0,0,0,0);

    // 12 meses hacia atrás (incluyendo el mes corriente)
    const meses = [];
    for (let i = 11; i >= 0; i--){
      const d = new Date(hoy.getFullYear(), hoy.getMonth() - i, 1);
      meses.push({
        y: d.getFullYear(), m: d.getMonth(),
        key: `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}`,
        nombre: d.toLocaleDateString('es-CL', {month:'short'}).replace('.', ''),
        es_actual: (d.getFullYear() === hoy.getFullYear() && d.getMonth() === hoy.getMonth()),
        prev: 0, corr: 0,
      });
    }
    const idx = {};
    meses.forEach((m,i) => idx[m.key] = i);

    // Solo visitas realizadas (completada + cerrada)
    let totalCompletadas = 0;
    visitas.forEach(v => {
      const est = (v.estado || '').toLowerCase();
      if (est !== 'completada' && est !== 'cerrada') return;
      const d = _visGetDate(v);
      if (!d) return;
      const k = `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}`;
      if (!(k in idx)) return;
      totalCompletadas++;
      const tipo = (v.tipo || '').toLowerCase();
      if (tipo === 'correctiva') meses[idx[k]].corr++;
      else if (tipo === 'preventiva' || tipo === 'garantia') meses[idx[k]].prev++;
      else meses[idx[k]].prev++; // otras se cuentan como preventiva
    });

    if (lbl) lbl.textContent = `${totalCompletadas} visita${totalCompletadas !== 1 ? 's' : ''} en 12 meses`;

    let html = '';
    meses.forEach(m => {
      let cls = '';
      let tip = `${m.nombre} ${m.y} — Sin visita`;
      if (m.prev > 0 && m.corr > 0){
        cls = 'mixto';
        tip = `${m.nombre} ${m.y} — ${m.prev} prev. + ${m.corr} corr.`;
      } else if (m.prev > 0){
        cls = 'preventiva';
        tip = `${m.nombre} ${m.y} — ${m.prev} preventiva${m.prev>1?'s':''}`;
      } else if (m.corr > 0){
        cls = 'correctiva';
        tip = `${m.nombre} ${m.y} — ${m.corr} correctiva${m.corr>1?'s':''}`;
      }
      if (m.es_actual) cls += ' current-month';
      html += `<div class="vis-ritmo-month">
        <div class="vis-ritmo-dot ${cls}"></div>
        <div class="vis-ritmo-lbl ${m.es_actual ? 'current' : ''}">${m.nombre[0].toUpperCase()}${m.nombre.slice(1,3)}</div>
        <div class="vis-ritmo-tooltip">${tip}</div>
      </div>`;
    });
    cont.innerHTML = html;
  }

  /* FIX 2026-06-10 — flujo en 2 pasos (dry_run → confirm).
     BUG anterior: el POST iba con body {} SIN confirm:true; el backend
     (por diseño anti-creación-masiva del 2026-05-22) respondía dry_run
     y NO creaba nada, pero el front mostraba "✓ 0 visita(s) generada(s)"
     y el usuario creía que calendarizó. Ahora:
       PASO 1: POST {} → el server devuelve las fechas que crearía.
       PASO 2: ilusConfirm con esas fechas → POST {confirm:true} real. */
  async function visCalendarizarProximas(){
    if (!CT_VIGENTE_ID){
      await ilusAlert({title:'Sin contrato', message:'No hay contrato vigente con frecuencia definida.', type:'warning'});
      return;
    }
    try {
      // PASO 1 — dry-run: obtener las fechas propuestas (no inserta nada)
      const r1 = await fetch(`/mantenciones/api/contratos/${CT_VIGENTE_ID}/auto-calendar`, {
        method:'POST', headers:{'Content-Type':'application/json'}, body:'{}'
      });
      const d1 = await r1.json();
      if (!d1.ok){
        await ilusAlert({title:'Error', message:d1.error || 'No se pudo calendarizar', type:'error'});
        return;
      }
      if (!d1.dry_run){
        // ok sin dry_run = no había nada que crear ("sin huecos")
        ilusToast(d1.mensaje || 'Sin huecos — ya estaba calendarizado', {type:'info'});
        return;
      }
      const fechas = Array.isArray(d1.fechas) ? d1.fechas : [];
      const n = d1.preview_count || fechas.length || 0;
      if (!n){
        ilusToast('Sin huecos — ya estaba calendarizado', {type:'info'});
        return;
      }
      const fechasFmt = fechas.slice(0, 8).map(f => {
        try { return new Date(f + 'T00:00:00').toLocaleDateString('es-CL'); } catch(e){ return f; }
      });
      const lista = fechasFmt.join(' · ') + (n > 8 ? ` · y ${n - 8} más` : '');

      // PASO 2 — confirmación con las fechas reales que se crearán
      const ok = await ilusConfirm({
        title: 'Calendarizar próximas mantenciones',
        message: `Se crearán ${n} visita${n !== 1 ? 's' : ''} preventiva${n !== 1 ? 's' : ''} según la frecuencia del contrato vigente:`,
        sub: lista,
        okLabel: `Calendarizar ${n} visita${n !== 1 ? 's' : ''}`,
        cancelLabel: 'Cancelar',
        type: 'question',
      });
      if (!ok) return;

      const r2 = await fetch(`/mantenciones/api/contratos/${CT_VIGENTE_ID}/auto-calendar`, {
        method:'POST', headers:{'Content-Type':'application/json'},
        body: JSON.stringify({confirm: true})
      });
      const d2 = await r2.json();
      if (d2.ok){
        ilusToast(`✓ ${d2.creadas || 0} visita(s) calendarizada(s)`, {type:'success'});
        // Invalidar cache y recargar
        window._visResumenLoaded = false;
        window._visTlLoaded = false;
        await renderVisResumen();
      } else {
        await ilusAlert({title:'Error', message:d2.error || 'No se pudo calendarizar', type:'error'});
      }
    } catch(e){
      await ilusAlert({title:'Error', message:e.message, type:'error'});
    }
  }

  function visAbrirReagendar(vid){
    // Reusa el módulo de OT — abre la visita directamente para que el usuario
    // edite la fecha desde allí (no duplicamos un modal de reagendar).
    window.location.href = `/mantenciones/ot/${vid}`;
  }

  // Arranque: cuando el usuario abre el tab Visitas (o si ya está visible)
  // se cargan los 3 bloques. Se hace de forma diferida con un microtask
  // para no bloquear el render inicial.
  document.addEventListener('DOMContentLoaded', () => {
    // Render diferido — si el tab no está visible no importa, se rinde igual
    // (los nodos ya existen en el DOM). Cache del browser cubre re-aperturas.
    setTimeout(() => { renderVisResumen(); }, 50);
  });

  function visSwitchVista(modo, btn){
    document.querySelectorAll('#visVistaToggle button').forEach(b => b.classList.remove('active'));
    if (btn) btn.classList.add('active');
    const lista = document.getElementById('visVistaLista');
    if (lista) lista.style.display = (modo === 'lista' ? '' : 'none');
    const tl = document.getElementById('visVistaTimeline');
    if (tl) tl.style.display = (modo === 'timeline' ? '' : 'none');
    if (modo === 'timeline'){
      cargarVisTimeline();
    }
  }

  async function cargarVisTimeline(){
    if (window._visTlLoaded){ renderVisTimeline(); return; }
    const cont = document.getElementById('visTlContent');
    cont.innerHTML = '<div class="text-muted text-center py-4 small"><div class="spinner-border spinner-border-sm me-2"></div>Cargando timeline…</div>';
    try {
      // Si el resumen ya cargó las visitas, reusamos el cache y solo
      // pedimos el mapa visita→máquinas (Plan 2026-05-21 — perf <300ms).
      const yaTengoVisitas = !!(window._visResumenLoaded && Array.isArray(window._visTlVisitas) && window._visTlVisitas.length);
      let promesas;
      if (yaTengoVisitas){
        promesas = [
          Promise.resolve({ json: () => Promise.resolve(window._visTlVisitas) }),
          fetch(`/mantenciones/api/clientes/${CID}/visitas-maquinas-map`),
        ];
      } else {
        promesas = [
          fetch(`/mantenciones/api/visitas?cliente_id=${CID}&start=1900-01-01&end=2100-12-31`),
          fetch(`/mantenciones/api/clientes/${CID}/visitas-maquinas-map`),
        ];
      }
      const [vRes, mRes] = await Promise.all(promesas);
      const arr = await vRes.json();
      window._visTlVisitas = Array.isArray(arr) ? arr : [];
      try {
        const mj = await mRes.json();
        const mapa = (mj && mj.ok) ? (mj.map || {}) : {};
        // Adjuntar maquina_ids a cada visita
        window._visTlVisitas.forEach(v => { v.maquina_ids = mapa[v.id] || []; });
      } catch(e){}
      window._visTlLoaded = true;
      renderVisTimeline();
    } catch(e){
      cont.innerHTML = `<div class="alert alert-danger" style="font-size:.82rem">Error: ${e.message}</div>`;
    }
  }

  function _visIsMobile(){ return (window.innerWidth || 800) < 768; }

  function renderVisTimeline(){
    const cont = document.getElementById('visTlContent');
    const rango = parseInt(document.getElementById('visTlRange').value);
    const maquinas = window._visTlMaquinasMap || {};
    const maqIds = Object.keys(maquinas).map(k => parseInt(k));
    if (!maqIds.length){
      cont.innerHTML = `
        <div class="text-muted text-center py-4 small">
          <i class="bi bi-bicycle" style="font-size:1.6rem;opacity:.4;display:block;margin-bottom:6px"></i>
          Sin equipos registrados — la vista Timeline requiere equipos.
        </div>`;
      return;
    }

    // Determinar ventana temporal (basada en fecha visita más antigua / rango)
    const hoy = new Date(); hoy.setDate(1); hoy.setHours(0,0,0,0);
    function _vDate(v){ return v.fecha ? new Date(v.fecha + 'T00:00:00') : (v.start ? new Date(v.start) : null); }
    let inicio;
    if (rango === 0){
      const fechas = (window._visTlVisitas || []).map(v => _vDate(v)).filter(Boolean);
      if (fechas.length){
        const min = new Date(Math.min(...fechas.map(d=>d.getTime())));
        inicio = new Date(min.getFullYear(), min.getMonth(), 1);
      } else {
        inicio = new Date(hoy.getFullYear(), hoy.getMonth() - 5, 1);
      }
    } else {
      inicio = new Date(hoy.getFullYear(), hoy.getMonth() - (rango - 1), 1);
    }
    // Mostrar también próximos 3 meses (visitas futuras programadas)
    const fin = new Date(hoy.getFullYear(), hoy.getMonth() + 3, 1);

    // Generar meses
    const meses = [];
    const cursor = new Date(inicio);
    while (cursor <= fin && meses.length < 36){
      meses.push({ y: cursor.getFullYear(), m: cursor.getMonth() });
      cursor.setMonth(cursor.getMonth() + 1);
    }

    // Bucket: visitas[equipo_id][YYYY-MM] = [visita, ...]
    const visitas = window._visTlVisitas || [];
    const bucket = {};
    visitas.forEach(v => {
      const ms = v.maquina_ids || [];
      const fStart = _vDate(v);
      if (!fStart || isNaN(fStart)) return;
      const ym = `${fStart.getFullYear()}-${String(fStart.getMonth()+1).padStart(2,'0')}`;
      const eqs = (ms.length ? ms : ['_sin_eq']);
      eqs.forEach(eq => {
        const key = String(eq);
        if (!bucket[key]) bucket[key] = {};
        if (!bucket[key][ym]) bucket[key][ym] = [];
        bucket[key][ym].push(v);
      });
    });

    if (_visIsMobile()){
      // ── Mobile: lista vertical agrupada por equipo ────────────
      let html = '';
      maqIds.forEach(eqId => {
        const eq = maquinas[eqId];
        const ymKeys = Object.keys(bucket[String(eqId)] || {}).sort().reverse();
        html += `<div style="border:1px solid var(--border);border-radius:10px;padding:10px 12px;margin-bottom:8px">
          <div class="fw-semibold" style="font-size:.85rem;color:#0f172a">${_visEsc(eq.nombre)}</div>
          <div class="small text-muted">${_visEsc(eq.marca||'')}${eq.serie ? ' · serie '+_visEsc(eq.serie) : ''}</div>
          <div style="margin-top:8px;display:flex;flex-wrap:wrap;gap:4px">`;
        if (!ymKeys.length){
          html += `<span class="small text-muted">Sin visitas en el rango</span>`;
        } else {
          ymKeys.forEach(ym => {
            (bucket[String(eqId)][ym] || []).forEach(v => {
              html += _visTlMobileChip(v);
            });
          });
        }
        html += `</div></div>`;
      });
      cont.innerHTML = html || '<div class="text-muted small py-3">Sin datos</div>';
      return;
    }

    // ── Desktop: grid CSS (equipos × meses) ──────────────────────
    const colTpl = `220px repeat(${meses.length}, minmax(36px, 1fr))`;
    let html = `<div class="vis-tl-header-row" style="grid-template-columns:${colTpl}">
      <div>Equipo</div>` +
      meses.map(m => `<div class="vis-tl-month-lbl">${['Ene','Feb','Mar','Abr','May','Jun','Jul','Ago','Sep','Oct','Nov','Dic'][m.m]} ${String(m.y).slice(-2)}</div>`).join('') +
      `</div>`;
    maqIds.forEach(eqId => {
      const eq = maquinas[eqId];
      html += `<div class="vis-tl-row" style="grid-template-columns:${colTpl}">
        <div class="vis-tl-eq" title="${_visEsc(eq.nombre)}">
          ${_visEsc(eq.nombre)}
          <div class="vis-tl-eq-sub">${_visEsc(eq.marca||'')}${eq.serie ? ' · '+_visEsc(eq.serie) : ''}</div>
        </div>`;
      const eqBucket = bucket[String(eqId)] || {};
      const nowYM = `${(new Date()).getFullYear()}-${String((new Date()).getMonth()+1).padStart(2,'0')}`;
      meses.forEach(m => {
        const ym = `${m.y}-${String(m.m+1).padStart(2,'0')}`;
        const arr = eqBucket[ym] || [];
        const cells = arr.slice(0, 6).map(v => _visTlIcon(v, ym > nowYM)).join('');
        const extra = arr.length > 6 ? `<span style="font-size:.6rem;color:#9ca3af;font-weight:700">+${arr.length-6}</span>` : '';
        html += `<div class="vis-tl-cell">${cells}${extra}</div>`;
      });
      html += `</div>`;
    });
    // "Sin equipo" si hay visitas sin equipo asociado
    if (bucket['_sin_eq']){
      const nowYM = `${(new Date()).getFullYear()}-${String((new Date()).getMonth()+1).padStart(2,'0')}`;
      html += `<div class="vis-tl-row" style="grid-template-columns:${colTpl}">
        <div class="vis-tl-eq" style="color:#9ca3af;font-style:italic">Sin equipo asociado</div>`;
      meses.forEach(m => {
        const ym = `${m.y}-${String(m.m+1).padStart(2,'0')}`;
        const arr = bucket['_sin_eq'][ym] || [];
        const cells = arr.slice(0, 6).map(v => _visTlIcon(v, ym > nowYM)).join('');
        html += `<div class="vis-tl-cell">${cells}</div>`;
      });
      html += `</div>`;
    }
    cont.innerHTML = html;
  }

  function _visEsc(s){ return String(s||'').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'})[c]); }

  function _visTlIcon(v, futura){
    const tipo = (v.tipo || '').toLowerCase();
    const col = _VIS_TIPO_COLOR[tipo] || '#475569';
    const lbl = _VIS_TIPO_ICON[tipo] || '?';
    const esRetro = !!v.es_retroactiva;
    const realCol = esRetro && tipo !== 'retroactiva' ? '#6b7280' : col;
    const dStr = v.fecha || v.start || '';
    const dFmt = dStr ? dStr.substring(0,10) : '';
    const tit = `${(v.title || v.titulo || tipo).replace(/"/g,'')} · ${dFmt}${v.estado ? ' · '+v.estado : ''}`;
    return `<span class="vis-tl-icon ${futura ? 'future' : ''}"
                  style="background:${realCol}"
                  title="${_visEsc(tit)}"
                  onclick="visTlAbrirDetalle(${v.id})">${lbl}</span>`;
  }

  function _visTlMobileChip(v){
    const tipo = (v.tipo || '').toLowerCase();
    const col = _VIS_TIPO_COLOR[tipo] || '#475569';
    const dStr = v.fecha || v.start || '';
    let dFmt = '—';
    try { if (dStr) dFmt = new Date(dStr + (dStr.length === 10 ? 'T00:00:00' : '')).toLocaleDateString('es-CL', {month:'short', day:'2-digit', year:'2-digit'}); } catch(e){}
    const futura = dStr && new Date(dStr) > new Date();
    return `<span style="background:${col};color:#fff;padding:3px 8px;border-radius:50px;font-size:.7rem;font-weight:700;cursor:pointer;opacity:${futura?.5:1}"
                  onclick="visTlAbrirDetalle(${v.id})">
              ${_VIS_TIPO_ICON[tipo] || '?'} ${dFmt}
            </span>`;
  }

  function visTlAbrirDetalle(vid){
    // Abre la OT en la misma pestaña (reusa la página existente).
    window.location.href = `/mantenciones/ot/${vid}`;
  }

  // Re-render en resize para alternar mobile/desktop
  window.addEventListener('resize', (() => {
    let _to;
    return () => {
      clearTimeout(_to);
      _to = setTimeout(() => {
        if (document.getElementById('visVistaTimeline')?.style.display !== 'none'
            && window._visTlLoaded){
          renderVisTimeline();
        }
      }, 250);
    };
  })());

/* ==== from ficha.html lines 7421-7441 ==== */
  function filtrarHistorial(){
    const acc  = (document.getElementById('histFilterAccion').value || '').toLowerCase();
    const usr  = document.getElementById('histFilterUser').value || '';
    const txt  = (document.getElementById('histFilterText').value || '').toLowerCase().trim();
    let visibles = 0;
    document.querySelectorAll('.log-item').forEach(it => {
      const a = (it.dataset.accion || '').toLowerCase();
      const u = it.dataset.usuario || '';
      const t = it.dataset.text || '';
      let show = true;
      if (acc && !a.includes(acc)) show = false;
      if (usr && u !== usr) show = false;
      if (txt && !t.includes(txt)) show = false;
      it.style.display = show ? '' : 'none';
      if (show) visibles++;
    });
    document.getElementById('histCount').textContent = visibles + ' registros' + (visibles !== document.querySelectorAll('.log-item').length ? ' (filtrado)' : '');
    document.getElementById('histEmptyMsg').style.display = visibles ? 'none' : '';
  }

/* ==== from ficha.html lines 7794-7993 ==== */
/* ──────────────────────────────────────────────────────────────
   Visita histórica — JS (Plan 2026-05-21 Capa 8)
   Reemplaza la lógica antigua que no estaba conectada.
   Usa POST /clientes/<cid>/visitas/retroactiva del backend nuevo.
   ────────────────────────────────────────────────────────────── */
window._vhTecnicosLoaded = false;

async function abrirVisitaHistorica(){
  // Reset form
  document.getElementById('vh_fecha').value = '';
  document.getElementById('vh_tipo').value = 'preventiva';
  document.getElementById('vh_titulo').value = '';
  document.getElementById('vh_observaciones').value = '';
  document.getElementById('vh_costo').value = '';
  const cotT = document.getElementById('vh_cot_tido'); if (cotT) cotT.value = '';
  const cotN = document.getElementById('vh_cot_nudo'); if (cotN) cotN.value = '';
  const ocN  = document.getElementById('vh_oc_numero'); if (ocN) ocN.value = '';
  const facT = document.getElementById('vh_fac_tido'); if (facT) facT.value = '';
  const facN = document.getElementById('vh_fac_nudo'); if (facN) facN.value = '';
  document.querySelectorAll('.vh-eq-chk').forEach(c => { c.checked = false; });
  const box = document.getElementById('vh_comercial_box');
  if (box) box.style.display = 'none';
  const car = document.getElementById('vh_comercial_caret');
  if (car){ car.classList.remove('bi-chevron-down'); car.classList.add('bi-chevron-right'); }
  document.getElementById('vh_preview').style.display = 'none';
  document.getElementById('vh_result').style.display  = 'none';
  document.getElementById('vh_btn').disabled = false;

  // Cargar técnicos (1 vez)
  if (!window._vhTecnicosLoaded){
    try {
      const r = await fetch('/mantenciones/api/tecnicos');
      const arr = await r.json();
      const sel = document.getElementById('vh_tecnico');
      if (sel && Array.isArray(arr)){
        sel.innerHTML = '<option value="">— Sin asignar —</option>' +
          arr.filter(t => t.activo).map(t => `<option value="${t.id}">${(t.nombre||'(sin nombre)').replace(/</g,'&lt;')}</option>`).join('');
      }
      window._vhTecnicosLoaded = true;
    } catch(e){ /* fallback: input vacío */ }
  }

  // Default fecha: hace 30 días
  const f = new Date(); f.setDate(f.getDate() - 30);
  document.getElementById('vh_fecha').value = f.toISOString().slice(0,10);

  if (!window._modalVisitaHistorica){
    window._modalVisitaHistorica = new bootstrap.Modal(document.getElementById('modalVisitaHistorica'));
  }
  window._modalVisitaHistorica.show();
}

function vhToggleComercial(){
  const box = document.getElementById('vh_comercial_box');
  const car = document.getElementById('vh_comercial_caret');
  if (!box || !car) return;
  if (box.style.display === 'none'){
    box.style.display = '';
    car.classList.remove('bi-chevron-right');
    car.classList.add('bi-chevron-down');
  } else {
    box.style.display = 'none';
    car.classList.remove('bi-chevron-down');
    car.classList.add('bi-chevron-right');
  }
}

function vhPreview(){
  // Heurística sencilla: solo muestra fecha legible. La sugerencia
  // real de próxima visita la calcula el backend en el response.
  const fStr = document.getElementById('vh_fecha').value;
  if (!fStr){ document.getElementById('vh_preview').style.display='none'; return; }
  try {
    const dt = new Date(fStr + 'T00:00:00').toLocaleDateString('es-CL');
    document.getElementById('vh_preview_text').textContent =
      `Visita histórica con fecha ${dt}. Si el contrato tiene frecuencia configurada, te sugeriré la próxima al guardar.`;
    document.getElementById('vh_preview').style.display = '';
  } catch(e){}
}

async function vhGuardar(){
  const btn = document.getElementById('vh_btn');
  const result = document.getElementById('vh_result');
  result.style.display = 'none';

  const fecha = document.getElementById('vh_fecha').value;
  if (!fecha){
    await ilusAlert({title:'Falta fecha', message:'Indica cuándo se realizó la mantención.', type:'warning'});
    return;
  }
  const tipo  = document.getElementById('vh_tipo').value || 'retroactiva';
  const titulo = (document.getElementById('vh_titulo').value || '').trim();
  const nota_libre = (document.getElementById('vh_observaciones').value || '').trim();
  const tecnico_id = document.getElementById('vh_tecnico').value || null;
  const equipos_ids = Array.from(document.querySelectorAll('.vh-eq-chk:checked')).map(c => parseInt(c.value));

  const cot_tido = document.getElementById('vh_cot_tido')?.value || '';
  const cot_nudo = (document.getElementById('vh_cot_nudo')?.value || '').trim();
  const oc_numero = (document.getElementById('vh_oc_numero')?.value || '').trim();
  const fac_tido = document.getElementById('vh_fac_tido')?.value || '';
  const fac_nudo = (document.getElementById('vh_fac_nudo')?.value || '').trim();

  btn.disabled = true;
  btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Guardando…';

  try {
    const body = {
      fecha, tipo, tecnico_id, equipos_ids,
      nota_libre: titulo ? (titulo + (nota_libre ? '\n' + nota_libre : '')) : nota_libre,
    };
    if (cot_tido && cot_nudo){ body.cotizacion_tido = cot_tido; body.cotizacion_nudo = cot_nudo; }
    if (oc_numero){ body.oc_numero = oc_numero; }
    if (fac_tido && fac_nudo){ body.factura_tido = fac_tido; body.factura_nudo = fac_nudo; }

    const r = await fetch(`/mantenciones/api/clientes/${CID}/visitas/retroactiva`, {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify(body),
    });
    const d = await r.json();
    if (!d.ok) throw new Error(d.error || 'Error al guardar');

    ilusToast('✓ Visita histórica registrada', {type:'success'});

    // ¿Sugerencia de próxima visita? Flujo dry_run → confirm.
    // FIX 2026-06-10 (mismo bug que visCalendarizarProximas): el POST iba
    // SIN confirm:true, el backend respondía dry_run y NO creaba nada
    // aunque el usuario aceptara. Ahora: 1er POST dry-run para obtener
    // las fechas reales, ilusConfirm con esas fechas, 2do POST confirm:true.
    const sug = d.proxima_sugerida;
    if (sug && sug.fecha){
      const ctId = (window.__FICHA_DATA || {}).contrato_vigente_id || null;
      if (!ctId){
        ilusToast('No se pudo identificar contrato vigente — agéndala manualmente', {type:'warning'});
      } else {
        try {
          // PASO 1 — dry-run: el server devuelve las fechas que crearía
          const preR = await fetch(`/mantenciones/api/contratos/${ctId}/auto-calendar`, {
            method:'POST', headers:{'Content-Type':'application/json'},
            body: JSON.stringify({desde_fecha: sug.fecha}),
          });
          const preD = await preR.json();
          if (!preD.ok) throw new Error(preD.error || 'Error');
          if (preD.dry_run && (preD.preview_count || 0) > 0){
            const fechasArr = Array.isArray(preD.fechas) ? preD.fechas : [];
            const nCal = preD.preview_count || fechasArr.length;
            const fechasFmt = fechasArr.slice(0, 8).map(f => {
              try { return new Date(f + 'T00:00:00').toLocaleDateString('es-CL'); } catch(e){ return f; }
            });
            const ok = await ilusConfirm({
              title: 'Calendarizar próximas visitas',
              message: sug.mensaje || 'Sugerimos calendarizar las próximas visitas preventivas.',
              sub: `Frecuencia: ${sug.frecuencia_meses || '—'} meses · Se crearán ${nCal} visita(s): ` +
                   fechasFmt.join(' · ') + (nCal > 8 ? ` · y ${nCal - 8} más` : ''),
              okLabel: `Calendarizar ${nCal} visita(s)`,
              cancelLabel: 'Después',
              type: 'question',
            });
            if (ok){
              // PASO 2 — confirmación real: ahora SÍ se insertan las OTs
              const acR = await fetch(`/mantenciones/api/contratos/${ctId}/auto-calendar`, {
                method:'POST', headers:{'Content-Type':'application/json'},
                body: JSON.stringify({desde_fecha: sug.fecha, confirm: true}),
              });
              const acD = await acR.json();
              if (acD.ok && acD.creadas > 0){
                ilusToast(`✓ ${acD.creadas} visita(s) calendarizadas`, {type:'success'});
              } else if (acD.ok){
                ilusToast('Sin huecos — ya estaba calendarizada', {type:'info'});
              } else {
                throw new Error(acD.error || 'Error');
              }
            }
          } else {
            ilusToast('Sin huecos — ya estaba calendarizada', {type:'info'});
          }
        } catch(e){
          ilusToast('No se pudo calendarizar: ' + e.message, {type:'error'});
        }
      }
    }

    // Cerrar modal y refrescar
    if (window._modalVisitaHistorica) window._modalVisitaHistorica.hide();
    // Refrescar tab Visitas, Finanzas y Timeline (si existen los renders)
    try {
      if (typeof cargarFinanzas === 'function') cargarFinanzas();
    } catch(e){}
    // Refrescar página completa para ver la nueva visita en el tab Visitas
    // (recargar solo el tab requeriría re-render server-side). Hacemos
    // una recarga suave manteniendo el tab actual via hash.
    setTimeout(() => { window.location.reload(); }, 800);
  } catch(e){
    result.innerHTML = `<div class="alert alert-danger" style="font-size:.82rem"><i class="bi bi-x-circle me-1"></i>${e.message}</div>`;
    result.style.display = '';
    btn.disabled = false;
    btn.innerHTML = '<i class="bi bi-save me-1"></i>Registrar mantención';
  }
}


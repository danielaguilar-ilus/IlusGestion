/* ════════════════════════════════════════════════════════
   ILUS — JS de ficha cliente (mantenciones/ficha.html)
   Extraído desde inline el 2026-05-18 para cachear con TTL 30d
   Datos inyectados via window.__FICHA_DATA
   ════════════════════════════════════════════════════════ */
'use strict';

const DATA = window.__FICHA_DATA || {};

const CID = DATA.cid;

// Solo el superadmin puede editar/eliminar reportes (regla de permisos).
// El backend lo blinda con 403; aquí ocultamos los botones para el resto.
const ES_SUPERADMIN = DATA.is_superadmin === true;
window.ES_SUPERADMIN = ES_SUPERADMIN;

// admin o superadmin (regla de permisos). Usado por la pestaña Inteligencia.
const ES_ADMIN = (DATA.is_admin === true);
window.ES_ADMIN = ES_ADMIN;

// ════════════════════════════════════════════════════════════════════
// ACCIONES DE IA (Claude)
// ════════════════════════════════════════════════════════════════════

function _aiOpenModal(title){
  document.getElementById('aiResultTitle').textContent = title;
  document.getElementById('aiResultBody').innerHTML = `
    <div class="text-center py-5">
      <div class="spinner-border text-primary"></div>
      <div class="small text-muted mt-3">Consultando a Claude... (~5-15s)</div>
    </div>`;
  new bootstrap.Modal(document.getElementById('modalAIResult')).show();
}

function _aiRenderError(err){
  document.getElementById('aiResultBody').innerHTML = `
    <div class="alert alert-danger">
      <strong><i class="bi bi-exclamation-triangle me-1"></i>La IA no pudo responder.</strong>
      <div class="mt-2 small">${err}</div>
      <hr class="my-2">
      <div class="small">
        <strong>Diagnóstico rápido:</strong>
        <ul class="mb-0 mt-1">
          <li>Verificá que <code>ANTHROPIC_API_KEY</code> esté seteada en Railway → Variables</li>
          <li>Click en <strong>IA → Diagnóstico de la IA</strong> para ver detalle</li>
        </ul>
      </div>
    </div>`;
}

// 2026-05-22 — Marca el sub-texto del item "Análisis económico" del dropdown IA
// con la elegibilidad actual (cache vigente / cambios detectados / etc.) para
// que el usuario sepa si va a gastar tokens ANTES de hacer click.
(function _aiHintEligibilidad(){
  function paintHint(elig){
    const item = document.querySelector('[onclick*="aiAnalisisCliente"]');
    if (!item) return;
    const sub = item.querySelector('.small.text-muted');
    if (!sub) return;
    const m = (elig && elig.motivo) || 'nunca_analizado';
    const map = {
      cache_vigente: {
        text: `Caché vigente (${elig.dias_desde_ultimo}d / ${elig.throttle_dias}d) — no gasta tokens`,
        color: '#16a34a', icon: 'bi-snow',
      },
      cambios_detectados: {
        text: `Hay ${elig.n_cambios} cambio(s) desde hace ${elig.dias_desde_ultimo}d — preguntará si regenerar`,
        color: '#f59e0b', icon: 'bi-exclamation-circle',
      },
      ventana_abierta: {
        text: `Han pasado ${elig.dias_desde_ultimo}d — recomendable regenerar`,
        color: '#3b82f6', icon: 'bi-arrow-clockwise',
      },
      nunca_analizado: {
        text: 'Aún sin análisis — generará uno nuevo',
        color: '#6b7280', icon: 'bi-stars',
      },
      fecha_indeterminada: {
        text: 'Score, MRR, riesgos, oportunidades',
        color: '#6b7280', icon: 'bi-stars',
      },
    };
    const cfg = map[m] || map.fecha_indeterminada;
    sub.innerHTML = `<i class="bi ${cfg.icon} me-1" style="color:${cfg.color}"></i><span style="color:${cfg.color}">${cfg.text}</span>`;
  }
  // 2026-06-06 (Daniel) — IA eliminada de la ficha del cliente.
  // El auto-fetch a /ia/elegibilidad que pintaba el hint del dropdown IA
  // quedó NEUTRALIZADO: ya no hay dropdown ni acciones IA, así que no se
  // dispara ninguna llamada a la IA al cargar la ficha. paintHint() queda
  // como no-op (no existe el elemento que pintaba).
  void paintHint;
})();

// ── Análisis económico y operativo del cliente ─────────────────────
// 2026-05-22 — throttle inteligente: chequear elegibilidad ANTES de gastar
// tokens. Si hay cache vigente, se devuelve el plan anterior sin llamar a
// Claude. Si hay cambios pero la ventana sigue abierta, preguntamos al
// usuario si quiere regenerar (modal claro con el costo estimado).
async function _aiPreguntarRegenerar(elig){
  // Construir lista de cambios para el modal
  const cambiosList = (elig.cambios || []).map(c => {
    const t = (c.delta != null && c.delta !== 0)
      ? ` (${c.delta > 0 ? '+' : ''}${c.delta})` : '';
    return `<li><strong>${c.campo}</strong>: ${c.antes ?? '—'} → ${c.ahora ?? '—'}${t}</li>`;
  }).join('') || '<li class="text-muted">Sin cambios listados.</li>';

  const html = `
    <div style="font-size:.86rem">
      Han pasado <strong>${elig.dias_desde_ultimo ?? '?'}</strong> día(s) desde el último análisis IA.
      Se detectaron <strong>${elig.n_cambios}</strong> cambio(s):
      <ul style="margin:6px 0 6px 18px;padding:0">${cambiosList}</ul>
      <div style="margin-top:8px;color:#6b7280">
        Costo estimado: <strong>~${elig.costo_estimado_tokens}</strong> tokens.
      </div>
    </div>`;
  return await ilusConfirm({
    title: 'Hay cambios desde el último análisis',
    message: '¿Regenerar el análisis IA?',
    sub: html, subHtml: true,
    okLabel: 'Sí, regenerar', cancelLabel: 'Más tarde',
  });
}

async function aiAnalisisCliente(){
  // ── Paso 1: chequear elegibilidad (sin gastar tokens) ──
  let elig = null;
  try {
    const re = await fetch(`/mantenciones/api/clientes/${CID}/ia/elegibilidad`);
    elig = await re.json();
    if (!elig || !elig.ok) elig = null;
  } catch(_) { elig = null; }

  let force = false;
  if (elig && elig.motivo === 'cache_vigente') {
    // Mostrar el plan en caché — no preguntar nada (no se gasta nada)
  } else if (elig && elig.motivo === 'cambios_detectados') {
    const yes = await _aiPreguntarRegenerar(elig);
    if (!yes) {
      // El usuario decidió no regenerar — dejamos sugerencia auditable
      try {
        await fetch(`/mantenciones/api/clientes/${CID}/ia/diferir`, {
          method:'POST', headers:{'Content-Type':'application/json'},
          body: JSON.stringify({nota: ''}),
        });
        ilusToast('Sugerencia "regenerar IA" guardada en evidencia', { type:'info' });
      } catch(_){}
      return;
    }
    force = true;
  } else if (elig && elig.motivo === 'ventana_abierta') {
    // > THROTTLE días: confirmar igual con resumen breve
    const yes = await ilusConfirm({
      title: 'Han pasado varios días',
      message: `Han pasado ${elig.dias_desde_ultimo} día(s) desde el último análisis. ¿Regenerar ahora?`,
      sub: `Costo estimado: ~${elig.costo_estimado_tokens} tokens.`,
      okLabel: 'Sí, regenerar', cancelLabel: 'Más tarde',
    });
    if (!yes) {
      try {
        await fetch(`/mantenciones/api/clientes/${CID}/ia/diferir`, {
          method:'POST', headers:{'Content-Type':'application/json'},
          body: JSON.stringify({nota: ''}),
        });
        ilusToast('Sugerencia "regenerar IA" guardada en evidencia', { type:'info' });
      } catch(_){}
      return;
    }
    force = true;
  }
  // else: 'nunca_analizado' o fallback → seguir directo

  _aiOpenModal('Análisis económico y operativo · Claude');
  try {
    const r = await fetch(`/mantenciones/api/clientes/${CID}/ai-analisis`, {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify({force}),
    });
    const d = await r.json();
    if (!d.ok){
      // Si el backend pide confirmación (no veníamos con force=true), re-pedir
      if (d.needs_confirmation && d.elegibilidad) {
        const yes = await _aiPreguntarRegenerar(d.elegibilidad);
        if (!yes) {
          bootstrap.Modal.getInstance(document.getElementById('modalAIResult'))?.hide();
          return;
        }
        const r2 = await fetch(`/mantenciones/api/clientes/${CID}/ai-analisis`, {
          method:'POST',
          headers:{'Content-Type':'application/json'},
          body: JSON.stringify({force:true}),
        });
        const d2 = await r2.json();
        if (!d2.ok){ _aiRenderError(d2.error || 'Error'); return; }
        return _aiRenderAnalisis(d2);
      }
      _aiRenderError(d.error || 'Error desconocido'); return;
    }
    _aiRenderAnalisis(d);
  } catch(e){ _aiRenderError(e.message || e); }
}

function _aiRenderAnalisis(d){
    const a = d.ai || {};
    // Banner cuando vino de cache (no se gastaron tokens)
    const cacheBanner = d.cached ? `
      <div class="alert py-2 mb-2" style="background:#fff7ed;border-left:4px solid #f59e0b;color:#92400e">
        <i class="bi bi-snow me-1"></i>
        <strong>Análisis en caché.</strong> No se llamó a Claude — sin gasto de tokens.
        ${d.elegibilidad ? `<small class="d-block mt-1">Generado hace ${d.elegibilidad.dias_desde_ultimo ?? '?'} día(s). Throttle: ${d.elegibilidad.throttle_dias} días.</small>` : ''}
      </div>` : '';
    const saludColor = {
      'excelente':'#16a34a', 'buena':'#22c55e', 'regular':'#f59e0b',
      'riesgo':'#dc2626', 'critica':'#7f1d1d'
    }[a.salud_cuenta] || '#6b7280';
    const oppsHtml = (a.oportunidades_venta||[]).map(o=>`
      <li><strong>${o.titulo}</strong>
        <span class="badge bg-success ms-1">$${(o.valor_estimado_clp||0).toLocaleString('es-CL')}</span>
        <span class="badge bg-secondary ms-1">plazo ${o.plazo}</span></li>
    `).join('') || '<li class="text-muted">No detectadas</li>';
    const accHtml = (a.proximas_acciones||[]).map(x=>`
      <li><span class="badge" style="background:${ {alta:'#dc2626',media:'#f59e0b',baja:'#6b7280'}[x.prioridad] || '#6b7280' }">${x.prioridad}</span>
        ${x.accion} <small class="text-muted">(${x.plazo_dias}d)</small></li>
    `).join('') || '<li class="text-muted">Sin acciones</li>';

    document.getElementById('aiResultBody').innerHTML = cacheBanner + `
      <div class="row g-3">
        <div class="col-md-4 text-center">
          <div style="font-size:.7rem;text-transform:uppercase;color:#6b7280;letter-spacing:.05em">Salud de cuenta</div>
          <div style="font-size:1.5rem;font-weight:900;color:${saludColor}">${(a.salud_cuenta||'—').toUpperCase()}</div>
          <div class="small text-muted">Score: <strong>${a.score_general||0}/100</strong></div>
        </div>
        <div class="col-md-4 text-center">
          <div style="font-size:.7rem;text-transform:uppercase;color:#6b7280;letter-spacing:.05em">MRR estimado</div>
          <div style="font-size:1.4rem;font-weight:900;color:#0f172a">$${(a.mrr_estimado_clp||0).toLocaleString('es-CL')}</div>
          <div class="small text-muted">/ Anual: $${(a.valor_anual_estimado_clp||0).toLocaleString('es-CL')}</div>
        </div>
        <div class="col-md-4 text-center">
          <div style="font-size:.7rem;text-transform:uppercase;color:#6b7280;letter-spacing:.05em">Rentabilidad</div>
          <div style="font-size:1.4rem;font-weight:900;color:#0f172a">${a.rentabilidad_estimada||'—'}</div>
          <div class="small text-muted">Visita cada ${a.frecuencia_visitas_recomendada_meses||'—'} mes(es)</div>
        </div>
        <div class="col-12">
          <div class="alert alert-info py-2 mb-0"><i class="bi bi-info-circle me-1"></i>${a.resumen_ejecutivo||''}</div>
        </div>
        <div class="col-md-6">
          <h6 class="fw-bold mt-2"><i class="bi bi-check-circle text-success me-1"></i>Fortalezas</h6>
          <ul class="small">${(a.fortalezas||[]).map(x=>'<li>'+x+'</li>').join('')||'<li class="text-muted">—</li>'}</ul>
          <h6 class="fw-bold mt-2"><i class="bi bi-x-circle text-danger me-1"></i>Debilidades</h6>
          <ul class="small">${(a.debilidades||[]).map(x=>'<li>'+x+'</li>').join('')||'<li class="text-muted">—</li>'}</ul>
        </div>
        <div class="col-md-6">
          <h6 class="fw-bold mt-2"><i class="bi bi-graph-up-arrow text-success me-1"></i>Oportunidades de venta</h6>
          <ul class="small">${oppsHtml}</ul>
          <h6 class="fw-bold mt-2"><i class="bi bi-exclamation-triangle text-warning me-1"></i>Riesgos inmediatos</h6>
          <ul class="small">${(a.riesgos_inmediatos||[]).map(x=>'<li>'+x+'</li>').join('')||'<li class="text-muted">—</li>'}</ul>
        </div>
        <div class="col-12">
          <h6 class="fw-bold mt-2"><i class="bi bi-list-check text-primary me-1"></i>Próximas acciones</h6>
          <ul class="small">${accHtml}</ul>
        </div>
        <div class="col-12">
          <div class="alert alert-warning py-2 mb-0"><i class="bi bi-bell me-1"></i><strong>Contrato:</strong> ${a.alerta_contrato||'—'}</div>
        </div>
      </div>`;
}

// ── Completar ficha del cliente con sugerencias IA ────────────────
async function aiCompletarFichaCliente(){
  _aiOpenModal('Completar ficha del cliente · Claude');
  try {
    const r = await fetch(`/mantenciones/api/clientes/${CID}/ai-completar-ficha`, {method:'POST'});
    const d = await r.json();
    if (!d.ok){ _aiRenderError(d.error || 'Error'); return; }
    const a = d.ai;
    const aplicadoHtml = Object.entries(d.aplicado||{}).map(([k,v]) =>
      `<li><strong>${k}:</strong> ${v}</li>`
    ).join('') || '<li class="text-muted">No había campos vacíos para llenar</li>';
    const faltantesHtml = (a.campos_faltantes_criticos||[]).map(f=>'<li>'+f+'</li>').join('')
      || '<li class="text-muted">Ninguno</li>';
    const preguntasHtml = (a.preguntas_para_ejecutivo||[]).map(p=>'<li>'+p+'</li>').join('')
      || '<li class="text-muted">Ninguna</li>';

    document.getElementById('aiResultBody').innerHTML = `
      <div class="alert alert-success py-2"><i class="bi bi-magic me-1"></i>
        Confianza del análisis: <strong>${a.confianza||0}%</strong>
      </div>
      <h6 class="fw-bold mt-3"><i class="bi bi-check2-square text-success me-1"></i>Campos completados automáticamente</h6>
      <ul class="small">${aplicadoHtml}</ul>
      <div class="row g-2 mt-2">
        <div class="col-md-6"><strong class="small">Giro sugerido:</strong> ${a.giro_sugerido||'—'}</div>
        <div class="col-md-6"><strong class="small">Tipo de cliente:</strong> ${a.tipo_cliente||'—'}</div>
        <div class="col-md-6"><strong class="small">Comuna estimada:</strong> ${a.comuna_estimada_segun_direccion||'—'}</div>
        <div class="col-md-6"><strong class="small">Región estimada:</strong> ${a.region_estimada||'—'}</div>
        <div class="col-12 mt-2">
          <strong class="small">Observaciones operativas:</strong>
          <div class="small text-muted">${a.observaciones_operativas||'—'}</div>
        </div>
      </div>
      <h6 class="fw-bold mt-3"><i class="bi bi-exclamation-circle text-warning me-1"></i>Campos críticos faltantes</h6>
      <ul class="small">${faltantesHtml}</ul>
      <h6 class="fw-bold mt-3"><i class="bi bi-question-circle text-info me-1"></i>Preguntas que deberías hacer al cliente</h6>
      <ul class="small">${preguntasHtml}</ul>
      <div class="text-center mt-3">
        <button class="btn btn-sm btn-outline-primary" onclick="window.location.reload()">
          <i class="bi bi-arrow-clockwise me-1"></i>Recargar ficha
        </button>
      </div>`;
  } catch(e){ _aiRenderError(e.message || e); }
}

// ── Diagnóstico de la IA ───────────────────────────────────────────
async function aiHealth(){
  _aiOpenModal('Diagnóstico de la IA · Claude');
  try {
    const r = await fetch('/mantenciones/api/ai/health');
    const d = await r.json();
    const row = (lbl, ok, val) => `
      <tr>
        <td class="fw-semibold">${lbl}</td>
        <td><span class="badge bg-${ok?'success':'danger'}">${ok?'OK':'FALLA'}</span></td>
        <td class="small text-muted">${val||''}</td>
      </tr>`;
    document.getElementById('aiResultBody').innerHTML = `
      <table class="table table-sm">
        <tbody>
          ${row('API Key configurada', d.key_configurada, d.key_configurada?'ANTHROPIC_API_KEY presente':'Configurar en Railway → Variables')}
          ${row('Librería anthropic instalada', d.anthropic_lib, d.anthropic_lib?'OK':'Agregar a requirements.txt')}
          ${row('Modelo Claude responde', d.modelo_funciona, d.modelo_usado||'')}
        </tbody>
      </table>
      ${d.error ? `<div class="alert alert-danger small"><i class="bi bi-x-circle me-1"></i>${d.error}</div>` : ''}
      ${d.modelo_funciona ? '<div class="alert alert-success small"><i class="bi bi-check-circle me-1"></i>La IA está completamente funcional.</div>' : ''}`;
  } catch(e){ _aiRenderError(e.message || e); }
}

// ─── Tabs con localStorage ─────────────────────────────────────
const TAB_KEY = `ficha_tab_${CID}`;
function switchTab(name) {
  document.querySelectorAll('.ftab-pane').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.ftab-btn').forEach(b => b.classList.remove('active'));
  document.getElementById(`tab-${name}`).classList.add('active');
  document.querySelector(`.ftab-btn[data-tab="${name}"]`).classList.add('active');
  localStorage.setItem(TAB_KEY, name);
}
(function() {
  const saved = localStorage.getItem(TAB_KEY);
  if (saved && document.getElementById(`tab-${saved}`)) switchTab(saved);
})();

// ─── Filtro de equipos ─────────────────────────────────────────
function filtrarEquipos(q) {
  const norm = q.toLowerCase().trim();
  let anyVisible = false;
  document.querySelectorAll('#maqListado .eq-item').forEach(el => {
    const name = el.getAttribute('data-eq-name') || '';
    const show = !norm || name.includes(norm);
    el.style.display = show ? '' : 'none';
    if (show) anyVisible = true;
  });
  const empty = document.getElementById('eqEmptyState');
  if (empty) empty.style.display = anyVisible ? 'none' : '';
}

// ─── Modales ─────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════
//  MODAL EDITAR CLIENTE — formato chileno en vivo + dirección Places
//  Daniel 30/05/2026: "el RUT no tiene formato de RUT… la dirección
//  que arregle automáticamente la comuna, la ciudad, la región, todo,
//  pero tiene que validar la dirección… algo bien inteligente."
//
//  - RUT  → 77.753.941-8  (idempotente con _formato_rut_chile backend)
//  - Tel  → +56 9 1234 5678 (móvil) / +56 2 2345 6789 (fijo)
//  - Dir  → Google Places: al elegir sugerencia valida y auto-rellena
//           comuna / ciudad / región + persiste lat/lng/place_id.
// ═══════════════════════════════════════════════════════════════

// RUT chileno legible. Extrae dígitos+K primero → idempotente.
function _ilusFmtRut(raw) {
  const clean = String(raw || '').replace(/[^0-9kK]/g, '').toUpperCase();
  if (!clean) return '';
  if (clean.length < 2) return clean;
  const cuerpo = clean.slice(0, -1);
  const dv     = clean.slice(-1);
  let out = '';
  for (let i = 0; i < cuerpo.length; i++) {
    if (i > 0 && (cuerpo.length - i) % 3 === 0) out += '.';
    out += cuerpo[i];
  }
  return out + '-' + dv;
}

// Teléfono chileno legible. Extrae dígitos primero → idempotente.
function _ilusFmtTel(raw) {
  let d = String(raw || '').replace(/\D/g, '');
  if (!d) return String(raw || '').trim();
  if (d.startsWith('56')) d = d.slice(2);          // quitar país
  if (d.length === 9 && d[0] === '9') {            // móvil
    return '+56 9 ' + d.slice(1, 5) + ' ' + d.slice(5);
  }
  if (d.length === 9) {                            // fijo c/código área (1 díg)
    return '+56 ' + d[0] + ' ' + d.slice(1, 5) + ' ' + d.slice(5);
  }
  if (d.length === 8) {                            // fijo s/código → Santiago (2)
    return '+56 2 ' + d.slice(0, 4) + ' ' + d.slice(4);
  }
  return String(raw || '').trim();                 // no reconocible: dejar como está
}

// Aplica máscara a un input (on blur siempre; on input opcional para RUT).
function _ilusBindFmt(el, fmtFn, liveInput) {
  if (!el || el.dataset.ilusFmt === '1') return;
  el.dataset.ilusFmt = '1';
  el.addEventListener('blur', function () {
    const v = fmtFn(el.value);
    if (v !== el.value) el.value = v;
  });
  if (liveInput) {
    el.addEventListener('input', function () {
      const v = fmtFn(el.value);
      if (v !== el.value) { el.value = v; }       // RUT secuencial: cursor al final OK
    });
  }
}

// "Región Metropolitana de Santiago" → "Metropolitana" (más legible).
function _ilusLimpiaRegion(r) {
  if (!r) return r;
  let s = String(r).replace(/^Regi[oó]n\s+(de\s+|del\s+|de\s+la\s+)?/i, '').trim();
  s = s.replace(/\s+de\s+Santiago$/i, '');
  return s || String(r);
}

// Inicializa Google Places en el campo dirección del modal (idempotente).
function _ilusInitDireccionCliente() {
  const input = document.getElementById('ec_direccion');
  if (!input || input.dataset.placesBound === '1') return;
  if (typeof ilusPlacesAutocomplete !== 'function') {
    // SDK aún no cargó: encolar y reintentar cuando __ilusGmapsReady dispare.
    if (window.__ilusGmapsPending) window.__ilusGmapsPending.push(_ilusInitDireccionCliente);
    return;
  }
  input.dataset.placesBound = '1';
  ilusPlacesAutocomplete('ec_direccion', {
    country: 'cl',
    types: ['address'],
    onPlaceSelected: function (place) {
      const set = (id, v) => { const e = document.getElementById(id); if (e) e.value = v; };
      set('ec_direccion_lat',      place.lat || '');
      set('ec_direccion_lng',      place.lng || '');
      set('ec_direccion_place_id', place.place_id || '');
      // Parsear address_components de Google → campos chilenos.
      const comps = place.componentes || [];
      const pick = function () {
        for (let i = 0; i < arguments.length; i++) {
          const t = arguments[i];
          const c = comps.find(x => (x.types || []).indexOf(t) >= 0);
          if (c) return c.long_name;
        }
        return '';
      };
      // CL: level_1=Región · level_3/locality=Comuna · locality/level_2=Ciudad
      const region = pick('administrative_area_level_1');
      const comuna = pick('administrative_area_level_3', 'locality', 'sublocality_level_1');
      const ciudad = pick('locality', 'administrative_area_level_2') || comuna;
      const setIf  = (id, v) => { const e = document.getElementById(id); if (e && v) e.value = v; };
      setIf('ec_region', _ilusLimpiaRegion(region));
      setIf('ec_comuna', comuna);
      setIf('ec_ciudad', ciudad);
      const hint = document.getElementById('ec_direccion_hint');
      if (hint) {
        const la = (typeof place.lat === 'number') ? place.lat.toFixed(4) : '?';
        const ln = (typeof place.lng === 'number') ? place.lng.toFixed(4) : '?';
        hint.innerHTML = '<i class="bi bi-check-circle-fill me-1" style="color:#16a34a"></i>' +
          'Dirección verificada · <small>' + la + ', ' + ln + '</small>';
      }
    },
    onNoSelection: function () {
      const hint = document.getElementById('ec_direccion_hint');
      if (hint) hint.innerHTML = '<i class="bi bi-exclamation-triangle me-1" style="color:#f59e0b"></i>' +
        'Selecciona una opción del menú para validar la dirección.';
    }
  });
}

// Configura formato + autocomplete cada vez que se abre el modal (idempotente).
function _ilusSetupEditarCliente(modalEl) {
  try { _ilusBindFmt(document.getElementById('ec_rut'), _ilusFmtRut, true); } catch (e) {}
  try {
    (modalEl || document).querySelectorAll('.ec-tel').forEach(function (el) {
      _ilusBindFmt(el, _ilusFmtTel, false);
    });
  } catch (e) {}
  try { _ilusInitDireccionCliente(); } catch (e) {}
}

function abrirEditarCliente() {
  const modalEl = document.getElementById('modalEditarCliente');
  if (!modalEl) return;
  new bootstrap.Modal(modalEl).show();
  // Configurar tras shown.bs.modal: Google Autocomplete necesita el input
  // ya visible para medir bien la posición del dropdown.
  modalEl.addEventListener('shown.bs.modal', function onShown() {
    modalEl.removeEventListener('shown.bs.modal', onShown);
    _ilusSetupEditarCliente(modalEl);
  }, { once: true });
}

// Atajo directo desde la KPI "Tipo cliente" del tab Resumen (Daniel 22/05/2026).
// Abre el modal, hace scroll al campo tipo_cliente y lo resalta brevemente
// para que sea obvio dónde cambiar. Si el usuario es técnico no debería poder
// llegar aquí (botón oculto en el template).
function abrirEditarTipoCliente() {
  const modalEl = document.getElementById('modalEditarCliente');
  if (!modalEl) return;
  const modal = new bootstrap.Modal(modalEl);
  modal.show();
  // Esperar a que termine la animación de apertura de Bootstrap (~250 ms)
  // antes de hacer scroll + focus, sino el select aún no está renderizado.
  modalEl.addEventListener('shown.bs.modal', function onShown() {
    modalEl.removeEventListener('shown.bs.modal', onShown);
    try { _ilusSetupEditarCliente(modalEl); } catch(_e) {}  // formato RUT/tel + dirección Places
    const sel = document.getElementById('ec_tipo_cliente');
    if (!sel) return;
    try { sel.scrollIntoView({ behavior: 'smooth', block: 'center' }); } catch(_e) {}
    setTimeout(() => {
      try { sel.focus(); } catch(_e) {}
      // Resaltar brevemente con un anillo rojo ILUS para guiar el ojo
      const wrap = sel.parentElement;
      if (wrap) {
        const prev = wrap.style.boxShadow;
        wrap.style.transition = 'box-shadow .25s ease';
        wrap.style.boxShadow = '0 0 0 4px rgba(220,38,38,.18), 0 0 0 1px #dc2626';
        setTimeout(() => { wrap.style.boxShadow = prev || ''; }, 1800);
      }
    }, 150);
  }, { once: true });
}
window.abrirEditarTipoCliente = abrirEditarTipoCliente;
function abrirErpModal() {
  new bootstrap.Modal(document.getElementById('modalErp')).show();
}
function abrirMaquinaManual() {
  ['mm_nombre','mm_sku','mm_serie','mm_doc','mm_notas'].forEach(id => {
    const el = document.getElementById(id);
    if (el) el.value = '';
  });
  document.getElementById('mm_cantidad').value = 1;
  new bootstrap.Modal(document.getElementById('modalMaqManual')).show();
}
function abrirContratoModal() {
  new bootstrap.Modal(document.getElementById('modalContrato')).show();
}
// ════════════════════════════════════════════════════════════════════
//  LEVANTAMIENTO FOTOGRÁFICO DE EQUIPOS
//  Flujo completo: técnico abre modal → selecciona equipos → captura
//  fotos con la cámara → cierra el levantamiento → queda en timeline.
//  Storage: Cloudinary persistente. Estados visuales por equipo.
// ════════════════════════════════════════════════════════════════════
// _LEV ya no almacena sesión de captura — solo el id de la última OT creada.
// La captura se hace desde la OT (módulo "Órdenes de Trabajo"), no desde la ficha.
// adjuntos_preliminares: archivos que el admin selecciona ANTES de crear la OT.
// Se acumulan en memoria; se suben tras crear la OT (cuando ya tenemos visita_id).
let _LEV = { id: null, adjuntos_preliminares: [] };

// ════════════════════════════════════════════════════════════
// ENTRY POINT: abre el modal de "Generar OT" (modal generalizado)
// Soporta varios tipos: levantamiento, instalacion, preventiva,
// correctiva, visita_tecnica, etc. Por compat, "Levantamiento de
// ficha" === "levantamiento fotográfico" (mismo tipo).
// ════════════════════════════════════════════════════════════
async function abrirGenerarOT(tipoPreset){
  abrirLevantamientoSelector(tipoPreset);
}
// Alias por compat con llamadas viejas
const abrirLevantamientoFotografico = abrirGenerarOT;

// Estado del modal de levantamiento (técnicos disponibles + seleccionados)
const _LEV_MODAL = { tecnicos_disponibles: [], tecnicos_seleccionados: new Set() };

// Plantillas disponibles (cache global del modal)
const _LEV_PLANTILLAS = { all: [], cargadas: false };

// Contactos del cliente (cache por cliente)
const _LEV_CONTACTOS = { lista: [], cargados: false, manual: false };

// ════════════════════════════════════════════════════════════
// Carga contactos del cliente para el selector del modal
// ════════════════════════════════════════════════════════════
async function _cargarContactos(){
  if (_LEV_CONTACTOS.cargados) return _LEV_CONTACTOS.lista;
  try {
    const r = await fetch(`/mantenciones/api/clientes/${CID}/contactos`);
    const d = await r.json();
    _LEV_CONTACTOS.lista = (d.ok && d.contactos) ? d.contactos : [];
    _LEV_CONTACTOS.cargados = true;
  } catch(e) {
    console.warn('Contactos load:', e);
    _LEV_CONTACTOS.lista = [];
  }
  return _LEV_CONTACTOS.lista;
}

function _renderContactosSelector(){
  const sel = document.getElementById('levContactoSel');
  if (!sel) return;
  const lista = _LEV_CONTACTOS.lista || [];
  let html = '<option value="">— Selecciona un contacto —</option>';
  lista.forEach((c, i) => {
    const meta = [c.cargo, c.tel].filter(Boolean).join(' · ');
    html += `<option value="${i}">${c.label || c.nombre} — ${c.nombre}${meta ? ' (' + meta + ')' : ''}</option>`;
  });
  html += '<option value="__manual">+ Ingresar manualmente</option>';
  sel.innerHTML = html;
}

function onContactoChange(){
  const sel = document.getElementById('levContactoSel');
  const box = document.getElementById('levContactoBox');
  const v = sel.value;
  if (v === '__manual'){
    _LEV_CONTACTOS.manual = true;
    box.style.display = '';
    document.getElementById('levContactoNombre').value = '';
    document.getElementById('levContactoCargo').value = '';
    document.getElementById('levContactoTel').value = '';
    document.getElementById('levContactoEmail').value = '';
    sel.dataset.origen = 'manual';
  } else if (v === ''){
    _LEV_CONTACTOS.manual = false;
    box.style.display = 'none';
    sel.dataset.origen = '';
  } else {
    _LEV_CONTACTOS.manual = false;
    const idx = parseInt(v);
    const c = _LEV_CONTACTOS.lista[idx];
    if (c){
      // Auto-fill (visible para que el usuario confirme/edite)
      box.style.display = '';
      document.getElementById('levContactoNombre').value = c.nombre || '';
      document.getElementById('levContactoCargo').value = c.cargo || '';
      document.getElementById('levContactoTel').value = c.tel || '';
      document.getElementById('levContactoEmail').value = c.email || '';
      sel.dataset.origen = c.origen || 'principal';
    }
  }
}

function toggleContactoManual(){
  const sel = document.getElementById('levContactoSel');
  sel.value = '__manual';
  onContactoChange();
}

// Mapa de plantillas extra seleccionadas por equipo { maquinaId: Set<plantillaId> }
const _LEV_EQ_PLANTILLAS = {};

async function _cargarPlantillas(){
  if (_LEV_PLANTILLAS.cargadas) return _LEV_PLANTILLAS.all;
  try {
    const r = await fetch('/mantenciones/api/plantillas?activa=1');
    const d = await r.json();
    _LEV_PLANTILLAS.all = Array.isArray(d) ? d : (d.plantillas || []);
    _LEV_PLANTILLAS.cargadas = true;
  } catch(e){ console.warn('cargar plantillas:', e); }
  return _LEV_PLANTILLAS.all;
}

async function abrirLevantamientoSelector(tipoPreset){
  const tbody = document.getElementById('levSelectTbody');
  if (!tbody) { ilusToast('Modal no inicializado', { type:'error' }); return; }
  // Cargar equipos desde la página actual (los que ya están renderizados en tab Equipos)
  const filas = Array.from(document.querySelectorAll('[data-maquina-id]'));
  if (!filas.length){
    ilusAlert({
      title: 'Sin equipos',
      message: 'Este cliente no tiene equipos registrados todavía.',
      sub: 'Agrega equipos primero desde la pestaña Equipos (manual o Importar ERP).',
      type: 'warning',
    });
    return;
  }

  // Cargar plantillas para el selector multi-plantilla
  await _cargarPlantillas();

  // Cargar contactos del cliente y poblar selector (auto-seleccionar el
  // primero — usualmente el contacto principal — para que el campo no
  // quede vacío por descuido del admin)
  await _cargarContactos();
  _renderContactosSelector();
  if (_LEV_CONTACTOS.lista.length > 0){
    const sel = document.getElementById('levContactoSel');
    sel.value = '0';  // primer contacto = principal
    onContactoChange();
  }

  // Reset multi-plantilla por equipo
  Object.keys(_LEV_EQ_PLANTILLAS).forEach(k => delete _LEV_EQ_PLANTILLAS[k]);

  // Equipos DESELECCIONADOS por defecto — el usuario decide qué levantar.
  // Botón de plantillas extra solo HABILITADO si el equipo está marcado
  // (evita confusión: agregar plantillas a equipos no seleccionados).
  tbody.innerHTML = filas.map(tr => {
    const mid = tr.dataset.maquinaId;
    const nombre = (tr.querySelector('.eq-name-main')?.textContent || '').trim() || `Equipo #${mid}`;
    const sku    = tr.dataset.sku || '';
    const serie  = tr.dataset.serie || '';
    return `<tr style="cursor:pointer" onclick="const c=this.querySelector('.lev-eq-chk');c.checked=!c.checked;levRecalcEqCount();event.stopPropagation();">
      <td><input type="checkbox" class="lev-eq-chk" data-id="${mid}" onchange="levRecalcEqCount()" onclick="event.stopPropagation()"></td>
      <td>
        <strong>${escHtml(nombre)}</strong>
        ${sku?`<div class="small text-muted">${escHtml(sku)}</div>`:''}
        ${serie?`<div class="small text-muted">S/N: ${escHtml(serie)}</div>`:''}
      </td>
      <td onclick="event.stopPropagation()">
        <button id="lev-pl-btn-${mid}" class="btn btn-xs btn-outline-primary w-100"
                style="font-size:.72rem;padding:.25rem .4rem;opacity:.4;pointer-events:none"
                onclick="abrirMultiPlantilla(${mid}, '${escAttr(nombre)}')"
                title="Selecciona el equipo primero">
          <i class="bi bi-lock me-1"></i><span id="lev-pl-count-${mid}">marca el equipo</span>
        </button>
      </td>
    </tr>`;
  }).join('');
  // Tipo de OT: preset si se pidió (Programar mantención = 'preventiva'); si no, 'levantamiento'.
  const tipoSel = document.getElementById('otTipo');
  if (tipoSel) {
    const _wanted = tipoPreset || 'levantamiento';
    const _has = Array.from(tipoSel.options).some(o => o.value === _wanted);
    tipoSel.value = _has ? _wanted : 'levantamiento';
  }
  onTipoOtChange(); // actualiza descripción + título sugerido

  document.getElementById('levSelectNotas').value = '';

  // Default dirección: la del cliente (editable por el usuario).
  // Si Google Maps está disponible, conecta autocomplete.
  const dirInput = document.getElementById('levDireccion');
  if (dirInput){
    const dirCliente = DATA.cliente_direccion;
    const comunaCliente = DATA.cliente_comuna;
    let dirCompleta = dirCliente;
    if (comunaCliente && !dirCompleta.toLowerCase().includes(comunaCliente.toLowerCase())){
      dirCompleta = (dirCompleta ? dirCompleta + ', ' : '') + comunaCliente;
    }
    dirInput.value = dirCompleta || '';
    // Inicializar autocomplete solo una vez por modal
    if (!dirInput.dataset.placesInit && typeof ilusPlacesAutocomplete === 'function'){
      ilusPlacesAutocomplete(dirInput, {
        country: 'cl',
        types: ['address'],
        onPlaceSelected: (place) => {
          const hint = document.getElementById('levDireccionHint');
          if (hint){
            hint.innerHTML = '<i class="bi bi-check-circle-fill text-success me-1"></i>' +
              'Dirección verificada por Google Maps · ' +
              `<small>${place.lat.toFixed(4)}, ${place.lng.toFixed(4)}</small>`;
          }
          // Guardar lat/lng en datasets para enviar después
          dirInput.dataset.lat = place.lat;
          dirInput.dataset.lng = place.lng;
          dirInput.dataset.placeId = place.place_id || '';
        },
      });
    }
  }

  // Default: fecha hoy
  const hoy = new Date();
  const yyyy = hoy.getFullYear();
  const mm = String(hoy.getMonth()+1).padStart(2,'0');
  const dd = String(hoy.getDate()).padStart(2,'0');
  document.getElementById('levFechaProg').value = `${yyyy}-${mm}-${dd}`;
  document.getElementById('levFechaFin').value = '';
  document.getElementById('levRangoDias').checked = false;
  document.getElementById('levFechaFinWrap').style.display = 'none';
  document.getElementById('levHoraIni').value = '09:00';
  document.getElementById('levHoraFin').value = '13:00';

  // Reset técnicos seleccionados
  _LEV_MODAL.tecnicos_seleccionados.clear();

  // Reset toggle button (todo deseleccionado al abrir)
  const tBtn = document.getElementById('btnLevToggleTodos');
  if (tBtn) tBtn.innerHTML = '<i class="bi bi-check2-square me-1"></i>Marcar todos';

  // Reset campos acceso/logística y adjuntos preliminares al abrir
  resetAccesoLogistica();
  resetLevAdjuntos();

  // Recalcular contadores
  levRecalcEqCount();
  levRenderTecnicos();

  new bootstrap.Modal(document.getElementById('modalLevSelector')).show();

  // Cargar técnicos asíncronamente
  try {
    const r = await fetch('/mantenciones/api/tecnicos');
    const d = await r.json();
    _LEV_MODAL.tecnicos_disponibles = Array.isArray(d) ? d : (d.tecnicos || []);
    levRenderTecnicos();
  } catch(e){
    document.getElementById('levTecnicosBox').innerHTML =
      '<span class="text-danger small">⚠ No se pudieron cargar los técnicos</span>';
  }
}

function levRenderTecnicos(){
  const box = document.getElementById('levTecnicosBox');
  if (!box) return;
  const techs = _LEV_MODAL.tecnicos_disponibles || [];
  if (!techs.length){
    // Si el usuario tiene permisos admin, le mostramos un link directo a Usuarios
    // (donde se crean app_users con rol=tecnico). Si no, le pedimos coordinar
    // con un superadmin/admin. El módulo legacy mant_tecnicos ya NO se usa.
    if (DATA.can_create_tecnicos) {
    box.innerHTML = '<div class="alert alert-warning py-2 mb-0 small w-100">' +
                    '<i class="bi bi-exclamation-triangle me-1"></i>' +
                    'No hay técnicos activos. ' +
                    '<a href="/admin/users?rol=tecnico" target="_blank" class="fw-bold">Crear uno en Usuarios →</a> ' +
                    '(asigna rol "Técnico" al crear)' +
                    '</div>';
} else {
    box.innerHTML = '<div class="alert alert-warning py-2 mb-0 small w-100">' +
                    '<i class="bi bi-exclamation-triangle me-1"></i>' +
                    'No hay técnicos activos. ' +
                    'Solicita a un administrador que cree un usuario con rol "Técnico".' +
                    '</div>';
}
    document.getElementById('levTecCount').textContent = '0';
    return;
  }
  box.innerHTML = techs.map(t => {
    const isSelected = _LEV_MODAL.tecnicos_seleccionados.has(t.id);
    const bg = isSelected ? 'background:linear-gradient(135deg,#1e40af,#3b82f6);color:#fff;border-color:#1e40af' : 'background:#fff;color:#0f172a;border-color:#cbd5e1';
    const icon = isSelected ? 'bi-check-circle-fill' : 'bi-person';
    return `<span class="badge rounded-pill border" style="cursor:pointer;padding:.5rem .85rem;font-size:.82rem;font-weight:500;${bg}"
                  onclick="levToggleTecnico(${t.id})">
              <i class="bi ${icon} me-1"></i>${escHtml(t.nombre || t.email || ('Téc #'+t.id))}
            </span>`;
  }).join('');
  document.getElementById('levTecCount').textContent = String(_LEV_MODAL.tecnicos_seleccionados.size);
}

function levToggleTecnico(tid){
  if (_LEV_MODAL.tecnicos_seleccionados.has(tid)){
    _LEV_MODAL.tecnicos_seleccionados.delete(tid);
  } else {
    _LEV_MODAL.tecnicos_seleccionados.add(tid);
  }
  levRenderTecnicos();
}

function levRecalcEqCount(){
  const checks = document.querySelectorAll('.lev-eq-chk');
  const n = document.querySelectorAll('.lev-eq-chk:checked').length;
  const tot = checks.length;
  const el = document.getElementById('levEqCount');
  if (el) el.textContent = String(n);
  // Actualizar el texto del botón toggle según el estado
  const tBtn = document.getElementById('btnLevToggleTodos');
  if (tBtn){
    if (n === tot && tot > 0){
      tBtn.innerHTML = '<i class="bi bi-square me-1"></i>Desmarcar todos';
    } else {
      tBtn.innerHTML = '<i class="bi bi-check2-square me-1"></i>Marcar todos';
    }
  }
  // Habilitar/deshabilitar botones de plantillas extra según selección del equipo
  checks.forEach(c => {
    const mid = c.dataset.id;
    const plBtn = document.getElementById('lev-pl-btn-' + mid);
    if (!plBtn) return;
    const seleccionado = c.checked;
    const tienePlantillas = _LEV_EQ_PLANTILLAS[mid] && _LEV_EQ_PLANTILLAS[mid].size > 0;
    if (seleccionado){
      plBtn.style.opacity = '1';
      plBtn.style.pointerEvents = 'auto';
      plBtn.title = 'Agregar plantillas extra a este equipo';
      // Actualizar texto según si ya tiene plantillas
      const countSpan = document.getElementById('lev-pl-count-' + mid);
      if (countSpan){
        if (tienePlantillas){
          const n = _LEV_EQ_PLANTILLAS[mid].size;
          countSpan.textContent = `${n} plantilla${n>1?'s':''} extra`;
          plBtn.querySelector('i').className = 'bi bi-list-check me-1';
        } else {
          countSpan.textContent = '+ agregar plantillas';
          plBtn.querySelector('i').className = 'bi bi-plus-circle me-1';
        }
      }
    } else {
      plBtn.style.opacity = '.4';
      plBtn.style.pointerEvents = 'none';
      plBtn.title = 'Selecciona el equipo primero';
      const countSpan = document.getElementById('lev-pl-count-' + mid);
      if (countSpan) countSpan.textContent = 'marca el equipo';
      plBtn.querySelector('i').className = 'bi bi-lock me-1';
      // Limpiar selección de plantillas si se desmarca el equipo
      if (_LEV_EQ_PLANTILLAS[mid]) delete _LEV_EQ_PLANTILLAS[mid];
    }
  });
}

// Toggle único: si todo está marcado, desmarca todo. Si no, marca todo.
function levToggleTodos(){
  const checks = document.querySelectorAll('.lev-eq-chk');
  const marcados = document.querySelectorAll('.lev-eq-chk:checked').length;
  const newState = marcados < checks.length; // si NO están todos marcados, marca todos; si lo están, desmarca
  checks.forEach(c => c.checked = newState);
  levRecalcEqCount();
}

// ════════════════════════════════════════════════════════════
// SELECTOR DE TIPO DE OT — actualiza descripción según el tipo
// ════════════════════════════════════════════════════════════
const _OT_TIPO_DESCRIPCIONES = {
  'levantamiento':  'Levantamiento de ficha: documentación visual de cada equipo. Se aplica automáticamente la plantilla estándar a todos los equipos seleccionados.',
  'instalacion':    'Instalación: registro de equipos nuevos puestos en sitio. Incluye verificación de embalaje, conexión, encendido y capacitación.',
  'preventiva':     'Mantención preventiva: visita planificada con checklist estándar (limpieza, lubricación, ajustes, test de carga).',
  'visita_tecnica': 'Visita técnica: atención puntual al cliente para diagnóstico, ajustes o consultas técnicas.',
  'correctiva':    'Mantención correctiva: reparación de falla específica reportada por el cliente.',
  'inspeccion':     'Inspección: revisión visual + funcional sin intervención, para diagnóstico o auditoría.',
};
function onTipoOtChange(){
  const tipo = document.getElementById('otTipo')?.value;
  const desc = document.getElementById('otTipoDescripcion');
  if (desc && tipo){
    desc.innerHTML = '<i class="bi bi-info-circle me-1"></i>' + (_OT_TIPO_DESCRIPCIONES[tipo] || '');
  }
  // Garantía aplica a TODOS los tipos excepto levantamiento (es flag opcional)
  const garWrap = document.getElementById('otGarantiaWrap');
  if (garWrap){
    if (tipo === 'levantamiento'){
      garWrap.style.display = 'none';
      const gChk = document.getElementById('otAplicaGarantia');
      if (gChk) gChk.checked = false;
    } else {
      garWrap.style.display = '';
    }
  }
  // Sugerir título según tipo si está vacío o tiene un prefijo conocido
  const tit = document.getElementById('levSelectTitulo');
  if (tit && (!tit.value || tit.value.startsWith('Levantamiento ') || tit.value.startsWith('Instalación ') || tit.value.startsWith('Mantención ') || tit.value.startsWith('Visita ') || tit.value.startsWith('Inspección '))){
    const fecha = new Date().toLocaleDateString('es-CL');
    const labels = {
      levantamiento: 'Levantamiento',
      instalacion: 'Instalación',
      preventiva: 'Mantención preventiva',
      visita_tecnica: 'Visita técnica',
      correctiva: 'Mantención correctiva',
      inspeccion: 'Inspección',
    };
    tit.value = `${labels[tipo] || 'OT'} ${fecha}`;
  }
}

// ════════════════════════════════════════════════════════════
// Boton "Nuevo tipo" — solo superadmin. Por ahora muestra modal
// informativo explicando cómo agregar (requiere ALTER del ENUM).
// ════════════════════════════════════════════════════════════
async function abrirCrearTipoOT(){
  await ilusAlert({
    title: 'Crear nuevo tipo de OT',
    message: 'Esta función está en desarrollo (próxima fase).',
    sub: 'Por ahora, los tipos están definidos en el ENUM de la tabla mant_visitas. ' +
         'Para agregar un tipo nuevo se requiere migración manual. ' +
         'Si necesitas un tipo urgente, contáctame directamente.',
    type: 'info',
  });
}

// ════════════════════════════════════════════════════════════
// MULTI-PLANTILLA POR EQUIPO — popover modal para seleccionar
// varias plantillas extra para un equipo dado.
// ════════════════════════════════════════════════════════════
function abrirMultiPlantilla(eqId, eqNombre){
  const plantillas = _LEV_PLANTILLAS.all || [];
  if (!plantillas.length){
    ilusAlert({
      title: 'Sin plantillas',
      message: 'No hay plantillas activas en el sistema.',
      sub: 'Pide a un administrador que cree plantillas en /mantenciones/plantillas.',
      type: 'warning',
    });
    return;
  }

  // Crear modal dinámicamente
  let modal = document.getElementById('modalMultiPlantilla');
  if (modal){
    // FIX 2026-05-17: dispose() de Bootstrap ANTES de remover el nodo.
    // Sin esto, Bootstrap retiene su _backdrop interno apuntando a un
    // nodo desconectado → siguiente .hide() deja backdrop huérfano.
    try { bootstrap.Modal.getInstance(modal)?.dispose(); } catch(e){}
    modal.remove();
  }
  modal = document.createElement('div');
  modal.id = 'modalMultiPlantilla';
  modal.className = 'modal fade';
  modal.tabIndex = -1;
  const seleccionadas = _LEV_EQ_PLANTILLAS[eqId] || new Set();
  const tipoActual = document.getElementById('otTipo')?.value || '';

  modal.innerHTML = `
    <div class="modal-dialog modal-dialog-centered modal-dialog-scrollable">
      <div class="modal-content" style="border-radius:12px">
        <div class="modal-header" style="background:linear-gradient(135deg,#1e3a8a,#3b82f6);color:#fff">
          <div>
            <h6 class="modal-title fw-bold mb-0"><i class="bi bi-list-check me-2"></i>Plantillas extra</h6>
            <small style="opacity:.85">${escHtml(eqNombre)}</small>
          </div>
          <button type="button" class="btn-close btn-close-white" data-bs-dismiss="modal"></button>
        </div>
        <div class="modal-body">
          <div class="alert alert-info py-2 small mb-2">
            <i class="bi bi-info-circle me-1"></i>
            La plantilla del tipo de OT (${tipoActual || '—'}) ya se aplica automáticamente.
            Aquí puedes agregar plantillas <strong>adicionales</strong> para este equipo.
          </div>
          <div id="multiPlantillaList">
            ${plantillas.map(p => `
              <label class="d-flex align-items-start gap-2 p-2 mb-1 border rounded" style="cursor:pointer;background:${seleccionadas.has(p.id)?'#eff6ff':'#fff'}">
                <input type="checkbox" class="mp-chk" data-pid="${p.id}" ${seleccionadas.has(p.id)?'checked':''} style="margin-top:3px">
                <div class="flex-grow-1">
                  <div class="fw-bold small">${escHtml(p.nombre)}</div>
                  <div class="text-muted" style="font-size:.7rem">
                    ${p.tipo_visita ? `<span class="badge bg-secondary me-1">${escHtml(p.tipo_visita)}</span>` : ''}
                    ${p.items_count || 0} tarea(s)
                    ${p.descripcion ? ' · ' + escHtml(p.descripcion.substring(0, 80)) : ''}
                  </div>
                </div>
              </label>
            `).join('')}
          </div>
        </div>
        <div class="modal-footer">
          <button type="button" class="btn btn-light" data-bs-dismiss="modal">Cancelar</button>
          <button type="button" class="btn btn-primary" onclick="guardarMultiPlantilla(${eqId})">
            <i class="bi bi-check-lg me-1"></i>Guardar selección
          </button>
        </div>
      </div>
    </div>`;
  document.body.appendChild(modal);
  new bootstrap.Modal(modal).show();
}

function guardarMultiPlantilla(eqId){
  const modal = document.getElementById('modalMultiPlantilla');
  if (!modal) return;
  const ids = Array.from(modal.querySelectorAll('.mp-chk:checked')).map(c => parseInt(c.dataset.pid));
  if (ids.length){
    _LEV_EQ_PLANTILLAS[eqId] = new Set(ids);
  } else {
    delete _LEV_EQ_PLANTILLAS[eqId];
  }
  // Actualizar el badge del botón
  const counter = document.getElementById('lev-pl-count-' + eqId);
  if (counter){
    counter.textContent = ids.length ? `${ids.length} plantilla${ids.length>1?'s':''} extra` : '0 plantillas';
  }
  bootstrap.Modal.getInstance(modal)?.hide();
  ilusToast(`✓ ${ids.length} plantilla(s) asignada(s) al equipo`, { type: 'success', duration: 2000 });
}

// ════════════════════════════════════════════════════════════
// Acceso y logística — toggles Sí/No segmented
// ════════════════════════════════════════════════════════════
function setAccesoYN(btn){
  const target = btn.dataset.target;
  const val = btn.dataset.val;
  const hidden = document.getElementById(target);
  if (!hidden) return;
  // ¿Toggle off? click sobre el activo lo limpia (volver a "no informado")
  if (hidden.value === val){
    hidden.value = '';
    document.querySelectorAll(`.lev-yn-btn[data-target="${target}"]`).forEach(b => b.classList.remove('active'));
    return;
  }
  hidden.value = val;
  document.querySelectorAll(`.lev-yn-btn[data-target="${target}"]`).forEach(b => {
    b.classList.toggle('active', b.dataset.val === val);
  });
}

function resetAccesoLogistica(){
  ['acceso_ascensor','acceso_estacionamiento'].forEach(id => {
    const h = document.getElementById(id);
    if (h) h.value = '';
    document.querySelectorAll(`.lev-yn-btn[data-target="${id}"]`).forEach(b => b.classList.remove('active'));
  });
  const piso = document.getElementById('acceso_piso');
  const notas = document.getElementById('acceso_notas');
  if (piso) piso.value = '';
  if (notas) notas.value = '';
}

// ════════════════════════════════════════════════════════════
// Adjuntos preliminares — selección antes de crear la OT
// ════════════════════════════════════════════════════════════
function _bytesPretty(n){
  if (!n && n !== 0) return '';
  if (n < 1024) return n + ' B';
  if (n < 1024*1024) return (n/1024).toFixed(0) + ' KB';
  return (n/(1024*1024)).toFixed(1) + ' MB';
}
function _adjIconClass(file){
  const m = (file.type || '').toLowerCase();
  const n = (file.name || '').toLowerCase();
  if (m.startsWith('image/')) return 'bi-camera text-primary';
  if (m === 'application/pdf' || n.endsWith('.pdf')) return 'bi-file-earmark-pdf text-danger';
  if (m.startsWith('video/')) return 'bi-film text-info';
  if (m.startsWith('audio/')) return 'bi-mic text-purple';
  if (/\.(docx?|xlsx?|pptx?|txt|csv)$/i.test(n)) return 'bi-file-earmark-text text-secondary';
  return 'bi-file-earmark text-muted';
}
// Límites alineados al endpoint /api/visitas/<vid>/adjuntos
const _LEV_ADJ_MAX = {
  foto:      15 * 1024 * 1024,
  pdf:       30 * 1024 * 1024,
  video:    100 * 1024 * 1024,
  audio:     30 * 1024 * 1024,
  documento: 25 * 1024 * 1024,
  otro:      25 * 1024 * 1024,
};
function _adjTipo(file){
  const m = (file.type || '').toLowerCase();
  const n = (file.name || '').toLowerCase();
  if (m.startsWith('image/')) return 'foto';
  if (m === 'application/pdf' || n.endsWith('.pdf')) return 'pdf';
  if (m.startsWith('video/')) return 'video';
  if (m.startsWith('audio/')) return 'audio';
  if (/\.(docx?|xlsx?|pptx?|txt|csv)$/i.test(n)) return 'documento';
  return 'otro';
}

function onLevAdjFiles(fileList){
  if (!fileList || !fileList.length) return;
  const rechazados = [];
  Array.from(fileList).forEach(f => {
    const tipo = _adjTipo(f);
    const max = _LEV_ADJ_MAX[tipo] || _LEV_ADJ_MAX.otro;
    if (f.size > max){
      rechazados.push(`${f.name} (${_bytesPretty(f.size)}, máx ${_bytesPretty(max)})`);
      return;
    }
    _LEV.adjuntos_preliminares.push(f);
  });
  // Limpia el input para permitir re-seleccionar el mismo archivo si se elimina
  const inp = document.getElementById('levAdjInput');
  if (inp) inp.value = '';
  _renderLevAdjList();
  if (rechazados.length){
    ilusToast(`Archivo(s) muy grande(s): ${rechazados[0]}${rechazados.length>1 ? ' y ' + (rechazados.length-1) + ' más' : ''}`, { type:'warning' });
  }
}

function _renderLevAdjList(){
  const wrap = document.getElementById('levAdjList');
  const counter = document.getElementById('levAdjCount');
  if (!wrap) return;
  const arr = _LEV.adjuntos_preliminares || [];
  if (counter) counter.textContent = arr.length;
  if (!arr.length){
    wrap.innerHTML = '';
    return;
  }
  wrap.innerHTML = arr.map((f, idx) => `
    <div class="lev-adj-item">
      <div class="lev-adj-thumb"><i class="bi ${_adjIconClass(f)}"></i></div>
      <div class="lev-adj-info">
        <div class="lev-adj-name" title="${escAttr(f.name)}">${escHtml(f.name)}</div>
        <div class="lev-adj-meta">${_adjTipo(f).toUpperCase()} · ${_bytesPretty(f.size)}</div>
      </div>
      <button type="button" class="lev-adj-rm" onclick="removeLevAdj(${idx})" title="Quitar">
        <i class="bi bi-x-lg"></i>
      </button>
    </div>
  `).join('');
}

function removeLevAdj(idx){
  if (!_LEV.adjuntos_preliminares) return;
  _LEV.adjuntos_preliminares.splice(idx, 1);
  _renderLevAdjList();
}

function resetLevAdjuntos(){
  _LEV.adjuntos_preliminares = [];
  _renderLevAdjList();
}

// Sube los adjuntos preliminares acumulados al endpoint existente
// /mantenciones/api/visitas/<vid>/adjuntos (uno por uno, secuencial).
// No bloquea la UI principal — se llama tras crear la OT.
async function _subirAdjuntosPreliminares(vid){
  const arr = _LEV.adjuntos_preliminares || [];
  if (!arr.length || !vid) return { ok: 0, fail: 0 };
  let ok = 0, fail = 0;
  for (let i = 0; i < arr.length; i++){
    const f = arr[i];
    try {
      ilusToast(`Subiendo ${i+1}/${arr.length}: ${f.name}`, { type:'info', duration: 1500 });
      const fd = new FormData();
      fd.append('archivo', f);
      fd.append('tipo', _adjTipo(f));
      const r = await fetch(`/mantenciones/api/visitas/${vid}/adjuntos`, {
        method: 'POST',
        body: fd
      });
      const d = await r.json().catch(() => ({}));
      if (r.ok && d.ok) ok++;
      else { fail++; console.warn('Adjunto falló:', f.name, d); }
    } catch (e){
      fail++;
      console.warn('Adjunto error de red:', f.name, e);
    }
  }
  return { ok, fail };
}

async function levIniciar(){
  // Aunque la función se llama levIniciar (legacy), su acción ahora es
  // CREAR la OT de levantamiento. No abre captura en este modal.
  const ids = Array.from(document.querySelectorAll('.lev-eq-chk:checked')).map(c => parseInt(c.dataset.id));
  if (!ids.length){
    ilusToast('Selecciona al menos un equipo', { type:'warning' });
    return;
  }

  // Validaciones programación
  const fechaProg = document.getElementById('levFechaProg').value;
  if (!fechaProg){
    ilusToast('Indica la fecha programada', { type:'warning' });
    return;
  }
  const horaIni = document.getElementById('levHoraIni').value || '';
  const horaFin = document.getElementById('levHoraFin').value || '';
  if (horaIni && horaFin && horaIni >= horaFin){
    ilusToast('La hora de término debe ser posterior a la de inicio', { type:'warning' });
    return;
  }
  const usaRango = document.getElementById('levRangoDias').checked;
  let fechaFin = '';
  if (usaRango){
    fechaFin = document.getElementById('levFechaFin').value;
    if (!fechaFin){
      ilusToast('Indica la fecha de término', { type:'warning' });
      return;
    }
    if (fechaFin < fechaProg){
      ilusToast('La fecha de término no puede ser anterior a la de inicio', { type:'warning' });
      return;
    }
  }

  // Dirección + contacto OBLIGATORIOS — calidad de información para el técnico
  const dirVal = (document.getElementById('levDireccion')?.value || '').trim();
  if (!dirVal){
    ilusToast('Indica la dirección de la visita', { type:'warning' });
    document.getElementById('levDireccion')?.focus();
    return;
  }
  const contactoNombre = (document.getElementById('levContactoNombre')?.value || '').trim();
  if (!contactoNombre){
    ilusToast('Indica el contacto que recibirá al técnico en sitio', { type:'warning' });
    document.getElementById('levContactoSel')?.focus();
    return;
  }

  // Multi-técnico OBLIGATORIO — la OT debe tener al menos 1 técnico asignado
  // porque el flujo es: admin crea OT → técnico la gestiona desde el módulo
  // "Órdenes de Trabajo" en su teléfono.
  const tecnicoIds = Array.from(_LEV_MODAL.tecnicos_seleccionados);
  if (!tecnicoIds.length){
    const hayTecnicos = (_LEV_MODAL.tecnicos_disponibles || []).length > 0;
    if (!hayTecnicos){
      await ilusAlert({
        title: 'Sin técnicos disponibles',
        message: 'No es posible crear la OT de levantamiento porque no hay técnicos activos en el sistema.',
        sub: 'Es necesario crear al menos un usuario con rol "Técnico" antes de continuar.',
        type: 'warning',
      });
    } else {
      ilusToast('Asigna al menos un técnico que ejecute la OT', { type:'warning' });
    }
    return;
  }

  const btn = document.getElementById('btnLevIniciar');
  const btnHTMLOrig = btn.innerHTML;
  btn.disabled = true; btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Creando OT…';
  try {
    // Construir mapa de plantillas extra por equipo (solo equipos seleccionados)
    const plantillasPorEq = {};
    ids.forEach(mid => {
      if (_LEV_EQ_PLANTILLAS[mid] && _LEV_EQ_PLANTILLAS[mid].size > 0){
        plantillasPorEq[mid] = Array.from(_LEV_EQ_PLANTILLAS[mid]);
      }
    });

    const r = await fetch(`/mantenciones/api/clientes/${CID}/levantamientos`, {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({
        titulo: document.getElementById('levSelectTitulo').value.trim(),
        notas:  document.getElementById('levSelectNotas').value.trim(),
        equipo_ids: ids,
        fecha_programada: fechaProg,
        hora_inicio: horaIni || null,
        hora_fin: horaFin || null,
        fecha_fin: usaRango ? fechaFin : null,
        hora_inicio_fin: usaRango ? (document.getElementById('levHoraIniFin')?.value || null) : null,
        hora_fin_fin: usaRango ? (document.getElementById('levHoraFinFin')?.value || null) : null,
        tecnico_ids: tecnicoIds,
        tipo_ot: document.getElementById('otTipo')?.value || 'levantamiento',
        aplica_garantia: document.getElementById('otAplicaGarantia')?.checked || false,
        plantillas_por_equipo: plantillasPorEq,
        // Dirección de la visita (puede ser distinta a la del cliente)
        direccion_visita: (document.getElementById('levDireccion')?.value || '').trim(),
        direccion_lat: parseFloat(document.getElementById('levDireccion')?.dataset.lat) || null,
        direccion_lng: parseFloat(document.getElementById('levDireccion')?.dataset.lng) || null,
        direccion_place_id: document.getElementById('levDireccion')?.dataset.placeId || null,
        // Contacto / contraparte en sitio
        contacto_nombre: (document.getElementById('levContactoNombre')?.value || '').trim(),
        contacto_cargo:  (document.getElementById('levContactoCargo')?.value || '').trim(),
        contacto_tel:    (document.getElementById('levContactoTel')?.value || '').trim(),
        contacto_email:  (document.getElementById('levContactoEmail')?.value || '').trim(),
        contacto_origen: document.getElementById('levContactoSel')?.dataset.origen || 'manual',
        // Acceso y logística del sitio (info crítica para el técnico)
        acceso_ascensor:        document.getElementById('acceso_ascensor')?.value || null,
        acceso_estacionamiento: document.getElementById('acceso_estacionamiento')?.value || null,
        acceso_piso:            (document.getElementById('acceso_piso')?.value || '').trim(),
        acceso_notas:           (document.getElementById('acceso_notas')?.value || '').trim(),
      })
    });
    const d = await r.json();
    if (!d.ok){
      // Ya no manejamos YA_HAY_ABIERTO con captura — solo informamos
      ilusToast(d.error || 'Error', { type:'error' });
      return;
    }
    _LEV.id = d.id;
    const visitaId = d.visita_id;
    // ── FIX 2026-05-17: backdrop huérfano ────────────────────────
    // Esperar a que Bootstrap termine SU cleanup del backdrop
    // ANTES de abrir el ilusAlert. Si abrimos el overlay encima
    // inmediatamente, roba el foco y Bootstrap aborta su transición,
    // dejando un .modal-backdrop colgado y body.modal-open activo.
    // ─────────────────────────────────────────────────────────────
    const _mLevEl = document.getElementById('modalLevSelector');
    const _mLevInst = bootstrap.Modal.getInstance(_mLevEl);
    if (_mLevInst){
      await new Promise(resolve => {
        let resolved = false;
        const done = () => { if (!resolved){ resolved = true; resolve(); } };
        _mLevEl.addEventListener('hidden.bs.modal', done, { once: true });
        _mLevInst.hide();
        // Failsafe por si el listener no dispara (caso edge)
        setTimeout(done, 600);
      });
    }
    // Cleanup defensivo extra (definido en ilus_ui.js)
    if (typeof ilusCleanModalBackdrops === 'function') ilusCleanModalBackdrops();

    // ──────────────────────────────────────────────────────────────
    // Subida de adjuntos preliminares (post-creación de OT).
    // Solo si hay archivos seleccionados Y la OT espejo (visita) se
    // creó correctamente. Se hace SECUENCIAL para mostrar progreso.
    // ──────────────────────────────────────────────────────────────
    let adjResult = null;
    const nAdj = (_LEV.adjuntos_preliminares || []).length;
    if (nAdj > 0 && visitaId){
      adjResult = await _subirAdjuntosPreliminares(visitaId);
    } else if (nAdj > 0 && !visitaId){
      // No hay OT espejo — los adjuntos no tienen dónde ir
      console.warn('[lev] Hay adjuntos preliminares pero no se creó la OT espejo (visita_id ausente).');
    }

    // ──────────────────────────────────────────────────────────────
    // FLUJO (2026-05-16): el admin SOLO genera la OT.
    // La gestión (captura de fotos, completar tareas) la hace el
    // técnico desde el módulo "Órdenes de Trabajo" en su teléfono.
    // Aquí solo informamos qué se creó y damos link opcional a la OT.
    // ──────────────────────────────────────────────────────────────
    const otHtml = d.ot_url
      ? `<a href="${d.ot_url}" class="fw-bold text-decoration-underline" style="color:#dc2626">${d.numero_ot}</a>`
      : '';
    let subMsg = `La OT está disponible para que el/los técnico(s) la gestionen desde su módulo de Órdenes de Trabajo. ` +
                 `${d.items_plantilla_aplicados||0} tarea(s) generadas por las plantillas aplicadas.`;
    if (adjResult){
      if (adjResult.fail === 0){
        subMsg += ` ${adjResult.ok} archivo(s) preliminar(es) adjunto(s).`;
      } else {
        subMsg += ` ${adjResult.ok}/${adjResult.ok + adjResult.fail} archivo(s) preliminares subidos (${adjResult.fail} falló/fallaron).`;
      }
    }
    await ilusAlert({
      title: '✅ Orden de Trabajo creada',
      message: `Se generó la OT ${otHtml} con ${d.n_items} equipo(s) y ${d.tecnicos_asignados||0} técnico(s) asignado(s).`,
      sub: subMsg,
      messageHtml: true,
      type: 'success',
      okLabel: 'Entendido',
    });
    // Reset de estado para que la próxima OT empiece limpia
    resetAccesoLogistica();
    resetLevAdjuntos();
    // El admin permanece en la ficha del cliente. La OT ya está en el sistema.
  } catch(e){
    ilusToast('Error de red: ' + e.message, { type:'error' });
  } finally {
    btn.disabled = false; btn.innerHTML = btnHTMLOrig;
  }
}

function escHtml(s){
  return String(s||'').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
}
function escAttr(s){
  return String(s||'').replace(/"/g,'&quot;').replace(/'/g,'&#39;');
}

function abrirNuevaVisita(tipoPreset) {
  // Daniel 2026-06-06: "Programar mantención" y "Nueva visita" REUSAN el modal
  // POTENTE de OT (7 pasos: tipo, dirección con Google [trae la del cliente,
  // editable], contacto, fecha/hora, técnicos, equipos+plantillas, acceso,
  // archivos). Así se calendariza Y se genera la OT en un mismo flujo.
  // El modal simple #modalVisita queda solo para EDITAR visitas existentes.
  abrirGenerarOT(tipoPreset || 'preventiva');
}
// Marca el toggle de garantía de la visita y refresca la nota.
// `aplica` true → "Aplica (cubierto)"; false → "No aplica (pago)".
function _viSetGarantia(aplica) {
  const si = document.getElementById('vi_gar_si');
  const no = document.getElementById('vi_gar_no');
  if (si) si.checked = !!aplica;
  if (no) no.checked = !aplica;
  if (typeof viGarToggleNota === 'function') viGarToggleNota();
}
function editarVisita(v) {
  document.getElementById('vi_id').value = v.id;
  document.getElementById('modalVisitaTitulo').innerHTML = '<i class="bi bi-calendar-check me-2"></i>Editar visita';
  document.getElementById('vi_titulo').value = v.titulo || '';
  document.getElementById('vi_tipo').value = v.tipo || 'preventiva';
  document.getElementById('vi_estado').value = v.estado || 'programada';
  // fecha_programada puede venir como 'YYYY-MM-DD' o como dict {__class__:'date',...} desde tojson
  let f = v.fecha_programada;
  if (f && typeof f === 'string') f = f.slice(0,10);
  document.getElementById('vi_fecha').value = f || '';
  document.getElementById('vi_hora_inicio').value = v.hora_inicio || '';
  document.getElementById('vi_hora_fin').value = v.hora_fin || '';
  document.getElementById('vi_tecnico').value = v.tecnico || '';
  document.getElementById('vi_costo').value = v.costo || '';
  document.getElementById('vi_descripcion').value = v.descripcion || '';
  // Garantía: "Aplica" si la visita está cubierta por garantía.
  _viSetGarantia(v.cubierto_por === 'garantia' || v.modalidad_cobro === 'garantia');
  document.getElementById('btnEliminarVisita').style.display = '';
  new bootstrap.Modal(document.getElementById('modalVisita')).show();
}

async function eliminarVisitaFromTabla(vid, titulo) {
  const ok = await ilusConfirm({
    title: 'Eliminar visita',
    message: `¿Eliminar la visita "${titulo}"?`,
    sub: 'Esta acción no se puede deshacer.',
    okLabel: 'Eliminar', cancelLabel: 'Cancelar',
    danger: true,
  });
  if (!ok) return;
  try {
    const r = await fetch(`/mantenciones/api/visitas/${vid}`, { method:'DELETE' });
    const d = await r.json().catch(() => ({}));
    if (!r.ok || !d.ok) {
      ilusToast('Error: ' + (d.error || 'no se pudo eliminar'), { type:'error' });
      return;
    }
    ilusToast('Visita eliminada', { type:'success' });
    setTimeout(() => location.reload(), 600);
  } catch(e){ ilusToast('Error de red: ' + e.message, { type:'error' }); }
}

// ─── Guardar cliente (PUT) ────────────────────────────────
// ════════════════════════════════════════════════════════════════════
// ACCIONES POR EQUIPO — modal genérico que adapta tipo (garantía,
// correctiva, preventiva, inspección). Cada acción genera UNA visita.
// ════════════════════════════════════════════════════════════════════
let _modalAccionEq = null;

const TIPO_ACCION_CFG = {
  garantia: {
    title:  'Cambio / Garantía',
    icon:   'bi-shield-check',
    headerBg: 'linear-gradient(135deg,#16a34a 0%,#15803d 100%)',
    btnClass: 'btn-success',
    estado_default: 'critico',
    estado_label: 'Crítico',
    placeholder: 'Ej: Banda rota tras 2 meses. Cliente reporta ruido. Cambio bajo garantía ERP.',
    hint_cantidad: '¿Cuántas unidades de este equipo están dañadas?',
    fecha_dias: 3,
  },
  correctiva: {
    title:  'Reparación correctiva',
    icon:   'bi-tools',
    headerBg: 'linear-gradient(135deg,#d97706 0%,#b45309 100%)',
    btnClass: 'btn-warning',
    estado_default: 'en_mantencion',
    estado_label: 'En mantención',
    placeholder: 'Ej: Pantalla con falla intermitente. Garantía vencida. Cliente acepta reparación pagada.',
    hint_cantidad: '¿Cuántas unidades necesitan reparación?',
    fecha_dias: 5,
  },
  preventiva: {
    title:  'Mantención preventiva',
    icon:   'bi-calendar-check',
    headerBg: 'linear-gradient(135deg,#2563eb 0%,#1e40af 100%)',
    btnClass: 'btn-primary',
    estado_default: 'operativo',
    estado_label: 'Operativo (sin cambios)',
    placeholder: 'Ej: Mantención programada según contrato. Lubricación, calibración, limpieza profunda.',
    hint_cantidad: 'Unidades a revisar',
    fecha_dias: 14,
  },
  inspeccion: {
    title:  'Inspección / Levantamiento',
    icon:   'bi-binoculars',
    headerBg: 'linear-gradient(135deg,#0891b2 0%,#0e7490 100%)',
    btnClass: 'btn-info',
    estado_default: 'operativo',
    estado_label: 'Operativo (sin cambios)',
    placeholder: 'Ej: Visita técnica para evaluar el estado actual del equipo y levantar requerimientos.',
    hint_cantidad: 'Unidades a inspeccionar',
    fecha_dias: 7,
  },
};

function abrirAccionEquipo(tipo, eq) {
  const cfg = TIPO_ACCION_CFG[tipo] || TIPO_ACCION_CFG.garantia;
  if (!_modalAccionEq) _modalAccionEq = new bootstrap.Modal(document.getElementById('modalAccionEquipo'));

  // Header dinámico
  document.getElementById('ae_header').style.background = cfg.headerBg;
  document.getElementById('ae_icon').className = 'bi ' + cfg.icon;
  document.getElementById('ae_title').textContent = cfg.title;
  const btn = document.getElementById('ae_btn_confirmar');
  btn.className = `btn ${cfg.btnClass} fw-bold px-4`;

  // Datos del equipo
  document.getElementById('ae_mid').value     = eq.id;
  document.getElementById('ae_tipo').value    = tipo;
  document.getElementById('ae_doc_fecha').value = eq.doc_fecha || '';
  document.getElementById('ae_nombre').textContent = eq.nombre || '—';
  document.getElementById('ae_sku').textContent    = eq.sku || '—';
  document.getElementById('ae_serie').textContent  = eq.serie || '—';
  document.getElementById('ae_total').textContent  = eq.cantidad || 1;
  document.getElementById('ae_cantidad').max       = eq.cantidad || 1;
  document.getElementById('ae_cantidad').value     = 1;
  document.getElementById('ae_cantidad_hint').textContent = cfg.hint_cantidad;

  // Default fecha según tipo
  const f = new Date(); f.setDate(f.getDate() + cfg.fecha_dias);
  document.getElementById('ae_fecha').value = f.toISOString().slice(0,10);

  // Estado default según tipo
  document.getElementById('ae_estado').value = cfg.estado_default;

  // Limpiar
  document.getElementById('ae_motivo').value = '';
  document.getElementById('ae_motivo').placeholder = cfg.placeholder;
  document.getElementById('ae_tecnico').value = '';

  // Hint de garantía si tipo es garantia o correctiva
  if (eq.doc_fecha && (tipo === 'garantia' || tipo === 'correctiva')) {
    const docFecha = new Date(eq.doc_fecha);
    const garantiaFin = new Date(docFecha); garantiaFin.setMonth(garantiaFin.getMonth() + 6);
    const enGarantia = new Date() <= garantiaFin;
    if (tipo === 'garantia' && !enGarantia) {
      document.getElementById('ae_motivo').placeholder =
        `⚠️ Garantía vencida ${garantiaFin.toLocaleDateString('es-CL')}. Considera usar "Correctiva" en su lugar.\n\n` + cfg.placeholder;
    }
  }

  actualizarResumenAccion();
  document.getElementById('ae_estado').onchange = actualizarResumenAccion;
  _modalAccionEq.show();
}

function actualizarResumenAccion() {
  const tipo = document.getElementById('ae_tipo').value;
  const cfg = TIPO_ACCION_CFG[tipo] || {};
  const estados = {critico:'🔴 Crítico', en_mantencion:'🟡 En mantención', operativo:'🟢 Operativo'};
  document.getElementById('ae_resumen_tipo').textContent = cfg.title || tipo;
  document.getElementById('ae_resumen_estado').textContent =
    estados[document.getElementById('ae_estado').value] || '—';
}

async function confirmarAccionEquipo() {
  const mid     = document.getElementById('ae_mid').value;
  const tipo    = document.getElementById('ae_tipo').value;
  const motivo  = document.getElementById('ae_motivo').value.trim();
  const cant    = parseInt(document.getElementById('ae_cantidad').value) || 1;
  const estado  = document.getElementById('ae_estado').value;
  const fecha   = document.getElementById('ae_fecha').value;
  const tecnico = document.getElementById('ae_tecnico').value.trim();

  if (motivo.length < 8) { alert('El motivo debe tener al menos 8 caracteres'); return; }
  if (!fecha) { alert('La fecha de visita es obligatoria'); return; }

  try {
    const r = await fetch(`/mantenciones/api/maquinas/${mid}/solicitar-cambio`, {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({
        cantidad_afectada: cant,
        motivo,
        tipo_visita: tipo,
        fecha_programada: fecha,
        estado_nuevo: estado,
        tecnico,
      })
    });
    const d = await r.json();
    if (!d.ok) { alert('Error: ' + (d.error||'')); return; }
    _modalAccionEq.hide();
    setTimeout(() => location.reload(), 200);
  } catch(e) {
    alert('Error de red: ' + e.message);
  }
}

// Compatibilidad con código antiguo (por si queda referencia)
function abrirSolicitudCambio(eq) { abrirAccionEquipo('garantia', eq); }


// ════════════════════════════════════════════════════════════════════
// SUCURSALES — CRUD del cliente
// ════════════════════════════════════════════════════════════════════
let _modalSucursal = null;

function abrirSucursal(s) {
  if (!_modalSucursal) _modalSucursal = new bootstrap.Modal(document.getElementById('modalSucursal'));
  // Reset
  ['suc_id','suc_nombre','suc_direccion','suc_comuna','suc_ciudad','suc_region',
   'suc_enc_nombre','suc_enc_cargo','suc_enc_tel','suc_enc_email',
   'suc_c2_nombre','suc_c2_cargo','suc_c2_tel','suc_c2_email','suc_notas']
    .forEach(id => { const el = document.getElementById(id); if (el) el.value = ''; });

  // Reset checkbox principal
  document.getElementById('suc_es_principal').checked = false;

  if (s) {
    document.getElementById('suc_modal_title').textContent = 'Editar sucursal';
    document.getElementById('suc_btn_label').textContent  = 'Guardar cambios';
    document.getElementById('suc_id').value           = s.id || '';
    document.getElementById('suc_nombre').value       = s.nombre || '';
    document.getElementById('suc_direccion').value    = s.direccion || '';
    document.getElementById('suc_comuna').value       = s.comuna || '';
    document.getElementById('suc_ciudad').value       = s.ciudad || '';
    document.getElementById('suc_region').value       = s.region || '';
    document.getElementById('suc_enc_nombre').value   = s.encargado_nombre || '';
    document.getElementById('suc_enc_cargo').value    = s.encargado_cargo || '';
    document.getElementById('suc_enc_tel').value      = s.encargado_tel || '';
    document.getElementById('suc_enc_email').value    = s.encargado_email || '';
    document.getElementById('suc_c2_nombre').value    = s.contacto2_nombre || '';
    document.getElementById('suc_c2_cargo').value     = s.contacto2_cargo || '';
    document.getElementById('suc_c2_tel').value       = s.contacto2_tel || '';
    document.getElementById('suc_c2_email').value     = s.contacto2_email || '';
    document.getElementById('suc_notas').value        = s.notas || '';
    document.getElementById('suc_es_principal').checked = !!s.es_principal;
  } else {
    document.getElementById('suc_modal_title').textContent = 'Nueva sucursal';
    document.getElementById('suc_btn_label').textContent  = 'Guardar sucursal';
  }
  _modalSucursal.show();
}

// ─── Buscador potente de dirección dentro del modal de sucursal ───
let _sucDirTimer = null;
let _sucDirResults = [];
let _sucDirIdx = -1;

function sucDirDebounce() {
  clearTimeout(_sucDirTimer);
  const q = document.getElementById('suc_direccion').value.trim();
  document.getElementById('suc_dir_ok').style.display = 'none';
  if (q.length < 4) {
    document.getElementById('suc_dir_dropdown').style.display = 'none';
    return;
  }
  document.getElementById('suc_dir_validating').style.display = '';
  _sucDirTimer = setTimeout(sucDirBuscar, 500);
}

async function sucDirBuscar() {
  const q = document.getElementById('suc_direccion').value.trim();
  const drop = document.getElementById('suc_dir_dropdown');
  if (q.length < 4) return;
  drop.innerHTML = '<div class="ac-loading"><span class="spinner-border spinner-border-sm me-1"></span>Buscando dirección…</div>';
  drop.style.display = 'block';

  // Mismo cascada de motores que usa el wizard (Nominatim → Photon)
  const variantes = [
    q,
    q.replace(/^(av\.?|avda\.?|avenida)\s+/i, ''),
    q.replace(/\s+\d+[A-Za-z]?\s*[,;]?\s*/g, ' ').trim()
  ].filter((v,i,a) => v && a.indexOf(v) === i);

  async function nominatim(query) {
    const url = `https://nominatim.openstreetmap.org/search?format=json&q=${encodeURIComponent(query+', Chile')}&countrycodes=cl&addressdetails=1&limit=8&accept-language=es&dedupe=1`;
    try {
      const r = await fetch(url, { headers: { 'Accept-Language': 'es' } });
      if (!r.ok) return [];
      const j = await r.json();
      return (j||[]).map(d => {
        const a = d.address || {};
        return {
          calle:  [a.road, a.house_number].filter(Boolean).join(' ') || d.display_name.split(',')[0],
          comuna: a.suburb || a.city_district || a.municipality || a.county || a.town || '',
          ciudad: a.city || a.town || a.state || '',
          region: a.state || '',
          display: d.display_name,
          fuente: 'OSM',
        };
      });
    } catch { return []; }
  }
  async function photon(query) {
    const url = `https://photon.komoot.io/api?q=${encodeURIComponent(query+' Chile')}&limit=10&bbox=-75.7,-56,-66.4,-17.5`;
    try {
      const r = await fetch(url);
      if (!r.ok) return [];
      const j = await r.json();
      return (j.features || []).filter(f => {
        const c = (f.properties.country || '').toLowerCase();
        return c === 'chile' || c.includes('chil');
      }).map(f => {
        const p = f.properties || {};
        return {
          calle:  [p.name, p.housenumber].filter(Boolean).join(' ') || p.street || p.name || '',
          comuna: p.district || p.locality || p.suburb || '',
          ciudad: p.city || p.county || p.state || '',
          region: p.state || '',
          display: [p.name, p.locality, p.city, p.country].filter(Boolean).join(', '),
          fuente: 'Photon',
        };
      });
    } catch { return []; }
  }

  let resultados = [];
  for (const v of variantes) {
    resultados = await nominatim(v);
    if (resultados.length) break;
  }
  if (!resultados.length) {
    for (const v of variantes) {
      resultados = await photon(v);
      if (resultados.length) break;
    }
  }
  _sucDirResults = resultados.filter(r => r.calle);
  _sucDirIdx = -1;
  document.getElementById('suc_dir_validating').style.display = 'none';

  if (!_sucDirResults.length) {
    drop.innerHTML = `<div class="ac-loading text-muted">
      <div><i class="bi bi-geo me-1"></i>Sin resultados exactos.</div>
      <div class="small mt-1">Puedes ingresarla manualmente — completa también comuna y ciudad.</div>
    </div>`;
    return;
  }

  drop.innerHTML = _sucDirResults.map((d,i) => {
    const tag = d.fuente === 'Photon'
      ? '<span class="badge bg-info-subtle text-info border ms-1" style="font-size:.55rem;font-weight:600">Photon</span>'
      : '';
    return `<div class="ac-item" onclick="sucDirSeleccionar(${i})">
      <div class="d-flex align-items-start gap-2">
        <i class="bi bi-geo-alt-fill text-danger mt-1" style="font-size:.8rem;flex-shrink:0"></i>
        <div style="flex:1;min-width:0">
          <div class="ac-name" style="font-size:.83rem">${d.calle}${tag}</div>
          <div class="ac-rut">${[d.comuna, d.ciudad].filter(Boolean).join(', ') || (d.display||'').slice(0,80)}</div>
        </div>
      </div>
    </div>`;
  }).join('');
}

function sucDirSeleccionar(i) {
  const d = _sucDirResults[i];
  if (!d) return;
  document.getElementById('suc_direccion').value = d.calle || '';
  if (d.comuna) document.getElementById('suc_comuna').value = d.comuna;
  if (d.ciudad) document.getElementById('suc_ciudad').value = d.ciudad;
  if (d.region) document.getElementById('suc_region').value = d.region;
  document.getElementById('suc_dir_dropdown').style.display = 'none';
  document.getElementById('suc_dir_ok').style.display = '';
}

function sucDirKeydown(e) {
  const drop = document.getElementById('suc_dir_dropdown');
  const items = drop.querySelectorAll('.ac-item');
  if (!items.length) return;
  if (e.key === 'ArrowDown') { _sucDirIdx = Math.min(_sucDirIdx+1, items.length-1); items.forEach((el,i)=>el.style.background = i===_sucDirIdx?'#f0f4ff':''); e.preventDefault(); }
  else if (e.key === 'ArrowUp') { _sucDirIdx = Math.max(_sucDirIdx-1, -1); items.forEach((el,i)=>el.style.background = i===_sucDirIdx?'#f0f4ff':''); e.preventDefault(); }
  else if (e.key === 'Enter' && _sucDirIdx >= 0) { sucDirSeleccionar(_sucDirIdx); e.preventDefault(); }
  else if (e.key === 'Escape') drop.style.display = 'none';
}
// Cerrar dropdown al click fuera
document.addEventListener('click', e => {
  if (!document.getElementById('suc_direccion')?.contains(e.target)) {
    const dd = document.getElementById('suc_dir_dropdown');
    if (dd) dd.style.display = 'none';
  }
});

async function guardarSucursal() {
  const $v = id => (document.getElementById(id)?.value || '').trim();
  const data = {
    nombre:           $v('suc_nombre'),
    direccion:        $v('suc_direccion'),
    comuna:           $v('suc_comuna'),
    ciudad:           $v('suc_ciudad'),
    region:           $v('suc_region'),
    encargado_nombre: $v('suc_enc_nombre'),
    encargado_cargo:  $v('suc_enc_cargo'),
    encargado_tel:    $v('suc_enc_tel'),
    encargado_email:  $v('suc_enc_email'),
    contacto2_nombre: $v('suc_c2_nombre'),
    contacto2_cargo:  $v('suc_c2_cargo'),
    contacto2_tel:    $v('suc_c2_tel'),
    contacto2_email:  $v('suc_c2_email'),
    notas:            $v('suc_notas'),
    es_principal:     document.getElementById('suc_es_principal').checked,
  };
  if (!data.nombre) { alert('El nombre de la sucursal es obligatorio'); return; }

  const sid = $v('suc_id');
  const url = sid
    ? `/mantenciones/api/sucursales/${sid}`
    : `/mantenciones/api/clientes/${CID}/sucursales`;
  const method = sid ? 'PUT' : 'POST';

  try {
    const r = await fetch(url, {
      method, headers:{'Content-Type':'application/json'},
      body: JSON.stringify(data)
    });
    const d = await r.json();
    if (d.ok) { location.reload(); }
    else { alert('Error: ' + (d.error || 'No se pudo guardar')); }
  } catch(e) { alert('Error de red'); }
}

async function eliminarSucursal(sid, nombre) {
  const ok = await ilusConfirm({
    title: 'Eliminar sucursal',
    message: `¿Eliminar la sucursal "${nombre}"?`,
    sub: 'Esta acción se puede revertir desde la BD si es necesario.',
    okLabel: 'Eliminar', danger: true,
  });
  if (!ok) return;
  try {
    const r = await fetch(`/mantenciones/api/sucursales/${sid}`, {method:'DELETE'});
    if (r.ok) location.reload();
    else ilusToast('Error al eliminar', { type:'error' });
  } catch(e) { ilusToast('Error de red', { type:'error' }); }
}

async function marcarSucursalPrincipal(sid, nombre) {
  const ok = await ilusConfirm({
    title: 'Cambiar dirección principal',
    message: `¿Marcar "${nombre}" como dirección principal?`,
    sub: 'Esta sucursal predominará sobre la dirección base del cliente. Cualquier otra principal se desmarcará.',
    okLabel: 'Sí, marcar principal',
  });
  if (!ok) return;
  try {
    const r = await fetch(`/mantenciones/api/sucursales/${sid}/marcar-principal`, {method:'POST'});
    if (r.ok) location.reload();
    else ilusToast('Error al marcar como principal', { type:'error' });
  } catch(e) { ilusToast('Error de red', { type:'error' }); }
}

// ════════════════════════════════════════════════════════════════════
// N° SERIE EDITABLE con tracking auditable (cambio sensible)
// ════════════════════════════════════════════════════════════════════
let _modalEditSerie = null;
let _modalAuditEq = null;

function editarSerie(eq) {
  if (!_modalEditSerie) _modalEditSerie = new bootstrap.Modal(document.getElementById('modalEditarSerie'));
  document.getElementById('es_mid').value = eq.id;
  document.getElementById('es_nombre').textContent = eq.nombre || '—';
  document.getElementById('es_sku').textContent = eq.sku || '—';
  document.getElementById('es_serie_actual').value = eq.serie || '';
  document.getElementById('es_serie_nueva').value = eq.serie || '';
  document.getElementById('es_motivo').value = '';
  _modalEditSerie.show();
  setTimeout(() => document.getElementById('es_serie_nueva').focus(), 300);
}

async function confirmarCambioSerie() {
  const mid    = document.getElementById('es_mid').value;
  const serie  = document.getElementById('es_serie_nueva').value.trim();
  const motivo = document.getElementById('es_motivo').value.trim();
  if (motivo.length < 5) { alert('El motivo debe tener al menos 5 caracteres'); return; }

  try {
    const r = await fetch(`/mantenciones/api/maquinas/${mid}/serie`, {
      method:'PUT', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({serie, motivo})
    });
    const d = await r.json();
    if (!d.ok) { alert('Error: ' + (d.error||'')); return; }
    if (d.sin_cambios) {
      alert('No hubo cambios — el N° serie es el mismo.');
      _modalEditSerie.hide();
      return;
    }
    // Actualizar visual en la fila sin recargar
    const span = document.getElementById('serie-' + mid);
    if (span) span.textContent = d.serie;
    _modalEditSerie.hide();
    // Toast inline
    setTimeout(() => {
      alert(`✓ N° serie actualizado.\n\nAntes: ${d.serie_anterior || '(vacío)'}\nAhora: ${d.serie}\n\nQuedó registrado en el historial del equipo.`);
    }, 200);
  } catch(e) {
    alert('Error de red: ' + e.message);
  }
}

// ════════════════════════════════════════════════════════════════════
// HISTORIAL DE OTs SOBRE UN EQUIPO
// Muestra todas las OTs donde se trabajó este equipo (vía mant_visita_tareas.maquina_id),
// con fecha, hora, técnico, estado y usuario que cerró cada OT. Read-only.
// ════════════════════════════════════════════════════════════════════
let _modalHistorialOT = null;
async function verHistorialOTEquipo(mid, nombre) {
  // Lazy-init: crear modal si no existe
  let m = document.getElementById('modalHistorialOTEquipo');
  if (!m) {
    document.body.insertAdjacentHTML('beforeend', `
      <div class="modal fade" id="modalHistorialOTEquipo" tabindex="-1">
        <div class="modal-dialog modal-dialog-centered modal-xl modal-dialog-scrollable">
          <div class="modal-content">
            <div class="modal-header" style="background:#0a0a0a;color:#fff">
              <h5 class="modal-title fw-bold"><i class="bi bi-clock-history me-2"></i>Historial de OTs — <span id="histot_eq_nombre">—</span></h5>
              <button type="button" class="btn-close btn-close-white" data-bs-dismiss="modal"></button>
            </div>
            <div class="modal-body" id="histot_content">
              <div class="text-center text-muted py-5"><span class="spinner-border spinner-border-sm me-2"></span>Cargando…</div>
            </div>
          </div>
        </div>
      </div>`);
    m = document.getElementById('modalHistorialOTEquipo');
  }
  document.getElementById('histot_eq_nombre').textContent = nombre || `Equipo #${mid}`;
  if (!_modalHistorialOT) _modalHistorialOT = new bootstrap.Modal(m);
  _modalHistorialOT.show();
  try {
    const r = await fetch(`/mantenciones/api/maquinas/${mid}/historial-ots`);
    const d = await r.json();
    if (!d.ok || !d.ots || !d.ots.length) {
      document.getElementById('histot_content').innerHTML = `
        <div class="text-center text-muted py-5">
          <i class="bi bi-clipboard2-x" style="font-size:2.5rem;opacity:.25"></i>
          <div class="fw-semibold mt-2">Sin historial</div>
          <div class="small mt-1">Este equipo aún no aparece en ninguna OT con tarea asociada.</div>
        </div>`;
      return;
    }
    const estadoBadge = {
      'programada':  '<span class="badge" style="background:#dbeafe;color:#1e40af">Pendiente</span>',
      'en_curso':    '<span class="badge" style="background:#fef3c7;color:#92400e">En curso</span>',
      'completada':  '<span class="badge" style="background:#dcfce7;color:#166534">Completada</span>',
      'cancelada':   '<span class="badge" style="background:#fee2e2;color:#991b1b">Cancelada</span>',
      'reagendada':  '<span class="badge bg-secondary">Reagendada</span>',
    };
    let html = `
      <div class="alert alert-info py-2 small mb-3">
        <i class="bi bi-info-circle me-1"></i>
        Se muestran <strong>${d.ots.length}</strong> OT(s) donde este equipo aparece como objetivo de una tarea.
        Cada fila enlaza a la ficha completa de la OT.
      </div>
      <div class="table-responsive">
        <table class="table table-sm align-middle" style="font-size:.85rem">
          <thead style="background:#f9fafb">
            <tr>
              <th>OT</th><th>Fecha</th><th>Tipo</th><th>Estado</th>
              <th>Técnico</th><th>Creado por</th><th>Cerrada</th><th></th>
            </tr>
          </thead>
          <tbody>`;
    d.ots.forEach(ot => {
      const tipo_lbl = ot.tipo ? ot.tipo.charAt(0).toUpperCase() + ot.tipo.slice(1) : '—';
      html += `
        <tr>
          <td class="font-monospace small fw-bold" style="color:#0f172a">${escHtml(ot.numero_ot)}</td>
          <td class="small text-muted" style="white-space:nowrap">${escHtml(ot.fecha)}
            ${ot.hora_inicio ? `<br><span style="font-size:.72rem">${escHtml(ot.hora_inicio)}${ot.hora_fin ? '–'+escHtml(ot.hora_fin) : ''}</span>` : ''}
          </td>
          <td class="small">${escHtml(tipo_lbl)}</td>
          <td>${estadoBadge[ot.estado] || `<span class="badge bg-light text-dark">${escHtml(ot.estado || '—')}</span>`}</td>
          <td class="small">${escHtml(ot.tecnico || '—')}</td>
          <td class="small text-muted">${escHtml(ot.creado_por || '—')}</td>
          <td class="small text-muted">${escHtml(ot.cerrada_at || '—')}</td>
          <td><a href="${ot.url}" target="_blank" class="btn btn-xs btn-outline-primary" title="Abrir OT">
            <i class="bi bi-box-arrow-up-right"></i>
          </a></td>
        </tr>`;
    });
    html += '</tbody></table></div>';
    document.getElementById('histot_content').innerHTML = html;
  } catch (e) {
    document.getElementById('histot_content').innerHTML =
      `<div class="alert alert-danger small">Error cargando historial: ${escHtml(e.message)}</div>`;
  }
}

async function verAuditSerie(mid) {
  if (!_modalAuditEq) _modalAuditEq = new bootstrap.Modal(document.getElementById('modalAuditEquipo'));
  document.getElementById('audit_content').innerHTML =
    '<div class="text-center text-muted py-4"><span class="spinner-border spinner-border-sm me-2"></span>Cargando…</div>';
  _modalAuditEq.show();
  try {
    const r = await fetch(`/mantenciones/api/maquinas/${mid}/audit`);
    const d = await r.json();
    if (!d.ok || !d.audit?.length) {
      document.getElementById('audit_content').innerHTML =
        `<div class="text-center text-muted py-4">
          <i class="bi bi-shield-check" style="font-size:2rem;opacity:.3;display:block;margin-bottom:8px"></i>
          Sin cambios registrados para este equipo.
          <div class="small mt-2">Cuando edites el N° serie u otros datos sensibles, aparecerán aquí.</div>
        </div>`;
      return;
    }
    let html = '<div class="table-responsive"><table class="table table-sm" style="font-size:.85rem">';
    html += '<thead style="background:#f9fafb"><tr><th>Fecha</th><th>Campo</th><th>Antes</th><th>Después</th><th>Usuario</th><th>Motivo</th></tr></thead><tbody>';
    d.audit.forEach(a => {
      html += `<tr>
        <td class="small text-muted" style="white-space:nowrap">${escHtml(a.fecha || '—')}</td>
        <td><span class="badge bg-secondary">${escHtml(a.campo)}</span></td>
        <td class="font-monospace small text-danger" style="text-decoration:line-through">${escHtml(a.valor_antes || '(vacío)')}</td>
        <td class="font-monospace small text-success">${escHtml(a.valor_nuevo || '')}</td>
        <td class="small">${escHtml(a.usuario || '—')}</td>
        <td class="small text-muted" style="max-width:280px">${escHtml(a.motivo || '')}</td>
      </tr>`;
    });
    html += '</tbody></table></div>';
    document.getElementById('audit_content').innerHTML = html;
  } catch(e) {
    document.getElementById('audit_content').innerHTML =
      `<div class="alert alert-danger small">Error: ${escHtml(e.message)}</div>`;
  }
}


// ════════════════════════════════════════════════════════════════════
// FICHA TÉCNICA COMPLETA — Modal con stats + visitas + fotos + alertas
// Daniel 2026-05-21: trazabilidad profunda por equipo
// ════════════════════════════════════════════════════════════════════
let _modalFichaTec = null;
let _ftCurrentMid = null;
let _ftCurrentData = null;

async function verFichaTecnicaEquipo(mid, nombre) {
  _ftCurrentMid = mid;
  _ftCurrentData = null;
  if (!_modalFichaTec) {
    _modalFichaTec = new bootstrap.Modal(document.getElementById('modalFichaTecnica'));
  }
  document.getElementById('ft_eq_nombre').textContent = nombre || `Equipo #${mid}`;
  document.getElementById('ft_loading').style.display = 'block';
  document.getElementById('ft_content').style.display = 'none';
  document.getElementById('ft_edit_panel').style.display = 'none';
  // Resetear tab activo a Resumen (default, 2026-05-26)
  try {
    document.querySelectorAll('#ftTabs .nav-link').forEach(b => b.classList.remove('active'));
    document.querySelectorAll('#modalFichaTecnica .tab-pane').forEach(p => p.classList.remove('show','active'));
    const _btnRes = document.querySelector('#ftTabs .nav-link[data-bs-target="#ftTabResumen"]');
    if (_btnRes) _btnRes.classList.add('active');
    const _paneRes = document.getElementById('ftTabResumen');
    if (_paneRes) _paneRes.classList.add('show','active');
  } catch(_){}
  _modalFichaTec.show();

  try {
    const r = await fetch(`/mantenciones/api/maquinas/${mid}/ficha-tecnica`);
    const d = await r.json();
    if (!d.ok) {
      document.getElementById('ft_loading').innerHTML =
        `<div class="alert alert-danger m-4">${escHtml(d.error || 'No se pudo cargar la ficha')}</div>`;
      return;
    }
    _ftCurrentData = d;
    _ftRender(d);
    document.getElementById('ft_loading').style.display = 'none';
    document.getElementById('ft_content').style.display = 'block';
  } catch(e) {
    document.getElementById('ft_loading').innerHTML =
      `<div class="alert alert-danger m-4">Error de red: ${escHtml(e.message)}</div>`;
  }
}

function _ftRender(d) {
  const eq = d.equipo || {};
  const stats = d.stats || {};
  const alertas = d.alertas || [];

  // ── Header con foto GRANDE del último levantamiento (Daniel 2026-05-27)
  // Prioridad: foto más reciente de galería > foto principal del equipo
  // > placeholder. La galería viene ordenada DESC (más reciente primero).
  const fotoEl = document.getElementById('ft_foto_principal');
  const fotosGal = d.fotos_galeria || [];
  const fotoUrl = (fotosGal.length && fotosGal[0].url)
                  || eq.foto_principal_url
                  || eq.foto_url
                  || '';
  if (fotoUrl) {
    fotoEl.innerHTML = `<img src="${escAttr(fotoUrl)}" loading="lazy" decoding="async"
                            style="width:100%;height:100%;object-fit:cover;cursor:zoom-in"
                            alt="Foto del equipo">`;
    fotoEl.dataset.fullUrl = fotoUrl;
    fotoEl.style.cursor = 'zoom-in';
  } else {
    fotoEl.innerHTML = `<div class="ft-foto-placeholder" style="text-align:center;padding:14px">
        <i class="bi bi-camera" style="font-size:2.6rem;color:#cbd5e1"></i>
        <div style="font-size:.72rem;color:#94a3b8;margin-top:8px;font-weight:700">SIN FOTO</div>
        <div style="font-size:.66rem;color:#cbd5e1;margin-top:2px">Captura una en próxima visita</div>
      </div>`;
    delete fotoEl.dataset.fullUrl;
    fotoEl.style.cursor = 'default';
  }
  // ── HEADER PRO (Daniel 2026-05-27 mockup) ──
  // Título grande + SKU debajo + grid de datos clave + chips de estado
  document.getElementById('ft_eq_titulo').textContent = eq.nombre || 'Equipo sin nombre';
  document.getElementById('ft_eq_subtitulo').textContent = eq.sku ? `SKU ${eq.sku}` : 'SKU no asignado';

  // Grid de datos (estilo "label / value" del mockup)
  const dataGridEl = document.getElementById('ft_data_grid');
  if (dataGridEl) {
    const fechaInicio = (d.contratos_relacionados || d.contratos || []).find(c => c.fecha_inicio)?.fecha_inicio || '';
    const estadoLower = (eq.estado || 'activo').toLowerCase();
    const estadoClass = estadoLower === 'baja' ? 'baja' : '';
    const estadoLabel = estadoLower === 'baja' ? 'BAJA' : (estadoLower === 'garantia' ? 'GARANTÍA' : 'ACTIVO');
    const contratos = d.contratos_relacionados || d.contratos || [];
    const ctActivo = contratos.find(c => c.estado === 'vigente' || c.estado === 'indefinido') || contratos[0];
    const cliente = eq.razon_social || '—';
    const marcaModelo = [eq.marca, eq.modelo].filter(Boolean).join(' / ') || '—';
    const ultimaVisita = stats.ultima_visita_fecha || '';
    const diasUlt = stats.dias_desde_ultima_visita;

    const blocks = [
      {label:'<i class="bi bi-upc"></i> Serie / Nº de serie',
       value: `<span style="font-family:monospace;font-size:.85rem">${escHtml(eq.serie_actual || eq.serie || '—')}</span>${(eq.serie_actual||eq.serie)?'<button class="btn btn-link p-0 ms-1" onclick="navigator.clipboard.writeText(\''+escAttr(eq.serie_actual||eq.serie)+'\');ilusToast(\'Copiado\',{type:\'success\'})" title="Copiar"><i class="bi bi-clipboard" style="font-size:.78rem;color:#94a3b8"></i></button>':''}`},
      {label:'<i class="bi bi-circle-fill" style="font-size:.45rem"></i> Estado',
       value: `<span class="estado-pill ${estadoClass}">${estadoLabel}</span>`},
      {label:'<i class="bi bi-building"></i> Cliente / Contrato',
       value: `<span>${escHtml(cliente)}${ctActivo?` <a href="/mantenciones/clientes/${eq.cliente_id}" class="text-decoration-none ms-1" target="_blank"><i class="bi bi-box-arrow-up-right" style="font-size:.78rem;color:#94a3b8"></i></a>`:''}</span>`},
    ];
    if (fechaInicio) blocks.push({label:'<i class="bi bi-calendar3"></i> Inicio contrato', value: escHtml(fechaInicio)});
    if (eq.ubicacion_sala || eq.ubicacion_cliente) blocks.push({label:'<i class="bi bi-geo-alt"></i> Ubicación', value: escHtml(eq.ubicacion_sala || eq.ubicacion_cliente)});
    if (eq.familia_equipo && eq.familia_equipo !== 'otros') blocks.push({label:'<i class="bi bi-grid-3x3"></i> Categoría', value: escHtml(eq.familia_equipo.charAt(0).toUpperCase()+eq.familia_equipo.slice(1))});
    if (marcaModelo !== '—') blocks.push({label:'<i class="bi bi-tag"></i> Marca / Modelo', value: escHtml(marcaModelo)});

    dataGridEl.innerHTML = blocks.map(b => `
      <div class="ft-data-block">
        <div class="ft-data-label">${b.label}</div>
        <div class="ft-data-value">${b.value}</div>
      </div>
    `).join('');
  }

  // Chips de estado bajo el header (pills coloridas)
  const chips = [];
  const estLow = (eq.estado || 'activo').toLowerCase();
  if (estLow === 'activo') chips.push('<span class="ft-chip-pro green"><i class="bi bi-check-circle"></i> Activo</span>');
  else if (estLow === 'baja') chips.push('<span class="ft-chip-pro red"><i class="bi bi-x-circle"></i> Baja</span>');
  else chips.push(`<span class="ft-chip-pro amber"><i class="bi bi-exclamation-circle"></i> ${escHtml(estLow)}</span>`);
  if ((d.contratos_relacionados||d.contratos||[]).length) chips.push('<span class="ft-chip-pro blue"><i class="bi bi-file-text"></i> Con contrato</span>');
  if ((d.fotos_galeria||[]).length) chips.push('<span class="ft-chip-pro blue"><i class="bi bi-images"></i> Con fotos</span>');
  if (stats.ultima_visita_fecha) {
    const dias = stats.dias_desde_ultima_visita;
    const diasTxt = dias != null ? ` (${dias} día${dias===1?'':'s'})` : '';
    chips.push(`<span class="ft-chip-pro blue"><i class="bi bi-calendar-check"></i> Última visita: ${escHtml(stats.ultima_visita_fecha)}${diasTxt}</span>`);
  }
  const chipsEl = document.getElementById('ft_eq_chips');
  if (chipsEl) chipsEl.innerHTML = chips.join('');

  // FIX 2026-05-27 (Daniel): los botones 'Abrir ficha completa', 'PDF',
  // 'Sync fotos' fueron eliminados del HTML. Hacemos getElementById defensivo
  // para que setear href NO explote con "Cannot set properties of null".
  const _btnFichaFull = document.getElementById('ft_btn_ficha_full');
  if (_btnFichaFull) _btnFichaFull.href = d.ficha_url || '#';

  // ── CALIDAD DE FICHA (Daniel 2026-05-26 — score 0-100 con criterios)
  _ftRenderCalidad(d);

  // ── LEVANTAMIENTO INICIAL (Daniel 2026-05-26) ──
  // El levantamiento es la PRIMERA revisión cronológica del equipo.
  // Datos disponibles: revisiones_timeline (DESC) + fotos_galeria (DESC).
  // La PRIMERA cronológicamente es la ÚLTIMA del array.
  _ftRenderLevantamiento(d);

  // ── Alertas ──
  const alertasEl = document.getElementById('ft_alertas');
  if (alertas.length) {
    alertasEl.innerHTML = alertas.map(a => `
      <span class="ft-alert-chip ft-alert-${escAttr(a.severidad || 'info')}">
        <i class="bi bi-${escAttr(a.icono || 'info-circle')}"></i>
        ${escHtml(a.texto)}
      </span>
    `).join('');
    alertasEl.style.display = 'block';
  } else {
    alertasEl.innerHTML = '';
    alertasEl.style.display = 'none';
  }

  // ── Stats cards ──
  _ftRenderStat('visitas', stats.n_visitas_total || 0, 'Visitas totales');
  _ftRenderStat('preventivas', stats.n_visitas_preventivas || 0, 'Preventivas', stats.n_visitas_preventivas ? 'success' : '');
  _ftRenderStat('correctivas', stats.n_visitas_correctivas || 0, 'Correctivas', stats.n_visitas_correctivas ? 'warn' : '');
  const diasUlt = stats.dias_desde_ultima_visita;
  _ftRenderStat('dias_ultima',
    diasUlt !== null && diasUlt !== undefined ? `${diasUlt}d` : '—',
    'Desde última visita',
    diasUlt !== null && diasUlt !== undefined && diasUlt > 120 ? 'warn' : ''
  );
  _ftRenderStat('edad',
    stats.edad_anios !== null && stats.edad_anios !== undefined ? `${stats.edad_anios}a` : '—',
    'Edad equipo'
  );
  const diasGar = stats.dias_en_garantia;
  let garLabel = '—';
  let garCls = '';
  if (diasGar !== null && diasGar !== undefined) {
    if (diasGar < 0) { garLabel = `Vencida`; garCls = 'danger'; }
    else if (diasGar <= 30) { garLabel = `${diasGar}d`; garCls = 'warn'; }
    else { garLabel = `${diasGar}d`; garCls = 'success'; }
  }
  _ftRenderStat('garantia', garLabel, 'Garantía', garCls);

  // ── Badges en tabs ──
  document.getElementById('ft_bdg_visitas').textContent = (d.historial_visitas || []).length;
  document.getElementById('ft_bdg_fotos').textContent = (d.fotos_galeria || []).length;
  document.getElementById('ft_bdg_seriales').textContent = (d.historial_seriales || []).length;
  document.getElementById('ft_bdg_estado').textContent = (d.historial_estado || []).length;
  document.getElementById('ft_bdg_contratos').textContent = (d.contratos_relacionados || []).length;
  // 2026-05-21 (Daniel) — Revisiones por equipo (trazabilidad profunda)
  const _bdgRev = document.getElementById('ft_bdg_revisiones');
  if (_bdgRev) _bdgRev.textContent = (d.revisiones_timeline || []).length;

  // ── Contenido de tabs ──
  _ftRenderResumen(d);
  _ftRenderVisitas(d.historial_visitas || []);
  _ftRenderFotos(d.fotos_galeria || []);
  _ftRenderSeriales(d.historial_seriales || []);
  _ftRenderEstado(d.historial_estado || []);
  _ftRenderContratos(d.contratos_relacionados || []);
  _ftRenderRevisiones(d.revisiones_timeline || [], d.revisiones_counters || {});
  _ftRenderAuditoria(d);  // 2026-05-27 — timeline unificado de cambios

  // Mostrar botón "Sincronizar fotos" solo si el equipo tiene OTs con fotos
  // pero la galería tiene menos fotos (huérfanas en mant_levantamiento_fotos).
  // 2026-05-26 (Daniel) — backfill de fotos del levantamiento.
  try {
    const visitas = d.historial_visitas || [];
    const fotosEnVisitas = visitas.reduce((acc, v) => acc + (v.fotos_count || 0), 0);
    const fotosEnGaleria = (d.fotos_galeria || []).length;
    const btnSync = document.getElementById('ft_btn_sync_fotos');
    if (btnSync) {
      if (fotosEnVisitas > fotosEnGaleria) {
        btnSync.style.display = '';
        btnSync.dataset.huerfanas = fotosEnVisitas - fotosEnGaleria;
        btnSync.title = `Hay ${fotosEnVisitas - fotosEnGaleria} foto(s) de OT que no aparecen en la galería. Click para sincronizar.`;
      } else {
        btnSync.style.display = 'none';
      }
    }
  } catch(_) {}
}

// ════════════════════════════════════════════════════════════════════
// 2026-05-26 (Daniel) — Tab "Resumen" — vista consolidada estilo dashboard
// con: datos del equipo (izq) | historial OT corto (centro) | contrato (der)
// + galería de fotografías abajo. Es el tab DEFAULT al abrir la ficha.
// ════════════════════════════════════════════════════════════════════
function _ftRenderResumen(d) {
  const el = document.getElementById('ftTabResumen');
  if (!el) return;
  const eq = d.equipo || {};
  const stats = d.stats || {};
  const visitas = (d.historial_visitas || []).slice(0, 5);
  const fotos = (d.fotos_galeria || []).slice(0, 10);
  const contratos = d.contratos_relacionados || [];

  // ── Helper para filas de datos
  const _row = (label, val, mono) => {
    const v = (val !== null && val !== undefined && val !== '') ? val : '—';
    return `
      <div class="ft-res-row">
        <div class="ft-res-lbl">${escHtml(label)}</div>
        <div class="ft-res-val${mono ? ' ft-res-mono' : ''}">${escHtml(String(v))}</div>
      </div>`;
  };

  // ── Tipo familia legible
  const familiaLabel = {
    cardio: 'Cardio',
    selectorizado: 'Selectorizado',
    peso_libre: 'Peso libre',
    funcional: 'Funcional',
    fuerza: 'Fuerza',
    accesorios: 'Accesorios',
    cross: 'Cross training',
    musculacion: 'Musculación',
    otros: 'Otros',
  }[(eq.familia_equipo || '').toLowerCase()] || (eq.familia_equipo || '—');

  // ── Datos del equipo (columna izq)
  const datosHtml = `
    <div class="ft-res-card">
      <div class="ft-res-card-hdr">
        <i class="bi bi-clipboard-data me-1"></i>Datos del equipo
      </div>
      <div class="ft-res-card-body">
        ${_row('SKU', eq.sku, true)}
        ${_row('N° de serie', eq.serie_actual || eq.serie, true)}
        ${_row('Marca / Modelo', [eq.marca, eq.modelo].filter(Boolean).join(' / '))}
        ${_row('Categoría', familiaLabel)}
        ${_row('Peso', eq.peso_kg ? `${eq.peso_kg} kg` : null)}
        ${_row('Dimensiones', eq.dimensiones)}
        ${_row('Año fabricación', eq.anio_fabricacion)}
        ${_row('Color', eq.color)}
        ${_row('Ubicación', eq.ubicacion_sala)}
        ${_row('Voltaje', eq.voltaje)}
        ${_row('Notas', eq.observaciones)}
      </div>
    </div>
  `;

  // ── Historial OT (columna centro)
  let historialHtml;
  if (!visitas.length) {
    historialHtml = `
      <div class="ft-res-card">
        <div class="ft-res-card-hdr">
          <i class="bi bi-list-check me-1"></i>Historial de OTs
        </div>
        <div class="ft-res-card-body text-center text-muted py-4">
          <i class="bi bi-calendar-x" style="font-size:1.6rem;opacity:.3"></i>
          <div class="small mt-1">Sin OTs aún</div>
        </div>
      </div>`;
  } else {
    const items = visitas.map(v => {
      const tipoBadgeColor = {
        levantamiento: '#dbeafe',
        preventiva:    '#dcfce7',
        correctiva:    '#fef3c7',
      }[(v.tipo||'').toLowerCase()] || '#f3f4f6';
      const tipoBadgeText = {
        levantamiento: '#1e40af',
        preventiva:    '#166534',
        correctiva:    '#92400e',
      }[(v.tipo||'').toLowerCase()] || '#374151';
      const estadoChip = {
        cerrada:    'Cerrada',
        completada: 'Cerrada',
        en_curso:   'En curso',
        programada: 'Pendiente',
      }[(v.estado||'').toLowerCase()] || (v.estado || '');
      return `
        <div class="ft-res-ot-item">
          <div class="ft-res-ot-num">
            <a href="${escAttr(v.url||'#')}" target="_blank" rel="noopener" class="text-decoration-none">
              ${escHtml(v.numero_ot || '—')}
            </a>
          </div>
          <div class="ft-res-ot-body">
            <div class="ft-res-ot-title">
              <span class="ft-res-ot-tipo" style="background:${tipoBadgeColor};color:${tipoBadgeText}">${escHtml(v.tipo || '—')}</span>
              ${v.titulo ? escHtml(v.titulo) : 'Sin título'}
              ${estadoChip ? `<span class="ft-res-ot-estado">${escHtml(estadoChip)}</span>` : ''}
            </div>
            <div class="ft-res-ot-meta">
              <i class="bi bi-calendar3"></i> ${escHtml(v.fecha || '—')}
              · <i class="bi bi-person"></i> ${escHtml(v.tecnico || '—')}
              ${v.fotos_count ? ` · <span style="color:#dc2626;font-weight:600"><i class="bi bi-camera"></i> ${v.fotos_count} foto(s)</span>` : ''}
            </div>
          </div>
        </div>
      `;
    }).join('');
    const totalOts = (d.historial_visitas || []).length;
    historialHtml = `
      <div class="ft-res-card">
        <div class="ft-res-card-hdr d-flex justify-content-between align-items-center">
          <span><i class="bi bi-list-check me-1"></i>Historial de OTs</span>
          ${totalOts > 5 ? `<span class="ft-res-card-link" onclick="document.querySelector('#ftTabs .nav-link[data-bs-target=&quot;#ftTabVisitas&quot;]').click()">Ver las ${totalOts} OTs <i class="bi bi-arrow-right"></i></span>` : ''}
        </div>
        <div class="ft-res-card-body" style="padding:6px 4px">
          ${items}
        </div>
      </div>`;
  }

  // ── Contrato (columna derecha)
  let contratoHtml;
  if (!contratos.length) {
    contratoHtml = `
      <div class="ft-res-card">
        <div class="ft-res-card-hdr">
          <i class="bi bi-file-earmark-text me-1"></i>Contrato / Arriendo / Mantención
        </div>
        <div class="ft-res-card-body text-center text-muted py-4">
          <i class="bi bi-file-earmark" style="font-size:1.6rem;opacity:.3"></i>
          <div class="small mt-1">Sin contratos vigentes</div>
        </div>
      </div>`;
  } else {
    const c = contratos[0];
    const dr = c.dias_restantes;
    let estadoBadge = { color:'#6b7280', text: c.es_indefinido ? 'Indefinido' : 'Vigente' };
    if (!c.es_indefinido && dr !== null && dr !== undefined) {
      if (dr < 0) estadoBadge = { color:'#dc2626', text:'Vencido' };
      else if (dr <= 30) estadoBadge = { color:'#f59e0b', text:`Vence en ${dr}d` };
      else estadoBadge = { color:'#16a34a', text:'Vigente' };
    }
    contratoHtml = `
      <div class="ft-res-card">
        <div class="ft-res-card-hdr">
          <i class="bi bi-file-earmark-text me-1"></i>Contrato / Arriendo / Mantención
        </div>
        <div class="ft-res-card-body">
          ${_row('Tipo', c.tipo || 'Mantención')}
          ${_row('N° de contrato', c.numero || c.nombre || `#${c.id}`, true)}
          ${_row('Inicio', c.fecha_inicio)}
          ${_row('Término', c.fecha_vencimiento || (c.es_indefinido ? 'Indefinido' : '—'))}
          <div class="ft-res-row">
            <div class="ft-res-lbl">Estado</div>
            <div class="ft-res-val">
              <span class="badge" style="background:${estadoBadge.color}15;color:${estadoBadge.color};border:1px solid ${estadoBadge.color}50;padding:4px 9px">
                ${escHtml(estadoBadge.text)}
              </span>
            </div>
          </div>
          ${contratos.length > 1 ? `
            <div class="ft-res-row" style="border-top:1px dashed #e5e7eb;margin-top:8px;padding-top:8px">
              <div class="ft-res-lbl">Otros contratos</div>
              <div class="ft-res-val">
                <span class="ft-res-card-link" onclick="document.querySelector('#ftTabs .nav-link[data-bs-target=&quot;#ftTabContratos&quot;]').click()">
                  ${contratos.length - 1} más <i class="bi bi-arrow-right"></i>
                </span>
              </div>
            </div>` : ''}
        </div>
      </div>`;
  }

  // ── Galería de fotos (fila inferior)
  let galeriaHtml;
  if (!fotos.length) {
    galeriaHtml = `
      <div class="ft-res-card ft-res-card-wide">
        <div class="ft-res-card-hdr">
          <i class="bi bi-images me-1"></i>Fotografías del equipo
        </div>
        <div class="ft-res-card-body text-center text-muted py-4">
          <i class="bi bi-image" style="font-size:1.6rem;opacity:.3"></i>
          <div class="small mt-1">Sin fotografías cargadas todavía</div>
          <div class="small mt-1" style="font-size:.7rem;opacity:.7">Las fotos del levantamiento y de visitas aparecerán acá.</div>
        </div>
      </div>`;
  } else {
    const totalFotos = (d.fotos_galeria || []).length;
    galeriaHtml = `
      <div class="ft-res-card ft-res-card-wide">
        <div class="ft-res-card-hdr d-flex justify-content-between align-items-center">
          <span><i class="bi bi-images me-1"></i>Fotografías del equipo <span class="badge bg-light text-dark ms-1">${totalFotos}</span></span>
          ${totalFotos > 10 ? `<span class="ft-res-card-link" onclick="document.querySelector('#ftTabs .nav-link[data-bs-target=&quot;#ftTabFotos&quot;]').click()">Ver todas <i class="bi bi-arrow-right"></i></span>` : ''}
        </div>
        <div class="ft-res-card-body">
          <div class="ft-res-fotos">
            ${fotos.map(f => `
              <div class="ft-res-foto" onclick="window.open('${escAttr(f.url)}','_blank')" title="${escAttr(f.descripcion || f.tomada_por || 'Foto')}">
                <img src="${escAttr(f.url)}" alt="" loading="lazy">
              </div>
            `).join('')}
          </div>
        </div>
      </div>`;
  }

  // ── Estilos del tab Resumen (inyectados una sola vez)
  const styleId = 'ft-resumen-style';
  if (!document.getElementById(styleId)) {
    const st = document.createElement('style');
    st.id = styleId;
    st.textContent = `
      .ft-res-grid{display:grid;grid-template-columns:1fr 1.3fr 1fr;gap:14px;align-items:start;margin-bottom:14px}
      @media (max-width: 992px){.ft-res-grid{grid-template-columns:1fr}}
      .ft-res-card{background:#fff;border:1px solid #e5e7eb;border-radius:12px;overflow:hidden}
      .ft-res-card-wide{grid-column:1/-1}
      .ft-res-card-hdr{padding:10px 14px;background:#f9fafb;border-bottom:1px solid #e5e7eb;font-weight:700;font-size:.84rem;color:#0f172a}
      .ft-res-card-body{padding:10px 14px}
      .ft-res-row{display:flex;justify-content:space-between;gap:10px;padding:6px 0;border-bottom:1px dashed #f3f4f6;font-size:.82rem}
      .ft-res-row:last-child{border-bottom:0}
      .ft-res-lbl{color:#6b7280;font-weight:500;min-width:90px}
      .ft-res-val{color:#0f172a;font-weight:600;text-align:right;flex:1;word-break:break-word}
      .ft-res-mono{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:.78rem}
      .ft-res-card-link{font-size:.74rem;color:#dc2626;font-weight:700;cursor:pointer}
      .ft-res-card-link:hover{text-decoration:underline}
      .ft-res-ot-item{display:flex;gap:10px;padding:9px 8px;border-bottom:1px solid #f3f4f6}
      .ft-res-ot-item:last-child{border-bottom:0}
      .ft-res-ot-num{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:.74rem;font-weight:700;background:#f3f4f6;padding:4px 6px;border-radius:6px;height:fit-content;white-space:nowrap;color:#0f172a}
      .ft-res-ot-num a{color:#0f172a}
      .ft-res-ot-body{flex:1;min-width:0}
      .ft-res-ot-title{font-weight:600;font-size:.82rem;color:#0f172a;line-height:1.25;display:flex;gap:6px;align-items:center;flex-wrap:wrap}
      .ft-res-ot-tipo{font-size:.66rem;font-weight:700;padding:2px 7px;border-radius:10px;text-transform:uppercase;letter-spacing:.02em}
      .ft-res-ot-estado{font-size:.66rem;color:#6b7280;background:#f3f4f6;padding:2px 7px;border-radius:10px;font-weight:600}
      .ft-res-ot-meta{font-size:.72rem;color:#6b7280;margin-top:3px}
      .ft-res-fotos{display:grid;grid-template-columns:repeat(auto-fill,minmax(86px,1fr));gap:7px}
      .ft-res-foto{aspect-ratio:1;border-radius:8px;overflow:hidden;cursor:pointer;background:#f3f4f6;border:1px solid #e5e7eb;transition:transform .15s ease, box-shadow .15s ease}
      .ft-res-foto:hover{transform:scale(1.03);box-shadow:0 4px 14px rgba(0,0,0,.12)}
      .ft-res-foto img{width:100%;height:100%;object-fit:cover}
    `;
    document.head.appendChild(st);
  }

  el.innerHTML = `
    <div class="ft-res-grid">
      ${datosHtml}
      ${historialHtml}
      ${contratoHtml}
    </div>
    ${galeriaHtml}
  `;
}

// ── Descargar PDF de la ficha técnica (2026-05-26 Daniel) ────────────
function ftDescargarPDF() {
  if (!_ftCurrentMid) return;
  // Endpoint genérico de PDF de ficha de equipo. Si no existe en backend,
  // se cae al print() nativo del navegador con la URL de ficha completa.
  const url = `/mantenciones/maquinas/${_ftCurrentMid}/ficha-tecnica.pdf`;
  // Probamos primero el PDF directo; si 404, abrimos ficha completa para imprimir.
  fetch(url, { method:'HEAD' }).then(r => {
    if (r.ok) {
      window.open(url, '_blank');
    } else {
      // Fallback: abre ficha completa en pestaña nueva y dispara print al cargar
      const w = window.open(`/mantenciones/maquinas/${_ftCurrentMid}?print=1`, '_blank');
      if (w) w.focus();
    }
  }).catch(() => {
    const w = window.open(`/mantenciones/maquinas/${_ftCurrentMid}?print=1`, '_blank');
    if (w) w.focus();
  });
}

// ── Sincronizar fotos huérfanas del levantamiento → galería del equipo
// 2026-05-26 (Daniel) — Si en alguna sesión vieja el INSERT a
// mant_maquina_fotos falló silenciosamente, las fotos quedaron solo en
// mant_levantamiento_fotos. Este botón llama al backfill para copiarlas.
async function ftSincronizarFotos() {
  if (!_ftCurrentMid) return;
  const btn = document.getElementById('ft_btn_sync_fotos');
  const ok = await ilusConfirm({
    title: 'Sincronizar fotos del equipo',
    message: 'Vamos a buscar fotos de las OT (levantamientos) de este equipo que no aparezcan todavía en la galería y copiarlas. ¿Continúo?',
    sub: 'Es seguro — usa anti-duplicado por URL.',
    okLabel: 'Sí, sincronizar', cancelLabel: 'Cancelar',
  });
  if (!ok) return;
  if (btn) {
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Sincronizando…';
  }
  try {
    const r = await fetch(`/mantenciones/api/maquinas/${_ftCurrentMid}/sync-fotos-lev`, {
      method:'POST',
      headers:{'Content-Type':'application/json'},
    });
    const d = await r.json();
    if (!d.ok) {
      await ilusAlert({ title:'Error', message: d.error || 'No se pudo sincronizar', type:'error' });
    } else {
      ilusToast(`✓ ${d.copiadas || 0} foto(s) copiada(s) a la galería`, { type:'success' });
      // Refrescar ficha
      if (_ftCurrentMid) {
        const eq = _ftCurrentData ? _ftCurrentData.equipo : null;
        await verFichaTecnicaEquipo(_ftCurrentMid, eq ? eq.nombre : '');
      }
    }
  } catch(e) {
    await ilusAlert({ title:'Error de red', message: e.message || 'No se pudo conectar', type:'error' });
  } finally {
    if (btn) {
      btn.disabled = false;
      btn.innerHTML = '<i class="bi bi-arrow-repeat me-1"></i>Sincronizar fotos';
    }
  }
}

// ════════════════════════════════════════════════════════════════════
// 2026-05-21 (Daniel) — Tab "Revisiones" — timeline de cada vez que
// este equipo apareció en una visita, con estado de revisión
// (verificado / con_cambios / saltado / falla_detectada).
// Da trazabilidad completa: "Revisado 5 veces, 1 saltado, 4 verificado".
// ════════════════════════════════════════════════════════════════════
function _ftRenderRevisiones(revisiones, counters) {
  const el = document.getElementById('ftTabRevisiones');
  if (!el) return;
  if (!revisiones.length) {
    el.innerHTML = `<div class="text-center text-muted py-4">
      <i class="bi bi-clipboard-pulse" style="font-size:2rem;opacity:.3"></i>
      <div class="fw-semibold mt-2">Sin revisiones registradas</div>
      <div class="small mt-1">Cuando un técnico revise este equipo en una OT, se mostrará aquí.</div>
    </div>`;
    return;
  }
  // ── Header con contadores ──
  const c = counters || {};
  const total = c.total || revisiones.length;
  const _statCard = (cls, icon, n, label) => `
    <div class="ft-rev-stat ${cls}">
      <div class="ft-rev-stat-icon"><i class="bi bi-${icon}"></i></div>
      <div>
        <div class="ft-rev-stat-num">${n || 0}</div>
        <div class="ft-rev-stat-lbl">${escHtml(label)}</div>
      </div>
    </div>`;
  const headerHtml = `
    <div class="ft-rev-counters">
      ${_statCard('all',           'list-check',              total,                 'Revisiones')}
      ${_statCard('verificado',    'check-circle-fill',       c.verificado || 0,     'Verificadas')}
      ${_statCard('con_cambios',   'pencil-fill',             c.con_cambios || 0,    'Con cambios')}
      ${_statCard('saltado',       'skip-forward-fill',       c.saltado || 0,        'Saltadas')}
      ${_statCard('falla',         'exclamation-triangle-fill', c.falla_detectada || 0, 'Fallas detectadas')}
    </div>
    <style>
      .ft-rev-counters{display:grid;grid-template-columns:repeat(auto-fit,minmax(110px,1fr));
        gap:8px;margin-bottom:16px;padding:8px 0;}
      .ft-rev-stat{display:flex;gap:8px;align-items:center;padding:9px 11px;
        border-radius:10px;border:1px solid #e5e7eb;background:#fafafa;}
      .ft-rev-stat-icon{width:28px;height:28px;border-radius:7px;display:flex;
        align-items:center;justify-content:center;color:#fff;font-size:.95rem;flex-shrink:0;}
      .ft-rev-stat.all       .ft-rev-stat-icon{background:#374151}
      .ft-rev-stat.verificado .ft-rev-stat-icon{background:#16a34a}
      .ft-rev-stat.con_cambios .ft-rev-stat-icon{background:#3b82f6}
      .ft-rev-stat.saltado    .ft-rev-stat-icon{background:#f59e0b}
      .ft-rev-stat.falla      .ft-rev-stat-icon{background:#dc2626}
      .ft-rev-stat-num{font-weight:800;font-size:1.1rem;color:#0f172a;line-height:1}
      .ft-rev-stat-lbl{font-size:.68rem;color:#6b7280;margin-top:2px;text-transform:uppercase;letter-spacing:.03em}
      .ft-rev-item{display:flex;gap:12px;padding:12px;border:1px solid #e5e7eb;
        border-radius:11px;margin-bottom:10px;background:#fff;}
      .ft-rev-item.saltado{border-left:3px solid #f59e0b;background:#fffbeb}
      .ft-rev-item.falla_detectada{border-left:3px solid #dc2626;background:#fef2f2}
      .ft-rev-item.con_cambios{border-left:3px solid #3b82f6}
      .ft-rev-item.verificado{border-left:3px solid #16a34a}
      .ft-rev-icon{width:34px;height:34px;border-radius:8px;display:flex;align-items:center;
        justify-content:center;font-size:1rem;color:#fff;flex-shrink:0;}
      .ft-rev-info{flex:1;min-width:0}
      .ft-rev-title{font-weight:700;color:#0f172a;font-size:.92rem;line-height:1.2}
      .ft-rev-meta{font-size:.72rem;color:#6b7280;margin-top:3px}
      .ft-rev-obs{margin-top:6px;font-size:.78rem;color:#374151;background:#f9fafb;
        padding:7px 9px;border-radius:7px;border-left:2px solid #d1d5db;}
      .ft-rev-link{font-size:.74rem;color:#dc2626;font-weight:600;text-decoration:none;}
      .ft-rev-link:hover{text-decoration:underline}
    </style>
  `;

  // ── Items ──
  const items = revisiones.map(r => {
    const estado = (r.estado_revision || 'verificado').toLowerCase();
    const iconBg = {
      'verificado':       '#16a34a',
      'con_cambios':      '#3b82f6',
      'saltado':          '#f59e0b',
      'falla_detectada':  '#dc2626',
    }[estado] || '#6b7280';
    const iconName = {
      'verificado':       'check-circle-fill',
      'con_cambios':      'pencil-fill',
      'saltado':          'skip-forward-fill',
      'falla_detectada':  'exclamation-triangle-fill',
    }[estado] || 'circle';
    const estadoLabel = {
      'verificado':       'Verificado',
      'con_cambios':      'Con cambios',
      'saltado':          `Saltado${r.razon_saltado ? ' · ' + r.razon_saltado.replace(/_/g,' ') : ''}`,
      'falla_detectada':  'Falla detectada',
    }[estado] || estado;
    const fecha = r.revisado_at || r.fecha || '';
    const tipoBadge = r.tipo_visita
      ? `<span class="badge bg-light text-dark me-1" style="font-size:.66rem;font-weight:600">${escHtml(r.tipo_visita)}</span>`
      : '';
    return `
      <div class="ft-rev-item ${estado}">
        <div class="ft-rev-icon" style="background:${iconBg}">
          <i class="bi bi-${iconName}"></i>
        </div>
        <div class="ft-rev-info">
          <div class="ft-rev-title">
            ${escHtml(estadoLabel)}
            ${r.fotos_count ? `<span class="badge bg-light text-dark ms-1" style="font-size:.66rem"><i class="bi bi-camera"></i> ${r.fotos_count}</span>` : ''}
          </div>
          <div class="ft-rev-meta">
            ${tipoBadge}
            <i class="bi bi-receipt"></i> ${escHtml(r.numero_ot || '')}
            ${fecha ? ` · <i class="bi bi-calendar3"></i> ${escHtml(fecha)}` : ''}
            ${r.revisado_por ? ` · <i class="bi bi-person"></i> ${escHtml(r.revisado_por)}` : ''}
          </div>
          ${r.observacion ? `<div class="ft-rev-obs">${escHtml(r.observacion)}</div>` : ''}
          ${r.url_ot ? `<a href="${escAttr(r.url_ot)}" class="ft-rev-link mt-1 d-inline-block" target="_blank" rel="noopener">
            <i class="bi bi-box-arrow-up-right"></i> Ver OT
          </a>` : ''}
        </div>
      </div>`;
  }).join('');

  el.innerHTML = headerHtml + items;
}

// Renderiza la card "Calidad de la ficha" — score visual + criterios pendientes.
function _ftRenderCalidad(d) {
  const card = document.getElementById('ft_calidad_card');
  if (!card) return;
  const cal = d.calidad_ficha;
  if (!cal) { card.style.display = 'none'; return; }

  const score = Math.max(0, Math.min(100, cal.score || 0));
  const estado = cal.estado || 'incompleta';
  const pendientes = cal.pendientes || [];

  // Color según estado
  const cfg = {
    'completa':       { bg: '#16a34a', label: '✓ FICHA COMPLETA',  textCol: '#15803d' },
    'buena':          { bg: '#3b82f6', label: 'BUENA',              textCol: '#1d4ed8' },
    'revisar_datos':  { bg: '#f59e0b', label: '⚠ REVISAR DATOS',    textCol: '#b45309' },
    'incompleta':     { bg: '#dc2626', label: '✕ INCOMPLETA',       textCol: '#991b1b' },
  }[estado] || { bg: '#94a3b8', label: '—', textCol: '#64748b' };

  // Barra + score grande
  const bar = document.getElementById('ft_cal_bar');
  if (bar) {
    bar.style.width = score + '%';
    bar.style.background = cfg.bg;
  }
  const scoreEl = document.getElementById('ft_cal_score');
  if (scoreEl) {
    scoreEl.textContent = score;
    scoreEl.style.color = cfg.bg;
  }
  // Badge de estado
  const badgeEl = document.getElementById('ft_cal_estado_badge');
  if (badgeEl) {
    badgeEl.innerHTML = `<span class="badge" style="background:${cfg.bg};color:#fff;font-size:.65rem;padding:3px 9px;font-weight:600">${cfg.label}</span>`;
  }
  // Pendientes (máx 4 visibles)
  const pendEl = document.getElementById('ft_cal_pendientes');
  if (pendEl) {
    if (pendientes.length === 0) {
      pendEl.innerHTML = '<i class="bi bi-check-circle-fill me-1" style="color:#16a34a"></i>Todos los criterios cumplidos.';
    } else {
      const muestra = pendientes.slice(0, 4).map(p => `<span class="badge" style="background:#fef2f2;color:#991b1b;font-size:.62rem;padding:2px 7px;margin-right:4px;margin-top:3px">✕ ${p}</span>`).join('');
      const extra = pendientes.length > 4 ? ` <span style="color:#94a3b8;font-size:.7rem">+${pendientes.length - 4} más</span>` : '';
      pendEl.innerHTML = `<div style="margin-top:2px"><strong style="color:${cfg.textCol}">Pendientes:</strong> ${muestra}${extra}</div>`;
    }
  }
  card.style.display = 'block';
}

// Renderiza la card "Levantamiento inicial" — fotó + fecha + estado +
// daños + observaciones (extraído de la PRIMERA revisión cronológica).
function _ftRenderLevantamiento(d) {
  // CAMBIO 2026-05-27 (Daniel): info levantamiento ahora va INTEGRADA en
  // el header del modal (ft_lev_info_inline), no como card separada.
  // La foto va ARRIBA con ft_foto_principal (manejado en _ftRender).
  // Aquí solo poblamos los datos textuales del levantamiento.
  const inline = document.getElementById('ft_lev_info_inline');
  if (!inline) return;
  const revisiones = d.revisiones_timeline || [];
  const fotos      = d.fotos_galeria || [];
  const eq         = d.equipo || {};

  // Última revisión cronológica = index 0 (orden DESC)
  const ultRev  = revisiones.length ? revisiones[0] : null;
  const ultFoto = fotos.length ? fotos[0] : null;

  // Mostrar solo si hay info de levantamiento real (revisión o foto)
  if (!ultRev && !ultFoto) {
    inline.style.display = 'none';
    return;
  }
  inline.style.display = 'block';

  // ── Fecha del último levantamiento ──
  let fecha = '';
  if (ultRev && ultRev.revisado_at)      fecha = ultRev.revisado_at;
  else if (ultRev && ultRev.fecha)       fecha = ultRev.fecha;
  else if (ultFoto && ultFoto.fecha)     fecha = ultFoto.fecha;
  document.getElementById('ft_lev_fecha').textContent = fecha ? ('📅 ' + fecha) : '— sin levantamientos registrados';

  // ── Estado capturado en la última revisión ──
  const estado = ultRev ? (ultRev.estado_revision || '').toLowerCase() : '';
  const estadoBadgeEl = document.getElementById('ft_lev_estado_badge');
  const estadosCfg = {
    'operativo':         { bg: '#16a34a', label: '✓ OPERATIVO' },
    'verificado':        { bg: '#16a34a', label: '✓ VERIFICADO' },
    'con_cambios':       { bg: '#3b82f6', label: '↻ CON CAMBIOS' },
    'con_falla':         { bg: '#dc2626', label: '⚠ CON FALLA' },
    'con_observaciones': { bg: '#f59e0b', label: '⚠ CON OBSERVACIONES' },
    'falla_detectada':   { bg: '#dc2626', label: '⚠ FALLA DETECTADA' },
    'fuera_servicio':    { bg: '#7c2d12', label: '✕ FUERA DE SERVICIO' },
    'saltado':           { bg: '#94a3b8', label: '— NO REVISADO' },
  };
  const cfg = estadosCfg[estado];
  estadoBadgeEl.innerHTML = cfg
    ? `<span class="badge" style="background:${cfg.bg};color:#fff;font-size:.68rem;padding:4px 9px;font-weight:700">${cfg.label}</span>`
    : (ultRev ? `<span class="badge" style="background:#94a3b8;color:#fff;font-size:.68rem;padding:4px 9px">${escHtml(estado.toUpperCase()||'—')}</span>` : '');

  // ── Daños: warning visible si estado indica problema ──
  const danosEl = document.getElementById('ft_lev_danos');
  const conDanos = ['con_falla', 'fuera_servicio', 'con_observaciones', 'falla_detectada'].includes(estado);
  danosEl.style.display = conDanos ? 'flex' : 'none';

  // ── Observaciones del técnico (de la última revisión) ──
  const obsEl = document.getElementById('ft_lev_observaciones');
  let obs = '';
  if (ultRev) {
    obs = (ultRev.observacion || ultRev.razon_saltado || '').trim();
  }
  if (!obs && ultFoto && ultFoto.descripcion) {
    obs = (ultFoto.descripcion || '').trim();
  }
  if (!obs && eq.observaciones) {
    obs = (eq.observaciones || '').trim();
  }
  if (obs) {
    obsEl.textContent = obs;
    obsEl.style.color = '#1f2937';
    obsEl.style.fontStyle = 'normal';
  } else {
    obsEl.textContent = 'Sin observaciones registradas. Captura observaciones en la próxima visita técnica.';
    obsEl.style.color = '#9ca3af';
    obsEl.style.fontStyle = 'italic';
  }

  // ── Técnico responsable + contador en una línea ──
  const tecEl = document.getElementById('ft_lev_tecnico');
  let tec = '';
  if (ultRev && ultRev.revisado_por) tec = ultRev.revisado_por;
  else if (ultFoto && ultFoto.tomada_por) tec = ultFoto.tomada_por;
  const partes = [];
  if (tec) partes.push(`<i class="bi bi-person-circle me-1"></i>Técnico: <strong>${escHtml(tec)}</strong>`);
  if (revisiones.length > 1) partes.push(`<i class="bi bi-clock-history ms-2 me-1"></i><strong>${revisiones.length}</strong> revisiones totales`);
  tecEl.innerHTML = partes.join('');
}

function _ftRenderStat(key, val, label, cls) {
  const el = document.querySelector(`.ft-stat[data-key="${key}"]`);
  if (!el) return;
  el.className = `ft-stat${cls ? ' ft-stat-' + cls : ''}`;
  el.dataset.key = key;
  el.innerHTML = `
    <div class="ft-stat-val">${escHtml(String(val))}</div>
    <div class="ft-stat-lbl">${escHtml(label)}</div>
  `;
}

function _ftRenderVisitas(visitas) {
  const el = document.getElementById('ftTabVisitas');
  if (!visitas.length) {
    el.innerHTML = `<div class="text-center text-muted py-4">
      <i class="bi bi-calendar-x" style="font-size:2rem;opacity:.3"></i>
      <div class="fw-semibold mt-2">Sin visitas registradas</div>
      <div class="small mt-1">Cuando este equipo aparezca en una OT, se mostrará aquí.</div>
    </div>`;
    return;
  }
  el.innerHTML = visitas.map(v => {
    const tipoLower = (v.tipo || '').toLowerCase();
    const estadoBadge = {
      'cerrada':     '<span class="badge" style="background:#dcfce7;color:#166534">Cerrada</span>',
      'completada':  '<span class="badge" style="background:#dcfce7;color:#166534">Completada</span>',
      'en_curso':    '<span class="badge" style="background:#fef3c7;color:#92400e">En curso</span>',
      'programada':  '<span class="badge" style="background:#dbeafe;color:#1e40af">Pendiente</span>',
      'cancelada':   '<span class="badge" style="background:#fee2e2;color:#991b1b">Cancelada</span>',
    }[v.estado] || `<span class="badge bg-secondary">${escHtml(v.estado||'—')}</span>`;
    const fact = v.factura ? `<span class="badge bg-light text-dark ms-1" title="Factura ERP" style="font-family:monospace">${escHtml(v.factura.tido)} ${escHtml(v.factura.nudo)}</span>` : '';
    return `
      <div class="ft-timeline-item ${tipoLower}">
        <div class="d-flex justify-content-between align-items-start gap-2 flex-wrap">
          <div style="flex:1;min-width:200px">
            <div class="fw-bold" style="font-size:.92rem">
              <a href="${escAttr(v.url)}" target="_blank" class="text-decoration-none text-dark" style="font-family:monospace">${escHtml(v.numero_ot)}</a>
              ${v.titulo ? ` · ${escHtml(v.titulo)}` : ''}
            </div>
            <div class="d-flex gap-2 flex-wrap mt-1" style="font-size:.78rem;color:#6b7280">
              <span><i class="bi bi-calendar3 me-1"></i>${escHtml(v.fecha || '—')}</span>
              <span><i class="bi bi-tag me-1"></i>${escHtml(v.tipo || '—')}</span>
              <span><i class="bi bi-person me-1"></i>${escHtml(v.tecnico || '—')}</span>
              ${v.fotos_count ? `<span style="color:#16a34a"><i class="bi bi-images me-1"></i>${v.fotos_count} foto(s)</span>` : ''}
              ${fact}
            </div>
            ${v.observaciones ? `<div class="small mt-1" style="color:#374151;background:#f9fafb;padding:6px 10px;border-radius:6px;border-left:3px solid #e5e7eb">${escHtml(v.observaciones)}</div>` : ''}
          </div>
          <div class="text-end">${estadoBadge}</div>
        </div>
      </div>
    `;
  }).join('');
}

// ── Galería tipo Facebook con lightbox (Daniel 2026-05-27) ──
// Foto principal grande + grid de miniaturas + lightbox al hacer click.
window._ftFotos = [];   // array de fotos para el lightbox
window._ftFotoIdx = 0;  // index actual en el lightbox

function _ftRenderFotos(fotos) {
  const el = document.getElementById('ftTabFotos');
  if (!fotos.length) {
    el.innerHTML = `<div class="text-center text-muted py-5">
      <i class="bi bi-images" style="font-size:3rem;opacity:.25"></i>
      <div class="fw-semibold mt-3" style="font-size:1rem">Aún no existen fotografías asociadas a este equipo</div>
      <div class="small mt-2" style="max-width:420px;margin:0 auto;color:#94a3b8">Cuando un técnico capture fotos del equipo en una OT (levantamiento, antes/después, evidencia de daño, etc.), aparecerán aquí ordenadas por fecha.</div>
    </div>`;
    return;
  }
  // Guardamos las fotos para el lightbox
  window._ftFotos = fotos;
  // Foto principal (la más reciente) + miniaturas
  const principal = fotos[0];
  const otras = fotos.slice(1);
  el.innerHTML = `
    <style>
      .ft-gal-main {
        width:100%;max-height:480px;border-radius:14px;background:#0f172a;
        display:flex;align-items:center;justify-content:center;overflow:hidden;
        cursor:zoom-in;position:relative;
      }
      .ft-gal-main img { max-width:100%;max-height:480px;object-fit:contain;display:block; }
      .ft-gal-main-info {
        position:absolute;left:0;right:0;bottom:0;
        background:linear-gradient(to top, rgba(0,0,0,.85), transparent);
        color:#fff;padding:18px 20px 14px;font-size:.84rem;
      }
      .ft-gal-grid {
        display:grid;grid-template-columns:repeat(auto-fill,minmax(110px,1fr));
        gap:8px;margin-top:14px;
      }
      .ft-gal-thumb {
        position:relative;aspect-ratio:1/1;border-radius:8px;overflow:hidden;
        cursor:zoom-in;background:#f1f5f9;border:2px solid transparent;
        transition:border-color .12s,transform .12s;
      }
      .ft-gal-thumb:hover { border-color:#dc2626;transform:scale(1.03); }
      .ft-gal-thumb img { width:100%;height:100%;object-fit:cover; }
      .ft-gal-thumb-label {
        position:absolute;top:6px;left:6px;background:rgba(0,0,0,.7);color:#fff;
        font-size:.62rem;padding:2px 7px;border-radius:10px;font-weight:600;
        text-transform:uppercase;letter-spacing:.3px;
      }
    </style>
    <div class="ft-gal-main" onclick="ftLightboxAbrir(0)">
      <img src="${escAttr(principal.url)}" alt="${escAttr(principal.descripcion||'Foto principal')}" loading="lazy">
      <div class="ft-gal-main-info">
        <div style="font-weight:600">${escHtml(principal.descripcion || 'Foto del equipo')}</div>
        <div style="opacity:.85;font-size:.74rem;margin-top:2px">
          <i class="bi bi-camera-fill me-1"></i>${escHtml(principal.tipo_foto || 'principal')}
          ${principal.fecha ? ' · <i class="bi bi-calendar3 me-1"></i>' + escHtml(principal.fecha) : ''}
          ${principal.tomada_por ? ' · <i class="bi bi-person-circle me-1"></i>' + escHtml(principal.tomada_por) : ''}
        </div>
      </div>
    </div>
    ${otras.length ? `
      <div class="ft-gal-grid">
        ${otras.map((f, i) => `
          <div class="ft-gal-thumb" onclick="ftLightboxAbrir(${i + 1})" title="${escAttr(f.descripcion || f.tipo_foto || 'Foto')}">
            <img src="${escAttr(f.url)}" alt="" loading="lazy">
            ${f.tipo_foto && f.tipo_foto !== 'principal' ? `<div class="ft-gal-thumb-label">${escHtml(f.tipo_foto)}</div>` : ''}
          </div>
        `).join('')}
      </div>
    ` : ''}
    <div class="small text-muted mt-3"><i class="bi bi-info-circle me-1"></i>${fotos.length} foto${fotos.length===1?'':'s'} · click para ampliar y navegar con flechas.</div>
  `;
}

// ── Lightbox: abrir, navegar, cerrar ──
function ftLightboxAbrir(idx) {
  const fotos = window._ftFotos || [];
  if (!fotos.length) return;
  window._ftFotoIdx = Math.max(0, Math.min(idx, fotos.length - 1));
  ftLightboxRender();
  document.getElementById('ftLightbox').style.display = 'block';
  // Permitir cerrar con ESC + navegar con flechas
  document.addEventListener('keydown', _ftLightboxKey);
}
function ftLightboxCerrar() {
  document.getElementById('ftLightbox').style.display = 'none';
  document.removeEventListener('keydown', _ftLightboxKey);
}
function ftLightboxPrev() {
  const fotos = window._ftFotos || [];
  if (!fotos.length) return;
  window._ftFotoIdx = (window._ftFotoIdx - 1 + fotos.length) % fotos.length;
  ftLightboxRender();
}
function ftLightboxNext() {
  const fotos = window._ftFotos || [];
  if (!fotos.length) return;
  window._ftFotoIdx = (window._ftFotoIdx + 1) % fotos.length;
  ftLightboxRender();
}
function ftLightboxRender() {
  const fotos = window._ftFotos || [];
  const f = fotos[window._ftFotoIdx];
  if (!f) return;
  document.getElementById('ftLightboxImg').src = f.url;
  const partes = [`<strong>${escHtml(f.descripcion || 'Foto del equipo')}</strong>`];
  if (f.tipo_foto)   partes.push(escHtml(f.tipo_foto));
  if (f.fecha)       partes.push(escHtml(f.fecha));
  if (f.tomada_por)  partes.push(escHtml(f.tomada_por));
  partes.push(`${window._ftFotoIdx + 1} / ${fotos.length}`);
  document.getElementById('ftLightboxInfo').innerHTML = partes.join(' · ');
}
function _ftLightboxKey(e) {
  if (e.key === 'Escape') ftLightboxCerrar();
  else if (e.key === 'ArrowLeft') ftLightboxPrev();
  else if (e.key === 'ArrowRight') ftLightboxNext();
}

function _ftRenderSeriales(seriales) {
  const el = document.getElementById('ftTabSeriales');
  if (!seriales.length) {
    el.innerHTML = `<div class="text-center text-muted py-4">
      <i class="bi bi-shield-check" style="font-size:2rem;opacity:.3"></i>
      <div class="fw-semibold mt-2">Sin cambios de N° serie</div>
      <div class="small mt-1">Cuando se actualice el serial del equipo, los cambios aparecerán aquí con su justificación.</div>
    </div>`;
    return;
  }
  el.innerHTML = `
    <div class="table-responsive">
      <table class="table table-sm" style="font-size:.84rem">
        <thead style="background:#f9fafb"><tr>
          <th>Fecha</th><th>Antes</th><th>Después</th><th>Usuario</th><th>Razón</th>
        </tr></thead>
        <tbody>
          ${seriales.map(s => `
            <tr>
              <td class="text-muted" style="white-space:nowrap">${escHtml(s.fecha)}</td>
              <td class="font-monospace text-danger small" style="text-decoration:line-through">${escHtml(s.valor_anterior || '(vacío)')}</td>
              <td class="font-monospace text-success small fw-bold">${escHtml(s.valor_nuevo || '')}</td>
              <td class="small">${escHtml(s.usuario || '—')}</td>
              <td class="small text-muted" style="max-width:280px">${escHtml(s.razon || '')}</td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    </div>
  `;
}

function _ftRenderEstado(estados) {
  const el = document.getElementById('ftTabEstado');
  if (!estados.length) {
    el.innerHTML = `<div class="text-center text-muted py-4">
      <i class="bi bi-clipboard-check" style="font-size:2rem;opacity:.3"></i>
      <div class="fw-semibold mt-2">Sin cambios de estado</div>
      <div class="small mt-1">Cuando se cambie el estado del equipo (activo/inactivo/baja), los cambios aparecerán aquí.</div>
    </div>`;
    return;
  }
  el.innerHTML = `
    <div class="table-responsive">
      <table class="table table-sm" style="font-size:.84rem">
        <thead style="background:#f9fafb"><tr>
          <th>Fecha</th><th>Estado anterior</th><th>Nuevo estado</th><th>Usuario</th><th>Justificación</th>
        </tr></thead>
        <tbody>
          ${estados.map(s => `
            <tr>
              <td class="text-muted" style="white-space:nowrap">${escHtml(s.fecha)}</td>
              <td><span class="badge bg-secondary">${escHtml(s.estado_anterior || '—')}</span></td>
              <td><span class="badge" style="background:#dc2626;color:#fff">${escHtml(s.estado_nuevo || '—')}</span></td>
              <td class="small">${escHtml(s.usuario || '—')}</td>
              <td class="small text-muted" style="max-width:280px">${escHtml(s.razon || '')}</td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    </div>
  `;
}

// ── Auditoría / Movimientos: timeline unificado (Daniel 2026-05-27) ──
// Combina revisiones + cambios de serial + cambios de estado + fotos
// agregadas, todo en un solo timeline cronológico DESC.
function _ftRenderAuditoria(d) {
  const el = document.getElementById('ftTabAuditoria');
  if (!el) return;
  const eventos = [];

  // Revisiones del técnico
  (d.revisiones_timeline || []).forEach(r => {
    eventos.push({
      ts: r.revisado_at || r.fecha || '',
      icono: 'bi-clipboard-pulse', color: '#3b82f6',
      usuario: r.revisado_por || 'Sistema',
      accion: `Revisó equipo en ${r.numero_ot || 'OT'}`,
      detalle: r.observacion || r.razon_saltado || `Estado: ${r.estado_revision || '—'}`,
      ot: r.numero_ot || '',
    });
  });
  // Cambios de serial
  (d.historial_seriales || []).forEach(s => {
    eventos.push({
      ts: s.fecha || '',
      icono: 'bi-upc', color: '#dc2626',
      usuario: s.usuario || 'Sistema',
      accion: 'Cambió N° de serie',
      detalle: `${escHtml(s.valor_anterior || '(vacío)')} → <strong style="color:#16a34a">${escHtml(s.valor_nuevo || '—')}</strong>${s.razon ? '<br><span style="opacity:.75">Motivo: ' + escHtml(s.razon) + '</span>' : ''}`,
    });
  });
  // Cambios de estado
  (d.historial_estado || []).forEach(s => {
    eventos.push({
      ts: s.fecha || '',
      icono: 'bi-clipboard-check', color: '#f59e0b',
      usuario: s.usuario || 'Sistema',
      accion: 'Cambió estado del equipo',
      detalle: `<span class="badge bg-secondary">${escHtml(s.estado_anterior || '—')}</span> → <span class="badge" style="background:#dc2626;color:#fff">${escHtml(s.estado_nuevo || '—')}</span>${s.razon ? '<br><span style="opacity:.75">' + escHtml(s.razon) + '</span>' : ''}`,
    });
  });
  // Fotos agregadas (agrupadas por fecha+usuario para no inundar)
  const fotosGrupos = {};
  (d.fotos_galeria || []).forEach(f => {
    const key = `${(f.fecha || '').slice(0,10)}__${f.tomada_por || ''}`;
    if (!fotosGrupos[key]) {
      fotosGrupos[key] = { ts: f.fecha || '', usuario: f.tomada_por || 'Técnico', count: 0 };
    }
    fotosGrupos[key].count++;
  });
  Object.values(fotosGrupos).forEach(g => {
    eventos.push({
      ts: g.ts,
      icono: 'bi-camera-fill', color: '#16a34a',
      usuario: g.usuario,
      accion: `Adjuntó ${g.count} fotografía${g.count === 1 ? '' : 's'}`,
      detalle: '',
    });
  });

  // Ordenar DESC por timestamp
  eventos.sort((a, b) => (b.ts || '').localeCompare(a.ts || ''));

  if (!eventos.length) {
    el.innerHTML = `<div class="text-center text-muted py-5">
      <i class="bi bi-clock-history" style="font-size:3rem;opacity:.25"></i>
      <div class="fw-semibold mt-3">Sin movimientos registrados</div>
      <div class="small mt-2" style="max-width:420px;margin:0 auto;color:#94a3b8">
        Cuando un técnico revise el equipo, cambie el serial, modifique el estado o adjunte fotos, todo quedará registrado acá con fecha, usuario y motivo.
      </div>
    </div>`;
    return;
  }

  el.innerHTML = `
    <style>
      .ft-audit-tl { position:relative;padding-left:30px; }
      .ft-audit-tl::before {
        content:'';position:absolute;left:14px;top:6px;bottom:6px;
        width:2px;background:#e5e7eb;
      }
      .ft-audit-item {
        position:relative;padding:10px 14px;margin-bottom:10px;
        background:#fff;border:1px solid #e5e7eb;border-radius:10px;
      }
      .ft-audit-item::before {
        content:'';position:absolute;left:-23px;top:14px;
        width:14px;height:14px;border-radius:50%;background:#fff;
        border:3px solid currentColor;z-index:2;
      }
      .ft-audit-head {
        display:flex;align-items:center;gap:8px;flex-wrap:wrap;
        font-size:.85rem;margin-bottom:3px;
      }
      .ft-audit-user { font-weight:700;color:#0f172a; }
      .ft-audit-accion { color:#374151; }
      .ft-audit-fecha {
        font-size:.7rem;color:#94a3b8;margin-left:auto;white-space:nowrap;
      }
      .ft-audit-detalle {
        font-size:.78rem;color:#475569;line-height:1.5;
      }
    </style>
    <div class="ft-audit-tl">
      ${eventos.slice(0, 40).map(ev => `
        <div class="ft-audit-item" style="color:${ev.color}">
          <div class="ft-audit-head">
            <i class="bi ${ev.icono}" style="color:${ev.color};font-size:1rem"></i>
            <span class="ft-audit-user">${escHtml(ev.usuario)}</span>
            <span class="ft-audit-accion">${escHtml(ev.accion)}</span>
            ${ev.ot ? `<span class="badge" style="background:#f1f5f9;color:#475569;font-size:.62rem">${escHtml(ev.ot)}</span>` : ''}
            <span class="ft-audit-fecha">${escHtml(ev.ts || 'sin fecha')}</span>
          </div>
          ${ev.detalle ? `<div class="ft-audit-detalle">${ev.detalle}</div>` : ''}
        </div>
      `).join('')}
    </div>
    ${eventos.length > 40 ? `<div class="text-center text-muted small mt-3">Mostrando los 40 movimientos más recientes de ${eventos.length} totales.</div>` : ''}
  `;
}

function _ftRenderContratos(contratos) {
  const el = document.getElementById('ftTabContratos');
  if (!contratos.length) {
    el.innerHTML = `<div class="text-center text-muted py-4">
      <i class="bi bi-file-earmark" style="font-size:2rem;opacity:.3"></i>
      <div class="fw-semibold mt-2">Sin contratos vigentes</div>
      <div class="small mt-1">El cliente no tiene contratos activos en este momento.</div>
    </div>`;
    return;
  }
  el.innerHTML = contratos.map(c => {
    const dr = c.dias_restantes;
    let chipColor = '#6b7280', chipText = c.es_indefinido ? 'Indefinido' : 'Vigente';
    if (!c.es_indefinido && dr !== null && dr !== undefined) {
      if (dr < 0) { chipColor = '#dc2626'; chipText = 'Vencido'; }
      else if (dr <= 30) { chipColor = '#f59e0b'; chipText = `Vence en ${dr}d`; }
      else { chipColor = '#16a34a'; chipText = `Vigente · ${dr}d`; }
    }
    return `
      <div class="border rounded p-3 mb-2 d-flex justify-content-between align-items-start gap-2 flex-wrap" style="background:#fafafa">
        <div style="flex:1;min-width:200px">
          <div class="fw-bold">${escHtml(c.nombre || `Contrato #${c.id}`)}</div>
          <div class="small text-muted mt-1">
            <i class="bi bi-calendar3 me-1"></i>${escHtml(c.fecha_inicio || '—')} → ${escHtml(c.fecha_vencimiento || (c.es_indefinido ? 'Indefinido' : '—'))}
          </div>
        </div>
        <span class="badge" style="background:${chipColor}15;color:${chipColor};border:1px solid ${chipColor}50;padding:6px 10px">${escHtml(chipText)}</span>
      </div>
    `;
  }).join('');
}

// ── Edición inline ────────────────────────────────────────────────────
function ftAbrirEditar() {
  const eq = _ftCurrentData ? _ftCurrentData.equipo : null;
  if (!eq) return;
  document.getElementById('ft_edit_serie').value = eq.serie_actual || eq.serie || '';
  document.getElementById('ft_edit_estado').value = (eq.estado || 'activo').toLowerCase();
  document.getElementById('ft_edit_estado_op').value = (eq.estado_op || 'operativo').toLowerCase();
  document.getElementById('ft_edit_ubicacion').value = eq.ubicacion_sala || '';
  document.getElementById('ft_edit_marca').value = eq.marca || '';
  document.getElementById('ft_edit_modelo').value = eq.modelo || '';
  document.getElementById('ft_edit_obs').value = eq.observaciones || '';
  document.getElementById('ft_edit_motivo').value = '';
  document.getElementById('ft_edit_panel').style.display = 'block';
  // Scroll al panel
  setTimeout(() => {
    document.getElementById('ft_edit_panel').scrollIntoView({behavior:'smooth', block:'nearest'});
  }, 100);
}

function ftCancelarEditar() {
  document.getElementById('ft_edit_panel').style.display = 'none';
}

async function ftGuardarEditar() {
  if (!_ftCurrentMid) return;
  const eq = _ftCurrentData ? _ftCurrentData.equipo : {};
  const payload = {
    serie: document.getElementById('ft_edit_serie').value.trim(),
    estado: document.getElementById('ft_edit_estado').value,
    estado_op: document.getElementById('ft_edit_estado_op').value,
    ubicacion_sala: document.getElementById('ft_edit_ubicacion').value.trim(),
    marca: document.getElementById('ft_edit_marca').value.trim(),
    modelo: document.getElementById('ft_edit_modelo').value.trim(),
    observaciones: document.getElementById('ft_edit_obs').value.trim(),
    motivo: document.getElementById('ft_edit_motivo').value.trim(),
  };
  // Validación local: si cambia serial o estado, exigir motivo
  const cambia_serial = (payload.serie || '') !== (eq.serie_actual || eq.serie || '');
  const cambia_estado = (payload.estado || '') !== ((eq.estado || 'activo').toLowerCase());
  if ((cambia_serial || cambia_estado) && payload.motivo.length < 5) {
    await ilusAlert({
      title: 'Motivo requerido',
      message: 'Para cambiar el N° serie o el estado del equipo necesitas ingresar un motivo de al menos 5 caracteres.',
      type: 'warning',
    });
    document.getElementById('ft_edit_motivo').focus();
    return;
  }
  try {
    const r = await fetch(`/mantenciones/api/maquinas/${_ftCurrentMid}`, {
      method: 'PATCH',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(payload),
    });
    const d = await r.json();
    if (!d.ok) {
      await ilusAlert({
        title: 'No se pudo guardar',
        message: d.error || 'Error desconocido',
        type: 'error',
      });
      return;
    }
    ilusToast('Equipo actualizado correctamente', { type: 'success' });
    // Refrescar el modal con los datos nuevos
    document.getElementById('ft_edit_panel').style.display = 'none';
    if (_ftCurrentMid) await verFichaTecnicaEquipo(_ftCurrentMid, eq.nombre);
    // Actualizar la fila en la tabla (al menos el serial)
    if (cambia_serial) {
      const span = document.getElementById('serie-' + _ftCurrentMid);
      if (span) span.textContent = payload.serie || '—';
    }
  } catch(e) {
    await ilusAlert({
      title: 'Error de red',
      message: e.message || 'No se pudo contactar al servidor.',
      type: 'error',
    });
  }
}


// ════════════════════════════════════════════════════════════════════
// BUSCADOR ERP en modal "Agregar equipo manual"
// Autocomplete por nombre o SKU desde catálogo ERP
// ════════════════════════════════════════════════════════════════════
let _mmErpTimer = null;
let _mmErpResults = [];
let _mmErpIdx = -1;

function mmErpDebounce() {
  clearTimeout(_mmErpTimer);
  const q = document.getElementById('mm_erp_search').value.trim();
  const drop = document.getElementById('mm_erp_drop');
  if (q.length < 2) { drop.style.display = 'none'; return; }
  drop.innerHTML = '<div class="ac-loading"><span class="spinner-border spinner-border-sm me-1"></span>Buscando en ERP…</div>';
  drop.style.display = 'block';
  _mmErpTimer = setTimeout(mmErpFetch, 300);
}

async function mmErpFetch() {
  const q = document.getElementById('mm_erp_search').value.trim();
  if (q.length < 2) return;
  try {
    const r = await fetch(`/mantenciones/api/productos/buscar?q=${encodeURIComponent(q)}`);
    _mmErpResults = await r.json();
    _mmErpIdx = -1;
    mmErpRender();
  } catch(e) {
    document.getElementById('mm_erp_drop').innerHTML =
      '<div class="ac-loading text-muted small">Error al buscar</div>';
  }
}

function mmErpRender() {
  const drop = document.getElementById('mm_erp_drop');
  if (!_mmErpResults.length) {
    drop.innerHTML = '<div class="ac-loading text-muted small">Sin resultados — completa los datos manualmente abajo</div>';
    return;
  }
  drop.innerHTML = _mmErpResults.map((p, i) => `
    <div class="ac-item${i===_mmErpIdx?' active':''}" onclick="mmErpSeleccionar(${i})">
      <div class="d-flex align-items-start gap-2">
        <i class="bi bi-box-seam text-primary mt-1" style="font-size:.8rem"></i>
        <div style="flex:1;min-width:0">
          <div class="ac-name" style="font-size:.83rem">${escHtml(p.nombre || '—')}</div>
          <div class="ac-rut">
            <span class="font-monospace" style="color:#0066cc;font-weight:600">${escHtml(p.sku || '')}</span>
            ${p.tipo ? ` · ${escHtml(p.tipo)}` : ''}
          </div>
        </div>
      </div>
    </div>
  `).join('');
  drop.style.display = 'block';
}

function mmErpSeleccionar(i) {
  const p = _mmErpResults[i];
  if (!p) return;
  document.getElementById('mm_nombre').value = p.nombre || '';
  document.getElementById('mm_sku').value    = p.sku || '';
  document.getElementById('mm_erp_search').value = '';
  document.getElementById('mm_erp_drop').style.display = 'none';
  // Foco en N° serie para que el usuario complete lo siguiente
  document.getElementById('mm_serie')?.focus();
}

function mmErpKeydown(e) {
  const drop = document.getElementById('mm_erp_drop');
  const items = drop.querySelectorAll('.ac-item');
  if (!items.length) return;
  if (e.key === 'ArrowDown') { _mmErpIdx = Math.min(_mmErpIdx+1, items.length-1); mmErpRender(); e.preventDefault(); }
  else if (e.key === 'ArrowUp') { _mmErpIdx = Math.max(_mmErpIdx-1, -1); mmErpRender(); e.preventDefault(); }
  else if (e.key === 'Enter' && _mmErpIdx >= 0) { mmErpSeleccionar(_mmErpIdx); e.preventDefault(); }
  else if (e.key === 'Escape') drop.style.display = 'none';
}

document.addEventListener('click', e => {
  if (!document.getElementById('mm_erp_search')?.contains(e.target)) {
    const dd = document.getElementById('mm_erp_drop');
    if (dd) dd.style.display = 'none';
  }
});

async function guardarCliente() {
  const $v = id => (document.getElementById(id)?.value || '').trim();
  const data = {
    // Empresa
    razon_social:      $v('ec_razon'),
    rut:               $v('ec_rut'),
    estado:            document.getElementById('ec_estado').value,
    tipo_cliente:      (document.getElementById('ec_tipo_cliente')?.value || 'mantencion'),
    giro:              $v('ec_giro'),
    email_empresa:     $v('ec_email_empresa'),
    tel_empresa:       $v('ec_tel_empresa'),
    // Ubicación
    direccion:          $v('ec_direccion'),
    direccion_lat:      $v('ec_direccion_lat'),
    direccion_lng:      $v('ec_direccion_lng'),
    direccion_place_id: $v('ec_direccion_place_id'),
    region:             $v('ec_region'),
    comuna:             $v('ec_comuna'),
    ciudad:             $v('ec_ciudad'),
    // Contacto principal
    contacto_nombre:   $v('ec_contacto_nombre'),
    contacto_cargo:    $v('ec_contacto_cargo'),
    contacto_tel:      $v('ec_contacto_tel'),
    contacto_email:    $v('ec_contacto_email'),
    // Contacto secundario
    contacto2_nombre:  $v('ec_contacto2_nombre'),
    contacto2_cargo:   $v('ec_contacto2_cargo'),
    contacto2_tel:     $v('ec_contacto2_tel'),
    contacto2_email:   $v('ec_contacto2_email'),
    // Notas
    notas:             $v('ec_notas'),
  };
  if (!data.razon_social) {
    if (typeof ilusAlert === 'function') ilusAlert({type:'error', message:'Razón social requerida'});
    return;
  }
  const r = await fetch(`/mantenciones/api/clientes/${CID}`, {
    method: 'PUT',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify(data)
  });
  if (r.ok) { location.reload(); }
  else {
    if (typeof ilusAlert === 'function') ilusAlert({type:'error', message:'Error al guardar'});
  }
}

// ─── ERP — tabs ──────────────────────────────────────────────
function fSetTab(tab) {
  document.getElementById('fTabDocBtn').classList.toggle('active', tab==='doc');
  document.getElementById('fTabRutBtn').classList.toggle('active', tab==='rut');
  document.getElementById('fPanelDoc').style.display = tab==='doc' ? '' : 'none';
  document.getElementById('fPanelRut').style.display = tab==='rut' ? '' : 'none';
}

// ─── Confirmar importación con RUT distinto al de la ficha ──────────
async function confirmarErpMismatch() {
  const ta = document.getElementById('erpMismatchMotivo');
  if (!ta) return;
  const motivo = (ta.value || '').trim();
  if (motivo.length < 8) {
    alert('Escribe un motivo de al menos 8 caracteres para auditoría.');
    ta.focus();
    return;
  }
  if (!window._erpMismatch) return;
  // Loggear el motivo en el backend (mant_logs)
  try {
    const r = await fetch(`/mantenciones/api/clientes/${DATA.cid}/equipos-import-mismatch`, {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({
        motivo:  motivo,
        rut_doc: window._erpMismatch.rutDoc,
        tido:    window._erpMismatch.tido,
        nudo:    window._erpMismatch.nudo,
      })
    });
    const d = await r.json();
    if (!d.ok) { alert('No se pudo registrar el motivo: ' + (d.error||'')); return; }
  } catch(e) {
    alert('Error de red al registrar motivo. Intenta de nuevo.');
    return;
  }
  window._erpMismatch.confirmado = true;
  window._erpMismatch.motivo     = motivo;
  // Reemplazar la alerta roja por confirmación verde
  const alerta = ta.closest('.alert');
  if (alerta) {
    alerta.className = 'alert alert-success d-flex align-items-start gap-2 mt-2 mb-3';
    alerta.style = 'border-left:5px solid #16a34a';
    alerta.innerHTML = '<i class="bi bi-check-circle-fill text-success mt-1"></i>' +
      '<div><strong>Importación autorizada y registrada</strong><br>' +
      '<span class="small">Motivo: ' + motivo.replace(/[<>&"]/g,'') + '</span></div>';
  }
}

// ─── ERP — buscar por documento (REST API) ────────────────────
let erpSeleccionadas = {};

// fBuscarDoc — flujo unificado: consulta el doc, cierra el modal de búsqueda
// y abre el modal LINDO (modalErpProductos) que ya usa "Por RUT/cliente".
// Así reciclamos el mismo flow del wizard: checkboxes confiables, saldo,
// elección por producto de "1 ficha vs N fichas", N° serie auto.
async function fBuscarDoc() {
  const tido = document.getElementById('fDocTido').value;
  const nudo = document.getElementById('fDocNudo').value.trim();
  const cont = document.getElementById('fDocRes');
  if (!nudo) {
    cont.innerHTML = '<div class="text-center text-warning py-3">Ingresa el número de documento</div>';
    return;
  }
  cont.innerHTML = `<div class="text-center py-4"><span class="spinner-border spinner-border-sm me-2"></span>Consultando ERP — ${tido} ${nudo}…</div>`;

  try {
    const r = await fetch(`/mantenciones/api/documento?tido=${tido}&nudo=${encodeURIComponent(nudo)}`);
    const data = await r.json();

    if (!data.ok) {
      cont.innerHTML = `<div class="alert alert-warning"><i class="bi bi-exclamation-triangle me-1"></i>${data.error || 'Documento no encontrado'}</div>`;
      return;
    }
    if (!data.items?.length) {
      let noItemsMsg = 'Este documento no tiene productos importables.';
      if (data.total_lineas > 0) noItemsMsg += ` (${data.total_lineas} líneas en el ERP, todas son servicios/fletes)`;
      cont.innerHTML = `<div class="text-center text-muted py-3">${noItemsMsg}</div>`;
      return;
    }

    // Construir un objeto "doc" compatible con abrirModalProductos()
    const tido_display = (tido === 'VD' || tido === 'WEB') ? tido : tido;
    const nudo_display = nudo;
    const fecha_iso = (() => {
      const p = (data.fecha || '').split('/');
      return p.length === 3 ? `${p[2]}-${p[1]}-${p[0]}` : '';
    })();
    const docCompat = {
      tido,
      nudo,
      tido_display,
      nudo_display,
      rut: data.rut || '',
      razon_social: data.cliente || '',
      fecha: data.fecha || '',
      fecha_iso,
      valor_total: 0
    };

    // Cerrar modal de búsqueda y abrir el modal lindo con productos pre-cargados
    bootstrap.Modal.getInstance(document.getElementById('modalErp'))?.hide();
    setTimeout(() => abrirModalProductosConDatos(docCompat, data.items), 250);

  } catch(e) {
    cont.innerHTML = `<div class="alert alert-danger">Error de conexión: ${e.message}</div>`;
  }
}

// Variante de abrirModalProductos que recibe los items ya cargados (sin re-fetch).
async function abrirModalProductosConDatos(doc, items) {
  _epDocActual = doc;
  _epProductos = items;
  _epSeleccion = new Set();
  if (!_epModal) _epModal = new bootstrap.Modal(document.getElementById('modalErpProductos'));

  document.getElementById('ep_docTitulo').textContent = `${doc.tido_display} ${doc.nudo_display}`;
  document.getElementById('ep_cliente').textContent = doc.razon_social || '—';
  document.getElementById('ep_rut').textContent = doc.rut || '—';
  document.getElementById('ep_fecha').textContent = doc.fecha || '—';
  document.getElementById('ep_total').textContent = '—';
  document.getElementById('ep_chkAll').checked = false;
  document.getElementById('ep_chkExpandir').checked = false;
  document.getElementById('ep_count').textContent = '0';
  document.getElementById('ep_total_items').textContent = items.length;
  document.getElementById('ep_btnImportar').disabled = true;
  document.getElementById('ep_btnCount').textContent = '0';

  // Validación de mismatch RUT — usar ilusRutsMatch que tolera DV
  // presente/ausente (ver static/ilus_ui.js).
  const noCoincide = !ilusRutsMatch(doc.rut, RUT_FICHA);
  const alertEl = document.getElementById('ep_mismatchAlert');
  if (noCoincide) {
    window._erpMismatch = {
      rutDoc: doc.rut, clienteDoc: doc.razon_social || doc.rut,
      tido: doc.tido, nudo: doc.nudo, confirmado: false, motivo: ''
    };
    alertEl.style.display = '';
    alertEl.innerHTML = `<div class="alert alert-danger small mb-0" style="border-left:4px solid #dc2626">
      <strong><i class="bi bi-exclamation-octagon-fill me-1"></i>RUT distinto al de la ficha</strong><br>
      Documento: ${escHtml(doc.razon_social)} (${escHtml(doc.rut)})<br>
      Ficha: ${escHtml(RAZON_FICHA)} (${escHtml(RUT_FICHA)})<br>
      <textarea id="ep_motivo" class="form-control form-control-sm mt-2" rows="2" minlength="8"
                placeholder="Motivo justificado (mínimo 8 caracteres)..."></textarea>
      <button class="btn btn-sm btn-warning mt-2" onclick="epConfirmarMismatch()">
        <i class="bi bi-check-lg me-1"></i>Confirmar y registrar motivo
      </button>
    </div>`;
  } else {
    alertEl.style.display = 'none';
    window._erpMismatch = null;
  }

  epRenderTabla();
  _epModal.show();
}

// ════════════════════════════════════════════════════════════════════════
// ERP — Búsqueda inteligente vía SQL Server directo (Random)
// ════════════════════════════════════════════════════════════════════════
const RUT_FICHA = DATA.cliente_rut;
const RAZON_FICHA = DATA.cliente_razon_social;
let _epModal = null;
let _epDocActual = null;   // {tido, nudo, fecha, cliente, rut}
let _epProductos = [];     // productos del documento abierto
let _epSeleccion = new Set(); // índices seleccionados

async function buscarErpSql() {
  const q = document.getElementById('erpQ').value.trim();
  if (q.length < 3) {
    document.getElementById('erpResultado').innerHTML =
      '<div class="alert alert-warning small mb-0">Mínimo 3 caracteres</div>';
    return;
  }
  const cont = document.getElementById('erpResultado');
  cont.innerHTML = '<div class="text-center text-muted py-4"><span class="spinner-border spinner-border-sm me-2"></span>Buscando en Random ERP…</div>';

  let data;
  try {
    const r = await fetch('/mantenciones/api/buscar-erp-sql', {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({q})
    });
    data = await r.json();
  } catch(e) {
    cont.innerHTML = `<div class="alert alert-danger small">Error de red: ${e.message}</div>`;
    return;
  }

  if (data.sin_conexion) {
    cont.innerHTML = `<div class="alert alert-warning">
      <i class="bi bi-plug me-1"></i><strong>ERP no conectado.</strong> ${data.error||''}
      <br><small class="text-muted">Pídele al admin que setee las variables RANDOM_SQL_* en Railway.</small>
    </div>`;
    return;
  }
  if (data.error) {
    cont.innerHTML = `<div class="alert alert-warning small">${data.error}</div>`;
    return;
  }
  if (!data.documentos?.length) {
    cont.innerHTML = `<div class="text-center text-muted py-4" style="font-size:.85rem">
      <i class="bi bi-search" style="font-size:1.6rem;opacity:.3;display:block;margin-bottom:8px"></i>
      Sin resultados para "${escHtml(q)}"
    </div>`;
    return;
  }

  // Guardar documentos y renderizar con filtro por tipo (FCV/GDV/VD/COV…)
  _erpDocsAll = data.documentos;
  _erpModoLbl = {rut:'RUT', numero:'Número doc.', nombre:'Nombre'}[data.modo] || '';
  _erpRenderDocs();
}

// Documentos ERP encontrados — guardados para filtrar por tipo sin re-buscar.
let _erpDocsAll = [];
let _erpModoLbl = '';

function _erpRenderDocs() {
  const cont = document.getElementById('erpResultado');
  if (!cont) return;
  const sel = document.getElementById('erpFiltroTipo');
  const filtro = sel ? sel.value : '';
  const docs = filtro ? _erpDocsAll.filter(d => (d.tido_display || '') === filtro) : _erpDocsAll;
  // Tipos distintos presentes en los resultados (para el dropdown), con conteo.
  const tipos = [...new Set(_erpDocsAll.map(d => d.tido_display || '').filter(Boolean))].sort();
  let opts = `<option value="">Todos (${_erpDocsAll.length})</option>`;
  tipos.forEach(t => {
    const n = _erpDocsAll.filter(d => (d.tido_display || '') === t).length;
    opts += `<option value="${escHtml(t)}"${t === filtro ? ' selected' : ''}>${escHtml(t)} (${n})</option>`;
  });
  let html = `<div class="d-flex justify-content-between align-items-center mb-2 gap-2 flex-wrap">
    <span class="small text-muted"><strong>${docs.length}</strong> de ${_erpDocsAll.length} documento(s) por <strong>${escHtml(_erpModoLbl)}</strong></span>
    <div class="d-flex align-items-center gap-1">
      <label class="mb-0 text-muted fw-semibold" style="font-size:.75rem"><i class="bi bi-funnel me-1"></i>Tipo:</label>
      <select id="erpFiltroTipo" class="form-select form-select-sm" style="width:auto;font-size:.78rem" onchange="_erpRenderDocs()">${opts}</select>
    </div>
  </div>
  <div class="table-responsive" style="max-height:360px;overflow-y:auto;border:1px solid #e5e7eb;border-radius:8px">
  <table class="table table-sm table-hover mb-0" style="font-size:.82rem">
    <thead class="sticky-top" style="background:#f9fafb;top:0">
      <tr>
        <th style="width:70px">Tipo</th>
        <th style="width:120px">Número</th>
        <th>Cliente</th>
        <th style="width:100px">Fecha</th>
        <th class="text-end" style="width:120px">Total</th>
        <th style="width:130px"></th>
      </tr>
    </thead>
    <tbody>`;
  if (!docs.length) {
    html += `<tr><td colspan="6" class="text-center text-muted py-3" style="font-size:.82rem">Sin documentos de tipo "${escHtml(filtro)}"</td></tr>`;
  }
  docs.forEach((d) => {
    // FIX 2026-05-19: tolerar que uno tenga DV y el otro no.
    const rutMatch = ilusRutsMatch(d.rut, RUT_FICHA);
    const tidoBadge = `<span class="badge bg-secondary" style="font-size:.62rem;font-family:monospace">${escHtml(d.tido_display)}</span>`;
    const total = d.valor_total ? '$' + Math.round(d.valor_total).toLocaleString('es-CL') : '—';
    html += `<tr>
      <td>${tidoBadge}</td>
      <td class="font-monospace">${escHtml(d.nudo_display)}</td>
      <td style="max-width:280px" class="text-truncate" title="${escHtml(d.razon_social)} (${escHtml(d.rut)})">
        ${escHtml(d.razon_social || '—')}
        ${rutMatch ? '' : '<i class="bi bi-exclamation-triangle text-warning ms-1" title="RUT distinto al de la ficha"></i>'}
      </td>
      <td class="small text-muted">${escHtml(d.fecha)}</td>
      <td class="text-end small">${total}</td>
      <td>
        <button class="btn btn-sm btn-ilus w-100" onclick='abrirModalProductos(${JSON.stringify(d).replace(/'/g, "&#39;")})'>
          <i class="bi bi-eye me-1"></i>Ver productos
        </button>
      </td>
    </tr>`;
  });
  html += '</tbody></table></div>';
  cont.innerHTML = html;
}

function escHtml(s) {
  return String(s||'').replace(/[&<>"']/g, c =>
    ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
}

// ─── Modal: Productos del documento ───────────────────────────
async function abrirModalProductos(doc) {
  _epDocActual = doc;
  _epProductos = [];
  _epSeleccion = new Set();
  if (!_epModal) _epModal = new bootstrap.Modal(document.getElementById('modalErpProductos'));

  document.getElementById('ep_docTitulo').textContent = `${doc.tido_display} ${doc.nudo_display}`;
  document.getElementById('ep_cliente').textContent = doc.razon_social || '—';
  document.getElementById('ep_rut').textContent = doc.rut || '—';
  document.getElementById('ep_fecha').textContent = doc.fecha || '—';
  document.getElementById('ep_total').textContent = doc.valor_total
    ? '$' + Math.round(doc.valor_total).toLocaleString('es-CL') : '—';
  document.getElementById('ep_chkAll').checked = false;
  document.getElementById('ep_chkExpandir').checked = false;
  document.getElementById('ep_count').textContent = '0';
  document.getElementById('ep_total_items').textContent = '…';
  document.getElementById('ep_btnImportar').disabled = true;
  document.getElementById('ep_btnCount').textContent = '0';

  // Validar RUT mismatch — tolerante a DV presente/ausente.
  // FIX 2026-05-19: antes hacíamos slice(0,-1) en AMBOS, lo que cortaba
  // un dígito real cuando uno no traía DV (ej: doc "78129118" vs
  // ficha "78.129.118-8" → comparaba "7812911" vs "78129118" → distinto).
  const noCoincide = !ilusRutsMatch(doc.rut, RUT_FICHA);
  const alertEl = document.getElementById('ep_mismatchAlert');
  if (noCoincide) {
    window._erpMismatch = { rutDoc:doc.rut, clienteDoc:doc.razon_social, tido:doc.tido, nudo:doc.nudo, confirmado:false, motivo:'' };
    alertEl.style.display = '';
    alertEl.innerHTML = `<div class="alert alert-danger small mb-0" style="border-left:4px solid #dc2626">
      <strong><i class="bi bi-exclamation-octagon-fill me-1"></i>RUT distinto al de la ficha</strong><br>
      Documento: ${escHtml(doc.razon_social)} (${escHtml(doc.rut)})<br>
      Ficha: ${escHtml(RAZON_FICHA)} (${escHtml(RUT_FICHA)})<br>
      <textarea id="ep_motivo" class="form-control form-control-sm mt-2" rows="2" minlength="8"
                placeholder="Motivo justificado (mínimo 8 caracteres)..."></textarea>
      <button class="btn btn-sm btn-warning mt-2" onclick="epConfirmarMismatch()">
        <i class="bi bi-check-lg me-1"></i>Confirmar y registrar motivo
      </button>
    </div>`;
  } else {
    alertEl.style.display = 'none';
    window._erpMismatch = null;
  }

  document.getElementById('ep_tabla').innerHTML =
    '<div class="text-center text-muted py-5"><span class="spinner-border spinner-border-sm me-2"></span>Cargando productos del ERP…</div>';
  _epModal.show();

  // Fetch productos del documento (vía REST API que ya existe)
  try {
    const r = await fetch(`/mantenciones/api/documento?tido=${encodeURIComponent(doc.tido)}&nudo=${encodeURIComponent(doc.nudo)}`);
    const data = await r.json();
    if (!data.ok || !data.items?.length) {
      document.getElementById('ep_tabla').innerHTML =
        '<div class="text-center text-muted py-4">Este documento no tiene productos importables.</div>';
      return;
    }
    _epProductos = data.items;
    document.getElementById('ep_total_items').textContent = _epProductos.length;
    epRenderTabla();
  } catch(e) {
    document.getElementById('ep_tabla').innerHTML =
      `<div class="alert alert-danger m-3 small">Error: ${escHtml(e.message)}</div>`;
  }
}

function epRenderTabla() {
  // Calcular saldo: cuántas unidades de cada SKU YA están en mant_maquinas con
  // doc_origen del documento actual. Lo leemos del DOM (lista actual de equipos).
  const docOrigKey = `${_epDocActual.tido_display} ${_epDocActual.nudo_display}`;
  const saldoPorSku = {};
  // Las máquinas existentes están en la tabla de equipos del cliente (#maqListado).
  const filas = document.querySelectorAll('#maqListado tr[data-doc-origen]');
  filas.forEach(tr => {
    if (tr.dataset.docOrigen === docOrigKey) {
      const sku = tr.dataset.sku || '';
      const qty = parseInt(tr.dataset.cantidad || '1') || 1;
      saldoPorSku[sku] = (saldoPorSku[sku] || 0) + qty;
    }
  });

  let html = `<table class="table table-hover mb-0" style="font-size:.85rem">
    <thead style="background:#f9fafb">
      <tr>
        <th style="width:50px"></th>
        <th>Producto</th>
        <th style="width:140px">SKU del modelo</th>
        <th style="width:130px" class="text-center">Saldo</th>
        <th style="width:170px" class="text-center" title="Si tiene cantidad &gt; 1, ¿crear N fichas individuales con N° serie único?">Crear fichas</th>
      </tr>
    </thead><tbody>`;
  _epProductos.forEach((p, i) => {
    const qty = parseInt(p.cantidad) || 1;
    const yaImp = saldoPorSku[p.sku || ''] || 0;
    const saldo = Math.max(0, qty - yaImp);
    const completo = saldo === 0 && yaImp > 0;
    p._saldo    = saldo;
    p._completo = completo;
    p._cantTotal = qty;

    const checked = _epSeleccion.has(i) ? 'checked' : '';
    const chkAttrs = completo ? 'disabled' : checked;
    const trStyle = completo ? 'opacity:.55;background:#f8f9fa' : '';

    const saldoCell = (yaImp > 0)
      ? (completo
          ? `<span class="badge bg-success" title="Las ${qty} unidades ya están en la ficha">
               <i class="bi bi-check-circle-fill me-1"></i>Completo
             </span>`
          : `<span class="badge bg-warning text-dark" title="${yaImp} ya en la ficha de ${qty} totales">
               ${yaImp}/${qty} · saldo ${saldo}
             </span>`)
      : `<span class="text-muted small">${qty}</span>`;

    const opciones = (saldo > 1)
      ? `<select class="form-select form-select-sm" data-ep-fichas-idx="${i}" style="font-size:.72rem">
            <option value="1" selected>1 ficha (cant. ${saldo})</option>
            <option value="${saldo}">${saldo} fichas individuales</option>
         </select>`
      : (completo ? '<span class="text-muted small">—</span>' : '<span class="text-muted small">1 ficha</span>');

    html += `<tr style="${trStyle}">
      <td><input type="checkbox" class="form-check-input ep-chk" data-idx="${i}" ${chkAttrs}
                 onchange="epToggleItem(${i}, this.checked)"></td>
      <td>${escHtml(p.nombre)}</td>
      <td class="font-monospace small">${escHtml(p.sku||'—')}</td>
      <td class="text-center">${saldoCell}</td>
      <td class="text-center">${opciones}</td>
    </tr>`;
  });
  html += '</tbody></table>';
  document.getElementById('ep_tabla').innerHTML = html;
}

function epToggleItem(idx, checked) {
  if (checked) _epSeleccion.add(idx);
  else _epSeleccion.delete(idx);
  epActualizarContador();
}

function epToggleAll(checked) {
  _epSeleccion.clear();
  if (checked) _epProductos.forEach((_, i) => _epSeleccion.add(i));
  document.querySelectorAll('.ep-chk').forEach(c => c.checked = checked);
  epActualizarContador();
}

function epActualizarContador() {
  const n = _epSeleccion.size;
  document.getElementById('ep_count').textContent = n;
  document.getElementById('ep_btnCount').textContent = n;
  document.getElementById('ep_btnImportar').disabled = n === 0;
}

async function epConfirmarMismatch() {
  const motivo = (document.getElementById('ep_motivo')?.value || '').trim();
  if (motivo.length < 8) { alert('Mínimo 8 caracteres en el motivo.'); return; }
  if (!window._erpMismatch) return;
  try {
    const r = await fetch(`/mantenciones/api/clientes/${DATA.cid}/equipos-import-mismatch`, {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({ motivo, rut_doc:window._erpMismatch.rutDoc, tido:window._erpMismatch.tido, nudo:window._erpMismatch.nudo })
    });
    const d = await r.json();
    if (!d.ok) { alert('Error: ' + (d.error||'')); return; }
    window._erpMismatch.confirmado = true;
    window._erpMismatch.motivo = motivo;
    document.getElementById('ep_mismatchAlert').innerHTML =
      `<div class="alert alert-success small mb-0"><i class="bi bi-check-circle-fill me-1"></i>Importación autorizada. Motivo registrado.</div>`;
  } catch(e) { alert('Error de red'); }
}

// ─── Modal de progreso dinámico (operaciones lentas: import ERP, etc.) ───────
// Reutilizable: ilusProgreso.open({titulo,subtitulo,paso}) → avanzar(hechos,total,
// etiqueta) → exito(msg) / error(msg) → cerrar(). Mucho movimiento visual para
// que la espera se sienta corta: barra animada, % que sube, frases rotativas,
// engranaje girando y chispas. NO usa innerHTML con datos del usuario (textContent).
const ilusProgreso = (function () {
  let modal = null, raf = null, frasesTimer = null, fraseI = 0, pctActual = 0, pctObjetivo = 0;
  const FRASES = [
    '🔌 Sincronizando con el ERP…',
    '🧬 Generando series únicas por equipo…',
    '🗂️ Creando fichas técnicas…',
    '📊 Cuadrando saldos del documento…',
    '🏷️ Aplicando catálogo ILUS Fitness…',
    '🔗 Enlazando equipos a la ficha…',
    '✨ Puliendo los últimos detalles…',
  ];
  function inject() {
    if (document.getElementById('ilusProgModal')) return;
    if (!document.getElementById('ilusProgCss')) {
      const st = document.createElement('style'); st.id = 'ilusProgCss';
      st.textContent =
        '@keyframes ipgStripes{from{background-position:0 0}to{background-position:44px 0}}' +
        '@keyframes ipgSpin{to{transform:rotate(360deg)}}' +
        '@keyframes ipgPop{0%{transform:scale(.4);opacity:0}60%{transform:scale(1.15)}100%{transform:scale(1);opacity:1}}' +
        '@keyframes ipgShim{0%{transform:translateX(-120%)}100%{transform:translateX(240%)}}' +
        '@keyframes ipgFloat{0%{transform:translateY(0);opacity:0}25%{opacity:.95}100%{transform:translateY(-52px);opacity:0}}' +
        '#ilusProgModal .ipg-spark{position:absolute;bottom:6px;width:6px;height:6px;border-radius:50%;animation:ipgFloat 2.3s linear infinite}';
      document.head.appendChild(st);
    }
    const html =
      '<div class="modal fade" id="ilusProgModal" tabindex="-1" data-bs-backdrop="static" data-bs-keyboard="false" aria-hidden="true">' +
      '<div class="modal-dialog modal-dialog-centered" style="max-width:430px">' +
      '<div class="modal-content" style="border:none;border-radius:20px;overflow:hidden;background:linear-gradient(160deg,#15151f,#0a0a0f);color:#fff;box-shadow:0 24px 70px rgba(0,0,0,.55)">' +
      '<div style="position:relative;padding:26px 26px 6px;text-align:center;overflow:hidden">' +
      '<div id="ipgSparks" style="position:absolute;inset:0;pointer-events:none"></div>' +
      '<div id="ipgIcon" style="width:74px;height:74px;margin:0 auto 14px;border-radius:18px;background:linear-gradient(135deg,#dc2626,#7f1d1d);display:flex;align-items:center;justify-content:center;box-shadow:0 8px 26px rgba(220,38,38,.5)">' +
      '<i class="bi bi-gear-wide-connected" style="font-size:2.1rem;animation:ipgSpin 2.4s linear infinite"></i></div>' +
      '<div id="ipgTitulo" style="font-size:1.12rem;font-weight:800;letter-spacing:.3px">Procesando…</div>' +
      '<div id="ipgSub" style="font-size:.82rem;color:#9ca3af;margin-top:3px"></div></div>' +
      '<div style="padding:6px 26px 24px">' +
      '<div style="display:flex;justify-content:space-between;align-items:flex-end;margin-bottom:7px">' +
      '<span id="ipgFrase" style="font-size:.8rem;color:#e5e7eb;font-weight:600;transition:opacity .2s">Iniciando…</span>' +
      '<span id="ipgPct" style="font-size:1.5rem;font-weight:900;color:#f87171;line-height:1">0%</span></div>' +
      '<div style="height:13px;border-radius:50px;background:#26262f;overflow:hidden;position:relative">' +
      '<div id="ipgBar" style="height:100%;width:0%;border-radius:50px;background:#dc2626;background-image:linear-gradient(45deg,rgba(255,255,255,.18) 25%,transparent 25%,transparent 50%,rgba(255,255,255,.18) 50%,rgba(255,255,255,.18) 75%,transparent 75%),linear-gradient(90deg,#dc2626,#f87171);background-size:44px 44px,100% 100%;transition:width .5s cubic-bezier(.22,1,.36,1);position:relative;animation:ipgStripes .9s linear infinite">' +
      '<div style="position:absolute;top:0;left:0;height:100%;width:70px;background:linear-gradient(90deg,transparent,rgba(255,255,255,.5),transparent);animation:ipgShim 1.4s ease-in-out infinite"></div></div></div>' +
      '<div id="ipgPaso" style="margin-top:14px;font-size:.84rem;color:#cbd5e1;min-height:20px;display:flex;align-items:center;gap:8px">' +
      '<span class="spinner-border spinner-border-sm" style="color:#f87171"></span><span id="ipgPasoTxt">Preparando…</span></div>' +
      '<div id="ipgContador" style="margin-top:6px;font-size:.74rem;color:#6b7280"></div>' +
      '</div></div></div></div>';
    document.body.insertAdjacentHTML('beforeend', html);
    const sp = document.getElementById('ipgSparks');
    for (let i = 0; i < 9; i++) {
      const s = document.createElement('span');
      s.className = 'ipg-spark';
      s.style.left = (8 + i * 11) + '%';
      s.style.animationDelay = (i * 0.25) + 's';
      s.style.background = (i % 2) ? '#f87171' : '#fca5a5';
      sp.appendChild(s);
    }
  }
  function animarPct() {
    cancelAnimationFrame(raf);
    const paso = () => {
      if (Math.abs(pctActual - pctObjetivo) < 0.5) pctActual = pctObjetivo;
      else pctActual += (pctObjetivo - pctActual) * 0.18;
      const v = Math.round(pctActual);
      const e = document.getElementById('ipgPct'); if (e) e.textContent = v + '%';
      const b = document.getElementById('ipgBar'); if (b) b.style.width = v + '%';
      if (pctActual !== pctObjetivo) raf = requestAnimationFrame(paso);
    };
    raf = requestAnimationFrame(paso);
  }
  return {
    open(opts) {
      opts = opts || {};
      inject();
      document.getElementById('ipgTitulo').textContent = opts.titulo || 'Procesando…';
      document.getElementById('ipgSub').textContent = opts.subtitulo || '';
      document.getElementById('ipgPaso').innerHTML =
        '<span class="spinner-border spinner-border-sm" style="color:#f87171"></span><span id="ipgPasoTxt"></span>';
      document.getElementById('ipgPasoTxt').textContent = opts.paso || 'Preparando…';
      document.getElementById('ipgContador').textContent = '';
      document.getElementById('ipgFrase').textContent = FRASES[0];
      const ic = document.getElementById('ipgIcon');
      ic.style.background = 'linear-gradient(135deg,#dc2626,#7f1d1d)';
      ic.innerHTML = '<i class="bi bi-gear-wide-connected" style="font-size:2.1rem;animation:ipgSpin 2.4s linear infinite"></i>';
      pctActual = 0; pctObjetivo = 0;
      document.getElementById('ipgBar').style.width = '0%';
      document.getElementById('ipgPct').textContent = '0%';
      if (!modal) modal = new bootstrap.Modal(document.getElementById('ilusProgModal'));
      modal.show();
      clearInterval(frasesTimer); fraseI = 0;
      frasesTimer = setInterval(() => {
        fraseI = (fraseI + 1) % FRASES.length;
        const f = document.getElementById('ipgFrase');
        if (f) { f.style.opacity = '0'; setTimeout(() => { f.textContent = FRASES[fraseI]; f.style.opacity = '1'; }, 190); }
      }, 1700);
    },
    avanzar(hechos, total, etiqueta) {
      pctObjetivo = total > 0 ? Math.min(100, Math.round(hechos / total * 100)) : 0;
      animarPct();
      if (etiqueta) document.getElementById('ipgPasoTxt').textContent = etiqueta;
      const c = document.getElementById('ipgContador');
      if (c) c.textContent = total ? (hechos + ' de ' + total + ' completados') : '';
    },
    exito(mensaje) {
      clearInterval(frasesTimer);
      pctObjetivo = 100; animarPct();
      const ic = document.getElementById('ipgIcon');
      ic.style.background = 'linear-gradient(135deg,#16a34a,#15803d)';
      ic.innerHTML = '<i class="bi bi-check-lg" style="font-size:2.5rem;animation:ipgPop .5s ease both"></i>';
      document.getElementById('ipgFrase').textContent = '¡Completado!';
      document.getElementById('ipgPaso').innerHTML =
        '<i class="bi bi-check-circle-fill" style="color:#22c55e"></i><span id="ipgPasoTxt"></span>';
      document.getElementById('ipgPasoTxt').textContent = mensaje || 'Listo';
    },
    error(mensaje) {
      clearInterval(frasesTimer); cancelAnimationFrame(raf);
      const ic = document.getElementById('ipgIcon');
      ic.style.background = 'linear-gradient(135deg,#dc2626,#7f1d1d)';
      ic.innerHTML = '<i class="bi bi-exclamation-triangle-fill" style="font-size:2.1rem"></i>';
      document.getElementById('ipgFrase').textContent = 'Con problemas';
      document.getElementById('ipgPaso').innerHTML =
        '<i class="bi bi-x-circle-fill" style="color:#f87171"></i><span id="ipgPasoTxt"></span>';
      document.getElementById('ipgPasoTxt').textContent = mensaje || 'Ocurrió un error';
    },
    cerrar() {
      clearInterval(frasesTimer); cancelAnimationFrame(raf);
      if (modal) modal.hide();
    },
  };
})();

async function epImportarSeleccionados() {
  if (_epSeleccion.size === 0) return;
  // Bloqueo si mismatch sin confirmar
  if (window._erpMismatch && !window._erpMismatch.confirmado) {
    if (typeof ilusAlert === 'function') {
      await ilusAlert({
        title: '⚠ RUT distinto sin confirmar',
        message: 'El documento ERP tiene un RUT diferente al del cliente actual.',
        sub: 'Antes de importar, registra el motivo en el campo de justificación que aparece arriba.',
        type: 'warning',
        okLabel: 'Entendido',
      });
    } else {
      ilusToast('Confirma primero el motivo del RUT distinto.', { type: 'warning' });
    }
    return;
  }

  const fecha_doc = _epDocActual.fecha_iso || '';
  const doc_origen = `${_epDocActual.tido_display} ${_epDocActual.nudo_display}`;
  const justifMismatch = window._erpMismatch?.motivo || '';

  // 1) Armar el PLAN de fichas a crear (leyendo los selects ANTES de procesar),
  //    así conocemos el total exacto y el progreso es REAL (no inventado).
  const plan = [];
  let bloqueados = 0;
  for (const idx of _epSeleccion) {
    const p = _epProductos[idx];
    if (p._completo) { bloqueados++; continue; }   // saldo 0 → no agrega
    const saldo = (p._saldo !== undefined) ? p._saldo : (parseInt(p.cantidad) || 1);
    const nombre = p.nombre || p.sku || '';
    const sel = document.querySelector(`select[data-ep-fichas-idx="${idx}"]`);
    const fichasElegidas = sel ? parseInt(sel.value) : 1;
    const filas = (fichasElegidas > 1) ? Math.min(fichasElegidas, saldo) : 1;
    const cantidadCadaUna = (fichasElegidas > 1) ? 1 : saldo;
    for (let n = 0; n < filas; n++) {
      plan.push({ sku: p.sku || '', nombre, cantidad: cantidadCadaUna });
    }
  }
  if (!plan.length) {
    ilusToast(bloqueados ? 'Esos equipos ya están completos en la ficha.' : 'No hay nada que importar.',
              { type: 'info' });
    return;
  }

  // 2) Cerrar los modales ERP (evita el glitch visual de modales encimados que
  //    reportó el usuario) y abrir SOLO el modal de progreso. Al final se recarga.
  document.getElementById('ep_btnImportar').disabled = true;
  try { if (_epModal) _epModal.hide(); } catch (e) {}
  try { bootstrap.Modal.getInstance(document.getElementById('modalErp'))?.hide(); } catch (e) {}
  ilusProgreso.open({
    titulo: 'Importando equipos desde el ERP',
    subtitulo: `${doc_origen} · ${plan.length} ficha(s) a crear`,
    paso: 'Conectando con el ERP…',
  });

  // 3) Crear las fichas una por una, actualizando el progreso REAL.
  let creados = 0, fallidos = 0;
  for (let i = 0; i < plan.length; i++) {
    const f = plan[i];
    ilusProgreso.avanzar(i, plan.length, `Creando ficha ${i + 1} de ${plan.length}: ${f.nombre}`);
    try {
      const r = await fetch(`/mantenciones/api/clientes/${CID}/maquinas`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          sku: f.sku,
          nombre: f.nombre,
          serie: '',                       // backend genera ILUS-{rut}-{sku}-{n}
          cantidad: f.cantidad,
          doc_origen,
          doc_fecha: fecha_doc,
          fecha_instalacion: fecha_doc,
          justif_doc_mismatch: justifMismatch,
        })
      });
      if (r.ok) creados++; else fallidos++;
    } catch (e) { fallidos++; }
  }

  ilusProgreso.avanzar(plan.length, plan.length, 'Finalizando…');
  if (creados === 0) {
    ilusProgreso.error(`No se pudieron crear las fichas (${fallidos} con error). Reintenta.`);
    document.getElementById('ep_btnImportar').disabled = false;
    setTimeout(() => ilusProgreso.cerrar(), 2800);
    return;
  }
  ilusProgreso.exito(`${creados} ficha(s) creada(s)${fallidos ? ' · ' + fallidos + ' con error' : ''}.`);
  setTimeout(() => { ilusProgreso.cerrar(); location.reload(); }, 1600);
}

function erpToggle(key, linea, checked) {
  if (checked) erpSeleccionadas[key] = linea;
  else delete erpSeleccionadas[key];
  actualizarContadorErp();
}
function actualizarContadorErp() {
  const n = Object.keys(erpSeleccionadas).length;
  document.getElementById('erpSelCount').textContent = `${n} línea(s) seleccionada(s)`;
}

async function importarDesdeErp() {
  const lineas = Object.values(erpSeleccionadas);
  if (!lineas.length) { alert('Selecciona al menos una línea'); return; }
  // Bloqueo: si hay mismatch de RUT y el usuario no lo confirmó, no se puede importar
  if (window._erpMismatch && !window._erpMismatch.confirmado) {
    alert('Este documento pertenece a otro RUT.\nDebes escribir el motivo y confirmar antes de importar los equipos.');
    const ta = document.getElementById('erpMismatchMotivo');
    if (ta) ta.focus();
    return;
  }
  const btn = document.querySelector('#modalErp .btn-ilus.fw-bold');
  if(btn) { btn.disabled=true; btn.innerHTML='<span class="spinner-border spinner-border-sm me-1"></span>Importando…'; }
  for (const l of lineas) {
    const qty = parseInt(l.cantidad) || 1;
    const nombre = l.nombre || l.producto || '';
    const esBulk = /disco|plate|pesa|dumbbell|kg\b/i.test(nombre);
    if (qty > 1 && !esBulk) {
      const expandir = confirm(`"${nombre}" tiene cantidad ${qty}.\n\n¿Crear ${qty} equipos individuales?\n→ Sí: ${qty} filas con cantidad 1\n→ No: 1 fila con cantidad ${qty}`);
      if (expandir) {
        for (let i = 0; i < qty; i++) {
          await fetch(`/mantenciones/api/clientes/${CID}/maquinas`, {
            method:'POST', headers:{'Content-Type':'application/json'},
            body: JSON.stringify({ sku:l.sku||'', nombre, doc_origen:l.doc_origen||'', doc_fecha:l.doc_fecha||l.fecha||'', cantidad:1 })
          });
        }
        continue;
      }
    }
    await fetch(`/mantenciones/api/clientes/${CID}/maquinas`, {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({ sku:l.sku||'', nombre, doc_origen:l.doc_origen||'', doc_fecha:l.doc_fecha||l.fecha||'', cantidad:qty })
    });
  }
  bootstrap.Modal.getInstance(document.getElementById('modalErp')).hide();
  location.reload();
}

// ─── Agregar máquina manual ───────────────────────────────
async function guardarMaquinaManual() {
  const nombre = document.getElementById('mm_nombre').value.trim();
  if (!nombre) {
    if (typeof ilusAlert === 'function') {
      await ilusAlert({title:'Nombre requerido', message:'El nombre del equipo es obligatorio.', type:'warning'});
    } else { alert('Nombre requerido'); }
    return;
  }
  // 2026-05-27 (Daniel): SKU y N° Serie auto si vienen vacíos. Backend genera.
  // Categoría/familia, marca, modelo, año, ubicación → todos opcionales.
  const payload = {
    nombre,
    sku:      document.getElementById('mm_sku').value.trim(),
    serie:    document.getElementById('mm_serie').value.trim(),
    cantidad: parseInt(document.getElementById('mm_cantidad').value) || 1,
    doc_origen: document.getElementById('mm_doc').value.trim(),
    notas:    document.getElementById('mm_notas').value.trim(),
    // Campos nuevos opcionales
    familia_equipo: (document.getElementById('mm_familia')||{}).value || '',
    marca:    (document.getElementById('mm_marca')||{}).value || '',
    modelo:   (document.getElementById('mm_modelo')||{}).value || '',
    anio_fabricacion: parseInt((document.getElementById('mm_anio')||{}).value) || null,
    ubicacion_sala: (document.getElementById('mm_ubicacion')||{}).value || '',
    auto_sku: true,    // pedirle al backend que rellene SKU si viene vacío
    auto_serie: true,  // pedirle al backend que rellene serie si viene vacío
  };
  const r = await fetch(`/mantenciones/api/clientes/${CID}/maquinas`, {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify(payload)
  });
  if (r.ok) {
    bootstrap.Modal.getInstance(document.getElementById('modalMaqManual')).hide();
    location.reload();
  } else {
    let err = 'Error al guardar';
    try { const d = await r.json(); err = d.error || err; } catch(_){}
    if (typeof ilusAlert === 'function') {
      await ilusAlert({title:'No se pudo agregar', message:err, type:'error'});
    } else { alert(err); }
  }
}

// 2026-05-22 (Daniel): "Eliminar equipo" pasa a ser flujo confidencial.
// Solo superadmin ve el botón (el template ya lo oculta a los demás roles),
// pero igual exigimos motivo (≥ 12 chars) + confirm text "ELIMINAR" para
// que quede en mant_logs con trazabilidad clara. El backend devuelve 403
// si por alguna razón llega un no-superadmin (defensa en profundidad).
async function eliminarMaquina(mid, nombre, btn) {
  // Paso 1 — Motivo obligatorio (≥ 12 chars). Reemplaza al simple Yes/No.
  // ilusPrompt escapa `message` por defecto; el HTML va en `sub` con subHtml:true.
  const nombreSafe = (nombre || ('Equipo #' + mid))
      .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  const motivo = await ilusPrompt({
    title: 'Eliminar equipo',
    message: 'Indica el motivo (mínimo 12 caracteres). Queda en auditoría para trazabilidad.',
    sub: `Vas a dar de baja: <strong style="color:#dc2626">${nombreSafe}</strong>`,
    subHtml: true,
    placeholder: 'Ej: equipo retirado del cliente por término de contrato',
    multiline: true,
    required: true,
    okLabel: 'Continuar',
    cancelLabel: 'Cancelar',
  });
  if (motivo === null) return;                       // canceló
  const motivoLimpio = (motivo || '').trim();
  if (motivoLimpio.length < 12) {
    await ilusAlert({
      title: 'Motivo insuficiente',
      message: 'El motivo debe tener al menos 12 caracteres.',
      type: 'warning',
    });
    return;
  }

  // Paso 2 — Confirm text "ELIMINAR" (defensa contra clicks accidentales)
  const confirmTxt = await ilusPrompt({
    title: 'Confirmación final',
    message: 'Esta acción quedará registrada en la auditoría del cliente.',
    sub: 'Escribe <strong style="color:#dc2626">ELIMINAR</strong> para confirmar.',
    subHtml: true,
    placeholder: 'ELIMINAR',
    required: true,
    okLabel: 'Eliminar definitivamente',
    cancelLabel: 'Cancelar',
  });
  if (confirmTxt === null) return;
  if ((confirmTxt || '').trim().toUpperCase() !== 'ELIMINAR') {
    await ilusAlert({
      title: 'Cancelado',
      message: 'No escribiste exactamente "ELIMINAR". La acción fue cancelada.',
      type: 'info',
    });
    return;
  }

  // Paso 3 — Disparar la baja
  if (btn) btn.disabled = true;
  try {
    const r = await fetch(`/mantenciones/api/maquinas/${mid}`, {
      method: 'DELETE',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ motivo: motivoLimpio, confirm_text: 'ELIMINAR' }),
    });
    const d = await r.json().catch(() => ({}));
    if (!r.ok || !d.ok) {
      if (btn) btn.disabled = false;
      const msg = (d && d.error) ? d.error : 'No se pudo eliminar el equipo.';
      await ilusAlert({
        title: 'No se pudo eliminar',
        message: msg,
        type: r.status === 403 ? 'danger' : 'error',
      });
      return;
    }
    // Animación + toast (no recargo, evito perder filtros y scroll)
    const row = document.getElementById(`maq-${mid}`);
    if (row) {
      row.style.transition = 'opacity .3s';
      row.style.opacity = '0';
      setTimeout(() => row.remove(), 300);
    }
    ilusToast('Equipo eliminado · auditoría registrada', { type: 'success' });
  } catch (e) {
    if (btn) btn.disabled = false;
    await ilusAlert({
      title: 'Error de red',
      message: 'No se pudo contactar al servidor: ' + (e.message || e),
      type: 'error',
    });
  }
}

// ─── Eliminar cliente (con doble confirmación) ─────────────────
function abrirEliminarCliente() {
  const inp = document.getElementById('del_confirm');
  inp.value = '';
  document.getElementById('del_error').classList.add('d-none');
  document.getElementById('btnConfirmarEliminar').disabled = true;
  // Habilitar botón solo cuando coincide
  const expected = [
    DATA.cliente_razon_social,
    DATA.cliente_rut
  ].map(v => (v||'').trim().toLowerCase()).filter(Boolean);
  inp.oninput = () => {
    const v = inp.value.trim().toLowerCase();
    const v_norm = v.replace(/[^0-9k]/g,'');
    const ok = expected.some(e => e === v || (e.replace(/[^0-9k]/g,'') === v_norm && v_norm.length>=7));
    document.getElementById('btnConfirmarEliminar').disabled = !ok;
  };
  new bootstrap.Modal(document.getElementById('modalEliminarCliente')).show();
}

async function confirmarEliminarCliente() {
  const btn = document.getElementById('btnConfirmarEliminar');
  const errBox = document.getElementById('del_error');
  const confirm_text = document.getElementById('del_confirm').value.trim();
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Eliminando...';
  errBox.classList.add('d-none');
  try {
    const r = await fetch(`/mantenciones/api/clientes/${CID}`, {
      method: 'DELETE',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({confirm_text})
    });
    const data = await r.json();
    if (data.ok) {
      alert(`✓ Cliente "${data.razon_social}" eliminado.\n\nSe eliminaron ${Object.entries(data.eliminado||{}).map(([k,v])=>`${v} ${k}`).join(', ') || 'datos asociados'}.`);
      window.location.href = '/mantenciones/clientes';
    } else {
      errBox.textContent = data.error || 'No se pudo eliminar.';
      errBox.classList.remove('d-none');
      btn.disabled = false;
      btn.innerHTML = '<i class="bi bi-trash3-fill me-1"></i>Eliminar definitivamente';
    }
  } catch (e) {
    errBox.textContent = 'Error de red: ' + e.message;
    errBox.classList.remove('d-none');
    btn.disabled = false;
    btn.innerHTML = '<i class="bi bi-trash3-fill me-1"></i>Eliminar definitivamente';
  }
}

// ─── Subir contrato ───────────────────────────────────────
// Flag global anti-doble-submit. Aunque el botón se deshabilite,
// esto previene también casos extremos (keypress repetido, etc.).
let _subirContratoEnCurso = false;

async function subirContrato() {
  // Guard anti doble-submit: ignorar clicks adicionales si ya hay
  // un upload en curso. Previene el bug del triple upload reportado.
  if (_subirContratoEnCurso) {
    if (typeof ilusToast === 'function') {
      ilusToast('Subida en curso, por favor espera…', { type: 'info' });
    }
    return;
  }
  const btn = document.getElementById('btnSubirContrato');
  const btnHtmlOrig = btn ? btn.innerHTML : '';

  const archivo = document.getElementById('ct_archivo').files[0];
  if (!archivo) { alert('Selecciona un archivo'); return; }

  // Validación frontend: solo PDF (espejo del backend para feedback rápido)
  const ext = (archivo.name.split('.').pop() || '').toLowerCase();
  if (ext !== 'pdf') {
    if (typeof ilusAlert === 'function') {
      await ilusAlert({
        title: 'Formato no permitido',
        message: 'Solo se aceptan archivos PDF.',
        sub: ['doc','docx'].includes(ext)
             ? 'Abre el documento en Word → Archivo → Guardar como → PDF, y vuelve a subirlo.'
             : 'Convierte el archivo a PDF antes de subirlo.',
        type: 'warning',
      });
    } else {
      alert('Solo se aceptan archivos PDF. Convierte el archivo y vuelve a intentarlo.');
    }
    return;
  }

  // Validación tamaño máximo (25 MB)
  const MAX_MB = 25;
  if (archivo.size > MAX_MB * 1024 * 1024) {
    const mb = (archivo.size / 1024 / 1024).toFixed(1);
    alert(`El archivo pesa ${mb} MB. Máximo permitido: ${MAX_MB} MB.\n\nReduce el peso con un compresor de PDF antes de subir.`);
    return;
  }

  // Activar guard + UI feedback
  _subirContratoEnCurso = true;
  if (btn) {
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Subiendo a Cloudinary…';
  }

  try {
    const fd = new FormData();
    fd.append('archivo', archivo);
    fd.append('nombre', document.getElementById('ct_nombre').value.trim() || archivo.name);
    fd.append('fecha_inicio', document.getElementById('ct_inicio').value);
    fd.append('fecha_vencimiento', document.getElementById('ct_vencimiento').value);
    fd.append('es_indefinido', document.getElementById('ct_indefinido').checked ? '1' : '');
    fd.append('monto_mensual', document.getElementById('ct_monto_mensual').value || '0');
    fd.append('frecuencia_meses', document.getElementById('ct_frecuencia').value || '0');
    fd.append('notas', document.getElementById('ct_notas').value.trim());

    const r = await fetch(`/mantenciones/api/clientes/${CID}/contratos`, { method:'POST', body:fd });
    let data = null;
    try { data = await r.json(); } catch(_) {}

    if (r.ok && data && data.ok) {
      bootstrap.Modal.getInstance(document.getElementById('modalContrato')).hide();
      if (data.reparado) {
        // El PDF estaba dañado y el sistema lo reparó — avisar explícito
        await ilusAlert({
          title: 'Contrato subido y reparado',
          message: 'El PDF estaba dañado y el sistema lo reparó automáticamente antes de guardarlo. Ya está disponible para visualizar.',
          type: 'success',
        });
      } else if (typeof ilusToast === 'function') {
        ilusToast('✓ Contrato subido correctamente (persistente)', { type: 'success' });
      }
      setTimeout(() => location.reload(), 900);
      return;
    }

    // PDF inválido — el sistema validó server-side y rechazó. Mantener
    // el modal abierto para que el usuario suba otro archivo de inmediato.
    if (data && data.error_codigo === 'PDF_INVALIDO') {
      await ilusAlert({
        title: 'El archivo no se puede usar',
        message: data.error,
        type: 'warning',
      });
      // Limpiar el input file para que el usuario elija otro
      const fInput = document.getElementById('ct_archivo');
      if (fInput) fInput.value = '';
      return;
    }

    // Errores codificados — mensaje específico
    if (data && data.error_codigo === 'LIMITE_CONTRATOS') {
      if (typeof ilusAlert === 'function') {
        await ilusAlert({
          title: 'Llegaste al máximo de contratos',
          message: data.error,
          sub: 'Cierra/elimina un contrato viejo o usa Documentos para anexos.',
          type: 'warning',
        });
      } else {
        alert(data.error);
      }
      return;
    }
    if (data && data.error_codigo === 'DUPLICADO_RAPIDO') {
      if (typeof ilusToast === 'function') {
        ilusToast('Ese archivo ya se subió hace segundos — no se duplicó.',
                  { type: 'warning' });
      }
      bootstrap.Modal.getInstance(document.getElementById('modalContrato')).hide();
      setTimeout(() => location.reload(), 900);
      return;
    }
    if (data && data.error_codigo === 'FORMATO_NO_PERMITIDO') {
      alert(data.error);
      return;
    }
    if (data && data.error_codigo === 'ARCHIVO_GRANDE') {
      alert(data.error);
      return;
    }
    if (data && data.error_codigo === 'CLOUDINARY_FAIL') {
      alert('Cloudinary no respondió. Por favor reinténtalo en unos segundos.');
      return;
    }
    if (data && data.error_codigo === 'ALMACENAMIENTO_NO_DISPONIBLE') {
      alert('El almacenamiento de archivos no está disponible. Contacta al administrador.');
      return;
    }

    // Error genérico
    const msg = (data && data.error) ? data.error : `Error al subir contrato (HTTP ${r.status})`;
    if (typeof ilusToast === 'function') ilusToast(msg, { type:'error' });
    else alert(msg);
  } catch (e) {
    if (typeof ilusToast === 'function') {
      ilusToast('Error de red: ' + e.message, { type:'error' });
    } else {
      alert('Error de red: ' + e.message);
    }
  } finally {
    // Liberar guard y restaurar UI
    _subirContratoEnCurso = false;
    if (btn) {
      btn.disabled = false;
      btn.innerHTML = btnHtmlOrig || '<i class="bi bi-upload me-1"></i>Subir';
    }
  }
}

// ─── Panel IA contrato ────────────────────────────────────
function toggleAiPanel(ctid) {
  const el = document.getElementById(`ai-${ctid}`);
  el.style.display = el.style.display === 'none' ? 'block' : 'none';
}

// ════════════════════════════════════════════════════════════════════
// MANTENCIÓN HISTÓRICA (LEGACY) — SUPERADO por la versión inline en
// ficha.html (abrirVisitaHistorica/vhPreview/vhGuardar). Estas viejas
// referenciaban #vh_crear_proxima (que ya no existe) → crasheaban con
// "Cannot set properties of null". Renombradas a _OLD_*_unused para que NO
// pisen a las inline. No se invocan desde ningún lado.
// ════════════════════════════════════════════════════════════════════
function _OLD_abrirVisitaHistorica_unused() {
  // Reset
  document.getElementById('vh_fecha').value = '';
  document.getElementById('vh_tipo').value = 'preventiva';
  document.getElementById('vh_tecnico').value = '';
  document.getElementById('vh_costo').value = '';
  document.getElementById('vh_titulo').value = 'Mantención preventiva';
  document.getElementById('vh_observaciones').value = '';
  document.getElementById('vh_crear_proxima').checked = true;
  document.getElementById('vh_preview').style.display = 'none';
  document.getElementById('vh_result').style.display = 'none';
  document.getElementById('vh_btn').disabled = false;
  document.getElementById('vh_btn').innerHTML = '<i class="bi bi-save me-1"></i>Registrar mantención';
  new bootstrap.Modal(document.getElementById('modalVisitaHistorica')).show();
}

// Preview en vivo (LEGACY, superado por inline en ficha.html)
async function _OLD_vhPreview_unused() {
  const fecha = document.getElementById('vh_fecha').value;
  const box = document.getElementById('vh_preview');
  const txt = document.getElementById('vh_preview_text');
  if (!fecha) { box.style.display = 'none'; return; }

  // Pedimos al backend la sugerencia de próxima (basada en frecuencia del contrato)
  // pero le decimos que la "última" es la fecha que el usuario está por registrar.
  // Para hacerlo simple: usamos el endpoint sugerir-proxima del cliente actual.
  // Si el cliente no tiene visitas previas, calculamos nosotros con la fecha tipeada.
  try {
    // Buscar frecuencia del contrato vigente
    const r = await fetch(`/mantenciones/api/clientes/${CID}/sugerir-proxima`);
    const d = await r.json();
    if (d.sin_frecuencia) {
      txt.innerHTML = `<span class="text-warning"><i class="bi bi-exclamation-triangle me-1"></i>${d.mensaje}</span>`;
      box.style.display = '';
      return;
    }
    const freq = d.frecuencia_meses;
    if (!freq) {
      txt.innerHTML = `<span class="text-muted">Sin frecuencia configurada en el contrato — la próxima visita se debe programar manualmente.</span>`;
      box.style.display = '';
      return;
    }

    // Calcular fecha sugerida en frontend
    const fechaD = new Date(fecha + 'T00:00:00');
    const proxima = new Date(fechaD);
    proxima.setMonth(proxima.getMonth() + freq);
    const hoy = new Date();
    hoy.setHours(0,0,0,0);
    const diasAlHoy = Math.round((proxima - hoy) / 86400000);
    const fmt = (d) => d.toLocaleDateString('es-CL', {day:'2-digit',month:'2-digit',year:'numeric'});

    let estadoTxt = '';
    let colorClass = 'text-success';
    if (diasAlHoy < 0) {
      const hoyMas7 = new Date(hoy);
      hoyMas7.setDate(hoyMas7.getDate() + 7);
      estadoTxt = `<br><span class="text-danger fw-bold">⚠ Esta fecha YA pasó hace ${-diasAlHoy} día(s)</span>. ` +
                  `Sugerencia: programar lo antes posible → <strong>${fmt(hoyMas7)}</strong> (hoy + 7 días).`;
      colorClass = 'text-danger';
    } else if (diasAlHoy <= 15) {
      estadoTxt = `<br><span class="text-warning fw-bold">Próxima visita en ${diasAlHoy} día(s)</span>`;
      colorClass = 'text-warning';
    } else {
      estadoTxt = `<br><span class="text-success">Próxima visita en ${diasAlHoy} días</span>`;
    }

    txt.innerHTML = `
      Última mantención: <strong>${fmt(fechaD)}</strong> ·
      Frecuencia contrato: <strong>cada ${freq} mes(es)</strong>
      <br>Próxima IDEAL: <strong class="${colorClass}">${fmt(proxima)}</strong>
      ${estadoTxt}
    `;
    box.style.display = '';
  } catch(e) {
    txt.innerHTML = `<span class="text-muted">No se pudo calcular: ${e.message}</span>`;
    box.style.display = '';
  }
}

async function _OLD_vhGuardar_unused() {
  const btn = document.getElementById('vh_btn');
  const box = document.getElementById('vh_result');
  const fecha = document.getElementById('vh_fecha').value;
  if (!fecha) {
    box.innerHTML = '<div class="alert alert-warning py-2 mb-0">Selecciona la fecha de la mantención.</div>';
    box.style.display = '';
    return;
  }
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Guardando…';

  try {
    const payload = {
      fecha_realizada:    fecha,
      tipo:               document.getElementById('vh_tipo').value,
      tecnico:            document.getElementById('vh_tecnico').value.trim(),
      titulo:             document.getElementById('vh_titulo').value.trim(),
      observaciones:      document.getElementById('vh_observaciones').value.trim(),
      costo:              parseFloat(document.getElementById('vh_costo').value || 0),
      crear_proxima:      document.getElementById('vh_crear_proxima').checked,
    };
    const r = await fetch(`/mantenciones/api/clientes/${CID}/visita-historica`, {
      method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(payload)
    });
    const d = await r.json();
    if (d.ok) {
      const sug = d.sugerencia || {};
      const proxMsg = d.proxima_creada_id
        ? `<div class="mt-2"><i class="bi bi-check-circle text-success me-1"></i>
           Próxima visita creada automáticamente (ID ${d.proxima_creada_id}) para
           <strong>${sug.fecha_sugerida || '—'}</strong></div>`
        : (sug.mensaje ? `<div class="mt-2 small text-muted">${sug.mensaje}</div>` : '');
      box.innerHTML = `
        <div class="alert alert-success mb-0" style="font-size:.85rem">
          <i class="bi bi-check-circle-fill me-1"></i>
          <strong>Mantención registrada.</strong>
          ${proxMsg}
          <div class="text-center mt-2">
            <button class="btn btn-sm btn-success" onclick="location.reload()">
              <i class="bi bi-arrow-clockwise me-1"></i>Recargar ficha
            </button>
          </div>
        </div>`;
      box.style.display = '';
      btn.innerHTML = '<i class="bi bi-check-lg me-1"></i>Listo';
    } else {
      box.innerHTML = `<div class="alert alert-danger py-2 mb-0" style="font-size:.85rem">
        <i class="bi bi-x-circle me-1"></i>${d.error || 'Error'}
      </div>`;
      box.style.display = '';
      btn.disabled = false;
      btn.innerHTML = '<i class="bi bi-save me-1"></i>Registrar mantención';
    }
  } catch(e) {
    box.innerHTML = `<div class="alert alert-danger py-2 mb-0">Error de red: ${e.message}</div>`;
    box.style.display = '';
    btn.disabled = false;
    btn.innerHTML = '<i class="bi bi-save me-1"></i>Registrar mantención';
  }
}

// ─── Re-subir archivo de contrato (cuando se perdió por deploy de Railway) ──
// Bucle persistente de re-subida: si el archivo está mal (PDF dañado, no es
// PDF real, etc.), el sistema muestra el problema y abre otra vez el picker
// hasta que el usuario suba uno válido o cancele explícitamente.
async function reSubirContrato(ctid, nombre) {
  // Confirmación inicial — solo se pide una vez
  const okStart = await ilusConfirm({
    title: 'Re-subir archivo del contrato',
    message: `«${nombre}»`,
    sub: 'Esto reemplaza el archivo anterior. Conserva los datos del contrato y el análisis IA actual.',
    okLabel: 'Elegir archivo',
  });
  if (!okStart) return;

  let intento = 0;
  let mensajeProblema = null;  // pasa de iteración a iteración

  while (true) {
    intento++;
    // Mostrar el problema de la iteración anterior (si lo hubo)
    if (mensajeProblema) {
      await ilusAlert({
        title: 'El archivo anterior no se pudo usar',
        message: mensajeProblema,
        type: 'warning',
      });
    }

    // Abrir file picker (envuelto en Promise para esperar a la elección)
    const f = await new Promise((resolve) => {
      const inp = document.createElement('input');
      inp.type = 'file';
      inp.accept = '.pdf';   // solo PDF — más estricto que antes
      inp.style.display = 'none';
      // Trick: si el usuario cancela el dialog, focus vuelve al body sin
      // disparar onchange. Detectamos con un listener temporal.
      const onCancel = () => {
        // Pequeño delay para no ganarle a onchange
        setTimeout(() => {
          if (!inp.files || inp.files.length === 0) resolve(null);
        }, 300);
        window.removeEventListener('focus', onCancel);
      };
      window.addEventListener('focus', onCancel, { once: true });
      inp.onchange = () => {
        window.removeEventListener('focus', onCancel);
        resolve(inp.files && inp.files[0] ? inp.files[0] : null);
        setTimeout(() => inp.remove(), 100);
      };
      document.body.appendChild(inp);
      inp.click();
    });

    if (!f) {
      // Usuario canceló — preguntamos si quiere salir del proceso
      const continuar = await ilusConfirm({
        title: '¿Salir sin re-subir?',
        message: 'El contrato sigue sin archivo válido. ¿Deseas cancelar el proceso?',
        okLabel: 'Sí, cancelar',
        cancelLabel: 'Volver a elegir',
        danger: true,
      });
      if (continuar) return;   // usuario abortó del todo
      mensajeProblema = null;   // no había problema, solo cerró el dialog
      continue;
    }

    // Subir el archivo seleccionado
    ilusToast(`Validando archivo (intento ${intento})…`, { type: 'info' });
    const fd = new FormData();
    fd.append('archivo', f);
    let d;
    try {
      const r = await fetch(`/mantenciones/api/contratos/${ctid}/re-subir`, {
        method: 'POST', body: fd
      });
      d = await r.json();
    } catch (e) {
      mensajeProblema = `Error de red: ${e.message}. Verifica tu conexión e intenta de nuevo.`;
      continue;   // vuelve a abrir el picker
    }

    if (d && d.ok) {
      // ÉXITO — mostrar mensaje y recargar
      const tituloOk = d.reparado
        ? 'Archivo re-subido y reparado'
        : 'Archivo re-subido correctamente';
      const msgOk = d.reparado
        ? `El PDF estaba dañado y el sistema lo reparó automáticamente antes de guardarlo. Ya está disponible (${d.n_pages || '—'} páginas).`
        : `Archivo válido (${d.n_pages || '—'} páginas). La página se recargará para mostrar el contrato.`;
      await ilusAlert({
        title: tituloOk,
        message: msgOk,
        type: 'success',
      });
      location.reload();
      return;   // salir del bucle
    }

    // FALLO — preparar mensaje para la siguiente iteración
    mensajeProblema = (d && d.error) ||
      'No se pudo procesar el archivo. Intenta con otro archivo PDF.';
    // El bucle continúa y vuelve a abrir el picker
  }
}

// Modal de eliminar contrato — Bootstrap custom, no confirm/prompt nativos
function eliminarContrato(ctid, nombre) {
  document.getElementById('elimCtrId').value = ctid;
  document.getElementById('elimCtrNombre').textContent = nombre || `Contrato #${ctid}`;
  document.getElementById('elimCtrInput').value = '';
  document.getElementById('elimCtrBtn').disabled = true;
  document.getElementById('elimCtrResult').style.display = 'none';
  new bootstrap.Modal(document.getElementById('modalEliminarContrato')).show();
}

async function _ejecutarEliminarContrato() {
  const ctid = document.getElementById('elimCtrId').value;
  const btn  = document.getElementById('elimCtrBtn');
  const box  = document.getElementById('elimCtrResult');
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Eliminando…';
  try {
    const r = await fetch(`/mantenciones/api/contratos/${ctid}`, {
      method: 'DELETE',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({confirm: 'ELIMINAR'})
    });
    const d = await r.json();
    if (d.ok) {
      box.innerHTML = `
        <div class="alert alert-success mb-0" style="font-size:.85rem">
          <i class="bi bi-check-circle-fill me-1"></i>
          <strong>Contrato eliminado.</strong>
          ${d.archivos_borrados ? `${d.archivos_borrados} archivo(s) físicos borrados.` : ''}
          Recargando ficha…
        </div>`;
      box.style.display = '';
      setTimeout(() => location.reload(), 1400);
    } else {
      box.innerHTML = `
        <div class="alert alert-danger mb-0" style="font-size:.85rem">
          <i class="bi bi-x-circle me-1"></i>${d.error || 'Error desconocido'}
        </div>`;
      box.style.display = '';
      btn.disabled = false;
      btn.innerHTML = '<i class="bi bi-trash3 me-1"></i>Eliminar definitivamente';
    }
  } catch(e) {
    box.innerHTML = `
      <div class="alert alert-danger mb-0" style="font-size:.85rem">
        Error de red: ${e.message || e}
      </div>`;
    box.style.display = '';
    btn.disabled = false;
    btn.innerHTML = '<i class="bi bi-trash3 me-1"></i>Eliminar definitivamente';
  }
}

async function analizarContrato(ctid, btn) {
  const orig = btn.innerHTML;
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Analizando contrato…';
  try {
    const r = await fetch(`/mantenciones/api/contratos/${ctid}/analizar`, { method:'POST' });
    const data = await r.json();
    if (data.ok) {
      _planTabActualizarStatus(true);
      // Si la IA detectó que NO es un contrato, avisamos (modal ILUS, no alert nativo).
      if (data.tipo_doc_detectado && data.tipo_doc_detectado !== 'contrato_servicio') {
        await ilusAlert({
          title: 'Este documento no parece un contrato',
          message: 'La IA detectó que el archivo subido no es un contrato de servicio. '
                 + 'El análisis se completó igual (con confianza baja). '
                 + 'Considera reemplazar el archivo por el contrato real.',
          sub: '<strong>Tipo detectado:</strong> ' + escHtml(data.tipo_doc_detectado)
             + '<br><strong>Razón:</strong> ' + escHtml(data.razon_deteccion || 'sin detalle'),
          subHtml: true,
          type: 'warning',
        });
      }
      // Mostramos el análisis 360° EN VIVO desde data.resultado.
      // Al cerrar el modal recargamos para refrescar el dashboard persistido del servidor.
      _mostrarAnalisisContrato360(data.resultado || {}, ctid);
      // El botón se restituye visualmente (igual va a recargar al cerrar el modal).
      btn.disabled = false;
      btn.innerHTML = orig;
    } else {
      // Si el documento fue RECHAZADO por la IA validadora
      if (data.error_codigo === 'NO_ES_CONTRATO') {
        await ilusAlert({
          title: 'El archivo no es un contrato',
          message: 'El documento subido no es un contrato de servicio. '
                 + 'Sube el contrato correcto y vuelve a intentar.',
          sub: '<strong>La IA detectó:</strong> ' + escHtml(data.tipo_doc_detectado || 'documento desconocido')
             + (data.razon_deteccion ? '<br><strong>Razón:</strong> ' + escHtml(data.razon_deteccion) : ''),
          subHtml: true,
          type: 'error',
        });
      } else {
        await ilusAlert({
          title: 'No se pudo analizar el contrato',
          message: data.error || 'Error desconocido',
          type: 'error',
        });
      }
      btn.disabled = false;
      btn.innerHTML = orig;
    }
  } catch(e) {
    btn.disabled = false;
    btn.innerHTML = orig;
    await ilusAlert({
      title: 'Error de conexión',
      message: e.message || String(e),
      type: 'error',
    });
  }
}

// ─── Análisis 360° del contrato (Comité de expertos IA) ──────────────────
// Renderiza el JSON enriquecido (exposición, garantía, cláusulas sugeridas,
// propuestas comerciales y rentabilidad) en el modal reusable #modalAIResult.
// Todo el texto que viene del JSON se escapa con escHtml() antes de inyectarse.
function _mostrarAnalisisContrato360(res, ctid) {
  res = res || {};
  const titleEl = document.getElementById('aiResultTitle');
  const bodyEl  = document.getElementById('aiResultBody');
  if (!bodyEl) { location.reload(); return; }   // sin modal → fallback al flujo viejo
  if (titleEl) titleEl.textContent = 'Análisis 360° del contrato';
  bodyEl.innerHTML = _ctaRenderHTML(res, ctid);

  const modalEl = document.getElementById('modalAIResult');
  const modal = new bootstrap.Modal(modalEl);
  // Al cerrar, recargamos la ficha para que el dashboard persistido (Jinja) se actualice.
  modalEl.addEventListener('hidden.bs.modal', () => location.reload(), { once: true });
  modal.show();
}

// Paleta semáforo reusable para niveles alto/medio/bajo.
function _ctaSemaforo(nivel) {
  const n = String(nivel || 'medio').toLowerCase();
  if (n === 'alto')  return { color:'#dc2626', bg:'#fef2f2', border:'#fecaca', label:'ALTO',  icon:'bi-exclamation-octagon-fill' };
  if (n === 'bajo')  return { color:'#16a34a', bg:'#f0fdf4', border:'#bbf7d0', label:'BAJO',  icon:'bi-shield-check' };
  return                    { color:'#f59e0b', bg:'#fffbeb', border:'#fde68a', label:'MEDIO', icon:'bi-exclamation-triangle-fill' };
}

// Paleta para prioridad/probabilidad alta/media/baja.
function _ctaPrioColor(p) {
  const n = String(p || '').toLowerCase();
  if (n === 'alta') return { color:'#dc2626', bg:'#fee2e2' };
  if (n === 'baja') return { color:'#16a34a', bg:'#dcfce7' };
  return                   { color:'#b45309', bg:'#fef3c7' };
}

// Formatea un número CLP con separador de miles (es-CL). Null/0 → null.
function _ctaCLP(v) {
  if (v === null || v === undefined || v === '') return null;
  const n = Number(v);
  if (!isFinite(n) || n === 0) return null;
  return '$' + n.toLocaleString('es-CL');
}

// Encabezado de sección reusable.
function _ctaHead(emoji, titulo, color) {
  return `<div style="display:flex;align-items:center;gap:8px;margin:22px 0 12px">
    <span style="font-size:1.15rem;line-height:1">${emoji}</span>
    <h6 style="margin:0;font-size:.95rem;font-weight:800;color:${color || '#0f172a'}">${escHtml(titulo)}</h6>
  </div>`;
}

// Construye TODO el HTML del análisis 360°.
function _ctaRenderHTML(res, ctid) {
  const score = Number(res.score) || 0;
  const sc = score >= 70 ? '#16a34a' : score >= 40 ? '#f59e0b' : '#dc2626';
  const scText = score >= 80 ? 'Excelente' : score >= 60 ? 'Bueno' : score >= 40 ? 'Regular' : 'Crítico';
  const riesgo = _ctaSemaforo(res.nivel_riesgo);

  let h = '';

  // Barra de acciones: exportar el análisis como documento PDF corporativo ILUS.
  if (ctid) {
    h += `<div style="display:flex;justify-content:flex-end;gap:8px;margin-bottom:10px">
      <button class="btn btn-sm btn-outline-dark fw-semibold"
              onclick="window.open('/mantenciones/api/contratos/${ctid}/analisis/pdf','_blank','noopener')">
        <i class="bi bi-file-earmark-pdf me-1"></i>Descargar PDF
      </button>
    </div>`;
  }

  // ── HERO: score + nivel de riesgo + resumen ──
  h += `<div style="display:flex;align-items:center;gap:20px;flex-wrap:wrap;
        background:linear-gradient(135deg,#0f172a,#1e293b);color:#fff;
        border-radius:14px;padding:20px 22px;margin-bottom:6px">
    <div style="position:relative;flex-shrink:0">
      <svg viewBox="0 0 100 100" style="width:104px;height:104px">
        <circle cx="50" cy="50" r="44" fill="none" stroke="rgba(255,255,255,.12)" stroke-width="8"></circle>
        <circle cx="50" cy="50" r="44" fill="none" stroke="${sc}" stroke-width="8"
                stroke-dasharray="${(score*2.764).toFixed(1)} ${(276.4-score*2.764).toFixed(1)}"
                transform="rotate(-90 50 50)" stroke-linecap="round"
                style="transition:stroke-dasharray .8s cubic-bezier(.34,1.56,.64,1)"></circle>
        <text x="50" y="48" text-anchor="middle" style="font-size:26px;font-weight:900;fill:#fff">${score}</text>
        <text x="50" y="63" text-anchor="middle" style="font-size:9px;font-weight:600;fill:rgba(255,255,255,.6)">de 100</text>
      </svg>
    </div>
    <div style="flex:1;min-width:230px">
      <div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:8px">
        <span style="background:${sc}22;color:${sc};border:1px solid ${sc}55;
              padding:3px 12px;border-radius:50px;font-size:.72rem;font-weight:800;
              text-transform:uppercase;letter-spacing:.4px">${scText}</span>
        <span style="background:${riesgo.color}22;color:${riesgo.color};border:1px solid ${riesgo.color}55;
              padding:3px 12px;border-radius:50px;font-size:.72rem;font-weight:800;
              text-transform:uppercase;letter-spacing:.4px">
          <i class="bi ${riesgo.icon} me-1"></i>Riesgo ${riesgo.label}</span>
        ${res.tipo_contrato ? `<span style="background:rgba(124,58,237,.25);color:#c4b5fd;
              padding:3px 12px;border-radius:50px;font-size:.72rem;font-weight:700">
              ${escHtml(res.tipo_contrato)}</span>` : ''}
      </div>
      ${res.resumen ? `<p style="margin:0;font-size:.88rem;line-height:1.55;color:#e2e8f0">${escHtml(res.resumen)}</p>` : ''}
    </div>
  </div>`;

  // ── KPIs económicos rápidos ──
  const kpis = [];
  const mm = _ctaCLP(res.costo_mensual);
  const ct = _ctaCLP(res.costo_total);
  const cpm = _ctaCLP(res.costo_por_mant);
  if (mm)  kpis.push(['bi-cash-stack', '#16a34a', '#dcfce7', mm,  'Monto mensual']);
  if (ct)  kpis.push(['bi-wallet2',    '#1e40af', '#dbeafe', ct,  'Costo total']);
  if (cpm) kpis.push(['bi-tools',      '#92400e', '#fef3c7', cpm, 'Por mantención']);
  if (res.sla_horas) kpis.push(['bi-stopwatch', '#6d28d9', '#ede9fe', res.sla_horas + 'h', 'SLA respuesta']);
  if (res.frecuencia_sugerida_meses) kpis.push(['bi-arrow-repeat', '#0e7490', '#cffafe', 'c/' + res.frecuencia_sugerida_meses + 'm', 'Frecuencia']);
  if (kpis.length) {
    h += `<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px;margin-top:14px">`;
    kpis.forEach(([ic, fg, bg, val, lbl]) => {
      h += `<div style="display:flex;align-items:center;gap:10px;background:#fff;border:1px solid #eef0f3;
            border-radius:10px;padding:11px 13px;box-shadow:0 1px 2px rgba(0,0,0,.04)">
        <div style="width:36px;height:36px;border-radius:9px;background:${bg};color:${fg};
              display:flex;align-items:center;justify-content:center;flex-shrink:0;font-size:1.05rem">
          <i class="bi ${ic}"></i></div>
        <div style="min-width:0">
          <div style="font-size:1rem;font-weight:800;color:#0f172a;line-height:1.1">${escHtml(val)}</div>
          <div style="font-size:.68rem;color:#6b7280;text-transform:uppercase;letter-spacing:.3px">${escHtml(lbl)}</div>
        </div></div>`;
    });
    h += `</div>`;
  }

  // ── ⚠️ EXPOSICIÓN (lo que más le importa a gerencia) ──
  const exp = res.exposicion;
  if (exp && (exp.nivel || exp.resumen || (exp.escenarios||[]).length)) {
    const s = _ctaSemaforo(exp.nivel);
    h += _ctaHead('⚠️', 'Exposición', '#0f172a');
    h += `<div style="background:${s.bg};border:1px solid ${s.border};border-left:5px solid ${s.color};
          border-radius:12px;padding:14px 16px">
      <div style="display:flex;align-items:center;gap:10px;margin-bottom:${exp.resumen ? '8px' : '0'}">
        <span style="display:inline-flex;align-items:center;gap:6px;background:${s.color};color:#fff;
              padding:4px 12px;border-radius:50px;font-size:.72rem;font-weight:800;
              text-transform:uppercase;letter-spacing:.5px;flex-shrink:0">
          <i class="bi ${s.icon}"></i>Nivel ${s.label}</span>
        ${exp.resumen ? '' : `<span style="font-size:.8rem;color:#64748b">Sin resumen disponible</span>`}
      </div>
      ${exp.resumen ? `<p style="margin:0;font-size:.86rem;line-height:1.55;color:#334155">${escHtml(exp.resumen)}</p>` : ''}
    </div>`;

    const escenarios = exp.escenarios || [];
    if (escenarios.length) {
      h += `<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:12px;margin-top:12px">`;
      escenarios.forEach(e => {
        const pp = _ctaPrioColor(e.probabilidad);
        h += `<div style="background:#fff;border:1px solid #eef0f3;border-radius:12px;padding:14px 15px;
              box-shadow:0 2px 8px rgba(0,0,0,.05);display:flex;flex-direction:column;gap:9px">
          <div style="display:flex;align-items:flex-start;justify-content:space-between;gap:8px">
            <div style="font-size:.86rem;font-weight:800;color:#0f172a;line-height:1.35">${escHtml(e.riesgo || 'Escenario de riesgo')}</div>
            ${e.probabilidad ? `<span style="background:${pp.bg};color:${pp.color};padding:2px 9px;
                  border-radius:50px;font-size:.64rem;font-weight:800;text-transform:uppercase;
                  letter-spacing:.3px;flex-shrink:0;white-space:nowrap">Prob. ${escHtml(e.probabilidad)}</span>` : ''}
          </div>
          ${e.impacto ? `<div style="font-size:.78rem;color:#475569;line-height:1.5">
            <i class="bi bi-lightning-charge-fill me-1" style="color:#f59e0b"></i>
            <strong>Impacto:</strong> ${escHtml(e.impacto)}</div>` : ''}
          ${e.mitigacion ? `<div style="font-size:.78rem;color:#166534;background:#f0fdf4;border-radius:8px;
                padding:8px 10px;line-height:1.5">
            <i class="bi bi-shield-check me-1"></i>
            <strong>Mitigación:</strong> ${escHtml(e.mitigacion)}</div>` : ''}
        </div>`;
      });
      h += `</div>`;
    }
  }

  // ── 🛡️ GARANTÍA ──
  const gar = res.analisis_garantia;
  if (gar && (gar.cubre_garantia !== undefined || gar.riesgo_terceros || gar.recomendacion)) {
    h += _ctaHead('🛡️', 'Garantía', '#0f172a');
    const cubre = !!gar.cubre_garantia;
    const cond  = !!gar.condicionada_a_mantencion_ilus;
    h += `<div style="background:#fff;border:1px solid #eef0f3;border-radius:12px;padding:14px 16px;
          box-shadow:0 2px 8px rgba(0,0,0,.05)">
      <div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:${(gar.riesgo_terceros||gar.recomendacion)?'12px':'0'}">
        <span style="display:inline-flex;align-items:center;gap:6px;
              background:${cubre?'#dcfce7':'#fee2e2'};color:${cubre?'#166534':'#991b1b'};
              padding:4px 12px;border-radius:50px;font-size:.74rem;font-weight:800">
          <i class="bi ${cubre?'bi-check-circle-fill':'bi-x-circle-fill'}"></i>
          ${cubre?'Cubre garantía':'No cubre garantía'}</span>
        <span style="display:inline-flex;align-items:center;gap:6px;
              background:${cond?'#dcfce7':'#fee2e2'};color:${cond?'#166534':'#991b1b'};
              padding:4px 12px;border-radius:50px;font-size:.74rem;font-weight:800">
          <i class="bi ${cond?'bi-link-45deg':'bi-unlock'}"></i>
          ${cond?'Condicionada a mantención ILUS':'No condicionada a ILUS'}</span>
      </div>
      ${gar.riesgo_terceros ? `<div style="font-size:.82rem;color:#475569;line-height:1.55;margin-bottom:${gar.recomendacion?'10px':'0'}">
        <i class="bi bi-people-fill me-1" style="color:#dc2626"></i>
        <strong>Riesgo de terceros:</strong> ${escHtml(gar.riesgo_terceros)}</div>` : ''}
      ${gar.recomendacion ? `<div style="font-size:.82rem;color:#1e3a8a;background:#eff6ff;border-radius:8px;
            padding:9px 11px;line-height:1.55">
        <i class="bi bi-lightbulb-fill me-1" style="color:#3b82f6"></i>
        <strong>Recomendación:</strong> ${escHtml(gar.recomendacion)}</div>` : ''}
    </div>`;
  }

  // ── ⚖️ CLÁUSULAS SUGERIDAS (con botón copiar) ──
  const cls = res.clausulas_sugeridas || [];
  if (cls.length) {
    h += _ctaHead('⚖️', 'Cláusulas sugeridas', '#0f172a');
    cls.forEach((c, i) => {
      const texto = c.texto || '';
      h += `<div style="background:#fff;border:1px solid #eef0f3;border-radius:12px;padding:14px 16px;
            margin-bottom:10px;box-shadow:0 2px 8px rgba(0,0,0,.05)">
        <div style="display:flex;align-items:flex-start;justify-content:space-between;gap:10px;margin-bottom:8px">
          <div style="font-size:.88rem;font-weight:800;color:#0f172a;line-height:1.35">
            <i class="bi bi-bookmark-star-fill me-1" style="color:#dc2626"></i>${escHtml(c.titulo || ('Cláusula ' + (i+1)))}</div>
          <button type="button" class="btn btn-sm btn-outline-secondary" style="flex-shrink:0;white-space:nowrap"
                  onclick="_ctaCopiarClausula(this)" data-clausula="${escAttr(texto)}">
            <i class="bi bi-clipboard me-1"></i>Copiar</button>
        </div>
        ${texto ? `<div style="font-size:.82rem;color:#334155;line-height:1.6;background:#f8fafc;
              border-left:3px solid #dc2626;border-radius:6px;padding:11px 13px;
              white-space:pre-wrap;font-family:Georgia,'Times New Roman',serif">${escHtml(texto)}</div>` : ''}
        ${c.justificacion ? `<div style="font-size:.74rem;color:#6b7280;margin-top:8px;line-height:1.5">
          <i class="bi bi-info-circle me-1"></i><strong>Por qué:</strong> ${escHtml(c.justificacion)}</div>` : ''}
      </div>`;
    });
  }

  // ── 💼 PROPUESTAS COMERCIALES ──
  const props = res.propuestas_comerciales || [];
  if (props.length) {
    h += _ctaHead('💼', 'Propuestas comerciales', '#0f172a');
    h += `<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(250px,1fr));gap:12px">`;
    props.forEach(p => {
      const pp = _ctaPrioColor(p.prioridad);
      h += `<div style="background:#fff;border:1px solid #eef0f3;border-radius:12px;padding:14px 15px;
            box-shadow:0 2px 8px rgba(0,0,0,.05);display:flex;flex-direction:column;gap:8px;
            border-top:3px solid ${pp.color}">
        <div style="display:flex;align-items:flex-start;justify-content:space-between;gap:8px">
          <div style="font-size:.86rem;font-weight:800;color:#0f172a;line-height:1.35">${escHtml(p.titulo || 'Propuesta comercial')}</div>
          ${p.prioridad ? `<span style="background:${pp.bg};color:${pp.color};padding:2px 9px;
                border-radius:50px;font-size:.64rem;font-weight:800;text-transform:uppercase;
                letter-spacing:.3px;flex-shrink:0;white-space:nowrap">${escHtml(p.prioridad)}</span>` : ''}
        </div>
        ${p.descripcion ? `<div style="font-size:.79rem;color:#475569;line-height:1.5">${escHtml(p.descripcion)}</div>` : ''}
        ${p.impacto_ingreso ? `<div style="font-size:.76rem;color:#166534;font-weight:700;margin-top:auto">
          <i class="bi bi-graph-up-arrow me-1"></i>${escHtml(p.impacto_ingreso)}</div>` : ''}
      </div>`;
    });
    h += `</div>`;
  }

  // ── 📊 RENTABILIDAD ──
  const rent = res.rentabilidad;
  if (rent && (rent.mrr_estimado_clp || rent.margen_estimado || rent.oportunidad_ingreso_clp)) {
    h += _ctaHead('📊', 'Rentabilidad', '#0f172a');
    h += `<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px">`;
    const mrr = _ctaCLP(rent.mrr_estimado_clp);
    const opp = _ctaCLP(rent.oportunidad_ingreso_clp);
    if (mrr) {
      h += `<div style="background:linear-gradient(135deg,#0f172a,#1e293b);color:#fff;border-radius:12px;padding:15px 16px">
        <div style="font-size:.68rem;text-transform:uppercase;letter-spacing:.5px;color:#94a3b8;margin-bottom:4px">
          <i class="bi bi-cash-coin me-1"></i>MRR estimado</div>
        <div style="font-size:1.5rem;font-weight:900;line-height:1">${escHtml(mrr)}</div>
        <div style="font-size:.66rem;color:#64748b;margin-top:3px">ingreso mensual recurrente</div>
      </div>`;
    }
    if (opp) {
      h += `<div style="background:linear-gradient(135deg,#16a34a,#15803d);color:#fff;border-radius:12px;padding:15px 16px">
        <div style="font-size:.68rem;text-transform:uppercase;letter-spacing:.5px;color:#bbf7d0;margin-bottom:4px">
          <i class="bi bi-graph-up-arrow me-1"></i>Oportunidad de ingreso</div>
        <div style="font-size:1.5rem;font-weight:900;line-height:1">${escHtml(opp)}</div>
        <div style="font-size:.66rem;color:#dcfce7;margin-top:3px">potencial adicional detectado</div>
      </div>`;
    }
    if (rent.margen_estimado) {
      h += `<div style="background:#fff;border:1px solid #eef0f3;border-radius:12px;padding:15px 16px;
            box-shadow:0 2px 8px rgba(0,0,0,.05)">
        <div style="font-size:.68rem;text-transform:uppercase;letter-spacing:.5px;color:#6b7280;margin-bottom:4px">
          <i class="bi bi-pie-chart-fill me-1"></i>Margen estimado</div>
        <div style="font-size:1.15rem;font-weight:800;color:#0f172a;line-height:1.25">${escHtml(rent.margen_estimado)}</div>
      </div>`;
    }
    h += `</div>`;
  }

  // ── Listas clásicas (cláusulas críticas / puntos críticos / alertas / mejoras) ──
  const _ctaLista = (emoji, titulo, items, color, bg, icon) => {
    const arr = (items || []).filter(Boolean);
    if (!arr.length) return '';
    let out = _ctaHead(emoji, titulo, '#0f172a');
    out += `<div style="display:flex;flex-direction:column;gap:7px">`;
    arr.forEach(it => {
      const txt = typeof it === 'string' ? it : (it.texto || it.titulo || it.descripcion || JSON.stringify(it));
      out += `<div style="display:flex;align-items:flex-start;gap:9px;background:${bg};
            border-radius:9px;padding:10px 12px;font-size:.82rem;color:#334155;line-height:1.5">
        <i class="bi ${icon}" style="color:${color};flex-shrink:0;margin-top:2px"></i>
        <span>${escHtml(txt)}</span></div>`;
    });
    out += `</div>`;
    return out;
  };
  h += _ctaLista('📌', 'Cláusulas críticas', res.clausulas_criticas, '#dc2626', '#fef2f2', 'bi-exclamation-diamond-fill');
  h += _ctaLista('🔎', 'Puntos críticos',    res.puntos_criticos,    '#b45309', '#fffbeb', 'bi-search');
  h += _ctaLista('🔔', 'Alertas',            res.alertas,            '#dc2626', '#fef2f2', 'bi-bell-fill');
  h += _ctaLista('🚀', 'Mejoras prioritarias', res.mejoras_prioritarias, '#16a34a', '#f0fdf4', 'bi-arrow-up-circle-fill');

  // ── Cobertura (texto largo) ──
  if (res.cobertura_descripcion) {
    h += _ctaHead('📋', 'Cobertura', '#0f172a');
    h += `<div style="font-size:.84rem;color:#334155;line-height:1.6;background:#f8fafc;
          border:1px solid #eef0f3;border-radius:10px;padding:13px 15px;white-space:pre-wrap">${escHtml(res.cobertura_descripcion)}</div>`;
  }

  // ── Pie: aviso de que al cerrar se refresca ──
  h += `<div style="margin-top:20px;padding-top:14px;border-top:1px dashed #e5e7eb;
        font-size:.74rem;color:#9ca3af;text-align:center">
    <i class="bi bi-stars me-1" style="color:#7c3aed"></i>
    Análisis generado por el comité de expertos IA. Al cerrar, la ficha se actualizará con el resumen guardado.
  </div>`;

  return h;
}

// Copia la redacción jurídica de una cláusula al portapapeles (toast ILUS).
async function _ctaCopiarClausula(btn) {
  const texto = btn.getAttribute('data-clausula') || '';
  try {
    await navigator.clipboard.writeText(texto);
    ilusToast('Copiado', { type:'success' });
    const orig = btn.innerHTML;
    btn.innerHTML = '<i class="bi bi-check2 me-1"></i>Copiado';
    setTimeout(() => { btn.innerHTML = orig; }, 1600);
  } catch(e) {
    // Fallback para navegadores sin clipboard API (o contexto no seguro)
    try {
      const ta = document.createElement('textarea');
      ta.value = texto; ta.style.position = 'fixed'; ta.style.opacity = '0';
      document.body.appendChild(ta); ta.select();
      document.execCommand('copy'); document.body.removeChild(ta);
      ilusToast('Copiado', { type:'success' });
    } catch(_) {
      ilusToast('No se pudo copiar', { type:'error' });
    }
  }
}

// ─── Visitas ──────────────────────────────────────────────
async function guardarVisita() {
  const vid = document.getElementById('vi_id').value;
  const fecha = document.getElementById('vi_fecha').value;
  if (!fecha) { ilusToast('La fecha es requerida', { type:'warning' }); return; }
  const data = {
    cliente_id:      CID,
    titulo:          document.getElementById('vi_titulo').value.trim(),
    tipo:            document.getElementById('vi_tipo').value,
    estado:          document.getElementById('vi_estado').value,
    fecha_programada: fecha,
    hora_inicio:     document.getElementById('vi_hora_inicio').value || null,
    hora_fin:        document.getElementById('vi_hora_fin').value || null,
    tecnico:         document.getElementById('vi_tecnico').value.trim(),
    costo:           parseFloat(document.getElementById('vi_costo').value) || 0,
    descripcion:     document.getElementById('vi_descripcion').value.trim(),
    // Garantía transversal (Aplica/No aplica) — independiente del tipo.
    garantia_aplica: document.getElementById('vi_gar_si')?.checked || false,
  };
  let url = '/mantenciones/api/visitas', method = 'POST';
  if (vid) { url = `/mantenciones/api/visitas/${vid}`; method = 'PUT'; }
  const r = await fetch(url, {
    method, headers:{'Content-Type':'application/json'}, body:JSON.stringify(data)
  });
  if (r.ok) {
    bootstrap.Modal.getInstance(document.getElementById('modalVisita')).hide();
    location.reload();
  } else { ilusToast('Error al guardar la visita', { type:'error' }); }
}

async function eliminarVisita() {
  const vid = document.getElementById('vi_id').value;
  if (!vid) return;
  const ok = await ilusConfirm({
    title: 'Eliminar visita',
    message: '¿Eliminar esta visita?',
    sub: 'No se puede deshacer.',
    okLabel: 'Eliminar', danger: true,
  });
  if (!ok) return;
  const r = await fetch(`/mantenciones/api/visitas/${vid}`, { method:'DELETE' });
  if (r.ok) {
    bootstrap.Modal.getInstance(document.getElementById('modalVisita')).hide();
    location.reload();
  } else {
    ilusToast('Error al eliminar la visita', { type:'error' });
  }
}

// ─── Plan de Mejora IA ────────────────────────────────────
// Estado del análisis (cacheado en memoria de la página) — se rellena
// al abrir el tab IA y después de cada acción que lo modifique.
let _PLAN_IA_ESTADO = null;

// Punto de entrada único del botón "Analizar con IA". Su comportamiento
// depende del estado actual:
//   - sin plan previo          → genera uno nuevo (no gasta tokens si ya
//                                 el backend tiene cache de 1h)
//   - plan no verificado       → abre modal de verificación
//   - plan vigente sin info nueva → no hace nada (botón debería estar deshabilitado)
//   - plan verificado >6 meses → genera uno nuevo
//   - info nueva detectada     → genera uno nuevo
async function planAccionIA() {
  if (!_PLAN_IA_ESTADO) {
    await planActualizarEstadoIA();
  }
  const e = _PLAN_IA_ESTADO || {};

  // Si hay un plan anterior y no está verificado → abrir modal
  if (e.ultimo_plan && !e.ultimo_plan.verificado_at) {
    planAbrirModalVerificar();
    return;
  }

  // Si no puede regenerar (>6 meses + sin info nueva) → toast informativo
  if (!e.puede_regenerar) {
    ilusToast(e.motivo_bloqueo || 'El plan anterior aún está vigente.', { type: 'info' });
    return;
  }

  // Genera (puede ser primer plan o re-generación legítima)
  await generarPlanMejora();
}

// Llama al endpoint de estado y refresca el botón / chips. NO gasta tokens.
async function planActualizarEstadoIA() {
  const btn = document.getElementById('btnGenerarPlan');
  try {
    const r = await fetch(`/mantenciones/api/clientes/${CID}/ia-plan-estado`);
    const data = await r.json();
    if (data && data.ok) {
      _PLAN_IA_ESTADO = data;
    } else {
      _PLAN_IA_ESTADO = null;
    }
  } catch(e) {
    _PLAN_IA_ESTADO = null;
  }
  planRenderEstadoIA();

  // Si hay un plan vigente, también lo rehidrata en pantalla sin volver a llamar
  // al endpoint IA (que sí gastaría tokens si el cache RAM expiró).
  const e = _PLAN_IA_ESTADO || {};
  if (e.ultimo_plan && !document.getElementById('planResultado').dataset.rendered) {
    // Solo intentamos rehidratar leyendo el campo plan_json — si no viene en el
    // payload, dejamos el área vacía. (El endpoint actual no lo expone para
    // evitar payloads pesados; la rehidratación visual aparece al regenerar.)
    // Nada que hacer aquí — el render real ocurre cuando el usuario presiona el botón.
  }
}

// Pinta el estado en el botón principal + chips informativos.
function planRenderEstadoIA() {
  const btn   = document.getElementById('btnGenerarPlan');
  const status = document.getElementById('planCtStatus');
  const chips  = document.getElementById('planEstadoChips');
  if (!btn || !status) return;

  const e = _PLAN_IA_ESTADO || {};
  const ult = e.ultimo_plan;

  // Por defecto, botón habilitado morado
  btn.disabled = false;
  btn.classList.remove('btn-secondary','btn-warning','btn-ilus','btn-purple','disabled');
  btn.style.background = '';
  btn.style.color = '';
  btn.style.borderColor = '';
  btn.classList.add('btn-ilus');

  // Reset chips
  if (chips) { chips.style.display = 'none'; chips.innerHTML = ''; }

  // CASO 1: sin plan previo
  if (!ult) {
    btn.innerHTML = '<i class="bi bi-stars me-2"></i>Generar análisis IA';
    status.innerHTML = '<span class="text-muted"><i class="bi bi-info-circle me-1"></i>Aún no se ha generado un análisis IA para este cliente</span>';
    return;
  }

  // Datos comunes
  const verif = !!ult.verificado_at;
  const edad  = ult.edad_dias ?? 0;
  const dias  = e.dias_para_proximo ?? 0;
  const infoNueva = !!e.info_nueva_disponible;
  const fechaGen = (ult.generado_at || '').slice(0, 10);

  // CASO 2: plan no verificado → botón ámbar
  if (!verif) {
    btn.classList.remove('btn-ilus');
    btn.style.background = '#f59e0b';
    btn.style.color = '#fff';
    btn.style.borderColor = '#f59e0b';
    btn.innerHTML = '<i class="bi bi-clipboard-check me-2"></i>Verificar cumplimiento del plan anterior';
    status.innerHTML = `<span style="color:#92400e"><i class="bi bi-exclamation-triangle me-1"></i>Plan del ${fechaGen} pendiente de verificación</span>`;
    if (chips) {
      chips.style.display = '';
      chips.innerHTML = `
        <span class="badge" style="background:#fef3c7;color:#92400e;font-size:.7rem">
          <i class="bi bi-exclamation-triangle me-1"></i>Falta verificar cumplimiento
        </span>
        <span class="badge" style="background:#f3f4f6;color:#374151;font-size:.7rem">
          <i class="bi bi-calendar3 me-1"></i>Análisis vigente del ${fechaGen}
        </span>
      `;
    }
    return;
  }

  // CASO 3: plan verificado + info nueva → botón morado con badge
  if (infoNueva) {
    btn.innerHTML = '<i class="bi bi-arrow-clockwise me-2"></i>Actualizar análisis (info nueva)';
    const motivos = (e.info_nueva_resumen || []).slice(0, 2).join(' · ');
    status.innerHTML = `<span style="color:#7c3aed"><i class="bi bi-stars me-1"></i>${motivos || 'Hay información nueva relevante'}</span>`;
    if (chips) {
      chips.style.display = '';
      chips.innerHTML = `
        <span class="badge" style="background:#dcfce7;color:#166534;font-size:.7rem">
          <i class="bi bi-stars me-1"></i>Info nueva disponible
        </span>
        <span class="badge" style="background:#f3f4f6;color:#374151;font-size:.7rem">
          <i class="bi bi-calendar3 me-1"></i>Plan vigente del ${fechaGen}
        </span>
        <span class="badge" style="background:#f3f4f6;color:#374151;font-size:.7rem">
          <i class="bi bi-check2-circle me-1"></i>Verificado
        </span>
      `;
    }
    return;
  }

  // CASO 4: plan verificado + >=6 meses (puede regenerar)
  if (e.puede_regenerar) {
    btn.innerHTML = '<i class="bi bi-stars me-2"></i>Generar nuevo análisis IA';
    status.innerHTML = `<span class="text-success"><i class="bi bi-check-circle-fill me-1"></i>Plan anterior verificado · ya pasaron 6 meses</span>`;
    if (chips) {
      chips.style.display = '';
      chips.innerHTML = `
        <span class="badge" style="background:#f3f4f6;color:#374151;font-size:.7rem">
          <i class="bi bi-calendar3 me-1"></i>Último plan: ${fechaGen} (${edad} días)
        </span>
      `;
    }
    return;
  }

  // CASO 5: plan verificado pero <6 meses sin info nueva → deshabilitado
  btn.disabled = true;
  btn.classList.remove('btn-ilus');
  btn.style.background = '#9ca3af';
  btn.style.color = '#fff';
  btn.style.borderColor = '#9ca3af';
  btn.innerHTML = `<i class="bi bi-hourglass-split me-2"></i>Análisis vigente — próximo en ${dias} días`;
  status.innerHTML = `<span class="text-muted"><i class="bi bi-info-circle me-1"></i>Plan del ${fechaGen} verificado · siguiente disponible en ${dias} días</span>`;
  if (chips) {
    chips.style.display = '';
    chips.innerHTML = `
      <span class="badge" style="background:#dcfce7;color:#166534;font-size:.7rem">
        <i class="bi bi-check2-circle me-1"></i>Verificado
      </span>
      <span class="badge" style="background:#f3f4f6;color:#374151;font-size:.7rem">
        <i class="bi bi-calendar3 me-1"></i>Plan vigente del ${fechaGen}
      </span>
    `;
  }
}

// Abre el modal de verificación con los objetivos del plan anterior.
async function planAbrirModalVerificar() {
  const e = _PLAN_IA_ESTADO || {};
  const ult = e.ultimo_plan;
  if (!ult || !ult.id) {
    ilusToast('No hay plan anterior por verificar', { type: 'info' });
    return;
  }

  // Necesitamos el JSON completo del plan anterior para listar los objetivos.
  // El endpoint /ia-plan-estado no lo devuelve por temas de payload. Pero el
  // plan ya quedó guardado en mant_ia_planes — para reconstruir objetivos en
  // la UI, sin gastar tokens, vamos a inferirlos de la última render del plan
  // (si el usuario lo vio antes). Si no hay render previo en esta sesión,
  // listamos un objetivo genérico ("Plan IA del DD/MM").
  let objetivos = [];
  const rendered = window._PLAN_IA_ULTIMO_RENDER;
  if (rendered && rendered.plan_id === ult.id) {
    const p = rendered.plan || {};
    // 1) Próxima visita sugerida
    if (p.proxima_visita && p.proxima_visita.razon) {
      objetivos.push({
        texto: `Realizar visita ${p.proxima_visita.tipo || ''} sugerida para ${p.proxima_visita.fecha_sugerida || ''}: ${p.proxima_visita.razon}`,
      });
    }
    // 2) Propuestas de mejora
    (p.propuestas_mejora || []).slice(0, 6).forEach(pm => {
      objetivos.push({ texto: pm.titulo || pm.descripcion || 'Propuesta de mejora' });
    });
    // 3) Recomendaciones equipos (solo las urgentes/atencion)
    (p.recomendaciones_equipos || []).filter(r => r.estado !== 'ok').slice(0, 4).forEach(r => {
      objetivos.push({ texto: `${r.equipo}: ${r.accion} (${r.plazo || ''})` });
    });
    // 4) Oportunidades comerciales
    (p.oportunidades_comerciales || []).slice(0, 3).forEach(oc => {
      objetivos.push({ texto: typeof oc === 'string' ? oc : (oc.titulo || JSON.stringify(oc)) });
    });
  }

  // Fallback: si la sesión actual no tiene el render del plan, ofrecer
  // un objetivo genérico para que el usuario aún pueda verificar.
  if (objetivos.length === 0) {
    objetivos = [
      { texto: `Plan IA del ${(ult.generado_at || '').slice(0,10)} — cumplimiento general` }
    ];
  }

  // Render del modal
  const cont = document.getElementById('vpObjetivos');
  const info = document.getElementById('vpInfoPlan');
  info.innerHTML = `
    <div><strong>Plan #${ult.id}</strong> generado el ${(ult.generado_at || '').slice(0,16).replace('T',' ')}</div>
    <div>Generado por: ${ult.generado_por || '—'} · Antigüedad: ${ult.edad_dias} días</div>
  `;
  cont.innerHTML = objetivos.map((o, idx) => `
    <div class="vp-obj p-2 rounded mb-2"
         style="border:1px solid #e5e7eb;background:#fff">
      <div class="form-check mb-2">
        <input class="form-check-input" type="checkbox" id="vp_obj_${idx}" checked>
        <label class="form-check-label fw-semibold" for="vp_obj_${idx}" style="font-size:.86rem">
          ${o.texto.replace(/</g,'&lt;')}
        </label>
      </div>
      <textarea class="form-control" id="vp_obj_${idx}_txt" rows="2"
                placeholder="Evidencia si se cumplió (ej: visita #156 el 15/05) o razón si no se cumplió"
                style="font-size:.82rem"></textarea>
    </div>
  `).join('');
  document.getElementById('vpNotas').value = '';
  // Guardar referencia para usar en confirmación
  window._VP_PLAN_ID = ult.id;
  window._VP_OBJETIVOS = objetivos;
  new bootstrap.Modal(document.getElementById('modalVerificarPlan')).show();
}

// Envía la verificación al backend y refresca el estado.
async function planConfirmarVerificacion() {
  const planId = window._VP_PLAN_ID;
  const objetivos = window._VP_OBJETIVOS || [];
  if (!planId) {
    ilusToast('No hay plan a verificar', { type: 'error' });
    return;
  }
  const payload = objetivos.map((o, idx) => {
    const cumplido = document.getElementById(`vp_obj_${idx}`).checked;
    const txt = (document.getElementById(`vp_obj_${idx}_txt`).value || '').trim();
    return cumplido
      ? { texto: o.texto, cumplido: true, evidencia: txt }
      : { texto: o.texto, cumplido: false, razon: txt };
  });
  const notas = (document.getElementById('vpNotas').value || '').trim();

  const btn = document.getElementById('vpBtnConfirmar');
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner-border spinner-border-sm me-2"></span>Guardando…';
  try {
    const r = await fetch(`/mantenciones/api/clientes/${CID}/ia-plan-verificar`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ plan_id: planId, objetivos_cumplidos: payload, notas }),
    });
    const data = await r.json();
    if (!r.ok || !data.ok) {
      ilusToast(data.error || 'Error al guardar la verificación', { type: 'error' });
      btn.disabled = false;
      btn.innerHTML = '<i class="bi bi-check2-circle me-1"></i>Confirmar cumplimiento';
      return;
    }
    bootstrap.Modal.getInstance(document.getElementById('modalVerificarPlan')).hide();
    ilusToast('Verificación guardada', { type: 'success' });
    await planActualizarEstadoIA();
    // Si tras verificar ya puede regenerar (caso info nueva), avisar al usuario.
    if (data.ya_puede_regenerar) {
      const ok = await ilusConfirm({
        title: '¿Generar análisis nuevo?',
        message: 'El plan anterior quedó verificado. Hay información nueva relevante.',
        sub: '¿Quieres generar un análisis IA actualizado ahora?',
        okLabel: 'Sí, generar', cancelLabel: 'Más tarde',
      });
      if (ok) await generarPlanMejora();
    }
  } catch(e) {
    ilusToast('Error de conexión: ' + e.message, { type: 'error' });
    btn.disabled = false;
    btn.innerHTML = '<i class="bi bi-check2-circle me-1"></i>Confirmar cumplimiento';
  }
}

// Llama al endpoint pesado (gasta tokens si no hay cache RAM en backend).
async function generarPlanMejora() {
  const btn = document.getElementById('btnGenerarPlan');
  const spinner = document.getElementById('planSpinner');
  const resultado = document.getElementById('planResultado');

  btn.disabled = true;
  btn.innerHTML = '<span class="spinner-border spinner-border-sm me-2"></span>Generando…';
  spinner.style.display = '';
  resultado.style.display = 'none';

  try {
    const url = `/mantenciones/api/clientes/${CID}/plan-mejora`;
    const r = await fetch(url, { method: 'POST' });
    const data = await r.json();

    spinner.style.display = 'none';

    if (r.status === 409) {
      // Bloqueo de política (no verificado / dentro de 6 meses)
      resultado.style.display = '';
      resultado.innerHTML = `<div class="alert alert-warning"><i class="bi bi-shield-exclamation me-1"></i>${data.error || 'No se puede regenerar ahora'}</div>`;
      await planActualizarEstadoIA();
      return;
    }

    if (!data.ok) {
      resultado.style.display = '';
      resultado.innerHTML = `<div class="alert alert-danger"><i class="bi bi-exclamation-triangle me-1"></i>${data.error || 'Error al generar plan'}</div>`;
      await planActualizarEstadoIA();
      return;
    }

    // Guardar referencia para que el modal de verificación pueda listar objetivos
    window._PLAN_IA_ULTIMO_RENDER = { plan_id: data.plan_id, plan: data.plan };
    renderPlan(data.plan, data.cliente, data);
    resultado.style.display = '';
    document.getElementById('planResultado').dataset.rendered = '1';
    // Refrescar estado del botón (ahora muestra "vigente" porque hay plan nuevo no verificado)
    await planActualizarEstadoIA();

  } catch(e) {
    spinner.style.display = 'none';
    resultado.style.display = '';
    resultado.innerHTML = `<div class="alert alert-danger">Error de conexión: ${e.message}</div>`;
    await planActualizarEstadoIA();
  }
}

// 2026-06-06 (Daniel) — IA eliminada de la ficha del cliente.
// El hook que auto-cargaba el estado del plan IA (al abrir el tab "IA & Plan"
// o si quedaba guardado en localStorage) quedó NEUTRALIZADO: ya no existe ese
// tab, así que no se dispara ninguna llamada a la IA al cargar la ficha.
(function _planEstadoIniHook() {
  return; // no-op: tab IA eliminado
})();

// SVG gauge sencillo (sin librerías) — círculo con stroke-dasharray.
// Mobile-first: 88x88 (44+44, cumple touch target). En desktop crece via flex.
function _ilusSvgGauge(value, label, color) {
  const v = Math.max(0, Math.min(100, Number(value) || 0));
  const radius = 36;
  const circ = 2 * Math.PI * radius;
  const offset = circ * (1 - v / 100);
  return `<div style="position:relative;width:88px;height:88px;flex-shrink:0">
    <svg width="88" height="88" viewBox="0 0 88 88" style="transform:rotate(-90deg)">
      <circle cx="44" cy="44" r="${radius}" fill="none" stroke="#e5e7eb" stroke-width="7"/>
      <circle cx="44" cy="44" r="${radius}" fill="none" stroke="${color}" stroke-width="7"
              stroke-linecap="round" stroke-dasharray="${circ.toFixed(2)}"
              stroke-dashoffset="${offset.toFixed(2)}"
              style="transition:stroke-dashoffset .6s ease"/>
    </svg>
    <div style="position:absolute;inset:0;display:flex;flex-direction:column;align-items:center;justify-content:center">
      <div style="font-size:1.35rem;font-weight:900;color:${color};line-height:1">${value ?? '—'}</div>
      <div style="font-size:.52rem;text-transform:uppercase;letter-spacing:.5px;color:#6b7280;font-weight:700;margin-top:2px">${label}</div>
    </div>
  </div>`;
}

// Formateo CLP compacto: 850000 → "$850.000"
function _ilusClp(n) {
  if (n == null || isNaN(Number(n))) return '';
  return '$' + Number(n).toLocaleString('es-CL');
}

function renderPlan(p, clienteNombre, envelope) {
  envelope = envelope || {};
  const estadoColor = {bueno:'#16a34a', regular:'#ea580c', critico:'#dc2626'}[p.estado_flota] || '#6b7280';
  const ringColor = (n) => n >= 70 ? '#16a34a' : n >= 40 ? '#ea580c' : '#dc2626';
  const impactoColor = {alto:'#dc2626', medio:'#ea580c', bajo:'#16a34a'};
  const catIcon = {contrato:'bi-file-earmark-text', equipo:'bi-bicycle', proceso:'bi-gear', costos:'bi-currency-dollar'};

  // Banda informativa cache + refresh
  const cached = !!envelope.cached;
  const meta = envelope.meta || {};
  const cacheBadge = cached
    ? `<span style="background:#fef3c7;color:#92400e;padding:2px 8px;border-radius:50px;font-size:.65rem;font-weight:700;margin-left:6px"
              title="Resultado en caché (TTL 1h). Click 'Refrescar' para regenerar.">
         <i class="bi bi-cloud-check"></i> Cache · ${Math.round((envelope.cache_age_seconds||0)/60)} min
       </span>`
    : `<span style="background:#dcfce7;color:#166534;padding:2px 8px;border-radius:50px;font-size:.65rem;font-weight:700;margin-left:6px">
         <i class="bi bi-stars"></i> Recién generado${meta.elapsed_ms ? ` · ${(meta.elapsed_ms/1000).toFixed(1)}s` : ''}
       </span>`;

  let html = `<div class="plan-result-card">`;

  // ── Resumen ejecutivo ──
  html += `<div class="plan-section" style="background:linear-gradient(135deg,#f8faff,#f0fdf4)">
    <div class="d-flex align-items-center justify-content-between gap-2 mb-2 flex-wrap">
      <div style="font-size:.72rem;font-weight:800;text-transform:uppercase;letter-spacing:.5px;color:#7c3aed">
        Plan generado por IA
        ${cacheBadge}
      </div>
      <button onclick="planAccionIA()"
              class="btn btn-sm btn-outline-secondary"
              title="Genera un análisis nuevo si la política IA lo permite (verificación + 6 meses o info nueva)"
              style="font-size:.72rem">
        <i class="bi bi-arrow-clockwise me-1"></i>Revisar / actualizar
      </button>
    </div>
    <div class="d-flex align-items-center gap-3 flex-wrap">
      <div class="health-ring" style="background:${ringColor(p.indice_salud)}">
        <span class="health-num">${p.indice_salud ?? '—'}</span>
        <span class="health-label">Salud</span>
      </div>
      ${typeof p.indice_cumplimiento_sla === 'number' ? `
      <div class="health-ring" style="background:${ringColor(p.indice_cumplimiento_sla)}" title="Cumplimiento de frecuencia contractual vs real">
        <span class="health-num">${p.indice_cumplimiento_sla}</span>
        <span class="health-label">SLA</span>
      </div>` : ''}
      ${typeof p.indice_cobranza === 'number' ? `
      <div class="health-ring" style="background:${ringColor(p.indice_cobranza)}" title="% facturado vs prestado (últimos 365 días)">
        <span class="health-num">${p.indice_cobranza}</span>
        <span class="health-label">Cobranza</span>
      </div>` : ''}
      <div style="flex:1;min-width:200px">
        <div class="d-flex align-items-center gap-2 mb-1 flex-wrap">
          <span style="background:${estadoColor};color:#fff;font-size:.6rem;padding:1px 8px;border-radius:50px;font-weight:700">${(p.estado_flota||'').toUpperCase()}</span>
          ${p.tipo_cliente_inferido ? `<span style="background:#e0e7ff;color:#3730a3;font-size:.6rem;padding:1px 8px;border-radius:50px;font-weight:700">Tipo: ${p.tipo_cliente_inferido}</span>` : ''}
        </div>
        <p style="font-size:.84rem;color:#374151;margin:0;line-height:1.55">${p.resumen_ejecutivo || ''}</p>
      </div>
    </div>
  </div>`;

  // ── Próxima visita sugerida ──
  if (p.proxima_visita) {
    const pv = p.proxima_visita;
    const prioColor = {alta:'#dc2626', media:'#ea580c', baja:'#16a34a'}[pv.prioridad] || '#6b7280';
    html += `<div class="plan-section">
      <div class="plan-section-title" style="color:#2563eb"><i class="bi bi-calendar-event me-1"></i>Próxima visita sugerida</div>
      <div class="vis-prox-card">
        <div class="d-flex align-items-center gap-3 flex-wrap">
          <div style="text-align:center;min-width:80px">
            <div style="font-size:1.3rem;font-weight:900;color:#1d4ed8">${pv.fecha_sugerida || '—'}</div>
            <div style="font-size:.62rem;text-transform:uppercase;color:#6b7280;letter-spacing:.5px">${pv.tipo || ''}</div>
          </div>
          <div style="flex:1;min-width:0">
            <div style="font-size:.82rem;color:#374151">${pv.razon || ''}</div>
            <div class="d-flex gap-2 mt-1 flex-wrap">
              <span style="background:${prioColor};color:#fff;font-size:.6rem;padding:1px 8px;border-radius:50px;font-weight:700">Prioridad ${(pv.prioridad||'').toUpperCase()}</span>
              ${pv.duracion_horas ? `<span style="font-size:.72rem;color:#6b7280"><i class="bi bi-clock me-1"></i>${pv.duracion_horas}h estimadas</span>` : ''}
            </div>
          </div>
        </div>
      </div>
    </div>`;
  }

  // ── Alertas críticas ──
  if (p.alertas_criticas?.length) {
    html += `<div class="plan-section">
      <div class="plan-section-title" style="color:#dc2626"><i class="bi bi-exclamation-triangle-fill me-1"></i>Alertas críticas</div>
      ${p.alertas_criticas.map(a => `<div class="alerta-item"><i class="bi bi-exclamation-circle-fill flex-shrink-0"></i>${a}</div>`).join('')}
    </div>`;
  }

  // ── Recomendaciones por equipo ──
  if (p.recomendaciones_equipos?.length) {
    html += `<div class="plan-section">
      <div class="plan-section-title" style="color:#2563eb"><i class="bi bi-bicycle me-1"></i>Recomendaciones por equipo</div>`;
    p.recomendaciones_equipos.forEach(rec => {
      const dc = {ok:'rec-ok', atencion:'rec-atencion', urgente:'rec-urgente'}[rec.estado] || 'rec-ok';
      const monto = (typeof rec.monto_estimado_clp === 'number') ? _ilusClp(rec.monto_estimado_clp) : '';
      html += `<div class="rec-item">
        <div class="rec-dot ${dc}" style="margin-top:4px"></div>
        <div style="flex:1;min-width:0">
          <div class="fw-semibold" style="font-size:.82rem">${rec.equipo}</div>
          <div style="font-size:.78rem;color:#374151;margin-top:2px">${rec.accion}</div>
          ${monto ? `<div style="font-size:.7rem;color:#7c2d12;font-weight:700;margin-top:3px"><i class="bi bi-cash me-1"></i>${monto}</div>` : ''}
        </div>
        <span style="font-size:.65rem;background:#f3f4f6;color:#374151;padding:1px 7px;border-radius:50px;font-weight:600;white-space:nowrap">${rec.plazo}</span>
      </div>`;
    });
    html += `</div>`;
  }

  // ── Proyección 12 meses ──
  if (p.proyeccion_12_meses?.length) {
    html += `<div class="plan-section">
      <div class="plan-section-title" style="color:#7c3aed"><i class="bi bi-bar-chart-line me-1"></i>Proyección 12 meses</div>
      <div style="max-height:280px;overflow-y:auto">`;
    p.proyeccion_12_meses.forEach((m, idx) => {
      const tipoCls = `proj-${m.tipo_visita}`;
      // Calcular fecha ISO aproximada para este mes
      const ahora = new Date();
      ahora.setMonth(ahora.getMonth() + idx);
      const fechaISO = ahora.toISOString().split('T')[0];
      const esAgendable = m.tipo_visita && m.tipo_visita !== 'ninguna';
      html += `<div class="proj-row">
        <span class="proj-mes">${m.mes}</span>
        <span class="proj-tipo ${tipoCls}">${m.tipo_visita}</span>
        <span style="flex:1;font-size:.79rem;color:#374151">${m.descripcion}</span>
        ${m.costo_estimado ? `<span style="font-size:.72rem;font-weight:700;color:#374151;white-space:nowrap;margin-right:6px">$${Number(m.costo_estimado).toLocaleString('es-CL')}</span>` : ''}
        ${esAgendable ? `<button onclick="agendarDesdeProyeccion('${fechaISO}', null)"
          style="background:#2563eb;color:#fff;border:none;border-radius:6px;padding:2px 10px;font-size:.65rem;font-weight:700;cursor:pointer;white-space:nowrap">
          <i class="bi bi-calendar-plus"></i> Agendar</button>` : ''}
      </div>`;
    });
    html += `</div></div>`;
  }

  // ── Propuestas de mejora ──
  if (p.propuestas_mejora?.length) {
    html += `<div class="plan-section">
      <div class="plan-section-title" style="color:#7c3aed"><i class="bi bi-lightbulb me-1"></i>Propuestas de mejora</div>`;
    p.propuestas_mejora.forEach(prop => {
      const ic = catIcon[prop.categoria] || 'bi-lightbulb';
      const col = impactoColor[prop.impacto] || '#6b7280';
      const monto = (typeof prop.monto_estimado_clp === 'number') ? _ilusClp(prop.monto_estimado_clp) : '';
      html += `<div class="prop-card prop-impacto-${prop.impacto}">
        <div class="d-flex align-items-start gap-2">
          <i class="bi ${ic}" style="color:${col};font-size:.9rem;flex-shrink:0;margin-top:2px"></i>
          <div style="flex:1">
            <div class="fw-bold" style="font-size:.83rem">${prop.titulo}</div>
            <div style="font-size:.78rem;color:#374151;margin-top:3px">${prop.descripcion}</div>
            <div class="mt-1 d-flex align-items-center gap-2 flex-wrap">
              <span style="background:${col};color:#fff;font-size:.6rem;padding:1px 7px;border-radius:50px;font-weight:700">Impacto ${prop.impacto}</span>
              <span style="font-size:.68rem;color:#9ca3af">${prop.categoria}</span>
              ${monto ? `<span style="font-size:.7rem;color:#7c2d12;font-weight:700"><i class="bi bi-cash me-1"></i>${monto}</span>` : ''}
            </div>
          </div>
        </div>
      </div>`;
    });
    html += `</div>`;
  }

  // ── Oportunidades comerciales ──
  // Compat: ahora cada item puede ser string (legacy) o {titulo,descripcion,monto_estimado_clp}.
  if (p.oportunidades_comerciales?.length) {
    html += `<div class="plan-section">
      <div class="plan-section-title" style="color:#16a34a"><i class="bi bi-graph-up-arrow me-1"></i>Oportunidades comerciales</div>`;
    p.oportunidades_comerciales.forEach(o => {
      if (typeof o === 'string') {
        html += `<div class="oport-item"><i class="bi bi-check-circle-fill flex-shrink-0"></i>${o}</div>`;
      } else if (o && typeof o === 'object') {
        const monto = (typeof o.monto_estimado_clp === 'number') ? _ilusClp(o.monto_estimado_clp) : '';
        html += `<div class="oport-item" style="flex-direction:column;align-items:stretch">
          <div class="d-flex align-items-start gap-2 w-100">
            <i class="bi bi-check-circle-fill flex-shrink-0"></i>
            <div style="flex:1">
              <div class="fw-bold" style="font-size:.82rem">${o.titulo || ''}</div>
              ${o.descripcion ? `<div style="font-size:.76rem;color:#14532d;margin-top:2px">${o.descripcion}</div>` : ''}
            </div>
            ${monto ? `<span style="font-size:.72rem;color:#14532d;font-weight:800;white-space:nowrap;margin-left:6px"><i class="bi bi-cash me-1"></i>${monto}</span>` : ''}
          </div>
        </div>`;
      }
    });
    html += `</div>`;
  }

  // ── Riesgos financieros (NUEVO 2026-05-21) ──
  if (Array.isArray(p.riesgos_financieros) && p.riesgos_financieros.length) {
    html += `<div class="plan-section">
      <div class="plan-section-title" style="color:#dc2626"><i class="bi bi-cash-stack me-1"></i>Riesgos financieros</div>`;
    p.riesgos_financieros.forEach(r => {
      const monto = (typeof r.monto_estimado === 'number')
        ? `<span style="font-weight:800;color:#7c2d12;white-space:nowrap;margin-left:8px">$${Number(r.monto_estimado).toLocaleString('es-CL')}</span>`
        : '';
      const tipoLbl = (r.tipo || '').replace(/_/g, ' ');
      html += `<div class="alerta-item" style="background:#fef2f2;border-color:#fecaca">
        <i class="bi bi-exclamation-octagon-fill flex-shrink-0" style="color:#dc2626"></i>
        <div style="flex:1">
          <div style="font-size:.7rem;text-transform:uppercase;letter-spacing:.4px;color:#991b1b;font-weight:800">${tipoLbl}</div>
          <div style="font-size:.82rem;color:#374151">${r.detalle || ''}</div>
        </div>
        ${monto}
      </div>`;
    });
    html += `</div>`;
  }

  // ── Renovación de contrato (NUEVO 2026-05-21) ──
  if (p.renovacion_contrato && (p.renovacion_contrato.aplica || p.renovacion_contrato.dias_para_vencer != null)) {
    const rc = p.renovacion_contrato;
    const rec = !!rc.recomendar_renovar;
    const dias = rc.dias_para_vencer;
    const urgente = (typeof dias === 'number' && dias <= 60);
    const bgGrad = rec
      ? 'linear-gradient(135deg,#f0fdf4,#dcfce7)'
      : 'linear-gradient(135deg,#fafafa,#f3f4f6)';
    html += `<div class="plan-section" style="background:${bgGrad}">
      <div class="plan-section-title" style="color:${rec ? '#166534' : '#475569'}">
        <i class="bi bi-file-earmark-medical me-1"></i>Renovación de contrato
      </div>
      <div class="d-flex align-items-center gap-3 flex-wrap">
        ${dias != null ? `
          <div style="text-align:center;min-width:90px">
            <div style="font-size:1.5rem;font-weight:900;color:${urgente ? '#dc2626' : '#475569'}">${dias}</div>
            <div style="font-size:.62rem;text-transform:uppercase;color:#6b7280;letter-spacing:.5px">días para vencer</div>
          </div>` : ''}
        <div style="flex:1;min-width:220px">
          <div class="mb-1">
            <span style="background:${rec ? '#16a34a' : '#6b7280'};color:#fff;font-size:.62rem;padding:2px 10px;border-radius:50px;font-weight:700">
              ${rec ? '✓ Recomendar renovar' : '⊘ No recomendar renovar'}
            </span>
          </div>
          ${Array.isArray(rc.argumentos) ? rc.argumentos.map(a => `
            <div style="font-size:.78rem;color:#374151;margin:3px 0;display:flex;gap:6px;align-items:flex-start">
              <i class="bi bi-dot" style="font-size:1rem;flex-shrink:0;color:${rec?'#16a34a':'#9ca3af'}"></i>
              <span>${a}</span>
            </div>`).join('') : ''}
        </div>
      </div>
    </div>`;
  }

  // ════════════════════════════════════════════════════════════════════
  // NUEVAS SECCIONES — Trazabilidad profunda (2026-05-21)
  // ════════════════════════════════════════════════════════════════════

  // ── Score Global (4 gauges SVG) ──
  if (p.score_global && typeof p.score_global === 'object') {
    const sg = p.score_global;
    const items = [
      { key: 'salud_operacional', lbl: 'Operacional' },
      { key: 'cumplimiento_sla',  lbl: 'SLA' },
      { key: 'salud_financiera',  lbl: 'Financiera' },
      { key: 'promedio',          lbl: 'Promedio' },
    ].filter(it => typeof sg[it.key] === 'number');
    if (items.length) {
      html += `<div class="plan-section" style="background:linear-gradient(135deg,#fafbff,#f0fdf4)">
        <div class="plan-section-title" style="color:#7c3aed"><i class="bi bi-speedometer2 me-1"></i>Score global</div>
        <div class="d-flex align-items-center gap-3 flex-wrap" style="justify-content:center">
          ${items.map(it => _ilusSvgGauge(sg[it.key], it.lbl, ringColor(sg[it.key]))).join('')}
        </div>
      </div>`;
    }
  }

  // ── Deuda técnica (NUEVO) ──
  const dt = p.deuda_tecnica;
  if (dt && typeof dt === 'object' && (
        (typeof dt.equipos_sin_intervencion_reciente === 'number' && dt.equipos_sin_intervencion_reciente > 0)
        || dt.edad_promedio_parque_anios != null
        || (Array.isArray(dt.componentes_obsoletos_detectados) && dt.componentes_obsoletos_detectados.length)
        || (typeof dt.monto_estimado_modernizacion_clp === 'number' && dt.monto_estimado_modernizacion_clp > 0)
      )) {
    const edad = dt.edad_promedio_parque_anios;
    const edadColor = (typeof edad === 'number')
      ? (edad >= 7 ? '#dc2626' : edad >= 4 ? '#ea580c' : '#16a34a')
      : '#6b7280';
    const monto = dt.monto_estimado_modernizacion_clp;
    html += `<div class="plan-section" style="background:#fff7ed">
      <div class="plan-section-title" style="color:#9a3412"><i class="bi bi-tools me-1"></i>Deuda técnica del parque</div>
      <div class="d-flex align-items-center gap-3 flex-wrap mb-2">
        ${(typeof edad === 'number') ? `
          <div style="text-align:center;min-width:90px">
            <div style="font-size:1.6rem;font-weight:900;color:${edadColor};line-height:1">${edad}</div>
            <div style="font-size:.6rem;text-transform:uppercase;color:#6b7280;letter-spacing:.5px">años edad promedio</div>
          </div>` : ''}
        ${(typeof dt.equipos_sin_intervencion_reciente === 'number') ? `
          <div style="text-align:center;min-width:90px">
            <div style="font-size:1.6rem;font-weight:900;color:#9a3412;line-height:1">${dt.equipos_sin_intervencion_reciente}</div>
            <div style="font-size:.6rem;text-transform:uppercase;color:#6b7280;letter-spacing:.5px">eq. sin intervención &gt;12m</div>
          </div>` : ''}
        ${(typeof monto === 'number' && monto > 0) ? `
          <div style="flex:1;min-width:160px;background:#fff;border:1px solid #fed7aa;border-radius:8px;padding:10px 14px">
            <div style="font-size:.62rem;text-transform:uppercase;color:#9a3412;font-weight:800;letter-spacing:.4px">Modernización estimada</div>
            <div style="font-size:1.05rem;font-weight:900;color:#7c2d12">${_ilusClp(monto)}</div>
          </div>` : ''}
      </div>
      ${(Array.isArray(dt.componentes_obsoletos_detectados) && dt.componentes_obsoletos_detectados.length) ? `
        <div style="margin-top:6px;font-size:.78rem;color:#7c2d12">
          <span style="font-weight:700">Componentes obsoletos detectados:</span>
          <ul style="margin:4px 0 0 18px;padding:0;font-size:.76rem;color:#374151">
            ${dt.componentes_obsoletos_detectados.slice(0,8).map(c => `<li>${c}</li>`).join('')}
          </ul>
        </div>` : ''}
    </div>`;
  }

  // ── Patrón de fallas (NUEVO) ──
  const pf = p.patron_fallas;
  if (pf && typeof pf === 'object'
      && Array.isArray(pf.equipos_problematicos)
      && pf.equipos_problematicos.length) {
    html += `<div class="plan-section" style="background:#fef2f2">
      <div class="plan-section-title" style="color:#991b1b"><i class="bi bi-graph-down-arrow me-1"></i>Patrón de fallas recurrentes</div>`;
    pf.equipos_problematicos.slice(0, 8).forEach(eq => {
      html += `<div class="alerta-item" style="background:#fff;border-color:#fecaca">
        <i class="bi bi-arrow-repeat flex-shrink-0" style="color:#dc2626"></i>
        <div style="flex:1">
          <div style="font-weight:800;font-size:.85rem;color:#7f1d1d">${eq.nombre || 'Equipo'}</div>
          <div style="font-size:.76rem;color:#991b1b;margin-top:2px">${eq.diagnostico || ''}</div>
        </div>
        <span style="background:#dc2626;color:#fff;font-size:.65rem;padding:2px 8px;border-radius:50px;font-weight:700;white-space:nowrap">
          ${eq.reparaciones_12m || '?'} rep / 12m
        </span>
      </div>`;
    });
    if (Array.isArray(pf.tipo_fallas_mas_comunes) && pf.tipo_fallas_mas_comunes.length) {
      html += `<div style="margin-top:6px;font-size:.76rem;color:#7f1d1d">
        <span style="font-weight:700">Tipos de falla más comunes:</span>
        ${pf.tipo_fallas_mas_comunes.map(t => `<span style="background:#fee2e2;color:#991b1b;padding:2px 8px;border-radius:50px;font-size:.7rem;font-weight:700;margin-right:4px">${t}</span>`).join('')}
      </div>`;
    }
    html += `</div>`;
  }

  // ── Seguimiento planes anteriores (NUEVO) ──
  if (Array.isArray(p.seguimiento_planes_anteriores) && p.seguimiento_planes_anteriores.length) {
    html += `<div class="plan-section" style="background:#fafaff">
      <div class="plan-section-title" style="color:#6d28d9"><i class="bi bi-clock-history me-1"></i>Seguimiento de planes anteriores</div>`;
    p.seguimiento_planes_anteriores.slice(0, 10).forEach(sp => {
      const cumplido = !!sp.cumplido;
      const bg = cumplido ? '#f0fdf4' : '#fef2f2';
      const border = cumplido ? '#bbf7d0' : '#fecaca';
      const ic = cumplido ? 'bi-check-circle-fill' : 'bi-x-circle-fill';
      const col = cumplido ? '#16a34a' : '#dc2626';
      html += `<div style="background:${bg};border:1px solid ${border};border-radius:8px;padding:10px 12px;margin-bottom:6px">
        <div class="d-flex align-items-start gap-2">
          <i class="bi ${ic} flex-shrink-0" style="color:${col};font-size:.95rem;margin-top:2px"></i>
          <div style="flex:1;min-width:0">
            <div style="font-size:.82rem;font-weight:700;color:#374151">${sp.titulo || ''}</div>
            ${(!cumplido && sp.razon_no_cumplimiento) ? `<div style="font-size:.74rem;color:#991b1b;margin-top:2px"><span style="font-weight:700">Por qué no:</span> ${sp.razon_no_cumplimiento}</div>` : ''}
            ${sp.accion_actual ? `<div style="font-size:.74rem;color:#1e3a8a;margin-top:3px"><span style="font-weight:700">Acción:</span> ${sp.accion_actual}</div>` : ''}
          </div>
        </div>
      </div>`;
    });
    html += `</div>`;
  }

  // ── Footer técnico (debug/auditoría) ──
  if (meta && meta.model) {
    const ct = meta.context_size || {};
    html += `<div style="margin-top:10px;padding:8px 12px;background:#fafafa;border-radius:8px;font-size:.66rem;color:#9ca3af;text-align:center;line-height:1.5">
      ${meta.model}${meta.elapsed_ms ? ' · ' + (meta.elapsed_ms/1000).toFixed(1) + 's' : ''}${(meta.tokens_in||meta.tokens_out) ? ` · ${meta.tokens_in||0}↑/${meta.tokens_out||0}↓ tokens` : ''}
      · ctx: ${ct.maquinas||0} eq · ${ct.contratos_vigentes||0} ct · ${ct.visitas_completadas||0}+${ct.visitas_futuras||0} visitas${ct.garantias_proximas ? ' · ' + ct.garantias_proximas + ' gar.' : ''}${ct.lev_items ? ' · ' + ct.lev_items + ' lev' : ''}${ct.eventos_24m ? ' · ' + ct.eventos_24m + ' eventos' : ''}${ct.planes_previos ? ' · ' + ct.planes_previos + ' planes prev.' : ''}
    </div>`;
  }

  html += `</div>`; // /plan-result-card

  document.getElementById('planResultado').innerHTML = html;
}

// ════════════════════════════════════════════════════════════════════════
// UNIVERSAL DOCUMENT VIEWER — Visor universal de contratos (Google Drive-like)
// ════════════════════════════════════════════════════════════════════════
// REESCRITO 2026-05-26 (Daniel — iframe en blanco):
//   Antes: iframe simple → falla cuando Cloudinary devuelve attachment header.
//   Ahora: cadena de fallback resiliente:
//     1. HEAD al endpoint → obtener Content-Type real + size
//     2. Iframe nativo con detección de "iframe en blanco" a los 6 segundos
//     3. <object> tag como segundo intento (algunos browsers prefieren object)
//     4. PDF.js canvas — renderiza PDF página por página (NO depende de headers)
//     5. Image renderer dedicado para JPG/PNG/WEBP
//     6. Office Online Viewer para DOCX/XLSX/PPTX si hay Cloudinary HTTPS
//     7. Metadata-only con descarga (último recurso, estilo Notion)
//
// Estado global del visor (singleton). Se resetea cada vez que se abre.
const UDV = {
  ctid:      null,
  baseUrl:   '',
  nombre:    '',
  tipo:      '',
  hasCloud:  false,
  esSuperadmin: false,
  // Detector de iframe en blanco
  iframeLoadTimer: null,
  iframeBlankTimer: null,
  iframeFinishedLoading: false,
  // PDF.js state
  pdfDoc:       null,
  pdfPageNum:   1,
  pdfPagesTotal: 0,
  pdfZoom:      1.0,
  pdfRendering: false,
  pdfPendingPage: null,
  // Logs
  logs: [],
};

function _udvLog(level, ...args) {
  const ts = new Date().toISOString().slice(11, 23);
  const msg = `[UDV ${ts}] [${level}] ` + args.map(a =>
    typeof a === 'object' ? JSON.stringify(a) : String(a)
  ).join(' ');
  UDV.logs.push(msg);
  if (level === 'ERROR') console.error(msg);
  else if (level === 'WARN') console.warn(msg);
  else console.log(msg);
}

function _udvSetStep(text) {
  const el = document.getElementById('udvLoadingStep');
  if (el) el.textContent = text;
  _udvLog('INFO', 'Step:', text);
}

function _udvShowStage(stage) {
  // stages: loading | iframe | pdfjs | image | error
  // FIX CRITICO 2026-05-26: Bootstrap d-flex/d-block aplican !important,
  // por lo que el.style.display='none' NO oculta el loader. Usamos
  // setProperty con 'important' explicito para ganar la batalla CSS.
  const stages = {
    loading:  'udvLoading',
    iframe:   'contratoFrame',
    pdfjs:    'udvPdfjsContainer',
    image:    'udvImageContainer',
    error:    'contratoNoViewer',
  };
  const displayWhenActive = {
    loading:  'flex',   // ya viene como d-flex
    iframe:   'block',
    pdfjs:    'block',
    image:    'flex',
    error:    'flex',
  };
  Object.entries(stages).forEach(([k, id]) => {
    const el = document.getElementById(id);
    if (!el) return;
    if (k === stage) {
      el.style.setProperty('display', displayWhenActive[k], 'important');
    } else {
      el.style.setProperty('display', 'none', 'important');
    }
  });
}

function _udvSetBadge(text, color) {
  const b = document.getElementById('udvStatusBadge');
  if (!b) return;
  if (!text) { b.style.display = 'none'; return; }
  b.textContent = text;
  b.style.background = color || '#16a34a';
  b.style.display = '';
}

function _udvShowError(opts) {
  const { titulo, mensaje, icono, iconColor, mostrarReintentar, mostrarResubir, metadata, errorTech } = opts || {};
  document.getElementById('contratoNoViewerIcon').className = icono || 'bi bi-file-earmark-x';
  document.getElementById('contratoNoViewerIcon').style.color = iconColor || '#dc2626';
  document.getElementById('contratoNoViewerTitulo').textContent = titulo || 'Este contrato debe re-subirse';
  document.getElementById('contratoNoViewerMsg').innerHTML = mensaje ||
    'No se pudo mostrar el archivo. Re-súbelo para que vuelva a quedar disponible para todos.';
  const meta = document.getElementById('udvMetadata');
  const metaContent = document.getElementById('udvMetaContent');
  if (metadata) {
    metaContent.innerHTML = Object.entries(metadata).map(([k,v]) =>
      `<div><span style="opacity:.6">${k}:</span> <span>${v}</span></div>`
    ).join('');
    meta.style.display = '';
  } else {
    meta.style.display = 'none';
  }
  // Botón Reintentar (solo si se pidió explícito)
  const btnRet = document.getElementById('btnReintentarUDV');
  if (btnRet) btnRet.style.display = mostrarReintentar ? '' : 'none';
  // Botón Re-subir: solo si superadmin Y el archivo es re-subible (contrato),
  // NO para adjuntos genéricos (esos se re-suben desde el tab Documentos).
  const btnRes = document.getElementById('btnUDVResubir');
  if (btnRes) btnRes.style.display =
    (mostrarResubir && UDV.esSuperadmin && UDV.allowResubir) ? '' : 'none';
  const errTech = document.getElementById('udvErrorTech');
  if (errorTech) {
    errTech.textContent = errorTech + '\n\n' + UDV.logs.slice(-20).join('\n');
    errTech.style.display = '';
  } else {
    errTech.style.display = 'none';
  }
  _udvShowStage('error');
  _udvSetBadge('', '');
}

function udvReintentar() {
  // Re-ejecuta con el mismo estado actual del visor
  if (UDV.baseUrl) {
    verArchivoUDV({
      baseUrl:  UDV.baseUrl,
      ctid:     UDV.ctid,
      nombre:   UDV.nombre,
      tipo:     UDV.tipo,
      hasCloud: UDV.hasCloud,
      allowDownload: UDV.allowDownload,
      allowResubir:  UDV.allowResubir,
    });
  }
}

// Re-sube el contrato actualmente abierto en el visor. Cierra el modal
// y dispara el file picker — reSubirContrato() ya hace el resto.
function udvResubirActual() {
  if (!UDV.ctid) return;
  const m = bootstrap.Modal.getInstance(document.getElementById('modalVerContrato'));
  if (m) m.hide();
  reSubirContrato(UDV.ctid, UDV.nombre || `Contrato #${UDV.ctid}`);
}

// ─── HEAD probe: detecta el Content-Type real del archivo ─────────────
async function _udvHead(url) {
  try {
    const r = await fetch(url, { method: 'HEAD', credentials: 'same-origin' });
    if (!r.ok) {
      _udvLog('WARN', 'HEAD status', r.status);
      return null;
    }
    const info = {
      status:      r.status,
      contentType: r.headers.get('Content-Type') || '',
      length:      r.headers.get('Content-Length') || '',
      disposition: r.headers.get('Content-Disposition') || '',
    };
    _udvLog('INFO', 'HEAD result:', info);
    return info;
  } catch (e) {
    _udvLog('WARN', 'HEAD failed:', e.message);
    return null;
  }
}

// ─── Iframe loader con detección de "iframe en blanco" ─────────────────
function _udvLoadIframe(url, opts) {
  const { onLoaded, onBlank, onError, blankTimeoutMs = 6000 } = opts || {};
  const frame = document.getElementById('contratoFrame');

  // Limpiar timers anteriores
  if (UDV.iframeBlankTimer) clearTimeout(UDV.iframeBlankTimer);
  UDV.iframeFinishedLoading = false;

  // Reset iframe
  frame.src = 'about:blank';
  _udvShowStage('iframe');

  // Pequeño delay para asegurar reset
  setTimeout(() => {
    // Detector de iframe en blanco: si después de blankTimeoutMs no
    // recibimos onload, asumimos que el browser descargó el archivo
    // en lugar de renderizarlo (síntoma clásico de Content-Disposition
    // attachment forzado por el CDN).
    UDV.iframeBlankTimer = setTimeout(() => {
      if (!UDV.iframeFinishedLoading) {
        _udvLog('WARN', `Iframe blank after ${blankTimeoutMs}ms — switching to fallback`);
        if (onBlank) onBlank();
      }
    }, blankTimeoutMs);

    frame.onload = () => {
      UDV.iframeFinishedLoading = true;
      if (UDV.iframeBlankTimer) clearTimeout(UDV.iframeBlankTimer);
      _udvLog('INFO', 'Iframe loaded:', url);
      // Verificar si el iframe realmente cargó contenido (no about:blank ni error)
      try {
        // Acceso al contentDocument puede tirar SecurityError si es cross-origin
        // (que con nuestro proxy NO es). Si es same-origin y document está vacío,
        // entonces el browser descargó el archivo.
        const doc = frame.contentDocument;
        if (doc) {
          const docUrl = doc.URL || '';
          if (docUrl === 'about:blank' || docUrl === '') {
            _udvLog('WARN', 'Iframe document is about:blank — likely downloaded');
            if (onBlank) onBlank();
            return;
          }
        }
      } catch (e) {
        // Cross-origin: no podemos inspeccionar, asumimos OK (Cloudinary, etc.)
        _udvLog('INFO', 'Iframe is cross-origin (OK):', e.message);
      }
      if (onLoaded) onLoaded();
    };

    frame.onerror = (e) => {
      UDV.iframeFinishedLoading = true;
      if (UDV.iframeBlankTimer) clearTimeout(UDV.iframeBlankTimer);
      _udvLog('ERROR', 'Iframe error:', e);
      if (onError) onError(e);
    };

    frame.src = url;
  }, 30);
}

// ─── PDF.js: render PDF en canvas (sin depender de headers) ───────────
async function _udvLoadPdfJs(url) {
  if (!window.pdfjsLib) {
    _udvLog('ERROR', 'pdfjsLib no está cargado');
    throw new Error('PDF.js no disponible — recarga la página');
  }
  _udvSetStep('Renderizando PDF con PDF.js (Mozilla)…');
  _udvShowStage('loading');
  try {
    const loadingTask = window.pdfjsLib.getDocument({ url, withCredentials: true });
    UDV.pdfDoc = await loadingTask.promise;
    UDV.pdfPagesTotal = UDV.pdfDoc.numPages;
    UDV.pdfPageNum = 1;
    UDV.pdfZoom = 1.0;
    _udvLog('INFO', 'PDF.js loaded', { pages: UDV.pdfPagesTotal });
    _udvShowStage('pdfjs');
    _udvSetBadge(`PDF.js · ${UDV.pdfPagesTotal} pág.`, '#16a34a');
    await _udvRenderPdfPage();
  } catch (e) {
    _udvLog('ERROR', 'PDF.js failed:', e.message);
    throw e;
  }
}

async function _udvRenderPdfPage() {
  if (!UDV.pdfDoc || UDV.pdfRendering) {
    UDV.pdfPendingPage = UDV.pdfPageNum;
    return;
  }
  UDV.pdfRendering = true;
  try {
    const page = await UDV.pdfDoc.getPage(UDV.pdfPageNum);
    const canvas = document.getElementById('udvPdfjsCanvas');
    const ctx = canvas.getContext('2d');
    const viewport = page.getViewport({ scale: UDV.pdfZoom * 1.5 });
    canvas.width = viewport.width;
    canvas.height = viewport.height;
    canvas.style.width  = (viewport.width / 1.5) + 'px';
    canvas.style.height = (viewport.height / 1.5) + 'px';
    await page.render({ canvasContext: ctx, viewport }).promise;
    document.getElementById('udvPdfjsPageInfo').textContent =
      `${UDV.pdfPageNum} / ${UDV.pdfPagesTotal}`;
    document.getElementById('udvPdfjsZoom').textContent =
      Math.round(UDV.pdfZoom * 100) + '%';
    document.getElementById('udvBtnPrev').disabled = (UDV.pdfPageNum <= 1);
    document.getElementById('udvBtnNext').disabled = (UDV.pdfPageNum >= UDV.pdfPagesTotal);
  } finally {
    UDV.pdfRendering = false;
    if (UDV.pdfPendingPage !== null) {
      const p = UDV.pdfPendingPage;
      UDV.pdfPendingPage = null;
      UDV.pdfPageNum = p;
      _udvRenderPdfPage();
    }
  }
}

function udvPdfjsPrev() {
  if (UDV.pdfPageNum > 1) { UDV.pdfPageNum--; _udvRenderPdfPage(); }
}
function udvPdfjsNext() {
  if (UDV.pdfPageNum < UDV.pdfPagesTotal) { UDV.pdfPageNum++; _udvRenderPdfPage(); }
}
function udvPdfjsZoomIn() {
  UDV.pdfZoom = Math.min(3.0, UDV.pdfZoom + 0.25); _udvRenderPdfPage();
}
function udvPdfjsZoomOut() {
  UDV.pdfZoom = Math.max(0.5, UDV.pdfZoom - 0.25); _udvRenderPdfPage();
}

// ─── Image renderer dedicado ──────────────────────────────────────────
function _udvLoadImage(url) {
  _udvSetStep('Cargando imagen…');
  const img = document.getElementById('udvImage');
  img.src = '';
  img.onload = () => {
    _udvLog('INFO', 'Image loaded:', url);
    _udvShowStage('image');
    _udvSetBadge('Imagen', '#3b82f6');
  };
  img.onerror = () => {
    _udvLog('ERROR', 'Image failed to load');
    _udvShowError({
      titulo: 'Este contrato debe re-subirse',
      mensaje: UDV.esSuperadmin
        ? 'La imagen no se pudo cargar. Súbela de nuevo y quedará disponible.'
        : 'No se pudo cargar la imagen. Avisa al administrador para que la vuelva a subir.',
      icono: 'bi bi-cloud-arrow-up',
      iconColor: '#f59e0b',
      mostrarResubir: true,
    });
  };
  img.src = url;
}

// ─── ENTRY POINT GENERICO: visualizar cualquier archivo en el UDV ─────
// opts: { baseUrl, nombre, tipo, hasCloud, ctid (opcional), kind (opcional),
//         allowDownload (bool), allowResubir (bool) }
async function verArchivoUDV(opts) {
  const o = opts || {};
  UDV.ctid     = o.ctid || null;
  UDV.baseUrl  = o.baseUrl;
  UDV.nombre   = o.nombre || 'Documento';
  UDV.tipo     = o.tipo || '';
  UDV.hasCloud = !!o.hasCloud;
  UDV.esSuperadmin = !!DATA.is_superadmin;
  // allowDownload: si el documento permite descarga server-side
  UDV.allowDownload = (o.allowDownload === undefined) ? UDV.esSuperadmin : !!o.allowDownload;
  // allowResubir: si tiene sentido ofrecer el botón "Re-subir" (solo contratos)
  UDV.allowResubir  = (o.allowResubir === undefined) ? !!o.ctid : !!o.allowResubir;
  UDV.logs = [];
  return _verArchivoInterno();
}

// Wrapper retro-compatible para contratos (mantiene la firma vieja)
async function verContrato(ctid, nombre, tipo, hasCloud) {
  return verArchivoUDV({
    baseUrl:  `/mantenciones/api/contratos/${ctid}/archivo`,
    ctid:     ctid,
    nombre:   nombre || `Contrato #${ctid}`,
    tipo:     tipo,
    hasCloud: hasCloud,
    allowDownload: !!DATA.is_superadmin,
    allowResubir:  true,
  });
}

async function _verArchivoInterno() {
  const ctid   = UDV.ctid;
  const nombre = UDV.nombre;
  const tipo   = UDV.tipo;
  const hasCloud = UDV.hasCloud;
  if (UDV.iframeBlankTimer) clearTimeout(UDV.iframeBlankTimer);
  if (UDV.pdfDoc) { try { UDV.pdfDoc.destroy(); } catch(e){} UDV.pdfDoc = null; }

  _udvLog('INFO', 'verArchivoUDV start', { ctid, nombre, tipo, hasCloud,
          allowDownload: UDV.allowDownload, allowResubir: UDV.allowResubir });

  // Header del modal
  document.getElementById('modalVerContratoTitulo').innerHTML =
    `<i class="bi bi-file-earmark-text me-2"></i>${UDV.nombre}`;
  document.getElementById('btnAbrirContratoNueva').href = UDV.baseUrl;
  document.getElementById('btnAbrirNuevaFallback').href = UDV.baseUrl;
  const btnDl   = document.getElementById('btnDescargarContrato');
  const btnWord = document.getElementById('btnDescWord');
  // SEGURIDAD: Botones de descarga solo si esSuperadmin Y allowDownload.
  // Otros roles ni siquiera ven el botón "Abrir en pestaña nueva" para
  // archivos donde no tienen autorización de descarga (cumple regla:
  // solo superadmin puede bajar archivos del módulo mantenciones).
  const puedeDescargar = !!UDV.allowDownload && !!UDV.esSuperadmin;
  if (puedeDescargar) {
    btnDl.style.display = '';
    btnDl.href = UDV.baseUrl + '?download=1';
    btnWord.style.display = '';
    btnWord.href = UDV.baseUrl + '?download=1';
  } else {
    btnDl.style.display = 'none';
    btnWord.style.display = 'none';
  }
  // El botón "Abrir en pestaña nueva" también descarga, así que lo
  // ocultamos si no es superadmin.
  const btnOpen = document.getElementById('btnAbrirContratoNueva');
  const btnOpenF = document.getElementById('btnAbrirNuevaFallback');
  if (btnOpen)  btnOpen.style.display  = puedeDescargar ? '' : 'none';
  if (btnOpenF) btnOpenF.style.display = puedeDescargar ? '' : 'none';

  // Abrir modal y mostrar loading
  _udvShowStage('loading');
  _udvSetBadge('', '');
  _udvSetStep('Conectando con el servidor…');
  new bootstrap.Modal(document.getElementById('modalVerContrato')).show();

  // ── Normalizar tipo (Python None → string vacío) ─────────────────────
  const t = (tipo === 'None' ? '' : (tipo || '')).toLowerCase();
  const isPdfHint   = (t === 'pdf');
  const isImgHint   = ['imagen','jpg','jpeg','png','gif','webp'].includes(t);
  const isOfficeHint= ['word','doc','docx','xls','xlsx','ppt','pptx'].includes(t);

  // ── OPTIMIZACION VELOCIDAD 2026-05-26 ────────────────────────────────
  // Si el hint del template ya nos da el tipo (caso normal: archivo_tipo
  // está en BD), saltamos el HEAD probe completamente y cargamos directo.
  // El HEAD agregaba ~200-500ms innecesarios en cada apertura.
  //
  // Solo hacemos HEAD probe si el tipo es desconocido (caso edge: contratos
  // legacy sin archivo_tipo). En ese caso, el HEAD nos dice qué es.
  let headInfo = null;
  let realMime = '';
  const tipoEsConocido = isPdfHint || isImgHint || isOfficeHint;

  if (!tipoEsConocido) {
    _udvSetStep('Detectando tipo de archivo…');
    headInfo = await _udvHead(UDV.baseUrl);
    if (headInfo && headInfo.status >= 400) {
      _udvShowError({
        titulo: 'Este contrato debe re-subirse',
        mensaje: UDV.esSuperadmin
          ? 'El archivo ya no está disponible en el servidor. Súbelo de nuevo con el botón de abajo y quedará operando al instante.'
          : 'El archivo ya no está disponible. Avisa al administrador para que lo vuelva a subir.',
        icono: 'bi bi-cloud-arrow-up',
        iconColor: '#f59e0b',
        mostrarResubir: true,
      });
      return;
    }
    realMime = (headInfo && headInfo.contentType || '').toLowerCase();
  } else {
    _udvLog('INFO', 'Skip HEAD probe — tipo conocido:', t);
  }

  const isPdf    = realMime.includes('pdf')        || isPdfHint;
  const isImg    = realMime.startsWith('image/')   || isImgHint;
  const isOffice = realMime.includes('msword') ||
                   realMime.includes('officedocument') ||
                   realMime.includes('ms-excel') ||
                   realMime.includes('ms-powerpoint') ||
                   isOfficeHint;

  _udvLog('INFO', 'Tipo decidido', { realMime, isPdf, isImg, isOffice, hint: t });

  // ── RUTA 1: Office docs con Cloudinary → Office Online Viewer ────────
  if (isOffice && UDV.hasCloud) {
    _udvSetStep('Cargando documento Word/Excel…');
    _udvLoadIframe(UDV.baseUrl + '?viewer=office', {
      onLoaded: () => {
        _udvLog('INFO', 'Office Viewer cargado');
        _udvSetBadge('Office Online', '#3b82f6');
      },
      onBlank: () => {
        _udvShowError({
          titulo: 'Este contrato debe re-subirse',
          mensaje: UDV.esSuperadmin
            ? 'El visor de Word/Excel no pudo cargar el archivo. Súbelo de nuevo y volverá a quedar disponible.'
            : 'No se pudo mostrar el archivo. Avisa al administrador para que lo vuelva a subir.',
          icono: 'bi bi-cloud-arrow-up',
          iconColor: '#f59e0b',
          mostrarResubir: true,
        });
      },
      blankTimeoutMs: 10000,  // Office viewer es más lento
    });
    return;
  }

  // ── RUTA 2: Office docs SIN Cloudinary → no se puede previsualizar ──
  if (isOffice && !UDV.hasCloud) {
    _udvShowError({
      titulo: 'Este contrato debe re-subirse',
      mensaje: UDV.esSuperadmin
        ? 'Este documento Word/Excel todavía no está en la nube. Vuelve a subirlo y quedará disponible para todos al instante.'
        : 'Este documento todavía no está disponible para preview. Avisa al administrador para que lo vuelva a subir.',
      icono: 'bi bi-cloud-arrow-up',
      iconColor: '#f59e0b',
      mostrarResubir: true,
    });
    return;
  }

  // ── RUTA 3: Imagen → image renderer dedicado ────────────────────────
  if (isImg) {
    _udvLoadImage(UDV.baseUrl);
    return;
  }

  // ── RUTA 4: PDF (o desconocido que probablemente sea PDF) ────────────
  // Estrategia: intentar iframe primero (más rápido si funciona).
  // Si el iframe queda en blanco a los 6s, switch a PDF.js canvas.
  _udvSetStep('Cargando documento…');
  _udvLoadIframe(UDV.baseUrl, {
    onLoaded: () => {
      _udvLog('INFO', 'Iframe OK');
      _udvSetBadge('Visor nativo', '#16a34a');
    },
    onBlank: async () => {
      _udvLog('WARN', 'Iframe blank → fallback a PDF.js');
      // El iframe quedó en blanco. Si es un PDF (real o probable),
      // intentamos PDF.js canvas. Para otros tipos, mostramos error.
      const probablyPdf = isPdf || realMime.includes('pdf') ||
                           (UDV.nombre || '').toLowerCase().endsWith('.pdf');
      if (probablyPdf && window.pdfjsLib) {
        try {
          await _udvLoadPdfJs(UDV.baseUrl);
        } catch (e) {
          _udvShowError({
            titulo: 'Este contrato debe re-subirse',
            mensaje: UDV.esSuperadmin
              ? 'El archivo está dañado o en un formato que no se puede mostrar. Súbelo de nuevo y quedará operando al instante.'
              : 'No se pudo mostrar el archivo. Avisa al administrador para que lo vuelva a subir.',
            icono: 'bi bi-cloud-arrow-up',
            iconColor: '#f59e0b',
            mostrarResubir: true,
            errorTech: 'PDF.js: ' + (e.message || e),
          });
        }
      } else {
        _udvShowError({
          titulo: 'Este contrato debe re-subirse',
          mensaje: UDV.esSuperadmin
            ? 'El archivo está en un formato que no se puede mostrar. Súbelo de nuevo (PDF o Word) y quedará disponible.'
            : 'Este archivo no se puede mostrar. Avisa al administrador para que lo vuelva a subir.',
          icono: 'bi bi-cloud-arrow-up',
          iconColor: '#f59e0b',
          mostrarResubir: true,
        });
      }
    },
    onError: (e) => {
      _udvShowError({
        titulo: 'Este contrato debe re-subirse',
        mensaje: UDV.esSuperadmin
          ? 'Hubo un problema con el archivo. Súbelo de nuevo y quedará operando.'
          : 'No se pudo cargar el archivo. Avisa al administrador para que lo vuelva a subir.',
        icono: 'bi bi-cloud-arrow-up',
        iconColor: '#f59e0b',
        mostrarResubir: true,
        errorTech: 'Iframe error: ' + (e.message || e),
      });
    },
  });
}

// ─── ERP por RUT — sección Equipos ──────────────────────────
let erpRutLoaded = false;
let erpRutOpen   = false;
const CID_CLIENTE = DATA.cid;

async function cargarDocumentosErpRut() {
  if (erpRutLoaded) return;
  erpRutLoaded = true;
  const cont = document.getElementById('erpRutContent');
  if (!cont) return;
  try {
    const r = await fetch(`/mantenciones/api/clientes/${CID_CLIENTE}/documentos-erp`);
    const data = await r.json();
    const badge = document.getElementById('erpRutBadge');

    if (data.sin_rut) {
      cont.innerHTML = '<div class="text-muted small py-2 px-1">Este cliente no tiene RUT registrado.</div>';
      return;
    }
    if (data.sin_conexion || !data.ok) {
      cont.innerHTML = `<div class="alert alert-warning py-2 mb-0" style="font-size:.8rem">
        <i class="bi bi-exclamation-triangle me-1"></i>${data.msg || 'No hay conexión al ERP.'}
        <br><small>Puedes buscar por número de documento usando el botón "Importar ERP".</small>
      </div>`;
      badge.textContent = '—';
      badge.className = 'badge bg-warning text-dark';
      return;
    }

    const docs = data.documentos || [];
    if (!docs.length) {
      cont.innerHTML = `<div class="text-center text-muted py-3 small">Sin documentos encontrados para RUT ${data.rut}</div>`;
      badge.textContent = '0';
      return;
    }

    badge.textContent = `${docs.length} doc.`;
    badge.className = 'badge bg-success';

    // Agrupar por tipo de documento
    const porTipo = {};
    docs.forEach(d => {
      if (!porTipo[d.tipo_doc]) porTipo[d.tipo_doc] = [];
      porTipo[d.tipo_doc].push(d);
    });

    const tipoIconos = { FCV:'bi-receipt', BLV:'bi-receipt-cutoff', GDV:'bi-box-seam', VD:'bi-shop', NVI:'bi-file-earmark', NVV:'bi-file-earmark-text' };

    let html = `<div class="table-responsive"><table class="table table-sm table-hover align-middle mb-0" style="font-size:.8rem">
      <thead class="table-light"><tr>
        <th>Tipo</th><th>Nº Doc</th><th>Fecha</th><th>Productos</th><th style="width:80px"></th>
      </tr></thead><tbody>`;
    docs.forEach(d => {
      const nLineas = d.lineas?.length || 0;
      const icono = tipoIconos[d.tipo_doc] || 'bi-file-earmark';
      html += `<tr>
        <td><span class="badge bg-primary" style="font-size:.65rem">${d.tipo_doc}</span></td>
        <td class="font-monospace fw-bold">${d.num_doc}</td>
        <td class="text-muted">${d.fecha || '—'}</td>
        <td>${nLineas} línea${nLineas!==1?'s':''}</td>
        <td>
          <button class="btn btn-xs btn-ilus" onclick="importarDocErpRut('${d.tipo_doc}','${d.num_doc_raw}')">
            <i class="bi bi-download me-1"></i>Importar
          </button>
        </td>
      </tr>`;
    });
    html += '</tbody></table></div>';
    cont.innerHTML = html;

  } catch(e) {
    const cont2 = document.getElementById('erpRutContent');
    if(cont2) cont2.innerHTML = `<div class="text-danger small py-2">Error al consultar ERP: ${e.message}</div>`;
  }
}

function toggleErpRutPanel() {
  const panel   = document.getElementById('erpRutPanel');
  const chevron = document.getElementById('erpRutChevron');
  erpRutOpen = !erpRutOpen;
  panel.style.display = erpRutOpen ? '' : 'none';
  chevron.style.transform = erpRutOpen ? 'rotate(180deg)' : '';
  if (erpRutOpen) cargarDocumentosErpRut();
}

async function importarDocErpRut(tido, nudo) {
  // Abre el modal ERP con los datos pre-cargados
  const modal = document.getElementById('modalErp');
  if (!modal) { alert('Abre el modal ERP manualmente'); return; }
  // Pre-llenar el tab de documento
  const tabDocBtn = document.getElementById('fTabDocBtn');
  if (tabDocBtn) { fSetTab('doc'); }
  const fDocTido = document.getElementById('fDocTido');
  const fDocNudo = document.getElementById('fDocNudo');
  if (fDocTido) fDocTido.value = tido;
  if (fDocNudo) fDocNudo.value = nudo;
  new bootstrap.Modal(modal).show();
  // Buscar automáticamente
  setTimeout(fBuscarDoc, 300);
}

// Auto-cargar RUT docs cuando se cambia al tab equipos
document.querySelectorAll('.ftab-btn').forEach(btn => {
  btn.addEventListener('click', () => {
    if (btn.dataset.tab === 'equipos') {
      // Abrir el panel y cargar si tiene RUT
      const panel = document.getElementById('erpRutPanel');
      if (panel && !erpRutLoaded) {
        setTimeout(() => {
          erpRutOpen = true;
          panel.style.display = '';
          const chevron = document.getElementById('erpRutChevron');
          if (chevron) chevron.style.transform = 'rotate(180deg)';
          cargarDocumentosErpRut();
        }, 150);
      }
    }
  });
});

// ─── Agendar desde proyección de contrato ─────────────────────
function agendarDesdeProyeccion(fechaISO, ctid) {
  // Pre-rellena el modal de visita con la fecha sugerida
  document.getElementById('vi_id').value = '';
  document.getElementById('vi_fecha').value = fechaISO;
  document.getElementById('vi_tipo').value = 'preventiva';
  document.getElementById('vi_estado').value = 'programada';
  document.getElementById('vi_titulo').value = 'Mantención preventiva programada';
  document.getElementById('vi_descripcion').value = '';
  // Garantía: default "No aplica" (visita nueva desde proyección de contrato)
  _viSetGarantia(false);
  const btnDel = document.getElementById('btnEliminarVisita');
  if (btnDel) btnDel.style.display = 'none';
  const modal = new bootstrap.Modal(document.getElementById('modalVisita'));
  modal.show();
}

function agendarDesdeContrato(ctid, freqMeses) {
  // Calcula la fecha sugerida basada en hoy + frecuencia
  const hoy = new Date();
  hoy.setMonth(hoy.getMonth() + freqMeses);
  const fechaISO = hoy.toISOString().split('T')[0];
  agendarDesdeProyeccion(fechaISO, ctid);
}

// ─── Gestión de contrato — variables y cláusulas ──────────────
let _gcCtid = null;
let _gcClausulas = [];
let _gcVarsExtra = {};

async function abrirGestionContrato(ctid) {
  _gcCtid = ctid;
  document.getElementById('gcCtid').value = ctid;
  // Limpiar
  document.getElementById('gcSla').value = '';
  document.getElementById('gcFrecuencia').value = '';
  document.getElementById('gcMonto').value = '';
  document.getElementById('gcMontoAnual').value = '';
  document.getElementById('gcNotas').value = '';
  document.getElementById('gcRiesgo').value = 'medio';
  document.getElementById('gcVarsExtra').innerHTML = '';
  document.getElementById('gcClausulasLista').innerHTML = '';
  document.getElementById('gcUltGuardado').textContent = '';
  _gcClausulas = [];
  _gcVarsExtra = {};

  // Cargar nombre del contrato desde el DOM
  const ctDiv = document.getElementById(`ct-${ctid}`);
  const nombre = ctDiv ? ctDiv.querySelector('h5')?.textContent?.trim() || 'Contrato' : 'Contrato';
  document.getElementById('gcNombreContrato').textContent = nombre;

  // Mostrar modal
  new bootstrap.Modal(document.getElementById('modalGestionContrato')).show();

  // Cargar datos desde servidor
  try {
    const r = await fetch(`/mantenciones/api/contratos/${ctid}/clausulas`);
    const data = await r.json();
    if (data.campos) {
      document.getElementById('gcSla').value        = data.campos.sla_horas || '';
      document.getElementById('gcFrecuencia').value = data.campos.frecuencia_meses || '';
      document.getElementById('gcMonto').value      = data.campos.monto_mensual || '';
      document.getElementById('gcMontoAnual').value = data.campos.monto_anual || '';
      document.getElementById('gcNotas').value      = data.campos.notas || '';
      document.getElementById('gcRiesgo').value     = data.campos.nivel_riesgo || 'medio';
    }
    _gcClausulas = data.clausulas || [];
    _gcVarsExtra = data.variables || {};
    gcRenderClausulas();
    gcRenderVarsExtra();
  } catch(e) { console.warn('Error cargando datos contrato', e); }
}

function gcSetTab(tab, btn) {
  document.getElementById('gcTabVars').style.display      = tab === 'vars'      ? '' : 'none';
  document.getElementById('gcTabClausulas').style.display = tab === 'clausulas' ? '' : 'none';
  document.querySelectorAll('#gcTabs .nav-link').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
}

function gcRenderClausulas() {
  const cont = document.getElementById('gcClausulasLista');
  if (!_gcClausulas.length) {
    cont.innerHTML = '<div class="text-muted small py-2">Sin cláusulas registradas todavía.</div>';
    return;
  }
  cont.innerHTML = _gcClausulas.map((c, i) => `
    <div class="gc-clausula-row">
      <div style="flex:1">
        <div class="d-flex gap-2 mb-2">
          <input type="text" class="form-control form-control-sm" placeholder="Título cláusula" value="${c.titulo||''}"
                 onchange="_gcClausulas[${i}].titulo=this.value">
          <select class="form-select form-select-sm" style="max-width:130px" onchange="_gcClausulas[${i}].tipo=this.value">
            <option value="obligacion" ${c.tipo==='obligacion'?'selected':''}>Obligación</option>
            <option value="exclusion" ${c.tipo==='exclusion'?'selected':''}>Exclusión</option>
            <option value="penalidad" ${c.tipo==='penalidad'?'selected':''}>Penalidad</option>
            <option value="garantia" ${c.tipo==='garantia'?'selected':''}>Garantía</option>
            <option value="otro" ${c.tipo==='otro'?'selected':''}>Otro</option>
          </select>
          <button class="btn btn-sm btn-outline-danger" onclick="_gcClausulas.splice(${i},1);gcRenderClausulas()">
            <i class="bi bi-trash"></i>
          </button>
        </div>
        <textarea class="form-control form-control-sm" rows="2" placeholder="Texto de la cláusula…"
                  onchange="_gcClausulas[${i}].texto=this.value">${c.texto||''}</textarea>
      </div>
    </div>`).join('');
}

function gcAddClausula() {
  _gcClausulas.push({ titulo:'', texto:'', tipo:'obligacion' });
  gcRenderClausulas();
  // Scroll al último
  const cont = document.getElementById('gcClausulasLista');
  cont.lastElementChild?.scrollIntoView({ behavior:'smooth' });
}

function gcRenderVarsExtra() {
  const cont = document.getElementById('gcVarsExtra');
  const entries = Object.entries(_gcVarsExtra);
  if (!entries.length) { cont.innerHTML=''; return; }
  cont.innerHTML = entries.map(([k,v], i) => `
    <div class="d-flex gap-2 mb-2 align-items-center">
      <input type="text" class="form-control form-control-sm" style="max-width:180px"
             placeholder="Nombre variable" value="${k}"
             data-idx="${i}" data-role="key" onchange="gcUpdateVar(this)">
      <input type="text" class="form-control form-control-sm"
             placeholder="Valor" value="${v}"
             data-idx="${i}" data-role="val" onchange="gcUpdateVar(this)">
      <button class="btn btn-sm btn-outline-danger" onclick="gcDelVar('${k}')">
        <i class="bi bi-dash"></i>
      </button>
    </div>`).join('');
}

function gcAddVar() {
  const key = `Variable ${Object.keys(_gcVarsExtra).length + 1}`;
  _gcVarsExtra[key] = '';
  gcRenderVarsExtra();
}

function gcUpdateVar(input) {
  const entries = Object.entries(_gcVarsExtra);
  const idx = parseInt(input.dataset.idx);
  const [oldKey, oldVal] = entries[idx];
  if (input.dataset.role === 'key') {
    delete _gcVarsExtra[oldKey];
    _gcVarsExtra[input.value] = oldVal;
  } else {
    _gcVarsExtra[oldKey] = input.value;
  }
}

function gcDelVar(key) {
  delete _gcVarsExtra[key];
  gcRenderVarsExtra();
}

async function gcGuardar() {
  const spin = document.getElementById('gcGuardandoSpin');
  spin.style.display = '';
  const variables = {
    sla_horas:       parseInt(document.getElementById('gcSla').value) || null,
    frecuencia_meses: parseInt(document.getElementById('gcFrecuencia').value) || null,
    monto_mensual:   parseFloat(document.getElementById('gcMonto').value) || null,
    monto_anual:     parseFloat(document.getElementById('gcMontoAnual').value) || null,
    notas:           document.getElementById('gcNotas').value,
    nivel_riesgo:    document.getElementById('gcRiesgo').value,
    ..._gcVarsExtra,
  };
  try {
    const r = await fetch(`/mantenciones/api/contratos/${_gcCtid}/clausulas`, {
      method:'PUT',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify({ clausulas: _gcClausulas, variables })
    });
    const data = await r.json();
    spin.style.display = 'none';
    if (data.ok) {
      const ahora = new Date().toLocaleString('es-CL');
      document.getElementById('gcUltGuardado').textContent = `Guardado ${ahora}`;
      // Indicar éxito sin cerrar modal
      const btn = document.querySelector('#modalGestionContrato .btn-ilus');
      const orig = btn.innerHTML;
      btn.innerHTML = '<i class="bi bi-check-circle-fill me-1"></i>Guardado';
      btn.classList.add('btn-success');
      setTimeout(() => { btn.innerHTML = orig; btn.classList.remove('btn-success'); }, 2000);
    } else {
      alert(data.error || 'Error al guardar');
    }
  } catch(e) {
    spin.style.display = 'none';
    alert('Error de conexión: ' + e.message);
  }
}

// ─── REPORTES ─────────────────────────────────────────────────
let _repMaquinas  = [], _repObjetivos = [], _repTrabajos = [], _repObservaciones = [];
let _repCurrentId = null, _repFotosTemp = [];
let _reportesCache = null;   // PERF: HTML del listado ya renderizado → re-navegación instantánea
function _invalidarReportes(){ _reportesCache = null; }

async function cargarReportes(force = false) {
  const lista = document.getElementById('reportesLista');
  if (!lista) return;
  // PERF 2026-06-03: cache hit → render instantáneo sin round-trip. Se invalida en
  // cada mutación (crear/editar/eliminar/generar/analizar) para no mostrar datos viejos.
  if (!force && _reportesCache !== null) { lista.innerHTML = _reportesCache; return; }
  lista.innerHTML = '<div class="text-center py-3 text-muted"><div class="spinner-border spinner-border-sm me-2"></div>Cargando…</div>';
  // Timeout duro: el spinner NUNCA queda infinito. Si el servidor se satura o
  // cuelga, a los 25s abortamos y mostramos error + botón "Reintentar".
  const _ac = new AbortController();
  const _to = setTimeout(() => _ac.abort(), 25000);
  try {
    const r = await fetch(`/mantenciones/api/clientes/${CID}/reportes`, { signal: _ac.signal });
    if (!r.ok) throw new Error('El servidor respondió ' + r.status);
    const data = await r.json();
    if (!Array.isArray(data)) throw new Error((data && data.error) ? data.error : 'Respuesta inesperada del servidor');
    if (!data.length) {
      lista.innerHTML = `<div class="text-center py-5 text-muted">
        <i class="bi bi-file-earmark-x" style="font-size:3rem;opacity:.25"></i>
        <div class="fw-semibold mt-2">Sin informes registrados</div>
        <div style="font-size:.82rem">Crea el primer informe post servicio</div>
        <button class="btn btn-sm btn-ilus mt-3 fw-bold" onclick="abrirNuevoReporte()">
          <i class="bi bi-plus-circle me-1"></i>Nuevo informe</button>
      </div>`;
      _reportesCache = lista.innerHTML;
      return;
    }
    const tipoIcon = {mantencion:'wrench',instalacion:'box-seam',inspeccion:'search',garantia:'shield-check',otro:'file'};
    const tipoLbl  = {mantencion:'Mantención',instalacion:'Instalación',inspeccion:'Inspección',garantia:'Garantía',otro:'Otro'};
    lista.innerHTML = data.map(rep => `
      <div class="rep-card">
        <div class="d-flex align-items-start gap-3 flex-wrap">
          <div style="min-width:36px;text-align:center">
            <i class="bi bi-${tipoIcon[rep.tipo]||'file'}" style="font-size:1.6rem;color:#6b7280"></i>
          </div>
          <div style="flex:1;min-width:0">
            <div class="d-flex align-items-center gap-2 flex-wrap mb-1">
              <span class="fw-bold" style="font-size:.95rem">
                ${(rep.ticket_num || rep.ot_num) ? `TICKET ${rep.ticket_num || '—'} / OT ${rep.ot_num || '—'} — ` : ''}${rep.asunto || 'Informe de servicio'}
              </span>
              <span class="rep-tipo-badge rep-tipo-${rep.tipo}">${tipoLbl[rep.tipo]||rep.tipo}</span>
              <span class="rep-tipo-badge rep-estado-${rep.estado||'borrador'}">${((rep.estado||'borrador').charAt(0).toUpperCase()+(rep.estado||'borrador').slice(1))}</span>
            </div>
            <div style="font-size:.75rem;color:#6b7280;display:flex;flex-wrap:wrap;gap:0 16px">
              ${rep.tecnico_junior ? `<span><i class="bi bi-person me-1"></i>${rep.tecnico_junior}</span>` : ''}
              ${rep.fecha_inicio   ? `<span><i class="bi bi-calendar me-1"></i>${rep.fecha_inicio} → ${rep.fecha_cierre||'—'}</span>` : ''}
              ${rep.ai_diagnostico ? `<span style="color:#7c3aed"><i class="bi bi-stars me-1"></i>Analizado por IA</span>` : ''}
            </div>
            ${rep.ai_diagnostico ? `<div class="rep-ai-card mt-2" style="font-size:.78rem;padding:10px 12px">
              <div class="fw-bold mb-1" style="color:#166534"><i class="bi bi-stars me-1"></i>Diagnóstico IA</div>
              <div style="color:#374151">${rep.ai_diagnostico.slice(0,220)}${rep.ai_diagnostico.length>220?'…':''}</div>
            </div>` : ''}
          </div>
          <div class="d-flex gap-1 flex-shrink-0 flex-wrap">
            <a href="/mantenciones/api/reportes/${rep.id}/html" target="_blank" class="btn btn-xs btn-outline-success fw-bold" title="Ver informe (rápido, sin esperar el motor PDF)">
              <i class="bi bi-eye me-1"></i>Ver
            </a>
            <a href="/mantenciones/api/reportes/${rep.id}/pdf" target="_blank" class="btn btn-xs btn-outline-danger" title="Descargar PDF (puede tardar si el motor PDF del servidor no está disponible)">
              <i class="bi bi-file-earmark-pdf"></i>
            </a>
            <a href="/mantenciones/api/reportes/${rep.id}/word" class="btn btn-xs btn-outline-dark" title="Descargar Word">
              <i class="bi bi-file-earmark-word"></i>
            </a>
            ${ES_SUPERADMIN ? `
            <button class="btn btn-xs btn-outline-primary" onclick="editarReporte(${rep.id})" title="Editar">
              <i class="bi bi-pencil"></i>
            </button>
            <button class="btn btn-xs btn-outline-danger" onclick="eliminarReporte(${rep.id})" title="Eliminar">
              <i class="bi bi-trash"></i>
            </button>` : ''}
          </div>
        </div>
        ${rep.html_generated_at ? `<div class="mt-2" style="font-size:.66rem;color:#9ca3af;text-align:right"><i class="bi bi-clock me-1"></i>HTML generado ${rep.html_generated_at}</div>` : ''}
      </div>`).join('');
    _reportesCache = lista.innerHTML;   // PERF: cachear para re-navegación instantánea
  } catch(e) {
    const msg = (e && e.name === 'AbortError')
      ? 'La carga tardó demasiado (servidor lento). Intenta de nuevo.'
      : ((e && e.message) ? e.message : 'Error desconocido');
    lista.innerHTML = `<div class="alert alert-warning d-flex align-items-center justify-content-between flex-wrap gap-2 mb-0">
      <span><i class="bi bi-exclamation-triangle me-1"></i>No se pudieron cargar los informes: ${msg}</span>
      <button class="btn btn-sm btn-outline-dark fw-bold" onclick="cargarReportes()"><i class="bi bi-arrow-clockwise me-1"></i>Reintentar</button>
    </div>`;
  } finally {
    clearTimeout(_to);
  }
}

// ─── INTELIGENCIA DE FICHA ─────────────────────────────────────
// Agente determinista (sin IA): el backend devuelve un diagnóstico
// completo del cliente. Aquí solo renderizamos. Mismo patrón que
// cargarReportes: cache en memoria + AbortController 25s + Reintentar.
let _intelCache = null;   // dict del diagnóstico ya renderizado → re-navegación instantánea
function _invalidarIntel(){ _intelCache = null; }

// Monto CLP con puntos de miles → "$ 1.234.567"
function _intelCLP(n){
  const v = Math.round(Number(n) || 0);
  return '$ ' + v.toLocaleString('es-CL');
}
function _intelEsc(s){
  return String(s == null ? '' : s)
    .replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')
    .replace(/"/g,'&quot;').replace(/'/g,'&#39;');
}
// Fecha en formato chileno DD/MM/YYYY para TODO el panel del Agente.
// El backend envía fechas ISO ("2025-09-30" o "2025-09-30 14:05"). Aquí las
// mostramos como "30/09/2025". Si el valor no es una fecha ISO reconocible,
// se devuelve tal cual (no rompemos textos como "—" o "Indefinido").
function _intelFecha(s){
  if (s == null) return '';
  const str = String(s).trim();
  if (!str) return '';
  const m = str.match(/^(\d{4})-(\d{2})-(\d{2})/);   // YYYY-MM-DD[...]
  if (!m) return str;
  return `${m[3]}/${m[2]}/${m[1]}`;
}
// Igual que _intelFecha pero ya escapado para inyectar en HTML.
function _intelFechaEsc(s){ return _intelEsc(_intelFecha(s)); }

async function cargarInteligencia(force = false) {
  const panel = document.getElementById('intelPanel');
  if (!panel) return;
  // cache hit → render instantáneo (se invalida al recalcular con force).
  if (!force && _intelCache !== null) { _intelRender(_intelCache); return; }
  panel.innerHTML = '<div class="text-center py-5 text-muted"><div class="spinner-border spinner-border-sm me-2"></div>Calculando inteligencia…</div>';
  // Timeout duro: el spinner NUNCA queda infinito. A los 25s abortamos.
  const _ac = new AbortController();
  const _to = setTimeout(() => _ac.abort(), 25000);
  // Al recalcular mostramos overlay premium (puede tardar unos segundos).
  if (force && window.ilusLoader) { try { ilusLoader.show('Recalculando inteligencia…'); } catch(e){} }
  try {
    const url = `/mantenciones/api/clientes/${CID}/inteligencia` + (force ? '?force=1' : '');
    const r = await fetch(url, { signal: _ac.signal });
    if (!r.ok) throw new Error('El servidor respondió ' + r.status);
    const data = await r.json();
    if (!data || typeof data !== 'object' || data.error) {
      throw new Error((data && data.error) ? data.error : 'Respuesta inesperada del servidor');
    }
    _intelCache = data;
    _intelRender(data);
    if (force && window.ilusToast) { try { ilusToast('✓ Inteligencia recalculada', { type:'success' }); } catch(e){} }
  } catch(e) {
    const msg = (e && e.name === 'AbortError')
      ? 'El cálculo tardó demasiado (servidor lento). Intenta de nuevo.'
      : ((e && e.message) ? e.message : 'Error desconocido');
    panel.innerHTML = `<div class="alert alert-warning d-flex align-items-center justify-content-between flex-wrap gap-2 mb-0">
      <span><i class="bi bi-exclamation-triangle me-1"></i>No se pudo calcular la inteligencia: ${_intelEsc(msg)}</span>
      <button class="btn btn-sm btn-outline-dark fw-bold" onclick="cargarInteligencia(true)"><i class="bi bi-arrow-clockwise me-1"></i>Reintentar</button>
    </div>`;
  } finally {
    clearTimeout(_to);
    if (window.ilusLoader) { try { ilusLoader.hide(); } catch(e){} }
  }
}

// Construye el HTML completo del diagnóstico y lo pinta en #intelPanel.
function _intelRender(d){
  const panel = document.getElementById('intelPanel');
  if (!panel) return;
  d = d || {};
  const score = Math.max(0, Math.min(100, parseInt(d.score_salud, 10) || 0));
  const cal   = Math.max(0, Math.min(100, parseInt(d.calidad_informacion, 10) || 0));
  const riesgo = (d.nivel_riesgo || 'bajo').toLowerCase();

  // Color del gauge por score (rojo<40, ámbar<70, verde≥70).
  const gaugeColor = score >= 70 ? '#16a34a' : (score >= 40 ? '#f59e0b' : '#dc2626');
  const riskIcon = { alto:'exclamation-octagon-fill', medio:'exclamation-triangle-fill', bajo:'shield-check' }[riesgo] || 'shield-check';
  const riskLbl  = { alto:'Riesgo alto', medio:'Riesgo medio', bajo:'Riesgo bajo' }[riesgo] || ('Riesgo ' + riesgo);

  // ════════════════════════════════════════════════════════════════
  // JERARQUÍA EJECUTIVA DEL PANEL (pensada para gerencia):
  //   1. HERO          → salud / riesgo / contrato (estado de un vistazo)
  //   2. CHAT          → la estrella: pregúntale al agente
  //   3. ATENCIÓN      → consultas que el agente necesita que resuelvas
  //   4. DIAGNÓSTICO   → KPIs + contrato/historia/agenda/facturación/…
  //   5. REPORTES      → generar informes y (a futuro) analizar PDFs
  // Cada bloque abre con un encabezado de sección consistente (.intel-section).
  // ════════════════════════════════════════════════════════════════
  let html = '';

  // ── 1) HERO del Agente: avatar + burbuja con el resumen + gauge de salud ──
  // El resumen "te habla" en primera persona (cálido, humano). Si el backend
  // no lo manda, caemos en un texto sensato para no dejar el hero vacío.
  const resumen = (d.resumen_ejecutivo && String(d.resumen_ejecutivo).trim())
    ? String(d.resumen_ejecutivo).trim()
    : 'Revisé esta ficha y preparé un diagnóstico con la salud del cliente, lo que falta y la mejor oportunidad comercial.';
  html += `
  <div class="intel-hero2">
    <div class="intel-hero2-main">
      <div class="intel-avatar"><i class="bi bi-robot"></i><span class="intel-pulse"></span></div>
      <div style="flex:1;min-width:0">
        <div class="intel-agent-name"><i class="bi bi-stars"></i>Agente de inteligencia ILUS · en línea</div>
        <div class="intel-bubble">${_intelEsc(resumen)}</div>
      </div>
    </div>
    <div class="intel-hero2-side">
      <div class="intel-gauge" style="background:conic-gradient(${gaugeColor} ${score*3.6}deg, #2a2a2a 0deg)">
        <div class="intel-gauge-inner">
          <div class="intel-gauge-num" style="color:${gaugeColor}">${score}</div>
          <div class="intel-gauge-lbl">Salud</div>
        </div>
      </div>
      <div class="d-flex align-items-center gap-2 flex-wrap justify-content-center">
        <span class="intel-risk intel-risk-${riesgo}"><i class="bi bi-${riskIcon}"></i>${_intelEsc(riskLbl)}</span>
        ${d.tiene_contrato
          ? '<span class="intel-chip" style="background:#dcfce7;color:#166534"><i class="bi bi-file-earmark-check"></i>Con contrato</span>'
          : '<span class="intel-chip" style="background:#fee2e2;color:#991b1b"><i class="bi bi-file-earmark-x"></i>Sin contrato</span>'}
      </div>
      ${_intelScoreDetalle(d.score_detalle, '#fca5a5')}
    </div>
  </div>`;

  // ── 2) CHAT: Pregúntale al agente (conversacional, determinista, sin IA) ──
  // Es la "estrella" del panel → va arriba y con marco rojo destacado.
  const _chipsHtml = INTEL_CHIPS.map(q =>
    `<button class="btn btn-sm btn-outline-secondary" style="font-size:.72rem;border-radius:50px" onclick='intelPreguntar(${JSON.stringify(q)})'>${_intelEsc(q)}</button>`
  ).join('');
  html += _intelSection('Pregúntale al agente', 'chat-dots-fill', 'La estrella: resuelve dudas de este cliente al instante');
  html += `
  <div class="intel-card intel-card-chat" id="intelChatCard">
    <div id="intelChatLog" style="max-height:320px;overflow-y:auto;margin:2px 0 10px;padding-right:4px">
      <div style="text-align:left;margin:4px 0"><span style="display:inline-block;background:#f3f4f6;color:#111827;padding:7px 12px;border-radius:14px 14px 14px 2px;font-size:.82rem">Pregúntame lo que necesites de este cliente — frecuencia, garantías, si se cobra o es gratis, cuánto cobrar, atrasos, fuga a terceros… Toca una sugerencia o escribe abajo. 👇</span></div>
    </div>
    <div class="d-flex flex-wrap gap-1 mb-2">${_chipsHtml}</div>
    <div class="d-flex gap-2">
      <input id="intelChatInput" class="form-control form-control-sm" placeholder="Ej: ¿esta mantención es gratis o se cobra?" onkeydown="if(event.key==='Enter')intelPreguntar()">
      <button class="btn btn-ilus btn-sm px-3" onclick="intelPreguntar()" title="Preguntar"><i class="bi bi-send"></i></button>
    </div>
  </div>`;

  // ── 3) ATENCIÓN: consultas accionables que el agente necesita resueltas ──
  // Siempre se pinta (si no hay nada pendiente, _intelConsultas muestra el
  // estado positivo verde). Encabezado de sección consistente arriba.
  html += _intelSection('Acciones que requieren tu atención', 'bell-fill', 'Lo que el agente necesita que resuelvas en esta ficha');
  html += _intelConsultas(d.consultas);

  // ════════════════════════════════════════════════════════════════
  // 4) DIAGNÓSTICO — la "foto completa" del cliente, agrupada y titulada.
  // ════════════════════════════════════════════════════════════════
  html += _intelSection('Diagnóstico del cliente', 'clipboard-data', 'Contrato, historia, agenda, facturación y métricas clave');

  // ── 4.0) Fila de KPIs (resumen numérico, abre el diagnóstico) ──
  const kpis = Array.isArray(d.kpis) ? d.kpis : [];
  if (kpis.length) {
    html += '<div class="row g-2 mb-3">';
    kpis.forEach(k => {
      const tono = (k.tono || 'ok').toLowerCase();
      // Los KPIs de fecha (Próxima/Última) deben mostrarse DD/MM/YYYY.
      // _intelFecha devuelve el texto original si no es una fecha ISO.
      const valor = _intelFecha(k.valor);
      html += `
      <div class="col-6 col-md-4 col-xl-3">
        <div class="intel-kpi intel-kpi-${tono}">
          <div class="intel-kpi-lbl">${_intelEsc(k.label)}</div>
          <div class="intel-kpi-val">${_intelEsc(valor)}${k.sufijo ? `<span style="font-size:.7rem;font-weight:700;color:#9ca3af"> ${_intelEsc(k.sufijo)}</span>` : ''}</div>
        </div>
      </div>`;
    });
    html += '</div>';
  }

  // ── 4.1) Historia (pasado) vs Agenda (futuro): dos tarjetas diferenciadas ──
  html += _intelHistoriaAgenda(d);

  // ── 4.2) Cierre de facturación (prioridad alta del dueño) ──
  html += _intelFacturacion(d);

  html += '<div class="intel-grid">';

  // ── Card: Universo del cliente ──
  const u = d.universo || {};
  const porTipo = u.por_tipo || {};
  const tiposEntries = Object.keys(porTipo);
  const ddu = (u.dias_desde_ultima == null) ? null : parseInt(u.dias_desde_ultima, 10);
  // "Frescura" de la relación: verde reciente, ámbar tibio, rojo frío.
  let dduTxt = '', dduColor = '#6b7280', dduIcon = 'clock-history';
  if (ddu != null) {
    if (ddu <= 0)       { dduTxt = 'hoy';                  dduColor = '#166534'; dduIcon = 'lightning-charge-fill'; }
    else if (ddu <= 90) { dduTxt = `hace ${ddu} días`;     dduColor = '#166534'; dduIcon = 'clock-history'; }
    else if (ddu <= 180){ dduTxt = `hace ${ddu} días`;     dduColor = '#b45309'; dduIcon = 'clock-history'; }
    else                { dduTxt = `hace ${ddu} días`;     dduColor = '#dc2626'; dduIcon = 'exclamation-circle-fill'; }
  }
  html += `
  <div class="intel-card">
    <div class="intel-card-title"><i class="bi bi-diagram-3" style="color:#7c3aed"></i>Universo del cliente</div>
    <div class="d-flex align-items-baseline gap-2 mb-2">
      <span class="intel-bignum">${parseInt(u.total_gestiones,10)||0}</span>
      <span style="font-size:.78rem;color:#6b7280;font-weight:600">gestiones totales · ${parseInt(u.realizadas,10)||0} realizadas</span>
    </div>
    ${tiposEntries.length ? `<div class="mb-2">${tiposEntries.map(t =>
      `<span class="intel-pill"><i class="bi bi-tag"></i>${_intelEsc(t)}: <b>${parseInt(porTipo[t],10)||0}</b></span>`).join('')}</div>` : ''}
    <div class="intel-dl">
      <div class="intel-dl-item"><div class="intel-dl-lbl">Levantamientos</div><div class="intel-dl-val">${parseInt(u.levantamientos,10)||0}</div></div>
      <div class="intel-dl-item"><div class="intel-dl-lbl">Última gestión</div><div class="intel-dl-val">${u.ultima_fecha ? _intelFechaEsc(u.ultima_fecha) : '—'}${dduTxt ? ` <span style="color:${dduColor};font-weight:800;font-size:.78rem"><i class="bi bi-${dduIcon}"></i> ${dduTxt}</span>` : ''}</div></div>
      <div class="intel-dl-item"><div class="intel-dl-lbl">Primera gestión</div><div class="intel-dl-val">${u.primera_fecha ? _intelFechaEsc(u.primera_fecha) : '—'}</div></div>
    </div>
  </div>`;

  // ── Card: Datos del cliente (completitud de la ficha) ──
  // Preferimos d.datos_cliente (nuevo) y caemos a calidad/faltantes legacy.
  const dc = d.datos_cliente || {};
  const camposTot = parseInt(dc.campos_total, 10) || 0;
  const camposOk  = parseInt(dc.campos_ok, 10);
  const dcFaltantes = Array.isArray(dc.faltantes) ? dc.faltantes
                    : (Array.isArray(d.faltantes) ? d.faltantes : []);
  // Porcentaje: si vienen los campos lo calculamos; si no, usamos `cal` legacy.
  const pctDatos = (camposTot > 0 && Number.isFinite(camposOk))
    ? Math.round((camposOk / camposTot) * 100) : cal;
  const dcColor = pctDatos >= 80 ? '#16a34a' : (pctDatos >= 50 ? '#f59e0b' : '#dc2626');
  const dcCompleto = (dc.completo === true) || (dcFaltantes.length === 0 && pctDatos >= 100);
  html += `
  <div class="intel-card">
    <div class="intel-card-title"><i class="bi bi-person-vcard" style="color:#3b82f6"></i>Datos del cliente
      ${dc.tiene_coordenadas ? '<span class="intel-chip" style="background:#dbeafe;color:#1e40af"><i class="bi bi-geo-alt-fill"></i>con coordenadas</span>' : ''}
    </div>
    <div class="d-flex align-items-center gap-2 mb-1">
      <div class="intel-bar" style="flex:1"><div class="intel-bar-fill" style="width:${pctDatos}%;background:${dcColor}"></div></div>
      <span class="fw-bold" style="font-size:1.05rem;color:${dcColor}">${pctDatos}%</span>
    </div>
    ${camposTot > 0 ? `<div class="intel-meter-lbl mb-1"><b style="color:${dcColor}">${Number.isFinite(camposOk)?camposOk:'—'}</b> de ${camposTot} campos completos</div>` : ''}
    ${(!dcCompleto && dcFaltantes.length) ? `
      <div style="font-size:.66rem;font-weight:800;text-transform:uppercase;letter-spacing:.4px;color:#9ca3af;margin:10px 0 6px">Le falta</div>
      <div>${dcFaltantes.map(f => `<span class="intel-pill" style="background:#fef2f2;color:#991b1b"><i class="bi bi-exclamation-circle"></i>${_intelEsc(f)}</span>`).join('')}</div>
      <div class="mt-3"><button type="button" class="btn btn-xs btn-ilus fw-bold" onclick="try{abrirEditarCliente()}catch(e){}"><i class="bi bi-pencil-square me-1"></i>Completar ficha</button></div>
    ` : '<div class="text-success" style="font-size:.84rem"><i class="bi bi-check-circle-fill me-1"></i>Ficha completa, ¡buen trabajo!</div>'}
  </div>`;

  // ── Card: Equipos del cliente ──
  const eq = d.equipos || {};
  const eqTotal   = parseInt(eq.total, 10) || 0;
  const eqSinSerie= parseInt(eq.sin_serie, 10) || 0;
  const eqSinFoto = parseInt(eq.sin_foto, 10) || 0;
  const eqCrit    = parseInt(eq.criticos, 10) || 0;
  html += `
  <div class="intel-card ${eqCrit > 0 ? 'intel-card-danger' : ''}">
    <div class="intel-card-title"><i class="bi bi-hdd-stack" style="color:${eqCrit>0?'#dc2626':'#0ea5e9'}"></i>Equipos
      ${eqCrit > 0 ? `<span class="intel-chip" style="background:#fee2e2;color:#991b1b"><i class="bi bi-exclamation-triangle-fill"></i>${eqCrit} crítico${eqCrit>1?'s':''}</span>` : ''}
    </div>
    <div class="d-flex align-items-baseline gap-2 mb-3">
      <span class="intel-bignum">${eqTotal}</span>
      <span style="font-size:.78rem;color:#6b7280;font-weight:600">equipo${eqTotal===1?'':'s'} en la ficha</span>
    </div>
    ${eqTotal > 0 ? `
      <div>
        <span class="intel-eqchip ${eqSinSerie>0?'bad':''}"><i class="bi bi-${eqSinSerie>0?'upc-scan':'check2'}"></i>${eqSinSerie>0?`${eqSinSerie} sin serie`:'todos con serie'}</span>
        <span class="intel-eqchip ${eqSinFoto>0?'bad':''}"><i class="bi bi-${eqSinFoto>0?'camera':'images'}"></i>${eqSinFoto>0?`${eqSinFoto} sin foto`:'todos con foto'}</span>
        ${eqCrit>0?`<span class="intel-eqchip bad"><i class="bi bi-exclamation-octagon"></i>${eqCrit} en estado crítico</span>`:''}
      </div>
      ${(eqSinSerie>0||eqSinFoto>0||eqCrit>0)?`<div class="mt-3"><button type="button" class="btn btn-xs btn-outline-dark fw-bold" onclick="switchTab('equipos')"><i class="bi bi-arrow-right-circle me-1"></i>Revisar equipos</button></div>`:''}
    ` : `<div style="font-size:.84rem;color:#991b1b"><i class="bi bi-exclamation-circle me-1"></i>No hay equipos cargados.</div>
        <div class="mt-3"><button type="button" class="btn btn-xs btn-ilus fw-bold" onclick="switchTab('equipos')"><i class="bi bi-plus-circle me-1"></i>Agregar equipos</button></div>`}
  </div>`;

  // ── Card: Contrato ──
  const c = d.diagnostico_contrato || {};
  const dias = (c.dias_para_vencer == null) ? null : parseInt(c.dias_para_vencer, 10);
  let vigTxt = '—', vigColor = '#0f172a';
  if (c.es_indefinido) { vigTxt = 'Indefinido'; vigColor = '#166534'; }
  else if (dias != null) {
    if (dias < 0)      { vigTxt = `Vencido hace ${Math.abs(dias)} días`; vigColor = '#dc2626'; }
    else if (dias <= 60){ vigTxt = `Vence en ${dias} días`;             vigColor = '#f59e0b'; }
    else                { vigTxt = `Vence en ${dias} días`;             vigColor = '#166534'; }
  }
  html += `
  <div class="intel-card">
    <div class="intel-card-title"><i class="bi bi-file-earmark-text" style="color:#5b21b6"></i>Contrato</div>
    <div class="intel-dl">
      <div class="intel-dl-item"><div class="intel-dl-lbl">Estado</div><div class="intel-dl-val">${_intelEsc(c.estado || '—')}</div></div>
      <div class="intel-dl-item"><div class="intel-dl-lbl">Vigencia</div><div class="intel-dl-val" style="color:${vigColor}">${_intelEsc(vigTxt)}</div></div>
      <div class="intel-dl-item"><div class="intel-dl-lbl">Frecuencia</div><div class="intel-dl-val">${c.frecuencia_meses ? (parseInt(c.frecuencia_meses,10) + ' meses') : '—'}${c.frecuencia_origen ? ` <span style="font-size:.62rem;color:#9ca3af;font-weight:600">(${_intelEsc(c.frecuencia_origen)})</span>` : ''}</div></div>
      <div class="intel-dl-item"><div class="intel-dl-lbl">Mant. gratis</div><div class="intel-dl-val">${c.incluye_mant_gratis ? '<span style="color:#166534">Sí incluye</span>' : 'No incluye'}</div></div>
      <div class="intel-dl-item"><div class="intel-dl-lbl">Gratis al año</div><div class="intel-dl-val">${parseInt(c.gratis_incluidas_anual,10)||0}${c.gratis_origen ? ` <span style="font-size:.62rem;color:#9ca3af;font-weight:600">(${_intelEsc(c.gratis_origen)})</span>` : ''}</div></div>
      <div class="intel-dl-item"><div class="intel-dl-lbl">Valor anual</div><div class="intel-dl-val">${_intelCLP(c.valor_anual)}</div></div>
    </div>
    ${(c.vigencia_inicio || c.vigencia_fin) ? `<div style="font-size:.7rem;color:#9ca3af;margin-top:10px"><i class="bi bi-calendar-range me-1"></i>${c.vigencia_inicio ? _intelFechaEsc(c.vigencia_inicio) : '—'} → ${c.vigencia_fin ? _intelFechaEsc(c.vigencia_fin) : (c.es_indefinido ? 'Indefinido' : '—')}</div>` : ''}
  </div>`;

  // ── Card: Brecha de mantenciones gratis (ROJA si pendientes>0) ──
  const b = d.brecha_gratis || {};
  const pend = parseInt(b.pendientes, 10) || 0;
  html += `
  <div class="intel-card ${pend > 0 ? 'intel-card-danger' : ''}">
    <div class="intel-card-title"><i class="bi bi-clipboard-x" style="color:${pend>0?'#dc2626':'#6b7280'}"></i>Brecha de mantenciones${pend > 0 ? ` <span class="intel-chip" style="background:#fee2e2;color:#991b1b">${pend} pendientes</span>` : ''}</div>
    ${b.mensaje ? `<div style="font-size:.85rem;color:${pend>0?'#991b1b':'#374151'};margin-bottom:10px;line-height:1.5">${_intelEsc(b.mensaje)}</div>` : ''}
    <div class="intel-dl">
      <div class="intel-dl-item"><div class="intel-dl-lbl">Esperadas</div><div class="intel-dl-val">${parseInt(b.esperadas,10)||0}</div></div>
      <div class="intel-dl-item"><div class="intel-dl-lbl">Cubiertas</div><div class="intel-dl-val">${parseInt(b.cubiertas,10)||0}</div></div>
      <div class="intel-dl-item"><div class="intel-dl-lbl">Pendientes</div><div class="intel-dl-val" style="color:${pend>0?'#dc2626':'#0f172a'}">${pend}</div></div>
      <div class="intel-dl-item"><div class="intel-dl-lbl">Exposición</div><div class="intel-dl-val" style="color:${pend>0?'#dc2626':'#0f172a'}">${_intelCLP(b.exposicion_clp)}</div></div>
    </div>
    ${b.riesgo_fuga_tercero ? '<div class="mt-2"><span class="intel-chip" style="background:#fee2e2;color:#991b1b"><i class="bi bi-box-arrow-up-right"></i>Riesgo de fuga a tercero</span></div>' : ''}
  </div>`;

  html += '</div>'; // /intel-grid

  // ── Card: Proyección de fechas (timeline) ──
  const m = d.mantenciones || {};
  const vencidas = Array.isArray(m.vencidas) ? m.vencidas : [];
  const proximas = Array.isArray(m.proximas) ? m.proximas : [];
  const proyeccion = Array.isArray(m.proyeccion) ? m.proyeccion : [];
  if (vencidas.length || proximas.length || proyeccion.length) {
    html += `
    <div class="intel-card mt-1">
      <div class="intel-card-title"><i class="bi bi-calendar2-week" style="color:#dc2626"></i>Proyección de fechas</div>
      <div class="row g-3" style="font-size:.7rem;color:#6b7280;margin-bottom:4px">
        <div class="col-12 col-md-4"><b>Esperadas a hoy:</b> ${parseInt(m.esperadas_a_hoy,10)||0}</div>
        <div class="col-12 col-md-4"><b>Última:</b> ${m.ultima_fecha ? _intelFechaEsc(m.ultima_fecha) : '—'}</div>
        <div class="col-12 col-md-4"><b>Próxima:</b> ${m.proxima_fecha ? _intelFechaEsc(m.proxima_fecha) : '—'}</div>
      </div>
      <div class="intel-tl">
        ${vencidas.map(v => `<div class="intel-tl-row intel-tl-vencida"><span style="color:#dc2626;font-weight:700">${_intelFechaEsc(v.fecha)}</span> <span style="color:#991b1b;font-size:.74rem">— vencida hace ${parseInt(v.dias_atraso,10)||0} días</span></div>`).join('')}
        ${proximas.map(p => `<div class="intel-tl-row intel-tl-prox"><span style="color:#166534;font-weight:700">${_intelFechaEsc(p.fecha)}</span> <span style="color:#15803d;font-size:.74rem">— en ${parseInt(p.dias_faltan,10)||0} días</span></div>`).join('')}
        ${proyeccion.map(f => `<div class="intel-tl-row intel-tl-proy"><span style="color:#6b7280">${_intelFechaEsc(f)}</span> <span style="color:#9ca3af;font-size:.74rem">— proyectada</span></div>`).join('')}
      </div>
    </div>`;
  }

  // ── Card: Propuesta comercial ──
  const val = d.valorizacion || {};
  const hasVal = (parseFloat(val.total) || 0) > 0 || val.pitch;
  if (hasVal) {
    html += `
    <div class="intel-card mt-1" style="border:1px solid #fecdd3;background:linear-gradient(135deg,#fff,#fff5f5)">
      <div class="intel-card-title"><i class="bi bi-megaphone" style="color:#dc2626"></i>Propuesta comercial</div>
      ${val.pitch ? `<div style="font-size:.88rem;color:#374151;margin-bottom:12px;line-height:1.5">${_intelEsc(val.pitch)}</div>` : ''}
      <div style="max-width:420px">
        <div class="intel-prop-row"><span>Precio unitario</span><span class="fw-bold">${_intelCLP(val.precio_unitario)}</span></div>
        ${(parseFloat(val.descuento_pct)||0) > 0 || (parseFloat(val.descuento_monto)||0) > 0 ? `<div class="intel-prop-row"><span>Descuento${val.descuento_pct ? ` (${parseFloat(val.descuento_pct)}%)` : ''}</span><span class="fw-bold" style="color:#16a34a">− ${_intelCLP(val.descuento_monto)}</span></div>` : ''}
        <div class="intel-prop-row"><span>Neto</span><span class="fw-bold">${_intelCLP(val.neto)}</span></div>
        <div class="intel-prop-row"><span>IVA${val.iva_pct ? ` (${parseFloat(val.iva_pct)}%)` : ''}</span><span class="fw-bold">${_intelCLP(val.iva_monto)}</span></div>
        <div class="intel-prop-row" style="border-bottom:none"><span class="fw-bold">Total</span><span class="intel-prop-total">${_intelCLP(val.total)}</span></div>
      </div>
      <div class="mt-3">
        <a href="/mantenciones/cotizaciones/nuevo?cliente_id=${CID}" class="btn btn-sm btn-ilus fw-bold">
          <i class="bi bi-file-earmark-plus me-1"></i>Crear cotización
        </a>
      </div>
    </div>`;
  }

  // ── Lista de acciones recomendadas ──
  const acciones = Array.isArray(d.acciones) ? d.acciones : [];
  if (acciones.length) {
    const urgIcon = { alta:'exclamation-octagon-fill', media:'exclamation-triangle-fill', baja:'info-circle-fill' };
    const urgColor = { alta:'#dc2626', media:'#f59e0b', baja:'#16a34a' };
    const btnCls  = { alta:'btn-ilus', media:'btn-outline-dark', baja:'btn-outline-secondary' };
    html += `
    <div class="intel-card mt-1">
      <div class="intel-card-title"><i class="bi bi-list-check" style="color:#dc2626"></i>Acciones recomendadas</div>
      ${acciones.map(a => {
        const urg = (a.urgencia || 'baja').toLowerCase();
        return `
        <div class="intel-accion intel-accion-${urg}">
          <i class="bi bi-${urgIcon[urg]||'info-circle-fill'}" style="font-size:1.25rem;color:${urgColor[urg]||'#6b7280'};flex-shrink:0"></i>
          <div style="flex:1;min-width:0">
            <div class="fw-bold" style="font-size:.9rem;color:#0f172a">${_intelEsc(a.titulo)}</div>
            ${a.tipo ? `<div style="font-size:.72rem;color:#9ca3af;text-transform:uppercase;letter-spacing:.4px;font-weight:700">${_intelEsc(a.tipo)}</div>` : ''}
          </div>
          ${a.url_accion ? `<a href="${_intelEsc(a.url_accion)}" class="btn btn-xs ${btnCls[urg]||'btn-outline-secondary'} fw-bold flex-shrink-0">Ir <i class="bi bi-arrow-right ms-1"></i></a>` : ''}
        </div>`;
      }).join('')}
    </div>`;
  }

  // ── Oportunidades (consultas de severidad baja / tipo oportunidad) ──
  html += _intelOportunidades(d.consultas);

  // ════════════════════════════════════════════════════════════════
  // 5) REPORTES Y DOCUMENTOS — todo lo de generar/analizar informes,
  // agrupado en UNA zona limpia con botones consistentes. A futuro acá
  // vivirá la carga de un PDF de OT para analizar (placeholder listo).
  // ════════════════════════════════════════════════════════════════
  html += _intelSection('Reportes y documentos', 'folder2-open', 'Genera informes del cliente y analiza documentos');
  html += _intelReportes(d);

  // ── Cómo trabaja tu Agente (ficha de instrucciones, al FINAL) ──
  html += _intelPrincipios(d);

  panel.innerHTML = html;
}

// ─── Encabezado de sección consistente del panel del Agente ─────────
// Todas las secciones (Chat, Atención, Diagnóstico, Reportes) abren con
// este mismo encabezado para dar jerarquía visual uniforme.
function _intelSection(titulo, icono, sub){
  return `
  <div class="intel-section">
    <div class="intel-section-bar"></div>
    <div class="intel-section-txt">
      <div class="intel-section-title"><i class="bi bi-${_intelEsc(icono)}"></i>${_intelEsc(titulo)}</div>
      ${sub ? `<div class="intel-section-sub">${_intelEsc(sub)}</div>` : ''}
    </div>
  </div>`;
}

// ─── Zona "Reportes y documentos" (agrupada y consistente) ──────────
// Reúne: (1) informe de gestión trimestral, (2) informe de gestión del
// cliente (PDF), (3) placeholder "Analizar OT (PDF)" para el futuro.
// Botones del mismo tamaño/estilo, alineados; nada desperdigado.
function _intelReportes(d){
  // (1) Informe trimestral (si el backend lo trae; si no, no se pinta).
  const trimestral = _intelInformeTrimestral(d.informe_trimestral);

  // (2) Informe de gestión COMPLETO del cliente (HTML imprimible → PDF).
  const informeFicha = `
  <div class="intel-card intel-report-card">
    <div class="intel-report-info">
      <div class="intel-report-title"><i class="bi bi-file-earmark-text text-danger"></i>Informe de gestión del cliente</div>
      <div class="intel-report-desc">Contrato, cláusulas, frecuencia, pagos, productos, historial y tus dolores — listo para imprimir o enviar.</div>
    </div>
    <button class="btn btn-ilus fw-bold intel-report-btn" onclick="intelInformeFicha()"><i class="bi bi-download me-1"></i>Generar informe (PDF)</button>
  </div>`;

  // (3) Placeholder: Analizar OT (PDF). Deshabilitado con tooltip "próximamente".
  const analizarOT = `
  <div class="intel-card intel-report-card intel-report-soon">
    <div class="intel-report-info">
      <div class="intel-report-title"><i class="bi bi-cloud-arrow-up text-muted"></i>Analizar OT (PDF) <span class="intel-chip" style="background:#f3f4f6;color:#6b7280">Próximamente</span></div>
      <div class="intel-report-desc">Sube el PDF de una OT y el agente lo analizará automáticamente para extraer trabajos, repuestos y observaciones.</div>
    </div>
    <button class="btn btn-outline-secondary fw-bold intel-report-btn" disabled title="Próximamente"><i class="bi bi-upload me-1"></i>Subir PDF</button>
  </div>`;

  return trimestral + informeFicha + analizarOT;
}

// ─── Tarjeta "Informe de gestión trimestral" ───────────────────────
// Usa d.informe_trimestral = {periodo, ya_generado, reporte_id, reporte_url}.
// 1 informe por trimestre (lo genera el backend de forma determinista y lo
// deja en la pestaña Reportes). Si no viene el dict, retorna '' (no se pinta).
function intelInformeFicha(){
  // Abre el Informe de Gestión del cliente (HTML imprimible → PDF en el navegador).
  if (typeof CID === 'undefined' || !CID) { ilusToast('Cliente no identificado', {type:'error'}); return; }
  window.open('/mantenciones/api/clientes/' + CID + '/informe-ficha', '_blank');
}

// ── Agente conversacional (chat determinista en el panel del Agente) ──
const INTEL_CHIPS = [
  "¿Cada cuánto se le hace mantención?",
  "¿Esta mantención es gratis o se cobra?",
  "¿Cómo van las garantías?",
  "¿Cuánto le cobro?",
  "¿Hay mantenciones atrasadas?",
  "¿Se está fugando a terceros?",
  "¿Qué tengo sin facturar?",
  "¿Cuándo es la próxima mantención?",
  "¿Cómo está este cliente?",
  "¿Qué hago ahora?",
];

function _intelChatPush(role, text){
  const log = document.getElementById('intelChatLog');
  if (!log) return null;
  const wrap = document.createElement('div');
  if (role === 'user'){
    wrap.style.cssText = 'text-align:right;margin:5px 0';
    wrap.innerHTML = `<span style="display:inline-block;background:#dc2626;color:#fff;padding:6px 11px;border-radius:14px 14px 2px 14px;font-size:.82rem;max-width:85%">${_intelEsc(text)}</span>`;
  } else {
    wrap.style.cssText = 'text-align:left;margin:5px 0';
    wrap.innerHTML = `<span style="display:inline-block;background:#f3f4f6;color:#111827;padding:7px 12px;border-radius:14px 14px 14px 2px;font-size:.82rem;max-width:92%;white-space:pre-wrap;line-height:1.45">${_intelEsc(text)}</span>`;
  }
  log.appendChild(wrap);
  log.scrollTop = log.scrollHeight;
  return wrap;
}

async function intelPreguntar(preset){
  if (typeof CID === 'undefined' || !CID) return;
  const input = document.getElementById('intelChatInput');
  const q = String(preset !== undefined ? preset : (input ? input.value : '')).trim();
  if (!q) return;
  if (input) input.value = '';
  _intelChatPush('user', q);
  const pending = _intelChatPush('bot', '…');
  try {
    const r = await fetch('/mantenciones/api/clientes/' + CID + '/preguntar', {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ pregunta: q })
    });
    const d = await r.json();
    if (pending) pending.remove();
    _intelChatPush('bot', (d && d.respuesta) ? d.respuesta : 'No pude responder con los datos actuales.');
  } catch(e){
    if (pending) pending.remove();
    _intelChatPush('bot', 'Error de red. Reintenta en unos segundos.');
  }
}

function intelInformePostservicio(vid){
  // Abre el Informe POST-SERVICIO de una visita realizada (imprimible → PDF).
  if (!vid) { ilusToast('Visita no identificada', {type:'error'}); return; }
  window.open('/mantenciones/api/visitas/' + vid + '/informe-postservicio', '_blank');
}

function _intelInformeTrimestral(it){
  if (!it || typeof it !== 'object') return '';
  const periodo = _intelEsc(it.periodo || '');
  if (!periodo) return '';
  const yaGenerado = it.ya_generado === true;
  const url = (it.reporte_url && String(it.reporte_url).trim()) ? String(it.reporte_url).trim() : '';

  // Cuerpo según estado (pendiente = CTA roja prominente; listo = estado verde).
  let cuerpo;
  if (!yaGenerado) {
    cuerpo = `
      <button type="button" class="btn btn-lg btn-ilus fw-bold" style="box-shadow:0 4px 14px rgba(220,38,38,.28)" onclick="intelInforme()">
        <i class="bi bi-file-earmark-text me-2"></i>Generar informe de ${periodo}
      </button>
      <div style="font-size:.74rem;color:#6b7280;margin-top:8px;line-height:1.45">
        El Agente analiza contrato, cláusulas, brechas y equipos, y lo deja en la pestaña Reportes.
      </div>`;
  } else {
    const verBtn = url
      ? `<button type="button" class="btn btn-sm btn-outline-dark fw-bold" onclick="window.open('${_intelEsc(url)}','_blank')"><i class="bi bi-box-arrow-up-right me-1"></i>Ver informe</button>`
      : `<button type="button" class="btn btn-sm btn-outline-dark fw-bold" onclick="switchTab('reportes'); if (typeof cargarReportes==='function') cargarReportes()"><i class="bi bi-folder2-open me-1"></i>Ver informe</button>`;
    cuerpo = `
      <div class="d-flex align-items-center gap-2 flex-wrap">
        <span class="fw-bold" style="color:#166534;font-size:.95rem"><i class="bi bi-check-circle-fill me-1"></i>Informe de ${periodo} generado</span>
        ${verBtn}
      </div>
      <div style="font-size:.74rem;color:#9ca3af;margin-top:8px"><i class="bi bi-info-circle me-1"></i>Podrás generar otro al cambiar de trimestre.</div>`;
  }

  return `
  <div class="intel-card mt-1" style="border:1px solid #fecdd3;background:linear-gradient(135deg,#fff,#fff5f5)">
    <div class="intel-card-title"><i class="bi bi-clipboard-data" style="color:#dc2626"></i>Informe de gestión trimestral</div>
    <div style="font-size:.86rem;color:#374151;margin-bottom:12px;line-height:1.5">Un informe por trimestre. Trimestre actual: <b>${periodo}</b>.</div>
    ${cuerpo}
  </div>`;
}

// ─── Historia (PASADO) vs Agenda (FUTURO): dos tarjetas diferenciadas ──
// d.historia = {total, items:[{fecha,dias_atras,tipo_label,cobertura_label,
//   costo,es_retroactiva,...}], ultima, dias_desde_ultima, gasto_total,
//   por_cobertura:{...}, retroactivas}
// d.agenda   = {total, items:[{fecha,dias_faltan,tipo_label,vencida,...}],
//   proxima, dias_a_proxima, vencidas_programadas}
// Diferenciación clara: la Historia va en verde/gris ("hecho", ícono
// calendar-check), la Agenda en azul ("programado", ícono calendar-event).
// Si una sección no viene en `d`, NO se pinta (cero datos inventados).
// Reusa las clases existentes (intel-grid/intel-card/intel-chip/intel-tl).
function _intelHistoriaAgenda(d){
  const h = (d && typeof d.historia === 'object' && d.historia) ? d.historia : null;
  const a = (d && typeof d.agenda   === 'object' && d.agenda)   ? d.agenda   : null;
  if (!h && !a) return '';   // backend no mandó ninguna → no pintamos nada

  // ── Tarjeta PASADO: Historia del cliente (verde/gris "hecho") ──
  let cardHist = '';
  if (h) {
    const total = parseInt(h.total, 10) || 0;
    const ddu   = (h.dias_desde_ultima == null) ? null : parseInt(h.dias_desde_ultima, 10);
    const gasto = parseFloat(h.gasto_total) || 0;
    const items = Array.isArray(h.items) ? h.items.slice(0, 8) : [];
    const retro = parseInt(h.retroactivas, 10) || 0;

    // Encabezado: total + última (hace N días).
    let head = `<span class="intel-bignum" style="font-size:1.9rem">${total}</span>
      <span style="font-size:.78rem;color:#6b7280;font-weight:600">gestión${total===1?'':'es'} realizada${total===1?'':'s'}</span>`;
    let sub = '';
    if (h.ultima) {
      const dduTxt = (ddu == null) ? '' : (ddu <= 0 ? ' · hoy' : ` · hace ${ddu} día${ddu===1?'':'s'}`);
      sub = `<div style="font-size:.74rem;color:#6b7280;margin-top:4px"><i class="bi bi-clock-history me-1" style="color:#16a34a"></i>Última: <b style="color:#0f172a">${_intelFechaEsc(h.ultima)}</b>${_intelEsc(dduTxt)}</div>`;
    }

    // Chips de cobertura. "Tercero (fuga)" en ROJO (es fuga, dato sensible).
    const cob = (h.por_cobertura && typeof h.por_cobertura === 'object') ? h.por_cobertura : {};
    const cobChips = Object.keys(cob).map(k => {
      const n = parseInt(cob[k], 10) || 0;
      if (n <= 0) return '';
      const esFuga = /fuga|tercero/i.test(k);
      const style = esFuga
        ? 'background:#fee2e2;color:#991b1b'
        : 'background:#dcfce7;color:#166534';
      const ico = esFuga ? 'box-arrow-up-right' : 'check2-circle';
      return `<span class="intel-pill" style="${style}"><i class="bi bi-${ico}"></i>${_intelEsc(k)}: <b>${n}</b></span>`;
    }).join('');

    // Gasto total (solo si > 0).
    const gastoLine = gasto > 0
      ? `<div style="font-size:.74rem;color:#374151;margin:8px 0 2px"><i class="bi bi-cash-stack me-1" style="color:#6b7280"></i>Gasto histórico: <b style="color:#0f172a">${_intelCLP(gasto)}</b></div>`
      : '';

    // Lista de items (orden ya viene desc). Ícono verde bi-calendar-check.
    let lista;
    if (total === 0 || !items.length) {
      lista = `<div style="font-size:.82rem;color:#6b7280;display:flex;align-items:center;gap:8px;padding:6px 0">
        <i class="bi bi-inbox" style="font-size:1.2rem;color:#9ca3af"></i>
        Aún no hay visitas registradas para este cliente.
      </div>`;
    } else {
      lista = `<div class="intel-tl" style="margin-top:8px">${items.map(it => {
        const costo = parseFloat(it.costo) || 0;
        const da = (it.dias_atras == null) ? null : parseInt(it.dias_atras, 10);
        const daTxt = (da == null) ? '' : (da <= 0 ? 'hoy' : `hace ${da} día${da===1?'':'s'}`);
        const cobLbl = (it.cobertura_label && String(it.cobertura_label).trim()) ? String(it.cobertura_label).trim() : '';
        const esFugaIt = /fuga|tercero/i.test(cobLbl);
        const tipoLbl = (it.tipo_label && String(it.tipo_label).trim()) ? String(it.tipo_label).trim() : (it.titulo || 'Gestión');
        return `<div class="intel-tl-row intel-tl-prox" style="padding-top:7px;padding-bottom:7px">
          <div style="display:flex;align-items:center;gap:6px;flex-wrap:wrap">
            <i class="bi bi-calendar-check" style="color:#16a34a"></i>
            <span style="color:#0f172a;font-weight:700;font-size:.82rem">${it.fecha ? _intelFechaEsc(it.fecha) : '—'}</span>
            <span style="color:#475569;font-size:.78rem">${_intelEsc(tipoLbl)}</span>
            ${it.es_retroactiva ? '<span class="intel-chip" style="background:#f3f4f6;color:#6b7280"><i class="bi bi-arrow-counterclockwise"></i>retroactiva</span>' : ''}
            ${it.id ? `<button class="btn btn-sm btn-outline-danger py-0 px-2 ms-auto" style="font-size:.68rem;line-height:1.5" onclick="intelInformePostservicio(${it.id})" title="Generar informe post-servicio"><i class="bi bi-file-earmark-text"></i> Informe</button>` : ''}
          </div>
          <div style="font-size:.72rem;color:#6b7280;margin-top:2px;padding-left:22px">
            ${cobLbl ? `<span style="color:${esFugaIt?'#dc2626':'#16a34a'};font-weight:700">${_intelEsc(cobLbl)}</span>` : ''}${(cobLbl && (costo>0||daTxt))?' · ':''}${costo>0 ? `<b style="color:#0f172a">${_intelCLP(costo)}</b>` : ''}${(costo>0&&daTxt)?' · ':''}${daTxt ? _intelEsc(daTxt) : ''}
          </div>
        </div>`;
      }).join('')}</div>`;
    }

    cardHist = `
    <div class="intel-card" style="border:1px solid #bbf7d0;background:linear-gradient(180deg,#fff,#f0fdf4)">
      <div class="intel-card-title"><i class="bi bi-clipboard-check" style="color:#16a34a"></i>📋 Historia del cliente
        <span class="intel-chip" style="background:#dcfce7;color:#166534">pasado</span>
        ${retro > 0 ? `<span class="intel-chip" style="background:#f3f4f6;color:#6b7280">${retro} retroactiva${retro===1?'':'s'}</span>` : ''}
      </div>
      <div class="d-flex align-items-baseline gap-2">${head}</div>
      ${sub}
      ${cobChips ? `<div style="margin-top:10px">${cobChips}</div>` : ''}
      ${gastoLine}
      ${lista}
    </div>`;
  }

  // ── Tarjeta FUTURO: Agenda (próximas) (azul "programado") ──
  let cardAgenda = '';
  if (a) {
    const total = parseInt(a.total, 10) || 0;
    const dap   = (a.dias_a_proxima == null) ? null : parseInt(a.dias_a_proxima, 10);
    const venc  = parseInt(a.vencidas_programadas, 10) || 0;
    const items = Array.isArray(a.items) ? a.items.slice(0, 8) : [];

    let head = `<span class="intel-bignum" style="font-size:1.9rem;color:#1e40af">${total}</span>
      <span style="font-size:.78rem;color:#6b7280;font-weight:600">programada${total===1?'':'s'}</span>`;
    let sub = '';
    if (a.proxima) {
      const dapTxt = (dap == null) ? '' : (dap <= 0 ? ' · hoy' : ` · en ${dap} día${dap===1?'':'s'}`);
      sub = `<div style="font-size:.74rem;color:#6b7280;margin-top:4px"><i class="bi bi-calendar-event me-1" style="color:#3b82f6"></i>Próxima: <b style="color:#0f172a">${_intelFechaEsc(a.proxima)}</b>${_intelEsc(dapTxt)}</div>`;
    }

    // Alerta de programadas ya vencidas (ámbar/roja).
    const alerta = venc > 0
      ? `<div style="margin-top:10px;font-size:.78rem;font-weight:700;color:#991b1b;background:#fef2f2;border:1px solid #fecaca;border-radius:10px;padding:8px 11px">
           <i class="bi bi-exclamation-triangle-fill me-1" style="color:#dc2626"></i>${venc} programada${venc===1?'':'s'} ya vencida${venc===1?'':'s'}
         </div>`
      : '';

    // Lista de items. Ícono azul bi-calendar-event.
    let lista;
    if (total === 0 || !items.length) {
      lista = `<div style="font-size:.82rem;color:#6b7280;display:flex;align-items:flex-start;gap:8px;padding:6px 0">
        <i class="bi bi-calendar-x" style="font-size:1.2rem;color:#9ca3af;flex-shrink:0"></i>
        <span>No hay visitas programadas. Genera una OT desde el tracking de fechas.</span>
      </div>`;
    } else {
      lista = `<div class="intel-tl" style="margin-top:8px">${items.map(it => {
        const df = (it.dias_faltan == null) ? null : parseInt(it.dias_faltan, 10);
        const esVencida = (it.vencida === true) || (df != null && df < 0);
        const tipoLbl = (it.tipo_label && String(it.tipo_label).trim()) ? String(it.tipo_label).trim() : (it.titulo || 'Mantención');
        let badge;
        if (esVencida) {
          const atraso = (df != null && df < 0) ? Math.abs(df) : null;
          badge = `<span class="intel-chip" style="background:#fee2e2;color:#991b1b"><i class="bi bi-exclamation-circle-fill"></i>vencida${atraso!=null?` hace ${atraso} día${atraso===1?'':'s'}`:''}</span>`;
        } else {
          const enTxt = (df == null) ? 'programada' : (df <= 0 ? 'hoy' : `en ${df} día${df===1?'':'s'}`);
          badge = `<span class="intel-chip" style="background:#dbeafe;color:#1e40af"><i class="bi bi-hourglass-split"></i>${_intelEsc(enTxt)}</span>`;
        }
        return `<div class="intel-tl-row ${esVencida?'intel-tl-vencida':'intel-tl-proy'}" style="padding-top:7px;padding-bottom:7px">
          <div style="display:flex;align-items:center;gap:6px;flex-wrap:wrap">
            <i class="bi bi-calendar-event" style="color:${esVencida?'#dc2626':'#3b82f6'}"></i>
            <span style="color:#0f172a;font-weight:700;font-size:.82rem">${it.fecha ? _intelFechaEsc(it.fecha) : '—'}</span>
            <span style="color:#475569;font-size:.78rem">${_intelEsc(tipoLbl)}</span>
            ${badge}
          </div>
        </div>`;
      }).join('')}</div>`;
    }

    cardAgenda = `
    <div class="intel-card ${venc > 0 ? 'intel-card-danger' : ''}" style="${venc > 0 ? '' : 'border:1px solid #bfdbfe;background:linear-gradient(180deg,#fff,#eff6ff)'}">
      <div class="intel-card-title"><i class="bi bi-calendar-week" style="color:#3b82f6"></i>🗓️ Agenda (próximas)
        <span class="intel-chip" style="background:#dbeafe;color:#1e40af">futuro</span>
      </div>
      <div class="d-flex align-items-baseline gap-2">${head}</div>
      ${sub}
      ${alerta}
      ${lista}
    </div>`;
  }

  // Lado a lado en desktop (intel-grid = 2 col → 1 col en ≤768px).
  return `<div class="intel-grid mt-1">${cardHist}${cardAgenda}</div>`;
}

// ─── Tarjeta "💰 Cierre de facturación" ────────────────────────────
// d.facturacion = {pendientes:int, items:[{id,fecha,dias,tipo_label,
//   estado_facturacion,ef_label,cobertura_label}], al_dia:bool,
//   mas_antigua_dias:int|null}
// Un servicio realizado NO está cerrado hasta que se factura (o se marca
// "no aplica"). Aquí solo mostramos el ESTADO del cierre — CERO montos
// (por ahora no cotizamos). Determinista: si no viene el dict, no pinta.
// Reusa intel-card / intel-card-title / intel-tl / intel-chip / intel-bignum.
function _intelFacturacion(d){
  const f = (d && typeof d.facturacion === 'object' && d.facturacion) ? d.facturacion : null;
  if (!f) return '';   // backend no mandó facturación → no pintamos nada

  // Botón "Ver facturación": si existe la pestaña Finanzas, cambiamos a
  // ella (switchTab revienta si el tab no existe en el DOM, por eso el
  // guard); si no, navegamos a la ficha del cliente.
  const verBtn = `<button type="button" class="btn btn-sm btn-outline-dark fw-bold"
        onclick="if (document.querySelector('.ftab-btn[data-tab=&quot;finanzas&quot;]')) { switchTab('finanzas'); if (typeof cargarFinanzas==='function') cargarFinanzas(); } else { window.location.href='/mantenciones/clientes/'+CID; }">
        <i class="bi bi-receipt me-1"></i>Ver facturación</button>`;

  const pend = parseInt(f.pendientes, 10) || 0;

  // ── Caso AL DÍA: tarjeta verde sobria ──
  if (pend <= 0 && f.al_dia === true) {
    return `
    <div class="intel-card mt-1" style="border:1px solid #bbf7d0;background:linear-gradient(180deg,#fff,#f0fdf4)">
      <div class="intel-card-title"><i class="bi bi-cash-coin" style="color:#16a34a"></i>💰 Cierre de facturación
        <span class="intel-chip" style="background:#dcfce7;color:#166534">al día</span>
      </div>
      <div style="font-size:.9rem;color:#166534;font-weight:700;display:flex;align-items:center;gap:8px">
        <i class="bi bi-check-circle-fill" style="font-size:1.15rem"></i>
        Facturación al día — no hay servicios pendientes de cerrar.
      </div>
    </div>`;
  }

  // ── Caso PENDIENTES: tarjeta en tono ALERTA (rojo/ámbar) ──
  if (pend > 0) {
    const masAnt = (f.mas_antigua_dias == null) ? null : parseInt(f.mas_antigua_dias, 10);
    const items  = Array.isArray(f.items) ? f.items.slice(0, 8) : [];

    // Color del estado de facturación de cada item (Sin cotizar = rojo,
    // Cotizado = ámbar, Con OC = azul). Lo decide ef_label, no inventamos.
    const efStyle = (lbl) => {
      const s = String(lbl || '').toLowerCase();
      if (/sin\s*cotiz/.test(s)) return 'background:#fee2e2;color:#991b1b';
      if (/cotizad/.test(s))     return 'background:#fff8e1;color:#b45309';
      if (/oc|orden\s*de\s*compra/.test(s)) return 'background:#dbeafe;color:#1e40af';
      return 'background:#f3f4f6;color:#6b7280';
    };

    let lista;
    if (!items.length) {
      lista = '';
    } else {
      lista = `<div class="intel-tl" style="margin-top:10px">${items.map(it => {
        const da = (it.dias == null) ? null : parseInt(it.dias, 10);
        const daTxt = (da == null) ? '' : (da <= 0 ? 'hoy' : `hace ${da} día${da===1?'':'s'}`);
        const tipoLbl = (it.tipo_label && String(it.tipo_label).trim()) ? String(it.tipo_label).trim() : 'Servicio';
        const efLbl   = (it.ef_label && String(it.ef_label).trim()) ? String(it.ef_label).trim() : '';
        const cobLbl  = (it.cobertura_label && String(it.cobertura_label).trim()) ? String(it.cobertura_label).trim() : '';
        return `<div class="intel-tl-row intel-tl-vencida" style="padding-top:7px;padding-bottom:7px">
          <div style="display:flex;align-items:center;gap:6px;flex-wrap:wrap">
            <i class="bi bi-receipt" style="color:#dc2626"></i>
            <span style="color:#0f172a;font-weight:700;font-size:.82rem">${it.fecha ? _intelFechaEsc(it.fecha) : '—'}</span>
            <span style="color:#475569;font-size:.78rem">${_intelEsc(tipoLbl)}</span>
            ${efLbl ? `<span class="intel-chip" style="${efStyle(efLbl)}">${_intelEsc(efLbl)}</span>` : ''}
          </div>
          <div style="font-size:.72rem;color:#6b7280;margin-top:2px;padding-left:22px">
            ${cobLbl ? `<span style="font-weight:700">${_intelEsc(cobLbl)}</span>` : ''}${(cobLbl && daTxt)?' · ':''}${daTxt ? _intelEsc(daTxt) : ''}
          </div>
        </div>`;
      }).join('')}</div>`;
    }

    const masAntLine = (masAnt != null)
      ? `<div style="font-size:.78rem;color:#991b1b;font-weight:700;margin-top:6px"><i class="bi bi-hourglass-bottom me-1"></i>El más antiguo lleva ${masAnt} día${masAnt===1?'':'s'}.</div>`
      : '';

    return `
    <div class="intel-card intel-card-danger mt-1">
      <div class="intel-card-title"><i class="bi bi-cash-coin" style="color:#dc2626"></i>💰 Cierre de facturación
        <span class="intel-chip" style="background:#fee2e2;color:#991b1b">proceso abierto</span>
      </div>
      <div class="d-flex align-items-baseline gap-2">
        <span class="intel-bignum" style="font-size:1.9rem;color:#dc2626">${pend}</span>
        <span style="font-size:.82rem;color:#991b1b;font-weight:700">servicio${pend===1?'':'s'} realizado${pend===1?'':'s'} SIN facturar</span>
      </div>
      ${masAntLine}
      <div style="font-size:.82rem;color:#374151;margin-top:8px;line-height:1.5">
        Un servicio no termina hasta facturarlo (o marcarlo <b>"no aplica"</b>).
      </div>
      ${lista}
      <div class="mt-3">${verBtn}</div>
    </div>`;
  }

  // Sin pendientes pero al_dia tampoco true → no afirmamos nada.
  return '';
}

// ─── Sección "🧭 Cómo trabaja tu Agente" ───────────────────────────
// d.principios = ["...","..."] → bullets sobrios (gris) que refuerzan que
// el Agente NO inventa y qué revisa. Es la "ficha de instrucciones" del
// Agente; va al FINAL del panel. Si no viene la lista, retorna ''.
function _intelPrincipios(d){
  const arr = (d && Array.isArray(d.principios)) ? d.principios.filter(x => x != null && String(x).trim()) : [];
  if (!arr.length) return '';

  const bullets = arr.map(p => `
    <li style="display:flex;align-items:flex-start;gap:8px;padding:5px 0;font-size:.82rem;color:#4b5563;line-height:1.5">
      <i class="bi bi-check2" style="color:#6b7280;font-size:.95rem;margin-top:2px;flex-shrink:0"></i>
      <span>${_intelEsc(String(p).trim())}</span>
    </li>`).join('');

  return `
  <div class="intel-card mt-3" style="border:1px solid #e5e7eb;background:#fafafa">
    <div class="intel-card-title" style="color:#6b7280"><i class="bi bi-shield-check" style="color:#6b7280"></i>🧭 Cómo trabaja tu Agente</div>
    <div style="font-size:.76rem;color:#9ca3af;margin-bottom:6px;line-height:1.5">
      Estas son las reglas del Agente: trabaja solo con datos reales de la ficha.
    </div>
    <ul style="list-style:none;margin:0;padding:0">${bullets}</ul>
  </div>`;
}

// POST /intel/informe-trimestral → genera el informe del trimestre actual y
// lo deja en Reportes. Mismo patrón que intelAccion (ilusLoader + ilusToast +
// re-render con la `intel` devuelta). Determinista en el backend: CERO IA aquí.
async function intelInforme(){
  const ok = await ilusConfirm({
    title: 'Generar informe trimestral',
    message: '¿Generar el informe de gestión de este trimestre y dejarlo en Reportes?',
    okLabel: 'Generar', cancelLabel: 'Cancelar',
  });
  if (!ok) return;

  if (window.ilusLoader) { try { ilusLoader.show('Generando informe…'); } catch(e){} }
  try {
    const r = await fetch(`/mantenciones/api/clientes/${CID}/intel/informe-trimestral`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
    });
    let data = {};
    try { data = await r.json(); } catch(e){ data = {}; }
    if (window.ilusLoader) { try { ilusLoader.hide(); } catch(e){} }

    // Éxito: re-render del panel con la inteligencia devuelta + avisos.
    if (r.ok && data && data.ok) {
      if (data.intel && typeof data.intel === 'object') {
        _intelCache = data.intel;
        _intelRender(data.intel);
      }
      if (window.ilusToast) {
        try { ilusToast('✓ Informe ' + (data.periodo || 'trimestral') + ' generado', { type:'success' }); } catch(e){}
        try { ilusToast('Disponible en la pestaña Reportes', { type:'info' }); } catch(e){}
      }
      return;
    }
    // Ya existe el informe de este trimestre (HTTP 409 / ya_generado).
    if (r.status === 409 || (data && data.ya_generado)) {
      if (window.ilusToast) { try { ilusToast((data && data.error) || 'Ya existe el informe de este trimestre', { type:'warning' }); } catch(e){} }
      return;
    }
    // Otro error: modal con detalle amigable.
    await ilusAlert({
      title: 'No se pudo generar',
      message: (data && data.error) || 'Intenta de nuevo',
      type: 'error',
    });
  } catch(e) {
    if (window.ilusLoader) { try { ilusLoader.hide(); } catch(e2){} }
    const msg = (e && e.message) ? e.message : 'Error de conexión';
    if (window.ilusToast) { try { ilusToast(msg, { type:'error' }); } catch(e2){} }
  }
}

// ─── AGENTE PROACTIVO: consultas accionables + desglose de score ───
// El backend devuelve `consultas` (preguntas que el agente necesita que
// resuelvas) y `score_detalle` (cómo se compone el número de salud).
// Aquí solo renderizamos + disparamos POST /intel/accion. Mismo estilo
// que el resto del panel (paleta #dc2626/#0a0a0a, escape XSS _intelEsc).

// Meta visual por severidad de consulta.
const _INTEL_SEV = {
  alta:  { color:'#dc2626', bg:'#fef2f2', icon:'exclamation-octagon-fill', chipBg:'#fee2e2', chipFg:'#991b1b', lbl:'Urgente' },
  media: { color:'#f59e0b', bg:'#fffbeb', icon:'exclamation-triangle-fill', chipBg:'#fff8e1', chipFg:'#92400e', lbl:'Importante' },
  baja:  { color:'#3b82f6', bg:'#eff6ff', icon:'info-circle-fill', chipBg:'#dbeafe', chipFg:'#1e40af', lbl:'Oportunidad' },
};
function _intelSevMeta(sev){ return _INTEL_SEV[(sev||'baja').toLowerCase()] || _INTEL_SEV.baja; }

// ¿Es una consulta de "atención" (va arriba) o de "oportunidad" (va abajo)?
function _intelEsAtencion(c){
  const sev = String((c && c.severidad) || '').toLowerCase();
  const tipo = String((c && c.tipo) || '').toLowerCase();
  return (sev === 'alta' || sev === 'media') && tipo !== 'oportunidad';
}

// Desglose del score → <details> "¿Por qué este número?" (dentro del hero oscuro).
function _intelScoreDetalle(sd, accentColor){
  sd = sd || {};
  const comps = Array.isArray(sd.componentes) ? sd.componentes : [];
  if (!comps.length && !sd.formula) return '';
  const rows = comps.map(c => {
    const ok = String(c.estado || '').toLowerCase() === 'ok';
    const col = ok ? '#86efac' : '#fca5a5';
    const ic  = ok ? 'check-circle-fill' : 'dash-circle-fill';
    const peso = (c.peso == null || c.peso === '') ? '' :
      `<span style="font-size:.66rem;font-weight:800;color:#9ca3af;flex-shrink:0">${(parseInt(c.peso,10) >= 0 ? '+' : '')}${parseInt(c.peso,10)||0}</span>`;
    return `
      <div style="display:flex;align-items:flex-start;gap:8px;padding:6px 0;border-bottom:1px solid rgba(255,255,255,.07)">
        <i class="bi bi-${ic}" style="color:${col};font-size:.95rem;flex-shrink:0;margin-top:1px"></i>
        <div style="flex:1;min-width:0">
          <div style="font-size:.78rem;font-weight:700;color:#f3f4f6">${_intelEsc(c.factor)}</div>
          ${c.detalle ? `<div style="font-size:.72rem;color:#9ca3af;line-height:1.4">${_intelEsc(c.detalle)}</div>` : ''}
        </div>
        ${peso}
      </div>`;
  }).join('');
  return `
  <details class="intel-score-detalle" style="margin-top:12px">
    <summary style="cursor:pointer;font-size:.76rem;font-weight:800;color:${accentColor||'#fff'};list-style:none;display:inline-flex;align-items:center;gap:6px">
      <i class="bi bi-question-circle"></i>¿Por qué este número?
    </summary>
    <div style="margin-top:10px;background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.08);border-radius:10px;padding:10px 13px">
      ${rows || ''}
      ${sd.formula ? `<div style="margin-top:9px;font-size:.7rem;color:#9ca3af;line-height:1.45"><i class="bi bi-calculator me-1"></i>${_intelEsc(sd.formula)}</div>` : ''}
    </div>
  </details>`;
}

// Botones / inputs de las acciones de UNA consulta.
function _intelAccionBtns(c){
  const acciones = Array.isArray(c.acciones) ? c.acciones : [];
  if (!acciones.length) return '';
  const cid = _intelEsc(c.id == null ? '' : c.id);
  const sev = _intelSevMeta(c.severidad);
  return `<div class="d-flex flex-wrap align-items-center gap-2 mt-2">` + acciones.map((a, idx) => {
    const tipo = String(a.accion || a.tipo || 'link').toLowerCase();
    const label = _intelEsc(a.label || 'Aplicar');
    // 1) link → simple <a href>, NO llama intelAccion.
    if (tipo === 'link' && a.url) {
      return `<a href="${_intelEsc(a.url)}" class="btn btn-xs btn-outline-dark fw-bold"><i class="bi bi-box-arrow-up-right me-1"></i>${label}</a>`;
    }
    // 2) inputs numéricos (set_frecuencia / set_gratuitas).
    if (tipo === 'set_frecuencia' || tipo === 'set_gratuitas') {
      const inpId = `intelInp_${cid}_${idx}`;
      const ph = _intelEsc(a.placeholder || (tipo === 'set_frecuencia' ? 'Meses' : 'Cantidad'));
      return `
        <span class="d-inline-flex align-items-center gap-1">
          <input type="number" id="${inpId}" min="1" step="1" placeholder="${ph}" class="form-control form-control-sm"
                 style="width:96px;height:32px;font-size:.82rem" inputmode="numeric">
          <button type="button" class="btn btn-xs btn-ilus fw-bold"
                  onclick="intelAccion('${_intelEsc(tipo)}', {__input:'${inpId}'})">${label}</button>
        </span>`;
    }
    // 3) registrar visita retroactiva (lleva fecha).
    if (tipo === 'registrar_visita_retro') {
      return `<button type="button" class="btn btn-xs btn-ilus fw-bold"
                onclick="intelAccion('registrar_visita_retro', {fecha:'${_intelEsc(a.fecha || '')}'})">
                <i class="bi bi-check-lg me-1"></i>${label}</button>`;
    }
    // 3b) generar OT desde el tracking de fechas (lleva fecha). Botón PROMINENTE.
    if (tipo === 'programar_ot') {
      return `<button type="button" class="btn btn-sm btn-ilus fw-bold intel-btn-ot"
                onclick="intelAccion('programar_ot', {fecha:'${_intelEsc(a.fecha || '')}'})">
                <i class="bi bi-clipboard-plus me-1"></i>${label || 'Generar OT ahora'}</button>`;
    }
    // 4) descartar consulta (sigue pendiente / no aplica).
    if (tipo === 'descartar_consulta') {
      return `<button type="button" class="btn btn-xs btn-outline-secondary fw-bold"
                onclick="intelAccion('descartar_consulta', {ref:'${_intelEsc(a.ref || '')}', __hideCard:'intelCard_${cid}'})">
                ${label}</button>`;
    }
    // Fallback: acción genérica sin input (manda solo la acción).
    return `<button type="button" class="btn btn-xs ${sev.color === '#dc2626' ? 'btn-ilus' : 'btn-outline-dark'} fw-bold"
              onclick="intelAccion('${_intelEsc(tipo)}', {})">${label}</button>`;
  }).join('') + `</div>`;
}

// Campos a rellenar inline (tipo:"completar_inline"). Cada campo es
// {campo, label, tipo}. Renderiza label + <input> + botón "Guardar" que
// dispara intelAccion('set_campo_cliente', {campo, valor}). El dueño lo pidió:
// "este cliente no tiene contactos, pásamelos por acá".
function _intelCamposInline(c){
  const campos = Array.isArray(c.campos) ? c.campos : [];
  if (!campos.length) return '';
  const cid = _intelEsc(c.id == null ? '' : c.id);
  return `<div class="d-flex flex-column gap-2 mt-2">` + campos.map((f, idx) => {
    const campo = _intelEsc(f.campo == null ? '' : f.campo);
    if (!campo) return '';
    const label = _intelEsc(f.label || f.campo || 'Dato');
    const itype = _intelEsc((f.tipo || 'text')).toLowerCase();
    const inpId = `intelCampo_${cid}_${idx}`;
    return `
      <div class="d-flex flex-wrap align-items-center gap-2">
        <label for="${inpId}" style="font-size:.82rem;font-weight:700;color:#374151;min-width:120px">${label}</label>
        <input type="${itype}" id="${inpId}" placeholder="${label}" class="form-control form-control-sm"
               style="flex:1;min-width:160px;max-width:280px;height:32px;font-size:.85rem">
        <button type="button" class="btn btn-xs btn-ilus fw-bold"
                onclick="intelAccion('set_campo_cliente', {campo:'${campo}', __input:'${inpId}', __inputAsValor:1})">
          <i class="bi bi-check-lg me-1"></i>Guardar</button>
      </div>`;
  }).join('') + `</div>`;
}

// Tarjeta de UNA consulta (pregunta + detalle + acciones).
function _intelConsultaCard(c){
  const sev = _intelSevMeta(c.severidad);
  const cid = _intelEsc(c.id == null ? '' : c.id);
  const esInline = String((c && c.tipo) || '').toLowerCase() === 'completar_inline';
  return `
  <div class="intel-consulta" id="intelCard_${cid}" style="border-left:4px solid ${sev.color};background:${sev.bg}">
    <i class="bi bi-${sev.icon}" style="font-size:1.35rem;color:${sev.color};flex-shrink:0;margin-top:1px"></i>
    <div style="flex:1;min-width:0">
      <div class="d-flex align-items-start justify-content-between gap-2 flex-wrap">
        <div class="fw-bold" style="font-size:.92rem;color:#0f172a;line-height:1.35">${_intelEsc(c.pregunta)}</div>
        <span class="intel-chip" style="background:${sev.chipBg};color:${sev.chipFg};flex-shrink:0">${_intelEsc(sev.lbl)}</span>
      </div>
      ${c.detalle ? `<div style="font-size:.8rem;color:#6b7280;line-height:1.45;margin-top:3px">${_intelEsc(c.detalle)}</div>` : ''}
      ${esInline ? _intelCamposInline(c) : ''}
      ${_intelAccionBtns(c)}
    </div>
  </div>`;
}

// Sección superior "El agente necesita tu ayuda" (severidad alta/media).
// Si no hay consultas accionables → estado positivo.
function _intelConsultas(consultas){
  const list = (Array.isArray(consultas) ? consultas : []).filter(_intelEsAtencion);
  if (!list.length) {
    return `
    <div class="intel-agente-ok">
      <i class="bi bi-robot" style="font-size:1.5rem;color:#16a34a;flex-shrink:0"></i>
      <div>
        <div class="fw-bold" style="font-size:.92rem;color:#166534">✓ Sin pendientes del agente</div>
        <div style="font-size:.8rem;color:#15803d">El Agente Proactivo no necesita tu ayuda con esta ficha por ahora.</div>
      </div>
    </div>`;
  }
  // Orden: alta primero, luego media.
  const rank = { alta:0, media:1 };
  list.sort((a,b) => (rank[(a.severidad||'').toLowerCase()] ?? 9) - (rank[(b.severidad||'').toLowerCase()] ?? 9));
  // El título grande lo aporta el encabezado de sección; aquí solo un
  // contador compacto para no duplicar el rótulo.
  return `
  <div class="intel-agente-box">
    <div class="intel-agente-head">
      <i class="bi bi-bell-fill" style="color:#dc2626"></i>
      <span>${list.length} pendiente${list.length===1?'':'s'} por resolver</span>
    </div>
    ${list.map(_intelConsultaCard).join('')}
  </div>`;
}

// Sección inferior "Oportunidades" (severidad baja / tipo oportunidad).
function _intelOportunidades(consultas){
  const list = (Array.isArray(consultas) ? consultas : []).filter(c => !_intelEsAtencion(c));
  if (!list.length) return '';
  return `
  <div class="intel-card mt-1" style="border:1px solid #bfdbfe;background:linear-gradient(135deg,#fff,#eff6ff)">
    <div class="intel-card-title"><i class="bi bi-lightbulb" style="color:#3b82f6"></i>Oportunidades</div>
    ${list.map(_intelConsultaCard).join('')}
  </div>`;
}

// POST /intel/accion → aplica la respuesta del usuario a una consulta del
// agente y re-renderiza el panel al instante con la inteligencia devuelta.
//   accion: string ("set_frecuencia" | "set_gratuitas" | "registrar_visita_retro"
//           | "programar_ot" | "set_campo_cliente" | "descartar_consulta" | ...).
//   params: { ...extra }. Soporta meta-claves NO enviadas al backend:
//     __input        → id de un <input>; su valor se manda como `valor`.
//     __inputAsValor → si está, el valor del __input se trata como TEXTO
//                      (no número): se valida no-vacío (para tel/email/text).
//     __hideCard     → id de la tarjeta a ocultar de inmediato (descartar).
async function intelAccion(accion, params){
  params = params || {};
  const inputId = params.__input;
  const asTexto = !!params.__inputAsValor;
  const hideCardId = params.__hideCard;
  const body = { accion };
  // Copiamos params reales (saltando las meta-claves internas).
  Object.keys(params).forEach(k => {
    if (k !== '__input' && k !== '__inputAsValor' && k !== '__hideCard') body[k] = params[k];
  });

  // Inputs: leer + validar antes de enviar.
  if (inputId) {
    const el = document.getElementById(inputId);
    if (asTexto) {
      // Campos de texto (tel/email/contacto): validar NO vacío.
      const v = el ? String(el.value || '').trim() : '';
      if (!v) {
        if (window.ilusToast) { try { ilusToast('Completa el campo antes de guardar', { type:'warning' }); } catch(e){} }
        if (el) { try { el.focus(); } catch(e){} }
        return;
      }
      body.valor = v;
    } else {
      // Inputs numéricos: validar > 0.
      const n = el ? parseInt(el.value, 10) : NaN;
      if (!Number.isFinite(n) || n <= 0) {
        if (window.ilusToast) { try { ilusToast('Ingresa un número mayor que 0', { type:'warning' }); } catch(e){} }
        if (el) { try { el.focus(); } catch(e){} }
        return;
      }
      body.valor = n;
    }
  }

  // Descartar: ocultar la tarjeta de inmediato (feedback óptimo).
  if (hideCardId) {
    const card = document.getElementById(hideCardId);
    if (card) card.style.display = 'none';
  }

  if (window.ilusLoader) { try { ilusLoader.show('Aplicando…'); } catch(e){} }
  try {
    const r = await fetch(`/mantenciones/api/clientes/${CID}/intel/accion`, {
      method:'POST',
      headers:{ 'Content-Type':'application/json' },
      body: JSON.stringify(body),
    });
    let data = {};
    try { data = await r.json(); } catch(e){ data = {}; }
    if (!r.ok || !data || data.error || data.ok === false) {
      // Falló: si habíamos ocultado la tarjeta, restaurarla.
      if (hideCardId) { const card = document.getElementById(hideCardId); if (card) card.style.display = ''; }
      const msg = (data && data.error) ? data.error : ('El servidor respondió ' + r.status);
      if (window.ilusToast) { try { ilusToast(msg, { type:'error' }); } catch(e){} }
      return;
    }
    // Éxito: re-render instantáneo con la inteligencia devuelta + refrescar cache.
    if (data.intel && typeof data.intel === 'object') {
      _intelCache = data.intel;
      _intelRender(data.intel);
    }
    if (window.ilusToast) { try { ilusToast(data.mensaje || '✓ Listo', { type:'success' }); } catch(e){} }
  } catch(e) {
    if (hideCardId) { const card = document.getElementById(hideCardId); if (card) card.style.display = ''; }
    const msg = (e && e.message) ? e.message : 'Error de conexión';
    if (window.ilusToast) { try { ilusToast(msg, { type:'error' }); } catch(e2){} }
  } finally {
    if (window.ilusLoader) { try { ilusLoader.hide(); } catch(e){} }
  }
}

// Marca el toggle de garantía del informe (Aplica/No aplica).
function _repSetGarantia(aplica) {
  const si = document.getElementById('rep_gar_si');
  const no = document.getElementById('rep_gar_no');
  if (si) si.checked = !!aplica;
  if (no) no.checked = !aplica;
}

function abrirNuevoReporte() {
  _repCurrentId = null;
  _repMaquinas = [{sku:'',descripcion:'',cantidad:1,modelo:'',serie:'',repuesto:'',garantia:'',observacion:''}];
  _repObjetivos = [''];
  _repTrabajos = [''];
  _repObservaciones = [''];
  _repFotosTemp = [];
  document.getElementById('repId').value = '';
  document.getElementById('repTipo').value = 'mantencion';
  document.getElementById('repEstado').value = 'borrador';
  document.getElementById('repTicket').value = '';
  { const _o = document.getElementById('repOt'); if (_o) _o.value = ''; }
  { const _e=document.getElementById('repOtDocEstado'); if(_e) _e.innerHTML='';
    const _i=document.getElementById('repOtDocInput'); if(_i) _i.value='';
    const _b=document.getElementById('repAnalisisBox'); if(_b){_b.style.display='none';_b.innerHTML='';} }
  document.getElementById('repAsunto').value = '';
  document.getElementById('repTecJunior').value = '';
  document.getElementById('repTecSenior').value = '';
  document.getElementById('repFechaSol').value = '';
  document.getElementById('repFechaIni').value = new Date().toISOString().split('T')[0];
  document.getElementById('repFechaCie').value = '';
  document.getElementById('repAntecedentes').value = '';
  document.getElementById('repFotosGrid').innerHTML = '';
  // Garantía: default "No aplica" en informe nuevo
  _repSetGarantia(false);
  repRenderMaquinas();
  repRenderLista('Objetivos');
  repRenderLista('Trabajos');
  repRenderLista('Observaciones');
  document.getElementById('modalRepTitulo').textContent = 'Nuevo Informe Post Servicio';
  document.getElementById('btnRepIA').style.display = 'none';
  new bootstrap.Modal(document.getElementById('modalReporte')).show();
}

async function editarReporte(rid) {
  const r = await fetch(`/mantenciones/api/reportes/${rid}`);
  const data = await r.json();
  _repCurrentId = rid;
  _repMaquinas = data.maquinas_json?.length ? data.maquinas_json : [{sku:'',descripcion:'',cantidad:1,modelo:'',serie:'',repuesto:'',garantia:'',observacion:''}];
  _repObjetivos = data.objetivos?.length ? data.objetivos : [''];
  _repTrabajos = data.trabajos?.length ? data.trabajos : [''];
  _repObservaciones = data.observaciones?.length ? data.observaciones : [''];
  document.getElementById('repId').value = rid;
  // Compat: reportes viejos con tipo='garantia' (cuando garantía era un tipo).
  // Ahora garantía es un flag aparte → mostramos el tipo como "Otro" y
  // encendemos el toggle de garantía. La garantía explícita (columna
  // garantia_aplica) tiene prioridad.
  let _repTipoVal = data.tipo || 'mantencion';
  let _repGarLegacy = false;
  if (_repTipoVal === 'garantia') { _repTipoVal = 'otro'; _repGarLegacy = true; }
  document.getElementById('repTipo').value = _repTipoVal;
  _repSetGarantia(data.garantia_aplica === true || _repGarLegacy);
  document.getElementById('repEstado').value = data.estado || 'borrador';
  document.getElementById('repTicket').value = data.ticket_num || '';
  { const _o = document.getElementById('repOt'); if (_o) _o.value = data.ot_num || ''; }
  { const _e=document.getElementById('repOtDocEstado'); if(_e) _e.innerHTML = data.ot_doc_url
      ? `<i class="bi bi-check-circle-fill text-success"></i> ${data.ot_doc_nombre||'PDF adjunto'}`
      : '<span class="text-danger">Falta adjuntar el PDF de la OT</span>';
    const _b=document.getElementById('repAnalisisBox'); if(_b){_b.style.display='none';_b.innerHTML='';} }
  document.getElementById('repAsunto').value = data.asunto || '';
  document.getElementById('repTecJunior').value = data.tecnico_junior || '';
  document.getElementById('repTecSenior').value = data.tecnico_senior || '';
  document.getElementById('repFechaSol').value = data.fecha_solicitado || '';
  document.getElementById('repFechaIni').value = data.fecha_inicio || '';
  document.getElementById('repFechaCie').value = data.fecha_cierre || '';
  document.getElementById('repAntecedentes').value = data.antecedentes || '';
  // Fotos
  const fotosGrid = document.getElementById('repFotosGrid');
  fotosGrid.innerHTML = (data.fotos||[]).map(f => `
    <div class="rep-foto-item">
      <img src="${f.url}" alt="${f.nombre}" onclick="window.open('${f.url}','_blank')">
      <button class="rep-foto-del" onclick="repDelFoto(${f.id}, this)" title="Eliminar">×</button>
    </div>`).join('');
  repRenderMaquinas();
  repRenderLista('Objetivos');
  repRenderLista('Trabajos');
  repRenderLista('Observaciones');
  document.getElementById('modalRepTitulo').textContent = `TICKET ${data.ticket_num||rid} — ${data.asunto||'Editar informe'}`;
  document.getElementById('btnRepIA').style.display = '';
  new bootstrap.Modal(document.getElementById('modalReporte')).show();
}

function repRenderMaquinas() {
  const cont = document.getElementById('repMaquinasList');
  if (!_repMaquinas.length) _repMaquinas = [{sku:'',descripcion:'',cantidad:1,modelo:'',serie:'',repuesto:'',garantia:'',observacion:''}];
  cont.innerHTML = _repMaquinas.map((m, i) => `
    <div class="rep-maquina-row">
      <div style="flex:1">
        <div class="row g-2 mb-1">
          <div class="col-3"><input type="text" class="form-control form-control-sm" placeholder="SKU"
            value="${m.sku||''}" onchange="_repMaquinas[${i}].sku=this.value;repGateIA()"></div>
          <div class="col-5"><input type="text" class="form-control form-control-sm" placeholder="Descripción equipo"
            value="${m.descripcion||''}" onchange="_repMaquinas[${i}].descripcion=this.value;repGateIA()"></div>
          <div class="col-2"><input type="number" class="form-control form-control-sm" placeholder="Cant" min="1"
            value="${m.cantidad||1}" onchange="_repMaquinas[${i}].cantidad=parseInt(this.value)||1"></div>
          <div class="col-2"><input type="text" class="form-control form-control-sm" placeholder="Modelo"
            value="${m.modelo||''}" onchange="_repMaquinas[${i}].modelo=this.value"></div>
        </div>
        <div class="row g-2">
          <div class="col-3"><input type="text" class="form-control form-control-sm" placeholder="N° Serie"
            value="${m.serie||''}" onchange="_repMaquinas[${i}].serie=this.value"></div>
          <div class="col-4"><input type="text" class="form-control form-control-sm" placeholder="Repuesto utilizado"
            value="${m.repuesto||''}" onchange="_repMaquinas[${i}].repuesto=this.value"></div>
          <div class="col-2"><input type="text" class="form-control form-control-sm" placeholder="Garantía"
            value="${m.garantia||''}" onchange="_repMaquinas[${i}].garantia=this.value"></div>
          <div class="col-3"><input type="text" class="form-control form-control-sm" placeholder="Observación"
            value="${m.observacion||''}" onchange="_repMaquinas[${i}].observacion=this.value"></div>
        </div>
      </div>
      <button class="btn btn-sm btn-outline-danger" onclick="_repMaquinas.splice(${i},1);repRenderMaquinas()" style="flex-shrink:0">
        <i class="bi bi-dash"></i>
      </button>
    </div>`).join('');
  repGateIA();
}

function repAddMaquina() {
  _repMaquinas.push({sku:'',descripcion:'',cantidad:1,modelo:'',serie:'',repuesto:'',garantia:'',observacion:''});
  repRenderMaquinas();
}

// ─── Gate del botón "Redactar con IA": exige al menos un equipo cargado ──────
// (Daniel: la IA depende del análisis de productos → no dejar redactar sin equipos.)
function repGateIA() {
  const btn = document.getElementById('btnRepRedactarIA');
  if (!btn) return;
  const hay = (_repMaquinas || []).some(m => (m.sku || '').trim() || (m.descripcion || '').trim());
  btn.disabled = !hay;
  btn.style.opacity = hay ? '' : '.5';
  btn.style.cursor = hay ? '' : 'not-allowed';
  btn.title = hay
    ? 'Redacta el informe con IA a partir de tus notas y los equipos'
    : 'Primero trae o agrega al menos un equipo de la ficha';
}

// ─── Traer equipos de la ficha del cliente al informe ───────────────────────
// Tabla scrollable con checkboxes (todos pre-seleccionados), seleccionar/
// deseleccionar todos y elegir cuáles incluir. Usa /clientes/<cid>/maquinas-list.
function _repEquiposInject() {
  if (document.getElementById('modalRepEquipos')) return;
  const html =
    '<div class="modal fade" id="modalRepEquipos" tabindex="-1">' +
    '<div class="modal-dialog modal-lg modal-dialog-centered modal-dialog-scrollable">' +
    '<div class="modal-content">' +
    '<div class="modal-header" style="background:#0a0a0a;color:#fff">' +
    '<h5 class="modal-title"><i class="bi bi-box-seam me-2"></i>Equipos de la ficha del cliente</h5>' +
    '<button type="button" class="btn-close btn-close-white" data-bs-dismiss="modal"></button></div>' +
    '<div class="modal-body" id="repEqBody"></div>' +
    '<div class="modal-footer">' +
    '<span class="me-auto small text-muted">Marca los equipos a incluir en el informe.</span>' +
    '<button class="btn btn-outline-secondary" data-bs-dismiss="modal">Cancelar</button>' +
    '<button class="btn btn-ilus fw-bold" id="repEqAgregar" onclick="repEquiposAgregar()">' +
    '<i class="bi bi-plus-lg me-1"></i>Agregar (<span id="repEqCount">0</span>)</button>' +
    '</div></div></div></div>';
  document.body.insertAdjacentHTML('beforeend', html);
}

async function repTraerEquipos() {
  _repEquiposInject();
  const body = document.getElementById('repEqBody');
  body.innerHTML = '<div class="text-center text-muted py-4"><span class="spinner-border spinner-border-sm me-2"></span>Cargando equipos de la ficha…</div>';
  const modal = bootstrap.Modal.getOrCreateInstance(document.getElementById('modalRepEquipos'));
  modal.show();
  try {
    const r = await fetch(`/mantenciones/api/clientes/${CID}/maquinas-list`);
    const eq = await r.json();
    if (!Array.isArray(eq) || !eq.length) {
      body.innerHTML = '<div class="text-center text-muted py-4"><i class="bi bi-inbox" style="font-size:2rem;opacity:.3"></i><div class="mt-2">Este cliente no tiene equipos registrados en la ficha.</div></div>';
      document.getElementById('repEqAgregar').disabled = true;
      document.getElementById('repEqCount').textContent = '0';
      return;
    }
    window._repEquiposCache = eq;
    body.innerHTML =
      '<div style="max-height:50vh;overflow:auto;border:1px solid #e5e7eb;border-radius:10px">' +
      '<table class="table table-sm table-hover align-middle mb-0" style="font-size:.85rem">' +
      '<thead style="position:sticky;top:0;z-index:1;background:#f8fafc">' +
      '<tr><th style="width:44px" class="text-center"><input type="checkbox" id="repEqAll" class="form-check-input" checked onchange="repEquiposToggleAll(this.checked)"></th>' +
      '<th>Equipo</th><th style="width:130px">SKU</th><th style="width:120px">N° Serie</th><th style="width:90px" class="text-center">Estado</th></tr></thead><tbody>' +
      eq.map((e, i) => {
        const baja = (e.estado_op || '').toLowerCase() === 'baja';
        return '<tr>' +
          `<td class="text-center"><input type="checkbox" class="rep-eq-chk form-check-input" data-i="${i}" checked onchange="repEquiposContar()"></td>` +
          `<td>${escHtml(e.nombre || '—')}</td>` +
          `<td class="font-monospace small">${escHtml(e.sku || '—')}</td>` +
          `<td class="small">${escHtml(e.serie || '—')}</td>` +
          `<td class="text-center"><span class="badge ${baja ? 'bg-secondary' : 'bg-success'}">${escHtml(e.estado_op || 'operativo')}</span></td>` +
          '</tr>';
      }).join('') +
      '</tbody></table></div>';
    document.getElementById('repEqAgregar').disabled = false;
    repEquiposContar();
  } catch (e) {
    body.innerHTML = `<div class="alert alert-danger m-2 small">No se pudieron cargar los equipos: ${escHtml(e.message)}</div>`;
  }
}

function repEquiposToggleAll(checked) {
  document.querySelectorAll('#modalRepEquipos .rep-eq-chk').forEach(c => { c.checked = checked; });
  repEquiposContar();
}

function repEquiposContar() {
  const n = document.querySelectorAll('#modalRepEquipos .rep-eq-chk:checked').length;
  const c = document.getElementById('repEqCount'); if (c) c.textContent = n;
  const all = document.getElementById('repEqAll');
  const total = document.querySelectorAll('#modalRepEquipos .rep-eq-chk').length;
  if (all) all.checked = (n === total && total > 0);
  const btn = document.getElementById('repEqAgregar'); if (btn) btn.disabled = (n === 0);
}

function repEquiposAgregar() {
  const chks = document.querySelectorAll('#modalRepEquipos .rep-eq-chk:checked');
  if (!chks.length) { ilusToast('No marcaste ningún equipo.', { type: 'warning' }); return; }
  const key = m => `${(m.sku || '').trim()}|${(m.serie || '').trim()}|${(m.descripcion || '').trim()}`.toLowerCase();
  // Conservar solo filas con datos (descartar las vacías iniciales).
  let base = (_repMaquinas || []).filter(m => (m.sku || '').trim() || (m.descripcion || '').trim() || (m.serie || '').trim());
  const existentes = new Set(base.map(key));
  let added = 0;
  chks.forEach(c => {
    const e = (window._repEquiposCache || [])[parseInt(c.dataset.i)];
    if (!e) return;
    const row = { sku: e.sku || '', descripcion: e.nombre || '', cantidad: 1, modelo: '',
                  serie: e.serie || '', repuesto: '', garantia: '', observacion: '' };
    if (existentes.has(key(row))) return;
    base.push(row); existentes.add(key(row)); added++;
  });
  _repMaquinas = base.length ? base
    : [{ sku: '', descripcion: '', cantidad: 1, modelo: '', serie: '', repuesto: '', garantia: '', observacion: '' }];
  repRenderMaquinas();
  bootstrap.Modal.getInstance(document.getElementById('modalRepEquipos'))?.hide();
  ilusToast(added ? `✓ ${added} equipo(s) agregado(s) al informe.` : 'Esos equipos ya estaban en el informe.',
            { type: added ? 'success' : 'info' });
}

function repRenderLista(tipo) {
  const arr = tipo==='Objetivos' ? _repObjetivos : tipo==='Trabajos' ? _repTrabajos : _repObservaciones;
  const cont = document.getElementById(`rep${tipo}List`);
  cont.innerHTML = arr.map((v, i) => `
    <div class="d-flex gap-2 mb-2">
      <input type="text" class="form-control form-control-sm" value="${v}"
             placeholder="Descripción…"
             onchange="${tipo==='Objetivos'?'_repObjetivos':tipo==='Trabajos'?'_repTrabajos':'_repObservaciones'}[${i}]=this.value">
      <button class="btn btn-xs btn-outline-danger" onclick="${tipo==='Objetivos'?'_repObjetivos':tipo==='Trabajos'?'_repTrabajos':'_repObservaciones'}.splice(${i},1);repRenderLista('${tipo}')">
        <i class="bi bi-dash"></i>
      </button>
    </div>`).join('');
}

function repAddItem(tipo) {
  if (tipo==='Objetivos')    _repObjetivos.push('');
  else if(tipo==='Trabajos') _repTrabajos.push('');
  else                       _repObservaciones.push('');
  repRenderLista(tipo);
}

function repMostrarFotoUpload() {
  const area = document.getElementById('repUploadFotoArea');
  area.style.display = area.style.display==='none' ? '' : 'none';
  if (!_repCurrentId) {
    const input = document.getElementById('repFotoInput');
    input.onchange = async () => {
      // Guardar primero para tener el ID, luego subir fotos
      await repGuardar(true);
    };
  } else {
    const input = document.getElementById('repFotoInput');
    input.onchange = async () => {
      for (const file of input.files) await repSubirFoto(file);
    };
  }
}

async function repSubirFoto(file) {
  if (!_repCurrentId) { alert('Guarda el informe primero'); return; }
  const fd = new FormData();
  fd.append('foto', file);
  const r = await fetch(`/mantenciones/api/reportes/${_repCurrentId}/fotos`,{method:'POST',body:fd});
  const data = await r.json();
  if (data.ok) {
    const grid = document.getElementById('repFotosGrid');
    const div = document.createElement('div');
    div.className = 'rep-foto-item';
    div.innerHTML = `<img src="${data.url}" alt="${data.nombre}" onclick="window.open('${data.url}','_blank')">
      <button class="rep-foto-del" onclick="repDelFoto(${data.id},this)">×</button>`;
    grid.appendChild(div);
  }
}

async function repDelFoto(fid, btn) {
  const ok = await ilusConfirm({
    title: 'Eliminar foto', message: '¿Eliminar foto?',
    okLabel: 'Eliminar', danger: true,
  });
  if (!ok) return;
  const r = await fetch(`/mantenciones/api/adjuntos/${fid}`,{method:'DELETE'});
  if (r.ok) btn.closest('.rep-foto-item').remove();
}

async function repGuardar(silencioso=false) {
  const data = {
    tipo:            document.getElementById('repTipo').value,
    estado:          document.getElementById('repEstado').value,
    ticket_num:      document.getElementById('repTicket').value.trim(),
    ot_num:          (document.getElementById('repOt')?.value || '').trim(),
    asunto:          document.getElementById('repAsunto').value.trim(),
    tecnico_junior:  document.getElementById('repTecJunior').value.trim(),
    tecnico_senior:  document.getElementById('repTecSenior').value.trim(),
    fecha_solicitado: document.getElementById('repFechaSol').value || null,
    fecha_inicio:    document.getElementById('repFechaIni').value || null,
    fecha_cierre:    document.getElementById('repFechaCie').value || null,
    antecedentes:    document.getElementById('repAntecedentes').value.trim(),
    objetivos:       _repObjetivos.filter(v=>v.trim()),
    trabajos:        _repTrabajos.filter(v=>v.trim()),
    observaciones:   _repObservaciones.filter(v=>v.trim()),
    maquinas:        _repMaquinas,
    // Garantía transversal a nivel de reporte (separada del tipo de servicio).
    garantia_aplica: document.getElementById('rep_gar_si')?.checked || false,
  };
  const rid = document.getElementById('repId').value;
  let url = `/mantenciones/api/clientes/${CID}/reportes`, method = 'POST';
  if (rid) { url = `/mantenciones/api/reportes/${rid}`; method = 'PUT'; }
  const r = await fetch(url, {method, headers:{'Content-Type':'application/json'}, body:JSON.stringify(data)});
  const resp = await r.json();
  if (resp.ok) {
    if (resp.id) {
      _repCurrentId = resp.id;
      document.getElementById('repId').value = resp.id;
      document.getElementById('btnRepIA').style.display = '';
    }
    if (!silencioso) {
      _invalidarReportes(); cargarReportes(true);
      bootstrap.Modal.getInstance(document.getElementById('modalReporte'))?.hide();
    }
  } else {
    ilusToast('Error guardando: ' + (resp.error||'desconocido'), { type:'error' });
  }
  return resp;
}

/* ── Generar informe DESDE una OT: autollena ticket+OT+cliente+equipos+trabajos ── */
async function generarDesdeOT(){
  const ot = await ilusPrompt({
    title: 'Generar informe desde OT',
    message: 'Ingresa el N° de OT del cliente. Traeré ticket, equipos y trabajos automáticamente; tú solo pules el detalle.',
    placeholder: 'Ej: 269', required: true,
  });
  if (!ot) return;
  ilusToast('Buscando la OT…', { type:'info', duration:1500 });
  try {
    const rv = await fetch(`/mantenciones/api/clientes/${CID}/visita-id?ot=${encodeURIComponent(ot)}`);
    const dv = await rv.json();
    if (!dv.ok){ ilusToast(dv.error || 'OT no encontrada', { type:'error' }); return; }
    const rg = await fetch(`/mantenciones/api/visitas/${dv.vid}/generar-informe`,
                           { method:'POST', headers:{'Content-Type':'application/json'}, body:'{}' });
    const dg = await rg.json();
    if (dg.ok){
      ilusToast(`✓ Informe creado (Ticket ${dg.ticket_num||'—'} / OT ${dg.ot_num||'—'}). Falta adjuntar el PDF de la OT.`, { type:'success' });
      _invalidarReportes(); await cargarReportes(true);
      editarReporte(dg.id);
    } else { ilusToast(dg.error || 'No se pudo generar el informe', { type:'error' }); }
  } catch(e){ ilusToast('Error de red al generar desde la OT', { type:'error' }); }
}

/* ── Adjuntar PDF de la OT (obligatorio para avanzar) ── */
async function subirOtDoc(){
  const rid = document.getElementById('repId').value;
  if (!rid){ ilusToast('Primero guarda el informe (para obtener su ID).', { type:'warning' }); return; }
  const inp = document.getElementById('repOtDocInput');
  if (!inp || !inp.files || !inp.files[0]){ ilusToast('Elige el archivo PDF de la OT', { type:'warning' }); return; }
  const fd = new FormData(); fd.append('doc', inp.files[0]);
  ilusToast('Subiendo documento de la OT…', { type:'info', duration:1500 });
  try {
    const r = await fetch(`/mantenciones/api/reportes/${rid}/ot-doc`, { method:'POST', body: fd });
    const d = await r.json();
    if (d.ok){
      const e = document.getElementById('repOtDocEstado');
      if (e) e.innerHTML = `<i class="bi bi-check-circle-fill text-success"></i> ${d.nombre}`;
      ilusToast('✓ Documento de la OT adjunto', { type:'success' });
    } else ilusToast(d.error || 'No se pudo subir el documento', { type:'error' });
  } catch(e){ ilusToast('Error de red', { type:'error' }); }
}

/* ── Agente interno (REGLAS, sin IA/tokens): valida obligatorios + diagnostica ── */
async function analizarInterno(){
  const rid = document.getElementById('repId').value;
  if (!rid){ ilusToast('Primero guarda el informe.', { type:'warning' }); return; }
  const btn = document.getElementById('btnRepInterno');
  const box = document.getElementById('repAnalisisBox');
  if (btn) btn.disabled = true;
  try {
    const r = await fetch(`/mantenciones/api/reportes/${rid}/analizar`,
                          { method:'POST', headers:{'Content-Type':'application/json'}, body:'{}' });
    const d = await r.json();
    if (!d.ok){ ilusToast(d.error || 'Error', { type:'error' }); return; }
    const a = d.analisis || {};
    const esc = (s)=>String(s||'').replace(/</g,'&lt;');
    let html = '';
    if (!d.completo){
      html += `<div style="background:#fef2f2;border:1px solid #fecaca;border-radius:10px;padding:10px 12px;margin-bottom:8px">
        <div class="fw-bold" style="color:#dc2626"><i class="bi bi-exclamation-triangle-fill me-1"></i>No se puede avanzar — faltan datos obligatorios</div>
        <ul style="margin:6px 0 0">${(d.faltantes||[]).map(f=>`<li>${esc(f)}</li>`).join('')}</ul></div>`;
    } else {
      html += `<div style="background:#dcfce7;border:1px solid #86efac;border-radius:10px;padding:8px 12px;margin-bottom:8px;color:#15803d;font-weight:700"><i class="bi bi-check-circle-fill me-1"></i>Informe completo — listo para generar el PDF</div>`;
    }
    html += `<div style="background:#f8fafc;border:1px solid #e5e7eb;border-radius:10px;padding:10px 12px">
      <div class="fw-bold mb-1"><i class="bi bi-cpu me-1" style="color:#dc2626"></i>Agente interno · ${a.indice_completitud||0}% completo</div>
      <div class="small mb-2" style="color:#374151">${esc(a.diagnostico)}</div>`;
    if ((a.acciones||[]).length){
      html += `<div class="small fw-bold mb-1">Recomendaciones:</div><ul class="small" style="margin:0">`;
      a.acciones.forEach(ac => { const col = ac.urgencia==='alta'?'#dc2626':(ac.urgencia==='media'?'#b45309':'#6b7280'); html += `<li style="color:${col}">${esc(ac.titulo)}</li>`; });
      html += `</ul>`;
    }
    html += `</div>`;
    if (box){ box.style.display='block'; box.innerHTML = html; }
    ilusToast(d.completo ? '✓ Informe completo' : 'Faltan datos obligatorios', { type: d.completo ? 'success':'warning' });
  } catch(e){ ilusToast('Error de red', { type:'error' }); }
  finally { if (btn) btn.disabled = false; }
}

async function eliminarReporte(rid) {
  const ok = await ilusConfirm({
    title: 'Eliminar informe', message: '¿Eliminar este informe?',
    okLabel: 'Eliminar', danger: true,
  });
  if (!ok) return;
  const r = await fetch(`/mantenciones/api/reportes/${rid}`,{method:'DELETE'});
  if (r.ok) { _invalidarReportes(); cargarReportes(true); }
}

// ─── Reporte: la IA REDACTA el contenido (objetivos/trabajos/observaciones) ──
// A diferencia de repAnalizarIA (diagnóstico de salud), esto genera el TEXTO del
// informe a partir de notas rápidas del técnico. No guarda: rellena el formulario.
async function repRedactarIA(btn) {
  const notas        = (document.getElementById('repIANotas')?.value || '').trim();
  const antecedentes = (document.getElementById('repAntecedentes')?.value || '').trim();
  const asunto       = (document.getElementById('repAsunto')?.value || '').trim();
  const hayMaquinas  = (_repMaquinas || []).some(m => (m.sku || '').trim() || (m.descripcion || '').trim());
  if (!notas && !antecedentes && !asunto && !hayMaquinas) {
    ilusToast('Escribe primero el asunto, la solicitud del cliente o unas notas para la IA.', { type:'warning' });
    return;
  }
  // Si ya hay contenido redactado, confirmar antes de reemplazar.
  const hayContenido = (_repObjetivos||[]).some(v=>v&&v.trim())
                    || (_repTrabajos||[]).some(v=>v&&v.trim())
                    || (_repObservaciones||[]).some(v=>v&&v.trim());
  if (hayContenido) {
    const ok = await ilusConfirm({
      title: 'Redactar con IA',
      message: 'Esto reemplazará Objetivos, Trabajos y Observaciones con la redacción de la IA.',
      sub: 'Podrás revisar y editar todo antes de guardar.',
      okLabel: 'Redactar', cancelLabel: 'Cancelar',
    });
    if (!ok) return;
  }
  const orig = btn.innerHTML;
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Redactando…';
  try {
    const r = await fetch(`/mantenciones/api/clientes/${CID}/reportes/redactar-ia`, {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify({
        tipo:           document.getElementById('repTipo')?.value || 'mantencion',
        garantia_aplica: document.getElementById('rep_gar_si')?.checked || false,
        asunto, antecedentes, notas,
        maquinas:       _repMaquinas,
      })
    });
    const data = await r.json();
    if (!data.ok) {
      await ilusAlert({ title:'No se pudo redactar', message: data.error || 'Error desconocido', type:'error' });
      return;
    }
    const res = data.resultado || {};
    if (res.asunto_sugerido && !asunto) {
      document.getElementById('repAsunto').value = res.asunto_sugerido;
    }
    if (res.antecedentes && !antecedentes) {
      document.getElementById('repAntecedentes').value = res.antecedentes;
    }
    if (res.objetivos     && res.objetivos.length)     _repObjetivos     = res.objetivos;
    if (res.trabajos      && res.trabajos.length)      _repTrabajos      = res.trabajos;
    if (res.observaciones && res.observaciones.length) _repObservaciones = res.observaciones;
    // Observación/recomendación por máquina: mapear por SKU.
    (res.maquinas || []).forEach(rm => {
      if (!rm.observacion) return;
      const m = (_repMaquinas || []).find(x => (x.sku || '') && x.sku === rm.sku);
      if (m) m.observacion = rm.observacion;
    });
    repRenderLista('Objetivos');
    repRenderLista('Trabajos');
    repRenderLista('Observaciones');
    repRenderMaquinas();
    ilusToast('✓ Borrador redactado por la IA. Revísalo y guarda.', { type:'success' });
  } catch (e) {
    await ilusAlert({ title:'Error de conexión', message: e.message || String(e), type:'error' });
  } finally {
    btn.disabled = false;
    btn.innerHTML = orig;
  }
}

async function repAnalizarIA() {
  const rid = document.getElementById('repId').value;
  if (!rid) { ilusToast('Guarda el informe primero', { type:'warning' }); return; }
  // Guardar primero
  await repGuardar(true);
  // Mostrar modal IA
  const modalIA = new bootstrap.Modal(document.getElementById('modalRepIA'));
  document.getElementById('modalRepIABody').innerHTML = `
    <div class="text-center py-5">
      <div class="spinner-border text-danger mb-3" style="width:3rem;height:3rem"></div>
      <div class="fw-semibold text-muted">Analizando con Claude AI…</div>
      <div class="text-muted mt-1" style="font-size:.82rem">Puede tomar hasta 30 segundos</div>
    </div>`;
  modalIA.show();
  try {
    const r = await fetch(`/mantenciones/api/reportes/${rid}/analizar-ia`,{method:'POST'});
    const data = await r.json();
    if (!data.ok) {
      document.getElementById('modalRepIABody').innerHTML = `<div class="alert alert-danger">${data.error}</div>`;
      return;
    }
    const res = data.resultado;
    const healthColor = res.indice_salud>=70?'#16a34a':res.indice_salud>=40?'#ea580c':'#dc2626';
    const urgIcon = {alta:'bi-exclamation-circle-fill',media:'bi-exclamation-triangle-fill',baja:'bi-info-circle-fill'};
    const urgColor = {alta:'#dc2626',media:'#f59e0b',baja:'#16a34a'};
    let html = `
      <div class="d-flex align-items-center gap-3 mb-4 p-3" style="background:linear-gradient(135deg,#f0fdf4,#eff6ff);border-radius:10px">
        <div style="width:70px;height:70px;border-radius:50%;background:${healthColor};display:flex;flex-direction:column;align-items:center;justify-content:center;color:#fff;flex-shrink:0">
          <div style="font-size:1.4rem;font-weight:900;line-height:1">${res.indice_salud}</div>
          <div style="font-size:.5rem;font-weight:700;letter-spacing:.5px">SALUD</div>
        </div>
        <div>
          <div style="font-size:.8rem;font-weight:700;color:#7c3aed;text-transform:uppercase">Estado: ${res.estado_flota||'—'}</div>
          <div style="font-size:.84rem;color:#374151;margin-top:4px;line-height:1.5">${res.diagnostico||''}</div>
        </div>
      </div>`;

    if (res.acciones?.length) {
      html += `<div class="mb-3"><div style="font-size:.72rem;font-weight:800;text-transform:uppercase;letter-spacing:.4px;color:#6b7280;margin-bottom:8px">Acciones recomendadas</div>`;
      res.acciones.forEach(a => {
        html += `<div class="rep-accion-row rep-urgencia-${a.urgencia}">
          <i class="bi ${urgIcon[a.urgencia]||'bi-circle'}" style="color:${urgColor[a.urgencia]||'#6b7280'};flex-shrink:0"></i>
          <div style="flex:1">
            <div style="font-size:.82rem;font-weight:700">${a.titulo}</div>
            <div style="font-size:.76rem;color:#374151;margin-top:2px">${a.descripcion}</div>
            <div style="font-size:.68rem;color:#9ca3af;margin-top:2px">Plazo: ${a.plazo||'—'} · Tipo: ${a.tipo||'—'}
              ${a.costo_estimado ? ` · Est: $${Number(a.costo_estimado).toLocaleString('es-CL')}` : ''}
            </div>
          </div>
        </div>`;
      });
      html += `</div>`;
    }

    if (res.piezas_criticas?.length) {
      html += `<div class="mb-3"><div style="font-size:.72rem;font-weight:800;text-transform:uppercase;letter-spacing:.4px;color:#6b7280;margin-bottom:6px">Piezas críticas</div>
        ${res.piezas_criticas.map(p=>`<span class="badge bg-danger me-1 mb-1">${p}</span>`).join('')}
      </div>`;
    }

    if (res.notificaciones_sugeridas?.length) {
      html += `<div class="alert alert-info mb-0" style="font-size:.78rem">
        <i class="bi bi-bell-fill me-1"></i>
        <strong>Notificaciones creadas:</strong> ${res.notificaciones_sugeridas.length} alerta(s) registrada(s) en el centro de notificaciones.
        <a href="/mantenciones/notificaciones" class="ms-2 fw-bold">Ver notificaciones →</a>
      </div>`;
    }

    document.getElementById('modalRepIABody').innerHTML = html;
    _invalidarReportes(); cargarReportes(true);
  } catch(e) {
    document.getElementById('modalRepIABody').innerHTML = `<div class="alert alert-danger">Error: ${e.message}</div>`;
  }
}

// ─── Reporte: Word + Enviar email ────────────────────────────────
function repDescargarWord() {
  const rid = document.getElementById('repId').value;
  if (!rid) { alert('Guarda el informe primero'); return; }
  window.open(`/mantenciones/api/reportes/${rid}/word`, '_blank');
}

async function repEnviarEmail() {
  const rid = document.getElementById('repId').value;
  if (!rid) { alert('Guarda el informe primero'); return; }
  await repGuardar(true);

  const html = `
    <div class="modal fade" id="modalEnvioRep" tabindex="-1">
      <div class="modal-dialog">
        <div class="modal-content">
          <div class="modal-header" style="background:#16a34a;color:#fff">
            <h5 class="modal-title"><i class="bi bi-envelope-paper me-2"></i>Enviar informe por email</h5>
            <button type="button" class="btn-close btn-close-white" data-bs-dismiss="modal"></button>
          </div>
          <div class="modal-body">
            <div class="mb-3">
              <label class="form-label small fw-semibold">Destinatarios (separados por coma)</label>
              <input type="text" id="enviar_to" class="form-control form-control-sm"
                     placeholder="cliente@empresa.cl, gerente@empresa.cl">
              <small class="text-muted">Si dejas vacío usa el email del cliente</small>
            </div>
            <div class="mb-3">
              <label class="form-label small fw-semibold">Asunto (opcional)</label>
              <input type="text" id="enviar_asunto" class="form-control form-control-sm">
            </div>
            <div class="mb-3">
              <label class="form-label small fw-semibold">Mensaje adicional (opcional)</label>
              <textarea id="enviar_mensaje" class="form-control form-control-sm" rows="3"
                        placeholder="Mensaje breve para el cliente"></textarea>
            </div>
            <div class="alert alert-info py-2" style="font-size:.78rem">
              <i class="bi bi-info-circle me-1"></i>Se adjuntará el informe en formato Word (.docx) y se incluirá la versión HTML en el cuerpo del email.
            </div>
          </div>
          <div class="modal-footer">
            <button type="button" class="btn btn-sm btn-outline-secondary" data-bs-dismiss="modal">Cancelar</button>
            <button class="btn btn-sm btn-success" id="btnDoEnvio">
              <i class="bi bi-send-fill me-1"></i>Enviar
            </button>
          </div>
        </div>
      </div>
    </div>`;
  // Remove old modal if exists
  const old = document.getElementById('modalEnvioRep');
  if (old) old.remove();
  document.body.insertAdjacentHTML('beforeend', html);
  const m = new bootstrap.Modal(document.getElementById('modalEnvioRep'));
  m.show();

  document.getElementById('btnDoEnvio').onclick = async () => {
    const btn = document.getElementById('btnDoEnvio');
    btn.disabled = true; btn.innerHTML = '<span class="spinner-border spinner-border-sm"></span>';
    const to = document.getElementById('enviar_to').value.trim();
    const payload = {
      destinatarios: to ? to.split(',').map(x => x.trim()).filter(Boolean) : null,
      asunto:        document.getElementById('enviar_asunto').value.trim() || null,
      mensaje:       document.getElementById('enviar_mensaje').value.trim(),
    };
    try {
      const r = await fetch(`/mantenciones/api/reportes/${rid}/enviar`, {
        method: 'POST',
        headers: {'Content-Type':'application/json'},
        body: JSON.stringify(payload)
      });
      const data = await r.json();
      if (data.ok) {
        m.hide();
        alert('✓ Informe enviado a: ' + (data.destinatarios||[]).join(', '));
      } else {
        alert('Error: ' + (data.error || 'No se pudo enviar'));
        btn.disabled = false; btn.innerHTML = '<i class="bi bi-send-fill me-1"></i>Enviar';
      }
    } catch (e) {
      alert('Error de red: ' + e.message);
      btn.disabled = false; btn.innerHTML = '<i class="bi bi-send-fill me-1"></i>Enviar';
    }
  };
}

// Habilitar botones Word/Email cuando hay reporte guardado
function _repToggleExportBtns(enabled) {
  const w = document.getElementById('btnRepWord');
  const e = document.getElementById('btnRepEnviar');
  if (w) w.disabled = !enabled;
  if (e) e.disabled = !enabled;
}
// Hook a repGuardar — observa el campo repId
(function() {
  const obs = new MutationObserver(() => {
    const v = document.getElementById('repId')?.value;
    _repToggleExportBtns(!!v);
  });
  document.addEventListener('DOMContentLoaded', () => {
    const el = document.getElementById('repId');
    if (el) obs.observe(el, {attributes:true, attributeFilter:['value']});
    // PERF 2026-06-03: prefetch del listado de Reportes en idle → al abrir la
    // pestaña los datos ya están cacheados (primera apertura instantánea, sin
    // competir con el render inicial de la ficha).
    if (typeof window.requestIdleCallback === 'function') {
      window.requestIdleCallback(() => { try { cargarReportes(); } catch(e){} }, { timeout: 4000 });
    } else {
      setTimeout(() => { try { cargarReportes(); } catch(e){} }, 2000);
    }
    // FIX 2026-06-06 (Daniel: el Agente quedaba "procesando" hasta Recalcular):
    // autocargar el panel del Agente en idle, igual que Reportes, para que el
    // diagnóstico esté listo aunque el tab "intel" sea el activo al cargar la ficha.
    if (typeof window.requestIdleCallback === 'function') {
      window.requestIdleCallback(() => { try { cargarInteligencia(); } catch(e){} }, { timeout: 4500 });
    } else {
      setTimeout(() => { try { cargarInteligencia(); } catch(e){} }, 2400);
    }
    // 2026-05-28 (Daniel — FASE 3) Polling de respaldo: subido de 800ms
    // a 5000ms. El MutationObserver de arriba ya detecta cambios en repId
    // en tiempo real; este interval es solo paracaídas para edge cases
    // raros donde se setea repId.value sin disparar el observer (assignar
    // .value directo en algunos browsers). 800ms cada modal abierto era
    // un waste innecesario de CPU; 5s es suficiente para el caso degradado.
    setInterval(() => {
      const modal = document.getElementById('modalReporte');
      if (modal && modal.classList.contains('show')) {
        _repToggleExportBtns(!!document.getElementById('repId')?.value);
      }
    }, 5000);
  });
})();

// ─── Adjuntos del contrato (lista compacta dentro del tab Contratos) ───
async function cargarAdjuntos(ctid) {
  const cont = document.getElementById('adjuntosLista');
  if (!cont) return;
  document.getElementById('cardAdjuntos').style.display = '';
  const r = await fetch(`/mantenciones/api/contratos/${ctid}/adjuntos`);
  const data = await r.json();
  if (!data.length) {
    cont.innerHTML = '<div class="text-muted small py-1">Sin adjuntos aún.</div>';
    return;
  }
  const iconos = {contrato:'file-earmark-pdf',imagen:'image',solicitud:'file-earmark-spreadsheet',otro:'paperclip'};
  const esSuper = !!DATA.is_superadmin;
  cont.innerHTML = data.map(a => {
    // URL del proxy (fuerza inline + valida permisos server-side)
    const urlProxy = `/mantenciones/api/adjuntos/${a.id}/archivo`;
    // Botón eliminar SOLO si superadmin
    const btnEliminar = esSuper
      ? `<button class="btn btn-xs btn-outline-danger" onclick="eliminarAdjunto(${a.id},this)" title="Eliminar (solo superadmin)"><i class="bi bi-trash"></i></button>`
      : '';
    // Botón descargar SOLO si superadmin
    const btnDescargar = esSuper
      ? `<a href="${urlProxy}?download=1" download class="btn btn-xs btn-outline-secondary" title="Descargar (solo superadmin)"><i class="bi bi-download"></i></a>`
      : '';
    // Mime hint para el visor (extensión desde archivo_nombre)
    const ext = (a.archivo_nombre||'').toLowerCase().split('.').pop();
    const tipoVisor = ['pdf'].includes(ext) ? 'pdf'
                     : ['jpg','jpeg','png','gif','webp'].includes(ext) ? 'imagen'
                     : ['docx','doc'].includes(ext) ? 'docx'
                     : ['xlsx','xls'].includes(ext) ? 'xlsx' : '';
    const nombreEsc = (a.nombre||'').replace(/'/g,"\\'");
    return `
      <div class="adj-row">
        <i class="bi bi-${iconos[a.tipo]||'paperclip'}" style="font-size:1.3rem;color:#6b7280;flex-shrink:0"></i>
        <div style="flex:1;min-width:0">
          <div style="font-size:.82rem;font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${a.nombre}</div>
          <div style="font-size:.68rem;color:#9ca3af">${a.tipo} · ${a.created_at} · ${a.created_by||''}</div>
        </div>
        <button class="btn btn-xs btn-outline-primary"
                onclick="verArchivoUDV({baseUrl:'${urlProxy}',nombre:'${nombreEsc}',tipo:'${tipoVisor}',hasCloud:${!!a.persistente},allowDownload:${esSuper},allowResubir:false})"
                title="Ver dentro del sistema">
          <i class="bi bi-eye"></i>
        </button>
        ${btnDescargar}
        ${btnEliminar}
      </div>`;
  }).join('');
}

function mostrarSubirAdjunto() {
  // Obtener el contrato activo
  const ctRow = document.querySelector('.contrato-pro');
  if (!ctRow) { alert('Sin contrato activo'); return; }
  const ctid = ctRow.id.replace('ct-','');
  const input = document.createElement('input');
  input.type = 'file'; input.accept = '.pdf,.doc,.docx,.jpg,.jpeg,.png,.xlsx';
  input.onchange = async () => {
    if (!input.files[0]) return;
    const fd = new FormData();
    fd.append('archivo', input.files[0]);
    fd.append('nombre', input.files[0].name);
    const r = await fetch(`/mantenciones/api/contratos/${ctid}/adjuntos`,{method:'POST',body:fd});
    const data = await r.json();
    if (data.ok) cargarAdjuntos(ctid);
    else alert('Error: ' + (data.error||'desconocido'));
  };
  input.click();
}

async function eliminarAdjunto(aid, btn) {
  const ok = await ilusConfirm({
    title: 'Eliminar adjunto', message: '¿Eliminar adjunto?',
    okLabel: 'Eliminar', danger: true,
  });
  if (!ok) return;
  const r = await fetch(`/mantenciones/api/adjuntos/${aid}`,{method:'DELETE'});
  if (r.ok) btn.closest('.adj-row').remove();
}

// ─── Actualizar estado Plan tab sin recargar ──────────────────
function _planTabActualizarStatus(ctAnalizado) {
  const el = document.getElementById('planCtStatus');
  if (!el) return;
  if (ctAnalizado) {
    el.innerHTML = '<span class="text-success"><i class="bi bi-check-circle-fill me-1"></i>Contrato analizado — plan completo disponible</span>';
  }
}

// ─── Dar de baja masiva todos los equipos de un cliente (solo superadmin) ──
async function bajaMasivaEquipos(cid) {
  const confirmText = await ilusPrompt({
    title: 'Dar de baja TODOS los equipos',
    message: 'Esta acción marcará como <strong style="color:#dc2626">BAJA</strong> todos los equipos activos de este cliente.',
    sub: 'Para confirmar, escribe <strong>BAJA TOTAL</strong> en el campo de abajo.',
    subHtml: true,
    placeholder: 'BAJA TOTAL',
    okLabel: 'Dar de baja',
    cancelLabel: 'Cancelar',
  });
  if (!confirmText) return;
  if (confirmText.trim().toUpperCase() !== 'BAJA TOTAL') {
    await ilusAlert({
      title: 'Confirmación incorrecta',
      message: 'Debes escribir exactamente <strong>BAJA TOTAL</strong> para confirmar.',
      subHtml: true,
      type: 'warning',
    });
    return;
  }
  ilusToast('Procesando baja masiva…', { type: 'info' });
  try {
    const r = await fetch(`/mantenciones/api/clientes/${cid}/equipos/baja-masiva`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ confirm_text: confirmText.trim() }),
    });
    const d = await r.json();
    if (d.ok) {
      await ilusAlert({
        title: 'Baja masiva completada',
        message: `Se dieron de baja <strong>${d.n}</strong> equipo${d.n !== 1 ? 's' : ''}. La página se recargará.`,
        subHtml: true,
        type: 'success',
      });
      location.reload();
    } else {
      await ilusAlert({ title: 'Error', message: d.error || 'No se pudo completar la baja masiva.', type: 'error' });
    }
  } catch (e) {
    await ilusAlert({ title: 'Error de red', message: 'No se pudo conectar con el servidor.', type: 'error' });
  }
}


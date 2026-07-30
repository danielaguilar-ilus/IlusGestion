// ══ Estado ══════════════════════════════════════════════════
const WZ = { paso:1, cid:null, ctid:null, equipos:[], aiData:null, calPreview:[], rut_override:false };
const adjFiles = [];
let eqCounter  = 0;
let erpEqSel   = {};

// ══════════════════════════════════════════════════════════════════
// DRAFT AUTOSAVE — localStorage para no perder trabajo si refrescas/cierras
// ══════════════════════════════════════════════════════════════════
const WZ_DRAFT_KEY = 'wiz_cliente_draft_v1';

function _wzCamposPaso1() {
  return [
    's1_razon','s1_rut','s1_email_empresa','s1_tel_empresa','s1_giro',
    's1_direccion','s1_comuna','s1_ciudad','s1_estado','s1_notas',
    's1_contacto_nombre','s1_contacto_cargo','s1_contacto_tel','s1_contacto_email',
    's1_contacto2_nombre','s1_contacto2_cargo','s1_contacto2_tel','s1_contacto2_email',
    'ct_nombre','ct_inicio','ct_vencimiento','ct_indefinido','ct_monto_mensual','ct_frecuencia',
  ];
}

let _wzDraftTimer = null;
function wzGuardarDraft() {
  try {
    const campos = {};
    _wzCamposPaso1().forEach(id => {
      const el = document.getElementById(id);
      if (!el) return;
      campos[id] = el.type === 'checkbox' ? el.checked : el.value;
    });
    const equipos = (typeof leerEquipos === 'function') ? leerEquipos() : [];
    const draft = {
      paso: WZ.paso || 1,
      ts: Date.now(),
      campos,
      equipos,
      rut_override: WZ.rut_override || false,
    };
    localStorage.setItem(WZ_DRAFT_KEY, JSON.stringify(draft));
  } catch(e) { /* localStorage lleno o no disponible — silencio */ }
}

function wzGuardarDraftDebounced() {
  if (_wzDraftTimer) clearTimeout(_wzDraftTimer);
  _wzDraftTimer = setTimeout(wzGuardarDraft, 600);
}

function wzCargarDraft() {
  try {
    const raw = localStorage.getItem(WZ_DRAFT_KEY);
    if (!raw) return;
    const d = JSON.parse(raw);
    if (!d || !d.campos) return;
    const ageMs = Date.now() - (d.ts || 0);
    const ageMin = Math.round(ageMs / 60000);
    const fechaTxt = new Date(d.ts || Date.now()).toLocaleString('es-CL');
    // Si tiene más de 7 días, ignorar (draft muy viejo)
    if (ageMs > 7 * 24 * 60 * 60 * 1000) {
      localStorage.removeItem(WZ_DRAFT_KEY);
      return;
    }
    const tieneRazon = (d.campos.s1_razon || '').trim();
    if (!tieneRazon && (d.equipos || []).length === 0) return;

    const resumen = (tieneRazon ? `Cliente: ${tieneRazon}\n` : '') +
                    `Equipos: ${(d.equipos||[]).length}\n` +
                    `Guardado: ${fechaTxt} (hace ${ageMin} min)`;

    if (confirm(
      `📋 Hay un borrador del wizard sin guardar:\n\n${resumen}\n\n` +
      `¿Recuperar este borrador?\n\n` +
      `• Aceptar = restaurar datos\n` +
      `• Cancelar = empezar desde cero (borra el borrador)`
    )) {
      // Restaurar campos del paso 1
      Object.entries(d.campos).forEach(([id, val]) => {
        const el = document.getElementById(id);
        if (!el) return;
        if (el.type === 'checkbox') el.checked = !!val;
        else el.value = val || '';
      });
      // Restaurar equipos
      if (typeof agregarFilaEquipo === 'function' && (d.equipos || []).length > 0) {
        // Limpiar tabla actual
        const tbody = document.getElementById('equiposTbody');
        if (tbody) tbody.innerHTML = '<tr id="eq-placeholder"><td colspan="11" class="text-center text-muted py-4">Sin equipos</td></tr>';
        d.equipos.forEach(eq => agregarFilaEquipo(eq));
      }
      WZ.rut_override = d.rut_override || false;
      // Refrescar validaciones
      if (typeof qfActualizar === 'function') qfActualizar();
      if (typeof qfRutValidar === 'function') {
        const rutEl = document.getElementById('s1_rut');
        if (rutEl) qfRutValidar(rutEl);
      }
    } else {
      localStorage.removeItem(WZ_DRAFT_KEY);
    }
  } catch(e) { /* JSON corrupto, ignorar */ }
}

function wzBorrarDraft() {
  try { localStorage.removeItem(WZ_DRAFT_KEY); } catch(_){}
}

// Conectar autosave a TODOS los inputs del paso 1 (+ contrato) en cuanto carga la página
document.addEventListener('DOMContentLoaded', () => {
  _wzCamposPaso1().forEach(id => {
    const el = document.getElementById(id);
    if (!el) return;
    el.addEventListener('input',  wzGuardarDraftDebounced);
    el.addEventListener('change', wzGuardarDraftDebounced);
  });
  // Intentar cargar draft existente
  setTimeout(wzCargarDraft, 300);
});

// Warning al cerrar la pestaña si hay datos sin guardar
window.addEventListener('beforeunload', (e) => {
  if (WZ.cid) return;   // ya guardado
  const razon = (document.getElementById('s1_razon') || {}).value;
  if (razon && razon.trim()) {
    e.preventDefault();
    e.returnValue = 'Tienes datos del wizard sin guardar. ¿Salir?';
    return e.returnValue;
  }
});

// ══ Progress ═════════════════════════════════════════════════
function actualizarProgress(p) {
  [1,2,3,4].forEach(i => {
    const d = document.getElementById(`dot-${i}`);
    d.classList.remove('active','done');
    if(i<p) d.classList.add('done'); else if(i===p) d.classList.add('active');
  });
  [1,2,3].forEach(i => {
    const l = document.getElementById(`line-${i}`);
    i<p ? l.classList.add('done') : l.classList.remove('done');
  });
}
function irPaso(n) {
  document.getElementById(`step-${WZ.paso}`).style.display='none';
  WZ.paso=n;
  document.getElementById(`step-${n}`).style.display='';
  actualizarProgress(n);
  window.scrollTo({top:0,behavior:'smooth'});
}

// ══ Tabs paso 1 ══════════════════════════════════════════════
function setTab(t) {
  document.getElementById('tab-manual').classList.toggle('active', t==='manual');
  document.getElementById('tab-contrato').classList.toggle('active', t==='contrato');
  document.getElementById('panel-manual').style.display   = t==='manual'   ? '' : 'none';
  document.getElementById('panel-contrato').style.display = t==='contrato' ? '' : 'none';
}

// ══════════════════════════════════════════════════════════════
// CALIDAD DE FICHA EN VIVO — coaching para usuario sin contexto
// ══════════════════════════════════════════════════════════════

// Validar RUT chileno (con dígito verificador)
function _validarRut(rut) {
  if (!rut) return false;
  const limpio = rut.replace(/[^0-9kK]/g,'').toUpperCase();
  if (limpio.length < 7 || limpio.length > 9) return false;
  const cuerpo = limpio.slice(0,-1);
  const dv = limpio.slice(-1);
  let suma = 0, mult = 2;
  for (let i = cuerpo.length - 1; i >= 0; i--) {
    suma += parseInt(cuerpo[i]) * mult;
    mult = mult === 7 ? 2 : mult + 1;
  }
  const resto = 11 - (suma % 11);
  const dvOk = resto === 11 ? '0' : resto === 10 ? 'K' : String(resto);
  return dv === dvOk;
}

function _dvEsperado(rut) {
  // Calcula el DV esperado para un RUT (módulo 11). Devuelve string DV o null.
  const limpio = String(rut||'').replace(/[^0-9kK]/g,'').toUpperCase();
  if (limpio.length < 2) return null;
  const cuerpo = limpio.slice(0,-1);
  if (!/^\d+$/.test(cuerpo)) return null;
  let suma = 0, mul = 2;
  for (let i = cuerpo.length - 1; i >= 0; i--) {
    suma += parseInt(cuerpo[i]) * mul;
    mul = mul === 7 ? 2 : mul + 1;
  }
  const resto = suma % 11;
  const calc = 11 - resto;
  return calc === 11 ? '0' : calc === 10 ? 'K' : String(calc);
}

function qfRutValidar(input) {
  const v = input.value.trim();
  const stat = document.getElementById('rut_status');
  if (!v) { stat.innerHTML = ''; input.style.borderColor=''; return; }
  if (_validarRut(v)) {
    stat.innerHTML = '<span class="text-success"><i class="bi bi-check-circle-fill"></i> Válido</span>';
    input.style.borderColor = '#16a34a';
  } else {
    const dvEsp = _dvEsperado(v);
    const sugerencia = dvEsp ?
      ` <small class="text-muted">(DV esperado: <strong>${dvEsp}</strong>)</small>` : '';
    stat.innerHTML = `<span class="text-danger"><i class="bi bi-x-circle-fill"></i> RUT inválido</span>${sugerencia}`;
    input.style.borderColor = '#dc2626';
  }
}

function qfRutFormat(input) {
  // Formato CRUDO estilo ERP: solo número-DV (sin puntos)
  // Ej: 76.123.456-7  →  76123456-7
  const limpio = input.value.replace(/[^0-9kK]/g,'').toUpperCase();
  if (limpio.length < 2) return;
  const cuerpo = limpio.slice(0,-1);
  const dv = limpio.slice(-1);
  input.value = cuerpo + '-' + dv;
}

function qfTelFormat(input) {
  // Normalizador inteligente de teléfono chileno.
  // Casos que maneja:
  //   "+569 1234 5678"  → +56 9 1234 5678   (ya válido)
  //   "+56912345678"    → +56 9 1234 5678
  //   "569 12345678"    → +56 9 1234 5678
  //   "912345678"       → +56 9 1234 5678   (móvil 9 dígitos sin prefijo)
  //   "12345678"        → +56 9 1234 5678   (8 dígitos: asume móvil sin el 9 inicial)
  //   "22XXXXXXX"       → +56 2 XXXX XXXX   (fijo Santiago, 9 dígitos)
  //   "32XXXXXXX"       → +56 3 XXXX XXXX   (fijo regiones)
  const stat = document.getElementById('tel_status');
  // Limpiar: solo dígitos y '+' inicial si existe
  let raw = (input.value || '').replace(/[^\d+]/g, '');
  // Quitar + intermedios (solo el primero cuenta)
  if (raw.includes('+')) raw = '+' + raw.replace(/\+/g, '');

  // Extraer solo los dígitos del cuerpo nacional (sin código país)
  let national = '';
  if (raw.startsWith('+56')) national = raw.slice(3);
  else if (raw.startsWith('56') && raw.length >= 10) national = raw.slice(2);
  else if (raw.startsWith('+')) national = raw.slice(1);   // otro país → no formatear
  else national = raw;

  // Si tiene + de otro país (no +56) lo dejo sin tocar
  if (raw.startsWith('+') && !raw.startsWith('+56')) {
    if (stat) stat.innerHTML = '';
    return;
  }

  // Auto-añadir el "9" inicial para móviles tipeados sin él
  if (national.length === 8 && /^[2-9]/.test(national)) {
    national = '9' + national;
  }

  // Validar largo final (9 dígitos = código de área + 8)
  let formatted = '';
  let valid = false;
  if (national.length === 9) {
    const area = national[0];
    const rest = national.slice(1);
    formatted = `+56 ${area} ${rest.slice(0,4)} ${rest.slice(4)}`;
    valid = true;
  } else if (national.length > 0 && national.length < 9) {
    // Aún incompleto: muestro lo que llevamos sin reformatear agresivo
    formatted = '+56 ' + national;
  } else {
    // Más de 9 dígitos: dejo lo que escribió tal cual (raro)
    formatted = raw;
  }

  input.value = formatted;
  if (stat) {
    stat.innerHTML = valid
      ? '<span class="text-success"><i class="bi bi-check-circle-fill"></i></span>'
      : '';
  }
}

// Estado pill update
function _updateEstadoPill() {
  const sel = document.getElementById('s1_estado');
  if (!sel) return;
  const v = sel.value;
  const lbl = sel.options[sel.selectedIndex].text.replace('● ','');
  const pill = document.getElementById('qfEstadoPill');
  const lblEl = document.getElementById('qfEstadoLbl');
  if (pill && lblEl) {
    pill.className = 'estado-pill estado-' + v;
    pill.style.marginTop = '4px';
    lblEl.textContent = lbl;
  }
}

// Cálculo de score y render del sidebar
function qfActualizar() {
  _updateEstadoPill();

  const $ = id => document.getElementById(id);
  const val = id => ($(id) || {}).value || '';
  const has = id => val(id).trim().length > 0;

  const isEmail = e => /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(e);
  const isTel   = t => /^\+?[0-9\s\-]{8,16}$/.test(t);

  // Datos críticos (peso 55%)
  const rutOk      = _validarRut(val('s1_rut'));
  const razonOk    = val('s1_razon').trim().length >= 3;
  const direccionOk = has('s1_direccion');
  const comunaOk   = has('s1_comuna');
  const ciudadOk   = has('s1_ciudad');
  const contactoOk = val('s1_contacto_nombre').trim().length >= 3;

  // Datos recomendados (peso 45%)
  const giroOk        = has('s1_giro');
  const emailEmpOk    = isEmail(val('s1_email_empresa'));
  const telEmpOk      = isTel(val('s1_tel_empresa'));
  const cargoOk       = val('s1_contacto_cargo').trim().length >= 2;
  const telOk         = isTel(val('s1_contacto_tel'));
  const emailOk       = isEmail(val('s1_contacto_email'));
  const cont2Ok       = val('s1_contacto2_nombre').trim().length >= 3 &&
                        (isEmail(val('s1_contacto2_email')) || isTel(val('s1_contacto2_tel')));

  const criticos = [
    {key:'razon',     label:'Razón social',          ok:razonOk,
     msg:razonOk?'Identificación legal del cliente':'Mínimo 3 caracteres', field:'s1_razon'},
    {key:'rut',       label:'RUT válido',            ok:rutOk,
     msg:rutOk?'RUT con dígito verificador correcto':'Formato sin puntos: 76123456-7', field:'s1_rut'},
    {key:'direccion', label:'Dirección',             ok:direccionOk,
     msg:direccionOk?'Dirección registrada':'Dónde se realizan los servicios', field:'s1_direccion'},
    {key:'comuna',    label:'Comuna',                ok:comunaOk,
     msg:comunaOk?'':'Se completa al elegir dirección', field:'s1_comuna'},
    {key:'ciudad',    label:'Ciudad / Región',       ok:ciudadOk,
     msg:ciudadOk?'':'Se completa al elegir dirección', field:'s1_ciudad'},
    {key:'contacto',  label:'Contacto principal',    ok:contactoOk,
     msg:contactoOk?'Persona de coordinación':'Nombre del contacto principal', field:'s1_contacto_nombre'},
  ];

  const recomendados = [
    {key:'giro',         label:'Giro / actividad',     ok:giroOk,
     msg:giroOk?'Desde ERP':'Lo trae el ERP', field:'s1_giro'},
    {key:'emailEmpresa', label:'Email empresa',        ok:emailEmpOk,
     msg:emailEmpOk?'Email institucional':'Lo trae el ERP — emails de la empresa', field:'s1_email_empresa'},
    {key:'telEmpresa',   label:'Teléfono empresa',     ok:telEmpOk,
     msg:telEmpOk?'':'Teléfono central', field:'s1_tel_empresa'},
    {key:'cargo',        label:'Cargo del contacto',   ok:cargoOk,
     msg:cargoOk?'':'Ej: Administrador, Jefe operaciones', field:'s1_contacto_cargo'},
    {key:'telefono',     label:'Tel. contacto',        ok:telOk,
     msg:telOk?'Validado':'Formato +56 9 XXXX XXXX', field:'s1_contacto_tel'},
    {key:'email',        label:'Email contacto',       ok:emailOk,
     msg:emailOk?'Email válido':'Para reportes y notificaciones', field:'s1_contacto_email'},
    {key:'contacto2',    label:'Contacto secundario',  ok:cont2Ok,
     msg:cont2Ok?'Backup registrado':'Backup útil (gerente, mantenedor)', field:'s1_contacto2_nombre'},
  ];

  const renderItem = (it) => {
    const icon = it.ok
      ? '<div class="qf-icon qf-icon-ok"><i class="bi bi-check-lg"></i></div>'
      : '<div class="qf-icon qf-icon-pending"><i class="bi bi-dash"></i></div>';
    const action = it.ok
      ? ''
      : `<a class="qf-action" onclick="qfFocusField('${it.field}');return false">Agregar →</a>`;
    return `<div class="qf-item ${it.ok?'':'actionable'}" ${it.ok?'':`onclick="qfFocusField('${it.field}')"`}>
      ${icon}
      <div class="qf-text">
        <strong>${it.label}</strong>
        <small>${it.msg}</small>
      </div>
      ${action}
    </div>`;
  };

  const elCrit = $('qfCriticos');
  const elRec  = $('qfRecomendados');
  if (elCrit) elCrit.innerHTML = criticos.map(renderItem).join('');
  if (elRec)  elRec.innerHTML  = recomendados.map(renderItem).join('');

  // Score: críticos pesan 55, recomendados 45
  const critOk = criticos.filter(c=>c.ok).length;
  const recOk  = recomendados.filter(c=>c.ok).length;
  const pct = Math.round((critOk / criticos.length) * 55 + (recOk / recomendados.length) * 45);

  const ringEl = $('qfRing');
  const pctEl  = $('qfPct');
  if (ringEl && pctEl) {
    pctEl.textContent = pct + '%';
    const color = pct >= 80 ? '#16a34a' : pct >= 50 ? '#f59e0b' : '#cc0000';
    ringEl.style.background = `conic-gradient(${color} ${pct*3.6}deg, #eaecf0 ${pct*3.6}deg)`;
  }
  const topPct = $('qfTopPct'); if (topPct) topPct.textContent = pct + '%';

  // Mensaje de estado y próxima acción
  const statusText = $('qfStatusText');
  const nextMsg = $('qfNextActionMsg');
  const topMsg = $('qfTopMsg');
  if (statusText) {
    if (pct < 20) statusText.textContent = 'Empezando…';
    else if (pct < 50) statusText.textContent = 'Datos básicos en progreso';
    else if (pct < 80) statusText.textContent = 'Datos suficientes';
    else if (pct < 100) statusText.textContent = '¡Casi completo!';
    else statusText.textContent = 'Ficha 100% lista ✓';
  }

  // Próxima acción dinámica
  let nextAction = null;
  for (const c of criticos) { if (!c.ok) { nextAction = c; break; } }
  if (!nextAction) for (const r of recomendados) { if (!r.ok) { nextAction = r; break; } }

  if (nextMsg) {
    if (nextAction) {
      nextMsg.innerHTML = `Falta <strong>${nextAction.label.toLowerCase()}</strong> · ${nextAction.msg}`;
    } else {
      nextMsg.innerHTML = '✓ Ficha completa. Puedes pasar al paso 2 (equipos).';
    }
  }
  if (topMsg) {
    if (pct < 30) topMsg.innerHTML = 'Empieza buscando el cliente por <strong>RUT o nombre</strong>. El sistema autocompletará los datos disponibles.';
    else if (nextAction) topMsg.innerHTML = `Falta agregar <strong>${nextAction.label.toLowerCase()}</strong>.`;
    else topMsg.innerHTML = '<strong>Excelente</strong> — la ficha está lista para pasar al paso 2.';
  }
}

function qfFocusField(fieldId) {
  const el = document.getElementById(fieldId);
  if (!el) return;
  el.focus();
  el.scrollIntoView({behavior:'smooth', block:'center'});
  el.classList.add('field-highlight');
  setTimeout(()=>el.classList.remove('field-highlight'), 1500);
}

// Inicializar al cargar
document.addEventListener('DOMContentLoaded', () => {
  qfActualizar();
  _wzInitDireccionGoogle();
});
window.qfActualizar = qfActualizar;

// Dirección del paso 1 validada con Google Places (mismo motor que la OT) →
// muestra "Powered by Google", autollena comuna/ciudad + lat/lng/place_id.
function _wzInitDireccionGoogle() {
  if (typeof ilusPlacesAutocomplete !== 'function') {
    if (window.__ilusGmapsPending) window.__ilusGmapsPending.push(_wzInitDireccionGoogle);
    return;
  }
  const input = document.getElementById('s1_direccion');
  if (!input || input.dataset.placesBound === '1') return;
  input.dataset.placesBound = '1';
  ilusPlacesAutocomplete('s1_direccion', {
    country: 'cl',
    types: ['address'],
    onPlaceSelected: function (place) {
      const set = (id, v) => { const e = document.getElementById(id); if (e) e.value = v; };
      set('s1_direccion_lat',      place.lat || '');
      set('s1_direccion_lng',      place.lng || '');
      set('s1_direccion_place_id', place.place_id || '');
      const comps = place.componentes || [];
      const pick = function () {
        for (let i = 0; i < arguments.length; i++) {
          const c = comps.find(x => (x.types || []).indexOf(arguments[i]) >= 0);
          if (c) return c.long_name;
        }
        return '';
      };
      const comuna = pick('administrative_area_level_3', 'locality', 'sublocality_level_1');
      const ciudad = pick('locality', 'administrative_area_level_2') || comuna;
      const setIf = (id, v) => {
        const e = document.getElementById(id);
        if (e && v) { e.value = v; e.style.borderColor = '#1a7a1a'; e.style.background = '#f1f8e9'; }
      };
      setIf('s1_comuna', comuna);
      setIf('s1_ciudad', ciudad);
      const ok = document.getElementById('dir_ok'); if (ok) ok.style.display = '';
      const hint = document.getElementById('s1_direccion_hint');
      if (hint) {
        const la = (typeof place.lat === 'number') ? place.lat.toFixed(4) : '?';
        const ln = (typeof place.lng === 'number') ? place.lng.toFixed(4) : '?';
        hint.innerHTML = '<i class="bi bi-check-circle-fill text-success me-1"></i>' +
          'Dirección verificada · <small>' + la + ', ' + ln + '</small>';
      }
      if (window.qfActualizar) window.qfActualizar();
    },
    onNoSelection: function () {
      const hint = document.getElementById('s1_direccion_hint');
      if (hint) hint.innerHTML = '<i class="bi bi-exclamation-triangle text-warning me-1"></i>' +
        'Selecciona una opción del menú para validar la dirección.';
    }
  });
}

// ══ AUTOCOMPLETE ═════════════════════════════════════════════
let acTimer = null;
let acResults = [];
let acIdx = -1;

function acDebounce() {
  clearTimeout(acTimer);
  acTimer = setTimeout(acBuscar, 300);
}

async function acBuscar() {
  const q = document.getElementById('ac_input').value.trim();
  const drop = document.getElementById('ac_dropdown');
  if(q.length < 2) { drop.style.display='none'; return; }
  drop.innerHTML = '<div class="ac-loading"><span class="spinner-border spinner-border-sm me-1"></span>Buscando en ERP…</div>';
  drop.style.display = 'block';   // ← IMPORTANTE: forzar visible (CSS tiene display:none)
  try {
    const r = await fetch(`/mantenciones/api/clientes/autocomplete?q=${encodeURIComponent(q)}`);
    if(!r.ok) throw new Error(`HTTP ${r.status}`);
    acResults = await r.json();
  } catch(err) {
    drop.innerHTML = `<div class="ac-loading text-danger"><i class="bi bi-exclamation-triangle me-1"></i>Error al buscar (${err.message})</div>`;
    return;
  }
  acIdx = -1;
  if(!acResults.length) {
    drop.innerHTML = `<div class="ac-loading text-muted">
      <i class="bi bi-search me-1"></i>Sin resultados para "<strong>${q}</strong>"<br>
      <span style="font-size:.72rem">Ingresa los datos manualmente o verifica el RUT/nombre.</span>
    </div>`;
    return;
  }
  drop.innerHTML = acResults.map((c,i) => {
    // Construir línea de meta: RUT | Región | Comuna
    const meta = [
      c.rut    ? `RUT: <strong>${c.rut}</strong>` : null,
      c.region ? `Región: ${c.region}`             : null,
      c.comuna ? `Comuna: ${c.comuna}`             : null,
    ].filter(Boolean).join(' &nbsp;|&nbsp; ');
    return `
    <div class="ac-item" onclick="acSeleccionar(${i})">
      <div class="d-flex align-items-start justify-content-between gap-2">
        <div style="min-width:0">
          <div class="ac-name">${c.razon_social}</div>
          ${meta ? `<div class="ac-meta">${meta}</div>` : ''}
        </div>
        <span class="ac-badge ${c.origen==='local'?'ac-local':'ac-erp'}" style="white-space:nowrap;margin-top:2px;flex-shrink:0">
          ${c.origen==='local'?'<i class="bi bi-database me-1"></i>Registrado':'<i class="bi bi-cloud me-1"></i>ERP'}
        </span>
      </div>
    </div>`;
  }).join('');
}

function acSeleccionar(i) {
  const c = acResults[i];
  if(!c) return;
  // RUT en formato crudo (sin puntos, con guion-DV) tal como viene del ERP
  const rutCrudo = (c.rut||'').replace(/\./g,'').toUpperCase();
  document.getElementById('s1_razon').value           = c.razon_social||'';
  document.getElementById('s1_rut').value             = rutCrudo;
  // Email empresa (institucional desde ERP)
  if(c.email) {
    const el = document.getElementById('s1_email_empresa');
    if(el) el.value = c.email;
    const tag = document.getElementById('emailEmp_erp');
    if(tag) tag.classList.add('visible'), tag.textContent = ' ERP';
  }
  document.getElementById('ac_input').value           = `${c.razon_social}${rutCrudo?' ('+rutCrudo+')':''}`;
  if(c.region)    { const el=document.getElementById('s1_ciudad');       if(el) el.value=c.region; }
  if(c.comuna)    { const el=document.getElementById('s1_comuna');       if(el) el.value=c.comuna; }
  if(c.direccion) { const el=document.getElementById('s1_direccion');    if(el) el.value=c.direccion; }
  if(c.telefono)  {
    const el=document.getElementById('s1_tel_empresa');
    if(el) el.value=c.telefono;
    const tag = document.getElementById('telEmp_erp');
    if(tag) tag.classList.add('visible'), tag.textContent = ' ERP';
  }
  if(c.giro)      { const el=document.getElementById('s1_giro');         if(el) el.value=c.giro; }
  document.getElementById('ac_dropdown').style.display = 'none';

  // Badge inicial mientras cargamos el enriquecimiento
  const st = document.getElementById('busq_status');
  st.innerHTML = `<div class="erp-info">
    <span class="spinner-border spinner-border-sm me-2" style="width:.8rem;height:.8rem"></span>
    Consultando ERP para datos completos…</div>`;
  st.style.display='block';

  // Enriquecimiento completo desde /entidades si tenemos RUT
  if(c.rut) {
    _enriquecerDesdeErp(c.rut, c.id);
  } else {
    _mostrarBadgeBasico(c);
  }
  // Actualizar calidad inmediatamente con lo que llegó
  if (window.qfActualizar) window.qfActualizar();
}

async function _enriquecerDesdeErp(rut, localId) {
  try {
    const r = await fetch(`/mantenciones/api/clientes/enriquecer?rut=${encodeURIComponent(rut)}`);
    const d = await r.json();
    const st = document.getElementById('busq_status');

    if(!d.encontrado) {
      st.innerHTML = `<div class="erp-warn"><i class="bi bi-exclamation-triangle me-1"></i>
        RUT no encontrado en el ERP. Completa los datos manualmente.</div>`;
      return;
    }

    // Sobrescribir campos con datos de alta calidad del ERP
    const fill = (id, val) => { const el=document.getElementById(id); if(el && val) el.value=val; };
    const tagErp = (tagId) => {
      const t = document.getElementById(tagId);
      if (t) { t.classList.add('visible'); t.textContent = ' ERP'; }
    };
    fill('s1_razon',         d.razon_social);
    fill('s1_email_empresa', d.email);          if(d.email) tagErp('emailEmp_erp');
    fill('s1_tel_empresa',   d.telefono);       if(d.telefono) tagErp('telEmp_erp');
    fill('s1_direccion',     d.direccion);
    fill('s1_comuna',        d.comuna);
    fill('s1_ciudad',        d.region);  // "Ciudad/Región" = nombre de región
    fill('s1_giro',          d.giro);    // Giro del ERP
    if(d.observaciones) {
      const obs = document.getElementById('s1_notas');
      if(obs && !obs.value.trim()) obs.value = d.observaciones;
    }
    // Sugerir el email del ERP como placeholder en el contacto principal
    // (NO lo escribimos, solo sugerimos — el usuario completa nombre/cargo/teléfono manualmente)
    if (d.email) {
      const ce = document.getElementById('s1_contacto_email');
      if (ce && !ce.value) {
        ce.placeholder = d.email + '  ← sugerido por ERP, completa o reemplaza';
        ce.style.background = '#fef9c3';
        ce.title = 'Email institucional del ERP. Pendiente de confirmar para el contacto principal.';
        ce.addEventListener('focus', function _once() {
          ce.style.background = '';
          ce.removeEventListener('focus', _once);
        });
        // Botón rápido "Usar este" si el usuario quiere copiarlo
        const wrap = ce.parentElement;
        if (wrap && !wrap.querySelector('.erp-email-suggest')) {
          const hint = document.createElement('div');
          hint.className = 'erp-email-suggest form-text';
          hint.style.cssText = 'font-size:.7rem;margin-top:2px;color:#92400e';
          hint.innerHTML = '<i class="bi bi-lightbulb me-1"></i>El ERP sugiere <strong>' + d.email + '</strong>. ' +
            '<a href="#" onclick="event.preventDefault();document.getElementById(\'s1_contacto_email\').value=\'' + d.email + '\';this.parentElement.remove();qfActualizar()" class="text-decoration-none">Usar este email</a>';
          wrap.appendChild(hint);
        }
      }
    }

    const infoPills = [
      d.region   ? `<span class="badge bg-primary bg-opacity-10 text-primary border border-primary border-opacity-25">${d.region}</span>` : '',
      d.comuna   ? `<span class="badge bg-secondary bg-opacity-10 text-secondary border">${d.comuna}</span>` : '',
      d.telefono ? `<span class="badge bg-secondary bg-opacity-10 text-secondary border">${d.telefono}</span>` : '',
      d.email    ? `<span class="badge bg-secondary bg-opacity-10 text-secondary border" style="font-size:.62rem">${d.email}</span>` : '',
      d.giro     ? `<span class="badge bg-light text-muted border">${d.giro}</span>` : '',
    ].filter(Boolean).join(' ');

    if(localId) {
      st.innerHTML = `<div class="erp-info"><i class="bi bi-info-circle me-1"></i>
        Cliente ya registrado en el sistema — <a href="/mantenciones/clientes/${localId}" class="fw-bold">ver ficha</a>.
        Datos ERP precargados: ${infoPills}</div>`;
    } else {
      st.innerHTML = `<div class="erp-ok">
        <div class="d-flex align-items-center gap-2 mb-1">
          <i class="bi bi-check-circle-fill text-success"></i>
          <strong>Datos completos desde ERP</strong>
        </div>
        <div>${infoPills}</div></div>`;
    }
    // Refrescar calidad de ficha con los nuevos datos
    if (window.qfActualizar) window.qfActualizar();
    // Refrescar validación visual del RUT
    const rutInp = document.getElementById('s1_rut');
    if (rutInp && rutInp.value) qfRutValidar(rutInp);
  } catch(err) {
    console.warn('Enriquecimiento ERP falló:', err);
    _mostrarBadgeBasico(null);
  }
}

function _mostrarBadgeBasico(c) {
  const st = document.getElementById('busq_status');
  st.innerHTML = `<div class="erp-ok"><i class="bi bi-check-circle me-1 text-success"></i>
    <strong>Datos cargados</strong> — Completa los campos que falten.</div>`;
}

function acKeydown(e) {
  const drop = document.getElementById('ac_dropdown');
  const items = drop.querySelectorAll('.ac-item');
  if(e.key==='ArrowDown') { acIdx=Math.min(acIdx+1,items.length-1); acHighlight(items); e.preventDefault(); }
  else if(e.key==='ArrowUp') { acIdx=Math.max(acIdx-1,-1); acHighlight(items); e.preventDefault(); }
  else if(e.key==='Enter' && acIdx>=0) { acSeleccionar(acIdx); e.preventDefault(); }
  else if(e.key==='Escape') drop.style.display='none';
}
function acHighlight(items) {
  items.forEach((el,i) => el.style.background = i===acIdx?'#f0f4ff':'');
}
document.addEventListener('click', e => {
  if(!document.getElementById('ac_input').contains(e.target)) {
    document.getElementById('ac_dropdown').style.display='none';
  }
  if(!document.getElementById('s1_direccion').contains(e.target)) {
    document.getElementById('dir_dropdown').style.display='none';
  }
});

// ══ DIRECCIÓN INTELIGENTE (Nominatim OSM — Chile) ════════════
let dirTimer = null;
let dirResults = [];
let dirIdx = -1;

function dirDebounce() {
  clearTimeout(dirTimer);
  const q = document.getElementById('s1_direccion').value.trim();
  if(q.length < 4) { document.getElementById('dir_dropdown').style.display='none'; return; }
  document.getElementById('dir_validating').style.display='';
  document.getElementById('dir_ok').style.display='none';
  dirTimer = setTimeout(dirBuscar, 500);
}

async function dirBuscar() {
  const q = document.getElementById('s1_direccion').value.trim();
  const drop = document.getElementById('dir_dropdown');
  if(q.length < 4) return;
  drop.innerHTML = '<div class="ac-loading"><span class="spinner-border spinner-border-sm me-1"></span>Buscando dirección…</div>';
  drop.style.display = 'block';

  // Estrategia multi-fuente para mejor cobertura en Chile:
  //   1) Nominatim con la query original
  //   2) Nominatim sin "Av/Avda/Avenida" (abreviaciones rompen el match)
  //   3) Photon (OSM con tolerancia a typos y abreviaturas)
  //   4) Nominatim con solo la calle (sin número) como último recurso
  const variantes = [
    q,
    q.replace(/^(av\.?|avda\.?|avenida)\s+/i, ''),    // "Av San Jose..." → "San Jose..."
    q.replace(/\s+\d+[A-Za-z]?\s*[,;]?\s*/g, ' ').trim()  // sin número
  ].filter((v, i, a) => v && a.indexOf(v) === i);

  async function nominatim(query) {
    const url = `https://nominatim.openstreetmap.org/search?format=json&q=${encodeURIComponent(query+', Chile')}&countrycodes=cl&addressdetails=1&limit=8&accept-language=es&dedupe=1`;
    try {
      const r = await fetch(url, { headers: { 'Accept-Language': 'es' } });
      if (!r.ok) return [];
      const j = await r.json();
      return (j||[]).map(d => {
        const addr = d.address || {};
        return {
          calle:  [addr.road, addr.house_number].filter(Boolean).join(' ') || d.display_name.split(',')[0],
          comuna: addr.suburb || addr.city_district || addr.municipality || addr.county || addr.town || '',
          ciudad: addr.city || addr.town || addr.state || '',
          display: d.display_name,
          fuente: 'OSM',
        };
      });
    } catch { return []; }
  }

  async function photon(query) {
    // Photon: motor OSM con tolerancia a typos/abreviaturas. Bbox limitado a Chile.
    // (Photon NO soporta lang=es; usa default que ya devuelve nombres locales)
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
        const calle = [p.name, p.housenumber].filter(Boolean).join(' ')
                   || [p.street, p.housenumber].filter(Boolean).join(' ')
                   || p.name || '';
        return {
          calle:  calle,
          comuna: p.district || p.locality || p.suburb || '',
          ciudad: p.city || p.county || p.state || '',
          display: [calle, p.locality || p.district, p.city, p.country].filter(Boolean).join(', '),
          fuente: 'Photon',
        };
      });
    } catch { return []; }
  }

  // Ejecutar en cascada: si la primera variante no devuelve, probar la siguiente.
  let resultados = [];
  for (const v of variantes) {
    resultados = await nominatim(v);
    if (resultados.length) break;
  }
  // Fallback Photon
  if (!resultados.length) {
    for (const v of variantes) {
      resultados = await photon(v);
      if (resultados.length) break;
    }
  }

  dirResults = resultados.filter(r => r.calle);
  dirIdx = -1;
  document.getElementById('dir_validating').style.display='none';

  if(!dirResults.length) {
    drop.innerHTML = `<div class="ac-loading text-muted">
      <div><i class="bi bi-geo me-1"></i>Sin resultados exactos para esta dirección.</div>
      <div class="small mt-1">Puedes ingresarla manualmente — completa también comuna y ciudad/región.</div>
    </div>`;
    return;
  }

  drop.innerHTML = dirResults.map((d,i) => {
    const fuenteBadge = d.fuente === 'Photon'
      ? '<span class="badge bg-info-subtle text-info border ms-1" style="font-size:.55rem;font-weight:600">Photon</span>'
      : '';
    return `<div class="ac-item" onclick="dirSeleccionar(${i})">
      <div class="d-flex align-items-start gap-2">
        <i class="bi bi-geo-alt-fill text-danger mt-1" style="font-size:.8rem;flex-shrink:0"></i>
        <div style="flex:1;min-width:0">
          <div class="ac-name" style="font-size:.83rem">${d.calle}${fuenteBadge}</div>
          <div class="ac-rut">${[d.comuna, d.ciudad].filter(Boolean).join(', ') || d.display.slice(0,80)}</div>
        </div>
      </div>
    </div>`;
  }).join('');
}

function dirSeleccionar(i) {
  const d = dirResults[i];
  if(!d) return;
  // El formato ya está normalizado por nominatim()/photon() — todos tienen calle/comuna/ciudad
  document.getElementById('s1_direccion').value = d.calle || (d.display || '').split(',').slice(0,2).join(',').trim();
  if(d.comuna) document.getElementById('s1_comuna').value = d.comuna;
  if(d.ciudad) document.getElementById('s1_ciudad').value = d.ciudad;
  document.getElementById('dir_dropdown').style.display='none';
  document.getElementById('dir_ok').style.display='';
  document.getElementById('dir_validating').style.display='none';

  // Feedback visual en campos autollenados
  ['s1_comuna','s1_ciudad'].forEach(id => {
    const el = document.getElementById(id);
    if(el.value) { el.style.borderColor='#1a7a1a'; el.style.background='#f1f8e9'; }
  });
  if (window.qfActualizar) window.qfActualizar();
}

function dirKeydown(e) {
  const drop = document.getElementById('dir_dropdown');
  const items = drop.querySelectorAll('.ac-item');
  if(e.key==='ArrowDown') { dirIdx=Math.min(dirIdx+1,items.length-1); items.forEach((el,i)=>el.style.background=i===dirIdx?'#f0f4ff':''); e.preventDefault(); }
  else if(e.key==='ArrowUp') { dirIdx=Math.max(dirIdx-1,-1); items.forEach((el,i)=>el.style.background=i===dirIdx?'#f0f4ff':''); e.preventDefault(); }
  else if(e.key==='Enter' && dirIdx>=0) { dirSeleccionar(dirIdx); e.preventDefault(); }
  else if(e.key==='Escape') drop.style.display='none';
}

// ══ AGENTE ILUS — cargar desde contrato (determinista, sin IA) ════
function onDropIA(e) {
  e.preventDefault();
  document.getElementById('dropZoneIA').classList.remove('drag-over');
  const f = e.dataTransfer.files[0];
  if(f) disparaAgente(f);
}
function lanzarAgente(fuente) {
  const inputId = fuente === 'camara' ? 'ia_camara' : 'ia_archivo';
  const f = document.getElementById(inputId).files[0];
  if(f) disparaAgente(f);
}

const _IMG_EXTS = ['jpg','jpeg','png','webp'];
function _esImagen(archivo) {
  const ext = archivo.name.split('.').pop().toLowerCase();
  return _IMG_EXTS.includes(ext) || archivo.type.startsWith('image/');
}

async function disparaAgente(archivo) {
  const esImg = _esImagen(archivo);
  const zone = document.getElementById('dropZoneIA');
  zone.classList.add('has-file');

  // Preview de imagen si es foto
  if(esImg) {
    const reader = new FileReader();
    reader.onload = ev => {
      document.getElementById('dropIAContent').innerHTML = `
        <img src="${ev.target.result}" style="max-height:160px;max-width:100%;border-radius:8px;object-fit:contain" alt="Contrato">
        <div class="fw-bold mt-2" style="color:#1a7a1a;font-size:.82rem"><i class="bi bi-camera-fill me-1"></i>${archivo.name}</div>
        <div class="small text-muted">${(archivo.size/1024/1024).toFixed(2)} MB — foto del contrato</div>`;
    };
    reader.readAsDataURL(archivo);
  } else {
    document.getElementById('dropIAContent').innerHTML = `
      <i class="bi bi-file-earmark-check" style="font-size:2rem;color:#1a7a1a;display:block;margin-bottom:8px"></i>
      <div class="fw-bold" style="color:#1a7a1a">${archivo.name}</div>
      <div class="small text-muted">${(archivo.size/1024/1024).toFixed(2)} MB</div>`;
  }

  const status = document.getElementById('ia_status');
  status.style.display='';
  status.innerHTML = `<div class="erp-info">
    <span class="spinner-border spinner-border-sm me-2"></span>
    <strong>Agente ILUS trabajando…</strong>
    ${esImg
      ? ' Es una foto: leeré lo que pueda y te pediré el RUT para traer todo del ERP.'
      : ' Leyendo el documento con código y extrayendo datos del cliente, cláusulas, costos y equipos.'
    }
  </div>`;

  const fd = new FormData();
  fd.append('archivo', archivo);
  try {
    const r = await fetch('/mantenciones/api/agente-contrato', { method:'POST', body:fd });
    const data = await r.json();

    // Error explícito del endpoint (status != 200): modal ILUS, sin alert nativo
    if(!r.ok || data.error) {
      status.innerHTML = `<div class="erp-warn"><i class="bi bi-exclamation-triangle me-1"></i>${data.error || 'No se pudo leer el contrato.'}</div>`;
      await ilusAlert({ title:'No se pudo leer', message: data.error || 'No se pudo leer el contrato.', type:'error' });
      return;
    }

    // Foto / PDF escaneado sin texto: el Agente pide el RUT y trae todo del ERP
    if(data.requiere_rut) {
      await _agenteContratoPorRut(archivo, data.mensaje, status);
      return;
    }

    if(!data.ok) {
      status.innerHTML = `<div class="erp-warn"><i class="bi bi-exclamation-triangle me-1"></i>${data.error || 'No se pudo leer el contrato.'}</div>`;
      await ilusAlert({ title:'No se pudo leer', message: data.error || 'No se pudo leer el contrato.', type:'error' });
      return;
    }

    const res = data.resultado || {};
    WZ.aiData = res.contrato;

    // Llenar datos del cliente
    const cl = res.cliente||{};
    if(cl.razon_social) document.getElementById('s1_razon').value         = cl.razon_social;
    if(cl.rut)          document.getElementById('s1_rut').value           = cl.rut;
    if(cl.direccion)    document.getElementById('s1_direccion').value     = cl.direccion;
    if(cl.comuna)       document.getElementById('s1_comuna').value        = cl.comuna;
    if(cl.ciudad || cl.region) document.getElementById('s1_ciudad').value = cl.ciudad || cl.region;
    if(cl.contacto_nombre) document.getElementById('s1_contacto_nombre').value = cl.contacto_nombre;
    if(cl.contacto_cargo)  document.getElementById('s1_contacto_cargo').value  = cl.contacto_cargo;
    if(cl.contacto_email)  document.getElementById('s1_contacto_email').value  = cl.contacto_email;
    if(cl.contacto_tel)    document.getElementById('s1_contacto_tel').value    = cl.contacto_tel;

    // Llenar contrato
    const ct = res.contrato||{};
    document.getElementById('ct_nombre').value        = ct.nombre||archivo.name.replace(/\.[^.]+$/,'');
    document.getElementById('ct_inicio').value        = ct.vigencia_inicio||'';
    document.getElementById('ct_vencimiento').value   = ct.vigencia_fin||'';
    document.getElementById('ct_monto_mensual').value = ct.monto_mensual||'';
    document.getElementById('ct_frecuencia').value    = ct.frecuencia_meses||'';
    document.getElementById('ct_indefinido').checked  = !!ct.es_indefinido;

    // Llenar panel de detalle del contrato
    poblarPanelIA(ct);

    // Guardar archivo para paso 3 (WZ.archivoContrato es más fiable que DataTransfer en móvil)
    WZ.archivoContrato = archivo;
    try {
      const dt = new DataTransfer();
      dt.items.add(archivo);
      document.getElementById('ct_archivo').files = dt.files;
    } catch(e) { /* Safari/iOS no soporta DataTransfer constructor, usar WZ.archivoContrato */ }
    mostrarArchivoCargado(archivo);

    // Equipos detectados
    if(res.equipos && res.equipos.length) {
      res.equipos.forEach(eq => agregarFilaEquipo(eq));
    }

    // Match ERP
    const match = res._erp_match||{};
    const matchDiv = document.getElementById('ia_match');
    matchDiv.style.display='';
    if(match.id) {
      matchDiv.innerHTML = `<div class="erp-info">
        <i class="bi bi-database me-1"></i>
        <strong>${match.razon_social}</strong> ya está registrado como cliente de mantención.
        <a href="/mantenciones/clientes/${match.id}" class="ms-2">Ver ficha →</a>
      </div>`;
    } else if(match.razon_social) {
      matchDiv.innerHTML = `<div class="erp-ok">
        <i class="bi bi-check-circle me-1 text-success"></i>
        <strong>${match.razon_social}</strong> encontrado en el ERP. Datos del cliente verificados.
      </div>`;
    }

    status.innerHTML = `<div class="erp-ok">
      <i class="bi bi-check-circle-fill text-success me-1"></i>
      <strong>Contrato leído.</strong> Se extrajeron datos del cliente, ${(ct.clausulas_criticas||[]).length} cláusulas críticas y ${(res.equipos||[]).length} equipos. Revisa los datos y continúa.
    </div>`;
    if (window.qfActualizar) window.qfActualizar();
    ilusToast('✓ Contrato leído por el Agente ILUS', { type:'success' });
  } catch(e) {
    status.innerHTML = `<div class="erp-warn"><i class="bi bi-x-circle me-1"></i>Error: ${e.message}</div>`;
    await ilusAlert({ title:'No se pudo leer', message: e.message, type:'error' });
  }
}

// Foto / PDF escaneado: pido el RUT y traigo todo del ERP reusando la lógica del autocomplete.
async function _agenteContratoPorRut(archivo, mensaje, status) {
  ilusToast(mensaje || 'Necesito el RUT para traer los datos del ERP', { type:'info' });

  const rut = await ilusPrompt({
    title: 'RUT del cliente',
    message: 'Escribe el RUT y traigo todo del ERP',
    placeholder: '76.123.456-7',
  });
  if(!rut || !rut.trim()) {
    status.innerHTML = `<div class="erp-warn"><i class="bi bi-info-circle me-1"></i>Carga cancelada. Puedes ingresar los datos manualmente.</div>`;
    return;
  }

  status.innerHTML = `<div class="erp-info">
    <span class="spinner-border spinner-border-sm me-2"></span>
    <strong>Agente ILUS</strong> · Buscando <strong>${rut.trim()}</strong> en el ERP…
  </div>`;

  let resultados = [];
  try {
    const r = await fetch(`/mantenciones/api/clientes/autocomplete?q=${encodeURIComponent(rut.trim())}`);
    if(!r.ok) throw new Error(`HTTP ${r.status}`);
    resultados = await r.json();
  } catch(err) {
    status.innerHTML = `<div class="erp-warn"><i class="bi bi-x-circle me-1"></i>Error consultando el ERP: ${err.message}</div>`;
    ilusToast('No pude consultar el ERP. Complétalo manualmente.', { type:'warning' });
    return;
  }

  if(!resultados.length) {
    status.innerHTML = `<div class="erp-warn"><i class="bi bi-exclamation-triangle me-1"></i>No encontré ese RUT en el ERP. Completa los datos manualmente.</div>`;
    ilusToast('No encontré ese RUT en el ERP. Complétalo manualmente.', { type:'warning' });
    return;
  }

  // Reusar EXACTAMENTE la lógica del autocomplete (rellena campos + enriquece desde ERP)
  acResults = resultados;
  acSeleccionar(0);

  // Conservar el archivo (foto) como adjunto del contrato para el paso 3
  WZ.archivoContrato = archivo;
  try {
    const dt = new DataTransfer();
    dt.items.add(archivo);
    document.getElementById('ct_archivo').files = dt.files;
  } catch(e) { /* Safari/iOS no soporta DataTransfer constructor */ }
  if(typeof mostrarArchivoCargado === 'function') mostrarArchivoCargado(archivo);

  status.innerHTML = `<div class="erp-ok">
    <i class="bi bi-check-circle-fill text-success me-1"></i>
    <strong>Datos traídos del ERP.</strong> Revisa el cliente y completa el contrato manualmente.
  </div>`;
  if (window.qfActualizar) window.qfActualizar();
  ilusToast('✓ Datos traídos del ERP', { type:'success' });
}

// ══ PASO 2 — EQUIPOS ═════════════════════════════════════════
function agregarFilaEquipo(datos={}) {
  const tbody = document.getElementById('equiposTbody');
  const ph = document.getElementById('eq-placeholder');
  if(ph) ph.remove();
  eqCounter++;
  const id = `eq-${eqCounter}`;
  const tr = document.createElement('tr');
  tr.id=id;
  const docOrigen = datos.doc_origen || datos.doc_origen || '';
  const docBadge = docOrigen
    ? `<span style="font-size:.62rem;color:#0066cc;font-family:monospace;white-space:nowrap">${docOrigen}</span>`
    : `<span style="font-size:.62rem;color:#888;font-style:italic">Manual</span>`;
  // Default fecha de instalación = fecha del documento (cubre garantía 6 meses)
  const fechaInst = datos.fecha_instalacion || datos.doc_fecha || datos.fecha || '';
  // N° Serie: si llega "(auto)" o vacío, generamos uno visible en cliente
  // Formato: {RUT}-{últimos4SKU}-{n} (ej: 65206047-0905-1)
  let serieIni = (datos.serie || '').trim();
  if (!serieIni || serieIni.startsWith('(auto')) {
    serieIni = _serieTmpAuto(datos.sku || '');
  }
  // Detectar si es serie autogenerada (rut-sku4-n) vs serie real del fabricante
  const rutWiz = _rutDelWizard();
  const serieEsTemp = serieIni.startsWith(rutWiz + '-') ||
                      serieIni.startsWith('ILUS-NEW-') || serieIni.startsWith('ILUS-TMP-');
  tr.innerHTML=`
    <td><input class="eq-input" data-field="nombre" value="${esc(datos.nombre||datos.producto||'')}" placeholder="Nombre equipo"></td>
    <td><input class="eq-input font-monospace" data-field="sku" value="${esc(datos.sku||'')}" title="SKU del modelo (compartido entre unidades iguales)"></td>
    <td><input class="eq-input font-monospace" data-field="serie" value="${esc(serieIni)}"
              style="${serieEsTemp ? 'background:#fef9c3;border-color:#fde68a' : ''}"
              title="N° serie único de este equipo (autogenerado). Si la representación trae uno real, escríbelo aquí. Sirve para etiqueta física, garantía y trazabilidad."
              onchange="if(this.value && !this.value.startsWith('ILUS-NEW-') && !this.value.startsWith('ILUS-TMP-')){this.style.background='';this.style.borderColor=''}else{this.style.background='#fef9c3';this.style.borderColor='#fde68a'}"></td>
    <td><input class="eq-input" data-field="tag_1" value="${esc(datos.tag_1||'')}"
              placeholder="—" maxlength="120"
              title="Etiqueta libre: usa para clasificar (ej: 'sala cardio', 'comprado 2024', 'garantía extendida')"></td>
    <td><input class="eq-input" data-field="tag_2" value="${esc(datos.tag_2||'')}"
              placeholder="—" maxlength="120"
              title="Segunda etiqueta libre"></td>
    <td><input class="eq-input" data-field="ubicacion" value="${esc(datos.ubicacion||'')}"></td>
    <td><select class="eq-input" data-field="estado_op">
      <option value="operativo" ${(datos.estado_op||'operativo')==='operativo'?'selected':''}>Operativo</option>
      <option value="critico"   ${datos.estado_op==='critico'?'selected':''}>Crítico</option>
      <option value="en_mantencion" ${datos.estado_op==='en_mantencion'?'selected':''}>En mantención</option>
    </select></td>
    <td><input type="date" class="eq-input" data-field="fecha_instalacion" value="${esc(fechaInst)}"
              data-original="${esc(fechaInst)}"
              onchange="eqValidarFechaInst(this)"
              title="Default: fecha del documento (cubre garantía). Si la cambias, se pedirá justificación."></td>
    <td><input type="number" class="eq-input text-center" data-field="cantidad" value="${datos.cantidad||1}" min="1" style="width:44px"></td>
    <td>
      <input type="hidden" data-field="doc_origen" value="${esc(docOrigen)}">
      <input type="hidden" data-field="doc_fecha"  value="${esc(datos.doc_fecha||datos.fecha||'')}">
      <input type="hidden" data-field="cantidad_original" value="${parseInt(datos.cantidad_original||0)||0}">
      <input type="hidden" data-field="split_to_rows" value="${datos._split_to_rows ? '1':'0'}">
      <input type="hidden" data-field="justif_doc_mismatch" value="${esc(datos.justif_doc_mismatch||'')}">
      <input type="hidden" data-field="justif_fecha_inst"   value="">
      ${docBadge}
    </td>
    <td><button class="btn btn-xs btn-outline-danger" onclick="this.closest('tr').remove()" title="Quitar"><i class="bi bi-trash"></i></button></td>`;
  tbody.appendChild(tr);
}

async function eqValidarFechaInst(inp) {
  const orig = inp.dataset.original || '';
  const nueva = inp.value || '';
  if (!orig || nueva === orig) return;
  const motivo = await ilusPrompt({
    title: 'Cambio de fecha de instalación',
    message: `Default (fecha documento): ${orig}\nNueva: ${nueva}`,
    sub: 'Esta fecha cubre garantía. Justifica por qué la cambias (mín. 8 caracteres).',
    placeholder: 'Motivo del cambio…',
    multiline: true,
    type: 'warning',
    okLabel: 'Guardar justificación',
  });
  if (!motivo || motivo.trim().length < 8) {
    inp.value = orig;
    ilusToast('Cambio cancelado: justificación requerida (mín. 8 caracteres)', { type:'warning' });
    return;
  }
  const tr = inp.closest('tr');
  const j = tr.querySelector('[data-field="justif_fecha_inst"]');
  if (j) j.value = motivo.trim();
}
function esc(s) { return String(s).replace(/"/g,'&quot;').replace(/</g,'&lt;'); }
function leerEquipos() {
  return Array.from(document.querySelectorAll('#equiposTbody tr:not(#eq-placeholder)')).map(tr => {
    const o={};
    tr.querySelectorAll('[data-field]').forEach(el=>o[el.dataset.field]=el.value);
    return o;
  }).filter(o=>o.nombre&&o.nombre.trim());
}

// ══ ERP modal equipos ════════════════════════════════════════

// ── Tab switcher (3 tabs) ─────────────────────────────────────
function erpEqSetTab(tab) {
  ['doc','client','prod'].forEach(t => {
    const btn = document.getElementById('tab'+t.charAt(0).toUpperCase()+t.slice(1)+'Btn');
    const panel = document.getElementById('panel'+t.charAt(0).toUpperCase()+t.slice(1));
    if(btn)   btn.classList.toggle('active', t===tab);
    if(panel) panel.style.display = t===tab ? '' : 'none';
  });
}

// ── Helper escape HTML ────────────────────────────────────────
function _escHtml(s){ return String(s||'').replace(/[&<>"']/g, c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); }

// ════════════════════════════════════════════════════════════════════
// WIZARD ERP — Búsqueda por RUT/nombre/número via SQL Server (Random)
// ════════════════════════════════════════════════════════════════════
let _wzDocActual = null;     // doc seleccionado para abrir modal productos
let _wzProductos = [];       // productos del doc abierto
let _wzSeleccion = new Set();

// Tab "Por cliente (RUT/nombre)" — usa el endpoint SQL nuevo
async function buscarErpEq() {
  const q = (document.getElementById('erpEqQ').value || '').trim();
  if (q.length < 3) {
    document.getElementById('erpEqRes').innerHTML =
      '<div class="alert alert-warning small mb-0">Mínimo 3 caracteres</div>';
    return;
  }
  const cont = document.getElementById('erpEqRes');
  cont.innerHTML = '<div class="text-center py-4"><span class="spinner-border spinner-border-sm me-2"></span>Buscando en Random ERP…</div>';

  let data;
  try {
    const r = await fetch('/mantenciones/api/buscar-erp-sql', {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({q})
    });
    data = await r.json();
  } catch(e) {
    cont.innerHTML = `<div class="alert alert-danger small">Error de red: ${_escHtml(e.message)}</div>`;
    return;
  }

  if (data.sin_conexion) {
    cont.innerHTML = `<div class="alert alert-warning">
      <i class="bi bi-plug me-1"></i><strong>ERP no conectado.</strong> ${_escHtml(data.error||'')}
      <br><small class="text-muted">Pídele al admin que setee RANDOM_SQL_* en Google Cloud.</small>
    </div>`;
    return;
  }
  if (data.error) { cont.innerHTML = `<div class="alert alert-warning small">${_escHtml(data.error)}</div>`; return; }
  if (!data.documentos?.length) {
    cont.innerHTML = `<div class="text-center text-muted py-4" style="font-size:.85rem">
      <i class="bi bi-search" style="font-size:1.6rem;opacity:.3;display:block;margin-bottom:8px"></i>
      Sin resultados para "${_escHtml(q)}"</div>`;
    return;
  }

  const rutWizard = (document.getElementById('s1_rut')?.value || '').replace(/[.\-\s]/g,'');
  const modoLbl = {rut:'RUT', numero:'Nº doc', nombre:'Nombre'}[data.modo] || '';
  let html = `<div class="d-flex justify-content-between align-items-center mb-2 small text-muted">
    <span><strong>${data.documentos.length}</strong> documento(s) por <strong>${modoLbl}</strong></span>
  </div>
  <div class="table-responsive" style="max-height:340px;overflow-y:auto;border:1px solid #e5e7eb;border-radius:8px">
    <table class="table table-sm table-hover mb-0" style="font-size:.82rem">
      <thead class="sticky-top" style="background:#f9fafb;top:0">
        <tr><th style="width:60px">Tipo</th><th style="width:120px">Número</th>
            <th>Cliente</th><th style="width:100px">Fecha</th>
            <th class="text-end" style="width:120px">Total</th>
            <th style="width:130px"></th></tr>
      </thead><tbody>`;
  data.documentos.forEach(d => {
    // FIX 2026-05-19: usar ilusRutsMatch para tolerar DV presente/ausente
    // (ej: "78.129.118-8" vs "78129118" deben coincidir).
    const rutMatch = ilusRutsMatch(d.rut, rutWizard);
    const total = d.valor_total ? '$' + Math.round(d.valor_total).toLocaleString('es-CL') : '—';
    html += `<tr>
      <td><span class="badge bg-secondary" style="font-size:.62rem">${_escHtml(d.tido_display)}</span></td>
      <td class="font-monospace">${_escHtml(d.nudo_display)}</td>
      <td class="text-truncate" style="max-width:220px" title="${_escHtml(d.razon_social)} (${_escHtml(d.rut)})">
        ${_escHtml(d.razon_social || '—')}
        ${rutWizard && !rutMatch ? '<i class="bi bi-exclamation-triangle text-warning ms-1" title="RUT distinto"></i>' : ''}
      </td>
      <td class="small text-muted">${_escHtml(d.fecha)}</td>
      <td class="text-end small">${total}</td>
      <td><button class="btn btn-sm btn-ilus w-100" onclick='wzAbrirModalProds(${JSON.stringify(d).replace(/'/g,"&#39;")})'>
        <i class="bi bi-eye me-1"></i>Ver productos
      </button></td>
    </tr>`;
  });
  html += '</tbody></table></div>';
  cont.innerHTML = html;
}

// ── Modal de productos del documento (en el wizard) ─────────────
let _wzModalProds = null;

async function wzAbrirModalProds(doc) {
  _wzDocActual = doc;
  _wzProductos = [];
  _wzSeleccion = new Set();

  // Crear modal dinámicamente si no existe (z-index alto para apilarse sobre modal padre)
  if (!document.getElementById('wzModalProds')) {
    const modalHtml = `<div class="modal fade" id="wzModalProds" tabindex="-1" style="z-index:1080" data-bs-backdrop="static">
      <div class="modal-dialog modal-xl modal-dialog-scrollable">
        <div class="modal-content" style="border:3px solid var(--ilus-red);box-shadow:0 0 60px rgba(0,0,0,.5)">
          <div class="modal-header" style="background:linear-gradient(135deg,var(--ilus-red) 0%,#990000 100%);color:#fff">
            <h5 class="modal-title fw-bold">
              <i class="bi bi-box-seam me-2"></i>Productos del documento
              <span id="wzpDocTit" class="ms-2 fw-normal" style="font-size:.85rem;opacity:.95;background:rgba(0,0,0,.25);padding:3px 10px;border-radius:50px"></span>
            </h5>
            <button type="button" class="btn-close btn-close-white" data-bs-dismiss="modal"></button>
          </div>
          <div class="modal-body p-0">
            <div id="wzpHeader" style="padding:14px 20px;background:#f9fafb;border-bottom:1px solid #e5e7eb;font-size:.84rem"></div>
            <div id="wzpMismatch" style="padding:0 20px;display:none"></div>
            <div style="padding:12px 20px;border-bottom:1px solid #e5e7eb">
              <div class="d-flex flex-wrap gap-3 align-items-center">
                <div class="form-check m-0">
                  <input class="form-check-input" type="checkbox" id="wzpChkAll" onchange="wzpToggleAll(this.checked)">
                  <label class="form-check-label fw-bold" for="wzpChkAll">Seleccionar todos</label>
                </div>
                <div class="form-check m-0" style="background:#fff7ed;padding:6px 12px;border-radius:6px;border:1px solid #fdba74">
                  <input class="form-check-input" type="checkbox" id="wzpChkExpandir" checked>
                  <label class="form-check-label" for="wzpChkExpandir">
                    <i class="bi bi-arrow-down-up me-1 text-warning"></i>
                    <strong>Crear 1 ficha individual por unidad</strong>
                    <span class="text-muted small ms-1">(recomendado: cada equipo con su propio código ILUS para trazabilidad)</span>
                  </label>
                </div>
                <div class="ms-auto small text-muted">
                  <span id="wzpCount">0</span> de <span id="wzpTotal">0</span> seleccionado(s)
                </div>
              </div>
            </div>
            <div id="wzpTabla">
              <div class="text-center text-muted py-5"><span class="spinner-border spinner-border-sm me-2"></span>Cargando…</div>
            </div>
          </div>
          <div class="modal-footer">
            <small class="me-auto text-muted">
              <i class="bi bi-info-circle me-1"></i>
              Fecha de instalación = fecha del documento. Código ILUS auto, editable después.
            </small>
            <button type="button" class="btn btn-outline-secondary" data-bs-dismiss="modal">Cancelar</button>
            <button type="button" class="btn btn-ilus fw-bold px-4" id="wzpBtnImp" onclick="wzpImportar()" disabled>
              <i class="bi bi-box-arrow-in-down me-1"></i>
              Agregar a equipos (<span id="wzpBtnCount">0</span>)
            </button>
          </div>
        </div>
      </div>
    </div>`;
    document.body.insertAdjacentHTML('beforeend', modalHtml);
    // Hook para subir z-index del backdrop cuando este modal se apila sobre otro
    document.getElementById('wzModalProds').addEventListener('shown.bs.modal', function() {
      const backdrops = document.querySelectorAll('.modal-backdrop');
      if (backdrops.length > 1) {
        backdrops[backdrops.length - 1].style.zIndex = '1075';
        backdrops[backdrops.length - 1].style.opacity = '0.7';   // más oscuro que el default
      }
    });
  }
  if (!_wzModalProds) _wzModalProds = new bootstrap.Modal(document.getElementById('wzModalProds'));

  // Header
  document.getElementById('wzpDocTit').textContent = `${doc.tido_display} ${doc.nudo_display}`;
  document.getElementById('wzpHeader').innerHTML = `
    <div class="d-flex flex-wrap gap-3">
      <div><span class="text-muted">Cliente:</span> <strong>${_escHtml(doc.razon_social||'—')}</strong></div>
      <div><span class="text-muted">RUT:</span> <span class="font-monospace">${_escHtml(doc.rut||'—')}</span></div>
      <div><span class="text-muted">Fecha:</span> <strong>${_escHtml(doc.fecha)}</strong></div>
    </div>`;
  document.getElementById('wzpChkAll').checked = false;
  document.getElementById('wzpCount').textContent = '0';
  document.getElementById('wzpTotal').textContent = '…';
  document.getElementById('wzpBtnImp').disabled = true;
  document.getElementById('wzpBtnCount').textContent = '0';

  // Validar mismatch RUT — tolerante a DV presente/ausente.
  // FIX 2026-05-19: ilusRutsMatch compara cuerpos correctamente
  // aunque uno traiga DV y el otro no.
  const rutWiz = (document.getElementById('s1_rut')?.value || '');
  const noCoincide = !!rutWiz && !!doc.rut && !ilusRutsMatch(doc.rut, rutWiz);
  const mEl = document.getElementById('wzpMismatch');
  if (noCoincide) {
    window._wzMismatch = { rutDoc:doc.rut, tido:doc.tido, nudo:doc.nudo, confirmado:false, motivo:'' };
    mEl.style.display = '';
    mEl.innerHTML = `<div class="alert alert-danger small mt-2 mb-2" style="border-left:4px solid #dc2626">
      <strong><i class="bi bi-exclamation-octagon-fill me-1"></i>RUT distinto al del cliente que estás creando</strong><br>
      <textarea id="wzpMotivo" class="form-control form-control-sm mt-2" rows="2"
                placeholder="¿Por qué estás importando equipos de otro RUT? (mínimo 8 caracteres)"></textarea>
      <button class="btn btn-sm btn-warning mt-2" onclick="wzpConfirmarMismatch()">
        <i class="bi bi-check-lg me-1"></i>Confirmar motivo
      </button>
    </div>`;
  } else {
    mEl.style.display = 'none';
    window._wzMismatch = null;
  }

  document.getElementById('wzpTabla').innerHTML =
    '<div class="text-center text-muted py-5"><span class="spinner-border spinner-border-sm me-2"></span>Cargando productos del ERP…</div>';
  _wzModalProds.show();

  // Fetch productos del documento
  try {
    const r = await fetch(`/mantenciones/api/documento?tido=${encodeURIComponent(doc.tido)}&nudo=${encodeURIComponent(doc.nudo)}`);
    const data = await r.json();
    if (!data.ok || !data.items?.length) {
      document.getElementById('wzpTabla').innerHTML = '<div class="text-center text-muted py-4">Este documento no tiene productos importables.</div>';
      return;
    }
    _wzProductos = data.items;
    document.getElementById('wzpTotal').textContent = _wzProductos.length;
    wzpRender();
  } catch(e) {
    document.getElementById('wzpTabla').innerHTML = `<div class="alert alert-danger m-3 small">Error: ${_escHtml(e.message)}</div>`;
  }
}

function wzpRender() {
  let html = `<table class="table table-hover mb-0" style="font-size:.85rem">
    <thead style="background:#f9fafb">
      <tr><th style="width:50px"></th><th>Producto</th><th style="width:160px">SKU del modelo</th>
          <th style="width:90px" class="text-center">Cantidad</th></tr>
    </thead><tbody>`;
  _wzProductos.forEach((p,i) => {
    const checked = _wzSeleccion.has(i) ? 'checked' : '';
    const qty = parseInt(p.cantidad) || 1;
    const expandHint = qty > 1 ? `<br><span class="text-warning small"><i class="bi bi-arrow-down-up me-1"></i>creará ${qty} fichas individuales</span>` : '';
    html += `<tr>
      <td><input type="checkbox" class="form-check-input wzp-chk" ${checked} onchange="wzpToggleItem(${i}, this.checked)"></td>
      <td>${_escHtml(p.nombre)}${expandHint}</td>
      <td class="font-monospace small text-muted">${_escHtml(p.sku||'—')}</td>
      <td class="text-center"><strong>${qty}</strong></td>
    </tr>`;
  });
  html += '</tbody></table>';
  document.getElementById('wzpTabla').innerHTML = html;
}

function wzpToggleItem(idx, checked) {
  if (checked) _wzSeleccion.add(idx); else _wzSeleccion.delete(idx);
  wzpUpdateCount();
}
function wzpToggleAll(checked) {
  _wzSeleccion.clear();
  if (checked) _wzProductos.forEach((_,i) => _wzSeleccion.add(i));
  document.querySelectorAll('.wzp-chk').forEach(c => c.checked = checked);
  wzpUpdateCount();
}
function wzpUpdateCount() {
  const n = _wzSeleccion.size;
  document.getElementById('wzpCount').textContent = n;
  document.getElementById('wzpBtnCount').textContent = n;
  document.getElementById('wzpBtnImp').disabled = n === 0;
}

function wzpConfirmarMismatch() {
  const motivo = (document.getElementById('wzpMotivo')?.value || '').trim();
  if (motivo.length < 8) { alert('Mínimo 8 caracteres en el motivo'); return; }
  if (!window._wzMismatch) return;
  window._wzMismatch.confirmado = true;
  window._wzMismatch.motivo = motivo;
  document.getElementById('wzpMismatch').innerHTML =
    `<div class="alert alert-success small mt-2 mb-2"><i class="bi bi-check-circle-fill me-1"></i>Motivo registrado. Puedes importar.</div>`;
}

function wzpImportar() {
  if (_wzSeleccion.size === 0) return;
  if (window._wzMismatch && !window._wzMismatch.confirmado) {
    if (typeof ilusAlert === 'function') {
      ilusAlert({
        title: '⚠ RUT distinto sin confirmar',
        message: 'El documento tiene un RUT diferente al del cliente que estás creando.',
        sub: 'Antes de importar, registra el motivo en el campo de justificación arriba.',
        type: 'warning',
        okLabel: 'Entendido',
      });
    } else {
      alert('Confirma primero el motivo del RUT distinto.');
    }
    return;
  }
  const expandir = document.getElementById('wzpChkExpandir').checked;
  const fecha_doc = _wzDocActual.fecha_iso || '';
  const doc_origen = `${_wzDocActual.tido_display} ${_wzDocActual.nudo_display}`;
  const justifMismatch = window._wzMismatch?.motivo || '';
  let creados = 0;

  for (const idx of _wzSeleccion) {
    const p = _wzProductos[idx];
    const qty = parseInt(p.cantidad) || 1;
    const nombre = p.nombre || p.sku || '';
    const esBulk = /disco|plate|pesa|mancuerna|kettleb|barra|kg\b|pares\b/i.test(nombre);
    const expandirEsto = expandir && qty > 1 && !esBulk;
    const filas = expandirEsto ? qty : 1;
    const cantidadCadaUna = expandirEsto ? 1 : qty;

    for (let n = 1; n <= filas; n++) {
      // N° serie auto VISIBLE (backend lo regenera con cid real al guardar)
      agregarFilaEquipo({
        nombre,
        sku: p.sku || '',
        serie: _serieTmpAuto(p.sku),
        cantidad: cantidadCadaUna,
        doc_origen,
        doc_fecha: fecha_doc,
        fecha_instalacion: fecha_doc,
        justif_doc_mismatch: justifMismatch,
      });
      creados++;
    }
  }

  // Cerrar modales y limpiar
  _wzModalProds.hide();
  setTimeout(() => {
    const mErp = bootstrap.Modal.getInstance(document.getElementById('modalErpEq'));
    if (mErp) mErp.hide();
  }, 200);
}

// ════════════════════════════════════════════════════════════════════
// importarEq() — botón "Importar seleccionadas" en el modal ERP grande
// Procesa selecciones de los tabs "Por documento" y "Por SKU" (que llenan erpEqSel)
// La decisión "1 ficha o N fichas" se toma POR PRODUCTO desde el select inline
// que pone el usuario en cada fila (data-fichas-key=KEY)
// ════════════════════════════════════════════════════════════════════
// Series temporales del wizard — único POR RUT del cliente que se está creando.
// Backend lo regenera con el formato definitivo (mismo formato) al guardar.
function _rutDelWizard() {
  const raw = (document.getElementById('s1_rut')?.value || '').replace(/[.\s-]/g,'').toUpperCase();
  if (!raw) return '00000000';
  return raw.length >= 8 ? raw.slice(0,-1) : raw;
}

function _serieTmpAuto(sku) {
  // Formato amigable: {RUT}-{SKU4}-{n}
  // Ej: 65206047-0905-1, 65206047-0905-2, ...
  const rut = _rutDelWizard();
  const skuClean = (sku || 'AUTO').toUpperCase().replace(/[^A-Z0-9]/g, '');
  const sku4 = skuClean.length >= 4 ? skuClean.slice(-4) : (skuClean.padStart(4, '0') || 'AUTO');
  const baseStr = `${rut}-${sku4}-`;
  let max = 0;
  document.querySelectorAll('#equiposTbody [data-field="serie"]').forEach(inp => {
    const v = (inp.value || '').toUpperCase();
    if (v.startsWith(baseStr)) {
      const suf = parseInt(v.slice(baseStr.length));
      if (!isNaN(suf) && suf > max) max = suf;
    }
  });
  return `${baseStr}${max + 1}`;
}

async function importarEq() {
  const lineas = Object.entries(erpEqSel);  // [key, line]
  if (!lineas.length) {
    ilusToast('Selecciona al menos un producto antes de importar.', { type:'warning' });
    return;
  }

  // Validar mismatch RUT por línea (documentos pueden traer rut)
  // FIX 2026-05-19: ilusRutsMatch maneja DV presente/ausente
  const rutWiz = (document.getElementById('s1_rut')?.value || '');
  let justifMismatch = '';
  for (const [, l] of lineas) {
    if (rutWiz && l.rut && !ilusRutsMatch(l.rut, rutWiz)) {
      const rutDoc = (l.rut || '').replace(/[.\s]/g,'');
      const motivo = await ilusPrompt({
        title: '⚠️ RUT distinto en documento',
        message: `Doc ${l.doc_origen||''} · RUT documento: ${rutDoc} · RUT cliente: ${rutWiz}`,
        sub: '¿Por qué importas equipos de otro RUT? Mínimo 8 caracteres.',
        placeholder: 'Justificación del cruce…',
        multiline: true,
        type: 'warning',
        okLabel: 'Justificar e importar',
      });
      if (!motivo || motivo.trim().length < 8) {
        ilusToast('Importación cancelada: justificación requerida (mín. 8 caracteres)', { type:'warning' });
        return;
      }
      justifMismatch = motivo.trim();
      break;
    }
  }

  // ── Detección de items con cantidad > 1 que NO se decidió cómo cargar ──
  // El dropdown default está en "1 fila". Si el usuario seleccionó items
  // con cantidad > 1 y no abrió el dropdown, le preguntamos explícitamente
  // antes de importar — UX clara, sin sorpresas.
  const ambiguos = [];
  for (const [key, l] of lineas) {
    if (l._completo) continue;
    const qty = parseInt(l.cantidad) || 1;
    if (qty <= 1) continue;
    const sel = document.querySelector(`select[data-fichas-key="${key}"]`);
    const fichasElegidas = sel ? parseInt(sel.value) : 1;
    if (fichasElegidas === 1) {
      ambiguos.push({ key, l, qty });
    }
  }

  if (ambiguos.length > 0) {
    // Pregunta agregada: 1 fila vs N fichas individuales.
    const listaText = ambiguos.map(a => `  · ${a.l.sku || '—'}: ${a.l.nombre} (cantidad ${a.qty})`).join('\n');
    const ans = confirm(
      `Tenés ${ambiguos.length} producto(s) con cantidad > 1 marcado(s) como "1 fila":\n\n${listaText}\n\n` +
      `¿Querés crear UNA FILA POR UNIDAD (con N° serie único por equipo)?\n\n` +
      `• Aceptar = crear N fichas individuales (recomendado para trotadoras, bicicletas, etc.)\n` +
      `• Cancelar = mantener 1 sola fila con cantidad=${ambiguos[0].qty} (recomendado para ítems idénticos sin serie)`
    );
    if (ans) {
      // Forzar split para todos los ambiguos
      for (const a of ambiguos) {
        const sel = document.querySelector(`select[data-fichas-key="${a.key}"]`);
        if (sel) sel.value = String(a.qty);
      }
    }
  }

  let creados = 0;
  let bloqueados = 0;
  for (const [key, l] of lineas) {
    if (l._completo) {            // saldo 0 → no se agrega
      bloqueados++;
      continue;
    }
    const qty = parseInt(l.cantidad) || 1;     // cantidad ya viene como SALDO disponible
    const qtyOriginal = parseInt(l.cantidad_original) || qty;
    const nombre = l.nombre || l.producto || '';
    // Leer la elección final del usuario: 1 ficha o N fichas
    const sel = document.querySelector(`select[data-fichas-key="${key}"]`);
    const fichasElegidas = sel ? parseInt(sel.value) : 1;
    const splitToRows = fichasElegidas > 1;
    const filas = splitToRows ? Math.min(fichasElegidas, qty) : 1;
    const cantidadCadaUna = splitToRows ? 1 : qty;
    for (let n = 1; n <= filas; n++) {
      agregarFilaEquipo({
        nombre,
        sku: l.sku || '',
        serie: _serieTmpAuto(l.sku),
        cantidad: cantidadCadaUna,
        cantidad_original: qtyOriginal,   // para validar saldo en backend
        _split_to_rows: splitToRows,
        doc_origen: l.doc_origen || '',
        doc_fecha: l.doc_fecha || l.fecha || '',
        fecha_instalacion: l.doc_fecha || l.fecha || '',
        justif_doc_mismatch: justifMismatch,
      });
      creados++;
    }
  }
  if (bloqueados > 0) {
    console.warn(`${bloqueados} producto(s) bloqueado(s) por saldo agotado`);
  }

  // Reset y cerrar modal
  erpEqSel = {};
  document.getElementById('erpEqCount').textContent = '0 seleccionadas';
  bootstrap.Modal.getInstance(document.getElementById('modalErpEq'))?.hide();
}
function abrirErpEquipos(){
  erpEqSetTab('doc');
  // Pre-llenar número de documento si el RUT del cliente ya está completado
  document.getElementById('docNudo').value = '';
  erpEqSel = {};
  document.getElementById('erpEqCount').textContent = '0 seleccionadas';
  document.getElementById('docErpRes').innerHTML = `
    <div class="text-center text-muted py-5" style="font-size:.85rem">
      <i class="bi bi-receipt" style="font-size:2rem;opacity:.3;display:block;margin-bottom:8px"></i>
      Ingresa tipo y número para traer los productos del documento
    </div>`;
  new bootstrap.Modal(document.getElementById('modalErpEq')).show();
}

// ── Buscar por documento REST API (con saldo global) ─────────
async function buscarDocErp() {
  const tido = document.getElementById('docTido').value;
  const nudo = document.getElementById('docNudo').value.trim();
  const cont = document.getElementById('docErpRes');
  if(!nudo) { cont.innerHTML='<div class="text-center text-warning py-3">Ingresa el número de documento</div>'; return; }

  cont.innerHTML=`<div class="text-center py-4"><span class="spinner-border spinner-border-sm me-2"></span>Consultando ERP y calculando saldos…</div>`;

  try {
    // Nuevo endpoint: además del documento, devuelve cantidad_original,
    // cantidad_asignada (GLOBAL, en TODA la BD), saldo_disponible y
    // asignaciones existentes en otros clientes.
    const r = await fetch(`/mantenciones/api/erp/documento-saldos?tido=${tido}&nudo=${encodeURIComponent(nudo)}`);
    const data = await r.json();

    if(!data.ok) {
      cont.innerHTML=`<div class="text-center text-danger py-3"><i class="bi bi-exclamation-triangle me-1"></i>${data.error||'Documento no encontrado'}</div>`;
      return;
    }

    if(!data.items?.length) {
      let msg = 'Este documento no tiene líneas de producto importables.';
      if(data.total_lineas > 0) {
        msg += ` (${data.total_lineas} línea(s) en el ERP, todas son servicios o fletes)`;
      } else {
        msg += ' (El documento no tiene líneas de detalle en el ERP)';
      }
      cont.innerHTML=`<div class="text-center text-muted py-3">${msg}</div>`;
      return;
    }

    const docOrigKey = `${tido} ${nudo}`;

    // BANNER: si TODO el documento ya está asignado en otros clientes
    let topBanner = '';
    if (data.doc_completo) {
      topBanner = `<div class="alert alert-warning py-2 mb-2" style="font-size:.85rem">
        <i class="bi bi-exclamation-triangle-fill me-1"></i>
        <strong>Documento ${docOrigKey} ya está totalmente asignado</strong>
        a otros clientes. No queda saldo disponible para importar.
        Si necesitás cambiar la asignación, primero eliminá los equipos
        del cliente anterior, o usa el modo override (no recomendado).
      </div>`;
    } else if (data.total_disponible > 0 && data.total_disponible < data.items.reduce((s,i)=>s+i.cantidad_original,0)) {
      topBanner = `<div class="alert alert-info py-2 mb-2" style="font-size:.85rem">
        <i class="bi bi-info-circle me-1"></i>
        Este documento tiene asignaciones parciales en otros clientes.
        Saldo total disponible: <strong>${data.total_disponible}</strong> unidad(es).
      </div>`;
    }

    // Guardar items: usamos saldo como cantidad "asignable"
    window._docItems = data.items.map(it => ({
      sku: it.sku || '',
      nombre: it.nombre || '',
      cantidad: it.saldo_disponible || 0,       // ← lo que se puede aún asignar
      cantidad_original: it.cantidad_original,  // ← lo original (para validar al guardar)
      cantidad_asignada: it.cantidad_asignada,
      doc_origen: docOrigKey,
      doc_fecha: data.fecha || '',
      rut: data.rut || '',
      cliente_doc: data.cliente || '',
      asignaciones: it.asignaciones || [],
      _completo: it.ya_completo
    }));

    // Header del documento
    let html = topBanner + `<div class="alert alert-success py-2 mb-3" style="font-size:.82rem">
      <strong><i class="bi bi-check-circle me-1"></i>${data.tido} ${data.nudo}</strong>
      ${data.fecha ? ` — ${data.fecha}` : ''}
      ${data.cliente ? ` &nbsp;·&nbsp; <strong>${esc(data.cliente)}</strong>` : ''}
      ${data.rut ? ` (${esc(data.rut)})` : ''}
    </div>`;

    html += `<div class="table-responsive"><table class="table table-sm table-hover align-middle" style="font-size:.82rem">
      <thead class="table-light"><tr>
        <th style="width:36px"></th>
        <th style="width:110px">SKU</th>
        <th>Producto</th>
        <th style="width:130px;text-align:center" title="Saldo = lo que falta por asignar de este documento">Saldo disponible</th>
        <th style="width:170px;text-align:center" title="Si tiene cantidad &gt; 1, ¿una sola fila o N fichas con N° serie único?">¿Cómo agregar?</th>
      </tr></thead><tbody>`;

    // También considerar saldo LOCAL: lo que ya está en la tabla del wizard
    // (sumado al saldo global ya calculado por el backend)
    const saldoLocalPorSku = {};
    Array.from(document.querySelectorAll('#equiposTbody tr:not(#eq-placeholder)')).forEach(tr => {
      const docF = tr.querySelector('[data-field="doc_origen"]')?.value || '';
      if (docF !== docOrigKey) return;
      const skuF = tr.querySelector('[data-field="sku"]')?.value || '';
      const qF   = parseInt(tr.querySelector('[data-field="cantidad"]')?.value || '1') || 1;
      saldoLocalPorSku[skuF] = (saldoLocalPorSku[skuF] || 0) + qF;
    });

    data.items.forEach((it, i) => {
      const key = `doc-${tido}-${nudo}-${i}`;
      const qtyOriginal = it.cantidad_original;
      const yaGlobal    = it.cantidad_asignada;
      const yaLocal     = saldoLocalPorSku[it.sku || ''] || 0;
      const saldoFinal  = Math.max(0, it.saldo_disponible - yaLocal);
      const completo    = saldoFinal === 0;

      // Tooltip de asignaciones existentes
      let tipAsignaciones = '';
      if ((it.asignaciones || []).length > 0) {
        const clientes = it.asignaciones
          .map(a => `${a.razon_social || 'Cliente #'+a.cliente_id} (${a.cantidad}u)`)
          .join(' | ');
        tipAsignaciones = `Ya asignado en: ${clientes}`;
      }

      // Selector "¿Cómo agregar?" — pregunta explícita por fila/cantidad
      const opciones = (saldoFinal > 1)
        ? `<select class="form-select form-select-sm wz-modo-fichas" data-fichas-key="${key}" style="font-size:.74rem;font-weight:600">
              <option value="1" selected>📋 1 fila (cantidad ${saldoFinal})</option>
              <option value="${saldoFinal}">🔢 ${saldoFinal} fichas individuales</option>
           </select>
           <div class="small text-muted" style="font-size:.66rem;margin-top:2px">
             Click el dropdown para elegir
           </div>`
        : `<span class="text-muted small">1 ficha</span>`;

      // Saldo cell con info detallada
      let saldoCell;
      if (completo) {
        saldoCell = `<span class="badge bg-success" title="${esc(tipAsignaciones || 'Asignado completo')}">
          <i class="bi bi-check-circle-fill me-1"></i>Sin saldo
        </span>
        <div class="small text-muted" style="font-size:.66rem">
          ${yaGlobal}/${qtyOriginal} ya asignadas
        </div>`;
      } else if (yaGlobal > 0 || yaLocal > 0) {
        saldoCell = `<span class="badge bg-warning text-dark" title="${esc(tipAsignaciones)}">
          ${saldoFinal} disp. de ${qtyOriginal}
        </span>
        <div class="small text-muted" style="font-size:.66rem">
          ${yaGlobal} otras fichas · ${yaLocal} en esta lista
        </div>`;
      } else {
        saldoCell = `<span class="badge bg-secondary">${qtyOriginal}</span>`;
      }

      const chkAttrs = completo ? 'disabled' : '';
      const trStyle = completo ? 'opacity:.55;background:#f8f9fa' : '';

      html += `<tr style="${trStyle}">
        <td><input type="checkbox" class="form-check-input wz-doc-chk" data-key="${key}" data-idx="${i}" ${chkAttrs}></td>
        <td class="font-monospace text-primary fw-bold">${esc(it.sku||'—')}</td>
        <td>${esc(it.nombre)}${tipAsignaciones ? `<div class="small text-muted" style="font-size:.7rem"><i class="bi bi-info-circle me-1"></i>${esc(tipAsignaciones)}</div>` : ''}</td>
        <td class="text-center">${saldoCell}</td>
        <td class="text-center">${completo ? '<span class="text-muted small">—</span>' : opciones}</td>
      </tr>`;

      // Para que importarEq sepa cuánto puede asignar
      if (window._docItems && window._docItems[i]) {
        window._docItems[i].cantidad = saldoFinal;
        window._docItems[i]._completo = completo;
      }
    });
    html += '</tbody></table></div>';

    // Botón "seleccionar todos"
    html = `<div class="d-flex justify-content-end mb-2">
      <button class="btn btn-sm btn-outline-secondary" onclick="docSelAll('${tido}','${nudo}',${data.items.length})">
        <i class="bi bi-check2-all me-1"></i>Seleccionar todos
      </button>
    </div>` + html;

    cont.innerHTML = html;

    // Pre-llenar datos del cliente si están vacíos
    if(data.cliente && !document.getElementById('s1_razon').value) {
      document.getElementById('s1_razon').value = data.cliente;
    }
    if(data.rut && !document.getElementById('s1_rut').value) {
      document.getElementById('s1_rut').value = data.rut;
    }

    // Conectar event delegation para los checkboxes (más robusto que onclick inline)
    cont.querySelectorAll('.wz-doc-chk').forEach(chk => {
      chk.addEventListener('change', function() {
        const idx = parseInt(this.dataset.idx);
        const key = this.dataset.key;
        const item = (window._docItems || [])[idx];
        if (!item) return;
        if (this.checked) {
          erpEqSel[key] = {...item};
        } else {
          delete erpEqSel[key];
        }
        document.getElementById('erpEqCount').textContent = `${Object.keys(erpEqSel).length} seleccionadas`;
      });
    });

  } catch(e) {
    cont.innerHTML=`<div class="text-center text-danger py-3">Error de red: ${e.message}</div>`;
  }
}

function docSelAll(tido, nudo, count) {
  const cont = document.getElementById('docErpRes');
  cont.querySelectorAll('.wz-doc-chk').forEach(chk => {
    if(!chk.checked) {
      chk.checked = true;
      chk.dispatchEvent(new Event('change'));
    }
  });
}

// ══ BUSCADOR INLINE DE EQUIPOS (paso 2) ══════════════════════
let eqSearchTimer = null;
let eqSearchResults = [];
let eqSearchIdx = -1;

function eqSearchDebounce() {
  clearTimeout(eqSearchTimer);
  const q = document.getElementById('eqSearchInput').value.trim();
  const drop = document.getElementById('eqProdDrop');
  if(q.length < 2) { drop.style.display='none'; return; }
  drop.innerHTML='<div class="eq-prod-item text-muted"><span class="spinner-border spinner-border-sm me-1"></span>Buscando…</div>';
  drop.style.display='block';
  eqSearchTimer = setTimeout(eqSearchFetch, 300);
}

async function eqSearchFetch() {
  const q = document.getElementById('eqSearchInput').value.trim();
  if(q.length < 2) return;
  try {
    const r = await fetch(`/mantenciones/api/productos/buscar?q=${encodeURIComponent(q)}`);
    eqSearchResults = await r.json();
    eqSearchIdx = -1;
    renderEqSearchDrop();
  } catch(e) {
    document.getElementById('eqProdDrop').innerHTML=
      '<div class="eq-prod-item text-muted small">Error al buscar</div>';
  }
}

// True si el SKU ya está en la tabla de equipos seleccionados de este wizard
function _skuYaEnLista(sku) {
  if (!sku) return false;
  const skuU = String(sku).trim().toUpperCase();
  const inputs = document.querySelectorAll('#equiposTbody input[data-field="sku"]');
  for (const inp of inputs) {
    if ((inp.value || '').trim().toUpperCase() === skuU) return true;
  }
  return false;
}

function renderEqSearchDrop() {
  const drop = document.getElementById('eqProdDrop');
  if(!eqSearchResults.length) {
    drop.innerHTML='<div class="eq-prod-item text-muted small">Sin resultados — prueba con otro nombre o SKU</div>';
    drop.style.display='block';
    return;
  }
  drop.innerHTML = eqSearchResults.map((p,i)=>{
    const ya = _skuYaEnLista(p.sku);
    const cls = ya ? 'eq-prod-item' : ('eq-prod-item'+(i===eqSearchIdx?' active':''));
    const click = ya ? '' : `onclick="eqSearchSeleccionar(${i})"`;
    const nombre = (p.nombre||'—').replace(/[<>]/g,'');
    const sku    = (p.sku||'').replace(/[<>]/g,'');
    const tipo   = (p.tipo||'').replace(/[<>]/g,'');
    return `
    <div class="${cls}" ${click} style="${ya ? 'opacity:.5;cursor:default;background:#f9fafb' : ''}">
      <div class="d-flex justify-content-between align-items-start gap-2">
        <div style="flex:1;min-width:0">
          <div class="eq-prod-name">${nombre}</div>
          <div class="eq-prod-sub">
            <span class="eq-prod-sku" style="font-family:monospace;font-weight:700;color:#0066cc">${sku}</span>
            ${tipo ? ` <span class="text-muted">· ${tipo}</span>` : ''}
          </div>
        </div>
        ${ya
          ? '<span class="badge bg-success-subtle text-success border border-success-subtle" style="font-size:.62rem;font-weight:700;align-self:center"><i class="bi bi-check-circle-fill me-1"></i>Ya agregado</span>'
          : '<span class="badge bg-light text-muted border" style="font-size:.62rem;font-weight:600;align-self:center"><i class="bi bi-plus-lg me-1"></i>Agregar</span>'
        }
      </div>
    </div>`;
  }).join('');
  drop.style.display='block';
}

function eqSearchSeleccionar(i) {
  const p = eqSearchResults[i];
  if(!p) return;
  agregarFilaEquipo({ nombre: p.nombre||'', sku: p.sku||'', cantidad:1 });
  document.getElementById('eqSearchInput').value='';
  document.getElementById('eqProdDrop').style.display='none';
  eqSearchResults=[];
}

function eqSearchKeydown(e) {
  const drop = document.getElementById('eqProdDrop');
  const items = drop.querySelectorAll('.eq-prod-item');
  if(!items.length) return;
  if(e.key==='ArrowDown') { eqSearchIdx=Math.min(eqSearchIdx+1,items.length-1); renderEqSearchDrop(); e.preventDefault(); }
  else if(e.key==='ArrowUp') { eqSearchIdx=Math.max(eqSearchIdx-1,-1); renderEqSearchDrop(); e.preventDefault(); }
  else if(e.key==='Enter' && eqSearchIdx>=0) { eqSearchSeleccionar(eqSearchIdx); e.preventDefault(); }
  else if(e.key==='Escape') { drop.style.display='none'; }
}

// Cerrar dropdown al hacer click fuera
document.addEventListener('click', e2 => {
  if(!document.getElementById('eqSearchInput')?.contains(e2.target))
    document.getElementById('eqProdDrop').style.display='none';
});

// ── Tab "Por producto" ────────────────────────────────────────
let erpProdTimer = null;
function erpProdDebounce() {
  clearTimeout(erpProdTimer);
  erpProdTimer = setTimeout(buscarErpProd, 350);
}

async function buscarErpProd() {
  const q = document.getElementById('erpProdQ').value.trim();
  const cont = document.getElementById('erpProdRes');
  if(q.length < 2) {
    cont.innerHTML='<div class="text-center text-muted py-4" style="font-size:.85rem">Ingresa al menos 2 caracteres para buscar</div>';
    return;
  }
  cont.innerHTML='<div class="text-center py-3"><span class="spinner-border spinner-border-sm me-1"></span>Buscando en catálogo ERP…</div>';
  try {
    const r = await fetch(`/mantenciones/api/productos/buscar?q=${encodeURIComponent(q)}`);
    const data = await r.json();
    if(!data.length) {
      cont.innerHTML='<div class="text-center text-muted py-3">Sin resultados para "'+q+'"</div>';
      return;
    }
    let html='<div class="table-responsive"><table class="table table-sm table-hover" style="font-size:.82rem">';
    html+='<thead><tr><th></th><th>SKU</th><th>Descripción</th><th style="width:70px">Cant.</th></tr></thead><tbody>';
    data.forEach((p,i)=>{
      const key=`prod-${i}-${p.sku}`;
      const ld=JSON.stringify({sku:p.sku,nombre:p.nombre,cantidad:1,doc_origen:'',doc_fecha:''}).replace(/"/g,'&quot;');
      html+=`<tr>
        <td><input type="checkbox" onchange="erpProdToggle('${key}',this)"></td>
        <td class="font-monospace text-primary fw-bold">${p.sku}</td>
        <td>${p.nombre||'—'}</td>
        <td><input type="number" id="qty-${key}" value="1" min="1" max="999" class="form-control form-control-sm text-center p-1"
             style="width:60px" data-sku="${p.sku}" data-nombre="${p.nombre.replace(/"/g,'&quot;')}"></td>
      </tr>`;
    });
    html+='</tbody></table></div>';
    cont.innerHTML=html;
  } catch(e) {
    cont.innerHTML='<div class="text-center text-danger py-3">Error al buscar: '+e.message+'</div>';
  }
}

function erpProdToggle(key, chk) {
  const qtyEl = document.getElementById('qty-'+key);
  if(chk.checked) {
    const qty = parseInt(qtyEl?.value)||1;
    erpEqSel[key] = {
      sku:        chk.closest('tr').querySelector('.font-monospace').textContent.trim(),
      nombre:     qtyEl?.dataset.nombre || '',
      cantidad:   qty,
      doc_origen: '',
      doc_fecha:  ''
    };
  } else {
    delete erpEqSel[key];
  }
  // Actualizar cantidad al cambiar el input
  if(qtyEl && !qtyEl._bound) {
    qtyEl._bound = true;
    qtyEl.addEventListener('change', ()=>{
      if(erpEqSel[key]) erpEqSel[key].cantidad = parseInt(qtyEl.value)||1;
    });
  }
  document.getElementById('erpEqCount').textContent=`${Object.keys(erpEqSel).length} seleccionadas`;
}

// ══ PASO 3 — CONTRATO ════════════════════════════════════════
function onArchivoSeleccionado(){
  const f=document.getElementById('ct_archivo').files[0];
  if(f) mostrarArchivoCargado(f);
}
function onDrop(e){
  e.preventDefault();
  document.getElementById('dropZone').classList.remove('drag-over');
  const f=e.dataTransfer.files[0];
  if(f){const dt=new DataTransfer();dt.items.add(f);document.getElementById('ct_archivo').files=dt.files;mostrarArchivoCargado(f);}
}
function mostrarArchivoCargado(f){
  const zone=document.getElementById('dropZone');
  zone.classList.add('has-file');
  document.getElementById('dropContent').innerHTML=`
    <i class="bi bi-file-earmark-check" style="font-size:2rem;color:#1a7a1a;display:block;margin-bottom:8px"></i>
    <div class="fw-bold" style="color:#1a7a1a">${f.name}</div>
    <div class="small text-muted">${(f.size/1024/1024).toFixed(2)} MB — haz clic para cambiar</div>`;
  document.getElementById('ct_meta').style.display='';
  if(!document.getElementById('ct_nombre').value) document.getElementById('ct_nombre').value=f.name.replace(/\.[^.]+$/,'');
}
function agregarAdjuntos(input){
  for(const f of Array.from(input.files)){
    if(adjFiles.length>=4){alert('Máximo 4 adjuntos');break;}
    adjFiles.push(f);
  }
  renderAdjuntos();input.value='';
}
function renderAdjuntos(){
  document.getElementById('adjuntosLista').innerHTML=adjFiles.map((f,i)=>
    `<span class="adj-chip"><i class="bi bi-paperclip text-muted"></i>${f.name} <button style="background:none;border:none;padding:0;color:#cc0000" onclick="adjFiles.splice(${i},1);renderAdjuntos()">✕</button></span>`
  ).join('');
}

async function analizarConIA(){
  const _lbl='<i class="bi bi-robot me-1"></i>Analizar contrato (Agente ILUS)';
  const archivo=document.getElementById('ct_archivo').files[0];
  if(!archivo){ilusToast('Sube el contrato primero', {type:'warning'});return;}
  const btn=document.getElementById('btnAnalizar');
  btn.disabled=true;btn.innerHTML='<span class="spinner-border spinner-border-sm me-1"></span>Analizando…';
  try{
    // Agente ILUS determinista (CERO IA): lee el documento y devuelve resultado.contrato.
    const fd=new FormData(); fd.append('archivo', archivo);
    const r=await fetch('/mantenciones/api/agente-contrato',{method:'POST',body:fd});
    const data=await r.json();
    if(!r.ok || data.error){
      ilusAlert({title:'No se pudo analizar', message:data.error||'Intenta de nuevo', type:'error'});
      btn.disabled=false;btn.innerHTML=_lbl;return;
    }
    if(data.requiere_rut){
      ilusToast(data.mensaje||'Para fotos usa el RUT en el Paso 1', {type:'info'});
      btn.disabled=false;btn.innerHTML=_lbl;return;
    }
    poblarPanelIA((data.resultado||{}).contrato || {});
    btn.innerHTML='<i class="bi bi-check-circle me-1"></i>Re-analizar';btn.disabled=false;
    ilusToast('✓ Contrato analizado por el Agente ILUS', {type:'success'});
  }catch(e){
    ilusAlert({title:'Error', message:'No se pudo analizar el contrato', type:'error'});
    btn.disabled=false;btn.innerHTML=_lbl;
  }
}

function poblarPanelIA(r){
  if(!r)return;
  document.getElementById('ai_panel').style.display='';
  const score=r.score||r.ai_score||0;
  const clr=score>=70?'#1a7a1a':score>=40?'#e65100':'#cc0000';
  const sc=document.getElementById('ai_score_circle');
  sc.textContent=score;sc.style.background=clr;
  const riesgo=(r.nivel_riesgo||r.nivel_riesgo_sug||'medio');
  const semMap={alto:'🔴 Riesgo Alto',medio:'🟡 Riesgo Medio',bajo:'🟢 Riesgo Bajo'};
  const clsMap={alto:'semaforo-alto',medio:'semaforo-medio',bajo:'semaforo-bajo'};
  document.getElementById('ai_semaforo').innerHTML=
    `<span class="score-pill" style="background:${riesgo==='alto'?'#fdecea':riesgo==='bajo'?'#e8f5e9':'#fff3e0'};color:${clr}">${semMap[riesgo]||riesgo}</span>`;
  document.getElementById('ai_tipo').value         = r.tipo_contrato||r.ai_tipo_contrato||'';
  document.getElementById('ai_riesgo').value        = riesgo;
  document.getElementById('ai_frecuencia').value    = r.frecuencia_sugerida_meses||r.frecuencia_meses||r.ai_frecuencia_sug||'';
  document.getElementById('ai_inicio').value         = r.vigencia_inicio||r.ai_vigencia_inicio||'';
  document.getElementById('ai_fin').value            = r.vigencia_fin||r.ai_vigencia_fin||'';
  document.getElementById('ai_sla').value            = r.sla_horas||'';
  document.getElementById('ai_costo_mensual').value  = r.costo_mensual||r.monto_mensual_sugerido||'';
  document.getElementById('ai_costo_visita').value   = r.costo_por_mant||'';
  document.getElementById('ai_costo_total').value    = r.costo_total||'';
  document.getElementById('ai_mant_gratis').checked  = !!(r.incluye_mant_gratis);
  document.getElementById('ai_repuestos').checked    = !!(r.incluye_repuestos);
  document.getElementById('ai_resumen').value        = r.resumen||r.ai_resumen||'';
  document.getElementById('ai_cobertura').value      = r.cobertura_descripcion||r.ai_cobertura||'';
  const clausulas=(r.clausulas_criticas||r.puntos_criticos||[]);
  document.getElementById('ai_clausulas').value      = Array.isArray(clausulas)?clausulas.join('\n'):clausulas;
  const alertas=(r.alertas||[]);
  document.getElementById('ai_alertas').value        = Array.isArray(alertas)?alertas.join('\n'):alertas;
  const mejoras=(r.mejoras_prioritarias||[]);
  document.getElementById('ai_mejoras').value        = Array.isArray(mejoras)?mejoras.join('\n'):mejoras;
  // Mostrar valor referencial en CLP si el contrato es en UF
  if((r.moneda||'').toUpperCase()==='UF' || (r.monto_mensual||0)>0&&(r.monto_mensual||0)<500) {
    const costoUF = parseFloat(r.costo_mensual||r.monto_mensual||0);
    if(costoUF>0 && costoUF<10000) { // probablemente en UF si es < 10000
      fetch('/api/uf-actual').then(r=>r.json()).then(uf=>{
        if(uf.ok&&uf.uf) {
          const clp = Math.round(costoUF * uf.uf);
          const existing = document.getElementById('ai_uf_ref');
          if(!existing) {
            const div=document.createElement('div');
            div.id='ai_uf_ref';
            div.className='erp-info mt-2';
            div.style.fontSize='.78rem';
            // 2026-07-24 (mismo bug reportado por Daniel en Cotizaciones --
            // "la fecha nuevamente es gringa" -- Regla #6): mindicador.cl
            // devuelve "YYYY-MM-DD", se muestra "DD/MM/YYYY". Regex inline
            // (único uso en este archivo) en vez de un helper nuevo --
            // no-op si el formato no calza (defensivo).
            const _ufFechaCl = (uf.fecha||'').replace(/^(\d{4})-(\d{2})-(\d{2})$/, '$3/$2/$1');
            div.innerHTML=`<i class="bi bi-calculator me-1"></i><strong>${costoUF} UF</strong> = <strong>$${clp.toLocaleString('es-CL')}</strong> CLP (UF al ${_ufFechaCl}: $${Math.round(uf.uf).toLocaleString('es-CL')})`;
            document.getElementById('ai_costo_mensual').closest('.ai-box')?.prepend(div)
              || document.getElementById('ai_panel').prepend(div);
          }
        }
      }).catch(()=>{});
    }
  }
  document.getElementById('ai_panel').scrollIntoView({behavior:'smooth',block:'nearest'});
}

function leerAiEditado(){
  return {
    ai_tipo_contrato:  document.getElementById('ai_tipo').value,
    nivel_riesgo:      document.getElementById('ai_riesgo').value,
    ai_vigencia_inicio:document.getElementById('ai_inicio').value||null,
    ai_vigencia_fin:   document.getElementById('ai_fin').value||null,
    frecuencia_meses:  parseInt(document.getElementById('ai_frecuencia').value)||0,
    sla_horas:         parseInt(document.getElementById('ai_sla').value)||0,
    monto_mensual:     parseFloat(document.getElementById('ai_costo_mensual').value)||0,
    costo_por_mant:    parseFloat(document.getElementById('ai_costo_visita').value)||0,
    costo_total:       parseFloat(document.getElementById('ai_costo_total').value)||0,
    incluye_mant_gratis: document.getElementById('ai_mant_gratis').checked?1:0,
    incluye_repuestos: document.getElementById('ai_repuestos').checked?1:0,
    ai_resumen:        document.getElementById('ai_resumen').value,
    ai_cobertura:      document.getElementById('ai_cobertura').value,
    ai_clausulas:      document.getElementById('ai_clausulas').value,
    ai_editable:       '1',
  };
}

// ══ Navegación entre pasos ════════════════════════════════════
function irPaso2(){
  const razonEl = document.getElementById('s1_razon');
  if(!razonEl.value.trim()){
    razonEl.focus();
    razonEl.classList.add('is-invalid');
    alert('La Razón Social es obligatoria');
    return;
  }
  razonEl.classList.remove('is-invalid');

  // Validación de RUT con override consciente: si el RUT está rellenado
  // pero su DV no coincide, preguntar antes de avanzar (no esperar al final).
  const rutEl = document.getElementById('s1_rut');
  const rutVal = (rutEl.value || '').trim();
  WZ.rut_override = false;
  if (rutVal && !_validarRut(rutVal)) {
    const dvEsp = (typeof _dvEsperado === 'function') ? _dvEsperado(rutVal) : null;
    const msg =
      `⚠ El RUT "${rutVal}" tiene dígito verificador inválido` +
      (dvEsp ? ` (DV esperado: ${dvEsp}).\n\n` : '.\n\n') +
      `¿Cómo quieres continuar?\n\n` +
      `• Aceptar = continuar con este RUT igual (RUT antiguo, dummy, ` +
      `extranjero o el sistema se equivoca)\n` +
      `• Cancelar = corregir el RUT antes de avanzar`;
    if (!confirm(msg)) {
      rutEl.focus();
      rutEl.classList.add('is-invalid');
      rutEl.scrollIntoView({block:'center', behavior:'smooth'});
      return;
    }
    WZ.rut_override = true;   // se aplicará al llamar /clientes/nuevo
  }
  rutEl.classList.remove('is-invalid');

  WZ.equipos=leerEquipos();
  document.getElementById('erpEqQ').value=document.getElementById('s1_rut').value||document.getElementById('s1_razon').value;
  irPaso(2);
}
function irPaso3(){WZ.equipos=leerEquipos();irPaso(3);}
async function irPaso4(){
  if(WZ.ctid){const e=leerAiEditado();if(e)await fetch(`/mantenciones/api/contratos/${WZ.ctid}/ai-editar`,{method:'PUT',headers:{'Content-Type':'application/json'},body:JSON.stringify(e)});}
  construirResumen();irPaso(4);
}

// ══ Resumen paso 4 ════════════════════════════════════════════
function construirResumen(){
  const razon=document.getElementById('s1_razon').value;
  const rut=document.getElementById('s1_rut').value;
  const dir=document.getElementById('s1_direccion').value;
  const com=document.getElementById('s1_comuna').value;
  document.getElementById('res_cliente').innerHTML=`
    <div class="fw-bold">${razon}</div>
    ${rut?`<div class="small text-muted font-monospace">${rut}</div>`:''}
    ${dir||com?`<div class="small text-muted">${dir}${com?', '+com:''}</div>`:''}
    ${document.getElementById('s1_contacto_nombre').value?`<div class="small text-muted"><i class="bi bi-person me-1"></i>${document.getElementById('s1_contacto_nombre').value}</div>`:''}`;
  const eqs=leerEquipos();
  document.getElementById('res_equipos').innerHTML=eqs.length
    ?eqs.map(e=>`<div class="small mb-1"><i class="bi bi-bicycle me-1 text-muted"></i><strong>${e.nombre}</strong>${e.sku?' · '+e.sku:''}</div>`).join('')
    :'<div class="small text-muted">Sin equipos</div>';
  const ctN=document.getElementById('ct_nombre').value;
  const arch=document.getElementById('ct_archivo').files[0];
  if(arch||WZ.ctid){
    const freq=document.getElementById('ai_frecuencia').value||document.getElementById('ct_frecuencia').value;
    const riesgo=document.getElementById('ai_riesgo').value||'medio';
    const score=document.getElementById('ai_score_circle').textContent;
    const clr={alto:'danger',medio:'warning',bajo:'success'};
    document.getElementById('res_contrato').innerHTML=`
      <div class="d-flex align-items-center gap-2 flex-wrap mb-1">
        <span class="fw-bold">${ctN||arch?.name||'Contrato'}</span>
        <span class="badge bg-${clr[riesgo]||'secondary'}">${riesgo}</span>
        ${score&&score!=='—'?`<span class="badge bg-secondary">Score ${score}/100</span>`:''}
      </div>
      ${freq?`<div class="small text-muted">Frecuencia: cada <strong>${freq} meses</strong></div>`:''}
      ${document.getElementById('ai_costo_mensual').value?`<div class="small text-muted">Mensual: <strong>$${Number(document.getElementById('ai_costo_mensual').value).toLocaleString('es-CL')}</strong></div>`:''}`;
    if(freq){document.getElementById('cal_section').style.display='';previewCalendario();}
  } else {
    document.getElementById('res_contrato').innerHTML='<div class="small text-muted">Sin contrato adjunto</div>';
  }
}

async function previewCalendario(){
  const freq=parseInt(document.getElementById('ai_frecuencia').value||document.getElementById('ct_frecuencia').value||3);
  const tipo=document.getElementById('cal_tipo').value;
  const meses=parseInt(document.getElementById('cal_meses').value)||12;
  if(WZ.cid){
    const r=await fetch(`/mantenciones/api/clientes/${WZ.cid}/generar-calendario`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({dry_run:true,tipo,meses})});
    const d=await r.json();WZ.calPreview=d.preview||[];
  } else {
    WZ.calPreview=genPreviewLocal(freq,meses,tipo);
  }
  renderCal();
}
function genPreviewLocal(freq,meses,tipo){
  const list=[];let d=new Date();
  const max=Math.ceil(meses/Math.max(freq,1));
  for(let i=0;i<max;i++){
    list.push({fecha:d.toISOString().slice(0,10),tipo});
    let mes=d.getMonth()+freq;let anio=d.getFullYear()+Math.floor(mes/12);
    d=new Date(anio,mes%12,1);
  }
  return list;
}
const TIPO_CLR={preventiva:'#1a7a1a',correctiva:'#cc0000',garantia:'#0066cc',inspeccion:'#f57c00'};
function renderCal(){
  const cont=document.getElementById('cal_preview');
  if(!WZ.calPreview.length){cont.innerHTML='';return;}
  cont.innerHTML=`<div class="small text-muted mb-1">${WZ.calPreview.length} visita(s) programada(s):</div>`+
    WZ.calPreview.slice(0,6).map(v=>`
      <div class="cal-row">
        <div style="width:10px;height:10px;border-radius:50%;background:${TIPO_CLR[v.tipo]||'#888'};flex-shrink:0"></div>
        <div class="fw-semibold">${v.fecha}</div>
        <div class="text-muted">${v.tipo}</div>
        <span class="badge bg-light text-secondary" style="font-size:.62rem">Programada</span>
      </div>`).join('')+
    (WZ.calPreview.length>6?`<div class="small text-muted">+ ${WZ.calPreview.length-6} más…</div>`:'');
}

// ══ Guardar ═══════════════════════════════════════════════════
async function crearCliente(){
  const $v = id => (document.getElementById(id)||{}).value?.trim() || '';
  const fd=new FormData();
  fd.append('razon_social',    $v('s1_razon'));
  fd.append('rut',             $v('s1_rut').replace(/\./g,'').toUpperCase());
  fd.append('email_empresa',   $v('s1_email_empresa'));
  fd.append('tel_empresa',     $v('s1_tel_empresa'));
  fd.append('giro',            $v('s1_giro'));
  fd.append('direccion',       $v('s1_direccion'));
  fd.append('direccion_lat',       $v('s1_direccion_lat'));
  fd.append('direccion_lng',       $v('s1_direccion_lng'));
  fd.append('direccion_place_id',  $v('s1_direccion_place_id'));
  fd.append('comuna',          $v('s1_comuna'));
  fd.append('ciudad',          $v('s1_ciudad'));
  fd.append('region',          $v('s1_ciudad')); // Ciudad/Región = región ERP
  // Contacto principal
  fd.append('contacto_nombre', $v('s1_contacto_nombre'));
  fd.append('contacto_cargo',  $v('s1_contacto_cargo'));
  fd.append('contacto_tel',    $v('s1_contacto_tel'));
  fd.append('contacto_email',  $v('s1_contacto_email'));
  // Contacto secundario
  fd.append('contacto2_nombre', $v('s1_contacto2_nombre'));
  fd.append('contacto2_cargo',  $v('s1_contacto2_cargo'));
  fd.append('contacto2_tel',    $v('s1_contacto2_tel'));
  fd.append('contacto2_email',  $v('s1_contacto2_email'));
  // Notas
  fd.append('notas',           $v('s1_notas'));
  fd.append('estado',          document.getElementById('s1_estado').value);
  // Si el usuario ya confirmó en paso 1 que el RUT es correcto aunque DV
  // falle, pasamos rut_force=1 desde el inicio para evitar el segundo prompt.
  if (WZ.rut_override) fd.append('rut_force', '1');

  let r, data;
  const fetchHeaders = {'X-Wizard':'1'};
  if (WZ.rut_override) fetchHeaders['X-RUT-Force'] = '1';
  try {
    r = await fetch('/mantenciones/clientes/nuevo',{method:'POST', headers: fetchHeaders, body: fd});
  } catch(e) {
    alert(`No se pudo conectar con el servidor.\n\nError: ${e.message || e}\n\nVerifica tu conexión a internet e intenta nuevamente.`);
    return false;
  }
  try { data = await r.json(); } catch(_) { data = null; }
  if (data && data.ok) { WZ.cid = data.id; return true; }

  // ── CASO ESPECIAL: RUT con DV inválido ───────────────────────────
  // Le ofrecemos al usuario "guardar de todos modos" para que no pierda
  // el trabajo. Útil cuando el RUT es real pero el cálculo módulo-11
  // falla (RUTs antiguos, dummy, extranjeros, datos del ERP).
  if (data && data.error_codigo === 'RUT_DV_INVALIDO') {
    const rutMostrado = data.rut_input || $v('s1_rut');
    const conf = confirm(
      `⚠ El RUT "${rutMostrado}" tiene el dígito verificador incorrecto según el cálculo estándar (módulo-11).\n\n` +
      `${data.error}\n\n` +
      `Si estás SEGURO que el RUT es correcto (puede ser un RUT antiguo, ` +
      `dummy o de empresa extranjera), pulsa Aceptar para guardar de todos modos.\n\n` +
      `Si no estás seguro, pulsa Cancelar y corrige el RUT en el paso 1.`
    );
    if (conf) {
      // Reintentar con rut_force=1
      fd.append('rut_force', '1');
      try {
        r = await fetch('/mantenciones/clientes/nuevo',{method:'POST',headers:{'X-Wizard':'1','X-RUT-Force':'1'},body:fd});
        data = await r.json();
        if (data && data.ok) { WZ.cid = data.id; return true; }
      } catch(e) {
        alert('Error al reintentar: ' + (e.message || e));
        return false;
      }
    } else {
      // Cancela → ir a paso 1
      if (typeof goStep === 'function') goStep(1);
      return false;
    }
  }

  // ── CASO DUPLICADO: ofrecer abrir el cliente existente ──
  if (data && data.duplicate_id) {
    const irExistente = confirm(
      `Ya existe un cliente con ese RUT (ID ${data.duplicate_id}).\n\n` +
      `• Aceptar = abrir la ficha existente\n` +
      `• Cancelar = corregir el RUT en el paso 1`
    );
    if (irExistente) {
      // Borrar draft local (vamos a la ficha real)
      try { localStorage.removeItem('wiz_cliente_draft_v1'); } catch(_){}
      window.location.href = `/mantenciones/clientes/${data.duplicate_id}`;
      return false;
    } else {
      if (typeof goStep === 'function') goStep(1);
      else if (typeof irPaso === 'function') irPaso(1);
      setTimeout(() => {
        const el = document.getElementById('s1_rut');
        if (el) { el.focus(); el.classList.add('is-invalid'); el.scrollIntoView({block:'center'}); }
      }, 150);
      return false;
    }
  }

  // ── ERROR GENÉRICO con focus al campo culpable ──
  const errMsg = (data && data.error) || `HTTP ${r.status} ${r.statusText}` || 'Error desconocido';
  alert(`No se pudo crear el cliente:\n\n${errMsg}\n\nRevisa los campos del paso 1.`);

  // Si conocemos el campo culpable, llevar al usuario a corregirlo
  if (data && data.error) {
    // Detectar campo desde el mensaje (RUT, email, etc.)
    let campoId = null;
    if (/RUT/i.test(data.error)) campoId = 's1_rut';
    else if (/email_empresa/i.test(data.error)) campoId = 's1_email_empresa';
    else if (/contacto_email/i.test(data.error)) campoId = 's1_contacto_email';
    else if (/contacto2_email/i.test(data.error)) campoId = 's1_contacto2_email';
    else if (/raz[oó]n social/i.test(data.error)) campoId = 's1_razon';

    if (campoId) {
      if (typeof irPaso === 'function') irPaso(1);
      setTimeout(() => {
        const el = document.getElementById(campoId);
        if (el) { el.focus(); el.classList.add('is-invalid'); el.scrollIntoView({block:'center'}); }
      }, 150);
    }
  }
  console.error('[crearCliente] error backend:', data, 'status:', r?.status);
  return false;
}
async function subirContrato(){
  const arch = WZ.archivoContrato || document.getElementById('ct_archivo').files[0];
  if(!arch)return true;
  if(!WZ.cid)return false;
  const fd=new FormData();
  fd.append('archivo',archivo=arch);
  fd.append('nombre',document.getElementById('ct_nombre').value.trim()||arch.name);
  fd.append('fecha_inicio',document.getElementById('ct_inicio').value);
  fd.append('fecha_vencimiento',document.getElementById('ct_vencimiento').value);
  fd.append('es_indefinido',document.getElementById('ct_indefinido').checked?'1':'');
  fd.append('monto_mensual',document.getElementById('ct_monto_mensual').value||'0');
  fd.append('frecuencia_meses',document.getElementById('ct_frecuencia').value||'0');
  const r=await fetch(`/mantenciones/api/clientes/${WZ.cid}/contratos`,{method:'POST',body:fd});
  const data=await r.json();
  if(data.ok){WZ.ctid=data.id;return true;}
  alert('Error al subir contrato');return false;
}
async function subirMaquinas(){
  const eqs = leerEquipos();
  const errores = [];
  let exitosos = 0;
  const TIMEOUT_MS = 60000;  // 60s por equipo (antes era infinito → 'Failed to fetch')
  const CONCURRENCIA = 4;    // 4 requests simultáneos máx (no saturar el ERP / worker)

  async function subirUno(eq){
    const codInt = (eq.codigo_interno || '').trim();
    const codigoFinal = (codInt.startsWith('(auto') || !codInt) ? '' : codInt;
    const qtyOriginal = parseInt(eq.cantidad_original || 0) || 0;
    const splitFlag   = String(eq.split_to_rows||'') === '1';
    const tieneDoc    = !!(eq.doc_origen && eq.sku);
    const ctrl = (typeof AbortController !== 'undefined') ? new AbortController() : null;
    const timer = ctrl ? setTimeout(() => ctrl.abort(), TIMEOUT_MS) : null;
    try {
      const r = await fetch(`/mantenciones/api/clientes/${WZ.cid}/maquinas`,{
        method:'POST',
        headers:{'Content-Type':'application/json'},
        signal: ctrl ? ctrl.signal : undefined,
        body:JSON.stringify({
          nombre:             eq.nombre,
          sku:                eq.sku,
          codigo_interno:     codigoFinal,
          serie:              eq.serie,
          cantidad:           parseInt(eq.cantidad)||1,
          cantidad_original:  qtyOriginal,
          split_to_rows:      splitFlag,
          validar_saldo:      tieneDoc && qtyOriginal > 0,
          ubicacion_cliente:  eq.ubicacion,
          estado_op:          eq.estado_op,
          fecha_instalacion:  eq.fecha_instalacion||null,
          doc_origen:         eq.doc_origen||'',
          doc_fecha:          eq.doc_fecha||null,
          justif_fecha_inst:  eq.justif_fecha_inst||'',
          justif_doc_mismatch:eq.justif_doc_mismatch||'',
        })
      });
      const d = await r.json();
      if (d.ok) {
        exitosos += (d.filas_creadas || 1);
      } else if (d.error_codigo === 'SALDO_INSUFICIENTE') {
        errores.push(`⚠ ${eq.sku} (${eq.nombre}): ${d.error}`);
      } else {
        errores.push(`✗ ${eq.sku || eq.nombre}: ${d.error || 'error desconocido'}`);
      }
    } catch(e) {
      if (e && e.name === 'AbortError') {
        errores.push(`✗ ${eq.sku || eq.nombre}: timeout (60s)`);
      } else {
        errores.push(`✗ ${eq.sku || eq.nombre}: error de red`);
      }
    } finally {
      if (timer) clearTimeout(timer);
    }
  }

  // Procesar en lotes de CONCURRENCIA (4 simultáneos): mucho más rápido que
  // serial, pero sin saturar gunicorn (2 workers).
  for (let i = 0; i < eqs.length; i += CONCURRENCIA) {
    const lote = eqs.slice(i, i + CONCURRENCIA);
    await Promise.all(lote.map(subirUno));
  }
  return { total: eqs.length, exitosos, errores };
}

async function finalizar(){
  const btn=document.getElementById('btnFinalizar');
  btn.disabled=true;btn.innerHTML='<span class="spinner-border spinner-border-sm me-1"></span>Guardando…';
  try{
    if(!WZ.cid){const ok=await crearCliente();if(!ok){btn.disabled=false;btn.innerHTML='<i class="bi bi-floppy me-1"></i>Guardar y finalizar';return;}}
    // QW2: si TODOS los equipos fallaron, abortar antes de redirect para que
    // el usuario corrija (no terminar con un cliente vacío).
    const resMaq = await subirMaquinas();
    if (resMaq && resMaq.total > 0 && resMaq.exitosos === 0) {
      btn.disabled=false;
      btn.innerHTML='<i class="bi bi-floppy me-1"></i>Guardar y finalizar';
      const detalle = (resMaq.errores || []).slice(0,8).join('\n');
      alert(
        `⚠️ Ningún equipo se pudo guardar (0 de ${resMaq.total}).\n\n` +
        `Detalles:\n${detalle}\n\n` +
        `El cliente quedó creado pero SIN equipos. Corrige los errores ` +
        `y vuelve a la pestaña de equipos antes de finalizar.`
      );
      return;
    }
    if (resMaq && resMaq.errores && resMaq.errores.length > 0) {
      // Algunos sí, algunos no — avisar pero continuar
      const detalle = resMaq.errores.slice(0,8).join('\n');
      alert(
        `Se importaron ${resMaq.exitosos} de ${resMaq.total} equipos.\n\n` +
        `Los siguientes NO se guardaron:\n${detalle}`
      );
    }
    if(!WZ.ctid)await subirContrato();
    if(WZ.ctid&&adjFiles.length){
      for(const f of adjFiles){const fd=new FormData();fd.append('archivo',f);await fetch(`/mantenciones/api/contratos/${WZ.ctid}/adjuntos`,{method:'POST',body:fd});}
    }
    if(WZ.ctid){const e=leerAiEditado();if(e)await fetch(`/mantenciones/api/contratos/${WZ.ctid}/ai-editar`,{method:'PUT',headers:{'Content-Type':'application/json'},body:JSON.stringify(e)});}
    if(WZ.cid&&document.getElementById('cal_generar')?.checked){
      await fetch(`/mantenciones/api/clientes/${WZ.cid}/generar-calendario`,{method:'POST',headers:{'Content-Type':'application/json'},
        body:JSON.stringify({dry_run:false,tipo:document.getElementById('cal_tipo').value,meses:parseInt(document.getElementById('cal_meses').value)||12})});
    }
    // Cliente guardado correctamente → limpiar el draft local
    wzBorrarDraft();
    window.location.href=`/mantenciones/clientes/${WZ.cid}`;
  }catch(e){
    btn.disabled=false;
    btn.innerHTML='<i class="bi bi-floppy me-1"></i>Guardar y finalizar';
    // "Failed to fetch" en JS suele significar: el server tardó demasiado,
    // se reinició el worker, o cayó la conexión. El cliente puede haber
    // quedado parcialmente guardado.
    const isNetworkErr = (e && (e.message === 'Failed to fetch' || e.name === 'TypeError'));
    const msg = isNetworkErr
      ? '⏱️ La operación tardó demasiado y se cortó la conexión.\n\n'
        + 'IMPORTANTE: el cliente probablemente SÍ se guardó parcialmente.\n'
        + 'Antes de reintentar:\n'
        + '  1) Ve a "Clientes" y busca a este cliente — si aparece, abre su ficha.\n'
        + '  2) Si tiene equipos, ya quedó listo (no reintentar).\n'
        + '  3) Si NO tiene equipos o falta el contrato, completa desde la ficha.\n\n'
        + 'Para evitar esto: importa equipos en lotes de 30 o menos, o desmarca '
        + '"Generar visitas automáticamente" antes de finalizar.'
      : 'Error: ' + (e && e.message ? e.message : e);
    if (typeof ilusAlert === 'function') {
      ilusAlert({
        title: isNetworkErr ? 'Timeout al guardar' : 'Error al guardar',
        message: msg,
        type: isNetworkErr ? 'warning' : 'error',
      });
    } else {
      alert(msg);
    }
  }
}

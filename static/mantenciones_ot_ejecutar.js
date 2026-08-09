/* ==================================================================
   mantenciones_ot_ejecutar.js - Ejecucion de OT en terreno (ILUS)
   Extraido TAL CUAL desde templates/mantenciones/ot_ejecutar.html, donde
   vivia en varios bloques <script> inline (~338 KB re-descargados en cada
   clic: el HTML se sirve con cache-control: no-store). Aqui se cachea
   por dias en el navegador del tecnico (celular, terreno, conexion debil).

   IMPORTANTE - datos del servidor (Jinja):
   Este archivo usa variables globales declaradas en un <script> INLINE que
   quedo en el template, ANTES del <script defer src="...ot_ejecutar.js">:
   VID, VISITA_ESTADO, VISITA_TIPO, VISITA_LEVANTAMIENTO_ID,
   ES_LEVANTAMIENTO, LEV_EDITABLE, DESTINO_LAT, DESTINO_LNG, DESTINO_DIR,
   RUTA_YA_INICIADA, EQUIPOS, EQUIPOS_IDX, PLANTILLAS_POR_MAQUINA,
   STATS_POR_MAQUINA, EQUIPOS_ESTADO_REVISION (let), PUEDE_EJECUTAR_FLAG,
   CURRENT_USER_ID, IS_ADMIN_LOCK, TAREAS_LOCKS (let), _CLOUD_TX_PROFILES,
   REGLAS_TERRENO, GEOF_PUEDE_EJECUTAR.
   NO se renombraron a un objeto tipo window.OT_DATA porque esas const/let
   son referenciadas cientos de veces en este archivo; reescribir cada
   referencia era el tipo de cambio de alto riesgo que esta tarea pidio
   evitar. En su lugar se aprovecha que TODOS los <script> de un mismo
   documento comparten el mismo scope global lexico: el <script> inline
   (sincrono) se ejecuta durante el parseo, ANTES de que cualquier
   <script defer> corra (los defer siempre esperan a que el parseo del
   documento completo termine) - por eso estas const/let ya existen
   cuando este archivo se ejecuta, sin importar en que posicion del
   documento quedo el <script defer src=...>.

   El bloque de "Finanzas de la OT" (margen/costo proveedor, con carga
   condicionada al permiso puede_metadata en el template) NO se movio
   aqui a proposito: es chico (~3 KB) y mover su condicional de carga
   agregaba riesgo/complejidad
   desproporcionado al ahorro. Sigue inline en el template.
================================================================== */

// ════════════════════════════════════════════════════════
//  PERF 2026-05-22 — Helpers Cloudinary + compresión
//  (Daniel: módulo de mantenciones maneja MUCHAS fotos)
// ════════════════════════════════════════════════════════

// Perfiles equivalentes al backend (_CLOUD_TX_PROFILES en app.py).
const _CLOUD_TX_PROFILES = {
  thumb:            'f_auto,q_auto:eco,w_120,c_limit,dpr_auto',
  card:             'f_auto,q_auto,w_400,c_limit,dpr_auto',
  gallery:          'f_auto,q_auto,w_800,c_limit,dpr_auto',
  full:             'f_auto,q_auto',
  blur_placeholder: 'f_auto,q_30,w_40,e_blur:200,c_limit'
};
const _CLOUD_TX_RE = /^(https?:\/\/res\.cloudinary\.com\/[^/]+\/(?:image|video|raw)\/upload\/)(.+)$/i;
const _CLOUD_TX_TOKEN_RE = /(?:^|,)[a-z]_[^,/]+/;

/**
 * Aplica transformaciones Cloudinary inline a una URL.
 * - URLs no-Cloudinary: pasan sin tocar.
 * - URLs con transformaciones existentes: se preservan (no se duplican).
 * @param {string} url
 * @param {'thumb'|'card'|'gallery'|'full'|'blur_placeholder'} kind
 * @returns {string}
 */
function cloudTx(url, kind){
  if (!url || typeof url !== 'string') return url || '';
  const m = url.match(_CLOUD_TX_RE);
  if (!m) return url;
  const prefix = m[1];
  const rest = m[2];
  const tx = _CLOUD_TX_PROFILES[kind] || _CLOUD_TX_PROFILES.card;
  // Detectar transforms ya presentes en el primer segmento.
  const firstSeg = rest.split('/')[0];
  if (firstSeg && _CLOUD_TX_TOKEN_RE.test(',' + firstSeg)) return url;
  return prefix + tx + '/' + rest;
}

/**
 * Comprime una imagen en cliente antes de subirla a Cloudinary.
 * - Si pesa <500KB o ya es WebP/AVIF/HEIC: skip.
 * - Si no: redibuja a max 1920px de ancho, JPG quality=0.85.
 * - Reduce ~70-90% el peso (iPhone 4032×3024 ≈ 4.5MB → ~400KB).
 *
 * @param {File} file  archivo input (image/*)
 * @returns {Promise<Blob>} blob listo para FormData
 */
async function _compressImageBeforeUpload(file){
  if (!file || !file.type || !file.type.startsWith('image/')) return file;
  // Skip si pesa poco — ya está aceptable.
  if (file.size && file.size < 500 * 1024) return file;
  // Skip si ya es WebP/AVIF/HEIC (formatos modernos ya optimizados).
  const t = (file.type || '').toLowerCase();
  if (t.includes('webp') || t.includes('avif') || t.includes('heic') || t.includes('heif')){
    return file;
  }
  try {
    const bitmap = await (window.createImageBitmap
      ? createImageBitmap(file).catch(() => null)
      : Promise.resolve(null));
    const w0 = bitmap ? bitmap.width  : 0;
    const h0 = bitmap ? bitmap.height : 0;
    if (!w0 || !h0){
      // createImageBitmap no soportado → fallback con <img>.
      return await _compressImageViaImgFallback(file);
    }
    const MAX_W = 1920;
    let w = w0, h = h0;
    if (w > MAX_W){
      h = Math.round(h * (MAX_W / w));
      w = MAX_W;
    }
    const canvas = document.createElement('canvas');
    canvas.width = w; canvas.height = h;
    const ctx = canvas.getContext('2d');
    ctx.drawImage(bitmap, 0, 0, w, h);
    if (bitmap.close) bitmap.close();
    const blob = await new Promise(res => canvas.toBlob(res, 'image/jpeg', 0.85));
    if (!blob) return file;
    // Si el resultado es MAYOR que el original (caso raro: PNG pequeño con
    // mucha transparencia que JPG no comprime bien), devolvemos el original.
    if (blob.size >= file.size) return file;
    // Anotamos un name para que el backend tenga un filename razonable.
    try {
      const newName = (file.name || 'foto').replace(/\.[^.]+$/, '') + '.jpg';
      return new File([blob], newName, { type: 'image/jpeg', lastModified: Date.now() });
    } catch(_e){
      return blob;
    }
  } catch(err){
    console.warn('[compress] fallback (error)', err);
    return file;
  }
}

// Fallback para navegadores muy viejos sin createImageBitmap (rare).
async function _compressImageViaImgFallback(file){
  return new Promise((resolve) => {
    const reader = new FileReader();
    reader.onload = () => {
      const img = new Image();
      img.onload = () => {
        const MAX_W = 1920;
        let w = img.width, h = img.height;
        if (w > MAX_W){ h = Math.round(h * (MAX_W / w)); w = MAX_W; }
        const canvas = document.createElement('canvas');
        canvas.width = w; canvas.height = h;
        canvas.getContext('2d').drawImage(img, 0, 0, w, h);
        canvas.toBlob(b => resolve(b || file), 'image/jpeg', 0.85);
      };
      img.onerror = () => resolve(file);
      img.src = reader.result;
    };
    reader.onerror = () => resolve(file);
    reader.readAsDataURL(file);
  });
}

// Estado de navegación
let _state = { mid: null, pid: null, view: 'maquinas' };

// ════════════════════════════════════════════════════════
//  NAVEGACIÓN DRILL-DOWN
//  IMPORTANTE: las keys de EQUIPOS_IDX/PLANTILLAS_POR_MAQUINA/STATS_POR_MAQUINA
//  son STRINGS (JSON estándar). Cualquier `mid` o `pid` que llegue de un
//  onclick lo casteamos a String() defensivamente para evitar undefined.
// ════════════════════════════════════════════════════════
function setView(v){
  document.querySelectorAll('.tx-view').forEach(el => el.classList.remove('active'));
  document.getElementById('view-' + v).classList.add('active');
  _state.view = v;
  window.scrollTo({ top: 0, behavior: 'smooth' });
}

function goToMaquinas(){
  _state = { mid: null, pid: null, view: 'maquinas' };
  setView('maquinas');
  renderCrumbs();
}

function goToPlantillas(mid){
  mid = String(mid);
  _state.mid = mid;
  _state.pid = null;
  setView('plantillas');
  renderPlantillas(mid);
  renderCrumbs();
}

function goToTareas(mid, pid){
  mid = String(mid); pid = String(pid);
  _state.mid = mid;
  _state.pid = pid;
  setView('tareas');
  renderTareas(mid, pid);
  renderCrumbs();
}

function renderCrumbs(){
  const cont = document.getElementById('crumbs');
  let html = `<button onclick="goToMaquinas()"><i class="bi bi-grid-3x3-gap-fill"></i> Máquinas</button>`;
  if (_state.mid){
    const eq = EQUIPOS_IDX[_state.mid];
    const nom = eq ? (eq.nombre || ('Equipo #' + _state.mid)) : 'Máquina';
    if (_state.view === 'plantillas'){
      html += `<span class="sep">›</span><span class="curr">${_escapeHtml(nom)}</span>`;
    } else {
      html += `<span class="sep">›</span><button onclick="goToPlantillas(${_state.mid})">${_escapeHtml(nom)}</button>`;
      if (_state.pid !== null){
        const grupo = (PLANTILLAS_POR_MAQUINA[_state.mid] || []).find(p => String(p.plantilla_id) === String(_state.pid));
        const nomPl = grupo ? grupo.plantilla_nombre : 'Plantilla';
        html += `<span class="sep">›</span><span class="curr">${_escapeHtml(nomPl)}</span>`;
      }
    }
  }
  cont.innerHTML = html;
}

// ════════════════════════════════════════════════════════
//  VISTA 2: render PLANTILLAS de una máquina
// ════════════════════════════════════════════════════════
function renderPlantillas(mid){
  const eq = EQUIPOS_IDX[mid];
  const pls = PLANTILLAS_POR_MAQUINA[mid] || [];
  if (!eq) return;
  // ── 2026-05-19 (Daniel) — En OT tipo levantamiento, mostramos un
  //    banner que avisa al técnico que sus fotos van a la ficha técnica.
  // ── 2026-05-20 (Daniel) — El botón "Ver ficha técnica" se movió a la
  //    tarjeta de cada equipo en la vista 1 (lista). El header de la vista
  //    2 ya no lo lleva para evitar duplicación.
  const levBannerHtml = ES_LEVANTAMIENTO
    ? `<div class="lev-banner-ficha" role="status" aria-live="polite">
         <i class="bi bi-camera-fill"></i>
         <div>
           <strong>Levantamiento de ficha</strong>
           <div>Las fotos y datos que captures aquí van a formar parte de la
             <strong>ficha técnica permanente</strong> del equipo
             ${_escapeHtml(eq.nombre || ('#' + eq.id))}.</div>
         </div>
       </div>`
    : '';
  // Hero
  document.getElementById('plHero').innerHTML = `
    <div class="h-foto" ${eq.foto_url ? `style="background-image:url('${_escapeAttr(eq.foto_url)}')"` : ''}>
      ${!eq.foto_url ? '<i class="bi bi-image"></i>' : ''}
    </div>
    <div class="h-info">
      <div class="h-name">${_escapeHtml(eq.nombre || ('Equipo #' + eq.id))}</div>
      <div class="h-meta">
        ${eq.sku ? `<i class="bi bi-tag"></i> ${_escapeHtml(eq.sku)} · ` : ''}
        ${eq.serie ? `S/N: ${_escapeHtml(eq.serie)}` : ''}
        ${eq.marca || eq.modelo ? `<br><i class="bi bi-tools"></i> ${_escapeHtml(eq.marca || '')} ${_escapeHtml(eq.modelo || '')}` : ''}
        ${eq.ubicacion_sala ? `<br><i class="bi bi-geo-alt"></i> ${_escapeHtml(eq.ubicacion_sala)}` : ''}
      </div>
    </div>
    ${levBannerHtml}`;

  // ── 2026-05-21 (Daniel) — Acciones rápidas + observación ────────────
  // Permiten saltar / marcar falla / dejar nota sin tener que completar
  // todas las tareas. El bloqueo (OT cerrada / no puede ejecutar) está
  // contemplado en _renderAccionesRapidasYObs.
  const accionesYObsHtml = _renderAccionesRapidasYObs(mid);

  // Lista de plantillas
  const cont = document.getElementById('plList');
  let plantillasHtml = '';
  if (!pls.length){
    plantillasHtml = '<div class="tx-empty"><i class="bi bi-list-check"></i><div class="fw-bold">Sin plantillas</div></div>';
  } else {
    plantillasHtml = pls.map(p => {
      const done = p.completas === p.total && p.total > 0;
      return `<div class="tx-pl-card${done ? ' done' : ''}" onclick="goToTareas(${mid}, ${p.plantilla_id})">
        <div class="pl-icon"><i class="bi bi-${done ? 'check-circle-fill' : 'list-check'}"></i></div>
        <div class="pl-info">
          <div class="pl-name">${_escapeHtml(p.plantilla_nombre)}</div>
          <div class="pl-meta">
            <span class="badge">${_escapeHtml(p.plantilla_tipo)}</span>
            <span>${p.completas}/${p.total} tareas</span>
            ${done ? '<span style="color:#16a34a;font-weight:700">✓ COMPLETA</span>' : ''}
          </div>
          <div class="pl-prog"><div style="width:${p.progreso}%"></div></div>
        </div>
        <i class="bi bi-chevron-right text-muted"></i>
      </div>`;
    }).join('');
  }
  cont.innerHTML = accionesYObsHtml + plantillasHtml;
}

// ════════════════════════════════════════════════════════
// 2026-05-21 (Daniel) — Acciones rápidas + observación por equipo
// Se inyectan al tope de la vista de plantillas, siempre visibles
// (no ocultas en submenú). Permiten al técnico:
//   - Saltar este equipo (con razón + observación)
//   - Marcar falla detectada (observación obligatoria)
//   - Marcar verificado (vuelve al default)
//   - Dejar nota libre (auto-save al blur)
// ════════════════════════════════════════════════════════
function _renderAccionesRapidasYObs(mid){
  const readonly = (VISITA_ESTADO === 'cerrada' ||
                    VISITA_ESTADO === 'pendiente_aprobacion' ||
                    !PUEDE_EJECUTAR_FLAG);
  const midStr = String(mid);
  const rev = EQUIPOS_ESTADO_REVISION[midStr] || {};
  const estado = rev.estado_revision || '';
  const obs = rev.observacion_tecnico || '';
  const revAt = rev.revisado_at ? `<span class="text-muted">· ${_escapeHtml(rev.revisado_at)}</span>` : '';
  const revBy = rev.revisado_por ? `por ${_escapeHtml(rev.revisado_por)}` : '';

  let badgeHtml = '';
  if (estado === 'saltado'){
    badgeHtml = `<span class="eqrev-badge eqrev-saltado"><i class="bi bi-skip-forward-fill"></i>Saltado${rev.razon_saltado ? ' · ' + _escapeHtml(rev.razon_saltado.replace(/_/g,' ')) : ''}</span>`;
  } else if (estado === 'falla_detectada'){
    badgeHtml = `<span class="eqrev-badge eqrev-falla_detectada"><i class="bi bi-exclamation-triangle-fill"></i>Falla detectada</span>`;
  } else if (estado === 'con_cambios'){
    badgeHtml = `<span class="eqrev-badge eqrev-con_cambios"><i class="bi bi-pencil-fill"></i>Con cambios</span>`;
  } else if (estado === 'verificado'){
    badgeHtml = `<span class="eqrev-badge eqrev-verificado"><i class="bi bi-check-circle-fill"></i>Verificado</span>`;
  }

  const accionesHtml = readonly ? '' : `
    <div class="eq-acciones-rapidas">
      <button type="button" class="eq-accion-btn warn"
              onclick="abrirModalSaltarEquipo(${mid})">
        <i class="bi bi-skip-forward-fill"></i>Saltar equipo
      </button>
      <button type="button" class="eq-accion-btn danger"
              onclick="abrirModalFallaEquipo(${mid})">
        <i class="bi bi-exclamation-triangle-fill"></i>Reportar falla
      </button>
      ${estado && estado !== 'verificado' ? `
        <button type="button" class="eq-accion-btn success"
                onclick="marcarEquipoEstado(${mid}, 'verificado')">
          <i class="bi bi-check-circle-fill"></i>Marcar verificado
        </button>` : ''}
    </div>
  `;

  return `
    <div class="d-flex align-items-center gap-2 mb-1" style="flex-wrap:wrap">
      ${badgeHtml}
      ${estado ? `<small class="text-muted" style="font-size:.7rem">${revBy} ${revAt}</small>` : ''}
    </div>
    ${accionesHtml}
    <div class="tx-obs-rapida">
      <label for="obs-rapida-${mid}">
        <i class="bi bi-chat-left-text-fill"></i>
        Observación rápida sobre este equipo
      </label>
      <textarea id="obs-rapida-${mid}"
        placeholder="¿Algo que quieras dejar registrado de este equipo? (limpieza, ruido raro, sugerencia, lo que sea)"
        ${readonly ? 'readonly' : ''}
        maxlength="2000"
        onblur="guardarObsRapida(${mid}, this.value)"
      >${_escapeHtml(obs)}</textarea>
      <div class="tx-obs-status" id="obs-rapida-status-${mid}"></div>
    </div>
  `;
}

// Guarda observación libre (sin cambiar estado) auto-save al blur.
async function guardarObsRapida(mid, valor){
  const statusEl = document.getElementById(`obs-rapida-status-${mid}`);
  const _setStatus = (cls, text) => {
    if (statusEl){
      statusEl.className = 'tx-obs-status ' + cls;
      statusEl.innerHTML = text;
    }
  };
  const prev = (EQUIPOS_ESTADO_REVISION[String(mid)] || {}).observacion_tecnico || '';
  if ((valor || '').trim() === prev.trim()) {
    return; // sin cambios
  }
  _setStatus('saving', '<i class="bi bi-arrow-clockwise"></i> Guardando…');
  try {
    const r = await fetch(`/mantenciones/api/visitas/${VID}/equipos/${mid}/observacion`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({observacion: valor || ''})
    });
    const d = await r.json();
    if (!d.ok){ throw new Error(d.error || 'No se pudo guardar'); }
    // Actualizar caché local
    EQUIPOS_ESTADO_REVISION[String(mid)] = {
      ...(EQUIPOS_ESTADO_REVISION[String(mid)] || {}),
      observacion_tecnico: d.observacion || '',
      revisado_por: d.revisado_por || '',
      revisado_at: (new Date()).toISOString().slice(0,19).replace('T',' '),
    };
    _setStatus('saved', '<i class="bi bi-check-circle-fill"></i> Guardado');
    // Auto-clear status tras 2.5s
    setTimeout(() => { if (statusEl) statusEl.innerHTML = ''; }, 2500);
  } catch (e){
    _setStatus('error', `<i class="bi bi-x-circle-fill"></i> ${_escapeHtml(e.message || 'Error')}`);
  }
}

// Cambia el badge visual de un equipo en la lista de máquinas (vista 1).
function _refreshEqCardBadge(mid){
  const rev = EQUIPOS_ESTADO_REVISION[String(mid)] || {};
  const estado = rev.estado_revision || '';
  const card = document.getElementById(`tx-eq-card-${mid}`);
  const badge = document.getElementById(`eqrev-badge-${mid}`);
  if (card){ card.setAttribute('data-rev', estado || ''); }
  if (badge){
    if (!estado){
      badge.style.display = 'none';
      return;
    }
    badge.style.display = '';
    badge.className = `eqrev-badge eqrev-${estado}`;
    const label = {
      verificado:       '<i class="bi bi-check-circle-fill"></i>Verificado',
      con_cambios:      '<i class="bi bi-pencil-fill"></i>Con cambios',
      saltado:          '<i class="bi bi-skip-forward-fill"></i>Saltado',
      falla_detectada:  '<i class="bi bi-exclamation-triangle-fill"></i>Falla',
    }[estado] || estado;
    badge.innerHTML = label;
  }
}

// Marca el equipo en un estado simple (sin modal). Útil para "volver a
// verificado" o flujos sin observación obligatoria.
async function marcarEquipoEstado(mid, estado, extra){
  extra = extra || {};
  try {
    const r = await fetch(`/mantenciones/api/visitas/${VID}/equipos/${mid}/marcar`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({estado_revision: estado, ...extra})
    });
    const d = await r.json();
    if (!d.ok){ throw new Error(d.error || 'No se pudo marcar'); }
    EQUIPOS_ESTADO_REVISION[String(mid)] = {
      estado_revision: d.estado_revision,
      razon_saltado:   d.razon || '',
      observacion_tecnico: d.observacion || '',
      revisado_at: (new Date()).toISOString().slice(0,19).replace('T',' '),
      revisado_por: d.revisado_por || '',
    };
    _refreshEqCardBadge(mid);
    if (_state.view === 'plantillas' && String(_state.mid) === String(mid)){
      renderPlantillas(mid);
    }
    // Recalcular lock del botón firmar — saltar / falla excluyen las tareas
    // del equipo, así que pueden destrabar el firmar.
    try { actualizarLockFirmar(_calcCtxGlobal()); } catch(_) {}
    if (typeof ilusToast === 'function'){
      const _msg = {
        verificado:       '✓ Marcado como verificado',
        con_cambios:      'Marcado con cambios',
        saltado:          '⏭️ Equipo saltado',
        falla_detectada:  '🚨 Falla detectada — registrada',
      }[estado] || 'Estado actualizado';
      ilusToast(_msg, {type: estado === 'falla_detectada' ? 'danger' : 'success'});
    }
  } catch (e){
    if (typeof ilusAlert === 'function'){
      ilusAlert({title:'Error', message: e.message || 'No se pudo guardar', type:'error'});
    } else {
      console.error(e);
    }
  }
}

// Modal "Saltar equipo" — pide razón (radios) + observación obligatoria.
let _modalSaltarEquipo = null;
let _saltarEquipoMid = null;
function abrirModalSaltarEquipo(mid){
  _saltarEquipoMid = mid;
  if (!_modalSaltarEquipo){
    _modalSaltarEquipo = new bootstrap.Modal(document.getElementById('modalSaltarEquipo'));
  }
  // Reset
  document.querySelectorAll('#modalSaltarEquipo input[name="skipRazon"]').forEach(r => r.checked = false);
  const txt = document.getElementById('skipObservacion');
  if (txt){ txt.value = ''; }
  // Header con nombre del equipo
  const eq = EQUIPOS_IDX[String(mid)] || {};
  const nameEl = document.getElementById('skipEqNombre');
  if (nameEl) nameEl.textContent = eq.nombre || `Equipo #${mid}`;
  _modalSaltarEquipo.show();
}

async function confirmarSaltarEquipo(){
  const mid = _saltarEquipoMid;
  if (!mid){ return; }
  const razonInput = document.querySelector('#modalSaltarEquipo input[name="skipRazon"]:checked');
  const obs = (document.getElementById('skipObservacion').value || '').trim();
  if (!razonInput){
    if (typeof ilusToast === 'function'){
      ilusToast('Selecciona una razón para saltar', {type:'warning'});
    }
    return;
  }
  if (obs.length < 5){
    if (typeof ilusToast === 'function'){
      ilusToast('Escribe al menos 5 caracteres de observación', {type:'warning'});
    }
    document.getElementById('skipObservacion').focus();
    return;
  }
  const razon = razonInput.value;
  // Confirmación final con ilusConfirm (regla ILUS #1)
  if (typeof ilusConfirm === 'function'){
    const ok = await ilusConfirm({
      title: 'Confirmar saltado',
      message: `¿Saltar este equipo y registrar la razón?`,
      sub: 'La OT NO se detendrá, pero quedará registrado quién lo saltó y por qué.',
      okLabel: 'Sí, saltar',
      cancelLabel: 'Cancelar',
      danger: false,
    });
    if (!ok) return;
  }
  try {
    const r = await fetch(`/mantenciones/api/visitas/${VID}/equipos/${mid}/saltar`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({razon, observacion: obs})
    });
    const d = await r.json();
    if (!d.ok){ throw new Error(d.error || 'No se pudo saltar'); }
    EQUIPOS_ESTADO_REVISION[String(mid)] = {
      estado_revision: 'saltado',
      razon_saltado: d.razon || razon,
      observacion_tecnico: d.observacion || obs,
      revisado_at: (new Date()).toISOString().slice(0,19).replace('T',' '),
      revisado_por: d.revisado_por || '',
    };
    _refreshEqCardBadge(mid);
    if (_state.view === 'plantillas' && String(_state.mid) === String(mid)){
      renderPlantillas(mid);
    }
    try { actualizarLockFirmar(_calcCtxGlobal()); } catch(_) {}
    if (_modalSaltarEquipo){ _modalSaltarEquipo.hide(); }
    if (typeof ilusToast === 'function'){
      ilusToast('⏭️ Equipo saltado — la OT continúa', {type:'success'});
    }
  } catch (e){
    if (typeof ilusAlert === 'function'){
      ilusAlert({title:'Error', message: e.message || 'No se pudo guardar', type:'error'});
    }
  }
}

// Modal "Reportar falla" — pide observación detallada (mín 5 chars).
async function abrirModalFallaEquipo(mid){
  if (typeof ilusPrompt !== 'function'){
    console.error('ilusPrompt no disponible');
    return;
  }
  const eq = EQUIPOS_IDX[String(mid)] || {};
  const nombreEq = eq.nombre || ('Equipo #'+mid);
  const desc = await ilusPrompt({
    title: 'Reportar falla — ' + nombreEq,
    message: 'Describe la falla detectada en este equipo:',
    sub: 'La OT no se detendrá. La falla queda registrada en el historial del equipo.',
    placeholder: 'Ej: ruido en motor, banda desgastada, display no enciende…',
    required: true,
    multiline: true,
  });
  if (!desc || desc.trim().length < 5){
    if (typeof ilusToast === 'function'){
      ilusToast('La descripción debe tener al menos 5 caracteres', {type:'warning'});
    }
    return;
  }
  await marcarEquipoEstado(mid, 'falla_detectada', {observacion: desc.trim()});
}

// ════════════════════════════════════════════════════════
//  VISTA 3: render TAREAS de una plantilla
// ════════════════════════════════════════════════════════
function renderTareas(mid, pid){
  const eq = EQUIPOS_IDX[mid];
  const grupo = (PLANTILLAS_POR_MAQUINA[mid] || []).find(p => String(p.plantilla_id) === String(pid));
  if (!grupo) return;

  document.getElementById('tareasHero').innerHTML = `
    <h4><i class="bi bi-list-check me-2"></i>${_escapeHtml(grupo.plantilla_nombre)}</h4>
    <div class="sub">${_escapeHtml(eq.nombre || ('Equipo #' + mid))} · <span id="tProg">${grupo.completas}</span>/${grupo.total} tareas</div>
    <div class="prog-wrap"><div class="prog-bar" id="tProgBar" style="width:${grupo.progreso}%"></div></div>`;

  const cont = document.getElementById('tareasList');
  const bloqueada = (grupo.bloqueado || VISITA_ESTADO === 'pendiente_aprobacion' || VISITA_ESTADO === 'cerrada' || !PUEDE_EJECUTAR_FLAG);
  cont.innerHTML = grupo.tareas.map(t => renderTareaHtml(t, bloqueada, mid, pid)).join('');
}

function renderTareaHtml(t, bloqueada, mid, pid){
  const done = !!t.completada;
  // ── FIX 2026-05-17 (GPS DEFINITIVO) ───────────────────────────────
  // Normalizar `tipo_respuesta`: a veces llega 'GPS' (mayúsculas),
  // 'Gps', con espacios, o NULL (tareas heredadas de plantillas viejas
  // sin migrar). Esto rompía el switch y nadie podía declarar ubicación.
  // ──────────────────────────────────────────────────────────────────
  const tipoRaw = (t.tipo_respuesta || 'check');
  const tipo = String(tipoRaw).trim().toLowerCase() || 'check';
  let valor = {};
  try { valor = t.valor_json ? (typeof t.valor_json === 'string' ? JSON.parse(t.valor_json) : t.valor_json) : {}; } catch(e){ valor = {}; }

  let ctrlHtml = '';
  switch(tipo){
    case 'check':
      ctrlHtml = ''; // El check ya está arriba; toggle al click
      break;
    case 'texto': {
      // 2026-05-18 (Mejora UX): contador de caracteres + auto-completar al blur
      // si supera el umbral. Mínimo 3 chars para opcionales, 10 para obligatorias.
      // El usuario ve en vivo cuántas letras le faltan ANTES de salir del campo.
      const _minChars = t.obligatoria ? 10 : 3;
      const _curTxt = (valor && valor.texto) ? String(valor.texto) : '';
      const _curLen = _curTxt.trim().length;
      // ── 2026-05-19 (Daniel) — Detectar inputs que piden serial/N° serie.
      //    Si la OT es levantamiento Y el título sugiere serial, mostramos
      //    un banner azul con "Sugerir serial registrado" que precarga el
      //    valor actual de mant_maquinas.serie (al guardar pisa la ficha).
      const _esSerialInput = _esCampoSerial(t.titulo || '');
      let _serialBannerHtml = '';
      if (ES_LEVANTAMIENTO && _esSerialInput && mid){
        const eq = EQUIPOS_IDX[String(mid)] || {};
        const serieReg = (eq.serie || '').trim();
        _serialBannerHtml = `<div class="tx-serial-aviso">
          <i class="bi bi-info-circle-fill"></i>
          <div class="meta">
            <strong>Serial actual en ficha:</strong>
            ${serieReg
              ? `<code style="background:#fff;padding:1px 5px;border-radius:4px;border:1px solid #bfdbfe">${_escapeHtml(serieReg)}</code>`
              : '<em style="color:#6b7280">sin registrar</em>'}
            <div style="font-size:.69rem;color:#475569;margin-top:2px">
              Si lo escribes igual, se mantiene. Si lo cambias, se actualiza la ficha al cerrar la OT.
            </div>
          </div>
          ${serieReg ? `<button type="button" class="btn-sugerir-serial"
            ${bloqueada ? 'disabled' : ''}
            data-serial="${_escapeAttr(serieReg)}"
            onclick="sugerirSerialRegistrado(${t.id}, this.dataset.serial, ${mid}, ${pid})">
            <i class="bi bi-magic"></i>Usar este serial
          </button>` : ''}
        </div>`;
      }
      // ── 2026-06-12 (Daniel) — Campo MARCA: combobox con el catálogo de
      //    marcas/familias (seed + casa + ERP Random vía /api/marcas).
      //    Texto libre permitido (el ERP no tiene TODAS las marcas);
      //    al blur se normaliza a la forma canónica y se guarda normal.
      const _esMarcaInput = _esCampoMarca(t.titulo || '');
      if (_esMarcaInput){ _chkCargarMarcas(); } // prefetch (idempotente, cacheado)
      const _marcaAttrs = _esMarcaInput
        ? `list="chkMarcasDL" autocomplete="off" onfocus="_chkCargarMarcas()"`
        : '';
      const _placeholderTxt = _esMarcaInput
        ? 'Ej: ILUS, Gymleco, Freemotion…'
        : 'Escribe tu respuesta…';
      ctrlHtml = `<div class="ctrl">
        ${_serialBannerHtml}
        <input type="text" id="texto-input-${t.id}" placeholder="${_placeholderTxt}"
          value="${_escapeAttr(_curTxt)}"
          ${_marcaAttrs}
          ${bloqueada ? 'disabled' : ''}
          oninput="_textoCharFeedback(this, ${t.id}, ${_minChars})"
          onblur="${_esMarcaInput ? '_chkNormalizarMarca(this); ' : ''}_textoOnBlur(this, ${t.id}, ${mid}, ${pid}, ${_minChars})">
        <div class="tx-texto-feedback" id="texto-fb-${t.id}"
             style="font-size:.72rem;margin-top:4px;font-weight:600;
                    display:flex;align-items:center;gap:6px">
          ${_textoFeedbackHtml(_curLen, _minChars)}
        </div>
      </div>`;
      break;
    }
    case 'numero':
      ctrlHtml = `<div class="ctrl d-flex gap-2 align-items-center">
        <input type="number" style="flex:1" placeholder="0"
          value="${valor.numero !== undefined && valor.numero !== null ? valor.numero : ''}"
          ${t.rango_min != null ? 'min="' + t.rango_min + '"' : ''}
          ${t.rango_max != null ? 'max="' + t.rango_max + '"' : ''}
          ${bloqueada ? 'disabled' : ''}
          onblur="guardarResp(${t.id}, parseFloat(this.value), ${mid}, ${pid})">
        ${t.unidad ? `<span class="text-muted small fw-bold">${_escapeHtml(t.unidad)}</span>` : ''}
      </div>
      ${t.rango_min != null || t.rango_max != null ? `<small class="text-muted">Rango: ${t.rango_min ?? '—'} a ${t.rango_max ?? '—'} ${t.unidad || ''}</small>` : ''}`;
      break;
    case 'sino':
      ctrlHtml = `<div class="ctrl tx-btn-group">
        <button type="button" class="tx-btn-pill success ${valor.valor === 'si' ? 'active' : ''}"
          ${bloqueada ? 'disabled' : ''} onclick="guardarResp(${t.id}, 'si', ${mid}, ${pid}); refreshPills(this, 'sino')">Sí</button>
        <button type="button" class="tx-btn-pill danger ${valor.valor === 'no' ? 'active' : ''}"
          ${bloqueada ? 'disabled' : ''} onclick="guardarResp(${t.id}, 'no', ${mid}, ${pid}); refreshPills(this, 'sino')">No</button>
        <button type="button" class="tx-btn-pill ${valor.valor === 'na' ? 'active' : ''}"
          ${bloqueada ? 'disabled' : ''} onclick="guardarResp(${t.id}, 'na', ${mid}, ${pid}); refreshPills(this, 'sino')">N/A</button>
      </div>`;
      break;
    case 'verificacion':
      ctrlHtml = `<div class="ctrl tx-btn-group">
        <button type="button" class="tx-btn-pill success ${valor.valor === 'aprobado' ? 'active' : ''}"
          ${bloqueada ? 'disabled' : ''} onclick="guardarResp(${t.id}, 'aprobado', ${mid}, ${pid}); refreshPills(this, 'verif')">🟢 Aprobado</button>
        <button type="button" class="tx-btn-pill warn ${valor.valor === 'alerta' ? 'active' : ''}"
          ${bloqueada ? 'disabled' : ''} onclick="guardarResp(${t.id}, 'alerta', ${mid}, ${pid}); refreshPills(this, 'verif')">🟡 Alerta</button>
        <button type="button" class="tx-btn-pill danger ${valor.valor === 'falla' ? 'active' : ''}"
          ${bloqueada ? 'disabled' : ''} onclick="guardarResp(${t.id}, 'falla', ${mid}, ${pid}); refreshPills(this, 'verif')">🔴 Falla</button>
      </div>`;
      break;
    case 'lista':
      let opts = [];
      try { opts = t.opciones_lista_json ? JSON.parse(t.opciones_lista_json) : []; } catch(e){ opts = []; }
      ctrlHtml = `<div class="ctrl"><select ${bloqueada ? 'disabled' : ''}
        onchange="guardarResp(${t.id}, this.value, ${mid}, ${pid})">
        <option value="">— Seleccionar —</option>
        ${opts.map(o => `<option value="${_escapeAttr(o)}" ${valor.opcion === o ? 'selected' : ''}>${_escapeHtml(o)}</option>`).join('')}
      </select></div>`;
      break;
    case 'fecha_hora':
      ctrlHtml = `<div class="ctrl"><input type="datetime-local"
        value="${_escapeAttr(valor.fecha || '')}"
        ${bloqueada ? 'disabled' : ''}
        onblur="guardarResp(${t.id}, this.value, ${mid}, ${pid})"></div>`;
      break;
    case 'gps':
      // FIX 2026-05-17 — Render GPS bulletproof:
      //   1. Guard de toFixed (lat/lng pueden venir null o string → crash que rompía
      //      el switch entero y dejaba al técnico sin ningún control visible).
      //   2. Botón MUY prominente con label claro + 2 alternativas siempre visibles.
      //   3. Si ya se capturó, mostramos cómo (GPS/IP/manual) + dirección si la hay.
      ctrlHtml = `<div class="ctrl">
        <div style="display:flex;flex-direction:column;gap:8px">
          <button type="button" class="tx-btn-gps"
            style="min-height:54px;font-size:.95rem;justify-content:center;font-weight:800"
            ${bloqueada ? 'disabled' : ''}
            onclick="capturarGPS(${t.id}, ${mid}, ${pid})">
            <i class="bi bi-geo-alt-fill"></i> Capturar mi ubicación (GPS)
          </button>
          {# ── POLÍTICA 2026-05-17 (Daniel) ──────────────────────────
             Solo se acepta GPS real del dispositivo. Los botones
             "Usar IP" y "Escribir dirección" fueron ELIMINADOS porque
             vulneran la auditoría — el técnico podría falsear su
             posición. Si el GPS está denegado, el flujo enviá al
             usuario a Ajustes (mostrarAyudaGPS) para habilitar permiso.
             ────────────────────────────────────────────────────────── #}
          <div style="background:#fef3c7;border:1px dashed #f59e0b;
            border-radius:8px;padding:7px 10px;font-size:.7rem;color:#92400e;
            display:flex;align-items:center;gap:6px">
            <i class="bi bi-shield-check" style="color:#a16207"></i>
            Solo se acepta GPS real del dispositivo para validar tu posición.
          </div>
        </div>
        ${_gpsValorRenderResultado(valor)}
      </div>`;
      break;
    case 'foto': {
      // ── FIX 2026-05-17 v2 — UN solo botón rojo grande + sheet de elección ──
      // Mobile: 1 botón "Agregar foto" → abre bottom-sheet con 2 opciones
      // (Cámara / Galería). El técnico decide ahí (ilusActionSheet).
      // Desktop: zona con drag&drop real (igual que antes).
      // Los 2 <input type=file> quedan ocultos y se disparan via click()
      // según lo que elija el técnico en el sheet.
      // ────────────────────────────────────────────────────────────
      // ── 2026-05-19 (Daniel) — Aviso visual: foto va a la ficha técnica.
      //    Solo se muestra en OTs tipo levantamiento, justo arriba del botón
      //    para que el técnico entienda que NO es solo evidencia de la OT.
      const _fotoAvisoHtml = (ES_LEVANTAMIENTO && mid)
        ? `<div class="tx-foto-aviso-ficha" role="status">
            <i class="bi bi-camera-fill"></i>
            <div>Esta foto se asignará a la <strong>ficha técnica del equipo</strong>
              al cerrar la OT.</div>
          </div>`
        : '';
      ctrlHtml = `<div class="ctrl">
        ${_fotoAvisoHtml}
        {# Desktop: zona con drag&drop real #}
        <label class="tx-btn-foto-zone d-none d-md-flex" id="dropzone-${t.id}"
          style="cursor:pointer;flex-direction:column;align-items:center;gap:6px;padding:18px;
          border:2px dashed #cbd5e1;border-radius:11px;background:#fafafa;
          transition:all .15s"
          ondragover="event.preventDefault();this.style.borderColor='#dc2626';this.style.background='#fef2f2'"
          ondragleave="this.style.borderColor='#cbd5e1';this.style.background='#fafafa'"
          ondrop="event.preventDefault();this.style.borderColor='#cbd5e1';this.style.background='#fafafa';
            const f=event.dataTransfer.files;if(f&&f.length){const inp=this.querySelector('input');
            const dt=new DataTransfer();dt.items.add(f[0]);inp.files=dt.files;
            subirFotoTarea(${t.id}, inp, ${mid}, ${pid});}">
          <i class="bi bi-camera-plus" style="font-size:1.8rem;color:#dc2626"></i>
          <div style="font-weight:700;color:#0f172a">Agregar foto</div>
          <small style="color:#94a3b8">Click o arrastra · JPG/PNG/HEIC</small>
          <input type="file" accept="image/*,image/heic,image/heif" style="display:none"
            ${bloqueada ? 'disabled' : ''}
            onchange="subirFotoTarea(${t.id}, this, ${mid}, ${pid})">
        </label>

        {# Mobile: UN SOLO botón rojo grande que abre sheet de elección #}
        <div class="d-md-none">
          <button type="button" class="ilus-foto-btn-main"
            ${bloqueada ? 'disabled' : ''}
            onclick="abrirSheetFoto(${t.id}, ${mid}, ${pid})"
            style="width:100%;display:flex;align-items:center;justify-content:center;gap:10px;
              padding:16px 18px;border-radius:12px;
              background:linear-gradient(135deg,#dc2626,#b91c1c);
              color:#fff;font-weight:700;font-size:1rem;
              min-height:56px;border:none;cursor:pointer;
              box-shadow:0 4px 12px rgba(220,38,38,.3);
              transition:transform .1s, box-shadow .15s;">
            <i class="bi bi-camera-plus-fill" style="font-size:1.3rem"></i>
            <span>📷 Agregar foto</span>
          </button>
          {# Inputs ocultos, disparados via click() desde abrirSheetFoto() #}
          <input type="file" id="fotoCam-${t.id}" accept="image/*,image/heic,image/heif"
            capture="environment" style="display:none"
            ${bloqueada ? 'disabled' : ''}
            onchange="subirFotoTarea(${t.id}, this, ${mid}, ${pid})">
          <input type="file" id="fotoGal-${t.id}" accept="image/*,image/heic,image/heif"
            style="display:none"
            ${bloqueada ? 'disabled' : ''}
            onchange="subirFotoTarea(${t.id}, this, ${mid}, ${pid})">
        </div>

        <div id="t-fotos-${t.id}" class="foto-result"></div>
      </div>`;
      break;
    }
  }

  const isCheckType = (tipo === 'check');
  // ── FIX 2026-05-17 (Daniel) — El mini botón "Adjuntar ubicación" se
  // quitó porque sólo aplica para tareas tipo `tipo_respuesta='gps'`. Ese
  // caso ya renderiza su propio botón "Capturar mi ubicación (GPS)" dentro
  // del case 'gps' del switch de tipos. Mostrarlo en TODAS las tareas
  // confundía al técnico (no tiene nada que ver con fotos ni otras tareas).
  // ── Concurrencia: si OTRO técnico tiene el lock vivo, mostrar badge
  //    y deshabilitar interactividad. Admin/supervisor ve el badge pero
  //    puede igual editar (override). El dueño del lock NO ve nada raro.
  const lockInfo = TAREAS_LOCKS[t.id];
  let lockBadge = '';
  let isLockedByOther = false;
  if (lockInfo && lockInfo.locked_by_user_id && lockInfo.locked_by_user_id !== CURRENT_USER_ID){
    isLockedByOther = true;
    const minRel = _lockMinutosRelativos(lockInfo.locked_at);
    const minStr = (minRel >= 0) ? `hace ${minRel} min` : 'recién';
    lockBadge = `<div class="tx-lock-badge" title="Coordínate con el técnico antes de editar">
      <i class="bi bi-lock-fill"></i>
      <span>Siendo gestionada por ${_escapeHtml(lockInfo.locked_by_nombre || 'otro técnico')} · ${minStr}</span>
    </div>`;
  }
  // El admin ve el badge pero NO se bloquea su input (override). Para
  // técnicos, isLockedByOther agrega class is-locked-other (CSS deshabilita).
  const lockClass = (isLockedByOther && !IS_ADMIN_LOCK) ? ' is-locked-other' : '';

  return `<div class="tx-tarea${done ? ' done' : ''}${bloqueada ? ' bloqueada' : ''}${lockClass}"
       id="tar-${t.id}" data-version="${t.version || 0}">
    <div class="ttl">
      <div class="ttl-chk" ${isCheckType && !bloqueada ? `onclick="toggleCheck(${t.id}, ${mid}, ${pid})"` : ''}
        style="${isCheckType && !bloqueada ? 'cursor:pointer' : 'cursor:default'}">
        ${done ? '<i class="bi bi-check-lg"></i>' : ''}
      </div>
      <div class="ttl-info">
        <div class="ttl-text">${_escapeHtml(t.titulo)}
          <span class="ttl-badges">
            ${t.obligatoria ? '<span class="tx-badge-obl">Obligatoria</span>' : ''}
            ${t.requiere_foto ? '<span class="tx-badge-foto">📷 Foto</span>' : ''}
            <span class="tx-badge-tipo">${_escapeHtml(tipo)}</span>
          </span>
        </div>
        ${t.descripcion ? `<div class="ttl-sub">${_escapeHtml(t.descripcion)}</div>` : ''}
        ${lockBadge}
      </div>
    </div>
    ${ctrlHtml}
  </div>`;
}

// Devuelve los minutos transcurridos desde locked_at (string ISO o similar).
function _lockMinutosRelativos(lockedAtStr){
  if (!lockedAtStr) return -1;
  try {
    // El backend devuelve "YYYY-MM-DD HH:MM:SS" (chile_time o naive UTC).
    // Convertimos a Date. Si falla, retornamos -1.
    const t = new Date(lockedAtStr.replace(' ', 'T'));
    if (!isFinite(t.getTime())) return -1;
    const diffSec = (Date.now() - t.getTime()) / 1000;
    return Math.max(0, Math.floor(diffSec / 60));
  } catch(e){ return -1; }
}

// ── FIX 2026-05-17 — Render del resultado GPS sin crash ──
// valor.lat/lng pueden venir null, string, NaN, o el JSON ser undefined.
// Antes el código hacía valor.lat.toFixed(5) lo que crasheaba todo el
// renderTareaHtml dejando al técnico sin ningún control visible.
function _gpsValorRenderResultado(valor){
  if (!valor) return '';
  // Caso normal: tarea GPS guardó lat/lng
  let lat = Number(valor.lat);
  let lng = Number(valor.lng);
  let source = valor.source || valor.method || '';
  let dir = valor.dir || valor.direccion || '';
  // Caso extra: ubicación adjunta a tarea no-GPS
  if ((!isFinite(lat) || !isFinite(lng)) && valor._gps_extra){
    lat = Number(valor._gps_extra.lat);
    lng = Number(valor._gps_extra.lng);
    source = valor._gps_extra.source || source;
    dir = valor._gps_extra.dir || dir;
  }
  if (!isFinite(lat) || !isFinite(lng)){
    // No hay coords pero quizás hay texto manual
    if (valor.texto && typeof valor.texto === 'string' && valor.texto.length > 5){
      return `<div class="gps-result" style="background:#fff8e1;color:#92400e">
        <i class="bi bi-pencil-square"></i> 📍 Manual: ${_escapeHtml(valor.texto)}
      </div>`;
    }
    return '';
  }
  // Label según fuente
  let label = 'GPS exacto', bg = '#dcfce7', color = '#15803d';
  if (source === 'ip'){
    label = 'Por IP (~5km precisión)';
    bg = '#fff8e1'; color = '#92400e';
  } else if (source === 'manual'){
    label = 'Manual';
    bg = '#dbeafe'; color = '#1e40af';
  } else if (source === 'gps_low' || source === 'watch'){
    label = 'GPS estándar';
  }
  const acc = isFinite(valor.accuracy) ? ` ±${Math.round(valor.accuracy)}m` : '';
  return `<div class="gps-result" style="background:${bg};color:${color}">
    <i class="bi bi-check-circle-fill"></i> 📍 ${_escapeHtml(label)}: ${lat.toFixed(5)}, ${lng.toFixed(5)}${acc}
    ${dir ? `<div style="font-size:.7rem;margin-top:3px;opacity:.85">${_escapeHtml(dir)}</div>` : ''}
  </div>`;
}

function refreshPills(btn, group){
  const cont = btn.closest('.tx-btn-group');
  cont.querySelectorAll('.tx-btn-pill').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
}

// ════════════════════════════════════════════════════════
// 2026-05-19 (Daniel) — INTEGRACIÓN LEVANTAMIENTO ↔ FICHA TÉCNICA
// Helpers que conectan el levantamiento fotográfico con la ficha
// permanente del equipo (mant_maquinas).
// ════════════════════════════════════════════════════════

// Heurística: detecta si el título de una tarea pide un serial/N° serie.
// Cubre las variantes castellano + inglés que usa Daniel en las plantillas.
function _esCampoSerial(titulo){
  if (!titulo) return false;
  // Solo evaluar el título base (antes del " — Nombre Equipo (S/N: ...)" que agrega el backend)
  const base = String(titulo).split(' — ')[0].trim();
  // 2026-06-12 (Daniel): exclusión DURA — Marca y Observaciones NUNCA son
  // campos de serial, aunque la plantilla mezcle palabras en el título.
  // (El serial se sugiere solo en campos que realmente piden serie.)
  if (/^\s*(marca|observaci)/i.test(base)) return false;
  // 'sn' anclado por ambos lados (\bsn\b) — antes 'sn\b' matcheaba la
  // subcadena 'sn' en medio de cualquier palabra.
  return /(\bserie\b|\bserial\b|n[°º]?\s*serie|\bsn\b|\bs\/n\b|numero\s+de\s+serie|n[uú]mero\s+de\s+serie)/i.test(base);
}

// 2026-06-12 (Daniel) — Detecta si el título de una tarea pide la MARCA del
// equipo ("Marca", "Marca del Equipo", "Marca/Fabricante"). En esos campos
// se ofrece el catálogo de marcas/familias (seed + casa + ERP Random) como
// combobox (datalist): el técnico elige una familia o escribe libre.
function _esCampoMarca(titulo){
  if (!titulo) return false;
  const base = String(titulo).split(' — ')[0].trim();
  return /^\s*marca\b/i.test(base);
}

// Catálogo de marcas para la CHECKLIST (mismo patrón que _capCargarMarcas
// del modal de captura, con datalist propio para no colisionar IDs).
let _chkMarcas = null;
async function _chkCargarMarcas(){
  if (_chkMarcas === null){
    try {
      const r = await fetch('/mantenciones/api/marcas');
      const d = await r.json();
      _chkMarcas = (d && d.marcas) || [];
    } catch(e){ _chkMarcas = []; }
  }
  const dl = document.getElementById('chkMarcasDL');
  if (dl && !dl.children.length && _chkMarcas.length){
    dl.innerHTML = _chkMarcas.map(m => `<option value="${_escapeAttr(m)}">`).join('');
  }
}

// Normaliza la marca al guardar: si coincide (case-insensitive) con una del
// catálogo, usa la forma canónica → BD consistente ("life fitness" → "Life Fitness").
function _chkNormalizarMarca(inp){
  const v = (inp.value || '').trim();
  if (v && _chkMarcas && _chkMarcas.length){
    const hit = _chkMarcas.find(m => m.toLowerCase() === v.toLowerCase());
    if (hit && hit !== v){ inp.value = hit; return hit; }
  }
  return v;
}

// Sugiere el serial actualmente registrado en mant_maquinas (que vino en
// EQUIPOS_IDX). Al click: precarga el input + dispara guardado normal.
// Importante: NO modifica nada en BD directamente — el serial pisará la
// ficha técnica recién cuando se cierre el levantamiento (vía
// _promover_levantamiento_a_maquina, que ya audita los cambios).
async function sugerirSerialRegistrado(tid, serieReg, mid, pid){
  const inp = document.getElementById('texto-input-' + tid);
  if (!inp){
    ilusToast('No se encontró el campo de serial', { type: 'error' });
    return;
  }
  if (!serieReg){
    ilusToast('No hay serial registrado en la ficha', { type: 'warning' });
    return;
  }
  // Si ya hay valor y es distinto, preguntar antes de pisarlo
  const curVal = (inp.value || '').trim();
  if (curVal && curVal !== serieReg){
    const ok = await ilusConfirm({
      title: 'Reemplazar serial',
      message: '¿Reemplazar el valor actual con el serial registrado en la ficha?',
      sub: `<strong>Actual:</strong> ${_escapeHtml(curVal)}<br><strong>Registrado:</strong> ${_escapeHtml(serieReg)}`,
      subHtml: true,
      okLabel: 'Sí, usar el registrado',
      cancelLabel: 'No, mantener actual',
    });
    if (!ok) return;
  }
  inp.value = serieReg;
  inp.focus();
  // Disparar el guardado normal (texto onblur) y feedback visual
  if (typeof _textoOnBlur === 'function'){
    _textoOnBlur(inp, tid, mid, pid, 3);
  } else {
    inp.dispatchEvent(new Event('blur'));
  }
  ilusToast('✓ Serial sugerido aplicado', { type: 'success' });
}

// ════════════════════════════════════════════════════════════
// 2026-05-22 (Daniel) — Modal de CAPTURA / FICHA TÉCNICA editable
//
// Antes: read-only. El técnico solo VALIDABA.
// Ahora: en OTs tipo levantamiento Y preventiva-con-equipos, el
// técnico EDITA estado/marca/modelo/año/voltaje/ubicación/daño/obs
// + sube N fotos al equipo. Cada cambio guarda al perder foco
// (auto-save). Al cerrar la OT, _promover_levantamiento_a_maquina
// reaplica TODO al mant_maquinas + mant_maquina_eventos.
//
// Read-only se activa si VISITA_ESTADO ∈ {cerrada, pendiente_aprobacion}
// o si el usuario NO puede ejecutar (PUEDE_EJECUTAR_FLAG=false).
// ════════════════════════════════════════════════════════════
async function abrirModalFichaTecnica(mid){
  const modalEl = document.getElementById('modalFichaTecnica');
  if (!modalEl){ ilusToast('Modal no disponible', { type: 'error' }); return; }
  const body = document.getElementById('modalFichaBody');
  const linkFull = document.getElementById('modalFichaAbrirFull');
  body.innerHTML = `<div class="text-center text-muted py-5">
      <div class="spinner-border" role="status" style="color:#dc2626"></div>
      <div class="mt-2 small">Cargando ficha técnica…</div>
    </div>`;
  if (linkFull) linkFull.href = '/mantenciones/maquinas/' + mid;
  const modal = bootstrap.Modal.getOrCreateInstance(modalEl);
  modal.show();
  try {
    // 1) Carga ficha técnica (datos del equipo)
    const r = await fetch('/mantenciones/api/maquinas/' + mid + '/ficha-tecnica', {
      headers: { 'Accept': 'application/json' },
    });
    const data = await r.json();
    if (!data.ok){
      body.innerHTML = `<div class="alert alert-warning" style="margin:0">
        <i class="bi bi-exclamation-triangle me-2"></i>${_escapeHtml(data.error || 'No se pudo cargar la ficha')}
      </div>`;
      return;
    }
    // 2) Carga fotos OT+ficha en paralelo (no bloquea render principal)
    let fotosData = { fotos: [], foto_principal: '' };
    try {
      const r2 = await fetch(
        `/mantenciones/api/visitas/${VID}/equipo/${mid}/fotos`,
        { headers: { 'Accept': 'application/json' } }
      );
      if (r2.ok){
        const j2 = await r2.json();
        if (j2.ok){ fotosData = j2; }
      }
    } catch(_) { /* sin fotos no bloquea */ }
    body.innerHTML = _renderFichaTecnicaModalBody(data, fotosData, mid);
    // 3) Init auto-save listeners (solo en modo captura editable)
    _initCapturaListeners(mid);
  } catch(err){
    console.error('[abrirModalFichaTecnica]', err);
    body.innerHTML = `<div class="alert alert-danger" style="margin:0">
      <i class="bi bi-x-circle me-2"></i>Error de conexión al cargar la ficha técnica.
    </div>`;
  }
}

// ── Estado en memoria de la captura activa ───────────────────
// Permite a los handlers de save saber qué máquina/estado están tocando
// y no re-leer el DOM cada vez.
let _capturaActiva = { mid: null, estadoCapturado: 'operativo', tieneDano: 0, completado: 0 };

// PATCH del equipo — ROBUSTO (F-LEV 2026-06-10, Daniel "sí o sí"):
//   · COLA secuencial: un PATCH a la vez (dos blurs rápidos ya no corren en
//     paralelo ni se pisan).
//   · Timeout 12s con AbortController + 1 reintento automático (4G lenta).
//   · Si el backend responde con warning (ej. estado inválido), se muestra.
let _capColaSave = Promise.resolve();

function _capturaSave(mid, payload){
  // Encadena en la cola; la promesa devuelta es la de ESTE guardado.
  const trabajo = _capColaSave.then(() => _capturaSaveReal(mid, payload, 0));
  // La cola nunca se rompe (errores quedan en la promesa del caller).
  _capColaSave = trabajo.catch(() => {});
  return trabajo;
}

async function _capturaSaveReal(mid, payload, intento){
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), 12000);
  try {
    const r = await fetch(
      `/mantenciones/api/visitas/${VID}/equipo/${mid}/datos`,
      {
        method: 'PATCH',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(payload),
        signal: ctrl.signal,
      }
    );
    clearTimeout(timer);
    const d = await r.json().catch(() => ({}));
    if (!r.ok || d.ok === false){
      throw new Error((d && d.error) || ('HTTP ' + r.status));
    }
    if (d.warning && typeof ilusToast === 'function'){
      ilusToast('⚠ ' + d.warning, { type: 'warning' });
    }
    return d;
  } catch(err){
    clearTimeout(timer);
    // 1 reintento automático en timeout/red caída (no en errores del servidor)
    const esRed = (err && (err.name === 'AbortError' || err.message === 'Failed to fetch'
                   || /network/i.test(err.message || '')));
    if (esRed && intento < 1){
      await new Promise(res => setTimeout(res, 900));
      return _capturaSaveReal(mid, payload, intento + 1);
    }
    throw err;
  }
}

// ── Catálogo de marcas (datalist, lazy al primer focus) ─────────────
let _capMarcas = null;
async function _capCargarMarcas(){
  if (_capMarcas === null){
    try {
      const r = await fetch('/mantenciones/api/marcas');
      const d = await r.json();
      _capMarcas = (d && d.marcas) || [];
    } catch(e){ _capMarcas = []; }
  }
  // FIX 2026-06-12: rellenar SIEMPRE que el datalist esté vacío. El modal
  // re-crea #capMarcasDL en cada apertura; antes el early-return del cache
  // dejaba el datalist nuevo sin opciones (sin sugerencias desde la 2ª vez).
  const dl = document.getElementById('capMarcasDL');
  if (dl && !dl.children.length && _capMarcas.length){
    dl.innerHTML = _capMarcas.map(m => `<option value="${_escapeAttr(m)}">`).join('');
  }
}

// Normaliza la marca al guardar: si coincide (case-insensitive) con una del
// catálogo, usa la forma canónica → BD consistente ("life fitness" → "Life Fitness").
function _capNormalizarMarca(inp){
  const v = (inp.value || '').trim();
  if (v && _capMarcas && _capMarcas.length){
    const hit = _capMarcas.find(m => m.toLowerCase() === v.toLowerCase());
    if (hit && hit !== v){ inp.value = hit; return hit; }
  }
  return v;
}

// Acepta la serie sugerida con un click (F-LEV).
function _capUsarSerieSugerida(mid, serie){
  const inp = document.getElementById('cap-serie');
  if (!inp) return;
  inp.value = serie;
  const wrap = document.getElementById('capSerieSugWrap');
  if (wrap) wrap.style.display = 'none';
  _capFieldBlur(mid, 'serie', serie);
}

// Helper: marca status del campo (saving/saved/error)
function _setCapStatus(fieldId, cls, txt){
  const st = document.getElementById('cap-st-' + fieldId);
  if (!st) return;
  st.className = 'save-status ' + cls;
  st.textContent = txt || '';
  if (cls === 'saved'){
    clearTimeout(st._t);
    st._t = setTimeout(() => { st.textContent = ''; }, 1800);
  }
}

// Auto-save de un campo de texto/number/select. Solo si el valor cambió.
async function _capFieldBlur(mid, field, valor){
  // Validación rápida cliente para anio_fabricacion
  if (field === 'anio_fabricacion' && valor){
    const n = parseInt(valor, 10);
    if (isNaN(n) || n < 1950 || n > (new Date().getFullYear() + 2)){
      _setCapStatus(field, 'error', 'Año inválido');
      return;
    }
  }
  _setCapStatus(field, 'saving', 'Guardando…');
  try {
    const payload = {};
    payload[field] = valor;
    await _capturaSave(mid, payload);
    // F-LEV: aviso suave si la serie quedó vacía (sin bloquear — puede que
    // la placa no exista), para que no pase desapercibido.
    if (field === 'serie' && !(valor || '').trim()){
      _setCapStatus(field, 'error', 'Sin serie — usa la sugerida si no encuentras la placa');
    } else {
      _setCapStatus(field, 'saved', '✓ Guardado');
    }
    // Actualizar caché local del equipo (para no perder el dato al cerrar modal)
    if (EQUIPOS_IDX[String(mid)]){
      EQUIPOS_IDX[String(mid)][field] = valor || '';
    }
  } catch(e){
    console.error('[_capFieldBlur]', field, e);
    // F-LEV: error HONESTO con reintento de un click (nunca perder el dato).
    _setCapStatus(field, 'error', '⚠ Sin guardar');
    const st = document.getElementById('cap-st-' + field);
    if (st){
      st.innerHTML = '⚠ Sin guardar — <a href="#" style="color:#dc2626;font-weight:800" ' +
        `onclick="event.preventDefault();_capFieldBlur(${mid}, '${field}', document.getElementById('cap-${field}') ? document.getElementById('cap-${field}').value : '')">Reintentar</a>`;
    }
    if (typeof ilusToast === 'function') ilusToast('No se guardó "' + field + '": ' + (e.message || 'error de red'), { type: 'error' });
  }
}

// Estado capturado: chip click
async function _capChipEstado(mid, estado){
  document.querySelectorAll('#modalFichaTecnica .cap-estado-chip')
    .forEach(c => c.setAttribute('data-active', c.dataset.estado === estado ? '1' : '0'));
  _capturaActiva.estadoCapturado = estado;
  _setCapStatus('estado_capturado', 'saving', 'Guardando…');
  try {
    await _capturaSave(mid, { estado_capturado: estado });
    _setCapStatus('estado_capturado', 'saved', '✓ Guardado');
  } catch(e){
    _setCapStatus('estado_capturado', 'error', 'Error al guardar');
  }
}

// Toggle daño
async function _capToggleDano(mid){
  const el = document.getElementById('cap-dano-toggle');
  if (!el) return;
  const next = el.getAttribute('data-active') === '1' ? 0 : 1;
  el.setAttribute('data-active', String(next));
  _capturaActiva.tieneDano = next;
  _setCapStatus('tiene_dano', 'saving', 'Guardando…');
  try {
    await _capturaSave(mid, { tiene_dano: !!next });
    _setCapStatus('tiene_dano', 'saved', next ? '⚠️ Daño marcado' : '✓ Sin daño');
    // F-LEV 2026-06-10: si marca daño y NO hay descripción → pedirla (suave).
    // El informe post-servicio lee estas observaciones; un daño sin describir
    // no le sirve a nadie.
    if (next){
      const obs = document.getElementById('cap-observaciones');
      if (obs && !(obs.value || '').trim()){
        if (typeof ilusToast === 'function')
          ilusToast('Describe el daño en Observaciones (qué tiene, dónde, gravedad)', { type: 'warning' });
        try { obs.focus(); obs.style.borderColor = '#f59e0b'; } catch(_){}
      }
    }
  } catch(e){
    _setCapStatus('tiene_dano', 'error', 'Error');
  }
}

// Toggle plan de mantención — llama PUT /api/maquinas/<mid>/aplica-mantencion
async function _capToggleAplica(mid, toggleEl){
  if (!toggleEl) return;
  const prev = toggleEl.dataset.active === '1';
  const nuevo = !prev;
  toggleEl.dataset.active = nuevo ? '1' : '0';
  const lbl = toggleEl.querySelector('.lbl');
  if (lbl) lbl.innerHTML = (nuevo
    ? 'Sí — incluido en el plan de mantención'
    : 'No — accesorio o no aplica mantención') + '<small>Tocá para cambiar</small>';
  _setCapStatus('aplica_mantencion', 'saving', 'Guardando…');
  try {
    const r = await fetch(`/mantenciones/api/maquinas/${mid}/aplica-mantencion`, {
      method: 'PUT',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({aplica: nuevo}),
    });
    const d = await r.json();
    if (r.ok && d.ok) {
      _setCapStatus('aplica_mantencion', 'saved', nuevo ? '✓ En el plan' : '✓ Excluido del plan');
      ilusToast(nuevo ? '✓ Incluido en el plan de mantención' : 'Excluido del plan de mantención',
                {type: nuevo ? 'success' : 'info'});
    } else { throw new Error(d.error || 'Error'); }
  } catch(e) {
    toggleEl.dataset.active = prev ? '1' : '0';
    if (lbl) lbl.innerHTML = (prev
      ? 'Sí — incluido en el plan de mantención'
      : 'No — accesorio o no aplica mantención') + '<small>Tocá para cambiar</small>';
    _setCapStatus('aplica_mantencion', 'error', '⚠ No se guardó');
    ilusToast('No se pudo actualizar el plan', {type: 'error'});
  }
}

// Marcar equipo como completado (toggle)
async function _capMarcarCompletado(mid){
  const btn = document.getElementById('cap-completado-btn');
  if (!btn) return;
  const next = btn.getAttribute('data-completado') === '1' ? 0 : 1;
  btn.disabled = true;
  try {
    await _capturaSave(mid, { completado: !!next });
    btn.setAttribute('data-completado', String(next));
    btn.innerHTML = next
      ? '<i class="bi bi-check-circle-fill"></i>Equipo marcado como completado — toca para revertir'
      : '<i class="bi bi-check2-square"></i>Marcar equipo como completado';
    _capturaActiva.completado = next;
    if (next){
      // Reflejar en la tarjeta y badge de la lista
      EQUIPOS_ESTADO_REVISION[String(mid)] = {
        ...(EQUIPOS_ESTADO_REVISION[String(mid)] || {}),
        estado_revision: 'con_cambios',
        revisado_at: (new Date()).toISOString().slice(0,19).replace('T',' '),
      };
      _refreshEqCardBadge(mid);
      ilusToast('✓ Equipo marcado como completado', { type: 'success' });
    } else {
      ilusToast('Marca de completado removida', { type: 'info' });
    }
  } catch(e){
    console.error('[_capMarcarCompletado]', e);
    ilusToast('No se pudo marcar el equipo', { type: 'error' });
  } finally {
    btn.disabled = false;
  }
}

// Subir foto (uploader o reemplazo)
async function _capSubirFoto(mid, fileInput){
  const rawFile = fileInput.files && fileInput.files[0];
  if (!rawFile) return;
  const uploader = document.getElementById('cap-foto-uploader');
  if (uploader){ uploader.classList.add('uploading'); uploader.innerHTML = '<i class="bi bi-arrow-clockwise spin"></i><span>Comprimiendo…</span>'; }
  // PERF 2026-05-22: comprimir en cliente antes de subir (iPhone 4-8MB → ~400KB).
  const file = await _compressImageBeforeUpload(rawFile);
  fileInput.value = '';

  // ── UI OPTIMISTA (perf 2026-05-31): mostramos la foto AL INSTANTE con un
  // objectURL local y dejamos al técnico seguir trabajando. La subida a
  // Cloudinary va en SEGUNDO PLANO; al terminar reemplazamos por la URL real.
  // Si falla, revertimos y avisamos. (Antes el técnico esperaba el round-trip.)
  const previewUrl = URL.createObjectURL(file);
  const eqYaTieneFoto = !!(EQUIPOS_IDX[String(mid)] && EQUIPOS_IDX[String(mid)].foto_url);
  const cardImg = document.querySelector(`#tx-eq-card-${mid} .eq-foto-img`);
  const prevCardSrc = cardImg ? cardImg.getAttribute('src') : null;
  // Si el equipo no tenía foto, ésta será la principal → mostrarla ya en la card.
  if (cardImg && !eqYaTieneFoto){ cardImg.src = previewUrl; }
  // Tile "cargando" al inicio de la galería del modal (si está abierta).
  const grid = document.getElementById('cap-fotos-grid');
  let optTile = null;
  if (grid){
    optTile = document.createElement('div');
    optTile.style.cssText = 'position:relative;aspect-ratio:1;border-radius:8px;overflow:hidden;opacity:.75';
    optTile.innerHTML =
      `<img src="${previewUrl}" style="width:100%;height:100%;object-fit:cover">` +
      `<span style="position:absolute;inset:0;display:flex;align-items:center;justify-content:center;background:rgba(0,0,0,.3)">` +
      `<i class="bi bi-arrow-clockwise spin" style="color:#fff;font-size:1.3rem"></i></span>`;
    grid.prepend(optTile);
  }
  // Liberar el uploader de inmediato: el técnico puede seguir.
  if (uploader){
    uploader.classList.remove('uploading');
    uploader.innerHTML = '<i class="bi bi-camera-fill"></i><span>Subir foto</span>';
  }
  ilusToast('Subiendo foto en segundo plano…', { type: 'info' });

  const fd = new FormData();
  fd.append('foto', file, file.name || rawFile.name || 'foto.jpg');
  // Si el equipo NO tiene foto principal, esta primera es la principal.
  // Si ya tiene, el backend degrada a 'detalle' automáticamente.
  fd.append('tipo_foto', 'principal');
  try {
    const r = await fetch(
      `/mantenciones/api/visitas/${VID}/equipo/${mid}/foto`,
      { method: 'POST', body: fd }
    );
    const d = await r.json();
    if (!d.ok){ throw new Error(d.error || 'No se pudo subir'); }
    ilusToast(d.degradado
      ? '✓ Foto agregada a la galería del equipo'
      : '✓ Foto principal guardada', { type: 'success' });
    // Si fue principal, fijar la URL REAL en hero/card/índice.
    if (d.es_principal){
      if (EQUIPOS_IDX[String(mid)]) EQUIPOS_IDX[String(mid)].foto_url = d.url;
      if (cardImg){ cardImg.src = d.url; }
    } else if (cardImg && !eqYaTieneFoto && prevCardSrc !== null){
      // Optimista asumió principal pero el backend la degradó → revertir card.
      cardImg.src = prevCardSrc;
    }
    // Refrescar galería (reemplaza el tile optimista por los reales).
    await _capRefreshFotos(mid);
  } catch(e){
    console.error('[_capSubirFoto]', e);
    // Revertir UI optimista.
    if (optTile){ optTile.remove(); }
    if (cardImg && !eqYaTieneFoto && prevCardSrc !== null){ cardImg.src = prevCardSrc; }
    ilusToast(e.message || 'No se pudo subir la foto', { type: 'error' });
  } finally {
    try { URL.revokeObjectURL(previewUrl); } catch(_){}
  }
}

// 2026-06-12 (Daniel, estilo Fracttal) — Borrar una foto de ESTA OT.
// Disponible mientras la OT no esté sellada por la firma del cliente.
// El backend además limpia la copia del levantamiento y libera la foto
// principal de la ficha si la puso esta misma OT (para poder re-subir).
async function _capBorrarFoto(mid, fotoId){
  const ok = await ilusConfirm({
    title: 'Eliminar foto',
    message: '¿Quitar esta foto de la OT?',
    sub: 'Se elimina de la evidencia de esta OT y no pasará a la ficha del equipo. Podrás subir otra.',
    okLabel: 'Eliminar', cancelLabel: 'Cancelar',
    danger: true,
  });
  if (!ok) return;
  try {
    const r = await fetch(`/mantenciones/api/visitas/${VID}/fotos/${fotoId}`, {
      method: 'DELETE',
      headers: { 'Accept': 'application/json' },
    });
    const j = await r.json().catch(() => ({}));
    if (!r.ok || !j.ok) throw new Error(j.error || 'No se pudo eliminar la foto');
    ilusToast('✓ Foto eliminada', { type: 'success' });
    await _capRefreshFotos(mid);
  } catch(e){
    console.error('[_capBorrarFoto]', e);
    ilusToast(e.message || 'No se pudo eliminar la foto', { type: 'error' });
  }
}

// Refresca solo la grilla de fotos del modal (sin recargar todo)
async function _capRefreshFotos(mid){
  // PERF 2026-05-22 — Skeleton mientras carga (mejora percepción de
  // velocidad en redes lentas / 4G de técnico en sala).
  const cont = document.getElementById('cap-fotos-grid');
  if (cont){
    cont.innerHTML = Array(6).fill(0).map(() => '<div class="skeleton-tile"></div>').join('');
  }
  try {
    const r = await fetch(
      `/mantenciones/api/visitas/${VID}/equipo/${mid}/fotos`,
      { headers: { 'Accept': 'application/json' } }
    );
    if (!r.ok) {
      if (cont) cont.innerHTML = '<div class="text-muted small" style="grid-column:span 3">No se pudo cargar la galería.</div>';
      return;
    }
    const j = await r.json();
    if (!j.ok) {
      if (cont) cont.innerHTML = '<div class="text-muted small" style="grid-column:span 3">No se pudo cargar la galería.</div>';
      return;
    }
    if (cont){
      cont.innerHTML = _renderFotosTiles(j.fotos, mid);
    }
  } catch(_) {
    if (cont) cont.innerHTML = '<div class="text-muted small" style="grid-column:span 3">Error de red al cargar fotos.</div>';
  }
}

function _renderFotosTiles(fotos, mid){
  const readonly = (VISITA_ESTADO === 'cerrada' ||
                    VISITA_ESTADO === 'pendiente_aprobacion' ||
                    !PUEDE_EJECUTAR_FLAG);
  let html = '';
  if (!readonly){
    html += `<label class="cap-foto-uploader" id="cap-foto-uploader" for="cap-foto-input">
      <i class="bi bi-camera-fill"></i><span>Subir foto</span>
      <input type="file" id="cap-foto-input" accept="image/*" capture="environment"
             style="display:none" onchange="_capSubirFoto(${mid}, this)">
    </label>`;
  }
  (fotos || []).forEach(f => {
    const cls = f.fuente === 'ot' ? 'fuente-ot' : 'fuente-ficha';
    const lbl = f.fuente === 'ot' ? 'NUEVA' : 'FICHA';
    // PERF 2026-05-22: 'card' (≤400px) para grid; el lightbox usa 'gallery'.
    const thumbUrl = cloudTx(f.url, 'card');
    // 2026-06-12 (Daniel, estilo Fracttal): el técnico puede BORRAR las fotos
    // de ESTA OT (fuente 'ot') mientras la OT no esté sellada por el cliente.
    // Las fotos históricas de la ficha (fuente 'ficha') no se tocan desde acá.
    const delBtn = (!readonly && f.fuente === 'ot' && f.id)
      ? `<button type="button" class="cap-foto-del" title="Eliminar esta foto"
           onclick="event.stopPropagation();_capBorrarFoto(${mid}, ${f.id})"
           style="position:absolute;top:4px;right:4px;width:26px;height:26px;
             border-radius:50%;border:0;background:rgba(220,38,38,.92);color:#fff;
             font-size:.8rem;line-height:1;display:flex;align-items:center;
             justify-content:center;cursor:pointer;z-index:2;
             box-shadow:0 1px 4px rgba(0,0,0,.35)">
           <i class="bi bi-trash-fill"></i>
         </button>`
      : '';
    html += `<div class="cap-foto-tile ${cls}" style="position:relative"
        title="${_escapeAttr(f.descripcion || f.tipo_foto || '')}">
      <img src="${_escapeAttr(thumbUrl)}" alt="${_escapeAttr(f.descripcion || '')}"
           loading="lazy" decoding="async">
      <span class="badge-fuente">${lbl}</span>
      ${delBtn}
    </div>`;
  });
  if (!fotos || !fotos.length){
    html += `<div style="grid-column:span 3;padding:18px 16px;text-align:center;
        background:linear-gradient(135deg,#fff8e1 0%,#fffaf2 100%);
        border:1px dashed #fcd34d;border-radius:12px;color:#7c2d12;
        font-size:.82rem;font-weight:600;line-height:1.5;
        animation:ilus-fade-up 320ms cubic-bezier(.16,1,.3,1) both;">
      <i class="bi bi-camera" style="font-size:1.4rem;display:block;margin-bottom:6px;opacity:.7"></i>
      <div style="font-weight:700;color:#92400e">Sin fotos capturadas</div>
      <div style="font-size:.74rem;font-weight:500;color:#a16207;margin-top:2px">
        Sube la primera para que quede registrada en la ficha del equipo.
      </div>
    </div>`;
  }
  return html;
}

// Inicializa listeners después de inyectar el HTML del body
function _initCapturaListeners(mid){
  // No hay magia adicional — los onchange/onblur ya vienen inline en el HTML.
  // Reseteamos estado local
  _capturaActiva = { mid: mid, estadoCapturado: 'operativo', tieneDano: 0, completado: 0 };
}

function _renderFichaTecnicaModalBody(data, fotosData, midParam){
  const e = data.equipo || {};
  const contratos = data.contratos_relacionados || data.contratos || [];
  const ots = data.historial_visitas || data.ots || [];
  const stats = data.stats || {};
  const mid = midParam || e.id;
  fotosData = fotosData || { fotos: [], foto_principal: e.foto_url || '' };

  // Modo: editable solo si OT en ejecución Y el usuario puede ejecutar
  const readonly = (VISITA_ESTADO === 'cerrada' ||
                    VISITA_ESTADO === 'pendiente_aprobacion' ||
                    !PUEDE_EJECUTAR_FLAG);

  // Datos del cliente (header)
  const clienteHtml = `
    <div style="background:#0a0a0a;color:#fff;padding:10px 14px;border-radius:10px;margin-bottom:14px">
      <div style="font-size:.7rem;opacity:.7;letter-spacing:.05em;text-transform:uppercase">Cliente</div>
      <div style="font-weight:800;font-size:1rem">${_escapeHtml(e.razon_social || '—')}</div>
      <div style="font-size:.75rem;opacity:.85;margin-top:2px">
        ${e.cli_rut ? 'RUT: ' + _escapeHtml(e.cli_rut) : ''}
        ${e.cli_direccion ? ' · ' + _escapeHtml(e.cli_direccion) : ''}
        ${e.cli_comuna ? ', ' + _escapeHtml(e.cli_comuna) : ''}
      </div>
    </div>`;

  // Banner: read-only OR "modo captura"
  const bannerHtml = readonly
    ? `<div class="cap-readonly-banner">
         <i class="bi bi-info-circle-fill"></i>
         <div>
           <strong>Modo solo-lectura.</strong>
           La OT ya está ${VISITA_ESTADO === 'cerrada' ? 'cerrada' : 'pendiente de aprobación'}.
           Los datos del equipo no se pueden modificar desde aquí.
         </div>
       </div>`
    : (ES_LEVANTAMIENTO
        ? `<div class="lev-banner-ficha" role="status">
             <i class="bi bi-clipboard-check-fill"></i>
             <div>
               <strong>Captura de ficha técnica</strong>
               <div>Cada cambio se guarda automáticamente. Al cerrar la OT,
                 estos datos se aplican a la <strong>ficha permanente del equipo</strong>.</div>
             </div>
           </div>`
        : `<div class="lev-banner-ficha" role="status"
                style="background:linear-gradient(135deg,#dbeafe,#bfdbfe);border-color:#93c5fd;color:#1e3a8a">
             <i class="bi bi-pencil-square" style="color:#3b82f6"></i>
             <div>
               <strong>Edición rápida del equipo</strong>
               <div>Puedes actualizar marca/modelo/datos si los conoces.
                 Los cambios quedan registrados en el historial del equipo.</div>
             </div>
           </div>`);

  // Foto principal actual + nombre del equipo (header del equipo)
  const fotoUrl = fotosData.foto_principal || e.foto_url || '';
  // PERF 2026-05-22: 'gallery' (≤800px) — la foto principal del header es
  // un poco más grande que las cards en grid.
  const fotoHtml = fotoUrl
    ? `<img src="${_escapeAttr(cloudTx(fotoUrl, 'gallery'))}" alt=""
            loading="lazy" decoding="async"
            style="width:100%;max-height:180px;object-fit:cover;border-radius:10px;border:1px solid #e5e7eb">`
    : `<div style="width:100%;height:120px;background:#f3f4f6;border:1px dashed #cbd5e1;border-radius:10px;display:flex;align-items:center;justify-content:center;color:#94a3b8"><i class="bi bi-image" style="font-size:2rem"></i></div>`;

  const headerEquipoHtml = `
    <div style="margin-bottom:14px">
      ${fotoHtml}
      <div style="margin-top:10px;font-weight:800;font-size:1.05rem;color:#0a0a0a">${_escapeHtml(e.nombre || ('Equipo #' + e.id))}</div>
      <div style="font-size:.78rem;color:#6b7280">
        ${e.sku ? 'SKU: ' + _escapeHtml(e.sku) : ''}
        ${e.serie ? ' · S/N: ' + _escapeHtml(e.serie) : ''}
      </div>
    </div>`;

  // ── SECCIÓN 1: Estado capturado (chips) ────────────────────
  const estadoActual = (e.estado_capturado || '').toLowerCase() || 'operativo';
  const estadosChips = [
    {k:'operativo',      icon:'bi-check-circle-fill', label:'Operativo'},
    {k:'advertencia',    icon:'bi-exclamation-triangle-fill', label:'Advertencia'},
    {k:'fuera_servicio', icon:'bi-x-circle-fill', label:'Fuera de servicio'},
    {k:'en_reparacion',  icon:'bi-tools', label:'En reparación'},
    {k:'dado_baja',      icon:'bi-archive-fill', label:'Dado de baja'},
    {k:'no_encontrado',  icon:'bi-question-circle-fill', label:'No encontrado'},
  ];
  const chipsHtml = estadosChips.map(s => `
    <button type="button" class="cap-estado-chip"
            data-estado="${s.k}"
            data-active="${estadoActual === s.k ? '1' : '0'}"
            ${readonly ? 'disabled' : ''}
            onclick="_capChipEstado(${mid}, '${s.k}')">
      <i class="bi ${s.icon}"></i>${_escapeHtml(s.label)}
    </button>`).join('');
  const seccionEstadoHtml = `
    <div class="captura-section">
      <h6 class="ttl"><i class="bi bi-activity"></i>Estado capturado</h6>
      <div class="cap-estado-chips">${chipsHtml}</div>
      <div class="save-status" id="cap-st-estado_capturado"></div>
      <div class="hint">¿Cómo encuentras este equipo ahora?</div>
    </div>`;

  // ── SECCIÓN 2: Datos técnicos (form editable) ──────────────
  const inp = (k, lbl, val, opts) => {
    opts = opts || {};
    const valAttr = (val === null || val === undefined) ? '' : String(val);
    const type = opts.type || 'text';
    const placeholder = opts.placeholder || '';
    return `<div class="cap-field${opts.full ? ' full' : ''}">
      <label for="cap-${k}">${_escapeHtml(lbl)}</label>
      <input id="cap-${k}" type="${type}" name="${k}"
        value="${_escapeAttr(valAttr)}"
        placeholder="${_escapeAttr(placeholder)}"
        ${opts.maxlength ? `maxlength="${opts.maxlength}"` : ''}
        ${opts.min ? `min="${opts.min}"` : ''}
        ${opts.max ? `max="${opts.max}"` : ''}
        ${readonly ? 'readonly' : ''}
        onblur="_capFieldBlur(${mid}, '${k}', this.value)">
      <div class="save-status" id="cap-st-${k}"></div>
    </div>`;
  };
  const txtArea = (k, lbl, val, opts) => {
    opts = opts || {};
    return `<div class="cap-field full">
      <label for="cap-${k}">${_escapeHtml(lbl)}</label>
      <textarea id="cap-${k}" name="${k}"
        placeholder="${_escapeAttr(opts.placeholder || '')}"
        ${opts.maxlength ? `maxlength="${opts.maxlength}"` : ''}
        ${readonly ? 'readonly' : ''}
        onblur="_capFieldBlur(${mid}, '${k}', this.value)">${_escapeHtml(val || '')}</textarea>
      <div class="save-status" id="cap-st-${k}"></div>
    </div>`;
  };
  // F-LEV 2026-06-10 (Daniel): serie SUGERIDA (la del producto) con un click,
  // y marca con CATÁLOGO (datalist /api/marcas: seed + casa + ERP read-only)
  // sin bloquear texto libre.
  const serieSug = (!e.serie && e.serie_sugerida) ? String(e.serie_sugerida) : '';
  const serieSugHtml = (serieSug && !readonly) ? `
      <div class="cap-serie-sug" id="capSerieSugWrap">
        <i class="bi bi-lightbulb-fill"></i>
        Serie sugerida: <b>${_escapeHtml(serieSug)}</b>
        <button type="button" class="cap-serie-sug-btn"
                onclick="_capUsarSerieSugerida(${mid}, '${_escapeAttr(serieSug)}')">Usar</button>
      </div>` : '';
  const seccionDatosHtml = `
    <div class="captura-section">
      <h6 class="ttl"><i class="bi bi-pencil-fill"></i>Datos del equipo</h6>
      <div class="cap-form-grid">
        <div class="cap-field">
          <label for="cap-serie">Serial / N° Serie</label>
          <input id="cap-serie" type="text" name="serie"
            value="${_escapeAttr(e.serie || '')}"
            placeholder="${serieSug ? 'Sugerida: ' + _escapeAttr(serieSug) : 'Ej: SN-12345'}"
            maxlength="100" ${readonly ? 'readonly' : ''}
            onblur="_capFieldBlur(${mid}, 'serie', this.value)">
          <div class="save-status" id="cap-st-serie"></div>
          ${serieSugHtml}
        </div>
        <div class="cap-field">
          <label for="cap-marca">Marca</label>
          <input id="cap-marca" type="text" name="marca" list="capMarcasDL"
            value="${_escapeAttr(e.marca || '')}"
            placeholder="Ej: ILUS, Gymleco, Freemotion…"
            maxlength="120" ${readonly ? 'readonly' : ''}
            onfocus="_capCargarMarcas()"
            onblur="_capFieldBlur(${mid}, 'marca', _capNormalizarMarca(this))">
          <div class="save-status" id="cap-st-marca"></div>
          <datalist id="capMarcasDL"></datalist>
        </div>
        ${inp('modelo', 'Modelo', e.modelo, {maxlength:120, placeholder:'Ej: 95Ti'})}
        ${inp('anio_fabricacion', 'Año fabricación', e.anio_fabricacion,
              {type:'number', min:'1950', max:String(new Date().getFullYear()+2),
               placeholder:'Ej: 2022'})}
        ${inp('voltaje', 'Voltaje', e.voltaje, {maxlength:40, placeholder:'Ej: 220V / 50Hz'})}
        ${inp('ubicacion_sala', 'Ubicación en sala', e.ubicacion_sala,
              {maxlength:200, full:true, placeholder:'Ej: Sala cardio, pared norte'})}
      </div>
    </div>`;

  // ── SECCIÓN 3: Daño visible (toggle) ───────────────────────
  const tieneDano = !!(e.tiene_dano);
  const seccionDanoHtml = `
    <div class="captura-section">
      <h6 class="ttl"><i class="bi bi-exclamation-triangle-fill"></i>¿Encuentras daño visible?</h6>
      <div class="cap-toggle"
           id="cap-dano-toggle"
           data-active="${tieneDano ? '1' : '0'}"
           ${readonly ? '' : `onclick="_capToggleDano(${mid})"`}
           role="button" tabindex="0">
        <span class="sw"></span>
        <span class="lbl">${tieneDano ? 'Sí, el equipo tiene daño visible' : 'No, el equipo se ve sin daño'}
          <small>Tocá para cambiar</small>
        </span>
      </div>
      <div class="save-status" id="cap-st-tiene_dano"></div>
    </div>`;

  // ── SECCIÓN 3b: Plan de mantención ────────────────────────
  const _aplMant = (e.aplica_mantencion !== 0 && e.aplica_mantencion !== false && e.aplica_mantencion != null);
  const seccionAplicaMantHtml = `
    <div class="captura-section">
      <h6 class="ttl"><i class="bi bi-tools"></i>¿Aplica mantención?</h6>
      <div class="cap-toggle"
           id="cap-aplica-toggle"
           data-active="${_aplMant ? '1' : '0'}"
           data-mid="${mid}"
           ${readonly ? '' : `onclick="_capToggleAplica(${mid}, this)"`}
           role="button" tabindex="${readonly ? '-1' : '0'}">
        <span class="sw"></span>
        <span class="lbl">${_aplMant
          ? 'Sí — incluido en el plan de mantención'
          : 'No — accesorio o no aplica mantención'}
          <small>${readonly ? '' : 'Tocá para cambiar'}</small>
        </span>
      </div>
      <div class="save-status" id="cap-st-aplica_mantencion"></div>
    </div>`;

  // ── SECCIÓN 4: Observaciones del técnico ───────────────────
  const seccionObsHtml = `
    <div class="captura-section">
      <h6 class="ttl"><i class="bi bi-chat-square-text-fill"></i>Observaciones del técnico</h6>
      ${txtArea('observaciones', '', e.observaciones,
                {maxlength:5000,
                 placeholder:'Anota lo que encontraste: estado de uso, ruidos, partes faltantes, sugerencias, etc.'})}
      <div class="hint">Se guarda automáticamente al salir del campo.</div>
    </div>`;

  // ── SECCIÓN 5: Galería + uploader de fotos ─────────────────
  const tilesHtml = _renderFotosTiles(fotosData.fotos, mid);
  const seccionFotosHtml = `
    <div class="captura-section">
      <h6 class="ttl"><i class="bi bi-camera-fill"></i>Fotos del equipo</h6>
      <div class="cap-fotos-grid" id="cap-fotos-grid">
        ${tilesHtml}
      </div>
      <div class="hint">
        ${readonly
          ? 'Galería de fotos del equipo.'
          : (fotosData.foto_principal
              ? 'La primera foto del equipo ya está en la ficha. Las nuevas se agregan a la galería.'
              : 'La primera foto que subas se guardará como la foto principal de la ficha.')}
      </div>
    </div>`;

  // ── SECCIÓN 6: Marcar como completado (sticky bottom) ──────
  const seccionCompletadoHtml = readonly ? '' : `
    <div class="cap-completado-wrap">
      <button id="cap-completado-btn" type="button"
              class="cap-completado-btn"
              data-completado="0"
              onclick="_capMarcarCompletado(${mid})">
        <i class="bi bi-check2-square"></i>Marcar equipo como completado
      </button>
    </div>`;

  // ── SECCIÓN HISTÓRICA (read-only): contratos + OTs ─────────
  const contratosHtml = contratos.length
    ? `<div class="ficha-section-title"><i class="bi bi-file-earmark-text-fill"></i>Contratos del cliente</div>
       ${contratos.slice(0,5).map(c => `<div class="ficha-row" style="margin-bottom:5px">
          <div class="lbl">${_escapeHtml(c.estado || 'vigente')}${c.es_indefinido ? ' · indefinido' : ''}</div>
          <div class="val">${_escapeHtml(c.nombre || ('Contrato #' + c.id))}</div>
        </div>`).join('')}`
    : '';

  const otsHtml = ots.length
    ? `<div class="ficha-section-title"><i class="bi bi-clock-history"></i>Últimas OTs (${Math.min(ots.length, 5)})</div>
       <div class="ficha-ots">
         ${ots.slice(0,5).map(o => `<a class="ot-row" href="/mantenciones/ot/${o.id}" target="_blank" rel="noopener">
            <div>
              <strong>${_escapeHtml(o.numero_ot || ('OT #' + o.id))}</strong>
              <span class="ot-meta">· ${_escapeHtml(o.tipo || '')} · ${_escapeHtml(o.estado || '')}</span>
            </div>
            <div class="ot-meta">${_escapeHtml(String(o.fecha_programada || o.fecha || ''))}${o.tecnico_nombre || o.tecnico ? ' · ' + _escapeHtml(o.tecnico_nombre || o.tecnico) : ''}</div>
          </a>`).join('')}
       </div>`
    : '';

  return clienteHtml + bannerHtml + headerEquipoHtml +
         seccionEstadoHtml + seccionDatosHtml + seccionDanoHtml +
         seccionAplicaMantHtml + seccionObsHtml + seccionFotosHtml +
         contratosHtml + otsHtml +
         seccionCompletadoHtml;
}

// ════════════════════════════════════════════════════════
// CONCURRENCIA MULTITÉCNICO (Parte 3 - anti duplicación)
// ════════════════════════════════════════════════════════

// Toma el lock antes de editar/responder. Devuelve true si OK seguir.
// Si el lock está tomado por otro y no soy admin, muestra alerta y false.
async function tomarLockTarea(tid){
  if (IS_ADMIN_LOCK) return true;  // admin nunca pide lock (override)
  try {
    const r = await fetch(`/mantenciones/api/visitas/${VID}/tareas/${tid}/lock`, {
      method: 'POST', headers: {'Content-Type':'application/json'},
    });
    if (r.status === 423){
      const d = await r.json();
      await ilusAlert({
        title: 'Tarea ocupada',
        message: d.error || 'Otro técnico está respondiendo esta tarea.',
        sub: 'Espera unos minutos o coordínate con el técnico antes de continuar.',
        type: 'warning',
      });
      // Actualizar el lock local para que pinte el badge
      TAREAS_LOCKS[tid] = {
        locked_by_user_id: d.locked_by_user_id,
        locked_by_nombre: d.locked_by_nombre,
        locked_at: d.locked_at,
      };
      _refrescarTareaActual(tid);
      return false;
    }
    if (r.ok){
      const d = await r.json();
      // Limpiar lock local (yo soy el holder ahora — no me muestro badge)
      delete TAREAS_LOCKS[tid];
      return true;
    }
  } catch(e){
    // Si la red falla, seguimos optimistas (el version-check del POST nos protege).
  }
  return true;
}

// Refresca el render visual de una tarea (sin recargar nada del backend).
function _refrescarTareaActual(tid){
  if (!_state.mid || !_state.pid) return;
  const grupo = (PLANTILLAS_POR_MAQUINA[_state.mid] || [])
    .find(p => String(p.plantilla_id) === String(_state.pid));
  if (!grupo) return;
  const tar = grupo.tareas.find(t => t.id === tid);
  if (!tar) return;
  const oldEl = document.getElementById('tar-' + tid);
  if (!oldEl) return;
  const wrap = document.createElement('div');
  wrap.innerHTML = renderTareaHtml(tar, false, _state.mid, _state.pid);
  oldEl.replaceWith(wrap.firstElementChild);
}

// Polling periódico de locks vivos para pintar badges sin que el técnico
// tenga que recargar. Si la red falla, no rompe nada.
async function _refrescarLocks(){
  try {
    const r = await fetch(`/mantenciones/api/visitas/${VID}/tareas/locks`);
    if (!r.ok) return;
    const d = await r.json();
    if (!d.ok) return;
    const nuevos = {};
    (d.locks || []).forEach(l => {
      // Filtrar mis propios locks (no quiero ver candado en mis tareas)
      if (l.locked_by_user_id !== CURRENT_USER_ID){
        nuevos[l.tarea_id] = l;
      }
      // Actualizar version en el modelo local (para evitar 409 si otro guardó)
      Object.values(PLANTILLAS_POR_MAQUINA).forEach(plList => {
        plList.forEach(p => {
          const t = p.tareas.find(tt => tt.id === l.tarea_id);
          if (t && typeof l.version === 'number' && l.version > (t.version || 0)){
            t.version = l.version;
          }
        });
      });
    });
    // Detectar cambios
    const keysAntes = Object.keys(TAREAS_LOCKS).join(',');
    const keysNuevo = Object.keys(nuevos).join(',');
    TAREAS_LOCKS = nuevos;
    if (keysAntes !== keysNuevo && _state.view === 'tareas' && _state.mid && _state.pid){
      // Re-render la vista de tareas para reflejar nuevos badges
      renderTareas(_state.mid, _state.pid);
    }
  } catch(e){ /* silencioso */ }
}

// Arrancar polling cada 25s (cache 5min — barato).
if (typeof window !== 'undefined'){
  // Primer pull rápido, después cada 25s.
  setTimeout(_refrescarLocks, 1500);
  setInterval(_refrescarLocks, 25000);
}

// ════════════════════════════════════════════════════════
//  ACCIONES DE TAREAS (guardado + actualización in-memory)
// ════════════════════════════════════════════════════════
async function toggleCheck(tid, mid, pid){
  // Encontrar tarea en data
  const grupo = PLANTILLAS_POR_MAQUINA[mid].find(p => String(p.plantilla_id) === String(pid));
  const tar = grupo.tareas.find(t => t.id === tid);
  // ── Lock + version check (concurrencia multitécnico) ──
  if (!await tomarLockTarea(tid)) return;
  const newVal = !tar.completada;
  try {
    const r = await fetch(`/mantenciones/api/visitas/${VID}/tareas/${tid}`, {
      method: 'PATCH', headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ completada: newVal ? 1 : 0, version: tar.version || 0 })
    });
    if (r.status === 409){
      const d = await r.json();
      await ilusAlert({
        title: 'Cambio detectado',
        message: 'Otro técnico modificó esta tarea mientras la estabas viendo.',
        sub: 'La página se va a recargar para mostrarte la versión actual.',
        type: 'warning',
      });
      window.location.reload();
      return;
    }
    const d = await r.json();
    if (d.ok){
      tar.completada = newVal;
      if (typeof d.version === 'number') tar.version = d.version;
      _updateProgress(mid, pid);
      ilusToast(newVal ? '✓ Marcado' : '○ Desmarcado', { type:'success', duration: 1200 });
    } else {
      ilusToast('Error: ' + (d.error || '?'), { type:'error' });
    }
  } catch(e){ ilusToast('Error de red', { type:'error' }); }
}

async function guardarResp(tid, valor, mid, pid){
  // ── Lock + version check (concurrencia multitécnico) ──
  if (!await tomarLockTarea(tid)) return;
  const grupo = PLANTILLAS_POR_MAQUINA[mid].find(p => String(p.plantilla_id) === String(pid));
  const tar = grupo ? grupo.tareas.find(t => t.id === tid) : null;
  const ver = (tar && typeof tar.version === 'number') ? tar.version : 0;
  try {
    const r = await fetch(`/mantenciones/api/visitas/${VID}/tareas/${tid}/respuesta`, {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ valor, version: ver })
    });
    if (r.status === 409){
      const d = await r.json();
      await ilusAlert({
        title: 'Cambio detectado',
        message: 'Otro técnico modificó esta tarea mientras la estabas respondiendo.',
        sub: 'La página se va a recargar para mostrarte la versión actual y no perder tu trabajo siguiente.',
        type: 'warning',
      });
      window.location.reload();
      return;
    }
    const d = await r.json();
    if (d.ok){
      if (tar){
        tar.completada = d.completada;
        tar.valor_json = d.valor_norm;
        if (typeof d.version === 'number') tar.version = d.version;
      }
      _updateProgress(mid, pid);
      if (d.warning){
        ilusToast(d.warning, { type:'warning' });
      } else if (d.completada){
        ilusToast('✓ Guardado', { type:'success', duration: 1200 });
      }
    } else {
      ilusToast('Error: ' + (d.error || '?'), { type:'error' });
    }
  } catch(e){ ilusToast('Error de red', { type:'error' }); }
}

// ════════════════════════════════════════════════════════════════
//  TAREA TIPO 'texto' — Feedback en vivo + auto-completar al blur
//  ─────────────────────────────────────────────────────────────
//  Daniel (2026-05-18): "No encuentro que sea obligatorio. Igual
//  debería ser amigable — no sé cuántas letras debo marcar para
//  completar". Solución: contador en vivo + auto-marca al salir
//  del campo si supera el umbral (3 chars opcional, 10 obligatoria).
// ════════════════════════════════════════════════════════════════

// HTML del feedback (verde si completa, gris claro si no llega al mín)
function _textoFeedbackHtml(curLen, minChars){
  if (curLen >= minChars){
    return `<span style="color:#16a34a">
      <i class="bi bi-check-circle-fill"></i> Gestionada (${curLen} caracteres)
    </span>`;
  }
  const falta = minChars - curLen;
  return `<span style="color:#94a3b8">
    <i class="bi bi-pencil"></i> ${curLen} caracter${curLen === 1 ? '' : 'es'} ·
    <span style="color:#6b7280">Min ${minChars} para marcar como gestionada</span>
    ${falta > 0 ? `<span style="color:#b45309">(faltan ${falta})</span>` : ''}
  </span>`;
}

// Actualiza el contador en vivo mientras el técnico escribe
function _textoCharFeedback(inputEl, tid, minChars){
  const fb = document.getElementById('texto-fb-' + tid);
  if (!fb) return;
  const curLen = (inputEl.value || '').trim().length;
  fb.innerHTML = _textoFeedbackHtml(curLen, minChars);
}

// Al perder foco: si llega al umbral, guarda y marca completada
// (el backend valida igual el umbral para evitar trampas vía API)
function _textoOnBlur(inputEl, tid, mid, pid, minChars){
  const valor = (inputEl.value || '').trim();
  // Persistimos siempre (incluso si quedó corto, para no perder lo escrito).
  // El backend decidirá si marcar completada o no según el umbral.
  guardarResp(tid, valor, mid, pid);
  // Refrescar feedback con el largo actual
  _textoCharFeedback(inputEl, tid, minChars);
}

// ════════════════════════════════════════════════════════
//  VALIDACIÓN DE RADIO — distancia entre GPS y destino OT
//  (Base para bloqueo futuro: <200m del cliente = OK)
// ════════════════════════════════════════════════════════
const RADIO_MAX_METROS = 200;   // 200m — futuro: bloquear ejecución si excedido
const RADIO_BLOQUEO_ACTIVO = false;  // por ahora solo advierte, no bloquea

function _distanciaMetros(lat1, lng1, lat2, lng2){
  // Fórmula Haversine
  const R = 6371000;
  const toRad = (g) => g * Math.PI / 180;
  const dLat = toRad(lat2 - lat1);
  const dLng = toRad(lng2 - lng1);
  const a = Math.sin(dLat/2) ** 2 +
            Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) *
            Math.sin(dLng/2) ** 2;
  return Math.round(2 * R * Math.asin(Math.sqrt(a)));
}

async function _validarRadio(lat, lng){
  // Si la OT no tiene lat/lng de destino, no podemos validar
  if (!DESTINO_LAT || !DESTINO_LNG) return { ok:true, motivo:'sin_destino' };
  const dist = _distanciaMetros(lat, lng, DESTINO_LAT, DESTINO_LNG);
  if (dist <= RADIO_MAX_METROS) return { ok:true, dist };
  // Fuera de radio
  if (RADIO_BLOQUEO_ACTIVO){
    await ilusAlert({
      title: '📍 Fuera de zona',
      message: `Estás a <strong>${dist} m</strong> del cliente.<br>` +
               `La OT solo puede gestionarse a ${RADIO_MAX_METROS} m o menos.`,
      messageHtml: true,
      sub: 'Acércate al lugar exacto y vuelve a capturar el GPS.',
      type: 'warning',
    });
    return { ok:false, dist };
  }
  // Solo advertencia (no bloquea aún)
  ilusToast(`⚠ Estás a ${dist}m del cliente (max permitido: ${RADIO_MAX_METROS}m)`, {
    type: 'warning', duration: 4500,
  });
  return { ok:true, dist, fuera:true };
}

// ════════════════════════════════════════════════════════════════
//  GPS — MOTOR DE 5 ESTRATEGIAS EN CASCADA (iOS Safari bulletproof)
//  ─────────────────────────────────────────────────────────────
//  Estrategias en orden:
//    A) Pre-check con navigator.permissions (corta si denied)
//    B) getCurrentPosition con highAccuracy:true (timeout 8s)
//    C) getCurrentPosition con highAccuracy:false (timeout 5s) — workaround iOS
//    D) watchPosition + manual timeout (workaround iOS idle bug)
//    E) IP geolocation (~5km accuracy, fallback de emergencia)
//    F) Manual: técnico escribe la dirección (Google Places autocomplete)
//
//  Bugs conocidos iOS Safari 17/18 que esto resuelve:
//    1. Una vez denegado, NO se vuelve a preguntar (revisamos antes)
//    2. permissions.query puede retornar 'prompt' aunque esté denegado
//    3. enableHighAccuracy:true falla con "User denied" en iPhones nuevos
//       cuando con highAccuracy:false sí funciona → degradamos
//    4. getCurrentPosition se cuelga si la página estuvo idle → timeout corto
//    5. watchPosition a veces resuelve cuando getCurrentPosition se quedó colgado
//
//  Telemetría: window.__gpsDiag almacena qué estrategias se probaron y
//  cuál funcionó (visible en consola para debug)
// ════════════════════════════════════════════════════════════════

// Resultado central de GPS (usado por capturarGPS y pedirGPS)
// { lat, lng, accuracy, method: 'gps_high'|'gps_low'|'watch'|'ip'|'manual' }
window.__ilusGPS = window.__ilusGPS || null;
window.__gpsDiag = { attempts: [], ua: navigator.userAgent, https: location.protocol === 'https:' };

function _gpsLog(stage, info){
  try {
    window.__gpsDiag.attempts.push({ t: Date.now(), stage, info });
    console.log('[GPS]', stage, info || '');
  } catch(e) {}
}

// Wraps getCurrentPosition con timeout EXTERNO (iOS a veces no respeta el timeout interno)
function _gpsGetPosition(highAccuracy, timeoutMs){
  return new Promise((resolve, reject) => {
    let resolved = false;
    const externalTimer = setTimeout(() => {
      if (resolved) return;
      resolved = true;
      reject({ code: 3, message: 'External timeout (iOS hang protection)' });
    }, timeoutMs + 1500); // +1.5s buffer sobre el interno
    navigator.geolocation.getCurrentPosition(
      (pos) => {
        if (resolved) return;
        resolved = true;
        clearTimeout(externalTimer);
        resolve(pos);
      },
      (err) => {
        if (resolved) return;
        resolved = true;
        clearTimeout(externalTimer);
        reject(err);
      },
      { enableHighAccuracy: !!highAccuracy, timeout: timeoutMs, maximumAge: 30000 }
    );
  });
}

// watchPosition con timeout manual — fallback cuando getCurrentPosition se cuelga
function _gpsWatch(timeoutMs){
  return new Promise((resolve, reject) => {
    if (!navigator.geolocation.watchPosition){
      return reject({ code: 2, message: 'watchPosition no soportado' });
    }
    let resolved = false;
    let wid = null;
    const t = setTimeout(() => {
      if (resolved) return;
      resolved = true;
      if (wid !== null) try { navigator.geolocation.clearWatch(wid); } catch(e) {}
      reject({ code: 3, message: 'watchPosition timeout' });
    }, timeoutMs);
    try {
      wid = navigator.geolocation.watchPosition(
        (pos) => {
          if (resolved) return;
          resolved = true;
          clearTimeout(t);
          try { navigator.geolocation.clearWatch(wid); } catch(e) {}
          resolve(pos);
        },
        (err) => {
          if (resolved) return;
          resolved = true;
          clearTimeout(t);
          try { navigator.geolocation.clearWatch(wid); } catch(e) {}
          reject(err);
        },
        { enableHighAccuracy: false, timeout: timeoutMs, maximumAge: 60000 }
      );
    } catch(e){
      clearTimeout(t);
      reject({ code: 2, message: e.message || 'watch threw' });
    }
  });
}

// IP geolocation — último recurso de baja precisión (~5km)
async function _gpsByIP(){
  // Lista de servicios — iteramos por si alguno está caído
  const services = [
    {
      url: 'https://ipapi.co/json/',
      parse: (j) => (j && j.latitude && j.longitude)
        ? { lat: +j.latitude, lng: +j.longitude, dir: (j.city || '') + ', ' + (j.region || '') + ', ' + (j.country_name || '') }
        : null
    },
    {
      url: 'https://ipwho.is/',
      parse: (j) => (j && j.success !== false && j.latitude && j.longitude)
        ? { lat: +j.latitude, lng: +j.longitude, dir: (j.city || '') + ', ' + (j.region || '') + ', ' + (j.country || '') }
        : null
    },
  ];
  for (const svc of services){
    try {
      const ctrl = new AbortController();
      const tid = setTimeout(() => ctrl.abort(), 6000);
      const r = await fetch(svc.url, { signal: ctrl.signal });
      clearTimeout(tid);
      const j = await r.json();
      const out = svc.parse(j);
      if (out && isFinite(out.lat) && isFinite(out.lng)){
        return { ...out, accuracy: 5000, method: 'ip' };
      }
    } catch(e){
      _gpsLog('ip_service_fail', { url: svc.url, error: e.message });
    }
  }
  throw new Error('Todos los servicios de IP geolocation fallaron');
}

// ════════════════════════════════════════════════════════════════
//  Cascade NATIVO — devuelve { lat, lng, accuracy, method } o lanza
//  ─────────────────────────────────────────────────────────────────
//  FIX 2026-05-17 CRÍTICO PARA iOS Safari:
//   Antes esto hacía `await navigator.permissions.query(...)` ANTES de
//   llamar a getCurrentPosition. En iOS Safari, **cualquier await que
//   resuelve antes de getCurrentPosition rompe el contexto de gesto del
//   usuario** → el prompt nativo NUNCA aparecía o aparecía sin permisos.
//   Ahora vamos DIRECTO: el primer call es getCurrentPosition síncrono
//   dentro del mismo "tick" del evento click. Sin permissions.query
//   previo, sin awaits previos.
//  Estrategias en orden (todas SIN await previo):
//    A) getCurrentPosition(highAccuracy=true, 10s) — el que dispara prompt
//    B) getCurrentPosition(highAccuracy=false, 6s) — workaround iOS hang
//    C) watchPosition(8s) — fallback idle bug iOS
//  Si todos fallan con code=1 (denied), se propaga para que el caller
//  muestre el modal de ayuda. Cualquier otro error → fallback IP/manual.
// ════════════════════════════════════════════════════════════════
async function _gpsCascade(){
  if (!navigator.geolocation){
    const err = new Error('Geolocalización no soportada');
    err.code = 2;
    throw err;
  }

  // ⚠️ NO permissions.query acá. Ese await rompía el gesto en iOS.
  // En vez de eso, dejamos que el primer getCurrentPosition haga
  // el prompt nativo si nunca pidió, o devuelva error 1 si denied.

  // ── A) High accuracy (10s) — CALL DIRECTO desde gesto ──
  try {
    _gpsLog('try_high_accuracy');
    const pos = await _gpsGetPosition(true, 10000);
    _gpsLog('high_accuracy_ok', { acc: pos.coords.accuracy });
    return {
      lat: pos.coords.latitude,
      lng: pos.coords.longitude,
      accuracy: pos.coords.accuracy || null,
      method: 'gps_high'
    };
  } catch(e){
    _gpsLog('high_accuracy_fail', { code: e.code, msg: e.message });
    if (e.code === 1) throw e; // permission denied → no insistir
  }

  // ── B) Low accuracy (6s) — workaround iOS ──
  try {
    _gpsLog('try_low_accuracy');
    const pos = await _gpsGetPosition(false, 6000);
    _gpsLog('low_accuracy_ok', { acc: pos.coords.accuracy });
    return {
      lat: pos.coords.latitude,
      lng: pos.coords.longitude,
      accuracy: pos.coords.accuracy || null,
      method: 'gps_low'
    };
  } catch(e){
    _gpsLog('low_accuracy_fail', { code: e.code, msg: e.message });
    if (e.code === 1) throw e;
  }

  // ── C) watchPosition (8s) — fallback iOS idle bug ──
  try {
    _gpsLog('try_watch');
    const pos = await _gpsWatch(8000);
    _gpsLog('watch_ok', { acc: pos.coords.accuracy });
    return {
      lat: pos.coords.latitude,
      lng: pos.coords.longitude,
      accuracy: pos.coords.accuracy || null,
      method: 'watch'
    };
  } catch(e){
    _gpsLog('watch_fail', { code: e.code, msg: e.message });
    if (e.code === 1) throw e;
  }

  // Si llegamos acá, las 3 estrategias nativas fallaron sin "denied"
  const err = new Error('Todas las estrategias GPS nativas fallaron');
  err.code = 99;
  throw err;
}

// ════════════════════════════════════════════════════════════════
//  capturarGPS — entrada PRINCIPAL desde tareas
//  FIX 2026-05-17 (DEFINITIVO):
//   - Sin navigator.geolocation → modal manual DIRECTO (no más ilusAlert
//     que dejaba al técnico atascado sin opción de continuar).
//   - Cada paso loguea a window.__gpsDiag (visible en DevTools del iPhone
//     vía Safari → Develop → connect).
//   - Tras cualquier éxito (GPS/IP/manual), guarda en backend Y muestra
//     toast con método + coord exacta.
//   - Si el cascade falla con DENIED, ofrece "ver instrucciones" O ir
//     directo a manual (no se queda colgado en el modal de ayuda).
// ════════════════════════════════════════════════════════════════
async function capturarGPS(tid, mid, pid){
  // ═══════════════════════════════════════════════════════════════
  // POLÍTICA 2026-05-17 (Daniel) — Solo GPS REAL del dispositivo.
  // NO se acepta IP (no es certero) ni manual (vulnera auditoría).
  // Si el GPS está denegado, el usuario debe ir a Ajustes a habilitarlo.
  // ═══════════════════════════════════════════════════════════════
  //
  // FIX 2026-05-17 (DEFINITIVO iPhone 15 Pro Max + iOS 17/18):
  //   El timeout era 15s + highAccuracy:true sin fallback. En iPhones
  //   nuevos indoor, highAccuracy:true se cuelga sin error visible.
  //   Ahora:
  //     1. Primer intento highAccuracy:true con 10s (no 15s — el técnico
  //        cree que está colgado).
  //     2. Si TIMEOUT (code=3), segundo intento highAccuracy:false con 6s.
  //        El segundo call NO está bajo gesto, pero si el primero ya
  //        obtuvo el permiso, iOS no requiere gesto otra vez.
  //     3. Si permiso denegado → modal de ayuda (sin reintentar).
  //   El handler de éxito se extrae para que ambos intentos lo reusen.
  // ═══════════════════════════════════════════════════════════════
  _gpsLog('tarea_capturar_start', { tid, mid, pid });
  if (!navigator.geolocation){
    _gpsLog('tarea_no_geolocation_api');
    await ilusAlert({
      title: 'GPS no disponible',
      message: 'Tu navegador no soporta geolocalización. ' +
               'Usa Safari o Chrome actualizado.',
      type: 'error',
    });
    return;
  }
  // ⚠️ CRÍTICO iOS Safari: el getCurrentPosition() debe disparar dentro
  // del MISMO macrotask del click. Cualquier await previo a ese call
  // rompe el contexto de gesto y iOS NO muestra el prompt. Por eso
  // ilusToast (que es sync) está OK, pero NO podemos meter awaits acá.
  ilusToast('📡 Pidiendo GPS…', { type:'info', duration: 1500 });

  // Handler de éxito — reusado por highAccuracy:true Y por el fallback false
  const _onSuccess = async (pos, methodTag) => {
    const lat = pos.coords.latitude;
    const lng = pos.coords.longitude;
    const acc = pos.coords.accuracy || null;
    _gpsLog('tarea_gps_ok', { lat, lng, accuracy: acc, method: methodTag });
    // Validar precisión razonable (GPS de iPhone suele dar <50m al aire libre,
    // hasta 100-200m en edificios. Si excede 500m, sospechoso → rechazar).
    if (acc && acc > 500){
      _gpsLog('tarea_gps_accuracy_too_low', { acc });
      await ilusAlert({
        title: 'GPS impreciso',
        message: `La precisión del GPS es de ±${Math.round(acc)} m, ` +
                 'demasiado baja para auditar tu posición.',
        sub: 'Sal al aire libre (lejos de paredes/metales) y reintenta.',
        type: 'warning',
      });
      return;
    }
    ilusToast(`📍 GPS: ${lat.toFixed(5)}, ${lng.toFixed(5)} ±${Math.round(acc||0)}m`, {
      type: 'success', duration: 3500,
    });
    try { await _validarRadio(lat, lng); } catch(_){}
    await _gpsGuardarEnTarea(tid, mid, pid, {
      lat, lng, accuracy: acc, source: 'gps',
    });
    // Refrescar barra GPS arriba
    try { await _gpsRegistrarResultado({ lat, lng, accuracy: acc, method: methodTag }); } catch(_){}
  };

  // Handler de error final — solo se llama cuando ya no hay más fallbacks
  const _onErrorFinal = async (err) => {
    _gpsLog('tarea_gps_error_final', { code: err && err.code, msg: err && err.message });
    if (err.code === 1){
      // PERMISSION_DENIED → modal claro con instrucciones iPhone + Reintentar
      const reintentar = await ilusConfirm({
        title: '🔒 GPS bloqueado en este iPhone',
        message: 'Para auditar tu posición necesitamos que actives el GPS en Ajustes. ' +
                 'Toca <strong>"Ver cómo activar"</strong> para ver el paso a paso.',
        messageHtml: true,
        sub: 'Una vez activado, vuelve aquí y toca "Capturar mi ubicación" otra vez.',
        type: 'warning',
        okLabel: 'Ver cómo activar',
        cancelLabel: 'Después',
        danger: false,
      });
      if (reintentar){ mostrarAyudaGPS(); }
      return;
    }
    if (err.code === 2){
      await ilusAlert({
        title: 'GPS no disponible',
        message: 'Tu iPhone no puede obtener señal GPS ahora.',
        sub: 'Verifica que los Servicios de Ubicación estén ON ' +
             '(Ajustes → Privacidad → Servicios de Ubicación) ' +
             'y sal al aire libre. Después toca "Capturar" otra vez.',
        type: 'error',
      });
      return;
    }
    if (err.code === 3){
      await ilusAlert({
        title: 'GPS tardó demasiado',
        message: 'No respondió a tiempo. Prueba otra vez en un lugar con vista al cielo.',
        type: 'warning',
      });
      return;
    }
    // Otro error
    await ilusAlert({
      title: 'Error GPS',
      message: err.message || 'Error desconocido al obtener ubicación.',
      type: 'error',
    });
  };

  // getCurrentPosition DIRECTO — el prompt nativo iOS aparece acá si
  // nunca preguntó antes. Si ya está granted, resuelve directo.
  // Timeout 10s (no 15s) para que el técnico no piense que está colgado.
  navigator.geolocation.getCurrentPosition(
    (pos) => _onSuccess(pos, 'gps_high'),
    (err) => {
      _gpsLog('tarea_gps_high_fail', { code: err && err.code, msg: err && err.message });
      // Si fue PERMISSION_DENIED → no insistir, ir directo al modal
      if (err && err.code === 1){
        _onErrorFinal(err);
        return;
      }
      // TIMEOUT o POSITION_UNAVAILABLE → fallback a highAccuracy:false (6s).
      // Funciona en iPhone 15 Pro Max indoor cuando highAccuracy:true falla.
      // No requiere gesto nuevo porque ya tenemos el permiso del primer intento.
      _gpsLog('tarea_gps_fallback_low_accuracy');
      ilusToast('📡 Reintentando GPS (precisión estándar)…',
                { type:'info', duration: 1500 });
      navigator.geolocation.getCurrentPosition(
        (pos) => _onSuccess(pos, 'gps_low'),
        (err2) => _onErrorFinal(err2),
        { enableHighAccuracy: false, timeout: 6000, maximumAge: 60000 }
      );
    },
    { enableHighAccuracy: true, timeout: 10000, maximumAge: 30000 }
  );
}

// ─── DEPRECADO 2026-05-17 ───────────────────────────────────────
// Las funciones capturarGPSPorIP y capturarGPSManual quedaron sin
// invocación desde la UI (botones eliminados) por política de Daniel:
// solo GPS real del dispositivo. Las dejo aquí marcadas como deprecated
// por si en el futuro Daniel quiere reactivar el flujo manual con
// alguna salvaguarda (ej. requerir aprobación supervisor para entries
// manuales). NO BORRAR sin discutir.
async function capturarGPSPorIP(tid, mid, pid){
  console.warn('[GPS] capturarGPSPorIP DEPRECATED — solo GPS real permitido');
  return;
}
async function capturarGPSManual(tid, mid, pid){
  console.warn('[GPS] capturarGPSManual DEPRECATED — solo GPS real permitido');
  return;
}

// ─── (resto del código manual histórico, ya no invocado) ────────
async function _capturarGPSManual_DEPRECATED(tid, mid, pid){
  _gpsLog('tarea_manual_start', { tid });
  const m = await abrirManualGPS();
  if (!m){ _gpsLog('tarea_manual_cancelado'); return; }
  _gpsLog('tarea_manual_ok', { lat: m.lat, lng: m.lng, dir: m.dir });
  try { await _validarRadio(m.lat, m.lng); } catch(_){}
  ilusToast(`📍 Manual: ${m.dir || (m.lat.toFixed(5) + ', ' + m.lng.toFixed(5))}`,
            { type:'success', duration: 3500 });
  await _gpsGuardarEnTarea(tid, mid, pid, {
    lat: m.lat, lng: m.lng,
    accuracy: m.accuracy || 50,
    source: 'manual', dir: m.dir || null,
  });
}

// ── Guardar en backend con metadata robusta ──
// Si la tarea es de tipo GPS → manda {lat, lng, accuracy, source, dir}
// Si NO es tipo GPS → manda {_gps_extra:{...}} para que backend lo agregue
// como metadata sin pisar el valor principal de la tarea.
async function _gpsGuardarEnTarea(tid, mid, pid, payload){
  let esTipoGps = false;
  try {
    const grupo = PLANTILLAS_POR_MAQUINA[mid].find(p => String(p.plantilla_id) === String(pid));
    if (grupo){
      const tar = grupo.tareas.find(t => t.id === tid);
      const tipoNorm = String(tar && tar.tipo_respuesta || '').trim().toLowerCase();
      esTipoGps = (tipoNorm === 'gps');
    }
  } catch(e) { _gpsLog('check_tipo_fail', e.message); }
  _gpsLog('guardar_tarea', { tid, esTipoGps, source: payload.source });
  const valorPayload = esTipoGps ? {
    lat: payload.lat, lng: payload.lng,
    accuracy: payload.accuracy, source: payload.source,
    dir: payload.dir || null,
  } : {
    _gps_extra: {
      lat: payload.lat, lng: payload.lng,
      accuracy: payload.accuracy, source: payload.source,
      dir: payload.dir || null,
    },
  };
  await guardarResp(tid, valorPayload, mid, pid);
  setTimeout(() => renderTareas(mid, pid), 600);
}

function _gpsMethodLabel(method){
  switch(method){
    case 'gps_high': return 'GPS exacto';
    case 'gps_low':  return 'GPS estándar';
    case 'watch':    return 'GPS (modo continuo)';
    case 'ip':       return 'Aproximada por IP (~5km)';
    case 'manual':   return 'Ingresada manualmente';
    default:         return 'Capturada';
  }
}

// ════════════════════════════════════════════════════════
//  SHEET DE FOTO — 1 botón abre menú Cámara/Galería
//  (fix mobile UX 2026-05-17: antes eran 2 botones stacked,
//  confusos. Ahora 1 botón rojo grande → sheet con elección).
// ════════════════════════════════════════════════════════
async function abrirSheetFoto(tid, mid, pid){
  if (typeof ilusActionSheet !== 'function'){
    // Fallback defensivo si ilus_ui.js no cargó por alguna razón
    document.getElementById('fotoGal-' + tid)?.click();
    return;
  }
  const choice = await ilusActionSheet({
    title: '📷 Agregar foto',
    message: 'Elige cómo quieres agregar la foto',
    options: [
      { label: 'Tomar foto con cámara', icon: 'bi-camera-fill',   value: 'camara',  style: 'dark' },
      { label: 'Elegir de galería',      icon: 'bi-images',         value: 'galeria', style: 'secondary' },
    ],
  });
  if (!choice) return;  // canceló
  // Disparo el click sobre el input correspondiente
  const inputId = choice === 'camara' ? `fotoCam-${tid}` : `fotoGal-${tid}`;
  const inp = document.getElementById(inputId);
  if (!inp){
    ilusToast('Error: no se encontró el input de foto', { type:'error' });
    return;
  }
  // Reset value para que el mismo archivo dispare onchange si lo eligen 2 veces
  try { inp.value = ''; } catch(e){}
  inp.click();
}

// ════════════════════════════════════════════════════════
//  SUBIDA DE FOTO — robusto con HEIC, logging detallado,
//  retry en error de red, y mensajes de error precisos.
//  (fix 2026-05-17: HEIC iOS rompía canvas decode y el
//  backend rechazaba el formato → "Error de red" silencioso.)
// ════════════════════════════════════════════════════════
function _esHeic(file){
  const name = (file.name || '').toLowerCase();
  const mime = (file.type || '').toLowerCase();
  return name.endsWith('.heic') || name.endsWith('.heif')
      || mime === 'image/heic' || mime === 'image/heif';
}

async function _subirFotoIntento(fd, intento){
  // Un solo intento — retorna {ok, status, data, networkError}
  try {
    const r = await fetch(`/mantenciones/api/visitas/${VID}/fotos/subir`, {
      method:'POST', body: fd,
    });
    let data = null;
    try { data = await r.json(); } catch(parseErr){
      // El backend devolvió no-JSON (HTML de error 500 de Flask, etc.)
      const txt = await r.text().catch(() => '');
      console.error('[subirFoto] respuesta NO-JSON', { status: r.status, body: txt.slice(0, 500) });
      data = { ok: false, error: `Respuesta inválida del servidor (HTTP ${r.status})` };
    }
    return { ok: r.ok && (data.ok || data.id), status: r.status, data, networkError: false };
  } catch(e){
    console.error('[subirFoto] network error intento ' + intento, e);
    return { ok: false, status: 0, data: null, networkError: true, exception: e };
  }
}

async function subirFotoTarea(tid, input, mid, pid){
  if (!input.files || !input.files[0]) return;
  let file = input.files[0];
  const sizeMB = (file.size / (1024*1024)).toFixed(2);
  console.log('[subirFoto] archivo:', { name: file.name, type: file.type, size: file.size, sizeMB, tid, mid, pid });

  // Validación de tamaño TEMPRANA — backend acepta máx 8MB; 20MB es la cota suave por si comprime
  if (file.size > 20 * 1024 * 1024){
    ilusToast(`Foto demasiado grande (${sizeMB} MB). Máx 20 MB.`, { type:'warning' });
    return;
  }

  // HEIC/HEIF (iOS): canvas decode no funciona en la mayoría de browsers
  // → enviamos el archivo crudo, el backend ya soporta esos formatos.
  const heic = _esHeic(file);
  if (heic){
    console.log('[subirFoto] HEIC detectado → skip compresión client-side');
  } else {
    try {
      file = await _comprimirImagen(file);
      console.log('[subirFoto] comprimido:', { name: file.name, type: file.type, size: file.size });
    } catch(e){
      console.warn('[subirFoto] compresión falló, mando original:', e.message);
    }
  }

  const fd = new FormData();
  fd.append('foto', file);
  fd.append('tarea_id', String(tid));
  // 2026-05-19 (Daniel) — vínculo formal foto→máquina para que al cerrar
  // un levantamiento, la foto se asigne automáticamente a la ficha técnica.
  // Si la OT es tipo `levantamiento`, además marcamos tipo_foto para la
  // promoción posterior (ver _promover_levantamiento_a_maquina).
  if (mid) fd.append('maquina_id', String(mid));
  if (VISITA_TIPO === 'levantamiento') fd.append('tipo_foto', 'levantamiento');

  const toastSub = ilusToast('Subiendo foto…', { type:'info', duration: 6000 });

  // Intento 1
  let res = await _subirFotoIntento(fd, 1);

  // Reintentar UNA VEZ si fue network error (no 4xx/5xx — esos son errores del backend)
  if (!res.ok && res.networkError){
    console.log('[subirFoto] reintentando (1/1) tras network error...');
    await new Promise(r => setTimeout(r, 800));
    res = await _subirFotoIntento(fd, 2);
  }

  if (toastSub && toastSub.close) toastSub.close();

  if (res.ok){
    ilusToast('✓ Foto subida', { type:'success' });
    try {
      await fetch(`/mantenciones/api/visitas/${VID}/tareas/${tid}`, {
        method: 'PATCH', headers: {'Content-Type':'application/json'},
        body: JSON.stringify({ completada: 1 })
      });
      const grupo = PLANTILLAS_POR_MAQUINA[mid].find(p => String(p.plantilla_id) === String(pid));
      const tar = grupo.tareas.find(t => t.id === tid);
      if (tar){ tar.completada = 1; _updateProgress(mid, pid); }
    } catch(e){
      console.warn('[subirFoto] marcar completada falló:', e);
    }
    const url = (res.data && (res.data.url || res.data.cloudinary_url)) || '';
    if (url){
      const cont = document.getElementById('t-fotos-' + tid);
      // PERF 2026-05-22: 'card' (≤400px) — preview inline después de subir.
      if (cont) cont.innerHTML += `<img src="${_escapeAttr(cloudTx(url, 'card'))}" alt=""
            loading="lazy" decoding="async"
            style="max-width:100%;border-radius:8px;margin-top:6px">`;
    }
    return;
  }

  // ── ERROR ──
  let msg = 'No se pudo subir la foto';
  if (res.networkError){
    msg = 'Sin conexión. Verifica tu internet e intenta de nuevo.';
  } else if (res.data && res.data.error){
    msg = res.data.error;
    // Si fue por formato no permitido y es HEIC, dar un hint claro al técnico
    if (heic && /formato/i.test(res.data.error)){
      msg = 'El servidor rechazó el formato HEIC. Prueba tomar la foto de nuevo (la app debería convertirla a JPG).';
    }
  } else if (res.status >= 500){
    msg = `Error del servidor (HTTP ${res.status}). Reintenta en unos segundos.`;
  } else if (res.status >= 400){
    msg = `Error en el envío (HTTP ${res.status}). Verifica el archivo.`;
  }
  console.error('[subirFoto] FAIL:', { status: res.status, data: res.data, networkError: res.networkError });
  ilusToast(msg, { type:'error', duration: 6000 });
}

function _updateProgress(mid, pid){
  // Recalcular grupo
  const grupo = PLANTILLAS_POR_MAQUINA[mid].find(p => String(p.plantilla_id) === String(pid));
  grupo.completas = grupo.tareas.filter(t => t.completada).length;
  grupo.progreso = grupo.total ? Math.round((grupo.completas / grupo.total) * 100) : 0;
  // 2026-06-12 (Daniel, estilo Fracttal): checklist completo NO se auto-bloquea.
  // El técnico corrige hasta la firma del CLIENTE (sello real = estado + permisos).
  grupo.bloqueado = false;

  // Update vista 3 si está activa
  const tProg = document.getElementById('tProg');
  const tProgBar = document.getElementById('tProgBar');
  if (tProg) tProg.textContent = grupo.completas;
  if (tProgBar) tProgBar.style.width = grupo.progreso + '%';

  // Marcar tarea como done visualmente
  document.querySelectorAll('.tx-tarea').forEach(el => {
    const tid = parseInt(el.id.replace('tar-', ''));
    const t = grupo.tareas.find(x => x.id === tid);
    if (t){
      el.classList.toggle('done', !!t.completada);
      const chk = el.querySelector('.ttl-chk');
      if (chk) chk.innerHTML = t.completada ? '<i class="bi bi-check-lg"></i>' : '';
    }
  });

  // Recalcular stats máquina
  const pls = PLANTILLAS_POR_MAQUINA[mid];
  let totM = 0, compM = 0;
  pls.forEach(p => { totM += p.total; compM += p.completas; });
  STATS_POR_MAQUINA[mid] = {
    total: totM, completas: compM,
    progreso: totM ? Math.round((compM / totM) * 100) : 0,
    bloqueada: (compM === totM && totM > 0),
    n_plantillas: pls.length,
  };

  // Update stats globales
  // 2026-05-18 (Mejora UX firma): además de total/completas globales, sacamos
  // contadores de OBLIGATORIAS — son las que destraban el botón firmar.
  const ctxG = _calcCtxGlobal();
  document.getElementById('statTot').textContent = ctxG.total;
  document.getElementById('statComp').textContent = ctxG.completas;
  const elOT = document.getElementById('statOblTot');
  const elOC = document.getElementById('statOblComp');
  if (elOT) elOT.textContent = ctxG.oblTot;
  if (elOC) elOC.textContent = ctxG.oblComp;

  // ⚠️ Lock/unlock botón "Completar y firmar OT" según obligatorias
  actualizarLockFirmar(ctxG);
}

// Calcula contadores globales: total/completas y obligatorias/completas_obligatorias.
// Centralizado para no duplicar el filtro t.obligatoria en varios lugares.
// 2026-05-21 (Daniel) — EXCLUYE tareas de equipos en estado_revision
// 'saltado' o 'falla_detectada': esos equipos no deben bloquear el cierre.
function _calcCtxGlobal(){
  let total = 0, completas = 0, oblTot = 0, oblComp = 0, nExcluidos = 0;
  Object.entries(PLANTILLAS_POR_MAQUINA).forEach(([midStr, pls]) => {
    const rev = (EQUIPOS_ESTADO_REVISION || {})[midStr] || {};
    const _excluir = (rev.estado_revision === 'saltado' ||
                      rev.estado_revision === 'falla_detectada');
    if (_excluir){ nExcluidos++; return; }
    // Tareas HUÉRFANAS (maquina_id NULL -> midStr '0', sin tarjeta de equipo)
    // en una OT que captura fichas: el técnico no puede abrirlas ni subirles
    // foto, así que no cuentan para el candado. MISMO criterio que el backend
    // (_ot_validar_cierre R1/R3 y el gate de firma) para que el número de la
    // pantalla y el del servidor nunca vuelvan a divergir.
    if (!(EQUIPOS_IDX || {})[midStr] && (typeof ES_LEVANTAMIENTO !== 'undefined') && ES_LEVANTAMIENTO) return;
    pls.forEach(p => {
      total += p.total;
      completas += p.completas;
      (p.tareas || []).forEach(t => {
        if (t.obligatoria){
          oblTot++;
          if (t.completada) oblComp++;
        }
      });
    });
  });
  // nExcluidos: cuántos equipos quedaron fuera por estar gestionados
  // (saltado / falla_detectada). Hace falta para distinguir "esta OT no
  // tiene tareas" de "todas sus tareas están cubiertas porque el técnico
  // gestionó todos los equipos" — dos situaciones que daban total=0 y que
  // el botón de firmar trataba igual, dejándolo bloqueado para siempre.
  return { total, completas, oblTot, oblComp, nExcluidos };
}

// 2026-08-08 (Daniel: "me gustaría que me indicara por equipo qué está
// pendiente... no sé qué es lo que adeudo"). Mismo filtro de exclusión que
// _calcCtxGlobal (equipos 'saltado'/'falla_detectada' no cuentan) para que
// el desglose sea consistente con el contador de arriba.
function _pendientesPorEquipo(){
  const out = [];
  Object.entries(PLANTILLAS_POR_MAQUINA || {}).forEach(([midStr, pls]) => {
    const rev = (EQUIPOS_ESTADO_REVISION || {})[midStr] || {};
    const _excluir = (rev.estado_revision === 'saltado' || rev.estado_revision === 'falla_detectada');
    if (_excluir) return;

    const eq = (EQUIPOS_IDX || {})[midStr];
    // maquina_id 0/NULL = tareas HUERFANAS (no ligadas a ningun equipo). Caso
    // real OT-2026-00056: 18 tareas obligatorias con el nombre del equipo solo
    // en el titulo ("Inspeccion: <equipo>") y sin FK. Agrupar todas bajo
    // "Equipo #0" no le sirve a nadie; en ese caso listamos por TITULO, que es
    // donde realmente esta el nombre del equipo.
    if (!eq){
      (pls || []).forEach(p => (p.tareas || []).forEach(t => {
        if (t.obligatoria && !t.completada){
          const nom = String(t.titulo || 'Tarea sin titulo')
            .replace(/^[^\wÀ-ɏ]+/, '')            // saca el emoji inicial
            .replace(/^(Inspeccion|Inspección|Documentar)\s*:\s*/i, '');
          out.push({ nombre: nom, pend: 1, huerfana: true });
        }
      }));
      return;
    }

    let pend = 0;
    (pls || []).forEach(p => (p.tareas || []).forEach(t => {
      if (t.obligatoria && !t.completada) pend++;
    }));
    if (pend > 0) out.push({ nombre: eq.nombre || `Equipo #${midStr}`, pend, huerfana: false });
  });
  return out.sort((a, b) => a.nombre.localeCompare(b.nombre));
}

// Pinta el desglose por equipo bajo el botón Firmar, y oculta "Agregar
// equipo"/"Duplicar último" SOLO al técnico una vez que las obligatorias
// están listas (Daniel: "quitar ese botón al técnico... si la OT para él
// debe ser cerrada" — evita que reabra la OT agregando equipos justo
// cuando está por firmarla; otros roles conservan el botón siempre).
// 2026-08-09 (Daniel: "un tracking espectacular y dinámico" en el header).
// Reusa EXACTAMENTE los mismos oblTot/oblComp que ya calcula
// _calcCtxGlobal() para el candado de "Firmar OT" -- nunca un cálculo
// paralelo que se pueda desincronizar del número real. Sin obligatorias
// (OT recién creada, o levantamiento puro sin checklist todavía), la
// barra se queda oculta: un "0%" ahí no le dice nada útil a nadie.
function _hdrSyncProgress(oblTot, oblComp){
  const wrap = document.getElementById('hdrProgressWrap');
  if (!wrap) return;
  if (!oblTot){ wrap.style.display = 'none'; return; }
  const pct = Math.max(0, Math.min(100, Math.round((oblComp / oblTot) * 100)));
  wrap.style.display = '';
  wrap.classList.toggle('is-completa', pct >= 100);
  const fill = document.getElementById('hdrProgressFill');
  const pctEl = document.getElementById('hdrProgressPct');
  const lblEl = document.getElementById('hdrProgressLbl');
  if (fill) fill.style.width = pct + '%';
  if (pctEl) pctEl.textContent = pct + '%';
  if (lblEl) lblEl.textContent = pct >= 100 ? 'Obligatorias completas' : `Avance · ${oblComp}/${oblTot}`;
}

function _actualizarPanelCierre(oblTot, oblComp){
  // 2026-08-08 (Daniel): "esa lista igual no me gusta, prefiero algo mas
  // sutil". Antes se listaban aquí uno por uno los equipos pendientes. Ahora
  // la señal por equipo es el SEMÁFORO de la franja izquierda de su tarjeta
  // (ver levdRender), y acá queda solo el resumen de una línea.
  const detalle = document.getElementById('btnFirmarDetalle');
  if (detalle){
    const pend = (oblComp < oblTot) ? _pendientesPorEquipo() : [];
    if (pend.length){
      detalle.style.display = '';
      detalle.innerHTML = '<div class="small" style="color:#b45309">' +
        `<i class="bi bi-circle-fill me-1" style="font-size:.55rem;color:#dc2626"></i>` +
        `${pend.length} equipo${pend.length>1?'s':''} con algo pendiente — míralos por la franja roja de su tarjeta.</div>`;
    } else {
      detalle.style.display = 'none';
      detalle.innerHTML = '';
    }
  }

  if (typeof IS_TECNICO !== 'undefined' && IS_TECNICO){
    // OJO (bug encontrado en la revisión adversarial 2026-08-08, antes de
    // llegar a producción): la condición NO puede ser "oblTot === 0", porque
    // en un LEVANTAMIENTO PURO de descubrimiento la OT nace con 0 tareas —
    // le escondíamos al técnico el botón "Agregar equipo" justo cuando
    // agregar equipos ES todo su trabajo, y el botón de firmar le exige
    // al menos 1 equipo: deadlock en terreno, exactamente lo que hay que
    // evitar. Solo se oculta cuando SÍ había obligatorias y ya están todas
    // cubiertas, que es el caso que pidió Daniel.
    const listoParaCerrar = (oblTot > 0) && (oblComp >= oblTot);
    document.querySelectorAll('.levd-add, #levdDupBtn').forEach(el => {
      if (!el) return;
      // Respeta el estado propio del botón Duplicar (se muestra/oculta según
      // haya un último equipo que duplicar) — solo lo forzamos a ocultarse,
      // nunca a mostrarse si su propia lógica ya lo tenía oculto.
      if (listoParaCerrar) el.style.display = 'none';
      else if (el.classList.contains('levd-add')) el.style.display = '';
    });
  }
}

// ════════════════════════════════════════════════════════
//  LOCK BOTÓN FIRMAR
//  2026-05-18 (Mejora UX): el botón se habilita cuando las
//  OBLIGATORIAS están completas. Si hay opcionales pendientes,
//  el botón sigue habilitado pero con hint ámbar amistoso.
//
//  Argumento: ctx = {total, completas, oblTot, oblComp}
//  (compat: si llega como 2 args sueltos, los normalizamos).
// ════════════════════════════════════════════════════════
function actualizarLockFirmar(ctxOrTotal, completas){
  // Backwards-compat: algunos callers viejos pasaban (total, completas) sueltos.
  let ctx;
  if (typeof ctxOrTotal === 'object' && ctxOrTotal !== null){
    ctx = ctxOrTotal;
  } else {
    ctx = { total: ctxOrTotal || 0, completas: completas || 0, oblTot: 0, oblComp: 0 };
  }
  const total    = ctx.total    | 0;
  const compl    = ctx.completas| 0;
  const oblTot   = ctx.oblTot   | 0;
  const oblComp  = ctx.oblComp  | 0;
  const opcTot   = total - oblTot;
  const opcComp  = compl - oblComp;
  const opcPend  = Math.max(0, opcTot - opcComp);

  const btn = document.getElementById('btnFirmar');
  const lbl = document.getElementById('btnFirmarLabel');
  const hint = document.getElementById('btnFirmarHint');
  if (!btn || !lbl) return;

  // Desglose por equipo + visibilidad de "Agregar equipo" para el técnico.
  // Va acá arriba (antes de los returns tempranos) para que se recalcule
  // en TODOS los casos, no solo en el camino feliz.
  _actualizarPanelCierre(oblTot, oblComp);
  _hdrSyncProgress(oblTot, oblComp);

  // CSS shared snippets para el hint inferior (siempre visible)
  const hintCounters =
    `<span style="color:#15803d;font-weight:700">Obligatorias: ${oblComp}/${oblTot}</span> ` +
    `<span style="color:#94a3b8">·</span> ` +
    `<span style="color:#6b7280;font-weight:600">Opcionales: ${opcComp}/${opcTot}</span>`;

  if (total === 0){
    // 2026-07-06 (fix Daniel — choque de reglas): en LEVANTAMIENTO PURO de
    // descubrimiento no hay equipos clásicos (EQUIPOS.length===0) ni
    // PLANTILLAS_POR_MAQUINA, así que `total` siempre da 0 aunque el técnico
    // ya haya descubierto equipos en terreno (_levdItems). Ahí el gate real
    // es "¿ya capturó al menos 1 equipo?", no el conteo de tareas clásicas.
    const _esDescubrimientoPuro = ES_LEVANTAMIENTO && (typeof EQUIPOS !== 'undefined') && EQUIPOS.length === 0;
    if (_esDescubrimientoPuro){
      const nDesc = (typeof _levdItems !== 'undefined') ? _levdItems.length : 0;
      if (nDesc > 0){
        btn.disabled = false;
        btn.dataset.locked = '0';
        btn.innerHTML = '<i class="bi bi-pen-fill"></i> <span id="btnFirmarLabel">' +
          `Completar y firmar OT — ${nDesc} equipo${nDesc>1?'s':''} descubierto${nDesc>1?'s':''} documentado${nDesc>1?'s':''}</span>`;
        btn.style.background = 'linear-gradient(135deg,#15803d,#16a34a)';
        if (hint){
          hint.innerHTML = `${nDesc} equipo${nDesc>1?'s':''} descubierto${nDesc>1?'s':''} documentado${nDesc>1?'s':''}. Puedes firmar cuando termines.`;
          hint.style.color = '#15803d';
        }
        return;
      }
      btn.disabled = true;
      btn.dataset.locked = '1';
      btn.innerHTML = '<i class="bi bi-lock-fill"></i> <span id="btnFirmarLabel">Agrega al menos 1 equipo</span>';
      btn.style.background = '#cbd5e1';
      if (hint){
        hint.innerHTML = 'Agrega al menos 1 equipo descubierto en terreno antes de firmar.';
        hint.style.color = '#94a3b8';
      }
      return;
    }
    // BUG REAL corregido 2026-08-08 (detectado en la auditoría del caso OT-56):
    // si el técnico gestionó TODOS los equipos (saltado / falla detectada),
    // _calcCtxGlobal deja total=0 y este branch le decía "Sin tareas
    // asignadas" con el botón deshabilitado PARA SIEMPRE — aunque el backend
    // sí lo dejaba firmar (su gate excluye las tareas de esos equipos). El
    // técnico quedaba trabado en terreno habiendo hecho todo bien.
    const _nExcl = ctx.nExcluidos | 0;
    if (_nExcl > 0){
      btn.disabled = false;
      btn.dataset.locked = '0';
      btn.innerHTML = '<i class="bi bi-pen-fill"></i> <span id="btnFirmarLabel">Completar y firmar OT</span>';
      btn.style.background = 'linear-gradient(135deg,#15803d,#16a34a)';
      if (hint){
        hint.innerHTML = `Gestionaste ${_nExcl} equipo${_nExcl>1?'s':''} (saltado o con falla). ` +
                         'No queda nada pendiente: puedes firmar.';
        hint.style.color = '#15803d';
      }
      return;
    }
    btn.disabled = true;
    btn.dataset.locked = '1';
    btn.innerHTML = '<i class="bi bi-lock-fill"></i> <span id="btnFirmarLabel">Sin tareas asignadas</span>';
    btn.style.background = '#cbd5e1';
    if (hint){
      hint.innerHTML = 'La OT no tiene tareas. Pide al administrador que revise el levantamiento.';
      hint.style.color = '#94a3b8';
    }
    return;
  }

  // Caso 1: NO HAY OBLIGATORIAS → desbloqueado siempre que haya al menos 1 tarea
  if (oblTot === 0){
    btn.disabled = false;
    btn.dataset.locked = '0';
    btn.innerHTML = '<i class="bi bi-pen-fill"></i> <span id="btnFirmarLabel">Completar y firmar OT</span>';
    btn.style.background = 'linear-gradient(135deg,#15803d,#16a34a)';
    if (hint){
      if (opcPend > 0){
        hint.innerHTML = `Esta OT no tiene tareas obligatorias. ${hintCounters}`;
        hint.style.color = '#6b7280';
      } else {
        hint.innerHTML = `Todo listo. ${hintCounters}`;
        hint.style.color = '#15803d';
      }
    }
    return;
  }

  // Caso 2: faltan OBLIGATORIAS → botón bloqueado
  if (oblComp < oblTot){
    btn.disabled = true;
    btn.dataset.locked = '1';
    const falta = oblTot - oblComp;
    btn.innerHTML = `<i class="bi bi-lock-fill"></i> <span id="btnFirmarLabel">` +
      `Faltan ${falta} tarea${falta>1?'s':''} obligatoria${falta>1?'s':''} (${oblComp}/${oblTot})</span>`;
    btn.style.background = '#cbd5e1';
    if (hint){
      hint.innerHTML = `Completa las tareas obligatorias para habilitar la firma. ${hintCounters}`;
      hint.style.color = '#b45309';
    }
    return;
  }

  // Caso 3: obligatorias OK, pero hay OPCIONALES pendientes → habilitado, hint ámbar
  if (opcPend > 0){
    btn.disabled = false;
    btn.dataset.locked = '0';
    btn.innerHTML = `<i class="bi bi-pen-fill"></i> <span id="btnFirmarLabel">` +
      `Firmar OT (${opcPend} opcional${opcPend>1?'es':''} pendiente${opcPend>1?'s':''})</span>`;
    btn.style.background = 'linear-gradient(135deg,#f59e0b,#d97706)';
    if (hint){
      hint.innerHTML = `Puedes firmar igual: las obligatorias están listas. ${hintCounters}`;
      hint.style.color = '#b45309';
    }
    return;
  }

  // Caso 4: TODO OK
  btn.disabled = false;
  btn.dataset.locked = '0';
  btn.innerHTML = '<i class="bi bi-pen-fill"></i> <span id="btnFirmarLabel">Completar y firmar OT</span>';
  btn.style.background = 'linear-gradient(135deg,#15803d,#16a34a)';
  if (hint){
    hint.innerHTML = `Todas las tareas listas. ${hintCounters}`;
    hint.style.color = '#15803d';
  }
}

// Llamar al cargar la página
document.addEventListener('DOMContentLoaded', () => {
  // 2026-05-18 (Mejora UX firma): incluir contadores de obligatorias
  actualizarLockFirmar(_calcCtxGlobal());
  // Cargar lista de adjuntos en la pestaña Información (read-only)
  cargarAdjuntos();
  // Drag&drop sobre el uploader (no-op si no existe el uploader — técnico)
  initAdjuntosDragDrop();
  // Pipeline tracking: actualizar "tiempo transcurrido" cada 60s
  actualizarPipelineTimes();
  setInterval(actualizarPipelineTimes, 60_000);
  // "Recorrido" de entrada -- SOLO una vez al cargar, nunca en el refresco
  // periódico de arriba (eso corromperia la animacion cada 60s).
  pipelineRecorridoReplay();
});

// ════════════════════════════════════════════════════════════
//  PIPELINE TRACKING — tiempo transcurrido + ETA
//  2026-05-17 — Refresca labels "Hace X min" / "Hace X horas"
//  desde data-ts-iso en cada .pipe-time, sin reload de página.
// ════════════════════════════════════════════════════════════
function _humanizeAgo(ts){
  // ts: ISO string (UTC desde MySQL) o Date
  if (!ts) return '';
  let d;
  try {
    // MySQL devuelve 'YYYY-MM-DD HH:MM:SS' (sin TZ) — asumir UTC
    if (typeof ts === 'string'){
      const norm = ts.replace(' ', 'T');
      d = new Date(norm.endsWith('Z') || norm.includes('+') ? norm : norm + 'Z');
    } else {
      d = ts;
    }
    if (isNaN(d.getTime())) return '';
  } catch(e){ return ''; }
  const diffMs = Date.now() - d.getTime();
  if (diffMs < 0) return 'En segundos';
  const secs = Math.floor(diffMs / 1000);
  if (secs < 60) return 'Hace ' + secs + 's';
  const mins = Math.floor(secs / 60);
  if (mins < 60) return 'Hace ' + mins + ' min';
  const hrs = Math.floor(mins / 60);
  if (hrs < 24){
    const restoMin = mins % 60;
    return restoMin ? `Hace ${hrs}h ${restoMin}m` : `Hace ${hrs}h`;
  }
  const days = Math.floor(hrs / 24);
  if (days < 30) return `Hace ${days}d`;
  return d.toLocaleDateString('es-CL');
}

function actualizarPipelineTimes(){
  // 2026-05-18 (Mejora estética): el timestamp queda en .pipe-time
  // (monospaced) y el "Hace X" pasa a un hermano .pipe-ago (chip).
  // El chip recibe color del estado vía CSS (done verde, current ámbar).
  const items = document.querySelectorAll('.pipe-time[data-ts-iso]');
  items.forEach(el => {
    const ts = el.getAttribute('data-ts-iso');
    if (!ts || ts === 'None' || ts === '') return;
    const ago = _humanizeAgo(ts);
    if (!ago) return;
    // Buscar chip hermano (mismo .pipe-meta), crearlo si falta
    const meta = el.parentElement;
    if (!meta) return;
    let chip = meta.querySelector('.pipe-ago');
    if (!chip){
      chip = document.createElement('span');
      chip.className = 'pipe-ago';
      el.insertAdjacentElement('afterend', chip);
    }
    chip.textContent = ago;
  });

  // Total elapsed: desde el primer timestamp (asignada/created) hasta ahora
  // O hasta el último timestamp si la OT ya está cerrada.
  const firstStep = document.querySelector('.pipe-step[data-key="asignada"]');
  const lastDone = document.querySelector('.pipe-step.done[data-key="cerrada"]');
  const elapsedEl = document.getElementById('pipeTotalElapsed');
  if (!elapsedEl) return;
  const tStart = firstStep ? firstStep.getAttribute('data-ts') : null;
  if (!tStart || tStart === 'None'){ elapsedEl.textContent = '—'; return; }
  let tEnd = null;
  if (lastDone){ tEnd = lastDone.getAttribute('data-ts'); }
  try {
    const norm = (s) => {
      const n = s.replace(' ', 'T');
      return new Date(n.endsWith('Z') || n.includes('+') ? n : n + 'Z');
    };
    const dStart = norm(tStart);
    const dEnd = (tEnd && tEnd !== 'None') ? norm(tEnd) : new Date();
    const diffMs = dEnd.getTime() - dStart.getTime();
    if (diffMs < 0){ elapsedEl.textContent = '—'; return; }
    const mins = Math.floor(diffMs / 60000);
    if (mins < 60){ elapsedEl.textContent = `${tEnd ? 'Cerrada en' : 'Iniciada hace'} ${mins} min`; return; }
    const hrs = Math.floor(mins / 60);
    const rmin = mins % 60;
    const txt = rmin ? `${hrs}h ${rmin}m` : `${hrs}h`;
    elapsedEl.textContent = (tEnd ? 'Cerrada en ' : 'Iniciada hace ') + txt;
  } catch(e){
    elapsedEl.textContent = '—';
  }

  // Etiqueta del paso actual
  const currLbl = document.getElementById('pipeCurrentLabel');
  if (currLbl){
    const currentStep = document.querySelector('.pipe-step.current:not(.done)');
    if (currentStep){
      const lblEl = currentStep.querySelector('.pipe-label');
      if (lblEl) currLbl.textContent = lblEl.textContent.trim();
    }
  }
}

// 2026-08-08 (Daniel: "el tracking debe tener transición de recorrido...
// algo bien espectacular... hay que empoderar y potenciar las OT" -- mismo
// patrón que stepperReplay() en templates/transporte/public_tracking.html,
// que a Daniel "le encanta"). En vez de aparecer directo en el estado real,
// el pipeline se "reproduce" paso por paso desde cero hasta llegar al
// estado actual -- el CSS (transition en .pipe-step::before, animation en
// .pipe-circle) hace el resto solo con el toggle de clases done/current.
function pipelineRecorridoReplay(){
  const steps = Array.from(document.querySelectorAll('.pipe-step'));
  if (!steps.length) return;
  const reduceMotion = window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  if (reduceMotion) return;  // el estado ya viene correcto desde el servidor
  const targets = steps.map(el => ({
    done: el.classList.contains('done'),
    current: el.classList.contains('current'),
  }));
  steps.forEach(el => el.classList.remove('done', 'current'));
  const STEP_MS = 260;
  steps.forEach((el, i) => {
    setTimeout(() => {
      if (targets[i].done) el.classList.add('done');
      if (targets[i].current) el.classList.add('current');
      // El último hito (Cerrada) gira al completarse, igual que "Entregado"
      // en el stepper de Transporte.
      if (i === steps.length - 1 && targets[i].done){
        const circle = el.querySelector('.pipe-circle');
        if (circle) circle.classList.add('pipe-final-spin');
      }
    }, 200 + i * STEP_MS);
  });
}

// ════════════════════════════════════════════════════════
//  HEADER COLAPSABLE — más espacio para tareas en móvil
// ════════════════════════════════════════════════════════
function toggleHdrCollapse(){
  const hdr = document.getElementById('txHdr');
  const ic = document.getElementById('txHdrToggleIcon');
  if (!hdr) return;
  hdr.classList.toggle('collapsed');
  const isCollapsed = hdr.classList.contains('collapsed');
  if (ic) ic.className = isCollapsed ? 'bi bi-chevron-down' : 'bi bi-chevron-up';
  try { sessionStorage.setItem('ot_hdr_collapsed_' + VID, isCollapsed ? '1' : '0'); } catch(e){}
}

// Auto-colapso al hacer scroll hacia abajo en móvil (UX común)
let _lastScrollY = 0;
let _scrollTicking = false;
window.addEventListener('scroll', () => {
  if (window.innerWidth >= 992) return;  // desktop: no colapsar
  if (_scrollTicking) return;
  _scrollTicking = true;
  requestAnimationFrame(() => {
    const y = window.scrollY;
    const hdr = document.getElementById('txHdr');
    if (hdr){
      if (y > 60 && y > _lastScrollY){
        // scrolling down → colapsar
        if (!hdr.classList.contains('collapsed')){
          hdr.classList.add('collapsed');
          const ic = document.getElementById('txHdrToggleIcon');
          if (ic) ic.className = 'bi bi-chevron-down';
        }
      } else if (y < 30){
        // back al top → expandir
        if (hdr.classList.contains('collapsed')){
          hdr.classList.remove('collapsed');
          const ic = document.getElementById('txHdrToggleIcon');
          if (ic) ic.className = 'bi bi-chevron-up';
        }
      }
    }
    _lastScrollY = y;
    _scrollTicking = false;
  });
}, { passive: true });

// Restaurar estado guardado
document.addEventListener('DOMContentLoaded', () => {
  try {
    const saved = sessionStorage.getItem('ot_hdr_collapsed_' + VID);
    if (saved === '1' && window.innerWidth < 992){
      const hdr = document.getElementById('txHdr');
      const ic = document.getElementById('txHdrToggleIcon');
      if (hdr) hdr.classList.add('collapsed');
      if (ic) ic.className = 'bi bi-chevron-down';
    }
  } catch(e){}
});

// ════════════════════════════════════════════════════════
//  TABS PRINCIPALES — Información / Gestión de tareas
// ════════════════════════════════════════════════════════
function cambiarTabOT(tab){
  document.querySelectorAll('.tx-tab-btn').forEach(b => b.classList.remove('active'));
  document.querySelectorAll('.tx-tab-panel').forEach(p => p.classList.remove('active'));
  const btn = document.getElementById('tabBtn-' + tab);
  const panel = document.getElementById('tabPanel-' + tab);
  if (btn) btn.classList.add('active');
  if (panel) panel.classList.add('active');
  // Si vuelve a la pestaña info, refrescar lista (por si subió algo en otro tab)
  if (tab === 'info') cargarAdjuntos();
  window.scrollTo({ top: 0, behavior: 'smooth' });
}

// ════════════════════════════════════════════════════════
//  ADJUNTOS — listar, subir, eliminar (mant_visita_adjuntos)
// ════════════════════════════════════════════════════════
async function cargarAdjuntos(){
  const cont = document.getElementById('adjList');
  if (!cont) return;
  try {
    const r = await fetch(`/mantenciones/api/visitas/${VID}/adjuntos`);
    const d = await r.json();
    if (!d.ok){
      cont.innerHTML = `<div class="text-muted small text-center py-2">Error: ${_escapeHtml(d.error || '?')}</div>`;
      return;
    }
    const items = d.adjuntos || [];
    document.getElementById('adjCountBadge').textContent = items.length;
    if (!items.length){
      cont.innerHTML = `<div class="text-muted small text-center py-3" style="font-style:italic">
        <i class="bi bi-folder-x" style="font-size:1.5rem;display:block;margin-bottom:4px;opacity:.4"></i>
        Sin documentos preliminares cargados
        <div style="font-size:.7rem;margin-top:4px;color:#94a3b8">
          Pídele al supervisor que adjunte cotizaciones, planos o manuales al generar la OT.
        </div>
      </div>`;
      return;
    }
    cont.innerHTML = items.map(renderAdjuntoItem).join('');
  } catch(e){
    cont.innerHTML = `<div class="text-muted small text-center py-2">Error de red al cargar adjuntos</div>`;
  }
}

function renderAdjuntoItem(a){
  const tipo = (a.tipo || 'otro').toLowerCase();
  const iconos = {
    pdf:'bi-file-earmark-pdf-fill', video:'bi-camera-video-fill',
    foto:'bi-image-fill', documento:'bi-file-earmark-text-fill',
    audio:'bi-mic-fill', otro:'bi-paperclip',
  };
  const url = a.url || a.cloudinary_url || (a.archivo_path ? '/' + a.archivo_path : '');
  const sizeMB = a.file_size_kb ? (a.file_size_kb / 1024).toFixed(1) + ' MB' : '—';
  // Ya viene formateada en hora Chile desde el backend (chile_fmt_filter).
  const fecha = a.created_at || '';
  const nombre = a.archivo_nombre || ('Archivo #' + a.id);
  // Preview thumbnail para foto / video
  const isImg = (tipo === 'foto' && url);
  const isVid = (tipo === 'video' && url);
  // Thumbnail Cloudinary para video: transform on-the-fly
  let vidThumb = '';
  if (isVid && a.cloudinary_url){
    // Reemplazar /video/upload/ → /video/upload/so_auto,w_240,h_180,c_fill,f_jpg/
    try {
      vidThumb = a.cloudinary_url.replace(
        '/video/upload/',
        '/video/upload/so_auto,w_240,h_180,c_fill,f_jpg/'
      ).replace(/\.(webm|mp4|mov|mkv|avi)$/i, '.jpg');
    } catch(e){ vidThumb = ''; }
  }
  // Tag visible si la descripción tiene "· [tag]"
  let tagTxt = '';
  if (a.descripcion){
    const m = String(a.descripcion).match(/\[([^\]]+)\]/);
    if (m) tagTxt = m[1];
  }
  const previewSlot = isImg
    ? `<img class="adj-item-icon" src="${_escapeAttr(cloudTx(url, 'thumb'))}"
            loading="lazy" decoding="async"
            style="object-fit:cover" alt="">`
    : (isVid && vidThumb
        ? `<div class="adj-item-icon" style="position:relative;background:#000;background-image:url('${_escapeAttr(vidThumb)}');background-size:cover;background-position:center;cursor:pointer" onclick="abrirVideoModal('${_escapeAttr(url)}','${_escapeAttr(nombre)}')">
             <i class="bi bi-play-circle-fill" style="color:#fff;text-shadow:0 2px 8px rgba(0,0,0,.7);font-size:1.6rem;position:absolute;top:50%;left:50%;transform:translate(-50%,-50%)"></i>
           </div>`
        : (isVid
            ? `<div class="adj-item-icon" style="cursor:pointer;background:#0a0a0a;color:#dc2626" onclick="abrirVideoModal('${_escapeAttr(url)}','${_escapeAttr(nombre)}')"><i class="bi bi-play-circle-fill"></i></div>`
            : `<div class="adj-item-icon"><i class="bi ${iconos[tipo] || iconos.otro}"></i></div>`)
      );
  return `<div class="adj-item adj-${tipo}" data-id="${a.id}">
    ${previewSlot}
    <div class="adj-item-info">
      <div class="adj-item-name" title="${_escapeAttr(nombre)}">${_escapeHtml(nombre)}</div>
      <div class="adj-item-meta">
        <span class="text-uppercase fw-bold" style="color:#dc2626">${_escapeHtml(tipo)}</span>
        · ${sizeMB} ${fecha ? '· ' + _escapeHtml(fecha) : ''}
        ${tagTxt ? ` · <span style="background:#fee2e2;color:#dc2626;padding:1px 7px;border-radius:50px;font-weight:700;font-size:.7rem">${_escapeHtml(tagTxt)}</span>` : ''}
      </div>
    </div>
    <div class="adj-actions">
      ${isVid ? `<button class="adj-btn" onclick="abrirVideoModal('${_escapeAttr(url)}','${_escapeAttr(nombre)}')" title="Reproducir">
        <i class="bi bi-play-fill"></i>
      </button>` : ''}
      ${url ? `<a class="adj-btn" href="${_escapeAttr(url)}" target="_blank" title="Ver / descargar">
        <i class="bi bi-eye-fill"></i>
      </a>` : ''}
      {# 2026-05-17 — REFACTOR UX: la pestaña Info del técnico es SOLO LECTURA.
         El técnico no puede eliminar adjuntos preliminares.
         (eliminarAdjunto sigue definida pero ningún botón la dispara desde acá.) #}
    </div>
  </div>`;
}

function abrirVideoModal(url, nombre){
  // Modal sencillo overlay para reproducir el video
  let overlay = document.getElementById('videoModalOverlay');
  if (!overlay){
    overlay = document.createElement('div');
    overlay.id = 'videoModalOverlay';
    overlay.style.cssText = 'position:fixed;inset:0;background:rgba(10,10,10,.92);z-index:99998;display:flex;align-items:center;justify-content:center;padding:18px;backdrop-filter:blur(4px)';
    overlay.onclick = (e) => { if (e.target === overlay) cerrarVideoModal(); };
    document.body.appendChild(overlay);
  }
  overlay.innerHTML = `
    <div style="max-width:980px;width:100%;background:#000;border-radius:14px;overflow:hidden;box-shadow:0 20px 60px rgba(0,0,0,.6);position:relative">
      <div style="display:flex;align-items:center;justify-content:space-between;padding:12px 16px;background:#0a0a0a;color:#fff">
        <div style="font-weight:700;font-size:.95rem;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">
          <i class="bi bi-camera-video-fill" style="color:#dc2626"></i>
          ${_escapeHtml(nombre || 'Video')}
        </div>
        <button onclick="cerrarVideoModal()" style="background:transparent;border:none;color:#cbd5e1;font-size:1.4rem;line-height:1;cursor:pointer;padding:4px 8px">
          <i class="bi bi-x-lg"></i>
        </button>
      </div>
      <video src="${_escapeAttr(url)}" controls autoplay playsinline
             style="width:100%;max-height:75vh;display:block;background:#000"></video>
      <div style="padding:10px 16px;background:#0a0a0a;display:flex;gap:8px;justify-content:flex-end">
        <a href="${_escapeAttr(url)}" download class="btn btn-sm" style="background:#fff;color:#0a0a0a;font-weight:700">
          <i class="bi bi-download"></i> Descargar
        </a>
      </div>
    </div>
  `;
  overlay.style.display = 'flex';
}

function cerrarVideoModal(){
  const overlay = document.getElementById('videoModalOverlay');
  if (overlay){ overlay.style.display = 'none'; overlay.innerHTML = ''; }
}

function initAdjuntosDragDrop(){
  const z = document.getElementById('adjUploader');
  if (!z) return;
  ['dragenter','dragover'].forEach(ev => {
    z.addEventListener(ev, (e) => {
      e.preventDefault(); e.stopPropagation();
      z.classList.add('dragover');
    });
  });
  ['dragleave','drop'].forEach(ev => {
    z.addEventListener(ev, (e) => {
      e.preventDefault(); e.stopPropagation();
      z.classList.remove('dragover');
    });
  });
  z.addEventListener('drop', (e) => {
    const files = e.dataTransfer?.files;
    if (!files || !files.length) return;
    [...files].forEach(f => subirAdjuntoFile(f, null));
  });
}

function subirAdjuntoDesdeInput(input, tipoHint){
  if (!input.files || !input.files[0]) return;
  const file = input.files[0];
  subirAdjuntoFile(file, tipoHint || null);
  // reset para que se pueda volver a subir el mismo archivo
  input.value = '';
}

async function subirAdjuntoFile(file, tipoHint){
  if (!file) return;
  if (VISITA_ESTADO === 'cerrada' || VISITA_ESTADO === 'pendiente_aprobacion' || !PUEDE_EJECUTAR_FLAG){
    ilusToast('OT bloqueada · no se pueden subir más adjuntos', { type:'warning' });
    return;
  }
  // Validar tamaño general (100 MB hard limit cliente, server vuelve a validar)
  const MAX_TOTAL = 100 * 1024 * 1024;
  if (file.size > MAX_TOTAL){
    ilusToast('Archivo demasiado grande (máx 100 MB)', { type:'error' });
    return;
  }
  const wrap = document.getElementById('adjProgressWrap');
  const bar = document.getElementById('adjProgressBar');
  const txt = document.getElementById('adjProgressText');
  wrap.style.display = 'block';
  bar.style.width = '5%';
  txt.textContent = `Subiendo ${file.name} (${(file.size / (1024*1024)).toFixed(1)} MB)…`;
  // Comprimir imagen si aplica
  let toSend = file;
  if ((tipoHint === 'foto') || (!tipoHint && /^image\//.test(file.type))){
    try { toSend = await _comprimirImagen(file); } catch(e) { /* fallback original */ }
  }
  const fd = new FormData();
  fd.append('archivo', toSend);
  if (tipoHint) fd.append('tipo', tipoHint);
  // Usar XHR para progreso real
  try {
    const d = await _uploadConProgreso(`/mantenciones/api/visitas/${VID}/adjuntos`, fd, (pct) => {
      bar.style.width = Math.max(5, pct) + '%';
      txt.textContent = `Subiendo… ${Math.round(pct)}%`;
    });
    if (d.ok){
      bar.style.width = '100%';
      txt.textContent = '✓ Subido';
      ilusToast('✓ ' + (d.nombre || d.archivo_nombre || 'Archivo') + ' subido', { type:'success' });
      setTimeout(() => { wrap.style.display = 'none'; bar.style.width = '0%'; }, 800);
      cargarAdjuntos();
    } else {
      wrap.style.display = 'none';
      ilusToast('Error: ' + (d.error || 'desconocido'), { type:'error' });
    }
  } catch(e){
    wrap.style.display = 'none';
    ilusToast('Error de red: ' + (e.message || ''), { type:'error' });
  }
}

function _uploadConProgreso(url, fd, onProgress){
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    xhr.open('POST', url);
    xhr.upload.addEventListener('progress', (e) => {
      if (e.lengthComputable && onProgress) onProgress((e.loaded / e.total) * 100);
    });
    xhr.onload = () => {
      try { resolve(JSON.parse(xhr.responseText)); }
      catch(e){ reject(new Error('Respuesta inválida')); }
    };
    xhr.onerror = () => reject(new Error('XHR error'));
    xhr.send(fd);
  });
}

async function eliminarAdjunto(aid){
  const ok = await ilusConfirm({
    title: 'Eliminar adjunto',
    message: '¿Quitar este archivo de la OT?',
    sub: 'No se puede deshacer. Se borrará también de Cloudinary.',
    okLabel: 'Eliminar', cancelLabel: 'Cancelar',
    danger: true,
  });
  if (!ok) return;
  try {
    const r = await fetch(`/mantenciones/api/visitas/${VID}/adjuntos/${aid}`, { method:'DELETE' });
    const d = await r.json();
    if (d.ok){
      ilusToast('✓ Archivo eliminado', { type:'success' });
      cargarAdjuntos();
    } else {
      ilusToast('Error: ' + (d.error || '?'), { type:'error' });
    }
  } catch(e){ ilusToast('Error de red', { type:'error' }); }
}

// ════════════════════════════════════════════════════════════════════
//  GRABADOR DE VIDEO IN-APP (Feature 2 — 2026-05-17)
//  MediaRecorder API + Cloudinary backend.
//  Soporta: Chrome/Edge/Firefox (WebM/VP9), Safari iOS 15+ (MP4/H.264).
//  Detección de codec con MediaRecorder.isTypeSupported.
//  Tags creativos opcionales para etiquetar la evidencia.
// ════════════════════════════════════════════════════════════════════
let _grabStream = null;
let _grabRecorder = null;
let _grabChunks = [];
let _grabBlob = null;
let _grabBlobMime = '';
let _grabTagSelected = null;
let _grabTimerInterval = null;
let _grabStartMs = 0;
let _grabPausedMs = 0;     // tiempo acumulado pausado
let _grabPauseAt = 0;

function _grabPickMime(){
  // Orden de preferencia. iOS Safari 15+ soporta MP4/H.264 nativo.
  const candidates = [
    'video/webm;codecs=vp9,opus',
    'video/webm;codecs=vp8,opus',
    'video/webm',
    'video/mp4;codecs=h264,aac',
    'video/mp4',
  ];
  if (typeof MediaRecorder === 'undefined') return null;
  for (const m of candidates){
    try {
      if (MediaRecorder.isTypeSupported && MediaRecorder.isTypeSupported(m)) return m;
    } catch(e){ /* ignore */ }
  }
  // Último recurso: vacío (deja al navegador elegir)
  return '';
}

function _grabCalidadConstraints(quality){
  // Mapa de calidad → constraints de video
  const base = { audio: true, video: { facingMode: { ideal: 'environment' } } };
  if (quality === '1080'){ base.video.width = { ideal: 1920 }; base.video.height = { ideal: 1080 }; }
  else if (quality === '720'){ base.video.width = { ideal: 1280 }; base.video.height = { ideal: 720 }; }
  else if (quality === '480'){ base.video.width = { ideal: 854 };  base.video.height = { ideal: 480 }; }
  // 'auto' deja al navegador decidir
  return base;
}

async function abrirGrabador(){
  if (VISITA_ESTADO === 'cerrada' || VISITA_ESTADO === 'pendiente_aprobacion' || !PUEDE_EJECUTAR_FLAG){
    ilusToast('OT bloqueada · no se pueden subir más adjuntos', { type:'warning' });
    return;
  }
  if (typeof MediaRecorder === 'undefined' ||
      !navigator.mediaDevices || !navigator.mediaDevices.getUserMedia){
    await ilusAlert({
      title: 'Navegador no compatible',
      message: 'Tu navegador no soporta grabación de video in-app. Prueba Chrome, Edge, Firefox o Safari 15+.',
      type: 'warning',
    });
    return;
  }
  document.getElementById('grabadorBox').style.display = 'block';
  _grabSetEstado('listo');
  _grabResetUI();
  // Detectar cámaras múltiples
  try {
    const devs = await navigator.mediaDevices.enumerateDevices();
    const cams = devs.filter(d => d.kind === 'videoinput');
    if (cams.length >= 2){
      document.getElementById('grabCamSelect').style.display = '';
    }
  } catch(e){ /* ignore */ }
  // Iniciar preview ya (sin grabar)
  try {
    await _grabAbrirStream();
  } catch(e){
    await ilusAlert({
      title: 'No se pudo acceder a la cámara',
      message: 'Verifica los permisos del sitio (cámara y micrófono).',
      sub: e && e.message ? e.message : '',
      type: 'error',
    });
    cerrarGrabador();
  }
}

async function _grabAbrirStream(){
  const calidad = document.getElementById('grabCalidad').value || 'auto';
  const camSel = document.getElementById('grabCamSelect');
  const constraints = _grabCalidadConstraints(calidad);
  if (camSel && camSel.value){
    constraints.video.facingMode = { ideal: camSel.value };
  }
  _grabCerrarStream();
  _grabStream = await navigator.mediaDevices.getUserMedia(constraints);
  const v = document.getElementById('grabVideoPreview');
  v.srcObject = _grabStream;
  v.muted = true;
  v.play().catch(()=>{});
}

function _grabCerrarStream(){
  try {
    if (_grabStream){
      _grabStream.getTracks().forEach(t => t.stop());
    }
  } catch(e){}
  _grabStream = null;
}

function cerrarGrabador(){
  _grabStopTimer();
  if (_grabRecorder && _grabRecorder.state !== 'inactive'){
    try { _grabRecorder.stop(); } catch(e){}
  }
  _grabCerrarStream();
  // FIX REVIEWER G4 — Revocar ObjectURL del playback antes de cerrar.
  // Sin esto, cada grabación filtra ~50-100MB de memoria del browser.
  try {
    const pb = document.getElementById('grabVideoPlayback');
    if (pb && pb.src && pb.src.startsWith('blob:')){
      URL.revokeObjectURL(pb.src);
      pb.removeAttribute('src');
    }
  } catch(e){}
  _grabRecorder = null;
  _grabChunks = [];
  _grabBlob = null;
  _grabTagSelected = null;
  document.getElementById('grabadorBox').style.display = 'none';
  _grabResetUI();
}

function _grabResetUI(){
  document.getElementById('grabVideoPreview').style.display = '';
  const pb = document.getElementById('grabVideoPlayback');
  pb.style.display = 'none';
  pb.removeAttribute('src');
  document.getElementById('grabRecDot').style.display = 'none';
  document.getElementById('grabTimer').style.display = 'none';
  document.getElementById('grabTimer').textContent = '00:00';
  document.getElementById('grabTagOverlay').style.display = 'none';
  document.getElementById('grabUploadWrap').style.display = 'none';
  document.getElementById('grabUploadBar').style.width = '0%';
  document.querySelectorAll('#grabTagsBar .grab-tag-pill').forEach(t => t.classList.remove('selected'));
  _grabSetEstado('listo');
}

function _grabSetEstado(est){
  // 'listo' | 'grabando' | 'pausado' | 'revision' | 'subiendo'
  const inicio = document.getElementById('grabPanelInicio');
  const grabando = document.getElementById('grabPanelGrabando');
  const revision = document.getElementById('grabPanelRevision');
  const txt = document.getElementById('grabEstadoTxt');
  const configBar = document.getElementById('grabConfigBar');
  inicio.style.display = (est === 'listo') ? 'flex' : 'none';
  grabando.style.display = (est === 'grabando' || est === 'pausado') ? 'flex' : 'none';
  revision.style.display = (est === 'revision') ? 'flex' : 'none';
  configBar.style.display = (est === 'listo') ? 'flex' : 'none';
  const map = {
    listo: 'Listo para grabar',
    grabando: 'Grabando…',
    pausado: 'Grabación pausada',
    revision: 'Revisión — listo para subir',
    subiendo: 'Subiendo…',
  };
  if (txt) txt.textContent = map[est] || '';
  // Botón pausa: cambiar texto
  const btnPause = document.getElementById('btnGrabarPause');
  if (btnPause){
    if (est === 'pausado'){
      btnPause.innerHTML = '<i class="bi bi-play-fill"></i> Reanudar';
    } else {
      btnPause.innerHTML = '<i class="bi bi-pause-fill"></i> Pausar';
    }
  }
}

function selTag(el){
  document.querySelectorAll('#grabTagsBar .grab-tag-pill').forEach(t => {
    if (t !== el) t.classList.remove('selected');
  });
  if (el.classList.contains('selected')){
    el.classList.remove('selected');
    _grabTagSelected = null;
    document.getElementById('grabTagOverlay').style.display = 'none';
  } else {
    el.classList.add('selected');
    _grabTagSelected = el.dataset.tag;
    const ov = document.getElementById('grabTagOverlay');
    ov.textContent = _grabTagSelected;
    ov.style.display = '';
  }
}

async function grabIniciar(){
  if (!_grabStream){
    try { await _grabAbrirStream(); }
    catch(e){
      ilusToast('No se pudo abrir la cámara', { type:'error' });
      return;
    }
  }
  const mime = _grabPickMime();
  _grabChunks = [];
  _grabBlob = null;
  _grabBlobMime = mime || (/(iPhone|iPad|iPod)/i.test(navigator.userAgent) ? 'video/mp4' : 'video/webm');
  try {
    const opts = mime ? { mimeType: mime } : {};
    _grabRecorder = new MediaRecorder(_grabStream, opts);
  } catch(e){
    // Fallback sin opciones
    try { _grabRecorder = new MediaRecorder(_grabStream); }
    catch(e2){
      await ilusAlert({
        title: 'No se pudo iniciar la grabación',
        message: 'MediaRecorder no está disponible en este navegador.',
        type: 'error',
      });
      return;
    }
  }
  _grabRecorder.ondataavailable = (ev) => {
    if (ev.data && ev.data.size > 0) _grabChunks.push(ev.data);
  };
  _grabRecorder.onstop = () => {
    // Concatenar y mostrar playback
    const blob = new Blob(_grabChunks, { type: _grabBlobMime });
    _grabBlob = blob;
    const pb = document.getElementById('grabVideoPlayback');
    document.getElementById('grabVideoPreview').style.display = 'none';
    pb.style.display = '';
    pb.src = URL.createObjectURL(blob);
    pb.load();
    _grabStopTimer();
    document.getElementById('grabRecDot').style.display = 'none';
    _grabSetEstado('revision');
  };
  _grabRecorder.start(1000); // chunks cada 1s
  _grabStartTimer();
  document.getElementById('grabRecDot').style.display = '';
  document.getElementById('grabTimer').style.display = '';
  _grabSetEstado('grabando');
}

function grabPausarReanudar(){
  if (!_grabRecorder) return;
  if (_grabRecorder.state === 'recording'){
    try {
      _grabRecorder.pause();
      _grabPauseAt = Date.now();
      _grabSetEstado('pausado');
    } catch(e){}
  } else if (_grabRecorder.state === 'paused'){
    try {
      _grabRecorder.resume();
      _grabPausedMs += (Date.now() - _grabPauseAt);
      _grabPauseAt = 0;
      _grabSetEstado('grabando');
    } catch(e){}
  }
}

function grabDetener(){
  if (!_grabRecorder) return;
  try {
    if (_grabRecorder.state !== 'inactive') _grabRecorder.stop();
  } catch(e){}
}

function grabCancelar(){
  if (_grabRecorder && _grabRecorder.state !== 'inactive'){
    try { _grabRecorder.stop(); } catch(e){}
  }
  _grabChunks = [];
  _grabBlob = null;
  _grabStopTimer();
  document.getElementById('grabRecDot').style.display = 'none';
  document.getElementById('grabTimer').style.display = 'none';
  document.getElementById('grabTimer').textContent = '00:00';
  // Reset stream visible
  document.getElementById('grabVideoPlayback').style.display = 'none';
  document.getElementById('grabVideoPreview').style.display = '';
  _grabSetEstado('listo');
}

function grabRehacer(){
  _grabBlob = null;
  _grabChunks = [];
  const pb = document.getElementById('grabVideoPlayback');
  pb.style.display = 'none';
  try { URL.revokeObjectURL(pb.src); } catch(e){}
  pb.removeAttribute('src');
  document.getElementById('grabVideoPreview').style.display = '';
  // Reabrir stream
  _grabAbrirStream().catch(()=>{
    ilusToast('No se pudo reabrir la cámara', { type:'error' });
  });
  _grabSetEstado('listo');
}

function _grabStartTimer(){
  _grabStartMs = Date.now();
  _grabPausedMs = 0;
  _grabPauseAt = 0;
  if (_grabTimerInterval) clearInterval(_grabTimerInterval);
  _grabTimerInterval = setInterval(() => {
    let elapsed = Date.now() - _grabStartMs - _grabPausedMs;
    if (_grabPauseAt) elapsed -= (Date.now() - _grabPauseAt);
    const sec = Math.max(0, Math.floor(elapsed / 1000));
    const mm = String(Math.floor(sec / 60)).padStart(2, '0');
    const ss = String(sec % 60).padStart(2, '0');
    document.getElementById('grabTimer').textContent = mm + ':' + ss;
  }, 500);
}

function _grabStopTimer(){
  if (_grabTimerInterval) clearInterval(_grabTimerInterval);
  _grabTimerInterval = null;
}

async function grabSubir(){
  if (!_grabBlob){
    ilusToast('No hay video grabado', { type:'warning' });
    return;
  }
  if (_grabBlob.size > 100 * 1024 * 1024){
    await ilusAlert({
      title: 'Video demasiado grande',
      message: `El video pesa ${(_grabBlob.size/(1024*1024)).toFixed(1)} MB. Máximo: 100 MB.`,
      sub: 'Prueba una calidad más baja o una grabación más corta.',
      type: 'warning',
    });
    return;
  }
  _grabSetEstado('subiendo');
  document.getElementById('grabUploadWrap').style.display = '';
  const bar = document.getElementById('grabUploadBar');
  const txt = document.getElementById('grabUploadTxt');
  bar.style.width = '5%';
  txt.textContent = 'Subiendo… 0%';
  // Calcular duración aproximada
  const durSec = Math.floor((Date.now() - _grabStartMs - _grabPausedMs) / 1000);
  const ext = (_grabBlobMime.indexOf('mp4') >= 0) ? 'mp4' : 'webm';
  const fname = `grabacion_v${VID}_${Date.now()}.${ext}`;
  const fd = new FormData();
  fd.append('video', _grabBlob, fname);
  if (_grabTagSelected) fd.append('tag', _grabTagSelected);
  if (durSec > 0) fd.append('duration_sec', String(durSec));
  try {
    const d = await _uploadConProgreso(
      `/mantenciones/api/visitas/${VID}/grabacion`,
      fd,
      (pct) => {
        bar.style.width = Math.max(5, pct) + '%';
        txt.textContent = 'Subiendo… ' + Math.round(pct) + '%';
      }
    );
    if (d.ok){
      bar.style.width = '100%';
      txt.textContent = '✓ Listo';
      ilusToast('✓ Video subido a la OT', { type:'success' });
      cargarAdjuntos();
      setTimeout(() => cerrarGrabador(), 800);
    } else {
      ilusToast('Error: ' + (d.error || '?'), { type:'error' });
      _grabSetEstado('revision');
    }
  } catch(e){
    ilusToast('Error de red al subir', { type:'error' });
    _grabSetEstado('revision');
  }
}

// Cuando cambia calidad o cámara y aún no grabaste, reabrir stream
document.addEventListener('DOMContentLoaded', function(){
  const calSel = document.getElementById('grabCalidad');
  const camSel = document.getElementById('grabCamSelect');
  if (calSel) calSel.addEventListener('change', () => {
    if (_grabRecorder && _grabRecorder.state !== 'inactive') return;
    if (document.getElementById('grabadorBox').style.display === 'none') return;
    _grabAbrirStream().catch(()=>{});
  });
  if (camSel) camSel.addEventListener('change', () => {
    if (_grabRecorder && _grabRecorder.state !== 'inactive') return;
    if (document.getElementById('grabadorBox').style.display === 'none') return;
    _grabAbrirStream().catch(()=>{});
  });
});

// ════════════════════════════════════════════════════════
//  HELPERS
// ════════════════════════════════════════════════════════
function _escapeHtml(s){
  return String(s||'').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
}
function _escapeAttr(s){ return String(s||'').replace(/"/g, '&quot;'); }

// ── ¿Aplica mantención? (2026-06-10, Daniel) ─────────────────────────
// El técnico excluye collarines/accesorios DESDE el levantamiento sin
// salir de la OT. Reversible con un toque. PUT al endpoint existente.
async function otToggleMantencion(mid){
  const btn = document.getElementById('eqAplBtn-' + mid);
  if (!btn) return;
  const ahora = btn.dataset.aplica === '1';
  const nuevo = !ahora;
  if (!nuevo) {
    const ok = await ilusConfirm({
      title: 'Excluir de mantención',
      message: '¿Marcar este equipo como "Sin mantención"?',
      sub: 'Para accesorios (collarines, mancuernas sueltas, etc.) que no se levantan ni se mantienen. Reversible.',
      okLabel: 'Excluir', cancelLabel: 'Cancelar',
    });
    if (!ok) return;
  }
  try {
    const r = await fetch(`/mantenciones/api/maquinas/${mid}/aplica-mantencion`, {
      method: 'PUT', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ aplica: nuevo }),
    });
    const d = await r.json().catch(() => ({}));
    if (!r.ok || !d.ok) { ilusToast('No se pudo: ' + (d.error || 'error'), { type: 'error' }); return; }
    btn.dataset.aplica = nuevo ? '1' : '0';
    btn.style.background  = nuevo ? '#f0fdf4' : '#f3f4f6';
    btn.style.borderColor = nuevo ? '#bbf7d0' : '#d1d5db';
    btn.style.color       = nuevo ? '#166534' : '#6b7280';
    btn.title = nuevo ? 'En plan de mantención — tocar para excluir (accesorio)'
                      : 'SIN mantención — tocar para volver a incluir';
    btn.innerHTML = nuevo ? '<i class="bi bi-wrench-adjustable"></i>' : '<i class="bi bi-dash-circle"></i>';
    ilusToast(nuevo ? '✓ Incluido en el plan de mantención' : 'Excluido de mantención (accesorio)',
              { type: nuevo ? 'success' : 'info' });
  } catch(e){ ilusToast('Error de red: ' + e.message, { type: 'error' }); }
}

async function _comprimirImagen(file, maxDim = 1600, quality = 0.82){
  if (file.size < 600 * 1024) return file;
  return new Promise((resolve, reject) => {
    const img = new Image();
    const reader = new FileReader();
    reader.onerror = () => reject(reader.error);
    reader.onload = () => {
      img.onerror = () => reject(new Error('Decode error'));
      img.onload = () => {
        let w = img.width, h = img.height;
        const ratio = Math.min(maxDim / Math.max(w, h), 1);
        const nw = Math.round(w * ratio), nh = Math.round(h * ratio);
        const canvas = document.createElement('canvas');
        canvas.width = nw; canvas.height = nh;
        const ctx = canvas.getContext('2d');
        ctx.imageSmoothingEnabled = true;
        ctx.imageSmoothingQuality = 'high';
        ctx.drawImage(img, 0, 0, nw, nh);
        canvas.toBlob((blob) => {
          if (!blob) return reject(new Error('toBlob falló'));
          if (blob.size >= file.size) { resolve(file); return; }
          resolve(new File([blob], file.name.replace(/\.(jpg|jpeg|png|webp)$/i, '.jpg'), {
            type: 'image/jpeg', lastModified: Date.now(),
          }));
        }, 'image/jpeg', quality);
      };
      img.src = reader.result;
    };
    reader.readAsDataURL(file);
  });
}

// ════════════════════════════════════════════════════════════════
//  GPS BAR (auditoría de inicio de ejecución)
//  ──────────────────────────────────────────────────────────────
//  FIX 2026-05-17 CRÍTICO iOS: NO disparamos getCurrentPosition en
//  page load. iOS Safari requiere gesto de usuario, y un auto-call
//  desde DOMContentLoaded NUNCA muestra el prompt nativo de iOS.
//  En vez de eso:
//   - Si NO tenemos GPS aún: mostramos "Sin ubicación · [Capturar ahora]".
//   - Si forzar=true (usuario tocó "Capturar ahora"): cascade NATIVO directo.
//   - Suscribimos permisson change (no-await, en idle) para refrescar UI
//     cuando el usuario vuelve de Ajustes.
// ════════════════════════════════════════════════════════════════
async function pedirGPS(forzar = false){
  const bar = document.getElementById('execGpsBar');
  const ttl = document.getElementById('execGpsTitle');
  const sub = document.getElementById('execGpsSub');
  const actions = document.getElementById('execGpsActions');
  if (!bar || !ttl || !sub) return;
  if (!navigator.geolocation){
    bar.classList.remove('gps-ok');
    bar.classList.add('gps-err');
    ttl.textContent = 'Geolocalización no soportada por este dispositivo';
    sub.innerHTML = '<small>Usa un navegador moderno (Chrome, Safari, Firefox).</small>';
    if (actions) actions.style.display = 'flex';
    return;
  }
  // ── Suscripción a cambios de permiso (idle, no bloquea gesto) ──
  if (!window.__gpsPermSubscribed && navigator.permissions && navigator.permissions.query){
    // ⚠️ NO await acá si forzar=true — rompería el gesto. Lo hacemos en background.
    Promise.resolve().then(async () => {
      try {
        const status = await navigator.permissions.query({ name: 'geolocation' });
        status.onchange = () => {
          _gpsLog('permission_changed', status.state);
          // Refrescamos UI; si granted y no tenemos GPS, mostramos botón visible
          if (status.state === 'granted'){
            ttl.innerHTML = '<i class="bi bi-geo-alt"></i> Ubicación lista — toca para capturar';
            sub.innerHTML = '<small>Permiso concedido. Toca <strong>Capturar ahora</strong>.</small>';
          }
        };
        window.__gpsPermSubscribed = true;
      } catch(e){ _gpsLog('subscribe_perm_failed', e.message); }
    });
  }

  // ── Si NO es forzado y NO tenemos GPS aún → solo mostrar el placeholder ──
  // En iOS Safari, el auto-prompt jamás funciona (sin gesto). Mostramos el
  // CTA grande para que el técnico decida tocarlo. Esto es lo que el prompt
  // nativo necesita.
  if (!forzar && !window.__ilusGPS){
    bar.classList.remove('gps-ok', 'gps-err');
    ttl.innerHTML = '<i class="bi bi-geo-alt"></i> Sin ubicación capturada';
    sub.innerHTML = '<small>Toca <strong>Capturar ahora</strong> para registrar tu posición. ' +
                    'iPhone te pedirá permiso la primera vez.</small>';
    if (actions) actions.style.display = 'flex';
    return;
  }

  // Si ya tenemos GPS y no se forzó, solo re-renderizamos el banner
  if (!forzar && window.__ilusGPS){
    _gpsRefrescarBanner();
    return;
  }

  // ── forzar=true (usuario tocó "Capturar ahora") ──
  // Estado intermedio
  ttl.innerHTML = '<i class="bi bi-arrow-repeat"></i> Detectando ubicación…';
  sub.innerHTML = '<small>Si iPhone te pregunta, toca <strong>Permitir</strong>.</small>';
  bar.classList.remove('gps-ok', 'gps-err');
  if (actions) actions.style.display = 'none';

  let result = null;
  try {
    // CALL DIRECTO — el await del cascade va ANTES de cualquier otro await
    result = await _gpsCascade();
  } catch(err){
    _gpsLog('cascade_failed_main', { code: err.code, msg: err.message });
    if (err.code === 1){
      // Denegado → mostrar bar de error + acciones + auto-abrir ayuda
      bar.classList.add('gps-err');
      ttl.innerHTML = '<i class="bi bi-shield-x"></i> Permiso de ubicación denegado';
      sub.innerHTML = '<small>iPhone bloqueó el GPS. Toca <strong>¿Cómo activar GPS?</strong> para ver instrucciones.</small>';
      if (actions) actions.style.display = 'flex';
      return;
    }
    // Otros errores (timeout, unavailable) → opción reintento + alternativas
    bar.classList.add('gps-err');
    if (err.code === 3){
      ttl.innerHTML = '<i class="bi bi-clock-history"></i> GPS lento — intenta de nuevo';
      sub.innerHTML = '<small>El iPhone tardó demasiado. Sal al aire libre y toca <strong>Reintentar GPS</strong>.</small>';
    } else if (err.code === 2){
      ttl.innerHTML = '<i class="bi bi-wifi-off"></i> GPS no disponible';
      sub.innerHTML = '<small>Tu iPhone no puede obtener GPS ahora. Prueba al aire libre.</small>';
    } else {
      ttl.innerHTML = '<i class="bi bi-exclamation-triangle-fill"></i> No pudimos detectar ubicación';
      sub.innerHTML = '<small>Toca <strong>Reintentar</strong>.</small>';
    }
    if (actions) actions.style.display = 'flex';
    return;
  }

  // Éxito (GPS) — registrar y mostrar
  await _gpsRegistrarResultado(result);
}

// Refresca el banner sin volver a pedir GPS (cuando ya tenemos resultado)
function _gpsRefrescarBanner(){
  const r = window.__ilusGPS;
  if (!r) return;
  _gpsRegistrarResultado(r);
}

// Aplica el resultado al banner + lo persiste en el backend
// FIX 2026-05-17: muestra coords + dirección + "captured X min ago"
async function _gpsRegistrarResultado(result){
  const bar = document.getElementById('execGpsBar');
  const ttl = document.getElementById('execGpsTitle');
  const sub = document.getElementById('execGpsSub');
  const actions = document.getElementById('execGpsActions');
  if (!bar || !ttl || !sub) return;

  const { lat, lng, method } = result;
  window.__ilusGPS = result;
  window.__execOrigenLat = lat;
  window.__execOrigenLng = lng;
  // Timestamp en ms — usado para "captured X min ago"
  if (!result.capturedAt) result.capturedAt = Date.now();
  window.__execGpsCapturedAt = result.capturedAt;

  // Si vino con `dir` (IP) ya tenemos dirección legible. Si no, reverse geocoding.
  let dir = result.dir || '';
  if (!dir && method !== 'ip'){
    try {
      const rev = await fetch(`https://nominatim.openstreetmap.org/reverse?format=json&lat=${lat}&lon=${lng}&zoom=18`,
        { headers: { 'Accept-Language': 'es' } });
      const dj = await rev.json();
      dir = dj.display_name || '';
      result.dir = dir; // cache para refresh
    } catch(e) { /* sin dirección, OK */ }
  }

  const methodLabel = _gpsMethodLabel(method);
  const isApprox = (method === 'ip');
  bar.classList.remove('gps-err');
  bar.classList.add(isApprox ? 'gps-err' : 'gps-ok');
  // Si IP, dejamos el banner amarillo/rojo para destacar que NO es preciso
  if (isApprox){
    bar.style.background = '#fff8e1';
    bar.style.borderColor = '#f59e0b';
  } else {
    bar.style.background = ''; bar.style.borderColor = '';
  }
  ttl.innerHTML = `<i class="bi bi-${isApprox ? 'geo' : 'check-circle-fill'}"></i> ` +
                  `${methodLabel}: <span style="font-family:monospace;font-weight:700">` +
                  `${lat.toFixed(5)}, ${lng.toFixed(5)}</span>`;
  const accStr = result.accuracy ? ` ±${Math.round(result.accuracy)}m` : '';
  const ago = _gpsTimeAgo(result.capturedAt);
  const dirStr = dir ? `<div>${_escapeHtml(dir)}</div>` : '';
  sub.innerHTML = `<small>${dirStr}<span style="color:#6b7280">capturado ${ago}${accStr}</span></small>`;
  // SIEMPRE mostrar las acciones (incluyendo recapturar) para que el técnico
  // pueda actualizar la posición en cualquier momento.
  if (actions) actions.style.display = 'flex';

  // Persistir en backend (best-effort)
  try {
    const dirPersist = isApprox || method === 'manual'
      ? `[${methodLabel}] ${dir || ''}`.trim()
      : dir;
    await fetch(`/mantenciones/api/visitas/${VID}/exec-gps`, {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ lat, lng, direccion: dirPersist })
    });
  } catch(e) { _gpsLog('exec_gps_persist_failed', e.message); }
}

// "hace X min" formato humano para el banner GPS
function _gpsTimeAgo(ts){
  if (!ts) return 'recién';
  const secs = Math.floor((Date.now() - ts) / 1000);
  if (secs < 30) return 'recién';
  if (secs < 90) return 'hace 1 min';
  if (secs < 3600) return 'hace ' + Math.floor(secs/60) + ' min';
  if (secs < 7200) return 'hace 1 hora';
  return 'hace ' + Math.floor(secs/3600) + ' horas';
}

// Refresca solo el "hace X min" en el banner cada 30s sin re-pedir GPS
setInterval(() => {
  if (window.__ilusGPS && window.__execGpsCapturedAt){
    const sub = document.getElementById('execGpsSub');
    if (!sub) return;
    // Solo si el banner está en "OK" state (no en error o capturando)
    const bar = document.getElementById('execGpsBar');
    if (!bar || !bar.classList.contains('gps-ok')) return;
    // Re-render solo el "hace X min" sin cambiar la dirección
    const r = window.__ilusGPS;
    const accStr = r.accuracy ? ` ±${Math.round(r.accuracy)}m` : '';
    const ago = _gpsTimeAgo(r.capturedAt);
    const dirStr = r.dir ? `<div>${_escapeHtml(r.dir)}</div>` : '';
    sub.innerHTML = `<small>${dirStr}<span style="color:#6b7280">capturado ${ago}${accStr}</span></small>`;
  }
}, 30000);

// ════════════════════════════════════════════════════════
//  AYUDA: cómo activar GPS en cada navegador
// ════════════════════════════════════════════════════════
async function mostrarAyudaGPS(){
  const ua = navigator.userAgent;
  const isIOS = /iphone|ipad|ipod/i.test(ua);
  const isAndroid = /android/i.test(ua);

  // Construimos un modal con 3 tabs (Activar | Verificar | Otras opciones)
  // usando ilusConfirm con messageHtml. Los tabs son botones que muestran/ocultan secciones.
  let tabActivar = '', tabVerificar = '', tabOtras = '';

  if (isIOS){
    tabActivar = `
      <div style="background:#fee2e2;border-left:4px solid #dc2626;padding:10px 12px;border-radius:8px;margin-bottom:12px">
        <strong style="color:#dc2626">⚠️ El GPS está bloqueado en Safari</strong><br>
        <small style="color:#7f1d1d">Una vez que iOS marca "denegado" no podemos volver a pedirlo desde acá. Hay que ir a Ajustes.</small>
      </div>
      <strong style="display:block;margin-bottom:6px;color:#0a0a0a">📱 Paso 1 — Servicios de Ubicación globales:</strong>
      <ol style="margin:0 0 12px;padding-left:22px;font-size:.88rem;line-height:1.7;color:#1f2937">
        <li>Abrí la app <strong>Ajustes</strong> (la gris con engranajes)</li>
        <li>Toca <strong>Privacidad y Seguridad</strong></li>
        <li>Toca <strong>Servicios de ubicación</strong></li>
        <li>Activa el <strong>switch verde de arriba</strong> ("Servicios de ubicación")</li>
      </ol>
      <strong style="display:block;margin-bottom:6px;color:#0a0a0a">🌐 Paso 2 — Permitir a Safari:</strong>
      <ol style="margin:0 0 12px;padding-left:22px;font-size:.88rem;line-height:1.7;color:#1f2937">
        <li>En la misma pantalla, desliza hasta encontrar <strong>"Safari"</strong> en la lista</li>
        <li>Toca <strong>Safari → Sitios web → Ubicación</strong></li>
        <li>O elige directamente <strong>"Preguntar la próxima vez"</strong> o <strong>"Mientras uso la app"</strong></li>
      </ol>
      <strong style="display:block;margin-bottom:6px;color:#0a0a0a">🔄 Paso 3 — Volver acá y reintentar:</strong>
      <ol style="margin:0 0 8px;padding-left:22px;font-size:.88rem;line-height:1.7;color:#1f2937">
        <li>Vuelve a Safari (desliza desde abajo o toca Safari)</li>
        <li>Esta página detectará el cambio automáticamente</li>
        <li>O toca el botón <strong>🔄 Reintentar GPS</strong> abajo</li>
      </ol>`;
    tabVerificar = `
      <strong style="display:block;margin-bottom:8px;color:#0a0a0a">¿Activado pero sigue sin funcionar?</strong>
      <div style="background:#fff8e1;border-left:4px solid #f59e0b;padding:10px 12px;border-radius:8px;margin-bottom:10px;font-size:.85rem;color:#92400e">
        En iPhone 15 Pro Max con iOS 17/18, Safari a veces guarda "denegado" para este sitio aunque los permisos globales estén OK.
      </div>
      <strong style="display:block;margin-bottom:6px;color:#0a0a0a">🔍 Verificar permiso específico de Safari:</strong>
      <ol style="margin:0 0 12px;padding-left:22px;font-size:.88rem;line-height:1.7;color:#1f2937">
        <li>Ajustes → <strong>Apps</strong> → buscar <strong>Safari</strong></li>
        <li>Toca Safari → <strong>Ubicación</strong></li>
        <li>Debe estar en <strong>"Preguntar"</strong> o <strong>"Permitir"</strong></li>
      </ol>
      <strong style="display:block;margin-bottom:6px;color:#0a0a0a">🌐 Limpiar caché del sitio:</strong>
      <ol style="margin:0 0 12px;padding-left:22px;font-size:.88rem;line-height:1.7;color:#1f2937">
        <li>Ajustes → Safari → <strong>Avanzado</strong> → Datos de sitios web</li>
        <li>Buscar el dominio de la app y eliminarlo</li>
        <li>Cerrar Safari completamente (swipe up desde abajo, deslizar arriba la card de Safari)</li>
        <li>Volver a abrir el link de la OT</li>
      </ol>
      <strong style="display:block;margin-bottom:6px;color:#0a0a0a">📶 Otras causas:</strong>
      <ul style="margin:0;padding-left:22px;font-size:.85rem;line-height:1.6;color:#1f2937">
        <li>Modo avión o sin señal celular → activar datos</li>
        <li>Modo de bajo consumo → desactivar</li>
        <li>VPN activa → desactivar temporalmente</li>
      </ul>`;
    tabOtras = `
      <strong style="display:block;margin-bottom:8px;color:#0a0a0a">Si no puedes activar el GPS:</strong>
      <div style="background:#dbeafe;border-left:4px solid #3b82f6;padding:10px 12px;border-radius:8px;margin-bottom:10px;font-size:.85rem;color:#1e40af">
        Estas opciones permiten <strong>avanzar la OT</strong> aunque el GPS no funcione. La precisión es menor pero queda registrada.
      </div>
      <div style="display:flex;flex-direction:column;gap:8px;margin-bottom:10px">
        <button type="button" onclick="_gpsDismissHelpAnd('manual')"
                style="background:#0a0a0a;color:#fff;border:none;border-radius:8px;padding:12px 14px;font-weight:600;cursor:pointer;min-height:44px;display:flex;align-items:center;gap:8px;justify-content:center">
          <i class="bi bi-pencil-square"></i> Ingresar dirección manualmente
        </button>
        <button type="button" onclick="_gpsDismissHelpAnd('ip')"
                style="background:#fff;color:#0a0a0a;border:2px solid #0a0a0a;border-radius:8px;padding:12px 14px;font-weight:600;cursor:pointer;min-height:44px;display:flex;align-items:center;gap:8px;justify-content:center">
          <i class="bi bi-globe2"></i> Usar ubicación aproximada (por IP)
        </button>
      </div>
      <small style="color:#6b7280;font-size:.78rem;display:block">
        Si todo falla y nada funciona, contactá a soporte: <strong>daniel.aguilar@sphs.cl</strong>
      </small>`;
  } else if (isAndroid){
    tabActivar = `
      <div style="background:#fee2e2;border-left:4px solid #dc2626;padding:10px 12px;border-radius:8px;margin-bottom:10px">
        <strong style="color:#dc2626">⚠️ El GPS está bloqueado en Chrome</strong>
      </div>
      <strong style="display:block;margin-bottom:6px;color:#0a0a0a">🌐 Activar en Chrome:</strong>
      <ol style="margin:0 0 12px;padding-left:22px;font-size:.88rem;line-height:1.7;color:#1f2937">
        <li>Toca el <strong>candado 🔒</strong> a la izquierda de la URL arriba</li>
        <li>Toca <strong>Permisos</strong> (o <strong>Configuración del sitio</strong>)</li>
        <li>En "Ubicación" elige <strong>"Permitir"</strong></li>
        <li>Vuelve aquí y toca <strong>Reintentar GPS</strong></li>
      </ol>`;
    tabVerificar = `
      <strong style="display:block;margin-bottom:6px;color:#0a0a0a">Si el candado no aparece:</strong>
      <ol style="margin:0 0 12px;padding-left:22px;font-size:.88rem;line-height:1.7;color:#1f2937">
        <li>Ajustes Android → <strong>Apps</strong> → Chrome → <strong>Permisos</strong> → Ubicación</li>
        <li>Cambiar a <strong>"Permitir solo mientras se usa la app"</strong></li>
        <li>Verificar también que el <strong>GPS general</strong> esté ON (Ajustes → Ubicación)</li>
      </ol>`;
    tabOtras = `
      <strong style="display:block;margin-bottom:8px;color:#0a0a0a">Alternativas:</strong>
      <div style="display:flex;flex-direction:column;gap:8px;margin-bottom:10px">
        <button type="button" onclick="_gpsDismissHelpAnd('manual')"
                style="background:#0a0a0a;color:#fff;border:none;border-radius:8px;padding:12px 14px;font-weight:600;cursor:pointer;min-height:44px">
          <i class="bi bi-pencil-square"></i> Ingresar dirección manualmente
        </button>
        <button type="button" onclick="_gpsDismissHelpAnd('ip')"
                style="background:#fff;color:#0a0a0a;border:2px solid #0a0a0a;border-radius:8px;padding:12px 14px;font-weight:600;cursor:pointer;min-height:44px">
          <i class="bi bi-globe2"></i> Usar ubicación aproximada por IP
        </button>
      </div>`;
  } else {
    tabActivar = `
      <strong>Chrome / Edge / Firefox desktop:</strong>
      <ol style="margin:8px 0;padding-left:20px;font-size:.88rem;line-height:1.7;color:#1f2937">
        <li>Click el <strong>candado 🔒</strong> a la izquierda de la URL</li>
        <li>En "Ubicación" cambiá a <strong>"Permitir"</strong></li>
        <li>Refresca la página (Ctrl+R / F5) y toca Reintentar</li>
      </ol>`;
    tabVerificar = `
      <strong>Si seguís sin GPS:</strong>
      <ul style="margin:8px 0;padding-left:20px;font-size:.88rem;line-height:1.6;color:#1f2937">
        <li>Asegurate de tener <strong>HTTPS</strong> (la URL debe empezar con https://)</li>
        <li>Prueba en modo incógnito (Ctrl+Shift+N) para descartar extensiones</li>
        <li>Revisá Ajustes del SO: en Windows 11 → Configuración → Privacidad → Ubicación</li>
      </ul>`;
    tabOtras = `
      <p style="font-size:.88rem;color:#1f2937">En desktop puedes ingresar la dirección manualmente:</p>
      <button type="button" onclick="_gpsDismissHelpAnd('manual')"
              style="background:#0a0a0a;color:#fff;border:none;border-radius:8px;padding:12px 14px;font-weight:600;cursor:pointer">
        <i class="bi bi-pencil-square"></i> Ingresar dirección manualmente
      </button>`;
  }

  // Tab UI dentro del messageHtml
  const tabsCss = `
    <style>
      .gps-help-tabs{display:flex;gap:4px;margin-bottom:12px;border-bottom:2px solid #e5e7eb}
      .gps-help-tabs button{
        background:none;border:none;padding:9px 12px;font-size:.82rem;font-weight:600;
        color:#6b7280;cursor:pointer;border-bottom:3px solid transparent;margin-bottom:-2px;
        min-height:40px;flex:1;display:flex;align-items:center;justify-content:center;gap:5px
      }
      .gps-help-tabs button.active{color:#dc2626;border-bottom-color:#dc2626}
      .gps-help-panel{display:none;font-size:.85rem;color:#0a0a0a}
      .gps-help-panel.active{display:block}
    </style>`;
  // FIX 2026-05-17 (Daniel): tab "Alternativas" ELIMINADA — solo
  // se permite GPS real del dispositivo. Los tabs ahora son Activar + Verificar.
  const tabsHtml = `
    ${tabsCss}
    <div class="gps-help-tabs" role="tablist">
      <button type="button" class="active" data-tab="activar"
              onclick="this.parentElement.querySelectorAll('button').forEach(b=>b.classList.remove('active'));this.classList.add('active');document.querySelectorAll('.gps-help-panel').forEach(p=>p.classList.remove('active'));document.getElementById('gpsHelpPanel-activar').classList.add('active');return false;">
        <i class="bi bi-toggle-on"></i> Activar
      </button>
      <button type="button" data-tab="verificar"
              onclick="this.parentElement.querySelectorAll('button').forEach(b=>b.classList.remove('active'));this.classList.add('active');document.querySelectorAll('.gps-help-panel').forEach(p=>p.classList.remove('active'));document.getElementById('gpsHelpPanel-verificar').classList.add('active');return false;">
        <i class="bi bi-search"></i> Verificar
      </button>
    </div>
    <div class="gps-help-panel active" id="gpsHelpPanel-activar">${tabActivar}</div>
    <div class="gps-help-panel" id="gpsHelpPanel-verificar">${tabVerificar}</div>`;

  const titulo = isIOS ? 'Activa la ubicación en tu iPhone'
    : isAndroid ? 'Activa la ubicación en tu Android'
    : 'Activa la ubicación en el navegador';

  const reintentar = await ilusConfirm({
    title: titulo,
    message: tabsHtml,
    messageHtml: true,
    type: 'warning',
    okLabel: '🔄 Reintentar GPS',
    cancelLabel: 'Cerrar',
  });
  if (reintentar){
    // El "OK" del modal cuenta como gesto. Si el permiso ya está
    // 'granted' (porque el técnico volvió de Ajustes), getCurrentPosition
    // funciona aunque pase un setTimeout (los browsers cachean el estado
    // de gesto unos ms). Damos delay corto para que el modal se cierre
    // antes de mostrar el banner intermedio.
    setTimeout(() => pedirGPS(true), 150);
  }
}

// Helper: cierra el modal de ayuda GPS y ejecuta una acción al terminar.
// Usado por los botones "manual"/"ip" dentro del tab Alternativas.
function _gpsDismissHelpAnd(action){
  // ilusConfirm crea el modal con clase ilus-overlay y los botones data-idx="0/1"
  // El idx 0 = cancel (lo usamos para cerrar)
  try {
    const cancelBtn = document.querySelector('.ilus-overlay .ilus-btn[data-idx="0"]');
    if (cancelBtn) cancelBtn.click();
  } catch(e) { _gpsLog('dismiss_help_click_failed', e.message); }
  // Esperar a que termine la animación de cierre antes de abrir lo siguiente
  setTimeout(() => {
    if (action === 'manual') abrirManualGPS();
    else if (action === 'ip') _gpsForzarIP();
  }, 350);
}

// Atajo desde el tab "Alternativas" → fuerza IP geolocation directamente
async function _gpsForzarIP(){
  ilusToast('Obteniendo ubicación por IP…', { type:'info', duration: 1500 });
  try {
    const ip = await _gpsByIP();
    await _gpsRegistrarResultado(ip);
    ilusToast('📍 Ubicación aproximada por IP cargada', { type:'success' });
  } catch(e){
    ilusToast('No pudimos obtener IP geolocation: ' + (e.message || 'error'), { type:'error' });
  }
}

// ════════════════════════════════════════════════════════════════
//  Diagnóstico GPS — muestra estado completo de window.__gpsDiag
//  + botones para copiar al portapapeles y forzar re-prueba.
//  FIX 2026-05-17: si Daniel reporta "no funciona", tocar este botón
//  y mandar screenshot — ya tenemos todo lo necesario para depurar.
// ════════════════════════════════════════════════════════════════
async function mostrarDiagnosticoGPS(){
  const diag = window.__gpsDiag || { attempts: [], note: 'sin datos' };
  // Resumen de capacidades
  const caps = {
    https: location.protocol === 'https:',
    geolocation_api: !!navigator.geolocation,
    permissions_api: !!(navigator.permissions && navigator.permissions.query),
    user_agent: (navigator.userAgent || '').slice(0, 180),
    last_result_method: (window.__ilusGPS && window.__ilusGPS.method) || null,
    last_result_lat: window.__ilusGPS && window.__ilusGPS.lat,
    last_result_lng: window.__ilusGPS && window.__ilusGPS.lng,
    intentos: (diag.attempts || []).length,
  };
  // Probar permission state on-demand
  let permState = '(api no disponible)';
  if (navigator.permissions && navigator.permissions.query){
    try {
      const s = await navigator.permissions.query({ name: 'geolocation' });
      permState = s.state;
    } catch(e){ permState = 'error: ' + (e.message || 'unknown'); }
  }
  caps.permission_state = permState;

  // Tomar últimos 20 intentos (para no inundar el modal)
  const ultimosIntentos = (diag.attempts || []).slice(-20);
  const intentosHtml = ultimosIntentos.length === 0
    ? '<p style="color:#94a3b8;font-style:italic;font-size:.85rem">No hay intentos registrados aún. Toca "Reintentar GPS" para empezar.</p>'
    : ultimosIntentos.map((a, i) => {
        const dt = new Date(a.t || Date.now());
        const hh = String(dt.getHours()).padStart(2,'0') + ':' +
                   String(dt.getMinutes()).padStart(2,'0') + ':' +
                   String(dt.getSeconds()).padStart(2,'0');
        const isErr = /fail|denied|timeout|error/i.test(a.stage);
        const color = isErr ? '#dc2626' : '#15803d';
        const info = a.info ? JSON.stringify(a.info).slice(0, 200) : '';
        return `<div style="display:flex;gap:8px;padding:5px 0;border-bottom:1px solid #f1f5f9;font-family:monospace;font-size:.74rem">
          <span style="color:#94a3b8;min-width:60px">${hh}</span>
          <span style="color:${color};font-weight:700;min-width:180px">${_escapeHtml(a.stage || '?')}</span>
          <span style="color:#475569;word-break:break-all">${_escapeHtml(info)}</span>
        </div>`;
      }).join('');

  const capsHtml = `
    <div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;padding:10px;margin-bottom:12px;font-family:monospace;font-size:.78rem;line-height:1.5">
      <div><strong>HTTPS:</strong> ${caps.https ? '✓ sí' : '✗ NO (geolocation API NO funciona sin https)'}</div>
      <div><strong>Geolocation API:</strong> ${caps.geolocation_api ? '✓ disponible' : '✗ NO disponible'}</div>
      <div><strong>Permissions API:</strong> ${caps.permissions_api ? '✓ disponible' : '✗ NO'}</div>
      <div><strong>Permission state:</strong> <span style="color:${caps.permission_state === 'denied' ? '#dc2626' : caps.permission_state === 'granted' ? '#15803d' : '#f59e0b'}">${_escapeHtml(caps.permission_state)}</span></div>
      <div><strong>Último método OK:</strong> ${_escapeHtml(caps.last_result_method || '(ninguno)')}</div>
      ${caps.last_result_lat ? `<div><strong>Última coord:</strong> ${caps.last_result_lat.toFixed(5)}, ${caps.last_result_lng.toFixed(5)}</div>` : ''}
      <div><strong>Intentos registrados:</strong> ${caps.intentos}</div>
      <div style="margin-top:5px;color:#94a3b8;font-size:.7rem">UA: ${_escapeHtml(caps.user_agent)}</div>
    </div>`;

  const fullHtml = `
    <div style="max-height:60vh;overflow-y:auto">
      <h6 style="font-size:.82rem;font-weight:700;color:#0a0a0a;margin:0 0 6px">📊 Capacidades del navegador</h6>
      ${capsHtml}
      <h6 style="font-size:.82rem;font-weight:700;color:#0a0a0a;margin:0 0 6px">📜 Últimos 20 intentos GPS</h6>
      <div style="background:#fff;border:1px solid #e2e8f0;border-radius:8px;padding:8px 10px;max-height:280px;overflow-y:auto">
        ${intentosHtml}
      </div>
      <div style="margin-top:10px;display:flex;flex-wrap:wrap;gap:8px;justify-content:flex-end">
        <button type="button" id="gpsDiagCopy"
          style="background:#0a0a0a;color:#fff;border:none;border-radius:6px;padding:7px 12px;font-size:.75rem;font-weight:600;cursor:pointer">
          <i class="bi bi-clipboard"></i> Copiar al portapapeles
        </button>
        {# FIX 2026-05-17 — Envío del diagnóstico al servidor para que Daniel (admin)
           lo revise SIN tener que pedirle al técnico que conecte el iPhone a un Mac.
           El backend devuelve un ID corto que el técnico le comparte por WhatsApp
           para acelerar el lookup en /mantenciones/diagnostico-gps. #}
        <button type="button" id="gpsDiagSend"
          style="background:#dc2626;color:#fff;border:none;border-radius:6px;padding:7px 12px;font-size:.75rem;font-weight:700;cursor:pointer">
          <i class="bi bi-send-fill"></i> Enviar al admin
        </button>
      </div>
    </div>`;

  await ilusAlert({
    title: '🔍 Diagnóstico GPS',
    message: fullHtml,
    messageHtml: true,
    type: 'info',
    okLabel: 'Cerrar',
  });

  // Bind del botón copiar (luego del render del modal)
  setTimeout(() => {
    const btn = document.getElementById('gpsDiagCopy');
    if (btn){
      btn.addEventListener('click', () => {
        const full = JSON.stringify({ caps, attempts: diag.attempts || [] }, null, 2);
        if (navigator.clipboard && navigator.clipboard.writeText){
          navigator.clipboard.writeText(full).then(() => {
            ilusToast('📋 Copiado al portapapeles', { type:'success' });
          }).catch(() => ilusToast('No se pudo copiar', { type:'error' }));
        } else {
          // Fallback: textarea + execCommand
          const ta = document.createElement('textarea');
          ta.value = full; document.body.appendChild(ta);
          ta.select(); document.execCommand('copy');
          document.body.removeChild(ta);
          ilusToast('📋 Copiado', { type:'success' });
        }
      });
    }

    // ── Botón ENVIAR AL ADMIN ──────────────────────────────────────
    // Hace POST /api/diagnostico/gps con todo el contexto. El backend
    // guarda en mant_diag_gps y devuelve un id. Mostramos toast +
    // ilusAlert con el número para que el técnico se lo diga a Daniel.
    const btnSend = document.getElementById('gpsDiagSend');
    if (btnSend){
      btnSend.addEventListener('click', async () => {
        // Antibloqueo doble click
        btnSend.disabled = true;
        const original = btnSend.innerHTML;
        btnSend.innerHTML = '<i class="bi bi-hourglass-split"></i> Enviando…';
        try {
          // Último error: el más reciente que tenga substring 'fail/denied/timeout/error'
          let lastError = null;
          const attempts = diag.attempts || [];
          for (let i = attempts.length - 1; i >= 0; i--){
            const a = attempts[i];
            if (a && /fail|denied|timeout|error/i.test(a.stage || '')){
              try {
                lastError = (a.stage || '') + ' :: ' + JSON.stringify(a.info || {}).slice(0, 300);
              } catch(e){ lastError = a.stage || 'error'; }
              break;
            }
          }
          const body = {
            visita_id: (typeof VID !== 'undefined') ? VID : null,
            user_agent: navigator.userAgent || '',
            https: location.protocol === 'https:',
            permissions_state: caps.permission_state || '?',
            attempts: attempts,
            browser_features: {
              geolocation_api: caps.geolocation_api,
              permissions_api: caps.permissions_api,
              last_result_method: caps.last_result_method,
              last_result_lat: caps.last_result_lat,
              last_result_lng: caps.last_result_lng,
              standalone_pwa: !!(window.navigator.standalone || window.matchMedia('(display-mode: standalone)').matches),
              language: navigator.language || '',
              platform: navigator.platform || '',
              cookieEnabled: !!navigator.cookieEnabled,
              online: !!navigator.onLine,
              screen: {
                w: screen && screen.width, h: screen && screen.height,
                dpr: window.devicePixelRatio || 1,
              },
            },
            last_error: lastError,
          };
          const res = await fetch('/api/diagnostico/gps', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'X-Requested-With': 'XMLHttpRequest' },
            body: JSON.stringify(body),
            credentials: 'same-origin',
          });
          const data = await res.json().catch(() => ({}));
          if (!res.ok || !data.ok){
            const msg = (data && data.error) || 'Error de red (HTTP ' + res.status + ')';
            ilusToast('No se pudo enviar: ' + msg, { type:'error' });
            btnSend.disabled = false;
            btnSend.innerHTML = original;
            return;
          }
          ilusToast('Diagnóstico #' + data.id + ' enviado', { type:'success' });
          // Aviso con el código grande para que el técnico lo lea a Daniel
          await ilusAlert({
            title: 'Diagnóstico enviado',
            message: 'Mándale a Daniel este número por WhatsApp:',
            sub: '<div style="font-size:2rem;font-weight:800;color:#dc2626;text-align:center;font-family:monospace;letter-spacing:.05em">#' + data.id + '</div><div style="text-align:center;font-size:.78rem;color:#6b7280;margin-top:4px">Él lo revisará en el panel admin.</div>',
            subHtml: true,
            type: 'success',
            okLabel: 'Entendido',
          });
        } catch(e){
          ilusToast('No se pudo enviar: ' + (e.message || 'error de red'), { type:'error' });
          btnSend.disabled = false;
          btnSend.innerHTML = original;
        }
      });
    }
  }, 200);
}

// ════════════════════════════════════════════════════════════════
//  FALLBACK MANUAL: técnico escribe la dirección donde está
//  Usa Google Places (ilusPlacesAutocomplete) si está disponible.
// ════════════════════════════════════════════════════════════════
async function abrirManualGPS(){
  // Modal a pelo (no ilusPrompt porque queremos Places autocomplete dentro)
  // Construimos overlay manual + input + cancel/save buttons.
  let overlay = document.getElementById('manualGpsOverlay');
  if (overlay) overlay.remove();
  overlay = document.createElement('div');
  overlay.id = 'manualGpsOverlay';
  overlay.style.cssText = `
    position:fixed;inset:0;background:rgba(10,10,10,.72);z-index:99990;
    display:flex;align-items:center;justify-content:center;padding:16px;
    backdrop-filter:blur(3px);-webkit-backdrop-filter:blur(3px)`;
  overlay.innerHTML = `
    <div style="background:#fff;border-radius:14px;padding:20px;max-width:480px;width:100%;
                box-shadow:0 20px 60px rgba(0,0,0,.4);max-height:92dvh;overflow-y:auto">
      <div style="display:flex;align-items:center;gap:10px;margin-bottom:12px">
        <div style="width:44px;height:44px;border-radius:12px;background:#0a0a0a;color:#fff;
                    display:flex;align-items:center;justify-content:center;font-size:1.2rem">
          <i class="bi bi-pencil-square"></i>
        </div>
        <div>
          <h5 style="margin:0;font-weight:700;color:#0a0a0a">Ingresar dirección manualmente</h5>
          <small style="color:#6b7280">Tipea la dirección donde estás. Te sugerimos opciones reales.</small>
        </div>
      </div>
      <label style="display:block;font-size:.82rem;font-weight:600;color:#0a0a0a;margin-bottom:6px">
        Dirección actual <span style="color:#dc2626">*</span>
      </label>
      <input type="text" id="gpsManualDireccion"
             placeholder="Ej: Av. Apoquindo 4501, Las Condes, Santiago"
             autocomplete="off" autocorrect="off" spellcheck="false"
             style="width:100%;padding:12px 14px;border:2px solid #e5e7eb;border-radius:10px;
                    font-size:16px;outline:none;min-height:48px;color:#0a0a0a;background:#fff">
      <div id="gpsManualHint" style="margin-top:8px;font-size:.78rem;color:#6b7280">
        <i class="bi bi-info-circle"></i> Empieza a escribir y elige una sugerencia de Google para mejor precisión.
      </div>
      <div id="gpsManualError" style="display:none;margin-top:8px;padding:8px 10px;background:#fee2e2;
                                       border-radius:6px;color:#dc2626;font-size:.82rem"></div>
      <div style="display:flex;gap:8px;margin-top:18px;flex-direction:column">
        <button type="button" id="gpsManualSave"
                style="background:#dc2626;color:#fff;border:none;border-radius:10px;padding:12px;
                       font-weight:700;cursor:pointer;min-height:48px;font-size:.92rem">
          <i class="bi bi-check-lg"></i> Guardar y continuar
        </button>
        <button type="button" id="gpsManualCancel"
                style="background:#fff;color:#0a0a0a;border:2px solid #e5e7eb;border-radius:10px;
                       padding:10px;font-weight:600;cursor:pointer;min-height:44px">
          Cancelar
        </button>
      </div>
    </div>`;
  document.body.appendChild(overlay);

  // Datos seleccionados por Google Places
  let _picked = null;

  const input = overlay.querySelector('#gpsManualDireccion');
  const hint = overlay.querySelector('#gpsManualHint');
  const errBox = overlay.querySelector('#gpsManualError');

  // Inicializar autocomplete si Google Places está disponible
  if (typeof ilusPlacesAutocomplete === 'function'){
    ilusPlacesAutocomplete(input, {
      country: 'cl',
      types: ['address'],
      onPlaceSelected: (place) => {
        _picked = place;
        hint.innerHTML = `<i class="bi bi-check-circle-fill" style="color:#16a34a"></i> ` +
                        `<span style="color:#16a34a">Dirección verificada en Google</span>`;
        errBox.style.display = 'none';
      },
      onNoSelection: () => {
        _picked = null;
        hint.innerHTML = `<i class="bi bi-exclamation-triangle" style="color:#f59e0b"></i> ` +
                        `<span style="color:#92400e">Elegí una sugerencia para verificar la dirección</span>`;
      }
    });
  } else {
    hint.innerHTML = `<i class="bi bi-info-circle"></i> Escribí la dirección lo más completa posible.`;
  }

  setTimeout(() => input.focus(), 100);

  return new Promise((resolve) => {
    const close_ = (result) => {
      overlay.remove();
      // Si guardó algo, persistir al backend
      if (result){
        _gpsRegistrarResultado(result);
        ilusToast('📍 Dirección guardada manualmente', { type: 'success' });
      }
      resolve(result);
    };
    overlay.querySelector('#gpsManualCancel').addEventListener('click', () => close_(null));
    overlay.addEventListener('click', (e) => { if (e.target === overlay) close_(null); });
    overlay.querySelector('#gpsManualSave').addEventListener('click', async () => {
      const txt = (input.value || '').trim();
      if (!txt){
        errBox.style.display = 'block';
        errBox.textContent = 'Escribí una dirección para continuar.';
        input.focus();
        return;
      }
      // Caso A: Google ya geocodificó
      if (_picked && isFinite(_picked.lat) && isFinite(_picked.lng)){
        return close_({
          lat: _picked.lat, lng: _picked.lng,
          accuracy: 50, method: 'manual',
          dir: _picked.direccion || txt
        });
      }
      // Caso B: usuario escribió pero no seleccionó sugerencia.
      // Intentar geocodificar con Nominatim (gratuito) como respaldo.
      errBox.style.display = 'none';
      hint.innerHTML = '<i class="bi bi-arrow-repeat"></i> Buscando dirección…';
      try {
        const r = await fetch(`https://nominatim.openstreetmap.org/search?format=json&q=${encodeURIComponent(txt)}&countrycodes=cl&limit=1`,
          { headers: { 'Accept-Language': 'es' } });
        const j = await r.json();
        if (j && j[0]){
          return close_({
            lat: parseFloat(j[0].lat), lng: parseFloat(j[0].lon),
            accuracy: 200, method: 'manual',
            dir: j[0].display_name || txt
          });
        }
        errBox.style.display = 'block';
        errBox.textContent = 'No encontramos esa dirección. Prueba con más detalle (calle, número, comuna, ciudad).';
        hint.innerHTML = `<i class="bi bi-info-circle"></i> Empieza a escribir y elige una sugerencia.`;
      } catch(e){
        errBox.style.display = 'block';
        errBox.textContent = 'No pudimos verificar la dirección. Revisá tu conexión a internet.';
      }
    });
    // Cerrar con ESC
    document.addEventListener('keydown', function esc(e){
      if (e.key === 'Escape'){ close_(null); document.removeEventListener('keydown', esc); }
    });
  });
}

// Wrapper para tarea GPS — devuelve { lat, lng, dir } o null
async function _gpsManualPrompt(){
  return new Promise(async (resolve) => {
    const r = await abrirManualGPS();
    resolve(r);
  });
}

document.addEventListener('DOMContentLoaded', () => {
  // BUG FIX 2026-05-17 — Solo invocar GPS si la barra está renderizada.
  // Si la OT no tiene tareas GPS (hay_gps=false en Jinja), execGpsBar no
  // existe y no tiene sentido pedir permisos GPS al cargar.
  if (document.getElementById('execGpsBar')) {
    pedirGPS();
  }
  if (!RUTA_YA_INICIADA && DESTINO_DIR && window.innerWidth < 992){
    setTimeout(() => abrirModalRuta(), 1200);
  }
  if (RUTA_YA_INICIADA && VISITA_ESTADO !== 'cerrada' && VISITA_ESTADO !== 'firmada_tecnico' && VISITA_ESTADO !== 'pendiente_aprobacion'){
    iniciarPingPeriodico();
  }
});

// ─── Auto-refresh del banner cuando el usuario vuelve a la pestaña tras Ajustes ──
// FIX 2026-05-17: en iOS Safari, getCurrentPosition desde visibilitychange NO tiene
// gesto del usuario → falla silenciosamente. Lo que hacemos:
//   1. Actualizar el banner para que el técnico vea claro que tiene que tocar
//      el botón nuevamente.
//   2. Si permissions.query reporta 'granted', mostrar mensaje de éxito que
//      lo invite a re-capturar.
document.addEventListener('visibilitychange', () => {
  if (!document.hidden && !window.__ilusGPS){
    // Pequeño delay para que el navegador termine de re-renderizar
    setTimeout(() => {
      if (window.__ilusGPS) return;
      _gpsLog('visibility_back');
      // Re-render del banner para que el técnico vea el CTA grande otra vez
      const bar = document.getElementById('execGpsBar');
      const ttl = document.getElementById('execGpsTitle');
      const sub = document.getElementById('execGpsSub');
      const actions = document.getElementById('execGpsActions');
      if (!bar || !ttl || !sub) return;
      bar.classList.remove('gps-ok', 'gps-err');
      ttl.innerHTML = '<i class="bi bi-geo-alt"></i> Volviste — listo para capturar';
      sub.innerHTML = '<small>Si activaste el permiso en Ajustes, toca <strong>Capturar ahora</strong>.</small>';
      if (actions) actions.style.display = 'flex';
    }, 600);
  }
});

function abrirModalRuta(){
  if (!DESTINO_DIR){ ilusToast('Sin dirección registrada', { type:'warning' }); return; }
  new bootstrap.Modal(document.getElementById('modalRuta')).show();
}

async function iniciarRuta(app){
  bootstrap.Modal.getInstance(document.getElementById('modalRuta'))?.hide();
  const lat = window.__execOrigenLat || null;
  const lng = window.__execOrigenLng || null;
  try {
    await fetch(`/mantenciones/api/visitas/${VID}/iniciar-ruta`, {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ app, lat, lng })
    });
  } catch(e) {}
  const btn = document.getElementById('btnRuta');
  if (btn){
    btn.classList.add('iniciada');
    btn.innerHTML = `<i class="bi bi-check-circle-fill"></i> Ruta iniciada · ${app.toUpperCase()}`;
  }
  if (app === 'saltado'){ ilusToast('OK, comienza a trabajar', { type:'success' }); return; }
  const dest = (DESTINO_LAT && DESTINO_LNG) ? `${DESTINO_LAT},${DESTINO_LNG}` : encodeURIComponent(DESTINO_DIR);
  let url = '';
  if (app === 'google'){
    url = `https://www.google.com/maps/dir/?api=1&destination=${dest}&travelmode=driving`;
  } else if (app === 'waze'){
    url = (DESTINO_LAT && DESTINO_LNG)
      ? `https://www.waze.com/ul?ll=${DESTINO_LAT}%2C${DESTINO_LNG}&navigate=yes`
      : `https://www.waze.com/ul?q=${encodeURIComponent(DESTINO_DIR)}&navigate=yes`;
  }
  if (url) window.open(url, '_blank');
}

let _pingInterval = null;
function iniciarPingPeriodico(){
  if (_pingInterval || !navigator.geolocation) return;
  function ping(){
    // No pingear si la pestaña está oculta (ahorra batería + servidor)
    if (document.hidden) return;
    navigator.geolocation.getCurrentPosition(async (pos) => {
      try {
        await fetch(`/mantenciones/api/visitas/${VID}/ping-ruta`, {
          method: 'POST', headers: {'Content-Type':'application/json'},
          body: JSON.stringify({
            lat: pos.coords.latitude, lng: pos.coords.longitude,
            speed: pos.coords.speed, accuracy: pos.coords.accuracy,
          })
        });
      } catch(e) {}
    }, () => {}, { enableHighAccuracy: false, timeout: 8000, maximumAge: 25000 });
  }
  ping();
  _pingInterval = setInterval(ping, 30000);
}
function _stopPing(){ if (_pingInterval){ clearInterval(_pingInterval); _pingInterval = null; } }
window.addEventListener('beforeunload', _stopPing);
// FIX 2026-05-17: pausar el ping cuando la pestaña entra a background.
// Sin esto, el setInterval seguía spammeando requests aunque el navegador
// los encole; al volver del idle, varios pings disparaban a la vez contra
// una BD con conexión zombi, saturando workers.
document.addEventListener('visibilitychange', () => {
  if (document.hidden){
    _stopPing();
  } else if (RUTA_YA_INICIADA &&
             VISITA_ESTADO !== 'cerrada' &&
             VISITA_ESTADO !== 'firmada_tecnico' &&
             VISITA_ESTADO !== 'pendiente_aprobacion'){
    iniciarPingPeriodico();
  }
});

// ════════════════════════════════════════════════════════
//  FIRMAS — canvas interactivo con undo, color picker, has-state
// ════════════════════════════════════════════════════════
const _sig = {};

function initCanvas(id){
  const cv = document.getElementById(id);
  if (!cv) return;
  const wrap = document.getElementById('wrap-' + id);
  const dpr = window.devicePixelRatio || 1;
  const rect = cv.getBoundingClientRect();
  cv.width = rect.width * dpr; cv.height = rect.height * dpr;
  const ctx = cv.getContext('2d');
  ctx.scale(dpr, dpr);
  ctx.lineWidth = 3; ctx.lineCap = 'round'; ctx.lineJoin = 'round';
  ctx.strokeStyle = '#0f172a';
  _sig[id] = {
    ctx, drawing: false, last: null, has: false,
    color: '#0f172a', wrap,
    history: [],  // para undo: stack de imágenes Base64
  };

  function pushHistory(){
    try {
      _sig[id].history.push(cv.toDataURL('image/png'));
      if (_sig[id].history.length > 20) _sig[id].history.shift();  // max 20 niveles
    } catch(e) {}
  }
  function setHas(){
    _sig[id].has = true;
    if (wrap) wrap.classList.add('has-firma');
  }

  const start = (x,y) => {
    _sig[id].drawing = true;
    _sig[id].last = {x,y};
    pushHistory();  // guarda estado ANTES de dibujar (para undo)
  };
  const move = (x,y) => {
    if (!_sig[id].drawing) return;
    ctx.beginPath();
    ctx.moveTo(_sig[id].last.x, _sig[id].last.y);
    ctx.lineTo(x, y);
    ctx.stroke();
    _sig[id].last = {x, y};
    setHas();
  };
  const end = () => { _sig[id].drawing = false; _sig[id].last = null; };

  const getXY = (ev) => {
    const r = cv.getBoundingClientRect();
    if (ev.touches){ const t = ev.touches[0]; return {x:t.clientX-r.left, y:t.clientY-r.top}; }
    return {x:ev.clientX-r.left, y:ev.clientY-r.top};
  };
  cv.addEventListener('mousedown', e => { const p = getXY(e); start(p.x, p.y); });
  cv.addEventListener('mousemove', e => { const p = getXY(e); move(p.x, p.y); });
  cv.addEventListener('mouseup', end);
  cv.addEventListener('mouseleave', end);
  cv.addEventListener('touchstart', e => { e.preventDefault(); const p = getXY(e); start(p.x, p.y); }, { passive: false });
  cv.addEventListener('touchmove', e => { e.preventDefault(); const p = getXY(e); move(p.x, p.y); }, { passive: false });
  cv.addEventListener('touchend', end);
}

function limpiarFirma(id){
  const cv = document.getElementById(id);
  if (!cv) return;
  cv.getContext('2d').clearRect(0, 0, cv.width, cv.height);
  if (_sig[id]) {
    _sig[id].has = false;
    _sig[id].history = [];
    if (_sig[id].wrap) _sig[id].wrap.classList.remove('has-firma');
  }
}

function undoFirma(id){
  if (!_sig[id] || !_sig[id].history.length) return;
  const cv = document.getElementById(id);
  if (!cv) return;
  const ctx = cv.getContext('2d');
  const dataUrl = _sig[id].history.pop();
  const img = new Image();
  img.onload = () => {
    ctx.clearRect(0, 0, cv.width, cv.height);
    ctx.drawImage(img, 0, 0, cv.width / (window.devicePixelRatio||1), cv.height / (window.devicePixelRatio||1));
    if (_sig[id].history.length === 0){
      _sig[id].has = false;
      if (_sig[id].wrap) _sig[id].wrap.classList.remove('has-firma');
    }
  };
  img.src = dataUrl;
}

function setFirmaColor(id, color, el){
  if (!_sig[id]) return;
  _sig[id].color = color;
  _sig[id].ctx.strokeStyle = color;
  // Actualizar UI: marcar color activo
  const wrap = _sig[id].wrap;
  if (wrap){
    const toolbar = wrap.nextElementSibling;
    if (toolbar) toolbar.querySelectorAll('.firma-color').forEach(c => c.classList.remove('active'));
  }
  if (el) el.classList.add('active');
}
// 2026-06-12 (Daniel) — FIRMAS SEPARADAS. El mismo modal #modalFirma sirve en
// dos etapas: 'tecnico' (firma el técnico → firmada_tecnico) y 'cliente'
// (firma el cliente → pendiente_aprobacion). _firmaStage controla qué sección
// se muestra y a qué endpoint se postea.
let _firmaStage = 'tecnico';

function _firmaSetStage(stage){
  _firmaStage = stage;
  const secTec = document.getElementById('firmaSecTec');
  const secCli = document.getElementById('firmaSecCli');
  const titulo = document.getElementById('firmaModalTitulo');
  const info   = document.getElementById('firmaModalInfo');
  const lbl    = document.getElementById('btnFirmarConfirmLabel');
  if (stage === 'cliente'){
    if (secTec) secTec.style.display = 'none';
    if (secCli) secCli.style.display = '';
    if (titulo) titulo.innerHTML = '<i class="bi bi-person-badge me-2"></i>Firma del cliente';
    if (info)   info.innerHTML = '<i class="bi bi-info-circle me-1"></i>Al firmar el cliente, la OT pasa a <strong>"Pendiente de aprobación"</strong> y el contenido queda sellado.';
    if (lbl)    lbl.textContent = 'Registrar firma del cliente';
  } else {
    if (secTec) secTec.style.display = '';
    if (secCli) secCli.style.display = 'none';
    if (titulo) titulo.innerHTML = '<i class="bi bi-pen-fill me-2"></i>Firma del técnico';
    if (info)   info.innerHTML = '<i class="bi bi-info-circle me-1"></i>Al firmar como técnico, la OT pasa a <strong>"Firmada por técnico"</strong>. El ejecutivo SSTT podrá revisar/corregir antes de la firma del cliente.';
    if (lbl)    lbl.textContent = 'Firmar como técnico';
  }
}

async function abrirModalFirma(){
  // Si hay tareas opcionales sin completar, pedir confirmación explícita.
  const ctx = _calcCtxGlobal();
  const pendTotal = ctx.total - ctx.completas;
  const pendObl   = Math.max(0, ctx.oblTot - ctx.oblComp);
  const pendOpc   = pendTotal - pendObl;
  if (pendOpc > 0){
    const ok = await ilusConfirm({
      title: 'Tareas sin completar',
      message: `Quedan ${pendOpc} tarea${pendOpc === 1 ? '' : 's'} opcional${pendOpc === 1 ? '' : 'es'} sin registrar.`,
      sub: 'Podés firmar igual, pero los checklist quedarán incompletos en el informe.',
      okLabel: 'Firmar de todas formas',
      cancelLabel: 'Volver al checklist',
    });
    if (!ok) return;
  }
  _firmaSetStage('tecnico');
  new bootstrap.Modal(document.getElementById('modalFirma')).show();
  setTimeout(() => { initCanvas('canvasTec'); }, 250);
}

// Etapa CLIENTE — se abre desde el botón "Capturar firma del cliente" cuando
// la OT está en 'firmada_tecnico' (tras la revisión/corrección).
function abrirModalFirmaCliente(){
  _firmaSetStage('cliente');
  new bootstrap.Modal(document.getElementById('modalFirma')).show();
  setTimeout(() => { initCanvas('canvasCli'); }, 250);
}

// SUPERADMIN: liberar la firma del técnico → la OT vuelve a 'en_ejecucion' para
// que el técnico pueda firmar de nuevo (caso: no alcanzó a firmar bien en sitio).
async function liberarFirmaTecnico(){
  const ok = await ilusConfirm({
    title: 'Liberar firma del técnico',
    message: '¿Liberar la firma del técnico de esta OT?',
    sub: 'La OT vuelve a edición (en ejecución) y el técnico podrá firmar de nuevo. Queda registrado en la bitácora. Solo superadmin.',
    okLabel: 'Liberar firma', cancelLabel: 'Cancelar', danger: true,
  });
  if (!ok) return;
  try {
    const r = await fetch(`/mantenciones/api/visitas/${VID}/liberar-firma-tecnico`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
    });
    const d = await r.json().catch(() => ({}));
    if (!r.ok || !d.ok){ await ilusAlert({ title: 'No se pudo', message: (d.error || 'Error'), type: 'error' }); return; }
    ilusToast('✓ Firma del técnico liberada — la OT volvió a edición', { type: 'success' });
    setTimeout(() => location.reload(), 900);
  } catch (e){ await ilusAlert({ title: 'Error de red', message: e.message, type: 'error' }); }
}

// Enviar al cliente un link para firmar la OT a distancia (cuando no está en sitio).
async function enviarFirmaRemota(){
  const email = await ilusPrompt({
    title: 'Enviar firma al cliente',
    message: 'Correo del cliente para enviarle el link de firma:',
    sub: 'Déjalo vacío para usar el correo registrado del cliente. El link vence en 5 días; al firmar nos avisa a todos.',
    placeholder: 'cliente@correo.cl', inputType: 'email', required: false,
  });
  if (email === null) return;
  try {
    const r = await fetch(`/mantenciones/api/visitas/${VID}/enviar-firma-remota`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email: (email || '').trim() }),
    });
    const d = await r.json().catch(() => ({}));
    if (!r.ok || !d.ok){ await ilusAlert({ title: 'No se pudo', message: (d.error || 'Error'), type: 'error' }); return; }
    if (d.enviado){
      await ilusAlert({ title: '✅ Link enviado', message: d.mensaje || ('Link de firma enviado a ' + d.email), type: 'success' });
    } else {
      await ilusAlert({ title: 'Link generado', message: (d.mensaje || ''), sub: (d.link || ''), type: 'warning' });
    }
  } catch (e){ await ilusAlert({ title: 'Error de red', message: e.message, type: 'error' }); }
}

async function enviarFirma(){
  const btn = document.getElementById('btnFirmarConfirm');
  if (_firmaStage === 'cliente'){ return _enviarFirmaCliente(btn); }

  // ── Etapa TÉCNICO ──
  const cvTec = document.getElementById('canvasTec');
  if (!_sig.canvasTec || !_sig.canvasTec.has){
    ilusToast('Falta tu firma', { type:'warning' }); return;
  }
  const firmaTec = cvTec.toDataURL('image/png');
  btn.disabled = true; btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Enviando…';
  try {
    const r = await fetch(`/mantenciones/api/visitas/${VID}/firmar-revision`, {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify({
        firma_tecnico: firmaTec,
        firma_tecnico_nombre: document.getElementById('firmaTecNombre').value.trim(),
      })
    });
    const d = await r.json();
    if (d.ok){
      bootstrap.Modal.getInstance(document.getElementById('modalFirma'))?.hide();
      await ilusAlert({
        title: '✅ Técnico firmó',
        message: 'La OT quedó en revisión. El ejecutivo SSTT puede corregir antes de la firma del cliente.',
        type: 'success',
      });
      setTimeout(() => location.reload(), 600);
    } else {
      // Gate de cierre (Fase 10): si faltan tareas por cubrir, detalle por equipo.
      if (Array.isArray(d.faltantes) && d.faltantes.length){
        await ilusAlert({
          title: '🚧 No puedes firmar esta OT todavía',
          message: d.error || 'Quedan tareas obligatorias sin completar ni justificar.',
          sub: d.faltantes.join(' · '),
          type: 'warning',
          okLabel: 'Entendido',
        });
      } else {
        ilusToast(d.error || 'Error', { type:'error' });
      }
      btn.disabled = false; btn.innerHTML = '<i class="bi bi-check-lg me-1"></i><span id="btnFirmarConfirmLabel">Firmar como técnico</span>';
    }
  } catch(e){
    ilusToast('Error de red', { type:'error' });
    btn.disabled = false; btn.innerHTML = '<i class="bi bi-check-lg me-1"></i><span id="btnFirmarConfirmLabel">Firmar como técnico</span>';
  }
}

async function _enviarFirmaCliente(btn){
  const cvCli = document.getElementById('canvasCli');
  if (!_sig.canvasCli || !_sig.canvasCli.has){
    ilusToast('Falta la firma del cliente', { type:'warning' }); return;
  }
  const cliNombreEl = document.getElementById('firmaCliNombre');
  const cliNombre = (cliNombreEl && cliNombreEl.value || '').trim();
  const cliRut = ((document.getElementById('firmaCliRut') || {}).value || '').trim();
  if (!cliNombre){ ilusToast('Falta el nombre de quien firma por el cliente', { type:'warning' }); return; }
  if (cliRut.replace(/[^0-9kK]/g,'').length < 7){ ilusToast('Falta el RUT de quien firma (RUT chileno válido)', { type:'warning' }); return; }
  const firmaCli = cvCli.toDataURL('image/png');
  btn.disabled = true; btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Enviando…';
  try {
    const r = await fetch(`/mantenciones/api/visitas/${VID}/firmar-cliente`, {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify({
        firma_cliente: firmaCli,
        firma_cliente_nombre: cliNombre,
        firma_cliente_rut: cliRut,
        firma_cliente_cargo: ((document.getElementById('firmaCliCargo') || {}).value || '').trim(),
        firma_cliente_tel: ((document.getElementById('firmaCliTel') || {}).value || '').trim(),
        firma_cliente_email: ((document.getElementById('firmaCliEmail') || {}).value || '').trim(),
        firma_cliente_sugerido_nombre: (cliNombreEl && cliNombreEl.dataset ? (cliNombreEl.dataset.sugerido || '') : ''),
      })
    });
    const d = await r.json();
    if (d.ok){
      bootstrap.Modal.getInstance(document.getElementById('modalFirma'))?.hide();
      await ilusAlert({
        title: '✅ Cliente firmó',
        message: 'La OT pasó a aprobación final. El ejecutivo SSTT la cerrará.',
        type: 'success',
      });
      setTimeout(() => location.reload(), 600);
    } else {
      ilusToast(d.error || 'Error', { type:'error' });
      btn.disabled = false; btn.innerHTML = '<i class="bi bi-check-lg me-1"></i><span id="btnFirmarConfirmLabel">Registrar firma del cliente</span>';
    }
  } catch(e){
    ilusToast('Error de red', { type:'error' });
    btn.disabled = false; btn.innerHTML = '<i class="bi bi-check-lg me-1"></i><span id="btnFirmarConfirmLabel">Registrar firma del cliente</span>';
  }
}

// ════════════════════════════════════════════════════════════════
//  FLUJO DE 3 FIRMAS — APROBACIÓN FINAL (creador / admin / supervisor)
//  2026-05-18 (Daniel): tras la firma del técnico+cliente, el creador
//  de la OT (o admin/supervisor) entra desde computador y firma como
//  aprobador. Al confirmar, la OT pasa a 'cerrada'.
// ════════════════════════════════════════════════════════════════

function abrirModalAprobacion(){
  const modalEl = document.getElementById('modalAprobacion');
  if (!modalEl){
    ilusToast('Modal de aprobación no disponible', { type:'error' });
    return;
  }
  new bootstrap.Modal(modalEl).show();
  // Init canvas con un pequeño delay para que el modal calcule layout
  setTimeout(() => { initCanvas('canvasAprob'); }, 250);
}

async function confirmarAprobacion(){
  const cv = document.getElementById('canvasAprob');
  if (!_sig.canvasAprob || !_sig.canvasAprob.has){
    ilusToast('Falta tu firma como aprobador', { type:'warning' });
    return;
  }
  const firma = cv.toDataURL('image/png');
  const nombre = (document.getElementById('aprobNombre')?.value || '').trim();
  const comentario = (document.getElementById('aprobComentario')?.value || '').trim();
  const btn = document.getElementById('btnAprobConfirm');
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Cerrando OT…';
  try {
    const r = await fetch(`/mantenciones/api/visitas/${VID}/aprobar-cierre`, {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({
        firma_supervisor: firma,
        firma_supervisor_nombre: nombre,
        comentario: comentario,
      })
    });
    const d = await r.json();
    if (d.ok){
      bootstrap.Modal.getInstance(document.getElementById('modalAprobacion'))?.hide();
      await ilusAlert({
        title: '✅ OT cerrada',
        message: 'Firmaste como aprobador. La OT quedó cerrada.',
        sub: 'El cliente recibirá la confirmación. El PDF ya está disponible.',
        type: 'success',
        okLabel: 'Ver OT',
      });
      setTimeout(() => location.reload(), 400);
    } else {
      ilusToast(d.error || 'Error al cerrar la OT', { type:'error' });
      btn.disabled = false;
      btn.innerHTML = '<i class="bi bi-check-lg me-1"></i>Firmar y cerrar OT';
    }
  } catch(e){
    ilusToast('Error de red al cerrar', { type:'error' });
    btn.disabled = false;
    btn.innerHTML = '<i class="bi bi-check-lg me-1"></i>Firmar y cerrar OT';
  }
}

async function rechazarAprobacion(){
  // Bootstrap mantiene un focus-trap dentro del modal abierto: el input
  // del ilusPrompt recibe foco visual pero el teclado no llega.
  // Solución: cerrar el modal primero, esperar que el backdrop se elimine
  // completamente (hidden.bs.modal), luego abrir el prompt.
  const _modalEl = document.getElementById('modalAprobacion');
  const _bsModal = bootstrap.Modal.getInstance(_modalEl);
  await new Promise(resolve => {
    if (_bsModal && (_modalEl || {}).classList && _modalEl.classList.contains('show')) {
      _modalEl.addEventListener('hidden.bs.modal', resolve, { once: true });
      _bsModal.hide();
    } else {
      resolve();
    }
  });

  const motivo = await ilusPrompt({
    title: 'Rechazar y devolver al técnico',
    message: 'Indica el motivo del rechazo:',
    sub: 'La OT volverá a estado "programada" y el técnico podrá corregir y firmar de nuevo.',
    placeholder: 'Ej: falta foto del equipo X, diagnóstico no es claro…',
    required: true,
    okLabel: 'Rechazar OT',
    cancelLabel: 'Cancelar',
  });
  if (!motivo) {
    if (_bsModal) _bsModal.show();
    return;
  }
  const btn = document.getElementById('btnRechazarAprob');
  if (btn){
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Rechazando…';
  }
  try {
    const r = await fetch(`/mantenciones/api/visitas/${VID}/rechazar-cierre`, {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ motivo: motivo })
    });
    const d = await r.json();
    if (d.ok){
      bootstrap.Modal.getInstance(document.getElementById('modalAprobacion'))?.hide();
      await ilusAlert({
        title: 'OT devuelta al técnico',
        message: 'La OT volvió a estado "programada".',
        sub: 'El técnico deberá corregir y firmar de nuevo.',
        type: 'warning',
      });
      setTimeout(() => location.reload(), 400);
    } else {
      ilusToast(d.error || 'Error al rechazar', { type:'error' });
      if (btn){
        btn.disabled = false;
        btn.innerHTML = '<i class="bi bi-x-circle me-1"></i>Rechazar y devolver';
      }
    }
  } catch(e){
    ilusToast('Error de red', { type:'error' });
    if (btn){
      btn.disabled = false;
      btn.innerHTML = '<i class="bi bi-x-circle me-1"></i>Rechazar y devolver';
    }
  }
}

// ════════════════════════════════════════════════════════════════
//  PWA — Instalación en iOS
//  Muestra el banner solo si:
//    · El dispositivo es iOS (iPhone/iPad)
//    · Y la app NO está corriendo en modo standalone (PWA instalada)
//  Las PWA en iOS mantienen los permisos de geolocation con más
//  estabilidad que Safari abierto — por eso pedimos instalación.
// ════════════════════════════════════════════════════════════════
(function detectIosAndShowInstallBanner(){
  try {
    const ua = navigator.userAgent || '';
    const isIos = /iPad|iPhone|iPod/.test(ua)
               || (navigator.platform === 'MacIntel' && navigator.maxTouchPoints > 1); // iPad iOS 13+
    const isStandalone = !!(window.navigator.standalone
                           || window.matchMedia('(display-mode: standalone)').matches);
    if (isIos && !isStandalone){
      const el = document.getElementById('iosInstallBanner');
      if (el) el.style.display = 'block';
    }
  } catch(e){ /* defensivo — nunca romper la página por el banner */ }
})();

// Instrucciones para "Añadir a pantalla de inicio" en iOS Safari.
// Modal informativo — usa ilusAlert (regla #1 de CLAUDE.md).
async function mostrarInstrIosInstall(){
  await ilusAlert({
    title: 'Instalar ILUS en tu iPhone',
    message: 'Sigue estos 3 pasos en Safari:',
    sub: `
      <ol style="text-align:left;font-size:.86rem;line-height:1.7;padding-left:20px;margin:8px 0 0">
        <li>Toca el botón <strong>Compartir</strong>
          <i class="bi bi-box-arrow-up" style="color:#dc2626"></i>
          en la barra inferior de Safari.
        </li>
        <li>Desliza hacia abajo y elige
          <strong>"Añadir a pantalla de inicio"</strong>
          <i class="bi bi-plus-square" style="color:#dc2626"></i>.
        </li>
        <li>Confirma el nombre <strong>ILUS</strong> y toca <strong>Agregar</strong>.</li>
      </ol>
      <div style="background:#fff8e1;border:1px solid #f59e0b;color:#92400e;padding:8px 10px;border-radius:6px;font-size:.78rem;margin-top:10px">
        <i class="bi bi-lightbulb"></i>
        <strong>Importante:</strong> abre ILUS desde el ícono en tu pantalla
        de inicio (no desde Safari). Los permisos del GPS quedan más estables
        cuando entras como app.
      </div>
    `,
    subHtml: true,
    type: 'info',
    okLabel: 'Entendido',
  });
}

/* ---- (limite del <script> original en el template) ---- */

// ════════════════════════════════════════════════════════════════
// LEVANTAMIENTO PURO (descubrimiento) — captura rápida de equipos
// nuevos en terreno. Daniel 2026-06-23. Los items van a
// mant_levantamiento_items (maquina_id NULL) y al cerrar la OT se
// materializan en mant_maquinas (ficha del cliente).
// ════════════════════════════════════════════════════════════════
let _levdItems = [];      // items descubiertos (maquina_id NULL) ya guardados
let _levdFotos = [];      // fotos pendientes (nuevas) del modal actual [{blob,url}]
let _levdFotosExistentes = []; // fotos YA subidas del item en edición [{id,url}]
let _levdUltimo = null;   // último equipo guardado (para Duplicar)
let _levdSeq = 1;         // correlativo para la serie sugerida
let _levdModal = null;
let _levdDoc = null;      // factura ERP verificada {tido, numero, fecha}
let _levdEditId = null;   // 2026-07-06: id del item en edición (null = modo crear)
let _levdCtx = null;      // 2026-08-08: contexto de autoguardado del equipo actual (ver levdAbrir)

function _levdEl(id){ return document.getElementById(id); }

async function levdInit(){
  if (!_levdEl('levdWrap') || !VISITA_LEVANTAMIENTO_ID) return;
  try {
    const r = await fetch(`/mantenciones/api/levantamientos/${VISITA_LEVANTAMIENTO_ID}`);
    const d = await r.json();
    if (!d.ok) return;
    _levdItems = (d.items || []).filter(it => !it.maquina_id);
    // La serie sugerida sigue desde el total de items del levantamiento
    _levdSeq = (d.items || []).length + 1;
    // 2026-08-08 (Daniel): la tarjeta "Levantamiento de descubrimiento" solo
    // tiene sentido antes de descubrir el primer equipo -- una vez hay al
    // menos 1, ocultarla (ya no es "sin equipos registrados").
    const _emptyBanner = _levdEl('levdEmptyBanner');
    if (_emptyBanner) _emptyBanner.style.display = _levdItems.length ? 'none' : '';
    levdRender();
    // Recalcular lock del botón firmar: en descubrimiento puro el conteo
    // de equipos descubiertos cambia el estado habilitado/bloqueado.
    // levdInit() es el único punto que refresca _levdItems (carga inicial +
    // tras cada guardar/eliminar), así que un solo hook cubre los 3 flujos.
    try { actualizarLockFirmar(_calcCtxGlobal()); } catch(_) {}
  } catch(e){ console.warn('[levd] init:', e); }
}

function levdRender(){
  const list = _levdEl('levdList'), cnt = _levdEl('levdCount');
  if (!list) return;
  cnt.textContent = _levdItems.length;
  if (!_levdItems.length){
    list.innerHTML = '<div class="text-muted small" style="padding:2px 4px">' +
      'Aún no capturas equipos. Toca el botón rojo para partir. 📸</div>';
    return;
  }
  list.innerHTML = _levdItems.map(it => {
    const foto = (it.fotos && it.fotos.length) ? it.fotos[0].url : '';
    const dano = (it.anomalias || '').trim();
    const fs = (it.estado_capturado === 'fuera_servicio');
    // 2026-07-06 (Daniel — ventana de corrección), ENDURECIDO 2026-07-08:
    // editar/eliminar libres mientras LEV_EDITABLE; tras firmar, TODOS
    // (incluido superadmin) solo pueden VISUALIZAR — candado para todos,
    // sin excepción de rol.
    let acciones;
    if (LEV_EDITABLE){
      // 2026-08-08 (Daniel): "el técnico no tiene por qué borrar máquina, así
      // que hay que sacar el botón de la basura". Solo se oculta al TÉCNICO —
      // admin/supervisor/ejecutivo/superadmin lo conservan (REGLA #4.2: no se
      // elimina la feature, se condiciona por rol). El backend también lo
      // bloquea (mant_lev_item_update, error_codigo LEV_TECNICO_SIN_DELETE):
      // esconder el botón sin cerrar la API no sería seguridad real.
      const _puedeBorrar = !(typeof IS_TECNICO !== 'undefined' && IS_TECNICO);
      acciones = `<button type="button" class="del" style="background:#eff6ff;color:#1d4ed8;margin-right:6px" onclick="levdEditar(${it.id})" title="Editar"><i class="bi bi-pencil-fill"></i></button>` +
                 (_puedeBorrar
                   ? `<button type="button" class="del" onclick="levdEliminar(${it.id})" title="Eliminar"><i class="bi bi-trash"></i></button>`
                   : '');
    } else {
      acciones = `<span style="color:#9ca3af;font-size:1.1rem" title="Congelado tras la firma del técnico — solo visualización"><i class="bi bi-lock-fill"></i></span>`;
    }
    // 2026-08-08 (Daniel: "la fecha está en formato gringo, y la hora no
    // corresponde a la región de Santiago"). El backend ya entrega
    // modificado_at pasado por chile_fmt_filter ("08/08/2026 14:53", hora
    // Chile). Antes acá se hacía .slice(0,16).replace('T',' ') sobre el UTC
    // crudo — justo lo que la REGLA #6 prohíbe (cortar el ISO a mano).
    const editInfo = it.modificado_por
      ? `<div class="mt" style="color:#b45309"><i class="bi bi-pencil-square me-1"></i>Editado por ${it.modificado_por}${it.modificado_at ? ' · ' + it.modificado_at : ''}</div>`
      : '';
    // ── Semáforo de la franja izquierda (Daniel 2026-08-08: "algo sutil de
    //    ver y decir: ah ok, ya se liberó ese producto"). Verde = liberado.
    //    Rojo = le falta algo, y el tooltip dice qué. Sin texto extra en la
    //    tarjeta: la señal es el color.
    // FIX 2026-08-08 (Daniel, caso EVA Mat): la versión anterior también
    // exigía "serie real" (no la sugerida LEV<vid>-N) para poner verde.
    // Mal criterio: hay equipos SIN serie de fábrica (mats, correas, bandas)
    // — Daniel tenía las 4 fotos y todo declarado, y igual salía rojo por
    // esto. Único criterio real de "liberado": tiene evidencia (foto). La
    // serie sigue siendo editable y sugerida, pero no bloquea el semáforo.
    const _falta = [];
    if (((it.fotos||[]).length || it.n_fotos || 0) === 0) _falta.push('falta al menos 1 foto');
    const _semClass = _falta.length ? ' falta' : '';
    const _semTitle = _falta.length
      ? 'Falta: ' + _falta.join(' · ')
      : 'Equipo liberado: con evidencia fotográfica';

    return `<div class="levd-card${_semClass}" title="${_escapeHtml(_semTitle)}">
      ${foto ? `<img src="${foto}" alt="">` : `<div style="width:52px;height:52px;border-radius:9px;background:#f3f4f6;display:flex;align-items:center;justify-content:center"><i class="bi bi-camera text-muted"></i></div>`}
      <div class="inf">
        <div class="nm">${(it.nombre_snap||'Equipo')}
          ${dano ? '<span class="levd-badge dano">DAÑO</span>' : ''}
          ${fs ? '<span class="levd-badge fs">FUERA DE SERVICIO</span>' : ''}
        </div>
        <div class="mt">${it.serie_snap ? 'Serie: ' + it.serie_snap : ''}${it.sku_snap ? ' · SKU: ' + it.sku_snap : ''} · ${(it.fotos||[]).length || it.n_fotos || 0} foto(s)${it.doc_origen ? ' · 🧾 ' + it.doc_origen : ''}</div>
        ${editInfo}
      </div>
      ${acciones}
    </div>`;
  }).join('');
}

function _levdSerieSugerida(){
  return `LEV${VID}-${String(_levdSeq).padStart(3,'0')}`;
}

// ══════════════════════════════════════════════════════════════════════
// AUTOGUARDADO + GEOCERCA (2026-08-08, Daniel: "todos los datos se van a
// guardar de manera automática, sin necesidad de presionar guardar, pero
// la ubicación nunca se va a guardar hasta que yo le dé al botón de
// guardar al equipo"). _levdCtx agrupa TODO lo que antes vivía en
// variables sueltas por-campo, para que un timer de un equipo nunca
// pueda escribir sobre el equipo que el técnico abrió después (bug real
// encontrado en la revisión adversarial del diseño original).
// ══════════════════════════════════════════════════════════════════════
function _levdNuevoUid(){
  try { if (window.crypto && crypto.randomUUID) return crypto.randomUUID(); } catch(_e){}
  return 'u' + Date.now() + Math.random().toString(36).slice(2, 10);
}

function _levdNoEsta(){
  const el = _levdEl('levd_no_esta');
  return !!(el && el.checked);
}

function _levdSnapshotCampos(){
  const dano = _levdEl('levd_dano').checked;
  const operativa = _levdEl('levd_operativa').checked;
  return {
    nombre: _levdEl('levd_nombre').value.trim(),
    sku: _levdEl('levd_sku').value.trim(),
    serie: _levdEl('levd_serie').value.trim(),
    ubicacion: _levdEl('levd_ubicacion').value.trim(),
    observaciones: _levdEl('levd_obs').value.trim(),
    anomalias: dano ? _levdEl('levd_dano_txt').value.trim() : '',
    // 2026-08-09 (Daniel: "si el equipo no está, se anula cualquier
    // obligatoriedad... solamente se declara que no está"): estado
    // dedicado, ya existía como valor válido de estado_capturado
    // (reusado del flujo de "saltar equipo" existente).
    estado_capturado: _levdNoEsta() ? 'no_encontrado'
                      : !operativa ? 'fuera_servicio' : (dano ? 'advertencia' : 'operativo'),
    doc_tido: _levdDoc ? _levdDoc.tido : '',
    doc_numero: _levdDoc ? _levdDoc.numero : '',
    doc_fecha: _levdDoc ? _levdDoc.fecha : '',
  };
}

// Toggle "el equipo no está" -- SIEMPRE el primer control del modal. Al
// activarse, los pasos 1-5 quedan visualmente atenuados (ya no obligatorios,
// ver levdRefreshStepStates) y el estado_capturado autoguardado pasa a
// 'no_encontrado' de inmediato.
function levdToggleNoEsta(checked){
  ['levdStep1','levdStep2','levdStep3','levdStep4','levdStep5'].forEach(id => {
    const sec = _levdEl(id);
    if (sec) sec.classList.toggle('levd-step-irrelevante', checked);
  });
  if (_levdCtx) _levdMarcarSucio('estado_capturado');
  levdRefreshStepStates();
}

// Mutex ÚNICO de creación compartido por TODOS los campos y fotos del
// equipo actual — si 2 campos cambian casi a la vez antes de que exista
// iid, ambos esperan la MISMA promesa en vez de disparar 2 POST (el
// segundo, sin esto, choca contra la BD y su dato se podía perder).
function _levdEnsureIid(ctx){
  if (ctx.iid) return Promise.resolve(ctx.iid);
  if (ctx.creating) return ctx.creating;
  ctx.creating = (async () => {
    const r = await fetch(`/mantenciones/api/levantamientos/${VISITA_LEVANTAMIENTO_ID}/items`, {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify(Object.assign(_levdSnapshotCampos(), { client_uid: ctx.clientUid })),
    });
    const d = await r.json().catch(() => ({}));
    if (!r.ok || !d.ok || !d.id) throw new Error(d.error || 'No se pudo crear el equipo');
    ctx.iid = d.id;
    return d.id;
  })();
  ctx.creating.catch(() => { ctx.creating = null; });
  return ctx.creating;
}

const _LEVD_DEBOUNCE_MS = 800;
const _LEVD_CAMPOS_AUTOSAVE = ['nombre','sku','serie','ubicacion','observaciones',
                                'anomalias','estado_capturado','doc_numero'];

function _levdAutosaveEstado(txt, color){
  const el = _levdEl('levdAutosaveEstado');
  if (el){ el.textContent = txt; el.style.color = color; }
}

async function _levdEnviarCampo(ctx, campo){
  ctx.timers.delete(campo);
  if (ctx !== _levdCtx) return;   // el modal ya cambió de equipo — no escribir encima de otro
  const snap = _levdSnapshotCampos();
  const val = snap[campo];
  if (ctx.ultimoOk[campo] === val) return;
  try {
    const iid = await _levdEnsureIid(ctx);
    if (ctx !== _levdCtx) return;
    const payload = {};
    payload[campo] = val;
    // anomalias/estado_capturado se derivan de 2 controles (switch + texto),
    // y doc_* de 3 inputs — viajan juntos para que el backend nunca reciba
    // una combinación a medias (ej. anomalias sin el estado que le corresponde).
    if (campo === 'anomalias' || campo === 'estado_capturado'){
      payload.anomalias = snap.anomalias; payload.estado_capturado = snap.estado_capturado;
    }
    if (campo === 'doc_numero'){
      payload.doc_tido = snap.doc_tido; payload.doc_numero = snap.doc_numero; payload.doc_fecha = snap.doc_fecha;
    }
    _levdAutosaveEstado('Guardando…', '#9ca3af');
    const r = await fetch(`/mantenciones/api/levantamiento-items/${iid}`, {
      method: 'PATCH', headers: {'Content-Type':'application/json'}, body: JSON.stringify(payload),
    });
    if (ctx !== _levdCtx) return;
    if (r.ok){ ctx.ultimoOk[campo] = val; _levdAutosaveEstado('Guardado ✓', '#15803d'); }
    else { _levdAutosaveEstado('No se pudo guardar — reintenta', '#dc2626'); }
  } catch(_e){
    if (ctx === _levdCtx) _levdAutosaveEstado('Sin conexión — reintenta', '#dc2626');
  }
}

function _levdMarcarSucio(campo){
  const ctx = _levdCtx;
  if (!ctx) return;
  if (ctx.timers.has(campo)) clearTimeout(ctx.timers.get(campo));
  ctx.timers.set(campo, setTimeout(() => _levdEnviarCampo(ctx, campo), _LEVD_DEBOUNCE_MS));
}

// Dispara YA lo que esté pendiente (nunca lo descarta) — se llama antes de
// abrir otro equipo, cerrar el modal, o finalizar. Daniel nunca pidió esto
// explícitamente, pero es lo que hace que "se guarda solo" sea cierto de
// verdad: sin este flush, cambiar de equipo en menos de 800ms perdía la
// última letra escrita en silencio.
function _levdFlushPendientes(ctx){
  if (!ctx) return Promise.resolve();
  const ps = [];
  for (const [campo, t] of ctx.timers){
    clearTimeout(t);
    ps.push(_levdEnviarCampo(ctx, campo));
  }
  ctx.timers.clear();
  return Promise.all(ps);
}

function _levdHaversineM(lat1, lon1, lat2, lon2){
  const rad = Math.PI / 180, R = 6371000;
  const dLat = (lat2 - lat1) * rad, dLon = (lon2 - lon1) * rad;
  const a = Math.sin(dLat/2)**2 + Math.cos(lat1*rad)*Math.cos(lat2*rad)*Math.sin(dLon/2)**2;
  return 2 * R * Math.asin(Math.sqrt(a));
}

// GPS: se pide UNA sola vez, en el click de "Guardar equipo" (Daniel:
// "el único dato que no se va a guardar si no tiene la ubicación"). Hace
// hasta 2 intentos quedándose con la lectura MÁS PRECISA — un GPS de
// celular puede reportar ±800m en interiores, comparar ciegamente contra
// el radio sin mirar la precisión reportada (coords.accuracy) haría que
// esto fuera una moneda al aire.
function _levdGeoObtener(){
  return new Promise((resolve, reject) => {
    if (!navigator.geolocation){ reject({codigo:'GPS_NO_DISPONIBLE'}); return; }
    let mejor = null, intentos = 0;
    const intentar = () => {
      intentos++;
      navigator.geolocation.getCurrentPosition(
        (p) => {
          const lec = { lat: p.coords.latitude, lng: p.coords.longitude, acc: p.coords.accuracy };
          if (!mejor || lec.acc < mejor.acc) mejor = lec;
          if (intentos < 2 && mejor.acc > 150) { intentar(); return; }
          resolve(mejor);
        },
        (err) => {
          if (mejor){ resolve(mejor); return; }
          reject({codigo: err && err.code === 1 ? 'GPS_DENEGADO' : 'GPS_TIMEOUT', raw: err});
        },
        { enableHighAccuracy: true, timeout: 15000, maximumAge: 0 }
      );
    };
    intentar();
  });
}

function _levdGeoMensaje(codigo, extra){
  extra = extra || {};
  if (codigo === 'GPS_DENEGADO') return 'No pudimos leer tu ubicación. Activa el GPS y da permiso al navegador, luego reintenta.';
  if (codigo === 'GPS_NO_DISPONIBLE') return 'Este dispositivo no tiene ubicación disponible.';
  if (codigo === 'GPS_TIMEOUT') return 'No se pudo obtener tu ubicación a tiempo. Reintenta.';
  if (codigo === 'GPS_IMPRECISO') return `La señal GPS está muy imprecisa (±${extra.accuracy_m} m). Acércate a una ventana o sal un momento y reintenta.`;
  if (codigo === 'GPS_FUERA_RANGO') return `Estás a ~${extra.dist_m >= 1000 ? (extra.dist_m/1000).toFixed(1)+' km' : extra.dist_m+' m'} del sitio (límite ${extra.radio_m} m). Tu trabajo quedó guardado, no se pierde nada.`;
  if (codigo === 'GPS_REQUERIDO') return 'Necesitamos tu ubicación para cerrar este equipo. Actívala y vuelve a tocar «Guardar equipo».';
  return extra.error || 'No se pudo verificar tu ubicación.';
}

function levdAbrir(prefill, editId){
  if (!_levdModal) _levdModal = new bootstrap.Modal(_levdEl('modalLevDesc'));
  // Flush del equipo anterior ANTES de armar el nuevo contexto — si el
  // técnico toca "Agregar equipo" o edita otro justo después de escribir,
  // lo pendiente se guarda en vez de perderse.
  if (_levdCtx) _levdFlushPendientes(_levdCtx);
  _levdCtx = {
    clientUid: _levdNuevoUid(), iid: editId || null, creating: null,
    timers: new Map(), ultimoOk: {},
  };
  _levdAutosaveEstado('', '#9ca3af');
  _levdEditId = editId || null;
  _levdFotos = [];
  _levdFotosExistentes = (editId && prefill && prefill.fotos) ? prefill.fotos.map(f => ({id: f.id, url: f.url})) : [];
  _levdRenderFotos();
  _levdEl('levd_nombre').value    = (prefill && prefill.nombre) || '';
  _levdEl('levd_sku').value       = (prefill && prefill.sku) || '';
  _levdEl('levd_serie').value     = (editId && prefill && prefill.serie) ? prefill.serie : _levdSerieSugerida();
  _levdEl('levd_ubicacion').value = (prefill && prefill.ubicacion) || '';
  _levdEl('levd_obs').value       = (editId && prefill && prefill.observaciones) || '';
  const _danoInicial = !!(editId && prefill && (prefill.anomalias || '').trim());
  _levdEl('levd_operativa').checked = !(editId && prefill && prefill.estado_capturado === 'fuera_servicio');
  _levdEl('levd_dano').checked = _danoInicial;
  _levdEl('levd_dano_txt').value = _danoInicial ? prefill.anomalias : '';
  _levdEl('levdDanoWrap').style.display = _danoInicial ? '' : 'none';
  _levdEl('levdErr').textContent = '';
  // 2026-08-09: el toggle "no está" nace SIEMPRE apagado en un equipo nuevo;
  // en edición, refleja lo que ya se guardó.
  const _noEstaInicial = !!(editId && prefill && prefill.estado_capturado === 'no_encontrado');
  _levdEl('levd_no_esta').checked = _noEstaInicial;
  levdToggleNoEsta(_noEstaInicial);

  // 2026-07-06 (Daniel): título/ícono/botón cambian según modo crear/editar.
  const _tit = _levdEl('levdModalTitulo'), _ico = _levdEl('levdModalIco');
  const _seqBadge = _levdEl('levdModalSeq'), _lbl = _levdEl('levdGuardarLbl');
  if (editId){
    if (_tit) _tit.textContent = 'Editar equipo';
    if (_ico) _ico.className = 'bi bi-pencil-fill me-2';
    if (_seqBadge) _seqBadge.style.display = 'none';
    if (_lbl) _lbl.textContent = 'Guardar cambios';
  } else {
    if (_tit) _tit.textContent = 'Nuevo equipo';
    if (_ico) _ico.className = 'bi bi-camera-fill me-2';
    if (_seqBadge){ _seqBadge.style.display = ''; _seqBadge.textContent = '#' + _levdSeq; }
    if (_lbl) _lbl.textContent = 'Guardar equipo';
  }

  // Factura de origen: en edición se precarga desde doc_origen ("FCV 12345");
  // al crear, siempre parte limpia y colapsada (no se duplica).
  _levdDoc = null;
  _levdEl('levd_doc_num').value = '';
  _levdEl('levdDocInfo').innerHTML = '';
  _levdEl('levdDocWrap').style.display = 'none';
  if (editId && prefill && prefill.doc_origen){
    const m = String(prefill.doc_origen).trim().match(/^([A-Za-z]+)\s+(\d+)$/);
    if (m){
      _levdEl('levd_doc_tipo').value = m[1].toUpperCase();
      _levdEl('levd_doc_num').value = m[2];
      _levdDoc = { tido: m[1].toUpperCase(), numero: m[2], fecha: '' };
      _levdEl('levdDocInfo').innerHTML = `<span style="color:#15803d"><i class="bi bi-check-circle-fill me-1"></i>${prefill.doc_origen}</span>`;
      _levdEl('levdDocWrap').style.display = '';
    }
  }

  _levdModal.show();
  if (typeof levdRefreshStepStates === 'function') levdRefreshStepStates();
  // Nunca abrir la cámara automáticamente al editar — solo al crear "de cero".
  // 2026-08-08: dispara el input sin capture (mismo que el botón único de fotos)
  // para que el selector nativo ofrezca cámara + galería juntos.
  if (!prefill && !editId) setTimeout(() => { try { _levdEl('levdFotoInputGaleria').click(); } catch(_e){} }, 450);
}

function levdDuplicar(){
  if (!_levdUltimo) return;
  // 2026-07-08 (Daniel — OT-2026-00031, "el serial siempre debe ser único"):
  // "Duplicar" precarga nombre/sku/ubicación del último equipo capturado
  // para agilizar una 2da unidad IDÉNTICA, pero la serie NUNCA debe
  // copiarse — cada máquina física tiene su propio N° de serie. Como acá
  // no se pasa editId, levdAbrir() ya cae en la rama de "nuevo equipo" y
  // recalcula _levdSerieSugerida() por su cuenta — pero la forzamos
  // explícitamente para blindar este flujo aunque cambie esa lógica
  // compartida con levdEditar() en el futuro.
  levdAbrir({ nombre: _levdUltimo.nombre, sku: _levdUltimo.sku, ubicacion: _levdUltimo.ubicacion });
  const _serieEl = _levdEl('levd_serie');
  if (_serieEl) _serieEl.value = _levdSerieSugerida();
}

// 2026-07-06 (Daniel — "somos seres humanos y nos podemos equivocar"): editar
// un equipo ya descubierto, mientras la OT siga editable (ver LEV_EDITABLE).
// Reusa el MISMO modal de captura, precargado con los datos reales del item.
function levdEditar(iid){
  const it = _levdItems.find(x => String(x.id) === String(iid));
  if (!it) return;
  levdAbrir({
    nombre: it.nombre_snap || '',
    sku: it.sku_snap || '',
    serie: it.serie_snap || '',
    ubicacion: it.ubicacion || '',
    observaciones: it.observaciones || '',
    anomalias: it.anomalias || '',
    estado_capturado: it.estado_capturado || 'operativo',
    doc_origen: it.doc_origen || '',
    fotos: it.fotos || [],
  }, iid);
}

// ── Factura de origen ERP (opcional): verificación read-only ──
async function levdDocVerificar(){
  const info = _levdEl('levdDocInfo');
  const num = (_levdEl('levd_doc_num').value || '').replace(/\D/g, '');
  if (!num){ info.innerHTML = '<span class="text-danger">Escribe el número del documento.</span>'; return; }
  const btn = _levdEl('levdDocBtn');
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner-border spinner-border-sm"></span>';
  _levdDoc = null;
  try {
    const r = await fetch('/mantenciones/api/erp/doc-info', {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ tipo: _levdEl('levd_doc_tipo').value, numero: num,
                             lid: VISITA_LEVANTAMIENTO_ID }),
    });
    const d = await r.json().catch(() => ({}));
    if (!r.ok || !d.ok){
      info.innerHTML = '<span class="text-danger"><i class="bi bi-x-circle me-1"></i>' +
        (d.error || 'No se pudo verificar.') + '</span>';
      return;
    }
    const doc = d.doc || {};
    _levdDoc = { tido: doc.tipo, numero: doc.numero, fecha: doc.fecha || '' };
    const rutOk = !d.analisis || d.analisis.match;
    info.innerHTML =
      '<span style="color:#15803d"><i class="bi bi-check-circle-fill me-1"></i>' +
      `${doc.tipo} ${doc.numero}${doc.fecha ? ' · ' + doc.fecha : ''}` +
      (doc.monto ? ' · ' + _factMonto(doc.monto) : '') + '</span>' +
      `<div class="text-muted">${doc.cliente_nombre || ''}</div>` +
      (rutOk ? '' :
        `<div style="color:#b45309"><i class="bi bi-exclamation-triangle-fill me-1"></i>${(d.analisis && d.analisis.detalle) || 'El RUT no coincide con el cliente.'} Se guarda igual (informativo).</div>`);
  } catch(_e){
    info.innerHTML = '<span class="text-danger">Error de conexión con el servidor.</span>';
  } finally {
    btn.disabled = false;
    btn.innerHTML = '<i class="bi bi-search"></i>';
    if (typeof levdRefreshStepStates === 'function') levdRefreshStepStates();
  }
}

// ── Fotos: compresión en el navegador (4G friendly) ──
function _levdComprimir(file){
  return new Promise((resolve) => {
    try {
      const img = new Image();
      const url = URL.createObjectURL(file);
      img.onload = () => {
        try {
          const MAX = 1600;
          let w = img.width, h = img.height;
          if (w > MAX || h > MAX){
            const k = Math.min(MAX / w, MAX / h);
            w = Math.round(w * k); h = Math.round(h * k);
          }
          const cv = document.createElement('canvas');
          cv.width = w; cv.height = h;
          cv.getContext('2d').drawImage(img, 0, 0, w, h);
          cv.toBlob(b => { URL.revokeObjectURL(url); resolve(b || file); }, 'image/jpeg', 0.82);
        } catch(_e){ URL.revokeObjectURL(url); resolve(file); }
      };
      img.onerror = () => { URL.revokeObjectURL(url); resolve(file); };
      img.src = url;
    } catch(_e){ resolve(file); }
  });
}

function _levdRenderFotos(){
  const wrap = _levdEl('levdFotos');
  // 2026-08-08 (Daniel: "el botón de tomar foto y el de elegir de la galería
  // debería estar resumido y no en dos objetos diferentes"): un solo tile.
  // Dispara el input SIN capture (levdFotoInputGaleria) -- sin el atributo
  // capture, el selector nativo del teléfono ya ofrece "Cámara" y
  // "Galería" juntos, así que un solo botón cubre ambos casos sin
  // reintroducir el bug de Heiser (capture=environment bloqueaba la
  // galería en su Android). El input con capture (levdFotoInput) se deja
  // en el DOM por si se necesita en el futuro, pero ya no tiene botón propio.
  const addBtn = '<button type="button" class="levd-foto-add" onclick="document.getElementById(\'levdFotoInputGaleria\').click()" title="Agregar foto"><i class="bi bi-camera-fill"></i></button>';
  // 2026-07-06: en modo edición, primero las fotos YA subidas (borrado real
  // vía DELETE al servidor), luego las nuevas pendientes (solo en memoria).
  const existentesHtml = _levdFotosExistentes.map((f, i) =>
    `<div class="levd-foto-th"><img src="${f.url}" alt="">` +
    `<button type="button" class="x" onclick="levdFotoEliminarExistente(${i})">✕</button></div>`
  ).join('');
  // 2026-08-08 (Daniel, autoguardado): las fotos suben de inmediato al
  // elegirlas (ver _levdFotoInputChange) -- 3 estados visuales: subiendo
  // (spinner), ok (miniatura normal), error (badge rojo, toca para
  // reintentar sin volver a tomar la foto).
  const nuevasHtml = _levdFotos.map((f, i) => {
    const overlay = f.estado === 'subiendo'
      ? '<div class="levd-foto-ov"><span class="spinner-border spinner-border-sm text-light"></span></div>'
      : f.estado === 'error'
      ? `<div class="levd-foto-ov levd-foto-ov-err" onclick="levdFotoReintentar(${i})" title="No se subió — toca para reintentar"><i class="bi bi-arrow-repeat"></i></div>`
      : '';
    return `<div class="levd-foto-th"><img src="${f.url}" alt="">${overlay}` +
      `<button type="button" class="x" onclick="levdQuitarFoto(${i})">✕</button></div>`;
  }).join('');
  wrap.innerHTML = existentesHtml + nuevasHtml + addBtn;
  if (typeof levdRefreshStepStates === 'function') levdRefreshStepStates();
}

async function levdFotoEliminarExistente(i){
  const f = _levdFotosExistentes[i];
  if (!f) return;
  const ok = await ilusConfirm({
    title: 'Eliminar foto',
    message: '¿Quitar esta foto del equipo?',
    okLabel: 'Eliminar', cancelLabel: 'Cancelar', danger: true,
  });
  if (!ok) return;
  try {
    const r = await fetch(`/mantenciones/api/levantamiento-fotos/${f.id}`, { method: 'DELETE' });
    const d = await r.json().catch(() => ({}));
    if (r.ok && d.ok){
      _levdFotosExistentes.splice(i, 1);
      _levdRenderFotos();
    } else if (typeof ilusToast === 'function'){
      ilusToast(d.error || 'No se pudo eliminar la foto', { type: 'error' });
    }
  } catch(_e){
    if (typeof ilusToast === 'function') ilusToast('Error de red', { type: 'error' });
  }
}

function levdQuitarFoto(i){
  const f = _levdFotos[i];
  if (!f) return;
  try { URL.revokeObjectURL(f.url); } catch(_e){}
  // Si ya se había subido al servidor, borrarla de verdad (mismo endpoint
  // que las fotos "existentes" de edición) -- si no, solo era un blob local.
  if (f.id){
    fetch(`/mantenciones/api/levantamiento-fotos/${f.id}`, { method: 'DELETE' }).catch(() => {});
  }
  _levdFotos.splice(i, 1);
  _levdRenderFotos();
}

function levdFotoReintentar(i){
  const f = _levdFotos[i];
  if (!f || !_levdCtx) return;
  f.estado = 'subiendo';
  _levdRenderFotos();
  _levdSubirFoto(_levdCtx, f);
}

// 2026-08-08 (Daniel: soporta "foto como primera acción, antes del nombre").
// Sube de inmediato -- antes solo se encolaba en memoria y el único POST
// ocurría al final de levdGuardar, cuando el iid ya existía sí o sí. Ahora
// una foto puede ser lo PRIMERO que crea el equipo, así que espera el
// mismo mutex de creación que los campos de texto.
async function _levdSubirFoto(ctx, ph){
  try {
    const iid = await _levdEnsureIid(ctx);
    if (ctx !== _levdCtx) return;
    const fd = new FormData();
    fd.append('foto', ph.blob, `equipo_${iid}_${Date.now()}.jpg`);
    const esPrimera = _levdFotosExistentes.length === 0 && _levdFotos.indexOf(ph) === 0;
    fd.append('tipo_foto', esPrimera ? 'general' : 'detalle');
    const rf = await fetch(`/mantenciones/api/levantamiento-items/${iid}/fotos`, { method: 'POST', body: fd });
    const df = await rf.json().catch(() => ({}));
    if (rf.ok && df.ok){ ph.estado = 'ok'; ph.id = df.id; }
    else { ph.estado = 'error'; }
  } catch(_e){
    ph.estado = 'error';
  }
  if (ctx === _levdCtx) _levdRenderFotos();
}

async function _levdFotoInputChange(e){
  const files = Array.from(e.target.files || []);
  e.target.value = '';
  const ctx = _levdCtx;
  if (!ctx) return;
  for (const f of files){
    const blob = await _levdComprimir(f);
    const ph = { blob, url: URL.createObjectURL(blob), estado: 'subiendo', id: null };
    _levdFotos.push(ph);
    _levdRenderFotos();
    await _levdSubirFoto(ctx, ph);
  }
}
document.addEventListener('DOMContentLoaded', () => {
  // 2026-08-08: dos inputs comparten el mismo handler -- cámara (capture)
  // y galería (sin capture, `multiple` para elegir varias de una).
  const inp = _levdEl('levdFotoInput');
  if (inp) inp.addEventListener('change', _levdFotoInputChange);
  const inpGal = _levdEl('levdFotoInputGaleria');
  if (inpGal) inpGal.addEventListener('change', _levdFotoInputChange);
  // Pasos verde/rojo: se recalculan al tipear/tocar los campos que deciden
  // cada paso. El resto (fotos, doc verificado) ya llama a
  // levdRefreshStepStates() desde sus propios handlers.
  ['levd_nombre', 'levd_dano_txt'].forEach(id => {
    const el = _levdEl(id);
    if (el) el.addEventListener('input', levdRefreshStepStates);
  });
  const elOperativa = _levdEl('levd_operativa');
  if (elOperativa) elOperativa.addEventListener('change', levdRefreshStepStates);

  // Autoguardado (2026-08-08): un listener por campo -> _levdMarcarSucio
  // arma/reinicia su propio debounce de 800ms. GPS/ubicación NUNCA entra
  // acá — solo texto, switches y la factura.
  [['levd_nombre','nombre'], ['levd_sku','sku'], ['levd_serie','serie'],
   ['levd_ubicacion','ubicacion'], ['levd_obs','observaciones'],
   ['levd_dano_txt','anomalias'], ['levd_doc_num','doc_numero']].forEach(([id, campo]) => {
    const el = _levdEl(id);
    if (el) el.addEventListener('input', () => _levdMarcarSucio(campo));
  });
  if (elOperativa) elOperativa.addEventListener('change', () => _levdMarcarSucio('estado_capturado'));
  const elDano = _levdEl('levd_dano');
  if (elDano) elDano.addEventListener('change', () => _levdMarcarSucio('estado_capturado'));

  // Flush al cerrar el modal (equis, "Listo", click fuera) — nunca se
  // pierde la última letra escrita por cerrar antes de que venza el debounce.
  const modalEl = _levdEl('modalLevDesc');
  if (modalEl) modalEl.addEventListener('hide.bs.modal', () => { if (_levdCtx) _levdFlushPendientes(_levdCtx); });
  // Igual al ocultar la pestaña en el celular (bloqueo de pantalla, cambio
  // de app) -- beforeunload no es confiable en mobile.
  document.addEventListener('visibilitychange', () => {
    if (document.visibilityState === 'hidden' && _levdCtx) _levdFlushPendientes(_levdCtx);
  });

  levdInit();
});

// ── Pasos numerados verde/rojo (2026-08-08, Daniel: "atrevámonos a hacer un
// cambio... que se vea más potente" — mismo lenguaje visual que .rb-step en
// Repuestos). Cada regla espeja EXACTAMENTE la validación real de
// levdGuardar() de abajo: un paso queda verde solo cuando levdGuardar() no
// lo bloquearía. Los pasos opcionales (3/4/5) parten verdes y solo se ponen
// rojos si el usuario los deja en un estado inválido a medio llenar.
const LEVD_STEP_RULES = {
  // 2026-08-08: solo cuentan fotos YA subidas (estado 'ok') -- una foto
  // recién elegida pero aún "subiendo" (o que falló) no debe pintar el
  // paso en verde, porque levdGuardar() tampoco la aceptaría como válida.
  1: () => (_levdFotosExistentes.length + _levdFotos.filter(f => f.estado === 'ok').length) > 0,
  2: () => !!(_levdEl('levd_nombre') && _levdEl('levd_nombre').value.trim()),
  3: () => {
    const dano = _levdEl('levd_dano') && _levdEl('levd_dano').checked;
    const danoTxt = _levdEl('levd_dano_txt') && _levdEl('levd_dano_txt').value.trim();
    return !(dano && !danoTxt);
  },
  4: () => {
    const num = (_levdEl('levd_doc_num') && _levdEl('levd_doc_num').value || '').replace(/\D/g, '');
    if (!num) return true;
    return !!(_levdDoc && _levdDoc.numero === num);
  },
  5: () => true,
};

function levdRefreshStepStates(){
  // "El equipo no está" anula CUALQUIER obligatoriedad (Daniel, textual) --
  // todos los pasos se pintan verdes sin evaluar sus reglas individuales.
  const noEsta = _levdNoEsta();
  for (const n of Object.keys(LEVD_STEP_RULES)){
    const sec = document.getElementById('levdStep' + n);
    if (!sec) continue;
    let ok = noEsta;
    if (!ok){ try { ok = !!LEVD_STEP_RULES[n](); } catch(_e){ ok = false; } }
    sec.classList.toggle('is-complete', ok);
  }
}

// 2026-08-08 (Daniel: "el único dato que no se va a guardar si no tiene la
// ubicación... para poder guardar la orden de trabajo necesito tomar los
// datos de GPS"). Todo lo demás YA está guardado por el autoguardado antes
// de llegar acá — este botón valida lo que falta, pide GPS UNA vez, y
// finaliza el equipo (es_borrador -> 0) mandando finalizar:true.
async function levdGuardar(){
  const err = _levdEl('levdErr');
  err.textContent = '';
  const editando = !!_levdEditId;
  const ctx = _levdCtx;
  if (!ctx){ err.textContent = 'Error interno — cierra y vuelve a abrir el equipo.'; return; }
  const noEsta = _levdNoEsta();
  const nombre = _levdEl('levd_nombre').value.trim();
  const dano = _levdEl('levd_dano').checked;
  // 2026-08-09 (Daniel, textual): "el equipo no está... ahí ya se anula
  // cualquier obligatoriedad de completar, porque no existe. Solamente se
  // declara que no está". CERO validaciones de nombre/foto/daño/factura
  // cuando el toggle está activo.
  if (!noEsta){
    if (!nombre){ err.textContent = 'El nombre del equipo es obligatorio.'; _levdEl('levd_nombre').focus(); return; }
    const fotosOk = _levdFotosExistentes.length + _levdFotos.filter(f => f.estado === 'ok').length;
    if (fotosOk === 0){
      const hayPendientes = _levdFotos.some(f => f.estado === 'subiendo');
      err.textContent = hayPendientes ? 'Espera a que termine de subir la foto…' : 'Toma al menos 1 foto del equipo.';
      return;
    }
    const fotosMalas = _levdFotos.filter(f => f.estado === 'error');
    if (fotosMalas.length){
      err.textContent = `${fotosMalas.length} foto(s) no se subieron. Tócalas para reintentar antes de guardar.`;
      return;
    }
    const danoTxt = _levdEl('levd_dano_txt').value.trim();
    if (dano && !danoTxt){ err.textContent = 'Describe brevemente el daño.'; _levdEl('levd_dano_txt').focus(); return; }

    const _docNum = (_levdEl('levd_doc_num').value || '').replace(/\D/g, '');
    if (_docNum && (!_levdDoc || _levdDoc.numero !== _docNum)){
      _levdEl('levdDocWrap').style.display = '';
      err.textContent = 'Verifica la factura con la lupa (o borra el número) antes de guardar.';
      return;
    }
  }
  const nombreLbl = nombre || '(equipo no encontrado)';

  const btn = _levdEl('levdGuardarBtn');
  const orig = btn.innerHTML;
  btn.disabled = true;
  try {
    // 1) Asegura que TODO lo tipeado quedó guardado (por si el técnico
    //    escribió y tocó el botón antes de que venciera el debounce).
    btn.innerHTML = `<span class="spinner-border spinner-border-sm me-1"></span>Guardando…`;
    await _levdFlushPendientes(ctx);
    if (ctx !== _levdCtx){ return; }  // el modal cambió mientras esperábamos

    // 2) Garantiza que existe el equipo (caso raro: click inmediato sin que
    //    ningún campo haya disparado su propio autoguardado todavía).
    let iid;
    try { iid = await _levdEnsureIid(ctx); }
    catch(_e){ err.textContent = 'No se pudo guardar el equipo. Revisa tu señal y reintenta.'; return; }

    // 3) GPS — SOLO acá, y SOLO si la regla está activa (por defecto está
    //    apagada). Nunca antes de este punto.
    const payload = { finalizar: true };
    if (REGLAS_TERRENO.geofence_lev_activo && !GEOF_ROL_EXENTO){
      btn.innerHTML = `<span class="spinner-border spinner-border-sm me-1"></span>Verificando ubicación…`;
      try {
        const g = await _levdGeoObtener();
        payload.gps_lat = g.lat; payload.gps_lng = g.lng; payload.gps_accuracy_m = Math.round(g.acc);
      } catch(e){
        err.textContent = _levdGeoMensaje(e.codigo, e);
        return;
      }
    }

    // 4) Finalizar en el servidor (revalida TODO de nuevo: nombre, fotos,
    //    y si corresponde, la geocerca contra la distancia real).
    btn.innerHTML = `<span class="spinner-border spinner-border-sm me-1"></span>${editando ? 'Guardando cambios…' : 'Guardando…'}`;
    const r = await fetch(`/mantenciones/api/levantamiento-items/${iid}`, {
      method: 'PATCH', headers: {'Content-Type':'application/json'}, body: JSON.stringify(payload),
    });
    const d = await r.json().catch(() => ({}));
    if (!r.ok || !d.ok){
      err.textContent = d.error_codigo === 'GPS_FUERA_RANGO' || d.error_codigo === 'GPS_IMPRECISO'
        ? _levdGeoMensaje(d.error_codigo, d)
        : (d.error || (d.error_codigo === 'LEV_CONGELADO'
            ? 'La OT ya fue firmada — no se puede editar.' : 'No se pudo guardar el equipo.'));
      return;
    }

    const operativa = _levdEl('levd_operativa').checked;
    const estado_capturado = noEsta ? 'no_encontrado' : (!operativa ? 'fuera_servicio' : (dano ? 'advertencia' : 'operativo'));
    const estLbl = estado_capturado === 'no_encontrado'  ? ' · 🚫 NO ENCONTRADO'
                 : estado_capturado === 'fuera_servicio' ? ' · ⚠️ FUERA DE SERVICIO'
                 : estado_capturado === 'advertencia'    ? ' · ⚠️ con DAÑO reportado'
                 : '';
    const fotosOk = _levdFotosExistentes.length + _levdFotos.filter(f => f.estado === 'ok').length;
    if (editando){
      if (typeof ilusToast === 'function'){
        ilusToast(`✓ ${nombreLbl} actualizado${estLbl}`, { type: estLbl ? 'warning' : 'success' });
      }
      _levdEditId = null;
      await levdInit();          // refresca lista + contador
      _levdModal.hide();         // en edición NO se encadena a un nuevo equipo
      return;
    }

    _levdUltimo = { nombre, sku: _levdEl('levd_sku').value.trim(), ubicacion: _levdEl('levd_ubicacion').value.trim() };
    _levdSeq++;
    if (typeof ilusToast === 'function'){
      // 2026-07-06 (Daniel): el toast ahora avisa el estado capturado — un
      // técnico reportó un equipo marcado "DAÑO" sin haber tocado el toggle
      // (no se encontró bug de código; lo más probable es un toque accidental
      // en terreno). Esta confirmación le da una oportunidad inmediata de
      // notarlo y corregirlo (editar o eliminar) antes de seguir.
      const _fotosMsg = noEsta ? '' : ` (${fotosOk} foto${fotosOk === 1 ? '' : 's'})`;
      ilusToast(`✓ ${nombreLbl} guardado${_fotosMsg}${estLbl}`,
                { type: estLbl ? 'warning' : 'success' });
    }
    await levdInit();          // refresca lista + contador
    _levdEl('levdDupBtn').style.display = '';
    // Encadenar: modal listo para el SIGUIENTE equipo
    levdAbrir();
  } catch(e){
    err.textContent = 'Error de conexión. Reintenta.';
  } finally {
    btn.disabled = false;
    btn.innerHTML = orig;
  }
}

async function levdEliminar(iid){
  const ok = await ilusConfirm({
    title: 'Eliminar equipo capturado',
    message: '¿Quitar este equipo del levantamiento?',
    sub: 'Se borran también sus fotos. Esta acción no se puede deshacer.',
    okLabel: 'Eliminar', cancelLabel: 'Cancelar', danger: true,
  });
  if (!ok) return;
  try {
    const r = await fetch(`/mantenciones/api/levantamiento-items/${iid}`, { method:'DELETE' });
    const d = await r.json().catch(() => ({}));
    if (d.ok){
      if (typeof ilusToast === 'function') ilusToast('Equipo eliminado', { type:'info' });
      await levdInit();
    } else if (typeof ilusToast === 'function'){
      ilusToast(d.error || 'No se pudo eliminar', { type:'error' });
    }
  } catch(_e){
    if (typeof ilusToast === 'function') ilusToast('Error de red', { type:'error' });
  }
}

// 2026-07-08 (Daniel — OT-2026-00031): reparación manual de equipos
// descubiertos que quedaron sin llegar a la ficha del cliente. Complementa
// el backfill automático de boot — sirve para no depender de un redeploy
// si aparece otro caso similar. Solo se ve el botón si el backend calculó
// lev_huerfanos_count > 0 y el usuario puede aprobar (mismo permiso que
// certifica el cierre de la OT).
async function levdRematerializar(lid){
  const ok = await ilusConfirm({
    title: 'Re-materializar equipos',
    message: 'Se intentará crear en la ficha del cliente los equipos descubiertos que aún no aparecen ahí.',
    sub: 'Es seguro repetir esta acción — los equipos ya materializados no se duplican.',
    okLabel: 'Re-materializar', cancelLabel: 'Cancelar',
  });
  if (!ok) return;
  const btn = document.getElementById('levdRematBtn');
  const orig = btn ? btn.innerHTML : '';
  if (btn){
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Materializando…';
  }
  try {
    const r = await fetch(`/mantenciones/api/levantamientos/${lid}/rematerializar`, { method:'POST' });
    const d = await r.json().catch(() => ({}));
    if (d.ok){
      const n = d.creados || 0;
      if (n > 0 && typeof ilusAlert === 'function'){
        await ilusAlert({
          title: 'Equipos materializados',
          message: `${n} equipo(s) fueron creados en la ficha del cliente.`,
          type: 'success',
        });
      } else if (typeof ilusToast === 'function'){
        ilusToast(d.mensaje || (n > 0 ? `${n} equipo(s) materializado(s)` : 'No había equipos pendientes'), { type: n > 0 ? 'success' : 'info' });
      }
      window.location.reload();
    } else {
      if (typeof ilusToast === 'function') ilusToast(d.error || 'No se pudo materializar', { type:'error' });
      if (btn){ btn.disabled = false; btn.innerHTML = orig; }
    }
  } catch(_e){
    if (typeof ilusToast === 'function') ilusToast('Error de red', { type:'error' });
    if (btn){ btn.disabled = false; btn.innerHTML = orig; }
  }
}

/* ---- (limite del <script> original en el template) ---- */

// ════════════════════════════════════════════════════════════════
// SELLO factura↔OT (Daniel 2026-06-23) — la OT cobrable no se firma
// sin factura asociada. Consulta al ERP (read-only) + análisis de RUT
// con dígito verificador + justificación obligatoria si no coincide.
// ════════════════════════════════════════════════════════════════
let _factPreviewOk = false;

function _factEl(id){ return document.getElementById(id); }

function _factMonto(m){
  if (m === null || m === undefined) return '—';
  try { return '$' + Math.round(m).toLocaleString('es-CL'); } catch(_e){ return '$' + m; }
}

async function factConsultar(){
  const err = _factEl('factErr'); err.textContent = '';
  const numero = (_factEl('factNumero').value || '').replace(/\D/g, '');
  if (!numero){ err.textContent = 'Escribe el número del documento.'; return; }
  const btn = _factEl('factBuscarBtn');
  const orig = btn.innerHTML;
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Consultando ERP…';
  _factPreviewOk = false;
  _factEl('factPreview').style.display = 'none';
  _factEl('factJustifWrap').style.display = 'none';
  _factEl('factAsociarBtn').style.display = 'none';
  try {
    const r = await fetch(`/mantenciones/api/visitas/${VID}/asociar-factura`, {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ tipo: _factEl('factTipo').value, numero, confirmar: false }),
    });
    const d = await r.json().catch(() => ({}));
    if (!r.ok || !d.ok){ err.textContent = d.error || 'No se pudo consultar.'; return; }
    const f = d.factura || {}, a = d.analisis || {};
    const okRut = !!(a.match);
    _factEl('factPreview').innerHTML =
      `<div class="p-2" style="background:#fff;border:1px solid #e5e7eb;border-radius:10px;font-size:.82rem">
        <div class="fw-bold">${f.tipo} N° ${f.numero} · ${_factMonto(f.monto)} ${f.fecha ? '· ' + f.fecha : ''}</div>
        <div>${(f.nombre || '—')} · RUT ${(f.rut || '—')}</div>
        <div class="mt-1 fw-semibold" style="color:${okRut ? '#15803d' : '#b45309'}">
          <i class="bi ${okRut ? 'bi-check-circle-fill' : 'bi-exclamation-triangle-fill'} me-1"></i>${a.detalle || ''}
        </div>
      </div>`;
    _factEl('factPreview').style.display = '';
    if (d.requiere_justificacion) _factEl('factJustifWrap').style.display = '';
    _factEl('factAsociarBtn').style.display = '';
    _factPreviewOk = true;
  } catch(_e){
    err.textContent = 'Error de conexión con el servidor.';
  } finally {
    btn.disabled = false;
    btn.innerHTML = orig;
  }
}

async function factAsociar(){
  const err = _factEl('factErr'); err.textContent = '';
  if (!_factPreviewOk){ err.textContent = 'Primero consulta el documento.'; return; }
  const numero = (_factEl('factNumero').value || '').replace(/\D/g, '');
  const justif = (_factEl('factJustif') ? _factEl('factJustif').value : '').trim();
  const btn = _factEl('factAsociarBtn');
  const orig = btn.innerHTML;
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Asociando…';
  try {
    const r = await fetch(`/mantenciones/api/visitas/${VID}/asociar-factura`, {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ tipo: _factEl('factTipo').value, numero,
                             confirmar: true, justificacion: justif }),
    });
    const d = await r.json().catch(() => ({}));
    if (!r.ok || !d.ok){
      err.textContent = d.error || 'No se pudo asociar.';
      if (d.error_codigo === 'RUT_NO_COINCIDE') _factEl('factJustifWrap').style.display = '';
      return;
    }
    if (typeof ilusToast === 'function'){
      ilusToast('✓ Factura asociada — ya puedes firmar el cierre', { type:'success' });
    }
    setTimeout(() => window.location.reload(), 900);
  } catch(_e){
    err.textContent = 'Error de conexión con el servidor.';
  } finally {
    btn.disabled = false;
    btn.innerHTML = orig;
  }
}

/* ---- (limite del <script> original en el template) ---- */

(function(){
  const R = REGLAS_TERRENO || {};
  const estadosTrabajo = ['creada','programada','asignada','en_curso','en_ejecucion','reagendada'];
  if (!R.geofence_activo) return;
  if (!GEOF_PUEDE_EJECUTAR) return;                    // solo quien ejecuta
  if (typeof VISITA_TIPO !== 'undefined' && !estadosTrabajo.includes(VISITA_ESTADO)) return;
  if (DESTINO_LAT === null || DESTINO_LNG === null) return;  // sin coords → no bloquea
  if (sessionStorage.getItem('geoOK_' + VID) === '1') return;

  const radio = Math.max(50, parseInt(R.geofence_radio_m || 200, 10));

  function _havM(lat1, lon1, lat2, lon2){
    const rad = Math.PI / 180, RT = 6371000;
    const dLat = (lat2 - lat1) * rad, dLon = (lon2 - lon1) * rad;
    const a = Math.sin(dLat/2)**2 +
      Math.cos(lat1*rad) * Math.cos(lat2*rad) * Math.sin(dLon/2)**2;
    return 2 * RT * Math.asin(Math.sqrt(a));
  }

  const ov = document.createElement('div');
  ov.id = 'geofOverlay';
  ov.style.cssText = 'position:fixed;inset:0;z-index:99990;background:rgba(10,10,10,.96);' +
    'display:flex;align-items:center;justify-content:center;padding:22px;';
  ov.innerHTML =
    '<div style="max-width:420px;width:100%;text-align:center;color:#fff">' +
      '<div style="font-size:2.6rem;margin-bottom:8px">📍</div>' +
      '<div style="font-weight:800;font-size:1.15rem">Check-in de ubicación</div>' +
      '<div style="color:#9ca3af;font-size:.88rem;margin:10px 0 4px;line-height:1.5">' +
        'Para gestionar esta OT debes estar a menos de <b style="color:#fff">' + radio + ' m</b> del cliente.<br>' +
        '<span style="font-size:.8rem">' + (DESTINO_DIR || '') + '</span></div>' +
      '<div id="geofMsg" style="min-height:40px;font-size:.85rem;color:#fca5a5;margin:8px 0"></div>' +
      '<button id="geofBtn" style="width:100%;background:#dc2626;color:#fff;border:0;border-radius:12px;' +
        'font-weight:800;font-size:1rem;padding:15px;min-height:52px;cursor:pointer">' +
        '<i class="bi bi-crosshair"></i> Verificar mi ubicación</button>' +
      '<div style="color:#6b7280;font-size:.72rem;margin-top:12px">Activado por ILUS en Configuración · ' +
        'Si crees que es un error, contacta a tu supervisor.</div>' +
    '</div>';
  document.body.appendChild(ov);
  document.body.style.overflow = 'hidden';

  document.getElementById('geofBtn').addEventListener('click', () => {
    const msg = document.getElementById('geofMsg');
    const btn = document.getElementById('geofBtn');
    if (!navigator.geolocation){ msg.textContent = 'Este dispositivo no tiene GPS disponible.'; return; }
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Obteniendo GPS…';
    navigator.geolocation.getCurrentPosition(pos => {
      const d = Math.round(_havM(pos.coords.latitude, pos.coords.longitude, DESTINO_LAT, DESTINO_LNG));
      if (d <= radio){
        sessionStorage.setItem('geoOK_' + VID, '1');
        msg.style.color = '#86efac';
        msg.textContent = '✓ Estás a ' + d + ' m — check-in correcto. ¡Buen trabajo!';
        setTimeout(() => { ov.remove(); document.body.style.overflow = ''; }, 900);
      } else {
        msg.style.color = '#fca5a5';
        msg.textContent = 'Estás a ~' + (d >= 1000 ? (d/1000).toFixed(1) + ' km' : d + ' m') +
          ' del cliente (límite ' + radio + ' m). Acércate al lugar y vuelve a verificar.';
        btn.disabled = false;
        btn.innerHTML = '<i class="bi bi-crosshair"></i> Reintentar';
      }
    }, err => {
      msg.textContent = 'No se pudo obtener tu ubicación (' + (err.message || 'GPS apagado') +
        '). Activa el GPS y otorga el permiso de ubicación.';
      btn.disabled = false;
      btn.innerHTML = '<i class="bi bi-crosshair"></i> Reintentar';
    }, { enableHighAccuracy: true, timeout: 15000, maximumAge: 30000 });
  });
})();

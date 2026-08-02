/* ==================================================================
   transporte_monitor.js - Monitor de Transporte (ILUS)
   Extraido TAL CUAL desde templates/transporte/index.html, donde vivia
   en 2 bloques <script> inline (~146 KB re-descargados en cada clic: el
   HTML se sirve con cache-control: no-store). Aqui se cachea por dias.

   Orden respetado: primero el script chico del hero (sincronizarHoy,
   limpiarMonitor, sincronizarMesActual, limpiarYResync - llamados desde
   onclick, por eso deben seguir siendo funciones globales), despues el
   script grande del final del body.

   Los DATOS de la pagina (manifiestos/estados/couriers) siguen inline en
   el template como window.MON - este archivo solo los lee.
   Se carga con <script defer>, que ejecuta despues del parseo del HTML,
   igual que un script inline al final del <body>.
   ================================================================== */

/* -- (1/2) venia del <script> de las lineas 225-395 del template -- */
// ════════════════════════════════════════════════════════════
//  SINCRONIZAR HOY — import rápido de docs emitidos en el día
// ════════════════════════════════════════════════════════════
async function sincronizarHoy(){
  const btn = document.getElementById('btnSyncHoy');
  const txt = btn.innerHTML;
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Sincronizando…';
  try {
    const r = await fetch('/transporte/api/sync-hoy', {method:'POST', headers:{'Content-Type':'application/json'}});
    const d = await r.json();
    if (!d.ok){
      if (typeof ilusToast === 'function') ilusToast('Error: ' + (d.error||'desconocido'), {type:'error'});
      else alert('Error: ' + (d.error||'desconocido'));
      return;
    }
    if (typeof ilusAlert === 'function') {
      await ilusAlert({
        title: 'Sincronización HOY',
        message: d.mensaje,
        sub: d.importados ? 'La lista se va a recargar para mostrar los nuevos documentos.' : '',
        type: d.importados ? 'success' : 'info',
      });
    } else {
      alert(d.mensaje);
    }
    // Cleanup defensivo de cualquier backdrop huérfano antes de reload
    if (typeof trLimpiarBackdrops === 'function') trLimpiarBackdrops();
    if (d.importados > 0) {
      setTimeout(() => location.reload(), 600);
    }
  } catch(e){
    if (typeof ilusToast === 'function') ilusToast('Error de red: '+e.message, {type:'error'});
    else alert('Error de red: '+e.message);
  } finally {
    btn.disabled = false;
    btn.innerHTML = txt;
    if (typeof trLimpiarBackdrops === 'function') trLimpiarBackdrops();
  }
}

// ════════════════════════════════════════════════════════════
//  LIMPIAR MONITOR — superadmin only. Borra TODOS los registros.
//  Útil para validar que el sync diario empieza limpio.
//
//  FIX 2026-07-31 (Fase 0, plan de mejora integral de Transporte):
//  antes llamaba a /transporte/api/limpiar-monitor (solo permiso
//  admin/superadmin, SIN texto de confirmación) -- ese endpoint legacy
//  se retiró de app.py. Ahora usa /limpiar-todo, que exige escribir
//  "LIMPIAR" exacto (vía ilusPrompt, no un prompt() nativo) además de
//  superadmin -- un solo click accidental ya no puede borrar todo el
//  monitor.
// ════════════════════════════════════════════════════════════
async function limpiarMonitor(){
  if (typeof ilusPrompt !== 'function') {
    ilusToast('No se pudo abrir el diálogo de confirmación.', {type:'error'});
    return;
  }
  const texto = await ilusPrompt({
    title: 'Limpiar TODO el monitor',
    message: 'Esto elimina TODOS los registros del monitor de transporte (no afecta al ERP; el próximo sync repuebla con los documentos del día).',
    sub: 'Escribe <strong>LIMPIAR</strong> para confirmar.',
    subHtml: true,
    placeholder: 'LIMPIAR',
    okLabel: 'Confirmar',
    cancelLabel: 'Cancelar',
    required: true,
  });
  if (!texto) return;
  if (texto.trim().toUpperCase() !== 'LIMPIAR') {
    ilusToast('Escribe exactamente "LIMPIAR" para confirmar. No se borró nada.', {type:'warning'});
    return;
  }
  const btn = document.getElementById('btnLimpiarMon');
  const txt = btn ? btn.innerHTML : '';
  if (btn){ btn.disabled = true; btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Limpiando…'; }
  try {
    const r = await fetch('/transporte/api/limpiar-todo', {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify({confirm_text: 'LIMPIAR'}),
    });
    const d = await r.json();
    if (!d.ok){
      if (typeof ilusToast === 'function') ilusToast('Error: ' + (d.error||'desconocido'), {type:'error'});
      else alert('Error: ' + (d.error||'desconocido'));
      return;
    }
    if (typeof ilusToast === 'function') ilusToast(d.mensaje || 'Monitor limpio', {type:'success'});
    else alert(d.mensaje || 'Monitor limpio');
    if (typeof trLimpiarBackdrops === 'function') trLimpiarBackdrops();
    setTimeout(() => location.reload(), 600);
  } catch(e){
    if (typeof ilusToast === 'function') ilusToast('Error de red: '+e.message, {type:'error'});
    else alert('Error de red: '+e.message);
  } finally {
    if (btn){ btn.disabled = false; btn.innerHTML = txt; }
    if (typeof trLimpiarBackdrops === 'function') trLimpiarBackdrops();
  }
}

// ════════════════════════════════════════════════════════════
//  SINCRONIZAR MES ACTUAL — trae todos los BLV+FCV con saldo
//  del primer día del mes hasta hoy. Idéntico a lo que corre el
//  cron 4 veces al día (09/11/15/17).
// ════════════════════════════════════════════════════════════
async function sincronizarMesActual(){
  const btn = document.getElementById('btnSyncMes');
  const txt = btn ? btn.innerHTML : '';
  if (btn){ btn.disabled = true; btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Sincronizando mes…'; }
  try {
    const r = await fetch('/transporte/api/sync/mes-actual', {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({ limpiar: false })
    });
    const d = await r.json();
    if (!d.ok){
      if (typeof ilusToast === 'function') ilusToast('Error: ' + (d.error||'desconocido'), {type:'error'});
      return;
    }
    if (typeof ilusAlert === 'function') {
      await ilusAlert({
        title: 'Sincronización del mes',
        message: d.mensaje || ('Documentos sincronizados: ' + (d.sincronizados||0)),
        sub: 'Rango: ' + (d.rango||'') + ((d.errores||[]).length ? ' · ' + d.errores.length + ' error(es) en el ERP' : ''),
        type: (d.sincronizados||0) > 0 ? 'success' : 'info',
      });
    }
    // Cleanup defensivo: si quedó algún backdrop huérfano del modal, lo
    // matamos antes del reload o de mostrar el listado refrescado.
    if (typeof trLimpiarBackdrops === 'function') trLimpiarBackdrops();
    if ((d.sincronizados||0) > 0 || (d.limpiados||0) > 0) {
      setTimeout(() => location.reload(), 500);
    }
  } catch(e){
    if (typeof ilusToast === 'function') ilusToast('Error de red: '+e.message, {type:'error'});
  } finally {
    if (btn){ btn.disabled = false; btn.innerHTML = txt; }
    if (typeof trLimpiarBackdrops === 'function') trLimpiarBackdrops();
  }
}

// ════════════════════════════════════════════════════════════
//  LIMPIAR PENDIENTES + RESYNC — solo admin
//  Borra pendientes del mes SIN manifiesto y vuelve a traer
//  desde ERP. No toca lo que ya está en manifiesto.
// ════════════════════════════════════════════════════════════
async function limpiarYResync(){
  const ok = (typeof ilusConfirm === 'function')
    ? await ilusConfirm({
        title: 'Limpiar pendientes del mes + resincronizar',
        message: '¿Borrar los documentos pendientes del mes corriente que aún NO están en un manifiesto y volver a traerlos desde el ERP?',
        sub: 'Los documentos ya asignados a un manifiesto se respetan. Es seguro repetir esta acción cuantas veces quieras.',
        okLabel: 'Sí, limpiar y resync',
        cancelLabel: 'Cancelar',
        type: 'danger',
      })
    : confirm('¿Limpiar pendientes del mes y resincronizar desde ERP?');
  if (!ok) return;
  const btn = document.getElementById('btnLimpiarResync');
  const txt = btn ? btn.innerHTML : '';
  if (btn){ btn.disabled = true; btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Limpiando + resync…'; }
  try {
    const r = await fetch('/transporte/api/sync/mes-actual', {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({ limpiar: true })
    });
    const d = await r.json();
    if (!d.ok){
      if (typeof ilusToast === 'function') ilusToast('Error: ' + (d.error||'desconocido'), {type:'error'});
      return;
    }
    if (typeof ilusAlert === 'function') {
      await ilusAlert({
        title: 'Limpieza + resync OK',
        message: 'Pendientes limpiados: ' + (d.limpiados||0) + ' · Sincronizados: ' + (d.sincronizados||0),
        sub: 'Rango: ' + (d.rango||''),
        type: 'success',
      });
    }
    if (typeof trLimpiarBackdrops === 'function') trLimpiarBackdrops();
    setTimeout(() => location.reload(), 500);
  } catch(e){
    if (typeof ilusToast === 'function') ilusToast('Error de red: '+e.message, {type:'error'});
  } finally {
    if (btn){ btn.disabled = false; btn.innerHTML = txt; }
    if (typeof trLimpiarBackdrops === 'function') trLimpiarBackdrops();
  }
}

/* -- (2/2) venia del <script> de las lineas 2502-5630 del template -- */
// ── Init ────────────────────────────────────────────────────────
var _vistaModal = null;
var _fotoModal  = null;
var _vistaCurrentSku = null;   // SKU activo para upload de fotos
var _vistaCurrentEmail   = null;  // correo del cliente del compromiso abierto (para reenvío manual)
var _vistaCurrentEstado  = '';    // estado_entrega actual del compromiso abierto
var _vistaCurrentCliente = '';    // nombre del cliente del compromiso abierto
var _trLastSync     = null;     // timestamp última sync para el header
var _trSyncTimer    = null;

document.addEventListener('DOMContentLoaded', function() {
  _vistaModal = new bootstrap.Modal(document.getElementById('vistaModal'));
  _fotoModal  = new bootstrap.Modal(document.getElementById('fotoModal'));
  setSync(30);   // Pre-fill rango sync: último mes
  cargarMonitor();
  // FIX 2026-08-01 (revisión adversarial): sin esta llamada, el banner de
  // alertas solo se pedía al backend al hacer clic en una pestaña de vista —
  // al abrir la página en frío no aparecía nunca, que es justo cuando más
  // importa ("cosas trabadas que nadie va a destrabar solo").
  renderAlertas();
  // Panel manifiesto: empieza CERRADO en escritorio y mobile.
  // Daniel pidió: solo aparece cuando el usuario lo activa (botón flotante)
  // o cuando arrastra un documento. Esto evita que estorbe el listado.
  var btn = document.getElementById('dpReopenBtn');
  if (btn) btn.style.display = 'inline-flex';
  // Iniciar refresh periódico del estado de sync (cada 30s)
  _trActualizarSyncStatus();
  if (_trSyncTimer) clearInterval(_trSyncTimer);
  _trSyncTimer = setInterval(_trActualizarSyncStatus, 30000);
});

// ── Helpers de UI nuevos ────────────────────────────────────────
// ── Número de documento SIN ceros a la izquierda ────────────────────────
// Daniel 2026-08-02: "quería también eliminar lo que son todos los ceros a
// la izquierda de la factura, boleta, guía, notas de venta, todo".
// "0000011149" -> "11149". Es SOLO presentación: el valor real se sigue
// guardando y consultando con el padding del ERP (es parte de la clave
// única tido+nudo, ver nudo_canonico en _tr_fetch_from_erp) -- por eso
// nunca hay que usar esto para construir búsquedas ni URLs.
// Las notas de venta son el caso especial que menciona Daniel: su número
// trae prefijo de letras ("VD00010223", "WEB-123"). El regex conserva
// cualquier prefijo no numérico y solo recorta los ceros del bloque de
// dígitos, así que "VD00010223" -> "VD10223" y no se rompe nada.
function trNudo(n) {
  var s = String(n == null ? '' : n).trim();
  if (!s) return '';
  return s.replace(/^([^0-9]*)0+(\d)/, '$1$2');
}

// Formatea una fecha del backend (ISO o dd/mm/yyyy) a "30 abr 2026"
// con relación humana ("hoy", "ayer", "hace 3d") como subtexto.
function trFormatFecha(s) {
  if (!s) return '<span style="color:var(--tr-text-soft)">—</span>';
  var meses = ['ene','feb','mar','abr','may','jun','jul','ago','sep','oct','nov','dic'];
  var d = null;
  try {
    // Soporta dd/mm/yyyy, yyyy-mm-dd e ISO
    if (/^\d{2}\/\d{2}\/\d{4}/.test(s)) {
      var parts = s.split(/[\/ ]/);
      d = new Date(parseInt(parts[2]), parseInt(parts[1])-1, parseInt(parts[0]));
    } else {
      d = new Date(s);
    }
    if (!d || isNaN(d.getTime())) return '<span>' + s + '</span>';
  } catch(e) { return '<span>' + s + '</span>'; }

  var hoy = new Date(); hoy.setHours(0,0,0,0);
  var solo = new Date(d.getFullYear(), d.getMonth(), d.getDate());
  var diff = Math.round((hoy - solo) / 86400000);
  var rel = '';
  if (diff === 0) rel = 'hoy';
  else if (diff === 1) rel = 'ayer';
  else if (diff > 1 && diff < 7) rel = 'hace ' + diff + 'd';
  else if (diff >= 7 && diff < 30) rel = 'hace ' + Math.floor(diff/7) + 'sem';
  else if (diff >= 30) rel = 'hace ' + Math.floor(diff/30) + 'm';

  var fechaTxt = d.getDate() + ' ' + meses[d.getMonth()] + ' ' + d.getFullYear();
  return '<div class="tr-fecha"><span class="tr-fecha-day">' + fechaTxt + '</span>' +
         (rel ? '<span class="tr-fecha-rel">' + rel + '</span>' : '') + '</div>';
}

// Cambia la vista del monitor desde los KPI cards (clic directo).
function setVistaMonitorById(vista) {
  var btn = document.querySelector('.tr-segment-btn[data-vista="' + vista + '"]');
  if (btn) setVistaMonitor(btn, vista);
}

// Actualiza el indicador de estado de sync en el header
function _trActualizarSyncStatus() {
  var el = document.getElementById('trSyncStatus');
  if (!el) return;
  if (!_trLastSync) {
    el.textContent = 'sincronizado al cargar';
    el.classList.remove('tr-sync-stale');
    return;
  }
  var diff = Math.round((Date.now() - _trLastSync) / 60000);
  if (diff < 1) { el.textContent = 'sincronizado ahora'; el.classList.remove('tr-sync-stale'); }
  else if (diff < 60) {
    el.textContent = 'hace ' + diff + ' min';
    el.classList.toggle('tr-sync-stale', diff > 30);
  } else {
    el.textContent = 'hace ' + Math.floor(diff/60) + ' h';
    el.classList.add('tr-sync-stale');
  }
}

// ════════════════════════════════════════════════════════════
//  WIDGET de auto-sync (header) — pinta último/próximo + estado SQL
// ════════════════════════════════════════════════════════════
var _trSyncWidgetTimer = null;
async function trCargarSyncStatus() {
  var dot   = document.getElementById('trSyncSqlDot');
  var ulVal = document.getElementById('trSyncWidgetUltimo');
  var pxVal = document.getElementById('trSyncWidgetProximo');
  try {
    var r = await fetch('/transporte/api/sync-status', {cache:'no-store'});
    if (!r.ok) throw new Error('HTTP ' + r.status);
    var d = await r.json();
    if (!d.ok) throw new Error('respuesta no-ok');

    // Estado del SQL Server (pulse-dot)
    if (dot) {
      dot.setAttribute('data-status', d.sql_status || 'down');
      var tooltipMap = {
        ok: 'ERP Random SQL Server: OK',
        degradado: 'ERP Random SQL Server: degradado (timeouts)',
        down: 'ERP Random SQL Server: no responde',
      };
      dot.setAttribute('title', tooltipMap[d.sql_status] || 'ERP: estado desconocido');
    }

    // Texto último/próximo
    if (ulVal) ulVal.textContent = d.ultimo_sync_relativo || '—';
    if (pxVal) {
      var hhmm = d.proximo_sync_hhmm || '';
      var rel  = d.proximo_sync_relativo || '';
      pxVal.textContent = hhmm + (rel ? ' (' + rel + ')' : '');
    }
  } catch(e) {
    if (dot) {
      dot.setAttribute('data-status', 'down');
      dot.setAttribute('title', 'ERP: no se pudo consultar el estado');
    }
    if (ulVal) ulVal.textContent = '—';
    if (pxVal) pxVal.textContent = '—';
  }
}

// Dispara sync ahora desde el widget. Reusa el flujo de "sincronizar mes".
async function trSyncWidgetSyncAhora() {
  var btn = document.getElementById('trSyncWidgetBtn');
  if (btn) {
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner-border spinner-border-sm" style="width:.85rem;height:.85rem"></span>';
  }
  try {
    var r = await fetch('/transporte/api/sync/mes-actual', {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({ limpiar: false })
    });
    var d = await r.json();
    if (d.ok) {
      if (typeof ilusToast === 'function') {
        ilusToast('Sync OK: ' + (d.mensaje || 'documentos actualizados'), {type:'success'});
      }
      // Recargar status + listado
      trCargarSyncStatus();
      if (typeof cargarMonitor === 'function') cargarMonitor();
    } else {
      if (typeof ilusToast === 'function') {
        ilusToast('Error de sync: ' + (d.error || 'desconocido'), {type:'error'});
      }
    }
  } catch(e) {
    if (typeof ilusToast === 'function') ilusToast('Error de red: '+e.message, {type:'error'});
  } finally {
    if (btn) {
      btn.disabled = false;
      btn.innerHTML = '<i class="bi bi-arrow-clockwise"></i><span>Sincronizar ahora</span>';
    }
  }
}

// Iniciar el widget cuando el DOM esté listo. Refresh cada 30s.
function _trIniciarSyncWidget() {
  if (!document.getElementById('trSyncWidget')) return;
  trCargarSyncStatus();
  if (_trSyncWidgetTimer) clearInterval(_trSyncWidgetTimer);
  _trSyncWidgetTimer = setInterval(trCargarSyncStatus, 30000);
  // Refresh al volver a la tab
  document.addEventListener('visibilitychange', function() {
    if (!document.hidden) trCargarSyncStatus();
  });
}
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', _trIniciarSyncWidget);
} else {
  _trIniciarSyncWidget();
}

// ════════════════════════════════════════════════════════════
//  IMPORTAR FORZADO (superadmin) — sin recargar página
// ════════════════════════════════════════════════════════════
async function trImportarForzado() {
  var tido = (document.getElementById('fiTido')?.value || '').trim().toUpperCase();
  var nudo = (document.getElementById('fiNudo')?.value || '').trim();
  var btn  = document.getElementById('btnImportarForzado');
  if (!nudo) {
    if (typeof ilusToast === 'function') ilusToast('Ingresa el N° de documento', {type:'warning'});
    return;
  }
  var orig = btn ? btn.innerHTML : '';
  if (btn) {
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span><span>Importando…</span>';
  }
  try {
    var r = await fetch('/transporte/api/importar-forzado', {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({ tido: tido, nudo: nudo })
    });
    var d = await r.json();
    if (!d.ok) {
      if (typeof ilusToast === 'function') ilusToast(d.error || 'No se pudo importar', {type:'error'});
      else alert(d.error || 'Error');
      return;
    }
    if (typeof ilusToast === 'function') {
      ilusToast(d.mensaje || (tido + ' ' + nudo + ' importado'), {type:'success'});
    }
    // Limpiar input para siguiente import
    var nudoEl = document.getElementById('fiNudo');
    if (nudoEl) { nudoEl.value = ''; nudoEl.focus(); }
    // Recargar el monitor sin refresh completo
    if (typeof cargarMonitor === 'function') cargarMonitor();
    trCargarSyncStatus();
  } catch(e) {
    if (typeof ilusToast === 'function') ilusToast('Error de red: '+e.message, {type:'error'});
  } finally {
    if (btn) { btn.disabled = false; btn.innerHTML = orig; }
  }
}

// ════════════════════════════════════════════════════════════
//  LIMPIAR MONITOR (superadmin) — versión con confirm_text=LIMPIAR
// ════════════════════════════════════════════════════════════
async function limpiarMonitorConfirmText() {
  if (typeof ilusPrompt !== 'function') {
    if (typeof ilusToast === 'function') ilusToast('UI helpers no disponibles', {type:'error'});
    return;
  }
  var texto = await ilusPrompt({
    title: 'Limpiar TODO el monitor',
    message: 'Esta acción borra TODOS los compromisos. Para confirmar, escribe la palabra:',
    sub: '<strong style="color:#dc2626">LIMPIAR</strong> (en mayúsculas)',
    subHtml: true,
    placeholder: 'Escribe LIMPIAR',
    okLabel: 'Borrar todo',
    cancelLabel: 'Cancelar',
    required: true,
  });
  if (!texto) return;
  if (texto.trim().toUpperCase() !== 'LIMPIAR') {
    if (typeof ilusToast === 'function') ilusToast('Texto incorrecto. Operación cancelada.', {type:'warning'});
    return;
  }
  var btn = document.getElementById('btnLimpiarTodoSA');
  var orig = btn ? btn.innerHTML : '';
  if (btn) {
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner-border spinner-border-sm me-2"></span>Limpiando…';
  }
  try {
    var r = await fetch('/transporte/api/limpiar-todo', {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({ confirm_text: 'LIMPIAR' })
    });
    var d = await r.json();
    if (!d.ok) {
      if (typeof ilusToast === 'function') ilusToast(d.error || 'Error', {type:'error'});
      return;
    }
    if (typeof ilusAlert === 'function') {
      await ilusAlert({
        title: 'Monitor limpiado',
        message: 'Se eliminaron ' + (d.eliminados||0) + ' registros.',
        sub: 'Próximo sync repuebla con datos del ERP.',
        type: 'success',
      });
    }
    if (typeof trLimpiarBackdrops === 'function') trLimpiarBackdrops();
    setTimeout(function(){ location.reload(); }, 400);
  } catch(e) {
    if (typeof ilusToast === 'function') ilusToast('Error de red: '+e.message, {type:'error'});
  } finally {
    if (btn) { btn.disabled = false; btn.innerHTML = orig; }
  }
}

// Cleanup de cualquier backdrop huérfano. Usa la función global del ilus_ui
// si existe, y agrega una pasada defensiva extra. Llamar tras cualquier
// modal Bootstrap O ilusAlert.
function trLimpiarBackdrops() {
  try {
    if (typeof window.ilusCleanModalBackdrops === 'function') {
      window.ilusCleanModalBackdrops();
    }
    // Pasada extra: matar cualquier .modal-backdrop si no queda modal visible
    var abiertos = document.querySelectorAll('.modal.show').length;
    var backdrops = document.querySelectorAll('.modal-backdrop');
    if (abiertos === 0 && backdrops.length > 0) {
      backdrops.forEach(function(b){ b.remove(); });
      document.body.classList.remove('modal-open');
      document.body.style.removeProperty('overflow');
      document.body.style.removeProperty('padding-right');
    }
    // También cleanup de ilus-overlay huérfanos (defensivo)
    var ilusOverlays = document.querySelectorAll('.ilus-overlay');
    ilusOverlays.forEach(function(ov){
      // Si no tiene class .show ni está animando in, es huérfano
      if (!ov.classList.contains('show')) ov.remove();
    });
  } catch(e) { console.warn('[transporte] trLimpiarBackdrops:', e); }
}

function setSync(days) {
  var hoy   = new Date();
  var desde = new Date(hoy - days * 86400000);
  document.getElementById('syncDesde').value = desde.toISOString().split('T')[0];
  document.getElementById('syncHasta').value = hoy.toISOString().split('T')[0];
}

function runSync() {
  var btn = document.getElementById('syncBtn');
  var res = document.getElementById('syncResult');
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Sincronizando… (puede tardar)';
  res.style.display = 'none';

  fetch('/transporte/api/sync', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({
      fecha_desde: document.getElementById('syncDesde').value,
      fecha_hasta: document.getElementById('syncHasta').value
    })
  })
  .then(function(r){ return r.json(); })
  .then(function(d) {
    btn.disabled = false;
    btn.innerHTML = '<i class="bi bi-arrow-repeat me-1"></i>Iniciar sincronización';
    res.style.display = '';
    var errsHtml = '';
    if (d.errores && d.errores.length) {
      errsHtml = '<div class="mt-1 small text-danger">' +
        d.errores.slice(0,5).map(function(e){ return '⚠ ' + e; }).join('<br>') +
        '</div>';
    }
    if (d.importados > 0) {
      res.innerHTML = '<div class="alert alert-success mb-0 py-2">' +
        '<i class="bi bi-check-circle-fill me-1"></i>' +
        '<strong>' + d.importados + ' documentos importados</strong> · ' + (d.rango||'') +
        errsHtml +
        '</div>';
      setTimeout(function(){ location.reload(); }, 1500);
    } else {
      res.innerHTML = '<div class="alert alert-warning mb-0 py-2">' +
        '<i class="bi bi-exclamation-triangle me-1"></i>' +
        'Sin documentos importados en ese rango.' + errsHtml + '</div>';
    }
  })
  .catch(function(e) {
    btn.disabled = false;
    btn.innerHTML = '<i class="bi bi-arrow-repeat me-1"></i>Iniciar sincronización';
    res.style.display = '';
    res.innerHTML = '<div class="alert alert-danger mb-0 py-2">Error de conexión: ' + e + '</div>';
  });
}

function runExcel() {
  var file = document.getElementById('excelFile').files[0];
  if (!file) { alert('Selecciona un archivo'); return; }
  var btn = document.getElementById('excelBtn');
  var res = document.getElementById('syncResult');
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Importando…';
  res.style.display = 'none';

  var fd = new FormData();
  fd.append('archivo', file);
  fetch('/transporte/api/importar-excel', {method:'POST', body:fd})
  .then(function(r){ return r.json(); })
  .then(function(d) {
    btn.disabled = false;
    btn.innerHTML = '<i class="bi bi-upload me-1"></i>Importar archivo';
    res.style.display = '';
    if (d.error) {
      res.innerHTML = '<div class="alert alert-danger mb-0 py-2">' + d.error + '</div>';
      return;
    }
    var previewHtml = '';
    if (d.preview && d.preview.length) {
      previewHtml = '<div class="mt-2 small"><strong>Muestra:</strong> ' +
        d.preview.map(function(p){
          return p.tido + ' ' + trNudo(p.nudo) + ' — ' + p.cliente;
        }).join(', ') + (d.importados > 5 ? '…' : '') + '</div>';
    }
    var errsHtml = d.errores && d.errores.length ?
      '<div class="mt-1 small text-danger">' + d.errores.slice(0,3).join('<br>') + '</div>' : '';
    if (d.importados > 0) {
      res.innerHTML = '<div class="alert alert-success mb-0 py-2">' +
        '<i class="bi bi-check-circle-fill me-1"></i>' +
        '<strong>' + d.importados + ' documentos importados</strong>' +
        previewHtml + errsHtml + '</div>';
      setTimeout(function(){ location.reload(); }, 2000);
    } else {
      res.innerHTML = '<div class="alert alert-warning mb-0 py-2">' +
        'Sin documentos con líneas ZZ y saldo en ese archivo.' + errsHtml + '</div>';
    }
  })
  .catch(function(e) {
    btn.disabled = false;
    btn.innerHTML = '<i class="bi bi-upload me-1"></i>Importar archivo';
    res.innerHTML = '<div class="alert alert-danger">Error: ' + e + '</div>';
    res.style.display = '';
  });
}

// ── Stock ERP (físico/comprometido/devengado) — mismo patrón que
// _stockTooltip() en templates/cubicador/asignar.html (2026-07-25) ──────
function _vistaFmtNum(v){
  return (Math.round((Number(v)||0) * 100) / 100).toLocaleString('es-CL', {maximumFractionDigits: 2});
}
function vistaStockTooltip(st){
  if(!st) return '';
  var tip = 'Stock físico (bodega): <b>' + _vistaFmtNum(st.fisico) + '</b><br>'
          + 'Comprometido (vendido, pend. despacho): <b>' + _vistaFmtNum(st.comprometido) + '</b><br>'
          + 'Devengado (en camino, no ha llegado): <b>' + _vistaFmtNum(st.devengado) + '</b><br>'
          + 'Disponible real: <b>' + _vistaFmtNum(st.disponible) + '</b>';
  return ' <i class="bi bi-info-circle stock-info-ic" data-bs-toggle="tooltip" data-bs-html="true" ' +
         'data-bs-placement="top" title="' + tip + '"></i>';
}

// ── Selección múltiple de líneas (checkbox + shift-click para rango) ────
// 2026-07-26 (pedido Daniel): "que los productos se puedan seleccionar
// múltiples veces" — permite marcar varias líneas a la vez para futuras
// acciones en lote sobre la vista del documento.
var _vistaLastCheckedIdx = null;
function selectVistaRow(ev, idx) {
  var boxes = Array.prototype.slice.call(document.querySelectorAll('#vistaTbody .vista-row-chk'));
  if (ev && ev.shiftKey && _vistaLastCheckedIdx !== null) {
    var a = Math.min(_vistaLastCheckedIdx, idx), b = Math.max(_vistaLastCheckedIdx, idx);
    var checked = boxes[idx] ? boxes[idx].checked : true;
    boxes.forEach(function(cb){
      var i = parseInt(cb.dataset.idx, 10);
      if (i >= a && i <= b) cb.checked = checked;
    });
  }
  _vistaLastCheckedIdx = idx;
  var all = document.getElementById('vistaCheckAll');
  if (all) all.checked = boxes.length > 0 && boxes.every(function(cb){ return cb.checked; });
}
function toggleVistaCheckAll(master) {
  document.querySelectorAll('#vistaTbody .vista-row-chk').forEach(function(cb){ cb.checked = master.checked; });
}

// ── Modal "Ítems en manifiestos" (Daniel 2026-07-26) ─────────────────────
// Namespace "im*". Lista TODOS los documentos ya agregados a un manifiesto
// activo (no cerrado/cancelado), avisando cuando el mismo documento está en
// más de uno (duplicidad de envío), con botón para quitarlo del manifiesto.
function imAbrir() {
  var tbody = document.getElementById('imTbody');
  var msg = document.getElementById('imMsg');
  tbody.innerHTML = '';
  msg.textContent = 'Cargando…';
  msg.style.display = '';
  fetch('/transporte/api/manifiestos/items-todos')
    .then(function(r){ return r.json(); })
    .then(function(d){
      if (!d.ok) { msg.textContent = d.error || 'No se pudo cargar'; return; }
      document.getElementById('imTotalBadge').textContent = d.items.length + ' ítem' + (d.items.length !== 1 ? 's' : '');
      var dupBadge = document.getElementById('imDupBadge');
      if (d.n_duplicados > 0) {
        dupBadge.style.display = '';
        dupBadge.textContent = d.n_duplicados + ' duplicado' + (d.n_duplicados !== 1 ? 's' : '');
      } else {
        dupBadge.style.display = 'none';
      }
      if (!d.items.length) {
        msg.textContent = 'No hay documentos en manifiestos activos por ahora.';
        return;
      }
      msg.style.display = 'none';
      d.items.forEach(function(it){
        var tr = document.createElement('tr');
        if (it.duplicado) tr.style.background = '#fee2e2';
        tr.innerHTML =
          '<td class="font-monospace fw-bold">' + (it.doc || '') + '</td>' +
          '<td>' + (it.cliente || '—') + '</td>' +
          '<td>' + (it.comuna || '') + '</td>' +
          '<td>' + (it.correlativo || ('#' + it.manifest_id)) +
            (it.duplicado ? ' <span class="badge bg-danger" title="Está en ' + it.n_manifiestos_activos + ' manifiestos activos">'
              + '<i class="bi bi-exclamation-triangle-fill"></i> x' + it.n_manifiestos_activos + '</span>' : '') +
          '</td>' +
          '<td>' + (it.manifest_estado || '') + '</td>' +
          '<td class="text-center">' + (it.n_bultos || 1) + '</td>' +
          '<td class="text-end">' + (it.costo_zz ? '$' + Math.round(it.costo_zz).toLocaleString('es-CL') : '—') + '</td>' +
          '<td class="text-center">' +
            '<button type="button" class="btn-cube-del" title="Quitar este documento del manifiesto" ' +
            'onclick="imQuitar(' + it.manifest_id + ',' + it.item_id + ', this)">' +
            '<i class="bi bi-trash"></i></button>' +
          '</td>';
        tbody.appendChild(tr);
      });
    })
    .catch(function(){ msg.textContent = 'Error de conexión'; });
}

function imQuitar(manifestId, itemId, btn) {
  ilusConfirm({
    title: 'Quitar del manifiesto',
    message: '¿Quitar este documento del manifiesto? Sigue disponible para agregarse de nuevo después.',
    okLabel: 'Quitar', cancelLabel: 'Cancelar', danger: true,
  }).then(function(ok){
    if (!ok) return;
    fetch('/transporte/manifiestos/' + manifestId + '/items/' + itemId, { method: 'DELETE' })
      .then(function(r){ return r.json().then(function(d){ return {status:r.status, d:d}; }); })
      .then(function(res){
        if (res.d && res.d.error) { ilusToast(res.d.error, {type:'error'}); return; }
        ilusToast('✓ Quitado del manifiesto', {type:'success'});
        var tr = btn.closest('tr'); if (tr) tr.remove();
        imAbrir(); // recarga para refrescar contadores/duplicados
      })
      .catch(function(){ ilusToast('Error de conexión', {type:'error'}); });
  });
}

// ── Modal de Vista de documento ─────────────────────────────────
function openVista(cid) {
  _cotizarCid = cid;
  _vistaLastCheckedIdx = null;
  // Resetear estado
  document.getElementById('vistaDocNum').textContent  = '…';
  document.getElementById('vistaFecha').textContent   = '';
  document.getElementById('vistaCliente').textContent = '';
  document.getElementById('vistaRut').textContent     = '';
  document.getElementById('vistaComuna').textContent  = '';
  document.getElementById('vistaCosto').textContent   = '';
  document.getElementById('vistaClasif').textContent  = '';
  document.getElementById('vistaDir').textContent      = '';
  document.getElementById('vistaTel').textContent      = '';
  document.getElementById('vistaEmail').textContent    = '';
  document.getElementById('vistaDirBlock').style.display   = 'none';
  document.getElementById('vistaTelBlock').style.display   = 'none';
  document.getElementById('vistaEmailBlock').style.display = 'none';
  document.getElementById('vistaTelLlamar').style.display  = 'none';
  document.getElementById('vistaTelWa').style.display      = 'none';
  document.getElementById('vistaTrackingChip').style.display = 'none';
  document.getElementById('vistaFotoUpload').style.display = 'none';
  document.getElementById('vistaFotoGallery').innerHTML    = '';
  document.getElementById('vistaLoading').style.display  = 'none';
  document.getElementById('vistaTablaWrap').style.display = 'none';
  document.getElementById('vistaError').style.display    = 'none';
  document.getElementById('vistaSinPeso').style.display  = 'none';

  // Proceso de espera ILUS (overlay premium con el logo). El modal se abre
  // cuando los productos están listos (con caché es casi instantáneo).
  if (window.ilusLoader) ilusLoader.show({ text: 'Trayendo productos del documento…',
                                           sub: 'un momento' });

  // Timeout DURO: si el backend no responde en 9s, abortamos y avisamos — el
  // modal NUNCA queda "procesando" sin fin (Daniel 2026-06-14).
  var _ctrl = new AbortController();
  var _to = setTimeout(function(){ try { _ctrl.abort(); } catch(_e){} }, 9000);

  fetch('/transporte/api/compromisos/' + cid + '/detalle', { signal: _ctrl.signal })
    .then(function(r){ return r.json(); })
    .then(function(d) {
      clearTimeout(_to);
      if (window.ilusLoader) ilusLoader.hide();
      if (!d.ok) {
        if (window.ilusToast) ilusToast(d.error || 'No se pudo abrir el documento', { type: 'error' });
        else { document.getElementById('vistaErrorMsg').textContent = d.error || 'Error';
               document.getElementById('vistaError').style.display = ''; _vistaModal.show(); }
        return;
      }

      // FIX 2026-07-31 (Daniel: unificar el "Ver" del Monitor con la ficha del
      // manifiesto — "que sea EXACTAMENTE el mismo modal, no una copia") — si
      // el documento ya tiene un despacho/manifest_item asignado (aunque esté
      // en un estado terminal), se abre el mismo modal premium que usa
      // manifiesto_detalle.html (abrirTrackingDetalle para FedEx,
      // abrirSimpliRouteModal para Felca/Milling/SimpliRoute), en vez del
      // vistaModal viejo de este Monitor. Solo si falta el item_id (dato
      // inconsistente/legado) se cae de vuelta al vistaModal como respaldo,
      // para nunca dejar un clic sin respuesta.
      var comp = d.compromiso || {};
      if (comp.manifiesto_id && comp.item_id) {
        var _courierL = (comp.tracking_courier || '').toLowerCase();
        if (_courierL.indexOf('fedex') !== -1) {
          abrirTrackingDetalle(comp.item_id);
        } else {
          abrirSimpliRouteModal({
            id:                comp.id,
            item_id:           comp.item_id,
            doc:               ((comp.tido || '') + ' ' + trNudo(comp.nudo || '')).trim(),
            cliente:           comp.cliente || '',
            comuna:            comp.comuna || '',
            courier:           comp.tracking_courier || '',
            estado:            comp.estado || '',
            edicion_bloqueada: !!comp.edicion_bloqueada,
            edicion_motivo:    comp.edicion_motivo || '',
          });
        }
        return;
      }

      // Opción 2 (Daniel): documento "pendiente" -- todavía SIN despacho ni
      // manifiesto asignado (o, en el caso defensivo de arriba, con datos
      // inconsistentes). Se mantiene el vistaModal (mismo hero oscuro +
      // tarjetas claras, mismos datos de documento/cliente/productos de
      // tr_detalle) -- ver el aviso "Aún no despachado" en renderVista().
      // La trazabilidad se sigue autocargando igual que antes (puede haber
      // logs a nivel de documento, ej. ediciones, aunque no haya manifiesto).
      _vistaModal.show();
      renderVista(d);
      cargarTrazabilidadInline(cid);
    })
    .catch(function(e) {
      clearTimeout(_to);
      if (window.ilusLoader) ilusLoader.hide();
      if (e && e.name === 'AbortError') {
        if (window.ilusToast) ilusToast('El ERP está lento ahora. Intenta de nuevo en unos segundos.', { type: 'warning' });
      } else if (window.ilusToast) {
        ilusToast('Error de conexión: ' + e, { type: 'error' });
      }
    });
}

// Colorea/actualiza el badge de estado del modal de vista. Extraído de
// renderVista() (2026-07-26) para que "Actualizar estado" pueda refrescar
// el badge sin duplicar el mapa de colores.
function renderEstadoBadge(estado) {
  var elBadge = document.getElementById('vistaEstadoBadge');
  if (!elBadge) return;
  if (!estado) { elBadge.style.display = 'none'; return; }
  var COLOR_ESTADO = {
    'Entregado':               'background:#16a34a;color:#fff',
    'Entregado completo':      'background:#16a34a;color:#fff',
    'En ruta':                 'background:#3b82f6;color:#fff',
    'Entregado a transporte':  'background:#3b82f6;color:#fff',
    'En preparación':          'background:#f59e0b;color:#0a0a0a',
    'En curso':                'background:#f59e0b;color:#0a0a0a',
    'Entrega fallida':         'background:#dc2626;color:#fff',
    'Problema':                'background:#dc2626;color:#fff',
  };
  elBadge.style.cssText = elBadge.style.cssText.replace(/background:[^;]*;?color:[^;]*;?/, '')
    + ';' + (COLOR_ESTADO[estado] || 'background:#6b7280;color:#fff');
  elBadge.textContent = estado;
  elBadge.style.display = '';
}

function renderVista(d) {
  var c = d.compromiso;
  var lineas = d.lineas || [];
  var lineas_zz = d.lineas_zz || [];
  var tot = d.totales || {};

  // Encabezado
  document.getElementById('vistaDocNum').textContent = c.tido + ' ' + trNudo(c.nudo);
  document.getElementById('vistaFecha').textContent  = c.fecha_emision || '';
  document.getElementById('vistaCliente').textContent = c.cliente || '—';
  document.getElementById('vistaRut').textContent     = c.rut    || '';
  document.getElementById('vistaComuna').textContent  = c.comuna || '—';
  document.getElementById('vistaCosto').textContent   =
    c.costo_zz ? '$' + Math.round(c.costo_zz).toLocaleString('es-CL') : '—';
  var clasifMap2 = {
    retiro:'↩ Retiro', instalacion:'🔧 Instalación',
    mantencion:'🔨 Mantención', garantia:'🛡 Garantía'
  };
  document.getElementById('vistaClasif').textContent  =
    clasifMap2[c.clasificacion] || '📦 Despacho';

  // 2026-07-31 (Daniel: reconocer cuando el documento trae mas de un
  // envio/instalacion/retiro, ej. una factura dividida en varios despachos).
  // d.zz_conteo = [{tipo, label, cantidad}, ...] solo con los tipos presentes.
  (function() {
    var wrap = document.getElementById('vistaZzConteo');
    var block = document.getElementById('vistaZzConteoBlock');
    if (!wrap || !block) return;
    var conteo = d.zz_conteo || [];
    if (!conteo.length) { block.style.display = 'none'; wrap.innerHTML = ''; return; }
    block.style.display = '';
    wrap.innerHTML = conteo.map(function(z) {
      var multi = z.cantidad > 1;
      return '<span style="display:inline-flex;align-items:center;gap:4px;'
        + 'background:' + (multi ? 'rgba(220,38,38,.18)' : 'rgba(255,255,255,.08)') + ';'
        + 'border:1px solid ' + (multi ? 'rgba(220,38,38,.4)' : 'rgba(255,255,255,.15)') + ';'
        + 'border-radius:20px;padding:3px 10px;font-size:.78rem;'
        + 'color:' + (multi ? '#ff8a8a' : '#ddd') + '">'
        + (multi ? ('<b>' + z.cantidad + '×</b> ') : '') + esc(z.label) + '</span>';
    }).join('');
  })();

  // Tracking del courier en el header (2026-07-29, Daniel: "quiero un
  // tracking arriba en el header") — visible apenas se abre el modal.
  (function() {
    var chip = document.getElementById('vistaTrackingChip');
    if (!chip) return;
    if (c.tracking_number) {
      var lbl = c.tracking_courier ? (c.tracking_courier + ' ') : '';
      document.getElementById('vistaTrackingNum').textContent = lbl + c.tracking_number;
      chip.style.display = '';
    } else {
      chip.style.display = 'none';
    }
  })();

  // Badge de estado actual + datos para el reenvío manual de correo (2026-07-26)
  _vistaCurrentEmail  = c.email  || null;
  _vistaCurrentEstado = c.estado || '';
  _vistaCurrentCliente = c.cliente || '';
  renderEstadoBadge(c.estado_logistico || c.estado);

  // Habilitar/deshabilitar el botón de reenvío según si el pedido tiene correo
  (function() {
    var btn = document.getElementById('btnReenviarNotif');
    if (!btn) return;
    btn.disabled = !c.email;
    btn.title = c.email
      ? 'Reenviar el estado actual por correo al cliente (' + c.email + ')'
      : 'Este pedido no tiene un correo de cliente registrado';
  })();

  // FIX 2026-07-28 (Daniel, viendo el modal en vivo: "acá veo editar campos
  // y todavía cuando entro a ver los productos, me da para editar los
  // campos" en un pedido que ya figura Entregado). Mismo criterio ya
  // aplicado a los botones de la fila de la tabla: si el estado es
  // terminal, "Asignar a manifiesto" y "Editar campos" quedan bloqueados
  // acá también -- las otras acciones (cotizar, reenviar correo, actualizar
  // estado, ver trazabilidad) siguen activas porque son de solo consulta o
  // no cambian el documento.
  //
  // FIX 2026-07-29 (Daniel: "si ya está asignado a manifiesto, no tengo
  // por qué asignarlo a menos que tenga un saldo") -- se suma un segundo
  // motivo de bloqueo específico para "Asignar a manifiesto": documento YA
  // con manifiesto asignado (c.manifiesto_id) Y sin saldo pendiente
  // (c.tiene_saldo === 0). "Editar campos" NO se ve afectado por esta regla
  // -- sigue bloqueado solo por estado terminal.
  (function() {
    var _term = (c.estado === 'Entregado' || c.estado === 'Devolución');
    var _motivo = 'Bloqueado: este documento ya está entregado';
    var _sinSaldoAsignado = !!(c.manifiesto_id) && (c.tiene_saldo === 0);
    var _motivoSaldo = 'Ya está asignado a ' + (c.manifiesto_correlativo || ('manifiesto #' + c.manifiesto_id))
      + ' y no le queda saldo pendiente por despachar.';
    var btnManif = document.getElementById('btnVistaManifiesto');
    var btnEditar = document.getElementById('btnVistaEditar');
    if (btnManif) {
      btnManif.disabled = _term || _sinSaldoAsignado;
      btnManif.title = _term ? _motivo
        : (_sinSaldoAsignado ? _motivoSaldo : 'Agregar este documento al panel de manifiesto');
    }
    if (btnEditar) {
      btnEditar.disabled = _term;
      btnEditar.title = _term ? _motivo : 'Editar estado, costo, notas del documento';
    }
  })();

  // FIX 2026-07-29 (Daniel: "identificar en que manifiesto esta y que las
  // acciones me lleven ahi") — botón "Ir al manifiesto", siempre activo
  // (incluso si el documento está bloqueado por estado terminal).
  (function() {
    var btnIrManif = document.getElementById('btnVistaIrManifiesto');
    if (!btnIrManif) return;
    if (c.manifiesto_id) {
      btnIrManif.href = '/transporte/manifiestos/' + c.manifiesto_id;
      btnIrManif.title = 'Ir a ' + (c.manifiesto_correlativo || ('manifiesto #' + c.manifiesto_id));
      btnIrManif.style.display = '';
    } else {
      btnIrManif.style.display = 'none';
    }
  })();

  // FIX 2026-07-31 (Daniel, Opción 2 de la unificación Monitor/manifiesto):
  // este vistaModal, desde ahora, solo se abre para documentos que TODAVÍA no
  // tienen manifest_item (los que sí tienen van directo a abrirTrackingDetalle
  // / abrirSimpliRouteModal — ver openVista). Se avisa con claridad en vez de
  // dejarlo implícito en una sección de trazabilidad vacía.
  (function() {
    var banner = document.getElementById('vistaPendienteBanner');
    if (!banner) return;
    banner.style.display = c.manifiesto_id ? 'none' : '';
  })();

  // Campos separados: dirección, teléfono, correo
  if (c.direccion) {
    document.getElementById('vistaDir').textContent = c.direccion;
    document.getElementById('vistaDirBlock').style.display = '';
  }
  if (c.telefono) {
    document.getElementById('vistaTel').textContent = c.telefono;
    document.getElementById('vistaTelBlock').style.display = '';
    // 2026-07-29 (Daniel: "que me pueda hacer atajos para yo hablarle al
    // cliente o escribirle por WhatsApp") — tel: y wa.me. Chile: normaliza
    // a E.164 sin símbolos para el link de WhatsApp (ej "+56 9 1234 5678" →
    // "56912345678"); si el número viene sin código de país y tiene 9
    // dígitos (celular chileno), se antepone "56".
    var _telDigits = String(c.telefono).replace(/[^0-9]/g, '');
    if (_telDigits.length === 9 && _telDigits[0] === '9') _telDigits = '56' + _telDigits;
    var _btnLlamar = document.getElementById('vistaTelLlamar');
    var _btnWa     = document.getElementById('vistaTelWa');
    if (_btnLlamar) {
      _btnLlamar.href = 'tel:+' + (_telDigits || String(c.telefono).replace(/[^0-9+]/g, ''));
      _btnLlamar.style.display = '';
    }
    if (_btnWa && _telDigits) {
      _btnWa.href = 'https://wa.me/' + _telDigits;
      _btnWa.style.display = '';
    } else if (_btnWa) {
      _btnWa.style.display = 'none';
    }
  }
  if (c.email) {
    document.getElementById('vistaEmail').textContent = c.email;
    document.getElementById('vistaEmailBlock').style.display = '';
  }

  // Panel de fotos: SKU de la primera línea de producto
  _vistaCurrentSku = null;
  if (lineas.length) {
    _vistaCurrentSku = lineas[0].sku || null;
    renderFotoPanel(lineas[0]);
  }

  // Tabla de productos
  var tbody = document.getElementById('vistaTbody');
  tbody.innerHTML = '';
  var sinPeso = true;

  lineas.forEach(function(l, rowIdx) {
    if (l.tiene_bultos) sinPeso = false;

    // Miniatura de fotos
    var fotosHtml = '';
    if (l.fotos && l.fotos.length) {
      fotosHtml = l.fotos.map(function(url) {
        return '<img src="' + url + '" alt="" ' +
          'style="width:36px;height:36px;object-fit:cover;border-radius:5px;cursor:pointer;' +
          'border:1px solid #ddd;margin-right:2px" ' +
          'onclick="verFoto(\'' + url + '\')" ' +
          'onerror="this.style.display=\'none\'">';
      }).join('');
    } else {
      fotosHtml = '<div style="width:36px;height:36px;background:#f0f0f0;border-radius:5px;' +
        'display:flex;align-items:center;justify-content:center">' +
        '<i class="bi bi-image text-muted" style="font-size:.65rem"></i></div>';
    }

    // Color de pred
    var esPredKg  = l.peso_kg_u  >= l.peso_vol_u;
    var kgClass   = esPredKg  ? 'fw-bold' : 'text-muted';
    var pvClass   = !esPredKg ? 'fw-bold' : 'text-muted';
    var predStyle = 'font-weight:700;color:var(--ilus-red)';

    // Saldo
    var saldo = (l.saldo !== undefined && l.saldo !== null) ? l.saldo : l.cantidad;
    var stockTip = vistaStockTooltip(l.stock);
    var saldoHtml = '<td class="text-center">' +
      (saldo > 0 ? '<span class="fw-bold" style="color:var(--ilus-red)">' + saldo + '</span>'
                 : '<span class="text-muted">0</span>') + stockTip + '</td>';

    // Guía de despacho real (2026-08-01, Daniel: columna "Guía" en la tabla
    // de productos del modal) -- l.guia_numero lo arma tr_detalle cruzando
    // transport_guias por SKU (la más reciente si hubo re-despacho). "—" si
    // el producto todavía no tiene guía registrada en el ERP.
    // _fmtGuiaSinCeros (definida en transporte_manifiesto_detalle.js, mismo
    // window global -- ambos scripts se cargan juntos en index.html) quita
    // los ceros a la izquierda del NUDO del ERP (Daniel 2026-08-01: "quítale
    // todos los ceros a la izquierda de la guía").
    var guiaHtml = '<td class="text-center">' +
      (l.guia_numero ? '<span class="font-monospace" style="font-size:.72rem">' + esc(_fmtGuiaSinCeros(l.guia_numero)) + '</span>'
                      : '<span class="text-muted">—</span>') +
      '</td>';

    // Botón editar medidas (solo si no tiene bultos)
    var editBtn = '';
    if (!l.tiene_bultos) {
      var skuEsc    = (l.sku||'').replace(/'/g, "\\'");
      var nombreEsc = (l.nombre_app || l.descripcion_erp || '').replace(/'/g, "\\'");
      editBtn = '<button class="btn btn-xs btn-outline-secondary btn-sm py-0 px-1" ' +
        'style="font-size:.7rem" title="Agregar medidas" ' +
        'onclick="abrirEditPeso(\'' + skuEsc + '\',\'' + nombreEsc + '\',' + rowIdx + ')">' +
        '<i class="bi bi-rulers"></i></button>';
    }

    var tr = document.createElement('tr');
    tr.innerHTML =
      '<td class="text-center"><input type="checkbox" class="vista-row-chk" data-idx="' + rowIdx + '" ' +
      'onclick="selectVistaRow(event, ' + rowIdx + ')"></td>' +
      '<td style="padding:8px 10px">' + fotosHtml + '</td>' +
      '<td class="font-monospace fw-bold" style="font-size:.77rem">' + (l.sku||'') + '</td>' +
      '<td>' +
        '<div class="fw-semibold" style="line-height:1.2;color:#212529">' + (l.nombre_app || l.descripcion_erp || '') + '</div>' +
        (l.nombre_app && l.diferencia
          ? '<div class="small text-muted" style="font-size:.7rem;font-style:italic">' + l.descripcion_erp + '</div>'
          : '') +
      '</td>' +
      '<td class="text-center fw-bold">' + (l.cantidad||0) + '</td>' +
      saldoHtml +
      guiaHtml +
      '<td class="text-end ' + kgClass + '">' + fmt3(l.peso_kg_u)  + '</td>' +
      '<td class="text-end ' + pvClass + '">' + fmt3(l.peso_vol_u) + '</td>' +
      '<td class="text-end">'  + fmt3(l.pred_u)     + '</td>' +
      '<td class="text-end" style="' + predStyle + '">' + fmt3(l.pred_tot) + '</td>' +
      '<td class="text-center">' + editBtn + '</td>';
    tbody.appendChild(tr);
  });

  // Tooltips del desglose de stock (bootstrap.Tooltip) — se recrean en cada
  // render porque tbody.innerHTML se reemplaza (mismo patrón que asignar.html).
  if (window.bootstrap && window.bootstrap.Tooltip) {
    tbody.querySelectorAll('[data-bs-toggle="tooltip"]').forEach(function(el){
      new bootstrap.Tooltip(el);
    });
  }

  // Totales
  var esTotKg = (tot.kg || 0) >= (tot.pv || 0);
  var tfoot = document.getElementById('vistaTfoot');
  tfoot.innerHTML =
    '<tr>' +
    // colspan=7 (antes 6): incluye la columna Guía agregada 2026-08-01 entre
    // Saldo y Kg/u -- si no se corre el colspan, los totales de Kg/PV/Pred
    // quedan desplazados una columna a la izquierda.
    '<td colspan="7" class="ps-3" style="font-size:.78rem;color:#666">TOTALES</td>' +
    '<td class="text-end ' + (esTotKg  ? 'fw-bold' : 'text-muted') + '">' + fmt3(tot.kg)   + '</td>' +
    '<td class="text-end ' + (!esTotKg ? 'fw-bold' : 'text-muted') + '">' + fmt3(tot.pv)   + '</td>' +
    '<td class="text-end"></td>' +
    '<td class="text-end" style="font-size:1rem;font-weight:800;color:var(--ilus-red)">' +
      fmt3(tot.pred) + ' ' +
      '<span style="font-size:.65rem;background:var(--ilus-red);color:#fff;' +
      'padding:1px 5px;border-radius:99px;vertical-align:middle">' +
      (esTotKg ? 'KG' : 'PV') + '</span>' +
    '</td>' +
    '<td></td>' +
    '</tr>';

  // Sección ZZ
  var zzSection = document.getElementById('vistaZZSection');
  var zzRows    = document.getElementById('vistaZZRows');
  var zzCosto   = document.getElementById('vistaZZCosto');
  if (lineas_zz && lineas_zz.length > 0) {
    var costoEnvio = c.costo_zz_envio || 0;
    if (costoEnvio) {
      zzCosto.textContent = '$' + costoEnvio.toLocaleString('es-CL');
    } else {
      zzCosto.textContent = '';
    }
    // FIX 2026-08-01 (Daniel, en vivo: "necesito el precio neto"): antes
    // usaba z.pred_u (peso predicho, siempre 0 para servicios sin ficha de
    // bultos) -- el precio neto real de la línea es z.vaneli (VANELI del
    // ERP, valor línea). Si el mismo SKU se repite (ej. 2 líneas de
    // instalación), cada línea es su propia fila con su propio precio --
    // no se agrupan ni se pisan entre sí.
    zzRows.innerHTML = lineas_zz.map(function(z) {
      var valNeto = z.vaneli || 0;
      var valFmt  = valNeto > 0
        ? '<span class="fw-bold" style="color:var(--ilus-red)">$' + Math.round(valNeto).toLocaleString('es-CL') + '</span>'
        : '<span class="text-muted">' + (z.cantidad||0) + ' u</span>';
      return '<div class="d-flex justify-content-between align-items-center py-1 border-bottom">' +
        '<span class="font-monospace text-muted" style="font-size:.75rem">' + (z.sku||'') + '</span>' +
        '<span class="flex-grow-1 px-2">' + (z.descripcion_erp||z.nombre_app||'') + '</span>' +
        valFmt +
        '</div>';
    }).join('');
    zzSection.style.display = '';
  } else {
    zzSection.style.display = 'none';
  }

  if (sinPeso && lineas.length > 0) {
    document.getElementById('vistaSinPeso').style.display = '';
  }
  document.getElementById('vistaTablaWrap').style.display = '';
}

var _editPesoModal = null;
var _editPesoRowIndex = null;
var _epBultoCount = 0;

function abrirEditPeso(sku, nombre, rowIdx) {
  if (!_editPesoModal) _editPesoModal = new bootstrap.Modal(document.getElementById('editPesoModal'));
  document.getElementById('editPesoSku').textContent = sku;
  document.getElementById('editPesoSkuVal').value    = sku;
  document.getElementById('editPesoNombreVal').value = nombre || '';
  document.getElementById('editPesoNombreLabel').textContent =
    sku + (nombre ? ' — ' + nombre : '');
  document.getElementById('epMsg').innerHTML = '';
  // Limpiar y agregar 1 bulto vacío
  _epBultoCount = 0;
  document.getElementById('epBultosContainer').innerHTML = '';
  epAgregarBulto();
  _editPesoRowIndex = rowIdx;
  _editPesoModal.show();
}

function epAgregarBulto(vals) {
  _epBultoCount++;
  var n = _epBultoCount;
  var row = document.createElement('div');
  row.className = 'row g-1 mb-1 ep-bulto-row';
  row.dataset.bulto = n;
  var v = vals || {};
  row.innerHTML =
    '<div class="col-1 d-flex align-items-center justify-content-center">' +
      '<span class="badge bg-secondary" style="font-size:.65rem">' + n + '</span>' +
    '</div>' +
    '<div class="col"><input type="number" class="form-control form-control-sm ep-largo" placeholder="0" min="0" step="0.1" value="' + (v.largo||'') + '"></div>' +
    '<div class="col"><input type="number" class="form-control form-control-sm ep-ancho" placeholder="0" min="0" step="0.1" value="' + (v.ancho||'') + '"></div>' +
    '<div class="col"><input type="number" class="form-control form-control-sm ep-alto"  placeholder="0" min="0" step="0.1" value="' + (v.alto||'') + '"></div>' +
    '<div class="col"><input type="number" class="form-control form-control-sm ep-peso"  placeholder="0" min="0" step="0.001" value="' + (v.peso||'') + '"></div>' +
    '<div class="col-1 d-flex align-items-center justify-content-center">' +
      (n > 1 ? '<button type="button" class="btn btn-sm btn-link text-danger p-0" onclick="epEliminarBulto(this)" title="Eliminar"><i class="bi bi-x-lg"></i></button>' : '') +
    '</div>';
  document.getElementById('epBultosContainer').appendChild(row);
}

function epEliminarBulto(btn) {
  var row = btn.closest('.ep-bulto-row');
  if (row) row.remove();
}

function guardarPesoInline() {
  var btn    = document.getElementById('epSaveBtn');
  var msg    = document.getElementById('epMsg');
  var sku    = document.getElementById('editPesoSkuVal').value;
  var nombre = document.getElementById('editPesoNombreVal').value;

  // Recoger todos los bultos
  var bultos = [];
  document.querySelectorAll('#epBultosContainer .ep-bulto-row').forEach(function(row) {
    var l = parseFloat(row.querySelector('.ep-largo').value) || 0;
    var a = parseFloat(row.querySelector('.ep-ancho').value) || 0;
    var h = parseFloat(row.querySelector('.ep-alto').value)  || 0;
    var p = parseFloat(row.querySelector('.ep-peso').value)  || 0;
    if (l && a && h && p) bultos.push({largo:l, ancho:a, alto:h, peso:p});
  });

  if (!bultos.length) {
    msg.innerHTML = '<span class="text-danger">Ingresa al menos un bulto completo (largo, ancho, alto y peso)</span>';
    return;
  }
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Guardando…';

  fetch('/transporte/api/inline-bulto', {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify({sku:sku, nombre:nombre, bultos:bultos})
  })
  .then(function(r){ return r.json(); })
  .then(function(d) {
    btn.disabled = false;
    btn.innerHTML = '<i class="bi bi-floppy me-1"></i>Guardar medidas';
    if (d.ok) {
      msg.innerHTML = '<span class="text-success"><i class="bi bi-check-circle me-1"></i>Guardado (' + bultos.length + ' bulto' + (bultos.length>1?'s':'') + ')</span>';
      // Actualizar fila en tabla
      if (_editPesoRowIndex !== null) {
        var rows = document.querySelectorAll('#vistaTbody tr');
        if (rows[_editPesoRowIndex]) {
          var tds = rows[_editPesoRowIndex].querySelectorAll('td');
          if (tds[5]) tds[5].innerHTML = '<span class="fw-bold">' + fmt3(d.peso_kg) + '</span>';
          if (tds[6]) tds[6].innerHTML = '<span class="fw-bold">' + fmt3(d.peso_vol) + '</span>';
          if (tds[7]) tds[7].textContent = fmt3(d.pred);
          if (tds[8]) tds[8].innerHTML = '<span style="font-weight:700;color:var(--ilus-red)">' + fmt3(d.pred) + '</span>';
          if (tds[9]) tds[9].innerHTML = '';
        }
      }
      setTimeout(function(){ if(_editPesoModal) _editPesoModal.hide(); }, 900);
    } else {
      msg.innerHTML = '<span class="text-danger">' + (d.error||'Error') + '</span>';
    }
  });
}

function fmt3(v) {
  /* 1 decimal + separador de miles — ej: 1.134,2 kg */
  if (v === null || v === undefined || v === 0) return '<span class="text-muted">—</span>';
  return parseFloat(v).toLocaleString('es-CL', {minimumFractionDigits:1, maximumFractionDigits:1});
}

function verFoto(url) {
  document.getElementById('fotoModalImg').src = url;
  _fotoModal.show();
}

// ── Monitor principal ────────────────────────────────────────────
var currentId = null;
var _monitorData = [];  // cache de compromisos cargados
var MON = window.MON || {};
var manifiestos = MON.manifiestos;
var ESTADOS = MON.estados;
var COURIERS = MON.couriers;
var ESTADO_COLORS = {
  'Pendiente':'warning',
  'Despachado parcial':'primary',
  'Entregado':'success',
  'En proceso':'primary','Despachado':'success',
  'Problema':'danger','Pedido de vuelta':'danger','Prioridad':'danger',
  'Preventa':'secondary','Indemnización':'danger','Garantía':'info',
  'Logística inversa':'secondary','Indemnización revisada':'warning',
  'Indemnización rechazada':'danger','Regalo':'info','Reentrega':'warning',
  'En preparación':'secondary','Entregado a transporte':'info',
  'En ruta':'primary','Devolución':'warning',
  // Estados LOGÍSTICOS (2026-08-01). "Por despachar" = no está en ningún
  // manifiesto todavía: nadie lo tomó. Es lo que antes se mostraba como
  // "Despachado" cuando el ERP cerraba la línea de flete.
  'Por despachar':'warning','Entrega fallida':'danger'
};

// Vista actual de GESTIÓN (pendientes/en_gestion/entregados/todos)
// Por defecto: pendientes (es lo que el operador ve al llegar).
// Migración: una preferencia vieja guardada como 'parciales' (modelo anterior
// "En camino") se mapea al nuevo bucket 'en_gestion'.
var _vistaActual = (function(){
  try {
    var v = localStorage.getItem('tr_vista_actual') || 'pendientes';
    if (v === 'parciales') v = 'en_gestion';
    return v;
  } catch(e){ return 'pendientes'; }
})();

function setVistaMonitor(btn, vista){
  _vistaActual = vista || 'pendientes';
  try { localStorage.setItem('tr_vista_actual', _vistaActual); } catch(e){}
  // Quitar active de TODOS los segmented btns y legacy .btn-vista
  document.querySelectorAll('.tr-segment-btn, .btn-vista').forEach(function(b){ b.classList.remove('active'); });
  if (btn) btn.classList.add('active');
  else {
    // Fallback: buscar por data-vista
    var target = document.querySelector('.tr-segment-btn[data-vista="' + _vistaActual + '"]');
    if (target) target.classList.add('active');
  }
  _trResetPagina();   // cambiar de vista siempre vuelve a la página 1
  cargarMonitor();
  renderAlertas();
}

// Ramo (tipo de servicio) actual — Despacho/Instalación/Retiro/Mantención/
// Garantía, o '' = Todos. 2026-08-01, Daniel: restructuración del Monitor
// con el ramo como eje principal de navegación (antes solo filtraba client-
// side las filas ya cargadas; ahora es un parámetro real al backend, igual
// que _vistaActual, y usa el mismo criterio multi-ramo que el resto de PR-B
// -- ver _tr_ramo_where en app.py -- así un documento con despacho+
// instalación aparece en AMBAS pestañas, no solo en la de mayor prioridad).
// Al cargar la página, prioriza lo que venga en la URL (?clasificacion=,
// por si se llegó desde el <select> viejo de filtros o un link directo)
// sobre lo guardado en localStorage.
var _ramoActual = (function(){
  try {
    var porUrl = new URLSearchParams(window.location.search).get('clasificacion');
    if (porUrl != null) return porUrl;
    return localStorage.getItem('tr_ramo_actual') || '';
  } catch(e){ return ''; }
})();

function setRamoMonitor(btn, ramo){
  _ramoActual = ramo || '';
  try { localStorage.setItem('tr_ramo_actual', _ramoActual); } catch(e){}
  document.querySelectorAll('.tr-tab').forEach(function(b){ b.classList.remove('active'); });
  if (btn) btn.classList.add('active');
  else {
    var target = document.querySelector('.tr-tab[data-clasif="' + _ramoActual + '"]');
    if (target) target.classList.add('active');
  }
  // Mantener el <select name="clasificacion"> del formulario de filtros en
  // sincronía: es el MISMO parámetro que estas pestañas (?clasificacion=).
  var _selCl = document.querySelector('form.tr-filters [name="clasificacion"]');
  if (_selCl) {
    var _ok = [].slice.call(_selCl.options).some(function(o){ return o.value === _ramoActual; });
    _selCl.value = _ok ? _ramoActual : '';
  }
  _trResetPagina();   // cambiar de ramo siempre vuelve a la página 1
  cargarMonitor();
}

// Sincronizar estado UI inicial al cargar (porque la vista/ramo vienen de
// localStorage o de la URL).
document.addEventListener('DOMContentLoaded', function(){
  document.querySelectorAll('.tr-segment-btn, .btn-vista').forEach(function(b){
    b.classList.toggle('active', b.dataset.vista === _vistaActual);
  });
  document.querySelectorAll('.tr-tab').forEach(function(b){
    b.classList.toggle('active', (b.dataset.clasif || '') === _ramoActual);
  });
});

// ════════════════════════════════════════════════════════════════
//  VISTA KANBAN (aditiva) — toggle Tabla/Kanban + render + drag&drop
//  La data es la MISMA que carga cargarMonitor() (_monitorData /
//  window._monitorCompromisos): NO se hace una segunda llamada al
//  backend. El PUT /transporte/api/compromisos/<id> cambia el estado;
//  el CSRF lo inyecta el wrapper global de fetch de base.html.
// ════════════════════════════════════════════════════════════════

// Modo de vista persistido (clave tr_monitor_viewmode). Default: tabla.
var _monitorViewMode = (function(){
  try { return localStorage.getItem('tr_monitor_viewmode') || 'tabla'; }
  catch(e){ return 'tabla'; }
})();

// Map estado del compromiso → columna Kanban (3 buckets). Fallback legacy
// usado solo si el compromiso no trae el campo de gestión del backend.
function _kanbanBucket(estado) {
  switch ((estado || '').trim()) {
    case 'Despachado':
    case 'Despachado parcial':
    case 'En ruta':
      return 'encamino';
    case 'Entregado':
      return 'entregados';
    default:
      // Pendiente, vacío, En proceso, Problema, Prioridad y cualquier otro → Pendientes
      return 'pendientes';
  }
}

// Bucket Kanban a partir del NUEVO modelo de gestión que entrega el backend:
//   gestion = pendiente | en_gestion | entregado
// (pendiente→Pendientes, en_gestion→En gestión [col "encamino"], entregado→Entregados)
// Si el compromiso no trae 'gestion' (fila vieja sin re-sync), derivamos un
// equivalente con en_manifiesto / cobertura / estado para no perder la tarjeta.
function _gestionBucket(c) {
  var g = (c && c.gestion ? String(c.gestion) : '').trim();
  if (g === 'pendiente')  return 'pendientes';
  if (g === 'en_gestion') return 'encamino';
  if (g === 'entregado')  return 'entregados';
  // Fallback derivado
  if (c && c.en_manifiesto) return 'encamino';
  if (c && (c.cobertura_pct || 0) >= 100) return 'entregados';
  if (c && c.tiene_saldo === 0) return 'entregados';
  return _kanbanBucket(c ? c.estado : '');
}

// Etiqueta humana de la gestión (para tooltips/badges).
function _gestionLabel(g) {
  return ({ pendientes:'Pendiente', encamino:'En gestión', entregados:'Entregado' }[g] || 'Pendiente');
}

// Días de atraso → HTML del mini-indicador (ámbar >3, rojo >7). Devuelve ''
// si no aplica (0 o sin dato). Usa el campo dias_atraso del backend.
function _diasAtrasoHtml(c) {
  var d = (c && c.dias_atraso != null) ? parseInt(c.dias_atraso, 10) : 0;
  if (!d || d <= 0 || isNaN(d)) return '';
  var color = d > 7 ? '#dc2626' : (d > 3 ? '#f59e0b' : '#94a3b8');
  var bg    = d > 7 ? '#fee2e2' : (d > 3 ? '#fff8e1' : '#f1f5f9');
  return '<span class="tr-atraso-chip" style="color:' + color + ';background:' + bg + '" ' +
    'title="' + d + ' día(s) desde la emisión sin gestión">' +
    '<i class="bi bi-clock-history"></i>' + d + ' d</span>';
}

// Badge PREVENTA (sin stock suficiente → preventa). Usa el campo preventa (0/1).
function _preventaBadge(c) {
  if (!c || !c.preventa) return '';
  return '<span class="tr-preventa-badge" title="Sin stock suficiente — preventa">' +
    '<i class="bi bi-hourglass-split"></i>PREVENTA</span>';
}

// Badge "En manifiesto / En gestión" (cuando el doc ya está en un manifiesto).
function _enManifiestoBadge(c) {
  if (!c || !c.en_manifiesto) return '';
  return '<span class="tr-manif-badge" title="Incluido en un manifiesto — en preparación">' +
    '<i class="bi bi-clipboard-check"></i>En manifiesto</span>';
}

// Estado representativo que se persiste al soltar en cada columna.
var _KANBAN_ESTADO = {
  pendientes: 'Pendiente',
  encamino:   'Despachado',
  entregados: 'Entregado',
};

// Alterna Tabla ↔ Kanban (y guarda la preferencia).
function setMonitorViewMode(mode) {
  _monitorViewMode = (mode === 'kanban') ? 'kanban' : 'tabla';
  try { localStorage.setItem('tr_monitor_viewmode', _monitorViewMode); } catch(e){}

  // Marcar el botón activo del toggle
  document.querySelectorAll('.tr-viewmode-btn').forEach(function(b){
    b.classList.toggle('active', b.dataset.viewmode === _monitorViewMode);
  });

  var esKanban = _monitorViewMode === 'kanban';
  // Tabla desktop + tarjetas mobile = la vista "Tabla". El Kanban es extra.
  var tabla = document.querySelector('.tr-table-card');
  var cards = document.getElementById('monitorCards');
  var kwrap = document.getElementById('kanbanWrap');
  // La tabla está en .d-none.d-md-block y las cards en .d-md-none: para no
  // pelear con esas clases responsive de Bootstrap, alternamos visibilidad
  // con un atributo data + display directo solo cuando ocultamos por Kanban.
  if (tabla) tabla.style.display = esKanban ? 'none' : '';
  if (cards) cards.style.display = esKanban ? 'none' : '';
  if (kwrap) kwrap.style.display = esKanban ? '' : 'none';

  if (esKanban) renderKanban();
}

// Construye el HTML de UNA tarjeta Kanban desde un compromiso.
function _kanbanCardHtml(c) {
  var pillClass = ({
    warning:'tr-estado-warning', primary:'tr-estado-primary',
    success:'tr-estado-success', danger:'tr-estado-danger',
    info:'tr-estado-info', secondary:'tr-estado-secondary',
  }[ESTADO_COLORS[c.estado_logistico || c.estado]] || 'tr-estado-secondary');

  // Costo del ENVÍO (fallback a costo_zz para filas viejas)
  var _costoVal = c.costo_envio || c.costo_zz;
  var costo = _costoVal
    ? '$' + Math.round(_costoVal).toLocaleString('es-CL')
    : null;

  // Acento lateral por flujo (igual que tabla/cards)
  var rowFlow = 'tr-row-pendiente';
  if (c.cobertura_pct >= 100) rowFlow = 'tr-row-entregado';
  else if (c.cobertura_pct > 0) rowFlow = 'tr-row-parcial';

  // Badge de clasificación
  var clasifMap = {
    retiro:      { cls:'tr-tipo-retiro',      icon:'bi-arrow-return-left', label:'Retiro' },
    instalacion: { cls:'tr-tipo-instalacion', icon:'bi-tools',             label:'Instalación' },
    mantencion:  { cls:'tr-tipo-mantencion',  icon:'bi-wrench-adjustable', label:'Mantención' },
    garantia:    { cls:'tr-tipo-garantia',    icon:'bi-shield-check',      label:'Garantía' },
    despacho:    { cls:'tr-tipo-despacho',    icon:'bi-box-arrow-right',   label:'Despacho' },
  };
  // PR-B (2026-08-01): `c.ramo` es el ramo de ESTA fila puntual (una
  // factura con despacho+instalación ahora puede llegar como 2 filas, cada
  // una con su propio ramo). Preferirlo sobre `c.clasificacion` (el valor
  // único por DOCUMENTO, que sigue existiendo) -- para documentos de un
  // solo ramo son el mismo valor, así que no cambia nada visible ahí.
  var tipo = clasifMap[c.ramo || c.clasificacion] || clasifMap.despacho;

  // Badge "En manifiesto" (reusa el helper compartido — coherente con tabla/cards).
  var manifBadge = _enManifiestoBadge(c);
  // Badge PREVENTA + mini-indicador de días de atraso (modelo de gestión nuevo).
  var preventaBadge = _preventaBadge(c);
  var atrasoHtml    = _diasAtrasoHtml(c);
  // Fecha de agenda (retiros): se muestra como pista de la acción "agendar".
  var agendaHtml = ((c.ramo || c.clasificacion) === 'retiro' && c.fecha_agenda)
    ? '<span class="tr-kcard-agenda" title="Retiro agendado"><i class="bi bi-calendar-check"></i> ' + esc(c.fecha_agenda) + '</span>'
    : '';

  return '<div class="tr-kcard ' + rowFlow + '" draggable="true" ' +
            'data-id="' + c.id + '" data-estado="' + attr(c.estado||'') + '" ' +
            'data-group="' + _gestionBucket(c) + '" ' +
            'data-en-manifiesto="' + (c.en_manifiesto ? '1' : '0') + '" ' +
            'tabindex="0" role="button" ' +
            'title="Ver detalle de ' + attr((c.tido||'') + ' ' + trNudo(c.nudo||'')) + '">' +
    '<div class="tr-kcard-top">' +
      '<div class="tr-kcard-doc">' +
        '<span class="tr-doc-tido">' + esc(c.tido||'') + '</span>' +
        '<span class="tr-doc-num">' + esc(trNudo(c.nudo||'')) + '</span>' +
      '</div>' +
      '<span class="tr-estado-pill ' + pillClass + '">' + esc((c.estado_logistico||c.estado)||'—') + '</span>' +
    '</div>' +
    '<div class="tr-kcard-cliente">' + esc(c.cliente||'—') + '</div>' +
    '<div class="tr-kcard-meta">' +
      (c.comuna ? '<span><i class="bi bi-geo-alt"></i> ' + esc(c.comuna) + '</span>' : '') +
      (agendaHtml || '') +
      (costo ? '<span class="tr-kcard-costo">' + costo + '</span>' : '') +
    '</div>' +
    '<div class="tr-kcard-badges">' +
      '<span class="tr-tipo-badge ' + tipo.cls + '"><i class="bi ' + tipo.icon + '"></i>' + tipo.label + '</span>' +
      preventaBadge +
      manifBadge +
      atrasoHtml +
    '</div>' +
  '</div>';
}

// 2026-07-28: estado vacío del Kanban migrado a .trx-empty (icono + título +
// explicación breve de qué significa la columna), reemplazando el viejo
// "Sin documentos" pelado. El MISMO markup vive hardcodeado en
// templates/transporte/index.html (primer paint, antes de que corra JS);
// esta función es la que reconstruye la columna cuando queda en 0 tras un
// reload o un drag&drop — si no coincidieran, el estado lindo se vería un
// instante y luego "revertiría" al viejo en cuanto el usuario interactuara.
var _K_EMPTY_CONF = {
  pendientes: { t:'Sin pendientes',   d:'Documentos con saldo y sin manifiesto aparecerán aquí.' },
  encamino:   { t:'Nada en gestión',  d:'Los documentos agregados a un manifiesto aparecerán aquí.' },
  entregados: { t:'Sin entregados',   d:'Aún no hay documentos completamente despachados.' },
};
function _kanbanEmptyHtml(g) {
  var conf = _K_EMPTY_CONF[g] || { t:'Sin documentos', d:'' };
  return '<div class="tr-kempty js-kempty">' +
    '<div class="trx-empty" style="padding:18px 8px">' +
      '<div class="trx-empty-ico" style="width:40px;height:40px;font-size:1.05rem;margin-bottom:8px"><i class="bi bi-inbox"></i></div>' +
      '<div class="trx-empty-t" style="font-size:.85rem;margin-bottom:3px">' + esc(conf.t) + '</div>' +
      '<div class="trx-empty-d" style="font-size:.74rem;margin:0">' + esc(conf.d) + '</div>' +
    '</div>' +
  '</div>';
}

// Renderiza las 3 columnas del Kanban desde _monitorData.
function renderKanban() {
  var board = document.getElementById('kanbanBoard');
  if (!board) return;
  var data = _monitorData || [];

  // Repartir por bucket de GESTIÓN (pendiente/en_gestion/entregado).
  var grupos = { pendientes: [], encamino: [], entregados: [] };
  data.forEach(function(c){
    var g = _gestionBucket(c);
    (grupos[g] || grupos.pendientes).push(c);
  });

  ['pendientes','encamino','entregados'].forEach(function(g){
    var body  = board.querySelector('.js-kbody[data-group="' + g + '"]');
    var count = board.querySelector('.js-kcount[data-group="' + g + '"]');
    if (count) count.textContent = grupos[g].length;
    if (!body) return;
    if (!grupos[g].length) {
      body.innerHTML = _kanbanEmptyHtml(g);
    } else {
      body.innerHTML = grupos[g].map(_kanbanCardHtml).join('');
    }
  });
}

// ── Drag & drop del Kanban (delegación en #kanbanBoard) ──────────────
(function(){
  var board = document.getElementById('kanbanBoard');
  if (!board) return;

  var dragCard = null;
  var dragging = false;

  function colBody(col){ return col.querySelector('.tr-kcol-body'); }

  function ajustarColumna(col){
    if (!col) return;
    var body  = colBody(col);
    var g     = col.dataset.group;
    var count = board.querySelector('.js-kcount[data-group="' + g + '"]');
    var n     = body ? body.querySelectorAll('.tr-kcard').length : 0;
    if (count) count.textContent = n;
    var ph = body ? body.querySelector('.js-kempty') : null;
    if (body && !ph && n === 0) {
      body.innerHTML = _kanbanEmptyHtml(g);
    } else if (ph) {
      ph.style.display = n > 0 ? 'none' : '';
    }
  }

  function moverDom(card, colDestino, antesDe){
    var colOrigen = card.closest('.tr-kcol');
    var body = colBody(colDestino);
    // Asegurar placeholder presente para insertar antes de él si hace falta
    var ph = body.querySelector('.js-kempty');
    var ref = (antesDe && antesDe.parentNode === body) ? antesDe : ph;
    body.insertBefore(card, ref || null);
    if (ph) ph.style.display = 'none';
    if (colOrigen !== colDestino){
      ajustarColumna(colOrigen);
      ajustarColumna(colDestino);
    }
  }

  // Movimiento optimista + PUT + revert si falla.
  async function moverCard(card, colDestino){
    var grupoDestino = colDestino.dataset.group;
    var nuevoEstado  = _KANBAN_ESTADO[grupoDestino];
    var grupoActual  = card.closest('.tr-kcol').dataset.group;
    if (!nuevoEstado || grupoActual === grupoDestino) return;

    var doc = card.querySelector('.tr-kcard-doc');
    var docTxt = doc ? doc.textContent.trim() : ('#' + card.dataset.id);

    // Snapshot para revertir
    var snap = {
      col:  card.closest('.tr-kcol'),
      next: card.nextElementSibling,
      estado: card.dataset.estado,
    };

    // Optimista: mover card + actualizar estado/acento/pill
    moverDom(card, colDestino);
    card.dataset.estado = nuevoEstado;
    card.dataset.group  = grupoDestino;
    card.classList.remove('tr-row-pendiente','tr-row-parcial','tr-row-entregado');
    card.classList.add(grupoDestino === 'entregados' ? 'tr-row-entregado'
                     : grupoDestino === 'encamino'   ? 'tr-row-parcial'
                     : 'tr-row-pendiente');
    var pill = card.querySelector('.tr-estado-pill');
    if (pill){
      var pc = ({
        warning:'tr-estado-warning', primary:'tr-estado-primary',
        success:'tr-estado-success', danger:'tr-estado-danger',
        info:'tr-estado-info', secondary:'tr-estado-secondary',
      }[ESTADO_COLORS[nuevoEstado]] || 'tr-estado-secondary');
      pill.className = 'tr-estado-pill ' + pc;
      pill.textContent = nuevoEstado;
    }

    try {
      var r = await fetch('/transporte/api/compromisos/' + card.dataset.id, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ estado: nuevoEstado }),
        credentials: 'same-origin',
      });
      var d = {};
      try { d = await r.json(); } catch(_){}
      // 2026-08-01 (SEV-5): antes se descartaba d.error y se mostraba
      // siempre el mismo "No se pudo actualizar" -- con el candado nuevo
      // de tr_update_compromiso (documento en manifiesto activo), el
      // usuario necesita el motivo real, no un mensaje genérico.
      if (!r.ok || !d.ok) throw new Error(d.error || ('HTTP ' + r.status));

      // Sincronizar la cache local para que un re-render no revierta el cambio.
      // Actualizamos también 'gestion' (que es lo que ahora usa _gestionBucket)
      // según la columna destino, para que renderKanban no devuelva la tarjeta.
      var _GESTION_BY_GRUPO = { pendientes:'pendiente', encamino:'en_gestion', entregados:'entregado' };
      var item = (_monitorData || []).find(function(x){ return String(x.id) === String(card.dataset.id); });
      if (item) {
        item.estado  = nuevoEstado;
        item.gestion = _GESTION_BY_GRUPO[grupoDestino] || item.gestion;
      }

      ilusToast('✓ Estado actualizado', { type: 'success' });
    } catch (err){
      // Revertir el movimiento optimista
      moverDom(card, snap.col, snap.next);
      card.dataset.estado = snap.estado;
      card.dataset.group  = snap.col.dataset.group;
      card.classList.remove('tr-row-pendiente','tr-row-parcial','tr-row-entregado');
      card.classList.add(snap.col.dataset.group === 'entregados' ? 'tr-row-entregado'
                       : snap.col.dataset.group === 'encamino'   ? 'tr-row-parcial'
                       : 'tr-row-pendiente');
      if (pill){
        var pc2 = ({
          warning:'tr-estado-warning', primary:'tr-estado-primary',
          success:'tr-estado-success', danger:'tr-estado-danger',
          info:'tr-estado-info', secondary:'tr-estado-secondary',
        }[ESTADO_COLORS[snap.estado]] || 'tr-estado-secondary');
        pill.className = 'tr-estado-pill ' + pc2;
        pill.textContent = snap.estado || '—';
      }
      ilusToast(err.message || 'No se pudo actualizar', { type: 'error' });
      console.error('[kanban monitor]', err);
    }
  }

  board.addEventListener('dragstart', function(e){
    var card = e.target.closest('.tr-kcard');
    if (!card) return;
    // FIX 2026-07-28 (Daniel, hallazgo H12: "el Kanban promete sacar de
    // gestión y no lo hace"): arrastrar una tarjeta que ya está en un
    // manifiesto solo cambiaba estado_entrega vía PUT /compromisos/<id> --
    // el documento seguía en el manifiesto, así que en el próximo refresh
    // volvía solo a "En gestión", confundiendo al operador. Se bloquea el
    // arrastre hacia afuera (misma decisión ya tomada para la tabla, ver
    // initDragRows / tr.dataset.enManifiesto más abajo en este archivo).
    if (card.dataset.enManifiesto === '1') {
      e.preventDefault();
      ilusToast('Este documento ya está en un manifiesto — quítalo desde la ficha del manifiesto para poder recolocarlo.', { type: 'warning' });
      return;
    }
    dragCard = card; dragging = true;
    card.classList.add('is-dragging');
    e.dataTransfer.effectAllowed = 'move';
    try { e.dataTransfer.setData('text/plain', card.dataset.id); } catch(_){}
  });

  board.addEventListener('dragend', function(){
    if (dragCard) dragCard.classList.remove('is-dragging');
    dragCard = null;
    board.querySelectorAll('.drop-ok').forEach(function(c){ c.classList.remove('drop-ok'); });
    setTimeout(function(){ dragging = false; }, 60);
  });

  board.addEventListener('dragover', function(e){
    if (!dragCard) return;
    var col = e.target.closest('.tr-kcol');
    board.querySelectorAll('.drop-ok').forEach(function(c){ if (c !== col) c.classList.remove('drop-ok'); });
    if (!col) return;
    e.preventDefault();
    e.dataTransfer.dropEffect = 'move';
    // Solo resaltar si es una columna distinta a la de origen
    if (col.dataset.group !== dragCard.closest('.tr-kcol').dataset.group) {
      col.classList.add('drop-ok');
    }
  });

  board.addEventListener('dragleave', function(e){
    var col = e.target.closest('.tr-kcol');
    if (col && !col.contains(e.relatedTarget)) col.classList.remove('drop-ok');
  });

  board.addEventListener('drop', function(e){
    var col = e.target.closest('.tr-kcol');
    if (!col || !dragCard) return;
    e.preventDefault();
    col.classList.remove('drop-ok');
    var card = dragCard;
    moverCard(card, col);
  });

  // Click simple (sin drag) abre el detalle del documento.
  board.addEventListener('click', function(e){
    var card = e.target.closest('.tr-kcard');
    if (!card || dragging) return;
    if (typeof openVista === 'function') openVista(parseInt(card.dataset.id, 10));
  });

  // Accesibilidad: Enter abre el detalle.
  board.addEventListener('keydown', function(e){
    if (e.key !== 'Enter') return;
    var card = e.target.closest('.tr-kcard');
    if (card && card === e.target && typeof openVista === 'function') {
      openVista(parseInt(card.dataset.id, 10));
    }
  });
})();

// Aplicar el modo de vista persistido al cargar (sin re-marcar pestañas Vista).
document.addEventListener('DOMContentLoaded', function(){
  setMonitorViewMode(_monitorViewMode);
});

/* ── ALERTAS OPERATIVAS (2026-08-01) ─────────────────────────────────────
   Avisa de cosas trabadas que nadie va a destrabar solo. Hoy: envíos
   multi-bulto con una parte entregada y el resto sin avanzar — desde que el
   poller exige que lleguen TODOS los bultos para marcar Entregado, esos
   despachos ya no se cierran solos.
   Se mantiene DELIBERADAMENTE fuera de cargarMonitor(): si el endpoint de
   alertas falla o tarda, la grilla tiene que cargar igual. */
function renderAlertas() {
  var cont = document.getElementById('trAlertas');
  if (!cont) return;

  fetch('/transporte/api/alertas')
    .then(function(r){ return r.json(); })
    .then(function(d) {
      if (!d || !d.ok || !d.alertas || !d.alertas.length) {
        cont.style.display = 'none';
        cont.innerHTML = '';
        return;
      }
      cont.innerHTML = d.alertas.map(function(a) {
        // Hasta 5 en el detalle; el resto se cuenta, no se lista (una alerta
        // de 40 líneas deja de leerse).
        var visibles = (a.items || []).slice(0, 5);
        var resto    = (a.items || []).length - visibles.length;

        var filas = visibles.map(function(it) {
          var link = it.manifiesto_id
            ? '/transporte/manifiestos/' + encodeURIComponent(it.manifiesto_id)
            : null;
          var doc = link
            ? '<a href="' + attr(link) + '" class="tr-alerta-doc">' + esc(it.documento) + '</a>'
            : '<span class="tr-alerta-doc">' + esc(it.documento) + '</span>';
          return '<li>' + doc +
                 ' · <strong>' + esc(it.resumen) + '</strong>' +
                 ' · ' + esc(it.cliente) +
                 // FIX 2026-08-02 (Daniel: "¿cuándo te dije yo que el ERP
                 // CHECK? Ese es el WMS. Random y Check son dos sistemas
                 // diferentes: Random es ERP, Check es WMS"). Este campo es
                 // KOTRPCVH de la guía -- NO es el courier del despacho, y
                 // mostrarlo suelto al final de la línea lo hacía parecer uno.
                 // Se etiqueta explícitamente como el sistema de origen.
                 (it.wms && it.wms !== '—' ? ' · <span class="text-muted">WMS ' + esc(it.wms) + '</span>' : '') +
                 '</li>';
        }).join('');

        return '<div class="tr-alerta tr-alerta-' + attr(a.severidad || 'warning') + '">' +
                 '<div class="tr-alerta-head">' +
                   '<i class="bi bi-exclamation-triangle-fill"></i>' +
                   '<strong>' + esc(a.titulo) + '</strong>' +
                 '</div>' +
                 '<div class="tr-alerta-sub">' + esc(a.detalle || '') + '</div>' +
                 '<ul class="tr-alerta-list">' + filas +
                   (resto > 0
                     ? '<li class="tr-alerta-mas">y ' + resto + ' más</li>'
                     : '') +
                 '</ul>' +
               '</div>';
      }).join('');
      cont.style.display = '';
    })
    .catch(function(e) {
      // Silencioso a propósito: una alerta que no carga no puede romper el
      // Monitor ni llenar la pantalla de errores.
      console.warn('[alertas] no se pudieron cargar:', e);
      cont.style.display = 'none';
    });
}

// Toggle "solo Problema" (2026-08-01) -- independiente del segmento de
// gestión (pendiente/en_gestion/entregado son mutuamente excluyentes,
// Problema no lo es: un ítem "en_gestion" puede tener un problema
// reportado). Reutiliza el filtro `estado` que ya soporta el backend
// (aplicado sobre estado_logistico, ver tr_compromisos_json) -- si el
// operador tenía otro estado elegido en el <select> de arriba, este
// toggle lo reemplaza mientras esté activo.
var _soloProblemaActivo = false;
function toggleSoloProblema(btn){
  _soloProblemaActivo = !_soloProblemaActivo;
  if (btn) btn.classList.toggle('active', _soloProblemaActivo);
  _trResetPagina();   // cambiar de filtro siempre vuelve a la página 1
  cargarMonitor();
}

// ════════════════════════════════════════════════════════════════════════
//  PAGINACIÓN DEL MONITOR (REGLA #4.3 — Daniel 2026-08-01: "hagámosla igual
//  que las etiquetas... contenida en la página, no necesitamos darle scroll")
//
//  Mismo pie de tabla que Etiquetas (templates/_partials/pagination.html):
//  "Mostrando A–B de N" · selector "N por pagina" · Anterior / Pagina X de Y /
//  Siguiente. El backend (tr_compromisos_json) pagina sobre la lista YA
//  filtrada por estado_logistico y YA expandida por ramo, así que estos
//  números son FILAS reales de la grilla, no documentos del SQL.
// ════════════════════════════════════════════════════════════════════════
var _TR_PER_PAGE_OPTS = [50, 100, 200, 500];
var _pagActual = 1;
var _perPage = (function(){
  try {
    var v = parseInt(localStorage.getItem('tr_per_page'), 10);
    return (_TR_PER_PAGE_OPTS.indexOf(v) >= 0) ? v : 100;
  } catch(e) { return 100; }
})();
var _pagTotalPaginas = 1;
var _pagTotalFilas   = 0;

// Vuelve a la página 1. OBLIGATORIO al cambiar CUALQUIER filtro: si no, el
// operador que estaba en la página 7 y filtra algo que ahora tiene 2 páginas
// se queda mirando una tabla vacía sin entender por qué.
function _trResetPagina() { _pagActual = 1; }

function trPaginaAnterior() {
  if (_pagActual <= 1) return;
  _pagActual--;
  cargarMonitor();
}

function trPaginaSiguiente() {
  if (_pagActual >= _pagTotalPaginas) return;
  _pagActual++;
  cargarMonitor();
}

function trCambiarPerPage(v) {
  var n = parseInt(v, 10);
  _perPage = (_TR_PER_PAGE_OPTS.indexOf(n) >= 0) ? n : 100;
  try { localStorage.setItem('tr_per_page', String(_perPage)); } catch(e){}
  _trResetPagina();   // cambiar el tamaño de página siempre vuelve a la 1
  cargarMonitor();
}

// Pinta el pie de tabla en los dos contenedores (desktop y mobile). Se llama
// SIEMPRE tras cargar, incluso con 0 filas: el selector de tamaño tiene que
// seguir accesible para poder volver a un tamaño manejable.
function _trRenderPaginador() {
  var desde = _pagTotalFilas ? ((_pagActual - 1) * _perPage) + 1 : 0;
  var hasta = Math.min(_pagActual * _perPage, _pagTotalFilas);
  var opts = _TR_PER_PAGE_OPTS.map(function(n){
    return '<option value="' + n + '"' + (n === _perPage ? ' selected' : '') +
           '>' + n + ' por pagina</option>';
  }).join('');
  var html =
    '<div class="tr-pag">' +
      '<div class="tr-pag-info">' +
        (_pagTotalFilas
          ? 'Mostrando <strong>' + desde + '–' + hasta + '</strong> de <strong>' + _pagTotalFilas + '</strong>'
          : 'Sin resultados') +
      '</div>' +
      '<div class="tr-pag-ctrls">' +
        '<select class="tr-pag-size" title="Filas por pagina" ' +
                'onchange="trCambiarPerPage(this.value)">' + opts + '</select>' +
        '<button type="button" class="tr-pag-btn" onclick="trPaginaAnterior()"' +
          (_pagActual <= 1 ? ' disabled' : '') + '>' +
          '<i class="bi bi-chevron-left"></i><span>Anterior</span></button>' +
        '<span class="tr-pag-cur">Pagina ' + _pagActual + ' de ' + _pagTotalPaginas + '</span>' +
        '<button type="button" class="tr-pag-btn tr-pag-btn-next" onclick="trPaginaSiguiente()"' +
          (_pagActual >= _pagTotalPaginas ? ' disabled' : '') + '>' +
          '<span>Siguiente</span><i class="bi bi-chevron-right"></i></button>' +
      '</div>' +
    '</div>';
  ['trPaginador', 'trPaginadorMobile'].forEach(function(id){
    var el = document.getElementById(id);
    if (el) el.innerHTML = html;
  });
}

// ════════════════════════════════════════════════════════════════════════
//  FILTROS EN VIVO — FIX del bug reportado por Daniel (2026-08-01):
//  "cuando filtras algo y sacas el filtro, no se limpia la tabla".
//
//  CAUSA REAL (encontrada leyendo el código, no supuesta): cargarMonitor()
//  armaba sus parámetros ÚNICAMENTE desde `window.location.search`. La URL
//  solo cambia al hacer SUBMIT del <form method="get"> de filtros, o sea con
//  una recarga completa. TODO el resto del Monitor (pestañas de ramo,
//  segmento de gestión, toggle "solo Problema", recarga post-sync,
//  post-importar, post-agregar-a-manifiesto y ahora el paginador) llama a
//  cargarMonitor() por AJAX SIN tocar la URL. Consecuencia: el operador
//  vaciaba el buscador o ponía un <select> en "Todos" y la URL seguía
//  llevando el filtro viejo, así que la siguiente consulta pedía otra vez
//  EXACTAMENTE el mismo filtro — la tabla se quedaba con el resultado
//  anterior aunque el control en pantalla ya estuviera vacío.
//
//  AGRAVANTE encontrado en el mismo camino: _ramoActual cae a
//  localStorage.tr_ramo_actual cuando la URL no trae ?clasificacion=. El
//  botón "Limpiar filtros" navegaba a /transporte/ SIN query — o sea que
//  limpiaba la URL pero el ramo guardado se volvía a aplicar solo: se
//  limpiaban los controles y la tabla seguía filtrada.
//
//  FIX: la fuente de verdad de los filtros pasa a ser el FORMULARIO VIVO.
//  Un control vacío BORRA su parámetro (ya no se hereda de la URL) y todos
//  los controles re-consultan al cambiar, también al quedar vacíos.
// ════════════════════════════════════════════════════════════════════════
function _trFormFiltros() {
  return document.querySelector('form.tr-filters');
}

// Aplica sobre `params` lo que dicen los controles del formulario AHORA.
// Clave del fix: un control vacío borra su parámetro en vez de dejar pasar
// el valor viejo que venía en la URL.
function _trAplicarFiltrosDelForm(params) {
  var form = _trFormFiltros();
  if (!form) return params;   // sin formulario: comportamiento de antes
  ['q', 'estado', 'clasificacion'].forEach(function(k){
    var el = form.querySelector('[name="' + k + '"]');
    if (!el) return;
    var v = (el.value || '').trim();
    if (v) params.set(k, v);
    else   params.delete(k);
  });
  // Las FECHAS son distintas: en el backend "parámetro ausente" NO es lo mismo
  // que "parámetro vacío" — ausente aplica el default de últimos 30 días,
  // vacío significa sin filtro de fecha (ver tr_compromisos_json). Se manda
  // siempre explícito para que lo que se ve en pantalla (inputs vacíos) sea
  // exactamente lo que hace el backend (sin filtrar por fecha).
  ['fecha_desde', 'fecha_hasta'].forEach(function(k){
    var el = form.querySelector('[name="' + k + '"]');
    if (el) params.set(k, (el.value || '').trim());
  });
  // `periodo` (chips "Último mes / 3 meses / Este año / Todo") ya viene
  // RESUELTO dentro de los inputs de fecha: el backend calcula fecha_desde/
  // fecha_hasta a partir de él y los renderiza en el formulario. Si se dejara
  // en los params, el backend le daría prioridad y las fechas del formulario
  // no harían nada (el operador movería una fecha y no pasaría nada).
  params.delete('periodo');
  return params;
}

// Handlers en vivo. Se disparan SIEMPRE que cambia un control — también
// cuando queda vacío, que era justo el caso roto.
document.addEventListener('DOMContentLoaded', function(){
  var form = _trFormFiltros();
  if (!form) return;

  // El <select> de clasificación comparte parámetro con las pestañas de ramo
  // (ambos son ?clasificacion=): se sincronizan para que los dos ejes digan
  // lo mismo. setRamoMonitor ya resetea la página y recarga.
  var selClasif = form.querySelector('[name="clasificacion"]');
  if (selClasif) {
    var _tieneOpt = [].slice.call(selClasif.options).some(function(o){
      return o.value === _ramoActual;
    });
    if (_tieneOpt) selClasif.value = _ramoActual;
    selClasif.addEventListener('change', function(){
      setRamoMonitor(null, selClasif.value);
    });
  }

  var selEstado = form.querySelector('[name="estado"]');
  if (selEstado) {
    selEstado.addEventListener('change', function(){
      // Si el toggle "solo Problema" estaba activo, elegir un estado a mano lo
      // desactiva: si no, el toggle pisaría la elección (params.set('estado',
      // 'Problema') va después) y parecería que el <select> no hace nada.
      if (_soloProblemaActivo) {
        _soloProblemaActivo = false;
        var bp = document.getElementById('btnSoloProblema');
        if (bp) bp.classList.remove('active');
      }
      _trResetPagina();
      cargarMonitor();
    });
  }

  ['fecha_desde', 'fecha_hasta'].forEach(function(k){
    var el = form.querySelector('[name="' + k + '"]');
    if (el) el.addEventListener('change', function(){
      _trResetPagina();
      cargarMonitor();
    });
  });

  // Buscador: debounce para no disparar una consulta por tecla, pero SIN
  // condicionar a que haya texto — borrar todo tiene que recargar igual (ese
  // es literalmente el bug reportado).
  var inpQ = form.querySelector('[name="q"]');
  if (inpQ) {
    var _tq = null;
    inpQ.addEventListener('input', function(){
      clearTimeout(_tq);
      _tq = setTimeout(function(){ _trResetPagina(); cargarMonitor(); }, 350);
    });
    inpQ.addEventListener('keydown', function(e){
      // Enter ya no necesita recargar la página entera: filtramos en vivo.
      if (e.key === 'Enter') {
        e.preventDefault();
        clearTimeout(_tq);
        _trResetPagina();
        cargarMonitor();
      }
    });
  }

  // "Limpiar filtros" (la X). Sigue siendo un <a href> real (funciona sin JS),
  // pero con JS limpia de verdad: controles + ramo guardado + toggle + URL,
  // y re-consulta sin recargar.
  var btnLimpiar = form.querySelector('.tr-filters-limpiar');
  if (btnLimpiar) {
    btnLimpiar.addEventListener('click', function(e){
      e.preventDefault();
      ['q', 'estado', 'clasificacion', 'fecha_desde', 'fecha_hasta'].forEach(function(k){
        var el = form.querySelector('[name="' + k + '"]');
        if (el) el.value = '';
      });
      _soloProblemaActivo = false;
      var bp = document.getElementById('btnSoloProblema');
      if (bp) bp.classList.remove('active');
      // El ramo guardado en localStorage era el que resucitaba el filtro
      // después de "limpiar" — ver el comentario del bloque de arriba.
      _ramoActual = '';
      try { localStorage.removeItem('tr_ramo_actual'); } catch(e2){}
      document.querySelectorAll('.tr-tab').forEach(function(b){
        b.classList.toggle('active', (b.dataset.clasif || '') === '');
      });
      // Dejar la URL sin los filtros viejos: si no, un F5 los resucita.
      try { history.replaceState(null, '', window.location.pathname); } catch(e3){}
      _trResetPagina();
      cargarMonitor();
    });
  }
});

function cargarMonitor() {
  // Leer parámetros del formulario de filtros + agregar la vista actual
  var params = new URLSearchParams(window.location.search);
  // FIX 2026-08-01 (bug "sacas el filtro y no se limpia la tabla"): los
  // filtros los manda el FORMULARIO VIVO, no la URL — ver el bloque de
  // comentarios de arriba con la causa real.
  _trAplicarFiltrosDelForm(params);
  params.set('vista', _vistaActual);
  // Ramo (2026-08-01): _ramoActual manda por sobre lo que traiga la URL --
  // así un clic en las pestañas Despacho/Instalación/etc. no necesita
  // recargar la página (mismo patrón que _vistaActual con 'vista').
  if (_ramoActual) params.set('clasificacion', _ramoActual);
  else params.delete('clasificacion');
  if (_soloProblemaActivo) {
    params.set('estado', 'Problema');
    // FIX 2026-08-02 (Daniel: "aún sigo viendo y no está en problema"):
    // "Problema" es una COLA de trabajo, no un sub-filtro de la pestaña de
    // estado activa. Cruzarlo con la vista escondía casi todo: un documento
    // con guía real y sin manifiesto queda con saldo 0 -> cae en la partición
    // "entregado", así que estando en la pestaña Pendiente el operador veía 2
    // de 20. Al activar el toggle se ignora la pestaña y se muestra la cola
    // completa; al desactivarlo se vuelve a la vista que tenía.
    params.set('vista', 'todos');
    // Sin filtro de fecha: un problema de hace 2 meses sigue siendo un
    // problema. Mismo criterio que ya usan los badges de conteo.
    params.set('fecha_desde', '');
    params.set('fecha_hasta', '');
  }
  // Paginación (REGLA #4.3). El backend clampa `page` si quedó fuera de rango.
  params.set('page', String(_pagActual));
  params.set('per_page', String(_perPage));
  var url = '/transporte/api/compromisos?' + params.toString();

  fetch(url)
    .then(function(r){ return r.json(); })
    .then(function(d) {
      if (!d.ok) return;
      _monitorData = d.compromisos || [];
      // Exponer la MISMA data al tablero Kanban (sin 2ª llamada al backend).
      // OJO: desde la paginación, el Kanban ve la PÁGINA actual, no las 500
      // filas de antes — es la contrapartida esperada de paginar de verdad
      // (el operador cambia de página y el Kanban lo sigue).
      window._monitorCompromisos = _monitorData;
      // Estado de paginación devuelto por el backend (es la autoridad: puede
      // haber clampeado la página si el filtro dejó menos páginas).
      _pagTotalFilas   = (d.total != null) ? d.total : _monitorData.length;
      _pagActual       = d.page || 1;
      _pagTotalPaginas = d.total_paginas || 1;
      if (d.per_page) _perPage = d.per_page;
      _trRenderPaginador();
      // Contador del pie: total de filas del filtro (no solo las de la página;
      // el desglose "Mostrando A–B" lo da el paginador de al lado).
      var countEl = document.getElementById('monitorCount');
      if (countEl) countEl.textContent = _pagTotalFilas;

      // Actualizar badges de cada tab de vista con los conteos del backend
      if (d.conteos) {
        var setBadge = function(id, n){
          var el = document.getElementById(id);
          if (el) el.textContent = n;
        };
        // Conteos del modelo de gestión. Compat: si el backend aún manda el
        // campo viejo 'parciales' (sin 'en_gestion'), lo usamos como fallback.
        var cEnGestion = (d.conteos.en_gestion != null) ? d.conteos.en_gestion
                       : (d.conteos.parciales  || 0);
        setBadge('vbPendientes', d.conteos.pendientes || 0);
        setBadge('vbPreventa',   d.conteos.preventa   || 0);
        setBadge('vbProblema',   d.conteos.problema   || 0);
        setBadge('vbEnGestion',  cEnGestion);
        setBadge('vbEntregados', d.conteos.entregados || 0);
        setBadge('vbTodos',      d.conteos.total      || 0);
        // KPI cards en hero
        setBadge('kpiPendientes', d.conteos.pendientes || 0);
        setBadge('kpiEnGestion',  cEnGestion);
        setBadge('kpiEntregados', d.conteos.entregados || 0);

        // Atrasados: lo entrega el backend (conteos.atrasados). Fallback al
        // cálculo client-side por si una respuesta vieja no lo trae.
        var atrasados = d.conteos.atrasados;
        if (atrasados == null) {
          atrasados = 0;
          try {
            (_monitorData || []).forEach(function(c){
              var dias = (c.dias_atraso != null) ? parseInt(c.dias_atraso, 10) : 0;
              if ((!dias || isNaN(dias)) && c.fecha) {
                var d2 = new Date(c.fecha);
                if (!isNaN(d2.getTime())) dias = Math.floor((Date.now() - d2.getTime()) / 86400000);
              }
              if (dias > 7 && (c.cobertura_pct||0) < 100) atrasados++;
            });
          } catch(e){}
        }
        setBadge('kpiAtrasados', atrasados || 0);

        // Preventa: pedidos que esperan stock (conteos.preventa). Fallback:
        // contar items con preventa==1 en la data cargada.
        var preventa = d.conteos.preventa;
        if (preventa == null) {
          preventa = 0;
          try { (_monitorData || []).forEach(function(c){ if (c.preventa) preventa++; }); } catch(e){}
        }
        setBadge('kpiPreventa', preventa || 0);

        // Badges de las pestañas de RAMO (2026-08-01) -- vienen del backend
        // (conteos_ramo), NO de contar filas del DOM: así reflejan el total
        // real de cada ramo (incluye documentos multi-ramo en AMBOS conteos,
        // y no se acotan a los 500 resultados que trae la tabla). Reemplaza
        // a _actualizarContadoresTabs(), que contaba desde el DOM y por eso
        // no podía ver más allá de lo ya renderizado.
        var cr = d.conteos_ramo || {};
        setBadge('cntDespacho',    cr.despacho    || 0);
        setBadge('cntRetiro',      cr.retiro      || 0);
        setBadge('cntInstalacion', cr.instalacion || 0);
        setBadge('cntMantencion',  cr.mantencion  || 0);
        setBadge('cntGarantia',    cr.garantia    || 0);
        // "Todos" usa cr.total (mismo criterio SIN fecha que los 5 ramos de
        // arriba) -- NO d.conteos.total, que SÍ respeta el rango de fechas y
        // por eso podía verse más chico que un ramo individual (bug real
        // visto en vivo: Todos=330 con fecha, Despacho=1279 sin fecha).
        setBadge('cntTodos', cr.total || 0);
      }
      // Marcar timestamp de sync para el indicador en el header
      _trLastSync = Date.now();
      _trActualizarSyncStatus();

      // Renderizar tabla desktop
      var tbody = document.getElementById('monitorTbody');
      if (tbody) {
        if (!_monitorData.length) {
          // Empty state con CTA según la vista actual
          var emptyConfig = {
            pendientes: {
              icon:  'bi-check2-circle',
              title: 'Sin pendientes',
              sub:   'No hay documentos pendientes de despacho. ¡Buen trabajo!',
              cta:   null,
            },
            en_gestion: {
              icon:  'bi-clipboard-check',
              title: 'Nada en gestión',
              sub:   'Cuando agregues un documento a un manifiesto aparecerá aquí, en preparación.',
              cta:   null,
            },
            entregados: {
              icon:  'bi-box-seam',
              title: 'Sin entregas completadas',
              sub:   'Los documentos entregados al 100% aparecerán acá una vez confirmados.',
              cta:   null,
            },
            todos: {
              icon:  'bi-cloud-download',
              title: 'Aún no hay datos',
              sub:   'Sincroniza con el ERP para traer los documentos del mes en curso.',
              cta:   '<button class="tr-empty-cta" onclick="sincronizarMesActual()"><i class="bi bi-arrow-clockwise"></i>Sincronizar ahora</button>',
            },
          };
          var conf = emptyConfig[_vistaActual] || emptyConfig.todos;
          tbody.innerHTML =
            '<tr><td colspan="15" style="padding:0">' +
              '<div class="tr-empty">' +
                '<div class="tr-empty-icon"><i class="bi ' + conf.icon + '"></i></div>' +
                '<div class="tr-empty-title">' + conf.title + '</div>' +
                '<div class="tr-empty-sub">' + conf.sub + '</div>' +
                (conf.cta || '') +
              '</div>' +
            '</td></tr>';
        } else {
          tbody.innerHTML = _monitorData.map(function(c) {
            // Map de colores del estado → clase pill
            var pillClass = ({
              warning: 'tr-estado-warning',
              primary: 'tr-estado-primary',
              success: 'tr-estado-success',
              danger:  'tr-estado-danger',
              info:    'tr-estado-info',
              secondary: 'tr-estado-secondary',
            }[ESTADO_COLORS[c.estado_logistico || c.estado]] || 'tr-estado-secondary');

            // Costo del ENVÍO (VANELI de la línea ZZENVIO). Fallback a costo_zz
            // para filas viejas aún no re-sincronizadas con el campo nuevo.
            var _costoVal = c.costo_envio || c.costo_zz;
            var costoHtml = _costoVal
              ? '<span class="tr-costo">$' + Math.round(_costoVal).toLocaleString('es-CL') + '</span>'
              : '<span style="color:var(--tr-text-soft)">—</span>';

            // Tipo (clasificación) badge
            var clasifMap = {
              retiro:      { cls:'tr-tipo-retiro',      icon:'bi-arrow-return-left',  label:'Retiro' },
              instalacion: { cls:'tr-tipo-instalacion', icon:'bi-tools',              label:'Instalación' },
              mantencion:  { cls:'tr-tipo-mantencion',  icon:'bi-wrench-adjustable', label:'Mantención' },
              garantia:    { cls:'tr-tipo-garantia',    icon:'bi-shield-check',       label:'Garantía' },
              despacho:    { cls:'tr-tipo-despacho',    icon:'bi-box-arrow-right',    label:'Despacho' },
            };
            // PR-B (2026-08-01): `c.ramo` (ramo de ESTA fila) sobre
            // `c.clasificacion` (único por documento) -- ver mismo criterio
            // en _kanbanCardHtml. Idéntico resultado para documentos de 1
            // solo ramo.
            var tipo = clasifMap[c.ramo || c.clasificacion] || clasifMap.despacho;
            var clasificHtml = '<span class="tr-tipo-badge ' + tipo.cls + '">' +
              '<i class="bi ' + tipo.icon + '"></i>' + tipo.label + '</span>';
            // Para RETIROS: mostrar la fecha de agenda (acción "agendar") si existe,
            // o una pista "Por agendar" si no. Para despachos no aplica.
            if ((c.ramo || c.clasificacion) === 'retiro') {
              clasificHtml += c.fecha_agenda
                ? '<div class="tr-agenda-chip" title="Retiro agendado"><i class="bi bi-calendar-check"></i> ' + esc(c.fecha_agenda) + '</div>'
                : '<div class="tr-agenda-chip tr-agenda-chip-todo" title="Falta agendar el retiro"><i class="bi bi-calendar-plus"></i> Por agendar</div>';
            }

            // Borde lateral por estado (orientado a flujo)
            var rowFlowClass = '';
            if (c.cobertura_pct >= 100) rowFlowClass = 'tr-row-entregado';
            else if (c.cobertura_pct > 0) rowFlowClass = 'tr-row-parcial';
            else rowFlowClass = 'tr-row-pendiente';

            // Fecha formateada bonito
            var fechaHtml = c.fecha ? trFormatFecha(c.fecha) : '<span style="color:var(--tr-text-soft)">—</span>';

            var lbl    = (c.tido+' '+trNudo(c.nudo)).replace(/"/g,'&quot;');
            var cli    = (c.cliente||'').replace(/"/g,'&quot;');
            var lblEsc = (c.tido+' '+c.nudo).replace(/'/g,"\\'");
            var cliEsc = (c.cliente||'').replace(/'/g,"\\'");
            // PR-B: data-clasif usa el ramo DE ESTA fila (post-manifiesto,
            // 1 fila por ramo real -- ver arriba). data-clasifs (plural) trae
            // TODOS los ramos del documento (2026-08-01, restructuración por
            // ramo): antes de tener manifiesto, un documento con
            // despacho+instalación solo colgaba de UN ramo (el de mayor
            // prioridad) y el otro quedaba invisible en cualquier filtro
            // client-side. El filtrado real por ramo ahora lo hace el
            // backend (?clasificacion=), data-clasifs solo queda como dato
            // auxiliar para estilos/depuración.
            return '<tr class="tr-monitor-row ' + rowFlowClass + '" data-id="' + c.id + '" ' +
              'data-clasif="' + (c.ramo || c.clasificacion || 'despacho') + '" ' +
              'data-clasifs="' + ((c.ramos && c.ramos.length) ? c.ramos.join(',') : (c.ramo || c.clasificacion || 'despacho')) + '" ' +
              'data-en-manifiesto="' + (c.en_manifiesto ? '1' : '0') + '" ' +
              'data-label="' + lbl + '" data-cliente="' + cli + '">' +

              /* ── Handle de drag — deshabilitado si ya está en un manifiesto
                 (2026-07-27, Daniel: "quitarle el manifiesto, porque ya está
                 asignado" — evita arrastrar de nuevo algo que ya tiene gestión) ── */
              (c.en_manifiesto
                ? '<td class="dp-drag-handle" title="Ya está asignado a un manifiesto">' +
                    '<i class="bi bi-check2-circle" style="font-size:1rem; color:var(--tr-text-soft); opacity:.5"></i>' +
                  '</td>'
                : '<td class="dp-drag-handle" title="Arrastrar al manifiesto" ' +
                    'ondragstart="_dragHandleStart(event,' + c.id + ',\'' + lblEsc + '\',\'' + cliEsc + '\')" draggable="true">' +
                  '<i class="bi bi-grip-vertical" style="font-size:1.1rem; color:var(--tr-text-soft); cursor:grab"></i>' +
                  '</td>') +

              /* ── Documento — TIDO + NUDO ── */
              '<td onclick="openVista(' + c.id + ')" style="cursor:pointer">' +
                '<span class="tr-doc-tido">' + (c.tido||'') + '</span>' +
                '<span class="tr-doc-num">' + trNudo(c.nudo||'') + '</span>' +
              '</td>' +

              /* ── Fecha ── */
              '<td onclick="openVista(' + c.id + ')" style="cursor:pointer">' + fechaHtml + '</td>' +

              /* ── Cliente ── */
              '<td onclick="openVista(' + c.id + ')" style="cursor:pointer">' +
                '<div class="tr-cliente-name">' + (c.cliente||'—') + '</div>' +
                (c.rut ? '<div class="tr-cliente-rut">' + c.rut + '</div>' : '') +
              '</td>' +

              /* ── Comuna ── */
              '<td onclick="openVista(' + c.id + ')" style="cursor:pointer; color:var(--tr-text-muted)">' +
                (c.comuna||'<span style="color:var(--tr-text-soft)">—</span>') +
              '</td>' +

              /* ── Dirección (truncada + tooltip) ── */
              (function() {
                var dir = (c.direccion||'').trim();
                if (!dir) return '<td><span class="tr-cell-truncate tr-cell-truncate-soft">—</span></td>';
                return '<td><div class="tr-cell-truncate" title="' + esc(dir) + '">' + esc(dir) + '</div></td>';
              })() +

              /* FIX 2026-07-28 (Daniel, viendo la tabla en vivo): "tengo
                 observaciones que casi no uso, y ZZ no sé qué es, en guía
                 tampoco sé qué es, y cobertura no sé qué es, porque dice
                 cero por ciento estando entregado". Se sacan las columnas
                 ZZ / Observaciones / Guía / Cobertura de esta vista -- ZZ y
                 Cobertura son datos financieros del ERP (líneas de servicio
                 de envío / % ya facturado) que no reflejan si el courier
                 entregó de verdad, y por eso confundían más que ayudaban.
                 <th> correspondientes ya sacados de index.html; colspan del
                 loading bajó de 15 a 11. */
              /* ── Estado (+ badges de gestión: En manifiesto · PREVENTA · atraso) ── */
              '<td>' +
                '<div class="tr-estado-stack">' +
                  /* 2026-08-01 (Daniel: "¿por qué dicen Despachado esas
                     facturas?"): se muestra el estado LOGÍSTICO real — dónde
                     está el despacho de verdad — y no `estado`, que lo escribe
                     el sync desde el saldo de la línea de flete del ERP y por
                     eso decía "Despachado" sin que nada se hubiera movido.
                     El del ERP sigue visible en el tooltip, con su nombre. */
                  '<span class="tr-estado-pill ' + pillClass + '" title="' +
                     attr('Estado en el ERP: ' + (c.estado_erp || c.estado || '—')) + '">' +
                     esc(c.estado_logistico || c.estado || '—') + '</span>' +
                  _enManifiestoBadge(c) +
                  _preventaBadge(c) +
                  _diasAtrasoHtml(c) +
                '</div>' +
              '</td>' +

              /* ── Courier asignado (2026-07-27) — migrado a .trx-courier
                 (2026-07-28): antes era texto plano en un pill; ahora va
                 con iniciales en un cuadrito + nombre, igual que la ficha
                 de couriers.
                 FIX 2026-07-31 (Daniel: "¿qué pasó con los logos... en la
                 lista de los courier de la tabla?") -- esta consulta no
                 trae logo_url, pero para los 3 couriers reales de ILUS
                 (FedEx/Felca/Milling) el logo es siempre el mismo archivo
                 estático, así que se matchea por NOMBRE (mismo criterio
                 que _srCourierLogo en transporte_manifiesto_detalle.js).
                 Cualquier courier no reconocido sigue con el fallback de
                 iniciales -- nunca se inventa un logo que no existe. ── */
              (function() {
                if (!c.courier) {
                  return '<td class="text-center"><span style="color:var(--tr-text-soft)">—</span></td>';
                }
                var _cl = c.courier.toLowerCase();
                var _logo = _cl.indexOf('felca') !== -1 ? '/static/courier_felca.png'
                  : _cl.indexOf('milling') !== -1 ? '/static/courier_milling.png'
                  : _cl.indexOf('fedex') !== -1 ? '/static/courier_fedex.png' : '';
                var _iniHtml = _logo
                  ? '<img class="trx-courier-logo" src="' + _logo + '" alt="' + esc(c.courier) + '">'
                  : '<span class="trx-courier-fallback">' + esc((c.courier.trim().split(/\s+/).slice(0,2)
                      .map(function(p){ return p.charAt(0) || ''; }).join('').toUpperCase()) || '?') + '</span>';
                // FIX 2026-07-29 (Daniel: "quiero identificar en que manifiesto
                // esta"): si el compromiso viene con manifiesto_id, se agrega
                // el correlativo como link directo a esa ficha (stopPropagation
                // para no disparar el onclick de la fila que abre "Ver").
                var _manLink = (c.manifiesto_id)
                  ? '<a class="trx-courier-manifiesto" href="/transporte/manifiestos/' + c.manifiesto_id + '" ' +
                    'onclick="event.stopPropagation()" title="Ir al manifiesto">' +
                      esc(c.manifiesto_correlativo || ('MAN #' + c.manifiesto_id)) +
                    '</a>'
                  : '';
                return '<td class="text-center">' +
                  '<div class="trx-courier" title="Courier asignado en el manifiesto">' +
                    _iniHtml +
                    '<div class="trx-courier-info">' +
                      '<span class="trx-courier-name">' + esc(c.courier) + '</span>' +
                      _manLink +
                    '</div>' +
                  '</div>' +
                '</td>';
              })() +

              /* ── Costo ── */
              '<td class="text-end">' + costoHtml + '</td>' +

              /* ── Clasificación ── */
              '<td class="text-center">' + clasificHtml + '</td>' +

              /* ── Acciones — botón primario "Ver" + botones secundarios ──
                 FIX 2026-07-28 (Daniel, mirando la demo de mañana: "todavía
                 puedo agregar a manifiesto y todavía puedo editar" un
                 documento que YA figura Entregado). Se bloquean "Editar" y
                 "Agregar al manifiesto" cuando el estado es terminal
                 (Entregado/Devolución) — mismo criterio que ya usa
                 ESTADOS_ENTREGA_TERMINALES en el backend, y mismo patrón
                 visual (disabled + title explicando por qué) que ya usa
                 manifiesto_detalle.html para "en gestión con el courier".
                 "Ver" queda siempre activo: hay que poder revisar un
                 documento entregado. Nota: usa `estado` (terminal), no el
                 bucket `gestion` -- un documento puede estar Entregado y
                 seguir "en gestión" si el manifiesto sigue abierto con otros
                 items pendientes; eso es un tema de saldo parcial que se
                 resuelve en el proyecto de fases/despachos, no acá. ── */
              (function(){
                var _term = (c.estado === 'Entregado' || c.estado === 'Devolución');
                var _lock = _term
                  ? ' disabled title="Bloqueado: este documento ya está entregado"'
                  : '';
                // FIX 2026-07-29 (Daniel: "deberíamos tener un poquito más de
                // acciones" en items bloqueados) — botón "Ir al manifiesto"
                // cuando el compromiso está en uno; queda disponible siempre
                // (incluso en items terminales, a diferencia de Agregar/Editar).
                var _irManifiesto = c.manifiesto_id
                  ? '<button class="tr-action" title="Ir al manifiesto ' + esc(c.manifiesto_correlativo || '') + '" ' +
                    'onclick="location.href=\'/transporte/manifiestos/' + c.manifiesto_id + '\'">' +
                    '<i class="bi bi-box-arrow-up-right"></i></button>'
                  : '';
                return '<td class="text-center tr-actions-cell" onclick="event.stopPropagation()">' +
                '<button class="tr-action tr-action-primary" title="Ver detalle" ' +
                  'onclick="openVista(' + c.id + ')">' +
                  '<i class="bi bi-eye"></i><span>Ver</span></button>' +
                '<button class="tr-action"' + (_term ? _lock : ' title="Agregar al manifiesto" onclick="abrirDragPanelConCid(' + c.id + ',\'' + lblEsc + '\',\'' + cliEsc + '\')"') + '>' +
                  '<i class="bi bi-clipboard-plus"></i></button>' +
                '<button class="tr-action"' + (_term ? _lock : ' title="Editar" onclick="openPanel(' + c.id + ')"') + '>' +
                  '<i class="bi bi-pencil-square"></i></button>' +
                _irManifiesto +
              '</td>';
              })() +
              '</tr>';
          }).join('');
        }
      }

      // Renderizar cards mobile
      var cards = document.getElementById('monitorCards');
      if (cards) {
        if (!_monitorData.length) {
          var emptyConf = ({
            pendientes: { icon:'bi-check2-circle', title:'Sin pendientes', sub:'Buen trabajo, no hay documentos por despachar.' },
            en_gestion: { icon:'bi-clipboard-check', title:'Nada en gestión', sub:'Cuando agregues algo a un manifiesto aparecerá aquí.' },
            entregados: { icon:'bi-box-seam', title:'Sin entregados', sub:'Aún no hay documentos completados.' },
            todos:      { icon:'bi-cloud-download', title:'Aún no hay datos', sub:'Sincroniza con el ERP para empezar.' },
          })[_vistaActual] || { icon:'bi-inbox', title:'Sin compromisos', sub:'' };
          cards.innerHTML =
            '<div class="tr-empty">' +
              '<div class="tr-empty-icon"><i class="bi ' + emptyConf.icon + '"></i></div>' +
              '<div class="tr-empty-title">' + emptyConf.title + '</div>' +
              '<div class="tr-empty-sub">' + emptyConf.sub + '</div>' +
              (_vistaActual === 'todos'
                ? '<button class="tr-empty-cta" onclick="sincronizarMesActual()">' +
                  '<i class="bi bi-arrow-clockwise"></i>Sincronizar ahora</button>'
                : '') +
            '</div>';
        } else {
          cards.innerHTML = _monitorData.map(function(c) {
            var pillClass = ({
              warning:'tr-estado-warning', primary:'tr-estado-primary',
              success:'tr-estado-success', danger:'tr-estado-danger',
              info:'tr-estado-info', secondary:'tr-estado-secondary',
            }[ESTADO_COLORS[c.estado_logistico || c.estado]] || 'tr-estado-secondary');
            var _costoVal = c.costo_envio || c.costo_zz;
            var costo = _costoVal
              ? '$' + Math.round(_costoVal).toLocaleString('es-CL')
              : null;
            var rowFlow = '';
            if (c.cobertura_pct >= 100) rowFlow = 'tr-row-entregado';
            else if (c.cobertura_pct > 0) rowFlow = 'tr-row-parcial';
            else rowFlow = 'tr-row-pendiente';
            var lblEsc = (c.tido+' '+c.nudo).replace(/'/g,"\\u0027");
            var cliEsc = (c.cliente||'').replace(/'/g,"\\u0027");
            // Badges de gestión (mismos helpers que tabla/Kanban) + agenda de retiro.
            var mcardBadges = _enManifiestoBadge(c) + _preventaBadge(c) + _diasAtrasoHtml(c);
            var mcardAgenda = (c.clasificacion === 'retiro')
              ? (c.fecha_agenda
                  ? '<span class="tr-agenda-chip" title="Retiro agendado"><i class="bi bi-calendar-check"></i> ' + esc(c.fecha_agenda) + '</span>'
                  : '<span class="tr-agenda-chip tr-agenda-chip-todo" title="Falta agendar el retiro"><i class="bi bi-calendar-plus"></i> Por agendar</span>')
              : '';
            return '<div class="tr-mcard ' + rowFlow + '" onclick="openVista(' + c.id + ')">' +
              '<div class="tr-mcard-top">' +
                '<div>' +
                  '<span class="tr-doc-tido">' + esc(c.tido) + '</span>' +
                  '<span class="tr-doc-num">' + esc(trNudo(c.nudo)) + '</span>' +
                '</div>' +
                '<span class="tr-estado-pill ' + pillClass + '">' + esc((c.estado_logistico||c.estado)||'') + '</span>' +
              '</div>' +
              '<div class="tr-mcard-cliente">' + esc(c.cliente||'—') + '</div>' +
              '<div class="tr-mcard-meta">' +
                (c.comuna ? '<span><i class="bi bi-geo-alt"></i> ' + esc(c.comuna) + '</span>' : '') +
                (c.fecha ? '<span><i class="bi bi-calendar3"></i> ' + esc(c.fecha) + '</span>' : '') +
                (costo ? '<span class="tr-mcard-costo">' + costo + '</span>' : '') +
              '</div>' +
              ((mcardBadges || mcardAgenda)
                ? '<div class="tr-mcard-badges">' + mcardBadges + mcardAgenda + '</div>'
                : '') +
              '<div class="tr-mcard-actions" onclick="event.stopPropagation()">' +
                '<button class="tr-btn tr-btn-ghost tr-btn-sm" style="flex:1" onclick="openVista(' + c.id + ')">' +
                  '<i class="bi bi-eye"></i><span>Ver detalle</span></button>' +
                /* FIX 2026-07-28: mismo bloqueo de "Agregar al manifiesto"
                   para documentos ya entregados que en la vista de tabla —
                   ver el comentario largo junto al bloque de acciones de la
                   tabla, unas líneas más arriba en este archivo. */
                ((c.estado === 'Entregado' || c.estado === 'Devolución')
                  ? '<button class="tr-btn tr-btn-ghost tr-btn-sm" style="flex:1" disabled ' +
                    'title="Bloqueado: este documento ya está entregado">' +
                    '<i class="bi bi-clipboard-plus"></i><span>Manifiesto</span></button>'
                  : '<button class="tr-btn tr-btn-primary tr-btn-sm" style="flex:1" ' +
                    'onclick="abrirDragPanelConCid(' + c.id + ',\'' + lblEsc + '\',\'' + cliEsc + '\')">' +
                    '<i class="bi bi-clipboard-plus"></i><span>Manifiesto</span></button>') +
                /* FIX 2026-07-29 (Daniel: "un poquito más de acciones") —
                   "Ir al manifiesto" disponible en mobile card, aunque el
                   documento esté bloqueado por estado terminal. */
                (c.manifiesto_id
                  ? '<button class="tr-btn tr-btn-ghost tr-btn-sm" style="flex:1" ' +
                    'onclick="location.href=\'/transporte/manifiestos/' + c.manifiesto_id + '\'">' +
                    '<i class="bi bi-box-arrow-up-right"></i><span>Ir al manifiesto</span></button>'
                  : '') +
              '</div>' +
            '</div>';
          }).join('');
        }
      }
      // Habilitar drag & drop en las filas
      initDragRows();
      // NOTA 2026-08-01: ya NO se re-filtra por _tabClasif ni se recalculan
      // contadores desde el DOM acá -- el ramo ahora es un filtro real del
      // backend (?clasificacion=, ver setRamoMonitor/_ramoActual), así que
      // lo que llega en `d.compromisos` YA es exactamente lo que corresponde
      // mostrar. Re-filtrar client-side con el criterio viejo (comparación
      // exacta contra data-clasif, 1 solo valor) escondía de nuevo filas de
      // documentos multi-ramo que el backend SÍ había incluido correctamente
      // (ver _tr_ramo_where en app.py). Los badges de ramo se pintan arriba
      // desde d.conteos_ramo (server-side, no cuenta filas del DOM).
      // Re-renderizar el tablero Kanban con la MISMA data (vista extra).
      renderKanban();
    })
    .catch(function(e) {
      console.error('cargarMonitor error:', e);
      var tbody = document.getElementById('monitorTbody');
      if (tbody) tbody.innerHTML = '<tr><td colspan="15" class="text-center py-4 text-danger">Error al cargar</td></tr>';
    });
}

function openPanel(id) {
  currentId = id;
  document.getElementById('panelDocId').textContent = '...';
  document.getElementById('panelCliente').textContent = '';
  document.getElementById('panelBody').innerHTML =
    '<div class="text-center py-4"><div class="spinner-border spinner-border-sm text-danger"></div></div>';
  document.getElementById('trPanel').style.right = '0';
  document.getElementById('panelOverlay').style.display = 'block';

  // Cargar datos vía fetch de la fila en la tabla
  var row = document.querySelector('tr[data-id="' + id + '"]') ||
            document.querySelector('[data-id="' + id + '"]');

  Promise.all([
    fetch('/transporte/api/compromisos/' + id + '/lineas').then(r=>r.json()),
    fetch('/transporte/api/compromisos/' + id + '/logs').then(r=>r.json()),
  ]).then(function(results) {
    var lineas = results[0];
    var logs   = results[1];
    renderPanel(id, lineas, logs, row);
  }).catch(function() {
    renderPanel(id, [], [], row);
  });
}

function renderPanel(id, lineas, logs, row) {
  // Obtener datos del commit desde la fila de la tabla (DOM scraping básico)
  var tds = row ? row.querySelectorAll('td') : [];
  var docNum  = tds[0] ? tds[0].textContent.trim() : '—';
  var fecha   = tds[1] ? tds[1].textContent.trim() : '';
  var cliente = tds[2] ? tds[2].querySelector('.fw-semibold')?.textContent.trim() : '';
  var rut     = tds[2] ? tds[2].querySelector('.text-muted')?.textContent.trim() : '';
  var comuna  = tds[3] ? tds[3].textContent.trim() : '';
  var estadoEl= tds[4] ? tds[4].querySelector('.badge') : null;
  var estado  = estadoEl ? estadoEl.textContent.trim() : 'Pendiente';
  var costoDB = tds[5] ? tds[5].textContent.trim().replace(/[$., ]/g,'') || '0' : '0';
  var clasif  = tds[6] ? (tds[6].textContent.includes('Retiro') ? 'retiro' : 'despacho') : 'despacho';

  document.getElementById('panelDocId').textContent  = docNum;
  document.getElementById('panelCliente').textContent = cliente;

  // Opciones estado
  var estadoOpts = ESTADOS.map(function(e) {
    return '<option value="' + e + '"' + (e===estado?' selected':'') + '>' + e + '</option>';
  }).join('');

  // Líneas ZZ
  var lineasHtml = lineas.length ? lineas.map(function(l) {
    return '<div class="d-flex justify-content-between align-items-center py-1 border-bottom" style="font-size:.78rem">' +
      '<div><div class="fw-bold">' + l.koprct + '</div>' +
      '<div class="text-muted">' + (l.nokopr||'') + '</div></div>' +
      '<div class="text-end"><div class="fw-bold" style="color:var(--ilus-red)">Saldo: ' + l.saldo + '</div>' +
      '<div class="text-muted">' + l.cantidad + ' / ' + l.cant_despachada + ' desp.</div></div>' +
      '</div>';
  }).join('') : '<div class="text-muted small">Sin líneas ZZ</div>';

  // Logs
  var logsHtml = logs.slice(0,5).map(function(l) {
    var dt = new Date(l.created_at);
    var fmt = dt.toLocaleString('es-CL',{day:'2-digit',month:'2-digit',year:'2-digit',hour:'2-digit',minute:'2-digit'});
    return '<div class="small py-1 border-bottom"><span class="text-muted">' + fmt + '</span> · ' +
           '<strong>' + (l.usuario||'?') + '</strong> · ' + l.accion +
           (l.detalle ? ' <span class="text-muted">(' + l.detalle + ')</span>' : '') + '</div>';
  }).join('') || '<div class="text-muted small">Sin historial</div>';

  // Manifiestos disponibles para asignar
  var manifestOpts = '<option value="">— Sin manifiesto —</option>' +
    manifiestos.map(function(m) {
      return '<option value="' + m.id + '">' + m.correlativo + ' · ' + m.courier + '</option>';
    }).join('');

  // Campo fecha agenda (solo retiro)
  var retiroBlock = clasif === 'retiro' ? `
    <div class="mb-3">
      <label class="form-label small fw-semibold">Fecha agenda retiro</label>
      <input type="date" class="form-control form-control-sm" id="panelFechaAgenda">
    </div>` : '';

  document.getElementById('panelBody').innerHTML = `
    <div class="mb-3">
      <div class="small text-muted mb-1">${fecha} · ${comuna}</div>
      <div class="small text-muted">${rut}</div>
    </div>

    <div class="mb-3">
      <label class="form-label small fw-semibold">Estado</label>
      <select class="form-select form-select-sm" id="panelEstado">${estadoOpts}</select>
    </div>

    <div class="mb-3">
      <label class="form-label small fw-semibold">Costo ZZ envío <span class="text-muted">(editable)</span></label>
      <div class="input-group input-group-sm">
        <span class="input-group-text">$</span>
        <input type="number" class="form-control" id="panelCostoZZ" value="${parseInt(costoDB)||0}" min="0" step="100">
      </div>
    </div>

    ${retiroBlock}

    <div class="mb-3">
      <label class="form-label small fw-semibold">Agregar a manifiesto</label>
      <select class="form-select form-select-sm" id="panelManifiesto">${manifestOpts}</select>
    </div>

    <div class="mb-3">
      <label class="form-label small fw-semibold">Notas</label>
      <textarea class="form-control form-control-sm" id="panelNotas" rows="2"></textarea>
    </div>

    <button class="btn btn-ilus btn-sm fw-bold w-100 mb-3" onclick="savePanel()">
      <i class="bi bi-floppy me-1"></i>Guardar cambios
    </button>
    <div id="panelMsg" class="small mb-3"></div>

    <div class="mb-3">
      <div class="fw-semibold small mb-2"><i class="bi bi-box me-1"></i>Líneas ZZ</div>
      ${lineasHtml}
    </div>

    <div>
      <div class="fw-semibold small mb-1"><i class="bi bi-clock-history me-1"></i>Historial</div>
      ${logsHtml}
    </div>`;
}

function savePanel() {
  if (!currentId) return;
  var data = {
    estado:   document.getElementById('panelEstado')?.value,
    costo_zz: parseFloat(document.getElementById('panelCostoZZ')?.value) || 0,
    notas:    document.getElementById('panelNotas')?.value || '',
  };
  var fa = document.getElementById('panelFechaAgenda');
  if (fa) data.fecha_agenda = fa.value || null;

  fetch('/transporte/api/compromisos/' + currentId, {
    method:'PUT', headers:{'Content-Type':'application/json'},
    body: JSON.stringify(data)
  })
  .then(function(r){ return r.json(); })
  .then(function(d) {
    var msg = document.getElementById('panelMsg');
    if (d.ok) {
      msg.innerHTML = '<span class="text-success"><i class="bi bi-check-circle me-1"></i>Guardado</span>';
      // Agregar a manifiesto si se seleccionó
      var mid = document.getElementById('panelManifiesto')?.value;
      if (mid) {
        agregarAManifiestoConAviso(mid, currentId, false);
      } else {
        setTimeout(function(){ location.reload(); }, 800);
      }
    } else {
      msg.innerHTML = '<span class="text-danger">' + (d.error||'Error') + '</span>';
    }
  });
}

// 2026-07-26 (pedido Daniel): avisar si el documento ya está en OTRO
// manifiesto antes de agregarlo — "siempre tiene que avisar si hay
// duplicidad, si estamos enviando algo dos veces". No bloquea: si el
// operador confirma, se agrega igual (reenvía con confirm_dup=1).
function agregarAManifiestoConAviso(mid, commitmentId, confirmDup) {
  fetch('/transporte/manifiestos/' + mid + '/items', {
    method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({commitment_id: commitmentId, confirm_dup: !!confirmDup})
  })
  .then(function(r){ return r.json().then(function(d){ return {status:r.status, d:d}; }); })
  .then(function(res){
    if (res.status === 409 && res.d && res.d.error === 'duplicado') {
      var dups = (res.d.duplicados || []).map(function(x){
        return '<div>· <strong>' + (x.correlativo || ('#' + x.manifest_id)) + '</strong> ('+(x.estado||'—')+')</div>';
      }).join('');
      ilusConfirm({
        title: 'Documento ya está en otro manifiesto',
        message: res.d.msg || 'Este documento ya fue agregado a otro manifiesto.',
        sub: dups,
        subHtml: true,
        okLabel: 'Agregar igual', cancelLabel: 'Cancelar',
        danger: true,
      }).then(function(ok){
        if (ok) agregarAManifiestoConAviso(mid, commitmentId, true);
        else setTimeout(function(){ location.reload(); }, 300);
      });
      return;
    }
    if (res.d && res.d.ok === false) {
      // PR-B: preferir `msg` (mensaje legible armado en el backend, ej. el
      // candado de ramos) sobre `error` (código corto tipo "ramo_conflicto").
      ilusToast(res.d.msg || res.d.error || 'No se pudo agregar al manifiesto', {type:'error'});
    }
    setTimeout(function(){ location.reload(); }, 600);
  })
  .catch(function(){ ilusToast('Error de conexión al agregar al manifiesto', {type:'error'}); });
}

function closePanel() {
  document.getElementById('trPanel').style.right = '-420px';
  document.getElementById('panelOverlay').style.display = 'none';
  currentId = null;
}

// ── PESTAÑAS DE RAMO ──────────────────────────────────────────────
// 2026-08-01: el filtrado client-side (filtrarTabMonitor/_tabClasif) y el
// conteo por DOM (_actualizarContadoresTabs) se retiraron -- el ramo ahora
// es un filtro real del backend (ver setRamoMonitor/_ramoActual arriba,
// junto a setVistaMonitor) y los badges vienen de d.conteos_ramo, no de
// contar filas ya renderizadas. Ver el comentario en cargarMonitor().

// ── DRAG & DROP — Asignar a manifiesto ──────────────────────────
var _dragCid   = null;   // commitment_id siendo arrastrado (arrastre activo)
var _dragLabel = '';
var _dpQueue   = [];     // cola de CIDs para el panel
var _dpData    = {};     // {cid: {label, cliente}}
var _manifestosActivos = null;

// Inicia el drag desde el handle dedicado de la fila
function _dragHandleStart(e, cid, label, cliente) {
  e.stopPropagation();   // evita que el dragstart del <tr> se dispare también
  _dragCid = cid;
  e.dataTransfer.effectAllowed = 'copy';
  e.dataTransfer.setData('text/plain', String(cid));
  document.body.classList.add('dp-dragging-active');
  var tr = e.currentTarget.closest('tr');
  if (tr) setTimeout(function(){ tr.classList.add('dp-row-dragging'); }, 0);
  _dpAddToQueue(cid, label, cliente);
}

// ── DRAG ROWS (arrastrabilidad de la fila completa) ────────────────
function initDragRows() {
  document.querySelectorAll('#monitorTbody tr[data-id]').forEach(function(tr) {
    // No permitir re-arrastrar filas ya asignadas a un manifiesto
    // (2026-07-27, Daniel) — el handle propio ya lo bloquea; esto cubre el
    // arrastre desde cualquier punto de la fila completa.
    if (tr.dataset.enManifiesto === '1') {
      tr.removeAttribute('draggable');
      return;
    }
    tr.setAttribute('draggable', 'true');
    tr.style.cursor = 'default';  // el handle propio tiene cursor:grab

    tr.addEventListener('dragstart', function(e) {
      _dragCid = parseInt(tr.dataset.id);
      e.dataTransfer.effectAllowed = 'copy';
      e.dataTransfer.setData('text/plain', String(_dragCid));
      document.body.classList.add('dp-dragging-active');
      setTimeout(function(){ tr.classList.add('dp-row-dragging'); }, 0);
      // Abrir panel y añadir a la cola
      _dpAddToQueue(_dragCid,
        tr.dataset.label || (tr.querySelector('.font-monospace')?.textContent?.trim() || ''),
        tr.dataset.cliente || ''
      );
    });
    tr.addEventListener('dragend', function() {
      tr.classList.remove('dp-row-dragging');
      document.body.classList.remove('dp-dragging-active');
      _dragCid = null;
    });
  });
}

/* Click en botón de la fila → añade a la cola y abre panel */
function abrirDragPanelConCid(cid, docNum, cliente) {
  _dpAddToQueue(cid, docNum, cliente);
}

// ── COLA (carrito de manifiestos) ────────────────────────────────
function _dpAddToQueue(cid, label, cliente) {
  cid = parseInt(cid);
  if (_dpQueue.indexOf(cid) !== -1) { return; } // ya está
  _dpQueue.push(cid);
  _dpData[cid] = { label: label || ('ID ' + cid), cliente: cliente || '' };
  _dpRenderCola();
  // cargar manifiestos si aún no se cargaron
  if (!_manifestosActivos) cargarDragPanel();
  // Auto-abrir el panel: cuando el usuario empieza a armar un manifiesto,
  // el panel pasa a estar visible (Daniel pidió que aparezca solo cuando
  // realmente se está creando un manifiesto, no siempre).
  var dp = document.getElementById('dragPanel');
  if (dp && !dp.classList.contains('dp-open')) openDragPanel();
}

function dpLimpiarCola() {
  _dpQueue = [];
  _dpData  = {};
  _dpRenderCola();
  document.querySelectorAll('#monitorTbody tr.dp-in-queue').forEach(function(tr){
    tr.classList.remove('dp-in-queue');
  });
}

function _dpRenderCola() {
  var list     = document.getElementById('dpQueueList');
  var empty    = document.getElementById('dpQueueEmpty');
  var resumen  = document.getElementById('dpResumen');
  var prevale  = document.getElementById('dpPrevale');
  var btnCrear = document.getElementById('dpBtnCrear');
  var badge    = document.getElementById('dpBadge');

  // Badge contador en cabecera
  if (badge) {
    badge.textContent = _dpQueue.length;
    badge.style.display = _dpQueue.length ? 'inline-block' : 'none';
  }
  // Badge en el botón flotante (si el panel está cerrado) + pulse-attention
  var reopenBadge = document.getElementById('dpReopenBadge');
  var reopenBtn   = document.getElementById('dpReopenBtn');
  if (reopenBadge) {
    if (_dpQueue.length) {
      reopenBadge.style.display = 'inline-flex';
      reopenBadge.textContent = _dpQueue.length;
    } else {
      reopenBadge.style.display = 'none';
    }
  }
  if (reopenBtn) {
    reopenBtn.classList.toggle('tr-has-items', _dpQueue.length > 0);
  }

  if (!_dpQueue.length) {
    // Cola vacía
    if (empty)   { empty.style.display = ''; }
    // Quitar todos los ítems animados
    Array.from(list.querySelectorAll('.dp-queue-item')).forEach(function(el){ el.remove(); });
    if (resumen) resumen.style.display = 'none';
    if (prevale) prevale.style.display = 'none';
    if (btnCrear) btnCrear.disabled = true;
    // Quitar resaltado tabla
    document.querySelectorAll('#monitorTbody tr.dp-in-queue').forEach(function(tr){
      tr.classList.remove('dp-in-queue');
    });
    return;
  }

  if (empty) empty.style.display = 'none';

  // Renderizar ítems (sólo agregar los nuevos, no re-renderizar todos)
  var existingIds = Array.from(list.querySelectorAll('.dp-queue-item[data-cid]'))
                         .map(function(el){ return parseInt(el.dataset.cid); });

  // Agregar ítems nuevos
  _dpQueue.forEach(function(cid) {
    if (existingIds.indexOf(cid) !== -1) return; // ya existe el elemento
    var d = _dpData[cid] || {};
    var el = document.createElement('div');
    el.className = 'dp-queue-item';
    el.dataset.cid = cid;
    el.innerHTML =
      '<div style="min-width:0;flex:1">' +
        '<div class="fw-bold font-monospace" style="font-size:.8rem;color:#fff;' +
             'white-space:nowrap;overflow:hidden;text-overflow:ellipsis">' + esc(d.label) + '</div>' +
        (d.cliente ? '<div style="color:#777;font-size:.7rem;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">' + esc(d.cliente) + '</div>' : '') +
        '<div class="dp-item-peso" style="color:#555;font-size:.68rem;margin-top:1px">—</div>' +
      '</div>' +
      '<button onclick="dpRemoveItem(' + cid + ')" ' +
              'style="background:none;border:none;color:#555;padding:0 0 0 8px;' +
                     'cursor:pointer;font-size:1rem;flex-shrink:0;line-height:1" ' +
              'title="Quitar del manifiesto" onmouseover="this.style.color=\'#f66\'" ' +
              'onmouseout="this.style.color=\'#555\'">×</button>';
    list.appendChild(el);
  });

  // Quitar ítems eliminados de la cola
  Array.from(list.querySelectorAll('.dp-queue-item[data-cid]')).forEach(function(el) {
    var cid = parseInt(el.dataset.cid);
    if (_dpQueue.indexOf(cid) === -1) {
      el.classList.add('dp-removing');
      setTimeout(function(){ if (el.parentNode) el.parentNode.removeChild(el); }, 200);
    }
  });

  if (resumen) resumen.style.display = '';
  if (prevale) prevale.style.display = '';
  if (btnCrear) btnCrear.disabled = false;

  // Resaltar filas en la tabla
  document.querySelectorAll('#monitorTbody tr[data-id]').forEach(function(tr) {
    tr.classList.toggle('dp-in-queue', _dpQueue.indexOf(parseInt(tr.dataset.id)) !== -1);
  });

  // Actualizar acumulado + prevale
  _dpActualizarPrevale();
}

function dpRemoveItem(cid) {
  cid = parseInt(cid);
  var idx = _dpQueue.indexOf(cid);
  if (idx !== -1) _dpQueue.splice(idx, 1);
  delete _dpData[cid];
  _dpRenderCola();
  var tr = document.querySelector('#monitorTbody tr[data-id="' + cid + '"]');
  if (tr) tr.classList.remove('dp-in-queue');
}

// ── PREVALE: acumulado + tarifas ─────────────────────────────────
var _dpPrevaleTimer = null;
async function _dpActualizarPrevale() {
  if (!_dpQueue.length) return;
  // Debounce 300ms para no hacer fetch en cada add rápido
  clearTimeout(_dpPrevaleTimer);
  _dpPrevaleTimer = setTimeout(async function() {
    var spinner = document.getElementById('dpResumenSpinner');
    var prevaleRows = document.getElementById('dpPrevaleRows');
    if (spinner) spinner.style.display = '';
    if (prevaleRows) prevaleRows.innerHTML =
      '<div style="color:#2d5a2d;font-size:.72rem;text-align:center">Calculando…</div>';

    try {
      var r = await fetch('/transporte/api/cola/cotizar', {
        method: 'POST',
        headers: {'Content-Type':'application/json'},
        body: JSON.stringify({commitment_ids: _dpQueue.slice()})
      });
      var d = await r.json();
      if (!d.ok) return;

      // Actualizar métricas acumuladas
      var fmtKgLocal = function(v){ return parseFloat(v||0).toLocaleString('es-CL',{minimumFractionDigits:1,maximumFractionDigits:1})+' kg'; };
      var pEl = document.getElementById('dpPesoTotal');
      var vEl = document.getElementById('dpVolTotal');
      var fEl = document.getElementById('dpPesoFacturable');
      if (pEl) pEl.textContent = fmtKgLocal(d.peso_real);
      if (vEl) vEl.textContent = fmtKgLocal(d.peso_vol);
      if (fEl) fEl.textContent = fmtKgLocal(d.peso_pred);

      // Actualizar pesos en ítems de la cola (si la API devuelve items)
      if (d.items) {
        Object.keys(d.items).forEach(function(cid) {
          var el = document.querySelector('.dp-queue-item[data-cid="'+cid+'"] .dp-item-peso');
          if (el) {
            var it = d.items[cid];
            el.textContent = (it.n || 0) + ' línea' + (it.n !== 1 ? 's' : '');
          }
        });
      }

      // Renderizar filas de prevale (couriers)
      if (!prevaleRows) return;
      if (!d.resultados || !d.resultados.length) {
        prevaleRows.innerHTML = '<div style="color:#2d5a2d;font-size:.72rem">Sin couriers configurados</div>';
        return;
      }
      var fmtCLP = function(v){ return v!=null ? '$'+Math.round(v).toLocaleString('es-CL') : '—'; };
      var html = d.resultados.slice(0,4).map(function(res, i) {
        var isBest = i===0 && res.costo!=null;
        var hasWarn = res.advertencias && res.advertencias.length > 0;
        var warnHtml = hasWarn
          ? '<span title="'+esc(res.advertencias.join(', '))+'" ' +
            'style="color:#f90;font-size:.65rem;margin-left:4px">⚠</span>' : '';
        return '<div class="dp-prevale-row">' +
          '<div style="display:flex;align-items:center;gap:4px;min-width:0">' +
            (isBest ? '<span style="color:#5cb85c;font-size:.7rem">★</span>' : '<span style="font-size:.7rem;color:#2d5a2d">&nbsp;</span>') +
            '<span style="font-size:.74rem;color:'+(isBest?'#7dba7d':'#4a8a4a')+';white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:130px">' +
              esc(res.nombre) +
            '</span>' +
            warnHtml +
          '</div>' +
          '<span style="font-size:.78rem;font-weight:'+(isBest?'800':'600')+';color:'+(isBest?'#5cb85c':'#3a7a3a')+';white-space:nowrap">' +
            (res.costo!=null ? fmtCLP(res.costo) : '<span style="color:#2d5a2d">Sin tarifa</span>') +
          '</span>' +
        '</div>';
      }).join('');
      prevaleRows.innerHTML = html;

    } catch(e) {
      if (prevaleRows) prevaleRows.innerHTML = '<div style="color:#c55;font-size:.72rem">Error al calcular</div>';
    } finally {
      if (spinner) spinner.style.display = 'none';
    }
  }, 320);
}

function _dpAbrirPanel() {
  openDragPanel();
}

function openDragPanel() {
  // Mostrar drawer + overlay oscuro detrás (desktop + mobile).
  // El drawer flota sobre la UI sin empujar el contenido.
  var dp = document.getElementById('dragPanel');
  var ov = document.getElementById('dragOverlay');
  if (dp) {
    dp.classList.add('dp-open');
    dp.setAttribute('aria-hidden', 'false');
  }
  if (ov) {
    ov.classList.add('dp-open');
    ov.setAttribute('aria-hidden', 'false');
  }
  // Bloquear scroll del body para que el contenido no se mueva detrás
  document.body.style.overflow = 'hidden';
  var btn = document.getElementById('dpReopenBtn');
  if (btn) {
    btn.style.display = 'none';
    btn.classList.remove('tr-has-items');
  }
  // Cargar manifiestos solo cuando se abre por primera vez
  if (!_manifestosActivos) cargarDragPanel();
}

function closeDragPanel() {
  var dp = document.getElementById('dragPanel');
  var ov = document.getElementById('dragOverlay');
  if (dp) {
    dp.classList.remove('dp-open');
    dp.setAttribute('aria-hidden', 'true');
  }
  if (ov) {
    ov.classList.remove('dp-open');
    ov.setAttribute('aria-hidden', 'true');
  }
  // Restaurar scroll del body
  document.body.style.overflow = '';
  // Compat: limpiar clase legacy si quedó pegada por código viejo
  document.body.classList.remove('dp-open-desktop');
  _dragCid = null;
  // Mostrar el botón flotante para reabrir. Si hay items en cola, mostrar badge + pulse.
  var btn = document.getElementById('dpReopenBtn');
  if (btn) {
    btn.style.display = 'inline-flex';
    var badge = document.getElementById('dpReopenBadge');
    if (_dpQueue.length) {
      btn.classList.add('tr-has-items');
      if (badge) {
        badge.style.display = 'inline-flex';
        badge.textContent = _dpQueue.length;
      }
    } else {
      btn.classList.remove('tr-has-items');
      if (badge) badge.style.display = 'none';
    }
  }
}

// ESC key cierra el drawer si está abierto
document.addEventListener('keydown', function(e) {
  if (e.key !== 'Escape' && e.keyCode !== 27) return;
  var dp = document.getElementById('dragPanel');
  if (dp && dp.classList.contains('dp-open')) {
    closeDragPanel();
  }
});

// ── CARGAR Y RENDERIZAR MANIFIESTOS ──────────────────────────────
// Timeout máximo 8s + botón Reintentar si falla. Daniel reportó loader
// colgado indefinidamente — esto garantiza que SIEMPRE termina en
// success / empty / error en menos de 8s.
async function cargarDragPanel(force) {
  if (!force && _manifestosActivos) {
    renderDragPanel(_manifestosActivos);
    return;
  }
  var body = document.getElementById('dragPanelBody');
  if (!body) return;
  body.innerHTML =
    '<div class="text-center py-3" style="color:#666">' +
    '<div class="spinner-border spinner-border-sm text-danger mb-2"></div>' +
    '<div style="font-size:.74rem">Cargando manifiestos…</div></div>';

  var ctrl = (typeof AbortController !== 'undefined') ? new AbortController() : null;
  var timeoutId = setTimeout(function() {
    if (ctrl) ctrl.abort();
  }, 8000);

  try {
    const r = await fetch('/transporte/api/manifiestos/activos',
      ctrl ? {signal: ctrl.signal, cache:'no-store'} : {cache:'no-store'});
    clearTimeout(timeoutId);
    if (!r.ok) throw new Error('HTTP ' + r.status);
    _manifestosActivos = await r.json();
    renderDragPanel(_manifestosActivos);
  } catch(err) {
    clearTimeout(timeoutId);
    var isTimeout = (err && (err.name === 'AbortError' || err.code === 20));
    body.innerHTML =
      '<div class="text-center py-3 px-2" style="color:#c66">' +
      '<i class="bi ' + (isTimeout ? 'bi-clock-history' : 'bi-wifi-off') +
      '" style="font-size:1.4rem;display:block;opacity:.55;margin-bottom:6px"></i>' +
      '<div style="font-size:.78rem;margin-bottom:8px">' +
      (isTimeout ? 'El servidor tardó demasiado' : 'No se pudieron cargar') + '</div>' +
      '<button type="button" class="btn btn-sm btn-outline-danger" ' +
      'onclick="cargarDragPanel(true)" style="font-size:.72rem">' +
      '<i class="bi bi-arrow-clockwise me-1"></i>Reintentar</button>' +
      '</div>';
  }
}

function renderDragPanel(lista) {
  var BADGE = {'En preparación':'warning','En curso':'primary','Cerrado':'secondary','Entregado completo':'success'};
  var html = '';

  if (lista && lista.length) {
    html = lista.map(function(m) {
      var badge = BADGE[m.estado] || 'secondary';
      var isDark = badge === 'warning';
      return '<div class="dp-manifest-zone" ' +
        'ondragover="event.preventDefault();this.classList.add(\'dp-drag-over\')" ' +
        'ondragleave="this.classList.remove(\'dp-drag-over\')" ' +
        'ondrop="dropEnManifiesto(event,' + m.id + ')" ' +
        'onclick="asignarManual(' + m.id + ')">' +
        '<div class="d-flex justify-content-between align-items-center">' +
          '<div style="min-width:0;flex:1">' +
            '<div class="fw-bold font-monospace text-white" style="font-size:.85rem">' + esc(m.correlativo) + '</div>' +
            '<div style="color:#888;font-size:.72rem;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">' +
              esc(m.courier || 'Sin courier') + '</div>' +
          '</div>' +
          '<div class="text-end ms-2" style="flex-shrink:0">' +
            '<span class="badge bg-' + badge + (isDark?' text-dark':'') + '" style="font-size:.62rem">' + esc(m.estado) + '</span>' +
            '<div style="color:#666;font-size:.68rem;margin-top:2px">' +
              (m.total_items||0) + ' doc' + ((m.total_items||0)!==1?'s':'') + '</div>' +
          '</div>' +
        '</div>' +
        '<div style="margin-top:6px;font-size:.68rem;color:#444;border-top:1px solid #1e1e1e;padding-top:5px">' +
          '<i class="bi bi-box-arrow-in-down me-1"></i>Suelta o haz clic para asignar aquí' +
        '</div>' +
      '</div>';
    }).join('');
  } else {
    html = '<div class="text-center py-4" style="color:#444">' +
      '<i class="bi bi-inbox" style="font-size:1.6rem;display:block;opacity:.35;margin-bottom:8px"></i>' +
      '<div style="font-size:.78rem">Sin manifiestos activos</div>' +
      '<div style="font-size:.7rem;color:#333;margin-top:3px">Crea uno con el botón de abajo</div>' +
    '</div>';
  }

  document.getElementById('dragPanelBody').innerHTML = html;

  // Rellenar datalist (legacy) + select de couriers
  var dl = document.getElementById('ddCourierList');
  if (dl) {
    dl.innerHTML = (COURIERS||[]).map(function(c){
      return '<option value="' + esc(c.nombre||c) + '">';
    }).join('');
  }
  _cargarCouriersSelect();
}

// ── COURIER SELECTOR — pobla <select> con couriers de BD ──────────
var _couriersCache = null;
async function _cargarCouriersSelect() {
  var sel = document.getElementById('ddCourier');
  if (!sel) return;
  if (!_couriersCache) {
    try {
      var r = await fetch('/transporte/api/couriers/lista');
      var d = await r.json();
      _couriersCache = (d && d.ok && d.couriers) ? d.couriers : [];
    } catch(err) { _couriersCache = []; }
  }
  // Rellenar solo si el select tiene solo el placeholder (evita reset al renderizar manifiestos)
  if (sel.options.length > 1) return;
  _couriersCache.forEach(function(c){
    var opt = document.createElement('option');
    opt.value = c.nombre;
    opt.textContent = c.nombre + (c.peso_max_guia ? ' (max ' + c.peso_max_guia + ' kg)' : '');
    opt.dataset.tipo = c.tipo || '';
    opt.dataset.pesoMax = c.peso_max_guia || 0;
    opt.dataset.cid = c.id || '';
    sel.appendChild(opt);
  });
}

function onCourierChange() {
  var sel = document.getElementById('ddCourier');
  var hint = document.getElementById('ddCourierHint');
  var btnCrear = document.getElementById('dpBtnCrear');
  if (!sel || !hint) return;
  var opt = sel.options[sel.selectedIndex];
  if (!opt || !sel.value) {
    hint.style.display = 'none';
    if (btnCrear) btnCrear.disabled = !_dpQueue.length || true;
    return;
  }
  // Mostrar hint con info del courier elegido
  var tipo = opt.dataset.tipo || 'nacional';
  var pesoMax = parseFloat(opt.dataset.pesoMax || 0);
  var bits = [];
  bits.push('<span style="color:#aaa"><i class="bi bi-info-circle me-1"></i>Tipo: <strong style="color:#ddd">' + esc(tipo) + '</strong></span>');
  if (pesoMax > 0) {
    bits.push('<span style="color:#aaa">Peso máx por guía: <strong style="color:#fbbf24">' + pesoMax + ' kg</strong></span>');
  }
  // Hint específico por courier conocido
  var nombre = sel.value.toLowerCase();
  if (/starken/.test(nombre)) {
    bits.push('<span style="color:#93c5fd"><i class="bi bi-card-list me-1"></i>Recuerda registrar el OT al imprimir</span>');
  } else if (/chilexp|chile express/.test(nombre)) {
    bits.push('<span style="color:#93c5fd"><i class="bi bi-card-list me-1"></i>Requiere número de tracking al cerrar</span>');
  } else if (/clickex|dropit/.test(nombre)) {
    bits.push('<span style="color:#86efac"><i class="bi bi-lightning-charge me-1"></i>Despacho mismo día disponible</span>');
  }
  hint.innerHTML = bits.join(' &middot; ');
  hint.style.display = '';
  // Habilitar crear si hay cola
  if (btnCrear) btnCrear.disabled = !_dpQueue.length;
}

// ── DROP HANDLERS ─────────────────────────────────────────────────
function dropEnZona(e, tipo) {
  e.preventDefault();
  document.getElementById('dragDropZoneNuevo').classList.remove('dp-drag-over');
  var cid = parseInt(e.dataTransfer.getData('text/plain')) || _dragCid;
  if (cid) _dpAddToQueue(cid, _dpData[cid]?.label||'', _dpData[cid]?.cliente||'');
  if (tipo === 'nuevo') dpCrearManifiesto();
}

async function dropEnManifiesto(e, mid) {
  e.preventDefault();
  e.currentTarget.classList.remove('dp-drag-over');
  // Si hay cola, asigna toda la cola; si no, el elemento arrastrado ahora
  var cid = parseInt(e.dataTransfer.getData('text/plain')) || _dragCid;
  if (cid && _dpQueue.indexOf(cid) === -1) _dpAddToQueue(cid, '', '');
  if (!_dpQueue.length) return;
  await _asignarAPI(_dpQueue.slice(), mid);
}

async function asignarManual(mid) {
  if (!_dpQueue.length) return;
  await _asignarAPI(_dpQueue.slice(), mid);
}

async function dpCrearManifiesto() {
  if (!_dpQueue.length) return;
  await _asignarAPI(_dpQueue.slice(), null);
}

async function _asignarAPI(cids, mid) {
  var courier = (document.getElementById('ddCourier')?.value || '').trim() || 'Por asignar';
  var msg = document.getElementById('ddMsg');
  if (msg) msg.innerHTML = '<span style="color:#ffb700"><div class="spinner-border spinner-border-sm me-1" style="display:inline-block"></div>Asignando ' + cids.length + ' documento(s)…</span>';
  if (document.getElementById('dpBtnCrear')) document.getElementById('dpBtnCrear').disabled = true;

  try {
    const r = await fetch('/transporte/api/manifiestos/asignar', {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({commitment_ids: cids, manifest_id: mid, courier: courier})
    });
    const d = await r.json();
    if (d.ok) {
      _manifestosActivos = null;
      var texto = mid
        ? ('Asignado' + (cids.length>1?' (' + cids.length + ' docs)':'') + ' al manifiesto existente')
        : ('Manifiesto <strong>' + esc(d.correlativo||'') + '</strong> creado con ' + cids.length + ' documento(s)');
      if (msg) msg.innerHTML = '<span style="color:#5cb85c"><i class="bi bi-check-circle-fill me-1"></i>' + texto + '</span>';

      // Quitar filas del monitor o marcar como asignadas
      cids.forEach(function(cid) {
        var tr = document.querySelector('#monitorTbody tr[data-id="' + cid + '"]');
        if (tr) {
          tr.style.transition = 'opacity .4s';
          tr.style.opacity = '.3';
          setTimeout(function(){ if(tr.parentNode) tr.parentNode.removeChild(tr); }, 500);
        }
      });
      dpLimpiarCola();
      // Recargar manifiestos tras breve pausa
      setTimeout(function(){ cargarDragPanel(true); }, 900);
    } else {
      // PR-B: mismo criterio, preferir `msg` sobre `error`.
      if (msg) msg.innerHTML = '<span style="color:#f66"><i class="bi bi-x-circle me-1"></i>' + esc(d.msg||d.error||'Error desconocido') + '</span>';
      if (document.getElementById('dpBtnCrear')) document.getElementById('dpBtnCrear').disabled = false;
    }
  } catch(err) {
    if (msg) msg.innerHTML = '<span style="color:#f66">Error de conexión</span>';
    if (document.getElementById('dpBtnCrear')) document.getElementById('dpBtnCrear').disabled = false;
  }
}

function esc(s){ return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }
// Escape para VALORES de atributo (esc() no cubre comillas). Global para que
// tabla, cards y Kanban lo reusen sin redefinirlo localmente.
function attr(s){ return esc(s).replace(/"/g,'&quot;'); }

// _cotizarCid: pese al nombre (histórico), ya NO tiene relación con el
// cotizador -- es el commitment_id activo del modal de vista, usado por
// vistaAsignarManifiesto/vistaEditar/reenviarNotificacionCliente/
// actualizarEstadoPuntual/cargarTrazabilidadInline. El botón y la sección
// "Cotizar envío" se retiraron del modal el 2026-07-29 (autorización
// explícita de Daniel: "cotizar envío, sácalo, ya no lo necesitamos") junto
// con cotizarActual()/closeCotizador()/fmtKg() -- confirmado por grep que no
// se usaban en ningún otro lugar del proyecto antes de borrarlos.
var _cotizarCid = null;

// ── Acciones del modal de vista ──────────────────────────────────
// "Agregar a manifiesto" desde el modal: cierra el modal y agrega el doc
// actual a la cola del panel de manifiesto (se auto-abre).
function vistaAsignarManifiesto() {
  if (!_cotizarCid) return;
  var docNum = (document.getElementById('vistaDocNum')?.textContent || '').trim();
  var cliente = (document.getElementById('vistaCliente')?.textContent || '').trim();
  if (_vistaModal) _vistaModal.hide();
  // pequeño delay para que el modal termine de cerrarse antes de abrir el panel
  setTimeout(function(){
    abrirDragPanelConCid(_cotizarCid, docNum, cliente);
  }, 220);
}

// "Editar campos" desde el modal: cierra el modal y abre el panel lateral
// con los inputs de estado/costo/notas (que sigue siendo útil para edición).
function vistaEditar() {
  if (!_cotizarCid) return;
  var cid = _cotizarCid;
  if (_vistaModal) _vistaModal.hide();
  setTimeout(function(){ openPanel(cid); }, 220);
}

// "Enviar estado por correo" desde el modal de vista (2026-07-26): reenvío
// MANUAL del correo de seguimiento, para cuando el cliente pregunta y Daniel
// quiere reenviarle el link de tracking / estado actual sin esperar el
// próximo cambio de estado automático. Salta el anti-spam de 600s en backend.
async function reenviarNotificacionCliente() {
  if (!_cotizarCid) return;
  var btn = document.getElementById('btnReenviarNotif');
  if (!_vistaCurrentEmail) {
    await ilusAlert({
      title: 'Sin correo registrado',
      message: 'Este pedido no tiene un correo de cliente registrado, así que no se puede reenviar el estado.',
      type: 'warning',
    });
    return;
  }
  var ok = await ilusConfirm({
    title: 'Reenviar estado por correo',
    message: '¿Reenviar el correo de seguimiento al cliente'
      + (_vistaCurrentCliente ? ' (' + _vistaCurrentCliente + ')' : '') + '?',
    sub: 'Se enviará a <strong>' + esc(_vistaCurrentEmail) + '</strong> con el estado actual'
      + (_vistaCurrentEstado ? ' (<strong>' + esc(_vistaCurrentEstado) + '</strong>)' : '') + '.',
    subHtml: true,
    okLabel: 'Enviar', cancelLabel: 'Cancelar',
  });
  if (!ok) return;

  var cid = _cotizarCid;
  var originalHtml = btn ? btn.innerHTML : '';
  if (btn) {
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span><span>Enviando…</span>';
  }
  try {
    var r = await fetch('/transporte/api/compromisos/' + cid + '/reenviar-notificacion', {
      method: 'POST', headers: { 'Content-Type': 'application/json' }
    });
    var d = await r.json();
    if (!d.ok) {
      await ilusAlert({ title: 'No se pudo enviar', message: d.error || 'Ocurrió un error al enviar el correo.', type: 'error' });
      return;
    }
    ilusToast('Correo de seguimiento reenviado a ' + d.enviado_a, { type: 'success' });
  } catch (e) {
    await ilusAlert({ title: 'Error de conexión', message: 'No se pudo contactar al servidor. Intenta de nuevo.', type: 'error' });
  } finally {
    if (btn) {
      btn.disabled = !_vistaCurrentEmail;
      btn.innerHTML = originalHtml;
    }
  }
}

// "Actualizar estado" desde el modal de vista (2026-07-26): consulta AHORA
// el estado real de ESTE pedido contra su courier (FedEx/SimpliRoute), sin
// esperar el poller automático de 24h/cron. Refresca el badge sin cerrar el modal.
async function actualizarEstadoPuntual() {
  if (!_cotizarCid) return;
  var cid = _cotizarCid;
  var btn = document.getElementById('btnActualizarEstado');
  var originalHtml = btn ? btn.innerHTML : '';
  if (btn) {
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span><span>Consultando…</span>';
  }
  try {
    var r = await fetch('/transporte/api/compromisos/' + cid + '/actualizar-estado', {
      method: 'POST', headers: { 'Content-Type': 'application/json' }
    });
    var d = await r.json();
    if (!d.ok) {
      await ilusAlert({ title: 'No se pudo actualizar', message: d.error || 'Ocurrió un error al consultar al courier.', type: 'error' });
      return;
    }
    if (d.mensaje && !d.actualizado) {
      ilusToast(d.mensaje, { type: 'info' });
    } else if (d.actualizado) {
      ilusToast('Estado actualizado: ' + (d.estado_actual || '—'), { type: 'success' });
    } else {
      ilusToast('Consultado — sin cambios de estado.', { type: 'info' });
    }
    if (d.estado_actual) {
      _vistaCurrentEstado = d.estado_actual;
      renderEstadoBadge(d.estado_actual);
    }
  } catch (e) {
    await ilusAlert({ title: 'Error de conexión', message: 'No se pudo contactar al servidor. Intenta de nuevo.', type: 'error' });
  } finally {
    if (btn) {
      btn.disabled = false;
      btn.innerHTML = originalHtml;
    }
  }
}

// Trazabilidad INTEGRADA en el modal de vista (2026-07-29, antes era un
// mini-modal aparte "Ver seguimiento y trazabilidad" -- Daniel pidió que
// dejara de ser un clic extra: se carga sola al abrir el documento, dentro
// del mismo vistaModal). Trae el link público del cliente (/t/<token>) y el
// historial combinado (transport_logs de commitment + manifest_item).
async function cargarTrazabilidadInline(cid) {
  if (!cid) return;

  document.getElementById('trazaCourierBlock').style.display = 'none';
  document.getElementById('trazaLinkBlock').style.display = 'none';
  document.getElementById('trazaLoading').style.display = '';
  document.getElementById('trazaEmpty').style.display = 'none';
  var tlEl = document.getElementById('trazaTimeline');
  tlEl.style.display = 'none';
  tlEl.innerHTML = '';
  // Reset de las secciones enriquecidas (2026-07-29)
  ['trazaRecibio','trazaEvidFotos','trazaDriverBlock','trazaProdBlock','trazaDespBlock']
    .forEach(function(id){ var el = document.getElementById(id); if (el) el.style.display = 'none'; });
  document.getElementById('trazaEvidFotos').innerHTML = '';
  document.getElementById('trazaProdRows').innerHTML = '';
  document.getElementById('trazaDespRows').innerHTML = '';

  try {
    var r = await fetch('/transporte/api/compromisos/' + cid + '/trazabilidad');
    var d = await r.json();
    document.getElementById('trazaLoading').style.display = 'none';
    if (!d.ok) {
      ilusToast(d.error || 'No se pudo cargar la trazabilidad.', { type: 'error' });
      return;
    }

    // Última milla: courier + estado + tracking nativo (2026-07-27)
    if (d.item && d.item.courier) {
      document.getElementById('trazaCourierBlock').style.display = '';
      document.getElementById('trazaCourierNombre').textContent = d.item.courier;
      var estBadge = document.getElementById('trazaCourierEstado');
      estBadge.textContent = d.item.estado || '—';
      estBadge.className = 'badge ' + (ESTADO_COLORS[d.item.estado] ? 'text-bg-' + ESTADO_COLORS[d.item.estado] : 'text-bg-secondary');
      var tnRow = document.getElementById('trazaCourierTrackingRow');
      if (d.item.courier_tracking_number) {
        tnRow.style.display = '';
        document.getElementById('trazaCourierTN').textContent = d.item.courier_tracking_number;
        var tnLink = document.getElementById('trazaCourierTNLink');
        if (d.item.courier_tracking_url) {
          tnLink.href = d.item.courier_tracking_url;
          tnLink.style.display = '';
        } else {
          tnLink.style.display = 'none';
        }
      } else {
        tnRow.style.display = 'none';
      }
    }

    if (d.tracking_url) {
      document.getElementById('trazaLinkBlock').style.display = '';
      document.getElementById('trazaLinkInput').value = d.tracking_url;
      document.getElementById('trazaLinkOpen').href = d.tracking_url;
    }

    // ── Evidencia de entrega: receptor + firma/fotos (2026-07-29) ──
    if (d.proof) {
      var pf = d.proof;
      var recBlock = document.getElementById('trazaRecibio');
      if (pf.receptor_nombre || pf.entregado_at) {
        document.getElementById('trazaRecibioNombre').textContent =
          pf.receptor_nombre ? ('Recibió: ' + pf.receptor_nombre) : 'Entrega confirmada';
        document.getElementById('trazaRecibioRel').textContent = pf.receptor_relacion || '';
        document.getElementById('trazaRecibioTs').textContent = pf.entregado_at || '';
        recBlock.style.display = '';
      }
      var fotosWrap = document.getElementById('trazaEvidFotos');
      var evidImgs = [];
      if (pf.firma_url) evidImgs.push(pf.firma_url);
      (pf.fotos || []).forEach(function(u){ evidImgs.push(u); });
      if (evidImgs.length) {
        fotosWrap.innerHTML = evidImgs.map(function(url) {
          return '<img src="' + url + '" alt="Evidencia de entrega" ' +
            'style="width:64px;height:64px;object-fit:cover;border-radius:8px;cursor:pointer;border:1px solid #ddd" ' +
            'onclick="verFoto(\'' + url + '\')" onerror="this.style.display=\'none\'">';
        }).join('');
        fotosWrap.style.display = '';
      }
    }

    // ── Chofer + última posición (2026-07-29) ──
    if (d.chofer) {
      var ch = d.chofer;
      var chBlock = document.getElementById('trazaDriverBlock');
      var inicial = (ch.nombre || '?').trim().charAt(0).toUpperCase();
      document.getElementById('trazaDriverAvatar').textContent = inicial || '?';
      document.getElementById('trazaDriverNombre').textContent = ch.nombre || 'Chofer sin nombre';
      var metaParts = [];
      if (ch.courier) metaParts.push(ch.courier);
      if (ch.patente) metaParts.push(ch.patente);
      if (ch.telefono) metaParts.push(ch.telefono);
      if (d.last_ping && d.last_ping.age_s != null) {
        var mins = Math.round(d.last_ping.age_s / 60);
        metaParts.push('GPS hace ' + (mins < 1 ? 'instantes' : mins + ' min'));
      }
      document.getElementById('trazaDriverMeta').textContent = metaParts.join(' · ');
      chBlock.style.display = '';
    }

    // ── Productos con foto (2026-07-29) ──
    var lineasProd = d.lineas || [];
    if (lineasProd.length) {
      var prodRows = document.getElementById('trazaProdRows');
      prodRows.innerHTML = lineasProd.map(function(l) {
        var thumb = (l.fotos && l.fotos[0])
          ? '<img src="' + l.fotos[0] + '" alt="" class="sr-prod-thumb" onclick="verFoto(\'' + l.fotos[0] + '\')" onerror="this.style.display=\'none\'">'
          : '<div class="sr-prod-thumb-ph"><i class="bi bi-image"></i></div>';
        var saldoChip = (l.saldo > 0)
          ? '<span class="sr-qty-chip saldo">Saldo ' + l.saldo + '</span>'
          : '<span class="sr-qty-chip desp">Completo</span>';
        return '<div class="sr-prod d-flex align-items-center gap-2 py-1">' + thumb +
          '<div class="sr-prod-info flex-grow-1">' +
          '<div class="sr-prod-name">' + esc(l.nombre || '') + '</div>' +
          '<div class="sr-prod-sku">' + esc(l.sku || '') + '</div>' +
          '</div>' +
          '<div class="sr-prod-qty">' + saldoChip + '</div>' +
          '</div>';
      }).join('');
      document.getElementById('trazaProdBlock').style.display = '';
    }

    // ── Despachos (si el documento se repartió en varios manifiestos) ──
    var despachos = d.despachos || [];
    if (despachos.length > 1) {
      var despRows = document.getElementById('trazaDespRows');
      despRows.innerHTML = despachos.map(function(x) {
        var estBadge2 = ESTADO_COLORS[x.estado_entrega] ? 'text-bg-' + ESTADO_COLORS[x.estado_entrega] : 'text-bg-secondary';
        return '<div class="sr-desp-row d-flex align-items-center gap-2' + (x.es_actual ? ' is-actual' : '') + '">' +
          '<div>' +
          '<div class="sr-desp-doc">' + esc(x.correlativo || ('#' + x.manifest_id)) + (x.es_actual ? ' <span class="sr-desp-tag">actual</span>' : '') + '</div>' +
          '<div class="sr-desp-meta">' + esc(x.courier || '') + (x.fecha ? ' · ' + esc(x.fecha) : '') + '</div>' +
          '</div>' +
          '<span class="badge ' + estBadge2 + '" style="font-size:.68rem">' + esc(x.estado_entrega || '—') + '</span>' +
          '</div>';
      }).join('');
      document.getElementById('trazaDespBlock').style.display = '';
    }

    var eventos = d.eventos || [];
    if (!eventos.length) {
      document.getElementById('trazaEmpty').style.display = '';
      return;
    }

    var ICONS = {
      commitment: 'bi-file-earmark-text',
      manifest_item: 'bi-truck',
    };
    var html = '<div class="d-flex flex-column gap-2">';
    eventos.forEach(function(ev) {
      var icon = ICONS[ev.entity_type] || 'bi-dot';
      html += '<div class="d-flex gap-2" style="border-left:2px solid #e9ecef;padding:2px 0 2px 12px;margin-left:4px">'
        + '<i class="bi ' + icon + ' text-danger" style="margin-left:-19px;background:#fff"></i>'
        + '<div>'
        + '<div class="fw-semibold" style="font-size:.82rem">' + esc(ev.accion || '') + '</div>'
        + (ev.detalle ? '<div class="text-muted" style="font-size:.78rem">' + esc(ev.detalle) + '</div>' : '')
        + '<div class="text-muted" style="font-size:.7rem">'
        + (ev.usuario ? esc(ev.usuario) + ' · ' : '') + esc(ev.created_at || '')
        + '</div></div></div>';
    });
    html += '</div>';
    tlEl.innerHTML = html;
    tlEl.style.display = '';
  } catch (e) {
    document.getElementById('trazaLoading').style.display = 'none';
    ilusToast('Error de conexión al cargar la trazabilidad.', { type: 'error' });
  }
}

function copiarLinkTraza() {
  var input = document.getElementById('trazaLinkInput');
  if (!input || !input.value) return;
  navigator.clipboard.writeText(input.value).then(function() {
    ilusToast('Link copiado al portapapeles', { type: 'success' });
  }).catch(function() {
    input.select();
    ilusToast('Selecciona y copia el link manualmente', { type: 'warning' });
  });
}

// ── FOTOS INLINE EN MODAL DE VISTA ───────────────────────────────
function renderFotoPanel(linea) {
  var panel  = document.getElementById('vistaFotoUpload');
  var gallery = document.getElementById('vistaFotoGallery');
  var label  = document.getElementById('vistaFotoLabel');
  var cnt    = document.getElementById('vistaFotoCount');
  if (!panel || !linea) return;

  var fotos  = linea.fotos || [];
  var sku    = linea.sku || '';
  _vistaCurrentSku = sku || null;

  cnt.textContent = fotos.length + '/2';
  gallery.innerHTML = fotos.map(function(url) {
    return '<img src="' + url + '" alt="" ' +
      'style="width:80px;height:80px;object-fit:cover;border-radius:8px;cursor:pointer;' +
      'border:2px solid #ddd" onclick="verFoto(\'' + url + '\')" ' +
      'onerror="this.style.display=\'none\'">';
  }).join('');

  if (fotos.length >= 2) {
    label.style.display = 'none';
  } else {
    label.style.display = '';
  }
  panel.style.display = '';
}

async function subirFotoVistaInline(input) {
  if (!input.files[0] || !_vistaCurrentSku) return;
  var label = document.getElementById('vistaFotoLabel');
  var orig  = label.innerHTML;
  label.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Subiendo…';

  var fd = new FormData();
  fd.append('sku',  _vistaCurrentSku);
  fd.append('foto', input.files[0]);

  try {
    var r = await fetch('/transporte/api/upload-foto', {method:'POST', body:fd});
    var d = await r.json();
    if (d.ok) {
      // Refresh foto gallery
      var gallery = document.getElementById('vistaFotoGallery');
      var cnt     = document.getElementById('vistaFotoCount');
      var img = document.createElement('img');
      img.src = d.url;
      img.alt = '';
      img.style.cssText = 'width:80px;height:80px;object-fit:cover;border-radius:8px;border:2px solid #ddd;cursor:pointer';
      img.onclick = function(){ verFoto(d.url); };
      gallery.appendChild(img);
      var n = parseInt((cnt.textContent||'0').split('/')[0]) + 1;
      cnt.textContent = n + '/2';
      if (n >= 2) label.style.display = 'none';
      else { label.innerHTML = orig; input.value = ''; }
    } else {
      alert('Error: ' + (d.error||'No se pudo subir'));
      label.innerHTML = orig; input.value = '';
    }
  } catch(e) {
    alert('Error de red al subir la foto');
    label.innerHTML = orig; input.value = '';
  }
}

function addDoc() {
  var btn = document.getElementById('addDocBtn');
  var msg = document.getElementById('addDocMsg');
  var tido = document.getElementById('addTido').value;
  var nudo = document.getElementById('addNudo').value.trim();
  if (!nudo) { msg.innerHTML='<span class="text-danger">Ingresa el número</span>'; return; }

  btn.disabled = true;
  btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Importando…';
  msg.innerHTML = '';

  var fd = new FormData();
  fd.append('tido', tido); fd.append('nudo', nudo);
  fetch('/transporte/api/agregar', {method:'POST', body:fd})
    .then(function(r){ return r.json(); })
    .then(function(d) {
      if (d.ok) {
        msg.innerHTML = '<span class="text-success"><i class="bi bi-check-circle me-1"></i>Importado correctamente</span>';
        setTimeout(function(){ location.reload(); }, 800);
      } else {
        msg.innerHTML = '<span class="text-danger">' + (d.error||'Error') + '</span>';
        btn.disabled=false; btn.innerHTML='<i class="bi bi-download me-1"></i>Importar desde ERP';
      }
    })
    .catch(function() {
      msg.innerHTML='<span class="text-danger">Error de conexión</span>';
      btn.disabled=false; btn.innerHTML='<i class="bi bi-download me-1"></i>Importar desde ERP';
    });
}

// ════════════════════════════════════════════════════════════════
//  PENDIENTES POR LÍNEA (Daniel 2026-07-25) — namespace "lp*" propio,
//  no comparte variables con el resto del archivo. Usa
//  /transporte/api/lineas-pendientes (lectura) y
//  /transporte/api/lineas-pendientes/enviar-manifiesto (acción masiva).
// ════════════════════════════════════════════════════════════════
var _lpLineas = [];
// FIX 2026-07-25: el backend (/transporte/api/lineas-pendientes/enviar-manifiesto)
// valida y agrupa por linea_id (transport_commitment_lines.id), NO por
// commitment_id -- necesita el id de LÍNEA para poder chequear el stock de
// ESE producto puntual (una factura puede tener una línea con stock y otra
// sin). Seleccionar por commitment_id habría mandado siempre "linea_ids"
// vacío y el envío habría fallado con 400 "Selecciona al menos una línea".
var _lpSeleccionLineaIds = new Set();
var _lpFiltroTimer = null;

function lpAbrir() {
  lpCargarManifiestosAbiertos();
  lpCargar();
}

function lpFiltrarDebounced() {
  clearTimeout(_lpFiltroTimer);
  _lpFiltroTimer = setTimeout(lpCargar, 350);
}

function lpCargarManifiestosAbiertos() {
  fetch('/transporte/manifiestos?estado=' + encodeURIComponent('En preparación') + '&page_size=50')
    .then(function(r){ return r.text(); })
    .catch(function(){ });
  // El listado de manifiestos abiertos vive en una página server-rendered
  // (no JSON); para no duplicar esa ruta, el selector ofrece "crear nuevo"
  // como opción principal — elegir uno existente es un refinamiento futuro
  // si Daniel lo pide (requeriría un endpoint JSON de manifiestos abiertos).
}

function lpCargar() {
  var sku = document.getElementById('lpFiltroSku').value.trim();
  var q   = document.getElementById('lpFiltroQ').value.trim();
  var pv  = document.getElementById('lpFiltroPreventa').value;
  var url = '/transporte/api/lineas-pendientes?sku=' + encodeURIComponent(sku) +
            '&q=' + encodeURIComponent(q) + '&preventa=' + encodeURIComponent(pv);
  document.getElementById('lpMsg').textContent = 'Cargando…';
  fetch(url).then(function(r){ return r.json(); }).then(function(d) {
    if (!d.ok) {
      document.getElementById('lpMsg').innerHTML = '<span class="text-danger">' + (d.error || 'Error al cargar') + '</span>';
      return;
    }
    _lpLineas = d.lineas || [];
    document.getElementById('lpTotalBadge').textContent = _lpLineas.length + ' línea(s)';
    document.getElementById('lpMsg').textContent = _lpLineas.length
      ? 'Mostrando ' + _lpLineas.length + ' línea(s) con saldo pendiente.'
      : 'No hay líneas con saldo pendiente para este filtro.';
    lpRenderTabla();
  }).catch(function() {
    document.getElementById('lpMsg').innerHTML = '<span class="text-danger">Error de conexión</span>';
  });
}

function _lpEsc(s) {
  return String(s == null ? '' : s).replace(/[&<>"']/g, function(c) {
    return ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'})[c];
  });
}

function lpRenderTabla() {
  var tbody = document.getElementById('lpTbody');
  if (!_lpLineas.length) { tbody.innerHTML = ''; return; }
  var html = '';
  for (var i = 0; i < _lpLineas.length; i++) {
    var l = _lpLineas[i];
    var checked = _lpSeleccionLineaIds.has(l.linea_id) ? 'checked' : '';
    var preventaBadge = '';
    if (l.preventa_manual === 'total') {
      preventaBadge = '<span class="badge bg-danger">Preventa total</span>';
    } else if (l.preventa_manual === 'parcial') {
      preventaBadge = '<span class="badge" style="background:#f59e0b">Preventa parcial</span>';
    } else if (l.preventa_auto) {
      preventaBadge = '<span class="badge bg-secondary">Sin stock (auto)</span>';
    } else {
      preventaBadge = '<span class="text-muted small">—</span>';
    }
    var stockHtml = '<span class="text-muted small">s/d</span>';
    if (l.stock) {
      var disp = l.stock.disponible;
      var color = disp > 0 ? '#16a34a' : '#dc2626';
      stockHtml = '<span style="color:' + color + '" title="Físico ' + l.stock.fisico +
        ' · Comprometido ' + l.stock.comprometido + ' · Devengado ' + l.stock.devengado + '">' +
        disp + ' disp.</span>';
    }
    html += '<tr' + (l.en_manifiesto ? ' style="opacity:.55" title="Ya está en un manifiesto (parcial)"' : '') + '>' +
      '<td><input type="checkbox" data-linea-id="' + l.linea_id + '" ' + checked +
        ' onchange="lpToggleLinea(' + l.linea_id + ', this.checked)"></td>' +
      '<td class="font-monospace">' + _lpEsc(l.tido) + ' ' + _lpEsc(l.nudo) + '</td>' +
      '<td>' + _lpEsc(l.cliente) + '</td>' +
      '<td>' + _lpEsc(l.comuna) + '</td>' +
      '<td class="font-monospace small">' + _lpEsc(l.sku) + '</td>' +
      '<td class="small">' + _lpEsc(l.nombre) + '</td>' +
      '<td class="text-end fw-bold">' + l.saldo + '</td>' +
      '<td>' + preventaBadge +
        '<div class="btn-group btn-group-sm mt-1">' +
        '<button class="btn btn-outline-secondary btn-sm py-0 px-1" style="font-size:.68rem" ' +
          'onclick="lpMarcarPreventa(' + l.commitment_id + ',\'parcial\')">Parcial</button>' +
        '<button class="btn btn-outline-secondary btn-sm py-0 px-1" style="font-size:.68rem" ' +
          'onclick="lpMarcarPreventa(' + l.commitment_id + ',\'total\')">Total</button>' +
        '<button class="btn btn-outline-secondary btn-sm py-0 px-1" style="font-size:.68rem" ' +
          'onclick="lpMarcarPreventa(' + l.commitment_id + ',\'\')" title="Desmarcar">✕</button>' +
        '</div></td>' +
      '<td>' + stockHtml + '</td>' +
      '<td></td>' +
      '</tr>';
  }
  tbody.innerHTML = html;
  lpActualizarSeleccion();
}

function lpToggleLinea(lineaId, on) {
  if (on) _lpSeleccionLineaIds.add(lineaId); else _lpSeleccionLineaIds.delete(lineaId);
  lpActualizarSeleccion();
}

function lpToggleAll(on) {
  document.querySelectorAll('#lpTbody input[type=checkbox]').forEach(function(cb) {
    cb.checked = on;
    var lineaId = parseInt(cb.getAttribute('data-linea-id'), 10);
    if (on) _lpSeleccionLineaIds.add(lineaId); else _lpSeleccionLineaIds.delete(lineaId);
  });
  lpActualizarSeleccion();
}

function lpActualizarSeleccion() {
  var n = _lpSeleccionLineaIds.size;
  document.getElementById('lpSeleccion').textContent =
    n + ' línea(s) seleccionada(s) para enviar' +
    (n ? ' (el documento completo entra al manifiesto — hoy el manifiesto no separa por línea individual)' : '');
}

function lpMarcarPreventa(cid, valor) {
  fetch('/transporte/api/compromisos/' + cid + '/preventa', {
    method: 'PUT',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify({valor: valor})
  }).then(function(r){ return r.json(); }).then(function(d) {
    if (d.ok) {
      if (typeof ilusToast === 'function') {
        ilusToast(valor ? ('Marcado como preventa ' + valor) : 'Desmarcado', {type:'success'});
      }
      lpCargar();
    } else if (typeof ilusToast === 'function') {
      ilusToast(d.error || 'No se pudo actualizar', {type:'error'});
    }
  }).catch(function() {
    if (typeof ilusToast === 'function') ilusToast('Error de conexión', {type:'error'});
  });
}

function lpEnviarAManifiesto() {
  var lineaIds = Array.from(_lpSeleccionLineaIds);
  if (!lineaIds.length) {
    if (typeof ilusToast === 'function') ilusToast('Selecciona al menos una línea', {type:'warning'});
    return;
  }
  var manifId = document.getElementById('lpManifiestoExistente').value;
  var body = {linea_ids: lineaIds};
  if (manifId) {
    body.manifest_id = parseInt(manifId, 10);
  } else {
    var courier = document.getElementById('lpCourierNuevo').value.trim();
    var fecha   = document.getElementById('lpFechaNueva').value;
    if (!courier || !fecha) {
      if (typeof ilusToast === 'function') {
        ilusToast('Para crear un manifiesto nuevo indica courier y fecha (o elige uno existente)', {type:'warning'});
      }
      return;
    }
    body.courier = courier; body.fecha = fecha;
  }
  var btn = document.getElementById('lpEnviarBtn');
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner-border spinner-border-sm"></span> Enviando…';
  _lpEnviarRequest(body, btn);
}

// FIX 2026-07-25: faltaba manejar el 409 "requiere_autorizacion" que devuelve
// el backend cuando alguna línea seleccionada no tiene stock confirmado
// (Daniel: "tiene que avisarme... a menos que haya una autorización"). Antes
// esto solo mostraba un toast de error genérico y no ofrecía ninguna forma
// de continuar -- ni siquiera para un admin/superadmin.
function _lpEnviarRequest(body, btn) {
  fetch('/transporte/api/lineas-pendientes/enviar-manifiesto', {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify(body)
  }).then(function(r){ return r.json().then(function(d){ return {status:r.status, d:d}; }); })
    .then(function(res) {
    var d = res.d;
    if (d.ok) {
      btn.disabled = false;
      btn.innerHTML = '<i class="bi bi-send me-1"></i>Enviar seleccionados';
      _lpSeleccionLineaIds.clear();
      if (typeof ilusToast === 'function') {
        ilusToast(d.agregados + ' documento(s) enviado(s) al manifiesto ' + (d.correlativo || '#' + d.manifest_id), {type:'success'});
      }
      lpCargar();
      if (typeof cargarMonitor === 'function') cargarMonitor();
      return;
    }
    if (res.status === 409 && d.requiere_autorizacion) {
      var detalle = (d.bloqueadas || []).slice(0, 8).map(function(b) {
        return '• ' + b.sku + ' (' + b.nombre + ') — ' + b.doc + ' · ' + b.cliente +
               (b.disponible != null ? ' — disponible: ' + b.disponible : ' — sin dato de stock');
      }).join('<br>');
      if (d.bloqueadas && d.bloqueadas.length > 8) detalle += '<br>…y ' + (d.bloqueadas.length - 8) + ' más';
      btn.disabled = false;
      btn.innerHTML = '<i class="bi bi-send me-1"></i>Enviar seleccionados';
      if (typeof ilusConfirm !== 'function') {
        if (typeof ilusToast === 'function') ilusToast(d.error || 'Faltan productos sin stock por autorizar', {type:'error'});
        return;
      }
      ilusConfirm({
        title: 'Productos sin stock confirmado',
        message: d.error || 'Algunos productos no tienen stock disponible confirmado.',
        sub: detalle + '<br><br><strong>¿Enviar de todas formas?</strong> Esto puede generar un quiebre de stock — quedará registrado quién lo autorizó.',
        subHtml: true,
        okLabel: 'Sí, enviar sin stock', cancelLabel: 'Cancelar', danger: true,
      }).then(function(ok) {
        if (!ok) return;
        btn.disabled = true;
        btn.innerHTML = '<span class="spinner-border spinner-border-sm"></span> Enviando…';
        body.autorizado = true;
        _lpEnviarRequest(body, btn);
      });
      return;
    }
    btn.disabled = false;
    btn.innerHTML = '<i class="bi bi-send me-1"></i>Enviar seleccionados';
    // PR-B: mismo criterio, preferir `msg` sobre `error`.
    if (typeof ilusToast === 'function') ilusToast(d.msg || d.error || 'No se pudo enviar', {type:'error'});
  }).catch(function() {
    btn.disabled = false;
    btn.innerHTML = '<i class="bi bi-send me-1"></i>Enviar seleccionados';
    if (typeof ilusToast === 'function') ilusToast('Error de conexión', {type:'error'});
  });
}

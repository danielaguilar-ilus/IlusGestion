// ==========================================================================
// transporte_manifiestos.js
// Extraido TAL CUAL del <script> inline de templates/transporte/manifiestos.html
// (2026-07-28). Motivo: las paginas HTML se sirven con cache-control:no-store,
// asi que estos ~9KB de JS se re-descargaban en cada clic. Desde /static el
// navegador los cachea por dias (cache-busting por hash via @app.url_defaults:
// NUNCA agregar ?v= manual).
// Se carga con <script defer> DESPUES de transporte_etiquetas_modal.js, igual
// que cuando estaba inline al final del bloque scripts.
// Es un MOVIMIENTO, no un refactor: nada fue renombrado ni reformateado.
// ==========================================================================

document.addEventListener('DOMContentLoaded', function() {
  var today = new Date().toISOString().split('T')[0];
  var f = document.getElementById('newFecha');
  if (f) f.value = today;
  _actualizarFechaPreview();
});

// El <input type="date"> nativo muestra el formato del idioma del navegador/SO
// (a veces en inglés, mm/dd/aaaa) — no se puede forzar desde el HTML. Daniel
// 2026-07-22: mostrar SIEMPRE una vista en español (día/mes/año) debajo, sin
// tocar el valor real que se envía (siempre ISO yyyy-mm-dd, sin cambios).
function _actualizarFechaPreview(){
  var f = document.getElementById('newFecha');
  var prev = document.getElementById('newFechaPreview');
  if (!f || !prev) return;
  if (!f.value) { prev.textContent = ''; return; }
  var partes = f.value.split('-');   // yyyy-mm-dd
  if (partes.length !== 3) { prev.textContent = ''; return; }
  prev.textContent = partes[2] + '/' + partes[1] + '/' + partes[0];
}

function changePageSize(ps){
  var u = new URL(window.location.href);
  u.searchParams.set('page_size', ps);
  u.searchParams.set('page', '1');
  window.location.href = u.toString();
}

function toggleMenu(ev, id){
  ev.stopPropagation();
  document.querySelectorAll('.menu-dropdown.open').forEach(function(el){
    if (el.id !== id) el.classList.remove('open');
  });
  var m = document.getElementById(id);
  if (!m) return;
  var btn = ev.currentTarget;
  var rect = btn.getBoundingClientRect();
  m.style.top = (window.scrollY + rect.bottom + 6) + 'px';
  m.style.left = (window.scrollX + rect.right - 180) + 'px';
  m.classList.toggle('open');
}
document.addEventListener('click', function(){
  document.querySelectorAll('.menu-dropdown.open').forEach(function(el){ el.classList.remove('open'); });
});

// Eliminar manifiesto (soft-delete, solo superadmin — 2026-07-25).
// REGLA #1: nunca confirm() nativo. Si ya tiene actividad real (subido a un
// courier o con prueba de entrega), el backend responde 409 pidiendo que se
// escriba el correlativo — un solo clic no alcanza para borrar algo con
// trazabilidad real (Daniel: "si ya hay un compromiso de trazabilidad...").
async function eliminarManifiesto(mid, correlativo, confirmText) {
  if (!confirmText) {
    const ok = await ilusConfirm({
      title: 'Eliminar manifiesto',
      message: '¿Eliminar el manifiesto ' + correlativo + '?',
      sub: 'Desaparece del listado. Los despachos que agrupa y su historial de ' +
           'seguimiento NO se borran — solo se deja de mostrar esta agrupación.',
      okLabel: 'Eliminar', cancelLabel: 'Cancelar',
      danger: true,
    });
    if (!ok) return;
  }
  try {
    const r = await fetch('/transporte/manifiestos/' + mid + '/eliminar', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(confirmText ? { confirm_text: confirmText } : {}),
    });
    const d = await r.json();
    if (r.status === 409 && d.requiere_confirmacion) {
      const texto = await ilusPrompt({
        title: 'Confirmación adicional requerida',
        message: d.error,
        placeholder: d.correlativo,
        required: true,
      });
      if (texto === null) return;
      if (texto.trim() !== d.correlativo) {
        ilusToast('No coincide con "' + d.correlativo + '" — no se eliminó.', { type: 'warning' });
        return;
      }
      return eliminarManifiesto(mid, correlativo, texto.trim());
    }
    if (!r.ok || d.error) throw new Error(d.error || ('HTTP ' + r.status));
    ilusToast('Manifiesto eliminado', { type: 'success' });
    setTimeout(function(){ location.reload(); }, 700);
  } catch (e) {
    await ilusAlert({ title: 'No se pudo eliminar', message: e.message || 'Error de conexión', type: 'error' });
  }
}

// Selección múltiple + borrado masivo de manifiestos (solo superadmin, marcha
// blanca agosto 2026: Daniel necesita poder limpiar manifiestos de PRUEBA sin
// pedirlo uno por uno). Mismo patrón visual/funcional que Catálogo (checkbox
// por fila + "seleccionar todos" + barra flotante + ilusConfirm). Soft-delete
// real vía el mismo endpoint que ya usa el borrado individual, en lote.
let _manSeleccionados = new Set();

window.manToggleSeleccion = function(id, checked){
  if (checked) _manSeleccionados.add(id); else _manSeleccionados.delete(id);
  const tr = document.getElementById('man-row-' + id);
  if (tr) tr.classList.toggle('seleccionada', checked);
  const todos = document.getElementById('manChkTodos');
  if (todos) {
    const visibles = document.querySelectorAll('.man-chk-fila').length;
    const marcados = document.querySelectorAll('.man-chk-fila:checked').length;
    todos.checked = visibles > 0 && marcados === visibles;
    todos.indeterminate = marcados > 0 && marcados < visibles;
  }
  manRenderBarraSeleccion();
};

window.manToggleSeleccionarTodo = function(checked){
  document.querySelectorAll('.man-chk-fila').forEach(function(chk){
    chk.checked = checked;
    const id = parseInt(chk.getAttribute('data-id'), 10);
    if (checked) _manSeleccionados.add(id); else _manSeleccionados.delete(id);
    const tr = document.getElementById('man-row-' + id);
    if (tr) tr.classList.toggle('seleccionada', checked);
  });
  manRenderBarraSeleccion();
};

function manRenderBarraSeleccion(){
  const bar = document.getElementById('manBarraSeleccion');
  if (!bar) return;
  const n = _manSeleccionados.size;
  if (!n) { bar.style.display = 'none'; bar.innerHTML = ''; return; }
  bar.style.display = 'flex';
  bar.innerHTML = '<i class="bi bi-check-square-fill"></i>'
    + '<span class="cnt">' + n + '</span> manifiesto' + (n === 1 ? '' : 's') + ' seleccionado' + (n === 1 ? '' : 's')
    + '<span class="spacer"></span>'
    + '<button type="button" class="btn btn-sm btn-outline-light" onclick="manToggleSeleccionarTodo(false); document.querySelectorAll(\'.man-chk-fila\').forEach(c=>c.checked=false); const t=document.getElementById(\'manChkTodos\'); if(t){t.checked=false;t.indeterminate=false;}">Quitar selección</button>'
    + '<button type="button" class="btn btn-sm btn-danger" onclick="manBorrarSeleccionados()"><i class="bi bi-trash me-1"></i>Eliminar seleccionados</button>';
}

window.manBorrarSeleccionados = async function(){
  const ids = Array.from(_manSeleccionados);
  if (!ids.length) return;
  const ok = await ilusConfirm({
    title: 'Eliminar ' + ids.length + ' manifiesto' + (ids.length === 1 ? '' : 's'),
    message: '¿Eliminar ' + ids.length + ' manifiesto' + (ids.length === 1 ? '' : 's') + ' del listado?',
    sub: 'Se archivan (soft-delete), no se borran para siempre — los despachos que agrupan y su historial de seguimiento no se pierden, solo dejan de mostrarse esta agrupación. Si alguno ya tiene actividad real con un courier, ese en particular no se tocará y se avisará por separado.',
    okLabel: 'Eliminar', cancelLabel: 'Cancelar', danger: true,
  });
  if (!ok) return;
  try {
    const r = await fetch('/transporte/manifiestos/bulk-eliminar', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ids: ids }),
    });
    const d = await r.json();
    if (!d.ok) { ilusToast(d.error || 'No se pudo eliminar', { type: 'error' }); return; }
    let msg = '✓ ' + d.eliminados + ' manifiesto' + (d.eliminados === 1 ? '' : 's') + ' eliminado' + (d.eliminados === 1 ? '' : 's');
    if (d.omitidos) msg += ' (' + d.omitidos + ' con actividad real, no se tocaron)';
    ilusToast(msg, { type: d.eliminados ? 'success' : 'warning' });
    _manSeleccionados.clear();
    setTimeout(function(){ location.reload(); }, 700);
  } catch (e) {
    ilusToast('Sin conexión', { type: 'error' });
  }
};

function crearManifiesto() {
  var courier = document.getElementById('newCourier').value.trim();
  var fecha   = document.getElementById('newFecha').value.trim();
  var notas   = document.getElementById('newNotas').value.trim();
  var msg     = document.getElementById('newManMsg');

  if (!courier) { msg.innerHTML='<span class="text-danger">Selecciona un courier</span>'; return; }
  if (!fecha)   { msg.innerHTML='<span class="text-danger">Indica la fecha</span>'; return; }

  var btn = document.getElementById('newManBtn');
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Creando…';
  msg.innerHTML = '';

  var fd = new FormData();
  fd.append('courier', courier);
  fd.append('fecha', fecha);
  fd.append('notas', notas);

  fetch('/transporte/manifiestos/nuevo', {method:'POST', body:fd})
    .then(function(r){ return r.json(); })
    .then(function(d) {
      if (d.ok) {
        msg.innerHTML = '<span class="text-success"><i class="bi bi-check-circle me-1"></i>Creado ' + d.correlativo + '</span>';
        setTimeout(function(){ window.location.href = '/transporte/manifiestos/' + d.id; }, 700);
      } else {
        msg.innerHTML = '<span class="text-danger">' + (d.error||'Error') + '</span>';
        btn.disabled = false;
        btn.innerHTML = '<i class="bi bi-check-circle-fill me-1"></i>Crear manifiesto';
      }
    })
    .catch(function() {
      msg.innerHTML = '<span class="text-danger">Error de conexión</span>';
      btn.disabled = false;
      btn.innerHTML = '<i class="bi bi-check-circle-fill me-1"></i>Crear manifiesto';
    });
}

/* ==========================================================================
   transporte_manifiesto_detalle.js
   Extraido TAL CUAL del bloque <script> inline de
   templates/transporte/manifiesto_detalle.html.
   Motivo: el HTML se sirve con Cache-Control: no-store, asi que estos ~108KB
   de JS se re-descargaban en cada clic. Como archivo /static el navegador los
   cachea (el cache-busting por hash de @app.url_defaults sigue aplicando).

   Las 11 interpolaciones Jinja que tenia el script ahora se leen del objeto de
   configuracion window.MFD, que el template define inline JUSTO ANTES de cargar
   este archivo:
       MFD.mid         <- manifiesto.id
       MFD.correlativo <- manifiesto.correlativo
       MFD.fechaLbl    <- manifiesto.fecha | chile_fmt('%d/%m/%Y')
       MFD.sinOt       <- sin_ot | tojson

   Se carga con `defer`: ejecuta despues de parsear el DOM y ANTES de
   DOMContentLoaded, o sea el mismo comportamiento que tenia inline al final
   del <body>. Sigue siendo un script clasico (no modulo), asi que las
   funciones declaradas aqui siguen siendo globales y los onclick="..." del
   HTML las encuentran igual.
   ========================================================================== */
var MFD = window.MFD || {};
var MID    = MFD.mid;
var SIN_OT = MFD.sinOt;

/* ── Expandir / colapsar productos de una factura (desktop + mobile) ── */
function toggleProductos(itemId) {
  // Desktop
  var prow = document.getElementById('prodrow-' + itemId);
  var chev = document.getElementById('chev-' + itemId);
  if (prow) {
    var open = prow.hasAttribute('hidden');
    if (open) { prow.removeAttribute('hidden'); } else { prow.setAttribute('hidden',''); }
    if (chev) chev.classList.toggle('open', open);
  }
  // Mobile
  var mrow = document.getElementById('mprodrow-' + itemId);
  var mchev = document.getElementById('mchev-' + itemId);
  if (mrow) {
    var mopen = mrow.hasAttribute('hidden');
    if (mopen) { mrow.removeAttribute('hidden'); } else { mrow.setAttribute('hidden',''); }
    if (mchev) mchev.classList.toggle('open', mopen);
  }
}

function cambiarEstado(mid, itemId, nuevoEstado, selectEl) {
  selectEl.disabled = true;
  fetch('/transporte/manifiestos/' + mid + '/items/' + itemId + '/estado', {
    method: 'PUT',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify({estado_entrega: nuevoEstado})
  })
  .then(function(r){ return r.json(); })
  .then(function(d) {
    selectEl.disabled = false;
    if (d.ok) {
      // Flash verde temporal en la fila
      var row = document.getElementById('row-' + itemId) ||
                document.getElementById('card-' + itemId);
      if (row) {
        row.style.transition = 'background .3s';
        row.style.background = '#e8f5e9';
        setTimeout(function(){ row.style.background = ''; }, 1000);
      }
      if (window.ilusToast) ilusToast('✓ Estado actualizado', { type: 'success' });
      actualizarResumen();
    } else {
      ilusToast(d.error || 'No se pudo guardar', { type: 'error' });
      location.reload();
    }
  })
  .catch(function() {
    selectEl.disabled = false;
    ilusToast('Error de conexión', { type: 'error' });
  });
}

async function abrirTrackingFedex(itemId) {
  // Trae el tracking actual (si existe) y abre prompt para asociar / actualizar.
  let cur = {};
  try {
    const r = await fetch('/transporte/api/items/' + itemId + '/tracking-fedex');
    cur = await r.json();
  } catch(e) {}
  const wasSet = !!(cur && cur.tracking_number);
  const placeholder = wasSet ? cur.tracking_number : 'Ej: 780123456789';
  // Sub con contexto (último poll si existe)
  let sub = '';
  if (!cur.fedex_configurado) {
    sub = '<div style="color:#b45309;background:#fff8e1;padding:.5rem .7rem;border-radius:8px;'
        + 'font-size:.78rem;border:1px solid #fcd34d">'
        + '<i class="bi bi-exclamation-triangle-fill"></i> FedEx Track API aún no está configurada '
        + 'en el servidor. El nº se guarda pero el estado no se actualizará solo.</div>';
  } else if (wasSet) {
    sub = '<div style="font-size:.78rem;color:#6b7280">Último poll: '
        + (cur.last_poll || '—') + ' · Estado FedEx: <strong>'
        + (cur.last_status || '—') + '</strong></div>';
  }
  const tn = await ilusPrompt({
    title: wasSet ? 'Actualizar tracking FedEx' : 'Asociar tracking FedEx',
    message: 'Pega el número de seguimiento (Tracking Number) que te dio FedEx. '
           + 'Lo consultamos en vivo y actualizamos el estado de la entrega solo.',
    sub: sub,
    subHtml: !!sub,
    placeholder: placeholder,
    okLabel: wasSet ? 'Actualizar' : 'Asociar y consultar',
    inputValue: wasSet ? cur.tracking_number : '',
  });
  if (tn === null || tn === undefined) return;   // canceló
  const value = String(tn || '').replace(/[^A-Za-z0-9]/g, '');
  if (value.length < 10) {
    if (window.ilusToast) ilusToast('El tracking debe tener al menos 10 caracteres', { type: 'warning' });
    return;
  }
  let res;
  try {
    const resp = await fetch('/transporte/api/items/' + itemId + '/tracking-fedex', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ tracking_number: value })
    });
    res = await resp.json();
  } catch(e) {
    if (window.ilusToast) ilusToast('Error de conexión', { type: 'error' });
    return;
  }
  if (!res || !res.ok) {
    if (window.ilusToast) ilusToast((res && res.error) || 'No se pudo guardar', { type: 'error' });
    return;
  }
  if (res.warning) {
    if (window.ilusToast) ilusToast(res.warning, { type: 'warning' });
    return;
  }
  let msg = '✓ Tracking guardado';
  if (res.changed) msg += ' · Estado actualizado a "' + (res.estado || '?') + '"';
  if (window.ilusToast) ilusToast(msg, { type: 'success' });
  // Si el estado cambió, recargar para que se vea el nuevo
  if (res.changed) setTimeout(function(){ location.reload(); }, 1000);
}

// 2026-07-26: se eliminó abrirAsignarChofer() + el listener delegado .ch-pick
// (pedido explícito de Daniel, reiterado — botón "Asignar chofer" fuera del
// menú del manifiesto). Ver nota junto al hero-actions más arriba.

async function mostrarLinkCliente(itemId) {
  // Pide el link público al backend (lazy-genera el token si aún no existe)
  // y lo muestra en un ilusAlert con botón "Copiar". El link sobrevive si
  // la factura se mueve a otro manifiesto (vive a nivel de commitment).
  let data;
  try {
    const resp = await fetch('/transporte/api/items/' + itemId + '/link-cliente');
    data = await resp.json();
  } catch (e) {
    if (window.ilusToast) ilusToast('Error de conexión', { type: 'error' });
    return;
  }
  if (!data || !data.ok) {
    if (window.ilusToast) ilusToast(data && data.error || 'No se pudo obtener el link', { type: 'error' });
    return;
  }
  const url = data.url || '';
  const safe = String(url).replace(/[&<>"']/g, function(c){
    return ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'})[c];
  });
  // Modal con el link + botón copiar + nota de "compártelo con el cliente"
  await ilusAlert({
    title: 'Link de seguimiento del cliente',
    message: 'Comparte este link con el cliente para que siga su despacho en tiempo real:',
    sub: '<div style="margin:.5rem 0;padding:.6rem .7rem;background:#f3f4f6;border-radius:8px;'
       + 'font-family:ui-monospace,Consolas,monospace;font-size:.78rem;word-break:break-all;border:1px solid #e5e7eb">'
       + safe + '</div>'
       + '<div style="font-size:.78rem;color:#6b7280;margin-top:.4rem">'
       + '<i class="bi bi-shield-check"></i> El link es privado: solo quien lo tenga puede ver el estado de '
       + '<strong>' + (data.doc || 'esta factura') + '</strong>. No expone datos sensibles del cliente.'
       + '</div>',
    subHtml: true,
    type: 'info',
    okLabel: 'Copiar link',
  });
  // Tras cerrar el modal, copiar al portapapeles
  try {
    await navigator.clipboard.writeText(url);
    if (window.ilusToast) ilusToast('✓ Link copiado al portapapeles', { type: 'success' });
  } catch (e) {
    await ilusPrompt({
      title: 'Copiar link manualmente',
      message: 'El navegador bloqueó el portapapeles. Copia el enlace con Ctrl+C.',
      defaultValue: url,
      okLabel: 'Listo',
      cancelLabel: 'Cerrar',
      required: false,
    });
  }
}

// FIX 2026-07-27 (pedido Daniel): "actualizar estado" y "ver trazabilidad"
// para facturas de couriers no-FedEx (Felca/Milling vía SimpliRoute) — mismo
// endpoint genérico que ya usa el modal de detalle del monitor
// (templates/transporte/index.html), no se duplica la lógica de consulta
// al courier.
async function actualizarEstadoItemGenerico(cid, btn) {
  if (btn) { btn.disabled = true; btn.classList.add('disabled'); }
  ilusToast('Consultando al courier…', { type: 'info' });
  try {
    var r = await fetch('/transporte/api/compromisos/' + cid + '/actualizar-estado', { method: 'POST' });
    var d = await r.json();
    if (!d.ok) {
      ilusToast(d.error || 'No se pudo consultar el estado', { type: 'error' });
      return;
    }
    var msg = d.actualizado
      ? '✓ Estado actualizado: ' + d.estado_actual
      : (d.mensaje || ('Sin cambios · estado actual: ' + d.estado_actual));
    ilusToast(msg, { type: d.actualizado ? 'success' : 'info' });
    if (d.actualizado) setTimeout(function(){ location.reload(); }, 1200);
  } catch (e) {
    ilusToast('Error de conexión', { type: 'error' });
  } finally {
    if (btn) { btn.disabled = false; btn.classList.remove('disabled'); }
  }
}

async function abrirTrazabilidadItemGenerico(cid) {
  let data;
  try {
    const resp = await fetch('/transporte/api/compromisos/' + cid + '/trazabilidad');
    data = await resp.json();
  } catch (e) {
    if (window.ilusToast) ilusToast('Error de conexión', { type: 'error' });
    return;
  }
  if (!data || !data.ok) {
    if (window.ilusToast) ilusToast((data && data.error) || 'No se pudo cargar la trazabilidad', { type: 'error' });
    return;
  }
  var esc = function(s){
    return String(s == null ? '' : s).replace(/[&<>"']/g, function(c){
      return ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'})[c];
    });
  };
  var eventos = data.eventos || [];
  var lista = eventos.length
    ? eventos.map(function(ev){
        return '<div style="padding:.4rem 0;border-bottom:1px solid #e5e7eb">'
          + '<div style="font-weight:700;font-size:.82rem">' + esc(ev.accion) + '</div>'
          + (ev.detalle ? '<div style="font-size:.78rem;color:#374151">' + esc(ev.detalle) + '</div>' : '')
          + '<div style="font-size:.7rem;color:#9ca3af">' + esc(ev.created_at) + (ev.usuario ? ' · ' + esc(ev.usuario) : '') + '</div>'
          + '</div>';
      }).join('')
    : '<div style="font-size:.8rem;color:#6b7280">Sin eventos registrados todavía.</div>';
  var linkHtml = data.tracking_url
    ? '<div style="margin:.3rem 0 .8rem;padding:.5rem .6rem;background:#f3f4f6;border-radius:8px;'
      + 'font-family:ui-monospace,Consolas,monospace;font-size:.74rem;word-break:break-all;border:1px solid #e5e7eb">'
      + esc(data.tracking_url) + '</div>'
    : '';
  await ilusAlert({
    title: 'Seguimiento y trazabilidad',
    message: data.tracking_url ? 'Link público que ve el cliente:' : 'Historial de este pedido:',
    sub: linkHtml + '<div style="max-height:280px;overflow-y:auto">' + lista + '</div>',
    subHtml: true,
    type: 'info',
  });
}

async function quitarItem(mid, itemId) {
  const ok = await ilusConfirm({
    title: 'Quitar factura del manifiesto',
    message: '¿Quitar esta factura del manifiesto?',
    sub: 'El compromiso volverá a estar disponible para asignar.',
    okLabel: 'Sí, quitar', danger: true,
  });
  if (!ok) return;
  fetch('/transporte/manifiestos/' + mid + '/items/' + itemId, {method:'DELETE'})
    .then(function(r){ return r.json(); })
    .then(function(d) {
      if (d.ok) {
        if (window.ilusToast) ilusToast('✓ Factura quitada · actualizando montos…', { type: 'success' });
        // Recargar para que TODOS los montos (cobrado, costo, margen, bultos,
        // KPIs y la columna costo_total) queden recalculados sin desfase.
        setTimeout(function(){ window.location.reload(); }, 600);
      } else {
        if (window.ilusToast) ilusToast(d.error || 'No se pudo quitar', { type: 'error' });
      }
    });
}

function actualizarResumen() {
  // Re-count estados desde los selects actuales
  var selects = document.querySelectorAll('.estado-select');
  var counts = {};
  selects.forEach(function(s){ counts[s.value] = (counts[s.value]||0)+1; });
  var total = selects.length;
  // Actualizar badge
  var subtitles = document.querySelectorAll('.sub');
  // simple: just reload page for accuracy
  // Could do a lightweight DOM update here, but reload keeps it simple
}

function guardarEstadoManifiesto() {
  var estado = document.getElementById('selectEstadoMan').value;
  var msg    = document.getElementById('estadoManMsg');
  fetch('/transporte/manifiestos/' + MID + '/estado', {
    method:'PUT',
    headers:{'Content-Type':'application/json'},
    body: JSON.stringify({estado: estado})
  })
  .then(function(r){ return r.json(); })
  .then(function(d){
    if (d.ok) {
      msg.innerHTML = '<span class="text-success"><i class="bi bi-check-circle me-1"></i>Guardado</span>';
      setTimeout(function(){ location.reload(); }, 600);
    } else {
      msg.innerHTML = '<span class="text-danger">' + (d.error||'Error') + '</span>';
    }
  });
}

// ── Editar contacto + medidas de un ítem (alimenta el export de carga masiva) ──
var _edModal = null;

// Normaliza texto sin acentos para comparar comunas (Ñuñoa == Nunoa).
function _edNorm(s){ return (s||'').normalize('NFD').replace(/[̀-ͯ]/g,'').toLowerCase().trim(); }
function _edEsc(s){ return (s||'').replace(/[&<>"]/g, function(c){ return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]; }); }

// Parsea address_components de Google Places y rellena comuna/región/CP.
// onlyEmpty=true (auto-verificar): solo escribe en los campos vacíos, respeta
// lo que el operador ya puso. onlyEmpty falsy (elegir sugerencia): sobrescribe.
// Devuelve {comuna, region, cp} para que el caller pueda comparar coherencia.
function _edParseComponentes(componentes, onlyEmpty) {
  if (!Array.isArray(componentes)) return null;
  var loc = '', l3 = '', l2 = '', region = '', cp = '';
  componentes.forEach(function(c){
    var t = c.types || [], n = c.long_name || '';
    if (t.indexOf('locality') !== -1 && !loc) loc = n;
    if (t.indexOf('administrative_area_level_3') !== -1 && !l3) l3 = n;
    if (t.indexOf('administrative_area_level_2') !== -1 && !l2) l2 = n;
    if (t.indexOf('administrative_area_level_1') !== -1) region = n;
    if (t.indexOf('postal_code') !== -1) cp = n;
  });
  // Comuna en Chile: ni 'locality' ni 'level_3' son confiables por sí solos
  // (Providencia → locality correcto; Viña del Mar → a veces level_3). Heurística:
  // preferir el que NO sea la provincia (level_2), comparando sin acentos.
  function _norm(s){ return (s||'').normalize('NFD').replace(/[\u0300-\u036f]/g,'').toLowerCase().trim(); }
  var comuna = '';
  if (loc && _norm(loc) !== _norm(l2))      comuna = loc;
  else if (l3 && _norm(l3) !== _norm(l2))   comuna = l3;
  else                                      comuna = loc || l3 || l2;
  function _set(id, val){
    if (!val) return;
    var el = document.getElementById(id);
    if (!el) return;
    if (onlyEmpty && el.value && el.value.trim()) return;  // ya tiene dato: no lo pisamos
    el.value = val;
  }
  _set('edComuna',    comuna);
  _set('edRegion',    region);
  _set('edCodPostal', cp);
  return { comuna: comuna, region: region, cp: cp };
}

function _edInitPlaces() {
  var input = document.getElementById('edDireccion');
  var hint  = document.getElementById('edDirHint');
  if (!input || input.dataset.placesInit === '1') return;
  if (typeof ilusPlacesAutocomplete !== 'function') {
    if (hint) hint.innerHTML = '<i class="bi bi-info-circle"></i> Escribe la dirección lo más completa posible';
    return;
  }
  ilusPlacesAutocomplete(input, {
    country: 'cl',
    types: ['address'],
    onPlaceSelected: function(place){
      if (place && place.direccion) input.value = place.direccion;
      if (place && place.componentes) _edParseComponentes(place.componentes);
      // Guardar las COORDENADAS (2026-07-22): antes se descartaban y, al
      // guardar, las lat/lng del compromiso quedaban apuntando a la dirección
      // anterior. Ahora viajan en el PUT (ver guardarEdicion) y el backend
      // las persiste; si se edita a mano sin elegir sugerencia, se limpian.
      if (place && place.lat != null && place.lng != null) {
        input.dataset.lat     = place.lat;
        input.dataset.lng     = place.lng;
        input.dataset.placeId = place.place_id || '';
      }
      if (hint) hint.innerHTML = '<i class="bi bi-check-circle-fill" style="color:#16a34a"></i> '
                               + '<span style="color:#16a34a">Dirección verificada en Google</span>';
    },
    onNoSelection: function(){
      if (hint) hint.innerHTML = '<i class="bi bi-exclamation-triangle" style="color:#f59e0b"></i> '
                               + '<span style="color:#92400e">Elige una sugerencia para verificar la dirección</span>';
    }
  });
  // 2026-06-14 (Daniel) — EL VALIDADOR debe impedir comuna↔dirección
  // desincronizadas (caso real: dirección de Santiago con comuna TEMUCO →
  // cotiza la zona FedEx mal SIEMPRE). Si el operador EDITA la dirección a
  // mano (teclea, no elige sugerencia), las que dependen de la dirección
  // quedan inválidas: las LIMPIAMOS y exigimos elegir una sugerencia (que SÍ
  // arrastra comuna+región+CP juntos). Setear input.value por código (al
  // elegir sugerencia) NO dispara 'input', así que esto corre solo al teclear.
  input.addEventListener('input', function(){
    var elC = document.getElementById('edComuna');
    var elR = document.getElementById('edRegion');
    var elP = document.getElementById('edCodPostal');
    if (elC) elC.value = '';
    if (elR) elR.value = '';
    if (elP) elP.value = '';
    // Las coordenadas también quedan inválidas al teclear a mano: se limpian
    // para que el backend NO reciba las de la dirección anterior.
    input.dataset.lat = ''; input.dataset.lng = ''; input.dataset.placeId = '';
    input.dataset.addrDirty = '1';
    if (hint) hint.innerHTML = '<i class="bi bi-exclamation-triangle" style="color:#f59e0b"></i> '
                             + '<span style="color:#92400e">Elige una sugerencia para verificar '
                             + '(comuna y región se completan solas)</span>';
  });
  input.dataset.placesInit = '1';
}

// Auto-verifica con Google la dirección YA cargada al abrir el modal de edición.
// Rellena SOLO los campos vacíos (comuna/región/código postal) — los que alimentan
// el Excel de carga masiva (FedEx / SimplyRoute). No pisa lo que el operador ya puso.
function _edAutoVerificar() {
  var input = document.getElementById('edDireccion');
  var hint  = document.getElementById('edDirHint');
  if (!input || !input.value.trim()) return;          // sin dirección: nada que verificar
  if (input.dataset.autoVerif === '1') return;        // ya verificada en esta apertura
  var elC = document.getElementById('edComuna');
  var elR = document.getElementById('edRegion');
  var elP = document.getElementById('edCodPostal');
  // 2026-06-14 (Daniel): ya NO retornamos temprano si está completo.
  // Geocodificamos SIEMPRE (1 vez por apertura, gateado por autoVerif) para
  // detectar incoherencias comuna↔dirección en registros viejos guardados mal
  // (ej. dirección de Santiago con comuna TEMUCO → zona FedEx y precio mal).
  var comunaAlAbrir = (elC && elC.value || '').trim();
  if (!window.google || !window.google.maps || !window.google.maps.Geocoder) {
    // El SDK de Maps aún no cargó → reintenta cuando esté listo (mismo buffer que el autocomplete)
    if (window.__ilusGmapsPending) window.__ilusGmapsPending.push(_edAutoVerificar);
    return;
  }
  input.dataset.autoVerif = '1';
  if (hint) hint.innerHTML = '<i class="bi bi-arrow-repeat"></i> '
                           + '<span style="color:#6b7280">Verificando dirección con Google…</span>';
  try {
    new google.maps.Geocoder().geocode(
      { address: input.value.trim(), componentRestrictions: { country: 'cl' }, language: 'es' },
      function(results, status){
        if (status === 'OK' && results && results[0] && results[0].address_components) {
          var parsed = _edParseComponentes(results[0].address_components, true);  // rellena vacíos
          if (parsed && parsed.comuna && comunaAlAbrir &&
              _edNorm(comunaAlAbrir) !== _edNorm(parsed.comuna)) {
            // 🔴 INCOHERENCIA: la comuna cargada NO coincide con la dirección.
            if (hint) hint.innerHTML = '<i class="bi bi-exclamation-triangle-fill" style="color:#dc2626"></i> '
              + '<span style="color:#991b1b">La comuna <b>' + _edEsc(comunaAlAbrir) + '</b> no coincide con la '
              + 'dirección (parece <b>' + _edEsc(parsed.comuna) + '</b>). Elige una sugerencia para corregir '
              + 'y cotizar bien.</span>';
          } else {
            if (hint) hint.innerHTML = '<i class="bi bi-check-circle-fill" style="color:#16a34a"></i> '
                                     + '<span style="color:#16a34a">Dirección verificada con Google</span>';
          }
        } else {
          if (hint) hint.innerHTML = '<i class="bi bi-exclamation-triangle" style="color:#f59e0b"></i> '
                                   + '<span style="color:#92400e">No pudimos verificarla — escribe y elige una sugerencia</span>';
        }
      }
    );
  } catch(e) {
    if (hint) hint.innerHTML = '<i class="bi bi-info-circle"></i> Escribe y elige una sugerencia para autocompletar';
  }
}

function abrirEditar(data) {
  data = data || {};
  if (data.edicion_bloqueada) {
    ilusToast(
      data.edicion_motivo || 'La edición está bloqueada porque el pedido ya está en gestión con el courier.',
      { type: 'warning' }
    );
    return;
  }
  document.getElementById('edCid').value        = data.id || '';
  var _eItem = document.getElementById('edItemId');
  if (_eItem) _eItem.value = data.item_id || '';
  document.getElementById('edDoc').textContent  = data.doc || '';
  document.getElementById('edCliente').value    = data.cliente || '';
  document.getElementById('edTelefono').value   = data.telefono || '';
  document.getElementById('edEmail').value      = data.email || '';
  document.getElementById('edDireccion').value  = data.direccion || '';
  var _edRef = document.getElementById('edDireccionRef');
  if (_edRef) _edRef.value = data.direccion_referencia || '';
  document.getElementById('edComuna').value     = data.comuna || '';
  document.getElementById('edRegion').value     = data.region || '';
  document.getElementById('edCodPostal').value  = data.cod_postal || '';
  // Recotización (2026-07-29): guarda la comuna/courier de apertura para
  // detectar si REALMENTE cambió de zona al guardar — ver guardarEdicion().
  var _elComunaRc = document.getElementById('edComuna');
  if (_elComunaRc) {
    _elComunaRc.dataset.original = data.comuna || '';
    _elComunaRc.dataset.courier  = data.courier || '';
  }
  var _rcBox = document.getElementById('edRecotizaBox');
  if (_rcBox) _rcBox.style.display = 'none';
  document.getElementById('edPeso').value       = data.peso || '';
  document.getElementById('edBultos').value     = data.bultos || 1;
  // BLOQUEO DE BULTOS (Daniel 2026-07-22): con la OT FedEx ya emitida (y no
  // cancelada), los bultos quedan cerrados — cambiarlos acá descuadraría ILUS
  // vs FedEx, donde la guía ya salió con N etiquetas. El backend también lo
  // rechaza (409): esto es solo para que se vea claro y no se intente.
  (function _bloquearBultosSiOT(){
    var elB = document.getElementById('edBultos');
    if (!elB) return;
    var hint = document.getElementById('edBultosLock');
    var bloqueado = !!(data.tracking && !data.ot_cancelada);
    elB.disabled = bloqueado;
    elB.style.background = bloqueado ? '#f3f4f6' : '';
    elB.style.cursor     = bloqueado ? 'not-allowed' : '';
    elB.title = bloqueado
      ? 'Bloqueado: la OT FedEx ' + data.tracking + ' ya fue emitida con ' +
        (data.bultos || 1) + ' bulto(s). Para cambiarlos, cancela la OT en FedEx y re-emítela.'
      : '';
    if (hint){
      hint.style.display = bloqueado ? '' : 'none';
      hint.innerHTML = bloqueado
        ? '<i class="bi bi-lock-fill me-1"></i>Cerrado: OT ' + data.tracking + ' ya emitida'
        : '';
    }
  })();
  var _eTrack = document.getElementById('edTracking');
  if (_eTrack) { _eTrack.value = data.tracking || ''; _eTrack.dataset.original = data.tracking || ''; }
  document.getElementById('edMsg').innerHTML    = '';
  // Reset visual hint + estado de auto-verificación para esta apertura
  var hint = document.getElementById('edDirHint');
  if (hint) hint.innerHTML = '<i class="bi bi-info-circle"></i> Escribe y elige una sugerencia para autocompletar';
  var _edDir = document.getElementById('edDireccion');
  if (_edDir) {
    _edDir.dataset.autoVerif = '';
    // Coordenadas: arrancan vacías en cada apertura del modal. Solo se llenan
    // si el operador elige una sugerencia de Google AHORA (ver _edInitPlaces).
    _edDir.dataset.lat = ''; _edDir.dataset.lng = ''; _edDir.dataset.placeId = '';
  }
  // Ficha read-only: pesos, tarifa, bultos
  _edFillFicha(data);
  // Botón "Ver seguimiento": solo si hay OT FedEx
  var verBtn = document.getElementById('edVerTracking');
  if (verBtn) verBtn.style.display = (data.tracking ? 'inline-flex' : 'none');

  if (!_edModal) _edModal = new bootstrap.Modal(document.getElementById('editarItemModal'));
  _edModal.show();
  // Inicializa Google Places (lazy — espera a que el modal esté visible)
  setTimeout(_edInitPlaces, 150);
  // Verifica con Google la dirección pre-cargada y completa comuna/región/CP vacíos
  setTimeout(_edAutoVerificar, 500);
}

function _fmtKg(n) { n = parseFloat(n) || 0; return n ? n.toFixed(1) + ' kg' : '—'; }
function _fmtMoney(n) { n = parseFloat(n) || 0; return n ? '$' + n.toLocaleString('es-CL') : '$0'; }

function _edFillFicha(data) {
  data = data || {};
  var real = parseFloat(data.peso_real) || 0;
  var vol  = parseFloat(data.peso_vol) || 0;
  var pred = parseFloat(data.peso_pred) || Math.max(real, vol);
  var cobrado = parseFloat(data.cobrado) || 0;
  var costo   = parseFloat(data.costo) || 0;
  var margen  = cobrado - costo;
  function setChip(id, val, cls) {
    var el = document.getElementById(id); if (!el) return;
    el.querySelector('.ed-chip-v').textContent = val;
    if (cls) el.querySelector('.ed-chip-v').style.color = cls;
  }
  setChip('edChipReal', _fmtKg(real));
  setChip('edChipVol', _fmtKg(vol));
  setChip('edChipPred', _fmtKg(pred));
  setChip('edChipBultos', (data.ship_bultos || data.bultos || 1) + ' bulto(s)');
  setChip('edChipCobrado', _fmtMoney(cobrado), '#16a34a');
  setChip('edChipCosto', _fmtMoney(costo));
  setChip('edChipMargen', _fmtMoney(margen), margen < 0 ? '#dc2626' : '#16a34a');
}

// ══════════════════════════════════════════════════════════════════════
//  TRACKING DETALLADO — modal premium con timeline FedEx
// ══════════════════════════════════════════════════════════════════════
var _trkModal = null;
window._trkItemId = null;
var _trkData = null;

var TRK_STEPS = [
  { label: 'En preparación',          icon: 'bi-clipboard-check' },
  { label: 'Entregado a transporte',  icon: 'bi-truck-flatbed' },
  { label: 'En ruta',                 icon: 'bi-truck' },
  { label: 'Entregado',               icon: 'bi-check-circle-fill' },
];

async function abrirTrackingDetalle(itemId, force) {
  if (!itemId) return;
  window._trkItemId = itemId;
  if (!_trkModal) _trkModal = new bootstrap.Modal(document.getElementById('trackingModal'));
  // Estado de carga
  document.getElementById('trkStateLabel').textContent = 'Consultando FedEx…';
  document.getElementById('trkDoc').textContent = '';
  document.getElementById('trkSub').textContent = '';
  document.getElementById('trkTn').textContent = '';
  document.getElementById('trkSteps').innerHTML = '';
  document.getElementById('trkFicha').innerHTML = '';
  document.getElementById('trkTabFedex').innerHTML = '<div class="trk-empty"><i class="bi bi-hourglass-split"></i> Cargando movimientos…</div>';
  document.getElementById('trkTabIlus').innerHTML = '';
  _trkModal.show();
  try {
    var r = await fetch('/transporte/api/items/' + itemId + '/tracking-detalle');
    var d = await r.json();
    if (!d.ok) {
      document.getElementById('trkStateLabel').textContent = 'Error';
      document.getElementById('trkTabFedex').innerHTML = '<div class="trk-empty">' + (d.error || 'No se pudo cargar') + '</div>';
      return;
    }
    _trkData = d;
    _trkRender(d);
  } catch(e) {
    document.getElementById('trkStateLabel').textContent = 'Error de conexión';
    document.getElementById('trkTabFedex').innerHTML = '<div class="trk-empty">No se pudo conectar.</div>';
  }
}

function _trkEsc(s) {
  return String(s == null ? '' : s).replace(/[&<>"]/g, function(c){
    return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c];
  });
}

function _trkRender(d) {
  document.getElementById('trkDoc').textContent = d.doc || '';
  document.getElementById('trkStateLabel').textContent = d.estado || '—';
  var stIcon = document.querySelector('#trkState i');
  if (stIcon) stIcon.className = 'bi ' + (d.estado_icon || 'bi-circle');
  var sub = [];
  if (d.cliente) sub.push(d.cliente);
  if (d.comuna) sub.push(d.comuna);
  if (d.region) sub.push(d.region);
  document.getElementById('trkSub').textContent = sub.join(' · ');
  document.getElementById('trkTn').textContent = d.tracking ? '#' + d.tracking : '';
  var fxLink = document.getElementById('trkFedexLink');
  if (fxLink) fxLink.href = d.tracking ? 'https://www.fedex.com/fedextrack/?trknbr=' + d.tracking : '#';

  // Stepper
  var cur = d.estado_step || 1;
  var danger = (d.estado === 'Entrega fallida' || d.estado === 'Problema' || d.estado === 'Devolución');
  var stepsHtml = '';
  TRK_STEPS.forEach(function(s, i) {
    var n = i + 1;
    var cls = '';
    if (n < cur) cls = 'done';
    else if (n === cur) cls = danger ? 'danger' : 'active';
    stepsHtml += '<div class="trk-step ' + cls + '">'
      + (i < TRK_STEPS.length - 1 ? '<div class="trk-step-line"></div>' : '')
      + '<div class="trk-step-dot"><i class="bi ' + s.icon + '"></i></div>'
      + '<div class="trk-step-label">' + s.label + '</div></div>';
  });
  document.getElementById('trkSteps').innerHTML = stepsHtml;

  // Ficha pesos/tarifa/bultos
  var p = d.pesos || {}, t = d.tarifa || {};
  var fichaHtml = ''
    + _trkFchip('Peso real', _fmtKg(p.real))
    + _trkFchip('Peso vol.', _fmtKg(p.vol))
    + _trkFchip('★ Predominante', _fmtKg(p.predominante), 'pred')
    + _trkFchip('Bultos', (d.bultos || 1) + '')
    + _trkFchip('Cobrado', _fmtMoney(t.cobrado), 'ok')
    + _trkFchip('Costo', _fmtMoney(t.costo))
    + _trkFchip('Margen', _fmtMoney(t.margen), (t.margen||0) < 0 ? 'bad' : 'ok');
  if (d.eta) fichaHtml += _trkFchip('ETA FedEx', d.eta.replace('T',' ').slice(0,16));
  document.getElementById('trkFicha').innerHTML = fichaHtml;

  // Timeline FedEx scans
  var fxHtml = '';
  if (d.fedex_scans && d.fedex_scans.length) {
    d.fedex_scans.forEach(function(s) {
      var meta = [];
      if (s.fecha_txt) meta.push('<span><i class="bi bi-clock me-1"></i>' + _trkEsc(s.fecha_txt) + '</span>');
      if (s.ubicacion) meta.push('<span class="loc"><i class="bi bi-geo-alt me-1"></i>' + _trkEsc(s.ubicacion) + '</span>');
      fxHtml += '<div class="trk-ev"><div class="trk-ev-desc">' + _trkEsc(s.descripcion || '—')
              + '</div><div class="trk-ev-meta">' + meta.join('') + '</div></div>';
    });
  } else {
    fxHtml = '<div class="trk-empty"><i class="bi bi-broadcast"></i><br>FedEx aún no reporta movimientos para este envío.<br>'
           + '<small>Aparecerán acá apenas el courier escanee el paquete.</small></div>';
  }
  document.getElementById('trkTabFedex').innerHTML = fxHtml;

  // Timeline ILUS
  var ilusHtml = '';
  if (d.eventos && d.eventos.length) {
    d.eventos.forEach(function(e) {
      var meta = [];
      if (e.ts) meta.push('<span><i class="bi bi-clock me-1"></i>' + _trkEsc(e.ts.replace('T',' ')) + '</span>');
      meta.push('<span><i class="bi bi-' + (e.fuente === 'fedex' ? 'broadcast' : (e.fuente === 'chofer' ? 'person-badge' : 'gear')) + ' me-1"></i>' + _trkEsc(e.fuente) + '</span>');
      ilusHtml += '<div class="trk-ev"><div class="trk-ev-desc"><i class="bi ' + (e.icon || 'bi-circle')
                + ' me-1"></i>' + _trkEsc(e.estado) + '</div>'
                + (e.comentario ? '<div class="trk-ev-meta"><span>' + _trkEsc(e.comentario) + '</span></div>' : '')
                + '<div class="trk-ev-meta">' + meta.join('') + '</div></div>';
    });
  } else {
    ilusHtml = '<div class="trk-empty">Sin eventos registrados todavía.</div>';
  }
  document.getElementById('trkTabIlus').innerHTML = ilusHtml;
}

function _trkFchip(k, v, cls) {
  return '<div class="trk-fchip ' + (cls || '') + '"><span class="trk-fchip-k">'
       + k + '</span><span class="trk-fchip-v">' + _trkEsc(v) + '</span></div>';
}

function trkSwitchTab(which) {
  document.querySelectorAll('.trk-tab').forEach(function(t){
    t.classList.toggle('is-active', t.dataset.tab === which);
  });
  document.getElementById('trkTabFedex').style.display = (which === 'fedex' ? 'block' : 'none');
  document.getElementById('trkTabIlus').style.display = (which === 'ilus' ? 'block' : 'none');
}

// ══════════════════════════════════════════════════════════════════════
//  MODAL SIMPLIROUTE — "Ver seguimiento y acciones" (Felca/Milling)
//  Consolida actualizar-estado + trazabilidad + acciones reales en 1 modal
//  (2026-07-27). Reusa los endpoints genéricos/generic existentes — no
//  duplica la llamada HTTP al courier.
// ══════════════════════════════════════════════════════════════════════
// FIX 2026-07-27 (Daniel: "el botón de seguimiento no abre ningún modal"):
// esta variable se llamaba _srModal, IGUAL que la del modal viejo "Subir a
// SimpliRoute" (más abajo, ~línea 3489). Al ser ambas "var" en el mismo
// scope global, la que se cargaba/asignaba después pisaba a la otra — quien
// hiciera clic en "Ver seguimiento y acciones" después de haber abierto (o
// cargado) el otro modal terminaba mostrando/reusando la instancia
// equivocada de Bootstrap. Se renombra a _srAccionesModal, único.
var _srAccionesModal = null;
var _srLoadToken = 0;
window._srItemData = null;

function _srEsc(s) {
  return String(s == null ? '' : s).replace(/[&<>"']/g, function(c){
    return ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'})[c];
  });
}

function srSwitchTab(which) {
  document.querySelectorAll('#simpliRouteModal .trk-tab').forEach(function(t){
    t.classList.toggle('is-active', t.dataset.srtab === which);
  });
  document.getElementById('srTabTimeline').style.display = (which === 'timeline' ? 'block' : 'none');
  document.getElementById('srTabAcciones').style.display = (which === 'acciones' ? 'block' : 'none');
}

function _srAplicarPoliticaEdicion(data) {
  var bloqueada = !!data.edicion_bloqueada;
  var motivo = data.edicion_motivo
    || 'Edición bloqueada: la factura ya está en gestión con el courier (OT/visita creada).';
  var btn = document.getElementById('srBtnEditar');
  var aviso = document.getElementById('srEditLock');
  var texto = document.getElementById('srEditLockText');

  if (btn) {
    btn.disabled = bloqueada;
    btn.setAttribute('aria-disabled', bloqueada ? 'true' : 'false');
    btn.title = bloqueada ? motivo : 'Editar contacto y bultos';
  }
  if (aviso) aviso.hidden = !bloqueada;
  if (texto) texto.textContent = motivo;
}

// Un evento del historial → HTML (factorizado para reusar en "ver todos")
function _srEvHtml(ev) {
  return '<div class="trk-ev"><div class="trk-ev-desc">' + _srEsc(ev.accion) + '</div>'
       + (ev.detalle ? '<div class="trk-ev-meta"><span>' + _srEsc(ev.detalle) + '</span></div>' : '')
       + '<div class="trk-ev-meta"><span><i class="bi bi-clock me-1"></i>' + _srEsc(ev.created_at) + '</span>'
       + (ev.usuario ? '<span><i class="bi bi-person me-1"></i>' + _srEsc(ev.usuario) + '</span>' : '') + '</div></div>';
}

// Rediseño 2026-07-29 (Daniel: "tengo que darle mucho scroll... el estado
// contenido abajito con un scroll aparte"): el historial vive en un
// contenedor con scroll propio Y ADEMÁS se colapsa a los primeros eventos
// si la lista es larga — el botón "Ver todos" los despliega sin salir del
// modal. Escala bien aunque el poller vuelva a generar muchos eventos.
var _SR_EV_COLAPSE = 6;
window._srEventosCache = [];

function srMostrarTodosEventos() {
  var evs = window._srEventosCache || [];
  document.getElementById('srTimeline').innerHTML = evs.map(_srEvHtml).join('');
}

function srCopiarLink(btn) {
  var url = btn.dataset.url || '';
  if (!url) return;
  var done = function () {
    btn.innerHTML = '<i class="bi bi-check-lg me-1"></i>Copiado';
    ilusToast('✓ Link de seguimiento copiado', { type: 'success' });
    setTimeout(function(){ btn.innerHTML = '<i class="bi bi-clipboard me-1"></i>Copiar'; }, 2000);
  };
  if (navigator.clipboard && navigator.clipboard.writeText) {
    navigator.clipboard.writeText(url).then(done).catch(function(){
      ilusToast('No se pudo copiar el link', { type: 'error' });
    });
  } else {
    // Fallback (contexto no seguro): textarea temporal + execCommand
    var ta = document.createElement('textarea');
    ta.value = url; document.body.appendChild(ta); ta.select();
    try { document.execCommand('copy'); done(); }
    catch (e) { ilusToast('No se pudo copiar el link', { type: 'error' }); }
    ta.remove();
  }
}

async function _srCargarTrazabilidad(data, token, force) {
  try {
    var r = await fetch(
      '/transporte/api/compromisos/' + data.id + '/trazabilidad',
      { cache: force ? 'reload' : 'default' }
    );
    var d = await r.json();
    if (token !== _srLoadToken) return;
    if (!d || !d.ok) {
      document.getElementById('srTimeline').innerHTML = '<div class="trk-empty">'
        + _srEsc((d && d.error) || 'No se pudo cargar la trazabilidad') + '</div>';
      return;
    }
    if (d.tracking_url) {
      var lb = document.getElementById('srLinkBox');
      lb.style.display = '';
      lb.innerHTML =
        '<i class="bi bi-link-45deg sr-link-ico"></i>' +
        '<span class="sr-link-url">' + _srEsc(d.tracking_url) + '</span>' +
        '<button type="button" class="sr-copy-btn" data-url="' + _srEsc(d.tracking_url) + '" onclick="srCopiarLink(this)">' +
          '<i class="bi bi-clipboard me-1"></i>Copiar</button>';
    }
    // Estado EN VIVO de la visita en SimpliRoute (si el backend lo trae):
    // chip discreto en el hero — le dice al operador qué ve el courier AHORA.
    var live = d.simpliroute_live;
    var lc = document.getElementById('srLiveChip');
    if (lc && live && live.status) {
      var partes = ['<i class="bi bi-broadcast me-1"></i>SimpliRoute en vivo: <strong>' + _srEsc(live.status) + '</strong>'];
      if (live.planned_date) partes.push('plan ' + _srEsc(live.planned_date));
      if (live.driver) partes.push('chofer ' + _srEsc(live.driver));
      lc.innerHTML = partes.join(' · ');
      lc.style.display = '';
    }
    var eventos = d.eventos || [];
    window._srEventosCache = eventos;
    var cnt = document.getElementById('srEvCount');
    if (cnt) cnt.textContent = eventos.length ? String(eventos.length) : '';
    if (!eventos.length) {
      document.getElementById('srTimeline').innerHTML =
        '<div class="trk-empty">Sin eventos registrados todavía.</div>';
      return;
    }
    var html = eventos.slice(0, _SR_EV_COLAPSE).map(_srEvHtml).join('');
    if (eventos.length > _SR_EV_COLAPSE) {
      html += '<button type="button" class="sr-more-btn" onclick="srMostrarTodosEventos()">' +
        '<i class="bi bi-chevron-down me-1"></i>Ver los ' + eventos.length + ' movimientos</button>';
    }
    document.getElementById('srTimeline').innerHTML = html;
  } catch (e) {
    if (token === _srLoadToken) {
      document.getElementById('srTimeline').innerHTML = '<div class="trk-empty">No se pudo conectar.</div>';
    }
  }
}

// Lightbox simple para fotos/firmas de evidencia (Daniel 2026-07-28: "que la
// abriera en un modal" en vez de abrir una pestaña nueva del navegador).
// Reutilizado por la evidencia de SimpliRoute y por "Prueba de entrega".
// FIX 2026-07-29 (Daniel, en vivo viendo "Prueba de entrega": "quiero que
// las imágenes tengan un menú potente... zoom, pasar entre imágenes,
// voltear, algo profesional"). Reemplaza el lightbox de una sola imagen
// por uno con navegación (flechas/teclado), zoom (botones/rueda/doble
// click) y rotar. Backward-compatible: si se llama con un solo string
// (como antes), se envuelve en un array de 1 elemento.
function _ilusLightbox(images, startIdx, altBase) {
  if (typeof images === 'string') images = [images];
  images = (images || []).filter(Boolean);
  if (!images.length) return;
  var idx = Math.max(0, Math.min(startIdx || 0, images.length - 1));
  var zoom = 1, rot = 0;
  var multi = images.length > 1;

  var ov = document.createElement('div');
  ov.className = 'ilus-lightbox-ov';
  ov.innerHTML =
    '<button type="button" class="ilus-lightbox-close" aria-label="Cerrar">&times;</button>' +
    (multi ? '<button type="button" class="ilus-lb-nav ilus-lb-prev" aria-label="Anterior"><i class="bi bi-chevron-left"></i></button>' : '') +
    (multi ? '<button type="button" class="ilus-lb-nav ilus-lb-next" aria-label="Siguiente"><i class="bi bi-chevron-right"></i></button>' : '') +
    (multi ? '<div class="ilus-lb-counter"></div>' : '') +
    '<div class="ilus-lb-stage"><img class="ilus-lb-img" src="" alt=""></div>' +
    '<div class="ilus-lb-toolbar">' +
      '<button type="button" class="ilus-lb-tool" data-act="zoom-out" title="Alejar"><i class="bi bi-zoom-out"></i></button>' +
      '<span class="ilus-lb-zoom-pct">100%</span>' +
      '<button type="button" class="ilus-lb-tool" data-act="zoom-in" title="Acercar"><i class="bi bi-zoom-in"></i></button>' +
      '<button type="button" class="ilus-lb-tool" data-act="rotate" title="Girar 90°"><i class="bi bi-arrow-clockwise"></i></button>' +
      '<button type="button" class="ilus-lb-tool" data-act="reset" title="Restablecer"><i class="bi bi-aspect-ratio"></i></button>' +
    '</div>';

  var img     = ov.querySelector('.ilus-lb-img');
  var counter = ov.querySelector('.ilus-lb-counter');
  var zoomPct = ov.querySelector('.ilus-lb-zoom-pct');

  function applyTransform() {
    img.style.transform = 'scale(' + zoom + ') rotate(' + rot + 'deg)';
    zoomPct.textContent = Math.round(zoom * 100) + '%';
  }
  function render() {
    img.src = images[idx];
    img.alt = (altBase || 'Evidencia') + (multi ? ' ' + (idx + 1) : '');
    zoom = 1; rot = 0; applyTransform();
    if (counter) counter.textContent = (idx + 1) + ' / ' + images.length;
  }
  function go(delta) { idx = (idx + delta + images.length) % images.length; render(); }
  function cerrar() { ov.remove(); document.removeEventListener('keydown', onKey); }
  function onKey(e) {
    if (e.key === 'Escape') cerrar();
    else if (multi && e.key === 'ArrowLeft')  go(-1);
    else if (multi && e.key === 'ArrowRight') go(1);
    else if (e.key === '+') { zoom = Math.min(zoom * 1.25, 5);   applyTransform(); }
    else if (e.key === '-') { zoom = Math.max(zoom / 1.25, .3); applyTransform(); }
  }

  ov.addEventListener('click', function (e) {
    if (e.target === ov || e.target.classList.contains('ilus-lightbox-close')) { cerrar(); return; }
    var nav = e.target.closest('.ilus-lb-nav');
    if (nav) { go(nav.classList.contains('ilus-lb-prev') ? -1 : 1); return; }
    var tool = e.target.closest('.ilus-lb-tool');
    if (tool) {
      var act = tool.dataset.act;
      if (act === 'zoom-in')       zoom = Math.min(zoom * 1.25, 5);
      else if (act === 'zoom-out') zoom = Math.max(zoom / 1.25, .3);
      else if (act === 'rotate')   rot  = (rot + 90) % 360;
      else if (act === 'reset')    { zoom = 1; rot = 0; }
      applyTransform();
    }
  });
  img.addEventListener('dblclick', function () { zoom = (zoom === 1) ? 2 : 1; applyTransform(); });
  ov.addEventListener('wheel', function (e) {
    e.preventDefault();
    zoom = Math.max(.3, Math.min(5, zoom * (e.deltaY < 0 ? 1.1 : .9)));
    applyTransform();
  }, { passive: false });

  document.addEventListener('keydown', onKey);
  render();
  (document.querySelector('.modal.show') || document.body).appendChild(ov);
}

// FIX 2026-07-29 (Daniel: "cuando vea el mapa, lo mismo... no me gusta que
// abra otras pestañas... que esto quede concentrado en un modal"). Antes,
// el GPS del chofer/entrega/eventos abría Google Maps en target="_blank".
// Ahora se embebe (iframe sin API key, formato /maps?...&output=embed) en
// un modal propio, apilado sobre el modal de "Prueba de entrega" (mismo
// patrón de z-index:1090 que ya usa #trazaModal sobre #vistaModal).
var _ilusMapModalInst = null;
function _ilusMapModal(lat, lng, label) {
  if (!document.getElementById('ilusMapModal')) {
    var wrap = document.createElement('div');
    wrap.className = 'modal fade';
    wrap.id = 'ilusMapModal';
    wrap.tabIndex = -1;
    wrap.style.zIndex = '1090';
    wrap.innerHTML =
      '<div class="modal-dialog modal-lg modal-dialog-centered">' +
        '<div class="modal-content" style="border-radius:14px;overflow:hidden">' +
          '<div class="modal-header" style="background:#0a0a0a;color:#fff;border:none;padding:12px 18px">' +
            '<h6 class="modal-title mb-0" id="ilusMapModalTitle"></h6>' +
            '<button type="button" class="btn-close btn-close-white" data-bs-dismiss="modal" aria-label="Cerrar"></button>' +
          '</div>' +
          '<div class="modal-body p-0"><iframe id="ilusMapModalFrame" style="width:100%;height:60vh;border:0" loading="lazy"></iframe></div>' +
          '<div class="modal-footer" style="padding:10px 18px">' +
            '<a id="ilusMapModalOpenExt" href="#" target="_blank" rel="noopener" class="btn btn-sm btn-outline-dark">Abrir en Google Maps</a>' +
            '<button type="button" class="btn btn-sm btn-outline-secondary" data-bs-dismiss="modal">Cerrar</button>' +
          '</div>' +
        '</div>' +
      '</div>';
    document.body.appendChild(wrap);
  }
  document.getElementById('ilusMapModalTitle').innerHTML =
    '<i class="bi bi-geo-alt-fill me-2" style="color:#dc2626"></i>' + _srEsc(label || 'Ubicación');
  document.getElementById('ilusMapModalFrame').src =
    'https://www.google.com/maps?q=' + lat + ',' + lng + '&output=embed';
  document.getElementById('ilusMapModalOpenExt').href =
    'https://www.google.com/maps?q=' + lat + ',' + lng;
  if (!_ilusMapModalInst) _ilusMapModalInst = new bootstrap.Modal(document.getElementById('ilusMapModal'));
  _ilusMapModalInst.show();
}

// ── Hero: icono + color según estado (rediseño 2026-07-29) ──
var _SR_ESTADO_UI = {
  'En preparación':          { ico: 'bi-box-seam',                color: '#cbd5e1' },
  'Entregado a transporte':  { ico: 'bi-box-arrow-right',         color: '#fbbf24' },
  'En ruta':                 { ico: 'bi-truck',                   color: '#93c5fd' },
  'Entregado':               { ico: 'bi-check-circle-fill',       color: '#4ade80' },
  'Entrega fallida':         { ico: 'bi-x-octagon-fill',          color: '#f87171' },
  'Problema':                { ico: 'bi-exclamation-triangle-fill', color: '#f87171' },
  'Devolución':              { ico: 'bi-arrow-counterclockwise',  color: '#fca5a5' },
};
function _srAplicarEstadoHero(estado) {
  var ui = _SR_ESTADO_UI[estado] || { ico: 'bi-geo-alt-fill', color: '#e2e8f0' };
  var icoEl = document.getElementById('srStateIcon');
  var stEl  = document.getElementById('srState');
  if (icoEl) icoEl.className = 'bi ' + ui.ico;
  if (stEl)  stEl.style.color = ui.color;
  document.getElementById('srStateLabel').textContent = estado || '—';
}

// Formato humano de días: 0.4 d → "9 h", 1.0 → "1 día", 2.5 → "2,5 días"
function _srFmtDias(d) {
  if (d == null) return '—';
  if (d < 1) {
    var h = Math.round(d * 24);
    return h <= 1 ? '1 h' : h + ' h';
  }
  var s = (Math.round(d * 10) / 10).toLocaleString('es-CL');
  return s + (d === 1 ? ' día' : ' días');
}

// ── Detalle consolidado (rediseño 2026-07-29): un solo GET a
// /transporte/api/buscar/<cid> alimenta KPIs de tiempo, productos con foto,
// evidencia de entrega (firma+fotos+receptor) y chofer con GPS. Antes esta
// función (_srCargarEvidencia) solo pintaba firma/fotos.
async function _srCargarDetalle(data, token, force) {
  try {
    var r = await fetch(
      '/transporte/api/buscar/' + data.id,
      { cache: force ? 'reload' : 'default' }
    );
    var d = await r.json();
    if (token !== _srLoadToken) return;
    if (!d || !d.ok) return;
    var det = d.detalle || {};

    // ── Estado del hero: preferir el estado fresco del item ──
    var estadoFresco = (det.manifest_item && det.manifest_item.estado_entrega) || data.estado || '';
    if (estadoFresco) _srAplicarEstadoHero(estadoFresco);

    // ── KPI de tiempo ("ya, mira, estuvo tantos días") ──
    var t = det.tiempos || {};
    var kCard = document.getElementById('srKpiTiempoCard');
    var kLab  = document.getElementById('srKpiTiempoLabel');
    var kVal  = document.getElementById('srKpiTiempo');
    var kHint = document.getElementById('srKpiTiempoHint');
    kCard.classList.remove('sr-kpi-ok', 'sr-kpi-run');
    if (t.dias_entrega != null) {
      kLab.textContent  = 'Tiempo de entrega';
      kVal.textContent  = _srFmtDias(t.dias_entrega);
      kHint.textContent = t.entregado_at ? ('entregado ' + t.entregado_at) : '';
      kCard.classList.add('sr-kpi-ok');
    } else if (t.dias_en_courier != null) {
      kLab.textContent  = 'En manos del courier';
      kVal.textContent  = _srFmtDias(t.dias_en_courier);
      kHint.textContent = t.en_courier_at ? ('desde ' + t.en_courier_at) : '';
      kCard.classList.add('sr-kpi-run');
    } else {
      kLab.textContent  = 'Tiempo';
      kVal.textContent  = '—';
      kHint.textContent = 'aún sin retiro del courier';
    }
    if (t.ultima_act_at) document.getElementById('srKpiAct').textContent = t.ultima_act_at;

    // ── Productos del pedido (con foto → _ilusLightbox, sin pestañas) ──
    var lineas = det.lineas || [];
    var secP = document.getElementById('srSecProductos');
    if (lineas.length) {
      window._srProdFotos = lineas.map(function(l){ return l.fotos || []; });
      var pHtml = lineas.map(function(l, i) {
        var thumb = (l.fotos && l.fotos.length)
          ? '<img class="sr-prod-thumb" src="' + _srEsc(l.fotos[0]) + '" alt="' + _srEsc(l.nombre) + '" loading="lazy" ' +
            'onclick="_ilusLightbox(window._srProdFotos[' + i + '], 0, \'' + _srEsc((l.sku || 'Producto')).replace(/'/g, '') + '\')">'
          : '<div class="sr-prod-thumb-ph"><i class="bi bi-image"></i></div>';
        var chips = '<span class="sr-qty-chip" title="Cantidad del documento">×' + (l.cantidad || 0) + '</span>';
        if (l.despachada > 0 && l.despachada !== l.cantidad) {
          chips += '<span class="sr-qty-chip desp" title="Cantidad ya despachada">desp. ' + l.despachada + '</span>';
        }
        if (l.saldo > 0 && l.saldo !== l.cantidad) {
          chips += '<span class="sr-qty-chip saldo" title="Saldo pendiente en el ERP">saldo ' + l.saldo + '</span>';
        }
        return '<div class="sr-prod">' + thumb +
          '<div class="sr-prod-info"><div class="sr-prod-name">' + _srEsc(l.nombre || l.sku) + '</div>' +
          '<div class="sr-prod-sku">' + _srEsc(l.sku) + '</div></div>' +
          '<div class="sr-prod-qty">' + chips + '</div></div>';
      }).join('');
      document.getElementById('srProductos').innerHTML = pHtml;
      document.getElementById('srProdCount').textContent = String(lineas.length);
      secP.style.display = '';
    }

    // ── Evidencia de entrega: receptor + firma + fotos ──
    var p = det.proof;
    var secE = document.getElementById('srSecEvidencia');
    if (p && (p.firma_url || (p.fotos && p.fotos.length) || p.receptor_nombre)) {
      var html = '';
      if (p.receptor_nombre || p.entregado_at) {
        html += '<div class="sr-recibio">' +
          '<i class="bi bi-person-check-fill"></i><div>' +
          (p.receptor_nombre
            ? '<div class="sr-recibio-n">Recibió: ' + _srEsc(p.receptor_nombre) +
              (p.receptor_relacion ? ' <span class="sr-recibio-rel">(' + _srEsc(p.receptor_relacion) + ')</span>' : '') + '</div>'
            : '') +
          (p.entregado_at ? '<div class="sr-recibio-ts"><i class="bi bi-clock me-1"></i>' + _srEsc(p.entregado_at) + '</div>' : '') +
          '</div></div>';
      }
      if (p.firma_url) {
        html += '<div class="pe-firma"><img src="' + _srEsc(p.firma_url) + '" alt="Firma" onclick="_ilusLightbox(this.src, 0, \'Firma\')"></div>';
      }
      if (p.fotos && p.fotos.length) {
        window._peFotosActuales = p.fotos;   // mismo patron que _peRenderDetalle: array completo para navegar
        html += '<div class="pe-fotos mt-2">';
        p.fotos.forEach(function(f, i){
          html += '<img src="' + _srEsc(f) + '" alt="Foto de entrega" loading="lazy" onclick="_ilusLightbox(window._peFotosActuales, ' + i + ', \'Foto de entrega\')">';
        });
        html += '</div>';
      }
      // Punto GPS exacto de la entrega → mapa embebido (nunca pestaña nueva)
      if (p.lat && p.lng) {
        html += '<button type="button" class="sr-map-btn mt-2" ' +
          'onclick="_ilusMapModal(' + Number(p.lat) + ',' + Number(p.lng) + ', \'Punto de entrega\')">' +
          '<i class="bi bi-geo-alt-fill me-1"></i>Ver punto de entrega en el mapa</button>';
      }
      document.getElementById('srEvidencia').innerHTML = html;
      secE.style.display = '';
    }

    // ── Chofer + GPS en vivo (mapa embebido con _ilusMapModal) ──
    var ch = det.chofer;
    var secC = document.getElementById('srSecChofer');
    if (ch && ch.nombre) {
      var ping = det.last_ping;
      var ini = (ch.nombre || '?').trim().charAt(0).toUpperCase();
      var cHtml = '<div class="sr-driver-row">' +
        '<div class="sr-driver-avatar">' + _srEsc(ini) + '</div>' +
        '<div class="sr-driver-info"><div class="sr-driver-n">' + _srEsc(ch.nombre) + '</div>' +
        '<div class="sr-driver-meta">' +
        (ch.courier ? '<span><i class="bi bi-truck me-1"></i>' + _srEsc(ch.courier) + '</span>' : '') +
        (ch.patente ? '<span><i class="bi bi-credit-card-2-front me-1"></i>' + _srEsc(ch.patente) + '</span>' : '') +
        (ch.telefono ? '<span><i class="bi bi-telephone me-1"></i>' + _srEsc(ch.telefono) + '</span>' : '') +
        '</div></div>';
      if (ping && ping.lat && ping.lng) {
        var age = '';
        if (ping.age_s != null) {
          var mins = Math.round(Number(ping.age_s) / 60);
          age = mins < 1 ? 'ahora mismo' : (mins < 60 ? 'hace ' + mins + ' min' : 'hace ' + Math.round(mins / 60) + ' h');
        }
        cHtml += '<button type="button" class="sr-map-btn" ' +
          'onclick="_ilusMapModal(' + Number(ping.lat) + ',' + Number(ping.lng) + ', \'Chofer: ' +
          _srEsc(ch.nombre).replace(/'/g, '') + '\')">' +
          '<i class="bi bi-geo-alt-fill me-1"></i>Ver en el mapa' +
          (age ? ' <span class="sr-map-age">· GPS ' + age + '</span>' : '') + '</button>';
      }
      cHtml += '</div>';
      document.getElementById('srChofer').innerHTML = cHtml;
      secC.style.display = '';
    }
  } catch (e) {
    // El detalle es complementario y no bloquea el seguimiento.
  }
}

async function abrirSimpliRouteModal(data, force) {
  data = data || {};
  if (!data.id) return; // data.id = commitment_id
  var token = ++_srLoadToken;
  window._srItemData = data;
  if (!_srAccionesModal) _srAccionesModal = new bootstrap.Modal(document.getElementById('simpliRouteModal'));

  // ── Hero + KPIs (rediseño 2026-07-29): lo que ya sabemos del item se
  // pinta al instante; el resto (tiempos, productos, evidencia, chofer)
  // llega async y va rellenando sus secciones sin bloquear.
  document.getElementById('srDoc').textContent = data.doc || '';
  _srAplicarEstadoHero(data.estado || '');
  document.getElementById('srSub').textContent =
    (data.cliente || '') + (data.comuna ? ' · ' + data.comuna : '');
  var courierChip = document.getElementById('srCourier');
  if (data.courier) {
    courierChip.textContent = data.courier;
    courierChip.style.display = '';
  } else {
    courierChip.style.display = 'none';
  }
  document.getElementById('srKpiBultos').textContent = data.bultos || '—';
  document.getElementById('srKpiComuna').textContent = data.comuna || '—';
  document.getElementById('srKpiAct').textContent = '—';
  var kCard = document.getElementById('srKpiTiempoCard');
  kCard.classList.remove('sr-kpi-ok', 'sr-kpi-run');
  document.getElementById('srKpiTiempoLabel').textContent = 'Tiempo';
  document.getElementById('srKpiTiempo').textContent = '…';
  document.getElementById('srKpiTiempoHint').textContent = '';
  document.getElementById('srLiveChip').style.display = 'none';

  // ── Reset de secciones del tab Timeline ──
  document.getElementById('srTimeline').innerHTML = '<div class="trk-empty"><i class="bi bi-hourglass-split"></i> Cargando trazabilidad…</div>';
  document.getElementById('srEvCount').textContent = '';
  document.getElementById('srLinkBox').style.display = 'none';
  ['srSecProductos', 'srSecEvidencia', 'srSecChofer'].forEach(function(id){
    document.getElementById(id).style.display = 'none';
  });
  document.getElementById('srProductos').innerHTML = '';
  document.getElementById('srEvidencia').innerHTML = '';
  document.getElementById('srChofer').innerHTML = '';
  document.getElementById('srAccMsg').innerHTML = '';
  _srAplicarPoliticaEdicion(data);
  // Fecha por defecto del picker de reprogramación: hoy
  var fEl = document.getElementById('srFechaInput');
  if (fEl && !fEl.value) fEl.value = new Date().toISOString().slice(0, 10);
  srSwitchTab('timeline');
  _srAccionesModal.show();

  await Promise.allSettled([
    _srCargarTrazabilidad(data, token, !!force),
    _srCargarDetalle(data, token, !!force),
  ]);
}

async function srActualizarEstado() {
  var data = window._srItemData || {};
  if (!data.id) return;
  var btn = document.getElementById('srBtnActualizar');
  await actualizarEstadoItemGenerico(data.id, btn);
}

function srAbrirEditar() {
  var data = window._srItemData;
  if (!data) return;
  if (data.edicion_bloqueada) {
    ilusToast(
      data.edicion_motivo || 'La edición está bloqueada porque el pedido ya está en gestión con el courier.',
      { type: 'warning' }
    );
    return;
  }
  abrirEditar(data);
}

async function srCancelarVisita() {
  var data = window._srItemData || {};
  if (!data.item_id) return;
  var ok = await ilusConfirm({
    title: 'Cancelar visita SimpliRoute',
    message: '¿Cancelar la visita de este pedido en SimpliRoute?',
    sub: 'Se borra del lado del courier. Podrás reenviarla después si hace falta.',
    okLabel: 'Sí, cancelar', cancelLabel: 'No', danger: true,
  });
  if (!ok) return;
  var msg = document.getElementById('srAccMsg');
  var btn = document.getElementById('srBtnCancelar');
  btn.disabled = true;
  msg.innerHTML = '<span class="text-muted">Cancelando…</span>';
  try {
    var r = await fetch('/transporte/api/items/' + data.item_id + '/simpliroute/cancelar', { method: 'POST' });
    var d = await r.json();
    if (!d.ok) {
      msg.innerHTML = '<span class="text-danger">' + _srEsc(d.error || 'No se pudo cancelar') + '</span>';
      ilusToast(d.error || 'No se pudo cancelar la visita', { type: 'error' });
      return;
    }
    msg.innerHTML = '<span class="text-success"><i class="bi bi-check-circle me-1"></i>Visita cancelada</span>';
    ilusToast('✓ Visita cancelada en SimpliRoute', { type: 'success' });
  } catch (e) {
    msg.innerHTML = '<span class="text-danger">Error de conexión</span>';
    ilusToast('Error de conexión', { type: 'error' });
  } finally {
    btn.disabled = false;
  }
}

async function srReprogramar() {
  var data = window._srItemData || {};
  if (!data.item_id) return;
  var fecha = (document.getElementById('srFechaInput').value || '').trim();
  if (!fecha) { ilusToast('Elige una fecha', { type: 'warning' }); return; }
  var msg = document.getElementById('srAccMsg');
  var btn = document.getElementById('srBtnReprogramar');
  btn.disabled = true;
  msg.innerHTML = '<span class="text-muted">Reprogramando…</span>';
  try {
    var r = await fetch('/transporte/api/items/' + data.item_id + '/simpliroute/reprogramar', {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ fecha: fecha }),
    });
    var d = await r.json();
    if (!d.ok) {
      msg.innerHTML = '<span class="text-danger">' + _srEsc(d.error || 'No se pudo reprogramar') + '</span>';
      await ilusAlert({
        title: 'No se pudo reprogramar vía API',
        message: d.error || 'SimpliRoute rechazó el cambio de fecha.',
        type: 'warning',
      });
      return;
    }
    msg.innerHTML = '<span class="text-success"><i class="bi bi-check-circle me-1"></i>Reprogramada a ' + _srEsc(d.fecha) + '</span>';
    ilusToast('✓ Visita reprogramada', { type: 'success' });
  } catch (e) {
    msg.innerHTML = '<span class="text-danger">Error de conexión</span>';
    ilusToast('Error de conexión', { type: 'error' });
  } finally {
    btn.disabled = false;
  }
}

async function srReenviarVisita() {
  var data = window._srItemData || {};
  if (!data.item_id) return;
  var msg = document.getElementById('srAccMsg');
  var btn = document.getElementById('srBtnReenviar');
  btn.disabled = true;
  msg.innerHTML = '<span class="text-muted">Reenviando a SimpliRoute…</span>';
  try {
    var r = await fetch('/transporte/api/items/' + data.item_id + '/simpliroute/reenviar', { method: 'POST' });
    var d = await r.json();
    if (!d.ok) {
      msg.innerHTML = '<span class="text-danger">' + _srEsc(d.error || 'No se pudo reenviar') + '</span>';
      ilusToast(d.error || 'No se pudo reenviar la visita', { type: 'error' });
      return;
    }
    msg.innerHTML = '<span class="text-success"><i class="bi bi-check-circle me-1"></i>Visita creada (id ' + _srEsc(d.visit_id) + ')</span>';
    ilusToast('✓ Visita reenviada a SimpliRoute', { type: 'success' });
    setTimeout(function(){ window.location.reload(); }, 1200);
  } catch (e) {
    msg.innerHTML = '<span class="text-danger">Error de conexión</span>';
    ilusToast('Error de conexión', { type: 'error' });
  } finally {
    btn.disabled = false;
  }
}

// Recotización (2026-07-29): si el operador toca la comuna a mano, el
// cuadro de recotización que hubiera quedado de una edición anterior ya no
// aplica — se oculta hasta el próximo Guardar.
function _edComunaTocada() {
  var box = document.getElementById('edRecotizaBox');
  if (box) box.style.display = 'none';
}

function guardarEdicion() {
  var cid = document.getElementById('edCid').value;
  if (!cid) return;
  var btn = document.getElementById('edGuardarBtn');
  var msg = document.getElementById('edMsg');
  btn.disabled = true;
  var _elComuna = document.getElementById('edComuna');
  var _comunaOriginal = (_elComuna.dataset.original || '').trim();
  var _comunaNueva    = _elComuna.value.trim();
  var _courierItem    = _elComuna.dataset.courier || '';
  var payload = {
    telefono:    document.getElementById('edTelefono').value.trim(),
    email:       document.getElementById('edEmail').value.trim(),
    direccion:   document.getElementById('edDireccion').value.trim(),
    direccion_referencia: (document.getElementById('edDireccionRef') || {}).value || '',
    comuna:      _comunaNueva,
    region:      document.getElementById('edRegion').value.trim(),
    cod_postal:  document.getElementById('edCodPostal').value.trim(),
    peso_export: parseFloat(document.getElementById('edPeso').value) || 0,
    n_bultos:    parseInt(document.getElementById('edBultos').value, 10) || 1
  };
  // Coordenadas de Google (2026-07-22): solo si el operador eligió una
  // sugerencia en ESTA edición. Si no van, y la dirección cambió, el backend
  // invalida las viejas (no deja coords apuntando a la dirección anterior).
  var _edDirEl = document.getElementById('edDireccion');
  if (_edDirEl && _edDirEl.dataset.lat && _edDirEl.dataset.lng) {
    payload.direccion_lat      = _edDirEl.dataset.lat;
    payload.direccion_lng      = _edDirEl.dataset.lng;
    payload.direccion_place_id = _edDirEl.dataset.placeId || '';
  }
  fetch('/transporte/api/compromisos/' + cid, {
    method: 'PUT',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify(payload)
  })
  .then(function(r){ return r.json(); })
  .then(async function(d){
    if (!d.ok) {
      btn.disabled = false;
      msg.innerHTML = '<span class="text-danger">' + (d.error || 'No se pudo guardar') + '</span>';
      return;
    }
    // Si hay campo de OT FedEx y cambió, asociarlo (consulta estado en vivo).
    var trackMsg = '';
    var elT = document.getElementById('edTracking');
    var itemId = (document.getElementById('edItemId') || {}).value;
    if (elT && itemId) {
      var tnRaw = (elT.value || '').replace(/[^A-Za-z0-9]/g, '');
      var orig  = (elT.dataset.original || '').replace(/[^A-Za-z0-9]/g, '');
      if (tnRaw && tnRaw !== orig) {
        if (tnRaw.length < 10) {
          btn.disabled = false;
          msg.innerHTML = '<span class="text-danger">El Nº de OT FedEx debe tener al menos 10 caracteres.</span>';
          return;
        }
        try {
          var rt = await fetch('/transporte/api/items/' + itemId + '/tracking-fedex', {
            method: 'POST', headers: {'Content-Type':'application/json'},
            body: JSON.stringify({ tracking_number: tnRaw })
          });
          var dt = await rt.json();
          if (dt && dt.ok) {
            trackMsg = dt.changed ? ' · OT asociada, estado: ' + (dt.estado || '?')
                                  : ' · OT asociada';
            if (dt.warning) trackMsg = ' · ' + dt.warning;
          } else {
            trackMsg = ' · OT no se pudo asociar: ' + ((dt && dt.error) || 'error');
          }
        } catch(e) { trackMsg = ' · OT: error de conexión'; }
      }
    }
    btn.disabled = false;
    msg.innerHTML = '<span class="text-success"><i class="bi bi-check-circle me-1"></i>Guardado</span>';
    if (window.ilusToast) ilusToast('✓ Datos actualizados' + trackMsg, { type: 'success' });

    // Recotización (2026-07-29, Daniel: "si le voy a cambiar la dirección
    // por acá, me recotice... vamos a recotizar esto porque cambia según el
    // courier"). Solo si la comuna REALMENTE cambió y el envío ya tiene
    // courier asignado — si no, se recarga normal (comportamiento de antes).
    var _cambioComuna = _comunaNueva && _comunaOriginal
                       && _comunaNueva.toLowerCase() !== _comunaOriginal.toLowerCase();
    if (_cambioComuna && _courierItem) {
      try {
        var rr = await fetch('/transporte/api/compromisos/' + cid + '/recotizar-courier?comuna=' + encodeURIComponent(_comunaNueva));
        var dr = await rr.json();
        if (dr.ok) {
          window._edRecotizaCid = cid;
          window._edRecotizaPrecio = dr.precio_nuevo;
          var box = document.getElementById('edRecotizaBox');
          var txt = document.getElementById('edRecotizaTexto');
          if (box && txt) {
            var signo = dr.diferencia > 0 ? '+' : '';
            txt.innerHTML = '<i class="bi bi-info-circle me-1"></i>'
              + 'Nueva comuna <b>' + dr.comuna + '</b> con <b>' + dr.courier + '</b>: '
              + 'costo courier <b>$' + Math.round(dr.precio_nuevo).toLocaleString('es-CL') + '</b> '
              + '(antes $' + Math.round(dr.precio_actual).toLocaleString('es-CL') + ', '
              + signo + '$' + Math.round(dr.diferencia).toLocaleString('es-CL') + ')';
            box.style.display = '';
          }
          return; // deja el modal abierto para que el operador vea/aplique la recotización
        }
        // Sin tarifa para la comuna nueva u otro error de cotización: no
        // bloquea el guardado (la dirección YA se guardó bien), solo avisa.
        if (window.ilusToast) ilusToast('Dirección guardada. ' + (dr.error || 'No se pudo recotizar automáticamente.'), { type: 'warning' });
      } catch (e) { /* recotización es best-effort, no bloquea el guardado */ }
    }
    setTimeout(function(){ location.reload(); }, 700);
  })
  .catch(function(){
    btn.disabled = false;
    msg.innerHTML = '<span class="text-danger">Error de conexión</span>';
  });
}

async function _edAplicarRecotizacion() {
  var cid = window._edRecotizaCid;
  var nuevo = window._edRecotizaPrecio;
  if (!cid || nuevo == null) return;
  var btn = document.getElementById('edRecotizaBtn');
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner-border spinner-border-sm"></span>';
  try {
    var r = await fetch('/transporte/api/compromisos/' + cid, {
      method: 'PUT',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ costo_courier: nuevo }),
    });
    var d = await r.json();
    if (!d.ok) {
      if (window.ilusToast) ilusToast(d.error || 'No se pudo aplicar el nuevo costo', { type: 'error' });
      btn.disabled = false;
      btn.innerHTML = '<i class="bi bi-arrow-repeat me-1"></i>Aplicar nuevo costo';
      return;
    }
    if (window.ilusToast) ilusToast('✓ Costo courier actualizado', { type: 'success' });
    location.reload();
  } catch (e) {
    if (window.ilusToast) ilusToast('Error de conexión al aplicar la recotización', { type: 'error' });
    btn.disabled = false;
    btn.innerHTML = '<i class="bi bi-arrow-repeat me-1"></i>Aplicar nuevo costo';
  }
}

// ══════════════════════════════════════════════════════════════════════
//  ASIGNACIÓN MASIVA OTs FedEx
// ══════════════════════════════════════════════════════════════════════

var _otsMasivoModal = null;

function _esc(s) {
  return String(s || '').replace(/[&<>"']/g, function(c){
    return ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'})[c];
  });
}

function _renderOTsTable() {
  var tbody = document.getElementById('otsMasivoTbody');
  if (!tbody) return;
  // Update only the input values that the user hasn't touched yet
  // (don't reset inputs already filled by distribuirOTs or manual entry)
}

function abrirAsignarOtsMasivo() {
  if (!document.getElementById('otsMasivoModal')) return;
  if (!_otsMasivoModal) _otsMasivoModal = new bootstrap.Modal(document.getElementById('otsMasivoModal'));

  // Render table of items without OT (SIN_OT from Jinja)
  var rows = '';
  if (!SIN_OT || !SIN_OT.length) {
    rows = '<tr><td colspan="3" class="text-center text-success py-3">'
         + '<i class="bi bi-check-circle-fill me-1"></i>Todas las facturas tienen OT asignada.</td></tr>';
  } else {
    SIN_OT.forEach(function(item, idx) {
      rows += '<tr id="ots-row-' + item.item_id + '">'
        + '<td class="font-monospace fw-semibold">' + _esc(item.doc) + '</td>'
        + '<td>' + _esc(item.cliente) + '</td>'
        + '<td style="min-width:200px">'
        + '<input type="text" class="form-control form-control-sm font-monospace ot-input"'
        + ' id="ot-inp-' + idx + '"'
        + ' data-item="' + item.item_id + '"'
        + ' placeholder="780…" maxlength="30" autocomplete="off" spellcheck="false">'
        + '</td></tr>';
    });
  }

  document.getElementById('otsMasivoTable').innerHTML =
    '<table class="ots-table"><thead><tr>'
    + '<th>Documento</th><th>Cliente / destino</th><th>Tracking number FedEx</th>'
    + '</tr></thead><tbody id="otsMasivoTbody">' + rows + '</tbody></table>';

  document.getElementById('otsPasteArea').value = '';
  document.getElementById('otsMasivoStatus').textContent =
    (SIN_OT && SIN_OT.length) ? SIN_OT.length + ' factura(s) sin OT' : 'Sin pendientes';
  var btn = document.getElementById('otsMasivoGuardarBtn');
  if (btn) btn.disabled = false;
  _otsMasivoModal.show();
}

function distribuirOTs() {
  var raw = document.getElementById('otsPasteArea').value || '';
  var lines = raw.split(/\r?\n/)
    .map(function(l){ return l.trim().replace(/[^A-Za-z0-9]/g, ''); })
    .filter(Boolean);
  var inputs = document.querySelectorAll('.ot-input');
  var filled = 0;
  for (var i = 0; i < inputs.length; i++) {
    if (i < lines.length) {
      inputs[i].value = lines[i];
      filled++;
    }
  }
  var status = document.getElementById('otsMasivoStatus');
  if (status) status.textContent = filled + ' tracking(s) distribuido(s)';
}

async function guardarOTsMasivo() {
  var items = [];
  document.querySelectorAll('.ot-input').forEach(function(inp) {
    var tn = (inp.value || '').trim().replace(/[^A-Za-z0-9]/g, '');
    if (tn && tn.length >= 10) {
      items.push({ item_id: parseInt(inp.dataset.item, 10), tracking_number: tn });
    }
  });
  if (!items.length) {
    if (window.ilusToast) ilusToast('Ingresa al menos un tracking number (mín. 10 caracteres)', { type: 'warning' });
    return;
  }
  var btn    = document.getElementById('otsMasivoGuardarBtn');
  var status = document.getElementById('otsMasivoStatus');
  if (btn)    btn.disabled = true;
  if (status) status.textContent = 'Guardando y consultando FedEx…';

  var res;
  try {
    var r = await fetch('/transporte/api/manifiestos/' + MID + '/trackings-masivo', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ items: items })
    });
    res = await r.json();
  } catch(e) {
    if (btn) btn.disabled = false;
    if (status) status.textContent = '';
    if (window.ilusToast) ilusToast('Error de conexión', { type: 'error' });
    return;
  }

  if (btn) btn.disabled = false;
  if (!res || !res.ok) {
    if (status) status.textContent = '';
    if (window.ilusToast) ilusToast((res && res.error) || 'Error al guardar', { type: 'error' });
    return;
  }

  // Tally results
  var resultados  = res.resultados || [];
  var ok_count    = resultados.filter(function(r){ return r.ok && !r.error; }).length;
  var changed_cnt = resultados.filter(function(r){ return r.changed; }).length;
  var warn_cnt    = resultados.filter(function(r){ return r.warning; }).length;

  // Visual feedback per row
  resultados.forEach(function(r) {
    var row = document.getElementById('ots-row-' + r.item_id);
    if (!row) return;
    if (r.error)   { row.classList.add('ots-row-warn'); return; }
    if (r.ok)       { row.classList.add('ots-row-ok');  }
    var td = row.querySelector('td:last-child');
    if (td && r.estado) {
      td.innerHTML += ' <span style="font-size:.7rem;color:#15803d;font-weight:700">'
                    + _esc(r.estado) + '</span>';
    }
  });

  var msg = ok_count + ' OT(s) guardada(s)';
  if (changed_cnt) msg += ' · ' + changed_cnt + ' estado(s) actualizado(s)';
  if (warn_cnt)    msg += ' · ' + warn_cnt + ' con advertencia';
  if (status) status.textContent = msg;
  if (window.ilusToast) ilusToast('✓ ' + msg, { type: 'success' });

  setTimeout(function(){ location.reload(); }, 1400);
}

// ══════════════════════════════════════════════════════════════════════
//  CARGA MASIVA DE OT FedEx POR EXCEL — arrastrar/seleccionar (2026-07-29)
//  Daniel: "borra este menú del estado lateral, llévalo a los manifiestos
//  solamente de FedEx... un modal donde yo pueda llamar al documento, lo
//  pueda arrastrar." Reusa el mismo backend que la página /transporte/
//  ot-masivo (/preview + /aplicar, matchea por N° de factura — el bug
//  real era CSRF, ya corregido ahí).
// ══════════════════════════════════════════════════════════════════════
var _cefModal = null;
var _cefFilasPreview = [];

var _CEF_STATUS_INFO = {
  'ok_nuevo':      { label: 'Listo',            color: 'success', icon: 'bi-check-circle-fill' },
  'ok_re_envio':   { label: 'Reemplaza OT',      color: 'warning', icon: 'bi-arrow-repeat' },
  'ya_existe':     { label: 'Ya tiene esta OT',  color: 'secondary', icon: 'bi-dash-circle' },
  'no_encontrada': { label: 'Factura no existe', color: 'danger', icon: 'bi-x-circle' },
  'sin_manifiesto':{ label: 'Sin manifiesto',    color: 'danger', icon: 'bi-exclamation-triangle' },
  'ot_duplicada':  { label: 'OT duplicada',      color: 'danger', icon: 'bi-exclamation-triangle' },
  'invalido':      { label: 'Fila inválida',     color: 'danger', icon: 'bi-x-circle' },
};

function abrirCargaExcelFedex() {
  if (!_cefModal) _cefModal = new bootstrap.Modal(document.getElementById('cargaExcelFedexModal'));
  _cefFilasPreview = [];
  document.getElementById('cefFileStatus').style.display = 'none';
  document.getElementById('cefResumen').innerHTML = '';
  document.getElementById('cefTablaWrap').style.display = 'none';
  document.getElementById('cefTablaBody').innerHTML = '';
  document.getElementById('cefStatus').textContent = '';
  document.getElementById('cefAplicarBtn').disabled = true;
  _cefWireDropZone();
  _cefModal.show();
}

function _cefWireDropZone() {
  var zone = document.getElementById('cefDropZone');
  if (!zone || zone.dataset.wired) return;
  zone.dataset.wired = '1';
  ['dragenter', 'dragover'].forEach(function(ev) {
    zone.addEventListener(ev, function(e) {
      e.preventDefault(); e.stopPropagation();
      zone.classList.add('dragover');
    });
  });
  ['dragleave', 'drop'].forEach(function(ev) {
    zone.addEventListener(ev, function(e) {
      e.preventDefault(); e.stopPropagation();
      zone.classList.remove('dragover');
    });
  });
  zone.addEventListener('drop', function(e) {
    var file = e.dataTransfer && e.dataTransfer.files && e.dataTransfer.files[0];
    if (file) _cefHandleFile(file);
  });
}

async function _cefHandleFile(file) {
  var name = (file.name || '').toLowerCase();
  if (!name.endsWith('.xlsx')) {
    await ilusAlert({
      title: 'Formato no soportado',
      message: 'Por ahora solo aceptamos archivos .xlsx (Excel moderno).',
      type: 'warning',
    });
    return;
  }
  document.getElementById('cefFileName').textContent =
    file.name + ' · ' + Math.round((file.size || 0) / 1024) + ' KB';
  document.getElementById('cefFileStatus').style.display = '';
  document.getElementById('cefStatus').textContent = 'Analizando archivo…';
  document.getElementById('cefAplicarBtn').disabled = true;

  var fd = new FormData();
  fd.append('archivo', file);
  var resp, data;
  try {
    resp = await fetch('/transporte/ot-masivo/preview', { method: 'POST', body: fd, credentials: 'same-origin' });
    data = await resp.json();
  } catch (e) {
    document.getElementById('cefStatus').textContent = '';
    await ilusAlert({
      title: 'No se pudo procesar',
      message: 'El servidor no respondió correctamente.',
      sub: String(e).slice(0, 180),
      type: 'error',
    });
    return;
  }
  if (!resp.ok || !data.ok) {
    document.getElementById('cefStatus').textContent = '';
    await ilusAlert({ title: 'Error analizando el archivo', message: data.error || ('HTTP ' + resp.status), type: 'error' });
    return;
  }

  _cefFilasPreview = data.filas || [];
  var r = data.resumen || {};
  document.getElementById('cefResumen').innerHTML =
    '<div class="d-flex flex-wrap gap-2">' +
    '<span class="badge bg-success">' + (r.aplicables_directas || 0) + ' listas</span>' +
    (r.aplicables_re_envio ? '<span class="badge bg-warning text-dark">' + r.aplicables_re_envio + ' reemplazan OT</span>' : '') +
    (r.requieren_revision ? '<span class="badge bg-danger">' + r.requieren_revision + ' con problema</span>' : '') +
    '<span class="badge bg-secondary">' + (r.total || 0) + ' filas totales</span>' +
    '</div>';

  var tbody = document.getElementById('cefTablaBody');
  tbody.innerHTML = _cefFilasPreview.map(function(f) {
    var info = _CEF_STATUS_INFO[f.status] || { label: f.status, color: 'secondary', icon: 'bi-question-circle' };
    return '<tr><td>' + f.row + '</td>' +
      '<td class="font-monospace">' + _srEsc((f.tido || '') + ' ' + (f.factura || '')) + '</td>' +
      '<td class="font-monospace">' + _srEsc(f.ot || '') + '</td>' +
      '<td><span class="badge bg-' + info.color + '"><i class="bi ' + info.icon + ' me-1"></i>' + info.label + '</span></td>' +
      '<td class="small text-muted">' + _srEsc(f.msg || '') + '</td></tr>';
  }).join('');
  document.getElementById('cefTablaWrap').style.display = '';

  document.getElementById('cefStatus').textContent = '';
  var aplicables = (r.aplicables_directas || 0) + (r.aplicables_re_envio || 0);
  document.getElementById('cefAplicarBtn').disabled = aplicables === 0;
  if (window.ilusToast) ilusToast(aplicables ? ('✓ ' + aplicables + ' fila(s) lista(s) para aplicar') : 'Ninguna fila quedó lista para aplicar — revisa la tabla', { type: aplicables ? 'success' : 'warning' });
}

async function _cefAplicar() {
  var aplicables = _cefFilasPreview.filter(function(f) { return f.status === 'ok_nuevo' || f.status === 'ok_re_envio'; });
  if (!aplicables.length) return;
  var hayReenvios = aplicables.some(function(f) { return f.status === 'ok_re_envio'; });
  var confirmReenvio = false;
  if (hayReenvios) {
    confirmReenvio = await ilusConfirm({
      title: 'Algunas filas reemplazan una OT existente',
      message: 'Al menos una factura ya tenía un tracking distinto asignado. ¿Sobrescribirlo con el nuevo?',
      sub: 'Si eliges "No", esas filas se omiten y el resto se aplica igual.',
      okLabel: 'Sí, sobrescribir', cancelLabel: 'No, omitir esas',
    });
  }
  var btn = document.getElementById('cefAplicarBtn');
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner-border spinner-border-sm"></span> Aplicando…';
  document.getElementById('cefStatus').textContent = 'Guardando y consultando FedEx…';
  try {
    var r = await fetch('/transporte/ot-masivo/aplicar', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ filas: _cefFilasPreview, confirm_re_envio: confirmReenvio }),
    });
    var d = await r.json();
    if (!d.ok) {
      document.getElementById('cefStatus').textContent = '';
      btn.disabled = false;
      btn.innerHTML = '<i class="bi bi-check-lg me-1"></i>Aplicar OTs';
      if (window.ilusToast) ilusToast(d.error || 'No se pudo aplicar', { type: 'error' });
      return;
    }
    var msg = (d.aplicadas || 0) + ' OT(s) aplicada(s)';
    if (d.omitidas) msg += ' · ' + d.omitidas + ' omitida(s)';
    if (d.errores && d.errores.length) msg += ' · ' + d.errores.length + ' error(es)';
    document.getElementById('cefStatus').textContent = msg;
    if (window.ilusToast) ilusToast('✓ ' + msg, { type: 'success' });
    setTimeout(function() { location.reload(); }, 1400);
  } catch (e) {
    document.getElementById('cefStatus').textContent = '';
    btn.disabled = false;
    btn.innerHTML = '<i class="bi bi-check-lg me-1"></i>Aplicar OTs';
    if (window.ilusToast) ilusToast('Error de conexión al aplicar', { type: 'error' });
  }
}

// ══════════════════════════════════════════════════════════════════════
//  CREAR / CANCELAR OT FedEx por item (Ship API individual)
// ══════════════════════════════════════════════════════════════════════

async function crearOTFedexItem(itemId) {
  var ok = await ilusConfirm({
    title: 'Crear OT FedEx',
    message: '¿Crear OT FedEx para esta factura?',
    sub: 'FedEx asignará el tracking number y devolverá la etiqueta PDF. '
       + 'Esto descuenta crédito de la cuenta y se puede cancelar el mismo día.',
    okLabel: 'Sí, crear OT',
  });
  if (!ok) return;
  ilusToast('Conectando con FedEx…', { type: 'info' });
  try {
    var r = await fetch('/transporte/api/items/' + itemId + '/crear-ot-fedex', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        label_format: 'PDF', pickup_type: 'USE_SCHEDULED_PICKUP', notificar: false,
      })
    });
    var d = await r.json();
    if (!d.ok) {
      ilusToast(d.error || 'FedEx rechazó la solicitud', { type: 'error' });
      return;
    }
    ilusToast('✓ OT creada: ' + d.master_tracking_number + ' (' + d.n_bultos + ' bulto/s)', { type: 'success' });
    setTimeout(function(){ location.reload(); }, 1200);
  } catch(e) {
    ilusToast('Error de conexión', { type: 'error' });
  }
}

async function cancelarOTFedex(itemId, btn) {
  var ok = await ilusConfirm({
    title: 'Cancelar OT FedEx',
    message: '¿Cancelar la OT FedEx de esta factura?',
    sub: 'Solo se puede cancelar el MISMO día hasta las 16:00 hora Chile. Pasada esa hora FedEx ya no acepta la cancelación desde el sistema.',
    okLabel: 'Sí, cancelar', danger: true,
  });
  if (!ok) return;
  try {
    var r = await fetch('/transporte/api/items/' + itemId + '/cancelar-ot-fedex', { method: 'POST' });
    var d = await r.json();
    if (!d.ok) {
      if (d.expired && btn) {
        btn.disabled = true;
        btn.classList.remove('btn-outline-danger');
        btn.classList.add('btn-outline-secondary');
        btn.removeAttribute('data-cancel-deadline-ms');
        btn.title = 'Cancelación solo disponible el mismo día hasta las 16:00 hora Chile';
      }
      ilusToast(d.error || 'No se pudo cancelar', { type: 'error' });
      return;
    }
    ilusToast('✓ OT FedEx cancelada', { type: 'success' });
    setTimeout(function(){ location.reload(); }, 800);
  } catch(e) {
    ilusToast('Error de conexión', { type: 'error' });
  }
}

async function reemitirOTFedex(itemId, btn) {
  var ok = await ilusConfirm({
    title: 'Re-emitir etiqueta FedEx',
    message: '¿Cancelar la OT actual y crear una nueva con los bultos actualizados?',
    sub: 'Se cancela la etiqueta vieja en FedEx y se emite una nueva con el número de bultos actual. '
       + 'Solo funciona el mismo día hasta las 16:00 hora Chile. Queda respaldo del tracking anterior.',
    okLabel: 'Sí, re-emitir',
  });
  if (!ok) return;
  if (btn) btn.disabled = true;
  ilusToast('Re-emitiendo con FedEx…', { type: 'info' });
  try {
    var r = await fetch('/transporte/api/items/' + itemId + '/reemitir-ot-fedex', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ label_format: 'PDF', pickup_type: 'USE_SCHEDULED_PICKUP' })
    });
    var d = await r.json();
    if (!d.ok) {
      if (btn) btn.disabled = false;
      ilusToast(d.error || 'No se pudo re-emitir', { type: 'error' });
      return;
    }
    ilusToast('✓ Re-emitida: ' + d.master_tracking_number + ' (' + d.n_bultos + ' bulto/s). '
            + 'TN anterior ' + d.old_tracking_number + ' cancelado.', { type: 'success' });
    setTimeout(function(){ location.reload(); }, 1500);
  } catch(e) {
    if (btn) btn.disabled = false;
    ilusToast('Error de conexión', { type: 'error' });
  }
}

// Cronómetro de cancelación FedEx: actualiza tooltip cada 30s con tiempo
// restante hasta las 16:00 hora Chile. Cuando expira, deshabilita el botón
// en vivo sin necesidad de recargar.
function fedexCancelTickAll() {
  var btns = document.querySelectorAll('.fx-cancel-btn[data-cancel-deadline-ms]');
  var now = Date.now();
  btns.forEach(function(btn){
    var ddl = parseInt(btn.dataset.cancelDeadlineMs, 10);
    if (!ddl) return;
    var ms = ddl - now;
    if (ms <= 0) {
      btn.disabled = true;
      btn.classList.remove('btn-outline-danger');
      btn.classList.add('btn-outline-secondary');
      btn.removeAttribute('data-cancel-deadline-ms');
      btn.removeAttribute('onclick');
      btn.title = 'Cancelación solo disponible el mismo día hasta las 16:00 hora Chile';
      return;
    }
    var totalMin = Math.floor(ms / 60000);
    var h = Math.floor(totalMin / 60);
    var m = totalMin % 60;
    var rest = h > 0 ? (h + 'h ' + (m<10?'0':'') + m + 'm') : (m + ' min');
    btn.title = 'Cancelar OT FedEx · ' + rest + ' hasta corte (16:00 Chile)';
  });
}
document.addEventListener('DOMContentLoaded', function(){
  fedexCancelTickAll();
  setInterval(fedexCancelTickAll, 30 * 1000);
});

// ══════════════════════════════════════════════════════════════════════
//  REFRESCAR TRACKING FedEx (Track API — individual y por manifiesto)
// ══════════════════════════════════════════════════════════════════════

async function refrescarTrackingItem(itemId, btn) {
  if (btn) { btn.disabled = true; btn.classList.add('disabled'); }
  ilusToast('Consultando FedEx…', { type: 'info' });
  try {
    var r = await fetch('/transporte/api/items/' + itemId + '/refrescar-tracking',
                        { method: 'POST' });
    var d = await r.json();
    if (!d.ok) {
      ilusToast(d.error || 'No se pudo consultar FedEx', { type: 'error' });
      return;
    }
    if (d.warning) {
      ilusToast(d.warning, { type: 'warning' });
      return;
    }
    var msg = 'FedEx: ' + (d.fedex_label || d.estado);
    if (d.changed) msg = '✓ Estado actualizado: ' + d.anterior + ' → ' + d.estado;
    ilusToast(msg, { type: d.changed ? 'success' : 'info' });
    setTimeout(function(){ location.reload(); }, 1200);
  } catch(e) {
    ilusToast('Error de conexión', { type: 'error' });
  } finally {
    if (btn) { btn.disabled = false; btn.classList.remove('disabled'); }
  }
}

async function refrescarTrackingManifiesto() {
  ilusToast('Consultando FedEx para todo el manifiesto…', { type: 'info' });
  try {
    var r = await fetch('/transporte/api/manifiestos/' + MFD.mid + '/refrescar-tracking',
                        { method: 'POST' });
    var d = await r.json();
    if (!d.ok) {
      ilusToast(d.error || 'No se pudo consultar FedEx', { type: 'error' });
      return;
    }
    if (!d.polled) {
      ilusToast(d.msg || 'No hay envíos activos para refrescar', { type: 'info' });
      return;
    }
    var msg = '✓ ' + d.polled + ' consulta(s) · ' + d.changed + ' cambio(s)';
    if (d.errores && d.errores.length) msg += ' · ' + d.errores.length + ' error(es)';
    ilusToast(msg, { type: d.changed ? 'success' : 'info' });
    setTimeout(function(){ location.reload(); }, 1500);
  } catch(e) {
    ilusToast('Error de conexión', { type: 'error' });
  }
}

// ══════════════════════════════════════════════════════════════════════
//  CREAR OTs FedEx MASIVO (Ship API)
// ══════════════════════════════════════════════════════════════════════

var _crearOtsFedexModal = null;

function abrirCrearOtsFedex() {
  if (!document.getElementById('crearOtsFedexModal')) return;
  if (!_crearOtsFedexModal) _crearOtsFedexModal = new bootstrap.Modal(document.getElementById('crearOtsFedexModal'));

  var lista = document.getElementById('fxOtsList');
  if (!SIN_OT || !SIN_OT.length) {
    lista.innerHTML = '<div class="text-center text-success py-3">'
                    + '<i class="bi bi-check-circle-fill me-1"></i>'
                    + 'Todas las facturas ya tienen OT asignada.</div>';
    document.getElementById('fxPendingCount').textContent = '0';
    document.getElementById('fxCrearBtn').disabled = true;
  } else {
    var html = '';
    SIN_OT.forEach(function(item, idx){
      html += '<div class="d-flex gap-2 align-items-center py-1 border-bottom" id="fx-row-' + item.item_id + '">'
            + '<div class="form-check m-0">'
            + '<input class="form-check-input fx-item-check" type="checkbox" checked '
            + 'data-item="' + item.item_id + '" id="fxchk-' + idx + '">'
            + '</div>'
            + '<div class="flex-grow-1 small">'
            + '<div class="font-monospace fw-semibold">' + _esc(item.doc) + '</div>'
            + '<div class="text-muted">' + _esc(item.cliente) + '</div>'
            + '</div>'
            + '<div class="small text-muted" id="fx-status-' + item.item_id + '"></div>'
            + '</div>';
    });
    lista.innerHTML = html;
    document.getElementById('fxPendingCount').textContent = SIN_OT.length;
    document.getElementById('fxCrearBtn').disabled = false;
  }

  document.getElementById('fxProgress').classList.add('d-none');
  document.getElementById('fxProgressBar').style.width = '0%';
  document.getElementById('fxProgressBar').textContent = '0%';

  _crearOtsFedexModal.show();
}

async function ejecutarCrearOTs() {
  var checks = document.querySelectorAll('.fx-item-check:checked');
  var ids = [];
  checks.forEach(function(c){ ids.push(parseInt(c.dataset.item, 10)); });
  if (!ids.length) {
    ilusToast('Selecciona al menos una factura', { type: 'warning' });
    return;
  }

  var ok = await ilusConfirm({
    title: 'Crear OTs FedEx',
    message: '¿Crear ' + ids.length + ' OT(s) en FedEx?',
    sub: 'Esto generará tracking numbers REALES en FedEx y descontará créditos de la cuenta. '
       + 'No se puede deshacer fácilmente (se puede cancelar el mismo día).',
    okLabel: 'Sí, crear OTs', danger: false,
  });
  if (!ok) return;

  var btn = document.getElementById('fxCrearBtn');
  btn.disabled = true;

  document.getElementById('fxProgress').classList.remove('d-none');
  var bar = document.getElementById('fxProgressBar');
  var msg = document.getElementById('fxProgressMsg');
  bar.style.width = '15%'; bar.textContent = 'Generando…';
  msg.textContent = 'Enviando solicitud a FedEx (puede tardar 10-20 seg por OT)…';

  var payload = {
    item_ids:      ids,
    service_type:  document.getElementById('fxService').value,
    label_format:  document.getElementById('fxLabelFormat').value,
    pickup_type:   document.getElementById('fxPickupType').value,
    notificar:     document.getElementById('fxNotify').checked,
  };

  var res;
  try {
    var r = await fetch('/transporte/api/manifiestos/' + MID + '/crear-ots-fedex-masivo', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });
    res = await r.json();
  } catch(e) {
    btn.disabled = false;
    bar.style.width = '0%';
    msg.textContent = '';
    ilusToast('Error de conexión con FedEx', { type: 'error' });
    return;
  }

  if (!res || !res.ok) {
    btn.disabled = false;
    bar.style.width = '0%';
    msg.textContent = '';
    ilusToast((res && res.error) || 'Error al crear OTs', { type: 'error' });
    return;
  }

  bar.style.width = '100%'; bar.textContent = '100%';
  msg.textContent = 'Listo · ' + res.creadas + ' creada(s) · ' + res.errores + ' error(es)';

  (res.resultados || []).forEach(function(r){
    var cell = document.getElementById('fx-status-' + r.item_id);
    if (!cell) return;
    if (r.ok && r.master_tracking_number) {
      cell.innerHTML = '<span style="color:#15803d;font-weight:700">'
                     + '<i class="bi bi-check-circle-fill me-1"></i>'
                     + _esc(r.master_tracking_number) + '</span>';
    } else if (r.error) {
      // Mostrar el texto del error directamente (no solo en tooltip)
      var errTxt = r.error || 'Error FedEx';
      // Truncar para que quepa en la celda; el texto completo en title
      var errShort = errTxt.length > 80 ? errTxt.slice(0, 77) + '…' : errTxt;
      cell.innerHTML = '<div style="color:#dc2626;font-size:.75rem;max-width:280px">'
                     + '<div style="font-weight:700"><i class="bi bi-x-circle-fill me-1"></i>Error FedEx</div>'
                     + '<div title="' + _esc(errTxt) + '" style="line-height:1.3;word-break:break-word">'
                     + _esc(errShort) + '</div></div>';
    }
  });

  if (res.errores > 0) {
    ilusToast(res.creadas + ' OT(s) creada(s) · ' + res.errores + ' error(es) — revisa la lista', { type: 'warning' });
  } else {
    ilusToast('✓ ' + res.creadas + ' OT(s) creada(s) en FedEx', { type: 'success' });
  }
  setTimeout(function(){ location.reload(); }, 3500);
}

// ══════════════════════════════════════════════════════════════════════
//  PROGRAMAR RETIRO FedEx (Pickup API)
// ══════════════════════════════════════════════════════════════════════

var _pickupFedexModal = null;

function abrirPickupFedex() {
  if (!document.getElementById('pickupFedexModal')) return;
  if (!_pickupFedexModal) _pickupFedexModal = new bootstrap.Modal(document.getElementById('pickupFedexModal'));

  // Default: mañana
  var fecha = document.getElementById('pkFecha');
  if (fecha && !fecha.value) {
    var d = new Date(); d.setDate(d.getDate() + 1);
    fecha.value = d.toISOString().slice(0, 10);
  }
  document.getElementById('pkMsg').innerHTML = '';
  _pickupFedexModal.show();
}

function _pickupPayload() {
  return {
    manifest_id:     MID,
    fecha:           document.getElementById('pkFecha').value,
    ready_time:      (document.getElementById('pkReadyTime').value || '09:00') + ':00',
    close_time:      (document.getElementById('pkCloseTime').value || '18:00') + ':00',
    package_count:   parseInt(document.getElementById('pkPackageCount').value, 10) || 1,
    total_weight_kg: parseFloat(document.getElementById('pkTotalWeight').value) || 1,
    direccion:       document.getElementById('pkDireccion').value.trim(),
    comuna:          document.getElementById('pkComuna').value.trim(),
    cod_postal:      document.getElementById('pkCodPostal').value.trim(),
    instrucciones:   document.getElementById('pkInstrucciones').value.trim(),
    notify_email:    document.getElementById('pkNotifyEmail').value.trim(),
    contacto: {
      nombre:    document.getElementById('pkContactoNombre').value.trim(),
      telefono:  document.getElementById('pkContactoTelefono').value.trim(),
    },
  };
}

async function consultarDisponibilidadPickup() {
  var msg = document.getElementById('pkMsg');
  msg.innerHTML = '<i class="bi bi-arrow-repeat"></i> Consultando disponibilidad…';
  var p = _pickupPayload();
  try {
    var r = await fetch('/transporte/api/pickup/fedex/disponibilidad', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        fecha: p.fecha, ready_time: p.ready_time, close_time: p.close_time,
        peso_kg: p.total_weight_kg,
        direccion: p.direccion, comuna: p.comuna, cod_postal: p.cod_postal,
      })
    });
    var data = await r.json();
    if (!data.ok) {
      msg.innerHTML = '<span class="text-danger"><i class="bi bi-x-circle"></i> ' + _esc(data.error || 'No disponible') + '</span>';
      return;
    }
    var opts = data.options || [];
    if (!opts.length) {
      msg.innerHTML = '<span class="text-warning"><i class="bi bi-exclamation-triangle"></i> Sin opciones para esa fecha</span>';
      return;
    }
    msg.innerHTML = '<span class="text-success"><i class="bi bi-check-circle"></i> Disponible · ' + opts.length + ' opción(es)</span>';
  } catch(e) {
    msg.innerHTML = '<span class="text-danger">Error de conexión</span>';
  }
}

async function ejecutarCrearPickup() {
  var p = _pickupPayload();
  if (!p.fecha) { ilusToast('Selecciona una fecha', { type: 'warning' }); return; }

  var ok = await ilusConfirm({
    title: 'Programar retiro FedEx',
    message: '¿Agendar retiro el ' + p.fecha + ' entre ' + p.ready_time.slice(0,5) + ' y ' + p.close_time.slice(0,5) + '?',
    sub: p.package_count + ' bulto(s) · ' + p.total_weight_kg + ' kg · ' + p.direccion + ', ' + p.comuna,
    okLabel: 'Sí, programar',
  });
  if (!ok) return;

  var btn = document.getElementById('pkCrearBtn');
  btn.disabled = true;
  var msg = document.getElementById('pkMsg');
  msg.innerHTML = '<i class="bi bi-arrow-repeat"></i> Enviando solicitud a FedEx…';

  try {
    var r = await fetch('/transporte/api/pickup/fedex/crear', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(p)
    });
    var data = await r.json();
    btn.disabled = false;
    if (!data.ok) {
      msg.innerHTML = '<span class="text-danger">' + _esc(data.error || 'Error') + '</span>';
      return;
    }
    msg.innerHTML = '<span class="text-success"><i class="bi bi-check-circle-fill"></i> '
                  + 'Retiro programado · Código: <strong>' + _esc(data.pickup_confirmation_code) + '</strong> '
                  + '· Centro: ' + _esc(data.location) + '</span>';
    ilusToast('✓ Retiro FedEx programado: ' + data.pickup_confirmation_code, { type: 'success' });
    setTimeout(function(){ if (_pickupFedexModal) _pickupFedexModal.hide(); }, 2500);
  } catch(e) {
    btn.disabled = false;
    msg.innerHTML = '<span class="text-danger">Error de conexión</span>';
  }
}

// ══════════════════════════════════════════════════════════════════════
//  CAMBIO DE ESTADO via badge + modal (reemplaza el <select> nativo)
// ══════════════════════════════════════════════════════════════════════

var ESTADOS_META = {
  'En preparación':         { bg:'#6b7280', icon:'bi-clipboard-check' },
  'Entregado a transporte': { bg:'#06b6d4', icon:'bi-truck-flatbed' },
  'En ruta':                { bg:'#3b82f6', icon:'bi-truck' },
  'Entregado':              { bg:'#16a34a', icon:'bi-check-circle-fill' },
  'Problema':               { bg:'#dc2626', icon:'bi-x-octagon-fill' },
  'Entrega fallida':        { bg:'#dc2626', icon:'bi-x-octagon-fill' },  // compat: solo para pintar filas viejas, no seleccionable
  'Devolución':             { bg:'#f59e0b', icon:'bi-arrow-return-left' }
};
// Estados que el modal ofrece para elegir manualmente. 'Entrega fallida' queda
// FUERA a propósito (2026-07-25): es el nombre viejo de 'Problema', ya no se
// escribe desde acá — solo se conserva en ESTADOS_META para pintar bien el
// badge de items ya guardados con ese valor.
var ESTADOS_SELECCIONABLES = [
  'En preparación', 'Entregado a transporte', 'En ruta', 'Entregado', 'Problema', 'Devolución'
];
var _modalEstado = null;
var _editEstadoCtx = null;

function abrirEditarEstado(mid, itemId, currentEstado) {
  _editEstadoCtx = { mid: mid, itemId: itemId, currentEstado: currentEstado };
  if (!_modalEstado) {
    _modalEstado = new bootstrap.Modal(document.getElementById('modalEditarEstado'));
  }
  // Texto contexto (doc + cliente) — leemos de la fila
  var row = document.getElementById('row-' + itemId) || document.getElementById('card-' + itemId);
  var docTxt = 'Item #' + itemId;
  if (row) {
    var docEl = row.querySelector('.mfd-doc, [data-doc]');
    if (docEl) docTxt = (docEl.textContent || docTxt).trim();
  }
  document.getElementById('mee-doc').innerHTML =
    '<i class="bi bi-receipt me-1"></i><strong>' + _esc(docTxt) + '</strong> · estado actual: <span style="color:' +
    (ESTADOS_META[currentEstado] || {}).bg + ';font-weight:600">' + _esc(currentEstado) + '</span>';

  var box = document.getElementById('mee-buttons');
  box.innerHTML = '';
  ESTADOS_SELECCIONABLES.forEach(function(estado) {
    var m = ESTADOS_META[estado];
    var btn = document.createElement('button');
    btn.type = 'button';
    btn.className = 'btn d-flex align-items-center w-100';
    btn.style.background = m.bg;
    btn.style.color = '#fff';
    btn.style.fontWeight = '600';
    btn.style.padding = '13px 16px';
    btn.style.borderRadius = '10px';
    btn.style.border = 'none';
    btn.style.fontSize = '.95rem';
    btn.style.boxShadow = '0 1px 4px rgba(0,0,0,.1)';
    var actualTag = '';
    if (estado === currentEstado) {
      btn.style.boxShadow = 'inset 0 0 0 3px #fff, 0 0 0 3px ' + m.bg + ', 0 4px 10px rgba(0,0,0,.2)';
      actualTag = '<span class="ms-auto small" style="opacity:.85"><i class="bi bi-check2-circle me-1"></i>actual</span>';
    }
    btn.innerHTML = '<i class="bi ' + m.icon + ' me-2" style="font-size:1.25rem"></i>' +
                    '<span>' + _esc(estado) + '</span>' + actualTag;
    btn.onclick = async function() {
      if (estado === currentEstado) { _modalEstado.hide(); return; }
      // "Problema" exige motivo obligatorio (pedido Daniel 2026-07-25): el
      // correo al cliente cuenta cuál fue el problema, así que sin motivo no
      // se guarda. Usamos ilusPrompt (REGLA #1 — nunca prompt() nativo).
      var _motivo = null;
      if (estado === 'Problema') {
        _motivo = await ilusPrompt({
          title: 'Marcar "Problema"',
          message: 'Cuéntanos qué pasó con la entrega. Este motivo se incluye en el correo que recibirá el cliente.',
          placeholder: 'Ej: dirección incorrecta, destinatario ausente, producto dañado...',
          required: true,
        });
        if (!_motivo) return;   // canceló o dejó vacío
      }
      aplicarCambioEstado(estado, btn, _motivo);
    };
    box.appendChild(btn);
  });

  _modalEstado.show();
}

async function aplicarCambioEstado(nuevoEstado, btnEl, comentario) {
  if (!_editEstadoCtx) return;
  var mid = _editEstadoCtx.mid, itemId = _editEstadoCtx.itemId;
  if (btnEl) { btnEl.disabled = true; btnEl.style.opacity = '.6'; }
  try {
    var r = await fetch('/transporte/manifiestos/' + mid + '/items/' + itemId + '/estado', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ estado_entrega: nuevoEstado, comentario: comentario || undefined })
    });
    var d = await r.json();
    if (!d.ok) {
      ilusToast(d.error || 'No se pudo guardar', { type: 'error' });
      if (btnEl) { btnEl.disabled = false; btnEl.style.opacity = ''; }
      return;
    }
    // Update inline del badge (sin reload)
    var badge = document.getElementById('badge-estado-' + itemId);
    if (badge) {
      var m = ESTADOS_META[nuevoEstado] || { bg:'#6b7280', icon:'bi-question-circle' };
      badge.style.background = m.bg;
      badge.setAttribute('data-estado', nuevoEstado);
      badge.innerHTML = '<i class="bi ' + m.icon + '"></i><span>' + _esc(nuevoEstado) + '</span>';
      badge.setAttribute('onclick',
        'abrirEditarEstado(' + mid + ', ' + itemId + ', ' + JSON.stringify(nuevoEstado) + ')');
      // Flash verde de confirmación
      var row = document.getElementById('row-' + itemId) || document.getElementById('card-' + itemId);
      if (row) {
        row.style.transition = 'background .3s';
        row.style.background = '#e8f5e9';
        setTimeout(function(){ row.style.background = ''; }, 1000);
      }
    }
    _modalEstado.hide();
    ilusToast('✓ Estado actualizado: ' + nuevoEstado, { type: 'success' });
    if (typeof actualizarResumen === 'function') actualizarResumen();
  } catch(e) {
    ilusToast('Error de conexión', { type: 'error' });
    if (btnEl) { btnEl.disabled = false; btnEl.style.opacity = ''; }
  }
}

// Helper de escape si no estaba definido a este nivel
if (typeof _esc !== 'function') {
  window._esc = function(s) {
    return String(s == null ? '' : s)
      .replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')
      .replace(/"/g,'&quot;').replace(/'/g,'&#39;');
  };
}

// ══════════════════════════════════════════════════════════════════════
//  IMPRIMIR / DESCARGAR ETIQUETAS FedEx del manifiesto
// ══════════════════════════════════════════════════════════════════════
// Copiar número de OT FedEx al portapapeles
function copiarOT(tn, btn) {
  if (!tn) return;
  var done = function() {
    if (window.ilusToast) ilusToast('✓ OT copiada: ' + tn, { type: 'success' });
    if (btn) {
      var ic = btn.querySelector('i');
      if (ic) { var old = ic.className; ic.className = 'bi bi-clipboard-check text-success';
        setTimeout(function(){ ic.className = old; }, 1400); }
    }
  };
  if (navigator.clipboard && navigator.clipboard.writeText) {
    navigator.clipboard.writeText(tn).then(done).catch(function(){ fallbackCopy(tn); done(); });
  } else { fallbackCopy(tn); done(); }
}
function fallbackCopy(txt) {
  try {
    var ta = document.createElement('textarea');
    ta.value = txt; ta.style.position = 'fixed'; ta.style.opacity = '0';
    document.body.appendChild(ta); ta.select();
    document.execCommand('copy'); ta.remove();
  } catch(e) {}
}

// Vista imprimible (rasterizada a 100×150mm exactos → sale cortada al imprimir)
// ══════════════════════════════════════════════════════════════════
//  SIMPLIROUTE — subir las facturas del manifiesto como visitas
// ══════════════════════════════════════════════════════════════════

// Escapa texto que va dentro de `sub` con subHtml:true. Se define acá con
// nombre propio (_srEsc) en vez de escHtml: escHtml existe en OTRAS pantallas
// del proyecto pero nunca en esta, y llamarlo tiraba "escHtml is not defined"
// justo cuando SimpliRoute devolvía un error — tapando el motivo real.
function _srEsc(s) {
  return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) {
    return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
  });
}

var _srModal = null;
var _srBusy = false;

// Abre el modal con progreso (reemplaza el spinner-en-botón + ilusAlert de
// antes). Mismo patrón visual que "Crear OTs FedEx" (barra de progreso +
// mensajes de estado dentro del modal), pedido por Daniel para que la subida
// a SimpliRoute se sienta igual de premium.
function subirASimpliRoute() {
  if (!document.getElementById('subirSimplirouteModal')) return;
  if (!_srModal) _srModal = new bootstrap.Modal(document.getElementById('subirSimplirouteModal'));

  // Reset del modal cada vez que se abre
  document.getElementById('srIntro').classList.remove('d-none');
  document.getElementById('srProgress').classList.add('d-none');
  document.getElementById('srResultado').classList.add('d-none');
  document.getElementById('srResultado').innerHTML = '';
  document.getElementById('srDondeVerla').classList.add('d-none');
  document.getElementById('srProgressBar').style.width = '0%';
  document.getElementById('srProgressBar').textContent = '0%';
  document.getElementById('srConfirmarBtn').classList.remove('d-none');
  document.getElementById('srConfirmarBtn').disabled = false;
  document.getElementById('srCancelarBtn').textContent = 'Cancelar';
  _srBusy = false;

  _srModal.show();
}

async function _srEjecutar() {
  if (_srBusy) return;
  _srBusy = true;

  var confirmarBtn = document.getElementById('srConfirmarBtn');
  var cancelarBtn = document.getElementById('srCancelarBtn');
  var closeBtn = document.getElementById('srModalCloseBtn');
  confirmarBtn.disabled = true;
  cancelarBtn.disabled = true;
  if (closeBtn) closeBtn.disabled = true;

  document.getElementById('srIntro').classList.add('d-none');
  var prog = document.getElementById('srProgress');
  var bar = document.getElementById('srProgressBar');
  var msg = document.getElementById('srProgressMsg');
  prog.classList.remove('d-none');

  bar.style.width = '15%'; bar.textContent = '15%';
  msg.textContent = 'Conectando con SimpliRoute…';

  // Progreso simulado (la API responde en un solo POST, no hay eventos
  // intermedios reales) — igual patrón que el modal de FedEx: mensajes que
  // avanzan mientras se espera la respuesta, para que no se sienta colgado.
  var _step2 = setTimeout(function () {
    bar.style.width = '55%'; bar.textContent = '55%';
    msg.textContent = 'Creando visitas en la cuenta del transportista…';
  }, 600);

  var d = null, errRed = null;
  try {
    var r = await fetch('/transporte/api/manifiestos/' + MFD.mid + '/subir-simpliroute',
                        { method: 'POST', headers: {'Content-Type':'application/json'} });
    d = await r.json();
    if (!r.ok || d.error) errRed = d.error || ('HTTP ' + r.status);
  } catch (e) {
    errRed = 'Error de conexión con SimpliRoute';
  }
  clearTimeout(_step2);

  var resDiv = document.getElementById('srResultado');
  resDiv.classList.remove('d-none');
  confirmarBtn.classList.add('d-none');
  cancelarBtn.disabled = false;
  if (closeBtn) closeBtn.disabled = false;
  cancelarBtn.textContent = 'Cerrar';

  if (errRed) {
    bar.classList.remove('progress-bar-animated');
    bar.style.width = '100%'; bar.textContent = 'Error';
    bar.style.background = '#dc2626';
    msg.textContent = '';
    resDiv.innerHTML = '<div class="alert alert-danger py-2 mb-0" style="font-size:.85rem">'
      + '<i class="bi bi-x-circle-fill me-1"></i>No se pudo subir a SimpliRoute: '
      + _srEsc(errRed) + '</div>';
    _srBusy = false;
    return;
  }

  bar.style.width = '100%'; bar.textContent = '100%';
  msg.textContent = '¡Listo!';

  var fechaLbl = d.fecha || MFD.fechaLbl;
  var fechaEl = document.getElementById('srFechaTxt');
  if (fechaEl) fechaEl.textContent = fechaLbl;

  if (d.creadas === 0 && d.errores === 0) {
    resDiv.innerHTML = '<div class="alert alert-info py-2 mb-0" style="font-size:.85rem">'
      + '<i class="bi bi-info-circle-fill me-1"></i>' + _srEsc(d.mensaje || 'Todas las facturas ya estaban en SimpliRoute.')
      + '</div>';
  } else if (d.errores === 0) {
    resDiv.innerHTML = '<div class="alert alert-success py-2 mb-1" style="font-size:.85rem">'
      + '<i class="bi bi-check-circle-fill me-1"></i>' + d.creadas + ' visita(s) creada(s) en la cuenta de '
      + _srEsc(d.courier || 'el transportista') + '.'
      + (d.ya_estaban ? ' (' + d.ya_estaban + ' ya estaban subidas y se saltaron.)' : '') + '</div>';
    document.getElementById('srDondeVerla').classList.remove('d-none');
    setTimeout(function(){ location.reload(); }, 4500);
  } else {
    var fallidos = (d.resultados || []).filter(function(x){ return !x.ok; });
    resDiv.innerHTML = '<div class="alert alert-warning py-2 mb-1" style="font-size:.85rem">'
      + '<i class="bi bi-exclamation-triangle-fill me-1"></i>' + d.creadas + ' visita(s) creada(s), '
      + d.errores + ' con problema.<br>'
      + fallidos.slice(0, 5).map(function(f){
          return '• Item ' + f.item_id + ': ' + _srEsc(f.error || 'sin detalle');
        }).join('<br>') + (fallidos.length > 5 ? '<br>…y ' + (fallidos.length - 5) + ' más' : '')
      + '</div>';
    if (d.creadas > 0) document.getElementById('srDondeVerla').classList.remove('d-none');
    setTimeout(function(){ location.reload(); }, 4500);
  }
  _srBusy = false;
}

function verImprimirEtiquetasFedex() {
  window.open('/transporte/manifiestos/' + MFD.mid + '/etiquetas-fedex/print', '_blank');
}
// PDF crudo de FedEx (respaldo / impresión directa Zebra)
function descargarPdfEtiquetasFedex() {
  window.open('/transporte/manifiestos/' + MFD.mid + '/etiquetas-fedex/pdf', '_blank');
}

// Re-emite las OTs que sólo tienen la master guardada (creadas antes del fix
// multi-pieza). Cancela la vieja en FedEx → crea una nueva → guarda todas las
// etiquetas por bulto. El TN master cambia.
async function actualizarEtiquetasFedex() {
  var ok = await ilusConfirm({
    title: 'Actualizar etiquetas multi-bulto',
    message: '¿Re-emitir las OTs que sólo tienen 1 etiqueta para obtener todas las piezas por bulto?',
    sub: 'Cancela la OT vieja en FedEx y crea una nueva con el mismo destinatario. '
       + 'El Nº de tracking cambiará — descarta cualquier etiqueta vieja que hayas impreso. '
       + 'Solo funciona dentro de la ventana de cancelación (16:00 hora Chile del día de creación).',
    okLabel: 'Actualizar etiquetas',
    cancelLabel: 'Cancelar',
  });
  if (!ok) return;
  ilusToast('Re-emitiendo OTs en FedEx…', { type: 'info' });
  try {
    var r = await fetch('/transporte/api/manifiestos/' + MFD.mid + '/actualizar-etiquetas-fedex', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: '{}',
    });
    var d = await r.json();
    if (!d.ok) {
      ilusToast(d.error || 'No se pudo actualizar', { type: 'error' });
      return;
    }
    if ((d.actualizadas || 0) === 0 && (d.errores || 0) === 0) {
      ilusToast(d.msg || 'No había etiquetas para actualizar', { type: 'info' });
      return;
    }
    var msg = '✓ ' + d.actualizadas + ' OT(s) actualizada(s) con todas sus etiquetas';
    if (d.errores) msg += ' · ' + d.errores + ' con error';
    ilusToast(msg, { type: d.errores ? 'warning' : 'success' });
    // Si hubo errores, mostrar detalle en modal para que Daniel los vea.
    if (d.errores && d.errores_detalle && d.errores_detalle.length && typeof ilusAlert !== 'undefined') {
      var html = '<ul style="margin:0;padding-left:1.1em;font-size:.85rem">';
      d.errores_detalle.forEach(function(e) {
        html += '<li><strong>' + (e.doc || '#' + e.item_id) + ':</strong> ' + (e.error || 'error') + '</li>';
      });
      html += '</ul>';
      await ilusAlert({
        title: 'Algunas OTs no se pudieron actualizar',
        message: 'Detalles:',
        sub: html, subHtml: true, type: 'warning',
      });
    }
    setTimeout(function(){ location.reload(); }, 1500);
  } catch(e) {
    ilusToast('Error de conexión', { type: 'error' });
  }
}

// ═══════════════════════════════════════════════════════════════════
// Modal: Manifiesto de firma (2026-07-26) — antes window.open, ahora
// modal con iframe a la misma URL /transporte/manifiestos/<id>/firma.
// ═══════════════════════════════════════════════════════════════════
var _manifiestoFirmaModal = null;
var _pruebaEntregaModal = null;

// Una sola superficie para factura y manifiesto: cualquier ajuste visual
// hecho en etiquetas.html llega al modal sin mantener una segunda version.
function abrirEtiquetasModal(commitmentId, trigger) {
  var url = '/transporte/factura/' + commitmentId + '/etiquetas?mid=' + MFD.mid;
  ilusEtiquetasModal.open(url, 'Etiquetas de la factura', trigger || document.activeElement);
}

function abrirEtiquetasManifiestoModal(trigger) {
  var url = '/transporte/manifiestos/' + MFD.mid + '/etiquetas';
  ilusEtiquetasModal.open(
    url,
    'Etiquetas · ' + MFD.correlativo,
    trigger || document.activeElement
  );
}

// FIX 2026-07-27 (Daniel: "quiero ver la firma, la foto, la entrega, todo
// bien bonito... no fue lo que pedí" -- la versión anterior embebía la
// página COMPLETA de /transporte/buscar, con sidebar y buscador, dentro de
// un iframe). Ahora es un modal propio: llama al MISMO JSON que ya usa esa
// página (/transporte/api/buscar/<commitment_id>) y renderiza solo el
// detalle -- sin duplicar esa lógica de backend.
async function abrirPruebaEntrega(commitmentId) {
  if (!_pruebaEntregaModal) {
    _pruebaEntregaModal = new bootstrap.Modal(document.getElementById('pruebaEntregaModal'));
  }
  var body = document.getElementById('pruebaEntregaBody');
  body.innerHTML = '<div class="text-center text-muted py-4"><i class="bi bi-hourglass-split"></i> Cargando…</div>';
  _pruebaEntregaModal.show();
  try {
    var r = await fetch('/transporte/api/buscar/' + commitmentId);
    var d = await r.json();
    if (!d.ok) {
      body.innerHTML = '<div class="text-center text-muted py-4">No se pudo cargar el detalle.</div>';
      return;
    }
    body.innerHTML = _peRenderDetalle(d.detalle);
  } catch (e) {
    body.innerHTML = '<div class="text-center text-muted py-4">Error de conexión.</div>';
  }
}

// Mismo renderizado que _renderDetalle() de templates/transporte/
// buscar_interno.html (calcado, no se reimplementa el JSON del backend,
// solo la vista) -- estado, cliente/destino, chofer+GPS, prueba de entrega
// (receptor/hora/GPS/firma/fotos) e historial completo.
function _peEsc(s) {
  return String(s == null ? '' : s).replace(/[&<>"]/g, function(c){
    return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c];
  });
}
function _peBadgeClass(e) {
  return ({'En preparación':'pe-b-prep','Entregado a transporte':'pe-b-trans','En ruta':'pe-b-ruta',
           'Entregado':'pe-b-entreg','Problema':'pe-b-fallida','Entrega fallida':'pe-b-fallida',
           'Devolución':'pe-b-fallida'}[e] || 'pe-b-prep');
}
function _peAgeTxt(s) {
  if (s < 60) return 'ahora';
  if (s < 3600) return 'hace ' + Math.round(s / 60) + ' min';
  return 'hace ' + Math.round(s / 3600) + 'h';
}
function _peRenderDetalle(d) {
  var c = d.commitment, mi = d.manifest_item, ch = d.chofer, lp = d.last_ping, p = d.proof;
  var est = mi ? mi.estado_entrega : (c.estado || 'En preparación');
  var html = '<div class="pe-hdr d-flex justify-content-between align-items-start gap-2">';
  html += '<div><div class="pe-doc">' + _peEsc(c.tido) + ' ' + _peEsc(c.nudo) + '</div>';
  html += '<div class="pe-cli">' + _peEsc(c.cliente_nombre || '—') + '</div></div>';
  html += '<span class="pe-badge ' + _peBadgeClass(est) + '">' + _peEsc(est) + '</span></div>';

  html += '<div class="pe-sec"><div class="pe-h"><i class="bi bi-person"></i>Cliente / Destino</div>';
  html += '<div class="pe-kv"><span class="pe-kv-k">RUT</span><span class="pe-kv-v" style="font-family:monospace">' + _peEsc(c.cliente_rut || '—') + '</span></div>';
  html += '<div class="pe-kv"><span class="pe-kv-k">Dirección</span><span class="pe-kv-v">' + _peEsc(c.direccion || '—') + '</span></div>';
  html += '<div class="pe-kv"><span class="pe-kv-k">Comuna</span><span class="pe-kv-v">' + _peEsc(c.comuna || '—') + '</span></div>';
  if (c.telefono) html += '<div class="pe-kv"><span class="pe-kv-k">Teléfono</span><span class="pe-kv-v">' + _peEsc(c.telefono) + '</span></div>';
  html += '</div>';

  if (mi) {
    html += '<div class="pe-sec"><div class="pe-h"><i class="bi bi-truck"></i>Despacho</div>';
    if (mi.correlativo) html += '<div class="pe-kv"><span class="pe-kv-k">Manifiesto</span><span class="pe-kv-v" style="font-family:monospace">' + _peEsc(mi.correlativo) + '</span></div>';
    if (mi.courier) html += '<div class="pe-kv"><span class="pe-kv-k">Transporte</span><span class="pe-kv-v">' + _peEsc(mi.courier) + '</span></div>';
    if (mi.tracking_number) html += '<div class="pe-kv"><span class="pe-kv-k">N° tracking</span><span class="pe-kv-v" style="font-family:monospace">' + _peEsc(mi.tracking_number) + '</span></div>';
    html += '</div>';
  }

  if (ch) {
    html += '<div class="pe-sec"><div class="pe-h"><i class="bi bi-person-badge"></i>Chofer asignado</div>';
    html += '<div class="pe-driver">';
    html += '<div style="font-weight:800">' + _peEsc(ch.nombre) + '</div>';
    html += '<div style="font-size:.78rem;color:#6b7280">' + _peEsc(ch.courier || '') + (ch.patente ? ' · ' + _peEsc(ch.patente) : '') + (ch.telefono ? ' · ' + _peEsc(ch.telefono) : '') + '</div>';
    if (lp) {
      html += '<div style="margin-top:.5rem;font-size:.8rem"><i class="bi bi-geo-alt-fill" style="color:#dc2626"></i> '
            + '<button type="button" class="pe-gps-btn" onclick="_ilusMapModal(' + lp.lat + ',' + lp.lng + ',\'Última posición del chofer\')">Ver en mapa</button> · ' + _peAgeTxt(lp.age_s)
            + (lp.speed_kmh != null ? ' · ' + Math.round(lp.speed_kmh) + ' km/h' : '') + '</div>';
    } else {
      html += '<div style="margin-top:.5rem;font-size:.78rem;color:#9ca3af">Sin pings recientes</div>';
    }
    html += '</div></div>';
  }

  if (p) {
    html += '<div class="pe-sec"><div class="pe-h"><i class="bi bi-shield-check"></i>Prueba de entrega</div>';
    html += '<div class="pe-kv"><span class="pe-kv-k">Receptor</span><span class="pe-kv-v">' + _peEsc(p.receptor_nombre || '—') + (p.receptor_relacion ? ' · ' + _peEsc(p.receptor_relacion) : '') + '</span></div>';
    if (p.receptor_rut) html += '<div class="pe-kv"><span class="pe-kv-k">RUT receptor</span><span class="pe-kv-v" style="font-family:monospace">' + _peEsc(p.receptor_rut) + '</span></div>';
    if (p.entregado_at) html += '<div class="pe-kv"><span class="pe-kv-k">Hora entrega</span><span class="pe-kv-v">' + _peEsc(p.entregado_at) + '</span></div>';
    if (p.lat && p.lng) html += '<div class="pe-kv"><span class="pe-kv-k">GPS entrega</span><span class="pe-kv-v"><button type="button" class="pe-gps-btn" onclick="_ilusMapModal(' + p.lat + ',' + p.lng + ',\'Punto de entrega\')">Ver punto</button></span></div>';
    if (p.firma_url) html += '<div class="pe-firma"><img src="' + p.firma_url + '" alt="Firma" onclick="_ilusLightbox(this.src, \'Firma\')"></div>';
    if (p.fotos && p.fotos.length) {
      // FIX 2026-07-29: array completo en una var global para poder pasar
      // entre las fotos desde el lightbox (antes cada <img> abria SU sola
      // foto sin poder avanzar a la siguiente).
      window._peFotosActuales = p.fotos;
      html += '<div class="pe-fotos mt-2">';
      p.fotos.forEach(function(f, i){ html += '<img src="' + f + '" alt="Foto de entrega" onclick="_ilusLightbox(window._peFotosActuales, ' + i + ', \'Foto de entrega\')">'; });
      html += '</div>';
    }
    html += '</div>';
  } else {
    html += '<div class="pe-sec"><div class="pe-h"><i class="bi bi-shield-check"></i>Prueba de entrega</div>'
          + '<div style="color:#9ca3af;font-size:.85rem">Sin evidencia registrada todavía (el transportista no ha subido foto/firma para esta entrega).</div></div>';
  }

  html += '<div class="pe-sec"><div class="pe-h"><i class="bi bi-clock-history"></i>Historial completo</div>';
  if (d.eventos && d.eventos.length) {
    html += '<div class="pe-tl">';
    d.eventos.forEach(function(e){
      var fc = ({chofer:'pe-fnt-chofer', fedex:'pe-fnt-fedex', aftership:'pe-fnt-fedex', manual:'pe-fnt-manual', sistema:'pe-fnt-sistema'}[e.fuente] || 'pe-fnt-manual');
      html += '<div class="pe-tl-row"><div class="pe-tl-est">' + _peEsc(e.estado) + '<span class="pe-tl-fnt ' + fc + '">' + _peEsc(e.fuente) + '</span></div>';
      html += '<div class="pe-tl-ts">' + _peEsc(e.ts) + (e.usuario ? ' · ' + _peEsc(e.usuario) : '') + '</div>';
      if (e.comentario) html += '<div class="pe-tl-com">' + _peEsc(e.comentario) + '</div>';
      if (e.lat && e.lng) html += '<div class="pe-tl-com" style="font-size:.74rem"><button type="button" class="pe-gps-btn" onclick="_ilusMapModal(' + e.lat + ',' + e.lng + ',\'Ubicación del evento\')">📍 Ver lugar del evento</button></div>';
      html += '</div>';
    });
    html += '</div>';
  } else {
    html += '<div style="color:#9ca3af;font-size:.85rem">Sin eventos registrados aún.</div>';
  }
  html += '</div>';
  return html;
}

function abrirManifiestoFirma() {
  var url = '/transporte/manifiestos/' + MID + '/firma';
  document.getElementById('manifiestoFirmaFrame').src = url;
  document.getElementById('mfPdfLink').href = url + '/pdf';
  if (!_manifiestoFirmaModal) {
    _manifiestoFirmaModal = new bootstrap.Modal(document.getElementById('manifiestoFirmaModal'));
  }
  _manifiestoFirmaModal.show();
  mrCargarRetiro();
}

// ═══════════════════════════════════════════════════════════════════
// Retiro de bodega (2026-07-26): chofer + RUT + teléfono + patente +
// firma digital simple (nombre tecleado). Un registro por manifiesto,
// editable. Al guardar, el backend intenta enviar el PDF firmado al
// correo del courier automáticamente (best-effort).
// ═══════════════════════════════════════════════════════════════════
var _mrLinkFirma = null;
var _mrCourierId = null;
var _mrChoferesRoster = [];
var _mrPesoTotalKg = 0;
var _mrVolumenTotalM3 = 0;

async function mrCargarRetiro() {
  document.getElementById('mrLoading').style.display = '';
  document.getElementById('mrReadonly').style.display = 'none';
  document.getElementById('mrForm').style.display = 'none';
  try {
    var r = await fetch('/transporte/manifiestos/' + MID + '/retiro');
    var d = await r.json();
    document.getElementById('mrLoading').style.display = 'none';
    _mrLinkFirma = d.link_firma || null;
    _mrCourierId = d.courier_id || null;
    _mrPesoTotalKg = d.peso_total_kg || 0;
    _mrVolumenTotalM3 = d.volumen_total_m3 || 0;
    mrCargarChoferesRoster();
    if (d.ok && d.retiro) {
      mrMostrarReadonly(d.retiro);
    } else {
      mrToggleForm(false);
    }
  } catch (e) {
    document.getElementById('mrLoading').style.display = 'none';
    mrToggleForm(false);
  }
}

async function mrCargarChoferesRoster() {
  var wrap = document.getElementById('mrChoferGuardadoWrap');
  var sel = document.getElementById('mrChoferGuardado');
  _mrChoferesRoster = [];
  if (!_mrCourierId) { wrap.style.display = 'none'; return; }
  try {
    var r = await fetch('/transporte/couriers/' + _mrCourierId + '/choferes');
    var d = await r.json();
    if (d.ok && d.choferes && d.choferes.length) {
      _mrChoferesRoster = d.choferes;
      sel.innerHTML = '<option value="">+ Nuevo chofer (escribir a mano)</option>' +
        d.choferes.map(function(c){
          return '<option value="' + c.id + '">' + c.nombre.replace(/[<>&]/g, function(x){return {'<':'&lt;','>':'&gt;','&':'&amp;'}[x];}) + ' — ' + (c.rut || '') + '</option>';
        }).join('');
      wrap.style.display = '';
    } else {
      wrap.style.display = 'none';
    }
  } catch (e) {
    wrap.style.display = 'none';
  }
}

var _mrChoferSeleccionado = null;

function mrAplicarChoferGuardado(choferId) {
  var aviso = document.getElementById('mrCapacidadAviso');
  if (!choferId) { _mrChoferSeleccionado = null; if (aviso) aviso.innerHTML = ''; return; }
  var c = _mrChoferesRoster.find(function(x){ return String(x.id) === String(choferId); });
  if (!c) return;
  _mrChoferSeleccionado = c;
  document.getElementById('mrChoferNombre').value = c.nombre || '';
  document.getElementById('mrChoferRut').value = c.rut || '';
  document.getElementById('mrChoferTelefono').value = c.telefono || '';
  document.getElementById('mrPatente').value = c.patente || '';
  // FIX 2026-07-27 (Daniel: "comparativa con el camión, para restringir el
  // manifiesto según su límite de peso/volumen"): capacidad guardada en el
  // perfil del chofer/camión (peso_max_kg/volumen_max_m3), comparada contra
  // el total real del manifiesto (ya calculado por el backend).
  if (!aviso) return;
  var excedePeso = c.peso_max_kg && _mrPesoTotalKg > parseFloat(c.peso_max_kg);
  var excedeVol  = c.volumen_max_m3 && _mrVolumenTotalM3 > parseFloat(c.volumen_max_m3);
  if (excedePeso || excedeVol) {
    var msgs = [];
    if (excedePeso) msgs.push('Peso: ' + _mrPesoTotalKg + ' kg vs. máximo ' + c.peso_max_kg + ' kg');
    if (excedeVol) msgs.push('Volumen: ' + _mrVolumenTotalM3 + ' m³ vs. máximo ' + c.volumen_max_m3 + ' m³');
    aviso.innerHTML = '<div class="alert alert-warning py-2 px-3 mb-0" style="font-size:.85rem">'
      + '<i class="bi bi-exclamation-triangle-fill me-1"></i><b>Este manifiesto excede la capacidad del camión de ' + (c.patente || c.nombre) + ':</b><br>'
      + msgs.join(' · ') + '</div>';
  } else {
    aviso.innerHTML = '';
  }
}

async function mrCopiarLinkFirma() {
  if (!_mrLinkFirma) {
    ilusToast('Aún no se pudo generar el link. Reintenta.', { type: 'warning' });
    return;
  }
  await ilusAlert({
    title: 'Link para que el chofer firme',
    message: 'Comparte este link con el chofer (SMS/WhatsApp) para que confirme el retiro desde su celular, sin necesidad de iniciar sesión:',
    sub: '<div style="margin:.5rem 0;padding:.6rem .7rem;background:#f3f4f6;border-radius:8px;'
       + 'font-family:ui-monospace,Consolas,monospace;font-size:.78rem;word-break:break-all;border:1px solid #e5e7eb">'
       + _mrLinkFirma + '</div>',
    subHtml: true,
    type: 'info',
    okLabel: 'Copiar link',
  });
  try {
    await navigator.clipboard.writeText(_mrLinkFirma);
    ilusToast('✓ Link copiado al portapapeles', { type: 'success' });
  } catch (e) {
    await ilusPrompt({
      title: 'Copiar link manualmente',
      message: 'El navegador bloqueó el portapapeles. Copia el enlace con Ctrl+C.',
      defaultValue: _mrLinkFirma,
      okLabel: 'Listo',
      cancelLabel: 'Cerrar',
      required: false,
    });
  }
}

// FIX 2026-07-27 (Daniel: "mejorar cómo enviar por WhatsApp"): arma el
// mensaje + link ya redactados y abre WhatsApp Web/app directo — si hay
// teléfono (del campo del formulario, que se autocompleta al elegir un
// chofer del roster), se lo manda a ESE número; si no, abre WhatsApp para
// elegir el contacto a mano (igual sirve para compartir el link).
async function mrEnviarWhatsapp() {
  if (!_mrLinkFirma) {
    ilusToast('Aún no se pudo generar el link. Reintenta.', { type: 'warning' });
    return;
  }
  var tel = (document.getElementById('mrChoferTelefono').value || '').replace(/[^\d+]/g, '');
  var msg = 'Hola, por favor confirma el retiro de este manifiesto firmando desde tu celular: ' + _mrLinkFirma;
  var url = 'https://wa.me/' + (tel ? tel.replace(/^\+/, '') : '') + '?text=' + encodeURIComponent(msg);
  window.open(url, '_blank');
}

function mrMostrarReadonly(retiro) {
  document.getElementById('mrRoNombre').textContent = retiro.chofer_nombre || '';
  document.getElementById('mrRoRut').textContent = retiro.chofer_rut || '';
  document.getElementById('mrRoPatente').textContent = retiro.patente || '';
  document.getElementById('mrRoFirma').textContent = retiro.firma_nombre || '';
  if (retiro.chofer_telefono) {
    document.getElementById('mrRoTel').textContent = retiro.chofer_telefono;
    document.getElementById('mrRoTelWrap').style.display = '';
  } else {
    document.getElementById('mrRoTelWrap').style.display = 'none';
  }
  window._mrUltimoRetiro = retiro;
  document.getElementById('mrReadonly').style.display = '';
  document.getElementById('mrForm').style.display = 'none';
}

// editar=true precarga el formulario con el registro existente (para corregir datos)
function mrToggleForm(editar) {
  document.getElementById('mrReadonly').style.display = 'none';
  var retiro = editar ? window._mrUltimoRetiro : null;
  document.getElementById('mrChoferGuardado').value = '';
  document.getElementById('mrChoferNombre').value = retiro ? (retiro.chofer_nombre || '') : '';
  document.getElementById('mrChoferRut').value = retiro ? (retiro.chofer_rut || '') : '';
  document.getElementById('mrChoferTelefono').value = retiro ? (retiro.chofer_telefono || '') : '';
  document.getElementById('mrPatente').value = retiro ? (retiro.patente || '') : '';
  document.getElementById('mrFirmaNombre').value = '';
  document.getElementById('mrForm').style.display = '';
}

async function mrGuardar(ev) {
  ev.preventDefault();
  var btn = document.getElementById('mrSubmitBtn');
  var payload = {
    chofer_nombre:   document.getElementById('mrChoferNombre').value.trim(),
    chofer_rut:      document.getElementById('mrChoferRut').value.trim(),
    chofer_telefono: document.getElementById('mrChoferTelefono').value.trim(),
    patente:         document.getElementById('mrPatente').value.trim(),
    firma_nombre:    document.getElementById('mrFirmaNombre').value.trim(),
  };
  if (!payload.chofer_nombre || !payload.chofer_rut || !payload.patente || !payload.firma_nombre) {
    ilusToast('Completa los campos obligatorios (*)', { type: 'warning' });
    return false;
  }
  // FIX 2026-07-27 (Daniel: "restrinja el manifiesto según el límite del
  // camión"): si el chofer elegido del roster tiene capacidad cargada y el
  // manifiesto la excede, no se bloquea en silencio -- se exige una
  // confirmación explícita antes de guardar.
  if (_mrChoferSeleccionado) {
    var c = _mrChoferSeleccionado;
    var excedePeso = c.peso_max_kg && _mrPesoTotalKg > parseFloat(c.peso_max_kg);
    var excedeVol  = c.volumen_max_m3 && _mrVolumenTotalM3 > parseFloat(c.volumen_max_m3);
    if (excedePeso || excedeVol) {
      var ok = await ilusConfirm({
        title: 'El manifiesto excede la capacidad del camión',
        message: 'El camión de ' + (c.patente || c.nombre) + ' tiene un límite cargado y este manifiesto lo excede.',
        sub: (excedePeso ? ('Peso: ' + _mrPesoTotalKg + ' kg vs. máximo ' + c.peso_max_kg + ' kg. ') : '')
           + (excedeVol ? ('Volumen: ' + _mrVolumenTotalM3 + ' m³ vs. máximo ' + c.volumen_max_m3 + ' m³.') : ''),
        okLabel: 'Confirmar igual', cancelLabel: 'Cancelar', danger: true,
      });
      if (!ok) return false;
    }
  }
  btn.disabled = true;
  btn.innerHTML = '<i class="bi bi-hourglass-split me-1"></i>Guardando…';
  try {
    var r = await fetch('/transporte/manifiestos/' + MID + '/retiro', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(payload),
    });
    var d = await r.json();
    btn.disabled = false;
    btn.innerHTML = '<i class="bi bi-check2-circle me-1"></i>Confirmar retiro y firmar';
    if (d.ok) {
      var msg = d.enviado_a
        ? ('✓ Retiro registrado. Manifiesto enviado a ' + d.enviado_a)
        : '✓ Retiro registrado.';
      ilusToast(msg, { type: 'success' });
      if (d.aviso) {
        await ilusAlert({ title: 'Aviso', message: d.aviso, type: 'warning' });
      }
      // Si el chofer no coincide con ninguno del roster guardado, se ofrece
      // agregarlo para la próxima vez (no automático — pedido de Daniel).
      var yaEnRoster = _mrChoferesRoster.some(function(c){
        return (c.rut || '').replace(/\./g,'').toUpperCase() === payload.chofer_rut.replace(/\./g,'').toUpperCase();
      });
      if (_mrCourierId && !yaEnRoster) {
        var agregar = await ilusConfirm({
          title: 'Guardar chofer',
          message: '¿Guardar a ' + payload.chofer_nombre + ' en el roster de este courier para la próxima vez?',
          okLabel: 'Guardar', cancelLabel: 'No, gracias',
        });
        if (agregar) {
          try {
            await fetch('/transporte/couriers/' + _mrCourierId + '/choferes', {
              method: 'POST',
              headers: {'Content-Type': 'application/json'},
              body: JSON.stringify({
                nombre: payload.chofer_nombre, rut: payload.chofer_rut,
                telefono: payload.chofer_telefono, patente: payload.patente,
              }),
            });
            ilusToast('✓ Chofer guardado en el roster', { type: 'success' });
          } catch (e) { /* best-effort */ }
        }
      }
      mrCargarRetiro();
    } else {
      await ilusAlert({
        title: 'No se pudo registrar el retiro',
        message: d.error || 'Error desconocido',
        type: 'error',
      });
    }
  } catch (e) {
    btn.disabled = false;
    btn.innerHTML = '<i class="bi bi-check2-circle me-1"></i>Confirmar retiro y firmar';
    ilusToast('Error de conexión al guardar', { type: 'error' });
  }
  return false;
}

async function enviarManifiestoFirmaCorreo() {
  var destStr = await ilusPrompt({
    title: 'Enviar manifiesto por correo',
    message: 'Ingresa los correos destinatarios (separados por coma)',
    placeholder: 'ejemplo1@correo.cl, ejemplo2@correo.cl',
    required: true,
  });
  if (!destStr) return;
  var destinatarios = destStr.split(',').map(function(s){ return s.trim(); }).filter(Boolean);
  if (!destinatarios.length) {
    ilusToast('No ingresaste ningún correo', { type: 'warning' });
    return;
  }
  ilusToast('Enviando manifiesto…', { type: 'info' });
  try {
    var r = await fetch('/transporte/manifiestos/' + MID + '/firma/enviar', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({destinatarios: destinatarios}),
    });
    var d = await r.json();
    if (d.ok) {
      ilusToast('✓ Enviado a ' + d.enviados.join(', '), { type: 'success' });
      if (d.fallidos && d.fallidos.length) {
        await ilusAlert({
          title: 'Algunos correos no se pudieron enviar',
          message: 'Fallaron: ' + d.fallidos.join(', '),
          type: 'warning',
        });
      }
    } else {
      await ilusAlert({
        title: 'No se pudo enviar el manifiesto',
        message: d.error || 'Error desconocido',
        type: 'error',
      });
    }
  } catch(e) {
    ilusToast('Error de conexión al enviar', { type: 'error' });
  }
}

// 2026-07-29: funciones de "Agregar factura" (abrirAgregarFactura/agBuscar/
// agAgregar) ELIMINADAS con autorizacion explicita de Daniel — el flujo
// oficial para agregar facturas es Asignar y Cotizar (cubicador) o el Monitor.

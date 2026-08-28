// ══════════════ Nueva cotización desde ERP — 2026-07-12 ══════════════
// Reusa el modal compartido tickets/_tka_modal.html (mode:'seleccionar'),
// mismo contrato que catalogo/list.html y tickets/list.html/ficha.html.
// Precio unitario queda en 0 (la fase de tarifas/pricing se difiere).
//
// 2026-07-15 (Blueprint Cotizaciones Fase 1): onSeleccionar ahora recibe
// (items, header) -- el header trae cliente/rut/email/telefono ya
// resueltos por el modal al cargar el documento. Se manda al backend para
// que la cotización nazca con el cliente real (antes quedaba siempre NULL).
//
// ══════ Wizard "Crear Cotización" (2026-07-22, réplica Triple A;
// 2026-07-24, Daniel: "un solo modal con scroll, sin pestañas ni
// Siguiente" -- rediseño estilo "Generar OT") ══════
// Un único scroll continuo con 5 secciones numeradas (Origen, Datos del
// cliente, Contacto, Condiciones, Productos y servicios) y un solo botón
// "Crear Cotización" al final -- ya no hay pestañas ni Paso 1/Paso 2. La
// tabla de ítems clasifica cada producto (característica) y el precio se
// calcula en vivo. La suma del resumen replica al backend
// (_tk_cotiz_recalcular) y al calculateTotals de Triple A: subtotal =
// ítems + ruta; descuento sobre subtotal; IVA sobre (subtotal − descuento).
let _WIZ = null;
let _wizEjecutivosCargados = false;

function cotWizAbrir(editCid){
  // editCid (2026-07-22, Daniel: "si selecciono la fila, me deje entrar a la
  // edición"): si viene un id, el wizard entra en MODO EDICIÓN -- precarga la
  // cotización y guarda con /actualizar en vez de /desde-erp.
  // BLINDAJE (bug real en producción 2026-07-22): el botón "Crear Cotización"
  // está enlazado con addEventListener('click', cotWizAbrir) -- el navegador
  // le pasa el PointerEvent como primer argumento, que aquí se disfrazaba de
  // editCid y disparaba GET /cotizaciones/[object PointerEvent] → 404
  // "Recurso no encontrado" + cierre inmediato del modal. Solo un entero
  // positivo cuenta como id de edición; cualquier otra cosa = crear.
  editCid = (typeof editCid === 'number' && isFinite(editCid) && editCid > 0)
    ? Math.floor(editCid)
    : ((typeof editCid === 'string' && /^\d+$/.test(editCid)) ? parseInt(editCid, 10) : null);
  // 2026-07-23 (Daniel: el dropdown de Google nunca aparecía en Dirección).
  // Cinturón y tirantes -- ver el fix real más abajo en _cotWizInitDireccion();
  // esta llamada es idempotente (guard input.dataset.placesBound) y cubre
  // cualquier caso donde la inicialización de más abajo hubiera fallado.
  _cotWizInitDireccion();
  // 2026-07-23 (Daniel, rediseño estilo "Generar OT"): máquina de estados
  // `origen` -- null|'documento'|'erp_cliente'|'ficha'|'manual'|'ticket'
  // (el deep-link ?desde_cliente=<CID> setea origen='ficha', no un valor
  // propio) -- controla la botonera del Paso 2 (ver
  // _cotWizAplicarOrigenPaso2) y viaja en el payload de creación como dato
  // de trazabilidad (columna tk_cotizaciones.origen, 100% informativo).
  _WIZ = { items: [], tipo: 'mantencion', rutaManual: false, sumaItems: 0, direccionValidada: false,
           origen: null, clienteId: null, ticketId: null, planInfo: null, docRef: null,
           fichaContactos: [], fichaMaquinas: [], editCid: null };
  ['cotWizCliQ','cotWizEmpresa','cotWizRut','cotWizDireccion','cotWizRegion','cotWizComuna',
   'cotWizEmail','cotWizTelefono','cotWizNotas','cotWizNotasInt',
   'cotWizDireccionLat','cotWizDireccionLng','cotWizDireccionPlaceId',
   'cotWizContactoNombre','cotWizContactoCargo','cotWizContactoTel','cotWizContactoEmail',
   'cotWizAlcance','cotWizRecomendacion','cotWizTerminos','cotWizDiasHabiles','cotWizFrecuenciaAnual'].forEach(function(id){
    const el = document.getElementById(id); if (el) el.value = '';
  });
  document.getElementById('cotWizDescModo').value = 'pct';
  document.getElementById('cotWizDescValor').value = '0';
  cotWizDescModoCambio();
  document.getElementById('cotWizCostoRuta').value = '0';
  document.getElementById('cotWizRutaExcluida').checked = false;
  document.getElementById('cotWizEjecutivo').value = '';
  _cotWizToggleContenidoPdf();
  document.getElementById('cotWizRutaHint').textContent = 'Se obtiene automáticamente según la comuna del cliente';
  document.getElementById('cotWizDireccionHint').innerHTML = '';
  document.querySelectorAll('.cot-wiz-campo.invalido, .cot-wiz-campo.valido').forEach(function(c){ c.classList.remove('invalido', 'valido'); });
  _cotWizRenderPlanBanner();
  _cotWizRenderContactos([]);
  _cotWizSetContactoExpandido(false);
  _cotWizAplicarOrigenPaso2();
  // "Válido hasta" = hoy + 15 días HÁBILES de Chile (saltando fines de
  // semana + feriados), calculado en el backend (2026-07-22, Daniel: "15
  // días hábiles de Chile... eso va a ser la sugerencia interna"). Queda
  // editable. Prefill instantáneo con +15 corridos (LOCAL, no UTC) y luego
  // se refina con la sugerencia real del backend cuando responde -- así el
  // campo nunca queda vacío aunque el endpoint tarde/falle.
  const _p2 = function(n){ return String(n).padStart(2, '0'); };
  const _fechaLocal = function(dt){ return dt.getFullYear() + '-' + _p2(dt.getMonth() + 1) + '-' + _p2(dt.getDate()); };
  const _tmp = new Date(); _tmp.setDate(_tmp.getDate() + 15);
  document.getElementById('cotWizValidoHasta').value = _fechaLocal(_tmp);
  fetch('/tickets/api/cotizaciones/valido-hasta-sugerido?dias=15')
    .then(function(r){ return r.json(); })
    .then(function(d){ if (d && d.ok && d.fecha) document.getElementById('cotWizValidoHasta').value = d.fecha; })
    .catch(function(){ /* se queda con el prefill de +15 corridos */ });
  document.querySelectorAll('.cot-wiz-ts-pill').forEach(function(b){
    b.classList.toggle('on', b.dataset.v === 'mantencion');
  });
  document.getElementById('cotWizBody').innerHTML =
    '<tr class="cot-wiz-vacio"><td colspan="7">Sin productos todavía — usa "Agregar de Bodega 02"</td></tr>';
  cotWizCargarEjecutivos();
  _cotWizCargarUf();
  cotWizResumen();
  // 2026-07-24 (Daniel: "un solo modal con scroll, sin pestañas ni
  // Siguiente"): ya no hay Paso a activar -- solo se asegura que el
  // scroll del panel arranque arriba del todo (Sección 1).
  const _panelScroll = document.querySelector('#cotWizModal .cot-clasif-panel');
  if (_panelScroll) _panelScroll.scrollTop = 0;
  // Título + botón según modo (crear vs editar).
  _cotWizSetModo(!!editCid);
  const m = document.getElementById('cotWizModal');
  m.classList.add('is-open'); m.style.display = 'flex';
  if (editCid) _cotWizCargarEdicion(editCid);
}
// Ajusta el título/botón del wizard según crear vs editar.
function _cotWizSetModo(esEdicion){
  const t = document.getElementById('cotWizTitulo');
  const b = document.getElementById('btnCotWizCrear');
  if (t) t.textContent = esEdicion ? 'Editar cotización' : 'Crear cotización';
  if (b) b.innerHTML = esEdicion
    ? '<i class="bi bi-save me-1"></i>Guardar cambios'
    : '<i class="bi bi-check-lg me-1"></i>Crear Cotización';
}
// Precarga una cotización existente en el wizard (modo edición).
async function _cotWizCargarEdicion(cid){
  try{
    const r = await fetch('/tickets/api/cotizaciones/' + cid);
    const d = await r.json();
    if (!d.ok){ ilusToast(d.error || 'No se pudo cargar la cotización', {type:'error'}); cotWizCerrar(); return; }
    const c = d.cotizacion;
    _WIZ.editCid = cid;
    _WIZ.editEstado = c.estado || 'draft';
    _WIZ.origen = c.origen || null;
    _WIZ.clienteId = c.cliente_id || null;
    _WIZ.ticketId = c.ticket_id || null;
    // Botonera/reglas de la Sección 5 coherentes con el origen precargado
    // (revisión Fable: sin esto, editar una cotización de ficha dejaba
    // habilitada la pill Instalación).
    _cotWizAplicarOrigenPaso2();
    // Espera el catálogo de ejecutivos ANTES de setear el valor (revisión
    // Fable: carrera en la primera apertura -- el select aún no tenía la
    // <option> y el ejecutivo guardado se descartaba, bloqueando el guardado).
    try{ await cotWizCargarEjecutivos(); }catch(e){}
    const setV = function(id, v){ const el = document.getElementById(id); if (el) el.value = (v == null ? '' : v); };
    setV('cotWizEmpresa', c.empresa); setV('cotWizRut', c.rut); setV('cotWizEmail', c.email);
    setV('cotWizTelefono', c.telefono); setV('cotWizComuna', c.comuna); setV('cotWizRegion', c.region);
    setV('cotWizDireccion', c.direccion); setV('cotWizDireccionLat', c.direccion_lat);
    setV('cotWizDireccionLng', c.direccion_lng); setV('cotWizDireccionPlaceId', c.direccion_place_id);
    setV('cotWizNotas', c.notas); setV('cotWizNotasInt', c.notas_internas);
    setV('cotWizContactoNombre', c.contacto_nombre); setV('cotWizContactoCargo', c.contacto_cargo);
    setV('cotWizContactoTel', c.contacto_tel); setV('cotWizContactoEmail', c.contacto_email);
    setV('cotWizAlcance', c.alcance); setV('cotWizRecomendacion', c.recomendacion);
    setV('cotWizTerminos', c.terminos); setV('cotWizDiasHabiles', c.dias_habiles_estimado);
    setV('cotWizFrecuenciaAnual', c.frecuencia_anual); setV('cotWizCostoRuta', c.costo_ruta || 0);
    document.getElementById('cotWizRutaExcluida').checked = !!c.ruta_excluida;
    setV('cotWizEjecutivo', c.ejecutivo);
    if (c.valida_hasta) setV('cotWizValidoHasta', c.valida_hasta);
    // Descuento (modo + valor)
    document.getElementById('cotWizDescModo').value = c.descuento_tipo || 'pct';
    document.getElementById('cotWizDescValor').value = (c.descuento_tipo === 'monto') ? (c.descuento_monto || 0) : (c.descuento_pct || 0);
    cotWizDescModoCambio();
    // Tipo de servicio
    _WIZ.tipo = c.tipo_servicio || 'mantencion';
    document.querySelectorAll('.cot-wiz-ts-pill').forEach(function(b){ b.classList.toggle('on', b.dataset.v === _WIZ.tipo); });
    _cotWizToggleContenidoPdf();
    if (c.contacto_nombre || c.contacto_email) _cotWizSetContactoExpandido(true);
    // Ítems
    _WIZ.items = (d.items || []).map(function(it){
      return { sku: it.sku || '', nombre: it.nombre || '', qty: (it.qty == null ? 1 : it.qty),
               tido: it.tido || null, nudo: it.nudo || null, koen: null,
               clase_producto: it.clase_producto || null,
               vaneli_original: (it.vaneli_original != null ? it.vaneli_original : null),
               precio_manual: (String(it.clase_producto || '').toLowerCase() === 'accesorio'
                 ? null : (it.precio_manual != null ? it.precio_manual : null)) };
    });
    _WIZ.rutaManual = (parseInt(c.costo_ruta, 10) || 0) > 0;
    // 2026-07-24 (Daniel, insiste: "quiero ver los productos que gestiono
    // en cotización con la clasificación... lo quiero ver allí [en el
    // catálogo]"): a diferencia de "agregar desde ficha"/"desde otra
    // fuente", este camino (abrir una cotización YA GUARDADA para editar)
    // nunca llamaba a cotWizClasificar() -- los ítems quedaban sin "pid"
    // (el id de cat_productos), así que cotWizClase() → _cotWizGuardarClaseEnCatalogo()
    // se abortaba en silencio (`if (!it.pid) return;`) cada vez que Daniel
    // cambiaba una clasificación en una cotización existente: nunca
    // llegaba al catálogo, sin ningún error visible. cotWizClasificar() no
    // pisa clase_producto si ya viene seteada (solo rellena "pid"), así
    // que es seguro llamarla acá también.
    await cotWizClasificar();
    await cotWizRender();
    await cotWizPrecios();
  }catch(e){
    ilusToast('Error de conexión al cargar la cotización', {type:'error'});
    cotWizCerrar();
  }
}
function cotWizCerrar(){
  const m = document.getElementById('cotWizModal');
  if (m){ m.classList.remove('is-open'); m.style.display = 'none'; }
  _WIZ = null;
}

// ── Fecha ISO ("YYYY-MM-DD", columna DATE sin componente horario -- no
//    aplica conversión de huso Chile, Regla #6 es para DATETIME) a DD/MM/YYYY ──
function _cotWizFechaCL(iso){
  if (!iso) return '';
  const p = String(iso).split('-');
  return (p.length === 3) ? (p[2] + '/' + p[1] + '/' + p[0]) : String(iso);
}

// ── Sección 2: franja "Cliente con contrato vigente" (informativo puro --
//    Daniel: "que el plan afecte el precio es lógica de pricing que
//    requiere reglas mías, no la inventes" -- NO se toca el cálculo). ──
function _cotWizRenderPlanBanner(){
  const banner = document.getElementById('cotWizPlanBanner');
  if (!banner) return;
  const plan = _WIZ && _WIZ.planInfo;
  if (!plan || !plan.activo || !plan.contratos || !plan.contratos.length){
    banner.style.display = 'none'; banner.innerHTML = '';
    return;
  }
  const c = plan.contratos[0];
  const venceTxt = c.es_indefinido ? 'contrato indefinido'
    : (c.fecha_vencimiento ? ('vence ' + _cotWizFechaCL(c.fecha_vencimiento)) : 'sin fecha de vencimiento registrada');
  const freqTxt = c.frecuencia_meses ? (' · mantención cada ' + c.frecuencia_meses + ' mes' + (c.frecuencia_meses === 1 ? '' : 'es')) : '';
  banner.innerHTML = '<i class="bi bi-patch-check-fill"></i><span>Cliente con contrato vigente: <strong>' +
    _cotEsc(c.nombre || 'Contrato') + '</strong> · ' + _cotEsc(venceTxt) + freqTxt + '</span>';
  banner.style.display = 'flex';
}

// ── Sección 3: contactos conocidos de la ficha (select) + expandir/colapsar ──
function _cotWizSetContactoExpandido(open){
  const body = document.getElementById('cotWizContactoBody');
  const btn = document.getElementById('cotWizContactoToggleBtn');
  if (!body) return;
  body.style.display = open ? '' : 'none';
  if (btn) btn.innerHTML = open ? '<i class="bi bi-dash-lg"></i> Ocultar' : '<i class="bi bi-plus-lg"></i> Agregar contacto';
}
function cotWizToggleContacto(){
  const body = document.getElementById('cotWizContactoBody');
  if (!body) return;
  _cotWizSetContactoExpandido(body.style.display === 'none');
}
function _cotWizRenderContactos(contactos){
  if (_WIZ) _WIZ.fichaContactos = contactos || [];
  const wrap = document.getElementById('cotWizContactoSelectWrap');
  const sel = document.getElementById('cotWizContactoSelect');
  if (!wrap || !sel) return;
  if (!contactos || !contactos.length){
    wrap.style.display = 'none';
    sel.innerHTML = '<option value="">Ingresar manualmente</option>';
    return;
  }
  sel.innerHTML = '<option value="">Ingresar manualmente</option>' + contactos.map(function(c, i){
    return '<option value="' + i + '">' + _cotEsc(c.label || 'Contacto') + ' — ' + _cotEsc(c.nombre || '') + '</option>';
  }).join('');
  wrap.style.display = '';
  // Hay al menos un contacto conocido -- se expande solo (Daniel lo ve de
  // inmediato, no tiene que acordarse de tocar "+ Agregar contacto").
  _cotWizSetContactoExpandido(true);
  sel.value = '0';
  cotWizContactoElegir('0');
}
function cotWizContactoElegir(idx){
  const nombre = document.getElementById('cotWizContactoNombre');
  const cargo = document.getElementById('cotWizContactoCargo');
  const tel = document.getElementById('cotWizContactoTel');
  const email = document.getElementById('cotWizContactoEmail');
  if (idx === '' || idx == null){
    if (nombre) nombre.value = ''; if (cargo) cargo.value = '';
    if (tel) tel.value = ''; if (email) email.value = '';
    return;
  }
  const c = (_WIZ && _WIZ.fichaContactos || [])[parseInt(idx, 10)];
  if (!c) return;
  if (nombre) nombre.value = c.nombre || '';
  if (cargo) cargo.value = c.cargo || '';
  if (tel) tel.value = c.tel || '';
  if (email) email.value = c.email || '';
}

// ── Resumen de ficha (endpoint 2.2.2) -- trae contactos + plan/contratos +
//    máquinas activas de un cliente YA reconocido en Mantenciones. Se
//    llama al elegir un resultado del buscador con cliente_id, y desde el
//    deep-link ?desde_cliente=<CID> (ver más abajo, PR-5). ──
async function _cotWizCargarFichaResumen(cid){
  if (!cid || !_WIZ) return;
  try{
    const r = await fetch('/tickets/api/clientes-ficha/' + cid + '/resumen');
    const d = await r.json();
    if (!d.ok || !_WIZ) return;
    _WIZ.planInfo = d.plan || null;
    _WIZ.fichaMaquinas = d.maquinas || [];
    _cotWizRenderPlanBanner();
    _cotWizRenderContactos(d.contactos || []);
    // La disponibilidad de "Traer equipos de la ficha" depende de
    // fichaMaquinas, que recién llegó -- refrescar la botonera del Paso 2
    // (no-op si el usuario todavía está en el Paso 1, los ids existen igual).
    _cotWizAplicarOrigenPaso2();
  }catch(e){ /* silencioso -- no bloquea el wizard, el dato es informativo */ }
}

// ── Paso 2: botonera condicional por origen (Daniel: "ya no hay necesidad
//    de mostrar ese botón, o déjalo bloqueado" -- para el caso 'documento').
//    #btnCotWizDoc (el histórico "Asignar desde documento") queda:
//      · visible pero DESHABILITADO cuando origen ya trajo ítems de un
//        documento/ticket (evita duplicar la misma acción dos veces);
//      · oculto en cualquier otro origen -- la capacidad de traer un
//        documento se movió al Paso 1 ("Desde documento ERP"), sigue
//        existiendo, solo cambió de lugar (Regla #4.2, confirmado con
//        Daniel en el resumen de este PR).
//    El botón "Agregar de Bodega 02" y "Traer equipos de la ficha" se
//    cablean en la siguiente iteración (Paso 2) -- esta función ya deja
//    funcionando el estado de btnCotWizDoc desde ahora. ──
function _cotWizVacioTexto(){
  // Texto de la fila vacía de la tabla -- cambia según qué botón es la
  // entrada principal para ESTE origen (Daniel, rediseño Paso 2).
  const origen = _WIZ ? _WIZ.origen : null;
  if (origen === 'ficha' && _WIZ && (_WIZ.fichaMaquinas || []).length){
    return 'Sin productos todavía — usa "Traer equipos de la ficha" o "Agregar de Bodega 02"';
  }
  return 'Sin productos todavía — usa "Agregar de Bodega 02"';
}
function _cotWizAplicarOrigenPaso2(){
  const btnDoc = document.getElementById('btnCotWizDoc');
  const btnTraerEquipos = document.getElementById('btnCotWizTraerEquipos');
  const hint = document.getElementById('cotWizPaso2Hint');
  const origen = _WIZ ? _WIZ.origen : null;
  const esDocumento = (origen === 'documento' || origen === 'ticket');
  const tieneEquiposFicha = origen === 'ficha' && _WIZ && (_WIZ.fichaMaquinas || []).length > 0;
  if (btnDoc){
    if (esDocumento){
      btnDoc.style.display = '';
      btnDoc.disabled = true;
      btnDoc.title = (_WIZ && _WIZ.docRef)
        ? ('Los productos ya vinieron del documento ' + _WIZ.docRef)
        : 'Los productos ya vinieron de este ticket';
    } else {
      btnDoc.style.display = 'none';
      btnDoc.disabled = false;
      btnDoc.title = '';
    }
  }
  if (btnTraerEquipos) btnTraerEquipos.style.display = tieneEquiposFicha ? '' : 'none';
  if (!tieneEquiposFicha){
    // Se ocultó el botón -- si el panel de equipos había quedado abierto,
    // ciérralo (evita un panel huérfano sin botón que lo controle).
    const panel = document.getElementById('cotWizEquiposFichaPanel');
    if (panel){ panel.style.display = 'none'; panel.innerHTML = ''; }
  }
  if (hint){
    if (esDocumento){
      hint.textContent = 'Los productos ya vinieron del documento' +
        (_WIZ && _WIZ.docRef ? (' ' + _WIZ.docRef) : '') +
        ' — usa "Agregar de Bodega 02" si necesitas sumar algo extra.';
    } else if (tieneEquiposFicha){
      hint.textContent = 'Trae los equipos ya registrados en la ficha del cliente, o suma productos manuales desde Bodega 02.';
    } else {
      hint.textContent = 'Busca productos en Bodega 02 y agrégalos a la cotización.';
    }
  }
  // La fila vacía de la tabla (si no hay ítems todavía) también refleja
  // el origen -- solo se repinta si de verdad está vacía, para no perder
  // ítems ya cargados.
  if (_WIZ && !_WIZ.items.length){
    const body = document.getElementById('cotWizBody');
    if (body) body.innerHTML = '<tr class="cot-wiz-vacio"><td colspan="7">' + _cotWizVacioTexto() + '</td></tr>';
  }
  // Choke point único por donde pasan TODOS los cambios de _WIZ.origen --
  // ideal para aplicar la regla de "Instalación" bloqueada (ver abajo).
  _cotWizAplicarReglaTipoServicio();
}

// ── Sección 4: origen 'ficha' bloquea "Instalación" (Daniel 2026-07-24:
//    la cotización que nace de la ficha de un cliente YA existente es
//    mantención/visita técnica, no instalación). Solo Instalación se
//    bloquea -- Venta de repuesto y Otro siguen habilitados para
//    cualquier origen (Daniel nombró explícitamente solo instalación). ──
function _cotWizAplicarReglaTipoServicio(){
  const esFicha = !!(_WIZ && _WIZ.origen === 'ficha');
  const pill = document.querySelector('.cot-wiz-ts-pill[data-v="instalacion"]');
  if (pill){
    if (esFicha && !pill.classList.contains('disabled')){
      pill.dataset.titleOrig = pill.title || '';
      pill.title = 'No disponible cuando la cotización nace de la ficha del cliente (solo mantención o visita técnica)';
      pill.classList.add('disabled');
    } else if (!esFicha && pill.classList.contains('disabled')){
      pill.title = pill.dataset.titleOrig || '';
      pill.classList.remove('disabled');
    }
  }
  if (esFicha && _WIZ && _WIZ.tipo === 'instalacion'){
    _WIZ.tipo = 'mantencion';
    document.querySelectorAll('.cot-wiz-ts-pill').forEach(function(b){
      b.classList.toggle('on', b.dataset.v === 'mantencion');
    });
    cotWizPrecios();
    ilusToast('Origen ficha: el tipo volvió a Mantención — instalación no aplica', {type:'info'});
  }
}

// ── Validación Paso 1 (2026-07-22, Daniel: "todos los valores obligatorios,
//    que son todos menos las observaciones y el descuento") ──
function _cotWizEmailValido(v){
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test((v || '').trim());
}
function _cotWizTelValido(v){
  const d = String(v || '').replace(/\D/g, '');
  const sinPais = d.startsWith('56') ? d.slice(2) : d;
  return sinPais.length === 8 || sinPais.length === 9;
}
function _cotWizMarcar(id, ok){
  const campo = document.getElementById(id);
  if (campo){
    campo.classList.toggle('invalido', !ok);
    // 2026-07-24 (Daniel, validación visual verde): solo los campos que
    // traen el div .cot-wiz-campo-ok (Email/Teléfono) se marcan .valido.
    if (campo.querySelector('.cot-wiz-campo-ok')) campo.classList.toggle('valido', !!ok);
    else campo.classList.remove('valido');
  }
  return ok;
}
function cotWizValidarPaso1(){
  let ok = true;
  const v = function(id){ return document.getElementById(id).value.trim(); };
  ok = _cotWizMarcar('campoEmpresa', !!v('cotWizEmpresa')) && ok;
  ok = _cotWizMarcar('campoRut', !!v('cotWizRut')) && ok;
  ok = _cotWizMarcar('campoComuna', !!v('cotWizComuna')) && ok;
  ok = _cotWizMarcar('campoEjecutivo', !!v('cotWizEjecutivo')) && ok;
  ok = _cotWizMarcar('campoValidoHasta', !!v('cotWizValidoHasta')) && ok;
  ok = _cotWizMarcar('campoEmail', _cotWizEmailValido(v('cotWizEmail'))) && ok;
  ok = _cotWizMarcar('campoTelefono', _cotWizTelValido(v('cotWizTelefono'))) && ok;
  // Dirección: NO basta con que tenga texto -- tiene que haberse VALIDADO
  // contra Google Places (Daniel: "hay que validar la dirección... y
  // colocar mensajes"). Un cliente traído del ERP puede traer comuna sin
  // haber pasado por Places -- igual se exige revalidar.
  const direccionOk = !!v('cotWizDireccion') && !!v('cotWizDireccionLat');
  ok = _cotWizMarcar('campoDireccion', direccionOk) && ok;
  ok = _cotWizMarcar('campoRegion', !!v('cotWizRegion')) && ok;
  return ok;
}

// ── Validación visual VERDE de Email/Teléfono en vivo (2026-07-24, Daniel:
//    no solo marcar rojo cuando está mal -- confirmar en verde cuando está
//    bien). En `input` se pinta verde apenas es válido (nunca rojo
//    mientras se sigue tecleando); en `blur` se aplica la marca completa
//    (rojo si quedó inválido y no vacío) vía _cotWizMarcar/cotWizValidarPaso1. ──
(function(){
  const email = document.getElementById('cotWizEmail');
  if (email){
    email.addEventListener('input', function(){
      const campo = document.getElementById('campoEmail');
      if (!campo) return;
      const v = email.value.trim();
      if (!v){ campo.classList.remove('invalido', 'valido'); return; }
      if (_cotWizEmailValido(v)){ campo.classList.add('valido'); campo.classList.remove('invalido'); }
      else { campo.classList.remove('valido'); }
    });
    email.addEventListener('blur', function(){
      const campo = document.getElementById('campoEmail');
      const v = email.value.trim();
      if (!v){ if (campo) campo.classList.remove('invalido', 'valido'); return; }
      _cotWizMarcar('campoEmail', _cotWizEmailValido(v));
    });
  }
  const tel = document.getElementById('cotWizTelefono');
  if (tel){
    tel.addEventListener('input', function(){
      const campo = document.getElementById('campoTelefono');
      if (!campo) return;
      const v = tel.value.trim();
      if (!v){ campo.classList.remove('invalido', 'valido'); return; }
      if (_cotWizTelValido(v)){ campo.classList.add('valido'); campo.classList.remove('invalido'); }
      else { campo.classList.remove('valido'); }
    });
    tel.addEventListener('blur', function(){
      const campo = document.getElementById('campoTelefono');
      const v = tel.value.trim();
      if (!v){ if (campo) campo.classList.remove('invalido', 'valido'); return; }
      _cotWizMarcar('campoTelefono', _cotWizTelValido(v));
    });
  }
})();

function cotWizTipoServ(btn){
  if (!_WIZ) return;
  // 2026-07-24 (Daniel, origen ficha bloquea Instalación -- ver
  // _cotWizAplicarReglaTipoServicio): un pill .disabled ya tiene
  // pointer-events:none por CSS, este guard es cinturón y tirantes por si
  // se dispara vía teclado/tabindex en vez de click.
  if (btn.classList.contains('disabled')) return;
  _WIZ.tipo = btn.dataset.v;
  document.querySelectorAll('.cot-wiz-ts-pill').forEach(function(b){ b.classList.toggle('on', b === btn); });
  cotWizPrecios();
  _cotWizToggleContenidoPdf();
}
// 2026-07-22: alcance/recomendación/días/frecuencia solo aplican al PDF
// "rico" de Mantención (ver modo_rico en tk_cotizacion_pdf) -- se ocultan
// para el resto de los tipos de servicio, igual criterio visual que el
// resto del wizard (Regla #4.2: campos opcionales, nunca obligatorios).
function _cotWizToggleContenidoPdf(){
  const on = !!(_WIZ && _WIZ.tipo === 'mantencion');
  ['campoContenidoPdfMantencion', 'campoContenidoPdfMantencion2',
   'campoContenidoPdfMantencion3', 'campoContenidoPdfMantencion4'].forEach(function(id){
    const el = document.getElementById(id);
    if (el) el.style.display = on ? '' : 'none';
  });
}
async function cotWizCargarEjecutivos(){
  const sel = document.getElementById('cotWizEjecutivo');
  if (!sel || _wizEjecutivosCargados) return;
  try{
    const r = await fetch('/tickets/api/asignables');
    const lista = await r.json();
    if (Array.isArray(lista) && lista.length){
      lista.forEach(function(u){
        const o = document.createElement('option');
        o.value = u.nombre || u.email || '';
        o.textContent = u.nombre || u.email || '';
        sel.appendChild(o);
      });
      _wizEjecutivosCargados = true;
    }
  }catch(e){ /* sin lista: queda "Seleccione", no bloquea */ }
}

// ── Búsqueda de cliente (ERP read-only; mismo endpoint del modal Nuevo Ticket) ──
let _wizCliTimer = null, _wizCliSeq = 0;
function _wizCliOcultar(){
  const dd = document.getElementById('cotWizCliResultados');
  if (dd) dd.style.display = 'none';
}
(function(){
  const inp = document.getElementById('cotWizCliQ');
  if (!inp) return;
  inp.addEventListener('input', function(){
    clearTimeout(_wizCliTimer);
    const q = inp.value.trim();
    if (q.length < 2){ _wizCliOcultar(); return; }
    _wizCliTimer = setTimeout(async function(){
      const seq = ++_wizCliSeq;
      try{
        const r = await fetch('/tickets/api/erp/buscar-cliente?q=' + encodeURIComponent(q));
        const d = await r.json();
        if (seq !== _wizCliSeq) return;
        const cont = document.getElementById('cotWizCliResultados');
        const res = (d && d.resultados) || [];
        if (!res.length){
          // 2026-07-22 (Daniel: "también existe la posibilidad de crear un
          // cliente como en Triple A"): accion explicita, no un mensaje
          // muerto -- igual que el "¿Deseas crear un nuevo cliente?" del
          // cotizador de Triple A.
          // data-q en vez de argumento inline (revisión Fable): un nombre
          // con apóstrofe ("D'ANGELO") rompía el onclick generado.
          cont.innerHTML = '<div class="cot-wiz-dd-vacio">Sin resultados en el ERP para "' + _cotEsc(q) + '"</div>' +
            '<button type="button" class="cot-wiz-dd-crear" data-q="' + _cotEsc(q) +
              '" onclick="cotWizCrearClienteNuevo(this.dataset.q)"><i class="bi bi-person-plus-fill me-1"></i>Crear cliente nuevo</button>';
        } else {
          cont.innerHTML = res.map(function(c, i){
            // 2026-07-23 (Daniel, rediseño Paso 1): badge "En plan" junto al
            // ya existente "Ya es cliente" -- 100% informativo, viene del
            // endpoint (plan_activo/plan_vence, PR-2), NO afecta el precio.
            const badges = (c.ya_es_cliente ? '<em>✓ Ya es cliente</em>' : '') +
              (c.plan_activo ? '<em>🟢 En plan' + (c.plan_vence ? ' · vence ' + _cotWizFechaCL(c.plan_vence) : '') + '</em>' : '');
            return '<button type="button" class="cot-wiz-dd-item" data-i="' + i + '">' +
              '<strong>' + _cotEsc(c.empresa) + '</strong>' +
              '<span>' + _cotEsc(c.rut || '') + (c.comuna ? ' · ' + _cotEsc(c.comuna) : '') + '</span>' +
              badges +
            '</button>';
          }).join('');
          cont.querySelectorAll('.cot-wiz-dd-item').forEach(function(b){
            b.addEventListener('click', function(){ cotWizElegirCliente(res[parseInt(b.dataset.i, 10)]); });
          });
        }
        cont.style.display = 'block';
      }catch(e){ /* ERP caído: se puede seguir a mano */ }
    }, 350);
  });
  document.addEventListener('click', function(ev){
    if (ev.target !== inp && !ev.target.closest('#cotWizCliResultados')) _wizCliOcultar();
  });
})();
function cotWizElegirCliente(c){
  if (!c) return;
  // 2026-07-23 (Daniel, rediseño Paso 1): máquina de estados `origen` --
  // si el resultado ya trae cliente_id (tiene ficha en Mantenciones),
  // origen='ficha' y se carga su resumen (contactos/plan/equipos, PR-2);
  // si no, origen='erp_cliente' (existe en el ERP pero sin ficha aún).
  if (_WIZ){
    _WIZ.clienteId = c.cliente_id || null;
    if (c.cliente_id){
      _WIZ.origen = 'ficha';
      _cotWizCargarFichaResumen(c.cliente_id);
    } else {
      _WIZ.origen = 'erp_cliente';
      _WIZ.planInfo = null;
      _cotWizRenderPlanBanner();
      _cotWizRenderContactos([]);
    }
    _cotWizAplicarOrigenPaso2();
  }
  document.getElementById('cotWizEmpresa').value = c.empresa || '';
  document.getElementById('cotWizRut').value = c.rut || '';
  // 2026-07-22 (Daniel: "hay que validar la dirección... colocar
  // mensajes"): el ERP trae comuna/dirección de texto, pero NUNCA pasó por
  // Google Places -- no alcanza lat/lng/place_id ni Región. Se deja como
  // punto de partida (ahorra tipeo) pero SIN marcarla como validada; el
  // aviso guía a re-escribir y elegir una sugerencia real.
  document.getElementById('cotWizDireccionLat').value = '';
  document.getElementById('cotWizDireccionLng').value = '';
  document.getElementById('cotWizDireccionPlaceId').value = '';
  document.getElementById('cotWizRegion').value = '';
  if (c.direccion) document.getElementById('cotWizDireccion').value = c.direccion;
  if (c.comuna) document.getElementById('cotWizComuna').value = c.comuna;
  document.getElementById('cotWizDireccionHint').innerHTML =
    '<i class="bi bi-hourglass-split me-1" style="color:#6b7280"></i>' +
    'Completando la Región desde la dirección del ERP…';
  // 2026-08-14 (Daniel, reportado por Juan Pablo): antes esto dejaba la
  // Región vacía y en rojo con un "vuelve a escribir la dirección", pero
  // la dirección del ERP muchas veces NO sirve para Google Places (el caso
  // real traía "las condes", que es una comuna, no una calle) -- el usuario
  // quedaba bloqueado sin salida. Ahora se resuelve la Región por
  // Geocoding, que sí tolera una dirección vaga. La validación fina con
  // Places sigue disponible y manda si el usuario elige una sugerencia.
  _cotWizResolverRegionERP(c.direccion || '', c.comuna || '');
  if (c.contacto_email) document.getElementById('cotWizEmail').value = c.contacto_email;
  if (c.contacto_tel) document.getElementById('cotWizTelefono').value = c.contacto_tel;
  document.getElementById('cotWizCliQ').value = (c.empresa || '') + (c.rut ? ' — ' + c.rut : '');
  _wizCliOcultar();
  cotWizBuscarRuta();
}
// Resuelve la Región desde la dirección/comuna que trajo el ERP, para que
// el formulario no quede bloqueado con la Región vacía (ver el comentario
// en cotWizElegirCliente). Falla en silencio: si Google no responde o no
// hay clave configurada, se deja el aviso de siempre y el usuario puede
// escribir la Región a mano — nunca rompe el wizard.
async function _cotWizResolverRegionERP(direccion, comuna){
  const hint = document.getElementById('cotWizDireccionHint');
  const elRegion = document.getElementById('cotWizRegion');
  const avisoManual =
    '<i class="bi bi-exclamation-triangle-fill me-1" style="color:#f59e0b"></i>' +
    'Dirección del ERP sin validar — vuelve a escribirla y elige una opción de la lista.';
  try{
    const r = await fetch('/tickets/api/geo/region?direccion=' +
      encodeURIComponent(direccion) + '&comuna=' + encodeURIComponent(comuna));
    const d = await r.json();
    if (d && d.ok && d.region){
      if (elRegion) elRegion.value = d.region;
      // Si Google devolvió una comuna más precisa y el campo está vacío, la usa.
      const elCom = document.getElementById('cotWizComuna');
      if (elCom && !elCom.value && d.comuna) elCom.value = d.comuna;
      if (hint) hint.innerHTML =
        '<i class="bi bi-check-circle-fill me-1" style="color:#16a34a"></i>' +
        'Región completada desde la dirección del ERP. Si quieres la dirección exacta ' +
        '(con calle y número), vuelve a escribirla y elige una opción de la lista.';
      // Revalida para que el campo Región salga del rojo al instante
      // (es la misma función que valida el paso, línea ~412).
      if (typeof cotWizValidarPaso1 === 'function') cotWizValidarPaso1();
    } else {
      if (hint) hint.innerHTML = avisoManual;
    }
  }catch(e){
    if (hint) hint.innerHTML = avisoManual;
  }
}

// 2026-07-22 (Daniel: "también existe la posibilidad de crear un cliente
// como en Triple A"): no hay una tabla de "clientes" separada en este
// wizard -- los datos del cliente viven directo en la cotización, así
// que "crear cliente" es habilitar la carga manual con una acción clara
// en vez de un mensaje de "sin resultados" pasivo.
function cotWizCrearClienteNuevo(q){
  _wizCliOcultar();
  if (_WIZ){
    _WIZ.origen = 'manual';
    _WIZ.clienteId = null;
    _WIZ.planInfo = null;
    _cotWizRenderPlanBanner();
    _cotWizRenderContactos([]);
    _cotWizAplicarOrigenPaso2();
  }
  document.getElementById('cotWizCliQ').value = '';
  const empresaInput = document.getElementById('cotWizEmpresa');
  // Si lo que tipeó no era un RUT (probable razón social), lo precarga.
  const pareceRut = /^[\d.\-kK\s]+$/.test(q || '');
  empresaInput.value = pareceRut ? '' : (q || '');
  if (pareceRut) document.getElementById('cotWizRut').value = q || '';
  empresaInput.focus();
  ilusToast('Completa los datos del cliente nuevo abajo', {type:'info'});
}

// ── Dirección con Google Places (2026-07-22, Daniel: "verificando la
//    dirección en el buscador de Google... una vez que validemos la
//    dirección, se autocompleta la comuna y la región") — mismo patrón
//    ya usado en mant_ficha.js/_ilusInitDireccionCliente, adaptado al
//    wizard. Región queda SIEMPRE automática (readonly); Comuna se
//    autocompleta pero sigue editable por si Google no calza exacto. ──
function _cotWizLimpiaRegion(r){
  if (!r) return r;
  let s = String(r).replace(/^Regi[oó]n\s+(de\s+|del\s+|de\s+la\s+)?/i, '').trim();
  s = s.replace(/\s+de\s+Santiago$/i, '');
  return s || String(r);
}
function _cotWizInitDireccion(){
  const input = document.getElementById('cotWizDireccion');
  if (!input || input.dataset.placesBound === '1') return;
  if (typeof ilusPlacesAutocomplete !== 'function'){
    if (window.__ilusGmapsPending) window.__ilusGmapsPending.push(_cotWizInitDireccion);
    return;
  }
  input.dataset.placesBound = '1';
  ilusPlacesAutocomplete('cotWizDireccion', {
    country: 'cl',
    types: ['address'],
    onPlaceSelected: function(place){
      document.getElementById('cotWizDireccionLat').value = place.lat || '';
      document.getElementById('cotWizDireccionLng').value = place.lng || '';
      document.getElementById('cotWizDireccionPlaceId').value = place.place_id || '';
      const comps = place.componentes || [];
      const pick = function(){
        for (let i = 0; i < arguments.length; i++){
          const t = arguments[i];
          const c = comps.find(function(x){ return (x.types || []).indexOf(t) >= 0; });
          if (c) return c.long_name;
        }
        return '';
      };
      // CL: level_1=Región · level_3/locality=Comuna
      const region = pick('administrative_area_level_1');
      const comuna = pick('administrative_area_level_3', 'locality', 'sublocality_level_1');
      document.getElementById('cotWizRegion').value = _cotWizLimpiaRegion(region);
      if (comuna) document.getElementById('cotWizComuna').value = comuna;
      const hint = document.getElementById('cotWizDireccionHint');
      const la = (typeof place.lat === 'number') ? place.lat.toFixed(4) : '?';
      const ln = (typeof place.lng === 'number') ? place.lng.toFixed(4) : '?';
      hint.innerHTML = '<i class="bi bi-check-circle-fill me-1" style="color:#16a34a"></i>' +
        'Dirección verificada · <small>' + la + ', ' + ln + '</small>';
      _cotWizMarcar('campoDireccion', true);
      _cotWizMarcar('campoRegion', !!region);
      if (comuna) cotWizBuscarRuta();
    },
    onNoSelection: function(){
      const hint = document.getElementById('cotWizDireccionHint');
      hint.innerHTML = '<i class="bi bi-exclamation-triangle me-1" style="color:#f59e0b"></i>' +
        'Elige una opción de la lista para validar la dirección.';
    }
  });
}
// 2026-07-23 (Daniel, con foto: "no me sale el drop down... powered by
// Google"). CAUSA REAL: esta llamada corría INLINE durante el parseo del
// HTML, antes de que static/ilus_ui.js (cargado con `defer`) y
// window.__ilusGmapsPending (definido en base.html, más abajo en el
// documento) existieran -- el guard de _cotWizInitDireccion() fallaba en
// silencio y la función nunca se reintentaba. Los scripts `defer` corren
// ANTES de DOMContentLoaded, así que esperar ese evento garantiza que
// ilusPlacesAutocomplete y __ilusGmapsPending ya existan. Comparar con el
// modal Generar OT (ficha.html), que SÍ funciona porque inicializa Places
// recién al abrir el modal (show.bs.modal), nunca al parsear la página.
if (document.readyState === 'loading'){
  document.addEventListener('DOMContentLoaded', _cotWizInitDireccion);
} else {
  _cotWizInitDireccion();
}

async function cotWizBuscarRuta(){
  if (!_WIZ || _WIZ.rutaManual) return;
  const comuna = document.getElementById('cotWizComuna').value.trim();
  if (!comuna) return;
  const hint = document.getElementById('cotWizRutaHint');
  try{
    const r = await fetch('/tickets/api/cotizaciones/costo-ruta?comuna=' + encodeURIComponent(comuna));
    const d = await r.json();
    if (d.ok && d.encontrado){
      document.getElementById('cotWizCostoRuta').value = d.costo;
      hint.textContent = 'Ruta encontrada para ' + (d.comuna || comuna) +
        (d.region ? ' (' + d.region + ')' : '') + ' — puedes ajustar el monto';
    } else {
      hint.textContent = 'Comuna sin ruta cargada — ingresa el costo manualmente';
    }
    cotWizRecalcLocal();
  }catch(e){ /* silencioso */ }
}
(function(){
  const cr = document.getElementById('cotWizCostoRuta');
  if (cr) cr.addEventListener('input', function(){ if (_WIZ) _WIZ.rutaManual = true; cotWizRecalcLocal(); });
  const cm = document.getElementById('cotWizComuna');
  if (cm) cm.addEventListener('change', cotWizBuscarRuta);
  const cre = document.getElementById('cotWizRutaExcluida');
  if (cre) cre.addEventListener('change', cotWizRecalcLocal);
  // El descuento (modo + valor) ya dispara cotWizResumen via onchange/oninput
  // inline en el HTML -- no hace falta un listener extra aquí.
})();

// ── Asignar desde documento ERP -- lógica COMPARTIDA entre el botón
//    histórico del Paso 2 (#btnCotWizDoc, hoy visible-deshabilitado
//    cuando origen ya es 'documento'/'ticket' -- ver _cotWizAplicarOrigenPaso2)
//    y el botón nuevo del Paso 1, Sección 1 ("Desde documento ERP",
//    #btnCotWizDesdeDocumento -- Daniel 2026-07-23, rediseño estilo
//    "Generar OT"). Se extrajo a una función única para no duplicar la
//    validación de "documento de otro cliente" ni el merge de header. ──
async function _cotWizAsignarDesdeDocumento(items, header){
  if (!_WIZ || !items || !items.length) return;
  // 2026-07-23 (Daniel: "los datos tienen que coincidir con el cliente,
  // porque si no sería una vulneración"): si el Paso 1 YA tiene un RUT
  // cargado (de una búsqueda previa) y el documento que se está asignando
  // pertenece a un RUT DISTINTO, avisar antes de traer los ítems -- nunca
  // mezclar en silencio los datos de dos clientes distintos.
  const rutActual = document.getElementById('cotWizRut').value.trim();
  const _rutLimpio = function(v){ return String(v || '').replace(/[^0-9kK]/g, '').toUpperCase(); };
  if (header && header.rut && rutActual && _rutLimpio(header.rut) !== _rutLimpio(rutActual)){
    const continuar = await ilusConfirm({
      title: 'El documento es de otro cliente',
      message: 'Este documento pertenece al RUT ' + header.rut + ', pero los Datos del cliente ya tienen cargado el RUT ' + rutActual + '.',
      sub: 'Si continúas, se agregarán los productos igual pero los Datos del cliente NO se van a tocar.',
      okLabel: 'Agregar productos igual', cancelLabel: 'Cancelar', danger: true,
    });
    if (!continuar) return;
  }
  // El header del documento SOLO rellena lo que el usuario dejó vacío en
  // el paso 1 (lo escrito a mano manda).
  if (header){
    const _completados = [];
    if (!document.getElementById('cotWizEmpresa').value.trim() && (header.cliente || header.empresa)){
      document.getElementById('cotWizEmpresa').value = header.cliente || header.empresa;
      _completados.push('empresa');
    }
    if (!rutActual && header.rut){
      document.getElementById('cotWizRut').value = header.rut;
      _completados.push('RUT');
    }
    if (!document.getElementById('cotWizEmail').value.trim() && header.email){
      document.getElementById('cotWizEmail').value = header.email;
      _completados.push('email');
    }
    if (!document.getElementById('cotWizTelefono').value.trim() && header.telefono){
      document.getElementById('cotWizTelefono').value = header.telefono;
      _completados.push('teléfono');
    }
    if (!document.getElementById('cotWizComuna').value.trim() && header.comuna){
      document.getElementById('cotWizComuna').value = header.comuna;
      _completados.push('comuna');
      cotWizBuscarRuta();
    }
    if (!document.getElementById('cotWizDireccion').value.trim() && header.direccion){
      document.getElementById('cotWizDireccion').value = header.direccion;
      _completados.push('dirección');
    }
    if (_completados.length){
      ilusToast('Cliente completado desde el documento (' + _completados.join(', ') + ')', {type:'info'});
    }
    // Origen 'documento' + referencia del documento (para el aviso del
    // Paso 2 -- ver _cotWizAplicarOrigenPaso2). Un cliente_id de una ficha
    // ya conocida (elegido en el Paso 1) NO se pisa por asignar un doc.
    if (header.tido && header.nudo){
      _WIZ.docRef = header.tido + '-' + header.nudo;
    }
  }
  // 2026-07-23 (revisión adversarial): 'manual' (cliente nuevo tipeado a
  // mano) es tan deliberado como 'ficha'/'erp_cliente' -- no se pisa solo
  // porque después se le asignen productos de un documento (el dato es
  // puramente informativo/auditoría, pero debe reflejar de dónde vino
  // realmente EL CLIENTE, no el último botón que se tocó).
  if (_WIZ.origen !== 'ficha' && _WIZ.origen !== 'erp_cliente' && _WIZ.origen !== 'manual'){
    _WIZ.origen = 'documento';
  }
  _cotWizAplicarOrigenPaso2();
  const nuevos = items.map(function(it){
    return { sku: it.sku || '', nombre: it.nombre || '', qty: (it.qty == null ? 1 : it.qty),
             // Multidocumento (2026-07-22): cada ítem conserva su documento
             // ERP de origen (tido+nudo) para la columna "Documento".
             tido: it.tido || null, nudo: it.nudo || null, koen: it.koen, clase_producto: null };
  });
  _WIZ.items = _WIZ.items.concat(nuevos);
  await cotWizClasificar();
  await cotWizRender();
  await cotWizPrecios();
}

// ── Sección 1: "Desde otra fuente" -- abre DIRECTO el modal compartido
//    _tka_modal.html con sus 4 pestañas (Daniel 2026-07-25, feedback en
//    vivo: "no quiero la lista, quiero el modal de búsqueda avanzada de
//    productos y clientes... por documento, por RUT, por ticket y por
//    ficha de cliente"). Reemplaza el mini-dropdown propio de la ronda
//    anterior (#cotWizFuenteDD: menú de 4 opciones + búsqueda inline con
//    debounce para ticket/ficha) -- ese dropdown ERA justo "la lista" que
//    Daniel rechazó, y su búsqueda solo disparaba con q.length>=2 (abrir
//    la pestaña sin escribir nada mostraba "no me trae nada"). Las
//    pestañas 'ticket'/'ficha' del modal compartido ahora traen LISTA POR
//    DEFECTO al abrirse (ver _tka_modal.html, tkaSetTab) -- resuelve
//    ambos síntomas de una sola vez, sin duplicar UI.
//
//    onSeleccionar (pestañas 'doc'/'cli') es EXACTAMENTE el mismo flujo de
//    siempre -- su lógica interna NO se tocó (Regla #4.2). onSeleccionarEntidad
//    (pestañas 'ticket'/'ficha', nuevas en _tka_modal.html) solo reenvía al
//    deep-link YA CONSTRUIDO y probado la ronda anterior (con sus guardias
//    ilusConfirm intactas -- ver _cotWizAplicarDeepLinkTicket /
//    _cotWizAplicarDeepLinkCliente más abajo).
function cotWizFuenteDocumento(){
  const rutActual = document.getElementById('cotWizRut').value.trim();
  tkaOpen({
    mode: 'seleccionar',
    tabs: ['doc', 'cli', 'ticket', 'ficha'],
    rutPrefill: rutActual,
    onSeleccionar: async function(items, header){
      if (!items || !items.length) return;
      // Caso borde (Fable, plan 2.1): la pestaña "Por RUT" puede traer
      // ítems de VARIOS documentos a la vez -- ahí el modal entrega
      // header=null (no hay UN documento único). Se rescata el cliente
      // pidiendo el documento del PRIMER ítem con tido/nudo (el mismo
      // motor unificado que ya usa el resto de Tickets).
      if (!header){
        const first = items.find(function(it){ return it.tido && it.nudo; });
        if (first){
          try{
            const rd = await fetch('/tickets/api/erp/documento/' + encodeURIComponent(first.tido) + '/' + encodeURIComponent(first.nudo));
            const dd = await rd.json();
            if (dd.ok && dd.documento){
              header = {
                tido: first.tido, nudo: first.nudo,
                cliente: dd.documento.cliente_nombre, rut: dd.documento.cliente_rut,
                email: dd.documento.email, telefono: dd.documento.telefono,
                comuna: dd.documento.comuna,
                // 2026-07-22 (Daniel: "la dirección no la está trayendo y
                // ese motor ya estaba listo para traer todo"): el endpoint
                // SIEMPRE devolvió direccion -- solo faltaba pasarla.
                direccion: dd.documento.direccion,
              };
            }
          }catch(e){ /* silencioso -- se sigue sin header, el usuario completa a mano */ }
        }
      }
      await _cotWizAsignarDesdeDocumento(items, header);
    },
    onSeleccionarEntidad: async function(sel){
      if (!sel) return;
      if (sel.tipo === 'ticket') await _cotWizAplicarDeepLinkTicket(sel.id);
      else if (sel.tipo === 'ficha') await _cotWizAplicarDeepLinkCliente(sel.id, { autoAgregarEquipos: true });
    }
  });
}
(function(){
  const btn = document.getElementById('btnCotWizDesdeDocumento');
  if (!btn) return;
  btn.addEventListener('click', function(){ cotWizFuenteDocumento(); });
})();

// ── Paso 2: botón histórico "Asignar desde documento" -- se mantiene
//    (Regla #4.2: la capacidad NO se elimina, solo se movió arriba como
//    entrada principal). Hoy queda visible-deshabilitado cuando el origen
//    ya es 'documento'/'ticket' y OCULTO en los demás orígenes -- ver
//    _cotWizAplicarOrigenPaso2(). Si algún día se re-habilita, sigue
//    funcionando exactamente igual (mismo helper compartido). ──
(function(){
  const btn = document.getElementById('btnCotWizDoc');
  if (!btn) return;
  btn.addEventListener('click', function(){
    if (btn.disabled) return;
    // 2026-08-25: se agrega tabs explícito (antes no tenía) para que las
    // pestañas nuevas "Catálogo"/"Cotización interna" (agregadas al modal
    // para Tickets) no aparezcan acá -- este botón es específicamente
    // "asignar DESDE UN DOCUMENTO", buscar dentro de otra cotización
    // mientras se arma esta no tiene sentido. Mismo criterio que el resto
    // de los llamados de este archivo, que ya scopeaban tabs.
    tkaOpen({
      mode: 'seleccionar',
      tabs: ['doc', 'cli'],
      onSeleccionar: async function(items, header){
        await _cotWizAsignarDesdeDocumento(items, header);
      }
    });
  });
})();

// ── Paso 2: "Agregar producto" -- entrada PRINCIPAL de productos para
//    cualquier origen que no sea 'documento'/'ticket' (Daniel 2026-07-23).
//    Los ítems de bodega/catálogo llegan con tido:null/nudo:null (el
//    modal ya lo tolera -- ver _tka_modal.html tkaAsociarSeleccion,
//    selBodega/selCatalogo). 2026-08-25 (Daniel, en vivo: "cuando
//    presiono aca aun me sale la bodega 18 quiero el maestro de
//    productos"): antes solo mostraba la pestaña "Productos" (ERP,
//    Bodega 02 -- lo que Daniel llamaba "bodega 18"). Se agrega
//    "catalogo" (cat_productos, el maestro) como pestaña PRINCIPAL
//    -- Bodega ERP se mantiene disponible como alternativa, no se quita
//    (Regla #4.2). ──
(function(){
  const btn = document.getElementById('btnCotWizBodega');
  if (!btn) return;
  btn.addEventListener('click', function(){
    tkaOpen({
      mode: 'seleccionar',
      tabs: ['catalogo', 'bodega'],
      onSeleccionar: async function(items){
        if (!_WIZ || !items || !items.length) return;
        const nuevos = items.map(function(it){
          return { sku: it.sku || '', nombre: it.nombre || '', qty: (it.qty == null ? 1 : it.qty),
                   tido: it.tido || null, koen: it.koen || null, clase_producto: null };
        });
        _WIZ.items = _WIZ.items.concat(nuevos);
        await cotWizClasificar();
        await cotWizRender();
        await cotWizPrecios();
      }
    });
  });
})();

// ── Paso 2: "Traer equipos de la ficha" -- sub-panel inline con checkboxes
//    de las máquinas activas del cliente (_WIZ.fichaMaquinas, cargadas por
//    _cotWizCargarFichaResumen cuando origen='ficha'). Solo visible/posible
//    cuando hay al menos un equipo (ver _cotWizAplicarOrigenPaso2). ──
function cotWizAbrirEquiposFicha(){
  const panel = document.getElementById('cotWizEquiposFichaPanel');
  if (!panel) return;
  const yaAbierto = panel.style.display !== 'none' && panel.innerHTML.trim() !== '';
  if (yaAbierto){ _cotWizCerrarEquiposFicha(); return; }
  _cotWizRenderEquiposFichaPanel();
}
function _cotWizCerrarEquiposFicha(){
  const panel = document.getElementById('cotWizEquiposFichaPanel');
  if (panel){ panel.style.display = 'none'; panel.innerHTML = ''; }
}
function _cotWizRenderEquiposFichaPanel(){
  const panel = document.getElementById('cotWizEquiposFichaPanel');
  if (!panel || !_WIZ) return;
  const maquinas = _WIZ.fichaMaquinas || [];
  if (!maquinas.length){ panel.style.display = 'none'; panel.innerHTML = ''; return; }
  // 2026-08-27 (Daniel: "que me reconociera los equipos que están bajo el
  // concepto de Plan, ya que si trae todo entonces es más desorden"). Los
  // equipos YA cubiertos por el plan de mantención (mant_maquinas.
  // aplica_mantencion, el mismo toggle "Poner en plan" de la ficha) casi
  // nunca son lo que hay que cotizar -- ya están pagados dentro del
  // contrato. Se separan en dos listas: la de "para cotizar" abierta y
  // marcable de entrada, y la del plan COLAPSADA aparte -- sigue
  // disponible con un clic (nada se oculta de verdad, Regla #4.2), pero
  // ya no compite por espacio con lo que sí hay que revisar.
  const itemHtml = function(m, i){
    return '<label class="cot-wiz-eqficha-item"><input type="checkbox" data-i="' + i + '">' +
      '<span>' + _cotEsc(m.nombre || m.sku || 'Equipo') +
      (m.serie ? ' <small>(' + _cotEsc(m.serie) + ')</small>' : '') + '</span></label>';
  };
  const fueraPlan = [], enPlan = [];
  maquinas.forEach(function(m, i){ (m.en_plan ? enPlan : fueraPlan).push(i); });
  const listaPrincipal = (fueraPlan.length ? fueraPlan : maquinas.map(function(_,i){return i;}));
  panel.innerHTML = '<div class="cot-wiz-eqficha-box">' +
    '<div class="cot-wiz-eqficha-head"><span>Equipos activos de la ficha (' + maquinas.length + ')</span>' +
      '<button type="button" class="cot-wiz-eqficha-close" onclick="_cotWizCerrarEquiposFicha()"><i class="bi bi-x-lg"></i></button></div>' +
    '<div class="cot-wiz-eqficha-list">' +
      listaPrincipal.map(function(i){ return itemHtml(maquinas[i], i); }).join('') +
    '</div>' +
    (fueraPlan.length && enPlan.length ? (
      '<details class="cot-wiz-eqficha-plan">' +
        '<summary>' + enPlan.length + ' equipo(s) más — ya están en el Plan de mantención</summary>' +
        '<div class="cot-wiz-eqficha-list">' +
          enPlan.map(function(i){ return itemHtml(maquinas[i], i); }).join('') +
        '</div>' +
      '</details>'
    ) : '') +
    '<div class="cot-wiz-eqficha-foot">' +
      '<button type="button" class="btn btn-sm fw-bold" style="background:#16a34a;color:#fff;border-radius:8px;padding:8px 16px;" ' +
        'onclick="_cotWizAgregarEquiposFicha()"><i class="bi bi-plus-lg me-1"></i>Agregar seleccionados</button>' +
    '</div>' +
  '</div>';
  panel.style.display = '';
}
// ── Helper compartido: agrega máquinas de la ficha como ítems de la
//    cotización (SKU/nombre/cantidad → fila de la tabla) -- usado tanto
//    por el checklist manual de abajo como por "Desde otra fuente → Por
//    ficha de cliente" (autoAgregarEquipos, ver _cotWizAplicarDeepLinkCliente). ──
async function _cotWizAgregarMaquinasComoItems(maquinas){
  if (!_WIZ || !maquinas || !maquinas.length) return;
  const nuevos = maquinas.map(function(m){
    return { sku: m.sku || '', nombre: m.nombre || 'Equipo', qty: m.cantidad || 1,
             tido: null, koen: null, clase_producto: null };
  });
  _WIZ.items = _WIZ.items.concat(nuevos);
  await cotWizClasificar();
  await cotWizRender();
  await cotWizPrecios();
  ilusToast('✓ ' + nuevos.length + ' equipo(s) agregado(s) desde la ficha', {type:'success'});
}
async function _cotWizAgregarEquiposFicha(){
  const panel = document.getElementById('cotWizEquiposFichaPanel');
  if (!panel || !_WIZ) return;
  const maquinas = _WIZ.fichaMaquinas || [];
  const marcados = Array.prototype.slice.call(panel.querySelectorAll('input[type=checkbox]:checked'));
  if (!marcados.length){ ilusToast('Selecciona al menos un equipo', {type:'warning'}); return; }
  const seleccionadas = marcados.map(function(cb){ return maquinas[parseInt(cb.dataset.i, 10)] || {}; });
  _cotWizCerrarEquiposFicha();
  await _cotWizAgregarMaquinasComoItems(seleccionadas);
}

// ── Paso 2: "Ingresar manual" -- Daniel (2026-08-20, voz a texto):
//    "dejar manual el ingreso de ítem a las cotizaciones... donde yo
//    digite el SKU, descripción, cantidad y precio". Los otros tres
//    caminos (documento ERP, bodega, ficha) siempre resuelven contra un
//    catálogo real; este es el escape hatch para lo que no está ahí
//    (ej. un cobro puntual sin SKU en Random todavía). El ítem se
//    agrega con la MISMA forma que los demás (sku/nombre/qty/
//    clase_producto) para que el resto del wizard (clasificación,
//    precios, guardado) no tenga que distinguir su origen -- salvo que
//    el precio nace en `precio_manual` en vez de calculado, igual que
//    cualquier línea donde el usuario ya escribe un precio a mano. ──
function cotWizAbrirManual(){
  const panel = document.getElementById('cotWizManualPanel');
  if (!panel) return;
  const yaAbierto = panel.style.display !== 'none' && panel.innerHTML.trim() !== '';
  if (yaAbierto){ _cotWizCerrarManual(); return; }
  _cotWizCerrarEquiposFicha();
  _cotWizRenderManualPanel();
}
function _cotWizCerrarManual(){
  const panel = document.getElementById('cotWizManualPanel');
  if (panel){ panel.style.display = 'none'; panel.innerHTML = ''; }
}
function _cotWizRenderManualPanel(){
  const panel = document.getElementById('cotWizManualPanel');
  if (!panel) return;
  panel.innerHTML =
    '<div class="cot-wiz-manual-box">' +
      '<div class="cot-wiz-manual-head"><span>Ítem manual</span>' +
        '<button type="button" class="cot-wiz-manual-close" onclick="_cotWizCerrarManual()"><i class="bi bi-x-lg"></i></button></div>' +
      '<div class="cot-wiz-manual-grid">' +
        '<div><label>SKU <span class="text-muted">(opcional)</span></label>' +
          '<input type="text" id="cotWizManualSku" class="form-control form-control-sm" placeholder="Ej: MAN-001"></div>' +
        '<div class="cot-wiz-manual-desc"><label>Descripción</label>' +
          '<input type="text" id="cotWizManualNombre" class="form-control form-control-sm" placeholder="Qué se está cobrando" autofocus></div>' +
        '<div><label>Cantidad</label>' +
          '<input type="number" id="cotWizManualQty" class="form-control form-control-sm" min="1" step="1" value="1"></div>' +
        '<div><label>Precio unitario</label>' +
          '<input type="number" id="cotWizManualPrecio" class="form-control form-control-sm" min="0" step="1" placeholder="$"></div>' +
      '</div>' +
      /* Buscador de BODEGA 18 (repuestos del ERP Random).
         Daniel (20-08-2026): "un botón bien colorido que me conecta a la
         bodega 18 para poder solicitar, sustituyendo el SKU y la
         descripción, pudiendo buscar por nombre y SKU".
         Es el MISMO comportamiento que ya pidió en agosto para el módulo
         de Repuestos, así que consume el MISMO endpoint (bodega 18 en vivo,
         SELECT puro — REGLA #4.1) en vez de duplicar la consulta. */
      '<div class="cot-b18">' +
        '<button type="button" class="cot-b18-btn" onclick="_cotB18Abrir()">' +
          '<i class="bi bi-box-seam-fill"></i>Buscar repuesto en Bodega 18' +
        '</button>' +
        '<span class="cot-b18-hint">Trae el SKU y la descripción desde el ERP</span>' +
      '</div>' +
      '<div id="cotB18Box" class="cot-b18-box" style="display:none">' +
        '<div class="cot-b18-search">' +
          '<i class="bi bi-search"></i>' +
          '<input type="text" id="cotB18Q" placeholder="Nombre o SKU del repuesto — mín. 2 letras" ' +
            'oninput="_cotB18Buscar()" autocomplete="off">' +
          '<button type="button" class="cot-b18-x" onclick="_cotB18Cerrar()" ' +
            'aria-label="Cerrar buscador"><i class="bi bi-x-lg"></i></button>' +
        '</div>' +
        '<div id="cotB18Res" class="cot-b18-res"></div>' +
      '</div>' +
      '<div class="cot-wiz-manual-foot">' +
        '<button type="button" class="btn btn-sm fw-bold" style="background:#0a0a0a;color:#fff;border-radius:8px;padding:8px 16px;" ' +
          'onclick="_cotWizAgregarManual()"><i class="bi bi-plus-lg me-1"></i>Agregar a la cotización</button>' +
      '</div>' +
    '</div>';
  panel.style.display = '';
  const first = document.getElementById('cotWizManualNombre');
  if (first) first.focus();
}
/* ══════════════════════════════════════════════════════════════════════
   BODEGA 18 — buscador de repuestos para el ítem manual
   ══════════════════════════════════════════════════════════════════════
   Daniel (20-08-2026). La bodega 18 del ERP Random ES la de Repuestos
   (app.py: REPUESTOS_BODEGA_ERP, configurable por env porque "Random puede
   renumerar bodegas sin avisar").

   Reusa /tickets/api/repuestos/bodega18 — la MISMA vista que ya servía al
   módulo de Repuestos desde agosto, ahora con un gate que suma a quien
   trabaja cotizaciones. No se duplicó la consulta al ERP: una sola query,
   un solo lugar donde arreglarla.
   ══════════════════════════════════════════════════════════════════════ */
let _COT_B18_T = null;      // debounce
let _COT_B18_SEQ = 0;       // descarta respuestas viejas que llegan tarde

function _cotB18Abrir(){
  const box = document.getElementById('cotB18Box');
  if (!box) return;
  box.style.display = '';
  const q = document.getElementById('cotB18Q');
  if (q){ q.focus(); if (q.value.trim().length >= 2) _cotB18Buscar(); }
}
function _cotB18Cerrar(){
  const box = document.getElementById('cotB18Box');
  if (box) box.style.display = 'none';
}

function _cotB18Buscar(){
  const inp = document.getElementById('cotB18Q');
  const res = document.getElementById('cotB18Res');
  if (!inp || !res) return;
  const q = (inp.value || '').trim();
  clearTimeout(_COT_B18_T);
  if (q.length < 2){
    res.innerHTML = '<div class="cot-b18-msg">Escribe al menos 2 letras.</div>';
    return;
  }
  res.innerHTML = '<div class="cot-b18-msg"><span class="cot-b18-spin"></span>Consultando bodega 18…</div>';
  const mi = ++_COT_B18_SEQ;
  _COT_B18_T = setTimeout(async () => {
    let d = null;
    try {
      const r = await fetch('/tickets/api/repuestos/bodega18?q=' + encodeURIComponent(q));
      d = await r.json();
      if (!r.ok && r.status === 403) d = {ok:false, error:'No tienes permiso para consultar bodega.'};
    } catch (e) {
      d = {ok:false, error:'Sin conexión con el ERP.'};
    }
    // Si mientras tanto se escribió otra cosa, esta respuesta ya no sirve.
    if (mi !== _COT_B18_SEQ) return;
    if (!d || !d.ok){
      res.innerHTML = '<div class="cot-b18-msg err"><i class="bi bi-exclamation-triangle-fill"></i>' +
        _cotEsc(d && d.error ? d.error : 'No se pudo consultar el ERP.') + '</div>';
      return;
    }
    const ps = d.productos || [];
    if (!ps.length){
      res.innerHTML = '<div class="cot-b18-msg">Sin resultados en bodega 18 para «' + _cotEsc(q) + '».</div>';
      return;
    }
    res.innerHTML = ps.map((p, i) => {
      const hay = Number(p.cantidad || 0);
      // El stock se informa, NUNCA bloquea: se puede cotizar algo que hay
      // que pedir. Solo cambia el color para que se vea de una.
      const cls = hay > 0 ? 'hay' : 'sin';
      const txt = hay > 0 ? (hay + ' en bodega') : 'sin stock';
      _COT_B18_CACHE[i] = p;
      return '<button type="button" class="cot-b18-item" onclick="_cotB18Elegir(' + i + ')">' +
        '<span class="sk">' + _cotEsc(p.sku || '—') + '</span>' +
        '<span class="ds">' + _cotEsc(p.descripcion || 'Sin descripción') + '</span>' +
        '<span class="st ' + cls + '">' + txt + '</span>' +
      '</button>';
    }).join('');
  }, 320);
}

const _COT_B18_CACHE = {};

function _cotB18Elegir(i){
  const p = _COT_B18_CACHE[i];
  if (!p) return;
  const sku = document.getElementById('cotWizManualSku');
  const nom = document.getElementById('cotWizManualNombre');
  // Sustituye SKU y descripción, que es literalmente lo pedido.
  if (sku){ sku.value = p.sku || ''; sku.style.background = '#dcfce7'; }
  if (nom){ nom.value = p.descripcion || ''; nom.style.background = '#dcfce7'; }
  _cotB18Cerrar();
  // El precio NO se rellena solo: lo que trae el ERP es COSTO, no precio de
  // venta. Autocompletarlo cotizaría al cliente a precio de costo.
  const pr = document.getElementById('cotWizManualPrecio');
  if (pr && !pr.value) pr.focus();
  if (typeof ilusToast === 'function'){
    ilusToast('✓ ' + (p.sku || 'Repuesto') + ' traído de bodega 18', {type:'success'});
  }
}

/* (no se define _cotEsc acá: ya existe más abajo en este mismo archivo y
   hace exactamente lo mismo. Declararla dos veces "funciona" -- la última
   gana -- pero deja dos escapadores que pueden divergir. Se usa la que ya
   estaba.) */

async function _cotWizAgregarManual(){
  if (!_WIZ) return;
  const nombre = (document.getElementById('cotWizManualNombre').value || '').trim();
  const sku = (document.getElementById('cotWizManualSku').value || '').trim();
  const qtyRaw = parseInt(document.getElementById('cotWizManualQty').value, 10);
  const qty = (isNaN(qtyRaw) || qtyRaw < 1) ? 1 : qtyRaw;
  const precioRaw = (document.getElementById('cotWizManualPrecio').value || '').replace(/\D/g, '');
  if (!nombre){
    ilusToast('Escribe una descripción para el ítem', {type:'warning'});
    return;
  }
  const nuevo = {
    sku: sku, nombre: nombre, qty: qty,
    tido: null, koen: null,
    // 2026-08-20 (Daniel): el item manual nace como "repuesto", NO sin
    // clasificar. Sin clase el motor no encuentra tarifa, el item queda en
    // $0 y la cotizacion no avanza -- "si dejo sin clasificar no me deja
    // avanzar". Y con cualquier otra clase el motor recalcula sobre su
    // tarifa y pisa lo que se escribio -- "me recalcula lo que declare".
    // "repuesto" existe justamente para esto: precio_fijo=0 en catalogo, asi
    // que nunca calcula nada por su cuenta y el precio_manual de la linea es
    // el que manda. Se puede cambiar despues desde la tabla de items.
    clase_producto: 'repuesto',
    // precio_manual nace del valor tecleado -- null (sin override) si el
    // campo quedó vacío, igual que cualquier línea sin precio manual.
    precio_manual: (precioRaw === '') ? null : Math.max(parseInt(precioRaw, 10) || 0, 0),
  };
  _WIZ.items = _WIZ.items.concat([nuevo]);
  _cotWizCerrarManual();
  await cotWizClasificar();
  await cotWizRender();
  await cotWizPrecios();
  ilusToast('✓ Ítem manual agregado', {type:'success'});
}

async function cotWizClasificar(){
  if (!_WIZ || !_WIZ.items.length) return;
  try{
    const r = await fetch('/tickets/api/cotizaciones/preview-clasificacion', {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ items: _WIZ.items.map(it => ({sku: it.sku, nombre: it.nombre, qty: it.qty})) })
    });
    const d = await r.json();
    if (d.ok && Array.isArray(d.items)){
      d.items.forEach(function(res, i){
        if (!_WIZ.items[i]) return;
        if (!_WIZ.items[i].clase_producto)
          _WIZ.items[i].clase_producto = res.clase_producto || null;
        // 2026-07-23 (Daniel: "necesito ver los movimientos... en el
        // catálogo de productos"): id de cat_productos por ítem -- lo usa
        // cotWizClase() para guardar la clase EN EL ACTO, sin esperar a
        // que la cotización completa se cree/guarde.
        _WIZ.items[i].pid = res.producto_id || null;
      });
    }
  }catch(e){ console.warn('preview-clasificacion:', e); }
}

function _cotWizEsAccesorio(it){
  return !!(it && String(it.clase_producto || '').trim().toLowerCase() === 'accesorio');
}

async function cotWizRender(){
  const body = document.getElementById('cotWizBody');
  if (!_WIZ || !_WIZ.items.length){
    body.innerHTML = '<tr class="cot-wiz-vacio"><td colspan="7">' + _cotWizVacioTexto() + '</td></tr>';
    return;
  }
  const clases = await _cotClasifCargarClases();
  const opts = clases.map(function(c){
    return '<option value="' + _cotEsc(c.value) + '">' + _cotEsc(c.label) + '</option>';
  }).join('');
  body.innerHTML = _WIZ.items.map(function(it, i){
    const accesorio = _cotWizEsAccesorio(it);
    return '<tr>' +
      '<td class="cot-rev-sku">' + _cotEsc(it.sku) + '</td>' +
      '<td class="cot-rev-nombre" title="' + _cotEsc(it.nombre) + '">' + _cotEsc(it.nombre) + '</td>' +
      '<td><input type="number" min="0" step="1" class="cot-rev-cant" value="' + _cotEsc(it.qty) + '" onchange="cotWizCantidad(' + i + ', this.value)"></td>' +
      '<td><select class="cot-rev-select' + (it.clase_producto ? '' : ' sin-clasificar') + '" data-i="' + i + '" onchange="cotWizClase(' + i + ', this)">' +
        '<option value="">Sin clasificar</option>' + opts + '</select></td>' +
      '<td class="cot-wiz-pu-cell"><input type="number" min="0" step="1" class="cot-rev-pu-input' + (accesorio ? ' accesorio-bloqueado' : '') + '" data-i="' + i + '" '
        + (accesorio ? 'value="0" disabled readonly title="Accesorio no cobrable: queda en la OT para foto y observación"'
          // 2026-08-20: restaura el precio_manual ya escrito -- sin esto,
          // CUALQUIER re-render (agregar otro ítem, cambiar cantidad de
          // otra fila) dejaba este input visualmente vacío aunque el
          // total ya lo estuviera calculando bien por debajo (el dato
          // vivía en _WIZ.items[i].precio_manual, solo no se pintaba).
          : 'placeholder="auto"' + (it.precio_manual != null ? ' value="' + _cotEsc(it.precio_manual) + '"' : '')
            + ' title="Precio base unitario — la ruta se muestra debajo" onchange="cotWizPrecioManual(' + i + ', this.value)"') + '>'
        + '<div class="cot-ruta-desglose' + (accesorio ? ' no-cobrable' : '') + '" data-ruta-i="' + i + '">' + (accesorio ? '<i class="bi bi-lock-fill"></i> $0 · no cobrable' : '') + '</div></td>' +
      '<td class="cot-wiz-tot-cell"><span class="cot-rev-precio cero">…</span></td>' +
      '<td><button type="button" class="cot-wiz-quitar" title="Quitar ítem" onclick="cotWizQuitar(' + i + ')"><i class="bi bi-x-lg"></i></button></td>' +
    '</tr>';
  }).join('');
  _WIZ.items.forEach(function(it, i){
    if (it.clase_producto){
      const s = body.querySelector('select[data-i="' + i + '"]');
      if (s) s.value = it.clase_producto;
    }
  });
}
function cotWizCantidad(i, v){
  if (!_WIZ || !_WIZ.items[i]) return;
  const n = parseInt(v, 10);
  _WIZ.items[i].qty = isNaN(n) || n < 0 ? 0 : n;
  cotWizPrecios();
}
function cotWizClase(i, sel){
  if (!_WIZ || !_WIZ.items[i]) return;
  const it = _WIZ.items[i];
  it.clase_producto = sel.value || null;
  if (_cotWizEsAccesorio(it)){
    it.precio_manual = null;
    it._precioCalc = 0;
  }
  sel.classList.toggle('sin-clasificar', !sel.value);
  cotWizRender().then(cotWizPrecios);
  _cotWizGuardarClaseEnCatalogo(it);
}
// 2026-07-23 (Daniel: "necesito ver los movimientos que he hecho de
// alimentar la clasificación de productos desde la cotización, y quiero
// ver esos cambios en el catálogo de productos"): antes, cambiar la clase
// acá era puramente local -- solo llegaba al catálogo si la cotización
// completa se creaba/guardaba. Ahora, si el ítem ya tiene un producto
// resuelto en el catálogo (pid, viene de preview-clasificacion), la clase
// se guarda EN EL ACTO. Fire-and-forget con feedback -- nunca bloquea el
// wizard ni revierte la selección si falla (el override sigue viajando
// también al crear/guardar la cotización, como red de respaldo).
async function _cotWizGuardarClaseEnCatalogo(it){
  if (!it || !it.pid) return;
  try{
    const r = await fetch('/catalogo/api/productos/' + it.pid, {
      method: 'PATCH', headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ clase_producto: it.clase_producto || '' })
    });
    const d = await r.json();
    if (d.ok) ilusToast('✓ Clase guardada en el catálogo', {type:'success'});
    else ilusToast(d.error || 'No se pudo guardar la clase en el catálogo', {type:'warning'});
  }catch(e){ ilusToast('Sin conexión — la clase no se guardó en el catálogo', {type:'warning'}); }
}
async function cotWizQuitar(i){
  if (!_WIZ) return;
  _WIZ.items.splice(i, 1);
  await cotWizRender();
  await cotWizPrecios();
}

let _wizPrecioSeq = 0;
async function cotWizPrecios(){
  if (!_WIZ) return;
  if (!_WIZ.items.length){ _WIZ.sumaItems = 0; cotWizResumen(); return; }
  // Guardia de secuencia: solo la llamada más reciente pinta el resultado
  // (una respuesta vieja llegando tarde no puede pisar precios nuevos).
  const miSeq = ++_wizPrecioSeq;
  try{
    const r = await fetch('/tickets/api/cotizaciones/preview-precio', {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify({
        tipo_servicio: _WIZ.tipo,
        items: _WIZ.items.map(it => ({clase_producto: it.clase_producto, cantidad: it.qty}))
      })
    });
    const d = await r.json();
    if (miSeq !== _wizPrecioSeq || !_WIZ) return;
    const precios = (d.ok && Array.isArray(d.precios)) ? d.precios : [];
    // Guarda el precio AUTOMÁTICO (calculado) de cada ítem; el render local
    // decide si mostrar ese o el precio_manual que el usuario haya escrito.
    _WIZ.items.forEach(function(it, i){
      const p = precios[i];
      it._precioCalc = _cotWizEsAccesorio(it) ? 0 : (p ? p.precio_unitario : null);
      if (_cotWizEsAccesorio(it)) it.precio_manual = null;
    });
    cotWizRecalcLocal();
  }catch(e){
    console.warn('preview-precio:', e);
    // 2026-07-23 (fix "queda cargando"): si el preview de precio falla, NO
    // dejar las celdas pegadas en "…" para siempre -- caer al cálculo local
    // (precio_manual o "sin clasificar") para que la UI SIEMPRE resuelva.
    // Guardado por miSeq: una respuesta vieja no pisa una más nueva.
    if (miSeq === _wizPrecioSeq && _WIZ) {
      try { cotWizRecalcLocal(); } catch(_e2){ /* nunca romper el wizard */ }
    }
  }
}

// Recalcula los totales de la tabla y del resumen SIN pegarle al backend,
// respetando el precio_manual por línea (Daniel 2026-07-22). precio efectivo
// = precio_manual si está seteado, si no el calculado. total línea = pu × qty.
function cotWizRecalcLocal(){
  if (!_WIZ) return;
  const inputsPu = document.querySelectorAll('#cotWizBody .cot-rev-pu-input');
  const filasTot = document.querySelectorAll('#cotWizBody .cot-wiz-tot-cell');
  const desgloses = document.querySelectorAll('#cotWizBody .cot-ruta-desglose');
  const rutaDetectada = Math.max(parseInt((document.getElementById('cotWizCostoRuta') || {}).value, 10) || 0, 0);
  const rutaExcluidaEl = document.getElementById('cotWizRutaExcluida');
  const rutaExcluida = !!(rutaExcluidaEl && rutaExcluidaEl.checked);
  const bases = _WIZ.items.map(function(it){
    if (_cotWizEsAccesorio(it)) return 0;
    const auto = (it._precioCalc != null) ? Math.round(it._precioCalc) : null;
    return (it.precio_manual != null) ? it.precio_manual : auto;
  });
  const unidadesCobrables = _WIZ.items.reduce(function(acc, it, i){
    const qty = Math.max(parseInt(it.qty, 10) || 0, 0);
    return acc + (!_cotWizEsAccesorio(it) && bases[i] != null && bases[i] > 0 ? qty : 0);
  }, 0);
  const rutaAplicada = (!rutaExcluida && unidadesCobrables > 0) ? rutaDetectada : 0;
  let rutaRestante = rutaAplicada;
  let filasCobrablesRestantes = _WIZ.items.filter(function(it, i){
    return !_cotWizEsAccesorio(it) && (parseInt(it.qty, 10) || 0) > 0 && bases[i] != null && bases[i] > 0;
  }).length;
  let suma = 0;
  _WIZ.items.forEach(function(it, i){
    const accesorio = _cotWizEsAccesorio(it);
    const pu = accesorio ? 0 : bases[i];
    const qty = Math.max(parseInt(it.qty, 10) || 0, 0);
    const esCobrable = !accesorio && pu != null && pu > 0 && qty > 0;
    const totBase = (pu != null) ? Math.round(pu * qty) : null;
    let rutaLinea = 0;
    if (esCobrable && rutaAplicada > 0){
      filasCobrablesRestantes -= 1;
      rutaLinea = filasCobrablesRestantes === 0
        ? rutaRestante
        : Math.round(rutaAplicada * qty / unidadesCobrables);
      rutaRestante -= rutaLinea;
    }
    const totConRuta = (totBase != null) ? totBase + rutaLinea : null;
    suma += totBase || 0;
    const inp = inputsPu[i];
    if (inp && document.activeElement !== inp){
      inp.value = (pu != null) ? pu : '';
      inp.disabled = accesorio;
      inp.readOnly = accesorio;
      inp.classList.toggle('manual', !accesorio && it.precio_manual != null);
      inp.classList.toggle('accesorio-bloqueado', accesorio);
    }
    if (desgloses[i]){
      if (accesorio){
        desgloses[i].className = 'cot-ruta-desglose no-cobrable';
        desgloses[i].innerHTML = '<i class="bi bi-lock-fill"></i> $0 · no cobrable';
      } else if (pu == null){
        desgloses[i].className = 'cot-ruta-desglose';
        desgloses[i].textContent = 'sin tarifa configurada';
      } else {
        const rutaUnidad = esCobrable && qty > 0 ? rutaLinea / qty : 0;
        desgloses[i].className = 'cot-ruta-desglose';
        desgloses[i].innerHTML = _wizCLP(pu) + ' base + ' + _wizCLP(rutaUnidad)
          + ' ruta = <b>' + _wizCLP(pu + rutaUnidad) + '/u</b>';
      }
    }
    if (filasTot[i]) filasTot[i].innerHTML = (totConRuta != null)
      ? '<span class="cot-rev-precio' + (accesorio ? ' cero' : '') + '">$' + totConRuta.toLocaleString('es-CL') + '</span>'
      : '<span class="cot-rev-precio cero">sin clasificar</span>';
  });
  _WIZ.sumaItems = suma;
  _WIZ.unidadesCobrables = unidadesCobrables;
  _WIZ.rutaAplicada = rutaAplicada;
  const hintRuta = document.getElementById('cotWizRutaProrrateoHint');
  if (hintRuta){
    hintRuta.classList.toggle('excluida', rutaExcluida || rutaAplicada === 0);
    if (rutaExcluida){
      hintRuta.textContent = 'Ruta excluida: no aumenta el precio de ningún equipo.';
    } else if (rutaDetectada > 0 && unidadesCobrables > 0){
      hintRuta.textContent = _wizCLP(rutaDetectada) + ' ÷ ' + unidadesCobrables
        + ' equipo(s) cobrable(s) = ' + _wizCLP(rutaDetectada / unidadesCobrables)
        + ' de ruta por unidad. Los accesorios quedan fuera.';
    } else if (rutaDetectada > 0){
      hintRuta.textContent = 'No hay equipos con precio: la ruta no se cobra ni se carga a accesorios.';
    } else {
      hintRuta.textContent = 'Ruta $0. Solo se reparte entre equipos con precio; los accesorios quedan fuera.';
    }
  }
  cotWizResumen();
}

// El usuario escribió un precio unitario a mano. Vacío o igual al calculado
// = vuelve a "auto" (sin override). Cualquier otro valor = override manual.
function cotWizPrecioManual(i, v){
  if (!_WIZ || !_WIZ.items[i]) return;
  const it = _WIZ.items[i];
  if (_cotWizEsAccesorio(it)){
    it.precio_manual = null;
    it._precioCalc = 0;
    cotWizRecalcLocal();
    return;
  }
  // FIX 2026-08-03 (Daniel: "1 piso vale 3500... 100x3500=350.000" no daba):
  // el input es type="number", así que si se escribe "3.500" (punto como
  // separador de miles, formato chileno) el navegador lo guarda como el
  // string "3.500" -- un decimal válido = 3.5 -- y parseInt("3.500",10)
  // trunca a 3. Se limpia cualquier caracter no-numérico ANTES de parsear
  // (los precios CLP de este wizard siempre son pesos enteros, sin
  // decimales, así que no hay ambigüedad con quitar el punto).
  const vLimpio = (v == null) ? '' : String(v).replace(/\D/g, '');
  const n = (vLimpio === '') ? null : Math.max(parseInt(vLimpio, 10) || 0, 0);
  const auto = (it._precioCalc != null) ? Math.round(it._precioCalc) : null;
  it.precio_manual = (n === null || (auto != null && n === auto)) ? null : n;
  cotWizRecalcLocal();
}

function _wizCLP(n){ return '$' + Math.round(n).toLocaleString('es-CL'); }
// 2026-07-24 (Daniel: "la fecha nuevamente es gringa, necesito día, mes,
// año" -- Regla #6): "YYYY-MM-DD" (lo que devuelve /api/uf-actual, tal
// cual viene de mindicador.cl) -> "DD/MM/YYYY". Defensivo: cualquier otra
// cosa se devuelve intacta en vez de mostrar basura.
function _ufFechaCl(iso){
  if (!iso || iso.length !== 10 || iso[4] !== '-' || iso[7] !== '-') return iso || '';
  return iso.slice(8, 10) + '/' + iso.slice(5, 7) + '/' + iso.slice(0, 4);
}

// ── Equivalente en UF del Total (2026-07-21, Daniel: "ese valor lo vamos
//    a expresar en UF, a la UF actual") ──
// Se pide cada vez que se ABRE el wizard (cotWizAbrir) y se cachea en
// _wizUf SOLO para no pegarle a la red en cada tecla dentro de esa misma
// apertura (cotWizResumen se dispara en cada tecla del formulario). El
// backend /api/uf-actual ya cachea 1h de por sí, así que esto es barato.
//
// 2026-07-24 (Daniel: "cada vez que yo entre a crear una cotización,
// entregue la última actualización... para evitar errores de cálculo"):
// ANTES, una vez que _wizUf se poblaba la primera vez, quedaba fijo para
// el resto de la sesión del navegador -- si Daniel dejaba la pestaña
// abierta todo el día y abría el wizard 10 veces, las 10 usaban la UF
// del primer fetch. Ahora cada apertura vuelve a pedirla (el guard
// _wizUfPidiendo solo evita 2 fetch simultáneos, no reemplaza el valor
// ya bueno mientras el nuevo está en camino). Si mindicador.cl está
// caído, se mantiene el último _wizUf conocido en vez de perderlo.
let _wizUf = null;
let _wizUfPidiendo = false;
async function _cotWizCargarUf(){
  if (_wizUfPidiendo) return;
  _wizUfPidiendo = true;
  try{
    const r = await fetch('/api/uf-actual');
    const d = await r.json();
    if (d && d.ok && d.uf) _wizUf = { valor: parseFloat(d.uf), fecha: d.fecha || '' };
  }catch(e){ /* sin UF disponible: se sigue mostrando solo CLP */ }
  _wizUfPidiendo = false;
  cotWizResumen();
}
function _cotWizActualizarUf(totalClp){
  const row = document.getElementById('cotWizResUfRow');
  const val = document.getElementById('cotWizResUf');
  const hint = document.getElementById('cotWizResUfHint');
  if (!row || !val) return;
  if (!_wizUf || !_wizUf.valor || !(totalClp > 0)){
    row.style.display = 'none';
    return;
  }
  const uf = totalClp / _wizUf.valor;
  const ufStr = uf.toLocaleString('es-CL', {minimumFractionDigits: 2, maximumFractionDigits: 2});
  val.textContent = '≈ ' + ufStr + ' UF';
  if (hint){
    hint.title = 'Valor UF hoy' + (_wizUf.fecha ? ' (' + _ufFechaCl(_wizUf.fecha) + ')' : '') + ': ' +
      _wizCLP(_wizUf.valor) + ' · Este monto equivale a ' + ufStr +
      ' UF, calculado desde las horas-hombre y el costo de ruta.';
  }
  row.style.display = '';
}
// Devuelve {modo:'pct'|'monto', valor:Number} del descuento del formulario.
function _cotWizDesc(){
  const modo = (document.getElementById('cotWizDescModo') || {}).value || 'pct';
  const valor = Math.max(parseFloat((document.getElementById('cotWizDescValor') || {}).value) || 0, 0);
  return {modo: modo, valor: valor};
}
function cotWizDescModoCambio(){
  const d = _cotWizDesc();
  const hint = document.getElementById('cotWizDescHint');
  if (hint) hint.textContent = d.modo === 'monto'
    ? 'Monto fijo en pesos a descontar del subtotal.'
    : 'Porcentaje aplicado sobre el subtotal (ítems + ruta).';
  cotWizResumen();
}
function cotWizResumen(){
  if (!_WIZ) return;
  const items = _WIZ.sumaItems || 0;
  // Se calcula en cotWizRecalcLocal porque depende también de cuántos
  // equipos tienen precio. Sin equipos cobrables (o con exclusión), es $0.
  const ruta = _WIZ.rutaAplicada || 0;
  const subtotal = items + ruta;
  const dsc = _cotWizDesc();
  // Descuento por % o por monto fijo (topeado al subtotal, nunca negativo).
  const desc = (dsc.modo === 'monto')
    ? Math.min(Math.round(dsc.valor), subtotal)
    : Math.round(subtotal * Math.min(dsc.valor, 100) / 100);
  const iva = Math.round((subtotal - desc) * 0.19);
  const total = subtotal - desc + iva;
  document.getElementById('cotWizResRuta').textContent = _wizCLP(ruta);
  document.getElementById('cotWizResItems').textContent = _wizCLP(items);
  document.getElementById('cotWizResSubtotal').textContent = _wizCLP(subtotal);
  document.getElementById('cotWizResDescRow').style.display = desc > 0 ? '' : 'none';
  document.getElementById('cotWizResDesc').textContent = '−' + _wizCLP(desc);
  document.getElementById('cotWizResIva').textContent = _wizCLP(iva);
  document.getElementById('cotWizResTotal').textContent = _wizCLP(total);
  _cotWizActualizarUf(total);
}

(function(){
  const btnNueva = document.getElementById('btnCotNuevaErp');
  if (btnNueva) btnNueva.addEventListener('click', cotWizAbrir);
  const btnCrear = document.getElementById('btnCotWizCrear');
  if (!btnCrear) return;
  btnCrear.addEventListener('click', async function(){
    // 2026-07-24 (Daniel: "un solo modal con scroll, sin pestañas ni
    // Siguiente"): antes esta validación completa solo corría al pasar
    // del Paso 1 al Paso 2; ahora es el único gate y corre acá, al pulsar
    // "Crear Cotización" -- también reemplaza al viejo chequeo suelto de
    // "falta empresa" (cotWizValidarPaso1 ya exige campoEmpresa).
    if (!cotWizValidarPaso1()){
      ilusToast('Completa los campos obligatorios marcados en rojo', {type:'warning'});
      const bad = document.querySelector('#cotWizModal .cot-wiz-campo.invalido');
      if (bad) bad.scrollIntoView({behavior:'smooth', block:'center'});
      return;
    }
    if (!_WIZ || !_WIZ.items.length){
      ilusToast('Agrega al menos un producto en la Sección 5', {type:'warning'});
      const s5 = document.getElementById('cotWizPaso2');
      if (s5) s5.scrollIntoView({behavior:'smooth', block:'start'});
      return;
    }
    const empresa = document.getElementById('cotWizEmpresa').value.trim();
    // 2026-07-22 (Daniel: "creó la cotización incompleta, no tiene ni
    // precio ni nada" -- bug real encontrado probando en vivo): antes se
    // podía crear con ítems SIN clasificar (quedaban en $0 para siempre,
    // sin aviso). Ahora se bloquea hasta que todos tengan característica.
    const sinClasificar = _WIZ.items.filter(function(it){ return !it.clase_producto; }).length;
    if (sinClasificar > 0){
      ilusToast('Faltan ' + sinClasificar + ' producto(s) por clasificar — sin eso no se puede calcular el precio', {type:'warning'});
      const s5b = document.getElementById('cotWizPaso2');
      if (s5b) s5b.scrollIntoView({behavior:'smooth', block:'start'});
      return;
    }
    const _orig = btnCrear.innerHTML;
    btnCrear.disabled = true;
    btnCrear.innerHTML = '<span class="spinner-border spinner-border-sm"></span>';
    try{
      const _desc = _cotWizDesc();
      const payload = {
        items: _WIZ.items.map(it => ({ sku: it.sku, nombre: it.nombre, qty: it.qty,
                                       // nudo (revisión Fable): sin él la columna
                                       // "Documento" del PDF multidoc quedaba muerta.
                                       tido: it.tido, nudo: it.nudo || null, koen: it.koen,
                                       clase_producto: it.clase_producto,
                                       vaneli_original: (it.vaneli_original != null ? it.vaneli_original : null),
                                       // precio unitario editado a mano (null = automático)
                                       precio_manual: (_cotWizEsAccesorio(it) ? null : (it.precio_manual != null ? it.precio_manual : null)) })),
        header: {
          cliente: empresa,
          rut: document.getElementById('cotWizRut').value.trim(),
          email: document.getElementById('cotWizEmail').value.trim(),
          telefono: document.getElementById('cotWizTelefono').value.trim(),
          comuna: document.getElementById('cotWizComuna').value.trim(),
        },
        tipo_servicio: _WIZ.tipo,
        region: document.getElementById('cotWizRegion').value.trim(),
        direccion: document.getElementById('cotWizDireccion').value.trim(),
        valida_hasta: document.getElementById('cotWizValidoHasta').value,
        notas: document.getElementById('cotWizNotas').value.trim(),
        notas_internas: document.getElementById('cotWizNotasInt').value.trim(),
        // Descuento por % o por monto fijo en $ (Daniel 2026-07-22).
        descuento_tipo: _desc.modo,
        descuento_pct: (_desc.modo === 'pct' ? _desc.valor : 0),
        descuento_monto: (_desc.modo === 'monto' ? Math.round(_desc.valor) : 0),
        costo_ruta: Math.max(parseInt(document.getElementById('cotWizCostoRuta').value, 10) || 0, 0),
        // 2026-07-24 (Daniel: checkbox "excluir costo de ruta... en caso
        // de que me soliciten comercial no incluirlo"): el monto de arriba
        // se guarda igual (Detalle/trazabilidad); este flag decide si
        // entra al subtotal/PDF.
        ruta_excluida: !!document.getElementById('cotWizRutaExcluida').checked,
        ejecutivo: document.getElementById('cotWizEjecutivo').value,
        // 2026-07-22: contenido opcional del PDF "rico" de Mantención (ver
        // _cotWizToggleContenidoPdf / modo_rico en tk_cotizacion_pdf).
        alcance: document.getElementById('cotWizAlcance').value.trim(),
        recomendacion: document.getElementById('cotWizRecomendacion').value.trim(),
        terminos: document.getElementById('cotWizTerminos').value.trim(),
        dias_habiles_estimado: document.getElementById('cotWizDiasHabiles').value || null,
        frecuencia_anual: document.getElementById('cotWizFrecuenciaAnual').value || null,
        // 2026-07-23 (Daniel, rediseño Paso 1 estilo "Generar OT"): contacto
        // de la persona + coordenadas de la dirección YA validada por
        // Google Places (antes se pedían pero se descartaban al enviar) +
        // cliente_id/origen para trazabilidad (PR-2, tk_cotizaciones).
        contacto: {
          nombre: document.getElementById('cotWizContactoNombre').value.trim(),
          cargo: document.getElementById('cotWizContactoCargo').value.trim(),
          tel: document.getElementById('cotWizContactoTel').value.trim(),
          email: document.getElementById('cotWizContactoEmail').value.trim(),
        },
        direccion_lat: document.getElementById('cotWizDireccionLat').value || null,
        direccion_lng: document.getElementById('cotWizDireccionLng').value || null,
        direccion_place_id: document.getElementById('cotWizDireccionPlaceId').value.trim(),
        cliente_id: (_WIZ && _WIZ.clienteId) || null,
        origen: (_WIZ && _WIZ.origen) || null,
        // 2026-07-23 (PR-5, deep-link ?desde_ticket=<TID>): si el wizard se
        // abrió desde la ficha de un ticket, la cotización nace YA asociada
        // a él (mismo criterio que el botón viejo de ficha.html -- backend
        // sin cambios, ticket_id ya era aceptado por /desde-erp).
        ticket_id: (_WIZ && _WIZ.ticketId) || null,
      };
      // MODO EDICIÓN (Daniel 2026-07-22): si el wizard se abrió sobre una
      // cotización existente, guarda con /actualizar (deja evidencia en el
      // historial); si no, crea con /desde-erp como siempre.
      const _esEdicion = !!(_WIZ && _WIZ.editCid);
      if (_esEdicion){
        if (_WIZ.editEstado === 'approved'){
          const okAprob = await ilusConfirm({
            title: 'Cotización aprobada',
            message: 'Vas a editar una cotización que YA está aprobada.',
            sub: 'Quedará registro en el historial de quién y qué cambió.',
            okLabel: 'Editar igual', cancelLabel: 'Cancelar', danger: true,
          });
          if (!okAprob){ btnCrear.disabled = false; btnCrear.innerHTML = _orig; return; }
        }
        const r = await fetch('/tickets/api/cotizaciones/' + _WIZ.editCid + '/actualizar', {
          method: 'POST', headers: {'Content-Type':'application/json'},
          body: JSON.stringify(payload)
        });
        const d = await r.json();
        if (!d.ok){
          // 2026-08-28: la guardia anti-borrado (ITEM_DOCUMENTO_ELIMINADO)
          // manda un mensaje largo y accionable -- un toast de 3.5s no
          // alcanza a leerse. Mensaje explícito con OK, como el resto de
          // los errores "importantes" del proyecto (REGLA #1/#8).
          if (d.error_codigo === 'ITEM_DOCUMENTO_ELIMINADO'){
            await ilusAlert({ title: 'No se puede guardar así', message: d.error, type: 'warning' });
          } else {
            ilusToast(d.error || 'No se pudo guardar la cotización', {type:'error'});
          }
          btnCrear.disabled = false; btnCrear.innerHTML = _orig;
          return;
        }
        ilusToast('✓ Cambios guardados' + (d.editada_post_aprobacion ? ' (quedó registrado en el historial)' : ''), {type:'success'});
        cotWizCerrar();
        setTimeout(function(){ location.reload(); }, 700);
        return;
      }
      const r = await fetch('/tickets/api/cotizaciones/desde-erp', {
        method: 'POST', headers: {'Content-Type':'application/json'},
        body: JSON.stringify(payload)
      });
      const d = await r.json();
      if (!d.ok){
        ilusToast(d.error || 'No se pudo crear la cotización', {type:'error'});
        btnCrear.disabled = false; btnCrear.innerHTML = _orig;
        return;
      }
      try{
        const rc = await fetch('/tickets/api/cotizaciones/' + d.id + '/recalcular', {method:'POST'});
        const dc = await rc.json();
        if (!dc.ok) console.warn('recalcular falló:', dc.error);
      }catch(e){ console.warn('recalcular: error de red', e); }
      ilusToast('✓ Cotización ' + (d.numero_cotizacion || '') + ' creada', {type:'success'});
      // Abre el VISOR de inmediato (carga instantánea, con Descargar dentro).
      try{ window.open('/tickets/cotizaciones/' + d.id + '/ver', '_blank'); }catch(e){}
      cotWizCerrar();
      setTimeout(function(){ location.reload(); }, 700);
    }catch(e){
      ilusToast('Error de conexión al guardar la cotización', {type:'error'});
      btnCrear.disabled = false; btnCrear.innerHTML = _orig;
    }
  });
})();

// ══════════════ Clasificación inline post-creación ══════════════
let _cotClasesCache = null;
async function _cotClasifCargarClases(){
  if (_cotClasesCache) return _cotClasesCache;
  try{
    const r = await fetch('/catalogo/api/clases');
    const d = await r.json();
    _cotClasesCache = (d && d.ok && Array.isArray(d.clases)) ? d.clases : [];
  }catch(e){
    _cotClasesCache = [];
  }
  return _cotClasesCache;
}
function _cotEsc(s){
  return (s == null ? '' : String(s))
    .replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')
    .replace(/"/g,'&quot;').replace(/'/g,'&#39;');
}
let _cotClasifCid = null;
async function cotAbrirClasificacion(pendientes, cid){
  _cotClasifCid = cid || null;
  const modal = document.getElementById('cotClasifModal');
  const cont = document.getElementById('cotClasifLista');
  if (!modal || !cont) return;
  const clases = await _cotClasifCargarClases();
  const opts = clases.map(function(c){
    return '<option value="' + _cotEsc(c.value) + '">' + _cotEsc(c.label) + '</option>';
  }).join('');
  cont.innerHTML = pendientes.map(function(it, i){
    const pid = parseInt(it.producto_id, 10) || 0;
    if (!pid) return '';
    return '<div class="cot-clasif-row" data-pid="' + pid + '">' +
      '<div class="cot-clasif-info">' +
        '<div class="cot-clasif-sku">' + _cotEsc(it.sku || '') + '</div>' +
        '<div class="cot-clasif-nombre" title="' + _cotEsc(it.nombre || it.sku || '') + '">' + _cotEsc(it.nombre || it.sku || 'Producto') + '</div>' +
      '</div>' +
      '<select class="cot-clasif-select" id="cotClasifSel' + i + '">' +
        '<option value="">Elegir clase…</option>' + opts +
      '</select>' +
      '<button type="button" class="cot-clasif-btn" onclick="cotClasifGuardar(' + pid + ', \'cotClasifSel' + i + '\', this)">Guardar</button>' +
      '<span class="cot-clasif-ok" style="display:none"><i class="bi bi-check-circle-fill"></i></span>' +
    '</div>';
  }).join('') || '<div class="cot-clasif-empty">Nada pendiente por clasificar.</div>';
  modal.classList.add('is-open');
  modal.style.display = 'flex';
}
async function cotClasifCerrar(){
  const modal = document.getElementById('cotClasifModal');
  if (modal){ modal.classList.remove('is-open'); modal.style.display = 'none'; }
  // La cotización ya quedó creada -- cerrar sin terminar de clasificar no
  // rompe nada (Regla #4.2). Recalcula precios con lo que sí se alcanzó a
  // clasificar (request nueva -- ver comentario en tk_api_cotizacion_recalcular)
  // y recarga para reflejarlo en el listado.
  if (_cotClasifCid){
    try{
      const rc = await fetch('/tickets/api/cotizaciones/' + _cotClasifCid + '/recalcular', {method:'POST'});
      const dc = await rc.json();
      if (!dc.ok) console.warn('recalcular falló:', dc.error);
    }catch(e){ console.warn('recalcular: error de red', e); }
  }
  location.reload();
}
async function cotClasifGuardar(pid, selId, btn){
  const sel = document.getElementById(selId);
  const val = sel ? sel.value : '';
  if (!val){ ilusToast('Elige una clase primero', {type:'warning'}); return; }
  const _orig = btn.innerHTML;
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner-border spinner-border-sm"></span>';
  try{
    const r = await fetch('/catalogo/api/productos/' + pid, {
      method: 'PATCH', headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ clase_producto: val })
    });
    const d = await r.json();
    if (!d.ok){
      ilusToast(d.error || 'No se pudo clasificar', {type:'error'});
      btn.disabled = false; btn.innerHTML = _orig;
      return;
    }
    if (sel) sel.disabled = true;
    btn.style.display = 'none';
    const row = btn.closest('.cot-clasif-row');
    const ok = row ? row.querySelector('.cot-clasif-ok') : null;
    if (ok) ok.style.display = 'inline-flex';
    ilusToast('✓ Producto clasificado', {type:'success'});
  }catch(e){
    ilusToast('Error de conexión al clasificar', {type:'error'});
    btn.disabled = false; btn.innerHTML = _orig;
  }
}

// ══════════════ Flujo inverso: generar un TICKET desde una cotización ══════════════
// Daniel 2026-07-15: "también quisiera generar un TICKET por una cotización".
// El botón por fila solo se pinta (ver Jinja arriba) si la cotización aún NO
// tiene ticket_id; el backend valida de nuevo por si acaso (409 si ya tiene
// uno -- una cotización no puede tener dos tickets). Sin redirect automático:
// se ofrece el link para que quien lo generó decida si entra o sigue en el
// listado (Regla #1: nada de alert()/confirm() nativos).
async function cotGenerarTicket(cid, btn){
  const _orig = btn.innerHTML;
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner-border spinner-border-sm"></span>';
  try{
    const r = await fetch('/tickets/api/cotizaciones/' + cid + '/generar-ticket', { method: 'POST' });
    const d = await r.json();
    if (!d.ok){
      ilusToast(d.error || 'No se pudo generar el ticket', {type:'error'});
      btn.disabled = false; btn.innerHTML = _orig;
      return;
    }
    const link = '<a href="/tickets/' + d.id + '" target="_blank" rel="noopener" style="color:#dc2626;font-weight:800;">'
      + _cotEsc(d.numero_ticket || ('Ticket #' + d.id)) + '</a>';
    await ilusAlert({
      title: '✅ Ticket generado',
      message: 'Se generó el ticket ' + link + ' a partir de esta cotización.',
      messageHtml: true, type: 'success', okLabel: 'Entendido',
    });
    location.reload();
  }catch(e){
    ilusToast('Error de conexión al generar el ticket', {type:'error'});
    btn.disabled = false; btn.innerHTML = _orig;
  }
}

// ══════════════ Cotizaciones v3 — tabla inteligente ══════════════
// Clic en la fila (menos en un botón/link) entra a editar.
function cotRowClick(e, id){
  if (e.target.closest('a, button, input, select')) return;
  cotEditar(id);
}
function cotEditar(id){ cotWizAbrir(id); }

// ── Chip UF del hero (2026-07-23, Daniel: "que la UF quede automática y
//    que tenga trazabilidad de dónde la estamos sacando y el día"). ──
function _cotUfChipPintar(d){
  const val = document.getElementById('cotUfChipVal');
  const meta = document.getElementById('cotUfChipMeta');
  if (!val) return;
  if (!d || !d.ok || d.uf == null){
    val.textContent = 'UF —';
    if (meta) meta.textContent = 'sin conexión';
    return;
  }
  val.textContent = 'UF $' + Number(d.uf).toLocaleString('es-CL', {minimumFractionDigits: 2, maximumFractionDigits: 2});
  // Trazabilidad visible: fecha + fuente real del valor (no solo tooltip).
  let fuente = 'mindicador.cl';
  if (d.manual) fuente = 'manual' + (d.set_by ? ' · ' + d.set_by : '');
  else if (d.stale) fuente = 'último conocido';
  if (meta) meta.textContent = (d.fecha ? _ufFechaCl(d.fecha) + ' · ' : '') + fuente;
}
async function cotUfChipRefrescar(){
  const chip = document.getElementById('cotUfChip');
  const ico = document.getElementById('cotUfChipIco');
  if (chip) chip.disabled = true;
  if (ico) ico.style.animation = 'spin .8s linear infinite';
  try{
    const r = await fetch('/api/uf-refresh', {method:'POST'});
    const d = await r.json();
    if (d && d.ok){
      _cotUfChipPintar(d);
      ilusToast('✓ UF actualizada' + (d.fecha ? ' (' + _ufFechaCl(d.fecha) + ')' : ''), {type:'success'});
    } else {
      ilusToast(d && d.error ? d.error : 'No se pudo actualizar la UF', {type:'error'});
    }
  }catch(e){ ilusToast('No se pudo actualizar la UF (error de conexión)', {type:'error'}); }
  finally{
    if (chip) chip.disabled = false;
    if (ico) ico.style.animation = '';
  }
}
// Al cargar la página, refrescar el chip con el valor VIVO (el render del
// servidor puede venir de un worker con cache de hasta 1h). GET liviano —
// el backend (_uf_valor_actual, app.py) ya invalida solo el cache cuando
// cambia el día calendario chileno, así que esto SIEMPRE muestra la UF de
// HOY, nunca la de ayer (2026-07-29, Daniel: "déjala infalible... que se
// actualice cada vez que se entre al módulo").
(function(){
  const ico = document.getElementById('cotUfChipIco');
  if (ico) ico.style.animation = 'spin .8s linear infinite';
  fetch('/api/uf-actual').then(function(r){ return r.json(); })
    .then(function(d){ _cotUfChipPintar(d); })
    .catch(function(){})
    .finally(function(){ if (ico) ico.style.animation = ''; });
})();

// Filtro client-side (número/empresa/RUT + estado). Barato para LIMIT 100.
function cotFiltrarLista(){
  const q = (document.getElementById('cotFiltroTexto').value || '').toLowerCase().trim();
  const est = document.getElementById('cotFiltroEstado').value || '';
  document.querySelectorAll('.cot-row').forEach(function(row){
    const okTxt = !q || (row.dataset.num||'').includes(q) || (row.dataset.emp||'').includes(q);
    const okEst = !est || row.dataset.estado === est;
    row.style.display = (okTxt && okEst) ? '' : 'none';
  });
}

// ── Aprobar / Rechazar (superadmin) ──
async function cotCambiarEstado(cid, estado, btn){
  const _txt = estado === 'approved' ? 'aprobar' : 'rechazar';
  const ok = await ilusConfirm({
    title: (estado === 'approved' ? 'Aprobar' : 'Rechazar') + ' cotización',
    message: '¿Seguro que quieres ' + _txt + ' esta cotización?',
    sub: 'Queda registrado en el historial quién y cuándo.',
    okLabel: estado === 'approved' ? 'Aprobar' : 'Rechazar',
    cancelLabel: 'Cancelar', danger: estado === 'rejected',
  });
  if (!ok) return;
  try{
    const r = await fetch('/tickets/api/cotizaciones/' + cid + '/estado', {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify({estado: estado})
    });
    const d = await r.json();
    if (!d.ok){ ilusToast(d.error || 'No se pudo cambiar el estado', {type:'error'}); return; }
    ilusToast('✓ Cotización ' + (estado === 'approved' ? 'aprobada' : 'rechazada'), {type:'success'});
    location.reload();
  }catch(e){ ilusToast('Error de conexión', {type:'error'}); }
}

// ── Recotizar con las reglas vigentes (superadmin) ──
async function cotRecalcular(cid, numero, btn){
  const ok = await ilusConfirm({
    title: 'Recotizar ' + numero,
    message: 'Se recalcularán todas las líneas con la clasificación y tarifas vigentes.',
    sub: 'Los accesorios quedarán en $0 y la ruta se dividirá solo entre equipos cobrables. No se borran productos, cliente ni evidencias.',
    okLabel: 'Recotizar', cancelLabel: 'Cancelar',
  });
  if (!ok) return;
  const original = btn ? btn.innerHTML : '';
  if (btn){ btn.disabled = true; btn.innerHTML = '<span class="spinner-border spinner-border-sm"></span>'; }
  try{
    const r = await fetch('/tickets/api/cotizaciones/' + cid + '/recalcular', {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify({manual: true})
    });
    const d = await r.json();
    if (!d.ok){
      ilusToast(d.error || 'No se pudo recotizar', {type:'error'});
      if (btn){ btn.disabled = false; btn.innerHTML = original; }
      return;
    }
    const nuevo = (d.totales || {}).total || 0;
    ilusToast('✓ ' + numero + ': ' + _wizCLP(d.total_anterior || 0) + ' → ' + _wizCLP(nuevo)
      + (d.editada_post_aprobacion ? ' · quedó marcada como editada tras aprobación' : ''), {type:'success'});
    setTimeout(function(){ location.reload(); }, 900);
  }catch(e){
    ilusToast('Error de conexión al recotizar', {type:'error'});
    if (btn){ btn.disabled = false; btn.innerHTML = original; }
  }
}

// ── Eliminar (soft, superadmin) ──
async function cotEliminar(cid, numero){
  const ok = await ilusConfirm({
    title: 'Eliminar cotización',
    message: '¿Quitar la cotización ' + numero + '?',
    sub: 'Deja de aparecer en el listado. Queda registro de quién la eliminó (no se borra de la base).',
    okLabel: 'Eliminar', cancelLabel: 'Cancelar', danger: true,
  });
  if (!ok) return;
  try{
    const r = await fetch('/tickets/api/cotizaciones/' + cid + '/eliminar', { method: 'POST' });
    const d = await r.json();
    if (!d.ok){ ilusToast(d.error || 'No se pudo eliminar', {type:'error'}); return; }
    ilusToast('✓ Cotización eliminada', {type:'success'});
    location.reload();
  }catch(e){ ilusToast('Error de conexión', {type:'error'}); }
}

// ── Historial (evidencia) ──
const _COT_HIST_ICONOS = {
  crear:'bi-plus-circle', editar:'bi-pencil', editar_aprobada:'bi-pencil-fill',
  recalcular:'bi-arrow-repeat', recalcular_aprobada:'bi-arrow-repeat',
  aprobar:'bi-check-circle', rechazar:'bi-x-circle', reabrir:'bi-arrow-counterclockwise',
  enviar_correo:'bi-envelope', generar_ticket:'bi-ticket-perforated',
  generar_ot:'bi-clipboard-check', eliminar:'bi-trash'
};
const _COT_HIST_LABEL = {
  crear:'Creada', editar:'Editada', editar_aprobada:'Editada (estaba aprobada)',
  recalcular:'Recotizada', recalcular_aprobada:'Recotizada (estaba aprobada)',
  aprobar:'Aprobada', rechazar:'Rechazada', reabrir:'Reabierta',
  enviar_correo:'Enviada por correo', generar_ticket:'Ticket generado',
  generar_ot:'OT generada', eliminar:'Eliminada'
};
async function cotVerHistorial(cid, numero){
  document.getElementById('cotHistNum').textContent = numero || '';
  document.getElementById('cotHistBody').innerHTML = '<div class="text-center text-muted py-3">Cargando…</div>';
  const modal = new bootstrap.Modal(document.getElementById('cotHistModal'));
  modal.show();
  try{
    const r = await fetch('/tickets/api/cotizaciones/' + cid + '/log');
    const d = await r.json();
    if (!d.ok){ document.getElementById('cotHistBody').innerHTML = '<div class="text-danger py-3">' + _cotEsc(d.error||'Sin permiso') + '</div>'; return; }
    if (!d.eventos.length){ document.getElementById('cotHistBody').innerHTML = '<div class="text-muted py-3">Sin eventos registrados.</div>'; return; }
    document.getElementById('cotHistBody').innerHTML = d.eventos.map(function(ev){
      const ico = _COT_HIST_ICONOS[ev.accion] || 'bi-dot';
      const lbl = _COT_HIST_LABEL[ev.accion] || ev.accion;
      let det = '';
      try{ if (ev.detalle){ const o = JSON.parse(ev.detalle); det = _cotHistDetalle(ev.accion, o); } }catch(e){}
      return '<div class="cot-hist-item"><span class="cot-hist-ico"><i class="bi ' + ico + '"></i></span>'
        + '<div class="cot-hist-body"><b>' + _cotEsc(lbl) + '</b>' + (det ? ' <span style="color:#6b7280;">' + det + '</span>' : '')
        + '<small>' + _cotEsc(ev.usuario || 'sistema') + ' · ' + _cotEsc(ev.fecha || '') + '</small></div></div>';
    }).join('');
  }catch(e){ document.getElementById('cotHistBody').innerHTML = '<div class="text-danger py-3">Error de conexión</div>'; }
}
function _cotHistDetalle(accion, o){
  if (accion === 'enviar_correo' && o.enviados) return '→ ' + o.enviados.map(_cotEsc).join(', ');
  if ((accion === 'editar' || accion === 'editar_aprobada') && o.cambios){
    const ks = Object.keys(o.cambios); if (ks.length) return '(' + ks.join(', ') + ')';
  }
  if (accion === 'generar_ticket' && o.numero_ticket) return '→ ' + _cotEsc(o.numero_ticket);
  return '';
}

// ══════════════ Enviar por correo (multi-destinatario) ══════════════
let _cotEnvCid = null, _cotEnvDest = [];
async function cotAbrirEnviar(cid){
  _cotEnvCid = cid; _cotEnvDest = [];
  document.getElementById('cotEnvMensaje').value = '';
  document.getElementById('cotEnvInput').value = '';
  document.getElementById('cotEnvNum').textContent = '';
  document.getElementById('cotEnvChips').innerHTML = '<span class="text-muted" style="font-size:.78rem;padding:6px;">Cargando destinatarios…</span>';
  const modal = new bootstrap.Modal(document.getElementById('cotEnviarModal'));
  modal.show();
  try{
    const r = await fetch('/tickets/api/cotizaciones/' + cid);
    const d = await r.json();
    if (d.ok){
      document.getElementById('cotEnvNum').textContent = d.cotizacion.numero_cotizacion || ('#' + cid);
      (d.destinatarios_sugeridos || []).forEach(function(s){ _cotEnvAdd(s.email, s.origen); });
    }
  }catch(e){}
  _cotEnvRender();
}
const _COT_ORIGEN_LBL = {cliente:'Cliente', contacto:'Contacto', ejecutivo:'Ejecutivo', yo:'Yo', manual:''};
function _cotEnvAdd(email, origen){
  email = (email||'').trim();
  if (!email) return false;
  if (_cotEnvDest.some(function(x){ return x.email.toLowerCase() === email.toLowerCase(); })) return false;
  _cotEnvDest.push({email: email, origen: origen || 'manual'});
  return true;
}
function _cotEnvRender(){
  const box = document.getElementById('cotEnvChips');
  if (!_cotEnvDest.length){ box.innerHTML = '<span class="text-muted" style="font-size:.78rem;padding:6px;">Sin destinatarios — agrega al menos uno.</span>'; }
  else {
    box.innerHTML = _cotEnvDest.map(function(x, i){
      const lbl = _COT_ORIGEN_LBL[x.origen];
      return '<span class="cot-chip">' + (lbl ? '<small>' + _cotEsc(lbl) + '</small> ' : '') + _cotEsc(x.email)
        + '<button type="button" onclick="cotEnvQuitar(' + i + ')" title="Quitar">&times;</button></span>';
    }).join('');
  }
  document.getElementById('cotEnvCount').textContent = _cotEnvDest.length;
  document.getElementById('cotEnvSubmit').disabled = _cotEnvDest.length === 0;
}
function cotEnvQuitar(i){ _cotEnvDest.splice(i, 1); _cotEnvRender(); }
function _cotEnvEmailOk(e){ return /^[^@\s]+@[^@\s]+\.[^@\s]{2,}$/.test((e||'').trim()); }
function cotEnvValidarInput(){
  const inp = document.getElementById('cotEnvInput');
  const v = inp.value.trim();
  inp.style.borderColor = !v ? '' : (_cotEnvEmailOk(v) ? '#16a34a' : '#dc2626');
}
function cotEnvAgregar(){
  const inp = document.getElementById('cotEnvInput');
  const v = inp.value.trim();
  if (!v) return;
  if (!_cotEnvEmailOk(v)){ ilusToast('Ese correo no es válido', {type:'warning'}); return; }
  if (_cotEnvAdd(v, 'manual')){ inp.value = ''; inp.style.borderColor = ''; _cotEnvRender(); }
  else { ilusToast('Ese correo ya está en la lista', {type:'info'}); }
}
async function cotEnviarSubmit(){
  if (!_cotEnvCid || !_cotEnvDest.length) return;
  const btn = document.getElementById('cotEnvSubmit');
  const _orig = btn.innerHTML;
  btn.disabled = true; btn.innerHTML = '<span class="spinner-border spinner-border-sm"></span> Enviando…';
  try{
    const r = await fetch('/tickets/api/cotizaciones/' + _cotEnvCid + '/enviar', {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify({
        destinatarios: _cotEnvDest.map(function(x){ return x.email; }),
        mensaje: document.getElementById('cotEnvMensaje').value.trim()
      })
    });
    const d = await r.json();
    if (!d.ok){ ilusToast(d.error || 'No se pudo enviar', {type:'error'}); btn.disabled = false; btn.innerHTML = _orig; return; }
    let msg = '✓ Enviada a ' + d.enviados + ' destinatario(s)';
    if (d.fallidos && d.fallidos.length) msg += ' · ' + d.fallidos.length + ' falló(aron)';
    ilusToast(msg, {type:'success'});
    bootstrap.Modal.getInstance(document.getElementById('cotEnviarModal')).hide();
    setTimeout(function(){ location.reload(); }, 800);
  }catch(e){ ilusToast('Error de conexión', {type:'error'}); btn.disabled = false; btn.innerHTML = _orig; }
}

// ══════════════ Auditoría (solo superadmin/admin, ver Jinja arriba) ══════════════
// Daniel 2026-07-23: "necesito saber quién lo hizo, cuándo se hizo y si se
// editó, quién lo editó, con fecha de inicio y fecha de última actualización".
// Regla #1: nada de alert() nativo -- se usa ilusAlert con el mismo patrón
// que cotGenerarTicket de arriba.
function cotVerAuditoria(btn){
  const d = btn.dataset;
  const creadoPor = d.creadoPor || 'sistema';
  const creadoAt = d.creadoAt || '—';
  const actPor = d.actualizadoPor || '';
  const actAt = d.actualizadoAt || '';
  let sub = '<div><strong>Creada por:</strong> ' + _cotEsc(creadoPor) + '</div>'
    + '<div style="color:#6b7280;">' + _cotEsc(creadoAt) + '</div>';
  if (actPor){
    sub += '<div style="margin-top:10px;"><strong>Última actualización:</strong> ' + _cotEsc(actPor) + '</div>'
      + '<div style="color:#6b7280;">' + _cotEsc(actAt) + '</div>';
  } else {
    sub += '<div style="margin-top:10px;color:#9ca3af;font-style:italic;">Sin ediciones posteriores registradas.</div>';
  }
  ilusAlert({
    title: 'Auditoría de la cotización',
    message: 'Trazabilidad de creación y edición.',
    sub: sub, subHtml: true, type: 'info', okLabel: 'Cerrar',
  });
}

// ══════════════ Deep-links: abrir el wizard YA prellenado ══════════════
// Daniel 2026-07-23 ("todo conectado" -- rediseño estilo "Generar OT"):
//   /tickets/cotizaciones?desde_ticket=<TID>  -- desde la ficha de un ticket
//   /tickets/cotizaciones?desde_cliente=<CID> -- desde la ficha de Mantenciones
// Ambos casos reusan cotWizAbrir() (mismo modal, mismos ids) y solo
// prellenan datos + fijan el origen -- NO se duplica el wizard.
async function _cotWizAplicarDeepLinkTicket(tid){
  if (!tid || !_WIZ) return;
  // 2026-07-24 (Daniel, "Desde otra fuente" → Por ticket): si YA hay
  // productos cargados en la Sección 5, confirmar antes de reemplazarlos
  // por los declarados en el ticket -- nunca pisar en silencio. El
  // deep-link ?desde_ticket= llama esto con el wizard recién abierto
  // (_WIZ.items siempre vacío ahí), así que ese caso nunca ve el diálogo.
  if (_WIZ.items.length){
    const continuar = await ilusConfirm({
      title: 'Reemplazar productos',
      message: 'Ya hay ' + _WIZ.items.length + ' producto(s) cargados. Al traer el ticket se reemplazan por los declarados en él.',
      okLabel: 'Reemplazar', cancelLabel: 'Cancelar', danger: true,
    });
    if (!continuar) return;
  }
  try{
    const r = await fetch('/tickets/api/tickets/' + tid);
    const d = await r.json();
    if (!d.ok || !_WIZ){
      ilusToast(d.error || 'No se pudo cargar el ticket', {type:'error'});
      return;
    }
    const t = d.ticket || {};
    _WIZ.origen = 'ticket';
    _WIZ.ticketId = tid;
    // 2026-07-24 (revisión adversarial): reflejar en el buscador de la
    // Sección 1 qué quedó cargado -- el dropdown viejo lo hacía, el modal
    // compartido nuevo no pasa el nombre por onSeleccionarEntidad (solo
    // {tipo,id}), así que se completa acá donde ya tenemos el dato real.
    document.getElementById('cotWizCliQ').value = (t.empresa || '') + (t.numero_ticket ? ' — ' + t.numero_ticket : '');
    document.getElementById('cotWizEmpresa').value = t.empresa || '';
    document.getElementById('cotWizRut').value = t.rut || '';
    document.getElementById('cotWizEmail').value = t.email || '';
    document.getElementById('cotWizTelefono').value = t.phone || '';
    if (t.direccion) document.getElementById('cotWizDireccion').value = t.direccion;
    if (t.comuna_nombre) document.getElementById('cotWizComuna').value = t.comuna_nombre;
    if (t.direccion){
      // Mismo criterio que cotWizElegirCliente: la dirección viene de datos
      // ya guardados, NO recién validada en ESTE formulario -- se exige
      // re-validar contra Google Places antes de avanzar (cotWizValidarPaso1).
      document.getElementById('cotWizDireccionHint').innerHTML =
        '<i class="bi bi-exclamation-triangle-fill me-1" style="color:#f59e0b"></i>' +
        'Dirección del ticket sin validar — vuelve a escribirla y elige una opción de la lista para completar la Región.';
    }
    // Contacto del ticket -- mismo select colapsable de la Sección 3 que ya
    // alimentan fichas/plan (_cotWizRenderContactos), con su propio
    // origen/label para distinguirlo en el selector.
    if (t.nombre_contacto){
      _cotWizRenderContactos([{ nombre: t.nombre_contacto, cargo: '', tel: t.phone || '', email: t.email || '',
                                 origen: 'ticket', label: 'Contacto del ticket' }]);
    }
    const equipos = d.equipos || [];
    _WIZ.items = equipos.map(function(eq){
      return { sku: eq.sku || eq.erp_kopr || '', nombre: eq.nombre || 'Equipo',
               qty: eq.cantidad || 1, tido: null, koen: null, clase_producto: null };
    });
    await cotWizClasificar();
    await cotWizRender();
    await cotWizPrecios();
    _cotWizAplicarOrigenPaso2();
    if (t.comuna_nombre) cotWizBuscarRuta();
    if (equipos.length){
      ilusToast('Ticket cargado con ' + equipos.length + ' equipo(s) — revisa y clasifica antes de crear', {type:'info'});
    }
  }catch(e){ ilusToast('Error de conexión al cargar el ticket', {type:'error'}); }
}
async function _cotWizAplicarDeepLinkCliente(cid, opts){
  if (!cid || !_WIZ) return;
  // 2026-07-24 (revisión adversarial): si vamos a auto-agregar los equipos
  // de la ficha y YA hay productos cargados, confirmar ANTES de tocar nada
  // -- si cancela, no se pisa ni el cliente ni los productos. Mismo criterio
  // que _cotWizAplicarDeepLinkTicket. Sin esto, elegir una ficha CONCATENABA
  // sus equipos en silencio (duplicaba líneas / mezclaba productos de dos
  // clientes -> total inflado sin aviso).
  const _reemplazarFicha = !!(opts && opts.autoAgregarEquipos && _WIZ.items.length);
  if (_reemplazarFicha){
    const continuar = await ilusConfirm({
      title: 'Reemplazar productos',
      message: 'Ya hay ' + _WIZ.items.length + ' producto(s) cargados. Al traer la ficha se reemplazan por los equipos activos del cliente.',
      okLabel: 'Reemplazar', cancelLabel: 'Cancelar', danger: true,
    });
    if (!continuar) return;
  }
  try{
    const r = await fetch('/tickets/api/clientes-ficha/' + cid + '/resumen');
    const d = await r.json();
    if (!d.ok || !_WIZ){
      ilusToast(d.error || 'No se pudo cargar el cliente', {type:'error'});
      return;
    }
    const cli = d.cliente || {};
    _WIZ.origen = 'ficha';
    _WIZ.clienteId = cli.id || cid;
    // 2026-07-24 (revisión adversarial): mismo criterio que el deep-link
    // de ticket -- reflejar en el buscador de la Sección 1 qué cliente
    // quedó cargado desde la ficha.
    document.getElementById('cotWizCliQ').value = (cli.razon_social || '') + (cli.rut ? ' — ' + cli.rut : '');
    document.getElementById('cotWizEmpresa').value = cli.razon_social || '';
    document.getElementById('cotWizRut').value = cli.rut || '';
    if (cli.direccion) document.getElementById('cotWizDireccion').value = cli.direccion;
    if (cli.comuna) document.getElementById('cotWizComuna').value = cli.comuna;
    if (cli.region) document.getElementById('cotWizRegion').value = cli.region;
    if (cli.email_empresa) document.getElementById('cotWizEmail').value = cli.email_empresa;
    if (cli.tel_empresa) document.getElementById('cotWizTelefono').value = cli.tel_empresa;
    if (cli.direccion){
      document.getElementById('cotWizDireccionHint').innerHTML =
        '<i class="bi bi-exclamation-triangle-fill me-1" style="color:#f59e0b"></i>' +
        'Dirección de la ficha sin validar — vuelve a escribirla y elige una opción de la lista para completar la Región.';
    }
    _WIZ.planInfo = d.plan || null;
    _WIZ.fichaMaquinas = d.maquinas || [];
    _cotWizRenderPlanBanner();
    _cotWizRenderContactos(d.contactos || []);
    _cotWizAplicarOrigenPaso2();
    if (cli.comuna) cotWizBuscarRuta();
    // 2026-07-24 (Daniel, "Desde otra fuente" → Por ficha de cliente):
    // además de prellenar los datos, agrega TODOS los equipos activos
    // como ítems automáticamente. El deep-link ?desde_cliente=<CID> (más
    // abajo) sigue llamando esta función SIN el flag -- comportamiento
    // intacto (Regla #4.2).
    // 2026-07-23 (Daniel: "que solo reconozca si es de ficha de cliente y
    // agregue solo los productos del plan"): mant_maquinas no tiene un
    // vínculo por equipo a un contrato específico -- lo único que separa
    // "equipos del plan" de "cualquier máquina activa" es que el cliente
    // tenga un plan/contrato VIGENTE (_WIZ.planInfo.activo, mismo criterio
    // que el badge "🟢 En plan" del buscador). Sin plan vigente, no se
    // auto-agrega nada -- el usuario sigue pudiendo sumarlos a mano con
    // "Traer equipos de la ficha" si igual corresponde.
    const _tienePlanVigente = !!(_WIZ.planInfo && _WIZ.planInfo.activo);
    if (opts && opts.autoAgregarEquipos && _WIZ.fichaMaquinas.length){
      if (!_tienePlanVigente){
        ilusToast('Este cliente no tiene un plan/contrato vigente — agrega los equipos manualmente con "Traer equipos de la ficha" si corresponde', {type:'warning'});
      } else {
        // 🔧 FIX 2026-08-27 (Daniel: "que me reconociera los equipos que
        // están bajo el concepto de Plan"). El comentario de arriba (2026-
        // 07-23) ya dejaba anotado el hueco: "lo único que separa equipos
        // del plan de cualquier máquina activa es que el cliente TENGA un
        // plan vigente" -- con eso, un cliente con contrato agregaba TODOS
        // sus equipos activos (en plan o no), justo el desorden que Daniel
        // reportó. Ahora que el backend manda `en_plan` por equipo
        // (mant_maquinas.aplica_mantencion), se filtra a los que de verdad
        // están en el plan -- que es lo que esta ruta siempre quiso hacer.
        const _eqDelPlan = _WIZ.fichaMaquinas.filter(function(m){ return m.en_plan; });
        if (!_eqDelPlan.length){
          ilusToast('Este cliente tiene plan vigente, pero ningún equipo activo está marcado "en plan" — agrégalos manualmente con "Traer equipos de la ficha" si corresponde', {type:'warning'});
        } else {
          // Reemplazo (no concat) cuando el usuario ya confirmó arriba: vaciar
          // primero para que el helper (que concatena, lo correcto para el panel
          // de checkboxes) termine reemplazando en este camino.
          if (_reemplazarFicha) _WIZ.items = [];
          await _cotWizAgregarMaquinasComoItems(_eqDelPlan);
        }
      }
    }
  }catch(e){ ilusToast('Error de conexión al cargar el cliente', {type:'error'}); }
}
(function(){
  const params = new URLSearchParams(window.location.search);
  const desdeTicket = params.get('desde_ticket');
  const desdeCliente = params.get('desde_cliente');
  if (!desdeTicket && !desdeCliente) return;
  const _iniciar = function(){
    cotWizAbrir();
    if (desdeTicket) _cotWizAplicarDeepLinkTicket(parseInt(desdeTicket, 10));
    else if (desdeCliente) _cotWizAplicarDeepLinkCliente(parseInt(desdeCliente, 10));
  };
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', _iniciar);
  else _iniciar();
})();

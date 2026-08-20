/* ══════════════════════════════════════════════════════════════════════
   OT 2.0 · MOTOR DEL FORMULARIO DE GENERAR OT   (namespace O2F / _O2F)
   ══════════════════════════════════════════════════════════════════════
   COPIA ADAPTADA del bloque del modal de static/tickets_ficha.js
   (2026-08-19). Daniel: "copio lo exacto como funciona ahora y avancemos
   con codigo nuevo, pero copiando las buenas costumbres que ya tenian".

   POR QUE UNA COPIA Y NO UN IMPORT: el modulo OT 2.0 tiene que poder
   sobrevivir a que se borre el modal viejo. Es el mismo criterio del
   CSS (static/ot2_modal_form.css) y del HTML
   (templates/ot2/_modal_ot_form.html).

   QUE SE ADAPTO respecto del original:
   · Namespace completo: _TKOT->_O2F, tkot*->o2f*, tkcal*->o2fcal*,
     tkday*->o2fday*, ids lev*->o2fLev* y otTipo/acceso_*->o2f_*. Asi los
     dos modales pueden convivir en una pagina sin pisarse.
   · TID y CID dejan de ser globales de la pagina (en Tickets venian del
     template) y pasan a setearse al abrir, via O2F.iniciar({...}).
   · Se apunta al endpoint propio POST /ot/api/crear en vez del core viejo.

   El resto -- calendario del mes con carga por dia, linea de tiempo del
   dia, deteccion de choque de agenda, sugerir horario, modalidad del
   levantamiento, semaforo de pasos -- es el MISMO codigo ya probado en
   produccion.
   ══════════════════════════════════════════════════════════════════════ */
(function () {
'use strict';

/* Contexto: en Tickets estos venian del template; aca los fija quien abre
   el modal (el selector de origen de OT 2.0). */
var TID = null;   // id del ticket de origen, o null
var CID = null;   // id del cliente con ficha, o null

/* Constantes que en el original venia inyectadas por el template de
   Tickets y que el renombrado por regex no podia resolver (van en
   MAYUSCULAS con guion bajo, fuera del patron \btkot). Sin ellas el
   listener show.bs.modal moria con ReferenceError y el calendario nunca
   llegaba a inicializarse.

   _TKOT_MODO_CLIENTE se recalcula en cada apertura (ver O2F.iniciar), no
   se congela al cargar el archivo: en OT 2.0 el mismo modal sirve para
   un origen con ticket y para uno sin el. */
var _TKOT_MODO_CLIENTE = true;

/* Rol de gestion: en Tickets lo inyectaba el template para decidir si el
   usuario puede reprogramar o reasignar OT ajenas desde la linea de
   tiempo. Aca el modulo entero esta detras de @_require_superadmin, asi
   que quien llega tiene ese permiso. */
var _TKOT_ROL_GESTION = true;

const _O2F = {
  cid: null,               // cliente_id resuelto por RUT en mant_clientes (null = sin ficha)
  clienteResuelto: false,
  modo: 'equipos',          // 'equipos' | 'descubrimiento' (solo tipo=levantamiento)
  forzarTodosEquipos: false, // true = instalación + cliente sin ficha (preselección total bloqueada)
  tecnicosDisponibles: [],
  tecnicosSel: new Set(),
  plantillas: { all: [], cargadas: false },
  eqPlantillas: {},         // { claveEquipo: Set<plantillaId> }
  contactos: { lista: [], cargados: false },
  adjuntos: [],
  cal: { anio: null, mes: null, cache: {}, error: {}, diaSel: null }, // calendario del mes (Paso 4 · col A)
  // Línea de tiempo del día (Paso 4 · col B). `fecha` = día que se está
  // mirando (normalmente == #levFechaProg; puede diferir si se usan las
  // flechas ‹ › para recorrer un rango multi-día). `choqueKeys` = claves de
  // las visitas que o2fChequearChoque() marcó en conflicto, para pintarlas
  // con .en-choque; `choqueFecha` = a qué día pertenece ese resultado.
  day: { fecha: null, rejilla: false, choque: false, choqueFecha: null, choqueKeys: null, visitas: [] },
  _choqueToken: 0, // invalida respuestas de choque obsoletas si el usuario sigue cambiando fecha/hora/tecnicos
  // Chips de duración (2026-07-19): espejo numérico del rango real
  // (1 | 2 | 3 | 5 | 'otro'). Ver levChipsRefresh().
  durN: 1,
  // Presets que abrirLevantamientoSelector() (mant_ficha.js) deja acá ANTES
  // de mostrar el modal en modo cliente -- el listener show.bs.modal los
  // consume y limpia. En modo ticket siempre quedan null (tipo=levantamiento,
  // fecha=hoy, comportamiento sin cambios).
  pendingTipoPreset: null,
  pendingFechaPreset: null,
  // FIX 2026-08-11 (Daniel, probando en vivo): mapa tipo_ot -> categoria
  // (mant_categoria_tipo_map), para que "Plantillas extra" (Paso 5, por
  // equipo) filtre por la MISMA categoría del tipo de OT elegido en el
  // Paso 1, en vez de listar TODAS las plantillas del sistema mezcladas.
  categoriaMap: null,
};

// ── Clave estable por fila de equipo: usa maquina_id si existe (para que
//    cruce con plantillas/lo que sea de la ficha real), si no usa el id
//    propio de tk_ticket_equipos con prefijo (equipo aún sin ficha). ──
function _tkotEqKey(e){ return e.maquina_id ? String(e.maquina_id) : ('teq_' + e.id); }

// ── Resolver si el cliente del ticket YA tiene ficha en Mantenciones
//    (mismo patrón que app.py: SELECT id, razon_social FROM mant_clientes
//    WHERE rut=%s). Reutiliza el autocomplete existente (no hay endpoint
//    dedicado de "buscar por rut exacto"), filtrando por coincidencia
//    exacta de RUT normalizado + origen local. ──
function _tkotNormRut(r){
  return String(r||'').replace(/[.\s]/g,'').toUpperCase();
}
async function _tkotResolverCliente(){
  _O2F.cid = null; _O2F.clienteResuelto = false;
  // Modo cliente: el cliente_id YA se conoce -- es la ficha que estamos
  // viendo (CID, mant_ficha.js). Nada que resolver por RUT.
  if (_TKOT_MODO_CLIENTE){
    _O2F.cid = CID;
    _O2F.clienteResuelto = true;
    return;
  }
  const t = ticketActual || {};
  const rut = (t.rut || '').trim();
  const q = rut || (t.empresa || '').trim();
  if(q.length < 2){ _O2F.clienteResuelto = true; return; }
  try{
    const r = await fetch('/mantenciones/api/clientes/autocomplete?q='+encodeURIComponent(q));
    let arr = await r.json();
    if(!Array.isArray(arr)) arr = [];
    let match = null;
    if(rut){
      match = arr.find(function(c){ return c.origen==='local' && c.id && _tkotNormRut(c.rut)===_tkotNormRut(rut); });
    }
    if(!match && !rut){
      match = arr.find(function(c){ return c.origen==='local' && c.id; });
    }
    if(match){ _O2F.cid = match.id; }
  }catch(e){ console.warn('o2f resolver cliente:', e); }
  _O2F.clienteResuelto = true;
}

// ── Contactos: solo si hay ficha de cliente (CID). Sin ficha, cae con
//    gracia a contacto 100% manual prellenado con los datos del ticket. ──
async function _tkotCargarContactos(){
  _O2F.contactos.lista = [];
  if(!_O2F.cid) return;
  try{
    const r = await fetch('/mantenciones/api/clientes/'+_O2F.cid+'/contactos');
    const d = await r.json();
    _O2F.contactos.lista = (d.ok && d.contactos) ? d.contactos : [];
  }catch(e){ console.warn('o2f contactos:', e); }
  _O2F.contactos.cargados = true;
}
function _tkotRenderContactosSelector(){
  const sel = document.getElementById('o2fLevContactoSel');
  if(!sel) return;
  const lista = _O2F.contactos.lista || [];
  let html = lista.length ? '<option value="">— Selecciona un contacto —</option>' : '<option value="">— Sin contactos registrados —</option>';
  lista.forEach(function(c, i){
    const meta = [c.cargo, c.tel].filter(Boolean).join(' · ');
    html += '<option value="'+i+'">'+esc(c.label || c.nombre)+' — '+esc(c.nombre)+(meta?' ('+esc(meta)+')':'')+'</option>';
  });
  html += '<option value="__manual">+ Ingresar manualmente</option>';
  sel.innerHTML = html;
}
function o2fContactoChange(){
  const sel = document.getElementById('o2fLevContactoSel');
  const box = document.getElementById('o2fLevContactoBox');
  const v = sel.value;
  if(v === '__manual'){
    box.style.display = '';
    (document.getElementById('o2fLevContactoNombre')||{}).value = '';
    (document.getElementById('o2fLevContactoCargo')||{}).value = '';
    (document.getElementById('o2fLevContactoTel')||{}).value = '';
    (document.getElementById('o2fLevContactoEmail')||{}).value = '';
    sel.dataset.origen = 'manual';
  } else if(v === ''){
    box.style.display = 'none';
    sel.dataset.origen = '';
  } else {
    const idx = parseInt(v);
    const c = _O2F.contactos.lista[idx];
    if(c){
      box.style.display = '';
      (document.getElementById('o2fLevContactoNombre')||{}).value = c.nombre || '';
      (document.getElementById('o2fLevContactoCargo')||{}).value = c.cargo || '';
      (document.getElementById('o2fLevContactoTel')||{}).value = c.tel || '';
      (document.getElementById('o2fLevContactoEmail')||{}).value = c.email || '';
      sel.dataset.origen = c.origen || 'principal';
    }
  }
  o2fRefreshStepStates();
}
function o2fToggleContactoManual(){
  const sel = document.getElementById('o2fLevContactoSel');
  sel.value = '__manual';
  o2fContactoChange();
}

// ── Plantillas activas (idéntico a Mantenciones) ──
async function _tkotCargarPlantillas(){
  if(_O2F.plantillas.cargadas) return _O2F.plantillas.all;
  try{
    const r = await fetch('/mantenciones/api/plantillas?activa=1');
    const d = await r.json();
    _O2F.plantillas.all = Array.isArray(d) ? d : (d.plantillas || []);
    _O2F.plantillas.cargadas = true;
  }catch(e){ console.warn('o2f plantillas:', e); }
  return _O2F.plantillas.all;
}

// FIX 2026-08-11 (Daniel, probando en vivo): mapa tipo_ot -> categoria,
// para filtrar "Plantillas extra" por la categoría del tipo de OT elegido.
// Best-effort: si falla, _O2F.categoriaMap queda null y el caller
// (o2fAbrirMultiPlantilla) cae de vuelta a mostrar todas sin filtrar --
// nunca bloquea agregar una plantilla extra por un problema de red.
async function _tkotCargarCategoriaMap(){
  if(_O2F.categoriaMap) return _O2F.categoriaMap;
  try{
    const r = await fetch('/mantenciones/api/plantillas/categorias');
    const d = await r.json();
    if(d && d.ok && d.mapa_tipo_ot) _O2F.categoriaMap = d.mapa_tipo_ot;
  }catch(e){ console.warn('o2f categoria map:', e); }
  return _O2F.categoriaMap;
}

// ── Técnicos (multi-select, idéntico a Mantenciones) ──
function o2fRenderTecnicos(){
  const box = document.getElementById('o2fLevTecnicosBox');
  if(!box) return;
  const techs = _O2F.tecnicosDisponibles || [];
  if(!techs.length){
    box.innerHTML = '<div class="alert alert-warning py-2 mb-0 small w-100">'
      + '<i class="bi bi-exclamation-triangle me-1"></i>No hay técnicos activos. '
      + 'Solicita a un administrador que cree un usuario con rol "Técnico".</div>';
    (document.getElementById('o2fLevTecCount')||{}).textContent = '0';
    return;
  }
  const fechaProg = document.getElementById('o2fLevFechaProg')?.value || '';
  box.innerHTML = techs.map(function(t){
    const isSel = _O2F.tecnicosSel.has(t.id);
    const bg = isSel ? 'background:linear-gradient(135deg,#1e40af,#3b82f6);color:#fff;border-color:#1e40af' : 'background:#fff;color:#0f172a;border-color:#cbd5e1';
    const icon = isSel ? 'bi-check-circle-fill' : 'bi-person';
    return '<span class="badge rounded-pill border" style="cursor:pointer;padding:.5rem .85rem;font-size:.82rem;font-weight:500;'+bg+'" '
      + 'onclick="o2fToggleTecnico('+t.id+')"><i class="bi '+icon+' me-1"></i>'+esc(t.nombre || t.email || ('Téc #'+t.id))
      + _tkotTecCargaChip(t.id, fechaProg) + '</span>';
  }).join('');
  (document.getElementById('o2fLevTecCount')||{}).textContent = String(_O2F.tecnicosSel.size);
}

// ── §4B "Carga del día por técnico, donde se decide": mini-contador de OTs
//    que ese técnico YA tiene en la fecha programada actual (mismo cache del
//    mes que alimenta el timeline -- cero backend). Sufijo "· N ese día":
//    gris con 1, ámbar con 2, rojo con 3+. Se refresca solo (o2fRenderTecnicos
//    se llama de nuevo) cada vez que cambia #levFechaProg -- ver o2fCalSelDia. ──
function _tkotTecCargaChip(tecnicoId, fecha){
  if(!fecha) return '';
  const key = _tkotCalKey(parseInt(fecha.slice(0, 4), 10), parseInt(fecha.slice(5, 7), 10));
  const mapa = _O2F.cal.cache[key] || {};
  const visitas = mapa[fecha] || [];
  const n = visitas.filter(function(v){
    return String(v.tecnico_id) === String(tecnicoId) && String(v.estado || '').toLowerCase() !== 'cancelada';
  }).length;
  if(!n) return '';
  let colores;
  if(n >= 3) colores = 'background:#fee2e2;color:#991b1b';        // rojo (REGLA #2)
  else if(n === 2) colores = 'background:#fff8e1;color:#b45309';  // ámbar (REGLA #2)
  else colores = 'background:#f3f4f6;color:#6b7280';              // gris (REGLA #2)
  return '<span class="o2f-tec-carga" style="' + colores + '">· ' + n + ' ese día</span>';
}
function o2fToggleTecnico(tid){
  if(_O2F.tecnicosSel.has(tid)) _O2F.tecnicosSel.delete(tid); else _O2F.tecnicosSel.add(tid);
  o2fRenderTecnicos();
  // El bloque "Tu OT" rotula "Nueva OT · N técnicos" -> se refresca al toque.
  if(typeof o2fdayRenderMine === 'function') o2fdayRenderMine();
  o2fChequearChoqueDebounced();
  o2fRefreshStepStates();
}

// ════════════════════════════════════════════════════════════
// CALENDARIO EMBEBIDO (Paso 4) + choque de horario en vivo
// Daniel: "algo 2026/2027, parecido al de retiros, bien estructurado...
// para ver quien esta agendado, con que OT, panorama general del mes,
// sin salir del modal". Consume GET /mantenciones/api/calendario/mes/
// <anio>/<mes> y GET /mantenciones/api/calendario/choque?tecnico_id=X&
// fecha=Y&hora_ini=Z&hora_fin=W -- contratos construidos EN PARALELO por
// otro agente sobre app.py (no se toca app.py desde este archivo). Si el
// endpoint aun no existe al momento de probar, el catch de cada función
// deja el panel en un estado vacío amigable en vez de romper el modal.
// ════════════════════════════════════════════════════════════
const TKCAL_DOW_ES = ['L','M','M','J','V','S','D'];
const TKCAL_MES_ES = ['enero','febrero','marzo','abril','mayo','junio','julio',
  'agosto','septiembre','octubre','noviembre','diciembre'];

// ── §4C "Indicador de día saturado": umbrales ajustables de carga TOTAL del
//    día (todas las OTs, no solo las de los técnicos seleccionados) para el
//    color del badge de conteo en el mini-calendario. ──
const TKCAL_UMBRAL_AMBAR = 3;   // 3–4 OTs ese día
const TKCAL_UMBRAL_ROJO = 5;    // 5+ OTs ese día

function _tkotCalKey(a, m){ return a + '-' + String(m).padStart(2, '0'); }

// ── Normaliza la respuesta del endpoint de mes a { "YYYY-MM-DD": [visitas] }
//    -- tolera 3 formas razonables de shape porque el contrato exacto lo
//    define otro agente en paralelo (ver notas del contrato al pie). ──
function _tkotCalNormalizarMes(d){
  const mapa = {};
  if(!d) return mapa;
  if(d.dias && !Array.isArray(d.dias) && typeof d.dias === 'object'){
    Object.keys(d.dias).forEach(function(f){
      const dd = d.dias[f];
      mapa[f] = Array.isArray(dd) ? dd : (dd && dd.visitas) || [];
    });
  } else if(Array.isArray(d.dias)){
    d.dias.forEach(function(dd){
      if(dd && dd.fecha) mapa[dd.fecha] = dd.visitas || [];
    });
  } else if(Array.isArray(d.visitas)){
    d.visitas.forEach(function(v){
      const f = v.fecha_programada || v.fecha;
      if(!f) return;
      if(!mapa[f]) mapa[f] = [];
      mapa[f].push(v);
    });
  }
  return mapa;
}

async function o2fCalCargarMes(){
  const a = _O2F.cal.anio, m = _O2F.cal.mes;
  const titEl = document.getElementById('o2fLevCalTitulo');
  if(titEl) titEl.textContent = (TKCAL_MES_ES[m - 1] || '') + ' ' + a;
  const key = _tkotCalKey(a, m);
  if(_O2F.cal.cache[key]){
    o2fCalRenderGrid();
    if(_O2F.cal.diaSel && _O2F.cal.diaSel.slice(0, 7) === key) o2fdayRender(_O2F.cal.diaSel, { silencio: true });
    return;
  }
  const grid = document.getElementById('o2fLevCalGrid');
  if(grid) grid.innerHTML = '<div class="text-muted small text-center py-3" style="grid-column:1/-1">Cargando…</div>';
  try{
    const r = await fetch('/mantenciones/api/calendario/mes/' + a + '/' + m);
    if(!r.ok) throw new Error('http ' + r.status);
    const d = await r.json();
    _O2F.cal.cache[key] = _tkotCalNormalizarMes(d);
  }catch(e){
    console.warn('o2f calendario mes:', e);
    _O2F.cal.cache[key] = {};
    _O2F.cal.error[key] = true;
    if(grid) grid.innerHTML = '<div class="text-muted small text-center py-3" style="grid-column:1/-1">'
      + '<i class="bi bi-calendar-x me-1"></i>No se pudo cargar el calendario de este mes.</div>';
    o2fdayRender(_O2F.cal.diaSel, { silencio: true });
    return;
  }
  delete _O2F.cal.error[key];
  o2fCalRenderGrid();
  // El timeline NO se borra al navegar de mes: si el día que se está
  // mirando pertenece al mes recién cargado, se repinta con los datos frescos.
  if(_O2F.cal.diaSel && _O2F.cal.diaSel.slice(0, 7) === key) o2fdayRender(_O2F.cal.diaSel, { silencio: true });
}

// ── Reintento del estado de error (§8c): purga el caché del mes y recarga. ──
function o2fCalReintentar(){
  delete _O2F.cal.cache[_tkotCalKey(_O2F.cal.anio, _O2F.cal.mes)];
  delete _O2F.cal.error[_tkotCalKey(_O2F.cal.anio, _O2F.cal.mes)];
  o2fCalCargarMes();
}

function o2fCalMes(delta){
  let m = _O2F.cal.mes + delta, a = _O2F.cal.anio;
  if(m > 12){ m = 1; a++; } else if(m < 1){ m = 12; a--; }
  _O2F.cal.anio = a; _O2F.cal.mes = m;
  // Se MANTIENE la selección (_O2F.cal.diaSel) y el timeline no se borra:
  // navegar de mes es solo mirar, no deselecciona lo agendado.
  o2fCalCargarMes();
}

function o2fCalHoy(){
  const hoy = new Date();
  _O2F.cal.anio = hoy.getFullYear(); _O2F.cal.mes = hoy.getMonth() + 1;
  o2fCalCargarMes().then(function(){
    const f = hoy.getFullYear() + '-' + String(hoy.getMonth() + 1).padStart(2, '0') + '-' + String(hoy.getDate()).padStart(2, '0');
    o2fCalClicDia(f);   // pasa por #levFechaProg: el calendario manda para el día
  });
}

// ── Rango multi-día activo (#levRangoDias + #levFechaFin) para pintar el
//    "pill continuo" en el mini-calendario. Compara fechas como STRING
//    'YYYY-MM-DD' (no índices) para que el pintado siga funcionando cuando
//    el rango cruza de mes y se navega con ‹ ›.
function _tkotRangoActivo(){
  const on = document.getElementById('o2fLevRangoDias')?.checked;
  if(!on) return null;
  const ini = document.getElementById('o2fLevFechaProg')?.value || '';
  const fin = document.getElementById('o2fLevFechaFin')?.value || '';
  if(!ini || !fin || fin < ini) return null;
  return { ini: ini, fin: fin };
}

function o2fCalRenderGrid(){
  const a = _O2F.cal.anio, m = _O2F.cal.mes;
  const mapa = _O2F.cal.cache[_tkotCalKey(a, m)] || {};
  const grid = document.getElementById('o2fLevCalGrid');
  if(!grid) return;
  const primerDia = new Date(a, m - 1, 1);
  const nDias = new Date(a, m, 0).getDate();
  let offset = primerDia.getDay() - 1; // lunes = 0
  if(offset < 0) offset = 6;
  const hoy = new Date();
  const hoyStr = hoy.getFullYear() + '-' + String(hoy.getMonth() + 1).padStart(2, '0') + '-' + String(hoy.getDate()).padStart(2, '0');
  const fechaProg = document.getElementById('o2fLevFechaProg')?.value || '';
  const rango = _tkotRangoActivo();
  let html = '';
  for(let i = 0; i < offset; i++) html += '<div class="o2fcal-day blank"></div>';
  for(let dia = 1; dia <= nDias; dia++){
    const fecha = a + '-' + String(m).padStart(2, '0') + '-' + String(dia).padStart(2, '0');
    const visitas = mapa[fecha] || [];
    const dow = new Date(a, m - 1, dia).getDay();
    const clases = ['o2fcal-day'];
    if(dow === 0 || dow === 6) clases.push('finde');
    if(fecha === hoyStr) clases.push('hoy');
    if(fecha === fechaProg) clases.push('sel');
    if(rango && fecha >= rango.ini && fecha <= rango.fin){
      clases.push('rango-in');
      if(fecha === rango.ini) clases.push('rango-ini');
      if(fecha === rango.fin) clases.push('rango-fin');
    }
    // Día que está mirando la línea de tiempo (anillo). Solo se marca aparte
    // cuando NO coincide con la fecha programada -- si coinciden, .sel ya lo dice.
    if(fecha === _O2F.cal.diaSel && fecha !== fechaProg) clases.push('vista');
    // ── Dots de técnico (máx 2 + "+N"): densidad visual del día con carga.
    //    Reemplazan al title= nativo (el "globito"): la información real
    //    ahora vive en la línea de tiempo de la derecha.
    let dots = '';
    if(visitas.length){
      const vistos = [];
      visitas.forEach(function(v){
        const tid = (v.tecnico_id == null ? '_' : String(v.tecnico_id));
        if(vistos.indexOf(tid) === -1) vistos.push(tid);
      });
      dots = '<span class="dots">'
        + vistos.slice(0, 2).map(function(tid){
            return '<em style="background:' + _tkdayColor(tid)[0] + '"></em>';
          }).join('')
        + (vistos.length > 2 ? '<b>+' + (vistos.length - 2) + '</b>' : '')
        + '</span>';
    }
    html += '<div class="' + clases.join(' ') + '" onclick="o2fCalClicDia(\'' + fecha + '\')"'
      + ' onkeydown="if(event.key===\'Enter\'||event.key===\' \'){event.preventDefault();o2fCalClicDia(\'' + fecha + '\');}"'
      + ' role="button" tabindex="0"'
      + ' aria-label="' + esc(fecha + ' — ' + (visitas.length ? (visitas.length + ' OT agendada(s)') : 'sin OTs agendadas')) + '">'
      + '<span class="num">' + dia + '</span>'
      + (visitas.length ? '<span class="cnt' + (visitas.length >= TKCAL_UMBRAL_ROJO ? '' : visitas.length >= TKCAL_UMBRAL_AMBAR ? ' o2fcal-carga-ambar' : ' o2fcal-carga-normal') + '">' + visitas.length + '</span>' : '')
      + dots
      + '</div>';
  }
  grid.innerHTML = html;
}

// ── Clic en un día del mini-calendario: el calendario MANDA para el día.
//    Setea #levFechaProg y dispara su evento change para que TODA la lógica
//    existente (o2fFechaProgChange → o2fCalSelDia + choque) reaccione
//    exactamente igual que si el usuario hubiera tipeado la fecha.
function o2fCalClicDia(fecha){
  const fp = document.getElementById('o2fLevFechaProg');
  if(!fp){ o2fCalSelDia(fecha); return; }
  fp.value = fecha;
  fp.dispatchEvent(new Event('change', { bubbles: true }));
}

// ── Selección de día: repinta la grilla + la LÍNEA DE TIEMPO del día
//    (superficie principal desde el rediseño 2026-07-15) y mantiene
//    #levCalDetalle relleno como FALLBACK oculto (REGLA #4.2: la lista
//    textual sigue existiendo en el DOM; solo dejó de ser la vista visible
//    porque los bloques por hora dan la misma info y más).
function o2fCalSelDia(fecha, opts){
  opts = opts || {};
  _O2F.cal.diaSel = fecha;
  o2fCalRenderGrid();
  o2fdayRender(fecha, { scroll: !opts.silencio });
  // §4B: la fecha programada es la que manda el conteo de carga por técnico
  // (chips del Paso 4) -- se refresca aquí, único punto de paso de todo
  // cambio de día (clic en el mini-calendario, tipeo directo, "Hoy",
  // apertura del modal y el nuevo o2fdaySugerirHueco()).
  o2fRenderTecnicos();
  const mapa = _O2F.cal.cache[_tkotCalKey(_O2F.cal.anio, _O2F.cal.mes)] || {};
  const visitas = mapa[fecha] || [];
  const det = document.getElementById('o2fLevCalDetalle');
  if(!det) return;
  const partes = fecha.split('-');
  const fechaFmt = partes.length === 3 ? (partes[2] + '/' + partes[1] + '/' + partes[0]) : fecha;
  let html = '<div class="o2fcal-detalle-titulo"><i class="bi bi-calendar3 me-1" style="color:#dc2626"></i>' + fechaFmt + '</div>';
  if(!visitas.length){
    html += '<div class="o2fcal-det-empty">Sin OTs agendadas este día.</div>';
  } else {
    visitas.forEach(function(v){
      const tec = esc(v.tecnico_nombre || v.tecnico || 'Sin técnico asignado');
      const hi = v.hora_inicio || '', hf = v.hora_fin || '';
      const num = esc(v.numero_ot || ('OT #' + (v.id || v.visita_id || '?')));
      html += '<div class="o2fcal-det-item"><span class="hora">' + (hi || '--') + (hf ? '–' + hf : '') + '</span>'
        + '<span class="who">' + tec + '</span><span class="num">' + num + '</span></div>';
    });
  }
  det.innerHTML = html;
}

// ── Se llama al abrir el modal (reset) y cada vez que cambia la fecha
//    programada, para que el calendario "salte" al mes correspondiente. ──
function o2fCalInit(){
  const fp = document.getElementById('o2fLevFechaProg')?.value || '';
  const base = fp ? new Date(fp + 'T00:00:00') : new Date();
  _O2F.cal.anio = base.getFullYear();
  _O2F.cal.mes = base.getMonth() + 1;
  _O2F.cal.diaSel = null;
  _O2F.cal.cache = {};
  _O2F.cal.error = {};
  _O2F.day.fecha = null;
  _O2F.day.rejilla = false;
  _O2F.day.choque = false;
  _O2F.day.choqueFecha = null;
  _O2F.day.choqueKeys = null;
  _O2F.day.visitas = [];
  const det = document.getElementById('o2fLevCalDetalle');
  if(det) det.style.display = 'none';
  const warn = document.getElementById('o2fLevCalWarnChoque');
  if(warn) warn.style.display = 'none';
  o2fdayCerrarDetalle();
  o2fdayRender(null, { silencio: true });   // estado (b) "toca un día" mientras carga
  o2fCalCargarMes().then(function(){
    if(fp) o2fCalSelDia(fp, { silencio: true });
  });
}

function o2fFechaProgChange(){
  // Chips de duración (2026-07-19, P4): re-anclaje SILENCIOSO de #levFechaFin
  // ANTES de cualquier render -- así o2fCalSelDia ya pinta el pill con la
  // fin correcta (evita el parpadeo fin<inicio del P1/P4).
  _levReanclarFin();
  const v = document.getElementById('o2fLevFechaProg')?.value || '';
  if(v){
    const y = parseInt(v.slice(0, 4)), m = parseInt(v.slice(5, 7));
    if(y && m && (y !== _O2F.cal.anio || m !== _O2F.cal.mes)){
      _O2F.cal.anio = y; _O2F.cal.mes = m;
      o2fCalCargarMes().then(function(){ o2fCalSelDia(v, { silencio: true }); });
    } else {
      o2fCalSelDia(v, { silencio: true });
    }
  } else {
    _O2F.cal.diaSel = null;
    o2fCalRenderGrid();
    o2fdayRender(null, { silencio: true });
  }
  levChipsRefresh();
  o2fChequearChoqueDebounced();
}

// ── Edición fina del horario: el bloque "Tu OT" se mueve EN VIVO (local,
//    sin red) en cada `input`, y el chequeo de choque va aparte con su
//    debounce de 500ms ya existente. ──
function o2fHoraInput(){
  o2fdayRenderMine();
  o2fChequearChoqueDebounced();
}

// ── Toggle del rango multi-día / cambio de #levFechaFin: repinta el "pill"
//    del calendario y el chip "Día X de N" + el bloque propio. ──
function o2fRangoChange(){
  o2fCalRenderGrid();
  o2fdayRender(_O2F.cal.diaSel, { silencio: true });
  // Chips de duración (2026-07-19): único punto de espejo estado→chips.
  // Cubre toggle manual, fin manual y chips en un solo lugar (P2).
  levChipsRefresh();
}

// ════════════════════════════════════════════════════════════
// Chips de duración (2026-07-19): [1 día][2 días][3 días][5 días][Otro…]
// junto al Paso 4 "Agenda". Un tap = extender la OT a N días sin tocar el
// toggle "¿Se extenderá más de un día?" a mano. Regla única de sincronía:
// mientras exista un rango válido, la duración SIEMPRE viaja con la fecha
// de inicio (venga de chip o de "Otro…") -- sin flag de modo aparte (P2).
// Los chips SOLO escriben en #levRangoDias/#levFechaFin/#levHoraIniFin/
// #levHoraFinFin, exactamente lo que ya lee o2fGenerar() -- cero cambios
// de backend. Portado 1:1 desde templates/mantenciones/ficha.html +
// static/mant_ficha.js (allá con _LEV_MODAL en vez de _O2F).
// ────────────────────────────────────────────────────────────

// NUEVO helper: _fmtYMD no existe en el repo (grep: 0 matches).
function _levFmtYMD(d){
  return d.getFullYear() + '-' + String(d.getMonth() + 1).padStart(2, '0') + '-' + String(d.getDate()).padStart(2, '0');
}

// Escritura SILENCIOSA de #levFechaFin = #levFechaProg + (durN-1) días
// corridos. No renderiza nada -- el llamador decide cuándo repintar.
function _levReanclarFin(){
  const n = _O2F.durN;
  if(!(typeof n === 'number' && n > 1)) return;
  const v = document.getElementById('o2fLevFechaProg')?.value || '';
  if(!v) return;
  const p = v.split('-');
  const d = new Date(parseInt(p[0], 10), parseInt(p[1], 10) - 1, parseInt(p[2], 10));
  d.setDate(d.getDate() + (n - 1));   // días CORRIDOS, findes incluidos
  (document.getElementById('o2fLevFechaFin')||{}).value = _levFmtYMD(d);
  (document.getElementById('o2fLevRangoDias')||{}).checked = true;
  (document.getElementById('o2fLevFechaFinWrap')||{}).style.display = '';
}

// Tap en un chip numérico [1|2|3|5].
function levDurSet(n){
  const fp = document.getElementById('o2fLevFechaProg');
  if(!fp || !fp.value){ ilusToast('Elige primero la fecha de inicio', { type: 'warning' }); return; }
  const chk = document.getElementById('o2fLevRangoDias');
  const wrap = document.getElementById('o2fLevFechaFinWrap');
  const veniaDe1 = !chk.checked;   // P3: solo heredar horas en la transición 1→N
  _O2F.durN = n;
  if(n <= 1){
    chk.checked = false; wrap.style.display = 'none';
    (document.getElementById('o2fLevFechaFin')||{}).value = '';
  } else {
    _levReanclarFin();
    if(veniaDe1){   // heredar horas SOLO al abrir el rango, nunca entre chips N→M (P3)
      (document.getElementById('o2fLevHoraIniFin')||{}).value = (document.getElementById('o2fLevHoraIni')||{}).value || '09:00';
      (document.getElementById('o2fLevHoraFinFin')||{}).value = (document.getElementById('o2fLevHoraFin')||{}).value || '13:00';
    }
  }
  o2fRangoChange();               // reusa pill + "Día X de N" + o2fdayRenderMine + levChipsRefresh
  o2fChequearChoqueDebounced();   // el choque ya manda fecha_fin
}

// Tap en "Otro…": abre el panel ámbar existente para que el usuario tipee
// la fecha de término a mano. No fuerza ninguna duración.
function levDurOtro(){
  (document.getElementById('o2fLevRangoDias')||{}).checked = true;
  (document.getElementById('o2fLevFechaFinWrap')||{}).style.display = '';
  _O2F.durN = 'otro';
  o2fRangoChange();
  document.getElementById('o2fLevFechaFin').focus();
}

// Único espejo estado→chips: recalcula durN desde el DOM real y repinta
// clases/aria-pressed + el texto "→ hasta …". Se llama al final de
// o2fRangoChange/o2fFechaProgChange/_tkdayIrADia/reset del modal.
function levChipsRefresh(){
  const cont = document.getElementById('o2fLevDurChips');
  if(!cont) return;
  const r = _tkotRangoActivo();
  let n = 1;
  if(r) n = Math.round((new Date(r.fin + 'T00:00:00') - new Date(r.ini + 'T00:00:00')) / 86400000) + 1;
  else if(document.getElementById('o2fLevRangoDias')?.checked) n = 'otro';
  _O2F.durN = n;
  cont.querySelectorAll('.lev-dur-chip').forEach(function(b){
    const act = String(b.dataset.n) === String(n)
      || (b.dataset.n === 'otro' && n !== 1 && [2, 3, 5].indexOf(n) === -1);
    b.classList.toggle('act', act);
    b.setAttribute('aria-pressed', act ? 'true' : 'false');
  });
  const h = document.getElementById('o2fLevDurHasta');
  if(h) h.innerHTML = r ? ('→ hasta <b>' + esc(_tkdayFechaLarga(r.fin)) + '</b>')
    : (n === 'otro' ? 'elige la fecha de término' : '');
}

// ── Flechas ‹ › del día: recorren el día que muestra la línea de tiempo SIN
//    tocar #levFechaProg (así se puede inspeccionar el día 2, 3, 4… de un
//    rango multi-día, o simplemente espiar el día siguiente antes de decidir).
function o2fdayPaso(delta){
  const base = _O2F.day.fecha || document.getElementById('o2fLevFechaProg')?.value || '';
  if(!base) return;
  const p = base.split('-');
  const d = new Date(parseInt(p[0], 10), parseInt(p[1], 10) - 1, parseInt(p[2], 10));
  d.setDate(d.getDate() + delta);
  const f = d.getFullYear() + '-' + String(d.getMonth() + 1).padStart(2, '0') + '-' + String(d.getDate()).padStart(2, '0');
  const y = d.getFullYear(), m = d.getMonth() + 1;
  _O2F.cal.diaSel = f;
  if(y !== _O2F.cal.anio || m !== _O2F.cal.mes){
    _O2F.cal.anio = y; _O2F.cal.mes = m;
    o2fCalCargarMes();    // al terminar repinta grilla + timeline (ver o2fCalCargarMes)
  } else {
    o2fCalRenderGrid();
    o2fdayRender(f, { silencio: true });
  }
}

// ════════════════════════════════════════════════════════════
// LÍNEA DE TIEMPO DEL DÍA (Paso 3 · columna B)
// ------------------------------------------------------------
// Daniel: el title= nativo del día (el "globito") no sirve para decidir.
// Ahora las OTs del día se DIBUJAN como bloques posicionados por su hora
// real, y la OT que se está creando se dibuja EN VIVO encima.
//
// GEOMETRÍA (fórmula explícita, sin magia):
//   DAY_START = 8*60 = 480 min · DAY_END = 20*60 = 1200 min
//   PX_MIN    = 0.8  ⇒ 48px por hora ⇒ lienzo 12h * 48 = 576px
//   ini    = max(toMin(hora_inicio), DAY_START)
//   fin    = min(toMin(hora_fin || hora_inicio), DAY_END)
//   top    = (ini - DAY_START) * PX_MIN
//   height = max((fin - ini) * PX_MIN, 18)
//   (+ clamp: si se sale del rango 08:00–20:00 se recorta al borde y el
//    bloque queda con chevron ▲/▼; si no hay hora_fin o fin <= ini, altura
//    mínima 18px y borde derecho punteado con .sin-fin)
//
// DATOS: el MISMO caché _O2F.cal.cache que llena _tkotCalNormalizarMes con
// GET /mantenciones/api/calendario/mes/<anio>/<mes>. Cero backend nuevo:
// el timeline es solo otra vista del mismo mapa[fecha].
// ════════════════════════════════════════════════════════════
const TKDAY_START = 8 * 60;      // 480
const TKDAY_END   = 20 * 60;     // 1200
const TKDAY_PXMIN = 0.8;         // 48px/hora
const TKDAY_H     = (TKDAY_END - TKDAY_START) * TKDAY_PXMIN;   // 576
// Alto MINIMO pintado de un bloque (una OT de 5 min igual debe poder tocarse).
// Su equivalente en minutos es la duracion que el bloque OCUPA de verdad en el
// lienzo: 18px / 0.8 = 22.5 min. El layout DEBE razonar con esta duracion
// efectiva, no con la cruda -- si no, dos bloques "sin hora_fin" a la misma
// hora tienen duracion 0, el test de solape (estricto) nunca dispara, y se
// pintan uno EXACTAMENTE encima del otro tapandose (el contador decia "2 OTs"
// y solo se veia 1). Ver _tkdayLayout/o2fdayRender.
const TKDAY_MINH   = 18;
const TKDAY_MINDUR = TKDAY_MINH / TKDAY_PXMIN;                 // 22.5 min
const TKDAY_DOW_LARGO = ['domingo','lunes','martes','miércoles','jueves','viernes','sábado'];

// Paleta determinista por técnico (tecnico_id % 8): [borde, fondo].
// El ROJO #dc2626 queda RESERVADO para "Tu OT" y el choque — jamás para un
// técnico, o se pierde la señal (REGLA #2: paleta de apoyo).
const TKDAY_PALETA = [
  ['#3b82f6','#eff6ff'], ['#8b5cf6','#f5f3ff'], ['#0891b2','#ecfeff'], ['#16a34a','#f0fdf4'],
  ['#d97706','#fffbeb'], ['#db2777','#fdf2f8'], ['#475569','#f8fafc'], ['#b45309','#fef3c7'],
];
function _tkdayColor(tid){
  let n = parseInt(tid, 10);
  if(isNaN(n)) n = 0;
  return TKDAY_PALETA[((n % 8) + 8) % 8];
}

// ── §4D "Icono por tipo de OT": distinción por FORMA (no por color -- el
//    color de los bloques ya está reservado para el técnico, ver paleta de
//    arriba). Mapa fijo; tipos personalizados (creados vía "Nuevo tipo",
//    superadmin) caen en el ícono por defecto. ──
const TKDAY_TIPO_ICONOS = {
  preventiva: 'bi-shield-check', correctiva: 'bi-tools', instalacion: 'bi-box-seam',
  inspeccion: 'bi-search', levantamiento: 'bi-clipboard2', visita_tecnica: 'bi-person-gear',
  garantia: 'bi-award', capacitacion: 'bi-mortarboard', repuesto: 'bi-gear',
  revision_interna: 'bi-clipboard-check', cambio_equipo: 'bi-arrow-repeat',
  desinstalacion: 'bi-box-arrow-up', visita_correctiva: 'bi-tools',
};
function _tkdayIconoTipo(tipo){
  return TKDAY_TIPO_ICONOS[String(tipo || '').toLowerCase()] || 'bi-clipboard-data';
}

function _tkdayToMin(hhmm){
  const p = String(hhmm == null ? '' : hhmm).split(':');
  const h = parseInt(p[0], 10);
  if(isNaN(h)) return null;
  const m = parseInt(p[1], 10);
  return h * 60 + (isNaN(m) ? 0 : m);
}
function _tkdayHHMM(min){
  return String(Math.floor(min / 60)).padStart(2, '0') + ':' + String(min % 60).padStart(2, '0');
}
function _tkdayGeom(hi, hf){
  const iniRaw = _tkdayToMin(hi);
  if(iniRaw === null) return null;
  let finRaw = _tkdayToMin(hf);
  const sinFin = (finRaw === null || finRaw <= iniRaw);
  if(sinFin) finRaw = iniRaw;                     // fin = toMin(hora_fin || hora_inicio)
  const clampTop = iniRaw < TKDAY_START;
  const clampBot = finRaw > TKDAY_END;
  const ini = Math.min(Math.max(iniRaw, TKDAY_START), TKDAY_END);
  const fin = Math.min(Math.max(finRaw, TKDAY_START), TKDAY_END);
  let top = (ini - TKDAY_START) * TKDAY_PXMIN;
  const height = Math.max((fin - ini) * TKDAY_PXMIN, TKDAY_MINH);
  if(top < 0) top = 0;
  if(top + height > TKDAY_H) top = Math.max(0, TKDAY_H - height);   // nunca fuera del lienzo
  // finLay = fin EFECTIVO que el bloque ocupa una vez pintado (nunca menor que
  // TKDAY_MINDUR). Es el que debe usar el reparto en columnas; finRaw es el
  // dato crudo y se conserva para el bloque resumen "+N mas".
  const finLay = Math.max(finRaw, iniRaw + TKDAY_MINDUR);
  return { iniRaw: iniRaw, finRaw: finRaw, finLay: finLay, top: top, height: height,
           sinFin: sinFin, clampTop: clampTop, clampBot: clampBot };
}
// Breakpoints de CONTENIDO por altura (≈duración) — ver tabla del diseño.
function _tkdayClase(h){
  if(h >= 64) return 'blk-lg';   // ≥ 80 min
  if(h >= 40) return 'blk-md';   // 50–79 min
  if(h >= 24) return 'blk-sm';   // 30–49 min
  return 'blk-xs';               // < 30 min
}
function _tkdayIniciales(nombre){
  const ps = String(nombre || '').trim().split(/\s+/).filter(Boolean);
  if(!ps.length) return '—';
  return (ps[0].charAt(0) + (ps[1] ? ps[1].charAt(0) : '')).toUpperCase();
}
function _tkdayFechaLarga(f){
  const p = String(f || '').split('-');
  if(p.length !== 3) return f || '—';
  const d = new Date(parseInt(p[0], 10), parseInt(p[1], 10) - 1, parseInt(p[2], 10));
  return TKDAY_DOW_LARGO[d.getDay()] + ' ' + p[2] + '/' + p[1] + '/' + p[0];
}
function _tkdayVisitaKey(v){
  if(v == null) return '';
  if(v.visita_id != null) return String(v.visita_id);
  if(v.id != null) return String(v.id);
  return 'n:' + String(v.numero_ot || '');
}
function _tkdayHoyStr(){
  const h = new Date();
  return h.getFullYear() + '-' + String(h.getMonth() + 1).padStart(2, '0') + '-' + String(h.getDate()).padStart(2, '0');
}

// ════════════════════════════════════════════════════════════
// §4A "Sugerir próximo horario libre" (propuesta Fable, tipo Doctoralia).
// Helper PURO: solo lee _O2F.cal.cache (ya cargado por o2fCalCargarMes) y
// los inputs vigentes del formulario -- cero backend nuevo, cero fetch.
//
//   _tkdayCalcularHueco(fecha, durMin, tecnicoIds) → 'HH:MM' | null
//
// Reglas:
//  - Toma las visitas del día `fecha` desde el cache del mes que corresponda.
//  - Si `tecnicoIds` trae elementos, filtra SOLO esos técnicos (carga real del
//    equipo elegido); si viene vacío, no filtra -- se asume ocupado cualquier
//    técnico (conservador: aún no se ha decidido a quién asignar).
//  - Excluye visitas canceladas (no representan carga real).
//  - Fusiona los intervalos ocupados y recorre candidatos cada 30 min desde
//    max(09:00, ahora+30min si `fecha` es hoy) hasta 20:00 - durMin.
//  - Devuelve el primer inicio sin intersección, o null si el día está lleno.
// ════════════════════════════════════════════════════════════
function _tkdayCalcularHueco(fecha, durMin, tecnicoIds){
  const key = _tkotCalKey(parseInt(fecha.slice(0, 4), 10), parseInt(fecha.slice(5, 7), 10));
  const mapa = _O2F.cal.cache[key] || {};
  const visitas = mapa[fecha] || [];
  const filtro = (Array.isArray(tecnicoIds) && tecnicoIds.length) ? new Set(tecnicoIds.map(String)) : null;
  const ocupados = [];
  visitas.forEach(function(v){
    if(String(v.estado || '').toLowerCase() === 'cancelada') return;
    if(filtro && !filtro.has(String(v.tecnico_id))) return;
    const ini = _tkdayToMin(v.hora_inicio);
    if(ini === null) return;
    let fin = _tkdayToMin(v.hora_fin);
    if(fin === null || fin <= ini) fin = ini + TKDAY_MINDUR;   // mismo criterio de duración mínima que el timeline
    ocupados.push([ini, fin]);
  });
  ocupados.sort(function(a, b){ return a[0] - b[0]; });
  const fusion = [];
  ocupados.forEach(function(iv){
    const last = fusion[fusion.length - 1];
    if(last && iv[0] <= last[1]) last[1] = Math.max(last[1], iv[1]);
    else fusion.push(iv.slice());
  });
  let candidato = 9 * 60;   // 09:00
  if(fecha === _tkdayHoyStr()){
    const ahora = new Date();
    let nowMin = ahora.getHours() * 60 + ahora.getMinutes() + 30;
    nowMin = Math.ceil(nowMin / 30) * 30;   // snap a la rejilla de 30 min
    candidato = Math.max(candidato, nowMin);
  }
  const fin = 20 * 60;   // 20:00
  for(; candidato + durMin <= fin; candidato += 30){
    const finCand = candidato + durMin;
    const choca = fusion.some(function(iv){ return iv[0] < finCand && candidato < iv[1]; });
    if(!choca) return _tkdayHHMM(candidato);
  }
  return null;
}

// ── Algoritmo de columnas tipo Google Calendar ──
//  1. ordenar por ini asc, luego fin desc
//  2. cluster = visitas encadenadas por solape (A.ini < B.fin && B.ini < A.fin);
//     termina cuando la siguiente empieza después del max(fin) acumulado
//  3. greedy: primera columna cuyo último fin <= ini de la visita; si no, columna nueva
//  4. left = col*(100/nCols)% · width = calc(100/nCols% - 4px)
//  5. si nCols > 4 ⇒ las columnas 4+ colapsan en un bloque resumen "+N más"
// _finLay: fin EFECTIVO (lo que el bloque ocupa pintado). Sin esto, un bloque
// sin hora_fin mide 0 min para el reparto pero 18px en pantalla, y dos de ellos
// a la misma hora acaban uno encima del otro.
function _finLay(it){ return it.finLay != null ? it.finLay : Math.max(it.fin, it.ini + TKDAY_MINDUR); }
function _tkdayLayout(items){
  const arr = items.slice().sort(function(a, b){ return (a.ini - b.ini) || (_finLay(b) - _finLay(a)); });
  const out = [];
  let i = 0;
  while(i < arr.length){
    let j = i, maxFin = _finLay(arr[i]);
    while(j + 1 < arr.length && arr[j + 1].ini < maxFin){
      j++;
      if(_finLay(arr[j]) > maxFin) maxFin = _finLay(arr[j]);
    }
    const cluster = arr.slice(i, j + 1);
    const colEnds = [];
    cluster.forEach(function(it){
      let col = -1;
      for(let c = 0; c < colEnds.length; c++){ if(colEnds[c] <= it.ini){ col = c; break; } }
      if(col === -1){ col = colEnds.length; colEnds.push(_finLay(it)); }
      else colEnds[col] = _finLay(it);
      it.col = col;
    });
    const n = colEnds.length;
    if(n > 4){
      const over = cluster.filter(function(it){ return it.col >= 3; });
      cluster.forEach(function(it){ if(it.col < 3){ it.nCols = 4; out.push(it); } });
      out.push({
        grupo: over.map(function(it){ return it.idx; }),
        ini: Math.min.apply(null, over.map(function(it){ return it.ini; })),
        fin: Math.max.apply(null, over.map(function(it){ return it.fin; })),
        col: 3, nCols: 4,
      });
    } else {
      cluster.forEach(function(it){ it.nCols = n; out.push(it); });
    }
    i = j + 1;
  }
  return out;
}

// ── Rejilla horaria + labels del gutter. Idempotente; se construye 1 vez. ──
function _tkdayRejilla(){
  if(_O2F.day.rejilla) return;
  const rej = document.getElementById('o2fLevDayRejilla');
  const gut = document.getElementById('o2fLevDayGutter');
  if(!rej || !gut) return;
  let r = '', g = '';
  for(let h = 8; h <= 20; h++){
    const y = (h - 8) * 48;
    r += '<div class="o2fday-hline" style="top:' + y + 'px"></div>';
    if(h < 20) r += '<div class="o2fday-hline half" style="top:' + (y + 24) + 'px"></div>';
    g += '<div class="o2fday-hlbl" style="top:' + (y - 7) + 'px">' + String(h).padStart(2, '0') + ':00</div>';
  }
  rej.innerHTML = r;
  gut.innerHTML = g;
  _O2F.day.rejilla = true;
}

// ── Línea "ahora": solo si el día mirado es hoy y la hora ∈ [08:00, 20:00]. ──
function _tkdayNow(fecha){
  const el = document.getElementById('o2fLevDayNow');
  if(!el) return;
  if(!fecha || fecha !== _tkdayHoyStr()){ el.innerHTML = ''; return; }
  const n = new Date();
  const min = n.getHours() * 60 + n.getMinutes();
  if(min < TKDAY_START || min > TKDAY_END){ el.innerHTML = ''; return; }
  el.innerHTML = '<div class="o2fday-now" style="top:' + ((min - TKDAY_START) * TKDAY_PXMIN) + 'px"'
    + ' aria-label="Ahora: ' + _tkdayHHMM(min) + '"></div>';
}

// ── Contenido del bloque según su altura ──
// §4D: el ícono por tipo solo se pinta en blk-lg/blk-md (variantes con
// espacio) -- blk-sm/blk-xs son demasiado comprimidas para sumar un glifo
// más sin atropellar hora/nombre, así que se quedan tal cual (REGLA #4.2).
function _tkdayInner(cls, o){
  const chk = o.done
    ? '<i class="bi bi-check-circle-fill" style="color:#16a34a;font-size:.7rem;flex-shrink:0" aria-hidden="true"></i>'
    : '';
  const numChip = o.num ? '<div><span class="otnum">' + esc(o.num) + '</span></div>' : '';
  const icoTipo = '<i class="bi ' + _tkdayIconoTipo(o.tipo) + ' me-1" style="font-size:.85em;flex-shrink:0" aria-hidden="true"></i>';
  if(cls === 'blk-lg'){
    return '<div class="l1"><span class="hora" style="color:' + o.borde + '">' + esc(o.rango) + '</span>' + chk + '</div>'
      + '<div class="who">' + icoTipo + esc(o.who) + '</div>' + numChip;
  }
  if(cls === 'blk-md'){
    return '<div class="l1" style="justify-content:space-between"><span class="who">' + icoTipo + esc(o.who) + '</span>'
      + '<span class="hora" style="color:' + o.borde + '">' + esc(o.rango) + '</span>' + chk + '</div>' + numChip;
  }
  if(cls === 'blk-sm'){
    return '<div class="l1" style="justify-content:space-between"><span class="who">' + esc(o.who) + '</span>'
      + '<span class="hora" style="color:' + o.borde + '">' + esc(o.hi) + '</span>' + chk + '</div>';
  }
  // blk-xs: solo iniciales + hora de inicio (todo el detalle vive en el popover)
  return '<div class="l1" style="justify-content:space-between"><span class="who">' + esc(o.ini2) + '</span>'
    + '<span class="hora" style="color:' + o.borde + '">' + esc(o.hi) + '</span></div>';
}

// ── Estados vacíos (§8). El lienzo con su rejilla NUNCA se oculta. ──
function _tkdayGhostSync(){
  const ghost = document.getElementById('o2fLevDayGhost');
  const scroll = document.getElementById('o2fLevDayScroll');
  const mine = document.getElementById('o2fLevDayMine');
  if(!ghost || !scroll) return;
  const fecha = _O2F.day.fecha;
  const hayBloques = (_O2F.day.visitas || []).length > 0;
  const hayMine = !!(mine && mine.style.display !== 'none');
  scroll.classList.toggle('sin-dia', !fecha);
  const key = fecha ? fecha.slice(0, 7) : null;
  if(fecha && _O2F.cal.error[key]){
    // (c) error de API — el modal jamás se rompe
    ghost.innerHTML = '<i class="bi bi-calendar-x" style="color:#fca5a5"></i>'
      + '<div class="g1">No se pudo cargar la agenda de este mes</div>'
      + '<div class="g-btn"><button type="button" class="btn btn-sm btn-outline-secondary" onclick="o2fCalReintentar()">'
      + '<i class="bi bi-arrow-clockwise me-1"></i>Reintentar</button></div>';
    ghost.style.display = '';
    return;
  }
  if(!fecha){
    // (b) sin día elegido
    ghost.innerHTML = '<i class="bi bi-hand-index-thumb" style="color:#cbd5e1"></i>'
      + '<div class="g1">Toca un día en el calendario</div>'
      + '<div class="g2">Ahí verás quién está agendado y podrás ubicar tu OT</div>';
    ghost.style.display = '';
    return;
  }
  if(!hayBloques && !hayMine){
    // (a) día libre
    ghost.innerHTML = '<i class="bi bi-calendar-check" style="color:#86efac"></i>'
      + '<div class="g1">Día libre — sin OTs agendadas</div>'
      + '<div class="g2">Tu OT se dibujará aquí al elegir horario</div>';
    ghost.style.display = '';
    return;
  }
  ghost.style.display = 'none';
}

// ── Render idempotente del día: reconstruye los bloques existentes.
//    El bloque .mine NO se reconstruye — solo se mueve (style.top/height)
//    para que la transición CSS opere y "se deslice" al teclear la hora.
function o2fdayRender(fecha, opts){
  opts = opts || {};
  const canvas = document.getElementById('o2fLevDayCanvas');
  const blks = document.getElementById('o2fLevDayBlks');
  if(!canvas || !blks) return;
  _tkdayRejilla();
  _O2F.day.fecha = fecha || null;

  // ── Header del día ──
  const elF = document.getElementById('o2fLevDayFecha');
  const elC = document.getElementById('o2fLevDayCnt');
  const elR = document.getElementById('o2fLevDayChipRango');
  const key = fecha ? _tkotCalKey(parseInt(fecha.slice(0, 4), 10), parseInt(fecha.slice(5, 7), 10)) : null;
  const mapa = key ? (_O2F.cal.cache[key] || {}) : {};
  const visitas = fecha ? (mapa[fecha] || []) : [];
  _O2F.day.visitas = visitas;
  if(elF) elF.textContent = fecha ? _tkdayFechaLarga(fecha) : '—';
  if(elC){
    if(fecha && visitas.length){
      elC.textContent = visitas.length + (visitas.length === 1 ? ' OT' : ' OTs');
      elC.style.display = '';
    } else elC.style.display = 'none';
  }
  if(elR){
    const rango = _tkotRangoActivo();
    if(rango && fecha && fecha >= rango.ini && fecha <= rango.fin){
      const d0 = new Date(rango.ini + 'T00:00:00'), d1 = new Date(rango.fin + 'T00:00:00'),
            dv = new Date(fecha + 'T00:00:00');
      const total = Math.round((d1 - d0) / 86400000) + 1;
      const nro = Math.round((dv - d0) / 86400000) + 1;
      elR.textContent = 'Día ' + nro + ' de ' + total + ' del rango';
      elR.style.display = '';
    } else elR.style.display = 'none';
  }
  const prevB = document.getElementById('o2fLevDayPrev'), nextB = document.getElementById('o2fLevDayNext');
  if(prevB) prevB.disabled = !fecha;
  if(nextB) nextB.disabled = !fecha;

  // ── Bloques existentes ──
  const items = [];
  visitas.forEach(function(v, idx){
    const g = _tkdayGeom(v.hora_inicio, v.hora_fin);
    if(!g) return;                       // sin hora_inicio utilizable: no se puede posicionar
    // finLay (fin efectivo pintado) va aparte de fin (crudo): el reparto en
    // columnas usa el primero, el bloque resumen "+N mas" el segundo.
    items.push({ idx: idx, ini: g.iniRaw, fin: g.finRaw, finLay: g.finLay, g: g, v: v });
  });
  const puestos = _tkdayLayout(items);
  const choqueKeys = _O2F.day.choqueKeys;
  const aplicaChoque = !!(choqueKeys && _O2F.day.choqueFecha && _O2F.day.choqueFecha === fecha);
  let html = '';
  puestos.forEach(function(p){
    const w = 'calc(' + (100 / p.nCols) + '% - 4px)';
    const l = (p.col * (100 / p.nCols)) + '%';
    if(p.grupo){
      // Resumen "+N más" (cluster con más de 4 columnas)
      const gg = _tkdayGeom(_tkdayHHMM(p.ini), _tkdayHHMM(p.fin));
      const h = gg ? gg.height : 18;
      html += '<div class="o2fday-blk blk-mas ' + _tkdayClase(h) + '" role="button" tabindex="0"'
        + ' style="top:' + (gg ? gg.top : 0) + 'px;height:' + h + 'px;left:' + l + ';width:' + w + ';'
        + 'border-color:#e5e7eb;border-left-color:#9ca3af"'
        + ' data-grupo="' + esc(p.grupo.join(',')) + '"'
        + ' aria-label="' + esc('+' + p.grupo.length + ' OTs más en esta franja') + '"'
        + ' onclick="o2fdayVerBloque(this)"'
        + ' onkeydown="if(event.key===\'Enter\'||event.key===\' \'){event.preventDefault();o2fdayVerBloque(this);}">'
        + '<div class="l1"><span class="who">+' + p.grupo.length + ' más</span></div></div>';
      return;
    }
    const v = p.v, g = p.g;
    const pal = _tkdayColor(v.tecnico_id);
    const who = v.tecnico_nombre || v.tecnico || 'Sin técnico asignado';
    const num = v.numero_ot || (v.visita_id != null ? ('OT #' + v.visita_id) : '');
    const hi = v.hora_inicio || '--';
    const hf = v.hora_fin || '';
    const est = String(v.estado || '').toLowerCase();
    const done = (est === 'completada' || est === 'cerrada');
    const cls = _tkdayClase(g.height);
    const extra = [];
    if(done) extra.push('est-done');
    if(est === 'en_curso') extra.push('est-curso');
    if(est === 'cancelada') extra.push('est-cancel');   // §2.4: aditivo, se sigue viendo (historial)
    if(g.sinFin) extra.push('sin-fin');
    if(g.clampTop) extra.push('clamp-top');
    if(g.clampBot) extra.push('clamp-bot');
    if(aplicaChoque && choqueKeys.has(_tkdayVisitaKey(v))) extra.push('en-choque');
    // aria-label lleva TODA la info (los lectores la leen); NO se usa title=
    // para no volver a traer el "globito" nativo.
    const aria = who + ' · ' + hi + (hf ? ' a ' + hf : '') + (num ? ' · ' + num : '')
      + (v.cliente_nombre ? ' · ' + v.cliente_nombre : '') + (est ? ' · ' + est : '');
    html += '<div class="o2fday-blk ' + cls + ' ' + extra.join(' ') + '" role="button" tabindex="0"'
      + ' style="top:' + g.top + 'px;height:' + g.height + 'px;left:' + l + ';width:' + w + ';'
      + 'border-color:' + pal[0] + ';border-left-color:' + pal[0] + ';background:' + pal[1] + '"'
      + ' data-idx="' + p.idx + '"'
      + ' aria-label="' + esc(aria) + '"'
      + ' onclick="o2fdayVerBloque(this)"'
      + ' onkeydown="if(event.key===\'Enter\'||event.key===\' \'){event.preventDefault();o2fdayVerBloque(this);}">'
      + _tkdayInner(cls, {
          borde: pal[0], who: who, num: num, hi: hi, tipo: v.tipo,
          rango: hi + (hf ? '–' + hf : ''), ini2: _tkdayIniciales(who), done: done,
        })
      + '</div>';
  });
  blks.innerHTML = html;
  _tkdayNow(fecha);
  o2fdayRenderMine();
  _tkdayAutoScroll();
}

// ── §1.1 "clic para crear": clic/tap en una zona VACÍA de la línea de
//    tiempo fija el horario de "Tu OT" ahí mismo. Un solo listener
//    delegado en el lienzo (no en cada .o2fday-blk) cubre toda el área
//    vacía sin instrumentar cada franja/hora por separado. ──
let _tkdayFlashToken = 0;
function o2fdaySlotClick(ev){
  if(ev.target.closest('.o2fday-blk')) return;   // clic en un bloque real -> lo maneja o2fdayVerBloque
  if(ev.target.closest('.g-btn')) return;        // clic en "Reintentar" del estado de error de mes
  if(!_O2F.day.fecha) return;                   // sin día elegido todavía: nada que fijar

  const canvas = document.getElementById('o2fLevDayCanvas');
  if(!canvas) return;

  // Geometría inversa (espejo de _tkdayGeom): y en pixeles -> minuto del día,
  // snap a bloques de 30 min, siempre dentro del rango 08:00–19:30 (para que
  // "hora inicio" nunca caiga en el último medio-bloque sin espacio a la derecha).
  const rect = canvas.getBoundingClientRect();
  const y = ev.clientY - rect.top;
  let min = TKDAY_START + (y / TKDAY_PXMIN);
  min = Math.floor(min / 30) * 30;
  min = Math.max(TKDAY_START, Math.min(min, TKDAY_END - 30));

  // Duración vigente del formulario se conserva; si no hay una válida (p.ej.
  // hora término <= hora inicio o campos vacíos), 60 min por defecto.
  const iniActual = _tkdayToMin(document.getElementById('o2fLevHoraIni')?.value || '');
  const finActual = _tkdayToMin(document.getElementById('o2fLevHoraFin')?.value || '');
  let dur = (iniActual != null && finActual != null) ? (finActual - iniActual) : 0;
  if(!(dur > 0)) dur = 60;
  const finMin = Math.min(min + dur, TKDAY_END);

  // Si se está mirando un día distinto al programado (flechas ‹ › de un
  // rango multi-día), el calendario "manda" primero -- mismo mecanismo que
  // un clic real en el mini-calendario (setea #levFechaProg y dispara su change).
  const fechaProg = document.getElementById('o2fLevFechaProg')?.value || '';
  if(_O2F.day.fecha !== fechaProg){
    o2fCalClicDia(_O2F.day.fecha);
  }

  (document.getElementById('o2fLevHoraIni')||{}).value = _tkdayHHMM(min);
  (document.getElementById('o2fLevHoraFin')||{}).value = _tkdayHHMM(finMin);
  o2fHoraInput();   // repinta "Tu OT" (transición CSS existente) + dispara el choque con debounce

  // Feedback visual -- sin toast (pedido explícito): pulso breve del marco
  // de "Tu OT". Token propio para que un segundo clic mientras el primer
  // pulso sigue activo no lo corte antes de tiempo (mismo patrón que
  // _O2F._choqueToken para respuestas async obsoletas).
  const mine = document.getElementById('o2fLevDayMine');
  if(mine){
    const miToken = ++_tkdayFlashToken;
    mine.classList.remove('flash');
    void mine.offsetWidth;   // fuerza reflow para poder re-disparar la animación en clics seguidos
    mine.classList.add('flash');
    setTimeout(function(){
      if(miToken === _tkdayFlashToken) mine.classList.remove('flash');
    }, 650);
  }
}
document.getElementById('o2fLevDayCanvas').addEventListener('click', o2fdaySlotClick);

// ── El bloque de la OT NUEVA. Se dibuja EN VIVO: en cada `input` de
//    #levHoraIni/#levHoraFin solo se actualiza style.top/height del MISMO
//    nodo (no se reconstruye), de modo que la transición CSS lo deslice. ──
function o2fdayRenderMine(){
  const mine = document.getElementById('o2fLevDayMine');
  if(!mine) return;
  const vista = _O2F.day.fecha;
  const fechaProg = document.getElementById('o2fLevFechaProg')?.value || '';
  let hi = '', hf = '';
  let dibujar = false;
  if(vista && fechaProg){
    if(vista === fechaProg){
      hi = document.getElementById('o2fLevHoraIni')?.value || '';
      hf = document.getElementById('o2fLevHoraFin')?.value || '';
      dibujar = true;
    } else {
      // Rango multi-día: "Tu OT" se dibuja en CADA día del rango al visitarlo.
      // Último día ⇒ sus horas propias; días intermedios ⇒ horas del primer día.
      const rango = _tkotRangoActivo();
      if(rango && vista > rango.ini && vista <= rango.fin){
        if(vista === rango.fin){
          hi = document.getElementById('o2fLevHoraIniFin')?.value || '';
          hf = document.getElementById('o2fLevHoraFinFin')?.value || '';
        } else {
          hi = document.getElementById('o2fLevHoraIni')?.value || '';
          hf = document.getElementById('o2fLevHoraFin')?.value || '';
        }
        dibujar = true;
      }
    }
  }
  const g = dibujar ? _tkdayGeom(hi, hf) : null;
  if(!g){ mine.style.display = 'none'; _tkdayGhostSync(); return; }

  const cls = _tkdayClase(g.height);
  const extra = ['mine', cls];
  if(g.sinFin) extra.push('sin-fin');
  if(g.clampTop) extra.push('clamp-top');
  if(g.clampBot) extra.push('clamp-bot');
  // El choque se pinta SOLO si el resultado vigente de o2fChequearChoque()
  // corresponde al día que se está mirando (para un día intermedio del rango
  // no hay resultado y no se inventa uno).
  if(_O2F.day.choque && _O2F.day.choqueFecha === vista) extra.push('choque');
  // Fix legibilidad (feedback Daniel 2026-07-15): si el bloque arranca pegado
  // al borde superior del lienzo, la píldora ".mine-tag" (que cabalga afuera
  // con top:-9px) quedaría recortada por el overflow del contenedor de
  // scroll -- se mete adentro del marco con la clase .tag-dentro (ver CSS).
  if(g.top < 12) extra.push('tag-dentro');
  // Preserva la clase temporal .flash (§1.1 "clic para crear", 600ms) si el
  // choque debounced (500ms) dispara un re-render mientras el pulso de
  // feedback sigue activo -- si no, este rebuild de className lo cortaría
  // antes de tiempo.
  if(mine.classList.contains('flash')) extra.push('flash');
  mine.className = 'o2fday-blk ' + extra.join(' ');
  mine.style.display = '';
  mine.style.top = g.top + 'px';
  mine.style.height = g.height + 'px';

  // Toda la info de "Tu OT" se muda a la píldora del borde superior (fuera
  // del área de texto): así nunca se superpone al texto de un bloque real
  // que coincida en horario. REGLA #4.2: misma información (hora +
  // técnicos), solo reubicada -- el cuerpo queda vacío a propósito, el
  // marco punteado + fondo translúcido siguen marcando la geometría.
  const nTec = _O2F.tecnicosSel.size;
  const rangoTxt = (hi || '--') + (hf && !g.sinFin ? '–' + hf : '');
  // Tarjeta viva (Fable 2026-07-19): la píldora lleva la identidad + rango
  // (siempre visible aun en bloques cortos); el cuerpo muestra la DURACIÓN
  // calculada y el estado de técnico, con densidad según la altura del bloque.
  let _dmin = 0;
  if(!g.sinFin && hi && hf && hi.indexOf(':') > 0 && hf.indexOf(':') > 0){
    const _pa = hi.split(':'), _pb = hf.split(':');
    _dmin = (parseInt(_pb[0],10)*60 + parseInt(_pb[1],10)) - (parseInt(_pa[0],10)*60 + parseInt(_pa[1],10));
  }
  let _durTxt = '';
  if(_dmin > 0){ const _h = Math.floor(_dmin/60), _m = _dmin % 60;
    _durTxt = (_h ? _h + ' h' : '') + (_h && _m ? ' ' : '') + (_m ? _m + ' min' : ''); }
  // 0 técnicos: guía neutra (se asignan en el Paso 3), no un "0" alarmante.
  const _tecPend = nTec === 0;
  const _tecTxt = _tecPend ? 'Asigna técnico en el paso 3' : (nTec + (nTec === 1 ? ' técnico' : ' técnicos'));
  const _tecIco = _tecPend ? 'bi-person-plus' : 'bi-person-check-fill';
  const _durLine = _durTxt ? '<div class="mine-dur"><i class="bi bi-clock-history"></i>' + esc(_durTxt) + '</div>' : '';
  const _tecLine = '<div class="mine-tec' + (_tecPend ? ' pendiente' : '') + '"><i class="bi ' + _tecIco + '"></i>' + esc(_tecTxt) + '</div>';
  const _H = g.height || 0;
  let _body = '';
  if(_H >= 48){ _body = '<i class="bi bi-calendar2-check mine-wm" aria-hidden="true"></i>' + _durLine + _tecLine; }
  else if(_H >= 28){ _body = _tecLine; }
  mine.innerHTML = '<span class="mine-tag"><i class="bi bi-geo-alt-fill" style="font-size:.5rem"></i>Tu OT · ' + esc(rangoTxt) + '</span>'
    + '<div class="mine-body">' + _body + '</div>';
  _tkdayGhostSync();
}

function _tkdayAutoScroll(){
  const scroll = document.getElementById('o2fLevDayScroll');
  const mine = document.getElementById('o2fLevDayMine');
  if(!scroll) return;
  let target = 48;   // 09:00 si no hay bloque propio
  if(mine && mine.style.display !== 'none') target = (parseFloat(mine.style.top) || 0) - 40;
  scroll.scrollTop = Math.max(0, target);
}

// ── Repinta SOLO las clases de choque (sin reconstruir el día) tras la
//    respuesta de /mantenciones/api/calendario/choque. ──
function o2fdayAplicarChoque(){
  const blks = document.getElementById('o2fLevDayBlks');
  if(!blks) return;
  const keys = _O2F.day.choqueKeys;
  const aplica = !!(keys && _O2F.day.choqueFecha && _O2F.day.choqueFecha === _O2F.day.fecha);
  const visitas = _O2F.day.visitas || [];
  blks.querySelectorAll('.o2fday-blk[data-idx]').forEach(function(el){
    const v = visitas[parseInt(el.dataset.idx, 10)];
    el.classList.toggle('en-choque', !!(aplica && v && keys.has(_tkdayVisitaKey(v))));
  });
  o2fdayRenderMine();
}

// ════════════════════════════════════════════════════════════
// DETALLE DE UN BLOQUE — overlay propio position:fixed, NO un modal
// Bootstrap anidado. Decisión de ingeniería: abrir un segundo
// bootstrap.Modal sobre #ot2ModalForm obliga a parchear a mano el
// z-index del modal (1080) y el de su backdrop (1079) en el tick
// siguiente, y a vigilar que Bootstrap restaure .modal-open/overflow del
// padre al cerrarse. Este overlay vive fuera de la pila de Bootstrap
// (z-index 1090 > 1055 del modal), trae su propio backdrop, no toca el
// body y no puede dejar el modal padre sin scroll. Cero bugs de backdrop.
// ════════════════════════════════════════════════════════════
let _tkdayPopEl = null;
let _tkdayPopKey = null;
let _tkdayPopVisita = null;   // visita de la OT del popover ABIERTO (solo caso uno===true) -- usada por Reprogramar/Reasignar/Cancelar

// Reusa las clases .tk-badge .bs-* YA definidas arriba en este archivo. Los
// estados de mant_visitas (programada/en_curso/completada/...) no tienen una
// clase propia -- caen en `bs-closed` (gris) en vez de quedar sin fondo.
const _TKDAY_BADGE_OK = ['open','in_progress','pending','resolved','closed','cancelado',
  'ot_pending_approval','ot_generated','ot_in_progress'];
function _tkdayBadgeCls(est){
  return 'tk-badge bs-' + (_TKDAY_BADGE_OK.indexOf(est) >= 0 ? est : 'closed');
}

function o2fdayVerBloque(el){
  if(!el) return;
  const visitas = _O2F.day.visitas || [];
  let lista = [];
  if(el.dataset.grupo){
    lista = el.dataset.grupo.split(',').map(function(s){ return visitas[parseInt(s, 10)]; }).filter(Boolean);
  } else {
    const v = visitas[parseInt(el.dataset.idx, 10)];
    if(v) lista = [v];
  }
  if(!lista.length) return;
  o2fdayAbrirDetalle(lista);
}

function _tkdayPopFila(icono, etiqueta, valor, mono){
  if(!valor) return '';
  return '<div class="pop-row"><i class="bi ' + icono + '" aria-hidden="true"></i>'
    + '<span class="text-muted" style="flex-shrink:0">' + esc(etiqueta) + '</span>'
    + '<span class="v' + (mono ? ' mono' : '') + '" style="margin-left:auto;text-align:right">' + esc(valor) + '</span></div>';
}

// ── §2.1 Gating: solo rol de gestión (superadmin/admin/supervisor/
//    ejecutivo, window._TKOT_ROL_GESTION) Y estado editable. El backend
//    (@_ot_can_metadata) vuelve a validar todo esto en el PUT -- esto es
//    solo para no mostrarle el botón a quien de todos modos sería rechazado. ──
const _TKDAY_ESTADOS_BLOQUEADOS = ['completada', 'cerrada', 'cancelada'];
function _tkdayPuedeGestionarUno(v){
  const est = String((v && v.estado) || '').toLowerCase();
  return !!window._TKOT_ROL_GESTION && _TKDAY_ESTADOS_BLOQUEADOS.indexOf(est) === -1;
}

// ── Busca una visita por id, primero en el día que se está mirando y si no
//    en la que quedó guardada al abrir el popover (fallback defensivo). ──
function _tkdayFindVisita(vid){
  const enLista = (_O2F.day.visitas || []).find(function(x){ return String(x.visita_id) === String(vid); });
  if(enLista) return enLista;
  return (_tkdayPopVisita && String(_tkdayPopVisita.visita_id) === String(vid)) ? _tkdayPopVisita : null;
}

function o2fdayAbrirDetalle(lista){
  o2fdayCerrarDetalle();
  const uno = lista.length === 1;
  const v0 = lista[0];
  const est0 = String(v0.estado || '').toLowerCase();
  _tkdayPopVisita = uno ? v0 : null;
  const vid0 = v0.visita_id != null ? v0.visita_id : '';
  // §4D: ícono por tipo también en la cabecera del popover -- solo cuando es
  // UNA OT (con varias en la misma franja no hay un tipo único que mostrar).
  const cab = uno
    ? '<i class="bi ' + _tkdayIconoTipo(v0.tipo) + '" style="font-size:1.05rem" aria-hidden="true"></i>'
      + '<span class="num">' + esc(v0.numero_ot || ('OT #' + (v0.visita_id != null ? v0.visita_id : '?'))) + '</span>'
      + (est0 ? '<span class="' + _tkdayBadgeCls(est0) + '">' + esc(est0.replace(/_/g, ' ')) + '</span>' : '')
    : '<span class="num">' + lista.length + ' OTs</span><span class="tk-badge bs-closed">misma franja</span>';

  const cuerpo = lista.map(function(v){
    const est = String(v.estado || '').toLowerCase();
    const hi = v.hora_inicio || '--', hf = v.hora_fin || '';
    // 2026-08-12 (Daniel: "que se distinga el cliente, que se indique qué
    // máquina, algo que le sirva de información para ubicarse"): CLIENTE
    // grande como título de la tarjeta (antes era una fila más, igual de
    // chica que "Ticket" o "Tipo" -- no había forma de distinguirlo de un
    // vistazo). El N° de OT + estado como sub-cabecera SOLO cuando hay
    // varias OT en la lista (uno===false) -- con una sola OT eso ya se
    // muestra en la cabecera del popover completo (variable `cab`, arriba);
    // repetirlo acá sería la misma info dos veces en la misma tarjeta.
    const cabecera = (uno ? '' : '<div class="pop-grp-head">'
        + '<span class="mono text-muted">' + esc(v.numero_ot || ('OT #' + (v.visita_id != null ? v.visita_id : '?'))) + '</span>'
        + (est ? '<span class="' + _tkdayBadgeCls(est) + '">' + esc(est.replace(/_/g, ' ')) + '</span>' : '')
        + '</div>')
      + '<div class="pop-grp-cliente">' + esc(v.cliente_nombre || 'Cliente sin nombre') + '</div>';
    // "equipos" lo entrega mant_calendario_mes desde 2026-08-12 (GROUP BY
    // batch en el backend, no 1 query por visita) -- puede venir vacío en OT
    // sin tareas ligadas a máquina todavía (recién creada, o levantamiento
    // por descubrimiento sin materializar). Tope de 3 nombres + "+N" para no
    // desbordar la tarjeta con clientes que tienen muchos equipos en una OT.
    const eqList = Array.isArray(v.equipos) ? v.equipos : [];
    const eqTxt = eqList.length
      ? (eqList.slice(0, 3).join(', ') + (eqList.length > 3 ? ' +' + (eqList.length - 3) : ''))
      : '';
    return '<div class="pop-grp est-' + esc(est || 'closed') + '">'
      + cabecera
      // SOLO campos que el endpoint YA entrega (mant_calendario_mes):
      // visita_id, numero_ot, ticket_id, numero_ticket, cliente_id,
      // cliente_nombre, titulo, tipo, estado, hora_inicio, hora_fin,
      // tecnico_id, tecnico_nombre, equipos.
      + _tkdayPopFila('bi-gear-wide-connected', 'Equipos', eqTxt, false)
      + _tkdayPopFila('bi-person-badge', 'Técnico', v.tecnico_nombre || v.tecnico || 'Sin asignar', false)
      + _tkdayPopFila('bi-clock', 'Horario', hi + (hf ? '–' + hf : ''), true)
      + _tkdayPopFila('bi-calendar3', 'Fecha', _O2F.day.fecha
          ? _O2F.day.fecha.split('-').reverse().join('/') : '', true)
      + _tkdayPopFila('bi-card-text', 'Título', v.titulo || '', false)
      + _tkdayPopFila('bi-tag', 'Tipo', v.tipo || '', false)
      + _tkdayPopFila('bi-ticket-detailed', 'Ticket', v.numero_ticket || '', true)
      + (uno ? '' : '<div class="pop-row" style="padding-top:6px">'
          // §2.5 "+N más": botón "Gestionar" junto a "Abrir OT completa" --
          // reabre el popover individual (con su barra de acciones) sin
          // lógica nueva, solo delega en o2fdayAbrirDetalle([v]).
          + (_tkdayPuedeGestionarUno(v)
              ? '<button type="button" class="btn btn-sm btn-outline-secondary" onclick="o2fdayGestionarUno(' + (v.visita_id != null ? v.visita_id : '') + ')">'
                + '<i class="bi bi-gear me-1"></i>Gestionar</button>'
              : '')
          + '<a class="btn btn-sm btn-outline-danger" style="margin-left:auto" target="_blank" rel="noopener"'
          + ' href="/mantenciones/ot/' + encodeURIComponent(v.visita_id != null ? v.visita_id : '') + '">'
          + 'Abrir OT completa <i class="bi bi-box-arrow-up-right ms-1"></i></a></div>')
      + '</div>';
  }).join('');

  // §2.1 — barra de acciones (Reprogramar/Reasignar/Cancelar) + contenedores
  // vacíos de los mini-formularios inline (§2.2/§2.3), solo para 1 OT.
  // "Abrir OT completa" se queda tal cual donde ya estaba (REGLA #4.2).
  const acciones = (uno && _tkdayPuedeGestionarUno(v0))
    ? '<div class="pop-actions">'
      + '<button type="button" class="btn btn-sm btn-outline-secondary" onclick="o2fdayPopAccion(\'reprogramar\',' + vid0 + ')"><i class="bi bi-calendar2-week me-1"></i>Reprogramar</button>'
      + '<button type="button" class="btn btn-sm btn-outline-secondary" onclick="o2fdayPopAccion(\'reasignar\',' + vid0 + ')"><i class="bi bi-person-gear me-1"></i>Reasignar</button>'
      + '<button type="button" class="btn btn-sm btn-outline-danger" onclick="o2fdayPopAccion(\'cancelar\',' + vid0 + ')"><i class="bi bi-x-circle me-1"></i>Cancelar OT</button>'
      + '</div>'
      + '<div id="o2fdayReprogForm"></div>'
      + '<div id="o2fdayReasigForm"></div>'
    : '';

  const pie = uno
    ? '<div class="pop-foot">'
      + '<button type="button" class="btn btn-sm btn-light" onclick="o2fdayCerrarDetalle()">Cerrar</button>'
      + '<a class="btn btn-sm btn-outline-danger" target="_blank" rel="noopener"'
      + ' href="/mantenciones/ot/' + encodeURIComponent(v0.visita_id != null ? v0.visita_id : '') + '">'
      + 'Abrir OT completa <i class="bi bi-box-arrow-up-right ms-1"></i></a></div>'
    : '<div class="pop-foot"><button type="button" class="btn btn-sm btn-light" onclick="o2fdayCerrarDetalle()">Cerrar</button></div>';

  const pop = document.createElement('div');
  pop.className = 'o2fday-pop';
  pop.setAttribute('role', 'dialog');
  pop.setAttribute('aria-modal', 'true');
  pop.innerHTML = '<div class="pop-card">'
    + '<div class="pop-head">' + cab
    + '<button type="button" class="btn-close btn-close-white" aria-label="Cerrar" onclick="o2fdayCerrarDetalle()"></button></div>'
    + '<div class="pop-body">' + cuerpo + '</div>' + acciones + pie + '</div>';
  // Clic en el backdrop propio cierra; clic dentro de la tarjeta no.
  pop.addEventListener('click', function(ev){ if(ev.target === pop) o2fdayCerrarDetalle(); });
  document.body.appendChild(pop);
  _tkdayPopEl = pop;
  // ESC: se captura ANTES de que llegue a Bootstrap, si no cerraría el
  // modal padre "Generar OT" y se perdería todo lo cargado.
  _tkdayPopKey = function(ev){
    if(ev.key === 'Escape'){ ev.preventDefault(); ev.stopPropagation(); o2fdayCerrarDetalle(); }
  };
  document.addEventListener('keydown', _tkdayPopKey, true);
  const foco = pop.querySelector('.btn-close');
  if(foco) foco.focus();
}

function o2fdayCerrarDetalle(){
  if(_tkdayPopKey){ document.removeEventListener('keydown', _tkdayPopKey, true); _tkdayPopKey = null; }
  if(_tkdayPopEl){ _tkdayPopEl.remove(); _tkdayPopEl = null; }
  if(_tkdayRpChoqueTimer){ clearTimeout(_tkdayRpChoqueTimer); _tkdayRpChoqueTimer = null; }
  _tkdayPopVisita = null;
}

// ── §2.5: atajo del listado "+N más" -- reabre el popover en modo 1 OT. ──
function o2fdayGestionarUno(vid){
  const v = _tkdayFindVisita(vid);
  if(v) o2fdayAbrirDetalle([v]);
}

// ════════════════════════════════════════════════════════════
// §2 "BLOQUES EDITABLES" — Reprogramar / Reasignar / Cancelar.
// Decisión de arquitectura (§2.0): NUNCA se abre el wizard de 7 pasos en
// modo edición. Los 3 reusan el backend YA EXISTENTE
// PUT /mantenciones/api/visitas/<vid> (mant_visita_update, app.py),
// protegido por @_ot_can_metadata -- el mismo que ya usa Mantenciones.
// El endpoint de choque YA soporta exclude_visita_id (reagendar sin chocar
// consigo misma). Cero endpoints nuevos.
// ════════════════════════════════════════════════════════════
function o2fdayPopAccion(accion, vid){
  if(accion === 'reprogramar') return o2fdayReprogAbrir(vid);
  if(accion === 'reasignar') return o2fdayReasigAbrir(vid);
  if(accion === 'cancelar') return o2fdayCancelarOT(vid);
}

// Cierra/vacía los 2 mini-formularios inline (botón "Cancelar" de cada uno,
// y al abrir uno se cierra el otro -- solo tiene sentido 1 a la vez).
function o2fdayFormsCerrar(){
  const rp = document.getElementById('o2fdayReprogForm');
  const ra = document.getElementById('o2fdayReasigForm');
  if(rp) rp.innerHTML = '';
  if(ra) ra.innerHTML = '';
  if(_tkdayRpChoqueTimer){ clearTimeout(_tkdayRpChoqueTimer); _tkdayRpChoqueTimer = null; }
}

// Recarga el mes tras cualquier PUT que haya podido mover una OT de día/mes:
// invalida TODO el caché (no solo el mes vigente, la fecha puede haber
// cambiado de mes) y vuelve a pedir el mes que se está mirando ahora mismo.
async function _tkdayRecargarMes(){
  _O2F.cal.cache = {};
  _O2F.cal.error = {};
  await o2fCalCargarMes();
}

// ── §2.2 Reprogramar: mini-formulario inline (SOLO camino "escribir los
//    campos" -- el "tocar y colocar" queda para una fase posterior). ──
function o2fdayReprogAbrir(vid){
  const v = _tkdayFindVisita(vid);
  if(!v) return;
  o2fdayFormsCerrar();
  const cont = document.getElementById('o2fdayReprogForm');
  if(!cont) return;
  const fechaBase = _O2F.day.fecha || '';
  cont.innerHTML = '<div class="pop-inline">'
    + '<div class="small fw-bold mb-2"><i class="bi bi-calendar2-week me-1" style="color:#dc2626"></i>Reprogramar</div>'
    + '<div class="row-fields">'
    + '<div><label for="o2fdayRpFecha">Fecha</label>'
    + '<input type="date" id="o2fdayRpFecha" class="form-control form-control-sm" value="' + esc(fechaBase) + '"></div>'
    + '<div><label for="o2fdayRpFechaFin">Fecha término (rango, opcional)</label>'
    + '<input type="date" id="o2fdayRpFechaFin" class="form-control form-control-sm" value="' + esc(v.fecha_fin || '') + '"></div>'
    + '</div>'
    + '<div class="row-fields">'
    + '<div><label for="o2fdayRpHoraIni">Hora inicio</label>'
    + '<input type="time" id="o2fdayRpHoraIni" class="form-control form-control-sm" value="' + esc(v.hora_inicio || '') + '"></div>'
    + '<div><label for="o2fdayRpHoraFin">Hora término</label>'
    + '<input type="time" id="o2fdayRpHoraFin" class="form-control form-control-sm" value="' + esc(v.hora_fin || '') + '"></div>'
    + '</div>'
    + '<div id="o2fdayRpChoqueAlert" class="o2fcal-choque-alert" style="display:none"></div>'
    + '<div class="actions">'
    + '<button type="button" class="btn btn-sm btn-light" onclick="o2fdayFormsCerrar()">Cancelar</button>'
    + '<button type="button" class="btn btn-sm btn-ilus" onclick="o2fdayReprogGuardar(' + vid + ')">Guardar cambios</button>'
    + '</div></div>';
  ['o2fdayRpFecha', 'o2fdayRpHoraIni', 'o2fdayRpHoraFin', 'o2fdayRpFechaFin'].forEach(function(id){
    const el = document.getElementById(id);
    if(el) el.addEventListener('change', function(){ _tkdayReprogChequearChoqueDebounced(v); });
  });
}

// Choque EN VIVO del mini-formulario -- debounce propio de 400ms (más corto
// que el del wizard porque el usuario está afinando un solo campo a la vez).
let _tkdayRpChoqueTimer = null;
function _tkdayReprogChequearChoqueDebounced(v){
  if(_tkdayRpChoqueTimer) clearTimeout(_tkdayRpChoqueTimer);
  _tkdayRpChoqueTimer = setTimeout(function(){ _tkdayReprogChequearChoque(v); }, 400);
}
async function _tkdayReprogChequearChoque(v){
  const alertEl = document.getElementById('o2fdayRpChoqueAlert');
  if(!alertEl) return;
  const fecha = document.getElementById('o2fdayRpFecha')?.value || '';
  const horaIni = document.getElementById('o2fdayRpHoraIni')?.value || '';
  const horaFin = document.getElementById('o2fdayRpHoraFin')?.value || '';
  const fechaFin = document.getElementById('o2fdayRpFechaFin')?.value || '';
  if(!fecha || !v.tecnico_id){ alertEl.style.display = 'none'; return; }
  try{
    const qs = new URLSearchParams({
      tecnico_id: String(v.tecnico_id), fecha: fecha,
      exclude_visita_id: String(v.visita_id),
    });
    if(horaIni) qs.set('hora_ini', horaIni);
    if(horaFin) qs.set('hora_fin', horaFin);
    if(fechaFin && fechaFin > fecha) qs.set('fecha_fin', fechaFin);
    const r = await fetch('/mantenciones/api/calendario/choque?' + qs.toString());
    const d = r.ok ? await r.json() : null;
    const lista = (d && Array.isArray(d.tecnicos)) ? d.tecnicos : [];
    const entry = lista.find(function(t){ return String(t.tecnico_id) === String(v.tecnico_id); }) || lista[0];
    if(entry && entry.choque){
      const detalle = (entry.visitas_choque || []).map(function(c){
        return (c.numero_ot || ('OT #' + c.visita_id)) + ' de ' + (c.hora_inicio || '--') + ' a ' + (c.hora_fin || '--');
      }).join(', ');
      alertEl.innerHTML = '<i class="bi bi-exclamation-triangle-fill"></i><div><b>'
        + esc(entry.tecnico_nombre || v.tecnico_nombre || 'El técnico')
        + '</b> ya tiene una visita agendada este día' + (detalle ? ' (' + esc(detalle) + ')' : '') + '.</div>';
      alertEl.style.display = 'flex';
    } else {
      alertEl.style.display = 'none';
    }
  }catch(e){
    console.warn('o2fday reprog choque:', e);   // aviso, no bloqueo -- no se toca el formulario
  }
}

function o2fdayReprogGuardar(vid){
  const v = _tkdayFindVisita(vid) || {};
  const fecha = document.getElementById('o2fdayRpFecha')?.value || '';
  const horaIni = document.getElementById('o2fdayRpHoraIni')?.value || '';
  const horaFin = document.getElementById('o2fdayRpHoraFin')?.value || '';
  const fechaFin = document.getElementById('o2fdayRpFechaFin')?.value || '';
  if(!fecha){ ilusToast('Indica la fecha', {type:'warning'}); return; }
  if(horaIni && horaFin && horaIni >= horaFin){ ilusToast('La hora de término debe ser posterior a la de inicio', {type:'warning'}); return; }
  if(fechaFin && fechaFin < fecha){ ilusToast('La fecha de término no puede ser anterior a la de inicio', {type:'warning'}); return; }

  // Solo los campos que realmente cambiaron (spec §2.2) -- compara contra
  // el día que se está mirando (== fecha_programada real de esta visita,
  // ver _tkdayFindVisita/o2fdayRender) y los campos crudos de `v`.
  const body = {};
  if(fecha !== (_O2F.day.fecha || '')) body.fecha_programada = fecha;
  if(horaIni !== (v.hora_inicio || '')) body.hora_inicio = horaIni;
  if(horaFin !== (v.hora_fin || '')) body.hora_fin = horaFin;
  if(fechaFin !== (v.fecha_fin || '')) body.fecha_fin = fechaFin || null;
  if(!Object.keys(body).length){ ilusToast('No hay cambios que guardar', {type:'info'}); return; }

  const numero = v.numero_ot || ('OT #' + vid);
  fetch('/mantenciones/api/visitas/' + vid, {
    method: 'PUT', headers: {'Content-Type':'application/json'}, body: JSON.stringify(body),
  }).then(function(r){
    return r.json().catch(function(){ return {}; }).then(function(d){ return {r: r, d: d}; });
  }).then(function(res){
    if(res.r.ok && res.d.ok !== false){
      ilusToast(numero + ' reprogramada', {type:'success'});
      o2fdayCerrarDetalle();
      _tkdayRecargarMes();
    } else {
      ilusToast(res.d.error || 'No se pudo reprogramar la OT', {type:'error'});
    }
  }).catch(function(){
    ilusToast('No se pudo reprogramar la OT', {type:'error'});
  });
}

// ── §2.3 Reasignar técnico: mini-formulario inline con los técnicos YA
//    disponibles en el modal (mismo array que puebla la sección de Técnicos
//    del wizard) -- sin fetch adicional. ──
function o2fdayReasigAbrir(vid){
  const v = _tkdayFindVisita(vid);
  if(!v) return;
  o2fdayFormsCerrar();
  const cont = document.getElementById('o2fdayReasigForm');
  if(!cont) return;
  const techs = _O2F.tecnicosDisponibles || [];
  const opts = techs.map(function(t){
    const sel = String(t.id) === String(v.tecnico_id) ? ' selected' : '';
    return '<option value="' + t.id + '"' + sel + '>' + esc(t.nombre || t.email || ('Téc #' + t.id)) + '</option>';
  }).join('');
  cont.innerHTML = '<div class="pop-inline">'
    + '<div class="small fw-bold mb-2"><i class="bi bi-person-gear me-1" style="color:#dc2626"></i>Reasignar técnico</div>'
    + '<label for="o2fdayRaSelect">Técnico asignado</label>'
    + '<select id="o2fdayRaSelect" class="form-select form-select-sm">' + opts + '</select>'
    + '<div class="actions">'
    + '<button type="button" class="btn btn-sm btn-light" onclick="o2fdayFormsCerrar()">Cancelar</button>'
    + '<button type="button" class="btn btn-sm btn-ilus" onclick="o2fdayReasigGuardar(' + vid + ')">Guardar</button>'
    + '</div></div>';
}

function o2fdayReasigGuardar(vid){
  const sel = document.getElementById('o2fdayRaSelect');
  const tid = sel ? sel.value : '';
  if(!tid){ ilusToast('Selecciona un técnico', {type:'warning'}); return; }
  const v = _tkdayFindVisita(vid) || {};
  const numero = v.numero_ot || ('OT #' + vid);
  fetch('/mantenciones/api/visitas/' + vid, {
    method: 'PUT', headers: {'Content-Type':'application/json'},
    body: JSON.stringify({ tecnico_user_id: parseInt(tid, 10) }),
  }).then(function(r){
    return r.json().catch(function(){ return {}; }).then(function(d){ return {r: r, d: d}; });
  }).then(function(res){
    if(res.r.ok && res.d.ok !== false){
      ilusToast(numero + ' reasignada', {type:'success'});
      o2fdayCerrarDetalle();
      _tkdayRecargarMes();
    } else {
      ilusToast(res.d.error || 'No se pudo reasignar la OT', {type:'error'});
    }
  }).catch(function(){
    ilusToast('No se pudo reasignar la OT', {type:'error'});
  });
}

// ── §2.4 Cancelar: SIEMPRE PUT estado='cancelada' -- NUNCA el DELETE
//    (conserva historial, la puede revertir un admin editando la OT). ──
async function o2fdayCancelarOT(vid){
  const v = _tkdayFindVisita(vid) || {};
  const numero = v.numero_ot || ('OT #' + vid);
  const ok = await ilusConfirm({
    title: 'Cancelar ' + numero,
    sub: 'La OT quedará cancelada pero conserva su historial. Esta acción la puede revertir un administrador editando la OT.',
    okLabel: 'Cancelar OT', cancelLabel: 'Volver',
    danger: true,
  });
  if(!ok) return;
  try{
    const r = await fetch('/mantenciones/api/visitas/' + vid, {
      method: 'PUT', headers: {'Content-Type':'application/json'}, body: JSON.stringify({ estado: 'cancelada' }),
    });
    const d = await r.json().catch(function(){ return {}; });
    if(r.ok && d.ok !== false){
      ilusToast(numero + ' cancelada', {type:'success'});
      o2fdayCerrarDetalle();
      await _tkdayRecargarMes();
    } else {
      ilusToast(d.error || 'No se pudo cancelar la OT', {type:'error'});
    }
  }catch(e){
    ilusToast('No se pudo cancelar la OT', {type:'error'});
  }
}

// ── Choque de horario EN VIVO. El backend de generar-ot solo valida al
//    técnico PRINCIPAL antes de crear la OT (ver reporte de investigación);
//    aquí, en el frontend, se consulta por CADA técnico seleccionado para
//    dar visibilidad completa sin bloquear -- es un aviso, no un gate.
//    Debounce 500ms para no saturar de requests mientras el usuario teclea. ──
let _tkotChoqueTimer = null;
function o2fChequearChoqueDebounced(){
  if(_tkotChoqueTimer) clearTimeout(_tkotChoqueTimer);
  _tkotChoqueTimer = setTimeout(o2fChequearChoque, 500);
}
async function o2fChequearChoque(){
  const warn = document.getElementById('o2fLevCalWarnChoque');
  if(!warn) return;
  const fecha = document.getElementById('o2fLevFechaProg')?.value || '';
  const horaIni = document.getElementById('o2fLevHoraIni')?.value || '';
  const horaFin = document.getElementById('o2fLevHoraFin')?.value || '';
  const tecnicoIds = Array.from(_O2F.tecnicosSel);
  if(!fecha || !tecnicoIds.length){
    warn.style.display = 'none';
    _O2F.day.choque = false; _O2F.day.choqueFecha = null; _O2F.day.choqueKeys = null;
    o2fdayAplicarChoque();
    return;
  }
  const idNombre = {};
  (_O2F.tecnicosDisponibles || []).forEach(function(t){ idNombre[t.id] = t.nombre || t.email || ('Téc #' + t.id); });
  // §2.6.2: si el formulario tiene el rango multi-día activo (#levRangoDias
  // marcado + #levFechaFin válida), se manda también fecha_fin -- el backend
  // YA lo acepta (fix reciente de choque), solo faltaba que el JS lo enviara.
  const rangoAct = _tkotRangoActivo();
  const miToken = ++_O2F._choqueToken;
  try{
    const resultados = await Promise.all(tecnicoIds.map(function(tid){
      const qs = new URLSearchParams({ tecnico_id: String(tid), fecha: fecha });
      if(horaIni) qs.set('hora_ini', horaIni);
      if(horaFin) qs.set('hora_fin', horaFin);
      if(rangoAct) qs.set('fecha_fin', rangoAct.fin);
      return fetch('/mantenciones/api/calendario/choque?' + qs.toString())
        .then(function(r){ return r.ok ? r.json() : null; })
        .then(function(d){ return { tid: tid, d: d }; })
        .catch(function(){ return { tid: tid, d: null }; });
    }));
    if(miToken !== _O2F._choqueToken) return; // el usuario siguió cambiando algo -- respuesta obsoleta
    const mensajes = [];
    // MISMO resultado, dos consumidores: el banner explicativo (el porqué) y
    // la geometría de los bloques (.choque en "Tu OT" + .en-choque en las
    // visitas en conflicto = la señal pre-verbal). NO se duplica la lógica.
    const choqueKeys = new Set();
    resultados.forEach(function(res){
      const d = res.d;
      // Contrato real del backend (/mantenciones/api/calendario/choque):
      // { tecnicos: [{tecnico_id, tecnico_nombre, choque:bool, visitas_choque:[...]}] }
      // -- se pide 1 tecnico_id por request, así que el técnico buscado
      // está en tecnicos[0] (o se busca por id como refuerzo).
      const lista = (d && Array.isArray(d.tecnicos)) ? d.tecnicos : [];
      const entry = lista.find(function(t){ return String(t.tecnico_id) === String(res.tid); }) || lista[0];
      if(!entry || !entry.choque) return;
      const nombre = entry.tecnico_nombre || idNombre[res.tid] || 'El técnico';
      const visitas = entry.visitas_choque || [];
      visitas.forEach(function(v){ choqueKeys.add(_tkdayVisitaKey(v)); });
      const detalle = visitas.map(function(v){
        return (v.numero_ot || ('OT #' + v.visita_id)) + ' de ' + (v.hora_inicio || '--') + ' a ' + (v.hora_fin || '--');
      }).join(', ');
      mensajes.push('<b>' + esc(nombre) + '</b> ya tiene una visita agendada este día'
        + (detalle ? ' (' + esc(detalle) + ')' : '') + '.');
    });
    if(mensajes.length){
      // §4A: dentro del banner de choque, un atajo directo a "buscar hueco
      // libre" -- llama exactamente la misma o2fdaySugerirHueco() del chip
      // de la cabecera del día. Cero lógica duplicada.
      warn.innerHTML = '<i class="bi bi-exclamation-triangle-fill"></i><div>' + mensajes.join('<br>')
        + '<div style="margin-top:8px"><button type="button" class="btn btn-sm btn-outline-danger" onclick="o2fdaySugerirHueco()">'
        + '<i class="bi bi-stars me-1"></i>Buscar hueco libre</button></div></div>';
      warn.style.display = 'flex';
    } else {
      warn.style.display = 'none';
    }
    _O2F.day.choque = mensajes.length > 0;
    _O2F.day.choqueFecha = fecha;
    _O2F.day.choqueKeys = choqueKeys;
    o2fdayAplicarChoque();
  }catch(e){
    console.warn('o2f choque:', e);
  }
}

// ════════════════════════════════════════════════════════════
// §4A (cont.) — orquestación de "Sugerir horario": dos puntos de entrada
// (chip de la cabecera + botón del banner de choque) llaman a la MISMA
// o2fdaySugerirHueco(). Cero backend nuevo: _tkdayAsegurarMesCache() reusa
// el mismo GET /mantenciones/api/calendario/mes/<anio>/<mes> que ya usa
// o2fCalCargarMes(), solo que sin tocar _O2F.cal.anio/mes (no mueve el
// mes visible mientras busca "de reojo" hacia adelante).
// ════════════════════════════════════════════════════════════

// ── Asegura que el caché del mes <anio>/<mes> esté cargado, SIN mover el
//    mes visible (_O2F.cal.anio/mes) ni repintar la grilla -- distinto de
//    o2fCalCargarMes(), que sí hace ambas cosas porque asume que el mes
//    pedido es el que se está mirando. ──
async function _tkdayAsegurarMesCache(anio, mes){
  const key = _tkotCalKey(anio, mes);
  if(_O2F.cal.cache[key]) return _O2F.cal.cache[key];
  try{
    const r = await fetch('/mantenciones/api/calendario/mes/' + anio + '/' + mes);
    if(!r.ok) throw new Error('http ' + r.status);
    const d = await r.json();
    _O2F.cal.cache[key] = _tkotCalNormalizarMes(d);
    delete _O2F.cal.error[key];
  }catch(e){
    console.warn('o2fday sugerir - cache mes:', e);
    _O2F.cal.cache[key] = _O2F.cal.cache[key] || {};
    _O2F.cal.error[key] = true;
  }
  return _O2F.cal.cache[key];
}

// ── Recorre `fecha` + hasta `maxDias` días hacia adelante buscando el
//    primer hueco (ver _tkdayCalcularHueco). Devuelve {fecha, hora} o null. ──
async function _tkdayBuscarHuecoRango(fechaInicio, durMin, tecnicoIds, maxDias){
  const p = fechaInicio.split('-').map(function(s){ return parseInt(s, 10); });
  const d = new Date(p[0], p[1] - 1, p[2]);
  for(let i = 0; i <= maxDias; i++){
    const f = d.getFullYear() + '-' + String(d.getMonth() + 1).padStart(2, '0') + '-' + String(d.getDate()).padStart(2, '0');
    await _tkdayAsegurarMesCache(d.getFullYear(), d.getMonth() + 1);
    const hora = _tkdayCalcularHueco(f, durMin, tecnicoIds);
    if(hora) return { fecha: f, hora: hora };
    d.setDate(d.getDate() + 1);
  }
  return null;
}

// ── Selecciona `fecha` como si el usuario hubiera hecho clic en el
//    mini-calendario (mismo efecto que o2fCalClicDia → o2fFechaProgChange),
//    pero AWAIT-eable: se necesita esperar a que el día quede pintado antes
//    de setear las horas encontradas y disparar el flash de "Tu OT". ──
async function _tkdayIrADia(fecha){
  const fp = document.getElementById('o2fLevFechaProg');
  if(fp) fp.value = fecha;
  // Chips de duración (2026-07-19, P1): este camino (Sugerir horario) escribe
  // #levFechaProg DIRECTO y nunca pasaba por o2fFechaProgChange -- el rango
  // quedaba sin re-anclar (fin < inicio) y los chips no se repintaban.
  _levReanclarFin();
  const y = parseInt(fecha.slice(0, 4), 10), m = parseInt(fecha.slice(5, 7), 10);
  if(y && m && (y !== _O2F.cal.anio || m !== _O2F.cal.mes)){
    _O2F.cal.anio = y; _O2F.cal.mes = m;
    await o2fCalCargarMes();   // repinta grilla del mes nuevo (cache ya tibio por la búsqueda)
  }
  o2fCalSelDia(fecha, { silencio: true });
  levChipsRefresh();
}

// ── Punto de entrada único (chip de cabecera + botón del banner de choque).
//    Busca el hueco, salta al día + hora encontrados y da el flash visual de
//    "Tu OT" (reusa .flash, mismo mecanismo que o2fdaySlotClick). Si no hay
//    hueco en 14 días, toast de aviso -- NUNCA alert() nativo (REGLA #1). ──
async function o2fdaySugerirHueco(){
  const btn = document.getElementById('o2fLevDaySugerirBtn');
  const original = btn ? btn.innerHTML : null;
  if(btn){ btn.disabled = true; btn.innerHTML = '<i class="bi bi-hourglass-split"></i>Buscando…'; }
  try{
    const iniActual = _tkdayToMin(document.getElementById('o2fLevHoraIni')?.value || '');
    const finActual = _tkdayToMin(document.getElementById('o2fLevHoraFin')?.value || '');
    const durMin = (iniActual != null && finActual != null && finActual > iniActual) ? (finActual - iniActual) : 60;
    const tecnicoIds = Array.from(_O2F.tecnicosSel);
    const fechaBase = _O2F.day.fecha || document.getElementById('o2fLevFechaProg')?.value || _tkdayHoyStr();

    const encontrado = await _tkdayBuscarHuecoRango(fechaBase, durMin, tecnicoIds, 14);
    if(!encontrado){
      ilusToast('Sin huecos en los próximos 14 días para ese equipo de técnicos', { type: 'warning' });
      return;
    }

    await _tkdayIrADia(encontrado.fecha);
    (document.getElementById('o2fLevHoraIni')||{}).value = encontrado.hora;
    (document.getElementById('o2fLevHoraFin')||{}).value = _tkdayHHMM(_tkdayToMin(encontrado.hora) + durMin);
    o2fHoraInput();   // repinta "Tu OT" en su nueva posición + dispara el choque (debería salir limpio)

    const mine = document.getElementById('o2fLevDayMine');
    if(mine){
      mine.classList.remove('flash');
      void mine.offsetWidth;   // fuerza reflow -- mismo truco que o2fdaySlotClick
      mine.classList.add('flash');
      setTimeout(function(){ mine.classList.remove('flash'); }, 650);
    }
  }catch(e){
    console.warn('o2fday sugerir hueco:', e);
    ilusToast('No se pudo calcular un horario sugerido', { type: 'error' });
  }finally{
    if(btn){ btn.disabled = false; btn.innerHTML = original; }
  }
}

// ── Paso 1: tipo de OT (idéntico a Mantenciones) ──
const _TKOT_TIPO_DESC = {
  levantamiento:  'Levantamiento de ficha: documentación visual de cada equipo. Se aplica automáticamente la plantilla estándar a todos los equipos seleccionados.',
  instalacion:    'Instalación: registro de equipos nuevos puestos en sitio. Incluye verificación de embalaje, conexión, encendido y capacitación.',
  preventiva:     'Mantención preventiva: visita planificada con checklist estándar (limpieza, lubricación, ajustes, test de carga).',
  visita_tecnica: 'Visita técnica: atención puntual al cliente para diagnóstico, ajustes o consultas técnicas.',
  correctiva:     'Mantención correctiva: reparación de falla específica reportada por el cliente.',
  inspeccion:     'Inspección: revisión visual + funcional sin intervención, para diagnóstico o auditoría.',
  // FIX 2026-08-11: descripciones para los 8 tipos agregados al select.
  garantia:          'Garantía: cobertura del fabricante/proveedor, sin cargo al cliente.',
  cambio_equipo:     'Cambio de equipo: retiro de un equipo y puesta en sitio de otro en su lugar.',
  desinstalacion:    'Desinstalación: retiro definitivo de un equipo del sitio del cliente.',
  capacitacion:      'Capacitación: instrucción al personal del cliente sobre uso o mantenimiento de equipos.',
  repuesto:          'Repuesto: cambio o instalación puntual de un repuesto específico.',
  revision_interna:  'Trabajo de bodega: tarea interna de ILUS (sin firma de cliente).',
  visita_correctiva: 'Visita correctiva: atención puntual a una falla reportada, fuera de un ciclo de mantención.',
  control_calidad:   'Control de Calidad: revisión/verificación de productos o equipos.',
};
// Nombre "de convención" de la plantilla estándar por tipo de OT. Espejo de
// _PLANTILLA_ESTANDAR_NOMBRE en app.py — aquí solo se MUESTRA qué checklist
// va a aplicar el backend.
const _TKOT_PLANTILLA_NOMBRE = {
  levantamiento:  'Levantamiento fotográfico estándar',
  instalacion:    'Instalación estándar',
  preventiva:     'Mantención preventiva estándar',
  correctiva:     'Mantención correctiva estándar',
  visita_tecnica: 'Visita técnica estándar',
  inspeccion:     'Inspección estándar',
  garantia:       'Garantía estándar',
};

function o2fPlantillaParaTipo(tipo){
  const todas = ((_O2F.plantillas && _O2F.plantillas.all) || [])
    .filter(p => p.activa !== false && (p.items_count || 0) > 0);
  const nom = _TKOT_PLANTILLA_NOMBRE[tipo];
  if(nom){
    const exacta = todas.find(p => p.nombre === nom);
    if(exacta) return exacta;
  }
  const cand = todas.filter(p => p.tipo_visita === tipo);
  cand.sort((a, b) =>
    (Number(!!b.es_sistema) - Number(!!a.es_sistema)) ||
    ((b.items_count || 0) - (a.items_count || 0)) ||
    ((a.id || 0) - (b.id || 0)));
  return cand[0] || null;
}

function o2fPintarPlantilla(tipo){
  const box = document.getElementById('o2f_otPlantillaInfo');
  if(!box) return;
  if(!(_O2F.plantillas && _O2F.plantillas.cargadas)){
    box.innerHTML = ''; box.removeAttribute('style'); return;
  }
  const p = o2fPlantillaParaTipo(tipo);
  if(p){
    box.style.cssText = 'font-size:.78rem;background:#dcfce7;color:#166534;border:1px solid #86efac';
    box.className = 'small mt-2 px-2 py-1 rounded';
    box.innerHTML = '<i class="bi bi-check-circle-fill me-1"></i>Checklist asociado: ' +
      '<strong>' + esc(p.nombre) + '</strong> · ' + p.items_count + ' tarea(s) por equipo';
  } else {
    box.style.cssText = 'font-size:.78rem;background:#fff8e1;color:#92400e;border:1px solid #fcd34d';
    box.className = 'small mt-2 px-2 py-1 rounded';
    box.innerHTML = '<i class="bi bi-exclamation-triangle-fill me-1"></i>' +
      'No hay checklist estándar para este tipo. Cada equipo llevará una tarea de ' +
      'registro con foto — puedes asignar una plantilla equipo por equipo en el paso ' +
      '<strong>Equipos</strong>.';
  }
}

// FIX 2026-08-11 (Daniel, probando en vivo): el selector de plantilla del
// Paso 1 ("Tarea 4", 2026-08-10) quedaba duplicado con "Plantillas extra"
// del Paso 5 (por equipo) -- se retiró. La elección de plantilla vive SOLO
// en el paso de Equipos ahora. Funciones _tkotCargarPlantillaSelector /
// _tkotRenderPlantillaSelector / _tkotPlantillaSelectorDebeOcultarse /
// o2fPlantillaSelectChange eliminadas (sin otro caller — grep verificado).

function o2fTipoChange(){
  const tipo = document.getElementById('o2f_otTipo')?.value;
  const desc = document.getElementById('o2f_otTipoDescripcion');
  if(desc && tipo) desc.innerHTML = '<i class="bi bi-info-circle me-1"></i>' + (_TKOT_TIPO_DESC[tipo] || '');
  o2fPintarPlantilla(tipo);

  const garWrap = document.getElementById('o2f_otGarantiaWrap');
  if(garWrap){
    if(tipo === 'levantamiento'){
      garWrap.style.display = 'none';
      const g = document.getElementById('o2f_otAplicaGarantia'); if(g) g.checked = false;
    } else garWrap.style.display = '';
  }

  // MANDA EL DESPLEGABLE (Daniel 2026-08-06): al cambiar de tipo la modalidad
  // se limpia SIEMPRE, no queda 'equipos' por debajo. Espejo de Mantenciones.
  const modoWrap = document.getElementById('o2f_otModoLevWrap');
  if(modoWrap){
    modoWrap.style.display = (tipo === 'levantamiento') ? '' : 'none';
    o2fModoSet(null);
  }

  const tit = document.getElementById('o2fLevSelectTitulo');
  if(tit && (!tit.value || /^(Levantamiento|Instalación|Mantención|Visita|Inspección) /.test(tit.value))){
    const fecha = new Date().toLocaleDateString('es-CL');
    const labels = { levantamiento:'Levantamiento', instalacion:'Instalación', preventiva:'Mantención preventiva',
      visita_tecnica:'Visita técnica', correctiva:'Mantención correctiva', inspeccion:'Inspección' };
    tit.value = (labels[tipo] || 'OT') + ' ' + fecha;
  }

  // Instalación + cliente SIN ficha -> forzar preselección total bloqueada.
  o2fAplicarForzadoInstalacion();
  o2fRefreshStepStates();
}

// null = sin elegir. La modalidad NO viene preseleccionada: manda el tipo de
// OT del desplegable (ver o2fTipoChange).
function o2fModoSet(modo){
  _O2F.modo = (modo === 'descubrimiento' || modo === 'equipos') ? modo : null;
  const cEq = document.getElementById('o2fLevModoEquipos');
  const cDes = document.getElementById('o2fLevModoDescubrir');
  const hint = document.getElementById('o2fLevModoHint');
  if(cEq) cEq.classList.toggle('on', _O2F.modo === 'equipos');
  if(cDes) cDes.classList.toggle('on', _O2F.modo === 'descubrimiento');
  if(hint) hint.style.display = (_O2F.modo === 'descubrimiento') ? '' : 'none';
  o2fRefreshStepStates();
}

async function o2fAbrirCrearTipoOT(){
  await ilusAlert({
    title: 'Crear nuevo tipo de OT',
    message: 'Esta función está en desarrollo (próxima fase).',
    sub: 'Los tipos están definidos en el ENUM de mant_visitas. Para agregar uno nuevo se requiere migración manual.',
    type: 'info',
  });
}

// ── Paso 5, modo cliente: equipos DE LA FICHA que ya están renderizados en
//    el DOM del tab "Equipos" ([data-maquina-id]) -- mismo origen que usaba
//    el modal viejo (#modalLevSelector) en abrirLevantamientoSelector()
//    (static/mant_ficha.js). Reconstruye un array con la MISMA forma que
//    o2fRenderEquipos()/o2fGenerar() esperan de equiposCache (e.maquina_id
//    presente en TODOS los casos, porque acá siempre son equipos de ficha
//    real -- nunca "sin ficha" como puede pasar con un ticket). ──
function _tkotLeerEquiposDesdeDOM(){
  return Array.from(document.querySelectorAll('[data-maquina-id]')).map(function(tr){
    const mid = tr.dataset.maquinaId;
    const nombre = (tr.querySelector('.eq-name-main')?.textContent || '').trim() || ('Equipo #' + mid);
    return {
      id: mid,
      maquina_id: mid,
      nombre: nombre,
      sku: tr.dataset.sku || '',
      serie: tr.dataset.serie || '',
      aplica: tr.dataset.aplica !== '0',
    };
  });
}

// ── Paso 5: equipos DEL TICKET (no del DOM de una ficha) ──
function o2fRenderEquipos(){
  const tbody = document.getElementById('o2fLevSelectTbody');
  if(!tbody) return;
  const eqs = equiposCache || [];
  // FIX 2026-08-12 (bug real reportado por Daniel: una plantilla que SÍ se
  // marcaba por equipo terminaba sin aplicarse -- la OT caía en la tarea
  // de respaldo genérica). Causa: esta función se vuelve a llamar cuando
  // cambia el tipo de OT y eso activa/desactiva el modo "instalación sin
  // ficha" (o2fAplicarForzadoInstalacion -> o2fRenderEquipos), y antes
  // borraba TODAS las plantillas ya elegidas por equipo, en silencio, sin
  // avisar. Ahora solo se limpia la selección de equipos que YA NO están
  // en la lista actual -- si el equipo sigue ahí, su plantilla elegida se
  // conserva. Con equiposCache vacío (modal recién abierto) esto sigue
  // limpiando todo, como antes.
  const _keysActuales = new Set(eqs.map(_tkotEqKey));
  Object.keys(_O2F.eqPlantillas).forEach(function(k){
    if(!_keysActuales.has(k)) delete _O2F.eqPlantillas[k];
  });
  if(!eqs.length){
    tbody.innerHTML = '<tr><td colspan="3" class="text-muted small text-center py-3">'
      + (_TKOT_MODO_CLIENTE ? 'Este cliente no tiene equipos registrados todavía.' : 'Este ticket no tiene equipos declarados.')
      + '</td></tr>';
    (document.getElementById('o2fLevEqCount')||{}).textContent = '0';
    return;
  }
  const forzado = _O2F.forzarTodosEquipos;
  tbody.innerHTML = eqs.map(function(e){
    const key = _tkotEqKey(e);
    const nombre = e.nombre || e.erp_kopr || 'Equipo';
    const sinFicha = !e.maquina_id;
    // `aplica` (modo cliente, ver _tkotLeerEquiposDesdeDOM): equipos marcados
    // `data-aplica="0"` en el DOM de la ficha (ej. accesorios/collarines que
    // no llevan mantención) -- se muestran atenuados con el mismo aviso que
    // usaba el modal viejo (#modalLevSelector), pero siguen siendo
    // seleccionables (Regla #4.2: no se pierde la opción de marcarlos).
    const noAplica = e.aplica === false;
    const rowCls = forzado ? ' lev-eq-forzado' : '';
    const rowOpacity = (!forzado && noAplica) ? 'opacity:.5;' : '';
    const checkedAttr = forzado ? 'checked disabled' : '';
    const sinMantBdg = noAplica ? ' <span style="font-size:.63rem;color:#9ca3af;margin-left:5px;font-weight:400">(sin mantención)</span>' : '';
    return '<tr class="'+rowCls+'" style="'+rowOpacity+'cursor:'+(forzado?'default':'pointer')+'" '
      + (forzado ? '' : 'onclick="const c=this.querySelector(\'.lev-eq-chk\');c.checked=!c.checked;o2fRecalcEqCount();event.stopPropagation();"') + '>'
      + '<td><input type="checkbox" class="lev-eq-chk" data-key="'+esc(key)+'" '+checkedAttr+' '
      + (forzado?'':'onchange="o2fRecalcEqCount()" onclick="event.stopPropagation()"') + '></td>'
      + '<td><strong>'+esc(nombre)+'</strong>'
      + (sinFicha ? ' <span style="font-size:.63rem;color:#9ca3af;margin-left:5px;font-weight:400">(sin ficha aún)</span>' : sinMantBdg)
      + (e.sku ? '<div class="small text-muted">'+esc(e.sku)+'</div>' : '')
      + (e.serie ? '<div class="small text-muted">S/N: '+esc(e.serie)+'</div>' : '')
      // 2026-07-19 (Daniel): la OT hereda el contexto del ticket -- observacion
      // por equipo (e.notas, ya viaja en memoria via equiposCache) como linea
      // secundaria, mismo patron/clase que la tabla de equipos de la ficha
      // (ver eq-name-sub ~linea 3119).
      + (e.notas ? '<div class="eq-name-sub text-truncate" style="max-width:220px" title="'+esc(e.notas)+'">'+esc(e.notas)+'</div>' : '')
      + '</td>'
      + '<td onclick="event.stopPropagation()">'
      // 2026-08-13 (Daniel probando en vivo, TK-2026-01313: "no puedo
      // seleccionar la plantilla del equipo"): un equipo forzado (cliente
      // sin ficha) SIEMPRE está marcado -- ya no hace falta bloquear el
      // botón esperando el click del checkbox, se renderiza habilitado
      // directo. El backend ya sabe aplicar esta selección a un equipo sin
      // ficha (plantillas_por_ticket_equipo, app.py) -- antes se descartaba
      // en silencio aunque se hubiera podido elegir.
      // 2026-08-13 (Daniel, en vivo: "no quiero nada automático, todo lo
      // debe escoger el usuario y si no escoge no lo debe dejar avanzar"):
      // ya no hay plantilla "incluida" gratis -- el botón parte en rojo
      // pidiendo la elección (obligatoria, el backend la exige) y pasa a
      // verde recién cuando el usuario elige algo en o2fGuardarMultiPlantilla.
      + (forzado
          ? '<button id="lev-pl-btn-'+esc(key)+'" class="btn btn-xs w-100 lev-pl-btn-pendiente" '
            + 'style="font-size:.72rem;padding:.25rem .4rem" '
            + 'onclick="o2fAbrirMultiPlantilla(\''+esc(key)+'\', \''+esc(nombre)+'\')" title="Elegir plantilla de checklist para este equipo (obligatorio)">'
            + '<i class="bi bi-exclamation-circle me-1"></i><span id="lev-pl-count-'+esc(key)+'">Elegir plantilla</span></button>'
          : '<button id="lev-pl-btn-'+esc(key)+'" class="btn btn-xs btn-outline-secondary w-100" '
            + 'style="font-size:.72rem;padding:.25rem .4rem;opacity:.4;pointer-events:none" '
            + 'onclick="o2fAbrirMultiPlantilla(\''+esc(key)+'\', \''+esc(nombre)+'\')" title="Selecciona el equipo primero">'
            + '<i class="bi bi-lock me-1"></i><span id="lev-pl-count-'+esc(key)+'">marca el equipo</span></button>')
      + '</td></tr>';
  }).join('');
  o2fRecalcEqCount();
}

// Cliente sin ficha (CID null): todos los equipos del ticket van completos,
// marcados y bloqueados (no hay "ficha" contra la cual elegir un subconjunto
// -- Daniel: "ya lo requeriría de un plan, porque el cliente no existe").
//
// 2026-08-12 (Daniel, probando en vivo un ticket de instalación con cliente
// nuevo): "necesito que si es instalación o visita cree el cliente... si es
// mantención correctiva, preventiva, [...] inspección, garantía, cambio,
// visita correctiva, ahí podría creársele". El backend YA crea la ficha
// mínima para CUALQUIER tipo cuando no hay cid ni match de RUT
// (tickets_module.py, rama `cliente_recien_creado` de tk_api_generar_ot --
// nunca estuvo condicionada por tipo_ot). Lo único que faltaba era esto:
// el frontend solo pintaba el modo "forzado" (equipos bloqueados + aviso)
// para 'instalacion' -- en los demás tipos, con un cliente nuevo, el
// usuario veía checkboxes normales sin ningún aviso de que estaba creando
// un cliente nuevo. Se extiende a los tipos que Daniel nombró explícitamente.
// Deliberadamente AFUERA: 'levantamiento' (tiene su propio flujo de
// descubrimiento) y 'desinstalacion' (quitar un equipo presupone que ya
// existe en la ficha de alguien, no tiene sentido crear cliente+equipo a
// la vez para eso).
const _TKOT_TIPOS_FUERZAN_CLIENTE_NUEVO = [
  'instalacion', 'correctiva', 'preventiva', 'visita_tecnica',
  'inspeccion', 'garantia', 'cambio_equipo', 'visita_correctiva',
];
function o2fAplicarForzadoInstalacion(){
  const tipo = document.getElementById('o2f_otTipo')?.value;
  const warn = document.getElementById('o2fAvisoSinFicha');
  const debeForzar = _TKOT_TIPOS_FUERZAN_CLIENTE_NUEVO.indexOf(tipo) !== -1
    && _O2F.clienteResuelto && !_O2F.cid && (equiposCache||[]).length > 0;
  if(warn) warn.style.display = (_O2F.clienteResuelto && !_O2F.cid) ? '' : 'none';
  if(debeForzar !== _O2F.forzarTodosEquipos){
    _O2F.forzarTodosEquipos = debeForzar;
    o2fRenderEquipos();
  }
}

function o2fToggleTodos(){
  if(_O2F.forzarTodosEquipos) return; // bloqueado -- no se puede desmarcar
  const checks = document.querySelectorAll('.lev-eq-chk');
  const marcados = document.querySelectorAll('.lev-eq-chk:checked').length;
  const newState = marcados < checks.length;
  checks.forEach(function(c){ c.checked = newState; });
  o2fRecalcEqCount();
}

function o2fRecalcEqCount(){
  const checks = document.querySelectorAll('.lev-eq-chk');
  const n = document.querySelectorAll('.lev-eq-chk:checked').length;
  const el = document.getElementById('o2fLevEqCount');
  if(el) el.textContent = String(n);
  const tBtn = document.getElementById('o2f_btnLevToggleTodos');
  if(tBtn){
    tBtn.innerHTML = (n === checks.length && checks.length > 0)
      ? '<i class="bi bi-square me-1"></i>Desmarcar todos'
      : '<i class="bi bi-check2-square me-1"></i>Marcar todos';
  }
  checks.forEach(function(c){
    const key = c.dataset.key;
    const plBtn = document.getElementById('lev-pl-btn-' + key);
    if(!plBtn) return;
    const seleccionado = c.checked;
    if(seleccionado){
      plBtn.style.opacity = '1'; plBtn.style.pointerEvents = 'auto';
      _tkotPintarBotonPlantilla(key);
    } else {
      plBtn.style.opacity = '.4'; plBtn.style.pointerEvents = 'none';
      plBtn.title = 'Selecciona el equipo primero';
    }
  });
  o2fRefreshStepStates();
}

// 2026-08-13 (Daniel: "si no escoge no lo debe dejar avanzar") -- estado
// visual único del botón de plantilla por equipo: rojo mientras falta
// elegir, verde con el conteo cuando ya eligió. Centralizado acá porque
// lo pintan 3 lugares distintos (render inicial, recalc al marcar el
// equipo, y al guardar la selección) y antes se desincronizaban.
// Equipos MARCADOS que todavía no tienen plantilla elegida. Devuelve
// [{key, nombre}]. Es la fuente única de la regla "sin plantilla no
// avanza" (Daniel 2026-08-13): la usan el estado del Paso 5 y la
// validación del submit, para que el punto verde y el bloqueo del botón
// nunca se contradigan. El backend valida lo mismo por su cuenta
// (_ot_validar_plantillas_elegidas) -- esto es solo para no hacerle
// perder el viaje al usuario.
function _tkotEquiposSinPlantilla(){
  const tipo = (document.getElementById('o2f_otTipo') || {}).value || '';
  // Levantamiento usa siempre el mismo checklist fotográfico estándar,
  // no se elige por equipo -- mismo criterio que el backend.
  if(tipo === 'levantamiento') return [];
  const faltan = [];
  document.querySelectorAll('.lev-eq-chk:checked').forEach(function(c){
    const key = c.dataset.key;
    const n = (_O2F.eqPlantillas[key] && _O2F.eqPlantillas[key].size) || 0;
    if(n) return;
    const fila = c.closest('tr');
    const nombre = (fila && fila.querySelector('td:nth-child(2) strong'))
      ? fila.querySelector('td:nth-child(2) strong').textContent.trim()
      : ('equipo ' + key);
    faltan.push({ key: key, nombre: nombre });
  });
  return faltan;
}

function _tkotPintarBotonPlantilla(key){
  const btn = document.getElementById('lev-pl-btn-' + key);
  const span = document.getElementById('lev-pl-count-' + key);
  if(!btn || !span) return;
  const n = (_O2F.eqPlantillas[key] && _O2F.eqPlantillas[key].size) || 0;
  btn.classList.remove('lev-pl-btn-pendiente', 'lev-pl-btn-ok', 'btn-outline-primary', 'btn-outline-secondary');
  if(n){
    btn.classList.add('lev-pl-btn-ok');
    btn.title = 'Cambiar la plantilla de checklist de este equipo';
    btn.querySelector('i')?.setAttribute('class', 'bi bi-check-circle-fill me-1');
    span.textContent = n + ' plantilla' + (n > 1 ? 's' : '');
  } else {
    btn.classList.add('lev-pl-btn-pendiente');
    btn.title = 'Elegir plantilla de checklist para este equipo (obligatorio)';
    btn.querySelector('i')?.setAttribute('class', 'bi bi-exclamation-circle me-1');
    span.textContent = 'Elegir plantilla';
  }
}

// ── Multi-plantilla por equipo (idéntico a Mantenciones, clave = _tkotEqKey) ──
async function o2fAbrirMultiPlantilla(key, eqNombre){
  const todas = _O2F.plantillas.all || [];
  if(!todas.length){
    ilusAlert({ title:'Sin plantillas', message:'No hay plantillas activas en el sistema.',
      sub:'Pide a un administrador que cree plantillas en /mantenciones/plantillas.', type:'warning' });
    return;
  }
  const tipoActual = document.getElementById('o2f_otTipo')?.value || '';
  // FIX 2026-08-11 (Daniel, probando en vivo): filtrar por la categoría del
  // tipo de OT elegido en el Paso 1 -- antes mostraba TODAS las plantillas
  // del sistema mezcladas (Instalación, Control de Calidad, Garantía,
  // Levantamiento...) sin importar qué tipo estuviera seleccionado.
  // Best-effort: si no se puede resolver la categoría (mapa no cargó, o el
  // tipo no tiene fila en mant_categoria_tipo_map), cae a mostrar todas --
  // nunca bloquea agregar una plantilla extra.
  const mapa = await _tkotCargarCategoriaMap();
  const categoriaActual = mapa ? mapa[tipoActual] : null;
  const plantillas = categoriaActual
    ? todas.filter(p => p.categoria_admin === categoriaActual)
    : todas;
  let modal = document.getElementById('modalMultiPlantilla');
  if(modal){ try{ bootstrap.Modal.getInstance(modal)?.dispose(); }catch(e){} modal.remove(); }
  modal = document.createElement('div');
  modal.id = 'modalMultiPlantilla';
  modal.className = 'modal fade';
  modal.tabIndex = -1;
  const seleccionadas = _O2F.eqPlantillas[key] || new Set();
  const _catAviso = (categoriaActual && plantillas.length < todas.length)
    ? ' Se muestran solo las plantillas de la categoría de este tipo de OT.'
    : '';
  modal.innerHTML = '<div class="modal-dialog modal-dialog-centered modal-dialog-scrollable">'
    + '<div class="modal-content" style="border-radius:12px">'
    + '<div class="modal-header" style="background:linear-gradient(135deg,#1e3a8a,#3b82f6);color:#fff">'
    + '<div><h6 class="modal-title fw-bold mb-0"><i class="bi bi-list-check me-2"></i>Plantillas extra</h6>'
    + '<small style="opacity:.85">'+esc(eqNombre)+'</small></div>'
    + '<button type="button" class="btn-close btn-close-white" data-bs-dismiss="modal"></button></div>'
    + '<div class="modal-body"><div class="alert alert-info py-2 small mb-2"><i class="bi bi-info-circle me-1"></i>'
    + 'La plantilla del tipo de OT ('+esc(tipoActual||'—')+') ya se aplica automáticamente. '
    + 'Aquí puedes agregar plantillas <strong>adicionales</strong> para este equipo.'+_catAviso+'</div>'
    + (plantillas.length ? '' : '<div class="text-muted small text-center py-3">No hay plantillas extra en esta categoría.</div>')
    + '<div id="multiPlantillaList">' + plantillas.map(function(p){
        return '<label class="d-flex align-items-start gap-2 p-2 mb-1 border rounded" style="cursor:pointer;background:'+(seleccionadas.has(p.id)?'#eff6ff':'#fff')+'">'
          + '<input type="checkbox" class="mp-chk" data-pid="'+p.id+'" '+(seleccionadas.has(p.id)?'checked':'')+' style="margin-top:3px">'
          + '<div class="flex-grow-1"><div class="fw-bold small">'+esc(p.nombre)+'</div>'
          + '<div class="text-muted" style="font-size:.7rem">'
          + (p.tipo_visita?'<span class="badge bg-secondary me-1">'+esc(p.tipo_visita)+'</span>':'')
          + (p.items_count||0)+' tarea(s)'
          + (p.descripcion?' · '+esc(p.descripcion.substring(0,80)):'') + '</div></div></label>';
      }).join('') + '</div></div>'
    + '<div class="modal-footer"><button type="button" class="btn btn-light" data-bs-dismiss="modal">Cancelar</button>'
    + '<button type="button" class="btn btn-primary" onclick="o2fGuardarMultiPlantilla(\''+esc(key)+'\')">'
    + '<i class="bi bi-check-lg me-1"></i>Guardar selección</button></div></div></div>';
  document.body.appendChild(modal);
  new bootstrap.Modal(modal).show();
}
function o2fGuardarMultiPlantilla(key){
  const modal = document.getElementById('modalMultiPlantilla');
  if(!modal) return;
  const ids = Array.from(modal.querySelectorAll('.mp-chk:checked')).map(function(c){ return parseInt(c.dataset.pid); });
  if(ids.length) _O2F.eqPlantillas[key] = new Set(ids); else delete _O2F.eqPlantillas[key];
  _tkotPintarBotonPlantilla(key);
  o2fRefreshStepStates();
  bootstrap.Modal.getInstance(modal)?.hide();
  if(ids.length){
    ilusToast('✓ ' + ids.length + ' plantilla(s) asignada(s) al equipo', {type:'success', duration:2000});
  } else {
    ilusToast('Este equipo quedó sin plantilla — es obligatoria para crear la OT', {type:'warning'});
  }
}

// ── Acceso y logística (idéntico a Mantenciones) ──
function o2fSetAccesoYN(btn){
  const target = btn.dataset.target, val = btn.dataset.val;
  const hidden = document.getElementById(target);
  if(!hidden) return;
  if(hidden.value === val){
    hidden.value = '';
    document.querySelectorAll('.lev-yn-btn[data-target="'+target+'"]').forEach(function(b){ b.classList.remove('active'); });
    o2fRefreshStepStates();
    return;
  }
  hidden.value = val;
  document.querySelectorAll('.lev-yn-btn[data-target="'+target+'"]').forEach(function(b){
    b.classList.toggle('active', b.dataset.val === val);
  });
  o2fRefreshStepStates();
}
function o2fResetAccesoLogistica(){
  ['o2f_acceso_ascensor','o2f_acceso_estacionamiento'].forEach(function(id){
    const h = document.getElementById(id);
    if(h) h.value = '';
    document.querySelectorAll('.lev-yn-btn[data-target="'+id+'"]').forEach(function(b){ b.classList.remove('active'); });
  });
  const piso = document.getElementById('o2f_acceso_piso');
  const notas = document.getElementById('o2f_acceso_notas');
  if(piso) piso.value = '';
  if(notas) notas.value = '';
  o2fRefreshStepStates();
}

// ── Adjuntos preliminares (idéntico a Mantenciones) ──
function _tkotBytesPretty(n){
  if(!n && n!==0) return '';
  if(n < 1024) return n + ' B';
  if(n < 1024*1024) return (n/1024).toFixed(0) + ' KB';
  return (n/(1024*1024)).toFixed(1) + ' MB';
}
function _tkotAdjIconClass(file){
  const m=(file.type||'').toLowerCase(), n=(file.name||'').toLowerCase();
  if(m.startsWith('image/')) return 'bi-camera text-primary';
  if(m==='application/pdf'||n.endsWith('.pdf')) return 'bi-file-earmark-pdf text-danger';
  if(m.startsWith('video/')) return 'bi-film text-info';
  if(m.startsWith('audio/')) return 'bi-mic text-purple';
  if(/\.(docx?|xlsx?|pptx?|txt|csv)$/i.test(n)) return 'bi-file-earmark-text text-secondary';
  return 'bi-file-earmark text-muted';
}
const _TKOT_ADJ_MAX = { foto:15*1024*1024, pdf:30*1024*1024, video:100*1024*1024, audio:30*1024*1024, documento:25*1024*1024, otro:25*1024*1024 };
function _tkotAdjTipo(file){
  const m=(file.type||'').toLowerCase(), n=(file.name||'').toLowerCase();
  if(m.startsWith('image/')) return 'foto';
  if(m==='application/pdf'||n.endsWith('.pdf')) return 'pdf';
  if(m.startsWith('video/')) return 'video';
  if(m.startsWith('audio/')) return 'audio';
  if(/\.(docx?|xlsx?|pptx?|txt|csv)$/i.test(n)) return 'documento';
  return 'otro';
}
function o2fAdjFiles(fileList){
  if(!fileList || !fileList.length) return;
  const rechazados = [];
  Array.from(fileList).forEach(function(f){
    const tipo = _tkotAdjTipo(f);
    const max = _TKOT_ADJ_MAX[tipo] || _TKOT_ADJ_MAX.otro;
    if(f.size > max){ rechazados.push(f.name+' ('+_tkotBytesPretty(f.size)+', máx '+_tkotBytesPretty(max)+')'); return; }
    _O2F.adjuntos.push(f);
  });
  const inp = document.getElementById('o2fLevAdjInput');
  if(inp) inp.value = '';
  _tkotRenderAdjList();
  if(rechazados.length) ilusToast('Archivo(s) muy grande(s): '+rechazados[0]+(rechazados.length>1?' y '+(rechazados.length-1)+' más':''), {type:'warning'});
}
function _tkotRenderAdjList(){
  const wrap = document.getElementById('o2fLevAdjList');
  const counter = document.getElementById('o2fLevAdjCount');
  if(!wrap) return;
  const arr = _O2F.adjuntos || [];
  if(counter) counter.textContent = arr.length;
  if(!arr.length){ wrap.innerHTML = ''; o2fRefreshStepStates(); return; }
  wrap.innerHTML = arr.map(function(f, idx){
    return '<div class="lev-adj-item"><div class="lev-adj-thumb"><i class="bi '+_tkotAdjIconClass(f)+'"></i></div>'
      + '<div class="lev-adj-info"><div class="lev-adj-name" title="'+esc(f.name)+'">'+esc(f.name)+'</div>'
      + '<div class="lev-adj-meta">'+_tkotAdjTipo(f).toUpperCase()+' · '+_tkotBytesPretty(f.size)+'</div></div>'
      + '<button type="button" class="lev-adj-rm" onclick="o2fRemoveAdj('+idx+')" title="Quitar"><i class="bi bi-x-lg"></i></button></div>';
  }).join('');
  o2fRefreshStepStates();
}
function o2fRemoveAdj(idx){ _O2F.adjuntos.splice(idx,1); _tkotRenderAdjList(); }
function o2fResetAdjuntos(){ _O2F.adjuntos = []; _tkotRenderAdjList(); }
async function _tkotSubirAdjuntos(vid){
  const arr = _O2F.adjuntos || [];
  if(!arr.length || !vid) return {ok:0, fail:0};
  let ok=0, fail=0;
  for(let i=0;i<arr.length;i++){
    const f = arr[i];
    try{
      ilusToast('Subiendo '+(i+1)+'/'+arr.length+': '+f.name, {type:'info', duration:1500});
      const fd = new FormData();
      fd.append('archivo', f);
      fd.append('tipo', _tkotAdjTipo(f));
      const r = await fetch('/mantenciones/api/visitas/'+vid+'/adjuntos', {method:'POST', body:fd});
      const d = await r.json().catch(function(){ return {}; });
      if(r.ok && d.ok) ok++; else fail++;
    }catch(e){ fail++; }
  }
  return {ok, fail};
}

// ════════════════════════════════════════════════════════════════════
// ESTADO VISUAL DE LOS 7 PASOS (2026-08-12)
// ════════════════════════════════════════════════════════════════════
// Hasta ahora las tarjetas del modal "Generar OT" eran mudas: los 7 pasos
// se veían igual estuvieran llenos o vacíos, así que había que recorrerlos
// uno por uno para saber qué faltaba. Ahora cada tarjeta se pinta verde
// (borde + cabecera + círculo con ✓) apenas cumple su regla, igual que
// PL_STEP_RULES (templates/mantenciones/plantillas.html),
// otiRefreshStepStates() (templates/mantenciones/ots_list.html) y
// RB_STEP_RULES (templates/mantenciones/_repuestos_bodega_pane.html).
//
// Las reglas son un ESPEJO de lo que ya valida o2fGenerar() antes de
// enviar -- no agregan ni quitan requisitos, solo los muestran antes:
//   1 Tipo      · hay tipo; si es levantamiento, además hay modalidad
//   2 Info      · título + dirección + nombre de contacto (los 3 que
//                 o2fGenerar() exige con toast)
//   3 Agenda    · hay fecha programada
//   4 Técnicos  · al menos uno
//   5 Equipos   · al menos un checkbox marcado; EXCEPTO levantamiento por
//                 descubrimiento, donde o2fGenerar() no exige equipos
//                 (los captura el técnico en terreno) -> cuenta completo
//   6 Acceso    · OPCIONAL (no bloquea el envío)
//   7 Documentos· OPCIONAL (no bloquea el envío)
// Los pasos 6 y 7 nacen con .is-optional (punteado gris) desde el HTML;
// si el usuario los llena pasan a verde, si los vacía vuelven a punteado.
// CERO cambio funcional: esto no valida, no bloquea y no toca el submit.
const TKOT_STEP_RULES = {
  1: function(){
    const tipo = (document.getElementById('o2f_otTipo') || {}).value || '';
    if(!tipo) return false;
    // Levantamiento sin modalidad elegida = paso a medias (Daniel 2026-08-06:
    // la modalidad NO viene preseleccionada, manda el desplegable).
    if(tipo === 'levantamiento') return _O2F.modo === 'equipos' || _O2F.modo === 'descubrimiento';
    return true;
  },
  2: function(){
    const val = function(id){ const e = document.getElementById(id); return (e && e.value || '').trim(); };
    return !!(val('o2fLevSelectTitulo') && val('o2fLevDireccion') && val('o2fLevContactoNombre'));
  },
  // 2026-08-13: Paso 3 y 4 se invirtieron en el HTML (Técnicos ahora va
  // primero -- ver comentario en _modal_generar_ot.html). Estas reglas
  // deben seguir la misma numeración que el id del card (o2fStep3/4), no
  // el campo que validaban antes.
  3: function(){ return _O2F.tecnicosSel.size > 0; },
  4: function(){
    const e = document.getElementById('o2fLevFechaProg');
    return !!(e && e.value);
  },
  5: function(){
    const tipo = (document.getElementById('o2f_otTipo') || {}).value || '';
    if(tipo === 'levantamiento' && _O2F.modo === 'descubrimiento') return true;
    // Instalación sin ficha (forzarTodosEquipos) pinta los checkbox como
    // "checked disabled" -> igual entran en este conteo, sin caso especial.
    if(document.querySelectorAll('.lev-eq-chk:checked').length === 0) return false;
    // 2026-08-13 (Daniel: "si no escoge no lo debe dejar avanzar"): el paso
    // no está completo mientras haya un equipo marcado sin plantilla.
    return _tkotEquiposSinPlantilla().length === 0;
  },
  6: function(){
    const val = function(id){ const e = document.getElementById(id); return (e && e.value || '').trim(); };
    return !!(val('o2f_acceso_ascensor') || val('o2f_acceso_estacionamiento') || val('o2f_acceso_piso') || val('o2f_acceso_notas'));
  },
  7: function(){ return (_O2F.adjuntos || []).length > 0; },
};
const TKOT_STEPS_OPCIONALES = { 6: true, 7: true };

function o2fRefreshStepStates(){
  Object.keys(TKOT_STEP_RULES).forEach(function(n){
    const card = document.getElementById('o2fStep' + n);
    if(!card) return;
    let ok = false;
    // Nunca romper el modal por un paso: si una regla falla (un id que
    // todavía no existe, por ejemplo), ese paso queda "no completo".
    try{ ok = !!TKOT_STEP_RULES[n](); }catch(e){ ok = false; }
    card.classList.toggle('is-complete', ok);
    if(TKOT_STEPS_OPCIONALES[n]) card.classList.toggle('is-optional', !ok);
  });
}

// Red de seguridad: además de las llamadas explícitas que hay en
// o2fTipoChange/o2fModoSet/o2fRecalcEqCount/etc., se escucha en
// delegación sobre el modal completo. Así también quedan cubiertos los
// campos que se escriben a mano (título, dirección, piso, notas) y
// cualquier control que se agregue al modal en el futuro sin acordarse de
// llamar a o2fRefreshStepStates(). El setTimeout(0) deja que corran
// primero los onclick/onchange inline del propio HTML.
(function(){
  const _m = document.getElementById('ot2ModalForm');
  if(!_m) return;
  const _tick = function(){ setTimeout(o2fRefreshStepStates, 0); };
  ['input', 'change', 'click'].forEach(function(ev){ _m.addEventListener(ev, _tick, true); });
})();

// ── Si el modal "Generar OT" se cierra con el detalle de un bloque abierto,
//    el overlay (que vive en <body>, fuera de la pila de Bootstrap) quedaría
//    huérfano tapando la pantalla. Se limpia siempre.
document.getElementById('ot2ModalForm').addEventListener('hide.bs.modal', function(){
  o2fdayCerrarDetalle();
});

// ── BUG2 (modo cliente, portado 2026-08-10 desde _levSugerirDiaPreferido()
//    de static/mant_ficha.js -- ver ese archivo para el original): sugiere
//    la fecha según el "día preferido" de mantención del cliente.
//    Consulta el backend (read-only) por el día habitual. Si existe:
//    (a) prellena #levFechaProg -- SOLO si no hay un preset explícito
//        (tipoPreset/fechaPreset de abrirLevantamientoSelector());
//    (b) muestra un hint sutil (ámbar) junto al campo de fecha.
//    Si el cliente no tiene día preferido, o el fetch falla, no muestra
//    nada. Nunca lanza: protege la apertura del modal. ──
function _tkotFechaProgHintEl(){
  let el = document.getElementById('o2fLevFechaProgHint');
  if (el) return el;
  const input = document.getElementById('o2fLevFechaProg');
  if (!input) return null;
  el = document.createElement('div');
  el.id = 'o2fLevFechaProgHint';
  el.className = 'small mt-1';
  el.style.cssText = 'display:none;font-size:.72rem;line-height:1.25;color:#b45309';
  (input.parentNode || input).appendChild(el);
  return el;
}
async function _tkotSugerirDiaPreferido(fechaPresetExplicita){
  const hintEl = _tkotFechaProgHintEl();
  if (hintEl){ hintEl.style.display = 'none'; hintEl.innerHTML = ''; }
  try{
    const r = await fetch('/mantenciones/api/clientes/' + CID + '/dia-preferido');
    if (!r.ok) return;
    const d = await r.json().catch(function(){ return null; });
    if (!d || !d.ok) return;
    const diaPref = d.dia_mantencion_pref;
    const sugerida = d.sugerida;
    if (diaPref == null || !sugerida || !/^\d{4}-\d{2}-\d{2}$/.test(sugerida)) return;

    const input = document.getElementById('o2fLevFechaProg');
    if (input && !fechaPresetExplicita){
      input.value = sugerida;
      if (typeof o2fFechaProgChange === 'function') o2fFechaProgChange();
    }

    const p = sugerida.split('-');
    const ddmm = p[2] + '/' + p[1];
    if (hintEl){
      hintEl.innerHTML = '<i class="bi bi-calendar-heart me-1"></i>'
        + 'Día preferido de este cliente: el <strong>' + esc(String(diaPref)) + '</strong> de cada mes'
        + ' — sugerimos el <strong>' + esc(ddmm) + '</strong>.';
      hintEl.style.display = '';
    }
  }catch(e){ /* falla en silencio -- nunca rompe la apertura del modal */ }
}

// ── Abrir el modal: reset + resolución de cliente + carga de todo ──
document.getElementById('ot2ModalForm').addEventListener('show.bs.modal', async function(){
  const tbody = document.getElementById('o2fLevSelectTbody');
  tbody.innerHTML = '<tr><td colspan="3" class="text-muted small text-center py-3">Cargando…</td></tr>';
  (document.getElementById('o2fAvisoSinFicha')||{}).style.display = 'none';

  // Modo cliente: los equipos salen del DOM de la ficha (ya renderizado),
  // no de un ticket. Se lee ANTES de o2fRenderEquipos()/o2fAplicarForzadoInstalacion()
  // más abajo, que son los que efectivamente pintan el Paso 5.
  if (_TKOT_MODO_CLIENTE) equiposCache = _tkotLeerEquiposDesdeDOM();

  await _tkotResolverCliente();
  await _tkotCargarPlantillas();
  await _tkotCargarContactos();
  _tkotRenderContactosSelector();
  if(_O2F.contactos.lista.length > 0){
    const sel = document.getElementById('o2fLevContactoSel');
    sel.value = '0';
    o2fContactoChange();
  } else {
    // Sin ficha o sin contactos registrados -> manual, prellenado con lo
    // que ya declaró el ticket (nombre_contacto/phone/email). En modo
    // cliente ticketActual es null -> queda en blanco para que el usuario
    // lo llene (mismo comportamiento que tenía el modal viejo
    // #modalLevSelector, que tampoco prellenaba estos 3 campos).
    const t = ticketActual || {};
    (document.getElementById('o2fLevContactoSel')||{}).value = '__manual';
    o2fContactoChange();
    (document.getElementById('o2fLevContactoNombre')||{}).value = t.nombre_contacto || '';
    (document.getElementById('o2fLevContactoTel')||{}).value = t.phone || '';
    (document.getElementById('o2fLevContactoEmail')||{}).value = t.email || '';
  }

  // Tipo de OT: en modo ticket siempre 'levantamiento'. En modo cliente,
  // respeta el preset que dejó abrirLevantamientoSelector() (mant_ficha.js)
  // -- ej. "Programar mantención" pide 'preventiva' -- igual que hacía el
  // modal viejo #modalLevSelector.
  const tipoSel = document.getElementById('o2f_otTipo');
  if(tipoSel){
    const _tipoWanted = (_TKOT_MODO_CLIENTE && _O2F.pendingTipoPreset) || 'levantamiento';
    const _tipoHas = Array.from(tipoSel.options).some(function(o){ return o.value === _tipoWanted; });
    tipoSel.value = _tipoHas ? _tipoWanted : 'levantamiento';
  }
  // Sin preselección de modalidad: o2fTipoChange() la deja limpia y el
  // usuario elige (Daniel 2026-08-06). Antes aquí se marcaba 'equipos'.
  o2fTipoChange();

  // 2026-07-19 (Daniel): la OT hereda el contexto del ticket -- si Notas
  // está vacío, precargar con la descripción del ticket (no se pisa si ya
  // hay algo tipeado, ej. reabrir el modal tras editar a mano). En modo
  // cliente ticketActual es null -> queda en '' (mismo default que el
  // modal viejo).
  const levSelectNotasEl = document.getElementById('o2fLevSelectNotas');
  if(!levSelectNotasEl.value.trim()){
    levSelectNotasEl.value = (ticketActual || {}).descripcion || '';
  }

  // Dirección: en modo ticket, default = la del ticket (jobsite real). En
  // modo cliente, default = la del cliente (mismo comportamiento que tenía
  // abrirLevantamientoSelector() en el modal viejo) -- editable de todas
  // formas, Google Maps valida al elegir una sugerencia.
  const dirInput = document.getElementById('o2fLevDireccion');
  if (_TKOT_MODO_CLIENTE){
    const _dirCliente = DATA.cliente_direccion || '';
    const _comunaCliente = DATA.cliente_comuna || '';
    let _dirCompleta = _dirCliente;
    if (_comunaCliente && !_dirCompleta.toLowerCase().includes(_comunaCliente.toLowerCase())){
      _dirCompleta = (_dirCompleta ? _dirCompleta + ', ' : '') + _comunaCliente;
    }
    dirInput.value = _dirCompleta || '';
  } else {
    const t0 = ticketActual || {};
    dirInput.value = t0.direccion || '';
    if(t0.direccion_lat) dirInput.dataset.lat = t0.direccion_lat;
    if(t0.direccion_lng) dirInput.dataset.lng = t0.direccion_lng;
    if(t0.direccion_place_id) dirInput.dataset.placeId = t0.direccion_place_id;
  }
  if(!dirInput.dataset.placesInit && typeof ilusPlacesAutocomplete === 'function'){
    ilusPlacesAutocomplete(dirInput, {
      country: 'cl', types: ['address'],
      onPlaceSelected: function(place){
        const hint = document.getElementById('o2fLevDireccionHint');
        if(hint) hint.innerHTML = '<i class="bi bi-check-circle-fill text-success me-1"></i>Dirección verificada por Google Maps · <small>'+place.lat.toFixed(4)+', '+place.lng.toFixed(4)+'</small>';
        dirInput.dataset.lat = place.lat;
        dirInput.dataset.lng = place.lng;
        dirInput.dataset.placeId = place.place_id || '';
      },
    });
    dirInput.dataset.placesInit = '1';
  }

  const hoy = new Date();
  const _hoyStr = hoy.getFullYear()+'-'+String(hoy.getMonth()+1).padStart(2,'0')+'-'+String(hoy.getDate()).padStart(2,'0');
  // Fecha: en modo cliente respeta el preset (ej. la agenda del Plan Anual
  // pide una fecha concreta), igual que hacía el modal viejo -- en modo
  // ticket siempre es hoy (sin cambios de comportamiento).
  const _fechaPreset = (_TKOT_MODO_CLIENTE && _O2F.pendingFechaPreset && /^\d{4}-\d{2}-\d{2}$/.test(_O2F.pendingFechaPreset))
    ? _O2F.pendingFechaPreset : null;
  const _fechaDef = _fechaPreset || _hoyStr;
  (document.getElementById('o2fLevFechaProg')||{}).value = _fechaDef;
  (document.getElementById('o2fLevFechaFin')||{}).value = '';
  (document.getElementById('o2fLevRangoDias')||{}).checked = false;
  (document.getElementById('o2fLevFechaFinWrap')||{}).style.display = 'none';
  (document.getElementById('o2fLevHoraIni')||{}).value = '09:00';
  (document.getElementById('o2fLevHoraFin')||{}).value = '13:00';
  // Chips de duración (2026-07-19, P9): el reset original NO limpiaba las
  // horas del "último día" del rango -- quedaban con el valor de la OT
  // anterior. Aditivo, no quita nada (REGLA #4.2).
  (document.getElementById('o2fLevHoraIniFin')||{}).value = '09:00';
  (document.getElementById('o2fLevHoraFinFin')||{}).value = '13:00';
  _O2F.durN = 1;
  if (typeof levChipsRefresh === 'function') levChipsRefresh();
  // Presets consumidos -- se limpian para no "pegarse" en la próxima
  // apertura del modal (ej. abrir por el botón normal después de haber
  // venido de la agenda del Plan Anual).
  _O2F.pendingTipoPreset = null;
  _O2F.pendingFechaPreset = null;

  o2fCalInit();

  // BUG2 (portado del modal viejo, 2026-06-23): sugerir la fecha según el
  // "día preferido" de mantención del cliente -- solo aplica en modo
  // cliente (un ticket no tiene "día preferido"). Falla en silencio.
  if (_TKOT_MODO_CLIENTE) _tkotSugerirDiaPreferido(!!_fechaPreset);

  _O2F.tecnicosSel.clear();
  const tBtn = document.getElementById('o2f_btnLevToggleTodos');
  if(tBtn) tBtn.innerHTML = '<i class="bi bi-check2-square me-1"></i>Marcar todos';

  o2fResetAccesoLogistica();
  o2fResetAdjuntos();

  o2fRenderEquipos();
  o2fAplicarForzadoInstalacion();
  o2fRenderTecnicos();

  try{
    const r = await fetch('/mantenciones/api/tecnicos');
    const d = await r.json();
    _O2F.tecnicosDisponibles = Array.isArray(d) ? d : (d.tecnicos || []);
    o2fRenderTecnicos();
  }catch(e){
    (document.getElementById('o2fLevTecnicosBox')||{}).innerHTML = '<span class="text-danger small">⚠ No se pudieron cargar los técnicos</span>';
  }

  // Estado inicial de los 7 pasos (2026-08-12): se calcula al final, con
  // todo ya prellenado (tipo, dirección, contacto, fecha, equipos).
  o2fRefreshStepStates();

  // Referencia de la cotización de origen (selector de /mantenciones/ots,
  // 2026-08-12) — puramente informativa, no toca equipo_ids/payload.
  // No se espera (no debe demorar la apertura del modal).
  _tkotMostrarRefCotizacion();
});

// ── Referencia de solo lectura: si esta OT nace de una cotización
//    (?cotizacion_id= en la URL, puesto por el selector de origen de
//    /mantenciones/ots), muestra sus ítems para que quien arma el Paso 5
//    sepa qué equipos marcar -- NO los auto-selecciona (los SKU de una
//    cotización no siempre calzan 1:1 con un mant_maquinas.id real del
//    cliente, y una asociación automática equivocada es peor que pedirle
//    a la persona que elija a mano con el contexto a la vista). Falla en
//    silencio: es un adorno informativo, nunca debe bloquear el modal. ──
async function _tkotMostrarRefCotizacion(){
  const box = document.getElementById('o2f_otCotizacionRef');
  if (!box) return;
  box.style.display = 'none';
  box.innerHTML = '';
  let cotId = null;
  try{ cotId = new URLSearchParams(window.location.search).get('cotizacion_id'); }
  catch(e){ return; }
  if (!cotId) return;
  try{
    const r = await fetch('/tickets/api/cotizaciones/' + encodeURIComponent(cotId));
    const d = await r.json();
    if (!d || !d.ok || !d.cotizacion) return;
    const items = (d.items || []).slice(0, 12);
    const lista = items.map(function(it){
      return '<li>' + (it.qty || 1) + '× ' + esc(it.nombre || it.sku || 'Ítem') + '</li>';
    }).join('');
    box.style.cssText = 'background:#eff6ff;color:#1e3a8a;border:1px solid #93c5fd;border-radius:8px;padding:8px 10px';
    box.innerHTML = '<i class="bi bi-file-earmark-text me-1"></i>'
      + 'Referencia — cotización <strong>' + esc(d.cotizacion.numero_cotizacion || ('#' + cotId)) + '</strong>'
      + (lista ? ':<ul class="mb-0 mt-1" style="padding-left:1.1rem">' + lista + '</ul>' : ' (sin ítems).')
      + '<div class="mt-1" style="opacity:.85">Elige en el Paso 5 los equipos que correspondan a estos productos.</div>';
    box.style.display = '';
  }catch(e){ /* referencia opcional -- nunca bloquea el modal */ }
}

// ── Envío final ──
async function o2fGenerar(){
  const tipoSel = document.getElementById('o2f_otTipo')?.value || 'levantamiento';
  const eqs = equiposCache || [];
  const keysMarcados = _O2F.forzarTodosEquipos
    ? eqs.map(function(e){ return _tkotEqKey(e); })
    : Array.from(document.querySelectorAll('.lev-eq-chk:checked')).map(function(c){ return c.dataset.key; });

  let esDescubrimiento = (tipoSel === 'levantamiento' && _O2F.modo === 'descubrimiento');
  if(!keysMarcados.length && !esDescubrimiento){
    if(tipoSel !== 'levantamiento'){ ilusToast('Selecciona al menos un equipo', {type:'warning'}); return; }
    const okDesc = await ilusConfirm({
      title: 'Levantamiento de descubrimiento',
      message: '¿Crear la OT sin equipos preseleccionados?',
      sub: 'El técnico capturará los equipos en terreno (foto + nombre + N° serie). Al cerrar la OT quedarán creados en la ficha del cliente.',
      okLabel: 'Sí, descubrir en terreno', cancelLabel: 'Volver a elegir',
    });
    if(!okDesc) return;
    esDescubrimiento = true;
    o2fModoSet('descubrimiento');
  }

  // 2026-08-13 (Daniel: "no quiero nada automático, todo lo debe escoger
  // el usuario y si no escoge no lo debe dejar avanzar"). El backend
  // rechaza igual (_ot_validar_plantillas_elegidas), pero acá se avisa
  // ANTES de mandar y diciendo QUÉ equipo falta, para no hacer perder el
  // viaje ni obligar a interpretar un error del servidor.
  const _sinPlantilla = _tkotEquiposSinPlantilla();
  if(_sinPlantilla.length){
    const _lista = _sinPlantilla.slice(0, 6).map(function(e){ return '• ' + esc(e.nombre); }).join('<br>');
    const _resto = _sinPlantilla.length > 6 ? '<br>…y ' + (_sinPlantilla.length - 6) + ' más' : '';
    await ilusAlert({
      title: 'Falta elegir la plantilla',
      message: 'Cada equipo necesita su plantilla de checklist antes de crear la OT.',
      sub: 'Sin plantilla en:<br>' + _lista + _resto
         + '<br><br>Ve al paso <strong>Equipos</strong> y usa el botón rojo <strong>Elegir plantilla</strong> de cada uno.',
      subHtml: true,
      type: 'warning',
    });
    // Deja al usuario mirando el equipo que falta.
    const _btn = document.getElementById('lev-pl-btn-' + _sinPlantilla[0].key);
    if(_btn) _btn.scrollIntoView({ behavior: 'smooth', block: 'center' });
    return;
  }

  const fechaProg = (document.getElementById('o2fLevFechaProg')||{}).value;
  if(!fechaProg){ ilusToast('Indica la fecha programada', {type:'warning'}); return; }
  const horaIni = (document.getElementById('o2fLevHoraIni')||{}).value || '';
  const horaFin = (document.getElementById('o2fLevHoraFin')||{}).value || '';
  if(horaIni && horaFin && horaIni >= horaFin){ ilusToast('La hora de término debe ser posterior a la de inicio', {type:'warning'}); return; }
  const usaRango = (document.getElementById('o2fLevRangoDias')||{}).checked;
  let fechaFin = '';
  if(usaRango){
    fechaFin = (document.getElementById('o2fLevFechaFin')||{}).value;
    if(!fechaFin){ ilusToast('Indica la fecha de término', {type:'warning'}); return; }
    if(fechaFin < fechaProg){ ilusToast('La fecha de término no puede ser anterior a la de inicio', {type:'warning'}); return; }
  }

  const dirVal = (document.getElementById('o2fLevDireccion')?.value || '').trim();
  if(!dirVal){ ilusToast('Indica la dirección de la visita', {type:'warning'}); document.getElementById('o2fLevDireccion')?.focus(); return; }
  const contactoNombre = (document.getElementById('o2fLevContactoNombre')?.value || '').trim();
  if(!contactoNombre){ ilusToast('Indica el contacto que recibirá al técnico en sitio', {type:'warning'}); document.getElementById('o2fLevContactoSel')?.focus(); return; }

  const tecnicoIds = Array.from(_O2F.tecnicosSel);
  if(!tecnicoIds.length){
    const hay = (_O2F.tecnicosDisponibles||[]).length > 0;
    if(!hay){
      await ilusAlert({ title:'Sin técnicos disponibles', message:'No es posible crear la OT porque no hay técnicos activos.', type:'warning' });
    } else {
      ilusToast('Asigna al menos un técnico que ejecute la OT', {type:'warning'});
    }
    return;
  }

  // Equipos: separa los que ya tienen maquina_id (ficha real) de los que
  // solo existen en el ticket (sin ficha aún) -- el backend necesita ambos
  // caminos para poder crear la OT de instalación-sin-ficha.
  const equiposPorKey = {};
  eqs.forEach(function(e){ equiposPorKey[_tkotEqKey(e)] = e; });
  const equipoIds = [];       // maquina_id ya existentes en mant_maquinas
  const equiposTicket = [];   // datos crudos de tk_ticket_equipos sin maquina_id
  keysMarcados.forEach(function(key){
    const e = equiposPorKey[key];
    if(!e) return;
    if(e.maquina_id) equipoIds.push(e.maquina_id);
    else equiposTicket.push({ ticket_equipo_id: e.id, nombre: e.nombre, sku: e.sku, serie: e.serie, cantidad: e.cantidad, tipo: e.tipo });
  });
  const plantillasPorEq = {};
  keysMarcados.forEach(function(key){
    if(_O2F.eqPlantillas[key] && _O2F.eqPlantillas[key].size > 0) plantillasPorEq[key] = Array.from(_O2F.eqPlantillas[key]);
  });

  const btn = document.getElementById('btnLevIniciar');
  const btnHTMLOrig = btn.innerHTML;
  btn.disabled = true; btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Creando OT…';
  try{
    // Daniel 2026-07-13: el backend puede responder "requiere_confirmacion"
    // (feriado y/o choque de horario) SIN "ok" ni "error" -- antes esto caia
    // en el generico "Error" a secas, sin dar oportunidad de ver el aviso ni
    // de forzar. Ahora se arma el payload una vez y se reintenta con los
    // flags forzar_feriado/forzar_choque si el usuario confirma.
    const payload = {
      ticket_id: TID,
      cliente_id: _O2F.cid,
      titulo: (document.getElementById('o2fLevSelectTitulo')||{}).value.trim(),
      notas: (document.getElementById('o2fLevSelectNotas')||{}).value.trim(),
      equipo_ids: equipoIds,
      equipos_ticket: equiposTicket,
      descubrimiento: esDescubrimiento,
      fecha_programada: fechaProg,
      hora_inicio: horaIni || null,
      hora_fin: horaFin || null,
      fecha_fin: usaRango ? fechaFin : null,
      hora_inicio_fin: usaRango ? (document.getElementById('o2fLevHoraIniFin')?.value || null) : null,
      hora_fin_fin: usaRango ? (document.getElementById('o2fLevHoraFinFin')?.value || null) : null,
      tecnico_ids: tecnicoIds,
      tipo_ot: tipoSel,
      aplica_garantia: document.getElementById('o2f_otAplicaGarantia')?.checked || false,
      plantillas_por_equipo: plantillasPorEq,
      direccion_visita: dirVal,
      direccion_lat: parseFloat(document.getElementById('o2fLevDireccion')?.dataset.lat) || null,
      direccion_lng: parseFloat(document.getElementById('o2fLevDireccion')?.dataset.lng) || null,
      direccion_place_id: document.getElementById('o2fLevDireccion')?.dataset.placeId || null,
      contacto_nombre: contactoNombre,
      contacto_cargo: (document.getElementById('o2fLevContactoCargo')?.value || '').trim(),
      contacto_tel: (document.getElementById('o2fLevContactoTel')?.value || '').trim(),
      contacto_email: (document.getElementById('o2fLevContactoEmail')?.value || '').trim(),
      contacto_origen: document.getElementById('o2fLevContactoSel')?.dataset.origen || 'manual',
      acceso_ascensor: document.getElementById('o2f_acceso_ascensor')?.value || null,
      acceso_estacionamiento: document.getElementById('o2f_acceso_estacionamiento')?.value || null,
      acceso_piso: (document.getElementById('o2f_acceso_piso')?.value || '').trim(),
      acceso_notas: (document.getElementById('o2f_acceso_notas')?.value || '').trim(),
      forzar_feriado: false,
      forzar_choque: false,
      forzar_crear_cliente: false,
      cliente_id_confirmado: null,
    };
    // FIX 2026-08-11: el selector de plantilla del Paso 1 se retiró (quedaba
    // duplicado con "Plantillas extra" del Paso 5, por equipo). Ya no se
    // manda plantilla_id a nivel de OT -- el backend calcula la estándar
    // con _plantilla_estandar_para_tipo, y las plantillas extra por equipo
    // (equipos_plantillas, más abajo) siguen funcionando igual que siempre.

    // ── OT 2.0: se escribe contra el MOTOR PROPIO ────────────────────
    // El original mandaba a /mantenciones/api/clientes/<CID>/levantamientos
    // o a /tickets/api/tickets/<TID>/generar-ot, que delegan los dos en
    // _mant_lev_crear_ot_core -- justo el nucleo que este modulo tiene que
    // dejar de usar. Aca se traduce el payload al formato de
    // POST /ot/api/crear y se manda ahi.
    //
    // La traduccion vive en UN solo lugar (esta funcion), asi el resto del
    // codigo copiado queda intacto y sigue siendo comparable con el
    // original linea a linea.
    const _o2fEquipos = [];
    (equipoIds || []).forEach(function (mid) {
      // plantillasPorEq viene como { claveEquipo: [ids] }; el motor nuevo
      // pide UNA plantilla por equipo, asi que se toma la primera elegida.
      var elegidas = (plantillasPorEq && (plantillasPorEq[mid] || plantillasPorEq[String(mid)])) || [];
      _o2fEquipos.push({
        maquina_id: parseInt(mid),
        plantilla_id: elegidas.length ? parseInt(elegidas[0]) : null
      });
    });

    const _o2fPayload = {
      tipo_ot: payload.tipo_ot,
      descubrimiento: payload.descubrimiento,
      cliente_id: payload.cliente_id,
      titulo: payload.titulo,
      descripcion: payload.notas,
      fecha_programada: payload.fecha_programada,
      hora_inicio: payload.hora_inicio,
      hora_fin: payload.hora_fin,
      tecnico_user_ids: payload.tecnico_ids || [],
      tecnico_lider_id: (payload.tecnico_ids || [])[0] || null,
      equipos: _o2fEquipos,
      ticket_id: TID || null,
      acceso: {
        ascensor: payload.acceso_ascensor === '' || payload.acceso_ascensor == null
                  ? null : (payload.acceso_ascensor === '1' || payload.acceso_ascensor === 1),
        estacionamiento: payload.acceso_estacionamiento === '' || payload.acceso_estacionamiento == null
                  ? null : (payload.acceso_estacionamiento === '1' || payload.acceso_estacionamiento === 1),
        piso: payload.acceso_piso,
        notas: payload.acceso_notas
      }
    };
    const _tkotUrl = '/ot/api/crear';
    let d;
    // 2026-08-12: hasta 3 intentos -- feriado/choque (paso 1) y cliente_nuevo
    // (paso 2, cuando ambos toques al mismo ticket) son confirmaciones
    // independientes que pueden encadenarse en la misma corrida.
    for(let intento = 0; intento < 3; intento++){
      const r = await fetch(_tkotUrl, {
        method: 'POST', headers: {'Content-Type':'application/json'},
        body: JSON.stringify(_o2fPayload)
      });
      d = await r.json();
      if(d.ok || !d.requiere_confirmacion) break;

      const adv = d.advertencias || {};

      // ── Cliente sin match exacto de RUT: se pregunta antes de crear una
      //    ficha nueva -- primero ofrece el candidato más parecido (mismo
      //    RUT sin dígito verificador, o nombre similar) si lo hay, y solo
      //    si el usuario lo descarta se ofrece crear cliente nuevo. Daniel
      //    2026-08-12: "chequear por el RUT, con y sin el código
      //    verificador, y el nombre... déjalo bien inteligente". ──
      if(adv.cliente_nuevo){
        const cn = adv.cliente_nuevo;
        const candidatos = cn.candidatos || [];
        let usarExistente = false;
        let c0 = null;
        if(candidatos.length){
          c0 = candidatos[0];
          const motivo = c0.match === 'rut_sin_dv' ? 'mismo RUT (sin dígito verificador)' : 'nombre parecido';
          usarExistente = await ilusConfirm({
            title: 'Cliente parecido encontrado',
            message: 'Este ticket no calzó por RUT exacto, pero encontramos:<br>'
              + '<strong>' + esc(c0.razon_social || 'Cliente') + '</strong>'
              + (c0.rut ? ' · RUT ' + esc(c0.rut) : '')
              + '<br><span style="font-size:.85rem;color:#6b7280">Coincide por ' + motivo + '.</span>',
            messageHtml: true,
            sub: '¿Usar este cliente para la OT en vez de crear uno nuevo?',
            okLabel: 'Usar este cliente', cancelLabel: 'No, crear cliente nuevo',
            type: 'question',
          });
        }
        if(usarExistente && c0){
          payload.cliente_id_confirmado = c0.id;
          continue;
        }
        const crear = await ilusConfirm({
          title: 'Crear cliente nuevo',
          message: 'No hay ficha para <strong>' + esc(cn.razon_social_ticket || 'este cliente') + '</strong>'
            + (cn.rut_ticket ? ' (RUT ' + esc(cn.rut_ticket) + ')' : '') + '.',
          messageHtml: true,
          sub: 'Se creará una ficha mínima, editable después desde Mantenciones.',
          okLabel: 'Sí, crear cliente', cancelLabel: 'Cancelar',
          type: 'question',
        });
        if(!crear) return;
        payload.forzar_crear_cliente = true;
        continue;
      }

      const partes = [];
      if(adv.feriado) partes.push('📅 ' + (adv.feriado.nombre || 'Feriado') + ' (' + (adv.feriado.fecha || fechaProg) + ')');
      if(adv.choque){
        const nomTec = adv.choque.tecnico_nombre || 'el técnico';
        const otras = (adv.choque.visitas || []).map(function(v){
          return (v.numero_ot || ('OT #'+v.visita_id)) + ' ' + (v.hora_inicio||'') + '–' + (v.hora_fin||'');
        }).join(', ');
        partes.push('⏰ ' + nomTec + ' ya tiene otra visita ese día' + (otras ? ' (' + otras + ')' : ''));
      }
      const seguir = await ilusConfirm({
        title: 'Antes de continuar',
        message: partes.join('<br>'), subHtml: true,
        sub: '¿Quieres generar la OT de todas formas?',
        okLabel: 'Sí, continuar', cancelLabel: 'Volver a revisar',
        type: 'warning',
      });
      if(!seguir) return;
      if(adv.feriado) payload.forzar_feriado = true;
      if(adv.choque) payload.forzar_choque = true;
    }
    if(!d.ok){
      // 2026-08-12 (Daniel: "esto está malo... que no lea, que sepa... el
      // plan me mete los ojos"): probó a propósito el caso de equipos sin
      // ficha y el toast de una sola línea con el texto crudo del backend
      // no le sirvió. El backend YA manda el detalle estructurado
      // (equipos_excluidos: [{nombre, sku, motivo}]) -- antes se descartaba
      // y solo se mostraba el conteo dentro de una oración. Ahora, para ESE
      // caso puntual, se arma una tarjeta por producto (mismo lenguaje
      // visual de las tarjetas de equipo del propio modal: ícono, nombre,
      // SKU) en vez de una frase para leer completa. Cualquier OTRO error
      // sigue con el toast de siempre -- no se toca ese camino.
      if(d.error_codigo === 'EQUIPO_SIN_FICHA' && Array.isArray(d.equipos_excluidos) && d.equipos_excluidos.length){
        await _tkotAlertEquiposSinFicha(d.equipos_excluidos);
      } else {
        ilusToast(d.error || 'No se pudo generar la OT', {type:'error'});
      }
      return;
    }

    const visitaId = d.visita_id;
    const modalInst = bootstrap.Modal.getInstance(document.getElementById('ot2ModalForm'));
    if(modalInst){
      await new Promise(function(resolve){
        let resolved = false;
        const done = function(){ if(!resolved){ resolved = true; resolve(); } };
        document.getElementById('ot2ModalForm').addEventListener('hidden.bs.modal', done, {once:true});
        modalInst.hide();
        setTimeout(done, 600);
      });
    }
    if(typeof ilusCleanModalBackdrops === 'function') ilusCleanModalBackdrops();

    let adjResult = null;
    if(_O2F.adjuntos.length > 0 && visitaId) adjResult = await _tkotSubirAdjuntos(visitaId);

    const otHtml = d.ot_url ? '<a href="'+d.ot_url+'" class="fw-bold text-decoration-underline" style="color:#dc2626">'+esc(d.numero_ot)+'</a>' : '';
    let subMsg = 'La OT está disponible para que el/los técnico(s) la gestionen desde su módulo de Órdenes de Trabajo. '
      + (d.items_plantilla_aplicados||0) + ' tarea(s) generadas por las plantillas aplicadas.';
    if(adjResult){
      subMsg += adjResult.fail === 0
        ? ' ' + adjResult.ok + ' archivo(s) preliminar(es) adjunto(s).'
        : ' ' + adjResult.ok + '/' + (adjResult.ok+adjResult.fail) + ' archivo(s) preliminares subidos.';
    }
    await ilusAlert({
      title: '✅ Orden de Trabajo creada',
      message: 'Se generó la OT ' + otHtml + ' con ' + (d.n_items||keysMarcados.length) + ' equipo(s) y ' + (d.tecnicos_asignados||tecnicoIds.length) + ' técnico(s) asignado(s).',
      sub: subMsg, messageHtml: true, type: 'success', okLabel: 'Entendido',
    });
    o2fResetAccesoLogistica();
    o2fResetAdjuntos();
    // Modo ticket: recarga la ficha del ticket (ahora con visita_id).
    // Modo cliente: el admin permanece en la ficha -- la OT ya quedó
    // creada, mismo comportamiento que tenía el modal viejo
    // #modalLevSelector ("El admin permanece en la ficha del cliente").
    if (TID !== null) cargar();
  }catch(e){
    ilusToast('Error de red: ' + e.message, {type:'error'});
  }finally{
    btn.disabled = false; btn.innerHTML = btnHTMLOrig;
  }
}

// 2026-08-12 (Daniel: "que no lea, que sepa" -- el equipo sin ficha tiene
// que verse, no leerse). Tarjeta por producto excluido (mismo lenguaje
// visual que las tarjetas de equipo del modal: ícono + nombre + SKU en
// mono) en vez de una frase de una sola línea con el conteo. Los dos
// caminos posibles (ficha o levantamiento) quedan como 2 líneas cortas con
// ícono, no una oración para leer completa.
async function _tkotAlertEquiposSinFicha(lista){
  const tarjetas = lista.map(function(eq){
    return '<div style="display:flex;align-items:center;gap:10px;padding:8px 10px;'
      + 'background:#fef2f2;border:1px solid #fecaca;border-radius:8px;margin-bottom:6px;text-align:left">'
      + '<i class="bi bi-box-seam" style="color:#dc2626;font-size:1.1rem;flex-shrink:0"></i>'
      + '<div style="min-width:0"><div style="font-weight:700;color:#0f172a;overflow-wrap:anywhere">' + esc(eq.nombre || 'Producto') + '</div>'
      + (eq.sku ? '<div style="font-size:.72rem;color:#991b1b;font-family:monospace">SKU ' + esc(eq.sku) + '</div>' : '')
      + '</div></div>';
  }).join('');
  const n = lista.length;
  const html = '<div style="margin:6px 0 12px">' + tarjetas + '</div>'
    + '<div style="text-align:left;font-size:.85rem;line-height:1.7">'
    + '<div>📋 <strong>Regístralo primero</strong> en la ficha del cliente, o</div>'
    + '<div>🔍 <strong>Genera un Levantamiento por descubrimiento</strong> para conocerlo en terreno</div>'
    + '</div>';
  await ilusAlert({
    title: (n === 1 ? '1 equipo no se pudo agregar' : n + ' equipos no se pudieron agregar'),
    message: html, messageHtml: true,
    sub: 'Fuera de un levantamiento, ILUS no crea tareas para equipos sin ficha registrada.',
    type: 'danger', okLabel: 'Entendido',
  });
}

// ── Tarjeta de estado en Acciones (llamada desde cargar()) ──
// 2026-07-15: el acceso a "Generar OT" ahora se pinta en DOS lugares -- la
// barra de la tarjeta "Equipo(s)" (el original, Regla #4.2: se queda) y el
// acceso destacado de la columna lateral (Daniel: "no veo el formulario
// dentro del ticket"). La DECISIÓN vive en un solo lugar (esta función):
// si el ticket ya tiene visita_id hay OT y se muestra el número + link; si
// no, se muestra el botón que abre #ot2ModalForm. Los dos lugares reciben
// exactamente el mismo estado, así que no pueden contradecirse. Para sumar
// un tercer acceso en otra parte de servicio técnico basta con agregarlo a
// DESTINOS_OT con sus 4 ids -- no se toca la lógica.
const DESTINOS_OT = [
  // Barra de la tarjeta "Equipo(s)" (columna principal)
  {sinOT:'otwSinOT',     conOT:'otwConOT',     num:'otwNumeroOtTxt',     link:'otwVerOtLink'},
  // Tarjeta "Orden de trabajo" (columna lateral)
  {sinOT:'otSideSinOT',  conOT:'otSideConOT',  num:'otSideNumeroOtTxt',  link:'otSideVerOtLink'},
];
function renderAccionesOT(t){
  const hayOT = !!t.visita_id;
  const numeroOT = hayOT ? (t.visita_numero_ot || ('OT #'+t.visita_id)) : '';
  const hrefOT = hayOT ? ('/mantenciones/ot/'+t.visita_id) : '#';
  DESTINOS_OT.forEach(function(d){
    const sinOT = document.getElementById(d.sinOT);
    const conOT = document.getElementById(d.conOT);
    if(!sinOT || !conOT) return;   // destino no presente en este render
    sinOT.style.display = hayOT ? 'none' : 'block';
    conOT.style.display = hayOT ? 'block' : 'none';
    if(hayOT){
      const num  = document.getElementById(d.num);
      const link = document.getElementById(d.link);
      if(num)  num.textContent = numeroOT;
      if(link) link.href = hrefOT;
    }
  });
}

// ══════════════ Cotización — generar DESDE este ticket ══════════════
// 2026-07-15 (Daniel: "necesito que... esta se pueda generar DENTRO del
// ticket"). 2026-07-23 (rediseño estilo "Generar OT" -- PR-5 "todo
// conectado"): el botón YA NO abre el modal ERP crudo -- ahora navega al
// wizard completo de /tickets/cotizaciones (4 secciones del Paso 1,
// contacto, plan/contrato vigente, botonera de Bodega 02, etc.) prellenado
// con los datos y equipos de ESTE ticket vía ?desde_ticket=<TID>
// (_cotWizAplicarDeepLinkTicket en cotizaciones.html). La vía "documento
// ERP" sigue disponible DENTRO del wizard (Sección 1, "Desde documento
// ERP") -- no se elimina ninguna capacidad, solo cambia dónde se hace
// (Regla #4.2). Se abre en pestaña nueva para no perder el estado de esta
// ficha (la tarjeta "Cotización" se actualiza sola al volver/recargar).
// 2026-08-10: tarjeta lateral "Cotización" -- exclusiva del ticket.
if (document.getElementById('btnGenerarCotizacionSide'))
document.getElementById('btnGenerarCotizacionSide').addEventListener('click', function(){
  window.open('/tickets/cotizaciones?desde_ticket=' + TID, '_blank');
});

// Tarjeta lateral "Cotización" -- pinta número/estado/total de cada
// cotización ya asociada a este ticket. A diferencia de "Orden de trabajo"
// (1 ticket -> máx. 1 OT), acá puede haber varias -- el botón de generar
// siempre queda visible, arriba solo se agrega la lista de existentes.
const COT_ESTADO_LABEL = {draft:'Borrador', sent:'Enviada', approved:'Aprobada', rejected:'Rechazada', expired:'Expirada'};
const COT_ESTADO_BG = {
  draft:'background:#f3f4f6;color:#6b7280', sent:'background:#dbeafe;color:#1e40af',
  approved:'background:#dcfce7;color:#166534', rejected:'background:#fee2e2;color:#991b1b',
  expired:'background:#fef3c7;color:#92400e',
};
function renderCotizaciones(cots){
  const vacio = document.getElementById('cotSideVacio');
  const lista = document.getElementById('cotSideLista');
  if (!vacio || !lista) return;
  if (!cots || !cots.length){
    vacio.style.display = 'block';
    lista.innerHTML = '';
    return;
  }
  vacio.style.display = 'none';
  lista.innerHTML = cots.map(function(c){
    const bg = COT_ESTADO_BG[c.estado] || COT_ESTADO_BG.draft;
    const total = '$' + Number(c.total || 0).toLocaleString('es-CL');
    return '<div style="display:flex;align-items:center;justify-content:space-between;gap:8px;'
      + 'padding:9px 11px;border-radius:9px;background:#f9fafb;border:1px solid #eef0f3;margin-bottom:8px;">'
      + '<div style="min-width:0;">'
      + '<div style="font-weight:800;font-size:.82rem;color:#0a0a0a;overflow-wrap:anywhere;">' + esc(c.numero_cotizacion || ('#' + c.id)) + '</div>'
      + '<div style="font-size:.72rem;color:#6b7280;">' + esc(total) + '</div>'
      + '</div>'
      + '<span style="padding:3px 9px;border-radius:50px;font-size:.62rem;font-weight:800;text-transform:uppercase;white-space:nowrap;' + bg + ';">'
      + esc(COT_ESTADO_LABEL[c.estado] || c.estado) + '</span>'
      + '</div>';
  }).join('') + '<a href="/tickets/cotizaciones" target="_blank" rel="noopener" '
    + 'style="font-size:.74rem;font-weight:700;color:#6b7280;display:inline-block;margin-top:2px;">'
    + 'Ver en Cotizaciones <i class="bi bi-box-arrow-up-right"></i></a>';
}

// 2026-08-10: en modo cliente (TID=null) no hay ticket que cargar --
// cargar() solo pintaría "No se pudo cargar el ticket" sobre elementos
// del hero/composer que ni siquiera existen en esta página. El modal
// #ot2ModalForm no depende de ninguna de las dos llamadas (los técnicos
// del Paso 3 salen de GET /mantenciones/api/tecnicos, disparado por su
// propio listener show.bs.modal).
if (TID !== null){
  cargarEjecutivos();
  cargar();
}

// ── Auto-abrir "Generar OT" al llegar con ?abrir_generar_ot=1 (2026-08-12)
//    El selector de origen de /mantenciones/ots (origen "Ticket", o
//    "Cotización" cuando la cotización ya tiene ticket_id) NO reimplementa
//    el modal de 7 pasos -- resuelve el ticket y NAVEGA a esta ficha con el
//    query param; acá lo abrimos solos, con el MISMO trigger nativo que
//    usan los botones de siempre (data-bs-toggle="modal"). Gate TID!==null
//    a propósito: en modo cliente este mismo archivo también se carga
//    (mantenciones/ficha.html, TID=null) y el query param ya lo maneja
//    abrirGenerarOT() desde static/mant_ficha.js -- sin este gate, el
//    modal se abriría DOS veces en esa página. ──
if (TID !== null){
  try{
    const _qsOt = new URLSearchParams(window.location.search);
    if (_qsOt.get('abrir_generar_ot') === '1'){
      _qsOt.delete('abrir_generar_ot');
      const _restoOt = _qsOt.toString();
      history.replaceState(null, '', window.location.pathname + (_restoOt ? '?' + _restoOt : ''));
      // Pequeño margen (mismo criterio que el resto de este cambio, ver
      // otogElegir() en ots_list.html) para que cargar() -- disparado
      // arriba, SIN await, a propósito para no bloquear el resto de la
      // ficha -- alcance a resolver ticketActual antes de que el listener
      // show.bs.modal intente prellenar dirección/contacto/equipos desde
      // ahí. No es una garantía dura (best-effort): si la red va lenta
      // igual abre con los campos en blanco, editables a mano.
      setTimeout(function(){
        const _mOt = document.getElementById('ot2ModalForm');
        if (_mOt) new bootstrap.Modal(_mOt).show();
      }, 400);
    }
  }catch(e){ console.warn('auto-abrir generar OT (modo ticket):', e); }
}

// ══════════════ Auto-refresco silencioso (Daniel 2026-07-12: "necesito que
// sea inmediata la velocidad") ══════════════
// Cada vez que se carga esta ficha, el backend ya dispara un chequeo del
// buzon de correo (ver tk_api_get). Pero si la respuesta del cliente entra
// DESPUES de que la pagina ya cargo, antes había que recargar a mano para
// verla. Este refresco silencioso (sin loader, sin mover el scroll del
// hilo si el usuario esta leyendo) reintenta cada 6s mientras la pestana
// siga abierta -- se detiene solo si la pestana pasa a segundo plano.
// Daniel 2026-07-12: "menos de diez segundos" -- 6s cliente + 8s de
// throttle del autopoll server (tickets_module.py) da un peor caso
// realista de ~14s y un caso tipico bastante por debajo de 10s.
let _tkAutoRefreshTimer = null;
function _tkAutoRefreshSeguro(){
  // No interrumpir si el usuario esta escribiendo algo (campo inline del
  // cliente, "Declarar equipo", el composer de Respuestas) o con un modal
  // Bootstrap abierto -- recargar por debajo le borraria lo que esta
  // tecleando. Se reintenta en el proximo ciclo, no se pierde el refresco.
  const ae = document.activeElement;
  const editando = ae && (ae.tagName === 'INPUT' || ae.tagName === 'TEXTAREA' || ae.tagName === 'SELECT');
  const enEditorQuill = ae && ae.closest && ae.closest('.ql-editor');
  const modalAbierto = document.querySelector('.modal.show');
  return !(editando || enEditorQuill || modalAbierto);
}
function _tkProgramarAutoRefresh(){
  clearTimeout(_tkAutoRefreshTimer);
  _tkAutoRefreshTimer = setTimeout(async function(){
    if (!document.hidden && _tkAutoRefreshSeguro()) { try { await cargar(); } catch(e) {} }
    _tkProgramarAutoRefresh();
  }, 6000);
}
// 2026-08-10: sin ticket que refrescar en modo cliente.
if (TID !== null) _tkProgramarAutoRefresh();


/* ── API publica del modulo ───────────────────────────────────────────── */
window.O2F = {
  /* Abre el formulario ya con su contexto resuelto.
       O2F.iniciar({origen:'ticket', ticket_id:70, cliente:{id:26,nombre:'...'}})
     Lo que no venga queda en null y el formulario lo pide. */
  iniciar: function (opts) {
    opts = opts || {};
    TID = opts.ticket_id || null;
    CID = (opts.cliente && opts.cliente.id) || null;
    // Sin ticket => modo cliente. Se recalcula en CADA apertura porque el
    // mismo modal sirve para los dos casos (a diferencia del original, que
    // lo fijaba una vez segun la pagina).
    _TKOT_MODO_CLIENTE = (TID === null);
    _O2F.cid = CID;
    _O2F.clienteResuelto = !!CID;
    _O2F.origen = opts.origen || 'cliente';
    try {
      if (typeof o2fInit === 'function') return o2fInit();
      if (typeof o2fAbrir === 'function') return o2fAbrir();
    } catch (e) {
      console.error('[O2F] iniciar:', e);
    }
  },
  estado: function () { return _O2F; }
};

})();

// Datos inyectados por Jinja vía el bootstrap inline window.TK_LIST_DATA
// (ver templates/tickets/list.html, script pequeño antes de este archivo).
const ESTADO_LABEL = window.TK_LIST_DATA.ESTADO_LABEL;
const TIPO_LABEL = window.TK_LIST_DATA.TIPO_LABEL;
// Mismo mapeo tipo→(icono, color, fondo) que usan los tipo-pills del modal
// "Nuevo Ticket" (variable Jinja `tipo_visual` definida arriba, tarjeta 1) --
// se reutiliza aca para la columna Tipo de la tabla (NO se inventa paleta nueva).
const TIPO_VISUAL = window.TK_LIST_DATA.TIPO_VISUAL;
const IS_SUPERADMIN = window.TK_LIST_DATA.IS_SUPERADMIN;
// Tipos 100% internos (sin cliente asociado, ej. control de calidad interno,
// trabajo de bodega, capacitación) -- para estos, el RUT/cliente/dirección
// NO son obligatorios en el modal "Nuevo Ticket" (validación condicional,
// ver ntActualizarObligatoriedadCliente / validarNtTodo más abajo).
const TK_TIPOS_SIN_CLIENTE = ['control_calidad', 'trabajo_bodega', 'capacitacion'];
let _tkTimer = null;

// ── Estado de la bandeja (orden / paginacion / toggle Hoy) ──
// Default SIEMPRE "Actualizado" desc (Daniel, 2026-08-13: "que la tabla
// de tickets se filtre SIEMPRE por el actualizado"). El viejo orden
// "inteligente" (no-leidos primero, etc. -- ver _TK_ORDER_DEFAULT en
// tickets_module.py) sigue vivo en el backend con sort='', pero ya no es
// el default ni queda expuesto por la UI (ver tkSortClick / btnLimpiar).
let tkSort = 'updated_at'; // columna activa (whitelist backend); default 'updated_at'
let tkDir = 'desc';       // 'desc' | 'asc'
let tkPage = 1;
let tkLimit = 50;         // default 50 (selector 10/25/50/100)
let tkHoy = false;        // toggle boton "Hoy" (manda hoy=1)
let _tkCache = {};        // id → ticket de la ultima carga (acciones por fila)

// El servidor ya envía las fechas formateadas en hora Chile (Regla #6, "%d/%m/%Y %H:%M").
function fmtFecha(s){ return s || ''; }
function fmtFechaPartes(s){
  if(!s) return {f:'—', h:''};
  const i = s.indexOf(' ');
  return i < 0 ? {f:s, h:''} : {f:s.slice(0,i), h:s.slice(i+1)};
}
// Escapa TAMBIÉN comillas: esc() se usa en contexto de atributo (href/onclick),
// no solo texto — sin escapar comillas habría riesgo de XSS almacenado.
function esc(s){ return (s==null?'':String(s))
  .replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')
  .replace(/"/g,'&quot;').replace(/'/g,'&#39;'); }
function debounce(fn,ms){ let t; return function(){ clearTimeout(t); const a=arguments,c=this; t=setTimeout(()=>fn.apply(c,a),ms); }; }

// ── Filtros actuales → URLSearchParams. COMPARTIDO por el listado y los dos
// reportes CSV (asi el CSV descarga EXACTAMENTE lo que se esta viendo).
// conOrden=false para el reporte SLA (no acepta sort/dir).
function tkParamsFiltros(conOrden){
  const p = new URLSearchParams();
  const tk = document.getElementById('fTicket').value.trim(); if(tk) p.set('ticket', tk);
  const q = document.getElementById('fQ').value.trim(); if(q) p.set('q', q);
  const rut = document.getElementById('fRut').value.trim(); if(rut) p.set('rut', rut);
  const e = document.getElementById('fEstado').value; if(e) p.set('estado', e);
  const t = document.getElementById('fTipo').value; if(t) p.set('tipo', t);
  const pr = document.getElementById('fPrio').value; if(pr) p.set('prioridad', pr);
  const resp = document.getElementById('fResp').value; if(resp) p.set('asignado_a', resp);
  if(tkHoy){ p.set('hoy','1'); }
  else{
    const fd = document.getElementById('fDesde').value; if(fd) p.set('fecha_desde', fd);
    const fh = document.getElementById('fHasta').value; if(fh) p.set('fecha_hasta', fh);
  }
  if(conOrden && tkSort){ p.set('sort', tkSort); p.set('dir', tkDir); }
  return p;
}

// ── Encabezados: [label, columna sort del backend o '' = no ordenable] ──
const TK_COLS = [
  ['Ticket','numero_ticket'], ['Cliente','empresa'], ['Tipo','tipo'], ['Estado','estado'],
  ['Avance',''], ['Asignado','asignado_a'], ['Origen','origen'],
  ['Creado','created_at'], ['Actualizado','updated_at'], ['Acciones',''],
];
function tkTheadHtml(){
  return '<tr>' + TK_COLS.map(function(c){
    if(!c[1]) return '<th>'+c[0]+'</th>';
    let cls = 'tk-th-sort', ic = 'bi-arrow-down-up';
    if(tkSort === c[1]){ cls += ' on'; ic = (tkDir === 'asc' ? 'bi-sort-up' : 'bi-sort-down'); }
    return '<th class="'+cls+'" onclick="tkSortClick(\''+c[1]+'\')" title="Ordenar por '+c[0]+'">'
      + c[0] + ' <i class="bi '+ic+'"></i></th>';
  }).join('') + '</tr>';
}
// Ciclo por clic (Daniel, 2026-08-13): "Actualizado" es el default y NUNCA
// cae al orden inteligente -- en su propia columna el ciclo es desc → asc →
// desc. En el resto de columnas el usuario ordena libre (desc → asc), y el
// 3er clic vuelve a "Actualizado" desc en vez de caer al orden inteligente
// (sort='' sigue existiendo en el backend, ver _TK_ORDER_DEFAULT, pero ya
// no queda expuesto por la UI -- Regla #4.2: no se borro, solo dejo de ser
// alcanzable por click).
window.tkSortClick = function(col){
  if(col === 'updated_at'){
    tkDir = (tkSort === 'updated_at' && tkDir === 'desc') ? 'asc' : 'desc';
    tkSort = 'updated_at';
  }
  else if(tkSort !== col){ tkSort = col; tkDir = 'desc'; }
  else if(tkDir === 'desc'){ tkDir = 'asc'; }
  else { tkSort = 'updated_at'; tkDir = 'desc'; }
  tkPage = 1; cargarTickets();
};

// ── Footer: "Mostrando X - Y de Z" + tamano de pagina + prev/next ──
function tkFooterHtml(data){
  const total = data.total||0, page = data.page||tkPage, pages = data.pages||0,
        limit = data.limit||tkLimit;
  const desde = total ? ((page-1)*limit+1) : 0;
  const hasta = Math.min(page*limit, total);
  return '<div class="tk-foot">'
    + '<div class="tk-foot-info">Mostrando <b>'+desde+' - '+hasta+'</b> de <b>'+total+'</b> registro'+(total===1?'':'s')+'</div>'
    + '<div class="tk-foot-ctrl">'
    +   '<label class="lbl-pp">Por página '
    +     '<select onchange="tkCambiarLimit(this.value)">'
    +       [10,25,50,100].map(n=>'<option value="'+n+'"'+(n===tkLimit?' selected':'')+'>'+n+'</option>').join('')
    +     '</select></label>'
    +   '<button type="button" class="tk-pg-btn" '+(page<=1?'disabled':'')+' onclick="tkPagina(-1)" title="Página anterior"><i class="bi bi-chevron-left"></i></button>'
    +   '<span class="tk-pg-num">'+page+' / '+Math.max(pages,1)+'</span>'
    +   '<button type="button" class="tk-pg-btn" '+(page>=pages?'disabled':'')+' onclick="tkPagina(1)" title="Página siguiente"><i class="bi bi-chevron-right"></i></button>'
    + '</div></div>';
}
window.tkPagina = function(d){ tkPage = Math.max(1, tkPage + d); cargarTickets(); };
window.tkCambiarLimit = function(v){ tkLimit = parseInt(v,10)||50; tkPage = 1; cargarTickets(); };

async function cargarTickets(){
  const p = tkParamsFiltros(true);
  p.set('page', String(tkPage));
  p.set('limit', String(tkLimit));
  let data;
  try{
    const r = await fetch('/tickets/api/tickets?'+p.toString());
    data = await r.json();
  }catch(err){ document.getElementById('tkLista').innerHTML =
    '<div class="empty-state"><i class="bi bi-wifi-off"></i>No se pudo cargar. Reintenta.</div>'; return; }
  if(!data.ok){ ilusToast(data.error||'Error al cargar','error'); return; }

  // Si la pagina quedo fuera de rango (ej: se borro el ultimo ticket de la
  // ultima pagina), retroceder a la ultima valida y recargar una vez.
  if(!data.tickets.length && (data.total||0) > 0 && tkPage > 1){
    tkPage = Math.max(1, data.pages||1);
    return cargarTickets();
  }

  document.getElementById('kpiTotal').textContent = data.kpis.total;
  document.getElementById('kpiActivos').textContent = data.kpis.activos;
  document.getElementById('kpiUrgentes').textContent = data.kpis.urgentes;
  document.getElementById('kpiVencidos').textContent = data.kpis.vencidos;

  _tkCache = {};
  data.tickets.forEach(function(t){ _tkCache[t.id] = t; });

  const cont = document.getElementById('tkLista');
  if(!data.tickets.length){
    cont.innerHTML = '<div class="empty-state"><i class="bi bi-inbox"></i>No hay tickets con estos filtros.</div>';
    return;
  }
  // "table-responsive" (Bootstrap) hereda el fade-hint de scroll horizontal
  // ya definido en mobile.css (.table-responsive:not(.scroll-hint-disabled)),
  // sin efecto en escritorio (Bootstrap solo agrega overflow-x:auto, ya
  // presente en .tk-table-wrap).
  cont.innerHTML = '<div class="tk-table-wrap table-responsive"><table class="tk-table"><thead>'
    + tkTheadHtml()
    + '</thead><tbody>'
    + data.tickets.map(tkFilaHtml).join('')
    + '</tbody></table></div>'
    + tkFooterHtml(data);

  tkActualizarUnread();
}

// ── Aviso de mensajes nuevos (Daniel 2026-07-12: "la bandeja te indica que
// tienes mensajes nuevos"): banner arriba de la tabla + toast cuando el
// conteo SUBE respecto al ultimo chequeo (nuevo mensaje mientras la bandeja
// esta abierta). Usa /unread-summary (cuenta mensajes de cliente sin leer
// en tickets activos), independiente de la pagina/filtro actual. ──
let _tkUnreadPrev = null;
async function tkActualizarUnread(){
  let d;
  try{
    const r = await fetch('/tickets/api/unread-summary');
    d = await r.json();
  }catch(e){ return; }
  if(!d || !d.ok) return;
  const n = d.unread || 0;
  const banner = document.getElementById('tkUnreadBanner');
  if(banner){
    if(n > 0){
      banner.style.display = '';
      banner.innerHTML = '<i class="bi bi-envelope-exclamation-fill"></i> Tienes <strong>'+n+'</strong> mensaje'
        +(n>1?'s':'')+' nuevo'+(n>1?'s':'')+' de cliente'+(n>1?'s':'')+' sin leer.';
    } else {
      banner.style.display = 'none';
    }
  }
  if(_tkUnreadPrev !== null && n > _tkUnreadPrev){
    ilusToast('📩 Tienes nuevos mensajes de cliente', {type:'info'});
  }
  _tkUnreadPrev = n;
}

// ── Columna Tipo: icono circular de color + label (paleta TIPO_VISUAL, misma del modal) ──
function tkTipoCelda(tk){
  if(!tk.tipo) return '<span class="text-muted" style="font-size:.78rem;">—</span>';
  const tv = TIPO_VISUAL[tk.tipo] || ['bi-question-circle','#6b7280','#f9fafb'];
  return '<div class="tkc-tipo"><span class="ic" style="background:'+tv[2]+';color:'+tv[1]+';"><i class="bi '+tv[0]+'"></i></span>'
    + '<span class="lbl">'+esc(TIPO_LABEL[tk.tipo]||tk.tipo)+'</span></div>';
}

// ── Columna Origen: form→WEB, backoffice→Interno, cualquier otro tal cual en mayúsculas ──
function tkOrigenCelda(origen){
  if(origen==='form') return '<span class="tk-origen-pill web"><i class="bi bi-globe"></i>WEB</span>';
  if(origen==='backoffice') return '<span class="tk-origen-pill backoffice"><i class="bi bi-building"></i>Interno</span>';
  return '<span class="tk-origen-pill otro">'+esc((origen||'—').toString().toUpperCase())+'</span>';
}

// ── Columna Avance: mini-stepper de 4 puntos (Abierto/En Curso/Resuelto/Cerrado),
// mismo indice que tkStepIndexParaEstado en ficha.html. 'pending'/'cancelado' no
// consumen su propio nodo -- se superponen como icono/color especial (nt pedido). ──
const TK_MINI_EN_CURSO = ['in_progress','ot_pending_approval','ot_generated','ot_in_progress'];
function tkMiniIndice(estado){
  if(estado==='open') return 0;
  if(TK_MINI_EN_CURSO.includes(estado)) return 1;
  if(estado==='resolved') return 2;
  if(estado==='closed') return 3;
  return 0; // pending/cancelado/desconocido: no rompen el stepper de 4 nodos
}
function tkMiniStepper(tk){
  const idx = tkMiniIndice(tk.estado);
  let dots = '';
  for(let i=0;i<4;i++){
    if(i>0) dots += '<span class="tk-mini-line'+(i<=idx?' done':'')+'"></span>';
    let cls = 'tk-mini-dot';
    if(i<idx) cls += ' done'; else if(i===idx) cls += ' current';
    dots += '<span class="'+cls+'"></span>';
  }
  let extra = '';
  if(tk.estado==='pending') extra = '<span class="tk-mini-extra pending" title="En espera"><i class="bi bi-hourglass-split"></i></span>';
  else if(tk.estado==='cancelado') extra = '<span class="tk-mini-extra cancel" title="Cancelado"><i class="bi bi-x-circle-fill"></i></span>';
  return '<div class="tk-mini-wrap"><div class="tk-mini-stepper">'+dots+'</div>'+extra+'</div>';
}

// ── Columna Acciones: Ver (todos) · Cerrar / Eliminar (solo superadmin).
// stopPropagation en cada icono para NO disparar la navegacion del click-en-fila.
// Solo se pasa el id numerico; el resto del ticket sale de _tkCache (evita
// meter strings del usuario dentro de atributos onclick).
function tkAccionesCelda(tk){
  let h = '<button type="button" class="tk-act ver" title="Ver ticket"'
    + ' onclick="event.stopPropagation();location.href=\'/tickets/'+tk.id+'\'"><i class="bi bi-eye"></i></button>';
  if(IS_SUPERADMIN && tk.estado !== 'closed' && tk.estado !== 'cancelado'){
    h += '<button type="button" class="tk-act cerrar" title="Cerrar ticket"'
      + ' onclick="tkAccionCerrar(event,'+tk.id+')"><i class="bi bi-check-circle"></i></button>';
  }
  if(IS_SUPERADMIN){
    h += '<button type="button" class="tk-act del" title="Eliminar ticket (superadmin)"'
      + ' onclick="tkAccionEliminar(event,'+tk.id+')"><i class="bi bi-trash"></i></button>';
  }
  return '<div class="tk-acciones">'+h+'</div>';
}

window.tkAccionCerrar = async function(ev, id){
  ev.stopPropagation();
  const tk = _tkCache[id] || {};
  const num = tk.numero_ticket || ('#'+id);
  const ok = await ilusConfirm({
    title: 'Cerrar ticket',
    message: '¿Marcar el ticket '+num+' como cerrado?',
    sub: 'El ticket pasará al estado Cerrado y saldrá de la bandeja de activos.',
    okLabel: 'Cerrar ticket', cancelLabel: 'Cancelar',
    type: 'question',
  });
  if(!ok) return;
  try{
    const r = await fetch('/tickets/api/tickets/'+id, {method:'PATCH',
      headers:{'Content-Type':'application/json'}, body:JSON.stringify({estado:'closed'})});
    const d = await r.json();
    if(!d.ok){ ilusToast(d.error||'No se pudo cerrar el ticket','error'); return; }
    ilusToast('✓ '+num+' cerrado','success');
    cargarTickets();
  }catch(e){ ilusToast('Error de red al cerrar el ticket','error'); }
};

window.tkAccionEliminar = async function(ev, id){
  ev.stopPropagation();
  const tk = _tkCache[id] || {};
  // El backend exige confirm == numero_ticket (o el id como string si no hay numero).
  const numConfirm = (tk.numero_ticket && String(tk.numero_ticket).trim()) || String(id);
  const ok = await ilusConfirm({
    title: 'Eliminar ticket',
    message: '¿Eliminar permanentemente el ticket '+numConfirm+'?',
    sub: 'Esta acción no se puede deshacer.',
    okLabel: 'Eliminar', cancelLabel: 'Cancelar',
    danger: true,
  });
  if(!ok) return;
  try{
    const r = await fetch('/tickets/api/tickets/'+id+'?confirm='+encodeURIComponent(numConfirm),
      {method:'DELETE'});
    const d = await r.json();
    if(!d.ok){ ilusToast(d.error||'No se pudo eliminar el ticket','error'); return; }
    ilusToast('✓ Ticket '+numConfirm+' eliminado','success');
    cargarTickets();
  }catch(e){ ilusToast('Error de red al eliminar el ticket','error'); }
};

function tkFilaHtml(tk){
  const cliente = tk.empresa || tk.nombre_contacto || tk.rut || 'Sin cliente';
  const titulo = tk.titulo || '';
  const unread = tk.unread_count > 0
    ? '<span class="tk-unread">'+tk.unread_count+' nuevo'+(tk.unread_count>1?'s':'')+'</span>' : '';
  const garBadge = tk.es_garantia ? '<span class="tk-gar-badge"><i class="bi bi-shield-check"></i> Garantía</span>' : '';
  const asignado = (tk.asignado_a && String(tk.asignado_a).trim())
    ? '<span class="tkc-asignado">'+esc(tk.asignado_a)+'</span>'
    : '<span class="tkc-asignado none">Sin asignar</span>';
  const fpCreado = fmtFechaPartes(fmtFecha(tk.created_at));
  const fpActualizado = fmtFechaPartes(fmtFecha(tk.updated_at));
  return '<tr class="prio-'+(tk.prioridad||'media')+'" onclick="location.href=\'/tickets/'+tk.id+'\'">'
    + '<td><div class="tkc-num"><span class="tk-num">'+esc(tk.numero_ticket||('#'+tk.id))+'</span>'+garBadge+unread+'</div></td>'
    + '<td><div class="tkc-cliente">'+esc(cliente)+'</div>'+(titulo?'<div class="tkc-titulo">'+esc(titulo)+'</div>':'')+'</td>'
    + '<td>'+tkTipoCelda(tk)+'</td>'
    + '<td><span class="tk-badge bs-'+tk.estado+'">'+esc(ESTADO_LABEL[tk.estado]||tk.estado)+'</span></td>'
    + '<td>'+tkMiniStepper(tk)+'</td>'
    + '<td>'+asignado+'</td>'
    + '<td>'+tkOrigenCelda(tk.origen)+'</td>'
    + '<td><div class="tkc-fecha">'+esc(fpCreado.f)+(fpCreado.h?'<span class="hora">'+esc(fpCreado.h)+'</span>':'')+'</div></td>'
    + '<td><div class="tkc-fecha">'+esc(fpActualizado.f)+(fpActualizado.h?'<span class="hora">'+esc(fpActualizado.h)+'</span>':'')+'</div></td>'
    + '<td>'+tkAccionesCelda(tk)+'</td>'
    + '</tr>';
}

// Cualquier cambio de filtro resetea a la pagina 1 (el resultado cambia).
function tkFiltroCambio(){ tkPage = 1; cargarTickets(); }
function debounced(){ clearTimeout(_tkTimer); _tkTimer = setTimeout(tkFiltroCambio, 350); }
['fTicket','fQ','fRut'].forEach(function(id){ document.getElementById(id).addEventListener('input', debounced); });
['fEstado','fTipo','fPrio','fResp','fDesde','fHasta'].forEach(function(id){
  document.getElementById(id).addEventListener('change', tkFiltroCambio);
});

// ── Toolbar ──
const btnHoy = document.getElementById('btnHoy');
btnHoy.addEventListener('click', function(){
  tkHoy = !tkHoy;
  btnHoy.classList.toggle('on', tkHoy);
  tkFiltroCambio();
});
document.getElementById('btnRefrescar').addEventListener('click', function(){ cargarTickets(); });
// Los reportes CSV descargan con los MISMOS filtros del listado actual.
document.getElementById('btnRepTickets').addEventListener('click', function(){
  window.location = '/tickets/api/reporte/tickets.csv?'+tkParamsFiltros(true).toString();
});
document.getElementById('btnRepSla').addEventListener('click', function(){
  window.location = '/tickets/api/reporte/sla.csv?'+tkParamsFiltros(false).toString();
});
document.getElementById('btnLimpiar').addEventListener('click', function(){
  ['fTicket','fQ','fRut','fDesde','fHasta','fEstado','fTipo','fPrio','fResp']
    .forEach(function(id){ document.getElementById(id).value=''; });
  tkHoy = false; btnHoy.classList.remove('on');
  tkSort = 'updated_at'; tkDir = 'desc'; tkPage = 1; // limpiar vuelve al default, no al orden inteligente
  cargarTickets();
});

// ── Select Responsable: usuarios activos (mismo endpoint que la ficha) ──
async function tkCargarResponsables(){
  try{
    const r = await fetch('/mantenciones/api/ejecutivos');
    const rows = await r.json();
    if(!Array.isArray(rows) || !rows.length) return;
    const sel = document.getElementById('fResp');
    const actual = sel.value;
    sel.innerHTML = '<option value="">Todos</option>'
      + '<option value="__sin_asignar__">Sin asignar</option>'
      + rows.map(function(e){ return '<option value="'+esc(e.nombre)+'">'+esc(e.nombre)+'</option>'; }).join('');
    sel.value = actual; // conserva la seleccion si ya habia una
  }catch(e){ /* sin red: quedan Todos / Sin asignar */ }
}
tkCargarResponsables();
cargarTickets();

// ══════════════ Auto-refresco silencioso de la bandeja (Daniel 2026-07-12:
// "necesito que sea inmediata la velocidad, menos de diez segundos") -- cada
// 10s, se salta si el usuario esta escribiendo en un filtro o con un modal
// abierto, para no interrumpir. ══════════════
let _tkListaAutoTimer = null;
function _tkListaAutoSeguro(){
  const ae = document.activeElement;
  const escribiendo = ae && (ae.tagName === 'INPUT' || ae.tagName === 'TEXTAREA' || ae.tagName === 'SELECT');
  return !(escribiendo || document.querySelector('.modal.show'));
}
function _tkProgramarAutoRefrescoLista(){
  clearTimeout(_tkListaAutoTimer);
  _tkListaAutoTimer = setTimeout(async function(){
    if (!document.hidden && _tkListaAutoSeguro()) { try { await cargarTickets(); } catch(e) {} }
    _tkProgramarAutoRefrescoLista();
  }, 10000);
}
_tkProgramarAutoRefrescoLista();

// ══════════════ MODAL Nuevo Ticket — logica ══════════════
// TODO este bloque va dentro de DOMContentLoaded a proposito: bootstrap.bundle
// (base.html) e ilus_ui.js cargan con <script defer>, que se ejecuta DESPUES
// de parsear el HTML pero ANTES de DOMContentLoaded. Historicamente este
// bloque vivia en un <script> inline SIN defer (corria en cuanto el parser
// lo encontraba, es decir ANTES que los defer). `new bootstrap.Modal(...)`
// a nivel superior del script (fuera de un listener) lanzaba "bootstrap is
// not defined" y mataba TODO el resto de este bloque (busqueda de cliente,
// validadores telefono/correo, Google Places, busqueda de equipo jamas se
// registraban -- exactamente lo que Daniel reporto: "no hace nada"). Mismo
// patron ya usado en comunicaciones/index.html (_modalTplPreview = new
// bootstrap.Modal(...) DENTRO de DOMContentLoaded). Este archivo (extraido
// a static/tickets_list.js el 2026-07-30) ahora carga con <script defer>
// tambien, pero el registro sigue DENTRO de DOMContentLoaded a proposito:
// mismo orden relativo, cero riesgo de regresion.
document.addEventListener('DOMContentLoaded', function(){
let ntTipoSel = '';
let ntEsGarantia = 0;      // garantia SEPARADA del tipo (aplica a cualquiera)
let ntCliente = null;      // {empresa, rut}
const ntEquipos = [];      // [{sku, nombre}]
let ntArchivos = [];       // [{file, estado:'pendiente'|'subiendo'|'ok'|'error', nombre}] -- adjuntos del nuevo ticket
const NT_MAX_ADJ = 15;     // mismo tope que el formulario publico /soporte
const NT_MAX_MB = 25;      // idem MAX_ADJUNTO_MB en tickets_module.py
const ntModalEl = document.getElementById('ntModal');
const ntModal = new bootstrap.Modal(ntModalEl);

// ── Validación visual (obligatorios) ──
const NT_REQ_WRAPS = ['fTipoWrap','fRutWrap','fEmpresaWrap','fContactoWrap','fPhoneWrap','fEmailWrap','fDirWrap','fDescWrap'];
function marcarNtOk(id){
  const el = document.getElementById(id); if(!el) return;
  el.classList.remove('err'); el.classList.add('ok');
  const em = el.querySelector('.nt-err-msg'); if(em) em.classList.remove('show');
}
function marcarNtErr(id){
  const el = document.getElementById(id); if(!el) return;
  el.classList.remove('ok'); el.classList.add('err');
  const em = el.querySelector('.nt-err-msg'); if(em) em.classList.add('show');
}
function limpiarNtErr(id){
  const el = document.getElementById(id); if(!el) return;
  el.classList.remove('err','ok');
  const em = el.querySelector('.nt-err-msg'); if(em) em.classList.remove('show');
}

function ntReset(){
  ntTipoSel = ''; ntCliente = null; ntEquipos.length = 0;
  ntEsGarantia = 0;
  document.querySelectorAll('.nt-gar-pill').forEach(x=>x.classList.toggle('sel', x.dataset.gar==='0'));
  document.querySelectorAll('.nt-tipo-pill').forEach(x=>x.classList.remove('sel'));
  document.getElementById('ntBuscarCli').value='';
  document.querySelector('#ntBuscarCliWrap .nt-search').classList.remove('has-tag');
  document.getElementById('ntRut').value=''; document.getElementById('ntEmpresa').value='';
  document.getElementById('ntContacto').value=''; document.getElementById('ntPhone').value='';
  document.getElementById('ntEmail').value=''; document.getElementById('ntSucursal').value='';
  document.getElementById('ntDireccion').value=''; document.getElementById('ntDirLat').value='';
  document.getElementById('ntDirLng').value=''; document.getElementById('ntDirPlace').value='';
  document.getElementById('ntComuna').value=''; document.getElementById('ntRegion').value='';
  document.getElementById('ntDirHint').innerHTML =
    '<i class="bi bi-info-circle me-1"></i>Selecciona una sugerencia para validar la dirección.';
  document.getElementById('ntBuscarEq').value='';
  document.getElementById('ntDesc').value=''; document.getElementById('ntDescCount').textContent='0';
  document.getElementById('ntDoc').value=''; document.getElementById('ntPrio').value='media';
  document.getElementById('ntVPhone').className='nt-vfeed'; document.getElementById('ntVEmail').className='nt-vfeed';
  NT_REQ_WRAPS.forEach(limpiarNtErr);
  ntActualizarObligatoriedadCliente();
  renderNtEquipos();
  ntArchivos = []; document.getElementById('ntFile').value=''; ntRenderArchivos();
}
ntModalEl.addEventListener('hidden.bs.modal', ntReset);

// ── Adjuntos: elegir/soltar/listar/quitar (subida real ocurre tras crear el ticket) ──
function ntIconArchivo(nombreOMime){
  const s = (nombreOMime||'').toLowerCase();
  if (/\.(jpg|jpeg|png|webp|gif|heic|bmp)$/.test(s) || s.startsWith('image')) return 'bi-file-image-fill';
  if (/\.(mp4|mov|webm|avi|mkv)$/.test(s) || s.startsWith('video')) return 'bi-file-play-fill';
  if (/\.pdf$/.test(s)) return 'bi-file-earmark-pdf-fill';
  if (/\.(doc|docx)$/.test(s)) return 'bi-file-earmark-word-fill';
  if (/\.(xls|xlsx)$/.test(s)) return 'bi-file-earmark-excel-fill';
  return 'bi-file-earmark-fill';
}
function ntFmtSize(bytes){
  if (bytes >= 1024*1024) return (bytes/(1024*1024)).toFixed(1)+' MB';
  return Math.max(1, Math.round(bytes/1024))+' KB';
}
const ntDrop = document.getElementById('ntDrop');
const ntFileInput = document.getElementById('ntFile');
ntDrop.addEventListener('click', ()=>ntFileInput.click());
['dragover','dragleave','drop'].forEach(ev=>ntDrop.addEventListener(ev, function(e){
  e.preventDefault(); e.stopPropagation();
  if (ev==='dragover') ntDrop.classList.add('drag'); else ntDrop.classList.remove('drag');
  if (ev==='drop') ntAgregarArchivos(e.dataTransfer.files);
}));
ntFileInput.addEventListener('change', function(e){ ntAgregarArchivos(e.target.files); this.value=''; });

function ntAgregarArchivos(fileList){
  const restantes = NT_MAX_ADJ - ntArchivos.length;
  if (restantes <= 0){ ilusToast('Máximo '+NT_MAX_ADJ+' archivos por ticket','warning'); return; }
  Array.from(fileList).slice(0, restantes).forEach(function(file){
    if (file.size > NT_MAX_MB*1024*1024){ ilusToast(file.name+' supera '+NT_MAX_MB+' MB','error'); return; }
    ntArchivos.push({file:file, estado:'pendiente', nombre:file.name});
  });
  if (fileList.length > restantes) ilusToast('Solo se agregaron '+restantes+' archivo(s), máximo '+NT_MAX_ADJ,'warning');
  ntRenderArchivos();
}
function ntRenderArchivos(){
  document.getElementById('ntFiles').innerHTML = ntArchivos.map(function(a,i){
    const icon = a.estado==='ok' ? 'bi-check-circle-fill' : a.estado==='error' ? 'bi-exclamation-circle-fill'
      : a.estado==='subiendo' ? 'bi-arrow-repeat' : ntIconArchivo(a.nombre);
    const cls = a.estado==='ok' ? 'up-ok' : a.estado==='error' ? 'up-err' : a.estado==='subiendo' ? 'up-going' : '';
    const quitar = a.estado==='pendiente' || a.estado==='error'
      ? '<span class="x" onclick="ntQuitarArchivo('+i+')">&times;</span>' : '';
    return '<span class="nt-file-chip '+cls+'"><i class="bi '+icon+'"></i><span class="nm">'+esc(a.nombre)+'</span>'
      + '<span class="sz">'+ntFmtSize(a.file.size)+'</span>' + quitar + '</span>';
  }).join('');
}
window.ntQuitarArchivo = function(i){ ntArchivos.splice(i,1); ntRenderArchivos(); };

async function ntSubirArchivos(ticketId){
  const pendientes = ntArchivos.filter(a=>a.estado==='pendiente' || a.estado==='error');
  let ok = 0, fallidos = 0;
  for (let idx=0; idx<pendientes.length; idx++){
    const a = pendientes[idx];
    a.estado = 'subiendo'; ntRenderArchivos();
    ilusLoader.show({text:'Subiendo '+(idx+1)+' de '+pendientes.length+' archivo(s)…', sub:a.nombre,
      progress: Math.round((idx/pendientes.length)*100)});
    const fd = new FormData(); fd.append('file', a.file);
    try{
      const r = await fetch('/tickets/api/tickets/'+ticketId+'/adjuntos', {method:'POST', body:fd});
      const d = await r.json();
      a.estado = d.ok ? 'ok' : 'error';
      if (d.ok) ok++; else fallidos++;
    }catch(e){ a.estado = 'error'; fallidos++; }
    ntRenderArchivos();
  }
  ilusLoader.hide();
  if (pendientes.length){
    if (fallidos===0) ilusToast('✓ '+ok+' adjunto(s) subido(s)','success');
    else if (ok===0) ilusToast('No se pudo subir ningún adjunto ('+fallidos+' fallido(s))','error');
    else ilusToast(ok+' adjunto(s) subido(s), '+fallidos+' fallido(s)','warning');
  }
}

// ── Cliente/RUT/dirección opcionales para tipos 100% internos (Regla #4.2:
// no se elimina la posibilidad de asociar cliente -- los campos siguen
// visibles y editables, solo dejan de exigirse). NT_CLIENTE_WRAPS son los
// wraps de NT_REQ_WRAPS que dependen de si hay cliente asociado (tipo y
// descripción quedan SIEMPRE obligatorios, fuera de esta lista). ──
const NT_CLIENTE_WRAPS = ['fRutWrap','fEmpresaWrap','fContactoWrap','fPhoneWrap','fEmailWrap','fDirWrap'];
function ntActualizarObligatoriedadCliente(){
  const esInterno = TK_TIPOS_SIN_CLIENTE.includes(ntTipoSel);
  NT_CLIENTE_WRAPS.forEach(function(id){
    const el = document.getElementById(id);
    if(!el) return;
    el.classList.toggle('req', !esInterno);
    if(esInterno){
      el.classList.remove('err','ok');
      const em = el.querySelector('.nt-err-msg'); if(em) em.classList.remove('show');
    }
  });
  const dirAst = document.getElementById('ntDirAsterisco');
  if(dirAst) dirAst.style.display = esInterno ? 'none' : '';
  const hintCli = document.getElementById('ntClienteOpcionalHint');
  if(hintCli) hintCli.style.display = esInterno ? 'block' : 'none';
  const hintDir = document.getElementById('ntDireccionOpcionalHint');
  if(hintDir) hintDir.style.display = esInterno ? 'block' : 'none';
}

function ntSeleccionarTipo(el){
  document.querySelectorAll('.nt-tipo-pill').forEach(x=>x.classList.remove('sel'));
  el.classList.add('sel'); ntTipoSel = el.dataset.tipo;
  marcarNtOk('fTipoWrap');
  ntActualizarObligatoriedadCliente();
}
document.querySelectorAll('.nt-tipo-pill').forEach(el=>{
  el.addEventListener('click', ()=>ntSeleccionarTipo(el));
  el.addEventListener('keydown', function(e){
    if(e.key==='Enter' || e.key===' '){ e.preventDefault(); ntSeleccionarTipo(el); }
  });
});

// ── Toggle garantía (separado del tipo) ──
function ntSeleccionarGar(el){
  document.querySelectorAll('.nt-gar-pill').forEach(x=>x.classList.remove('sel'));
  el.classList.add('sel'); ntEsGarantia = el.dataset.gar === '1' ? 1 : 0;
}
document.querySelectorAll('.nt-gar-pill').forEach(el=>{
  el.addEventListener('click', ()=>ntSeleccionarGar(el));
  el.addEventListener('keydown', function(e){
    if(e.key==='Enter' || e.key===' '){ e.preventDefault(); ntSeleccionarGar(el); }
  });
});

// ── Autocomplete cliente (ERP) ──
const ntAcCli = document.getElementById('ntAcCli');
const ntBuscarCliente = debounce(async function(){
  const q = document.getElementById('ntBuscarCli').value.trim();
  if(q.length < 2){ ntAcCli.classList.remove('open'); return; }
  ntAcCli.innerHTML = '<div class="nt-ac-empty"><i class="bi bi-hourglass-split"></i>Buscando en el ERP…</div>'; ntAcCli.classList.add('open');
  try{
    const r = await fetch('/tickets/api/erp/buscar-cliente?q='+encodeURIComponent(q));
    const d = await r.json();
    const rows = d.resultados || [];
    if(!d.ok && d.error){ ntAcCli.innerHTML = '<div class="nt-ac-empty err"><i class="bi bi-exclamation-triangle"></i>'+esc(d.error)+'</div>'; return; }
    if(!rows.length){ ntAcCli.innerHTML = '<div class="nt-ac-empty"><i class="bi bi-search"></i>Sin resultados en el ERP</div>'; return; }
    // 2026-07-12 (Daniel): "hay RUT con varias direcciones -- necesito
    // identificar cual sucursal es" -- se muestra direccion+comuna debajo
    // del nombre para distinguir sucursales del mismo RUT (ej. varios
    // GoFit), y un badge si ese RUT YA tiene ficha en el modulo Clientes
    // (conexion cercana, para no crear un cliente duplicado sin darse cuenta).
    ntAcCli.innerHTML = rows.map((c,i)=>{
      const dirTxt = [c.direccion, c.comuna].filter(Boolean).join(', ');
      const yaCliente = c.ya_es_cliente
        ? '<span class="nt-ac-badge-cli"><i class="bi bi-patch-check-fill"></i> Ya es cliente</span>' : '';
      return '<div class="nt-ac-item" data-i="'+i+'">'
        + '<div>'+esc(c.empresa)+' '+yaCliente+'</div>'
        + '<div class="rut">'+esc(c.rut||'—')+(dirTxt?' · '+esc(dirTxt):'')+'</div></div>';
    }).join('');
    ntAcCli.querySelectorAll('.nt-ac-item').forEach(it=>it.addEventListener('click',()=>ntSelCliente(rows[+it.dataset.i])));
  }catch(e){ ntAcCli.innerHTML = '<div class="nt-ac-empty err"><i class="bi bi-wifi-off"></i>ERP no disponible</div>'; }
}, 320);
document.getElementById('ntBuscarCli').addEventListener('input', ntBuscarCliente);

function ntSelCliente(c){
  ntCliente = c;
  ntAcCli.classList.remove('open');
  document.getElementById('ntBuscarCli').value = c.empresa || c.rut || '';
  document.getElementById('ntRut').value = c.rut || '';
  document.getElementById('ntEmpresa').value = c.empresa || '';
  // Precarga direccion/comuna del ERP como punto de partida -- sigue siendo
  // editable/verificable con Google Places (perfil logistico de Daniel).
  if(c.direccion) document.getElementById('ntDireccion').value = c.direccion;
  if(c.comuna) document.getElementById('ntComuna').value = c.comuna;
  limpiarNtErr('fRutWrap'); limpiarNtErr('fEmpresaWrap');
  if((c.rut||'').trim()) marcarNtOk('fRutWrap');
  if((c.empresa||'').trim()) marcarNtOk('fEmpresaWrap');
  document.querySelector('#ntBuscarCliWrap .nt-search').classList.add('has-tag');
  // 2026-07-19 (Daniel): si el RUT ya tiene ficha en Clientes, precarga el
  // contacto conocido -- SOLO en campos vacios (si el usuario ya tipeo algo
  // en Contacto/Telefono/Correo, no se pisa).
  if(c.ya_es_cliente){
    const ntContactoEl = document.getElementById('ntContacto');
    const ntPhoneEl = document.getElementById('ntPhone');
    const ntEmailEl = document.getElementById('ntEmail');
    if(c.contacto_nombre && !ntContactoEl.value.trim()){
      ntContactoEl.value = c.contacto_nombre;
      limpiarNtErr('fContactoWrap'); marcarNtOk('fContactoWrap');
    }
    if(c.contacto_tel && !ntPhoneEl.value.trim()){
      ntPhoneEl.value = c.contacto_tel;
      ntPhoneEl.dispatchEvent(new Event('blur'));
    }
    if(c.contacto_email && !ntEmailEl.value.trim()){
      ntEmailEl.value = c.contacto_email;
      ntEmailEl.dispatchEvent(new Event('blur'));
    }
    ilusToast('Este RUT ya tiene ficha en el módulo Clientes' + (c.cliente_id ? ' (#'+c.cliente_id+')' : '') + '.', {type:'info'});
  }
}
// El RUT/Empresa quedan editables tras seleccionar — si el usuario los borra
// a mano, vuelven a marcarse como pendientes al validar (validarNtTodo).
['ntRut','ntEmpresa'].forEach(id=>document.getElementById(id).addEventListener('input', function(){
  document.querySelector('#ntBuscarCliWrap .nt-search').classList.remove('has-tag');
}));

// ── Validación teléfono chileno ──
function ntNormTel(v){ return String(v||'').replace(/[^\d]/g,'').replace(/^56/,''); }
document.getElementById('ntPhone').addEventListener('blur', function(){
  const v = this.value.trim(); const f = document.getElementById('ntVPhone');
  if(!v){ f.className='nt-vfeed'; f.textContent=''; marcarNtErr('fPhoneWrap'); return; }
  const n = ntNormTel(v);
  if(/^9\d{8}$/.test(n)){ f.className='nt-vfeed ok'; f.innerHTML='<i class="bi bi-check-circle-fill"></i> Móvil válido';
    this.value = '+56 9 '+n.slice(1,5)+' '+n.slice(5); marcarNtOk('fPhoneWrap'); }
  else if(/^\d{8,9}$/.test(n)){ f.className='nt-vfeed ok'; f.innerHTML='<i class="bi bi-check-circle-fill"></i> Número válido'; marcarNtOk('fPhoneWrap'); }
  else { f.className='nt-vfeed err'; f.innerHTML='<i class="bi bi-exclamation-circle"></i> Teléfono chileno inválido (9 dígitos)'; marcarNtErr('fPhoneWrap'); }
});

// ── Validación correo ──
document.getElementById('ntEmail').addEventListener('blur', function(){
  const v = this.value.trim(); const f = document.getElementById('ntVEmail');
  if(!v){ f.className='nt-vfeed'; f.textContent=''; marcarNtErr('fEmailWrap'); return; }
  if(/^[^@\s]+@[^@\s]+\.[^@\s]{2,}$/.test(v)){ f.className='nt-vfeed ok'; f.innerHTML='<i class="bi bi-check-circle-fill"></i> Correo válido'; marcarNtOk('fEmailWrap'); }
  else { f.className='nt-vfeed err'; f.innerHTML='<i class="bi bi-exclamation-circle"></i> Correo inválido'; marcarNtErr('fEmailWrap'); }
});

// ── Google Places (dirección) — siempre validar (perfil logístico de Daniel) ──
// (ya estamos dentro de DOMContentLoaded, ilusPlacesAutocomplete existe)
function ntInitDir(){
  if(typeof ilusPlacesAutocomplete !== 'function'){
    if(window.__ilusGmapsPending){ window.__ilusGmapsPending.push(ntInitDir); } return;
  }
  ilusPlacesAutocomplete('ntDireccion', { country:'cl', types:['address'],
    onPlaceSelected:function(place){
      document.getElementById('ntDirLat').value = place.lat;
      document.getElementById('ntDirLng').value = place.lng;
      document.getElementById('ntDirPlace').value = place.place_id || '';
      if(place.componentes){
        const cm = place.componentes.find(c=>c.types.indexOf('locality')>=0 || c.types.indexOf('administrative_area_level_3')>=0);
        if(cm) document.getElementById('ntComuna').value = cm.long_name;
        // Daniel 2026-07-12: "no me completo... region... esto tiene que ser
        // a nivel general" -- antes solo se guardaba comuna, nunca la Region
        // (Chile: administrative_area_level_1 en Google Places).
        const rg = place.componentes.find(c=>c.types.indexOf('administrative_area_level_1')>=0);
        if(rg) document.getElementById('ntRegion').value = rg.long_name;
      }
      marcarNtOk('fDirWrap');
      const h = document.getElementById('ntDirHint');
      h.innerHTML = '<i class="bi bi-check-circle-fill text-success me-1"></i>Dirección verificada · <small>'
        + place.lat.toFixed(4)+', '+place.lng.toFixed(4)+'</small>';
    } });
}
ntInitDir();

// ── Autocomplete equipo (ERP: del cliente si ya se identifico, si no catalogo general) ──
const ntAcEq = document.getElementById('ntAcEq');
const ntBuscarEquipo = debounce(async function(){
  const q = document.getElementById('ntBuscarEq').value.trim();
  if(q.length < 2){ ntAcEq.classList.remove('open'); return; }
  const rut = document.getElementById('ntRut').value.trim();
  ntAcEq.innerHTML = '<div class="nt-ac-empty"><i class="bi bi-hourglass-split"></i>Buscando equipos…</div>'; ntAcEq.classList.add('open');
  try{
    const r = await fetch('/tickets/api/erp/buscar-producto?rut='+encodeURIComponent(rut)+'&q='+encodeURIComponent(q));
    const d = await r.json();
    const rows = d.resultados || [];
    if(!d.ok && d.error){ ntAcEq.innerHTML = '<div class="nt-ac-empty err"><i class="bi bi-exclamation-triangle"></i>'+esc(d.error)+'</div>'; return; }
    if(!rows.length){ ntAcEq.innerHTML = '<div class="nt-ac-empty"><i class="bi bi-search"></i>Sin equipos que coincidan</div>'; return; }
    ntAcEq.innerHTML = rows.map((pr,i)=>'<div class="nt-ac-item" data-i="'+i+'"><div>'+esc(pr.nombre)+'</div>'
      + '<div class="rut">'+esc(pr.sku||'')+(pr.fecha? '  ·  '+esc(pr.fecha):'')+'</div></div>').join('');
    ntAcEq.querySelectorAll('.nt-ac-item').forEach(it=>it.addEventListener('click',()=>ntAddEquipo(rows[+it.dataset.i])));
  }catch(e){ ntAcEq.innerHTML = '<div class="nt-ac-empty err"><i class="bi bi-wifi-off"></i>ERP no disponible</div>'; }
}, 320);
document.getElementById('ntBuscarEq').addEventListener('input', ntBuscarEquipo);

function ntAddEquipo(p){
  if(ntEquipos.some(e=>(e.sku && e.sku===p.sku) || e.nombre===p.nombre)){ ilusToast('Ese equipo ya está agregado','info'); }
  else ntEquipos.push({sku:p.sku||'', nombre:p.nombre||'', notas:''});
  ntAcEq.classList.remove('open'); document.getElementById('ntBuscarEq').value='';
  renderNtEquipos();
}
function renderNtEquipos(){
  const l = document.getElementById('ntListaEq');
  // Contador visible (Daniel: "puede ser un equipo, veinte, cincuenta... tiene
  // que tener ese dinamismo") -- se actualiza en vivo con cada add/quitar.
  const counter = document.getElementById('ntEqCount');
  const n = ntEquipos.length;
  counter.classList.toggle('show', n > 0);
  document.getElementById('ntEqCountTxt').textContent = n===1 ? '1 equipo agregado' : n+' equipos agregados';
  // Cada equipo agregado lleva su propia observación (Daniel 2026-07-11: con
  // varias maquinas con problemas distintos, la descripcion general del
  // ticket no alcanza -- se necesita una nota por maquina).
  l.innerHTML = ntEquipos.map((e,i)=>'<div class="nt-eq-item">'
    + '<div class="nt-eqrow"><span>'+esc(e.nombre)
    + (e.sku? ' <span class="sku">· '+esc(e.sku)+'</span>':'')
    + '</span><button type="button" class="btn btn-sm btn-link text-danger p-0" onclick="ntDelEquipo('+i+')"><i class="bi bi-x-lg"></i></button></div>'
    + '<input type="text" class="form-control form-control-sm nt-eq-notas" data-i="'+i+'" placeholder="Observación de este equipo (opcional)" value="'+esc(e.notas||'')+'">'
    + '</div>').join('');
  l.querySelectorAll('.nt-eq-notas').forEach(function(inp){
    inp.addEventListener('input', function(){
      const idx = +this.dataset.i;
      if(ntEquipos[idx]) ntEquipos[idx].notas = this.value;
    });
  });
}
window.ntDelEquipo = function(i){ ntEquipos.splice(i,1); renderNtEquipos(); };

document.getElementById('ntDesc').addEventListener('input', function(){ document.getElementById('ntDescCount').textContent = this.value.length; });

document.addEventListener('click', function(e){
  if(!e.target.closest('#ntBuscarCli') && !e.target.closest('#ntAcCli')) ntAcCli.classList.remove('open');
  if(!e.target.closest('#ntBuscarEq') && !e.target.closest('#ntAcEq')) ntAcEq.classList.remove('open');
});

function validarNtTodo(){
  let ok = true;
  if(!ntTipoSel){ marcarNtErr('fTipoWrap'); ok=false; } else marcarNtOk('fTipoWrap');

  // Tipos 100% internos (control_calidad/trabajo_bodega/capacitacion): el
  // cliente/RUT/dirección NO son obligatorios -- Daniel lo pidió explícito
  // para estos 3 tipos. Tipo y descripción siguen exigidos siempre.
  const esInterno = TK_TIPOS_SIN_CLIENTE.includes(ntTipoSel);
  if(!esInterno){
    const rutVal = document.getElementById('ntRut').value.trim();
    if(!rutVal){ marcarNtErr('fRutWrap'); ok=false; } else marcarNtOk('fRutWrap');

    const empresaVal = document.getElementById('ntEmpresa').value.trim();
    if(!empresaVal){ marcarNtErr('fEmpresaWrap'); ok=false; } else marcarNtOk('fEmpresaWrap');

    const contactoVal = document.getElementById('ntContacto').value.trim();
    if(!contactoVal){ marcarNtErr('fContactoWrap'); ok=false; } else marcarNtOk('fContactoWrap');

    const phoneEl = document.getElementById('ntPhone');
    if(!phoneEl.value.trim()) phoneEl.dispatchEvent(new Event('blur'));
    if(!document.getElementById('ntVPhone').classList.contains('ok')) ok=false;

    const emailEl = document.getElementById('ntEmail');
    if(!emailEl.value.trim()) emailEl.dispatchEvent(new Event('blur'));
    if(!document.getElementById('ntVEmail').classList.contains('ok')) ok=false;

    const dirVal = document.getElementById('ntDireccion').value.trim();
    const dirLat = document.getElementById('ntDirLat').value;
    if(!dirVal || !dirLat){ marcarNtErr('fDirWrap'); ok=false; } else marcarNtOk('fDirWrap');
  }

  const descVal = document.getElementById('ntDesc').value.trim();
  if(!descVal){ marcarNtErr('fDescWrap'); ok=false; } else marcarNtOk('fDescWrap');

  return ok;
}

document.getElementById('ntBtnCrear').addEventListener('click', async function(){
  if(!validarNtTodo()){
    ilusToast('Revisa los campos marcados en rojo','warning');
    const firstErr = document.querySelector('#ntModal .nt-card .err, #ntModal .nt-field.err');
    if(firstErr) firstErr.scrollIntoView({behavior:'smooth', block:'center'});
    return;
  }
  const btn = this; btn.disabled = true;
  const payload = {
    tipo: ntTipoSel,
    es_garantia: ntEsGarantia,
    prioridad: document.getElementById('ntPrio').value,
    rut: document.getElementById('ntRut').value.trim(),
    empresa: document.getElementById('ntEmpresa').value.trim(),
    nombre_contacto: document.getElementById('ntContacto').value.trim(),
    phone: document.getElementById('ntPhone').value.trim(),
    email: document.getElementById('ntEmail').value.trim(),
    sucursal: document.getElementById('ntSucursal').value.trim(),
    direccion: document.getElementById('ntDireccion').value.trim(),
    direccion_lat: document.getElementById('ntDirLat').value || null,
    direccion_lng: document.getElementById('ntDirLng').value || null,
    direccion_place_id: document.getElementById('ntDirPlace').value || null,
    comuna_nombre: document.getElementById('ntComuna').value || null,
    region_nombre: document.getElementById('ntRegion').value || null,
    numero_documento: document.getElementById('ntDoc').value.trim(),
    producto: ntEquipos.map(e=>e.nombre).join(' · '),
    sku: ntEquipos.map(e=>e.sku).filter(Boolean).join(', '),
    descripcion: document.getElementById('ntDesc').value.trim(),
    equipos: ntEquipos.map(e=>({kopr:e.sku, nombre:e.nombre, sku:e.sku, notas:e.notas||''})),
  };
  try{
    const r = await fetch('/tickets/api/tickets', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(payload)});
    const d = await r.json();
    if(!d.ok){ ilusToast(d.error||'Error al crear','error'); btn.disabled=false; return; }
    ilusToast('✓ '+(d.numero_ticket||'Ticket')+' creado','success');
    ntModal.hide();
    if (ntArchivos.length){
      await ntSubirArchivos(d.id);
    }
    setTimeout(()=>location.href='/tickets/'+d.id, 400);
  }catch(e){ ilusToast('Error de red','error'); btn.disabled=false; }
});

// ══════════════ Crear ticket desde documento ERP (búsqueda avanzada) ══════════════
// Daniel 2026-07-12: reemplaza el flujo viejo de 2 ilusPrompt() (tipo+numero
// a ciegas, sin ver lineas ni saldo) por el modal "tka" -- mismo espiritu
// que el buscador avanzado de Retiros (pestañas, saldo por linea, seleccion
// granular, contador). Ver templates/tickets/_tka_modal.html.
document.getElementById('btnDesdeDoc').addEventListener('click', function(){
  tkaOpen({ mode: 'crear' });
});

// ══════════════ Importar CSV Triple A (superadmin) ══════════════
// Flujo: elegir "Reporte Tickets.csv" (obligatorio) → opcionalmente el
// "Reporte SLA.csv" → dry-run con resumen → confirmar → importar real.
const btnTaa = document.getElementById('btnImportarTaa');
if(btnTaa){
  const fTk = document.getElementById('taaFileTickets');
  const fSla = document.getElementById('taaFileSla');

  async function taaEnviar(dry){
    const fd = new FormData();
    fd.append('csv_tickets', fTk.files[0]);
    if(fSla.files[0]) fd.append('csv_sla', fSla.files[0]);
    const r = await fetch('/tickets/api/admin/importar-taa?dry_run='+(dry?'1':'0'), {method:'POST', body:fd});
    return (await r.json());
  }

  async function taaFlujo(){
    ilusLoader.show('Analizando CSV de Triple A…');
    let prev;
    try{ prev = await taaEnviar(true); }
    catch(e){ ilusLoader.hide(); ilusToast('No se pudo analizar el CSV','error'); return; }
    ilusLoader.hide();
    if(!prev.ok){ ilusToast(prev.error||'CSV inválido','error'); return; }
    const rz = prev.resumen || {};
    if(rz.error){ ilusToast(rz.error,'error'); return; }
    if(!rz.importados){
      await ilusAlert({title:'Nada nuevo que importar',
        message:(rz.ya_importados||0)+' ticket(s) del CSV ya están en el sistema.',
        sub:(rz.invalidos? rz.invalidos+' fila(s) ilegibles del export se omiten.':''), type:'info'});
      return;
    }
    const ok = await ilusConfirm({
      title:'Importar tickets de Triple A',
      message:'Se importarán '+rz.importados+' ticket(s) nuevo(s) de '+rz.validos+' válidos en el CSV'
        +(rz.ya_importados? ' ('+rz.ya_importados+' ya estaban)':'')+'.',
      sub:'El correo de contacto quedará como daniel.aguilar@sphs.cl (editable); el original se conserva en las notas.'
        +(rz.invalidos? ' · '+rz.invalidos+' fila(s) ilegibles se omiten.':''),
      okLabel:'Importar ahora', cancelLabel:'Cancelar',
    });
    if(!ok) return;
    ilusLoader.show('Importando tickets de Triple A…');
    try{
      const res = await taaEnviar(false);
      ilusLoader.hide();
      const rr = res.resumen || {};
      if(!res.ok){ ilusToast(res.error||'Error al importar','error'); return; }
      if(rr.errores){ ilusToast(rr.importados+' importados, '+rr.errores+' con error (revisa logs)','warning'); }
      else { ilusToast('✓ '+(rr.importados||0)+' ticket(s) de Triple A importados','success'); }
      cargarTickets();
    }catch(e){ ilusLoader.hide(); ilusToast('Error al importar','error'); }
  }

  btnTaa.addEventListener('click', function(){ fTk.value=''; fSla.value=''; fTk.click(); });
  fTk.addEventListener('change', async function(){
    if(!fTk.files[0]) return;
    const conSla = await ilusConfirm({
      title:'Reporte SLA (opcional)',
      message:'¿Quieres adjuntar también el "Reporte SLA" CSV?',
      sub:'Aporta la fecha real de resolución y la duración de cada ticket. Si no lo tienes, continúa sin él.',
      okLabel:'Sí, adjuntar SLA', cancelLabel:'Continuar sin SLA',
    });
    if(conSla){ fSla.click(); } else { taaFlujo(); }
  });
  fSla.addEventListener('change', function(){ taaFlujo(); });
  // Si cierra el selector del SLA sin elegir archivo, seguir sin SLA
  fSla.addEventListener('cancel', function(){ taaFlujo(); });
}

// ══════════════ Purgar tickets por correo (superadmin, 2026-07-12) ══════════════
// Deja SOLO los tickets de un correo; borra el resto (incluye migrados de
// Triple A si no coinciden -- confirmado explícitamente por Daniel). Mismo
// patron dry-run -> confirmar con el conteo real -> ejecutar que el
// importador CSV de arriba, pero con confirmacion ESCRITA (no solo Sí/No)
// por el alcance/tamaño de lo que se borra permanentemente.
const btnPurgar = document.getElementById('btnPurgarTickets');
if(btnPurgar){
  async function purgarEnviar(keepEmail, dryRun, confirmText){
    const r = await fetch('/tickets/api/admin/purgar-por-correo', {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({keep_email:keepEmail, dry_run:dryRun, confirm:confirmText||''}),
    });
    return await r.json();
  }

  btnPurgar.addEventListener('click', async function(){
    const keepEmail = await ilusPrompt({
      title:'Purgar tickets',
      message:'¿Qué correo de cliente quieres CONSERVAR? Se borrarán todos los tickets que NO tengan ese correo.',
      placeholder:'correo@ejemplo.com', defaultValue:'juandaniel.aguilar17@gmail.com',
      required:true,
    });
    if(!keepEmail) return;

    ilusLoader.show('Calculando cuántos tickets se borrarían…');
    let prev;
    try{ prev = await purgarEnviar(keepEmail, true); }
    catch(e){ ilusLoader.hide(); ilusToast('No se pudo calcular la purga','error'); return; }
    ilusLoader.hide();
    if(!prev.ok){ ilusToast(prev.error||'Error al calcular la purga','error'); return; }
    const rz = prev.resumen || {};
    if(!rz.a_borrar){
      await ilusAlert({title:'Nada que purgar',
        message:'Todos los tickets ya tienen el correo '+keepEmail+' (o no hay tickets).', type:'info'});
      return;
    }
    const confirmEsperado = 'BORRAR ' + keepEmail.toUpperCase();
    const texto = await ilusPrompt({
      title:'Confirmar purga PERMANENTE',
      message:'Se borrarán '+rz.a_borrar+' de '+rz.total+' ticket(s), incluyendo '+rz.a_borrar_triple_a
        +' migrado(s) de Triple A. Quedarán '+rz.a_conservar+' ticket(s) con correo '+keepEmail+'.',
      sub:'Esta acción NO se puede deshacer. Para confirmar, escribe exactamente: '+confirmEsperado,
      placeholder:confirmEsperado, type:'danger', required:true,
    });
    if(!texto || texto.trim().toUpperCase() !== confirmEsperado){
      if(texto) ilusToast('Texto de confirmación incorrecto, no se borró nada','warning');
      return;
    }
    ilusLoader.show('Purgando tickets…');
    try{
      const res = await purgarEnviar(keepEmail, false, texto);
      ilusLoader.hide();
      if(!res.ok){ ilusToast(res.error||'No se pudo purgar','error'); return; }
      ilusToast('✓ '+res.eliminados+' ticket(s) eliminados permanentemente','success');
      cargarTickets();
    }catch(e){ ilusLoader.hide(); ilusToast('Error de red al purgar','error'); }
  });
}

// ══════════════ Revisar documentos nuevos ZZ-Instalación (superadmin) ══════════════
// Escaneo bajo demanda (no hay cron/scheduler en este proyecto -- ver
// CLAUDE.md y blueprint de este feature): el superadmin aprieta el botón
// cuando quiere revisar si llegaron documentos ERP nuevos con línea
// ZZINSTALACION y aún no tienen ticket. El backend (endpoint pendiente,
// POST /tickets/api/zz-instalacion/escanear) es responsable de la
// idempotencia real (tabla tk_zz_instalacion_scan); este botón solo dispara
// el escaneo y muestra el resultado.
const btnZzInstalacion = document.getElementById('btnZzInstalacion');
if(btnZzInstalacion){
  btnZzInstalacion.addEventListener('click', async function(){
    const ok = await ilusConfirm({
      title:'Revisar documentos nuevos',
      message:'¿Buscar en el ERP documentos recientes con línea ZZINSTALACION que todavía no tengan ticket?',
      sub:'Se creará un ticket de instalación (sin responsable asignado) por cada documento nuevo encontrado. Los ya revisados no se duplican.',
      okLabel:'Sí, revisar ahora', cancelLabel:'Cancelar',
    });
    if(!ok) return;
    btnZzInstalacion.disabled = true;
    const _origHtml = btnZzInstalacion.innerHTML;
    btnZzInstalacion.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Revisando…';
    try{
      const r = await fetch('/tickets/api/zz-instalacion/escanear', {
        method:'POST', headers:{'Content-Type':'application/json', 'X-Requested-With':'XMLHttpRequest'},
      });
      let d;
      try{ d = await r.json(); }
      catch(_){
        ilusAlert({type:'error', title:'Función no disponible todavía',
          message:'El endpoint de escaneo ZZ-Instalación aún no está desplegado en el backend.'});
        return;
      }
      if(!d.ok){
        ilusAlert({type:'error', title:'No se pudo revisar', message: d.error || 'Error desconocido'});
        return;
      }
      const creados = d.tickets_creados || [];
      if(!creados.length){
        ilusToast('Sin documentos nuevos de instalación (revisados: '+(d.documentos_revisados||0)+', ya existían: '+(d.ya_existian||0)+')', {type:'info'});
      } else {
        const links = creados.map(function(t){
          return '<a href="/tickets/'+t.id+'" target="_blank" style="color:var(--ilus-red);font-weight:700">'
            + _tkaEsc(t.numero_ticket || ('#'+t.id)) + '</a>';
        }).join(', ');
        await ilusAlert({
          type:'success', title: creados.length+' ticket(s) de instalación creado(s)',
          message:'Documentos revisados: '+(d.documentos_revisados||0)+' · Ya existían: '+(d.ya_existian||0)+'.',
          sub:'Nuevos: '+links, subHtml:true,
        });
        cargarTickets();
      }
      if((d.errores||[]).length){
        ilusToast((d.errores.length)+' documento(s) con error al procesar, revisa el log', {type:'warning'});
      }
    }catch(e){
      ilusAlert({type:'error', title:'Error de red', message: e.message});
    }finally{
      btnZzInstalacion.disabled = false;
      btnZzInstalacion.innerHTML = _origHtml;
    }
  });
}
}); // cierre de document.addEventListener('DOMContentLoaded', ...)

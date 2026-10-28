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
  // Tickets 2.0: el chip de estado (Activos/Todos/…) manda sobre el select.
  const e = (typeof tkEstadoChip !== 'undefined' && tkEstadoChip) || document.getElementById('fEstado').value; if(e) p.set('estado', e);
  const t = document.getElementById('fTipo').value; if(t) p.set('tipo', t);
  const pr = document.getElementById('fPrio').value; if(pr) p.set('prioridad', pr);
  const resp = document.getElementById('fResp').value; if(resp) p.set('asignado_a', resp);
  if(tkHoy){ p.set('hoy','1'); }
  else{
    const fd = document.getElementById('fDesde').value; if(fd) p.set('fecha_desde', fd);
    const fh = document.getElementById('fHasta').value; if(fh) p.set('fecha_hasta', fh);
  }
  if(typeof tkSla !== 'undefined' && tkSla) p.set('sla', tkSla);
  if(conOrden && tkSort){ p.set('sort', tkSort); p.set('dir', tkDir); }
  return p;
}

// ── Encabezados: [label, columna sort del backend o '' = no ordenable] ──
// Tickets 2.0 (27-oct, Daniel: "duplica el formato, la tabla, los SLA" del
// Monitor de Retiros): el avance va bajo el estado, origen y fecha de creación
// dentro de la celda del ticket, y «Actualizado» dentro de «Última actividad».
// Columnas nuevas: «Tiempo» (SLA de respuesta, 4 h hábiles) y «Última actividad».
const TK_COLS = [
  ['Ticket','created_at'], ['Cliente','empresa'], ['Tipo','tipo'], ['Estado','estado'],
  ['Responsable','asignado_a'], ['Tiempo · SLA','espera_desde'], ['Última actividad','updated_at'],
  ['Acciones',''],
];
let tkSla = '';           // filtro por semáforo del SLA: '' | 'rojo' | 'ambar' | 'verde'
let tkEstadoChip = '__activos__'; // chips de estado: la bandeja abre en «Activos» (como el Monitor de Retiros)
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
  // «Tiempo»: primero lo que lleva MÁS rato esperando (asc = espera más antigua arriba).
  else if(tkSort !== col){ tkSort = col; tkDir = (col === 'espera_desde' ? 'asc' : 'desc'); }
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
  // Tickets 2.0: «Vencidos (SLA)» = sin respuesta al cliente en 4 h hábiles.
  // La fecha límite vencida (lo que contaba antes) queda en la línea de abajo.
  const kv = document.getElementById('kpiVencidos');
  const kvSub = document.getElementById('kpiVencidosSub');
  const _set = function(id, v){ const el = document.getElementById(id); if(el) el.textContent = (v == null ? '—' : v); };
  if(data.kpis.sla_rojo != null){
    kv.textContent = data.kpis.sla_rojo;
    if(kvSub) kvSub.textContent = (data.kpis.vencidos||0) + ' con fecha límite vencida';
  } else {
    kv.textContent = data.kpis.vencidos;
    if(kvSub) kvSub.textContent = 'Por fecha límite';
  }
  _set('kpiPorVencer', data.kpis.sla_ambar);
  _set('kpiEsperando', data.kpis.sla_esperando);
  _set('kpiSinAsignar', data.kpis.sin_asignar);
  tkMarcarKpiActivo();
  const estEl = document.getElementById('tkEstados');
  if(estEl){ estEl.innerHTML = tkEstadosHtml(data.estados || {}); }
  const semEl = document.getElementById('tkSemaforo');
  if(semEl){ const h = tkSemaforoHtml(data.kpis); semEl.innerHTML = h; semEl.style.display = h ? '' : 'none'; }

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
  const sla = tk.sla_resp || null;
  const sem = sla ? sla.semaforo : '';
  const urg = tk.prioridad === 'urgente' ? '<span class="tkc-urg"><i class="bi bi-lightning-charge-fill"></i>Urgente</span>' : '';
  const contacto = (tk.nombre_contacto && tk.nombre_contacto !== cliente)
    ? '<div class="tkc-sub"><i class="bi bi-person"></i>'+esc(tk.nombre_contacto)+'</div>' : '';
  const fono = tk.phone ? '<div class="tkc-sub"><i class="bi bi-telephone"></i>'+esc(tk.phone)+'</div>' : '';
  const rut = (tk.rut && tk.rut !== cliente) ? '<div class="tkc-sub">RUT '+esc(tk.rut)+'</div>' : '';
  return '<tr class="prio-'+(tk.prioridad||'media')+(sem?' sla-'+sem:'')+'" onclick="location.href=\'/tickets/'+tk.id+'\'">'
    + '<td><div class="tkc-num"><span class="tk-num">'+esc(tk.numero_ticket||('#'+tk.id))+'</span>'+garBadge+unread+urg+'</div>'
    +   '<div class="tkc-orig">'+tkOrigenCelda(tk.origen)+'<span class="tkc-hace">'+esc(fpCreado.f)+(fpCreado.h?' '+esc(fpCreado.h):'')+'</span></div></td>'
    + '<td><div class="tkc-cliente">'+esc(cliente)+'</div>'+rut+contacto+fono
    +   (titulo?'<div class="tkc-titulo">'+esc(titulo)+'</div>':'')+'</td>'
    + '<td>'+tkTipoCelda(tk)+'</td>'
    + '<td><span class="tk-badge bs-'+tk.estado+'">'+esc(ESTADO_LABEL[tk.estado]||tk.estado)+'</span>'
    +   '<div class="tkc-avance">'+tkMiniStepper(tk)+'</div></td>'
    + '<td>'+tkRespCelda(tk)+'</td>'
    + '<td>'+tkSlaCelda(tk)+'</td>'
    + '<td>'+tkActividadCelda(tk, fpActualizado)+'</td>'
    + '<td>'+tkAccionesCelda(tk)+'</td>'
    + '</tr>';
}

// ── Responsable: avatar con iniciales + nombre completo (REGLA #15) ──
function tkIniciales(s){
  const p = String(s||'').replace(/@.*/,'').replace(/[._-]+/g,' ').trim().split(/\s+/).filter(Boolean);
  if(!p.length) return '?';
  return (p[0][0] + (p.length>1 ? p[p.length-1][0] : '')).toUpperCase();
}
function tkRespCelda(tk){
  const a = (tk.asignado_a && String(tk.asignado_a).trim()) ? String(tk.asignado_a).trim() : '';
  if(!a) return '<span class="tkc-resp none"><i class="bi bi-person-dash"></i>Sin asignar</span>';
  return '<span class="tkc-resp"><span class="av" aria-hidden="true">'+esc(tkIniciales(a))+'</span>'+esc(a)+'</span>';
}

// ── Tiempo · SLA de respuesta (4 h hábiles por mensaje del cliente), mismos
//    textos del reloj de Retiros: «Sin responder · 1 h 22 min hábiles». ──
function tkSlaCelda(tk){
  const s = tk.sla_resp;
  if(!s) return '<span class="text-muted" style="font-size:.78rem;">—</span>';
  let ic = 'bi-check-circle-fill';
  if(s.nivel === 'vencido') ic = 'bi-exclamation-octagon-fill';
  else if(s.nivel === 'atencion') ic = 'bi-hourglass-split';
  else if(s.nivel === 'ok') ic = (s.pausa ? 'bi-pause-circle-fill' : 'bi-stopwatch-fill');
  else if(s.nivel === 'cerrado') ic = 'bi-lock-fill';
  let sub = s.sub || '';
  if(s.nivel === 'respondido' && tk.ultima_resp_hace) sub = 'Respondido ' + tk.ultima_resp_hace;
  return '<div class="tkc-sla '+esc(s.semaforo)+'"><span class="pill"><i class="bi '+ic+'"></i>'+esc(s.pill)+'</span>'
    + (sub ? '<div class="sub">'+esc(sub)+'</div>' : '') + '</div>';
}

// ── Última actividad: quién escribió, hace cuánto y un extracto. ──
function tkActividadCelda(tk, fpAct){
  const m = tk.ult_msg;
  const fecha = '<div class="tkc-fecha">'+esc(fpAct.f)+(fpAct.h?'<span class="hora">'+esc(fpAct.h)+'</span>':'')+'</div>';
  if(!m) return fecha;
  const cls = m.tipo === 'client_message' ? 'cli' : (m.tipo === 'comentario' ? 'int' : 'eq');
  return '<div class="tkc-act '+cls+'"><div class="who"><i class="bi '
    + (cls==='cli' ? 'bi-person-fill' : (cls==='int' ? 'bi-lock-fill' : 'bi-reply-fill')) + '"></i>'
    + esc(m.quien)+(m.hace ? ' · '+esc(m.hace) : '')+'</div>'
    + (m.texto ? '<div class="txt">'+esc(m.texto)+'</div>' : '') + '</div>';
}

// ── Chips del semáforo (como «Todos los semáforos / Urgente / Atención / En orden» de Retiros) ──
function tkSemaforoHtml(k){
  if(k == null || k.sla_rojo == null){ return ''; }
  const verde = Math.max((k.total||0) - (k.sla_rojo||0) - (k.sla_ambar||0), 0);
  const chip = function(v, lbl, n, cls, tip){
    return '<button type="button" class="tk-sem-chip '+cls+(tkSla===v?' on':'')+'" onclick="tkSemaforo(\''+v+'\')" title="'+esc(tip)+'">'
      + (cls ? '<span class="dot"></span>' : '') + lbl + (n!=null ? ' <b>'+n+'</b>' : '') + '</button>';
  };
  return chip('', 'Todos los semáforos', null, '', 'Ver todos')
    + chip('rojo', 'Vencido', k.sla_rojo, 'rojo', 'Más de 4 h hábiles sin responderle al cliente')
    + chip('ambar', 'Por vencer', k.sla_ambar, 'ambar', 'Queda 1 h hábil o menos para responder')
    + chip('verde', 'En orden', verde, 'verde', 'Respondidos o dentro del plazo')
    + '<span class="tk-sem-nota"><i class="bi bi-stopwatch"></i>SLA: 4 h hábiles por mensaje del cliente · '
    + (k.sla_esperando||0) + ' esperando respuesta</span>';
}
window.tkSemaforo = function(v){ tkSla = (tkSla === v ? '' : v); tkPage = 1; cargarTickets(); };

// ── Chips de estado con su conteo (filtros rápidos con un clic) ──
const TK_EST_ORDEN = ['open','in_progress','pending','ot_pending_approval','ot_generated','ot_in_progress','resolved','closed','cancelado'];
function tkEstadosHtml(cnt){
  const chip = function(v, lbl, n, ic){
    return '<button type="button" class="tk-est-chip'+(tkEstadoChip===v?' on':'')+'" onclick="tkEstado(\''+v+'\')">'
      + (ic?'<i class="bi '+ic+'"></i>':'') + esc(lbl) + ' <b>'+(n||0)+'</b></button>';
  };
  let h = chip('__activos__', 'Activos', cnt.__activos__, 'bi-lightning-charge-fill') + chip('', 'Todos', cnt.__todos__, 'bi-list-ul');
  TK_EST_ORDEN.forEach(function(e){ if(cnt[e]) h += chip(e, ESTADO_LABEL[e] || e, cnt[e], ''); });
  return h;
}
window.tkEstado = function(v){
  tkEstadoChip = v;
  const sel = document.getElementById('fEstado');
  if(sel) sel.value = (v && v.indexOf('__') !== 0) ? v : '';
  tkPage = 1; cargarTickets();
};

// ── Tarjetas KPI (mismo patrón del Monitor de Retiros): clic = filtrar la
//    tabla; otro clic en la misma tarjeta quita el filtro. ──
function tkMarcarKpiActivo(){
  const resp = (document.getElementById('fResp')||{}).value || '';
  document.querySelectorAll('#tkKpis [data-tk-filtro]').forEach(function(el){
    const f = el.getAttribute('data-tk-filtro').split(':');
    const on = (f[0] === 'sla' && tkSla === f[1]) || (f[0] === 'resp' && resp === f[1]);
    el.classList.toggle('is-active', !!on);
    el.setAttribute('aria-pressed', on ? 'true' : 'false');
  });
}
function tkKpiFiltrar(el){
  const f = el.getAttribute('data-tk-filtro').split(':');
  if(f[0] === 'sla'){ tkSemaforo(f[1]); return; }
  if(f[0] === 'resp'){
    const sel = document.getElementById('fResp');
    sel.value = (sel.value === f[1] ? '' : f[1]);
    tkFiltroCambio();
  }
}
document.querySelectorAll('#tkKpis [data-tk-filtro]').forEach(function(el){
  el.addEventListener('click', function(){ tkKpiFiltrar(el); });
  el.addEventListener('keydown', function(ev){
    if(ev.key === 'Enter' || ev.key === ' '){ ev.preventDefault(); tkKpiFiltrar(el); }
  });
});

// Cualquier cambio de filtro resetea a la pagina 1 (el resultado cambia).
function tkFiltroCambio(){ tkPage = 1; cargarTickets(); }
function debounced(){ clearTimeout(_tkTimer); _tkTimer = setTimeout(tkFiltroCambio, 350); }
['fTicket','fQ','fRut'].forEach(function(id){ document.getElementById(id).addEventListener('input', debounced); });
document.getElementById('fEstado').addEventListener('change', function(){ tkEstadoChip = this.value; });
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
  tkSla = '';
  tkEstadoChip = '__activos__';
  tkSort = 'updated_at'; tkDir = 'desc'; tkPage = 1; // limpiar vuelve al default, no al orden inteligente
  cargarTickets();
});

// ── Select Responsable: usuarios activos (mismo endpoint que la ficha) ──
async function tkCargarResponsables(){
  try{
    // Tickets 2.0: antes /mantenciones/api/ejecutivos = TODOS los usuarios
    // activos (salía gente que no lleva tickets). Ahora la misma lista que la
    // ficha: el permiso «Responsable de tickets» (o, si nadie lo tiene, los
    // roles de Tickets).
    const r = await fetch('/tickets/api/responsables');
    const j = await r.json();
    const rows = (j && Array.isArray(j.responsables)) ? j.responsables : [];
    const sel = document.getElementById('fResp');
    const actual = sel.value;
    // Quien no ve todo recibe igual solo los suyos y los sin asignar: «Todos»
    // se rotula así para que se entienda qué está mirando.
    const lblTodos = (j && j.ve_todo === false) ? 'Míos y sin asignar' : 'Todos';
    sel.innerHTML = '<option value="">'+lblTodos+'</option>'
      + '<option value="__sin_asignar__">Sin asignar</option>'
      + rows.map(function(e){ return '<option value="'+esc(e.nombre)+'">'+esc(e.nombre)
          + (j && j.yo && e.nombre === j.yo ? ' (yo)' : '')+'</option>'; }).join('');
    sel.value = actual; // conserva la seleccion si ya habia una
    // Tickets 2.0 (Daniel 2026-10-27: «abrir con los nombres filtrados… se
    // podrá sacar»): la primera vez, la bandeja abre con MIS tickets; el
    // filtro se quita como cualquier otro (o con «Limpiar filtros»).
    if(!_tkRespInicial && j && j.soy_responsable && j.yo && !actual){
      sel.value = j.yo;
    }
    _tkRespInicial = true;
  }catch(e){ /* sin red: quedan Todos / Sin asignar */ }
}
let _tkRespInicial = false;
// Primero la lista de responsables (decide el filtro inicial «mis tickets»),
// después la bandeja: una sola carga, ya filtrada.
tkCargarResponsables().finally(function(){ cargarTickets(); });

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
// Tickets 2.0 (27-oct): LATIDO. Antes se recargaba la página entera de la
// bandeja (más el conteo de no leídos) cada 10 s por cada persona conectada.
// Ahora cada 4 s se pregunta solo "¿cambió algo?" (/bandeja/latido: tickets,
// mensajes y no leídos, consultas por índice) y se recarga solo si cambió.
// Más rápido para ver un mensaje nuevo y mucho más liviano para la base.
let _tkListaVer = null;
async function _tkListaLatido(){
  if (document.hidden) return;
  try {
    const r = await fetch('/tickets/api/bandeja/latido', {cache:'no-store'});
    const j = await r.json();
    if (!j || !j.ok || !j.ver) return;
    if (_tkListaVer === null) { _tkListaVer = j.ver; return; }   // 1.er latido: ya se cargó arriba
    if (j.ver !== _tkListaVer && _tkListaAutoSeguro()) {
      await cargarTickets();
      _tkListaVer = j.ver;
    }
  } catch(e) {}
}
function _tkProgramarAutoRefrescoLista(){
  clearTimeout(_tkListaAutoTimer);
  _tkListaAutoTimer = setTimeout(async function(){
    await _tkListaLatido();
    _tkProgramarAutoRefrescoLista();
  }, 4000);
}
document.addEventListener('visibilitychange', function(){ if (!document.hidden) _tkListaLatido(); });
_tkListaLatido();   // toma la firma de inmediato: ningún cambio queda en el hueco
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
// Tickets 2.0 (Daniel 2026-10-27): el modal hereda el FORMATO del «Nuevo
// retiro interno» de Retiros (static/retiros_nuevo_interno.js): 5 pasos con
// semáforo rojo→ámbar→verde, «N de 5 pasos listos» en la cabecera, «Falta: …»
// bajo cada título, OT/documento del ERP que traen cliente y productos, chips
// «Detectado en ERP → usar», «Editar manualmente» y el botón principal
// deshabilitado hasta que todo esté en verde. Prefijo ntw* (nuevo ticket
// wizard). Todo lo de antes (ids, validadores, Google Places, adjuntos,
// catálogo) sigue acá tal cual (REGLA #4.2).
let ntTipoSel = '';
let ntEsGarantia = 0;      // garantia SEPARADA del tipo (aplica a cualquiera)
let ntCliente = null;      // {empresa, rut}
// [{sku, nombre, notas, sel, origen:'ot'|'doc'|'manual', origenLabel, tido, nudo, cantidad, serie}]
const ntEquipos = [];
let ntArchivos = [];       // [{file, estado:'pendiente'|'subiendo'|'ok'|'error', nombre}] -- adjuntos del nuevo ticket
const NT_MAX_ADJ = 15;     // mismo tope que el formulario publico /soporte
const NT_MAX_MB = 25;      // idem MAX_ADJUNTO_MB en tickets_module.py
const ntModalEl = document.getElementById('ntModal');
const ntModal = new bootstrap.Modal(ntModalEl);
const $nt = function(id){ return document.getElementById(id); };

// ── Estado del asistente (Tickets 2.0) ──
let ntwOt = null;            // OT asociada: {id, numero_ot, titulo, tipo_label, estado_label, fecha, cliente, equipos, documentos}
const ntwDocs = new Map();   // 'TIDO|NUDO' → {tido, nudo, cliente, rut, fecha, lineas, origen:'erp'|'ot'}
let ntwClienteManual = false;
let ntwRespCargados = false;
const ntwChips = new Map();  // 'campo|valor' → {campo, valor, origen}
const NTW_MOTIVOS = (window.TK_LIST_DATA && window.TK_LIST_DATA.MOTIVOS) || {};
const NTW_MOTIVOS_GENERICOS = ['Consulta general', 'Reclamo', 'Seguimiento de una solicitud anterior'];
const NTW_ICONO_ESTADO = { rojo: 'bi-x-circle-fill', ambar: 'bi-exclamation-triangle-fill', verde: 'bi-check-circle-fill' };
let ntwUltimaEval = [];

// ── Validación visual (obligatorios) ──
const NT_REQ_WRAPS = ['fTipoWrap','fRutWrap','fEmpresaWrap','fContactoWrap','fPhoneWrap','fEmailWrap','fDirWrap','fMotivoWrap','fDescWrap'];
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

// ── RUT chileno: limpiar, validar dígito verificador y formatear ──
function ntwRutLimpio(v){ return String(v||'').replace(/[^0-9kK]/g,'').toUpperCase(); }
function ntwRutValido(v){
  const c = ntwRutLimpio(v);
  if(c.length < 8) return false;
  const cuerpo = c.slice(0,-1), dv = c.slice(-1);
  if(!/^\d+$/.test(cuerpo)) return false;
  let suma = 0, mul = 2;
  for(let i = cuerpo.length-1; i >= 0; i--){ suma += parseInt(cuerpo[i],10)*mul; mul = mul===7 ? 2 : mul+1; }
  const r = 11 - (suma % 11);
  const dvOk = r===11 ? '0' : (r===10 ? 'K' : String(r));
  return dv === dvOk;
}
function ntwRutFormato(v){
  const c = ntwRutLimpio(v);
  if(c.length < 2) return c;
  const cuerpo = c.slice(0,-1), dv = c.slice(-1);
  return cuerpo.replace(/\B(?=(\d{3})+(?!\d))/g,'.') + '-' + dv;
}
function ntwRutClave(v){ return ntwRutLimpio(v).replace(/^0+/,''); }

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
  const vr = $nt('ntwVRut'); if(vr){ vr.className='nt-vfeed'; vr.textContent=''; }
  NT_REQ_WRAPS.forEach(limpiarNtErr);
  // Tickets 2.0: origen, chips, edición manual, motivo, responsable
  ntwOt = null; ntwDocs.clear(); ntwChips.clear();
  if($nt('ntMotivo')) $nt('ntMotivo').value = '';
  if($nt('ntwOtQ')) $nt('ntwOtQ').value = '';
  if($nt('ntwOtSearch')) $nt('ntwOtSearch').style.display = 'none';
  if($nt('ntwOtAc')) $nt('ntwOtAc').classList.remove('open');
  if($nt('ntwBtnOt')) $nt('ntwBtnOt').classList.remove('is-on');
  ntwSetClienteManual(false);
  ntwRenderChips();
  ntwRenderOrigenes();
  ntwRenderMotivos();
  ntwRespDefault();
  ntActualizarObligatoriedadCliente();
  renderNtEquipos();
  ntArchivos = []; document.getElementById('ntFile').value=''; ntRenderArchivos();
  ntwRefresh();
}
ntModalEl.addEventListener('hidden.bs.modal', ntReset);
ntModalEl.addEventListener('shown.bs.modal', function(){ ntwCargarResponsables(); ntwRefresh(); });

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
  ntwRefresh();
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
window.ntQuitarArchivo = function(i){ ntArchivos.splice(i,1); ntRenderArchivos(); ntwRefresh(); };

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
function ntwEsInterno(){ return TK_TIPOS_SIN_CLIENTE.includes(ntTipoSel); }
function ntActualizarObligatoriedadCliente(){
  const esInterno = ntwEsInterno();
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
  ntwRenderMotivos();
  ntwRefresh();
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
  ntwRefresh();
}
document.querySelectorAll('.nt-gar-pill').forEach(el=>{
  el.addEventListener('click', ()=>ntSeleccionarGar(el));
  el.addEventListener('keydown', function(e){
    if(e.key==='Enter' || e.key===' '){ e.preventDefault(); ntSeleccionarGar(el); }
  });
});

// ── Motivo principal: el sistema SUGIERE según el tipo, la persona elige o
// escribe (REGLA #32). Listas en tickets_nuevo_motivos.py → TK_LIST_DATA.MOTIVOS. ──
function ntwRenderMotivos(){
  const cont = $nt('ntwMotivos'), origen = $nt('ntwMotivoOrigen'), inp = $nt('ntMotivo');
  if(!cont) return;
  if(!ntTipoSel){
    cont.innerHTML = '';
    if(origen) origen.innerHTML = '<i class="bi bi-lightbulb-fill"></i><span>Elige el tipo de solicitud y te sugerimos motivos; también puedes escribir el tuyo.</span>';
    return;
  }
  const lista = (NTW_MOTIVOS[ntTipoSel] && NTW_MOTIVOS[ntTipoSel].length) ? NTW_MOTIVOS[ntTipoSel] : NTW_MOTIVOS_GENERICOS;
  const actual = (inp && inp.value || '').trim().toLowerCase();
  cont.innerHTML = lista.map(function(m){
    return '<button type="button" class="ntw-motivo-chip'+(actual===m.toLowerCase()?' sel':'')+'" data-motivo="'+esc(m)+'">'+esc(m)+'</button>';
  }).join('');
  if(origen) origen.innerHTML = '<i class="bi bi-lightbulb-fill"></i><span>Sugeridos para «'+esc(TIPO_LABEL[ntTipoSel]||ntTipoSel)+'» — elige uno o escribe el tuyo.</span>';
}
$nt('ntwMotivos') && $nt('ntwMotivos').addEventListener('click', function(ev){
  const b = ev.target.closest('[data-motivo]'); if(!b) return;
  const inp = $nt('ntMotivo'); if(!inp) return;
  inp.value = b.getAttribute('data-motivo');
  ntwRenderMotivos();
  marcarNtOk('fMotivoWrap');
  ntwRefresh();
});
$nt('ntMotivo') && $nt('ntMotivo').addEventListener('input', function(){
  const v = this.value.trim().toLowerCase();
  document.querySelectorAll('.ntw-motivo-chip').forEach(function(c){ c.classList.toggle('sel', c.getAttribute('data-motivo').toLowerCase() === v); });
  if(v) marcarNtOk('fMotivoWrap'); else limpiarNtErr('fMotivoWrap');
});

// ── Paso 2 · «Editar manualmente» (Retiros 2026-09-22: Empresa y RUT nacen
// como vitrina de la OT/documento/ERP; el botón es la única forma de
// destrabarlos; el resto de campos siempre se edita) ──
function ntwSetClienteManual(on){
  ntwClienteManual = !!on;
  ['ntEmpresa','ntRut'].forEach(function(id){
    const el = $nt(id); if(!el) return;
    if(ntwClienteManual){ el.removeAttribute('readonly'); el.classList.remove('ntw-locked'); }
    else { el.setAttribute('readonly',''); el.classList.add('ntw-locked'); }
  });
  const btn = $nt('ntwClienteManualBtn'), badge = $nt('ntwClienteManualBadge');
  if(btn){
    btn.classList.toggle('is-on', ntwClienteManual); btn.disabled = ntwClienteManual;
    btn.innerHTML = ntwClienteManual ? '<i class="bi bi-pencil-fill"></i>Edición manual activada' : '<i class="bi bi-pencil"></i>Editar manualmente';
  }
  if(badge) badge.style.display = ntwClienteManual ? '' : 'none';
}
$nt('ntwClienteManualBtn') && $nt('ntwClienteManualBtn').addEventListener('click', function(){
  ntwSetClienteManual(true);
  try{ $nt('ntEmpresa').focus(); }catch(_){}
  ntwRefresh();
});

// ── Chips «Detectado en ERP → usar» (Retiros): el correo NUNCA se precarga
// solo (feedback de Daniel: email del ERP = sugerencia); el resto se precarga
// si el campo está vacío y, si ya tenía otro valor, queda como chip. ──
const NTW_CHIP_LABEL = { ntEmpresa:'Razón social', ntRut:'RUT', ntContacto:'Contacto', ntPhone:'Teléfono', ntEmail:'Correo' };
function ntwProponerChip(campo, valor, origen){
  valor = String(valor||'').trim(); if(!valor) return;
  const inp = $nt(campo); if(!inp) return;
  const actual = String(inp.value||'').trim();
  if(campo === 'ntRut'){ if(ntwRutClave(actual) === ntwRutClave(valor)) return; }
  else if(actual.toLowerCase() === valor.toLowerCase()) return;
  ntwChips.set(campo+'|'+valor.toLowerCase(), { campo:campo, valor:valor, origen:origen||'ERP' });
  ntwRenderChips();
}
function ntwFlash(inp){ if(!inp) return; inp.classList.add('ntw-precargado'); setTimeout(function(){ inp.classList.remove('ntw-precargado'); }, 1800); }
function ntwPrecargarOChip(campo, valor, origen){
  valor = String(valor||'').trim(); if(!valor) return false;
  const inp = $nt(campo); if(!inp) return false;
  if(campo === 'ntEmail'){ ntwProponerChip(campo, valor, origen); return false; }
  if(!String(inp.value||'').trim()){
    inp.value = campo === 'ntRut' ? (ntwRutFormato(valor) || valor) : valor;
    ntwFlash(inp);
    if(campo === 'ntPhone') inp.dispatchEvent(new Event('blur'));
    if(campo === 'ntRut') ntwPintarRut();
    const wrap = { ntEmpresa:'fEmpresaWrap', ntRut:'fRutWrap', ntContacto:'fContactoWrap' }[campo];
    if(wrap){ limpiarNtErr(wrap); marcarNtOk(wrap); }
    return true;
  }
  ntwProponerChip(campo, valor, origen);
  return false;
}
function ntwRenderChips(){
  const cont = $nt('ntwCliChips'); if(!cont) return;
  Array.from(ntwChips.entries()).forEach(function(kv){
    const c = kv[1], inp = $nt(c.campo); if(!inp) return;
    const actual = String(inp.value||'').trim();
    const igual = c.campo === 'ntRut' ? ntwRutClave(actual) === ntwRutClave(c.valor) : actual.toLowerCase() === c.valor.toLowerCase();
    if(igual) ntwChips.delete(kv[0]);
  });
  cont.innerHTML = Array.from(ntwChips.entries()).map(function(kv){
    const c = kv[1];
    const val = c.campo === 'ntRut' ? (ntwRutFormato(c.valor) || c.valor) : c.valor;
    return '<button type="button" class="ntw-chip" data-chip="'+esc(kv[0])+'" title="Reemplaza el campo '+esc(NTW_CHIP_LABEL[c.campo]||c.campo)+'">'
      + '<i class="bi bi-cpu"></i><span class="lbl">'+esc(NTW_CHIP_LABEL[c.campo]||c.campo)+' detectado en ERP'+(c.origen ? ' <em>('+esc(c.origen)+')</em>' : '')+'</span>'
      + '<span class="val">'+esc(val)+'</span><span class="usar">→ usar</span></button>';
  }).join('');
}
$nt('ntwCliChips') && $nt('ntwCliChips').addEventListener('click', function(ev){
  const b = ev.target.closest('[data-chip]'); if(!b) return;
  const c = ntwChips.get(b.getAttribute('data-chip')); if(!c) return;
  const inp = $nt(c.campo); if(!inp) return;
  inp.value = c.campo === 'ntRut' ? (ntwRutFormato(c.valor) || c.valor) : c.valor;
  ntwChips.delete(b.getAttribute('data-chip'));
  ntwFlash(inp);
  if(c.campo === 'ntPhone' || c.campo === 'ntEmail') inp.dispatchEvent(new Event('blur'));
  if(c.campo === 'ntRut') ntwPintarRut();
  const wrap = { ntEmpresa:'fEmpresaWrap', ntRut:'fRutWrap', ntContacto:'fContactoWrap' }[c.campo];
  if(wrap){ limpiarNtErr(wrap); marcarNtOk(wrap); }
  ntwRenderChips();
  ntwRefresh();
});

// Datos de cliente que llegan de una OT, un documento o el ERP → se proponen
// al Paso 2 (precarga o chip, nunca pisan lo escrito). src = {empresa, rut,
// contacto, telefono, email, direccion, comuna, region}.
function ntwProponerCliente(src, origen){
  if(!src) return;
  ntwPrecargarOChip('ntEmpresa', src.empresa, origen);
  ntwPrecargarOChip('ntRut', src.rut, origen);
  ntwPrecargarOChip('ntContacto', src.contacto, origen);
  ntwPrecargarOChip('ntPhone', src.telefono, origen);
  ntwPrecargarOChip('ntEmail', src.email, origen);
  // Dirección: punto de partida, sigue editable/verificable con Google Places
  const dir = $nt('ntDireccion');
  if(dir && !dir.value.trim() && src.direccion){
    dir.value = src.direccion;
    if(src.comuna) $nt('ntComuna').value = src.comuna;
    if(src.region) $nt('ntRegion').value = src.region;
    $nt('ntDirHint').innerHTML = '<i class="bi bi-info-circle me-1"></i>Precargada desde '+esc(origen||'el ERP')+': elige la sugerencia de Google para validarla.';
    ntwFlash(dir);
  }
  if((src.empresa||'').trim()) document.querySelector('#ntBuscarCliWrap .nt-search').classList.add('has-tag');
  ntwRefresh();
}

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
  // Lo elegido en el buscador del ERP ES el cliente: pisa Empresa/RUT (son
  // la vitrina del ERP), igual que antes de Tickets 2.0.
  document.getElementById('ntRut').value = c.rut ? (ntwRutFormato(c.rut) || c.rut) : '';
  document.getElementById('ntEmpresa').value = c.empresa || '';
  ntwPintarRut();
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
  // en Contacto/Telefono/Correo, no se pisa). Tickets 2.0: el correo va como
  // chip «Detectado → usar», nunca se escribe solo.
  if(c.ya_es_cliente){
    ntwPrecargarOChip('ntContacto', c.contacto_nombre, 'ficha Clientes');
    ntwPrecargarOChip('ntPhone', c.contacto_tel, 'ficha Clientes');
    ntwPrecargarOChip('ntEmail', c.contacto_email, 'ficha Clientes');
    ilusToast('Este RUT ya tiene ficha en el módulo Clientes' + (c.cliente_id ? ' (#'+c.cliente_id+')' : '') + '.', {type:'info'});
  }
  ntwRenderChips();
  ntwRefresh();
}
// El RUT/Empresa quedan editables tras seleccionar — si el usuario los borra
// a mano, vuelven a marcarse como pendientes al validar (validarNtTodo).
['ntRut','ntEmpresa'].forEach(id=>document.getElementById(id).addEventListener('input', function(){
  document.querySelector('#ntBuscarCliWrap .nt-search').classList.remove('has-tag');
}));

// ── Validación RUT (dígito verificador) — avisa, no traba ──
function ntwPintarRut(){
  const inp = $nt('ntRut'), f = $nt('ntwVRut'); if(!inp || !f) return;
  const c = ntwRutLimpio(inp.value);
  inp.classList.remove('is-valid','is-invalid');
  if(!c){ f.className = 'nt-vfeed'; f.textContent = ''; return; }
  if(ntwRutValido(c)){ inp.classList.add('is-valid'); f.className = 'nt-vfeed ok'; f.innerHTML = '<i class="bi bi-check-circle-fill"></i> RUT válido'; }
  else if(c.length >= 8){ inp.classList.add('is-invalid'); f.className = 'nt-vfeed err'; f.innerHTML = '<i class="bi bi-exclamation-circle"></i> El dígito verificador no cuadra — revisa el RUT'; }
  else { f.className = 'nt-vfeed'; f.textContent = ''; }
}
$nt('ntRut').addEventListener('input', ntwPintarRut);
$nt('ntRut').addEventListener('blur', function(){
  if(ntwRutLimpio(this.value).length >= 2) this.value = ntwRutFormato(this.value);
  ntwPintarRut(); ntwRefresh();
});

// ── Validación teléfono chileno ──
function ntNormTel(v){ return String(v||'').replace(/[^\d]/g,'').replace(/^56/,''); }
function ntwTelOk(v){ const n = ntNormTel(v); return /^9\d{8}$/.test(n) || /^\d{8,9}$/.test(n); }
function ntwEmailOk(v){ return /^[^@\s]+@[^@\s]+\.[^@\s]{2,}$/.test(String(v||'').trim()); }
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
  if(ntwEmailOk(v)){ f.className='nt-vfeed ok'; f.innerHTML='<i class="bi bi-check-circle-fill"></i> Correo válido'; marcarNtOk('fEmailWrap'); }
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
      ntwRefresh();
    } });
}
ntInitDir();
// Si se edita la dirección a mano después de verificarla, la verificación cae
document.getElementById('ntDireccion').addEventListener('input', function(){
  if(document.getElementById('ntDirLat').value){
    document.getElementById('ntDirLat').value=''; document.getElementById('ntDirLng').value=''; document.getElementById('ntDirPlace').value='';
    document.getElementById('ntDirHint').innerHTML = '<i class="bi bi-info-circle me-1"></i>Selecciona una sugerencia para validar la dirección.';
  }
});

// ══════════════ PASO 1 · ¿De dónde viene? (OT existente o documento del ERP) ══════════════
// ── OT existente: buscador propio (GET /tickets/api/ot/buscar, solo lectura) ──
const ntwOtAc = $nt('ntwOtAc');
$nt('ntwBtnOt') && $nt('ntwBtnOt').addEventListener('click', function(){
  const box = $nt('ntwOtSearch'); if(!box) return;
  const abrir = box.style.display === 'none';
  box.style.display = abrir ? '' : 'none';
  this.classList.toggle('is-on', abrir);
  if(abrir){ try{ $nt('ntwOtQ').focus(); }catch(_){} }
});
const ntwBuscarOt = debounce(async function(){
  const q = ($nt('ntwOtQ').value||'').trim();
  if(q.length < 2){ ntwOtAc.classList.remove('open'); return; }
  ntwOtAc.innerHTML = '<div class="nt-ac-empty"><i class="bi bi-hourglass-split"></i>Buscando órdenes de trabajo…</div>'; ntwOtAc.classList.add('open');
  try{
    const r = await fetch('/tickets/api/ot/buscar?q='+encodeURIComponent(q));
    const d = await r.json();
    const rows = d.resultados || [];
    if(!d.ok){ ntwOtAc.innerHTML = '<div class="nt-ac-empty err"><i class="bi bi-exclamation-triangle"></i>'+esc(d.error||'No se pudo buscar')+'</div>'; return; }
    if(!rows.length){ ntwOtAc.innerHTML = '<div class="nt-ac-empty"><i class="bi bi-search"></i>Ninguna OT coincide con «'+esc(q)+'»</div>'; return; }
    ntwOtAc.innerHTML = rows.map(function(o,i){
      const cli = o.cliente || {};
      const meta = [o.tipo_label, o.estado_label, o.fecha ? 'Agendada '+o.fecha : '', o.equipos_n ? o.equipos_n+' equipo'+(o.equipos_n===1?'':'s') : ''].filter(Boolean).join(' · ');
      const lock = o.ticket_existente ? '<span class="lock"><i class="bi bi-lock-fill"></i> Ya tiene ticket '+esc(o.ticket_existente.numero_ticket||('#'+o.ticket_existente.id))+'</span>' : '';
      return '<div class="nt-ac-item" data-i="'+i+'"><div class="ntw-ot-item">'
        + '<span class="num">'+esc(o.numero_ot||('OT #'+o.id))+(o.titulo ? ' <span style="color:#374151;font-weight:600">· '+esc(o.titulo)+'</span>' : '')+'</span>'
        + '<span class="cli">'+esc(cli.razon_social||'Sin cliente (trabajo interno)')+(cli.rut ? ' <span style="color:#6b7280;font-weight:500">· '+esc(cli.rut)+'</span>' : '')+'</span>'
        + '<span class="meta">'+esc(meta)+(lock ? ' · '+lock : '')+'</span></div></div>';
    }).join('');
    ntwOtAc.querySelectorAll('.nt-ac-item').forEach(function(it){ it.addEventListener('click', function(){ ntwElegirOt(rows[+it.dataset.i]); }); });
  }catch(e){ ntwOtAc.innerHTML = '<div class="nt-ac-empty err"><i class="bi bi-wifi-off"></i>No se pudo consultar las OT</div>'; }
}, 320);
$nt('ntwOtQ') && $nt('ntwOtQ').addEventListener('input', ntwBuscarOt);

async function ntwElegirOt(o){
  ntwOtAc.classList.remove('open');
  if(o.ticket_existente){
    const num = o.ticket_existente.numero_ticket || ('#'+o.ticket_existente.id);
    const abrir = await ilusConfirm({
      title: 'Esta OT ya tiene ticket',
      message: (o.numero_ot||'La OT')+' ya está ligada al ticket '+num+'. Una OT solo puede tener un ticket.',
      sub: '¿Abrir ese ticket en otra pestaña? Si lo que necesitas es otro ticket para este cliente, créalo sin asociar la OT.',
      okLabel: 'Abrir '+num, cancelLabel: 'Volver', type: 'question',
    });
    if(abrir) window.open('/tickets/'+o.ticket_existente.id, '_blank', 'noopener');
    return;
  }
  try{
    const r = await fetch('/tickets/api/ot/'+encodeURIComponent(o.id));
    const d = await r.json();
    if(!d.ok || !d.ot){ ilusToast(d.error||'No se pudo leer la OT', {type:'error'}); return; }
    ntwAsociarOt(d.ot);
  }catch(e){ ilusToast('No se pudo leer la OT', {type:'error'}); }
}
function ntwAsociarOt(ot){
  if(ntwOt) ntwQuitarOt(true);
  ntwOt = ot;
  const origen = ot.numero_ot || ('OT #'+ot.id);
  (ot.equipos||[]).forEach(function(e){
    if(ntEquipos.some(function(x){ return x.origen==='ot' && x.maquina_id && x.maquina_id===e.maquina_id; })) return;
    ntEquipos.push({ sku:e.sku||'', nombre:e.nombre||'', serie:e.serie||'', maquina_id:e.maquina_id||null, notas:'', sel:true,
      origen:'ot', origenLabel:origen, tido:'', nudo:'', cantidad:1 });
  });
  (ot.documentos||[]).forEach(function(dc){
    if(!dc.tido || !dc.nudo) return;
    const k = String(dc.tido).toUpperCase()+'|'+String(dc.nudo);
    if(!ntwDocs.has(k)) ntwDocs.set(k, { tido:String(dc.tido).toUpperCase(), nudo:String(dc.nudo), cliente:(ot.cliente||{}).razon_social||'', rut:(ot.cliente||{}).rut||'', fecha:'', lineas:0, origen:'ot', origenLabel:origen });
  });
  const c = ot.cliente || {};
  ntwProponerCliente({ empresa:c.razon_social, rut:c.rut, contacto:c.contacto_nombre, telefono:c.contacto_tel, email:c.contacto_email,
    direccion:c.direccion, comuna:c.comuna, region:c.region }, origen);
  const box = $nt('ntwOtSearch'); if(box) box.style.display = 'none';
  $nt('ntwBtnOt').classList.remove('is-on'); $nt('ntwOtQ').value = '';
  ntwRenderOrigenes(); renderNtEquipos(); ntwRefresh();
  ilusToast('✓ '+origen+' asociada: '+(ot.equipos||[]).length+' equipo(s) y cliente cargados', {type:'success'});
  try{ $nt('ntwStep2').scrollIntoView({behavior:'smooth', block:'start'}); }catch(_){}
}
function ntwQuitarOt(silencioso){
  if(!ntwOt) return;
  for(let i = ntEquipos.length-1; i >= 0; i--){ if(ntEquipos[i].origen==='ot') ntEquipos.splice(i,1); }
  Array.from(ntwDocs.entries()).forEach(function(kv){ if(kv[1].origen==='ot') ntwDocs.delete(kv[0]); });
  const num = ntwOt.numero_ot; ntwOt = null;
  ntwRenderOrigenes(); renderNtEquipos(); ntwRefresh();
  if(!silencioso) ilusToast(num+' quitada del ticket (el cliente escrito se conserva)', {type:'info'});
}

// ── Documento del ERP: el buscador estándar (templates/tickets/_tka_modal.html)
// en modo 'seleccionar' → devuelve líneas con su documento + el encabezado ──
$nt('ntwBtnDoc') && $nt('ntwBtnDoc').addEventListener('click', function(){
  if(typeof window.tkaOpen !== 'function'){ ilusToast('El buscador de documentos del ERP no está disponible en esta pantalla.', {type:'error'}); return; }
  // tkaClose() libera document.body.style.overflow sin saber que este modal
  // Bootstrap sigue abierto detrás: se observa su cierre y se devuelve el
  // scroll-lock (mismo patrón que Retiros / templates/ot2/_modal_crear.html).
  const tkaEl = $nt('tkaModal');
  if(tkaEl && window.MutationObserver){
    const mo = new MutationObserver(function(){
      if(!tkaEl.classList.contains('is-open')){
        mo.disconnect();
        if(ntModalEl.classList.contains('show')) document.body.style.overflow = 'hidden';
      }
    });
    mo.observe(tkaEl, { attributes:true, attributeFilter:['class'] });
  }
  const rut = $nt('ntRut').value.trim();
  window.tkaOpen({
    mode: 'seleccionar',
    tabs: ['doc', 'cli'],
    rutPrefill: ntwRutValido(rut) ? rut : '',
    docsYaAgregados: Array.from(ntwDocs.values()).map(function(d){ return { tido:d.tido, nudo:d.nudo }; }),
    docsYaAgregadosLabel: 'Ya en este ticket',
    onSeleccionar: ntwOnSeleccionTka,
  });
});
async function ntwOnSeleccionTka(items, header){
  if(!items || !items.length) return;
  let nuevosDocs = 0, nuevosEq = 0;
  items.forEach(function(it){
    if(!it.tido || !it.nudo) return;
    const tido = String(it.tido).toUpperCase(), nudo = String(it.nudo).replace(/^0+/,'') || String(it.nudo);
    const k = tido+'|'+nudo;
    if(!ntwDocs.has(k)){
      const h = (header && String(header.nudo||'').replace(/^0+/,'') === nudo) ? header : {};
      ntwDocs.set(k, { tido:tido, nudo:nudo, cliente:h.cliente||'', rut:h.rut||'', fecha:h.fecha||'', lineas:0, origen:'erp', origenLabel:'ERP' });
      nuevosDocs++;
    }
    const doc = ntwDocs.get(k);
    const qty = Math.max(1, parseInt(it.qty||1, 10) || 1);
    if(it.sku && !ntEquipos.some(function(e){ return e.origen==='doc' && e.sku===it.sku && e.tido===tido && e.nudo===nudo; })){
      ntEquipos.push({ sku:it.sku||'', nombre:it.nombre||'', serie:'', notas:'', sel:true, origen:'doc', origenLabel:tido+' '+nudo, tido:tido, nudo:nudo, cantidad:qty });
      doc.lineas++; nuevosEq++;
    }
  });
  if(header && (header.cliente || header.rut)){
    ntwProponerCliente({ empresa:header.cliente, rut:header.rut, telefono:header.telefono, email:header.email,
      direccion:header.direccion, comuna:header.comuna }, (header.tido||'')+' '+(header.nudo||''));
  }
  ntwRenderOrigenes(); renderNtEquipos(); ntwRefresh();
  ilusToast('✓ '+nuevosDocs+' documento(s) y '+nuevosEq+' producto(s) agregados al ticket', {type:'success'});
}
function ntwQuitarDoc(key){
  const d = ntwDocs.get(key); if(!d) return;
  for(let i = ntEquipos.length-1; i >= 0; i--){ const e = ntEquipos[i]; if(e.origen==='doc' && e.tido===d.tido && e.nudo===d.nudo) ntEquipos.splice(i,1); }
  ntwDocs.delete(key);
  ntwRenderOrigenes(); renderNtEquipos(); ntwRefresh();
}
function ntwRenderOrigenes(){
  const wrap = $nt('ntwOrigenesWrap'), cont = $nt('ntwOrigenes'), cnt = $nt('ntwOrigenesCount'); if(!cont) return;
  const cards = [];
  if(ntwOt){
    const c = ntwOt.cliente || {};
    const meta = [ntwOt.tipo_label, ntwOt.estado_label, ntwOt.fecha ? 'Agendada '+ntwOt.fecha : ''].filter(Boolean).join(' · ');
    cards.push('<div class="ntw-doc-card"><div class="ntw-doc-num"><small>Orden de trabajo</small>'+esc(ntwOt.numero_ot||('OT #'+ntwOt.id))+'</div>'
      + '<div class="ntw-doc-meta"><span class="ddate">'+esc(meta)+'</span><span class="dcli">'+esc(c.razon_social||'Sin cliente (trabajo interno)')+'</span>'
      + '<span class="dlin">'+(ntwOt.titulo ? esc(ntwOt.titulo)+' · ' : '')+(ntwOt.equipos||[]).length+' equipo(s) · '+(ntwOt.documentos||[]).length+' documento(s)</span></div>'
      + '<span class="ntw-pill ot"><i class="bi bi-clipboard2-pulse"></i>OT</span>'
      + '<button type="button" class="ntw-doc-quitar" data-quitar-ot="1"><i class="bi bi-x-lg"></i>Quitar</button></div>');
  }
  ntwDocs.forEach(function(d, k){
    cards.push('<div class="ntw-doc-card"><div class="ntw-doc-num"><small>'+esc(d.tido)+'</small>'+esc(d.nudo)+'</div>'
      + '<div class="ntw-doc-meta">'+(d.fecha ? '<span class="ddate">'+esc(d.fecha)+'</span>' : '')+'<span class="dcli">'+esc(d.cliente||'—')+(d.rut ? ' · '+esc(d.rut) : '')+'</span>'
      + '<span class="dlin">'+(d.lineas ? d.lineas+' producto(s) elegido(s)' : (d.origen==='ot' ? 'Documento de la '+esc(d.origenLabel) : 'Sin productos elegidos'))+'</span></div>'
      + '<span class="ntw-pill '+(d.origen==='ot' ? 'ok' : 'ok')+'"><i class="bi bi-receipt"></i>'+(d.origen==='ot' ? 'Desde la OT' : 'ERP')+'</span>'
      + '<button type="button" class="ntw-doc-quitar" data-quitar-doc="'+esc(k)+'"><i class="bi bi-x-lg"></i>Quitar</button></div>');
  });
  cont.innerHTML = cards.join('');
  if(wrap) wrap.style.display = cards.length ? '' : 'none';
  if(cnt) cnt.textContent = String(cards.length);
  const bo = $nt('ntwBtnOtTxt'); if(bo) bo.textContent = ntwOt ? 'Cambiar la OT asociada' : 'Asociar una OT existente';
  const bd = $nt('ntwBtnDocTxt'); if(bd) bd.textContent = ntwDocs.size ? 'Agregar otro documento del ERP' : 'Agregar factura, boleta o nota de venta';
}
$nt('ntwOrigenes') && $nt('ntwOrigenes').addEventListener('click', function(ev){
  const bo = ev.target.closest('[data-quitar-ot]'); if(bo){ ntwQuitarOt(false); return; }
  const bd = ev.target.closest('[data-quitar-doc]'); if(bd){ ntwQuitarDoc(bd.getAttribute('data-quitar-doc')); }
});

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
  else ntEquipos.push({sku:p.sku||'', nombre:p.nombre||'', serie:'', notas:'', sel:true, origen:'manual', origenLabel:'Catálogo', tido:'', nudo:'', cantidad:1});
  ntAcEq.classList.remove('open'); document.getElementById('ntBuscarEq').value='';
  renderNtEquipos(); ntwRefresh();
}
function renderNtEquipos(){
  const l = document.getElementById('ntListaEq');
  // Contador visible (Daniel: "puede ser un equipo, veinte, cincuenta... tiene
  // que tener ese dinamismo") -- se actualiza en vivo con cada add/quitar.
  const counter = document.getElementById('ntEqCount');
  const n = ntEquipos.length, sel = ntEquipos.filter(e=>e.sel).length;
  counter.classList.toggle('show', n > 0);
  document.getElementById('ntEqCountTxt').textContent = n===0 ? '0 equipos agregados' : (sel+' de '+n+' producto'+(n===1?'':'s')+' marcado'+(sel===1?'':'s'));
  const tg = $nt('ntwEqToggleAll'); if(tg) tg.disabled = n === 0;
  if(!n){
    l.innerHTML = '<div class="ntw-empty"><i class="bi bi-box-seam"></i>Todavía no hay productos. Llegan solos desde la OT o el documento del Paso 1, o agrégalos abajo desde el catálogo.</div>';
    return;
  }
  // Cada equipo agregado lleva su propia observación (Daniel 2026-07-11: con
  // varias maquinas con problemas distintos, la descripcion general del
  // ticket no alcanza -- se necesita una nota por maquina). Nombres
  // COMPLETOS, nunca truncados (REGLA #15).
  l.innerHTML = ntEquipos.map((e,i)=>'<label class="ntw-eq-row'+(e.sel?' is-on':'')+'" data-i="'+i+'">'
    + '<input type="checkbox" class="chk" data-eq-chk="'+i+'"'+(e.sel?' checked':'')+' aria-label="Incluir '+esc(e.nombre)+'">'
    + '<div class="ntw-eq-body"><div class="ntw-eq-name">'+esc(e.nombre||e.sku||'Producto')+'</div>'
    + '<div class="ntw-eq-meta">'+(e.sku ? '<span class="sku">'+esc(e.sku)+'</span>' : '')
    + (e.serie ? '<span>Serie '+esc(e.serie)+'</span>' : '')
    + (e.cantidad > 1 ? '<span>× '+e.cantidad+'</span>' : '')
    + '<span class="ntw-pill '+(e.origen==='manual' ? '' : 'ok')+'">'+esc(e.origenLabel||'')+'</span></div>'
    + '<input type="text" class="form-control form-control-sm nt-eq-notas" data-i="'+i+'" placeholder="Observación de este equipo (opcional)" value="'+esc(e.notas||'')+'">'
    + '</div>'
    + '<button type="button" class="ntw-eq-quitar" data-eq-del="'+i+'" title="Quitar de la lista" aria-label="Quitar '+esc(e.nombre)+'"><i class="bi bi-x-lg"></i></button>'
    + '</label>').join('');
  l.querySelectorAll('.nt-eq-notas').forEach(function(inp){
    inp.addEventListener('click', function(ev){ ev.preventDefault(); ev.stopPropagation(); });
    inp.addEventListener('input', function(){
      const idx = +this.dataset.i;
      if(ntEquipos[idx]) ntEquipos[idx].notas = this.value;
    });
  });
}
window.ntDelEquipo = function(i){ ntEquipos.splice(i,1); renderNtEquipos(); ntwRefresh(); };
function ntwToggleEq(i, val){ if(!ntEquipos[i]) return; ntEquipos[i].sel = (typeof val === 'boolean') ? val : !ntEquipos[i].sel; }
$nt('ntListaEq').addEventListener('change', function(ev){
  const c = ev.target.closest('[data-eq-chk]'); if(!c) return;
  ntwToggleEq(+c.getAttribute('data-eq-chk'), c.checked);
  renderNtEquipos(); ntwRefresh();
});
$nt('ntListaEq').addEventListener('click', function(ev){
  const b = ev.target.closest('[data-eq-del]'); if(!b) return;
  ev.preventDefault(); window.ntDelEquipo(+b.getAttribute('data-eq-del'));
});
// REGLA #14: un solo toggle — si alguno está sin marcar, marca todos; si ya
// están todos, los desmarca. Pasa por el toggle individual de cada fila.
$nt('ntwEqToggleAll') && $nt('ntwEqToggleAll').addEventListener('click', function(){
  if(!ntEquipos.length) return;
  const marcarTodos = ntEquipos.some(e=>!e.sel);
  ntEquipos.forEach(function(_, i){ ntwToggleEq(i, marcarTodos); });
  renderNtEquipos(); ntwRefresh();
});

document.getElementById('ntDesc').addEventListener('input', function(){ document.getElementById('ntDescCount').textContent = this.value.length; });

document.addEventListener('click', function(e){
  if(!e.target.closest('#ntBuscarCli') && !e.target.closest('#ntAcCli')) ntAcCli.classList.remove('open');
  if(!e.target.closest('#ntBuscarEq') && !e.target.closest('#ntAcEq')) ntAcEq.classList.remove('open');
  if(ntwOtAc && !e.target.closest('#ntwOtQ') && !e.target.closest('#ntwOtAc')) ntwOtAc.classList.remove('open');
});

// ── Responsable en ILUS (Paso 5): /tickets/api/responsables; por defecto quien
// crea, si es responsable ──
async function ntwCargarResponsables(){
  if(ntwRespCargados) return;
  const sel = $nt('ntResp'); if(!sel) return;
  try{
    const r = await fetch('/tickets/api/responsables');
    const d = await r.json();
    const lista = (d && d.responsables) || [];
    sel.innerHTML = '<option value="">Sin asignar por ahora</option>' + lista.map(function(p){
      return '<option value="'+esc(p.nombre||'')+'">'+esc(p.nombre||'')+'</option>';
    }).join('');
    sel.dataset.yo = (d && d.soy_responsable && d.yo) ? d.yo : '';
    ntwRespCargados = true;
  }catch(e){
    sel.innerHTML = '<option value="">Sin asignar por ahora</option>';
  }
  ntwRespDefault();
  ntwRefresh();
}
function ntwRespDefault(){
  const sel = $nt('ntResp'); if(!sel) return;
  const yo = sel.dataset.yo || '';
  sel.value = yo;
  if(sel.value !== yo) sel.value = '';
  const f = $nt('ntwRespYo'); if(f) f.classList.toggle('show', !!(yo && sel.value === yo));
}
$nt('ntResp') && $nt('ntResp').addEventListener('change', function(){
  const f = $nt('ntwRespYo'); if(f) f.classList.toggle('show', !!(this.dataset.yo && this.value === this.dataset.yo));
});

// ══════════════ Semáforo de los 5 pasos (mismo evaluador que Retiros) ══════════════
function ntwVal(id){ const el = $nt(id); return el ? String(el.value||'').trim() : ''; }
function evalPaso1(){
  const r = { faltan:[], avisos:[], ok:'' };
  const partes = [];
  if(ntwOt) partes.push(ntwOt.numero_ot || ('OT #'+ntwOt.id));
  ntwDocs.forEach(function(d){ partes.push(d.tido+' '+d.nudo); });
  const aMano = ntwVal('ntDoc');
  if(partes.length) r.ok = 'Asociado: '+partes.join(' · ')+(aMano ? ' · a mano: '+aMano : '');
  else if(aMano) r.ok = 'Documento escrito a mano: '+aMano+' (sin cruce con el ERP)';
  else if(ntwEsInterno()) r.ok = 'Trabajo interno: no necesita OT ni documento';
  else r.avisos.push({ txt:'Sin OT ni documento: el cliente y los productos se escriben a mano (recomendado asociar uno)' });
  return r;
}
function evalPaso2(){
  const r = { faltan:[], avisos:[], ok:'' };
  const empresa = ntwVal('ntEmpresa'), rut = ntwVal('ntRut'), contacto = ntwVal('ntContacto');
  const tel = ntwVal('ntPhone'), mail = ntwVal('ntEmail'), dir = ntwVal('ntDireccion'), lat = ntwVal('ntDirLat');
  if(ntwEsInterno()){
    r.ok = empresa ? 'Cliente opcional (trabajo interno): '+empresa : 'Cliente opcional para un trabajo interno';
    if(rut && !ntwRutValido(rut)) r.avisos.push({ txt:'El RUT no cuadra (dígito verificador)' });
    return r;
  }
  if(!empresa) r.faltan.push('empresa');
  if(!rut) r.faltan.push('RUT');
  else if(!ntwRutValido(rut)) r.avisos.push({ txt:'El RUT no cuadra (dígito verificador): revísalo' });
  if(!contacto) r.faltan.push('nombre de contacto');
  if(!tel) r.faltan.push('teléfono'); else if(!ntwTelOk(tel)) r.faltan.push('teléfono válido (9 dígitos)');
  if(!mail) r.faltan.push('correo'); else if(!ntwEmailOk(mail)) r.faltan.push('correo válido');
  if(!dir) r.faltan.push('dirección del equipo'); else if(!lat) r.faltan.push('dirección verificada con Google (elige una sugerencia)');
  if(ntwChips.size) r.avisos.push({ txt: ntwChips.size+' dato'+(ntwChips.size===1?'':'s')+' detectado'+(ntwChips.size===1?'':'s')+' en el ERP sin usar (toca el chip para usarlo)' });
  if(!r.faltan.length) r.ok = empresa+' · '+ntwRutFormato(rut)+' · '+contacto;
  return r;
}
function evalPaso3(){
  const r = { faltan:[], avisos:[], ok:'' };
  const n = ntEquipos.length, sel = ntEquipos.filter(e=>e.sel).length;
  if(!n){ r.ok = 'Sin productos (opcional)'; return r; }
  if(!sel){ r.avisos.push({ txt: n+' producto'+(n===1?'':'s')+' en la lista y ninguno marcado: marca los que corresponden o quítalos' }); return r; }
  r.ok = sel+' de '+n+' producto'+(n===1?'':'s')+' marcado'+(sel===1?'':'s')+' para el ticket';
  return r;
}
function evalPaso4(){
  const r = { faltan:[], avisos:[], ok:'' };
  const motivo = ntwVal('ntMotivo'), desc = ntwVal('ntDesc');
  if(!ntTipoSel) r.faltan.push('tipo de solicitud');
  if(!motivo) r.faltan.push('motivo principal');
  if(!desc) r.faltan.push('descripción de qué está pasando');
  else if(desc.length < 15) r.avisos.push({ txt:'La descripción es muy corta: agrega el detalle que te dio el cliente' });
  if(!r.faltan.length) r.ok = (TIPO_LABEL[ntTipoSel]||ntTipoSel)+' · '+motivo+(ntEsGarantia ? ' · garantía' : '');
  return r;
}
function evalPaso5(){
  const r = { faltan:[], avisos:[], ok:'' };
  const prio = ntwVal('ntPrio'), resp = ntwVal('ntResp');
  if(!resp) r.avisos.push({ txt:'Sin responsable: el ticket quedará en «Sin asignar» de la bandeja' });
  r.ok = 'Prioridad '+prio+(resp ? ' · Responsable: '+resp : '')+(ntArchivos.length ? ' · '+ntArchivos.length+' adjunto'+(ntArchivos.length===1?'':'s') : '');
  return r;
}
function ntwPintarPaso(idx, r){
  const card = $nt('ntwStep'+idx), linea = $nt('ntwEstado'+idx);
  const estado = r.faltan.length ? 'rojo' : (r.avisos.length ? 'ambar' : 'verde');
  r.idx = idx; r.estado = estado;
  if(card){ card.classList.toggle('is-complete', estado==='verde'); card.classList.toggle('is-warn', estado==='ambar'); }
  if(linea){
    let txt;
    if(estado==='rojo') txt = 'Falta: '+r.faltan.join(' · ')+(r.avisos.length ? ' · '+r.avisos.map(a=>a.txt).join(' · ') : '');
    else if(estado==='ambar') txt = r.avisos.map(a=>a.txt).join(' · ');
    else txt = r.ok;
    linea.innerHTML = txt ? '<i class="bi '+NTW_ICONO_ESTADO[estado]+'"></i><span>'+esc(txt)+'</span>' : '';
  }
  const contAvisos = idx===1 ? $nt('ntwOrigenAvisos') : (idx===2 ? $nt('ntwCliAvisos') : null);
  if(contAvisos){
    const html = r.avisos.filter(a=>a.html).map(a=>a.html).join('');
    if(contAvisos.innerHTML !== html) contAvisos.innerHTML = html;
  }
}
function ntwPintarProgreso(res){
  let verdes = 0, rojos = 0, avisos = 0;
  res.forEach(function(r){ if(r.estado==='verde') verdes++; if(r.estado==='rojo') rojos++; avisos += r.avisos.length; });
  const txt = $nt('ntwProgresoTxt'), bar = $nt('ntwProgresoBar'), av = $nt('ntwProgresoAvisos'), btn = $nt('ntBtnCrear');
  if(txt) txt.textContent = verdes+' de '+res.length+' pasos listos';
  if(bar) bar.style.width = Math.round(verdes/Math.max(1,res.length)*100)+'%';
  if(av) av.textContent = avisos ? '· '+avisos+' aviso'+(avisos===1?'':'s') : '';
  // Daniel: el botón principal queda deshabilitado hasta que esté completo
  if(btn && !btn.dataset.enviando){
    btn.disabled = rojos > 0;
    btn.title = rojos > 0 ? 'Completa los pasos en rojo para crear el ticket' : 'Crear el ticket';
  }
}
function ntwRefresh(){
  const evals = [evalPaso1, evalPaso2, evalPaso3, evalPaso4, evalPaso5];
  const res = evals.map(function(fn, i){
    let r;
    try{ r = fn(); }catch(e){ console.warn('[ntw] evalPaso'+(i+1), e); r = { faltan:[], avisos:[], ok:'' }; }
    ntwPintarPaso(i+1, r);
    return r;
  });
  ntwPintarProgreso(res);
  ntwUltimaEval = res;
  return res;
}
window.ntwRefresh = ntwRefresh;
// Cualquier tecleo o cambio dentro del modal re-evalúa el semáforo
const ntwRefreshDeb = debounce(ntwRefresh, 80);
ntModalEl.addEventListener('input', ntwRefreshDeb);
ntModalEl.addEventListener('change', ntwRefreshDeb);

// Pre-chequeo de «Crear Ticket»: rojo → al primer paso que falta; ámbar → confirmar
async function ntwPreCrear(){
  const res = ntwRefresh();
  const rojos = res.filter(r=>r.estado==='rojo');
  if(rojos.length){
    const card = $nt('ntwStep'+rojos[0].idx);
    if(card){
      try{ card.scrollIntoView({behavior:'smooth', block:'center'}); }catch(_){ card.scrollIntoView(); }
      card.classList.remove('ntw-flash'); void card.offsetWidth; card.classList.add('ntw-flash');
      setTimeout(function(){ card.classList.remove('ntw-flash'); }, 1300);
    }
    ilusToast('Falta: '+rojos.map(r=>'Paso '+r.idx+' → '+r.faltan.join(', ')).join(' · '), {type:'warning'});
    return false;
  }
  const avisos = [];
  res.forEach(function(r){ r.avisos.forEach(function(a){ avisos.push('Paso '+r.idx+': '+a.txt); }); });
  if(!avisos.length) return true;
  return !!(await ilusConfirm({
    title: 'Hay '+avisos.length+' advertencia'+(avisos.length===1?'':'s'),
    message: '¿Crear el ticket igual?',
    sub: avisos.map(a=>'<div>• '+esc(a)+'</div>').join(''),
    subHtml: true,
    okLabel: 'Crear igual', cancelLabel: 'Revisar',
    type: 'warning',
  }));
}

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

  const motivoVal = ntwVal('ntMotivo');
  if(!motivoVal){ marcarNtErr('fMotivoWrap'); ok=false; } else marcarNtOk('fMotivoWrap');

  const descVal = document.getElementById('ntDesc').value.trim();
  if(!descVal){ marcarNtErr('fDescWrap'); ok=false; } else marcarNtOk('fDescWrap');

  return ok;
}

function ntwArmarPayload(conOt){
  const seleccionados = ntEquipos.filter(e=>e.sel);
  const docsTxt = Array.from(ntwDocs.values()).map(d=>d.tido+'-'+d.nudo).join(', ');
  const resp = ntwVal('ntResp');
  return {
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
    numero_documento: document.getElementById('ntDoc').value.trim() || docsTxt,
    producto: seleccionados.map(e=>e.nombre).join(' · '),
    sku: seleccionados.map(e=>e.sku).filter(Boolean).join(', '),
    descripcion: document.getElementById('ntDesc').value.trim(),
    // Tickets 2.0
    motivo_principal: ntwVal('ntMotivo'),
    visita_id: (conOt && ntwOt) ? ntwOt.id : null,
    asignado_a: resp,
    sin_asignar: resp ? 0 : 1,
    documentos: Array.from(ntwDocs.values()).map(d=>({ tido:d.tido, nudo:d.nudo })),
    equipos: seleccionados.map(e=>({ kopr:e.sku, nombre:e.nombre, sku:e.sku, notas:e.notas||'', cantidad:e.cantidad||1,
      tido:e.tido||'', nudo:e.nudo||'', serie:e.serie||'' })),
  };
}

document.getElementById('ntBtnCrear').addEventListener('click', async function(){
  if(!(await ntwPreCrear())) return;
  if(!validarNtTodo()){
    ilusToast('Revisa los campos marcados en rojo','warning');
    const firstErr = document.querySelector('#ntModal .nt-field.err, #ntModal #fTipoWrap.err');
    if(firstErr) firstErr.scrollIntoView({behavior:'smooth', block:'center'});
    return;
  }
  const btn = this; btn.disabled = true; btn.dataset.enviando = '1';
  let conOt = true;
  try{
    let d;
    for(let intento = 0; intento < 2; intento++){
      const r = await fetch('/tickets/api/tickets', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(ntwArmarPayload(conOt))});
      d = await r.json();
      if(!d.ok && d.error_codigo === 'OT_YA_TIENE_TICKET' && conOt){
        const seguir = await ilusConfirm({
          title: 'La OT ya tiene ticket',
          message: d.error || 'Esa OT ya está ligada a otro ticket.',
          sub: '¿Crear este ticket SIN asociar la OT? El cliente y los productos ya cargados se conservan.',
          okLabel: 'Crear sin la OT', cancelLabel: 'Revisar', type: 'warning',
        });
        if(!seguir){ btn.disabled=false; delete btn.dataset.enviando; return; }
        conOt = false; continue;
      }
      break;
    }
    if(!d.ok){ ilusToast(d.error||'Error al crear','error'); btn.disabled=false; delete btn.dataset.enviando; return; }
    ilusToast('✓ '+(d.numero_ticket||'Ticket')+' creado','success');
    ntModal.hide();
    if (ntArchivos.length){
      await ntSubirArchivos(d.id);
    }
    setTimeout(()=>location.href='/tickets/'+d.id, 400);
  }catch(e){ ilusToast('Error de red','error'); btn.disabled=false; delete btn.dataset.enviando; }
});
// Primer semáforo (el modal puede abrirse antes del primer input)
ntwRenderOrigenes(); ntwRenderMotivos(); renderNtEquipos(); ntwSetClienteManual(false); ntwRefresh();

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
/* ═══════════════════════════════════════════════════════════════════
   ACTUALIZAR LA BANDEJA (2026-09-03, Daniel: "un boton donde podamos
   actualizar los tickets que estan en el correo... actualmente hay
   tickets que estan saltandose").

   Una sola llamada hace las dos cosas que antes eran dos botones:
   leer el buzon de verdad y revisar documentos nuevos de instalacion.
   Las dos son idempotentes, asi que apretar de mas no hace dano.

   El resultado se cuenta COMPLETO, incluido lo que no salio bien: si el
   barrido se corto por tiempo o el ERP no respondio, se dice -- un
   'listo' cuando quedaron correos afuera es justo lo que hace que
   alguien crea que ya reviso y no vuelva.
   ═══════════════════════════════════════════════════════════════════ */
/* 📊 2026-09-03 (Daniel: "me gustaria que tuviera una rayita de proceso,
   una barra, para saber cuanto le falta, y que me de un informe de cuantos
   tickets actualizo").

   HONESTIDAD SOBRE LA BARRA: el servidor hace UNA sola llamada y no
   informa avance parcial, asi que un porcentaje real no existe. Poner un
   numero inventado subiendo al 90% seria mentir sobre algo que no sabemos.
   Lo que SI sabemos es cuanto suele demorar, asi que la barra avanza por
   ETAPAS declaradas ("leyendo el buzon", "revisando documentos") y se
   queda esperando en la ultima hasta que llega la respuesta -- no finge
   precision que no tiene. El informe de abajo, ese si es exacto: son los
   numeros que devuelve el servidor. */
function _tkBarraProgreso(){
  let caja = document.getElementById('tkActBarra');
  if (!caja){
    caja = document.createElement('div');
    caja.id = 'tkActBarra';
    caja.style.cssText =
      'position:fixed;left:50%;top:18px;transform:translateX(-50%);z-index:2000;' +
      'min-width:min(420px,92vw);background:#0a0a0a;color:#fff;border-radius:14px;' +
      'padding:14px 18px;box-shadow:0 18px 48px -12px rgba(0,0,0,.7);' +
      'border:1px solid rgba(255,255,255,.12);font-size:.85rem';
    caja.innerHTML =
      '<div style="display:flex;align-items:center;gap:10px;margin-bottom:9px">' +
        '<i class="bi bi-envelope-arrow-down" style="color:#dc2626;font-size:1.1rem"></i>' +
        '<b style="flex:1">Actualizando la bandeja</b>' +
        '<span id="tkActPaso" style="font-size:.74rem;color:#a1a1aa"></span>' +
      '</div>' +
      '<div style="height:7px;border-radius:7px;background:rgba(255,255,255,.12);overflow:hidden">' +
        '<div id="tkActFill" style="height:100%;width:0%;border-radius:7px;' +
          'background:linear-gradient(90deg,#dc2626,#f87171);' +
          'transition:width .5s cubic-bezier(.4,0,.2,1)"></div>' +
      '</div>' +
      '<div id="tkActDet" style="font-size:.72rem;color:#a1a1aa;margin-top:7px"></div>';
    document.body.appendChild(caja);
  }
  caja.style.display = 'block';
  const fill = document.getElementById('tkActFill');
  const paso = document.getElementById('tkActPaso');
  const det  = document.getElementById('tkActDet');
  return {
    etapa(pct, txt, sub){
      if (fill) fill.style.width = pct + '%';
      if (paso) paso.textContent = pct + '%';
      if (det)  det.textContent = sub || txt || '';
    },
    cerrar(){ caja.style.display = 'none'; },
  };
}

const btnActualizarBandeja = document.getElementById('btnActualizarBandeja');
if (btnActualizarBandeja){
  btnActualizarBandeja.addEventListener('click', async function(){
    btnActualizarBandeja.disabled = true;
    const _orig = btnActualizarBandeja.innerHTML;
    btnActualizarBandeja.innerHTML =
      '<span class="spinner-border spinner-border-sm me-1"></span>Actualizando…';
    const barra = _tkBarraProgreso();
    barra.etapa(8, '', 'Conectando con el buzón de soporte…');
    // Las etapas se marcan por tiempo esperado, no por avance real del
    // servidor -- ver el comentario de arriba. Si la respuesta llega antes,
    // se cancelan y la barra salta al 100%.
    const _pasos = [
      [1200, 30, 'Leyendo los correos nuevos…'],
      [4000, 55, 'Creando tickets y enganchando respuestas…'],
      [9000, 78, 'Revisando documentos nuevos del ERP…'],
      [16000, 90, 'Casi listo — el buzón tenía bastante que revisar…'],
    ];
    const _timers = _pasos.map(([ms, pct, txt]) =>
      setTimeout(() => barra.etapa(pct, '', txt), ms));
    const _limpiar = () => _timers.forEach(clearTimeout);
    try {
      const r = await fetch('/tickets/api/actualizar', {
        method:'POST',
        headers:{'Content-Type':'application/json','X-Requested-With':'XMLHttpRequest'},
        body: JSON.stringify({dias: 15, documentos: true}),
      });
      let d = null;
      try { d = await r.json(); } catch(_){ d = null; }
      if (!d){
        _limpiar(); barra.cerrar();
        await ilusAlert({type:'error', title:'Sin respuesta del servidor',
          message:'No sabemos si alcanzó a revisar el buzón. Recarga y vuelve a intentar.'});
        return;
      }
      if (!d.ok && !(d.correo && d.correo.ok)){
        _limpiar(); barra.cerrar();
        await ilusAlert({type:'error', title:'No se pudo actualizar',
          message: d.error || (d.correo && d.correo.error) || 'El buzón no respondió.'});
        return;
      }
      const c = d.correo || {}, doc = d.documentos || {};
      const nuevos  = c.auto_creados || 0;      // tickets creados desde un correo
      const mensajes= c.ingresados || 0;        // respuestas enganchadas a su ticket
      const revisados = c.candidatos || 0;
      const docNuevos = (doc.tickets_creados || []).length;
      const partes = [];
      partes.push(revisados + ' correo' + (revisados===1?'':'s') + ' revisado' + (revisados===1?'':'s'));
      if (nuevos)   partes.push('<b>' + nuevos + '</b> ticket' + (nuevos===1?'':'s') + ' nuevo' + (nuevos===1?'':'s') + ' desde el correo');
      if (mensajes) partes.push('<b>' + mensajes + '</b> respuesta' + (mensajes===1?'':'s') + ' enganchada' + (mensajes===1?'':'s'));
      if (docNuevos) partes.push('<b>' + docNuevos + '</b> ticket' + (docNuevos===1?'':'s') + ' de instalacion desde el ERP');
      const avisos = (d.avisos || []);
      const hubo = nuevos + mensajes + docNuevos;
      _limpiar();
      barra.etapa(100, '', 'Listo');
      await new Promise(r => setTimeout(r, 420));
      barra.cerrar();
      /* El informe que pidió Daniel: exacto, no aproximado. Se listan
         también los ceros -- "0 tickets nuevos" es información: dice que
         el buzón se revisó y no había nada, que es distinto de que no se
         haya revisado. */
      const _fila = (ico, n, txt, color) =>
        '<div style="display:flex;align-items:center;gap:9px;padding:5px 0">' +
          '<i class="bi bi-' + ico + '" style="color:' + color + ';width:18px"></i>' +
          '<b style="min-width:34px;text-align:right;font-variant-numeric:tabular-nums">' + n + '</b>' +
          '<span>' + txt + '</span></div>';
      const informe =
        _fila('envelope', revisados, 'correos revisados en el buzón', '#93c5fd') +
        _fila('ticket-detailed', nuevos, 'tickets nuevos creados desde un correo',
              nuevos ? '#4ade80' : '#71717a') +
        _fila('chat-left-text', mensajes, 'respuestas de clientes enganchadas a su ticket',
              mensajes ? '#4ade80' : '#71717a') +
        _fila('receipt', docNuevos, 'tickets de instalación desde documentos del ERP',
              docNuevos ? '#4ade80' : '#71717a');
      if (avisos.length){
        await ilusAlert({
          type: hubo ? 'warning' : 'error',
          title: hubo ? 'Actualizado, pero quedó algo pendiente' : 'No se pudo completar',
          message: 'Esto es lo que alcanzó a hacer:',
          sub: informe + '<div style="margin-top:9px;padding-top:9px;' +
               'border-top:1px dashed rgba(0,0,0,.15)">' +
               avisos.map(function(a){ return '⚠ ' + a; }).join('<br>') + '</div>',
          subHtml: true,
        });
      } else {
        await ilusAlert({
          type: hubo ? 'success' : 'info',
          title: hubo ? 'Bandeja al día' : 'Revisado — no había nada nuevo',
          message: hubo
            ? 'Esto es lo que entró:'
            : 'Se revisó el buzón completo y todo lo que había ya estaba en la bandeja.',
          sub: informe, subHtml: true,
        });
      }
      if (hubo && typeof cargarTickets === 'function') cargarTickets();
    } catch(e){
      _limpiar(); barra.cerrar();
      await ilusAlert({type:'error', title:'Error de conexión',
        message:'No se pudo contactar al servidor: ' + (e.message || e)});
    } finally {
      _limpiar(); barra.cerrar();
      btnActualizarBandeja.disabled = false;
      btnActualizarBandeja.innerHTML = _orig;
    }
  });
}

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

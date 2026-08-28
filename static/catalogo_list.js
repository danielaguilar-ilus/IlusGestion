// 2026-07-23 (Daniel, incidente en vivo: "sigo sin ver los cambios... queda
// cargando"): guardián INDEPENDIENTE del fetch de cargarCatalogo(). El
// AbortController de 12s dentro de cargarCatalogo() ya cubre el caso "el
// fetch nunca resuelve ni rechaza" -- pero si por lo que sea el hilo
// principal queda ocupado (carga en frío, muchos recursos compitiendo) y
// ese setTimeout interno no llega a dispararse a tiempo, o si el script
// grande de más abajo ni siquiera llega a ejecutar cargarCatalogo(), este
// script chico y AISLADO (se ejecuta en cuanto el parser llega aquí, antes
// del resto de la página) queda armado igual y no depende de nada de eso.
// 2026-07-23 (2): cargarCatalogo() ahora se reintenta SOLA una vez si la
// primera demora es por timeout (instancia de Cloud Run recién levantada --
// Daniel eligió esto en vez de pagar una segunda instancia siempre lista),
// así que el ciclo interno normal puede tardar hasta ~24s (2x12s) antes de
// mostrar su propio aviso. Este guardián queda a 28s para nunca disparar
// ANTES que el reintento automático interno tenga su oportunidad -- sigue
// siendo el respaldo de último recurso si ni siquiera ese reintento llega a
// correr (hilo principal realmente trabado).
(function(){
  setTimeout(function(){
    var el = document.getElementById('catLista');
    if (el && el.querySelector('.bi-hourglass-split')){
      el.innerHTML = '<div class="empty-state"><i class="bi bi-wifi-off"></i>'
        + 'La carga está tardando demasiado — revisa tu conexión o un bloqueador de anuncios/extensión, y '
        + '<button type="button" class="btn btn-sm btn-outline-secondary ms-1" '
        + 'onclick="if(typeof cargarCatalogo===\'function\'){cargarCatalogo();}else{location.reload();}">Reintentar</button></div>';
    }
  }, 28000);
})();

// 2026-07-14 (Daniel: "no entiendo por que [la varita magica] solamente
// deja hacerla al superadministrador, agregale los roles"): mismo criterio
// que el gate backend _catalogo_required (catalogo_module.py) -- permiso
// 'mantenciones' O superadmin. Úsalo SOLO para decidir si se muestra el
// botón/wizard de la varita mágica (catwAbrir), NO reemplaza a
// IS_SUPERADMIN en el resto del archivo (ficha completa, eliminar
// definitivo, etc. siguen siendo solo-superadmin -- _catalogo_admin_required
// intacto en el backend, ver catwAbrir() para el detalle de qué pasos del
// wizard quedan visibles/editables según el rol).
// 2026-07-30: valores inyectados por Jinja movidos a window.CAT_LIST_DATA
// (bootstrap inline en templates/catalogo/list.html), ya que este archivo
// pasó a ser estático (no pasa por el motor de plantillas).
const IS_SUPERADMIN = window.CAT_LIST_DATA.isSuperadmin;
const CAN_CARGAR_PIOLAS = window.CAT_LIST_DATA.canCargarPiolas;
// 2026-07-21 (Daniel, "acciones unificadas"): "eliminarlo solamente para el
// superadministrador con opciones a agregarlo en los roles" -- flag granular
// nuevo (matriz /admin/roles, módulo "catalogo" -> acción "eliminar", ver
// PERMISSIONS_MATRIX en app.py). Nace en False para todos los roles hasta
// que Daniel lo prenda ahí; superadmin siempre lo tiene. Gatea SOLO el
// archivar/soft-delete (botón "Eliminar producto" del footer de la ficha) --
// el hard-delete definitivo sigue exclusivo de IS_SUPERADMIN, sin cambios.
const CAN_ELIMINAR_CATALOGO = window.CAT_LIST_DATA.canEliminarCatalogo;
// 2026-08-25 (Daniel, dictado): vio a un técnico descargar un manual y lo
// consideró delicado -- flag granular nuevo (matriz /admin/roles, módulo
// "catalogo" -> acción "descargar_manual"). Nace en False para todos los
// roles hasta que Daniel lo prenda; superadmin siempre lo tiene. Gatea
// SOLO el botón "Descargar" (guarda el PDF en el equipo) -- "Ver" (visor
// embebido, catvAbrir) sigue abierto a cualquiera con acceso al módulo,
// y desde ahí se puede imprimir directo con el visor del navegador.
const CAN_DESCARGAR_MANUAL = window.CAT_LIST_DATA.canDescargarManual;
const MAX_FOTOS = 10;
const MAX_PIOLAS = 10;  // 2026-07-21 (Daniel): vuelve a 10 (ver MAX_PIOLAS_POR_PRODUCTO en catalogo_module.py)
let _catTimer = null;
let _catfPiolas = [];

// ── Estado de la bandeja (orden / paginacion) ──
let catSort = 'updated_at';
let catDir = 'desc';
let catPage = 1;
let catLimit = 50;
let catVerArchivados = false;
// 2026-07-23 (Daniel, insiste: "necesito que me muestre todos los
// productos trabajados... así tenga al menos una de las tres
// solicitudes"): default ON -- oculta los SKU 100% en blanco que se
// auto-crean al pasar por una cotización, para que la lista muestre lo
// que de verdad tiene trabajo encima. Se puede apagar (checkbox) para
// auditar/ver todo.
let catSoloTrabajados = true;
let _catCache = {};          // id → producto de la última carga
let _catFamilias = new Set(); // acumulado best-effort para el <select> Familia
                               // (el contrato no define un endpoint de "familias
                               // distintas" — se completa con lo que se va viendo).
let _catClases = null;        // cache de GET /catalogo/api/clases (se pide 1 sola vez)

// ── Estado de carga/refresco (2026-07-23, Daniel: "no me agrada que la
// tabla deba recargar") ──
// true desde la PRIMERA vez que cargarCatalogo() renderizó con éxito
// (filas o estado-vacío legítimo). A partir de ahí, un fallo de red JAMÁS
// vuelve a destruir #catLista: la tabla vieja queda intacta + toast.
let _catUltimaCargaOk = false;
// Guardia de carrera (mismo patrón que _wizPrecioSeq en cotizaciones.html):
// cada llamada toma ++_catSeq; solo la más reciente puede pintar el DOM.
let _catSeq = 0;
// AbortController de la petición EN VUELO: una llamada nueva aborta la
// anterior de inmediato (no tiene sentido esperarle 12s a una respuesta
// que igual será descartada por _catSeq).
let _catCtrlVuelo = null;

// ── Selección múltiple para borrado masivo (2026-07-24, Daniel: "quiero
// tener un checkbox... para seleccionar y borrar rápido... como super
// administrador") -- solo se renderiza para IS_SUPERADMIN (ver
// catTheadHtml/catFilaHtml). Se limpia si el usuario cambia filtro/orden/
// página (_catUltimaFirma distinto), pero NO en un simple refresco de la
// misma vista (botón Actualizar / reintento automático) -- mismo espíritu
// que el resto de esta pantalla: nunca perder trabajo en curso por un
// refresco de fondo.
let _catSeleccionados = new Set();
let _catUltimaFirma = null;

// ── Clases de producto: fetch único + cache, reusado en filtro + wizard ──
async function catObtenerClases(){
  if(_catClases) return _catClases;
  // Mismo resguardo de timeout que cargarCatalogo() (2026-07-23) -- un fetch
  // colgado sin resolver ni rechazar dejaría el <select> de Clase vacío
  // para siempre sin ningún aviso.
  const _ctrl = new AbortController();
  const _timeoutId = setTimeout(() => _ctrl.abort(), 12000);
  try{
    const r = await fetch('/catalogo/api/clases', {signal: _ctrl.signal});
    const d = await r.json();
    _catClases = (d.ok && d.clases) ? d.clases : [];
  }catch(e){ _catClases = []; }
  finally{ clearTimeout(_timeoutId); }
  return _catClases;
}
async function catPoblarSelectClase(){
  const sel = document.getElementById('catFClase');
  if(sel.dataset.poblado) return;
  const clases = await catObtenerClases();
  sel.innerHTML = '<option value="">Todas</option>'
    + clases.map(c=>'<option value="'+esc(c.value)+'">'+esc(c.label)+'</option>').join('');
  sel.dataset.poblado = '1';
}
catPoblarSelectClase();

function esc(s){ return (s==null?'':String(s))
  .replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')
  .replace(/"/g,'&quot;').replace(/'/g,'&#39;'); }
function debounce(fn,ms){ let t; return function(){ clearTimeout(t); const a=arguments,c=this; t=setTimeout(()=>fn.apply(c,a),ms); }; }
function catRefrescoIndicador(on){
  const bar = document.getElementById('catRefrescoBar');
  if (bar) bar.classList.toggle('on', !!on);
  const btn = document.getElementById('catBtnActualizar');
  if (btn) btn.disabled = !!on;
}
// El servidor ya envía las fechas formateadas en hora Chile (Regla #6).
function fmtFecha(s){ return s || '—'; }
function fmtKb(kb){
  if(kb==null) return '';
  return kb >= 1024 ? (kb/1024).toFixed(1)+' MB' : Math.round(kb)+' KB';
}

// ── Filtros → URLSearchParams ──
function catParamsFiltros(conOrden){
  const p = new URLSearchParams();
  const q = document.getElementById('catFQ').value.trim(); if(q) p.set('q', q);
  const fam = document.getElementById('catFFamilia').value; if(fam) p.set('familia', fam);
  const clase = document.getElementById('catFClase').value; if(clase) p.set('clase_producto', clase);
  p.set('activo', catVerArchivados ? '0' : '1');
  p.set('solo_trabajados', catSoloTrabajados ? '1' : '0');
  if(conOrden && catSort){ p.set('sort', catSort); p.set('dir', catDir); }
  return p;
}

const CAT_COLS = [
  ['Foto',''], ['SKU','sku'], ['Nombre','nombre'], ['Familia','familia'], ['Clase','clase_producto'],
  ['Fotos','total_fotos'], ['Manual',''], ['Estado',''], ['Actualizado','updated_at'], ['Acciones',''],
];
function catTheadHtml(){
  // 2026-07-24 (Daniel, voz: "quiero tener un checkbox... para
  // seleccionar y borrar rápido... como super administrador"): columna
  // de selección SOLO para superadmin -- para cualquier otro rol la
  // tabla se ve exactamente igual que antes, cero cambio visual.
  const chkTh = IS_SUPERADMIN
    ? '<th class="cat-th-chk"><input type="checkbox" id="catChkTodos" title="Seleccionar todos los visibles" onclick="catToggleSeleccionarTodo(this.checked)"></th>'
    : '';
  return '<tr>' + chkTh + CAT_COLS.map(function(c){
    if(!c[1]) return '<th>'+c[0]+'</th>';
    let cls = 'cat-th-sort', ic = 'bi-arrow-down-up';
    if(catSort === c[1]){ cls += ' on'; ic = (catDir === 'asc' ? 'bi-sort-up' : 'bi-sort-down'); }
    return '<th class="'+cls+'" onclick="catSortClick(\''+c[1]+'\')" title="Ordenar por '+c[0]+'">'
      + c[0] + ' <i class="bi '+ic+'"></i></th>';
  }).join('') + '</tr>';
}
window.catSortClick = function(col){
  if(catSort !== col){ catSort = col; catDir = 'desc'; }
  else if(catDir === 'desc'){ catDir = 'asc'; }
  else { catSort = ''; catDir = 'desc'; }
  catPage = 1; cargarCatalogo();
};

function catFooterHtml(data){
  const total = data.total||0, page = data.page||catPage, pages = data.pages||0,
        limit = data.limit||catLimit;
  const desde = total ? ((page-1)*limit+1) : 0;
  const hasta = Math.min(page*limit, total);
  return '<div class="cat-foot">'
    + '<div class="cat-foot-info">Mostrando <b>'+desde+' - '+hasta+'</b> de <b>'+total+'</b> producto'+(total===1?'':'s')+'</div>'
    + '<div class="cat-foot-ctrl">'
    +   '<label class="lbl-pp">Por página '
    +     '<select onchange="catCambiarLimit(this.value)">'
    +       [10,25,50,100].map(n=>'<option value="'+n+'"'+(n===catLimit?' selected':'')+'>'+n+'</option>').join('')
    +     '</select></label>'
    +   '<button type="button" class="cat-pg-btn" '+(page<=1?'disabled':'')+' onclick="catPagina(-1)" title="Página anterior"><i class="bi bi-chevron-left"></i></button>'
    +   '<span class="cat-pg-num">'+page+' / '+Math.max(pages,1)+'</span>'
    +   '<button type="button" class="cat-pg-btn" '+(page>=pages?'disabled':'')+' onclick="catPagina(1)" title="Página siguiente"><i class="bi bi-chevron-right"></i></button>'
    + '</div></div>';
}
window.catPagina = function(d){ catPage = Math.max(1, catPage + d); cargarCatalogo(); };
window.catCambiarLimit = function(v){ catLimit = parseInt(v,10)||50; catPage = 1; cargarCatalogo(); };

async function cargarCatalogo(_reintentoAuto){
  // ── Guardia de carrera: esta llamada toma su turno y mata a la anterior ──
  const miSeq = ++_catSeq;
  if (_catCtrlVuelo){ try{ _catCtrlVuelo.abort(); }catch(_e){} }
  const esRefresco = _catUltimaCargaOk;   // ¿ya hay datos buenos en pantalla?
  catRefrescoIndicador(true);

  const p = catParamsFiltros(true);
  p.set('page', String(catPage));
  p.set('limit', String(catLimit));
  // Filtro/orden/página distinto de la última consulta -> las filas que se
  // ven ya no son las mismas, la selección de borrado masivo pierde
  // sentido. Un refresco de la MISMA vista (Actualizar / reintento auto)
  // no cambia la firma -- no borra lo que el usuario ya venía marcando.
  const _firma = p.toString();
  if (_catUltimaFirma !== null && _catUltimaFirma !== _firma) _catSeleccionados.clear();
  _catUltimaFirma = _firma;
  let data;
  // 2026-07-23 (Daniel: "queda puro cargando y no pasa nada"): sin timeout,
  // si el fetch se queda colgado (bloqueador de anuncios/extensión filtrando
  // "/api/productos", proxy, red inestable) la promesa nunca resuelve NI
  // rechaza -- el catch de abajo nunca se alcanza y "Cargando…" queda pegado
  // para siempre, sin ningún error visible. AbortController fuerza un límite
  // de 12s: pasado ese tiempo, se cancela y se muestra un error accionable
  // en vez de girar eternamente.
  const _ctrl = new AbortController();
  _catCtrlVuelo = _ctrl;
  const _timeoutId = setTimeout(() => _ctrl.abort(), 12000);
  try{
    try{
      const r = await fetch('/catalogo/api/productos?'+p.toString(), {signal: _ctrl.signal});
      data = await r.json();
    }catch(err){
      clearTimeout(_timeoutId);
      // Respuesta superada por una llamada más nueva (incluye el abort por
      // supersede de arriba): morir en silencio, la nueva es quien manda.
      if (miSeq !== _catSeq) return;
      const esTimeout = err && err.name === 'AbortError';
      if (esRefresco){
        // Había datos buenos en pantalla: NO tocar #catLista. La tabla
        // vieja sigue siendo válida; solo avisar y dejar el reintento
        // manual (botón Actualizar de la toolbar).
        ilusToast('No se pudo actualizar el catálogo — sigues viendo los datos anteriores. Usa “Actualizar” para reintentar.', {type:'warning'});
        return;
      }
      // 2026-07-23 (Daniel: la demora real es una instancia de Cloud Run
      // recién levantada -- decidió NO pagar una segunda instancia siempre
      // lista, "mejor ahorrarnos ese dinero" -- que la página se reintente
      // SOLA en su lugar). Si es la primera demora (todavía no es un
      // reintento automático), la página se recarga sola una vez sin
      // mostrarle ningún error a Daniel: para cuando este segundo intento
      // sale, la instancia ya está tibia y casi siempre responde rápido.
      // Solo si el reintento automático TAMBIÉN se cuelga se muestra el
      // aviso con el botón manual.
      if(esTimeout && !_reintentoAuto){
        return cargarCatalogo(true);
      }
      document.getElementById('catLista').innerHTML =
        '<div class="empty-state"><i class="bi bi-wifi-off"></i>'
        + (esTimeout
            ? 'La carga está tardando demasiado — revisa tu conexión o un bloqueador de anuncios/extensión, y '
            : 'No se pudo cargar. ')
        + '<button type="button" class="btn btn-sm btn-outline-secondary ms-1" onclick="cargarCatalogo()">Reintentar</button></div>';
      return;
    }
    clearTimeout(_timeoutId);
    if (miSeq !== _catSeq) return;  // respuesta vieja: descartada
    if (!data.ok){
      if (esRefresco){ ilusToast(data.error||'Error al actualizar — sigues viendo los datos anteriores.', {type:'warning'}); return; }
      // Primera carga con error del backend: panel completo (antes esto
      // dejaba el "Cargando…" pegado hasta el guardián de 28s).
      document.getElementById('catLista').innerHTML =
        '<div class="empty-state"><i class="bi bi-wifi-off"></i>No se pudo cargar. '
        + '<button type="button" class="btn btn-sm btn-outline-secondary ms-1" onclick="cargarCatalogo()">Reintentar</button></div>';
      return;
    }

    // 2026-07-23 (Daniel: "necesito que me muestre todos los productos
    // trabajados"): transparencia -- si "Solo trabajados" ocultó filas,
    // decirlo explícito (nunca esconder un filtro en silencio).
    const hintEl = document.getElementById('catTrabajadosHint');
    if(hintEl){
      const ocultos = (data.solo_trabajados && data.total_sin_filtro > data.total)
        ? (data.total_sin_filtro - data.total) : 0;
      hintEl.textContent = ocultos > 0
        ? `${data.total} trabajado${data.total===1?'':'s'} · ${ocultos} sin clasificación/foto/manual (ocultos)`
        : '';
    }

    if(!data.rows.length && (data.total||0) > 0 && catPage > 1){
      catPage = Math.max(1, data.pages||1);
      return cargarCatalogo();   // la recursión toma su propio ++_catSeq
    }

    _catCache = {};
    (data.rows||[]).forEach(function(row){
      _catCache[row.id] = row;
      if(row.familia) _catFamilias.add(row.familia);
    });
    catPoblarSelectFamilia();

    const cont = document.getElementById('catLista');
    if(!data.rows.length){
      const hayOcultosPorFiltro = data.solo_trabajados && (data.total_sin_filtro||0) > 0;
      cont.innerHTML = '<div class="empty-state"><i class="bi bi-inbox"></i>No hay productos con estos filtros.'
        + (hayOcultosPorFiltro
            ? ' Tienes ' + data.total_sin_filtro + ' producto(s) sin clasificación/foto/manual — '
              + '<button type="button" class="btn btn-sm btn-outline-secondary ms-1" onclick="document.getElementById(\'catSoloTrabajados\').click()">Ver todos</button>'
            : '')
        + '</div>';
      _catUltimaCargaOk = true;   // vacío legítimo = carga exitosa
      return;
    }
    cont.innerHTML = '<div class="cat-table-wrap table-responsive"><table class="cat-table"><thead>'
      + catTheadHtml()
      + '</thead><tbody>'
      + data.rows.map(catFilaHtml).join('')
      + '</tbody></table></div>'
      + catFooterHtml(data);
    _catUltimaCargaOk = true;
  } finally {
    // Solo la llamada más reciente apaga el indicador; si esta llamada fue
    // superada (o se relanzó a sí misma: reintento auto / corrección de
    // página), la nueva sigue en vuelo y la barra debe seguir encendida.
    if (miSeq === _catSeq) catRefrescoIndicador(false);
  }
}

function catPoblarSelectFamilia(){
  const sel = document.getElementById('catFFamilia');
  const actual = sel.value;
  const opts = Array.from(_catFamilias).sort((a,b)=>a.localeCompare(b));
  sel.innerHTML = '<option value="">Todas</option>'
    + opts.map(f=>'<option value="'+esc(f)+'">'+esc(f)+'</option>').join('');
  sel.value = actual;
  document.getElementById('catnFamiliaList').innerHTML = opts.map(f=>'<option value="'+esc(f)+'">').join('');
}

function catFilaHtml(row){
  const foto = row.foto_thumb_url
    ? '<img class="cat-thumb" src="'+esc(row.foto_thumb_url)+'" alt="">'
    : '<div class="cat-thumb-ph"><i class="bi bi-image"></i></div>';
  const totalFotos = row.total_fotos || 0;
  const badgeFotos = '<span class="cat-badge-fotos'+(totalFotos>=MAX_FOTOS?' full':'')+'">'+totalFotos+'/'+MAX_FOTOS+'</span>';
  const manualIc = row.tiene_manual
    ? '<i class="bi bi-file-earmark-pdf-fill cat-manual-ic on" title="Tiene manual"></i>'
    : '<i class="bi bi-file-earmark cat-manual-ic off" title="Sin manual"></i>';
  const badgeReg = row.registrado
    ? '<span class="cat-badge-reg si"><i class="bi bi-check-circle-fill"></i>Registrado</span>'
    : '<span class="cat-badge-reg no"><i class="bi bi-exclamation-circle-fill"></i>Pendiente</span>';
  const clase = row.clase_producto_label
    ? '<span class="cat-badge-clase"><i class="bi bi-tag-fill"></i>'+esc(row.clase_producto_label)+'</span>'
    : '<span class="text-muted">—</span>';
  const chkTd = IS_SUPERADMIN
    ? '<td onclick="event.stopPropagation();"><input type="checkbox" class="cat-chk-fila" data-id="'+row.id+'"'
      + (_catSeleccionados.has(row.id)?' checked':'') + ' onchange="catToggleSeleccion('+row.id+',this.checked)"></td>'
    : '';
  return '<tr class="'+(row.activo===0?'archivado':'')+(_catSeleccionados.has(row.id)?' seleccionada':'')+'" onclick="catAbrirFicha('+row.id+')">'
    + chkTd
    + '<td>'+foto+'</td>'
    + '<td><span class="cat-sku">'+esc(row.sku)+'</span></td>'
    + '<td><div class="cat-nombre">'+esc(row.nombre)+'</div></td>'
    + '<td>'+(row.familia?esc(row.familia):'<span class="text-muted">—</span>')+'</td>'
    + '<td>'+clase+'</td>'
    + '<td>'+badgeFotos+'</td>'
    + '<td>'+manualIc+'</td>'
    + '<td>'+badgeReg+'</td>'
    + '<td>'+esc(fmtFecha(row.updated_at))+'</td>'
    + '<td>'+catAccionesCelda(row)+'</td>'
    + '</tr>';
}

// 2026-07-21 (Daniel, dictado): "En las acciones lo que quiero es un único
// punto de entrada. Verlo, editarlo, eliminarlo, pero solamente eliminarlo
// para el superadministrador con opciones a agregarlo en los roles" --
// antes había hasta 3 botones por fila (ojo=ver ficha, varita=wizard
// guiado, tacho=eliminar). Se fusionan en UN solo botón por fila: abre
// SIEMPRE la ficha (#catfModal), que ya es la superficie única de
// ver+editar (inline, sin modo "solo lectura" artificial) y que ya trae
// "Eliminar producto" en su footer (gateado a superadmin más abajo, ver
// catfCargar). El wizard guiado (#catwModal) NO se borra (Regla #4.2) --
// sigue disponible: si el producto está incompleto, la propia ficha ofrece
// un aviso con botón "Continuar con el asistente guiado" (ver catfCargar).
function catAccionesCelda(row){
  const titulo = row.registrado ? 'Ver / editar ficha' : 'Ver ficha (registro incompleto)';
  return '<div class="cat-acciones">'
    + '<button type="button" class="cat-act ver" title="'+titulo+'"'
    + ' onclick="event.stopPropagation();catAbrirFicha('+row.id+')"><i class="bi bi-eye"></i></button>'
    + '</div>';
}

// ══════════════ Selección múltiple + borrado masivo (2026-07-24) ══════════════
// Daniel (voz): "quiero tener un checkbox... para seleccionar y borrar
// rápido, en caso de que quiera... como super administrador". Solo se
// renderiza para IS_SUPERADMIN (catTheadHtml/catFilaHtml) y el backend
// (cat_api_bulk_eliminar) vuelve a exigir superadmin igual -- nunca confiar
// solo en que el botón esté oculto en el frontend.
window.catToggleSeleccion = function(id, checked){
  if(checked) _catSeleccionados.add(id); else _catSeleccionados.delete(id);
  const tr = document.querySelector('.cat-chk-fila[data-id="'+id+'"]')?.closest('tr');
  if(tr) tr.classList.toggle('seleccionada', checked);
  const todos = document.getElementById('catChkTodos');
  if(todos){
    const visibles = document.querySelectorAll('.cat-chk-fila').length;
    const marcadosVisibles = document.querySelectorAll('.cat-chk-fila:checked').length;
    todos.checked = visibles > 0 && marcadosVisibles === visibles;
    todos.indeterminate = marcadosVisibles > 0 && marcadosVisibles < visibles;
  }
  catRenderBarraSeleccion();
};
window.catToggleSeleccionarTodo = function(checked){
  document.querySelectorAll('.cat-chk-fila').forEach(function(chk){
    const id = parseInt(chk.dataset.id, 10);
    chk.checked = checked;
    if(checked) _catSeleccionados.add(id); else _catSeleccionados.delete(id);
    const tr = chk.closest('tr');
    if(tr) tr.classList.toggle('seleccionada', checked);
  });
  catRenderBarraSeleccion();
};
function catRenderBarraSeleccion(){
  const bar = document.getElementById('catBarraSeleccion');
  if(!bar) return;   // no-superadmin: el bloque ni existe en el HTML
  const n = _catSeleccionados.size;
  if(!n){ bar.style.display = 'none'; bar.innerHTML = ''; return; }
  bar.style.display = 'flex';
  bar.innerHTML = '<i class="bi bi-check-square-fill"></i>'
    + '<span class="cnt">'+n+'</span> producto'+(n===1?'':'s')+' seleccionado'+(n===1?'':'s')
    + '<span class="spacer"></span>'
    + '<button type="button" class="btn btn-sm btn-outline-light" onclick="catToggleSeleccionarTodo(false)">Quitar selección</button>'
    + '<button type="button" class="btn btn-sm btn-danger" onclick="catBorrarSeleccionados()"><i class="bi bi-trash me-1"></i>Eliminar seleccionados</button>';
}
window.catBorrarSeleccionados = async function(){
  const ids = Array.from(_catSeleccionados);
  if(!ids.length) return;
  const ok = await ilusConfirm({
    title: 'Eliminar '+ids.length+' producto'+(ids.length===1?'':'s'),
    message: '¿Archivar '+ids.length+' producto'+(ids.length===1?'':'s')+' del catálogo?',
    sub: 'Se archivan (soft-delete), no se borran para siempre — dejan de listarse pero quedan en la base de datos. Puedes restaurarlos activando "Ver archivados" y editándolos uno por uno si hiciera falta.',
    okLabel: 'Eliminar', cancelLabel: 'Cancelar', danger: true,
  });
  if(!ok) return;
  try{
    const r = await fetch('/catalogo/api/productos/bulk-eliminar', {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ids: ids}),
    });
    const d = await r.json();
    if(!d.ok){ ilusToast(d.error || 'No se pudo eliminar', {type:'error'}); return; }
    ilusToast('✓ ' + d.eliminados + ' producto'+(d.eliminados===1?'':'s')+' eliminado'+(d.eliminados===1?'':'s'), {type:'success'});
    _catSeleccionados.clear();
    catRenderBarraSeleccion();
    cargarCatalogo();
  }catch(e){ ilusToast('Sin conexión', {type:'error'}); }
};

// ── Filtros / toolbar ──
function catFiltroCambio(){ catPage = 1; cargarCatalogo(); }
function catDebounced(){ clearTimeout(_catTimer); _catTimer = setTimeout(catFiltroCambio, 350); }
document.getElementById('catFQ').addEventListener('input', catDebounced);
document.getElementById('catFFamilia').addEventListener('change', catFiltroCambio);
document.getElementById('catFClase').addEventListener('change', catFiltroCambio);
document.getElementById('catVerArchivados').addEventListener('change', function(){
  catVerArchivados = this.checked; catFiltroCambio();
});
document.getElementById('catSoloTrabajados').addEventListener('change', function(){
  catSoloTrabajados = this.checked; catFiltroCambio();
});
document.getElementById('catBtnActualizar').addEventListener('click', function(){
  cargarCatalogo();   // NO resetea catPage: refresca exactamente la vista actual
});
document.getElementById('catBtnLimpiar').addEventListener('click', function(){
  document.getElementById('catFQ').value = '';
  document.getElementById('catFFamilia').value = '';
  document.getElementById('catFClase').value = '';
  document.getElementById('catVerArchivados').checked = false;
  catVerArchivados = false;
  document.getElementById('catSoloTrabajados').checked = true;
  catSoloTrabajados = true;
  catSort = 'updated_at'; catDir = 'desc'; catPage = 1;
  cargarCatalogo();
});

// ══════════════ Sincronizar bodega desde ERP (2026-07-12, Daniel:
// "carga la bodega 02 sin los servicios ZZ, ya es necesario") ══════════════
const btnSyncErp = document.getElementById('btnSyncErp');
if (btnSyncErp){
  btnSyncErp.addEventListener('click', async function(){
    const original = btnSyncErp.innerHTML;
    btnSyncErp.disabled = true;
    btnSyncErp.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Sincronizando…';
    try{
      const r = await fetch('/catalogo/api/sync-erp', {
        method: 'POST', headers: {'Content-Type':'application/json'},
        body: JSON.stringify({limit: 5000}),
      });
      const d = await r.json();
      if (!d.ok){ ilusToast(d.error || 'No se pudo sincronizar', {type:'error'}); return; }
      if (d.creados > 0){
        ilusToast('✓ ' + d.creados + ' producto(s) nuevo(s) traídos de la bodega', {type:'success'});
        cargarCatalogo();
      } else {
        ilusToast('El catálogo ya tenía todos los productos de la bodega (sin novedades).', {type:'info'});
      }
    }catch(e){ ilusToast('Error de red al sincronizar', {type:'error'}); }
    finally{ btnSyncErp.disabled = false; btnSyncErp.innerHTML = original; }
  });
}

// ══════════════ Fotos desde ecommerce (2026-07-14, Daniel: "tráelos
// automáticos y déjalo vacío si da error") ══════════════
// Recorre los productos activos SIN foto y les busca la imagen en la
// tienda ilusfitness.com (match exacto por SKU). El botón solo se
// renderiza para superadmin (Jinja); el backend igual lo gatea.
const btnFotosEcom = document.getElementById('btnFotosEcom');
if (btnFotosEcom){
  btnFotosEcom.addEventListener('click', async function(){
    const original = btnFotosEcom.innerHTML;
    btnFotosEcom.disabled = true;
    btnFotosEcom.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Buscando fotos…';
    try{
      const r = await fetch('/catalogo/api/fotos-desde-ecommerce', {
        method: 'POST', headers: {'Content-Type':'application/json'},
        body: JSON.stringify({}),
      });
      const d = await r.json();
      if (!d.ok){ ilusToast(d.error || 'No se pudo buscar las fotos', {type:'error'}); return; }
      let msg = '';
      if (d.con_foto) msg += '✓ ' + d.con_foto + ' producto(s) con foto nueva';
      if (d.sin_match) msg += (msg ? ' · ' : '') + d.sin_match + ' sin foto en la tienda';
      if (d.errores) msg += (msg ? ' · ' : '') + d.errores + ' con error';
      if (!msg) msg = 'No había productos sin foto pendientes';
      if (d.restantes > 0) msg += ' · quedan ' + d.restantes + ', vuelve a presionar';
      ilusToast(msg, {type: d.con_foto ? 'success' : 'info'});
      if (d.con_foto) cargarCatalogo();
    }catch(e){ ilusToast('Error de red al buscar las fotos', {type:'error'}); }
    finally{ btnFotosEcom.disabled = false; btnFotosEcom.innerHTML = original; }
  });
}

// ══════════════ Buscar en ERP (puntual) — 2026-07-12 (Daniel: "en vez de
// sincronizar el RP completo, llamemos al formulario/modal y hagamos
// búsquedas por SKU, búsqueda intermedia, búsquedas por documento... para
// ir a agregarle los productos que están con piola. Cambiemos esa
// modalidad para agregarlo, y después que lo agregue, en el mismo
// formulario poner cantidad de piolas y las medidas."):
//   1. Abre el modal compartido _tka_modal.html (tickets/_tka_modal.html,
//      mode:'seleccionar' -- genérico, no toca nada de Tickets).
//      Ver comentario de tkaOpen() en ese archivo para el contrato exacto.
//   2. Por cada item seleccionado: POST /catalogo/api/productos/desde-erp
//      (crea si no existe; si ya existe, no duplica -- Regla #4.2).
//   3. Encola los productos (nuevos o existentes) y abre el wizard de
//      piolas (catwAbrir, ya construido) para el primero; al finalizar
//      cada wizard, se abre automáticamente el siguiente de la cola.
// ══════════════════════════════════════════════════════════════════════
window._catwColaERP = [];
function _catwAbrirSiguienteDeCola(){
  if (!window._catwColaERP || !window._catwColaERP.length) return;
  const next = window._catwColaERP.shift();
  setTimeout(function(){ catwAbrir(next.id); }, 350); // esperar a que el modal previo termine su transición de cierre
}
const btnBuscarErp = document.getElementById('btnBuscarErp');
if (btnBuscarErp){
  btnBuscarErp.addEventListener('click', function(){
    tkaOpen({
      mode: 'seleccionar',
      // 2026-07-14 (Daniel): Bodega 02 primera pestaña visual Y activa por
      // defecto al abrir desde Catálogo -- _tkaApplyTabsFilter() en
      // _tka_modal.html usa tabs[0] tanto para el orden visual (CSS order)
      // como para la pestaña activa por defecto.
      tabs: ['bodega', 'doc', 'cli'],
      onSeleccionar: async function(items){
        if (!items || !items.length){ return; }
        let creados = 0, existentes = 0, errores = 0;
        const cola = [];
        for (const it of items){
          try{
            const r = await fetch('/catalogo/api/productos/desde-erp', {
              method: 'POST', headers: {'Content-Type':'application/json'},
              body: JSON.stringify({ sku: it.sku, nombre: it.nombre })
            });
            const d = await r.json();
            if (!d.ok || !d.id){ errores++; continue; }
            if (d.creado) creados++; else existentes++;
            cola.push({ id: d.id, sku: it.sku });
          }catch(e){ errores++; }
        }
        let msg = '';
        if (creados) msg += '✓ ' + creados + ' producto(s) agregado(s) al catálogo';
        if (existentes) msg += (msg ? ' · ' : '') + existentes + ' ya existían';
        if (errores) msg += (msg ? ' · ' : '') + errores + ' con error';
        if (msg) ilusToast(msg, {type: errores && !creados && !existentes ? 'error' : 'success'});
        cargarCatalogo();
        if (cola.length){
          window._catwColaERP = cola;
          _catwAbrirSiguienteDeCola();
        }
      }
    });
  });
}

// ══════════════ Modal: Nuevo producto ══════════════
document.getElementById('catnBtnCrear').addEventListener('click', async function(){
  const sku = document.getElementById('catnSku').value.trim().toUpperCase();
  const nombre = document.getElementById('catnNombre').value.trim();
  if(!sku){ ilusToast('El SKU es obligatorio', {type:'warning'}); return; }
  if(!nombre){ ilusToast('El nombre es obligatorio', {type:'warning'}); return; }
  const payload = {
    sku: sku, nombre: nombre,
    familia: document.getElementById('catnFamilia').value.trim(),
    observacion: document.getElementById('catnObservacion').value.trim(),
  };
  const btn = this; const original = btn.innerHTML;
  btn.disabled = true; btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Creando…';
  try{
    const r = await fetch('/catalogo/api/productos', {method:'POST',
      headers:{'Content-Type':'application/json'}, body:JSON.stringify(payload)});
    const d = await r.json();
    btn.disabled = false; btn.innerHTML = original;
    if(d.ok){
      document.getElementById('catnSku').value = '';
      document.getElementById('catnNombre').value = '';
      document.getElementById('catnFamilia').value = '';
      document.getElementById('catnObservacion').value = '';
      if (typeof bootstrap !== 'undefined') bootstrap.Modal.getOrCreateInstance(document.getElementById('catnModal')).hide();
      ilusToast('✓ Producto creado', {type:'success'});
      cargarCatalogo();
    } else ilusToast(d.error||'No se pudo crear el producto', {type:'error'});
  }catch(e){ btn.disabled = false; btn.innerHTML = original; ilusToast('Sin conexión', {type:'error'}); }
});

// 2026-07-24 (bug real en vivo: "catAbrirFicha is not defined" -- la
// ficha del producto no abría con NADA, ni clic en la fila ni en el ícono
// de ver). Causa raíz: bootstrap (la librería, cargada desde un CDN
// externo -- cdn.jsdelivr.net) a veces no está lista todavía cuando el
// parser llega a este <script> inline, y "new bootstrap.Modal(...)" a
// nivel superior (fuera de cualquier función) tira ReferenceError ahí
// mismo -- eso corta en seco TODO el resto del script que viene después
// en el archivo (catAbrirFicha, el wizard completo, el visor de PDF, el
// historial de piolas), aunque lo de ANTES de esta línea (buscador,
// filtros, la tabla misma) siga funcionando bien. _bsModalLazy() nunca
// toca "bootstrap" al definirse -- solo la primera vez que de verdad se
// llama a .show()/.hide() (un clic real del usuario, momento en el que
// el CDN ya tuvo tiempo de sobra para cargar) -- así esta línea nunca
// puede volver a tumbar el resto del script.
function _bsModalLazy(el){
  let _inst = null;
  function _get(){ return _inst || (_inst = bootstrap.Modal.getOrCreateInstance(el)); }
  return { show: function(){ _get().show(); }, hide: function(){ _get().hide(); } };
}

// ══════════════ Modal: Ficha de producto ══════════════
let catfProductoId = null;
let catfProductoActual = null;
const catfModalEl = document.getElementById('catfModal');
const catfModal = _bsModalLazy(catfModalEl);

window.catAbrirFicha = async function(id){
  catfProductoId = id;
  catfModal.show();
  await catfCargar();
};

async function catfCargar(){
  let d;
  try{
    const r = await fetch('/catalogo/api/productos/'+catfProductoId);
    d = await r.json();
  }catch(e){ ilusToast('No se pudo cargar el producto', {type:'error'}); return; }
  if(!d.ok){ ilusToast(d.error||'Error al cargar el producto', {type:'error'}); return; }
  const p = d.producto;
  catfProductoActual = p;

  // Aviso de registro incompleto -- punto de entrada al asistente guiado
  // (ver comentario junto a #catfAvisoIncompleto más arriba).
  document.getElementById('catfAvisoIncompleto').classList.toggle('d-none', !!p.registrado);

  document.getElementById('catfNombre').textContent = p.nombre || p.sku;
  document.getElementById('catfSkuTag').textContent = p.sku || '';
  document.getElementById('catfValSku').textContent = p.sku || '—';
  document.getElementById('catfValNombre').textContent = p.nombre || '—';
  document.getElementById('catfValFamilia').textContent = p.familia || '—';
  document.getElementById('catfValClaseProducto').textContent = p.clase_producto_label || '—';
  document.getElementById('catfValObservacion').textContent = p.observacion || '—';
  const badgeClase = document.getElementById('catfBadgeClase');
  if(p.clase_producto_label){ badgeClase.textContent = p.clase_producto_label; badgeClase.style.display = 'inline-flex'; }
  else { badgeClase.style.display = 'none'; }

  // Fotos
  const fotos = d.fotos || [];
  document.getElementById('catfFotosBadge').textContent = fotos.length + '/' + MAX_FOTOS;
  const grid = document.getElementById('catfFotosGrid');
  grid.innerHTML = fotos.map(function(f){
    return '<div class="catf-foto" data-foto-id="'+f.id+'">'
      + '<img src="'+esc(f.url)+'" alt="Foto de '+esc(p.nombre||p.sku)+'">'
      + '<button type="button" class="x" title="Eliminar foto" onclick="catfEliminarFoto('+f.id+')"><i class="bi bi-x-lg"></i></button>'
      + '</div>';
  }).join('') + (fotos.length < MAX_FOTOS
    ? '<div class="catf-foto-add" id="catfFotoAdd"><i class="bi bi-camera-plus"></i><span>Agregar foto</span></div>'
    : '');
  const addBtn = document.getElementById('catfFotoAdd');
  if(addBtn) addBtn.addEventListener('click', function(){ document.getElementById('catfFotoInput').click(); });
  grid.querySelectorAll('.catf-foto img').forEach(function(img, i){
    img.addEventListener('click', function(){
      // Visor de zoom reutilizado de foto_editor.js (canEdit/canDelete=false:
      // el catálogo NO tiene endpoint "replace" en el contrato, así que aquí
      // solo se usa como visor — agregar/eliminar corre por los botones propios
      // de esta ficha, contra las rutas EXACTAS del contrato).
      ilusFotos.view({
        key: 'catalogo-'+catfProductoId,
        photo: fotos[i],
        canEdit: false, canDelete: false,
      });
    });
  });

  // Manual
  const manualWrap = document.getElementById('catfManualWrap');
  if(d.manual && d.manual.tiene){
    // 2026-08-25 (Daniel, dictado): vio a un técnico descargar el manual y
    // lo consideró delicado -- "Descargar" ahora es CAN_DESCARGAR_MANUAL
    // (permiso editable en /admin/roles), no un botón fijo para todos.
    // "Ver" sigue abierto: el visor embebido (catvAbrir) deja imprimir
    // directo desde la pantalla sin bajar el archivo. "Quitar" se alinea
    // con IS_SUPERADMIN, mismo permiso que ya exige el backend (DELETE es
    // _catalogo_admin_required) -- antes se mostraba a cualquiera y solo
    // el backend lo bloqueaba con un 403 confuso.
    const btnDescargarManual = CAN_DESCARGAR_MANUAL
      ? '<a class="btn btn-sm btn-outline-secondary" href="/catalogo/api/productos/'+catfProductoId+'/manual/descargar?download=1">'
        + '<i class="bi bi-download me-1"></i>Descargar</a>'
      : '';
    const btnQuitarManual = IS_SUPERADMIN
      ? '<button type="button" class="btn btn-sm btn-outline-danger" id="catfManualQuitar">'
        + '<i class="bi bi-trash me-1"></i>Quitar</button>'
      : '';
    manualWrap.innerHTML = '<div class="catf-manual-box">'
      + '<i class="bi bi-file-earmark-pdf-fill ic"></i>'
      + '<div class="info"><div class="nm">'+esc(d.manual.nombre||'manual.pdf')+'</div>'
      +   '<div class="sz">'+esc(fmtKb(d.manual.size_kb))+'</div></div>'
      + '<button type="button" class="btn btn-sm btn-outline-primary" id="catfManualVer">'
      +   '<i class="bi bi-eye me-1"></i>Ver</button>'
      + btnDescargarManual
      + '<button type="button" class="btn btn-sm btn-outline-secondary" id="catfManualEnviar">'
      +   '<i class="bi bi-envelope me-1"></i>Enviar por correo</button>'
      + '<button type="button" class="btn btn-sm btn-outline-secondary" id="catfManualReemplazar">'
      +   '<i class="bi bi-arrow-repeat me-1"></i>Reemplazar</button>'
      + btnQuitarManual
      + '</div>';
    document.getElementById('catfManualVer').addEventListener('click', function(){
      catvAbrir('/catalogo/api/productos/'+catfProductoId+'/manual/descargar', d.manual.nombre||'manual.pdf');
    });
    document.getElementById('catfManualEnviar').addEventListener('click', catfEnviarManualCorreo);
    document.getElementById('catfManualReemplazar').addEventListener('click', function(){
      document.getElementById('catfManualInput').click();
    });
    if(IS_SUPERADMIN){
      document.getElementById('catfManualQuitar').addEventListener('click', catfQuitarManual);
    }
  } else {
    manualWrap.innerHTML = '<div class="catf-drop" id="catfManualDrop">'
      + '<i class="bi bi-file-earmark-arrow-up"></i>'
      + '<div class="t1">Subir manual (PDF)</div>'
      + '<div class="t2">Hasta 25 MB</div></div>';
    document.getElementById('catfManualDrop').addEventListener('click', function(){
      document.getElementById('catfManualInput').click();
    });
  }
  catfRenderManualesMulti(d.manuales);

  // Eliminar (archivar/soft-delete): superadmin O el permiso granular
  // CAN_ELIMINAR_CATALOGO (ver nota junto a su declaración más arriba) --
  // cat_api_delete ya acepta ambos vía _catalogo_eliminar_required en el
  // backend, esto solo alinea el botón con lo que el backend permite.
  // Eliminar DEFINITIVO (hard-delete): siempre solo superadmin, sin cambios.
  document.getElementById('catfBtnEliminar').style.display = CAN_ELIMINAR_CATALOGO ? 'inline-block' : 'none';
  document.getElementById('catfBtnEliminarDef').style.display = IS_SUPERADMIN ? 'inline-block' : 'none';

  // Auditoría: solo superadmin. El backend (cat_api_detalle) ya omite
  // created_by/updated_by del JSON para cualquier otro rol -- este bloque
  // solo decide si se MUESTRA, con esos mismos campos si llegaron.
  const audSec = document.getElementById('catfAuditoriaSection');
  if(IS_SUPERADMIN && (p.created_by || p.updated_by)){
    const creado = p.created_by
      ? 'Creado por <b>'+esc(p.created_by)+'</b>'+(p.created_at ? ' el '+esc(p.created_at) : '')
      : (p.created_at ? 'Creado el '+esc(p.created_at) : '');
    const actualizado = p.updated_by
      ? 'Última actualización por <b>'+esc(p.updated_by)+'</b>'+(p.updated_at ? ' el '+esc(p.updated_at) : '')
      : (p.updated_at ? 'Última actualización el '+esc(p.updated_at) : '');
    document.getElementById('catfAuditoriaBody').innerHTML =
      (creado ? '<div>'+creado+'</div>' : '') + (actualizado ? '<div>'+actualizado+'</div>' : '');
    audSec.style.display = '';
  } else {
    audSec.style.display = 'none';
  }

  // Piolas: vienen en el mismo payload de detalle (d.piolas), 1 sola llamada.
  _catfPiolas = d.piolas || [];
  catpRender();
}

// ══════════════ Piolas ══════════════
// 2026-07-23: acordeón con diametro_mm/largo_m/descripcion/observacion +
// 2 fotos por piola, reemplaza la tabla vieja de 1 sola foto + medida_cm.
let _catpAccOpen = new Set();   // ids de piolas con el acordeón expandido
let _catpEditingId = null;      // id de la piola en modo edición (null = ninguna)
let _catpSlotActivo = null;     // {piolaId, slot} que está llenando el input de foto compartido

function catpRender(){
  const acc = document.getElementById('catpAcc');
  document.getElementById('catpBadge').textContent = _catfPiolas.length + '/' + MAX_PIOLAS;
  const cls = _catfPiolas.length >= MAX_PIOLAS ? ' full' : '';
  document.getElementById('catpBadge').className = 'catp-badge' + cls;

  if(!_catfPiolas.length){
    acc.innerHTML = '<div class="catp-empty">Sin piolas registradas.</div>';
  } else {
    acc.innerHTML = _catfPiolas.map(catpRenderItem).join('');
  }
  catpRenderAddRow();
}

function catpFotoBadge(p){
  const n = (p.foto_url ? 1 : 0) + (p.foto_url2 ? 1 : 0);
  const cls = n === 0 ? '' : (n === 1 ? ' half' : ' full');
  const ic = n >= 2 ? '<i class="bi bi-check-lg me-1"></i>' : '';
  return '<span class="catp-fbadge'+cls+'">'+ic+n+'/2</span>';
}

// 2026-07-24 (Daniel: "asociar la piola a la foto... algo entretenido,
// dinámico de manejar y ordenado"): reemplaza a catpFotoBadge en la
// cabecera colapsada -- en vez de un badge de texto "2/2", muestra las
// fotos REALES de la piola como miniaturas apiladas, para reconocer la
// pieza a simple vista sin tener que abrir cada fila. Tap en miniatura
// abre el visor directo (stopPropagation, no expande el acordeón); tap
// en el slot fantasma sube la foto a ese slot directo desde la cabecera.
function catpRenderHeaderThumbs(p){
  const n = (p.foto_url ? 1 : 0) + (p.foto_url2 ? 1 : 0);
  const dot = n === 2 ? '<span class="dot full" title="Fotos completas (2 de 2)"></span>'
            : n === 1 ? '<span class="dot half" title="Falta 1 foto"></span>' : '';
  function slot(num){
    const url = num === 1 ? p.foto_url : p.foto_url2;
    if(url){
      return '<button type="button" class="catp-acc-thumb" title="Ver foto '+num+'" '
        + 'aria-label="Ver en grande la foto '+num+' de la piola" '
        + 'onclick="event.stopPropagation();catpFotoVer('+p.id+','+num+')">'
        + '<img src="'+esc(url)+'" alt="" loading="lazy" decoding="async">'
        + '</button>';
    }
    if(CAN_CARGAR_PIOLAS){
      return '<button type="button" class="catp-acc-thumb ghost" title="Agregar foto '+num+'" '
        + 'aria-label="Agregar la foto '+num+' a la piola" '
        + 'onclick="event.stopPropagation();catpFotoSlotClick('+p.id+','+num+')">'
        + '<i class="bi bi-camera"></i></button>';
    }
    return '<span class="catp-acc-thumb ghost noclick" title="Sin foto '+num+'"><i class="bi bi-camera"></i></span>';
  }
  return '<span class="catp-acc-thumbs" role="group" aria-label="Fotos de la piola: '+n+' de 2">'
    + slot(1) + slot(2) + dot + '</span>';
}

function catpRenderItem(p){
  const abierto = _catpAccOpen.has(p.id);
  const editando = _catpEditingId === p.id;
  const resumen = p.legado
    ? '<span class="catp-acc-legado-badge">Legado · '+esc(p.medida_cm_legada!=null?String(p.medida_cm_legada)+' cm':'—')+'</span>'
      + '<span class="desc">'+esc(p.descripcion||'')+'</span>'
    : 'Ø '+esc(String(p.diametro_mm))+' mm × '+esc(String(p.largo_m))+' m — <span class="desc">'+esc(p.descripcion||'')+'</span>';

  return '<div class="catp-acc-item'+(abierto?' is-open':'')+'" data-piola-id="'+p.id+'">'
    + '<div class="catp-acc-header" onclick="catpToggle('+p.id+')">'
    +   '<span class="catp-acc-num">'+(p.orden!=null?esc(String(p.orden)):'')+'</span>'
    +   '<span class="catp-acc-summary">'+resumen+'</span>'
    +   catpRenderHeaderThumbs(p)
    +   '<i class="bi bi-chevron-down catp-acc-chevron'+(abierto?' open':'')+'"></i>'
    + '</div>'
    + '<div class="catp-acc-body'+(abierto?' open':'')+'">'
    +   (editando ? catpRenderEditBody(p) : catpRenderViewBody(p))
    +   catpRenderFotosSlots(p)
    +   (editando ? '' : catpRenderAcciones(p))
    + '</div>'
    + '</div>';
}

function catpRenderViewBody(p){
  if(p.legado){
    return '<div class="catp-acc-legado-note"><i class="bi bi-info-circle"></i>'
      + '<span>Esta piola es antigua y no tiene diámetro/largo registrados todavía — edítala para completarlos.</span></div>'
      + '<div class="catp-acc-campos">'
      +   '<div class="catp-acc-campo full"><label>Descripción</label><div class="v">'+esc(p.descripcion||'—')+'</div></div>'
      +   (p.observacion ? '<div class="catp-acc-campo full"><label>Observación</label><div class="v">'+esc(p.observacion)+'</div></div>' : '')
      + '</div>';
  }
  return '<div class="catp-acc-campos">'
    + '<div class="catp-acc-campo"><label>Diámetro</label><div class="v">'+esc(String(p.diametro_mm))+' mm</div></div>'
    + '<div class="catp-acc-campo"><label>Largo</label><div class="v">'+esc(String(p.largo_m))+' m</div></div>'
    + '<div class="catp-acc-campo full"><label>Descripción</label><div class="v">'+esc(p.descripcion||'—')+'</div></div>'
    + (p.observacion ? '<div class="catp-acc-campo full"><label>Observación</label><div class="v">'+esc(p.observacion)+'</div></div>' : '')
    + '</div>';
}

function catpRenderEditBody(p){
  const diam = p.diametro_mm!=null ? p.diametro_mm : '';
  const largo = p.largo_m!=null ? p.largo_m : '';
  return '<div class="catp-acc-edit-grid">'
    + '<div class="fld"><label>Diámetro (mm)</label>'
    +   '<input type="number" step="0.1" min="3" max="10" class="form-control form-control-sm" id="catpEdDiam_'+p.id+'" value="'+esc(String(diam))+'"></div>'
    + '<div class="fld"><label>Largo (m)</label>'
    +   '<input type="number" step="0.1" min="0.1" max="200" class="form-control form-control-sm" id="catpEdLargo_'+p.id+'" value="'+esc(String(largo))+'"></div>'
    + '<div class="fld full"><label>Descripción *</label>'
    +   '<input type="text" class="form-control form-control-sm" id="catpEdDesc_'+p.id+'" maxlength="300" value="'+esc(p.descripcion||'')+'"></div>'
    + '<div class="fld full"><label>Observación</label>'
    +   '<input type="text" class="form-control form-control-sm" id="catpEdObs_'+p.id+'" maxlength="300" value="'+esc(p.observacion||'')+'"></div>'
    + '</div>'
    + '<div class="catp-acc-edit-actions">'
    +   '<button type="button" class="btn btn-sm btn-ilus" onclick="catpGuardarFila('+p.id+')"><i class="bi bi-check-lg me-1"></i>Guardar</button>'
    +   '<button type="button" class="btn btn-sm btn-outline-secondary" onclick="catpCancelarEdicion('+p.id+')">Cancelar</button>'
    + '</div>';
}

function catpRenderAcciones(p){
  return '<div class="catp-acc-actions">'
    + '<button type="button" class="btn btn-sm btn-outline-secondary" onclick="catpEditarFila('+p.id+')"><i class="bi bi-pencil me-1"></i>Editar</button>'
    + '<button type="button" class="btn btn-sm btn-outline-danger" onclick="catpEliminar('+p.id+')"><i class="bi bi-trash me-1"></i>Eliminar piola</button>'
    + '</div>';
}

// ── Fotos: 2 slots (1/2) por piola, input compartido + kebab de opciones ──
function catpRenderFotoSlot(p, slot){
  const url = slot === 1 ? p.foto_url : p.foto_url2;
  if(!url){
    const puedeSubir = CAN_CARGAR_PIOLAS;
    const cls = 'catp-foto-slot empty' + (puedeSubir ? '' : ' disabled');
    const onclick = puedeSubir ? ' onclick="catpFotoSlotClick('+p.id+','+slot+')"' : '';
    return '<div class="'+cls+'"'+onclick+'>'
      + '<span class="catp-foto-slot-num">'+slot+'</span>'
      + '<i class="bi bi-camera-plus"></i><span>Foto '+slot+'</span>'
      + '</div>';
  }
  const canDel = IS_SUPERADMIN;
  return '<div class="catp-foto-slot">'
    + '<span class="catp-foto-slot-num">'+slot+'</span>'
    + '<img src="'+esc(url)+'" alt="Foto '+slot+' de la piola" onclick="catpFotoVer('+p.id+','+slot+')">'
    + '<div class="dropdown" style="position:absolute;top:4px;right:4px;">'
    +   '<button type="button" class="catp-foto-kebab" data-bs-toggle="dropdown" aria-expanded="false" title="Opciones"><i class="bi bi-three-dots-vertical"></i></button>'
    +   '<ul class="dropdown-menu dropdown-menu-end">'
    +     '<li><a class="dropdown-item" href="#" onclick="event.preventDefault();catpFotoVer('+p.id+','+slot+')"><i class="bi bi-eye me-2"></i>Ver en grande</a></li>'
    +     (CAN_CARGAR_PIOLAS ? '<li><a class="dropdown-item" href="#" onclick="event.preventDefault();catpFotoSlotClick('+p.id+','+slot+')"><i class="bi bi-arrow-repeat me-2"></i>Reemplazar</a></li>' : '')
    +     '<li><a class="dropdown-item'+(canDel?'':' catp-menu-disabled')+'" href="#" onclick="event.preventDefault();catpFotoEliminar('+p.id+','+slot+','+canDel+')"><i class="bi bi-trash me-2"></i>Eliminar</a></li>'
    +   '</ul>'
    + '</div>'
    + '</div>';
}

function catpRenderFotosSlots(p){
  return '<div class="catp-fotos-wrap">' + catpRenderFotoSlot(p, 1) + catpRenderFotoSlot(p, 2) + '</div>';
}

window.catpFotoSlotClick = function(piolaId, slot){
  if(!CAN_CARGAR_PIOLAS) return;
  _catpSlotActivo = {piolaId: piolaId, slot: slot};
  document.getElementById('catpFotoInput').click();
};
document.getElementById('catpFotoInput').addEventListener('change', async function(e){
  const file = e.target.files && e.target.files[0];
  this.value = '';
  if(!file || !_catpSlotActivo) return;
  if(!/^image\//.test(file.type)){ ilusToast('El archivo no es una imagen', {type:'warning'}); return; }
  const piolaId = _catpSlotActivo.piolaId;
  const slot = _catpSlotActivo.slot;
  ilusLoader.show('Subiendo foto…');
  try{
    // Reusa el compresor compartido de foto_editor.js (ilusFotos.compressForUpload)
    // en vez de duplicar la lógica localmente.
    const comp = await ilusFotos.compressForUpload(file);
    const fd = new FormData();
    fd.append('file', new File([comp.blob], comp.name || 'piola.jpg', {type: comp.blob.type || 'image/jpeg'}));
    const r = await fetch('/catalogo/api/productos/'+catfProductoId+'/piolas/'+piolaId+'/foto?slot='+slot, {method:'POST', body: fd});
    const d = await r.json();
    ilusLoader.hide();
    if(d.ok){ ilusToast('✓ Foto de la piola guardada', {type:'success'}); await catpRecargar(); }
    else ilusToast(d.error||'No se pudo subir la foto', {type:'error'});
  }catch(err){ ilusLoader.hide(); ilusToast('Sin conexión al subir la foto', {type:'error'}); }
});

window.catpFotoVer = function(piolaId, slot){
  const p = _catfPiolas.find(function(x){ return x.id === piolaId; });
  const url = p ? (slot === 1 ? p.foto_url : p.foto_url2) : null;
  if(!url) return;
  // Visor de zoom reutilizado (mismo patrón que las fotos del producto),
  // solo lectura -- reemplazar/eliminar corre por el menú propio del slot.
  ilusFotos.view({
    key: 'catalogo-piola-'+piolaId+'-s'+slot,
    photo: {id: piolaId, url: url},
    canEdit: false, canDelete: false,
  });
};

window.catpFotoEliminar = async function(piolaId, slot, puede){
  // 2026-07-23 (hallazgo de revisión): no confiar solo en "puede" (horneado
  // en el onclick al renderizar) -- la función queda en window y cualquiera
  // podría invocarla desde la consola saltándose ese argumento. Se revisa
  // IS_SUPERADMIN acá mismo, igual que hace catpFotoSlotClick con
  // CAN_CARGAR_PIOLAS. El backend igual lo bloquea (_catalogo_admin_required),
  // esto es defensa en profundidad en el cliente.
  if(!puede || !IS_SUPERADMIN){ ilusToast('Solo el superadministrador puede eliminar fotos', {type:'info'}); return; }
  const ok = await ilusConfirm({
    title: 'Eliminar foto', message: '¿Eliminar la foto '+slot+' de esta piola?',
    okLabel: 'Eliminar', cancelLabel: 'Cancelar', danger: true,
  });
  if(!ok) return;
  try{
    const r = await fetch('/catalogo/api/productos/'+catfProductoId+'/piolas/'+piolaId+'/foto?slot='+slot, {method:'DELETE'});
    const d = await r.json();
    if(d.ok){ ilusToast('✓ Foto eliminada', {type:'success'}); await catpRecargar(); }
    else ilusToast(d.error||'No se pudo eliminar la foto', {type:'error'});
  }catch(e){ ilusToast('Sin conexión', {type:'error'}); }
};

// ── Agregar piola nueva (fila al fondo del acordeón) ──
function catpRenderAddRow(){
  const wrap = document.getElementById('catpAddWrap');
  if(_catfPiolas.length >= MAX_PIOLAS){
    wrap.innerHTML = '<div class="catp-empty">Máximo '+MAX_PIOLAS+' piolas por producto.</div>';
    return;
  }
  wrap.innerHTML = '<div class="catp-add-row">'
    + '<div class="fld diametro"><label>Diámetro (mm)</label>'
    +   '<input type="number" step="0.1" min="3" max="10" class="form-control form-control-sm" id="catpNuevaDiam"></div>'
    + '<div class="fld largo"><label>Largo (m)</label>'
    +   '<input type="number" step="0.1" min="0.1" max="200" class="form-control form-control-sm" id="catpNuevaLargo"></div>'
    + '<div class="fld desc"><label>Descripción</label>'
    +   '<input type="text" class="form-control form-control-sm" id="catpNuevaDesc" placeholder="Ej: cable de la polea alta" maxlength="300"></div>'
    + '<div class="fld obs"><label>Observación</label>'
    +   '<input type="text" class="form-control form-control-sm" id="catpNuevaObs" placeholder="Opcional" maxlength="300"></div>'
    + '<button type="button" class="btn btn-sm btn-ilus" id="catpBtnAgregar"><i class="bi bi-plus-lg me-1"></i>Agregar piola</button>'
    + '</div>';
  document.getElementById('catpBtnAgregar').addEventListener('click', catpAgregar);
}

async function catpAgregar(){
  const diamEl = document.getElementById('catpNuevaDiam');
  const largoEl = document.getElementById('catpNuevaLargo');
  const descEl = document.getElementById('catpNuevaDesc');
  const obsEl = document.getElementById('catpNuevaObs');
  const diam = parseFloat(diamEl.value);
  const largo = parseFloat(largoEl.value);
  const desc = descEl.value.trim();
  const obs = obsEl.value.trim();
  if(isNaN(diam) || diam < 3 || diam > 10){ ilusToast('Ingresa un diámetro válido (3.0 a 10.0 mm)', {type:'warning'}); return; }
  if(isNaN(largo) || largo <= 0 || largo > 200){ ilusToast('Ingresa un largo válido (mayor a 0 m)', {type:'warning'}); return; }
  if(!desc){ ilusToast('La descripción es obligatoria (para ubicar el cable en la máquina)', {type:'warning'}); return; }
  const btn = document.getElementById('catpBtnAgregar');
  btn.disabled = true;
  const body = {diametro_mm: diam, largo_m: largo, descripcion: desc};
  if(obs) body.observacion = obs;
  try{
    const r = await fetch('/catalogo/api/productos/'+catfProductoId+'/piolas', {method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify(body)});
    const d = await r.json();
    if(d.ok){
      ilusToast('✓ Piola agregada', {type:'success'});
      await catpRecargar();
    } else { ilusToast(d.error||'No se pudo agregar la piola', {type:'error'}); btn.disabled = false; }
  }catch(e){ ilusToast('Sin conexión', {type:'error'}); btn.disabled = false; }
}

async function catpRecargar(){
  // No hay endpoint propio en el contrato para refrescar solo piolas dentro
  // de la ficha ya abierta — se reusa GET detalle (mismo que abre la ficha),
  // que ya trae "piolas" en el payload.
  try{
    const r = await fetch('/catalogo/api/productos/'+catfProductoId);
    const d = await r.json();
    if(d.ok){ _catfPiolas = d.piolas || []; catpRender(); }
  }catch(e){ ilusToast('Sin conexión', {type:'error'}); }
}

window.catpToggle = function(piolaId){
  if(_catpAccOpen.has(piolaId)) _catpAccOpen.delete(piolaId);
  else _catpAccOpen.add(piolaId);
  catpRender();
};

window.catpEditarFila = function(piolaId){
  _catpEditingId = piolaId;
  _catpAccOpen.add(piolaId);
  catpRender();
};

window.catpCancelarEdicion = function(piolaId){
  _catpEditingId = null;
  catpRender();
};

window.catpGuardarFila = async function(piolaId){
  const diamEl = document.getElementById('catpEdDiam_'+piolaId);
  const largoEl = document.getElementById('catpEdLargo_'+piolaId);
  const descEl = document.getElementById('catpEdDesc_'+piolaId);
  const obsEl = document.getElementById('catpEdObs_'+piolaId);
  const desc = descEl.value.trim();
  if(!desc){ ilusToast('La descripción es obligatoria', {type:'warning'}); return; }
  const body = {descripcion: desc};
  const diamRaw = diamEl.value.trim();
  if(diamRaw !== ''){
    const diam = parseFloat(diamRaw);
    if(isNaN(diam) || diam < 3 || diam > 10){ ilusToast('El diámetro debe estar entre 3.0 y 10.0 mm', {type:'warning'}); return; }
    body.diametro_mm = diam;
  }
  const largoRaw = largoEl.value.trim();
  if(largoRaw !== ''){
    const largo = parseFloat(largoRaw);
    if(isNaN(largo) || largo <= 0 || largo > 200){ ilusToast('El largo debe ser mayor a 0 y hasta 200 m', {type:'warning'}); return; }
    body.largo_m = largo;
  }
  // 2026-07-23 (hallazgo de revisión): siempre se manda "observacion",
  // incluso vacía -- si no, borrar el texto del campo no tenía forma de
  // limpiar el valor guardado (el backend solo toca la columna cuando la
  // llave está presente en el body).
  body.observacion = obsEl.value.trim();
  try{
    const r = await fetch('/catalogo/api/productos/'+catfProductoId+'/piolas/'+piolaId, {method:'PATCH',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify(body)});
    const d = await r.json();
    if(d.ok){
      ilusToast('✓ Piola actualizada', {type:'success'});
      _catpEditingId = null;
      await catpRecargar();
    } else ilusToast(d.error||'No se pudo guardar', {type:'error'});
  }catch(e){ ilusToast('Sin conexión', {type:'error'}); }
};

window.catpEliminar = async function(piolaId){
  const ok = await ilusConfirm({
    title: 'Eliminar piola',
    message: '¿Quitar esta piola del registro?',
    sub: 'Queda igual en el historial de auditoría.',
    okLabel: 'Eliminar', cancelLabel: 'Cancelar', danger: true,
  });
  if(!ok) return;
  try{
    const r = await fetch('/catalogo/api/productos/'+catfProductoId+'/piolas/'+piolaId, {method:'DELETE'});
    const d = await r.json();
    if(d.ok){
      ilusToast('✓ Piola eliminada', {type:'success'});
      _catpAccOpen.delete(piolaId);
      if(_catpEditingId === piolaId) _catpEditingId = null;
      await catpRecargar();
    }
    else ilusToast(d.error||'No se pudo eliminar', {type:'error'});
  }catch(e){ ilusToast('Sin conexión', {type:'error'}); }
};

// ── Historial de piolas (modal Bootstrap: lista puede ser larga, Regla #8) ──
const catpHistModalEl = document.getElementById('catpHistModal');
const catpHistModal = _bsModalLazy(catpHistModalEl);
document.getElementById('catpBtnHistorial').addEventListener('click', async function(){
  catpHistModal.show();
  const body = document.getElementById('catpHistBody');
  body.innerHTML = '<div class="catp-empty">Cargando…</div>';
  let d;
  try{
    const r = await fetch('/catalogo/api/productos/'+catfProductoId+'/piolas/historial');
    d = await r.json();
  }catch(e){ body.innerHTML = '<div class="catp-empty">No se pudo cargar el historial.</div>'; return; }
  if(!d.ok){ body.innerHTML = '<div class="catp-empty">'+esc(d.error||'No se pudo cargar el historial.')+'</div>'; return; }
  const eventos = d.eventos || [];
  if(!eventos.length){ body.innerHTML = '<div class="catp-empty">Sin eventos registrados.</div>'; return; }
  const accionLbl = {cat_piola_crear:'Creó', cat_piola_editar:'Editó', cat_piola_eliminar:'Eliminó'};
  const accionCls = {cat_piola_crear:'crear', cat_piola_editar:'editar', cat_piola_eliminar:'eliminar'};
  body.innerHTML = eventos.map(function(ev){
    const det = ev.detalle || {};
    let diff = '';
    if(det.medida_cm_antes != null || det.medida_cm_despues != null){
      diff += 'Medida: '+esc(det.medida_cm_antes!=null?String(det.medida_cm_antes)+' cm':'—')
        +' → '+esc(det.medida_cm_despues!=null?String(det.medida_cm_despues)+' cm':'—')+'. ';
    }
    if(det.observacion_antes != null || det.observacion_despues != null){
      diff += 'Observación: "'+esc(det.observacion_antes||'—')+'" → "'+esc(det.observacion_despues||'—')+'". ';
    }
    if(det.diametro_mm_antes != null || det.diametro_mm_despues != null){
      diff += 'Diámetro: '+esc(det.diametro_mm_antes!=null?String(det.diametro_mm_antes)+' mm':'—')
        +' → '+esc(det.diametro_mm_despues!=null?String(det.diametro_mm_despues)+' mm':'—')+'. ';
    }
    if(det.largo_m_antes != null || det.largo_m_despues != null){
      diff += 'Largo: '+esc(det.largo_m_antes!=null?String(det.largo_m_antes)+' m':'—')
        +' → '+esc(det.largo_m_despues!=null?String(det.largo_m_despues)+' m':'—')+'. ';
    }
    if(det.descripcion_antes != null || det.descripcion_despues != null){
      diff += 'Descripción: "'+esc(det.descripcion_antes||'—')+'" → "'+esc(det.descripcion_despues||'—')+'".';
    }
    const accion = ev.accion || '';
    return '<div class="catp-hist-item">'
      + '<div class="hd"><span class="quien">'+esc(ev.usuario||'—')+' <span class="text-muted" style="font-weight:400;">('+esc(ev.rol||'')+')</span></span>'
      +   '<span class="cuando">'+esc(ev.fecha||'')+'</span></div>'
      + '<span class="accion '+(accionCls[accion]||'')+'">'+esc(accionLbl[accion]||accion)+'</span>'
      + '<div class="detalle">'+diff+'</div>'
      + '</div>';
  }).join('');
});

// ── Enviar manual por correo (Regla #1: ilusPrompt, no prompt() nativo) ──
async function catfEnviarManualCorreo(){
  const email = await ilusPrompt({
    title: 'Enviar manual por correo',
    message: 'Correo de destino',
    placeholder: 'cliente@dominio.cl',
    required: true,
  });
  if(email === null || email === undefined) return;
  const correo = String(email).trim();
  const reEmail = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  if(!reEmail.test(correo)){ ilusToast('Correo inválido', {type:'warning'}); return; }
  // 2026-07-23 (blueprint piolas/manuales, Daniel: "se puede agregar una
  // copia"): segundo prompt opcional para CC -- cancelarlo NO aborta el
  // envío al destinatario principal, solo se manda sin copia.
  const ccRaw = await ilusPrompt({
    title: 'Copia (CC) — opcional',
    message: '¿Agregar un correo en copia?',
    placeholder: 'copia@dominio.cl',
    required: false,
  });
  const cc = (ccRaw !== null && ccRaw !== undefined) ? String(ccRaw).trim() : '';
  if(cc && !reEmail.test(cc)){ ilusToast('El correo de copia (CC) es inválido', {type:'warning'}); return; }
  ilusLoader.show('Enviando manual…');
  try{
    const r = await fetch('/catalogo/api/productos/'+catfProductoId+'/manual/enviar-correo', {method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify({email: correo, cc: cc || undefined})});
    const d = await r.json();
    ilusLoader.hide();
    if(d.ok) ilusToast('✓ Manual enviado a '+correo, {type:'success'});
    else ilusToast(d.error||'No se pudo enviar el correo', {type:'error'});
  }catch(e){ ilusLoader.hide(); ilusToast('Sin conexión', {type:'error'}); }
}

// ── Edición inline de Datos (mismo patrón "clic en el lápiz" de tickets/ficha.html) ──
window.catfEditar = function(campo){
  const el = document.getElementById('catfVal'+campo.charAt(0).toUpperCase()+campo.slice(1));
  if(!el || el.querySelector('input,textarea')) return;
  const original = catfProductoActual[campo] || '';
  const esArea = campo === 'observacion';
  el.innerHTML = esArea
    ? '<textarea class="form-control form-control-sm" rows="2">'+esc(original)+'</textarea>'
    : '<input type="text" class="form-control form-control-sm" value="'+esc(original)+'">';
  const input = el.querySelector('input,textarea');
  input.focus();
  if(!esArea) input.select();
  let guardado = false;
  async function guardar(){
    if(guardado) return; guardado = true;
    const nuevo = input.value.trim();
    if(nuevo === (original||'')){ el.textContent = original || '—'; return; }
    try{
      const r = await fetch('/catalogo/api/productos/'+catfProductoId, {method:'PATCH',
        headers:{'Content-Type':'application/json'}, body:JSON.stringify({[campo]: nuevo})});
      const d = await r.json();
      if(d.ok){
        catfProductoActual[campo] = nuevo;
        el.textContent = nuevo || '—';
        if(campo==='nombre') document.getElementById('catfNombre').textContent = nuevo || catfProductoActual.sku;
        if(campo==='sku') document.getElementById('catfSkuTag').textContent = nuevo;
        ilusToast('✓ Guardado', {type:'success'});
        cargarCatalogo();
      } else {
        el.textContent = original || '—';
        ilusToast(d.error||'No se pudo guardar', {type:'error'});
      }
    }catch(e){ el.textContent = original || '—'; ilusToast('Sin conexión', {type:'error'}); }
  }
  input.addEventListener('blur', guardar);
  input.addEventListener('keydown', function(e){
    if(e.key === 'Enter' && !esArea){ e.preventDefault(); input.blur(); }
    if(e.key === 'Escape'){ guardado = true; el.textContent = original || '—'; }
  });
};

// ── Edición de "Clase de producto" (select, no texto libre — Regla #1: sin prompt nativo) ──
window.catfEditarClase = async function(){
  const el = document.getElementById('catfValClaseProducto');
  if(!el || el.querySelector('select')) return;
  const original = catfProductoActual.clase_producto || '';
  const clases = await catObtenerClases();
  const sel = document.createElement('select');
  sel.className = 'form-select form-select-sm';
  sel.innerHTML = '<option value="">— Sin clase —</option>'
    + clases.map(c=>'<option value="'+esc(c.value)+'"'+(c.value===original?' selected':'')+'>'+esc(c.label)+'</option>').join('');
  el.innerHTML = ''; el.appendChild(sel);
  sel.focus();
  let guardado = false;
  async function guardar(){
    if(guardado) return; guardado = true;
    const nuevo = sel.value;
    if(nuevo === original){
      const lbl = clases.find(c=>c.value===original);
      el.textContent = (lbl && lbl.label) || '—';
      return;
    }
    try{
      const r = await fetch('/catalogo/api/productos/'+catfProductoId, {method:'PATCH',
        headers:{'Content-Type':'application/json'}, body:JSON.stringify({clase_producto: nuevo})});
      const d = await r.json();
      if(d.ok){
        catfProductoActual.clase_producto = nuevo;
        const lbl = clases.find(c=>c.value===nuevo);
        const texto = (lbl && lbl.label) || '—';
        catfProductoActual.clase_producto_label = nuevo ? texto : '';
        el.textContent = texto;
        const badgeClase = document.getElementById('catfBadgeClase');
        if(nuevo){ badgeClase.textContent = texto; badgeClase.style.display = 'inline-flex'; }
        else { badgeClase.style.display = 'none'; }
        ilusToast('✓ Guardado', {type:'success'});
        cargarCatalogo();
      } else {
        const lbl = clases.find(c=>c.value===original);
        el.textContent = (lbl && lbl.label) || '—';
        ilusToast(d.error||'No se pudo guardar', {type:'error'});
      }
    }catch(e){
      const lbl = clases.find(c=>c.value===original);
      el.textContent = (lbl && lbl.label) || '—';
      ilusToast('Sin conexión', {type:'error'});
    }
  }
  sel.addEventListener('change', guardar);
  sel.addEventListener('blur', guardar);
};

// ── Fotos: agregar / eliminar (rutas EXACTAS del contrato) ──
// Daniel 2026-07-27 (Jaizer): "podria hasta recibir alguna imagen que
// este copiada en el portapapeles" -- se extrae la subida a una función
// compartida para poder llamarla también desde Ctrl+V (pegar), no solo
// desde el selector de archivos. Acepta cualquier tipo de imagen (el
// backend ya no restringe por extensión — ver _img_resize_bytes).
async function catfSubirFotoArchivo(file){
  if(!file) return;
  const fd = new FormData(); fd.append('file', file);
  ilusLoader.show('Subiendo foto…');
  try{
    const r = await fetch('/catalogo/api/productos/'+catfProductoId+'/fotos', {method:'POST', body:fd});
    const d = await r.json();
    ilusLoader.hide();
    if(d.ok){ ilusToast('✓ Foto agregada', {type:'success'}); catfCargar(); cargarCatalogo(); }
    else ilusToast(d.error||'No se pudo subir la foto', {type:'error'});
  }catch(err){ ilusLoader.hide(); ilusToast('Sin conexión', {type:'error'}); }
}
document.getElementById('catfFotoInput').addEventListener('change', function(e){
  const file = e.target.files && e.target.files[0];
  this.value = '';
  catfSubirFotoArchivo(file);
});
// Pegar imagen desde el portapapeles (Ctrl+V) mientras la ficha está
// abierta — típico caso: recortar una captura de pantalla y pegarla
// directo, sin tener que guardarla como archivo primero.
if (catfModalEl){
  catfModalEl.addEventListener('paste', function(e){
    if(!catfProductoId) return;
    const items = (e.clipboardData || window.clipboardData)?.items;
    if(!items) return;
    for (const item of items){
      if(item.kind === 'file' && item.type.startsWith('image/')){
        const file = item.getAsFile();
        if(file) { e.preventDefault(); catfSubirFotoArchivo(file); }
        break;
      }
    }
  });
}

window.catfEliminarFoto = async function(fotoId){
  const ok = await ilusConfirm({title:'Eliminar foto', message:'¿Eliminar esta foto del producto?', danger:true, okLabel:'Eliminar'});
  if(!ok) return;
  try{
    const r = await fetch('/catalogo/api/productos/'+catfProductoId+'/fotos/'+fotoId, {method:'DELETE'});
    const d = await r.json();
    if(d.ok){ ilusToast('✓ Foto eliminada', {type:'success'}); catfCargar(); cargarCatalogo(); }
    else ilusToast(d.error||'No se pudo eliminar la foto', {type:'error'});
  }catch(e){ ilusToast('Sin conexión', {type:'error'}); }
};

// ── Manual PDF: subir / reemplazar / quitar (rutas EXACTAS del contrato) ──
document.getElementById('catfManualInput').addEventListener('change', async function(e){
  const file = e.target.files && e.target.files[0];
  this.value = '';
  if(!file) return;
  if(!/\.pdf$/i.test(file.name) || file.type !== 'application/pdf'){
    ilusToast('El manual debe ser un archivo PDF', {type:'warning'}); return;
  }
  if(file.size > 25*1024*1024){ ilusToast('El manual supera 25 MB', {type:'error'}); return; }
  const fd = new FormData(); fd.append('file', file);
  ilusLoader.show('Subiendo manual…');
  try{
    const r = await fetch('/catalogo/api/productos/'+catfProductoId+'/manual', {method:'POST', body:fd});
    const d = await r.json();
    ilusLoader.hide();
    if(d.ok){ ilusToast('✓ Manual subido', {type:'success'}); catfCargar(); cargarCatalogo(); }
    else ilusToast(d.error||'No se pudo subir el manual', {type:'error'});
  }catch(err){ ilusLoader.hide(); ilusToast('Sin conexión', {type:'error'}); }
});

async function catfQuitarManual(){
  const ok = await ilusConfirm({title:'Quitar manual', message:'¿Quitar el manual PDF de este producto?', danger:true, okLabel:'Quitar'});
  if(!ok) return;
  try{
    const r = await fetch('/catalogo/api/productos/'+catfProductoId+'/manual', {method:'DELETE'});
    const d = await r.json();
    if(d.ok){ ilusToast('✓ Manual eliminado', {type:'success'}); catfCargar(); cargarCatalogo(); }
    else ilusToast(d.error||'No se pudo quitar el manual', {type:'error'});
  }catch(e){ ilusToast('Sin conexión', {type:'error'}); }
}

// ══════════════ Visor de PDF en modal (manuales, hasta 5 + legado) ══════════════
const catvModalEl = document.getElementById('catvModal');
const catvModal = _bsModalLazy(catvModalEl);
let _catvBlobUrl = null;
let _catvAbortCtrl = null;
let _catvReqToken = 0;
// 2026-07-13 (stress-test/revisión adversarial): abrir/cerrar el modal
// repetido (o hacer click en 2 manuales seguidos) antes lanzaba fetch()
// "zombis" que, al resolver fuera de orden, podían revocar el blob URL que
// el iframe ACTUAL estaba usando o pisar el PDF que el usuario ya veía con
// uno viejo. Fix: token de request + AbortController — se cancela el
// fetch anterior y se ignora cualquier respuesta que llegue después de que
// otra apertura (o el cierre del modal) la haya vuelto obsoleta.
window.catvAbrir = async function(url, nombre){
  if(_catvAbortCtrl){ try{ _catvAbortCtrl.abort(); }catch(e){} }
  const myToken = ++_catvReqToken;
  _catvAbortCtrl = new AbortController();
  document.getElementById('catvNombre').textContent = nombre || 'Manual';
  document.getElementById('catvSub').textContent = '';
  // 2026-08-25: el botón "Descargar" del propio visor sigue el mismo
  // permiso que el resto (CAN_DESCARGAR_MANUAL) -- ver comentario junto a
  // su declaración arriba. Con el permiso, agrega ?download=1 para que el
  // backend entregue el PDF como attachment (ver catalogo_module.py).
  const btnDescargar = document.getElementById('catvBtnDescargar');
  btnDescargar.style.display = CAN_DESCARGAR_MANUAL ? '' : 'none';
  btnDescargar.href = CAN_DESCARGAR_MANUAL ? (url + (url.includes('?') ? '&' : '?') + 'download=1') : '#';
  const body = document.getElementById('catvBody');
  body.innerHTML = '<div class="text-white-50"><span class="spinner-border spinner-border-sm me-2"></span>Cargando PDF…</div>';
  catvModal.show();
  try{
    const r = await fetch(url, {signal: _catvAbortCtrl.signal});
    if(!r.ok) throw new Error('HTTP '+r.status);
    const blob = await r.blob();
    if(myToken !== _catvReqToken) return; // obsoleto: otra apertura/cierre tomó el control
    if(_catvBlobUrl){ URL.revokeObjectURL(_catvBlobUrl); }
    _catvBlobUrl = URL.createObjectURL(blob);
    body.innerHTML = '<iframe class="catv-frame" src="'+_catvBlobUrl+'" title="'+esc(nombre||'Manual')+'"></iframe>';
  }catch(e){
    if(myToken !== _catvReqToken) return; // abortado a propósito, no mostrar error
    body.innerHTML = '<div class="text-white-50 text-center p-4">No se pudo cargar la vista previa'
      + (CAN_DESCARGAR_MANUAL ? '. Usa "Descargar".' : '. Intenta de nuevo o avisa a un administrador.') + '</div>';
  }
};
catvModalEl.addEventListener('hidden.bs.modal', function(){
  if(_catvAbortCtrl){ try{ _catvAbortCtrl.abort(); }catch(e){} }
  _catvReqToken++; // invalida cualquier fetch en vuelo
  if(_catvBlobUrl){ URL.revokeObjectURL(_catvBlobUrl); _catvBlobUrl = null; }
  document.getElementById('catvBody').innerHTML = '';
});

// ── Manuales multi (hasta 5) en la ficha de detalle: Ver / Descargar / Quitar.
//    No reemplaza el manual único legado (arriba) -- Regla #4.2, se agrega
//    aparte con su propia sección visible solo si hay manuales cargados. ──
function catfRenderManualesMulti(manuales){
  const section = document.getElementById('catfManualesMultiSection');
  const wrap = document.getElementById('catfManualesMultiLista');
  if(!manuales || !manuales.length){ section.style.display = 'none'; wrap.innerHTML = ''; return; }
  section.style.display = '';
  wrap.innerHTML = manuales.map(function(m){
    // 2026-08-25: mismo criterio que el manual único (ver comentario en
    // catfCargar) -- Descargar = CAN_DESCARGAR_MANUAL, Quitar =
    // IS_SUPERADMIN (alineado con el gate real del backend), Ver siempre.
    const btnDescargar = CAN_DESCARGAR_MANUAL
      ? '<a class="btn btn-sm btn-outline-secondary" href="'+esc(m.url)+'?download=1" title="Descargar"><i class="bi bi-download"></i></a>'
      : '';
    const btnQuitar = IS_SUPERADMIN
      ? '<button type="button" class="btn btn-sm btn-outline-danger" title="Quitar" onclick="catfQuitarManualMulti('+m.id+')"><i class="bi bi-trash"></i></button>'
      : '';
    return '<div class="catw-manual-item catv-manual-item" data-manual-id="'+m.id+'">'
      + '<i class="bi bi-file-earmark-pdf-fill ic"></i>'
      + '<div class="nm">'+esc(m.nombre)+'</div>'
      + '<button type="button" class="btn btn-sm btn-outline-primary ver" title="Ver" data-ver-url="'+esc(m.url)+'" data-ver-nombre="'+esc(m.nombre)+'"><i class="bi bi-eye"></i></button>'
      + btnDescargar
      + '<button type="button" class="btn btn-sm btn-outline-secondary" title="Enviar por correo" data-enviar-id="'+m.id+'" data-enviar-nombre="'+esc(m.nombre)+'"><i class="bi bi-envelope"></i></button>'
      + btnQuitar
      + '</div>';
  }).join('');
  wrap.querySelectorAll('[data-ver-url]').forEach(function(btn){
    btn.addEventListener('click', function(){
      catvAbrir(btn.getAttribute('data-ver-url'), btn.getAttribute('data-ver-nombre'));
    });
  });
  wrap.querySelectorAll('[data-enviar-id]').forEach(function(btn){
    btn.addEventListener('click', function(){
      catfEnviarManualMultiCorreo(btn.getAttribute('data-enviar-id'), btn.getAttribute('data-enviar-nombre'));
    });
  });
}

// ── Enviar por correo UNO específico de los manuales multi (hasta 5) --
//    mismo patrón/endpoint que catfEnviarManualCorreo (legado, arriba),
//    apuntando al manual puntual en vez del manual_pdf_key singular
//    (Regla #1: ilusPrompt/ilusToast, nunca prompt()/alert() nativos). ──
async function catfEnviarManualMultiCorreo(manualId, nombre){
  const email = await ilusPrompt({
    title: 'Enviar manual por correo',
    message: 'Correo de destino',
    sub: nombre ? ('Archivo: '+nombre) : '',
    placeholder: 'cliente@dominio.cl',
    required: true,
  });
  if(email === null || email === undefined) return;
  const correo = String(email).trim();
  const reEmail = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  if(!reEmail.test(correo)){ ilusToast('Correo inválido', {type:'warning'}); return; }
  const ccRaw = await ilusPrompt({
    title: 'Copia (CC) — opcional',
    message: '¿Agregar un correo en copia?',
    placeholder: 'copia@dominio.cl',
    required: false,
  });
  const cc = (ccRaw !== null && ccRaw !== undefined) ? String(ccRaw).trim() : '';
  if(cc && !reEmail.test(cc)){ ilusToast('El correo de copia (CC) es inválido', {type:'warning'}); return; }
  ilusLoader.show('Enviando manual…');
  try{
    const r = await fetch('/catalogo/api/productos/'+catfProductoId+'/manuales/'+manualId+'/enviar-correo', {method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify({email: correo, cc: cc || undefined})});
    const d = await r.json();
    ilusLoader.hide();
    if(d.ok) ilusToast('✓ Manual enviado a '+correo, {type:'success'});
    else ilusToast(d.error||'No se pudo enviar el correo', {type:'error'});
  }catch(e){ ilusLoader.hide(); ilusToast('Sin conexión', {type:'error'}); }
}

window.catfQuitarManualMulti = async function(manualId){
  const ok = await ilusConfirm({
    title: 'Quitar manual', message: '¿Quitar este manual del producto?',
    okLabel: 'Quitar', cancelLabel: 'Cancelar', danger: true,
  });
  if(!ok) return;
  try{
    const r = await fetch('/catalogo/api/productos/'+catfProductoId+'/manuales/'+manualId, {method:'DELETE'});
    const d = await r.json();
    if(d.ok){ ilusToast('✓ Manual eliminado', {type:'success'}); catfCargar(); }
    else ilusToast(d.error||'No se pudo quitar el manual', {type:'error'});
  }catch(e){ ilusToast('Sin conexión', {type:'error'}); }
};

// ── Eliminar producto (soft) / definitivamente (superadmin, requiere el SKU exacto) ──
document.getElementById('catfBtnEliminar').addEventListener('click', async function(){
  const p = catfProductoActual || {};
  const ok = await ilusConfirm({
    title: 'Eliminar producto',
    message: '¿Archivar "'+(p.nombre||p.sku||'')+'"?',
    sub: 'Queda oculto del catálogo (se puede recuperar activando "Ver archivados").',
    okLabel: 'Eliminar', cancelLabel: 'Cancelar', danger: true,
  });
  if(!ok) return;
  try{
    const r = await fetch('/catalogo/api/productos/'+catfProductoId, {method:'DELETE'});
    const d = await r.json();
    if(d.ok){
      ilusToast('✓ Producto eliminado', {type:'success'});
      catfModal.hide();
      cargarCatalogo();
    } else ilusToast(d.error||'No se pudo eliminar', {type:'error'});
  }catch(e){ ilusToast('Sin conexión', {type:'error'}); }
});
document.getElementById('catfBtnEliminarDef').addEventListener('click', async function(){
  const p = catfProductoActual || {};
  const texto = await ilusPrompt({
    title: 'Eliminar definitivamente',
    message: 'Esta acción NO se puede deshacer. Escribe el SKU exacto para confirmar:',
    sub: 'SKU: '+(p.sku||''),
    placeholder: p.sku||'',
    required: true,
    type: 'danger',
  });
  if(texto === null || texto === undefined) return;
  if(String(texto).trim().toUpperCase() !== String(p.sku||'').toUpperCase()){
    ilusToast('El SKU no coincide. No se eliminó nada.', {type:'warning'}); return;
  }
  try{
    // El backend lee confirm_text del BODY json de la request DELETE (no de
    // query string) — cat_api_delete hace request.get_json(silent=True).
    const r = await fetch('/catalogo/api/productos/'+catfProductoId, {method:'DELETE',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify({confirm_text: String(texto).trim()})});
    const d = await r.json();
    if(d.ok){
      ilusToast('✓ Producto eliminado definitivamente', {type:'success'});
      catfModal.hide();
      cargarCatalogo();
    } else ilusToast(d.error||'No se pudo eliminar', {type:'error'});
  }catch(e){ ilusToast('Sin conexión', {type:'error'}); }
});

// "Continuar con el asistente guiado" -- puente desde la ficha (#catfModal)
// hacia el wizard (#catwModal) cuando el producto está incompleto (ver
// #catfAvisoIncompleto). Mismo patrón de cierre-y-reapertura que
// _catwAbrirSiguienteDeCola (350ms para que termine la transición de cierre).
document.getElementById('catfBtnContinuarWizard').addEventListener('click', function(){
  const id = catfProductoId;
  catfModal.hide();
  setTimeout(function(){ catwAbrir(id); }, 350);
});

// ══════════════ Wizard "Registrar producto" (2026-07-12) ══════════════
// Guarda el progreso real en BD en cada paso (Daniel: "si cierro el navegador
// a medias no quiero perder lo ya cargado"), reusando los endpoints que YA
// existen (PATCH familia, POST/PATCH piolas, POST manuales) — sin endpoint
// nuevo de "guardar todo junto".
const MAX_MANUALES_WIZ = 5;
const CATW_STEPS = [
  {n:1, label:'Producto', icon:'bi-box-seam'},
  {n:2, label:'Datos',    icon:'bi-tag'},
  {n:3, label:'Piolas',   icon:'bi-cable'},
  {n:4, label:'Manuales', icon:'bi-file-earmark-pdf'},
];
let catwProductoId = null;
let catwStep = 1;
let catwProducto = null;
let catwPiolasExistentes = []; // piolas ya guardadas (con id) para el producto
let catwManuales = [];
const catwModalEl = document.getElementById('catwModal');
const catwModal = _bsModalLazy(catwModalEl);

window.catwAbrir = async function(id){
  catwProductoId = id;
  catwStep = 1;
  catwModal.show();
  document.getElementById('catwNombreProducto').textContent = 'Cargando…';
  let d;
  try{
    const r = await fetch('/catalogo/api/productos/'+id);
    d = await r.json();
  }catch(e){ ilusToast('No se pudo cargar el producto', {type:'error'}); catwModal.hide(); return; }
  if(!d.ok){ ilusToast(d.error||'Error al cargar el producto', {type:'error'}); catwModal.hide(); return; }
  catwProducto = d.producto;
  catwPiolasExistentes = d.piolas || [];
  catwManuales = d.manuales || [];

  document.getElementById('catwSkuTag').textContent = catwProducto.sku || '';
  document.getElementById('catwNombreProducto').textContent = catwProducto.nombre || catwProducto.sku;
  document.getElementById('catwSkuProducto').textContent = catwProducto.sku || '';
  document.getElementById('catwFamilia').value = catwProducto.familia || '';
  document.getElementById('catwFamiliaList').innerHTML =
    Array.from(_catFamilias).map(f=>'<option value="'+esc(f)+'">').join('');
  document.getElementById('catwCantPiolas').value = catwPiolasExistentes.length || '';
  const clasesW = await catObtenerClases();
  const claseActual = catwProducto.clase_producto || '';
  document.getElementById('catwClase').innerHTML = '<option value="">— Sin clase —</option>'
    + clasesW.map(c=>'<option value="'+esc(c.value)+'"'+(c.value===claseActual?' selected':'')+'>'+esc(c.label)+'</option>').join('');

  // 2026-07-14: familia/clase (PATCH /catalogo/api/productos/<id>) y
  // subir/quitar manuales son _catalogo_admin_required en el backend
  // (solo superadmin) -- ver comentario en catalogo_module.py junto a
  // _catalogo_admin_required. Para un rol ampliado (CAN_CARGAR_PIOLAS sin
  // ser superadmin) esos controles se ocultan aquí para que el wizard
  // nunca intente una llamada que el backend va a rechazar.
  document.getElementById('catwFamiliaClaseWrap').style.display = IS_SUPERADMIN ? '' : 'none';
  document.getElementById('catwFamiliaClaseAviso').style.display = IS_SUPERADMIN ? 'none' : '';

  catwRenderManuales();
  catwMostrarPaso();
};

function catwRenderStepper(){
  document.getElementById('catwStepper').innerHTML = CATW_STEPS.map(function(s){
    const cls = s.n < catwStep ? 'done' : (s.n === catwStep ? 'current' : '');
    const inner = s.n < catwStep ? '<i class="bi bi-check-lg"></i>' : s.n;
    // 2026-08-28 (Daniel: "la encuentro muy rígida, muy estricta, larga" --
    // basta clasificación O piolas O manual, ver "registrado" OR en
    // catalogo_module.py desde el 23-jul, pero el wizard seguía obligando
    // a pasar linealmente por los 4 pasos). Los círculos ahora saltan
    // directo al paso que se toque -- ya no hace falta "Siguiente" 3 veces
    // para llegar a Manuales si eso es lo único que se quiere completar.
    return '<div class="catw-step '+cls+'" onclick="catwIrPaso('+s.n+')" style="cursor:pointer" role="button" tabindex="0">'
      + '<div class="catw-step-circle">'+inner+'</div>'
      + '<div class="catw-step-label">'+s.label+'</div></div>';
  }).join('');
}
function catwIrPaso(n){
  if (n === catwStep) return;
  catwStep = n;
  catwMostrarPaso();
}

function catwMostrarPaso(){
  catwRenderStepper();
  catwModalEl.querySelectorAll('.catw-panel').forEach(function(p){
    p.classList.toggle('active', Number(p.dataset.step) === catwStep);
  });
  document.getElementById('catwBtnAtras').style.visibility = catwStep === 1 ? 'hidden' : 'visible';
  const btnSig = document.getElementById('catwBtnSiguiente');
  btnSig.innerHTML = catwStep === 4
    ? '<i class="bi bi-check-lg me-1"></i>Finalizar'
    : 'Siguiente<i class="bi bi-arrow-right ms-1"></i>';
  if(catwStep === 3) catwRenderPiolasInputs();
}

document.getElementById('catwBtnAtras').addEventListener('click', function(){
  if(catwStep > 1){ catwStep--; catwMostrarPaso(); }
});

document.getElementById('catwBtnSiguiente').addEventListener('click', async function(){
  const btn = this; const original = btn.innerHTML;
  if(catwStep === 1){
    catwStep = 2; catwMostrarPaso(); return;
  }
  if(catwStep === 2){
    const cant = parseInt(document.getElementById('catwCantPiolas').value, 10);
    if(isNaN(cant) || cant < 0 || cant > 10){ ilusToast('La cantidad de piolas debe ser un número entre 0 y 10', {type:'warning'}); return; }
    // 2026-07-14: familia/clase (PATCH admin-only) solo se piden/guardan
    // si es superadmin -- para roles ampliados el bloque está oculto
    // (ver catwAbrir) y se salta directo a piolas, que sí es su permiso.
    if(!IS_SUPERADMIN){ catwStep = 3; catwMostrarPaso(); return; }
    const familia = document.getElementById('catwFamilia').value.trim();
    const clase = document.getElementById('catwClase').value;
    // 2026-08-28 (Daniel: "obligatoria al menos una -- la clasificación o
    // las piolas o el manual"): clasificación deja de ser obligatoria PARA
    // AVANZAR -- basta con una de las tres (mismo criterio OR que ya usa
    // "registrado" en catalogo_module.py desde el 23-jul). Si de verdad no
    // hay nada que guardar en este paso, se avanza sin llamar al backend.
    if(!familia && !clase){ catwStep = 3; catwMostrarPaso(); return; }
    btn.disabled = true; btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Guardando…';
    try{
      const r = await fetch('/catalogo/api/productos/'+catwProductoId, {method:'PATCH',
        headers:{'Content-Type':'application/json'}, body: JSON.stringify({familia: familia, clase_producto: clase})});
      const d = await r.json();
      btn.disabled = false; btn.innerHTML = original;
      if(!d.ok){ ilusToast(d.error||'No se pudo guardar la familia', {type:'error'}); return; }
      if (familia) _catFamilias.add(familia);
      catwStep = 3; catwMostrarPaso();
    }catch(e){ btn.disabled = false; btn.innerHTML = original; ilusToast('Sin conexión', {type:'error'}); }
    return;
  }
  if(catwStep === 3){
    // 2026-08-28 (Daniel: "obligatoria al menos una"): una fila de piola a
    // medio llenar ya NO bloquea avanzar -- se guardan solo las filas
    // completas (medida Y observación) y las incompletas se ignoran en
    // silencio, en vez de exigir TODAS antes de dejar pasar al paso 4.
    const filas = Array.from(document.querySelectorAll('.catw-piola-row')).filter(function(fila){
      const medida = parseFloat(fila.querySelector('.catw-piola-medida').value);
      const obs = fila.querySelector('.catw-piola-obs').value.trim();
      return !isNaN(medida) && medida > 0 && !!obs;
    });
    if(!filas.length){ catwStep = 4; catwMostrarPaso(); return; }
    btn.disabled = true; btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Guardando…';
    let _omitidas = 0;
    try{
      for(const fila of filas){
        const medida = parseFloat(fila.querySelector('.catw-piola-medida').value);
        const obs = fila.querySelector('.catw-piola-obs').value.trim();
        const piolaId = fila.dataset.piolaId;
        if(piolaId){
          // 2026-07-14: editar una piola YA existente (PATCH) es
          // _catalogo_admin_required (solo superadmin, decisión explícita
          // de Daniel 2026-07-12) -- crear una piola NUEVA (POST) sí es de
          // rol ampliado. Para no lanzar un PATCH que el backend va a
          // rechazar, un rol ampliado la omite y avisa al terminar.
          if(!IS_SUPERADMIN){ _omitidas++; continue; }
          await fetch('/catalogo/api/productos/'+catwProductoId+'/piolas/'+piolaId, {method:'PATCH',
            headers:{'Content-Type':'application/json'}, body: JSON.stringify({medida_cm: medida, observacion: obs})});
        } else {
          await fetch('/catalogo/api/productos/'+catwProductoId+'/piolas', {method:'POST',
            headers:{'Content-Type':'application/json'}, body: JSON.stringify({medida_cm: medida, observacion: obs})});
        }
      }
      btn.disabled = false; btn.innerHTML = original;
      if(_omitidas > 0){
        ilusToast('✓ Piolas nuevas guardadas — ' + _omitidas + ' piola(s) ya existentes no se modificaron (solo el superadministrador puede editarlas)', {type:'warning'});
      } else {
        ilusToast('✓ Piolas guardadas', {type:'success'});
      }
      catwStep = 4; catwMostrarPaso();
    }catch(e){ btn.disabled = false; btn.innerHTML = original; ilusToast('Sin conexión al guardar piolas', {type:'error'}); }
    return;
  }
  if(catwStep === 4){
    catwModal.hide();
    ilusToast('✓ Ficha del producto registrada', {type:'success'});
    cargarCatalogo();
    _catwAbrirSiguienteDeCola(); // sigue con el próximo producto de "Buscar en ERP", si hay
    return;
  }
});

function catwRenderPiolasInputs(){
  const cant = parseInt(document.getElementById('catwCantPiolas').value, 10) || 0;
  const wrap = document.getElementById('catwPiolasWrap');
  if(cant === 0){
    wrap.innerHTML = '<div class="text-muted small">Este producto no tiene piolas — puedes avanzar directo.</div>';
    return;
  }
  let html = '';
  for(let i = 0; i < cant; i++){
    const existente = catwPiolasExistentes[i];
    const label = 'Piola ' + (i+1);
    // 2026-07-14: una piola YA existente solo la edita el superadmin
    // (PATCH admin-only) -- para un rol ampliado se muestra bloqueada
    // en vez de dejar que el usuario la edite y esa edición se pierda
    // silenciosamente al guardar (ver handler de "Siguiente" paso 3).
    const bloqueada = existente && !IS_SUPERADMIN;
    const dis = bloqueada ? 'disabled title="Solo el superadministrador puede editar una piola ya existente"' : '';
    html += '<div class="catw-piola-row" data-piola-id="'+(existente ? existente.id : '')+'" data-label="'+label+'">'
      + '<span class="lbl">'+label+(bloqueada ? ' <i class="bi bi-lock-fill text-muted" style="font-size:.7rem;"></i>' : '')+'</span>'
      + '<input type="number" class="form-control form-control-sm catw-piola-medida" style="max-width:110px;" min="0.1" step="0.1" '+dis+' '
      +   'placeholder="Medida (cm)" value="'+(existente && existente.medida_cm_legada != null ? existente.medida_cm_legada : '')+'">'
      + '<input type="text" class="form-control form-control-sm catw-piola-obs" placeholder="Observación (ej: cable de polea alta)" '+dis+' '
      +   'value="'+esc(existente ? (existente.observacion||'') : '')+'">'
      + '</div>';
  }
  wrap.innerHTML = html;
}

function catwRenderManuales(){
  const lista = document.getElementById('catwManualesLista');
  if(!catwManuales.length){
    lista.innerHTML = '';
  } else {
    lista.innerHTML = catwManuales.map(function(m){
      // 2026-07-14: eliminar manual (DELETE) es _catalogo_admin_required
      // (solo superadmin) -- se oculta el botón para roles ampliados,
      // que solo pueden ver/descargar los manuales ya cargados.
      const btnDel = IS_SUPERADMIN
        ? '<button type="button" class="btn btn-sm btn-outline-danger" onclick="catwEliminarManual('+m.id+')"><i class="bi bi-trash"></i></button>'
        : '';
      // 2026-08-25: descargar (guardar el PDF) es un permiso aparte de
      // ver -- ver CAN_DESCARGAR_MANUAL arriba. "Ver" nunca se oculta.
      const btnDescargar = CAN_DESCARGAR_MANUAL
        ? '<a class="btn btn-sm btn-outline-secondary" href="'+esc(m.url)+'?download=1"><i class="bi bi-download"></i></a>'
        : '';
      return '<div class="catw-manual-item" data-manual-id="'+m.id+'">'
        + '<i class="bi bi-file-earmark-pdf-fill ic"></i>'
        + '<div class="nm">'+esc(m.nombre)+'</div>'
        + '<button type="button" class="btn btn-sm btn-outline-primary" title="Ver" data-ver-url="'+esc(m.url)+'" data-ver-nombre="'+esc(m.nombre)+'"><i class="bi bi-eye"></i></button>'
        + btnDescargar
        + btnDel
        + '</div>';
    }).join('');
    lista.querySelectorAll('[data-ver-url]').forEach(function(btn){
      btn.addEventListener('click', function(){
        catvAbrir(btn.getAttribute('data-ver-url'), btn.getAttribute('data-ver-nombre'));
      });
    });
  }
  const drop = document.getElementById('catwManualDrop');
  const hint = document.getElementById('catwManualHint');
  const aviso = document.getElementById('catwManualAviso');
  // 2026-07-23: subir manual (POST) es _catalogo_producto_write_required
  // (mantenciones o superadmin) -- para roles SIN ninguno de los dos se
  // oculta el dropzone entero y se muestra el aviso en su lugar, en vez de
  // dejarlos subir y recibir un 403 silencioso. (Corregido 2026-08-25: acá
  // decía IS_SUPERADMIN y le escondía el dropzone a un técnico que el
  // backend sí habría aceptado -- Daniel: "es importante que el usuario
  // suba lo que son los manuales".)
  if(!CAN_CARGAR_PIOLAS){
    drop.style.display = 'none';
    aviso.style.display = '';
  } else if(catwManuales.length >= MAX_MANUALES_WIZ){
    drop.style.display = 'none';
    aviso.style.display = 'none';
  } else {
    drop.style.display = '';
    aviso.style.display = 'none';
    hint.textContent = 'Hasta 25 MB · ' + catwManuales.length + '/' + MAX_MANUALES_WIZ + ' manuales';
  }
}

document.getElementById('catwManualDrop').addEventListener('click', function(){
  document.getElementById('catwManualInput').click();
});
document.getElementById('catwManualInput').addEventListener('change', async function(){
  const f = this.files[0];
  this.value = '';
  if(!f) return;
  if(catwManuales.length >= MAX_MANUALES_WIZ){ ilusToast('Máximo '+MAX_MANUALES_WIZ+' manuales por producto', {type:'warning'}); return; }
  const fd = new FormData(); fd.append('file', f);
  try{
    const r = await fetch('/catalogo/api/productos/'+catwProductoId+'/manuales', {method:'POST', body: fd});
    const d = await r.json();
    if(!d.ok){ ilusToast(d.error||'No se pudo subir el manual', {type:'error'}); return; }
    catwManuales.push({id: d.id, nombre: d.nombre, size_kb: d.size_kb,
      url: '/catalogo/api/productos/'+catwProductoId+'/manuales/'+d.id+'/descargar'});
    catwRenderManuales();
    ilusToast('✓ Manual subido', {type:'success'});
  }catch(e){ ilusToast('Sin conexión al subir el manual', {type:'error'}); }
});

window.catwEliminarManual = async function(manualId){
  const ok = await ilusConfirm({
    title: 'Quitar manual', message: '¿Quitar este manual del producto?',
    okLabel: 'Quitar', cancelLabel: 'Cancelar', danger: true,
  });
  if(!ok) return;
  try{
    const r = await fetch('/catalogo/api/productos/'+catwProductoId+'/manuales/'+manualId, {method:'DELETE'});
    const d = await r.json();
    if(!d.ok){ ilusToast(d.error||'No se pudo quitar el manual', {type:'error'}); return; }
    catwManuales = catwManuales.filter(m => m.id !== manualId);
    catwRenderManuales();
  }catch(e){ ilusToast('Sin conexión', {type:'error'}); }
};

cargarCatalogo();

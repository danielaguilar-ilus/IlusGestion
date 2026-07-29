/* ==================================================================
   cubicador_asignar.js - Asignar y Cotizar (Cubicador) - ILUS
   Extraido TAL CUAL desde templates/cubicador/asignar.html (2 bloques
   <script> del template original, concatenados en el mismo orden en
   que aparecian en la pagina; sin variables Jinja -- cero interpolaciones
   ni bloques de plantilla en ninguno de los dos). Cargado con defer: cubicador_tabs.js
   (que se sigue cargando de forma sincronica despues de este archivo)
   y el resto de la pagina no dependen de que este script corra ANTES
   del DOMContentLoaded, solo despues (ver listeners DOMContentLoaded
   dentro de este mismo archivo).
================================================================== */

// ════════════════════════════════════════════════════════════
//  ESTADO GLOBAL
// ════════════════════════════════════════════════════════════
let _docData      = null;
let _courierSel   = null;   // ahora es objeto {id, nombre, precio, transito, ...} o null
let _precioFedex  = null;   // legacy — el cotizador unificado lo deja en _cotizaciones
let _zonaActual   = null;   // '1'..'6' — sólo informativo para el badge de zona
let _preciosManuales = {};  // legacy — ya no se usa (cotización viene del backend)

// Límite físico de FedEx: NO acepta bultos de más de 68kg cada uno — un
// pedido con algún bulto sobre ese peso no puede despacharse automático por
// FedEx (API/manifiesto), Alison debe gestionarlo manual con otro courier.
// Alerta VISUAL, no bloqueante (pedido Daniel 2026-07-29, previo a directorio).
const FEDEX_MAX_KG_POR_BULTO = 68;

// ════════════════════════════════════════════════════════════
//  FORMATO NUMÉRICO CHILENO
// ════════════════════════════════════════════════════════════
function fCl(n, dec=1){
  if(n==null||isNaN(n)) return '—';
  const r = parseFloat(parseFloat(n).toFixed(dec));
  const [ip, dp='0'] = r.toFixed(dec).split('.');
  const neg=ip.startsWith('-'), abs=ip.replace('-','');
  let s='';
  for(let i=0;i<abs.length;i++){
    if(i>0&&(abs.length-i)%3===0) s+='.';
    s+=abs[i];
  }
  return (neg?'-':'')+s+','+dp;
}
function fVol(n){
  if(n==null||isNaN(n)) return '—';
  // Volumen en m³ (entra cm³): 5673482 → '5,673'
  return (parseFloat(n)/1000000).toFixed(3).replace('.', ',');
}
function fClp(n){
  if(n==null||isNaN(n)) return '—';
  const v = Math.round(parseFloat(n));
  const neg = v < 0, abs = Math.abs(v).toString();
  let s='';
  for(let i=0;i<abs.length;i++){
    if(i>0&&(abs.length-i)%3===0) s+='.';
    s+=abs[i];
  }
  return (neg?'-':'')+'$'+s;
}

// ════════════════════════════════════════════════════════════
//  COMUNAS CHILENAS (todas las comunas del territorio nacional)
// ════════════════════════════════════════════════════════════
// Formato: [nombre, zona_fedex]
// Zonas: 1=RM, 2=V/VI/VII, 3=VIII/IX, 4=XIV/X, 5=XI/XII, 6=I/II/III/IV
const COMUNAS = [
  // Región Metropolitana — Zona 1
  ['Santiago','1'],['Providencia','1'],['Las Condes','1'],['Vitacura','1'],['Ñuñoa','1'],
  ['La Florida','1'],['Maipú','1'],['Pudahuel','1'],['Quilicura','1'],['Renca','1'],
  ['Cerro Navia','1'],['Estación Central','1'],['Peñalolén','1'],['Macul','1'],
  ['San Joaquín','1'],['La Granja','1'],['La Pintana','1'],['El Bosque','1'],
  ['San Bernardo','1'],['Buin','1'],['Pirque','1'],['Calera de Tango','1'],
  ['Talagante','1'],['Paine','1'],['Isla de Maipo','1'],['Melipilla','1'],
  ['Colina','1'],['Lampa','1'],['Tiltil','1'],['Peñaflor','1'],['Curacaví','1'],
  ['María Pinto','1'],['Alhué','1'],['Padre Hurtado','1'],['El Monte','1'],
  ['Lo Barnechea','1'],['Huechuraba','1'],['Recoleta','1'],['Independencia','1'],
  ['Lo Prado','1'],['Quinta Normal','1'],['Lo Espejo','1'],['San Miguel','1'],
  ['La Cisterna','1'],['Pedro Aguirre Cerda','1'],['San Ramón','1'],['La Reina','1'],
  ['Puente Alto','1'],['San José de Maipo','1'],['Conchalí','1'],
  // Región de Valparaíso — Zona 2
  ['Valparaíso','2'],['Viña del Mar','2'],['Concón','2'],['Quilpué','2'],
  ['Villa Alemana','2'],['San Antonio','2'],['Quillota','2'],['La Calera','2'],
  ['Los Andes','2'],['San Felipe','2'],['Limache','2'],['Olmué','2'],
  ['Cabildo','2'],['Petorca','2'],['La Ligua','2'],['Zapallar','2'],
  ['Papudo','2'],['Puchuncaví','2'],['Quintero','2'],['Casablanca','2'],
  ['El Quisco','2'],['El Tabo','2'],['Cartagena','2'],['Santo Domingo','2'],
  ['Algarrobo','2'],['San Esteban','2'],['Catemu','2'],['Llaillay','2'],['Putaendo','2'],
  ['Santa María','2'],['Panquehue','2'],['Calle Larga','2'],['Rinconada','2'],
  ['Isla de Pascua','2'],['Juan Fernández','2'],
  // Región de O'Higgins — Zona 2
  ['Rancagua','2'],['Machalí','2'],['Graneros','2'],['Mostazal','2'],['Codegua','2'],
  ['Olivar','2'],['Coinco','2'],['Coltauco','2'],['Doñihue','2'],['Las Cabras','2'],
  ['Peumo','2'],['Pichidegua','2'],['San Vicente','2'],['Rengo','2'],['Requínoa','2'],
  ['Malloa','2'],['Quinta de Tilcoco','2'],['San Fernando','2'],['Chimbarongo','2'],
  ['Nancagua','2'],['Placilla','2'],['Palmilla','2'],['Peralillo','2'],
  ['Santa Cruz','2'],['Lolol','2'],['Pumanque','2'],['Marchihue','2'],
  ['Paredones','2'],['Pichilemu','2'],['Litueche','2'],['La Estrella','2'],
  ['Navidad','2'],['Peumo','2'],
  // Región del Maule — Zona 2
  ['Talca','2'],['Curicó','2'],['Linares','2'],['Constitución','2'],['Cauquenes','2'],
  ['Molina','2'],['Teno','2'],['Romeral','2'],['Sagrada Familia','2'],['Hualañé','2'],
  ['Licantén','2'],['Vichuquén','2'],['Rauco','2'],['San Clemente','2'],
  ['Pencahue','2'],['Río Claro','2'],['San Rafael','2'],['Pelarco','2'],
  ['Maule','2'],['Curepto','2'],['Empedrado','2'],['San Javier','2'],['Villa Alegre','2'],
  ['Yerbas Buenas','2'],['Colbún','2'],['Longaví','2'],['Parral','2'],['Retiro','2'],
  ['Villa Alemana','2'],
  // Región del Biobío — Zona 3
  ['Concepción','3'],['Talcahuano','3'],['Hualpén','3'],['San Pedro de la Paz','3'],
  ['Coronel','3'],['Lota','3'],['Tomé','3'],['Penco','3'],['Chiguayante','3'],
  ['Hualqui','3'],['Santa Juana','3'],['Florida','3'],['Arauco','3'],['Cañete','3'],
  ['Lebu','3'],['Los Álamos','3'],['Curanilahue','3'],['Contulmo','3'],['Tirúa','3'],
  ['Los Ángeles','3'],['Chillán','3'],['Nacimiento','3'],['Negrete','3'],['Mulchén','3'],
  ['Quilaco','3'],['Quilleco','3'],['Santa Bárbara','3'],['Antuco','3'],['Tucapel','3'],
  ['Yumbel','3'],['Cabrero','3'],['Laja','3'],['San Rosendo','3'],['Chillán Viejo','3'],
  ['Bulnes','3'],['Cobquecura','3'],['Coelemu','3'],['Ñiquén','3'],['San Carlos','3'],
  ['San Fabián','3'],['San Nicolás','3'],['Ninhue','3'],['Portezuelo','3'],
  ['Quirihue','3'],['Ranquil','3'],['Treguaco','3'],['Pemuco','3'],['El Carmen','3'],
  ['Pinto','3'],['Coihueco','3'],['Yungay','3'],['San Ignacio','3'],
  // Región de La Araucanía — Zona 3
  ['Temuco','3'],['Padre las Casas','3'],['Villarrica','3'],['Pucón','3'],['Angol','3'],
  ['Victoria','3'],['Lautaro','3'],['Freire','3'],['Gorbea','3'],['Loncoche','3'],
  ['Panguipulli','3'],['Curacautín','3'],['Lonquimay','3'],['Melipeuco','3'],
  ['Cunco','3'],['Vilcún','3'],['Perquenco','3'],['Galvarino','3'],['Collipulli','3'],
  ['Ercilla','3'],['Los Sauces','3'],['Lumaco','3'],['Purén','3'],['Traiguén','3'],
  ['Lebu','3'],['Renaico','3'],['Pitrufquén','3'],['Toltén','3'],['Teodoro Schmidt','3'],
  ['Saavedra','3'],['Carahue','3'],['Nueva Imperial','3'],['Puerto Saavedra','3'],
  ['Cholchol','3'],
  // Región de Los Ríos — Zona 4
  ['Valdivia','4'],['La Unión','4'],['Río Bueno','4'],['Lago Ranco','4'],
  ['Futrono','4'],['Panguipulli','4'],['Los Lagos','4'],['Corral','4'],['Mariquina','4'],
  ['Lanco','4'],['Máfil','4'],['Paillaco','4'],
  // Región de Los Lagos — Zona 4
  ['Puerto Montt','4'],['Puerto Varas','4'],['Osorno','4'],['Castro','4'],['Ancud','4'],
  ['Quellón','4'],['Calbuco','4'],['Maullín','4'],['Los Muermos','4'],['Frutillar','4'],
  ['Llanquihue','4'],['Purranque','4'],['Puerto Octay','4'],['Fresia','4'],['Nancagua','4'],
  ['San Pablo','4'],['Puyehue','4'],['Río Negro','4'],['Puerto Aysén','4'],
  ['San Juan de la Costa','4'],['Chaitén','4'],['Futaleufú','4'],['Palena','4'],
  ['Hualaihué','4'],['Quemchi','4'],['Dalcahue','4'],['Curaco de Vélez','4'],
  ['Puqueldón','4'],['Chonchi','4'],['Queilén','4'],['Quinchao','4'],
  // Región de Aysén — Zona 5
  ['Coyhaique','5'],['Puerto Aysén','5'],['Chile Chico','5'],['Cochrane','5'],
  ['O\'Higgins','5'],['Tortel','5'],['Cisnes','5'],['Guaitecas','5'],['Lago Verde','5'],
  ['Río Ibáñez','5'],['Puerto Cisnes','5'],
  // Región de Magallanes — Zona 5
  ['Punta Arenas','5'],['Puerto Natales','5'],['Porvenir','5'],['Primavera','5'],
  ['Timaukel','5'],['Laguna Blanca','5'],['Río Verde','5'],['San Gregorio','5'],
  ['Cabo de Hornos','5'],['Antártica','5'],
  // Región de Tarapacá — Zona 6
  ['Iquique','6'],['Alto Hospicio','6'],['Pozo Almonte','6'],['Pica','6'],
  ['Colchane','6'],['Camiña','6'],['Huara','6'],
  // Región de Arica y Parinacota — Zona 6
  ['Arica','6'],['Camarones','6'],['Putre','6'],['General Lagos','6'],
  // Región de Antofagasta — Zona 6
  ['Antofagasta','6'],['Calama','6'],['Tocopilla','6'],['Mejillones','6'],
  ['Taltal','6'],['San Pedro de Atacama','6'],['Ollagüe','6'],['María Elena','6'],
  // Región de Atacama — Zona 6
  ['Copiapó','6'],['Caldera','6'],['Chañaral','6'],['Diego de Almagro','6'],
  ['Vallenar','6'],['Freirina','6'],['Huasco','6'],['Alto del Carmen','6'],['Tierra Amarilla','6'],
  // Región de Coquimbo — Zona 6
  ['La Serena','6'],['Coquimbo','6'],['Ovalle','6'],['Illapel','6'],['Los Vilos','6'],
  ['Salamanca','6'],['Canela','6'],['Monte Patria','6'],['Punitaqui','6'],['Río Hurtado','6'],
  ['Vicuña','6'],['Paihuano','6'],['La Higuera','6'],['Paiguano','6'],['Andacollo','6'],
  ['Combarbalá','6'],
];

// Mapa rápido nombre→zona (normalizado)
const COMUNAS_ZONA_MAP = {};
const COMUNAS_NOMBRES = [];
for(const [nom, zona] of COMUNAS){
  COMUNAS_NOMBRES.push(nom);
  COMUNAS_ZONA_MAP[nom.toLowerCase().normalize('NFD').replace(/[̀-ͯ]/g,'')] = {nombre:nom, zona};
}

const ZONA_NOMBRES = {
  '1':'Col. 1 — Región Metropolitana',
  '2':'Col. 2 — V, VI, VII Región',
  '3':'Col. 3 — VIII, IX Región',
  '4':'Col. 4 — XIV, X Región',
  '5':'Col. 5 — XI, XII Región',
  '6':'Col. 6 — I, II, III, IV Región',
};

function comunaNormalizada(s){
  return s.toLowerCase().normalize('NFD').replace(/[̀-ͯ]/g,'').trim();
}
function getZona(comunaStr){
  const key = comunaNormalizada(comunaStr);
  return COMUNAS_ZONA_MAP[key]?.zona || null;
}

// ════════════════════════════════════════════════════════════
//  AUTOCOMPLETE COMUNAS
// ════════════════════════════════════════════════════════════
let _acIdx = -1;

function acFilter(val){
  const dd = document.getElementById('acDropdown');
  if(!val || val.length < 1){ dd.classList.remove('open'); return; }
  const q = comunaNormalizada(val);
  const matches = COMUNAS_NOMBRES.filter(n => comunaNormalizada(n).includes(q)).slice(0,10);
  if(!matches.length){ dd.classList.remove('open'); return; }
  dd.innerHTML = matches.map((m,i)=>{
    const hi = m.replace(new RegExp('('+val.replace(/[.*+?^${}()|[\]\\]/g,'\\$&')+')', 'gi'),'<mark>$1</mark>');
    return `<div class="ac-option" data-val="${m}" onmousedown="acSelect('${m.replace(/'/g,"\\'")}')">${hi}</div>`;
  }).join('');
  _acIdx = -1;
  dd.classList.add('open');
}

function acSelect(val){
  document.getElementById('cli-comuna').value = val;
  document.getElementById('acDropdown').classList.remove('open');
  onComunaChange(val);
}

function acKeydown(e){
  const dd = document.getElementById('acDropdown');
  const opts = dd.querySelectorAll('.ac-option');
  if(!opts.length) return;
  if(e.key==='ArrowDown'){ e.preventDefault(); _acIdx=Math.min(_acIdx+1,opts.length-1); acHighlight(opts); }
  else if(e.key==='ArrowUp'){ e.preventDefault(); _acIdx=Math.max(_acIdx-1,-1); acHighlight(opts); }
  else if(e.key==='Enter'&&_acIdx>=0){
    e.preventDefault();
    acSelect(opts[_acIdx].dataset.val);
  } else if(e.key==='Escape'){
    dd.classList.remove('open');
  }
}
function acHighlight(opts){
  opts.forEach((o,i)=>o.classList.toggle('focused',i===_acIdx));
  if(_acIdx>=0) opts[_acIdx].scrollIntoView({block:'nearest'});
}

document.addEventListener('click', e=>{
  if(!e.target.closest('.autocomplete-wrap')) document.getElementById('acDropdown').classList.remove('open');
});

function onComunaChange(val){
  const zona = getZona(val);
  _zonaActual = zona;
  // Sincronizar _docData.header.comuna también (lo lee el cotizador)
  if(_docData && _docData.header) _docData.header.comuna = val || '';
  if(zona){
    document.getElementById('zonaDetectadaWrap').style.display='';
    document.getElementById('zonaDetectadaTag').textContent = 'Col. '+zona;
    document.getElementById('zonaDetectadaNombre').textContent = ZONA_NOMBRES[zona]||'';
  } else {
    document.getElementById('zonaDetectadaWrap').style.display='none';
  }
  if((val||'').trim()){
    document.getElementById('avisoComuna').style.display='none';
    document.getElementById('courierList').classList.remove('courier-locked');
    actualizarTarifas();
  } else {
    document.getElementById('avisoComuna').style.display='';
    document.getElementById('courierList').classList.add('courier-locked');
    _cotizaciones = [];
    _recomendadoId = null;
    renderCouriers();
  }
}

// ════════════════════════════════════════════════════════════
//  VALIDAR + NORMALIZAR TELÉFONO CHILENO (inteligente, Daniel 2026-06-14)
//  Acepta "9 1234 5678", "912345678", "56912345678", "+56 9 1234 5678",
//  "0912345678" → normaliza a "+56 9 XXXX XXXX". Le pone el +56 9 solo.
// ════════════════════════════════════════════════════════════
function _normalizarTelefonoCl(raw){
  let d = (raw || '').replace(/[^\d]/g, '');     // solo dígitos
  if (d.startsWith('56')) d = d.slice(2);        // quita código país
  d = d.replace(/^0+/, '');                       // quita ceros iniciales
  if (d.length === 9 && d[0] === '9') return '+56' + d;   // móvil completo 9XXXXXXXX
  if (d.length === 8) return '+569' + d;          // faltó el 9 → asumir móvil
  return null;                                    // no se pudo normalizar con confianza
}
function _telPretty(norm){
  const n = norm.slice(3);                        // 9XXXXXXXX
  return `+56 ${n.slice(0,1)} ${n.slice(1,5)} ${n.slice(5)}`;
}
function validarTelefono(input){
  const hint = document.getElementById('tel-hint');
  const raw = (input.value || '').trim();
  if (!raw){ hint.textContent = ''; hint.className = 'tel-hint'; input.classList.remove('required-empty'); return; }
  const norm = _normalizarTelefonoCl(raw);
  if (norm){
    hint.textContent = '✓ ' + _telPretty(norm);
    hint.className = 'tel-hint ok';
    input.classList.remove('required-empty');
  } else {
    hint.textContent = 'Teléfono chileno: +56 9 XXXX XXXX';
    hint.className = 'tel-hint err';
    input.classList.add('required-empty');
  }
}
// Al salir del campo, deja el número ya formateado bonito (+56 9 XXXX XXXX).
function normalizarTelefonoBlur(input){
  const norm = _normalizarTelefonoCl(input.value || '');
  if (norm){ input.value = _telPretty(norm); validarTelefono(input); }
}

// ════════════════════════════════════════════════════════════
//  SINCRONIZAR BULTOS
// ════════════════════════════════════════════════════════════
function sincronizarBultos(val){
  // Actualiza el resumen panel
  const n = parseInt(val)||0;
  const rb = document.getElementById('res-bultos');
  if(rb) rb.textContent = n;
}

// ════════════════════════════════════════════════════════════
//  ZZ ENVÍO EDITABLE (Daniel 2026-07-22)
// ════════════════════════════════════════════════════════════
// El ERP trae el ZZ Envío TOTAL de la factura. Si Daniel está despachando
// solo una PARTE de esa factura (envío parcializado), ese total no
// corresponde al envío actual — y de ahí sale el margen (zzVal - precio
// courier) que se muestra junto a cada cotización (ver renderCouriers).
// Editar este campo corrige _docData.zzenvio_valor y vuelve a pintar el
// margen al instante, sin pedir una cotización nueva al backend.
function actualizarZzEnvioManual(val){
  if(!_docData) return;
  const n = Math.max(0, parseFloat(val) || 0);
  _docData.zzenvio_valor = n;
  const rs = document.getElementById('res-skus');
  if(rs) rs.textContent = n > 0 ? fClp(n) : '—';
  // Re-pinta el margen de las cotizaciones YA obtenidas con el nuevo ZZ Envío
  // (no hace falta re-cotizar: el precio del courier no cambia, solo el margen).
  if (typeof renderCouriers === 'function' && _cotizaciones && _cotizaciones.length) {
    renderCouriers();
  }
}

// Guarda el saldo de ZZ Envío EN EL SERVIDOR (Daniel 2026-07-22): un despacho
// parcializado deja un saldo que debe recordarse para la próxima vez que se
// consulte el mismo documento — no solo mientras dure esta pestaña abierta.
async function guardarZzEnvioSaldo(){
  if(!_docData || !_docData.header) return;
  const tido = _docData.header.tido, nudo = _docData.header.nudo;
  if(!tido || !nudo){
    if(window.ilusToast) ilusToast('Busca un documento primero', {type:'warning'});
    return;
  }
  const el = document.getElementById('cli-zzenvio');
  const saldo = Math.max(0, parseFloat(el ? el.value : 0) || 0);
  try {
    const r = await fetch('/api/asignar/zzenvio-saldo', {
      method: 'PUT',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({
        tido, nudo, saldo,
        valor_original: _docData._zzenvioValorAlCargar || saldo,
      }),
    });
    const d = await r.json();
    if(!r.ok || d.error) throw new Error(d.error || ('HTTP '+r.status));
    _docData._zzenvioValorAlCargar = saldo;  // ya es el nuevo "punto de partida"
    _docData.zzenvio_valor    = saldo;
    _docData.zzenvio_es_saldo = true;
    const badge = document.getElementById('zz-saldo-badge');
    if(badge) badge.style.display = '';
    // FIX 2026-07-22 (revisión adversarial): _docCache (TTL 90s) guardaba la
    // respuesta de ANTES de guardar el saldo. Si se re-busca el mismo
    // documento dentro de esos 90s, el cache-hit devolvía zzenvio_es_saldo
    // desactualizado y el badge desaparecía (el número seguía correcto, solo
    // el aviso visual se perdía). Se actualiza la entrada cacheada también.
    const _cacheKey = `${tido}|${nudo}`;
    const _cached = _docCache.get(_cacheKey);
    if(_cached && _cached.data){
      _cached.data.zzenvio_valor    = saldo;
      _cached.data.zzenvio_es_saldo = true;
    }
    if(window.ilusToast) ilusToast('✓ Saldo de ZZ Envío guardado — quedará disponible la próxima vez que busques este documento', {type:'success'});
  } catch(e){
    if(window.ilusToast) ilusToast('Error al guardar el saldo: '+e.message, {type:'error'});
  }
}

// ════════════════════════════════════════════════════════════
//  VALOR NETO EDITABLE (Daniel 2026-07-22)
// ════════════════════════════════════════════════════════════
// El ERP trae el valor neto TOTAL de la factura. Igual que ZZ Envío, si el
// despacho es parcial ese total no corresponde al Seguro (1,2%) de ESTE
// envío. Editar recalcula el Seguro al instante (en pantalla y en el
// resumen) y actualiza _docData.header.valor_neto para que cualquier otro
// cálculo que lo use (ej. al re-cotizar) tome el valor corregido.
function actualizarValorNetoManual(val){
  if(!_docData || !_docData.header) return;
  const n = Math.max(0, parseFloat(val) || 0);
  _docData.header.valor_neto = n;
  const seguro = n * 0.012;
  setText('cli-seguro', fClp(seguro));
  setText('res-neto',   n > 0 ? fClp(n) : '—');
  setText('res-seguro', fClp(seguro));
}

// ════════════════════════════════════════════════════════════
//  BUSCAR DOCUMENTO
// ════════════════════════════════════════════════════════════
// Cache JS de documentos buscados (TTL 90s, mismo que backend)
const _docCache = new Map();
const _docCacheTTL = 90_000;

// Helper: escapar HTML para evitar XSS al inyectar datos del ERP
function escHtml(s){
  return String(s||'').replace(/[&<>"']/g, c =>
    ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
}

// Exporta el documento actual como HTML imprimible (PDF vía Ctrl+P)
function exportarDocumento(){
  if (!_docData){ alert('Primero busca un documento'); return; }
  const h = _docData.header || {};
  const d = _docData;
  const tot = d.totales || {};
  const lineas = d.lineas || [];
  const fmt = v => (v||0).toLocaleString('es-CL',{minimumFractionDigits:0,maximumFractionDigits:0});
  const fClp = v => '$' + fmt(Math.round(v||0));

  const win = window.open('', '_blank');
  const html = `<!doctype html><html><head><meta charset="utf-8">
<title>Documento ${h.tido} ${h.nudo} — ${h.cliente_nombre||''}</title>
<style>
body{font-family:Arial,sans-serif;color:#0a0a0a;padding:30px;max-width:900px;margin:auto}
.hdr{display:flex;justify-content:space-between;align-items:start;border-bottom:3px solid #dc2626;padding-bottom:14px;margin-bottom:20px}
.hdr h1{margin:0;font-size:1.6rem;color:#dc2626;letter-spacing:-.01em}
.hdr .meta{text-align:right;font-size:.84rem;color:#6b7280}
.info-grid{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:24px}
.info-cell{background:#f9fafb;border-radius:8px;padding:10px 14px;border-left:3px solid #dc2626}
.info-cell .lbl{font-size:.65rem;text-transform:uppercase;color:#6b7280;font-weight:700;letter-spacing:.06em;margin-bottom:3px}
.info-cell .val{font-size:.95rem;font-weight:700;color:#0a0a0a}
table{width:100%;border-collapse:collapse;font-size:.84rem;margin-bottom:20px}
table th{background:#0f172a;color:#fff;padding:8px 10px;text-align:left;font-size:.7rem;text-transform:uppercase;letter-spacing:.04em}
table td{padding:7px 10px;border-bottom:1px solid #e5e7eb}
.totales{background:#0f172a;color:#fff;padding:14px 18px;border-radius:8px;display:flex;justify-content:space-around}
.totales > div{text-align:center}
.totales .lbl{font-size:.65rem;color:#9ca3af;text-transform:uppercase;letter-spacing:.05em}
.totales .val{font-size:1.25rem;font-weight:900;margin-top:3px}
.totales .val.red{color:#fee2e2}
.footer{margin-top:30px;padding-top:14px;border-top:1px solid #e5e7eb;text-align:center;font-size:.75rem;color:#6b7280}
@media print{body{padding:18px}}
</style></head><body>
<div class="hdr">
  <div>
    <h1>${h.tido||''} ${h.nudo_display||h.nudo||''}</h1>
    <div style="color:#6b7280;font-size:.86rem">Fecha emisión: <strong>${h.fecha||'—'}</strong></div>
  </div>
  <div class="meta">
    <div style="font-size:1.2rem;font-weight:900;color:#0a0a0a">ILUS</div>
    <div>Sport and Health Solutions SPA</div>
    <div>Bodega Quilicura</div>
  </div>
</div>

<div class="info-grid">
  <div class="info-cell"><div class="lbl">Cliente</div><div class="val">${h.cliente_nombre||'—'}</div></div>
  <div class="info-cell"><div class="lbl">RUT</div><div class="val">${h.cliente_rut||'—'}</div></div>
  <div class="info-cell"><div class="lbl">Email</div><div class="val">${h.email||'—'}</div></div>
  <div class="info-cell"><div class="lbl">Teléfono</div><div class="val">${h.telefono||'—'}</div></div>
  <div class="info-cell" style="grid-column:span 2"><div class="lbl">Dirección de despacho</div><div class="val">${h.direccion||'—'}</div></div>
  <div class="info-cell"><div class="lbl">Comuna</div><div class="val">${h.comuna||'—'}</div></div>
  <div class="info-cell"><div class="lbl">Valor neto</div><div class="val">${fClp(h.valor_neto)}</div></div>
  ${h.observaciones ? `<div class="info-cell" style="grid-column:span 2;background:#fff7ed;border-left-color:#f59e0b"><div class="lbl">Observaciones</div><div class="val">${h.observaciones}</div></div>` : ''}
</div>

<table>
  <thead><tr><th>SKU</th><th>Producto</th><th style="text-align:right">Cant</th><th style="text-align:right">Bultos</th><th style="text-align:right">Peso real (kg)</th><th style="text-align:right">Peso vol (kg)</th><th style="text-align:right">Predominante</th></tr></thead>
  <tbody>
    ${lineas.map(l => `<tr>
      <td style="font-family:monospace;color:#dc2626">${l.sku||''}</td>
      <td>${l.descripcion_erp||''}</td>
      <td style="text-align:right">${l.cantidad}</td>
      <td style="text-align:right">${l.bultos_tot||0}</td>
      <td style="text-align:right">${(l.peso_kg_u*l.cantidad).toFixed(2)}</td>
      <td style="text-align:right">${(l.peso_vol_u*l.cantidad).toFixed(2)}</td>
      <td style="text-align:right;font-weight:700;color:#dc2626">${(l.pred_tot||0).toFixed(2)} kg</td>
    </tr>`).join('')}
  </tbody>
</table>

<div class="totales">
  <div><div class="lbl">Cantidad</div><div class="val">${tot.total_qty||0}</div></div>
  <div><div class="lbl">Bultos</div><div class="val">${tot.total_bultos||0}</div></div>
  <div><div class="lbl">Peso real</div><div class="val">${(tot.peso_kg||0).toFixed(2)} kg</div></div>
  <div><div class="lbl">Peso volumétrico</div><div class="val">${(tot.peso_pv||0).toFixed(2)} kg</div></div>
  <div><div class="lbl">Predominante</div><div class="val red">${(tot.peso_pred||0).toFixed(2)} kg</div></div>
</div>

<div class="footer">
  Generado ${new Date().toLocaleString('es-CL')} · ILUS Sport and Health Solutions
</div>
<scr${''}ipt>setTimeout(()=>window.print(),300);</scr${''}ipt>
</body></html>`;
  win.document.write(html);
  win.document.close();
}

// ── PREFETCH: cuando el operador tipea un nudo, dispara la búsqueda en
//    background (debounce 700ms) si tiene >=4 dígitos. El backend cachea 5 min,
//    así que al apretar Enter la respuesta es instantánea (cache HIT local).
let _prefetchTimer = null;
let _prefetchInFlight = null;   // AbortController del último prefetch
async function _schedulePrefetch(){
  if (_prefetchTimer) { clearTimeout(_prefetchTimer); _prefetchTimer = null; }
  const nudo = (document.getElementById('docNudo').value || '').trim();
  const tido = document.getElementById('docTipo').value;
  if (nudo.length < 4) return;
  // Si ya está en cache local, no hace falta prefetch
  const key = `${tido}|${nudo}`;
  const cached = _docCache.get(key);
  if (cached && (Date.now() - cached.ts) < _docCacheTTL) return;
  _prefetchTimer = setTimeout(async () => {
    // Cancelar prefetch anterior en vuelo (el operador siguió tipeando)
    if (_prefetchInFlight){ try { _prefetchInFlight.abort(); } catch(e){} }
    _prefetchInFlight = new AbortController();
    try {
      const r = await fetch('/api/asignar/documento', {
        method: 'POST',
        headers: {'Content-Type':'application/json'},
        body: JSON.stringify({ tido, nudo, prefetch: true }),
        signal: _prefetchInFlight.signal,
      });
      if (!r.ok) return;
      const d = await r.json();
      if (d && d.ok){
        _docCache.set(key, { data: d, ts: Date.now() });
        if (_docCache.size > 30){
          _docCache.delete(_docCache.keys().next().value);
        }
        console.log('[asignar] prefetch OK ' + key + (d.from_cache ? ' (server cache)' : ''));
      }
    } catch(e){
      // Silencioso: prefetch no debe molestar al usuario
      if (e.name !== 'AbortError') console.warn('[asignar] prefetch fail', e);
    } finally {
      _prefetchInFlight = null;
    }
  }, 700);
}

async function buscarDoc(){
  // TELEMETRIA DEFENSIVA — si esto NO aparece en consola al hacer clic,
  // el JS esta roto antes de llegar aqui (typicamente otra etiqueta cerrando
  // el script literal, o un syntax error mas arriba).
  console.log('[asignar] buscarDoc() iniciado');

  const tido = document.getElementById('docTipo').value;
  const nudo = document.getElementById('docNudo').value.trim();
  console.log('[asignar] tido=', tido, 'nudo=', nudo);
  if(!nudo){ showAlert('alertBuscar','Ingresa el número de documento'); return; }
  hideAlert('alertBuscar');

  const cacheKey = `${tido}|${nudo}`;
  const cached = _docCache.get(cacheKey);
  if (cached && (Date.now() - cached.ts) < _docCacheTTL){
    console.log('[asignar] cache HIT', cacheKey);
    _docData = cached.data;
    renderDoc(cached.data);
    return;
  }

  // ── BARRA DE PROGRESO ──
  setBuscarLoading(true);
  const t0 = Date.now();
  showProgress([
    { ms: 200,  pct: 15, txt: '🔌 Conectando al ERP Random...' },
    { ms: 800,  pct: 35, txt: '📄 Buscando documento...' },
    { ms: 1800, pct: 55, txt: '👤 Consultando datos del cliente (variantes RUT)...' },
    { ms: 2800, pct: 75, txt: '📦 Calculando cubicaje y bultos...' },
    { ms: 4500, pct: 90, txt: '⏳ ERP demora... esperando respuesta' },
  ]);

  try {
    console.log('[asignar] POST /api/asignar/documento');
    const r = await fetch('/api/asignar/documento',{
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify({tido, nudo})
    });
    console.log('[asignar] response status=', r.status);
    const d = await r.json();
    console.log('[asignar] response body=', d);
    finishProgress();
    if(!r.ok||d.error){
      // Mostrar diagnóstico expandido si vino en la respuesta (motor ERP)
      let msg = d.error || `Error al consultar ERP (HTTP ${r.status})`;
      if (d.diagnostics){
        msg += `\n\nDiagnóstico:\n• Nudos probados: ${(d.diagnostics.nudo_tried||[]).join(', ')}\n• RUTs probados: ${(d.diagnostics.rut_tried||[]).join(', ')}\n• Latencia: ${d.diagnostics.latency_ms||'?'}ms`;
      }
      showAlert('alertBuscar', msg);
      console.warn('[asignar] búsqueda falló:', msg);
      return;
    }
    _docData = d;
    _docCache.set(cacheKey, { data: d, ts: Date.now() });
    if (_docCache.size > 30){
      _docCache.delete(_docCache.keys().next().value);
    }
    const elapsedMs = Date.now() - t0;
    console.log('[asignar] OK en', elapsedMs, 'ms — cliente:', d.header?.cliente_nombre);
    renderDoc(d, elapsedMs);
  } catch(e){
    finishProgress();
    console.error('[asignar] EXCEPCIÓN:', e);
    showAlert('alertBuscar','Error de conexión: '+e.message);
  } finally {
    setBuscarLoading(false);
  }
}

// ── Barra de progreso con mensajes secuenciales ─────────────
let _progressTimers = [];
function showProgress(steps){
  _progressTimers.forEach(t => clearTimeout(t));
  _progressTimers = [];
  // ── PREMIUM: casco ILUS girando (overlay full-screen) ──
  // Misma secuencia de mensajes del ERP, ahora con el loader de marca.
  // Fallback a la barra DOM clásica si ilusLoader no estuviera disponible.
  if (window.ilusLoader){
    ilusLoader.show({ text:'Iniciando búsqueda…', sub:'ERP Random · cloud.random.cl', progress:8 });
    steps.forEach(s => {
      _progressTimers.push(setTimeout(() => {
        ilusLoader.text(s.txt);
        ilusLoader.progress(s.pct);
      }, s.ms));
    });
    return;
  }
  const bar = document.getElementById('progressBar');
  const msg = document.getElementById('progressMsg');
  const fill = document.getElementById('progressFill');
  if (!bar) return;
  bar.style.display = 'block';
  fill.style.width = '8%';
  msg.textContent = 'Iniciando búsqueda...';
  steps.forEach(s => {
    _progressTimers.push(setTimeout(() => {
      fill.style.width = s.pct + '%';
      msg.textContent = s.txt;
    }, s.ms));
  });
}
function finishProgress(){
  _progressTimers.forEach(t => clearTimeout(t));
  _progressTimers = [];
  if (window.ilusLoader){
    ilusLoader.progress(100);
    ilusLoader.text('¡Listo!');
    setTimeout(() => ilusLoader.hide(), 350);
    const _b = document.getElementById('progressBar'); // por si quedó visible la barra clásica
    if (_b) _b.style.display = 'none';
    return;
  }
  const bar = document.getElementById('progressBar');
  const fill = document.getElementById('progressFill');
  if (!bar) return;
  fill.style.width = '100%';
  setTimeout(() => { bar.style.display = 'none'; fill.style.width = '0'; }, 400);
}

// ════════════════════════════════════════════════════════════
//  RENDER DOCUMENTO
// ════════════════════════════════════════════════════════════
function renderDoc(d, elapsedMs){
  const h = d.header, tot = d.totales;

  // ── BANNER RESUMEN — todos los datos clave de un vistazo ──
  const banner = document.getElementById('docBanner');
  if (banner){
    const tiempoTxt = elapsedMs ? `<span style="background:rgba(255,255,255,.15);padding:2px 8px;border-radius:50px;font-size:.66rem;font-weight:700">⚡ ${(elapsedMs/1000).toFixed(1)}s</span>` : '';

    // ★★★ Badge de tipo de operación derivado de SKUs ZZ ★★★
    let tipoOpBadge = '';
    if (h.tipo_operacion) {
      const tc = (h.tipo_codigo || '').toUpperCase();
      let bgColor = '#16a34a', icon = '🚚';
      if (tc === 'ZZRETIRO')        { bgColor = '#dc2626'; icon = '📦'; }
      else if (tc === 'ZZINSTALACION') { bgColor = '#7c3aed'; icon = '🔧'; }
      else if (tc === 'ZZSERVTEC' || tc === 'ZZMANTENCION') { bgColor = '#0891b2'; icon = '🛠️'; }
      else if (tc === 'ZZINGREPUESTO' || tc === 'ZZINGARREQUIP') { bgColor = '#ea580c'; icon = '⚙️'; }
      tipoOpBadge = `<span style="background:${bgColor};color:#fff;padding:5px 12px;border-radius:50px;font-size:.78rem;font-weight:800;letter-spacing:.04em;box-shadow:0 2px 6px rgba(0,0,0,.3)">${icon} ${escHtml(h.tipo_operacion).toUpperCase()}</span>`;
    }

    const zzTxt = d.zzenvio_valor > 0
      ? `<span class="db-pill" style="background:rgba(34,197,94,.2);color:#86efac"><i class="bi bi-truck me-1"></i>ZZ Envío: ${fClp(d.zzenvio_valor)}</span>`
      : '<span class="db-pill" style="background:rgba(156,163,175,.15);color:#9ca3af"><i class="bi bi-truck me-1"></i>Sin ZZ Envío</span>';

    // Observación: SIEMPRE visible (con o sin valor), con fuente si vacía
    // Observaciones: si hay → mostrar bonito; si no → discreto y sin jerga técnica.
    const obsTxt = h.observaciones
      ? `<div class="db-row" style="background:rgba(251,146,60,.15);border-left:3px solid #fb923c">
           <strong style="color:#fed7aa">📝 Observaciones del documento</strong>
           <span style="color:#fff;font-weight:500">${escHtml(h.observaciones)}</span>
         </div>`
      : `<div class="db-row" style="background:rgba(107,114,128,.18);border:1px dashed rgba(156,163,175,.40)">
           <strong style="color:#9ca3af">📝 Observaciones</strong>
           <span style="color:#9ca3af;font-style:italic">Sin observaciones</span>
         </div>`;

    banner.innerHTML = `
      <div style="background:linear-gradient(135deg,#0a0a0a 0%,#1c1c1c 100%);color:#fff;border-radius:10px;padding:16px 20px;margin-bottom:14px;border-left:4px solid #dc2626">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px;flex-wrap:wrap;gap:8px">
          <div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap">
            <div>
              <div style="font-size:.65rem;color:#9ca3af;text-transform:uppercase;letter-spacing:.06em">Documento ERP encontrado</div>
              <div style="font-size:1.25rem;font-weight:900;color:#fff">${h.tido||''} ${h.nudo_display||h.nudo||''}</div>
            </div>
            ${tipoOpBadge}
          </div>
          ${tiempoTxt}
        </div>
        <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(190px,1fr));gap:8px;margin-bottom:8px">
          <div class="db-row"><strong>👤 Cliente</strong><span>${escHtml(h.cliente_nombre||'⚠ Sin nombre')}</span></div>
          <div class="db-row"><strong>🆔 RUT</strong><span class="font-mono">${escHtml(h.cliente_rut||'—')}</span></div>
          <div class="db-row"><strong>📧 Email</strong><span>${escHtml(h.email||'—')}</span></div>
          <div class="db-row"><strong>📞 Teléfono</strong><span>${escHtml(h.telefono||'—')}</span></div>
          <div class="db-row"><strong>📍 Dirección</strong><span>${escHtml(h.direccion||'—')}</span></div>
          <div class="db-row"><strong>🏙 Comuna</strong><span>${escHtml(h.comuna||'⚠ Sin comuna')}</span></div>
          <div class="db-row"><strong>📅 Fecha</strong><span>${escHtml(h.fecha||'—')}</span></div>
          <div class="db-row"><strong>💵 Neto</strong><span class="font-mono">${fClp(h.valor_neto)}</span></div>
        </div>
        <div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:8px">${zzTxt}</div>
        ${obsTxt}
      </div>
    `;
  }

  // ── Datos del documento
  // Nombre del cliente — backend ya normaliza ("Cliente no informado por ERP"
  // cuando no hay dato real, p.ej. boletas a consumidor final). Daniel
  // 2026-07-27: en ese caso el campo debe quedar editable y VACÍO (no el
  // placeholder literal) para que el operador escriba el nombre real a
  // mano — antes era un <div> de solo lectura y no había forma de agregarlo.
  const _nomERP = (h.cliente_nombre || '').trim();
  const _nomEsPlaceholder = !_nomERP || _nomERP.toLowerCase() === 'cliente no informado por erp';
  setInput('cli-nombre', _nomEsPlaceholder ? '' : _nomERP);
  setText('cli-rut',    h.cliente_rut    || '—');
  setText('cli-fecha',  h.fecha          || '—');
  // Valor Neto ahora es editable (Daniel 2026-07-22): igual que ZZ Envío, si
  // el despacho es parcial el total de la factura no corresponde al seguro
  // de ESTE envío. Ver actualizarValorNetoManual.
  const _elNeto = document.getElementById('cli-neto');
  if (_elNeto) _elNeto.value = h.valor_neto > 0 ? Math.round(h.valor_neto) : '';
  // ZZ Envío ahora es editable (Daniel 2026-07-22): input numérico crudo, no
  // texto formateado. Se precarga con el valor del ERP (editable si el
  // despacho es parcial — ver actualizarZzEnvioManual).
  const _elZz = document.getElementById('cli-zzenvio');
  if (_elZz) _elZz.value = d.zzenvio_valor > 0 ? Math.round(d.zzenvio_valor) : '';
  // Referencia para el guardado (Daniel 2026-07-22): lo que se ve AL CARGAR
  // el documento — sea el total crudo del ERP o un saldo ya guardado antes.
  _docData._zzenvioValorAlCargar = d.zzenvio_valor || 0;
  const _zzBadge = document.getElementById('zz-saldo-badge');
  if (_zzBadge) _zzBadge.style.display = d.zzenvio_es_saldo ? '' : 'none';
  setText('cli-seguro', fClp(h.valor_neto * 0.012));

  // Nota histórica (Daniel 2026-05-31, vigente): la vista operativa nunca
  // muestra ruido técnico del ERP. Si faltan datos del cliente, el campo
  // editable queda vacío y el operador completa — sin alerta, sin panel de
  // diagnóstico. (2026-07-25: se quitó también el botón "Diagnosticar ERP" y
  // el panel de campos crudos — Daniel pidió eliminarlos.)

  // ── Campos editables — pre-rellena con datos del ERP (editables por usuario)
  setInput('cli-email', h.email     || '');
  setInput('cli-tel',   h.telefono  || '');
  setInput('cli-dir',   h.direccion || '');
  // Nuevo documento → la dirección del ERP aún NO está georreferenciada
  // (Daniel 2026-07-22: obligatorio validarla con Google antes de asignar a
  // manifiesto). Limpiar cualquier lat/lng que hubiera quedado del documento anterior.
  setInput('cli-dir-lat', '');
  setInput('cli-dir-lng', '');
  setInput('cli-dir-place-id', '');
  // Región: la trae el ERP si existe; si no, queda vacía y se completa sola
  // cuando el operador valide la dirección con Google.
  setInput('cli-region', h.region || '');
  const _dirHint0 = document.getElementById('cli-dir-hint');
  if (_dirHint0) _dirHint0.innerHTML = '<i class="bi bi-info-circle"></i> Escribe y elige una sugerencia para validar la dirección — obligatorio para asignar a manifiesto.';
  if (document.getElementById('cli-dir')) document.getElementById('cli-dir').dataset.validatedValue = '';
  setInput('cli-obs',   h.observaciones || '');
  setInput('cli-notas', '');   // notas de entrega: las escribe el usuario, no vienen del ERP

  // Bultos calculados (editable)
  setInput('cli-bultos', tot.total_bultos || 0);
  sincronizarBultos(tot.total_bultos || 0);

  // Comuna (si el ERP trae una, precargarla)
  const comunaErp = h.comuna || '';
  if(comunaErp){
    const input = document.getElementById('cli-comuna');
    input.value = comunaErp;
    onComunaChange(comunaErp);
  } else {
    // Sin comuna — mostrar aviso
    document.getElementById('avisoComuna').style.display='';
    document.getElementById('courierList').classList.add('courier-locked');
  }

  show('cardDatos');

  // ── Tabla cubicaje (render + totales centralizados en renderCubaje())
  renderCubaje();

  show('cardCubaje');
  show('cardNotas');
  show('totalsBar');
  show('cardFlujo');

  // ── Resumen panel derecho
  setText('res-doc',    `${h.tido} N°${h.nudo_display}`);
  setText('res-fecha',  h.fecha || '—');
  setText('res-skus',   d.zzenvio_valor > 0 ? fClp(d.zzenvio_valor) : '—');
  setText('res-bultos', tot.total_bultos ?? '—');
  setText('res-neto',   fClp(h.valor_neto));
  setText('res-seguro', fClp(h.valor_neto * 0.012));
  hide('emptyResumen');
  show('resumenContent');
  show('badgeCubicado');
  show('cardCouriers');
  show('cardAcciones');
}

// ════════════════════════════════════════════════════════════
//  TABLA CUBICAJE — render + totales (reutilizable tras eliminar línea)
// ════════════════════════════════════════════════════════════
// Render aislado para poder re-pintar la tabla cuando el operador
// elimina un producto de la VISTA (no toca ERP ni BD — READ-ONLY abs.).
// Lee/recalcula desde _docData.lineas y reescribe _docData.totales.

// Badge de saldo por línea. saldo viene del backend (fórmula Random:
// CAPRCO1 - CAPRAD1 - CAPREX1 - CAPRNC1, ya forzado a 0 si ESLIDO cierra).
// Además, si el backend trae l.stock (desglose MAEPR: físico/devengado/
// comprometido/disponible — stock GLOBAL del producto, distinto al saldo
// DEL DOCUMENTO), se agrega un ícono de info con tooltip Bootstrap para no
// llenar la tabla de números (Daniel 2026-07-25).
function _fmtNum(v){
  return (Math.round((Number(v)||0) * 100) / 100).toLocaleString('es-CL', {maximumFractionDigits: 2});
}
function _stockTooltip(st){
  if(!st) return '';
  // Datos 100% numéricos generados acá (nunca texto libre del ERP/usuario) —
  // seguro pasarlos como HTML del tooltip (data-bs-html) para tener saltos
  // de línea legibles.
  const tip = `Stock físico (bodega): <b>${_fmtNum(st.fisico)}</b><br>`
            + `Comprometido (vendido, pend. despacho): <b>${_fmtNum(st.comprometido)}</b><br>`
            + `Devengado (en camino, no ha llegado): <b>${_fmtNum(st.devengado)}</b><br>`
            + `Disponible real: <b>${_fmtNum(st.disponible)}</b>`;
  // tip es 100% generado acá (números + etiquetas fijas, cero texto libre
  // del ERP/usuario) — no se escapa para permitir el <br>, pero SÍ va sin
  // comillas dobles propias (no las contiene).
  return ` <i class="bi bi-info-circle stock-info-ic" data-bs-toggle="tooltip" data-bs-html="true" `
       + `data-bs-placement="top" title="${tip}"></i>`;
}
function _saldoBadge(l){
  const s = l.saldo;
  const stockTip = _stockTooltip(l.stock);
  if(s === undefined || s === null){
    return '<span class="pill-saldo nd"><i class="bi bi-dash-circle"></i>s/d</span>' + stockTip;
  }
  const v = Number(s);
  // Daniel (2026-07-25): quiere ver el NÚMERO real del saldo del ERP, no
  // solo el sí/no. Se muestra formateado (sin decimales si es entero).
  const vTxt = _fmtNum(v);
  if(v > 0){
    return '<span class="pill-saldo si"><i class="bi bi-check-circle-fill"></i>con saldo: ' + vTxt + '</span>' + stockTip;
  }
  return '<span class="pill-saldo no"><i class="bi bi-x-circle-fill"></i>sin saldo</span>' + stockTip;
}

// ════════════════════════════════════════════════════════════
//  ALERTA LÍMITE FÍSICO FEDEX (68kg por bulto) — no bloqueante
// ════════════════════════════════════════════════════════════
// IMPORTANTE sobre el dato disponible: la ficha técnica NO guarda el peso
// de cada bulto individual — guarda, por línea/SKU, el peso real TOTAL de
// UNA unidad del producto (l.peso_kg_u) y en cuántos bultos se reparte esa
// unidad (l.total_bultos). Cuando total_bultos===1 este valor ES el peso
// real exacto de ese bulto. Cuando total_bultos>1 lo que se puede calcular
// es un PROMEDIO (peso_kg_u / total_bultos) — es la mejor aproximación
// disponible con los datos reales que existen hoy (no hay desglose de
// bulto-por-bulto en el modelo), pero no está garantizado que cada bulto de
// esa unidad pese exactamente igual si vienen en cajas de tamaños distintos.
function _pesoPromedioBultoLinea(l){
  if(!l || !l.tiene_bultos) return null;
  const n  = parseInt(l.total_bultos) || 0;
  const kg = parseFloat(l.peso_kg_u) || 0;
  if(n <= 0) return null;
  return kg / n;
}

// Líneas del documento cuyo peso real (por bulto) supera el límite físico
// de FedEx. Devuelve [] si no hay datos de cubicaje o ninguna excede.
function _lineasExcedenFedex(){
  if(!_docData || !Array.isArray(_docData.lineas)) return [];
  const out = [];
  _docData.lineas.forEach(function(l){
    const pesoBulto = _pesoPromedioBultoLinea(l);
    if(pesoBulto != null && pesoBulto > FEDEX_MAX_KG_POR_BULTO){
      out.push({
        sku:         l.sku,
        nombre:      l.descripcion_erp || l.nombre_app || l.sku || '—',
        pesoBulto:   pesoBulto,
        totalBultos: parseInt(l.total_bultos) || 0,
      });
    }
  });
  return out;
}

// Pinta (o esconde) el banner de alerta sobre la tabla de cubicaje.
function _renderFedexAlertBanner(overweightLines){
  const el = document.getElementById('fedexAlertBanner');
  if(!el) return;
  if(!overweightLines || !overweightLines.length){
    el.style.display = 'none';
    el.innerHTML = '';
    return;
  }
  const detalle = overweightLines.map(x => `<li><b>${escHtml(x.sku||'')}</b> ${escHtml(x.nombre||'')}`
    + ` — ≈${fCl(x.pesoBulto)} kg/bulto (${x.totalBultos} bulto${x.totalBultos===1?'':'s'})</li>`).join('');
  el.style.display = '';
  el.innerHTML = `
    <div class="fedex-limit-banner">
      <div class="fedex-limit-banner-hdr">
        <i class="bi bi-exclamation-triangle-fill"></i>
        <span>${overweightLines.length} producto${overweightLines.length===1?'':'s'} supera${overweightLines.length===1?'':'n'}
          el límite de FedEx (${FEDEX_MAX_KG_POR_BULTO} kg por bulto) — asignar manualmente con otro courier.</span>
      </div>
      <ul>${detalle}</ul>
    </div>`;
}

function renderCubaje(){
  if(!_docData || !Array.isArray(_docData.lineas)) return;
  const lineas = _docData.lineas;
  const tbody = document.getElementById('tbodyCubaje');
  if(!tbody) return;

  // ── Recalcular totales desde las líneas actuales (en memoria)
  let tQty=0, tKg=0, tPv=0, tVol=0, tPred=0, tBult=0;
  const overweightFedex = [];   // líneas que superan FEDEX_MAX_KG_POR_BULTO

  let rows = '';
  lineas.forEach((l, idx) => {
    const qty = parseFloat(l.cantidad) || 0;
    const sf  = !l.tiene_bultos;
    const tipo = sf
      ? '<span class="pill-sf">s/f</span>'
      : l.peso_kg_u >= l.peso_vol_u
        ? '<span class="pill-kg">REAL</span>'
        : '<span class="pill-pv">VOL</span>';
    const pesoBultoLinea = _pesoPromedioBultoLinea(l);
    const excedeFedex    = pesoBultoLinea != null && pesoBultoLinea > FEDEX_MAX_KG_POR_BULTO;
    if(excedeFedex){
      overweightFedex.push({
        sku: l.sku, nombre: l.descripcion_erp || l.nombre_app || l.sku || '—',
        pesoBulto: pesoBultoLinea, totalBultos: parseInt(l.total_bultos) || 0,
      });
    }
    const bultosCell = sf
      ? '<span style="color:#2a2a2a">—</span>'
      : `<span class="bultos-badge">${l.total_bultos}</span>`
        + (excedeFedex
            ? ` <span class="fedex-limit-ico" title="Peso real ≈${fCl(pesoBultoLinea)} kg por bulto — supera el límite de FedEx (${FEDEX_MAX_KG_POR_BULTO}kg). No apto para despacho automático por FedEx, asignar manual con otro courier."><i class="bi bi-exclamation-triangle-fill"></i></span>`
            : '');

    // Acumular totales (líneas s/f aportan 0 peso/vol)
    tQty  += qty;
    tKg   += (parseFloat(l.peso_kg_u)  || 0) * qty;
    tPv   += (parseFloat(l.peso_vol_u) || 0) * qty;
    tVol  += (parseFloat(l.vol_u)      || 0) * qty;
    tPred += (parseFloat(l.pred_tot)   || 0);
    tBult += (parseInt(l.bultos_tot)   || 0);

    // Datos para los botones de medidas/etiquetas (reusa el mismo mecanismo
    // global de static/cubicador_tabs.js que ya usan index.html/ficha.html —
    // el click-delegation ya está enganchado a nivel documento, acá solo hay
    // que pintar el botón con los data-* correctos).
    const _skuAttr    = escHtml(l.sku || '');
    const _nombreAttr = escHtml(l.descripcion_erp || l.nombre_app || '');
    const _appIdAttr  = l.app_id != null ? l.app_id : '';
    const _bultosAttr = sf ? 0 : (parseInt(l.total_bultos) || 0);

    rows += `<tr class="cub-row" data-sku="${_skuAttr}" data-app-id="${_appIdAttr}" data-qty="${parseInt(qty)}">
      <td class="tc" data-label=""><input type="checkbox" class="cube-chk" data-idx="${idx}" onchange="actualizarBotonEliminarSeleccionados()"></td>
      <td class="mono" data-label="SKU" data-cell="sku">${l.sku}</td>
      <td class="cube-desc" data-label="Descripción" style="font-size:.82rem;max-width:200px;line-height:1.3">${l.descripcion_erp||'—'}</td>
      <td class="tc" data-label="Cant.">${parseInt(qty)}</td>
      <td class="tc" data-label="Bultos/u" data-cell="bultos">${bultosCell}</td>
      <td class="tr" data-label="KG Real/u" data-cell="kg-u">${sf?'<span class="sf">—</span>':fCl(l.peso_kg_u)}</td>
      <td class="tr" data-label="KG Vol/u" data-cell="pv-u">${sf?'<span class="sf">—</span>':fCl(l.peso_vol_u)}</td>
      <td class="tr" data-label="Vol. m³/u" data-cell="vol-u" style="font-size:.78rem;color:#555">${sf?'<span class="sf">—</span>':fVol(l.vol_u)}</td>
      <td class="tr pred-u" data-label="Predom./u" data-cell="pred-u">${sf?'<span class="sf">—</span>':fCl(l.pred_u)}</td>
      <td class="tr pred-tot" data-label="Total Predom." data-cell="pred-tot">${sf?'<span class="sf">—</span>':fCl(l.pred_tot)}</td>
      <td class="tc" data-label="Tipo">${tipo}</td>
      <td class="tc" data-label="Saldo">${_saldoBadge(l)}</td>
      <td class="tc cube-act" data-label="Acción">
        <div class="d-inline-flex gap-1">
          <button type="button" class="btn-cube-med" title="${sf ? 'Cargar medidas (sin ficha todavía)' : 'Editar medidas'}"
                  data-action="cub-medidas" data-sku="${_skuAttr}" data-app-id="${_appIdAttr}"
                  data-nombre="${_nombreAttr}" data-bultos="${_bultosAttr}">
            <i class="bi bi-rulers"></i>
          </button>
          <button type="button" class="btn-cube-del" title="Quitar este producto de la cotización"
                  onclick="eliminarLineaCubaje(${idx})">
            <i class="bi bi-trash"></i>
          </button>
        </div>
      </td>
    </tr>`;
  });

  if(!rows){
    rows = `<tr class="cube-empty-row"><td colspan="13" class="cube-empty-cell" style="text-align:center;padding:26px 12px;color:#9ca3af">
      <div style="font-size:1.4rem">📦</div>
      <div style="font-weight:700;margin-top:4px">Sin productos en la cotización</div>
      <div style="font-size:.78rem;margin-top:2px">Busca un documento para cargar su detalle.</div>
    </td></tr>`;
  }
  tbody.innerHTML = rows;
  // Cada re-render recrea los checkboxes (todos desmarcados) — resetear el
  // "seleccionar todos" y ocultar el botón de eliminación masiva.
  const _chkTodos = document.getElementById('chkTodosCubaje');
  if(_chkTodos) _chkTodos.checked = false;
  actualizarBotonEliminarSeleccionados();

  // Tooltips del desglose de stock (bootstrap.Tooltip) — se recrean en cada
  // render porque tbody.innerHTML reemplaza los nodos anteriores.
  if(window.bootstrap && window.bootstrap.Tooltip){
    tbody.querySelectorAll('[data-bs-toggle="tooltip"]').forEach(el => {
      new bootstrap.Tooltip(el);
    });
  }

  // ── Persistir totales recalculados (los lee actualizarTarifas / resumen)
  // Si el operador ya cargó medidas MANUALES (documento sin ficha técnica,
  // ver cotizarConMedidasManuales), NO las pisamos recalculando desde líneas
  // vacías/sin ficha — eso volvería a dejar los totales en 0.
  _docData.totales = _docData.totales || {};
  if(!_docData._medidasManuales){
    _docData.totales.total_qty    = Math.round(tQty);
    _docData.totales.total_bultos = Math.round(tBult);
    _docData.totales.peso_kg      = Math.round(tKg   * 1000) / 1000;
    _docData.totales.peso_pv      = Math.round(tPv   * 1000) / 1000;
    _docData.totales.vol_cm3      = Math.round(tVol  * 10)   / 10;
    _docData.totales.peso_pred    = Math.round(tPred * 1000) / 1000;
  }

  const tot = _docData.totales;
  setText('tot-qty',  tot.total_qty  ?? '—');
  setText('tot-bult', tot.total_bultos ?? '—');
  setText('tot-kg',   fCl(tot.peso_kg));
  setText('tot-pv',   fCl(tot.peso_pv));
  setText('tot-vol',  fVol(tot.vol_cm3));
  setText('tot-pred', fCl(tot.peso_pred));

  // Mantener sincronizados los campos del resumen / bultos editable
  setText('res-bultos', tot.total_bultos ?? '—');

  // Alerta límite físico FedEx (68kg/bulto) — no bloqueante, solo aviso.
  _renderFedexAlertBanner(overweightFedex);
}

// Daniel (2026-07-25): "necesito poder acceder a las etiquetas y rellenarlo
// [medidas] y que se actualice, y que se recalcule. Todo tiene que ser
// automático." — al guardar medidas desde el botón de esta misma tabla
// (data-action="cub-medidas"), static/cubicador_tabs.js dispara este evento
// genérico. Actualizamos la línea correspondiente en _docData y volvemos a
// pintar la tabla completa (recalcula totales solo).
document.addEventListener('cubicador:medidas-guardadas', function(ev){
  if(!_docData || !Array.isArray(_docData.lineas)) return;
  const { sku, pid, t } = ev.detail || {};
  if(!sku || !t) return;
  let huboCambio = false;
  _docData.lineas.forEach(function(l){
    if(l.sku !== sku) return;
    const qty = parseFloat(l.cantidad) || 0;
    l.tiene_bultos = t.n > 0;
    l.tiene_ficha  = true;
    l.total_bultos = t.n;
    l.bultos_tot   = t.n * qty;
    l.peso_kg_u    = t.kg;
    l.peso_vol_u   = t.pv;
    l.vol_u        = t.vol;
    l.pred_u       = t.pred;
    l.pred_tot     = Math.round(t.pred * qty * 1000) / 1000;
    if(pid && !l.app_id) l.app_id = pid;
    huboCambio = true;
  });
  if(huboCambio){
    renderCubaje();
    // El precio de los couriers depende del cubicaje: si la tarjeta ya está
    // visible (es decir, ya se había cotizado antes), re-cotiza sola para no
    // dejar precios calculados con datos viejos.
    const _cardCouriers = document.getElementById('cardCouriers');
    if (_cardCouriers && _cardCouriers.style.display !== 'none'
        && typeof actualizarTarifas === 'function') {
      actualizarTarifas();
    }
  }
});

// Elimina un producto SOLO de la vista/cotización (no toca ERP ni BD).
// Recalcula totales (incl. peso predominante) y re-cotiza couriers.
async function eliminarLineaCubaje(idx){
  if(!_docData || !Array.isArray(_docData.lineas)) return;
  const l = _docData.lineas[idx];
  if(!l) return;

  const ok = await ilusConfirm({
    title: 'Quitar producto',
    message: `¿Quitar este producto de la cotización?`,
    sub: `<strong style="color:#dc2626">${escHtml(l.sku)}</strong> · ${escHtml(l.descripcion_erp || '')}`,
    subHtml: true,
    okLabel: 'Quitar', cancelLabel: 'Cancelar',
    danger: true,
    type: 'danger',
  });
  if(!ok) return;

  // Quitar de los datos en memoria
  _docData.lineas.splice(idx, 1);

  // Re-render (recalcula totales incl. predominante) + re-cotizar
  renderCubaje();
  // El peso predominante cambió → las tarifas dependen de él
  actualizarTarifas();

  ilusToast(`✓ Producto ${l.sku} quitado de la cotización`, { type: 'success' });
}

// Marca/desmarca TODOS los checkboxes de la tabla de cubicaje de una vez
// (Daniel 2026-07-22: con 20 productos y solo 1 por enviar, es más rápido
// "seleccionar todos" y luego destildar el que sí tiene saldo).
function toggleSeleccionarTodoCubaje(marcar){
  document.querySelectorAll('#tbodyCubaje .cube-chk').forEach(chk => { chk.checked = marcar; });
  actualizarBotonEliminarSeleccionados();
}

// Muestra/oculta el botón "Eliminar seleccionados" y actualiza el contador.
function actualizarBotonEliminarSeleccionados(){
  const n = document.querySelectorAll('#tbodyCubaje .cube-chk:checked').length;
  const btn = document.getElementById('btnEliminarSeleccionados');
  const cnt = document.getElementById('cntSeleccionados');
  if(cnt) cnt.textContent = n;
  if(btn) btn.style.display = n > 0 ? '' : 'none';
}

// Elimina TODOS los productos marcados de una sola vez (Daniel 2026-07-22):
// evita repetir "eliminar" producto por producto cuando hay muchos que quitar.
// Misma lógica que eliminarLineaCubaje pero en lote, con 1 sola confirmación.
async function eliminarLineasSeleccionadas(){
  if(!_docData || !Array.isArray(_docData.lineas)) return;
  const checks = Array.from(document.querySelectorAll('#tbodyCubaje .cube-chk:checked'));
  if(!checks.length) return;
  const idxs = checks.map(c => parseInt(c.dataset.idx, 10)).filter(n => !isNaN(n));
  if(!idxs.length) return;

  const skus = idxs.map(i => _docData.lineas[i]).filter(Boolean).map(l => l.sku);
  const ok = await ilusConfirm({
    title: 'Quitar productos seleccionados',
    message: `¿Quitar ${idxs.length} producto(s) de la cotización?`,
    sub: skus.slice(0, 6).map(s => escHtml(s)).join(', ') + (skus.length > 6 ? ` +${skus.length - 6} más` : ''),
    okLabel: 'Quitar todos', cancelLabel: 'Cancelar',
    danger: true,
    type: 'danger',
  });
  if(!ok) return;

  // Borrar de mayor a menor índice para no desfasar los índices restantes.
  idxs.sort((a, b) => b - a).forEach(i => _docData.lineas.splice(i, 1));

  renderCubaje();
  actualizarTarifas();

  ilusToast(`✓ ${idxs.length} producto(s) quitado(s) de la cotización`, { type: 'success' });
}

// ════════════════════════════════════════════════════════════
//  COURIERS — Cotización integral (todos en paralelo)
// ════════════════════════════════════════════════════════════
// Estado: _cotizaciones se llena desde /api/asignar/cotizar-couriers
// _courierSel ahora guarda { id, nombre, precio, ... } del seleccionado.
// _recomendadoId: id del courier con menor precio (cobertura sí).

let _cotizaciones    = [];      // Array de {courier_id, courier_nombre, precio, ...}
let _recomendadoId   = null;
const _cotCache      = new Map();
const _cotCacheTTL   = 90_000;
let _cotDebounceTimer = null;

// Logos hardcodeados por nombre — se usa si el courier en BD no tiene logo_url
const _COURIER_LOGOS = {
  'fedex':                '<b style="color:#4D148C;font-size:.92rem">Fed<span style="color:#FF6200">Ex</span></b>',
  'starken':              '<b style="color:#e53e3e;font-size:.86rem">★ Starken</b>',
  'blue express':         '<b style="color:#2563eb;font-size:.86rem">blue</b>',
  'chilexpress':          '<b style="color:#e5a900;font-size:.78rem">//chilexpress</b>',
  'clickex':              '<b style="color:#0ea5e9;font-size:.86rem">⚡ Clickex</b>',
  'transporte felca':     '<b style="color:#6b7280;font-size:.78rem">Trans. Felca</b>',
  'transportes milling':  '<b style="color:#6b7280;font-size:.78rem">Trans. Milling</b>',
  'transportes melling':  '<b style="color:#6b7280;font-size:.78rem">Trans. Milling</b>',
  'envíame':              '<b style="color:#16a34a;font-size:.86rem">Envíame</b>',
  'enviame':              '<b style="color:#16a34a;font-size:.86rem">Envíame</b>',
};

// FIX 2026-07-27 (Daniel, captura real): el nombre grande de la tarjeta
// mostraba "Transportes Melling" (alias legacy mal escrito de la BD) mientras
// el logo de al lado ya corregía a "Trans. Milling" -- quedaba inconsistente
// y además el nombre largo rompía el layout en móvil. Normaliza el nombre
// visible en TODOS los lugares que lo muestran, sin tocar el dato real de
// la BD (que sigue igual para couriers/matching).
function _courierDisplay(nombre){
  const n = (nombre||'').trim();
  return /^transportes?\s+melling$/i.test(n) ? 'Transportes Milling' : n;
}
function _logoFor(c){
  if(c.logo_url) return `<img src="${c.logo_url}" alt="" style="max-height:22px;max-width:90px;object-fit:contain">`;
  const key = (c.courier_nombre||'').trim().toLowerCase();
  if(_COURIER_LOGOS[key]) return _COURIER_LOGOS[key];
  return `<b style="color:#374151;font-size:.82rem">${escHtml(_courierDisplay(c.courier_nombre)||'—')}</b>`;
}

function actualizarTarifas(){
  // Debounce 200ms para evitar N llamadas en clicks rápidos
  clearTimeout(_cotDebounceTimer);
  _cotDebounceTimer = setTimeout(_actualizarCotizacionesReal, 200);
}

async function _actualizarCotizacionesReal(){
  if(!_docData) return;
  // Prefiere la comuna VIVA del campo (editable a mano) → al dar "Actualizar"
  // recalcula con la comuna que el operador escribió, no la (vacía) del ERP.
  const _comInput = (document.getElementById('cli-comuna')?.value || '').trim();
  const comuna = (_comInput || _docData.header.comuna || '').trim();
  if(_docData.header) _docData.header.comuna = comuna;  // sincroniza para el resto del flujo
  if(!comuna){
    _cotizaciones = [];
    renderCouriers();
    return;
  }

  // Guard (Daniel 2026-06-16): sin datos REALES de cubicaje (documento sin
  // líneas, o con líneas pero sin ficha técnica → peso/bultos en 0) → NO
  // cotizar. El backend tiene un piso de 0.5kg que, si llamamos igual,
  // fabricaría precios completos y creíbles de couriers para un envío que
  // no existe (ej. VD 10131: "Sin productos en la cotización" pero mostraba
  // cotizaciones reales). Mejor decir explícitamente que no hay datos.
  const _tot = _docData.totales || {};
  const sinCubicaje = !((_tot.total_bultos > 0) || (_tot.peso_kg > 0) || (_tot.peso_pred > 0));
  if(sinCubicaje){
    _cotizaciones  = [];
    _recomendadoId = null;
    renderCouriers({sinDatos:true});
    return;
  }

  const resid = document.getElementById('chkResid')?.checked || false;

  const cacheKey = JSON.stringify({
    peso:   _docData.totales.peso_pred,
    pesokg: _docData.totales.peso_kg,
    comuna, resid,
    valor:  _docData.header.valor_neto,
  });
  const cached = _cotCache.get(cacheKey);
  if(cached && (Date.now() - cached.ts) < _cotCacheTTL){
    _cotizaciones  = cached.cot;
    _recomendadoId = cached.rec;
    renderCouriers();
    return;
  }

  renderCouriers({loading:true});
  try {
    const r = await fetch('/api/asignar/cotizar-couriers', {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify({
        peso_kg:        _docData.totales.peso_kg,
        peso_pred_kg:   _docData.totales.peso_pred,
        comuna:         comuna,
        es_residencial: resid,
        valor_neto:     _docData.header.valor_neto || 0,
        // Contexto del documento para audit trail
        tido:           _docData.header.tido || '',
        nudo:           _docData.header.nudo || '',
      })
    });
    const d = await r.json();
    if(!r.ok || d.error) throw new Error(d.error || ('HTTP '+r.status));
    _cotizaciones  = d.cotizaciones || [];
    _recomendadoId = d.recomendado_id || null;
    _cotCache.set(cacheKey, { cot: _cotizaciones, rec: _recomendadoId, ts: Date.now() });
    if(_cotCache.size > 30){
      _cotCache.delete(_cotCache.keys().next().value);
    }
  } catch(e){
    console.warn('[cotizar-couriers] error:', e);
    _cotizaciones  = [];
    _recomendadoId = null;
    if(window.ilusToast) ilusToast('Error al cotizar: '+e.message, {type:'error'});
  }
  renderCouriers();
}

// Cotiza con medidas ingresadas MANUALMENTE por el operador (Daniel
// 2026-06-16): cuando el documento no trae ficha técnica (0 productos en
// cubicaje), en vez de bloquear la cotización se puede escribir el peso y
// bultos reales a mano y cotizar igual. Queda marcado con _medidasManuales
// para trazabilidad (se muestra un aviso junto a las cotizaciones) — NO se
// guarda en la ficha técnica del producto ni se envía nada al ERP.
async function cotizarConMedidasManuales(){
  const pesoInput   = document.getElementById('manualPesoKg');
  const bultosInput = document.getElementById('manualBultos');
  const peso   = parseFloat(pesoInput  ? pesoInput.value   : '') || 0;
  const bultos = parseInt(bultosInput  ? bultosInput.value : '') || 0;
  if(peso <= 0){
    if(window.ilusToast) ilusToast('Ingresa un peso mayor a 0 para cotizar', {type:'warning'});
    if(pesoInput) pesoInput.focus();
    return;
  }
  if(!_docData) return;

  _docData.totales = _docData.totales || {};
  _docData.totales.peso_kg      = peso;
  _docData.totales.peso_pred    = peso;
  _docData.totales.total_bultos = bultos > 0 ? bultos : 1;
  _docData._medidasManuales = true;

  // Refleja las medidas manuales en el resumen visible (mismos ids que usa
  // renderCubaje/actualizarTarifas, para que quede coherente en toda la vista).
  setText('tot-kg',     fCl(peso));
  setText('tot-pred',   fCl(peso));
  setText('tot-bult',   _docData.totales.total_bultos);
  setText('res-bultos', _docData.totales.total_bultos);

  if(window.ilusToast) ilusToast('Cotizando con medidas ingresadas manualmente', {type:'info'});
  await _actualizarCotizacionesReal();
}

function renderCouriers(opts){
  const list = document.getElementById('courierList');
  if(!list) return;

  // Mostrar/ocultar toggle costo/venta + comparador top según haya cotizaciones
  const toggleWrap = document.getElementById('toggleCostoVentaWrap');
  const compTop    = document.getElementById('comparadorTop');

  // Loading state
  if(opts && opts.loading){
    list.innerHTML = `
      <div style="text-align:center;padding:24px 12px;color:#9ca3af">
        <i class="bi bi-arrow-clockwise" style="font-size:1.6rem;animation:spin 1s linear infinite;display:inline-block"></i>
        <div style="margin-top:8px;font-size:.82rem">Cotizando con todos los couriers…</div>
      </div>`;
    if(toggleWrap) toggleWrap.style.display = 'none';
    if(compTop)    compTop.style.display    = 'none';
    return;
  }

  // Sin datos reales de cubicaje (Daniel 2026-06-16): distinto de "falta
  // comuna" — aquí el documento no tiene productos con peso/bultos, así que
  // NO se cotizó (no se inventan precios de couriers para un envío vacío).
  if(opts && opts.sinDatos){
    list.innerHTML = `
      <div class="empty-state" style="padding-bottom:24px">
        <i class="bi bi-box-seam"></i>
        <p>Este documento no tiene productos con datos de cubicaje (peso/bultos).
           No se puede cotizar automáticamente.</p>
        <p style="margin-top:-4px">Si conoces el peso y bultos reales, puedes
           ingresarlos manualmente para cotizar igual:</p>
        <div style="display:flex;gap:10px;justify-content:center;align-items:flex-end;
                    flex-wrap:wrap;margin-top:8px;text-align:left">
          <div>
            <label style="font-size:.7rem;color:#6b7280;display:block;margin-bottom:2px">Peso real (kg)</label>
            <input type="number" id="manualPesoKg" min="0" step="0.1" placeholder="0.0"
                   style="width:100px;border:1.5px solid #d1d5db;border-radius:6px;
                          padding:6px 8px;font-size:.85rem;font-weight:600;color:#111827"
                   onkeydown="if(event.key==='Enter') cotizarConMedidasManuales()">
          </div>
          <div>
            <label style="font-size:.7rem;color:#6b7280;display:block;margin-bottom:2px">Bultos</label>
            <input type="number" id="manualBultos" min="1" step="1" placeholder="1"
                   style="width:70px;border:1.5px solid #d1d5db;border-radius:6px;
                          padding:6px 8px;font-size:.85rem;font-weight:600;color:#111827"
                   onkeydown="if(event.key==='Enter') cotizarConMedidasManuales()">
          </div>
          <button type="button" onclick="cotizarConMedidasManuales()"
                  style="background:#dc2626;border:none;color:#fff;font-weight:700;
                         font-size:.8rem;padding:7px 16px;border-radius:6px;cursor:pointer;
                         white-space:nowrap">
            <i class="bi bi-calculator me-1"></i>Cotizar con estas medidas
          </button>
        </div>
        <p style="font-size:.7rem;color:#b45309;margin-top:10px">
          <i class="bi bi-info-circle me-1"></i>Esto NO reemplaza la ficha técnica
          del producto — es solo para esta cotización puntual.
        </p>
      </div>`;
    if(toggleWrap) toggleWrap.style.display = 'none';
    if(compTop)    compTop.style.display    = 'none';
    return;
  }

  if(!_cotizaciones || !_cotizaciones.length){
    list.innerHTML = `
      <div class="empty-state">
        <i class="bi bi-truck"></i>
        <p>Define la comuna destino para cotizar con los couriers disponibles.</p>
      </div>`;
    if(toggleWrap) toggleWrap.style.display = 'none';
    if(compTop)    compTop.style.display    = 'none';
    return;
  }

  const conCob = _cotizaciones.filter(c => c.tiene_cobertura);
  const sinCob = _cotizaciones.filter(c => !c.tiene_cobertura);
  const zzVal  = (_docData && _docData.zzenvio_valor) ? _docData.zzenvio_valor : 0;

  // Si hay al menos una cotización con desglose costo/venta → mostrar toggle
  const hayDesglose = conCob.some(c => c.desglose && c.desglose.precio_costo != null);
  if(toggleWrap){
    toggleWrap.style.display = hayDesglose ? '' : 'none';
  }
  // Comparador top 3 (más barato + validado)
  renderComparadorTop(conCob);

  // Header: recomendado + ZZ envío comparativa
  let head = '';
  // Alerta límite físico FedEx (68kg/bulto) — no bloqueante. Se muestra acá
  // (además del banner sobre la tabla de cubicaje) porque es justo en este
  // panel donde Alison elige el courier — pedido Daniel 2026-07-29.
  const _fedexOverweight = _lineasExcedenFedex();
  if(_fedexOverweight.length){
    const detalleFedex = _fedexOverweight
      .map(x => `${escHtml(x.sku||'')} (≈${fCl(x.pesoBulto)} kg/bulto)`)
      .join(', ');
    head += `
      <div style="background:#fee2e2;border:1.5px solid #dc2626;border-radius:9px;
                  padding:8px 12px;margin-bottom:10px;font-size:.78rem;color:#7f1d1d;
                  display:flex;align-items:flex-start;gap:8px">
        <span style="font-size:1.1rem;line-height:1">⚠️</span>
        <div style="flex:1;min-width:0">
          <div style="font-weight:800">Supera límite de FedEx (${FEDEX_MAX_KG_POR_BULTO} kg por bulto)</div>
          <div style="font-size:.72rem;margin-top:2px">${detalleFedex} — asignar manualmente con otro courier, FedEx automático no acepta este bulto.</div>
        </div>
      </div>`;
  }
  // Aviso persistente (Daniel 2026-06-16): estas cotizaciones se calcularon
  // con peso/bultos ingresados A MANO (el documento no traía ficha técnica),
  // no con datos verificados del ERP/ficha del producto. Transparencia para
  // quien revise el precio después.
  if(_docData && _docData._medidasManuales){
    head += `
      <div style="background:#eff6ff;border:1px solid #93c5fd;border-radius:8px;
                  padding:7px 10px;margin-bottom:10px;font-size:.74rem;color:#1e40af;
                  display:flex;align-items:center;gap:6px">
        <i class="bi bi-pencil-square"></i>
        <span>Cotizado con medidas ingresadas manualmente — no vienen de la ficha técnica del producto.</span>
      </div>`;
  }
  if(_recomendadoId){
    const rec = conCob.find(c => c.courier_id === _recomendadoId);
    if(rec){
      head += `
        <div style="background:linear-gradient(90deg,#fffbeb 0%,#fff 100%);border:1.5px solid #f59e0b;
                    border-radius:9px;padding:8px 12px;margin-bottom:10px;font-size:.82rem;
                    display:flex;align-items:center;gap:8px">
          <span style="font-size:1.1rem">🏆</span>
          <div style="flex:1;min-width:0">
            <div style="font-weight:700;color:#92400e">Recomendado: ${escHtml(_courierDisplay(rec.courier_nombre))}</div>
            <div style="font-size:.7rem;color:#9a6411">${fClp(rec.precio)} · ${escHtml(rec.tiempo_transito||'—')}</div>
          </div>
        </div>`;
    }
  }

  // Render con cobertura
  let html = head;
  for(const c of conCob){
    const sel = (_courierSel && _courierSel.id === c.courier_id);
    const isRec = (c.courier_id === _recomendadoId);

    let badgeProfit = '';
    if(zzVal > 0 && c.precio){
      const diff = zzVal - c.precio;
      const color = diff >= 0 ? '#16a34a' : '#dc2626';
      const icon  = diff >= 0 ? '▲' : '▼';
      const txt   = diff >= 0 ? `+${fClp(diff)}` : `${fClp(diff)}`;
      const margenLabel = diff >= 0 ? 'margen' : 'pérdida';
      badgeProfit = `<div style="font-size:.65rem;color:${color};font-weight:700;margin-top:3px;letter-spacing:.3px">
        ${icon} ${txt} ${margenLabel}
      </div>`;
    }

    const recIcon  = isRec ? ' <span title="Más económico" style="font-size:.75rem">🏆</span>' : '';
    const profitIcon = (zzVal>0 && c.precio && (zzVal - c.precio) > 0) ? ' <span title="Margen positivo">💰</span>' : '';
    // 2026-07-25 (Daniel — confidencial, no revertir sin indicación suya):
    // Felca y Milling tienen una política comercial propia. El backend
    // (app.py) la reporta con fuente='tabla', IGUAL que una tarifa negociada
    // real — a propósito, para que en este panel (incluido "Auditar") no
    // haya ninguna seña que la distinga. No reintroducir un chip/campo
    // 'regla' ni leer c.es_regla_comercial/c.regla_pct: ninguno de los dos
    // llega en el payload desde 2026-07-25.
    const fuente = c.fuente === 'api' ? '<span style="background:#dbeafe;color:#1e40af;padding:1px 6px;border-radius:50px;font-size:.6rem;font-weight:700">API</span>' :
                   c.fuente === 'tabla_fallback' ? '<span style="background:#fff8e1;color:#92400e;padding:1px 6px;border-radius:50px;font-size:.6rem;font-weight:700">Tabla (API caída)</span>' :
                   c.fuente === 'estimado' ? '<span title="Precio ESTIMADO: no hay tarifa cargada para esta comuna/tramo y el valor es una proyección. Confírmalo con el courier antes de despachar." style="background:#fee2e2;color:#991b1b;padding:1px 6px;border-radius:50px;font-size:.6rem;font-weight:800;letter-spacing:.3px">ESTIMADO</span>' :
                   '<span style="background:#f3f4f6;color:#6b7280;padding:1px 6px;border-radius:50px;font-size:.6rem;font-weight:700">Tabla</span>';

    // ── AUDIT/TRACE indicators ─────────────────────────────────
    const trace = c.trace || {};
    const advs  = Array.isArray(trace.advertencias) ? trace.advertencias : [];
    const hasWarn = advs.length > 0;
    const isValidado = !!trace.validado;
    // Tooltip resumen (atributo title del span info)
    const tipBracket = trace.bracket ? `Bracket: ${trace.bracket}` : '';
    const tipFormula = trace.formula ? trace.formula : '';
    const tipTooltip = [tipBracket, tipFormula].filter(Boolean).join(' · ');
    // Chip de validación
    const chipValid = isValidado
      ? `<span title="Tarifa validada manualmente contra Excel maestro"
              style="background:#dcfce7;color:#15803d;padding:1px 6px;border-radius:50px;
                     font-size:.6rem;font-weight:700;margin-left:4px">✓ VALIDADA</span>`
      : '';
    // Chip de advertencia
    const chipWarn = hasWarn
      ? `<span title="${escHtml(advs.join(' | '))}"
              style="background:#fff8e1;color:#92400e;padding:1px 6px;border-radius:50px;
                     font-size:.6rem;font-weight:700;margin-left:4px;cursor:help">⚠ Revisar</span>`
      : '';
    // Botón "i" → modal auditoría
    const auditId = c.audit_id || '';
    const btnAudit = `<button type="button"
        onclick="event.stopPropagation();abrirAuditoriaCourier(${auditId||'null'},${JSON.stringify(c).replace(/"/g,'&quot;').replace(/'/g,'&#39;')})"
        title="Ver detalle de auditoría del cálculo"
        style="background:#f3f4f6;border:1px solid #e5e7eb;border-radius:6px;padding:2px 7px;
               font-size:.66rem;color:#374151;cursor:pointer;line-height:1;margin-left:4px;
               font-weight:600">
        <i class="bi bi-info-circle"></i> Auditar
      </button>`;

    // ── Precio según vista activa (costo / venta) ────────────────
    const precioVisible = _precioMostrar(c);
    const vistaEsCosto  = (_vistaPrecio === 'costo');
    const subLineaPrecio = (c.desglose && c.desglose.precio_costo != null)
      ? (vistaEsCosto
          ? `<div style="font-size:.62rem;color:#dc2626;margin-top:1px;font-weight:600" title="Precio venta al cliente">venta ${fClp(c.precio)}</div>`
          : `<div style="font-size:.62rem;color:#9ca3af;margin-top:1px;font-weight:500" title="Costo ILUS (Excel)">costo ${fClp(c.desglose.precio_costo)}</div>`)
      : '';

    // ── Chip límite físico FedEx (68kg/bulto) — solo en la fila de FedEx.
    const esFedex = (c.courier_nombre||'').toLowerCase().includes('fedex');
    const chipFedexLimit = (esFedex && _fedexOverweight.length)
      ? `<span title="${escHtml('Bulto(s) sobre 68kg: ' + _fedexOverweight.map(x => (x.sku||'')+' ≈'+x.pesoBulto.toFixed(1)+'kg').join(', '))}"
              style="background:#fee2e2;color:#7f1d1d;padding:1px 6px;border-radius:50px;
                     font-size:.6rem;font-weight:800;margin-left:4px;cursor:help">⚠ &gt;68kg: manual</span>`
      : '';
    const chipFedexOk = (esFedex && !_fedexOverweight.length && _docData && Array.isArray(_docData.lineas) && _docData.lineas.length)
      ? `<span title="Ningún bulto de este pedido supera 68kg"
              style="color:#16a34a;font-size:.62rem;font-weight:700;margin-left:4px">
              <i class="bi bi-check-circle-fill"></i> Apto FedEx auto</span>`
      : '';

    html += `
    <div class="courier-item${sel?' selected':''}" onclick='setCourier(${JSON.stringify(c).replace(/'/g, "&#39;")})'>
      <div class="courier-radio"></div>
      <div class="courier-logo">${_logoFor(c)}</div>
      <div class="courier-info">
        <div class="courier-name">${escHtml(_courierDisplay(c.courier_nombre))}${recIcon}${profitIcon}${chipValid}${chipWarn}${chipFedexLimit}${chipFedexOk}</div>
        <div class="courier-svc">${escHtml(c.servicio||'Standard')}</div>
        <div class="courier-etd" title="${escHtml(tipTooltip)}">${escHtml(c.tiempo_transito||'—')} ${fuente} ${btnAudit}</div>
      </div>
      <div class="courier-pricebox">
        <div class="courier-price" style="${vistaEsCosto?'color:#475569':''}" title="${c.desglose ? 'Costo $'+Math.round(c.desglose.precio_costo).toLocaleString('es-CL')+' + margen '+(c.desglose.margen_pct||0).toFixed(0)+'% + IVA '+(c.desglose.iva_pct||0).toFixed(0)+'%' : 'Precio cliente'}">${fClp(precioVisible)}</div>
        ${subLineaPrecio}
        ${badgeProfit}
      </div>
    </div>`;
  }

  // Render sin cobertura (colapsable)
  if(sinCob.length){
    html += `
      <details style="margin-top:6px">
        <summary style="cursor:pointer;color:#9ca3af;font-size:.74rem;padding:7px 10px;
                        background:#f9fafb;border-radius:6px;list-style:none;display:flex;
                        align-items:center;justify-content:space-between">
          <span><i class="bi bi-x-circle me-1"></i>Sin cobertura: ${sinCob.length} courier${sinCob.length>1?'s':''}</span>
          <span style="font-size:.66rem;opacity:.6">click para expandir</span>
        </summary>
        <div style="margin-top:6px;display:flex;flex-direction:column;gap:5px">
          ${sinCob.map(c => `
            <div style="display:flex;align-items:center;gap:10px;padding:7px 12px;
                       border:1px dashed #e5e7eb;border-radius:7px;background:#fafafa;opacity:.7">
              <div style="min-width:96px">${_logoFor(c)}</div>
              <div style="flex:1;font-size:.74rem;color:#6b7280">
                <b>${escHtml(_courierDisplay(c.courier_nombre))}</b><br>
                <span style="font-size:.66rem">${escHtml(c.mensaje||'Sin cobertura')}</span>
              </div>
            </div>`).join('')}
        </div>
      </details>`;
  }

  list.innerHTML = html;
}

function setCourier(c){
  // c es {courier_id, courier_nombre, precio, ...} de _cotizaciones
  if(typeof c === 'string'){
    // Compat con llamadas legacy si quedan en algún lado — buscar por nombre
    const found = _cotizaciones.find(x => (x.courier_nombre||'').toLowerCase() === c.toLowerCase());
    if(found) c = found;
    else return;
  }
  _courierSel = {
    id:        c.courier_id,
    nombre:    c.courier_nombre,
    precio:    c.precio,
    transito:  c.tiempo_transito,
    fuente:    c.fuente,
    servicio:  c.servicio,
  };
  renderCouriers();
}

// ════════════════════════════════════════════════════════════
//  AUDITORÍA DE COTIZACIÓN (TRACE + HISTÓRICO)
// ════════════════════════════════════════════════════════════
let _auditCtx = null; // { courier_id, courier_nombre, comuna, bracket, precio }

function abrirAuditoriaCourier(auditId, cotizacion){
  // Guardar contexto para el botón "Validar"
  const trace = cotizacion.trace || {};
  _auditCtx = {
    courier_id:     cotizacion.courier_id,
    courier_nombre: cotizacion.courier_nombre,
    comuna:         (_docData?.header?.comuna || ''),
    bracket:        trace.bracket,
    precio:         cotizacion.precio,
  };

  const modal = document.getElementById('modalAudit');
  modal.style.display = 'flex';
  document.getElementById('audTitle').textContent =
    `Auditoría · ${cotizacion.courier_nombre || 'Courier'}`;

  const body = document.getElementById('audBody');
  body.innerHTML = `<div class="text-center text-muted" style="padding:32px">
    <div class="spinner-border spinner-border-sm text-danger"></div>
    <div style="margin-top:8px">Cargando histórico…</div>
  </div>`;

  // Render con datos del trace inmediato + fetch del histórico
  const fmt = n => n != null ? '$'+Math.round(n).toLocaleString('es-CL') : '—';

  let html = '<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:14px">';
  html += `<div style="background:#f8fafc;padding:10px 12px;border-radius:8px;border:1px solid #e2e8f0">
    <div style="font-size:.66rem;color:#64748b;font-weight:700;text-transform:uppercase;letter-spacing:.5px">Precio al cliente</div>
    <div style="font-size:1.45rem;font-weight:900;color:#dc2626;margin-top:2px">${fmt(cotizacion.precio)}</div>
    <div style="font-size:.72rem;color:#64748b">Peso usado: ${(trace.peso_usado||0).toFixed(2)} kg</div>
  </div>`;
  html += `<div style="background:#f8fafc;padding:10px 12px;border-radius:8px;border:1px solid #e2e8f0">
    <div style="font-size:.66rem;color:#64748b;font-weight:700;text-transform:uppercase;letter-spacing:.5px">Bracket aplicado</div>
    <div style="font-size:1.45rem;font-weight:900;color:#0f172a;margin-top:2px">${escHtml(trace.bracket||'—')}</div>
    <div style="font-size:.72rem;color:#64748b">Comuna: ${escHtml(trace.comuna_db||_auditCtx.comuna)}</div>
  </div>`;
  html += '</div>';

  // ── Desglose comercial Costo → Margen → IVA → Venta (Daniel 22/05/2026) ──
  // Si la cotización viene del backend con desglose, lo mostramos. Es la
  // pieza clave para auditoría FedEx: muestra cómo se llegó al precio final.
  const dsg = cotizacion.desglose || trace.desglose;
  if(dsg && dsg.precio_costo != null){
    html += `<div style="background:linear-gradient(135deg,#fef2f2 0%,#fff 100%);border:1.5px solid #dc2626;border-radius:8px;padding:12px 14px;margin-bottom:12px">
      <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px">
        <b style="color:#0f172a;font-size:.88rem"><i class="bi bi-calculator me-1" style="color:#dc2626"></i>Desglose del cálculo (auditable)</b>
        <span style="font-size:.66rem;color:#64748b;font-weight:600">Fuente Excel + política ILUS</span>
      </div>
      <table style="width:100%;font-size:.82rem;border-collapse:collapse">
        <tbody>
          <tr style="border-bottom:1px solid #fde2e2">
            <td style="padding:6px 0;color:#64748b">Costo del Excel</td>
            <td style="padding:6px 0;text-align:right;font-weight:700;color:#0f172a">${fmt(dsg.precio_costo)}</td>
          </tr>
          <tr style="border-bottom:1px solid #fde2e2">
            <td style="padding:6px 0;color:#64748b">+ Margen ${(dsg.margen_pct||0).toFixed(0)}%</td>
            <td style="padding:6px 0;text-align:right;color:#16a34a;font-weight:600">+ ${fmt(dsg.margen_clp)}</td>
          </tr>
          <tr style="border-bottom:1.5px solid #cbd5e1;background:#f8fafc">
            <td style="padding:6px 0;color:#475569;font-weight:600;padding-left:8px">= Subtotal neto</td>
            <td style="padding:6px 0;text-align:right;font-weight:700;color:#0f172a">${fmt(dsg.subtotal_neto)}</td>
          </tr>
          <tr style="border-bottom:1px solid #fde2e2">
            <td style="padding:6px 0;color:#64748b">+ IVA ${(dsg.iva_pct||0).toFixed(0)}%</td>
            <td style="padding:6px 0;text-align:right;color:#16a34a;font-weight:600">+ ${fmt(dsg.iva_clp)}</td>
          </tr>
          <tr style="background:#fff;border-top:2px solid #dc2626">
            <td style="padding:8px 0 4px;color:#dc2626;font-weight:800;text-transform:uppercase;font-size:.78rem">Precio venta cliente</td>
            <td style="padding:8px 0 4px;text-align:right;font-weight:900;color:#dc2626;font-size:1.05rem">${fmt(dsg.precio_venta)}</td>
          </tr>
        </tbody>
      </table>
    </div>`;
  }

  // Validación / fuente
  const fuenteLabel = {
    'api': 'API en vivo (FedEx)',
    'tabla': 'Tabla de tarifas',
    'tabla_fallback': 'Tabla (API caída)',
    'validado_excel': 'Validado contra Excel',
    'manual': 'Edición manual',
    'no_cobertura': 'Sin cobertura',
    'error': 'Error',
  }[cotizacion.fuente] || cotizacion.fuente || '—';
  const validadoChip = trace.validado
    ? '<span style="background:#dcfce7;color:#15803d;padding:3px 8px;border-radius:50px;font-size:.7rem;font-weight:700">✓ VALIDADA</span>'
    : '<span style="background:#fff8e1;color:#92400e;padding:3px 8px;border-radius:50px;font-size:.7rem;font-weight:700">Sin validar</span>';

  html += `<div style="background:#fff;border:1px solid #e2e8f0;border-radius:8px;padding:10px 12px;margin-bottom:12px;font-size:.82rem">
    <div style="display:flex;align-items:center;justify-content:space-between;gap:10px;margin-bottom:6px">
      <b style="color:#0f172a">Trazabilidad del cálculo</b>${validadoChip}
    </div>
    <div style="font-size:.78rem;color:#374151;line-height:1.55">
      <div><b>Fuente:</b> ${escHtml(fuenteLabel)}</div>
      <div><b>Fórmula:</b> <code style="font-size:.74rem;background:#f3f4f6;padding:1px 5px;border-radius:3px">${escHtml(trace.formula||'—')}</code></div>
      <div><b>Brackets disponibles:</b> ${(trace.json_brackets_disponibles||[]).map(b=>`<code style="background:#f3f4f6;padding:0 4px;border-radius:3px;margin-right:3px">${escHtml(b)}</code>`).join(' ') || '—'}</div>
      ${cotizacion.audit_id ? `<div style="font-size:.7rem;color:#9ca3af;margin-top:4px">Audit ID #${cotizacion.audit_id}</div>` : ''}
    </div>
  </div>`;

  // Advertencias
  const advs = trace.advertencias || [];
  if(advs.length){
    html += `<div style="background:#fef3c7;border:1.5px solid #f59e0b;border-radius:8px;padding:10px 12px;margin-bottom:12px;font-size:.8rem;color:#92400e">
      <b><i class="bi bi-exclamation-triangle me-1"></i>Advertencias:</b>
      <ul style="margin:6px 0 0 22px;padding:0">
        ${advs.map(a => `<li style="margin-bottom:3px">${escHtml(a)}</li>`).join('')}
      </ul>
    </div>`;
  }

  // Placeholder histórico (se rellena con fetch)
  html += `<div id="audHistorico">
    <div style="text-align:center;color:#9ca3af;padding:20px 0;font-size:.82rem">
      <div class="spinner-border spinner-border-sm" style="opacity:.5"></div>
      <div style="margin-top:4px">Cargando histórico…</div>
    </div>
  </div>`;

  body.innerHTML = html;

  // Habilitar botón "Marcar como validada" solo si hay bracket válido
  const btnVal = document.getElementById('audBtnValidar');
  if(trace.bracket && cotizacion.tiene_cobertura && !trace.validado){
    btnVal.style.display = '';
  } else {
    btnVal.style.display = 'none';
  }

  // Cargar histórico
  cargarAuditHistorico(auditId, cotizacion);
}

async function cargarAuditHistorico(auditId, cotizacion){
  const el = document.getElementById('audHistorico');
  if(!el) return;
  try {
    let url;
    if(auditId){
      url = `/api/transporte/courier-audit/${auditId}`;
      const r = await fetch(url);
      const d = await r.json();
      if(!r.ok || d.error) throw new Error(d.error || 'HTTP '+r.status);
      _renderAuditHistorico(el, d.historico || [], 'misma comuna + courier');
    } else {
      url = `/api/transporte/courier-audit?courier_id=${cotizacion.courier_id}&comuna=${encodeURIComponent(_auditCtx.comuna)}&limit=10`;
      const r = await fetch(url);
      const d = await r.json();
      if(!r.ok || d.error) throw new Error(d.error || 'HTTP '+r.status);
      _renderAuditHistorico(el, d.rows || [], 'misma comuna + courier');
    }
  } catch(e){
    el.innerHTML = `<div style="color:#dc2626;font-size:.78rem;text-align:center;padding:12px">
      Error cargando histórico: ${escHtml(e.message)}</div>`;
  }
}

function _renderAuditHistorico(el, rows, contextLabel){
  if(!rows.length){
    el.innerHTML = `<div style="text-align:center;color:#9ca3af;padding:14px;font-size:.78rem;
      background:#f9fafb;border-radius:6px">
      Sin cotizaciones previas para ${escHtml(contextLabel)}.
    </div>`;
    return;
  }
  const fmt = n => n != null ? '$'+Math.round(n).toLocaleString('es-CL') : '—';
  let html = `<div style="margin-top:4px"><b style="font-size:.82rem;color:#374151">
    Últimas ${rows.length} cotizaciones (${escHtml(contextLabel)})
  </b></div>`;
  html += `<div style="margin-top:6px;border:1px solid #e5e7eb;border-radius:8px;overflow:hidden">
    <table style="width:100%;font-size:.74rem;border-collapse:collapse">
    <thead style="background:#f9fafb"><tr>
      <th style="text-align:left;padding:6px 8px;border-bottom:1px solid #e5e7eb">Fecha</th>
      <th style="text-align:left;padding:6px 8px;border-bottom:1px solid #e5e7eb">Peso</th>
      <th style="text-align:left;padding:6px 8px;border-bottom:1px solid #e5e7eb">Bracket</th>
      <th style="text-align:right;padding:6px 8px;border-bottom:1px solid #e5e7eb">Precio</th>
      <th style="text-align:left;padding:6px 8px;border-bottom:1px solid #e5e7eb">Fuente</th>
      <th style="text-align:left;padding:6px 8px;border-bottom:1px solid #e5e7eb">Por</th>
    </tr></thead><tbody>`;
  for(const h of rows){
    const validIcon = h.validado ? ' <span style="color:#16a34a" title="Validada">✓</span>' : '';
    html += `<tr style="border-bottom:1px solid #f3f4f6">
      <td style="padding:5px 8px;color:#6b7280">${escHtml((h.cotizacion_at||'').slice(0,16))}</td>
      <td style="padding:5px 8px">${(h.peso_kg||0).toFixed(1)} kg</td>
      <td style="padding:5px 8px"><code style="background:#f3f4f6;padding:0 4px;border-radius:3px;font-size:.72rem">${escHtml(h.bracket_aplicado||'—')}</code></td>
      <td style="padding:5px 8px;text-align:right;font-weight:600">${fmt(h.precio_calculado)}${validIcon}</td>
      <td style="padding:5px 8px;color:#6b7280;font-size:.7rem">${escHtml(h.fuente||'—')}</td>
      <td style="padding:5px 8px;color:#6b7280;font-size:.7rem">${escHtml(h.cotizado_por||'—')}</td>
    </tr>`;
  }
  html += '</tbody></table></div>';
  el.innerHTML = html;
}

function cerrarAuditoriaCourier(){
  document.getElementById('modalAudit').style.display = 'none';
  _auditCtx = null;
}

async function validarTarifaActual(){
  if(!_auditCtx || !_auditCtx.courier_id) return;
  const precioRaw = await ilusPrompt({
    title: 'Validar tarifa contra Excel',
    message: `Confirma el precio CORRECTO para ${_auditCtx.courier_nombre} en ${_auditCtx.comuna} bracket "${_auditCtx.bracket}":`,
    placeholder: 'Ej: 105237',
    defaultValue: String(_auditCtx.precio || ''),
    required: true,
  });
  if(precioRaw === null) return;
  const precio = parseInt(precioRaw.replace(/[.,$\s]/g,''), 10);
  if(isNaN(precio) || precio <= 0){
    await ilusAlert({title:'Precio inválido', message:'Debe ser un número entero positivo.', type:'warning'});
    return;
  }
  const notas = await ilusPrompt({
    title: 'Notas de validación',
    message: 'Fuente o referencia (ej: Excel maestro 21/05/2026)',
    placeholder: 'Excel maestro 21/05/2026',
    required: false,
  });
  if(notas === null) return;

  try {
    const r = await fetch(`/api/transporte/couriers/${_auditCtx.courier_id}/tarifa-validar`, {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({
        comuna:  _auditCtx.comuna,
        bracket: _auditCtx.bracket,
        precio_correcto: precio,
        notas: notas || '',
      })
    });
    const d = await r.json();
    if(!r.ok || d.error) throw new Error(d.error || 'HTTP '+r.status);
    ilusToast(`✓ Tarifa validada: ${_auditCtx.courier_nombre} ${_auditCtx.comuna} = $${precio.toLocaleString('es-CL')}`, {type:'success'});
    cerrarAuditoriaCourier();
    // Refrescar cotizaciones
    _cotCache.clear?.();
    actualizarTarifas();
  } catch(e){
    await ilusAlert({title:'Error al validar', message:e.message, type:'error'});
  }
}

// ════════════════════════════════════════════════════════════
//  TOGGLE COSTO / VENTA + COMPARADOR TOP 3
//  Daniel quiere ver de un vistazo: el más barato, el validado y
//  el recomendado. Y en cuotas (sin re-fetch) puede cambiar entre
//  "ver precio venta cliente" vs "ver costo ILUS" para analizar margen.
// ════════════════════════════════════════════════════════════
let _vistaPrecio = 'venta';   // 'venta' (default) | 'costo'

function setVistaPrecio(modo){
  if(modo !== 'venta' && modo !== 'costo') modo = 'venta';
  _vistaPrecio = modo;
  // Actualizar estilos de los segmentos
  document.querySelectorAll('#toggleCostoVentaWrap .seg-btn').forEach(b => {
    const active = (b.dataset.vista === modo);
    b.style.background = active ? '#dc2626' : 'transparent';
    b.style.color      = active ? '#fff'    : '#6b7280';
  });
  const hint = document.getElementById('vistaPrecioHint');
  if(hint){
    hint.textContent = modo === 'costo'
      ? 'Precio de tabla SPHS, sin seguro.'
      : 'Precio de tabla SPHS + seguro 1,2% del valor declarado.';
  }
  renderCouriers();
}

function _precioMostrar(c){
  // Devuelve el precio según vista activa. Si no hay desglose, usa precio.
  if(_vistaPrecio === 'costo' && c.desglose && c.desglose.precio_costo != null){
    return c.desglose.precio_costo;
  }
  return c.precio;
}

function renderComparadorTop(conCob){
  const wrap = document.getElementById('comparadorTop');
  if(!wrap) return;
  if(!conCob || conCob.length === 0){
    wrap.style.display = 'none';
    return;
  }

  // Identificar 3 categorías clave (puede haber solape)
  // - cheapest: el más barato (por precio venta)
  // - validated: el más barato con trace.validado === true
  // - fastest: en futuro podríamos rankear por tiempo, ahora solo "recomendado"
  const sorted = [...conCob].sort((a,b) => (a.precio||0) - (b.precio||0));
  const cheapest  = sorted[0] || null;
  const validated = sorted.find(c => c.trace && c.trace.validado) || null;
  // El "validado distinto al más barato" — si coinciden no lo mostramos 2 veces
  const validatedDistinct = (validated && validated.courier_id !== (cheapest?.courier_id)) ? validated : null;

  // Si solo tenemos un courier o todos coinciden → no mostramos comparador
  if(conCob.length < 2 && !validatedDistinct){
    wrap.style.display = 'none';
    return;
  }

  const fmt = n => n != null ? '$'+Math.round(n).toLocaleString('es-CL') : '—';
  const cell = (c, label, color, icon) => {
    if(!c) return '';
    const p = _precioMostrar(c);
    const isVenta = (_vistaPrecio === 'venta');
    const subPrice = (c.desglose && c.desglose.precio_costo != null && isVenta)
      ? `<div style="font-size:.6rem;color:#9ca3af;margin-top:1px">costo ${fmt(c.desglose.precio_costo)}</div>`
      : '';
    return `
      <div onclick='setCourier(${JSON.stringify(c).replace(/'/g, "&#39;")})'
           style="background:#fff;border:1.5px solid ${color};border-radius:9px;
                  padding:9px 11px;cursor:pointer;flex:1;min-width:0;
                  transition:transform .12s,box-shadow .12s"
           onmouseover="this.style.transform='translateY(-1px)';this.style.boxShadow='0 4px 12px rgba(0,0,0,.08)'"
           onmouseout="this.style.transform='';this.style.boxShadow=''">
        <div style="font-size:.62rem;color:${color};font-weight:800;text-transform:uppercase;letter-spacing:.4px">
          ${icon} ${label}
        </div>
        <div style="font-weight:700;color:#0f172a;font-size:.82rem;margin-top:2px;
                    white-space:nowrap;overflow:hidden;text-overflow:ellipsis">
          ${escHtml(_courierDisplay(c.courier_nombre) || '—')}
        </div>
        <div style="color:${color};font-weight:900;font-size:1.08rem;margin-top:1px">
          ${fmt(p)}
        </div>
        ${subPrice}
      </div>`;
  };

  const cells = [
    cell(cheapest,           'Más barato',  '#16a34a', '🏆'),
    cell(validatedDistinct,  'Validada',    '#0ea5e9', '✓'),
  ].filter(Boolean);

  wrap.innerHTML = `
    <div style="display:flex;gap:8px;align-items:stretch">
      ${cells.join('')}
    </div>`;
  wrap.style.display = '';
}

// ════════════════════════════════════════════════════════════
//  HISTÓRICO DE COTIZACIONES POR DOCUMENTO
//  Daniel cotiza FCV 10644 hoy y la próxima semana → puede comparar
//  ambas sesiones lado a lado en este modal.
// ════════════════════════════════════════════════════════════
async function abrirHistoricoDoc(){
  if(!_docData || !_docData.header){
    if(window.ilusAlert) await ilusAlert({
      title: 'Carga un documento primero',
      message: 'Para ver el histórico necesito un TIDO+NUDO cargado.',
      type: 'info',
    });
    return;
  }
  const tido = _docData.header.tido || document.getElementById('docTipo')?.value || '';
  const nudo = _docData.header.nudo || (document.getElementById('docNudo')?.value || '').trim();
  if(!tido || !nudo){
    if(window.ilusToast) ilusToast('Falta TIDO o NUDO en el documento.', {type:'warning'});
    return;
  }

  // Asegurar que existe el modal
  let modal = document.getElementById('modalHistDoc');
  if(!modal){
    modal = document.createElement('div');
    modal.id = 'modalHistDoc';
    modal.className = 'modal-mf';
    modal.style.display = 'none';
    modal.innerHTML = `
      <div class="modal-mf-backdrop" onclick="cerrarHistoricoDoc()"></div>
      <div class="modal-mf-box" style="width:min(880px,96vw)">
        <div class="modal-mf-hdr">
          <i class="bi bi-clock-history" style="color:#fff;font-size:1.2rem"></i>
          <h3 id="hdTitle">Histórico de cotizaciones</h3>
          <button class="modal-mf-x" onclick="cerrarHistoricoDoc()" aria-label="Cerrar">&times;</button>
        </div>
        <div class="modal-mf-body" id="hdBody">
          <div class="text-center text-muted" style="padding:32px">
            <div class="spinner-border spinner-border-sm text-danger"></div>
            <div style="margin-top:8px">Cargando histórico…</div>
          </div>
        </div>
        <div class="modal-mf-foot">
          <button class="btn btn-light" onclick="cerrarHistoricoDoc()">Cerrar</button>
          <button class="btn btn-dark" id="hdBtnExport" onclick="exportarAuditDoc()">
            <i class="bi bi-file-earmark-excel me-1"></i>Exportar a Excel
          </button>
        </div>
      </div>`;
    document.body.appendChild(modal);
  }

  modal.style.display = 'flex';
  document.getElementById('hdTitle').textContent =
    `Histórico · ${tido} ${nudo}`;
  const body = document.getElementById('hdBody');
  body.innerHTML = `<div class="text-center text-muted" style="padding:32px">
    <div class="spinner-border spinner-border-sm text-danger"></div>
    <div style="margin-top:8px">Cargando histórico…</div>
  </div>`;

  try {
    const r = await fetch(`/api/transporte/courier-audit/por-documento?tido=${encodeURIComponent(tido)}&nudo=${encodeURIComponent(nudo)}`);
    const d = await r.json();
    if(!r.ok || d.error) throw new Error(d.error || 'HTTP '+r.status);
    _renderHistoricoDoc(body, d);
  } catch(e){
    body.innerHTML = `<div style="color:#dc2626;font-size:.86rem;text-align:center;padding:18px">
      Error cargando histórico: ${escHtml(e.message)}</div>`;
  }
}

function _renderHistoricoDoc(el, data){
  const sesiones = data.sesiones || [];
  const fmt = n => n != null ? '$'+Math.round(n).toLocaleString('es-CL') : '—';
  if(!sesiones.length){
    el.innerHTML = `<div style="text-align:center;color:#9ca3af;padding:30px;font-size:.88rem;
                                background:#f9fafb;border-radius:8px">
      <i class="bi bi-inbox" style="font-size:2rem;display:block;margin-bottom:6px;color:#cbd5e1"></i>
      Sin cotizaciones previas para este documento.<br>
      <span style="font-size:.74rem">La primera cotización quedará registrada aquí.</span>
    </div>`;
    return;
  }

  let html = `<div style="font-size:.82rem;color:#64748b;margin-bottom:12px">
    <b>${sesiones.length}</b> sesión${sesiones.length>1?'es':''} de cotización
    encontrada${sesiones.length>1?'s':''} para <b>${escHtml(data.tido)} ${escHtml(data.nudo)}</b>.
    Cada sesión agrupa cotizaciones cercanas en tiempo.
  </div>`;

  for(let i=0; i<sesiones.length; i++){
    const s = sesiones[i];
    const couriersCount = s.couriers.length;
    const minPrecio = s.couriers.reduce((m, c) => (c.precio_venta && (!m || c.precio_venta < m)) ? c.precio_venta : m, null);
    const isOpenByDefault = (i === 0); // primera sesión expandida

    html += `<details ${isOpenByDefault?'open':''}
      style="border:1px solid #e5e7eb;border-radius:9px;margin-bottom:8px;background:#fff">
      <summary style="padding:10px 14px;cursor:pointer;list-style:none;
                      display:flex;align-items:center;justify-content:space-between;gap:8px">
        <div style="display:flex;align-items:center;gap:10px">
          <i class="bi bi-chevron-right" style="font-size:.74rem;color:#6b7280"></i>
          <div>
            <div style="font-weight:700;color:#0f172a;font-size:.86rem">
              <i class="bi bi-calendar3 me-1" style="color:#dc2626"></i>
              ${escHtml(s.session_at)} · ${escHtml(s.comuna || 'Sin comuna')}
            </div>
            <div style="font-size:.7rem;color:#6b7280;margin-top:2px">
              Peso: ${(s.peso_kg||0).toFixed(1)} kg
              · Por: ${escHtml(s.cotizado_por || '—')}
              · ${couriersCount} courier${couriersCount>1?'s':''}
              · desde ${fmt(minPrecio)}
            </div>
          </div>
        </div>
      </summary>
      <div style="padding:0 14px 12px">
        <table style="width:100%;font-size:.76rem;border-collapse:collapse">
          <thead style="background:#f9fafb">
            <tr>
              <th style="text-align:left;padding:6px 8px;border-bottom:1px solid #e5e7eb">Courier</th>
              <th style="text-align:left;padding:6px 8px;border-bottom:1px solid #e5e7eb">Bracket</th>
              <th style="text-align:right;padding:6px 8px;border-bottom:1px solid #e5e7eb">Costo</th>
              <th style="text-align:right;padding:6px 8px;border-bottom:1px solid #e5e7eb">Margen</th>
              <th style="text-align:right;padding:6px 8px;border-bottom:1px solid #e5e7eb">IVA</th>
              <th style="text-align:right;padding:6px 8px;border-bottom:1px solid #e5e7eb">Venta</th>
              <th style="text-align:center;padding:6px 8px;border-bottom:1px solid #e5e7eb">Fuente</th>
            </tr>
          </thead>
          <tbody>`;
    for(const c of s.couriers){
      const cheap = (c.precio_venta === minPrecio) ? ' 🏆' : '';
      const validIcon = c.validado ? ' <span title="Validada" style="color:#16a34a">✓</span>' : '';
      html += `<tr style="border-bottom:1px solid #f3f4f6">
        <td style="padding:5px 8px;font-weight:600;color:#0f172a">${escHtml(_courierDisplay(c.courier_nombre))}${cheap}${validIcon}</td>
        <td style="padding:5px 8px">
          <code style="background:#f3f4f6;padding:1px 5px;border-radius:3px;font-size:.7rem">${escHtml(c.bracket || '—')}</code>
        </td>
        <td style="padding:5px 8px;text-align:right;color:#6b7280">${c.precio_costo ? fmt(c.precio_costo) : '—'}</td>
        <td style="padding:5px 8px;text-align:right;color:#16a34a">${c.margen_clp ? '+'+fmt(c.margen_clp) : '—'}</td>
        <td style="padding:5px 8px;text-align:right;color:#16a34a">${c.iva_clp ? '+'+fmt(c.iva_clp) : '—'}</td>
        <td style="padding:5px 8px;text-align:right;font-weight:800;color:#dc2626">${fmt(c.precio_venta)}</td>
        <td style="padding:5px 8px;text-align:center;font-size:.66rem;color:#6b7280">${escHtml(c.fuente || '—')}</td>
      </tr>`;
    }
    html += `</tbody></table></div></details>`;
  }
  el.innerHTML = html;
}

function cerrarHistoricoDoc(){
  const m = document.getElementById('modalHistDoc');
  if(m) m.style.display = 'none';
}

function exportarAuditDoc(){
  // Descarga el Excel filtrado por tido+nudo del documento actual.
  if(!_docData || !_docData.header){
    if(window.ilusToast) ilusToast('Carga un documento primero.', {type:'warning'});
    return;
  }
  const tido = _docData.header.tido || document.getElementById('docTipo')?.value || '';
  const nudo = _docData.header.nudo || (document.getElementById('docNudo')?.value || '').trim();
  if(!tido || !nudo){
    if(window.ilusToast) ilusToast('Falta TIDO o NUDO.', {type:'warning'});
    return;
  }
  const url = `/api/transporte/courier-audit.xlsx?tido=${encodeURIComponent(tido)}&nudo=${encodeURIComponent(nudo)}&limit=5000`;
  // Trigger download
  window.location.href = url;
  if(window.ilusToast) ilusToast('Descargando Excel…', {type:'info'});
}

// ════════════════════════════════════════════════════════════
//  VALIDACIÓN MANIFIESTO
//  ACTUALIZADO 2026-07-22 (Daniel, reemplaza la decisión de 2026-05-31):
//  "validar que el teléfono sea chileno y que sea obligatorio, porque si no
//  después la otra llamada [a FedEx] da error". Teléfono, email y dirección
//  georreferenciada (lat/lng vía Google Places) AHORA BLOQUEAN — antes
//  tel/email solo advertían. La validación real y definitiva vive en el
//  backend (POST /transporte/api/cubicador/enviar-manifiesto); esta función
//  es la primera línea de defensa para dar feedback inmediato sin esperar
//  el viaje de red.
// ════════════════════════════════════════════════════════════

// Validadores chilenos reusables
function _esTelChileno(tel){
  // Acepta: +56 9 XXXX XXXX (móvil) o +56 X XXXX XXXX (fijo).
  // Normalizado: 9 dígitos después del +56 (o sin +56, partiendo con 9 o 2).
  const t = (tel || '').replace(/[\s\-()]/g, '');
  if (!t) return null; // vacío = sin opinión
  return /^(\+?56)?[29]\d{8}$/.test(t);
}
function _esEmailValido(email){
  const e = (email || '').trim();
  if (!e) return null; // vacío = sin opinión
  return /^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$/.test(e);
}

// Aplica clase visual is-valid / is-invalid (Bootstrap) según validador
function _aplicarValidacion(el, fnValidar){
  if (!el) return;
  const v = fnValidar(el.value);
  el.classList.remove('is-valid','is-invalid');
  if (v === true)  el.classList.add('is-valid');
  if (v === false) el.classList.add('is-invalid');
}

// Wire-up de validación en vivo (idempotente, llamar al cargar la página
// y también después de renderDoc para inputs nuevos).
function _wireValidacionVivo(){
  const tel = document.getElementById('cli-tel');
  if (tel && !tel.dataset.valWired){
    tel.dataset.valWired = '1';
    tel.addEventListener('input', () => _aplicarValidacion(tel, _esTelChileno));
    tel.addEventListener('blur',  () => _aplicarValidacion(tel, _esTelChileno));
  }
  const mail = document.getElementById('cli-email');
  if (mail && !mail.dataset.valWired){
    mail.dataset.valWired = '1';
    mail.addEventListener('input', () => _aplicarValidacion(mail, _esEmailValido));
    mail.addEventListener('blur',  () => _aplicarValidacion(mail, _esEmailValido));
  }
}
document.addEventListener('DOMContentLoaded', _wireValidacionVivo);

// Extrae Comuna + Región de los address_components de Google y los aplica a
// los campos (Daniel 2026-07-22). Heurística de comuna PORTADA de
// _edParseComponentes (manifiesto_detalle.html): en Chile ni 'locality' ni
// 'administrative_area_level_3' son confiables por sí solos —
// Providencia llega como locality, Viña del Mar a veces como level_3 — así
// que se prefiere el que NO sea la provincia (level_2), comparando sin
// acentos. Región = administrative_area_level_1.
function _aplicarComunaRegionDeGoogle(componentes){
  if (!Array.isArray(componentes)) return;
  var loc = '', l3 = '', l2 = '', region = '';
  componentes.forEach(function(c){
    var t = c.types || [], n = c.long_name || '';
    if (t.indexOf('locality') !== -1 && !loc) loc = n;
    if (t.indexOf('administrative_area_level_3') !== -1 && !l3) l3 = n;
    if (t.indexOf('administrative_area_level_2') !== -1 && !l2) l2 = n;
    if (t.indexOf('administrative_area_level_1') !== -1) region = n;
  });
  function _norm(s){
    return (s||'').normalize('NFD').replace(/[̀-ͯ]/g,'').toLowerCase().trim();
  }
  var comuna = '';
  if (loc && _norm(loc) !== _norm(l2))     comuna = loc;
  else if (l3 && _norm(l3) !== _norm(l2))  comuna = l3;
  else                                     comuna = loc || l3 || l2;

  var elComuna = document.getElementById('cli-comuna');
  var elRegion = document.getElementById('cli-region');
  // Se PISA el valor existente a propósito: si el operador validó con Google,
  // Google manda (el dato del ERP puede venir con código o mal escrito).
  if (elComuna && comuna) elComuna.value = comuna;
  if (elRegion && region) elRegion.value = region;
  // La comuna cambió → el margen/cotización de couriers depende de ella.
  if (comuna && typeof actualizarTarifas === 'function') actualizarTarifas();
}

// ── Google Places para la dirección (Daniel 2026-07-22) ──────────────────
// Llena cli-dir-lat/lng/place-id — validarParaManifiesto() los exige antes
// de asignar a manifiesto (ver skill direcciones-google-places, ya usado en
// Mantenciones con el mismo patrón). Si el operador edita el texto DESPUÉS
// de elegir una sugerencia, se invalida la geolocalización (para no dejar
// un lat/lng viejo pegado a un texto que ya no es el mismo).
function _wireDireccionPlaces(){
  const dirInput = document.getElementById('cli-dir');
  if (!dirInput || dirInput.dataset.placesWired) return;
  function initDir(){
    if (typeof ilusPlacesAutocomplete !== 'function'){
      if (window.__ilusGmapsPending) window.__ilusGmapsPending.push(initDir);
      return;
    }
    dirInput.dataset.placesWired = '1';
    ilusPlacesAutocomplete('cli-dir', {
      country: 'cl',
      types: ['address'],
      onPlaceSelected: function(place){
        document.getElementById('cli-dir-lat').value      = place.lat;
        document.getElementById('cli-dir-lng').value      = place.lng;
        document.getElementById('cli-dir-place-id').value = place.place_id || '';
        dirInput.dataset.validatedValue = dirInput.value;
        const hint = document.getElementById('cli-dir-hint');
        if (hint){
          hint.innerHTML = '<i class="bi bi-check-circle-fill text-success me-1"></i>' +
            'Dirección verificada · <small>' + place.lat.toFixed(4) + ', ' + place.lng.toFixed(4) + '</small>';
        }
        // Comuna + Región desde Google (FIX 2026-07-22, reportado por Daniel:
        // "valido la dirección y no me cambió la comuna"). La versión anterior
        // solo llenaba la comuna SI el campo estaba vacío (`!comunaInput.value`)
        // — y el ERP casi siempre trae una comuna, así que la validación de
        // Google nunca la corregía. Ahora SIEMPRE se pisa con lo que dice
        // Google (es la fuente autoritativa cuando el operador valida) y
        // además se completa la Región.
        _aplicarComunaRegionDeGoogle(place.componentes);
      }
    });
  }
  initDir();
  dirInput.addEventListener('input', function(){
    if (dirInput.dataset.validatedValue && dirInput.value !== dirInput.dataset.validatedValue){
      document.getElementById('cli-dir-lat').value      = '';
      document.getElementById('cli-dir-lng').value      = '';
      document.getElementById('cli-dir-place-id').value = '';
      const hint = document.getElementById('cli-dir-hint');
      if (hint){
        hint.innerHTML = '<i class="bi bi-info-circle"></i> Escribe y elige una sugerencia para ' +
          'validar la dirección — obligatorio para asignar a manifiesto.';
      }
    }
  });
}
document.addEventListener('DOMContentLoaded', _wireDireccionPlaces);

async function validarParaManifiesto(){
  if(!_docData){
    await ilusAlert({title:'Documento requerido', message:'Carga un documento primero.', type:'warning'});
    return false;
  }

  // Dirección/comuna/bultos/teléfono/email — TODOS obligatorios ahora
  // (Daniel 2026-07-22: sin esto la llamada a FedEx falla después).
  const campos = [
    { id:'cli-dir',    nombre:'Dirección' },
    { id:'cli-bultos', nombre:'Total Bultos' },
    { id:'cli-comuna', nombre:'Comuna' },
    { id:'cli-tel',    nombre:'Teléfono' },
    { id:'cli-email',  nombre:'Email' },
  ];
  const vacios = campos.filter(c => !(document.getElementById(c.id)?.value||'').trim());
  if(vacios.length){
    await ilusAlert({
      title:'Faltan datos para el courier',
      message:'Completa los siguientes campos antes de asignar al manifiesto:',
      sub: vacios.map(c=>'• '+c.nombre).join('<br>'),
      subHtml:true,
      type:'warning',
    });
    document.getElementById(vacios[0].id)?.focus();
    return false;
  }

  // Validación DURA de formato (Daniel 2026-07-22): ya no se puede "avanzar
  // igual" — si el teléfono no es chileno o el email no tiene formato
  // válido, se bloquea (antes solo advertía).
  const tel  = document.getElementById('cli-tel').value.trim();
  const mail = document.getElementById('cli-email').value.trim();
  if (_esTelChileno(tel) === false){
    await ilusAlert({
      title: 'Teléfono inválido',
      message: `El teléfono "${tel}" no parece un número chileno válido.`,
      sub: 'Formato esperado: +56 9 XXXX XXXX (móvil) o +56 X XXXX XXXX (fijo).',
      type: 'error',
    });
    document.getElementById('cli-tel')?.focus();
    return false;
  }
  if (_esEmailValido(mail) === false){
    await ilusAlert({
      title: 'Email inválido',
      message: `El email "${mail}" no tiene un formato válido.`,
      type: 'error',
    });
    document.getElementById('cli-email')?.focus();
    return false;
  }

  // Dirección GEORREFERENCIADA obligatoria (Daniel 2026-07-22): la única
  // forma de garantizar esto es exigir que el operador haya elegido una
  // sugerencia del autocomplete de Google (eso llena cli-dir-lat/lng). Si el
  // texto de la dirección cambió después de seleccionar la sugerencia, se
  // pierde la validación y hay que volver a elegir.
  const lat = document.getElementById('cli-dir-lat')?.value;
  const lng = document.getElementById('cli-dir-lng')?.value;
  const _dirManual = !!document.getElementById('cli-dir-manual-check')?.checked;
  if (!lat && !lng && !_dirManual){
    await ilusAlert({
      title: 'Dirección sin validar',
      message: 'La dirección debe validarse con el buscador de Google antes de asignar a manifiesto.',
      sub: 'Escribe la dirección en el campo y elige una de las sugerencias del listado. ' +
           'Si el documento es una boleta y Google no reconoce la dirección (caso frecuente ' +
           'en consumidor final), marca la casilla "confirmarla manualmente" debajo del campo.',
      subHtml: true,
      type: 'error',
    });
    document.getElementById('cli-dir')?.focus();
    return false;
  }

  if(!_courierSel || !_courierSel.id){
    await ilusAlert({
      title:'Selecciona un courier',
      message:'Elige un courier de la lista antes de asignar al manifiesto.',
      type:'warning',
    });
    return false;
  }
  return true;
}

// ════════════════════════════════════════════════════════════
//  ACCIONES
// ════════════════════════════════════════════════════════════
async function enviarManifiesto(){
  if(!(await validarParaManifiesto())) return;
  abrirEnvioManifiesto();
}

async function abrirEnvioManifiesto(){
  const h = _docData.header || {};
  const courierNom = _courierSel?.nombre || '—';
  const costo      = _courierSel?.precio || 0;

  // Resumen del envío en el modal
  document.getElementById('mfResumen').innerHTML = `
    <div><b>Documento:</b> ${document.getElementById('docTipo').value} N° ${h.nudo || '—'}</div>
    <div><b>Cliente:</b> ${h.cliente_nombre || '—'}</div>
    <div><b>Destino:</b> ${h.comuna || '—'}</div>
    <div><b>Bultos:</b> ${document.getElementById('cli-bultos').value || _docData.totales?.total_bultos || 0}
         &nbsp;·&nbsp;
         <b>Courier:</b> ${courierNom}</div>
    <div><b>Costo cotizado:</b> ${costo ? '$' + Math.round(costo).toLocaleString('es-CL') : '—'}</div>
  `;
  // Inputs por defecto
  document.getElementById('mfFecha').value = new Date().toISOString().slice(0,10);
  document.getElementById('mfNotas').value = '';
  document.getElementById('mfAlert').style.display = 'none';
  // Por defecto: agregar a existente
  const radioExist = document.querySelector('input[name=mfModo][value=existente]');
  if (radioExist) radioExist.checked = true;
  _mfToggleModo();
  document.getElementById('modalManifiesto').style.display = 'flex';

  // Cargar manifiestos abiertos DEL MISMO COURIER (strict por default).
  // Decisión Daniel (2026-05-31): un manifiesto = un courier. No mezclar.
  const sel = document.getElementById('mfSelectExistente');
  sel.innerHTML = '<option value="">— Cargando… —</option>';
  try {
    const r = await fetch(`/transporte/api/manifiestos/abiertos?courier=${encodeURIComponent(courierNom)}&strict=1`);
    const d = await r.json();
    if (!r.ok || d.error) throw new Error(d.error || ('HTTP ' + r.status));
    const items        = d.manifiestos || [];
    const otrosCount   = d.otros_courier_count || 0;
    const radioExistente = document.querySelector('input[name=mfModo][value=existente]');
    const labelExistente = document.getElementById('mfRadioExistenteLabel');
    const subExistente   = document.getElementById('mfRadioExistenteSub');
    const radioNuevo     = document.querySelector('input[name=mfModo][value=nuevo]');
    if (!items.length) {
      // Sin manifiestos abiertos DEL COURIER → bloquear "existente" y forzar "nuevo"
      sel.innerHTML = `<option value="">— No hay manifiestos abiertos de ${courierNom} —</option>`;
      sel.disabled = true;
      if (radioExistente) radioExistente.disabled = true;
      if (labelExistente) {
        labelExistente.style.opacity = '0.45';
        labelExistente.style.cursor  = 'not-allowed';
      }
      if (subExistente) subExistente.textContent = `Ninguno en preparación para ${courierNom}`;
      const otrosTxt = otrosCount > 0
        ? ` <span style="color:#9ca3af;font-size:.78rem">(hay ${otrosCount} de otros courier, no aplica)</span>`
        : '';
      document.getElementById('mfHintExistente').innerHTML =
        `<i class="bi bi-info-circle"></i> No hay manifiestos abiertos de <b>${courierNom}</b> — se creará uno nuevo.${otrosTxt}`;
      if (radioNuevo) { radioNuevo.checked = true; _mfToggleModo(); }
    } else {
      // Hay manifiestos del courier → habilitar "existente" con la lista filtrada
      sel.disabled = false;
      if (radioExistente) radioExistente.disabled = false;
      if (labelExistente) { labelExistente.style.opacity = ''; labelExistente.style.cursor = ''; }
      if (subExistente) subExistente.textContent = `Selecciona uno de ${courierNom}`;
      // Como ya están filtrados por courier, el option no necesita repetir el courier.
      // Columnas: correlativo · fecha · ítems · estado
      sel.innerHTML = items.map(m => {
        const corr   = (m.correlativo || '').padEnd(14, ' ');
        // La API devuelve la fecha en ISO (YYYY-MM-DD) para uso interno;
        // acá se muestra en formato chileno DD-MM-YYYY (Daniel, Regla #6).
        const _fISO  = m.fecha || '';
        const _fDMY  = /^\d{4}-\d{2}-\d{2}$/.test(_fISO)
          ? _fISO.slice(8, 10) + '-' + _fISO.slice(5, 7) + '-' + _fISO.slice(0, 4)
          : _fISO;
        const fecha  = _fDMY.padEnd(10, ' ');
        const items_ = String(m.total_items || 0).padStart(3, ' ');
        const estado = m.estado || '';
        return `<option value="${m.id}" data-courier="${m.courier || ''}" data-estado="${estado}">`
             + `${corr} · ${fecha} · ${items_} ítems · ${estado}</option>`;
      }).join('');
      sel.style.fontFamily = 'ui-monospace, SFMono-Regular, Menlo, Consolas, monospace';
      sel.style.fontSize = '.82rem';
      const otrosTxt = otrosCount > 0
        ? ` <span style="color:#9ca3af;font-size:.78rem">· ${otrosCount} de otros courier ocultos</span>`
        : '';
      document.getElementById('mfHintExistente').innerHTML =
        `<i class="bi bi-list-check"></i> <b>${items.length}</b> manifiesto(s) abierto(s) de <b>${courierNom}</b>.${otrosTxt}`;
    }
  } catch(e) {
    sel.innerHTML = '<option value="">(error cargando)</option>';
    document.getElementById('mfAlert').textContent = 'Error cargando manifiestos: ' + e.message;
    document.getElementById('mfAlert').style.display = '';
  }
}

function _mfToggleModo(){
  const sel = document.querySelector('input[name=mfModo]:checked');
  const modo = sel ? sel.value : 'existente';
  document.getElementById('mfBoxExistente').style.display = (modo === 'existente') ? '' : 'none';
  document.getElementById('mfBoxNuevo').style.display     = (modo === 'nuevo')     ? '' : 'none';
}

function cerrarModalManifiesto(){
  document.getElementById('modalManifiesto').style.display = 'none';
}

async function enviarAManifiesto(){
  const btn = document.getElementById('mfBtnEnviar');
  const alertBox = document.getElementById('mfAlert');
  alertBox.style.display = 'none';

  const modoEl = document.querySelector('input[name=mfModo]:checked');
  const modo = modoEl ? modoEl.value : 'existente';
  const tido = document.getElementById('docTipo').value;
  const nudo = (document.getElementById('docNudo').value || '').trim();
  const courierNom = _courierSel?.nombre || '';
  const costo      = _courierSel?.precio || 0;

  const _t = _docData?.totales || {};
  const _h = _docData?.header   || {};
  const _gv = id => (document.getElementById(id)?.value || '').trim();
  // Productos declarados (lo que se ve en el cubicaje) → árbol del manifiesto
  const _productos = (_docData?.lineas || []).map(l => ({
    sku:      l.sku || '',
    nombre:   l.descripcion_erp || l.nombre_app || '',
    cantidad: l.cantidad || 0,
    saldo:    (l.saldo === undefined ? null : l.saldo),
  }));
  const payload = {
    tido, nudo,
    courier: courierNom,
    costo_cotizado: costo,
    notas_entrega: _gv('cli-notas'),
    // Cubicaje declarado → se persiste en el commitment (no se pierde aguas abajo)
    peso_real:         _t.peso_kg   || 0,
    peso_vol:          _t.peso_pv   || 0,
    volumen_m3:        (_t.vol_cm3 || 0) / 1000000,
    peso_predominante: _t.peso_pred || 0,
    n_bultos:          parseInt(_gv('cli-bultos') || _t.total_bultos || 0, 10) || null,
    // Header completo (lo que el operador VE/EDITA) → factura llega completa
    // cliente_nombre ahora se lee del input editable (Daniel 2026-07-27):
    // en boletas sin cliente identificado, el operador lo escribe a mano.
    cliente_nombre: _gv('cli-nombre') || _h.cliente_nombre || '',
    cliente_rut:    _h.cliente_rut || '',
    email:          _gv('cli-email') || _h.email || '',
    telefono:       _gv('cli-tel')   || _h.telefono || '',
    direccion:      _gv('cli-dir')   || _h.direccion || '',
    comuna:         _gv('cli-comuna') || _h.comuna || '',
    region:         _gv('cli-region') || _h.region || '',
    valor_neto:     _h.valor_neto || 0,
    productos:      _productos,
    // Dirección georreferenciada (Daniel 2026-07-22): el backend la exige
    // antes de asignar a manifiesto. Se llenan al elegir una sugerencia del
    // autocomplete de Google (ver _wireDireccionPlaces).
    direccion_lat:      _gv('cli-dir-lat')      || null,
    direccion_lng:      _gv('cli-dir-lng')      || null,
    direccion_place_id: _gv('cli-dir-place-id') || '',
    // Confirmación manual (Daniel 2026-07-27): boletas a veces traen una
    // dirección que Google no reconoce/sugiere. Con este checkbox marcado
    // el operador confirma la dirección escrita a mano — ver
    // #cli-dir-manual-check y el bypass correspondiente en el backend.
    direccion_manual: !!document.getElementById('cli-dir-manual-check')?.checked,
  };

  if (modo === 'existente') {
    const sel = document.getElementById('mfSelectExistente');
    const mid = sel.value;
    if (!mid) {
      alertBox.textContent = 'Selecciona un manifiesto o crea uno nuevo.';
      alertBox.style.display = ''; return;
    }
    const opt = sel.options[sel.selectedIndex];
    if (opt.dataset.estado === 'Cerrado' || opt.dataset.estado === 'Entregado completo') {
      alertBox.textContent = 'El manifiesto seleccionado está cerrado.';
      alertBox.style.display = ''; return;
    }
    if (opt.dataset.courier && opt.dataset.courier !== courierNom) {
      const ok = await ilusConfirm({
        title: 'Mismatch de courier',
        message: `El manifiesto es de ${opt.dataset.courier} pero la cotización es ${courierNom}.`,
        sub: '¿Continuar igual?',
        okLabel: 'Sí, continuar',
      });
      if (!ok) return;
    }
    payload.manifest_id = parseInt(mid, 10);
  } else {
    payload.manifest_id = null;
    payload.fecha = document.getElementById('mfFecha').value;
    payload.notas = document.getElementById('mfNotas').value.trim();
    if (!payload.fecha) {
      alertBox.textContent = 'La fecha es obligatoria para crear un manifiesto nuevo.';
      alertBox.style.display = ''; return;
    }
  }

  btn.disabled = true;
  btn.innerHTML = '<i class="bi bi-hourglass-split me-1"></i>Enviando…';
  // Loader ILUS con pasos ajustados a la velocidad real del backend (<500ms con cache)
  _mfShowProgress([
    { ms: 60,   pct: 25, txt: '📤 Enviando datos del documento…' },
    { ms: 200,  pct: 55, txt: '📦 Registrando en manifiesto…' },
    { ms: 450,  pct: 80, txt: '🔗 Vinculando productos…' },
    { ms: 900,  pct: 92, txt: '⏳ Confirmando con el servidor…' },
  ], { title: 'Enviando al manifiesto' });
  try {
    const r = await fetch('/transporte/api/cubicador/enviar-manifiesto', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(payload),
    });
    const d = await r.json();
    _mfFinishProgress();
    if (!r.ok || d.error) throw new Error(d.error || ('HTTP ' + r.status));
    cerrarModalManifiesto();
    if (window.ilusToast) {
      ilusToast(
        d.duplicate
          ? `Ya estaba en el manifiesto ${d.correlativo || ('#'+d.manifest_id)}`
          : `✓ Enviado al manifiesto ${d.correlativo || ('#'+d.manifest_id)}`,
        { type: d.duplicate ? 'warning' : 'success' }
      );
    }
    // Limpiar el cubicador para ingresar la siguiente factura SIN recargar
    _resetCubicadorParaSiguiente({
      manifest_id:   d.manifest_id,
      correlativo:   d.correlativo,
      duplicate:     d.duplicate,
    });
  } catch(e) {
    _mfFinishProgress();
    alertBox.textContent = 'Error: ' + e.message;
    alertBox.style.display = '';
    if (window.ilusToast) ilusToast('Error al enviar: ' + e.message, { type:'error' });
  } finally {
    btn.disabled = false;
    btn.innerHTML = '<i class="bi bi-send me-1"></i>Enviar al manifiesto';
  }
}

// ── Loader ILUS del modal "Enviar a manifiesto" (estilo Claude Design) ──
let _mfProgressTimers = [];
function _mfShowProgress(steps, opts){
  opts = opts || {};
  const overlay = document.getElementById('mfLoaderOverlay');
  if (!overlay) return;
  const title = document.getElementById('mfLoaderTitle');
  const fill  = document.getElementById('mfLoaderFill');
  const msg   = document.getElementById('mfLoaderMsg');
  if (title) title.textContent = opts.title || 'Enviando al manifiesto';
  if (fill)  fill.style.width  = '6%';
  if (msg)   msg.textContent   = 'Iniciando…';
  overlay.style.display = 'flex';
  _mfProgressTimers.forEach(t => clearTimeout(t));
  _mfProgressTimers = [];
  steps.forEach(s => {
    _mfProgressTimers.push(setTimeout(() => {
      if (fill) fill.style.width = s.pct + '%';
      if (msg)  { msg.style.opacity = '0'; setTimeout(() => { msg.textContent = s.txt; msg.style.opacity = '1'; }, 120); }
    }, s.ms));
  });
}
function _mfFinishProgress(){
  _mfProgressTimers.forEach(t => clearTimeout(t));
  _mfProgressTimers = [];
  const overlay = document.getElementById('mfLoaderOverlay');
  const fill    = document.getElementById('mfLoaderFill');
  const msg     = document.getElementById('mfLoaderMsg');
  if (fill) fill.style.width = '100%';
  if (msg)  msg.textContent  = '✓ Listo';
  setTimeout(() => {
    if (overlay) {
      overlay.style.opacity = '0';
      setTimeout(() => {
        overlay.style.display = 'none';
        overlay.style.opacity = '';  // reset para próxima vez
      }, 220);
    }
  }, 400);
}

// ── Reset del cubicador para ingresar siguiente factura SIN recargar ──
function _resetCubicadorParaSiguiente(info){
  info = info || {};
  // 1) Limpiar estado
  _docData = null;
  _courierSel = null;
  // 2) Ocultar todas las cards de resultado
  ['cardDatos','cardCubaje','cardNotas','cardCouriers','cardAcciones','cardFlujo']
    .forEach(id => { const el = document.getElementById(id); if (el) el.style.display = 'none'; });
  // 3) Limpiar banner de documento
  const banner = document.getElementById('docBanner');
  if (banner) banner.innerHTML = '';
  // 4) Mostrar mini-banner verde de "última factura enviada" con link al manifiesto
  const tip = document.getElementById('docBanner');
  if (tip && info.manifest_id){
    const corr = info.correlativo || ('#' + info.manifest_id);
    const verbo = info.duplicate ? 'Ya estaba en' : 'Enviada a';
    tip.innerHTML = `
      <div style="background:#dcfce7;border:1px solid #16a34a;border-left:4px solid #16a34a;
                  border-radius:10px;padding:12px 16px;margin-bottom:14px;
                  display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px">
        <div style="color:#166534;font-weight:600;font-size:.92rem">
          <i class="bi bi-check-circle-fill me-1"></i>
          ${verbo} manifiesto <strong>${corr}</strong> · Ingresa la siguiente factura ↓
        </div>
        <a href="/transporte/manifiestos/${info.manifest_id}" target="_blank"
           style="background:#16a34a;color:#fff;padding:6px 14px;border-radius:50px;
                  font-size:.78rem;font-weight:700;text-decoration:none;white-space:nowrap">
          <i class="bi bi-box-arrow-up-right"></i> Ver manifiesto
        </a>
      </div>`;
  }
  // 5) Limpiar el número de documento y enfocar para tipear el siguiente
  const nudoInput = document.getElementById('docNudo');
  if (nudoInput){ nudoInput.value = ''; setTimeout(() => nudoInput.focus(), 200); }
  // 6) Limpiar inputs del cliente que el operador pudo haber tocado
  ['cli-tel','cli-email','cli-dir','cli-comuna','cli-bultos','cli-notas',
   'cli-dir-lat','cli-dir-lng','cli-dir-place-id','cli-region']
    .forEach(id => { const el = document.getElementById(id); if (el) el.value = ''; });
  if (document.getElementById('cli-dir')) document.getElementById('cli-dir').dataset.validatedValue = '';
  // 7) Scroll suave al input para que se vea bien en mobile
  if (nudoInput) nudoInput.scrollIntoView({ behavior: 'smooth', block: 'center' });
}
function imprimir()      { window.print(); }
function exportarExcel() { if(!_docData) return; alert('Generando Excel…'); }
function exportarPdf()   { if(!_docData) return; alert('Generando PDF…'); }

// ════════════════════════════════════════════════════════════
//  HELPERS UI
// ════════════════════════════════════════════════════════════
function show(id){ const e=document.getElementById(id); if(e) e.style.display=''; }
function hide(id){ const e=document.getElementById(id); if(e) e.style.display='none'; }
function showAlert(id,msg){ const e=document.getElementById(id); if(e){e.textContent=msg;e.style.display='';} }
function hideAlert(id){ const e=document.getElementById(id); if(e) e.style.display='none'; }
function setText(id,val){ const e=document.getElementById(id); if(e) e.textContent=val; }
function setInput(id,val){ const e=document.getElementById(id); if(e) e.value=val; }
function setBuscarLoading(on){
  document.getElementById('btnBuscar').disabled = on;
  document.getElementById('spinBuscar').style.display = on?'':'none';
  document.getElementById('icoBuscar').style.display  = on?'none':'';
  document.getElementById('txtBuscar').textContent    = on?'Consultando ERP…':'Buscar en ERP';
}

/* ════════════════════════════════════════════════════════════════════
   STEPPER GUÍA + UX espectacular del módulo Asignar y Cotizar
   Auto-detecta el estado del flujo y actualiza el stepper superior
   sin tocar la lógica del módulo (es 100% adicional).
   ════════════════════════════════════════════════════════════════════ */
(function(){
  'use strict';

  const STEPS_EL = {
    1: document.querySelector('.ac-step[data-step="1"]'),
    2: document.querySelector('.ac-step[data-step="2"]'),
    3: document.querySelector('.ac-step[data-step="3"]'),
  };
  const BARS_EL = {
    1: document.getElementById('acBar1'),
    2: document.getElementById('acBar2'),
  };
  let _currentStep = 1;
  let _completed = { 1:false, 2:false, 3:false };

  function _setStepperState(){
    [1,2,3].forEach(function(n){
      const el = STEPS_EL[n];
      if (!el) return;
      el.classList.remove('active','done');
      if (_completed[n]) el.classList.add('done');
      else if (n === _currentStep) el.classList.add('active');
    });
    if (BARS_EL[1]) BARS_EL[1].classList.toggle('done', _completed[1]);
    if (BARS_EL[2]) BARS_EL[2].classList.toggle('done', _completed[2]);
  }

  // Scroll suave al paso elegido (deja el stepper sticky visible)
  window.acStepGoTo = function(n){
    _currentStep = n;
    _setStepperState();
    const targetId = 'acSec' + n;
    const t = document.getElementById(targetId);
    if (!t) return;
    const rect = t.getBoundingClientRect();
    const y = window.scrollY + rect.top - 88;   // 88 = altura stepper sticky
    window.scrollTo({ top: y, behavior: 'smooth' });
  };

  // Detección reactiva: usa MutationObserver sobre los cards clave para
  // saber cuándo el flujo avanzó. No requiere modificar el JS existente.
  // Flags para mostrar toast solo cuando un paso pasa de false→true
  let _notified = { 1:false, 2:false, 3:false };

  function _refreshFlowState(){
    const cardDatos    = document.getElementById('cardDatos');
    const cardCouriers = document.getElementById('cardCouriers');
    const datosVisible = cardDatos    && cardDatos.style.display !== 'none';
    const courVisible  = cardCouriers && cardCouriers.style.display !== 'none';

    const wasCompleted = { ..._completed };

    // Paso 1 completo cuando cardDatos se muestra (doc encontrado)
    if (datosVisible !== _completed[1]) _completed[1] = datosVisible;

    // Paso 2 completo cuando hay teléfono + email + dirección no-placeholder.
    if (datosVisible){
      const tel   = (document.getElementById('cli-tel')   || {}).value || '';
      const email = (document.getElementById('cli-email') || {}).value || '';
      const dir   = (document.getElementById('cli-dir')   || {}).value || '';
      const validTel   = /^\+?56\s*9?\s*[0-9]{4}\s*[0-9]{4}$/.test(tel.replace(/\s+/g,'')) ||
                         /^\+?569\d{8}$/.test(tel.replace(/\s+/g,''));
      const validEmail = /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
      const validDir   = (dir.trim().length >= 6);
      _completed[2] = (validTel && validEmail && validDir);
    } else {
      _completed[2] = false;
    }
    _completed[3] = courVisible;

    // Toast cuando un paso se completó por primera vez
    [1,2,3].forEach(function(n){
      if (_completed[n] && !wasCompleted[n] && !_notified[n]) {
        _notified[n] = true;
        const labels = {
          1: '✓ Documento ERP encontrado',
          2: '✓ Contacto y dirección validados',
          3: '✓ Couriers cotizados',
        };
        if (window.ilusToast) ilusToast(labels[n], { type:'success', duration: 2400 });
      }
      if (!_completed[n] && wasCompleted[n]) _notified[n] = false;
    });

    if      (!_completed[1]) _currentStep = 1;
    else if (!_completed[2]) _currentStep = 2;
    else                     _currentStep = 3;
    _setStepperState();
    _updateFab();
  }

  // ── Botón flotante "Continuar al paso N" (FAB contextual) ─────
  function _ensureFab(){
    let fab = document.getElementById('acFab');
    if (fab) return fab;
    fab = document.createElement('button');
    fab.id = 'acFab';
    fab.type = 'button';
    fab.className = 'ac-fab';
    fab.innerHTML = '<span class="ac-fab-lbl">Continuar</span> <i class="bi bi-arrow-right-circle-fill"></i>';
    fab.addEventListener('click', function(){
      const n = parseInt(fab.dataset.target || '1', 10);
      acStepGoTo(n);
    });
    document.body.appendChild(fab);
    return fab;
  }
  function _updateFab(){
    // Mostrar el FAB solo si hay un próximo paso disponible y no estoy ahí
    let next = 0;
    if (_completed[1] && !_completed[2]) next = 2;
    else if (_completed[2] && !_completed[3]) next = 3;
    const fab = _ensureFab();
    if (!next) { fab.classList.remove('show'); return; }
    // No mostrarlo si la sección destino ya está visible en viewport
    const t = document.getElementById('acSec' + next);
    if (t) {
      const rect = t.getBoundingClientRect();
      const inView = rect.top < window.innerHeight * 0.65 && rect.bottom > 60;
      if (inView) { fab.classList.remove('show'); return; }
    }
    const labels = { 2: 'Validar contacto', 3: 'Cotizar couriers' };
    fab.dataset.target = String(next);
    fab.querySelector('.ac-fab-lbl').textContent = labels[next] || 'Continuar';
    fab.classList.add('show');
  }
  // Recalcular FAB on scroll (oculta cuando llegas)
  window.addEventListener('scroll', function(){
    if (window.__acFabScrollTm) cancelAnimationFrame(window.__acFabScrollTm);
    window.__acFabScrollTm = requestAnimationFrame(_updateFab);
  }, { passive: true });

  // Observa cambios de visibilidad/style en los cards clave
  const _obs = new MutationObserver(_refreshFlowState);
  ['cardDatos','cardCubaje','cardCouriers'].forEach(function(id){
    const el = document.getElementById(id);
    if (el) _obs.observe(el, { attributes:true, attributeFilter:['style'] });
  });

  // También recalcula al editar tel/email/dir (validación en vivo de paso 2)
  ['cli-tel','cli-email','cli-dir'].forEach(function(id){
    const el = document.getElementById(id);
    if (el) {
      el.addEventListener('input', _onFieldInput);
      el.addEventListener('blur',  _onFieldInput);
    }
  });

  function _onFieldInput(ev){
    const id = ev.target.id;
    const v  = (ev.target.value || '').trim();
    ev.target.classList.remove('is-ok','is-warn','is-err');
    let ok = false;
    if (id === 'cli-tel') {
      const clean = v.replace(/\s+/g,'');
      ok = /^\+?56\s*9?\s*[0-9]{4}\s*[0-9]{4}$/.test(clean) || /^\+?569\d{8}$/.test(clean);
      if (v && !ok) ev.target.classList.add('is-warn');
      else if (ok)  ev.target.classList.add('is-ok');
    } else if (id === 'cli-email') {
      ok = /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(v);
      if (v && !ok) ev.target.classList.add('is-warn');
      else if (ok)  ev.target.classList.add('is-ok');
    } else if (id === 'cli-dir') {
      ok = (v.length >= 6);
      if (v && !ok) ev.target.classList.add('is-warn');
      else if (ok)  ev.target.classList.add('is-ok');
    }
    _refreshFlowState();
  }

  // ── Atajos de teclado para velocidad ──────────────────────────
  // Enter en docNudo ya está manejado por el código existente (buscarDoc).
  // Agregamos: 1/2/3 con Alt para saltar entre pasos, Esc para limpiar.
  document.addEventListener('keydown', function(ev){
    // Solo si no estamos escribiendo en un input/textarea
    const tag = (ev.target.tagName || '').toLowerCase();
    const inField = (tag === 'input' || tag === 'textarea' || tag === 'select');
    if (ev.altKey && !inField) {
      if (ev.key === '1') { ev.preventDefault(); acStepGoTo(1); }
      if (ev.key === '2') { ev.preventDefault(); acStepGoTo(2); }
      if (ev.key === '3') { ev.preventDefault(); acStepGoTo(3); }
    }
    // Atajo "/" para enfocar la búsqueda (estilo Slack/GitHub)
    if (ev.key === '/' && !inField) {
      ev.preventDefault();
      const inp = document.getElementById('docNudo');
      if (inp) { inp.focus(); inp.select && inp.select(); }
    }
  });

  // ── Inicializa al cargar la página ────────────────────────────
  document.addEventListener('DOMContentLoaded', function(){
    _refreshFlowState();
    // Si ya hay un doc cargado (recarga con datos), avanza al stepper
    setTimeout(_refreshFlowState, 300);
  });

  // Exponemos un helper público para que el código existente pueda
  // forzar el refresh tras búsqueda/cotización si quiere (opcional).
  window.acRefreshStepper = _refreshFlowState;
})();

/* ════════════════════════════════════════════════════════════════════
   PREFETCH inteligente de couriers — al cargar la página, traemos la
   lista de couriers activos en background. Cuando llega paso 3, el
   usuario ya tiene la data en memoria (cero espera).
   ════════════════════════════════════════════════════════════════════ */
(function(){
  if (window.__acCouriersPrefetched) return;
  window.__acCouriersPrefetched = true;
  // Solo si la API existe y estamos en idle
  if (typeof window.requestIdleCallback !== 'function') return;
  window.requestIdleCallback(function(){
    try {
      fetch('/transporte/api/couriers/lista', {
        method: 'GET',
        headers: {'Accept': 'application/json'},
        cache: 'force-cache',
      }).then(function(r){
        if (!r.ok) return null;
        return r.json();
      }).then(function(data){
        if (data) window.__acCouriersCache = data;
      }).catch(function(){ /* silencio */ });
    } catch(e){}
  }, { timeout: 2500 });
})();

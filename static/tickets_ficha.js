let ticketActual = null;
let rpAdjuntos = [];   // [{id, url, nombre}] ya subidos, pendientes de enviar con el proximo mensaje
let rpModo = 'cliente'; // 'cliente' | 'interno'
let equiposCache = []; // último d.equipos cargado — para poblar el modal de garantía sin otro fetch
let garEquipoActual = null; // equipo en edición dentro del modal de garantía

// ══════════════ Alto dinamico del hilo Y del editor de Respuestas ══════════════
// Daniel 2026-07-12: primero pidio que el composer nunca quedara debajo
// del pliegue (sin scrollear la pagina) -- eso llevo a encoger el hilo
// bastante en pantallas chicas. Corrigio: "la caja de mensaje esta
// extremadamente pequeña... tiene que ser una mediacion entre los dos...
// la caja de texto [el editor] es mas grande que el visualizador de
// mensajes [el hilo]" -- el hilo es el contenido PRINCIPAL (como en
// Triple A, "para ahorrar espacio") y debe ser >= el editor, no al
// reves. Nueva prioridad: el editor usa un alto COMODO PERO COMPACTO
// por defecto (150px, no necesita mas para escribir una respuesta
// normal) y el hilo se lleva TODO el resto del espacio disponible --
// el editor solo se encoge (hasta un piso de 70px) si ni asi alcanza a
// darle al hilo su propio piso minimo (160px). El techo del editor
// (220px) evita que crezca de mas aunque sobre espacio en un monitor
// grande -- ese espacio de mas se lo lleva el hilo, no el editor.
// ACTUALIZACION 2026-07-14 (Daniel): "con el scroll quiero consumirme todo
// el header y que se vea el panel completo, aprovechar los espacios para
// agrandar la caja de mensajes". Antes se dimensionaba para que el composer
// quedara visible SIN scrollear la pagina (con el hero ocupando pantalla) --
// eso dejaba el hilo chico. Ahora se dimensiona como si el hero YA se
// hubiera scrolleado fuera de vista: la pagina scrollea lo justo (el alto
// del hero, sin sticky, de forma natural) y con el hero consumido el hilo +
// composer llenan practicamente toda la pantalla.
function tkHeroAltoConsumible(){
  // Cuanto de la parte superior de la pagina (hero + su margen inferior)
  // desaparece del viewport al scrollear hacia abajo.
  const hero = document.getElementById('tkHero');
  if(!hero) return 0;
  const r = hero.getBoundingClientRect();
  let mb = 16;
  try{ mb = parseFloat(getComputedStyle(hero).marginBottom) || 0; }catch(e){}
  // Borde inferior del hero (con margen) medido en el viewport: si aun es
  // positivo, ese alto todavia se puede "consumir" scrolleando la pagina.
  return Math.max(0, r.bottom + mb);
}
function ajustarAltoHilo(){
  const paneConv = document.getElementById('pane-conv');
  if(!paneConv || !paneConv.classList.contains('active')) return; // oculto: rects en 0, no calcular
  const th = document.getElementById('rpThread');
  const composer = document.querySelector('.rp-composer');
  const qlContainer = document.querySelector('.rp-editor-wrap .ql-container');
  if(!th || !composer || !qlContainer) return;

  // 2026-07-15 (Daniel): "en la parte responsiva se ve muy chiquitita la caja
  // de mensajes... si recuperamos el mayor espacio posible quiero darselo a la
  // caja de los mensajes para que sea comodo. La caja de TEXTO puede ir hasta
  // mas chica si es necesario... para que la caja de los MENSAJES es la que
  // predomina". Estas constantes eran FIJAS y estaban calibradas para un
  // monitor: en un telefono, 180px de editor + el "chrome" del composer
  // (mode-tabs + toolbar de Quill + barra de adjuntos/enviar) se comian casi
  // toda la pantalla y al hilo le quedaban migajas. Ahora el default del
  // editor depende del viewport y TODO el espacio recuperado va al hilo.
  // Mismo breakpoint mobile del proyecto (768px), MAS el alto: un telefono en
  // horizontal mide 812x375 -- por ancho pasaria por "escritorio" y se llevaria
  // el editor de 180px en una pantalla de 375px de alto, justo el caso donde el
  // hilo mas necesita el espacio. Con esto, rotar el telefono recalcula bien en
  // ambos sentidos (la funcion ya se re-ejecuta en resize).
  const esMovil = window.innerWidth <= 768 || window.innerHeight <= 500;
  const AIRE_INFERIOR = 24;    // respiro entre el composer y el borde de la ventana
  // Alto comodo del editor. Escritorio: 180 (Daniel 2026-07-14: "mas altura
  // minima"). Mobile: 110 -- suficiente para ~4 lineas y para ver lo que
  // escribes; si necesitas mas, el editor ya tiene su propio scroll interno.
  const EDITOR_DEFAULT = esMovil ? 110 : 180;
  // Piso absoluto del editor si el espacio es muy chico (ej. telefono en
  // horizontal): en mobile baja a 70px -- el hilo manda.
  const EDITOR_MIN = esMovil ? 70 : 90;
  const HILO_MIN = 200;        // piso del hilo (el area PRINCIPAL)

  // Tope del hilo "efectivo": donde quedara el hilo cuando el hero ya se
  // haya scrolleado fuera de vista (tabs + cabecera del card solamente).
  const threadTop = th.getBoundingClientRect().top - tkHeroAltoConsumible();
  // "Chrome" del composer que NO es el editor (mode-tabs + headers De/Para/CC
  // si estan visibles + barra de adjuntos/enviar) -- fijo independientemente
  // del alto que le demos al editor.
  const otrosComposer = composer.getBoundingClientRect().height - qlContainer.getBoundingClientRect().height;
  const totalDisponible = window.innerHeight - threadTop - AIRE_INFERIOR;
  const restante = totalDisponible - otrosComposer; // a repartir entre hilo + editor

  let alturaEditor = EDITOR_DEFAULT;
  let alturaHilo = restante - alturaEditor;
  if(alturaHilo < HILO_MIN){
    // no alcanza el piso del hilo con el editor en su default -> encoger
    // el editor primero (el hilo manda), hasta su propio piso minimo.
    alturaEditor = Math.max(EDITOR_MIN, restante - HILO_MIN);
    alturaHilo = Math.max(HILO_MIN, restante - alturaEditor);
  }
  qlContainer.style.maxHeight = alturaEditor + 'px';
  // El 180 aca era una copia hardcodeada de EDITOR_DEFAULT -- en mobile habria
  // dejado un min-height de escritorio peleando contra el max-height nuevo.
  // (alturaEditor nunca supera EDITOR_DEFAULT, asi que esto equivale a fijar
  // el alto del editor; se deja el Math.min por seguridad si alguien cambia
  // el reparto de arriba.)
  qlContainer.style.minHeight = Math.min(alturaEditor, EDITOR_DEFAULT) + 'px';
  // 2026-07-15 (Daniel): "no la hagas dinamica, dejala siempre lo mas
  // estirada posible" -- antes solo se fijaba max-height + un min-height
  // chico (HILO_MIN=200px de piso), asi que con pocos mensajes el div
  // (sin `height` fijo, block normal) se encogia al alto del contenido
  // real en vez de ocupar el espacio maximo disponible. Ahora es un alto
  // FIJO (height, no min/max): el hilo siempre mide lo mismo, tenga 1
  // mensaje o 50 -- si esta casi vacio simplemente queda con espacio en
  // blanco abajo, que es exactamente lo que Daniel pidio.
  th.style.height = alturaHilo + 'px';
  th.style.maxHeight = alturaHilo + 'px';
  th.style.minHeight = alturaHilo + 'px';
}
let _ajustarAltoHiloTimer = null;
function ajustarAltoHiloDebounced(){
  clearTimeout(_ajustarAltoHiloTimer);
  _ajustarAltoHiloTimer = setTimeout(ajustarAltoHilo, 80);
}
window.addEventListener('resize', ajustarAltoHiloDebounced);

// Escapa TAMBIÉN comillas (esc se usa en atributos href/onclick, no solo texto) → anti-XSS.
function esc(s){ return (s==null?'':String(s))
  .replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')
  .replace(/"/g,'&quot;').replace(/'/g,'&#39;'); }
// El servidor ya envía las fechas formateadas en hora Chile (Regla #6).
function fmtFechaHora(s){ return s || ''; }
// 2026-07-14 (Daniel): "dice visto por Daniel Aguilar y lo dice en inglés,
// con una fecha que no corresponde -- tiene que poner la fecha de Santiago".
// Causa: visto_at NO pasa por _fmt_row en el backend (no está en dt_keys),
// así que Flask/jsonify lo serializa crudo en formato RFC-1123 EN INGLÉS y
// EN UTC/GMT ("Mon, 13 Jul 2026 22:10:05 GMT"). Este helper lo convierte a
// hora de Santiago en dd/mm/aaaa HH:MM. Es defensivo: si el backend algún
// día lo manda ya formateado (dd/mm/aaaa …), se usa tal cual.
function fmtFechaVisto(s){
  if(s == null) return '';
  const str = String(s).trim();
  if(!str || /^\d{2}\/\d{2}\/\d{4}/.test(str)) return str; // ya viene en hora Chile
  const d = new Date(str);
  if(isNaN(d.getTime())) return str;
  try{
    const partes = new Intl.DateTimeFormat('es-CL', {
      timeZone:'America/Santiago', day:'2-digit', month:'2-digit',
      year:'numeric', hour:'2-digit', minute:'2-digit', hour12:false,
    }).formatToParts(d);
    const g = {};
    partes.forEach(function(p){ g[p.type] = p.value; });
    return g.day+'/'+g.month+'/'+g.year+' '+g.hour+':'+g.minute;
  }catch(e){ return str; }
}
// href seguro: solo rutas internas /f/ o http(s) — evita esquemas peligrosos.
function safeUrl(u){ u=String(u||''); return (u.startsWith('/') || /^https?:/i.test(u)) ? u : '#'; }
// 2026-07-12 (Daniel — tickets 590/591): algunos adjuntos quedaron con
// mime_type generico/incorrecto en la BD (el navegador/SO no siempre manda
// el Content-Type real al subir, ej. fotos desde el picker de archivos del
// celular) -- se agrega fallback por extension del nombre para que esos
// adjuntos YA guardados tambien se vean como imagen sin migrar datos.
const _EXT_IMAGEN = /\.(jpe?g|png|gif|webp|heic|bmp)$/i;
function esImagen(mime, nombre){
  return /^image\//.test(mime||'') || _EXT_IMAGEN.test(String(nombre||''));
}

document.querySelectorAll('.tk-tab').forEach(function(tab){
  tab.addEventListener('click', function(){
    document.querySelectorAll('.tk-tab').forEach(t=>t.classList.remove('active'));
    document.querySelectorAll('.tk-pane').forEach(p=>p.classList.remove('active'));
    tab.classList.add('active');
    document.getElementById('pane-'+tab.dataset.pane).classList.add('active');
    // Respuestas: renderThread() ya intenta bajar el scroll al fondo, pero si
    // el pane estaba oculto (display:none) en ese momento, scrollHeight leia 0
    // y el scroll quedaba en 0 -- se veian los mensajes mas antiguos primero
    // (Daniel 2026-07-12: "esta invertida"). Al mostrar recien el pane, con el
    // layout real ya calculado (requestAnimationFrame), reintentar el scroll.
    if(tab.dataset.pane === 'conv'){
      requestAnimationFrame(function(){
        ajustarAltoHilo();
        const th = document.getElementById('rpThread');
        if(th) th.scrollTop = th.scrollHeight;
        // 2026-07-14 (Daniel): al entrar a Respuestas, consumir el header
        // scrolleando la pagina de forma natural (sin sticky) para que el
        // panel de conversacion ocupe practicamente toda la pantalla.
        const consumir = tkHeroAltoConsumible();
        if(consumir > 0){
          try{ window.scrollTo({top: window.scrollY + consumir, behavior:'smooth'}); }
          catch(e){ window.scrollTo(0, window.scrollY + consumir); }
        }
      });
    }
  });
});

// ══════════════ Chips de correo con validación en vivo (2026-07-12) ══════════════
// Daniel: "si el correo está bien formado que quede seleccionado y
// verificado, con punto y coma... para no obligarme a enviar algo que no
// va a llegar". Solo valida FORMATO (sintaxis) -- no hay forma confiable
// de verificar que un correo realmente EXISTE sin enviarle algo (un
// handshake SMTP de verificación es lento, muchos servidores lo bloquean,
// y da falsos negativos). El formato ya evita el 90% de los errores reales
// (typos, faltó el @, faltó el dominio).
const EMAIL_RE = /^[^\s@,;]+@[^\s@,;]+\.[^\s@,;]+$/;
function _emailChipsInit(boxId, hiddenId){
  const box = document.getElementById(boxId);
  const hidden = document.getElementById(hiddenId);
  // 2026-08-10: este archivo ahora también se carga en modo "cliente"
  // (mantenciones/ficha.html, TID=null -- ver _TKOT_MODO_CLIENTE), donde el
  // composer de respuestas del ticket (#rpToBox/#rpCcBox) no existe en el
  // DOM. Sin este guard, box.querySelector() de la línea siguiente tira
  // TypeError y aborta la ejecución de TODO el script (incluida la lógica
  // del modal #modalGenerarOT que sí se necesita en esa página).
  if (!box || !hidden) return { setValue: function(){}, hayInvalidos: function(){ return false; } };
  const input = box.querySelector('.email-chip-input');

  function valoresValidos(){
    return Array.from(box.querySelectorAll('.email-chip:not(.invalido)'))
      .map(c => c.dataset.email);
  }
  function sync(){ hidden.value = valoresValidos().join('; '); }
  function hayInvalidos(){ return !!box.querySelector('.email-chip.invalido'); }

  function agregarChip(correo){
    correo = correo.trim().replace(/[;,]+$/, '');
    if (!correo) return;
    const valido = EMAIL_RE.test(correo);
    const chip = document.createElement('span');
    chip.className = 'email-chip' + (valido ? '' : ' invalido');
    chip.dataset.email = correo;
    chip.innerHTML = '<i class="bi ' + (valido ? 'bi-check-circle-fill' : 'bi-exclamation-triangle-fill')
      + '" title="' + (valido ? 'Formato válido' : 'Formato inválido — revísalo') + '"></i>'
      + '<span>' + esc(correo) + '</span><span class="x" title="Quitar">✕</span>';
    chip.querySelector('.x').addEventListener('click', function(){ chip.remove(); sync(); });
    box.insertBefore(chip, input);
    sync();
  }

  input.addEventListener('keydown', function(e){
    if ([',', ';', 'Enter', 'Tab'].includes(e.key) && input.value.trim()){
      e.preventDefault();
      agregarChip(input.value);
      input.value = '';
    } else if (e.key === 'Backspace' && !input.value){
      const last = box.querySelector('.email-chip:last-of-type');
      if (last){ last.remove(); sync(); }
    }
  });
  input.addEventListener('blur', function(){
    if (input.value.trim()){ agregarChip(input.value); input.value = ''; }
  });
  box.addEventListener('click', function(e){ if (e.target === box) input.focus(); });

  // API pública para prefill programático (ej. autocompletar el correo del ticket).
  return {
    setValue: function(valor){
      box.querySelectorAll('.email-chip').forEach(c => c.remove());
      String(valor || '').split(/[;,]/).forEach(c => { if (c.trim()) agregarChip(c); });
    },
    hayInvalidos: hayInvalidos,
  };
}
const rpToChips = _emailChipsInit('rpToBox', 'rpTo');
const rpCcChips = _emailChipsInit('rpCcBox', 'rpCc');

// ══════════════ Editor Quill (caja de texto "potente") ══════════════
// 2026-08-10: en "modo cliente" (mantenciones/ficha.html) #rpEditor no
// existe -- el composer de Respuestas es exclusivo del ticket. `new
// Quill(...)` con un selector que no matchea NINGÚN elemento tira un error
// síncrono ("Invalid Quill container") que abortaría el resto del script
// (incluida la lógica del modal #modalGenerarOT). rpQuill queda `null` en
// ese caso -- todo lo que lo usa vive dentro de funciones que solo se
// disparan desde botones del composer, inexistentes en modo cliente.
let rpQuill = null;
if (document.getElementById('rpEditor')){
  rpQuill = new Quill('#rpEditor', {
    theme: 'snow',
    placeholder: 'Escribe tu respuesta…',
    // 2026-07-12 (Daniel): "editable, con muchas herramientas, y elegante" --
    // toolbar ampliada (antes solo bold/italic/underline/listas/link/clean).
    modules: { toolbar: [
      [{header: [false, 2, 3]}],
      ['bold','italic','underline','strike'],
      [{color: []}, {background: []}],
      [{list:'ordered'},{list:'bullet'}],
      ['blockquote'],
      ['link','clean'],
    ] },
  });
}
// 2026-07-18 (Daniel, bug real del Reintentar del micrófono): si el usuario
// tuvo que recargar la página para que Chrome aplique el permiso de
// micrófono recién activado, el borrador que estaba escribiendo se guarda
// en sessionStorage antes del reload y se restaura aquí solo.
try{
  var _draftKey = 'rpDraft:' + TID;
  var _draft = sessionStorage.getItem(_draftKey);
  if (_draft){
    sessionStorage.removeItem(_draftKey);
    rpQuill.clipboard.dangerouslyPasteHTML(_draft);
    if (window.ilusToast) ilusToast('Borrador restaurado', { type: 'info' });
  }
}catch(e){}
// 2026-07-13 (Daniel, URGENTE): "en las herramientas de la caja de texto
// quiero que le dejes un placeholder para saber que es, porque mucha gente
// va a tener curiosidad" -- Quill (tema snow) no trae tooltips nativos para
// varios de estos controles nuevos (header/color/background/blockquote).
// Se agregan atributos title a mano sobre los botones que Quill genera.
(function(){
  const tb = document.querySelector('.rp-editor-wrap .ql-toolbar');
  if(!tb) return;
  const titulos = {
    '.ql-header': 'Encabezado',
    '.ql-bold': 'Negrita',
    '.ql-italic': 'Cursiva',
    '.ql-underline': 'Subrayado',
    '.ql-strike': 'Tachado',
    '.ql-color': 'Color de texto',
    '.ql-background': 'Color de resaltado',
    '.ql-list[value="ordered"]': 'Lista numerada',
    '.ql-list[value="bullet"]': 'Lista con viñetas',
    '.ql-blockquote': 'Cita',
    '.ql-link': 'Insertar enlace',
    '.ql-clean': 'Borrar formato',
  };
  Object.keys(titulos).forEach(function(sel){
    tb.querySelectorAll(sel).forEach(function(el){ el.setAttribute('title', titulos[sel]); });
  });
})();

// ══════════════ Dictado por voz — Google Cloud Speech-to-Text (2026-07-12) ══
// Daniel: el dictado nativo del navegador (SpeechRecognition) daba
// "not-allowed" en computador de forma PERSISTENTE incluso permitiendo el
// micrófono -- Chrome trata el permiso de reconocimiento de voz distinto
// al de micrófono normal, y una vez denegado no vuelve a preguntar solo.
// Reemplazo: grabar con MediaRecorder (permiso de micrófono ESTÁNDAR,
// mucho más confiable) y transcribir en el servidor. En Safari/iPhone
// (sin soporte de audio/webm) se mantiene el dictado nativo, que Daniel
// confirmó que YA funciona ahí (solo algo lento).
(function(){
  const btnDictar = document.getElementById('rpBtnDictar');
  if (!btnDictar) return;

  // ── Capa premium de permisos (2026-07-14) ─────────────────────────────
  // Daniel: "si pide permiso, que lo derive". Tres estados posibles:
  //   granted → grabar directo, cero fricción.
  //   prompt  → getUserMedia dispara el diálogo NATIVO del navegador (esa
  //             es la derivación); si acepta, la grabación parte en el
  //             mismo gesto.
  //   denied  → getUserMedia fallaría EN SILENCIO (el navegador ya no
  //             pregunta). En vez de un toast seco, guía visual paso a
  //             paso con Reintentar + auto-detección del cambio.

  async function consultarPermisoMic(){
    // Devuelve el PermissionStatus o null si la API no está disponible
    // (Safari viejo no soporta {name:'microphone'} y lanza TypeError).
    try{
      if (!navigator.permissions || !navigator.permissions.query) return null;
      return await navigator.permissions.query({ name: 'microphone' });
    }catch(e){ return null; }
  }

  function actualizarBadgeMic(state){
    // Puntito rojo sobre el botón cuando el permiso está denegado, para
    // que se entienda visualmente que requiere atención.
    btnDictar.classList.toggle('rp-mic-denegado', state === 'denied');
  }

  // Badge inicial + auto-actualización si el usuario cambia el permiso.
  consultarPermisoMic().then(function(st){
    if (!st) return;
    actualizarBadgeMic(st.state);
    try{ st.addEventListener('change', function(){ actualizarBadgeMic(st.state); }); }catch(e){}
  });

  let _micModalAbierto = false;
  function abrirModalMicBloqueado(onListo){
    // onListo() se ejecuta cuando el permiso ya no está denegado
    // (Reintentar exitoso o auto-detección del cambio).
    if (_micModalAbierto) return;
    _micModalAbierto = true;

    const ov = document.createElement('div');
    ov.className = 'rp-mic-overlay';
    ov.innerHTML =
        '<div class="rp-mic-modal" role="dialog" aria-modal="true" aria-labelledby="rpMicModalTitulo">'
      +   '<h5 id="rpMicModalTitulo" style="font-weight:800;margin:0;color:#0f172a;">Activa tu micrófono</h5>'
      +   '<div style="font-size:.85rem;color:#64748b;margin-top:4px;">El navegador tiene bloqueado el micrófono para este sitio. Habilitarlo toma 10 segundos:</div>'
      +   '<div class="rp-mic-illus">'
      +     '<span class="rp-mic-illus-icon off"><i class="bi bi-mic-mute"></i></span>'
      +     '<i class="bi bi-arrow-right"></i>'
      +     '<span class="rp-mic-illus-icon on"><i class="bi bi-mic-fill"></i></span>'
      +   '</div>'
      +   '<div class="rp-mic-step"><span class="rp-mic-step-num">1</span>'
      +     '<span class="rp-mic-step-txt">Haz clic en el ícono de <strong>candado</strong> o <strong>ajustes</strong> junto a la dirección del sitio (arriba a la izquierda).</span></div>'
      +   '<div class="rp-mic-step"><span class="rp-mic-step-num">2</span>'
      +     '<span class="rp-mic-step-txt">Activa el permiso de <strong>Micrófono</strong>.</span></div>'
      +   '<div class="rp-mic-step"><span class="rp-mic-step-num">3</span>'
      +     '<span class="rp-mic-step-txt">Presiona <strong>Reintentar</strong> aquí abajo.</span></div>'
      +   '<div class="rp-mic-ok"><i class="bi bi-check-circle-fill"></i> Micrófono habilitado — iniciando dictado…</div>'
      +   '<div class="rp-mic-still" style="display:none;background:#fff8e1;border:1px solid #fde68a;'
      +   'color:#78350f;border-radius:10px;padding:10px 12px;font-size:.8rem;margin-top:12px;">'
      +   '<i class="bi bi-info-circle-fill me-1"></i>'
      +   'Aun aparece bloqueado. Si ya activaste el micrófono en el candado, '
      +   '<strong>Chrome necesita recargar la página</strong> para aplicar el cambio. '
      +   'Tu respuesta escrita se guardará y se restaurará sola.'
      +   '<div style="margin-top:8px;text-align:right;">'
      +   '<button type="button" class="btn btn-sm btn-outline-secondary rp-mic-recargar">'
      +   '<i class="bi bi-arrow-clockwise me-1"></i>Recargar página</button></div></div>'
      +   '<div style="display:flex;gap:8px;justify-content:flex-end;margin-top:14px;">'
      +     '<button type="button" class="btn btn-outline-secondary rp-mic-cancelar">Cancelar</button>'
      +     '<button type="button" class="btn btn-ilus rp-mic-reintentar"><i class="bi bi-arrow-clockwise me-1"></i>Reintentar</button>'
      +   '</div>'
      + '</div>';
    document.body.appendChild(ov);
    requestAnimationFrame(function(){ ov.classList.add('show'); });

    let statusVivo = null, autoLanzado = false;

    function cerrar(){
      if (statusVivo){ try{ statusVivo.removeEventListener('change', onCambio); }catch(e){} }
      ov.classList.remove('show');
      setTimeout(function(){ ov.remove(); _micModalAbierto = false; }, 220);
    }

    function habilitadoYPartir(){
      // Check verde + partir la grabación (lo más fluido: sin pasos extra).
      if (autoLanzado) return;
      autoLanzado = true;
      actualizarBadgeMic('granted');
      ov.querySelector('.rp-mic-ok').classList.add('show');
      setTimeout(function(){ cerrar(); onListo(); }, 700);
    }

    function mostrarAunBloqueado(){
      // 2026-07-18 (Daniel, bug real): Chrome puede seguir reportando el
      // permiso como bloqueado en la Permissions API hasta que la página
      // se recarga, aunque el usuario ya lo haya activado en el candado.
      // getUserMedia (probado en comenzarGrabacion) es la fuente de verdad;
      // si igual falla, ofrecemos la puerta de escape de recargar.
      const st = ov.querySelector('.rp-mic-still');
      if (st) st.style.display = '';
    }

    function onCambio(){
      if (statusVivo && statusVivo.state === 'granted') habilitadoYPartir();
    }

    // Auto-detección: si habilita el micrófono con el modal abierto, no
    // hace falta ni tocar Reintentar.
    consultarPermisoMic().then(function(st){
      if (!st) return;
      statusVivo = st;
      try{ st.addEventListener('change', onCambio); }catch(e){}
    });

    ov.querySelector('.rp-mic-cancelar').addEventListener('click', cerrar);
    ov.addEventListener('click', function(ev){ if (ev.target === ov) cerrar(); });
    ov.querySelector('.rp-mic-reintentar').addEventListener('click', async function(){
      const btnR = this;
      if (btnR.disabled) return;
      btnR.disabled = true;
      const prev = btnR.innerHTML;
      btnR.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Probando...';
      try{
        // Intento REAL en el mismo gesto del usuario: la Permissions API puede
        // seguir reportando 'denied' hasta recargar (Chrome), aunque el permiso
        // ya esté activado. getUserMedia es la fuente de verdad.
        const s = await navigator.mediaDevices.getUserMedia({ audio: true });
        s.getTracks().forEach(function(t){ t.stop(); });   // solo era una prueba
        habilitadoYPartir();
      }catch(e){
        mostrarAunBloqueado();
      }finally{
        btnR.disabled = false;
        btnR.innerHTML = prev;
      }
    });
    ov.querySelector('.rp-mic-recargar').addEventListener('click', function(){
      // Preservar el borrador del editor antes de recargar (no perder lo escrito)
      try{
        if (typeof rpQuill !== 'undefined' && rpQuill && rpQuill.getLength() > 1){
          sessionStorage.setItem('rpDraft:' + TID, rpQuill.root.innerHTML);
        }
      }catch(e){}
      location.reload();
    });
  }

  const MR_OK = window.MediaRecorder
    && navigator.mediaDevices && navigator.mediaDevices.getUserMedia
    && (MediaRecorder.isTypeSupported('audio/webm;codecs=opus') || MediaRecorder.isTypeSupported('audio/webm'));

  if (MR_OK){
    let mediaRecorder = null, chunks = [], grabando = false, streamActual = null;
    const mimeType = MediaRecorder.isTypeSupported('audio/webm;codecs=opus') ? 'audio/webm;codecs=opus' : 'audio/webm';

    function detenerUI(){
      grabando = false;
      btnDictar.classList.remove('rp-dictando');
      btnDictar.title = 'Dictar mensaje por voz';
    }

    async function transcribirYInsertar(blob){
      btnDictar.disabled = true;
      const original = btnDictar.innerHTML;
      btnDictar.innerHTML = '<span class="spinner-border spinner-border-sm"></span>';
      try{
        const fd = new FormData();
        fd.append('audio', blob, 'dictado.webm');
        const r = await fetch('/tickets/api/transcribir', { method: 'POST', body: fd });
        // 2026-07-14 (Daniel: "dictar por voz no está funcionando"): antes,
        // cualquier respuesta NO-JSON (sesión expirada -> redirect a login,
        // 413 del proxy, error HTML del servidor) reventaba en r.json() y
        // caía al genérico "Error de red" sin pista alguna. Se diferencia el
        // caso para poder diagnosticar de verdad.
        let d = null;
        try{ d = await r.json(); }catch(e){ d = null; }
        if (!d){
          ilusToast('El servidor no respondió como se esperaba (HTTP '+r.status+'). '
            + (r.status === 401 || r.status === 403 || (r.redirected && /login/i.test(r.url||''))
               ? 'Tu sesión pudo haber expirado — recarga la página.' : 'Intenta de nuevo.'), { type: 'error' });
          return;
        }
        if (!d.ok){ ilusToast(d.error || 'No se pudo transcribir', { type: 'error' }); return; }
        if (!d.texto){ ilusToast('No se detectó voz en la grabación.', { type: 'info' }); return; }
        const range = rpQuill.getSelection(true) || { index: rpQuill.getLength(), length: 0 };
        rpQuill.insertText(range.index, d.texto + ' ', 'user');
        rpQuill.setSelection(range.index + d.texto.length + 1);
      }catch(e){ ilusToast('Error de red al transcribir', { type: 'error' }); }
      finally{ btnDictar.disabled = false; btnDictar.innerHTML = original; }
    }

    let _dictadoTimer = null; // auto-stop: recognize síncrono de Google acepta ~60s máx

    async function comenzarGrabacion(){
      if (grabando) return;
      try{
        streamActual = await navigator.mediaDevices.getUserMedia({ audio: true });
      }catch(e){
        const nombre = (e && e.name) || '';
        if (nombre === 'NotAllowedError' || nombre === 'PermissionDeniedError' || nombre === 'SecurityError'){
          // Permiso denegado (o la Permissions API no estaba disponible
          // para detectarlo antes): guía visual en vez de toast seco.
          actualizarBadgeMic('denied');
          abrirModalMicBloqueado(comenzarGrabacion);
        } else if (nombre === 'NotFoundError' || nombre === 'DevicesNotFoundError'){
          ilusToast('No se encontró ningún micrófono conectado en este equipo.', { type: 'error' });
        } else {
          ilusToast('No se pudo acceder al micrófono' + (nombre ? ' (' + nombre + ')' : '') + ' — intenta de nuevo.', { type: 'error' });
        }
        return;
      }
      actualizarBadgeMic('granted');
      chunks = [];
      try{
        mediaRecorder = new MediaRecorder(streamActual, { mimeType: mimeType });
      }catch(e){
        streamActual.getTracks().forEach(function(t){ t.stop(); });
        ilusToast('No se pudo iniciar la grabación en este navegador.', { type: 'error' });
        return;
      }
      mediaRecorder.ondataavailable = function(ev){ if (ev.data && ev.data.size) chunks.push(ev.data); };
      mediaRecorder.onerror = function(){
        clearTimeout(_dictadoTimer);
        try{ streamActual.getTracks().forEach(function(t){ t.stop(); }); }catch(e){}
        detenerUI();
        ilusToast('La grabación falló — intenta de nuevo.', { type: 'error' });
      };
      mediaRecorder.onstop = function(){
        clearTimeout(_dictadoTimer);
        streamActual.getTracks().forEach(function(t){ t.stop(); });
        detenerUI();
        const blob = new Blob(chunks, { type: mimeType });
        if (blob.size > 500) transcribirYInsertar(blob);
        else ilusToast('La grabación quedó vacía — habla y luego detén con el mismo botón.', { type: 'info' });
      };
      // timeslice 250ms: junta datos DURANTE la grabación (más robusto que
      // un único chunk al final si la pestaña pierde foco o algo interrumpe).
      mediaRecorder.start(250);
      grabando = true;
      btnDictar.classList.add('rp-dictando');
      btnDictar.title = 'Grabando… clic para detener y transcribir';
      // El endpoint usa recognize() síncrono de Google (límite ~60s de audio):
      // se corta solo a los 55s para que la transcripción no falle por largo.
      _dictadoTimer = setTimeout(function(){
        if (grabando && mediaRecorder && mediaRecorder.state === 'recording'){
          ilusToast('Se alcanzó el máximo de ~1 minuto de dictado — transcribiendo lo grabado.', { type: 'info' });
          mediaRecorder.stop();
        }
      }, 55000);
    }

    btnDictar.addEventListener('click', async function(){
      if (grabando){ mediaRecorder && mediaRecorder.state === 'recording' && mediaRecorder.stop(); return; }
      const st = await consultarPermisoMic();
      if (st && st.state === 'denied'){
        // getUserMedia fallaría sin mostrar NADA: mejor la guía visual.
        abrirModalMicBloqueado(comenzarGrabacion);
        return;
      }
      // granted → parte directo. prompt → diálogo nativo del navegador en
      // este mismo gesto. null (sin Permissions API) → getUserMedia decide.
      comenzarGrabacion();
    });
  } else {
    // Fallback: Web Speech API nativa (Safari/iPhone -- confirmado que
    // funciona, solo mas lento que el motor de Google del lado servidor).
    const SR = window.SpeechRecognition || window.webkitSpeechRecognition;
    if (!SR){ btnDictar.style.display = 'none'; return; }
    let rec = null, escuchando = false;
    function detener(){
      escuchando = false;
      btnDictar.classList.remove('rp-dictando');
      btnDictar.title = 'Dictar mensaje por voz';
    }
    function iniciarReconocimiento(){
      rec = new SR();
      rec.lang = 'es-CL';
      rec.continuous = true;
      rec.interimResults = false;
      rec.onstart = function(){
        escuchando = true;
        actualizarBadgeMic('granted');
        btnDictar.classList.add('rp-dictando');
        btnDictar.title = 'Escuchando… clic para detener';
      };
      rec.onresult = function(ev){
        let texto = '';
        for (let i = ev.resultIndex; i < ev.results.length; i++){
          if (ev.results[i].isFinal) texto += ev.results[i][0].transcript;
        }
        texto = texto.trim();
        if (!texto) return;
        const range = rpQuill.getSelection(true) || { index: rpQuill.getLength(), length: 0 };
        rpQuill.insertText(range.index, texto + ' ', 'user');
        rpQuill.setSelection(range.index + texto.length + 1);
      };
      rec.onerror = function(ev){
        if (ev.error === 'not-allowed' || ev.error === 'service-not-allowed'){
          // Misma guía visual que en el flujo MediaRecorder (2026-07-14).
          actualizarBadgeMic('denied');
          abrirModalMicBloqueado(iniciarReconocimiento);
        } else if (ev.error !== 'no-speech' && ev.error !== 'aborted') {
          ilusToast('No se pudo escuchar: ' + (ev.error || 'error desconocido'), { type: 'error' });
        }
        detener();
      };
      rec.onend = detener;
      rec.start();
    }
    btnDictar.addEventListener('click', function(){
      if (escuchando){ rec && rec.stop(); return; }
      iniciarReconocimiento();
    });
  }
})();

// ══════════════ Traductor (Google Cloud Translation) 2026-07-12 ══════════════
(function(){
  // 2026-08-10: sin guard, este IIFE tira TypeError en modo cliente
  // (#rpTradIdiomaMenu no existe fuera del composer de un ticket) y aborta
  // el resto del script -- mismo patrón que los demás IIFE de este archivo.
  if (!document.getElementById('rpTradIdiomaMenu')) return;
  let rpTradLang = 'en';
  document.getElementById('rpTradIdiomaMenu').addEventListener('click', function(ev){
    const a = ev.target.closest('a[data-lang]');
    if (!a) return;
    ev.preventDefault();
    rpTradLang = a.dataset.lang;
    document.getElementById('rpTradIdiomaLabel').textContent = a.dataset.nombre;
  });

  async function llamarTraductor(texto, target){
    const r = await fetch('/tickets/api/traducir', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ texto: texto, target: target }),
    });
    return r.json();
  }

  const btnTraducir = document.getElementById('rpBtnTraducir');
  btnTraducir.addEventListener('click', async function(){
    const texto = (rpQuill.getText() || '').trim();
    if (!texto){ ilusToast('Escribe tu mensaje primero.', { type: 'info' }); return; }
    const original = btnTraducir.innerHTML;
    btnTraducir.disabled = true;
    btnTraducir.innerHTML = '<span class="spinner-border spinner-border-sm"></span>';
    try{
      const d = await llamarTraductor(texto, rpTradLang);
      if (!d.ok){ ilusToast(d.error || 'No se pudo traducir', { type: 'error' }); return; }
      document.getElementById('rpTradIdiomaNombre').textContent =
        document.getElementById('rpTradIdiomaLabel').textContent;
      document.getElementById('rpTradTexto').textContent = d.traduccion;
      document.getElementById('rpTradPreview').style.display = 'block';
    }catch(e){ ilusToast('Error de red al traducir', { type: 'error' }); }
    finally{ btnTraducir.disabled = false; btnTraducir.innerHTML = original; }
  });

  document.getElementById('rpBtnUsarTraduccion').addEventListener('click', function(){
    const texto = document.getElementById('rpTradTexto').textContent || '';
    rpQuill.setText(texto);
    document.getElementById('rpTradPreview').style.display = 'none';
  });

  // Traducir un mensaje del hilo (cliente/proveedor) al español -- delegado
  // porque el hilo se re-renderiza en cada cargar().
  window.tkTraducirMensaje = async function(btn, mid){
    const cont = document.getElementById('rpTrad_' + mid);
    if (cont.style.display !== 'none'){ cont.style.display = 'none'; return; }
    const texto = btn.dataset.texto || '';
    if (!texto.trim()) return;
    const original = btn.innerHTML;
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner-border spinner-border-sm"></span>';
    try{
      const d = await llamarTraductor(texto, 'es');
      if (!d.ok){ ilusToast(d.error || 'No se pudo traducir', { type: 'error' }); return; }
      cont.textContent = d.traduccion;
      cont.style.display = 'block';
    }catch(e){ ilusToast('Error de red al traducir', { type: 'error' }); }
    finally{ btn.disabled = false; btn.innerHTML = original; }
  };

  // 2026-07-19 (Fable) — Copiar mensaje (NUEVO, aditivo, Regla #4.2 no afecta
  // nada existente). innerText de .rp-content: evita copiar HTML crudo (F3)
  // y no incluye los chips de adjuntos (viven fuera, en .rp-adj-lista).
  window.tkCopiarMensaje = function(btn){
    const cont = btn.closest('.rp-bubble');
    const div = cont && cont.querySelector('.rp-content');
    const t = div ? div.innerText : '';
    if(!t.trim()) return;
    navigator.clipboard.writeText(t).then(
      function(){ ilusToast('✓ Mensaje copiado', {type:'success'}); },
      function(){ ilusToast('No se pudo copiar', {type:'error'}); });
  };
})();

// ══════════════ Conexión — envío "inteligente" ══════════════
const rpOfflineBar = document.getElementById('rpOffline');
function actualizarConexion(){
  // 2026-08-10: #rpOffline/#rpBtnEnviar no existen en modo cliente.
  if (!rpOfflineBar) return;
  const offline = !navigator.onLine;
  rpOfflineBar.classList.toggle('show', offline);
  const btn = document.getElementById('rpBtnEnviar');
  if (btn) btn.disabled = offline;
}
window.addEventListener('online', actualizarConexion);
window.addEventListener('offline', actualizarConexion);
actualizarConexion();

// ══════════════ Modo: responder al cliente vs comentario interno ══════════════
document.querySelectorAll('.rp-mode-tab').forEach(function(el){
  el.addEventListener('click', function(){
    document.querySelectorAll('.rp-mode-tab').forEach(x=>x.classList.remove('active'));
    el.classList.add('active');
    rpModo = el.dataset.mode;
    document.getElementById('rpHeaders').classList.toggle('show', rpModo === 'cliente');
    const btn = document.getElementById('rpBtnEnviar');
    btn.innerHTML = rpModo === 'cliente'
      ? '<i class="bi bi-send-fill me-1"></i>Enviar'
      : '<i class="bi bi-chat-left-text me-1"></i>Comentar';
    // Mostrar/ocultar De/Para/CC cambia el alto del composer -- recalcular
    // el alto del hilo para que el composer siga siendo visible sin scroll.
    requestAnimationFrame(ajustarAltoHilo);
  });
});

async function cargar(){
  let d;
  try{ const r = await fetch('/tickets/api/tickets/'+TID); d = await r.json(); }
  catch(e){ ilusToast('No se pudo cargar el ticket', {type:'error'}); return; }
  if(!d.ok){ ilusToast(d.error||'Error', {type:'error'}); return; }
  const t = d.ticket;
  ticketActual = t;

  renderStepper(t, d.mensajes);
  renderQueFalta(t, d);

  document.getElementById('hNum').textContent = t.numero_ticket || ('#'+t.id);
  document.getElementById('hCliente').textContent = t.empresa || t.nombre_contacto || t.rut || 'Sin cliente';
  const be = document.getElementById('hEstado');
  be.textContent = ESTADO_LABEL[t.estado]||t.estado; be.className = 'tk-badge bs-'+t.estado;
  // 2026-07-15: badge de estado GRANDE en la tarjeta "Estado y gestión" de la
  // columna lateral (rediseño #pane-info) -- mismo texto/color que el badge
  // del hero (hEstado), solo que más visible dentro de esa tarjeta.
  const beGrande = document.getElementById('estGestionBadge');
  if(beGrande){ beGrande.textContent = ESTADO_LABEL[t.estado]||t.estado; beGrande.className = 'tk-badge-grande bs-'+t.estado; }
  // 2026-07-15 (Daniel): título de la pestaña dinámico por ticket (antes
  // era "Ticket — ILUS" fijo para todos). El bloque title de Jinja (arriba
  // del archivo) sigue siendo el fallback razonable antes de esta carga.
  document.title = (t.numero_ticket || ('#'+t.id))
    + (t.empresa || t.nombre_contacto ? ' · ' + (t.empresa || t.nombre_contacto) : '')
    + ' — ILUS';

  // El estado puede ser uno de los 3 AUTOMATICOS del ciclo de vida de la OT
  // (ot_generated/ot_in_progress/ot_pending_approval), que no aparecen como
  // <option> en selEstado (el backend tampoco los acepta via PATCH manual).
  // En ese caso el select se deshabilita y se muestra el badge de solo lectura.
  const selEstadoEl = document.getElementById('selEstado');
  const notaEstadoAuto = document.getElementById('notaEstadoAuto');
  const esEstadoAutomatico = !selEstadoEl.querySelector('option[value="'+t.estado+'"]');
  if(esEstadoAutomatico){
    selEstadoEl.disabled = true;
    if(notaEstadoAuto) notaEstadoAuto.style.display = 'block';
  } else {
    selEstadoEl.disabled = false;
    selEstadoEl.value = t.estado;
    if(notaEstadoAuto) notaEstadoAuto.style.display = 'none';
  }
  document.getElementById('selPrio').value = t.prioridad;
  poblarSelectEjecutivo();
  document.getElementById('txtDesc').textContent = t.descripcion || '—';
  if(!document.getElementById('rpTo').value && t.email) rpToChips.setValue(t.email);

  // ── Fila secundaria del hero: prioridad / tipo / garantía / SLA / meta ──
  const hPrio = document.getElementById('hPrio');
  if(t.prioridad){ hPrio.style.display='inline-block'; hPrio.textContent = t.prioridad.charAt(0).toUpperCase()+t.prioridad.slice(1); hPrio.className = 'tk-badge pr-'+t.prioridad; }
  else hPrio.style.display='none';
  const hTipo = document.getElementById('hTipo');
  if(t.tipo){ hTipo.style.display='inline-block'; hTipo.textContent = TIPO_LABEL[t.tipo]||t.tipo; } else hTipo.style.display='none';
  document.getElementById('hGarantia').style.display = t.es_garantia ? 'inline-flex' : 'none';
  const hSla = document.getElementById('hSla');
  if(t.fecha_limite){
    hSla.style.display='inline-block';
    const partes = String(t.fecha_limite).split('/');
    let venc=false, pronto=false;
    if(partes.length===3){
      const dLimite = new Date(+partes[2], +partes[1]-1, +partes[0]);
      const hoy = new Date(); hoy.setHours(0,0,0,0);
      const dias = Math.round((dLimite-hoy)/86400000);
      venc = dias<0; pronto = !venc && dias<=2;
    }
    hSla.className = 'tk-sla'+(venc?' venc':(pronto?' pronto':''));
    hSla.innerHTML = '<i class="bi bi-'+(venc?'alarm-fill':'calendar-event')+' me-1"></i>'
      + (venc?'Vencido ':'Vence ')+esc(t.fecha_limite);
  } else hSla.style.display='none';
  // ── SLA de respuesta vencido (campos nuevos del backend 2026-07-14:
  // t.sla_vencido bool + t.sla_horas float + d.sla_umbral_horas int).
  // Badge rojo pulsante "SLA vencido · Nh" — solo si sla_vencido===true
  // (los estados terminales llegan con sla_horas=null / sla_vencido=false).
  const hSlaV = document.getElementById('hSlaVencido');
  if(t.sla_vencido === true){
    const hrs = (typeof t.sla_horas === 'number') ? Math.round(t.sla_horas) : null;
    document.getElementById('hSlaVencidoTxt').textContent =
      'SLA vencido' + (hrs !== null ? ' · ' + hrs + 'h' : '');
    hSlaV.title = 'Sin resolución hace ' + (hrs !== null ? hrs + ' horas' : 'más de lo permitido')
      + (d.sla_umbral_horas ? ' (umbral: ' + d.sla_umbral_horas + 'h)' : '');
    hSlaV.style.display = 'inline-flex';
  } else hSlaV.style.display = 'none';
  document.getElementById('hSub').textContent = 'Creado '+fmtFechaHora(t.created_at)+(t.created_by?' · '+t.created_by:'');
  const hDirRow = document.getElementById('hDireccionRow');
  if(t.direccion && String(t.direccion).trim()){
    hDirRow.style.display='inline-flex';
    document.getElementById('hDireccion').textContent = t.direccion + (t.comuna_nombre ? ', '+t.comuna_nombre : '');
  } else hDirRow.style.display='none';
  const hRespRow = document.getElementById('hResponsableRow');
  if(t.asignado_a && String(t.asignado_a).trim()){
    hRespRow.style.display='inline-flex';
    document.getElementById('hResponsable').textContent = t.asignado_a;
  } else hRespRow.style.display='none';

  // ── Visto por (quién lo abrió, cuándo) — línea discreta en Estado y gestión ──
  const vp = document.getElementById('vistoPor');
  if(d.vistas && d.vistas.length){
    vp.style.display = 'block';
    vp.innerHTML = '<i class="bi bi-eye me-1"></i>Visto por: ' + d.vistas.map(function(v){
      return '<b>'+esc(v.usuario)+'</b> ('+esc(fmtFechaVisto(v.visto_at))+')';
    }).join(' · ');
  } else vp.style.display = 'none';

  // ── Identificación del Cliente ──
  document.getElementById('idFechaIngreso').textContent = t.created_at ? ('Ingreso: '+fmtFechaHora(t.created_at)) : '';
  const idOrigenBadge = document.getElementById('idOrigenBadge');
  if(t.origen){ idOrigenBadge.style.display='inline-block'; idOrigenBadge.textContent = String(t.origen).toUpperCase(); }
  else idOrigenBadge.style.display='none';
  renderIdentificacionCompacta(t);

  // ── Problema: título + chips de metadata ──
  const tTitulo = document.getElementById('txtTitulo');
  if(t.titulo){ tTitulo.style.display='block'; tTitulo.textContent = t.titulo; } else tTitulo.style.display='none';
  // Nota 2026-07-12 (Daniel): 'Producto' y 'SKU' se quitaron de acá porque con
  // varios equipos asociados llegan concatenados de forma mal-agregada
  // (ej: "Equipo A / Equipo B · Equipo C"). Esa info ya se ve bien desglosada
  // por equipo en la tabla de Equipo(s) más abajo — no se repite acá.
  const chips = [
    ['tag','Marca', t.marca],
    ['file-earmark-ruled','N° documento', t.numero_documento], ['diagram-3','Origen', t.origen],
  ].filter(c=>c[2]);
  document.getElementById('chipsProblema').innerHTML = chips.map(c=>
    '<span class="tk-chip"><i class="bi bi-'+c[0]+'"></i>'+esc(c[1])+': <b>'+esc(c[2])+'</b></span>').join('');

  // ── Notas internas ──
  const cardNotas = document.getElementById('cardNotas');
  if(t.notas_internas){ cardNotas.style.display='block'; document.getElementById('txtNotas').textContent = t.notas_internas; }
  else cardNotas.style.display='none';

  // Equipos — tabla "eq-table" (mismo lenguaje visual que Mantenciones):
  // Icono | Equipo(+notas/+N unidades) | SKU/Código | Serie | Tipo | Fecha Emisión | Garantía | Acción
  const le = document.getElementById('listaEquipos');
  equiposCache = d.equipos || [];
  renderGarantiaAlerta(t, equiposCache);
  if(d.equipos.length){
    le.innerHTML = '<div class="table-responsive"><table class="eq-table" id="tkEqTabla"><thead><tr>'
      + '<th style="width:32px"></th><th>Equipo</th><th>SKU / Código</th><th>Documento</th><th>Serie</th>'
      + '<th>Tipo</th><th>Fecha Emisión</th><th>Garantía</th><th style="width:44px"></th>'
      + '</tr></thead><tbody>'
      + d.equipos.map(function(eq){
          const codigo = eq.sku || eq.erp_kopr;
          const nombreTxt = esc(eq.nombre||eq.erp_kopr||'—');
          const nombreHtml = eq.maquina_id
            ? '<a href="/mantenciones/maquinas/'+eq.maquina_id+'" class="text-decoration-none text-dark" title="Ver ficha del equipo">'
              + '<div class="eq-name-main" style="cursor:pointer">'+nombreTxt
              + ' <i class="bi bi-arrow-up-right-square ms-1 text-muted" style="font-size:.75rem;opacity:.5"></i></div></a>'
            : '<div class="eq-name-main">'+nombreTxt+'</div>';
          return '<tr>'
            + '<td><div class="eq-cell-icon">'+eqEmoji(eq.nombre)+'</div></td>'
            + '<td>'
              + nombreHtml
              + (eq.notas?'<div class="eq-name-sub text-truncate" style="max-width:220px" title="'+esc(eq.notas)+'">'+esc(eq.notas)+'</div>':'')
              + ((eq.cantidad||1) > 1 ? '<div class="eq-name-sub"><i class="bi bi-layers me-1"></i>'+eq.cantidad+' unidades</div>' : '')
            + '</td>'
            + '<td>'+(codigo?'<span class="eq-sku">'+esc(codigo)+'</span>':'<span class="text-muted">—</span>')+'</td>'
            // 2026-07-14 (Daniel): documento de origen del equipo
            // (tk_ticket_equipos.documento_garantia, ej. "Factura 10599") ahora
            // en columna propia (antes era un chip escondido bajo el nombre).
            + '<td>'+(eq.documento_garantia && String(eq.documento_garantia).trim()
                ? '<span class="eq-sku" title="Documento de origen: '+esc(eq.documento_garantia)+'">'+esc(eq.documento_garantia)+'</span>'
                : '<span class="text-muted">—</span>')+'</td>'
            + '<td>'+(eq.serie?'<span style="font-family:monospace;font-size:.78rem;color:#374151">'+esc(eq.serie)+'</span>':'<span class="text-muted">—</span>')+'</td>'
            + '<td>'+(eq.tipo?esc(eq.tipo):'<span class="text-muted">—</span>')+'</td>'
            + '<td>'+(eq.fecha_emision?'<span style="font-size:.78rem;color:#374151">'+fmtFechaISO(eq.fecha_emision)+'</span>':'<span class="text-muted">—</span>')+'</td>'
            + '<td>'+renderGarantiaChip(eq)+'</td>'
            + '<td class="text-end"><button class="tk-eq-actionbtn" onclick="delEquipo('+eq.id+')" title="Quitar equipo"><i class="bi bi-trash"></i></button></td>'
            + '</tr>';
        }).join('')
      + '</tbody></table></div>';
  } else {
    le.innerHTML = '<div class="tk-empty-mini"><i class="bi bi-tools"></i><span>Sin equipos asociados a este ticket.</span></div>';
  }

  // Acciones — tarjeta "Orden de Trabajo" (Generar OT / ya vinculada)
  renderAccionesOT(t);

  // Tarjeta "Cotización" — lista las generadas DESDE este ticket (número/estado/total)
  renderCotizaciones(d.cotizaciones);

  // Documentos ERP del ticket — ahora renderizados dentro de la tarjeta Equipo(s)
  const ld = document.getElementById('listaDocs');
  ld.innerHTML = d.documentos.length ? d.documentos.map(function(x){
    // 2026-07-13 (Daniel, URGENTE): "0000-00-00" -- MySQL puede guardar una
    // fecha "cero" literal (no NULL) en columnas DATE cuando nunca se
    // especifico una; se trata igual que "sin fecha" en vez de mostrar el
    // valor crudo.
    const fechaOk = x.fecha && !/^0000-00-00/.test(String(x.fecha)) ? x.fecha : null;
    const meta = [fechaOk, (x.monto!=null && x.monto!=='') ? '$'+Number(x.monto).toLocaleString('es-CL') : null]
      .filter(Boolean).join(' · ');
    return '<div class="tk-doc"><span class="doc-id"><i class="bi bi-file-earmark-text"></i>'
      + esc((x.erp_tido||'')+' '+(x.erp_nudo||''))+'</span>'
      + (meta?'<span class="doc-meta">'+esc(meta)+'</span>':'')+'</div>';
  }).join('') : '<div class="tk-empty-mini"><i class="bi bi-file-earmark-x"></i><span>Sin documentos ERP asociados a este ticket.</span></div>';

  // Adjuntos (galería) — pestaña Información, aditivo a lo que ya se ve en Respuestas
  renderAdjuntosInfo(d.adjuntos);

  renderThread(d.mensajes, d.adjuntos);

  // Línea de tiempo de actividad (supervisión) — tarjeta al final de Información.
  // Solo se llama si el nodo existe en el DOM (gate server-side en el Jinja).
  if (PUEDE_VER_ACTIVIDAD) renderActividad(d);

  // Daniel 2026-07-12: "cuando las lea ya, quiero que se borren" -- antes
  // se contaban TODOS los mensajes de cliente del historial (nunca bajaba
  // de ahi). d.unread_count ya viene calculado en el backend comparando
  // contra staff_last_read_at (fix real, no en JS -- ver tk_api_get).
  const nuevos = d.unread_count || 0;
  const cb = document.getElementById('convBadge');
  if(nuevos>0){ cb.style.display='inline-block'; cb.textContent=nuevos; } else cb.style.display='none';

  // marcar leído
  fetch('/tickets/api/tickets/'+TID+'/marcar-leido',{method:'PATCH'}).catch(()=>{});
}

// ══════════════ Stepper de estado (tracking visual de avance) ══════════════
// 4 nodos fijos; 'pending' es transversal (no consume nodo, se muestra como
// píldora); 'cancelado' reemplaza todo el stepper por un banner.
const TK_STEP_DEFS = [
  {estado:'open', label:'Abierto', icon:'bi-inbox-fill'},
  {estado:'in_progress', label:'En Curso', icon:'bi-gear-fill'},
  {estado:'resolved', label:'Resuelto', icon:'bi-check-circle-fill'},
  {estado:'closed', label:'Cerrado', icon:'bi-archive-fill'},
];
const TK_EN_CURSO_SET = ['in_progress','ot_pending_approval','ot_generated','ot_in_progress'];
function tkStepIndexParaEstado(e){
  if(e==='open') return 0;
  if(TK_EN_CURSO_SET.includes(e)) return 1;
  if(e==='resolved') return 2;
  if(e==='closed') return 3;
  return 0;
}
function tkEstadoDesdeLabel(label){
  for(const k in ESTADO_LABEL){ if(ESTADO_LABEL[k]===label) return k; }
  return null;
}
// 'pending' puede pasar en cualquier punto del flujo. Como el ticket solo
// guarda el estado actual (no un historial estructurado), reconstruimos el
// "último estado activo" leyendo la bitácora de cambios (tk_mensajes tipo
// 'cambio_estado', ya cargada con el ticket) para no romper el stepper de 4 nodos.
function tkEstadoBaseParaStepper(t, mensajes){
  if(t.estado !== 'pending') return t.estado;
  const cambios = (mensajes||[]).filter(m=>m.tipo==='cambio_estado');
  for(let i=cambios.length-1;i>=0;i--){
    const partes = String(cambios[i].contenido||'').split('→');
    if(partes.length===2){
      const destEstado = tkEstadoDesdeLabel(partes[1].trim());
      if(destEstado && destEstado!=='pending') return destEstado;
    }
  }
  return 'open'; // sin bitácora previa: asumimos que recién se abrió
}
function renderStepper(t, mensajes){
  const wrap = document.getElementById('tkStepperWrap');
  if(t.estado === 'cancelado'){
    wrap.innerHTML = '<div class="tk-cancel-banner"><i class="bi bi-x-circle-fill"></i>Ticket cancelado</div>';
    return;
  }
  const base = tkEstadoBaseParaStepper(t, mensajes);
  const curIdx = tkStepIndexParaEstado(base);
  const targetPorIdx = ['open','in_progress','resolved','closed'];
  const stepsHtml = TK_STEP_DEFS.map(function(s, i){
    let cls = 'tk-step';
    if(i < curIdx) cls += ' done'; else if(i === curIdx) cls += ' current';
    let badge = '';
    if(i===1 && curIdx===1 && TK_EN_CURSO_SET.includes(t.estado) && t.estado!=='in_progress'){
      badge = '<span class="tk-step-badge">'+esc(ESTADO_LABEL[t.estado]||t.estado)+'</span>';
    }
    return '<div class="'+cls+'" data-idx="'+i+'">'+badge
      + '<div class="tk-step-circle"><i class="bi '+s.icon+'"></i></div>'
      + '<div class="tk-step-label">'+s.label+'</div></div>';
  }).join('');
  const pendingPill = t.estado === 'pending'
    ? '<div class="tk-pending-pill"><i class="bi bi-hourglass-split"></i>En espera</div>' : '';
  wrap.innerHTML = '<div class="tk-stepper">'+stepsHtml+'</div>' + pendingPill;
  wrap.querySelectorAll('.tk-step').forEach(function(el){
    el.addEventListener('click', async function(){
      const idx = +el.dataset.idx;
      if(idx === curIdx) return;
      const nuevoEstado = targetPorIdx[idx];
      const label = ESTADO_LABEL[nuevoEstado] || nuevoEstado;
      const ok = await ilusConfirm({title:'Cambiar estado', message:'¿Cambiar el estado del ticket a "'+label+'"?',
        okLabel:'Cambiar', cancelLabel:'Cancelar'});
      if(!ok) return;
      const r = await fetch('/tickets/api/tickets/'+TID, {method:'PATCH',
        headers:{'Content-Type':'application/json'}, body:JSON.stringify({estado: nuevoEstado})});
      const d2 = await r.json();
      if(d2.ok){ ilusToast('✓ Estado actualizado', {type:'success'}); cargar(); }
      else ilusToast(d2.error||'Error', {type:'error'});
    });
  });
}

// ══════════════ "¿Qué falta?" — informativo, no bloquea nada ══════════════
function renderQueFalta(t, d){
  const checks = [
    ['Ejecutivo asignado', !!(t.asignado_a && String(t.asignado_a).trim())],
    ['Al menos un equipo asociado', ((d.equipos)||[]).length > 0],
    ['N° de documento o boleta', !!(t.numero_documento && String(t.numero_documento).trim())],
    ['Al menos un adjunto', ((d.adjuntos)||[]).length > 0],
    ['Dirección registrada', !!(t.direccion && String(t.direccion).trim())],
  ];
  const completos = checks.filter(c=>c[1]).length;
  document.getElementById('queFaltaContador').textContent = completos+' de '+checks.length+' completos';
  document.getElementById('queFaltaLista').innerHTML = checks.map(function(c){
    const ok = c[1];
    return '<div class="tk-qf-item '+(ok?'ok':'falta')+'"><i class="bi '
      + (ok?'bi-check-circle-fill':'bi-exclamation-circle-fill')+'"></i>'+esc(c[0])+'</div>';
  }).join('');
}

// ══════════════ Identificación del Cliente — vista compacta con edición campo por campo ══════════════
// Reemplaza el patrón anterior de "Editar" único que revelaba toda la grilla
// (pedido explícito de Daniel): cada dato tiene su propio lápiz; un clic
// convierte SOLO esa fila en un input inline (ver tkIdEmpezarEdicion).
const TK_ID_CAMPOS = [
  {key:'rut',              label:'RUT',           icon:'person-badge'},
  {key:'empresa',          label:'Empresa',       icon:'briefcase'},
  {key:'nombre_contacto',  label:'Contacto',      icon:'person'},
  {key:'email',            label:'Correo',        icon:'envelope', tipo:'email'},
  {key:'phone',            label:'Teléfono',      icon:'telephone'},
  {key:'direccion',        label:'Dirección',     icon:'geo-alt'},
  {key:'region_nombre',    label:'Región',        icon:'map'},
  {key:'comuna_nombre',    label:'Comuna',        icon:'pin-map'},
  {key:'sucursal',         label:'Sucursal',      icon:'building'},
  {key:'numero_documento', label:'N° Documento',  icon:'file-earmark-ruled'},
  {key:'tipo',             label:'Tipo',          icon:'tag', tipo:'select'},
];
function tkIdCampoConfig(key){ return TK_ID_CAMPOS.find(c=>c.key===key); }
function tkIdValorMostrado(campo, t){
  const v = t[campo.key];
  if(campo.key === 'tipo') return v ? esc(TIPO_LABEL[v]||v) : '<span class="text-muted fw-normal">—</span>';
  if(campo.key === 'email') return v ? '<a href="mailto:'+esc(v)+'">'+esc(v)+'</a>' : '<span class="text-muted fw-normal">—</span>';
  if(campo.key === 'phone') return v ? '<a href="tel:'+esc(v)+'">'+esc(v)+'</a>' : '<span class="text-muted fw-normal">—</span>';
  if(campo.key === 'direccion'){
    if(!v) return '<span class="text-muted fw-normal">—</span>';
    const destino = v + (t.comuna_nombre ? (', '+t.comuna_nombre) : '');
    const mapsHref = 'https://www.google.com/maps/dir/?api=1&destination='+encodeURIComponent(destino);
    return esc(v) + ' <a href="'+mapsHref+'" target="_blank" rel="noopener">Ver en mapa</a>';
  }
  return v ? esc(v) : '<span class="text-muted fw-normal">—</span>';
}
// 2026-07-14 (Daniel): "lo veo con muchos campos vacios" -- las filas SIN
// dato ya no se muestran de entrada: van colapsadas tras "Ver campos
// vacios (N)". El estado expandido persiste entre re-renders (guardar/
// cancelar una edicion re-pinta la tarjeta y no debe colapsar de vuelta).
let tkIdVaciosAbierto = false;
function tkIdFilaHtml(campo, t){
  return '<i class="bi bi-'+campo.icon+'"></i><dt>'+esc(campo.label)+'</dt>'
    + '<dd data-field="'+campo.key+'">'
    + '<span class="tk-dd-val">'+tkIdValorMostrado(campo, t)+'</span>'
    + '<i class="bi bi-pencil-fill tk-edit-pencil" data-field="'+campo.key+'" title="Editar '+esc(campo.label)+'"></i>'
    + '</dd>';
}
function renderIdentificacionCompacta(t){
  document.getElementById('idCompactoNombre').textContent = t.empresa || t.nombre_contacto || 'Sin nombre registrado';
  const dl = document.getElementById('idCompactoDatos');
  const dlVacios = document.getElementById('idCompactoVacios');
  const btnVacios = document.getElementById('idVaciosBtn');
  const wrapVacios = document.getElementById('idVaciosWrap');
  const tieneDato = function(campo){
    const v = t[campo.key];
    return !(v === null || v === undefined || String(v).trim() === '');
  };
  let llenos = TK_ID_CAMPOS.filter(tieneDato);
  let vacios = TK_ID_CAMPOS.filter(function(c){ return !tieneDato(c); });
  // Ticket recien creado sin NINGUN dato: mostrar todo de una (una tarjeta
  // 100% colapsada no orienta a nadie).
  if(!llenos.length){ llenos = vacios; vacios = []; }
  dl.innerHTML = llenos.map(function(campo){ return tkIdFilaHtml(campo, t); }).join('');
  if(vacios.length){
    dlVacios.innerHTML = vacios.map(function(campo){ return tkIdFilaHtml(campo, t); }).join('');
    document.getElementById('idVaciosBtnTxt').textContent = tkIdVaciosAbierto
      ? 'Ocultar campos vacíos' : 'Ver campos vacíos ('+vacios.length+')';
    btnVacios.style.display = 'inline-flex';
    btnVacios.classList.toggle('abierto', tkIdVaciosAbierto);
    wrapVacios.classList.toggle('show', tkIdVaciosAbierto);
  } else {
    dlVacios.innerHTML = '';
    btnVacios.style.display = 'none';
    wrapVacios.classList.remove('show');
  }
  // Lapices de AMBAS listas (llenos + vacios) siguen editando igual.
  document.querySelectorAll('#idVistaCompacta .tk-edit-pencil').forEach(function(p){
    p.addEventListener('click', function(){ tkIdEmpezarEdicion(p.dataset.field); });
  });
  const badge = document.getElementById('idCompactoTipoBadge');
  if(t.tipo){ badge.style.display='inline-block'; badge.textContent = TIPO_LABEL[t.tipo]||t.tipo; } else badge.style.display='none';
}
(function(){
  const btn = document.getElementById('idVaciosBtn');
  if(!btn) return;
  btn.addEventListener('click', function(){
    tkIdVaciosAbierto = !tkIdVaciosAbierto;
    btn.classList.toggle('abierto', tkIdVaciosAbierto);
    document.getElementById('idVaciosWrap').classList.toggle('show', tkIdVaciosAbierto);
    const n = document.querySelectorAll('#idCompactoVacios dd').length;
    document.getElementById('idVaciosBtnTxt').textContent = tkIdVaciosAbierto
      ? 'Ocultar campos vacíos' : 'Ver campos vacíos ('+n+')';
  });
})();

// Convierte la fila del campo indicado en un input/select editable inline.
function tkIdEmpezarEdicion(key){
  // El dd puede vivir en la lista de campos CON dato (#idCompactoDatos) o en
  // la de campos vacios (#idCompactoVacios) -- se busca en el contenedor.
  const dd = document.querySelector('#idVistaCompacta dd[data-field="'+key+'"]');
  if(!dd || dd.dataset.editing === '1') return;
  const campo = tkIdCampoConfig(key);
  const valorActual = (ticketActual && ticketActual[key]) || '';
  dd.dataset.editing = '1';
  let inputHtml;
  if(campo.tipo === 'select'){
    const opts = ['<option value="">—</option>'].concat(Object.keys(TIPO_LABEL).map(function(k){
      return '<option value="'+esc(k)+'"'+(k===valorActual?' selected':'')+'>'+esc(TIPO_LABEL[k])+'</option>';
    }));
    inputHtml = '<select class="form-select form-select-sm tk-dd-input">'+opts.join('')+'</select>';
  } else if(key === 'direccion'){
    // Daniel 2026-07-12 ("¿no debería estar validada?... esto tiene que ser
    // a nivel general"): editar Dirección pasaba por un input de texto
    // plano -- nunca completaba region/comuna/coordenadas (perfil logistico
    // de Daniel: TODA direccion se valida con Google Places, sin excepcion).
    inputHtml = '<input type="text" id="tkDirEditInput" class="form-control form-control-sm tk-dd-input" '
      + 'placeholder="Empieza a escribir y elige del dropdown…" autocomplete="off" value="'+esc(valorActual)+'">';
  } else {
    const tipoInput = campo.tipo === 'email' ? 'email' : 'text';
    inputHtml = '<input type="'+tipoInput+'" class="form-control form-control-sm tk-dd-input" value="'+esc(valorActual)+'">';
  }
  dd.innerHTML = inputHtml;
  const input = dd.querySelector('.tk-dd-input');
  input.focus();
  if(input.select) input.select();
  let resuelto = false;
  let extraDireccion = null; // {region_nombre, comuna_nombre, direccion_lat/lng/place_id} si se eligio un resultado real
  function cancelar(){
    if(resuelto) return; resuelto = true;
    renderIdentificacionCompacta(ticketActual);
  }
  function guardar(){
    if(resuelto) return; resuelto = true;
    tkIdGuardarCampo(key, input.value.trim(), extraDireccion);
  }
  if(key === 'direccion'){
    // ilusPlacesAutocomplete se registra recien ahora (el input recien se
    // creo) -- misma funcion global que ya usa el modal "Nuevo Ticket"
    // (list.html), solo que aca vive dentro de un editor inline por campo.
    const initDir = function(){
      if(typeof ilusPlacesAutocomplete !== 'function'){
        if(window.__ilusGmapsPending){ window.__ilusGmapsPending.push(initDir); }
        return;
      }
      ilusPlacesAutocomplete('tkDirEditInput', { country:'cl', types:['address'],
        onPlaceSelected:function(place){
          const comp = place.componentes || [];
          const cComuna = comp.find(c=>c.types.indexOf('locality')>=0 || c.types.indexOf('administrative_area_level_3')>=0);
          const cRegion = comp.find(c=>c.types.indexOf('administrative_area_level_1')>=0);
          extraDireccion = {
            direccion_lat: place.lat, direccion_lng: place.lng,
            direccion_place_id: place.place_id || null,
            comuna_nombre: cComuna ? cComuna.long_name : null,
            region_nombre: cRegion ? cRegion.long_name : null,
          };
        } });
    };
    initDir();
  }
  input.addEventListener('blur', guardar);
  input.addEventListener('keydown', function(e){
    if(e.key === 'Enter'){ e.preventDefault(); input.blur(); }
    else if(e.key === 'Escape'){ e.preventDefault(); cancelar(); }
  });
}

async function tkIdGuardarCampo(key, valor, extra){
  const payload = {}; payload[key] = valor;
  // Si se eligio una direccion REAL del dropdown de Google, guardamos
  // tambien region/comuna/coordenadas en el MISMO PATCH -- si el usuario
  // solo escribio texto libre sin elegir sugerencia, extra es null y no
  // se toca comuna/region existentes (no clobbear datos buenos con vacio).
  if(extra){ Object.assign(payload, extra); }
  try{
    const r = await fetch('/tickets/api/tickets/'+TID, {method:'PATCH',
      headers:{'Content-Type':'application/json'}, body:JSON.stringify(payload)});
    const d = await r.json();
    if(d.ok){
      Object.assign(ticketActual, payload);
      renderIdentificacionCompacta(ticketActual);
      ilusToast(extra ? '✓ Dirección verificada y guardada' : '✓ Guardado', {type:'success'});
      cargar();
    } else {
      renderIdentificacionCompacta(ticketActual);
      ilusToast(d.error||'Error al guardar', {type:'error'});
    }
  }catch(e){
    renderIdentificacionCompacta(ticketActual);
    ilusToast('Sin conexión — no se pudo guardar', {type:'error'});
  }
}

// 2026-07-19 (Fable) — Conversación v2: marca de agua del último id renderizado.
// Vive fuera del DOM (sobrevive al rebuild total de innerHTML) para que solo
// el/los mensaje(s) NUEVOS desde el último render animen (R1/R2/R3 del encargo).
let _rpUltimoMsgId = null; // null = aún sin primer render
function renderThread(mensajes, adjuntos){
  const adjPorMensaje = {};
  (adjuntos||[]).forEach(function(a){
    const k = a.mensaje_id || 'sin_mensaje';
    (adjPorMensaje[k] = adjPorMensaje[k] || []).push(a);
  });
  const th = document.getElementById('rpThread');
  // Auto-refresco silencioso cada 6s (ver abajo del archivo) re-renderiza
  // este hilo aunque el usuario este leyendo mensajes antiguos -- sin esto,
  // el scroll saltaba al fondo cada vez y era imposible leer historial.
  // Solo se autoscrollea si el usuario YA estaba abajo (o es la carga inicial).
  // CORRECCION 2026-07-12: el hilo SI tiene scroll propio (contenido) --
  // "abajo" se mide contra el DIV, no contra la pagina (ver .rp-thread arriba).
  const yaEstabaAbajo = th.childElementCount === 0
    || (th.scrollHeight - th.scrollTop - th.clientHeight) < 60;
  if(!mensajes.length){
    th.innerHTML = '<div style="color:#9ca3af;font-size:.85rem;text-align:center;padding:20px;">Aún no hay mensajes.</div>';
    // R1: primer render vacío -- deja la marca en 0 (no null) para que el
    // primer mensaje real que llegue SÍ anime.
    _rpUltimoMsgId = 0;
    return;
  }
  // 2026-07-19 (Fable): marca de agua anti re-animación (ver _rpUltimoMsgId
  // arriba de esta función). prevMax es el snapshot de ANTES de este render;
  // maxId se recalcula recorriendo mensajes y queda grabado al final.
  const prevMax = _rpUltimoMsgId;
  let maxId = prevMax || 0;
  th.innerHTML = mensajes.map(function(m, i){
    // D1: el orden real del hilo es COALESCE(message_date,created_at), pero la
    // fecha MOSTRADA es created_at -- un correo ingerido tarde puede romper la
    // monotonicidad y el separador de fecha puede repetirse. Es correcto, no
    // "consolidar" fechas.
    const prev = i > 0 ? mensajes[i-1] : null;
    const fecha = _rpFechaDe(m), fechaPrev = prev ? _rpFechaDe(prev) : null;
    const sep = (fecha && fecha !== fechaPrev)
      ? '<div class="rp-fecha-sep"><span>'+esc(_rpEtiquetaFecha(fecha))+'</span></div>' : '';
    if(m.id && Number(m.id) > maxId) maxId = Number(m.id);
    const esNuevo = prevMax !== null && !!m.id && Number(m.id) > prevMax;

    const esEvento = !['comentario','mensaje','client_message'].includes(m.tipo);
    if(esEvento){
      // R2: los eventos NO llevan avatar ni animan (decisión explícita).
      return sep + '<div class="rp-msg evento"><div class="rp-bubble">'+esc(m.contenido||'')+' · '+fmtFechaHora(m.created_at)+'</div></div>';
    }
    const esCliente = m.tipo === 'client_message';
    const esInterno = !!m.es_interno && m.tipo !== 'client_message';
    // 2026-07-14 (Daniel): "resaltarlo en blanco cuando es automatico" --
    // un mensaje saliente SIN usuario lo genero el SISTEMA (respuesta
    // automatica / notificacion); recibe la clase .auto (variante blanca).
    const esAuto = !esCliente && !esInterno && !m.usuario;
    const cls = esCliente ? 'in' : (esInterno ? 'interno' : ('out' + (esAuto ? ' auto' : '')));
    const who = esCliente ? (esc(ticketActual && ticketActual.nombre_contacto || 'Cliente')) : esc(m.usuario || 'Sistema');
    const autoTag = esAuto ? ' <span class="rp-auto-tag"><i class="bi bi-robot"></i>Automático</span>' : '';
    let estadoTag = '';
    if(!esCliente && !esInterno){
      estadoTag = m.estado_envio === 'fallido'
        ? ' · <span class="rp-estado-fail"><i class="bi bi-exclamation-triangle-fill"></i> no se pudo enviar</span>'
        : ' · <span class="rp-estado-ok"><i class="bi bi-check2-all"></i> enviado</span>';
    }

    // 2026-07-19 (Fable): agrupación -- mismo autor/tipo consecutivo Y misma
    // fecha colapsa el avatar y redondea el borde superior de la burbuja
    // (D2: .rp-msg.interno queda a la izquierda al igual que .rp-msg.in --
    // eso es correcto, no "corregir").
    const prevEsEvento = prev && !['comentario','mensaje','client_message'].includes(prev.tipo);
    const prevCls = (prev && !prevEsEvento)
      ? (prev.tipo==='client_message' ? 'in' : ((prev.es_interno && prev.tipo!=='client_message') ? 'interno'
          : ('out'+((!prev.usuario)?' auto':'')))) : null;
    const prevWho = (prev && !prevEsEvento)
      ? (prev.tipo==='client_message' ? (esc(ticketActual && ticketActual.nombre_contacto || 'Cliente')) : esc(prev.usuario||'Sistema')) : null;
    const esAgrupado = !!(prev && !prevEsEvento && prevCls===cls && prevWho===who && fecha && fecha===fechaPrev);

    const adjs = adjPorMensaje[m.id] || [];
    // Daniel 2026-07-12: "cuando los presione, necesito que salga el modal
    // potente que habiamos hablado" -- antes esto abria en pestaña nueva,
    // inconsistente con el visor universal (#tkLightbox) que YA usa el resto
    // de la pagina. Mismo patron data-lightbox que renderAdjuntosInfo().
    // 2026-07-19 (Fable): adjuntos premium -- miniatura grande + nombre para
    // imágenes, "tile" de icono + nombre + peso para documentos. Mismo href/
    // data-* de siempre (Regla #4.2: el listener más abajo solo lee dataset,
    // sigue funcionando igual).
    const adjHtml = adjs.map(function(a){
      const url = safeUrl(a.archivo_url);
      const esImg = esImagen(a.mime_type, a.archivo_nombre);
      const nom = esc(a.archivo_nombre||'archivo');
      const inner = esImg
        ? '<img src="'+esc(url)+'" loading="lazy" decoding="async"><span class="rp-adj-nom">'+nom+'</span>'
        : '<span class="rp-adj-tile"><i class="bi '+iconoAdjunto(a.mime_type,a.archivo_nombre)+'"></i></span>'
          + '<span class="rp-adj-col"><span class="rp-adj-nom">'+nom+'</span>'
          + (_rpPesoFmt(a.file_size_kb)?'<span class="rp-adj-peso">'+_rpPesoFmt(a.file_size_kb)+'</span>':'')
          + '</span>';
      return '<a class="rp-adj-chip '+(esImg?'es-img':'es-doc')+'" href="'+esc(url)+'" data-lightbox="1" data-url="'+esc(url)+'"'
        + ' data-mime="'+esc(a.mime_type||'')+'" data-nombre="'+esc(a.archivo_nombre||'archivo')+'">'+inner+'</a>';
    }).join('');
    // 'comentario'/'mensaje' YA pasan por _sanitizar_html_mensaje() en el
    // backend antes de guardarse, por eso se renderizan como HTML crudo.
    // 'client_message' es un tipo RESERVADO para Fase 3 (mensajes entrantes
    // de clientes por correo/whatsapp) que aun no sanitiza nada -- mientras
    // no exista ese sanitizador, se escapa como texto plano (fail-safe).
    // 2026-07-19 (Fable): en mensajes del cliente, limpiar placeholders tipo
    // "<Video.mov>" SOLO cuando coinciden con un adjunto real de ESE mensaje
    // (_rpLimpiarPlaceholders) -- el predicado conservador es obligatorio,
    // no una regex genérica (ver comentario del helper).
    const contenidoCrudo = esCliente ? _rpLimpiarPlaceholders(m.contenido||'', adjs) : (m.contenido||'');
    const contenidoHtml = esCliente ? esc(contenidoCrudo) : contenidoCrudo;
    // client_message es texto plano escapado (sin <p>/<br> de Quill) -- sin
    // white-space:pre-wrap los saltos de linea del correo original se
    // colapsaban en una sola linea visual (Daniel 2026-07-12: "no respeta
    // los saltos de pagina"). Solo se aplica a "in" (cliente): los tipos
    // con HTML real (mensaje/comentario) ya traen sus propios <br>/<p>.
    // Traductor 2026-07-12 (Daniel): boton "Traducir" solo en mensajes del
    // cliente/proveedor (cls==='in') -- el texto va en data-texto (mismo
    // esc() que ya usa el resto del bubble, atributo HTML-safe).
    const tradHtml = esCliente
      ? '<button type="button" class="rp-btn-traducir" data-texto="'+esc(m.contenido||'')+'" '
        + 'onclick="tkTraducirMensaje(this,'+m.id+')"><i class="bi bi-translate"></i> Traducir</button>'
        + '<div class="rp-traduccion" id="rpTrad_'+m.id+'" style="display:none"></div>'
      : '';
    // 2026-07-19 (Fable): Copiar -- NUEVO, aditivo (ver window.tkCopiarMensaje).
    const copiarHtml = '<button type="button" class="rp-btn-copiar" onclick="tkCopiarMensaje(this)" aria-label="Copiar mensaje">'
      + '<i class="bi bi-copy"></i> Copiar</button>';
    // meta-mini (mensaje agrupado): sin "·" huérfano (F4) -- solo hora + estado.
    const metaHtml = esAgrupado
      ? '<div class="rp-meta rp-meta-mini">'+esc(_rpHoraDe(m))+estadoTag+'</div>'
      : '<div class="rp-meta"><span class="rp-who">'+who+'</span>'+autoTag+' · '+fmtFechaHora(m.created_at)+estadoTag+'</div>';
    return sep + '<div class="rp-msg '+cls+(esAgrupado?' rp-agrupado':'')+(esNuevo?' rp-nuevo':'')+'">'
      + '<div class="rp-avatar">'+(esAuto?'<i class="bi bi-robot"></i>':_rpIniciales(who))+'</div>'
      + '<div class="rp-bubble">'
      + metaHtml
      + (contenidoHtml ? '<div class="rp-content'+(esCliente?' rp-content-plano':'')+'">'+contenidoHtml+'</div>' : '')
      + (adjHtml? '<div class="rp-adj-lista">'+adjHtml+'</div>':'')
      + tradHtml
      + copiarHtml
      + '</div></div>';
  }).join('');
  // R3: por D1, un mensaje nuevo puede insertarse en MEDIO del hilo -- la
  // marca por id lo anima igual (correcto); si el usuario está scrolleado
  // arriba leyendo historial, el scroll no se mueve hacia él (aceptable,
  // yaEstabaAbajo ya decide eso más abajo).
  _rpUltimoMsgId = maxId;
  // Galería navegable de "Respuestas": Daniel pidió explícitamente "si me
  // envían diez adjuntos, así sean diferentes, pueda pasarlos para verlos
  // todos juntos" — se junta TODAS las imágenes de TODA la conversación
  // visible en el DOM (no solo del mensaje clickeado), en orden cronológico
  // de aparición (= orden del DOM, que ya viene ordenado por fecha).
  th.querySelectorAll('.rp-adj-chip[data-lightbox]').forEach(function(el){
    el.addEventListener('click', function(e){
      e.preventDefault();
      const esImg = esImagen(el.dataset.mime, el.dataset.nombre);
      if(esImg){
        const chipsImg = Array.from(th.querySelectorAll('.rp-adj-chip[data-lightbox]'))
          .filter(function(c){ return esImagen(c.dataset.mime, c.dataset.nombre); });
        const imgs = chipsImg.map(function(c){
          return {url: c.dataset.url, mime: c.dataset.mime, nombre: c.dataset.nombre};
        });
        const idx = chipsImg.indexOf(el);
        tkAbrirLightboxGaleria(imgs, idx >= 0 ? idx : 0);
      } else {
        tkAbrirAdjunto(el.dataset.url, el.dataset.mime, el.dataset.nombre);
      }
    });
  });
  requestAnimationFrame(function(){
    ajustarAltoHilo();
    if(yaEstabaAbajo) th.scrollTop = th.scrollHeight;
  });
}

// ══════════════ Adjuntos — tarjeta "Información" (galería de miniaturas) ══════════════
function iconoAdjunto(mime, nombre){
  const ext = String(nombre||'').split('.').pop().toLowerCase();
  if(/^video\//.test(mime||'')) return 'bi-file-earmark-play';
  if(mime==='application/pdf' || ext==='pdf') return 'bi-file-earmark-pdf';
  if(/word/.test(mime||'') || ['doc','docx'].includes(ext)) return 'bi-file-earmark-word';
  if(/excel|spreadsheet/.test(mime||'') || ['xls','xlsx','csv'].includes(ext)) return 'bi-file-earmark-excel';
  return 'bi-file-earmark';
}

/* ══ 2026-07-19 (Fable) — helpers de la conversación v2 (avatares, agrupación,
   separadores de fecha, limpieza de placeholders de adjuntos). Aditivos: no
   reemplazan nada existente. ══ */
function _rpIniciales(who){
  const p = String(who||'').trim().split(/\s+/);
  return ((p[0]||'?')[0]+(p[1]?p[1][0]:'')).toUpperCase();
}
function _rpPesoFmt(kb){
  kb = Number(kb)||0;
  if(!kb) return '';
  return kb < 1024 ? (kb+' KB') : ((kb/1024).toFixed(1)+' MB');
}
// El backend formatea created_at como 'dd/mm/aaaa HH:MM' hora Chile (Regla #6).
// Si algún día no viniera en ese formato, se degrada sin romper (sin fecha/
// separador, D1: el orden real del hilo es COALESCE(message_date,created_at)
// y no siempre es monótono contra el created_at mostrado).
function _rpFechaDe(m){ // 'dd/mm/yyyy' o '' si el formato no es el esperado
  const s = String(m.created_at||'');
  return /^\d{2}\/\d{2}\/\d{4}/.test(s) ? s.slice(0,10) : '';
}
function _rpHoraDe(m){
  const s = String(m.created_at||'');
  return /^\d{2}\/\d{2}\/\d{4} \d{2}:\d{2}/.test(s) ? s.slice(11,16) : s;
}
function _rpEtiquetaFecha(f){ // f = 'dd/mm/yyyy'
  const hoy = new Date(), ayer = new Date(Date.now()-86400000);
  const fmt = function(d){ return ('0'+d.getDate()).slice(-2)+'/'+('0'+(d.getMonth()+1)).slice(-2)+'/'+d.getFullYear(); };
  if(f === fmt(hoy)) return 'Hoy';
  if(f === fmt(ayer)) return 'Ayer';
  const MES = ['enero','febrero','marzo','abril','mayo','junio','julio','agosto','septiembre','octubre','noviembre','diciembre'];
  const mi = parseInt(f.slice(3,5),10)-1;
  return MES[mi] ? (parseInt(f.slice(0,2),10)+' de '+MES[mi]) : f;
}
// SOLO borra "<nombre.ext>" si coincide EXACTO con un adjunto de ESE mensaje.
// NO relajar a una regex genérica: adjuntos rechazados por tamaño/extensión
// en la ingesta de correo nunca llegan a tk_adjuntos, y en ese caso el
// placeholder es la ÚNICA evidencia de que el cliente envió algo (ver
// crítica adversarial, punto 3) -- destruirla sería una pérdida de datos.
function _rpLimpiarPlaceholders(texto, adjs){
  if(!adjs.length) return texto;
  const nombres = new Set(adjs.map(function(a){ return String(a.archivo_nombre||'').toLowerCase(); }));
  return texto.replace(/<([^<>\n]{1,120}\.[a-z0-9]{2,5})>/gi,
    function(full, nom){ return nombres.has(nom.toLowerCase()) ? '' : full; })
    .replace(/\n{3,}/g,'\n\n').trim();
}

function renderAdjuntosInfo(adjuntos){
  const c = document.getElementById('listaAdjuntosInfo');
  if(!adjuntos || !adjuntos.length){
    c.innerHTML = '<div class="tk-empty-mini"><i class="bi bi-paperclip"></i><span>Sin adjuntos en este ticket.</span></div>';
    return;
  }
  // Agrupar por fecha — el backend ya entrega created_at formateado en hora
  // Chile como "dd/mm/aaaa HH:MM" (Regla #6); usamos solo la parte de fecha
  // como encabezado de grupo, preservando el orden de llegada.
  const grupos = []; const porFecha = {};
  adjuntos.forEach(function(a){
    const fecha = String(a.created_at||'').trim().split(' ')[0] || 'Sin fecha';
    if(!porFecha[fecha]){ porFecha[fecha] = []; grupos.push(fecha); }
    porFecha[fecha].push(a);
  });
  c.innerHTML = grupos.map(function(fecha){
    const items = porFecha[fecha].map(function(a){
      const url = safeUrl(a.archivo_url);
      const esImg = esImagen(a.mime_type, a.archivo_nombre);
      const thumb = esImg ? '<img src="'+esc(url)+'" loading="lazy">' : '<i class="bi '+iconoAdjunto(a.mime_type, a.archivo_nombre)+'"></i>';
      const meta = [a.file_size_kb ? (a.file_size_kb+' KB') : null, a.created_at ? fmtFechaHora(a.created_at) : null]
        .filter(Boolean).join(' · ');
      // TODOS los tipos abren en el visor universal (#tkLightbox) — nada
      // navega a pestaña nueva (pedido explícito de Daniel: "un visor
      // potente, con superpoderes", sin abrir otra página).
      return '<a class="tk-adj-item" href="'+esc(url)+'" data-lightbox="1" data-url="'+esc(url)+'"'
        + ' data-mime="'+esc(a.mime_type||'')+'" data-nombre="'+esc(a.archivo_nombre||'archivo')+'"'
        + ' title="'+esc(a.archivo_nombre||'archivo')+'">'
        + '<div class="tk-adj-thumb">'+thumb+'</div>'
        + '<div class="tk-adj-name">'+esc(a.archivo_nombre||'archivo')+'</div>'
        + (meta?'<div class="tk-adj-meta">'+esc(meta)+'</div>':'')
        + '</a>';
    }).join('');
    return '<div class="tk-adj-grupo-titulo">'+esc(fecha)+'</div><div class="tk-adj-grid">'+items+'</div>';
  }).join('');
  // Galería navegable: TODAS las imágenes de "Información" (no solo la
  // clickeada) — pedido de Daniel ("poder pasarlas para verlas todas
  // juntas"). Si lo clickeado no es imagen (PDF/Word/etc.), se abre solo
  // ese archivo, sin flechas (no tiene sentido navegar entre no-imágenes).
  const imgsInfo = (adjuntos||[])
    .filter(function(a){ return esImagen(a.mime_type, a.archivo_nombre); })
    .map(function(a){ return {url: a.archivo_url, mime: a.mime_type, nombre: a.archivo_nombre||'archivo'}; });
  c.querySelectorAll('.tk-adj-item[data-lightbox]').forEach(function(el){
    el.addEventListener('click', function(e){
      e.preventDefault();
      const esImg = esImagen(el.dataset.mime, el.dataset.nombre);
      if(esImg && imgsInfo.length){
        const idx = imgsInfo.findIndex(function(it){ return it.url === el.dataset.url; });
        tkAbrirLightboxGaleria(imgsInfo, idx >= 0 ? idx : 0);
      } else {
        tkAbrirAdjunto(el.dataset.url, el.dataset.mime, el.dataset.nombre);
      }
    });
  });
}

// ══════════════ Actividad del ticket — línea de tiempo de supervisión ══════════════
// Pedido de Daniel: ver TODO lo que pasó en el ticket (quién lo creó, cambios
// de estado, asignaciones, correos y si llegaron, respuestas del cliente,
// archivos, quién lo vio). Se alimenta de d.mensajes + d.vistas que YA vienen
// con el ticket — filtros y expansión 100% client-side, sin re-fetch.
const ACT_DEFS = {
  creacion:       {icon:'bi-flag-fill',              color:'#dc2626', bg:'#fee2e2'},
  cambio_estado:  {icon:'bi-arrow-left-right',       color:'#3b82f6', bg:'#dbeafe'},
  asignacion:     {icon:'bi-person-check-fill',      color:'#3b82f6', bg:'#dbeafe'},
  mensaje:        {icon:'bi-envelope-fill',          color:'#16a34a', bg:'#dcfce7'},
  client_message: {icon:'bi-chat-dots-fill',         color:'#16a34a', bg:'#dcfce7'},
  comentario:     {icon:'bi-chat-left-text',         color:'#f59e0b', bg:'#fff8e1'},
  archivo:        {icon:'bi-paperclip',              color:'#6b7280', bg:'#f3f4f6'},
  cierre:         {icon:'bi-archive-fill',           color:'#6b7280', bg:'#f3f4f6'},
  reapertura:     {icon:'bi-arrow-counterclockwise', color:'#f59e0b', bg:'#fff8e1'},
  vista:          {icon:'bi-eye',                    color:'#6b7280', bg:'#f3f4f6'},
  otro:           {icon:'bi-dot',                    color:'#6b7280', bg:'#f3f4f6'},
};
const ACT_FILTROS = {
  cambios:  ['creacion','cambio_estado','asignacion','cierre','reapertura'],
  mensajes: ['mensaje','client_message','comentario'],
  archivos: ['archivo'],
};
const ACT_LIMITE = 15;
let actEventos = [];
let actFiltro = 'todo';
let actMostrarTodo = false;

// "dd/mm/aaaa HH:MM" (hora Chile, ya formateada por el server — Regla #6)
// → clave ordenable como string "aaaa-mm-dd HH:MM". Si no calza el formato,
// se devuelve tal cual (queda al fondo del orden, pero no rompe nada).
function actKeyFecha(s){
  const m = String(s||'').trim().match(/^(\d{2})\/(\d{2})\/(\d{4})(.*)$/);
  return m ? (m[3]+'-'+m[2]+'-'+m[1]+m[4]) : String(s||'');
}
// metadata puede venir como string JSON, objeto o null.
function actMeta(raw){
  if(!raw) return {};
  if(typeof raw === 'object') return raw;
  try{ return JSON.parse(raw) || {}; }catch(e){ return {}; }
}
// Extracto de texto plano (~120 chars) desde HTML o texto. DOMParser NO
// ejecuta scripts NI carga recursos (a diferencia de un div temporal con
// innerHTML) — importante porque client_message aún no pasa por sanitizador.
function actExtracto(html){
  if(!html) return '';
  let txt = '';
  try{ txt = new DOMParser().parseFromString(String(html), 'text/html').body.textContent || ''; }
  catch(e){ txt = String(html); }
  txt = txt.replace(/\s+/g,' ').trim();
  return txt.length > 120 ? (txt.slice(0,120)+'…') : txt;
}

function actConstruirEventos(mensajes, vistas){
  const evts = [];
  (mensajes||[]).forEach(function(m, i){
    evts.push({tipo: (ACT_DEFS[m.tipo] ? m.tipo : 'otro'), m: m, key: actKeyFecha(m.created_at), seq: i});
  });
  (vistas||[]).forEach(function(v, i){
    evts.push({tipo:'vista', v: v, key: actKeyFecha(fmtFechaVisto(v.visto_at)), seq: 100000+i});
  });
  // Más reciente ARRIBA; a igual fecha/hora, gana el que se registró después.
  evts.sort(function(a,b){
    if(a.key === b.key) return b.seq - a.seq;
    return a.key < b.key ? 1 : -1;
  });
  return evts;
}

function actEventoHtml(ev){
  let def = ACT_DEFS[ev.tipo] || ACT_DEFS.otro;
  let texto = '', extracto = '', tags = '', fecha = '';
  if(ev.tipo === 'vista'){
    const v = ev.v;
    fecha = fmtFechaVisto(v.visto_at) || '';
    texto = '<b>'+esc(v.usuario||'Alguien')+'</b> abrió el ticket';
  } else {
    const m = ev.m;
    const meta = actMeta(m.metadata);
    fecha = m.created_at || '';
    const who = esc(m.usuario || (ev.tipo==='client_message'
      ? ((ticketActual && ticketActual.nombre_contacto) || 'Cliente') : 'Sistema'));
    switch(ev.tipo){
      case 'creacion':
        texto = '<b>'+who+'</b> creó el ticket';
        extracto = actExtracto(m.contenido);
        break;
      case 'cambio_estado':
        // El contenido ya trae "Estado: X → Y" — va inline, es el dato clave.
        texto = '<b>'+who+'</b> cambió el estado'
          + (m.contenido ? ' · '+esc(actExtracto(m.contenido)) : '');
        break;
      case 'asignacion':
        texto = '<b>'+who+'</b>'
          + (m.contenido ? ' · '+esc(actExtracto(m.contenido)) : ' asignó el ticket');
        break;
      case 'mensaje': {
        const to = m.to_email || meta.to || meta.to_email || '';
        const fallo = m.estado_envio === 'fallido';
        if(fallo) def = {icon: def.icon, color:'#dc2626', bg:'#fee2e2'};
        texto = '<b>'+who+'</b> — Correo a '+(to ? '<b>'+esc(to)+'</b>' : 'cliente');
        if(fallo) tags = '<span class="tk-act-tag env-fail"><i class="bi bi-exclamation-triangle-fill"></i>Fallido</span>';
        else if(m.estado_envio === 'enviado') tags = '<span class="tk-act-tag env-ok"><i class="bi bi-check2-all"></i>Enviado</span>';
        extracto = actExtracto(m.contenido);
        break;
      }
      case 'client_message':
        texto = '<b>'+who+'</b>';
        tags = '<span class="tk-act-tag cliente"><i class="bi bi-chat-dots-fill"></i>El cliente respondió</span>';
        extracto = actExtracto(m.contenido);
        break;
      case 'comentario':
        texto = '<b>'+who+'</b> comentó';
        if(m.es_interno) tags = '<span class="tk-act-tag interno"><i class="bi bi-lock-fill"></i>Interno</span>';
        extracto = actExtracto(m.contenido);
        break;
      case 'archivo':
        texto = '<b>'+who+'</b> subió un archivo'
          + (m.contenido ? ' · '+esc(actExtracto(m.contenido)) : '');
        break;
      case 'cierre':
        texto = '<b>'+who+'</b> cerró el ticket';
        extracto = actExtracto(m.contenido);
        break;
      case 'reapertura':
        texto = '<b>'+who+'</b> reabrió el ticket';
        extracto = actExtracto(m.contenido);
        break;
      default:
        texto = '<b>'+who+'</b>'
          + (m.contenido ? ' · '+esc(actExtracto(m.contenido)) : ' · actividad');
    }
  }
  return '<div class="tk-act-item">'
    + '<div class="tk-act-nodo" style="background:'+def.bg+';color:'+def.color+';"><i class="bi '+def.icon+'"></i></div>'
    + '<div class="tk-act-body"><div class="tk-act-linea">'
    + '<span class="tk-act-texto">'+texto+(tags ? ' '+tags : '')+'</span>'
    + '<span class="tk-act-fecha">'+esc(fecha)+'</span></div>'
    + (extracto ? '<div class="tk-act-extracto">'+esc(extracto)+'</div>' : '')
    + '</div></div>';
}

function actPintarTimeline(){
  const cont = document.getElementById('actTimeline');
  const btn = document.getElementById('actVerTodo');
  if(!cont || !btn) return; // tarjeta oculta (usuario sin permiso admin/superadmin)
  const tipos = ACT_FILTROS[actFiltro];
  const filtrados = tipos ? actEventos.filter(e=>tipos.includes(e.tipo)) : actEventos;
  if(!filtrados.length){
    cont.innerHTML = '<div class="tk-empty-mini"><i class="bi bi-activity"></i><span>Sin actividad registrada'
      + (actFiltro!=='todo' ? ' para este filtro' : '')+'.</span></div>';
    btn.style.display = 'none';
    return;
  }
  const visibles = actMostrarTodo ? filtrados : filtrados.slice(0, ACT_LIMITE);
  cont.innerHTML = visibles.map(actEventoHtml).join('');
  if(!actMostrarTodo && filtrados.length > ACT_LIMITE){
    btn.style.display = 'block';
    btn.innerHTML = '<i class="bi bi-chevron-down me-1"></i>Ver toda la actividad ('+filtrados.length+')';
  } else {
    btn.style.display = 'none';
  }
}

function renderActividad(d){
  if(!PUEDE_VER_ACTIVIDAD) return; // tarjeta ni siquiera se renderizó (Jinja) para este usuario
  actEventos = actConstruirEventos(d.mensajes, d.vistas);
  actPintarTimeline();
}

document.querySelectorAll('#actFiltros .tk-act-chip').forEach(function(ch){
  ch.addEventListener('click', function(){
    document.querySelectorAll('#actFiltros .tk-act-chip').forEach(x=>x.classList.remove('active'));
    ch.classList.add('active');
    actFiltro = ch.dataset.filtro;
    actPintarTimeline();
  });
});
const _actVerTodoBtn = document.getElementById('actVerTodo');
if(_actVerTodoBtn){
  _actVerTodoBtn.addEventListener('click', function(){
    actMostrarTodo = true;
    actPintarTimeline();
  });
}

// ══════════════ Visor universal de adjuntos (imagen / PDF / video / descarga) ══════════════
// Reutiliza esImagen()/iconoAdjunto() ya existentes para detectar el tipo.
function esVideo(mime){ return /^video\//.test(mime||''); }
function esPdfArchivo(mime, nombre){
  const ext = String(nombre||'').split('.').pop().toLowerCase();
  return mime === 'application/pdf' || ext === 'pdf';
}
function esOfficeArchivo(mime, nombre){
  const ext = String(nombre||'').split('.').pop().toLowerCase();
  return /word|excel|spreadsheet|powerpoint|presentation|msword|officedocument/.test(mime||'')
    || ['doc','docx','xls','xlsx','csv','ppt','pptx'].includes(ext);
}
// 2026-07-13 (Daniel): navegación prev/next entre TODAS las imágenes de un
// mismo contexto (galería de "Información" o conjunto de la conversación en
// "Respuestas"), sin perder ningún caller existente de tkAbrirAdjunto().
// _tkLb guarda el set de imágenes navegables + el índice actual.
const _tkLb = { items: [], idx: 0 };

function tkAbrirLightboxGaleria(imagenes, indiceInicial){
  imagenes = Array.isArray(imagenes) ? imagenes.filter(Boolean) : [];
  if(!imagenes.length) return;
  _tkLb.items = imagenes;
  _tkLb.idx = Math.max(0, Math.min(indiceInicial|0, imagenes.length - 1));
  document.getElementById('tkLightbox').style.display = 'flex';
  _tkLbPintarActual();
}
// Alias con el nombre "oficial" pedido para abrir el lightbox compartido con
// un array de imágenes + índice inicial (misma función, mismo comportamiento
// de navegación prev/next — solo se agrega el alias, no se duplica lógica).
function tkAbrirLightbox(imagenes, indiceInicial){ return tkAbrirLightboxGaleria(imagenes, indiceInicial); }
function _tkLbPintarActual(){
  const it = _tkLb.items[_tkLb.idx];
  if(!it) return;
  const url = safeUrl(it.url), mime = it.mime, nombre = it.nombre;
  document.getElementById('tkLightboxNombre').textContent = nombre || 'archivo';
  const descargar = document.getElementById('tkLightboxDescargar');
  descargar.href = url;
  descargar.setAttribute('download', nombre || '');
  const body = document.getElementById('tkLightboxBody');
  if(esImagen(mime, nombre)){
    body.innerHTML = '<img src="'+esc(url)+'" alt="'+esc(nombre||'')+'">';
  } else if(esPdfArchivo(mime, nombre)){
    // Los navegadores modernos renderizan PDF nativamente dentro de un iframe.
    body.innerHTML = '<iframe src="'+esc(url)+'" title="'+esc(nombre||'PDF')+'"></iframe>';
  } else if(esVideo(mime)){
    body.innerHTML = '<video src="'+esc(url)+'" controls></video>';
  } else if(esOfficeArchivo(mime, nombre) && /^https:\/\//.test(url)){
    // Word/Excel/PowerPoint: vista previa real vía Office Online Viewer
    // (Daniel 2026-07-12: "algo bien potente... digno de una empresa que
    // pago diez mil dolares"). /f/<key> es publica (key no-adivinable, sin
    // login), asi que el servicio de Microsoft puede leerla. Si por algun
    // motivo no logra renderizar, el propio iframe de Microsoft muestra su
    // mensaje de error con opcion de descargar -- no se pierde el archivo.
    const src = 'https://view.officeapps.live.com/op/embed.aspx?src=' + encodeURIComponent(url);
    body.innerHTML = '<iframe src="'+esc(src)+'" title="'+esc(nombre||'documento')+'"></iframe>';
  } else {
    // Otros tipos sin previsualizacion posible — estado amigable + Descargar.
    body.innerHTML = '<div class="tk-lightbox-fallback"><i class="bi '+iconoAdjunto(mime, nombre)+'"></i>'
      + '<div class="nombre">'+esc(nombre||'archivo')+'</div>'
      + '<div class="msg">Vista previa no disponible para este tipo de archivo.<br>Usa el botón "Descargar" para abrirlo.</div>'
      + '</div>';
  }
  const total = _tkLb.items.length;
  const nav = document.getElementById('tkLightboxNav');
  const contador = document.getElementById('tkLightboxContador');
  // Con 1 solo elemento no mostramos flechas ni "1/1" (pedido: evitar ruido
  // confuso cuando no hay nada entre qué navegar).
  if(nav) nav.style.display = total > 1 ? 'flex' : 'none';
  if(contador){
    contador.style.display = total > 1 ? '' : 'none';
    contador.textContent = (_tkLb.idx + 1) + '/' + total;
  }
}
function tkLightboxAnterior(){
  if(_tkLb.items.length < 2) return;
  _tkLb.idx = (_tkLb.idx - 1 + _tkLb.items.length) % _tkLb.items.length;
  _tkLbPintarActual();
}
function tkLightboxSiguiente(){
  if(_tkLb.items.length < 2) return;
  _tkLb.idx = (_tkLb.idx + 1) % _tkLb.items.length;
  _tkLbPintarActual();
}
// Compatibilidad hacia atrás: TODOS los callers existentes de un solo
// adjunto siguen funcionando igual (galería de 1 elemento, sin flechas).
function tkAbrirAdjunto(url, mime, nombre){
  tkAbrirLightboxGaleria([{url:url, mime:mime, nombre:nombre}], 0);
}
function tkCerrarLightbox(){
  const _el = document.getElementById('tkLightbox');
  if (!_el) return;   // modo cliente: no existe
  _el.style.display = 'none';
  document.getElementById('tkLightboxBody').innerHTML = '';
  _tkLb.items = []; _tkLb.idx = 0;
}
// 2026-08-10: #tkLightbox y compañía no existen en modo cliente (el
// visor universal de adjuntos es exclusivo del ticket) -- sin este guard
// los 4 addEventListener de abajo tiran TypeError sobre `null` y abortan
// el resto del script.
if (document.getElementById('tkLightbox')){
  document.getElementById('tkLightbox').addEventListener('click', function(e){
    if(e.target === this) tkCerrarLightbox();
  });
  document.getElementById('tkLightboxClose').addEventListener('click', tkCerrarLightbox);
  document.getElementById('tkLightboxPrev').addEventListener('click', function(e){
    e.stopPropagation(); tkLightboxAnterior();
  });
  document.getElementById('tkLightboxNext').addEventListener('click', function(e){
    e.stopPropagation(); tkLightboxSiguiente();
  });
}
document.addEventListener('keydown', function(e){
  if(e.key === 'Escape'){ tkCerrarLightbox(); return; }
  const _tkLbEl = document.getElementById('tkLightbox');
  if(!_tkLbEl || _tkLbEl.style.display === 'none') return;
  if(e.key === 'ArrowLeft') tkLightboxAnterior();
  else if(e.key === 'ArrowRight') tkLightboxSiguiente();
});

// ══════════════ Ejecutivo asignado — select poblado desde /tickets/api/asignables
// (2026-07-12: SOLO usuarios cuyo rol tiene el flag es_ejecutivo/es_tecnico en
// la matriz de permisos de Tickets -- ya no la lista completa de usuarios) ══════
let listaEjecutivos = [];
async function cargarEjecutivos(){
  try{
    const r = await fetch('/tickets/api/asignables');
    listaEjecutivos = await r.json();
    if(!Array.isArray(listaEjecutivos)) listaEjecutivos = [];
  }catch(e){ listaEjecutivos = []; }
  poblarSelectEjecutivo();
}
function poblarSelectEjecutivo(){
  const sel = document.getElementById('selEjecutivo');
  if (!sel) return;   // modo cliente: no hay tarjeta "Estado y gestión"
  const actual = ticketActual ? (ticketActual.asignado_a || '') : '';
  let opts = '<option value="">Sin asignar</option>' + listaEjecutivos.map(function(e){
    return '<option value="'+esc(e.nombre)+'">'+esc(e.nombre)+'</option>';
  }).join('');
  // Si el valor guardado no calza con ningún ejecutivo activo (ej. texto libre
  // histórico o usuario dado de baja), se agrega igual para no perder el dato.
  if(actual && !listaEjecutivos.some(e=>e.nombre===actual)){
    opts += '<option value="'+esc(actual)+'">'+esc(actual)+' (no activo)</option>';
  }
  sel.innerHTML = opts;
  sel.value = actual;
}

// 2026-08-10: #btnGuardar/#btnEqDesdeDoc (y toda la sección "Estado y
// gestión"/"Equipos declarados en el ticket") no existen en modo cliente.
if (document.getElementById('btnGuardar'))
document.getElementById('btnGuardar').addEventListener('click', async function(){
  const selEstadoBtn = document.getElementById('selEstado');
  const payload = {
    prioridad: document.getElementById('selPrio').value,
    asignado_a: document.getElementById('selEjecutivo').value,
  };
  // Si el select está deshabilitado (estado automático de la OT), NO se
  // manda 'estado' en el payload -- ese campo lo controla exclusivamente
  // _tk_set_estado_automatico, y el backend igual lo rechazaría (ver
  // tk_api_update / ESTADO_AUTOMATICO_NO_MANUAL).
  if(!selEstadoBtn.disabled) payload.estado = selEstadoBtn.value;
  const r = await fetch('/tickets/api/tickets/'+TID, {method:'PATCH',
    headers:{'Content-Type':'application/json'}, body:JSON.stringify(payload)});
  const d = await r.json();
  if(d.ok){ ilusToast('✓ Guardado', {type:'success'}); cargar(); } else ilusToast(d.error||'Error', {type:'error'});
});

// ══════════════ Equipos — asignar desde documento ERP (búsqueda avanzada) ══════════════
// Daniel 2026-07-12: reemplaza el flujo viejo de 2 ilusPrompt() (tipo+numero
// a ciegas) por el modal "tka" (mismo componente que list.html, contexto
// 'agregar' -- agrega equipos a ESTE ticket ya existente). Ver
// templates/tickets/_tka_modal.html.
if (document.getElementById('btnEqDesdeDoc'))
document.getElementById('btnEqDesdeDoc').addEventListener('click', function(){
  tkaOpen({
    mode: 'agregar',
    ticketId: TID,
    onDone: cargar,
    rutPrefill: (ticketActual && ticketActual.rut) || '',
  });
});

async function delEquipo(eid){
  const ok = await ilusConfirm({title:'Quitar equipo',message:'¿Quitar este equipo del ticket?',danger:true,okLabel:'Quitar'});
  if(!ok) return;
  const r = await fetch('/tickets/api/tickets/'+TID+'/equipos/'+eid, {method:'DELETE'});
  const d = await r.json(); if(d.ok) cargar(); else ilusToast(d.error||'Error', {type:'error'});
}

// ══════════════ Garantía del equipo (chip por fila + modal PATCH) ══════════════
// Convierte 'YYYY-MM-DD' (formato nativo de <input type=date> y de lo que
// devuelve el backend) a 'dd/mm/aaaa' para mostrar. No es chile_fmt (Regla
// #6, esa es para datetimes UTC) porque estos son DATE puros sin huso horario.
// 2026-07-13 (Daniel, URGENTE): tarjeta de alerta cuando el cliente indica
// garantia -- NUNCA la da por sentada, calcula meses transcurridos desde
// la fecha de emision mas antigua registrada entre los equipos del ticket
// contra el plazo legal chileno (Ley del Consumidor / SERNAC: 6 meses).
function renderGarantiaAlerta(t, equipos){
  const card = document.getElementById('cardGarantiaAlerta');
  if(!t.es_garantia){ card.style.display = 'none'; return; }
  card.style.display = 'block';
  const box = document.getElementById('garantiaCalculoBox');
  const fechas = (equipos||[]).map(e=>e.fecha_emision).filter(Boolean).sort();
  if(!fechas.length){
    box.style.background = '#fff8e1'; box.style.color = '#92400e';
    box.innerHTML = '<i class="bi bi-exclamation-triangle-fill me-1"></i>'
      + 'Sin fecha de emisión registrada en ningún equipo — no se puede calcular el plazo legal todavía. '
      + 'Complétala en la ficha del equipo (ícono junto a cada máquina) con la fecha de la boleta/factura.';
    return;
  }
  const fechaMasAntigua = fechas[0];
  const dEmision = new Date(fechaMasAntigua+'T00:00:00');
  const hoy = new Date();
  const dias = Math.floor((hoy - dEmision) / 86400000);
  const meses = Math.floor(dias / 30.44);
  const LIMITE_MESES = 6; // Ley del Consumidor chilena (SERNAC): garantía legal minima
  if(meses >= LIMITE_MESES){
    box.style.background = '#fee2e2'; box.style.color = '#991b1b';
    box.innerHTML = '<i class="bi bi-x-octagon-fill me-1"></i>'
      + 'Han pasado <b>'+meses+' meses</b> desde la fecha de emisión más antigua ('+fmtFechaISO(fechaMasAntigua)+') — '
      + 'superó el plazo legal de '+LIMITE_MESES+' meses (SERNAC). No hay obligación legal de garantía; '
      + 'si aplica igual, debe ser por una garantía extendida del proveedor, no por ley.';
  } else {
    box.style.background = '#dcfce7'; box.style.color = '#166534';
    box.innerHTML = '<i class="bi bi-check-circle-fill me-1"></i>'
      + 'Han pasado <b>'+meses+' meses</b> desde la fecha de emisión más antigua ('+fmtFechaISO(fechaMasAntigua)+') — '
      + 'dentro del plazo legal de '+LIMITE_MESES+' meses (SERNAC). Igual debe confirmarse con el técnico que la falla '
      + 'corresponde a un defecto de fábrica y no a mal uso.';
  }
}
function fmtFechaISO(iso){
  if(!iso) return '';
  const p = String(iso).split('-');
  return p.length===3 ? (p[2]+'/'+p[1]+'/'+p[0]) : String(iso);
}
// Réplica en JS del cálculo que hace el backend (fecha_emision + N meses,
// recortando el día al último día del mes destino) — solo para la vista
// previa dentro del modal; el valor que realmente queda guardado siempre
// lo calcula y devuelve el servidor en la respuesta del PATCH.
function calcVencimiento(fechaISO, meses){
  if(!fechaISO) return null;
  const p = String(fechaISO).split('-');
  if(p.length!==3) return null;
  const anio0 = +p[0], mes0 = +p[1], dia0 = +p[2];
  const m = +meses || 0;
  const totalMeses = (mes0 - 1) + m;
  const anio = anio0 + Math.floor(totalMeses/12);
  const mes = (totalMeses % 12) + 1;
  const ultimoDia = new Date(anio, mes, 0).getDate(); // día 0 del mes siguiente = último día de "mes"
  const dia = Math.min(dia0, ultimoDia);
  return anio+'-'+String(mes).padStart(2,'0')+'-'+String(dia).padStart(2,'0');
}
// Chip de garantía con clases "gar-chip*" (mismo lenguaje visual que
// Mantenciones) pero calculado con los datos REALES de tk_ticket_equipos
// (con_garantia/fecha_vencimiento/garantia_meses), no la regla fija de 180
// días de Mantenciones (ver contrato: eso no aplica aquí).
function renderGarantiaChip(eq){
  const attrs = 'onclick="abrirGarantia('+eq.id+')" title="Editar ficha del equipo (documento, comentario, garantía)" style="cursor:pointer"';
  if(!eq.con_garantia){
    return '<span class="gar-chip gar-chip-vencida" '+attrs+'><i class="bi bi-shield-slash"></i>Sin garantía</span>';
  }
  if(!eq.fecha_vencimiento){
    return '<span class="gar-chip gar-chip-vencida" '+attrs+'><i class="bi bi-shield-exclamation"></i>Sin registro</span>';
  }
  const hoy = new Date(); hoy.setHours(0,0,0,0);
  const venc = new Date(eq.fecha_vencimiento+'T00:00:00');
  const diasRest = Math.round((venc - hoy)/86400000);
  const vencTxt = esc(fmtFechaISO(eq.fecha_vencimiento));
  if(diasRest < 0){
    return '<span class="gar-chip gar-chip-vencida" '+attrs+'><i class="bi bi-shield-x"></i>Vencida</span>';
  }
  if(diasRest <= 30){
    return '<span class="gar-chip gar-chip-warn" '+attrs+'><i class="bi bi-shield-exclamation"></i>Vence '+vencTxt+'</span>';
  }
  return '<span class="gar-chip gar-chip-ok" '+attrs+'><i class="bi bi-shield-check"></i>Vence '+vencTxt+'</span>';
}
// Emoji por tipo de equipo — equivalente JS de la macro eq_emoji() de
// templates/mantenciones/ficha.html (misma lista de reglas).
function eqEmoji(nombre){
  const n = (nombre||'').toLowerCase();
  if(n.includes('trotad')||n.includes('cinta')||n.includes('treadmill')) return '🏃‍♂️';
  if(n.includes('bicicleta')||n.includes('bike')||n.includes('spinning')) return '🚴‍♂️';
  if(n.includes('eliptic')||n.includes('elliptic')||n.includes('climber')||n.includes('escalad')) return '🪜';
  if(n.includes('remad')||n.includes('rower')||n.includes('row machine')) return '🚣';
  if(n.includes('mancuerna')||n.includes('dumbbell')) return '🏋️‍♂️';
  if(n.includes('kettleb')||n.includes('pesa rusa')) return '🔔';
  if(n.includes('disco')||n.includes('plate')||n.includes('bumper')) return '⚪';
  if(n.includes('barra')||n.includes('bar')) return '━';
  if(n.includes('banco')||n.includes('bench')) return '🪑';
  if(n.includes('rack')||n.includes('jaula')||n.includes('cage')) return '🗄️';
  if(n.includes('jungle')||n.includes('multi')||n.includes('stack')) return '🌳';
  if(n.includes('cable')||n.includes('pulley')||n.includes('pull')) return '🔗';
  if(n.includes('leg')||n.includes('press')) return '🦵';
  if(n.includes('chest')||n.includes('pectoral')) return '💪';
  if(n.includes('shoulder')||n.includes('hombro')) return '🤲';
  if(n.includes('glute')||n.includes('gluteo')) return '🍑';
  if(n.includes('abdomin')||n.includes('abs')||n.includes('sit')) return '🧘';
  if(n.includes('sand bag')||n.includes('sandbag')||n.includes('wall ball')) return '🎯';
  if(n.includes('cuerda')||n.includes('rope')) return '🪢';
  if(n.includes('banda')||n.includes('band')) return '〰️';
  if(n.includes('yoga')||n.includes('mat')||n.includes('colchon')) return '🧘‍♀️';
  if(n.includes('rollo')||n.includes('roller')) return '🛢️';
  if(n.includes('plyo')||n.includes('box')) return '📦';
  if(n.includes('set')) return '📦';
  return '🏋️';
}
function abrirGarantia(eid){
  const eq = (equiposCache||[]).find(x=>x.id===eid);
  if(!eq){ ilusToast('No se encontró el equipo', {type:'error'}); return; }
  garEquipoActual = eq;
  document.getElementById('garEqNombre').textContent = eq.nombre || eq.erp_kopr || ('Equipo #'+eid);
  document.getElementById('garConGarantia').checked = !!eq.con_garantia;
  document.getElementById('garDocumento').value = eq.documento_garantia || '';
  document.getElementById('garNotas').value = eq.notas || '';
  document.getElementById('garFechaEmision').value = eq.fecha_emision || '';
  document.getElementById('garMeses').value = eq.garantia_meses || 6;
  actualizarGarCampos();
  actualizarGarVencPreview();
  bootstrap.Modal.getOrCreateInstance(document.getElementById('modalGarantiaEquipo')).show();
}
function actualizarGarCampos(){
  const on = document.getElementById('garConGarantia').checked;
  const wrap = document.getElementById('garCamposWrap');
  wrap.style.opacity = on ? '1' : '.55';
}
function actualizarGarVencPreview(){
  const fecha = document.getElementById('garFechaEmision').value;
  const meses = +document.getElementById('garMeses').value || 6;
  const venc = calcVencimiento(fecha, meses);
  document.getElementById('garVencPreview').innerHTML = venc
    ? '<i class="bi bi-calendar-check me-1"></i>Vence: <b>'+esc(fmtFechaISO(venc))+'</b>'
    : '<span class="text-muted">Sin fecha de emisión — no se calculará vencimiento.</span>';
}
// 2026-08-10: el modal "Garantía del equipo" (#modalGarantiaEquipo) es
// exclusivo del ticket -- no existe en modo cliente.
if (document.getElementById('garConGarantia')){
  document.getElementById('garConGarantia').addEventListener('change', actualizarGarCampos);
  document.getElementById('garFechaEmision').addEventListener('input', actualizarGarVencPreview);
  document.getElementById('garMeses').addEventListener('input', actualizarGarVencPreview);
  document.getElementById('btnGarGuardar').addEventListener('click', async function(){
    if(!garEquipoActual) return;
    const payload = {
      con_garantia: document.getElementById('garConGarantia').checked,
      documento_garantia: document.getElementById('garDocumento').value.trim(),
      notas: document.getElementById('garNotas').value.trim(),
      fecha_emision: document.getElementById('garFechaEmision').value || '',
      garantia_meses: +document.getElementById('garMeses').value || 6,
    };
    const btn = this; const original = btn.innerHTML;
    btn.disabled = true; btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Guardando…';
    try{
      const r = await fetch('/tickets/api/tickets/'+TID+'/equipos/'+garEquipoActual.id, {method:'PATCH',
        headers:{'Content-Type':'application/json'}, body:JSON.stringify(payload)});
      const d = await r.json();
      btn.disabled = false; btn.innerHTML = original;
      if(d.ok){
        bootstrap.Modal.getOrCreateInstance(document.getElementById('modalGarantiaEquipo')).hide();
        ilusToast('✓ Ficha del equipo actualizada', {type:'success'});
        cargar();
      } else ilusToast(d.error||'Error al guardar la garantía', {type:'error'});
    }catch(e){ btn.disabled = false; btn.innerHTML = original; ilusToast('Sin conexión', {type:'error'}); }
  });
}

// ══════════════ Adjuntos del composer (subida inmediata, se linkean al enviar) ══════════════
// 2026-08-10: #rpFile (clip del composer) no existe en modo cliente.
if (document.getElementById('rpFile'))
document.getElementById('rpFile').addEventListener('change', async function(e){
  const files = Array.from(e.target.files || []);
  this.value = '';
  for(const f of files){ await rpSubirArchivo(f); }
});
// 2026-07-13 (Daniel): pegar una imagen copiada (Ctrl+V) directo en el
// editor de la respuesta debe adjuntarla, igual que si se hubiera elegido
// con el clip 📎 — reutiliza EXACTAMENTE el mismo flujo de subida inmediata
// (rpSubirArchivo → rpAdjuntos → renderRpAdjPreview) para no duplicar lógica.
// 2026-08-10: rpQuill es null en modo cliente (ver constructor guardado
// más arriba) -- no hay editor donde pegar.
if (rpQuill)
rpQuill.root.addEventListener('paste', function(e){
  const items = (e.clipboardData && e.clipboardData.items) || [];
  let imgItem = null;
  for(let i = 0; i < items.length; i++){
    if(/^image\//.test(items[i].type || '')){ imgItem = items[i]; break; }
  }
  if(!imgItem) return; // sin imagen en el portapapeles -- pegar texto normal
  e.preventDefault();
  try{
    const blob = imgItem.getAsFile();
    if(!blob) throw new Error('sin archivo');
    const file = new File([blob], 'pegado-'+Date.now()+'.png', {type: blob.type || 'image/png'});
    rpSubirArchivo(file);
  }catch(err){
    ilusToast('No se pudo pegar la imagen', {type:'error'});
  }
});
async function rpSubirArchivo(file){
  const fd = new FormData(); fd.append('file', file);
  ilusLoader.show('Subiendo '+file.name+'…');
  try{
    const r = await fetch('/tickets/api/tickets/'+TID+'/adjuntos', {method:'POST', body:fd});
    const d = await r.json(); ilusLoader.hide();
    if(!d.ok){ ilusToast(d.error||'No se pudo subir '+file.name, {type:'error'}); return; }
    rpAdjuntos.push({id:d.id, url:d.url, nombre:d.nombre, mime:d.mime||''});
    renderRpAdjPreview();
  }catch(e){ ilusLoader.hide(); ilusToast('Sin conexión — no se pudo subir '+file.name, {type:'error'}); }
}
// 2026-07-12 (Daniel): "no puedo ver los adjuntos que estoy enviando" -- el
// chip de vista previa del composer solo mostraba el nombre, sin forma de
// verlo antes de mandarlo. Ahora abre el MISMO visor universal (#tkLightbox)
// que ya usa el hilo de mensajes (imagenes/PDF/video/Word/Excel/PowerPoint).
function renderRpAdjPreview(){
  const c = document.getElementById('rpAdjPreview');
  c.innerHTML = rpAdjuntos.map((a,i)=>'<span class="chip" style="cursor:pointer" title="Ver adjunto">'
    + '<i class="bi '+iconoAdjunto(a.mime, a.nombre)+'"></i>'+esc(a.nombre)
    + ' <span class="x" title="Quitar" onclick="event.stopPropagation();rpQuitarAdjunto('+i+')">&times;</span></span>').join('');
  c.querySelectorAll('.chip').forEach(function(chip, i){
    chip.addEventListener('click', function(){
      const a = rpAdjuntos[i];
      if (a) tkAbrirAdjunto(a.url, a.mime, a.nombre);
    });
  });
}
window.rpQuitarAdjunto = function(i){ rpAdjuntos.splice(i,1); renderRpAdjPreview(); };

// ══════════════ Plantillas ══════════════
function rpPlantillaItemHtml(p){
  return '<li><div class="rp-pl-row" data-pid="'+p.id+'">'
    + '<a class="dropdown-item rp-pl-nombre" href="#" data-pid="'+p.id+'">'+esc(p.titulo)+'</a>'
    + (p.puede_gestionar ? '<button type="button" class="rp-pl-dots" data-pid="'+p.id+'" aria-label="Opciones de plantilla"><i class="bi bi-three-dots-vertical"></i></button>' : '')
    + '</div></li>';
}

function rpPlantillaCerrarDropdown(){
  const toggle = document.getElementById('rpPlantillasToggle');
  if (toggle) bootstrap.Dropdown.getOrCreateInstance(toggle).hide();
}

async function rpCargarPlantillas(){
  const menu = document.getElementById('rpPlantillasMenu');
  if (!menu) return;   // modo cliente: sin composer, sin dropdown de plantillas
  try{
    const r = await fetch('/tickets/api/plantillas');
    const d = await r.json();
    const pls = (d.ok && d.plantillas) || [];
    const mias = pls.filter(function(p){ return p.propia; });
    const equipo = pls.filter(function(p){ return !p.propia; });
    let html = '<li><h6 class="dropdown-header rp-pl-header">Mis plantillas</h6></li>';
    html += mias.length
      ? mias.map(function(p){ return rpPlantillaItemHtml(p); }).join('')
      : '<li><span class="dropdown-item-text text-muted small">Guarda tu primera plantilla con el botón de al lado</span></li>';
    if (equipo.length){
      html += '<li><h6 class="dropdown-header rp-pl-header">Del equipo</h6></li>';
      html += equipo.map(function(p){ return rpPlantillaItemHtml(p); }).join('');
    }
    menu.innerHTML = html;
    menu.querySelectorAll('a.rp-pl-nombre').forEach(function(a){
      a.addEventListener('click', async function(ev){
        ev.preventDefault();
        const p = pls.find(function(x){ return String(x.id) === a.dataset.pid; });
        if (p){
          // 2026-07-19 (Daniel): la plantilla no debe pisar en silencio lo que
          // el usuario ya escribió en el composer -- si el editor tiene texto,
          // confirmar antes de reemplazar; si está vacío, pegar directo (igual
          // que antes).
          const yaHayTexto = (rpQuill.getText() || '').trim().length > 0;
          if (yaHayTexto){
            const reemplazar = await ilusConfirm({
              title: 'Reemplazar texto',
              message: '¿Reemplazar el texto actual con la plantilla?',
              sub: 'Se perderá lo que ya escribiste en el mensaje.',
              okLabel: 'Reemplazar', cancelLabel: 'Cancelar',
            });
            if (!reemplazar){ rpPlantillaCerrarDropdown(); return; }
            rpQuill.setText('');
          }
          rpQuill.clipboard.dangerouslyPasteHTML(p.cuerpo || '');
        }
        rpPlantillaCerrarDropdown();
      });
    });
    menu.querySelectorAll('.rp-pl-dots').forEach(function(btn){
      btn.addEventListener('click', function(ev){
        ev.preventDefault();
        ev.stopPropagation();
        const p = pls.find(function(x){ return String(x.id) === btn.dataset.pid; });
        if (p) rpPlantillaModoAcciones(btn.closest('.rp-pl-row'), p);
      });
    });
  }catch(e){ menu.innerHTML = '<li><span class="dropdown-item-text text-muted">No se pudieron cargar</span></li>'; }
}

function rpPlantillaModoAcciones(row, p){
  row.innerHTML =
      '<div class="rp-pl-acciones">'
    +   '<button type="button" class="btn btn-sm btn-outline-secondary rp-pl-editar">Editar</button>'
    +   '<button type="button" class="btn btn-sm btn-outline-danger rp-pl-eliminar">Eliminar</button>'
    +   '<button type="button" class="btn-close btn-sm rp-pl-cerrar ms-auto" aria-label="Cerrar"></button>'
    + '</div>';
  row.querySelector('.rp-pl-cerrar').addEventListener('click', function(ev){
    ev.preventDefault(); ev.stopPropagation();
    rpCargarPlantillas();
  });
  row.querySelector('.rp-pl-editar').addEventListener('click', function(ev){
    ev.preventDefault(); ev.stopPropagation();
    rpPlantillaEditar(p);
  });
  row.querySelector('.rp-pl-eliminar').addEventListener('click', function(ev){
    ev.preventDefault(); ev.stopPropagation();
    rpPlantillaEliminar(p);
  });
}

async function rpPlantillaEditar(p){
  const nuevoTitulo = await ilusPrompt({title:'Editar plantilla', message:'Nombre de la plantilla', placeholder:'Nombre de la plantilla', defaultValue:p.titulo, required:true});
  if(!nuevoTitulo){ rpCargarPlantillas(); return; }
  const body = {titulo: nuevoTitulo};
  if (rpQuill.getText().trim()){
    const reemplazar = await ilusConfirm({
      title:'Actualizar contenido',
      message:'¿Reemplazar el contenido de la plantilla con el texto actual del editor?',
      okLabel:'Sí, actualizar', cancelLabel:'Solo renombrar',
    });
    if (reemplazar) body.cuerpo = rpQuill.root.innerHTML;
  }
  try{
    const r = await fetch('/tickets/api/plantillas/'+p.id, {method:'PUT', headers:{'Content-Type':'application/json'}, body:JSON.stringify(body)});
    const d = await r.json();
    if(d.ok){ ilusToast('✓ Plantilla actualizada', {type:'success'}); }
    else ilusToast(d.error||'Error al actualizar', {type:'error'});
  }catch(e){ ilusToast('Sin conexión', {type:'error'}); }
  rpCargarPlantillas();
}

async function rpPlantillaEliminar(p){
  const ok = await ilusConfirm({
    title:'Eliminar plantilla',
    message:'¿Eliminar la plantilla "' + p.titulo + '"?',
    sub:'Esta acción no se puede deshacer.',
    okLabel:'Eliminar', cancelLabel:'Cancelar', danger:true,
  });
  if(!ok){ rpCargarPlantillas(); return; }
  try{
    const r = await fetch('/tickets/api/plantillas/'+p.id, {method:'DELETE'});
    const d = await r.json();
    if(d.ok){ ilusToast('✓ Plantilla eliminada', {type:'success'}); }
    else ilusToast(d.error||'Error al eliminar', {type:'error'});
  }catch(e){ ilusToast('Sin conexión', {type:'error'}); }
  rpCargarPlantillas();
}

rpCargarPlantillas();

// 2026-08-10: composer de Respuestas -- no existe en modo cliente.
if (document.getElementById('rpGuardarPlantilla'))
document.getElementById('rpGuardarPlantilla').addEventListener('click', async function(){
  const cuerpo = rpQuill.root.innerHTML;
  if(!rpQuill.getText().trim()){ ilusToast('Escribe algo antes de guardarlo como plantilla', {type:'warning'}); return; }
  const titulo = await ilusPrompt({title:'Guardar plantilla', message:'Nombre de la plantilla', placeholder:'Ej: Pedir fotos del equipo', required:true});
  if(!titulo) return;
  try{
    const r = await fetch('/tickets/api/plantillas', {method:'POST', headers:{'Content-Type':'application/json'},
      body:JSON.stringify({titulo:titulo, cuerpo:cuerpo})});
    const d = await r.json();
    if(d.ok){ ilusToast('✓ Plantilla guardada', {type:'success'}); rpCargarPlantillas(); }
    else ilusToast(d.error||'Error al guardar', {type:'error'});
  }catch(e){ ilusToast('Sin conexión', {type:'error'}); }
});

// ══════════════ Envío "inteligente" ══════════════
// 2026-08-10: #rpBtnEnviar (enviar respuesta/comentario) no existe en modo
// cliente.
if (document.getElementById('rpBtnEnviar'))
document.getElementById('rpBtnEnviar').addEventListener('click', async function(){
  if(!navigator.onLine){
    ilusToast('Sin conexión a internet. Revisa tu red e intenta de nuevo — tu texto no se perdió.', {type:'error'});
    return;
  }
  const html = rpQuill.root.innerHTML;
  const soloTexto = rpQuill.getText().trim();
  if(!soloTexto && !rpAdjuntos.length){
    ilusToast('Escribe un mensaje o adjunta un archivo', {type:'warning'}); return;
  }
  const btn = this;
  const originalHtml = btn.innerHTML;
  btn.disabled = true; btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Enviando…';

  const adjIds = rpAdjuntos.map(a=>a.id);
  let url, payload;
  if(rpModo === 'interno'){
    url = '/tickets/api/tickets/'+TID+'/comentario';
    payload = {contenido: html, es_interno: true, adjunto_ids: adjIds};
  }else{
    if(rpToChips.hayInvalidos() || rpCcChips.hayInvalidos()){
      ilusToast('Hay un correo mal escrito en "Para" o "CC" (marcado en rojo) — corrígelo o quítalo antes de enviar.', {type:'warning'});
      btn.disabled=false; btn.innerHTML=originalHtml; return;
    }
    const to = document.getElementById('rpTo').value.trim();
    if(!to){ ilusToast('Falta el correo del destinatario ("Para")', {type:'warning'}); btn.disabled=false; btn.innerHTML=originalHtml; return; }
    url = '/tickets/api/tickets/'+TID+'/responder-cliente';
    payload = {contenido: html, to: to, cc: document.getElementById('rpCc').value.trim(), adjunto_ids: adjIds};
  }

  let d, huboErrorRed = false;
  try{
    const r = await fetch(url, {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(payload)});
    d = await r.json();
  }catch(e){
    huboErrorRed = true;
  }
  btn.disabled = false; btn.innerHTML = originalHtml;

  if(huboErrorRed){
    ilusToast('No se pudo enviar: sin conexión o el servidor no respondió. Tu mensaje no se perdió, intenta de nuevo.', {type:'error'});
    return; // conserva texto y adjuntos para reintentar
  }
  if(!d.ok){
    ilusToast(d.error || 'No se pudo enviar', {type:'error'});
    // Si el servidor igual guardo el intento (ver estado_envio='fallido'), refrescamos
    // el hilo para que se vea el mensaje marcado como "no se pudo enviar".
    if(d.mensaje_id){ rpQuill.setText(''); rpAdjuntos = []; renderRpAdjPreview(); cargar(); }
    return;
  }
  rpQuill.setText('');
  rpAdjuntos = []; renderRpAdjPreview();
  ilusToast(rpModo==='interno' ? '✓ Comentario guardado' : '✓ Enviado al cliente', {type:'success'});
  cargar();
});

// ══════════════ Generar OT — adaptación del modal REAL de Mantenciones
// (#modalLevSelector, static/mant_ficha.js) a la ficha de Ticket. Daniel
// rechazó el wizard genérico de 3 pasos anterior (.otw-*) -- este bloque
// lo reemplaza por completo, copiando el MISMO lenguaje visual/JS del
// modal real y adaptando SOLO 2 cosas: (1) la fuente de los equipos, que
// aquí viene de tk_ticket_equipos (equiposCache) en vez de leerse del DOM
// de una ficha de cliente ya renderizada; y (2) la lógica especial de
// "instalación sin ficha" (cliente sin mant_clientes -> se preseleccionan
// TODOS los equipos del ticket, bloqueados).
//
// NOTA (actualizada 2026-07-19 -- Daniel: el comentario anterior quedó
// obsoleto): el submit apunta a POST /tickets/api/tickets/<TID>/generar-ot
// (tickets_module.py, endpoint tk_api_generar_ot ~línea 3724), que YA está
// adaptado al contrato NUEVO que arma tkotGenerar() -- acepta tecnico_ids[]
// plural (o tecnico_user_id como compat), resuelve cliente_id por RUT del
// ticket (creando una ficha mínima en mant_clientes si aún no existe),
// tipo_ot/aplica_garantia/descubrimiento, notas/título con defaults desde
// el propio ticket, etc. Ya está en producción: hay tickets con estado
// "OT GENERADA" creados por este flujo. Si a futuro se detecta un campo
// del payload que el backend no reconoce, es una brecha puntual a
// reportar -- no asumir que el endpoint completo sigue sin adaptar.
// ════════════════════════════════════════════════════════════

// 2026-08-10: TID es una constante Jinja fija -- en tickets/ficha.html es
// {{ ticket_id }} (entero), en mantenciones/ficha.html es `null` (ver
// bloque de scripts, junto a ESTADO_LABEL/TIPO_LABEL). Todo el resto de
// este archivo debe tratar TID===null como "modo cliente, sin ticket":
// los defaults del modal salen de CID/DATA (mant_ficha.js) en vez de
// ticketActual, y el submit postea a un endpoint distinto (ver
// tkotGenerar()). Ningún dato de negocio se duplica -- ambos endpoints
// delegan al mismo _mant_lev_crear_ot_core (app.py).
const _TKOT_MODO_CLIENTE = (typeof TID === 'undefined' || TID === null);

const _TKOT = {
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
  cal: { anio: null, mes: null, cache: {}, error: {}, diaSel: null }, // calendario del mes (Paso 3 · col A)
  // Línea de tiempo del día (Paso 3 · col B). `fecha` = día que se está
  // mirando (normalmente == #levFechaProg; puede diferir si se usan las
  // flechas ‹ › para recorrer un rango multi-día). `choqueKeys` = claves de
  // las visitas que tkotChequearChoque() marcó en conflicto, para pintarlas
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
  _TKOT.cid = null; _TKOT.clienteResuelto = false;
  // Modo cliente: el cliente_id YA se conoce -- es la ficha que estamos
  // viendo (CID, mant_ficha.js). Nada que resolver por RUT.
  if (_TKOT_MODO_CLIENTE){
    _TKOT.cid = CID;
    _TKOT.clienteResuelto = true;
    return;
  }
  const t = ticketActual || {};
  const rut = (t.rut || '').trim();
  const q = rut || (t.empresa || '').trim();
  if(q.length < 2){ _TKOT.clienteResuelto = true; return; }
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
    if(match){ _TKOT.cid = match.id; }
  }catch(e){ console.warn('tkot resolver cliente:', e); }
  _TKOT.clienteResuelto = true;
}

// ── Contactos: solo si hay ficha de cliente (CID). Sin ficha, cae con
//    gracia a contacto 100% manual prellenado con los datos del ticket. ──
async function _tkotCargarContactos(){
  _TKOT.contactos.lista = [];
  if(!_TKOT.cid) return;
  try{
    const r = await fetch('/mantenciones/api/clientes/'+_TKOT.cid+'/contactos');
    const d = await r.json();
    _TKOT.contactos.lista = (d.ok && d.contactos) ? d.contactos : [];
  }catch(e){ console.warn('tkot contactos:', e); }
  _TKOT.contactos.cargados = true;
}
function _tkotRenderContactosSelector(){
  const sel = document.getElementById('levContactoSel');
  if(!sel) return;
  const lista = _TKOT.contactos.lista || [];
  let html = lista.length ? '<option value="">— Selecciona un contacto —</option>' : '<option value="">— Sin contactos registrados —</option>';
  lista.forEach(function(c, i){
    const meta = [c.cargo, c.tel].filter(Boolean).join(' · ');
    html += '<option value="'+i+'">'+esc(c.label || c.nombre)+' — '+esc(c.nombre)+(meta?' ('+esc(meta)+')':'')+'</option>';
  });
  html += '<option value="__manual">+ Ingresar manualmente</option>';
  sel.innerHTML = html;
}
function tkotContactoChange(){
  const sel = document.getElementById('levContactoSel');
  const box = document.getElementById('levContactoBox');
  const v = sel.value;
  if(v === '__manual'){
    box.style.display = '';
    document.getElementById('levContactoNombre').value = '';
    document.getElementById('levContactoCargo').value = '';
    document.getElementById('levContactoTel').value = '';
    document.getElementById('levContactoEmail').value = '';
    sel.dataset.origen = 'manual';
  } else if(v === ''){
    box.style.display = 'none';
    sel.dataset.origen = '';
  } else {
    const idx = parseInt(v);
    const c = _TKOT.contactos.lista[idx];
    if(c){
      box.style.display = '';
      document.getElementById('levContactoNombre').value = c.nombre || '';
      document.getElementById('levContactoCargo').value = c.cargo || '';
      document.getElementById('levContactoTel').value = c.tel || '';
      document.getElementById('levContactoEmail').value = c.email || '';
      sel.dataset.origen = c.origen || 'principal';
    }
  }
  tkotRefreshStepStates();
}
function tkotToggleContactoManual(){
  const sel = document.getElementById('levContactoSel');
  sel.value = '__manual';
  tkotContactoChange();
}

// ── Plantillas activas (idéntico a Mantenciones) ──
async function _tkotCargarPlantillas(){
  if(_TKOT.plantillas.cargadas) return _TKOT.plantillas.all;
  try{
    const r = await fetch('/mantenciones/api/plantillas?activa=1');
    const d = await r.json();
    _TKOT.plantillas.all = Array.isArray(d) ? d : (d.plantillas || []);
    _TKOT.plantillas.cargadas = true;
  }catch(e){ console.warn('tkot plantillas:', e); }
  return _TKOT.plantillas.all;
}

// FIX 2026-08-11 (Daniel, probando en vivo): mapa tipo_ot -> categoria,
// para filtrar "Plantillas extra" por la categoría del tipo de OT elegido.
// Best-effort: si falla, _TKOT.categoriaMap queda null y el caller
// (tkotAbrirMultiPlantilla) cae de vuelta a mostrar todas sin filtrar --
// nunca bloquea agregar una plantilla extra por un problema de red.
async function _tkotCargarCategoriaMap(){
  if(_TKOT.categoriaMap) return _TKOT.categoriaMap;
  try{
    const r = await fetch('/mantenciones/api/plantillas/categorias');
    const d = await r.json();
    if(d && d.ok && d.mapa_tipo_ot) _TKOT.categoriaMap = d.mapa_tipo_ot;
  }catch(e){ console.warn('tkot categoria map:', e); }
  return _TKOT.categoriaMap;
}

// ── Técnicos (multi-select, idéntico a Mantenciones) ──
function tkotRenderTecnicos(){
  const box = document.getElementById('levTecnicosBox');
  if(!box) return;
  const techs = _TKOT.tecnicosDisponibles || [];
  if(!techs.length){
    box.innerHTML = '<div class="alert alert-warning py-2 mb-0 small w-100">'
      + '<i class="bi bi-exclamation-triangle me-1"></i>No hay técnicos activos. '
      + 'Solicita a un administrador que cree un usuario con rol "Técnico".</div>';
    document.getElementById('levTecCount').textContent = '0';
    return;
  }
  const fechaProg = document.getElementById('levFechaProg')?.value || '';
  box.innerHTML = techs.map(function(t){
    const isSel = _TKOT.tecnicosSel.has(t.id);
    const bg = isSel ? 'background:linear-gradient(135deg,#1e40af,#3b82f6);color:#fff;border-color:#1e40af' : 'background:#fff;color:#0f172a;border-color:#cbd5e1';
    const icon = isSel ? 'bi-check-circle-fill' : 'bi-person';
    return '<span class="badge rounded-pill border" style="cursor:pointer;padding:.5rem .85rem;font-size:.82rem;font-weight:500;'+bg+'" '
      + 'onclick="tkotToggleTecnico('+t.id+')"><i class="bi '+icon+' me-1"></i>'+esc(t.nombre || t.email || ('Téc #'+t.id))
      + _tkotTecCargaChip(t.id, fechaProg) + '</span>';
  }).join('');
  document.getElementById('levTecCount').textContent = String(_TKOT.tecnicosSel.size);
}

// ── §4B "Carga del día por técnico, donde se decide": mini-contador de OTs
//    que ese técnico YA tiene en la fecha programada actual (mismo cache del
//    mes que alimenta el timeline -- cero backend). Sufijo "· N ese día":
//    gris con 1, ámbar con 2, rojo con 3+. Se refresca solo (tkotRenderTecnicos
//    se llama de nuevo) cada vez que cambia #levFechaProg -- ver tkotCalSelDia. ──
function _tkotTecCargaChip(tecnicoId, fecha){
  if(!fecha) return '';
  const key = _tkotCalKey(parseInt(fecha.slice(0, 4), 10), parseInt(fecha.slice(5, 7), 10));
  const mapa = _TKOT.cal.cache[key] || {};
  const visitas = mapa[fecha] || [];
  const n = visitas.filter(function(v){
    return String(v.tecnico_id) === String(tecnicoId) && String(v.estado || '').toLowerCase() !== 'cancelada';
  }).length;
  if(!n) return '';
  let colores;
  if(n >= 3) colores = 'background:#fee2e2;color:#991b1b';        // rojo (REGLA #2)
  else if(n === 2) colores = 'background:#fff8e1;color:#b45309';  // ámbar (REGLA #2)
  else colores = 'background:#f3f4f6;color:#6b7280';              // gris (REGLA #2)
  return '<span class="tkot-tec-carga" style="' + colores + '">· ' + n + ' ese día</span>';
}
function tkotToggleTecnico(tid){
  if(_TKOT.tecnicosSel.has(tid)) _TKOT.tecnicosSel.delete(tid); else _TKOT.tecnicosSel.add(tid);
  tkotRenderTecnicos();
  // El bloque "Tu OT" rotula "Nueva OT · N técnicos" -> se refresca al toque.
  if(typeof tkdayRenderMine === 'function') tkdayRenderMine();
  tkotChequearChoqueDebounced();
  tkotRefreshStepStates();
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

async function tkotCalCargarMes(){
  const a = _TKOT.cal.anio, m = _TKOT.cal.mes;
  const titEl = document.getElementById('levCalTitulo');
  if(titEl) titEl.textContent = (TKCAL_MES_ES[m - 1] || '') + ' ' + a;
  const key = _tkotCalKey(a, m);
  if(_TKOT.cal.cache[key]){
    tkotCalRenderGrid();
    if(_TKOT.cal.diaSel && _TKOT.cal.diaSel.slice(0, 7) === key) tkdayRender(_TKOT.cal.diaSel, { silencio: true });
    return;
  }
  const grid = document.getElementById('levCalGrid');
  if(grid) grid.innerHTML = '<div class="text-muted small text-center py-3" style="grid-column:1/-1">Cargando…</div>';
  try{
    const r = await fetch('/mantenciones/api/calendario/mes/' + a + '/' + m);
    if(!r.ok) throw new Error('http ' + r.status);
    const d = await r.json();
    _TKOT.cal.cache[key] = _tkotCalNormalizarMes(d);
  }catch(e){
    console.warn('tkot calendario mes:', e);
    _TKOT.cal.cache[key] = {};
    _TKOT.cal.error[key] = true;
    if(grid) grid.innerHTML = '<div class="text-muted small text-center py-3" style="grid-column:1/-1">'
      + '<i class="bi bi-calendar-x me-1"></i>No se pudo cargar el calendario de este mes.</div>';
    tkdayRender(_TKOT.cal.diaSel, { silencio: true });
    return;
  }
  delete _TKOT.cal.error[key];
  tkotCalRenderGrid();
  // El timeline NO se borra al navegar de mes: si el día que se está
  // mirando pertenece al mes recién cargado, se repinta con los datos frescos.
  if(_TKOT.cal.diaSel && _TKOT.cal.diaSel.slice(0, 7) === key) tkdayRender(_TKOT.cal.diaSel, { silencio: true });
}

// ── Reintento del estado de error (§8c): purga el caché del mes y recarga. ──
function tkotCalReintentar(){
  delete _TKOT.cal.cache[_tkotCalKey(_TKOT.cal.anio, _TKOT.cal.mes)];
  delete _TKOT.cal.error[_tkotCalKey(_TKOT.cal.anio, _TKOT.cal.mes)];
  tkotCalCargarMes();
}

function tkotCalMes(delta){
  let m = _TKOT.cal.mes + delta, a = _TKOT.cal.anio;
  if(m > 12){ m = 1; a++; } else if(m < 1){ m = 12; a--; }
  _TKOT.cal.anio = a; _TKOT.cal.mes = m;
  // Se MANTIENE la selección (_TKOT.cal.diaSel) y el timeline no se borra:
  // navegar de mes es solo mirar, no deselecciona lo agendado.
  tkotCalCargarMes();
}

function tkotCalHoy(){
  const hoy = new Date();
  _TKOT.cal.anio = hoy.getFullYear(); _TKOT.cal.mes = hoy.getMonth() + 1;
  tkotCalCargarMes().then(function(){
    const f = hoy.getFullYear() + '-' + String(hoy.getMonth() + 1).padStart(2, '0') + '-' + String(hoy.getDate()).padStart(2, '0');
    tkotCalClicDia(f);   // pasa por #levFechaProg: el calendario manda para el día
  });
}

// ── Rango multi-día activo (#levRangoDias + #levFechaFin) para pintar el
//    "pill continuo" en el mini-calendario. Compara fechas como STRING
//    'YYYY-MM-DD' (no índices) para que el pintado siga funcionando cuando
//    el rango cruza de mes y se navega con ‹ ›.
function _tkotRangoActivo(){
  const on = document.getElementById('levRangoDias')?.checked;
  if(!on) return null;
  const ini = document.getElementById('levFechaProg')?.value || '';
  const fin = document.getElementById('levFechaFin')?.value || '';
  if(!ini || !fin || fin < ini) return null;
  return { ini: ini, fin: fin };
}

function tkotCalRenderGrid(){
  const a = _TKOT.cal.anio, m = _TKOT.cal.mes;
  const mapa = _TKOT.cal.cache[_tkotCalKey(a, m)] || {};
  const grid = document.getElementById('levCalGrid');
  if(!grid) return;
  const primerDia = new Date(a, m - 1, 1);
  const nDias = new Date(a, m, 0).getDate();
  let offset = primerDia.getDay() - 1; // lunes = 0
  if(offset < 0) offset = 6;
  const hoy = new Date();
  const hoyStr = hoy.getFullYear() + '-' + String(hoy.getMonth() + 1).padStart(2, '0') + '-' + String(hoy.getDate()).padStart(2, '0');
  const fechaProg = document.getElementById('levFechaProg')?.value || '';
  const rango = _tkotRangoActivo();
  let html = '';
  for(let i = 0; i < offset; i++) html += '<div class="tkcal-day blank"></div>';
  for(let dia = 1; dia <= nDias; dia++){
    const fecha = a + '-' + String(m).padStart(2, '0') + '-' + String(dia).padStart(2, '0');
    const visitas = mapa[fecha] || [];
    const dow = new Date(a, m - 1, dia).getDay();
    const clases = ['tkcal-day'];
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
    if(fecha === _TKOT.cal.diaSel && fecha !== fechaProg) clases.push('vista');
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
    html += '<div class="' + clases.join(' ') + '" onclick="tkotCalClicDia(\'' + fecha + '\')"'
      + ' onkeydown="if(event.key===\'Enter\'||event.key===\' \'){event.preventDefault();tkotCalClicDia(\'' + fecha + '\');}"'
      + ' role="button" tabindex="0"'
      + ' aria-label="' + esc(fecha + ' — ' + (visitas.length ? (visitas.length + ' OT agendada(s)') : 'sin OTs agendadas')) + '">'
      + '<span class="num">' + dia + '</span>'
      + (visitas.length ? '<span class="cnt' + (visitas.length >= TKCAL_UMBRAL_ROJO ? '' : visitas.length >= TKCAL_UMBRAL_AMBAR ? ' tkcal-carga-ambar' : ' tkcal-carga-normal') + '">' + visitas.length + '</span>' : '')
      + dots
      + '</div>';
  }
  grid.innerHTML = html;
}

// ── Clic en un día del mini-calendario: el calendario MANDA para el día.
//    Setea #levFechaProg y dispara su evento change para que TODA la lógica
//    existente (tkotFechaProgChange → tkotCalSelDia + choque) reaccione
//    exactamente igual que si el usuario hubiera tipeado la fecha.
function tkotCalClicDia(fecha){
  const fp = document.getElementById('levFechaProg');
  if(!fp){ tkotCalSelDia(fecha); return; }
  fp.value = fecha;
  fp.dispatchEvent(new Event('change', { bubbles: true }));
}

// ── Selección de día: repinta la grilla + la LÍNEA DE TIEMPO del día
//    (superficie principal desde el rediseño 2026-07-15) y mantiene
//    #levCalDetalle relleno como FALLBACK oculto (REGLA #4.2: la lista
//    textual sigue existiendo en el DOM; solo dejó de ser la vista visible
//    porque los bloques por hora dan la misma info y más).
function tkotCalSelDia(fecha, opts){
  opts = opts || {};
  _TKOT.cal.diaSel = fecha;
  tkotCalRenderGrid();
  tkdayRender(fecha, { scroll: !opts.silencio });
  // §4B: la fecha programada es la que manda el conteo de carga por técnico
  // (chips del Paso 4) -- se refresca aquí, único punto de paso de todo
  // cambio de día (clic en el mini-calendario, tipeo directo, "Hoy",
  // apertura del modal y el nuevo tkdaySugerirHueco()).
  tkotRenderTecnicos();
  const mapa = _TKOT.cal.cache[_tkotCalKey(_TKOT.cal.anio, _TKOT.cal.mes)] || {};
  const visitas = mapa[fecha] || [];
  const det = document.getElementById('levCalDetalle');
  if(!det) return;
  const partes = fecha.split('-');
  const fechaFmt = partes.length === 3 ? (partes[2] + '/' + partes[1] + '/' + partes[0]) : fecha;
  let html = '<div class="tkcal-detalle-titulo"><i class="bi bi-calendar3 me-1" style="color:#dc2626"></i>' + fechaFmt + '</div>';
  if(!visitas.length){
    html += '<div class="tkcal-det-empty">Sin OTs agendadas este día.</div>';
  } else {
    visitas.forEach(function(v){
      const tec = esc(v.tecnico_nombre || v.tecnico || 'Sin técnico asignado');
      const hi = v.hora_inicio || '', hf = v.hora_fin || '';
      const num = esc(v.numero_ot || ('OT #' + (v.id || v.visita_id || '?')));
      html += '<div class="tkcal-det-item"><span class="hora">' + (hi || '--') + (hf ? '–' + hf : '') + '</span>'
        + '<span class="who">' + tec + '</span><span class="num">' + num + '</span></div>';
    });
  }
  det.innerHTML = html;
}

// ── Se llama al abrir el modal (reset) y cada vez que cambia la fecha
//    programada, para que el calendario "salte" al mes correspondiente. ──
function tkotCalInit(){
  const fp = document.getElementById('levFechaProg')?.value || '';
  const base = fp ? new Date(fp + 'T00:00:00') : new Date();
  _TKOT.cal.anio = base.getFullYear();
  _TKOT.cal.mes = base.getMonth() + 1;
  _TKOT.cal.diaSel = null;
  _TKOT.cal.cache = {};
  _TKOT.cal.error = {};
  _TKOT.day.fecha = null;
  _TKOT.day.rejilla = false;
  _TKOT.day.choque = false;
  _TKOT.day.choqueFecha = null;
  _TKOT.day.choqueKeys = null;
  _TKOT.day.visitas = [];
  const det = document.getElementById('levCalDetalle');
  if(det) det.style.display = 'none';
  const warn = document.getElementById('levCalWarnChoque');
  if(warn) warn.style.display = 'none';
  tkdayCerrarDetalle();
  tkdayRender(null, { silencio: true });   // estado (b) "toca un día" mientras carga
  tkotCalCargarMes().then(function(){
    if(fp) tkotCalSelDia(fp, { silencio: true });
  });
}

function tkotFechaProgChange(){
  // Chips de duración (2026-07-19, P4): re-anclaje SILENCIOSO de #levFechaFin
  // ANTES de cualquier render -- así tkotCalSelDia ya pinta el pill con la
  // fin correcta (evita el parpadeo fin<inicio del P1/P4).
  _levReanclarFin();
  const v = document.getElementById('levFechaProg')?.value || '';
  if(v){
    const y = parseInt(v.slice(0, 4)), m = parseInt(v.slice(5, 7));
    if(y && m && (y !== _TKOT.cal.anio || m !== _TKOT.cal.mes)){
      _TKOT.cal.anio = y; _TKOT.cal.mes = m;
      tkotCalCargarMes().then(function(){ tkotCalSelDia(v, { silencio: true }); });
    } else {
      tkotCalSelDia(v, { silencio: true });
    }
  } else {
    _TKOT.cal.diaSel = null;
    tkotCalRenderGrid();
    tkdayRender(null, { silencio: true });
  }
  levChipsRefresh();
  tkotChequearChoqueDebounced();
}

// ── Edición fina del horario: el bloque "Tu OT" se mueve EN VIVO (local,
//    sin red) en cada `input`, y el chequeo de choque va aparte con su
//    debounce de 500ms ya existente. ──
function tkotHoraInput(){
  tkdayRenderMine();
  tkotChequearChoqueDebounced();
}

// ── Toggle del rango multi-día / cambio de #levFechaFin: repinta el "pill"
//    del calendario y el chip "Día X de N" + el bloque propio. ──
function tkotRangoChange(){
  tkotCalRenderGrid();
  tkdayRender(_TKOT.cal.diaSel, { silencio: true });
  // Chips de duración (2026-07-19): único punto de espejo estado→chips.
  // Cubre toggle manual, fin manual y chips en un solo lugar (P2).
  levChipsRefresh();
}

// ════════════════════════════════════════════════════════════
// Chips de duración (2026-07-19): [1 día][2 días][3 días][5 días][Otro…]
// junto al Paso 3 "Agenda". Un tap = extender la OT a N días sin tocar el
// toggle "¿Se extenderá más de un día?" a mano. Regla única de sincronía:
// mientras exista un rango válido, la duración SIEMPRE viaja con la fecha
// de inicio (venga de chip o de "Otro…") -- sin flag de modo aparte (P2).
// Los chips SOLO escriben en #levRangoDias/#levFechaFin/#levHoraIniFin/
// #levHoraFinFin, exactamente lo que ya lee tkotGenerar() -- cero cambios
// de backend. Portado 1:1 desde templates/mantenciones/ficha.html +
// static/mant_ficha.js (allá con _LEV_MODAL en vez de _TKOT).
// ────────────────────────────────────────────────────────────

// NUEVO helper: _fmtYMD no existe en el repo (grep: 0 matches).
function _levFmtYMD(d){
  return d.getFullYear() + '-' + String(d.getMonth() + 1).padStart(2, '0') + '-' + String(d.getDate()).padStart(2, '0');
}

// Escritura SILENCIOSA de #levFechaFin = #levFechaProg + (durN-1) días
// corridos. No renderiza nada -- el llamador decide cuándo repintar.
function _levReanclarFin(){
  const n = _TKOT.durN;
  if(!(typeof n === 'number' && n > 1)) return;
  const v = document.getElementById('levFechaProg')?.value || '';
  if(!v) return;
  const p = v.split('-');
  const d = new Date(parseInt(p[0], 10), parseInt(p[1], 10) - 1, parseInt(p[2], 10));
  d.setDate(d.getDate() + (n - 1));   // días CORRIDOS, findes incluidos
  document.getElementById('levFechaFin').value = _levFmtYMD(d);
  document.getElementById('levRangoDias').checked = true;
  document.getElementById('levFechaFinWrap').style.display = '';
}

// Tap en un chip numérico [1|2|3|5].
function levDurSet(n){
  const fp = document.getElementById('levFechaProg');
  if(!fp || !fp.value){ ilusToast('Elige primero la fecha de inicio', { type: 'warning' }); return; }
  const chk = document.getElementById('levRangoDias');
  const wrap = document.getElementById('levFechaFinWrap');
  const veniaDe1 = !chk.checked;   // P3: solo heredar horas en la transición 1→N
  _TKOT.durN = n;
  if(n <= 1){
    chk.checked = false; wrap.style.display = 'none';
    document.getElementById('levFechaFin').value = '';
  } else {
    _levReanclarFin();
    if(veniaDe1){   // heredar horas SOLO al abrir el rango, nunca entre chips N→M (P3)
      document.getElementById('levHoraIniFin').value = document.getElementById('levHoraIni').value || '09:00';
      document.getElementById('levHoraFinFin').value = document.getElementById('levHoraFin').value || '13:00';
    }
  }
  tkotRangoChange();               // reusa pill + "Día X de N" + tkdayRenderMine + levChipsRefresh
  tkotChequearChoqueDebounced();   // el choque ya manda fecha_fin
}

// Tap en "Otro…": abre el panel ámbar existente para que el usuario tipee
// la fecha de término a mano. No fuerza ninguna duración.
function levDurOtro(){
  document.getElementById('levRangoDias').checked = true;
  document.getElementById('levFechaFinWrap').style.display = '';
  _TKOT.durN = 'otro';
  tkotRangoChange();
  document.getElementById('levFechaFin').focus();
}

// Único espejo estado→chips: recalcula durN desde el DOM real y repinta
// clases/aria-pressed + el texto "→ hasta …". Se llama al final de
// tkotRangoChange/tkotFechaProgChange/_tkdayIrADia/reset del modal.
function levChipsRefresh(){
  const cont = document.getElementById('levDurChips');
  if(!cont) return;
  const r = _tkotRangoActivo();
  let n = 1;
  if(r) n = Math.round((new Date(r.fin + 'T00:00:00') - new Date(r.ini + 'T00:00:00')) / 86400000) + 1;
  else if(document.getElementById('levRangoDias')?.checked) n = 'otro';
  _TKOT.durN = n;
  cont.querySelectorAll('.lev-dur-chip').forEach(function(b){
    const act = String(b.dataset.n) === String(n)
      || (b.dataset.n === 'otro' && n !== 1 && [2, 3, 5].indexOf(n) === -1);
    b.classList.toggle('act', act);
    b.setAttribute('aria-pressed', act ? 'true' : 'false');
  });
  const h = document.getElementById('levDurHasta');
  if(h) h.innerHTML = r ? ('→ hasta <b>' + esc(_tkdayFechaLarga(r.fin)) + '</b>')
    : (n === 'otro' ? 'elige la fecha de término' : '');
}

// ── Flechas ‹ › del día: recorren el día que muestra la línea de tiempo SIN
//    tocar #levFechaProg (así se puede inspeccionar el día 2, 3, 4… de un
//    rango multi-día, o simplemente espiar el día siguiente antes de decidir).
function tkdayPaso(delta){
  const base = _TKOT.day.fecha || document.getElementById('levFechaProg')?.value || '';
  if(!base) return;
  const p = base.split('-');
  const d = new Date(parseInt(p[0], 10), parseInt(p[1], 10) - 1, parseInt(p[2], 10));
  d.setDate(d.getDate() + delta);
  const f = d.getFullYear() + '-' + String(d.getMonth() + 1).padStart(2, '0') + '-' + String(d.getDate()).padStart(2, '0');
  const y = d.getFullYear(), m = d.getMonth() + 1;
  _TKOT.cal.diaSel = f;
  if(y !== _TKOT.cal.anio || m !== _TKOT.cal.mes){
    _TKOT.cal.anio = y; _TKOT.cal.mes = m;
    tkotCalCargarMes();    // al terminar repinta grilla + timeline (ver tkotCalCargarMes)
  } else {
    tkotCalRenderGrid();
    tkdayRender(f, { silencio: true });
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
// DATOS: el MISMO caché _TKOT.cal.cache que llena _tkotCalNormalizarMes con
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
// y solo se veia 1). Ver _tkdayLayout/tkdayRender.
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
// Helper PURO: solo lee _TKOT.cal.cache (ya cargado por tkotCalCargarMes) y
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
  const mapa = _TKOT.cal.cache[key] || {};
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
  if(_TKOT.day.rejilla) return;
  const rej = document.getElementById('levDayRejilla');
  const gut = document.getElementById('levDayGutter');
  if(!rej || !gut) return;
  let r = '', g = '';
  for(let h = 8; h <= 20; h++){
    const y = (h - 8) * 48;
    r += '<div class="tkday-hline" style="top:' + y + 'px"></div>';
    if(h < 20) r += '<div class="tkday-hline half" style="top:' + (y + 24) + 'px"></div>';
    g += '<div class="tkday-hlbl" style="top:' + (y - 7) + 'px">' + String(h).padStart(2, '0') + ':00</div>';
  }
  rej.innerHTML = r;
  gut.innerHTML = g;
  _TKOT.day.rejilla = true;
}

// ── Línea "ahora": solo si el día mirado es hoy y la hora ∈ [08:00, 20:00]. ──
function _tkdayNow(fecha){
  const el = document.getElementById('levDayNow');
  if(!el) return;
  if(!fecha || fecha !== _tkdayHoyStr()){ el.innerHTML = ''; return; }
  const n = new Date();
  const min = n.getHours() * 60 + n.getMinutes();
  if(min < TKDAY_START || min > TKDAY_END){ el.innerHTML = ''; return; }
  el.innerHTML = '<div class="tkday-now" style="top:' + ((min - TKDAY_START) * TKDAY_PXMIN) + 'px"'
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
  const ghost = document.getElementById('levDayGhost');
  const scroll = document.getElementById('levDayScroll');
  const mine = document.getElementById('levDayMine');
  if(!ghost || !scroll) return;
  const fecha = _TKOT.day.fecha;
  const hayBloques = (_TKOT.day.visitas || []).length > 0;
  const hayMine = !!(mine && mine.style.display !== 'none');
  scroll.classList.toggle('sin-dia', !fecha);
  const key = fecha ? fecha.slice(0, 7) : null;
  if(fecha && _TKOT.cal.error[key]){
    // (c) error de API — el modal jamás se rompe
    ghost.innerHTML = '<i class="bi bi-calendar-x" style="color:#fca5a5"></i>'
      + '<div class="g1">No se pudo cargar la agenda de este mes</div>'
      + '<div class="g-btn"><button type="button" class="btn btn-sm btn-outline-secondary" onclick="tkotCalReintentar()">'
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
function tkdayRender(fecha, opts){
  opts = opts || {};
  const canvas = document.getElementById('levDayCanvas');
  const blks = document.getElementById('levDayBlks');
  if(!canvas || !blks) return;
  _tkdayRejilla();
  _TKOT.day.fecha = fecha || null;

  // ── Header del día ──
  const elF = document.getElementById('levDayFecha');
  const elC = document.getElementById('levDayCnt');
  const elR = document.getElementById('levDayChipRango');
  const key = fecha ? _tkotCalKey(parseInt(fecha.slice(0, 4), 10), parseInt(fecha.slice(5, 7), 10)) : null;
  const mapa = key ? (_TKOT.cal.cache[key] || {}) : {};
  const visitas = fecha ? (mapa[fecha] || []) : [];
  _TKOT.day.visitas = visitas;
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
  const prevB = document.getElementById('levDayPrev'), nextB = document.getElementById('levDayNext');
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
  const choqueKeys = _TKOT.day.choqueKeys;
  const aplicaChoque = !!(choqueKeys && _TKOT.day.choqueFecha && _TKOT.day.choqueFecha === fecha);
  let html = '';
  puestos.forEach(function(p){
    const w = 'calc(' + (100 / p.nCols) + '% - 4px)';
    const l = (p.col * (100 / p.nCols)) + '%';
    if(p.grupo){
      // Resumen "+N más" (cluster con más de 4 columnas)
      const gg = _tkdayGeom(_tkdayHHMM(p.ini), _tkdayHHMM(p.fin));
      const h = gg ? gg.height : 18;
      html += '<div class="tkday-blk blk-mas ' + _tkdayClase(h) + '" role="button" tabindex="0"'
        + ' style="top:' + (gg ? gg.top : 0) + 'px;height:' + h + 'px;left:' + l + ';width:' + w + ';'
        + 'border-color:#e5e7eb;border-left-color:#9ca3af"'
        + ' data-grupo="' + esc(p.grupo.join(',')) + '"'
        + ' aria-label="' + esc('+' + p.grupo.length + ' OTs más en esta franja') + '"'
        + ' onclick="tkdayVerBloque(this)"'
        + ' onkeydown="if(event.key===\'Enter\'||event.key===\' \'){event.preventDefault();tkdayVerBloque(this);}">'
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
    html += '<div class="tkday-blk ' + cls + ' ' + extra.join(' ') + '" role="button" tabindex="0"'
      + ' style="top:' + g.top + 'px;height:' + g.height + 'px;left:' + l + ';width:' + w + ';'
      + 'border-color:' + pal[0] + ';border-left-color:' + pal[0] + ';background:' + pal[1] + '"'
      + ' data-idx="' + p.idx + '"'
      + ' aria-label="' + esc(aria) + '"'
      + ' onclick="tkdayVerBloque(this)"'
      + ' onkeydown="if(event.key===\'Enter\'||event.key===\' \'){event.preventDefault();tkdayVerBloque(this);}">'
      + _tkdayInner(cls, {
          borde: pal[0], who: who, num: num, hi: hi, tipo: v.tipo,
          rango: hi + (hf ? '–' + hf : ''), ini2: _tkdayIniciales(who), done: done,
        })
      + '</div>';
  });
  blks.innerHTML = html;
  _tkdayNow(fecha);
  tkdayRenderMine();
  _tkdayAutoScroll();
}

// ── §1.1 "clic para crear": clic/tap en una zona VACÍA de la línea de
//    tiempo fija el horario de "Tu OT" ahí mismo. Un solo listener
//    delegado en el lienzo (no en cada .tkday-blk) cubre toda el área
//    vacía sin instrumentar cada franja/hora por separado. ──
let _tkdayFlashToken = 0;
function tkdaySlotClick(ev){
  if(ev.target.closest('.tkday-blk')) return;   // clic en un bloque real -> lo maneja tkdayVerBloque
  if(ev.target.closest('.g-btn')) return;        // clic en "Reintentar" del estado de error de mes
  if(!_TKOT.day.fecha) return;                   // sin día elegido todavía: nada que fijar

  const canvas = document.getElementById('levDayCanvas');
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
  const iniActual = _tkdayToMin(document.getElementById('levHoraIni')?.value || '');
  const finActual = _tkdayToMin(document.getElementById('levHoraFin')?.value || '');
  let dur = (iniActual != null && finActual != null) ? (finActual - iniActual) : 0;
  if(!(dur > 0)) dur = 60;
  const finMin = Math.min(min + dur, TKDAY_END);

  // Si se está mirando un día distinto al programado (flechas ‹ › de un
  // rango multi-día), el calendario "manda" primero -- mismo mecanismo que
  // un clic real en el mini-calendario (setea #levFechaProg y dispara su change).
  const fechaProg = document.getElementById('levFechaProg')?.value || '';
  if(_TKOT.day.fecha !== fechaProg){
    tkotCalClicDia(_TKOT.day.fecha);
  }

  document.getElementById('levHoraIni').value = _tkdayHHMM(min);
  document.getElementById('levHoraFin').value = _tkdayHHMM(finMin);
  tkotHoraInput();   // repinta "Tu OT" (transición CSS existente) + dispara el choque con debounce

  // Feedback visual -- sin toast (pedido explícito): pulso breve del marco
  // de "Tu OT". Token propio para que un segundo clic mientras el primer
  // pulso sigue activo no lo corte antes de tiempo (mismo patrón que
  // _TKOT._choqueToken para respuestas async obsoletas).
  const mine = document.getElementById('levDayMine');
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
document.getElementById('levDayCanvas').addEventListener('click', tkdaySlotClick);

// ── El bloque de la OT NUEVA. Se dibuja EN VIVO: en cada `input` de
//    #levHoraIni/#levHoraFin solo se actualiza style.top/height del MISMO
//    nodo (no se reconstruye), de modo que la transición CSS lo deslice. ──
function tkdayRenderMine(){
  const mine = document.getElementById('levDayMine');
  if(!mine) return;
  const vista = _TKOT.day.fecha;
  const fechaProg = document.getElementById('levFechaProg')?.value || '';
  let hi = '', hf = '';
  let dibujar = false;
  if(vista && fechaProg){
    if(vista === fechaProg){
      hi = document.getElementById('levHoraIni')?.value || '';
      hf = document.getElementById('levHoraFin')?.value || '';
      dibujar = true;
    } else {
      // Rango multi-día: "Tu OT" se dibuja en CADA día del rango al visitarlo.
      // Último día ⇒ sus horas propias; días intermedios ⇒ horas del primer día.
      const rango = _tkotRangoActivo();
      if(rango && vista > rango.ini && vista <= rango.fin){
        if(vista === rango.fin){
          hi = document.getElementById('levHoraIniFin')?.value || '';
          hf = document.getElementById('levHoraFinFin')?.value || '';
        } else {
          hi = document.getElementById('levHoraIni')?.value || '';
          hf = document.getElementById('levHoraFin')?.value || '';
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
  // El choque se pinta SOLO si el resultado vigente de tkotChequearChoque()
  // corresponde al día que se está mirando (para un día intermedio del rango
  // no hay resultado y no se inventa uno).
  if(_TKOT.day.choque && _TKOT.day.choqueFecha === vista) extra.push('choque');
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
  mine.className = 'tkday-blk ' + extra.join(' ');
  mine.style.display = '';
  mine.style.top = g.top + 'px';
  mine.style.height = g.height + 'px';

  // Toda la info de "Tu OT" se muda a la píldora del borde superior (fuera
  // del área de texto): así nunca se superpone al texto de un bloque real
  // que coincida en horario. REGLA #4.2: misma información (hora +
  // técnicos), solo reubicada -- el cuerpo queda vacío a propósito, el
  // marco punteado + fondo translúcido siguen marcando la geometría.
  const nTec = _TKOT.tecnicosSel.size;
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
  // 0 técnicos: guía neutra (se asignan en el Paso 4), no un "0" alarmante.
  const _tecPend = nTec === 0;
  const _tecTxt = _tecPend ? 'Asigna técnico en el paso 4' : (nTec + (nTec === 1 ? ' técnico' : ' técnicos'));
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
  const scroll = document.getElementById('levDayScroll');
  const mine = document.getElementById('levDayMine');
  if(!scroll) return;
  let target = 48;   // 09:00 si no hay bloque propio
  if(mine && mine.style.display !== 'none') target = (parseFloat(mine.style.top) || 0) - 40;
  scroll.scrollTop = Math.max(0, target);
}

// ── Repinta SOLO las clases de choque (sin reconstruir el día) tras la
//    respuesta de /mantenciones/api/calendario/choque. ──
function tkdayAplicarChoque(){
  const blks = document.getElementById('levDayBlks');
  if(!blks) return;
  const keys = _TKOT.day.choqueKeys;
  const aplica = !!(keys && _TKOT.day.choqueFecha && _TKOT.day.choqueFecha === _TKOT.day.fecha);
  const visitas = _TKOT.day.visitas || [];
  blks.querySelectorAll('.tkday-blk[data-idx]').forEach(function(el){
    const v = visitas[parseInt(el.dataset.idx, 10)];
    el.classList.toggle('en-choque', !!(aplica && v && keys.has(_tkdayVisitaKey(v))));
  });
  tkdayRenderMine();
}

// ════════════════════════════════════════════════════════════
// DETALLE DE UN BLOQUE — overlay propio position:fixed, NO un modal
// Bootstrap anidado. Decisión de ingeniería: abrir un segundo
// bootstrap.Modal sobre #modalGenerarOT obliga a parchear a mano el
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

function tkdayVerBloque(el){
  if(!el) return;
  const visitas = _TKOT.day.visitas || [];
  let lista = [];
  if(el.dataset.grupo){
    lista = el.dataset.grupo.split(',').map(function(s){ return visitas[parseInt(s, 10)]; }).filter(Boolean);
  } else {
    const v = visitas[parseInt(el.dataset.idx, 10)];
    if(v) lista = [v];
  }
  if(!lista.length) return;
  tkdayAbrirDetalle(lista);
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
  const enLista = (_TKOT.day.visitas || []).find(function(x){ return String(x.visita_id) === String(vid); });
  if(enLista) return enLista;
  return (_tkdayPopVisita && String(_tkdayPopVisita.visita_id) === String(vid)) ? _tkdayPopVisita : null;
}

function tkdayAbrirDetalle(lista){
  tkdayCerrarDetalle();
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
      + _tkdayPopFila('bi-calendar3', 'Fecha', _TKOT.day.fecha
          ? _TKOT.day.fecha.split('-').reverse().join('/') : '', true)
      + _tkdayPopFila('bi-card-text', 'Título', v.titulo || '', false)
      + _tkdayPopFila('bi-tag', 'Tipo', v.tipo || '', false)
      + _tkdayPopFila('bi-ticket-detailed', 'Ticket', v.numero_ticket || '', true)
      + (uno ? '' : '<div class="pop-row" style="padding-top:6px">'
          // §2.5 "+N más": botón "Gestionar" junto a "Abrir OT completa" --
          // reabre el popover individual (con su barra de acciones) sin
          // lógica nueva, solo delega en tkdayAbrirDetalle([v]).
          + (_tkdayPuedeGestionarUno(v)
              ? '<button type="button" class="btn btn-sm btn-outline-secondary" onclick="tkdayGestionarUno(' + (v.visita_id != null ? v.visita_id : '') + ')">'
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
      + '<button type="button" class="btn btn-sm btn-outline-secondary" onclick="tkdayPopAccion(\'reprogramar\',' + vid0 + ')"><i class="bi bi-calendar2-week me-1"></i>Reprogramar</button>'
      + '<button type="button" class="btn btn-sm btn-outline-secondary" onclick="tkdayPopAccion(\'reasignar\',' + vid0 + ')"><i class="bi bi-person-gear me-1"></i>Reasignar</button>'
      + '<button type="button" class="btn btn-sm btn-outline-danger" onclick="tkdayPopAccion(\'cancelar\',' + vid0 + ')"><i class="bi bi-x-circle me-1"></i>Cancelar OT</button>'
      + '</div>'
      + '<div id="tkdayReprogForm"></div>'
      + '<div id="tkdayReasigForm"></div>'
    : '';

  const pie = uno
    ? '<div class="pop-foot">'
      + '<button type="button" class="btn btn-sm btn-light" onclick="tkdayCerrarDetalle()">Cerrar</button>'
      + '<a class="btn btn-sm btn-outline-danger" target="_blank" rel="noopener"'
      + ' href="/mantenciones/ot/' + encodeURIComponent(v0.visita_id != null ? v0.visita_id : '') + '">'
      + 'Abrir OT completa <i class="bi bi-box-arrow-up-right ms-1"></i></a></div>'
    : '<div class="pop-foot"><button type="button" class="btn btn-sm btn-light" onclick="tkdayCerrarDetalle()">Cerrar</button></div>';

  const pop = document.createElement('div');
  pop.className = 'tkday-pop';
  pop.setAttribute('role', 'dialog');
  pop.setAttribute('aria-modal', 'true');
  pop.innerHTML = '<div class="pop-card">'
    + '<div class="pop-head">' + cab
    + '<button type="button" class="btn-close btn-close-white" aria-label="Cerrar" onclick="tkdayCerrarDetalle()"></button></div>'
    + '<div class="pop-body">' + cuerpo + '</div>' + acciones + pie + '</div>';
  // Clic en el backdrop propio cierra; clic dentro de la tarjeta no.
  pop.addEventListener('click', function(ev){ if(ev.target === pop) tkdayCerrarDetalle(); });
  document.body.appendChild(pop);
  _tkdayPopEl = pop;
  // ESC: se captura ANTES de que llegue a Bootstrap, si no cerraría el
  // modal padre "Generar OT" y se perdería todo lo cargado.
  _tkdayPopKey = function(ev){
    if(ev.key === 'Escape'){ ev.preventDefault(); ev.stopPropagation(); tkdayCerrarDetalle(); }
  };
  document.addEventListener('keydown', _tkdayPopKey, true);
  const foco = pop.querySelector('.btn-close');
  if(foco) foco.focus();
}

function tkdayCerrarDetalle(){
  if(_tkdayPopKey){ document.removeEventListener('keydown', _tkdayPopKey, true); _tkdayPopKey = null; }
  if(_tkdayPopEl){ _tkdayPopEl.remove(); _tkdayPopEl = null; }
  if(_tkdayRpChoqueTimer){ clearTimeout(_tkdayRpChoqueTimer); _tkdayRpChoqueTimer = null; }
  _tkdayPopVisita = null;
}

// ── §2.5: atajo del listado "+N más" -- reabre el popover en modo 1 OT. ──
function tkdayGestionarUno(vid){
  const v = _tkdayFindVisita(vid);
  if(v) tkdayAbrirDetalle([v]);
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
function tkdayPopAccion(accion, vid){
  if(accion === 'reprogramar') return tkdayReprogAbrir(vid);
  if(accion === 'reasignar') return tkdayReasigAbrir(vid);
  if(accion === 'cancelar') return tkdayCancelarOT(vid);
}

// Cierra/vacía los 2 mini-formularios inline (botón "Cancelar" de cada uno,
// y al abrir uno se cierra el otro -- solo tiene sentido 1 a la vez).
function tkdayFormsCerrar(){
  const rp = document.getElementById('tkdayReprogForm');
  const ra = document.getElementById('tkdayReasigForm');
  if(rp) rp.innerHTML = '';
  if(ra) ra.innerHTML = '';
  if(_tkdayRpChoqueTimer){ clearTimeout(_tkdayRpChoqueTimer); _tkdayRpChoqueTimer = null; }
}

// Recarga el mes tras cualquier PUT que haya podido mover una OT de día/mes:
// invalida TODO el caché (no solo el mes vigente, la fecha puede haber
// cambiado de mes) y vuelve a pedir el mes que se está mirando ahora mismo.
async function _tkdayRecargarMes(){
  _TKOT.cal.cache = {};
  _TKOT.cal.error = {};
  await tkotCalCargarMes();
}

// ── §2.2 Reprogramar: mini-formulario inline (SOLO camino "escribir los
//    campos" -- el "tocar y colocar" queda para una fase posterior). ──
function tkdayReprogAbrir(vid){
  const v = _tkdayFindVisita(vid);
  if(!v) return;
  tkdayFormsCerrar();
  const cont = document.getElementById('tkdayReprogForm');
  if(!cont) return;
  const fechaBase = _TKOT.day.fecha || '';
  cont.innerHTML = '<div class="pop-inline">'
    + '<div class="small fw-bold mb-2"><i class="bi bi-calendar2-week me-1" style="color:#dc2626"></i>Reprogramar</div>'
    + '<div class="row-fields">'
    + '<div><label for="tkdayRpFecha">Fecha</label>'
    + '<input type="date" id="tkdayRpFecha" class="form-control form-control-sm" value="' + esc(fechaBase) + '"></div>'
    + '<div><label for="tkdayRpFechaFin">Fecha término (rango, opcional)</label>'
    + '<input type="date" id="tkdayRpFechaFin" class="form-control form-control-sm" value="' + esc(v.fecha_fin || '') + '"></div>'
    + '</div>'
    + '<div class="row-fields">'
    + '<div><label for="tkdayRpHoraIni">Hora inicio</label>'
    + '<input type="time" id="tkdayRpHoraIni" class="form-control form-control-sm" value="' + esc(v.hora_inicio || '') + '"></div>'
    + '<div><label for="tkdayRpHoraFin">Hora término</label>'
    + '<input type="time" id="tkdayRpHoraFin" class="form-control form-control-sm" value="' + esc(v.hora_fin || '') + '"></div>'
    + '</div>'
    + '<div id="tkdayRpChoqueAlert" class="tkcal-choque-alert" style="display:none"></div>'
    + '<div class="actions">'
    + '<button type="button" class="btn btn-sm btn-light" onclick="tkdayFormsCerrar()">Cancelar</button>'
    + '<button type="button" class="btn btn-sm btn-ilus" onclick="tkdayReprogGuardar(' + vid + ')">Guardar cambios</button>'
    + '</div></div>';
  ['tkdayRpFecha', 'tkdayRpHoraIni', 'tkdayRpHoraFin', 'tkdayRpFechaFin'].forEach(function(id){
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
  const alertEl = document.getElementById('tkdayRpChoqueAlert');
  if(!alertEl) return;
  const fecha = document.getElementById('tkdayRpFecha')?.value || '';
  const horaIni = document.getElementById('tkdayRpHoraIni')?.value || '';
  const horaFin = document.getElementById('tkdayRpHoraFin')?.value || '';
  const fechaFin = document.getElementById('tkdayRpFechaFin')?.value || '';
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
    console.warn('tkday reprog choque:', e);   // aviso, no bloqueo -- no se toca el formulario
  }
}

function tkdayReprogGuardar(vid){
  const v = _tkdayFindVisita(vid) || {};
  const fecha = document.getElementById('tkdayRpFecha')?.value || '';
  const horaIni = document.getElementById('tkdayRpHoraIni')?.value || '';
  const horaFin = document.getElementById('tkdayRpHoraFin')?.value || '';
  const fechaFin = document.getElementById('tkdayRpFechaFin')?.value || '';
  if(!fecha){ ilusToast('Indica la fecha', {type:'warning'}); return; }
  if(horaIni && horaFin && horaIni >= horaFin){ ilusToast('La hora de término debe ser posterior a la de inicio', {type:'warning'}); return; }
  if(fechaFin && fechaFin < fecha){ ilusToast('La fecha de término no puede ser anterior a la de inicio', {type:'warning'}); return; }

  // Solo los campos que realmente cambiaron (spec §2.2) -- compara contra
  // el día que se está mirando (== fecha_programada real de esta visita,
  // ver _tkdayFindVisita/tkdayRender) y los campos crudos de `v`.
  const body = {};
  if(fecha !== (_TKOT.day.fecha || '')) body.fecha_programada = fecha;
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
      tkdayCerrarDetalle();
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
function tkdayReasigAbrir(vid){
  const v = _tkdayFindVisita(vid);
  if(!v) return;
  tkdayFormsCerrar();
  const cont = document.getElementById('tkdayReasigForm');
  if(!cont) return;
  const techs = _TKOT.tecnicosDisponibles || [];
  const opts = techs.map(function(t){
    const sel = String(t.id) === String(v.tecnico_id) ? ' selected' : '';
    return '<option value="' + t.id + '"' + sel + '>' + esc(t.nombre || t.email || ('Téc #' + t.id)) + '</option>';
  }).join('');
  cont.innerHTML = '<div class="pop-inline">'
    + '<div class="small fw-bold mb-2"><i class="bi bi-person-gear me-1" style="color:#dc2626"></i>Reasignar técnico</div>'
    + '<label for="tkdayRaSelect">Técnico asignado</label>'
    + '<select id="tkdayRaSelect" class="form-select form-select-sm">' + opts + '</select>'
    + '<div class="actions">'
    + '<button type="button" class="btn btn-sm btn-light" onclick="tkdayFormsCerrar()">Cancelar</button>'
    + '<button type="button" class="btn btn-sm btn-ilus" onclick="tkdayReasigGuardar(' + vid + ')">Guardar</button>'
    + '</div></div>';
}

function tkdayReasigGuardar(vid){
  const sel = document.getElementById('tkdayRaSelect');
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
      tkdayCerrarDetalle();
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
async function tkdayCancelarOT(vid){
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
      tkdayCerrarDetalle();
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
function tkotChequearChoqueDebounced(){
  if(_tkotChoqueTimer) clearTimeout(_tkotChoqueTimer);
  _tkotChoqueTimer = setTimeout(tkotChequearChoque, 500);
}
async function tkotChequearChoque(){
  const warn = document.getElementById('levCalWarnChoque');
  if(!warn) return;
  const fecha = document.getElementById('levFechaProg')?.value || '';
  const horaIni = document.getElementById('levHoraIni')?.value || '';
  const horaFin = document.getElementById('levHoraFin')?.value || '';
  const tecnicoIds = Array.from(_TKOT.tecnicosSel);
  if(!fecha || !tecnicoIds.length){
    warn.style.display = 'none';
    _TKOT.day.choque = false; _TKOT.day.choqueFecha = null; _TKOT.day.choqueKeys = null;
    tkdayAplicarChoque();
    return;
  }
  const idNombre = {};
  (_TKOT.tecnicosDisponibles || []).forEach(function(t){ idNombre[t.id] = t.nombre || t.email || ('Téc #' + t.id); });
  // §2.6.2: si el formulario tiene el rango multi-día activo (#levRangoDias
  // marcado + #levFechaFin válida), se manda también fecha_fin -- el backend
  // YA lo acepta (fix reciente de choque), solo faltaba que el JS lo enviara.
  const rangoAct = _tkotRangoActivo();
  const miToken = ++_TKOT._choqueToken;
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
    if(miToken !== _TKOT._choqueToken) return; // el usuario siguió cambiando algo -- respuesta obsoleta
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
      // libre" -- llama exactamente la misma tkdaySugerirHueco() del chip
      // de la cabecera del día. Cero lógica duplicada.
      warn.innerHTML = '<i class="bi bi-exclamation-triangle-fill"></i><div>' + mensajes.join('<br>')
        + '<div style="margin-top:8px"><button type="button" class="btn btn-sm btn-outline-danger" onclick="tkdaySugerirHueco()">'
        + '<i class="bi bi-stars me-1"></i>Buscar hueco libre</button></div></div>';
      warn.style.display = 'flex';
    } else {
      warn.style.display = 'none';
    }
    _TKOT.day.choque = mensajes.length > 0;
    _TKOT.day.choqueFecha = fecha;
    _TKOT.day.choqueKeys = choqueKeys;
    tkdayAplicarChoque();
  }catch(e){
    console.warn('tkot choque:', e);
  }
}

// ════════════════════════════════════════════════════════════
// §4A (cont.) — orquestación de "Sugerir horario": dos puntos de entrada
// (chip de la cabecera + botón del banner de choque) llaman a la MISMA
// tkdaySugerirHueco(). Cero backend nuevo: _tkdayAsegurarMesCache() reusa
// el mismo GET /mantenciones/api/calendario/mes/<anio>/<mes> que ya usa
// tkotCalCargarMes(), solo que sin tocar _TKOT.cal.anio/mes (no mueve el
// mes visible mientras busca "de reojo" hacia adelante).
// ════════════════════════════════════════════════════════════

// ── Asegura que el caché del mes <anio>/<mes> esté cargado, SIN mover el
//    mes visible (_TKOT.cal.anio/mes) ni repintar la grilla -- distinto de
//    tkotCalCargarMes(), que sí hace ambas cosas porque asume que el mes
//    pedido es el que se está mirando. ──
async function _tkdayAsegurarMesCache(anio, mes){
  const key = _tkotCalKey(anio, mes);
  if(_TKOT.cal.cache[key]) return _TKOT.cal.cache[key];
  try{
    const r = await fetch('/mantenciones/api/calendario/mes/' + anio + '/' + mes);
    if(!r.ok) throw new Error('http ' + r.status);
    const d = await r.json();
    _TKOT.cal.cache[key] = _tkotCalNormalizarMes(d);
    delete _TKOT.cal.error[key];
  }catch(e){
    console.warn('tkday sugerir - cache mes:', e);
    _TKOT.cal.cache[key] = _TKOT.cal.cache[key] || {};
    _TKOT.cal.error[key] = true;
  }
  return _TKOT.cal.cache[key];
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
//    mini-calendario (mismo efecto que tkotCalClicDia → tkotFechaProgChange),
//    pero AWAIT-eable: se necesita esperar a que el día quede pintado antes
//    de setear las horas encontradas y disparar el flash de "Tu OT". ──
async function _tkdayIrADia(fecha){
  const fp = document.getElementById('levFechaProg');
  if(fp) fp.value = fecha;
  // Chips de duración (2026-07-19, P1): este camino (Sugerir horario) escribe
  // #levFechaProg DIRECTO y nunca pasaba por tkotFechaProgChange -- el rango
  // quedaba sin re-anclar (fin < inicio) y los chips no se repintaban.
  _levReanclarFin();
  const y = parseInt(fecha.slice(0, 4), 10), m = parseInt(fecha.slice(5, 7), 10);
  if(y && m && (y !== _TKOT.cal.anio || m !== _TKOT.cal.mes)){
    _TKOT.cal.anio = y; _TKOT.cal.mes = m;
    await tkotCalCargarMes();   // repinta grilla del mes nuevo (cache ya tibio por la búsqueda)
  }
  tkotCalSelDia(fecha, { silencio: true });
  levChipsRefresh();
}

// ── Punto de entrada único (chip de cabecera + botón del banner de choque).
//    Busca el hueco, salta al día + hora encontrados y da el flash visual de
//    "Tu OT" (reusa .flash, mismo mecanismo que tkdaySlotClick). Si no hay
//    hueco en 14 días, toast de aviso -- NUNCA alert() nativo (REGLA #1). ──
async function tkdaySugerirHueco(){
  const btn = document.getElementById('levDaySugerirBtn');
  const original = btn ? btn.innerHTML : null;
  if(btn){ btn.disabled = true; btn.innerHTML = '<i class="bi bi-hourglass-split"></i>Buscando…'; }
  try{
    const iniActual = _tkdayToMin(document.getElementById('levHoraIni')?.value || '');
    const finActual = _tkdayToMin(document.getElementById('levHoraFin')?.value || '');
    const durMin = (iniActual != null && finActual != null && finActual > iniActual) ? (finActual - iniActual) : 60;
    const tecnicoIds = Array.from(_TKOT.tecnicosSel);
    const fechaBase = _TKOT.day.fecha || document.getElementById('levFechaProg')?.value || _tkdayHoyStr();

    const encontrado = await _tkdayBuscarHuecoRango(fechaBase, durMin, tecnicoIds, 14);
    if(!encontrado){
      ilusToast('Sin huecos en los próximos 14 días para ese equipo de técnicos', { type: 'warning' });
      return;
    }

    await _tkdayIrADia(encontrado.fecha);
    document.getElementById('levHoraIni').value = encontrado.hora;
    document.getElementById('levHoraFin').value = _tkdayHHMM(_tkdayToMin(encontrado.hora) + durMin);
    tkotHoraInput();   // repinta "Tu OT" en su nueva posición + dispara el choque (debería salir limpio)

    const mine = document.getElementById('levDayMine');
    if(mine){
      mine.classList.remove('flash');
      void mine.offsetWidth;   // fuerza reflow -- mismo truco que tkdaySlotClick
      mine.classList.add('flash');
      setTimeout(function(){ mine.classList.remove('flash'); }, 650);
    }
  }catch(e){
    console.warn('tkday sugerir hueco:', e);
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

function tkotPlantillaParaTipo(tipo){
  const todas = ((_TKOT.plantillas && _TKOT.plantillas.all) || [])
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

function tkotPintarPlantilla(tipo){
  const box = document.getElementById('otPlantillaInfo');
  if(!box) return;
  if(!(_TKOT.plantillas && _TKOT.plantillas.cargadas)){
    box.innerHTML = ''; box.removeAttribute('style'); return;
  }
  const p = tkotPlantillaParaTipo(tipo);
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
// tkotPlantillaSelectChange eliminadas (sin otro caller — grep verificado).

function tkotTipoChange(){
  const tipo = document.getElementById('otTipo')?.value;
  const desc = document.getElementById('otTipoDescripcion');
  if(desc && tipo) desc.innerHTML = '<i class="bi bi-info-circle me-1"></i>' + (_TKOT_TIPO_DESC[tipo] || '');
  tkotPintarPlantilla(tipo);

  const garWrap = document.getElementById('otGarantiaWrap');
  if(garWrap){
    if(tipo === 'levantamiento'){
      garWrap.style.display = 'none';
      const g = document.getElementById('otAplicaGarantia'); if(g) g.checked = false;
    } else garWrap.style.display = '';
  }

  // MANDA EL DESPLEGABLE (Daniel 2026-08-06): al cambiar de tipo la modalidad
  // se limpia SIEMPRE, no queda 'equipos' por debajo. Espejo de Mantenciones.
  const modoWrap = document.getElementById('otModoLevWrap');
  if(modoWrap){
    modoWrap.style.display = (tipo === 'levantamiento') ? '' : 'none';
    tkotModoSet(null);
  }

  const tit = document.getElementById('levSelectTitulo');
  if(tit && (!tit.value || /^(Levantamiento|Instalación|Mantención|Visita|Inspección) /.test(tit.value))){
    const fecha = new Date().toLocaleDateString('es-CL');
    const labels = { levantamiento:'Levantamiento', instalacion:'Instalación', preventiva:'Mantención preventiva',
      visita_tecnica:'Visita técnica', correctiva:'Mantención correctiva', inspeccion:'Inspección' };
    tit.value = (labels[tipo] || 'OT') + ' ' + fecha;
  }

  // Instalación + cliente SIN ficha -> forzar preselección total bloqueada.
  tkotAplicarForzadoInstalacion();
  tkotRefreshStepStates();
}

// null = sin elegir. La modalidad NO viene preseleccionada: manda el tipo de
// OT del desplegable (ver tkotTipoChange).
function tkotModoSet(modo){
  _TKOT.modo = (modo === 'descubrimiento' || modo === 'equipos') ? modo : null;
  const cEq = document.getElementById('levModoEquipos');
  const cDes = document.getElementById('levModoDescubrir');
  const hint = document.getElementById('levModoHint');
  if(cEq) cEq.classList.toggle('on', _TKOT.modo === 'equipos');
  if(cDes) cDes.classList.toggle('on', _TKOT.modo === 'descubrimiento');
  if(hint) hint.style.display = (_TKOT.modo === 'descubrimiento') ? '' : 'none';
  tkotRefreshStepStates();
}

async function tkotAbrirCrearTipoOT(){
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
//    tkotRenderEquipos()/tkotGenerar() esperan de equiposCache (e.maquina_id
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
function tkotRenderEquipos(){
  const tbody = document.getElementById('levSelectTbody');
  if(!tbody) return;
  const eqs = equiposCache || [];
  // FIX 2026-08-12 (bug real reportado por Daniel: una plantilla que SÍ se
  // marcaba por equipo terminaba sin aplicarse -- la OT caía en la tarea
  // de respaldo genérica). Causa: esta función se vuelve a llamar cuando
  // cambia el tipo de OT y eso activa/desactiva el modo "instalación sin
  // ficha" (tkotAplicarForzadoInstalacion -> tkotRenderEquipos), y antes
  // borraba TODAS las plantillas ya elegidas por equipo, en silencio, sin
  // avisar. Ahora solo se limpia la selección de equipos que YA NO están
  // en la lista actual -- si el equipo sigue ahí, su plantilla elegida se
  // conserva. Con equiposCache vacío (modal recién abierto) esto sigue
  // limpiando todo, como antes.
  const _keysActuales = new Set(eqs.map(_tkotEqKey));
  Object.keys(_TKOT.eqPlantillas).forEach(function(k){
    if(!_keysActuales.has(k)) delete _TKOT.eqPlantillas[k];
  });
  if(!eqs.length){
    tbody.innerHTML = '<tr><td colspan="3" class="text-muted small text-center py-3">'
      + (_TKOT_MODO_CLIENTE ? 'Este cliente no tiene equipos registrados todavía.' : 'Este ticket no tiene equipos declarados.')
      + '</td></tr>';
    document.getElementById('levEqCount').textContent = '0';
    return;
  }
  const forzado = _TKOT.forzarTodosEquipos;
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
    return '<tr class="'+rowCls+'" style="'+rowOpacity+'cursor:'+(forzado?'not-allowed':'pointer')+'" '
      + (forzado ? '' : 'onclick="const c=this.querySelector(\'.lev-eq-chk\');c.checked=!c.checked;tkotRecalcEqCount();event.stopPropagation();"') + '>'
      + '<td><input type="checkbox" class="lev-eq-chk" data-key="'+esc(key)+'" '+checkedAttr+' '
      + (forzado?'':'onchange="tkotRecalcEqCount()" onclick="event.stopPropagation()"') + '></td>'
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
      + (forzado
          ? '<span class="small text-muted"><i class="bi bi-lock me-1"></i>incluido completo</span>'
          : '<button id="lev-pl-btn-'+esc(key)+'" class="btn btn-xs btn-outline-primary w-100" '
            + 'style="font-size:.72rem;padding:.25rem .4rem;opacity:.4;pointer-events:none" '
            + 'onclick="tkotAbrirMultiPlantilla(\''+esc(key)+'\', \''+esc(nombre)+'\')" title="Selecciona el equipo primero">'
            + '<i class="bi bi-lock me-1"></i><span id="lev-pl-count-'+esc(key)+'">marca el equipo</span></button>')
      + '</td></tr>';
  }).join('');
  tkotRecalcEqCount();
}

// Instalación + cliente sin ficha (CID null): todos los equipos del ticket
// van completos, marcados y bloqueados (no hay "ficha" contra la cual
// elegir un subconjunto -- Daniel: "ya lo requeriría de un plan, porque
// el cliente no existe").
function tkotAplicarForzadoInstalacion(){
  const tipo = document.getElementById('otTipo')?.value;
  const warn = document.getElementById('tkotSinFichaWarn');
  const debeForzar = (tipo === 'instalacion') && _TKOT.clienteResuelto && !_TKOT.cid && (equiposCache||[]).length > 0;
  if(warn) warn.style.display = (_TKOT.clienteResuelto && !_TKOT.cid) ? '' : 'none';
  if(debeForzar !== _TKOT.forzarTodosEquipos){
    _TKOT.forzarTodosEquipos = debeForzar;
    tkotRenderEquipos();
  }
}

function tkotToggleTodos(){
  if(_TKOT.forzarTodosEquipos) return; // bloqueado -- no se puede desmarcar
  const checks = document.querySelectorAll('.lev-eq-chk');
  const marcados = document.querySelectorAll('.lev-eq-chk:checked').length;
  const newState = marcados < checks.length;
  checks.forEach(function(c){ c.checked = newState; });
  tkotRecalcEqCount();
}

function tkotRecalcEqCount(){
  const checks = document.querySelectorAll('.lev-eq-chk');
  const n = document.querySelectorAll('.lev-eq-chk:checked').length;
  const el = document.getElementById('levEqCount');
  if(el) el.textContent = String(n);
  const tBtn = document.getElementById('btnLevToggleTodos');
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
      plBtn.title = 'Agregar plantillas extra a este equipo';
      const countSpan = document.getElementById('lev-pl-count-' + key);
      if(countSpan){
        const n2 = (_TKOT.eqPlantillas[key] && _TKOT.eqPlantillas[key].size) || 0;
        countSpan.textContent = n2 ? (n2 + ' plantilla' + (n2>1?'s':'') + ' extra') : '0 plantillas';
      }
    } else {
      plBtn.style.opacity = '.4'; plBtn.style.pointerEvents = 'none';
      plBtn.title = 'Selecciona el equipo primero';
    }
  });
  tkotRefreshStepStates();
}

// ── Multi-plantilla por equipo (idéntico a Mantenciones, clave = _tkotEqKey) ──
async function tkotAbrirMultiPlantilla(key, eqNombre){
  const todas = _TKOT.plantillas.all || [];
  if(!todas.length){
    ilusAlert({ title:'Sin plantillas', message:'No hay plantillas activas en el sistema.',
      sub:'Pide a un administrador que cree plantillas en /mantenciones/plantillas.', type:'warning' });
    return;
  }
  const tipoActual = document.getElementById('otTipo')?.value || '';
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
  const seleccionadas = _TKOT.eqPlantillas[key] || new Set();
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
    + '<button type="button" class="btn btn-primary" onclick="tkotGuardarMultiPlantilla(\''+esc(key)+'\')">'
    + '<i class="bi bi-check-lg me-1"></i>Guardar selección</button></div></div></div>';
  document.body.appendChild(modal);
  new bootstrap.Modal(modal).show();
}
function tkotGuardarMultiPlantilla(key){
  const modal = document.getElementById('modalMultiPlantilla');
  if(!modal) return;
  const ids = Array.from(modal.querySelectorAll('.mp-chk:checked')).map(function(c){ return parseInt(c.dataset.pid); });
  if(ids.length) _TKOT.eqPlantillas[key] = new Set(ids); else delete _TKOT.eqPlantillas[key];
  const counter = document.getElementById('lev-pl-count-' + key);
  if(counter) counter.textContent = ids.length ? (ids.length + ' plantilla' + (ids.length>1?'s':'') + ' extra') : '0 plantillas';
  bootstrap.Modal.getInstance(modal)?.hide();
  ilusToast('✓ ' + ids.length + ' plantilla(s) asignada(s) al equipo', {type:'success', duration:2000});
}

// ── Acceso y logística (idéntico a Mantenciones) ──
function tkotSetAccesoYN(btn){
  const target = btn.dataset.target, val = btn.dataset.val;
  const hidden = document.getElementById(target);
  if(!hidden) return;
  if(hidden.value === val){
    hidden.value = '';
    document.querySelectorAll('.lev-yn-btn[data-target="'+target+'"]').forEach(function(b){ b.classList.remove('active'); });
    tkotRefreshStepStates();
    return;
  }
  hidden.value = val;
  document.querySelectorAll('.lev-yn-btn[data-target="'+target+'"]').forEach(function(b){
    b.classList.toggle('active', b.dataset.val === val);
  });
  tkotRefreshStepStates();
}
function tkotResetAccesoLogistica(){
  ['acceso_ascensor','acceso_estacionamiento'].forEach(function(id){
    const h = document.getElementById(id);
    if(h) h.value = '';
    document.querySelectorAll('.lev-yn-btn[data-target="'+id+'"]').forEach(function(b){ b.classList.remove('active'); });
  });
  const piso = document.getElementById('acceso_piso');
  const notas = document.getElementById('acceso_notas');
  if(piso) piso.value = '';
  if(notas) notas.value = '';
  tkotRefreshStepStates();
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
function tkotAdjFiles(fileList){
  if(!fileList || !fileList.length) return;
  const rechazados = [];
  Array.from(fileList).forEach(function(f){
    const tipo = _tkotAdjTipo(f);
    const max = _TKOT_ADJ_MAX[tipo] || _TKOT_ADJ_MAX.otro;
    if(f.size > max){ rechazados.push(f.name+' ('+_tkotBytesPretty(f.size)+', máx '+_tkotBytesPretty(max)+')'); return; }
    _TKOT.adjuntos.push(f);
  });
  const inp = document.getElementById('levAdjInput');
  if(inp) inp.value = '';
  _tkotRenderAdjList();
  if(rechazados.length) ilusToast('Archivo(s) muy grande(s): '+rechazados[0]+(rechazados.length>1?' y '+(rechazados.length-1)+' más':''), {type:'warning'});
}
function _tkotRenderAdjList(){
  const wrap = document.getElementById('levAdjList');
  const counter = document.getElementById('levAdjCount');
  if(!wrap) return;
  const arr = _TKOT.adjuntos || [];
  if(counter) counter.textContent = arr.length;
  if(!arr.length){ wrap.innerHTML = ''; tkotRefreshStepStates(); return; }
  wrap.innerHTML = arr.map(function(f, idx){
    return '<div class="lev-adj-item"><div class="lev-adj-thumb"><i class="bi '+_tkotAdjIconClass(f)+'"></i></div>'
      + '<div class="lev-adj-info"><div class="lev-adj-name" title="'+esc(f.name)+'">'+esc(f.name)+'</div>'
      + '<div class="lev-adj-meta">'+_tkotAdjTipo(f).toUpperCase()+' · '+_tkotBytesPretty(f.size)+'</div></div>'
      + '<button type="button" class="lev-adj-rm" onclick="tkotRemoveAdj('+idx+')" title="Quitar"><i class="bi bi-x-lg"></i></button></div>';
  }).join('');
  tkotRefreshStepStates();
}
function tkotRemoveAdj(idx){ _TKOT.adjuntos.splice(idx,1); _tkotRenderAdjList(); }
function tkotResetAdjuntos(){ _TKOT.adjuntos = []; _tkotRenderAdjList(); }
async function _tkotSubirAdjuntos(vid){
  const arr = _TKOT.adjuntos || [];
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
// Las reglas son un ESPEJO de lo que ya valida tkotGenerar() antes de
// enviar -- no agregan ni quitan requisitos, solo los muestran antes:
//   1 Tipo      · hay tipo; si es levantamiento, además hay modalidad
//   2 Info      · título + dirección + nombre de contacto (los 3 que
//                 tkotGenerar() exige con toast)
//   3 Agenda    · hay fecha programada
//   4 Técnicos  · al menos uno
//   5 Equipos   · al menos un checkbox marcado; EXCEPTO levantamiento por
//                 descubrimiento, donde tkotGenerar() no exige equipos
//                 (los captura el técnico en terreno) -> cuenta completo
//   6 Acceso    · OPCIONAL (no bloquea el envío)
//   7 Documentos· OPCIONAL (no bloquea el envío)
// Los pasos 6 y 7 nacen con .is-optional (punteado gris) desde el HTML;
// si el usuario los llena pasan a verde, si los vacía vuelven a punteado.
// CERO cambio funcional: esto no valida, no bloquea y no toca el submit.
const TKOT_STEP_RULES = {
  1: function(){
    const tipo = (document.getElementById('otTipo') || {}).value || '';
    if(!tipo) return false;
    // Levantamiento sin modalidad elegida = paso a medias (Daniel 2026-08-06:
    // la modalidad NO viene preseleccionada, manda el desplegable).
    if(tipo === 'levantamiento') return _TKOT.modo === 'equipos' || _TKOT.modo === 'descubrimiento';
    return true;
  },
  2: function(){
    const val = function(id){ const e = document.getElementById(id); return (e && e.value || '').trim(); };
    return !!(val('levSelectTitulo') && val('levDireccion') && val('levContactoNombre'));
  },
  3: function(){
    const e = document.getElementById('levFechaProg');
    return !!(e && e.value);
  },
  4: function(){ return _TKOT.tecnicosSel.size > 0; },
  5: function(){
    const tipo = (document.getElementById('otTipo') || {}).value || '';
    if(tipo === 'levantamiento' && _TKOT.modo === 'descubrimiento') return true;
    // Instalación sin ficha (forzarTodosEquipos) pinta los checkbox como
    // "checked disabled" -> igual entran en este conteo, sin caso especial.
    return document.querySelectorAll('.lev-eq-chk:checked').length > 0;
  },
  6: function(){
    const val = function(id){ const e = document.getElementById(id); return (e && e.value || '').trim(); };
    return !!(val('acceso_ascensor') || val('acceso_estacionamiento') || val('acceso_piso') || val('acceso_notas'));
  },
  7: function(){ return (_TKOT.adjuntos || []).length > 0; },
};
const TKOT_STEPS_OPCIONALES = { 6: true, 7: true };

function tkotRefreshStepStates(){
  Object.keys(TKOT_STEP_RULES).forEach(function(n){
    const card = document.getElementById('tkotStep' + n);
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
// tkotTipoChange/tkotModoSet/tkotRecalcEqCount/etc., se escucha en
// delegación sobre el modal completo. Así también quedan cubiertos los
// campos que se escriben a mano (título, dirección, piso, notas) y
// cualquier control que se agregue al modal en el futuro sin acordarse de
// llamar a tkotRefreshStepStates(). El setTimeout(0) deja que corran
// primero los onclick/onchange inline del propio HTML.
(function(){
  const _m = document.getElementById('modalGenerarOT');
  if(!_m) return;
  const _tick = function(){ setTimeout(tkotRefreshStepStates, 0); };
  ['input', 'change', 'click'].forEach(function(ev){ _m.addEventListener(ev, _tick, true); });
})();

// ── Si el modal "Generar OT" se cierra con el detalle de un bloque abierto,
//    el overlay (que vive en <body>, fuera de la pila de Bootstrap) quedaría
//    huérfano tapando la pantalla. Se limpia siempre.
document.getElementById('modalGenerarOT').addEventListener('hide.bs.modal', function(){
  tkdayCerrarDetalle();
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
  let el = document.getElementById('levFechaProgHint');
  if (el) return el;
  const input = document.getElementById('levFechaProg');
  if (!input) return null;
  el = document.createElement('div');
  el.id = 'levFechaProgHint';
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

    const input = document.getElementById('levFechaProg');
    if (input && !fechaPresetExplicita){
      input.value = sugerida;
      if (typeof tkotFechaProgChange === 'function') tkotFechaProgChange();
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
document.getElementById('modalGenerarOT').addEventListener('show.bs.modal', async function(){
  const tbody = document.getElementById('levSelectTbody');
  tbody.innerHTML = '<tr><td colspan="3" class="text-muted small text-center py-3">Cargando…</td></tr>';
  document.getElementById('tkotSinFichaWarn').style.display = 'none';

  // Modo cliente: los equipos salen del DOM de la ficha (ya renderizado),
  // no de un ticket. Se lee ANTES de tkotRenderEquipos()/tkotAplicarForzadoInstalacion()
  // más abajo, que son los que efectivamente pintan el Paso 5.
  if (_TKOT_MODO_CLIENTE) equiposCache = _tkotLeerEquiposDesdeDOM();

  await _tkotResolverCliente();
  await _tkotCargarPlantillas();
  await _tkotCargarContactos();
  _tkotRenderContactosSelector();
  if(_TKOT.contactos.lista.length > 0){
    const sel = document.getElementById('levContactoSel');
    sel.value = '0';
    tkotContactoChange();
  } else {
    // Sin ficha o sin contactos registrados -> manual, prellenado con lo
    // que ya declaró el ticket (nombre_contacto/phone/email). En modo
    // cliente ticketActual es null -> queda en blanco para que el usuario
    // lo llene (mismo comportamiento que tenía el modal viejo
    // #modalLevSelector, que tampoco prellenaba estos 3 campos).
    const t = ticketActual || {};
    document.getElementById('levContactoSel').value = '__manual';
    tkotContactoChange();
    document.getElementById('levContactoNombre').value = t.nombre_contacto || '';
    document.getElementById('levContactoTel').value = t.phone || '';
    document.getElementById('levContactoEmail').value = t.email || '';
  }

  // Tipo de OT: en modo ticket siempre 'levantamiento'. En modo cliente,
  // respeta el preset que dejó abrirLevantamientoSelector() (mant_ficha.js)
  // -- ej. "Programar mantención" pide 'preventiva' -- igual que hacía el
  // modal viejo #modalLevSelector.
  const tipoSel = document.getElementById('otTipo');
  if(tipoSel){
    const _tipoWanted = (_TKOT_MODO_CLIENTE && _TKOT.pendingTipoPreset) || 'levantamiento';
    const _tipoHas = Array.from(tipoSel.options).some(function(o){ return o.value === _tipoWanted; });
    tipoSel.value = _tipoHas ? _tipoWanted : 'levantamiento';
  }
  // Sin preselección de modalidad: tkotTipoChange() la deja limpia y el
  // usuario elige (Daniel 2026-08-06). Antes aquí se marcaba 'equipos'.
  tkotTipoChange();

  // 2026-07-19 (Daniel): la OT hereda el contexto del ticket -- si Notas
  // está vacío, precargar con la descripción del ticket (no se pisa si ya
  // hay algo tipeado, ej. reabrir el modal tras editar a mano). En modo
  // cliente ticketActual es null -> queda en '' (mismo default que el
  // modal viejo).
  const levSelectNotasEl = document.getElementById('levSelectNotas');
  if(!levSelectNotasEl.value.trim()){
    levSelectNotasEl.value = (ticketActual || {}).descripcion || '';
  }

  // Dirección: en modo ticket, default = la del ticket (jobsite real). En
  // modo cliente, default = la del cliente (mismo comportamiento que tenía
  // abrirLevantamientoSelector() en el modal viejo) -- editable de todas
  // formas, Google Maps valida al elegir una sugerencia.
  const dirInput = document.getElementById('levDireccion');
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
        const hint = document.getElementById('levDireccionHint');
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
  const _fechaPreset = (_TKOT_MODO_CLIENTE && _TKOT.pendingFechaPreset && /^\d{4}-\d{2}-\d{2}$/.test(_TKOT.pendingFechaPreset))
    ? _TKOT.pendingFechaPreset : null;
  const _fechaDef = _fechaPreset || _hoyStr;
  document.getElementById('levFechaProg').value = _fechaDef;
  document.getElementById('levFechaFin').value = '';
  document.getElementById('levRangoDias').checked = false;
  document.getElementById('levFechaFinWrap').style.display = 'none';
  document.getElementById('levHoraIni').value = '09:00';
  document.getElementById('levHoraFin').value = '13:00';
  // Chips de duración (2026-07-19, P9): el reset original NO limpiaba las
  // horas del "último día" del rango -- quedaban con el valor de la OT
  // anterior. Aditivo, no quita nada (REGLA #4.2).
  document.getElementById('levHoraIniFin').value = '09:00';
  document.getElementById('levHoraFinFin').value = '13:00';
  _TKOT.durN = 1;
  if (typeof levChipsRefresh === 'function') levChipsRefresh();
  // Presets consumidos -- se limpian para no "pegarse" en la próxima
  // apertura del modal (ej. abrir por el botón normal después de haber
  // venido de la agenda del Plan Anual).
  _TKOT.pendingTipoPreset = null;
  _TKOT.pendingFechaPreset = null;

  tkotCalInit();

  // BUG2 (portado del modal viejo, 2026-06-23): sugerir la fecha según el
  // "día preferido" de mantención del cliente -- solo aplica en modo
  // cliente (un ticket no tiene "día preferido"). Falla en silencio.
  if (_TKOT_MODO_CLIENTE) _tkotSugerirDiaPreferido(!!_fechaPreset);

  _TKOT.tecnicosSel.clear();
  const tBtn = document.getElementById('btnLevToggleTodos');
  if(tBtn) tBtn.innerHTML = '<i class="bi bi-check2-square me-1"></i>Marcar todos';

  tkotResetAccesoLogistica();
  tkotResetAdjuntos();

  tkotRenderEquipos();
  tkotAplicarForzadoInstalacion();
  tkotRenderTecnicos();

  try{
    const r = await fetch('/mantenciones/api/tecnicos');
    const d = await r.json();
    _TKOT.tecnicosDisponibles = Array.isArray(d) ? d : (d.tecnicos || []);
    tkotRenderTecnicos();
  }catch(e){
    document.getElementById('levTecnicosBox').innerHTML = '<span class="text-danger small">⚠ No se pudieron cargar los técnicos</span>';
  }

  // Estado inicial de los 7 pasos (2026-08-12): se calcula al final, con
  // todo ya prellenado (tipo, dirección, contacto, fecha, equipos).
  tkotRefreshStepStates();

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
  const box = document.getElementById('otCotizacionRef');
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
async function tkotGenerar(){
  const tipoSel = document.getElementById('otTipo')?.value || 'levantamiento';
  const eqs = equiposCache || [];
  const keysMarcados = _TKOT.forzarTodosEquipos
    ? eqs.map(function(e){ return _tkotEqKey(e); })
    : Array.from(document.querySelectorAll('.lev-eq-chk:checked')).map(function(c){ return c.dataset.key; });

  let esDescubrimiento = (tipoSel === 'levantamiento' && _TKOT.modo === 'descubrimiento');
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
    tkotModoSet('descubrimiento');
  }

  const fechaProg = document.getElementById('levFechaProg').value;
  if(!fechaProg){ ilusToast('Indica la fecha programada', {type:'warning'}); return; }
  const horaIni = document.getElementById('levHoraIni').value || '';
  const horaFin = document.getElementById('levHoraFin').value || '';
  if(horaIni && horaFin && horaIni >= horaFin){ ilusToast('La hora de término debe ser posterior a la de inicio', {type:'warning'}); return; }
  const usaRango = document.getElementById('levRangoDias').checked;
  let fechaFin = '';
  if(usaRango){
    fechaFin = document.getElementById('levFechaFin').value;
    if(!fechaFin){ ilusToast('Indica la fecha de término', {type:'warning'}); return; }
    if(fechaFin < fechaProg){ ilusToast('La fecha de término no puede ser anterior a la de inicio', {type:'warning'}); return; }
  }

  const dirVal = (document.getElementById('levDireccion')?.value || '').trim();
  if(!dirVal){ ilusToast('Indica la dirección de la visita', {type:'warning'}); document.getElementById('levDireccion')?.focus(); return; }
  const contactoNombre = (document.getElementById('levContactoNombre')?.value || '').trim();
  if(!contactoNombre){ ilusToast('Indica el contacto que recibirá al técnico en sitio', {type:'warning'}); document.getElementById('levContactoSel')?.focus(); return; }

  const tecnicoIds = Array.from(_TKOT.tecnicosSel);
  if(!tecnicoIds.length){
    const hay = (_TKOT.tecnicosDisponibles||[]).length > 0;
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
    if(_TKOT.eqPlantillas[key] && _TKOT.eqPlantillas[key].size > 0) plantillasPorEq[key] = Array.from(_TKOT.eqPlantillas[key]);
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
      cliente_id: _TKOT.cid,
      titulo: document.getElementById('levSelectTitulo').value.trim(),
      notas: document.getElementById('levSelectNotas').value.trim(),
      equipo_ids: equipoIds,
      equipos_ticket: equiposTicket,
      descubrimiento: esDescubrimiento,
      fecha_programada: fechaProg,
      hora_inicio: horaIni || null,
      hora_fin: horaFin || null,
      fecha_fin: usaRango ? fechaFin : null,
      hora_inicio_fin: usaRango ? (document.getElementById('levHoraIniFin')?.value || null) : null,
      hora_fin_fin: usaRango ? (document.getElementById('levHoraFinFin')?.value || null) : null,
      tecnico_ids: tecnicoIds,
      tipo_ot: tipoSel,
      aplica_garantia: document.getElementById('otAplicaGarantia')?.checked || false,
      plantillas_por_equipo: plantillasPorEq,
      direccion_visita: dirVal,
      direccion_lat: parseFloat(document.getElementById('levDireccion')?.dataset.lat) || null,
      direccion_lng: parseFloat(document.getElementById('levDireccion')?.dataset.lng) || null,
      direccion_place_id: document.getElementById('levDireccion')?.dataset.placeId || null,
      contacto_nombre: contactoNombre,
      contacto_cargo: (document.getElementById('levContactoCargo')?.value || '').trim(),
      contacto_tel: (document.getElementById('levContactoTel')?.value || '').trim(),
      contacto_email: (document.getElementById('levContactoEmail')?.value || '').trim(),
      contacto_origen: document.getElementById('levContactoSel')?.dataset.origen || 'manual',
      acceso_ascensor: document.getElementById('acceso_ascensor')?.value || null,
      acceso_estacionamiento: document.getElementById('acceso_estacionamiento')?.value || null,
      acceso_piso: (document.getElementById('acceso_piso')?.value || '').trim(),
      acceso_notas: (document.getElementById('acceso_notas')?.value || '').trim(),
      forzar_feriado: false,
      forzar_choque: false,
    };
    // FIX 2026-08-11: el selector de plantilla del Paso 1 se retiró (quedaba
    // duplicado con "Plantillas extra" del Paso 5, por equipo). Ya no se
    // manda plantilla_id a nivel de OT -- el backend calcula la estándar
    // con _plantilla_estandar_para_tipo, y las plantillas extra por equipo
    // (equipos_plantillas, más abajo) siguen funcionando igual que siempre.

    // Modo ticket -> POST /tickets/api/tickets/<TID>/generar-ot (exige un
    // ticket real, vincula tk_tickets.visita_id). Modo cliente -> POST
    // /mantenciones/api/clientes/<CID>/levantamientos (el endpoint real del
    // modal viejo #modalLevSelector). Mismo payload para ambos -- los dos
    // delegan a _mant_lev_crear_ot_core (app.py); `ticket_id`/`cliente_id`
    // sobrantes en el payload los ignora el endpoint que no los necesita.
    const _tkotUrl = _TKOT_MODO_CLIENTE
      ? '/mantenciones/api/clientes/' + CID + '/levantamientos'
      : '/tickets/api/tickets/' + TID + '/generar-ot';
    let d;
    for(let intento = 0; intento < 2; intento++){
      const r = await fetch(_tkotUrl, {
        method: 'POST', headers: {'Content-Type':'application/json'},
        body: JSON.stringify(payload)
      });
      d = await r.json();
      if(d.ok || !d.requiere_confirmacion) break;

      const adv = d.advertencias || {};
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
    if(!d.ok){ ilusToast(d.error || 'No se pudo generar la OT', {type:'error'}); return; }

    const visitaId = d.visita_id;
    const modalInst = bootstrap.Modal.getInstance(document.getElementById('modalGenerarOT'));
    if(modalInst){
      await new Promise(function(resolve){
        let resolved = false;
        const done = function(){ if(!resolved){ resolved = true; resolve(); } };
        document.getElementById('modalGenerarOT').addEventListener('hidden.bs.modal', done, {once:true});
        modalInst.hide();
        setTimeout(done, 600);
      });
    }
    if(typeof ilusCleanModalBackdrops === 'function') ilusCleanModalBackdrops();

    let adjResult = null;
    if(_TKOT.adjuntos.length > 0 && visitaId) adjResult = await _tkotSubirAdjuntos(visitaId);

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
    tkotResetAccesoLogistica();
    tkotResetAdjuntos();
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

// ── Tarjeta de estado en Acciones (llamada desde cargar()) ──
// 2026-07-15: el acceso a "Generar OT" ahora se pinta en DOS lugares -- la
// barra de la tarjeta "Equipo(s)" (el original, Regla #4.2: se queda) y el
// acceso destacado de la columna lateral (Daniel: "no veo el formulario
// dentro del ticket"). La DECISIÓN vive en un solo lugar (esta función):
// si el ticket ya tiene visita_id hay OT y se muestra el número + link; si
// no, se muestra el botón que abre #modalGenerarOT. Los dos lugares reciben
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
// #modalGenerarOT no depende de ninguna de las dos llamadas (los técnicos
// del Paso 4 salen de GET /mantenciones/api/tecnicos, disparado por su
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
        const _mOt = document.getElementById('modalGenerarOT');
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

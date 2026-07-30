// ════════════════════════════════════════════════════════
//  HERO SLIDESHOW
// ════════════════════════════════════════════════════════
let _heroIdx = 0;
const _heroSlides = document.querySelectorAll('.hero-slide');
const _heroDots = document.querySelectorAll('.hero-slide-dot');
const _motionReduced = !!(window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches);
function setHero(i){
  if (!_heroSlides.length) return;
  _heroIdx = (i + _heroSlides.length) % _heroSlides.length;
  _heroSlides.forEach((s,k) => s.classList.toggle('active', k === _heroIdx));
  _heroDots.forEach((d,k) => d.classList.toggle('active', k === _heroIdx));
}
_heroDots.forEach((d,i) => d.addEventListener('click', () => setHero(i)));
// 2026-06-09: autoplay solo si hay >1 slide y el usuario no pidió reduced-motion.
// Pausa al hover (antes era un setInterval incondicional).
let _heroTimer = null;
function _heroPlay(){
  if (_heroTimer || _motionReduced || _heroSlides.length < 2) return;
  _heroTimer = setInterval(() => setHero(_heroIdx + 1), 4500);
}
function _heroPause(){
  if (_heroTimer){ clearInterval(_heroTimer); _heroTimer = null; }
}
const _heroBox = document.getElementById('heroSlides');
if (_heroBox){
  _heroBox.addEventListener('mouseenter', _heroPause);
  _heroBox.addEventListener('mouseleave', _heroPlay);
}
_heroPlay();

// ════════════════════════════════════════════════════════
//  ROTADOR DE FRASES DEL H1 (2026-06-09)
//  La línea roja rota entre 4 frases con fade. Estático si el
//  usuario prefiere reduced-motion.
// ════════════════════════════════════════════════════════
(function(){
  const el = document.getElementById('heroRotator');
  if (!el || _motionReduced) return;
  const frases = ['LISTO PARA RETIRAR', 'SIN FILAS NI PAPELEO', 'CON SEGUIMIENTO ONLINE', 'EN 3 PASOS SIMPLES'];
  let idx = 0;
  setInterval(() => {
    el.classList.add('rot-out');
    setTimeout(() => {
      idx = (idx + 1) % frases.length;
      el.textContent = frases[idx];
      el.classList.remove('rot-out');
    }, 360);
  }, 3600);
})();

// ════════════════════════════════════════════════════════
//  COUNT-UP DE STATS (2026-06-09)
//  Anima los números del stats-strip al entrar al viewport.
//  IntersectionObserver + requestAnimationFrame; con
//  reduced-motion (o sin IO) pinta el valor final directo.
// ════════════════════════════════════════════════════════
(function(){
  const els = document.querySelectorAll('[data-countup]');
  if (!els.length) return;
  function setFinal(el){
    el.textContent = (el.dataset.countup || '') + (el.dataset.suffix || '');
  }
  function animate(el){
    const target = parseFloat(el.dataset.countup) || 0;
    const suffix = el.dataset.suffix || '';
    if (_motionReduced){ setFinal(el); return; }
    const dur = 1100;
    let start = null;
    function frame(ts){
      if (start === null) start = ts;
      const p = Math.min(1, (ts - start) / dur);
      const eased = 1 - Math.pow(1 - p, 3);   // ease-out cúbico
      el.textContent = Math.round(target * eased) + suffix;
      if (p < 1) requestAnimationFrame(frame);
    }
    requestAnimationFrame(frame);
  }
  if (!('IntersectionObserver' in window) || _motionReduced){
    els.forEach(setFinal);
    return;
  }
  const io = new IntersectionObserver((entries) => {
    entries.forEach(en => {
      if (en.isIntersecting){
        animate(en.target);
        io.unobserve(en.target);
      }
    });
  }, { threshold: 0.4 });
  els.forEach(el => io.observe(el));
})();

// ════════════════════════════════════════════════════════
//  IR A SEGUIMIENTO (banner mini)
// ════════════════════════════════════════════════════════
function irASeguimiento(){
  const v = document.getElementById('trackingLink').value.trim();
  if (!v){
    if (typeof ilusToast === 'function'){
      ilusToast('Pega el enlace o código RET-XXXXXX que recibiste por email', { type: 'info' });
    } else {
      alert('Pega el enlace o el código RET-XXXXXX que recibiste por email');
    }
    return;
  }
  // Si es URL completa, redirige tal cual; si es código, redirige a búsqueda
  if (v.startsWith('http') || v.includes('/retiros/seguimiento/')){
    location.href = v;
    return;
  }
  // FIX 2026-06-09: los códigos nuevos son ALFANUMÉRICOS (ej: RET-CCE24P).
  // El regex anterior (/^RET-\d+$/) solo aceptaba dígitos y mandaba los
  // códigos nuevos a la ruta de token (404). Trim + uppercase antes de evaluar.
  const code = v.toUpperCase().trim();
  if (/^RET-[A-Z0-9]{4,}$/.test(code)){
    location.href = '/retiros/buscar?code=' + encodeURIComponent(code);
  } else {
    // asumimos token
    location.href = '/retiros/seguimiento/' + encodeURIComponent(v);
  }
}

// ════════════════════════════════════════════════════════
//  VALIDACIÓN RUT CHILENO (módulo 11)
// ════════════════════════════════════════════════════════
function cleanRUT(rut){ return String(rut||'').replace(/[^0-9kK]/g,'').toUpperCase(); }
function calcDV(num){
  let suma = 0, mul = 2;
  for (let i = num.length-1; i >= 0; i--){
    suma += parseInt(num[i],10) * mul;
    mul = mul === 7 ? 2 : mul + 1;
  }
  const r = 11 - (suma % 11);
  if (r === 11) return '0';
  if (r === 10) return 'K';
  return String(r);
}
function isValidRUT(rut){
  const c = cleanRUT(rut);
  if (c.length < 8 || c.length > 9) return false;
  return /^\d+$/.test(c.slice(0,-1)) && calcDV(c.slice(0,-1)) === c.slice(-1);
}
function formatRUT(input){
  const c = cleanRUT(input.value);
  if (c.length < 2){ input.value = c; return; }
  const num = c.slice(0,-1), dv = c.slice(-1);
  let f = '', rev = num.split('').reverse().join('');
  for (let i = 0; i < rev.length; i++){
    f = rev[i] + f;
    if ((i+1) % 3 === 0 && i !== rev.length-1) f = '.' + f;
  }
  input.value = f + '-' + dv;
}
// onRutInput: formatea + actualiza panel declaración mientras el usuario tipea
function onRutInput(input){
  formatRUT(input);
  // No mostrar errores mientras tipea — solo limpiar estados rojos
  const errEl = document.getElementById(input.id + '_err');
  const okEl  = document.getElementById(input.id + '_ok');
  input.classList.remove('field-valid', 'field-invalid');
  if (errEl) errEl.style.display = 'none';
  if (okEl) okEl.style.display = 'none';
  // Si los dos RUTs ya están válidos, refrescamos panel de declaración
  checkTerceroPanel();
}
function validateRUT(input){
  const has = input.value.length > 0;
  const ok = has && isValidRUT(input.value);
  const errEl = document.getElementById(input.id + '_err');
  const okEl  = document.getElementById(input.id + '_ok');
  input.classList.toggle('field-valid', ok);
  input.classList.toggle('field-invalid', has && !ok);
  if (errEl) errEl.style.display = (has && !ok) ? 'block' : 'none';
  if (okEl){
    if (ok){
      okEl.textContent = '✓ RUT ' + input.value;
      okEl.style.display = 'block';
    } else {
      okEl.style.display = 'none';
    }
  }
  return ok;
}

// ════════════════════════════════════════════════════════
//  VALIDACIÓN EMAIL (robusto: regex + chequeos extra)
// ════════════════════════════════════════════════════════
// Regex alineado con backend (_PICKUP_EMAIL_RE en pickups_module.py):
//   user@dominio.tld con TLD de 2+ letras.
// Rechaza: doble @, espacios, sin TLD, más de 200 chars.
const _EMAIL_RX = /^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$/;
function isValidEmail(email){
  if (!email) return false;
  const e = String(email).trim().toLowerCase();
  if (!e || e.length > 200) return false;
  if (e.indexOf(' ') !== -1) return false;
  // Exactamente un @
  if ((e.match(/@/g) || []).length !== 1) return false;
  if (!_EMAIL_RX.test(e)) return false;
  return true;
}
function onEmailInput(input){
  // Limpiar estados visuales mientras tipea (sin marcar rojo prematuramente)
  const errEl = document.getElementById(input.id + '_err');
  const okEl  = document.getElementById(input.id + '_ok');
  input.classList.remove('field-valid', 'field-invalid');
  if (errEl) errEl.style.display = 'none';
  if (okEl) okEl.style.display = 'none';
}
function validateEmail(input){
  const has = input.value.length > 0;
  const ok = has && isValidEmail(input.value);
  const errEl = document.getElementById(input.id + '_err');
  const okEl  = document.getElementById(input.id + '_ok');
  input.classList.toggle('field-valid', ok);
  input.classList.toggle('field-invalid', has && !ok);
  if (errEl) errEl.style.display = (has && !ok) ? 'block' : 'none';
  if (okEl){
    if (ok){
      // Normalizar a lowercase
      input.value = input.value.trim().toLowerCase();
      okEl.textContent = '✓ Email válido';
      okEl.style.display = 'block';
    } else {
      okEl.style.display = 'none';
    }
  }
  return ok;
}

// ════════════════════════════════════════════════════════
//  VALIDACIÓN TELÉFONO CL
// ════════════════════════════════════════════════════════
function cleanPhone(p){ return String(p||'').replace(/[^\d+]/g,''); }
function isValidCLPhone(p){
  const c = cleanPhone(p).replace(/^\+/,'');
  return /^(56)?9\d{8}$/.test(c);
}
function formatPhone(input){
  // Solo limpia estados visuales mientras el usuario tipea.
  // NO reescribimos input.value aquí — hacerlo en oninput destruye
  // la posición del cursor y hace imposible editar en medio del número.
  const errEl = document.getElementById(input.id + '_err');
  const okEl  = document.getElementById(input.id + '_ok');
  input.classList.remove('field-valid', 'field-invalid');
  if (errEl) errEl.style.display = 'none';
  if (okEl) okEl.style.display = 'none';
}
function phoneOnFocus(input){
  // Al enfocar: mostrar solo dígitos para que el usuario pueda editar libremente.
  // Si el campo tiene "+56 9 1234 5678", lo convierte en "912345678" para edición.
  let c = cleanPhone(input.value).replace(/^\+/,'');
  if (c.startsWith('56') && c.length > 2) c = c.slice(2);
  if (c.length > 0){
    input.value = c;
    setTimeout(() => input.select(), 0); // seleccionar todo para fácil reemplazo
  }
}
function phoneOnBlur(input){
  // Al salir del campo: normaliza a "+56 9 XXXX XXXX" y luego valida.
  let c = cleanPhone(input.value).replace(/^\+/,'');
  if (c.startsWith('56') && c.length > 2) c = c.slice(2);
  if (c.length > 9) c = c.slice(0, 9);
  if (c.length > 0){
    let out = '+56 ';
    if (c.length >= 1) out += c[0];
    if (c.length >= 2) out += ' ' + c.slice(1, 5);
    if (c.length >= 6) out += ' ' + c.slice(5, 9);
    input.value = out;
  } else {
    input.value = '';
  }
  validatePhone(input);
}
function validatePhone(input){
  const has = input.value.length > 0;
  const ok = has && isValidCLPhone(input.value);
  const errEl = document.getElementById(input.id + '_err');
  const okEl  = document.getElementById(input.id + '_ok');
  input.classList.toggle('field-valid', ok);
  input.classList.toggle('field-invalid', has && !ok);
  if (errEl) errEl.style.display = (has && !ok) ? 'block' : 'none';
  if (okEl){
    if (ok){
      okEl.textContent = '✓ ' + input.value;
      okEl.style.display = 'block';
    } else {
      okEl.style.display = 'none';
    }
  }
  return ok;
}

// ════════════════════════════════════════════════════════
//  PANEL DECLARACIÓN AUTORIZACIÓN TERCERO
//  Aparece si RUT cliente != RUT quien retira, ambos válidos.
//  Bloquea submit hasta que el checkbox esté marcado.
// ════════════════════════════════════════════════════════
function checkTerceroPanel(){
  const panel = document.getElementById('declTercero');
  if (!panel) return;
  const authActive = document.getElementById('auth_active').value === '1';
  const cliRut = document.getElementById('customer_rut').value;
  const retRut = document.getElementById('pickup_person_rut').value;
  const cliName = document.querySelector('input[name=customer_name]').value.trim();
  const retName = document.getElementById('auth_name').value.trim();

  // Sólo mostramos panel si: toggle activo + ambos RUTs válidos + son distintos
  const muestraPanel = authActive
                    && isValidRUT(cliRut) && isValidRUT(retRut)
                    && cleanRUT(cliRut) !== cleanRUT(retRut);

  if (muestraPanel){
    document.getElementById('declClienteName').textContent = cliName || '(sin nombre)';
    document.getElementById('declClienteRut').textContent  = cliRut ? '· RUT ' + cliRut : '';
    document.getElementById('declTerceroName').textContent = retName || '(sin nombre)';
    document.getElementById('declTerceroRut').textContent  = retRut ? '· RUT ' + retRut : '';
    panel.classList.add('show');
  } else {
    panel.classList.remove('show');
    // Si el panel se oculta, desmarcamos por seguridad para forzar nueva confirmación
    const cb = document.getElementById('acepta_tercero');
    if (cb) cb.checked = false;
    const lbl = document.getElementById('declTerceroLabel');
    if (lbl) lbl.classList.remove('checked');
  }
}
function onDeclTerceroChange(){
  const cb = document.getElementById('acepta_tercero');
  const lbl = document.getElementById('declTerceroLabel');
  if (cb && lbl) lbl.classList.toggle('checked', cb.checked);
}

// ════════════════════════════════════════════════════════
//  SELECTOR DE SLOTS INTELIGENTE (v2 — mayo 2026)
//  - Cada bloque dura 30 min (slot_minutes del backend).
//  - Cliente selecciona N bloques contiguos.
//  - Duración = N x 30 min (NO se suma media hora extra).
//  - Bloques de colación NO clickeables.
//  - Bloques 'completo' (2/2) NO clickeables.
//  - Bloques 'ocupado' (1/2) SÍ clickeables con badge.
//  - Botones 1h/2h/3h preseleccionan desde primer disponible.
// ════════════════════════════════════════════════════════
let _disponibilidad = null;
let _slotsDelDia    = [];   // [{time_from,time_to,estado,puede_iniciar,...}]
let _slotStartIdx   = null;
let _slotEndIdx     = null;

// Helper: toast (con fallback)
function _showToast(msg, type){
  if (typeof ilusToast === 'function'){
    ilusToast(msg, { type: type || 'warning' });
  } else {
    console.warn('[ILUS] toast:', msg);
  }
}

// Helpers HH:MM ↔ minutos
function _hmToMin(hm){
  const parts = String(hm || '').split(':').map(Number);
  return (parts[0] || 0) * 60 + (parts[1] || 0);
}
function _minToHM(min){
  return `${String(Math.floor(min/60)).padStart(2,'0')}:${String(min%60).padStart(2,'0')}`;
}

async function cargarDisponibilidad() {
  const msg   = document.getElementById('cal_fecha_msg');
  const retry = document.getElementById('calRetryBtn');
  try {
    const r = await fetch('/retiros/api/disponibilidad-publica');
    if (!r.ok) throw new Error('HTTP ' + r.status);
    const payload = await r.json();
    if (!payload || !payload.from || !payload.to || !payload.dias) throw new Error('payload incompleto');
    _disponibilidad = payload;
    // Límites min/max SIEMPRE del payload (hora Chile del servidor) —
    // NUNCA del reloj del navegador (bug timezone conocido).
    const fechaInput = document.getElementById('cal_fecha');
    fechaInput.min = _disponibilidad.from;
    fechaInput.max = _disponibilidad.to;
    _applyOperacionTexts();
    _initIlusCalendar();
    if (retry) retry.style.display = 'none';
    if (fechaInput.value){
      // Había una fecha elegida (ej: reintento o re-render con fd) → refrescar slots
      onFechaChange();
    } else if (msg){
      msg.textContent = '';
      msg.style.color = '';
    }
  } catch (e) {
    // FALLBACK (2026-06-09): si el API falla, el form ya NO queda muerto.
    // Mostramos el input date nativo como respaldo + botón Reintentar.
    _disponibilidad = null;
    const wrap = document.getElementById('ilusCalWrap');
    const fb   = document.getElementById('calFallback');
    if (wrap) wrap.style.display = 'none';
    if (fb) fb.style.display = '';
    if (retry) retry.style.display = 'inline-flex';
    if (msg){
      msg.textContent = 'No se pudo cargar la disponibilidad. Reintenta en unos segundos.';
      msg.style.color = '#dc2626';
    }
  }
}

// Reintento manual del fallback (botón "Reintentar disponibilidad")
async function reintentarDisponibilidad(){
  const btn = document.getElementById('calRetryBtn');
  if (btn){
    btn.disabled = true;
    btn.innerHTML = '<i class="bi bi-arrow-clockwise me-1"></i>Cargando…';
  }
  await cargarDisponibilidad();
  if (btn){
    btn.disabled = false;
    btn.innerHTML = '<i class="bi bi-arrow-clockwise me-1"></i>Reintentar disponibilidad';
  }
}

// Textos de horario y anticipación alimentados del payload.operacion.
// NOTA: el backend HOY no expone min_notice_hours en el payload — se lee
// defensivamente con fallback 24 (el texto actual). Si algún día se agrega
// al payload, esto lo toma solo.
function _applyOperacionTexts(){
  const op = (_disponibilidad && _disponibilidad.operacion) || {};
  const mn = parseInt(op.min_notice_hours, 10) || 24;
  const noticeEl = document.getElementById('noticeHelp');
  if (noticeEl) noticeEl.textContent = 'Necesitamos mínimo ' + mn + ' horas de anticipación para preparar tu pedido.';
  const horEl = document.getElementById('horarioAtencion');
  if (horEl && op.open_time && op.lunch_start && op.lunch_end && op.close_time){
    // Strings HH:MM controlados por el servidor — seguros para innerHTML
    horEl.innerHTML = 'Atendemos <strong>' + op.open_time + ' a ' + op.lunch_start +
                      '</strong> en la mañana y <strong>' + op.lunch_end + ' a ' + op.close_time +
                      '</strong> en la tarde, solo días hábiles.';
  }
}

// ════════════════════════════════════════════════════════
//  CALENDARIO CUSTOM ILUS (2026-06-09)
//  Grid mensual Lun-Dom pintado desde _disponibilidad.dias dentro
//  del horizonte [from, to] del API. Días no disponibles en gris
//  con candado y motivo al tocar; disponibles con punto de ocupación
//  (verde / ámbar / rojo según ocupacion_pct); selección en rojo ILUS.
//  El input #cal_fecha sigue siendo la fuente del valor (contrato
//  intacto con requested_date y el render de slots).
// ════════════════════════════════════════════════════════
const _CAL_MESES = ['enero','febrero','marzo','abril','mayo','junio',
                    'julio','agosto','septiembre','octubre','noviembre','diciembre'];
let _calCursor = null;   // { y, m } — mes visible (m: 0-11)

function _isoAddDays(iso, delta){
  const p = String(iso || '').split('-').map(Number);
  const dt = new Date(Date.UTC(p[0] || 1970, (p[1] || 1) - 1, (p[2] || 1) + delta));
  return dt.toISOString().slice(0, 10);
}
function _calMonthKey(y, m){ return y * 12 + m; }
function _calKeyOfIso(iso){
  return _calMonthKey(parseInt(iso.slice(0, 4), 10), parseInt(iso.slice(5, 7), 10) - 1);
}

function _initIlusCalendar(){
  if (!_disponibilidad || !_disponibilidad.from) return;
  const wrap = document.getElementById('ilusCalWrap');
  const fb   = document.getElementById('calFallback');
  if (!wrap) return;
  wrap.style.display = '';
  if (fb) fb.style.display = 'none';
  const sel = document.getElementById('cal_fecha').value;
  const base = (sel && sel >= _disponibilidad.from && sel <= _disponibilidad.to) ? sel : _disponibilidad.from;
  _calCursor = { y: parseInt(base.slice(0, 4), 10), m: parseInt(base.slice(5, 7), 10) - 1 };
  const prev = document.getElementById('calPrev');
  const next = document.getElementById('calNext');
  if (prev) prev.onclick = () => _calNav(-1);
  if (next) next.onclick = () => _calNav(1);
  _renderIlusCal();
}

function _calNav(dir){
  if (!_calCursor || !_disponibilidad) return;
  let m = _calCursor.m + dir, y = _calCursor.y;
  if (m < 0){ m = 11; y--; }
  if (m > 11){ m = 0; y++; }
  const k = _calMonthKey(y, m);
  if (k < _calKeyOfIso(_disponibilidad.from) || k > _calKeyOfIso(_disponibilidad.to)) return;
  _calCursor = { y: y, m: m };
  _renderIlusCal();
}

function _renderIlusCal(){
  if (!_calCursor || !_disponibilidad) return;
  const grid  = document.getElementById('calGrid');
  const label = document.getElementById('calMonthLabel');
  if (!grid) return;
  const y = _calCursor.y, m = _calCursor.m;
  if (label) label.textContent = _CAL_MESES[m] + ' ' + y;

  const fromIso = _disponibilidad.from, toIso = _disponibilidad.to;
  const curKey  = _calMonthKey(y, m);
  const prev = document.getElementById('calPrev');
  const next = document.getElementById('calNext');
  if (prev) prev.disabled = (curKey <= _calKeyOfIso(fromIso));
  if (next) next.disabled = (curKey >= _calKeyOfIso(toIso));

  // "Hoy" derivado del payload: from = mañana en hora Chile → hoy = from - 1.
  // (Marcador cosmético; nunca usamos el reloj del navegador para límites.)
  const todayIso = _isoAddDays(fromIso, -1);
  const selected = document.getElementById('cal_fecha').value;

  const daysInMonth = new Date(Date.UTC(y, m + 1, 0)).getUTCDate();
  const firstWd = (new Date(Date.UTC(y, m, 1)).getUTCDay() + 6) % 7;  // 0 = lunes

  grid.innerHTML = '';
  for (let b = 0; b < firstWd; b++){
    const ph = document.createElement('span');
    ph.className = 'ilus-cal-day is-empty';
    grid.appendChild(ph);
  }
  for (let d = 1; d <= daysInMonth; d++){
    const iso = y + '-' + String(m + 1).padStart(2, '0') + '-' + String(d).padStart(2, '0');
    const btn = document.createElement('button');
    btn.type = 'button';
    btn.className = 'ilus-cal-day';
    btn.appendChild(document.createTextNode(String(d)));
    const inRange = (iso >= fromIso && iso <= toIso);
    const dia = inRange ? (_disponibilidad.dias || {})[iso] : null;
    let aria = d + ' de ' + _CAL_MESES[m] + ' de ' + y;

    if (!inRange){
      btn.classList.add('is-out');
      btn.disabled = true;
      aria += ' — fuera del rango agendable';
    } else if (!dia || !dia.disponible){
      // No disponible: gris + candado + motivo visible al tocar
      btn.classList.add('is-off');
      const lock = document.createElement('span');
      lock.className = 'cal-lock';
      lock.innerHTML = '<i class="bi bi-lock-fill"></i>';
      btn.appendChild(lock);
      const razon = (dia && dia.razon) || 'Día no disponible';
      aria += ' — ' + razon;
      btn.setAttribute('aria-disabled', 'true');
      btn.addEventListener('click', () => {
        const msg = document.getElementById('cal_fecha_msg');
        if (msg){ msg.textContent = razon; msg.style.color = '#dc2626'; }
        _showToast(razon, 'info');
      });
    } else {
      const pct = Math.max(0, Math.min(100, parseInt(dia.ocupacion_pct, 10) || 0));
      const dot = document.createElement('span');
      dot.className = 'cal-dot';
      let nivel;
      if (pct >= 80){ dot.style.background = '#dc2626'; nivel = 'casi lleno'; }
      else if (pct >= 40){ dot.style.background = '#f59e0b'; nivel = 'ocupación media'; }
      else { dot.style.background = '#16a34a'; nivel = 'con cupos libres'; }
      btn.appendChild(dot);
      aria += ' — disponible, ' + nivel + ' (' + pct + '% ocupado)';
      btn.addEventListener('click', () => _calPickDay(iso));
    }
    if (iso === todayIso) btn.classList.add('is-today');
    if (selected && iso === selected) btn.classList.add('is-selected');
    btn.setAttribute('aria-label', aria);
    grid.appendChild(btn);
  }
}

function _calPickDay(iso){
  const inp = document.getElementById('cal_fecha');
  if (!inp) return;
  inp.value = iso;
  // Dispara el listener 'change' → onFechaChange() (requested_date + slots)
  // y los watchers de estado de los steps. Un solo camino para ambos modos.
  inp.dispatchEvent(new Event('change', { bubbles: true }));
  _renderIlusCal();
}

function onFechaChange() {
  const fecha = document.getElementById('cal_fecha').value;
  const msg   = document.getElementById('cal_fecha_msg');
  const grid  = document.getElementById('slotGrid');
  const quick = document.getElementById('slotQuickWrap');

  document.querySelector('input[name=requested_date]').value = fecha;
  clearSlots();

  if (!fecha || !_disponibilidad){
    grid.innerHTML = '<div class="text-center text-muted small w-100 py-3" style="grid-column:1/-1"><i class="bi bi-calendar3 me-1"></i>Selecciona una fecha primero</div>';
    quick.style.display = 'none';
    return;
  }

  // Horizonte mínimo desde el PAYLOAD (hora Chile del servidor) — antes se
  // usaba el reloj del navegador (bug timezone conocido). Comparación de
  // strings ISO YYYY-MM-DD: orden lexicográfico == orden cronológico.
  if (fecha < _disponibilidad.from){
    const mnH = parseInt(((_disponibilidad.operacion || {}).min_notice_hours), 10) || 24;
    msg.textContent = 'Necesitamos mínimo ' + mnH + ' horas de anticipación';
    msg.style.color = '#dc2626';
    const desde = _disponibilidad.from.split('-').reverse().join('/');
    grid.innerHTML = '<div class="text-center w-100 py-3" style="grid-column:1/-1;color:#dc2626"><i class="bi bi-exclamation-triangle me-1"></i>Elige una fecha desde el ' + desde + '</div>';
    quick.style.display = 'none';
    return;
  }

  const dia = _disponibilidad.dias[fecha];
  if (!dia || !dia.disponible){
    msg.textContent = (dia && dia.razon) || 'Dia sin cupos';
    msg.style.color = '#dc2626';
    grid.innerHTML = `<div class="text-center w-100 py-3" style="grid-column:1/-1;color:#dc2626"><i class="bi bi-x-circle me-1"></i>${(dia && dia.razon) || 'Dia sin cupos disponibles'}</div>`;
    quick.style.display = 'none';
    return;
  }

  _slotsDelDia = dia.slots || [];
  // Contar bloques que puedan INICIAR un rango
  const libres = _slotsDelDia.filter(s => s.puede_iniciar).length;
  msg.textContent = `${libres} bloque${libres===1?'':'s'} libre${libres===1?'':'s'} este dia`;
  msg.style.color = libres > 0 ? '#16a34a' : '#dc2626';

  renderSlotGrid();
  quick.style.display = libres > 0 ? 'flex' : 'none';
}

function renderSlotGrid(){
  const grid = document.getElementById('slotGrid');
  if (!_slotsDelDia.length){
    grid.innerHTML = '<div class="text-center text-muted small w-100 py-3" style="grid-column:1/-1">Sin bloques disponibles</div>';
    return;
  }
  // ── Daniel mayo 2026: separar visualmente Mañana (antes colación) y Tarde
  //    (después colación). Si hay slots en ambos lados, insertamos un header.
  const lunchStart = (_disponibilidad && _disponibilidad.lunch_start) ? _disponibilidad.lunch_start : '12:30';
  const lunchStartMin = _hmToMin(lunchStart);

  const slotHtml = (s, i) => {
    const cls = ['slot-item'];
    let badge = '';
    let title = '';
    let estado = s.estado;
    if (!estado){
      if (s.lunch) estado = 'colacion';
      else if (s.razon && !s.disponible) estado = 'bloqueado';
      else if (!s.disponible) estado = 'completo';
      else if ((s.ocupados || 0) > 0 && (s.ocupados || 0) < (s.max || 2)) estado = 'ocupado';
      else estado = 'disponible';
    }

    // Daniel 2026-05-24: el cliente NO debe ver detalle de quién ocupa ni
    // cuántos cupos quedan. Solo "disponible" o "no disponible" con candado.
    // La info de capacidad (1/2, 2/2) sigue visible para el operador interno
    // en internal_detail / calendario.html.
    if (estado === 'colacion'){
      cls.push('is-lunch', 'is-disabled');
      title = 'Hora no disponible';
    } else if (estado === 'completo'){
      // 2/2 = lleno → candado, NO clickeable.
      cls.push('is-full', 'is-disabled');
      title = 'Sin cupos disponibles';
    } else if (estado === 'ocupado'){
      // MEDIO CUPO (1/2): Juan Daniel 2026-06-05 — atendemos hasta 2 clientes
      // por bloque (dos agendas). Queda 1 lugar → SIGUE seleccionable.
      cls.push('is-busy');
      badge = '<span class="slot-badge">½</span>';
      title = 'Medio cupo · queda 1 lugar';
    } else if (estado === 'bloqueado'){
      cls.push('is-blocked', 'is-disabled');
      title = 'Hora no disponible';
    } else if (estado === 'no_disponible'){
      // FIX 2026-06-09: 'no_disponible' (hora pasada / min_notice) caía al
      // else y quedaba CLICKEABLE. Ahora va gris con candado, sin onclick.
      cls.push('is-blocked', 'is-disabled');
      title = 'Hora no disponible';
    } else {
      title = 'Disponible';
    }

    // Política 2026-06-05: selección de UN solo bloque (no rangos) —
    // se removieron las clases muertas is-start / is-end / is-in-range.
    if (_slotStartIdx !== null && i === _slotStartIdx){
      cls.push('is-selected');
    }

    const hora = s.time_from || s.hora || '';
    // Los deshabilitados NO llevan onclick (antes solo se filtraba en el handler)
    const disabled = cls.indexOf('is-disabled') !== -1;
    const action = disabled ? 'aria-disabled="true"' : `onclick="onSlotClick(${i})"`;
    return `<div class="${cls.join(' ')}" data-idx="${i}" ${action} title="${title}">${badge}${hora}</div>`;
  };

  // Buscar el índice del primer slot post-colación
  let postLunchIdx = -1;
  for (let i = 0; i < _slotsDelDia.length; i++){
    const s = _slotsDelDia[i];
    const startMin = _hmToMin(s.time_from || s.hora || '00:00');
    const isLunch = (s.estado === 'colacion') || s.lunch;
    if (!isLunch && startMin >= lunchStartMin){
      postLunchIdx = i; break;
    }
  }
  const hasMorning = _slotsDelDia.some((s, i) => postLunchIdx === -1 ? true : i < postLunchIdx);
  const hasAfternoon = postLunchIdx !== -1;

  let html = '';
  if (hasMorning && hasAfternoon){
    html += '<div class="slot-section-title">☀️ Mañana</div>';
    html += '<div class="slot-grid-inner">' + _slotsDelDia.slice(0, postLunchIdx).map((s, i) => slotHtml(s, i)).join('') + '</div>';
    html += '<div class="slot-section-title">🌤 Tarde</div>';
    html += '<div class="slot-grid-inner">' + _slotsDelDia.slice(postLunchIdx).map((s, j) => slotHtml(s, postLunchIdx + j)).join('') + '</div>';
  } else {
    html = '<div class="slot-grid-inner">' + _slotsDelDia.map((s, i) => slotHtml(s, i)).join('') + '</div>';
  }
  grid.innerHTML = html;
}

function onSlotClick(i){
  const s = _slotsDelDia[i];
  if (!s) return;
  let estado = s.estado;
  if (!estado){
    if (s.lunch) estado = 'colacion';
    else if (s.razon && !s.disponible) estado = 'bloqueado';
    else if (!s.disponible) estado = 'completo';
    else if ((s.ocupados || 0) > 0 && (s.ocupados || 0) < (s.max || 2)) estado = 'ocupado';
    else estado = 'disponible';
  }

  // Juan Daniel 2026-06-05: 'ocupado' (1/2 = MEDIO CUPO) SÍ es agendable —
  // atendemos hasta 2 clientes por bloque (dos agendas). Solo 'completo' (2/2),
  // colación, bloqueado y no_disponible (pasado / min_notice) quedan fuera.
  if (estado === 'colacion' || estado === 'completo' || estado === 'bloqueado' || estado === 'no_disponible'){
    _showToast('Esa hora ya no tiene cupos. Elige otro bloque.', 'warning');
    return;
  }
  // 'disponible' (libre) y 'ocupado' (medio cupo) son clickeables

  // ── POLÍTICA 2026-06-05 (Juan Daniel) ──────────────────────────────
  // El cliente elige UNA SOLA hora de llegada, en bloques de 30 min —
  // NO un rango. Cada clic fija ese único bloque; un clic en el mismo
  // bloque lo deselecciona. Es el estándar de retiros ILUS: "el cliente
  // no escoge un rango, llega a la hora que seleccionó".
  if (_slotStartIdx === i && _slotEndIdx === i){
    _slotStartIdx = _slotEndIdx = null;   // toggle: deseleccionar
  } else {
    _slotStartIdx = _slotEndIdx = i;      // SIEMPRE un único bloque de 30 min
  }
  updateSlotSummary();
  renderSlotGrid();
}

// 2026-06-09: removida setQuickRange() (rangos 1h/2h/3h) — código muerto desde
// la política 2026-06-05 de "un solo bloque de 30 min". Ningún botón la llamaba.

function clearSlots(){
  _slotStartIdx = _slotEndIdx = null;
  updateSlotSummary();
  renderSlotGrid();
}

function updateSlotSummary(){
  const txt = document.getElementById('slotSummaryText');
  const clearBtn = document.getElementById('clearSlotBtn');
  const summaryBox = document.getElementById('slotSummary');
  if (_slotStartIdx === null){
    txt.className = 'sum-empty';
    txt.textContent = 'No has seleccionado bloque aun';
    clearBtn.style.display = 'none';
    if (summaryBox) summaryBox.classList.remove('is-active');
    document.querySelector('input[name=requested_time_from]').value = '';
    document.querySelector('input[name=requested_time_to]').value = '';
    return;
  }
  const slotMin = (_disponibilidad && _disponibilidad.slot_minutes) ? _disponibilidad.slot_minutes : 30;
  const startSlot = _slotsDelDia[_slotStartIdx];
  const endSlot   = _slotsDelDia[_slotEndIdx];
  // time_from del INICIO, time_to del FIN (backend lo calcula).
  // Fallback (legacy): si no viene time_to, hora_del_slot + slotMin
  const startStr = startSlot.time_from || startSlot.hora;
  let endStr = endSlot.time_to;
  if (!endStr){
    const baseMin = _hmToMin(endSlot.time_from || endSlot.hora);
    endStr = _minToHM(baseMin + slotMin);
  }
  const nBlocks = _slotEndIdx - _slotStartIdx + 1;
  const totalMin = nBlocks * slotMin;
  let durTxt;
  if (totalMin < 60){
    durTxt = `${totalMin} min`;
  } else if (totalMin % 60 === 0){
    durTxt = `${totalMin/60} h`;
  } else {
    durTxt = `${(totalMin/60).toFixed(1)} h`;
  }

  txt.className = '';
  txt.innerHTML = `<i class="bi bi-clock-fill me-2" style="color:#16a34a"></i><span class="sum-range">${startStr} - ${endStr}</span><span class="sum-dur"><i class="bi bi-stopwatch me-1"></i>duración ${durTxt}</span>`;
  clearBtn.style.display = '';
  if (summaryBox) summaryBox.classList.add('is-active');
  document.querySelector('input[name=requested_time_from]').value = startStr;
  document.querySelector('input[name=requested_time_to]').value = endStr;
  // Refresca estado del step 4 (cuándo) — se vuelve verde si todo OK.
  if (typeof _refreshStepStates === 'function') _refreshStepStates();
}

// ════════════════════════════════════════════════════════
//  TOGGLE: ¿Autorizar a otra persona?
// ════════════════════════════════════════════════════════
function toggleAutorizado(){
  const cb = document.getElementById('authCheckbox');
  const wrap = document.getElementById('authToggle');
  const fields = document.getElementById('authFields');
  const flag = document.getElementById('auth_active');
  // Si fue click en el div, alternamos el checkbox
  if (event && event.target && event.target.id !== 'authCheckbox'){
    cb.checked = !cb.checked;
  }
  if (cb.checked){
    fields.style.display = 'block';
    wrap.classList.add('active');
    flag.value = '1';
  } else {
    fields.style.display = 'none';
    wrap.classList.remove('active');
    flag.value = '0';
    // Limpiar campos al desactivar para que no se envíen
    document.getElementById('auth_name').value = '';
    document.getElementById('pickup_person_rut').value = '';
    document.getElementById('auth_phone').value = '';
    // Resetear estados visuales y panel de declaración
    ['pickup_person_rut', 'auth_phone'].forEach(id => {
      const el = document.getElementById(id);
      if (el) el.classList.remove('field-valid', 'field-invalid');
    });
    const cbDecl = document.getElementById('acepta_tercero');
    if (cbDecl) cbDecl.checked = false;
  }
  checkTerceroPanel();
}

// ════════════════════════════════════════════════════════
//  VALIDACIÓN AL ENVIAR (con scroll + ilusToast)
// ════════════════════════════════════════════════════════
document.getElementById('form-solicitud').addEventListener('submit', function(e){
  // Si NO autorizó a otra persona, copiar datos del cliente al "autorizado"
  // (en backend se asume que el cliente mismo retira)
  const authActive = document.getElementById('auth_active').value === '1';
  if (!authActive){
    // El "contact_name" es el del cliente — usar customer_name
    document.getElementById('contact_name').value = document.querySelector('input[name=customer_name]').value;
  } else {
    // El "contact_name" sería el del autorizado
    document.getElementById('contact_name').value = document.getElementById('auth_name').value;
  }

  // ── Recolecta inválidos (con ID para scroll/foco) ──
  const errores = [];
  const invalidIds = [];

  // RUT del cliente
  const elCliRut = document.getElementById('customer_rut');
  if (!isValidRUT(elCliRut.value)){
    errores.push('RUT del cliente inválido — revisa el dígito verificador');
    invalidIds.push('customer_rut');
    elCliRut.classList.add('field-invalid');
    elCliRut.classList.remove('field-valid');
  }

  // Email del contacto
  const elEmail = document.getElementById('contact_email');
  if (!isValidEmail(elEmail.value)){
    errores.push('Email inválido — usa el formato nombre@dominio.cl');
    invalidIds.push('contact_email');
    elEmail.classList.add('field-invalid');
    elEmail.classList.remove('field-valid');
  }

  // Teléfono del contacto (normalizar antes de validar, por si no hubo blur)
  const elPhone = document.getElementById('contact_phone');
  phoneOnBlur(elPhone);
  if (!isValidCLPhone(elPhone.value)){
    errores.push('Teléfono inválido — debe ser móvil chileno (+56 9 XXXX XXXX)');
    invalidIds.push('contact_phone');
    elPhone.classList.add('field-invalid');
    elPhone.classList.remove('field-valid');
  }

  // Si autorizó a otra persona — validamos sus campos
  if (authActive){
    const elAuthName = document.getElementById('auth_name');
    if (!elAuthName.value.trim()){
      errores.push('Nombre del autorizado obligatorio');
      invalidIds.push('auth_name');
    }
    const elAuthRut = document.getElementById('pickup_person_rut');
    if (!isValidRUT(elAuthRut.value)){
      errores.push('RUT del autorizado inválido');
      invalidIds.push('pickup_person_rut');
      elAuthRut.classList.add('field-invalid');
      elAuthRut.classList.remove('field-valid');
    }
    const elAuthPhone = document.getElementById('auth_phone');
    if (elAuthPhone.value) phoneOnBlur(elAuthPhone);
    if (elAuthPhone.value && !isValidCLPhone(elAuthPhone.value)){
      errores.push('Teléfono del autorizado inválido');
      invalidIds.push('auth_phone');
      elAuthPhone.classList.add('field-invalid');
      elAuthPhone.classList.remove('field-valid');
    }

    // Si los RUTs son distintos, exigir checkbox de declaración
    if (isValidRUT(elCliRut.value) && isValidRUT(elAuthRut.value)
        && cleanRUT(elCliRut.value) !== cleanRUT(elAuthRut.value)){
      const cbDecl = document.getElementById('acepta_tercero');
      if (!cbDecl || !cbDecl.checked){
        errores.push('Debes marcar la declaración de autorización (el cliente autoriza a este tercero a retirar)');
        invalidIds.push('declTercero');
      }
    }
  }

  // Bloque horario
  if (!document.querySelector('input[name=requested_time_from]').value){
    errores.push('Selecciona al menos un bloque horario');
    invalidIds.push('slotPicker');
  }

  if (errores.length){
    e.preventDefault();

    // Scroll al primer inválido
    const firstId = invalidIds[0];
    const firstEl = firstId ? document.getElementById(firstId) : null;
    if (firstEl){
      firstEl.scrollIntoView({ behavior: 'smooth', block: 'center' });
      try { firstEl.focus({ preventScroll: true }); } catch(_){}
    }

    // Usar ilusToast (regla #1 ILUS). Fallback a alert si helper no cargó.
    if (typeof ilusToast === 'function'){
      ilusToast('Revisa los campos marcados en rojo (' + errores.length + ')',
                { type: 'warning', duration: 4500 });
    } else if (typeof ilusAlert === 'function'){
      ilusAlert({
        title: 'Revisa el formulario',
        message: 'Hay ' + errores.length + ' campo(s) con error',
        sub: errores.join(' · '),
        type: 'warning',
      });
    } else {
      alert('Revisa estos errores:\n• ' + errores.join('\n• '));
    }
    return false;
  }
});

// ════════════════════════════════════════════════════════
//  MICRO-INTERACTIONS PREMIUM
//  - IntersectionObserver: fade in cada step al entrar al viewport
//  - Watcher: marca step como completo cuando sus campos requeridos están válidos
//  - Submit con loading state + pulso
// ════════════════════════════════════════════════════════
function _initStepReveal(){
  const steps = document.querySelectorAll('.step-section');
  // Fallback: si no hay IntersectionObserver, mostrar todo
  if (!('IntersectionObserver' in window)){
    steps.forEach(s => s.classList.add('in-view'));
    return;
  }
  const obs = new IntersectionObserver((entries) => {
    entries.forEach(entry => {
      if (entry.isIntersecting){
        entry.target.classList.add('in-view');
        obs.unobserve(entry.target);
      }
    });
  }, { threshold: 0.08, rootMargin: '0px 0px -20px 0px' });
  steps.forEach((s, i) => {
    s.style.transitionDelay = (i * 60) + 'ms';
    obs.observe(s);
  });
  // Defensa: si por alguna razón el observer no marca los primeros pasos
  // en 800 ms (ej. browser viejo, scroll restoration), forzamos in-view.
  setTimeout(() => {
    steps.forEach(s => {
      if (!s.classList.contains('in-view')){
        const r = s.getBoundingClientRect();
        if (r.top < window.innerHeight) s.classList.add('in-view');
      }
    });
  }, 800);
}

// Marca un step como "completo" (círculo verde + check + pulso) si todos
// sus campos requeridos están válidos. Llamado en cada blur/change relevante.
function _isStepComplete(stepNum){
  switch (stepNum){
    case 1:{
      const dt = document.getElementById('document_type');
      const dn = document.getElementById('document_number');
      return !!(dt && dt.value && dn && dn.value.trim().length > 0);
    }
    case 2:{
      const name = document.getElementById('customer_name');
      const rut  = document.getElementById('customer_rut');
      const em   = document.getElementById('contact_email');
      const ph   = document.getElementById('contact_phone');
      if (!name || !name.value.trim()) return false;
      if (!rut || !isValidRUT(rut.value)) return false;
      if (!em  || !isValidEmail(em.value)) return false;
      if (!ph  || !isValidCLPhone(ph.value)) return false;
      return true;
    }
    case 3:{
      // Step 3 = autorizar tercero. Considerado "completo" cuando:
      //   (a) NO autorizó a otra persona (default — retiras tú) → siempre OK
      //   (b) SÍ autorizó: nombre + RUT válido + checkbox declaración (si aplica) marcado
      const flag = document.getElementById('auth_active');
      const auth = flag && flag.value === '1';
      if (!auth) return true;
      const an = document.getElementById('auth_name');
      const ar = document.getElementById('pickup_person_rut');
      if (!an || !an.value.trim()) return false;
      if (!ar || !isValidRUT(ar.value)) return false;
      // Si RUT cliente != RUT autorizado, exige checkbox de declaración
      const cRut = document.getElementById('customer_rut');
      if (cRut && isValidRUT(cRut.value) && cleanRUT(cRut.value) !== cleanRUT(ar.value)){
        const cb = document.getElementById('acepta_tercero');
        if (!cb || !cb.checked) return false;
      }
      return true;
    }
    case 4:{
      const fecha = document.getElementById('cal_fecha');
      const tf = document.querySelector('input[name=requested_time_from]');
      const tt = document.querySelector('input[name=requested_time_to]');
      return !!(fecha && fecha.value && tf && tf.value && tt && tt.value);
    }
    case 5:{
      // Comentarios = opcional, nunca bloquea. No marcamos como completo (sigue gris).
      return false;
    }
  }
  return false;
}

function _refreshStepStates(){
  document.querySelectorAll('.step-section').forEach(sec => {
    const n = parseInt(sec.dataset.step, 10);
    if (!n || n === 5) return;
    const wasComplete = sec.classList.contains('is-complete');
    const isComplete = _isStepComplete(n);
    if (isComplete && !wasComplete){
      sec.classList.add('is-complete');
    } else if (!isComplete && wasComplete){
      sec.classList.remove('is-complete');
    }
  });
}

// Re-evaluamos estados cada vez que algo relevante cambia.
function _wireStepWatchers(){
  const watchedIds = [
    'document_type', 'document_number',
    'customer_name', 'customer_rut',
    'contact_email', 'contact_phone',
    'auth_name', 'pickup_person_rut', 'auth_phone', 'acepta_tercero',
    'cal_fecha',
  ];
  watchedIds.forEach(id => {
    const el = document.getElementById(id);
    if (!el) return;
    el.addEventListener('blur', _refreshStepStates);
    el.addEventListener('change', _refreshStepStates);
    el.addEventListener('input', () => { setTimeout(_refreshStepStates, 50); });
  });
  // El bloque horario se actualiza via inputs hidden (requested_time_from/to)
  // — los toca updateSlotSummary, así que invocamos al final también.
  const slotPick = document.getElementById('slotPicker');
  if (slotPick){
    slotPick.addEventListener('click', () => { setTimeout(_refreshStepStates, 60); });
  }
  // Toggle autorizar
  const at = document.getElementById('authToggle');
  if (at) at.addEventListener('click', () => { setTimeout(_refreshStepStates, 80); });
}

// ════════════════════════════════════════════════════════
//  SUBMIT — OVERLAY 3D ESPARTANO + CARD PREMIUM (v2 mobile-first)
//  Daniel mayo 2026: "el directorio diga oooohhhh"
//  - Loading dots elegantes en el botón
//  - Overlay con CARD glassmorphism centrado (mobile-first)
//  - 3 steps verticales con bullet rojo pulse → check verde
//  - Ripple radial + sparkles ambient + confetti final
//  - El submit real continúa en background (form HTTP normal)
// ════════════════════════════════════════════════════════
function spartanShowStep(n, text){
  const steps = document.querySelectorAll('.sp-step');
  steps.forEach((el, i) => {
    const isActive = (i === (n-1));
    const isDone   = (i <  (n-1));
    el.classList.toggle('active',  isActive);
    el.classList.toggle('done',    isDone);
    el.classList.toggle('pending', !isActive && !isDone);
  });
  const fill = document.getElementById('spFill');
  if (fill) fill.style.width = (n/3*100) + '%';
  const t = document.getElementById('spartanText');
  if (t && text){
    t.textContent = text;
  }
}

// ═════════════════════════════════════════════════════════════════════
//  SUBMIT FIRE-AND-FORGET (Daniel 2026-05-24)
//  Antes: form.submit() nativo → browser esperaba SMTP (~15 s) → overlay
//  artificialmente largo. Ahora: fetch() AJAX → backend responde en
//  ~150 ms con el código RET-XXX. SMTP/notificaciones corren en thread
//  daemon del server. El overlay muestra el código GRANDE como
//  protagonista y redirige al tracking.
// ═════════════════════════════════════════════════════════════════════
document.getElementById('form-solicitud').addEventListener('submit', async function(e){
  if (e.defaultPrevented) return; // Validación falló — no entrar en loading
  e.preventDefault();             // Tomamos control: enviamos por fetch()

  const form = e.currentTarget;
  const btn = document.getElementById('submitBtn');
  const overlay = document.getElementById('spartanOverlay');

  if (btn){
    btn.classList.add('is-loading', 'pulse');
    const txt = btn.querySelector('.submit-btn-text');
    if (txt) txt.textContent = 'Procesando';
    btn.disabled = true;
    setTimeout(() => btn.classList.remove('pulse'), 700);
  }

  // Mostrar overlay (visual feedback inmediato)
  if (overlay){
    overlay.hidden = false;
    void overlay.offsetWidth;
    requestAnimationFrame(() => overlay.classList.add('active'));
    document.body.style.overflow = 'hidden';
    document.body.classList.add('sending'); // oculta el CTA fijo mobile bajo el overlay

    // Animaciones RÁPIDAS (~600 ms total) — solo para que el ojo perciba
    // los 3 pasos. El cliente ya tiene su código en <1 s.
    spartanShowStep(1, 'Validando tus datos…');
    setTimeout(() => spartanShowStep(2, 'Reservando tu slot…'), 200);
    setTimeout(() => spartanShowStep(3, 'Generando código…'), 400);
  }

  // Helper compartido: cerrar overlay + reactivar botón (sin mensaje)
  function _resetSubmitUI(){
    if (overlay){
      overlay.classList.remove('active');
      setTimeout(() => { overlay.hidden = true; }, 280);
      document.body.style.overflow = '';
    }
    document.body.classList.remove('sending'); // re-muestra el CTA fijo mobile
    if (btn){
      btn.classList.remove('is-loading');
      btn.disabled = false;
      const txt = btn.querySelector('.submit-btn-text');
      if (txt) txt.textContent = 'Enviar solicitud';
    }
  }

  // Helper para mostrar error + cerrar overlay + reactivar botón
  function _onError(msg){
    _resetSubmitUI();
    if (typeof ilusAlert === 'function'){
      ilusAlert({
        title: 'No pudimos enviar tu solicitud',
        message: msg || 'Revisa tu conexión e inténtalo nuevamente.',
        type: 'error',
      });
    } else if (typeof ilusToast === 'function'){
      ilusToast(msg || 'Error al enviar', { type: 'error', duration: 5000 });
    } else {
      console.error('[ILUS submit]', msg || 'Error al enviar');
    }
  }

  // 2026-06-09: el slot se ocupó entre que el cliente lo eligió y envió.
  // Recargamos disponibilidad fresca, repintamos calendario + slots del día
  // y avisamos con toast — antes solo salía un alert genérico y el cliente
  // reintentaba contra el mismo bloque lleno.
  async function _onSlotTaken(){
    _resetSubmitUI();
    clearSlots();
    try {
      const r = await fetch('/retiros/api/disponibilidad-publica');
      if (r.ok){
        const fresh = await r.json();
        if (fresh && fresh.from && fresh.dias) _disponibilidad = fresh;
      }
    } catch (_e){ /* sin red: conservamos los datos previos */ }
    if (_disponibilidad){
      const wrap = document.getElementById('ilusCalWrap');
      if (wrap && wrap.style.display !== 'none' && typeof _renderIlusCal === 'function'){
        _renderIlusCal();
      }
      onFechaChange();  // repinta los slots del día seleccionado con datos frescos
    }
    _showToast('Ese cupo se acaba de ocupar. Actualizamos la disponibilidad — elige otro bloque.', 'warning');
    const sp = document.getElementById('slotPicker');
    if (sp) sp.scrollIntoView({ behavior: 'smooth', block: 'center' });
  }

  // Helper: pintar el código RET-XXX GRANDE como protagonista
  // 2026-06-09: + botón "Copiar código" (clipboard con fallback), botón
  // "Ir al seguimiento ahora" y redirect a 6 s (antes 1.5 s — ilegible).
  function _showSuccess(code, trackingUrl){
    spartanShowStep(3, '¡Solicitud enviada!');
    const safeCode = String(code || '').replace(/[^A-Za-z0-9\-]/g, '') || 'RET-XXXXXX';
    window._ilusRetCode = safeCode;
    window._ilusTrackingUrl = trackingUrl || '/retiros';
    if (overlay){
      overlay.classList.add('done');
      const fill = document.getElementById('spFill');
      if (fill) fill.style.width = '100%';

      // Reemplazar el sub por el código GRANDE + acciones
      const sub = document.getElementById('spartanSub');
      if (sub){
        sub.innerHTML =
          '<div style="text-align:center;width:100%">' +
          '  <div style="font-size:.78rem;color:rgba(255,255,255,.65);font-weight:600;letter-spacing:.06em;text-transform:uppercase;margin-bottom:6px">Tu código de retiro</div>' +
          '  <div style="font-family:\'Bebas Neue\',\'Inter\',sans-serif;font-size:2.6rem;font-weight:900;color:#fff;letter-spacing:.05em;line-height:1;text-shadow:0 2px 14px rgba(0,0,0,.5)">' + safeCode + '</div>' +
          '  <div style="display:flex;gap:10px;justify-content:center;flex-wrap:wrap;margin-top:16px">' +
          '    <button type="button" onclick="copiarCodigoRet(this)" style="min-height:44px;padding:10px 18px;border-radius:10px;border:1.5px solid rgba(255,255,255,.4);background:rgba(255,255,255,.08);color:#fff;font-weight:800;font-size:.86rem;cursor:pointer"><i class="bi bi-clipboard me-1"></i>Copiar código</button>' +
          '    <button type="button" onclick="irAlSeguimientoAhora()" style="min-height:44px;padding:10px 18px;border-radius:10px;border:none;background:linear-gradient(180deg,#dc2626 0%,#b91c1c 100%);color:#fff;font-weight:800;font-size:.86rem;cursor:pointer;box-shadow:0 8px 18px -6px rgba(220,38,38,.55)"><i class="bi bi-arrow-right-circle me-1"></i>Ir al seguimiento ahora</button>' +
          '  </div>' +
          '  <div style="font-size:.82rem;color:rgba(255,255,255,.75);margin-top:14px;line-height:1.45">Recibirás un email de confirmación en los próximos minutos.</div>' +
          '  <div style="font-size:.74rem;color:rgba(255,255,255,.5);margin-top:6px">Te llevamos al seguimiento en unos segundos…</div>' +
          '</div>';
      }
      // Reemplazar el texto principal por un check
      const t = document.getElementById('spartanText');
      if (t) t.innerHTML = '<i class="bi bi-check-circle-fill" style="color:#16a34a;margin-right:6px"></i>Recibido';
    }

    // 6 s: tiempo real para leer y copiar el código antes del redirect
    setTimeout(() => {
      window.location.href = window._ilusTrackingUrl;
    }, 6000);
  }

  // Enviar por fetch — el backend detecta X-Requested-With y responde JSON
  try {
    const fd = new FormData(form);
    const t0 = Date.now();
    const resp = await fetch(form.action || window.location.pathname, {
      method: 'POST',
      body: fd,
      headers: {
        'X-Requested-With': 'XMLHttpRequest',
        'Accept': 'application/json',
      },
      credentials: 'same-origin',
    });
    const elapsed = Date.now() - t0;
    console.log('[ILUS submit] backend respondió en ' + elapsed + 'ms');

    // Parsear JSON defensivamente (si por algún motivo viene HTML)
    let data = null;
    try {
      data = await resp.json();
    } catch(_je){
      _onError('Respuesta inválida del servidor. Inténtalo nuevamente.');
      return;
    }

    if (!resp.ok || !data || !data.ok){
      const errs = (data && Array.isArray(data.errors)) ? data.errors.join(' · ')
                 : (data && data.error) ? data.error
                 : 'No pudimos procesar tu solicitud.';
      // 2026-06-09: si el error es de cupo (slot tomado entre la elección y
      // el envío), recargamos disponibilidad y guiamos a elegir otro bloque.
      if (/slot|cupo|lleno/i.test(errs)){
        _onSlotTaken();
      } else {
        _onError(errs);
      }
      return;
    }

    // ÉXITO — mostrar código RET-XXX
    _showSuccess(data.code, data.tracking_url);
  } catch (err){
    console.error('[ILUS submit] fetch error', err);
    _onError('Sin conexión o el servidor no responde. Verifica tu internet.');
  }
});

// ════════════════════════════════════════════════════════
//  COPIAR CÓDIGO RET (2026-06-09) — clipboard API + fallback
//  execCommand para navegadores viejos / contextos sin HTTPS.
// ════════════════════════════════════════════════════════
function copiarCodigoRet(btnEl){
  const code = window._ilusRetCode || '';
  if (!code) return;
  const done = () => {
    if (btnEl) btnEl.innerHTML = '<i class="bi bi-check-lg me-1"></i>Copiado';
    _showToast('Código ' + code + ' copiado al portapapeles', 'success');
  };
  const fallback = () => {
    try {
      const ta = document.createElement('textarea');
      ta.value = code;
      ta.setAttribute('readonly', '');
      ta.style.position = 'fixed';
      ta.style.opacity = '0';
      document.body.appendChild(ta);
      ta.select();
      document.execCommand('copy');
      document.body.removeChild(ta);
      done();
    } catch (_e){
      _showToast('No se pudo copiar automáticamente. Tu código: ' + code, 'info');
    }
  };
  if (navigator.clipboard && navigator.clipboard.writeText){
    navigator.clipboard.writeText(code).then(done).catch(fallback);
  } else {
    fallback();
  }
}
function irAlSeguimientoAhora(){
  window.location.href = window._ilusTrackingUrl || '/retiros';
}

document.addEventListener('DOMContentLoaded', () => {
  // Listener de fecha UNA sola vez aquí (antes se agregaba dentro de
  // cargarDisponibilidad — cada reintento apilaba un listener más).
  const _fi = document.getElementById('cal_fecha');
  if (_fi) _fi.addEventListener('change', onFechaChange);
  cargarDisponibilidad();
  // Si el formulario se re-renderizó con datos previos (fd), validar los campos
  // ya cargados para que el usuario vea el estado verde/rojo de inmediato.
  ['customer_rut', 'pickup_person_rut'].forEach(id => {
    const el = document.getElementById(id);
    if (el && el.value) validateRUT(el);
  });
  const _em = document.getElementById('contact_email');
  if (_em && _em.value) validateEmail(_em);
  ['contact_phone', 'auth_phone'].forEach(id => {
    const el = document.getElementById(id);
    if (el && el.value) validatePhone(el);
  });
  // Si fd trae datos del autorizado, abrimos el toggle automáticamente
  const _authName = document.getElementById('auth_name');
  if (_authName && _authName.value && _authName.value.trim().length > 0){
    const cb = document.getElementById('authCheckbox');
    if (cb && !cb.checked){
      cb.checked = true;
      document.getElementById('authFields').style.display = 'block';
      document.getElementById('authToggle').classList.add('active');
      document.getElementById('auth_active').value = '1';
    }
  }
  checkTerceroPanel();
  _initStepReveal();
  _wireStepWatchers();
  _refreshStepStates();
});

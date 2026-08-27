// ════════════════════════════════════════════════════════════════
//  RETIROS — Seguimiento público (public_tracking.html)
//  Extraído de los <script> inline el 2026-07-30 (mismo patrón ya
//  aplicado en otros templates internos). Página PÚBLICA — cero
//  cambio de comportamiento. Las variables Jinja del backend se
//  inyectan vía window.TRK_DATA (bootstrap inline chico, ver
//  public_tracking.html) para poder servir este archivo estático
//  y cacheable.
// ════════════════════════════════════════════════════════════════

// ── Stepper de confirmación de fecha (modal #confirmStepperModal) ──
            // ═══════════════════════════════════════════════════════════
            //  STEPPER DE CONFIRMACIÓN — paso a paso con AJAX
            //  Daniel mayo 2026
            //  - Paso 1: GET disponibilidad-publica para re-verificar
            //  - Paso 2: POST seguimiento con action=confirm (AJAX, JSON)
            //  - Paso 3: éxito → redirect al tracking actualizado
            //  - Cualquier error: muestra el detalle EN EL PASO que falló
            //    + botón "Reintentar" (vuelve a ejecutar todo).
            // ═══════════════════════════════════════════════════════════
            (function(){
              'use strict';
              const openBtn = document.getElementById('openConfirmStepper');
              const modalEl = document.getElementById('confirmStepperModal');
              if (!openBtn || !modalEl) return;

              const TRK = window.TRK_DATA || {};
              const TARGET_DATE = TRK.targetDate || '';
              const TARGET_TF   = TRK.targetTf || '';
              const TARGET_TT   = TRK.targetTt || '';
              const POST_URL    = TRK.trackUrl || '';
              const TRACK_URL   = TRK.trackUrl || '';
              const DISP_URL    = TRK.dispUrl || '';
              // Token del propio retiro: al re-verificar disponibilidad lo
              // enviamos como exclude_token para que la PROPIA reserva del
              // cliente no cuente como ocupación contra él mismo (bug
              // "No encontramos tu bloque" — Daniel 2026-05-25).
              const SELF_TOKEN  = TRK.selfToken || '';

              const steps = Array.from(modalEl.querySelectorAll('.cs-step'));
              const summaryOk = document.getElementById('csSummarySuccess');
              const summaryErr = document.getElementById('csSummaryError');
              const errorDetail = document.getElementById('csErrorDetail');
              const cancelBtn = document.getElementById('csCancelBtn');
              const retryBtn  = document.getElementById('csRetryBtn');
              const doneBtn   = document.getElementById('csDoneBtn');
              const closeBtn  = document.getElementById('csCloseBtn');

              let _modalInst = null;
              let _running = false;

              function _step(n){ return steps.find(s => parseInt(s.dataset.step,10) === n); }
              function _resetVisual(){
                steps.forEach(s => s.classList.remove('is-active','is-done','is-error'));
                summaryOk.style.display = 'none';
                summaryErr.style.display = 'none';
                retryBtn.style.display = 'none';
                doneBtn.style.display = 'none';
                cancelBtn.disabled = false;
                closeBtn.disabled  = false;
                _setSub(1, 'Comprobando que tu horario sigue libre…');
                _setSub(2, 'Avisando a la bodega y bloqueando tu bloque…');
                _setSub(3, 'Te enviamos un correo con todos los detalles.');
              }
              function _setSub(n, txt){
                const el = _step(n);
                if (!el) return;
                const sub = el.querySelector('.cs-step-sub');
                if (sub) sub.textContent = txt;
              }
              function _activate(n){
                steps.forEach(s => s.classList.remove('is-active'));
                const el = _step(n);
                if (el) el.classList.add('is-active');
              }
              function _done(n){
                const el = _step(n);
                if (!el) return;
                el.classList.remove('is-active','is-error');
                el.classList.add('is-done');
              }
              function _fail(n, msg){
                const el = _step(n);
                if (!el) return;
                el.classList.remove('is-active','is-done');
                el.classList.add('is-error');
                _setSub(n, msg || 'Ocurrió un problema en este paso.');
                summaryErr.style.display = 'flex';
                errorDetail.textContent = msg || 'Intenta nuevamente o elige otro horario.';
                retryBtn.style.display  = 'inline-flex';
                cancelBtn.disabled = false;
                closeBtn.disabled  = false;
              }

              async function _runFlow(){
                if (_running) return;
                _running = true;
                _resetVisual();
                cancelBtn.disabled = true;
                closeBtn.disabled  = true;

                // ── PASO 1: re-verificar disponibilidad ───────────
                _activate(1);
                try {
                  const _dispUrl = DISP_URL
                    + '?date=' + encodeURIComponent(TARGET_DATE)
                    + (SELF_TOKEN ? '&exclude_token=' + encodeURIComponent(SELF_TOKEN) : '');
                  const r = await fetch(_dispUrl, {
                    headers: {'X-Requested-With': 'XMLHttpRequest'},
                    cache: 'no-store',
                    credentials: 'same-origin',
                  });
                  if (!r.ok) throw new Error('HTTP ' + r.status);
                  const payload = await r.json();
                  const dia = (payload.dias || {})[TARGET_DATE];
                  if (!dia || !dia.disponible){
                    _running = false;
                    _fail(1, (dia && dia.razon) || 'El día ya no está disponible.');
                    return;
                  }
                  const slot = (dia.slots || []).find(s =>
                    (s.time_from || s.hora) === TARGET_TF
                  );
                  // El grid público solo muestra bloques estándar (09:00-12:30
                  // y 14:00-17:00). Si ILUS propuso un horario que cruza el
                  // buffer/colación (propuesta interna con bypass), ese bloque
                  // NO aparece en el grid — pero ES válido. En ese caso NO
                  // bloqueamos acá: dejamos que el POST confirm (paso 2), que
                  // es la fuente de verdad y respeta la propuesta interna,
                  // decida. Solo bloqueamos si el bloque SÍ está en el grid y
                  // quedó realmente lleno por OTRO retiro (su propia reserva ya
                  // viene excluida vía exclude_token).
                  if (slot && (slot.estado === 'completo' || slot.estado === 'colacion' || slot.estado === 'bloqueado')){
                    _running = false;
                    _fail(1, 'Tu horario ya no está libre: ' + (slot.razon || slot.estado));
                    return;
                  }
                  _setSub(1, 'Horario disponible. Reservando…');
                  _done(1);
                } catch (e){
                  _running = false;
                  _fail(1, 'No pudimos comprobar disponibilidad. Verifica tu conexión.');
                  return;
                }

                // ── PASO 2: POST AJAX confirm ─────────────────────
                _activate(2);
                try {
                  const fd = new FormData();
                  fd.append('action', 'confirm');
                  const r2 = await fetch(POST_URL, {
                    method: 'POST',
                    body: fd,
                    headers: {
                      'X-Requested-With': 'XMLHttpRequest',
                      'Accept': 'application/json',
                    },
                    credentials: 'same-origin',
                  });
                  let data = null;
                  try { data = await r2.json(); } catch(_){}
                  if (!r2.ok || !data || data.ok === false){
                    const msg = (data && data.error) || ('HTTP ' + r2.status);
                    _running = false;
                    _fail(2, msg);
                    return;
                  }
                  _setSub(2, 'Slot reservado para ' + (data.fecha || TARGET_DATE) + ' · ' + (data.hora_desde || TARGET_TF));
                  _done(2);
                } catch (e){
                  _running = false;
                  _fail(2, 'No pudimos reservar el slot. Intenta nuevamente.');
                  return;
                }

                // ── PASO 3: éxito + redirect ─────────────────────
                _activate(3);
                _setSub(3, 'Cita confirmada. Redirigiendo…');
                _done(3);
                summaryOk.style.display = 'flex';
                doneBtn.style.display   = 'inline-flex';
                doneBtn.disabled        = false;
                closeBtn.disabled       = false;
                cancelBtn.style.display = 'none';
                _running = false;

                // Auto-redirect tras 1.2s — el cliente alcanza a ver el ✓
                setTimeout(() => { window.location.href = TRACK_URL; }, 1200);
              }

              openBtn.addEventListener('click', () => {
                if (!_modalInst && window.bootstrap){
                  _modalInst = new bootstrap.Modal(modalEl);
                }
                if (_modalInst){
                  _modalInst.show();
                  _runFlow();
                }
              });

              retryBtn.addEventListener('click', () => { _runFlow(); });
              doneBtn.addEventListener('click',  () => { window.location.href = TRACK_URL; });

              // Reset al cerrar (por si vuelve a abrir)
              modalEl.addEventListener('hidden.bs.modal', () => {
                _resetVisual();
              });
            })();

  // ════════════════════════════════════════════════════════════════
  //  TRACKING LIVE — Daniel mayo 2026
  //  - Anima el progreso del stepper al cargar.
  //  - Polling cada 30s al endpoint /retiros/api/seguimiento/<token>
  //  - Si cambia el estado: refresca la página (sin parpadeo brusco).
  //  - Rating en post-retirada (UI only — no persiste todavía).
  // ════════════════════════════════════════════════════════════════
  (function(){
    'use strict';

    const TRK = window.TRK_DATA || {};
    const PUBLIC_TOKEN = TRK.publicToken || "";
    const POLL_URL    = TRK.pollUrl || "";
    const POLL_INTERVAL = 30000; // 30s
    const CURRENT_STATUS = TRK.currentStatus || "";

    // ── 1) "Recorrido" de entrada del stepper (2026-07-30, Daniel: "que
    // cuando entres haga el recorrido de manera espectacular y lo deje
    // justo donde está, con una transición") -- en vez de aparecer directo
    // en el hito actual, recorre cada hito en secuencia hasta llegar al
    // real, con la línea de avance creciendo en sincronía. Respeta
    // prefers-reduced-motion (salta directo al estado final).
    function animateStepperFill() {
      const stepper = document.getElementById('stepperRoot');
      const fill    = document.getElementById('stepperFill');
      if (!stepper || !fill) return;
      const idx = parseInt(stepper.dataset.idx, 10);
      // Daniel 2026-06-16: línea con left:10% y ancho idx*20% (5 nodos = centros
      // en 10%,30%,50%,70%,90%). El relleno termina EXACTO en el centro del nodo
      // activo, sin el hack *0.84 anterior. idx -1 (cancelada) = casi sin avance.
      let pct = 0;
      if (idx === -1)      pct = 4;
      else                 pct = Math.min(Math.max(idx, 0), 4) * 20;

      const reduceMotion = window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches;
      const stepEls = Array.from(stepper.querySelectorAll('.step'));
      if (reduceMotion || idx === -1 || !stepEls.length) {
        requestAnimationFrame(() => { fill.style.width = pct + '%'; });
        return;
      }
      const targets = stepEls.map((el) => el.classList.contains('done') ? 'done'
        : (el.classList.contains('active') ? 'active' : ''));
      stepEls.forEach((el) => el.classList.remove('done', 'active'));
      fill.style.width = '0%';
      const STEP_MS = 460;
      stepEls.forEach((el, i) => {
        setTimeout(() => {
          if (targets[i]) el.classList.add(targets[i]);
          // 2026-07-30 (Daniel: "el entregado tiene que ser como un giro,
          // algo bien dinámico") -- el ÚLTIMO hito gira al completarse.
          if (i === stepEls.length - 1 && targets[i] === 'done') {
            const circle = el.querySelector('.circle');
            if (circle) circle.classList.add('step-final-spin');
          }
          const segPct = Math.min((i + 1) * 20, pct);
          fill.style.width = segPct + '%';
          if (i === stepEls.length - 1) fill.style.width = pct + '%';
        }, 260 + i * STEP_MS);
      });
    }

    // ── 2) Polling silencioso del estado ────────────────────────
    let pollTimer = null;
    let pollFailures = 0;
    // Índice del hito con que se RENDERIZÓ la página (baseline del servidor).
    const _sr = document.getElementById('stepperRoot');
    const CURRENT_IDX = _sr ? parseInt(_sr.dataset.idx, 10) : NaN;
    // Firma del estado: cualquier cambio en estos campos = novedad visible para
    // el cliente. Incluye journey_idx (refresca aunque el `status` exacto pase a
    // otro estado del MISMO hito) y la fecha/propuesta (caso ping-pong: nueva
    // propuesta sin cambiar de estado). Daniel 2026-06-17 (canal EN VIVO).
    let lastSig = null;
    function _stateSig(d) {
      return [
        d.status, d.journey_idx,
        d.has_pending_proposal ? 1 : 0,
        d.confirmed_date || '', d.confirmed_time_from || '',
        // CONECTIVIDAD (2026-06-21): cada check de bodega en el picking WMS
        // cambia la firma → la barra de preparación avanza EN VIVO.
        (d.prep_hechos || 0) + '/' + (d.prep_total || 0)
      ].join('|');
    }

    function startPolling() {
      // Si el estado es terminal, no polleamos
      const terminales = ['retirada','cerrada','rechazada','fallida'];
      if (terminales.indexOf(CURRENT_STATUS) !== -1) {
        const lb = document.getElementById('liveBadge');
        if (lb) { lb.innerHTML = '<i class="bi bi-archive"></i> ARCHIVADO'; lb.style.background='rgba(156,163,175,.15)'; lb.style.borderColor='rgba(156,163,175,.3)'; lb.style.color='#9ca3af'; }
        return;
      }
      pollTimer = setInterval(checkStatus, POLL_INTERVAL);
    }

    async function checkStatus() {
      if (document.hidden) return; // No malgastar si la pestaña está oculta
      try {
        const resp = await fetch(POLL_URL, { credentials: 'same-origin', cache: 'no-store' });
        if (!resp.ok) { pollFailures++; return; }
        const data = await resp.json();
        pollFailures = 0;
        if (!data || !data.ok || !data.status) return;
        const sig = _stateSig(data);
        if (lastSig === null) {
          // Primer poll: fijamos baseline. Si YA difiere del estado con que se
          // renderizó la página (cambió entre el render y este poll), refrescamos.
          lastSig = sig;
          const idxChanged = Number.isFinite(CURRENT_IDX) && data.journey_idx !== CURRENT_IDX;
          if (data.status !== CURRENT_STATUS || idxChanged) {
            showStatusChangeAlert(data.status_label || data.status);
            setTimeout(() => { window.location.reload(); }, 1400);
          }
          return;
        }
        if (sig !== lastSig) {
          // Cualquier novedad visible (estado, hito, propuesta o fecha) → refrescar
          showStatusChangeAlert(data.status_label || data.status);
          setTimeout(() => { window.location.reload(); }, 1400);
        }
      } catch (e) {
        pollFailures++;
        // Tras 5 fallos seguidos, callar el polling para no martillar
        if (pollFailures >= 5 && pollTimer) {
          clearInterval(pollTimer); pollTimer = null;
          const lb = document.getElementById('liveBadge');
          if (lb) lb.innerHTML = '<i class="bi bi-wifi-off"></i> SIN CONEXIÓN';
        }
      }
    }

    // ── 3) Banner "estado cambió" antes de recargar ─────────────
    function showStatusChangeAlert(newLabel) {
      const overlay = document.createElement('div');
      overlay.innerHTML = `
        <div style="position:fixed;inset:0;background:rgba(0,0,0,.55);display:grid;place-items:center;z-index:9999;backdrop-filter:blur(6px);animation:fadeIn .25s ease-out">
          <div style="background:#fff;border-radius:16px;padding:28px 32px;text-align:center;max-width:340px;box-shadow:0 22px 48px rgba(0,0,0,.3)">
            <div style="font-size:2.6rem;color:#dc2626;margin-bottom:8px"><i class="bi bi-arrow-clockwise"></i></div>
            <div style="font-weight:900;font-size:1.05rem;color:#0a0a0a;margin-bottom:6px">Hay novedades</div>
            <div style="color:#6b7280;font-size:.92rem">Tu retiro pasó a: <strong style="color:#dc2626">${newLabel}</strong></div>
            <div style="margin-top:10px;color:#9ca3af;font-size:.78rem">Actualizando…</div>
          </div>
        </div>`;
      document.body.appendChild(overlay);
    }

    // ── 4) Rating UI (post-retirada) ────────────────────────────
    function initRating() {
      const wrap = document.getElementById('ratingStars');
      if (!wrap) return;
      const fb = document.getElementById('ratingFeedback');
      let chosen = 0;
      const stars = wrap.querySelectorAll('.star');
      const labels = ['', 'Mala', 'Regular', 'Buena', 'Muy buena', '¡Excelente!'];
      stars.forEach((s, idx) => {
        s.addEventListener('mouseenter', () => {
          stars.forEach((s2, i) => s2.classList.toggle('on', i <= idx));
          if (fb) fb.textContent = labels[idx+1];
        });
        s.addEventListener('mouseleave', () => {
          stars.forEach((s2, i) => s2.classList.toggle('on', i < chosen));
          if (fb) fb.textContent = chosen ? `Gracias por tu ${chosen} ★` : 'Toca las estrellas para puntuar';
        });
        s.addEventListener('click', () => {
          chosen = idx + 1;
          stars.forEach((s2, i) => s2.classList.toggle('on', i < chosen));
          if (fb) fb.innerHTML = `Gracias por tu ${chosen} ★ <small style="display:block;margin-top:4px;color:#9ca3af">Tu valoración nos ayuda a mejorar</small>`;
          // Persistencia futura: POST a /retiros/seguimiento/<token>/rating
          // Por ahora se queda en UI (sin BD) — Daniel decide si quiere persistir.
        });
      });
    }

    // ── 5) Animación reveal-on-scroll (marketing cards) ──────────
    function initRevealOnScroll() {
      const items = document.querySelectorAll('.reveal-on-scroll');
      if (!items.length) return;
      // Sin IntersectionObserver: muestra todo de una (degradación elegante)
      if (!('IntersectionObserver' in window)) {
        items.forEach(el => el.classList.add('is-visible'));
        return;
      }
      const io = new IntersectionObserver((entries) => {
        entries.forEach((e) => {
          if (e.isIntersecting) {
            e.target.classList.add('is-visible');
            io.unobserve(e.target);
          }
        });
      }, { threshold: 0.12, rootMargin: '0px 0px -40px 0px' });
      items.forEach(el => io.observe(el));
    }

    // ── 5b) Auto-dismiss de flash toasts (Daniel 2026-05-23) ─────
    function initFlashToasts() {
      const toasts = document.querySelectorAll('[data-flash-toast]');
      toasts.forEach((t, i) => {
        // Stagger ligero entre toasts
        t.style.animationDelay = (i * 0.08) + 's';
        // Auto-dismiss a los 6.5s (excepto warning/error que duran más)
        const cat = t.className.match(/flash-toast\s+(\w+)/);
        const ttl = (cat && (cat[1] === 'warning' || cat[1] === 'error' || cat[1] === 'danger')) ? 9000 : 6500;
        setTimeout(() => {
          if (t && t.isConnected) {
            t.classList.add('is-leaving');
            setTimeout(() => { try { t.remove(); } catch(_){} }, 350);
          }
        }, ttl);
      });
    }

    // ── 6) BOOT ──────────────────────────────────────────────────
    // Botón "Email soporte" robusto (Daniel 2026-06-17: "nunca me abrió").
    // Mantenemos el mailto (si hay cliente de correo, se abre) PERO copiamos la
    // dirección al portapapeles y avisamos, por si el equipo no tiene cliente de
    // correo asociado (típico en kioscos / demos). REGLA #1: nada de alert nativo.
    function initMailFallback(){
      const btn = document.getElementById('ctaMailSoporte');
      if (!btn) return;
      btn.addEventListener('click', () => {
        const mail = btn.dataset.mail || 'soportetec@sphs.cl';
        try {
          if (navigator.clipboard && navigator.clipboard.writeText) {
            navigator.clipboard.writeText(mail)
              .then(() => _toast('Correo copiado: ' + mail, 'success'))
              .catch(() => _toast('Escríbenos a ' + mail, 'info'));
          } else {
            _toast('Escríbenos a ' + mail, 'info');
          }
        } catch(_){ _toast('Escríbenos a ' + mail, 'info'); }
      });
    }

    document.addEventListener('DOMContentLoaded', () => {
      animateStepperFill();
      initRating();
      initRevealOnScroll();
      initFlashToasts();
      initMailFallback();
      // Iniciar polling después de 8s (no martillar al cargar)
      setTimeout(startPolling, 8000);
    });

    // Si la pestaña vuelve a foco, pedir status inmediato (UX más fluido)
    document.addEventListener('visibilitychange', () => {
      if (!document.hidden && pollTimer) checkStatus();
    });

    // Inyectar @keyframes para el overlay del banner (no está en CSS para no contaminar)
    const kf = document.createElement('style');
    kf.textContent = '@keyframes fadeIn{from{opacity:0}to{opacity:1}}';
    document.head.appendChild(kf);
  })();

  // ════════════════════════════════════════════════════════════════
  //  MODAL "Proponer otra fecha" — CALENDARIO INTELIGENTE
  //  Daniel 2026-05-23 (Bug #2): replica el calendario del formulario
  //  público dentro del modal. Misma seguridad: no permite pasado,
  //  ni slot ocupado, ni colación. Fuente única de verdad:
  //  endpoint /retiros/api/disponibilidad-publica.
  // ════════════════════════════════════════════════════════════════
  (function(){
    'use strict';

    const API_URL = '/retiros/api/disponibilidad-publica';
    let _cntDisponibilidad = null;   // payload completo del endpoint
    let _cntSlotsDelDia    = [];     // slots del día seleccionado
    let _cntStartIdx       = null;
    let _cntEndIdx         = null;
    let _cntLoaded         = false;  // lazy load: solo cargamos al abrir modal

    function _hmToMin(hm){
      const p = String(hm || '').split(':').map(Number);
      return (p[0] || 0) * 60 + (p[1] || 0);
    }
    function _minToHM(min){
      return `${String(Math.floor(min/60)).padStart(2,'0')}:${String(min%60).padStart(2,'0')}`;
    }

    function _toast(msg, type){
      // Usar helper ILUS si existe, fallback a alert nativo (que en ILUS
      // también está shimmeado vía ilus_ui.js, así que igual sale bonito)
      if (typeof window.ilusToast === 'function') {
        try { return window.ilusToast(msg, { type: type || 'info', duration: 4500 }); } catch(_){}
      }
      try { window.alert(msg); } catch(_){}
    }

    async function _loadDisponibilidad(){
      if (_cntLoaded && _cntDisponibilidad) return _cntDisponibilidad;
      try {
        const r = await fetch(API_URL, { credentials:'same-origin', cache:'no-store' });
        if (!r.ok) throw new Error('HTTP ' + r.status);
        _cntDisponibilidad = await r.json();
        _cntLoaded = true;
        return _cntDisponibilidad;
      } catch (e){
        const msgEl = document.getElementById('cnt_fecha_msg');
        if (msgEl){
          msgEl.innerHTML = '<i class="bi bi-exclamation-triangle me-1 text-danger"></i>No se pudo cargar disponibilidad. Reintenta en unos segundos.';
          msgEl.style.color = '#dc2626';
        }
        return null;
      }
    }

    function _setMinMax(){
      const inp = document.getElementById('cnt_fecha_picker');
      if (!inp || !_cntDisponibilidad) return;
      const tomorrow = new Date(Date.now() + 24*3600*1000);
      const tomorrowStr = tomorrow.toISOString().slice(0,10);
      inp.min = (_cntDisponibilidad.from < tomorrowStr) ? tomorrowStr : _cntDisponibilidad.from;
      inp.max = _cntDisponibilidad.to;
    }

    function _onFechaChange(){
      const inp  = document.getElementById('cnt_fecha_picker');
      const msg  = document.getElementById('cnt_fecha_msg');
      const grid = document.getElementById('cnt_slot_grid');
      const quick= document.getElementById('cnt_slot_quick_wrap');
      const fecha = inp.value;
      _cntClearSelection();

      if (!fecha || !_cntDisponibilidad){
        grid.innerHTML = '<div class="text-center text-muted small py-3"><i class="bi bi-calendar3 me-1"></i>Selecciona una fecha primero</div>';
        quick.style.display = 'none';
        return;
      }
      // Defense in depth: nunca aceptar fecha de hoy/pasado
      const minDate = new Date(Date.now() + 24*3600*1000); minDate.setHours(0,0,0,0);
      if (new Date(fecha + 'T00:00:00') < minDate){
        msg.innerHTML = '<i class="bi bi-exclamation-triangle me-1"></i>Necesitamos mínimo 24 horas de anticipación';
        msg.style.color = '#dc2626';
        grid.innerHTML = '<div class="text-center py-3" style="color:#dc2626"><i class="bi bi-exclamation-triangle me-1"></i>Elige una fecha desde mañana</div>';
        quick.style.display = 'none';
        return;
      }
      const dia = (_cntDisponibilidad.dias || {})[fecha];
      if (!dia || !dia.disponible){
        const razon = (dia && dia.razon) || 'Día sin cupos';
        msg.innerHTML = '<i class="bi bi-x-circle me-1"></i>' + razon;
        msg.style.color = '#dc2626';
        grid.innerHTML = `<div class="text-center py-3" style="color:#dc2626"><i class="bi bi-x-circle me-1"></i>${razon}</div>`;
        quick.style.display = 'none';
        return;
      }
      _cntSlotsDelDia = dia.slots || [];
      const libres = _cntSlotsDelDia.filter(s => s.puede_iniciar).length;
      msg.innerHTML = `<i class="bi bi-check-circle me-1" style="color:${libres>0?'#16a34a':'#dc2626'}"></i>${libres} bloque${libres===1?'':'s'} libre${libres===1?'':'s'} este día`;
      msg.style.color = libres > 0 ? '#16a34a' : '#dc2626';
      _renderGrid();
      quick.style.display = libres > 0 ? 'flex' : 'none';
    }

    function _renderGrid(){
      const grid = document.getElementById('cnt_slot_grid');
      if (!_cntSlotsDelDia.length){
        grid.innerHTML = '<div class="text-center text-muted small py-3">Sin bloques disponibles</div>';
        return;
      }
      const lunchStart = (_cntDisponibilidad && _cntDisponibilidad.lunch_start) ? _cntDisponibilidad.lunch_start : '12:30';
      const lunchStartMin = _hmToMin(lunchStart);

      const slotHtml = (s, i) => {
        const cls = ['cnt-slot-item'];
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
        // Daniel 2026-05-24: vista cliente unificada — cualquier slot no
        // plenamente libre se ve como candado, sin info extra (ni cupos
        // numéricos ni razón). El cliente solo elige entre libre / no libre.
        if (estado === 'colacion'){
          cls.push('is-lunch', 'is-disabled');
          title = 'Hora no disponible';
        } else if (estado === 'completo' || estado === 'ocupado'){
          cls.push('is-full', 'is-disabled');
          title = 'Hora no disponible';
        } else if (estado === 'bloqueado'){
          cls.push('is-blocked', 'is-disabled');
          title = 'Hora no disponible';
        } else {
          title = 'Disponible';
        }
        if (_cntStartIdx !== null && _cntEndIdx !== null){
          if (i === _cntStartIdx && i === _cntEndIdx){
            cls.push('is-selected', 'is-start', 'is-end');
          } else if (i === _cntStartIdx){
            cls.push('is-selected', 'is-start');
          } else if (i === _cntEndIdx){
            cls.push('is-selected', 'is-end');
          } else if (i > _cntStartIdx && i < _cntEndIdx){
            cls.push('is-in-range');
          }
        }
        const hora = s.time_from || s.hora || '';
        return `<div class="${cls.join(' ')}" data-cnt-idx="${i}" title="${title}">${badge}${hora}</div>`;
      };

      let postLunchIdx = -1;
      for (let i = 0; i < _cntSlotsDelDia.length; i++){
        const s = _cntSlotsDelDia[i];
        const startMin = _hmToMin(s.time_from || s.hora || '00:00');
        const isLunch = (s.estado === 'colacion') || s.lunch;
        if (!isLunch && startMin >= lunchStartMin){ postLunchIdx = i; break; }
      }
      const hasMorning   = _cntSlotsDelDia.some((s, i) => postLunchIdx === -1 ? true : i < postLunchIdx);
      const hasAfternoon = postLunchIdx !== -1;

      let html = '';
      if (hasMorning && hasAfternoon){
        html += '<div class="cnt-slot-section-title">☀️ Mañana</div>';
        html += '<div class="cnt-slot-grid-inner">' + _cntSlotsDelDia.slice(0, postLunchIdx).map((s, i) => slotHtml(s, i)).join('') + '</div>';
        html += '<div class="cnt-slot-section-title">🌤 Tarde</div>';
        html += '<div class="cnt-slot-grid-inner">' + _cntSlotsDelDia.slice(postLunchIdx).map((s, j) => slotHtml(s, postLunchIdx + j)).join('') + '</div>';
      } else {
        html = '<div class="cnt-slot-grid-inner">' + _cntSlotsDelDia.map((s, i) => slotHtml(s, i)).join('') + '</div>';
      }
      grid.innerHTML = html;

      // Event delegation
      grid.querySelectorAll('[data-cnt-idx]').forEach(el => {
        el.addEventListener('click', () => _onSlotClick(parseInt(el.dataset.cntIdx, 10)));
      });
    }

    function _estadoDeSlot(s){
      let e = s.estado;
      if (!e){
        if (s.lunch) e = 'colacion';
        else if (s.razon && !s.disponible) e = 'bloqueado';
        else if (!s.disponible) e = 'completo';
        else if ((s.ocupados || 0) > 0 && (s.ocupados || 0) < (s.max || 2)) e = 'ocupado';
        else e = 'disponible';
      }
      return e;
    }

    function _onSlotClick(i){
      const s = _cntSlotsDelDia[i];
      if (!s) return;
      const estado = _estadoDeSlot(s);
      // Daniel 2026-05-24: mensaje genérico, sin detalle interno.
      if (estado === 'colacion' || estado === 'completo' ||
          estado === 'ocupado' || estado === 'bloqueado'){
        _toast('Esa hora no está disponible. Elige otro bloque.', 'warning');
        return;
      }

      if (_cntStartIdx === null){
        _cntStartIdx = _cntEndIdx = i;
      } else if (_cntStartIdx === i && _cntEndIdx === i){
        _cntStartIdx = _cntEndIdx = null;
      } else {
        const from = Math.min(_cntStartIdx, i);
        const to   = Math.max(_cntStartIdx, i);
        let invalido = false;
        for (let k = from; k <= to; k++){
          const ek = _estadoDeSlot(_cntSlotsDelDia[k]);
          if (ek === 'colacion' || ek === 'completo' ||
              ek === 'ocupado' || ek === 'bloqueado'){
            invalido = true; break;
          }
        }
        if (invalido){
          _toast('El rango cruza una hora no disponible. Acórtalo o elige otra hora.', 'warning');
          _cntStartIdx = _cntEndIdx = i;
        } else {
          _cntStartIdx = from; _cntEndIdx = to;
        }
      }
      _updateSummary();
      _renderGrid();
    }

    function _setQuickRange(hours){
      if (!hours || hours <= 0) { _cntClearSelection(); return; }
      const slotMin = (_cntDisponibilidad && _cntDisponibilidad.slot_minutes) ? _cntDisponibilidad.slot_minutes : 30;
      const slotsNeeded = Math.max(1, Math.round(hours * 60 / slotMin));
      let foundStart = -1, foundEnd = -1;
      for (let i = 0; i < _cntSlotsDelDia.length; i++){
        const s0 = _cntSlotsDelDia[i];
        // Solo aceptar slots PLENAMENTE libres (sin ocupación).
        const ocup0 = (s0.ocupados || 0);
        const puedeIni = s0.disponible && !s0.lunch && ocup0 === 0;
        if (!puedeIni) continue;
        const endIdx = i + slotsNeeded - 1;
        if (endIdx >= _cntSlotsDelDia.length) continue;
        let ok = true;
        for (let k = i; k <= endIdx; k++){
          const ek = _estadoDeSlot(_cntSlotsDelDia[k]);
          if (ek === 'colacion' || ek === 'completo' ||
              ek === 'ocupado' || ek === 'bloqueado'){ ok = false; break; }
        }
        if (ok){ foundStart = i; foundEnd = endIdx; break; }
      }
      if (foundStart < 0){
        _toast(`No hay ${hours} hora${hours===1?'':'s'} contiguas libres este día. Prueba otra fecha o menos tiempo.`, 'warning');
        return;
      }
      _cntStartIdx = foundStart; _cntEndIdx = foundEnd;
      _updateSummary(); _renderGrid();
    }

    function _cntClearSelection(){
      _cntStartIdx = _cntEndIdx = null;
      _updateSummary();
      _renderGrid();
    }

    function _updateSummary(){
      const txt = document.getElementById('cnt_slot_summary_text');
      const sumBox = document.getElementById('cnt_slot_summary');
      const subBtn = document.getElementById('cnt_submit_btn');
      const inpTf  = document.getElementById('cnt_tf_input');
      const inpTt  = document.getElementById('cnt_tt_input');
      const inpD   = document.getElementById('cnt_date_input');
      const fecha  = document.getElementById('cnt_fecha_picker').value;

      if (_cntStartIdx === null){
        txt.className = 'sum-empty';
        txt.textContent = 'No has seleccionado bloque aún';
        if (sumBox) sumBox.classList.remove('is-active');
        if (subBtn) subBtn.disabled = true;
        if (inpTf) inpTf.value = '';
        if (inpTt) inpTt.value = '';
        if (inpD)  inpD.value  = fecha || '';
        return;
      }
      const slotMin = (_cntDisponibilidad && _cntDisponibilidad.slot_minutes) ? _cntDisponibilidad.slot_minutes : 30;
      const sS = _cntSlotsDelDia[_cntStartIdx];
      const sE = _cntSlotsDelDia[_cntEndIdx];
      const startStr = sS.time_from || sS.hora;
      let endStr = sE.time_to;
      if (!endStr){
        const baseMin = _hmToMin(sE.time_from || sE.hora);
        endStr = _minToHM(baseMin + slotMin);
      }
      const nBlocks = _cntEndIdx - _cntStartIdx + 1;
      const totalMin = nBlocks * slotMin;
      let durTxt;
      if (totalMin < 60) durTxt = `${totalMin} min`;
      else if (totalMin % 60 === 0) durTxt = `${totalMin/60} h`;
      else durTxt = `${(totalMin/60).toFixed(1)} h`;

      txt.className = '';
      txt.innerHTML = `<i class="bi bi-clock-fill me-2" style="color:#16a34a"></i><span class="sum-range">${startStr} – ${endStr}</span><span class="sum-dur"><i class="bi bi-stopwatch me-1"></i>${durTxt}</span>`;
      if (sumBox) sumBox.classList.add('is-active');
      if (subBtn) subBtn.disabled = !fecha;
      if (inpTf) inpTf.value = startStr;
      if (inpTt) inpTt.value = endStr;
      if (inpD)  inpD.value  = fecha || '';
    }

    function _initModal(){
      const modalEl = document.getElementById('counterModal');
      if (!modalEl) return;
      const formEl  = document.getElementById('counterForm');
      const inpFecha= document.getElementById('cnt_fecha_picker');
      const clearBtn= document.getElementById('cnt_slot_clear_btn');
      const quicks  = document.querySelectorAll('[data-cnt-quick]');

      modalEl.addEventListener('shown.bs.modal', async () => {
        // Lazy load: pedimos disponibilidad solo cuando el cliente abre el modal
        await _loadDisponibilidad();
        _setMinMax();
        // Reset visual
        _cntClearSelection();
        if (inpFecha) inpFecha.value = '';
        const msg = document.getElementById('cnt_fecha_msg');
        if (msg){
          msg.innerHTML = '<i class="bi bi-calendar3 me-1"></i>Selecciona un día desde mañana.';
          msg.style.color = '';
        }
      });
      modalEl.addEventListener('hidden.bs.modal', () => {
        // Limpiamos selección y submit al cerrar para evitar resubmit accidental
        _cntClearSelection();
      });

      if (inpFecha) inpFecha.addEventListener('change', _onFechaChange);
      if (clearBtn) clearBtn.addEventListener('click', _cntClearSelection);
      quicks.forEach(b => b.addEventListener('click', () => _setQuickRange(parseInt(b.dataset.cntQuick, 10))));

      // Validación al enviar — defense in depth
      if (formEl) formEl.addEventListener('submit', (e) => {
        const fecha = (inpFecha && inpFecha.value) || '';
        const tf    = document.getElementById('cnt_tf_input').value;
        const tt    = document.getElementById('cnt_tt_input').value;
        if (!fecha){ e.preventDefault(); _toast('Elige una fecha primero.', 'warning'); return false; }
        if (!tf || !tt){ e.preventDefault(); _toast('Selecciona uno o más bloques horarios.', 'warning'); return false; }
        // Anti-pasado redundante
        const minDate = new Date(Date.now() + 24*3600*1000); minDate.setHours(0,0,0,0);
        if (new Date(fecha + 'T00:00:00') < minDate){
          e.preventDefault();
          _toast('La fecha debe ser desde mañana en adelante.', 'warning');
          return false;
        }
        const btn = document.getElementById('cnt_submit_btn');
        if (btn){ btn.disabled = true; btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Enviando…'; }
      });
    }

    // ── Reject modal: feedback de loading al enviar ──
    function _initRejectModal(){
      const f = document.getElementById('rejectForm');
      const b = document.getElementById('rejectSubmitBtn');
      if (!f || !b) return;
      f.addEventListener('submit', () => {
        b.disabled = true;
        b.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Cancelando…';
      });
    }

    document.addEventListener('DOMContentLoaded', () => {
      _initModal();
      _initRejectModal();
    });
  })();

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
  //  MODAL "Proponer otra fecha" — CALENDARIO COMPARTIDO
  //  Daniel 2026-05-23 (Bug #2): replica el calendario del formulario
  //  público dentro del modal. Misma seguridad: no permite pasado, ni
  //  slot ocupado, ni colación. Fuente única de verdad: endpoint
  //  /retiros/api/disponibilidad-publica.
  //  2026-09-14 (Daniel: "aplica el mismo calendario al modal del
  //  cliente"): el picker propio (~350 líneas: grilla, atajos 1h/2h/3h,
  //  rangos) se reemplaza por el widget compartido IlusRetirosCalendar
  //  (static/retiros_calendar.js) con mes + bloques en dos columnas,
  //  UN solo bloque de 30 min (misma regla que el formulario público).
  //  El widget llena los hidden counter_date / counter_time_from /
  //  counter_time_to; el submit y el backend no cambiaron. La propia
  //  reserva del cliente se excluye del cupo vía exclude_token.
  // ════════════════════════════════════════════════════════════════
  (function(){
    'use strict';

    const TRK = window.TRK_DATA || {};
    let _cal = null;

    function _toast(msg, type){
      if (typeof window.ilusToast === 'function') {
        try { return window.ilusToast(msg, { type: type || 'info', duration: 4500 }); } catch(_){}
      }
      try { window.alert(msg); } catch(_){}
    }

    function _mountCal(){
      if (_cal || typeof window.IlusRetirosCalendar === 'undefined') return;
      const subBtn = document.getElementById('cnt_submit_btn');
      _cal = window.IlusRetirosCalendar.mount({
        container:      '#cnt_slot_grid',
        dateInput:      '#cnt_fecha_picker',
        summaryEl:      '#cnt_slot_summary',
        quickActionsEl: '#cnt_slot_quick_wrap',
        hiddenDate:     '#cnt_date_input',
        hiddenTimeFrom: '#cnt_tf_input',
        hiddenTimeTo:   '#cnt_tt_input',
        monthContainer: '#cnt_month',
        monthHelp:      'Necesitamos mínimo 24 horas de anticipación para preparar tu pedido.',
        apiUrl:         TRK.dispUrl || '/retiros/api/disponibilidad-publica',
        excludeToken:   TRK.selfToken || null,
        includeOwners:  false,   // cliente: sin nombres ni cupos internos
        allowToday:     false,   // cliente: desde mañana (min_notice)
        allowCrossLunch: false,
        enableDragSelect: false,
        enableMultiBlock: false, // una sola hora de llegada
        suggestedDurationMin: 30,
        onLoadFail: function(){
          const fb = document.getElementById('cnt_fecha_fallback');
          if (fb) fb.style.display = '';
          const msgEl = document.getElementById('cnt_fecha_msg');
          if (msgEl){
            msgEl.innerHTML = '<i class="bi bi-exclamation-triangle me-1 text-danger"></i>No se pudo cargar disponibilidad. Reintenta en unos segundos.';
            msgEl.style.color = '#dc2626';
          }
        },
        onChange: function(sel){
          if (subBtn) subBtn.disabled = !sel;
        },
      });
    }

    function _initModal(){
      const modalEl = document.getElementById('counterModal');
      if (!modalEl) return;
      const formEl  = document.getElementById('counterForm');

      modalEl.addEventListener('shown.bs.modal', () => {
        // Lazy: montamos recién cuando el modal está visible (el grid mide
        // mal en display:none). Al reabrir, refrescamos cupos.
        if (!_cal) _mountCal();
        else if (_cal.reload) _cal.reload();
      });
      modalEl.addEventListener('hidden.bs.modal', () => {
        // Limpiamos selección y submit al cerrar para evitar resubmit accidental
        if (_cal && _cal.clearSelection) _cal.clearSelection();
        const subBtn = document.getElementById('cnt_submit_btn');
        if (subBtn) subBtn.disabled = true;
      });

      // Validación al enviar — defense in depth
      if (formEl) formEl.addEventListener('submit', (e) => {
        const fecha = (document.getElementById('cnt_date_input') || {}).value || '';
        const tf    = (document.getElementById('cnt_tf_input') || {}).value || '';
        const tt    = (document.getElementById('cnt_tt_input') || {}).value || '';
        if (!fecha){ e.preventDefault(); _toast('Elige una fecha primero.', 'warning'); return false; }
        if (!tf || !tt){ e.preventDefault(); _toast('Toca tu hora de llegada en el calendario.', 'warning'); return false; }
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

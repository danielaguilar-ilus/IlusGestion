/* ════════════════════════════════════════════════════════════════════
   ILUS UI — Modales y toasts internos del sistema
   Reemplaza alert(), confirm() y prompt() nativos por componentes
   estilizados con la paleta ILUS (rojo, negro, blanco).

   Uso:
     await ilusConfirm({ title:'¿Eliminar?', message:'...', danger:true })
       → resuelve a true|false
     ilusToast('Visita creada', { type:'success' })
     ilusAlert({ title:'Atención', message:'...', type:'warning' })
       → resuelve a true cuando el usuario cierra
═══════════════════════════════════════════════════════════════════════ */

(function(global){
  'use strict';

  // CSS — solo se inyecta una vez
  const STYLE_ID = '__ilus_ui_styles';
  function ensureStyles(){
    if (document.getElementById(STYLE_ID)) return;
    const s = document.createElement('style');
    s.id = STYLE_ID;
    s.textContent = `
      .ilus-overlay{
        position:fixed;inset:0;background:rgba(15,23,42,.55);
        backdrop-filter:blur(3px);
        display:flex;align-items:center;justify-content:center;
        z-index:99999;padding:16px;
        opacity:0;transition:opacity .14s ease;
      }
      .ilus-overlay.show{opacity:1}
      .ilus-modal{
        background:#fff;border-radius:14px;max-width:480px;width:100%;
        box-shadow:0 20px 60px rgba(0,0,0,.35);
        border:2px solid #dc2626;
        overflow:hidden;
        transform:translateY(8px) scale(.97);
        transition:transform .18s cubic-bezier(.2,.8,.2,1.05);
      }
      .ilus-overlay.show .ilus-modal{transform:translateY(0) scale(1)}
      .ilus-modal-head{
        background:linear-gradient(135deg,#0f172a 0%,#1f2937 100%);
        color:#fff;padding:14px 18px;
        border-bottom:3px solid #dc2626;
        display:flex;align-items:center;gap:10px;
      }
      .ilus-modal-head .ilus-icon{
        width:34px;height:34px;border-radius:50%;
        display:flex;align-items:center;justify-content:center;
        font-size:1.1rem;flex-shrink:0;
      }
      .ilus-modal-head h6{margin:0;font-weight:700;font-size:1rem;flex:1}
      .ilus-modal-body{padding:18px;color:#374151;font-size:.92rem;line-height:1.5}
      .ilus-modal-body .ilus-msg-sub{color:#6b7280;font-size:.83rem;margin-top:6px}
      .ilus-modal-foot{
        display:flex;justify-content:flex-end;gap:8px;
        padding:12px 18px;background:#fafafa;border-top:1px solid #e5e7eb;
      }
      .ilus-btn{
        padding:7px 18px;border-radius:8px;font-weight:600;font-size:.85rem;
        border:1px solid transparent;cursor:pointer;
        transition:all .12s;display:inline-flex;align-items:center;gap:6px;
      }
      .ilus-btn-secondary{background:#fff;color:#374151;border-color:#d1d5db}
      .ilus-btn-secondary:hover{background:#f3f4f6;border-color:#9ca3af}
      .ilus-btn-primary{background:#dc2626;color:#fff;border-color:#dc2626}
      .ilus-btn-primary:hover{background:#b91c1c;border-color:#b91c1c}
      .ilus-btn-danger{background:#dc2626;color:#fff;border-color:#dc2626}
      .ilus-btn-danger:hover{background:#991b1b;border-color:#991b1b}

      /* Toasts */
      .ilus-toast-wrap{
        position:fixed;top:20px;right:20px;
        display:flex;flex-direction:column;gap:8px;
        z-index:99998;pointer-events:none;
        max-width:380px;
      }
      .ilus-toast{
        background:#0f172a;color:#fff;
        border-radius:10px;padding:11px 14px;
        display:flex;align-items:center;gap:10px;
        box-shadow:0 10px 30px rgba(0,0,0,.25);
        border-left:4px solid #6b7280;
        font-size:.88rem;font-weight:600;
        pointer-events:auto;
        transform:translateX(110%);
        transition:transform .25s cubic-bezier(.2,.8,.2,1.1);
      }
      .ilus-toast.show{transform:translateX(0)}
      .ilus-toast .ico{font-size:1.15rem;flex-shrink:0}
      .ilus-toast .msg{flex:1;line-height:1.35}
      .ilus-toast .x{
        background:transparent;border:none;color:#9ca3af;
        font-size:1.05rem;cursor:pointer;padding:2px 6px;
      }
      .ilus-toast .x:hover{color:#fff}
      .ilus-toast.success{border-left-color:#22c55e}
      .ilus-toast.success .ico{color:#22c55e}
      .ilus-toast.error  {border-left-color:#ef4444}
      .ilus-toast.error   .ico{color:#ef4444}
      .ilus-toast.warning{border-left-color:#f59e0b}
      .ilus-toast.warning .ico{color:#f59e0b}
      .ilus-toast.info   {border-left-color:#3b82f6}
      .ilus-toast.info    .ico{color:#3b82f6}
    `;
    document.head.appendChild(s);
  }

  // Mapa tipo → icon + color del head
  const TYPE_CFG = {
    info:    { icon:'bi-info-circle-fill',     color:'#3b82f6', bg:'rgba(59,130,246,.18)' },
    success: { icon:'bi-check-circle-fill',    color:'#22c55e', bg:'rgba(34,197,94,.18)' },
    warning: { icon:'bi-exclamation-triangle-fill', color:'#f59e0b', bg:'rgba(245,158,11,.18)' },
    error:   { icon:'bi-x-circle-fill',        color:'#ef4444', bg:'rgba(239,68,68,.18)' },
    danger:  { icon:'bi-exclamation-octagon-fill', color:'#dc2626', bg:'rgba(220,38,38,.18)' },
    question:{ icon:'bi-question-circle-fill', color:'#dc2626', bg:'rgba(220,38,38,.18)' },
  };

  function buildModal({title, message, sub, type, buttons, subHtml, messageHtml}){
    ensureStyles();
    const cfg = TYPE_CFG[type] || TYPE_CFG.info;
    const overlay = document.createElement('div');
    overlay.className = 'ilus-overlay';
    // sub/message admiten HTML opcional con las flags subHtml/messageHtml = true.
    // ⚠ Solo pasar HTML literal seguro, NUNCA input del usuario sin sanitizar.
    overlay.innerHTML = `
      <div class="ilus-modal" role="dialog" aria-modal="true">
        <div class="ilus-modal-head">
          <div class="ilus-icon" style="background:${cfg.bg};color:${cfg.color}">
            <i class="bi ${cfg.icon}"></i>
          </div>
          <h6>${escapeHtml(title || '')}</h6>
        </div>
        <div class="ilus-modal-body">
          <div>${messageHtml ? (message || '') : escapeHtml(message || '')}</div>
          ${sub ? `<div class="ilus-msg-sub">${subHtml ? sub : escapeHtml(sub)}</div>` : ''}
        </div>
        <div class="ilus-modal-foot">
          ${buttons.map((b,i) =>
            `<button class="ilus-btn ilus-btn-${b.style||'secondary'}" data-idx="${i}">${escapeHtml(b.label)}</button>`
          ).join('')}
        </div>
      </div>`;
    document.body.appendChild(overlay);
    requestAnimationFrame(() => overlay.classList.add('show'));
    return overlay;
  }

  function close(overlay, val, resolve){
    overlay.classList.remove('show');
    setTimeout(() => { overlay.remove(); resolve(val); }, 160);
  }

  function escapeHtml(s){
    return String(s||'').replace(/[&<>"']/g, c =>
      ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  }

  // ── ilusConfirm ─────────────────────────────────────────────────────
  function ilusConfirm(opts = {}){
    const {
      title='Confirmar acción', message='¿Estás seguro?', sub='',
      okLabel='Aceptar', cancelLabel='Cancelar',
      danger=false, type=null,
      subHtml=false, messageHtml=false,
    } = opts;
    return new Promise(resolve => {
      const finalType = type || (danger ? 'danger' : 'question');
      const overlay = buildModal({
        title, message, sub, type: finalType,
        subHtml, messageHtml,
        buttons: [
          { label: cancelLabel, style:'secondary' },
          { label: okLabel, style: danger ? 'danger' : 'primary' },
        ]
      });
      overlay.querySelectorAll('.ilus-btn').forEach(btn => {
        btn.addEventListener('click', () => {
          const idx = parseInt(btn.dataset.idx, 10);
          close(overlay, idx === 1, resolve);
        });
      });
      overlay.addEventListener('click', e => {
        if (e.target === overlay) close(overlay, false, resolve);
      });
      document.addEventListener('keydown', function esc(e){
        if (e.key === 'Escape'){ close(overlay, false, resolve); document.removeEventListener('keydown', esc); }
        if (e.key === 'Enter') {
          const ok = overlay.querySelector('.ilus-btn[data-idx="1"]');
          if (ok) ok.click();
        }
      });
      setTimeout(() => {
        const ok = overlay.querySelector('.ilus-btn[data-idx="1"]');
        if (ok) ok.focus();
      }, 200);
    });
  }

  // ── ilusAlert ───────────────────────────────────────────────────────
  function ilusAlert(opts = {}){
    const {
      title='Atención', message='', sub='',
      okLabel='Aceptar', type='info',
      subHtml=false, messageHtml=false,
    } = (typeof opts === 'string' ? {message: opts} : opts);
    return new Promise(resolve => {
      const overlay = buildModal({
        title, message, sub, type,
        subHtml, messageHtml,
        buttons: [{ label: okLabel, style: 'primary' }]
      });
      const close_ = () => close(overlay, true, resolve);
      overlay.querySelector('.ilus-btn').addEventListener('click', close_);
      overlay.addEventListener('click', e => { if (e.target === overlay) close_(); });
      document.addEventListener('keydown', function esc(e){
        if (e.key === 'Escape' || e.key === 'Enter'){ close_(); document.removeEventListener('keydown', esc); }
      });
    });
  }

  // ── ilusToast ───────────────────────────────────────────────────────
  function ilusToast(message, opts = {}){
    ensureStyles();
    const { type='info', duration=3500 } = opts;
    let wrap = document.querySelector('.ilus-toast-wrap');
    if (!wrap){
      wrap = document.createElement('div');
      wrap.className = 'ilus-toast-wrap';
      document.body.appendChild(wrap);
    }
    const cfg = TYPE_CFG[type] || TYPE_CFG.info;
    const t = document.createElement('div');
    t.className = `ilus-toast ${type}`;
    t.innerHTML = `
      <i class="bi ${cfg.icon} ico"></i>
      <div class="msg">${escapeHtml(message)}</div>
      <button class="x" aria-label="Cerrar"><i class="bi bi-x-lg"></i></button>
    `;
    wrap.appendChild(t);
    requestAnimationFrame(() => t.classList.add('show'));
    const remove = () => {
      t.classList.remove('show');
      setTimeout(() => t.remove(), 250);
    };
    t.querySelector('.x').addEventListener('click', remove);
    if (duration > 0) setTimeout(remove, duration);
    return { close: remove };
  }

  // ── ilusPrompt ──────────────────────────────────────────────────────
  // Reemplazo de window.prompt() — input con label y validación.
  // Retorna Promise<string|null>. Null = cancelado.
  function ilusPrompt(opts = {}){
    const {
      title='Ingresar valor', message='', sub='',
      placeholder='', defaultValue='', okLabel='Aceptar', cancelLabel='Cancelar',
      type='question', inputType='text', required=true, multiline=false,
    } = (typeof opts === 'string' ? {message: opts} : opts);

    return new Promise(resolve => {
      ensureStyles();
      const cfg = TYPE_CFG[type] || TYPE_CFG.question;
      const overlay = document.createElement('div');
      overlay.className = 'ilus-overlay';
      const inputHtml = multiline
        ? `<textarea class="ilus-prompt-input" rows="4" placeholder="${escapeHtml(placeholder)}">${escapeHtml(defaultValue)}</textarea>`
        : `<input type="${escapeHtml(inputType)}" class="ilus-prompt-input" placeholder="${escapeHtml(placeholder)}" value="${escapeHtml(defaultValue)}">`;
      // sub permite HTML opcional con la flag `subHtml: true` (cuidado: NO
      // pasar input del usuario sin sanitizar — solo strings literales seguros).
      const subHtml = opts.subHtml === true;
      overlay.innerHTML = `
        <div class="ilus-modal" role="dialog" aria-modal="true">
          <div class="ilus-modal-head">
            <div class="ilus-icon" style="background:${cfg.bg};color:${cfg.color}">
              <i class="bi ${cfg.icon}"></i>
            </div>
            <h6>${escapeHtml(title)}</h6>
          </div>
          <div class="ilus-modal-body">
            ${message ? `<div style="margin-bottom:10px">${escapeHtml(message)}</div>` : ''}
            ${inputHtml}
            ${sub ? `<div class="ilus-msg-sub">${subHtml ? sub : escapeHtml(sub)}</div>` : ''}
          </div>
          <div class="ilus-modal-foot">
            <button class="ilus-btn ilus-btn-secondary" data-idx="0">${escapeHtml(cancelLabel)}</button>
            <button class="ilus-btn ilus-btn-primary" data-idx="1">${escapeHtml(okLabel)}</button>
          </div>
        </div>`;
      // Estilo del input — inyectado una vez
      if (!document.getElementById('__ilus_prompt_input_css')){
        const s = document.createElement('style');
        s.id = '__ilus_prompt_input_css';
        s.textContent = `
          .ilus-prompt-input{
            width:100%;padding:9px 12px;border:1.5px solid #d1d5db;
            border-radius:8px;font-size:.92rem;background:#fff;color:#111827;
            outline:none;transition:border-color .12s, box-shadow .12s;
            font-family:inherit;
          }
          .ilus-prompt-input:focus{
            border-color:#dc2626;box-shadow:0 0 0 3px rgba(220,38,38,.15);
          }
        `;
        document.head.appendChild(s);
      }
      document.body.appendChild(overlay);
      requestAnimationFrame(() => overlay.classList.add('show'));
      const inputEl = overlay.querySelector('.ilus-prompt-input');
      setTimeout(() => { inputEl.focus(); inputEl.select && inputEl.select(); }, 200);

      function done(ok){
        const val = ok ? inputEl.value : null;
        if (ok && required && (!val || !val.trim())){
          inputEl.style.borderColor = '#dc2626';
          inputEl.focus();
          return;
        }
        overlay.classList.remove('show');
        setTimeout(() => { overlay.remove(); resolve(ok ? val : null); }, 160);
      }

      overlay.querySelector('[data-idx="0"]').addEventListener('click', () => done(false));
      overlay.querySelector('[data-idx="1"]').addEventListener('click', () => done(true));
      overlay.addEventListener('click', e => { if (e.target === overlay) done(false); });
      inputEl.addEventListener('keydown', e => {
        if (e.key === 'Escape') done(false);
        if (e.key === 'Enter' && !multiline){ e.preventDefault(); done(true); }
      });
    });
  }

  // ── ilusActionSheet ─────────────────────────────────────────────────
  // Sheet de N opciones (NO un Yes/No como ilusConfirm).
  // Mobile-first: aparece desde abajo, full-width, touch-friendly.
  // Desktop: centrado como modal normal.
  // Cada opción es { label, icon (opcional bi-clase), value, style (primary|secondary|danger|dark) }
  // Resuelve a `value` de la opción elegida, o null si canceló.
  //
  // Uso:
  //   const v = await ilusActionSheet({
  //     title: 'Agregar foto',
  //     options: [
  //       { label:'Tomar foto', icon:'bi-camera-fill', value:'camara', style:'dark' },
  //       { label:'Galería',    icon:'bi-images',     value:'galeria', style:'secondary' },
  //     ],
  //   });
  //   if (v === 'camara') ...
  function ilusActionSheet(opts = {}){
    const {
      title='Elegí una opción', message='', sub='',
      options=[], cancelLabel='Cancelar',
      subHtml=false,
    } = opts;
    // Inyectar CSS de sheet la primera vez
    if (!document.getElementById('__ilus_action_sheet_css')){
      const s = document.createElement('style');
      s.id = '__ilus_action_sheet_css';
      s.textContent = `
        .ilus-sheet-overlay{
          position:fixed;inset:0;background:rgba(15,23,42,.55);
          backdrop-filter:blur(3px);
          z-index:99999;
          opacity:0;transition:opacity .14s ease;
          display:flex;align-items:flex-end;justify-content:center;
        }
        .ilus-sheet-overlay.show{opacity:1}
        .ilus-sheet{
          background:#fff;width:100%;max-width:520px;
          border-radius:18px 18px 0 0;
          box-shadow:0 -10px 40px rgba(0,0,0,.35);
          border-top:3px solid #dc2626;
          overflow:hidden;
          transform:translateY(100%);
          transition:transform .22s cubic-bezier(.2,.8,.2,1.05);
          padding-bottom:env(safe-area-inset-bottom, 0);
        }
        .ilus-sheet-overlay.show .ilus-sheet{transform:translateY(0)}
        .ilus-sheet-head{
          padding:14px 18px 10px;text-align:center;position:relative;
        }
        .ilus-sheet-head::before{
          content:'';display:block;width:42px;height:5px;
          background:#cbd5e1;border-radius:3px;margin:0 auto 10px;
        }
        .ilus-sheet-head h6{
          margin:0;font-weight:700;font-size:1.02rem;color:#0f172a;
        }
        .ilus-sheet-head .sheet-msg{
          color:#475569;font-size:.85rem;margin-top:4px;
        }
        .ilus-sheet-head .sheet-sub{
          color:#94a3b8;font-size:.78rem;margin-top:3px;
        }
        .ilus-sheet-body{padding:6px 14px 10px}
        .ilus-sheet-opt{
          display:flex;align-items:center;gap:12px;width:100%;
          padding:14px 16px;border-radius:12px;
          background:#fff;color:#0f172a;
          border:1.5px solid #e5e7eb;
          font-weight:600;font-size:.95rem;
          cursor:pointer;margin-bottom:8px;
          min-height:54px;text-align:left;
          transition:all .12s;
        }
        .ilus-sheet-opt:hover{background:#f9fafb;border-color:#cbd5e1}
        .ilus-sheet-opt:active{transform:scale(.98)}
        .ilus-sheet-opt .ico{
          font-size:1.35rem;width:30px;flex-shrink:0;text-align:center;
        }
        .ilus-sheet-opt .lbl{flex:1}
        .ilus-sheet-opt .chev{color:#cbd5e1;font-size:1.1rem}
        /* Estilos */
        .ilus-sheet-opt.dark{
          background:linear-gradient(135deg,#0a0a0a,#2a2a2a);
          color:#fff;border-color:#1f1f1f;
        }
        .ilus-sheet-opt.dark:hover{background:linear-gradient(135deg,#1a1a1a,#3a3a3a)}
        .ilus-sheet-opt.dark .chev{color:#525252}
        .ilus-sheet-opt.primary{
          background:#dc2626;color:#fff;border-color:#dc2626;
        }
        .ilus-sheet-opt.primary:hover{background:#b91c1c;border-color:#b91c1c}
        .ilus-sheet-opt.primary .chev{color:#fecaca}
        .ilus-sheet-opt.danger{
          background:#fee2e2;color:#991b1b;border-color:#fecaca;
        }
        .ilus-sheet-opt.danger:hover{background:#fecaca}
        .ilus-sheet-cancel{
          width:calc(100% - 28px);margin:6px 14px 14px;
          padding:13px;border-radius:12px;
          background:#f3f4f6;color:#374151;border:none;
          font-weight:700;font-size:.95rem;cursor:pointer;
          min-height:48px;
        }
        .ilus-sheet-cancel:hover{background:#e5e7eb}
        @media (min-width: 640px){
          .ilus-sheet-overlay{align-items:center;padding:16px}
          .ilus-sheet{
            border-radius:14px;
            border:2px solid #dc2626;border-top-width:3px;
            transform:translateY(20px) scale(.97);
          }
          .ilus-sheet-overlay.show .ilus-sheet{transform:translateY(0) scale(1)}
          .ilus-sheet-head::before{display:none}
        }
      `;
      document.head.appendChild(s);
    }

    return new Promise(resolve => {
      const overlay = document.createElement('div');
      overlay.className = 'ilus-sheet-overlay';
      const optsHtml = (options || []).map((o, i) => `
        <button type="button" class="ilus-sheet-opt ${o.style || 'secondary'}" data-idx="${i}">
          ${o.icon ? `<i class="bi ${escapeHtml(o.icon)} ico"></i>` : '<span class="ico"></span>'}
          <span class="lbl">${escapeHtml(o.label || '')}</span>
          <i class="bi bi-chevron-right chev"></i>
        </button>
      `).join('');
      overlay.innerHTML = `
        <div class="ilus-sheet" role="dialog" aria-modal="true">
          <div class="ilus-sheet-head">
            <h6>${escapeHtml(title)}</h6>
            ${message ? `<div class="sheet-msg">${escapeHtml(message)}</div>` : ''}
            ${sub ? `<div class="sheet-sub">${subHtml ? sub : escapeHtml(sub)}</div>` : ''}
          </div>
          <div class="ilus-sheet-body">${optsHtml}</div>
          <button type="button" class="ilus-sheet-cancel" data-cancel="1">${escapeHtml(cancelLabel)}</button>
        </div>`;
      document.body.appendChild(overlay);
      requestAnimationFrame(() => overlay.classList.add('show'));

      function done(val){
        overlay.classList.remove('show');
        setTimeout(() => { overlay.remove(); resolve(val); }, 180);
      }
      overlay.querySelectorAll('.ilus-sheet-opt').forEach(btn => {
        btn.addEventListener('click', () => {
          const idx = parseInt(btn.dataset.idx, 10);
          done(options[idx] ? (options[idx].value !== undefined ? options[idx].value : idx) : null);
        });
      });
      overlay.querySelector('.ilus-sheet-cancel').addEventListener('click', () => done(null));
      overlay.addEventListener('click', e => { if (e.target === overlay) done(null); });
      document.addEventListener('keydown', function esc(e){
        if (e.key === 'Escape'){ done(null); document.removeEventListener('keydown', esc); }
      });
    });
  }

  // Export global
  global.ilusConfirm = ilusConfirm;
  global.ilusAlert   = ilusAlert;
  global.ilusToast   = ilusToast;
  global.ilusPrompt  = ilusPrompt;
  global.ilusActionSheet = ilusActionSheet;

  // ════════════════════════════════════════════════════════════════
  //  GOOGLE PLACES AUTOCOMPLETE — helper reusable (LAZY load)
  //
  //  Uso:
  //    ilusPlacesAutocomplete('inputId', {
  //      onPlaceSelected: (place) => { ... },
  //      country: 'cl',         // Chile por default
  //      types: ['address'],    // o 'establishment', '(regions)', etc.
  //    });
  //
  //  Performance: el SDK de Google Maps (~250 KB) NO se carga al
  //  abrir la página. Recién en la primera llamada a esta función
  //  inyectamos el <script> async. Páginas que no usan autocomplete
  //  (login, dashboards, listados) ahorran 250 KB + handshake TLS.
  //
  //  Buffer __ilusGmapsPending sigue funcionando: cualquier código
  //  legacy que pushee callbacks ahí se ejecuta cuando el SDK termina
  //  de cargar (callback __ilusGmapsReady — definido en base.html).
  // ════════════════════════════════════════════════════════════════
  function ensureGmapsSdk(){
    if (window.__ilusGmapsSdkRequested) return;       // ya pedimos
    if (window.google && window.google.maps && window.google.maps.places) return;  // ya cargó
    if (!window.__ILUS_GMAPS_KEY) return;             // sin API key, fallback texto plano
    window.__ilusGmapsSdkRequested = true;
    var s = document.createElement('script');
    s.async = true;
    s.defer = true;
    s.src = 'https://maps.googleapis.com/maps/api/js?key=' +
            encodeURIComponent(window.__ILUS_GMAPS_KEY) +
            '&libraries=places&language=es&region=CL&callback=__ilusGmapsReady&loading=async';
    document.head.appendChild(s);
  }

  function ilusPlacesAutocomplete(inputIdOrEl, opts){
    opts = opts || {};
    const input = (typeof inputIdOrEl === 'string')
      ? document.getElementById(inputIdOrEl)
      : inputIdOrEl;
    if (!input) {
      console.warn('[ilusPlaces] input no encontrado:', inputIdOrEl);
      return;
    }
    // Lazy-load del SDK: solo lo pedimos cuando una página realmente
    // necesita autocomplete (no en login/dashboard).
    ensureGmapsSdk();
    // Si Google Maps NO se cargó (no hay API key), fallback silencioso:
    // el input sigue funcionando como texto plano.
    function tryInit(){
      if (!window.google || !window.google.maps || !window.google.maps.places){
        // Esperar a que cargue el SDK
        if (window.__ilusGmapsPending){
          window.__ilusGmapsPending.push(tryInit);
        } else {
          console.warn('[ilusPlaces] Google Maps SDK no disponible (sin API key). Input funcionará como texto plano.');
        }
        return;
      }
      try {
        const country = (opts.country || 'cl').toLowerCase();
        const ac = new google.maps.places.Autocomplete(input, {
          componentRestrictions: { country: [country] },
          types: opts.types || ['address'],
          fields: ['formatted_address', 'geometry', 'address_components', 'name', 'place_id'],
        });
        ac.addListener('place_changed', function(){
          const place = ac.getPlace();
          if (!place || !place.geometry){
            // El usuario tipeó pero no eligió sugerencia — opcional callback
            if (typeof opts.onNoSelection === 'function') opts.onNoSelection(input.value);
            return;
          }
          // Llenar el input con la dirección completa
          if (place.formatted_address) input.value = place.formatted_address;
          // Callback con datos enriquecidos
          if (typeof opts.onPlaceSelected === 'function'){
            opts.onPlaceSelected({
              direccion: place.formatted_address,
              lat: place.geometry.location.lat(),
              lng: place.geometry.location.lng(),
              place_id: place.place_id,
              componentes: place.address_components,
              raw: place,
            });
          }
        });
        // Marcar el input como inicializado
        input.dataset.placesInit = '1';
      } catch(e){
        console.warn('[ilusPlaces] error inicializando autocomplete:', e);
      }
    }
    tryInit();
  }
  global.ilusPlacesAutocomplete = ilusPlacesAutocomplete;
  global.ilusGmapsDisponible = function(){
    return !!(window.google && window.google.maps && window.google.maps.places);
  };

  // ════════════════════════════════════════════════════════════════
  //  SHIM GLOBAL — window.alert() → ilusToast/ilusAlert
  //
  //  Objetivo: eliminar TODOS los popups grises feos del navegador
  //  ("web-production-XXXX dice...") sin tener que tocar 199+
  //  llamadas dispersas por los templates.
  //
  //  Estrategia:
  //    - Mensajes cortos (≤ 80 chars) → ilusToast tipo warning (no
  //      bloqueante, se va solo en 3.5s)
  //    - Mensajes largos → ilusAlert (modal explícito que el usuario
  //      acepta — caso típico: límites, advertencias importantes)
  //    - Si por algo falla la UI ilus, cae al alert nativo (último
  //      recurso para no perder mensajes críticos).
  //
  //  Para window.confirm: NO se intercepta porque es síncrono y
  //  el ilusConfirm es asíncrono — un reemplazo silencioso rompería
  //  el flujo de decisión de los callers. Se migran manualmente.
  // ════════════════════════════════════════════════════════════════
  const _nativeAlert = global.alert.bind(global);
  global.alert = function(msg) {
    try {
      const text = (msg == null) ? '' : String(msg);
      const isLong = text.length > 80 || text.indexOf('\n') >= 0;
      // Heurísticas para mejor tipo de toast/alert
      const lower = text.toLowerCase();
      let type = 'warning';
      if (lower.indexOf('error') >= 0 || lower.indexOf('falló') >= 0 ||
          lower.indexOf('no se pudo') >= 0) {
        type = 'error';
      } else if (lower.indexOf('éxito') >= 0 || lower.indexOf('exitoso') >= 0 ||
                 lower.indexOf('✓') >= 0 || lower.indexOf('correctamente') >= 0) {
        type = 'success';
      } else if (lower.indexOf('listo') >= 0 || lower.indexOf('info') >= 0) {
        type = 'info';
      }
      if (isLong) {
        // Mensaje largo: usar modal (bloqueante con OK)
        // Si tiene salto de línea, primera línea como título.
        const lines = text.split('\n');
        const title = (lines.length > 1 && lines[0].length < 80) ? lines[0] : 'Atención';
        const body = (lines.length > 1) ? lines.slice(1).join('\n') : text;
        ilusAlert({
          title,
          message: body,
          type: type === 'warning' ? 'warning' : type,
        });
      } else {
        // Mensaje corto: toast
        ilusToast(text, { type, duration: 4500 });
      }
    } catch (e) {
      // Si la UI ilus falla, no perder el mensaje
      console.warn('[ilus_ui] alert shim falló, caigo al nativo:', e);
      _nativeAlert(msg);
    }
  };

  // Helper expuesto por si algún template lo necesita explícito
  global.alertNativo = _nativeAlert;

  // ════════════════════════════════════════════════════════════════
  // MODAL BACKDROP CLEANUP — fix global para backdrops huérfanos
  // (Bug 2026-05-17: al cerrar modal "Generar OT" la página queda
  // oscurecida hasta refrescar)
  //
  // Causa raíz: Bootstrap .hide() es async (~300ms). Si se abre un
  // ilus-overlay encima inmediatamente, roba foco y Bootstrap aborta
  // el cleanup → .modal-backdrop queda huérfano + body.modal-open.
  //
  // Solución: listener global a hidden.bs.modal + shown.bs.modal que
  // sincroniza backdrops con modales realmente abiertos. Defensivo —
  // no interfiere con flujos normales.
  // ════════════════════════════════════════════════════════════════
  function ilusCleanModalBackdrops(){
    try {
      const abiertos = document.querySelectorAll('.modal.show').length;
      const backdrops = document.querySelectorAll('.modal-backdrop');
      if (abiertos === 0){
        backdrops.forEach(b => b.remove());
        document.body.classList.remove('modal-open');
        document.body.style.removeProperty('overflow');
        document.body.style.removeProperty('padding-right');
      } else if (backdrops.length > abiertos){
        // Hay más backdrops que modales abiertos → matar los sobrantes
        for (let i = backdrops.length - 1; i >= abiertos; i--){
          backdrops[i].remove();
        }
      }
    } catch (e) {
      console.warn('[ilus_ui] cleanModalBackdrops:', e);
    }
  }
  global.ilusCleanModalBackdrops = ilusCleanModalBackdrops;

  // Auto-cleanup cuando CUALQUIER modal Bootstrap se cierra.
  // setTimeout(0) garantiza que corremos DESPUÉS del propio cleanup
  // de Bootstrap (evita race contra su transitionend listener).
  document.addEventListener('hidden.bs.modal', function(){
    setTimeout(ilusCleanModalBackdrops, 0);
    // Segundo intento por si quedó algo colgado en transitionend
    setTimeout(ilusCleanModalBackdrops, 400);
  });
  document.addEventListener('shown.bs.modal', ilusCleanModalBackdrops);

  // ESC sale de cualquier overlay huérfano (último recurso)
  document.addEventListener('keydown', function(e){
    if (e.key === 'Escape'){
      // Si no hay modal Bootstrap visible pero queda backdrop → matar
      const abiertos = document.querySelectorAll('.modal.show').length;
      const backdrops = document.querySelectorAll('.modal-backdrop');
      if (abiertos === 0 && backdrops.length > 0){
        ilusCleanModalBackdrops();
      }
    }
  });

  // Cleanup periódico defensivo (cada 5s) — DETECTA backdrops huérfanos
  // que dejaron pasar los listeners de Bootstrap. Costo: ~0.1ms cada 5s.
  setInterval(function(){
    const abiertos = document.querySelectorAll('.modal.show').length;
    const backdrops = document.querySelectorAll('.modal-backdrop');
    if (abiertos === 0 && backdrops.length > 0){
      console.warn('[ilus_ui] backdrop huérfano detectado, limpiando');
      ilusCleanModalBackdrops();
    }
  }, 5000);
})(window);

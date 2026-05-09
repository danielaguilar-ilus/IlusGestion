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

  function buildModal({title, message, sub, type, buttons}){
    ensureStyles();
    const cfg = TYPE_CFG[type] || TYPE_CFG.info;
    const overlay = document.createElement('div');
    overlay.className = 'ilus-overlay';
    overlay.innerHTML = `
      <div class="ilus-modal" role="dialog" aria-modal="true">
        <div class="ilus-modal-head">
          <div class="ilus-icon" style="background:${cfg.bg};color:${cfg.color}">
            <i class="bi ${cfg.icon}"></i>
          </div>
          <h6>${escapeHtml(title || '')}</h6>
        </div>
        <div class="ilus-modal-body">
          <div>${escapeHtml(message || '')}</div>
          ${sub ? `<div class="ilus-msg-sub">${escapeHtml(sub)}</div>` : ''}
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
    } = opts;
    return new Promise(resolve => {
      const finalType = type || (danger ? 'danger' : 'question');
      const overlay = buildModal({
        title, message, sub, type: finalType,
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
    } = (typeof opts === 'string' ? {message: opts} : opts);
    return new Promise(resolve => {
      const overlay = buildModal({
        title, message, sub, type,
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

  // Export global
  global.ilusConfirm = ilusConfirm;
  global.ilusAlert   = ilusAlert;
  global.ilusToast   = ilusToast;
})(window);

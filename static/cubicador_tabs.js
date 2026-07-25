/* ══════════════════════════════════════════════════════════════════════════
   ILUS · CUBICADOR — Pestañas + Modal de Medidas + Botón Etiquetas
   ──────────────────────────────────────────────────────────────────────────
   Archivo NUEVO (2026-07-24). NO reemplaza ni modifica nada del Cubicador
   actual: sólo AGREGA comportamiento sobre marcado nuevo (REGLA #4.2).

   Se carga desde templates/cubicador/index.html DESPUÉS de ilus_ui.js:
       <script src="{{ url_for('static', filename='cubicador_tabs.js') }}"></script>
   (sin ?v= — el proyecto tiene cache-busting automático por hash).

   Qué hace
   ────────
   1) PESTAÑAS con memoria (localStorage 'cubicador_tab'). Imprescindible
      porque "Cubicar" recarga la página entera.
   2) MODAL DE MEDIDAS por fila: precarga los bultos existentes, edita
      largo/ancho/alto/peso, calcula volumen y peso volumétrico en vivo,
      guarda, maneja el 409 de reducción de bultos y actualiza la fila +
      los totales de la página SIN recargar.
   3) BOTÓN ETIQUETAS: abre la impresión del SKU en pestaña nueva; si el
      producto no tiene medidas avisa y ofrece abrir "Medidas".

   Reglas respetadas
   ─────────────────
   · REGLA #1  — cero alert/confirm/prompt nativos (ilusToast/ilusConfirm).
   · REGLA #2  — rojo #dc2626 / negro #0a0a0a / blanco.
   · REGLA #3  — mobile-first: modal fullscreen, inputs 16px, botones 44px.
   · CSRF      — fetch va "pelado": el wrapper global de base.html inyecta
                 X-CSRF-Token en todo método != GET/HEAD.
   · Defensivo — si falta un elemento NO explota: el Cubicador actual debe
                 seguir funcionando aunque este archivo falle entero.

   Convenciones del cálculo (espejo del backend, app.py):
       vol_cm3   = Σ (largo × ancho × alto)          [cm]
       peso_vol  = vol_cm3 / 4000                     [kg]
       predom    = max(peso_kg, peso_vol)             [kg]
   ══════════════════════════════════════════════════════════════════════════ */

(function (global) {
  'use strict';

  /* ────────────────────────────────────────────────────────────────────
     CONFIGURACIÓN
     El template puede sobreescribir cualquier clave declarando ANTES:
       window.CUBICADOR_TABS_CFG = {
         medidasTpl: "{{ url_for('cubicador_api_medidas', pid=0) }}",
         printSetupTpl: "{{ url_for('product_print_setup', pid=0) }}",
       };
     Se usa el patrón "pid=0 y reemplazo" que ya usa quickView() (QUICK_TPL).
     ──────────────────────────────────────────────────────────────────── */
  var DEFAULTS = {
    // GET/POST de medidas. El 0 se reemplaza por el product_id real.
    medidasTpl: '/cubicador/api/producto/0/medidas',
    // Guardado cuando la línea NO tiene ficha local (app_id nulo): resuelve o
    // crea el producto por SKU y aplica las MISMAS redes de seguridad.
    medidasPorSkuUrl: '/cubicador/api/producto/por-sku/medidas',
    // Endpoint LEGACY de transporte. Sigue existiendo y funcionando, pero
    // borra y reinserta sin 409, sin auditoría y sin tope de 0 bultos: solo se
    // usa si el usuario lo confirma explícitamente (ver guardarMedidas).
    inlineBultoUrl: '/transporte/api/inline-bulto',
    // Pantalla de impresión de etiquetas (HTML, se abre con window.open).
    printSetupTpl: '/products/0/print-setup',
    // Clave de localStorage para la pestaña activa.
    tabStorageKey: 'cubicador_tab',
    // Pestaña por defecto si no hay nada guardado.
    defaultTab: 'cubicador',
    divisorVolumetrico: 4000,
  };

  var CFG = {};
  (function mergeCfg() {
    var user = global.CUBICADOR_TABS_CFG || {};
    for (var k in DEFAULTS) { if (Object.prototype.hasOwnProperty.call(DEFAULTS, k)) CFG[k] = DEFAULTS[k]; }
    for (var u in user) { if (Object.prototype.hasOwnProperty.call(user, u) && user[u]) CFG[u] = user[u]; }
  })();

  function urlFor(tpl, pid) {
    var s = String(tpl || '');
    // Soporta ".../0/medidas", ".../producto/0" y "{pid}".
    if (s.indexOf('{pid}') !== -1) return s.replace('{pid}', String(pid));
    if (s.indexOf('/0/') !== -1) return s.replace('/0/', '/' + pid + '/');
    if (/\/0$/.test(s)) return s.replace(/\/0$/, '/' + pid);
    return s + (s.indexOf('?') === -1 ? '?' : '&') + 'pid=' + encodeURIComponent(pid);
  }

  /* ────────────────────────────────────────────────────────────────────
     UTILIDADES
     ──────────────────────────────────────────────────────────────────── */
  function $(sel, root) { try { return (root || document).querySelector(sel); } catch (e) { return null; } }
  function $$(sel, root) {
    try { return Array.prototype.slice.call((root || document).querySelectorAll(sel)); }
    catch (e) { return []; }
  }
  function esc(s) {
    return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) {
      return ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[c];
    });
  }
  /* Acepta "12,5" (coma chilena) y "12.5". Devuelve Number (0 si no sirve). */
  function num(v) {
    if (v == null || v === '') return 0;
    var n = Number(String(v).replace(/\s/g, '').replace(',', '.'));
    return isFinite(n) ? n : 0;
  }
  /* Espejo de |fkg del backend: 1.234,5 */
  function fkg(n, decimals) {
    decimals = (decimals == null) ? 1 : decimals;
    var v = Number(n) || 0;
    var s = v.toFixed(decimals);
    var neg = s.charAt(0) === '-';
    if (neg) s = s.slice(1);
    var parts = s.split('.');
    var intPart = parts[0], decPart = parts[1] || '', out = '';
    for (var i = 0, l = intPart.length; i < l; i++) {
      if (i > 0 && (l - i) % 3 === 0) out += '.';
      out += intPart.charAt(i);
    }
    return (neg ? '-' : '') + out + (decPart ? (',' + decPart) : '');
  }
  /* Espejo de |fm3: entra cm³ → '5,673' */
  function fm3(cm3) {
    return ((Number(cm3) || 0) / 1000000).toFixed(3).replace('.', ',');
  }

  function toast(msg, type) {
    if (typeof global.ilusToast === 'function') { global.ilusToast(msg, { type: type || 'info' }); return; }
    try { console.log('[cubicador]', msg); } catch (e) {}
  }
  function confirmar(opts) {
    if (typeof global.ilusConfirm === 'function') return global.ilusConfirm(opts);
    // REGLA #1: prohibido confirm() nativo, incluso como fallback. Si
    // ilus_ui.js no cargó por algún motivo, no bloqueamos con un popup gris:
    // avisamos por consola y devolvemos false (no continuar) para no
    // ejecutar por accidente una acción que el usuario no pudo confirmar.
    try { console.warn('[cubicador] ilusConfirm no disponible — se cancela la accion'); } catch (e) {}
    return Promise.resolve(false);
  }

  /* fetch + parseo tolerante (una respuesta HTML de login no debe explotar) */
  function fetchJson(url, opts) {
    return fetch(url, opts || {}).then(function (res) {
      return res.text().then(function (txt) {
        var data = null;
        try { data = txt ? JSON.parse(txt) : null; } catch (e) { data = null; }
        return { status: res.status, ok: res.ok, data: data, raw: txt };
      });
    });
  }

  /* ══════════════════════════════════════════════════════════════════════
     1) PESTAÑAS CON MEMORIA
     Marcado esperado (lo escribe el template):
        <div class="cubtabs-wrap"><div class="cubtabs" role="tablist">
          <button class="cubtab-btn active" data-tab="cubicador">…</button>
          <button class="cubtab-btn"        data-tab="cotizador">…</button>
          <button class="cubtab-btn"        data-tab="packing">…</button>
        </div></div>
        <div class="cubtab-pane active" id="cubtab-cubicador">…</div>
        <div class="cubtab-pane"        id="cubtab-cotizador">…</div>
        <div class="cubtab-pane"        id="cubtab-packing">…</div>
     Convención: id del panel = "cubtab-" + data-tab del botón.
     ══════════════════════════════════════════════════════════════════════ */

  var TAB_STYLE_ID = '__cub_tabs_styles';
  function ensureTabStyles() {
    if (document.getElementById(TAB_STYLE_ID)) return;
    var s = document.createElement('style');
    s.id = TAB_STYLE_ID;
    s.textContent = [
      '.cubtabs-wrap{background:#fff;border-bottom:2px solid #e5e7eb;margin-bottom:14px;',
      '  border-radius:8px 8px 0 0;box-shadow:0 2px 10px rgba(0,0,0,.06);}',
      '.cubtabs{display:flex;gap:0;padding:0 6px;overflow-x:auto;overflow-y:hidden;',
      '  scrollbar-width:none;-webkit-overflow-scrolling:touch;}',
      '.cubtabs::-webkit-scrollbar{display:none;}',
      '.cubtab-btn{background:none;border:none;outline:none;padding:12px 18px;',
      '  font-size:.85rem;font-weight:600;color:#6b7280;cursor:pointer;',
      '  border-bottom:3px solid transparent;margin-bottom:-2px;white-space:nowrap;',
      '  min-height:44px;display:inline-flex;align-items:center;gap:6px;',
      '  transition:color .15s,border-color .15s;}',
      '.cubtab-btn:hover{color:#374151;}',
      '.cubtab-btn:focus-visible{box-shadow:inset 0 0 0 2px rgba(220,38,38,.35);border-radius:6px;}',
      '.cubtab-btn.active{color:#dc2626;border-bottom-color:#dc2626;}',
      '.cubtab-btn .cubtab-badge{font-size:.62rem;background:#f3f4f6;color:#6b7280;',
      '  border-radius:999px;padding:1px 7px;font-weight:700;}',
      '.cubtab-btn.active .cubtab-badge{background:#fee2e2;color:#dc2626;}',
      '.cubtab-pane{display:none;}',
      '.cubtab-pane.active{display:block;}',
      '@media (max-width:767.98px){',
      '  .cubtabs-wrap{position:sticky;top:0;z-index:1020;margin-left:-4px;margin-right:-4px;border-radius:0;}',
      '  .cubtab-btn{padding:12px 14px;font-size:.8rem;}',
      '}',
      '@media print{.cubtabs-wrap{display:none !important;}}'
    ].join('\n');
    document.head.appendChild(s);
  }

  function tabButtons() { return $$('.cubtab-btn[data-tab]'); }
  function tabPane(name) { return document.getElementById('cubtab-' + name); }

  /* Enriquece el marcado con ARIA si el template no lo trae ya. */
  function enhanceTabsA11y() {
    var btns = tabButtons();
    if (!btns.length) return;
    var bar = btns[0].parentNode;
    if (bar && !bar.getAttribute('role')) bar.setAttribute('role', 'tablist');
    if (bar && !bar.getAttribute('aria-label')) bar.setAttribute('aria-label', 'Secciones del Cubicador');
    btns.forEach(function (b) {
      var name = b.getAttribute('data-tab');
      var pane = tabPane(name);
      if (!b.getAttribute('role')) b.setAttribute('role', 'tab');
      if (!b.id) b.id = 'cubtab-btn-' + name;
      var isActive = b.classList.contains('active');
      b.setAttribute('aria-selected', isActive ? 'true' : 'false');
      b.setAttribute('tabindex', isActive ? '0' : '-1');
      if (pane) {
        b.setAttribute('aria-controls', pane.id);
        if (!pane.getAttribute('role')) pane.setAttribute('role', 'tabpanel');
        pane.setAttribute('aria-labelledby', b.id);
        pane.setAttribute('tabindex', '0');
      }
    });
  }

  /**
   * Cambia de pestaña. Global: el HTML la llama con onclick="cubSwitchTab('x')".
   * @param {string} name  valor de data-tab (id del panel = 'cubtab-'+name)
   * @param {object} [opts] {focus:false, silent:false}  silent = no persiste
   * @returns {boolean} true si la pestaña existe y se activó
   */
  function cubSwitchTab(name, opts) {
    opts = opts || {};
    var btns = tabButtons();
    if (!btns.length) return false;
    var target = tabPane(name);
    var btn = null;
    for (var i = 0; i < btns.length; i++) {
      if (btns[i].getAttribute('data-tab') === name) { btn = btns[i]; break; }
    }
    if (!btn) return false;

    $$('.cubtab-pane').forEach(function (p) { p.classList.remove('active'); });
    btns.forEach(function (b) {
      b.classList.remove('active');
      b.setAttribute('aria-selected', 'false');
      b.setAttribute('tabindex', '-1');
    });
    if (target) target.classList.add('active');
    btn.classList.add('active');
    btn.setAttribute('aria-selected', 'true');
    btn.setAttribute('tabindex', '0');
    if (opts.focus) { try { btn.focus(); } catch (e) {} }

    if (!opts.silent) {
      try { localStorage.setItem(CFG.tabStorageKey, name); } catch (e) {}
    }
    try {
      document.dispatchEvent(new CustomEvent('cubicador:tab', { detail: { tab: name } }));
    } catch (e) {}
    return true;
  }

  function initTabs() {
    var btns = tabButtons();
    if (!btns.length) return;              // La página no trae pestañas → nada que hacer.
    ensureTabStyles();
    enhanceTabsA11y();

    // Click (además del onclick inline que puede traer el template).
    btns.forEach(function (b) {
      b.addEventListener('click', function () {
        cubSwitchTab(b.getAttribute('data-tab'));
      });
    });

    // Teclado: ←/→ mueven, Home/End a los extremos (patrón WAI-ARIA tabs).
    var bar = btns[0].parentNode;
    if (bar) {
      bar.addEventListener('keydown', function (ev) {
        var k = ev.key;
        if (['ArrowRight', 'ArrowLeft', 'Home', 'End'].indexOf(k) === -1) return;
        var list = tabButtons();
        var cur = list.indexOf(document.activeElement);
        if (cur === -1) {
          cur = 0;
          for (var i = 0; i < list.length; i++) { if (list[i].classList.contains('active')) { cur = i; break; } }
        }
        var next = cur;
        if (k === 'ArrowRight') next = (cur + 1) % list.length;
        else if (k === 'ArrowLeft') next = (cur - 1 + list.length) % list.length;
        else if (k === 'Home') next = 0;
        else if (k === 'End') next = list.length - 1;
        ev.preventDefault();
        cubSwitchTab(list[next].getAttribute('data-tab'), { focus: true });
      });
    }

    // Restaurar la última pestaña usada (el botón "Cubicar" recarga la página).
    var saved = null;
    try { saved = localStorage.getItem(CFG.tabStorageKey); } catch (e) {}
    if (saved && tabPane(saved)) {
      cubSwitchTab(saved, { silent: true });
    } else {
      // Si el template no marcó ninguna activa, activamos la primera.
      var anyActive = btns.some(function (b) { return b.classList.contains('active'); });
      if (!anyActive) cubSwitchTab(btns[0].getAttribute('data-tab'), { silent: true });
    }
  }

  /* ══════════════════════════════════════════════════════════════════════
     2) MODAL DE MEDIDAS
     ══════════════════════════════════════════════════════════════════════ */

  var MODAL_STYLE_ID = '__cub_medidas_styles';
  function ensureModalStyles() {
    if (document.getElementById(MODAL_STYLE_ID)) return;
    var s = document.createElement('style');
    s.id = MODAL_STYLE_ID;
    s.textContent = [
      /* z-index 99990: POR DEBAJO de .ilus-overlay (99999) para que
         ilusConfirm/ilusToast queden siempre encima del modal. */
      '.cubm-overlay{position:fixed;inset:0;background:rgba(10,10,10,.62);',
      '  backdrop-filter:blur(3px);display:flex;align-items:center;justify-content:center;',
      '  z-index:99990;padding:16px;opacity:0;transition:opacity .14s ease;}',
      '.cubm-overlay.show{opacity:1;}',
      '.cubm-modal{background:#fff;border-radius:14px;max-width:640px;width:100%;',
      '  max-height:92vh;display:flex;flex-direction:column;overflow:hidden;',
      '  border:2px solid #dc2626;box-shadow:0 24px 70px rgba(0,0,0,.4);',
      '  transform:translateY(10px) scale(.975);transition:transform .18s cubic-bezier(.2,.8,.2,1.05);}',
      '.cubm-overlay.show .cubm-modal{transform:none;}',
      '.cubm-head{background:linear-gradient(135deg,#0a0a0a 0%,#1f2937 100%);color:#fff;',
      '  padding:14px 16px;border-bottom:3px solid #dc2626;display:flex;align-items:flex-start;gap:10px;}',
      '.cubm-title{font-weight:800;font-size:1rem;line-height:1.2;}',
      '.cubm-sub{font-size:.78rem;color:#cbd5e1;margin-top:3px;word-break:break-word;}',
      '.cubm-sub .cubm-sku{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;color:#fff;font-weight:700;}',
      '.cubm-x{margin-left:auto;background:transparent;border:none;color:#9ca3af;font-size:1.05rem;',
      '  cursor:pointer;min-width:44px;min-height:44px;border-radius:8px;}',
      '.cubm-x:hover{color:#fff;background:rgba(255,255,255,.08);}',
      '.cubm-body{padding:14px 16px;overflow-y:auto;flex:1;background:#fff;color:#111827;}',
      '.cubm-hint{font-size:.76rem;color:#6b7280;margin-bottom:10px;}',
      '.cubm-row{border:1px solid #e5e7eb;border-radius:10px;padding:10px 12px;margin-bottom:10px;background:#fafafa;}',
      '.cubm-row.invalid{border-color:#dc2626;background:#fef2f2;}',
      '.cubm-row-head{display:flex;align-items:center;justify-content:space-between;margin-bottom:8px;}',
      '.cubm-row-title{font-size:.78rem;font-weight:800;text-transform:uppercase;letter-spacing:.05em;color:#0a0a0a;}',
      '.cubm-del{background:#fff;border:1px solid #fecaca;color:#dc2626;border-radius:8px;',
      '  min-width:44px;min-height:36px;cursor:pointer;}',
      '.cubm-del:hover{background:#fee2e2;}',
      '.cubm-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:8px;}',
      '.cubm-field{display:flex;flex-direction:column;gap:3px;}',
      '.cubm-field label{font-size:.66rem;font-weight:700;color:#6b7280;text-transform:uppercase;letter-spacing:.04em;}',
      '.cubm-field input{width:100%;padding:9px 10px;border:1.5px solid #d1d5db;border-radius:8px;',
      '  font-size:16px;background:#fff;color:#111827;outline:none;',
      '  transition:border-color .12s,box-shadow .12s;}',
      '.cubm-field input:focus{border-color:#dc2626;box-shadow:0 0 0 3px rgba(220,38,38,.15);}',
      '.cubm-row-calc{margin-top:8px;font-size:.74rem;color:#6b7280;display:flex;flex-wrap:wrap;gap:12px;}',
      '.cubm-row-calc b{color:#0a0a0a;}',
      '.cubm-add{width:100%;min-height:44px;border:1.5px dashed #dc2626;background:#fff;color:#dc2626;',
      '  border-radius:10px;font-weight:700;font-size:.85rem;cursor:pointer;}',
      '.cubm-add:hover{background:#fee2e2;}',
      '.cubm-totals{margin-top:14px;background:#0a0a0a;border-radius:10px;padding:12px;',
      '  display:grid;grid-template-columns:repeat(4,1fr);gap:6px;text-align:center;}',
      '.cubm-tot-lbl{font-size:.6rem;color:#9ca3af;text-transform:uppercase;letter-spacing:.06em;font-weight:700;}',
      '.cubm-tot-val{font-size:1.05rem;font-weight:800;color:#fff;line-height:1.2;}',
      '.cubm-tot-val.red{color:#dc2626;}',
      '.cubm-foot{display:flex;justify-content:flex-end;gap:8px;padding:12px 16px;',
      '  background:#fafafa;border-top:1px solid #e5e7eb;}',
      '.cubm-btn{min-height:44px;padding:8px 18px;border-radius:9px;font-weight:700;font-size:.86rem;',
      '  border:1px solid transparent;cursor:pointer;display:inline-flex;align-items:center;gap:6px;}',
      '.cubm-btn[disabled]{opacity:.6;cursor:not-allowed;}',
      '.cubm-btn-sec{background:#fff;color:#374151;border-color:#d1d5db;}',
      '.cubm-btn-sec:hover{background:#f3f4f6;}',
      '.cubm-btn-pri{background:#dc2626;color:#fff;border-color:#dc2626;}',
      '.cubm-btn-pri:hover{background:#b91c1c;}',
      /* Antes el disabled quedaba rojo desaturado (parecía activo pero
         "raro"). Estado disabled explícito, mismo criterio que el resto
         de la app (gris claro + texto gris). */
      '.cubm-btn-pri:disabled{background:#f3f4f6;color:#9ca3af;border-color:#e5e7eb;cursor:not-allowed;opacity:1;}',
      '.cubm-state{padding:26px 10px;text-align:center;color:#6b7280;font-size:.88rem;}',
      '.cubm-state .spin{display:inline-block;width:26px;height:26px;border:3px solid #e5e7eb;',
      '  border-top-color:#dc2626;border-radius:50%;animation:cubmspin .8s linear infinite;}',
      '@keyframes cubmspin{to{transform:rotate(360deg);}}',
      /* Mobile: pantalla completa (REGLA #3) */
      '@media (max-width:767.98px){',
      '  .cubm-overlay{padding:0;align-items:stretch;}',
      '  .cubm-modal{max-width:100%;width:100%;max-height:100%;height:100dvh;border-radius:0;border-width:0;',
      '    border-top:3px solid #dc2626;padding-bottom:env(safe-area-inset-bottom);}',
      '  .cubm-grid{grid-template-columns:repeat(2,1fr);}',
      '  .cubm-totals{grid-template-columns:repeat(2,1fr);gap:10px;}',
      '  .cubm-foot{position:sticky;bottom:0;}',
      '  .cubm-btn{flex:1;justify-content:center;}',
      /* Estado de error/carga centrado verticalmente: antes quedaba
         pegado arriba con un desierto blanco hasta el footer. */
      '  .cubm-body{display:flex;flex-direction:column;}',
      '  .cubm-state{display:flex;flex-direction:column;justify-content:center;flex:1;}',
      '}',
      '@media (prefers-reduced-motion:reduce){',
      '  .cubm-overlay,.cubm-modal{transition-duration:.01ms !important;}',
      '  .cubm-overlay.show .cubm-modal{transform:none !important;}',
      '}'
    ].join('\n');
    document.head.appendChild(s);
  }

  /* Estado del modal abierto (uno a la vez). */
  var M = null;

  function bultoCalc(b) {
    var vol = num(b.largo) * num(b.ancho) * num(b.alto);       // cm³
    return { vol: vol, pv: vol / (CFG.divisorVolumetrico || 4000), kg: num(b.peso) };
  }
  function bultoCompleto(b) {
    return num(b.largo) > 0 && num(b.ancho) > 0 && num(b.alto) > 0 && num(b.peso) > 0;
  }
  function bultoVacio(b) {
    return num(b.largo) === 0 && num(b.ancho) === 0 && num(b.alto) === 0 && num(b.peso) === 0;
  }
  function totalesDe(bultos) {
    var t = { n: 0, kg: 0, vol: 0, pv: 0, pred: 0 };
    (bultos || []).forEach(function (b) {
      if (!bultoCompleto(b)) return;
      var c = bultoCalc(b);
      t.n += 1; t.kg += c.kg; t.vol += c.vol; t.pv += c.pv;
    });
    t.kg = Math.round(t.kg * 10000) / 10000;
    t.pv = Math.round(t.pv * 10000) / 10000;
    t.vol = Math.round(t.vol * 100) / 100;
    t.pred = Math.max(t.kg, t.pv);
    return t;
  }

  function buildModalShell() {
    ensureModalStyles();
    var ov = document.createElement('div');
    ov.className = 'cubm-overlay';
    ov.setAttribute('role', 'dialog');
    ov.setAttribute('aria-modal', 'true');
    ov.setAttribute('aria-labelledby', 'cubmTitle');
    ov.innerHTML = [
      '<div class="cubm-modal">',
      '  <div class="cubm-head">',
      '    <div style="min-width:0">',
      '      <div class="cubm-title" id="cubmTitle"><i class="bi bi-rulers me-1"></i>Medidas del producto</div>',
      '      <div class="cubm-sub"><span class="cubm-sku">—</span> <span class="cubm-nombre"></span></div>',
      '    </div>',
      '    <button type="button" class="cubm-x" aria-label="Cerrar"><i class="bi bi-x-lg"></i></button>',
      '  </div>',
      '  <div class="cubm-body">',
      '    <div class="cubm-state cubm-loading"><div class="spin"></div><div style="margin-top:10px">Cargando medidas…</div></div>',
      '    <div class="cubm-state cubm-error" style="display:none;color:#dc2626"></div>',
      '    <div class="cubm-content" style="display:none">',
      '      <div class="cubm-hint"><i class="bi bi-info-circle me-1"></i>Medidas en <b>cm</b> y peso en <b>kg</b>, por bulto. El peso volumétrico se calcula L×A×H/' + (CFG.divisorVolumetrico || 4000) + '.</div>',
      '      <div class="cubm-rows"></div>',
      '      <button type="button" class="cubm-add"><i class="bi bi-plus-lg me-1"></i>Agregar bulto</button>',
      '      <div class="cubm-totals">',
      '        <div><div class="cubm-tot-lbl">Bultos</div><div class="cubm-tot-val" data-tot="n">0</div></div>',
      '        <div><div class="cubm-tot-lbl">Peso kg</div><div class="cubm-tot-val" data-tot="kg">0,0</div></div>',
      '        <div><div class="cubm-tot-lbl">Peso vol</div><div class="cubm-tot-val" data-tot="pv">0,0</div></div>',
      '        <div><div class="cubm-tot-lbl">Predominante</div><div class="cubm-tot-val red" data-tot="pred">0,0</div></div>',
      '      </div>',
      '    </div>',
      '  </div>',
      '  <div class="cubm-foot">',
      '    <button type="button" class="cubm-btn cubm-btn-sec cubm-cancel">Cancelar</button>',
      '    <button type="button" class="cubm-btn cubm-btn-pri cubm-save" disabled><i class="bi bi-save me-1"></i>Guardar medidas</button>',
      '  </div>',
      '</div>'
    ].join('\n');
    document.body.appendChild(ov);
    requestAnimationFrame(function () { ov.classList.add('show'); });
    return ov;
  }

  function renderRows() {
    if (!M) return;
    var wrap = $('.cubm-rows', M.overlay);
    if (!wrap) return;
    if (!M.bultos.length) {
      wrap.innerHTML = '<div class="cubm-state" style="padding:14px 6px">Sin bultos todavía. Agrega el primero.</div>';
    } else {
      wrap.innerHTML = M.bultos.map(function (b, i) {
        return [
          '<div class="cubm-row" data-idx="' + i + '">',
          '  <div class="cubm-row-head">',
          '    <span class="cubm-row-title">Bulto ' + (i + 1) + '</span>',
          '    <button type="button" class="cubm-del" data-del="' + i + '" aria-label="Quitar bulto ' + (i + 1) + '"><i class="bi bi-trash"></i></button>',
          '  </div>',
          '  <div class="cubm-grid">',
          field('Largo (cm)', 'largo', b.largo, i),
          field('Ancho (cm)', 'ancho', b.ancho, i),
          field('Alto (cm)', 'alto', b.alto, i),
          field('Peso (kg)', 'peso', b.peso, i),
          '  </div>',
          '  <div class="cubm-row-calc" data-calc="' + i + '"></div>',
          '</div>'
        ].join('\n');
      }).join('\n');
    }
    refreshCalcs();
  }

  function field(label, key, val, idx) {
    return [
      '<div class="cubm-field">',
      '  <label for="cubm-' + key + '-' + idx + '">' + esc(label) + '</label>',
      '  <input id="cubm-' + key + '-' + idx + '" type="number" inputmode="decimal" step="0.01" min="0"',
      '         data-f="' + key + '" data-idx="' + idx + '" value="' + esc(val === 0 || val ? val : '') + '">',
      '</div>'
    ].join('\n');
  }

  /* Recalcula las etiquetas por bulto + los totales del modal (en vivo). */
  function refreshCalcs() {
    if (!M) return;
    M.bultos.forEach(function (b, i) {
      var el = $('[data-calc="' + i + '"]', M.overlay);
      if (!el) return;
      var c = bultoCalc(b);
      el.innerHTML = 'Vol: <b>' + fm3(c.vol) + ' m³</b> · Peso vol: <b>' + fkg(c.pv) + ' kg</b>'
        + (bultoCompleto(b) ? '' : ' · <span style="color:#f59e0b">incompleto</span>');
      var row = $('.cubm-row[data-idx="' + i + '"]', M.overlay);
      if (row) row.classList.toggle('invalid', !bultoCompleto(b) && !bultoVacio(b));
    });
    var t = totalesDe(M.bultos);
    var set = function (k, v) { var el = $('[data-tot="' + k + '"]', M.overlay); if (el) el.textContent = v; };
    set('n', String(t.n));
    set('kg', fkg(t.kg));
    set('pv', fkg(t.pv));
    set('pred', fkg(t.pred));
    var save = $('.cubm-save', M.overlay);
    // Se permite guardar 0 bultos (= borrar medidas), pero nunca reactivamos
    // el botón mientras hay un guardado en vuelo.
    if (save && !M.saving) save.disabled = false;
  }

  function closeModal(force) {
    if (!M) return Promise.resolve(true);
    var doClose = function () {
      var ov = M.overlay, prev = M.lastFocus;
      // Devolver el scroll del fondo tal como estaba (mobile fullscreen).
      try { document.body.style.overflow = M.prevOverflow || ''; } catch (e) {}
      M = null;
      if (ov) { ov.classList.remove('show'); setTimeout(function () { if (ov.parentNode) ov.parentNode.removeChild(ov); }, 160); }
      if (prev && typeof prev.focus === 'function') { try { prev.focus(); } catch (e) {} }
      return true;
    };
    if (force || !M.dirty) return Promise.resolve(doClose());
    return confirmar({
      title: 'Descartar cambios',
      message: 'Tienes medidas sin guardar para ' + (M.sku || 'este producto') + '.',
      sub: 'Si cierras ahora se pierden los cambios.',
      okLabel: 'Descartar', cancelLabel: 'Seguir editando', danger: true,
    }).then(function (ok) { return ok ? doClose() : false; });
  }

  /**
   * Abre el modal de medidas de un producto.
   * Global: lo llama el botón "Medidas" de la fila.
   * @param {number|string|null} pid  product_id local (l.app_id). Puede ser null.
   * @param {string} sku
   * @param {object} [opts] {nombre, row}   row = elemento .cub-row de referencia
   * @returns {Promise<boolean>} true si el modal se abrió (false si faltaba
   *          el SKU o ya había otro modal abierto). El guardado es posterior
   *          y notifica por ilusToast + actualiza la fila en el DOM.
   */
  function cubAbrirMedidas(pid, sku, opts) {
    opts = opts || {};
    sku = String(sku || '').trim();
    if (!sku) { toast('No pude identificar el SKU de esta línea.', 'error'); return Promise.resolve(false); }
    if (M) return Promise.resolve(false);          // Ya hay uno abierto.

    ensureModalStyles();
    var overlay = buildModalShell();
    M = {
      overlay: overlay,
      pid: (pid === 0 || pid) ? String(pid) : null,
      sku: sku,
      nombre: opts.nombre || '',
      bultos: [],
      originales: 0,
      dirty: false,
      saving: false,
      saved: false,
      lastFocus: document.activeElement,
      prevOverflow: (document.body && document.body.style.overflow) || '',
    };
    // Bloquear el scroll del fondo mientras el modal está abierto.
    try { document.body.style.overflow = 'hidden'; } catch (e) {}

    var skuEl = $('.cubm-sku', overlay);
    if (skuEl) skuEl.textContent = sku;
    var nomEl = $('.cubm-nombre', overlay);
    if (nomEl) nomEl.textContent = M.nombre ? ('· ' + M.nombre) : '';

    wireModalEvents(overlay);

    // ── Precarga: NUNCA abrir vacío un producto que ya tiene bultos ──
    cargarMedidas();
    return Promise.resolve(true);
  }

  /* Extraído de cubAbrirMedidas() (2026-07-25) para poder reintentar sin
     cerrar el modal: el botón "↻ Reintentar" del estado de error llama a
     esta misma función. */
  function cargarMedidas() {
    if (!M) return;
    if (!M.pid) {
      // Sin ficha local todavía → no hay bultos que precargar.
      hydrate([], M.nombre);
      return;
    }
    var l = $('.cubm-loading', M.overlay); if (l) l.style.display = '';
    var e = $('.cubm-error', M.overlay); if (e) e.style.display = 'none';
    var c = $('.cubm-content', M.overlay); if (c) c.style.display = 'none';
    fetchJson(urlFor(CFG.medidasTpl, M.pid), { headers: { 'Accept': 'application/json' }, credentials: 'same-origin' })
      .then(function (r) {
        if (!M) return;                            // Cerraron mientras cargaba.
        if (r.status === 403) { showError('No tienes permiso para ver las medidas de este producto.'); return; }
        if (r.status === 404 || !r.data) {
          // El endpoint aún no existe o el producto no está: arrancamos limpio.
          hydrate([], (r.data && (r.data.nombre || (r.data.producto && r.data.producto.nombre))) || M.nombre);
          return;
        }
        if (!r.ok && r.data && r.data.error) { showError(String(r.data.error)); return; }
        var d = r.data || {};
        var prod = d.producto || d.product || d;
        hydrate(normalizeBultos(d), prod.nombre || prod.nombre_app || M.nombre);
      })
      .catch(function () {
        if (!M) return;
        showError('No pude cargar las medidas (problema de red). Puedes reintentar o cargarlas a mano.');
      });
  }

  function normalizeBultos(d) {
    var arr = (d && (d.bultos
      || (d.producto && d.producto.bultos)
      || (d.product && d.product.bultos)
      || (d.data && d.data.bultos))) || [];
    if (!Array.isArray(arr)) return [];
    return arr.map(function (b) {
      b = b || {};
      return {
        largo: b.largo != null ? b.largo : (b.length != null ? b.length : ''),
        ancho: b.ancho != null ? b.ancho : (b.width != null ? b.width : ''),
        alto: b.alto != null ? b.alto : (b.height != null ? b.height : ''),
        peso: b.peso != null ? b.peso : (b.weight != null ? b.weight : ''),
      };
    });
  }

  function hydrate(bultos, nombre) {
    if (!M) return;
    M.bultos = (bultos && bultos.length) ? bultos : [{ largo: '', ancho: '', alto: '', peso: '' }];
    M.originales = (bultos || []).filter(bultoCompleto).length;
    if (nombre) {
      M.nombre = nombre;
      var nomEl = $('.cubm-nombre', M.overlay);
      if (nomEl) nomEl.textContent = '· ' + nombre;
    }
    var l = $('.cubm-loading', M.overlay); if (l) l.style.display = 'none';
    var e = $('.cubm-error', M.overlay); if (e) e.style.display = 'none';
    var c = $('.cubm-content', M.overlay); if (c) c.style.display = '';
    renderRows();
    setTimeout(function () {
      var first = M && $('.cubm-row input', M.overlay);
      if (first) { try { first.focus(); first.select && first.select(); } catch (err) {} }
    }, 180);
  }

  function showError(msg) {
    if (!M) return;
    var l = $('.cubm-loading', M.overlay); if (l) l.style.display = 'none';
    var e = $('.cubm-error', M.overlay);
    if (e) {
      e.style.display = '';
      // El texto de error promete "reintentar": antes solo existía el botón
      // manual y la promesa quedaba incumplida (2026-07-25, revisión diseño).
      e.innerHTML = '<i class="bi bi-exclamation-triangle me-1"></i>' + esc(msg)
        + '<div style="margin-top:12px;display:flex;gap:8px;justify-content:center;flex-wrap:wrap">'
        + '<button type="button" class="cubm-btn cubm-btn-sec cubm-retry"><i class="bi bi-arrow-clockwise me-1"></i>Reintentar</button>'
        + '<button type="button" class="cubm-btn cubm-btn-sec cubm-manual">Cargar medidas a mano</button>'
        + '</div>';
      var b = $('.cubm-manual', e);
      if (b) b.addEventListener('click', function () { hydrate([], M ? M.nombre : ''); });
      var rt = $('.cubm-retry', e);
      if (rt) rt.addEventListener('click', function () { cargarMedidas(); });
    }
    var c = $('.cubm-content', M.overlay); if (c) c.style.display = 'none';
  }

  function wireModalEvents(overlay) {
    var body = $('.cubm-body', overlay);

    // Edición en vivo
    if (body) {
      body.addEventListener('input', function (ev) {
        var inp = ev.target;
        if (!inp || !inp.getAttribute || !inp.getAttribute('data-f')) return;
        if (!M) return;
        var idx = parseInt(inp.getAttribute('data-idx'), 10);
        var f = inp.getAttribute('data-f');
        if (isNaN(idx) || !M.bultos[idx]) return;
        M.bultos[idx][f] = inp.value;
        M.dirty = true;
        refreshCalcs();
      });

      // Quitar bulto
      body.addEventListener('click', function (ev) {
        var del = ev.target.closest && ev.target.closest('[data-del]');
        if (!del || !M) return;
        var idx = parseInt(del.getAttribute('data-del'), 10);
        if (isNaN(idx)) return;
        M.bultos.splice(idx, 1);
        M.dirty = true;
        renderRows();
      });
    }

    // Agregar bulto
    var add = $('.cubm-add', overlay);
    if (add) {
      add.addEventListener('click', function () {
        if (!M) return;
        M.bultos.push({ largo: '', ancho: '', alto: '', peso: '' });
        M.dirty = true;
        renderRows();
        var inputs = $$('.cubm-row input', overlay);
        var target = inputs[(M.bultos.length - 1) * 4];
        if (target) { try { target.focus(); } catch (e) {} }
      });
    }

    // Cerrar
    var x = $('.cubm-x', overlay); if (x) x.addEventListener('click', function () { closeModal(false); });
    var cancel = $('.cubm-cancel', overlay); if (cancel) cancel.addEventListener('click', function () { closeModal(false); });
    overlay.addEventListener('click', function (ev) { if (ev.target === overlay) closeModal(false); });
    overlay.addEventListener('keydown', function (ev) {
      if (ev.key === 'Escape') { ev.stopPropagation(); closeModal(false); return; }
      // Focus trap: el Tab no se escapa del modal.
      if (ev.key !== 'Tab') return;
      var foco = $$('button, input, [href], select, textarea', overlay).filter(function (el) {
        return !el.disabled && el.offsetParent !== null;
      });
      if (!foco.length) return;
      var first = foco[0], last = foco[foco.length - 1];
      if (ev.shiftKey && document.activeElement === first) { ev.preventDefault(); last.focus(); }
      else if (!ev.shiftKey && document.activeElement === last) { ev.preventDefault(); first.focus(); }
    });

    // Guardar
    var save = $('.cubm-save', overlay);
    if (save) save.addEventListener('click', function () { guardarMedidas(false); });
  }

  function setSaving(on) {
    if (!M) return;
    M.saving = !!on;
    var save = $('.cubm-save', M.overlay);
    if (!save) return;
    save.disabled = !!on;
    save.innerHTML = on
      ? '<span class="spin" style="width:14px;height:14px;border-width:2px;vertical-align:-2px"></span> Guardando…'
      : '<i class="bi bi-save me-1"></i>Guardar medidas';
  }

  function guardarMedidas(confirmarReduccion) {
    if (!M) return;
    // Ignoramos filas totalmente vacías; bloqueamos las a medio llenar.
    var limpios = M.bultos.filter(function (b) { return !bultoVacio(b); });
    var incompletos = limpios.filter(function (b) { return !bultoCompleto(b); });
    if (incompletos.length) {
      toast('Hay ' + incompletos.length + ' bulto(s) incompleto(s): largo, ancho, alto y peso deben ser mayores a 0.', 'warning');
      refreshCalcs();
      return;
    }

    var payloadBultos = limpios.map(function (b, i) {
      return {
        bulto_num: i + 1,
        largo: num(b.largo), ancho: num(b.ancho), alto: num(b.alto), peso: num(b.peso),
      };
    });

    // Sin product_id NO se usa el endpoint legacy: para eso existe el de
    // por-SKU, que resuelve/crea la ficha y mantiene las redes de seguridad
    // (nunca 0 bultos, 409 si reduce, audit log).
    var url = M.pid ? urlFor(CFG.medidasTpl, M.pid) : CFG.medidasPorSkuUrl;
    var payload = {
      sku: M.sku,
      nombre: M.nombre || '',
      bultos: payloadBultos,
      confirmar_reduccion: !!confirmarReduccion,
    };

    setSaving(true);
    fetchJson(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' },
      credentials: 'same-origin',
      body: JSON.stringify(payload),
    }).then(function (r) {
      if (!M) return;

      // El módulo nuevo no está desplegado (404/405). NO se reintenta solo
      // contra /transporte/api/inline-bulto: ese endpoint borra los bultos
      // existentes y reinserta lo que llegue, sin aviso ni vuelta atrás — que
      // es justo el accidente que este modal evita. Se pregunta primero.
      if (r.status === 404 || r.status === 405) {
        return guardarPorLegacy(payload, payloadBultos);
      }
      return handleSaveResponse(r, payloadBultos, false);
    }).catch(function () {
      setSaving(false);
      toast('Error de red al guardar las medidas. Intenta de nuevo.', 'error');
    });
  }

  /* Camino LEGACY (/transporte/api/inline-bulto). Se conserva porque es el
     único que queda vivo si el módulo nuevo no está registrado, pero exige un
     "sí" explícito: no tiene red de seguridad de ninguna clase. */
  function guardarPorLegacy(payload, payloadBultos) {
    var pendientes = (M && M.originales) || 0;
    return confirmar({
      title: 'Guardar por la vía antigua',
      message: 'El guardado seguro del Cubicador no está disponible en este servidor.',
      sub: 'La vía antigua REEMPLAZA todos los bultos de ' + (M ? M.sku : 'el producto') +
           (pendientes ? (' (hoy tiene ' + pendientes + ')') : '') +
           ' por los ' + payloadBultos.length + ' que estás guardando, sin confirmación ni ' +
           'forma de deshacerlo. ¿Continuamos?',
      subHtml: false,
      okLabel: 'Sí, guardar igual', cancelLabel: 'Cancelar', danger: true,
    }).then(function (ok) {
      if (!ok) {
        setSaving(false);
        toast('Guardado cancelado. No se modificó nada.', 'info');
        return;
      }
      setSaving(true);
      return fetchJson(CFG.inlineBultoUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' },
        credentials: 'same-origin',
        body: JSON.stringify(payload),
      }).then(function (r2) { return handleSaveResponse(r2, payloadBultos, true); });
    });
  }

  function handleSaveResponse(r, payloadBultos, viaFallback) {
    if (!M) return;
    setSaving(false);

    // ── 409: estás reduciendo la cantidad de bultos ──
    if (r.status === 409) {
      var d = r.data || {};
      var antes = d.bultos_actuales != null ? d.bultos_actuales
        : (d.actuales != null ? d.actuales : (d.total_actual != null ? d.total_actual : M.originales));
      var despues = d.bultos_nuevos != null ? d.bultos_nuevos
        : (d.nuevos != null ? d.nuevos : payloadBultos.length);
      return confirmar({
        title: 'Vas a reducir los bultos',
        message: 'El producto ' + M.sku + ' tiene ' + antes + ' bulto(s) registrados y quedaría con ' + despues + '.',
        sub: (d.mensaje || d.error || 'Se eliminarán los bultos sobrantes y sus medidas. Esta acción no se puede deshacer.'),
        okLabel: 'Sí, reducir a ' + despues,
        cancelLabel: 'Cancelar',
        danger: true,
      }).then(function (ok) {
        if (!ok) { toast('Guardado cancelado. No se modificó nada.', 'info'); return; }
        guardarMedidas(true);
      });
    }

    if (r.status === 403) { toast('No tienes permiso para editar medidas.', 'error'); return; }

    // ── Éxito SOLO con ok:true ─────────────────────────────────────────
    // Antes bastaba con que el cuerpo no fuera JSON parseable (data === null)
    // para caer en la rama de éxito: un redirect a /login, un flash+redirect
    // por falta de permiso o un 502 HTML de Cloud Run devolvían 200 con HTML,
    // y el modal decía "✓ Medidas guardadas" sin haber guardado nada — y
    // encima parcheaba la fila, los totales y el payload del Excel.
    if (r.data === null) {
      toast('No se pudo guardar: el servidor no respondió datos (puede haber expirado tu sesión). ' +
            'Recarga la página y vuelve a intentar.', 'error');
      return;
    }
    if (!r.ok || r.data.ok !== true || r.data.error) {
      toast('No se pudo guardar: ' + ((r.data.error || r.data.mensaje) || ('HTTP ' + r.status)), 'error');
      return;
    }

    // ── OK ──
    var t = totalesDe(payloadBultos);
    var srv = r.data || {};
    // Si el backend devuelve sus propios números, mandan los suyos.
    if (srv.peso_kg != null) t.kg = num(srv.peso_kg);
    if (srv.peso_vol != null) t.pv = num(srv.peso_vol);
    if (srv.vol_cm3 != null) t.vol = num(srv.vol_cm3);
    if (srv.total_bultos != null) t.n = parseInt(srv.total_bultos, 10) || t.n;
    t.pred = (srv.pred != null) ? num(srv.pred) : Math.max(t.kg, t.pv);

    // Si el producto se acaba de crear, el backend puede devolver el pid.
    var nuevoPid = srv.product_id || srv.pid || (srv.producto && srv.producto.id) || null;
    if (!M.pid && nuevoPid) M.pid = String(nuevoPid);

    var sku = M.sku, pid = M.pid;
    M.dirty = false;
    M.saved = true;

    aplicarMedidasEnFilas(sku, pid, t);
    cubicadorRefrescarTotales();
    actualizarPayloadExcel(sku, t);
    // Evento genérico para páginas con su PROPIA tabla/estado (ej.
    // templates/cubicador/asignar.html, que arma _docData.lineas en vez de
    // leer .cub-row del DOM) -- así se enteran del guardado sin que este
    // archivo tenga que conocerlas. Sin listener, es un no-op.
    try {
      document.dispatchEvent(new CustomEvent('cubicador:medidas-guardadas', {
        detail: { sku: sku, pid: pid, t: t },
      }));
    } catch (e) { /* CustomEvent no disponible: nunca romper el guardado */ }

    toast('✓ Medidas guardadas · ' + t.n + ' bulto(s) · ' + fkg(t.pred) + ' kg predominante', 'success');
    closeModal(true);
  }

  /* ══════════════════════════════════════════════════════════════════════
     3) BOTÓN ETIQUETAS
     ══════════════════════════════════════════════════════════════════════ */

  /**
   * Abre la pantalla de impresión de etiquetas del SKU en una pestaña nueva.
   * Si el producto no tiene medidas, avisa y ofrece abrir "Medidas".
   * @param {number|string|null} pid
   * @param {string} sku
   * @param {object} [opts] {bultos:Number, nombre:String, row:Element}
   */
  function cubAbrirEtiquetas(pid, sku, opts) {
    opts = opts || {};
    sku = String(sku || '').trim();
    var bultos = parseInt(opts.bultos, 10);
    if (isNaN(bultos)) bultos = 0;

    var puedeImprimir = (typeof global.CAN_PRINT === 'undefined') ? true : !!global.CAN_PRINT;
    if (!puedeImprimir) {
      toast('Tu rol no tiene el permiso de impresión de etiquetas.', 'warning');
      return;
    }

    if (pid && bultos > 0) {
      // window.open SÍNCRONO dentro del click → no lo bloquea el navegador.
      try { global.open(urlFor(CFG.printSetupTpl, pid), '_blank', 'noopener'); }
      catch (e) { toast('El navegador bloqueó la ventana emergente.', 'warning'); }
      return;
    }

    // Sin medidas → no abrimos una etiqueta vacía.
    toast('El SKU ' + sku + ' no tiene medidas cargadas: la etiqueta saldría vacía.', 'warning');
    confirmar({
      title: 'Faltan las medidas',
      message: 'Para imprimir etiquetas de ' + sku + ' primero hay que cargar los bultos (largo, ancho, alto y peso).',
      sub: '¿Abrimos ahora el editor de medidas?',
      okLabel: 'Abrir medidas', cancelLabel: 'Ahora no', type: 'warning',
    }).then(function (ok) {
      if (ok) cubAbrirMedidas(pid, sku, { nombre: opts.nombre, row: opts.row });
    });
  }

  /* ══════════════════════════════════════════════════════════════════════
     4) SINCRONIZACIÓN CON LA TABLA DEL CUBICADOR
     Reimplementamos el cálculo de totales porque el IIFE original
     (index.html) es un scope cerrado: recalcularTotales() no es global.
     Leemos y escribimos EXACTAMENTE los mismos data-* y marcadores.
     ══════════════════════════════════════════════════════════════════════ */

  function filasDe(sku) {
    return $$('.cub-row').filter(function (r) {
      return r.dataset && r.dataset.sku === sku && !r.classList.contains('removing');
    });
  }

  /* Busca una celda por marcador nuevo (data-cell) y, si no está, cae al
     índice de columna conocido de la tabla desktop. Todo bajo try/catch:
     si el layout cambia, se pierde el refresco visual pero nada revienta. */
  function celda(row, nombre, idxDesktop) {
    var byAttr = $('[data-cell="' + nombre + '"]', row);
    if (byAttr) return byAttr;
    if (row.tagName === 'TR' && idxDesktop != null) {
      var tds = row.children;
      if (tds && tds[idxDesktop]) return tds[idxDesktop];
    }
    return null;
  }

  /** Actualiza en el DOM las filas (desktop + mobile) de un SKU. */
  function aplicarMedidasEnFilas(sku, pid, t) {
    var rows = filasDe(sku);
    if (!rows.length) return;
    rows.forEach(function (row) {
      try {
        var qty = num(row.dataset.qty) || 0;
        // data-* que consume el recalculo de totales (mismos nombres de siempre)
        row.dataset.bultos = String(t.n);
        row.dataset.kgTot = String(Math.round(t.kg * qty * 10000) / 10000);
        row.dataset.pvTot = String(Math.round(t.pv * qty * 10000) / 10000);
        row.dataset.volTot = String(Math.round(t.vol * qty * 100) / 100);
        row.dataset.predTot = String(Math.round(t.pred * qty * 10000) / 10000);
        if (pid && !row.dataset.appId) row.dataset.appId = String(pid);

        var esTr = row.tagName === 'TR';
        if (esTr) {
          // ── Tabla desktop ──
          var cB = celda(row, 'bultos', 4);
          if (cB) {
            cB.innerHTML = t.n > 0
              ? '<span class="badge rounded-pill" style="background:var(--ilus-black,#0a0a0a)">' + t.n + '</span>'
              : '<span class="text-muted">—</span>';
          }
          var setNum = function (nom, idx, txt) {
            var c = celda(row, nom, idx);
            if (c) c.innerHTML = txt;
          };
          if (t.n > 0) {
            var kgMayor = t.kg >= t.pv;
            setNum('kg-u', 5, fkg(t.kg));
            setNum('pv-u', 6, fkg(t.pv));
            setNum('vol-u', 7, fm3(t.vol));
            setNum('pred-u', 8, '<span style="color:' + (kgMayor ? '#CC0000' : '#1a7a1a') + '">' + fkg(t.pred)
              + ' <sup style="font-size:.6rem">' + (kgMayor ? 'kg' : 'pv') + '</sup></span>');
            setNum('pred-tot', 9, fkg(t.pred * qty));
          } else {
            ['kg-u', 'pv-u', 'vol-u', 'pred-u', 'pred-tot'].forEach(function (nom, i) {
              setNum(nom, 5 + i, '<span class="text-muted">—</span>');
            });
          }
          // Aviso "Sin ficha" / "Sin bultos" del SKU
          var cSku = celda(row, 'sku', 1);
          if (cSku) {
            var hint = $('[data-cell="estado"]', cSku);
            if (!hint) {
              hint = $$('div', cSku).filter(function (d) {
                var tx = (d.textContent || '');
                return tx.indexOf('Sin ficha') !== -1 || tx.indexOf('Sin bultos') !== -1;
              })[0];
            }
            if (hint && t.n > 0) {
              hint.setAttribute('data-cell', 'estado');
              hint.innerHTML = '<i class="bi bi-check-circle-fill text-success me-1"></i>Medidas actualizadas';
            }
          }
        } else {
          // ── Card mobile ──
          var cells = $$('[data-cell]', row);
          var byName = {};
          cells.forEach(function (c) { byName[c.getAttribute('data-cell')] = c; });
          var vals = [fkg(t.kg * qty), fkg(t.pv * qty), fm3(t.vol * qty), fkg(t.pred * qty)];
          var names = ['kg-tot', 'pv-tot', 'vol-tot', 'pred-tot'];
          var fallback = $$('.row .col-3 .fw-bold', row);
          names.forEach(function (nm, i) {
            var el = byName[nm] || fallback[i];
            if (el) el.textContent = t.n > 0 ? vals[i] : '—';
          });
          // Badge "N bulto(s)" — si la línea no tenía bultos el badge no
          // existe en el HTML: lo creamos junto al de cantidad (AGREGAR, no
          // tocar el botón de eliminar ni el badge de cantidad).
          var badge = byName['bultos'] || $('.badge.bg-secondary', row);
          if (!badge && t.n > 0) {
            var delBtn = $('[data-action="del-row"]', row);
            var col = delBtn ? delBtn.parentNode : null;
            if (col) {
              badge = document.createElement('span');
              badge.className = 'badge rounded-pill bg-secondary';
              badge.setAttribute('data-cell', 'bultos');
              badge.style.fontSize = '.68rem';
              col.insertBefore(badge, delBtn);
            }
          }
          if (badge) {
            badge.textContent = t.n + ' bulto(s)';
            badge.style.display = t.n > 0 ? '' : 'none';
          }
        }
      } catch (e) { /* refresco cosmético: nunca romper la página */ }
    });
  }

  /**
   * Recalcula los totales de la página leyendo los data-* de las filas vivas.
   * Espejo exacto de recalcularTotales() del template (que es privado).
   */
  function cubicadorRefrescarTotales() {
    try {
      var tbody = document.getElementById('cubicadorTbody');
      var mobileWrap = document.getElementById('cubicadorMobileWrap');
      var vivas = function (root) {
        if (!root) return [];
        return $$('.cub-row', root).filter(function (r) { return !r.classList.contains('removing'); });
      };
      var desktop = vivas(tbody), mobile = vivas(mobileWrap);
      var src = desktop.length ? desktop : mobile;
      if (!src.length) return;

      var T = { qty: 0, bultos: 0, kg: 0, pv: 0, vol: 0, pred: 0 };
      src.forEach(function (r) {
        T.qty += num(r.dataset.qty);
        T.bultos += num(r.dataset.bultos);
        T.kg += num(r.dataset.kgTot);
        T.pv += num(r.dataset.pvTot);
        T.vol += num(r.dataset.volTot);
        T.pred += num(r.dataset.predTot);
      });

      [['data-foot'], ['data-foot-d'], ['data-foot-m']].forEach(function (pair) {
        var a = pair[0];
        $$('[' + a + '="qty"]').forEach(function (el) { el.textContent = Math.round(T.qty); });
        $$('[' + a + '="bultos"]').forEach(function (el) { el.textContent = Math.round(T.bultos); });
        $$('[' + a + '="kg"]').forEach(function (el) { el.textContent = fkg(T.kg); });
        $$('[' + a + '="pv"]').forEach(function (el) { el.textContent = fkg(T.pv); });
        $$('[' + a + '="vol"]').forEach(function (el) { el.textContent = fm3(T.vol); });
        $$('[' + a + '="pred"]').forEach(function (el) { el.textContent = fkg(T.pred); });
      });
    } catch (e) { /* no-op */ }
  }

  /**
   * Deja el payload del Excel al día con las medidas nuevas.
   * Escribe en dataset.original Y en value porque actualizarPayloadExcel()
   * del template regenera el value a partir de dataset.original al borrar
   * líneas: si sólo tocáramos value, el próximo borrado lo revertiría.
   */
  function actualizarPayloadExcel(sku, t) {
    var input = document.getElementById('excelPayloadInput');
    if (!input) return;
    try {
      if (!input.dataset.original) input.dataset.original = input.value;
      var patch = function (jsonStr) {
        var obj = JSON.parse(jsonStr);
        (obj.lineas || []).forEach(function (l) {
          if (String(l.sku || '') !== sku) return;
          var qty = num(l.cantidad);
          l.total_bultos = t.n;
          l.tiene_bultos = t.n > 0;
          l.tiene_ficha = true;
          l.peso_kg_u = t.kg;
          l.peso_vol_u = t.pv;
          l.vol_u = t.vol;
          l.pred_u = t.pred;
          l.peso_kg_tot = Math.round(t.kg * qty * 10000) / 10000;
          l.peso_vol_tot = Math.round(t.pv * qty * 10000) / 10000;
          l.vol_tot = Math.round(t.vol * qty * 100) / 100;
          l.pred_tot = Math.round(t.pred * qty * 10000) / 10000;
        });
        return JSON.stringify(obj);
      };
      input.dataset.original = patch(input.dataset.original);
      input.value = patch(input.value);
    } catch (e) { /* si falla, se exporta el payload como estaba */ }
  }

  /* ══════════════════════════════════════════════════════════════════════
     5) DELEGACIÓN DE EVENTOS DE LOS BOTONES NUEVOS
     Botones esperados (los agrega el template en la celda "Acciones"):
       <button data-action="cub-medidas">…</button>
       <button data-action="cub-etiquetas">…</button>
     Los datos se leen del propio botón (data-sku / data-app-id) y, si no
     están, de la fila padre .cub-row.
     ══════════════════════════════════════════════════════════════════════ */

  function datosDeFila(btn) {
    var row = btn.closest ? btn.closest('.cub-row') : null;
    var ds = (row && row.dataset) || {};
    var pid = btn.getAttribute('data-app-id') || ds.appId || '';
    var sku = btn.getAttribute('data-sku') || ds.sku || '';
    var nom = btn.getAttribute('data-nombre') || ds.nombre || '';
    var blt = btn.getAttribute('data-bultos');
    if (blt == null) blt = ds.bultos;
    return {
      row: row,
      pid: pid ? String(pid) : null,
      sku: String(sku || ''),
      nombre: String(nom || ''),
      bultos: parseInt(blt, 10) || 0,
    };
  }

  function onDocClick(ev) {
    var t = ev.target;
    if (!t || !t.closest) return;

    var bMed = t.closest('[data-action="cub-medidas"]');
    if (bMed) {
      ev.preventDefault();
      var d = datosDeFila(bMed);
      cubAbrirMedidas(d.pid, d.sku, { nombre: d.nombre, row: d.row });
      return;
    }
    var bEti = t.closest('[data-action="cub-etiquetas"]');
    if (bEti) {
      ev.preventDefault();
      var e = datosDeFila(bEti);
      cubAbrirEtiquetas(e.pid, e.sku, { bultos: e.bultos, nombre: e.nombre, row: e.row });
      return;
    }
  }

  /* ══════════════════════════════════════════════════════════════════════
     INIT
     ══════════════════════════════════════════════════════════════════════ */
  function init() {
    try { initTabs(); } catch (e) { try { console.warn('[cubicador_tabs] tabs:', e); } catch (_) {} }
    try { document.addEventListener('click', onDocClick); }
    catch (e) { try { console.warn('[cubicador_tabs] click:', e); } catch (_) {} }
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }

  /* ── API pública (la usa el HTML del cubicador) ── */
  global.cubSwitchTab = cubSwitchTab;
  global.cubAbrirMedidas = cubAbrirMedidas;
  global.cubAbrirEtiquetas = cubAbrirEtiquetas;
  global.cubicadorRefrescarTotales = cubicadorRefrescarTotales;
  global.CubicadorTabs = {
    switchTab: cubSwitchTab,
    abrirMedidas: cubAbrirMedidas,
    abrirEtiquetas: cubAbrirEtiquetas,
    refrescarTotales: cubicadorRefrescarTotales,
    cfg: CFG,
    version: '1.0.0',
  };

})(window);

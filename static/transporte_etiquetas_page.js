/* =====================================================================
   Etiquetas de transporte - logica de pagina (barcode, auto-fit del texto
   de la etiqueta, selector de bultos, descargas PDF/ZIP, editor de bultos).

   ORIGEN: bloque <script> inline de templates/transporte/etiquetas.html
   (movimiento literal 2026-07-28, sin refactor: mismas funciones, mismos
   nombres, mismo orden de ejecucion).

   POR QUE: la pagina se sirve con cache-control: no-store, asi que estos
   ~18KB de JS se re-descargaban en CADA carga. Como archivo estatico el
   navegador lo cachea (cache-busting por hash via @app.url_defaults --
   NUNCA agregar ?v= a mano).

   CONFIG: las 4 constantes que antes venian de Jinja ahora llegan por
   window.ETQ, que el template define inline JUSTO ANTES de cargar este
   archivo:
       window.ETQ = { pdfMode, manifestId, basePdf, baseZip }

   OJO: en el render PDF (Playwright usa page.set_content, baseURI =
   about:blank) este archivo NO se puede bajar por URL. El template lo
   embebe con static_inline('transporte_etiquetas_page.js'). Si renombras
   este archivo, actualiza AMBOS usos del template.
   ===================================================================== */

document.addEventListener("DOMContentLoaded", function () {
  function renderBarcode(barcode) {
    const code = (barcode.dataset.code || "").trim();
    if (!code) { barcode.dataset.rendered = "1"; return; }
    const wrap = barcode.closest(".barcode-wrap");
    const wrapWidth = wrap ? wrap.clientWidth : 280;
    // CODE128 ancho, con modulo minimo legible y zona silenciosa exterior.
    const estimatedModules = 11 * (Math.max(code.length, 4) + 3) + 13;
    const usableWidth = Math.max(wrapWidth * .94 - 24, 120);
    const lineWidth = Math.max(1.25, Math.min(4.2, usableWidth / estimatedModules));
    try {
      JsBarcode(barcode, code, {
        format: "CODE128",
        displayValue: false,
        lineColor: "#000",
        background: "#fff",
        width: lineWidth,
        height: 46,
        margin: 12
      });
    } catch (e) {
      // Código inválido: deja el SVG vacío sin romper la página.
    }
    barcode.dataset.rendered = "1";
  }
  document.querySelectorAll(".barcode").forEach(renderBarcode);

  const pxPerMm = 96 / 25.4;

  function hasOverflow(element) {
    return element.scrollHeight > element.clientHeight + 1
      || element.scrollWidth > element.clientWidth + 1;
  }

  function fitTextElement(element) {
    element.style.fontSize = "";
    const selector = element.dataset.fitWithin;
    const container = selector ? element.closest(selector) : element.parentElement;
    if (!container || element.clientWidth <= 0) return;

    let fontPx = parseFloat(window.getComputedStyle(element).fontSize) || 12;
    const minPx = (parseFloat(element.dataset.fitMinMm) || 1.8) * pxPerMm;
    const overflows = function () {
      const textIsTooWide = element.scrollWidth > element.clientWidth + 1;
      return textIsTooWide || hasOverflow(container);
    };

    for (let step = 0; step < 80 && overflows() && fontPx > minPx; step += 1) {
      fontPx = Math.max(minPx, fontPx - .45);
      element.style.fontSize = fontPx.toFixed(2) + "px";
    }
  }

  function fitCourierCell(cell) {
    const value = cell.querySelector(".js-fit-text");
    const content = cell.querySelector(".cr-content");
    if (!value || !content) return;

    cell.classList.remove("is-compact", "is-tight");
    fitTextElement(value);
    if (hasOverflow(content)) {
      cell.classList.add("is-compact");
      fitTextElement(value);
    }
    if (hasOverflow(content)) {
      cell.classList.add("is-tight");
      fitTextElement(value);
    }
  }

  function fitDestinationRoute(route) {
    const address = route.querySelector(".direccion");
    const commune = route.querySelector(".comuna");
    const region = route.querySelector(".region");
    if (!address || !commune || route.clientHeight <= 0) return;

    route.classList.remove("is-compact", "is-tight");
    let addressMm = 4.7;
    let communeMm = 5.5;
    let regionMm = 3;
    const applySizes = function () {
      address.style.fontSize = addressMm.toFixed(2) + "mm";
      commune.style.fontSize = communeMm.toFixed(2) + "mm";
      if (region) region.style.fontSize = regionMm.toFixed(2) + "mm";
    };

    const overflows = function () {
      return route.scrollHeight > route.clientHeight + 1
        || route.scrollWidth > route.clientWidth + 1;
    };

    applySizes();
    for (let step = 0; step < 36 && overflows(); step += 1) {
      if (step === 8) route.classList.add("is-compact");
      if (step === 20) route.classList.add("is-tight");
      addressMm = Math.max(2.75, addressMm - .11);
      communeMm = Math.max(3.2, communeMm - .1);
      regionMm = Math.max(2.05, regionMm - .05);
      applySizes();
    }
  }

  function fitRecipientInfo(info) {
    const client = info.querySelector(".v.big");
    const phone = info.querySelector(".dest-line .v");
    if (!client || info.clientHeight <= 0) return;

    info.classList.remove("is-compact", "is-tight");
    let clientMm = 6.2;
    let phoneMm = 4;
    const applySizes = function () {
      client.style.fontSize = clientMm.toFixed(2) + "mm";
      if (phone) phone.style.fontSize = phoneMm.toFixed(2) + "mm";
    };
    applySizes();
    for (let step = 0; step < 80 && info.scrollHeight > info.clientHeight + 1; step += 1) {
      if (step === 6) info.classList.add("is-compact");
      if (step === 22) info.classList.add("is-tight");
      clientMm = Math.max(1.9, clientMm - .11);
      phoneMm = Math.max(1.8, phoneMm - .05);
      applySizes();
    }
  }

  function fitPackageCount(main) {
    const current = main.querySelector(".b-x");
    const total = main.querySelector(".b-n");
    if (!current || !total || main.clientWidth <= 0) return;

    let numberMm = 13;
    current.style.fontSize = numberMm + "mm";
    total.style.fontSize = numberMm + "mm";
    for (let step = 0; step < 24 && hasOverflow(main); step += 1) {
      numberMm = Math.max(5.5, numberMm - .35);
      current.style.fontSize = numberMm + "mm";
      total.style.fontSize = numberMm + "mm";
    }
  }

  function fitLabelDetails() {
    document.querySelectorAll(".cr-cell").forEach(fitCourierCell);
    document.querySelectorAll(".dest-info").forEach(fitRecipientInfo);
    document.querySelectorAll(".dest-route").forEach(fitDestinationRoute);
    document.querySelectorAll(".b-main").forEach(fitPackageCount);
    document.querySelectorAll(".js-fit-text").forEach(fitTextElement);
    const watched = document.querySelectorAll(
      ".etiqueta, .lbl-courier, .cr-cell, .cr-content, .lbl-doc, .dest-info, " +
      ".dest-route, .b-main, .lbl-scan, .lbl-foot"
    );
    const layoutHasOverflow = Array.from(watched).some(function (element) {
      const overflowing = hasOverflow(element);
      element.dataset.layoutOverflow = overflowing ? "1" : "0";
      return overflowing;
    });
    document.documentElement.dataset.labelLayoutOverflow = layoutHasOverflow ? "1" : "0";
    document.documentElement.dataset.labelLayoutReady = layoutHasOverflow ? "0" : "1";
  }

  fitLabelDetails();
  requestAnimationFrame(fitLabelDetails);
  window.addEventListener("load", fitLabelDetails, { once: true });
  window.addEventListener("beforeprint", fitLabelDetails);
  let fitTimer = null;
  window.addEventListener("resize", function () {
    window.clearTimeout(fitTimer);
    fitTimer = window.setTimeout(fitLabelDetails, 80);
  });
  if (document.fonts && document.fonts.ready) {
    document.fonts.ready.then(fitLabelDetails);
  }
});

/* ══════════════════════════════════════════════════════════════════════
   SELECTOR ULTRAPOTENTE — filtrado, rangos, descarga PDF/ZIP
   ══════════════════════════════════════════════════════════════════════ */
(function() {
  // Config inyectada por el template (antes eran constantes Jinja inline).
  const ETQ = window.ETQ || {};
  const PDF_MODE = ETQ.pdfMode === true;
  if (PDF_MODE) return;   // En modo Playwright no inicializa nada interactivo.

  const MANIFEST_ID = ETQ.manifestId || 0;
  const TIPO    = MANIFEST_ID ? "manifiesto" : "factura";
  const BASE_PDF = ETQ.basePdf || "";
  const BASE_ZIP = ETQ.baseZip || "";
  const CSRF_TOKEN = document.querySelector('meta[name="csrf-token"]').content;

  const wraps   = Array.from(document.querySelectorAll(".et-wrap"));
  const cntEl   = document.getElementById("sp-cnt");
  const total   = wraps.length;

  function refresh() {
    let on = 0;
    wraps.forEach(w => {
      const ck = w.querySelector('input[type=checkbox]');
      if (ck && ck.checked) {
        w.classList.remove("is-off");
        on++;
      } else {
        w.classList.add("is-off");
      }
    });
    if (cntEl) cntEl.textContent = on;
    // Activar chip "Todas" si están todas marcadas
    document.querySelectorAll(".sp-chip[data-quick]").forEach(c => c.classList.remove("is-active"));
    if (on === total) {
      const allChip = document.querySelector('.sp-chip[data-quick="all"]');
      if (allChip) allChip.classList.add("is-active");
    } else if (on === 0) {
      const noneChip = document.querySelector('.sp-chip[data-quick="none"]');
      if (noneChip) noneChip.classList.add("is-active");
    }
  }

  function setAll(state) {
    wraps.forEach(w => {
      const ck = w.querySelector('input[type=checkbox]');
      if (ck) ck.checked = state;
    });
    refresh();
  }

  function setFactura(cid) {
    wraps.forEach(w => {
      const ck = w.querySelector('input[type=checkbox]');
      if (ck) ck.checked = (String(w.dataset.cid) === String(cid));
    });
    refresh();
  }

  function invert() {
    wraps.forEach(w => {
      const ck = w.querySelector('input[type=checkbox]');
      if (ck) ck.checked = !ck.checked;
    });
    refresh();
  }

  /**
   * Parser de rangos: "10589:1,3,5-8 ; 10590:2-4"
   * Devuelve {doc: [num,num,...], doc2: [num,...]}
   * Si una entrada no tiene ':doc', se aplica a TODAS las facturas que matchean ese rango.
   */
  function parseRango(spec) {
    if (!spec) return null;
    const out = {};
    spec.split(/[,;]/).forEach(part => {
      part = part.trim();
      if (!part) return;
      let doc = null, nums = part;
      if (part.includes(":")) {
        const ix = part.indexOf(":");
        doc = part.substring(0, ix).trim();
        nums = part.substring(ix + 1).trim();
      }
      // ahora nums puede ser "1" o "1-3" o "5-8"
      let lo, hi;
      if (nums.includes("-")) {
        const ab = nums.split("-");
        lo = parseInt(ab[0], 10); hi = parseInt(ab[1], 10);
      } else {
        lo = hi = parseInt(nums, 10);
      }
      if (isNaN(lo) || isNaN(hi)) return;
      if (lo > hi) { const t = lo; lo = hi; hi = t; }
      const key = doc || "*";
      if (!out[key]) out[key] = new Set();
      for (let n = lo; n <= hi; n++) out[key].add(n);
    });
    return out;
  }

  function aplicarRango() {
    const inp = document.getElementById("sp-rango");
    const spec = inp.value.trim();
    if (!spec) {
      if (typeof ilusToast !== "undefined") ilusToast("Ingresa un rango (ej: 10589:1,3,5-8)", { type: "warning" });
      return;
    }
    const parsed = parseRango(spec);
    if (!parsed || Object.keys(parsed).length === 0) {
      if (typeof ilusToast !== "undefined") ilusToast("Rango no válido. Formato: DOC:1,3,5-8", { type: "error" });
      return;
    }
    wraps.forEach(w => {
      const ck = w.querySelector('input[type=checkbox]');
      if (!ck) return;
      const doc = w.dataset.doc;
      const num = parseInt(w.dataset.num, 10);
      let on = false;
      if (parsed[doc] && parsed[doc].has(num)) on = true;
      else if (parsed["*"] && parsed["*"].has(num)) on = true;
      ck.checked = on;
    });
    refresh();
    if (typeof ilusToast !== "undefined") {
      const sel = wraps.filter(w => w.querySelector('input[type=checkbox]').checked).length;
      ilusToast("✓ " + sel + " etiqueta" + (sel === 1 ? "" : "s") + " coinciden", { type: "success" });
    }
  }

  /** Devuelve string 'cid:num,cid:num,...' de las etiquetas seleccionadas. */
  function getSeleccionParam() {
    const partes = [];
    wraps.forEach(w => {
      const ck = w.querySelector('input[type=checkbox]');
      if (ck && ck.checked) partes.push(w.dataset.cid + ":" + w.dataset.num);
    });
    return partes.join(",");
  }

  function descargar(individual) {
    const sel = getSeleccionParam();
    if (!sel) {
      ilusAlert({ title: "Sin selección", message: "Marca al menos una etiqueta antes de descargar.", type: "warning" });
      return;
    }
    if (!BASE_PDF) {
      ilusAlert({
        title: "PDF no disponible",
        message: "La ruta de descarga no está configurada para esta etiqueta.",
        type: "error"
      });
      return;
    }
    const btn = document.getElementById(individual ? "sp-zip" : "sp-pdf");
    if (btn) btn.classList.add("is-busy");
    const u = new URL(BASE_PDF, location.origin);
    u.searchParams.set("b", sel);
    if (individual) u.searchParams.set("individual", "1");
    // Forzar descarga (no inline) en modo individual; para PDF mostramos en tab
    if (individual) {
      const a = document.createElement("a");
      a.href = u.toString();
      a.download = "";
      a.style.display = "none";
      document.body.appendChild(a);
      a.click();
      a.remove();
    } else {
      window.open(u.toString(), "_blank", "noopener");
    }
    setTimeout(() => { if (btn) btn.classList.remove("is-busy"); }, 1500);
  }

  // Wire-up
  document.querySelectorAll(".sp-chip").forEach(chip => {
    chip.addEventListener("click", () => {
      const q = chip.dataset.quick;
      if (q === "all")        setAll(true);
      else if (q === "none")  setAll(false);
      else if (q === "invert") invert();
      else if (q === "factura") setFactura(chip.dataset.cid);
    });
  });

  document.getElementById("sp-aplicar").addEventListener("click", aplicarRango);
  document.getElementById("sp-rango").addEventListener("keydown", (ev) => {
    if (ev.key === "Enter") { ev.preventDefault(); aplicarRango(); }
  });
  document.getElementById("sp-pdf").addEventListener("click", () => descargar(false));
  document.getElementById("sp-zip").addEventListener("click", () => descargar(true));

  wraps.forEach(w => {
    const ck = w.querySelector('input[type=checkbox]');
    if (ck) ck.addEventListener("change", refresh);
  });

  refresh();

  // Atajo global para el botón "Imprimir (navegador)" del toolbar
  window.ilusImprimirSel = function() {
    // Las desmarcadas ya tienen .is-off (oculto en print). Solo lanza print.
    window.print();
  };

  /* ─────────────────────────────────────────────────────────────────
     Editor de bultos por factura
     - +/- ajusta el número, marca la card como "dirty"
     - Guardar → POST al endpoint, valida que no haya bultos escaneados,
       recarga la página para mostrar las nuevas etiquetas.
     ───────────────────────────────────────────────────────────────── */
  function markDirty(card) {
    const input = card.querySelector('.bf-input');
    const orig  = parseInt(input.dataset.original, 10);
    const now   = parseInt(input.value, 10);
    const saveBtn = card.querySelector('.bf-save');
    if (isNaN(now) || now < 1 || now > 999) {
      card.classList.remove('is-dirty');
      saveBtn.hidden = true;
      return;
    }
    if (now !== orig) {
      card.classList.add('is-dirty');
      saveBtn.hidden = false;
    } else {
      card.classList.remove('is-dirty');
      saveBtn.hidden = true;
    }
  }

  async function guardarBultos(card) {
    const cid      = card.dataset.cid;
    const doc      = card.dataset.doc;
    const input    = card.querySelector('.bf-input');
    const nuevo    = parseInt(input.value, 10);
    const original = parseInt(input.dataset.original, 10) || 1;
    if (isNaN(nuevo) || nuevo < 1) {
      if (typeof ilusToast !== "undefined")
        ilusToast("Ingresa un número válido (1-999)", { type: "error" });
      return;
    }

    // Confirmación DELIBERADA (Daniel 2026-07-25): corregir el total de
    // bultos cambia TODAS las etiquetas ya emitidas de esta factura (el "de
    // N" que se repite en cada una) — nunca se dispara sin que quede claro
    // el impacto. La confirmación usa siempre el helper visual de ILUS.
    if (typeof ilusConfirm === "undefined") {
      if (typeof ilusToast !== "undefined") {
        ilusToast("No se pudo cargar el diálogo de confirmación. Recarga la página e intenta de nuevo.",
                  { type: "error" });
      }
      return;
    }
    const confirmado = await ilusConfirm({
      title: "Corregir total de bultos",
      message: `${doc}: vas a cambiar de ${original} a ${nuevo} bulto${nuevo !== 1 ? 's' : ''}.`,
      sub: `Esto cambia TODAS las etiquetas de esta factura — de ahora en adelante todas van a decir `
         + `"de ${nuevo}" en vez de "de ${original}". Si ya imprimiste etiquetas con el número viejo, `
         + `tendrás que reimprimirlas.`,
      okLabel: "Sí, corregir",
      cancelLabel: "Cancelar",
      danger: true,
    });
    if (!confirmado) return;

    card.classList.add('is-saving');
    try {
      const r = await fetch(`/transporte/factura/${cid}/n-bultos`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-CSRF-Token": CSRF_TOKEN
        },
        body: JSON.stringify({
          n_bultos: nuevo,
          expected_n_bultos: original,
          manifest_id: MANIFEST_ID
        })
      });
      const j = await r.json();
      if (!r.ok || !j.ok) {
        const msg = j.error || "Error guardando";
        await ilusAlert({ title: "No se pudo actualizar", message: msg, type: "error" });
        return;
      }
      if (typeof ilusToast !== "undefined") {
        ilusToast(`✓ ${doc}: ${j.previo} → ${j.n_bultos} bultos · regenerando etiquetas...`,
                  { type: "success" });
      }
      // Si la factura ya tenía OT FedEx, la etiqueta FedEx quedó desfasada: avisar.
      if (j.fedex_desfasada && typeof ilusAlert !== "undefined") {
        await ilusAlert({
          title: "Etiqueta FedEx desfasada",
          message: "Esta factura ya tenía una OT FedEx creada. Al cambiar los bultos, "
                 + "la etiqueta FedEx quedó con el número viejo.",
          sub: "Entra al manifiesto y usa el botón naranja \"Re-emitir\" para cancelar la OT vieja "
             + "y generar una nueva con los bultos actuales (mismo día hasta las 16:00 Chile).",
          type: "warning",
        });
      }
      // Recargar la página para que las nuevas etiquetas se rendericen
      setTimeout(() => { window.location.reload(); }, 800);
    } catch (e) {
      await ilusAlert({ title: "Error de red", message: String(e), type: "error" });
    } finally {
      card.classList.remove('is-saving');
    }
  }

  document.querySelectorAll('.bf-card').forEach(card => {
    const input = card.querySelector('.bf-input');
    const minus = card.querySelector('.bf-step[data-step="-1"]');
    const plus  = card.querySelector('.bf-step[data-step="1"]');
    const save  = card.querySelector('.bf-save');

    minus.addEventListener('click', () => {
      const v = parseInt(input.value, 10) || 1;
      if (v > 1) { input.value = v - 1; markDirty(card); }
    });
    plus.addEventListener('click', () => {
      const v = parseInt(input.value, 10) || 1;
      if (v < 999) { input.value = v + 1; markDirty(card); }
    });
    input.addEventListener('input', () => markDirty(card));
    input.addEventListener('keydown', (ev) => {
      if (ev.key === 'Enter' && card.classList.contains('is-dirty')) {
        ev.preventDefault();
        guardarBultos(card);
      }
    });
    save.addEventListener('click', () => guardarBultos(card));
  });
})();

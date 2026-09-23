/* ═══════════════════════════════════════════════════════════════════════
   ILUS Fitness · Retiros · "Nuevo retiro interno" POTENTE (2026-09-15)
   ───────────────────────────────────────────────────────────────────────
   Complementa el modal #modalNuevoRetiroInterno de
   templates/retiros/internal_dashboard.html. Es 100% ADITIVO: no toca el
   flujo de siempre (el operador puede seguir llenando todo a mano); solo
   le pone encima el ERP Random como asistente:

     · Paso 1 — Documento: autobúsqueda al escribir el N°, buscador
       estándar (tickets/_tka_modal.html) y lista "Documentos del retiro".
     · Paso 2 — Cliente: RUT con formato/DV en vivo, ficha del cliente
       desde el ERP (/retiros/api/cliente/<rut>/ficha), documentos con
       saldo pendiente con checkbox + "Seleccionar/deseleccionar todo"
       (REGLA #14) y autocompletar por razón social.
     · Paso 5 — Carga: kg / m³ / bultos calculados desde las fichas
       logísticas de las líneas seleccionadas (el backend valida la
       capacidad del bloque con esos números).
     · Crear = asociar de verdad: tras POST /retiros/nuevo, cada documento
       de window._nriDocs se asocia con POST /retiros/<id>/docs/agregar.

   Decisión de Daniel (2026-09-15, caso Jeremías): el EMAIL NUNCA se
   autocompleta — siempre va como chip "Detectado en ERP → usar". El
   titular del documento recibiría el correo de confirmación si no es la
   misma persona que retira.

   Todo lo que consulta al ERP pasa por endpoints ya existentes (REGLA
   #4.1: solo lectura). Nada de alert/confirm/prompt nativos (REGLA #1).
   ═══════════════════════════════════════════════════════════════════════ */
(function(){
  'use strict';

  /* ── Mapa de tipos: réplica JS de PICKUP_TIDO_INVERSO (pickups_module.py).
     Sirve para setear el <select name="document_type"> del Paso 1 a partir
     del TIDO que devuelve el ERP. NVI (factura electrónica de importación)
     se muestra como factura. ── */
  window.NRI_TIDO_INVERSO = window.NRI_TIDO_INVERSO || {
    FCV: 'factura', BLV: 'boleta', GDV: 'guia', GDP: 'guia', VD: 'nota_venta',
    NVV: 'nota_venta', WEB: 'pedido', NVI: 'factura',
  };
  var NRI_TIDO_INVERSO = window.NRI_TIDO_INVERSO;
  /* tipo del <select> → TIDOs del ERP que le corresponden (para filtrar
     candidatos de la autobúsqueda). */
  var NRI_TIPO_A_TIDOS = {
    factura: ['FCV', 'NVI'], boleta: ['BLV'], guia: ['GDV', 'GDP'],
    guia_despacho: ['GDV', 'GDP'], nota_venta: ['VD', 'NVV'], venta_directa: ['VD'],
    pedido: ['WEB'], cotizacion: ['COV'],
  };
  var NRI_TIDO_LABEL = {
    FCV: 'Factura', BLV: 'Boleta', GDV: 'Guía de despacho', VD: 'Nota de venta',
    NVV: 'Nota de venta', WEB: 'Pedido web', NVI: 'Factura (importación)', COV: 'Cotización',
  };
  var NRI_PLACEHOLDERS = [
    'cliente no informado por erp', 'consumidor final', 'boleta', 'particular',
    'cliente', 'sin nombre', 'n/a', '-', '—',
  ];

  var modalEl = document.getElementById('modalNuevoRetiroInterno');
  var form    = document.getElementById('formNuevoRetiroInterno');
  if (!modalEl || !form) return;

  /* ═══════════════════ Helpers genéricos ═══════════════════ */
  function $(id){ return document.getElementById(id); }
  function campo(name){ return form.querySelector('[name="' + name + '"]'); }
  function esc(s){
    return String(s == null ? '' : s).replace(/[<>&"']/g, function(c){
      return { '<':'&lt;', '>':'&gt;', '&':'&amp;', '"':'&quot;', "'":'&#39;' }[c];
    });
  }
  function debounce(fn, ms){
    var t = null;
    return function(){
      var args = arguments, self = this;
      clearTimeout(t);
      t = setTimeout(function(){ fn.apply(self, args); }, ms);
    };
  }
  function toast(msg, type){
    if (window.ilusToast) window.ilusToast(msg, { type: type || 'info' });
    else console.log('[nri]', msg);
  }
  function refreshSteps(){
    try { if (typeof window.nriRefreshSteps === 'function') window.nriRefreshSteps(); } catch(_){}
  }
  function esPlaceholder(nombre){
    var n = String(nombre || '').trim().toLowerCase();
    if (!n) return true;
    return NRI_PLACEHOLDERS.indexOf(n) !== -1;
  }
  function num(v){ var n = Number(v); return isFinite(n) ? n : 0; }
  function fmtKg(v){ return (Math.round(num(v) * 100) / 100).toLocaleString('es-CL', { minimumFractionDigits: 0, maximumFractionDigits: 2 }); }
  function fmtCLP(v){ return Math.round(num(v)).toLocaleString('es-CL'); }

  /* fetch que SIEMPRE devuelve algo legible. Si el servidor responde HTML
     (sesión expirada → página de login) r.json() falla: lo traducimos a un
     error entendible en vez de "Unexpected token '<'". */
  async function fetchJson(url, opts){
    opts = opts || {};
    opts.credentials = opts.credentials || 'same-origin';
    opts.headers = Object.assign({ 'X-Requested-With': 'XMLHttpRequest' }, opts.headers || {});
    var r = await fetch(url, opts);
    var d = null;
    try { d = await r.json(); }
    catch(_){
      if (r.status === 401 || r.status === 403 || r.redirected){
        throw new Error('Tu sesión expiró — vuelve a iniciar sesión y reintenta.');
      }
      throw new Error('Respuesta inválida del servidor (HTTP ' + r.status + ').');
    }
    return { r: r, d: d || {} };
  }
  function postJson(url, body){
    return fetchJson(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body || {}),
    });
  }

  /* ═══════════════════ RUT chileno (módulo 11) ═══════════════════
     Copiado de static/retiros_public_request.js (cleanRUT/calcDV/isValidRUT/
     formatRUT) para no cargar ese archivo en el dashboard. */
  function cleanRUT(rut){ return String(rut || '').replace(/[^0-9kK]/g, '').toUpperCase(); }
  function calcDV(numStr){
    var suma = 0, mul = 2;
    for (var i = numStr.length - 1; i >= 0; i--){
      suma += parseInt(numStr[i], 10) * mul;
      mul = mul === 7 ? 2 : mul + 1;
    }
    var r = 11 - (suma % 11);
    if (r === 11) return '0';
    if (r === 10) return 'K';
    return String(r);
  }
  function isValidRUT(rut){
    var c = cleanRUT(rut);
    if (c.length < 8 || c.length > 9) return false;
    return /^\d+$/.test(c.slice(0, -1)) && calcDV(c.slice(0, -1)) === c.slice(-1);
  }
  function formatRUTStr(rut){
    var c = cleanRUT(rut);
    if (c.length < 2) return c;
    var numStr = c.slice(0, -1), dv = c.slice(-1);
    var f = '', rev = numStr.split('').reverse().join('');
    for (var i = 0; i < rev.length; i++){
      f = rev[i] + f;
      if ((i + 1) % 3 === 0 && i !== rev.length - 1) f = '.' + f;
    }
    return f + '-' + dv;
  }
  /* ¿El texto trae el DV explícito? Sí cuando viene con guion, termina en K
     o tiene 9 caracteres (un cuerpo chileno tiene máximo 8 dígitos). Un
     string de 7-8 dígitos pelados es AMBIGUO: lo tipeado por una persona
     suele ser cuerpo+DV, lo que devuelve el ERP (MAEEN.RTEN, ENDO, campo
     `rut` de buscar-erp) es SIEMPRE el cuerpo sin DV. */
  function traeDV(raw){
    var s = String(raw || '').trim(), c = cleanRUT(s);
    return s.indexOf('-') !== -1 || /K$/.test(c) || c.length === 9;
  }
  /* RUT COMPLETO (lo que tipea el operador o ya viene con guion): valida el
     DV; si no cuadra devuelve ''. NO adivina cuerpos. */
  function rutCompleto(conDV){
    var c = cleanRUT(conDV);
    if (!c) return '';
    return isValidRUT(c) ? formatRUTStr(c) : '';
  }
  /* RUT DESDE EL ERP (FIX revisor B2): el valor es el CUERPO sin DV — se le
     calcula SIEMPRE el DV. Antes se probaba isValidRUT primero y 1 de cada
     11 cuerpos de 8 dígitos "validaba" como RUT ajeno ('76990018' →
     '7.699.001-8' en vez de '76.990.018-7'). Si el ERP igual mandó guion/K/9
     chars (resolver de cliente), se respeta como completo. */
  function rutDesdeCuerpoErp(cuerpoErp){
    var raw = String(cuerpoErp || '').trim(), c = cleanRUT(raw);
    if (!c) return '';
    if (traeDV(raw)) return rutCompleto(c);
    if (/^\d{6,8}$/.test(c)) return formatRUTStr(c + calcDV(c));
    return '';
  }
  /* Cuerpo sin DV para COMPARAR (FIX revisor B3): corta el último char solo
     si el texto trae DV; un cuerpo pelado del ERP se usa entero. `esErp`
     fuerza la lectura "cuerpo". */
  function rutClave(rut, esErp){
    var raw = String(rut || '').trim(), c = cleanRUT(raw);
    if (!c) return '';
    if (esErp && !traeDV(raw)) return c;
    if (traeDV(raw)) return c.slice(0, -1);
    /* FIX revisor 2: lo TIPEADO de 8 chars sin guion con DV válido es
       cuerpo(7)+DV — igual que asume isValidRUT — no un cuerpo de 8. Sin
       esto, '76990018' (tipeado) y '7.699.001-8' (formateado en blur) daban
       claves distintas y el semáforo nunca reconocía al cliente. */
    if (c.length === 8 && isValidRUT(c)) return c.slice(0, -1);
    return c;
  }
  /* Teléfono chileno: +56 9 XXXX XXXX (móvil) o +56 2 XXXX XXXX (fijo) =
     9 dígitos tras quitar el código de país. */
  function fonoChilenoOk(v){
    var d = String(v || '').replace(/\D/g, '');
    if (!d) return true;
    if (d.length === 11 && d.slice(0, 2) === '56') d = d.slice(2);
    if (d.length === 10 && d[0] === '0') d = d.slice(1);
    return /^[2-9]\d{8}$/.test(d);
  }
  function emailOk(v){
    var s = String(v || '').trim();
    if (!s) return true;
    return /^[^\s@]+@[^\s@]+\.[^\s@]{2,}$/.test(s);
  }

  /* ═══════════════════ Estado único ═══════════════════
     window._nriDocs: Map(key 'TIDO|nudo' → {tido, nudo_display, hdr, lineas,
     seleccion:{sku→cantidad_seleccionada}}). */
  window._nriDocs = window._nriDocs || new Map();
  var DOCS = window._nriDocs;
  var docCache   = new Map();   // key → PROMESA de /api/erp/documento (M5: dos pedidos del mismo doc comparten un solo fetch)
  var docLoading = new Set();   // keys con fetch en vuelo
  var docsGen = 0;              // sube en cada reset: descarta respuestas del ERP de un formulario ya descartado
  var dirty = { total_packages: false, total_weight_kg: false, total_volume_m3: false };
  var ultimoRutConsultado = '';
  var sugerenciasActuales = [];   // candidatos del Paso 1 (buscar-erp)
  var rutDocsActuales     = [];   // docs con saldo del Paso 2 (saldo-pendiente)
  /* Capa de inteligencia (I1-I4): lo que sabemos del cliente para el semáforo. */
  var fichaCliente   = null;   // {claveRut, razon_social} de la última ficha ERP exitosa
  var retirosActivos = null;   // {claveRut, lista:[{id,code,status,status_label,fecha,url}]}
  var mantenerDistinto = {};   // key doc → true cuando el operador dijo "Mantener" al aviso RUT distinto
  var ultimaEvaluacion = null; // resultado de nriEvaluarPasos() (lo usa nriPreCrear)

  function docKey(tido, nudo){
    return String(tido || '').toUpperCase().trim() + '|' + String(nudo || '').replace(/^0+/, '').trim();
  }
  function nudoLimpio(n){ return String(n || '').trim().replace(/^0+/, '') || '0'; }

  /* Cola de fetch al ERP: máximo 3 concurrentes (patrón CHUNK=3 de
     retiros_internal_detail.js — la pool de pymssql es chica). */
  var _enVuelo = 0, _cola = [];
  function conCupo(fn){
    return new Promise(function(resolve, reject){
      _cola.push(function(){
        _enVuelo++;
        Promise.resolve().then(fn).then(resolve, reject).then(function(){
          _enVuelo--; _drenar();
        });
      });
      _drenar();
    });
  }
  function _drenar(){ while (_enVuelo < 3 && _cola.length) _cola.shift()(); }

  /* M5: la caché guarda la PROMESA, así dos llamadas simultáneas del mismo
     documento (checkbox del Paso 2 + "Usar este documento" del Paso 1)
     comparten un único fetch. Si falla, se saca de la caché para reintentar. */
  function fetchDocumento(tido, nudo){
    var key = docKey(tido, nudo);
    if (docCache.has(key)) return docCache.get(key);
    var p = conCupo(function(){ return postJson('/api/erp/documento', { tido: tido, nudo: nudoLimpio(nudo) }); })
      .then(function(res){
        if (!res.r.ok || res.d.error){
          var e = new Error(res.d.error || (res.r.status === 404 ? 'Documento no encontrado en el ERP' : 'ERP no responde'));
          e.status = res.r.status;
          throw e;
        }
        return res.d;
      });
    p.catch(function(){ if (docCache.get(key) === p) docCache.delete(key); });
    docCache.set(key, p);
    return p;
  }

  /* ═══════════════════ PASO 1 — DOCUMENTO ═══════════════════
     2026-09-22 (bloqueante): selTipo/inpNumero pasan a ser "vitrina" readonly
     del ERP — los llena SOLO sincronizarPaso1DesdeDocs() desde DOCS, nunca el
     operador. inpBuscar (#nriDocBuscar) es el campo nuevo donde SÍ se tipea:
     dispara la autobúsqueda y reemplaza a inpNumero como disparador. */
  var selTipo   = campo('document_type');
  var inpNumero = campo('document_number');
  var inpBuscar = $('nriDocBuscar');

  function seleccionPorDefecto(lineas){
    var sel = {};
    (lineas || []).forEach(function(l){
      if (l.es_zz || l.es_descuento) return;
      var saldo = num(l.saldo);
      if (saldo > 0) sel[l.sku] = saldo;
    });
    return sel;
  }

  /* Agrega (o actualiza) un documento al estado. `seleccion` opcional
     {sku→cantidad}: viene del modal estándar; si no, se arma por defecto. */
  async function agregarDoc(tido, nudo, opts){
    opts = opts || {};
    tido = String(tido || '').toUpperCase().trim();
    var key = docKey(tido, nudo);
    if (!tido || !nudoLimpio(nudo)) return null;
    if (DOCS.has(key) && !opts.seleccion){
      if (!opts.silencioso) toast('El documento ' + tido + ' ' + nudoLimpio(nudo) + ' ya está en este retiro.', 'info');
      return DOCS.get(key);
    }
    docLoading.add(key);
    renderDocsSel();
    /* 2026-09-23 (revisión): si el modal se descarta mientras el ERP responde,
       la respuesta tardía NO debe volver a meter la factura (docsGen sube en
       el reset). */
    var gen = docsGen;
    try {
      var d = await fetchDocumento(tido, nudo);
      if (gen !== docsGen) return null;
      var lineas = d.lineas || [];
      var sel;
      if (opts.seleccion){
        sel = {};
        lineas.forEach(function(l){
          if (l.es_zz || l.es_descuento) return;
          if (Object.prototype.hasOwnProperty.call(opts.seleccion, l.sku)){
            var q = num(opts.seleccion[l.sku]);
            if (q > 0) sel[l.sku] = q;
          }
        });
        if (!Object.keys(sel).length) sel = seleccionPorDefecto(lineas);
      } else {
        sel = seleccionPorDefecto(lineas);
      }
      /* M3: qué SKUs marcó el operador "sin saldo" en el modal tka (línea
         que el ERP reporta entregada pero igual se retira). Viaja a
         /docs/agregar como marcada_sin_saldo por línea. */
      var sinSaldo = {};
      if (opts.sinSaldo){
        Object.keys(opts.sinSaldo).forEach(function(sku){ if (opts.sinSaldo[sku] && sel[sku] != null) sinSaldo[sku] = true; });
      }
      var entry = {
        tido: tido,
        nudo_display: nudoLimpio(nudo),
        hdr: d.hdr || {},
        lineas: lineas,
        seleccion: sel,
        sinSaldo: sinSaldo,
        meta: opts.meta || null,   // fila del buscador (fecha, tiene_saldo, ya_tiene_retiro, ya_tiene_retiro_code/id…)
      };
      DOCS.set(key, entry);
      docLoading.delete(key);
      alCambiarDocs();
      return entry;
    } catch(e){
      if (gen !== docsGen) return null;
      docLoading.delete(key);
      renderDocsSel();
      toast('No se pudo traer ' + tido + ' ' + nudoLimpio(nudo) + ': ' + (e.message || 'error'), 'error');
      return null;
    }
  }
  function quitarDoc(key){
    if (!DOCS.has(key)) return;
    DOCS.delete(key);
    alCambiarDocs();
  }
  window.nriAgregarDoc = agregarDoc;
  window.nriQuitarDoc  = quitarDoc;

  /* Todo lo que depende del conjunto de documentos se refresca acá. */
  function alCambiarDocs(){
    renderDocsSel();
    sincronizarPaso1DesdeDocs();
    proponerClienteDesdeDocs();
    recalcularCarga();
    sincronizarChecksRutDocs();
    marcarSugerencias();
    refreshSteps();
  }

  /* 2026-09-22 (bloqueante): selTipo/inpNumero son SOLO "vitrina" del ERP —
     ya no hay tecleo manual que respetar (readonly en el template), así que
     esta función simplemente refleja el documento PRINCIPAL de DOCS, sin la
     lógica de divergencia "asistenteEscribio" que existía cuando el operador
     podía escribir tipo/número a mano. */
  function sincronizarPaso1DesdeDocs(){
    var badge = $('nriDocBadge');
    var n = DOCS.size;
    if (badge){
      if (n){
        badge.style.display = '';
        badge.innerHTML = '<i class="bi bi-patch-check-fill"></i>' + n + ' documento' + (n === 1 ? '' : 's') + ' verificado' + (n === 1 ? '' : 's') + ' en ERP';
      } else {
        badge.style.display = 'none';
      }
    }
    if (!n){
      if (selTipo) selTipo.value = '';
      if (inpNumero) inpNumero.value = '';
      return;
    }
    /* pickup_requests guarda UN documento: va el PRIMERO con tipo "humano"
       (M4: NVI/COV no tienen opción en el <select>; si el primero es de
       esos, se prefiere el primer doc que sí mapee); el resto se asocia con
       /docs/agregar al crear. */
    var principal = null, tipo = '';
    DOCS.forEach(function(e){
      if (principal) return;
      var t = NRI_TIDO_INVERSO[e.tido] || '';
      if (t && selTipo && selTipo.querySelector('option[value="' + t + '"]')){ principal = e; tipo = t; }
    });
    if (!principal){
      principal = DOCS.values().next().value;
      tipo = '';   // sin tipo humano exacto: el <select> queda vacío, no se inventa
    }
    if (selTipo) selTipo.value = tipo;
    /* Sin tipo humano (NVI/COV) no se deja un número huérfano: la cabecera
       quedaría 'sin_documento' + N°; el doc igual se asocia con
       /docs/agregar al crear. */
    if (inpNumero) inpNumero.value = tipo ? principal.nudo_display : '';
  }

  /* Cliente del documento (nombre / RUT formateado / clave de comparación).
     hdr.cliente_rut y meta.rut vienen del ERP → lectura "cuerpo" (B2). */
  function docNombreCliente(e){
    var h = e.hdr || {}, m = e.meta || {};
    if (!esPlaceholder(h.cliente_nombre)) return h.cliente_nombre;
    if (!esPlaceholder(m.razon_social)) return m.razon_social;
    if (!esPlaceholder(m.cliente)) return m.cliente;
    return '';
  }
  function docRutCliente(e){
    var h = e.hdr || {}, m = e.meta || {};
    return rutDesdeCuerpoErp(h.cliente_rut || m.rut || '');
  }
  function docClaveRut(e){ return rutClave(docRutCliente(e)); }
  /* Badge candado "Ya tiene retiro" — con link a la ficha si el backend
     entrega ya_tiene_retiro_code / ya_tiene_retiro_id (I2a; contrato con el
     agente de backend, falla suave: sin esos campos solo el texto). */
  function retiroExistenteInfo(m){
    m = m || {};
    var code = m.ya_tiene_retiro_code || '', id = m.ya_tiene_retiro_id;
    var url = m.ya_tiene_retiro_url || (id ? '/retiros/' + encodeURIComponent(id) : '');
    return { code: code, id: id, url: url };
  }
  function pillYaRetiro(m){
    var info = retiroExistenteInfo(m);
    var txt = info.code ? 'Ya está en ' + esc(info.code) : 'Ya tiene retiro';
    if (info.url) return '<a class="nri-pill lock" href="' + esc(info.url) + '" target="_blank" rel="noopener" title="Abrir el retiro existente" style="text-decoration:none"><i class="bi bi-lock-fill"></i>' + txt + '</a>';
    return '<span class="nri-pill lock"><i class="bi bi-lock-fill"></i>' + txt + '</span>';
  }

  function lineasResumen(entry){
    var conSaldo = 0, kg = 0, total = 0;
    (entry.lineas || []).forEach(function(l){
      if (l.es_zz || l.es_descuento) return;
      total++;
      if (num(l.saldo) > 0) conSaldo++;
      var sel = num(entry.seleccion[l.sku]);
      if (sel > 0 && l.tiene_ficha && l.tiene_bultos){
        var cant = num(l.cantidad);
        kg += num(l.peso_kg_tot) * (cant > 0 ? sel / cant : 1);
      }
    });
    return { conSaldo: conSaldo, total: total, kg: kg, seleccionadas: Object.keys(entry.seleccion).length };
  }

  /* 2026-09-23: el botón grande del Paso 1 cambia según haya facturas —
     vacío: "Agregar factura o boleta" (grande, con pulso); con facturas:
     "Agregar otra factura" (compacto). Multi-documento a la vista. */
  function pintarBotonAgregarDoc(n){
    var btn = $('nriDocErpBtn'), t = $('nriDocErpBtnTxt'), sub = $('nriDocErpBtnSub');
    if (btn) btn.classList.toggle('is-otra', n > 0);
    if (t) t.textContent = n > 0 ? 'Agregar otra factura o boleta' : 'Agregar factura o boleta';
    if (sub) sub.textContent = n > 0
      ? 'Si el cliente retira varias, agrégalas todas aquí'
      : 'Toca aquí y búscala por su número en el ERP';
  }
  function renderDocsSel(){
    var wrap = $('nriDocsSelWrap'), cont = $('nriDocsSel'), cnt = $('nriDocsSelCount');
    pintarBotonAgregarDoc(DOCS.size);
    if (!wrap || !cont) return;
    var keys = Array.from(DOCS.keys());
    var cargando = Array.from(docLoading).filter(function(k){ return !DOCS.has(k); });
    if (!keys.length && !cargando.length){ wrap.style.display = 'none'; cont.innerHTML = ''; return; }
    wrap.style.display = '';
    if (cnt) cnt.textContent = keys.length;
    var html = keys.map(function(k){
      var e = DOCS.get(k), h = e.hdr || {}, m = e.meta || {};
      var r = lineasResumen(e);
      var nombre = docNombreCliente(e);
      var rut = docRutCliente(e);
      var yaRetiro = !!m.ya_tiene_retiro;
      var conSaldo = m.tiene_saldo != null ? !!m.tiene_saldo : r.conSaldo > 0;
      var badgeSaldo = conSaldo
        ? '<span class="nri-pill ok"><i class="bi bi-check-circle"></i>Con saldo</span>'
        : '<span class="nri-pill warn"><i class="bi bi-exclamation-triangle"></i>Sin saldo</span>';
      var badgeRetiro = yaRetiro ? pillYaRetiro(m) : '';
      return '<div class="nri-doc-card" data-key="' + esc(k) + '">' +
        '<div class="nri-doc-num">' + esc(e.tido) + ' ' + esc(e.nudo_display) + '<small>' + esc(NRI_TIDO_LABEL[e.tido] || '') + '</small></div>' +
        '<div class="nri-doc-meta">' +
          '<span class="ddate">' + esc(h.fecha || m.fecha || '') + (rut ? ' · RUT ' + esc(rut) : '') + '</span>' +
          '<span class="dcli">' + esc(nombre || 'Cliente no informado por ERP') + '</span>' +
          '<span class="dlin">' + r.seleccionadas + ' de ' + r.total + ' línea' + (r.total === 1 ? '' : 's') + ' seleccionada' + (r.seleccionadas === 1 ? '' : 's') + ' · ' + r.conSaldo + ' con saldo · ' + fmtKg(r.kg) + ' kg estimados</span>' +
        '</div>' +
        '<div class="nri-doc-totals">' + badgeSaldo + badgeRetiro + '</div>' +
        '<button type="button" class="nri-doc-quitar" data-quitar="' + esc(k) + '" title="Quitar del retiro"><i class="bi bi-x-lg"></i>Quitar</button>' +
      '</div>';
    }).join('');
    html += cargando.map(function(k){
      var p = k.split('|');
      return '<div class="nri-doc-card is-loading"><div class="nri-doc-num">' + esc(p[0]) + ' ' + esc(p[1]) + '</div>' +
        '<div class="nri-doc-meta"><span class="dcli"><span class="spinner-border spinner-border-sm me-1"></span>Consultando ERP Random…</span></div></div>';
    }).join('');
    cont.innerHTML = html;
  }
  $('nriDocsSel') && $('nriDocsSel').addEventListener('click', function(ev){
    var b = ev.target.closest('[data-quitar]');
    if (b) quitarDoc(b.getAttribute('data-quitar'));
  });

  /* ── Autobúsqueda al escribir en #nriDocBuscar (N° o RUT) — debounce 450 ms,
     ≥3 chars, secuencia anti-race, 1 request en vuelo con re-disparo si el
     texto cambió. 2026-09-22: inpBuscar reemplaza a inpNumero como
     disparador — inpNumero ahora es solo la "vitrina" readonly del ERP. ── */
  var busqSeq = 0, busqEnVuelo = false, busqPendiente = null;
  /* FIX revisor B1: el CSS tenía display:none y acá se "mostraba" con
     display:'' → nunca se veía. Ahora: inline-flex visible / none oculto. */
  function setDocStatus(html, tipo){
    var el = $('nriDocStatus'); if (!el) return;
    el.className = 'nri-doc-status' + (tipo ? ' is-' + tipo : '');
    el.innerHTML = html || '';
    el.style.display = html ? 'inline-flex' : 'none';
  }
  async function buscarPorNumero(q){
    if (busqEnVuelo){ busqPendiente = q; return; }
    var seq = ++busqSeq;
    busqEnVuelo = true;
    setDocStatus('<span class="spinner-border spinner-border-sm me-1"></span>Consultando ERP Random…', 'loading');
    try {
      var res = await postJson('/retiros/api/buscar-erp', { q: q });
      if (seq !== busqSeq) return;
      var d = res.d;
      if (!res.r.ok || d.sin_conexion){
        setDocStatus('<i class="bi bi-plug me-1"></i>El ERP no respondió — espera unos segundos y vuelve a escribir el número.', 'warn');
        renderSugerencias([]);
        return;
      }
      if (d.modo === 'rut'){
        /* 2026-09-22: en vez de pedirle al operador que retipee el RUT en el
           Paso 2, lo movemos solos (moverARutCliente ya existe más abajo —
           function declaration, hoisted) y disparamos ficha/saldo-pendiente. */
        setDocStatus('<i class="bi bi-info-circle me-1"></i>Eso es un RUT — se pasó al Paso 2.', 'info');
        renderSugerencias([]);
        moverARutCliente(q, 'búsqueda del Paso 1', function(){
          if (inpBuscar) inpBuscar.value = '';
          busqSeq++; setDocStatus(''); renderSugerencias([]);
        });
        return;
      }
      /* 2026-09-23: sin filtro por tipo — el tipo ya no lo elige el operador
         (es el del primer documento, oculto) y en un retiro multi-documento
         escondía boletas u otros tipos con el mismo número. */
      var filtrados = d.documentos || [];
      if (!filtrados.length){
        setDocStatus('<i class="bi bi-search me-1"></i>Sin documentos con ese número. Prueba con el botón "Agregar factura o boleta" de arriba.', 'info');
      } else {
        setDocStatus('<i class="bi bi-check2-circle me-1"></i>' + filtrados.length + ' documento' + (filtrados.length === 1 ? '' : 's') + ' encontrado' + (filtrados.length === 1 ? '' : 's') + ' en el ERP', 'ok');
      }
      renderSugerencias(filtrados);
    } catch(e){
      if (seq !== busqSeq) return;
      setDocStatus('<i class="bi bi-plug me-1"></i>El ERP no respondió — espera unos segundos y vuelve a escribir el número.', 'warn');
      renderSugerencias([]);
    } finally {
      busqEnVuelo = false;
      if (busqPendiente != null && busqPendiente !== q){ var p = busqPendiente; busqPendiente = null; buscarPorNumero(p); }
      else busqPendiente = null;
    }
  }
  var buscarPorNumeroDeb = debounce(function(){
    var q = (inpBuscar && inpBuscar.value || '').trim();
    if (q.length < 3){ busqSeq++; setDocStatus(''); renderSugerencias([]); return; }
    buscarPorNumero(q);
  }, 450);
  inpBuscar && inpBuscar.addEventListener('input', buscarPorNumeroDeb);

  /* 2026-09-23 (Daniel: "siempre tiene que haber una factura, una boleta o
     al menos una nota de venta"): se quitó la excepción "Continuar sin
     documento" del modal interno (con su "sí" explícito). */

  function renderSugerencias(docs){
    sugerenciasActuales = docs || [];
    var cont = $('nriDocSugerencias'); if (!cont) return;
    if (!sugerenciasActuales.length){ cont.innerHTML = ''; cont.style.display = 'none'; return; }
    cont.style.display = '';
    cont.innerHTML = sugerenciasActuales.map(function(doc, i){
      var k = docKey(doc.tido_display, doc.nudo_display);
      var ya = DOCS.has(k);
      var badgeSaldo = doc.tiene_saldo
        ? '<span class="nri-pill ok"><i class="bi bi-check-circle"></i>Con saldo</span>'
        : '<span class="nri-pill warn"><i class="bi bi-exclamation-triangle"></i>Sin saldo</span>';
      var badgeRetiro = doc.ya_tiene_retiro ? pillYaRetiro(doc) : '';
      return '<div class="nri-doc-card is-sug' + (ya ? ' is-already' : '') + '" data-key="' + esc(k) + '">' +
        '<div class="nri-doc-num">' + esc(doc.tido_display) + ' ' + esc(doc.nudo_display) + '<small>' + esc(NRI_TIDO_LABEL[doc.tido_display] || '') + '</small></div>' +
        '<div class="nri-doc-meta">' +
          '<span class="ddate">' + esc(doc.fecha || '') + (doc.rut ? ' · RUT ' + esc(rutDesdeCuerpoErp(doc.rut) || doc.rut) : '') + '</span>' +
          '<span class="dcli">' + esc(doc.razon_social || 'Cliente no informado por ERP') + '</span>' +
          '<span class="dlin">' + (doc.n_lineas || 0) + ' línea' + (doc.n_lineas === 1 ? '' : 's') + ' · $' + fmtCLP(doc.valor_total) + '</span>' +
        '</div>' +
        '<div class="nri-doc-totals">' + badgeSaldo + badgeRetiro + '</div>' +
        (ya
          ? '<button type="button" class="nri-erp-btn is-done" disabled><i class="bi bi-check"></i>En el retiro</button>'
          : '<button type="button" class="nri-erp-btn" data-usar="' + i + '"><i class="bi bi-plus-lg"></i>Usar este documento</button>') +
      '</div>';
    }).join('');
  }
  function marcarSugerencias(){ if (sugerenciasActuales.length) renderSugerencias(sugerenciasActuales); }
  $('nriDocSugerencias') && $('nriDocSugerencias').addEventListener('click', async function(ev){
    var b = ev.target.closest('[data-usar]');
    if (!b) return;
    var doc = sugerenciasActuales[parseInt(b.getAttribute('data-usar'), 10)];
    if (!doc) return;
    if (doc.ya_tiene_retiro){
      var infoYa = retiroExistenteInfo(doc);
      toast('Ojo: ' + doc.tido_display + ' ' + doc.nudo_display + ' ya está en ' + (infoYa.code || 'otro retiro') + '.', 'warning');
    }
    b.disabled = true; b.innerHTML = '<span class="spinner-border spinner-border-sm"></span>';
    var agregado = await agregarDoc(doc.tido_display, doc.nudo_display, { meta: doc });
    /* M1: si /api/erp/documento falló, agregarDoc devuelve null y ya mostró
       el toast — se re-renderizan las sugerencias para que el botón vuelva
       a "Usar este documento" en vez de quedar con el spinner. */
    if (!agregado) marcarSugerencias();
  });

  /* ── Modal estándar de búsqueda ERP (tickets/_tka_modal.html) ── */
  function abrirBuscadorErp(){
    if (typeof window.tkaOpen !== 'function'){
      toast('El buscador de documentos ERP no está disponible en esta pantalla.', 'error');
      return;
    }
    /* tkaClose() libera document.body.style.overflow sin saber que este
       modal Bootstrap sigue abierto detrás. Se observa el cierre de tka
       (clase is-open) para devolverle el scroll-lock a este modal (patrón
       de templates/ot2/_modal_crear.html). */
    var tkaEl = $('tkaModal');
    if (tkaEl && window.MutationObserver){
      var mo = new MutationObserver(function(){
        if (!tkaEl.classList.contains('is-open')){
          mo.disconnect();
          if (modalEl.classList.contains('show')) document.body.style.overflow = 'hidden';
        }
      });
      mo.observe(tkaEl, { attributes: true, attributeFilter: ['class'] });
    }
    var yaAgregados = Array.from(DOCS.values()).map(function(e){ return { tido: e.tido, nudo: e.nudo_display }; });
    window.tkaOpen({
      mode: 'seleccionar',
      /* 2026-09-23 (Daniel: "con número de documento y número de RUT, ambas
         modalidades"): la pestaña por RUT usa el buscador de Retiros
         (mismo motor, permiso de Retiros — no exige permiso de Tickets). */
      tabs: ['doc', 'cli'],
      cliEndpoint: '/retiros/api/buscar-erp',
      /* "Agregar OTRA factura": con el cliente ya conocido se abre directo en
         "Por RUT" con todas sus facturas (multi-documento en un paso). */
      rutPrefill: (DOCS.size && inpRut && isValidRUT(val(inpRut))) ? val(inpRut) : '',
      docsYaAgregados: yaAgregados,
      docsYaAgregadosLabel: 'Ya en este retiro',
      onSeleccionar: onSeleccionTka,
    });
    /* Pre-carga: si ya se escribió algo en el buscador del Paso 1, lo
       llevamos de una (patrón de templates/transporte/_modal_cotizacion_logistica.html).
       tkaOpen ya corrió _tkaResetEstado() de forma síncrona, así que
       seteamos después. 2026-09-22: la fuente es inpBuscar (#nriDocBuscar),
       no inpNumero (que ahora es solo la vitrina readonly del ERP). */
    var n = (inpBuscar && inpBuscar.value || '').trim();
    /* 2026-09-23: no precargar un número que YA está en el retiro — con el
       botón "Agregar otra factura" reabría la misma. Sin preselección de
       tipo: el tipo oculto es el del primer documento, no una elección. */
    var yaEsta = n && Array.from(DOCS.values()).some(function(e){ return e.nudo_display === nudoLimpio(n); });
    if (n && /^[0-9]+$/.test(n) && !yaEsta){
      var inpN = $('tkaDocNudo');
      if (inpN){ inpN.value = n; }
      if (typeof window.tkaBuscarPorDoc === 'function') setTimeout(function(){ try { window.tkaBuscarPorDoc(); } catch(_){} }, 60);
    }
  }
  $('nriDocErpBtn') && $('nriDocErpBtn').addEventListener('click', abrirBuscadorErp);

  /* items: [{tido, nudo, sku, nombre, qty, marcada_sin_saldo}]. Se agrupa
     por documento y para CADA doc se pide /api/erp/documento (el header
     que manda tka puede ser null o de otro documento — no se confía). */
  async function onSeleccionTka(items){
    if (!items || !items.length) return;
    var porDoc = {};
    items.forEach(function(it){
      if (!it.tido || !it.nudo) return;
      var k = docKey(it.tido, it.nudo);
      var g = porDoc[k] = porDoc[k] || { tido: String(it.tido).toUpperCase(), nudo: nudoLimpio(it.nudo), sel: {}, sinSaldo: {} };
      var q = num(it.qty);
      if (q > 0 && it.sku){
        g.sel[it.sku] = (g.sel[it.sku] || 0) + q;
        if (it.marcada_sin_saldo) g.sinSaldo[it.sku] = true;   // M3: viaja hasta /docs/agregar
      }
    });
    var grupos = Object.keys(porDoc).map(function(k){ return porDoc[k]; });
    if (!grupos.length){ toast('La selección no traía documentos del ERP.', 'warning'); return; }
    var repetidos = grupos.filter(function(g){ return DOCS.has(docKey(g.tido, g.nudo)); });
    if (repetidos.length) toast(repetidos.map(function(g){ return g.tido + ' ' + g.nudo; }).join(', ') + ' ya estaba en el retiro — se actualizó la selección de líneas.', 'info');
    var gen = docsGen;
    var res = await Promise.all(grupos.map(function(g){ return agregarDoc(g.tido, g.nudo, { seleccion: g.sel, sinSaldo: g.sinSaldo, silencioso: true }); }));
    if (gen !== docsGen) return;   // el modal se descartó mientras respondía el ERP
    var ok = res.filter(Boolean).length;
    if (ok) toast('✓ ' + ok + ' documento' + (ok === 1 ? '' : 's') + ' agregado' + (ok === 1 ? '' : 's') + ' al retiro', 'success');
  }

  /* ═══════════════════ PASO 2 — CLIENTE ═══════════════════
     2026-09-22: inpNombre/inpRut nacen readonly en el template (solo el ERP
     los llena — precargarOChip/proponerClienteDesdeDocs no cambian, siguen
     escribiendo por JS sin que el atributo readonly se los impida). El botón
     "Editar manualmente" (SIEMPRE visible) es la ÚNICA forma de destrabarlos. */
  var inpNombre = campo('customer_name');
  var inpRut    = campo('customer_rut');
  var inpFono   = campo('contact_phone');
  var inpEmail  = campo('contact_email');
  var clienteManual = false;

  function setCliStatus(html, tipo){   // B1: inline-flex visible / none oculto
    var el = $('nriCliStatus'); if (!el) return;
    el.className = 'nri-doc-status' + (tipo ? ' is-' + tipo : '');
    el.innerHTML = html || '';
    el.style.display = html ? 'inline-flex' : 'none';
  }

  /* Comuna/dirección del cliente — Daniel 2026-09-23: solo informativo,
     para que el operador considere si viene de una región lejana antes
     de agendar. Alimenta desde consultarFichaCliente() (lookup por RUT)
     y proponerClienteDesdeDocs() (al elegir un documento en Paso 1). */
  function pintarUbicacionCliente(comuna, direccion){
    var el = $('nriClienteUbicacion'); if (!el) return;
    comuna = String(comuna || '').trim();
    direccion = String(direccion || '').trim();
    if (!comuna && !direccion){ el.style.display = 'none'; el.innerHTML = ''; return; }
    var partes = [];
    if (direccion) partes.push(esc(direccion));
    if (comuna) partes.push(esc(comuna));
    el.innerHTML = '<i class="bi bi-geo-alt"></i>' + partes.join(', ');
    el.style.display = 'inline-flex';
  }

  function nriHabilitarEdicionClienteManual(){
    clienteManual = true;
    if (inpNombre) inpNombre.removeAttribute('readonly');
    if (inpRut) inpRut.removeAttribute('readonly');
    var btn = $('nriClienteManualBtn'), badge = $('nriClienteManualBadge');
    if (btn){ btn.classList.add('is-on'); btn.disabled = true; btn.innerHTML = '<i class="bi bi-pencil-fill"></i>Edición manual activada'; }
    if (badge) badge.style.display = '';
    if (inpNombre) try { inpNombre.focus(); } catch(_){}
    refreshSteps();
  }
  window.nriHabilitarEdicionClienteManual = nriHabilitarEdicionClienteManual;
  $('nriClienteManualBtn') && $('nriClienteManualBtn').addEventListener('click', nriHabilitarEdicionClienteManual);

  /* Chips "Detectado en ERP → usar". Dedup por campo+valor; al usarlo se
     escribe el campo y el chip desaparece. */
  var chips = new Map();   // key campo|valor → {campo, valor, origen}
  var CHIP_LABEL = { customer_name: 'Razón social', customer_rut: 'RUT', contact_phone: 'Teléfono', contact_email: 'Email' };
  function proponerChip(campoName, valor, origen){
    valor = String(valor || '').trim();
    if (!valor) return;
    var inp = campo(campoName); if (!inp) return;
    var actual = String(inp.value || '').trim();
    if (campoName === 'customer_rut'){
      if (rutClave(actual) === rutClave(valor)) return;
    } else if (actual.toLowerCase() === valor.toLowerCase()) return;
    chips.set(campoName + '|' + valor.toLowerCase(), { campo: campoName, valor: valor, origen: origen || 'ERP' });
    renderChips();
  }
  /* Precarga si el campo está vacío; si ya hay algo distinto, queda como
     chip. El email NUNCA se precarga (decisión de Daniel 2026-09-15). */
  function precargarOChip(campoName, valor, origen){
    valor = String(valor || '').trim();
    if (!valor) return false;
    var inp = campo(campoName); if (!inp) return false;
    if (campoName === 'contact_email'){ proponerChip(campoName, valor, origen); return false; }
    if (!String(inp.value || '').trim()){
      inp.value = campoName === 'customer_rut' ? (rutCompleto(valor) || valor) : valor;
      inp.classList.add('nri-precargado');
      setTimeout(function(){ inp.classList.remove('nri-precargado'); }, 1800);
      return true;
    }
    proponerChip(campoName, valor, origen);
    return false;
  }
  function renderChips(){
    var cont = $('nriCliChips'); if (!cont) return;
    // Descartar chips cuyo valor ya quedó en el campo
    Array.from(chips.entries()).forEach(function(kv){
      var c = kv[1], inp = campo(c.campo);
      if (!inp) return;
      var actual = String(inp.value || '').trim();
      var igual = c.campo === 'customer_rut' ? rutClave(actual) === rutClave(c.valor) : actual.toLowerCase() === c.valor.toLowerCase();
      if (igual) chips.delete(kv[0]);
    });
    if (!chips.size){ cont.innerHTML = ''; cont.style.display = 'none'; return; }
    cont.style.display = '';
    cont.innerHTML = Array.from(chips.entries()).map(function(kv){
      var c = kv[1];
      var val = c.campo === 'customer_rut' ? (rutCompleto(c.valor) || c.valor) : c.valor;
      return '<button type="button" class="nri-chip" data-chip="' + esc(kv[0]) + '" title="Reemplaza el campo ' + esc(CHIP_LABEL[c.campo] || c.campo) + '">' +
        '<i class="bi bi-cpu"></i><span class="lbl">' + esc(CHIP_LABEL[c.campo] || c.campo) + ' detectado en ERP' + (c.origen ? ' <em>(' + esc(c.origen) + ')</em>' : '') + '</span>' +
        '<span class="val">' + esc(val) + '</span><span class="usar">→ usar</span></button>';
    }).join('');
  }
  $('nriCliChips') && $('nriCliChips').addEventListener('click', function(ev){
    var b = ev.target.closest('[data-chip]'); if (!b) return;
    var c = chips.get(b.getAttribute('data-chip')); if (!c) return;
    var inp = campo(c.campo); if (!inp) return;
    inp.value = c.campo === 'customer_rut' ? (rutCompleto(c.valor) || c.valor) : c.valor;
    chips.delete(b.getAttribute('data-chip'));
    renderChips();
    refreshSteps();
    if (c.campo === 'customer_rut') consultarRutSiValido();
  });

  /* ── RUT: validación DV en vivo, formato al salir del campo (M6: formatear
     en cada tecla movía el cursor al final y no dejaba corregir al medio) ── */
  function pintarValidezRut(inp){
    if (!inp) return;
    inp.classList.remove('is-valid', 'is-invalid');
    var c = cleanRUT(inp.value);
    if (c.length >= 8) inp.classList.add(isValidRUT(c) ? 'is-valid' : 'is-invalid');
  }
  inpRut && inpRut.addEventListener('input', function(){
    pintarValidezRut(inpRut);
    var c = cleanRUT(inpRut.value);
    if (!c) setCliStatus('');
    consultarRutDeb();
  });
  inpRut && inpRut.addEventListener('blur', function(){
    var c = cleanRUT(inpRut.value);
    if (c.length >= 2) inpRut.value = formatRUTStr(inpRut.value);
    pintarValidezRut(inpRut);
    if (c && !isValidRUT(c)){
      inpRut.classList.add('is-invalid');
      setCliStatus('<i class="bi bi-exclamation-triangle me-1"></i>El dígito verificador no cuadra — revisa el RUT.', 'warn');
    }
    renderChips();   // un chip "RUT detectado" cuyo valor ya coincide se retira al formatear
    refreshSteps();
  });
  var consultarRutDeb = debounce(consultarRutSiValido, 400);

  function consultarRutSiValido(){
    var c = cleanRUT(inpRut ? inpRut.value : '');
    if (!isValidRUT(c)){
      if (!c) { ultimoRutConsultado = ''; fichaCliente = null; retirosActivos = null; renderRutDocs([]); setCliStatus(''); refreshSteps(); }
      return;
    }
    if (c === ultimoRutConsultado) return;
    ultimoRutConsultado = c;
    fichaCliente = null; retirosActivos = null;
    consultarFichaCliente(c);
    consultarSaldoCliente(c);
    consultarRetirosActivos(c);
  }

  /* ── I3: retiros ACTIVOS del cliente (GET /retiros/api/cliente/<rut>/retiros-activos).
     Contrato: 200 {ok, retiros:[{id, code, status, status_label, fecha, url}]}
     ([] si ninguno) · 400 RUT inválido. Lo construye el backend en paralelo:
     si aún no existe (404) o falla (500) se ignora en silencio. ── */
  var activosSeq = 0;
  async function consultarRetirosActivos(rutClean){
    var seq = ++activosSeq;
    try {
      var res = await fetchJson('/retiros/api/cliente/' + encodeURIComponent(formatRUTStr(rutClean)) + '/retiros-activos');
      if (seq !== activosSeq) return;
      if (!res.r.ok || !res.d.ok) return;   // falla suave
      var lista = Array.isArray(res.d.retiros) ? res.d.retiros : [];
      retirosActivos = { claveRut: rutClave(rutClean), lista: lista.map(function(r){
        return {
          id: r.id, code: r.code || ('#' + r.id), status: r.status || '',
          status_label: r.status_label || r.status || '', fecha: r.fecha || '',
          url: r.url || (r.id ? '/retiros/' + encodeURIComponent(r.id) : ''),
        };
      }) };
      refreshSteps();
    } catch(_){ /* falla suave */ }
  }

  var fichaSeq = 0;
  async function consultarFichaCliente(rutClean){
    var seq = ++fichaSeq;
    setCliStatus('<span class="spinner-border spinner-border-sm me-1"></span>Consultando ficha del cliente en ERP Random…', 'loading');
    try {
      /* B4: el RUT viaja CON guion (igual que saldo-pendiente). Sin guion, un
         RUT de 8 chars el backend lo lee como cuerpo de 8 dígitos → ficha de
         OTRO cliente. */
      var res = await fetchJson('/retiros/api/cliente/' + encodeURIComponent(formatRUTStr(rutClean)) + '/ficha');
      if (seq !== fichaSeq) return;
      var d = res.d;
      if (res.r.status === 404 || (!d.ok && /no encontrado/i.test(d.error || ''))){
        setCliStatus('<i class="bi bi-person-x me-1"></i>RUT sin ficha en ERP — completa a mano.', 'info');
        refreshSteps();
        return;
      }
      if (res.r.status === 503 || !res.r.ok || !d.ok){
        setCliStatus('<i class="bi bi-plug me-1"></i>' + esc(d.error || 'ERP no responde') + ' — completa a mano.', 'warn');
        refreshSteps();
        return;
      }
      var c = d.cliente || {};
      fichaCliente = { claveRut: rutClave(rutClean), razon_social: c.razon_social || '' };
      var precargados = [];
      if (!c.placeholder && c.razon_social && precargarOChip('customer_name', c.razon_social, 'ficha ERP')) precargados.push('razón social');
      if (c.telefono && precargarOChip('contact_phone', c.telefono, 'ficha ERP')) precargados.push('teléfono');
      if (c.email) proponerChip('contact_email', c.email, 'ficha ERP');
      var extra = [];
      if (c.comuna) extra.push(esc(c.comuna));
      if (c.giro) extra.push(esc(c.giro));
      setCliStatus('<i class="bi bi-person-check-fill me-1"></i>Ficha ERP: <strong>' + esc(c.razon_social || 'sin razón social') + '</strong>' +
        (extra.length ? ' · ' + extra.join(' · ') : '') +
        (precargados.length ? ' · precargado: ' + precargados.join(', ') : '') +
        (c.email ? ' · email disponible como chip' : ''), 'ok');
      pintarUbicacionCliente(c.comuna, c.direccion);
      refreshSteps();
    } catch(e){
      if (seq !== fichaSeq) return;
      setCliStatus('<i class="bi bi-plug me-1"></i>ERP no responde — completa a mano.', 'warn');
    }
  }

  /* ── Documentos con saldo pendiente del cliente (checkbox + toggle-all) ── */
  var saldoSeq = 0;
  async function consultarSaldoCliente(rutClean){
    var seq = ++saldoSeq;
    var wrap = $('nriRutDocsWrap'), cont = $('nriRutDocs');
    if (!wrap || !cont) return;
    wrap.style.display = '';
    setRutDocsHint('', '');
    cont.innerHTML = '<div class="nri-skel"><div></div><div></div></div><div class="nri-loading-text"><span class="spinner-border spinner-border-sm me-1"></span>Consultando documentos con saldo pendiente en ERP Random…</div>';
    try {
      var res = await fetchJson('/retiros/api/cliente/' + encodeURIComponent(formatRUTStr(rutClean)) + '/saldo-pendiente?dias=90&solo_con_saldo=1');
      if (seq !== saldoSeq) return;
      var d = res.d;
      if (d.hint) setRutDocsHint(d.hint, (d.resumen && d.resumen.con_saldo > 0) ? '' : 'is-warn');
      if (!res.r.ok || (d.error && !(d.docs || []).length)){
        cont.innerHTML = '<div class="nri-loading-text"><i class="bi bi-plug me-1"></i>' + esc(d.error || 'ERP no responde') + ' — puedes seguir a mano.</div>';
        rutDocsActuales = [];
        return;
      }
      renderRutDocs(d.docs || []);
    } catch(e){
      if (seq !== saldoSeq) return;
      cont.innerHTML = '<div class="nri-loading-text"><i class="bi bi-plug me-1"></i>ERP no responde — puedes seguir a mano.</div>';
      rutDocsActuales = [];
    }
  }
  function setRutDocsHint(msg, clase){
    var h = $('nriRutDocsHint'), m = $('nriRutDocsHintMsg');
    if (!h || !m) return;
    if (!msg){ h.style.display = 'none'; return; }
    h.className = 'nri-smart-hint ' + (clase || '');
    m.innerHTML = esc(msg);
    h.style.display = 'flex';
  }
  function renderRutDocs(docs){
    rutDocsActuales = docs || [];
    var wrap = $('nriRutDocsWrap'), cont = $('nriRutDocs'), btnAll = $('nriRutDocsToggleAll');
    if (!wrap || !cont) return;
    if (!rutDocsActuales.length){
      if (ultimoRutConsultado){
        cont.innerHTML = '<div class="nri-loading-text"><i class="bi bi-info-circle me-1"></i>Sin documentos con saldo pendiente en los últimos 90 días. Si tienes el N°, escríbelo en el Paso 1.</div>';
        if (btnAll) btnAll.style.display = 'none';
      } else {
        wrap.style.display = 'none'; cont.innerHTML = '';
      }
      return;
    }
    wrap.style.display = '';
    if (btnAll) btnAll.style.display = '';
    cont.innerHTML = rutDocsActuales.map(function(doc, i){
      var k = docKey(doc.tido_display, doc.nudo_display);
      var marcado = DOCS.has(k);
      var lock = !!doc.ya_tiene_retiro;
      return '<label class="nri-doc-card is-check' + (lock ? ' is-lock' : '') + (marcado ? ' is-on' : '') + '" data-key="' + esc(k) + '">' +
        '<input type="checkbox" class="form-check-input nri-rutdoc-chk" data-idx="' + i + '"' + (marcado ? ' checked' : '') + (lock ? ' data-lock="1"' : '') + '>' +
        '<div class="nri-doc-num">' + esc(doc.tido_display) + ' ' + esc(doc.nudo_display) + '<small>' + esc(NRI_TIDO_LABEL[doc.tido_display] || '') + '</small></div>' +
        '<div class="nri-doc-meta">' +
          '<span class="ddate">' + esc(doc.fecha || '') + '</span>' +
          '<span class="dcli">' + esc(doc.cliente || '') + '</span>' +
          '<span class="dlin">' + (doc.n_lineas || 0) + ' línea' + (doc.n_lineas === 1 ? '' : 's') + ' · $' + fmtCLP(doc.total) + '</span>' +
        '</div>' +
        '<div class="nri-doc-totals">' +
          (doc.tiene_saldo ? '<span class="nri-pill ok"><i class="bi bi-check-circle"></i>Con saldo</span>' : '<span class="nri-pill warn"><i class="bi bi-exclamation-triangle"></i>Sin saldo</span>') +
          (lock ? pillYaRetiro(doc) : '') +
        '</div>' +
      '</label>';
    }).join('');
  }
  $('nriRutDocs') && $('nriRutDocs').addEventListener('change', async function(ev){
    var chk = ev.target.closest('.nri-rutdoc-chk'); if (!chk) return;
    var doc = rutDocsActuales[parseInt(chk.getAttribute('data-idx'), 10)]; if (!doc) return;
    var k = docKey(doc.tido_display, doc.nudo_display);
    var card = chk.closest('.nri-doc-card');
    if (chk.checked){
      if (doc.ya_tiene_retiro){
        var infoYa2 = retiroExistenteInfo(doc);
        toast('Ojo: ' + doc.tido_display + ' ' + doc.nudo_display + ' ya está en ' + (infoYa2.code || 'otro retiro') + '. Se agrega igual porque lo marcaste tú.', 'warning');
      }
      if (card) card.classList.add('is-on');
      var e = await agregarDoc(doc.tido_display, doc.nudo_display, { meta: doc, silencioso: true });
      if (!e){ chk.checked = false; if (card) card.classList.remove('is-on'); }
    } else {
      if (card) card.classList.remove('is-on');
      quitarDoc(k);
    }
  });
  function sincronizarChecksRutDocs(){
    var cont = $('nriRutDocs'); if (!cont) return;
    cont.querySelectorAll('.nri-doc-card.is-check').forEach(function(card){
      var on = DOCS.has(card.getAttribute('data-key'));
      var chk = card.querySelector('.nri-rutdoc-chk');
      if (chk) chk.checked = on;
      card.classList.toggle('is-on', on);
    });
  }
  /* REGLA #14 — toggle único: si ALGUNO (no bloqueado) está sin marcar,
     marca todos; si ya están todos, los desmarca. Los "Ya tiene retiro"
     quedan fuera del marcar-todo a propósito (variante tkaToggleAllDoc);
     se marcan uno a uno con aviso. Reutiliza el flujo por fila (dispara
     'change' en cada checkbox) para no duplicar lógica. */
  $('nriRutDocsToggleAll') && $('nriRutDocsToggleAll').addEventListener('click', function(){
    var chks = Array.from(document.querySelectorAll('#nriRutDocs .nri-rutdoc-chk:not([data-lock])'));
    if (!chks.length){ toast('Todos los documentos de la lista ya tienen retiro — márcalos uno a uno si igual corresponde.', 'info'); return; }
    var marcar = chks.some(function(c){ return !c.checked; });
    chks.forEach(function(c){
      if (c.checked === marcar) return;
      c.checked = marcar;
      c.dispatchEvent(new Event('change', { bubbles: true }));
    });
  });

  /* ── Autocompletar por razón social (dropdown .nri-ac) ── */
  var acBox = $('nriNombreAc');
  var acSeq = 0, acItems = [], acHi = -1;
  var buscarNombreDeb = debounce(async function(){
    var q = (inpNombre.value || '').trim();
    if (q.length < 3 || /^[0-9.\-kK\s]+$/.test(q)){ cerrarAc(); return; }
    var seq = ++acSeq;
    try {
      var res = await postJson('/retiros/api/buscar-erp', { q: q });
      if (seq !== acSeq) return;
      var d = res.d;
      if (!res.r.ok || d.sin_conexion || d.modo !== 'nombre'){ cerrarAc(); return; }
      /* B2/B3: doc.rut es el CUERPO sin DV (ENDO del ERP) → rutDesdeCuerpoErp
         y rutClave(…, true). Antes rutClave cortaba el último dígito del
         cuerpo, colapsaba clientes distintos y el contador daba siempre 0. */
      var vistos = {}, lista = [];
      (d.documentos || []).forEach(function(doc){
        var rk = rutClave(doc.rut, true);
        if (!rk || vistos[rk]) return;
        vistos[rk] = true;
        lista.push({ razon_social: doc.razon_social || '', rut: rutDesdeCuerpoErp(doc.rut) || doc.rut, clave: rk, n: 0 });
      });
      (d.documentos || []).forEach(function(doc){
        var rk = rutClave(doc.rut, true);
        lista.forEach(function(it){ if (it.clave === rk) it.n++; });
      });
      renderAc(lista.slice(0, 12));
    } catch(_){ if (seq === acSeq) cerrarAc(); }
  }, 350);
  function renderAc(lista){
    acItems = lista || []; acHi = -1;
    if (!acBox) return;
    if (!acItems.length){
      acBox.innerHTML = '<div class="nri-ac-empty">Sin coincidencias en el ERP — puedes escribir el nombre a mano.</div>';
      acBox.classList.add('open');
      return;
    }
    acBox.innerHTML = acItems.map(function(it, i){
      return '<div class="nri-ac-item" data-i="' + i + '">' +
        '<div class="nom">' + esc(it.razon_social || 'Cliente no informado por ERP') + '</div>' +
        '<div class="sub"><span class="rut">' + esc(it.rut) + '</span> · ' + it.n + ' documento' + (it.n === 1 ? '' : 's') + ' recientes</div>' +
      '</div>';
    }).join('');
    acBox.classList.add('open');
  }
  function cerrarAc(){ if (acBox){ acBox.classList.remove('open'); acBox.innerHTML = ''; } acItems = []; acHi = -1; }
  function elegirAc(i){
    var it = acItems[i]; if (!it) return;
    if (inpNombre && !esPlaceholder(it.razon_social)) inpNombre.value = it.razon_social;
    if (inpRut && it.rut){
      /* it.rut ya viene con DV calculado (rutDesdeCuerpoErp); si no pudo
         (cuerpo raro), se deja tal cual y el semáforo avisa. */
      inpRut.value = rutCompleto(it.rut) || it.rut;
      pintarValidezRut(inpRut);
    }
    cerrarAc();
    renderChips();
    refreshSteps();
    consultarRutSiValido();
  }
  if (inpNombre){
    inpNombre.setAttribute('autocomplete', 'off');
    inpNombre.addEventListener('input', buscarNombreDeb);
    inpNombre.addEventListener('keydown', function(ev){
      if (!acBox || !acBox.classList.contains('open')) return;
      if (ev.key === 'ArrowDown'){ ev.preventDefault(); acHi = Math.min(acItems.length - 1, acHi + 1); pintarHi(); }
      else if (ev.key === 'ArrowUp'){ ev.preventDefault(); acHi = Math.max(0, acHi - 1); pintarHi(); }
      else if (ev.key === 'Enter' && acHi >= 0){ ev.preventDefault(); elegirAc(acHi); }
      else if (ev.key === 'Escape'){ cerrarAc(); }
    });
    inpNombre.addEventListener('blur', function(){ setTimeout(cerrarAc, 180); });
  }
  function pintarHi(){
    if (!acBox) return;
    acBox.querySelectorAll('.nri-ac-item').forEach(function(el, i){ el.classList.toggle('hi', i === acHi); });
  }
  acBox && acBox.addEventListener('mousedown', function(ev){
    var it = ev.target.closest('.nri-ac-item'); if (!it) return;
    ev.preventDefault();
    elegirAc(parseInt(it.getAttribute('data-i'), 10));
  });

  /* ── Datos del cliente desde los documentos elegidos (punto 8) ── */
  function proponerClienteDesdeDocs(){
    if (!DOCS.size) return;
    var primero = DOCS.values().next().value;
    var h = primero.hdr || {};
    var origen = 'documento ' + primero.tido + ' ' + primero.nudo_display;
    var nombre = docNombreCliente(primero);
    var rut = docRutCliente(primero);   // B2: cuerpo del ERP → siempre cuerpo + DV calculado
    if (nombre) precargarOChip('customer_name', nombre, origen);
    if (rut) precargarOChip('customer_rut', rut, origen);
    if (h.telefono) precargarOChip('contact_phone', h.telefono, origen);
    /* Daniel 2026-09-23: "vamos a sustituir... correo" — a diferencia de
       consultarFichaCliente() (lookup solo por RUT, sigue siendo chip por
       la regla general de 2026-09-15), acá el operador ya ELIGIÓ un
       documento específico: el email SÍ se precarga igual que nombre/RUT/
       teléfono (si el campo está vacío; si ya hay algo distinto, chip). */
    if (h.email){
      var _emailVacio = inpEmail && !String(inpEmail.value || '').trim();
      if (_emailVacio){
        inpEmail.value = h.email;
        inpEmail.classList.add('nri-precargado');
        setTimeout(function(){ inpEmail.classList.remove('nri-precargado'); }, 1800);
      } else {
        proponerChip('contact_email', h.email, origen);
      }
    }
    pintarUbicacionCliente(h.comuna, h.direccion);
    renderChips();
    if (rut && inpRut && rutClave(inpRut.value) === rutClave(rut)){
      inpRut.classList.remove('is-invalid'); inpRut.classList.add('is-valid');
      consultarRutSiValido();
    }
  }

  /* ═══════════════════ PASO 5 — CARGA PRECARGADA ═══════════════════ */
  var inpBultos = campo('total_packages'), inpKg = campo('total_weight_kg'), inpM3 = campo('total_volume_m3');
  [['total_packages', inpBultos], ['total_weight_kg', inpKg], ['total_volume_m3', inpM3]].forEach(function(p){
    if (!p[1]) return;
    p[1].addEventListener('input', function(){ dirty[p[0]] = true; pintarCargaInfo(ultimoCalculo); });
  });
  var ultimoCalculo = null;

  function recalcularCarga(){
    if (!DOCS.size){
      // Sin documentos: los campos que el operador NO tocó vuelven a los
      // valores por defecto del form (1 bulto / 0 kg / 0 m³) para no dejar
      // colgado el último cálculo de un documento que ya quitó.
      if (ultimoCalculo){
        if (inpBultos && !dirty.total_packages) inpBultos.value = '1';
        if (inpKg && !dirty.total_weight_kg) inpKg.value = '0';
        if (inpM3 && !dirty.total_volume_m3) inpM3.value = '0';
      }
      ultimoCalculo = null; pintarCargaInfo(null); renderTablaProductosNRI(); return;
    }
    var kg = 0, m3 = 0, bultos = 0, N = 0, M = 0, sinFicha = [];
    DOCS.forEach(function(e){
      (e.lineas || []).forEach(function(l){
        if (l.es_zz || l.es_descuento) return;
        var sel = num(e.seleccion[l.sku]);
        if (sel <= 0) return;
        M++;
        if (l.tiene_ficha && l.tiene_bultos){
          N++;
          var cant = num(l.cantidad), ratio = cant > 0 ? sel / cant : 1;
          kg += num(l.peso_kg_tot) * ratio;
          m3 += (num(l.vol_tot) / 1e6) * ratio;
          bultos += num(l.total_bultos) * ratio;
        } else {
          sinFicha.push(l.nombre || l.descripcion_erp || l.sku);
        }
      });
    });
    var bultosFinal = bultos > 0 ? Math.max(1, Math.ceil(bultos - 1e-9)) : (N > 0 ? N : 0);
    ultimoCalculo = { kg: kg, m3: m3, bultos: bultosFinal, N: N, M: M, sinFicha: sinFicha };
    if (inpKg && !dirty.total_weight_kg) inpKg.value = (Math.round(kg * 100) / 100).toFixed(2);
    if (inpM3 && !dirty.total_volume_m3) inpM3.value = (Math.round(m3 * 1000) / 1000).toFixed(3);
    if (inpBultos && !dirty.total_packages && bultosFinal > 0) inpBultos.value = String(bultosFinal);
    pintarCargaInfo(ultimoCalculo);
    renderTablaProductosNRI();
  }
  /* 🆕 Daniel 2026-09-23: "una tabla con todos los datos de los productos,
     indicándome el peso y el volumen... bien potente que no necesiten de
     una hoja para poder actuar... agrégale el nro de documento". Tabla de
     solo lectura, SKU/N° doc/descripción/cantidad/peso/volumen — una fila
     por línea de cada documento asociado (excluye ZZ/descuento, mismo
     criterio que recalcularCarga). Peso/volumen de la fila = el TOTAL para
     la cantidad seleccionada (mismo ratio que el cálculo agregado de
     arriba), no el catálogo crudo — para que sumen exacto contra el total. */
  function renderTablaProductosNRI(){
    var wrap = $('nriTablaProductosWrap'), body = $('nriTablaProductosBody'), cnt = $('nriTablaProductosCount');
    if (!wrap || !body) return;
    var filas = [];
    DOCS.forEach(function(e){
      var docLbl = e.tido + ' ' + e.nudo_display;
      (e.lineas || []).forEach(function(l){
        if (l.es_zz || l.es_descuento) return;
        var sel = num(e.seleccion[l.sku]);
        if (sel <= 0) return;
        var cant = num(l.cantidad), ratio = cant > 0 ? sel / cant : 1;
        var pesoFila = num(l.peso_kg_tot) * ratio;
        var volFila = (num(l.vol_tot) / 1e6) * ratio;
        filas.push({
          sku: l.sku || '—', doc: docLbl,
          desc: l.nombre || l.descripcion_erp || '(sin descripción)',
          cant: sel, peso: pesoFila, vol: volFila,
        });
      });
    });
    if (!filas.length){ wrap.style.display = 'none'; body.innerHTML = ''; return; }
    wrap.style.display = '';
    if (cnt) cnt.textContent = filas.length;
    body.innerHTML = filas.map(function(f){
      return '<tr><td class="mono">' + esc(f.sku) + '</td>' +
        '<td><span class="nri-tp-doc">' + esc(f.doc) + '</span></td>' +
        '<td>' + esc(f.desc) + '</td>' +
        '<td class="num">' + (Math.round(f.cant * 100) / 100) + '</td>' +
        '<td class="num">' + fmtKg(f.peso) + ' kg</td>' +
        '<td class="num">' + (Math.round(f.vol * 1000) / 1000).toFixed(3) + ' m³</td></tr>';
    }).join('');
  }
  function pintarCargaInfo(c){
    var el = $('nriCargaInfo'); if (!el) return;
    if (!c){
      /* 2026-09-22: estado inicial VISIBLE (antes display:none silencioso) —
         el cálculo sigue siendo 100% automático, esto es solo visibilidad. */
      el.style.display = '';
      el.innerHTML = '<div class="nri-carga-line is-wait"><i class="bi bi-hourglass-split"></i>' +
        '<div>Esperando el documento del Paso 1 para calcular bultos, peso y volumen…</div></div>';
      return;
    }
    el.style.display = '';
    var editados = Object.keys(dirty).filter(function(k){ return dirty[k]; });
    var html = '<div class="nri-carga-line' + (c.N < c.M ? ' is-warn' : ' is-ok') + '">' +
      '<i class="bi ' + (c.N < c.M ? 'bi-exclamation-triangle-fill' : 'bi-box-seam-fill') + '"></i>' +
      '<div><strong>Calculado desde el ERP:</strong> ' + c.N + ' de ' + c.M + ' línea' + (c.M === 1 ? '' : 's') + ' con ficha logística → ' +
      fmtKg(c.kg) + ' kg · ' + (Math.round(c.m3 * 1000) / 1000).toFixed(3) + ' m³ · ' + c.bultos + ' bulto' + (c.bultos === 1 ? '' : 's') +
      (c.N < c.M ? '<div class="sub">Las otras ' + (c.M - c.N) + ' no suman peso ni volumen (sin ficha logística): ' + esc(c.sinFicha.slice(0, 4).join(' · ')) + (c.sinFicha.length > 4 ? ' · …' : '') + '. Ajusta a mano si hace falta.</div>' : '') +
      /* M2: no prometer que el manual se respeta al final — /retiros/nuevo
         toma tus valores para validar capacidad, pero /docs/agregar
         recalcula el m³ desde las fichas al asociar cada documento. */
      (editados.length ? '<div class="sub">Editaste ' + editados.map(function(k){ return { total_packages: 'bultos', total_weight_kg: 'peso', total_volume_m3: 'volumen' }[k]; }).join(', ') + ' a mano — así se envía al crear. Ojo: al asociar los documentos, la ficha del retiro recalcula el m³ desde las fichas logísticas. <a href="#" data-recalc="1">Volver al cálculo del ERP</a></div>' : '') +
      '</div></div>';
    el.innerHTML = html;
  }
  $('nriCargaInfo') && $('nriCargaInfo').addEventListener('click', function(ev){
    var a = ev.target.closest('[data-recalc]'); if (!a) return;
    ev.preventDefault();
    dirty = { total_packages: false, total_weight_kg: false, total_volume_m3: false };
    recalcularCarga();
    refreshSteps();
  });

  /* ═══════════════════ CREAR = ASOCIAR DE VERDAD ═══════════════════
     Lo llama el handler de #btnGuardarRetiroInterno (template) tras el
     POST /retiros/nuevo exitoso y ANTES del redirect. Devuelve
     {fallos:[...]} y, si hubo fallos, ya mostró el ilusAlert con el
     detalle — nunca en silencio. */
  window.nriAsociarDocsTrasCrear = async function(d, btn){
    var fallos = [];
    var docs = Array.from(DOCS.values());
    if (!docs.length || !d || !d.id) return { fallos: fallos, asociados: 0 };
    var okN = 0;
    for (var i = 0; i < docs.length; i++){
      var e = docs[i];
      if (btn) btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Asociando ' + (i + 1) + '/' + docs.length + '…';
      var lineas = Object.keys(e.seleccion).map(function(sku){
        // M3: marcada_sin_saldo por SKU tal como lo marcó el operador en el modal tka
        return { sku: sku, cantidad_seleccionada: num(e.seleccion[sku]), incluida: true, marcada_sin_saldo: !!(e.sinSaldo && e.sinSaldo[sku]) };
      });
      try {
        var res = await postJson('/retiros/' + encodeURIComponent(d.id) + '/docs/agregar', {
          document_type: e.tido, document_number: e.nudo_display, lineas: lineas,
        });
        if (res.r.ok && res.d.ok){ okN++; continue; }
        if (res.r.status === 409 || res.d.code === 'DUPLICATE'){ okN++; continue; }   // ya estaba: OK
        fallos.push(e.tido + ' ' + e.nudo_display + ': ' + (res.d.error || ('HTTP ' + res.r.status)));
      } catch(err){
        fallos.push(e.tido + ' ' + e.nudo_display + ': ' + (err.message || 'error de red'));
      }
    }
    if (fallos.length && window.ilusAlert){
      await window.ilusAlert({
        title: 'Retiro creado, pero faltó asociar documentos',
        message: 'El retiro ' + (d.code || '') + ' se creó' + (okN ? ' y se asociaron ' + okN + ' documento' + (okN === 1 ? '' : 's') : '') +
          '; no se pudo asociar: ' + fallos.join(' · ') + '. Complétalo en la ficha del retiro.',
        type: 'warning',
      });
    }
    return { fallos: fallos, asociados: okN };
  };

  /* ═══════════════════ CAPA DE INTELIGENCIA (Daniel 2026-09-15) ═══════════
     "que el modal sea lo suficientemente inteligente para que identifique
     patrones, repetidos y guíe al usuario con un semáforo hasta que los
     datos y el contorno del objeto estén en verde".

     · nriEvaluarPasos(): evaluador central. Por paso devuelve
       {idx, estado:'rojo'|'ambar'|'verde', faltan:[], avisos:[{txt, html?}], ok}
       y PINTA: borde/círculo de la tarjeta (.is-complete / .is-warn / nada),
       la línea .nri-step-estado con lo que falta o está raro, los avisos con
       acción (#nriDocAvisos / #nriCliAvisos) y el progreso del header.
       La llama nriRefreshSteps() del template (aditivo: el semáforo binario
       de siempre sigue corriendo debajo).
     · nriPreCrear(): pre-chequeo del botón "Crear y confirmar". NUNCA
       deshabilita el botón (regla de Daniel: proponer, no bloquear): con
       rojos hace scroll + resalte + toast; con solo ámbar pide confirmación.
     ═══════════════════════════════════════════════════════════════════════ */
  var inpPersona = campo('pickup_person_name'), inpPersonaRut = campo('pickup_person_rut'), selRelacion = campo('pickup_person_relation');
  var inpPersonaFono = campo('pickup_person_phone');
  var selResponsable = campo('responsable_user_id'), selCanal = campo('canal');

  /* ═══════════════ Paso 3 · ¿Quién viene a retirar? (2026-09-23) ═══════════
     Daniel: "acá no son los mismos trabajadores, así que deberíamos colocar
     que es el mismo cliente". Dos opciones como el formulario público:
       · 'cliente' (por defecto): retira_mismo_cliente=1 y el BACKEND toma
         nombre/RUT/teléfono del cliente. Los campos pickup_person_* NO se
         tocan en este modo — así, si el operador ya había escrito al chofer
         y toca "El mismo cliente" por error, al volver siguen ahí.
       · 'otra': se abren nombre/RUT/teléfono/relación para un tercero. */
  var modoRetira = 'cliente';
  function setModoRetira(modo){
    modoRetira = modo === 'otra' ? 'otra' : 'cliente';
    var esOtra = modoRetira === 'otra';
    var optCli = $('nriRetiraOptCliente'), optOtra = $('nriRetiraOptOtra');
    if (optCli){ optCli.classList.toggle('is-on', !esOtra); var r1 = optCli.querySelector('input'); if (r1) r1.checked = !esOtra; }
    if (optOtra){ optOtra.classList.toggle('is-on', esOtra); var r2 = optOtra.querySelector('input'); if (r2) r2.checked = esOtra; }
    var hid = $('nriRetiraMismoCliente'); if (hid) hid.value = esOtra ? '0' : '1';
    var campos = $('nriRetiraOtraFields'); if (campos) campos.style.display = esOtra ? '' : 'none';
    if (inpPersona){
      if (esOtra) inpPersona.setAttribute('required', 'required'); else inpPersona.removeAttribute('required');
      inpPersona.classList.toggle('nri-req', esOtra);   // nriPreCrear enfoca el primer .nri-req vacío
    }
  }
  /* Solo pinta la vista previa "El mismo cliente: Nombre · RUT". */
  function syncPersonaDesdeCliente(){
    var prev = $('nriRetiraClientePreview');
    if (!prev) return;
    var nom = val(inpNombre), rut = val(inpRut);
    prev.textContent = nom
      ? nom + (rut ? ' · RUT ' + (isValidRUT(rut) ? formatRUTStr(rut) : rut) : '')
      : 'Se toman los datos del Paso 2 (todavía vacío)';
  }
  function cambiarModoRetira(modo){
    var antes = modoRetira;
    setModoRetira(modo);
    if (modoRetira === 'otra' && antes === 'cliente'){
      if (selRelacion && selRelacion.value === 'dueno') selRelacion.value = 'otro';
      if (inpPersona) setTimeout(function(){ try { inpPersona.focus(); } catch(_){} }, 60);
    }
    refreshSteps();
  }
  document.querySelectorAll('input[name="nri_retira_modo"]').forEach(function(r){
    r.addEventListener('change', function(){ if (this.checked) cambiarModoRetira(this.value); });
  });
  setModoRetira('cliente');
  var btnCrear = $('btnGuardarRetiroInterno');
  var ICONO_ESTADO = { rojo: 'bi-x-circle-fill', ambar: 'bi-exclamation-triangle-fill', verde: 'bi-check-circle-fill' };

  /* ═══════════════════ Selectores de RESPONSABLES (Paso 3 + Paso 4) ═══════
     2026-09-22: #nriResponsable (Paso 4, obligatorio, SE envía) y
     (2026-09-23: #nriPersonaRetiraSel se quitó; queda solo #nriResponsable) comparten el
     mismo catálogo de /retiros/api/responsables — un solo fetch para los
     dos. El template los llama dentro de shown.bs.modal. */
  var _responsablesPromise = null;
  function _fetchResponsables(){
    if (!_responsablesPromise){
      _responsablesPromise = fetchJson('/retiros/api/responsables').then(function(res){
        if (!res.r.ok || !res.d.ok) throw new Error('no-ok');
        return res.d.responsables || [];
      }).catch(function(err){ _responsablesPromise = null; throw err; });
    }
    return _responsablesPromise;
  }
  window.nriPoblarSelectResponsables = function(selId){
    var sel = $(selId);
    if (!sel || sel.dataset.loaded === '1') return;
    var placeholder = selId === 'nriResponsable' ? 'Selecciona un responsable…' : 'Selecciona (opcional)…';
    _fetchResponsables().then(function(lista){
      var opts = lista.map(function(u){
        return '<option value="' + esc(u.id) + '" data-rut="' + esc(u.rut || '') + '">' + esc(u.nombre) + '</option>';
      }).join('');
      sel.innerHTML = '<option value="">' + placeholder + '</option>' + opts;
      sel.dataset.loaded = '1';
      refreshSteps();
    }).catch(function(){
      sel.innerHTML = '<option value="">(no se pudo cargar — reintenta)</option>';
    });
  };

  function val(inp){ return inp ? String(inp.value || '').trim() : ''; }
  function textoOpcion(sel){
    if (!sel || !sel.value) return '';
    var o = sel.options[sel.selectedIndex];
    return o ? String(o.textContent || '').trim() : sel.value;
  }
  function fmtFechaISO(iso){
    var m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(String(iso || '').trim());
    return m ? m[3] + '/' + m[2] + '/' + m[1] : String(iso || '');
  }
  /* ¿Ese texto es un RUT completo con DV que cuadra? (8-9 chars) */
  function esRutPlausible(txt){
    var c = cleanRUT(txt);
    return c.length >= 8 && c.length <= 9 && /^\d+[\dK]$/.test(c) && isValidRUT(c);
  }
  /* HTML de un aviso con acción. acts: [{act, label, primary, key}] */
  function avisoHtml(icono, innerHtml, acts){
    var botones = (acts || []).map(function(a){
      return '<button type="button" class="nri-aviso-btn' + (a.primary ? ' is-primary' : '') + '" data-act="' + esc(a.act) + '"' + (a.key != null ? ' data-key="' + esc(a.key) + '"' : '') + '>' + esc(a.label) + '</button>';
    }).join('');
    return '<div class="nri-aviso"><i class="bi ' + esc(icono) + '"></i><div class="txt">' + innerHtml + '</div>' + (botones ? '<div class="acts">' + botones + '</div>' : '') + '</div>';
  }
  function plural(n, uno, varios){ return n === 1 ? uno : varios; }

  /* ── Paso 1 · Documento (BLOQUEANTE) ──
     2026-09-23 (Daniel: "siempre tiene que haber una factura, una boleta o
     al menos una nota de venta"): rojo mientras no haya al menos UN
     documento de venta real. Se quitó la excepción "Continuar sin
     documento". Una cotización sola (COV) no cuenta — el backend la
     rechaza, así que aquí también queda en rojo. */
  function evalPaso1(){
    var r = { faltan: [], avisos: [], ok: '' };
    var buscado = val(inpBuscar), n = DOCS.size;
    if (n && (!val(selTipo) || !val(inpNumero))){
      r.faltan.push('una factura, boleta o nota de venta (una cotización sola no sirve)');
      return r;
    }
    if (!n){
      r.faltan.push('al menos una factura, boleta o nota de venta del ERP');
      if (buscado && esRutPlausible(buscado)){
        /* I2d: 8-9 dígitos con DV válido → probablemente es un RUT */
        r.avisos.push({
          txt: '¿' + formatRUTStr(buscado) + ' es un RUT? Va en el Paso 2',
          html: avisoHtml('bi-person-badge', '<strong>' + esc(formatRUTStr(buscado)) + '</strong> parece un RUT, no un N° de documento.',
            [{ act: 'numero-a-rut', label: 'Es un RUT → buscar cliente', primary: true }]),
        });
      }
      return r;
    }
    r.ok = n + ' documento' + plural(n, '', 's') + ' verificado' + plural(n, '', 's') + ' en ERP';
    var claves = {};
    DOCS.forEach(function(e){
      var lbl = e.tido + ' ' + e.nudo_display, m = e.meta || {};
      var rr = lineasResumen(e);
      var conSaldo = m.tiene_saldo != null ? !!m.tiene_saldo : rr.conSaldo > 0;
      if (!conSaldo) r.avisos.push({ txt: lbl + ' sin saldo (ya despachado)' });
      if (m.ya_tiene_retiro){
        /* I2a: repetido — ya está asociado a otro retiro */
        var info = retiroExistenteInfo(m);
        r.avisos.push({
          txt: lbl + ' ya está en ' + (info.code || 'otro retiro'),
          html: avisoHtml('bi-lock-fill', '<strong>' + esc(lbl) + '</strong> ya está en <strong>' + esc(info.code || 'otro retiro') + '</strong>.' +
            (info.url ? ' <a href="' + esc(info.url) + '" target="_blank" rel="noopener">ver retiro</a>' : '') + ' Se asocia igual porque lo elegiste tú.', []),
        });
      }
      var ck = docClaveRut(e);
      if (ck) claves[ck] = (claves[ck] || 0) + 1;
    });
    var nRuts = Object.keys(claves).length;
    if (nRuts > 1) r.avisos.push({ txt: 'Los documentos son de ' + nRuts + ' clientes (RUT) distintos' });   // I2c
    return r;
  }

  /* ── Paso 2 · Cliente ── */
  function evalPaso2(){
    var r = { faltan: [], avisos: [], ok: '' };
    var nombre = val(inpNombre), rut = val(inpRut), fono = val(inpFono), email = val(inpEmail);
    if (!nombre) r.faltan.push('nombre / razón social');
    else if (/^[0-9.\-kK\s]+$/.test(nombre) && esRutPlausible(nombre)){
      /* I2e: el "nombre" es un RUT */
      r.avisos.push({
        txt: 'El nombre parece un RUT',
        html: avisoHtml('bi-person-badge', 'El nombre <strong>' + esc(nombre) + '</strong> parece un RUT.',
          [{ act: 'nombre-a-rut', label: 'Es un RUT → buscar cliente', primary: true }]),
      });
    } else if (/^\d+$/.test(nombre)) r.avisos.push({ txt: 'El nombre tiene solo dígitos' });
    if (rut && !isValidRUT(rut)) r.avisos.push({ txt: 'RUT: dígito verificador no cuadra' });
    if (fono && !fonoChilenoOk(fono)) r.avisos.push({ txt: 'Teléfono no parece chileno (+56 9 XXXX XXXX)' });
    if (email && !emailOk(email)) r.avisos.push({ txt: 'Email con formato inválido' });
    var claveCli = rut ? rutClave(rut) : '';

    /* I3: cliente con retiro(s) ACTIVO(s) */
    if (retirosActivos && claveCli && retirosActivos.claveRut === claveCli && retirosActivos.lista.length){
      var L = retirosActivos.lista;
      var cab = 'Este cliente ya tiene ' + L.length + ' retiro' + plural(L.length, '', 's') + ' activo' + plural(L.length, '', 's');
      r.avisos.push({
        txt: cab + ': ' + L.map(function(x){ return x.code + (x.status_label ? ' (' + x.status_label + ')' : ''); }).join(', '),
        html: avisoHtml('bi-arrow-repeat', '<strong>' + esc(cab) + '</strong>: ' + L.map(function(x){
          return (x.url ? '<a href="' + esc(x.url) + '" target="_blank" rel="noopener">' + esc(x.code) + '</a>' : esc(x.code)) +
            (x.status_label ? ' (' + esc(x.status_label) + ')' : '') + (x.fecha ? ' · ' + esc(x.fecha) : '') + ' · <a href="' + esc(x.url || '#') + '" target="_blank" rel="noopener">ver</a>';
        }).join(' &nbsp;|&nbsp; ') + '. Revisa que no sea el mismo antes de crear otro.', []),
      });
    }

    /* I2b: RUT del/los documentos distinto al RUT escrito */
    if (claveCli){
      DOCS.forEach(function(e, key){
        var ck = docClaveRut(e);
        if (!ck || ck === claveCli || mantenerDistinto[key]) return;
        var lbl = e.tido + ' ' + e.nudo_display, nomDoc = docNombreCliente(e) || 'cliente no informado por ERP', rutDoc = docRutCliente(e);
        var quien = nombre || formatRUTStr(rut);
        r.avisos.push({
          txt: lbl + ' es de ' + nomDoc + ' (' + rutDoc + '), distinto a ' + quien,
          html: avisoHtml('bi-people-fill', '<strong>' + esc(lbl) + '</strong> es de <strong>' + esc(nomDoc) + '</strong> (' + esc(rutDoc) + '), distinto a <strong>' + esc(quien) + '</strong>. ' +
            'El dueño del documento es el cliente oficial; quien declaraste puede quedar como persona que retira.',
            [{ act: 'usar-cliente-doc', key: key, label: 'Usar cliente del documento', primary: true }, { act: 'mantener-distinto', key: key, label: 'Mantener' }]),
        });
      });
    }

    /* 2026-09-22: modo manual activo (botón "Editar manualmente") + el
       valor no coincide con ningún documento/ficha ERP conocido → AVISO,
       nunca bloqueo (el operador puede escribir clientes que el ERP no
       tiene, ej. cliente nuevo). */
    if (clienteManual && nombre){
      var coincideDoc = false;
      if (claveCli) DOCS.forEach(function(e){ if (docClaveRut(e) && docClaveRut(e) === claveCli) coincideDoc = true; });
      var coincideFicha = !!(fichaCliente && claveCli && fichaCliente.claveRut === claveCli);
      if (!coincideDoc && !coincideFicha) r.avisos.push({ txt: 'Cliente escrito a mano, no verificado en ERP' });
    }
    if (!r.faltan.length){
      r.ok = (fichaCliente && claveCli && fichaCliente.claveRut === claveCli) ? 'Cliente verificado en ERP' : 'Cliente completo';
    }
    return r;
  }

  /* ── Paso 3 · Persona que retira + Responsable (FUSIONADOS, 2026-09-23) ──
     Antes eran evalPaso3 (persona) + evalPaso4 (responsable) por separado,
     cada uno con su propia tarjeta. Daniel las quiso juntas ("la persona
     quien retira, el responsable del retiro") -- un solo semáforo para las
     dos, "falta" acumula lo que falte de cualquiera de las dos mitades. */
  /* 2026-09-23: dos modos — 'cliente' (por defecto, retira el mismo
     cliente del Paso 2) u 'otra' (tercero con sus datos). El responsable
     de ILUS se pide una sola vez. */
  function evalPaso3(){
    var r = { faltan: [], avisos: [], ok: '' };
    var nombre = val(inpPersona), rut = val(inpPersonaRut), fono = val(inpPersonaFono);
    var nomCli = val(inpNombre);
    var quien;
    if (modoRetira === 'cliente'){
      if (!nomCli) r.faltan.push('el cliente del Paso 2 (retira el mismo cliente)');
      quien = 'Retira el mismo cliente';
    } else {
      if (nombre.length < 2) r.faltan.push('nombre de quien retira');
      if (rut && !isValidRUT(rut)) r.avisos.push({ txt: 'RUT de quien retira: dígito verificador no cuadra' });
      if (fono && !fonoChilenoOk(fono)) r.avisos.push({ txt: 'Teléfono de quien retira no parece chileno (+56 9 XXXX XXXX)' });
      if (nombre && nomCli && nombre.toLowerCase() === nomCli.toLowerCase()){
        r.avisos.push({ txt: 'Es el mismo nombre del cliente — si viene él, marca "El mismo cliente"' });
      }
      quien = 'Retira ' + nombre + (rut && isValidRUT(rut) ? ' · ' + formatRUTStr(rut) : '') + (selRelacion && selRelacion.value ? ' (' + textoOpcion(selRelacion).toLowerCase() + ')' : '');
    }
    if (!val(selResponsable)) r.faltan.push('responsable en ILUS');
    if (!r.faltan.length){
      r.ok = quien + ' · Responsable ILUS: ' + textoOpcion(selResponsable);
    }
    return r;
  }

  /* ── Paso 5 · Carga (opcional) ── */
  function evalPaso5(){
    var r = { faltan: [], avisos: [], ok: '' };
    var kg = num(val(inpKg)), m3 = num(val(inpM3)), bultos = num(val(inpBultos));
    if (kg > 0){
      r.ok = fmtKg(kg) + ' kg · ' + (Math.round(m3 * 1000) / 1000).toFixed(3) + ' m³ · ' + bultos + ' bulto' + plural(bultos, '', 's');
      return r;
    }
    if (DOCS.size){
      var c = ultimoCalculo, sin = c ? Math.max(0, c.M - c.N) : 0;
      r.avisos.push({ txt: sin ? sin + ' línea' + plural(sin, '', 's') + ' sin ficha logística → peso 0, revisa' : 'Documentos asociados pero peso 0 — revisa la carga' });
    } else if (kg <= 0 && m3 <= 0){
      r.avisos.push({ txt: 'Sin carga declarada' });
    }
    r.ok = 'Carga declarada';
    return r;
  }

  /* ── Paso 6 · Agenda ── */
  function evalPaso6(){
    var r = { faltan: [], avisos: [], ok: '' };
    var f = val($('nriHidDate')), tf = val($('nriHidTf')), tt = val($('nriHidTt'));
    if (!f && !tf) r.faltan.push('fecha y bloque horario');
    else if (!f) r.faltan.push('fecha');
    else if (!tf) r.faltan.push('bloque horario');
    if (!r.faltan.length) r.ok = 'Día ' + fmtFechaISO(f) + ' · ' + tf + (tt ? '–' + tt : '');
    return r;
  }

  /* ── Paso 6 · Canal de entrada (2026-09-23: ya no es "por dónde aceptó",
     es por dónde ENTRÓ la solicitud) ── */
  function evalPaso7(){
    var r = { faltan: [], avisos: [], ok: '' };
    if (!val(selCanal)) r.faltan.push('canal por el que entró la solicitud');
    else r.ok = 'Entró por ' + textoOpcion(selCanal);
    return r;
  }

  function pintarPaso(idx, r){
    var card = $('nriStep' + idx), linea = $('nriStepEstado' + idx);
    var estado = r.faltan.length ? 'rojo' : (r.avisos.length ? 'ambar' : 'verde');
    r.idx = idx; r.estado = estado;
    if (card){
      card.classList.toggle('is-complete', estado === 'verde');
      card.classList.toggle('is-warn', estado === 'ambar');
    }
    if (linea){
      var txt;
      if (estado === 'rojo') txt = 'Falta: ' + r.faltan.join(' · ') + (r.avisos.length ? ' · ' + r.avisos.map(function(a){ return a.txt; }).join(' · ') : '');
      else if (estado === 'ambar') txt = r.avisos.map(function(a){ return a.txt; }).join(' · ');
      else txt = r.ok;
      linea.innerHTML = txt ? '<i class="bi ' + ICONO_ESTADO[estado] + '"></i><span>' + esc(txt) + '</span>' : '';
    }
    var contAvisos = idx === 1 ? $('nriDocAvisos') : (idx === 2 ? $('nriCliAvisos') : null);
    if (contAvisos){
      var html = r.avisos.filter(function(a){ return a.html; }).map(function(a){ return a.html; }).join('');
      if (contAvisos.innerHTML !== html) contAvisos.innerHTML = html;
    }
  }
  function pintarProgreso(res){
    var verdes = 0, rojos = 0, avisos = 0;
    res.forEach(function(r){ if (r.estado === 'verde') verdes++; if (r.estado === 'rojo') rojos++; avisos += r.avisos.length; });
    var txt = $('nriProgresoTxt'), bar = $('nriProgresoBar'), av = $('nriProgresoAvisos');
    if (txt) txt.textContent = verdes + ' de ' + res.length + ' pasos listos';
    if (bar) bar.style.width = Math.round(verdes / Math.max(1, res.length) * 100) + '%';
    if (av) av.textContent = avisos ? '· ' + avisos + ' aviso' + plural(avisos, '', 's') : '';
    if (btnCrear) btnCrear.classList.toggle('is-pending', rojos > 0);
  }

  /* Evaluador central. Cada paso va en su try/catch: un dato raro en uno no
     puede apagar el semáforo de los demás. */
  window.nriEvaluarPasos = function(){
    /* 2026-09-23: 6 pasos (bajó de 7 al fusionar Persona+Responsable en
       evalPaso3). Los nombres de función quedan igual -- solo cambia el
       ORDEN/cantidad del array, que es lo que decide a qué #nriStepN se
       pinta cada uno (ver pintarPaso: usa la posición i+1, no el nombre). */
    var evals = [evalPaso1, evalPaso2, evalPaso3, evalPaso5, evalPaso6, evalPaso7];
    /* 2026-09-23: "el mismo cliente" se copia en cada evaluación — así
       cualquier cambio del Paso 2 (tipeado o precargado del ERP) llega solo
       al Paso 3 sin un listener por campo. */
    try { syncPersonaDesdeCliente(); } catch(eSync){ console.warn('[nri] syncPersona', eSync); }
    var res = evals.map(function(fn, i){
      var r;
      try { r = fn(); } catch(e){ console.warn('[nri] evalPaso' + (i + 1), e); r = { faltan: [], avisos: [], ok: '' }; }
      pintarPaso(i + 1, r);
      return r;
    });
    pintarProgreso(res);
    ultimaEvaluacion = res;
    return res;
  };

  /* ── Pre-chequeo de "Crear y confirmar" (I4) ── */
  window.nriPreCrear = async function(){
    var res = window.nriEvaluarPasos();
    var rojos = res.filter(function(r){ return r.estado === 'rojo'; });
    if (rojos.length){
      var card = $('nriStep' + rojos[0].idx);
      if (card){
        try { card.scrollIntoView({ behavior: 'smooth', block: 'center' }); } catch(_){ card.scrollIntoView(); }
        card.classList.remove('nri-flash'); void card.offsetWidth; card.classList.add('nri-flash');
        setTimeout(function(){ card.classList.remove('nri-flash'); }, 1300);
        /* el primer campo obligatorio VACÍO (no el primero que aparezca) */
        var f = Array.prototype.find.call(card.querySelectorAll('.nri-req:not([type="hidden"])'), function(el){
          return String(el.value || '').trim().length < (el.name === 'pickup_person_name' ? 2 : 1);
        });
        if (f) try { f.focus({ preventScroll: true }); } catch(_){}
      }
      toast('Falta: ' + rojos.map(function(r){ return 'Paso ' + r.idx + ' → ' + r.faltan.join(', '); }).join(' · '), 'warning');
      return false;
    }
    var avisos = [];
    res.forEach(function(r){ r.avisos.forEach(function(a){ avisos.push('Paso ' + r.idx + ': ' + a.txt); }); });
    if (!avisos.length) return true;
    if (typeof window.ilusConfirm !== 'function') return true;
    return !!(await window.ilusConfirm({
      title: 'Hay ' + avisos.length + ' advertencia' + plural(avisos.length, '', 's'),
      message: '¿Crear el retiro igual?',
      sub: avisos.map(function(a){ return '<div>• ' + esc(a) + '</div>'; }).join(''),
      subHtml: true,
      okLabel: 'Crear igual', cancelLabel: 'Revisar',
      type: 'warning',
    }));
  };

  /* ── Acciones de los avisos (delegación en #nriDocAvisos y #nriCliAvisos) ── */
  function scrollAPaso(idx){
    var card = $('nriStep' + idx);
    if (card) try { card.scrollIntoView({ behavior: 'smooth', block: 'start' }); } catch(_){}
  }
  /* Mueve un texto que en realidad es un RUT al campo RUT del Paso 2 y
     dispara la ficha. Si el RUT ya tiene otro valor, se ofrece como chip. */
  function moverARutCliente(txt, origen, limpiar){
    var rut = formatRUTStr(txt);
    if (!inpRut) return;
    var actual = val(inpRut);
    if (!actual || rutClave(actual) === rutClave(rut)){
      inpRut.value = rut;
      inpRut.classList.add('nri-precargado'); setTimeout(function(){ inpRut.classList.remove('nri-precargado'); }, 1800);
    } else {
      proponerChip('customer_rut', rut, origen);
    }
    if (limpiar) limpiar();
    pintarValidezRut(inpRut);
    renderChips();
    consultarRutSiValido();
    refreshSteps();
    scrollAPaso(2);
    toast('RUT movido al Paso 2 — consultando la ficha del cliente en el ERP…', 'info');
  }
  async function usarClienteDelDoc(key){
    var e = DOCS.get(key); if (!e) return;
    var declarado = { nombre: val(inpNombre), rut: val(inpRut), fono: val(inpFono) };
    var nomDoc = docNombreCliente(e), rutDoc = docRutCliente(e), h = e.hdr || {};
    if (nomDoc && inpNombre) inpNombre.value = nomDoc;
    if (rutDoc && inpRut){ inpRut.value = rutDoc; pintarValidezRut(inpRut); }
    if (h.telefono && inpFono && !val(inpFono)) inpFono.value = h.telefono;
    [inpNombre, inpRut].forEach(function(i){ if (i){ i.classList.add('nri-precargado'); setTimeout(function(){ i.classList.remove('nri-precargado'); }, 1800); } });
    renderChips();
    consultarRutSiValido();
    refreshSteps();
    /* Regla de Daniel: dueño del documento = cliente oficial; el declarado
       puede quedar como persona que retira → se OFRECE, no se hace solo. */
    var nombreDistinto = declarado.nombre && nomDoc && declarado.nombre.toLowerCase() !== nomDoc.toLowerCase();
    /* 2026-09-23: en modo "el mismo cliente" inpPersona trae la copia del
       cliente, así que también se ofrece; aceptar pasa a "Otra persona". */
    if (nombreDistinto && inpPersona && (modoRetira === 'cliente' || !val(inpPersona)) && typeof window.ilusConfirm === 'function'){
      var ok = await window.ilusConfirm({
        title: 'Persona que retira',
        message: '¿Dejar a ' + declarado.nombre + ' como la persona que retira (Paso 3)?',
        sub: 'El cliente oficial queda ' + nomDoc + (rutDoc ? ' (' + rutDoc + ')' : '') + '.',
        okLabel: 'Sí, como persona que retira', cancelLabel: 'No',
        type: 'question',
      });
      if (ok){
        cambiarModoRetira('otra');
        inpPersona.value = declarado.nombre;
        if (inpPersonaRut) inpPersonaRut.value = (declarado.rut && isValidRUT(declarado.rut)) ? formatRUTStr(declarado.rut) : '';
        if (inpPersonaFono) inpPersonaFono.value = declarado.fono || '';
        if (selRelacion && selRelacion.value === 'otro' && selRelacion.querySelector('option[value="autorizado"]')) selRelacion.value = 'autorizado';
        pintarValidezRut(inpPersonaRut);
        refreshSteps();
      }
    }
    toast('✓ Cliente tomado del documento ' + e.tido + ' ' + e.nudo_display, 'success');
  }
  function onAccionAviso(ev){
    var b = ev.target.closest('[data-act]'); if (!b) return;
    var act = b.getAttribute('data-act'), key = b.getAttribute('data-key');
    if (act === 'numero-a-rut'){
      /* 2026-09-22: la fuente es inpBuscar (#nriDocBuscar) — inpNumero ya no
         se tipea a mano, es la vitrina readonly del ERP. */
      var n = val(inpBuscar);
      moverARutCliente(n, 'búsqueda del Paso 1', function(){
        if (inpBuscar) inpBuscar.value = '';
        busqSeq++; setDocStatus(''); renderSugerencias([]);
      });
    } else if (act === 'nombre-a-rut'){
      var nm = val(inpNombre);
      moverARutCliente(nm, 'campo nombre', function(){ if (inpNombre) inpNombre.value = ''; cerrarAc(); });
    } else if (act === 'usar-cliente-doc'){
      usarClienteDelDoc(key);
    } else if (act === 'mantener-distinto'){
      mantenerDistinto[key] = true;
      refreshSteps();
      toast('Se mantiene el cliente que escribiste; el documento queda asociado igual.', 'info');
    }
  }
  $('nriDocAvisos') && $('nriDocAvisos').addEventListener('click', onAccionAviso);
  $('nriCliAvisos') && $('nriCliAvisos').addEventListener('click', onAccionAviso);

  /* 2026-09-23: se quitaron el chip "Es el mismo cliente" (ahora es la
     opción por defecto del Paso 3), "Es el mismo responsable" y la lista
     de responsables repetida como atajo — Daniel: "la persona que retira
     y el responsable está como dos veces, mejora esto". */
  inpPersonaRut && inpPersonaRut.addEventListener('input', function(){ pintarValidezRut(inpPersonaRut); });
  inpPersonaRut && inpPersonaRut.addEventListener('blur', function(){
    if (cleanRUT(inpPersonaRut.value).length >= 2) inpPersonaRut.value = formatRUTStr(inpPersonaRut.value);
    pintarValidezRut(inpPersonaRut);
    refreshSteps();
  });

  /* ═══════════════════ Reset al cerrar sin crear ═══════════════════
     2026-09-23 (Daniel: "cerré el modal, lo volví a abrir y me trajo los
     mismos datos... si lo cerramos a propósito, eso se tiene que limpiar"):
     el template ahora llama form.reset() en hidden.bs.modal (cierre con la
     X o "Cancelar"; el backdrop es static, así que no hay cierre accidental).
     Si falla la red al crear, el modal NO se cierra y los datos quedan. */
  window.nriTieneDatos = function(){
    if (DOCS.size || docLoading.size) return true;
    var hay = ['customer_name', 'customer_rut', 'contact_phone', 'contact_email', 'responsable_user_id', 'canal', 'observations']
      .some(function(n){ return !!val(campo(n)); });
    if (hay) return true;
    if (modoRetira === 'otra' && (val(inpPersona) || val(inpPersonaRut) || val(inpPersonaFono))) return true;
    return !!(val(inpBuscar) || val($('nriHidTf')));
  };
  form.addEventListener('reset', function(){
    DOCS.clear(); docLoading.clear(); chips.clear(); dirty = { total_packages: false, total_weight_kg: false, total_volume_m3: false };
    /* descarta TODAS las respuestas del ERP que lleguen después del cierre
       (revisión 2026-09-23: facturas en carga, búsqueda pendiente,
       autocompletar y retiros activos también) */
    docsGen++; busqSeq++; fichaSeq++; saldoSeq++; activosSeq++; acSeq++;
    busqPendiente = null;
    cerrarAc();
    ultimoRutConsultado = ''; sugerenciasActuales = []; rutDocsActuales = []; ultimoCalculo = null;
    fichaCliente = null; retirosActivos = null; mantenerDistinto = {}; ultimaEvaluacion = null;
    setDocStatus(''); setCliStatus(''); renderChips(); renderSugerencias([]); renderRutDocs([]); renderDocsSel(); pintarCargaInfo(null);
    sincronizarPaso1DesdeDocs();   // oculta el badge "N documentos verificados"
    try { renderTablaProductosNRI(); } catch(_){}
    pintarUbicacionCliente('', '');
    setModoRetira('cliente');
    if (inpPersonaRut) inpPersonaRut.classList.remove('is-valid', 'is-invalid');
    if (inpRut) inpRut.classList.remove('is-valid', 'is-invalid');
    /* 2026-09-22: re-bloquear Cliente (Paso 2) si quedó en edición manual. */
    clienteManual = false;
    if (inpNombre) inpNombre.setAttribute('readonly', 'readonly');
    if (inpRut) inpRut.setAttribute('readonly', 'readonly');
    var _mBtn = $('nriClienteManualBtn'), _mBadge = $('nriClienteManualBadge');
    if (_mBtn){ _mBtn.classList.remove('is-on'); _mBtn.disabled = false; _mBtn.innerHTML = '<i class="bi bi-pencil"></i>Editar manualmente'; }
    if (_mBadge) _mBadge.style.display = 'none';
    /* form.reset() restaura los .value de forma asíncrona respecto a este
       evento: se re-evalúa en el siguiente tick para pintar el semáforo limpio. */
    setTimeout(refreshSteps, 0);
  });

  /* Valores iniciales ya presentes (p.ej. reapertura) */
  renderDocsSel();
  pintarCargaInfo(ultimoCalculo);   // 2026-09-22: estado "Esperando el documento…" visible desde el arranque
  if (inpRut && isValidRUT(inpRut.value)) consultarRutSiValido();
  /* Primera pintada del semáforo de tres estados (I5): window.nriRefreshSteps
     ya existe porque el <script> inline del template corre antes que este
     archivo (defer). */
  refreshSteps();
})();

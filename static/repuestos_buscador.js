/* ═══════════════════════════════════════════════════════════════════════════
   repuestos_buscador.js — Motor de búsqueda de repuestos de la Bodega,
   COMPARTIDO por los dos modales que piden repuestos (2026-09-26):

     · /repuestos → "Solicitar repuesto" (gestión, templates/clientes_hub/repuestos.html)
     · OT 2.0     → "Solicitar repuestos" del técnico (#modalRepSol, templates/ot2/detalle.html)

   Daniel (2026-09-26, textual): "Este modal debe ser súper poderoso... tengo
   que poder buscar por proveedor y por equipo, es decir, buscar por los
   modelos compatibles" y "el motor de búsqueda igual deberá aplicar cuando el
   técnico tenga la OT y solicitará repuesto: proveedor, modelo compatible y
   repuesto, obviamente SKU también".

   Backend: GET /ot/api/repuestos/bodega-buscar (app.py) -- UN solo endpoint
   con filtros combinables (q, proveedor_id, marca_id, maquina_id/modelo_id,
   solo_compat) y ranking (SKU exacto → compatibles → empieza con → A-Z). El
   semáforo de stock (verde/ámbar/rojo, disponible/comprometido/por llegar)
   viene YA calculado del backend (_otrep_fmt_stock): acá no se recalcula.

   Permisos: lo que el usuario NO puede ver simplemente no viene en el JSON
   (_OTREP_STOCK_NO_EXTERNO): la tarjeta pinta solo los campos presentes. Con
   `mostrarProveedor:false` (técnico externo) tampoco se ofrece el filtro.

   Uso:
     var b = RepBuscador.crear({
       mount: el,                 // contenedor donde se pinta todo
       ctx: 'gestion' | 'ot',     // 'ot' recorta costo en el backend (ctx=ot)
       modo: 'agregar' | 'seleccion',
                                  // agregar: cantidad + botón "Agregar" por tarjeta
                                  // seleccion: la tarjeta entera es un botón (flujo OT)
       equipo: {id, nombre, sku} | null,   // contexto "Compatibles con <equipo>"
       compatPorDefecto: true|false,       // arranca filtrando compatibles (gestión) o solo rankeando (OT)
       mostrarProveedor: true|false,
       provs: [...] | null, marcas: [...] | null,   // null = se cargan solas
       headers: {},               // headers extra para fetch (ej. X-Requested-With)
       enLista: function(id) → cantidad ya elegida (0 si no),
       onAgregar(item, cantidad), onSeleccionar(item, cardEl), onResultados(items)
     });
     b.setEquipo(eq) · b.setProveedores(lista) · b.setSeleccion(id|null)
     b.limpiar() · b.buscar() · b.refrescar() · b.focus() · b.getItems()
   ═══════════════════════════════════════════════════════════════════════ */
(function (global) {
  'use strict';

  var CANAL = {
    whatsapp: { ico: 'bi-whatsapp', lbl: 'WhatsApp', cls: 'whatsapp' },
    wechat:   { ico: 'bi-wechat',   lbl: 'WeChat',   cls: 'wechat' },
    telefono: { ico: 'bi-telephone', lbl: 'Teléfono', cls: 'telefono' },
    email:    { ico: 'bi-envelope',  lbl: 'Email',    cls: 'email' }
  };

  function esc(s) {
    return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) {
      return ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[c];
    });
  }
  function fmt(n) { n = Number(n); return isFinite(n) ? String(Math.round(n * 100) / 100) : ''; }

  /* Chip de canal preferido del proveedor (mismos colores que el modal
     "Asignar a ticket" de Seguimiento: WhatsApp verde, WeChat verde claro,
     teléfono azul, email gris). */
  function canalChip(canal) {
    var c = CANAL[canal];
    if (!c) return '';
    return '<span class="rpb-canal ' + c.cls + '"><i class="bi ' + c.ico + '"></i>' + c.lbl + '</span>';
  }

  /* Semáforo de stock. Usa `semaforo`/`disponible`/`comprometido`/`por_llegar`
     del backend; si no vienen (técnico externo) cae a con_stock sí/no. */
  function semaforoHTML(it) {
    if (it.disponible == null) {
      if (it.con_stock == null) return '';
      return it.con_stock
        ? '<span class="rpb-sem verde"><i class="bi bi-check-circle-fill"></i>En stock</span>'
        : '<span class="rpb-sem rojo"><i class="bi bi-x-circle-fill"></i>Sin stock</span>';
    }
    var sem = it.semaforo || (it.disponible > 0 ? 'verde' : ((it.cantidad || 0) > 0 ? 'ambar' : 'rojo'));
    var main;
    if (sem === 'verde') main = '<span class="rpb-sem verde"><i class="bi bi-check-circle-fill"></i>Disponible ' + fmt(it.disponible) + '</span>';
    else if (sem === 'ambar') main = '<span class="rpb-sem ambar"><i class="bi bi-exclamation-triangle-fill"></i>Falta · todo comprometido</span>';
    else main = '<span class="rpb-sem rojo"><i class="bi bi-x-circle-fill"></i>Sin stock</span>';
    var sub = [];
    sub.push('físico ' + fmt(it.cantidad || 0));
    if ((it.comprometido || 0) > 0) sub.push('comprometido ' + fmt(it.comprometido));
    if ((it.por_llegar || 0) > 0) sub.push('por llegar ' + fmt(it.por_llegar));
    return main + '<span class="rpb-sem-sub">' + esc(sub.join(' · ')) + '</span>';
  }

  /* Pastilla de proveedor: nombre, o "Sin proveedor" en ámbar. Solo si el
     campo VIENE en el JSON (un externo no lo recibe → no se pinta nada). */
  function proveedorBadge(it) {
    if (!('proveedor' in it) && !('proveedor_id' in it)) return '';
    if (it.proveedor) {
      return '<span class="rpb-tag prov"><i class="bi bi-truck"></i>' + esc(it.proveedor) + canalChip(it.proveedor_canal) + '</span>';
    }
    return '<span class="rpb-tag sinprov"><i class="bi bi-exclamation-triangle-fill"></i>Sin proveedor</span>';
  }

  function modelosHTML(it, modeloCtxId) {
    var ms = it.modelos || [];
    if (!ms.length) return '<div class="rpb-modelos rpb-modelos-vacio"><i class="bi bi-diagram-3"></i>Sin modelos compatibles declarados</div>';
    return '<div class="rpb-modelos"><i class="bi bi-diagram-3" title="Modelos compatibles"></i>' + ms.map(function (m) {
      var on = modeloCtxId && Number(m.id) === Number(modeloCtxId);
      return '<span class="rpb-modelo' + (on ? ' on' : '') + '" title="' + esc(m.sku || '') + '">' + esc(m.nombre || m.sku || '') + '</span>';
    }).join('') + '</div>';
  }

  function crear(opts) {
    opts = opts || {};
    var st = {
      mount: opts.mount, ctx: opts.ctx || 'gestion',
      modo: opts.modo === 'seleccion' ? 'seleccion' : 'agregar',
      equipo: opts.equipo || null,
      soloCompat: opts.compatPorDefecto !== false,
      modelo: null, q: '', proveedor: '', marca: '',
      provs: Array.isArray(opts.provs) ? opts.provs : null,
      marcas: Array.isArray(opts.marcas) ? opts.marcas : null,
      mostrarProveedor: opts.mostrarProveedor !== false,
      headers: opts.headers || {},
      enLista: typeof opts.enLista === 'function' ? opts.enLista : function () { return 0; },
      onAgregar: opts.onAgregar, onSeleccionar: opts.onSeleccionar, onResultados: opts.onResultados,
      items: [], modeloCtx: null, seleccionId: null, timer: null, modeloTimer: null, req: 0,
      sinModelo: false
    };
    if (!st.mount) throw new Error('RepBuscador: falta mount');

    /* ── shell ────────────────────────────────────────────────────────── */
    st.mount.classList.add('rpb');
    if (!st.mostrarProveedor) st.mount.classList.add('sin-prov');
    st.mount.innerHTML =
      '<div class="rpb-barra">' +
        '<div class="rpb-q"><i class="bi bi-search"></i>' +
          '<input type="search" class="rpb-q-in" autocomplete="off" ' +
            'placeholder="' + (st.mostrarProveedor ? 'Nombre, SKU, código de fabricante, marca, modelo o proveedor…' : 'Nombre, SKU, código de fabricante, marca o modelo…') + '">' +
          '<button type="button" class="rpb-q-x" title="Borrar texto" aria-label="Borrar texto"><i class="bi bi-x-lg"></i></button>' +
        '</div>' +
        '<div class="rpb-filtros">' +
          (st.mostrarProveedor ? '<select class="rpb-prov" aria-label="Proveedor"><option value="">Proveedor: todos</option><option value="sin">⚠ Sin proveedor</option></select>' : '') +
          '<select class="rpb-marca" aria-label="Marca"><option value="">Marca: todas</option></select>' +
          '<div class="rpb-modelo-wrap">' +
            '<input type="search" class="rpb-modelo-in" autocomplete="off" placeholder="Modelo compatible (nombre o SKU)…" aria-label="Modelo compatible">' +
            '<div class="rpb-modelo-drop"></div>' +
          '</div>' +
          '<button type="button" class="rpb-limpiar"><i class="bi bi-eraser"></i>Limpiar</button>' +
        '</div>' +
        '<div class="rpb-ctx"></div>' +
      '</div>' +
      '<div class="rpb-aviso"></div>' +
      '<div class="rpb-res"></div>';

    var $ = function (sel) { return st.mount.querySelector(sel); };
    var inQ = $('.rpb-q-in'), selProv = $('.rpb-prov'), selMarca = $('.rpb-marca'),
        inModelo = $('.rpb-modelo-in'), dropModelo = $('.rpb-modelo-drop'),
        ctxBox = $('.rpb-ctx'), avisoBox = $('.rpb-aviso'), resBox = $('.rpb-res');

    /* ── filtros: proveedores / marcas ───────────────────────────────── */
    function pintarProvs() {
      if (!selProv) return;
      var cur = selProv.value;
      selProv.innerHTML = '<option value="">Proveedor: todos</option><option value="sin">⚠ Sin proveedor</option>' +
        (st.provs || []).map(function (p) { return '<option value="' + p.id + '">' + esc(p.nombre) + '</option>'; }).join('');
      selProv.value = cur;
      if (selProv.value !== cur) selProv.value = '';
    }
    function pintarMarcas() {
      var cur = selMarca.value;
      selMarca.innerHTML = '<option value="">Marca: todas</option>' +
        (st.marcas || []).map(function (m) { return '<option value="' + m.id + '">' + esc(m.nombre) + '</option>'; }).join('');
      selMarca.value = cur;
      if (selMarca.value !== cur) selMarca.value = '';
    }
    async function cargarOpciones() {
      if (st.mostrarProveedor && !st.provs) {
        try {
          var r = await fetch('/mantenciones/api/proveedores-repuesto', { credentials: 'same-origin', headers: st.headers });
          var j = await r.json();
          st.provs = j.proveedores || [];
        } catch (e) { st.provs = []; }
      }
      pintarProvs();
      if (!st.marcas) {
        try {
          var r2 = await fetch('/mantenciones/api/repuestos-stock/buscar-marcas?todas=1', { credentials: 'same-origin', headers: st.headers });
          var j2 = await r2.json();
          st.marcas = j2.marcas || [];
        } catch (e2) { st.marcas = []; }
      }
      pintarMarcas();
    }
    cargarOpciones();

    /* ── contexto: equipo / modelo ───────────────────────────────────── */
    function pintarCtx() {
      var h = '';
      if (st.modelo) {
        h += '<span class="rpb-chip on"><i class="bi bi-diagram-3"></i><span>Compatibles con el modelo <b>' + esc(st.modelo.nombre || st.modelo.sku) + '</b></span>' +
             '<button type="button" class="rpb-chip-x" data-act="quitar-modelo" title="Quitar modelo" aria-label="Quitar modelo"><i class="bi bi-x-lg"></i></button></span>';
      } else if (st.equipo) {
        var nombreEq = st.equipo.nombre || ('Equipo #' + st.equipo.id);
        if (st.sinModelo) {
          h += '<span class="rpb-chip off" title="Este equipo no tiene modelo en el Catálogo"><i class="bi bi-diagram-3"></i><span>Sin modelo para <b>' + esc(nombreEq) + '</b></span></span>';
        } else {
          h += '<button type="button" class="rpb-chip' + (st.soloCompat ? ' on' : '') + '" data-act="compat"><i class="bi bi-diagram-3"></i><span>Compatibles con <b>' + esc(nombreEq) + '</b></span></button>' +
               '<button type="button" class="rpb-chip' + (!st.soloCompat ? ' on' : '') + '" data-act="todos"><i class="bi bi-grid"></i><span>Toda la bodega' + (st.soloCompat ? '' : ' <small>(compatibles primero)</small>') + '</span></button>';
        }
      }
      ctxBox.innerHTML = h;
      ctxBox.style.display = h ? '' : 'none';
    }
    ctxBox.addEventListener('click', function (e) {
      var b = e.target.closest('[data-act]');
      if (!b) return;
      if (b.dataset.act === 'compat') { st.soloCompat = true; pintarCtx(); buscar(); }
      else if (b.dataset.act === 'todos') { st.soloCompat = false; pintarCtx(); buscar(); }
      else if (b.dataset.act === 'quitar-modelo') { st.modelo = null; inModelo.value = ''; pintarCtx(); buscar(); }
    });

    /* Typeahead de modelos del Catálogo (cat_productos) -- mismo endpoint
       que ya usa "Nuevo repuesto" para declarar compatibilidad. */
    inModelo.addEventListener('input', function () {
      clearTimeout(st.modeloTimer);
      var q = (inModelo.value || '').trim();
      if (q.length < 2) { dropModelo.style.display = 'none'; dropModelo.innerHTML = ''; return; }
      st.modeloTimer = setTimeout(async function () {
        try {
          var r = await fetch('/mantenciones/api/repuestos-stock/buscar-modelos?q=' + encodeURIComponent(q), { credentials: 'same-origin', headers: st.headers });
          var j = await r.json();
          var ps = j.productos || [];
          dropModelo.innerHTML = ps.length ? ps.map(function (p) {
            return '<button type="button" class="rpb-modelo-opt" data-id="' + p.id + '" data-sku="' + esc(p.sku || '') + '" data-nombre="' + esc(p.nombre || '') + '">' +
              '<b>' + esc(p.nombre || '') + '</b><small>' + esc(p.sku || '') + '</small></button>';
          }).join('') : '<div class="rpb-modelo-vacio">Ningún modelo del Catálogo con "' + esc(q) + '".</div>';
          dropModelo.style.display = '';
        } catch (e) { dropModelo.innerHTML = '<div class="rpb-modelo-vacio">No se pudo buscar modelos.</div>'; dropModelo.style.display = ''; }
      }, 250);
    });
    dropModelo.addEventListener('click', function (e) {
      var b = e.target.closest('.rpb-modelo-opt');
      if (!b) return;
      st.modelo = { id: parseInt(b.dataset.id, 10), sku: b.dataset.sku, nombre: b.dataset.nombre };
      inModelo.value = '';
      dropModelo.style.display = 'none';
      pintarCtx();
      buscar();
    });
    document.addEventListener('click', function (e) {
      if (!st.mount.contains(e.target)) dropModelo.style.display = 'none';
    });

    /* ── búsqueda ────────────────────────────────────────────────────── */
    function hayCriterio() {
      return st.q.length >= 2 || !!st.proveedor || !!st.marca || !!st.modelo || (!!st.equipo && st.soloCompat && !st.sinModelo);
    }
    function urlBusqueda() {
      var p = new URLSearchParams();
      if (st.q.length >= 2) p.set('q', st.q);
      p.set('ctx', st.ctx === 'ot' ? 'ot' : 'gestion');
      if (st.proveedor) p.set('proveedor_id', st.proveedor);
      if (st.marca) p.set('marca_id', st.marca);
      if (st.modelo) { p.set('modelo_id', st.modelo.id); p.set('solo_compat', '1'); }
      else if (st.equipo) { p.set('maquina_id', st.equipo.id); p.set('solo_compat', st.soloCompat ? '1' : '0'); }
      return '/ot/api/repuestos/bodega-buscar?' + p.toString();
    }
    async function buscar() {
      clearTimeout(st.timer);
      avisoBox.innerHTML = '';
      if (!hayCriterio()) {
        st.items = [];
        pintarHint();
        if (st.onResultados) st.onResultados(st.items);
        return;
      }
      var mio = ++st.req;
      resBox.innerHTML = '<div class="rpb-vacio"><span class="spinner-border spinner-border-sm me-1"></span> Buscando en la bodega…</div>';
      var j;
      try {
        var r = await fetch(urlBusqueda(), { credentials: 'same-origin', headers: st.headers });
        j = await r.json();
      } catch (e) { j = { ok: false }; }
      if (mio !== st.req) return; // llegó una búsqueda más nueva
      if (!j || !j.ok) {
        resBox.innerHTML = '<div class="rpb-vacio rpb-error"><i class="bi bi-wifi-off"></i>' + esc((j && j.error) || 'No se pudo buscar en la bodega. Intenta de nuevo.') + '</div>';
        return;
      }
      st.items = j.repuestos || [];
      st.modeloCtx = j.modelo ? j.modelo.id : null;
      if (j.compatibles_sin_modelo && st.equipo && !st.modelo) {
        // El equipo elegido no tiene modelo en el catálogo: no hay cómo
        // filtrar compatibles. Se avisa y se cae a "toda la bodega".
        st.sinModelo = true;
        st.soloCompat = false;
        pintarCtx();
        avisoBox.innerHTML = '<div class="rpb-nota ambar"><i class="bi bi-info-circle-fill"></i>El equipo <b>' + esc(st.equipo.nombre || '') + '</b>' +
          (st.equipo.sku ? ' (SKU ' + esc(st.equipo.sku) + ')' : '') + ' no tiene modelo en el Catálogo, así que no se pueden buscar compatibles: se muestra toda la bodega.</div>';
        if (!hayCriterio()) { st.items = []; pintarHint(); if (st.onResultados) st.onResultados(st.items); return; }
        return buscar();
      }
      if (j.truncado) {
        avisoBox.innerHTML = '<div class="rpb-nota"><i class="bi bi-funnel-fill"></i>Se muestran los primeros ' + st.items.length + ' resultados: afina la búsqueda con texto, marca' + (st.mostrarProveedor ? ', proveedor' : '') + ' o modelo.</div>';
      }
      pintarResultados();
      if (st.onResultados) st.onResultados(st.items);
    }

    function tarjetaHTML(it) {
      var enLista = Number(st.enLista(it.id) || 0);
      var compat = !!it.es_compatible && !!st.modeloCtx;
      var clases = 'rpb-card' + (compat ? ' compat' : '') + (st.seleccionId === it.id ? ' sel' : '') + (enLista ? ' enlista' : '');
      var foto = it.foto_url ? '<img src="' + esc(it.foto_url) + '" alt="" loading="lazy">' : '<i class="bi bi-gear"></i>';
      var meta = [];
      if (it.sku) meta.push('<span class="rpb-sku">' + esc(it.sku) + '</span>');
      if (it.marca) meta.push('<span><i class="bi bi-tag"></i>' + esc(it.marca) + '</span>');
      if (it.codigo_fabricante) meta.push('<span title="Código de fabricante">cód. ' + esc(it.codigo_fabricante) + '</span>');
      var tags = proveedorBadge(it);
      if (it.ubicacion_codigo) tags += '<span class="rpb-tag ubi"><i class="bi bi-geo-alt"></i>' + esc(it.ubicacion_codigo) + '</span>';
      if (compat) tags += '<span class="rpb-tag compat"><i class="bi bi-check2-circle"></i>Compatible con ' + esc((st.modelo && (st.modelo.nombre || st.modelo.sku)) || (st.equipo && st.equipo.nombre) || 'el modelo') + '</span>';
      var lado = '<div class="rpb-lado">' + semaforoHTML(it);
      if (st.modo === 'agregar') {
        lado += '<div class="rpb-add">' +
          '<input type="number" class="rpb-cant" inputmode="decimal" min="0.5" step="0.5" value="1" aria-label="Cantidad">' +
          '<button type="button" class="rpb-btn-add" data-id="' + it.id + '"><i class="bi bi-plus-lg"></i>' + (enLista ? 'Sumar' : 'Agregar') + '</button>' +
          '</div>' +
          (enLista ? '<span class="rpb-enlista"><i class="bi bi-check-circle-fill"></i>En la lista ×' + fmt(enLista) + '</span>' : '');
      } else if (st.seleccionId === it.id) {
        lado += '<span class="rpb-enlista"><i class="bi bi-check-circle-fill"></i>Elegido</span>';
      }
      lado += '</div>';
      var inner =
        '<div class="rpb-foto">' + foto + '</div>' +
        '<div class="rpb-info">' +
          '<div class="rpb-nombre">' + esc(it.descripcion || '') + '</div>' +
          (meta.length ? '<div class="rpb-meta">' + meta.join('<span class="rpb-dot">·</span>') + '</div>' : '') +
          (tags ? '<div class="rpb-tags">' + tags + '</div>' : '') +
          modelosHTML(it, st.modeloCtx) +
        '</div>' + lado;
      if (st.modo === 'seleccion') {
        return '<button type="button" class="' + clases + '" data-rpb-id="' + it.id + '">' + inner + '</button>';
      }
      return '<div class="' + clases + '" data-rpb-id="' + it.id + '">' + inner + '</div>';
    }
    function pintarHint() {
      resBox.innerHTML = '<div class="rpb-vacio"><i class="bi bi-search"></i>Escribe al menos 2 letras (nombre, SKU, marca, modelo' + (st.mostrarProveedor ? ' o proveedor' : '') + ') o usa un filtro.</div>';
    }
    function pintarResultados() {
      if (!st.items.length) {
        // Sin criterio de búsqueda no hay "nada encontrado": hay que decir
        // qué escribir (refrescar() desde afuera no debe pisar la pista).
        if (!hayCriterio()) { pintarHint(); return; }
        var que = st.q.length >= 2 ? ' con "' + esc(st.q) + '"' : '';
        resBox.innerHTML = '<div class="rpb-vacio"><i class="bi bi-inbox"></i>Nada en la bodega' + que +
          (st.modelo || (st.equipo && st.soloCompat && !st.sinModelo) ? ' compatible con ese modelo. Prueba "Toda la bodega", otro texto, o escríbelo manual.' : '. Prueba con otro texto o filtro, o escríbelo manual.') + '</div>';
        return;
      }
      resBox.innerHTML = '<div class="rpb-grid">' + st.items.map(tarjetaHTML).join('') + '</div>';
    }
    function itemPorId(id) {
      for (var i = 0; i < st.items.length; i++) if (Number(st.items[i].id) === Number(id)) return st.items[i];
      return null;
    }
    resBox.addEventListener('click', function (e) {
      var badd = e.target.closest('.rpb-btn-add');
      if (badd) {
        var it = itemPorId(badd.dataset.id);
        if (!it) return;
        var card = badd.closest('.rpb-card');
        var inp = card ? card.querySelector('.rpb-cant') : null;
        var cant = inp ? parseFloat(String(inp.value).replace(',', '.')) : 1;
        if (!isFinite(cant) || cant <= 0) {
          if (global.ilusToast) ilusToast('Indica una cantidad válida (mayor que cero).', { type: 'warning' });
          if (inp) inp.focus();
          return;
        }
        if (st.onAgregar) st.onAgregar(it, cant);
        if (inp) inp.value = '1';
        pintarResultados();
        return;
      }
      if (st.modo === 'seleccion') {
        var card2 = e.target.closest('.rpb-card[data-rpb-id]');
        if (!card2) return;
        var it2 = itemPorId(card2.dataset.rpbId);
        if (!it2) return;
        st.seleccionId = it2.id;
        if (st.onSeleccionar) st.onSeleccionar(it2, card2);
        pintarResultados();
      }
    });
    // Enter en la cantidad = Agregar (sin tener que llegar al botón).
    resBox.addEventListener('keydown', function (e) {
      if (e.key !== 'Enter' || !e.target.classList.contains('rpb-cant')) return;
      e.preventDefault();
      var card = e.target.closest('.rpb-card');
      var b = card && card.querySelector('.rpb-btn-add');
      if (b) b.click();
    });

    /* ── eventos de la barra ─────────────────────────────────────────── */
    inQ.addEventListener('input', function () {
      st.q = (inQ.value || '').trim();
      clearTimeout(st.timer);
      // Al vaciar el texto se re-consulta igual (REGLA #4.3: al limpiar un
      // filtro la lista se recarga, no se queda con el resultado anterior).
      st.timer = setTimeout(buscar, 260);
    });
    inQ.addEventListener('keydown', function (e) { if (e.key === 'Enter') { e.preventDefault(); st.q = (inQ.value || '').trim(); buscar(); } });
    $('.rpb-q-x').addEventListener('click', function () { inQ.value = ''; st.q = ''; buscar(); inQ.focus(); });
    if (selProv) selProv.addEventListener('change', function () { st.proveedor = selProv.value; buscar(); });
    selMarca.addEventListener('change', function () { st.marca = selMarca.value; buscar(); });
    $('.rpb-limpiar').addEventListener('click', function () { limpiar(true); });

    function limpiar(mantenerEquipo) {
      st.q = ''; inQ.value = '';
      st.proveedor = ''; if (selProv) selProv.value = '';
      st.marca = ''; selMarca.value = '';
      st.modelo = null; inModelo.value = ''; dropModelo.style.display = 'none';
      st.seleccionId = null;
      if (!mantenerEquipo) { st.equipo = null; st.sinModelo = false; }
      st.soloCompat = opts.compatPorDefecto !== false;
      pintarCtx();
      buscar();
    }

    pintarCtx();
    buscar();

    /* ── API pública ─────────────────────────────────────────────────── */
    return {
      buscar: buscar,
      limpiar: function () { limpiar(false); },
      refrescar: pintarResultados,
      getItems: function () { return st.items.slice(); },
      focus: function () { try { inQ.focus(); } catch (e) {} },
      setEquipo: function (eq) {
        st.equipo = eq || null;
        st.sinModelo = false;
        st.soloCompat = opts.compatPorDefecto !== false;
        pintarCtx();
        buscar();
      },
      setProveedores: function (lista) { st.provs = Array.isArray(lista) ? lista : []; pintarProvs(); },
      setSeleccion: function (id) { st.seleccionId = (id == null ? null : Number(id)); pintarResultados(); },
      getEstado: function () { return { q: st.q, proveedor: st.proveedor, marca: st.marca, modelo: st.modelo, equipo: st.equipo, soloCompat: st.soloCompat }; }
    };
  }

  global.RepBuscador = { crear: crear, esc: esc, fmt: fmt, canalChip: canalChip, semaforoHTML: semaforoHTML, CANAL: CANAL };
})(window);

/* ══════════════════════════════════════════════════════════════════════════════════════════════
   CONTROL DE CORREOS — pestaña de Comunicaciones (Daniel, 2026-10-20)
   «Una pestaña donde veo TODOS los correos que el sistema envía, puedo APAGAR cada uno (con motivo),
   ver su estado real, la vista previa, mandarme una prueba y ver cuántos salieron o se frenaron.»

   Datos: GET /api/comunicaciones/correos (el registro + el estado real + las cifras de 30 días).
   Cambios: PUT /api/comunicaciones/correos/<clave> {activo, motivo, estado_visto}.
   Solo APAGA: un correo que apaga una variable de entorno o el código no se puede encender desde aquí.
   Reglas del proyecto: sin alert/confirm/prompt nativos (usa ilusPrompt/ilusConfirm/ilusAlert/ilusToast, REGLA #1) ·
   seleccionar/deseleccionar todo (REGLA #14) · tabla paginada 25/50/100 (REGLA #4.3) · fechas ya vienen en hora Chile (REGLA #6).
   Prefijo «ctc» (ids, clases y funciones) para no chocar con «cc…» de la identidad de empresa del otro tab.
   ══════════════════════════════════════════════════════════════════════════════════════════════ */
(function () {
  'use strict';

  var PER_OK = [25, 50, 100];
  var S = {
    datos: null, porClave: {}, page: 1, per: 25, sel: new Set(), seq: 0, cargadoOk: false,
    filtros: { modulo: '', dest: '', estado: '', modo: '', q: '' }, previa: null, cambiosAbiertos: new Set(),
    ocupados: new Set(), debounce: null
  };
  try {
    var pp = parseInt(localStorage.getItem('commCorreosPerPage') || '', 10);
    if (PER_OK.indexOf(pp) >= 0) S.per = pp;                       // el tamaño de página es del usuario (REGLA #4.3)
  } catch (_e) { /* almacenamiento bloqueado: se usa 25 */ }

  function $(id) { return document.getElementById(id); }
  function esc(s) {
    return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
    });
  }
  function fmt(n) { return Number(n || 0).toLocaleString('es-CL'); }
  function toast(m, t) { if (typeof ilusToast === 'function') ilusToast(m, { type: t || 'info' }); }

  var ICONO_DEST = {
    cliente: 'bi-person', tecnico: 'bi-wrench-adjustable', responsable_ot: 'bi-person-badge', bodega: 'bi-box-seam',
    proveedor_courier: 'bi-truck', interno: 'bi-people', usuario: 'bi-person-circle'
  };

  /* ─────────────────────────────── carga ─────────────────────────────── */
  function setCargando(on) {
    var el = $('ctcCargando'); if (el) el.classList.toggle('d-none', !on);
    var l = $('ctcLista'); if (l) l.classList.toggle('ctc-cargando-lista', !!on && !!S.datos);
  }

  function aviso(msg) {
    var el = $('ctcError'); if (!el) return;
    el.innerHTML = msg ? '<div class="ctc-aviso error" role="alert"><i class="bi bi-exclamation-octagon-fill"></i><div>' + esc(msg) + '</div></div>' : '';
  }

  async function cargar(opts) {
    opts = opts || {};
    var seq = ++S.seq;
    setCargando(true);
    try {
      var r = await fetch('/api/comunicaciones/correos' + (opts.fresco ? '?fresco=1' : ''),
        { cache: 'no-store', headers: { 'Accept': 'application/json' } });
      var d = null;
      try { d = await r.json(); } catch (_e) { d = null; }
      if (seq !== S.seq) return;                                    // llegó tarde: ya hay una consulta más nueva
      if (!r.ok || !d || !d.ok) throw new Error((d && d.error) || 'El servidor no respondió como se esperaba.');
      S.datos = d; S.porClave = {};
      d.correos.forEach(function (c) { S.porClave[c.clave] = c; });
      S.sel.forEach(function (k) { if (!S.porClave[k]) S.sel.delete(k); });
      S.cargadoOk = true;
      aviso('');
      render();
    } catch (e) {
      if (seq !== S.seq) return;
      aviso((S.cargadoOk ? 'No se pudo actualizar: ' : 'No se pudo cargar el control de correos: ') + (e && e.message ? e.message : e));
    } finally {
      if (seq === S.seq) setCargando(false);
    }
  }

  /* ─────────────────────────────── filtros ─────────────────────────────── */
  function coincideEstado(c, f) {
    var e = c.estado;
    if (f === 'sale') return e.efectivo === 'sale';
    if (f === 'apagado') return !e.switch_activo && !c.protegido;
    if (f === 'config') return e.efectivo === 'config' || e.efectivo === 'codigo';
    if (f === 'frenado') return e.efectivo === 'general' || e.efectivo === 'llave';
    if (f === 'frenados30') return (c.cifras.frenados || 0) > 0;
    return true;
  }

  function visibles() {
    if (!S.datos) return [];
    var f = S.filtros, q = (f.q || '').trim().toLowerCase();
    var ordMod = {}, ordDest = {};
    S.datos.modulos.forEach(function (m, i) { ordMod[m.clave] = i; });
    S.datos.destinatarios.forEach(function (d, i) { ordDest[d.clave] = i; });
    var lista = S.datos.correos.map(function (c, i) { return [c, i]; }).filter(function (par) {
      var c = par[0];
      if (f.modulo && c.modulo_ui !== f.modulo) return false;
      if (f.dest && c.destinatario !== f.dest) return false;
      if (f.modo && !(c.modo === f.modo || c.modo === 'mixto')) return false;
      if (f.estado && !coincideEstado(c, f.estado)) return false;
      if (q) {
        var hay = [c.nombre, c.clave, c.disparo, c.avisa_a, c.llave_label, c.destinatario_label, c.nota,
          c.plantilla ? c.plantilla.estado : ''].join(' ').toLowerCase();
        if (hay.indexOf(q) < 0) return false;
      }
      return true;
    });
    lista.sort(function (a, b) {
      return ((ordMod[a[0].modulo_ui] || 0) - (ordMod[b[0].modulo_ui] || 0)) ||
        ((ordDest[a[0].destinatario] || 0) - (ordDest[b[0].destinatario] || 0)) || (a[1] - b[1]);
    });
    return lista.map(function (x) { return x[0]; });
  }

  function hayFiltros() {
    var f = S.filtros; return !!(f.modulo || f.dest || f.estado || f.modo || (f.q || '').trim());
  }

  function pintarFiltros() {
    var f = S.filtros;
    var set = function (id, v) { var el = $(id); if (el && el.value !== v) el.value = v || ''; };
    set('ctcFModulo', f.modulo); set('ctcFDest', f.dest); set('ctcFEstado', f.estado); set('ctcFModo', f.modo); set('ctcFQ', f.q);
    var m = $('ctcFModulo');
    if (m && S.datos && m.options.length <= 1) {
      S.datos.modulos.forEach(function (x) { var o = document.createElement('option'); o.value = x.clave; o.textContent = x.nombre; m.appendChild(o); });
      m.value = f.modulo || '';
    }
    var d = $('ctcFDest');
    if (d && S.datos && d.options.length <= 1) {
      S.datos.destinatarios.forEach(function (x) { var o = document.createElement('option'); o.value = x.clave; o.textContent = x.nombre; d.appendChild(o); });
      d.value = f.dest || '';
    }
  }

  /* ─────────────────────────────── render ─────────────────────────────── */
  function render() {
    if (!S.datos) return;
    pintarFiltros();
    renderAvisos(); renderResumen(); renderLista(); renderHerramientas();
  }

  function renderAvisos() {
    var a = (S.datos.avisos || []).map(function (x) {
      return '<div class="ctc-aviso" role="status"><i class="bi bi-exclamation-triangle-fill"></i><div>' + esc(x.texto) + '</div></div>';
    }).join('');
    $('ctcAvisos').innerHTML = a;
  }

  function renderResumen() {
    var r = S.datos.resumen, f = S.filtros.estado;
    var ficha = function (cls, filtro, n, txt) {
      return '<button type="button" class="ctc-ficha ' + cls + (f === filtro && filtro ? ' activa' : '') + '" data-ctc="ficha" data-filtro="' + filtro + '"' +
        ' aria-pressed="' + (f === filtro && filtro ? 'true' : 'false') + '"><b>' + fmt(n) + '</b><span>' + txt + '</span></button>';
    };
    $('ctcResumen').innerHTML =
      ficha('', '', r.total, 'Correos en total') +
      ficha('ok', 'sale', r.salen, 'Salen hoy') +
      ficha('am', 'apagado', r.apagados_usuario, 'Apagados aquí por una persona') +
      ficha('gr', 'config', r.apagados_config, 'Apagados por configuración o código') +
      ficha('az', 'frenados30', r.frenados_30d, 'Frenados en los últimos ' + (S.datos.ventana_dias || 30) + ' días');
    var d = $('ctcDesde');
    if (d) {
      d.textContent = !S.datos.cifras_disponibles
        ? 'Las cifras de envíos aún no están disponibles (la base se está preparando). Los interruptores funcionan igual.'
        : (S.datos.contando_desde
          ? 'Las cifras cuentan desde el ' + S.datos.contando_desde + ' (no se rellenan envíos anteriores).'
          : '');
    }
  }

  function tags(c) {
    var t = '<span class="ctc-tag dest"><i class="bi ' + (ICONO_DEST[c.destinatario] || 'bi-person') + '"></i>' + esc(c.destinatario_label) + '</span>';
    var auto = c.modo === 'automatico' ? '<i class="bi bi-robot"></i>Automático'
      : (c.modo === 'manual' ? '<i class="bi bi-hand-index-thumb"></i>Manual' : '<i class="bi bi-robot"></i>Automático y manual');
    t += '<span class="ctc-tag auto">' + auto + '</span>';
    t += '<span class="ctc-tag"><i class="bi bi-key"></i>Llave: ' + esc(c.llave_label) + '</span>';
    if (c.protegido) t += '<span class="ctc-tag prot"><i class="bi bi-lock-fill"></i>Correo de seguridad</span>';
    if (c.sensible) t += '<span class="ctc-tag sens"><i class="bi bi-shield-exclamation"></i>Autorizaciones</span>';
    if (c.tipo === 'externo') t += '<span class="ctc-tag ter"><i class="bi bi-box-arrow-up-right"></i>Lo envía un tercero</span>';
    if (c.tambien_whatsapp) t += '<span class="ctc-tag"><i class="bi bi-whatsapp"></i>También va por WhatsApp</span>';
    return t;
  }

  function pillTxt(c) {
    switch (c.estado.efectivo) {
      case 'sale': return '<i class="bi bi-check-circle-fill"></i>Sale';
      case 'correo': return '<i class="bi bi-pause-circle-fill"></i>Apagado aquí';
      case 'llave': return '<i class="bi bi-key-fill"></i>Frenado: llave de ' + esc(c.llave_label);
      case 'general': return '<i class="bi bi-power"></i>Frenado: interruptor general';
      case 'config': return '<i class="bi bi-gear-fill"></i>Apagado por configuración';
      case 'codigo': return '<i class="bi bi-code-slash"></i>Apagado en el código';
    }
    return '';
  }

  function textoPaso(c, p) {
    if (p.estado === 'saltado') return 'No se revisa';
    switch (p.clave) {
      case 'general': return p.estado === 'ok' ? 'Encendido' : 'Apagado';
      case 'llave': return p.estado === 'ok' ? 'Abierta' : 'Cerrada';
      case 'correo': return p.estado === 'protegido' ? 'Seguridad: sin interruptor' : (p.estado === 'ok' ? 'Encendido' : 'Apagado aquí');
      case 'config': return p.estado === 'ok' ? 'En regla' : (c.estado.apagado_en_codigo ? 'Apagado en el código' : 'Apagado por configuración');
    }
    return '';
  }

  var NOMBRE_PASO = { general: 'Interruptor general', llave: 'Llave del módulo', correo: 'Este correo', config: 'Configuración' };

  function pasos(c) {
    var e = c.estado;
    return '<ol class="ctc-pasos" aria-label="Por dónde pasa este correo antes de salir">' + e.pasos.map(function (p) {
      var ic = p.estado === 'ok' ? '<i class="bi bi-check-lg"></i>'
        : p.estado === 'apagado' ? '<i class="bi bi-pause-fill"></i>'
        : p.estado === 'config' ? (e.apagado_en_codigo ? '<i class="bi bi-code-slash"></i>' : '<i class="bi bi-gear-fill"></i>')
        : p.estado === 'protegido' ? '<i class="bi bi-lock-fill"></i>' : String(p.n);
      var frena = e.frena_en === p.n && (p.estado === 'apagado' || p.estado === 'config');
      return '<li class="ctc-paso ' + p.estado + (frena ? ' frena' : '') + '">' +
        '<span class="ctc-pn">Paso ' + p.n + '</span><span class="ctc-pc" aria-hidden="true">' + ic + '</span>' +
        '<span class="ctc-pt n">' + esc(p.clave === 'llave' ? 'Llave de ' + c.llave_label : NOMBRE_PASO[p.clave]) + '</span>' +
        '<span class="ctc-pt s">' + esc(textoPaso(c, p)) + '</span>' +
        (frena ? '<span class="frena-txt">Frena aquí</span>' : '') + '</li>';
    }).join('') + '</ol>';
  }

  function lineaEstado(c) {
    var e = c.estado, t;
    switch (e.efectivo) {
      case 'sale': t = c.protegido ? 'Correo de seguridad: sale siempre y no se puede apagar desde aquí.' : 'Hoy este correo sale cuando ocurre lo que lo dispara.'; break;
      case 'correo': t = 'Está apagado: no sale aunque pase lo que lo dispara. Cada intento queda en el Historial como «Apagado».'; break;
      case 'llave': t = 'La llave de paso de ' + c.llave_label + ' está cerrada (pestaña Plantillas): ningún correo de ese módulo sale.' +
        (e.llave_motivo ? ' Motivo: ' + e.llave_motivo : ''); break;
      case 'general': t = 'El interruptor general de correos está apagado: no sale ningún correo.'; break;
      case 'codigo': t = 'Apagado en el código.' + (c.nota ? ' ' + c.nota : ''); break;
      default:
        t = e.condiciones.filter(function (x) { return x.alcance === 'todo' && !x.activa; }).map(function (x) { return x.texto; }).join(' ') ||
          'Apagado por configuración.';
    }
    // El interruptor de ESTE correo puede estar encendido y aun así no salir: lo dice, para que «Encendido» no confunda.
    if ((e.efectivo === 'config' || e.efectivo === 'codigo') && e.switch_activo) {
      t += ' El interruptor de este correo está encendido, pero igual no sale: lo frena eso.';
    }
    return '<p class="ctc-estado-linea ' + e.efectivo + '">' + esc(t) + '</p>';
  }

  function condiciones(c) {
    var todas = c.estado.condiciones || [];
    if (!todas.length) return '';
    return '<div class="ctc-conds">' + todas.map(function (x) {
      var on = !!x.activa;
      return '<div class="ctc-cond ' + (on ? 'on' : 'off') + '"><i class="bi ' + (on ? 'bi-check-circle-fill' : 'bi-dash-circle-fill') + '"></i>' +
        '<span>' + (x.variable ? '<code>' + esc(x.variable) + '</code> · ' : '') + esc(x.texto) + '</span></div>';
    }).join('') + '</div>';
  }

  function plantilla(c) {
    var p = c.plantilla;
    if (!p) return '<div class="ctc-aviso-plantilla info"><i class="bi bi-code-square"></i><span>Texto en el código: este correo no tiene plantilla en el editor.</span></div>';
    if (!p.existe) return '<div class="ctc-aviso-plantilla"><i class="bi bi-exclamation-triangle-fill"></i><span>Su plantilla aún no está guardada en el editor: sale con el texto del código.</span></div>';
    if (!p.activo) return '<div class="ctc-aviso-plantilla"><i class="bi bi-exclamation-triangle-fill"></i><span>La plantilla está <b>apagada</b> en el editor. ' + esc(p.texto_apagada) + '</span></div>';
    return '';
  }

  function cifras(c) {
    var k = c.cifras || {};
    if (!S.datos.cifras_disponibles) return '';
    return '<div class="ctc-cifras" aria-label="Últimos ' + (S.datos.ventana_dias || 30) + ' días">' +
      '<span class="c" style="color:#6b7280">Últimos ' + (S.datos.ventana_dias || 30) + ' días</span>' +
      '<span class="c ok"><i class="bi bi-send-check"></i>Enviados <b>' + fmt(k.enviados) + '</b></span>' +
      '<span class="c fr"><i class="bi bi-sign-stop"></i>Frenados <b>' + fmt(k.frenados) + '</b></span>' +
      '<span class="c fa"><i class="bi bi-x-circle"></i>Fallidos <b>' + fmt(k.fallidos) + '</b></span>' +
      '<span class="ult">' + (k.ultimo ? 'Último envío: ' + esc(k.ultimo) : 'Sin envíos registrados') + '</span></div>';
  }

  function cambioLinea(c) {
    var e = c.estado;
    if (!e.apagado_por && !e.apagado_en) return '';
    return '<p class="ctc-cambio">Cambiado por <b>' + esc(e.apagado_por || '—') + '</b>' + (e.apagado_en ? ' el ' + esc(e.apagado_en) : '') +
      (e.switch_activo ? ' (lo volvió a encender)' : '') + (e.motivo ? ' · Motivo: ' + esc(e.motivo) : '') + '</p>';
  }

  function interruptor(c) {
    var e = c.estado, ocupado = S.ocupados.has(c.clave);
    if (c.protegido) {
      return '<span class="ctc-bloqueado-msg"><i class="bi bi-lock-fill"></i>Correo de seguridad: no se apaga aquí</span>';
    }
    var on = e.switch_activo;
    var deshab = ocupado || (!on && !e.puede_encender);
    var tip = (!on && !e.puede_encender)
      ? 'No se puede encender desde aquí: lo apaga la configuración o el código'
      : (on ? 'Apagar este correo' : 'Encender este correo');
    return '<button type="button" class="ctc-switch" role="switch" aria-checked="' + (on ? 'true' : 'false') + '" data-ctc="interruptor"' +
      ' data-clave="' + esc(c.clave) + '"' + (deshab ? ' disabled' : '') + ' title="' + esc(tip) + '"' +
      ' aria-label="' + esc((on ? 'Apagar ' : 'Encender ') + c.nombre) + '">' +
      '<span class="perilla"></span><span>' + (on ? 'Encendido' : 'Apagado') + '</span></button>';
  }

  function tarjeta(c) {
    var e = c.estado, sel = S.sel.has(c.clave);
    var h = '<article class="ctc-tarjeta est-' + e.efectivo + '" id="ctc-' + esc(c.clave) + '" data-clave="' + esc(c.clave) + '">';
    h += '<div class="ctc-th">' +
      '<label class="ctc-chk" title="Seleccionar este correo"><input type="checkbox" data-ctc="sel" data-clave="' + esc(c.clave) + '"' + (sel ? ' checked' : '') +
      ' aria-label="Seleccionar ' + esc(c.nombre) + '"></label>' +
      '<div class="ctc-tit"><h6 class="ctc-nombre">' + esc(c.nombre) + '</h6><div class="ctc-tags">' + tags(c) + '</div></div>' +
      '<span class="ctc-pill est-' + e.efectivo + '">' + pillTxt(c) + '</span></div>';
    h += '<p class="ctc-disparo"><b>Cuándo sale:</b> ' + esc(c.disparo) + '</p>';
    var meta = [];
    if (c.avisa_a) meta.push('<b>A quién:</b> ' + esc(c.avisa_a));
    if (c.adjunta) meta.push('<b>Adjunta:</b> ' + esc(c.adjunta));
    if (meta.length) h += '<p class="ctc-meta">' + meta.join(' · ') + '</p>';
    if (c.nota) h += '<p class="ctc-nota">' + esc(c.nota) + '</p>';
    h += pasos(c) + lineaEstado(c) + condiciones(c) + plantilla(c);
    if (c.tambien_whatsapp) h += '<p class="ctc-nota"><i class="bi bi-whatsapp"></i> Esto apaga solo el correo: el aviso por WhatsApp no cambia.</p>';
    h += cifras(c) + cambioLinea(c);
    h += '<div id="ctcCambios-' + esc(c.clave) + '"></div>';
    h += '<div class="ctc-acciones">';
    h += c.plantilla && c.plantilla.existe
      ? '<button type="button" class="ctc-btn" data-ctc="plantilla" data-clave="' + esc(c.clave) + '"><i class="bi bi-pencil-square"></i>Editar plantilla</button>'
      : '<span class="ctc-btn" style="cursor:default;opacity:.75" title="Su texto está en el código"><i class="bi bi-code-square"></i>Texto en el código</span>';
    h += '<button type="button" class="ctc-btn" data-ctc="previa" data-clave="' + esc(c.clave) + '"><i class="bi bi-eye"></i>Vista previa</button>';
    h += '<button type="button" class="ctc-btn" data-ctc="prueba" data-clave="' + esc(c.clave) + '"><i class="bi bi-send"></i>Enviarme una prueba</button>';
    h += '<button type="button" class="ctc-btn" data-ctc="historial" data-clave="' + esc(c.clave) + '"><i class="bi bi-list-ul"></i>Ver en historial</button>';
    h += '<button type="button" class="ctc-btn" data-ctc="cambios" data-clave="' + esc(c.clave) + '" aria-expanded="' + (S.cambiosAbiertos.has(c.clave) ? 'true' : 'false') + '"><i class="bi bi-clock-history"></i>Cambios</button>';
    h += '<span class="ctc-spacer"></span>' + interruptor(c) + '</div>';
    return h + '</article>';
  }

  function renderLista() {
    var lista = visibles(), total = lista.length;
    var pages = Math.max(1, Math.ceil(total / S.per));
    if (S.page > pages) S.page = pages;
    if (S.page < 1) S.page = 1;
    var ini = (S.page - 1) * S.per, pagina = lista.slice(ini, ini + S.per);
    S.pagina = pagina;
    var mods = {}, dests = {};
    S.datos.modulos.forEach(function (m) { mods[m.clave] = m; });
    S.datos.destinatarios.forEach(function (d) { dests[d.clave] = d; });
    var conteoMod = {};
    lista.forEach(function (c) { conteoMod[c.modulo_ui] = (conteoMod[c.modulo_ui] || 0) + 1; });
    var h = '', modAct = null, destAct = null;
    pagina.forEach(function (c) {
      if (c.modulo_ui !== modAct) {
        modAct = c.modulo_ui; destAct = null;
        var m = mods[modAct] || { nombre: modAct, icono: 'bi-envelope', descripcion: '' };
        h += '<div class="ctc-grupo"><span class="ic"><i class="bi ' + esc(m.icono) + '"></i></span><div><h5>' + esc(m.nombre) + '</h5><p>' + esc(m.descripcion) +
          '</p></div><span class="n">' + fmt(conteoMod[modAct] || 0) + (conteoMod[modAct] === 1 ? ' correo' : ' correos') + '</span></div>';
      }
      if (c.destinatario !== destAct) {
        destAct = c.destinatario;
        h += '<div class="ctc-subgrupo"><i class="bi ' + (ICONO_DEST[destAct] || 'bi-person') + '"></i>Para: ' + esc((dests[destAct] || {}).nombre || destAct) + '</div>';
      }
      h += tarjeta(c);
    });
    if (!pagina.length) {
      h = '<div class="ctc-vacio"><i class="bi bi-envelope-x"></i><div class="t">' +
        (hayFiltros() ? 'Ningún correo coincide con los filtros' : 'No hay correos para mostrar') + '</div>' +
        (hayFiltros() ? '<button type="button" class="ctc-btn mt-2" data-ctc="limpiar"><i class="bi bi-arrow-counterclockwise"></i>Ver todos los correos</button>' : '') + '</div>';
    }
    $('ctcLista').innerHTML = h;
    // Pie (REGLA #4.3): Mostrando A–B de N · N por página · Anterior / Página X de Y / Siguiente
    var put = function (id, v) { var el = $(id); if (el) el.textContent = v; };
    put('ctcDesdeN', fmt(total ? ini + 1 : 0)); put('ctcHastaN', fmt(ini + pagina.length)); put('ctcTotalN', fmt(total));
    put('ctcPagAct', fmt(S.page)); put('ctcPagTot', fmt(pages));
    var prev = $('ctcPrev'), next = $('ctcNext'), per = $('ctcPerPage');
    if (prev) prev.disabled = S.page <= 1;
    if (next) next.disabled = S.page >= pages;
    if (per) per.value = String(S.per);
    renderBarra(lista);
    S.cambiosAbiertos.forEach(function (k) { if (S.porClave[k]) cargarCambios(k, true); });
  }

  function renderBarra(lista) {
    var pag = S.pagina || [];
    var marcadasPag = pag.filter(function (c) { return S.sel.has(c.clave); }).length;
    var info = $('ctcSelInfo');
    if (info) info.textContent = S.sel.size ? fmt(S.sel.size) + (S.sel.size === 1 ? ' correo seleccionado' : ' correos seleccionados') : 'Ninguno seleccionado';
    var b = $('ctcBtnTodos');
    if (b) {
      var todas = pag.length > 0 && marcadasPag === pag.length;
      b.innerHTML = '<i class="bi bi-check-all"></i>' + (todas ? 'Deseleccionar todo' : 'Seleccionar todo');
      b.disabled = !pag.length;
    }
    var bf = $('ctcBtnFiltrados');
    if (bf) { bf.classList.toggle('d-none', !(lista.length > pag.length)); bf.textContent = 'Seleccionar los ' + fmt(lista.length) + ' filtrados'; }
    ['ctcBtnApagarSel', 'ctcBtnEncenderSel'].forEach(function (id) { var x = $(id); if (x) x.disabled = !S.sel.size; });
  }

  function renderHerramientas() {
    var h = $('ctcHerramientas'); if (!h) return;
    var hs = S.datos.herramientas || [];
    h.innerHTML = hs.length ? '<div class="ctc-herr"><b>Herramientas que no se apagan aquí:</b> ' + hs.map(function (x) {
      return esc(x.nombre) + (x.nota ? ' (' + esc(x.nota) + ')' : '');
    }).join(' · ') + '</div>' : '';
  }

  /* ─────────────────────────────── cambiar el interruptor ─────────────────────────────── */
  /* Las fichas de arriba se recalculan con lo que ya se sabe (las cifras de 30 días no cambian por un clic). */
  function recalcular() {
    var cs = S.datos.correos, r = S.datos.resumen;
    r.total = cs.length;
    r.salen = cs.filter(function (c) { return c.estado.efectivo === 'sale'; }).length;
    r.apagados_usuario = cs.filter(function (c) { return !c.estado.switch_activo && !c.protegido; }).length;
    r.apagados_config = cs.filter(function (c) { return c.estado.efectivo === 'config' || c.estado.efectivo === 'codigo'; }).length;
    r.frenados_llave = cs.filter(function (c) { return c.estado.efectivo === 'general' || c.estado.efectivo === 'llave'; }).length;
  }

  function mensajeApagar(c) {
    var partes = [];
    if (c.destinatario === 'cliente') partes.push('Los clientes dejarán de recibir este correo.');
    else if (c.destinatario === 'tecnico') partes.push('Los técnicos dejarán de recibir este aviso.');
    else if (c.destinatario === 'responsable_ot') partes.push('El responsable de la OT dejará de recibir este aviso.');
    else if (c.destinatario === 'bodega') partes.push('Bodega dejará de recibir este aviso.');
    else if (c.destinatario === 'proveedor_courier') partes.push('Los proveedores o couriers dejarán de recibir este correo.');
    else partes.push('Dejará de llegar este correo.');
    if (c.sensible) partes.push('La bandeja sigue funcionando; solo deja de llegar el correo.');
    if (c.tambien_whatsapp) partes.push('Esto apaga solo el correo; WhatsApp no cambia.');
    if (c.tipo === 'externo') partes.push('ILUS dejará de entregar el correo del destinatario a quien envía el aviso.');
    return partes.join(' ');
  }

  async function cambiar(c, activo, motivo) {
    S.ocupados.add(c.clave); renderLista();
    try {
      var r = await fetch('/api/comunicaciones/correos/' + encodeURIComponent(c.clave), {
        method: 'PUT', headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' },
        body: JSON.stringify({ activo: !!activo, motivo: motivo || '', estado_visto: c.estado.version })
      });
      var d = null;
      try { d = await r.json(); } catch (_e) { d = null; }
      if (r.ok && d && d.ok) {
        if (!d.correo) { cargar({ fresco: true }); return { ok: true }; }       // quedó guardado pero el servidor no pudo releerlo: se recarga la lista
        S.porClave[c.clave] = d.correo;
        S.datos.correos = S.datos.correos.map(function (x) { return x.clave === c.clave ? d.correo : x; });
        recalcular();
        return { ok: true };
      }
      if (r.status === 409 && d && d.error_codigo === 'CAMBIO_AJENO') cargar({ fresco: true });      // otra persona ya lo cambió
      return { ok: false, codigo: d && d.error_codigo, error: (d && d.error) || ('Error ' + r.status) };
    } catch (e) {
      return { ok: false, error: 'No se pudo conectar con el servidor — ' + (e && e.message ? e.message : e) };
    } finally {
      S.ocupados.delete(c.clave);
    }
  }

  async function alternar(clave) {
    var c = S.porClave[clave];
    if (!c || c.protegido || S.ocupados.has(clave)) return;
    if (c.estado.switch_activo) {
      var motivo = await ilusPrompt({
        title: 'Apagar «' + c.nombre + '»', message: mensajeApagar(c),
        placeholder: 'Motivo (opcional, ej.: «pruebas hasta el viernes»)', required: false, okLabel: 'Apagar correo', type: 'danger'
      });
      if (motivo === null) return;                                   // canceló
      var r = await cambiar(c, false, motivo);
      if (r.ok) { toast('Correo apagado: ' + c.nombre + ' · se aplica en todo el sistema en menos de 30 segundos', 'warning'); render(); }
      else { toast(r.error, 'error'); render(); }
    } else {
      if (!c.estado.puede_encender) {
        await ilusAlert({
          title: 'No se puede encender desde aquí',
          message: 'Este correo está apagado por la configuración o por el código. La pantalla solo puede apagar.',
          sub: 'Encenderlo se hace con la variable de entorno y la autorización explícita de Daniel.', type: 'warning'
        });
        return;
      }
      var ok = await ilusConfirm({
        title: 'Encender «' + c.nombre + '»',
        message: c.destinatario === 'cliente' ? 'Vuelve a llegarle a clientes reales.' : 'Este aviso vuelve a enviarse.',
        sub: c.estado.efectivo === 'llave' || c.estado.efectivo === 'general'
          ? 'Ojo: la llave de paso o el interruptor general siguen frenando los correos de este módulo.' : '',
        okLabel: 'Encender', cancelLabel: 'Cancelar'
      });
      if (!ok) return;
      var r2 = await cambiar(c, true, '');
      if (r2.ok) { toast('Correo encendido: ' + c.nombre + ' · se aplica en todo el sistema en menos de 30 segundos', 'success'); render(); }
      else { toast(r2.error, 'error'); render(); }
    }
  }

  async function masivo(activo) {
    var claves = Array.from(S.sel).filter(function (k) { return S.porClave[k]; });
    if (!claves.length) return;
    var objetivo = claves.map(function (k) { return S.porClave[k]; });
    var saltados = objetivo.filter(function (c) {
      return c.protegido || (activo ? (!c.estado.puede_encender) : false) || c.estado.switch_activo === !!activo;
    });
    var aplicar = objetivo.filter(function (c) { return saltados.indexOf(c) < 0; });
    var protegidos = saltados.filter(function (c) { return c.protegido; }).length;
    var candado = saltados.filter(function (c) { return !c.protegido && activo && !c.estado.puede_encender; }).length;
    var igual = saltados.length - protegidos - candado;
    var nota = saltados.length
      ? ' Se saltan ' + saltados.length + ': ' + [protegidos ? protegidos + ' de seguridad' : '', candado ? candado + ' con candado de configuración' : '',
        igual ? igual + (igual === 1 ? ' que ya estaba así' : ' que ya estaban así') : ''].filter(Boolean).join(', ') + '.' : '';
    if (!aplicar.length) {
      await ilusAlert({ title: 'Nada que cambiar', message: 'Ninguno de los seleccionados se puede ' + (activo ? 'encender' : 'apagar') + ' desde aquí.' + nota, type: 'info' });
      return;
    }
    var motivo = '';
    if (!activo) {
      motivo = await ilusPrompt({
        title: 'Apagar ' + aplicar.length + (aplicar.length === 1 ? ' correo' : ' correos'),
        message: 'Dejarán de salir los ' + aplicar.length + ' seleccionados.' + nota,
        placeholder: 'Motivo (opcional, queda en cada uno)', required: false, okLabel: 'Apagar seleccionados', type: 'danger'
      });
      if (motivo === null) return;
    } else {
      var ok = await ilusConfirm({
        title: 'Encender ' + aplicar.length + (aplicar.length === 1 ? ' correo' : ' correos'),
        message: aplicar.some(function (c) { return c.destinatario === 'cliente'; }) ? 'Vuelven a llegarle a clientes reales.' : 'Estos avisos vuelven a enviarse.',
        sub: nota.trim(), okLabel: 'Encender seleccionados', cancelLabel: 'Cancelar'
      });
      if (!ok) return;
    }
    var hechos = 0, fallos = [];
    for (var i = 0; i < aplicar.length; i++) {
      var c = S.porClave[aplicar[i].clave] || aplicar[i];            // la versión más nueva de cada uno
      var r = await cambiar(c, activo, motivo);
      if (r.ok) hechos++; else fallos.push(c.nombre + ': ' + r.error);
    }
    S.sel.clear();
    render();
    if (fallos.length) {
      await ilusAlert({ title: 'Se cambiaron ' + hechos + ' de ' + aplicar.length, message: fallos.slice(0, 6).join('\n'), type: 'warning' });
    } else {
      toast((activo ? 'Encendidos: ' : 'Apagados: ') + hechos + (saltados.length ? ' · saltados: ' + saltados.length : '') +
        ' · se aplica en todo el sistema en menos de 30 segundos', activo ? 'success' : 'warning');
    }
  }

  /* ─────────────────────────────── vista previa y prueba ─────────────────────────────── */
  async function previa(clave) {
    var c = S.porClave[clave]; if (!c) return;
    var modalEl = $('modalCtcPrevia'); if (!modalEl || typeof bootstrap === 'undefined') return;
    S.previa = c;
    $('ctcPreviaTitulo').textContent = c.nombre;
    $('ctcPreviaInfo').innerHTML = '<span class="spinner-border spinner-border-sm me-2"></span>Preparando la vista previa…';
    $('ctcPreviaCuerpo').innerHTML = '';
    var m = bootstrap.Modal.getOrCreateInstance(modalEl); m.show();
    try {
      var r = await fetch('/api/comunicaciones/correos/' + encodeURIComponent(clave) + '/vista-previa', {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}'
      });
      var d = null; try { d = await r.json(); } catch (_e) { d = null; }
      if (!r.ok || !d || !d.ok) throw new Error((d && d.error) || 'No se pudo preparar la vista previa.');
      if (d.sin_plantilla) {
        $('ctcPreviaInfo').textContent = 'Datos de mentira · este correo no usa una plantilla del editor';
        $('ctcPreviaCuerpo').innerHTML = '<div class="ctc-previa-vacia"><i class="bi bi-code-square" style="font-size:2rem"></i>' +
          '<p class="mt-2 mb-3">' + esc(d.mensaje) + '</p>' +
          '<button type="button" class="ctc-btn primario" data-ctc="historial-desde-previa" data-clave="' + esc(clave) + '"><i class="bi bi-list-ul"></i>Ver el último enviado</button></div>';
      } else {
        $('ctcPreviaInfo').innerHTML = '<b>Asunto:</b> ' + esc(d.asunto) +
          '<div class="small text-muted mt-1">Con datos de mentira: no lee ni envía nada real.' +
          (d.aviso_plantilla_apagada ? ' <b style="color:#92400e">La plantilla está apagada en el editor. ' + esc(d.aviso_plantilla_apagada) + '</b>' : '') + '</div>';
        $('ctcPreviaCuerpo').innerHTML = '<iframe title="Vista previa del correo" sandbox="" referrerpolicy="no-referrer"></iframe>';
        $('ctcPreviaCuerpo').querySelector('iframe').srcdoc = d.html;
      }
    } catch (e) {
      $('ctcPreviaInfo').innerHTML = '<span style="color:#b91c1c">' + esc(e.message || e) + '</span>';
    }
  }

  async function prueba(clave, btn) {
    var c = S.porClave[clave]; if (!c) return;
    var txt = btn ? btn.innerHTML : '';
    if (btn) { btn.disabled = true; btn.innerHTML = '<span class="spinner-border spinner-border-sm"></span>Enviando…'; }
    try {
      var r = await fetch('/api/comunicaciones/correos/' + encodeURIComponent(clave) + '/prueba', {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}'
      });
      var d = null; try { d = await r.json(); } catch (_e) { d = null; }
      if (r.ok && d && d.ok) toast('✓ Prueba enviada a ' + d.enviado_a + ' (asunto con «[PRUEBA]»)', 'success');
      else await ilusAlert({ title: 'No se envió la prueba', message: (d && d.error) || ('Error ' + r.status), type: 'warning' });
    } catch (e) {
      toast('No se pudo conectar con el servidor — ' + (e && e.message ? e.message : e), 'error');
    } finally {
      if (btn) { btn.disabled = false; btn.innerHTML = txt; }
    }
  }

  async function cargarCambios(clave, silencioso) {
    var cont = $('ctcCambios-' + clave); if (!cont) return;
    if (!silencioso) cont.innerHTML = '<div class="ctc-cambios"><span class="spinner-border spinner-border-sm me-2"></span>Cargando…</div>';
    try {
      var r = await fetch('/api/comunicaciones/correos/' + encodeURIComponent(clave) + '/cambios', { cache: 'no-store' });
      var d = null; try { d = await r.json(); } catch (_e) { d = null; }
      if (!r.ok || !d || !d.ok) throw new Error((d && d.error) || 'No se pudieron leer los cambios.');
      cont.innerHTML = '<div class="ctc-cambios"><b>Últimos cambios del interruptor</b>' + (d.cambios.length ? d.cambios.map(function (x) {
        return '<div class="fila"><span>' + esc(x.fecha) + '</span><span><b>' + esc(x.usuario || '—') + '</b> ' +
          (x.despues ? 'lo encendió' : 'lo apagó') + (x.motivo ? ' · ' + esc(x.motivo) : '') + '</span></div>';
      }).join('') : '<div class="fila text-muted">Nadie lo ha tocado: sale como siempre.</div>') + '</div>';
    } catch (e) {
      cont.innerHTML = '<div class="ctc-cambios" style="color:#b91c1c">' + esc(e.message || e) + '</div>';
    }
  }

  function alternarCambios(clave) {
    if (S.cambiosAbiertos.has(clave)) {
      S.cambiosAbiertos.delete(clave);
      var cont = $('ctcCambios-' + clave); if (cont) cont.innerHTML = '';
      var b = document.querySelector('#ctc-' + clave + ' [data-ctc="cambios"]'); if (b) b.setAttribute('aria-expanded', 'false');
    } else {
      S.cambiosAbiertos.add(clave);
      var b2 = document.querySelector('#ctc-' + clave + ' [data-ctc="cambios"]'); if (b2) b2.setAttribute('aria-expanded', 'true');
      cargarCambios(clave);
    }
  }

  /* ─────────────────────────────── navegación hacia otras pestañas ─────────────────────────────── */
  function irAHistorial(clave) {
    var m = bootstrap.Modal.getInstance($('modalCtcPrevia')); if (m) m.hide();
    if (typeof histAbrirDesdeCorreo === 'function') histAbrirDesdeCorreo(clave);
  }

  function irAPlantilla(clave) {
    var c = S.porClave[clave]; if (!c || !c.plantilla) return;
    if (typeof tplAbrirEstado === 'function') tplAbrirEstado(c.plantilla.modulo, c.plantilla.estado);
  }

  /* Desde el Historial: abre esta pestaña, limpia los filtros y deja a la vista la tarjeta de ese correo */
  function irATarjeta(clave) {
    var btn = document.querySelector('[data-bs-target="#tabCorreos"]');
    var enfocar = function () {
      S.filtros = { modulo: '', dest: '', estado: '', modo: '', q: '' };
      var lista = visibles(), idx = lista.findIndex(function (x) { return x.clave === clave; });
      if (idx < 0) return;
      S.page = Math.floor(idx / S.per) + 1;
      render();
      var el = $('ctc-' + clave);
      if (el) { el.scrollIntoView({ behavior: 'smooth', block: 'center' }); el.classList.add('resaltada'); setTimeout(function () { el.classList.remove('resaltada'); }, 2000); }
    };
    if (btn) btn.click();
    if (S.datos) setTimeout(enfocar, 60);
    else cargar().then(enfocar);
  }

  /* ─────────────────────────────── eventos ─────────────────────────────── */
  function setSel(clave, on) { if (on) S.sel.add(clave); else S.sel.delete(clave); }

  function alternarTodos() {
    var pag = S.pagina || [];
    var hayDesmarcada = pag.some(function (c) { return !S.sel.has(c.clave); });        // REGLA #14: un solo toggle
    pag.forEach(function (c) { setSel(c.clave, hayDesmarcada); });                   // la misma acción que cada casilla
    renderLista();
  }

  function limpiarFiltros() {
    S.filtros = { modulo: '', dest: '', estado: '', modo: '', q: '' };
    S.page = 1;
    pintarFiltros();
    cargar({ fresco: true });                                                          // REGLA #4.3: al limpiar un filtro se recarga
  }

  function filtro(campo, valor) {
    S.filtros[campo] = valor; S.page = 1;
    if (S.datos) { renderResumen(); renderLista(); }
  }

  document.addEventListener('click', function (ev) {
    var el = ev.target.closest('#tabCorreos [data-ctc], #modalCtcPrevia [data-ctc]');
    if (!el || el.tagName === 'INPUT') return;
    var acc = el.dataset.ctc, clave = el.dataset.clave;
    switch (acc) {
      case 'interruptor': alternar(clave); break;
      case 'previa': previa(clave); break;
      case 'prueba': prueba(clave, el); break;
      case 'plantilla': irAPlantilla(clave); break;
      case 'historial': irAHistorial(clave); break;
      case 'historial-desde-previa': irAHistorial(clave); break;
      case 'cambios': alternarCambios(clave); break;
      case 'ficha':
        var f = el.dataset.filtro || '';
        S.filtros.estado = (S.filtros.estado === f) ? '' : f; S.page = 1; render(); break;
      case 'todos': alternarTodos(); break;
      case 'filtrados':
        visibles().forEach(function (c) { setSel(c.clave, true); }); renderLista(); break;
      case 'apagar-sel': masivo(false); break;
      case 'encender-sel': masivo(true); break;
      case 'limpiar': limpiarFiltros(); break;
      case 'refrescar': cargar({ fresco: true }); break;
      case 'filtros-abrir':
        var fl = $('ctcFiltros'); if (fl) { fl.classList.toggle('abiertos'); el.setAttribute('aria-expanded', fl.classList.contains('abiertos') ? 'true' : 'false'); }
        break;
      case 'prev': if (S.page > 1) { S.page--; renderLista(); } break;
      case 'next': S.page++; renderLista(); break;
      case 'prueba-previa': if (S.previa) prueba(S.previa.clave, el); break;
    }
  });

  document.addEventListener('change', function (ev) {
    var t = ev.target;
    if (t.matches('#tabCorreos input[data-ctc="sel"]')) { setSel(t.dataset.clave, t.checked); renderBarra(visibles()); return; }
    if (t.id === 'ctcFModulo') filtro('modulo', t.value);
    else if (t.id === 'ctcFDest') filtro('dest', t.value);
    else if (t.id === 'ctcFEstado') { filtro('estado', t.value); }
    else if (t.id === 'ctcFModo') filtro('modo', t.value);
    else if (t.id === 'ctcPerPage') {
      var n = parseInt(t.value, 10);
      if (PER_OK.indexOf(n) >= 0) {
        S.per = n; S.page = 1;
        try { localStorage.setItem('commCorreosPerPage', String(n)); } catch (_e) { /* sin almacenamiento: no se recuerda */ }
        renderLista();
      }
    }
  });

  document.addEventListener('input', function (ev) {
    if (ev.target.id === 'ctcFQ') {
      clearTimeout(S.debounce);
      var v = ev.target.value;
      S.debounce = setTimeout(function () { filtro('q', v); }, 250);                // también al BORRAR el texto
    }
  });

  /* Primera carga: al abrir la pestaña (siempre se vuelve a leer: lo que se cerró en Plantillas se ve al volver) */
  (function init() {
    var btn = document.querySelector('[data-bs-target="#tabCorreos"]');
    if (btn) btn.addEventListener('shown.bs.tab', function () { cargar(); });
    document.addEventListener('DOMContentLoaded', function () {
      if ((window.location.hash || '').indexOf('#tabCorreos') === 0 && btn) btn.click();
      else { var pane = $('tabCorreos'); if (pane && pane.classList.contains('active')) cargar(); }
    });
  })();

  window.ctcIrATarjeta = irATarjeta;
  window.ctcRecargar = function () { return cargar({ fresco: true }); };
})();

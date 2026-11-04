/* Solicitudes de vehículo · Flota — pestaña VEHÍCULOS: tabla inteligente con foto y calidad de la ficha, ficha con los documentos y su PDF,
   alta y edición, habilitar / inhabilitar, archivar y borrar (el superadministrador o quien tenga la llave «administrar»).
   Reglas del proyecto que cumple: sin alert/confirm/prompt nativos (REGLA #1: ilusConfirm / ilusPrompt / ilusToast), tabla paginada con pie
   «Mostrando A–B de N» (REGLA #4.3), campos que pasan de rojo a verde mientras se escribe y al guardar se alerta, no se traba (REGLA #42),
   datos completos sin cortar (REGLA #15) y celular primero (REGLA #3).
   Quién ve qué lo decide el SERVIDOR (flota_reglas.niveles); acá solo se esconden los botones que no corresponden (window.FLOTA_PERMISOS).
   Todo lo que valida acá se vuelve a validar allá (flota_reglas.validar_vehiculo). La patente puede escribirse con su dígito verificador
   (TVDZ88-5): el servidor la separa en patente y patente_dv. */
(function () {
  'use strict';

  var API = '/flota/api/vehiculos';
  var API_ARCH = '/flota/api/archivos';
  var PERM = window.FLOTA_PERMISOS || {};
  var S = {
    vehiculos: [], estado: 'todos', q: '', orden: 'prioridad', pagina: 1, porPagina: 25, resumen: {},
    hoy: null, editando: null, fichaId: null, intento: false, guardando: false, verArchivados: false, subiendo: false,
    config: { tipos: [], combustibles: [], clases: [], clase_sugerida: 'B', dias_alerta: 30 }
  };
  var tocados = {};
  var DOCS = [
    { clave: 'revision_tecnica', campo: 'revision_tecnica_vence', detalle: 'revision_tecnica_detalle', nombre: 'Revisión técnica' },
    { clave: 'permiso_circulacion', campo: 'permiso_circulacion_vence', detalle: 'permiso_circulacion_detalle', nombre: 'Permiso de circulación' },
    { clave: 'soap', campo: 'soap_vence', detalle: 'soap_detalle', nombre: 'SOAP' },
    { clave: 'seguro', campo: 'seguro_vence', detalle: 'seguro_detalle', nombre: 'Seguro' }
  ];
  var TEXTOS = ['patente', 'tipo', 'marca', 'modelo', 'anio', 'color', 'vin', 'numero_motor', 'cilindrada', 'propietario', 'propietario_rut',
    'capacidad_kg', 'capacidad_m3', 'combustible', 'estanque_l', 'km_actual',
    'revision_tecnica_vence', 'permiso_circulacion_vence', 'soap_vence', 'seguro_vence',
    'revision_tecnica_detalle', 'permiso_circulacion_detalle', 'soap_detalle', 'seguro_detalle', 'notas'];
  var NOMBRES = { patente: 'la patente', tipo: 'el tipo de vehículo', licencia_clases: 'la clase de licencia' };
  var ADJ_COMBUSTIBLE = { bencina: 'bencinero', gasolina: 'bencinero', 'diésel': 'diésel', diesel: 'diésel', 'eléctrico': 'eléctrico',
    electrico: 'eléctrico', 'híbrido': 'híbrido', hibrido: 'híbrido', gas: 'a gas' };

  function $(id) { return document.getElementById(id); }
  function esc(s) {
    return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
    });
  }
  function num(n) { return n == null ? '—' : Number(n).toLocaleString('es-CL'); }
  function toast(msg, type) {
    if (typeof window.ilusToast === 'function') window.ilusToast(msg, { type: type || 'info' });
  }
  function conGuion(p) {
    if (/^[A-Z]{2}\d{4}$/.test(p)) return p.slice(0, 2) + '-' + p.slice(2);
    if (/^[A-Z]{4}\d{2}$/.test(p)) return p.slice(0, 4) + '-' + p.slice(4);
    return p;
  }
  function normPat(v) { return String(v || '').toUpperCase().replace(/[^A-Z0-9]/g, ''); }
  /* 'TVDZ88-5' → {base:'TVDZ88', dv:'5'} (mismo cálculo que flota_reglas.separar_patente) */
  function sepPat(v) {
    var up = String(v || '').toUpperCase().trim();
    var m = /^([A-Z]{2,4}[\s.\-]*\d{2,4})\s*-\s*([0-9K])$/.exec(up);
    return m ? { base: normPat(m[1]), dv: m[2] } : { base: normPat(up), dv: null };
  }
  function norm(t) {
    var s = String(t == null ? '' : t).toLowerCase();
    if (s.normalize) s = s.normalize('NFD').replace(/[̀-ͯ]/g, '');
    return s.replace(/[^a-z0-9]/g, '');
  }
  function plural(n, uno, varios) { return n + ' ' + (n === 1 ? uno : varios); }
  function rutValido(t) {
    var r = String(t || '').replace(/[^0-9kK]/g, '').toUpperCase();
    if (!/^\d{7,8}[0-9K]$/.test(r)) return false;
    var cuerpo = r.slice(0, -1), dv = r.slice(-1), suma = 0, f = 2;
    for (var i = cuerpo.length - 1; i >= 0; i--) { suma += parseInt(cuerpo.charAt(i), 10) * f; f = f === 7 ? 2 : f + 1; }
    var e = 11 - (suma % 11);
    return dv === (e === 11 ? '0' : (e === 10 ? 'K' : String(e)));
  }

  function api(url, opts) {
    opts = opts || {};
    var init = { method: opts.method || 'GET', headers: { Accept: 'application/json' }, credentials: 'same-origin' };
    if (opts.body !== undefined) {
      init.headers['Content-Type'] = 'application/json';
      init.body = JSON.stringify(opts.body);
    }
    return fetch(url, init).then(function (r) {
      return r.json().catch(function () { return { ok: false, error: 'El servidor respondió algo inesperado.' }; })
        .then(function (d) { d.status = r.status; return d; });
    }).catch(function () {
      return { ok: false, status: 0, error: 'No hay conexión con el servidor. Revisa tu internet e intenta de nuevo.' };
    });
  }
  function subir(entidad, id, tipo, archivo) {
    var fd = new FormData();
    fd.append('entidad', entidad); fd.append('entidad_id', id); fd.append('tipo', tipo); fd.append('archivo', archivo);
    return fetch(API_ARCH, { method: 'POST', body: fd, credentials: 'same-origin', headers: { Accept: 'application/json' } }).then(function (r) {
      return r.json().catch(function () { return { ok: false, error: 'El servidor respondió algo inesperado.' }; })
        .then(function (d) { d.status = r.status; return d; });
    }).catch(function () {
      return { ok: false, status: 0, error: 'No hay conexión con el servidor. Revisa tu internet e intenta de nuevo.' };
    });
  }
  function despuesDeCerrar(idModal, fn) {
    var el = $(idModal);
    var inst = window.bootstrap.Modal.getInstance(el);
    if (!inst || !el.classList.contains('show')) { fn(); return; }
    el.addEventListener('hidden.bs.modal', function h() { el.removeEventListener('hidden.bs.modal', h); fn(); });
    inst.hide();
  }

  /* ── Fechas (mismo cálculo que el servidor: hoy viene del servidor, en hora Chile) ── */
  function aFecha(iso) {
    var m = /^(\d{4})-(\d{2})-(\d{2})/.exec(iso || '');
    return m ? new Date(Number(m[1]), Number(m[2]) - 1, Number(m[3])) : null;
  }
  function diasHasta(iso) {
    var f = aFecha(iso), h = aFecha(S.hoy);
    if (!f || !h) return null;
    return Math.round((f - h) / 86400000);
  }
  function textoDias(d) {
    if (d === 0) return 'Vence hoy';
    if (d === 1) return 'Vence mañana';
    if (d === -1) return 'Venció ayer';
    return d > 1 ? 'Vence en ' + d + ' días' : 'Venció hace ' + (-d) + ' días';
  }

  /* ═══ Carga y lista ═══ */
  function cargar() {
    return api(API + (S.verArchivados ? '?archivados=1' : '')).then(function (d) {
      if (!d.ok) { toast(d.error || 'No se pudo leer la flota.', 'error'); pintarVacio('No se pudo leer la flota', d.error || 'Intenta de nuevo en un momento.'); return; }
      S.vehiculos = d.vehiculos || [];
      S.hoy = d.hoy || null;
      if (d.config) S.config = d.config;
      if (d.permisos) PERM = d.permisos;
      S.resumen = d.resumen || {};
      var r = S.resumen;
      $('flKpiHabilitados').textContent = r.habilitados != null ? r.habilitados : '–';
      $('flKpiInhabilitados').textContent = r.inhabilitados != null ? r.inhabilitados : '–';
      $('flKpiPorVencer').textContent = r.por_vencer != null ? r.por_vencer : '–';
      $('flKpiVencidos').textContent = r.vencidos != null ? r.vencidos : '–';
      $('flKpiIncompletos').textContent = r.incompletos != null ? r.incompletos : '–';
      $('flKpiIncompletosCaja').hidden = !PERM.documentos;
      $('flChipIncompletos').hidden = !PERM.documentos;
      var ca = $('flChipArchivados');
      ca.hidden = !PERM.admin;
      ca.textContent = 'Archivados' + (r.archivados ? ' (' + r.archivados + ')' : '');
      pintarLista();
      if (S.fichaId != null && $('flModalFicha').classList.contains('show')) {
        var v = buscar(S.fichaId);
        if (v) pintarFicha(v);
      }
      if (S.editando && $('flModalVeh').classList.contains('show')) {     // PDF subido desde el formulario: se ve al tiro
        var ve = buscar(S.editando.id);
        if (ve) { S.editando = ve; pintarPdfsForm(ve); }
      }
    });
  }
  function buscar(id) {
    var r = null;
    S.vehiculos.forEach(function (v) { if (String(v.id) === String(id)) r = v; });
    return r;
  }

  var PESO_DOCS = { vencido: 0, por_vencer: 1, incompleto: 2, vigente: 3 };
  function claveVence(v) { return v.proximo_vencimiento ? v.proximo_vencimiento.vence_fmt.split('/').reverse().join('') : '99999999'; }
  function ordenar(lista) {
    var o = S.orden;
    return lista.slice().sort(function (a, b) {
      if (o === 'patente') return a.patente.localeCompare(b.patente);
      if (o === 'vencimiento') return claveVence(a).localeCompare(claveVence(b)) || a.patente.localeCompare(b.patente);
      var pa = (a.habilitado ? 0 : 10) + (PESO_DOCS[a.docs_estado] != null ? PESO_DOCS[a.docs_estado] : 3);
      var pb = (b.habilitado ? 0 : 10) + (PESO_DOCS[b.docs_estado] != null ? PESO_DOCS[b.docs_estado] : 3);
      return pa - pb || claveVence(a).localeCompare(claveVence(b)) || a.patente.localeCompare(b.patente);
    });
  }
  function filtrados() {
    var q = norm(S.q);
    var lista = S.vehiculos.filter(function (v) {
      if (S.estado === 'alertas' && v.docs_estado !== 'vencido' && v.docs_estado !== 'por_vencer') return false;
      if (S.estado === 'incompletos' && !(v.calidad && v.calidad.pct < 100)) return false;
      if (S.estado === 'inhabilitados' && v.habilitado) return false;
      if (!q) return true;
      return norm([v.patente, v.patente_completa, v.tipo, v.marca, v.modelo, v.titulo, v.propietario, v.numero_motor, v.vin, v.color].join(' ')).indexOf(q) >= 0;
    });
    return ordenar(lista);
  }

  function pintarVacio(titulo, texto) {
    var v = $('flVacio');
    v.innerHTML = '<b>' + esc(titulo) + '</b>' + esc(texto || '');
    v.hidden = false;
    $('flTabla').style.display = 'none';
    $('flPie').style.display = 'none';
  }

  function adjCombustible(c) {
    var k = String(c || '').toLowerCase().trim();
    return ADJ_COMBUSTIBLE[k] || c || '';
  }
  /* «Año 2025 · Furgón bencinero · Motor 1.2 L · Color blanco» */
  function lineaMeta(v) {
    var p = [];
    if (v.anio) p.push('Año ' + v.anio);
    var t = [v.tipo, adjCombustible(v.combustible)].filter(Boolean).join(' ');
    if (t) p.push(t);
    if (v.cilindrada) p.push('Motor ' + v.cilindrada);
    if (v.color) p.push('Color ' + String(v.color).toLowerCase());
    return p.join(' · ');
  }
  function tituloVeh(v) {
    var nombre = [v.marca, v.modelo].filter(Boolean).join(' ') || v.tipo || '';
    return v.patente_completa + (nombre ? ' — ' + nombre : '');
  }
  function fotoHtml(v, grande) {
    return v.foto_id
      ? '<img src="' + API_ARCH + '/' + v.foto_id + '" alt="Foto del vehículo ' + esc(v.patente_fmt) + '" loading="lazy">'
      : '<i class="bi bi-truck-front" aria-hidden="true"></i>' + (grande ? '<span>Sin foto</span>' : '');
  }

  function celdaDocs(v) {
    var cuenta = { vigente: 0, por_vencer: 0, vencido: 0, sin_fecha: 0 };
    var segs = v.documentos.map(function (d) {
      cuenta[d.estado] = (cuenta[d.estado] || 0) + 1;
      var tit = d.nombre + ': ' + (d.estado === 'sin_fecha' ? 'sin fecha registrada' : d.vence_fmt + ' · ' + d.texto) + (d.tiene_pdf ? ' · con PDF' : '');
      return '<i class="fl-seg fl-seg--' + esc(d.estado) + '" title="' + esc(tit) + '" aria-label="' + esc(tit) + '"></i>';
    }).join('');
    var partes = [];
    if (cuenta.vencido) partes.push('<b class="fl-t-bad">' + plural(cuenta.vencido, 'vencido', 'vencidos') + '</b>');
    if (cuenta.por_vencer) partes.push('<b class="fl-t-warn">' + plural(cuenta.por_vencer, 'por vencer', 'por vencer') + '</b>');
    if (cuenta.vigente) partes.push('<span class="fl-t-ok">' + plural(cuenta.vigente, 'al día', 'al día') + '</span>');
    if (cuenta.sin_fecha) partes.push('<span class="fl-t-mute">' + plural(cuenta.sin_fecha, 'sin fecha', 'sin fecha') + '</span>');
    var pv = v.proximo_vencimiento;
    var prox = pv ? '<span class="fl-dprox fl-dprox--' + esc(pv.estado) + '">Lo próximo: <b>' + esc(pv.nombre) + '</b> · ' + esc(pv.vence_fmt) + ' · ' + esc(pv.texto.toLowerCase()) + '</span>'
      : '<span class="fl-dprox fl-dprox--sin_fecha">Ningún documento tiene fecha registrada</span>';
    return '<div class="fl-dsem" role="img" aria-label="Estado de los cuatro documentos">' + segs + '</div><span class="fl-dres">' +
      partes.join(' · ') + '</span>' + prox;
  }
  function celdaCalidad(v) {
    var c = v.calidad;
    if (!c) return '<span class="fl-t-mute">—</span>';
    var cls = c.pct >= 100 ? 'ok' : (c.pct >= 60 ? 'warn' : 'bad');
    var falta = c.faltan.length
      ? '<span class="fl-falta">Falta: ' + esc(c.faltan.slice(0, 2).join(', ')) + (c.faltan.length > 2 ? ' y ' + (c.faltan.length - 2) + ' más' : '') + '</span>'
      : '<span class="fl-falta fl-falta--ok">Ficha completa</span>';
    return '<div class="fl-calidad"><span class="fl-barra fl-barra--' + cls + '" role="img" aria-label="Ficha al ' + c.pct + ' por ciento"><i style="width:' + c.pct + '%"></i></span><b>' + c.pct + '%</b></div>' + falta;
  }
  function celdaEstado(v) {
    if (v.archivado) {
      return '<span class="fl-estado fl-estado--off"><i class="bi bi-archive"></i>Archivado</span><span class="fl-motivo">' + esc(v.archivado_motivo || '') +
        (v.archivado_por ? ' · ' + esc(v.archivado_por) : '') + (v.archivado_fmt ? ' · ' + esc(v.archivado_fmt) : '') + '</span>';
    }
    return v.habilitado
      ? '<span class="fl-estado fl-estado--on"><i class="bi bi-check-circle-fill"></i>Habilitado</span>'
      : '<span class="fl-estado fl-estado--off"><i class="bi bi-slash-circle"></i>Inhabilitado</span>' +
        '<span class="fl-motivo">' + esc(v.inhabilitado_motivo || '') + (v.inhabilitado_por ? ' · ' + esc(v.inhabilitado_por) : '') +
        (v.inhabilitado_fmt ? ' · ' + esc(v.inhabilitado_fmt) : '') + '</span>';
  }
  function bloqueDoc(d) {
    var linea = d.estado === 'sin_fecha' ? 'Sin fecha registrada' : d.vence_fmt;
    var sub = d.estado === 'sin_fecha' ? '' : d.texto;
    var pdf = d.tiene_pdf ? '<span class="fl-vd-pdf fl-vd-pdf--ok"><i class="bi bi-file-earmark-check"></i>PDF guardado</span>'
      : '<span class="fl-vd-pdf"><i class="bi bi-file-earmark-x"></i>Sin PDF</span>';
    return '<div class="fl-vd fl-vd--' + esc(d.estado) + '"><span class="fl-vd-n">' + esc(d.nombre) + '</span>' +
      '<span class="fl-vd-f"><i class="fl-pto fl-pto--doc-' + esc(d.estado) + '"></i>' + esc(linea) + '</span>' +
      (sub ? '<span class="fl-vd-t">' + esc(sub) + '</span>' : '') + pdf + '</div>';
  }
  function filaVeh(v) {
    var linea2 = [];
    if (v.color) linea2.push('Color ' + String(v.color).toLowerCase());
    linea2.push('Licencia ' + v.licencia_texto.replace(/^Clase/, 'clase'));
    linea2.push(v.km_actual == null ? 'Sin kilometraje registrado' : num(v.km_actual) + ' km');
    var acc = '<button type="button" class="fl-btn fl-btn--primario" data-acc="ficha"><i class="bi bi-card-checklist"></i>Ver ficha</button>';
    if (PERM.admin) acc += '<button type="button" class="fl-btn" data-acc="editar"><i class="bi bi-pencil"></i>Editar</button>';
    if (PERM.gestionar) acc += '<button type="button" class="fl-btn" data-acc="historial"><i class="bi bi-clock-history"></i>Historial</button>';
    var meta = [v.anio ? 'Año ' + v.anio : null, [v.tipo, adjCombustible(v.combustible)].filter(Boolean).join(' '),
      v.cilindrada ? 'Motor ' + v.cilindrada : null].filter(Boolean).join(' · ');
    var cal = v.calidad ? '<div class="fl-vtar-cal">' + celdaCalidad(v) + '</div>' : '';
    return '<article class="fl-vtar' + (v.habilitado && !v.archivado ? '' : ' inhab') + '" role="listitem" data-id="' + v.id + '">' +
      '<div class="fl-vtar-top"><div class="fl-vtar-foto">' + fotoHtml(v, true) + (v.foto_referencial ? '<span class="fl-vtar-ref">Foto referencial</span>' : '') + '</div>' +
      '<div class="fl-vtar-info"><h3 class="fl-vtar-tit">' + esc(tituloVeh(v)) + '</h3>' +
      (meta ? '<p class="fl-vtar-meta">' + esc(meta) + '</p>' : '') +
      '<p class="fl-vtar-l2">' + esc(linea2.join(' · ')) + '</p>' +
      (v.propietario ? '<p class="fl-vtar-prop">Propietario registrado: <b>' + esc(v.propietario) + '</b></p>' : '') +
      '<div class="fl-vtar-pie"><div class="fl-vtar-estado">' + celdaEstado(v) + '</div>' + cal + '<div class="fl-acc">' + acc + '</div></div>' +
      '</div></div>' +
      '<div class="fl-vtar-docs" aria-label="Documentos">' + v.documentos.map(bloqueDoc).join('') + '</div></article>';
  }
  function pintarLista() {
    var lista = filtrados();
    if (!S.vehiculos.length) {
      pintarVacio(S.verArchivados ? 'No hay vehículos archivados' : 'Aún no hay vehículos registrados',
        PERM.admin && !S.verArchivados ? 'Agrega el primero con «Agregar vehículo». Si compran más, se ingresan igual desde acá.' : '');
      return;
    }
    if (!lista.length) {
      pintarVacio('Ningún vehículo coincide', 'Prueba con otra búsqueda o cambia el filtro.');
      return;
    }
    $('flVacio').hidden = true;
    $('flTabla').style.display = '';
    $('flPie').style.display = '';
    var paginas = Math.max(1, Math.ceil(lista.length / S.porPagina));
    if (S.pagina > paginas) S.pagina = paginas;
    var ini = (S.pagina - 1) * S.porPagina;
    var pag = lista.slice(ini, ini + S.porPagina);
    $('flCuerpo').innerHTML = pag.map(filaVeh).join('');
    $('flPieInfo').textContent = 'Mostrando ' + (ini + 1) + '–' + (ini + pag.length) + ' de ' + lista.length;
    $('flPaginaN').textContent = 'Página ' + S.pagina + ' de ' + paginas;
    $('flAnterior').disabled = S.pagina <= 1;
    $('flSiguiente').disabled = S.pagina >= paginas;
  }

  /* ═══ Ficha del vehículo ═══ */
  function dato(et, val) {
    return '<div class="fl-dato' + (val ? '' : ' fl-dato--vacio') + '"><span>' + esc(et) + '</span><b>' + esc(val || 'Sin registrar') + '</b></div>';
  }
  function pillDoc(d) {
    var t = { vigente: 'Al día', por_vencer: 'Por vencer', vencido: 'Vencido', sin_fecha: 'Sin fecha' }[d.estado] || d.estado;
    return '<span class="fl-doc-pill fl-doc-pill--' + esc(d.estado) + '">' + esc(t) + '</span>';
  }
  function celdaPdf(v, d) {
    var admin = PERM.admin;
    if (!PERM.documentos) {
      return d.tiene_pdf ? '<span class="fl-t-ok"><i class="bi bi-file-earmark-check"></i> PDF guardado</span><span class="fl-sub">Solo lo ven quienes tienen el permiso de documentos</span>'
        : '<span class="fl-t-mute">Sin PDF</span>';
    }
    var a = d.archivo;
    if (a) {
      return '<a class="fl-pdf" href="' + API_ARCH + '/' + a.id + '" target="_blank" rel="noopener"><i class="bi bi-file-earmark-pdf"></i>Ver PDF</a>' +
        '<span class="fl-sub">' + esc(a.nombre || '') + '</span><span class="fl-sub">Subido por ' + esc(a.subido_por || '—') + ' · ' + esc(a.subido_fmt || '') + '</span>' +
        (admin ? '<div class="fl-mini-acc"><button type="button" class="fl-btn fl-btn--chico" data-farch="subir" data-tipo="' + esc(d.clave) + '"><i class="bi bi-arrow-repeat"></i>Reemplazar</button>' +
          '<button type="button" class="fl-btn fl-btn--chico fl-btn--peligro" data-farch="quitar" data-aid="' + a.id + '"><i class="bi bi-x-lg"></i>Quitar</button></div>' : '');
    }
    return '<span class="fl-sin-pdf"><i class="bi bi-file-earmark-x"></i>Sin PDF</span>' +
      (admin ? '<div class="fl-mini-acc"><button type="button" class="fl-btn fl-btn--chico" data-farch="subir" data-tipo="' + esc(d.clave) + '"><i class="bi bi-upload"></i>Subir PDF</button></div>' : '');
  }
  function pintarFicha(v) {
    S.fichaId = v.id;
    $('flModalFichaTitulo').textContent = 'Ficha · ' + v.patente_completa;
    var admin = PERM.admin;
    var foto = '<div class="fl-ficha-foto"><div class="fl-foto fl-foto--grande">' + fotoHtml(v, true) + '</div>' +
      (admin ? '<div class="fl-mini-acc"><button type="button" class="fl-btn fl-btn--chico" data-farch="subir" data-tipo="foto"><i class="bi bi-camera"></i>' + (v.foto_id ? 'Cambiar foto' : 'Subir foto') + '</button>' +
        (v.foto_id ? '<button type="button" class="fl-btn fl-btn--chico fl-btn--peligro" data-farch="quitar" data-aid="' + v.foto_id + '"><i class="bi bi-x-lg"></i>Quitar</button>' : '') + '</div>' : '') + '</div>';
    var cab = '<div class="fl-ficha-cab">' + foto + '<div class="fl-ficha-tit"><h4>' + esc(tituloVeh(v)) + '</h4><p>' + esc(lineaMeta(v)) + '</p>' +
      (v.propietario ? '<p class="fl-veh-prop">Propietario registrado: <b>' + esc(v.propietario) + '</b></p>' : '') + '<div>' + celdaEstado(v) + '</div></div></div>';
    var cap = [v.capacidad_kg != null ? num(v.capacidad_kg) + ' kg' : '', v.capacidad_m3 != null ? num(v.capacidad_m3) + ' m³' : ''].filter(Boolean).join(' · ');
    var datos = '<div class="fl-datos">' +
      dato('Patente', v.patente_completa) + dato('Tipo', v.tipo) + dato('Marca', v.marca) + dato('Modelo', v.modelo) + dato('Año', v.anio ? String(v.anio) : '') +
      dato('Color', v.color) + dato('Combustible', v.combustible) + dato('Cilindrada / motor', v.cilindrada) +
      dato('Kilometraje', v.km_actual == null ? '' : num(v.km_actual) + ' km') + dato('Licencia que exige', v.licencia_texto) +
      dato('Capacidad de carga', cap) + dato('Estanque', v.estanque_l != null ? num(v.estanque_l) + ' L' : '') +
      (PERM.documentos ? dato('N° de chasis (VIN)', v.vin) + dato('N° de motor', v.numero_motor) + dato('RUT del propietario', v.propietario_rut_fmt) : '') +
      dato('Propietario registrado', v.propietario) + '</div>' + (v.notas ? '<p class="fl-notas"><b>Observaciones:</b> ' + esc(v.notas) + '</p>' : '');
    var filas = v.documentos.map(function (d) {
      return '<tr><td data-label="Documento"><b>' + esc(d.nombre) + '</b></td>' +
        '<td data-label="Detalle">' + (PERM.documentos ? esc(d.detalle || '—') : '<span class="fl-t-mute">—</span>') + '</td>' +
        '<td data-label="Vence"><b>' + esc(d.vence_fmt || '—') + '</b><span class="fl-sub">' + esc(d.estado === 'sin_fecha' ? 'Sin fecha registrada' : d.texto) + '</span></td>' +
        '<td data-label="Estado">' + pillDoc(d) + '</td><td data-label="PDF" class="fl-td-pdf">' + celdaPdf(v, d) + '</td></tr>';
    }).join('');
    var docs = '<h6 class="fl-ficha-h">Documentos al día</h6><div class="fl-ftabla-wrap"><table class="fl-ftabla"><thead><tr><th>Documento</th><th>N° o detalle</th><th>Vence</th><th>Estado</th><th>PDF</th></tr></thead><tbody>' + filas + '</tbody></table></div>';
    var cal = '';
    if (v.calidad) {
      var c = v.calidad, cls = c.pct >= 100 ? 'ok' : (c.pct >= 60 ? 'warn' : 'bad');
      cal = '<h6 class="fl-ficha-h">Calidad de la ficha</h6><div class="fl-calidad fl-calidad--grande"><span class="fl-barra fl-barra--' + cls + '"><i style="width:' + c.pct + '%"></i></span><b>' + c.pct + '%</b><span class="fl-t-mute">' +
        c.hechos + ' de ' + c.total + ' datos registrados</span></div>' +
        (c.faltan.length ? '<ul class="fl-faltan">' + c.faltan.map(function (f) { return '<li>' + esc(f) + '</li>'; }).join('') + '</ul>' : '<p class="fl-t-ok">La ficha está completa y al día.</p>');
    }
    var repo = PERM.documentos ? '<h6 class="fl-ficha-h">Repositorio de documentos</h6><div class="fl-repo" id="flRepo"><span class="fl-t-mute">Cargando…</span></div>' : '';
    var usos = '<h6 class="fl-ficha-h">Usos y devoluciones</h6><div class="fl-usos" id="flUsos"><span class="fl-t-mute">Cargando…</span></div>';
    $('flFichaCuerpo').innerHTML = cab + datos + usos + docs + repo + cal;
    if (PERM.documentos) pintarRepositorio(v.id);
    USOS.pag = 1; cargarUsos(v.id);
    var b = [];
    if (admin) {
      b.push('<button type="button" class="btn btn-outline-secondary" data-facc="editar"><i class="bi bi-pencil me-1"></i>Editar datos</button>');
      if (!v.archivado) b.push(v.habilitado
        ? '<button type="button" class="btn btn-outline-danger" data-facc="inhabilitar"><i class="bi bi-slash-circle me-1"></i>Inhabilitar</button>'
        : '<button type="button" class="btn btn-outline-success" data-facc="habilitar"><i class="bi bi-check-circle me-1"></i>Habilitar</button>');
      b.push(v.archivado
        ? '<button type="button" class="btn btn-outline-secondary" data-facc="desarchivar"><i class="bi bi-box-arrow-up me-1"></i>Desarchivar</button>'
        : '<button type="button" class="btn btn-outline-secondary" data-facc="archivar"><i class="bi bi-archive me-1"></i>Archivar</button>');
      b.push('<button type="button" class="btn btn-outline-danger" data-facc="eliminar"><i class="bi bi-trash me-1"></i>Borrar definitivo</button>');
    }
    b.push('<button type="button" class="btn btn-dark" data-bs-dismiss="modal">Cerrar</button>');
    $('flFichaAcc').innerHTML = b.join('');
  }
  /* Usos y devoluciones (Daniel, 03-nov-2026: «se van a poder registrar las entregas de cada uno… ver qué tan fuera del plan maneja
     los tiempos cada quien»): cada retiro con lo reservado contra lo real y los km de ese uso. Paginado como Etiquetas (REGLA #4.3). */
  var USOS = { lista: [], pag: 1, por: 10, vid: null };
  function chipDesvio(d, vacio) {
    if (!d) return '<span class="fl-dsv fl-dsv--nada">' + esc(vacio) + '</span>';
    return '<span class="fl-dsv fl-dsv--' + esc(d.estado) + '">' + esc(d.texto) + '</span>';
  }
  function cargarUsos(vid) {
    USOS.vid = vid;
    api(API + '/' + vid + '/usos').then(function (d) {
      if (S.fichaId !== vid) return;
      USOS.lista = d.ok ? (d.usos || []) : null;
      pintarUsos(d.ok ? '' : (d.error || 'No se pudieron leer los usos.'));
    });
  }
  function pintarUsos(error) {
    var el = document.querySelector('#flFichaCuerpo .fl-usos');
    if (!el) return;
    if (error) { el.innerHTML = '<span class="fl-t-mute">' + esc(error) + '</span>'; return; }
    var L = USOS.lista || [];
    if (!L.length) { el.innerHTML = '<span class="fl-t-mute">Todavía no tiene usos: aquí aparece cada retiro de llave y su devolución.</span>'; return; }
    var tarde = L.filter(function (u) { return (u.desvio_retiro && u.desvio_retiro.estado === 'tarde') || (u.desvio_devolucion && u.desvio_devolucion.estado === 'tarde'); }).length;
    var conKm = L.filter(function (u) { return u.km_uso != null; });
    var km = conKm.reduce(function (t, u) { return t + u.km_uso; }, 0);
    var paginas = Math.max(1, Math.ceil(L.length / USOS.por)), p = Math.min(USOS.pag, paginas), desde = (p - 1) * USOS.por;
    var filas = L.slice(desde, desde + USOS.por).map(function (u) {
      return '<tr><td data-label="Uso"><b>' + esc(u.numero) + '</b><span class="fl-sub">' + esc(u.rango_texto) + '</span></td>' +
        '<td data-label="Chofer"><b>' + esc(u.chofer) + '</b><span class="fl-sub">' + esc((u.area || '') + (u.solicitante ? ' · pidió ' + u.solicitante : '')) + '</span></td>' +
        '<td data-label="Retiró la llave">' + esc(u.llave_fmt || '—') + chipDesvio(u.desvio_retiro, 'Sin registro') + '</td>' +
        '<td data-label="Devolvió">' + (u.estado === 'devuelta' ? esc(u.devuelta_fmt || '—') + chipDesvio(u.desvio_devolucion, 'Sin registro')
          : '<span class="fl-dsv fl-dsv--uso">En uso</span>') + '</td>' +
        '<td data-label="Kilometraje">' + (u.dev_km != null ? '<b>' + num(u.dev_km) + ' km</b>' : '—') +
          (u.km_uso != null ? '<span class="fl-sub">+' + num(u.km_uso) + ' km en este uso</span>' : '') + '</td>' +
        '<td data-label="Combustible y estado">' + esc(u.combustible_texto || '—') +
          (u.estado === 'devuelta' ? '<span class="fl-sub' + (u.novedad ? ' fl-t-warn' : '') + '">' + (u.novedad ? 'Con novedad' : 'Sin novedad') + '</span>' : '') + '</td></tr>';
    }).join('');
    el.innerHTML = '<div class="fl-usos-res"><span><b>' + L.length + '</b> ' + (L.length === 1 ? 'uso' : 'usos') + '</span>' +
      (conKm.length ? '<span><b>' + num(km) + ' km</b> recorridos</span>' : '<span>Los km de cada uso se cuentan desde la segunda devolución</span>') +
      '<span class="' + (tarde ? 'fl-t-warn' : 'fl-t-ok') + '"><b>' + tarde + '</b> fuera de horario (más de 15 min)</span></div>' +
      '<div class="fl-ftabla-wrap"><table class="fl-ftabla fl-ftabla--usos"><thead><tr><th>Uso</th><th>Chofer</th><th>Retiró la llave</th><th>Devolvió</th><th>Kilometraje</th><th>Combustible y estado</th></tr></thead><tbody>' +
      filas + '</tbody></table></div>' +
      '<div class="fl-pie fl-pie--usos"><div class="fl-pie-info">Mostrando ' + (desde + 1) + '–' + Math.min(desde + USOS.por, L.length) + ' de ' + L.length + '</div>' +
      '<div class="fl-pie-ctrl"><button type="button" class="fl-pg" data-upag="-1"' + (p <= 1 ? ' disabled' : '') + '>Anterior</button>' +
      '<span class="fl-pg-n">Página ' + p + ' de ' + paginas + '</span>' +
      '<button type="button" class="fl-pg" data-upag="1"' + (p >= paginas ? ' disabled' : '') + '>Siguiente</button></div></div>';
  }
  /* Repositorio: TODO lo que se le ha subido al vehículo, el vigente y los anteriores (Daniel, 02-nov-2026). Nada se borra. */
  function pintarRepositorio(vid) {
    api(API + '/' + vid + '/archivos').then(function (d) {
      var el = document.querySelector('#flFichaCuerpo .fl-repo');      // se dibuja con la ficha (no está en la plantilla)
      if (!el || S.fichaId !== vid) return;
      if (!d.ok) { el.innerHTML = '<span class="fl-t-mute">' + esc(d.error || 'No se pudo leer el repositorio.') + '</span>'; return; }
      var lista = d.archivos || [];
      if (!lista.length) { el.innerHTML = '<span class="fl-t-mute">Todavía no se ha subido ningún archivo.</span>'; return; }
      el.innerHTML = '<div class="fl-ftabla-wrap"><table class="fl-ftabla"><thead><tr><th>Documento</th><th>Archivo</th><th>Subido</th><th>Estado</th></tr></thead><tbody>' +
        lista.map(function (a) {
          var kb = a.bytes ? ' · ' + (a.bytes >= 1048576 ? (a.bytes / 1048576).toFixed(1).replace('.', ',') + ' MB' : Math.max(1, Math.round(a.bytes / 1024)) + ' KB') : '';
          return '<tr class="' + (a.vigente ? '' : 'inhab') + '"><td data-label="Documento"><b>' + esc(a.etiqueta || a.tipo) + '</b></td>' +
            '<td data-label="Archivo"><a class="fl-pdf" href="' + API_ARCH + '/' + a.id + '" target="_blank" rel="noopener"><i class="bi ' + (a.tipo === 'foto' ? 'bi-image' : 'bi-file-earmark-pdf') + '"></i>Abrir</a>' +
            '<span class="fl-sub">' + esc((a.nombre || '') + kb) + '</span></td>' +
            '<td data-label="Subido">' + esc(a.subido_fmt || '—') + '<span class="fl-sub">' + esc(a.subido_por || '') + '</span></td>' +
            '<td data-label="Estado">' + (a.vigente ? '<span class="fl-doc-pill fl-doc-pill--vigente">Vigente</span>'
              : '<span class="fl-doc-pill fl-doc-pill--sin_fecha">Anterior</span><span class="fl-sub">' + esc((a.quitado_por ? 'Reemplazado por ' + a.quitado_por : 'Reemplazado') + (a.quitado_fmt ? ' · ' + a.quitado_fmt : '')) + '</span>') + '</td></tr>';
        }).join('') + '</tbody></table></div>';
    });
  }
  function abrirFicha(v) {
    pintarFicha(v);
    window.bootstrap.Modal.getOrCreateInstance($('flModalFicha')).show();
  }

  /* Archivos: foto y PDF (el servidor valida de nuevo tipo, peso y contenido) */
  var pendiente = null;
  function elegirArchivo(tipo) {
    pendiente = tipo;
    var inp = $('flArchivoInput');
    inp.accept = tipo === 'foto' ? 'image/jpeg,image/png,image/webp' : 'application/pdf';
    inp.value = '';
    inp.click();
  }
  function alSeleccionarArchivo(ev) {
    var f = ev.target.files && ev.target.files[0];
    if (!f || !pendiente || S.fichaId == null) return;
    var tipo = pendiente;
    pendiente = null;
    var foto = tipo === 'foto';
    var okTipo = foto ? /^image\/(jpeg|png|webp)$/.test(f.type) : (f.type === 'application/pdf' || /\.pdf$/i.test(f.name));
    if (!okTipo) { toast(foto ? 'La foto debe ser JPG, PNG o WebP.' : 'El documento debe ser un PDF.', 'warning'); return; }
    if (f.size > (foto ? 10 : 15) * 1024 * 1024) { toast('El archivo pesa demasiado (máximo ' + (foto ? 10 : 15) + ' MB).', 'warning'); return; }
    if (S.subiendo) return;
    S.subiendo = true;
    toast('Guardando el archivo…', 'info');
    subir('vehiculo', S.fichaId, tipo, f).then(function (d) {
      S.subiendo = false;
      if (!d.ok) { toast(d.error || 'No se pudo guardar el archivo.', 'error'); return; }
      toast(foto ? 'Foto guardada.' : 'PDF guardado.', 'success');
      cargar();
    });
  }
  function quitarArchivo(aid) {
    window.ilusConfirm({
      title: 'Quitar el archivo', message: '¿Dejar de usar este archivo? El original queda guardado en el historial; después puedes subir otro.',
      okLabel: 'Quitar', cancelLabel: 'Volver', danger: true
    }).then(function (ok) {
      if (!ok) return;
      api(API_ARCH + '/' + aid + '/quitar', { method: 'POST', body: {} }).then(function (d) {
        if (!d.ok) { toast(d.error || 'No se pudo quitar el archivo.', 'error'); return; }
        toast('Archivo quitado.', 'success');
        cargar();
      });
    });
  }

  /* ═══ Formulario (alta y edición) ═══ */
  function res(estado, msg) { return { estado: estado, msg: msg || '' }; }
  function numeroOpc(min, max, decimal, unidad) {
    return function (v) {
      v = String(v).trim();
      if (!v) return res('');
      var t = v.replace(/\s/g, '');
      var n;
      if (decimal) {
        if (!/^\d+([.,]\d+)?$/.test(t)) return res('bad', 'Debe ser un número (puedes usar coma o punto).');
        n = parseFloat(t.replace(',', '.'));
      } else {
        if (!/^\d+$/.test(t) && !/^\d{1,3}(\.\d{3})+$/.test(t)) return res('bad', 'Debe ser un número entero.');
        n = parseInt(t.replace(/\./g, ''), 10);
      }
      if (n < min || n > max) return res('bad', 'Debe estar entre ' + min + ' y ' + max + (unidad ? ' ' + unidad : '') + '.');
      return res('ok', '');
    };
  }
  function regDoc(v) {
    v = String(v).trim();
    if (!v) return res('warn', 'Sin fecha: no se podrá avisar antes de que venza.');
    var f = aFecha(v);
    if (!f || f.getFullYear() < 2000 || f.getFullYear() > 2100) return res('bad', 'Fecha no válida.');
    var d = diasHasta(v);
    if (d == null) return res('ok', '');
    if (d < 0) return res('bad', textoDias(d) + '.');
    if (d <= (S.config.dias_alerta || 30)) return res('warn', textoDias(d) + '.');
    return res('ok', 'Vigente · ' + textoDias(d).toLowerCase() + '.');
  }
  var REGLAS = {
    patente: function (v) {
      var s = sepPat(v), p = s.base;
      if (!p) return res('bad', 'La patente es obligatoria.');
      if (!/^[A-Z0-9]{5,7}$/.test(p) || !/[A-Z]/.test(p) || !/\d/.test(p)) return res('bad', 'Debe tener entre 5 y 7 letras y números (por ejemplo ABCD12 o TVDZ88-5).');
      return res('ok', 'Patente válida: ' + conGuion(p) + (s.dv ? ' · dígito verificador ' + s.dv : ' (puedes agregar su dígito, como TVDZ88-5)'));
    },
    tipo: function (v) {
      v = String(v).trim();
      if (!v) return res('bad', 'Indica el tipo de vehículo (por ejemplo Furgón o Camión).');
      if (v.length < 2) return res('bad', 'El tipo es muy corto.');
      return res('ok', '');
    },
    anio: function (v) {
      var anio = new Date().getFullYear() + 1;
      return numeroOpc(1950, anio, false)(v);
    },
    propietario_rut: function (v) {
      v = String(v).trim();
      if (!v) return res('');
      return rutValido(v) ? res('ok', 'RUT válido.') : res('bad', 'El RUT no es válido (revisa el dígito verificador).');
    },
    capacidad_kg: numeroOpc(0, 100000, false, 'kg'),
    capacidad_m3: numeroOpc(0, 200, true, 'm³'),
    estanque_l: numeroOpc(0, 2000, false, 'litros'),
    km_actual: numeroOpc(0, 3000000, false, 'km'),
    revision_tecnica_vence: regDoc, permiso_circulacion_vence: regDoc, soap_vence: regDoc, seguro_vence: regDoc
  };
  var OBLIGATORIOS = ['patente', 'tipo'];
  var PASOS = {
    1: ['patente', 'tipo', 'anio', 'propietario_rut'],
    2: ['capacidad_kg', 'capacidad_m3', 'estanque_l', 'km_actual'],
    4: ['revision_tecnica_vence', 'permiso_circulacion_vence', 'soap_vence', 'seguro_vence']
  };

  function campoEl(c) { return document.querySelector('#flForm .fl-campo[data-campo="' + c + '"]'); }
  function valor(c) { var el = $('flForm').elements[c]; return el ? el.value : ''; }
  function clasesSel() {
    return Array.prototype.slice.call(document.querySelectorAll('#flClases input:checked')).map(function (i) { return i.value; });
  }
  function escribir(el, txt) { if (el && el.textContent !== txt) el.textContent = txt; }   // aria-live: solo si cambió

  function marcar(c, r, forzar) {
    var el = campoEl(c);
    if (!el) return;
    var requerido = OBLIGATORIOS.indexOf(c) >= 0 || c === 'licencia_clases';
    var visible = !(requerido && r.estado === 'bad') || tocados[c] || S.intento || forzar;
    el.classList.remove('ok', 'bad', 'warn');
    if (visible && r.estado) el.classList.add(r.estado);
    escribir(el.querySelector('.fl-msg'), visible ? r.msg : '');
  }

  function evaluar() {
    var estado = {};
    Object.keys(REGLAS).forEach(function (c) {
      var r = REGLAS[c](valor(c));
      estado[c] = r.estado;
      marcar(c, r);
    });
    var rc = clasesSel().length ? res('ok', 'Licencia exigida: clase ' + clasesSel().join(' o ') + '.') : res('bad', 'Elige al menos una clase de licencia.');
    estado.licencia_clases = rc.estado;
    marcar('licencia_clases', rc);
    Array.prototype.forEach.call(document.querySelectorAll('#flClases .fl-clase'), function (l) {
      l.classList.toggle('on', l.querySelector('input').checked);
    });
    function paso(n, cls) {
      var el = document.querySelector('#flForm .fl-paso[data-paso="' + n + '"]');
      if (el) { el.classList.remove('ok', 'bad', 'warn'); if (cls) el.classList.add(cls); }
    }
    function resumen(campos, opcionales) {
      var e = campos.map(function (c) { return estado[c]; });
      if (e.indexOf('bad') >= 0) return 'bad';
      if (!opcionales) return e.every(function (x) { return x === 'ok'; }) ? 'ok' : 'bad';
      return e.some(function (x) { return x === 'ok'; }) ? 'ok' : '';
    }
    paso(1, resumen(['patente', 'tipo']) === 'ok' && estado.anio !== 'bad' && estado.propietario_rut !== 'bad' ? 'ok' : 'bad');
    paso(2, resumen(PASOS[2], true));
    paso(3, rc.estado);
    var docs = PASOS[4].map(function (c) { return estado[c]; });
    paso(4, docs.indexOf('bad') >= 0 ? 'bad' : (docs.every(function (x) { return x === 'ok'; }) ? 'ok' : 'warn'));
    var faltan = faltantes();
    var pie = $('flPieForm');
    pie.className = 'fl-pie-form' + (faltan.length && S.intento ? ' bad' : '');
    escribir(pie, faltan.length ? 'Falta: ' + faltan.join(', ') + '.' : 'Todo listo para guardar.');
  }

  function faltantes() {
    var f = [];
    OBLIGATORIOS.forEach(function (c) { if (REGLAS[c](valor(c)).estado !== 'ok') f.push(NOMBRES[c]); });
    ['anio', 'capacidad_kg', 'capacidad_m3', 'estanque_l', 'km_actual'].forEach(function (c) {
      if (REGLAS[c](valor(c)).estado === 'bad') f.push(c === 'anio' ? 'el año' : 'un número mal escrito');
    });
    if (REGLAS.propietario_rut(valor('propietario_rut')).estado === 'bad') f.push('el RUT del propietario');
    if (!clasesSel().length) f.push(NOMBRES.licencia_clases);
    return f.filter(function (x, i) { return f.indexOf(x) === i; });
  }
  var agendar;
  function evaluarLuego() { clearTimeout(agendar); agendar = setTimeout(evaluar, 0); }   // setTimeout, nunca requestAnimationFrame

  function poblarListas() {
    $('flTiposList').innerHTML = (S.config.tipos || []).map(function (t) { return '<option value="' + esc(t) + '">'; }).join('');
    $('flCombustiblesList').innerHTML = (S.config.combustibles || []).map(function (t) { return '<option value="' + esc(t) + '">'; }).join('');
    pintarCombustibles();
  }
  /* Combustible en botones (Daniel, 02-nov-2026: «si no es diésel es a gasolina, puede ser híbrido, puede ser eléctrico»); el campo sigue
     aceptando otro valor escrito a mano. */
  function pintarCombustibles() {
    var actual = norm(valor('combustible'));
    $('flCombRapidos').innerHTML = (S.config.combustibles || []).map(function (t) {
      var on = actual && norm(t) === actual;
      return '<button type="button" class="fl-sug' + (on ? ' on' : '') + '" data-comb="' + esc(t) + '" aria-pressed="' + on + '">' + esc(t) + '</button>';
    }).join('');
  }
  /* PDF de cada documento dentro del formulario (Daniel, 02-nov-2026: «quisiera los documentos y poderlos ver… súbelos»). Mismo camino
     que la ficha (POST /flota/api/archivos); en un vehículo nuevo se sube después de guardarlo. */
  function pintarPdfsForm(v) {
    Array.prototype.forEach.call(document.querySelectorAll('#flForm [data-docpdf]'), function (el) {
      var clave = el.getAttribute('data-docpdf');
      if (!v) { el.innerHTML = '<span class="fl-t-mute"><i class="bi bi-info-circle"></i> Guarda el vehículo y luego sube el PDF desde aquí o desde su ficha.</span>'; return; }
      var d = (v.documentos || []).filter(function (x) { return x.clave === clave; })[0];
      el.innerHTML = d ? '<div class="fl-docpdf-c">' + celdaPdf(v, d) + '</div>' : '';
    });
  }
  function pintarClases(sel) {
    $('flClases').innerHTML = (S.config.clases || []).map(function (c) {
      var sug = c.codigo === S.config.clase_sugerida ? ' · sugerida' : '';
      return '<label class="fl-clase"><input type="checkbox" value="' + esc(c.codigo) + '"' + (sel.indexOf(c.codigo) >= 0 ? ' checked' : '') + '>' +
        '<span><b>Clase ' + esc(c.codigo) + esc(sug) + '</b><span>' + esc(c.nombre) + '</span></span></label>';
    }).join('');
  }

  function abrirForm(v) {
    S.editando = v || null; S.intento = false; tocados = {};
    $('flModalVehTitulo').textContent = v ? 'Editar vehículo ' + v.patente_completa : 'Agregar vehículo';
    $('flMotivoCambioCampo').hidden = !v;
    $('flGuardar').innerHTML = '<i class="bi bi-check2-circle me-1"></i>' + (v ? 'Guardar cambios' : 'Guardar vehículo');
    poblarListas();
    var f = $('flForm');
    f.reset();
    TEXTOS.forEach(function (c) {
      var el = f.elements[c];
      if (el) el.value = v && v[c] != null ? v[c] : '';
    });
    if (v) f.elements.patente.value = v.patente_completa;
    if (v && v.propietario_rut) f.elements.propietario_rut.value = v.propietario_rut_fmt || v.propietario_rut;
    pintarClases(v ? v.licencia_clases : [S.config.clase_sugerida]);
    if (v) S.fichaId = v.id;              // los PDF que se suban desde el formulario van a este vehículo
    pintarCombustibles();
    pintarPdfsForm(v);
    evaluar();
    window.bootstrap.Modal.getOrCreateInstance($('flModalVeh')).show();
  }

  function leerForm() {
    var d = {};
    TEXTOS.forEach(function (c) { d[c] = valor(c); });
    d.licencia_clases = clasesSel();
    if (S.editando) d.motivo_cambio = valor('motivo_cambio');
    return d;
  }

  function marcarErroresServidor(errores) {
    Object.keys(errores || {}).forEach(function (c) {
      var el = campoEl(c);
      if (!el) return;
      el.classList.remove('ok', 'warn'); el.classList.add('bad');
      escribir(el.querySelector('.fl-msg'), errores[c]);
    });
    var primero = Object.keys(errores || {})[0];
    var elp = primero && campoEl(primero);
    if (elp) { var inp = elp.querySelector('input,textarea'); if (inp) inp.focus(); }
  }

  function guardar() {
    if (S.guardando) return;
    S.intento = true;
    evaluar();
    var faltan = faltantes();
    if (faltan.length) {
      toast('Falta: ' + faltan.join(', ') + '.', 'warning');
      var malo = document.querySelector('#flForm .fl-campo.bad');
      if (malo) { var i = malo.querySelector('input,textarea'); if (i) i.focus(); }
      return;
    }
    var vencidos = DOCS.filter(function (d) { return regDoc(valor(d.campo)).estado === 'bad' && String(valor(d.campo)).trim(); });
    var seguir = vencidos.length
      ? window.ilusConfirm({
        title: 'Hay documentos vencidos', message: 'Estos documentos ya vencieron. ¿Guardar el vehículo igual?',
        sub: vencidos.map(function (d) { return d.nombre; }).join(', '), okLabel: 'Guardar igual', cancelLabel: 'Revisar'
      })
      : Promise.resolve(true);
    seguir.then(function (ok) { if (ok) enviar(leerForm()); });
  }

  function enviar(payload) {
    S.guardando = true;
    var btn = $('flGuardar');
    btn.disabled = true;
    var editando = S.editando;
    api(editando ? API + '/' + editando.id : API, { method: editando ? 'PUT' : 'POST', body: payload }).then(function (d) {
      S.guardando = false; btn.disabled = false;
      if (!d.ok) {
        if (d.errores) marcarErroresServidor(d.errores);
        toast(d.error || 'No se pudo guardar.', 'error');
        return;
      }
      window.bootstrap.Modal.getOrCreateInstance($('flModalVeh')).hide();
      toast(d.sin_cambios ? 'No había cambios que guardar.' : (editando ? 'Vehículo actualizado.' : 'Vehículo agregado y habilitado.'), 'success');
      cargar().then(function () {
        // Un vehículo recién agregado se abre en su ficha: ahí se sube la foto y los PDF.
        if (!editando && d.vehiculo) { var nv = buscar(d.vehiculo.id); if (nv) abrirFicha(nv); }
      });
    });
  }

  /* ═══ Habilitar, archivar, borrar e historial ═══ */
  function inhabilitar(v) {
    window.ilusPrompt({
      title: 'Inhabilitar ' + v.patente_completa,
      message: 'Cuéntanos por qué deja de estar disponible (en mantención, en reparación, vendido, siniestrado…). Queda en su historial.',
      placeholder: 'Motivo', okLabel: 'Inhabilitar', type: 'warning', multiline: true, required: true, minLength: 5
    }).then(function (motivo) {
      if (motivo == null) return;
      api(API + '/' + v.id + '/inhabilitar', { method: 'POST', body: { motivo: motivo } }).then(function (d) {
        if (!d.ok) { toast(d.error || 'No se pudo inhabilitar.', 'error'); return; }
        toast(v.patente_completa + ' quedó inhabilitado.', 'success');
        if (d.avisos && d.avisos.length) toast(d.avisos.join(' '), 'warning');   // alertar, no trabar: tenía reservas por delante
        cargar();
      });
    });
  }
  function habilitar(v) {
    window.ilusConfirm({
      title: 'Habilitar ' + v.patente_completa, message: '¿Dejar este vehículo disponible de nuevo?',
      sub: v.inhabilitado_motivo ? 'Estaba inhabilitado por: ' + v.inhabilitado_motivo : '', okLabel: 'Habilitar', cancelLabel: 'Cancelar'
    }).then(function (ok) {
      if (!ok) return;
      api(API + '/' + v.id + '/habilitar', { method: 'POST', body: {} }).then(function (d) {
        if (!d.ok) { toast(d.error || 'No se pudo habilitar.', 'error'); return; }
        toast(v.patente_completa + ' quedó habilitado.', 'success');
        if (d.avisos && d.avisos.length) toast('Ojo con sus documentos: ' + d.avisos.join('; ') + '.', 'warning');
        cargar();
      });
    });
  }
  function archivar(v) {
    window.ilusPrompt({
      title: 'Archivar ' + v.patente_completa,
      message: 'Sale de las listas pero conserva sus datos, sus archivos y el historial de sus reservas. Puedes desarchivarlo cuando quieras.',
      placeholder: 'Motivo (vendido, dado de baja, siniestro…)', okLabel: 'Archivar', type: 'warning', multiline: true, required: true, minLength: 5
    }).then(function (motivo) {
      if (motivo == null) return;
      api(API + '/' + v.id + '/archivar', { method: 'POST', body: { motivo: motivo } }).then(function (d) {
        if (!d.ok) { toast(d.error || 'No se pudo archivar.', 'error'); return; }
        toast(v.patente_completa + ' quedó archivado.', 'success');
        S.fichaId = null;
        despuesDeCerrar('flModalFicha', cargar);
      });
    });
  }
  function desarchivar(v) {
    api(API + '/' + v.id + '/desarchivar', { method: 'POST', body: {} }).then(function (d) {
      if (!d.ok) { toast(d.error || 'No se pudo desarchivar.', 'error'); return; }
      toast(v.patente_completa + ' salió del archivo.', 'success');
      (d.avisos || []).forEach(function (a) { toast(a, 'warning'); });
      S.fichaId = null;
      despuesDeCerrar('flModalFicha', cargar);
    });
  }
  function eliminar(v) {
    window.ilusPrompt({
      title: 'Borrar definitivo ' + v.patente_completa,
      message: 'Se borra para siempre y no se puede deshacer. Solo se puede si nunca tuvo reservas; si no, archívalo. Para confirmar escribe su patente.',
      placeholder: v.patente_completa, okLabel: 'Borrar definitivo', type: 'danger', required: true
    }).then(function (txt) {
      if (txt == null) return;
      api(API + '/' + v.id, { method: 'DELETE', body: { confirmacion: txt } }).then(function (d) {
        if (!d.ok) { toast(d.error || 'No se pudo borrar.', 'error'); return; }
        toast(v.patente_completa + ' se borró definitivamente.', 'success');
        S.fichaId = null;
        despuesDeCerrar('flModalFicha', cargar);
      });
    });
  }
  function historial(v) {
    $('flModalHistTitulo').textContent = 'Historial de ' + v.patente_completa;
    $('flHistCuerpo').innerHTML = '<div class="text-muted">Cargando…</div>';
    window.bootstrap.Modal.getOrCreateInstance($('flModalHist')).show();
    api(API + '/' + v.id + '/historial').then(function (d) {
      if (!d.ok) { $('flHistCuerpo').innerHTML = '<div class="text-danger">' + esc(d.error || 'No se pudo leer el historial.') + '</div>'; return; }
      if (!d.eventos.length) { $('flHistCuerpo').innerHTML = '<div class="text-muted">Todavía no hay movimientos.</div>'; return; }
      $('flHistCuerpo').innerHTML = d.eventos.map(function (e) {
        return '<div class="fl-ev fl-ev--' + esc(e.accion) + '"><div class="fl-ev-t">' + esc(e.titulo) + '</div>' +
          '<div class="fl-ev-m">' + esc(e.fecha) + (e.usuario ? ' · ' + esc(e.usuario) : '') + '</div>' +
          (e.lineas.length ? '<ul>' + e.lineas.map(function (l) { return '<li>' + esc(l) + '</li>'; }).join('') + '</ul>' : '') + '</div>';
      }).join('');
    });
  }

  /* ═══ Eventos ═══ */
  function accionFila(a, v) {
    if (a === 'ficha') abrirFicha(v);
    else if (a === 'editar') abrirForm(v);
    else if (a === 'historial') historial(v);
  }
  function init() {
    $('flBtnNuevo').addEventListener('click', function () { abrirForm(null); });
    $('flGuardar').addEventListener('click', guardar);
    $('flCuerpo').addEventListener('click', function (ev) {
      var b = ev.target.closest('[data-acc]');
      if (!b) return;
      var v = buscar(b.closest('[data-id]').getAttribute('data-id'));
      if (v) accionFila(b.getAttribute('data-acc'), v);
    });
    $('flCuerpo').addEventListener('error', function (ev) {          // foto que no carga: vuelve el ícono
      var im = ev.target;
      if (im && im.tagName === 'IMG') { var d = document.createElement('i'); d.className = 'bi bi-truck-front'; im.replaceWith(d); }
    }, true);
    $('flFichaCuerpo').addEventListener('click', function (ev) {
      var pg = ev.target.closest('[data-upag]');
      if (pg) { USOS.pag = Math.max(1, USOS.pag + parseInt(pg.getAttribute('data-upag'), 10)); pintarUsos(''); return; }
      var b = ev.target.closest('[data-farch]');
      if (!b) return;
      if (b.getAttribute('data-farch') === 'subir') elegirArchivo(b.getAttribute('data-tipo'));
      else quitarArchivo(b.getAttribute('data-aid'));
    });
    $('flFichaAcc').addEventListener('click', function (ev) {
      var b = ev.target.closest('[data-facc]');
      var v = buscar(S.fichaId);
      if (!b || !v) return;
      var a = b.getAttribute('data-facc');
      if (a === 'editar') despuesDeCerrar('flModalFicha', function () { abrirForm(v); });
      else if (a === 'inhabilitar') inhabilitar(v);
      else if (a === 'habilitar') habilitar(v);
      else if (a === 'archivar') archivar(v);
      else if (a === 'desarchivar') desarchivar(v);
      else if (a === 'eliminar') eliminar(v);
    });
    $('flArchivoInput').addEventListener('change', alSeleccionarArchivo);
    $('flForm').addEventListener('click', function (ev) {
      var c = ev.target.closest('[data-comb]');
      if (c) { $('flForm').elements.combustible.value = c.getAttribute('data-comb'); tocados.combustible = true; pintarCombustibles(); evaluarLuego(); return; }
      var b = ev.target.closest('[data-farch]');
      if (!b || !S.editando) return;
      S.fichaId = S.editando.id;
      if (b.getAttribute('data-farch') === 'subir') elegirArchivo(b.getAttribute('data-tipo'));
      else quitarArchivo(b.getAttribute('data-aid'));
    });
    $('flCombustible').addEventListener('input', pintarCombustibles);
    var t;
    $('flBuscar').addEventListener('input', function (e) {
      clearTimeout(t);
      t = setTimeout(function () { S.q = e.target.value; S.pagina = 1; pintarLista(); }, 150);
    });
    Array.prototype.forEach.call(document.querySelectorAll('#flPanelVehiculos .fl-chip'), function (b) {
      b.addEventListener('click', function () {
        Array.prototype.forEach.call(document.querySelectorAll('#flPanelVehiculos .fl-chip'), function (x) { x.classList.toggle('on', x === b); });
        var est = b.getAttribute('data-estado');
        var ver = est === 'archivados';
        S.estado = ver ? 'todos' : est; S.pagina = 1;
        if (ver !== S.verArchivados) { S.verArchivados = ver; cargar(); } else pintarLista();
      });
    });
    $('flOrden').addEventListener('change', function (e) { S.orden = e.target.value; S.pagina = 1; pintarLista(); });
    $('flPorPagina').addEventListener('change', function (e) { S.porPagina = parseInt(e.target.value, 10) || 25; S.pagina = 1; pintarLista(); });
    $('flAnterior').addEventListener('click', function () { if (S.pagina > 1) { S.pagina--; pintarLista(); } });
    $('flSiguiente').addEventListener('click', function () { S.pagina++; pintarLista(); });
    var form = $('flForm');
    form.addEventListener('input', function (e) {
      var c = e.target.name;
      if (c === 'patente' || c === 'numero_motor') e.target.value = e.target.value.toUpperCase();
      if (c === 'propietario_rut' && window.ilusPersona) {          // RUT chileno mientras se escribe (12.345.678-5)
        var P = window.ilusPersona, inp = e.target, pos = inp.selectionStart == null ? inp.value.length : inp.selectionStart;
        var der = P.rutLimpiar(inp.value.slice(pos)).length;
        inp.value = P.rutFormatear(inp.value);
        var q = inp.value.length, n = 0;
        while (q > 0 && n < der) { q--; if (/[0-9K]/.test(inp.value.charAt(q))) n++; }
        try { inp.setSelectionRange(q, q); } catch (err) { /* sin selección */ }
      }
      if (c) tocados[c] = true;
      evaluarLuego();
    });
    form.addEventListener('change', function (e) { if (e.target.type === 'checkbox') tocados.licencia_clases = true; evaluarLuego(); });
    form.addEventListener('submit', function (e) { e.preventDefault(); guardar(); });
    cargar();
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init); else init();
})();

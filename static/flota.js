/* Solicitudes de vehículo · Flota (Fase 1a): registro de vehículos, habilitar / inhabilitar e historial.
   Reglas del proyecto que cumple: sin alert/confirm/prompt nativos (REGLA #1, usa ilusConfirm / ilusPrompt / ilusToast), tabla paginada con pie
   «Mostrando A–B de N» (REGLA #4.3), campos que pasan de rojo a verde mientras se escribe y al guardar se alerta, no se traba (REGLA #42).
   El servidor es quien manda: todo lo que valida acá se vuelve a validar allá (flota_reglas.validar_vehiculo). */
(function () {
  'use strict';

  var API = '/flota/api/vehiculos';
  var S = {
    vehiculos: [], estado: 'todos', q: '', pagina: 1, porPagina: 25,
    hoy: null, editando: null, intento: false, guardando: false,
    config: { tipos: [], combustibles: [], clases: [], clase_sugerida: 'B', dias_alerta: 30 }
  };
  var tocados = {};
  var DOCS = [
    { clave: 'revision_tecnica_vence', nombre: 'Revisión técnica' },
    { clave: 'permiso_circulacion_vence', nombre: 'Permiso de circulación' },
    { clave: 'soap_vence', nombre: 'SOAP' },
    { clave: 'seguro_vence', nombre: 'Seguro' }
  ];
  var TEXTOS = ['patente', 'tipo', 'marca', 'modelo', 'anio', 'color', 'vin', 'capacidad_kg', 'capacidad_m3', 'combustible',
    'estanque_l', 'km_actual', 'revision_tecnica_vence', 'permiso_circulacion_vence', 'soap_vence', 'seguro_vence', 'notas'];
  var NOMBRES = { patente: 'la patente', tipo: 'el tipo de vehículo', licencia_clases: 'la clase de licencia' };

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
  function plural(n, uno, varios) { return n + ' ' + (n === 1 ? uno : varios); }

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

  /* ── Carga y lista ── */
  function cargar() {
    return api(API).then(function (d) {
      if (!d.ok) { toast(d.error || 'No se pudo leer la flota.', 'error'); pintarVacio('No se pudo leer la flota', d.error || 'Intenta de nuevo en un momento.'); return; }
      S.vehiculos = d.vehiculos || [];
      S.hoy = d.hoy || null;
      if (d.config) S.config = d.config;
      var r = d.resumen || {};
      $('flKpiHabilitados').textContent = r.habilitados != null ? r.habilitados : '–';
      $('flKpiInhabilitados').textContent = r.inhabilitados != null ? r.inhabilitados : '–';
      $('flKpiPorVencer').textContent = r.por_vencer != null ? r.por_vencer : '–';
      $('flKpiVencidos').textContent = r.vencidos != null ? r.vencidos : '–';
      pintarLista();
    });
  }

  function filtrados() {
    var q = S.q.trim().toLowerCase();
    return S.vehiculos.filter(function (v) {
      if (S.estado === 'habilitados' && !v.habilitado) return false;
      if (S.estado === 'inhabilitados' && v.habilitado) return false;
      if (!q) return true;
      return [v.patente, v.patente_fmt, v.tipo, v.marca, v.modelo, v.titulo].join(' ').toLowerCase().indexOf(q) >= 0;
    });
  }

  function pintarVacio(titulo, texto) {
    var v = $('flVacio');
    v.innerHTML = '<b>' + esc(titulo) + '</b>' + esc(texto || '');
    v.hidden = false;
    $('flTabla').style.display = 'none';
    $('flPie').style.display = 'none';
  }

  function chipDoc(d) {
    var linea = d.estado === 'sin_fecha' ? 'Sin fecha registrada' : d.vence_fmt + ' · ' + d.texto;
    return '<div class="fl-doc fl-doc--' + esc(d.estado) + '"><b>' + esc(d.nombre) + '</b><span>' + esc(linea) + '</span></div>';
  }

  function detalleVeh(v) {
    var p = [];
    if (v.capacidad_kg != null) p.push('Carga ' + num(v.capacidad_kg) + ' kg');
    if (v.capacidad_m3 != null) p.push(num(v.capacidad_m3) + ' m³');
    if (v.combustible) p.push(v.combustible);
    if (v.estanque_l != null) p.push('Estanque ' + num(v.estanque_l) + ' L');
    if (v.color) p.push(v.color);
    return p.length ? '<span class="fl-sub">' + esc(p.join(' · ')) + '</span>' : '';
  }

  function filaVeh(v) {
    var estado = v.habilitado
      ? '<span class="fl-estado fl-estado--on"><i class="bi bi-check-circle-fill"></i>Habilitado</span>'
      : '<span class="fl-estado fl-estado--off"><i class="bi bi-slash-circle"></i>Inhabilitado</span>' +
        '<span class="fl-motivo">' + esc(v.inhabilitado_motivo || '') +
        (v.inhabilitado_por ? ' · ' + esc(v.inhabilitado_por) : '') + (v.inhabilitado_fmt ? ' · ' + esc(v.inhabilitado_fmt) : '') + '</span>';
    var cambiar = v.habilitado
      ? '<button type="button" class="fl-btn fl-btn--peligro" data-acc="inhabilitar"><i class="bi bi-slash-circle"></i>Inhabilitar</button>'
      : '<button type="button" class="fl-btn fl-btn--ok" data-acc="habilitar"><i class="bi bi-check-circle"></i>Habilitar</button>';
    return '<tr class="' + (v.habilitado ? '' : 'inhab') + '" data-id="' + v.id + '">' +
      '<td class="fl-td-veh"><span class="fl-pat">' + esc(v.patente_fmt) + '</span>' +
      '<span class="fl-tit">' + esc(v.titulo || v.tipo) + '</span>' + detalleVeh(v) + '</td>' +
      '<td data-label="Licencia">' + esc(v.licencia_texto) + '</td>' +
      '<td data-label="Kilómetros"><span class="fl-km">' + (v.km_actual == null ? '—' : num(v.km_actual) + ' km') + '</span></td>' +
      '<td class="fl-td-docs" data-label="Documentos"><div class="fl-docs">' + v.documentos.map(chipDoc).join('') + '</div></td>' +
      '<td data-label="Estado">' + estado + '</td>' +
      '<td data-label="Acciones"><div class="fl-acc">' +
      '<button type="button" class="fl-btn" data-acc="editar"><i class="bi bi-pencil"></i>Editar</button>' +
      '<button type="button" class="fl-btn" data-acc="historial"><i class="bi bi-clock-history"></i>Historial</button>' +
      cambiar + '</div></td></tr>';
  }

  function pintarLista() {
    var lista = filtrados();
    if (!S.vehiculos.length) {
      pintarVacio('Aún no hay vehículos registrados', 'Agrega el primero con «Agregar vehículo». Si compran más, se ingresan igual desde acá.');
      return;
    }
    if (!lista.length) {
      pintarVacio('Ningún vehículo coincide', 'Prueba con otra búsqueda o cambia el filtro de estado.');
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

  /* ── Formulario: reglas en vivo (rojo → verde) ── */
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
      var p = normPat(v);
      if (!p) return res('bad', 'La patente es obligatoria.');
      if (!/^[A-Z0-9]{5,7}$/.test(p) || !/[A-Z]/.test(p) || !/\d/.test(p)) return res('bad', 'Debe tener entre 5 y 7 letras y números (por ejemplo ABCD12 o AB1234).');
      return res('ok', 'Patente válida: ' + conGuion(p));
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
    capacidad_kg: numeroOpc(0, 100000, false, 'kg'),
    capacidad_m3: numeroOpc(0, 200, true, 'm³'),
    estanque_l: numeroOpc(0, 2000, false, 'litros'),
    km_actual: numeroOpc(0, 3000000, false, 'km'),
    revision_tecnica_vence: regDoc, permiso_circulacion_vence: regDoc, soap_vence: regDoc, seguro_vence: regDoc
  };
  var OBLIGATORIOS = ['patente', 'tipo'];
  var PASOS = {
    1: ['patente', 'tipo', 'anio'],
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
    paso(1, resumen(['patente', 'tipo']) === 'ok' && estado.anio !== 'bad' ? 'ok' : 'bad');
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
    if (!clasesSel().length) f.push(NOMBRES.licencia_clases);
    return f.filter(function (x, i) { return f.indexOf(x) === i; });
  }
  var agendar;
  function evaluarLuego() { clearTimeout(agendar); agendar = setTimeout(evaluar, 0); }   // setTimeout, nunca requestAnimationFrame

  function poblarListas() {
    $('flTiposList').innerHTML = (S.config.tipos || []).map(function (t) { return '<option value="' + esc(t) + '">'; }).join('');
    $('flCombustiblesList').innerHTML = (S.config.combustibles || []).map(function (t) { return '<option value="' + esc(t) + '">'; }).join('');
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
    $('flModalVehTitulo').textContent = v ? 'Editar vehículo ' + v.patente_fmt : 'Agregar vehículo';
    $('flGuardar').innerHTML = '<i class="bi bi-check2-circle me-1"></i>' + (v ? 'Guardar cambios' : 'Guardar vehículo');
    poblarListas();
    var f = $('flForm');
    f.reset();
    TEXTOS.forEach(function (c) {
      var el = f.elements[c];
      if (el) el.value = v && v[c] != null ? v[c] : '';
    });
    if (v) f.elements.patente.value = v.patente_fmt;
    pintarClases(v ? v.licencia_clases : [S.config.clase_sugerida]);
    evaluar();
    window.bootstrap.Modal.getOrCreateInstance($('flModalVeh')).show();
  }

  function leerForm() {
    var d = {};
    TEXTOS.forEach(function (c) { d[c] = valor(c); });
    d.licencia_clases = clasesSel();
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
    var vencidos = DOCS.filter(function (d) { return regDoc(valor(d.clave)).estado === 'bad' && String(valor(d.clave)).trim(); });
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
      cargar();
    });
  }

  /* ── Habilitar / inhabilitar / historial ── */
  function inhabilitar(v) {
    window.ilusPrompt({
      title: 'Inhabilitar ' + v.patente_fmt,
      message: 'Cuéntanos por qué deja de estar disponible (en mantención, en reparación, vendido, siniestrado…). Queda en su historial.',
      placeholder: 'Motivo', okLabel: 'Inhabilitar', type: 'warning', multiline: true, required: true, minLength: 5
    }).then(function (motivo) {
      if (motivo == null) return;
      api(API + '/' + v.id + '/inhabilitar', { method: 'POST', body: { motivo: motivo } }).then(function (d) {
        if (!d.ok) { toast(d.error || 'No se pudo inhabilitar.', 'error'); return; }
        toast(v.patente_fmt + ' quedó inhabilitado.', 'success');
        cargar();
      });
    });
  }

  function habilitar(v) {
    window.ilusConfirm({
      title: 'Habilitar ' + v.patente_fmt, message: '¿Dejar este vehículo disponible de nuevo?',
      sub: v.inhabilitado_motivo ? 'Estaba inhabilitado por: ' + v.inhabilitado_motivo : '', okLabel: 'Habilitar', cancelLabel: 'Cancelar'
    }).then(function (ok) {
      if (!ok) return;
      api(API + '/' + v.id + '/habilitar', { method: 'POST', body: {} }).then(function (d) {
        if (!d.ok) { toast(d.error || 'No se pudo habilitar.', 'error'); return; }
        toast(v.patente_fmt + ' quedó habilitado.', 'success');
        if (d.avisos && d.avisos.length) toast('Ojo con sus documentos: ' + d.avisos.join('; ') + '.', 'warning');
        cargar();
      });
    });
  }

  function historial(v) {
    $('flModalHistTitulo').textContent = 'Historial de ' + v.patente_fmt;
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

  /* ── Eventos ── */
  function init() {
    $('flBtnNuevo').addEventListener('click', function () { abrirForm(null); });
    $('flGuardar').addEventListener('click', guardar);
    $('flCuerpo').addEventListener('click', function (ev) {
      var b = ev.target.closest('[data-acc]');
      if (!b) return;
      var fila = b.closest('tr');
      var v = S.vehiculos.filter(function (x) { return String(x.id) === fila.getAttribute('data-id'); })[0];
      if (!v) return;
      var a = b.getAttribute('data-acc');
      if (a === 'editar') abrirForm(v);
      else if (a === 'historial') historial(v);
      else if (a === 'inhabilitar') inhabilitar(v);
      else if (a === 'habilitar') habilitar(v);
    });
    var t;
    $('flBuscar').addEventListener('input', function (e) {
      clearTimeout(t);
      t = setTimeout(function () { S.q = e.target.value; S.pagina = 1; pintarLista(); }, 150);
    });
    Array.prototype.forEach.call(document.querySelectorAll('.fl-chip'), function (b) {
      b.addEventListener('click', function () {
        Array.prototype.forEach.call(document.querySelectorAll('.fl-chip'), function (x) { x.classList.toggle('on', x === b); });
        S.estado = b.getAttribute('data-estado'); S.pagina = 1; pintarLista();
      });
    });
    $('flPorPagina').addEventListener('change', function (e) { S.porPagina = parseInt(e.target.value, 10) || 25; S.pagina = 1; pintarLista(); });
    $('flAnterior').addEventListener('click', function () { if (S.pagina > 1) { S.pagina--; pintarLista(); } });
    $('flSiguiente').addEventListener('click', function () { S.pagina++; pintarLista(); });
    var form = $('flForm');
    form.addEventListener('input', function (e) {
      var c = e.target.name;
      if (c === 'patente') e.target.value = e.target.value.toUpperCase();
      if (c) tocados[c] = true;
      evaluarLuego();
    });
    form.addEventListener('change', function (e) { if (e.target.type === 'checkbox') tocados.licencia_clases = true; evaluarLuego(); });
    form.addEventListener('submit', function (e) { e.preventDefault(); guardar(); });
    cargar();
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init); else init();
})();

/* Solicitudes de vehículo · Flota (Fase 1b): solicitudes en lista y calendario, repositorio de choferes y las tres pestañas.
   Reglas del proyecto que cumple: sin alert/confirm/prompt nativos (REGLA #1: ilusConfirm / ilusPrompt / ilusToast), tablas con pie
   «Mostrando A–B de N» (REGLA #4.3), formulario por tarjetas cuyo círculo pasa de rojo a verde mientras se escribe (REGLAS #15 y #42),
   nombres y horarios completos sin cortar, y celular primero (REGLA #3: la tabla se vuelve tarjetas y el calendario una agenda por día).
   El servidor es quien manda: lo que se valida acá se vuelve a validar allá (flota_reglas.validar_solicitud / evaluar_chofer) y el
   choque de horario lo decide el sello del servidor. Este módulo no maneja precios ni costos. */
(function () {
  'use strict';

  var API = '/flota/api';
  var DIAS = ['lun', 'mar', 'mié', 'jue', 'vie', 'sáb', 'dom'];
  var ICONOS = { solicitada: 'bi-hourglass-split', confirmada: 'bi-check-circle-fill', llave_entregada: 'bi-key-fill', cancelada: 'bi-x-circle' };
  var OCUPAN = ['solicitada', 'confirmada', 'llave_entregada'];
  var S = {
    tab: 'solicitudes', hoy: null, ahora: '00:00',
    config: { areas: [], clases: [], clase_sugerida: 'B', dias_alerta: 30, max_dias_uso: 7 },
    veh: [], chof: [], resumenCh: {},
    sol: [], solVista: 'proximas', solEstado: 'todas', solQ: '', solVeh: '', solPag: 1, solPor: 25, ocup: [],
    vista: 'lista', mes: null, cal: null, diaSel: null, det: null,
    editSol: null, solIntento: false, solTocados: {}, guardandoSol: false,
    chQ: '', chEstado: 'todos', chPag: 1, chPor: 25, editCh: null, chIntento: false, chTocados: {}, guardandoCh: false
  };

  function $(id) { return document.getElementById(id); }
  function esc(s) {
    return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
    });
  }
  function toast(msg, type) {
    if (typeof window.ilusToast === 'function') window.ilusToast(msg, { type: type || 'info' });
  }
  function pad(n) { return (n < 10 ? '0' : '') + n; }
  function norm(t) {
    var s = String(t == null ? '' : t).toLowerCase();
    if (s.normalize) s = s.normalize('NFD').replace(/[̀-ͯ]/g, '');
    return s.replace(/[^a-z0-9]/g, '');
  }
  function escribir(el, txt) { if (el && el.textContent !== txt) el.textContent = txt; }   // aria-live: solo si cambió
  function res(estado, msg) { return { estado: estado, msg: msg || '' }; }

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

  /* ── Fechas y horas (la fecha de hoy y la hora vienen del servidor, en hora de Chile) ── */
  function aFecha(iso) {
    var m = /^(\d{4})-(\d{2})-(\d{2})/.exec(iso || '');
    return m ? new Date(Number(m[1]), Number(m[2]) - 1, Number(m[3])) : null;
  }
  function fechaCorta(iso) {
    var m = /^(\d{4})-(\d{2})-(\d{2})/.exec(iso || '');
    return m ? m[3] + '/' + m[2] + '/' + m[1] : '';
  }
  function diaTexto(iso) {
    var f = aFecha(iso);
    return f ? DIAS[(f.getDay() + 6) % 7] + ' ' + fechaCorta(iso) : '';
  }
  function minutos(hhmm) {
    var m = /^(\d{2}):(\d{2})$/.exec(hhmm || '');
    return m ? parseInt(m[1], 10) * 60 + parseInt(m[2], 10) : null;
  }
  function diasHasta(iso) {
    var f = aFecha(iso), h = aFecha(S.hoy);
    return f && h ? Math.round((f - h) / 86400000) : null;
  }
  function plural(n, uno, varios) { return n + ' ' + (n === 1 ? uno : varios); }
  function duracionTexto(ini, fin) {
    var a = new Date(ini), b = new Date(fin);
    if (isNaN(a) || isNaN(b) || b <= a) return '';
    var min = Math.round((b - a) / 60000), d = Math.floor(min / 1440), h = Math.floor((min % 1440) / 60), m = min % 60;
    var p = [];
    if (d) p.push(plural(d, 'día', 'días'));
    if (h) p.push(h + ' h');
    if (m) p.push(m + ' min');
    return p.join(' ');
  }
  function despuesDeCerrar(idModal, fn) {
    var el = $(idModal);
    var inst = window.bootstrap.Modal.getInstance(el);
    if (!inst || !el.classList.contains('show')) { fn(); return; }
    el.addEventListener('hidden.bs.modal', function h() { el.removeEventListener('hidden.bs.modal', h); fn(); });
    inst.hide();
  }
  function abrirModal(id) { window.bootstrap.Modal.getOrCreateInstance($(id)).show(); }
  function cerrarModal(id) { var i = window.bootstrap.Modal.getInstance($(id)); if (i) i.hide(); }

  /* ── Pestañas ── */
  function irTab(t) {
    if (['solicitudes', 'vehiculos', 'choferes'].indexOf(t) < 0) t = 'solicitudes';
    S.tab = t;
    Array.prototype.forEach.call(document.querySelectorAll('.fl-tab[data-tab]'), function (b) {
      var on = b.getAttribute('data-tab') === t;
      b.classList.toggle('on', on);
      b.setAttribute('aria-selected', on ? 'true' : 'false');
    });
    Array.prototype.forEach.call(document.querySelectorAll('.fl-panel[data-panel]'), function (p) {
      p.hidden = p.getAttribute('data-panel') !== t;
    });
    $('flBtnSolicitud').hidden = t !== 'solicitudes';
    $('flBtnNuevo').hidden = t !== 'vehiculos';
    $('flBtnChofer').hidden = t !== 'choferes';
    try { history.replaceState(null, '', '#' + t); } catch (e) { /* sin historial: no importa */ }
    if (t === 'solicitudes' && S.vista === 'calendario') cargarCal();
  }

  /* ═══ CARGA ═══ */
  function cargarVeh() {
    return api(API + '/vehiculos').then(function (d) {
      if (!d.ok) return;
      S.veh = d.vehiculos || [];
      if (d.config) S.config = d.config;
      S.hoy = S.hoy || d.hoy;
      pintarFiltroVeh();
    });
  }
  function cargarChof() {
    return api(API + '/choferes').then(function (d) {
      if (!d.ok) { pintarChVacio('No se pudo leer el repositorio de choferes', d.error || 'Intenta de nuevo en un momento.'); return; }
      S.chof = d.choferes || [];
      S.resumenCh = d.resumen || {};
      if (d.config) S.config = d.config;
      S.hoy = S.hoy || d.hoy;
      var r = S.resumenCh;
      $('flKpiChHab').textContent = r.habilitados != null ? r.habilitados : '–';
      $('flKpiChInhab').textContent = r.inhabilitados != null ? r.inhabilitados : '–';
      $('flKpiChPorVencer').textContent = r.por_vencer != null ? r.por_vencer : '–';
      $('flKpiChVencidas').textContent = r.vencidas != null ? r.vencidas : '–';
      pintarChoferes();
    });
  }
  function cargarSol() {
    return api(API + '/solicitudes?vista=' + S.solVista).then(function (d) {
      if (!d.ok) { toast(d.error || 'No se pudo leer las solicitudes.', 'error'); pintarSolVacio('No se pudo leer las solicitudes', d.error || 'Intenta de nuevo en un momento.'); return; }
      S.sol = d.solicitudes || [];
      S.hoy = d.hoy || S.hoy;
      S.ahora = d.ahora_hhmm || S.ahora;
      if (d.config) S.config = d.config;
      if (d.areas) S.config.areas = d.areas;
      if (d.max_dias) S.config.max_dias_uso = d.max_dias;
      var r = d.resumen || {};
      $('flKpiPorConfirmar').textContent = r.por_confirmar != null ? r.por_confirmar : '–';
      $('flKpiConfirmadas').textContent = r.confirmadas != null ? r.confirmadas : '–';
      $('flKpiLlave').textContent = r.llave_entregada != null ? r.llave_entregada : '–';
      $('flKpiHoy').textContent = r.hoy != null ? r.hoy : '–';
      if (!S.mes && S.hoy) S.mes = S.hoy.slice(0, 7);
      pintarSolLista();
    });
  }
  function cargarCal() {
    if (!S.mes) S.mes = (S.hoy || '').slice(0, 7);
    return api(API + '/calendario?mes=' + encodeURIComponent(S.mes)).then(function (d) {
      if (!d.ok) { toast(d.error || 'No se pudo leer el calendario.', 'error'); return; }
      S.cal = d;
      S.mes = d.mes;
      pintarCal();
    });
  }
  function recargarSol() {
    var p = [cargarSol()];
    if (S.vista === 'calendario') p.push(cargarCal());
    return Promise.all(p);
  }
  function pintarFiltroVeh() {
    var sel = $('flSolVeh'), actual = sel.value;
    sel.innerHTML = '<option value="">Todos los vehículos</option>' + S.veh.map(function (v) {
      return '<option value="' + v.id + '">' + esc(v.patente_fmt + ' · ' + (v.titulo || v.tipo)) + '</option>';
    }).join('');
    sel.value = actual;
  }

  /* ═══ SOLICITUDES · LISTA ═══ */
  function pill(s) {
    return '<span class="fl-est fl-est--' + esc(s.estado) + '"><i class="bi ' + (ICONOS[s.estado] || 'bi-circle') + '"></i>' + esc(s.estado_texto) + '</span>';
  }
  function solFiltradas() {
    var q = norm(S.solQ);
    return S.sol.filter(function (s) {
      if (S.solEstado !== 'todas' && s.estado !== S.solEstado) return false;
      if (S.solVeh && String(s.vehiculo.id) !== S.solVeh) return false;
      if (!q) return true;
      return norm([s.numero, s.vehiculo.patente_fmt, s.vehiculo.titulo, s.area, s.solicitante, s.chofer.nombre, s.motivo].join(' ')).indexOf(q) >= 0;
    });
  }
  function pintarSolVacio(titulo, texto) {
    var v = $('flSolVacio');
    v.innerHTML = '<b>' + esc(titulo) + '</b>' + esc(texto || '');
    v.hidden = false;
    $('flSolTabla').style.display = 'none';
    $('flSolPie').style.display = 'none';
  }
  function botonesFila(s) {
    var p = s.puede || {}, b = ['<button type="button" class="fl-btn" data-acc="ver"><i class="bi bi-eye"></i>Ver</button>'];
    if (p.confirmar) b.push('<button type="button" class="fl-btn fl-btn--ok" data-acc="confirmar"><i class="bi bi-check-circle"></i>Confirmar</button>');
    if (p.llave) b.push('<button type="button" class="fl-btn fl-btn--llave" data-acc="llave"><i class="bi bi-key"></i>Llave entregada</button>');
    if (p.cancelar) b.push('<button type="button" class="fl-btn fl-btn--peligro" data-acc="cancelar"><i class="bi bi-x-circle"></i>Cancelar</button>');
    return b.join('');
  }
  function filaSol(s) {
    return '<tr class="' + (s.estado === 'cancelada' ? 'inhab' : '') + '" data-id="' + s.id + '">' +
      '<td class="fl-td-veh"><span class="fl-num">' + esc(s.numero) + '</span><span class="fl-sub">Sellada el ' + esc(s.sello_fmt) + '</span></td>' +
      '<td data-label="Vehículo"><span class="fl-pat">' + esc(s.vehiculo.patente_fmt) + '</span><span class="fl-tit">' + esc(s.vehiculo.titulo) + '</span></td>' +
      '<td data-label="Cuándo"><b class="fl-cuando">' + esc(s.rango_texto) + '</b></td>' +
      '<td data-label="Área y quién pidió"><b>' + esc(s.area) + '</b><span class="fl-sub">Pidió ' + esc(s.solicitante || '—') + '</span>' +
      '<span class="fl-sub">' + esc(s.motivo) + '</span></td>' +
      '<td data-label="Chofer">' + esc(s.chofer.nombre) + (s.chofer.telefono ? '<span class="fl-sub">' + esc(s.chofer.telefono) + '</span>' : '') + '</td>' +
      '<td data-label="Estado">' + pill(s) + estadoExtra(s) + '</td>' +
      '<td data-label="Acciones"><div class="fl-acc">' + botonesFila(s) + '</div></td></tr>';
  }
  function estadoExtra(s) {
    if (s.estado === 'llave_entregada') return '<span class="fl-sub">' + esc((s.llave_por || '') + ' · ' + (s.llave_fmt || '')) + '</span>';
    if (s.estado === 'confirmada') return '<span class="fl-sub">' + esc((s.confirmada_por || '') + ' · ' + (s.confirmada_fmt || '')) + '</span>';
    if (s.estado === 'cancelada') return '<span class="fl-sub">' + esc(s.cancelada_motivo || '') + '</span>';
    return '';
  }
  function pintarSolLista() {
    var lista = solFiltradas();
    if (!S.sol.length) {
      pintarSolVacio(S.solVista === 'pasadas' ? 'No hay solicitudes pasadas' : 'Aún no hay solicitudes por delante',
        S.veh.length ? 'Crea la primera con «Nueva solicitud».' : 'Primero agrega un vehículo y un chofer; después podrás reservar.');
      return;
    }
    if (!lista.length) { pintarSolVacio('Ninguna solicitud coincide', 'Prueba con otra búsqueda o cambia los filtros.'); return; }
    $('flSolVacio').hidden = true;
    $('flSolTabla').style.display = '';
    $('flSolPie').style.display = '';
    var paginas = Math.max(1, Math.ceil(lista.length / S.solPor));
    if (S.solPag > paginas) S.solPag = paginas;
    var ini = (S.solPag - 1) * S.solPor, pag = lista.slice(ini, ini + S.solPor);
    $('flSolCuerpo').innerHTML = pag.map(filaSol).join('');
    $('flSolPieInfo').textContent = 'Mostrando ' + (ini + 1) + '–' + (ini + pag.length) + ' de ' + lista.length;
    $('flSolPaginaN').textContent = 'Página ' + S.solPag + ' de ' + paginas;
    $('flSolAnterior').disabled = S.solPag <= 1;
    $('flSolSiguiente').disabled = S.solPag >= paginas;
  }
  function buscarSol(id) {
    var todas = S.sol.concat((S.cal && S.cal.solicitudes) || []), r = null;
    todas.forEach(function (s) { if (String(s.id) === String(id)) r = s; });
    return r;
  }

  /* ═══ SOLICITUDES · CALENDARIO ═══ */
  function horaEnDia(s, iso) {
    if (s.fecha === s.fecha_hasta) return s.hora_desde + '–' + s.hora_hasta;
    if (iso === s.fecha) return 'desde ' + s.hora_desde;
    if (iso === s.fecha_hasta) return 'hasta ' + s.hora_hasta;
    return 'todo el día';
  }
  function solDelDia(iso) {
    return ((S.cal && S.cal.solicitudes) || []).filter(function (s) { return s.fecha <= iso && s.fecha_hasta >= iso; });
  }
  function filtradasCal(lista) {
    return lista.filter(function (s) { return !S.solVeh || String(s.vehiculo.id) === S.solVeh; });
  }
  function pintarCal() {
    var c = S.cal;
    if (!c) return;
    $('flCalTitulo').textContent = c.mes_texto;
    var cab = DIAS.map(function (d) { return '<div class="fl-cal-d">' + d + '</div>'; }).join('');
    var celdas = '';
    for (var i = 0; i < (c.dias[0] ? c.dias[0].semana : 0); i++) celdas += '<div class="fl-dia fl-dia--vacio" aria-hidden="true"></div>';
    var totalReservas = {}, enNoLaboral = 0;
    c.dias.forEach(function (d) {
      var items = filtradasCal(solDelDia(d.fecha));
      items.forEach(function (s) { totalReservas[s.id] = true; });
      if (d.no_laboral && items.some(function (s) { return s.fecha === d.fecha; })) enNoLaboral++;
      var cls = (d.no_laboral ? ' fl-dia--nolab' : '') + (d.fecha === c.hoy ? ' fl-dia--hoy' : '') + (items.length ? ' fl-dia--con' : '');
      var chips = items.slice(0, 3).map(function (s) {
        return '<span class="fl-cs fl-cs--' + esc(s.estado) + '"><b>' + esc(horaEnDia(s, d.fecha)) + '</b> ' + esc(s.vehiculo.patente_fmt) + '</span>';
      }).join('');
      var mas = items.length > 3 ? '<span class="fl-cs-mas">+' + (items.length - 3) + ' más</span>' : '';
      var puntos = items.slice(0, 4).map(function (s) { return '<i class="fl-pto fl-pto--' + esc(s.estado) + '"></i>'; }).join('');
      var etiqueta = diaTexto(d.fecha) + (d.nombre ? ', feriado: ' + d.nombre : (d.no_laboral ? ', fin de semana' : '')) + ', ' + plural(items.length, 'reserva', 'reservas');
      celdas += '<button type="button" class="fl-dia' + cls + '" data-fecha="' + d.fecha + '" aria-label="' + esc(etiqueta) + '">' +
        '<span class="fl-dia-n">' + d.dia + '</span>' + (d.nombre ? '<span class="fl-dia-nom">' + esc(d.nombre) + '</span>' : '') +
        '<span class="fl-puntos">' + puntos + '</span>' + chips + mas + '</button>';
    });
    $('flCalGrid').innerHTML = cab + celdas;
    var habiles = c.dias.filter(function (d) { return !d.no_laboral; }).length;
    var info = plural(Object.keys(totalReservas).length, 'reserva', 'reservas') + ' este mes · ' + plural(habiles, 'día hábil', 'días hábiles');
    if (enNoLaboral) info += ' · ' + plural(enNoLaboral, 'día no laboral con salida', 'días no laborales con salida') + ' (ILUS no opera esos días)';
    $('flCalInfo').textContent = info;
    pintarAgenda();
  }
  function pintarAgenda() {
    var c = S.cal, html = '';
    c.dias.forEach(function (d) {
      var items = filtradasCal(solDelDia(d.fecha));
      if (!items.length && !d.nombre) return;
      html += '<div class="fl-ag-dia' + (d.no_laboral ? ' fl-ag-dia--nolab' : '') + '"><div class="fl-ag-cab"><b>' + esc(diaTexto(d.fecha)) + '</b>' +
        (d.nombre ? '<span>Feriado: ' + esc(d.nombre) + '</span>' : '') + '</div>' +
        items.map(function (s) {
          return '<button type="button" class="fl-ag-res" data-id="' + s.id + '">' + pill(s) +
            '<span><b>' + esc(horaEnDia(s, d.fecha)) + ' · ' + esc(s.vehiculo.patente_fmt) + '</b><span>' + esc(s.area) + ' · ' + esc(s.chofer.nombre) + '</span></span></button>';
        }).join('') + '</div>';
    });
    $('flCalAgenda').innerHTML = html || '<div class="fl-vacio"><b>Sin reservas este mes</b>Toca un día del calendario o «Nueva solicitud» para reservar.</div>';
  }
  function mesMas(delta) {
    var p = (S.mes || (S.hoy || '').slice(0, 7)).split('-');
    var d = new Date(Number(p[0]), Number(p[1]) - 1 + delta, 1);
    S.mes = d.getFullYear() + '-' + pad(d.getMonth() + 1);
    cargarCal();
  }
  function cambiarVista(v) {
    S.vista = v;
    Array.prototype.forEach.call(document.querySelectorAll('.fl-fchip[data-vista]'), function (b) { b.classList.toggle('on', b.getAttribute('data-vista') === v); });
    $('flSolListaWrap').hidden = v !== 'lista';
    $('flSolCalWrap').hidden = v !== 'calendario';
    if (v === 'calendario') cargarCal();
  }

  /* Día del calendario: quién tiene cada vehículo y de qué hora a qué hora */
  function bloquesDia(items, iso) {
    var DESDE = 360, HASTA = 1320;      // la pista va de 06:00 a 22:00
    return items.map(function (s) {
      var a = s.fecha === iso ? minutos(s.hora_desde) : 0, b = s.fecha_hasta === iso ? minutos(s.hora_hasta) : 1440;
      a = Math.max(a, DESDE); b = Math.min(b, HASTA);
      if (b <= a) return '';
      return '<button type="button" class="fl-blq fl-blq--' + esc(s.estado) + '" data-id="' + s.id + '" style="left:' + ((a - DESDE) * 100 / (HASTA - DESDE)).toFixed(2) +
        '%;width:' + ((b - a) * 100 / (HASTA - DESDE)).toFixed(2) + '%" title="' + esc(horaEnDia(s, iso) + ' · ' + s.area) + '" aria-label="' + esc(s.numero + ', ' + horaEnDia(s, iso)) + '"></button>';
    }).join('');
  }
  function abrirDia(iso) {
    S.diaSel = iso;
    var d = ((S.cal && S.cal.dias) || []).filter(function (x) { return x.fecha === iso; })[0] || {};
    $('flModalDiaTitulo').textContent = diaTexto(iso);
    var items = solDelDia(iso), html = '';
    if (d.no_laboral) {
      html += '<div class="fl-aviso fl-aviso--warn"><i class="bi bi-calendar-x"></i><span>' + (d.nombre ? 'Feriado: ' + esc(d.nombre) + '.' : 'Es fin de semana.') +
        ' ILUS no opera ese día; puedes reservar igual, pero no habrá atención.</span></div>';
    }
    var vehs = S.veh.filter(function (v) { return v.habilitado; });
    if (!vehs.length) html += '<div class="fl-vacio"><b>No hay vehículos habilitados</b>Agrega uno en la pestaña Vehículos.</div>';
    vehs.forEach(function (v) {
      var propias = items.filter(function (s) { return s.vehiculo.id === v.id; });
      html += '<div class="fl-linea"><div class="fl-linea-cab"><span class="fl-pat">' + esc(v.patente_fmt) + '</span><b>' + esc(v.titulo || v.tipo) + '</b></div>' +
        '<div class="fl-pista">' + bloquesDia(propias, iso) + '</div>' +
        '<div class="fl-pista-horas"><span>06:00</span><span>10:00</span><span>14:00</span><span>18:00</span><span>22:00</span></div>' +
        (propias.length ? '<div class="fl-res-lista">' + propias.map(function (s) {
          return '<button type="button" class="fl-res" data-id="' + s.id + '">' + pill(s) + '<span><b>' + esc(horaEnDia(s, iso)) + '</b> · ' + esc(s.area) +
            '<span class="fl-sub">Chofer ' + esc(s.chofer.nombre) + ' · pidió ' + esc(s.solicitante || '—') + '</span></span></button>';
        }).join('') + '</div>' : '<div class="fl-libre"><i class="bi bi-check-circle"></i>Libre todo el día</div>') + '</div>';
    });
    $('flDiaCuerpo').innerHTML = html;
    abrirModal('flModalDia');
  }

  /* ═══ SOLICITUDES · DETALLE Y ACCIONES DE LA GESTIÓN ═══ */
  function bloqueDet(titulo, cuerpo) { return '<div class="fl-det-b"><span class="fl-det-t">' + esc(titulo) + '</span><div>' + cuerpo + '</div></div>'; }
  function pintarDetalle(s, eventos) {
    $('flModalSolDetTitulo').textContent = 'Solicitud ' + s.numero;
    var ex = '';
    if (s.confirmada_por) ex += bloqueDet('Confirmada', esc(s.confirmada_por) + '<span class="fl-sub">' + esc(s.confirmada_fmt) + '</span>');
    if (s.estado === 'llave_entregada' || s.llave_por) {
      ex += bloqueDet('Llave entregada', esc(s.llave_por || '') + '<span class="fl-sub">' + esc(s.llave_fmt) + '</span>' + (s.llave_nota ? '<span class="fl-sub">' + esc(s.llave_nota) + '</span>' : ''));
    }
    if (s.estado === 'cancelada') ex += bloqueDet('Cancelada', esc(s.cancelada_por || '') + '<span class="fl-sub">' + esc(s.cancelada_fmt) + '</span><span class="fl-sub">' + esc(s.cancelada_motivo || '') + '</span>');
    if (s.modificada_por) ex += bloqueDet('Última modificación', esc(s.modificada_por) + '<span class="fl-sub">' + esc(s.modificada_fmt) + '</span>');
    $('flSolDetCuerpo').innerHTML =
      '<div class="fl-det-cab"><span class="fl-num">' + esc(s.numero) + '</span>' + pill(s) + '</div>' +
      '<div class="fl-det-grid">' +
      bloqueDet('Vehículo', '<span class="fl-pat">' + esc(s.vehiculo.patente_fmt) + '</span><span class="fl-tit">' + esc(s.vehiculo.titulo) + '</span>') +
      bloqueDet('Cuándo', '<b class="fl-cuando">' + esc(s.rango_texto) + '</b><span class="fl-sub">' + esc(duracionTexto(s.inicio, s.fin)) + '</span>') +
      bloqueDet('Chofer que retira la llave', esc(s.chofer.nombre) + (s.chofer.telefono ? '<span class="fl-sub">' + esc(s.chofer.telefono) + '</span>' : '')) +
      bloqueDet('Área que lo pidió', esc(s.area) + '<span class="fl-sub">Pidió ' + esc(s.solicitante || '—') + '</span><span class="fl-sub">Sellada el ' + esc(s.sello_fmt) + '</span>') +
      bloqueDet('Motivo', esc(s.motivo)) + ex + '</div>' +
      '<h6 class="fl-det-h">Bitácora</h6><div class="fl-hist">' + (eventos || []).map(function (e) {
        return '<div class="fl-ev fl-ev--' + esc(e.accion) + '"><div class="fl-ev-t">' + esc(e.titulo) + '</div><div class="fl-ev-m">' + esc(e.fecha) +
          (e.usuario ? ' · ' + esc(e.usuario) : '') + '</div>' + (e.lineas.length ? '<ul>' + e.lineas.map(function (l) { return '<li>' + esc(l) + '</li>'; }).join('') + '</ul>' : '') + '</div>';
      }).join('') + '</div>';
    var p = s.puede || {}, b = [];
    if (p.confirmar) b.push('<button type="button" class="btn btn-success" data-dacc="confirmar"><i class="bi bi-check-circle me-1"></i>Confirmar</button>');
    if (p.llave) b.push('<button type="button" class="btn btn-dark" data-dacc="llave"><i class="bi bi-key me-1"></i>Llave entregada</button>');
    if (p.editar) b.push('<button type="button" class="btn btn-outline-secondary" data-dacc="editar"><i class="bi bi-pencil me-1"></i>Mover o cambiar</button>');
    if (p.cancelar) b.push('<button type="button" class="btn btn-outline-danger" data-dacc="cancelar"><i class="bi bi-x-circle me-1"></i>Cancelar</button>');
    b.push('<button type="button" class="btn btn-outline-secondary" data-bs-dismiss="modal">Cerrar</button>');
    $('flSolDetAcc').innerHTML = b.join('');
  }
  function refrescarDetalle(id) {
    return api(API + '/solicitudes/' + id).then(function (d) {
      if (!d.ok) { $('flSolDetCuerpo').innerHTML = '<div class="text-danger">' + esc(d.error || 'No se pudo leer la solicitud.') + '</div>'; $('flSolDetAcc').innerHTML = ''; return; }
      S.det = d.solicitud;
      pintarDetalle(d.solicitud, d.eventos);
    });
  }
  function abrirDetalle(id) {
    $('flSolDetCuerpo').innerHTML = '<div class="text-muted">Cargando…</div>';
    $('flSolDetAcc').innerHTML = '';
    abrirModal('flModalSolDet');
    refrescarDetalle(id);
  }
  function tras(d, okMsg) {
    if (!d.ok) {
      toast(d.error || 'No se pudo completar la acción.', 'error');
      if (d.solicitud) recargarSol();
      return false;
    }
    toast(okMsg, 'success');
    recargarSol();
    if ($('flModalSolDet').classList.contains('show') && S.det) refrescarDetalle(S.det.id);
    return true;
  }
  function confirmar(s) {
    window.ilusConfirm({
      title: 'Confirmar ' + s.numero, message: '¿Dejas confirmada esta reserva?',
      sub: s.vehiculo.patente_fmt + ' · ' + s.rango_texto + ' · chofer ' + s.chofer.nombre, okLabel: 'Confirmar', cancelLabel: 'Volver'
    }).then(function (ok) {
      if (!ok) return;
      api(API + '/solicitudes/' + s.id + '/confirmar', { method: 'POST', body: {} }).then(function (d) { tras(d, s.numero + ' quedó confirmada.'); });
    });
  }
  function entregarLlave(s) {
    window.ilusPrompt({
      title: 'Llave entregada · ' + s.numero,
      message: 'Se registra la hora de ahora y que se la entregaste a ' + s.chofer.nombre + '. Si quieres, deja una nota.',
      placeholder: 'Nota (opcional)', okLabel: 'Marcar como entregada', required: false, type: 'question'
    }).then(function (nota) {
      if (nota == null) return;
      api(API + '/solicitudes/' + s.id + '/llave', { method: 'POST', body: { nota: nota } }).then(function (d) { tras(d, 'Llave entregada a ' + s.chofer.nombre + '.'); });
    });
  }
  function cancelar(s) {
    window.ilusPrompt({
      title: 'Cancelar ' + s.numero,
      message: 'El horario queda libre para otras solicitudes. Cuéntanos por qué se cancela; queda en la bitácora.',
      sub: s.estado === 'llave_entregada' ? 'Ojo: la llave ya había sido entregada.' : '',
      placeholder: 'Motivo', okLabel: 'Cancelar solicitud', cancelLabel: 'Volver', type: 'danger', multiline: true, required: true
    }).then(function (motivo) {
      if (motivo == null) return;
      api(API + '/solicitudes/' + s.id + '/cancelar', { method: 'POST', body: { motivo: motivo } }).then(function (d) { tras(d, s.numero + ' quedó cancelada.'); });
    });
  }
  function accion(a, s) {
    if (a === 'ver') abrirDetalle(s.id);
    else if (a === 'confirmar') confirmar(s);
    else if (a === 'llave') entregarLlave(s);
    else if (a === 'cancelar') cancelar(s);
    else if (a === 'editar') despuesDeCerrar('flModalSolDet', function () { abrirSolicitud(s); });
  }

  /* ═══ SOLICITUDES · FORMULARIO (nueva y mover) ═══ */
  var NOMBRES_SOL = {
    fecha: 'el día', hora_desde: 'la hora de inicio', hora_hasta: 'la hora de término', fecha_hasta: 'el día de devolución',
    vehiculo_id: 'el vehículo', chofer_id: 'el chofer', area: 'el área', motivo: 'el motivo', motivo_cambio: 'el motivo del cambio'
  };
  function campoSol(c) { return document.querySelector('#flFormSol .fl-campo[data-campo="' + c + '"]'); }
  function vSol(c) { var el = $('flFormSol').elements[c]; return el && el.value != null ? String(el.value) : ''; }
  function vehSel() { var r = document.querySelector('#flSolVehiculos input:checked'); return r ? r.value : ''; }
  function vehPorId(id) { return S.veh.filter(function (v) { return String(v.id) === String(id); })[0] || null; }
  function chofPorId(id) { return S.chof.filter(function (c) { return String(c.id) === String(id); })[0] || null; }
  function ventana() {
    var f = vSol('fecha'), d = vSol('hora_desde'), h = vSol('hora_hasta');
    var fh = $('flSolOtroDia').checked ? (vSol('fecha_hasta') || f) : f;
    if (!f || minutos(d) == null || minutos(h) == null) return null;
    return { ini: f + 'T' + d, fin: fh + 'T' + h, f: f, fh: fh };
  }
  function choques(campo, id, ven) {
    if (!ven || !id) return [];
    var propio = S.editSol ? S.editSol.id : null;
    return S.ocup.filter(function (s) {
      if (OCUPAN.indexOf(s.estado) < 0 || s.id === propio) return false;
      var sid = campo === 'v' ? s.vehiculo.id : s.chofer.id;
      return String(sid) === String(id) && s.inicio < ven.fin && s.fin > ven.ini;
    });
  }
  function textoChoque(s) {
    return s.numero + ' · ' + s.area + ' · ' + s.rango_texto + ' (pidió ' + (s.solicitante || 'otra persona') + ' el ' + s.sello_fmt + ')';
  }
  function reglaFecha() {
    var v = vSol('fecha');
    if (!v) return res('bad', 'Elige el día.');
    var f = aFecha(v);
    if (!f || f.getFullYear() < 2000 || f.getFullYear() > 2100) return res('bad', 'Fecha no válida.');
    if (!S.editSol && S.hoy && v < S.hoy) return res('bad', 'Ese día ya pasó. Elige hoy o una fecha futura.');
    var dow = (f.getDay() + 6) % 7;
    if (dow >= 5) return res('warn', diaTexto(v) + ' es fin de semana: ILUS no opera ese día. Puedes reservar igual.');
    var d = diasHasta(v);
    return res('ok', d === 0 ? 'Es hoy.' : (d === 1 ? 'Es mañana.' : ''));
  }
  function reglaHoraDesde() {
    var v = vSol('hora_desde');
    if (!v) return res('bad', 'Indica a qué hora lo necesitas.');
    return minutos(v) == null ? res('bad', 'Hora no válida (usa el formato 08:30).') : res('ok', '');
  }
  function reglaHoraHasta() {
    var v = vSol('hora_hasta');
    if (!v) return res('bad', 'Indica hasta qué hora lo usarás.');
    if (minutos(v) == null) return res('bad', 'Hora no válida (usa el formato 08:30).');
    var ven = ventana();
    if (!ven) return res('ok', '');
    if (ven.fin <= ven.ini) return res('bad', 'La hora de término debe ser después de la de inicio.');
    if (S.hoy && ven.fin <= S.hoy + 'T' + S.ahora) return res('bad', 'Ese horario ya terminó. Elige uno que no haya pasado.');
    if (new Date(ven.fin) - new Date(ven.ini) > (S.config.max_dias_uso || 7) * 86400000) return res('bad', 'Una solicitud puede durar hasta ' + (S.config.max_dias_uso || 7) + ' días.');
    return res('ok', 'Uso de ' + duracionTexto(ven.ini, ven.fin) + '.');
  }
  function reglaFechaHasta() {
    if (!$('flSolOtroDia').checked) return res('');
    var v = vSol('fecha_hasta');
    if (!v) return res('bad', 'Elige el día de devolución.');
    if (vSol('fecha') && v < vSol('fecha')) return res('bad', 'No puede ser antes del día de salida.');
    return res('ok', '');
  }
  function reglaVehiculo() {
    var id = vehSel();
    if (!id) return res('bad', 'Elige un vehículo.');
    var v = vehPorId(id);
    if (!v) return res('bad', 'Elige un vehículo de la lista.');
    if (!v.habilitado) return res('bad', v.patente_fmt + ' está inhabilitado.');
    var ch = choques('v', id, ventana());
    if (ch.length) return res('bad', v.patente_fmt + ' ya está reservado en ese horario: ' + textoChoque(ch[0]) + '. Elige otro horario o el otro vehículo.');
    var f = vSol('fecha'), venc = (v.documentos || []).filter(function (d) { return d.vence && f && d.vence < f; });
    if (venc.length) return res('warn', 'Ojo: ' + venc.map(function (d) { return d.nombre + ' vence el ' + fechaCorta(d.vence); }).join(', ') + ', antes del día de uso.');
    return res('ok', ventana() ? 'Libre en ese horario.' : '');
  }
  function reglaChofer() {
    var id = vSol('chofer_id');
    if (!id) return res('bad', 'Elige el chofer que retirará la llave.');
    var c = chofPorId(id);
    if (!c) return res('bad', 'Elige un chofer de la lista.');
    if (!c.habilitado || !c.usuario_activo) return res('bad', c.nombre + ' está inhabilitado como chofer.');
    var v = vehPorId(vehSel());
    if (v) {
      var exig = v.licencia_clases || [];
      if (exig.length && !exig.some(function (x) { return (c.licencia_clases || []).indexOf(x) >= 0; })) {
        return res('bad', c.nombre + ' no tiene registrada la licencia que exige este vehículo (clase ' + exig.join(' o ') + ').');
      }
    }
    if (!c.licencia_vence) return res('bad', c.nombre + ' no tiene registrado el vencimiento de su licencia.');
    var fin = (ventana() || {}).fh || vSol('fecha');
    if (fin && c.licencia_vence < fin) return res('bad', 'La licencia de ' + c.nombre + ' vence el ' + c.licencia_vence_fmt + ', antes de terminar este uso.');
    var ch = choques('c', id, ventana());
    if (ch.length) return res('bad', c.nombre + ' ya tiene otra reserva en ese horario: ' + ch[0].numero + ' · ' + ch[0].vehiculo.patente_fmt + ' · ' + ch[0].rango_texto + '.');
    if (c.licencia_estado === 'por_vencer') return res('warn', 'Su licencia ' + c.licencia_texto_dias.toLowerCase() + ' (' + c.licencia_vence_fmt + ').');
    return res('ok', 'Licencia vigente hasta el ' + c.licencia_vence_fmt + '.');
  }
  function reglaArea() {
    var v = vSol('area').trim();
    if (!v) return res('bad', 'Indica desde qué área lo pides (por ejemplo Logística o Servicio Técnico).');
    if (v.length < 2) return res('bad', 'El área es muy corta.');
    return res('ok', '');
  }
  function reglaMotivo() {
    var v = vSol('motivo').trim();
    if (v.length < 5) return res('bad', 'Cuenta para qué lo necesitas (mínimo 5 caracteres).');
    return res('ok', '');
  }
  function reglaCambio() {
    if (!S.editSol) return res('');
    return vSol('motivo_cambio').trim().length < 5 ? res('bad', 'Cuenta brevemente por qué (mínimo 5 caracteres).') : res('ok', '');
  }
  function marcarSol(c, r) {
    var el = campoSol(c);
    if (!el) return;
    var visible = r.estado !== 'bad' || S.solTocados[c] || S.solIntento;
    el.classList.remove('ok', 'bad', 'warn');
    if (visible && r.estado) el.classList.add(r.estado);
    escribir(el.querySelector('.fl-msg'), visible ? r.msg : '');
  }
  function disponibilidadTarjetas() {
    var ven = ventana();
    Array.prototype.forEach.call(document.querySelectorAll('#flSolVehiculos .fl-vcard-disp'), function (el) {
      var id = el.getAttribute('data-veh'), v = vehPorId(id), t, cls;
      if (!v || !v.habilitado) { t = 'Inhabilitado'; cls = 'off'; }
      else if (!ven) { t = 'Elige día y horario para ver si está libre'; cls = 'sin'; }
      else {
        var ch = choques('v', id, ven);
        if (ch.length) { t = 'Ocupado: ' + ch[0].numero + ' · ' + ch[0].area + ' · ' + ch[0].rango_texto; cls = 'ocupado'; }
        else { t = 'Libre en ese horario'; cls = 'libre'; }
      }
      el.className = 'fl-vcard-disp fl-vcard-disp--' + cls;
      escribir(el, t);
    });
    Array.prototype.forEach.call(document.querySelectorAll('#flSolVehiculos .fl-vcard'), function (l) {
      var i = l.querySelector('input');
      l.classList.toggle('on', !!(i && i.checked));
    });
  }
  function evaluarSol() {
    var r = {
      fecha: reglaFecha(), hora_desde: reglaHoraDesde(), hora_hasta: reglaHoraHasta(), fecha_hasta: reglaFechaHasta(),
      vehiculo_id: reglaVehiculo(), chofer_id: reglaChofer(), area: reglaArea(), motivo: reglaMotivo(), motivo_cambio: reglaCambio()
    };
    Object.keys(r).forEach(function (c) { marcarSol(c, r[c]); });
    disponibilidadTarjetas();
    function paso(n, campos) {
      var el = document.querySelector('#flFormSol .fl-paso[data-paso="' + n + '"]');
      if (!el) return;
      var e = campos.map(function (c) { return r[c].estado; });
      el.classList.remove('ok', 'bad', 'warn');
      el.classList.add(e.indexOf('bad') >= 0 ? 'bad' : (e.indexOf('warn') >= 0 ? 'warn' : 'ok'));
    }
    paso(1, ['fecha', 'hora_desde', 'hora_hasta', 'fecha_hasta']);
    paso(2, ['vehiculo_id']);
    paso(3, ['chofer_id']);
    paso(4, ['area', 'motivo']);
    paso(5, ['motivo_cambio']);
    var faltan = Object.keys(r).filter(function (c) { return r[c].estado === 'bad'; });
    var pie = $('flSolPieForm');
    pie.className = 'fl-pie-form' + (faltan.length && S.solIntento ? ' bad' : '');
    if (faltan.length) escribir(pie, 'Falta: ' + faltan.map(function (c) { return NOMBRES_SOL[c]; }).join(', ') + '.');
    else {
      var v = vehPorId(vehSel()), c = chofPorId(vSol('chofer_id')), ven = ventana();
      escribir(pie, 'Todo listo: ' + (v ? v.patente_fmt : '') + ' · ' + (ven ? diaTexto(ven.f) + ' ' + vSol('hora_desde') + ' a ' + vSol('hora_hasta') : '') + ' · chofer ' + (c ? c.nombre : ''));
    }
    return { reglas: r, faltan: faltan };
  }
  var agendarSol;
  function evaluarSolLuego() { clearTimeout(agendarSol); agendarSol = setTimeout(evaluarSol, 0); }   // setTimeout, nunca requestAnimationFrame

  function tarjetasVehiculos(selId) {
    if (!S.veh.length) {
      $('flSolVehiculos').innerHTML = '<div class="fl-vacio"><b>No hay vehículos registrados</b>Agrega uno en la pestaña Vehículos para poder reservar.</div>';
      return;
    }
    $('flSolVehiculos').innerHTML = S.veh.map(function (v) {
      var dis = !v.habilitado;
      return '<label class="fl-vcard' + (dis ? ' off' : '') + '"><input type="radio" name="vehiculo_id" value="' + v.id + '"' +
        (String(selId) === String(v.id) ? ' checked' : '') + (dis ? ' disabled' : '') + '>' +
        '<span class="fl-vcard-c"><span class="fl-pat">' + esc(v.patente_fmt) + '</span><b class="fl-vcard-t">' + esc(v.titulo || v.tipo) + '</b>' +
        '<span class="fl-vcard-s">Licencia: ' + esc(v.licencia_texto) + (dis ? ' · Inhabilitado' : '') + '</span>' +
        '<span class="fl-vcard-disp" data-veh="' + v.id + '"></span></span></label>';
    }).join('');
  }
  function opcionesChofer(selId) {
    var o = ['<option value="">Elige al chofer que retira la llave…</option>'];
    S.chof.forEach(function (c) {
      var mal = !c.habilitado || !c.usuario_activo || c.licencia_estado === 'vencido' || c.licencia_estado === 'sin_fecha';
      var nota = !c.habilitado || !c.usuario_activo ? ' · inhabilitado' : (c.licencia_estado === 'vencido' ? ' · licencia vencida' : '');
      var txt = c.nombre + ' · ' + c.licencia_texto + (c.licencia_vence_fmt ? ' · vence ' + c.licencia_vence_fmt : '') + nota;
      o.push('<option value="' + c.id + '"' + (mal && String(c.id) !== String(selId) ? ' disabled' : '') + '>' + esc(txt) + '</option>');
    });
    return o.join('');
  }
  function refrescarOcup() {
    return api(API + '/solicitudes?vista=proximas').then(function (d) { if (d.ok) S.ocup = d.solicitudes || []; });
  }
  function abrirSolicitud(s, pref) {
    pref = pref || {};
    S.editSol = s || null; S.solIntento = false; S.solTocados = {};
    $('flModalSolTitulo').textContent = s ? 'Mover o cambiar ' + s.numero : 'Nueva solicitud de vehículo';
    $('flSolGuardar').innerHTML = s ? '<i class="bi bi-check2-circle me-1"></i>Guardar cambios' : '<i class="bi bi-send-check me-1"></i>Solicitar vehículo';
    $('flSolPasoCambio').hidden = !s;
    var f = $('flFormSol');
    f.reset();
    $('flSolFecha').min = s ? '' : (S.hoy || '');
    $('flSolFechaHasta').min = S.hoy || '';
    tarjetasVehiculos(s ? s.vehiculo.id : (pref.vehiculo_id || ''));
    $('flSolChofer').innerHTML = opcionesChofer(s ? s.chofer.id : '');
    if (s) $('flSolChofer').value = String(s.chofer.id);
    $('flSolAreasList').innerHTML = (S.config.areas || []).map(function (a) { return '<option value="' + esc(a) + '">'; }).join('');
    $('flSolAreasRapidas').innerHTML = (S.config.areas || []).map(function (a) {
      return '<button type="button" class="fl-sug" data-area="' + esc(a) + '">' + esc(a) + '</button>';
    }).join('');
    f.elements.fecha.value = s ? s.fecha : (pref.fecha || S.hoy || '');
    f.elements.hora_desde.value = s ? s.hora_desde : '';
    f.elements.hora_hasta.value = s ? s.hora_hasta : '';
    var otro = !!(s && s.fecha_hasta !== s.fecha);
    $('flSolOtroDia').checked = otro;
    $('flSolOtroDiaCampo').hidden = !otro;
    f.elements.fecha_hasta.value = otro ? s.fecha_hasta : '';
    f.elements.area.value = s ? s.area : '';
    f.elements.motivo.value = s ? s.motivo : '';
    abrirModal('flModalSol');
    evaluarSol();
    // Trae lo más reciente (reservas vigentes, vehículos y choferes) y vuelve a mostrar la disponibilidad real.
    Promise.all([refrescarOcup(), cargarVeh(), cargarChof()]).then(function () {
      var v = vehSel(), c = vSol('chofer_id');
      tarjetasVehiculos(v || (s ? s.vehiculo.id : (pref.vehiculo_id || '')));
      $('flSolChofer').innerHTML = opcionesChofer(c);
      $('flSolChofer').value = c;
      evaluarSol();
    });
  }
  function payloadSol() {
    return {
      fecha: vSol('fecha'), hora_desde: vSol('hora_desde'), hora_hasta: vSol('hora_hasta'),
      fecha_hasta: $('flSolOtroDia').checked ? vSol('fecha_hasta') : '',
      vehiculo_id: vehSel(), chofer_id: vSol('chofer_id'), area: vSol('area'), motivo: vSol('motivo'), motivo_cambio: vSol('motivo_cambio')
    };
  }
  function marcarErroresSol(errores) {
    Object.keys(errores || {}).forEach(function (c) {
      var el = campoSol(c);
      if (!el) return;
      el.classList.remove('ok', 'warn'); el.classList.add('bad');
      escribir(el.querySelector('.fl-msg'), errores[c]);
    });
    var primero = Object.keys(errores || {})[0], elp = primero && campoSol(primero);
    if (elp) { var inp = elp.querySelector('input,textarea,select'); if (inp) inp.focus(); }
  }
  function guardarSol() {
    if (S.guardandoSol) return;
    S.solIntento = true;
    var ev = evaluarSol();
    if (ev.faltan.length) {
      toast('Falta: ' + ev.faltan.map(function (c) { return NOMBRES_SOL[c]; }).join(', ') + '.', 'warning');
      var malo = document.querySelector('#flFormSol .fl-campo.bad');
      if (malo) { var i = malo.querySelector('input,textarea,select'); if (i) i.focus(); }
      return;
    }
    var editando = S.editSol;
    S.guardandoSol = true;
    $('flSolGuardar').disabled = true;
    api(editando ? API + '/solicitudes/' + editando.id : API + '/solicitudes', { method: editando ? 'PUT' : 'POST', body: payloadSol() }).then(function (d) {
      S.guardandoSol = false; $('flSolGuardar').disabled = false;
      if (!d.ok) {
        if (d.errores) marcarErroresSol(d.errores);
        toast(d.error || 'No se pudo guardar la solicitud.', 'error');
        if (d.error_codigo === 'CHOQUE_VEHICULO' || d.error_codigo === 'CHOQUE_CHOFER') refrescarOcup().then(evaluarSol);
        return;
      }
      cerrarModal('flModalSol');
      toast(editando ? 'Reserva actualizada.' : 'Solicitud ' + d.solicitud.numero + ' enviada. Quedó sellada a tu nombre.', 'success');
      (d.avisos || []).slice(0, 3).forEach(function (a) { toast(a, 'warning'); });
      recargarSol();
    });
  }

  /* ═══ CHOFERES ═══ */
  function chFiltrados() {
    var q = norm(S.chQ);
    return S.chof.filter(function (c) {
      if (S.chEstado === 'habilitados' && !c.habilitado) return false;
      if (S.chEstado === 'inhabilitados' && c.habilitado) return false;
      if (!q) return true;
      return norm([c.nombre, c.username, c.licencia_texto, c.cargo].join(' ')).indexOf(q) >= 0;
    });
  }
  function pintarChVacio(titulo, texto) {
    var v = $('flChVacio');
    v.innerHTML = '<b>' + esc(titulo) + '</b>' + esc(texto || '');
    v.hidden = false;
    $('flChTabla').style.display = 'none';
    $('flChPie').style.display = 'none';
  }
  function filaCh(c) {
    var estado = c.habilitado
      ? '<span class="fl-estado fl-estado--on"><i class="bi bi-check-circle-fill"></i>Habilitado</span>'
      : '<span class="fl-estado fl-estado--off"><i class="bi bi-slash-circle"></i>Inhabilitado</span><span class="fl-motivo">' + esc(c.inhabilitado_motivo || '') +
        (c.inhabilitado_por ? ' · ' + esc(c.inhabilitado_por) : '') + (c.inhabilitado_fmt ? ' · ' + esc(c.inhabilitado_fmt) : '') + '</span>';
    var lic = '<div class="fl-doc fl-doc--' + (c.licencia_estado === 'sin_fecha' ? 'sin_fecha' : esc(c.licencia_estado)) + '"><b>' + esc(c.licencia_texto) + '</b><span>' +
      (c.licencia_vence_fmt ? esc(c.licencia_vence_fmt) + ' · ' : '') + esc(c.licencia_texto_dias) + '</span></div>';
    var cambiar = c.habilitado
      ? '<button type="button" class="fl-btn fl-btn--peligro" data-acc="inhabilitar"><i class="bi bi-slash-circle"></i>Inhabilitar</button>'
      : '<button type="button" class="fl-btn fl-btn--ok" data-acc="habilitar"><i class="bi bi-check-circle"></i>Habilitar</button>';
    return '<tr class="' + (c.habilitado ? '' : 'inhab') + '" data-id="' + c.id + '">' +
      '<td class="fl-td-veh"><span class="fl-nombre">' + esc(c.nombre) + '</span>' +
      (c.cargo ? '<span class="fl-sub">' + esc(c.cargo) + '</span>' : '') + (c.telefono ? '<span class="fl-sub">' + esc(c.telefono) + '</span>' : '') +
      (c.usuario_activo ? '' : '<span class="fl-sub fl-sub--bad">Su usuario de ILUS está desactivado</span>') + '</td>' +
      '<td class="fl-td-docs" data-label="Licencia">' + lic + '</td>' +
      '<td data-label="Estado">' + estado + '</td>' +
      '<td data-label="Acciones"><div class="fl-acc"><button type="button" class="fl-btn" data-acc="editar"><i class="bi bi-pencil"></i>Editar</button>' +
      '<button type="button" class="fl-btn" data-acc="historial"><i class="bi bi-clock-history"></i>Historial</button>' + cambiar + '</div></td></tr>';
  }
  function pintarChoferes() {
    var lista = chFiltrados();
    if (!S.chof.length) { pintarChVacio('Aún no hay choferes registrados', 'Agrega el primero con «Agregar chofer». Debe ser un usuario de ILUS.'); return; }
    if (!lista.length) { pintarChVacio('Ningún chofer coincide', 'Prueba con otra búsqueda o cambia el filtro de estado.'); return; }
    $('flChVacio').hidden = true;
    $('flChTabla').style.display = '';
    $('flChPie').style.display = '';
    var paginas = Math.max(1, Math.ceil(lista.length / S.chPor));
    if (S.chPag > paginas) S.chPag = paginas;
    var ini = (S.chPag - 1) * S.chPor, pag = lista.slice(ini, ini + S.chPor);
    $('flChCuerpo').innerHTML = pag.map(filaCh).join('');
    $('flChPieInfo').textContent = 'Mostrando ' + (ini + 1) + '–' + (ini + pag.length) + ' de ' + lista.length;
    $('flChPaginaN').textContent = 'Página ' + S.chPag + ' de ' + paginas;
    $('flChAnterior').disabled = S.chPag <= 1;
    $('flChSiguiente').disabled = S.chPag >= paginas;
  }
  function campoCh(c) { return document.querySelector('#flFormCh .fl-campo[data-campo="' + c + '"]'); }
  function vCh(c) { var el = $('flFormCh').elements[c]; return el && el.value != null ? String(el.value) : ''; }
  function clasesCh() { return Array.prototype.slice.call(document.querySelectorAll('#flChClases input:checked')).map(function (i) { return i.value; }); }
  var NOMBRES_CH = { usuario_id: 'el usuario', licencia_clases: 'la clase de licencia', licencia_vence: 'el vencimiento de la licencia' };
  function reglasCh() {
    var r = {};
    r.usuario_id = S.editCh ? res('ok', '') : (vCh('usuario_id') ? res('ok', '') : res('bad', 'Elige a qué usuario de ILUS corresponde este chofer.'));
    r.licencia_clases = clasesCh().length ? res('ok', 'Licencia clase ' + clasesCh().join(' y ') + '.') : res('bad', 'Elige al menos una clase de licencia (por ejemplo B).');
    var v = vCh('licencia_vence');
    if (!v) r.licencia_vence = res('bad', 'Indica cuándo vence la licencia: sin esa fecha no se puede comprobar que esté vigente.');
    else {
      var f = aFecha(v), d = diasHasta(v);
      if (!f || f.getFullYear() < 2000 || f.getFullYear() > 2100) r.licencia_vence = res('bad', 'Fecha no válida.');
      else if (d != null && d < 0) r.licencia_vence = res('bad', 'Venció hace ' + (-d) + ' días: no podrá recibir reservas.');
      else if (d != null && d <= (S.config.dias_alerta || 30)) r.licencia_vence = res('warn', 'Vence ' + (d === 0 ? 'hoy' : (d === 1 ? 'mañana' : 'en ' + d + ' días')) + '.');
      else r.licencia_vence = res('ok', d != null ? 'Vigente · vence en ' + d + ' días.' : '');
    }
    return r;
  }
  function marcarCh(c, r) {
    var el = campoCh(c);
    if (!el) return;
    var visible = r.estado !== 'bad' || S.chTocados[c] || S.chIntento;
    el.classList.remove('ok', 'bad', 'warn');
    if (visible && r.estado) el.classList.add(r.estado);
    escribir(el.querySelector('.fl-msg'), visible ? r.msg : '');
  }
  function evaluarCh() {
    var r = reglasCh();
    Object.keys(r).forEach(function (c) { marcarCh(c, r[c]); });
    Array.prototype.forEach.call(document.querySelectorAll('#flChClases .fl-clase'), function (l) { l.classList.toggle('on', l.querySelector('input').checked); });
    function paso(n, campos) {
      var el = document.querySelector('#flFormCh .fl-paso[data-paso="' + n + '"]');
      if (!el) return;
      var e = campos.map(function (c) { return r[c].estado; });
      el.classList.remove('ok', 'bad', 'warn');
      el.classList.add(e.indexOf('bad') >= 0 ? 'bad' : (e.indexOf('warn') >= 0 ? 'warn' : 'ok'));
    }
    paso(1, ['usuario_id']); paso(2, ['licencia_clases', 'licencia_vence']);
    var p3 = document.querySelector('#flFormCh .fl-paso[data-paso="3"]');
    if (p3) { p3.classList.remove('bad', 'warn'); p3.classList.add('ok'); }
    var faltan = Object.keys(r).filter(function (c) { return r[c].estado === 'bad'; });
    var pie = $('flChPieForm');
    pie.className = 'fl-pie-form' + (faltan.length && S.chIntento ? ' bad' : '');
    escribir(pie, faltan.length ? 'Falta: ' + faltan.map(function (c) { return NOMBRES_CH[c]; }).join(', ') + '.' : 'Todo listo para guardar.');
    return { reglas: r, faltan: faltan };
  }
  var agendarCh;
  function evaluarChLuego() { clearTimeout(agendarCh); agendarCh = setTimeout(evaluarCh, 0); }
  function pintarClasesCh(sel) {
    $('flChClases').innerHTML = (S.config.clases || []).map(function (c) {
      return '<label class="fl-clase"><input type="checkbox" value="' + esc(c.codigo) + '"' + (sel.indexOf(c.codigo) >= 0 ? ' checked' : '') + '>' +
        '<span><b>Clase ' + esc(c.codigo) + '</b><span>' + esc(c.nombre) + '</span></span></label>';
    }).join('');
  }
  function abrirChofer(c) {
    S.editCh = c || null; S.chIntento = false; S.chTocados = {};
    $('flModalChoferTitulo').textContent = c ? 'Editar chofer ' + c.nombre : 'Agregar chofer';
    $('flChGuardar').innerHTML = '<i class="bi bi-check2-circle me-1"></i>' + (c ? 'Guardar cambios' : 'Guardar chofer');
    var f = $('flFormCh');
    f.reset();
    var campoUsuario = campoCh('usuario_id');
    campoUsuario.hidden = !!c;
    $('flChFijo').hidden = !c;
    $('flChFijo').innerHTML = c ? '<b>' + esc(c.nombre) + '</b>' + (c.cargo ? '<span class="fl-sub">' + esc(c.cargo) + '</span>' : '') + (c.telefono ? '<span class="fl-sub">' + esc(c.telefono) + '</span>' : '') : '';
    $('flChUsuario').innerHTML = '<option value="">Cargando usuarios…</option>';
    pintarClasesCh(c ? c.licencia_clases : [S.config.clase_sugerida || 'B']);
    f.elements.licencia_vence.value = c && c.licencia_vence ? c.licencia_vence : '';
    f.elements.notas.value = c && c.notas ? c.notas : '';
    abrirModal('flModalChofer');
    evaluarCh();
    if (!c) {
      api(API + '/choferes/usuarios').then(function (d) {
        if (!d.ok) { $('flChUsuario').innerHTML = '<option value="">No se pudo leer la lista de usuarios</option>'; toast(d.error || 'No se pudo leer la lista de usuarios.', 'error'); return; }
        $('flChUsuario').innerHTML = '<option value="">Elige un usuario de ILUS…</option>' + (d.usuarios || []).map(function (u) {
          return '<option value="' + u.id + '">' + esc(u.nombre + (u.cargo ? ' · ' + u.cargo : '') + (u.username ? ' (' + u.username + ')' : '')) + '</option>';
        }).join('');
        if (!(d.usuarios || []).length) $('flChUsuario').innerHTML = '<option value="">Todos los usuarios activos ya son choferes</option>';
        evaluarCh();
      });
    }
  }
  function guardarCh() {
    if (S.guardandoCh) return;
    S.chIntento = true;
    var ev = evaluarCh();
    if (ev.faltan.length) {
      toast('Falta: ' + ev.faltan.map(function (c) { return NOMBRES_CH[c]; }).join(', ') + '.', 'warning');
      var malo = document.querySelector('#flFormCh .fl-campo.bad');
      if (malo) { var i = malo.querySelector('input,textarea,select'); if (i) i.focus(); }
      return;
    }
    var d = diasHasta(vCh('licencia_vence'));
    // Alertar, no trabar: una licencia ya vencida se puede dejar registrada, pero esa persona no podrá recibir reservas.
    var seguir = d != null && d < 0
      ? window.ilusConfirm({
        title: 'La licencia ya está vencida', message: 'Este chofer no podrá recibir reservas hasta que registres una fecha vigente. ¿Guardar igual?',
        sub: 'Venció el ' + fechaCorta(vCh('licencia_vence')), okLabel: 'Guardar igual', cancelLabel: 'Revisar'
      })
      : Promise.resolve(true);
    seguir.then(function (ok) {
      if (!ok) return;
      var editando = S.editCh;
      var body = { licencia_clases: clasesCh(), licencia_vence: vCh('licencia_vence'), notas: vCh('notas') };
      if (!editando) body.usuario_id = vCh('usuario_id');
      S.guardandoCh = true;
      $('flChGuardar').disabled = true;
      api(editando ? API + '/choferes/' + editando.id : API + '/choferes', { method: editando ? 'PUT' : 'POST', body: body }).then(function (r) {
        S.guardandoCh = false; $('flChGuardar').disabled = false;
        if (!r.ok) {
          Object.keys(r.errores || {}).forEach(function (c) {
            var el = campoCh(c);
            if (!el) return;
            el.classList.remove('ok', 'warn'); el.classList.add('bad');
            escribir(el.querySelector('.fl-msg'), r.errores[c]);
          });
          toast(r.error || 'No se pudo guardar el chofer.', 'error');
          return;
        }
        cerrarModal('flModalChofer');
        toast(r.sin_cambios ? 'No había cambios que guardar.' : (editando ? 'Chofer actualizado.' : 'Chofer agregado y habilitado.'), 'success');
        cargarChof();
      });
    });
  }
  function inhabilitarCh(c) {
    window.ilusPrompt({
      title: 'Inhabilitar a ' + c.nombre,
      message: 'Cuéntanos por qué deja de ser chofer por ahora (licencia suspendida, licencia médica, cambio de funciones…). Queda en su historial.',
      placeholder: 'Motivo', okLabel: 'Inhabilitar', type: 'warning', multiline: true, required: true
    }).then(function (motivo) {
      if (motivo == null) return;
      api(API + '/choferes/' + c.id + '/inhabilitar', { method: 'POST', body: { motivo: motivo } }).then(function (d) {
        if (!d.ok) { toast(d.error || 'No se pudo inhabilitar.', 'error'); return; }
        toast(c.nombre + ' quedó inhabilitado como chofer.', 'success');
        (d.avisos || []).forEach(function (a) { toast(a, 'warning'); });
        cargarChof();
      });
    });
  }
  function habilitarCh(c) {
    window.ilusConfirm({
      title: 'Habilitar a ' + c.nombre, message: '¿Dejarlo disponible de nuevo como chofer?',
      sub: c.inhabilitado_motivo ? 'Estaba inhabilitado por: ' + c.inhabilitado_motivo : '', okLabel: 'Habilitar', cancelLabel: 'Cancelar'
    }).then(function (ok) {
      if (!ok) return;
      api(API + '/choferes/' + c.id + '/habilitar', { method: 'POST', body: {} }).then(function (d) {
        if (!d.ok) { toast(d.error || 'No se pudo habilitar.', 'error'); return; }
        toast(c.nombre + ' quedó habilitado.', 'success');
        (d.avisos || []).forEach(function (a) { toast(a, 'warning'); });
        cargarChof();
      });
    });
  }
  function historialCh(c) {
    $('flModalHistTitulo').textContent = 'Historial de ' + c.nombre;
    $('flHistCuerpo').innerHTML = '<div class="text-muted">Cargando…</div>';
    abrirModal('flModalHist');
    api(API + '/choferes/' + c.id + '/historial').then(function (d) {
      if (!d.ok) { $('flHistCuerpo').innerHTML = '<div class="text-danger">' + esc(d.error || 'No se pudo leer el historial.') + '</div>'; return; }
      if (!d.eventos.length) { $('flHistCuerpo').innerHTML = '<div class="text-muted">Todavía no hay movimientos.</div>'; return; }
      $('flHistCuerpo').innerHTML = d.eventos.map(function (e) {
        return '<div class="fl-ev fl-ev--' + esc(e.accion) + '"><div class="fl-ev-t">' + esc(e.titulo) + '</div><div class="fl-ev-m">' + esc(e.fecha) +
          (e.usuario ? ' · ' + esc(e.usuario) : '') + '</div>' + (e.lineas.length ? '<ul>' + e.lineas.map(function (l) { return '<li>' + esc(l) + '</li>'; }).join('') + '</ul>' : '') + '</div>';
      }).join('');
    });
  }

  /* ═══ EVENTOS ═══ */
  function chips(selector, attr, fn) {
    Array.prototype.forEach.call(document.querySelectorAll(selector), function (b) {
      b.addEventListener('click', function () {
        Array.prototype.forEach.call(document.querySelectorAll(selector), function (x) { x.classList.toggle('on', x === b); });
        fn(b.getAttribute(attr));
      });
    });
  }
  function init() {
    Array.prototype.forEach.call(document.querySelectorAll('.fl-tab[data-tab]'), function (b) {
      b.addEventListener('click', function () { irTab(b.getAttribute('data-tab')); });
    });
    $('flBtnSolicitud').addEventListener('click', function () { abrirSolicitud(null); });
    $('flBtnChofer').addEventListener('click', function () { abrirChofer(null); });

    /* Solicitudes: vista, filtros y paginación */
    chips('.fl-fchip[data-vista]', 'data-vista', cambiarVista);
    chips('.fl-fchip[data-sest]', 'data-sest', function (v) { S.solEstado = v; S.solPag = 1; pintarSolLista(); });
    chips('.fl-fchip[data-svista]', 'data-svista', function (v) { S.solVista = v; S.solPag = 1; cargarSol(); });
    var t;
    $('flSolBuscar').addEventListener('input', function (e) {
      clearTimeout(t);
      t = setTimeout(function () { S.solQ = e.target.value; S.solPag = 1; pintarSolLista(); }, 150);
    });
    $('flSolVeh').addEventListener('change', function (e) { S.solVeh = e.target.value; S.solPag = 1; pintarSolLista(); if (S.vista === 'calendario') pintarCal(); });
    $('flSolPorPagina').addEventListener('change', function (e) { S.solPor = parseInt(e.target.value, 10) || 25; S.solPag = 1; pintarSolLista(); });
    $('flSolAnterior').addEventListener('click', function () { if (S.solPag > 1) { S.solPag--; pintarSolLista(); } });
    $('flSolSiguiente').addEventListener('click', function () { S.solPag++; pintarSolLista(); });
    $('flSolCuerpo').addEventListener('click', function (ev) {
      var b = ev.target.closest('[data-acc]');
      if (!b) return;
      var s = buscarSol(b.closest('tr').getAttribute('data-id'));
      if (s) accion(b.getAttribute('data-acc'), s);
    });
    $('flSolDetAcc').addEventListener('click', function (ev) {
      var b = ev.target.closest('[data-dacc]');
      if (b && S.det) accion(b.getAttribute('data-dacc'), S.det);
    });

    /* Calendario */
    $('flCalPrev').addEventListener('click', function () { mesMas(-1); });
    $('flCalSig').addEventListener('click', function () { mesMas(1); });
    $('flCalHoy').addEventListener('click', function () { S.mes = (S.hoy || '').slice(0, 7); cargarCal(); });
    $('flCalGrid').addEventListener('click', function (ev) {
      var b = ev.target.closest('.fl-dia[data-fecha]');
      if (b) abrirDia(b.getAttribute('data-fecha'));
    });
    $('flCalAgenda').addEventListener('click', function (ev) {
      var b = ev.target.closest('[data-id]');
      if (b) abrirDetalle(b.getAttribute('data-id'));
    });
    $('flDiaCuerpo').addEventListener('click', function (ev) {
      var b = ev.target.closest('[data-id]');
      if (b) { var id = b.getAttribute('data-id'); despuesDeCerrar('flModalDia', function () { abrirDetalle(id); }); }
    });
    $('flDiaReservar').addEventListener('click', function () {
      var iso = S.diaSel;
      despuesDeCerrar('flModalDia', function () { abrirSolicitud(null, { fecha: iso }); });
    });

    /* Formulario de solicitud */
    var fs = $('flFormSol');
    fs.addEventListener('input', function (e) { if (e.target.name) S.solTocados[e.target.name] = true; evaluarSolLuego(); });
    fs.addEventListener('change', function (e) {
      if (e.target.name) S.solTocados[e.target.name] = true;
      if (e.target.id === 'flSolOtroDia') {
        $('flSolOtroDiaCampo').hidden = !e.target.checked;
        if (e.target.checked && !vSol('fecha_hasta')) fs.elements.fecha_hasta.value = vSol('fecha');
      }
      evaluarSolLuego();
    });
    fs.addEventListener('submit', function (e) { e.preventDefault(); guardarSol(); });
    $('flSolGuardar').addEventListener('click', guardarSol);
    $('flSolAreasRapidas').addEventListener('click', function (ev) {
      var b = ev.target.closest('[data-area]');
      if (!b) return;
      fs.elements.area.value = b.getAttribute('data-area');
      S.solTocados.area = true;
      evaluarSolLuego();
    });

    /* Choferes */
    $('flChCuerpo').addEventListener('click', function (ev) {
      var b = ev.target.closest('[data-acc]');
      if (!b) return;
      var c = chofPorId(b.closest('tr').getAttribute('data-id'));
      if (!c) return;
      var a = b.getAttribute('data-acc');
      if (a === 'editar') abrirChofer(c);
      else if (a === 'historial') historialCh(c);
      else if (a === 'inhabilitar') inhabilitarCh(c);
      else if (a === 'habilitar') habilitarCh(c);
    });
    var tc;
    $('flChBuscar').addEventListener('input', function (e) {
      clearTimeout(tc);
      tc = setTimeout(function () { S.chQ = e.target.value; S.chPag = 1; pintarChoferes(); }, 150);
    });
    chips('.fl-fchip[data-chest]', 'data-chest', function (v) { S.chEstado = v; S.chPag = 1; pintarChoferes(); });
    $('flChPorPagina').addEventListener('change', function (e) { S.chPor = parseInt(e.target.value, 10) || 25; S.chPag = 1; pintarChoferes(); });
    $('flChAnterior').addEventListener('click', function () { if (S.chPag > 1) { S.chPag--; pintarChoferes(); } });
    $('flChSiguiente').addEventListener('click', function () { S.chPag++; pintarChoferes(); });
    var fc = $('flFormCh');
    fc.addEventListener('input', function (e) { if (e.target.name) S.chTocados[e.target.name] = true; evaluarChLuego(); });
    fc.addEventListener('change', function (e) {
      if (e.target.type === 'checkbox') S.chTocados.licencia_clases = true;
      if (e.target.name) S.chTocados[e.target.name] = true;
      evaluarChLuego();
    });
    fc.addEventListener('submit', function (e) { e.preventDefault(); guardarCh(); });
    $('flChGuardar').addEventListener('click', guardarCh);

    var h = (location.hash || '').replace('#', '');
    Promise.all([cargarVeh(), cargarChof(), cargarSol()]).then(function () { irTab(h || 'solicitudes'); });
    irTab(h || 'solicitudes');
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init); else init();
})();

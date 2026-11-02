/* Solicitudes de vehículo · Flota (Fases 1b y 1c): solicitudes en lista y calendario, repositorio de choferes (RUT, nombre completo,
   dirección y PDF de la licencia) y las tres pestañas. Quién ve qué lo decide el servidor; acá solo se esconden los botones que no corresponden.
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
  var PERM = window.FLOTA_PERMISOS || {};
  var S = {
    tab: 'solicitudes', hoy: null, ahora: '00:00',
    config: { areas: [], clases: [], clase_sugerida: 'B', dias_alerta: 30, max_dias_uso: 7 },
    veh: [], chof: [], resumenCh: {},
    sol: [], solVista: 'proximas', solEstado: 'todas', solQ: '', solVeh: '', solPag: 1, solPor: 25, ocup: [],
    vista: 'lista', mes: null, cal: null, diaSel: null, det: null,
    meses: {}, semana: null, mesVista: null, calArea: '', pk: { mes: null, ancla: null },
    editSol: null, solIntento: false, solTocados: {}, guardandoSol: false,
    chQ: '', chEstado: 'todos', chPag: 1, chPor: 25, editCh: null, chIntento: false, chTocados: {}, guardandoCh: false, verArchCh: false, usuariosCh: []
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
    $('flBtnNuevo').hidden = t !== 'vehiculos' || !PERM.admin;        // agregar vehículos y choferes: solo el superadministrador
    $('flBtnChofer').hidden = t !== 'choferes' || !PERM.admin;
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
    return api(API + '/choferes' + (S.verArchCh ? '?archivados=1' : '')).then(function (d) {
      if (!d.ok) { pintarChVacio('No se pudo leer el repositorio de choferes', d.error || 'Intenta de nuevo en un momento.'); return; }
      S.chof = d.choferes || [];
      S.resumenCh = d.resumen || {};
      if (d.permisos) PERM = d.permisos;
      var cac = $('flChChipArchivados');
      cac.hidden = !PERM.admin;
      cac.textContent = 'Archivados' + (S.resumenCh.archivados ? ' (' + S.resumenCh.archivados + ')' : '');
      $('flBtnChofer').hidden = S.tab !== 'choferes' || !PERM.admin;
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
  function recargarSol() {
    var p = [cargarSol()];
    if (S.vista === 'calendario') p.push(cargarCal(true));
    else S.meses = {};
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
    var todas = S.sol.concat(todasCal()), r = null;
    todas.forEach(function (s) { if (String(s.id) === String(id)) r = s; });
    return r;
  }

  /* ═══ SOLICITUDES · AGENDA DE LA FLOTA (calendario) ═══
     Mismo esqueleto que la Agenda interna de Retiros (Daniel, 02-nov-2026: «fíjate el de Retiros… algo potente»; «tengo que tener
     claro cuándo voy a pedir… cuándo no puedo topar con Logística… el que más tiene problemas es el KIA»). Lee el mismo
     /flota/api/calendario?mes=, mes a mes, y guarda cada mes leído (una semana puede cruzar dos meses). Todo lo que se calcula acá
     (demanda, hueco libre) solo INFORMA: el choque lo sigue decidiendo el sello del servidor. */
  var AREAS = {
    log: { n: 'Logística', ic: 'bi-box-seam' }, sst: { n: 'Servicio Técnico', ic: 'bi-tools' },
    ger: { n: 'Gerencia', ic: 'bi-briefcase' }, mkt: { n: 'Marketing', ic: 'bi-megaphone' }, otra: { n: 'Otra área', ic: 'bi-three-dots' }
  };
  var JORNADA = { desde: 8 * 60, hasta: 18 * 60 };   // horas hábiles para medir la demanda (L–V, sin feriados)
  var HUECO_MIN = 120;                                // un hueco «sirve» si deja al menos 2 horas seguidas
  var PX_HORA = 40;
  var DIAS_LARGO = ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo'];

  function areaKey(a) {
    var n = norm(a);
    if (n.indexOf('logist') >= 0) return 'log';
    if (n.indexOf('servicio') >= 0 || n.indexOf('tecnic') >= 0 || n === 'sstt') return 'sst';
    if (n.indexOf('gerenc') >= 0) return 'ger';
    if (n.indexOf('market') >= 0) return 'mkt';
    return 'otra';
  }
  function isoDe(d) { return d.getFullYear() + '-' + pad(d.getMonth() + 1) + '-' + pad(d.getDate()); }
  function sumarDias(iso, n) { var d = aFecha(iso); d.setDate(d.getDate() + n); return isoDe(d); }
  function lunesDe(iso) { var d = aFecha(iso); d.setDate(d.getDate() - (d.getDay() + 6) % 7); return isoDe(d); }
  function hhmm(min) { min = Math.max(0, Math.min(1440, min)); return pad(Math.floor(min / 60) % 24 === 0 && min === 1440 ? 24 : Math.floor(min / 60)) + ':' + pad(min % 60); }
  function ahoraCL() {
    try {
      var t = new Date().toLocaleString('sv-SE', { timeZone: 'America/Santiago' }).replace(' ', 'T');   // «2026-11-02T14:05:09»
      if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}/.test(t)) return { iso: t.slice(0, 10), hm: t.slice(11, 16), dt: t.slice(0, 16) };
    } catch (e) { /* navegador sin zona horaria: usa la hora que mandó el servidor */ }
    return { iso: S.hoy || '', hm: S.ahora || '00:00', dt: (S.hoy || '') + 'T' + (S.ahora || '00:00') };
  }
  function vehFoto(v, cls) {
    return v && v.foto_id
      ? '<img class="' + cls + '" src="/flota/api/archivos/' + v.foto_id + '" alt="" loading="lazy">'
      : '<span class="' + cls + ' ' + cls + '--sin" aria-hidden="true"><i class="bi bi-truck"></i></span>';
  }
  function patCorta(v) { return v ? v.patente_fmt : ''; }

  /* ── Carga: cada mes una vez (se vacía al cambiar algo) ── */
  function cargarMes(mes) {
    if (S.meses[mes]) return Promise.resolve(S.meses[mes]);
    return api(API + '/calendario?mes=' + encodeURIComponent(mes)).then(function (d) {
      if (!d.ok) { toast(d.error || 'No se pudo leer el calendario.', 'error'); return null; }
      S.meses[d.mes] = d;
      if (d.hoy) S.hoy = d.hoy;
      return d;
    });
  }
  function cargarCal(forzar) {
    if (forzar) S.meses = {};
    var hoy = ahoraCL().iso || S.hoy;
    if (!S.semana) S.semana = lunesDe(hoy);
    if (!S.mesVista) S.mesVista = hoy.slice(0, 7);
    var meses = {};
    [S.mesVista, S.semana.slice(0, 7), sumarDias(S.semana, 6).slice(0, 7), hoy.slice(0, 7), sumarDias(hoy, 14).slice(0, 7)]
      .forEach(function (m) { meses[m] = true; });
    return Promise.all(Object.keys(meses).map(cargarMes)).then(pintarCal);
  }
  function todasCal() {
    var vistas = {}, out = [];
    Object.keys(S.meses).forEach(function (m) {
      (S.meses[m].solicitudes || []).forEach(function (s) { if (!vistas[s.id]) { vistas[s.id] = true; out.push(s); } });
    });
    return out;
  }
  function diaInfo(iso) {
    var m = S.meses[iso.slice(0, 7)], r = null;
    if (m) (m.dias || []).forEach(function (d) { if (d.fecha === iso) r = d; });
    if (r) return r;
    var dow = (aFecha(iso).getDay() + 6) % 7;
    return { fecha: iso, dia: aFecha(iso).getDate(), semana: dow, no_laboral: dow >= 5 ? 'fin_de_semana' : null, nombre: null };
  }
  function horaEnDia(s, iso) {
    if (s.fecha === s.fecha_hasta) return s.hora_desde + '–' + s.hora_hasta;
    if (iso === s.fecha) return 'desde ' + s.hora_desde;
    if (iso === s.fecha_hasta) return 'hasta ' + s.hora_hasta;
    return 'todo el día';
  }
  function tramo(s, iso) {        // minutos [a, b) que la reserva ocupa ese día
    return [s.fecha === iso ? minutos(s.hora_desde) : 0, s.fecha_hasta === iso ? minutos(s.hora_hasta) : 1440];
  }
  function solDelDia(iso) {
    return todasCal().filter(function (s) { return s.fecha <= iso && s.fecha_hasta >= iso && OCUPAN.indexOf(s.estado) >= 0; });
  }
  function vehsCal() {
    return S.veh.filter(function (v) { return v.habilitado && (!S.solVeh || String(v.id) === S.solVeh); });
  }
  function pasaFiltro(s) {
    if (S.solVeh && String(s.vehiculo.id) !== S.solVeh) return false;
    return !S.calArea || areaKey(s.area) === S.calArea;
  }
  function diasSemana() { var o = []; for (var i = 0; i < 7; i++) o.push(sumarDias(S.semana, i)); return o; }

  /* ── Ocupación: minutos tomados dentro de una ventana (las reservas de un vehículo no se pisan: el servidor lo impide) ── */
  function ocupado(vid, iso, a, b) {
    var t = 0;
    solDelDia(iso).forEach(function (s) {
      if (String(s.vehiculo.id) !== String(vid)) return;
      var r = tramo(s, iso), x = Math.max(a, r[0]), y = Math.min(b, r[1]);
      if (y > x) t += y - x;
    });
    return t;
  }
  function huecosDia(vid, iso, desde) {
    var tomados = solDelDia(iso).filter(function (s) { return String(s.vehiculo.id) === String(vid); })
      .map(function (s) { return tramo(s, iso); }).sort(function (p, q) { return p[0] - q[0]; });
    var out = [], cur = Math.max(JORNADA.desde, desde || 0);
    tomados.forEach(function (r) {
      if (r[0] > cur) out.push([cur, Math.min(r[0], JORNADA.hasta)]);
      cur = Math.max(cur, r[1]);
    });
    if (cur < JORNADA.hasta) out.push([cur, JORNADA.hasta]);
    return out.filter(function (h) { return h[1] - h[0] >= HUECO_MIN; });
  }
  function proximoHueco(vid) {
    var ahora = ahoraCL();
    for (var i = 0; i < 15; i++) {
      var iso = sumarDias(ahora.iso, i), d = diaInfo(iso);
      if (d.no_laboral) continue;
      var desde = i === 0 ? Math.ceil(minutos(ahora.hm) / 30) * 30 : 0;
      var h = huecosDia(vid, iso, desde);
      if (h.length) return { iso: iso, a: h[0][0], b: h[0][1] };
    }
    return null;
  }
  function cuandoTexto(iso) {
    var n = diasHasta(iso);
    return n === 0 ? 'Hoy' : (n === 1 ? 'Mañana' : diaTexto(iso));
  }

  /* ── Pintar ── */
  function pintarCal() {
    if (!S.semana) return;
    pintarHero();
    pintarMes();
    pintarChips();
    pintarSemana();
    pintarListaSemana();
    pintarDemanda();
    pintarLeyenda();
  }
  function enSemana() {
    var fin = sumarDias(S.semana, 6);
    return todasCal().filter(function (s) { return s.fecha <= fin && s.fecha_hasta >= S.semana && OCUPAN.indexOf(s.estado) >= 0 && pasaFiltro(s); });
  }
  function stat(n, l, ic, cls) {
    return '<div class="fla-stat"><span class="fla-stat-ic ' + cls + '"><i class="bi ' + ic + '"></i></span><span><b class="fla-stat-n">' + n + '</b><span class="fla-stat-l">' + l + '</span></span></div>';
  }
  function pintarHero() {
    var lista = enSemana(), horas = 0, cuenta = { solicitada: 0, confirmada: 0, llave_entregada: 0 };
    lista.forEach(function (s) {
      cuenta[s.estado] = (cuenta[s.estado] || 0) + 1;
      horas += Math.max(0, (new Date(s.fin) - new Date(s.inicio)) / 3600000);
    });
    $('flaStats').innerHTML =
      stat(lista.length, 'Reservas de la semana', 'bi-calendar-event', 'fla-ic--azul') +
      stat(cuenta.solicitada, 'Por confirmar', 'bi-hourglass-split', 'fla-ic--ambar') +
      stat(cuenta.confirmada, 'Confirmadas', 'bi-check-circle-fill', 'fla-ic--verde') +
      stat(cuenta.llave_entregada, 'Con la llave', 'bi-key-fill', 'fla-ic--negro') +
      stat(Math.round(horas), 'Horas reservadas', 'bi-clock-history', 'fla-ic--rojo');
    pintarProximo();
  }
  function pintarProximo() {
    var ahora = ahoraCL().dt, enUso = null, prox = null;
    todasCal().forEach(function (s) {
      if (OCUPAN.indexOf(s.estado) < 0 || (S.solVeh && String(s.vehiculo.id) !== S.solVeh)) return;
      if (s.inicio <= ahora && s.fin > ahora) { if (!enUso || s.fin < enUso.fin) enUso = s; }
      else if (s.inicio > ahora && (!prox || s.inicio < prox.inicio)) prox = s;
    });
    var s = enUso || prox, b = $('flaNext');
    b.classList.toggle('vacio', !s);
    b.classList.toggle('en-uso', !!enUso);
    b.setAttribute('data-id', s ? s.id : '');
    if (!s) { escribir($('flaNextL'), 'Próximo uso'); escribir($('flaNextCd'), '—'); escribir($('flaNextQ'), 'Sin reservas por delante'); return; }
    var mins = Math.round((new Date(enUso ? s.fin : s.inicio) - new Date(ahora)) / 60000);
    var d = Math.floor(mins / 1440), h = Math.floor((mins % 1440) / 60), m = mins % 60;
    var cd = (d ? d + ' d ' : '') + (d || h ? h + ' h ' : '') + m + ' min';
    escribir($('flaNextL'), enUso ? 'En uso ahora · se devuelve en' : 'Próximo uso en');
    escribir($('flaNextCd'), cd);
    escribir($('flaNextQ'), s.vehiculo.patente_fmt + ' · ' + s.area + ' · ' + s.chofer.nombre);
  }
  function pintarMes() {
    var c = S.meses[S.mesVista];
    if (!c) return;
    $('flCalTitulo').textContent = c.mes_texto;
    var hoy = ahoraCL().iso, finSem = sumarDias(S.semana, 6);
    var html = DIAS.map(function (d) { return '<span class="fla-mes-d">' + d.charAt(0).toUpperCase() + d.slice(1, 2) + '</span>'; }).join('');
    for (var i = 0; i < (c.dias[0] ? c.dias[0].semana : 0); i++) html += '<span class="fla-mes-v" aria-hidden="true"></span>';
    var conSalida = 0;
    c.dias.forEach(function (d) {
      var items = solDelDia(d.fecha).filter(pasaFiltro);
      if (items.length) conSalida++;
      var cls = (d.no_laboral ? ' nolab' : '') + (d.fecha === hoy ? ' hoy' : '') + (d.fecha >= S.semana && d.fecha <= finSem ? ' sem' : '');
      var ptos = items.slice(0, 4).map(function (s) { return '<i class="fla-a--' + areaKey(s.area) + '"></i>'; }).join('');
      var et = diaTexto(d.fecha) + (d.nombre ? ', feriado: ' + d.nombre : (d.no_laboral ? ', fin de semana' : '')) + ', ' + plural(items.length, 'reserva', 'reservas');
      html += '<button type="button" class="fla-mes-dia' + cls + '" data-fecha="' + d.fecha + '" aria-label="' + esc(et) + '" title="' + esc(et) + '">' +
        '<span>' + d.dia + '</span><span class="fla-ptos">' + ptos + '</span></button>';
    });
    $('flCalGrid').innerHTML = html;
    var habiles = c.dias.filter(function (d) { return !d.no_laboral; }).length;
    var fer = c.dias.filter(function (d) { return d.nombre; }).map(function (d) { return d.dia + ': ' + d.nombre; });
    $('flCalInfo').innerHTML = esc(plural(habiles, 'día hábil', 'días hábiles') + ' · ' + plural(conSalida, 'día con reservas', 'días con reservas')) +
      (fer.length ? '<span class="fla-fer"><i class="bi bi-flag"></i>Feriados: ' + esc(fer.join(' · ')) + '</span>' : '');
  }
  function pintarChips() {
    var vh = '<button type="button" class="fla-chip' + (S.solVeh ? '' : ' on') + '" data-cveh="">Todos</button>' + S.veh.filter(function (v) { return v.habilitado; }).map(function (v) {
      return '<button type="button" class="fla-chip fla-chip--veh' + (S.solVeh === String(v.id) ? ' on' : '') + '" data-cveh="' + v.id + '">' + vehFoto(v, 'fla-chip-foto') +
        '<span><b>' + esc(v.patente_fmt) + '</b><span>' + esc(v.tipo || '') + '</span></span></button>';
    }).join('');
    $('flaVehChips').innerHTML = vh;
    var ah = '<span class="fla-chips-l">Área:</span><button type="button" class="fla-chip' + (S.calArea ? '' : ' on') + '" data-carea="">Todas</button>' +
      Object.keys(AREAS).map(function (k) {
        return '<button type="button" class="fla-chip' + (S.calArea === k ? ' on' : '') + '" data-carea="' + k + '"><i class="fla-sw fla-a--' + k + '"></i>' + esc(AREAS[k].n) + '</button>';
      }).join('');
    $('flaAreaChips').innerHTML = ah;
    var fin = sumarDias(S.semana, 6);
    $('flaRango').textContent = 'Semana del ' + fechaCorta(S.semana).slice(0, 5) + ' al ' + fechaCorta(fin);
  }
  function rangoHoras(dias, vehs) {
    var a = 7 * 60, b = 20 * 60;   // 07:00 a 20:00, y se estira si alguna reserva de la semana sale de ahí
    dias.forEach(function (iso) {
      solDelDia(iso).forEach(function (s) {
        if (!vehs.some(function (v) { return v.id === s.vehiculo.id; })) return;
        var r = tramo(s, iso);
        a = Math.min(a, Math.floor(r[0] / 60) * 60); b = Math.max(b, Math.ceil(r[1] / 60) * 60);
      });
    });
    return [Math.max(0, a), Math.min(1440, b)];
  }
  function pintarSemana() {
    var dias = diasSemana(), vehs = vehsCal(), ahora = ahoraCL();
    var cont = $('flaSemana');
    if (!vehs.length) { cont.innerHTML = '<div class="fl-vacio"><b>No hay vehículos habilitados</b>Agrega o habilita uno en la pestaña Vehículos.</div>'; return; }
    var R = rangoHoras(dias, vehs), alto = (R[1] - R[0]) / 60 * PX_HORA;
    cont.style.setProperty('--fla-pistas', vehs.length);
    var html = '<div class="fla-sem-head"><div class="fla-esq">Hora</div>';
    dias.forEach(function (iso, i) {
      var d = diaInfo(iso), n = solDelDia(iso).filter(pasaFiltro).length;
      html += '<button type="button" class="fla-dh' + (d.no_laboral ? ' nolab' : '') + (iso === ahora.iso ? ' hoy' : '') + '" data-dia="' + iso + '" title="Ver el detalle del día">' +
        '<span class="fla-dh-n">' + DIAS[i] + ' <b>' + aFecha(iso).getDate() + '</b></span>' +
        (d.nombre ? '<span class="fla-dh-fer">' + esc(d.nombre) + '</span>' : (d.no_laboral ? '<span class="fla-dh-fer">No laboral</span>' : '')) +
        '<span class="fla-dh-c">' + (n ? plural(n, 'reserva', 'reservas') : 'Libre') + '</span>' +
        '<span class="fla-dh-pistas">' + vehs.map(function (v) { return '<span class="fla-dh-p" title="' + esc(v.patente_fmt + ' · ' + (v.titulo || v.tipo)) + '">' + vehFoto(v, 'fla-dh-foto') + esc(v.patente_fmt) + '</span>'; }).join('') + '</span>' +
        '</button>';
    });
    html += '</div><div class="fla-sem-body" style="height:' + alto + 'px"><div class="fla-horas">';
    for (var m = R[0]; m <= R[1]; m += 60) html += '<span style="top:' + ((m - R[0]) / 60 * PX_HORA) + 'px">' + hhmm(m) + '</span>';
    html += '</div>';
    dias.forEach(function (iso) {
      var d = diaInfo(iso);
      html += '<div class="fla-col' + (d.no_laboral ? ' nolab' : '') + (iso === ahora.iso ? ' hoy' : '') + '">';
      vehs.forEach(function (v) {
        html += '<div class="fla-pista" data-dia="' + iso + '" data-veh="' + v.id + '" data-ini="' + R[0] + '" title="Toca para pedir ' + esc(v.patente_fmt) + ' a esa hora">';
        if (iso < ahora.iso || (iso === ahora.iso)) {
          var pasadoHasta = iso < ahora.iso ? R[1] : Math.min(R[1], Math.max(R[0], minutos(ahora.hm)));
          if (pasadoHasta > R[0]) html += '<span class="fla-pasado" style="height:' + ((pasadoHasta - R[0]) / 60 * PX_HORA) + 'px"></span>';
        }
        solDelDia(iso).forEach(function (s) {
          if (s.vehiculo.id !== v.id) return;
          var r = tramo(s, iso), a = Math.max(r[0], R[0]), b = Math.min(r[1], R[1]);
          if (b <= a) return;
          var k = areaKey(s.area), h = (b - a) / 60 * PX_HORA, atenuada = !pasaFiltro(s);
          html += '<button type="button" class="fla-b fla-a--' + k + ' fla-e--' + esc(s.estado) + (atenuada ? ' tenue' : '') + (h < 34 ? ' corta' : '') + '" data-id="' + s.id + '" ' +
            'style="top:' + ((a - R[0]) / 60 * PX_HORA) + 'px;height:' + Math.max(h, 16) + 'px" ' +
            'title="' + esc(s.numero + ' · ' + s.vehiculo.patente_fmt + ' · ' + horaEnDia(s, iso) + ' · ' + s.area + ' · chofer ' + s.chofer.nombre + ' · ' + s.estado_texto) + '" ' +
            'aria-label="' + esc(s.numero + ', ' + s.vehiculo.patente_fmt + ', ' + horaEnDia(s, iso) + ', ' + s.area) + '">' +
            '<span class="fla-b-t">' + (s.estado === 'llave_entregada' ? '<i class="bi bi-key-fill"></i>' : (s.estado === 'solicitada' ? '<i class="bi bi-hourglass-split"></i>' : '')) + esc(horaEnDia(s, iso)) + '</span>' +
            '<span class="fla-b-a">' + esc(s.area) + '</span><span class="fla-b-c">' + esc(s.chofer.nombre) + '</span></button>';
        });
        html += '<span class="fla-fantasma" aria-hidden="true"></span></div>';
      });
      if (iso === ahora.iso) {
        var mn = minutos(ahora.hm);
        if (mn >= R[0] && mn <= R[1]) html += '<span class="fla-ahora" style="top:' + ((mn - R[0]) / 60 * PX_HORA) + 'px"><b>' + esc(ahora.hm) + '</b></span>';
      }
      html += '</div>';
    });
    html += '</div>';
    cont.innerHTML = html;
  }
  function pintarListaSemana() {
    var html = '', ahora = ahoraCL();
    diasSemana().forEach(function (iso, i) {
      var d = diaInfo(iso), items = solDelDia(iso).filter(pasaFiltro).sort(function (p, q) { return p.inicio < q.inicio ? -1 : 1; });
      html += '<div class="fla-ld' + (d.no_laboral ? ' nolab' : '') + (iso === ahora.iso ? ' hoy' : '') + '"><div class="fla-ld-cab"><span><b>' + DIAS_LARGO[i] + (iso === ahora.iso ? ' · hoy' : '') + '</b>' +
        '<span>' + esc(fechaCorta(iso)) + (d.nombre ? ' · Feriado: ' + esc(d.nombre) : (d.no_laboral ? ' · No laboral' : '')) + '</span></span>' +
        (iso >= ahora.iso ? '<button type="button" class="fl-btn fl-btn--chico" data-pedir="' + iso + '"><i class="bi bi-calendar-plus"></i>Pedir</button>' : '') + '</div>';
      html += items.length ? items.map(function (s) {
        var v = vehPorId(s.vehiculo.id), k = areaKey(s.area);
        return '<button type="button" class="fla-lr fla-a--' + k + '" data-id="' + s.id + '">' + vehFoto(v, 'fla-lr-foto') +
          '<span class="fla-lr-c"><b>' + esc(horaEnDia(s, iso)) + ' · ' + esc(s.vehiculo.patente_fmt) + '</b>' +
          '<span><i class="fla-sw fla-a--' + k + '"></i>' + esc(s.area) + ' · chofer ' + esc(s.chofer.nombre) + '</span></span>' + pill(s) + '</button>';
      }).join('') : '<div class="fla-ld-libre"><i class="bi bi-check-circle"></i>' + (S.solVeh ? 'Libre todo el día' : 'Todos los vehículos libres') + '</div>';
      html += '</div>';
    });
    $('flCalAgenda').innerHTML = html;
  }
  function pintarDemanda() {
    var dias = diasSemana().filter(function (iso) { return !diaInfo(iso).no_laboral; });
    var cap = dias.length * (JORNADA.hasta - JORNADA.desde);
    var html = S.veh.filter(function (v) { return v.habilitado; }).map(function (v) {
      var tomado = 0, porArea = {};
      dias.forEach(function (iso) {
        solDelDia(iso).forEach(function (s) {
          if (s.vehiculo.id !== v.id) return;
          var r = tramo(s, iso), x = Math.max(JORNADA.desde, r[0]), y = Math.min(JORNADA.hasta, r[1]);
          if (y <= x) return;
          tomado += y - x;
          var k = areaKey(s.area);
          porArea[k] = (porArea[k] || 0) + (y - x);
        });
      });
      var pct = cap ? Math.round(tomado * 100 / cap) : 0;
      var nivel = pct >= 70 ? ['alta', 'Alta demanda'] : (pct >= 40 ? ['media', 'Demanda media'] : ['baja', 'Holgado']);
      var barra = Object.keys(AREAS).filter(function (k) { return porArea[k]; }).map(function (k) {
        return '<i class="fla-a--' + k + '" style="width:' + (porArea[k] * 100 / (cap || 1)).toFixed(1) + '%" title="' + esc(AREAS[k].n + ': ' + Math.round(porArea[k] / 60 * 10) / 10 + ' h') + '"></i>';
      }).join('');
      var areasTxt = Object.keys(AREAS).filter(function (k) { return porArea[k]; }).map(function (k) { return AREAS[k].n + ' ' + Math.round(porArea[k] / 6) / 10 + ' h'; }).join(' · ');
      var hueco = proximoHueco(v.id);
      return '<div class="fla-dem fla-dem--' + nivel[0] + '">' +
        '<div class="fla-dem-cab">' + vehFoto(v, 'fla-dem-foto') + '<span><b class="fl-pat">' + esc(v.patente_fmt) + '</b><span class="fla-dem-t">' + esc(v.titulo || v.tipo) + '</span></span></div>' +
        '<div class="fla-dem-n"><b>' + pct + '%</b><span class="fla-dem-niv">' + nivel[1] + '</span></div>' +
        '<div class="fla-dem-barra" role="img" aria-label="' + esc('Ocupado ' + pct + '% del horario hábil de la semana') + '">' + barra + '</div>' +
        '<div class="fla-dem-s">' + esc(Math.round(tomado / 6) / 10 + ' h de ' + Math.round(cap / 60) + ' h hábiles (L–V 08:00–18:00)') + (areasTxt ? '<br>' + esc(areasTxt) : '') + '</div>' +
        (hueco ? '<button type="button" class="fla-hueco" data-hveh="' + v.id + '" data-hdia="' + hueco.iso + '" data-ha="' + hueco.a + '" data-hb="' + hueco.b + '">' +
          '<i class="bi bi-calendar-check"></i><span>Próximo hueco libre<b>' + esc(cuandoTexto(hueco.iso) + ' ' + hhmm(hueco.a) + '–' + hhmm(hueco.b)) + '</b></span></button>'
          : '<div class="fla-hueco fla-hueco--no"><i class="bi bi-exclamation-triangle"></i><span>Sin huecos de 2 h en las próximas 2 semanas</span></div>') +
        '</div>';
    }).join('');
    $('flaDemanda').innerHTML = html || '<div class="fl-sub">Sin vehículos habilitados.</div>';
  }
  function pintarLeyenda() {
    $('flaLeyenda').innerHTML = Object.keys(AREAS).map(function (k) {
      return '<span><i class="fla-sw fla-a--' + k + '"></i>' + esc(AREAS[k].n) + '</span>';
    }).join('') +
      '<span><i class="fla-sw fla-sw--sol"></i>Rayado: por confirmar</span>' +
      '<span><i class="fla-sw fla-sw--conf"></i>Lleno: confirmada</span>' +
      '<span><i class="bi bi-key-fill"></i>Llave entregada</span>' +
      '<span><i class="fla-sw fla-sw--nolab"></i>Feriado o fin de semana</span>';
  }
  function mesMas(delta) {
    var p = S.mesVista.split('-');
    var d = new Date(Number(p[0]), Number(p[1]) - 1 + delta, 1);
    S.mesVista = d.getFullYear() + '-' + pad(d.getMonth() + 1);
    cargarCal();
  }
  function semanaMas(delta) {
    S.semana = sumarDias(S.semana, 7 * delta);
    S.mesVista = sumarDias(S.semana, 3).slice(0, 7);
    cargarCal();
  }
  function irHoy() {
    var hoy = ahoraCL().iso;
    S.semana = lunesDe(hoy); S.mesVista = hoy.slice(0, 7);
    cargarCal();
  }
  function cambiarVista(v) {
    S.vista = v;
    Array.prototype.forEach.call(document.querySelectorAll('.fl-fchip[data-vista]'), function (b) { b.classList.toggle('on', b.getAttribute('data-vista') === v); });
    $('flSolListaWrap').hidden = v !== 'lista';
    $('flSolCalWrap').hidden = v !== 'calendario';
    $('flSolBuscarWrap').hidden = v === 'calendario';     // la agenda filtra por vehículo y área con sus propios botones
    if (v === 'calendario') cargarCal(true);
  }
  /* Tocar un hueco de la pista: abre la solicitud llena con ese vehículo, día y hora (2 h por defecto; la persona lo cambia). */
  function minutoEnPista(pista, clientY) {
    var r = pista.getBoundingClientRect(), ini = parseInt(pista.getAttribute('data-ini'), 10) || 0;
    return ini + Math.floor((clientY - r.top) / PX_HORA * 2) * 30;
  }
  function pedirEnPista(pista, clientY) {
    var iso = pista.getAttribute('data-dia'), vid = pista.getAttribute('data-veh'), a = minutoEnPista(pista, clientY), ahora = ahoraCL();
    if (iso < ahora.iso || (iso === ahora.iso && a + 30 <= minutos(ahora.hm))) { toast('Ese horario ya pasó. Elige uno desde ahora en adelante.', 'warning'); return; }
    var siguiente = 1440;
    solDelDia(iso).forEach(function (s) {
      if (String(s.vehiculo.id) !== String(vid)) return;
      var r = tramo(s, iso);
      if (r[0] >= a && r[0] < siguiente) siguiente = r[0];
    });
    var b = Math.min(a + 120, siguiente, 1439);
    if (b <= a) b = Math.min(a + 30, 1439);
    abrirSolicitud(null, { fecha: iso, vehiculo_id: vid, hora_desde: hhmm(a), hora_hasta: hhmm(b) });
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
    var d = diaInfo(iso);
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
      html += '<div class="fl-linea"><div class="fl-linea-cab">' + vehFoto(v, 'fla-dh-foto') + '<span class="fl-pat">' + esc(v.patente_fmt) + '</span><b>' + esc(v.titulo || v.tipo) + '</b></div>' +
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
  function vSol(c) {
    var el = $('flFormSol').elements[c];
    if (!el) return '';
    if (el.type === 'radio') return el.checked ? String(el.value) : '';     // un solo chofer: no es RadioNodeList
    return el.value != null ? String(el.value) : '';
  }
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
    disponibilidadChoferes();
    pkPintar();
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
  /* «¿Quién retira la llave?» y el área, en tarjetas (Daniel, 02-nov-2026: «con tarjetas… logística, servicio técnico, gerencia o
     marketing»). Cada chofer trae su licencia y si puede tomar ESTE vehículo en ESTE horario; los que no pueden quedan a la vista con
     el motivo, sin poder elegirse. */
  function iniciales(n) {
    return String(n || '?').split(/\s+/).filter(Boolean).slice(0, 2).map(function (p) { return p.charAt(0).toUpperCase(); }).join('');
  }
  function estadoChofer(c) {
    if (!c.habilitado || !c.usuario_activo) return { cls: 'off', t: 'Inhabilitado como chofer', bloq: true };
    if (!c.licencia_vence || c.licencia_estado === 'sin_fecha') return { cls: 'off', t: 'Sin vencimiento de licencia registrado', bloq: true };
    if (c.licencia_estado === 'vencido') return { cls: 'off', t: 'Licencia vencida el ' + c.licencia_vence_fmt, bloq: true };
    var v = vehPorId(vehSel());
    if (v) {
      var exig = v.licencia_clases || [];
      if (exig.length && !exig.some(function (x) { return (c.licencia_clases || []).indexOf(x) >= 0; })) return { cls: 'mal', t: 'Su licencia no sirve para ' + v.patente_fmt + ' (exige clase ' + exig.join(' o ') + ')' };
    }
    var ven = ventana();
    if (ven && c.licencia_vence < ven.fh) return { cls: 'mal', t: 'Su licencia vence el ' + c.licencia_vence_fmt + ', antes de devolverlo' };
    var ch = choques('c', c.id, ven);
    if (ch.length) return { cls: 'mal', t: 'Ocupado: ' + ch[0].numero + ' · ' + ch[0].vehiculo.patente_fmt + ' · ' + ch[0].rango_texto };
    if (c.licencia_estado === 'por_vencer') return { cls: 'warn', t: 'Disponible · licencia ' + String(c.licencia_texto_dias || '').toLowerCase() };
    return { cls: 'ok', t: ven ? 'Disponible en ese horario' : 'Licencia vigente' };
  }
  function tarjetasChoferes(selId) {
    var cont = $('flSolChoferes');
    var lista = S.chof.filter(function (c) { return !c.archivado || String(c.id) === String(selId); });
    var html = lista.map(function (c) {
      var e = estadoChofer(c), bloq = e.bloq && String(c.id) !== String(selId);
      return '<label class="fl-ccard' + (bloq ? ' off' : '') + '"><input type="radio" name="chofer_id" value="' + c.id + '"' +
        (String(c.id) === String(selId) ? ' checked' : '') + (bloq ? ' disabled' : '') + '>' +
        '<span class="fl-ccard-av" aria-hidden="true">' + esc(iniciales(c.nombre)) + '</span>' +
        '<span class="fl-ccard-c"><b>' + esc(c.nombre) + '</b><span class="fl-ccard-s">Licencia ' + esc(c.licencia_texto || '—') + (c.licencia_vence_fmt ? ' · vence ' + esc(c.licencia_vence_fmt) : '') + '</span>' +
        '<span class="fl-ccard-e fl-ccard-e--' + e.cls + '" data-ch="' + c.id + '">' + esc(e.t) + '</span></span></label>';
    }).join('');
    html += PERM.admin
      ? '<button type="button" class="fl-ccard fl-ccard--nuevo" id="flSolNuevoChofer"><span class="fl-ccard-av" aria-hidden="true"><i class="bi bi-person-plus"></i></span><span class="fl-ccard-c"><b>Agregar chofer</b><span class="fl-ccard-s">Lo sumas al repositorio y vuelves acá</span></span></button>'
      : '<div class="fl-ccard-nota"><i class="bi bi-info-circle"></i>¿Falta alguien? Pídeselo al superadministrador: él agrega los choferes.</div>';
    cont.innerHTML = lista.length || PERM.admin ? html : '<div class="fl-vacio"><b>Aún no hay choferes</b>El superadministrador los agrega en la pestaña Choferes.</div>';
  }
  function disponibilidadChoferes() {
    Array.prototype.forEach.call(document.querySelectorAll('#flSolChoferes .fl-ccard-e'), function (el) {
      var c = chofPorId(el.getAttribute('data-ch'));
      if (!c) return;
      var e = estadoChofer(c);
      el.className = 'fl-ccard-e fl-ccard-e--' + e.cls;
      escribir(el, e.t);
    });
    Array.prototype.forEach.call(document.querySelectorAll('#flSolChoferes label.fl-ccard'), function (l) {
      var i = l.querySelector('input');
      l.classList.toggle('on', !!(i && i.checked));
    });
  }
  function tarjetasAreas(valor) {
    var areas = S.config.areas && S.config.areas.length ? S.config.areas : ['Logística', 'Servicio Técnico', 'Gerencia', 'Marketing'];
    var esLista = !valor || areas.some(function (a) { return a === valor; });
    $('flSolAreasRapidas').innerHTML = areas.map(function (a) {
      var k = areaKey(a), on = valor === a;
      return '<button type="button" class="fl-acard fla-a--' + k + (on ? ' on' : '') + '" role="radio" aria-checked="' + on + '" data-area="' + esc(a) + '">' +
        '<i class="bi ' + (AREAS[k] || AREAS.otra).ic + '"></i><span>' + esc(a) + '</span></button>';
    }).join('') +
      '<button type="button" class="fl-acard fla-a--otra' + (!esLista ? ' on' : '') + '" role="radio" aria-checked="' + !esLista + '" data-area="__otra">' +
      '<i class="bi bi-three-dots"></i><span>Otra área</span></button>';
    $('flSolArea').hidden = esLista;
  }
  function elegirArea(a) {
    var inp = $('flSolArea');
    if (a === '__otra') {
      tarjetasAreas('__otra_' + Date.now());       // marca «Otra área» y muestra el campo
      inp.hidden = false; inp.value = ''; inp.focus();
    } else {
      inp.value = a;
      tarjetasAreas(a);
    }
    S.solTocados.area = true;
    evaluarSolLuego();
  }

  /* ── Calendario del modal (Daniel, 02-nov-2026: «estoy hablando del calendario del modal… tiene que ser potente, dinámico, con
     objetos bien tecnológicos»). Reemplaza el selector gris del navegador por el esqueleto del Paso 4 de /retiros/solicitar: a la
     izquierda el mes, donde cada día trae una barra por vehículo con lo ocupado de su jornada (verde libre, ámbar parcial, rojo lleno);
     a la derecha la jornada del día elegido, en bloques de 30 minutos, una fila por vehículo con su foto y patente. Se toca el bloque
     donde parte y el bloque donde termina; los campos Día / Desde / Hasta siguen ahí (son la verdad del formulario) y se llenan solos.
     Solo muestra: el choque lo decide el servidor con su sello. */
  var PK_DESDE = 7 * 60, PK_HASTA = 20 * 60, PK_PASO = 30;
  function pkReservasDia(iso) {
    var propio = S.editSol ? S.editSol.id : null;
    return solDelDia(iso).filter(function (s) { return s.id !== propio; });
  }
  function pkOcupante(vid, iso, m) {
    var r = null;
    pkReservasDia(iso).forEach(function (s) {
      if (String(s.vehiculo.id) !== String(vid)) return;
      var t = tramo(s, iso);
      if (t[0] < m + PK_PASO && t[1] > m) r = s;
    });
    return r;
  }
  function pkNivel(vid, iso) {
    var t = 0;
    pkReservasDia(iso).forEach(function (s) {
      if (String(s.vehiculo.id) !== String(vid)) return;
      var r = tramo(s, iso), x = Math.max(JORNADA.desde, r[0]), y = Math.min(JORNADA.hasta, r[1]);
      if (y > x) t += y - x;
    });
    var p = t / (JORNADA.hasta - JORNADA.desde);
    return { p: p, cls: p >= 0.95 ? 'lleno' : (p > 0 ? 'parcial' : 'libre') };
  }
  function pkPintarMes() {
    var c = S.meses[S.pk.mes], cont = $('flPkMes');
    if (!cont || !S.pk.mes) return;
    var p = S.pk.mes.split('-');
    $('flPkMesT').textContent = c ? c.mes_texto : (fr_mes(Number(p[1])) + ' ' + p[0]);
    if (!c) { cont.innerHTML = '<div class="fl-pk-cargando"><span class="fl-pk-spin"></span>Leyendo la agenda…</div>'; return; }
    var hoy = ahoraCL().iso, sel = vSol('fecha'), vehs = S.veh.filter(function (v) { return v.habilitado; });
    var html = DIAS.map(function (d) { return '<span class="fl-pk-d">' + d.slice(0, 2) + '</span>'; }).join('');
    for (var i = 0; i < (c.dias[0] ? c.dias[0].semana : 0); i++) html += '<span aria-hidden="true"></span>';
    c.dias.forEach(function (d) {
      var pasado = !S.editSol && d.fecha < hoy;
      var barras = vehs.map(function (v) {
        var n = pkNivel(v.id, d.fecha);
        return '<i class="fl-pk-bar fl-pk-bar--' + n.cls + '" style="--p:' + Math.max(n.p, n.p > 0 ? 0.18 : 0).toFixed(2) + '"></i>';
      }).join('');
      var resumen = vehs.map(function (v) {
        var n = pkNivel(v.id, d.fecha);
        return v.patente_fmt + ' ' + (n.cls === 'libre' ? 'libre' : (n.cls === 'lleno' ? 'ocupado' : 'ocupado en parte'));
      }).join(', ');
      var et = diaTexto(d.fecha) + (d.nombre ? ', feriado ' + d.nombre : (d.no_laboral ? ', fin de semana' : '')) + (pasado ? ', ya pasó' : ', ' + resumen);
      html += '<button type="button" class="fl-pk-dia' + (d.no_laboral ? ' nolab' : '') + (d.fecha === hoy ? ' hoy' : '') + (d.fecha === sel ? ' sel' : '') + (pasado ? ' pasado' : '') +
        '" data-pkdia="' + d.fecha + '"' + (pasado ? ' disabled' : '') + ' aria-pressed="' + (d.fecha === sel) + '" aria-label="' + esc(et) + '" title="' + esc(et) + '">' +
        '<span class="fl-pk-n">' + (pasado ? '<i class="bi bi-lock-fill"></i>' : d.dia) + '</span><span class="fl-pk-barras">' + (pasado ? '' : barras) + '</span></button>';
    });
    cont.innerHTML = html;
  }
  function fr_mes(n) { return ['Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio', 'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre'][n - 1] || ''; }
  function pkPintarBloques() {
    var cont = $('flPkBloques');
    if (!cont) return;
    var iso = vSol('fecha');
    if (!iso) { cont.innerHTML = '<div class="fl-pk-vacio"><i class="bi bi-hand-index"></i>Elige un día en el calendario.</div>'; $('flPkSel').innerHTML = ''; return; }
    if (!S.meses[iso.slice(0, 7)]) { cont.innerHTML = '<div class="fl-pk-cargando"><span class="fl-pk-spin"></span>Leyendo la agenda…</div>'; return; }
    var d = diaInfo(iso), ahora = ahoraCL(), vehs = S.veh.filter(function (v) { return v.habilitado; });
    var vsel = vehSel(), md = minutos(vSol('hora_desde')), mh = minutos(vSol('hora_hasta'));
    var otroDia = $('flSolOtroDia').checked && vSol('fecha_hasta') && vSol('fecha_hasta') !== iso;
    var html = '<div class="fl-pk-dia-cab"><b>' + esc(DIAS_LARGO[(aFecha(iso).getDay() + 6) % 7] + ' ' + fechaCorta(iso)) + '</b>' +
      (d.nombre ? '<span class="fl-pk-fer"><i class="bi bi-flag-fill"></i>Feriado: ' + esc(d.nombre) + '</span>' : (d.no_laboral ? '<span class="fl-pk-fer"><i class="bi bi-moon-stars"></i>Fin de semana: ILUS no opera</span>' : '')) + '</div>';
    html += '<div class="fl-pk-escala" aria-hidden="true"><span></span>';
    for (var m = PK_DESDE; m < PK_HASTA; m += 60) html += '<span>' + pad(m / 60) + '</span>';
    html += '</div>';
    vehs.forEach(function (v) {
      var libres = 0;
      var celdas = '';
      for (var x = PK_DESDE; x < PK_HASTA; x += PK_PASO) {
        var ocu = pkOcupante(v.id, iso, x);
        var pasado = iso < ahora.iso || (iso === ahora.iso && x + PK_PASO <= minutos(ahora.hm));
        var enSel = String(v.id) === String(vsel) && md != null && (otroDia ? x >= md : (mh != null && x >= md && x < mh));
        var cls = ocu ? 'ocu fla-a--' + areaKey(ocu.area) : (pasado ? 'pas' : 'lib');
        if (!ocu && !pasado) libres++;
        if (enSel) cls += ocu ? ' choca' : ' sel';
        if (S.pk.ancla && String(S.pk.ancla.veh) === String(v.id) && S.pk.ancla.m === x) cls += ' ancla';
        var t = hhmm(x) + '–' + hhmm(x + PK_PASO) + ': ' + (ocu ? 'ocupado · ' + ocu.numero + ' · ' + ocu.area + ' · ' + horaEnDia(ocu, iso) + ' (pidió ' + (ocu.solicitante || 'otra persona') + ')' : (pasado ? 'ya pasó' : 'libre'));
        celdas += '<button type="button" class="fl-pk-s ' + cls + '" data-pkveh="' + v.id + '" data-pkm="' + x + '"' + (pasado && !ocu ? ' disabled' : '') +
          ' title="' + esc(t) + '" aria-label="' + esc(v.patente_fmt + ', ' + t) + '"></button>';
      }
      html += '<div class="fl-pk-fila' + (String(v.id) === String(vsel) ? ' on' : '') + '">' +
        '<div class="fl-pk-veh">' + vehFoto(v, 'fl-pk-foto') + '<span><b class="fl-pat">' + esc(v.patente_fmt) + '</b><span>' + esc(v.tipo || '') + '</span>' +
        '<span class="fl-pk-libres">' + (libres ? plural(libres / 2, 'h libre', 'h libres').replace('.5 h', ',5 h') : 'Sin horas libres') + '</span></span></div>' +
        '<div class="fl-pk-tira">' + celdas + '</div></div>';
    });
    html += '<div class="fl-pk-atajos" role="group" aria-label="Horarios rápidos">' +
      '<button type="button" class="fl-pk-at" data-pkat="08:00-13:00"><i class="bi bi-sunrise"></i>Mañana 08–13</button>' +
      '<button type="button" class="fl-pk-at" data-pkat="14:00-18:00"><i class="bi bi-sunset"></i>Tarde 14–18</button>' +
      '<button type="button" class="fl-pk-at" data-pkat="08:00-18:00"><i class="bi bi-brightness-high"></i>Jornada 08–18</button>' +
      '<span class="fl-pk-ayuda"><i class="bi bi-hand-index-thumb"></i>Toca dónde parte y dónde termina</span></div>';
    cont.innerHTML = html;
    pkPintarSel();
  }
  function pkPintarSel() {
    var el = $('flPkSel');
    if (!el) return;
    var v = vehPorId(vehSel()), ven = ventana();
    if (!v || !ven) { el.className = 'fl-pk-sel'; el.innerHTML = '<i class="bi bi-info-circle"></i><span>' + (S.pk.ancla ? 'Ahora toca el bloque donde terminas.' : 'Elige el vehículo y el horario tocando los bloques verdes.') + '</span>'; return; }
    var ch = choques('v', v.id, ven);
    el.className = 'fl-pk-sel ' + (ch.length ? 'bad' : 'ok');
    el.innerHTML = vehFoto(v, 'fl-pk-sel-foto') + '<span><b>' + esc(v.patente_fmt + ' · ' + (v.titulo || v.tipo)) + '</b>' +
      '<span>' + esc(diaTexto(ven.f) + ' ' + vSol('hora_desde') + (ven.fh !== ven.f ? ' → ' + diaTexto(ven.fh) : ' a') + ' ' + vSol('hora_hasta') + ' · ' + duracionTexto(ven.ini, ven.fin)) + '</span></span>' +
      '<span class="fl-pk-sel-est">' + (ch.length ? '<i class="bi bi-x-octagon-fill"></i>Choca con ' + esc(ch[0].numero + ' · ' + ch[0].area) : '<i class="bi bi-check-circle-fill"></i>Libre') + '</span>';
  }
  function pkPintar() { pkPintarMes(); pkPintarBloques(); }
  function pkCargar(mes) {
    S.pk.mes = mes;
    pkPintarMes();
    return Promise.all([cargarMes(mes), cargarMes(sumarDias(mes + '-01', 40).slice(0, 7))]).then(pkPintar);
  }
  function pkPonerHoras(vid, a, b) {
    var f = $('flFormSol');
    var r = document.querySelector('#flSolVehiculos input[value="' + vid + '"]');
    if (r && !r.disabled) r.checked = true;
    f.elements.hora_desde.value = hhmm(a);
    f.elements.hora_hasta.value = hhmm(Math.min(b, 1439));
    ['vehiculo_id', 'hora_desde', 'hora_hasta'].forEach(function (c) { S.solTocados[c] = true; });
    evaluarSol();
  }
  function pkTocar(vid, m) {
    var iso = vSol('fecha'), ocu = pkOcupante(vid, iso, m);
    if (ocu) { toast(vehPorId(vid).patente_fmt + ' está ocupado a esa hora: ' + ocu.numero + ' · ' + ocu.area + ' · ' + horaEnDia(ocu, iso) + '.', 'warning'); return; }
    var a = S.pk.ancla;
    if (a && String(a.veh) === String(vid) && m >= a.m) {
      for (var x = a.m; x <= m; x += PK_PASO) {      // el tramo no puede cruzar otra reserva
        if (pkOcupante(vid, iso, x)) { toast('Entre esas horas hay otra reserva. Elige un tramo sin bloques rojos.', 'warning'); return; }
      }
      S.pk.ancla = null;
      pkPonerHoras(vid, a.m, m + PK_PASO);
      return;
    }
    S.pk.ancla = { veh: vid, m: m };
    var fin = m + PK_PASO;
    while (fin < m + 120 && fin < PK_HASTA && !pkOcupante(vid, iso, fin)) fin += PK_PASO;   // propone 2 h o hasta la próxima reserva
    pkPonerHoras(vid, m, fin);
  }
  function pkAtajo(rango) {
    var p = rango.split('-'), a = minutos(p[0]), b = minutos(p[1]), iso = vSol('fecha');
    if (!iso) { toast('Primero elige el día.', 'warning'); return; }
    var vehs = S.veh.filter(function (v) { return v.habilitado; });
    var pref = vehSel() ? [vehPorId(vehSel())].concat(vehs) : vehs;
    var libre = null;
    pref.forEach(function (v) {
      if (libre || !v) return;
      var ok = true;
      for (var x = a; x < b; x += PK_PASO) if (pkOcupante(v.id, iso, x)) ok = false;
      if (ok) libre = v;
    });
    S.pk.ancla = null;
    if (!libre) { toast('Ningún vehículo está libre ' + p[0] + '–' + p[1] + ' ese día. Prueba otro horario u otro día.', 'warning'); return; }
    pkPonerHoras(libre.id, a, b);
    if (vehSel() && String(libre.id) !== String(vehSel())) toast('El vehículo que tenías está ocupado en ese horario; te dejé ' + libre.patente_fmt + '.', 'info');
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
    tarjetasChoferes(s ? s.chofer.id : '');
    $('flSolAreasList').innerHTML = (S.config.areas || []).map(function (a) { return '<option value="' + esc(a) + '">'; }).join('');
    f.elements.fecha.value = s ? s.fecha : (pref.fecha || S.hoy || '');
    f.elements.hora_desde.value = s ? s.hora_desde : (pref.hora_desde || '');
    f.elements.hora_hasta.value = s ? s.hora_hasta : (pref.hora_hasta || '');
    var otro = !!(s && s.fecha_hasta !== s.fecha);
    $('flSolOtroDia').checked = otro;
    $('flSolOtroDiaCampo').hidden = !otro;
    f.elements.fecha_hasta.value = otro ? s.fecha_hasta : '';
    f.elements.area.value = s ? s.area : '';
    f.elements.motivo.value = s ? s.motivo : '';
    tarjetasAreas(s ? s.area : '');
    S.pk.ancla = null;
    var mesPk = (f.elements.fecha.value || S.hoy || '').slice(0, 7);
    delete S.meses[mesPk];                     // la agenda del modal se lee fresca al abrir
    S.pk.mes = mesPk;
    abrirModal('flModalSol');
    evaluarSol();
    pkCargar(mesPk);
    // Trae lo más reciente (reservas vigentes, vehículos y choferes) y vuelve a mostrar la disponibilidad real.
    Promise.all([refrescarOcup(), cargarVeh(), cargarChof()]).then(function () {
      var v = vehSel(), c = vSol('chofer_id');
      tarjetasVehiculos(v || (s ? s.vehiculo.id : (pref.vehiculo_id || '')));
      tarjetasChoferes(c);
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
      return norm([c.nombre, c.nombre_completo, c.rut, c.username, c.licencia_texto, c.cargo].join(' ')).indexOf(q) >= 0;
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
    var estado;
    if (c.archivado) {
      estado = '<span class="fl-estado fl-estado--off"><i class="bi bi-archive"></i>Archivado</span><span class="fl-motivo">' + esc(c.archivado_motivo || '') +
        (c.archivado_por ? ' · ' + esc(c.archivado_por) : '') + (c.archivado_fmt ? ' · ' + esc(c.archivado_fmt) : '') + '</span>';
    } else if (c.habilitado) {
      estado = '<span class="fl-estado fl-estado--on"><i class="bi bi-check-circle-fill"></i>Habilitado</span>';
    } else {
      estado = '<span class="fl-estado fl-estado--off"><i class="bi bi-slash-circle"></i>Inhabilitado</span><span class="fl-motivo">' + esc(c.inhabilitado_motivo || '') +
        (c.inhabilitado_por ? ' · ' + esc(c.inhabilitado_por) : '') + (c.inhabilitado_fmt ? ' · ' + esc(c.inhabilitado_fmt) : '') + '</span>';
    }
    var lic = '<div class="fl-doc fl-doc--' + (c.licencia_estado === 'sin_fecha' ? 'sin_fecha' : esc(c.licencia_estado)) + '"><b>' + esc(c.licencia_texto) + '</b><span>' +
      (c.licencia_vence_fmt ? esc(c.licencia_vence_fmt) + ' · ' : '') + esc(c.licencia_texto_dias) + '</span></div>';
    if (PERM.documentos) {
      lic += c.licencia_archivo
        ? '<a class="fl-pdf" href="/flota/api/archivos/' + c.licencia_archivo.id + '" target="_blank" rel="noopener"><i class="bi bi-file-earmark-pdf"></i>Ver licencia</a>'
        : '<span class="fl-sin-pdf"><i class="bi bi-file-earmark-x"></i>Sin PDF de la licencia</span>';
    }
    var personales = '';
    if (PERM.documentos) {
      personales = (c.rut_fmt ? '<span class="fl-sub">RUT ' + esc(c.rut_fmt) + '</span>' : '<span class="fl-sub fl-sub--bad">RUT sin registrar</span>') +
        (c.direccion ? '<span class="fl-sub">' + esc(c.direccion) + '</span>' : '<span class="fl-sub fl-t-mute">Dirección sin registrar</span>');
    }
    var nombre = PERM.documentos && c.nombre_completo ? c.nombre_completo : c.nombre;
    var acc = '';
    if (PERM.admin) acc += '<button type="button" class="fl-btn" data-acc="editar"><i class="bi bi-pencil"></i>Editar</button>';
    if (PERM.gestionar) acc += '<button type="button" class="fl-btn" data-acc="historial"><i class="bi bi-clock-history"></i>Historial</button>';
    return '<tr class="' + (c.habilitado && !c.archivado ? '' : 'inhab') + '" data-id="' + c.id + '">' +
      '<td class="fl-td-veh"><span class="fl-nombre">' + esc(nombre) + '</span>' +
      (PERM.documentos && c.nombre_completo && c.nombre_completo !== c.nombre ? '<span class="fl-sub">Usuario ILUS: ' + esc(c.nombre) + '</span>' : '') +
      (c.cargo ? '<span class="fl-sub">' + esc(c.cargo) + '</span>' : '') + (c.telefono ? '<span class="fl-sub">' + esc(c.telefono) + '</span>' : '') + personales +
      (c.usuario_activo ? '' : '<span class="fl-sub fl-sub--bad">Su usuario de ILUS está desactivado</span>') + '</td>' +
      '<td class="fl-td-docs" data-label="Licencia">' + lic + '</td>' +
      '<td data-label="Estado">' + estado + '</td>' +
      '<td data-label="Acciones"><div class="fl-acc">' + acc + '</div></td></tr>';
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
  var NOMBRES_CH = { usuario_id: 'el usuario', nombre_completo: 'el nombre completo', rut: 'el RUT', direccion: 'la dirección validada con Google',
    licencia_clases: 'la clase de licencia', licencia_vence: 'el vencimiento de la licencia' };
  function rutValido(t) {
    var r = String(t || '').replace(/[^0-9kK]/g, '').toUpperCase();
    if (!/^\d{7,8}[0-9K]$/.test(r)) return false;
    var cuerpo = r.slice(0, -1), dv = r.slice(-1), suma = 0, f = 2;
    for (var i = cuerpo.length - 1; i >= 0; i--) { suma += parseInt(cuerpo.charAt(i), 10) * f; f = f === 7 ? 2 : f + 1; }
    var e = 11 - (suma % 11);
    return dv === (e === 11 ? '0' : (e === 10 ? 'K' : String(e)));
  }
  /* RUT chileno mientras se escribe (12.345.678-5): la misma función de Mi cuenta (static/ilus_persona_fields.js). */
  function formatoRut(v) { return window.ilusPersona && window.ilusPersona.rutFormatear ? window.ilusPersona.rutFormatear(v) : v; }
  function rutEnVivo(inp) {
    if (!window.ilusPersona) return;
    var P = window.ilusPersona, pos = inp.selectionStart == null ? inp.value.length : inp.selectionStart;
    var aLaDerecha = P.rutLimpiar(inp.value.slice(pos)).length;
    inp.value = P.rutFormatear(inp.value);
    var p = inp.value.length, cuenta = 0;
    while (p > 0 && cuenta < aLaDerecha) { p--; if (/[0-9K]/.test(inp.value.charAt(p))) cuenta++; }
    try { inp.setSelectionRange(p, p); } catch (e) { /* algunos campos no aceptan selección */ }
  }
  /* Dirección: solo vale la elegida de la lista de Google (coordenadas + place_id). Si se vuelve a escribir, se pierde la validación. */
  var dirValidada = null;
  function geoCh(lat, lng, pid) { $('flChDirLat').value = lat || ''; $('flChDirLng').value = lng || ''; $('flChDirPid').value = pid || ''; }
  var googleListo = false;
  function iniciarGoogleCh() {
    if (googleListo || typeof window.ilusPlacesAutocomplete !== 'function') return;
    googleListo = true;
    window.ilusPlacesAutocomplete('flChDireccion', {
      country: 'cl', types: ['address'],
      onPlaceSelected: function (pl) {
        geoCh(pl.lat, pl.lng, pl.place_id);
        dirValidada = { valor: $('flChDireccion').value, lat: pl.lat, lng: pl.lng, pid: pl.place_id };
        S.chTocados.direccion = true;
        evaluarChLuego();
      }
    });
  }
  function reglasCh() {
    var r = {};
    r.usuario_id = S.editCh ? res('ok', '') : (vCh('usuario_id') ? res('ok', '') : res('bad', 'Elige a qué usuario de ILUS corresponde este chofer.'));
    r.nombre_completo = vCh('nombre_completo').trim().length >= 5 ? res('ok', '') : res('bad', 'Escribe el nombre completo, como figura en la licencia.');
    var rut = vCh('rut').trim();
    r.rut = !rut ? res('bad', 'Indica el RUT del chofer (por ejemplo 12.345.678-5).') : (rutValido(rut) ? res('ok', 'RUT válido: ' + formatoRut(rut) + '.') : res('bad', 'El RUT no es válido (revisa el dígito verificador).'));
    var dir = vCh('direccion').trim();
    if (!dir) r.direccion = res('bad', 'Indica la dirección del chofer: escríbela y elige una sugerencia de Google.');
    else if (vCh('direccion_place_id')) r.direccion = res('ok', 'Dirección verificada con Google Maps.');
    else if (S.editCh && dir === (S.editCh.direccion || '').trim()) r.direccion = res('warn', 'Guardada sin validar: vuelve a elegirla de la lista de Google.');
    else r.direccion = res('bad', 'Elige la dirección de la lista de sugerencias de Google para validarla.');
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
    paso(1, ['usuario_id', 'nombre_completo', 'rut', 'direccion']); paso(2, ['licencia_clases', 'licencia_vence']);
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
  function subirArchivoCh(id, archivo) {
    var fd = new FormData();
    fd.append('entidad', 'chofer'); fd.append('entidad_id', id); fd.append('tipo', 'licencia'); fd.append('archivo', archivo);
    return fetch('/flota/api/archivos', { method: 'POST', body: fd, credentials: 'same-origin', headers: { Accept: 'application/json' } }).then(function (r) {
      return r.json().catch(function () { return { ok: false, error: 'El servidor respondió algo inesperado.' }; })
        .then(function (d) { d.status = r.status; return d; });
    }).catch(function () { return { ok: false, status: 0, error: 'No hay conexión con el servidor. Revisa tu internet e intenta de nuevo.' }; });
  }
  function pintarArchivoActualCh(c) {
    var a = c && c.licencia_archivo;
    $('flChArchivoActual').innerHTML = a
      ? '<a class="fl-pdf" href="/flota/api/archivos/' + a.id + '" target="_blank" rel="noopener"><i class="bi bi-file-earmark-pdf"></i>Ver la licencia guardada</a>' +
        '<span class="fl-sub">' + esc(a.nombre || '') + ' · subida por ' + esc(a.subido_por || '—') + ' · ' + esc(a.subido_fmt || '') + '</span>' +
        '<button type="button" class="fl-btn fl-btn--chico fl-btn--peligro" data-gcc="quitar-lic" data-aid="' + a.id + '"><i class="bi bi-x-lg"></i>Quitar</button>'
      : (c ? '<span class="fl-sin-pdf"><i class="bi bi-file-earmark-x"></i>Todavía no hay PDF de la licencia</span>' : '');
  }
  function pintarGestionCh(c) {
    var b = [];
    if (c && PERM.admin) {
      if (!c.archivado) b.push(c.habilitado
        ? '<button type="button" class="btn btn-outline-danger" data-gcc="inhabilitar"><i class="bi bi-slash-circle me-1"></i>Inhabilitar</button>'
        : '<button type="button" class="btn btn-outline-success" data-gcc="habilitar"><i class="bi bi-check-circle me-1"></i>Habilitar</button>');
      b.push(c.archivado
        ? '<button type="button" class="btn btn-outline-secondary" data-gcc="desarchivar"><i class="bi bi-box-arrow-up me-1"></i>Desarchivar</button>'
        : '<button type="button" class="btn btn-outline-secondary" data-gcc="archivar"><i class="bi bi-archive me-1"></i>Archivar</button>');
      b.push('<button type="button" class="btn btn-outline-danger" data-gcc="eliminar"><i class="bi bi-trash me-1"></i>Borrar definitivo</button>');
    }
    $('flChGestion').innerHTML = b.join('');
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
    f.elements.nombre_completo.value = c ? (c.nombre_completo || c.nombre || '') : '';
    f.elements.rut.value = c ? (c.rut_fmt || '') : '';
    f.elements.direccion.value = c ? (c.direccion || '') : '';
    geoCh(c && c.direccion_lat, c && c.direccion_lng, c && c.direccion_place_id);
    dirValidada = c && c.direccion_place_id ? { valor: c.direccion || '', lat: c.direccion_lat, lng: c.direccion_lng, pid: c.direccion_place_id } : null;
    iniciarGoogleCh();
    f.elements.licencia_vence.value = c && c.licencia_vence ? c.licencia_vence : '';
    f.elements.notas.value = c && c.notas ? c.notas : '';
    $('flChArchivo').value = '';
    $('flChArchivoMsg').textContent = '';
    pintarArchivoActualCh(c);
    pintarGestionCh(c);
    abrirModal('flModalChofer');
    evaluarCh();
    if (!c) {
      api(API + '/choferes/usuarios').then(function (d) {
        if (!d.ok) { $('flChUsuario').innerHTML = '<option value="">No se pudo leer la lista de usuarios</option>'; toast(d.error || 'No se pudo leer la lista de usuarios.', 'error'); return; }
        S.usuariosCh = d.usuarios || [];
        $('flChUsuario').innerHTML = '<option value="">Elige un usuario de ILUS…</option>' + S.usuariosCh.map(function (u) {
          return '<option value="' + u.id + '">' + esc(u.nombre + (u.cargo ? ' · ' + u.cargo : '') + (u.username ? ' (' + u.username + ')' : '')) + '</option>';
        }).join('');
        if (!S.usuariosCh.length) $('flChUsuario').innerHTML = '<option value="">Todos los usuarios activos ya son choferes</option>';
        evaluarCh();
      });
    }
  }
  /* Al elegir al usuario se SUGIERE lo que ya tiene su perfil; la persona corrige lo que haga falta (REGLA #32). */
  function sugerirDesdePerfil() {
    var u = (S.usuariosCh || []).filter(function (x) { return String(x.id) === vCh('usuario_id'); })[0];
    if (!u) return;
    var f = $('flFormCh');
    if (!vCh('nombre_completo').trim()) f.elements.nombre_completo.value = u.nombre || '';
    if (!vCh('rut').trim() && u.rut) f.elements.rut.value = u.rut;
    if (!vCh('direccion').trim() && u.direccion) {
      f.elements.direccion.value = u.direccion;
      // Si en su perfil ya está validada con Google, se sugiere con su ubicación; si no, hay que elegirla de la lista.
      geoCh(u.direccion_lat, u.direccion_lng, u.direccion_place_id);
      dirValidada = u.direccion_place_id ? { valor: u.direccion, lat: u.direccion_lat, lng: u.direccion_lng, pid: u.direccion_place_id } : null;
    }
    if (vCh('rut')) f.elements.rut.value = formatoRut(vCh('rut'));
    evaluarChLuego();
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
      var archivo = $('flChArchivo').files && $('flChArchivo').files[0];
      var body = { rut: vCh('rut'), nombre_completo: vCh('nombre_completo'), direccion: vCh('direccion'),
        direccion_lat: vCh('direccion_lat'), direccion_lng: vCh('direccion_lng'), direccion_place_id: vCh('direccion_place_id'),
        licencia_clases: clasesCh(), licencia_vence: vCh('licencia_vence'), notas: vCh('notas') };
      if (!editando) body.usuario_id = vCh('usuario_id');
      S.guardandoCh = true;
      $('flChGuardar').disabled = true;
      api(editando ? API + '/choferes/' + editando.id : API + '/choferes', { method: editando ? 'PUT' : 'POST', body: body }).then(function (r) {
        if (!r.ok) {
          S.guardandoCh = false; $('flChGuardar').disabled = false;
          Object.keys(r.errores || {}).forEach(function (c) {
            var el = campoCh(c);
            if (!el) return;
            el.classList.remove('ok', 'warn'); el.classList.add('bad');
            escribir(el.querySelector('.fl-msg'), r.errores[c]);
          });
          toast(r.error || 'No se pudo guardar el chofer.', 'error');
          return;
        }
        var id = (r.chofer && r.chofer.id) || (editando && editando.id);
        var terminar = function () {
          S.guardandoCh = false; $('flChGuardar').disabled = false;
          cerrarModal('flModalChofer');
          toast(r.sin_cambios && !archivo ? 'No había cambios que guardar.' : (editando ? 'Chofer actualizado.' : 'Chofer agregado y habilitado.'), 'success');
          cargarChof();
        };
        if (archivo && id) {
          subirArchivoCh(id, archivo).then(function (u) {
            if (!u.ok) toast('El chofer se guardó, pero la licencia no: ' + (u.error || 'no se pudo subir el archivo.'), 'warning');
            terminar();
          });
        } else terminar();
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
        cerrarModal('flModalChofer');
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
        cerrarModal('flModalChofer');
        cargarChof();
      });
    });
  }
  function archivarCh(c) {
    window.ilusPrompt({
      title: 'Archivar a ' + c.nombre,
      message: 'Sale de las listas pero conserva sus datos, su licencia y el historial de sus reservas. Puedes desarchivarlo cuando quieras.',
      placeholder: 'Motivo (dejó la empresa, cambio de funciones…)', okLabel: 'Archivar', type: 'warning', multiline: true, required: true, minLength: 5
    }).then(function (motivo) {
      if (motivo == null) return;
      api(API + '/choferes/' + c.id + '/archivar', { method: 'POST', body: { motivo: motivo } }).then(function (d) {
        if (!d.ok) { toast(d.error || 'No se pudo archivar.', 'error'); return; }
        toast(c.nombre + ' quedó archivado.', 'success');
        cerrarModal('flModalChofer');
        cargarChof();
      });
    });
  }
  function desarchivarCh(c) {
    api(API + '/choferes/' + c.id + '/desarchivar', { method: 'POST', body: {} }).then(function (d) {
      if (!d.ok) { toast(d.error || 'No se pudo desarchivar.', 'error'); return; }
      toast(c.nombre + ' salió del archivo.', 'success');
      (d.avisos || []).forEach(function (a) { toast(a, 'warning'); });
      cerrarModal('flModalChofer');
      cargarChof();
    });
  }
  function eliminarCh(c) {
    var esperado = c.nombre_completo || c.nombre;
    window.ilusPrompt({
      title: 'Borrar definitivo a ' + c.nombre,
      message: 'Se borra para siempre y no se puede deshacer. Solo se puede si nunca tuvo reservas; si no, archívalo. Para confirmar escribe su nombre completo.',
      placeholder: esperado, okLabel: 'Borrar definitivo', type: 'danger', required: true
    }).then(function (txt) {
      if (txt == null) return;
      api(API + '/choferes/' + c.id, { method: 'DELETE', body: { confirmacion: txt } }).then(function (d) {
        if (!d.ok) { toast(d.error || 'No se pudo borrar.', 'error'); return; }
        toast(c.nombre + ' se borró definitivamente.', 'success');
        cerrarModal('flModalChofer');
        cargarChof();
      });
    });
  }
  function quitarLicencia(aid) {
    window.ilusConfirm({
      title: 'Quitar la licencia guardada', message: '¿Dejar de usar este archivo? El original queda guardado en el historial; después puedes subir otro.',
      okLabel: 'Quitar', cancelLabel: 'Volver', danger: true
    }).then(function (ok) {
      if (!ok) return;
      api('/flota/api/archivos/' + aid + '/quitar', { method: 'POST', body: {} }).then(function (d) {
        if (!d.ok) { toast(d.error || 'No se pudo quitar el archivo.', 'error'); return; }
        toast('Archivo quitado.', 'success');
        if (S.editCh && d.chofer) { S.editCh = d.chofer; pintarArchivoActualCh(d.chofer); }
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

    /* Agenda de la flota */
    $('flCalPrev').addEventListener('click', function () { mesMas(-1); });
    $('flCalSig').addEventListener('click', function () { mesMas(1); });
    $('flCalHoy').addEventListener('click', irHoy);
    $('flaSemPrev').addEventListener('click', function () { semanaMas(-1); });
    $('flaSemSig').addEventListener('click', function () { semanaMas(1); });
    $('flCalGrid').addEventListener('click', function (ev) {
      var b = ev.target.closest('[data-fecha]');
      if (!b) return;
      S.semana = lunesDe(b.getAttribute('data-fecha'));
      cargarCal();
    });
    $('flaVehChips').addEventListener('click', function (ev) {
      var b = ev.target.closest('[data-cveh]');
      if (!b) return;
      S.solVeh = b.getAttribute('data-cveh');
      $('flSolVeh').value = S.solVeh;
      S.solPag = 1; pintarSolLista(); pintarCal();
    });
    $('flaAreaChips').addEventListener('click', function (ev) {
      var b = ev.target.closest('[data-carea]');
      if (b) { S.calArea = b.getAttribute('data-carea'); pintarCal(); }
    });
    $('flaSemana').addEventListener('click', function (ev) {
      var b = ev.target.closest('.fla-b[data-id]');
      if (b) { abrirDetalle(b.getAttribute('data-id')); return; }
      var dh = ev.target.closest('.fla-dh[data-dia]');
      if (dh) { abrirDia(dh.getAttribute('data-dia')); return; }
      var p = ev.target.closest('.fla-pista');
      if (p) pedirEnPista(p, ev.clientY);
    });
    $('flaSemana').addEventListener('mousemove', function (ev) {      // muestra dónde partiría la reserva antes de tocar
      var p = ev.target.closest('.fla-pista');
      if (!p || ev.target.closest('.fla-b')) return;
      var g = p.querySelector('.fla-fantasma'), ini = parseInt(p.getAttribute('data-ini'), 10) || 0, m = minutoEnPista(p, ev.clientY);
      g.style.top = ((m - ini) / 60 * PX_HORA) + 'px';
      escribir(g, '+ ' + hhmm(m));
    });
    $('flCalAgenda').addEventListener('click', function (ev) {
      var p = ev.target.closest('[data-pedir]');
      if (p) { abrirSolicitud(null, { fecha: p.getAttribute('data-pedir'), vehiculo_id: S.solVeh || '' }); return; }
      var b = ev.target.closest('[data-id]');
      if (b) abrirDetalle(b.getAttribute('data-id'));
    });
    $('flaDemanda').addEventListener('click', function (ev) {
      var b = ev.target.closest('[data-hveh]');
      if (!b) return;
      var a = parseInt(b.getAttribute('data-ha'), 10), z = parseInt(b.getAttribute('data-hb'), 10);
      abrirSolicitud(null, { fecha: b.getAttribute('data-hdia'), vehiculo_id: b.getAttribute('data-hveh'), hora_desde: hhmm(a), hora_hasta: hhmm(Math.min(z, a + 120)) });
    });
    $('flaNext').addEventListener('click', function () {
      var id = this.getAttribute('data-id');
      if (id) abrirDetalle(id);
    });
    var ticks = 0;
    setInterval(function () {                 // en vivo: la cuenta regresiva cada 30 s y la agenda cada 2 min
      if (document.hidden || S.tab !== 'solicitudes' || S.vista !== 'calendario') return;
      ticks++;
      if (ticks % 4 === 0) cargarCal(true); else pintarProximo();
    }, 30000);
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
      if (e.target.name === 'fecha' && e.target.value && e.target.value.slice(0, 7) !== S.pk.mes) pkCargar(e.target.value.slice(0, 7));
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
      if (b) elegirArea(b.getAttribute('data-area'));
    });
    $('flSolChoferes').addEventListener('click', function (ev) {
      if (!ev.target.closest('#flSolNuevoChofer')) return;
      despuesDeCerrar('flModalSol', function () { irTab('choferes'); abrirChofer(null); });
    });
    /* Calendario del modal */
    $('flPkPrev').addEventListener('click', function () { pkCargar(sumarDias(S.pk.mes + '-01', -1).slice(0, 7)); });
    $('flPkSig').addEventListener('click', function () { pkCargar(sumarDias(S.pk.mes + '-01', 32).slice(0, 7)); });
    $('flPkMes').addEventListener('click', function (ev) {
      var b = ev.target.closest('[data-pkdia]');
      if (!b || b.disabled) return;
      fs.elements.fecha.value = b.getAttribute('data-pkdia');
      S.solTocados.fecha = true;
      S.pk.ancla = null;
      evaluarSol();
    });
    $('flPkBloques').addEventListener('click', function (ev) {
      var at = ev.target.closest('[data-pkat]');
      if (at) { pkAtajo(at.getAttribute('data-pkat')); return; }
      var b = ev.target.closest('.fl-pk-s[data-pkveh]');
      if (b && !b.disabled) pkTocar(b.getAttribute('data-pkveh'), parseInt(b.getAttribute('data-pkm'), 10));
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
    });
    var tc;
    $('flChBuscar').addEventListener('input', function (e) {
      clearTimeout(tc);
      tc = setTimeout(function () { S.chQ = e.target.value; S.chPag = 1; pintarChoferes(); }, 150);
    });
    chips('.fl-fchip[data-chest]', 'data-chest', function (v) {
      var ver = v === 'archivados';
      S.chEstado = ver ? 'todos' : v; S.chPag = 1;
      if (ver !== S.verArchCh) { S.verArchCh = ver; cargarChof(); } else pintarChoferes();
    });
    $('flChPorPagina').addEventListener('change', function (e) { S.chPor = parseInt(e.target.value, 10) || 25; S.chPag = 1; pintarChoferes(); });
    $('flChAnterior').addEventListener('click', function () { if (S.chPag > 1) { S.chPag--; pintarChoferes(); } });
    $('flChSiguiente').addEventListener('click', function () { S.chPag++; pintarChoferes(); });
    var fc = $('flFormCh');
    fc.addEventListener('input', function (e) {
      if (e.target.name) S.chTocados[e.target.name] = true;
      if (e.target.name === 'rut') rutEnVivo(e.target);
      if (e.target.name === 'direccion') {
        if (dirValidada && e.target.value === dirValidada.valor) geoCh(dirValidada.lat, dirValidada.lng, dirValidada.pid);
        else geoCh('', '', '');
      }
      evaluarChLuego();
    });
    $('flChDireccion').addEventListener('keydown', function (e) { if (e.key === 'Enter') e.preventDefault(); });   // Enter elige la sugerencia, no envía
    fc.addEventListener('change', function (e) {
      if (e.target.type === 'checkbox') S.chTocados.licencia_clases = true;
      if (e.target.name) S.chTocados[e.target.name] = true;
      evaluarChLuego();
    });
    fc.addEventListener('submit', function (e) { e.preventDefault(); guardarCh(); });
    $('flChGuardar').addEventListener('click', guardarCh);
    $('flChUsuario').addEventListener('change', sugerirDesdePerfil);
    $('flChGestion').addEventListener('click', function (ev) {
      var b = ev.target.closest('[data-gcc]');
      var c = S.editCh;
      if (!b || !c) return;
      var a = b.getAttribute('data-gcc');
      if (a === 'inhabilitar') inhabilitarCh(c);
      else if (a === 'habilitar') habilitarCh(c);
      else if (a === 'archivar') archivarCh(c);
      else if (a === 'desarchivar') desarchivarCh(c);
      else if (a === 'eliminar') eliminarCh(c);
    });
    $('flChArchivoActual').addEventListener('click', function (ev) {
      var b = ev.target.closest('[data-gcc="quitar-lic"]');
      if (b) quitarLicencia(b.getAttribute('data-aid'));
    });
    $('flChArchivo').addEventListener('change', function (ev) {
      var f = ev.target.files && ev.target.files[0], msg = $('flChArchivoMsg');
      if (!f) { msg.textContent = ''; return; }
      var ok = f.type === 'application/pdf' || /\.pdf$/i.test(f.name) || /^image\/(jpeg|png|webp)$/.test(f.type);
      if (!ok) { ev.target.value = ''; msg.textContent = 'Sube un PDF (o la foto del carnet en JPG, PNG o WebP).'; return; }
      if (f.size > 15 * 1024 * 1024) { ev.target.value = ''; msg.textContent = 'El archivo pesa demasiado (máximo 15 MB).'; return; }
      msg.textContent = 'Se guardará al presionar «Guardar chofer»: ' + f.name;
    });

    var h = (location.hash || '').replace('#', '');
    Promise.all([cargarVeh(), cargarChof(), cargarSol()]).then(function () { irTab(h || 'solicitudes'); });
    irTab(h || 'solicitudes');
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init); else init();
})();

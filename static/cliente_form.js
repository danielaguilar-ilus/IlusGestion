/* 🧾 Editar cliente — semáforo, pasos y validaciones en vivo · 2026-10-24
 * Solo lee y marca los campos del modal #modalEditarCliente (ids ec_*): NO cambia lo que se guarda (guardarCliente() de mant_ficha.js sigue
 * igual). Cada campo pasa de ROJO (falta o está mal) a VERDE (válido); cada tarjeta de paso: rojo → verde con check; opcional vacío → ámbar.
 *
 * DOS REGLAS DE DANIEL (24-oct):
 *  · «La ubicación se debe verificar para poder avanzar»: la dirección solo cuenta como verificada si se eligió una sugerencia de Google Places
 *    (place_id) y el texto verificado no se tocó (se puede AGREGAR oficina, depto o referencias al final). Sin eso el botón Guardar queda
 *    apagado y el pie dice por qué. Dos salidas para no dejar un callejón sin salida: si Google no está disponible no hay cómo verificar (se
 *    avisa al guardar), y si la dirección NO se tocó y nunca estuvo verificada (cliente anterior a esta regla, o sin dirección) se puede guardar
 *    con un aviso ámbar: se exige verificar lo que se cambia, no el pasado. El place_id y las coordenadas ocultos siempre calzan con el texto.
 *  · «Región, comuna y ciudad deben ser grandes… se va a entrecortar cualquier dato»: los textos largos van en cajas que CRECEN con lo escrito
 *    (<textarea rows="1" class="ecf-auto">): nunca se corta un dato, en ningún ancho. Enter no agrega líneas.
 */
(function () {
  'use strict';
  var $ = function (id) { return document.getElementById(id); };
  function val(id) { var e = $(id); return e ? String(e.value || '').trim() : ''; }
  function esc(s) { return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) { return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]; }); }

  // RUT chileno: módulo 11 (mismo criterio que el servidor).
  function rutOk(s) {
    var c = String(s || '').replace(/[^0-9kK]/g, '').toUpperCase();
    if (c.length < 2) return false;
    var cuerpo = c.slice(0, -1), dv = c.slice(-1), suma = 0, m = 2;
    for (var i = cuerpo.length - 1; i >= 0; i--) { suma += parseInt(cuerpo.charAt(i), 10) * m; m = (m === 7) ? 2 : m + 1; }
    var r = 11 - (suma % 11), esp = (r === 11) ? '0' : (r === 10 ? 'K' : String(r));
    return dv === esp;
  }
  function emailOk(s) { return /^[^\s@]+@[^\s@]+\.[^\s@]{2,}$/.test(String(s || '')); }
  function telOk(s) {
    var d = String(s || '').replace(/\D/g, '');
    if (d.indexOf('56') === 0 && d.length >= 11) d = d.slice(2);
    return d.length === 9 || d.length === 8;
  }

  // ── Dirección: qué está verificado y qué se puede guardar ──────────────────────────────────────────────
  // Se recuerdan TODAS las direcciones verificadas que se vieron (la que vino del servidor y cada sugerencia de Google elegida): si la persona
  // prueba otra y vuelve a la anterior, sigue verificada. Cerrar y reabrir el modal NO borra ni inventa verificaciones (el estado vive acá, no
  // se recalcula del texto que haya en el campo). El place_id y las coordenadas (campos ocultos) siempre calzan con el texto: si el texto se
  // aparta de lo verificado se vacían —nunca se guarda un texto nuevo con las coordenadas de otro lugar— y vuelven si la persona regresa.
  // Región, comuna y ciudad (que Google llena al elegir una sugerencia) siguen la misma regla: cada verificación recuerda las suyas y se reponen al
  // volver a ella, SOLO si nadie las tocó a mano; lo que una sugerencia no trae no se queda con el valor de OTRA dirección.
  var inicial = { dir: '', pid: '', lat: '', lng: '', geo: { region: '', comuna: '', ciudad: '' } };   // lo que vino del servidor al cargar la página
  var verifs = [];            // [{texto, pid, lat, lng, geo}] direcciones verificadas conocidas
  var activa = inicial;       // la verificación (o el estado guardado) cuya región/comuna/ciudad están hoy en los campos
  var abiertoEn = 0;          // cuándo se abrió el modal (para distinguir «conectando con Google» de «Google no responde»)
  var GEO = ['region', 'comuna', 'ciudad'];

  // Si Google rechaza la clave (restricción de dominio, facturación…) llama a window.gm_authFailure: el campo queda «enganchado» pero nunca ofrece
  // sugerencias. Eso es «Google no disponible» (se avisa al guardar), no un callejón sin salida.
  var _gmFallo = false;
  var _gmPrevio = window.gm_authFailure;
  window.gm_authFailure = function () {
    _gmFallo = true;
    try { if ($('modalEditarCliente')) evaluar(); } catch (e) {}
    if (typeof _gmPrevio === 'function') { try { _gmPrevio.apply(this, arguments); } catch (e) {} }
  };

  // ¿Google Places ya está enganchado al campo de dirección? (lo marca ilusPlacesAutocomplete). Sin eso no hay cómo verificar.
  function googleListo() { var d = $('ec_direccion'); return !_gmFallo && !!(d && d.dataset && d.dataset.placesInit === '1'); }
  function googleConectando() { return !_gmFallo && !googleListo() && !!abiertoEn && (Date.now() - abiertoEn) < 8000; }

  function geoActual() { return { region: val('ec_region'), comuna: val('ec_comuna'), ciudad: val('ec_ciudad') }; }
  function setVal(id, v) { var e = $(id); if (e) e.value = v || ''; }
  // Lo que Google devolvió es «…, Chile»; el detalle (oficina, depto, referencias) se agrega DESPUÉS. Al cargar un cliente con el detalle ya guardado,
  // la base verificada es lo que llega hasta «, Chile»: así el detalle se puede editar o quitar sin volver a verificar.
  function baseGoogle(dir) {
    var m = /,\s*Chile\b/i.exec(dir);
    return m ? dir.slice(0, m.index + m[0].length) : dir;
  }
  function capturarInicial() {
    var dir = val('ec_direccion'), pid = val('ec_direccion_place_id'), geo = geoActual();
    inicial = { dir: dir, pid: pid, lat: val('ec_direccion_lat'), lng: val('ec_direccion_lng'), geo: geo };
    verifs = pid && dir ? [{ texto: baseGoogle(dir), pid: pid, lat: inicial.lat, lng: inicial.lng, geo: geo }] : [];
    activa = verifs.length ? verifs[0] : inicial;
  }
  // El texto empieza con lo verificado y lo que sigue se AGREGÓ al final, separado («…4501, oficina 5»). «…4501A» ya no es la misma dirección.
  function empiezaCon(dir, base) {
    if (!base || dir.indexOf(base) !== 0) return false;
    var resto = dir.slice(base.length);
    return resto === '' || /^[\s,;\-–—]/.test(resto);
  }
  // La verificación que respalda el texto actual (la más larga si hay varias), o null.
  function coincidencia() {
    var dir = val('ec_direccion'), mejor = null;
    if (!dir) return null;
    verifs.forEach(function (v) { if (v.pid && empiezaCon(dir, v.texto) && (!mejor || v.texto.length > mejor.texto.length)) mejor = v; });
    return mejor;
  }
  function direccionVerificada() { return !!coincidencia(); }
  function fijar(id, v) { var e = $(id); if (e && String(e.value || '') !== String(v || '')) e.value = v || ''; }
  function sincronizarPlace() {
    var c = coincidencia(), destino = c || (val('ec_direccion') === inicial.dir ? inicial : null);
    var o = destino || { pid: '', lat: '', lng: '' };
    fijar('ec_direccion_place_id', o.pid); fijar('ec_direccion_lat', o.lat); fijar('ec_direccion_lng', o.lng);
    // Región, comuna y ciudad siguen a la dirección: al volver a una dirección ya vista se reponen las que siguen valiendo lo que puso la otra
    // sugerencia (lo que la persona escribió a mano nunca se pisa).
    if (destino && destino !== activa) {
      GEO.forEach(function (k) { if (val('ec_' + k) === (activa.geo[k] || '')) setVal('ec_' + k, destino.geo[k]); });
      activa = destino;
    }
  }
  // Estado de la ubicación para guardar:
  //  verificada → listo · heredada → no se tocó y nunca estuvo verificada: se guarda con aviso · google → no hay cómo verificar ahora (se avisa al guardar)
  //  vacia · cambiada · sin_verificar → traban (Daniel: «la ubicación se debe verificar para poder avanzar»)
  function estadoUbicacion() {
    var dir = val('ec_direccion');
    if (direccionVerificada()) return 'verificada';
    if (dir === inicial.dir && !inicial.pid) return 'heredada';
    if (!googleListo()) return 'google';
    if (!dir) return 'vacia';
    return verifs.length ? 'cambiada' : 'sin_verificar';
  }

  // Las regiones aria-live (verificación, pie, progreso) solo se reescriben si cambiaron: un lector de pantalla no debe releerlas en cada tecla.
  var _ult = {};
  function escribir(el, clase, html) {
    if (!el) return;
    var k = clase + '\u0001' + html;
    if (_ult[el.id] === k) return;
    _ult[el.id] = k;
    el.className = clase; el.innerHTML = html;
  }

  function marca(id, estado, msg) {
    var e = $(id); if (!e) return;
    e.classList.remove('ecf-ok', 'ecf-bad', 'ecf-warn');
    if (estado) e.classList.add('ecf-' + estado);
    var m = document.querySelector('[data-ecf-msg="' + id + '"]');
    if (m) { m.textContent = msg || ''; m.className = 'ecf-msg' + (estado ? ' ' + estado : ''); }
  }
  // Campo opcional con formato: vacío = sin marca; válido = verde; inválido = rojo.
  function opcional(id, okFn, msgOk, msgMal) {
    var v = val(id);
    if (!v) { marca(id, '', ''); return null; }
    if (okFn(v)) { marca(id, 'ok', msgOk || ''); return true; }
    marca(id, 'bad', msgMal || 'Revisa este dato.'); return false;
  }
  function paso(n, estado) {   // estado: 'complete' | 'pending' | 'neutral'
    var s = $('ecfSec' + n); if (!s) return;
    s.classList.toggle('is-complete', estado === 'complete');
    s.classList.toggle('is-neutral', estado === 'neutral');
  }

  // ── Cajas que CRECEN con lo escrito: ningún dato se corta ───────────────────────────────────────
  var AUTO = ['ec_razon', 'ec_giro', 'ec_region', 'ec_comuna', 'ec_ciudad', 'ec_email_empresa', 'ec_contacto_nombre', 'ec_contacto_cargo', 'ec_contacto_email',
              'ec_contacto2_nombre', 'ec_contacto2_cargo', 'ec_contacto2_email'];
  function crecer(e) {
    if (!e || !e.offsetParent) return;            // oculto (modal cerrado): scrollHeight sería 0
    e.style.height = 'auto';
    e.style.height = Math.max(e.scrollHeight + 2, 46) + 'px';
  }
  function crecerTodo() { AUTO.forEach(function (id) { crecer($(id)); }); }

  function ir(id) {
    var e = $(id); if (!e) return;
    try { e.scrollIntoView({ behavior: 'smooth', block: 'center' }); } catch (x) {}
    setTimeout(function () { try { e.focus(); } catch (x) {} }, 250);
  }

  // Pie del modal: dice POR QUÉ Guardar está apagado (si lo está) y lleva a ese lugar al tocarlo.
  function pie(razonOk, est) {
    var p = $('ecfFootEstado'), b = $('btnGuardarCliente'); if (!p) return;
    var falta = [];
    if (!razonOk) falta.push('la razón social');
    if (est === 'vacia' || est === 'cambiada' || est === 'sin_verificar') falta.push('la dirección verificada con Google');
    p.hidden = false;
    if (falta.length) {
      escribir(p, 'ecf-foot-estado bad', '<i class="bi bi-lock-fill"></i><span>Para guardar falta ' + falta.join(' y ') + '</span>');
      p.dataset.ir = !razonOk ? 'ec_razon' : 'ec_direccion';
      if (b) b.disabled = true;
    } else if (est === 'heredada') {                      // dirección que no se tocó y nunca se verificó: se puede guardar, se recomienda verificarla
      escribir(p, 'ecf-foot-estado warn', '<i class="bi bi-exclamation-triangle-fill"></i><span>' + (val('ec_direccion')
        ? 'La dirección nunca se verificó con Google · puedes guardar igual' : 'Este cliente no tiene dirección · puedes guardar igual') + '</span>');
      p.dataset.ir = 'ec_direccion';
      if (b) b.disabled = false;
    } else if (est === 'google') {                        // Google no está (todavía o no responde): no se traba, se avisa al guardar
      escribir(p, 'ecf-foot-estado warn', '<i class="bi bi-exclamation-triangle-fill"></i><span>' + (googleConectando()
        ? 'Conectando con Google para verificar la dirección…' : 'Sin Google no se pudo verificar la dirección') + '</span>');
      p.dataset.ir = 'ec_direccion';
      if (b) b.disabled = false;
    } else {
      escribir(p, 'ecf-foot-estado ok', '<i class="bi bi-check-circle-fill"></i><span>Listo para guardar</span>');
      p.dataset.ir = '';
      if (b) b.disabled = false;
    }
  }

  function evaluar() {
    if (!$('modalEditarCliente')) return;
    // ── Paso 1 · Empresa ───────────────────────────────────────────
    var razonOk = val('ec_razon').length >= 2;     // «3M», «AB»: nombres cortos reales; solo el vacío (y 1 letra) traba
    marca('ec_razon', razonOk ? 'ok' : 'bad', razonOk ? '' : (val('ec_razon') ? 'Escribe el nombre completo de la empresa.' : 'La razón social es obligatoria.'));
    var rut = val('ec_rut'), rutValido = !!rut && rutOk(rut);
    if (!rut) marca('ec_rut', 'warn', 'Recomendado: con el RUT se cruza con el ERP y los documentos.');
    else marca('ec_rut', rutValido ? 'ok' : 'bad', rutValido ? 'RUT válido.' : 'El dígito verificador no calza.');
    var dia = val('ec_dia_pref');
    if (!dia) marca('ec_dia_pref', '', '');
    else { var dn = parseInt(dia, 10); marca('ec_dia_pref', (dn >= 1 && dn <= 28) ? 'ok' : 'bad', (dn >= 1 && dn <= 28) ? '' : 'Debe ser un día entre 1 y 28.'); }
    opcional('ec_email_empresa', emailOk, 'Correo válido.', 'Le falta la @ o el dominio.');
    opcional('ec_tel_empresa', telOk, 'Teléfono válido.', 'Teléfono incompleto (ej: +56 2 2345 6789).');
    var s1 = razonOk && rutValido;
    paso(1, s1 ? 'complete' : 'pending');

    // ── Paso 2 · Ubicación (Google Places): VERIFICADA para poder guardar ─────────
    sincronizarPlace();                                   // place_id y coordenadas siempre calzan con el texto que hay ahora
    var dir = val('ec_direccion');
    var est = estadoUbicacion(), verif = (est === 'verificada'), vig = coincidencia();
    var caja = $('ecfVerif');
    if (caja) {
      var tip = 'Si Google no encuentra el número exacto, elige la calle más cercana y escribe el número, oficina o referencia <b>al final</b>.';
      // Salida directa de un retoque accidental: volver a la dirección que estaba guardada (con sus coordenadas, región, comuna y ciudad).
      var volver = (inicial.dir && dir !== inicial.dir)
        ? '<button type="button" class="ecf-verif-btn" onclick="ecfRestaurarDireccion()"><i class="bi bi-arrow-counterclockwise"></i>Volver a la dirección guardada</button>' : '';
      if (est === 'verificada') {
        var extra = dir.slice(vig.texto.length).replace(/^[\s,;\-–—]+/, '');
        escribir(caja, 'ecf-verif ok', '<i class="bi bi-check-circle-fill"></i><div class="ecf-verif-t"><b>Dirección verificada con Google.</b>'
          + '<span class="ecf-verif-dir">' + esc(vig.texto) + '</span>'
          + (extra ? '<span class="ecf-verif-sub">Detalle agregado: <b>' + esc(extra) + '</b></span>'
                   : '<span class="ecf-verif-sub">Si hace falta, agrega oficina, depto o referencias <b>al final</b>: la dirección sigue verificada.</span>') + '</div>');
      } else if (est === 'heredada') {                    // no se tocó y nunca estuvo verificada: alertar, no trabar
        var hayGoogle = googleListo();
        escribir(caja, 'ecf-verif warn', '<i class="bi bi-exclamation-triangle-fill"></i><div class="ecf-verif-t">'
          + (dir ? '<b>Esta dirección nunca se verificó con Google.</b> Puedes guardar sin tocarla'
                 : '<b>Este cliente no tiene dirección.</b> Puedes guardar sin ella')
          + (hayGoogle
              ? (dir ? ', pero conviene verificarla: elige una sugerencia del menú para que el técnico y el transporte lleguen al punto exacto.'
                     : ', pero conviene agregarla: escríbela y elige una sugerencia de Google.')
              : (googleConectando() ? '. Google está cargando: en unos segundos podrás verificarla.' : '. Ahora no hay conexión con Google, así que no se puede verificar.'))
          + (hayGoogle ? '<span class="ecf-verif-sub">' + tip + '</span>' : '') + '</div>');
      } else if (est === 'google') {
        escribir(caja, 'ecf-verif warn', '<i class="bi bi-exclamation-triangle-fill"></i><div class="ecf-verif-t">' + (googleConectando()
          ? '<b>Conectando con Google…</b> En unos segundos podrás elegir una sugerencia para verificar la dirección.'
          : '<b>No se pudo conectar con Google.</b> Sin Google no hay cómo verificar la dirección ahora: podrás guardar igual, avisándote antes.') + '</div>');
      } else if (est === 'vacia') {
        escribir(caja, 'ecf-verif bad', '<i class="bi bi-geo-alt-fill"></i><div class="ecf-verif-t"><b>Falta la dirección.</b> Escríbela y elige una sugerencia de Google: sin dirección verificada no se puede guardar.'
          + '<span class="ecf-verif-sub">' + tip + '</span>' + volver + '</div>');
      } else if (est === 'cambiada') {
        escribir(caja, 'ecf-verif bad', '<i class="bi bi-exclamation-triangle-fill"></i><div class="ecf-verif-t"><b>Cambiaste la dirección verificada.</b> Elige otra sugerencia del menú de Google para poder guardar.'
          + '<span class="ecf-verif-sub">Para agregar oficina, depto o referencias, escríbelos <b>al final</b> sin tocar el resto.</span>' + volver + '</div>');
      } else {
        escribir(caja, 'ecf-verif bad', '<i class="bi bi-exclamation-triangle-fill"></i><div class="ecf-verif-t"><b>Falta verificar la dirección.</b> Elige una sugerencia del menú de Google para poder guardar.'
          + '<span class="ecf-verif-sub">' + tip + '</span>' + volver + '</div>');
      }
    }
    marca('ec_direccion', verif ? 'ok' : ((est === 'heredada' || est === 'google') ? 'warn' : 'bad'), '');
    var reg = val('ec_region'), com = val('ec_comuna'), ciu = val('ec_ciudad');
    marca('ec_region', reg ? 'ok' : '', ''); marca('ec_comuna', com ? 'ok' : '', ''); marca('ec_ciudad', ciu ? 'ok' : '', '');
    var s2 = verif && !!reg && !!com;
    paso(2, s2 ? 'complete' : 'pending');

    // ── Paso 3 · Contacto principal ────────────────────────────────
    var nom = val('ec_contacto_nombre').length >= 3;
    marca('ec_contacto_nombre', nom ? 'ok' : 'bad', nom ? '' : 'Indica con quién hablar.');
    marca('ec_contacto_cargo', val('ec_contacto_cargo') ? 'ok' : '', '');
    var tel = opcional('ec_contacto_tel', telOk, 'Teléfono válido.', 'Teléfono incompleto (ej: +56 9 1234 5678).');
    var mail = opcional('ec_contacto_email', emailOk, 'Correo válido.', 'Le falta la @ o el dominio.');
    var viaOk = (tel === true) || (mail === true);
    var s3 = nom && viaOk && tel !== false && mail !== false;
    if (nom && !viaOk) {
      var m = document.querySelector('[data-ecf-msg="ec_contacto_tel"]');
      if (m && !m.textContent) { m.textContent = 'Falta un teléfono o un correo para contactarlo.'; m.className = 'ecf-msg warn'; }
    }
    paso(3, s3 ? 'complete' : 'pending');

    // ── Paso 4 · Ejecutivo ─────────────────────────────────────────
    var ej = val('ec_ejecutivo_user_id');
    marca('ec_ejecutivo_user_id', ej ? 'ok' : 'warn', ej ? '' : 'Sin responsable asignado: nadie se encarga de comunicarse con este cliente.');
    paso(4, ej ? 'complete' : 'pending');

    // ── Paso 5 · Contacto secundario (opcional) ────────────────────
    var n2 = val('ec_contacto2_nombre'), t2 = val('ec_contacto2_tel'), e2 = val('ec_contacto2_email'), c2 = val('ec_contacto2_cargo');
    var alguno = !!(n2 || t2 || e2 || c2);
    var t2ok = opcional('ec_contacto2_tel', telOk, 'Teléfono válido.', 'Teléfono incompleto.');
    var e2ok = opcional('ec_contacto2_email', emailOk, 'Correo válido.', 'Le falta la @ o el dominio.');
    marca('ec_contacto2_nombre', n2 ? (n2.length >= 3 ? 'ok' : 'bad') : '', '');
    marca('ec_contacto2_cargo', c2 ? 'ok' : '', '');
    var s5 = n2.length >= 3 && (t2ok === true || e2ok === true) && t2ok !== false && e2ok !== false;
    paso(5, !alguno ? 'neutral' : (s5 ? 'complete' : 'pending'));

    // ── Paso 6 · Notas (opcional) ──────────────────────────────────
    paso(6, val('ec_notas') ? 'complete' : 'neutral');

    // Progreso de la cabecera: los 4 pasos que importan.
    var listos = [s1, s2, s3, !!ej].filter(Boolean).length;
    var t = $('ecfProgTxt'), b = $('ecfProgBar'), p = document.querySelector('.ecf-prog');
    var txtProg = listos + ' de 4 pasos listos';
    if (t && t.textContent !== txtProg) t.textContent = txtProg;      // región aria-live: solo se reescribe si cambió
    if (b) b.style.width = (listos * 25) + '%';
    if (p) p.classList.toggle('ok', listos === 4);

    pie(razonOk, est);
    crecerTodo();
  }

  var _tm = 0;   // setTimeout y no requestAnimationFrame: este último no corre con la pestaña en segundo plano
  function evaluarPronto() { if (_tm) return; _tm = setTimeout(function () { _tm = 0; try { evaluar(); } catch (e) { if (window.console) console.warn('[ecf]', e); } }, 30); }

  var _vig = 0;
  function vigilarGoogle() {
    if (_vig) clearInterval(_vig);
    var n = 0;
    _vig = setInterval(function () {
      n++;
      try { evaluar(); } catch (e) {}
      if (googleListo() || n > 24) { clearInterval(_vig); _vig = 0; }
    }, 500);
  }

  function iniciar() {
    var m = $('modalEditarCliente'); if (!m || m.dataset.ecf === '1') return;
    m.dataset.ecf = '1';
    m.addEventListener('input', function (ev) {
      // Las cajas que crecen son de UNA línea: Enter no agrega saltos y, si se pega texto con saltos, se vuelven espacios.
      var t = ev && ev.target;
      if (t && t.classList && t.classList.contains('ecf-auto') && /[\r\n]/.test(t.value)) t.value = t.value.replace(/[\r\n]+/g, ' ');
      evaluarPronto();
    });
    m.addEventListener('keydown', function (ev) {
      var t = ev && ev.target;
      if (ev && ev.key === 'Enter' && t && t.classList && t.classList.contains('ecf-auto')) ev.preventDefault();
    });
    // Esc: el modal lleva data-bs-keyboard="false" porque los avisos de guardarCliente() (ilusConfirm / ilusAlert) se montan DENTRO de él y también
    // cierran con Esc: con el Esc de Bootstrap se cerraba además el formulario entero. Acá se cierra el formulario solo cuando NO hay un aviso encima
    // ni la lista de sugerencias de Google abierta (ese Esc es de ellos); así el teclado sigue funcionando.
    m.addEventListener('keydown', function (ev) {
      if (!ev || ev.key !== 'Escape' || ev.defaultPrevented) return;
      if (m.querySelector && m.querySelector('.ilus-overlay')) return;
      var pac = document.querySelector && document.querySelector('.pac-container');
      if (pac && pac.offsetParent !== null && pac.children && pac.children.length) return;
      try { var inst = window.bootstrap && window.bootstrap.Modal && window.bootstrap.Modal.getInstance(m); if (inst) inst.hide(); } catch (e) {}
    });
    m.addEventListener('change', evaluarPronto);
    m.addEventListener('blur', evaluarPronto, true);
    capturarInicial();      // lo que vino del servidor, antes de que alguien edite
    m.addEventListener('show.bs.modal', function () {
      abiertoEn = Date.now();       // NO se recalcula qué está verificado desde el campo: cerrar y reabrir no puede «verificar» un texto cambiado
      evaluar();
    });
    // Google carga después de abrir: se vuelve a mirar cada medio segundo (hasta 12 s) para pasar de «conectando» a verificable (o a «sin Google»).
    m.addEventListener('shown.bs.modal', function () { evaluar(); vigilarGoogle(); });
    var pe = $('ecfFootEstado');
    if (pe) pe.addEventListener('click', function () { if (pe.dataset.ir) ir(pe.dataset.ir); });
    if (window.addEventListener) window.addEventListener('resize', crecerTodo);
  }

  // Lo llama mant_ficha.js cuando Google Places entrega una sugerencia: esa dirección queda verificada.
  // `geoGoogle` = {region, comuna, ciudad} que Google trajo para esta sugerencia ('' si no trajo alguno).
  window.ecfDireccionVerificada = function (texto, geoGoogle) {
    // mant_ficha.js ya dejó el place_id, las coordenadas y la región/comuna/ciudad de ESTA sugerencia en los campos: se recuerdan junto con su texto.
    var t = String(texto || val('ec_direccion')).trim(), pid = val('ec_direccion_place_id');
    if (t && pid) {
      // Lo que Google NO trajo (p. ej. una sugerencia sin comuna) no puede quedarse con el valor de la dirección anterior, salvo que la persona lo haya escrito.
      if (geoGoogle && activa) GEO.forEach(function (k) { if (!geoGoogle[k] && val('ec_' + k) === (activa.geo[k] || '')) setVal('ec_' + k, ''); });
      var nueva = { texto: t, pid: pid, lat: val('ec_direccion_lat'), lng: val('ec_direccion_lng'), geo: geoActual() };
      verifs = verifs.filter(function (v) { return v.texto !== t; });
      verifs.push(nueva);
      activa = nueva;
    }
    evaluar();
  };
  // «Volver a la dirección guardada»: deshace un retoque de la dirección (texto, place_id, coordenadas, región, comuna y ciudad) y deja Guardar listo.
  window.ecfRestaurarDireccion = function () {
    var d = $('ec_direccion'); if (!d) return;
    d.value = inicial.dir;
    var destino = coincidencia() || inicial;
    GEO.forEach(function (k) { setVal('ec_' + k, destino.geo[k]); });
    activa = destino;
    evaluar();
    try { d.focus(); } catch (e) {}
  };
  window.ecfEvaluar = evaluar;
  window.ecfIr = ir;

  // ¿Se puede guardar con esta dirección? { ok:true } · { ok:true, aviso:'heredada' } (no se tocó y nunca estuvo verificada: se guarda con aviso)
  // · { ok:false, motivo:'vacia'|'sin_verificar'|'cambiada'|'google_conectando'|'google_no_disponible' }.
  // Sin Google no hay cómo verificar: ese caso NO traba (guardarCliente avisa y deja decidir); los demás sí (Daniel: «para poder avanzar»).
  // `google_conectando`: Google todavía está cargando (primeros segundos): guardarCliente pide esperar en vez de decir que no hay conexión.
  window.ecfDireccionPendiente = function () {
    evaluar();
    var est = estadoUbicacion();
    if (est === 'verificada') return { ok: true };
    if (est === 'heredada') return { ok: true, aviso: 'heredada' };
    if (est === 'google') return { ok: false, motivo: googleConectando() ? 'google_conectando' : 'google_no_disponible' };
    return { ok: false, motivo: est };
  };

  // Datos ESCRITOS pero mal (rojo con valor): guardarCliente() los muestra antes de guardar y la persona decide («alertar, no trabar»).
  // Un campo vacío no entra: faltar un dato es de la tarjeta de paso (rojo/ámbar), no un error de escritura.
  var ETIQUETAS = {
    ec_razon: 'Razón social', ec_rut: 'RUT', ec_dia_pref: 'Día de mantención',
    ec_email_empresa: 'Email de la empresa', ec_tel_empresa: 'Teléfono de la empresa',
    ec_contacto_tel: 'Contacto principal · teléfono', ec_contacto_email: 'Contacto principal · email',
    ec_contacto2_nombre: 'Contacto secundario · nombre', ec_contacto2_tel: 'Contacto secundario · teléfono',
    ec_contacto2_email: 'Contacto secundario · email'
  };
  window.ecfProblemas = function () {
    evaluar();
    var out = [];
    Object.keys(ETIQUETAS).forEach(function (id) {
      var e = $(id);
      if (!e || !val(id) || !e.classList.contains('ecf-bad')) return;
      var m = document.querySelector('[data-ecf-msg="' + id + '"]');
      out.push({ id: id, etiqueta: ETIQUETAS[id], msg: (m && m.textContent) || 'Revisa este dato.' });
    });
    return out;
  };

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', iniciar); else iniciar();
})();

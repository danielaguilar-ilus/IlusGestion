/* ILUS · Entrar con Face ID / huella (passkeys) — Daniel, 05-nov-2026: «que desbloquee la aplicación con la huella o los métodos de seguridad que
   tenga el teléfono… que le pregunte si quiere y dejarlo habilitado por el front… en Mi cuenta, donde salen mis datos, necesito controlarlo».

   ES UN MODO ADICIONAL: la contraseña sigue igual y sigue siendo el respaldo. Se activa dispositivo por dispositivo y se apaga igual.
   Este archivo hace tres cosas, todas opcionales y sin romper nada si el teléfono no lo soporta:
     1) Mi cuenta (#pkCard): el interruptor «Usar Face ID / huella en este dispositivo» y la lista de dispositivos con «Quitar».
     2) Login (#pkLoginBtn): un enlace chico «Entrar con Face ID / huella»; si este teléfono ya lo activó, Face ID se pide SOLO al abrir.
     3) Una pregunta amable en el celular, una vez, después de entrar con la contraseña («¿Quieres entrar con Face ID?»).
   La verificación de verdad la hace el servidor (passkeys_reglas.py); acá solo se habla con el teléfono (WebAuthn) y se pasan los datos. */
(function () {
  'use strict';
  var API = '/passkeys';
  var K_CRED = 'ilus_pk_cred', K_OK = 'ilus_pk_ok', K_NO = 'ilus_pk_no', K_TARDE = 'ilus_pk_tarde', K_NOAUTO = 'ilus_pk_noauto';
  var meta = document.querySelector('meta[name="ilus-passkeys"]');
  var ACTIVO = !!(meta && meta.getAttribute('content') === '1');

  /* ── almacenamiento que no rompe en modo privado ── */
  function leer(k) { try { return window.localStorage.getItem(k); } catch (e) { return null; } }
  function escribir(k, v) { try { window.localStorage.setItem(k, v); } catch (e) { /* nada */ } }
  function borrar(k) { try { window.localStorage.removeItem(k); } catch (e) { /* nada */ } }

  /* ── base64url ↔ bytes ── */
  function aB64u(buf) {
    var b = new Uint8Array(buf), s = '';
    for (var i = 0; i < b.length; i++) s += String.fromCharCode(b[i]);
    return btoa(s).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
  }
  function deB64u(t) {
    var s = String(t || '').replace(/-/g, '+').replace(/_/g, '/');
    while (s.length % 4) s += '=';
    var bin = atob(s), b = new Uint8Array(bin.length);
    for (var i = 0; i < bin.length; i++) b[i] = bin.charCodeAt(i);
    return b.buffer;
  }

  /* ── ¿Este teléfono puede? Contexto seguro + autenticador del propio dispositivo (Face ID, huella, PIN) ── */
  function soportado() {
    try {
      if (!ACTIVO || !window.isSecureContext || !window.PublicKeyCredential || !navigator.credentials) return Promise.resolve(false);
      if (typeof window.PublicKeyCredential.isUserVerifyingPlatformAuthenticatorAvailable !== 'function') return Promise.resolve(false);
      return window.PublicKeyCredential.isUserVerifyingPlatformAuthenticatorAvailable().then(function (v) { return !!v; }, function () { return false; });
    } catch (e) { return Promise.resolve(false); }
  }

  /* «iPhone · Safari», «Android · Chrome», «iPhone · app instalada»: así reconoces tus dispositivos en la lista. */
  function etiquetaDispositivo() {
    var ua = (navigator.userAgent || '');
    var so = /iPhone/.test(ua) ? 'iPhone' : (/iPad/.test(ua) ? 'iPad' : (/Android/.test(ua) ? 'Android' : (/Windows/.test(ua) ? 'Windows' : (/Mac OS X|Macintosh/.test(ua) ? 'Mac' : 'Dispositivo'))));
    var instalada = false;
    try { instalada = navigator.standalone === true || (window.matchMedia && window.matchMedia('(display-mode: standalone)').matches); } catch (e) { /* nada */ }
    var nav = /Edg\//.test(ua) ? 'Edge' : (/CriOS|Chrome\//.test(ua) ? 'Chrome' : (/FxiOS|Firefox\//.test(ua) ? 'Firefox' : (/Safari\//.test(ua) ? 'Safari' : 'navegador')));
    return so + ' · ' + (instalada ? 'app instalada' : nav);
  }
  function esCelular() {
    try {
      return (window.matchMedia && (window.matchMedia('(max-width: 860px)').matches || window.matchMedia('(display-mode: standalone)').matches)) ||
        navigator.standalone === true || /Android|iPhone|iPad/i.test(navigator.userAgent || '');
    } catch (e) { return false; }
  }

  /* ── hablar con el servidor ── */
  function post(url, body) {
    return fetch(url, {
      method: 'POST', credentials: 'same-origin',
      headers: { 'Content-Type': 'application/json', 'Accept': 'application/json', 'X-Requested-With': 'XMLHttpRequest' },
      body: JSON.stringify(body || {})
    }).then(function (r) {
      return r.json().catch(function () { return { ok: false, error: 'El servidor respondió algo inesperado.' }; }).then(function (d) { d.status = r.status; return d; });
    }).catch(function () { return { ok: false, status: 0, error: 'No hay conexión con el servidor. Revisa tu internet e intenta de nuevo.' }; });
  }
  function leerJson(url) {
    return fetch(url, { credentials: 'same-origin', headers: { 'Accept': 'application/json', 'X-Requested-With': 'XMLHttpRequest' } }).then(function (r) {
      return r.json().catch(function () { return { ok: false, error: 'El servidor respondió algo inesperado.' }; }).then(function (d) { d.status = r.status; return d; });
    }).catch(function () { return { ok: false, status: 0, error: 'No hay conexión con el servidor. Revisa tu internet e intenta de nuevo.' }; });
  }

  /* El teléfono dice «NotAllowedError» cuando cancelas o se acaba el tiempo: no es un fallo, se explica con calma. */
  function mensajeDeError(e) {
    var n = e && e.name;
    if (n === 'NotAllowedError' || n === 'AbortError') return 'Se canceló. Cuando quieras, vuelve a intentarlo.';
    if (n === 'InvalidStateError') return 'Este dispositivo ya estaba activado.';
    if (n === 'NotSupportedError') return 'Este teléfono no permite activar Face ID o huella aquí.';
    if (n === 'SecurityError') return 'Por seguridad, Face ID solo se activa desde la dirección oficial de ILUS.';
    return (e && e.message) || 'No se pudo completar. Intenta de nuevo.';
  }

  /* Safari (iPhone) exige que el teléfono se llame justo después del toque; si entre el toque y la llamada hay una espera de red, puede negarse. Por eso
     el desafío se pide ANTES (cuando aparece el botón, el interruptor o la pregunta) y se guarda hasta 90 segundos; el desafío del servidor vale 2 minutos. */
  var VIDA_DESAFIO_MS = 90000, cacheReg = null, cacheLogin = null;
  function opcionesReg() {
    if (!cacheReg || Date.now() - cacheReg.t > VIDA_DESAFIO_MS) cacheReg = { t: Date.now(), p: post(API + '/registro/opciones') };
    return cacheReg.p;
  }
  function opcionesLogin() {
    if (!cacheLogin || Date.now() - cacheLogin.t > VIDA_DESAFIO_MS) cacheLogin = { t: Date.now(), p: post(API + '/login/opciones') };
    return cacheLogin.p;
  }

  /* ── REGISTRAR este dispositivo ── */
  function registrar() {
    return opcionesReg().then(function (o) {
      cacheReg = null;                                         // cada desafío se usa una sola vez
      if (!o.ok) return o;
      var p = o.publicKey;
      p.challenge = deB64u(p.challenge);
      p.user.id = deB64u(p.user.id);
      (p.excludeCredentials || []).forEach(function (c) { c.id = deB64u(c.id); });
      return navigator.credentials.create({ publicKey: p }).then(function (cred) {
        var r = cred && cred.response;
        if (!r || typeof r.getPublicKey !== 'function' || typeof r.getAuthenticatorData !== 'function') {
          return { ok: false, error: 'Este navegador no permite activar Face ID. Actualiza tu teléfono e intenta de nuevo.' };
        }
        return post(API + '/registro/verificar', {
          id: cred.id, clientDataJSON: aB64u(r.clientDataJSON), authenticatorData: aB64u(r.getAuthenticatorData()),
          publicKey: aB64u(r.getPublicKey()), publicKeyAlgorithm: r.getPublicKeyAlgorithm(),
          transports: typeof r.getTransports === 'function' ? r.getTransports() : [], etiqueta: etiquetaDispositivo()
        }).then(function (v) {
          if (v.ok) { escribir(K_CRED, v.credencial || cred.id); escribir(K_OK, '1'); }
          return v;
        });
      }, function (e) { return { ok: false, cancelado: e && (e.name === 'NotAllowedError' || e.name === 'AbortError'), error: mensajeDeError(e) }; });
    });
  }

  /* ── ENTRAR con Face ID / huella (desde el login) ── */
  function entrar(siguiente) {
    return opcionesLogin().then(function (o) {
      cacheLogin = null;
      if (!o.ok) return o;
      var p = o.publicKey;
      p.challenge = deB64u(p.challenge);
      var guardada = leer(K_CRED);
      p.allowCredentials = guardada ? [{ type: 'public-key', id: deB64u(guardada) }] : [];
      return navigator.credentials.get({ publicKey: p }).then(function (a) {
        var r = a.response;
        return post(API + '/login/verificar', {
          id: a.id, clientDataJSON: aB64u(r.clientDataJSON), authenticatorData: aB64u(r.authenticatorData), signature: aB64u(r.signature),
          userHandle: r.userHandle ? aB64u(r.userHandle) : null, next: siguiente || '/'
        });
      }, function (e) { return { ok: false, cancelado: e && (e.name === 'NotAllowedError' || e.name === 'AbortError'), error: mensajeDeError(e) }; });
    });
  }

  function mis() { return leerJson(API + '/mis'); }
  function quitar(id) { return post(API + '/' + encodeURIComponent(id) + '/quitar'); }

  function avisar(texto, tipo) {
    if (typeof window.ilusToast === 'function') window.ilusToast(texto, { type: tipo || 'info' });
  }
  function esc(t) { return String(t == null ? '' : t).replace(/[&<>"']/g, function (c) { return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]; }); }

  /* ═══ 1) Mi cuenta: el interruptor y la lista de dispositivos ═══ */
  function iniciarMiCuenta() {
    var card = document.getElementById('pkCard');
    if (!card) return;
    var sw = document.getElementById('pkSwitch'), txt = document.getElementById('pkEstado'), lista = document.getElementById('pkLista'),
      aviso = document.getElementById('pkAviso'), ocupado = false;
    function pintar(d, apto) {
      var disp = (d && d.dispositivos) || [];
      var mia = leer(K_CRED);
      var esteActivo = !!mia && disp.some(function (x) { return x.credencial === mia; });
      if (mia && !esteActivo) { borrar(K_CRED); borrar(K_OK); }          // ya lo quitaron desde otro lado
      sw.disabled = !apto || ocupado;
      sw.checked = esteActivo;
      txt.textContent = !apto ? 'Este navegador no permite Face ID o huella.' : (esteActivo ? 'Activado en este dispositivo' : 'Desactivado en este dispositivo');
      card.setAttribute('data-estado', !apto ? 'no' : (esteActivo ? 'si' : 'no-activado'));
      lista.innerHTML = disp.length ? disp.map(function (x) {
        var yo = mia && x.credencial === mia;
        return '<li class="pk-disp"><i class="bi bi-phone" aria-hidden="true"></i><span class="pk-disp-t"><b>' + esc(x.etiqueta) + (yo ? ' <span class="pk-este">este dispositivo</span>' : '') +
          '</b><span>Activado el ' + esc(x.creada_fmt) + ' · último uso: ' + esc(x.ultimo_uso_fmt) + '</span></span>' +
          '<button type="button" class="pk-quitar" data-pk-quitar="' + x.id + '"><i class="bi bi-x-lg" aria-hidden="true"></i>Quitar</button></li>';
      }).join('') : '<li class="pk-vacio">Todavía no has activado Face ID o huella en ningún dispositivo.</li>';
      aviso.hidden = apto;
    }
    function recargar() {
      return Promise.all([soportado(), mis()]).then(function (r) { pintar(r[1], r[0]); return r; });
    }
    sw.addEventListener('change', function () {
      if (ocupado) return;
      ocupado = true; sw.disabled = true;
      var pidio = sw.checked;
      var tarea;
      if (pidio) {
        txt.textContent = 'Activando… sigue las indicaciones de tu teléfono';
        tarea = registrar().then(function (r) { if (r.ok) avisar('✓ Face ID / huella activado en este dispositivo', 'success'); else if (!r.cancelado) avisar(r.error || 'No se pudo activar.', 'error'); else avisar(r.error, 'info'); });
      } else {
        var mia = leer(K_CRED);
        tarea = mis().then(function (d) {
          var fila = ((d && d.dispositivos) || []).filter(function (x) { return x.credencial === mia; })[0];
          if (!fila) { borrar(K_CRED); borrar(K_OK); return null; }
          return quitar(fila.id).then(function (r) {
            if (r.ok) { borrar(K_CRED); borrar(K_OK); avisar('Face ID / huella desactivado en este dispositivo', 'info'); } else avisar(r.error || 'No se pudo desactivar.', 'error');
          });
        });
      }
      tarea.then(function () { ocupado = false; return recargar(); }).then(function () { opcionesReg(); });
    });
    lista.addEventListener('click', function (ev) {
      var b = ev.target.closest('[data-pk-quitar]');
      if (!b || ocupado) return;
      var id = b.getAttribute('data-pk-quitar');
      var confirmar = typeof window.ilusConfirm === 'function'
        ? window.ilusConfirm({ title: 'Quitar este dispositivo', message: 'Ese dispositivo ya no podrá entrar con Face ID o huella. Tu contraseña sigue funcionando.', okLabel: 'Quitar', cancelLabel: 'Volver', danger: true })
        : Promise.resolve(true);
      confirmar.then(function (si) {
        if (!si) return null;
        ocupado = true;
        return quitar(id).then(function (r) {
          if (r.ok) { var mia = leer(K_CRED); if (mia && !((r.dispositivos || []).some(function (x) { return x.credencial === mia; }))) { borrar(K_CRED); borrar(K_OK); } avisar('Dispositivo quitado', 'info'); }
          else avisar(r.error || 'No se pudo quitar.', 'error');
        }).then(function () { ocupado = false; return recargar(); });
      });
    });
    recargar().then(function (r) { if (r && r[0]) opcionesReg(); });
  }

  /* ═══ 2) Login (Daniel, 05-nov-2026: «más directo… el botón más sutil») ═══
     · El acceso es un ENLACE chico («Entrar con Face ID / huella»), no un botón grande.
     · Si este teléfono ya lo activó, Face ID se pide SOLO al abrir la pantalla, sin tocar nada. Si el teléfono exige un toque (Safari del iPhone lo exige),
       no pasa nada malo: queda el enlace chico, sin mensajes de error.
     · No se pide solo si la persona acaba de cerrar sesión a propósito (podría querer entrar con otra cuenta), si ya escribió su correo (por ejemplo se
       equivocó de clave) o si la pantalla está en segundo plano. Se intenta UNA vez por visita: si lo cierra, no insiste. */
  function iniciarLogin() {
    var btn = document.getElementById('pkLoginBtn');
    if (!btn) return;
    var msg = document.getElementById('pkLoginMsg'), usuario = document.querySelector('input[name="username"]'), intentando = false;
    function intentar(esAuto) {
      if (intentando) return;
      intentando = true; btn.disabled = true;
      msg.textContent = esAuto ? 'Verificando con Face ID…' : 'Mira tu teléfono…'; msg.className = 'pk-msg';
      entrar(btn.getAttribute('data-next') || '/').then(function (r) {
        if (r && r.ok) { msg.textContent = 'Listo. Entrando…'; window.location.href = r.redirect || '/'; return; }
        intentando = false; btn.disabled = false;
        opcionesLogin();                                              // el desafío de este intento ya se usó: se pide otro para el siguiente
        if (r && r.error_codigo === 'DESCONOCIDA') { borrar(K_OK); borrar(K_CRED); }       // lo quitaron en otro lado: ya no se pide solo
        if (esAuto && r && r.cancelado) { msg.textContent = ''; return; }                   // lo cerró (o el teléfono pide un toque): sin reclamos
        msg.textContent = (r && r.error) || 'No se pudo entrar. Usa tu contraseña.';
        msg.className = 'pk-msg ' + (r && r.cancelado ? '' : 'pk-msg--mal');
      });
    }
    function pedirseSolo() {
      if (!leer(K_OK)) return false;                                  // solo donde la persona ya lo activó
      try { if (window.sessionStorage.getItem(K_NOAUTO)) { window.sessionStorage.removeItem(K_NOAUTO); return false; } } catch (e) { /* nada */ }
      return !document.hidden && !(usuario && usuario.value);
    }
    var solo = pedirseSolo();
    soportado().then(function (ok) {
      if (!ok) return;
      btn.hidden = false;
      opcionesLogin();
      window.setInterval(function () { if (!btn.disabled) { cacheLogin = null; opcionesLogin(); } }, VIDA_DESAFIO_MS);
      if (solo) window.setTimeout(function () { if (!document.hidden && !(usuario && usuario.value)) intentar(true); }, 350);
    });
    btn.addEventListener('click', function () { intentar(false); });
  }

  /* Cerrar sesión a propósito: en la pantalla de login que sigue, Face ID NO se pide solo (la persona puede querer entrar con otra cuenta). */
  function marcarCierreDeSesion() {
    document.addEventListener('submit', function (ev) {
      var f = ev && ev.target, accion = f && f.getAttribute && f.getAttribute('action');
      if (accion && /\/logout(\?|$)/.test(accion)) { try { window.sessionStorage.setItem(K_NOAUTO, '1'); } catch (e) { /* nada */ } }
    }, true);
  }

  /* ═══ 3) La pregunta, una vez, en el celular (Daniel: «que le pregunte si quiere») ═══ */
  function ofrecer() {
    var uid = String(window.__ILUS_UID || '0');
    if (!ACTIVO || uid === '0' || document.getElementById('pkCard') || document.getElementById('pkLoginBtn')) return;
    if (leer(K_OK) || Number(leer(K_NO) || 0) >= 3 || Date.now() < Number(leer(K_TARDE) || 0)) return;
    if (!esCelular() || typeof window.ilusConfirm !== 'function') return;
    soportado().then(function (ok) {
      if (!ok) return;
      window.setTimeout(function () {
        if (document.querySelector('.modal.show') || document.hidden) return;
        opcionesReg();                                                // se pide mientras la persona lee la pregunta
        window.ilusConfirm({
          title: '¿Entrar con Face ID?', message: 'La próxima vez puedes entrar a ILUS con tu cara o tu huella, sin escribir la contraseña.',
          sub: 'Se activa solo en este teléfono y lo puedes apagar cuando quieras en Mi cuenta.', okLabel: 'Activar', cancelLabel: 'Ahora no', type: 'question'
        }).then(function (si) {
          if (!si) { escribir(K_NO, String(Number(leer(K_NO) || 0) + 1)); escribir(K_TARDE, String(Date.now() + 14 * 86400000)); return; }
          registrar().then(function (r) {
            if (r.ok) avisar('✓ Face ID / huella activado. La próxima vez, entra con un toque.', 'success');
            else { avisar(r.error || 'No se pudo activar.', r.cancelado ? 'info' : 'error'); escribir(K_TARDE, String(Date.now() + 2 * 86400000)); }
          });
        });
      }, 4000);
    });
  }

  window.ilusPasskeys = { soportado: soportado, registrar: registrar, entrar: entrar, mis: mis, quitar: quitar, etiquetaDispositivo: etiquetaDispositivo,
    aB64u: aB64u, deB64u: deB64u, mensajeDeError: mensajeDeError, esCelular: esCelular };

  function arrancar() {
    try { marcarCierreDeSesion(); iniciarMiCuenta(); iniciarLogin(); ofrecer(); } catch (e) { /* nunca romper la página por esto */ }
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', arrancar); else arrancar();
})();

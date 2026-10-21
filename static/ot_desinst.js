/* 🧩 2026-10-20 — Ficha de la OT: desinstalación unida a su instalación (Daniel, 19-oct).
   «El usuario tendrá que agendar en otra hija cuando se va a hacer la desinstalación para que quede de una vez agendado… que lo
   calendarice y deje agendado todo para que el técnico sepa.»  «Que se controle por el front, tú solo debes mostrar las opciones.»
   Este archivo da vida a lo que dibuja detalle.html (window.OTD_DESINST_CFG):
     · OTDesinst.abrirProgramar()   → modal «Programar desinstalación» (instalación sin desinstalación agendada): fecha y horario,
                                       técnicos (los mismos, sugeridos, o los que elijas), costo, cobro si queda saldo en la nota de
                                       venta, equipos con su checklist, nota y, si hace falta, el argumento para Daniel.
     · OTDesinst.abrirDeclarar()    → anotar la desinstalación prevista (fecha por definir) sin crear todavía la orden.
     · OTDesinst.abrirQuitar()      → quitar la desinstalación prevista con un motivo (p. ej. el mall compra la trotadora).
   El sistema SUGIERE (fecha, técnicos, checklist) y la persona DECIDE: lo que se ve en el modal es exactamente lo que viaja.
   Sin alert/confirm/prompt nativos (REGLA #1). Nada de montos para un técnico: estos botones no existen para uno (REGLA #26).
   2026-10-21: se quitó «Evento» (Daniel: «el de eventos no me sirve, elimínalo y el código»).
   API: GET/POST /ot/api/<vid>/desinstalacion, POST /ot/api/<vid>/desinstalacion-prevista. */
(function () {
  'use strict';
  var CFG = window.OTD_DESINST_CFG || {};
  var VID = CFG.vid;

  function esc(s) {
    return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
    });
  }
  function api(url, cuerpo) {
    var init = { credentials: 'same-origin', method: cuerpo === undefined ? 'GET' : 'POST', headers: {} };
    if (cuerpo !== undefined) { init.headers['Content-Type'] = 'application/json'; init.body = JSON.stringify(cuerpo); }
    return fetch(url, init).then(function (r) {
      return r.json().catch(function () { return null; }).then(function (j) { return { http: r.status, ok: r.ok && !(j && j.ok === false), j: j || {} }; });
    }).catch(function () { return { http: 0, ok: false, j: { error: 'No hay conexión con el servidor. Reintenta.' } }; });
  }
  function toast(m, t, d) { if (window.ilusToast) window.ilusToast(m, { type: t || 'info', duration: d || 6000 }); }
  function bonita(iso) { var m = /^(\d{4})-(\d{2})-(\d{2})/.exec(String(iso || '')); return m ? m[3] + '/' + m[2] + '/' + m[1] : String(iso || ''); }
  function dow(iso) {
    var p = String(iso || '').split('-'); if (p.length < 3) return '';
    return ['domingo', 'lunes', 'martes', 'miércoles', 'jueves', 'viernes', 'sábado'][new Date(+p[0], +p[1] - 1, +p[2]).getDay()];
  }
  function sumarDias(iso, n) {
    var p = String(iso || '').split('-'); if (p.length < 3) return '';
    var d = new Date(+p[0], +p[1] - 1, +p[2] + n);
    return d.getFullYear() + '-' + String(d.getMonth() + 1).padStart(2, '0') + '-' + String(d.getDate()).padStart(2, '0');
  }
  function sumarMeses(iso, n) {
    var p = String(iso || '').split('-'); if (p.length < 3) return '';
    var d = new Date(+p[0], +p[1] - 1 + n, +p[2]);
    return d.getFullYear() + '-' + String(d.getMonth() + 1).padStart(2, '0') + '-' + String(d.getDate()).padStart(2, '0');
  }
  function hoyIso() {
    var h = new Date();
    return h.getFullYear() + '-' + String(h.getMonth() + 1).padStart(2, '0') + '-' + String(h.getDate()).padStart(2, '0');
  }
  function esDomingo(iso) { return dow(iso) === 'domingo'; }
  function habil(iso) { return esDomingo(iso) ? sumarDias(iso, 1) : iso; }
  function num(v) { var n = parseInt(String(v == null ? '' : v).replace(/[^0-9]/g, ''), 10); return isNaN(n) ? null : n; }
  function miles(v) { var n = num(v); return n === null ? '' : n.toLocaleString('es-CL'); }
  function clp(n) { return '$' + Math.round(n || 0).toLocaleString('es-CL'); }
  function recargar() { setTimeout(function () { location.reload(); }, 600); }

  /* ── Armado del modal Bootstrap (se crea una vez y se vuelve a pintar) ─────────────────────────────────────── */
  var modal = null, el = null;
  function asegurarModal() {
    if (el) return;
    el = document.createElement('div');
    el.className = 'modal fade';
    el.id = 'otdDesinstModal';
    el.tabIndex = -1;
    el.setAttribute('aria-hidden', 'true');
    el.innerHTML = '<div class="modal-dialog modal-lg modal-dialog-centered modal-dialog-scrollable modal-fullscreen-sm-down"><div class="modal-content"></div></div>';
    document.body.appendChild(el);
    modal = new bootstrap.Modal(el);
  }
  function contenido(html) { el.querySelector('.modal-content').innerHTML = html; }
  function cabecera(titulo, sub) {
    return '<div class="ev-mh"><i class="bi bi-box-arrow-up" style="font-size:1.5rem;color:#dc2626"></i><div><h5>' + esc(titulo) + '</h5>' +
      (sub ? '<small>' + esc(sub) + '</small>' : '') + '</div><button type="button" class="x" data-bs-dismiss="modal" aria-label="Cerrar">&times;</button></div>';
  }

  /* ══════════════════════════ PROGRAMAR DESINSTALACIÓN ══════════════════════════════════════════════════════ */
  var S = null;   // estado del formulario
  function nuevoEstado(info) {
    var tm = (info.tecnicos_madre || []).map(function (t) { return t.id; });
    var costoAuto = '0';
    return {
      info: info, fecha: '', hini: '09:00', hfin: '13:00', tec_modo: tm.length ? 'mismos' : 'otros', tecnicos: [], lider: null,
      tecM: tm, liderM: info.lider_madre || tm[0] || null, tecCatalogo: null,
      costo: tm.length ? costoAuto : '', costoAuto: !!tm.length, costoDesp: '', nota: '', cobro: null, argumento: '', cobraS: '', cobraD: '',
      fuera: {}, manFuera: {}, plant: {}, plantillas: null, argAut: '', forzar: false, enviando: false, error: '', choque: null
    };
  }
  function tecEfectivos() { return S.tec_modo === 'mismos' ? S.tecM.slice() : S.tecnicos.slice(); }
  function liderEfectivo() { return S.tec_modo === 'mismos' ? S.liderM : S.lider; }
  function tecObj(id) { return (S.tecCatalogo || []).filter(function (t) { return t.id === id; })[0] || null; }
  function tecNombre(id) {
    var m = (S.info.tecnicos_madre || []).filter(function (t) { return t.id === id; })[0];
    if (m && m.nombre) return m.nombre;
    var t = tecObj(id); return t ? t.nombre : ('Técnico #' + id);
  }
  function hayExterno() { return tecEfectivos().some(function (id) { var t = tecObj(id); return !!(t && t.es_externo); }); }
  function equiposIncluidos() { return (S.info.equipos || []).filter(function (e) { return !S.fuera[e.maquina_id]; }); }
  function plantEleg(e) { return S.plant[e.maquina_id] || e.plantilla_sugerida_id || null; }
  function remanente() { return S.info.remanente || { servicio: 0, despacho: 0, leido: false, completo: true, elige: false }; }
  /* 2026-10-20 (revisión): una instalación que no se cobra (garantía, regalía, arriendo) no cubre a su desinstalación, y con
     Random sin responder (lectura incompleta) no se agenda a ciegas: el servidor lo exige, acá se le dice a la persona. */
  function madreCobra() { return S.info.madre_cobra !== false; }
  function lecturaIncompleta() { return remanente().completo === false; }
  function requiereAut() { return !!(S.info.contexto && S.info.contexto.requiere_autorizacion) && (remanente().elige ? S.cobro !== 'cobra' : true); }

  function abrirProgramar() {
    if (!CFG.puedeMetadata) { toast('No tienes permiso para editar esta OT.', 'warning'); return; }
    asegurarModal();
    contenido(cabecera('Programar desinstalación', 'Cargando lo que se copia de la instalación…') +
      '<div class="modal-body"><div class="ev-vacio"><span class="spinner-border spinner-border-sm me-2"></span>Cargando…</div></div>');
    modal.show();
    api('/ot/api/' + VID + '/desinstalacion').then(function (r) {
      if (!r.ok || !r.j.contexto) {
        contenido(cabecera('Programar desinstalación', '') + '<div class="modal-body"><div class="ev-error">' + esc(r.j.error || 'No se pudo leer la instalación.') + '</div></div>' +
          '<div class="modal-footer"><button type="button" class="ev-btn" data-bs-dismiss="modal">Cerrar</button></div>');
        return;
      }
      var c = r.j.contexto;
      if (!c.es_madre || !c.puede_programar) {
        contenido(cabecera('Programar desinstalación', '') + '<div class="modal-body"><div class="ev-aviso"><i class="bi bi-info-circle-fill"></i><span>' +
          esc(c.motivo_no_programar || 'Esta OT no admite una desinstalación.') + '</span></div></div>' +
          '<div class="modal-footer"><button type="button" class="ev-btn" data-bs-dismiss="modal">Cerrar</button></div>');
        return;
      }
      S = nuevoEstado(r.j);
      pintar();
      /* Catálogos que ayudan a decidir: técnicos (nombres, empresa) y checklists de desinstalación. */
      api('/mantenciones/api/tecnicos').then(function (rt) {
        var l = Array.isArray(rt.j) ? rt.j : (rt.j.tecnicos || []);
        S.tecCatalogo = l;
        if (S.tec_modo === 'mismos') autoCosto();
        pintar();
      });
      api('/mantenciones/api/plantillas?tipo_ot=desinstalacion&activa=1').then(function (rp) {
        S.plantillas = ((rp.j && rp.j.plantillas) || []).filter(function (p) { return (p.items_count || 0) > 0; });
        pintar();
      });
    });
  }

  /* El costo sugerido de un técnico propio es $0 (visible y editable); con un externo se deja vacío para que lo escriba. */
  function autoCosto() {
    var vacio = String(S.costo == null ? '' : S.costo).trim() === '';
    if (hayExterno()) { if (S.costoAuto) { S.costo = ''; S.costoAuto = false; } }
    else if (tecEfectivos().length && (vacio || S.costoAuto)) { S.costo = '0'; S.costoAuto = true; }
  }

  function avisosFecha() {
    var h = '', f = S.fecha, fer = (S.info.feriados || {});
    if (f && CFG.fechaMadre && f < CFG.fechaMadre) h += '<div class="ev-aviso rojo" style="margin-top:10px"><i class="bi bi-exclamation-octagon-fill"></i><span>La desinstalación no puede ir antes de la instalación (<b>' + bonita(CFG.fechaMadre) + '</b>).</span></div>';
    if (f && esDomingo(f)) h += '<div class="ev-aviso rojo" style="margin-top:10px"><i class="bi bi-slash-circle-fill"></i><span>Los domingos no se agendan órdenes de trabajo. Elige otro día.</span></div>';
    if (f && fer[f]) h += '<div class="ev-aviso" style="margin-top:10px"><i class="bi bi-exclamation-triangle-fill"></i><span>Es feriado (<b>' + esc(fer[f]) + '</b>): confirma con el cliente y el técnico.</span></div>';
    else if (f && dow(f) === 'sábado') h += '<div class="ev-aviso" style="margin-top:10px"><i class="bi bi-exclamation-triangle-fill"></i><span>Cae en sábado: ILUS no opera fines de semana, confirma que esté acordado.</span></div>';
    if (S.hini && S.hfin && S.hini >= S.hfin) h += '<div class="ev-aviso rojo" style="margin-top:10px"><i class="bi bi-clock-history"></i><span>La hora de término debe ser posterior a la de inicio.</span></div>';
    return h;
  }
  function completo() {
    if (!S.fecha || esDomingo(S.fecha) || (CFG.fechaMadre && S.fecha < CFG.fechaMadre)) return false;
    if (!S.hini || !S.hfin || S.hini >= S.hfin) return false;
    if (!tecEfectivos().length || !liderEfectivo()) return false;
    if (String(S.costo == null ? '' : S.costo).trim() === '') return false;
    var r = remanente();
    if (lecturaIncompleta()) return false;
    if (!madreCobra() && !(r.elige && S.cobro === 'cobra')) return false;
    if (r.elige) {
      if (!S.cobro) return false;
      if (S.cobro === 'cobra' && !((num(S.cobraS) || 0) + (num(S.cobraD) || 0) > 0)) return false;
      if (S.cobro === 'cubierta' && (S.argumento || '').trim().length < 30) return false;
    }
    if ((S.info.equipos || []).length) {
      var inc = equiposIncluidos();
      if (!inc.length || inc.some(function (e) { return !plantEleg(e); })) return false;
    }
    if (requiereAut() && (S.argAut || '').trim().length < 30) return false;
    return true;
  }
  function pintar() {
    if (!S) return;
    var r = remanente(), her = S.info.hereda || {};
    var okFecha = !!S.fecha && !esDomingo(S.fecha) && !(CFG.fechaMadre && S.fecha < CFG.fechaMadre) && S.hini < S.hfin;
    var sug = S.info.fecha_sugerida;
    var atajos = '';
    var base = CFG.fechaMadre || hoyIso();
    var op = [['En 1 semana', sumarDias(base, 7)], ['En 2 semanas', sumarDias(base, 14)], ['En 1 mes', sumarMeses(base, 1)], ['En 2 meses', sumarMeses(base, 2)]];
    atajos = '<div class="o2m-di-atajos" role="group" aria-label="Fechas rápidas">' +
      (sug ? '<button type="button" class="o2m-di-atajo sug" data-fecha="' + esc(sug) + '"><i class="bi bi-stars"></i>Sugerida: ' + bonita(sug) +
        (S.info.fecha_sugerida_motivo ? ' · ' + esc(String(S.info.fecha_sugerida_motivo).replace(/^Propuesta:\s*/, '')) : '') + '</button>' : '') +
      op.map(function (x) { var f = habil(x[1]); return '<button type="button" class="o2m-di-atajo" data-fecha="' + f + '">' + x[0] + ' · ' + bonita(f) + '</button>'; }).join('') + '</div>';

    var lista = tecEfectivos(), lid = liderEfectivo();
    var tecHtml = '<div class="o2m-di-sub ' + (lista.length && lid ? 'ok' : 'mal') + '"><h6><span class="pt"></span>Técnicos de la desinstalación</h6>' +
      '<div class="o2m-di-modo">' +
      '<button type="button" class="o2m-di-op' + (S.tec_modo === 'mismos' ? ' sel' : '') + '" data-tecmodo="mismos"' + (S.tecM.length ? '' : ' disabled style="opacity:.5"') + ' aria-pressed="' + (S.tec_modo === 'mismos') + '">' +
        '<i class="bi bi-people-fill ic"></i><span class="n">Los mismos de la instalación</span><span class="l">Sugerido: quienes ya conocen el lugar</span></button>' +
      '<button type="button" class="o2m-di-op' + (S.tec_modo === 'otros' ? ' sel' : '') + '" data-tecmodo="otros" aria-pressed="' + (S.tec_modo === 'otros') + '">' +
        '<i class="bi bi-person-lines-fill ic"></i><span class="n">Elegir otros</span><span class="l">Otro equipo para desarmar y embalar</span></button></div>' +
      (S.tec_modo === 'otros' ? pickerHtml() : '') +
      (lista.length ? '<div class="o2m-di-tec" style="margin-top:10px">' + lista.map(function (id) {
        return '<span class="o2m-di-tchip' + (id === lid ? ' lider' : '') + '"><i class="bi bi-person-fill"></i>' + esc(tecNombre(id)) + (id === lid ? ' · líder' : '') + '</span>';
      }).join('') + '</div>' : '<div class="ev-aviso rojo" style="margin-top:10px"><i class="bi bi-person-x-fill"></i><span>Elige al menos un técnico: ninguna orden sale sin responsable.</span></div>') + '</div>';

    var ext = hayExterno();
    var costoHtml = '<div class="o2m-di-sub ' + (String(S.costo).trim() !== '' ? 'ok' : 'falta') + '"><h6><span class="pt"></span>Cuánto cuesta la desinstalación</h6>' +
      '<div class="o2m-di-fila dos"><div><label for="dsCosto">' + (ext ? 'Lo que te cobra el técnico o proveedor *' : 'Lo que cuesta el técnico (0 si es propio) *') + '</label>' +
      '<input id="dsCosto" inputmode="numeric" autocomplete="off" placeholder="$ 0" value="' + esc(miles(S.costo)) + '" data-monto="costo"></div>' +
      '<div><label for="dsCostoD">Despacho (opcional)</label><input id="dsCostoD" inputmode="numeric" autocomplete="off" placeholder="$ 0" value="' + esc(miles(S.costoDesp)) + '" data-monto="costoDesp"></div></div>' +
      '<div class="ev-ayuda" style="margin-top:6px">Es obligatorio para poder pagarle al proveedor sin equivocarnos. ' + (ext ? 'Es un técnico externo: se le manda el anexo de servicios.' : '') + '</div></div>';

    var cobroHtml = '';
    var sinCobroTxt = '<div class="ev-aviso rojo" style="margin:10px 0"><i class="bi bi-shield-lock-fill"></i><span><b>Esta instalación no se cobra</b> (garantía, regalía, arriendo o sin costo): ' +
      'su desinstalación no puede quedar cubierta por ella, porque ese $0 se autorizó solo para la instalación. ' +
      (r.elige ? 'Si la nota de venta trae una línea de desinstalación, elige «La cobra la desinstalación». ' : '') +
      'Si no, créala como una OT de desinstalación aparte, con su propio documento o su propia autorización de $0.</span></div>';
    if (lecturaIncompleta()) {
      cobroHtml = '<div class="ev-aviso rojo" style="margin:10px 0"><i class="bi bi-cloud-slash-fill"></i><span><b>No pudimos leer el saldo de la nota de venta en Random.</b> ' +
        'Por seguridad la desinstalación no se agenda a ciegas (podría cobrarse dos veces la misma línea). Espera unos segundos y reintenta.</span>' +
        '<button type="button" class="ev-mini-btn" data-reintentar="1" style="margin-left:8px">Reintentar</button></div>';
    } else if (r.elige) {
      cobroHtml = '<div class="o2m-di-sub ' + (S.cobro ? 'ok' : 'falta') + '"><h6><span class="pt"></span>¿Quién cobra la desinstalación?</h6>' +
        '<p class="ev-ayuda" style="margin:0 0 10px">A la nota de venta le queda saldo sin usar después de la instalación: <b>' + clp(r.servicio) + '</b>' +
        (r.despacho > 0 ? ' de servicio y <b>' + clp(r.despacho) + '</b> de despacho' : '') + '. Elige cómo se trata; nunca se cobra dos veces la misma línea.</p>' +
        '<div class="o2m-di-cobro">' +
        '<button type="button" class="o2m-di-op' + (S.cobro === 'cobra' ? ' sel' : '') + '" data-cobro="cobra" aria-pressed="' + (S.cobro === 'cobra') + '"><i class="bi bi-receipt-cutoff ic"></i><span class="n">La cobra la desinstalación</span><span class="l">Toma ese saldo de la nota de venta como su propio cobro</span></button>' +
        (madreCobra() ? '<button type="button" class="o2m-di-op' + (S.cobro === 'cubierta' ? ' sel' : '') + '" data-cobro="cubierta" aria-pressed="' + (S.cobro === 'cubierta') + '"><i class="bi bi-link-45deg ic"></i><span class="n">Cubierta por la instalación</span><span class="l">No cobra aparte: el saldo queda libre para otras órdenes</span></button>' : '') + '</div>' +
        (madreCobra() ? '' : sinCobroTxt) +
        (S.cobro === 'cobra' ? '<div class="o2m-di-fila dos" style="margin-top:10px"><div><label for="dsCobS">Servicio (máx. ' + clp(r.servicio) + ')</label><input id="dsCobS" inputmode="numeric" value="' + esc(miles(S.cobraS)) + '" data-monto="cobraS"></div>' +
          '<div><label for="dsCobD">Despacho (máx. ' + clp(r.despacho) + ')</label><input id="dsCobD" inputmode="numeric" value="' + esc(miles(S.cobraD)) + '" data-monto="cobraD"></div></div>' : '') +
        (S.cobro === 'cubierta' ? '<div style="margin-top:10px"><label class="ev-lbl" for="dsArg">Por qué queda saldo sin usar (mínimo 30 caracteres) *</label>' +
          '<textarea class="ev-in" id="dsArg" maxlength="1000" data-txt="argumento" placeholder="Ej: el cliente acordó que la desinstalación va incluida en el precio de la instalación">' + esc(S.argumento) + '</textarea>' +
          '<div class="ev-ayuda" id="dsArgN">' + (S.argumento || '').trim().length + ' / 30 caracteres mínimos. Queda guardado con tu nombre.</div></div>' : '') + '</div>';
    } else if (!madreCobra()) {
      cobroHtml = sinCobroTxt;
    } else {
      cobroHtml = '<div class="ev-ayuda" style="margin:10px 0"><i class="bi bi-receipt me-1"></i>Cubierta por la instalación: no consume saldo aparte.</div>';
    }

    var eqs = S.info.equipos || [], plants = S.plantillas || [];
    var eqHtml = '';
    if (eqs.length) {
      eqHtml = '<div class="o2m-di-sub ' + (equiposIncluidos().length && equiposIncluidos().every(function (e) { return !!plantEleg(e); }) ? 'ok' : 'falta') + '"><h6><span class="pt"></span>Equipos que se desinstalan y su checklist</h6>' +
        '<p class="ev-ayuda" style="margin:0 0 6px">Son los equipos de la instalación. Deja marcados los que se retiran; el checklist sugerido se puede cambiar.</p>' +
        (eqs.length > 1 ? '<button type="button" class="o2m-di-atajo" data-eqtodo="1" style="margin-bottom:6px"><i class="bi bi-check-all"></i>Seleccionar/deseleccionar todo</button>' : '') +
        eqs.map(function (e) {
          var on = !S.fuera[e.maquina_id], pe = plantEleg(e), opts = plants.slice();
          if (pe && !opts.some(function (x) { return x.id === pe; })) opts.unshift({ id: pe, nombre: e.plantilla_sugerida_nombre || ('Checklist #' + pe) });
          var sel = opts.length
            ? '<select aria-label="Checklist de ' + esc(e.nombre || e.sku) + '" data-eqplant="' + e.maquina_id + '"' + (on ? '' : ' disabled') + '>' + (pe ? '' : '<option value="">Elige un checklist…</option>') +
              opts.map(function (x) { return '<option value="' + x.id + '"' + (x.id === pe ? ' selected' : '') + '>' + esc(x.nombre) + '</option>'; }).join('') + '</select>' +
              '<small class="sel-name">Checklist elegido: <b>' + esc((opts.filter(function (x) { return x.id === pe; })[0] || {}).nombre || '') + '</b></small>'
            : '<small>' + esc(e.plantilla_sugerida_nombre || 'Sin checklist configurado') + '</small>';
          return '<label class="o2m-di-eq"><input type="checkbox" data-eq="' + e.maquina_id + '"' + (on ? ' checked' : '') + '><span class="d"><b>' + esc(e.nombre || e.modelo || ('Equipo #' + e.maquina_id)) +
            (e.marca ? ' · ' + esc(e.marca) : '') + '</b>' + ((e.sku || e.serie) ? '<small>' + (e.sku ? 'SKU ' + esc(e.sku) : '') + (e.serie ? ' · Serie ' + esc(e.serie) : '') + '</small>' : '') +
            (on ? sel : '<small>No se desinstala en esta orden</small>') + '</span></label>';
        }).join('') + '</div>';
    }
    /* 2026-10-20 (revisión): antes los productos manuales de la instalación se sumaban a la desinstalación sin poder quitarlos
       (el modal solo los nombraba). Ahora cada uno se ve y se puede dejar fuera: lo que se ve y se elige es lo que se crea. */
    var mans = S.info.manuales || [];
    var manuales = mans.length
      ? '<div class="o2m-di-sub ok"><h6><span class="pt"></span>Productos manuales de la instalación</h6>' +
        '<p class="ev-ayuda" style="margin:0 0 6px">Estos productos se cargaron a mano en la instalación. Deja marcados los que también se desinstalan.</p>' +
        (mans.length > 1 ? '<button type="button" class="o2m-di-atajo" data-mantodo="1" style="margin-bottom:6px"><i class="bi bi-check-all"></i>Seleccionar/deseleccionar todo</button>' : '') +
        mans.map(function (m, i) {
          return '<label class="o2m-di-eq"><input type="checkbox" data-man="' + i + '"' + (S.manFuera[i] ? '' : ' checked') + '><span class="d"><b>' + esc(m.titulo) +
            (m.cantidad ? ' ×' + m.cantidad : '') + '</b>' + (m.sku ? '<small>SKU ' + esc(m.sku) + '</small>' : '') +
            (S.manFuera[i] ? '<small>No se desinstala en esta orden</small>' : '') + '</span></label>';
        }).join('') + '</div>' : '';

    var autHtml = '';
    if (requiereAut()) {
      autHtml = '<div class="o2m-di-sub falta"><h6><span class="pt"></span>Declarar la desinstalación pide autorización</h6>' +
        '<p class="ev-ayuda" style="margin:0 0 8px">Esta instalación no tenía la desinstalación prevista. ' +
        (CFG.esSuperadmin ? 'Como superadministrador queda registrada a tu nombre.' : 'Se le pide a Daniel y la desinstalación se agenda sola cuando la apruebe.') + ' Explica por qué es necesaria (mínimo 30 caracteres).</p>' +
        '<textarea class="ev-in" id="dsAut" maxlength="1000" data-txt="argAut" placeholder="Ej: el mall pidió retirar el equipo al terminar la feria">' + esc(S.argAut) + '</textarea>' +
        '<div class="ev-ayuda" id="dsAutN">' + (S.argAut || '').trim().length + ' / 30 caracteres mínimos.</div></div>';
    }
    var heredaHtml = '<div class="ev-aviso azul" style="margin:12px 0 0"><i class="bi bi-copy"></i><span><b>Se copia de la instalación:</b> ' +
      [her.lugar ? 'lugar ' + esc(her.lugar) : '', her.contacto && her.contacto.nombre ? 'contacto ' + esc(her.contacto.nombre) + (her.contacto.tel ? ' (' + esc(her.contacto.tel) + ')' : '') : '',
        her.centro_costo ? 'centro de costo ' + esc(her.centro_costo) : '',
        (her.documentos || []).length ? 'documentos ' + (her.documentos || []).map(function (d) { return esc(d.titulo); }).join(', ') : ''].filter(Boolean).join(' · ') +
      '. La desinstalación no le escribe nada al cliente.</span></div>';

    var errHtml = S.error ? '<div class="ev-err"><i class="bi bi-exclamation-triangle-fill me-1"></i>' + esc(S.error) +
      (S.choque && S.choque.puede_forzar && !S.forzar ? ' <button type="button" class="ev-mini-btn" data-forzar="1" style="margin-left:8px">Agendar igual</button>' : '') + '</div>' : '';

    contenido(cabecera('Programar desinstalación', 'Se crea una segunda orden, unida a esta instalación') +
      '<div class="modal-body">' +
      '<div class="o2m-di-sub ' + (okFecha ? 'ok' : 'falta') + '" style="margin-top:0"><h6><span class="pt"></span>Fecha y horario</h6>' +
      '<div class="o2m-di-fila"><div><label for="dsFecha">Fecha de la desinstalación *</label><input type="date" id="dsFecha" value="' + esc(S.fecha) + '" min="' + esc(CFG.fechaMadre || hoyIso()) + '"></div>' +
      '<div><label for="dsHini">Hora de inicio</label><input type="time" id="dsHini" value="' + esc(S.hini) + '" data-txt="hini"></div>' +
      '<div><label for="dsHfin">Hora de término</label><input type="time" id="dsHfin" value="' + esc(S.hfin) + '" data-txt="hfin"></div></div>' +
      (S.fecha ? '<div class="ev-ayuda" style="margin-top:6px"><i class="bi bi-calendar3 me-1"></i><b>' + esc(dow(S.fecha).replace(/^./, function (m) { return m.toUpperCase(); })) + ' ' + bonita(S.fecha) + '</b> · la instalación es el ' + bonita(CFG.fechaMadre) + '</div>' : '') +
      atajos + avisosFecha() + '</div>' +
      tecHtml + costoHtml + cobroHtml + eqHtml + manuales +
      '<div class="o2m-di-sub" style="border-color:#e5e7eb"><h6><span class="pt" style="background:#9ca3af"></span>Nota para el técnico (opcional)</h6>' +
      '<textarea class="ev-in" id="dsNota" maxlength="1500" data-txt="nota" placeholder="Ej: desarmar y embalar en las cajas originales">' + esc(S.nota) + '</textarea></div>' +
      autHtml + heredaHtml + errHtml + '</div>' +
      '<div class="modal-footer"><button type="button" class="ev-btn" data-bs-dismiss="modal">Cancelar</button>' +
      '<button type="button" class="ev-btn pri" id="dsGuardar"' + (completo() && !S.enviando ? '' : ' disabled') + '>' +
      (S.enviando ? '<span class="spinner-border spinner-border-sm me-1"></span>Programando…' : '<i class="bi bi-calendar-check"></i>Programar desinstalación') + '</button></div>');
    enlazar();
  }
  function pickerHtml() {
    var techs = S.tecCatalogo;
    if (!techs) return '<div class="ev-ayuda">Cargando técnicos…</div>';
    if (!techs.length) return '<div class="ev-ayuda">No hay técnicos cargados.</div>';
    var bloqueada = null;
    techs.forEach(function (x) { if (S.tecnicos.indexOf(x.id) >= 0 && x.empresa_id) bloqueada = x.empresa_id; });
    var grupos = [], mapa = {};
    techs.forEach(function (t) {
      var k = t.empresa_id ? ('emp_' + t.empresa_id) : 'interno';
      if (!mapa[k]) { mapa[k] = { titulo: t.empresa_id ? (t.empresa_nombre || ('Empresa #' + t.empresa_id)) : 'Internos', items: [] }; grupos.push(k); }
      mapa[k].items.push(t);
    });
    return grupos.map(function (k) {
      var g = mapa[k];
      return '<div style="border:1px solid ' + (k === 'interno' ? '#e5e7eb' : '#dbeafe') + ';border-radius:10px;padding:.5rem .65rem;margin-bottom:.5rem;background:' + (k === 'interno' ? '#fafafa' : '#f8fafc') + '">' +
        '<div style="font-size:.72rem;font-weight:700;text-transform:uppercase;letter-spacing:.03em;color:#64748b;margin-bottom:.35rem"><i class="bi ' + (k === 'interno' ? 'bi-people-fill' : 'bi-building-fill') + ' me-1"></i>' + esc(g.titulo) + '</div>' +
        '<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(200px,1fr));gap:8px">' + g.items.map(function (t) {
          var sel = S.tecnicos.indexOf(t.id) >= 0, bloq = !sel && bloqueada && t.empresa_id && t.empresa_id !== bloqueada;
          return '<button type="button" class="o2m-di-op' + (sel ? ' sel' : '') + '" data-tec="' + t.id + '" aria-pressed="' + sel + '"' + (bloq ? ' style="opacity:.45;cursor:not-allowed" data-bloq="1"' : '') + '>' +
            '<span class="n" style="font-size:.9rem">' + (sel ? '<i class="bi bi-check-circle-fill me-1"></i>' : '') + esc(t.nombre) + '</span>' +
            '<span class="l">' + esc(t.especialidad || t.nivel || 'Técnico') + (t.es_externo ? ' · externo' : '') + (S.lider === t.id ? ' · <b>líder</b>' : '') + '</span></button>';
        }).join('') + '</div></div>';
    }).join('');
  }
  function refrescarBoton() { var b = document.getElementById('dsGuardar'); if (b) b.disabled = !(completo() && !S.enviando); }
  function enlazar() {
    var q = function (s) { return Array.prototype.slice.call(el.querySelectorAll(s)); };
    var f = document.getElementById('dsFecha');
    if (f) f.addEventListener('change', function () {
      if (this.value && esDomingo(this.value)) { toast('Los domingos no se agendan órdenes de trabajo. Elige otro día.', 'warning'); }
      S.fecha = this.value; S.error = ''; S.choque = null; S.forzar = false; pintar();
    });
    q('[data-fecha]').forEach(function (b) { b.addEventListener('click', function () { S.fecha = b.getAttribute('data-fecha'); S.error = ''; S.choque = null; pintar(); }); });
    q('[data-tecmodo]').forEach(function (b) {
      b.addEventListener('click', function () { if (b.disabled) return; S.tec_modo = b.getAttribute('data-tecmodo'); autoCosto(); S.error = ''; pintar(); });
    });
    q('[data-tec]').forEach(function (b) {
      b.addEventListener('click', function () {
        if (b.getAttribute('data-bloq')) { toast('No puedes mezclar técnicos externos de dos empresas distintas en la misma OT.', 'warning'); return; }
        var id = parseInt(b.getAttribute('data-tec'), 10), i = S.tecnicos.indexOf(id);
        if (i < 0) { S.tecnicos.push(id); if (!S.lider) S.lider = id; } else { S.tecnicos.splice(i, 1); if (S.lider === id) S.lider = S.tecnicos[0] || null; }
        autoCosto(); S.error = ''; pintar();
      });
    });
    q('[data-monto]').forEach(function (inp) {
      inp.addEventListener('input', function () {
        var k = inp.getAttribute('data-monto'), antes = inp.value, resto = antes.length - (inp.selectionStart || 0);
        S[k] = String(inp.value).replace(/[^0-9]/g, '');
        if (k === 'costo') S.costoAuto = false;
        var fm = miles(S[k]); inp.value = fm;
        try { var pos = Math.max(0, fm.length - resto); inp.setSelectionRange(pos, pos); } catch (_) { /* sin cursor */ }
        S.error = ''; refrescarBoton();
      });
    });
    q('[data-txt]').forEach(function (t) {
      t.addEventListener('input', function () {
        var k = t.getAttribute('data-txt'); S[k] = t.value; S.error = '';
        if (k === 'argumento') { var n = document.getElementById('dsArgN'); if (n) n.textContent = (S.argumento || '').trim().length + ' / 30 caracteres mínimos. Queda guardado con tu nombre.'; }
        if (k === 'argAut') { var m = document.getElementById('dsAutN'); if (m) m.textContent = (S.argAut || '').trim().length + ' / 30 caracteres mínimos.'; }
        refrescarBoton();
      });
    });
    q('[data-cobro]').forEach(function (b) {
      b.addEventListener('click', function () {
        S.cobro = b.getAttribute('data-cobro'); var r = remanente();
        if (S.cobro === 'cobra') { if (S.cobraS === '') S.cobraS = r.servicio || ''; if (S.cobraD === '') S.cobraD = r.despacho || ''; }
        S.error = ''; pintar();
      });
    });
    q('[data-eq]').forEach(function (c) {
      c.addEventListener('change', function () { var id = parseInt(c.getAttribute('data-eq'), 10); S.fuera[id] = !c.checked; pintar(); });
    });
    q('[data-eqtodo]').forEach(function (b) {
      b.addEventListener('click', function () {
        var eqs = S.info.equipos || [], algunaFuera = eqs.some(function (e) { return S.fuera[e.maquina_id]; });
        eqs.forEach(function (e) { S.fuera[e.maquina_id] = algunaFuera ? false : true; });
        pintar();
      });
    });
    q('[data-eqplant]').forEach(function (s) {
      s.addEventListener('change', function () { S.plant[parseInt(s.getAttribute('data-eqplant'), 10)] = s.value ? parseInt(s.value, 10) : null; refrescarBoton(); });
    });
    q('[data-man]').forEach(function (c) {
      c.addEventListener('change', function () { var i = parseInt(c.getAttribute('data-man'), 10); S.manFuera[i] = !c.checked; pintar(); });
    });
    q('[data-mantodo]').forEach(function (b) {
      b.addEventListener('click', function () {
        var ms = S.info.manuales || [], algunaFuera = ms.some(function (_m, i) { return S.manFuera[i]; });
        ms.forEach(function (_m, i) { S.manFuera[i] = algunaFuera ? false : true; });
        pintar();
      });
    });
    q('[data-reintentar]').forEach(function (b) { b.addEventListener('click', function () { abrirProgramar(); }); });
    q('[data-forzar]').forEach(function (b) { b.addEventListener('click', function () { S.forzar = true; S.error = ''; guardar(); }); });
    var g = document.getElementById('dsGuardar');
    if (g) g.addEventListener('click', guardar);
  }

  function cuerpo() {
    var o = { lleva: 'si', fecha: S.fecha, hora_inicio: S.hini, hora_fin: S.hfin, tecnico_user_ids: tecEfectivos(), tecnico_lider_id: liderEfectivo(),
      costo_proveedor: num(S.costo) || 0 };
    var cd = num(S.costoDesp); if (cd !== null) o.costo_despacho = cd;
    var lt = tecObj(liderEfectivo());
    if (lt) { o.proveedor_tipo = lt.es_externo ? 'externo' : 'interno'; o.proveedor_nombre = lt.es_externo ? (lt.empresa_nombre || lt.nombre) : ''; }
    if ((S.nota || '').trim()) o.nota = S.nota.trim();
    if (remanente().elige && S.cobro) {
      o.cobro = S.cobro;
      if (S.cobro === 'cubierta') o.argumento_cubierta = (S.argumento || '').trim();
      if (S.cobro === 'cobra') { o.cobra_servicio = num(S.cobraS) || 0; o.cobra_despacho = num(S.cobraD) || 0; }
    }
    if ((S.info.equipos || []).length) o.equipos = equiposIncluidos().map(function (e) { return { maquina_id: e.maquina_id, plantilla_id: plantEleg(e) }; });
    var mf = []; (S.info.manuales || []).forEach(function (_m, i) { if (S.manFuera[i]) mf.push(i); });
    if (mf.length) o.manuales_fuera = mf;
    if (requiereAut()) o.argumento = (S.argAut || '').trim();
    if (S.forzar) o.forzar_choque = true;
    return o;
  }
  function guardar() {
    if (!completo() || S.enviando) return;
    S.enviando = true; S.error = ''; pintar();
    api('/ot/api/' + VID + '/desinstalacion', cuerpo()).then(function (r) {
      S.enviando = false;
      var j = r.j || {};
      if (r.http === 202 || j.pendiente_autorizacion) {
        contenido(cabecera('Se pidió autorización a Daniel', '') + '<div class="modal-body"><div class="ev-aviso azul"><i class="bi bi-shield-lock-fill"></i><span>' +
          esc(j.mensaje || 'Cuando la apruebe, la desinstalación se agenda sola.') + '</span></div></div>' +
          '<div class="modal-footer">' + (j.url ? '<a class="ev-btn" href="' + esc(j.url) + '"><i class="bi bi-eye"></i>Ver la solicitud</a>' : '') +
          '<button type="button" class="ev-btn pri" data-bs-dismiss="modal">Entendido</button></div>');
        return;
      }
      if (!r.ok) {
        S.error = j.error || 'No se pudo programar la desinstalación.';
        S.choque = (j.error_codigo === 'CHOQUE_HORARIO' || j.codigo === 'CHOQUE_HORARIO') ? { puede_forzar: !!j.puede_forzar } : null;
        pintar();
        return;
      }
      var d = j.desinstalacion || {};
      contenido(cabecera('Desinstalación programada', '') + '<div class="modal-body"><div class="ev-aviso azul" style="background:#f0fdf4;border-color:#bbf7d0;color:#166534"><i class="bi bi-check-circle-fill"></i><span><b>' +
        esc(d.numero_ot || 'La orden') + '</b> quedó agendada para el <b>' + esc(d.fecha_txt || '') + '</b>.</span></div>' +
        ((d.avisos || []).length ? d.avisos.map(function (a) { return '<div class="ev-aviso"><i class="bi bi-exclamation-triangle-fill"></i><span>' + esc(a) + '</span></div>'; }).join('') : '') + '</div>' +
        '<div class="modal-footer">' + (d.ot_url ? '<a class="ev-btn" href="' + esc(d.ot_url) + '"><i class="bi bi-box-arrow-up-right"></i>Abrir ' + esc(d.numero_ot || 'la orden') + '</a>' : '') +
        '<button type="button" class="ev-btn pri" id="dsListo">Listo</button></div>');
      var l = document.getElementById('dsListo'); if (l) l.addEventListener('click', function () { location.reload(); });
      el.addEventListener('hidden.bs.modal', function () { location.reload(); }, { once: true });
    });
  }

  /* ══════════════════════════ DESINSTALACIÓN PREVISTA (sin crear la orden) ══════════════════════════════════ */
  function abrirDeclarar() {
    if (!CFG.puedeMetadata) { toast('No tienes permiso para editar esta OT.', 'warning'); return; }
    var c = CFG.ctx || {};
    var cambiar = !!c.prevista_iso;
    var base = CFG.fechaMadre || hoyIso();
    var aut = !!c.requiere_autorizacion && !cambiar;
    var ov = document.createElement('div');
    ov.className = 'ev-ov';
    ov.setAttribute('role', 'dialog'); ov.setAttribute('aria-modal', 'true'); ov.setAttribute('aria-label', 'Desinstalación prevista');
    ov.innerHTML = '<div class="ev-ov-box"><div class="ev-ov-h"><b><i class="bi bi-box-arrow-up"></i>' + (cambiar ? 'Cambiar la desinstalación prevista' : 'Anotar la desinstalación prevista') + '</b>' +
      '<button type="button" class="ev-ov-x" data-x aria-label="Cerrar">&times;</button></div>' +
      '<div class="ev-ov-b"><div class="full ev-ayuda" style="font-size:.84rem">Anota cuándo se espera desinstalar esta instalación, aunque todavía no estén definidos el técnico o la hora. Después se programa la orden con «Programar desinstalación».</div>' +
      '<div class="full"><label class="ev-lbl" for="dpFecha">Fecha prevista *</label><input class="ev-in" type="date" id="dpFecha" value="' + esc(c.prevista_iso || '') + '" min="' + esc(base) + '"></div>' +
      (aut ? '<div class="full"><label class="ev-lbl" for="dpArg">Por qué se desinstala (mínimo 30 caracteres) *</label><textarea class="ev-in" id="dpArg" maxlength="1000" placeholder="Ej: el mall pidió retirar el equipo al terminar la feria"></textarea>' +
        '<div class="ev-ayuda">' + (CFG.esSuperadmin ? 'Queda registrada a tu nombre.' : 'Se le pide autorización a Daniel; queda anotada cuando la apruebe.') + '</div></div>' : '') +
      '<div class="full" id="dpErr"></div></div>' +
      '<div class="ev-ov-f"><button type="button" class="ev-btn" data-x>Cancelar</button><button type="button" class="ev-btn pri" id="dpOk"><i class="bi bi-check-lg"></i>Guardar</button></div></div>';
    document.body.appendChild(ov);
    function cerrar() { ov.remove(); }
    Array.prototype.forEach.call(ov.querySelectorAll('[data-x]'), function (b) { b.addEventListener('click', cerrar); });
    ov.addEventListener('keydown', function (e) { if (e.key === 'Escape') cerrar(); });
    ov.querySelector('#dpOk').addEventListener('click', function () {
      var f = ov.querySelector('#dpFecha').value, arg = aut ? (ov.querySelector('#dpArg').value || '').trim() : '';
      var err = ov.querySelector('#dpErr');
      function mal(m) { err.innerHTML = '<div class="ev-err">' + esc(m) + '</div>'; }
      if (!f) return mal('Elige la fecha prevista.');
      if (esDomingo(f)) return mal('Los domingos no se agendan órdenes de trabajo. Elige otro día.');
      if (CFG.fechaMadre && f < CFG.fechaMadre) return mal('La desinstalación no puede ir antes de la instalación (' + bonita(CFG.fechaMadre) + ').');
      if (aut && arg.length < 30) return mal('Explica por qué se desinstala (mínimo 30 caracteres).');
      api('/ot/api/' + VID + '/desinstalacion-prevista', { fecha: f, argumento: arg }).then(function (r) {
        if (r.http === 202 || (r.j && r.j.pendiente_autorizacion)) { cerrar(); toast(r.j.mensaje || 'Se pidió autorización a Daniel.', 'success', 8000); return; }
        if (!r.ok) return mal(r.j.error || 'No se pudo guardar.');
        cerrar(); toast('✓ Desinstalación prevista anotada para el ' + bonita(f), 'success'); recargar();
      });
    });
    setTimeout(function () { var i = ov.querySelector('#dpFecha'); if (i) i.focus(); }, 80);
  }

  function abrirQuitar() {
    if (!CFG.puedeMetadata) { toast('No tienes permiso para editar esta OT.', 'warning'); return; }
    window.ilusPrompt({
      title: 'Quitar la desinstalación prevista', message: 'Explica por qué esta instalación ya no se desinstala.',
      sub: 'Queda guardado con tu nombre y la fecha. Por ejemplo: el cliente compró el equipo.',
      placeholder: 'Ej: el mall compró la trotadora', required: true, multiline: true, okLabel: 'Quitar', cancelLabel: 'Volver'
    }).then(function (motivo) {
      if (!motivo) return;
      if (String(motivo).trim().length < 10) { toast('Explica el motivo con al menos 10 caracteres.', 'warning'); return; }
      api('/ot/api/' + VID + '/desinstalacion-prevista', { fecha: null, motivo: String(motivo).trim() }).then(function (r) {
        if (!r.ok) { toast(r.j.error || 'No se pudo quitar la desinstalación prevista.', 'error'); return; }
        toast('Desinstalación prevista quitada.', 'success'); recargar();
      });
    });
  }

  window.OTDesinst = { abrirProgramar: abrirProgramar, abrirDeclarar: abrirDeclarar, abrirQuitar: abrirQuitar };
})();

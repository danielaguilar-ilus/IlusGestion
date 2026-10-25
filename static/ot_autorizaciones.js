/* Autorizaciones remotas (Daniel 2026-10-07): "pidiendo autorización remota con un argumento… que dejemos con la
   trazabilidad de quién autorizó". Dos pantallas con este mismo archivo:
     · /ot/autorizaciones        → bandeja paginada (REGLA #4.3): <div id="ppAut" data-modo="lista">
     · /ot/autorizaciones/<id>   → una solicitud con todo lo que se creará: <div id="ppAut" data-modo="detalle" data-id="N">
   Aprobar → ilusConfirm; Rechazar → ilusPrompt con comentario obligatorio. Sin alert/confirm/prompt nativos (REGLA #1).
   API: GET /ot/api/autorizaciones, GET /ot/api/autorizaciones/<id>, POST …/aprobar, POST …/rechazar. */
(function () {
  'use strict';
  var raiz = document.getElementById('ppAut');
  if (!raiz) return;
  var modo = raiz.getAttribute('data-modo');
  var esSA = raiz.getAttribute('data-sa') === '1';
  var estado = 'pendiente', page = 1, porPag = 20;

  function esc(s) {
    return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
    });
  }
  function clp(n) {
    if (n === null || n === undefined || n === '' || isNaN(Number(n))) return '—';
    return '$' + Math.round(Number(n)).toString().replace(/\B(?=(\d{3})+(?!\d))/g, '.');
  }
  function api(url, body) {
    var init = { credentials: 'same-origin', method: body === undefined ? 'GET' : 'POST', headers: {} };
    if (body !== undefined) { init.headers['Content-Type'] = 'application/json'; init.body = JSON.stringify(body); }
    return fetch(url, init).then(function (r) {
      return r.json().catch(function () { return null; }).then(function (j) { return { ok: r.ok && !(j && j.ok === false), j: j || {} }; });
    }).catch(function () { return { ok: false, j: { error: 'No hay conexión con el servidor.' } }; });
  }
  function toast(m, t) { if (window.ilusToast) window.ilusToast(m, { type: t || 'info' }); }
  var CENTROS = { sstt: 'Servicio Técnico', logistica: 'Logística', comercial: 'Comercial', marketing: 'Marketing' };
  var ICONOS = { pendiente: 'bi-hourglass-split', aprobada: 'bi-check-lg', rechazada: 'bi-x-lg', anulada: 'bi-slash-circle' };
  var ENTIDAD = { ot: 'Orden de trabajo', ticket: 'Ticket', cotizacion: 'Cotización' };

  /* Una solicitud que CREA la OT al aprobarse: 'crear_sin_documento', 'documento_observado' (2026-10-12) y 'exceder_saldo'
     cuando todavía no hay OT (2026-10-08). */
  function creaOt(a) {
    return a.tipo === 'crear_sin_documento' || a.tipo === 'documento_observado' || (a.tipo === 'exceder_saldo' && !a.visita_id);
  }
  function tarjeta(a, completa) {
    var titulo = a.tipo_txt + (a.motivo_txt ? ' · ' + a.motivo_txt : '');
    var destino = a.visita_id
      ? '<a href="/ot/' + a.visita_id + '">' + esc(a.numero_ot || ('OT ' + a.visita_id)) + '</a>'
      : (creaOt(a) ? 'OT por crear' : '—');
    var h = '<article class="pp-card ' + esc(a.estado) + '" data-id="' + a.id + '"><span class="pp-circ"><i class="bi ' + (ICONOS[a.estado] || 'bi-circle') + '"></i></span>' +
      '<h3><a href="' + esc(a.url) + '">' + esc(titulo) + '</a><span class="pp-estado">' + esc(a.estado_txt) + '</span></h3>' +
      '<div class="pp-sub">Solicitud N° ' + a.id + ' · ' + esc(ENTIDAD[a.entidad] || a.entidad) + ' · ' + esc(a.cliente || 'Sin cliente') + '</div>' +
      '<dl class="pp-datos">' +
      '<div><dt>La pidió</dt><dd>' + esc(a.solicitado_por_nombre) + '<br><small>' + esc(a.solicitado_at) + '</small></dd></div>' +
      '<div><dt>' + (a.visita_id ? 'Orden de trabajo' : 'Qué se creará') + '</dt><dd>' + destino + '</dd></div>' +
      '<div><dt>Centro de costo</dt><dd>' + esc(CENTROS[a.centro_costo] || a.centro_costo || 'Sin definir') + '</dd></div>' +
      '<div><dt>Valorizado sugerido</dt><dd>' + clp(a.valorizado_clp) + '</dd></div>' +
      /* 🧭 2026-10-25 (REGLA #47): el «por qué» clasificado y quién lo asume (la trazabilidad completa: quién la pidió, por qué, quién autorizó y cuándo). */
      '<div><dt>Por qué (clasificación)</dt><dd>' + (a.intencion_txt ? '<b>' + esc(a.intencion_txt) + '</b>' : '<small>Sin clasificar</small>') + '</dd></div>' +
      '<div><dt>Quién lo asume</dt><dd>' + esc(a.asume_txt || CENTROS[a.centro_costo] || 'Sin definir') + '</dd></div></dl>' +
      (a.mensaje_sistema ? '<div class="pp-arg" style="border-left-color:#92400e;background:#fff8e1"><small>Lo que detectó el sistema</small>' + esc(a.mensaje_sistema) + '</div>' : '') +
      '<div class="pp-arg"><small>Argumento (completo) · de ' + esc(a.solicitado_por_nombre || 'quien la pide') + '</small>' + esc(a.argumento) + '</div>';
    /* 🧩 2026-10-20: si la solicitud trae una desinstalación, quien autoriza lo lee: al aprobar se crean las dos órdenes. */
    if (a.incluye_desinstalacion) {
      h += '<div class="pp-arg" style="border-left-color:#1d4ed8;background:#eff6ff"><small>Incluye</small><i class="bi bi-box-arrow-up me-1"></i>' + esc(a.incluye_desinstalacion) + '</div>';
    }
    /* 2026-10-08: cobrar más que el saldo de la línea del documento: qué se quiere cobrar. */
    if (a.tipo === 'exceder_saldo' && a.payload && a.payload.saldo_pedido) {
      var sp = a.payload.saldo_pedido;
      h += '<div class="pp-arg"><small>Quiere cobrar por encima del saldo</small>' +
        [sp.servicio ? 'Servicio ' + clp(sp.servicio) : '', sp.despacho ? 'Despacho ' + clp(sp.despacho) : ''].filter(Boolean).join(' · ') + '</div>';
    }
    /* 🗓️ 2026-10-12: documento observado (más de 3 meses o productos sin saldo en Random). Lo calculó el servidor al
       pedirla: es exactamente lo que se autoriza (si aparece otra observación, hay que volver a pedir). */
    if (a.payload && a.payload._observaciones_doc && a.payload._observaciones_doc.length) {
      h += '<div class="pp-arg pp-perdida"><small>Documento observado: lo que autorizas</small>' +
        a.payload._observaciones_doc.map(function (d) {
          return (d.observaciones || []).map(function (o) { return esc(o.texto); }).join('<br>');
        }).join('<br>') + '</div>';
    }
    /* 📒 2026-10-11: cerrar con pérdida: la cuenta de la OT al pedirla (se autoriza hasta esa pérdida). */
    if (a.tipo === 'cerrar_con_perdida' && a.payload && a.payload.perdida) {
      /* ⚖️ 2026-10-12: si viene del pago al proveedor, se ve contra lo que la factura respalda (punto de equilibrio). */
      if (a.payload.pago_proveedor !== undefined) {
        h += '<div class="pp-arg pp-perdida"><small>Pago al proveedor sobre el punto de equilibrio</small>Se le paga ' +
          clp(a.payload.pago_proveedor) + ' al proveedor · la OT cobra ' + clp(a.payload.cobro_efectivo) +
          ' con respaldo de su factura' + (a.payload.saldo_usado_por_otras ? ' (' + clp(a.payload.saldo_usado_por_otras) +
          ' de su factura ya lo usaron otras OT)' : '') + ' · <b>se pierden ' + clp(a.payload.perdida) + '</b></div>';
      } else {
        h += '<div class="pp-arg pp-perdida"><small>La cuenta de la OT al pedirla</small>Cobramos ' + clp(a.payload.cobre) +
          ' · nos cobraron ' + clp(a.payload.me_cobraron) + ' · <b>se pierden ' + clp(a.payload.perdida) + '</b></div>';
      }
    }
    if (a.constancia) h += '<div class="pp-const"><b>' + esc(a.estado_txt) + '.</b> ' + esc(a.constancia) + '</div>';
    if (a.estado === 'aprobada' && a.visita_creada_id) h += '<div class="pp-const">La OT creada: <a href="/ot/' + a.visita_creada_id + '"><b>abrirla</b></a></div>';
    h += '<div class="pp-acc">';
    if (a.estado === 'pendiente' && esSA) {
      /* Aprobar una creación crea la OT en ese instante, sin vuelta atrás: desde la lista solo se rechaza o se abre
         el detalle ("Ver todo": tipo de trabajo, equipos, montos, proveedor); se aprueba viendo qué se creará. */
      if (creaOt(a) && !completa) {
        h += '<a class="pp-btn ok" href="' + esc(a.url) + '"><i class="bi bi-eye"></i> Ver qué se creará y aprobar</a>';
      } else {
        h += '<button type="button" class="pp-btn ok" data-aprobar="' + a.id + '"><i class="bi bi-check-lg"></i> Aprobar</button>';
      }
      h += '<button type="button" class="pp-btn mal" data-rechazar="' + a.id + '"><i class="bi bi-x-lg"></i> Rechazar</button>';
    }
    if (!completa && !(a.estado === 'pendiente' && esSA && creaOt(a))) h += '<a class="pp-btn" href="' + esc(a.url) + '"><i class="bi bi-eye"></i> Ver todo</a>';
    if (a.visita_id) h += '<a class="pp-btn" href="/ot/' + a.visita_id + '"><i class="bi bi-clipboard2-pulse"></i> Abrir la OT</a>';
    return h + '</div></article>';
  }

  /* Lo que se va a crear (solicitud 'crear_sin_documento'): el payload del asistente, en palabras. */
  function queSeCreara(a) {
    var p = a.payload; if (!p || typeof p !== 'object') return '';
    var f = p.finanzas || {};
    /* 2026-10-12: el tipo con su nombre legible y la fecha en día/mes/año (REGLA #6: nunca ISO crudo). */
    var TIPOS = { levantamiento: 'Levantamiento', instalacion: 'Instalación', preventiva: 'Mantención preventiva',
      correctiva: 'Mantención correctiva', visita_tecnica: 'Visita técnica', inspeccion: 'Inspección', garantia: 'Garantía',
      cambio_equipo: 'Cambio de equipo', desinstalacion: 'Desinstalación', capacitacion: 'Capacitación' };
    var fecha = String(p.fecha_programada || '');
    var m = fecha.match(/^(\d{4})-(\d{2})-(\d{2})/);
    if (m) fecha = m[3] + '/' + m[2] + '/' + m[1];
    var filas = [
      ['Tipo de trabajo', TIPOS[p.tipo_ot] || p.tipo_ot], ['Título', p.titulo], ['Fecha programada', fecha],
      ['Equipos', (p.equipos || []).length ? (p.equipos || []).length + ' equipo(s)' : ''],
      ['Técnicos', (p.tecnico_user_ids || []).length ? (p.tecnico_user_ids || []).length + ' técnico(s)' : ''],
      ['Se le cobra', f.zz_monto != null ? clp(f.zz_monto) : (a.motivo ? '$0 (' + (a.motivo_txt || a.motivo) + ')' : '')],
      ['Lo que cobrará el proveedor', f.costo_proveedor != null ? clp(f.costo_proveedor) : ''],
      ['Centro de costo', CENTROS[f.centro_costo] || f.centro_costo || '']
    ].filter(function (x) { return x[1]; });
    if (!filas.length) return '';
    return '<div class="pp-que"><h4>Qué se creará si apruebas</h4><dl class="pp-datos">' +
      filas.map(function (x) { return '<div><dt>' + esc(x[0]) + '</dt><dd>' + esc(x[1]) + '</dd></div>'; }).join('') + '</dl>' +
      (p.descripcion ? '<div class="pp-arg"><small>Motivo de la visita</small>' + esc(p.descripcion) + '</div>' : '') + '</div>';
  }

  function aprobar(id, despues) {
    return window.ilusConfirm({ title: 'Autorizar', message: '¿Autorizas esta solicitud?',
      sub: 'Queda registrado con tu nombre, la fecha y la hora. Si es una OT por crear, se crea en este momento.',
      okLabel: 'Sí, autorizar', cancelLabel: 'Todavía no', type: 'question' }).then(function (ok) {
      if (!ok) return;
      return api('/ot/api/autorizaciones/' + id + '/aprobar', {}).then(function (r) {
        if (!r.ok) { toast(r.j.error || 'No se pudo autorizar.', 'error'); return; }
        var d = r.j.desinstalacion;   /* 2026-10-20 (revisión): lo que pasó con la desinstalación que venía en la solicitud */
        var base = r.j.numero_ot ? 'Autorizada. Se creó ' + r.j.numero_ot + '.' : 'Autorizada y registrada.';
        if (d && d.ok && d.numero_ot) base += ' Desinstalación ' + d.numero_ot + ' agendada' + (d.fecha_txt ? ' el ' + d.fecha_txt : '') + '.';
        else if (d && d.ok && d.pendiente) base += ' Quedó anotada la desinstalación prevista' + (d.fecha_txt ? ' para el ' + d.fecha_txt : '') + ': falta agendarla.';
        toast(base, 'success');
        if (d && d.ok === false) {
          /* La instalación quedó creada pero la desinstalación no se pudo agendar: no es un aviso efímero, se lee completo. */
          var msg = 'La instalación quedó creada, pero la desinstalación no se pudo agendar: ' + (d.error || 'error desconocido') +
            ' Se le avisó a quien la pidió; prográmala desde la ficha de la instalación («Programar desinstalación»).';
          if (window.ilusAlert) return window.ilusAlert({ title: 'La desinstalación no se agendó', message: msg, type: 'warning' }).then(function () { despues(); });
          toast(msg, 'warning');
        }
        despues();
      });
    });
  }
  function rechazar(id, despues) {
    return window.ilusPrompt({ title: 'Rechazar la solicitud', message: 'Explica por qué: es lo que va a leer quien la pidió.',
      placeholder: 'Ej: pídele la factura al cliente antes', required: true, multiline: true, okLabel: 'Rechazar', cancelLabel: 'Cancelar' }).then(function (txt) {
      if (!txt) return;
      return api('/ot/api/autorizaciones/' + id + '/rechazar', { comentario: txt }).then(function (r) {
        if (!r.ok) { toast(r.j.error || 'No se pudo rechazar.', 'error'); return; }
        toast('Rechazada. Se le avisó a quien la pidió.', 'success');
        despues();
      });
    });
  }

  raiz.addEventListener('click', function (e) {
    var a = e.target.closest('[data-aprobar]'), r = e.target.closest('[data-rechazar]');
    var f = e.target.closest('[data-estado]'), pg = e.target.closest('[data-pag]');
    if (a) aprobar(a.getAttribute('data-aprobar'), recargar);
    else if (r) rechazar(r.getAttribute('data-rechazar'), recargar);
    else if (f) { estado = f.getAttribute('data-estado'); page = 1; recargar(); }
    else if (pg) { page = parseInt(pg.getAttribute('data-pag'), 10) || 1; recargar(); }
  });
  raiz.addEventListener('change', function (e) {
    if (e.target.id === 'ppPor') { porPag = parseInt(e.target.value, 10) || 20; page = 1; recargar(); }
  });

  function recargar() { return modo === 'detalle' ? detalle() : lista(); }

  function lista() {
    raiz.classList.add('cargando');
    var q = '/ot/api/autorizaciones?page=' + page + '&per_page=' + porPag + (estado ? '&estado=' + estado : '');
    return Promise.all([api(q), api('/ot/api/autorizaciones?per_page=1&estado=pendiente')]).then(function (rs) {
      raiz.classList.remove('cargando');
      var r = rs[0];
      if (!r.ok) { raiz.innerHTML = '<div class="pp-error">' + esc(r.j.error || 'No se pudo leer la bandeja.') + '</div>'; return; }
      var j = r.j, pend = rs[1].ok ? rs[1].j.total : j.pendientes;
      var chips = [['pendiente', 'Esperando a Daniel'], ['aprobada', 'Autorizadas'], ['rechazada', 'Rechazadas'], ['', 'Todas']].map(function (c) {
        return '<button type="button" class="pp-chip' + (estado === c[0] ? ' on' : '') + '" data-estado="' + c[0] + '">' + c[1] +
          (c[0] === 'pendiente' ? ' <b>' + (pend || 0) + '</b>' : '') + '</button>';
      }).join('');
      var cuerpo = j.items.length ? '<div class="pp-lista">' + j.items.map(function (a) { return tarjeta(a, false); }).join('') + '</div>'
        : '<div class="pp-vacio"><i class="bi bi-inbox" style="font-size:1.8rem"></i><br>No hay solicitudes ' + (estado === 'pendiente' ? 'esperando respuesta.' : 'en esta vista.') + '</div>';
      var pie = '<div class="pp-pie"><span class="t">Mostrando ' + j.desde + '–' + j.hasta + ' de ' + j.total + '</span>' +
        '<label class="t">Por página <select id="ppPor">' + [10, 20, 50, 100].map(function (n) { return '<option' + (n === porPag ? ' selected' : '') + '>' + n + '</option>'; }).join('') + '</select></label>' +
        '<span><button type="button" class="pp-btn" data-pag="' + (j.page - 1) + '"' + (j.page <= 1 ? ' disabled' : '') + '>Anterior</button> ' +
        '<span class="t">Página ' + j.page + ' de ' + j.pages + '</span> ' +
        '<button type="button" class="pp-btn" data-pag="' + (j.page + 1) + '"' + (j.page >= j.pages ? ' disabled' : '') + '>Siguiente</button></span></div>';
      raiz.innerHTML = '<div class="pp-chips">' + chips + '</div>' + cuerpo + pie;
    });
  }

  function detalle() {
    var id = raiz.getAttribute('data-id');
    return api('/ot/api/autorizaciones/' + id).then(function (r) {
      if (!r.ok) { raiz.innerHTML = '<div class="pp-error">' + esc(r.j.error || 'No encontramos esa solicitud.') + '</div>'; return; }
      var a = r.j.autorizacion;
      raiz.innerHTML = '<div class="pp-lista">' + tarjeta(a, true) + '</div>' + queSeCreara(a) +
        '<div class="pp-acc" style="margin-top:14px">' + (esSA
          ? '<a class="pp-btn" href="/ot/autorizaciones"><i class="bi bi-arrow-left"></i> Volver a la bandeja</a>'
          : '<a class="pp-btn" href="/ot/"><i class="bi bi-arrow-left"></i> Volver a las OT</a>') + '</div>';
    });
  }
  recargar();
})();

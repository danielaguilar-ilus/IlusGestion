/* 🧩 2026-10-20 — Cancelar o reagendar una OT que está UNIDA a otra (instalación ↔ desinstalación). Revisión adversarial:
   el servidor responde 409 con un código y un mensaje claro, pero ninguna pantalla sabía qué hacer con él (solo mostraban el
   error y la persona quedaba sin salida). Este archivo da la salida, para las tres pantallas que cancelan con
   PUT /mantenciones/api/visitas/<id>: el calendario del día, el calendario de OT y la ficha del ticket.
     · OT_CON_DESINSTALACION (cancelar una instalación cuya desinstalación está agendada): pregunta «¿cancelar las dos?» y, si la
       persona confirma, reenvía el mismo PUT con `incluir_desinstalacion: 1` (el servidor cancela las dos en una transacción).
     · REAGENDAR_DESINSTALACION_PRIMERO / DESINST_EN_CURSO / DESINST_ANTES_DE_INSTALACION: avisa con un enlace a la otra orden.
   Sin alert/confirm/prompt nativos (REGLA #1). Nada de montos: solo números de OT. */
(function () {
  'use strict';
  function esc(s) {
    return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
    });
  }
  function put(vid, cuerpo) {
    return fetch('/mantenciones/api/visitas/' + vid, {
      method: 'PUT', credentials: 'same-origin', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(cuerpo)
    }).then(function (r) {
      return r.json().catch(function () { return {}; }).then(function (d) { return { r: r, d: d || {} }; });
    }).catch(function () { return { r: { ok: false, status: 0 }, d: { error: 'No hay conexión con el servidor.' } }; });
  }
  /* PUT de la OT con la salida para «cancelar las dos». Devuelve una promesa {ok, d, cancelado}: `cancelado` = la persona
     prefirió volver (no es un error). */
  function enviar(vid, cuerpo) {
    return put(vid, cuerpo).then(function (res) {
      var d = res.d;
      /* 📜 REGLA #52: cancelar o anular una OT que tiene un anexo vivo (firmado, enviado o visto) lo deja sin efecto y avisa al proveedor y a
         ILUS. El servidor EXIGE este aviso previo (409): aquí se muestra con ilusConfirm y, si la persona confirma, se reenvía con
         `confirmar_anexo: 1`. Si la OT puede reactivarse más adelante, el aviso lo dice. */
      if (!res.r.ok && d.error_codigo === 'ANEXO_REQUIERE_CONFIRMACION' && d.anexo_aviso && window.ilusConfirm) {
        var av = d.anexo_aviso;
        var sub = (av.lineas || []).map(function (l) { return '<div style="text-align:left;margin-top:4px">' + esc(l) + '</div>'; }).join('');
        return window.ilusConfirm({
          title: av.titulo || 'Esta OT tiene un anexo',
          message: '¿Confirmas que quieres ' + (av.accion === 'anular' ? 'anular' : 'cancelar') + ' la OT?',
          sub: sub, subHtml: true, okLabel: 'Sí, continuar', cancelLabel: 'Volver', danger: true
        }).then(function (ok) {
          if (!ok) return { ok: false, d: d, cancelado: true };
          return enviar(vid, Object.assign({}, cuerpo, { confirmar_anexo: 1 }));
        });
      }
      if (res.r.ok && d.ok !== false) {
        var extra = [];
        if (d.anexo_anulado && d.anexo_anulado.lineas && d.anexo_anulado.lineas.length) extra = extra.concat(d.anexo_anulado.lineas);
        if (d.anexo_reactivada) extra.push(d.anexo_reactivada);
        if (extra.length && window.ilusAlert) {
          return window.ilusAlert({ title: d.anexo_reactivada && !d.anexo_anulado ? 'Anexo anulado' : 'Anexo anulado por la anulación del servicio',
            message: 'Esto es lo que pasó con el anexo de la OT:', type: 'info',
            sub: extra.map(function (l) { return '<div style="text-align:left;margin-top:4px">' + esc(l) + '</div>'; }).join(''), subHtml: true
          }).then(function () { return { ok: true, d: d, cancelado: false }; });
        }
      }
      if (!res.r.ok && d.error_codigo === 'OT_CON_DESINSTALACION' && d.puede_incluir_desinstalacion && window.ilusConfirm) {
        var num = d.hija_numero || 'su desinstalación';
        return window.ilusConfirm({
          title: 'Esta instalación tiene su desinstalación agendada',
          message: 'La desinstalación ' + num + ' todavía no empieza. ¿Cancelas las dos órdenes?',
          sub: 'Las dos quedan canceladas juntas y conservan su historial. Si solo quieres cancelar la instalación, cancela primero la desinstalación.',
          okLabel: 'Cancelar las dos', cancelLabel: 'Volver', danger: true
        }).then(function (ok) {
          if (!ok) return { ok: false, d: d, cancelado: true };
          return put(vid, Object.assign({}, cuerpo, { incluir_desinstalacion: 1 })).then(function (res2) {
            return { ok: res2.r.ok && res2.d.ok !== false, d: res2.d, cancelado: false };
          });
        });
      }
      return { ok: res.r.ok && d.ok !== false, d: d, cancelado: false };
    });
  }
  var CON_ENLACE = ['REAGENDAR_DESINSTALACION_PRIMERO', 'DESINST_EN_CURSO', 'DESINST_ANTES_DE_INSTALACION', 'MADRE_CON_DESINSTALACION'];
  /* Si el error es de la unión instalación ↔ desinstalación, lo muestra con el enlace a la otra orden y devuelve true; si no,
     devuelve false para que la pantalla muestre su aviso de siempre. */
  function avisarError(d) {
    d = d || {};
    if (CON_ENLACE.indexOf(d.error_codigo) < 0 || !window.ilusAlert) return false;
    var otra = d.hija_id ? '<a href="/ot/' + encodeURIComponent(d.hija_id) + '">Abrir la desinstalación' + (d.hija_numero ? ' ' + esc(d.hija_numero) : '') + '</a>' : '';
    window.ilusAlert({ title: 'Primero la otra orden', message: d.error || 'La instalación y su desinstalación van unidas.', type: 'warning',
      sub: otra, subHtml: !!otra });
    return true;
  }
  window.OTCancelar = { enviar: enviar, avisarError: avisarError };
})();

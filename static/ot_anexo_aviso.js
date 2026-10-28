/* 📜 REGLA #52 — «Reenviar aviso del anexo anulado».
   Daniel (2026-10-28): un anexo cuya OT se eliminó, se canceló o se anuló queda anulado «por la anulación del servicio» y se avisa por correo al
   proveedor y a ILUS. Este botón REENVÍA ese mismo aviso. A pedido de Daniel el aviso es SOLO TEXTO: nunca lleva el anexo adjunto.

   otAnexoAviso.abrir(aid):
     1. pide al servidor a quién llegaría (GET /ot/api/anexos/<id>/aviso-anulado) y lo MUESTRA ANTES: proveedor / equipo ILUS, con el motivo si no
        hay correo o si ese correo hoy está apagado;
     2. deja elegir proveedor y/o ILUS (con «marcar / desmarcar todo», REGLA #14) y agregar otro correo (recibe la versión del proveedor);
     3. pide confirmación con ilusConfirm (REGLA #1: nada de diálogos nativos);
     4. envía (POST /ot/api/anexos/<id>/reenviar-aviso) y dice la verdad por destinatario.
   Se usa en la ficha de la OT y en la pantalla «Anexos anulados» (los huérfanos de una OT eliminada). Todo texto del servidor se escapa. */
(function (w) {
  'use strict';

  var MODAL_ID = 'otAxAvisoModal';

  function esc(s) {
    return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
    });
  }

  function api(method, url, body) {
    var opt = { method: method, credentials: 'same-origin', headers: { 'Accept': 'application/json' } };
    if (body != null) { opt.headers['Content-Type'] = 'application/json'; opt.body = JSON.stringify(body); }
    return fetch(url, opt).then(function (r) {
      return r.json().catch(function () { return {}; }).then(function (d) { return { r: r, d: d || {} }; });
    }).catch(function () { return { r: { ok: false, status: 0 }, d: { error: 'No hay conexión con el servidor.' } }; });
  }

  function toast(msg, tipo) {
    if (w.ilusToast) w.ilusToast(msg, { type: tipo || 'info' });
  }

  function alerta(titulo, mensaje, tipo, lineas) {
    if (!w.ilusAlert) return Promise.resolve(true);
    var sub = (lineas || []).map(function (l) { return '<div style="text-align:left;margin-top:4px">' + esc(l) + '</div>'; }).join('');
    return w.ilusAlert({ title: titulo, message: mensaje, type: tipo, sub: sub, subHtml: !!sub });
  }

  function asegurarModal() {
    var el = document.getElementById(MODAL_ID);
    if (el) return el;
    el = document.createElement('div');
    el.className = 'modal fade';
    el.id = MODAL_ID;
    el.tabIndex = -1;
    el.setAttribute('aria-hidden', 'true');
    el.setAttribute('aria-labelledby', MODAL_ID + 'T');
    el.innerHTML =
      '<div class="modal-dialog modal-dialog-centered modal-dialog-scrollable"><div class="modal-content">' +
      '<div class="modal-header"><h5 class="modal-title" id="' + MODAL_ID + 'T"><i class="bi bi-envelope-arrow-up me-2" style="color:#dc2626"></i>' +
      'Reenviar aviso del anexo anulado</h5><button type="button" class="btn-close" style="min-width:44px;min-height:44px;box-sizing:border-box" data-bs-dismiss="modal" aria-label="Cerrar"></button></div>' +
      '<div class="modal-body" id="' + MODAL_ID + 'B"></div>' +
      '<div class="modal-footer" style="flex-wrap:wrap;gap:8px">' +
      '<button type="button" class="btn btn-light" style="min-height:44px" data-bs-dismiss="modal">Cancelar</button>' +
      '<button type="button" class="btn btn-danger fw-bold" style="min-height:44px" id="' + MODAL_ID + 'Ok">' +
      '<i class="bi bi-send me-1"></i>Reenviar aviso</button></div></div></div>';
    document.body.appendChild(el);
    return el;
  }

  function opcion(id, titulo, detalle, deshabilitado, aviso) {
    return '<label class="otax-op" for="' + id + '" style="display:flex;gap:10px;align-items:flex-start;padding:12px;border:1px solid #e5e7eb;' +
      'border-radius:12px;margin-bottom:8px;min-height:44px;cursor:' + (deshabilitado ? 'not-allowed' : 'pointer') + ';background:' + (deshabilitado ? '#f9fafb' : '#fff') + '">' +
      '<input type="checkbox" id="' + id + '" ' + (deshabilitado ? 'disabled' : 'checked') + ' style="width:22px;height:22px;margin-top:2px;accent-color:#dc2626;flex:none">' +
      '<span style="min-width:0"><b style="display:block">' + esc(titulo) + '</b>' +
      '<span style="display:block;font-size:.88rem;color:#374151;overflow-wrap:anywhere">' + detalle + '</span>' +
      (aviso ? '<span style="display:block;font-size:.82rem;margin-top:4px;color:#78350f;background:#fff8e1;border:1px solid #fde68a;border-radius:8px;padding:4px 8px">' + esc(aviso) + '</span>' : '') +
      '</span></label>';
  }

  function pintar(d) {
    var prov = d.proveedor || {}, int_ = d.interno || {};
    var destProv = prov.destinos || [], personas = int_.personas || [];
    var ot = d.ot || {};
    var body = document.getElementById(MODAL_ID + 'B');
    body.innerHTML =
      '<div style="font-size:.92rem;margin-bottom:10px"><b>Anexo N° ' + esc(d.numero) + '</b> · ' + esc(d.proveedor_nombre) +
      '<div style="color:#4b5563">' + (ot.existe ? 'OT ' + esc(ot.numero) : 'OT ' + esc(ot.numero) + ' (eliminada)') +
      ' · ' + esc(d.motivo) + ' · por ' + esc(d.anulado_por) + ' el ' + esc(d.anulado_el) + '</div></div>' +
      '<div style="font-size:.84rem;background:#f3f4f6;border:1px solid #e5e7eb;border-radius:10px;padding:8px 10px;margin-bottom:12px">' +
      '<i class="bi bi-info-circle me-1"></i>' + esc(d.nota || 'El aviso es solo un texto: no lleva el anexo adjunto.') + '</div>' +
      '<div class="d-flex justify-content-between align-items-center mb-2"><b>¿A quién le llega?</b>' +
      '<button type="button" class="btn btn-sm btn-outline-secondary" style="min-height:44px" id="' + MODAL_ID + 'Todo">' +
      '<i class="bi bi-check-all me-1"></i>Marcar / desmarcar todo</button></div>' +
      opcion(MODAL_ID + 'Prov', 'Al proveedor',
        destProv.length ? destProv.map(esc).join(', ') : 'No tiene un correo registrado: no se le puede avisar por aquí, habrá que hacerlo por otro medio.',
        !destProv.length, destProv.length ? (prov.apagado ? 'Hoy ese correo no saldría: ' + prov.apagado + '.' : '') : '') +
      opcion(MODAL_ID + 'Int', 'Al equipo ILUS',
        personas.length ? personas.map(function (p) { return esc(p.nombre || p.correo) + ' (' + esc(p.correo) + ')'; }).join(', ')
                        : 'No hay un correo interno al que avisar.',
        !personas.length, personas.length ? (int_.apagado ? 'Hoy ese correo no saldría: ' + int_.apagado + '.' : '') : '') +
      '<label for="' + MODAL_ID + 'Extra" style="font-weight:700;margin:12px 0 4px;display:block">Agregar otro correo</label>' +
      '<input type="email" id="' + MODAL_ID + 'Extra" class="form-control" style="min-height:44px;font-size:16px" autocomplete="off" ' +
      'placeholder="nombre@empresa.cl" aria-describedby="' + MODAL_ID + 'ExtraAy">' +
      '<div id="' + MODAL_ID + 'ExtraAy" style="font-size:.8rem;color:#6b7280;margin-top:4px">Recibe el mismo aviso que el proveedor (sin datos internos).</div>' +
      '<div id="' + MODAL_ID + 'Err" role="alert" style="display:none;color:#991b1b;font-size:.88rem;margin-top:8px"></div>';
    document.getElementById(MODAL_ID + 'Todo').onclick = function () {
      var cks = [MODAL_ID + 'Prov', MODAL_ID + 'Int'].map(function (i) { return document.getElementById(i); }).filter(function (c) { return c && !c.disabled; });
      var hayDesmarcado = cks.some(function (c) { return !c.checked; });      // si ALGUNO está sin marcar → marca todos; si no → desmarca todos
      cks.forEach(function (c) { c.checked = hayDesmarcado; });
    };
  }

  function resumenDestino(d, marcaProv, marcaInt, extra) {
    var partes = [];
    var prov = (d.proveedor || {}).destinos || [];
    var pers = (d.interno || {}).personas || [];
    var aProv = (marcaProv ? prov : []).concat(extra ? [extra] : []);
    if (aProv.length) partes.push('Proveedor: ' + aProv.join(', '));
    if (marcaInt && pers.length) partes.push('Equipo ILUS: ' + pers.map(function (p) { return p.correo; }).join(', '));
    return partes;
  }

  async function abrir(aid) {
    var res = await api('GET', '/ot/api/anexos/' + encodeURIComponent(aid) + '/aviso-anulado');
    if (!res.r.ok || !res.d.ok) {
      toast(res.d.error || 'No se pudo revisar el anexo.', 'error');
      return false;
    }
    var datos = res.d;
    var el = asegurarModal();
    pintar(datos);
    var okBtn = document.getElementById(MODAL_ID + 'Ok');
    var inst = (w.bootstrap && w.bootstrap.Modal) ? w.bootstrap.Modal.getOrCreateInstance(el) : null;
    okBtn.disabled = false;
    okBtn.onclick = async function () {
      var marcaProv = document.getElementById(MODAL_ID + 'Prov').checked;
      var marcaInt = document.getElementById(MODAL_ID + 'Int').checked;
      var extra = (document.getElementById(MODAL_ID + 'Extra').value || '').trim();
      var err = document.getElementById(MODAL_ID + 'Err');
      err.style.display = 'none';
      var partes = resumenDestino(datos, marcaProv, marcaInt, extra);
      if (!partes.length) {
        err.textContent = 'Elige a quién se lo mandas: al proveedor, a ILUS o escribe un correo.';
        err.style.display = 'block';
        return;
      }
      var ok = w.ilusConfirm ? await w.ilusConfirm({
        title: 'Reenviar aviso del Anexo N° ' + datos.numero,
        message: 'Se enviará un correo con el aviso de que el anexo quedó sin efecto por la anulación del servicio.',
        sub: partes.map(esc).join('<br>') + '<br><small>Solo texto: no lleva el anexo adjunto.</small>', subHtml: true,
        okLabel: 'Reenviar aviso', cancelLabel: 'Volver'
      }) : true;
      if (!ok) return;
      okBtn.disabled = true;
      var r2 = await api('POST', '/ot/api/anexos/' + encodeURIComponent(aid) + '/reenviar-aviso',
        { proveedor: marcaProv, interno: marcaInt, extra: extra });
      okBtn.disabled = false;
      if (r2.r.status === 400 && r2.d.error_codigo === 'CORREO_INVALIDO') {
        err.textContent = r2.d.error;
        err.style.display = 'block';
        return;
      }
      if (inst) inst.hide();
      if (r2.d.lineas) {
        await alerta(r2.r.ok && r2.d.ok ? 'Aviso reenviado' : 'El aviso no salió',
          r2.r.ok && r2.d.ok ? 'Esto es lo que pasó con cada correo:' : (r2.d.error || 'No salió ningún correo.'),
          r2.r.ok && r2.d.ok ? 'success' : 'warning', r2.d.lineas);
      } else {
        toast(r2.d.error || 'No se pudo reenviar el aviso.', 'error');
      }
    };
    if (inst) inst.show();
    return true;
  }

  w.otAnexoAviso = { abrir: abrir, esc: esc };
})(window);

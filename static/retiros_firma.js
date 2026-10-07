/* ILUS Fitness · Retiros · Firma digital de recepción (ficha interna).
   · Bloque «Firma de recepción del cliente» dentro del modal «Marcar como retirado»: si se firmó, PRIMERO se guarda con POST /retiros/<id>/firma y
     luego sigue el envío de siempre del modal (/status). La firma es RECOMENDADA: sin firma el modal funciona exactamente como antes.
   · Con el retiro cerrado y sin firma: botón «Registrar firma de recepción» → modal propio. El servidor permite esa firma a propósito aunque el
     retiro esté en solo lectura; el candado del JS solo frena las rutas de gestión y /firma no está entre ellas.
   Avisos con ilusToast/ilusConfirm (REGLA #1), nunca alert/confirm nativos. */
(function () {
  'use strict';
  // El dibujo lo hace el componente único static/ilus_firma.js (vectores, nítido en retina, redibuja al girar).
  var bloque = null, canvas = null, firma = null;

  function $(id) { return document.getElementById(id); }
  function toast(msg, tipo) { if (typeof window.ilusToast === 'function') window.ilusToast(msg, { type: tipo || 'info', duration: 6000 }); }
  function rid() { return ((window.RETIROS_DETAIL_DATA || {}).reqId) || (bloque && bloque.dataset.rid) || 0; }

  // ── RUT (módulo 11), igual que el servidor ──
  function rutLimpio(v) { return String(v || '').replace(/[^0-9kK]/g, '').toUpperCase(); }
  function rutValido(v) {
    var l = rutLimpio(v);
    if (l.length < 7 || l.length > 9 || !/^\d+$/.test(l.slice(0, -1))) return false;
    var cuerpo = l.slice(0, -1), dv = l.slice(-1), suma = 0, mult = 2;
    if (parseInt(cuerpo, 10) < 1000000) return false;
    for (var i = cuerpo.length - 1; i >= 0; i--) { suma += parseInt(cuerpo[i], 10) * mult; mult = mult === 7 ? 2 : mult + 1; }
    var r = 11 - (suma % 11), esp = r === 11 ? '0' : (r === 10 ? 'K' : String(r));
    return esp === dv;
  }
  function rutFormato(v) {
    var l = rutLimpio(v);
    if (l.length < 2) return l;
    return l.slice(0, -1).replace(/\B(?=(\d{3})+(?!\d))/g, '.') + '-' + l.slice(-1);
  }

  // ── lienzo ──
  function limpiarLienzo() {
    if (!firma) return;
    firma.limpiar();      // onCambio muestra la guía y recalcula el semáforo
  }
  function iniciarLienzo() {
    canvas = $('frmCanvas');
    if (!canvas || canvas.dataset.listo) return;
    canvas.dataset.listo = '1';
    firma = window.IlusFirma.crear(canvas, {
      color: '#0a0a0a', grosor: 3.2, guia: 'frmGuia',
      onCambio: function () { estado(); }
    });
    var l = $('frmLimpiar'); if (l) l.addEventListener('click', limpiarLienzo);
    ['frmNombre', 'frmRut', 'frmRelacion', 'frmConforme', 'frmObs'].forEach(function (id) {
      var el = $(id); if (el) el.addEventListener('input', estado);
    });
    var rutEl = $('frmRut');
    if (rutEl) rutEl.addEventListener('blur', function () { if (rutEl.value) rutEl.value = rutFormato(rutEl.value); estado(); });
    estado();
  }
  function firmado() { return !!firma && firma.firmado(); }

  // semáforo: rojo = falta la firma · ámbar = firmó pero faltan datos · verde = lista
  function estado() {
    var chip = $('frmChip'), circ = $('frmCirc');
    if (!chip) return;
    var datos = ($('frmNombre').value || '').trim().length >= 2 && rutValido($('frmRut').value);
    var cls, txt;
    if (!firmado()) { cls = 'is-pend'; txt = 'Falta la firma'; }
    else if (!datos) { cls = 'is-warn'; txt = 'Faltan nombre o RUT válido'; }
    else { cls = 'is-ok'; txt = 'Firma lista'; }
    chip.className = 'frm-chip ' + cls; chip.textContent = txt;
    if (circ) circ.className = 'frm-circ' + (cls === 'is-ok' ? ' is-ok' : (cls === 'is-warn' ? ' is-warn' : ''));
  }

  function datosFirma() {
    var nombre = ($('frmNombre').value || '').trim();
    if (nombre.length < 2) return { error: 'Escribe el nombre de quien recibe.' };
    if (!rutValido($('frmRut').value)) return { error: 'El RUT no es válido: revisa el número y el dígito verificador.' };
    if (!firmado()) return { error: 'Falta la firma en el recuadro.' };
    return { datos: {
      nombre: nombre, rut: rutFormato($('frmRut').value), relacion: $('frmRelacion').value,
      conformidad: $('frmConforme').checked, observaciones: ($('frmObs').value || '').trim(),
      firma: firma.aDataUrl()
    } };
  }

  // POST /firma. Devuelve true si la firma quedó guardada (o ya lo estaba); false si hay que detenerse (ya mostró el motivo).
  async function guardarFirma(datos) {
    try {
      var r = await fetch('/retiros/' + rid() + '/firma', {
        method: 'POST', credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json', 'Accept': 'application/json', 'X-Requested-With': 'XMLHttpRequest' },
        body: JSON.stringify(datos)
      });
      var d = {}; try { d = await r.json(); } catch (e) { /* respuesta sin JSON */ }
      if (r.ok && d.ok) return true;
      if (r.status === 409 && d.code === 'FIRMA_YA_REGISTRADA') return true;
      toast((d && d.error) || 'No se pudo guardar la firma. Reintenta.', 'error');
      return false;
    } catch (e) {
      toast('Sin conexión: no se guardó la firma.', 'error');
      return false;
    }
  }

  // ── Flujo A: modal «Marcar como retirado» ──
  function enlazarModalRetirar() {
    var modal = $('modalRetirar'); if (!modal) return;
    var form = modal.querySelector('form'); if (!form || form.dataset.frmListo) return;
    form.dataset.frmListo = '1';
    form.addEventListener('submit', async function (ev) {
      if (!$('frmBloque') || form.dataset.firmaOk === '1' || !firmado()) return;   // sin firma (o ya guardada): el flujo de siempre
      ev.preventDefault(); ev.stopPropagation();
      var f = datosFirma();
      if (f.error) { toast(f.error, 'warning'); return; }
      var btn = form.querySelector('button[type=submit]');
      if (btn) { btn.disabled = true; }
      if (!(await guardarFirma(f.datos))) { if (btn) btn.disabled = false; return; }
      // quién retiró: si el campo del modal está vacío, se completa con el firmante
      var por = form.querySelector('[name=retirado_por]'), rutm = form.querySelector('[name=retirado_por_rut]');
      if (por && !por.value.trim()) por.value = f.datos.nombre;
      if (rutm && !rutm.value.trim()) rutm.value = f.datos.rut;
      form.dataset.firmaOk = '1';
      form.submit();
    }, true);
  }

  // ── Flujo B: retiro cerrado y sin firma ──
  function enlazarModalFirma() {
    var abrir = $('frmAbrir'), modal = $('modalFirma'), guardar = $('frmGuardar');
    if (!abrir || !modal) return;
    abrir.addEventListener('click', function () {
      if (window.bootstrap) window.bootstrap.Modal.getOrCreateInstance(modal).show();
    });
    modal.addEventListener('shown.bs.modal', function () { iniciarLienzo(); });
    if (guardar) guardar.addEventListener('click', async function () {
      var f = datosFirma();
      if (f.error) { toast(f.error, 'warning'); return; }
      var ok = typeof window.ilusConfirm === 'function' ? await window.ilusConfirm({
        title: '¿Guardar la firma de recepción?',
        message: 'Quedará como respaldo de la entrega de ' + f.datos.nombre + '.',
        sub: 'Una firma no se puede reemplazar ni borrar después.', okLabel: 'Sí, guardar firma', cancelLabel: 'Volver', type: 'question'
      }) : true;
      if (!ok) return;
      guardar.disabled = true;
      if (await guardarFirma(f.datos)) {
        toast('✓ Firma de recepción guardada', 'success');
        setTimeout(function () { window.location.reload(); }, 700);
      } else { guardar.disabled = false; }
    });
  }

  function init() {
    bloque = $('frmBloque');
    if (bloque) {
      // el canvas del modal «retirar» está oculto hasta abrirlo: se prepara al mostrarse
      var m = $('modalRetirar');
      if (m) m.addEventListener('shown.bs.modal', iniciarLienzo);
      enlazarModalRetirar();
    }
    enlazarModalFirma();
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init); else init();
})();

/* 📧 2026-10-28 · Enviar lotes seleccionados por correo (Facturación de proveedor).
   Daniel: «un envío de lotes seleccionados, con un mensaje y los Excel y los montos en el mail, y un cuerpo de correo bien automático».
   El navegador solo manda los ids y los textos que la persona editó; el cuerpo del correo SIEMPRE lo arma el servidor (facprov_envio.py),
   así lo que se ve en la vista previa es exactamente lo que sale. Las funciones puras de arriba las prueba tests/test_facprov_envio_js.py. */
(function (root) {
  'use strict';

  // ───────────────────────── funciones puras ─────────────────────────
  function clp(n) {
    var v = Math.round(Number(n) || 0);
    return (v < 0 ? '-$' : '$') + Math.abs(v).toString().replace(/\B(?=(\d{3})+(?!\d))/g, '.');
  }

  /** Lo marcado: cuántos lotes y cuánto suman. filas = [{monto}] */
  function resumen(filas) {
    var total = 0;
    (filas || []).forEach(function (f) { total += Number(f && f.monto) || 0; });
    return { n: (filas || []).length, total: total };
  }

  /** REGLA #14: un solo control. Si ALGÚN lote visible está sin marcar → marcar todos; si ya están todos → desmarcar. items = [{marcado}] */
  function alternarTodo(items) {
    var hayPorMarcar = (items || []).some(function (i) { return !i.marcado; });
    return hayPorMarcar ? 'marcar' : 'desmarcar';
  }

  /** Separa una lista de correos escrita por una persona (coma, punto y coma, espacios o saltos de línea). */
  function partirCorreos(txt) {
    var out = [];
    String(txt || '').split(/[\s,;]+/).forEach(function (c) { c = c.trim().toLowerCase(); if (c && out.indexOf(c) === -1) out.push(c); });
    return out;
  }

  /** Agrega un correo a una lista escrita, sin repetirlo. */
  function agregarCorreo(txt, correo) {
    var l = partirCorreos(txt);
    correo = String(correo || '').trim().toLowerCase();
    if (correo && l.indexOf(correo) === -1) l.push(correo);
    return l.join(', ');
  }

  /** Lo que se le pide al servidor. El asunto y el saludo solo viajan si la persona los tocó: si no, el servidor los recalcula
      (el saludo depende de a quién va, el asunto de cuántos lotes son). */
  function armarPedido(f, tocado) {
    tocado = tocado || {};
    var p = {
      ids: f.ids, para: f.para, cc: f.cc, intro: f.intro, cierre: f.cierre, con_total: !!f.conTotal,
      lineas: (f.lineas || []).map(function (l) { return { id: l.id, etiqueta: l.etiqueta }; })
    };
    if (tocado.asunto) p.asunto = f.asunto;
    if (tocado.saludo) p.saludo = f.saludo;
    return p;
  }

  var API = { clp: clp, resumen: resumen, alternarTodo: alternarTodo, partirCorreos: partirCorreos,
              agregarCorreo: agregarCorreo, armarPedido: armarPedido };
  if (typeof module !== 'undefined' && module.exports) module.exports = API;
  root.FacprovEnvio = API;

  // ───────────────────────── pantalla ─────────────────────────
  function $(id) { return document.getElementById(id); }
  function toast(m, t) { if (typeof ilusToast === 'function') ilusToast(m, { type: t || 'info' }); }

  function iniciar() {
    var tabla = $('fpvTabla'), barra = $('fpvEnvBar'), modalEl = $('fpeModal');
    if (!tabla || !barra || !modalEl) return;
    var modal = null;
    var estado = { tocado: { asunto: false, saludo: false }, seq: 0, envio: { puede: true, motivo: '' }, hayRechazados: false, ocupado: false };
    var timer = null;

    function chks() { return Array.prototype.slice.call(tabla.querySelectorAll('.fpv-env-chk')); }
    function filaDe(cb) { return cb.closest('tr.rm-row'); }
    function marcados() { return chks().filter(function (cb) { return cb.checked; }); }
    function idsMarcados() {
      return marcados().map(function (cb) { return Number(cb.getAttribute('data-fid')); }).sort(function (a, b) { return a - b; });
    }

    function pintarSeleccion() {
      chks().forEach(function (cb) {
        var tr = filaDe(cb), on = cb.checked;
        if (tr) tr.classList.toggle('fpv-env-sel', on);
        var lb = cb.closest('.fpv-env');
        if (lb) lb.classList.toggle('on', on);
      });
      var sel = marcados().map(function (cb) { var tr = filaDe(cb); return { monto: tr ? Number(tr.getAttribute('data-monto')) : 0 }; });
      var r = resumen(sel);
      $('fpvEnvN').textContent = String(r.n);
      $('fpvEnvNlbl').textContent = r.n === 1 ? 'lote' : 'lotes';
      $('fpvEnvMonto').textContent = clp(r.total);
      barra.classList.toggle('on', r.n > 0);
    }

    tabla.addEventListener('change', function (ev) {
      if (ev.target && ev.target.classList && ev.target.classList.contains('fpv-env-chk')) pintarSeleccion();
    });

    // REGLA #14: marcar/desmarcar los lotes de la página que se ve
    var bt = $('fpvEnvTodo');
    if (bt) bt.addEventListener('click', function () {
      var visibles = chks().filter(function (cb) { var tr = filaDe(cb); return tr && !tr.hidden; });
      if (!visibles.length) { toast('No hay lotes a la vista', 'warning'); return; }
      var accion = alternarTodo(visibles.map(function (cb) { return { marcado: cb.checked }; }));
      visibles.forEach(function (cb) { cb.checked = (accion === 'marcar'); });
      pintarSeleccion();
    });
    var bl = $('fpvEnvLimpiar');
    if (bl) bl.addEventListener('click', function () { chks().forEach(function (cb) { cb.checked = false; }); pintarSeleccion(); });

    // ── llamadas al servidor ──
    function url(nombre) { return modalEl.getAttribute('data-url-' + nombre); }
    function post(nombre, cuerpo) {
      return fetch(url(nombre), { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(cuerpo) });
    }
    async function pedirJson(nombre, cuerpo) {
      try {
        var r = await post(nombre, cuerpo);
        var d = await r.json().catch(function () { return {}; });
        if (!r.ok && d.ok === undefined) d.ok = false;
        if (!r.ok && !d.error) d.error = 'El servidor respondió ' + r.status + '.';
        return d;
      } catch (e) {
        return { ok: false, error: 'Error de conexión. Intenta de nuevo.' };
      }
    }

    // ── el formulario ──
    function lineasDelForm() {
      return Array.prototype.slice.call($('fpeLotes').querySelectorAll('.fpe-lote')).map(function (el) {
        return { id: Number(el.getAttribute('data-id')), etiqueta: el.querySelector('.fpe-et').value };
      });
    }
    function leerForm() {
      return { ids: estado.ids, para: $('fpePara').value, cc: $('fpeCc').value, asunto: $('fpeAsunto').value, saludo: $('fpeSaludo').value,
               intro: $('fpeIntro').value, cierre: $('fpeCierre').value, conTotal: $('fpeConTotal').checked, lineas: lineasDelForm() };
    }

    function el(tag, clase, texto) {
      var e = document.createElement(tag);
      if (clase) e.className = clase;
      if (texto != null) e.textContent = texto;
      return e;
    }
    function aviso(clase, icono, texto) {
      var d = el('div', 'fpe-aviso ' + (clase || ''));
      d.appendChild(el('i', 'bi bi-' + icono));
      d.appendChild(el('span', '', texto));
      return d;
    }

    function pintarLotes(d) {
      var cont = $('fpeLotes');
      cont.textContent = '';
      d.lineas.forEach(function (ln) {
        var fila = el('div', 'fpe-lote');
        fila.setAttribute('data-id', ln.id);
        var a = el('div');
        a.appendChild(el('div', 'fpe-lote-nom', ln.proveedor));
        a.appendChild(el('div', 'fpe-lote-sub', 'Lote #' + ln.id));
        var b = el('div', 'fpe-lote-et');
        var lb = el('label', 'fpe-lbl', 'Cómo lo llamas en el correo');
        var inp = el('input', 'form-control fpe-et');
        inp.type = 'text'; inp.maxLength = 40; inp.value = ln.etiqueta; inp.autocomplete = 'off';
        inp.setAttribute('aria-label', 'Nombre de ' + ln.proveedor + ' en el correo');
        inp.addEventListener('input', programar);
        b.appendChild(lb); b.appendChild(inp);
        fila.appendChild(a); fila.appendChild(b); fila.appendChild(el('div', 'fpe-lote-monto', ln.monto_txt));
        cont.appendChild(fila);
      });
      $('fpeTotal').textContent = d.total_txt;
      $('fpeTotalFila').hidden = d.lineas.length < 2;
    }

    function pintarEstado(d) {
      estado.envio = d.envio || { puede: true, motivo: '' };
      estado.hayRechazados = !!(d.rechazados && d.rechazados.length);
      var av = $('fpeAvisos');
      av.textContent = '';
      if (!estado.envio.puede) {
        av.appendChild(aviso('', 'exclamation-triangle-fill', 'Hoy el correo no saldría: ' + estado.envio.motivo +
          '. Puedes descargar el borrador para Outlook y enviarlo desde ahí.'));
      }
      if (estado.hayRechazados) {
        av.appendChild(aviso('', 'exclamation-triangle-fill', 'No se puede incluir: ' + d.rechazados.slice(0, 4).join(', ') +
          '. Solo correos @sphs.cl o @ilusfitness.com de personas que no son técnicos.'));
      }
      (d.avisos || []).forEach(function (t) { av.appendChild(aviso('', 'info-circle-fill', t)); });

      // «Se enviará a…»: se dice ANTES de enviar, con nombre y apellido (REGLA #37)
      var q = $('fpeQuien');
      q.textContent = '';
      if (d.para.length) {
        var p1 = el('div'); p1.appendChild(el('b', '', 'Para: ')); p1.appendChild(document.createTextNode(d.para.join(', ')));
        q.appendChild(p1);
      } else {
        q.appendChild(el('div', '', 'Escribe a quién se lo envías.'));
      }
      if (d.cc.length) {
        var p2 = el('div'); p2.appendChild(el('b', '', 'Con copia: ')); p2.appendChild(document.createTextNode(d.cc.join(', ')));
        q.appendChild(p2);
      }
      if (d.remitente && d.remitente.correo) {
        var p3 = el('div', 'fpe-help', 'Si responden, la respuesta te llega a ' + d.remitente.correo + '.');
        q.appendChild(p3);
      }
      var adj = $('fpeAdj');
      adj.textContent = '';
      d.lineas.forEach(function (ln) {
        var s = el('span');
        s.appendChild(el('i', 'bi bi-file-earmark-excel'));
        s.appendChild(document.createTextNode(ln.archivo));
        adj.appendChild(s);
      });
      $('fpePrev').innerHTML = d.cuerpo_html;        // lo arma el servidor y escapa todo lo que escribe una persona
      $('fpePrev').classList.remove('cargando');
      botones();
    }

    function botones() {
      var puedeEnviar = estado.envio.puede && !estado.hayRechazados && !estado.ocupado && $('fpePara').value.trim() !== '';
      $('fpeEnviar').disabled = !puedeEnviar;
      $('fpeBorrador').disabled = estado.hayRechazados || estado.ocupado;
    }

    function llenarSugeridos(d) {
      ['fpeParaAdd', 'fpeCcAdd'].forEach(function (id) {
        var s = $(id);
        s.textContent = '';
        var o0 = el('option', '', '+ Agregar una persona…'); o0.value = '';
        s.appendChild(o0);
        (d.sugeridos || []).forEach(function (p) {
          var o = el('option', '', (p.nombre ? p.nombre + ' · ' : '') + p.correo);
          o.value = p.correo;
          s.appendChild(o);
        });
      });
    }

    function programar() {
      $('fpePrev').classList.add('cargando');
      clearTimeout(timer);
      timer = setTimeout(refrescar, 350);
    }
    async function refrescar() {
      var mi = ++estado.seq;
      var d = await pedirJson('previa', armarPedido(leerForm(), estado.tocado));
      if (mi !== estado.seq) return;                  // llegó una respuesta más nueva
      if (!d.ok) { $('fpePrev').classList.remove('cargando'); toast(d.error || 'No se pudo actualizar la vista previa', 'error'); return; }
      if (!estado.tocado.saludo) $('fpeSaludo').value = d.saludo;
      if (!estado.tocado.asunto) $('fpeAsunto').value = d.asunto;
      $('fpeTotal').textContent = d.total_txt;
      pintarEstado(d);
    }

    // ── abrir ──
    var abrirBtn = $('fpvEnvAbrir');
    if (abrirBtn) abrirBtn.addEventListener('click', async function () {
      var ids = idsMarcados();
      if (!ids.length) { toast('Marca al menos un lote', 'warning'); return; }
      abrirBtn.disabled = true;
      var d = await pedirJson('previa', { ids: ids });
      abrirBtn.disabled = false;
      if (!d.ok) {
        if (typeof ilusAlert === 'function') ilusAlert({ title: 'No se pudo armar el correo', message: d.error || 'Intenta de nuevo.', type: 'warning' });
        else toast(d.error, 'error');
        return;
      }
      estado.ids = ids;
      estado.tocado = { asunto: false, saludo: false };
      estado.ocupado = false;
      pintarLotes(d);
      llenarSugeridos(d);
      $('fpePara').value = d.para.join(', ');
      $('fpeCc').value = d.cc.join(', ');
      $('fpeAsunto').value = d.asunto;
      $('fpeSaludo').value = d.saludo;
      $('fpeIntro').value = d.intro;
      $('fpeCierre').value = d.cierre;
      $('fpeConTotal').checked = !!d.con_total;
      pintarEstado(d);
      if (!modal) modal = bootstrap.Modal.getOrCreateInstance(modalEl);
      modal.show();
    });

    ['fpePara', 'fpeCc', 'fpeIntro', 'fpeCierre'].forEach(function (id) { $(id).addEventListener('input', programar); });
    $('fpeConTotal').addEventListener('change', programar);
    $('fpeAsunto').addEventListener('input', function () { estado.tocado.asunto = true; programar(); });
    $('fpeSaludo').addEventListener('input', function () { estado.tocado.saludo = true; programar(); });
    [['fpeParaAdd', 'fpePara'], ['fpeCcAdd', 'fpeCc']].forEach(function (par) {
      $(par[0]).addEventListener('change', function () {
        if (!this.value) return;
        $(par[1]).value = agregarCorreo($(par[1]).value, this.value);
        this.value = '';
        programar();
      });
    });

    function ocupar(si, boton, texto) {
      estado.ocupado = si;
      if (boton) {
        if (si) { boton.setAttribute('data-txt', boton.innerHTML); boton.innerHTML = '<span class="spinner-border spinner-border-sm me-2"></span>' + texto; }
        else if (boton.getAttribute('data-txt')) { boton.innerHTML = boton.getAttribute('data-txt'); }
      }
      botones();
    }

    // ── enviar ──
    $('fpeEnviar').addEventListener('click', async function () {
      var boton = this;
      var pedido = armarPedido(leerForm(), estado.tocado);
      var para = partirCorreos($('fpePara').value), cc = partirCorreos($('fpeCc').value);
      var n = estado.ids.length;
      var ok = await ilusConfirm({
        title: 'Enviar el cierre de ' + n + (n === 1 ? ' lote' : ' lotes'),
        message: 'Se enviará un correo con ' + n + (n === 1 ? ' Excel adjunto' : ' Excel adjuntos') + ' y los montos de cada proveedor a:',
        sub: 'Para: ' + para.join(', ') + (cc.length ? ' · Con copia: ' + cc.join(', ') : ''),
        okLabel: 'Enviar ahora', cancelLabel: 'Revisar', type: 'question'
      });
      if (!ok) return;
      ocupar(true, boton, 'Enviando…');
      var d = await pedirJson('enviar', pedido);
      ocupar(false, boton);
      if (!d.ok) {
        ilusAlert({ title: 'No salió el correo', message: d.error || 'Intenta de nuevo.', type: 'error' });
        return;
      }
      toast('✓ Cierre enviado a ' + d.enviados.join(', '), 'success');
      if (modal) modal.hide();
      chks().forEach(function (cb) { cb.checked = false; });
      pintarSeleccion();
    });

    // ── borrador para Outlook ──
    $('fpeBorrador').addEventListener('click', async function () {
      var boton = this;
      ocupar(true, boton, 'Armando…');
      try {
        var r = await post('borrador', armarPedido(leerForm(), estado.tocado));
        var tipo = r.headers.get('content-type') || '';
        if (!r.ok || tipo.indexOf('json') !== -1) {
          var d = await r.json().catch(function () { return {}; });
          ilusAlert({ title: 'No se pudo armar el borrador', message: d.error || 'Intenta de nuevo.', type: 'error' });
        } else {
          var blob = await r.blob();
          var disp = r.headers.get('content-disposition') || '';
          var m = /filename\*?=(?:UTF-8'')?"?([^";]+)"?/i.exec(disp);
          var a = document.createElement('a');
          a.href = URL.createObjectURL(blob);
          a.download = m ? decodeURIComponent(m[1]) : 'cierre_proveedores.eml';
          document.body.appendChild(a); a.click(); a.remove();
          setTimeout(function () { URL.revokeObjectURL(a.href); }, 4000);
          toast('Borrador descargado: ábrelo con Outlook y revísalo antes de enviar', 'success');
        }
      } catch (e) {
        toast('Error de conexión. Intenta de nuevo.', 'error');
      }
      ocupar(false, boton);
    });

    pintarSeleccion();
  }

  if (typeof document !== 'undefined') {
    if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', iniciar);
    else iniciar();
  }
})(typeof window !== 'undefined' ? window : (typeof globalThis !== 'undefined' ? globalThis : this));

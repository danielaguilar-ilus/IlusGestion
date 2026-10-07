/* ILUS Fitness · Editor de preguntas de la encuesta de Retiros.
   Usa ilusConfirm / ilusToast (REGLA #1). Todo texto del usuario se inserta con textContent (nunca innerHTML).
   El fetch global de base.html agrega el header X-CSRF-Token. */
(function () {
  'use strict';
  var cfg = JSON.parse(document.getElementById('encConfig').textContent);
  var $ = function (id) { return document.getElementById(id); };
  var estado = { preguntas: [], editando: null };

  function toast(msg, tipo) {
    if (typeof window.ilusToast === 'function') window.ilusToast(msg, { type: tipo || 'info' });
  }
  function confirmar(op) {
    if (typeof window.ilusConfirm === 'function') return window.ilusConfirm(op);
    return Promise.resolve(true);
  }
  function el(tag, cls, txt) {
    var e = document.createElement(tag);
    if (cls) e.className = cls;
    if (txt !== undefined && txt !== null) e.textContent = txt;
    return e;
  }
  function api(url, cuerpo) {
    var op = cuerpo === undefined ? { method: 'GET' } :
      { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(cuerpo) };
    return fetch(url, op).then(function (r) {
      return r.json().catch(function () { return { ok: false, error: 'Respuesta no válida.' }; })
        .then(function (j) { j._estado = r.status; return j; });
    });
  }
  function refrescarPrevia() {
    var f = $('encPrev');
    if (f) { try { f.contentWindow.location.reload(); } catch (_) { f.src = cfg.previa; } }
  }

  // ── lista ─────────────────────────────────────────────────────────
  function cargar() {
    return api(cfg.datos).then(function (j) {
      if (!j.ok) { $('encLista').textContent = j.error || 'No se pudieron cargar las preguntas.'; return; }
      estado.preguntas = j.preguntas;
      pintar(j);
    });
  }

  function chip(txt, cls) { return el('span', 'ence-chip ' + (cls || ''), txt); }

  function pintar(j) {
    var cont = $('encLista');
    cont.textContent = '';
    var aviso = $('encAviso');
    if (j.activas > cfg.max) {
      aviso.hidden = false;
      aviso.textContent = 'Hay ' + j.activas + ' preguntas activas. Una encuesta de más de ' + cfg.max + ' preguntas baja la cantidad de clientes que la contestan.';
    } else { aviso.hidden = true; }

    var activas = j.preguntas.filter(function (p) { return p.activa; });
    var archivadas = j.preguntas.filter(function (p) { return !p.activa; });
    cont.appendChild(el('h6', 'ence-sub', 'Preguntas activas (' + activas.length + ')'));
    if (!activas.length) cont.appendChild(el('p', 'ence-nota', 'No hay preguntas activas: la encuesta no tendría nada que mostrar.'));
    activas.forEach(function (p, i) { cont.appendChild(tarjeta(p, i, activas.length)); });
    if (archivadas.length) {
      cont.appendChild(el('h6', 'ence-sub', 'Archivadas (' + archivadas.length + ')'));
      archivadas.forEach(function (p) { cont.appendChild(tarjeta(p, -1, 0)); });
    }
  }

  function tarjeta(p, idx, total) {
    var c = el('article', 'ence-card ence-preg' + (p.activa ? '' : ' is-arch'));
    var num = el('div', 'ence-num', p.activa ? String(idx + 1) : '–');
    var cuerpo = el('div', 'ence-preg-cuerpo');
    cuerpo.appendChild(el('div', 'ence-preg-texto', p.texto));
    if (p.ayuda) cuerpo.appendChild(el('div', 'ence-preg-ayuda', p.ayuda));
    var chips = el('div', 'ence-chips');
    chips.appendChild(chip(cfg.tipos[p.tipo] || p.tipo));
    chips.appendChild(chip(p.obligatoria ? 'Obligatoria' : 'Opcional', p.obligatoria ? 'is-obl' : ''));
    chips.appendChild(chip('Versión ' + p.version));
    chips.appendChild(chip(p.respuestas + ' respuesta' + (p.respuestas === 1 ? '' : 's')));
    cuerpo.appendChild(chips);
    if (p.tipo === 'opcion' && p.opciones.length) {
      var ops = el('ul', 'ence-ops');
      p.opciones.forEach(function (o) {
        var li = el('li', 'ence-op ence-op-' + o.color);
        li.appendChild(el('span', 'ence-dot'));
        li.appendChild(document.createTextNode(o.etiqueta));
        ops.appendChild(li);
      });
      cuerpo.appendChild(ops);
    }
    var acc = el('div', 'ence-acc');
    if (p.activa) {
      var sube = el('button', 'btn btn-sm btn-outline-dark', '▲'); sube.type = 'button';
      sube.setAttribute('aria-label', 'Subir pregunta'); sube.disabled = idx === 0;
      sube.onclick = function () { mover(p, 'arriba'); };
      var baja = el('button', 'btn btn-sm btn-outline-dark', '▼'); baja.type = 'button';
      baja.setAttribute('aria-label', 'Bajar pregunta'); baja.disabled = idx === total - 1;
      baja.onclick = function () { mover(p, 'abajo'); };
      acc.appendChild(sube); acc.appendChild(baja);
    }
    var ed = el('button', 'btn btn-sm btn-outline-dark fw-bold', 'Editar'); ed.type = 'button';
    ed.onclick = function () { abrirForm(p); };
    var ar = el('button', 'btn btn-sm ' + (p.activa ? 'btn-outline-danger' : 'btn-outline-success') + ' fw-bold', p.activa ? 'Archivar' : 'Activar');
    ar.type = 'button';
    ar.onclick = function () { alternar(p); };
    acc.appendChild(ed); acc.appendChild(ar);
    c.appendChild(num); c.appendChild(cuerpo); c.appendChild(acc);
    return c;
  }

  function mover(p, dir) {
    api(cfg.base + '/' + p.id + '/mover', { dir: dir }).then(function (j) {
      if (!j.ok) { toast(j.error || 'No se pudo mover.', 'error'); return; }
      cargar().then(refrescarPrevia);
    });
  }

  function alternar(p) {
    var activar = !p.activa;
    var ir = function () {
      api(cfg.base + '/' + p.id + '/activa', { activa: activar }).then(function (j) {
        if (!j.ok) { toast(j.error || 'No se pudo cambiar.', 'error'); return; }
        toast(activar ? 'Pregunta activada.' : 'Pregunta archivada.', 'success');
        cargar().then(refrescarPrevia);
      });
    };
    if (activar) { ir(); return; }
    confirmar({
      title: 'Archivar pregunta',
      message: '¿Dejar de mostrar esta pregunta a los clientes?',
      sub: 'No se borra nada: sus respuestas quedan guardadas y puedes volver a activarla.',
      okLabel: 'Archivar', cancelLabel: 'Cancelar', danger: true
    }).then(function (ok) { if (ok) ir(); });
  }

  // ── formulario ────────────────────────────────────────────────────
  function filaOpcion(o) {
    var f = el('div', 'ence-opfila');
    var t = el('input'); t.type = 'text'; t.maxLength = 120; t.placeholder = 'Texto de la opción';
    t.value = o.etiqueta || ''; t.setAttribute('aria-label', 'Texto de la opción');
    t.dataset.valor = o.valor || '';
    var s = el('select'); s.setAttribute('aria-label', 'Color del semáforo');
    cfg.colores.forEach(function (col) {
      var op = el('option', null, { verde: 'Verde (bueno)', ambar: 'Ámbar (regular)', rojo: 'Rojo (malo)', gris: 'Gris (neutro)' }[col]);
      op.value = col; if (col === (o.color || 'gris')) op.selected = true; s.appendChild(op);
    });
    var x = el('button', 'btn btn-sm btn-outline-danger', '✕'); x.type = 'button';
    x.setAttribute('aria-label', 'Quitar opción');
    x.onclick = function () { f.remove(); };
    f.appendChild(t); f.appendChild(s); f.appendChild(x);
    return f;
  }

  function actualizarTipo() {
    $('bloqueOpciones').hidden = $('fTipo').value !== 'opcion';
    if ($('fTipo').value === 'opcion' && !$('listaOpciones').children.length) {
      $('listaOpciones').appendChild(filaOpcion({ color: 'verde' }));
      $('listaOpciones').appendChild(filaOpcion({ color: 'rojo' }));
    }
  }

  function abrirForm(p) {
    estado.editando = p || null;
    $('encFormTitulo').textContent = p ? 'Editar pregunta' : 'Nueva pregunta';
    $('fId').value = p ? p.id : '';
    $('fTexto').value = p ? p.texto : '';
    $('fAyuda').value = p ? p.ayuda : '';
    $('fTipo').value = p ? p.tipo : 'estrellas';
    $('fObl').checked = p ? p.obligatoria : true;
    $('listaOpciones').textContent = '';
    (p && p.tipo === 'opcion' ? p.opciones : []).forEach(function (o) { $('listaOpciones').appendChild(filaOpcion(o)); });
    ['eTexto', 'eTipo', 'eOpciones'].forEach(function (i) { $(i).textContent = ''; });
    actualizarTipo();
    $('encForm').hidden = false;
    $('encForm').scrollIntoView({ behavior: 'smooth', block: 'start' });
    $('fTexto').focus({ preventScroll: true });
  }
  function cerrarForm() { $('encForm').hidden = true; estado.editando = null; }

  function recolectar() {
    var ops = [];
    Array.prototype.forEach.call($('listaOpciones').children, function (f) {
      var t = f.querySelector('input'); var s = f.querySelector('select');
      if (t.value.trim()) ops.push({ valor: t.dataset.valor || '', etiqueta: t.value.trim(), color: s.value });
    });
    return {
      id: $('fId').value ? parseInt($('fId').value, 10) : null,
      texto: $('fTexto').value, ayuda: $('fAyuda').value, tipo: $('fTipo').value,
      obligatoria: $('fObl').checked, opciones: $('fTipo').value === 'opcion' ? ops : []
    };
  }

  function guardar() {
    var cuerpo = recolectar();
    var p = estado.editando;
    var ir = function () {
      api(cfg.guardar, cuerpo).then(function (j) {
        ['eTexto', 'eTipo', 'eOpciones'].forEach(function (i) { $(i).textContent = ''; });
        if (!j.ok) {
          if (j.errores) {
            $('eTexto').textContent = j.errores.texto || '';
            $('eTipo').textContent = j.errores.tipo || '';
            $('eOpciones').textContent = j.errores.opciones || '';
          }
          toast(j.error || 'Revisa los campos marcados.', 'error');
          return;
        }
        var msg = { creada: 'Pregunta creada.', editada: 'Pregunta guardada.', sin_cambios: 'No había cambios.',
                    nueva_version: 'Guardada como versión nueva (las respuestas anteriores conservan el texto anterior).' }[j.modo] || 'Guardado.';
        toast(msg, 'success');
        cerrarForm();
        cargar().then(refrescarPrevia);
      });
    };
    if (p && p.respuestas > 0) {
      confirmar({
        title: 'Guardar como versión nueva',
        message: 'Esta pregunta ya tiene ' + p.respuestas + ' respuesta' + (p.respuestas === 1 ? '' : 's') + '.',
        sub: 'Se creará la versión ' + (p.version + 1) + '. Las respuestas anteriores seguirán asociadas al texto que vio el cliente.',
        okLabel: 'Guardar versión nueva', cancelLabel: 'Cancelar'
      }).then(function (ok) { if (ok) ir(); });
    } else { ir(); }
  }

  $('encNueva').onclick = function () { abrirForm(null); };
  $('encCancelar').onclick = cerrarForm;
  $('encGuardar').onclick = guardar;
  $('fTipo').onchange = actualizarTipo;
  $('encAddOpcion').onclick = function () {
    if ($('listaOpciones').children.length >= 6) { toast('Máximo 6 opciones.', 'warning'); return; }
    $('listaOpciones').appendChild(filaOpcion({ color: 'gris' }));
  };
  cargar();
})();

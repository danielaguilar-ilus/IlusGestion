/* ══════════════════════════════════════════════════════════════════════
   OT 2.0 · MOTOR DEL FORMULARIO DE GENERAR OT   (namespace O2F)
   ══════════════════════════════════════════════════════════════════════
   Da vida a templates/ot2/_modal_ot_form.html — la réplica independiente
   del modal viejo. Escribe contra POST /ot/api/crear, el motor propio de
   OT 2.0, y NO comparte una sola línea con _TKOT (el JS del modal de
   Tickets): si mañana se borra aquel, esto sigue funcionando.

   Qué hace:
     · Carga el CLIENTE y SUS EQUIPOS según el origen elegido.
     · Pinta los equipos con foto, marca, modelo, N° de serie y estado.
     · Carga técnicos y plantillas de checklist reales.
     · Pinta el semáforo de pasos (rojo → verde al completarse).
     · Valida lo mismo que el backend, para avisar antes de mandar.
     · Crea la OT y lleva a su ficha.

   El CSRF lo inyecta el wrapper global de base.html en todo fetch().
   ══════════════════════════════════════════════════════════════════════ */
window.O2F = (function () {
  'use strict';

  var S = null;              // estado del formulario
  var cachePlant = [];       // plantillas de checklist (se piden una vez)
  var cacheTec = [];

  function $(id) { return document.getElementById(id); }
  function esc(s) {
    return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
    });
  }
  function avisar(msg, tipo) {
    if (window.ilusToast) ilusToast(msg, { type: tipo || 'warning' });
  }

  /* ── Arranque ─────────────────────────────────────────────────────── */
  function iniciar(opts) {
    opts = opts || {};
    S = {
      origen: opts.origen || 'cliente',
      cliente: opts.cliente || null,     // {id, nombre}
      ticket_id: opts.ticket_id || null,
      equipos: [],                       // [{maquina_id, plantilla_id}]
      tecnicos: [], lider: null
    };

    // Un trabajo interno no lleva cliente ni datos de acceso al sitio.
    var interno = (S.origen === 'interno');
    ocultarPaso('o2fStep2_cliente', false);
    var av = $('o2fAvisoSinFicha');
    if (av) av.style.display = 'none';

    // Tipo: en trabajo interno solo se ofrecen los tipos internos.
    var sel = $('o2f_otTipo');
    if (sel && interno) {
      var permitidos = ['revision_interna', 'capacitacion', 'control_calidad'];
      Array.prototype.forEach.call(sel.options, function (o) {
        o.hidden = permitidos.indexOf(o.value) < 0;
      });
      sel.value = 'revision_interna';
    } else if (sel) {
      Array.prototype.forEach.call(sel.options, function (o) { o.hidden = false; });
      // Garantía y repuesto salieron de la oferta por decisión de Daniel
      // (19-08): la garantía es un atributo, no un tipo de trabajo.
      ['garantia', 'repuesto'].forEach(function (v) {
        var o = sel.querySelector('option[value="' + v + '"]');
        if (o) o.hidden = true;
      });
    }

    // Título por defecto y fecha de hoy, para que nada nazca vacío.
    var hoy = new Date();
    var iso = hoy.getFullYear() + '-' +
      String(hoy.getMonth() + 1).padStart(2, '0') + '-' +
      String(hoy.getDate()).padStart(2, '0');
    if ($('o2fLevFechaProg') && !$('o2fLevFechaProg').value) $('o2fLevFechaProg').value = iso;
    if ($('o2fLevHoraIni') && !$('o2fLevHoraIni').value) $('o2fLevHoraIni').value = '09:00';
    if ($('o2fLevHoraFin') && !$('o2fLevHoraFin').value) $('o2fLevHoraFin').value = '13:00';

    cargarTecnicos();
    cargarPlantillas().then(function () {
      if (S.cliente) cargarEquipos();
      else pintarEquiposVacio(interno
        ? 'El trabajo interno no lleva equipos del cliente.'
        : 'Elige primero un cliente para ver sus equipos.');
    });

    pintarCliente();
    engancharEventos();
    refrescarPasos();
  }

  function ocultarPaso(id, ocultar) {
    var el = $(id);
    if (el) el.style.display = ocultar ? 'none' : '';
  }

  /* ── Cliente ──────────────────────────────────────────────────────── */
  function pintarCliente() {
    var t = $('o2fLevSelectTitulo');
    if (t && !t.value) {
      var tipo = $('o2f_otTipo') ? $('o2f_otTipo').selectedOptions[0].textContent.trim() : 'Orden';
      var f = $('o2fLevFechaProg') ? $('o2fLevFechaProg').value : '';
      var partes = f ? f.split('-') : [];
      t.value = tipo + (partes.length === 3 ? ' ' + partes[2] + '-' + partes[1] + '-' + partes[0] : '');
    }
    var sub = $('o2fSubtitulo');
    if (sub && S.cliente) sub.textContent = 'Cliente: ' + S.cliente.nombre;
  }

  /* ── Equipos del cliente, con toda su información ─────────────────── */
  function cargarEquipos() {
    fetch('/mantenciones/api/clientes/' + S.cliente.id + '/maquinas-list',
          { credentials: 'same-origin' })
      .then(function (r) { return r.json(); })
      .then(function (j) {
        var arr = Array.isArray(j) ? j : (j.maquinas || j.items || []);
        pintarEquipos(arr);
      })
      .catch(function () {
        pintarEquiposVacio('No se pudieron cargar los equipos de este cliente.');
      });
  }

  function pintarEquiposVacio(msg) {
    var tb = $('o2fLevSelectTbody');
    if (tb) tb.innerHTML = '<tr><td colspan="3" class="text-muted small p-3">' + esc(msg) + '</td></tr>';
    var c = $('o2fLevEqCount'); if (c) c.textContent = '0';
  }

  function pintarEquipos(arr) {
    var tb = $('o2fLevSelectTbody');
    if (!tb) return;
    if (!arr.length) {
      return pintarEquiposVacio('Este cliente no tiene equipos en su ficha.');
    }
    var ops = cachePlant.map(function (p) {
      return '<option value="' + p.id + '">' + esc(p.nombre) + '</option>';
    }).join('');

    tb.innerHTML = arr.map(function (m) {
      var sel = S.equipos.find(function (e) { return e.maquina_id === m.id; });
      var foto = m.foto_url || '';
      // Ficha del equipo: lo que el usuario necesita para reconocerlo.
      var meta = [];
      if (m.marca) meta.push(esc(m.marca));
      if (m.modelo) meta.push(esc(m.modelo));
      var serie = m.numero_serie || m.serie || '';
      var estado = m.estado || '';
      return '' +
        '<tr data-mid="' + m.id + '">' +
          '<td class="align-middle">' +
            '<input type="checkbox" class="form-check-input o2f-eq-chk" ' +
                   (sel ? 'checked' : '') + ' data-mid="' + m.id + '">' +
          '</td>' +
          '<td>' +
            '<div class="d-flex align-items-center gap-2">' +
              (foto
                ? '<img src="' + esc(foto) + '" alt="" style="width:44px;height:44px;' +
                  'object-fit:cover;border-radius:7px;border:1px solid #e5e7eb;flex:none">'
                : '<span style="width:44px;height:44px;border-radius:7px;background:#f3f4f6;' +
                  'display:grid;place-items:center;color:#c7cbd1;flex:none">' +
                  '<i class="bi bi-image"></i></span>') +
              '<div style="min-width:0">' +
                '<div class="fw-bold" style="font-size:.84rem">' +
                  esc(m.nombre || m.modelo || ('Equipo ' + m.id)) + '</div>' +
                '<div class="text-muted" style="font-size:.72rem">' +
                  (meta.length ? meta.join(' · ') : 'Sin marca ni modelo') +
                  (m.sku ? ' · SKU ' + esc(m.sku) : '') + '</div>' +
                '<div style="font-size:.7rem;margin-top:2px">' +
                  (serie
                    ? '<span class="badge bg-light text-dark border">N° ' + esc(serie) + '</span>'
                    : '<span class="badge bg-warning-subtle text-warning-emphasis border">Sin N° de serie</span>') +
                  (estado ? ' <span class="badge bg-light text-dark border">' + esc(estado) + '</span>' : '') +
                '</div>' +
              '</div>' +
            '</div>' +
          '</td>' +
          '<td>' +
            '<select class="form-select form-select-sm o2f-eq-plant" data-mid="' + m.id + '">' +
              '<option value="">Elige el checklist…</option>' + ops +
            '</select>' +
          '</td>' +
        '</tr>';
    }).join('');

    // Restaurar lo ya elegido
    S.equipos.forEach(function (e) {
      var s = tb.querySelector('.o2f-eq-plant[data-mid="' + e.maquina_id + '"]');
      if (s && e.plantilla_id) s.value = e.plantilla_id;
    });

    tb.querySelectorAll('.o2f-eq-chk').forEach(function (chk) {
      chk.addEventListener('change', function () {
        var mid = parseInt(this.dataset.mid);
        var i = S.equipos.findIndex(function (e) { return e.maquina_id === mid; });
        if (this.checked && i < 0) S.equipos.push({ maquina_id: mid, plantilla_id: null });
        if (!this.checked && i >= 0) S.equipos.splice(i, 1);
        refrescarPasos();
      });
    });
    tb.querySelectorAll('.o2f-eq-plant').forEach(function (s) {
      s.addEventListener('change', function () {
        var mid = parseInt(this.dataset.mid);
        var e = S.equipos.find(function (x) { return x.maquina_id === mid; });
        if (!e) {  // elegir checklist marca el equipo, es lo natural
          e = { maquina_id: mid, plantilla_id: null };
          S.equipos.push(e);
          var chk = tb.querySelector('.o2f-eq-chk[data-mid="' + mid + '"]');
          if (chk) chk.checked = true;
        }
        e.plantilla_id = this.value ? parseInt(this.value) : null;
        refrescarPasos();
      });
    });
    refrescarPasos();
  }

  /* ── Técnicos ─────────────────────────────────────────────────────── */
  function cargarTecnicos() {
    fetch('/mantenciones/api/tecnicos', { credentials: 'same-origin' })
      .then(function (r) { return r.json(); })
      .then(function (j) {
        cacheTec = Array.isArray(j) ? j : (j.tecnicos || []);
        var box = $('o2fLevTecnicosBox');
        if (!box) return;
        if (!cacheTec.length) {
          box.innerHTML = '<span class="text-muted small">No hay técnicos cargados.</span>';
          return;
        }
        box.innerHTML = cacheTec.map(function (t) {
          return '<button type="button" class="btn btn-sm btn-outline-secondary o2f-tec" ' +
                 'data-tid="' + t.id + '" style="min-height:40px">' +
                 '<i class="bi bi-person me-1"></i>' + esc(t.nombre) + '</button>';
        }).join('');
        box.querySelectorAll('.o2f-tec').forEach(function (b) {
          b.addEventListener('click', function () {
            var tid = parseInt(this.dataset.tid);
            var i = S.tecnicos.indexOf(tid);
            if (i >= 0) {
              S.tecnicos.splice(i, 1);
              this.classList.remove('btn-danger'); this.classList.add('btn-outline-secondary');
              if (S.lider === tid) S.lider = S.tecnicos[0] || null;
            } else {
              S.tecnicos.push(tid);
              this.classList.add('btn-danger'); this.classList.remove('btn-outline-secondary');
              if (!S.lider) S.lider = tid;
            }
            var c = $('o2fLevTecCount'); if (c) c.textContent = S.tecnicos.length;
            refrescarPasos();
          });
        });
      })
      .catch(function () {
        var box = $('o2fLevTecnicosBox');
        if (box) box.innerHTML = '<span class="text-danger small">No se pudo cargar la lista de técnicos.</span>';
      });
  }

  function cargarPlantillas() {
    if (cachePlant.length) return Promise.resolve();
    return fetch('/mantenciones/api/plantillas', { credentials: 'same-origin' })
      .then(function (r) { return r.json(); })
      .then(function (j) {
        cachePlant = (j.plantillas || []).filter(function (p) { return p.activa; });
      })
      .catch(function () { cachePlant = []; });
  }

  /* ── Semáforo de pasos ────────────────────────────────────────────── */
  function pasoCompleto(n) {
    var interno = (S.origen === 'interno');
    switch (n) {
      case 1: return !!($('o2f_otTipo') && $('o2f_otTipo').value);
      case 2: return !!($('o2fLevSelectTitulo') && $('o2fLevSelectTitulo').value.trim());
      case 3: return S.tecnicos.length > 0;
      case 4: return !!($('o2fLevFechaProg') && $('o2fLevFechaProg').value);
      case 5: return interno
        ? true
        : (S.equipos.length > 0 && S.equipos.every(function (e) { return !!e.plantilla_id; }));
      case 6: return interno || (
        $('o2f_acceso_ascensor') && $('o2f_acceso_ascensor').value !== '' &&
        $('o2f_acceso_estacionamiento') && $('o2f_acceso_estacionamiento').value !== '');
      default: return false;
    }
  }

  function refrescarPasos() {
    for (var n = 1; n <= 7; n++) {
      var card = $('o2fStep' + n);
      if (!card) continue;
      if (n >= 7) continue;                      // el 7 es opcional
      card.classList.toggle('is-complete', pasoCompleto(n));
    }
    var btn = $('o2fBtnCrear');
    if (btn) {
      var listo = pasoCompleto(1) && pasoCompleto(2) && pasoCompleto(3) &&
                  pasoCompleto(4) && pasoCompleto(5) && pasoCompleto(6);
      btn.disabled = !listo;
      btn.title = listo ? '' : 'Faltan pasos por completar';
    }
    avisosAgenda();
  }

  /* Jornada real: 08:00–17:00 con colación 13:00–14:00. No bloquea, avisa. */
  function avisosAgenda() {
    var cont = $('o2fLevCalWarnChoque');
    if (!cont) return;
    var hi = $('o2fLevHoraIni') ? $('o2fLevHoraIni').value : '';
    var hf = $('o2fLevHoraFin') ? $('o2fLevHoraFin').value : '';
    var fe = $('o2fLevFechaProg') ? $('o2fLevFechaProg').value : '';
    var a = [];
    if (hi && hi < '08:00') a.push('Empieza antes de las 08:00, fuera de la jornada.');
    if (hf && hf > '17:00') a.push('Termina después de las 17:00: es jornada extendida.');
    if (hi && hf && hi < '14:00' && hf > '13:00') a.push('Cruza la colación (13:00–14:00).');
    if (fe) {
      var d = new Date(fe + 'T12:00:00').getDay();
      if (d === 0 || d === 6) a.push('Cae en fin de semana, cuando no hay operación.');
    }
    if (!a.length) { cont.style.display = 'none'; return; }
    cont.style.display = '';
    cont.innerHTML = '<div class="alert alert-warning py-2 mb-0" style="font-size:.8rem">' +
      '<i class="bi bi-exclamation-triangle-fill me-1"></i>Se puede agendar igual, pero ojo:' +
      '<ul class="mb-0 mt-1">' + a.map(function (x) { return '<li>' + esc(x) + '</li>'; }).join('') +
      '</ul></div>';
  }

  function engancharEventos() {
    ['o2f_otTipo', 'o2fLevSelectTitulo', 'o2fLevFechaProg', 'o2fLevHoraIni',
     'o2fLevHoraFin', 'o2f_acceso_ascensor', 'o2f_acceso_estacionamiento'
    ].forEach(function (id) {
      var el = $(id);
      if (el && !el.dataset.o2fBound) {
        el.dataset.o2fBound = '1';
        el.addEventListener('change', refrescarPasos);
        el.addEventListener('input', refrescarPasos);
      }
    });
  }

  /* ── Crear ────────────────────────────────────────────────────────── */
  function crear() {
    var interno = (S.origen === 'interno');
    var body = {
      tipo_ot: $('o2f_otTipo') ? $('o2f_otTipo').value : '',
      cliente_id: interno ? null : (S.cliente ? S.cliente.id : null),
      titulo: $('o2fLevSelectTitulo') ? $('o2fLevSelectTitulo').value : '',
      descripcion: $('o2fLevSelectNotas') ? $('o2fLevSelectNotas').value : '',
      fecha_programada: $('o2fLevFechaProg') ? $('o2fLevFechaProg').value : '',
      hora_inicio: $('o2fLevHoraIni') ? $('o2fLevHoraIni').value : '',
      hora_fin: $('o2fLevHoraFin') ? $('o2fLevHoraFin').value : '',
      tecnico_user_ids: S.tecnicos,
      tecnico_lider_id: S.lider,
      equipos: interno ? [] : S.equipos,
      plantilla_id: interno ? (cachePlant[0] && cachePlant[0].id) : null,
      ticket_id: S.ticket_id,
      acceso: interno ? {} : {
        ascensor: $('o2f_acceso_ascensor') ? $('o2f_acceso_ascensor').value === '1' : null,
        estacionamiento: $('o2f_acceso_estacionamiento') ? $('o2f_acceso_estacionamiento').value === '1' : null,
        piso: $('o2f_acceso_piso') ? $('o2f_acceso_piso').value : '',
        notas: $('o2f_acceso_notas') ? $('o2f_acceso_notas').value : ''
      }
    };

    var btn = $('o2fBtnCrear');
    if (btn) { btn.disabled = true; btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Creando…'; }

    fetch('/ot/api/crear', {
      method: 'POST', credentials: 'same-origin',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body)
    })
      .then(function (r) { return r.json(); })
      .then(function (j) {
        if (!j.ok) {
          if (btn) { btn.disabled = false; btn.innerHTML = '<i class="bi bi-clipboard2-plus me-1"></i>Generar OT'; }
          avisar(j.error || 'No se pudo crear la orden.', 'error');
          return;
        }
        avisar('✓ ' + j.numero_ot + ' creada · ' + j.n_tareas + ' tareas', 'success');
        (j.avisos || []).forEach(function (a) { avisar(a, 'warning'); });
        setTimeout(function () { location.href = j.ot_url; }, 700);
      })
      .catch(function () {
        if (btn) { btn.disabled = false; btn.innerHTML = '<i class="bi bi-clipboard2-plus me-1"></i>Generar OT'; }
        avisar('Se cortó la conexión. Intenta de nuevo.', 'error');
      });
  }

  return { iniciar: iniciar, crear: crear, refrescarPasos: refrescarPasos };
})();

/* 🧭 MOTOR DE FINANZAS Y DOCUMENTOS DE LA OT — un solo componente, dos lugares.
   Daniel, 2026-10-07: "este módulo lo necesito potente, con espacios ergonómicos donde se pueda visualizar todo el
   panorama: cuántas facturas, cuántos servicios… siempre hay que pasar todas completas, pero con un detalle… y
   finalmente en el segundo modal, cuando firma el autorizador que se va a cerrar la OT, necesito que ese mismo
   motor funcione porque tengo que tener una segunda opción de modificar".

   Se monta en cada <div data-fin-motor data-vid="N" data-modo="ficha|modal"> (parcial templates/ot2/_fin_motor.html):
   la ficha de la OT y el modal de aprobación y cierre usan ESTE archivo, idéntico.
   2026-10-08 (Daniel: «la finanza y finanzas y documentos de la OT te quedó mucho espacio… compactemos todo en un mismo
   lugar»): UN solo bloque compacto, de arriba abajo: encabezado en una fila (Actualizar · Corregir finanzas · OT con
   finanzas dudosas) → franja de 6 chips (el recorrido) → contadores en chips → la cuenta en una línea con el centro de
   costo al lado → dos columnas balanceadas: «Documentos» | «Lo que nos costó» + «Modificar». Sin documentos: una línea
   (sin caja punteada) y «Lo que nos costó» a todo el ancho. El diseño responde al ancho del bloque (container queries).
   Lee:  GET /ot/api/<vid>/recorrido  (6 pasos, cuenta, autorizaciones)  +  GET /ot/api/<vid>/panorama (documentos
         completos con sus líneas del ERP, contadores, lo que nos costó).
   Escribe (todo con registro de quién y cuándo): POST /ot/api/<vid>/documentos, POST /ot/api/<vid>/centro-costo,
         POST /ot/api/<vid>/costo-proveedor, POST /ot/api/autorizaciones (+ aprobar si quien pide es superadmin).
   Sin alert/confirm/prompt nativos (REGLA #1): ilusToast/ilusConfirm y un diálogo propio para formularios.
   2026-10-25 (REGLA #44, Daniel con la OT-2026-00354: «contradicciones y muy poco explicativo… no tantas tarjetas»): v2. De arriba abajo: encabezado
   (UN camino para tocar la plata) → UNA frase dinámica (rec.resumen) → «El recorrido» (seis filas con el texto completo de cada paso) | «La cuenta»
   (UNA tarjeta: cifras + anexo del proveedor + centro de costo) → Documentos (con sus contadores) | Lo que nos costó → pie de superadmin. El
   servidor cuenta la autorización del $0 con la MISMA regla que el candado de cierre, así que la pantalla no se puede contradecir.
   2026-10-25 (Daniel, con la OT-2026-00247: «los costos, resumidos y con pocos clics»): v3. «Lo que nos costó» se fundió en La cuenta (UN solo lugar para
   la plata: quién nos cobró, su anexo y los repuestos en la misma tabla); «Modificar» queda detrás de UN botón «Más opciones» cuando todo está en
   orden (si algo falta, el botón que lo resuelve se ve, en azul); y el recorrido se pliega en una línea de chips cuando ningún paso falta.
   Orden: encabezado → frase → La cuenta | El recorrido (plegado: su línea arriba y La cuenta debajo) → Documentos a ancho completo → pie. */
(function (global) {
  'use strict';

  var CAMPANA_DAN = 'Daniel';
  var montados = [];
  /* 2026-10-08 (Daniel: «la autorización del trabajo interno estaba presentando problemas, no deberían para el cierre
     ya que son trabajos internos y no tienen clientes ni facturas o documentos»). Una OT interna SIN cliente no tiene a
     quién cobrarle ni documento que pedir: el servidor la deja pasar (puerta + aprobar-cierre) y lo informa en
     recorrido.interna / panorama.interna. Con eso el motor NO ofrece agregar documento, declarar cobro/$0 ni pedir
     autorización: antes la barra «Modificar» se los ofrecía igual y el servidor respondía «no necesita autorización». */
  var TXT_INTERNA = 'Trabajo interno: no necesita documento ni autorización.';
  var SOLO_CLIENTE = { ligarDoc: 1, pedirCero: 1, quitarCero: 1, pedirCierre: 1, declararCobro: 1, resolverSaldo: 1, resolverSaldoOt: 1 };
  function esInterna(inst) {
    return !!(inst && ((inst.rec && inst.rec.interna) || (inst.pan && inst.pan.interna)));
  }

  function esc(s) {
    return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
    });
  }
  function clp(n) {
    if (n === null || n === undefined || n === '' || isNaN(Number(n))) return '—';
    var v = Math.round(Number(n));
    return (v < 0 ? '−' : '') + '$' + Math.abs(v).toString().replace(/\B(?=(\d{3})+(?!\d))/g, '.');
  }
  function toast(msg, tipo) {
    if (global.ilusToast) global.ilusToast(msg, { type: tipo || 'info' });
  }
  function api(url, opts) {
    opts = opts || {};
    var init = { method: opts.method || 'GET', credentials: 'same-origin', headers: {} };
    if (opts.body !== undefined) {
      init.headers['Content-Type'] = 'application/json';
      init.body = JSON.stringify(opts.body);
    }
    return fetch(url, init).then(function (r) {
      return r.json().catch(function () { return null; }).then(function (j) {
        return { ok: r.ok && !(j && j.ok === false), status: r.status, j: j || {} };
      });
    }).catch(function () { return { ok: false, status: 0, j: { error: 'No hay conexión con el servidor.' } }; });
  }

  /* ── Diálogo de formulario (a nivel raíz del documento: sirve dentro de cualquier modal) ─────────────────── */
  function dialogo(o) {
    return new Promise(function (resolve) {
      var ov = document.createElement('div');
      ov.className = 'fm-ov';
      ov.setAttribute('role', 'dialog');
      ov.setAttribute('aria-modal', 'true');
      var cuerpo = '';
      if (o.intro) cuerpo += '<p class="fm-dlg-intro">' + o.intro + '</p>';
      (o.campos || []).forEach(function (c, i) {
        var id = 'fmf' + Date.now() + '_' + i;
        c._id = id;
        cuerpo += '<div class="fm-fld"><label for="' + id + '">' + esc(c.label) + (c.req ? ' <span class="fm-req">*</span>' : '') + '</label>';
        if (c.tipo === 'select') {
          cuerpo += '<select id="' + id + '">' + (c.opciones || []).map(function (op) {
            return '<option value="' + esc(op.v) + '"' + (String(op.v) === String(c.valor) ? ' selected' : '') + '>' + esc(op.n) + '</option>';
          }).join('') + '</select>';
        } else if (c.tipo === 'area') {
          cuerpo += '<textarea id="' + id + '" rows="4" maxlength="1000" placeholder="' + esc(c.placeholder || '') + '">' + esc(c.valor || '') + '</textarea>' +
            (c.min ? '<div class="fm-cnt" id="' + id + '_c"></div>' : '');
        } else if (c.tipo === 'monto') {
          cuerpo += '<div class="fm-money"><span>$</span><input id="' + id + '" type="text" inputmode="numeric" autocomplete="off" placeholder="' + esc(c.placeholder || '0') + '" value="' + esc(c.valor == null ? '' : c.valor) + '"></div>';
        } else {
          cuerpo += '<input id="' + id + '" type="text" autocomplete="off" inputmode="' + (c.tipo === 'numero' ? 'numeric' : 'text') + '" placeholder="' + esc(c.placeholder || '') + '" value="' + esc(c.valor == null ? '' : c.valor) + '">';
        }
        if (c.ayuda) cuerpo += '<div class="fm-help">' + esc(c.ayuda) + '</div>';
        cuerpo += '</div>';
      });
      ov.innerHTML = '<div class="fm-dlg"><div class="fm-dlg-h"><b>' + esc(o.titulo || '') + '</b>' +
        '<button type="button" class="fm-x" aria-label="Cerrar">&times;</button></div>' +
        '<div class="fm-dlg-b">' + cuerpo + '<div class="fm-dlg-err" id="fmErr" role="alert"></div></div>' +
        '<div class="fm-dlg-f"><button type="button" class="fm-btn" data-r="no">' + esc(o.cancelar || 'Cancelar') + '</button>' +
        '<button type="button" class="fm-btn fm-btn-pri" data-r="si">' + esc(o.ok || 'Aceptar') + '</button></div></div>';
      /* El diálogo va DENTRO del modal Bootstrap abierto (si lo hay): con el focus trap de Bootstrap 5.3, un
         diálogo colgado de <body> no deja escribir en sus cajas dentro de «Firmar y cerrar OT». Mismo criterio que
         _ilusOverlayHost de ilus_ui.js (z-index 2100 del overlay > modal). */
      var host = document.body;
      try {
        var tka = document.getElementById('tkaModal');
        host = (tka && tka.classList.contains('is-open')) ? tka : (document.querySelector('.modal.show') || document.body);
      } catch (e) { host = document.body; }
      host.appendChild(ov);
      var cerrado = false;
      function cerrar(v) {
        if (cerrado) return;
        cerrado = true;
        document.removeEventListener('keydown', alTecla, true);
        if (ov.parentNode) ov.parentNode.removeChild(ov);
        resolve(v);
      }
      function alTecla(e) { if (e.key === 'Escape') { e.stopPropagation(); cerrar(null); } }
      document.addEventListener('keydown', alTecla, true);
      (o.campos || []).forEach(function (c) {
        var el = document.getElementById(c._id);
        if (!el) return;
        if (c.tipo === 'monto') {
          el.addEventListener('input', function () {
            var d = el.value.replace(/\D/g, '');
            el.value = d ? d.replace(/\B(?=(\d{3})+(?!\d))/g, '.') : '';
          });
        }
        if (c.tipo === 'area' && c.min) {
          var cnt = document.getElementById(c._id + '_c');
          var pinta = function () {
            var n = el.value.trim().length;
            cnt.textContent = n + ' de ' + c.min + ' caracteres como mínimo' + (n >= c.min ? ' ✓' : '');
            cnt.className = 'fm-cnt' + (n >= c.min ? ' ok' : '');
          };
          el.addEventListener('input', pinta);
          pinta();
        }
      });
      ov.addEventListener('click', function (e) {
        var b = e.target.closest('[data-r]');
        if (e.target === ov) return;
        if (e.target.closest('.fm-x')) return cerrar(null);
        if (!b) return;
        if (b.getAttribute('data-r') === 'no') return cerrar(null);
        var vals = {}, err = '';
        (o.campos || []).forEach(function (c) {
          var el = document.getElementById(c._id);
          var v = el ? el.value.trim() : '';
          if (c.tipo === 'monto') v = v.replace(/\./g, '');
          if (!err && c.req && !v) err = 'Falta completar «' + c.label + '».';
          if (!err && c.min && v.length < c.min) err = '«' + c.label + '» necesita al menos ' + c.min + ' caracteres: es lo que va a leer quien autoriza.';
          vals[c.k] = v;
        });
        if (err) { var eb = document.getElementById('fmErr'); eb.textContent = err; return; }
        cerrar(vals);
      });
      var primero = ov.querySelector('input,select,textarea');
      if (primero) setTimeout(function () { try { primero.focus(); } catch (e) { } }, 60);
    });
  }

  /* ── Datos ───────────────────────────────────────────────────────────────────────────────────────────────── */
  function cargar(inst) {
    inst.el.classList.add('cargando');
    return Promise.all([
      api('/ot/api/' + inst.vid + '/recorrido'),
      api('/ot/api/' + inst.vid + '/panorama')
    ]).then(function (r) {
      inst.el.classList.remove('cargando');
      if (!r[0].ok || !r[1].ok) {
        /* La franja del anexo (la dibuja el servidor y adoptarAnexo la guardó dentro de la tarjeta) no se pierde si el bloque no se puede leer. */
        var franjaAnx = inst.anexoNodo || document.getElementById('otdAnexoVinculoFicha');
        inst.el.innerHTML = '<div class="fm-err"><i class="bi bi-exclamation-triangle-fill"></i> ' +
          esc((r[0].j && r[0].j.error) || (r[1].j && r[1].j.error) || 'No se pudo leer las finanzas y documentos de esta OT.') +
          ' <button type="button" class="fm-btn" data-fm-act="recargar">Reintentar</button>' + (inst.editar || '') + '</div>';
        if (franjaAnx) { inst.anexoNodo = franjaAnx; try { franjaAnx.hidden = false; inst.el.appendChild(franjaAnx); } catch (e) { } }
        /* 2026-10-08: la vista de solo lectura de la tarjeta «Finanzas de la OT» se oculta mientras el motor está presente;
           si el motor no pudo leer, vuelve a mostrarse para no dejar la OT sin ver sus finanzas. */
        try {
          var viejaFin = document.getElementById('otdCardFinanzas');
          if (viejaFin) {
            viejaFin.classList.add('fin-motor-fallo');
            /* 2026-10-08: la tarjeta vive a nivel raíz (en el modal de edición, o suelta si no se puede editar). Sin permiso de
               edición, su vista de solo lectura se acomoda justo debajo del bloque para que se vea junto a él. */
            var bloque = document.getElementById('otdCardMotor');
            if (!viejaFin.closest('.modal') && bloque && bloque.parentNode && inst.el.closest('#otdCardMotor')) bloque.parentNode.insertBefore(viejaFin, bloque.nextSibling);
          }
        } catch (e) { }
        return;
      }
      inst.rec = r[0].j;
      inst.pan = r[1].j;
      try { var viejaOk = document.getElementById('otdCardFinanzas'); if (viejaOk) viejaOk.classList.remove('fin-motor-fallo'); } catch (e) { }
      pintar(inst);
    });
  }

  /* ── Pintado ─────────────────────────────────────────────────────────────────────────────────────────────── */
  var ICONO_PASO = { hecho: 'bi-check-lg', falta: 'bi-exclamation-lg', no_aplica: 'bi-dash-lg', pendiente: 'bi-hourglass-split' };

  function boton(act, label, icono, extra) {
    return '<button type="button" class="fm-btn' + (extra ? ' ' + extra : '') + '" data-fm-act="' + act + '"><i class="bi ' + icono + '"></i> ' + esc(label) + '</button>';
  }

  /* ═══ LA TARJETA v2 — 2026-10-25 (Daniel, con la OT-2026-00354: «contradicciones y muy poco explicativo… tratemos de resumir esto, no tantas tarjetas;
     en una puedo sacar cuánto me cobraron, cuánto es mi margen y el porcentaje… entiendo que es un paso a paso, pero lo necesito más específico»).
     Un SOLO sentido: ① ¿Se cobra? → ② Factura → ③ Cobré → ④ Me cobró el proveedor (= el anexo) → ⑤ Margen → ⑥ Cierre.
       · Arriba, UNA frase dinámica (rec.resumen) que dice si la OT está lista para cerrar o qué le falta, con las mismas palabras de los pasos.
       · A la izquierda, el recorrido con el texto ESPECÍFICO y completo de cada paso (nada de chips cortados); a la derecha, UNA tarjeta: la
         cuenta (Cobramos · Nos cobraron · Ganamos/Perdimos con su %), el vínculo con el anexo del proveedor y el centro de costo.
       · Los once contadores pasaron a unos pocos chips dentro de «Documentos» (solo los que dicen algo).
       · La plata se toca por UN camino según el estado de la OT: abierta → «Editar cobro y costos»; cerrada → «Corregir finanzas». Lo avanzado
         de superadmin vive en un pie tranquilo, con una línea que explica qué hace cada cosa. */
  function htmlCabecera(inst) {
    var p = inst.pan;
    var sub = esc(p.numero_ot || '') + (p.cliente ? ' · ' + esc(p.cliente) : '');
    return '<div class="fm-head"><h3><i class="bi bi-compass-fill"></i> Costos y documentación' + (inst.modo === 'modal' ? ' para cerrar' : ' de la OT') +
      (sub ? '<small>' + sub + '</small>' : '') + '</h3>' +
      '<div class="fm-head-a"><button type="button" class="fm-btn" data-fm-act="recargar" title="Volver a leer lo declarado"><i class="bi bi-arrow-clockwise"></i> Actualizar</button>' +
      (inst.editar || '') + (p.cerrada ? (inst.btnCorregir || '') : '') + '</div></div>';
  }

  /* El pie de superadmin (solo en la ficha): «Corregir finanzas» (si la OT está abierta; en una cerrada ya es el botón de arriba) y «OT con
     finanzas dudosas», cada uno con UNA línea que dice qué hace. Daniel preguntó si «Editar costos», «Corregir finanzas» y «OT con finanzas
     dudosas» eran lo mismo: no lo son, y ahora la pantalla lo explica. */
  function htmlPie(inst) {
    if (inst.modo !== 'ficha' || !(inst.btnCorregir || inst.lnkDudosas)) return '';
    var h = '<div class="fm-pie"><span class="fm-pie-t"><i class="bi bi-shield-lock"></i> Solo superadmin</span>';
    if (inst.btnCorregir && !inst.pan.cerrada)
      h += '<span class="fm-pie-i">' + inst.btnCorregir + '<small>reabre lo ya declarado y deja registrado quién lo cambió y por qué</small></span>';
    if (inst.lnkDudosas)
      h += '<span class="fm-pie-i">' + inst.lnkDudosas + '<small>lista todas las OT con la plata por revisar (sin cobro, sin costo o sin factura)</small></span>';
    return h + '</div>';
  }

  /* Botón chico dentro de un chip del recorrido: el texto completo queda en el tooltip. */
  function botonChip(act, label, icono, pri, titulo) {
    return '<button type="button" class="fm-btn fm-btn-chip' + (pri ? ' fm-btn-pri' : '') + '" data-fm-act="' + act + '" title="' + esc(titulo || label) + '"><i class="bi ' + icono + '"></i> ' + esc(label) + '</button>';
  }

  /* LA frase de arriba: la dice el servidor (rec.resumen) con las mismas reglas que los pasos, así que no puede contradecirlos. */
  var ICONO_RESUMEN = { listo: 'bi-check-circle-fill', cerrada: 'bi-lock-fill', espera: 'bi-hourglass-split', falta: 'bi-exclamation-circle-fill' };
  /* La frase TAL COMO se muestra (la del servidor, más el saldo excedido, que solo mide el panorama): la usan la frase de arriba y la decisión de
     plegar el recorrido, para que las dos digan siempre lo mismo. */
  function resumenDe(inst) {
    var r = inst.rec && inst.rec.resumen;
    if (!r || !r.titulo) return null;
    /* Lo que se cobra no puede pasar del saldo de las facturas (el servidor frena el cierre por saldo consumido): se mide en el panorama, no en el
       recorrido, así que la frase lo suma acá para no decir «Lista para cerrar» cuando el cierre se va a rechazar. */
    var sal = inst.pan && inst.pan.saldo, via = (inst.rec.puerta && inst.rec.puerta.via) || '';
    if (r.estado === 'listo' && sal && sal.excedido && inst.rec.fin && inst.rec.fin.cobra && !inst.rec.cerrada && via !== 'autorizacion_cerrar' && via !== 'pendiente_madre')
      r = { estado: 'falta', titulo: 'Falta resolver el saldo de la factura: lo que se cobra pasa de lo que le queda' };
    return r;
  }
  function htmlResumen(inst) {
    var r = resumenDe(inst);
    if (!r) return '';
    return '<div class="fm-resumen ' + esc(r.estado || 'falta') + '" role="status"><i class="bi ' + (ICONO_RESUMEN[r.estado] || 'bi-info-circle-fill') + '"></i><b>' + esc(r.titulo) + '</b></div>';
  }

  /* ¿El costo del proveedor viene de un anexo vivo? (REGLA #34: el costo de la OT y el anexo son UN solo valor). */
  function anexoVivo(inst) { return (inst.rec && inst.rec.anexo) || null; }

  function htmlPasos(inst) {
    var rec = inst.rec, puede = inst.pan.puede_editar || (inst.pan.cerrada && inst.pan.puede_regularizar);
    var pend = rec.solicitud_pendiente;
    var html = '';
    var interna = esInterna(inst);
    var ax = anexoVivo(inst);
    if (pend && !interna) {
      html += '<div class="fm-espera"><i class="bi bi-hourglass-split"></i><div><b>Esperando autorización de ' + CAMPANA_DAN + ' desde ' + esc(pend.solicitado_at) + '</b>' +
        '<span>' + esc(pend.tipo_txt) + (pend.motivo_txt ? ' · ' + esc(pend.motivo_txt) : '') + ' · la pidió ' + esc(pend.solicitado_por_nombre) + '. ' +
        'Hasta que responda, la OT no se puede cerrar.</span></div>' +
        '<a class="fm-btn" href="' + esc(pend.url) + '">Ver la solicitud</a></div>';
    }
    if (inst.rechazo) {
      html += '<div class="fm-rechazo" id="fmRechazo"><i class="bi bi-x-octagon-fill"></i><div><b>No se pudo cerrar la OT</b><span>' + esc(inst.rechazo.error || '') + '</span></div>' +
        accionDeRechazo(inst.rechazo) + '</div>';
    }
    /* El recorrido: seis filas, cada una con su círculo de estado, el título y el texto ESPECÍFICO y completo del paso. */
    html += '<ol class="fm-pasos">';
    (rec.pasos || []).forEach(function (p) {
      var act = '';
      if (p.estado === 'falta' && puede) {
        /* Interna sin cliente: solo el costo del proveedor (paso 4) puede ofrecer algo; documento, cobro y cierre no. */
        if (p.n === 1 && !interna) act = botonChip('pedirCero', 'Pedir autorización', 'bi-shield-check', false, 'Pedir autorización del $0');
        else if (p.n === 2 && !interna) act = botonChip('ligarDoc', 'Agregar factura', 'bi-link-45deg', true, 'Agregar factura o boleta') + botonChip('pedirCierre', 'Pedir autorización', 'bi-shield-lock', false, 'Pedir autorización a ' + CAMPANA_DAN);
        else if (p.n === 3 && !interna && inst.pan.puede_editar) act = botonChip('declararCobro', 'Declarar cobro', 'bi-cash-coin', false, 'Declarar lo que cobré');
        /* Con un anexo vivo el costo ES el del anexo: la salida es alinearlo con un clic, no escribirlo a mano (el servidor lo rechazaría). */
        else if (p.n === 4 && inst.pan.puede_editar && ax && ax.abierta && ax.vinculo !== 'alineado' && global.otdCostoDesdeAnexo)
          act = botonChip('usarAnexo', 'Usar el valor del anexo', 'bi-link-45deg', true, 'Dejar el costo de la OT igual al del Anexo N° ' + ax.numero);
        else if (p.n === 4 && inst.pan.puede_editar && !(ax && ax.abierta)) act = botonChip('corregirProv', 'Declarar costo', 'bi-pencil-square', false, 'Declarar lo que cobró el proveedor');
        else if (p.n === 6 && !pend && !interna && p.accion === 'pedir_aut_perdida') act = botonChip('pedirPerdida', 'Pedir autorización', 'bi-shield-exclamation', true, 'Pedir a ' + CAMPANA_DAN + ' la autorización para cerrar con pérdida');
        else if (p.n === 6 && !pend && !interna) act = botonChip('pedirCierre', 'Pedir autorización', 'bi-shield-lock', false, 'Pedir autorización a ' + CAMPANA_DAN);
      }
      html += '<li class="fm-paso ' + esc(p.estado) + (p.clase ? ' c-' + esc(p.clase) : '') + '" data-paso="' + esc(p.n) + '">' +
        '<span class="fm-circ"><i class="bi ' + (ICONO_PASO[p.estado] || 'bi-circle') + '"></i></span>' +
        '<div class="fm-paso-c"><b>' + p.n + '. ' + esc(p.titulo) + '</b><span class="fm-paso-e">' + esc(p.texto || '') + '</span></div>' +
        (act ? '<div class="fm-paso-a">' + act + '</div>' : '') + '</li>';
    });
    return html + '</ol>';
  }

  /* ═══ EL RECORRIDO, PLEGADO CUANDO NO HAY NADA QUE HACER ═══════════════════════════════════════════════════════════════════
     2026-10-25 (Daniel, con la OT-2026-00247: «esto está bien, se puede mejorar es los costos: resumidos y con pocos clics»): si ningún paso está en
     «falta» (todo hecho, no aplica o esperando la firma), las seis filas no le dicen nada nuevo a nadie: se ven como UNA línea de chips
     («① Se cobra ✓ · ② Factura 11567 ✓ · … · ⑥ Papeles al día») y un toque abre las seis filas con su texto completo (REGLA #15: el texto sigue en la
     página, solo plegado). Si algo falta, está esperando a Daniel o el cierre fue rechazado, el recorrido queda abierto como siempre: ahí están los botones
     de «te toca». */
  var NUMEROS = ['①', '②', '③', '④', '⑤', '⑥'];
  function recorridoPlegable(inst) {
    var rec = inst.rec, pasos = rec.pasos || [], r = resumenDe(inst);
    if (pasos.length !== 6 || inst.rechazo) return false;
    if (rec.solicitud_pendiente && !esInterna(inst)) return false;
    if (pasos.some(function (p) { return p.estado === 'falta'; })) return false;
    if (r && r.estado !== 'listo' && r.estado !== 'cerrada') return false;   /* «falta» (también la del saldo) y «espera» dejan el recorrido abierto */
    return true;
  }
  /* Un chip por paso, con la cifra o el dato que importa; el texto COMPLETO del paso viaja en su tooltip y, abierto el recorrido, en su fila. */
  function chipsRecorrido(inst) {
    var rec = inst.rec, pasos = rec.pasos || [], fin = rec.fin || {}, C = fin.cobre || {}, M = fin.me_cobraron || {}, Q = fin.queda || {};
    var docs = ((inst.pan && inst.pan.documentos) || []).filter(function (d) { return d.es_cobro && d.categoria === 'cobro' && !d.dada_de_baja_por; });
    var centro = ((inst.pan && inst.pan.centro) || {}).nombre || '';
    var r = resumenDe(inst), t = [];
    t[0] = fin.cobra ? 'Se cobra' : (esInterna(inst) ? 'Trabajo interno' : String(fin.cobertura_txt || 'No se cobra').split(':')[0]);
    t[1] = docs.length
      ? docs.slice(0, 2).map(function (d) { return (d.tipo_txt || 'Documento') + ' ' + d.nudo; }).join(' y ') + (docs.length > 2 ? ' +' + (docs.length - 2) : '')
      : 'Factura: no aplica';
    t[2] = fin.cobra ? 'Cobré ' + (C.hay ? clp(C.total) : '—') : 'No se cobra';
    t[3] = 'Proveedor ' + (M.falta_tecnico ? '—' : clp(M.total));
    t[4] = (fin.cobra && Q.mostrar) ? 'Margen ' + (Q.pct !== null && Q.pct !== undefined ? pctTxt(Q.pct) : signo(Q.total)) : (centro ? 'Se carga a ' + centro : 'Margen');
    t[5] = String((r && r.titulo) || (pasos[5] || {}).titulo || 'Revisión al cerrar').split(/[,:·]/)[0].trim();
    return pasos.slice(0, 6).map(function (p, i) {
      var ico = p.estado === 'hecho' ? 'bi-check-lg' : (p.estado === 'pendiente' ? 'bi-hourglass-split' : 'bi-dash-lg');
      return '<span class="fm-pchip ' + esc(p.estado) + '" title="' + esc(p.n + '. ' + p.titulo + ': ' + (p.texto || '')) + '"><b>' + NUMEROS[i] + '</b> ' + esc(t[i]) +
        ' <i class="bi ' + ico + '"></i></span>';
    }).join('');
  }
  function htmlRecorrido(inst, plegar) {
    var h = '<section class="fm-s1' + (plegar ? ' fm-rec' : '') + '" aria-label="El recorrido de la plata de esta OT"><h4 class="fm-col-t"><i class="bi bi-signpost-split-fill"></i> El recorrido</h4>';
    if (!plegar) return h + htmlPasos(inst) + '</section>';
    var abierto = !!inst.recAbierto, id = 'fmRec' + inst.id;
    return h + '<button type="button" class="fm-rec-sum" data-fm-act="recTog" aria-expanded="' + abierto + '" aria-controls="' + id + '">' +
      '<span class="fm-rec-chips">' + chipsRecorrido(inst) + '</span>' +
      '<span class="fm-rec-ver">' + (abierto ? 'Ocultar el detalle' : 'Ver el detalle') + ' <i class="bi bi-chevron-' + (abierto ? 'up' : 'down') + '"></i></span></button>' +
      '<div class="fm-rec-det" id="' + id + '"' + (abierto ? '' : ' hidden') + '>' + htmlPasos(inst) + '</div></section>';
  }

  function accionDeRechazo(d) {
    var a = d && d.accion; if (!a) return '';
    var m = { ligar_factura: 'ligarDoc', pedir_autorizacion: 'pedirCierre', pedir_aut_perdida: 'pedirPerdida', declarar_cobro: 'declararCobro',
      declarar_centro: 'enfocarCentro', declarar_costo_proveedor: 'corregirProv', resolver_saldo: 'resolverSaldo' }[a.tipo];
    if (a.tipo === 'esperar_autorizacion') return '<a class="fm-btn fm-btn-pri" href="' + esc(a.url || '/ot/autorizaciones') + '">Ver la solicitud</a>';
    /* 2026-10-19: la desinstalación cubierta cierra cuando su instalación ya tiene la factura o boleta: el botón lleva a ESA OT. */
    if (a.tipo === 'abrir_ot_madre') return a.url ? '<a class="fm-btn fm-btn-pri" href="' + esc(a.url) + '">Abrir la instalación' + (a.madre_numero ? ' ' + esc(a.madre_numero) : '') + '</a>' : '<span class="fm-help">' + esc(a.label || '') + '</span>';
    if (a.tipo === 'actualizar_anexo') return '<span class="fm-help">' + esc(a.label || '') + '</span>';
    if (!m) return a.label ? '<span class="fm-help">' + esc(a.label) + '</span>' : '';
    return boton(m, a.label || 'Resolver', 'bi-arrow-right-circle', 'fm-btn-pri');
  }

  /* Los contadores, ahora DENTRO de «Documentos» y solo los que dicen algo (2026-10-25, Daniel: «no tantas tarjetas»): antes eran once chips fijos, con ceros y
     datos repetidos (documentos con servicio / líneas de servicio). Ahora: qué tipo de documentos hay, cuáles traen instalación o despacho y, si pasa, el aviso
     de que otra OT usa su saldo o que se agotó. El detalle de cada uno va en su tooltip. */
  function htmlContadores(inst) {
    var c = inst.pan.contadores || {};
    if (!(c.total || 0)) return '';   /* trabajo interno o sin documentos: no hay nada que contar */
    var baja = c.notas_venta_dadas_de_baja || 0;
    var chips = '';
    function tile(n, titulo, sub, cls) {
      if (!n) return;
      chips += '<span class="fm-tile ' + (cls || '') + '"' + (sub ? ' title="' + esc(titulo + ': ' + sub) + '"' : '') + '><b>' + n + '</b><span>' + esc(titulo) + '</span></span>';
    }
    tile(c.facturas, 'Facturas y boletas', 'Documentos que cobran', 'ok');
    tile(c.notas_venta, 'Notas de venta', baja ? (baja + ' ya dada' + (baja > 1 ? 's' : '') + ' de baja por factura') : 'Falta agregar la factura', 'nv');
    tile(c.cotizaciones, 'Cotizaciones', 'Referencia', '');
    tile(c.otros, 'Otros documentos', 'Guías y otros', '');
    tile(c.docs_con_servicio, 'Con instalación o servicio', 'Traen línea ZZ de servicio' + (c.servicios ? ' · ' + c.servicios + ' línea' + (c.servicios > 1 ? 's' : '') : ''), 'ser');
    tile(c.docs_con_despacho, 'Con despacho', 'Traen línea ZZENVIO' + (c.despachos ? ' · ' + c.despachos + ' línea' + (c.despachos > 1 ? 's' : '') : ''), 'des');
    tile(c.docs_usados_en_otras, 'Usados en otras OT', 'Otra OT ya toma plata de ellos', 'warn');
    tile(c.docs_saldo_agotado, 'Saldo agotado', 'No queda nada por cobrar', 'mal');
    if (c.incompleto) tile(c.sin_leer || 1, 'Sin leer en Random', 'Documentos que Random no devolvió ahora: las cifras pueden estar incompletas', 'warn');
    return chips ? '<div class="fm-tiles">' + chips + '</div>' : '';
  }

  /* Las líneas de un documento como filas de UNA tabla (la etiqueta Servicio/Despacho va en la primera columna). */
  function filaLinea(l, conCant, cls, tag) {
    return '<tr class="' + cls + '">' + (tag || '') + '<td class="fm-d">' + (conCant && l.cantidad ? '<span class="fm-cant">' + esc(l.cantidad % 1 ? l.cantidad : Math.round(l.cantidad)) + ' ×</span> ' : '') + esc(l.descripcion) +
      ' <small>' + esc(l.sku) + '</small></td><td class="fm-n">' + clp(l.monto) + '</td></tr>';
  }
  function grupoLineas(titulo, arr, cls, conCant) {
    if (!arr || !arr.length) return '';
    return arr.map(function (l, i) {
      return filaLinea(l, conCant, cls, i === 0 ? '<th class="fm-tg ' + cls + '" scope="rowgroup" rowspan="' + arr.length + '">' + esc(titulo) + '</th>' : '');
    }).join('');
  }

  var RUT_TXT = { ok: ['ok', 'El RUT coincide con el del cliente'], justificado: ['aviso', 'RUT distinto, justificado'],
    distinto: ['mal', 'RUT distinto, sin justificar'], sin_verificar: ['gris', 'RUT sin verificar'], no_aplica: ['gris', 'No aplica'] };

  /* Lo que ESTE documento aporta al cobro (servicio + despacho), o por qué no suma. 2026-10-08 (Daniel: «los
     servicios por factura»): cada documento es un bloque con sus líneas y lo que aporta. */
  function sumaMontos(arr) { var t = 0; (arr || []).forEach(function (l) { t += Number(l.monto) || 0; }); return t; }
  function aporteDoc(d) {
    if (d.dada_de_baja_por) return { cls: 'no', txt: 'No suma: la factura que la reemplaza ya cobra' };
    /* 2026-10-27 (OT-2026-00220): una nota de venta cuya factura o boleta TAMBIÉN está en la OT es el mismo cobro (el saldo ya la
       omite) aunque la factura no la haya «dado de baja» formalmente: sumarla dos veces mostraba $248.880 donde se cobró $124.440. */
    if (d.saldo && d.saldo.omitido) return { cls: 'no', txt: 'No suma: su factura o boleta ya cobra' };
    if (d.origen === 'cotizacion') return { cls: 'no', txt: 'Referencia: no suma al cobro' };
    if (!d.es_cobro) return { cls: 'no', txt: 'No suma: referencia de la garantía' };
    var s = null, e = null;
    if (d.lineas) { s = sumaMontos(d.lineas.servicio); e = sumaMontos(d.lineas.despacho); }
    if (!s && !e) { s = d.zz_serv; e = d.zz_envio; }
    if (!s && !e) {
      if (d.categoria === 'nota_venta') return { cls: 'nv', txt: 'Promesa de cobro: falta la factura' };
      return { cls: 'no', txt: 'No trae líneas de servicio ni despacho' };
    }
    return { cls: 'si', total: (s || 0) + (e || 0), txt: 'Aporta al cobro: servicio ' + clp(s || 0) + ' + despacho ' + clp(e || 0) + ' = ' + clp((s || 0) + (e || 0)) };
  }

  /* SALDO por línea de servicio y despacho (Daniel 2026-10-08: «evitar que dos instalaciones se paguen con el mismo
     saldo»): monto de la línea, cuánto ya usan OTRAS OT (con su número, cliente y enlace) y lo que queda. */
  var CAT_TXT = { servicio: 'Instalación o servicio', despacho: 'Despacho' };
  function saldoConLineas(d) {
    var s = d.saldo;
    return !!(s && ((s.servicio && s.servicio.hay_lineas) || (s.despacho && s.despacho.hay_lineas)));
  }
  /* Estado del saldo del documento (semáforo): verde = sin usar, ámbar = usado en parte, rojo = agotado. */
  function saldoEstado(d) {
    if (!saldoConLineas(d)) return null;
    var s = d.saldo, agotado = false, usado = false;
    ['servicio', 'despacho'].forEach(function (c) {
      var x = s[c]; if (!x || !x.hay_lineas) return;
      if (x.saldo <= 0) agotado = true;
      if (x.usado > 0) usado = true;
    });
    return agotado ? ['mal', 'Saldo agotado'] : (usado ? ['aviso', 'Usado en parte por otras OT'] : ['ok', 'Sin usar en otras OT']);
  }
  /* La tabla del saldo ES la lista de líneas del documento (servicio y despacho, cada una con su monto, lo que ya usan
     otras OT y lo que queda): cuando existe, las líneas no se repiten arriba. */
  function htmlSaldoDoc(d) {
    var s = d.saldo;
    if (!saldoConLineas(d)) return '';
    var filas = '';
    ['servicio', 'despacho'].forEach(function (c) {
      var x = s[c]; if (!x || !x.hay_lineas) return;
      (x.lineas || []).forEach(function (l) {
        filas += '<tr class="' + (l.saldo <= 0 ? 'cero' : (l.usado > 0 ? 'parcial' : 'libre')) + '"><td class="fm-d"><b class="fm-tgi ' + (c === 'servicio' ? 'ser' : 'des') + '">' + esc(CAT_TXT[c]) + '</b> ' +
          (l.descripcion && l.descripcion !== l.sku ? esc(l.descripcion) + ' ' : '') + '<small>' + esc(l.sku) + '</small></td>' +
          '<td class="fm-n" data-l="Monto">' + clp(l.monto) + '</td><td class="fm-n" data-l="Usado en otras OT">' + clp(l.usado) + '</td><td class="fm-n" data-l="Saldo"><b>' + clp(l.saldo) + '</b></td></tr>';
      });
    });
    var usos = (s.usos || []).map(function (u) {
      var partes = [];
      if (u.servicio) partes.push('servicio ' + clp(u.servicio));
      if (u.despacho) partes.push('despacho ' + clp(u.despacho));
      return '<li><a href="' + esc(u.url || ('/ot/' + u.vid)) + '"><b>' + esc(u.numero_ot || ('OT #' + u.vid)) + '</b></a>' +
        (u.cliente ? ' · ' + esc(u.cliente) : '') + ' · usa ' + partes.join(' + ') + '</li>';
    }).join('');
    return '<div class="fm-saldo' + (d.dada_de_baja_por ? ' off' : '') + '">' +
      '<table class="fm-lt fm-st"><thead><tr><th>Servicio y despacho del documento</th><th>Monto</th><th>Usado en otras OT</th><th>Saldo</th></tr></thead><tbody>' + filas + '</tbody></table>' +
      (usos ? '<ul class="fm-usos">' + usos + '</ul>' : '') +
      (s.omitido ? '<div class="fm-help">' + esc(s.omitido) + '</div>' : '') + '</div>';
  }

  function htmlDoc(d) {
    var rut = RUT_TXT[d.rut_estado] || RUT_TXT.sin_verificar;
    var ap = aporteDoc(d);
    var baja = d.dada_de_baja_por;
    var cls = 'fm-doc cat-' + esc(d.categoria) + (baja ? ' baja' : '');
    var se = saldoEstado(d);
    var meta = '<dl class="fm-meta">' +
      '<div><dt>Fecha de emisión</dt><dd>' + (esc(d.fecha) || '—') + '</dd></div>' +
      '<div><dt>RUT</dt><dd class="' + rut[0] + '">' + rut[1] + (d.rut ? ' · ' + esc(d.rut) : '') + (d.rut_justif ? '<small>' + esc(d.rut_justif) + '</small>' : '') + '</dd></div>' +
      '<div><dt>Lo agregó</dt><dd>' + (esc(d.asociado_por) || 'No consta (documento anterior)') + (d.asociado_at ? '<small>' + esc(d.asociado_at) + '</small>' : '') + '</dd></div>' +
      '<div><dt>Monto del documento</dt><dd>' + clp(d.monto) + '</dd></div>' +
      (d.etiqueta ? '<div><dt>Etiqueta</dt><dd>' + esc(d.etiqueta) + '</dd></div>' : '') +
      (se ? '<div class="fm-meta-sd"><span class="fm-sd ' + se[0] + '">' + se[1] + '</span></div>' : '') + '</dl>';
    var nota = '';
    if (baja) nota = '<div class="fm-baja"><i class="bi bi-arrow-repeat"></i> Dada de baja por la factura <b>' + esc(baja.titulo) + '</b>. Las dos quedan visibles.</div>';
    else if (d.da_de_baja && d.da_de_baja.length) nota = '<div class="fm-baja ok"><i class="bi bi-check-circle-fill"></i> Dio de baja la nota de venta <b>' + d.da_de_baja.map(function (x) { return esc(x.titulo); }).join(', ') + '</b>.</div>';
    var lineas = '';
    if (d.origen === 'cotizacion') {
      lineas = '<div class="fm-nolin">Cotización interna: sirve de referencia, no de cobro.</div>';
    } else if (d.lineas) {
      var ln = d.lineas;
      // 2026-10-08 (Daniel, viendo la VD 10653: "no me interesan los productos en este punto… me interesa la
      // instalación y el envío… ese detalle está muy largo, no consideres los productos"): en las finanzas de la OT
      // cada documento muestra SOLO sus líneas de servicio (instalación/mantención) y de despacho. Los productos y la
      // suma neta del documento no aportan a la cuenta del servicio y alargaban la tarjeta.
      if (!ln.servicio.length && !ln.despacho.length)
        lineas = '<div class="fm-nolin">' + (ln.productos.length ? 'Este documento no trae línea de instalación ni de despacho (solo productos).' : 'Random no devolvió líneas para este documento.') + '</div>';
      else if (!se)
        lineas = '<table class="fm-lt fm-lineas"><tbody>' + grupoLineas('Servicio', ln.servicio, 'ser', false) + grupoLineas('Despacho', ln.despacho, 'des', false) + '</tbody></table>';
    } else if (d.lineas_omitidas) {
      lineas = '<div class="fm-nolin">Hay muchos documentos: las líneas de este se leen en su ficha de Random.</div>';
    } else if (d.tido && ['GDV', 'COV'].indexOf(d.tido) < 0) {
      lineas = '<div class="fm-nolin"><i class="bi bi-wifi-off"></i> No pudimos leer las líneas en Random ahora. El documento sigue declarado en la OT.</div>';
    }
    return '<article class="' + cls + '"><header><span class="fm-chip">' + esc(d.tipo_txt) + '</span><h4>' + esc(d.titulo) + '</h4>' +
      '<span class="fm-cuenta">' + esc(cuentaTxt(d)) + '</span>' +
      (d.es_principal ? '<span class="fm-pri">Principal</span>' : '') +
      '<span class="fm-aporta ' + ap.cls + '">' + esc(ap.txt) + '</span></header>' + meta + nota + lineas + htmlSaldoDoc(d) + '</article>';
  }
  function cuentaTxt(d) {
    return { servicio: 'Cobro del servicio', despacho: 'Cobro del despacho',
      nota_venta: 'Nota de venta: promesa de cobro', cotizacion: 'Cotización: solo referencia',
      referencia_garantia: 'Referencia de la garantía: no se cobra',
      otros: 'Factura de productos u otros: no es del servicio' }[d.cuenta] || (d.es_cobro ? 'Documento de cobro' : 'Referencia');
  }

  /* Sin documentos que mostrar (OT interna, o cliente que todavía no liga ninguno): NO se dibuja una caja grande, sino
     UNA línea. «Lo que nos costó» pasa a ocupar el ancho completo. */
  function htmlSinDocs(inst) {
    var p = inst.pan, puede = p.puede_editar || (p.cerrada && p.puede_regularizar);
    if (esInterna(inst)) {
      /* (el recorrido ya dice «Trabajo interno: no necesita documento ni autorización» en sus pasos: esta línea no lo repite) */
      return '<div class="fm-sindoc interna"><i class="bi bi-info-circle"></i><b>Sin documentos: es un trabajo interno.</b>' +
        '<span>No tiene cliente: no hay a quién cobrarle ni factura que agregar.</span></div>';
    }
    return '<div class="fm-sindoc"><i class="bi bi-file-earmark-x"></i><b>Sin documentos aún.</b>' +
      '<span>Agrega una factura, boleta, nota de venta o cotización; o pide la autorización de ' + CAMPANA_DAN + '.</span>' +
      (puede ? boton('ligarDoc', 'Agregar factura, boleta o nota de venta', 'bi-link-45deg', 'fm-btn-toca') : '') + '</div>';
  }

  function htmlDocs(inst) {
    var docs = inst.pan.documentos || [];
    if (!docs.length) return '';
    var h = '<div class="fm-sec-h"><h3>Documentos <small>' + docs.length + '</small></h3>' +
      '<span class="fm-help">Cada documento con sus líneas de servicio y despacho, y lo que aporta al cobro.</span></div>' +
      htmlContadores(inst);     /* 2026-10-25: los contadores viven acá, y solo los que dicen algo */
    /* 2026-10-08 (revisión): el «Cobré» de la OT sale de lo declarado (zz_monto / despacho), no de las líneas leídas en
       Random. Si no coinciden, se dice: dos números distintos en la misma pantalla sin explicación confunden. */
    var fin = (inst.rec && inst.rec.fin) || null, sumaAp = 0, hayAp = false;
    docs.forEach(function (d) { var a = aporteDoc(d); if (a.total !== undefined) { sumaAp += a.total; hayAp = true; } });
    if (fin && fin.cobra && fin.cobre && hayAp && Math.abs(sumaAp - (fin.cobre.hay ? Number(fin.cobre.total) || 0 : 0)) >= 1) {
      h += '<div class="fm-aviso"><i class="bi bi-exclamation-triangle-fill"></i> Las líneas de los documentos suman ' + clp(sumaAp) +
        ', pero lo cobrado en la cuenta de la OT es ' + (fin.cobre.hay ? clp(fin.cobre.total) : 'nada todavía (falta declararlo)') +
        '. Manda lo declarado en la OT: revisa el cobro del servicio.</div>';
    }
    var sal = inst.pan.saldo;
    if (sal && sal.excedido) {
      h += '<div class="fm-saldo-alerta" role="alert"><i class="bi bi-exclamation-octagon-fill"></i><div><b>Esta OT cobra más de lo que le queda a sus documentos</b>' +
        '<span>' + esc(sal.texto || 'Un mismo servicio de una factura no se puede cobrar en dos OT.') + '</span></div>' +
        ((inst.pan.puede_editar) ? boton('resolverSaldoOt', 'Resolver el saldo', 'bi-sliders', 'fm-btn-pri') : '') + '</div>';
    }
    return h + '<div class="fm-docs">' + docs.map(htmlDoc).join('') + '</div>';
  }

  function signo(n) { return (n > 0 ? '+' : '') + clp(n); }

  /* ═══ LO QUE NOS COSTÓ, DENTRO DE LA CUENTA ═════════════════════════════════════════════════════════════════════════════════
     2026-10-25 (Daniel, con la OT-2026-00247: «esto está bien, se puede mejorar es los costos: resumidos y con pocos clics»): la tarjeta aparte «Lo que
     nos costó» repetía lo que la cuenta ya dice (cuánto nos cobraron, por negocio). Ahora vive acá, en UN solo lugar para la plata: cada fila de
     «Nos cobraron» dice QUIÉN nos cobró (el técnico o proveedor) y, con un anexo vivo, lleva su candado (REGLA #34: el costo es el del anexo); los
     repuestos van debajo, uno por uno, con su origen; y lo que falta se dice en ámbar, en la misma fila. Nada de lo que mostraba se perdió (REGLA #4.2):
     solo dejó de estar en dos tarjetas. */
  function regsDe(inst) { return (inst.pan && inst.pan.costos && inst.pan.costos.registros) || []; }
  function regPorClave(inst, clave) { return regsDe(inst).filter(function (r) { return r.clave === clave; })[0] || null; }
  /* El candado del anexo vivo en las filas del técnico/proveedor y del despacho (REGLA #34). */
  function chipAnexo(inst, clave) {
    var ax = anexoVivo(inst);
    if (!ax || (clave !== 'tecnico' && clave !== 'despacho')) return '';
    return '<span class="fm-lock" title="El costo de la OT y el anexo del proveedor son un solo valor"><i class="bi bi-lock-fill"></i> Anexo N° ' + esc(ax.numero) + (ax.firmado ? ' · firmado' : ' · vigente') + '</span>';
  }
  function celdaMonto(v, falta) {
    if (falta) return '<span class="fm-pend">Falta declararlo</span>';
    return (v === null || v === undefined) ? '—' : clp(v);
  }
  /* Encabezado de fila: el rótulo, quién nos cobró (el «detalle» del registro), el candado del anexo y, si hay, una nota. */
  function thFila(inst, rotulo, reg, nota) {
    return '<th scope="row">' + esc(rotulo) + (reg && reg.detalle ? '<small class="fm-quien">' + esc(reg.detalle) + '</small>' : '') +
      (reg ? chipAnexo(inst, reg.clave) : '') + (nota ? '<small class="fm-nota">' + esc(nota) + '</small>' : '') + '</th>';
  }
  /* Cada repuesto instalado, con su origen (bodega, compra, manual) y su costo. */
  function filasRepuestos(inst, conCobro) {
    return regsDe(inst).filter(function (r) { return /^rep/.test(r.clave); }).map(function (r) {
      return '<tr class="fm-sub"><th scope="row">' + esc(String(r.rotulo || '').replace(/^Repuesto:\s*/, '')) + (r.detalle ? '<small class="fm-quien">' + esc(r.detalle) + '</small>' : '') + '</th>' +
        (conCobro ? '<td></td>' : '') + '<td data-l="Nos cobraron">' + celdaMonto(r.monto, r.falta) + '</td>' + (conCobro ? '<td></td>' : '') + '</tr>';
    }).join('');
  }
  function notaRepuestos(M) {
    var dsg = M.repuestos_desglose || {}, partes = [];
    if (dsg.bodega) partes.push('bodega ' + clp(dsg.bodega));
    if (dsg.compra) partes.push('compra ' + clp(dsg.compra));
    if (dsg.manual) partes.push('manual ' + clp(dsg.manual));
    if (M.repuestos_sin_costo) partes.push(M.repuestos_sin_costo + ' sin costo registrado');
    return partes.join(' · ');
  }

  /* El desglose por negocio. Con cobro: Cobramos / Nos cobraron / Queda para la instalación o el servicio, el despacho, los repuestos (si hay) y el
     total. Sin cobro (garantía, regalía, interno…) o sin lo cobrado declarado todavía: la misma tabla con solo «Lo que nos costó». */
  function htmlNegocios(inst) {
    var fin = inst.rec.fin, C = fin.cobre || {}, M = fin.me_cobraron || {}, Q = fin.queda || {};
    var rTec = regPorClave(inst, 'tecnico'), rDes = regPorClave(inst, 'despacho');
    var hayRep = M.repuestos > 0 || M.repuestos_sin_costo > 0;
    var kDes = M.despacho === null || M.despacho === undefined ? (M.falta_despacho ? null : 0) : M.despacho;
    var notaRep = hayRep ? notaRepuestos(M) : '';
    var h;
    if (fin.cobra && C.hay) {
      var fila = function (th, cob, cue, q, falta, tot) {
        var cls = falta ? '' : (q > 0 ? 'pos' : (q < 0 ? 'neg' : 'cero'));
        return '<tr' + (tot ? ' class="tot"' : '') + '>' + th + '<td data-l="Cobramos">' + clp(cob) + '</td><td data-l="Nos cobraron">' + cue + '</td>' +
          '<td data-l="Queda" class="' + cls + '">' + (falta ? '—' : signo(q)) + '</td></tr>';
      };
      h = '<table class="fm-neg"><thead><tr><th>Por negocio</th><th>Cobramos</th><th>Nos cobraron</th><th>Queda</th></tr></thead><tbody>' +
        fila(thFila(inst, 'Instalación o servicio', rTec, (C.servicio <= 0 && (M.tecnico || 0) > 0) ? 'se pagó el servicio y no se le cobró al cliente' : ''),
          C.servicio, celdaMonto(M.tecnico, M.falta_tecnico), Q.servicio, M.falta_tecnico) +
        fila(thFila(inst, 'Despacho', rDes, (C.despacho === 0 && (M.despacho || 0) > 0) ? 'se pagó despacho y no se le cobró al cliente' : ''),
          C.despacho, celdaMonto(kDes, M.falta_despacho), Q.despacho, M.falta_despacho);
      if (hayRep)
        h += fila(thFila(inst, 'Repuestos instalados', null, notaRep), 0, celdaMonto(M.repuestos > 0 ? M.repuestos : null, false), Q.repuestos, M.repuestos <= 0) + filasRepuestos(inst, true);
      h += fila('<th scope="row">Total</th>', C.total, M.falta_tecnico ? '—' : clp(M.total), Q.total, !Q.mostrar, true);
      return h + '</tbody></table>';
    }
    var fc = function (th, cue, tot) { return '<tr' + (tot ? ' class="tot"' : '') + '>' + th + '<td data-l="Nos cobraron">' + cue + '</td></tr>'; };
    h = '<table class="fm-neg fm-neg-c"><thead><tr><th>Lo que nos costó</th><th>Monto</th></tr></thead><tbody>' +
      fc(thFila(inst, 'Instalación o servicio', rTec, ''), celdaMonto(M.tecnico, M.falta_tecnico));
    if (rDes || (M.despacho || 0) > 0 || M.falta_despacho) h += fc(thFila(inst, 'Despacho', rDes, ''), celdaMonto(M.despacho, M.falta_despacho));
    if (hayRep) h += fc(thFila(inst, 'Repuestos instalados', null, notaRep), celdaMonto(M.repuestos > 0 ? M.repuestos : null, false)) + filasRepuestos(inst, false);
    h += fc('<th scope="row">Total</th>', M.falta_tecnico ? '—' : clp(M.total), true);
    return h + '</tbody></table>';
  }

  function pctTxt(p) { return p === null || p === undefined ? '' : String(p).replace('.', ',').replace('-', '−') + ' %'; }

  /* «De dónde sale lo cobrado», en palabras de Daniel: de la VD 10653: servicio $899.726 + despacho $553.460. Un renglón por
     documento que de verdad suma al cobro (los de referencia, dados de baja o sin líneas de servicio no entran). */
  function origenCobro(inst) {
    var partes = [];
    ((inst.pan && inst.pan.documentos) || []).forEach(function (d) {
      if (aporteDoc(d).cls !== 'si') return;
      var s = null, e = null;
      if (d.lineas) { s = sumaMontos(d.lineas.servicio); e = sumaMontos(d.lineas.despacho); }
      if (!s && !e) { s = d.zz_serv; e = d.zz_envio; }
      var p = [];
      if (s) p.push('servicio ' + clp(s));
      if (e) p.push('despacho ' + clp(e));
      if (p.length) partes.push('la ' + d.titulo + ': ' + p.join(' + '));
    });
    return partes;
  }

  /* LA CUENTA, en palabras de Daniel (2026-10-08: «dejemos cuánto ganamos, cuánto cobramos, cuánto nos cobraron, cuánto perdimos…
     todo bien detalladito»): «Cobramos $X» (servicio + despacho) · «Nos cobraron $Y» (técnico o proveedor + despacho + repuestos) y el
     resultado en grande y con color: «Ganamos $Z (n %)» o «Perdimos $Z»; si no se cobra (garantía, regalía, interno): «No se cobra ·
     nos costó $Y». Debajo, la mini-tabla por negocio y UNA línea por dato: de dónde sale lo cobrado, el valorizado de referencia, quién
     lo declaró y cuándo, y la autorización si la hay. La frase completa del modelo viaja en el tooltip.
     2026-10-25 (Daniel: «no tantas tarjetas… en una puedo sacar cuánto me cobraron, cuánto es mi margen, cuánto es el porcentaje»): es LA tarjeta:
     además de las cifras trae, adentro, el vínculo con el anexo del proveedor (el costo es UNO solo: REGLA #34) y el centro de costo al que se carga
     (una garantía, regalía o contrato SIEMPRE se carga a uno, así se puede reportar). La constancia de quién autorizó el $0 ya no se repite acá: la
     cuenta el paso ① del recorrido, con las mismas palabras (así tarjeta y recorrido no pueden contradecirse). */
  function htmlCuenta(inst, sinLigar) {
    var fin = inst.rec.fin, cobra = fin.cobra, M = fin.me_cobraron || {}, C = fin.cobre || {}, Q = fin.queda || {};
    var clase = (cobra && Q.mostrar && Q.total < 0) ? 'rojo' : (fin.clase || 'gris');
    var h = '<section class="fm-cta c-' + esc(clase) + '" aria-label="La cuenta de esta OT" title="' + esc(fin.frase || '') + '">';
    h += '<h4 class="fm-col-t"><i class="bi bi-calculator-fill"></i> La cuenta</h4>';
    /* 2026-10-25: dos mitades. A: las cifras (cobramos, nos cobraron —con quién y su anexo—, margen, de dónde sale). B: el anexo, el centro de costo y lo que se
       puede modificar. En un bloque ancho con el recorrido plegado van lado a lado (REGLA #16); si no, una debajo de la otra. */
    h += '<div class="fm-cta-in"><div class="fm-cta-a">';
    /* El semáforo («Margen sano», «Pérdida», «Falta…») va al final de la misma línea (baja si no cabe). */
    var sem = (cobra || M.falta_tecnico) ? '<span class="fm-sem">' + esc(fin.label || '') + '</span>' : '';
    h += '<div class="fm-res">';
    if (cobra) {
      h += '<div class="fm-res-i"><small>Cobramos</small><b>' + (C.hay ? clp(C.total) : '—') + '</b></div>' +
        '<div class="fm-res-i"><small>Nos cobraron</small><b>' + (M.falta_tecnico ? '—' : clp(M.total)) + '</b></div>';
      var rot, val, pie = '';
      if (Q.mostrar && Q.total > 0) { rot = 'Ganamos'; val = clp(Q.total); pie = pctTxt(Q.pct); }
      else if (Q.mostrar && Q.total < 0) { rot = 'Perdimos'; val = clp(Math.abs(Q.total)); pie = pctTxt(Q.pct); }
      else if (Q.mostrar) { rot = 'Resultado'; val = '$0'; pie = 'ni ganamos ni perdimos'; }
      else { rot = 'Resultado'; val = '—'; pie = 'falta un dato para calcularlo'; }
      h += '<div class="fm-res-i q"><small>' + rot + '</small><b>' + val + (pie ? '<em>' + (Q.mostrar && Q.total !== 0 ? '(' + esc(pie) + ')' : esc(pie)) + '</em>' : '') + '</b></div>' + sem;
    } else {
      var motivo = (inst.rec.cobro_cero || {}).motivo_txt || String(fin.cobertura_txt || '').split(':')[0];
      h += '<div class="fm-res-i q"><small>No se cobra</small><b>' + (M.falta_tecnico ? 'falta declarar lo que nos costó' : 'nos costó ' + clp(M.total)) + '</b></div>' +
        '<span class="fm-motivo">' + esc(motivo) + '</span>' + sem;
    }
    h += '</div>';
    h += htmlNegocios(inst);
    var det = [];
    if (cobra) {
      var orig = origenCobro(inst);
      if (orig.length) det.push({ o: true, t: 'Lo cobrado sale de ' + esc(orig.join(' · ')) });
      else if (C.hay && fin.cobre && fin.cobre.fuente) det.push({ o: true, t: 'Lo cobrado: ' + esc(fin.cobre.fuente) + ' (no viene de un documento de Random)' });
    }
    if (fin.valorizado && fin.valorizado.monto)
      det.push({ t: 'Valorizado (referencia): <b>' + clp(fin.valorizado.monto) + '</b> · no se suma a lo cobrado' });
    var declaro = '';
    try { declaro = inst.el.getAttribute('data-declaro') || ''; } catch (e) { declaro = ''; }
    if (declaro) det.push({ t: esc(declaro.replace(/^Declaradas por/, 'Costos declarados por')) });
    var aprob = (inst.rec.autorizaciones || []).filter(function (a) { return a.estado === 'aprobada'; })[0];
    if (cobra && aprob && !esInterna(inst))
      det.push({ t: 'Autorización: ' + esc(aprob.tipo_txt) + (aprob.motivo_txt ? ' · ' + esc(aprob.motivo_txt) : '') + ', aprobada por ' + esc(aprob.resuelto_por_nombre) + (aprob.resuelto_at ? ' el ' + esc(aprob.resuelto_at) : '') });
    if (det.length) h += '<ul class="fm-det">' + det.map(function (x) { return '<li' + (x.o ? ' class="o"' : '') + '>' + x.t + '</li>'; }).join('') + '</ul>';
    (fin.avisos || []).forEach(function (a) { h += '<div class="fm-aviso">' + esc(a) + '</div>'; });
    /* Repuestos instalados sin costo registrado: se dice acá (en ámbar) solo si la tabla no lo dijo ya en su fila de repuestos. */
    var cst = inst.pan.costos || {};
    if (cst.sin_costo && !(M.repuestos > 0 || M.repuestos_sin_costo > 0)) h += '<div class="fm-aviso">' + cst.sin_costo + ' repuesto(s) instalado(s) sin costo registrado: no suman todavía.</div>';
    h += '</div><div class="fm-cta-b">';
    /* El vínculo con el anexo del proveedor (REGLA #34): en la ficha, el servidor lo dibuja con su macro y acá se ADOPTA (ver adoptarAnexo);
       en el modal de cierre solo se dice en una línea de qué anexo sale lo que nos cobraron. */
    var ax = anexoVivo(inst);
    if (ax && inst.modo === 'ficha') h += '<div class="fm-anx" data-fm-anx hidden></div>';
    else if (ax) h += '<p class="fm-anx-l"><i class="bi bi-lock-fill"></i> Lo que nos cobraron es el del <b>Anexo N° ' + esc(ax.numero) + '</b> (' + (ax.firmado ? 'firmado' : 'vigente') + ', ' + esc(ax.total_txt) + ')' +
      (ax.vinculo === 'alineado' ? '' : (ax.vinculo === 'sin_declarar' ? ' y la OT aún no lo declara' : ' y la OT dice ' + esc(ax.costo_txt || 'otro valor')) + ': ' +
        /* Con la OT abierta el botón alinea ahí mismo (y vuelve a abrir este cierre); cerrada, solo se corrige desde la ficha. */
        (ax.abierta && global.otdCostoDesdeAnexo ? '<span class="fm-anx-b">' + botonChip('usarAnexo', 'Usar el valor del anexo', 'bi-link-45deg', true, 'Dejar el costo de la OT igual al del Anexo N° ' + ax.numero) + '</span>'
          : (ax.abierta ? 'alinéalo desde la ficha de la OT' : 'se corrige desde la ficha de la OT') + '.')) + '</p>';
    return h + htmlCentro(inst) + htmlAcciones(inst, sinLigar) + '</div></div></section>';
  }

  /* Centro de costo con BOTONES (Daniel 2026-10-08: «hazlo con botones, dinámico, bonito»): el mismo diseño que los
     rectángulos del asistente de creación (.o2m-cc), un solo componente para la ficha y el modal de cierre. */
  var CC_META = {
    sstt: { cls: 'cc-sstt', ico: 'bi-wrench-adjustable-circle-fill', sub: 'El costo es nuestro' },
    logistica: { cls: 'cc-log', ico: 'bi-truck', sub: 'Despacho o bodega' },
    comercial: { cls: 'cc-com', ico: 'bi-briefcase-fill', sub: 'Convenio o venta' },
    marketing: { cls: 'cc-mkt', ico: 'bi-megaphone-fill', sub: 'Campaña o contenido' }
  };

  function htmlCentro(inst) {
    var c = inst.pan.centro, puede = inst.pan.puede_editar, fin = (inst.rec && inst.rec.fin) || {};
    /* 2026-10-25 (Daniel: «garantías o coberturas asociadas a centro de costo y poder reportar esas situaciones»): una OT que no se cobra
       (garantía, regalía, arriendo o contrato) SIEMPRE se carga a un centro de costo, y el Excel de OT suma lo que costaron por cobertura y centro. */
    var sinCobro = !fin.cobra && !esInterna(inst);
    var h = '<section class="fm-cen"><h4>' + (sinCobro ? 'Se carga a este centro de costo' : 'Centro de costo') + ' <small class="' + (c.valor ? 'ok' : 'mal') + '">' + (c.valor ? 'declarado' : 'obligatorio') + '</small></h4>';
    h += '<div class="fm-cc-grid" data-fm-cc-grid role="group" aria-label="Centro de costo">' + c.opciones.map(function (o) {
      var m = CC_META[o.v] || { cls: '', ico: 'bi-circle', sub: '' };
      var on = o.v === c.valor;
      return '<button type="button" class="fm-cc ' + m.cls + (on ? ' on' : '') + '" data-fm-cc="' + esc(o.v) + '" aria-pressed="' + (on ? 'true' : 'false') + '" title="' + esc(o.n + (m.sub ? ' · ' + m.sub : '')) + '"' +
        (puede ? '' : ' disabled') + '><i class="bi ' + m.ico + '"></i><b>' + esc(o.n) + '</b></button>';
    }).join('') + '</div>' +
      '<span class="fm-help">' + (inst.pan.cerrada ? 'OT cerrada: el superadministrador lo corrige con «Corregir finanzas» (queda registrado).'
        : (sinCobro ? 'Lo que no se le cobra al cliente (garantía, regalía, arriendo o contrato) siempre se carga a un centro de costo: así se puede reportar cuánto cuesta cada cobertura. Se guarda al elegirlo y queda registrado.'
          : 'Se guarda al elegirlo y queda registrado. Sin centro de costo la OT no se cierra.')) + '</span></section>';
    return h;
  }

  function htmlHistorial(inst) {
    var a = inst.rec.autorizaciones || [];
    if (!a.length) return '';
    var h = '<div class="fm-sec-h"><h3>Autorizaciones de esta OT <small>' + a.length + '</small></h3></div><ul class="fm-hist">';
    a.forEach(function (x) {
      h += '<li class="' + esc(x.estado) + '"><b>' + esc(x.tipo_txt) + (x.motivo_txt ? ' · ' + esc(x.motivo_txt) : '') + ' <span>' + esc(x.estado_txt) + '</span></b>' +
        '<small>La pidió ' + esc(x.solicitado_por_nombre) + ' el ' + esc(x.solicitado_at) + '</small>' +
        '<p>' + esc(x.argumento) + '</p>' +
        (x.resuelto_por_nombre ? '<small class="res">' + esc(x.estado === 'rechazada' ? 'Rechazada' : 'Autorizada') + ' por ' + esc(x.resuelto_por_nombre) + ' el ' + esc(x.resuelto_at) + (x.comentario ? ': ' + esc(x.comentario) : '') + '</small>' : '') +
        '</li>';
    });
    return h + '</ul>';
  }

  /* «Modificar»: dentro de La cuenta (antes vivía en la tarjeta «Lo que nos costó», que ya no existe aparte). `sinLigar`: la línea «Sin documentos aún» ya ofrece agregar.
     2026-10-25: los botones dependen del ESTADO de la OT (nada que ya está hecho se ofrece de nuevo):
       · $0 ya autorizado (garantía, regalía…) → solo «Sí se cobra: quitar…»; «No se cobra: declarar $0» solo si todavía no hay autorización;
       · con un anexo vivo el costo del proveedor ES el del anexo (REGLA #34): se dice y la salida es «Usar el valor del anexo» (o emitir uno nuevo
         si está firmado), no escribirlo a mano (el servidor lo rechazaría con COSTO_ES_DEL_ANEXO). */
  function accionesLista(inst, sinLigar) {
    var p = inst.pan, items = [], ayudas = [];
    /* 2026-10-08: trabajo interno sin cliente. Ni agregar documento, ni «declarar $0», ni pedir autorización: no aplican. */
    if (esInterna(inst)) {
      if (p.puede_editar) items.push({ act: 'corregirProv', label: 'Corregir lo que cobró el proveedor', ico: 'bi-pencil-square' });
      return { items: items, ayudas: ayudas };
    }
    var ax = anexoVivo(inst);
    if (!sinLigar) items.push({ act: 'ligarDoc', label: 'Agregar factura, boleta o nota de venta', ico: 'bi-link-45deg' });
    if (p.cerrada) {
      items.push({ act: 'pedirCierre', label: p.superadmin ? 'Autorizar sin documento' : 'Pedir autorización a ' + CAMPANA_DAN, ico: 'bi-shield-lock' });
      ayudas.push('<span class="fm-help fm-help-ancho">OT cerrada (evidencia): solo se regulariza el documento y la plata; el estado, las firmas y las fechas no se tocan.</span>');
    } else if (p.puede_editar) {
      /* 2026-10-19 (OT-2026-00276, Aarón no podía cerrarla): si la OT está leída como $0 (garantía, regalía…), la
         salida para volver a cobrarla vive AQUÍ, donde se mira la plata. Antes solo existía más abajo en el modal y
         solo si la modalidad decía garantía (no si la marcaba `cubierto_por`). */
      var cobAct = ((inst.rec && inst.rec.fin) || {}).cobertura;
      var yaAutorizada = !!(inst.rec && inst.rec.resumen && inst.rec.resumen.aut_usada_id);
      if (['garantia', 'sin_costo', 'regalia', 'arriendo_leasing'].indexOf(cobAct) >= 0)
        items.push({ act: 'quitarCero', label: cobAct === 'garantia' ? 'Sí se cobra: quitar la garantía' : 'Sí se cobra: quitar el $0', ico: 'bi-arrow-counterclockwise' });
      if (!yaAutorizada) items.push({ act: 'pedirCero', label: 'No se cobra: declarar $0', ico: 'bi-shield-check' });
      if (ax) {
        /* («Usar el valor del anexo» ya está en el paso ④ y en la franja del anexo de la cuenta: aquí solo se dice por qué no se escribe a mano.) */
        ayudas.push('<span class="fm-help fm-help-ancho"><i class="bi bi-lock-fill"></i> El costo del proveedor lo fija el Anexo N° ' + esc(ax.numero) + ' (' + (ax.firmado ? 'firmado' : 'vigente') + '): ' +
          (ax.firmado ? 'para cambiarlo, emite un anexo nuevo' : 'para cambiarlo, corrige el monto en el anexo') + ' y la OT lo sigue sola.</span>');
      } else {
        items.push({ act: 'corregirProv', label: 'Corregir lo que cobró el proveedor', ico: 'bi-pencil-square' });
      }
      items.push({ act: 'pedirCierre', label: p.superadmin ? 'Autorizar cierre sin documento' : 'Pedir autorización a ' + CAMPANA_DAN, ico: 'bi-shield-lock' });
    }
    return { items: items, ayudas: ayudas };
  }
  /* Qué botones de «Modificar» RESUELVEN algo que hoy falta (el paso del recorrido está en «falta»): esos se ven siempre, en azul de «te toca». */
  function actosQueResuelven(inst) {
    var s = {};
    ((inst.rec && inst.rec.pasos) || []).forEach(function (p) {
      if (p.estado !== 'falta') return;
      if (p.n === 1) s.pedirCero = 1;          /* el $0 todavía no tiene la autorización de Daniel */
      if (p.n === 2) s.ligarDoc = 1;           /* falta la factura o boleta */
      if (p.n === 4) s.corregirProv = 1;       /* falta lo que cobró el proveedor (con un anexo vivo la salida es «Usar el valor del anexo», en el paso) */
      if (p.n === 6 && p.accion !== 'pedir_aut_perdida') s.pedirCierre = 1;   /* el cierre pide la autorización (la de pérdida tiene su propio botón en el paso) */
    });
    /* Con la factura por agregar, «autorizar el cierre sin documento» es la EXCEPCIÓN: lo que toca es agregarla (la excepción queda en «Más opciones»). */
    if (s.ligarDoc) delete s.pedirCierre;
    return s;
  }
  /* «Modificar»: dentro de La cuenta. 2026-10-25 (Daniel: «resumidos y con pocos clics»): si la OT ya tiene su factura y la cuenta está completa NO se
     muestran botones sueltos: todo lo excepcional (declarar $0, autorizar el cierre sin documento, agregar otro documento, corregir el costo) va detrás de
     UN botón «Más opciones». Si algo falta, el botón que lo resuelve queda a la vista, en azul («te toca»). */
  function htmlAcciones(inst, sinLigar) {
    var p = inst.pan;
    if (!(p.puede_editar || (p.cerrada && p.puede_regularizar))) return '';
    var L = accionesLista(inst, sinLigar);
    if (!L.items.length && !L.ayudas.length) return '';
    var toca = actosQueResuelven(inst), vis = '', oc = '';
    L.items.forEach(function (b) {
      if (toca[b.act]) vis += boton(b.act, b.label, b.ico, 'fm-btn-toca');
      else oc += boton(b.act, b.label, b.ico);
    });
    var hayMas = !!(oc || L.ayudas.length), abierto = !!inst.masAbierto, id = 'fmMas' + inst.id;
    return '<div class="fm-acciones"><span>Modificar</span>' + vis +
      (hayMas ? '<button type="button" class="fm-btn fm-btn-mas" data-fm-act="masTog" aria-expanded="' + abierto + '" aria-controls="' + id + '">' +
        '<i class="bi bi-three-dots"></i> Más opciones <i class="bi bi-chevron-' + (abierto ? 'up' : 'down') + '"></i></button>' : '') + '</div>' +
      (hayMas ? '<div class="fm-mas-p" id="' + id + '"' + (abierto ? '' : ' hidden') + '>' + oc + L.ayudas.join('') + '</div>' : '');
  }

  /* El vínculo costo ↔ anexo (REGLA #34) lo dibuja el servidor con su macro de siempre (templates/ot2/_anexo_vinculo.html, con sus botones) DEBAJO del
     bloque; acá se ADOPTA dentro de la tarjeta de la cuenta, justo junto a «Nos cobraron», para que se lea donde está el número (2026-10-25). El nodo se
     guarda la primera vez: al repintar, el bloque se reescribe entero y el nodo se vuelve a poner en su lugar (no se duplica ni se pierde). */
  function adoptarAnexo(inst) {
    try {
      if (inst.modo !== 'ficha') return;
      var slot = inst.el.querySelector && inst.el.querySelector('[data-fm-anx]');
      if (!slot) return;
      if (!inst.anexoNodo) inst.anexoNodo = document.getElementById('otdAnexoVinculoFicha');
      if (inst.anexoNodo) { slot.appendChild(inst.anexoNodo); slot.hidden = false; }
    } catch (e) { }
  }

  function pintar(inst) {
    var p = inst.pan, rec = inst.rec;
    var hayDocs = (p.documentos || []).length > 0;
    var puedeLigar = !esInterna(inst) && !!(p.puede_editar || (p.cerrada && p.puede_regularizar));
    /* 2026-10-25 (Daniel, con la OT-2026-00354: «contradicciones y muy poco explicativo… no tantas tarjetas»). De arriba abajo: encabezado en una fila
       (un solo camino para tocar la plata) → UNA frase que dice si la OT está lista para cerrar o qué le falta → dos columnas: «El recorrido» (seis
       filas con el texto específico de cada paso) | «La cuenta» (UNA tarjeta con las cifras, el anexo y el centro de costo) → documentos | lo que nos
       costó (REGLA #16: la fila de arriba mide parecido a cada lado) → pie de superadmin. Sin documentos: una línea y «Lo que nos costó» a todo el ancho. */
    /* 2026-10-25 (Daniel, con la OT-2026-00247: «los costos, resumidos y con pocos clics»): «Lo que nos costó» y «Modificar» viven dentro de La cuenta (UN solo
       lugar para la plata); abajo, a ancho completo, los documentos y las autorizaciones. El recorrido se pliega en una línea cuando nada falta. */
    var plegar = recorridoPlegable(inst);
    var recorrido = htmlRecorrido(inst, plegar), cuenta = htmlCuenta(inst, puedeLigar && !hayDocs);
    var cuerpo = (hayDocs ? '' : htmlSinDocs(inst)) +
      '<div class="fm-main"><section class="fm-sec">' + htmlDocs(inst) + '</section><section class="fm-sec">' + htmlHistorial(inst) + '</section></div>';
    inst.el.innerHTML =
      htmlCabecera(inst) +
      htmlResumen(inst) +
      /* Con algo por hacer: La cuenta | El recorrido (dos columnas; en celular la plata primero). Plegado: la línea del recorrido arriba y La cuenta debajo. */
      '<div class="fm-top' + (plegar ? ' plegado' : '') + '">' + (plegar ? recorrido + cuenta : cuenta + recorrido) + '</div>' +
      cuerpo +
      htmlPie(inst);
    adoptarAnexo(inst);
    var r = inst.el.querySelector('#fmRechazo');
    if (r && inst.rechazo && inst.rechazo.__nuevo) { inst.rechazo.__nuevo = false; try { r.scrollIntoView({ behavior: 'smooth', block: 'center' }); } catch (e) { } }
  }

  /* ── Acciones ────────────────────────────────────────────────────────────────────────────────────────────── */
  function despuesDeEscribir(inst, msg) {
    if (msg) toast(msg, 'success');
    /* Fuera de la ficha (p. ej. la bandeja Regularizar) quien llama decide qué refrescar. */
    if (typeof inst.hecho === 'function') { inst.hecho(msg); return; }
    /* La ficha y el modal de cierre se pintan en el servidor con lo que había al abrir: se recargan para que
       todo diga lo mismo. En el modal de cierre se vuelve a abrir solo. */
    try { if (inst.modo === 'modal') sessionStorage.setItem('otfReabrirCierre_' + inst.vid, '1'); } catch (e) { }
    setTimeout(function () { location.reload(); }, 800);
  }

  function ligarDoc(inst, pre) {
    if (esInterna(inst)) { toast(TXT_INTERNA, 'info'); return Promise.resolve(); }
    return dialogo({
      titulo: 'Agregar un documento a la OT',
      intro: 'Se busca en Random (solo lectura) y se valida el RUT del cliente. Si ya hay una nota de venta, la factura la da de baja y las dos quedan visibles.',
      ok: 'Buscar y agregar',
      campos: [
        { k: 'tipo', label: 'Tipo de documento', tipo: 'select', valor: (pre && pre.tipo) || 'FCV', opciones: [
          { v: 'FCV', n: 'Factura' }, { v: 'FCE', n: 'Factura exenta' }, { v: 'BLV', n: 'Boleta' }, { v: 'BLE', n: 'Boleta exenta' },
          { v: 'NVV', n: 'Nota de venta' }, { v: 'COT', n: 'Cotización interna (COT-000045)' }] },
        { k: 'numero', label: 'Número', req: true, tipo: 'texto', valor: (pre && pre.numero) || '', placeholder: 'Ej: 11439' },
        /* 2026-10-25: la sugerencia de «Trazabilidad en Random» (static/ot_doc_cadena.js) llega con la etiqueta ya escrita. */
        { k: 'etiqueta', label: 'Para qué es (opcional)', tipo: 'texto', valor: (pre && pre.etiqueta) || '', placeholder: 'Ej: Instalación' }
      ]
    }).then(function (v) {
      if (!v) return;
      if (v.tipo === 'COT') return enviarDoc(inst, { origen: 'cotizacion', cotizacion: v.numero, etiqueta: v.etiqueta });
      var cuerpo = { origen: 'erp', tipo: v.tipo, numero: v.numero, etiqueta: v.etiqueta };
      return revisarAntesDeAgregar(inst, v.tipo, v.numero).then(function (seguir) { if (seguir) return enviarDoc(inst, cuerpo); });
    });
  }

  /* 🚦 2026-10-12 (Daniel: «la OT le debe avisar al usuario, ya que si no sabe puede duplicar el servicio con un
     documento con el saldo ya consumido»): antes de agregar, el semáforo del documento en palabras. Consumido, doble
     cobro o una nota de venta que ya se facturó → hay que confirmar a conciencia; disponible → solo un aviso breve.
     Si no se puede revisar, sigue (el servidor igual cuida el saldo al agregar). */
  function revisarAntesDeAgregar(inst, tipo, numero) {
    var n = String(numero || '').replace(/[^0-9]/g, '');
    if (!n) return Promise.resolve(true);
    toast('Revisando el saldo del documento…', 'info');
    return api('/ot/api/saldo-servicio/' + encodeURIComponent(tipo) + '/' + encodeURIComponent(n) + '?excluir_vid=' + inst.vid).then(function (r) {
      var av = r.ok && r.j && r.j.aviso;
      if (!av || !av.semaforo) return true;
      var hayAmbar = (av.avisos || []).some(function (a) { return a.nivel === 'ambar'; });
      var dg = (av.desglose && av.desglose.texto) ? ' ' + av.desglose.texto : '';
      if (!av.alerta && av.semaforo.nivel !== 'ambar' && !hayAmbar) {
        toast(av.semaforo.titulo + ': ' + av.semaforo.texto + dg, av.semaforo.nivel === 'verde' ? 'success' : 'info');
        return true;
      }
      return global.ilusConfirm({
        title: av.semaforo.titulo + ' · ' + (r.j.titulo || (tipo + ' ' + n)),
        message: av.semaforo.texto,
        sub: (av.avisos || []).map(function (a) { return a.texto; }).join(' ') + dg,
        okLabel: 'Agregar de todas formas', cancelLabel: 'No agregar', danger: av.semaforo.nivel === 'rojo'
      });
    });
  }
  function enviarDoc(inst, cuerpo) {
    toast('Buscando el documento en Random…', 'info');
    return api('/ot/api/' + inst.vid + '/documentos/regularizar', { method: 'POST', body: cuerpo }).then(function (r) {
      if (r.ok) return despuesDeEscribir(inst, 'Documento agregado. Quedó registrado con tu nombre.');
      var j = r.j || {};
      if (j.error_codigo === 'RUT_NO_COINCIDE') {
        return dialogo({ titulo: 'El RUT no coincide', intro: esc(j.error || ''), ok: 'Agregar de todas formas',
          campos: [{ k: 'justificacion', label: '¿Por qué corresponde este documento?', tipo: 'area', req: true, min: 10,
            placeholder: 'Ej: factura emitida a la casa matriz del cliente' }] }).then(function (v) {
          if (v) { cuerpo.justificacion = v.justificacion; return enviarDoc(inst, cuerpo); }
        });
      }
      if (j.error_codigo === 'POSIBLE_DUPLICADO') {
        return global.ilusConfirm({ title: 'Parece el mismo cobro', message: j.error || '', sub: 'Si es la factura de esa nota de venta, sumarla cobraría dos veces.',
          okLabel: 'Es otro cobro, agregar', cancelLabel: 'Cancelar', danger: true }).then(function (ok) {
          if (ok) { cuerpo.confirmar_duplicado = true; return enviarDoc(inst, cuerpo); }
        });
      }
      if (j.error_codigo === 'ZZ_SALDO_CONSUMIDO') {
        return resolverSaldo(inst, j, function (extra) { for (var k in extra) cuerpo[k] = extra[k]; return enviarDoc(inst, cuerpo); });
      }
      toast(j.error || 'No se pudo agregar el documento.', 'error');
    });
  }

  /* ── Saldo consumido: nunca un callejón sin salida ───────────────────────────────────────────────────────── */
  var ICONO_ACC = { tomar_saldo: 'bi-arrow-down-circle-fill', ligar_factura: 'bi-link-45deg', pasar_garantia: 'bi-shield-check',
    pedir_autorizacion: 'bi-shield-lock' };
  function dialogoAcciones(o) {
    return new Promise(function (resolve) {
      var ov = document.createElement('div');
      ov.className = 'fm-ov'; ov.setAttribute('role', 'dialog'); ov.setAttribute('aria-modal', 'true');
      ov.innerHTML = '<div class="fm-dlg"><div class="fm-dlg-h"><b>' + esc(o.titulo || '') + '</b><button type="button" class="fm-x" aria-label="Cerrar">&times;</button></div>' +
        '<div class="fm-dlg-b"><p class="fm-dlg-intro">' + (o.intro || '') + '</p><div class="fm-acc-list">' +
        (o.acciones || []).map(function (a) {
          return '<button type="button" class="fm-acc" data-a="' + esc(a.tipo) + '"><i class="bi ' + (ICONO_ACC[a.tipo] || 'bi-arrow-right-circle') + '"></i><span>' + esc(a.label) + '</span></button>';
        }).join('') + '</div></div>' +
        '<div class="fm-dlg-f"><button type="button" class="fm-btn" data-r="no">Ahora no</button></div></div>';
      var host = document.body;
      try {
        var tka = document.getElementById('tkaModal');
        host = (tka && tka.classList.contains('is-open')) ? tka : (document.querySelector('.modal.show') || document.body);
      } catch (e) { host = document.body; }
      host.appendChild(ov);
      var cerrado = false;
      function cerrar(v) {
        if (cerrado) return; cerrado = true;
        document.removeEventListener('keydown', alTecla, true);
        if (ov.parentNode) ov.parentNode.removeChild(ov);
        resolve(v);
      }
      function alTecla(e) { if (e.key === 'Escape') { e.stopPropagation(); cerrar(null); } }
      document.addEventListener('keydown', alTecla, true);
      ov.addEventListener('click', function (e) {
        if (e.target === ov) return;
        if (e.target.closest('.fm-x') || e.target.closest('[data-r="no"]')) return cerrar(null);
        var b = e.target.closest('[data-a]');
        if (b) cerrar(b.getAttribute('data-a'));
      });
      var primero = ov.querySelector('.fm-acc'); if (primero) setTimeout(function () { try { primero.focus(); } catch (e) { } }, 60);
    });
  }

  /* El servidor rechazó un cobro porque supera el saldo de las líneas de sus documentos: ofrece las cuatro salidas.
     `reintento(extra)` (opcional) repite la petición original con {tomar_saldo:true}; sin él, «tomar solo el saldo»
     baja el cobro de la OT con POST /ot/api/<vid>/saldo-servicio/tomar. */
  function resolverSaldo(inst, err, reintento) {
    err = err || {};
    var acc = err.acciones || [];
    if (!acc.length) { toast(err.error || 'El cobro supera el saldo disponible del documento.', 'warning'); return Promise.resolve(); }
    return dialogoAcciones({ titulo: 'Ese cobro supera el saldo del documento', intro: esc(err.error || ''), acciones: acc }).then(function (tipo) {
      if (!tipo) return;
      if (tipo === 'tomar_saldo') {
        if (reintento) return reintento({ tomar_saldo: true });
        return api('/ot/api/' + inst.vid + '/saldo-servicio/tomar', { method: 'POST', body: {} }).then(function (r) {
          if (r.ok) return despuesDeEscribir(inst, (r.j && r.j.mensaje) || 'El cobro quedó en el saldo disponible.');
          toast((r.j && r.j.error) || 'No se pudo ajustar el cobro al saldo.', 'error');
        });
      }
      if (tipo === 'ligar_factura') return ligarDoc(inst);
      if (tipo === 'pasar_garantia') return pedirAutorizacion(inst, 'cobro_cero');
      if (tipo === 'pedir_autorizacion') return pedirExceder(inst, err);
    });
  }

  /* 🧭 2026-10-25 (REGLA #47, Daniel: «¿Quién lo asume? ¿Por qué? Autorizaron: toda esa trazabilidad la necesito»): cada solicitud de autorización lleva su
     «¿Por qué?» clasificado (arriendo, descuadre de precio, pérdida, costo asumido por Marketing o Comercial…), aparte del argumento de quien pide. El
     catálogo lo administra el superadministrador (/ot/intenciones) y el servidor ofrece solo lo que esa persona puede elegir; si no responde, queda «Otro
     motivo» para no dejar la solicitud sin salida (el servidor la valida igual). El sistema SUGIERE según el centro de costo; la persona decide. */
  var INTEN_RESPALDO = { items: [{ clave: 'otro', nombre: 'Otro motivo', familia_cero: null }], sugerida: null };
  function cargarIntenciones(uso, centro) {
    var q = '/ot/api/intenciones?uso=' + encodeURIComponent(uso) + (centro ? '&centro=' + encodeURIComponent(centro) : '');
    return api(q).then(function (r) {
      return (r.ok && r.j && r.j.items && r.j.items.length) ? r.j : INTEN_RESPALDO;
    });
  }
  function campoPorQue(cat, etiqueta) {
    var sug = cat.sugerida;
    return { k: 'intencion', label: etiqueta || '¿Por qué?', tipo: 'select', req: true, valor: sug ? sug.clave : '',
      opciones: [{ v: '', n: 'Elige el motivo…' }].concat((cat.items || []).map(function (i) { return { v: i.clave, n: i.nombre }; })),
      ayuda: sug ? sug.origen + ': puedes cambiarla.' : 'Queda guardado junto a la autorización: quién lo asume y por qué.' };
  }
  function familiaDe(cat, clave) {
    var it = (cat.items || []).filter(function (i) { return i.clave === clave; })[0];
    return (it && it.familia_cero) || null;
  }

  function pedirExceder(inst, err) {
    var sa = inst.pan && inst.pan.superadmin, sp = {};
    (err.excesos || []).forEach(function (e) { sp[e.categoria] = e.pedido; });
    var centroEx = inst.pan && inst.pan.centro && inst.pan.centro.valor;
    return cargarIntenciones('exceder_saldo', centroEx).then(function (cat) {
      return dialogo({
        titulo: 'Cobrar más que el saldo del documento',
        intro: sa ? 'Eres superadministrador: tu decisión queda registrada como autorización, con tu nombre y la hora.'
          : 'Esto le llega a ' + CAMPANA_DAN + ' a su celular, con tu argumento. Mientras responde, el cobro no puede pasar del saldo.',
        ok: sa ? 'Autorizar y registrar' : 'Pedir autorización a ' + CAMPANA_DAN,
        campos: [campoPorQue(cat, '¿Por qué se cobra distinto del documento?'), { k: 'argumento', label: 'Argumento', tipo: 'area', req: true, min: 30,
          placeholder: 'Ej: la factura trae una sola línea de instalación pero cubre dos equipos instalados en visitas distintas' }]
      });
    }).then(function (v) {
      if (!v) return;
      return api('/ot/api/autorizaciones', { method: 'POST', body: { tipo: 'exceder_saldo', visita_id: inst.vid, argumento: v.argumento, intencion: v.intencion, saldo_pedido: sp } }).then(function (r) {
        if (!r.ok) { toast((r.j && r.j.error) || 'No se pudo pedir la autorización.', 'error'); return; }
        if (!sa) return despuesDeEscribir(inst, 'Se pidió autorización a ' + CAMPANA_DAN + '.');
        return api('/ot/api/autorizaciones/' + r.j.id + '/aprobar', { method: 'POST', body: { comentario: 'Autorizado por quien lo declara (superadministrador).' } }).then(function (a) {
          if (a.ok) return despuesDeEscribir(inst, 'Autorizado y registrado con tu nombre.');
          toast((a.j && a.j.error) || 'Se creó la solicitud pero no se pudo aprobar.', 'error');
        });
      });
    });
  }

  function corregirProv(inst) {
    var reg = {}; (inst.pan.costos.registros || []).forEach(function (r) { reg[r.clave] = r; });
    return dialogo({
      titulo: 'Lo que cobró el técnico o proveedor',
      intro: 'Úsalo cuando hubo una desviación o un error del proveedor. Queda en la bitácora: el valor anterior, el nuevo, quién lo cambió y por qué.',
      ok: 'Guardar y registrar',
      campos: [
        { k: 'costo_proveedor', label: 'Lo que cobró por la instalación o el servicio', tipo: 'monto', valor: reg.tecnico && reg.tecnico.monto != null ? Math.round(reg.tecnico.monto) : '', ayuda: 'Escribe 0 si lo hizo un técnico propio y no se le paga aparte.' },
        { k: 'costo_despacho', label: 'Costo del despacho (si hubo)', tipo: 'monto', valor: reg.despacho && reg.despacho.monto != null ? Math.round(reg.despacho.monto) : '' },
        { k: 'motivo', label: 'Motivo del cambio', tipo: 'area', req: true, min: 10, placeholder: 'Ej: el proveedor cobró 2 visitas por un error de su facturación' }
      ]
    }).then(function (v) {
      if (!v) return;
      if (v.costo_proveedor === '' && v.costo_despacho === '') { toast('Escribe al menos un monto.', 'warning'); return; }
      var body = { motivo: v.motivo };
      if (v.costo_proveedor !== '') body.costo_proveedor = v.costo_proveedor;
      if (v.costo_despacho !== '') body.costo_despacho = v.costo_despacho;
      return api('/ot/api/' + inst.vid + '/costo-proveedor', { method: 'POST', body: body }).then(function (r) {
        if (r.ok) return despuesDeEscribir(inst, r.j.sin_cambios ? 'Ya estaba con esos valores.' : 'Costo corregido. Quedó registrado con tu nombre y el motivo.');
        toast((r.j && r.j.error) || 'No se pudo guardar.', 'error');
      });
    });
  }

  function pedirAutorizacion(inst, tipo) {
    if (esInterna(inst)) { toast(TXT_INTERNA, 'info'); return Promise.resolve(); }
    var cero = tipo === 'cobro_cero', sa = inst.pan.superadmin;
    var cat = INTEN_RESPALDO;
    return cargarIntenciones(cero ? 'cobertura' : 'cerrar_sin_documento', inst.pan.centro.valor).then(function (c) {
      cat = c;
      var campos = [];
      /* 🧭 REGLA #47: el «Motivo del $0» ahora es el «¿Por qué?» del catálogo (las tres familias de siempre + las que cree el superadministrador); la familia
         contable (garantía, regalía, arriendo/leasing) la fija la clasificación elegida. «Garantía» sigue llamándose Garantía. */
      campos.push(campoPorQue(cat, cero ? 'Motivo del $0 (¿por qué?)' : '¿Por qué se cierra sin factura ni boleta?'));
      campos.push({ k: 'argumento', label: 'Argumento', tipo: 'area', req: true, min: 30,
        placeholder: cero ? 'Ej: equipo con 8 meses de uso, falla de fábrica cubierta por la garantía del proveedor' : 'Ej: servicio sin costo acordado con gerencia; no habrá factura' });
      campos.push({ k: 'centro_costo', label: 'Centro de costo (quién lo asume)', req: true, tipo: 'select', valor: inst.pan.centro.valor || '',
        opciones: [{ v: '', n: 'Elige…' }].concat(inst.pan.centro.opciones) });
      if (cero) campos.push({ k: 'valorizado_clp', label: 'Cuánto vale (sugerido, opcional)', tipo: 'monto', ayuda: 'Solo de referencia: lo que de verdad cuenta es lo que nos costó.' });
      return dialogo({
        titulo: cero ? 'Cobrar $0 en esta OT' : 'Cerrar sin documento',
        intro: sa ? 'Eres superadministrador: tu decisión queda registrada como autorización, con tu nombre y la hora.'
          : 'Esto le llega a ' + CAMPANA_DAN + ' a su celular. Mientras responde, la OT queda «Esperando autorización de ' + CAMPANA_DAN + '».',
        ok: sa ? 'Autorizar y registrar' : 'Pedir autorización a ' + CAMPANA_DAN, campos: campos
      });
    }).then(function (v) {
      if (!v) return;
      var body = { tipo: tipo, visita_id: inst.vid, argumento: v.argumento, centro_costo: v.centro_costo, intencion: v.intencion };
      if (cero) { body.motivo = familiaDe(cat, v.intencion) || v.intencion; if (v.valorizado_clp) body.valorizado_clp = Number(v.valorizado_clp); }
      return api('/ot/api/autorizaciones', { method: 'POST', body: body }).then(function (r) {
        if (!r.ok) { toast((r.j && r.j.error) || 'No se pudo pedir la autorización.', 'error'); return; }
        if (!sa) return despuesDeEscribir(inst, 'Se pidió autorización a ' + CAMPANA_DAN + '.');
        return api('/ot/api/autorizaciones/' + r.j.id + '/aprobar', { method: 'POST', body: { comentario: 'Autorizado por quien lo declara (superadministrador).' } }).then(function (a) {
          if (a.ok) return despuesDeEscribir(inst, 'Autorizado y registrado con tu nombre.');
          toast((a.j && a.j.error) || 'Se creó la solicitud pero no se pudo aprobar.', 'error');
        });
      });
    });
  }

  /* 📒 2026-10-11 (Daniel: «te va a indicar si la factura va a pérdida y eso iría autorizado por mí»): una OT que se
     cobra y cierra con lo cobrado por debajo de lo que nos cobraron necesita la autorización 'cerrar_con_perdida'. El
     diálogo muestra la cuenta completa para que el argumento se escriba viendo los números; Daniel la recibe en su
     celular y autoriza HASTA esa pérdida (si después crece, se vuelve a pedir). */
  function pedirPerdida(inst) {
    if (esInterna(inst)) { toast(TXT_INTERNA, 'info'); return Promise.resolve(); }
    var sa = inst.pan && inst.pan.superadmin;
    var pr = (inst.rec && inst.rec.perdida) || (inst.rechazo && inst.rechazo.perdida) || {};
    var cuenta = (pr.perdida ? '<span class="fm-perdida-cuenta"><span>Cobramos <b>' + clp(pr.cobre) + '</b></span><span>Nos cobraron <b>' + clp(pr.me_cobraron) + '</b></span>' +
      '<span class="mal">Se pierden <b>' + clp(pr.perdida) + '</b></span></span>' : '');
    var centroP = inst.pan && inst.pan.centro && inst.pan.centro.valor;
    return cargarIntenciones('cerrar_con_perdida', centroP).then(function (cat) {
      return dialogo({
        titulo: 'Cerrar esta OT con pérdida',
        intro: cuenta + (sa ? 'Eres superadministrador: tu decisión queda registrada como autorización, con tu nombre y la hora.'
          : 'Esto le llega a ' + CAMPANA_DAN + ' a su celular con la cuenta de la OT. Mientras no responda, la OT no se puede cerrar.'),
        ok: sa ? 'Autorizar y registrar' : 'Pedir autorización a ' + CAMPANA_DAN,
        campos: [campoPorQue(cat, '¿Quién asume la pérdida / por qué?'), { k: 'argumento', label: 'Por qué se cierra con pérdida', tipo: 'area', req: true, min: 30,
          placeholder: 'Ej: el proveedor cobró un despacho extra por acceso difícil que no se le cobró al cliente; se acordó absorberlo' }]
      });
    }).then(function (v) {
      if (!v) return;
      return api('/ot/api/autorizaciones', { method: 'POST', body: { tipo: 'cerrar_con_perdida', visita_id: inst.vid, argumento: v.argumento, intencion: v.intencion } }).then(function (r) {
        if (!r.ok) { toast((r.j && r.j.error) || 'No se pudo pedir la autorización.', 'error'); return; }
        if (!sa) return despuesDeEscribir(inst, 'Se pidió autorización a ' + CAMPANA_DAN + '.');
        return api('/ot/api/autorizaciones/' + r.j.id + '/aprobar', { method: 'POST', body: { comentario: 'Autorizado por quien lo declara (superadministrador).' } }).then(function (a) {
          if (a.ok) return despuesDeEscribir(inst, 'Autorizado y registrado con tu nombre.');
          toast((a.j && a.j.error) || 'Se creó la solicitud pero no se pudo aprobar.', 'error');
        });
      });
    });
  }

  /* «Sí se cobra»: deshace un $0 (garantía, regalía…) con motivo, por el camino de siempre (/cobertura, que deja la
     constancia en la bitácora y limpia el $0). No toca estado ni firmas. Después, el cierre pide lo cobrado con su
     documento como cualquier OT que se cobra. */
  function quitarCero(inst) {
    if (esInterna(inst)) { toast(TXT_INTERNA, 'info'); return Promise.resolve(); }
    return dialogo({
      titulo: 'Esta OT sí se cobra',
      intro: 'La OT deja de estar en garantía (o en $0) y vuelve a cobrarse. Para cerrarla va a necesitar lo cobrado ' +
        'con su documento de Random. Queda registrado con tu nombre y el motivo.',
      ok: 'Quitar la garantía',
      campos: [
        { k: 'motivo', label: 'Por qué vuelve a cobrarse', tipo: 'area', req: true, min: 10,
          placeholder: 'Ej: el servicio se cobró en la factura del cliente; no corresponde garantía' }
      ]
    }).then(function (v) {
      if (!v) return;
      return api('/mantenciones/api/visitas/' + inst.vid + '/cobertura', { method: 'POST', body: { garantia_aplica: false, motivo: v.motivo } })
        .then(function (r) {
          if (r.ok) return despuesDeEscribir(inst, 'Listo: la OT vuelve a cobrarse.');
          toast((r.j && r.j.error) || 'No se pudo quitar la garantía.', 'error');
        });
    });
  }

  function guardarCentro(inst, valor, btn) {
    if (!valor) return;
    if (btn) { btn.classList.add('pulsa'); }
    api('/ot/api/' + inst.vid + '/centro-costo', { method: 'POST', body: { centro_costo: valor } }).then(function (r) {
      if (r.ok) return despuesDeEscribir(inst, 'Centro de costo guardado.');
      toast((r.j && r.j.error) || 'No se pudo guardar el centro de costo.', 'error');
      cargar(inst);
    });
  }

  /* «Declarar lo que cobré» sin salir del modal de cierre: servicio y despacho escritos a mano, con motivo. Es el
     mismo POST que la tarjeta (/ot/api/finanzas/<vid>): rotula el cobro «escrito a mano», deja el motivo y queda en la
     bitácora (finanzas_declaradas). No toca estado ni firmas. */
  function declararCobro(inst) {
    if (esInterna(inst)) { toast(TXT_INTERNA, 'info'); return Promise.resolve(); }
    var fin = (inst.rec && inst.rec.fin) || {};
    var anotado = fin.precio_anotado;
    return dialogo({
      titulo: 'Declarar lo que cobré',
      intro: 'Escribe lo que se le cobró al cliente por esta OT.' + (anotado ? ' Hay un precio anotado de <b>' + clp(anotado) + '</b> sin documento: no cuenta como cobro hasta que lo declares aquí o agregues el documento.' : '') +
        ' Si hay factura o boleta de Random, lo mejor es agregarla.',
      ok: 'Guardar y registrar',
      campos: [
        { k: 'zz_monto', label: 'Cobro del servicio', tipo: 'monto', req: true, valor: anotado ? Math.round(anotado) : '' },
        { k: 'zz_envio_monto', label: 'Cobro del despacho (si hubo)', tipo: 'monto', valor: '' },
        { k: 'zz_motivo_manual', label: 'Por qué va escrito a mano', tipo: 'area', req: true, min: 10, placeholder: 'Ej: el documento no trae una línea de servicio; se cobró según la cotización 45' }
      ]
    }).then(function (v) {
      if (!v) return;
      var body = { zz_monto: v.zz_monto, zz_motivo_manual: v.zz_motivo_manual };
      if (v.zz_envio_monto !== '') body.zz_envio_monto = v.zz_envio_monto;
      function enviar() {
        return api('/ot/api/finanzas/' + inst.vid, { method: 'POST', body: body }).then(function (r) {
          if (r.ok) return despuesDeEscribir(inst, 'Cobro declarado. Quedó registrado con tu nombre y el motivo.');
          if (r.j && r.j.error_codigo === 'ZZ_SALDO_CONSUMIDO') {
            return resolverSaldo(inst, r.j, function (extra) { for (var k in extra) body[k] = extra[k]; return enviar(); });
          }
          toast((r.j && r.j.error) || 'No se pudo guardar el cobro.', 'error');
        });
      }
      return enviar();
    });
  }

  function irFinanzas(inst) {
    var modal = inst.el.closest('.modal');
    try { if (modal && global.bootstrap) { var m = global.bootstrap.Modal.getInstance(modal); if (m) m.hide(); } } catch (e) { }
    setTimeout(function () {
      if (global.otdIrAFinanzas) { global.otdIrAFinanzas(); return; }
      var c = document.getElementById('otdCardFinanzas');
      if (!c || !c.offsetParent) c = document.getElementById('otdCardMotor') || c;
      if (c) c.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }, modal ? 350 : 0);
  }

  var ACCIONES = {
    recargar: function (inst) { return cargar(inst); },
    ligarDoc: function (inst) { return ligarDoc(inst); },
    corregirProv: function (inst) { return corregirProv(inst); },
    pedirCero: function (inst) { return pedirAutorizacion(inst, 'cobro_cero'); },
    quitarCero: function (inst) { return quitarCero(inst); },
    pedirCierre: function (inst) { return pedirAutorizacion(inst, 'cerrar_sin_documento'); },
    pedirPerdida: function (inst) { return pedirPerdida(inst); },
    irFinanzas: function (inst) { return irFinanzas(inst); },
    /* REGLA #34: «Usar el valor del anexo» (deja el costo de la OT igual al del anexo vivo, con constancia en la bitácora). Vive en la ficha de la OT. */
    usarAnexo: function (inst) {
      if (!global.otdCostoDesdeAnexo) { toast('Alinea el costo con el anexo desde la ficha de la OT.', 'info'); return; }
      /* En el modal de cierre, la página se recarga al alinear: el aviso hace que el cierre se vuelva a abrir solo. Si la persona cancela (la página
         NO se recarga), el aviso se retira para que un recargo posterior no abra el cierre sin que nadie lo pida. */
      var k = 'otfReabrirCierre_' + inst.vid;
      try { if (inst.modo === 'modal') sessionStorage.setItem(k, '1'); } catch (e) { }
      return Promise.resolve(global.otdCostoDesdeAnexo(inst.vid)).then(function () {
        setTimeout(function () { try { sessionStorage.removeItem(k); } catch (e) { } }, 3000);
      });
    },
    declararCobro: function (inst) { return declararCobro(inst); },
    /* El cierre fue rechazado por saldo consumido: sus salidas se ofrecen en el mismo modal. */
    resolverSaldo: function (inst) { return resolverSaldo(inst, inst.rechazo); },
    /* El panel del motor detectó que lo cobrado supera el saldo: mismas salidas, con los datos del panorama. */
    resolverSaldoOt: function (inst) {
      var s = inst.pan.saldo || {};
      return resolverSaldo(inst, { error: s.texto || '', excesos: s.excesos || [], acciones: s.acciones || [] });
    },
    enfocarCentro: function (inst) { var s = inst.el.querySelector('[data-fm-cc-grid]'); if (s) { s.scrollIntoView({ behavior: 'smooth', block: 'center' }); var b = s.querySelector('[data-fm-cc]'); if (b) b.focus(); } },
    /* 2026-10-25: abrir/cerrar el recorrido plegado y «Más opciones». Se repinta el bloque (el estado vive en la instancia, así «Actualizar» no lo cierra) y el foco
       vuelve al mismo botón (teclado y lectores de pantalla). */
    recTog: function (inst) { return alternar(inst, 'recAbierto', 'recTog'); },
    masTog: function (inst) { return alternar(inst, 'masAbierto', 'masTog'); }
  };
  function alternar(inst, clave, act) {
    inst[clave] = !inst[clave];
    pintar(inst);
    try { var b = inst.el.querySelector('[data-fm-act="' + act + '"]'); if (b && b.focus) b.focus(); } catch (e) { }
  }

  function montar(el) {
    if (el.__fmInst) return el.__fmInst;
    var inst = { el: el, vid: parseInt(el.getAttribute('data-vid'), 10), modo: el.getAttribute('data-modo') || 'ficha', id: montados.length + 1, rechazo: null };
    el.__fmInst = inst;
    /* Botones extra del encabezado (solo superadmin: Corregir finanzas y OT con finanzas dudosas): los dibuja la
       plantilla del servidor, con su permiso, dentro de <template data-fm-extra>; se leen ANTES del primer pintado. */
    try { var ex = el.querySelector && el.querySelector('template[data-fm-extra]'); inst.extra = ex ? ex.innerHTML : ''; } catch (e) { inst.extra = ''; }
    /* 2026-10-25: «Corregir finanzas» (el botón) y «OT con finanzas dudosas» (el enlace) se separan para acomodarlos según el estado de la OT: la
       corrección va arriba si la OT está cerrada y, si no, en el pie junto a una línea que explica qué hace cada uno. */
    try {
      var mb = /<button[\s\S]*?<\/button>/.exec(inst.extra || ''), ma = /<a\s[\s\S]*?<\/a>/.exec(inst.extra || '');
      inst.btnCorregir = mb ? mb[0] : '';
      inst.lnkDudosas = ma ? ma[0] : '';
    } catch (e) { inst.btnCorregir = ''; inst.lnkDudosas = ''; }
    try { var ed = el.querySelector && el.querySelector('template[data-fm-edit]'); inst.editar = ed ? ed.innerHTML : ''; } catch (e) { inst.editar = ''; }
    montados.push(inst);
    el.addEventListener('click', function (e) {
      var b = e.target.closest('[data-fm-act]');
      if (!b || !el.contains(b)) return;
      var act = b.getAttribute('data-fm-act');
      var fn = ACCIONES[act];
      if (fn && SOLO_CLIENTE[act] && esInterna(inst)) { e.preventDefault(); toast(TXT_INTERNA, 'info'); return; }
      if (fn) { e.preventDefault(); fn(inst); }
    });
    el.addEventListener('click', function (e) {
      var b = e.target.closest('[data-fm-cc]');
      if (!b || !el.contains(b) || b.disabled) return;
      e.preventDefault();
      guardarCentro(inst, b.getAttribute('data-fm-cc'), b);
    });
    var modal = el.closest('.modal');
    if (modal) {
      modal.addEventListener('show.bs.modal', function () { cargar(inst); });
      if (modal.classList.contains('show')) cargar(inst);
    } else {
      cargar(inst);
    }
    return inst;
  }

  function iniciar() {
    var els = document.querySelectorAll('[data-fin-motor]');
    for (var i = 0; i < els.length; i++) montar(els[i]);
  }

  global.OTFinMotor = {
    montar: montar,
    iniciar: iniciar,
    refrescar: function () { montados.forEach(cargar); },
    /* Para otras pantallas (bandeja Regularizar): los mismos diálogos, sin montar el motor. */
    ligarDocumento: function (vid, o) {
      o = o || {};
      return ligarDoc({ vid: vid, modo: 'ficha', hecho: o.hecho || function () { }, pan: {} }, o.pre);
    },
    pedirAutorizacion: function (vid, tipo, o) {
      o = o || {};
      return pedirAutorizacion({ vid: vid, modo: 'ficha', hecho: o.hecho || function () { },
        pan: { superadmin: !!o.superadmin, centro: { valor: o.centro || '', opciones: o.opciones || [] } } }, tipo);
    },
    /* El servidor rechazó un cobro por saldo consumido (tarjeta de Finanzas, «Otros documentos», «Asociar factura»
       del modal de cierre): las mismas cuatro salidas. `o.reintento(extra)` repite la petición original con
       {tomar_saldo:true}; `o.superadmin` hace que «pedir autorización» quede aprobada al declararla. */
    resolverSaldoExterno: function (vid, err, o) {
      o = o || {};
      return resolverSaldo({ vid: vid, modo: 'ficha', hecho: o.hecho, pan: { superadmin: !!o.superadmin, centro: { valor: '', opciones: [] } } },
        err, o.reintento);
    },
    /* El servidor rechazó el cierre: cada rechazo trae su acción ({tipo,label,url}) y se resuelve acá mismo. */
    alCerrarRechazado: function (d) {
      var hecho = false;
      montados.forEach(function (inst) {
        if (inst.modo !== 'modal' || !inst.rec) return;
        d.__nuevo = true;
        inst.rechazo = d;
        pintar(inst);
        hecho = true;
      });
      return hecho;
    }
  };

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', iniciar);
  else iniciar();
})(window);

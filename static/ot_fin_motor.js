/* 🧭 MOTOR DE FINANZAS Y DOCUMENTOS DE LA OT — un solo componente, dos lugares.
   Daniel, 2026-10-07: "este módulo lo necesito potente, con espacios ergonómicos donde se pueda visualizar todo el
   panorama: cuántas facturas, cuántos servicios… siempre hay que pasar todas completas, pero con un detalle… y
   finalmente en el segundo modal, cuando firma el autorizador que se va a cerrar la OT, necesito que ese mismo
   motor funcione porque tengo que tener una segunda opción de modificar".

   Se monta en cada <div data-fin-motor data-vid="N" data-modo="ficha|modal"> (parcial templates/ot2/_fin_motor.html):
   la ficha de la OT y el modal de aprobación y cierre usan ESTE archivo, idéntico.
   Lee:  GET /ot/api/<vid>/recorrido  (6 pasos, cuenta, autorizaciones)  +  GET /ot/api/<vid>/panorama (documentos
         completos con sus líneas del ERP, contadores, lo que nos costó).
   Escribe (todo con registro de quién y cuándo): POST /ot/api/<vid>/documentos, POST /ot/api/<vid>/centro-costo,
         POST /ot/api/<vid>/costo-proveedor, POST /ot/api/autorizaciones (+ aprobar si quien pide es superadmin).
   Sin alert/confirm/prompt nativos (REGLA #1): ilusToast/ilusConfirm y un diálogo propio para formularios. */
(function (global) {
  'use strict';

  var CAMPANA_DAN = 'Daniel';
  var montados = [];

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
      document.body.appendChild(ov);
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
        inst.el.innerHTML = '<div class="fm-err"><i class="bi bi-exclamation-triangle-fill"></i> ' +
          esc((r[0].j && r[0].j.error) || (r[1].j && r[1].j.error) || 'No se pudo leer las finanzas y documentos de esta OT.') +
          ' <button type="button" class="fm-btn" data-fm-act="recargar">Reintentar</button></div>';
        return;
      }
      inst.rec = r[0].j;
      inst.pan = r[1].j;
      pintar(inst);
    });
  }

  /* ── Pintado ─────────────────────────────────────────────────────────────────────────────────────────────── */
  var ICONO_PASO = { hecho: 'bi-check-lg', falta: 'bi-exclamation-lg', no_aplica: 'bi-dash-lg', pendiente: 'bi-hourglass-split' };

  function boton(act, label, icono, extra) {
    return '<button type="button" class="fm-btn' + (extra ? ' ' + extra : '') + '" data-fm-act="' + act + '"><i class="bi ' + icono + '"></i> ' + esc(label) + '</button>';
  }

  function htmlPasos(inst) {
    var rec = inst.rec, puede = inst.pan.puede_editar || (inst.pan.cerrada && inst.pan.puede_regularizar);
    var pend = rec.solicitud_pendiente;
    var html = '';
    if (pend) {
      html += '<div class="fm-espera"><i class="bi bi-hourglass-split"></i><div><b>Esperando autorización de ' + CAMPANA_DAN + ' desde ' + esc(pend.solicitado_at) + '</b>' +
        '<span>' + esc(pend.tipo_txt) + (pend.motivo_txt ? ' · ' + esc(pend.motivo_txt) : '') + ' · la pidió ' + esc(pend.solicitado_por_nombre) + '. ' +
        'Hasta que responda, la OT no se puede cerrar.</span></div>' +
        '<a class="fm-btn" href="' + esc(pend.url) + '">Ver la solicitud</a></div>';
    }
    if (inst.rechazo) {
      html += '<div class="fm-rechazo" id="fmRechazo"><i class="bi bi-x-octagon-fill"></i><div><b>No se pudo cerrar la OT</b><span>' + esc(inst.rechazo.error || '') + '</span></div>' +
        accionDeRechazo(inst.rechazo) + '</div>';
    }
    html += '<ol class="fm-pasos">';
    (rec.pasos || []).forEach(function (p) {
      var act = '';
      if (p.estado === 'falta' && puede) {
        if (p.n === 1) act = boton('pedirCero', 'Pedir autorización del $0', 'bi-shield-check');
        else if (p.n === 2) act = boton('ligarDoc', 'Ligar factura o boleta', 'bi-link-45deg', 'fm-btn-pri') + boton('pedirCierre', 'Pedir autorización a ' + CAMPANA_DAN, 'bi-shield-lock');
        else if (p.n === 3 && inst.pan.puede_editar) act = boton('irFinanzas', 'Declarar lo que cobré', 'bi-cash-coin');
        else if (p.n === 4 && inst.pan.puede_editar) act = boton('corregirProv', 'Declarar lo que cobró el proveedor', 'bi-pencil-square');
        else if (p.n === 6 && !pend) act = boton('pedirCierre', 'Pedir autorización a ' + CAMPANA_DAN, 'bi-shield-lock');
      }
      html += '<li class="fm-paso ' + esc(p.estado) + (p.clase ? ' c-' + esc(p.clase) : '') + '">' +
        '<span class="fm-circ"><i class="bi ' + (ICONO_PASO[p.estado] || 'bi-circle') + '"></i></span>' +
        '<div class="fm-paso-t"><small>Paso ' + p.n + '</small><b>' + esc(p.titulo) + '</b></div>' +
        '<p>' + esc(p.texto || '') + '</p>' + (act ? '<div class="fm-paso-a">' + act + '</div>' : '') + '</li>';
    });
    return html + '</ol>';
  }

  function accionDeRechazo(d) {
    var a = d && d.accion; if (!a) return '';
    var m = { ligar_factura: 'ligarDoc', pedir_autorizacion: 'pedirCierre', declarar_cobro: 'irFinanzas',
      declarar_centro: 'enfocarCentro', declarar_costo_proveedor: 'corregirProv' }[a.tipo];
    if (a.tipo === 'esperar_autorizacion') return '<a class="fm-btn fm-btn-pri" href="' + esc(a.url || '/ot/autorizaciones') + '">Ver la solicitud</a>';
    if (a.tipo === 'actualizar_anexo') return '<span class="fm-help">' + esc(a.label || '') + '</span>';
    if (!m) return a.label ? '<span class="fm-help">' + esc(a.label) + '</span>' : '';
    return boton(m, a.label || 'Resolver', 'bi-arrow-right-circle', 'fm-btn-pri');
  }

  function htmlContadores(inst) {
    var c = inst.pan.contadores || {};
    var baja = c.notas_venta_dadas_de_baja || 0;
    function tile(n, titulo, sub, cls) {
      return '<div class="fm-tile ' + (cls || '') + (n ? '' : ' cero') + '"><b>' + n + '</b><span>' + esc(titulo) + '</span>' + (sub ? '<small>' + esc(sub) + '</small>' : '') + '</div>';
    }
    return '<div class="fm-tiles">' +
      tile(c.facturas || 0, 'Facturas y boletas', 'Documentos que cobran', 'ok') +
      tile(c.notas_venta || 0, 'Notas de venta', baja ? (baja + ' ya dada' + (baja > 1 ? 's' : '') + ' de baja por factura') : ((c.notas_venta || 0) ? 'Falta ligar la factura' : ''), 'nv') +
      tile(c.cotizaciones || 0, 'Cotizaciones', 'Referencia', '') +
      tile(c.servicios || 0, 'Servicios', 'Líneas de servicio (ZZ)', 'ser') +
      tile(c.despachos || 0, 'Despachos', 'Líneas de despacho', 'des') +
      tile(c.otros || 0, 'Otros documentos', 'Guías y otros', '') +
      '</div>';
  }

  function filaLinea(l, conCant) {
    return '<tr><td class="fm-d">' + (conCant && l.cantidad ? '<span class="fm-cant">' + esc(l.cantidad % 1 ? l.cantidad : Math.round(l.cantidad)) + ' ×</span> ' : '') + esc(l.descripcion) +
      '<small>' + esc(l.sku) + '</small></td><td class="fm-n">' + clp(l.monto) + '</td></tr>';
  }
  function grupoLineas(titulo, arr, cls, conCant) {
    if (!arr || !arr.length) return '';
    return '<div class="fm-grp ' + cls + '"><h5>' + esc(titulo) + ' <span>' + arr.length + '</span></h5><table class="fm-lt"><tbody>' +
      arr.map(function (l) { return filaLinea(l, conCant); }).join('') + '</tbody></table></div>';
  }

  var RUT_TXT = { ok: ['ok', 'El RUT coincide con el del cliente'], justificado: ['aviso', 'RUT distinto, justificado'],
    distinto: ['mal', 'RUT distinto, sin justificar'], sin_verificar: ['gris', 'RUT sin verificar'], no_aplica: ['gris', 'No aplica'] };

  function htmlDoc(d) {
    var rut = RUT_TXT[d.rut_estado] || RUT_TXT.sin_verificar;
    var baja = d.dada_de_baja_por;
    var cls = 'fm-doc cat-' + esc(d.categoria) + (baja ? ' baja' : '');
    var meta = '<dl class="fm-meta">' +
      '<div><dt>Fecha de emisión</dt><dd>' + (esc(d.fecha) || '—') + '</dd></div>' +
      '<div><dt>RUT</dt><dd class="' + rut[0] + '">' + rut[1] + (d.rut ? ' · ' + esc(d.rut) : '') + (d.rut_justif ? '<small>' + esc(d.rut_justif) + '</small>' : '') + '</dd></div>' +
      '<div><dt>Lo ligó</dt><dd>' + (esc(d.asociado_por) || 'No consta (documento anterior)') + (d.asociado_at ? '<small>' + esc(d.asociado_at) + '</small>' : '') + '</dd></div>' +
      '<div><dt>Monto del documento</dt><dd>' + clp(d.monto) + '</dd></div>' +
      (d.etiqueta ? '<div><dt>Etiqueta</dt><dd>' + esc(d.etiqueta) + '</dd></div>' : '') + '</dl>';
    var nota = '';
    if (baja) nota = '<div class="fm-baja"><i class="bi bi-arrow-repeat"></i> Dada de baja por la factura <b>' + esc(baja.titulo) + '</b>. Las dos quedan visibles.</div>';
    else if (d.da_de_baja && d.da_de_baja.length) nota = '<div class="fm-baja ok"><i class="bi bi-check-circle-fill"></i> Dio de baja la nota de venta <b>' + d.da_de_baja.map(function (x) { return esc(x.titulo); }).join(', ') + '</b>.</div>';
    var lineas = '';
    if (d.origen === 'cotizacion') {
      lineas = '<div class="fm-nolin">Cotización interna: sirve de referencia, no de cobro.</div>';
    } else if (d.lineas) {
      var ln = d.lineas;
      lineas = '<div class="fm-lineas">' + grupoLineas('Servicio', ln.servicio, 'ser', false) + grupoLineas('Despacho', ln.despacho, 'des', false) +
        grupoLineas('Productos', ln.productos, 'pro', true) +
        '<div class="fm-ltot">Suma de las líneas (neto): <b>' + clp(ln.total) + '</b></div></div>';
      if (!ln.servicio.length && !ln.despacho.length && !ln.productos.length) lineas = '<div class="fm-nolin">Random no devolvió líneas para este documento.</div>';
    } else if (d.lineas_omitidas) {
      lineas = '<div class="fm-nolin">Hay muchos documentos: las líneas de este se leen en su ficha de Random.</div>';
    } else if (d.tido && ['GDV', 'COV'].indexOf(d.tido) < 0) {
      lineas = '<div class="fm-nolin"><i class="bi bi-wifi-off"></i> No pudimos leer las líneas en Random ahora. El documento sigue declarado en la OT.</div>';
    }
    return '<article class="' + cls + '"><header><span class="fm-chip">' + esc(d.tipo_txt) + '</span><h4>' + esc(d.titulo) + '</h4>' +
      '<span class="fm-cuenta">' + esc(cuentaTxt(d)) + '</span>' +
      (d.es_principal ? '<span class="fm-pri">Principal</span>' : '') + '</header>' + meta + nota + lineas + '</article>';
  }
  function cuentaTxt(d) {
    return { servicio: 'Cobro del servicio', despacho: 'Cobro del despacho',
      nota_venta: 'Nota de venta: promesa de cobro', cotizacion: 'Cotización: solo referencia',
      referencia_garantia: 'Referencia de la garantía: no se cobra' }[d.cuenta] || (d.es_cobro ? 'Documento de cobro' : 'Referencia');
  }

  function htmlDocs(inst) {
    var docs = inst.pan.documentos || [];
    var h = '<div class="fm-sec-h"><span class="fm-num">2</span><h3>Todos los documentos de la OT <small>' + docs.length + '</small></h3></div>';
    if (!docs.length) {
      h += '<div class="fm-vacio"><i class="bi bi-file-earmark-x"></i><b>Esta OT no tiene ningún documento declarado.</b>' +
        '<span>Liga una factura, boleta, nota de venta o cotización; o pide la autorización de ' + CAMPANA_DAN + '.</span></div>';
    } else {
      h += '<div class="fm-docs">' + docs.map(htmlDoc).join('') + '</div>';
    }
    return h;
  }

  function htmlCostos(inst) {
    var c = inst.pan.costos, puede = inst.pan.puede_editar;
    var h = '<div class="fm-sec-h"><span class="fm-num">3</span><h3>Lo que nos costó</h3>' +
      (puede ? boton('corregirProv', 'Corregir lo que cobró el proveedor', 'bi-pencil-square') : '') + '</div>' +
      '<table class="fm-costos"><tbody>';
    (c.registros || []).forEach(function (r) {
      h += '<tr class="' + (r.falta ? 'falta' : '') + '"><td class="fm-d"><b>' + esc(r.rotulo) + '</b><small>' + esc(r.detalle) + '</small></td>' +
        '<td class="fm-n">' + (r.falta ? '<span class="fm-pend">Falta declararlo</span>' : clp(r.monto)) + '</td></tr>';
    });
    h += '<tr class="tot"><td>Total que nos costó</td><td class="fm-n">' + clp(c.total) + '</td></tr></tbody></table>';
    if (c.sin_costo) h += '<div class="fm-aviso">' + c.sin_costo + ' repuesto(s) instalado(s) sin costo registrado: no suman todavía.</div>';
    return h;
  }

  function htmlCuenta(inst) {
    var fin = inst.rec.fin, cc = inst.rec.cobro_cero || {};
    var cobra = fin.cobra;
    var h = '<div class="fm-sec-h"><span class="fm-num">4</span><h3>La cuenta de esta OT</h3></div>';
    if (cobra) {
      var q = fin.queda;
      h += '<div class="fm-cuenta-g c-' + esc(fin.clase) + '">' +
        '<div><small>Cobré</small><b>' + clp(fin.cobre.total) + '</b><span>servicio ' + clp(fin.cobre.servicio) + ' + despacho ' + clp(fin.cobre.despacho) + '</span></div>' +
        '<i>−</i><div><small>Me cobraron</small><b>' + clp(fin.me_cobraron.total) + '</b><span>técnico ' + clp(fin.me_cobraron.tecnico || 0) + ' + despacho ' + clp(fin.me_cobraron.despacho || 0) + ' + repuestos ' + clp(fin.me_cobraron.repuestos) + '</span></div>' +
        '<i>=</i><div class="q"><small>Queda</small><b>' + (q.mostrar ? clp(q.total) : '—') + '</b><span>' + esc(q.mostrar && q.pct != null ? String(q.pct).replace('.', ',') + ' %' : fin.label) + '</span></div></div>';
    } else {
      var motivo = cc.motivo_txt || String(fin.cobertura_txt || '').split(':')[0];
      var centro = inst.pan.centro.nombre || 'sin centro de costo';
      h += '<div class="fm-cero c-info"><b>$0</b><span>' + esc(motivo) + ' · nos costó <b>' + clp(fin.me_cobraron.total) + '</b> · centro ' + esc(centro) +
        (fin.valorizado && fin.valorizado.monto ? ' · valorizada en ' + clp(fin.valorizado.monto) : '') + '</span>' +
        '<p class="fm-const">' + (cc.constancia ? esc(cc.constancia) : '<em>Sin autorización de ' + CAMPANA_DAN + ' todavía.</em>') + '</p></div>';
    }
    h += '<div class="fm-frase">' + esc(fin.frase || '') + '</div>';
    (fin.avisos || []).forEach(function (a) { h += '<div class="fm-aviso">' + esc(a) + '</div>'; });
    return h;
  }

  function htmlCentro(inst) {
    var c = inst.pan.centro, puede = inst.pan.puede_editar;
    var h = '<div class="fm-sec-h"><span class="fm-num">5</span><h3>Centro de costo <small class="' + (c.valor ? 'ok' : 'mal') + '">' + (c.valor ? 'declarado' : 'obligatorio') + '</small></h3></div>';
    h += '<div class="fm-centro"><select id="fmCentro' + inst.id + '" data-fm-centro ' + (puede ? '' : 'disabled') + '>' +
      '<option value="">Elige el centro de costo…</option>' + c.opciones.map(function (o) {
        return '<option value="' + esc(o.v) + '"' + (o.v === c.valor ? ' selected' : '') + '>' + esc(o.n) + '</option>';
      }).join('') + '</select>' +
      '<span class="fm-help">' + (inst.pan.cerrada ? 'OT cerrada: el superadministrador lo corrige con «Corregir finanzas» (queda registrado).' : 'Se guarda al elegirlo y queda registrado. Sin centro de costo la OT no se cierra.') + '</span></div>';
    return h;
  }

  function htmlHistorial(inst) {
    var a = inst.rec.autorizaciones || [];
    if (!a.length) return '';
    var h = '<div class="fm-sec-h"><span class="fm-num">6</span><h3>Autorizaciones de esta OT <small>' + a.length + '</small></h3></div><ul class="fm-hist">';
    a.forEach(function (x) {
      h += '<li class="' + esc(x.estado) + '"><b>' + esc(x.tipo_txt) + (x.motivo_txt ? ' · ' + esc(x.motivo_txt) : '') + ' <span>' + esc(x.estado_txt) + '</span></b>' +
        '<small>La pidió ' + esc(x.solicitado_por_nombre) + ' el ' + esc(x.solicitado_at) + '</small>' +
        '<p>' + esc(x.argumento) + '</p>' +
        (x.resuelto_por_nombre ? '<small class="res">' + esc(x.estado === 'rechazada' ? 'Rechazada' : 'Autorizada') + ' por ' + esc(x.resuelto_por_nombre) + ' el ' + esc(x.resuelto_at) + (x.comentario ? ': ' + esc(x.comentario) : '') + '</small>' : '') +
        '</li>';
    });
    return h + '</ul>';
  }

  function htmlAcciones(inst) {
    var p = inst.pan;
    if (!(p.puede_editar || (p.cerrada && p.puede_regularizar))) return '';
    var h = '<div class="fm-acciones"><span>Modificar</span>' +
      boton('ligarDoc', 'Ligar factura, boleta o nota de venta', 'bi-link-45deg', 'fm-btn-pri');
    if (p.cerrada) {
      h += '<span class="fm-help" style="width:100%">OT cerrada (evidencia): solo se regulariza el documento y la plata; el estado, las firmas y las fechas no se tocan.</span>' +
        boton('pedirCierre', p.superadmin ? 'Autorizar sin documento' : 'Pedir autorización a ' + CAMPANA_DAN, 'bi-shield-lock');
    } else if (p.puede_editar) {
      h += boton('pedirCero', 'No se cobra: declarar $0', 'bi-shield-check') +
        boton('corregirProv', 'Corregir lo que cobró el proveedor', 'bi-pencil-square') +
        boton('pedirCierre', p.superadmin ? 'Autorizar cierre sin documento' : 'Pedir autorización a ' + CAMPANA_DAN, 'bi-shield-lock');
    }
    return h + '</div>';
  }

  function pintar(inst) {
    var p = inst.pan, rec = inst.rec;
    inst.el.innerHTML =
      '<div class="fm-head"><div><h3><i class="bi bi-compass-fill"></i> Finanzas y documentos' + (inst.modo === 'modal' ? ' para cerrar' : ' de la OT') + '</h3>' +
      '<p>' + esc(p.numero_ot || '') + (p.cliente ? ' · ' + esc(p.cliente) : '') + ' · todo lo declarado, completo y con quién lo hizo.</p></div>' +
      '<button type="button" class="fm-btn" data-fm-act="recargar" title="Volver a leer"><i class="bi bi-arrow-clockwise"></i> Actualizar</button></div>' +
      '<section class="fm-sec fm-s1">' + htmlPasos(inst) + '</section>' +
      '<section class="fm-sec fm-s0">' + '<div class="fm-sec-h"><span class="fm-num">1</span><h3>El panorama</h3></div>' + htmlContadores(inst) + '</section>' +
      '<section class="fm-sec">' + htmlDocs(inst) + '</section>' +
      '<div class="fm-dos"><section class="fm-sec">' + htmlCostos(inst) + '</section>' +
      '<section class="fm-sec">' + htmlCuenta(inst) + '</section></div>' +
      '<div class="fm-dos"><section class="fm-sec">' + htmlCentro(inst) + '</section>' +
      '<section class="fm-sec">' + htmlHistorial(inst) + '</section></div>' +
      htmlAcciones(inst);
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
    return dialogo({
      titulo: 'Ligar un documento a la OT',
      intro: 'Se busca en Random (solo lectura) y se valida el RUT del cliente. Si ya hay una nota de venta, la factura la da de baja y las dos quedan visibles.',
      ok: 'Buscar y ligar',
      campos: [
        { k: 'tipo', label: 'Tipo de documento', tipo: 'select', valor: (pre && pre.tipo) || 'FCV', opciones: [
          { v: 'FCV', n: 'Factura' }, { v: 'FCE', n: 'Factura exenta' }, { v: 'BLV', n: 'Boleta' },
          { v: 'NVV', n: 'Nota de venta' }, { v: 'COT', n: 'Cotización interna (COT-000045)' }] },
        { k: 'numero', label: 'Número', req: true, tipo: 'texto', valor: (pre && pre.numero) || '', placeholder: 'Ej: 11439' },
        { k: 'etiqueta', label: 'Para qué es (opcional)', tipo: 'texto', placeholder: 'Ej: Instalación' }
      ]
    }).then(function (v) {
      if (!v) return;
      var cuerpo = v.tipo === 'COT'
        ? { origen: 'cotizacion', cotizacion: v.numero, etiqueta: v.etiqueta }
        : { origen: 'erp', tipo: v.tipo, numero: v.numero, etiqueta: v.etiqueta };
      return enviarDoc(inst, cuerpo);
    });
  }
  function enviarDoc(inst, cuerpo) {
    toast('Buscando el documento en Random…', 'info');
    return api('/ot/api/' + inst.vid + '/documentos/regularizar', { method: 'POST', body: cuerpo }).then(function (r) {
      if (r.ok) return despuesDeEscribir(inst, 'Documento ligado. Quedó registrado con tu nombre.');
      var j = r.j || {};
      if (j.error_codigo === 'RUT_NO_COINCIDE') {
        return dialogo({ titulo: 'El RUT no coincide', intro: esc(j.error || ''), ok: 'Ligar de todas formas',
          campos: [{ k: 'justificacion', label: '¿Por qué corresponde este documento?', tipo: 'area', req: true, min: 10,
            placeholder: 'Ej: factura emitida a la casa matriz del cliente' }] }).then(function (v) {
          if (v) { cuerpo.justificacion = v.justificacion; return enviarDoc(inst, cuerpo); }
        });
      }
      if (j.error_codigo === 'POSIBLE_DUPLICADO') {
        return global.ilusConfirm({ title: 'Parece el mismo cobro', message: j.error || '', sub: 'Si es la factura de esa nota de venta, sumarla cobraría dos veces.',
          okLabel: 'Es otro cobro, ligar', cancelLabel: 'Cancelar', danger: true }).then(function (ok) {
          if (ok) { cuerpo.confirmar_duplicado = true; return enviarDoc(inst, cuerpo); }
        });
      }
      toast(j.error || 'No se pudo ligar el documento.', 'error');
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
    var cero = tipo === 'cobro_cero', sa = inst.pan.superadmin;
    var campos = [];
    if (cero) campos.push({ k: 'motivo', label: 'Motivo del $0', tipo: 'select', valor: 'garantia', opciones: [
      { v: 'garantia', n: 'Garantía' }, { v: 'regalia', n: 'Regalía' }, { v: 'arriendo_leasing', n: 'Arriendo o leasing' }] });
    campos.push({ k: 'argumento', label: 'Argumento', tipo: 'area', req: true, min: 30,
      placeholder: cero ? 'Ej: equipo con 8 meses de uso, falla de fábrica cubierta por la garantía del proveedor' : 'Ej: servicio sin costo acordado con gerencia; no habrá factura' });
    campos.push({ k: 'centro_costo', label: 'Centro de costo', req: true, tipo: 'select', valor: inst.pan.centro.valor || '',
      opciones: [{ v: '', n: 'Elige…' }].concat(inst.pan.centro.opciones) });
    if (cero) campos.push({ k: 'valorizado_clp', label: 'Cuánto vale (sugerido, opcional)', tipo: 'monto', ayuda: 'Solo de referencia: lo que de verdad cuenta es lo que nos costó.' });
    return dialogo({
      titulo: cero ? 'Cobrar $0 en esta OT' : 'Cerrar sin documento',
      intro: sa ? 'Eres superadministrador: tu decisión queda registrada como autorización, con tu nombre y la hora.'
        : 'Esto le llega a ' + CAMPANA_DAN + ' a su celular. Mientras responde, la OT queda «Esperando autorización de ' + CAMPANA_DAN + '».',
      ok: sa ? 'Autorizar y registrar' : 'Pedir autorización a ' + CAMPANA_DAN, campos: campos
    }).then(function (v) {
      if (!v) return;
      var body = { tipo: tipo, visita_id: inst.vid, argumento: v.argumento, centro_costo: v.centro_costo };
      if (cero) { body.motivo = v.motivo; if (v.valorizado_clp) body.valorizado_clp = Number(v.valorizado_clp); }
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

  function guardarCentro(inst, sel) {
    var valor = sel.value;
    if (!valor) return;
    api('/ot/api/' + inst.vid + '/centro-costo', { method: 'POST', body: { centro_costo: valor } }).then(function (r) {
      if (r.ok) return despuesDeEscribir(inst, 'Centro de costo guardado.');
      toast((r.j && r.j.error) || 'No se pudo guardar el centro de costo.', 'error');
      cargar(inst);
    });
  }

  function irFinanzas(inst) {
    var modal = inst.el.closest('.modal');
    try { if (modal && global.bootstrap) { var m = global.bootstrap.Modal.getInstance(modal); if (m) m.hide(); } } catch (e) { }
    setTimeout(function () {
      if (global.otdIrAFinanzas) { global.otdIrAFinanzas(); return; }
      var c = document.getElementById('otdCardFinanzas');
      if (c) c.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }, modal ? 350 : 0);
  }

  var ACCIONES = {
    recargar: function (inst) { return cargar(inst); },
    ligarDoc: function (inst) { return ligarDoc(inst); },
    corregirProv: function (inst) { return corregirProv(inst); },
    pedirCero: function (inst) { return pedirAutorizacion(inst, 'cobro_cero'); },
    pedirCierre: function (inst) { return pedirAutorizacion(inst, 'cerrar_sin_documento'); },
    irFinanzas: function (inst) { return irFinanzas(inst); },
    enfocarCentro: function (inst) { var s = inst.el.querySelector('[data-fm-centro]'); if (s) { s.scrollIntoView({ behavior: 'smooth', block: 'center' }); s.focus(); } }
  };

  function montar(el) {
    if (el.__fmInst) return el.__fmInst;
    var inst = { el: el, vid: parseInt(el.getAttribute('data-vid'), 10), modo: el.getAttribute('data-modo') || 'ficha', id: montados.length + 1, rechazo: null };
    el.__fmInst = inst;
    montados.push(inst);
    el.addEventListener('click', function (e) {
      var b = e.target.closest('[data-fm-act]');
      if (!b || !el.contains(b)) return;
      var fn = ACCIONES[b.getAttribute('data-fm-act')];
      if (fn) { e.preventDefault(); fn(inst); }
    });
    el.addEventListener('change', function (e) {
      var s = e.target.closest('[data-fm-centro]');
      if (s) guardarCentro(inst, s);
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

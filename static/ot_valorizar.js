/* 🧮 Valorización sugerida — la línea compacta de la ficha de la OT (REGLA #60, 2026-10-29).
   Daniel: «Si no está valorizado por el usuario, TÚ lo SUGIERES: busca en las COTIZACIONES cuál es el precio más acorde».
   · UNA línea: el valor sugerido, su confianza (semáforo chico) y su fuente. El detalle (por clase, rango, comparables, equipos sin
     precio, traslado) solo al hacer clic en «Ver detalle».
   · OT que se cobra: solo informa («Cobramos $X · el mercado sugiere ≈ $Y») y avisa en ámbar si el cobro está más de 30 % bajo lo
     sugerido con confianza media o alta. No ofrece guardar.
   · OT que no se cobra: «Usar este valor» / «Editar» / «Descartar» SOLO para el superadministrador (el servidor lo exige, recalcula
     la sugerencia y decide el rótulo: el navegador nunca lo fija). Nada se guarda sin la confirmación de una persona (REGLA #32).
   · Sin alert/confirm/prompt nativos (REGLA #1): ilusConfirm / ilusPrompt / ilusToast.
   Lee GET /ot/api/<vid>/valorizacion-sugerida (solo lectura) y escribe con POST /ot/api/<vid>/valorizacion-aplicar. */
(function (global) {
  'use strict';

  var CONF_TXT = { alta: 'Confianza alta', media: 'Confianza media', baja: 'Confianza baja' };

  function esc(t) {
    return String(t == null ? '' : t).replace(/[&<>"']/g, function (c) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
    });
  }
  function clp(n) {
    if (n === null || n === undefined || n === '' || isNaN(Number(n))) return '—';
    var v = Math.round(Number(n)), s = String(Math.abs(v)).replace(/\B(?=(\d{3})+(?!\d))/g, '.');
    return (v < 0 ? '−' : '') + '$' + s;
  }
  function cant(c) {
    var n = Number(c) || 0;
    return Math.abs(n - Math.round(n)) < 1e-9 ? String(Math.round(n)) : String(Math.round(n * 100) / 100).replace('.', ',');
  }
  function sem(conf) {
    if (!conf) return '';
    return '<span class="vz-sem ' + esc(conf) + '" title="Qué tan respaldado está el número">' + esc(CONF_TXT[conf] || conf) + '</span>';
  }
  function btn(act, txt, ico, pri, extra) {
    return '<button type="button" class="vz-btn' + (pri ? ' pri' : '') + '" data-vz="' + act + '"' + (extra || '') +
           '><i class="bi ' + ico + '" aria-hidden="true"></i>' + esc(txt) + '</button>';
  }
  function primeraFrase(t) {
    var s = String(t || ''), i = s.indexOf('. ');
    return i > 0 ? s.slice(0, i + 1) : s;
  }

  /* ── La línea (PURA: devuelve {clase, html}) ── */
  function htmlLinea(j, sa, abierto) {
    j = j || {};
    var ot = j.ot || {}, val = ot.valorizado || {}, ult = j.ultima_decision || {};
    var ver = (j.hay || j.texto) ? btn('ver', abierto ? 'Ocultar detalle' : 'Ver detalle', abierto ? 'bi-chevron-up' : 'bi-chevron-down', false,
                                       ' aria-expanded="' + (abierto ? 'true' : 'false') + '"') : '';
    var fte = j.fuente_corta ? '<span class="vz-fte">' + esc(j.fuente_corta) + '</span>' : '';
    var ico = '<i class="bi bi-calculator vz-ico" aria-hidden="true"></i>';
    if (j.ok === false && !j.texto) {
      return { clase: 'mudo', html: ico + '<span class="vz-txt">No se pudo calcular el valor sugerido ahora.</span><span class="vz-acc">' +
               btn('recargar', 'Reintentar', 'bi-arrow-clockwise') + '</span>' };
    }
    if (j.modo === 'mercado') {
      if (!j.hay) return null;                                   // una OT que se cobra sin datos de mercado: no se dice nada
      var c = j.comparacion || {}, cmp = '';
      if (c.cobrado !== null && c.cobrado !== undefined) {
        var p = Number(c.diferencia_pct);
        cmp = '<span class="vz-cmp">Cobramos ' + clp(c.cobrado) + ' · el mercado sugiere ≈ ' + clp(j.monto) +
              (p > 0 ? ' (' + cant(Math.round(p)) + ' % bajo)' : p < 0 ? ' (' + cant(Math.round(-p)) + ' % sobre)' : '') + '</span>';
      } else {
        cmp = '<span><b>Precio de mercado ≈ ' + clp(j.monto) + '</b></span>';
      }
      return { clase: c.alerta ? 'amb' : '', html: ico + '<span class="vz-txt">' + cmp + sem(j.confianza) + fte +
               (c.alerta ? '<span class="vz-cmp">Revisa si fue un descuento autorizado.</span>' : '') + '</span>' +
               '<span class="vz-acc">' + ver + '</span>' };
    }
    var acc = ver, txt, clase = '';
    var tieneVal = val.monto !== null && val.monto !== undefined;
    if (tieneVal) {
      clase = 'ok';
      txt = '<span><b>Valorizado ' + clp(val.monto) + '</b> · ' + esc(val.fuente_txt || 'guardado') + '</span>';
      if (j.hay) txt += '<span class="vz-fte">Sugerido ≈ ' + clp(j.monto) + '</span>' + sem(j.confianza);
      if (sa && j.hay && j.puede_guardar && Math.round(j.monto) !== Math.round(val.monto)) acc += btn('usar', 'Usar el sugerido', 'bi-check2-circle');
    } else if (!j.hay) {
      clase = 'mudo';
      txt = '<span>' + esc(primeraFrase(j.texto) || 'Sin valor sugerido.') + '</span>';
      if (sa) acc += btn('editar', 'Escribir el valor', 'bi-pencil');   // a mano y con motivo (la persona decide)
    } else if (ult.decision === 'descartada') {
      clase = 'mudo';
      txt = '<span>Sugerencia descartada' + (ult.usuario ? ' por ' + esc(ult.usuario) : '') + (ult.cuando ? ' el ' + esc(ult.cuando) : '') +
            ' · era ≈ ' + clp(j.monto) + '</span>' + sem(j.confianza);
      if (sa && j.puede_guardar) acc += btn('usar', 'Usar este valor', 'bi-check2-circle');
    } else {
      txt = '<span><b>Valor sugerido ≈ ' + clp(j.monto) + '</b></span>' + sem(j.confianza) + fte;
      if (sa && j.puede_guardar) {
        acc += btn('usar', 'Usar este valor', 'bi-check2-circle', true) + btn('editar', 'Editar', 'bi-pencil') + btn('descartar', 'Descartar', 'bi-x-circle');
      } else if (!sa && j.puede_guardar) {
        txt += '<span class="vz-fte">(lo guarda el superadministrador)</span>';
      }
    }
    return { clase: clase, html: ico + '<span class="vz-txt">' + txt + '</span><span class="vz-acc">' + acc + '</span>' };
  }

  /* ── El detalle (PURO): por clase, lo que quedó fuera, el traslado ── */
  function htmlDetalle(j) {
    j = j || {};
    var h = '<p>' + esc(j.texto || '') + '</p>';
    var cls = j.por_clase || [];
    if (cls.length) {
      h += '<h4>Por tipo de equipo</h4>';
      cls.forEach(function (c) {
        var fuente = c.fuente_txt + (c.n ? ' (' + c.n + ')' : '') + (c.respaldo ? ', incluye borradores' : '');
        var rango = (c.p25 !== null && c.p25 !== undefined && c.p75 !== null && c.p75 !== undefined && c.p25 !== c.p75)
          ? ' · entre ' + clp(c.p25) + ' y ' + clp(c.p75) + ' c/u' : '';
        var ref = c.ref && c.ref.numero ? ' · la más parecida: ' + esc(c.ref.numero) + (c.ref.cliente ? ' de ' + esc(c.ref.cliente) : '') +
          (c.ref.fecha ? ', del ' + esc(c.ref.fecha) : '') : '';
        h += '<div class="vz-cl"><span class="vz-cl-n">' + esc(c.clase_nombre) + ' × ' + cant(c.unidades) + ' a ' + clp(c.unit) + ' c/u</span>' +
             '<span class="vz-cl-m">' + clp(c.monto) + '</span>' +
             '<span class="vz-cl-s">' + esc(fuente) + rango + ref + ' · ' + esc(CONF_TXT[c.confianza] || '') +
             ((c.equipos || []).length ? '<br>' + (c.equipos || []).map(esc).join(' · ') : '') + '</span></div>';
      });
    }
    if ((j.sin_precio || []).length) {
      h += '<h4>Sin precio (quedan fuera)</h4><ul class="vz-lista">' + j.sin_precio.map(function (s) {
        return '<li>' + esc(s.clase_nombre) + ' × ' + cant(s.unidades) + (s.motivo === 'solo_explicita' ? ' — solo con una partida explícita en su cotización' :
               ' — sin cotizaciones, OT comparables ni tarifa') + '</li>';
      }).join('') + '</ul>';
    }
    if ((j.sin_clasificar || []).length) {
      h += '<h4>Equipos sin clasificar (quedan fuera)</h4><ul class="vz-lista">' + j.sin_clasificar.map(function (s) {
        return '<li>' + esc(s.nombre) + (Number(s.unidades) > 1 ? ' × ' + cant(s.unidades) : '') + '</li>';
      }).join('') + '</ul><div class="vz-nota">Clasifícalos en el <a href="/catalogo" target="_blank" rel="noopener">catálogo</a> para poder compararlos.</div>';
    }
    if ((j.accesorios || []).length) {
      h += '<div class="vz-nota">Accesorios que no se cobran (no cuentan): ' + j.accesorios.map(function (a) {
        return esc(a.nombre) + (Number(a.unidades) > 1 ? ' × ' + cant(a.unidades) : '');
      }).join(' · ') + '.</div>';
    }
    if (j.traslado && j.traslado.monto) {
      h += '<div class="vz-nota">Traslado' + (j.traslado.comuna ? ' a ' + esc(j.traslado.comuna) : '') + ': ' + clp(j.traslado.monto) +
           ', aparte (no entra al valorizado).</div>';
    }
    h += '<div class="vz-nota">Es solo referencia: el valorizado nunca se suma a lo cobrado.</div>';
    return h;
  }

  /* ── Montaje en la ficha ── */
  function pintar(el) {
    var st = el._vz || {};
    if (!st.j) {
      el.innerHTML = '<div class="vz-linea mudo"><i class="bi bi-hourglass-split vz-ico" aria-hidden="true"></i>' +
                     '<span class="vz-txt">Buscando el valor sugerido en las cotizaciones…</span></div>';
      return;
    }
    var l = htmlLinea(st.j, st.sa, st.abierto);
    if (!l) { el.innerHTML = ''; el.hidden = true; return; }
    el.hidden = false;
    el.innerHTML = '<div class="vz-linea ' + l.clase + '">' + l.html + '</div>' +
                   '<div class="vz-det"' + (st.abierto ? '' : ' hidden') + '>' + (st.abierto ? htmlDetalle(st.j) : '') + '</div>';
  }

  function cargar(el) {
    var st = el._vz;
    st.j = null; pintar(el);
    return global.fetch('/ot/api/' + encodeURIComponent(st.vid) + '/valorizacion-sugerida', { credentials: 'same-origin' })
      .then(function (r) { return r.json().catch(function () { return { ok: false }; }); })
      .then(function (j) { st.j = j || { ok: false }; pintar(el); })
      .catch(function () { st.j = { ok: false }; pintar(el); });
  }

  function toast(m, t) { try { if (global.ilusToast) global.ilusToast(m, { type: t || 'info' }); } catch (e) { } }

  /* Lo guardado se refleja en la ficha sin recargar: la base del formulario de edición (así «Editar cobro y costos» no lo vuelve a mandar
     como un cambio a mano) y el bloque «Costos y documentación». */
  function sincronizarFicha(v) {
    try {
      var F = global.OTD_FIN;
      if (F && F.base) { F.base.valorizado_clp = v.monto; F.base.valorizado_fuente = v.fuente; }
      var inp = global.document && global.document.getElementById('otdFinValorizado');
      if (inp && global.otdMoneySet) {
        global.otdMoneySet('otdFinValorizado', v.monto);
        inp.dataset.origen = v.fuente || 'a_mano';
        inp.dataset.origenValor = String(v.monto == null ? '' : v.monto);
      }
      if (global.OTFinMotor && global.OTFinMotor.refrescar) global.OTFinMotor.refrescar();
    } catch (e) { }
  }

  function aplicar(el, cuerpo) {
    var st = el._vz;
    return global.fetch('/ot/api/' + encodeURIComponent(st.vid) + '/valorizacion-aplicar', {
      method: 'POST', credentials: 'same-origin', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(cuerpo)
    }).then(function (r) {
      return r.json().catch(function () { return {}; }).then(function (j) { return { ok: r.ok && j && j.ok !== false, status: r.status, j: j || {} }; });
    });
  }

  function trasGuardar(el, res, decision) {
    var st = el._vz;
    if (!res.ok) {
      if (res.j && res.j.error_codigo === 'SUGERENCIA_CAMBIO') {
        toast(res.j.error || 'La sugerencia cambió: revísala de nuevo.', 'warning');
        return cargar(el);
      }
      toast((res.j && res.j.error) || 'No se pudo guardar.', 'error');
      return;
    }
    if (res.j.escribio && res.j.valorizado) {
      st.j.ot = st.j.ot || {};
      st.j.ot.valorizado = res.j.valorizado;
      sincronizarFicha(res.j.valorizado);
      toast('Valorizado guardado en ' + clp(res.j.valorizado.monto) + ' (' + (res.j.valorizado.fuente_txt || '') + '). Es solo referencia.', 'success');
    } else if (res.j.sin_cambios) {
      toast('El valorizado ya era ese: no cambió nada.', 'info');
    } else if (decision === 'descartada') {
      st.j.ultima_decision = { decision: 'descartada', usuario: 'ti', cuando: 'recién' };
      toast('Sugerencia descartada. Queda registrado.', 'info');
    }
    pintar(el);
  }

  function numeroOT(el) { return el.getAttribute('data-numero') || ('OT ' + el._vz.vid); }

  function usar(el) {
    var j = el._vz.j || {};
    var reemplaza = j.ot && j.ot.valorizado && j.ot.valorizado.monto != null;
    return global.ilusConfirm({
      title: 'Guardar el valor sugerido', message: numeroOT(el) + ' · ≈ ' + clp(j.monto) + ' (' + (j.fuente_corta || '') + ', ' + (CONF_TXT[j.confianza] || '').toLowerCase() + ')',
      sub: (reemplaza ? 'Reemplaza el valorizado actual (' + clp(j.ot.valorizado.monto) + '). ' : '') +
           'Es solo referencia: no se suma a lo cobrado. El servidor vuelve a calcular la sugerencia antes de guardar y queda registrado con tu nombre.',
      okLabel: 'Guardar', cancelLabel: 'Cancelar', type: 'question'
    }).then(function (ok) {
      if (!ok) return;
      return aplicar(el, { monto: j.monto, decision: 'aceptada' }).then(function (r) { return trasGuardar(el, r, 'aceptada'); });
    });
  }

  function editar(el) {
    var j = el._vz.j || {};
    return global.ilusPrompt({
      title: 'Valor de referencia', message: numeroOT(el) + (j.hay ? ' · el sugerido es ≈ ' + clp(j.monto) : '') + '. ¿Cuánto vale este trabajo (neto)?',
      placeholder: 'Ej: 450000', defaultValue: j.hay ? String(j.monto) : '', inputType: 'text', okLabel: 'Siguiente', cancelLabel: 'Cancelar',
      sanitize: function (v) { return String(v || '').replace(/[^\d]/g, ''); },
      validate: function (v) {
        var n = parseInt(String(v || '').replace(/[^\d]/g, ''), 10);
        return (n > 0) ? { ok: true, valor: String(n) } : { ok: false, error: 'Escribe un monto mayor que cero.' };
      }
    }).then(function (monto) {
      if (monto === null || monto === undefined || monto === '') return;
      var n = parseInt(String(monto).replace(/[^\d]/g, ''), 10);
      return global.ilusPrompt({
        title: '¿Por qué este valor?', message: 'Queda guardado junto al monto, con tu nombre y la hora.',
        placeholder: 'Ej: el cliente tiene precio de contrato distinto', multiline: true, minLength: 15, okLabel: 'Guardar', cancelLabel: 'Cancelar'
      }).then(function (motivo) {
        if (!motivo) return;
        return aplicar(el, { monto: n, decision: 'editada', motivo: motivo }).then(function (r) { return trasGuardar(el, r, 'editada'); });
      });
    });
  }

  function descartar(el) {
    var j = el._vz.j || {};
    return global.ilusPrompt({
      title: 'Descartar la sugerencia', message: numeroOT(el) + ' · ≈ ' + clp(j.monto) + '. No se guarda ningún valor; queda la constancia.',
      placeholder: 'Opcional: por qué no sirve', multiline: true, required: false, okLabel: 'Descartar', cancelLabel: 'Cancelar', type: 'warning'
    }).then(function (motivo) {
      if (motivo === null || motivo === undefined) return;
      return aplicar(el, { decision: 'descartada', motivo: motivo || '' }).then(function (r) { return trasGuardar(el, r, 'descartada'); });
    });
  }

  function onClick(ev) {
    var b = ev.target && ev.target.closest ? ev.target.closest('button[data-vz]') : null;
    if (!b) return;
    var el = this, st = el._vz, act = b.getAttribute('data-vz');
    if (st.ocupado) return;
    if (act === 'ver') { st.abierto = !st.abierto; pintar(el); return; }
    if (act === 'recargar') { cargar(el); return; }
    var fn = { usar: usar, editar: editar, descartar: descartar }[act];
    if (!fn) return;
    st.ocupado = true;
    Promise.resolve(fn(el)).catch(function () { toast('No se pudo completar. Inténtalo de nuevo.', 'error'); })
      .then(function () { st.ocupado = false; });
  }

  function montar(el) {
    if (!el || el._vz) return;
    el._vz = { vid: el.getAttribute('data-vid'), sa: el.getAttribute('data-sa') === '1', j: null, abierto: false, ocupado: false };
    el.addEventListener('click', onClick);
    cargar(el);
  }

  var API = { htmlLinea: htmlLinea, htmlDetalle: htmlDetalle, clp: clp, montar: montar, pintar: pintar, cargar: cargar };
  global.OTValSug = API;
  if (typeof module !== 'undefined' && module.exports) module.exports = API;
  if (global.document && global.document.addEventListener) {
    var arrancar = function () { global.document.querySelectorAll('[data-val-sug]').forEach(montar); };
    if (global.document.readyState === 'loading') global.document.addEventListener('DOMContentLoaded', arrancar);
    else arrancar();
  }
})(typeof window !== 'undefined' ? window : globalThis);

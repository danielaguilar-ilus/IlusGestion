/* 🧮 «Valorizar en lote» (REGLA #60, 2026-10-29). Solo superadmin.
   · La lista viene paginada del servidor (GET /ot/api/valorizacion/lote, solo lectura, caché de 90 s).
   · Nada viene marcado. «Seleccionar/deseleccionar todo» es un interruptor sobre lo VISIBLE (REGLA #14); «Marcar las de confianza alta»
     marca las de confianza alta de TODAS las páginas (el servidor manda la lista `alta`). La selección se recuerda al cambiar de página.
   · «Revisar» SOLO LEE: vuelve a pedir la sugerencia de esa OT (GET /ot/api/<vid>/valorizacion-sugerida) y la muestra.
   · «Aplicar las seleccionadas»: confirmación con cuántas y el total, y se guardan DE A 3 a la vez por POST /ot/api/<vid>/valorizacion-aplicar
     (el servidor recalcula cada una antes de escribir; si cambió, no se toca y queda en ámbar).
   · Sin alert/confirm/prompt nativos (REGLA #1). Nombres completos (REGLA #15). */
(function (global) {
  'use strict';
  var doc = global.document;
  var S = { periodo: 'todo', desde: '', hasta: '', estado: 'cerradas', cobertura: '', confianza: '', q: '', desc: '', page: 1, por: 50,
            datos: null, sel: {}, res: {}, ocupado: false, pedido: 0 };
  var CONF = { alta: 'Confianza alta', media: 'Confianza media', baja: 'Confianza baja' };

  function $(id) { return doc.getElementById(id); }
  function esc(t) {
    return String(t == null ? '' : t).replace(/[&<>"']/g, function (c) { return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]; });
  }
  function clp(n) {
    if (n === null || n === undefined || n === '' || isNaN(Number(n))) return '—';
    var v = Math.round(Number(n)), s = String(Math.abs(v)).replace(/\B(?=(\d{3})+(?!\d))/g, '.');
    return (v < 0 ? '−' : '') + '$' + s;
  }
  function toast(m, t) { try { if (global.ilusToast) global.ilusToast(m, { type: t || 'info' }); } catch (e) { } }
  function sem(c) { return c ? '<span class="vz-sem ' + esc(c) + '">' + esc(CONF[c] || c) + '</span>' : ''; }
  function pad(n) { return (n < 10 ? '0' : '') + n; }

  function params() {
    var p = { estado: S.estado, page: S.page, por: S.por };
    if (S.periodo === 'mes_anterior') {
      var h = new Date(), a = new Date(h.getFullYear(), h.getMonth() - 1, 1);
      p.modo = 'mes'; p.ancla = a.getFullYear() + '-' + pad(a.getMonth() + 1) + '-01';
    } else {
      p.modo = S.periodo;
    }
    if (S.periodo === 'rango') { p.desde = S.desde; p.hasta = S.hasta; }
    if (S.cobertura) p.cobertura = S.cobertura;
    if (S.confianza) p.confianza = S.confianza;
    if (S.q.trim()) p.q = S.q.trim();
    if (S.desc) p.ver_descartadas = '1';
    return Object.keys(p).map(function (k) { return encodeURIComponent(k) + '=' + encodeURIComponent(p[k]); }).join('&');
  }

  async function pedir(url, opts) {
    var r = await global.fetch(url, Object.assign({ credentials: 'same-origin', headers: { 'Content-Type': 'application/json' } }, opts || {}));
    var j = null;
    try { j = await r.json(); } catch (e) { }
    return { ok: !!(r.ok && j && j.ok !== false), status: r.status, j: j || {} };
  }

  async function cargar(actualizar) {
    var n = ++S.pedido;
    $('vzlBody').innerHTML = '<tr><td colspan="7" class="vzl-vacio">Cargando…</td></tr>';
    var r;
    try { r = await pedir('/ot/api/valorizacion/lote?' + params() + (actualizar ? '&actualizar=1' : '')); }
    catch (e) { r = { ok: false, j: {} }; }
    if (n !== S.pedido) return;                      // llegó una respuesta vieja: manda la última pedida
    if (!r.ok) {
      S.datos = { filas: [], total: 0, paginas: 1, page: 1, desde: 0, hasta: 0, kpis: null, alta: [] };
      toast((r.j && r.j.error) || 'No se pudo cargar la lista.', 'error');
    } else {
      S.datos = r.j;
      S.page = r.j.page || 1;
    }
    pintar();
  }

  function pintarKpis() {
    var k = (S.datos && S.datos.kpis) || null;
    $('vzlKpiDejamos').textContent = k ? clp(k.dejamos_de_cobrar) : '—';
    $('vzlKpiDejamosS').textContent = k ? (k.n_valorizadas + ' OT que no se cobraron, ya valorizadas · ' + (S.datos.periodo || '')) : '';
    $('vzlKpiInternos').textContent = k ? clp(k.internos) : '—';
    $('vzlKpiInternosS').textContent = k ? (k.n_internos + ' trabajos internos valorizados' +
      (k.por_valorizar_internos ? ' · faltan ≈ ' + clp(k.por_valorizar_internos) + ' por valorizar' : '')) : '';
    $('vzlKpiPor').textContent = k ? (k.por_valorizar_n + ' OT') : '—';
    $('vzlKpiPorS').textContent = k ? (k.por_valorizar_con_sugerencia + ' con valor sugerido, en total ≈ ' + clp(k.por_valorizar_sugerido) +
      ' (aún no guardado)') : '';
    var av = $('vzlAviso');
    if (S.datos && S.datos.truncado) {
      av.hidden = false;
      av.className = 'vzl-aviso';
      av.textContent = 'Se leyeron las ' + S.datos.tope + ' OT más recientes del período: acota el período para ver el resto.';
    } else if (!S.res.__resumen) {
      av.hidden = true;
    }
  }

  function celdaEstado(f) {
    var r = S.res[f.id];
    var h = '';
    if (r) h += '<span class="vzl-est ' + esc(r.fase) + '"><span>' + esc(r.msg) + '</span></span>';
    else if (f.descartada) h += '<span class="vzl-est aviso"><span>' + esc(f.descartada_txt || 'Descartada') + '</span></span>';
    if (f.monto !== null && f.monto !== undefined && !(r && r.fase === 'hecha')) {
      h += ' <button type="button" class="vzl-btn" data-act="revisar" data-id="' + f.id + '"><i class="bi bi-search"></i>Revisar</button>';
    }
    if (r && r.texto) h += '<div class="vzl-texto">' + esc(r.texto) + '</div>';
    return h || '<span class="vzl-sub">—</span>';
  }

  function fila(f) {
    var sel = !!S.sel[f.id], marcable = f.puede_aplicar && !(S.res[f.id] && S.res[f.id].fase === 'hecha');
    // «× 2» nunca queda solo en una línea: espacios que no se cortan alrededor de la cantidad.
    var eq = (f.equipos || []).map(function (e) { return '<li>' + esc(e).replace(/ × /g, ' × ') + '</li>'; }).join('') ||
             '<li class="vzl-sub">Sin equipos</li>';
    var sug = (f.monto !== null && f.monto !== undefined)
      ? '<span class="vzl-monto">≈ ' + clp(f.monto) + '</span>' +
        (f.rango && f.rango.p25 !== f.rango.p75 ? '<div class="vzl-rango">entre ' + clp(f.rango.p25) + ' y ' + clp(f.rango.p75) + '</div>' : '')
      : '<span class="vzl-sin">' + esc(String(f.texto || 'Sin valor sugerido.').replace(/^Sin valor sugerido: /, 'Sin sugerencia: ')) + '</span>';
    var cub = (f.cobertura_equipos !== null && f.cobertura_equipos !== undefined)
      ? ' · cubre ' + Math.round(Number(f.cobertura_equipos) * 100) + ' % de los equipos' : '';
    return '<tr>' +
      '<td class="ck">' + (marcable ? '<input type="checkbox" data-sel="' + f.id + '"' + (sel ? ' checked' : '') +
        ' aria-label="Seleccionar ' + esc(f.numero) + '">' : '') + '</td>' +
      '<td data-k="OT"><a class="vzl-ot" href="/ot/' + f.id + '">' + esc(f.numero) + '</a>' +
        '<div class="vzl-sub">' + esc(f.tipo_txt || '') + ' · ' + esc(f.cobertura_txt || '') + (f.fecha ? ' · ' + (f.estado === 'cerrada' ? 'cerrada el ' : 'creada el ') + esc(f.fecha) : '') + '</div></td>' +
      '<td data-k="Cliente"><span class="vzl-cli">' + esc(f.cliente || '—') + '</span></td>' +
      '<td data-k="Equipos"><ul class="vzl-eq">' + eq + '</ul></td>' +
      '<td data-k="Sugerido" class="n">' + sug + '</td>' +
      '<td data-k="Confianza">' + (f.confianza ? sem(f.confianza) + '<div class="vzl-sub">' + esc(f.fuente_corta || '') + esc(cub) + '</div>' : '<span class="vzl-sub">—</span>') + '</td>' +
      '<td data-k="Estado">' + celdaEstado(f) + '</td></tr>';
  }

  function pintar() {
    var d = S.datos || { filas: [], total: 0, paginas: 1, page: 1, desde: 0, hasta: 0 };
    var h = (d.filas || []).map(fila).join('');
    if (!h) {
      h = '<tr><td colspan="7" class="vzl-vacio">' + ((d.n_lista || 0) ? 'Ninguna OT coincide con los filtros.'
        : '<i class="bi bi-check-circle-fill"></i>Todas las OT del período que no se cobran ya tienen su valor de referencia.') + '</td></tr>';
    }
    $('vzlBody').innerHTML = h;
    $('vzlPie').innerHTML = '<span>Mostrando ' + (d.desde || 0) + '–' + (d.hasta || 0) + ' de ' + (d.total || 0) + '</span>' +
      '<div class="vzl-pie-pg"><button type="button" data-pg="-1"' + (d.page <= 1 ? ' disabled' : '') + '>Anterior</button>' +
      '<span>Página ' + (d.page || 1) + ' de ' + (d.paginas || 1) + '</span>' +
      '<button type="button" data-pg="1"' + (d.page >= d.paginas ? ' disabled' : '') + '>Siguiente</button></div>';
    pintarKpis();
    resumenSel();
  }

  function seleccionadas() { return Object.keys(S.sel).map(function (k) { return S.sel[k]; }); }
  function resumenSel() {
    var s = seleccionadas(), total = 0;
    s.forEach(function (x) { total += Number(x.monto) || 0; });
    $('vzlSel').textContent = s.length ? (s.length + ' seleccionada' + (s.length === 1 ? '' : 's') + ' · ≈ ' + clp(total)) : '';
    $('vzlRevisar').disabled = S.ocupado || !s.length;
    $('vzlAplicar').disabled = S.ocupado || !s.length;
    var alta = (S.datos && S.datos.alta) || [];
    $('vzlAlta').disabled = S.ocupado || !alta.length;
    $('vzlAlta').lastChild.textContent = alta.length ? 'Marcar las de confianza alta (' + alta.length + ')' : 'Marcar las de confianza alta';
  }

  function porId(id) {
    var lista = (S.datos && S.datos.filas) || [];
    for (var i = 0; i < lista.length; i++) if (String(lista[i].id) === String(id)) return lista[i];
    return null;
  }

  /* ── Revisar: SOLO LEE ── */
  async function revisar(x) {
    S.res[x.id] = { fase: 'revisada', msg: 'Revisando…' };
    pintar();
    var r;
    try { r = await pedir('/ot/api/' + x.id + '/valorizacion-sugerida'); } catch (e) { r = { ok: false, j: {} }; }
    if (!r.ok) { S.res[x.id] = { fase: 'error', msg: (r.j && r.j.error) || 'No se pudo revisar.' }; return; }
    var j = r.j;
    if (!j.hay || !j.puede_guardar) {
      S.res[x.id] = { fase: 'aviso', msg: 'Hoy no hay un valor que se pueda guardar.', texto: j.texto };
      if (S.sel[x.id]) delete S.sel[x.id];
      return;
    }
    var cambio = Math.round(j.monto) !== Math.round(Number(x.monto));
    if (S.sel[x.id]) S.sel[x.id].monto = j.monto;
    var f = porId(x.id);
    if (f) { f.monto = j.monto; f.rango = j.rango; f.confianza = j.confianza; f.fuente_corta = j.fuente_corta; }
    S.res[x.id] = { fase: cambio ? 'aviso' : 'revisada', texto: j.texto,
                    msg: cambio ? 'La sugerencia cambió: ahora ≈ ' + clp(j.monto) + '.' : 'Revisada: ≈ ' + clp(j.monto) + '.' };
  }

  /* ── Aplicar: el servidor recalcula y decide ── */
  async function aplicar(x) {
    var r;
    try { r = await pedir('/ot/api/' + x.id + '/valorizacion-aplicar', { method: 'POST', body: JSON.stringify({ monto: x.monto, decision: 'aceptada' }) }); }
    catch (e) { r = { ok: false, j: {} }; }
    if (r.ok) {
      var v = r.j.valorizado || {};
      S.res[x.id] = { fase: 'hecha', msg: 'Guardado: ' + clp(v.monto) + ' (' + (v.fuente_txt || '') + ').' };
      delete S.sel[x.id];
    } else if (r.j && r.j.error_codigo === 'SUGERENCIA_CAMBIO') {
      S.res[x.id] = { fase: 'aviso', msg: 'No se tocó: la sugerencia cambió' + (r.j.sugerido ? ' (ahora ≈ ' + clp(r.j.sugerido) + ')' : '') + '. Revísala.' };
      if (S.sel[x.id] && r.j.sugerido) S.sel[x.id].monto = r.j.sugerido;
    } else {
      S.res[x.id] = { fase: 'error', msg: (r.j && r.j.error) || 'No se pudo guardar.' };
    }
  }

  /* De a 3 a la vez: nunca todas juntas. */
  async function correr(lista, fn) {
    if (!lista.length) return;
    S.ocupado = true; resumenSel();
    var barra = $('vzlProg'), paso = barra.firstElementChild, hechas = 0, i = 0;
    barra.style.display = 'block'; paso.style.width = '0';
    async function obrero() {
      while (i < lista.length) {
        var x = lista[i++];
        await fn(x);
        hechas++; paso.style.width = Math.round(hechas * 100 / lista.length) + '%';
        pintar();
      }
    }
    await Promise.all([obrero(), obrero(), obrero()]);
    S.ocupado = false; barra.style.display = 'none';
    pintar();
  }

  function recargarConFiltros() { S.page = 1; return cargar(); }

  /* ── Eventos ── */
  function enlazar() {
    $('vzlPeriodo').addEventListener('change', function () {
      S.periodo = this.value;
      $('vzlDesdeL').hidden = $('vzlHastaL').hidden = S.periodo !== 'rango';
      if (S.periodo !== 'rango' || (S.desde && S.hasta)) recargarConFiltros();
    });
    $('vzlDesde').addEventListener('change', function () { S.desde = this.value; if (S.desde && S.hasta) recargarConFiltros(); });
    $('vzlHasta').addEventListener('change', function () { S.hasta = this.value; if (S.desde && S.hasta) recargarConFiltros(); });
    $('vzlEstado').addEventListener('change', function () { S.estado = this.value; recargarConFiltros(); });
    $('vzlCobertura').addEventListener('change', function () { S.cobertura = this.value; recargarConFiltros(); });
    $('vzlConfianza').addEventListener('change', function () { S.confianza = this.value; recargarConFiltros(); });
    $('vzlDesc').addEventListener('change', function () { S.desc = this.value; recargarConFiltros(); });
    $('vzlPor').addEventListener('change', function () { S.por = parseInt(this.value, 10) || 50; recargarConFiltros(); });
    var tq = null;
    $('vzlQ').addEventListener('input', function () {
      var v = this.value;
      if (tq) clearTimeout(tq);
      tq = setTimeout(function () { S.q = v; recargarConFiltros(); }, 300);   // al borrar la búsqueda también se recarga (REGLA #4.3)
    });
    $('vzlRecargar').addEventListener('click', function () { S.res = {}; cargar(true); });
    $('vzlTodo').addEventListener('click', function () {
      var vis = ((S.datos && S.datos.filas) || []).filter(function (f) { return f.puede_aplicar && !(S.res[f.id] && S.res[f.id].fase === 'hecha'); });
      var falta = vis.some(function (f) { return !S.sel[f.id]; });
      vis.forEach(function (f) { if (falta) S.sel[f.id] = { id: f.id, numero: f.numero, monto: f.monto }; else delete S.sel[f.id]; });
      pintar();
    });
    $('vzlAlta').addEventListener('click', function () {
      ((S.datos && S.datos.alta) || []).forEach(function (a) { S.sel[a.id] = { id: a.id, numero: a.numero, monto: a.monto }; });
      pintar();
    });
    $('vzlBody').addEventListener('change', function (ev) {
      var id = ev.target && ev.target.getAttribute && ev.target.getAttribute('data-sel');
      if (!id) return;
      var f = porId(id);
      if (ev.target.checked && f) S.sel[id] = { id: f.id, numero: f.numero, monto: f.monto }; else delete S.sel[id];
      resumenSel();
    });
    $('vzlBody').addEventListener('click', async function (ev) {
      var b = ev.target && ev.target.closest ? ev.target.closest('button[data-act]') : null;
      if (!b || S.ocupado) return;
      var f = porId(b.getAttribute('data-id'));
      if (!f) return;
      S.ocupado = true;
      try { await revisar({ id: f.id, numero: f.numero, monto: f.monto }); } finally { S.ocupado = false; }
      pintar();
    });
    $('vzlPie').addEventListener('click', function (ev) {
      var b = ev.target && ev.target.closest ? ev.target.closest('button[data-pg]') : null;
      if (!b || b.disabled) return;
      S.page = Math.max(1, (S.page || 1) + parseInt(b.getAttribute('data-pg'), 10));
      cargar();
    });
    $('vzlRevisar').addEventListener('click', async function () {
      var s = seleccionadas();
      if (s.length > 200) { toast('Son ' + s.length + ' OT: se revisan las primeras 200.', 'warning'); s = s.slice(0, 200); }
      await correr(s, revisar);
      toast('Revisión lista: ' + s.length + ' OT. No se guardó nada.', 'info');
    });
    $('vzlAplicar').addEventListener('click', async function () {
      var s = seleccionadas();
      if (!s.length) return;
      var total = 0;
      s.forEach(function (x) { total += Number(x.monto) || 0; });
      var ok = await global.ilusConfirm({
        title: 'Guardar el valorizado de ' + s.length + ' OT',
        message: 'Se guardará el valor sugerido de ' + s.length + ' OT, en total ≈ ' + clp(total) + '.',
        sub: 'Es solo referencia: no se suma a lo cobrado. Antes de guardar, el servidor vuelve a calcular cada OT: si su sugerencia cambió, ' +
             'esa no se toca y queda en ámbar. Queda registrado con tu nombre y la hora.',
        okLabel: 'Aplicar', cancelLabel: 'Cancelar', type: 'question' });
      if (!ok) return;
      await correr(s, aplicar);
      var hechas = s.filter(function (x) { return S.res[x.id] && S.res[x.id].fase === 'hecha'; }).length;
      var resto = s.length - hechas;
      S.res.__resumen = true;
      var av = $('vzlAviso');
      av.hidden = false;
      av.className = 'vzl-aviso' + (resto ? '' : ' ok');       // todo guardado = verde; si algo quedó sin tocar, ámbar (guiar sin gritar)
      av.textContent = 'Se guardaron ' + hechas + ' de ' + s.length + '.' + (resto ? ' ' + resto + ' no se tocaron (mira su estado en la tabla).' : '');
      toast('Listo: ' + hechas + ' guardada' + (hechas === 1 ? '' : 's') + (resto ? ', ' + resto + ' sin tocar' : '') + '.', hechas ? 'success' : 'warning');
      var res = S.res;
      await cargar(false);
      S.res = res;
      pintar();
    });
  }

  global.VZL = { S: S, pintar: pintar, cargar: cargar, params: params, aplicar: aplicar, revisar: revisar, correr: correr };
  enlazar();
  cargar();
})(typeof window !== 'undefined' ? window : globalThis);

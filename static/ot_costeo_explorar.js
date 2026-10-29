/* 🔎 Costeo de OT · Explorar (Daniel 2026-10-29, FASE 1) — el tablero para disertar ante la gerencia.
   «En el mes tuvimos 10 millones: lo pueda desglosar en centro de costo, técnico, rango de tiempo, interno o externo, quiénes
   son los proveedores… multifiltro, estilo Power BI: presionas un objeto y se filtra todo.»

   El servidor (/ot/api/costeo/costos-dataset, solo superadmin) entrega UNA fila por OT con la cuenta YA HECHA (la de su ficha,
   _ot_finanzas). Aquí solo se filtra y se suma:
     · filtrar(filas, filtros, excepto) → cada visual usa los filtros de las OTRAS dimensiones (ve el contexto y resalta lo
       elegido); las cifras y la tabla usan TODOS. Espejo en Python: ot_costeo.filtrar (el Excel de lo filtrado).
     · kpis(filas) → la suma única del proyecto (espejo de _ot_fin_agregado, por `clase`): interno y contrato APARTE, las que
       no se pueden medir FUERA y contadas, garantías y coberturas ADENTRO. Nunca un $0 ni un 100 % inventado.
     · La política de margen (REGLA #57) llega del servidor (meta.politica): aquí no hay un segundo número escrito a mano.
   Clic en una barra, celda o período: deja SOLO ese valor (otro clic lo quita); Ctrl+clic (o un toque en el celular) suma o
   quita. Al quitar un filtro SE RECALCULA TODO (REGLA #4.3, punto 4). El estado vive en el enlace (#explorar?…).
   Sin alert/confirm/prompt nativos (REGLA #1), nombres completos sin cortar (REGLA #15), celular primero (REGLA #3). */
(function (raiz, fabrica) {
  'use strict';
  var api = fabrica();
  if (typeof module === 'object' && module.exports) { module.exports = api; return; }
  raiz.OtCosteoExplorar = api;
  if (typeof document !== 'undefined') {
    if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', api.arrancar);
    else api.arrancar();
  }
})(typeof window !== 'undefined' ? window : this, function () {
  'use strict';

  var DIMS_TIEMPO = ['anio', 'mes', 'sem', 'nsem', 'dia'];
  var MESES = ['enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio', 'julio', 'agosto', 'septiembre', 'octubre',
    'noviembre', 'diciembre'];
  var MESES_CORTO = ['ene', 'feb', 'mar', 'abr', 'may', 'jun', 'jul', 'ago', 'sep', 'oct', 'nov', 'dic'];
  var DIAS = ['lunes', 'martes', 'miércoles', 'jueves', 'viernes', 'sábado', 'domingo'];
  var REGALADAS_DEF = ['garantia', 'regalia', 'arriendo_leasing', 'sin_costo'];
  var POL_DEF = { objetivo: 30, tolerancia: 20, perdida: 0 };
  var DIM_TXT_DEF = {
    estado: 'Estado', cli: 'Cliente', reg: 'Región', tec: 'Técnico', ttipo: 'Interno o externo', emp: 'Empresa o proveedor',
    cc: 'Centro de costo', tipo: 'Tipo de OT', cob: 'Cobertura', int: 'Intención', doc: 'Documento hoy',
    nac: 'Cómo nació la OT', banda: 'Margen según la política', clase: 'En la cuenta', anio: 'Año', mes: 'Mes',
    sem: 'Semana', nsem: 'N° de semana', dia: 'Día'
  };

  // ════════════════════════════════════════════════════════════════════════════════════════════════════════════
  //  Números y textos
  // ════════════════════════════════════════════════════════════════════════════════════════════════════════════
  /* round() de Python: correcto con el valor binario exacto y, en un empate exacto, al PAR (toFixed lo manda hacia
     afuera). Así las cifras del navegador son las mismas de _ot_fin_agregado. */
  function pyRound(x, d) {
    if (x === null || x === undefined || !isFinite(x)) return x;
    var s = Number(x).toFixed(Math.min(100, d + 60));
    var i = s.indexOf('.');
    var resto = s.slice(i + 1 + d);
    if (!/^50*$/.test(resto)) return Number(Number(x).toFixed(d));
    var base = s.slice(0, d ? i + 1 + d : i);
    var dig = base.replace('-', '').replace('.', '');
    if (parseInt(dig.charAt(dig.length - 1), 10) % 2 === 0) return Number(base);
    return Number(Number(x).toFixed(d));
  }

  function clp(n, signo) {
    if (n === null || n === undefined) return '—';
    var v = Math.round(Number(n) || 0);
    var s = '$' + String(Math.abs(v)).replace(/\B(?=(\d{3})+(?!\d))/g, '.');
    if (v < 0) return '-' + s;
    return (signo && v > 0) ? '+' + s : s;
  }

  /* Cifra corta para el eje del gráfico: «$5,2 M», «$850 mil». La exacta va en la etiqueta y en «Ver los números». */
  function compacto(n) {
    var v = Number(n) || 0, a = Math.abs(v), s = v < 0 ? '-$' : '$';
    if (a >= 1e6) return s + (Math.round(a / 1e5) / 10).toFixed(1).replace('.', ',').replace(/,0$/, '') + ' M';
    if (a >= 1e3) return s + Math.round(a / 1e3) + ' mil';
    return s + Math.round(a);
  }

  function pctTxt(p) {
    if (p === null || p === undefined) return '—';
    var t = pyRound(Number(p), 1).toFixed(1).replace('.', ',');     // mismo redondeo que ot_gerencial.pct_txt
    if (/,0$/.test(t)) t = t.slice(0, -2);
    return t + ' %';
  }

  function esc(s) {
    return String(s === null || s === undefined ? '' : s).replace(/&/g, '&amp;').replace(/</g, '&lt;')
      .replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#39;');
  }

  // ════════════════════════════════════════════════════════════════════════════════════════════════════════════
  //  Fechas (claves AAAA-MM-DD; a la vista siempre dd/mm/aaaa, REGLA #6)
  // ════════════════════════════════════════════════════════════════════════════════════════════════════════════
  function fechaUTC(iso) {
    var p = String(iso || '').split('-');
    return new Date(Date.UTC(+p[0], (+p[1] || 1) - 1, +p[2] || 1));
  }
  function isoDe(d) { return d.toISOString().slice(0, 10); }
  function sumarDias(iso, n) { var d = fechaUTC(iso); d.setUTCDate(d.getUTCDate() + n); return isoDe(d); }
  function diaSemana(iso) { return (fechaUTC(iso).getUTCDay() + 6) % 7; }        // 0 = lunes
  function lunesDe(iso) { return sumarDias(iso, -diaSemana(iso)); }
  function semanaISO(iso) {
    var d = fechaUTC(iso);
    d.setUTCDate(d.getUTCDate() + 3 - diaSemana(iso));                           // el jueves de esa semana
    var ene4 = new Date(Date.UTC(d.getUTCFullYear(), 0, 4));
    return 1 + Math.round(((d - ene4) / 86400000 - 3 + (ene4.getUTCDay() + 6) % 7) / 7);
  }
  function fechaTxt(iso) {
    if (!iso) return '';
    var p = String(iso).slice(0, 10).split('-');
    return p.length === 3 ? p[2] + '/' + p[1] + '/' + p[0] : '';
  }
  function mesMas(clave, k) {
    var p = clave.split('-');
    var i = (+p[0]) * 12 + (+p[1] - 1) + k;
    var a = Math.floor(i / 12), m = i % 12 + 1;
    return a + '-' + (m < 10 ? '0' : '') + m;
  }

  /* Nombre legible de una clave de tiempo (mismo texto que ot_costeo.etiqueta_tiempo). */
  function etiquetaTiempo(dim, k) {
    if (!k) return 'Sin fecha';
    if (dim === 'anio') return k;
    if (dim === 'mes') { var p = k.split('-'); var n = MESES[+p[1] - 1]; return n.charAt(0).toUpperCase() + n.slice(1) + ' ' + p[0]; }
    if (dim === 'nsem') return 'Semana ' + parseInt(k, 10);
    if (dim === 'sem') {
      var dom = sumarDias(k, 6), a = fechaUTC(k), b = fechaUTC(dom), txt;
      if (a.getUTCMonth() === b.getUTCMonth()) txt = a.getUTCDate() + ' al ' + b.getUTCDate() + ' de ' + MESES[b.getUTCMonth()];
      else txt = a.getUTCDate() + ' de ' + MESES_CORTO[a.getUTCMonth()] + ' al ' + b.getUTCDate() + ' de ' + MESES_CORTO[b.getUTCMonth()];
      return 'Semana ' + semanaISO(k) + ' · ' + txt + ' de ' + b.getUTCFullYear();
    }
    return DIAS[diaSemana(k)] + ' ' + fechaTxt(k);
  }
  /* Rótulo corto del eje del gráfico (el largo va en el título y en la etiqueta accesible). */
  function etiquetaCorta(dim, k) {
    if (!k) return '—';
    if (dim === 'anio') return k;
    if (dim === 'mes') { var p = k.split('-'); return MESES_CORTO[+p[1] - 1] + ' ' + p[0]; }
    if (dim === 'nsem') return 'S' + parseInt(k, 10);
    if (dim === 'sem') return 'S' + semanaISO(k) + ' · ' + fechaTxt(k).slice(0, 5);
    return fechaTxt(k).slice(0, 5);
  }

  // ════════════════════════════════════════════════════════════════════════════════════════════════════════════
  //  Datos: filtrar y sumar (espejo de ot_costeo.filtrar / ot_costeo.kpis)
  // ════════════════════════════════════════════════════════════════════════════════════════════════════════════
  function aObjetos(payload) {
    var cols = (payload && payload.cols) || [];
    return ((payload && payload.rows) || []).map(function (fila) {
      var o = {};
      for (var i = 0; i < cols.length; i++) o[cols[i]] = fila[i];
      return o;
    });
  }

  function txt(v) { return v === null || v === undefined ? '' : String(v); }

  function filtrar(filas, filtros, excepto) {
    var ex = excepto || [];
    var activos = [];
    Object.keys(filtros || {}).forEach(function (d) {
      var vs = filtros[d];
      if (vs && vs.length && ex.indexOf(d) < 0) {
        var s = Object.create(null);
        vs.forEach(function (v) { s[String(v)] = true; });
        activos.push([d, s]);
      }
    });
    if (!activos.length) return filas.slice();
    return filas.filter(function (r) {
      for (var i = 0; i < activos.length; i++) {
        if (!activos[i][1][txt(r[activos[i][0]])]) return false;
      }
      return true;
    });
  }

  function banda(pct, queda, pol) {
    pol = pol || POL_DEF;
    if (pct === null || pct === undefined) return (queda !== null && queda !== undefined && queda < 0) ? 'rojo' : null;
    if (pct < pol.perdida) return 'rojo';
    if (pct < pol.tolerancia) return 'amarillo';
    if (pct < pol.objetivo) return 'tolerancia';
    return 'objetivo';
  }

  function kpis(filas, regaladas, pol) {
    var reg = regaladas || REGALADAS_DEF;
    var t = { n: 0, cobre: 0, me_cobraron: 0, n_fuera: 0, cobre_fuera: 0, n_no_cobra: 0, costo_no_cobra: 0, aparte: {},
      n_ot: filas.length, a_pagar: 0, costo_total: 0, val_reg: 0, n_perdidas: 0, perdido: 0, costo_cubiertas: 0 };
    filas.forEach(function (r) {
      t.a_pagar += r.a_pagar || 0;
      t.costo_total += r.me_total || 0;
      if (reg.indexOf(r.cob) >= 0) t.val_reg += r.val || 0;
      var c = r.clase;
      if (c === 'aparte') {
        var a = t.aparte[r.cob] || (t.aparte[r.cob] = { n: 0, costo: 0 });
        a.n += 1;
        a.costo = pyRound(a.costo + (r.me_total || 0), 2);
        return;
      }
      if (c === 'sin_medir') { t.n_fuera += 1; t.cobre_fuera += r.cobre || 0; return; }
      t.n += 1;
      t.cobre += r.cobre || 0;
      t.me_cobraron += r.me_total || 0;
      if (!r.cobra) {
        t.n_no_cobra += 1;
        t.costo_no_cobra += r.me_total || 0;
        if (c === 'cubierta') t.costo_cubiertas += r.me_total || 0;
      } else if (c === 'perdida') {
        t.n_perdidas += 1;
        t.perdido += -(r.queda || 0);
      }
    });
    ['cobre', 'me_cobraron', 'cobre_fuera', 'costo_no_cobra', 'a_pagar', 'costo_total', 'val_reg', 'perdido',
      'costo_cubiertas'].forEach(function (k) { t[k] = pyRound(t[k], 2); });
    t.queda = pyRound(t.cobre - t.me_cobraron, 2);
    t.pct = t.cobre > 0 ? pyRound(t.queda / t.cobre * 100, 1) : null;
    var ac = 0, na = 0;
    Object.keys(t.aparte).forEach(function (k) { ac += t.aparte[k].costo; na += t.aparte[k].n; });
    t.aparte_costo = pyRound(ac, 2);
    t.n_aparte = na;
    // rojo sin % solo si hay algo QUE SE COBRA a pérdida (un grupo de puras garantías no «pierde»: cuesta, y va en gris)
    t.banda = banda(t.pct, (t.n && t.n_no_cobra < t.n) ? t.queda : null, pol);
    return t;
  }

  function agrupar(filas, dim, regaladas, pol) {
    var g = Object.create(null), orden = [];
    filas.forEach(function (r) {
      var k = txt(r[dim]);
      if (!g[k]) { g[k] = []; orden.push(k); }
      g[k].push(r);
    });
    return orden.map(function (k) { var s = kpis(g[k], regaladas, pol); s.clave = k; return s; });
  }

  var MEDIDAS = {
    cobre: { txt: 'Cobré', v: function (s) { return s.cobre; } },
    me: { txt: 'Me cobraron', v: function (s) { return s.me_cobraron; } },
    queda: { txt: 'Margen $', v: function (s) { return s.queda; } },
    a_pagar: { txt: 'Nos cobraron técnicos y proveedores', v: function (s) { return s.a_pagar; } },
    costo_total: { txt: 'Costo total con repuestos', v: function (s) { return s.costo_total; } },
    val_reg: { txt: 'Valorizado regalado (garantías y coberturas)', v: function (s) { return s.val_reg; } },
    n: { txt: 'N° de OT', v: function (s) { return s.n_ot; } }
  };
  function valorMedida(s, medida) { return (MEDIDAS[medida] || MEDIDAS.cobre).v(s); }

  function ordenar(grupos, medida, fijo) {
    if (fijo && fijo.length) {
      return grupos.slice().sort(function (a, b) {
        var ia = fijo.indexOf(a.clave), ib = fijo.indexOf(b.clave);
        return (ia < 0 ? 999 : ia) - (ib < 0 ? 999 : ib) || (a.clave < b.clave ? -1 : 1);
      });
    }
    return grupos.slice().sort(function (a, b) {
      return (valorMedida(b, medida) - valorMedida(a, medida)) || (b.n_ot - a.n_ot) || (a.clave < b.clave ? -1 : a.clave > b.clave ? 1 : 0);
    });
  }

  /* Matriz filas × columnas (técnico × centro de costo): valor de cada celda con la medida elegida y el mayor de cada
     columna (quién gastó más en ese centro). */
  function matriz(filas, dimF, dimC, medida, regaladas, pol) {
    var porF = Object.create(null), porC = Object.create(null), celdas = Object.create(null);
    filas.forEach(function (r) {
      var f = txt(r[dimF]), c = txt(r[dimC]);
      (porF[f] = porF[f] || []).push(r);
      (porC[c] = porC[c] || []).push(r);
      var k = f + '\u0001' + c;
      (celdas[k] = celdas[k] || []).push(r);
    });
    var totF = {}, totC = {};
    Object.keys(porF).forEach(function (f) { totF[f] = valorMedida(kpis(porF[f], regaladas, pol), medida); });
    Object.keys(porC).forEach(function (c) { totC[c] = valorMedida(kpis(porC[c], regaladas, pol), medida); });
    var fs = Object.keys(porF).sort(function (a, b) { return (totF[b] - totF[a]) || (a < b ? -1 : 1); });
    var cs = Object.keys(porC).sort(function (a, b) { return (totC[b] - totC[a]) || (a < b ? -1 : 1); });
    var val = {}, max = {}, n = {};
    Object.keys(celdas).forEach(function (k) {
      var p = k.split('\u0001');
      var v = valorMedida(kpis(celdas[k], regaladas, pol), medida);
      (val[p[0]] = val[p[0]] || {})[p[1]] = v;
      (n[p[0]] = n[p[0]] || {})[p[1]] = celdas[k].length;
      if (max[p[1]] === undefined || v > max[p[1]]) max[p[1]] = v;
    });
    var total = valorMedida(kpis(filas, regaladas, pol), medida);
    return { filas: fs, cols: cs, val: val, n: n, max: max, totF: totF, totC: totC, total: total };
  }

  /* Claves de tiempo consecutivas entre `desde` y `hasta` (para que la serie no salte los períodos sin OT). */
  function clavesRango(gran, desde, hasta) {
    var out = [];
    if (!desde || !hasta) return out;
    if (gran === 'anio') { for (var a = +desde.slice(0, 4); a <= +hasta.slice(0, 4); a++) out.push(String(a)); return out; }
    if (gran === 'mes') { var m = desde.slice(0, 7); while (m <= hasta.slice(0, 7) && out.length < 400) { out.push(m); m = mesMas(m, 1); } return out; }
    if (gran === 'sem') { var l = lunesDe(desde); while (l <= hasta && out.length < 200) { out.push(l); l = sumarDias(l, 7); } return out; }
    if (gran === 'dia') {
      if ((fechaUTC(hasta) - fechaUTC(desde)) / 86400000 > 120) return out;
      var d = desde; while (d <= hasta) { out.push(d); d = sumarDias(d, 1); } return out;
    }
    return out;
  }

  function serie(filas, gran, desde, hasta, regaladas, pol) {
    var mapa = Object.create(null);
    agrupar(filas, gran, regaladas, pol).forEach(function (g) { mapa[g.clave] = g; });
    var claves = clavesRango(gran, desde, hasta);
    Object.keys(mapa).forEach(function (k) { if (k && claves.indexOf(k) < 0) claves.push(k); });
    claves.sort();
    return claves.map(function (k) {
      if (mapa[k]) return mapa[k];
      var v = kpis([], regaladas, pol); v.clave = k; return v;
    });
  }

  /* Contra el período anterior (mismo criterio que ot_gerencial.comparar). */
  function comparar(valor, previo, mejor) {
    var v = Number(valor) || 0;
    if (previo === null || previo === undefined) return { flecha: 'sin_base', delta: null, delta_pct: null, tono: 'neutro', previo: null };
    var p = Number(previo) || 0;
    var delta = pyRound(v - p, 2);
    if (Math.abs(delta) < 0.5) return { flecha: 'igual', delta: 0, delta_pct: 0, tono: 'neutro', previo: p };
    var dpct = Math.abs(p) >= 1 ? pyRound(delta / Math.abs(p) * 100, 1) : null;
    var flecha = delta > 0 ? 'sube' : 'baja';
    var tono = mejor === 'neutro' ? 'neutro' : (flecha === mejor ? 'bueno' : 'malo');
    return { flecha: flecha, delta: delta, delta_pct: dpct, tono: tono, previo: p };
  }

  function desplazar(dim, k) {
    if (dim === 'anio') return String(+k - 1);
    if (dim === 'mes') return mesMas(k, -1);
    if (dim === 'sem') return sumarDias(k, -7);
    if (dim === 'dia') return sumarDias(k, -1);
    if (dim === 'nsem') { var n = parseInt(k, 10) - 1; return n >= 1 ? (n < 10 ? '0' : '') + n : null; }
    return null;
  }
  function inicioDe(dim, k) {
    if (dim === 'anio') return k + '-01-01';
    if (dim === 'mes') return k + '-01';
    return k;
  }

  /* Las OT del período ANTERIOR con los mismos filtros (para comparar). Sin filtro de tiempo: el período anterior del mismo
     largo que el rango (viene cargado). Con UNA unidad de tiempo filtrada: la unidad anterior (sep → oct, 2025 → 2026…),
     siempre que esté cargada. Con dos unidades a la vez no hay un anterior único: se dice. */
  function previo(todas, filtros, meta) {
    meta = meta || {};
    var tiempo = DIMS_TIEMPO.filter(function (d) { return (filtros[d] || []).length; });
    var resto = {};
    Object.keys(filtros || {}).forEach(function (d) { if (DIMS_TIEMPO.indexOf(d) < 0) resto[d] = filtros[d]; });
    if (!tiempo.length) {
      return { filas: filtrar(todas.filter(function (r) { return !!r.prev; }), resto),
        texto: 'el período anterior (' + fechaTxt(meta.carga_desde) + ' al ' + fechaTxt(meta.previo_hasta) + ')' };
    }
    if (tiempo.length > 1) return { filas: null, texto: 'Con filtros en dos unidades de tiempo a la vez no hay un período anterior único para comparar.' };
    var d = tiempo[0], vals = [];
    for (var i = 0; i < filtros[d].length; i++) {
      var w = desplazar(d, filtros[d][i]);
      if (w === null) return { filas: null, texto: 'La semana 1 no tiene una semana anterior para comparar.' };
      vals.push(w);
    }
    if (d !== 'nsem' && meta.carga_desde) {
      var ini = vals.map(function (w) { return inicioDe(d, w); }).sort()[0];
      if (ini < meta.carga_desde) return { filas: null, texto: 'El período anterior a lo elegido no está cargado: amplía «Desde» para compararlo.' };
    }
    var f2 = {};
    Object.keys(resto).forEach(function (k) { f2[k] = resto[k]; });
    f2[d] = vals;
    return { filas: filtrar(todas, f2), texto: vals.map(function (w) { return etiquetaTiempo(d, w); }).join(', ') };
  }

  // ── Filtros: alternar, quitar ──────────────────────────────────────────────────────────────────────────────────
  function clonar(f) { var o = {}; Object.keys(f || {}).forEach(function (k) { o[k] = (f[k] || []).slice(); }); return o; }

  /* modo 'solo': deja SOLO ese valor en su dimensión (si ya era el único, lo quita). modo 'sumar' (Ctrl+clic o toque):
     agrega o quita ese valor sin tocar los demás. */
  function alternar(filtros, dim, valor, modo) {
    var f = clonar(filtros);
    var vs = f[dim] || [];
    var i = vs.indexOf(valor);
    if (modo === 'sumar') { if (i >= 0) vs.splice(i, 1); else vs.push(valor); }
    else vs = (i >= 0 && vs.length === 1) ? [] : [valor];
    if (vs.length) f[dim] = vs; else delete f[dim];
    return f;
  }
  function alternarPar(filtros, d1, v1, d2, v2, modo) {
    var f = clonar(filtros);
    var ya = (f[d1] || []).length === 1 && f[d1][0] === v1 && (f[d2] || []).length === 1 && f[d2][0] === v2;
    if (ya) { delete f[d1]; delete f[d2]; return f; }
    if (modo === 'sumar') {
      [[d1, v1], [d2, v2]].forEach(function (p) { var vs = f[p[0]] || []; if (vs.indexOf(p[1]) < 0) vs.push(p[1]); f[p[0]] = vs; });
      return f;
    }
    f[d1] = [v1];
    f[d2] = [v2];
    return f;
  }
  function quitar(filtros, dim, valor) {
    var f = clonar(filtros);
    if (valor === undefined) { delete f[dim]; return f; }
    var vs = (f[dim] || []).filter(function (x) { return x !== valor; });
    if (vs.length) f[dim] = vs; else delete f[dim];
    return f;
  }

  // ── Estado en el enlace (#explorar?…) ──────────────────────────────────────────────────────────────────────────
  var DEF = { d: '', h: '', fe: 'cierre', al: 'cerradas', g: 'mes', m: 'cobre', mx: 'a_pagar', mr: 'tec', f: {} };
  var VALIDOS = {
    fe: ['cierre', 'ejecucion', 'creacion', 'programada'], al: ['cerradas', 'todas'], g: DIMS_TIEMPO,
    m: ['cobre', 'me', 'queda', 'a_pagar', 'n'], mx: ['a_pagar', 'costo_total', 'val_reg', 'cobre'], mr: ['tec', 'emp']
  };
  var FILTRABLES = ['estado', 'cli', 'reg', 'tec', 'ttipo', 'emp', 'cc', 'tipo', 'cob', 'int', 'doc', 'nac', 'banda',
    'clase'].concat(DIMS_TIEMPO);

  function estadoAHash(e) {
    var q = new URLSearchParams();
    ['d', 'h', 'fe', 'al', 'g', 'm', 'mx', 'mr'].forEach(function (k) { if (e[k]) q.set(k, e[k]); });
    if (e.f && Object.keys(e.f).length) q.set('f', JSON.stringify(e.f));
    return '#explorar?' + q.toString();
  }

  function hashAEstado(hash) {
    if (!/^#explorar/.test(hash || '')) return null;
    var q = new URLSearchParams(String(hash).split('?').slice(1).join('?'));
    var e = clonar({});
    Object.keys(DEF).forEach(function (k) { e[k] = k === 'f' ? {} : DEF[k]; });
    ['d', 'h'].forEach(function (k) { var v = q.get(k) || ''; if (/^\d{4}-\d{2}-\d{2}$/.test(v)) e[k] = v; });
    Object.keys(VALIDOS).forEach(function (k) { var v = q.get(k); if (v && VALIDOS[k].indexOf(v) >= 0) e[k] = v; });
    try {
      var f = JSON.parse(q.get('f') || '{}');
      Object.keys(f || {}).forEach(function (d) {
        if (FILTRABLES.indexOf(d) >= 0 && Array.isArray(f[d])) {
          var vs = f[d].filter(function (x) { return typeof x === 'string' || typeof x === 'number'; }).map(String).slice(0, 500);
          if (vs.length) e.f[d] = vs;
        }
      });
    } catch (err) { /* un enlace dañado no rompe la pantalla: arranca sin filtros */ }
    return e;
  }

  // ════════════════════════════════════════════════════════════════════════════════════════════════════════════
  //  Pantalla
  // ════════════════════════════════════════════════════════════════════════════════════════════════════════════
  var UI = { raiz: null, estado: null, payload: null, todas: [], actuales: [], cargado: null, cargando: false, error: null,
    sel: {}, pag: 1, pp: 25, ord: 'fecha', ver: {}, puntero: 'mouse', pres: false };

  function meta() { return (UI.payload && UI.payload.meta) || {}; }
  function pol() { return meta().politica || POL_DEF; }
  function reg() { return meta().regaladas || REGALADAS_DEF; }
  function dimTxt(d) { return (meta().dims_txt || DIM_TXT_DEF)[d] || DIM_TXT_DEF[d] || d; }
  function lab(dim, k) {
    if (DIMS_TIEMPO.indexOf(dim) >= 0) return etiquetaTiempo(dim, k);
    var m = (UI.payload && UI.payload.dims && UI.payload.dims[dim]) || {};
    return m[k] !== undefined ? m[k] : k;
  }
  function bandaTxt(b) { var t = (pol().txt || {}); return b ? (t[b] || b) : 'Sin medir'; }

  function irA(pestana, escribir) {
    var exp = pestana === 'explorar';
    var res = document.getElementById('gcApp');
    if (res) res.hidden = exp;
    if (UI.raiz) UI.raiz.hidden = !exp;
    Array.prototype.forEach.call(document.querySelectorAll('[data-gc-pestana]'), function (t) {
      var on = t.getAttribute('data-gc-pestana') === pestana;
      t.classList.toggle('on', on);
      t.setAttribute('aria-selected', on ? 'true' : 'false');
      t.tabIndex = on ? 0 : -1;
    });
    Array.prototype.forEach.call(document.querySelectorAll('[data-gc-solo="resumen"]'), function (el) { el.hidden = exp; });
    if (exp) {
      if (escribir) guardarHash();
      asegurarDatos();
    } else if (escribir && window.history && history.replaceState) {
      history.replaceState(null, '', location.pathname + location.search);
    }
  }

  function guardarHash() {
    if (window.history && history.replaceState) history.replaceState(null, '', location.pathname + location.search + estadoAHash(UI.estado));
  }

  function claveCarga() { var e = UI.estado; return [e.d, e.h, e.fe, e.al].join('|'); }

  function asegurarDatos(forzar) {
    if (UI.cargando) return;
    if (!forzar && UI.payload && UI.cargado === claveCarga()) { pintar(); return; }
    cargar(forzar);
  }

  function cargar(forzar) {
    var e = UI.estado;
    var pedido = claveCarga();
    var q = new URLSearchParams();
    if (e.d) q.set('desde', e.d);
    if (e.h) q.set('hasta', e.h);
    q.set('fecha', e.fe);
    q.set('alcance', e.al);
    if (forzar) q.set('actualizar', '1');
    UI.cargando = true;
    UI.error = null;
    pintarCarga();
    fetch(UI.raiz.getAttribute('data-url') + '?' + q.toString(), {
      credentials: 'same-origin', headers: { 'Accept': 'application/json', 'X-Requested-With': 'XMLHttpRequest' }
    }).then(function (r) {
      return r.json().catch(function () { return {}; }).then(function (j) { return { ok: r.ok, j: j }; });
    }).then(function (x) {
      UI.cargando = false;
      if (!x.ok || !x.j || !x.j.ok) throw new Error((x.j && x.j.error) || 'No pudimos leer el costeo. Vuelve a intentarlo.');
      if (claveCarga() !== pedido) { cargar(false); return; }   // el rango cambió mientras se leía: se pide el nuevo
      UI.payload = x.j;
      UI.todas = aObjetos(x.j);
      UI.actuales = UI.todas.filter(function (r) { return !r.prev; });
      UI.estado.d = x.j.meta.desde;
      UI.estado.h = x.j.meta.hasta;
      UI.cargado = claveCarga();
      UI.pag = 1;
      UI.sel = {};
      guardarHash();
      pintar();
    }).catch(function (err) {
      UI.cargando = false;
      UI.error = (err && err.message) || 'No pudimos leer el costeo.';
      pintarError();
    });
  }

  function pintarCarga() {
    if (!UI.payload) UI.raiz.innerHTML = '<div class="cx-cargando" role="status"><i class="bi bi-hourglass-split"></i><span>Cargando las OT del período…</span></div>';
    else UI.raiz.classList.add('cx-ocupado');
  }

  function pintarError() {
    UI.raiz.classList.remove('cx-ocupado');
    UI.raiz.innerHTML = '<div class="cx-aviso" role="alert"><i class="bi bi-exclamation-circle"></i><span>' + esc(UI.error) +
      '</span> <button type="button" class="cx-btn cx-btn-sec" data-cx-acc="reintentar"><i class="bi bi-arrow-clockwise"></i>Reintentar</button></div>';
  }

  // ── Piezas de la pantalla ───────────────────────────────────────────────────────────────────────────────────────
  function ayuda(t) {
    return '<details class="cx-ayuda"><summary><i class="bi bi-question-circle"></i> ¿Cómo se calcula?</summary><p>' + esc(t) + '</p></details>';
  }

  function clasif() {
    return '<span class="cx-clasif"><i class="bi bi-shield-lock-fill"></i>' + esc(meta().clasificacion || 'Confidencial · solo gerencia') + '</span>';
  }

  function barraSuperior() {
    var e = UI.estado, M = meta(), ft = M.fechas_txt || {};
    var opciones = ['cierre', 'ejecucion', 'creacion', 'programada'].map(function (k) {
      return '<option value="' + k + '"' + (e.fe === k ? ' selected' : '') + '>' + esc(ft[k] || k) + '</option>';
    }).join('');
    return '<header class="cx-cab"><div><h2>Explorar el costeo</h2><p>Toca una barra, una celda o un período y todo el tablero se filtra. ' +
      '<b>Ctrl + clic</b> suma otro valor; en el celular cada toque suma o quita.</p></div>' + clasif() + '</header>' +
      '<form class="cx-rango" data-cx-form="rango">' +
      '<label>Desde<input type="date" name="d" value="' + esc(e.d) + '" required></label>' +
      '<label>Hasta<input type="date" name="h" value="' + esc(e.h) + '" required></label>' +
      '<label>Fecha que manda<select name="fe">' + opciones + '</select></label>' +
      '<label class="cx-check"><input type="checkbox" name="al"' + (e.al === 'todas' ? ' checked' : '') + '><span>Incluir OT abiertas</span></label>' +
      '<button type="submit" class="cx-btn"><i class="bi bi-funnel"></i>Aplicar</button>' +
      '</form>' +
      '<div class="cx-atajos" role="group" aria-label="Rangos rápidos">' +
      [['mes', 'Este mes'], ['mes_ant', 'Mes anterior'], ['anio', 'Este año'], ['12m', 'Últimos 12 meses']].map(function (a) {
        return '<button type="button" class="cx-chip-t" data-cx-acc="atajo" data-val="' + a[0] + '">' + a[1] + '</button>';
      }).join('') +
      '<span class="cx-esp"></span>' +
      '<button type="button" class="cx-btn cx-btn-sec" data-cx-acc="actualizar"><i class="bi bi-arrow-clockwise"></i>Actualizar</button>' +
      '<button type="button" class="cx-btn cx-btn-sec" data-cx-acc="presentacion"><i class="bi bi-display"></i>' + (UI.pres ? 'Salir de la presentación' : 'Modo presentación') + '</button>' +
      '<button type="button" class="cx-btn" data-cx-acc="excel"><i class="bi bi-file-earmark-spreadsheet"></i>Excel de lo filtrado</button>' +
      '</div>';
  }

  function chips() {
    var f = UI.estado.f, out = [];
    FILTRABLES.forEach(function (d) {
      (f[d] || []).forEach(function (v) {
        out.push('<button type="button" class="cx-chip" data-cx-acc="quitar" data-dim="' + esc(d) + '" data-val="' + esc(v) +
          '" aria-label="Quitar el filtro ' + esc(dimTxt(d) + ': ' + lab(d, v)) + '"><span><b>' + esc(dimTxt(d)) + ':</b> ' +
          esc(lab(d, v)) + '</span><i class="bi bi-x-lg" aria-hidden="true"></i></button>');
      });
    });
    var M = meta();
    var rango = 'Del ' + fechaTxt(M.desde) + ' al ' + fechaTxt(M.hasta) + ' · ' + esc(M.fecha_txt || '') + ' · ' +
      esc((M.alcances_txt || {})[M.alcance] || '');
    if (!out.length) {
      return '<div class="cx-chips" aria-live="polite"><span class="cx-rango-txt">' + rango + '</span><span class="cx-chips-vacio">Sin filtros: se ven todas las OT del rango.</span></div>';
    }
    return '<div class="cx-chips" aria-live="polite"><span class="cx-rango-txt">' + rango + '</span>' + out.join('') +
      '<button type="button" class="cx-chip cx-chip-limpiar" data-cx-acc="limpiar"><i class="bi bi-eraser"></i><span>Limpiar todo</span></button></div>';
  }

  function avisos() {
    var M = meta(), out = [];
    if (M.truncado) out.push('Se leyeron las ' + M.limite + ' OT más recientes: hay más en el rango (y su período anterior). Acorta el rango para verlas todas.');
    if (M.fallos && M.fallos.length) out.push('No se pudo leer: ' + M.fallos.join(', ') + '. Lo que depende de eso puede verse incompleto; usa «Actualizar».');
    if (M.fecha === 'cierre' && M.alcance === 'todas') out.push('Con la fecha de cierre, las OT abiertas no aparecen (todavía no tienen fecha de cierre). Para verlas, elige la fecha de creación, programada o de ejecución.');
    return out.map(function (t) { return '<div class="cx-aviso"><i class="bi bi-info-circle"></i><span>' + esc(t) + '</span></div>'; }).join('');
  }

  /* modo: '$' (montos), 'puntos' (un margen %) o 'n' (cantidad de OT). */
  function cmpHTML(c, modo) {
    var previo = function (v) { return modo === 'puntos' ? pctTxt(v) : modo === 'n' ? String(Math.round(v)) + ' OT' : clp(v); };
    if (c.flecha === 'sin_base') return '<p class="cx-cmp n"><i class="bi bi-dash-circle"></i>Sin período anterior para comparar</p>';
    if (c.flecha === 'igual') return '<p class="cx-cmp n"><i class="bi bi-dash-lg"></i>Igual que antes (' + esc(previo(c.previo)) + ')</p>';
    var signo = c.delta > 0 ? '+' : '';
    var d = modo === 'puntos' ? signo + String(pyRound(c.delta, 1)).replace('.', ',') + ' puntos'
      : modo === 'n' ? signo + Math.round(c.delta) + ' OT' + (c.delta_pct !== null ? ' (' + pctTxt(c.delta_pct) + ')' : '')
        : clp(c.delta, true) + (c.delta_pct !== null ? ' (' + pctTxt(c.delta_pct) + ')' : '');
    return '<p class="cx-cmp ' + c.tono + '"><i class="bi ' + (c.flecha === 'sube' ? 'bi-arrow-up-right' : 'bi-arrow-down-right') + '"></i>' +
      esc(d) + ' frente a antes (' + esc(previo(c.previo)) + ')</p>';
  }

  function tarjetas(k, kp, pr) {
    var hay = !!kp;
    var cmpTxt = hay ? '<p class="cx-cmp-base">Comparado con ' + esc(pr.texto) + '</p>' : '<p class="cx-cmp-base">' + esc(pr.texto) + '</p>';
    var cmp = function (clave, mejor) { return cmpHTML(comparar(k[clave], hay ? kp[clave] : null, mejor), '$'); };
    var cmpPct = (k.pct !== null && hay && kp.pct !== null) ? cmpHTML(comparar(k.pct, kp.pct, 'sube'), 'puntos')
      : '<p class="cx-cmp n"><i class="bi bi-dash-circle"></i>Sin margen para comparar</p>';
    var b = k.banda;
    var escapa = pyRound(k.costo_no_cobra + k.aparte_costo, 2);
    var P = pol();
    var t = [];
    t.push('<article class="cx-kpi"><header><i class="bi bi-cash-coin"></i>Cobré</header><div class="cx-cifra">' + clp(k.cobre) + '</div>' + cmp('cobre', 'sube') +
      ayuda('Lo cobrado al cliente (servicio + despacho) en las OT medidas que se cobran. Cada monto es el de la ficha de la OT; la suma es la regla única del proyecto (la misma de Vida del cliente y del Resumen).') + '</article>');
    t.push('<article class="cx-kpi"><header><i class="bi bi-person-workspace"></i>Me cobraron</header><div class="cx-cifra">' + clp(k.me_cobraron) + '</div>' + cmp('me_cobraron', 'neutro') +
      ayuda('Lo que cobraron técnicos y proveedores (con el despacho) más los repuestos instalados, en las OT medidas. Incluye lo que costaron las garantías y coberturas: es plata que salió.') + '</article>');
    t.push('<article class="cx-kpi ' + (k.queda < 0 && b === 'rojo' ? 'k-rojo' : '') + '"><header><i class="bi bi-graph-up-arrow"></i>Margen $</header><div class="cx-cifra">' + clp(k.queda) + '</div>' + cmp('queda', 'sube') +
      ayuda('Cobré − Me cobraron. Las OT a las que les falta un costo no entran (no se inventa un cero) y el trabajo interno y los contratos van aparte.') + '</article>');
    t.push('<article class="cx-kpi k-banda b-' + (b || 'gris') + '"><header><i class="bi bi-bullseye"></i>Margen %</header><div class="cx-cifra">' + pctTxt(k.pct) + '</div>' +
      '<p class="cx-banda-txt">' + esc(k.pct === null ? 'Sin cobros medidos para sacar el margen' : bandaTxt(b)) + '</p>' + cmpPct +
      ayuda('Margen sobre lo cobrado. Política de ILUS Fitness: objetivo ' + P.objetivo + ' % o más; tolerancia desde ' + P.tolerancia + ' %; bajo ' + P.tolerancia + ' % en amarillo; bajo ' + P.perdida + ' % en rojo (se pierde plata).') + '</article>');
    t.push('<article class="cx-kpi"><header><i class="bi bi-list-ol"></i>N° de OT</header><div class="cx-cifra">' + k.n_ot + '</div>' +
      '<p class="cx-det">' + k.n + ' medidas · ' + k.n_fuera + ' sin medir · ' + k.n_aparte + ' aparte</p>' + cmpHTML(comparar(k.n_ot, hay ? kp.n_ot : null, 'neutro'), 'n') +
      ayuda('Todas las OT filtradas. «Medidas» entran a las cifras; «sin medir» les falta un costo; «aparte» son trabajo interno o mantención de contrato.') + '</article>');
    t.push('<article class="cx-kpi ' + (k.n_fuera ? 'k-ambar' : '') + '"><header><i class="bi bi-calculator"></i>Sin medir</header><div class="cx-cifra">' + k.n_fuera + '</div>' +
      '<p class="cx-det">Cobrado en ellas: ' + clp(k.cobre_fuera) + '</p>' +
      (k.n_fuera ? '<button type="button" class="cx-btn-link" data-cx-acc="filtro" data-dim="clase" data-val="sin_medir">Ver cuáles</button>' : '') +
      ayuda('OT a las que les falta lo que cobró el técnico o el costo del despacho. No están en las cifras: sumarlas con un cero inventaría una ganancia. Completar el costo en la OT las suma.') + '</article>');
    t.push('<article class="cx-kpi"><header><i class="bi bi-box-arrow-up-right"></i>Se escapa</header><div class="cx-cifra">' + clp(escapa) + '</div>' +
      '<p class="cx-det">Garantías y coberturas ' + clp(k.costo_no_cobra) + ' · interno y contratos ' + clp(k.aparte_costo) + '</p>' +
      '<p class="cx-det">Valor del servicio regalado: ' + clp(k.val_reg) + '</p>' +
      ayuda('Lo que costó lo que NO se cobró: garantías, regalías, arriendo o leasing, cortesías y desinstalaciones cubiertas (ya restan en el margen) más el trabajo interno y las mantenciones de contrato (van aparte, no son negocio con nadie). El valor regalado es la tarifa de referencia, no un cobro.') + '</article>');
    return '<section class="cx-kpis" aria-label="Cifras de lo filtrado">' + t.join('') + '</section>' + cmpTxt;
  }

  function claseItem(dim, k) {
    var vs = UI.estado.f[dim] || [];
    if (!vs.length) return '';
    return vs.indexOf(k) >= 0 ? ' on' : ' apagado';
  }
  function pressed(dim, k) { return (UI.estado.f[dim] || []).indexOf(k) >= 0 ? 'true' : 'false'; }

  /* El margen de un grupo. Lo que no se cobra (garantías, interno) dice lo que costó, en gris: el rojo es solo para una
     pérdida real de algo que se cobra. */
  function margenHTML(s) {
    if (!s.n) return '<em class="b-gris">' + (s.n_aparte && !s.n_fuera ? 'va aparte · costó ' + clp(s.aparte_costo) : 'sin medir') + '</em>';
    if (s.cobre === 0 && s.n_no_cobra === s.n) return '<em class="b-gris">no se cobra · nos costó ' + clp(s.me_cobraron) + '</em>';
    if (s.pct === null) return '<em class="b-' + (s.banda || 'gris') + '">' + clp(s.queda) + '</em>';
    return '<em class="b-' + (s.banda || 'gris') + '">' + clp(s.queda) + ' (' + pctTxt(s.pct) + ')</em>';
  }
  function detalleGrupo(s) {
    if (!s.n || (s.cobre === 0 && s.n_no_cobra === s.n)) return margenHTML(s);
    return 'Cobré ' + clp(s.cobre) + ' · Me cobraron ' + clp(s.me_cobraron) + ' · Margen ' + margenHTML(s);
  }

  /* Lista de barras de una dimensión: se calcula con los filtros de las OTRAS dimensiones; lo elegido queda resaltado. */
  function barras(dim, titulo, opciones) {
    opciones = opciones || {};
    var e = UI.estado, M = meta();
    var filas = filtrar(UI.actuales, e.f, [dim]);
    var medida = opciones.medida || e.m;
    var fijo = opciones.fijo ? ((M.orden || {})[dim] || opciones.fijo) : null;
    var grupos = ordenar(agrupar(filas, dim, reg(), pol()), medida, fijo);
    var tope = opciones.tope || 8;
    var todos = !!UI.ver[dim];
    var visibles = todos ? grupos : grupos.slice(0, tope);
    var maxAbs = 0;
    grupos.forEach(function (g) { maxAbs = Math.max(maxAbs, Math.abs(valorMedida(g, medida))); });
    var items = visibles.map(function (g) {
      var v = valorMedida(g, medida);
      var ancho = (maxAbs > 0 && v) ? Math.max(1.5, Math.abs(v) / maxAbs * 100) : 0;
      var sub = g.n_ot + ' OT';
      if (dim === 'tec' && UI.payload.tec_info && UI.payload.tec_info[g.clave] && UI.payload.tec_info[g.clave].ttipo === 'externo') {
        sub = lab('emp', UI.payload.tec_info[g.clave].emp) + ' · ' + sub;
      } else if (dim === 'tec' && UI.payload.tec_info && UI.payload.tec_info[g.clave]) {
        sub = lab('ttipo', UI.payload.tec_info[g.clave].ttipo) + ' · ' + sub;
      }
      var det = detalleGrupo(g);
      if (dim === 'tec' || dim === 'emp' || dim === 'ttipo') det += ' · <span class="cx-nos">Nos cobró ' + clp(g.a_pagar) + '</span>';
      return '<li><button type="button" class="cx-item' + claseItem(dim, g.clave) + '" data-cx-acc="filtro" data-dim="' + esc(dim) +
        '" data-val="' + esc(g.clave) + '" aria-pressed="' + pressed(dim, g.clave) + '">' +
        '<span class="cx-item-t"><b>' + esc(lab(dim, g.clave)) + '</b><small>' + esc(sub) + '</small></span>' +
        '<span class="cx-item-v' + (v < 0 ? ' neg' : '') + '">' + (medida === 'n' ? v : clp(v)) + '</span>' +
        '<span class="cx-pista"><i class="' + (v < 0 ? 'neg' : '') + (dim === 'banda' ? ' b-' + g.clave : '') + '" style="width:' + ancho.toFixed(1) + '%"></i></span>' +
        '<span class="cx-item-d">' + det + '</span></button></li>';
    }).join('');
    var resto = grupos.length - visibles.length;
    var otros = '';
    if (opciones.otros && resto > 0 && !todos) {
      var oFilas = [];
      var claves = {};
      grupos.slice(tope).forEach(function (g) { claves[g.clave] = true; });
      filas.forEach(function (r) { if (claves[txt(r[dim])]) oFilas.push(r); });
      var o = kpis(oFilas, reg(), pol());
      otros = '<li><div class="cx-item cx-otros"><span class="cx-item-t"><b>Otros ' + resto + '</b><small>' + o.n_ot + ' OT</small></span>' +
        '<span class="cx-item-v">' + (medida === 'n' ? o.n_ot : clp(valorMedida(o, medida))) + '</span>' +
        '<span class="cx-item-d">' + detalleGrupo(o) + '</span></div></li>';
    }
    var ver = grupos.length > tope ? '<button type="button" class="cx-btn-link" data-cx-acc="ver" data-dim="' + esc(dim) + '">' +
      (todos ? 'Ver menos' : 'Ver los ' + grupos.length) + '</button>' : '';
    var limpiar = (e.f[dim] || []).length ? '<button type="button" class="cx-btn-link" data-cx-acc="limpiar-dim" data-dim="' + esc(dim) + '">Quitar este filtro</button>' : '';
    return '<article class="cx-card cx-lista" data-cx-dim="' + esc(dim) + '"><header class="cx-card-h"><h3>' + esc(titulo || dimTxt(dim)) + '</h3>' +
      '<small>' + esc((MEDIDAS[medida] || MEDIDAS.cobre).txt) + '</small></header>' +
      (grupos.length ? '<ul>' + items + otros + '</ul>' : '<div class="cx-vacio">Sin OT con los filtros de las otras dimensiones.</div>') +
      '<div class="cx-card-pie">' + ver + limpiar + '</div>' + (opciones.ayuda ? ayuda(opciones.ayuda) : '') + '</article>';
  }

  function selectorMedida() {
    var e = UI.estado;
    return '<div class="cx-medida"><label>Las barras muestran<select data-cx-cambio="m">' + ['cobre', 'me', 'queda', 'a_pagar', 'n'].map(function (m) {
      return '<option value="' + m + '"' + (e.m === m ? ' selected' : '') + '>' + esc(MEDIDAS[m].txt) + '</option>';
    }).join('') + '</select></label></div>';
  }

  function serieHTML() {
    var e = UI.estado, M = meta(), g = e.g;
    var filas = filtrar(UI.actuales, e.f, [g]);
    var s = serie(filas, g, M.desde, M.hasta, reg(), pol());
    var max = 0;
    s.forEach(function (x) { max = Math.max(max, x.cobre, x.me_cobraron); });
    var seg = [['dia', 'Día'], ['sem', 'Semana'], ['nsem', 'N° de semana'], ['mes', 'Mes'], ['anio', 'Año']].map(function (o) {
      return '<button type="button" class="cx-seg-b' + (g === o[0] ? ' on' : '') + '" data-cx-acc="gran" data-val="' + o[0] + '" aria-pressed="' + (g === o[0]) + '">' + o[1] + '</button>';
    }).join('');
    var cols = s.map(function (x) {
      var hc = max > 0 ? x.cobre / max * 100 : 0, hm = max > 0 ? x.me_cobraron / max * 100 : 0;
      var largo = etiquetaTiempo(g, x.clave);
      var aria = largo + ': cobré ' + clp(x.cobre) + ', me cobraron ' + clp(x.me_cobraron) + ', margen ' + (x.pct === null ? 'sin medir' : pctTxt(x.pct)) + ', ' + x.n_ot + ' OT';
      return '<button type="button" class="cx-col' + claseItem(g, x.clave) + '" data-cx-acc="filtro" data-dim="' + g + '" data-val="' + esc(x.clave) +
        '" aria-pressed="' + pressed(g, x.clave) + '" aria-label="' + esc(aria) + '" title="' + esc(aria) + '">' +
        '<span class="cx-col-pct b-' + (x.banda || 'gris') + '">' + (x.pct === null ? '·' : esc(pctTxt(x.pct))) + '</span>' +
        '<span class="cx-col-barras"><i class="c-cobre" style="height:' + hc.toFixed(1) + '%"></i><i class="c-me" style="height:' + hm.toFixed(1) + '%"></i></span>' +
        '<span class="cx-col-v">' + esc(compacto(x.cobre)) + '</span>' +
        '<span class="cx-col-et">' + esc(etiquetaCorta(g, x.clave)) + '</span></button>';
    }).join('');
    var tabla = '<details class="cx-ayuda"><summary><i class="bi bi-table"></i> Ver los números</summary><div class="cx-tabla-wrap"><table class="cx-mini"><thead><tr><th scope="col">' +
      esc(dimTxt(g)) + '</th><th scope="col">OT</th><th scope="col">Cobré</th><th scope="col">Me cobraron</th><th scope="col">Margen</th></tr></thead><tbody>' +
      s.map(function (x) { return '<tr><td>' + esc(etiquetaTiempo(g, x.clave)) + '</td><td>' + x.n_ot + '</td><td>' + clp(x.cobre) + '</td><td>' + clp(x.me_cobraron) + '</td><td>' + margenHTML(x) + '</td></tr>'; }).join('') +
      '</tbody></table></div></details>';
    return '<section class="cx-card cx-serie" aria-labelledby="cxSerieT"><header class="cx-card-h"><h3 id="cxSerieT">En el tiempo</h3>' +
      '<div class="cx-seg" role="group" aria-label="Agrupar por">' + seg + '</div></header>' +
      (s.length ? '<div class="cx-serie-scroll"><div class="cx-serie-barras">' + cols + '</div></div>' : '<div class="cx-vacio">Sin OT en el rango con estos filtros.</div>') +
      '<div class="cx-leyenda"><span><i class="c-cobre"></i>Cobré</span><span><i class="c-me"></i>Me cobraron</span><span>Arriba, el margen con el color de la política. Toca un período para filtrarlo.</span></div>' +
      (g === 'dia' && clavesRango('dia', M.desde, M.hasta).length === 0 ? '<p class="cx-nota">Con más de 120 días se muestran solo los días con OT.</p>' : '') +
      tabla + '</section>';
  }

  function matrizHTML() {
    var e = UI.estado, dimF = e.mr, dimC = 'cc';
    var filas = filtrar(UI.actuales, e.f, [dimF, dimC]);
    var m = matriz(filas, dimF, dimC, e.mx, reg(), pol());
    var todos = !!UI.ver.matriz;
    var fs = todos ? m.filas : m.filas.slice(0, 20);
    var maxCelda = 0;
    fs.forEach(function (f) { m.cols.forEach(function (c) { var v = (m.val[f] || {})[c]; if (v > maxCelda) maxCelda = v; }); });
    var selMx = '<label>Medida<select data-cx-cambio="mx">' + ['a_pagar', 'costo_total', 'val_reg', 'cobre'].map(function (k) {
      var t = { a_pagar: 'Nos cobraron (técnico + despacho)', costo_total: 'Costo total con repuestos', val_reg: 'Valorizado regalado', cobre: 'Cobré' }[k];
      return '<option value="' + k + '"' + (e.mx === k ? ' selected' : '') + '>' + t + '</option>';
    }).join('') + '</select></label>';
    var selMr = '<label>Filas<select data-cx-cambio="mr"><option value="tec"' + (dimF === 'tec' ? ' selected' : '') + '>Técnico</option><option value="emp"' +
      (dimF === 'emp' ? ' selected' : '') + '>Empresa o proveedor</option></select></label>';
    var cab = '<tr><th scope="col" class="cx-m-esq">' + esc(dimTxt(dimF)) + '</th>' + m.cols.map(function (c) {
      return '<th scope="col"><button type="button" class="cx-m-cab' + claseItem(dimC, c) + '" data-cx-acc="filtro" data-dim="' + dimC + '" data-val="' + esc(c) + '" aria-pressed="' + pressed(dimC, c) + '">' + esc(lab(dimC, c)) + '</button></th>';
    }).join('') + '<th scope="col">Total</th></tr>';
    var cuerpo = fs.map(function (f) {
      return '<tr><th scope="row"><button type="button" class="cx-m-fila' + claseItem(dimF, f) + '" data-cx-acc="filtro" data-dim="' + dimF + '" data-val="' + esc(f) + '" aria-pressed="' + pressed(dimF, f) + '">' + esc(lab(dimF, f)) + '</button></th>' +
        m.cols.map(function (c) {
          var v = (m.val[f] || {})[c];
          if (v === undefined) return '<td class="cx-m-vacia">—</td>';
          var a = maxCelda > 0 ? Math.max(0.06, Math.max(0, v) / maxCelda) : 0;
          var esMax = v > 0 && v === m.max[c];
          var sel = (e.f[dimF] || []).indexOf(f) >= 0 && (e.f[dimC] || []).indexOf(c) >= 0;
          var aria = lab(dimF, f) + ' en ' + lab(dimC, c) + ': ' + (e.mx === 'n' ? v : clp(v)) + ' en ' + ((m.n[f] || {})[c] || 0) + ' OT' + (esMax ? ' (el mayor de este centro)' : '');
          return '<td class="' + (esMax ? 'cx-m-max' : '') + (sel ? ' cx-m-sel' : '') + '" style="--a:' + a.toFixed(3) + '"><button type="button" class="cx-m-celda' + (a > 0.55 ? ' osc' : '') +
            '" data-cx-acc="celda" data-dimf="' + dimF + '" data-valf="' + esc(f) + '" data-dimc="' + dimC + '" data-valc="' + esc(c) + '" aria-label="' + esc(aria) + '" title="' + esc(aria) + '">' +
            (esMax ? '<i class="bi bi-star-fill" aria-hidden="true"></i>' : '') + clp(v) + '</button></td>';
        }).join('') + '<td class="cx-m-tot">' + clp(m.totF[f]) + '</td></tr>';
    }).join('');
    var pie = '<tr><th scope="row">Total</th>' + m.cols.map(function (c) { return '<td class="cx-m-tot">' + clp(m.totC[c]) + '</td>'; }).join('') + '<td class="cx-m-tot">' + clp(m.total) + '</td></tr>';
    var ver = m.filas.length > 20 ? '<button type="button" class="cx-btn-link" data-cx-acc="ver" data-dim="matriz">' + (todos ? 'Ver menos' : 'Ver las ' + m.filas.length + ' filas') + '</button>' : '';
    return '<section class="cx-card cx-matriz" aria-labelledby="cxMatT"><header class="cx-card-h"><h3 id="cxMatT">¿Quién gastó más en cada centro de costo?</h3>' +
      '<div class="cx-sel">' + selMr + selMx + '</div></header>' +
      (m.filas.length ? '<div class="cx-matriz-scroll"><table class="cx-m"><thead>' + cab + '</thead><tbody>' + cuerpo + '</tbody><tfoot>' + pie + '</tfoot></table></div>' : '<div class="cx-vacio">Sin OT con estos filtros.</div>') +
      '<div class="cx-card-pie">' + ver + '<span class="cx-nota"><i class="bi bi-star-fill"></i> = quien más gastó en ese centro. Toca una celda para filtrar el técnico y el centro a la vez.</span></div>' +
      ayuda('«Nos cobraron» es lo que cobraron el técnico y el despacho en TODAS las OT filtradas (también trabajo interno, contratos y las que aún no se pueden medir: es plata que salió). «Costo total» suma los repuestos instalados. «Valorizado regalado» es la tarifa de referencia de las garantías, regalías, arriendos y cortesías. Las OT sin costo declarado suman $0 aquí.') + '</section>';
  }

  function ordenarTabla(filas) {
    var o = UI.ord;
    var f = filas.slice();
    var num = function (k) { return function (a, b) { return ((b[k] === null || b[k] === undefined ? -Infinity : b[k]) - (a[k] === null || a[k] === undefined ? -Infinity : a[k])) || (b.id - a.id); }; };
    if (o === 'cobre') f.sort(num('cobre'));
    else if (o === 'me') f.sort(num('me_total'));
    else if (o === 'queda') f.sort(num('queda'));
    else if (o === 'pct') f.sort(num('pct'));
    else f.sort(function (a, b) { return (a.dia < b.dia ? 1 : a.dia > b.dia ? -1 : 0) || (b.id - a.id); });
    return f;
  }

  function urlOT(id) { return String(UI.raiz.getAttribute('data-ot') || '/ot/0').replace(/\/0$/, '/' + id); }

  function margenFila(r) {
    if (r.clase === 'aparte') return '<span class="cx-tag b-gris">Aparte</span>';
    if (r.clase === 'sin_medir') return '<span class="cx-tag b-amarillo">Sin medir</span>';
    if (r.clase === 'cobertura' || r.clase === 'cubierta') return '<span class="cx-tag b-gris">Nos costó ' + clp(r.me_total) + '</span>';
    return '<span class="cx-tag b-' + (r.banda && r.banda !== 'sin_medir' ? r.banda : 'gris') + '">' + clp(r.queda) + (r.pct !== null && r.pct !== undefined ? ' · ' + pctTxt(r.pct) : '') + '</span>';
  }

  function tablaHTML(filtradas) {
    var e = UI.estado, M = meta();
    var filas = ordenarTabla(filtradas);
    var n = filas.length, pp = UI.pp;
    var paginas = Math.max(1, Math.ceil(n / pp));
    if (UI.pag > paginas) UI.pag = paginas;
    var ini = (UI.pag - 1) * pp, fin = Math.min(n, ini + pp);
    var nSel = filas.filter(function (r) { return UI.sel[r.id]; }).length;
    var cuerpo = filas.slice(ini, fin).map(function (r) {
      var ti = UI.payload.tec_info && UI.payload.tec_info[r.tec];
      var tecTxt = lab('tec', r.tec) + (ti && ti.ttipo === 'externo' && lab('emp', r.emp) !== lab('tec', r.tec) ? ' · ' + lab('emp', r.emp) : '');
      var td = function (et, html, cls) { return '<td data-et="' + esc(et) + '"' + (cls ? ' class="' + cls + '"' : '') + '><span class="cx-td">' + html + '</span></td>'; };
      return '<tr' + (UI.sel[r.id] ? ' class="sel"' : '') + '>' +
        td('Elegir', '<input type="checkbox" class="cx-chk" data-cx-cambio="sel" data-id="' + r.id + '"' + (UI.sel[r.id] ? ' checked' : '') + ' aria-label="Elegir ' + esc(r.numero) + '">') +
        td('OT', '<a href="' + esc(urlOT(r.id)) + '"><b>' + esc(r.numero) + '</b></a><small>' + esc(r.titulo || '') + '</small>') +
        td(M.fecha_txt || 'Fecha', esc(fechaTxt(r.dia))) +
        td('Cliente', esc(lab('cli', r.cli))) +
        td('Técnico', esc(tecTxt)) +
        td('Centro de costo', esc(lab('cc', r.cc))) +
        td('Cobertura', esc(lab('cob', r.cob))) +
        td('Documento hoy', esc(lab('doc', r.doc))) +
        td('Cobré', clp(r.cobre), 'num') +
        td('Me cobraron', (r.falta_tec ? '<span class="cx-tag b-amarillo">Falta el técnico</span> ' : '') + clp(r.me_total), 'num') +
        td('Margen', margenFila(r), 'num') + '</tr>';
    }).join('');
    var opciones = [10, 25, 50, 100, 200].map(function (t) { return '<option value="' + t + '"' + (t === pp ? ' selected' : '') + '>' + t + '</option>'; }).join('');
    var ords = [['fecha', 'Fecha (más reciente primero)'], ['cobre', 'Cobré (mayor primero)'], ['me', 'Me cobraron (mayor primero)'], ['queda', 'Margen $ (mayor primero)'], ['pct', 'Margen % (mayor primero)']].map(function (o) {
      return '<option value="' + o[0] + '"' + (UI.ord === o[0] ? ' selected' : '') + '>' + o[1] + '</option>';
    }).join('');
    return '<section class="cx-card cx-tabla-card" aria-labelledby="cxTablaT"><header class="cx-card-h"><h3 id="cxTablaT">Las OT filtradas</h3>' +
      '<div class="cx-sel"><label>Ordenar<select data-cx-cambio="ord">' + ords + '</select></label>' +
      '<button type="button" class="cx-btn cx-btn-sec" data-cx-acc="sel-todo"><i class="bi bi-check-all"></i>Seleccionar/deseleccionar todo</button>' +
      '<button type="button" class="cx-btn" data-cx-acc="excel"><i class="bi bi-file-earmark-spreadsheet"></i>' +
      (nSel ? 'Excel de las ' + nSel + ' elegidas' : 'Excel de lo filtrado (' + n + ')') + '</button></div></header>' +
      (n ? '<div class="cx-tabla-wrap"><table class="cx-tabla"><thead><tr><th scope="col"><span class="cx-sr">Elegir</span></th><th scope="col">OT</th><th scope="col">' + esc(M.fecha_txt || 'Fecha') +
        '</th><th scope="col">Cliente</th><th scope="col">Técnico</th><th scope="col">Centro de costo</th><th scope="col">Cobertura</th><th scope="col">Documento hoy</th><th scope="col" class="num">Cobré</th><th scope="col" class="num">Me cobraron</th><th scope="col" class="num">Margen</th></tr></thead><tbody>' +
        cuerpo + '</tbody></table></div>' : '<div class="cx-vacio">Ninguna OT cumple todos los filtros. Quita alguno para ver más.</div>') +
      '<div class="cx-pag"><span>' + (n ? 'Mostrando ' + (ini + 1) + '–' + fin + ' de ' + n : 'Sin resultados con estos filtros') + (nSel ? ' · ' + nSel + ' elegidas' : '') + '</span>' +
      '<label>Por página <select data-cx-cambio="pp" aria-label="Filas por página">' + opciones + '</select></label>' +
      '<span class="cx-pag-btns"><button type="button" data-cx-acc="pag" data-val="-1"' + (UI.pag <= 1 ? ' disabled' : '') + '>Anterior</button>' +
      '<span>Página ' + UI.pag + ' de ' + paginas + '</span>' +
      '<button type="button" data-cx-acc="pag" data-val="1"' + (UI.pag >= paginas ? ' disabled' : '') + '>Siguiente</button></span></div></section>';
  }

  function pie() {
    var M = meta();
    return '<footer class="cx-pie"><p><b>Cómo leer este tablero.</b> Cada OT trae la cuenta de su ficha («Cobré − Me cobraron = Queda»); aquí solo se filtra y se suma con la regla única del proyecto: el trabajo interno y las mantenciones de contrato van aparte, y a las OT que les falta un costo se las cuenta aparte en vez de inventar un cero. Fechas en hora de Chile.</p>' +
      '<p>Cada gráfico se calcula con los filtros de los demás, para que veas el contexto y lo que elegiste resaltado; las cifras de arriba y la tabla usan todos los filtros. El Excel reaplica los mismos filtros en el servidor.</p>' +
      '<p>Solo lectura: no escribe nada, no consulta a Random ni a Check y nunca le llega a un técnico. Leído a las ' + esc(M.generado || '') + ' (hora de Chile); se guarda 90 segundos: usa «Actualizar» para releer. ' + clasif() + '</p></footer>';
  }

  function pintar() {
    if (!UI.raiz || !UI.payload) return;
    var foco = document.activeElement && UI.raiz.contains(document.activeElement) ? firma(document.activeElement) : null;
    var e = UI.estado, M = meta();
    var filtradas = filtrar(UI.actuales, e.f);
    var k = kpis(filtradas, reg(), pol());
    var pr = previo(UI.todas, e.f, M);
    var kp = pr.filas ? kpis(pr.filas, reg(), pol()) : null;
    var mosaico = [
      barras('cc', 'Por centro de costo'), barras('cob', 'Por cobertura'), barras('doc', 'Por documento que respalda el cobro (hoy)', { fijo: true }),
      barras('tipo', 'Por tipo de OT'), barras('ttipo', 'Interno o externo', { fijo: ['externo', 'elevado', 'interno', 'sin_asignar'] }),
      barras('int', 'Por intención'), barras('nac', 'Cómo nació la OT'),
      barras('banda', 'Margen según la política', { fijo: true, ayuda: 'Cada OT que se cobra, según su margen: objetivo, tolerancia, bajo o pérdida. Las que no se cobran o les falta un costo se dicen aparte.' }),
      barras('reg', 'Por región del cliente')
    ];
    if (M.alcance === 'todas') mosaico.push(barras('estado', 'Por estado de la OT'));
    UI.raiz.classList.remove('cx-ocupado');
    UI.raiz.classList.toggle('cx-pres', UI.pres);
    UI.raiz.innerHTML = barraSuperior() + chips() + avisos() + tarjetas(k, kp, pr) + selectorMedida() + serieHTML() +
      '<div class="cx-dos">' + barras('tec', 'Por técnico', { ayuda: 'Cada técnico con lo que se cobró en sus OT, lo que nos cobraron en ellas y el margen. «Nos cobró» es lo que cobró él (técnico + despacho) en todas sus OT. Los externos llevan su empresa al lado.' }) +
      barras('emp', 'Por empresa o proveedor', { ayuda: 'Los externos agrupados por empresa (una empresa puede mandar a distintas personas); los técnicos propios van juntos.' }) + '</div>' +
      matrizHTML() + '<div class="cx-mosaico">' + mosaico.join('') + '</div>' +
      barras('cli', 'Por cliente (los 15 primeros)', { tope: 15, otros: true }) + tablaHTML(filtradas) + pie();
    if (foco) restaurarFoco(foco);
  }

  function firma(el) {
    var acc = el.getAttribute('data-cx-acc') || el.getAttribute('data-cx-cambio');
    if (!acc) return null;
    return { acc: acc, dim: el.getAttribute('data-dim') || el.getAttribute('data-dimf') || '', val: el.getAttribute('data-val') || el.getAttribute('data-valf') || el.getAttribute('data-id') || '' };
  }
  function restaurarFoco(f) {
    var lista = UI.raiz.querySelectorAll('[data-cx-acc],[data-cx-cambio]');
    for (var i = 0; i < lista.length; i++) {
      var g = firma(lista[i]);
      if (g && g.acc === f.acc && g.dim === f.dim && g.val === f.val) { try { lista[i].focus({ preventScroll: true }); } catch (err) { lista[i].focus(); } return; }
    }
  }

  function cambiarFiltros(f) {
    UI.estado.f = f;
    UI.pag = 1;
    guardarHash();
    pintar();
  }

  function urlExcel() {
    var e = UI.estado;
    var q = new URLSearchParams();
    q.set('desde', e.d);
    q.set('hasta', e.h);
    q.set('fecha', e.fe);
    q.set('alcance', e.al);
    if (Object.keys(e.f).length) q.set('f', JSON.stringify(e.f));
    var filtradas = filtrar(UI.actuales, e.f);
    var ids = filtradas.filter(function (r) { return UI.sel[r.id]; }).map(function (r) { return r.id; });
    if (ids.length) q.set('ids', ids.join(','));
    return UI.raiz.getAttribute('data-excel') + '?' + q.toString();
  }

  function atajo(clave) {
    var hoy = meta().hoy || isoDe(new Date());
    var a = hoy.slice(0, 4), m = hoy.slice(0, 7), d, h;
    if (clave === 'mes') { d = m + '-01'; h = hoy; }
    else if (clave === 'mes_ant') { var p = mesMas(m, -1); d = p + '-01'; h = sumarDias(m + '-01', -1); }
    else if (clave === 'anio') { d = a + '-01-01'; h = hoy; }
    else { d = mesMas(m, -11) + '-01'; h = hoy; }
    UI.estado.d = d;
    UI.estado.h = h;
    DIMS_TIEMPO.forEach(function (t) { delete UI.estado.f[t]; });   // el rango nuevo manda sobre los filtros de tiempo
    guardarHash();
    asegurarDatos();
  }

  function presentacion() {
    UI.pres = !UI.pres;
    var r = UI.raiz;
    try {
      if (UI.pres && r.requestFullscreen && !document.fullscreenElement) r.requestFullscreen().catch(function () {});
      else if (!UI.pres && document.fullscreenElement && document.exitFullscreen) document.exitFullscreen().catch(function () {});
    } catch (err) { /* sin pantalla completa: queda el modo grande igual */ }
    pintar();
  }

  function alClic(ev) {
    var b = ev.target.closest('[data-cx-acc]');
    if (!b || !UI.raiz.contains(b)) return;
    var acc = b.getAttribute('data-cx-acc');
    var e = UI.estado;
    var modo = (ev.ctrlKey || ev.metaKey || UI.puntero === 'touch' || UI.puntero === 'pen') ? 'sumar' : 'solo';
    if (acc === 'filtro') cambiarFiltros(alternar(e.f, b.getAttribute('data-dim'), b.getAttribute('data-val'), modo));
    else if (acc === 'celda') cambiarFiltros(alternarPar(e.f, b.getAttribute('data-dimf'), b.getAttribute('data-valf'), b.getAttribute('data-dimc'), b.getAttribute('data-valc'), modo));
    else if (acc === 'quitar') cambiarFiltros(quitar(e.f, b.getAttribute('data-dim'), b.getAttribute('data-val')));
    else if (acc === 'limpiar-dim') cambiarFiltros(quitar(e.f, b.getAttribute('data-dim')));
    else if (acc === 'limpiar') cambiarFiltros({});
    else if (acc === 'gran') { e.g = b.getAttribute('data-val'); guardarHash(); pintar(); }
    else if (acc === 'ver') { var d = b.getAttribute('data-dim'); UI.ver[d] = !UI.ver[d]; pintar(); }
    else if (acc === 'pag') { UI.pag += parseInt(b.getAttribute('data-val'), 10) || 0; pintar(); }
    else if (acc === 'sel-todo') {
      var filas = filtrar(UI.actuales, e.f);
      var alguno = filas.some(function (r) { return !UI.sel[r.id]; });          // REGLA #14: si falta alguno, marca todos
      filas.forEach(function (r) { if (alguno) UI.sel[r.id] = true; else delete UI.sel[r.id]; });
      pintar();
    }
    else if (acc === 'excel') window.location.href = urlExcel();
    else if (acc === 'actualizar') cargar(true);
    else if (acc === 'reintentar') cargar(false);
    else if (acc === 'presentacion') presentacion();
    else if (acc === 'atajo') atajo(b.getAttribute('data-val'));
  }

  function alCambiar(ev) {
    var el = ev.target;
    var c = el.getAttribute && el.getAttribute('data-cx-cambio');
    if (!c) return;
    var e = UI.estado;
    if (c === 'sel') {
      var id = parseInt(el.getAttribute('data-id'), 10);
      if (el.checked) UI.sel[id] = true; else delete UI.sel[id];
      pintar();
      return;
    }
    if (c === 'pp') { UI.pp = parseInt(el.value, 10) || 25; UI.pag = 1; pintar(); return; }
    if (c === 'ord') { UI.ord = el.value; UI.pag = 1; pintar(); return; }
    if (VALIDOS[c] && VALIDOS[c].indexOf(el.value) >= 0) { e[c] = el.value; guardarHash(); pintar(); }
  }

  function alEnviar(ev) {
    var form = ev.target.closest('[data-cx-form="rango"]');
    if (!form) return;
    ev.preventDefault();
    var e = UI.estado;
    var d = form.elements.d.value, h = form.elements.h.value;
    if (/^\d{4}-\d{2}-\d{2}$/.test(d)) e.d = d;
    if (/^\d{4}-\d{2}-\d{2}$/.test(h)) e.h = h;
    if (VALIDOS.fe.indexOf(form.elements.fe.value) >= 0) e.fe = form.elements.fe.value;
    e.al = form.elements.al.checked ? 'todas' : 'cerradas';
    DIMS_TIEMPO.forEach(function (t) { delete e.f[t]; });
    guardarHash();
    asegurarDatos();
  }

  function arrancar() {
    var raiz = document.getElementById('cxApp');
    if (!raiz) return;
    UI.raiz = raiz;
    UI.estado = hashAEstado(location.hash) || hashAEstado('#explorar');
    raiz.addEventListener('click', alClic);
    raiz.addEventListener('change', alCambiar);
    raiz.addEventListener('submit', alEnviar);
    raiz.addEventListener('pointerdown', function (ev) { UI.puntero = ev.pointerType || 'mouse'; }, true);
    document.addEventListener('fullscreenchange', function () {
      if (!document.fullscreenElement && UI.pres) { UI.pres = false; pintar(); }
    });
    var tabs = Array.prototype.slice.call(document.querySelectorAll('[data-gc-pestana]'));
    tabs.forEach(function (t, i) {
      t.addEventListener('click', function () { irA(t.getAttribute('data-gc-pestana'), true); });
      t.addEventListener('keydown', function (ev) {
        if (ev.key !== 'ArrowRight' && ev.key !== 'ArrowLeft') return;
        var otro = tabs[(i + (ev.key === 'ArrowRight' ? 1 : tabs.length - 1)) % tabs.length];
        otro.focus();
        irA(otro.getAttribute('data-gc-pestana'), true);
      });
    });
    window.addEventListener('hashchange', function () {
      var e = hashAEstado(location.hash);
      if (e) { UI.estado = e; irA('explorar', false); }
    });
    if (/^#explorar/.test(location.hash)) irA('explorar', false);
  }

  return {
    arrancar: arrancar, pyRound: pyRound, clp: clp, compacto: compacto, pctTxt: pctTxt, esc: esc, fechaTxt: fechaTxt, semanaISO: semanaISO,
    lunesDe: lunesDe, sumarDias: sumarDias, mesMas: mesMas, etiquetaTiempo: etiquetaTiempo, etiquetaCorta: etiquetaCorta,
    aObjetos: aObjetos, filtrar: filtrar, kpis: kpis, banda: banda, agrupar: agrupar, ordenar: ordenar, matriz: matriz,
    serie: serie, clavesRango: clavesRango, comparar: comparar, previo: previo, desplazar: desplazar, alternar: alternar,
    alternarPar: alternarPar, quitar: quitar, estadoAHash: estadoAHash, hashAEstado: hashAEstado, valorMedida: valorMedida,
    MEDIDAS: MEDIDAS, DIMS_TIEMPO: DIMS_TIEMPO, FILTRABLES: FILTRABLES
  };
});

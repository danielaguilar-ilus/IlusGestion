/* 🧠 Facturación de proveedor "inteligente" — sumas en vivo, frase guía y sugerencia de qué OT calzan con la factura.
   Daniel, 2026-10-07: "el módulo está bonito, pero quiero que se sienta más inteligente".

   NO tiene fórmulas de plata propias: cada OT llega con lo que dice la cuenta única de la OT (_ot_finanzas en
   app.py -> `sugerido` = a_pagar_proveedor, `cobrado_cliente` = Cobré), y acá solo se SUMA lo que la persona marca.
   La regla de la ganancia es la misma de _mfp_resumen_filas: compara solo OT con cobro declarado; las que no se cobran
   (garantía, regalía, arriendo/leasing, cortesía) y las que no tienen cobro declarado van aparte.

   Cada item: {id, numero, prov, provNombre, pagar, cobrado, garantia:bool, sincobro:bool, listo:bool}. */
(function (global) {
  'use strict';

  function clp(n) {
    return '$' + Math.round(Math.abs(Number(n) || 0)).toString().replace(/\B(?=(\d{3})+(?!\d))/g, '.');
  }
  function clpFirma(n) {            // con signo explícito: +$1.000 / −$1.000
    var v = Math.round(Number(n) || 0);
    return (v > 0 ? '+' : (v < 0 ? '−' : '')) + clp(v);
  }
  function soloDigitos(s) { return Number(String(s == null ? '' : s).replace(/\D/g, '')) || 0; }

  /* Totales de un conjunto de OT (lo marcado). */
  function sumar(items) {
    var t = {n: 0, pagar: 0, cobrado: 0, margen: 0, comp_n: 0, comp_pagar: 0, comp_cobrado: 0,
             gar_n: 0, gar_pagar: 0, sin_n: 0, sin_pagar: 0, listas: 0, faltan: 0};
    (items || []).forEach(function (it) {
      var pag = Number(it.pagar) || 0, cob = Number(it.cobrado) || 0;
      t.n++; t.pagar += pag;
      if (it.listo) t.listas++; else t.faltan++;
      if (it.garantia) { t.gar_n++; t.gar_pagar += pag; }
      else if (it.sincobro) { t.sin_n++; t.sin_pagar += pag; }
      else { t.comp_n++; t.comp_pagar += pag; t.comp_cobrado += cob; }
    });
    t.cobrado = t.comp_cobrado;
    t.margen = t.comp_cobrado - t.comp_pagar;
    t.margen_pct = t.comp_cobrado > 0 ? t.margen / t.comp_cobrado * 100 : null;
    return t;
  }

  /* Semáforo de la diferencia contra el monto de la factura del proveedor:
     ok = cuadra · warn = la factura cobra más que lo marcado (faltan) · neg = lo marcado suma más que la factura. */
  function semaforo(objetivo, pagar) {
    if (!(objetivo > 0)) return {clase: 'muted', dif: 0};
    var dif = Math.round(objetivo - pagar);
    return {clase: dif === 0 ? 'ok' : (dif > 0 ? 'warn' : 'neg'), dif: dif};
  }

  /* Subconjunto de `cands` cuya suma sea la más cercana (sin pasarse) a `objetivo`. Exacto si existe.
     Las "listas para facturar" se prueban primero, así a igual suma gana la combinación más lista. */
  function sugerir(cands, objetivo) {
    objetivo = Math.round(Number(objetivo) || 0);
    if (!(objetivo > 0)) return null;
    var lista = (cands || []).filter(function (c) { return (Number(c.pagar) || 0) > 0; });
    lista.sort(function (a, b) { return (b.listo ? 1 : 0) - (a.listo ? 1 : 0); });
    lista = lista.slice(0, 60);
    var alcanzable = new Map();           // suma -> {prev, idx}
    alcanzable.set(0, null);
    var TOPE = 200000, mejor = 0;
    for (var i = 0; i < lista.length; i++) {
      var m = Math.round(Number(lista[i].pagar));
      if (m > objetivo) continue;
      var nuevos = [];
      alcanzable.forEach(function (_nodo, s) {
        var ns = s + m;
        if (ns <= objetivo && !alcanzable.has(ns)) nuevos.push([ns, {prev: s, idx: i}]);
      });
      for (var k = 0; k < nuevos.length; k++) {
        if (alcanzable.size >= TOPE) break;
        alcanzable.set(nuevos[k][0], nuevos[k][1]);
        if (nuevos[k][0] > mejor) mejor = nuevos[k][0];
      }
      if (alcanzable.has(objetivo)) { mejor = objetivo; break; }
    }
    if (mejor === 0) return null;
    var ids = [], cur = mejor;
    while (cur !== 0) {
      var nodo = alcanzable.get(cur);
      ids.push(lista[nodo.idx]);
      cur = nodo.prev;
    }
    return {items: ids.reverse(), suma: mejor, exacto: mejor === objetivo, dif: objetivo - mejor};
  }

  /* Elige el proveedor al que mejor le calza el monto. grupos: {clave: {nombre, items}}.
     Devuelve {clave, nombre, sug} o null. */
  function sugerirPorProveedor(grupos, objetivo) {
    var mejor = null;
    Object.keys(grupos || {}).forEach(function (k) {
      var g = grupos[k], s = sugerir(g.items, objetivo);
      if (!s) return;
      var puntaje = (s.exacto ? 1e12 : 0) - Math.abs(s.dif) * 10 + s.items.filter(function (x) { return x.listo; }).length;
      if (!mejor || puntaje > mejor.puntaje) mejor = {clave: k, nombre: g.nombre, sug: s, puntaje: puntaje};
    });
    return mejor;
  }

  /* ¿Hay una OT (o dos) que explique la diferencia? Ayuda a decir "¿será la OT-123?". */
  function explicarDif(candidatas, dif) {
    if (!dif) return '';
    var v = Math.abs(dif), lista = (candidatas || []).filter(function (c) { return (Number(c.pagar) || 0) > 0; });
    for (var i = 0; i < lista.length; i++)
      if (Math.round(lista[i].pagar) === v) return lista[i].numero + ' (' + clp(v) + ')';
    for (var a = 0; a < lista.length; a++)
      for (var b = a + 1; b < lista.length; b++)
        if (Math.round(lista[a].pagar + lista[b].pagar) === v)
          return lista[a].numero + ' + ' + lista[b].numero + ' (' + clp(v) + ')';
    return '';
  }

  /* La frase de arriba, en palabras de persona.
     o = {n, tot (de sumar), objetivo, provNombre, marcadas, noMarcadas} */
  function frase(o) {
    var n = o.n || 0, objetivo = Math.round(o.objetivo || 0), prov = o.provNombre || '';
    if (!n) {
      return objetivo > 0
        ? 'La factura del proveedor dice ' + clp(objetivo) + '. Marca las OT que cubre; si quieres, te propongo cuáles calzan.'
        : 'Marca las OT que vas a pagar. Si el proveedor ya te mandó su factura, escribe su monto y te sugiero cuáles marcar.';
    }
    var t = o.tot, txt = 'Seleccionaste ' + n + ' OT' + (prov ? ' de ' + prov : '') + ' por ' + clp(t.pagar) + '.';
    if (t.gar_n) txt += ' ' + t.gar_n + ' va' + (t.gar_n === 1 ? '' : 'n') + ' sin cobro al cliente (' + clp(t.gar_pagar) + '): se paga igual.';
    if (t.faltan) txt += ' ' + t.faltan + ' con datos pendientes (no cerrada o sin costo del técnico).';
    if (objetivo > 0) {
      var s = semaforo(objetivo, t.pagar);
      if (s.clase === 'ok') txt += ' La factura dice ' + clp(objetivo) + ': cuadra exacto.';
      else if (s.dif > 0) {
        txt += ' La factura dice ' + clp(objetivo) + ': faltan ' + clp(s.dif);
        var e = explicarDif(o.noMarcadas, s.dif);
        txt += e ? ' (¿será la ' + e + '?).' : ' (¿una OT sin marcar o un despacho sin declarar?).';
      } else {
        txt += ' La factura dice ' + clp(objetivo) + ': sobran ' + clp(-s.dif);
        var e2 = explicarDif(o.marcadas, -s.dif);
        txt += e2 ? ' (¿sobra la ' + e2 + '?).' : ' (una OT de más o un costo mal cargado).';
      }
    }
    return txt;
  }

  global.fpi = {clp: clp, clpFirma: clpFirma, soloDigitos: soloDigitos, sumar: sumar, semaforo: semaforo,
                sugerir: sugerir, sugerirPorProveedor: sugerirPorProveedor, explicarDif: explicarDif, frase: frase};
  if (typeof module !== 'undefined' && module.exports) module.exports = global.fpi;
})(typeof window !== 'undefined' ? window : globalThis);

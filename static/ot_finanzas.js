/* 💰 Cuenta única de una OT en el navegador — "Cobré − Me cobraron = Queda" (+ Valorizado aparte).
   Daniel, 2026-10-07: "tenemos que definir cuánto me cobraron, cuánto cobré yo… necesito que ya quede
   completamente entendible y amigable".

   ESPEJO EXACTO de _ot_finanzas / _ot_cobertura en app.py (la fuente de verdad es el servidor). Existe solo
   para que una pantalla pueda recalcular MIENTRAS la persona escribe, sin ir al servidor en cada tecla.
   tests/test_ot_finanzas_js_espejo.py corre los mismos casos en Python y en este archivo y falla si difieren:
   si cambias una regla, cámbiala en los dos lados.

   Uso: ilusOtFinanzas(v, rep) con v = {modalidad_cobro, cubierto_por, tipo, cliente_id, contrato_real, costo,
   zz_monto, zz_codigo, zz_envio_monto, valor_origen, costo_proveedor, costo_despacho, proveedor_tipo,
   valorizado_clp, valorizado_fuente, cobro_cero_motivo} y rep = {costo, n_sin_costo, por_origen} (repuestos instalados) o null. */
(function(global){
  'use strict';
  var ORIGENES_NO_COBRO = ['estimado', 'interno'];
  var FUENTE_COBRO = {zz: 'línea del documento', doc_total: 'total del documento', cotizacion: 'cotización',
    contrato: 'precio acordado', manual: 'escrito a mano', supuesto: 'escrito a mano', '': 'declarado'};
  var ZZ_NO_SERVICIO = ['ZZRETIRO'];
  /* valor_origen que respalda un «Precio al cliente» anotado sin línea de servicio (espejo de ORIGENES_COBRO_RESPALDO) */
  var ORIGENES_COBRO_RESPALDO = ['cotizacion', 'contrato', 'manual', 'supuesto'];   /* manual/supuesto solo con motivo escrito */
  var COBERTURA_TXT = {
    cobra: 'Se le cobra al cliente',
    garantia: 'Garantía: no se le cobra',
    sin_costo: 'Cortesía: no se le cobra',
    /* 2026-10-07 (Daniel): únicos motivos de $0: garantía, regalía, arriendo o leasing (cobro_cero_motivo). */
    regalia: 'Regalía: no se le cobra',
    arriendo_leasing: 'Arriendo o leasing: incluido en el arriendo',
    interno: 'Trabajo interno: no se le cobra',
    contrato: 'Mantención de contrato: se paga con el contrato'
  };
  var UMBRAL_BAJO = 10;

  function num(x){
    if (x === null || x === undefined || x === '') return null;
    var n = Number(x);
    return isFinite(n) ? n : null;
  }
  function low(x){ return String(x == null ? '' : x).trim().toLowerCase(); }
  function clp(n){ return '$' + Math.round(Math.abs(n || 0)).toString().replace(/\B(?=(\d{3})+(?!\d))/g, '.'); }
  function r2(n){ return Math.round(n * 100) / 100; }
  function esInterna(v){
    /* 2026-10-07 (Daniel): interna = SIN cliente. Con cliente, 'interno'/'revision_interna' no exime. */
    var tieneClave = Object.prototype.hasOwnProperty.call(v, 'cliente_id');
    if (tieneClave && v.cliente_id === null) return true;
    if (tieneClave && v.cliente_id !== null && v.cliente_id !== undefined) return false;
    return low(v.modalidad_cobro) === 'interno' || low(v.tipo) === 'revision_interna';
  }

  function cobertura(v){
    v = v || {};
    var mod = low(v.modalidad_cobro), cub = low(v.cubierto_por), tipo = low(v.tipo), cero = low(v.cobro_cero_motivo);
    if (mod === 'garantia' || cub === 'garantia' || tipo === 'garantia' || cero === 'garantia') return 'garantia';
    if (esInterna(v)) return 'interno';
    if (mod === 'sin_costo' || cero === 'regalia' || cero === 'arriendo_leasing'){
      if (cero === 'regalia') return 'regalia';
      if (cero === 'arriendo_leasing') return 'arriendo_leasing';
      return 'sin_costo';
    }
    if (v.contrato_real && tipo === 'preventiva'){
      var zz = num(v.zz_monto) || 0, origen = low(v.valor_origen);
      if (!(zz > 0 && (origen === 'zz' || origen === 'doc_total'))) return 'contrato';
    }
    return 'cobra';
  }

  function finanzas(v, rep){
    v = v || {};
    rep = rep || {costo: 0, por_origen: {bodega: 0, compra: 0, manual: 0}, n_sin_costo: 0};
    var cob = cobertura(v), cobra = cob === 'cobra';
    var origen = low(v.valor_origen);
    var zz = num(v.zz_monto), zzCod = String(v.zz_codigo || '').trim().toUpperCase();
    var zzNoServ = ZZ_NO_SERVICIO.indexOf(zzCod) >= 0;
    var env = num(v.zz_envio_monto), tot = num(v.costo);
    var kI = num(v.costo_proveedor), kD = num(v.costo_despacho);
    if (kI === null && cob === 'interno' && low(v.proveedor_tipo) !== 'externo') kI = 0;
    var kRep = r2(Number(rep.costo || 0));
    var avisos = [];

    // COBRÉ
    var zzEsCobro = zz !== null && ORIGENES_NO_COBRO.indexOf(origen) < 0 && !zzNoServ;
    var serv = null, fuente = null, precioAnotado = null;
    if (zzEsCobro){ serv = zz; fuente = FUENTE_COBRO.hasOwnProperty(origen) ? FUENTE_COBRO[origen] : 'declarado'; }
    else if ((zz === null || zzNoServ) && tot !== null && tot > 0){
      var respaldo = ORIGENES_COBRO_RESPALDO.indexOf(origen) >= 0 &&
        ((origen !== 'manual' && origen !== 'supuesto') || String(v.zz_motivo_manual == null ? '' : v.zz_motivo_manual).trim() !== '');
      if (zz === null && respaldo){
        serv = Math.max(tot - (env || 0), 0); fuente = 'precio al cliente (sin separar servicio y despacho)';
      } else { precioAnotado = tot; }
    }
    var cServ, cDesp, hayCobro;
    if (cobra){
      cServ = serv || 0; cDesp = env || 0; hayCobro = serv !== null || env !== null;
      if (precioAnotado !== null && serv === null)
        avisos.push('Hay un precio anotado (' + clp(precioAnotado) + ') sin documento de cobro: no cuenta como cobro. Falta el documento de Random o declarar el cobro con su motivo.');
      if (fuente && fuente.indexOf('precio al cliente') === 0)
        avisos.push('El cobro sale del «Precio al cliente» anotado: no separa servicio y despacho.');
      if (zzEsCobro && tot !== null && tot > 0 && Math.abs(tot - (cServ + cDesp)) >= 1)
        avisos.push('El «Precio al cliente» anotado (' + clp(tot) + ') no coincide con lo cobrado (' + clp(cServ + cDesp) + '): manda lo cobrado.');
      if (zz !== null && zz > 0 && ORIGENES_NO_COBRO.indexOf(origen) >= 0 && serv === null)
        avisos.push('El monto anotado (' + clp(zz) + ') es un estimado, no un cobro: falta declarar cuánto se cobró.');
    } else { cServ = 0; cDesp = 0; hayCobro = true; }
    if (zzNoServ && zz !== null)
      avisos.push('La línea del documento (' + zzCod + ', ' + clp(zz) + ') no es de servicio: no cuenta como cobro.');
    var cobreTotal = r2(cServ + cDesp);

    // ME COBRARON
    var faltaTec = kI === null;
    var faltaDesp = kD === null && cobra && (env || 0) > 0;
    var mTec = kI || 0, mDesp = kD || 0;
    var meTotal = r2(mTec + mDesp + kRep);
    if (Number(rep.n_sin_costo || 0)) avisos.push(Number(rep.n_sin_costo) + ' repuesto(s) instalado(s) sin costo: no suman.');

    // QUEDA
    var quedaTotal = r2(cobreTotal - meTotal);
    var pct = (cobra && cobreTotal > 0) ? Math.round(quedaTotal / cobreTotal * 1000) / 10 : null;

    // ESTADO
    var clase, label, mostrar = true;
    var corto = COBERTURA_TXT[cob].split(':')[0];
    if (!cobra){
      if (faltaTec){ clase = 'ambar'; label = 'Falta lo que te cobró el técnico'; mostrar = false; }
      else { clase = 'info'; label = corto + ' · nos costó ' + clp(meTotal); }
    } else if (!hayCobro){ clase = 'gris'; label = 'Falta el documento de cobro'; mostrar = false; }
    else if (faltaTec){ clase = 'ambar'; label = 'Falta lo que te cobró el técnico'; mostrar = false; }
    else if (faltaDesp){ clase = 'ambar'; label = 'Falta el costo del despacho'; mostrar = false; }
    else if (cobreTotal <= 0){ clase = meTotal > 0 ? 'rojo' : 'gris'; label = 'Cobro declarado en $0'; }
    else if (quedaTotal < 0){ clase = 'rojo'; label = 'Pérdida'; }
    else if (pct !== null && pct < UMBRAL_BAJO){ clase = 'bajo'; label = 'Margen bajo (menos de ' + UMBRAL_BAJO + ' %)'; }
    else { clase = 'ok'; label = 'Margen sano'; }

    // VALORIZADO
    var val = num(v.valorizado_clp), valF = String(v.valorizado_fuente || '').trim() || null;
    if (val === null && !cobra){
      if (tot !== null && tot > 0){ val = tot; valF = cob === 'interno' ? 'interno' : 'dato_antiguo'; }
      else if (zz !== null && zz > 1 && !zzNoServ){ val = zz; valF = origen || 'documento'; }
    }
    if (val === null && cobra && zz !== null && zz > 0 && ORIGENES_NO_COBRO.indexOf(origen) >= 0){ val = zz; valF = origen; }
    if (val === null && cobra && precioAnotado !== null){ val = precioAnotado; valF = 'precio_anotado'; }

    var frase;
    if (!cobra && !faltaTec) frase = COBERTURA_TXT[cob] + '. Nos costó ' + clp(meTotal) + (val ? ' (valorizada en ' + clp(val) + ')' : '') + '.';
    else if (mostrar) frase = 'Cobré ' + clp(cobreTotal) + ' − me cobraron ' + clp(meTotal) + ' = ' + (quedaTotal >= 0 ? 'quedan' : 'se pierden') + ' ' + clp(quedaTotal)
      + (pct !== null ? ' (' + pct.toFixed(1).replace('.', ',') + ' %)' : '') + '.';
    else frase = label + '.';

    return {
      cobertura: cob, cobertura_txt: COBERTURA_TXT[cob], cobra: cobra,
      cobre: {servicio: r2(cServ), despacho: r2(cDesp), total: cobreTotal, fuente: cobra ? fuente : null, hay: !!hayCobro},
      me_cobraron: {tecnico: kI !== null ? r2(kI) : null, despacho: kD !== null ? r2(kD) : null, repuestos: kRep, total: meTotal,
        falta_tecnico: faltaTec, falta_despacho: faltaDesp, repuestos_sin_costo: Number(rep.n_sin_costo || 0),
        repuestos_desglose: rep.por_origen || {bodega: 0, compra: 0, manual: 0}},
      queda: {servicio: r2(cServ - mTec), despacho: r2(cDesp - mDesp), repuestos: r2(-kRep), total: quedaTotal, pct: pct, mostrar: mostrar},
      a_pagar_proveedor: r2(mTec + mDesp),
      valorizado: {monto: val !== null ? r2(val) : null, fuente: valF},
      precio_anotado: (cobra && precioAnotado !== null && serv === null) ? r2(precioAnotado) : null,
      clase: clase, label: label, frase: frase, avisos: avisos
    };
  }

  global.ilusOtCobertura = cobertura;
  global.ilusOtFinanzas = finanzas;
  global.ilusOtFinClp = clp;
  if (typeof module !== 'undefined' && module.exports) module.exports = {cobertura: cobertura, finanzas: finanzas};
})(typeof window !== 'undefined' ? window : globalThis);

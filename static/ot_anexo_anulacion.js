/* 📜 REGLA #52 — Anexo FIRMADO + OT eliminada.
   Daniel (2026-10-28): «cuando eliminemos una OT y el anexo está firmado, que se le envíe al correo que el anexo queda inválido por la
   anulación del servicio. Por ambas partes».

   Dos ayudas para TODA pantalla que ofrezca «Eliminar OT» (ficha de la OT, calendario, ficha del cliente):
     · otAnexoAnulacion.avisoHtml(vid)  → el aviso ANTES de pedir el número de OT: qué anexo quedará inválido y a quiénes se les avisará por
       correo (el servidor usa el MISMO plan al borrar: lo que se promete es lo que se hace). '' si la OT no tiene anexo firmado o no se pudo
       consultar: nunca frena la eliminación (el servidor vuelve a revisarlo todo).
     · otAnexoAnulacion.resultadoTxt(d) → lo que pasó con el anexo y con cada correo, para el aviso de «OT eliminada».
   Todo texto del servidor se escapa. Sin diálogos nativos del navegador (REGLA #1): el aviso viaja dentro del ilusPrompt de la eliminación. */
(function (w) {
  'use strict';

  function esc(s) {
    return String(s == null ? '' : s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  }

  async function avisoHtml(vid) {
    try {
      var r = await fetch('/ot/api/' + encodeURIComponent(vid) + '/anexo-anulacion',
        { credentials: 'same-origin', headers: { 'Accept': 'application/json' } });
      var d = await r.json().catch(function () { return null; });
      if (!r.ok || !d || !d.ok || !d.aplica) return '';
      return '<div class="otax-aviso" style="text-align:left;margin:0 0 12px;padding:10px 12px;border-radius:10px;background:#fff8e1;'
        + 'border:1px solid #fde68a;color:#78350f;font-size:.88rem;line-height:1.45">'
        + '<div style="font-weight:800;margin-bottom:4px"><i class="bi bi-exclamation-triangle-fill" style="color:#f59e0b"></i> ' + esc(d.titulo) + '</div>'
        + (d.lineas || []).map(function (l) { return '<div style="margin-top:3px">' + esc(l) + '</div>'; }).join('')
        + '</div>';
    } catch (e) {
      return '';
    }
  }

  function resultadoTxt(d) {
    var a = d && d.anexo_anulado;
    return (a && a.lineas && a.lineas.length) ? ' ' + a.lineas.join(' ') : '';
  }

  w.otAnexoAnulacion = { avisoHtml: avisoHtml, resultadoTxt: resultadoTxt, esc: esc };
})(window);

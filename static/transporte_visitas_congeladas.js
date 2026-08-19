/* ═══════════════════════════════════════════════════════════════════════
   Visitas sin planificar — panel de rescate
   ───────────────────────────────────────────────────────────────────────
   2026-08-18. Nace del incidente que Alison y el jefe de bodega reportaron
   como "subo a Felca y no llega". La API SI estaba conectada: la visita
   nacia con la fecha del MANIFIESTO, caia en un dia ya cerrado del tablero
   del despachador y quedaba en 'pending' sin chofer, invisible, para
   siempre. Se detectaron 19 asi, la mas vieja de 14 dias.

   El arreglo de raiz ya esta (la visita nace con la fecha de hoy, ver
   _sr_planned_date_hoy en app.py). Esta pantalla es la otra mitad: que
   cualquiera pueda VER las que quedaron atrapadas y sacarlas, sin depender
   de que alguien corra una consulta a mano.

   Por que consulta en vivo y no una alerta calculada en la base: el estado
   "sin planificar" solo existe en SimpliRoute -- ILUS no lo guarda. Un
   atajo desde MySQL daria falsos positivos (una visita legitimamente en
   ruta se veria igual), y una alerta que grita cuando no pasa nada es peor
   que no tenerla: se aprende a ignorarla.
   ═══════════════════════════════════════════════════════════════════════ */
(function () {
  'use strict';

  var _cong = [];        // lo ultimo que devolvio el backend
  var _modal = null;
  var _cargando = false;

  function esc(s) {
    return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) {
      return ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[c];
    });
  }

  /* ISO -> dd/mm/aaaa. REGLA #6: nunca se muestra ISO crudo.
     Se parte el string a mano: new Date('2026-08-18') se lee como UTC y en
     Chile (UTC-4) retrocede un dia. */
  function fechaCL(iso) {
    var m = /^(\d{4})-(\d{2})-(\d{2})/.exec(String(iso || ''));
    return m ? (m[3] + '/' + m[2] + '/' + m[1]) : '—';
  }

  /* Proximo dia habil, en hora local del navegador (que es Chile).
     Es el default del selector: reprogramar "a hoy" a las 19:30 no sirve
     -- el tablero del dia ya cerro y amanece vencida otra vez. */
  function proximoHabil() {
    var d = new Date();
    if (d.getHours() >= 17) d.setDate(d.getDate() + 1);   // ya no alcanza hoy
    while (d.getDay() === 0 || d.getDay() === 6) d.setDate(d.getDate() + 1);
    return d.getFullYear() + '-'
         + String(d.getMonth() + 1).padStart(2, '0') + '-'
         + String(d.getDate()).padStart(2, '0');
  }

  function hoyISO() {
    var d = new Date();
    return d.getFullYear() + '-'
         + String(d.getMonth() + 1).padStart(2, '0') + '-'
         + String(d.getDate()).padStart(2, '0');
  }

  // ── Pintado ──────────────────────────────────────────────────────────
  function filaHtml(c) {
    var dias = c.dias_en_el_pasado || 0;
    var tono = dias >= 7 ? '#dc2626' : (dias >= 3 ? '#f59e0b' : '#6b7280');
    return ''
      + '<tr>'
      +   '<td class="vc-check">'
      +     '<input type="checkbox" class="form-check-input vc-sel" value="' + esc(c.item_id) + '" checked>'
      +   '</td>'
      +   '<td><strong>' + esc(c.doc || '—') + '</strong>'
      +     '<div class="vc-sub">' + esc(c.correlativo || '') + '</div></td>'
      +   '<td>' + esc(c.cliente || '—')
      +     '<div class="vc-sub">' + esc(c.comuna || '') + '</div></td>'
      +   '<td>' + esc((c.courier || '').replace('Transporte ', '')) + '</td>'
      +   '<td class="vc-num">' + fechaCL(c.planned_date) + '</td>'
      +   '<td class="vc-num"><span class="vc-dias" style="color:' + tono + '">'
      +     dias + ' día' + (dias === 1 ? '' : 's') + '</span></td>'
      + '</tr>';
  }

  function pintar(d) {
    var cont = document.getElementById('vcCuerpo');
    var pie  = document.getElementById('vcPie');
    if (!cont) return;

    _cong = (d && d.congeladas) || [];

    if (!_cong.length) {
      cont.innerHTML =
        '<div class="vc-vacio">'
        + '<i class="bi bi-check-circle-fill"></i>'
        + '<div class="vc-vacio-t">Ninguna visita quedó atrapada</div>'
        + '<div class="vc-vacio-s">Se revisaron ' + ((d && d.revisadas) || 0)
        +   ' visitas activas contra la cuenta de cada transportista. '
        +   'Todas están planificadas o ya las tomó el courier.</div>'
        + '</div>';
      if (pie) pie.classList.add('d-none');
      return;
    }

    var sinResp   = (d && d.sin_respuesta) || 0;
    var nVencidas = (d && d.total_vencidas) || 0;
    var plural    = _cong.length === 1 ? '' : 's';

    // Se arman aparte: encadenar un ternario dentro de la concatenacion se
    // vuelve ilegible y es donde se cuelan los errores de sintaxis.
    var txtVencidas = nVencidas > 0
      ? ('<strong>' + nVencidas + '</strong> con la fecha ya vencida: esas no las '
         + 've nadie hasta que se reprogramen.')
      : 'Ninguna está vencida todavía.';
    var txtSinResp = sinResp > 0
      ? ('<div class="vc-aviso"><i class="bi bi-exclamation-triangle-fill me-1"></i>'
         + sinResp + ' visita(s) no respondieron a la consulta y quedaron fuera '
         + 'de esta lista.</div>')
      : '';

    cont.innerHTML = ''
      + '<div class="vc-resumen">'
      +   '<div class="vc-resumen-n">' + _cong.length + '</div>'
      +   '<div>'
      +     '<div class="vc-resumen-t">visita' + plural + ' creada' + plural
      +       ' por ILUS siguen sin planificar</div>'
      +     '<div class="vc-resumen-s">Están en la cuenta del transportista pero sin '
      +       'chofer asignado. ' + txtVencidas + '</div>'
      +   '</div>'
      + '</div>'
      + txtSinResp
      + '<div class="vc-tabla-wrap"><table class="vc-tabla">'
      +   '<thead><tr>'
      +     '<th class="vc-check"><input type="checkbox" class="form-check-input" id="vcTodas" checked></th>'
      +     '<th>Documento</th><th>Cliente</th><th>Courier</th>'
      +     '<th class="vc-num">Programada</th><th class="vc-num">Vencida hace</th>'
      +   '</tr></thead><tbody>'
      +   _cong.map(filaHtml).join('')
      + '</tbody></table></div>';

    var todas = document.getElementById('vcTodas');
    if (todas) {
      todas.addEventListener('change', function () {
        document.querySelectorAll('.vc-sel').forEach(function (ch) { ch.checked = todas.checked; });
        refrescarContador();
      });
    }
    document.querySelectorAll('.vc-sel').forEach(function (ch) {
      ch.addEventListener('change', refrescarContador);
    });

    if (pie) pie.classList.remove('d-none');
    var inp = document.getElementById('vcFecha');
    if (inp) { inp.min = hoyISO(); inp.value = proximoHabil(); }
    refrescarContador();
  }

  function seleccionados() {
    return Array.prototype.slice.call(document.querySelectorAll('.vc-sel:checked'))
      .map(function (ch) { return parseInt(ch.value, 10); })
      .filter(function (n) { return !isNaN(n); });
  }

  function refrescarContador() {
    var n = seleccionados().length;
    var b = document.getElementById('vcBtnRescatar');
    if (!b) return;
    b.disabled = n === 0;
    b.innerHTML = '<i class="bi bi-calendar-check-fill me-1"></i>Reprogramar '
                + n + ' visita' + (n === 1 ? '' : 's');
  }

  // ── Carga ────────────────────────────────────────────────────────────
  async function cargar() {
    if (_cargando) return;
    _cargando = true;
    var cont = document.getElementById('vcCuerpo');
    var pie  = document.getElementById('vcPie');
    if (pie) pie.classList.add('d-none');
    if (cont) {
      cont.innerHTML = '<div class="vc-cargando">'
        + '<span class="spinner-border spinner-border-sm me-2"></span>'
        + 'Preguntándole a cada transportista por sus visitas…</div>';
    }
    var d;
    try {
      var r = await fetch('/transporte/api/simpliroute/visitas-congeladas',
                          { headers: { 'Accept': 'application/json' } });
      d = await r.json();
    } catch (e) {
      d = null;
    }
    _cargando = false;
    if (!d || !d.ok) {
      if (cont) {
        cont.innerHTML = '<div class="vc-error"><i class="bi bi-wifi-off me-1"></i>'
          + esc((d && d.error) || 'No se pudo consultar a los transportistas. Intenta de nuevo.')
          + '</div>';
      }
      return;
    }
    pintar(d);
  }

  // ── Rescate ──────────────────────────────────────────────────────────
  async function rescatar() {
    var ids = seleccionados();
    if (!ids.length) return;

    var inp = document.getElementById('vcFecha');
    var fecha = (inp && inp.value) || '';
    if (!fecha) {
      ilusToast('Elige la fecha a la que quieres reprogramarlas', { type: 'warning' });
      return;
    }

    // Confirmacion explicita: esto escribe en el tablero de un tercero.
    var ok = await ilusConfirm({
      title: 'Reprogramar en el transportista',
      message: 'Se van a mover ' + ids.length + ' visita(s) al ' + fechaCL(fecha) + '.',
      sub: 'Aparecerán ese día en el tablero del courier para que les asigne chofer. '
         + 'Las que el despachador ya haya planificado no se tocan.',
      okLabel: 'Reprogramar', cancelLabel: 'Cancelar',
    });
    if (!ok) return;

    var btn = document.getElementById('vcBtnRescatar');
    var htmlOrig = btn ? btn.innerHTML : '';
    if (btn) {
      btn.disabled = true;
      btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Reprogramando…';
    }

    var d;
    try {
      var r = await fetch('/transporte/api/simpliroute/rescatar-congeladas', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ item_ids: ids, fecha: fecha }),
      });
      d = await r.json();
    } catch (e) {
      d = null;
    }
    if (btn) { btn.disabled = false; btn.innerHTML = htmlOrig; }

    if (!d || !d.ok) {
      ilusToast((d && d.error) || 'No se pudo reprogramar', { type: 'error' });
      return;
    }

    var cont = document.getElementById('vcCuerpo');
    var fallidos = (d.resultados || []).filter(function (x) { return !x.ok; });
    if (cont) {
      cont.innerHTML = ''
        + '<div class="vc-resultado">'
        +   '<div class="vc-resultado-n">' + (d.rescatadas || 0) + '</div>'
        +   '<div class="vc-resultado-t">visita' + ((d.rescatadas === 1) ? '' : 's')
        +     ' reprogramada' + ((d.rescatadas === 1) ? '' : 's') + ' al ' + fechaCL(d.fecha) + '</div>'
        +   '<div class="vc-resultado-s">Ya están en el tablero de ese día. '
        +     'Falta que el despachador les asigne chofer.</div>'
        + '</div>'
        + (fallidos.length
            ? '<div class="vc-aviso mt-2"><div class="fw-bold mb-1">'
              + fallidos.length + ' no se tocaron:</div>'
              + fallidos.map(function (f) {
                  return '• <strong>' + esc(f.doc || f.item_id) + '</strong>: ' + esc(f.error || '—');
                }).join('<br>')
              + '</div>'
            : '');
    }
    var pie = document.getElementById('vcPie');
    if (pie) pie.classList.add('d-none');
    if (d.rescatadas) ilusToast('✓ ' + d.rescatadas + ' visita(s) reprogramada(s)', { type: 'success' });
  }

  // ── Enganche ─────────────────────────────────────────────────────────
  document.addEventListener('DOMContentLoaded', function () {
    var abrir = document.getElementById('btnVisitasCongeladas');
    var modalEl = document.getElementById('vcModal');
    if (!abrir || !modalEl || typeof bootstrap === 'undefined') return;

    _modal = new bootstrap.Modal(modalEl);
    abrir.addEventListener('click', function () { _modal.show(); cargar(); });

    var btnR = document.getElementById('vcBtnRescatar');
    if (btnR) btnR.addEventListener('click', rescatar);

    var btnRe = document.getElementById('vcBtnRecargar');
    if (btnRe) btnRe.addEventListener('click', cargar);
  });
})();

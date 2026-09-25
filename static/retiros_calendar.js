/* ════════════════════════════════════════════════════════════════════
 * ILUS · Retiros — Calendario Inteligente Reusable
 * --------------------------------------------------------------------
 * Widget único de selección de slots de retiro. Consume el endpoint
 *   /retiros/api/disponibilidad-publica (con `?include_owners=1` para
 *   la vista interna del operador).
 *
 * Diseñado para que el FORMULARIO público, el MODAL "Proponer otra
 * fecha" del tracking y el WIZARD interno del operador usen EXACTAMENTE
 * la misma lógica: separación Mañana/Tarde por colación, capacidad
 * paralela, click para INICIO/FIN, atajos 1h/2h/3h, validación de
 * cruces (colación/lleno/bloqueado), persistencia de la duración total
 * en el resumen.
 *
 * Uso:
 *   const cal = IlusRetirosCalendar.mount({
 *     container:        '#mi-grid',         // donde renderiza los slots
 *     dateInput:        '#mi-fecha',        // input[type=date]
 *     summaryEl:        '#mi-resumen',      // barra inferior con rango+duración
 *     quickActionsEl:   '#mi-atajos',       // contenedor de chips 1h/2h/3h
 *     hiddenDate:       'input[name=...]',  // input oculto del form
 *     hiddenTimeFrom:   'input[name=...]',
 *     hiddenTimeTo:     'input[name=...]',
 *     includeOwners:    true,               // solo interno
 *     suggestedDurationMin: 30,             // sugerencia inicial (auto-pick N slots)
 *     onChange: (selection) => {...},       // callback al cambiar selección
 *     monthContainer:   '#mi-mes',          // OPCIONAL (Daniel 2026-09-14: "calendario
 *                                            // tan potente como el de las OT"): mini-
 *                                            // calendario mensual con densidad de cupos
 *                                            // (heatmap verde/ámbar/rojo por día) — clic
 *                                            // en un día = setDate(ese día). Si se omite,
 *                                            // el widget se comporta EXACTAMENTE como
 *                                            // antes (día suelto vía dateInput), cero
 *                                            // impacto en los mounts existentes.
 *   });
 *   cal.suggestDurationMinutes(60);    // recálculo cuando cambia el tiempo estimado
 *   cal.reload();                       // forzar refresh del endpoint
 *   cal.setDate('2026-05-25');          // cambiar día programáticamente
 *
 * Esta función:
 *   - No depende de jQuery / Bootstrap.
 *   - Es idempotente: se puede instanciar varias veces en la misma página.
 *   - Cachea el payload por instancia (ahorra red, refresca con .reload()).
 *
 * Daniel 2026-05-23: el calendario es la "fuente única de verdad" de
 * disponibilidad. Si aquí dice ocupado, también lo dice en los otros lados.
 * ════════════════════════════════════════════════════════════════════ */
(function(global){
  'use strict';

  if (global.IlusRetirosCalendar) return;  // idempotente

  // ───── Helpers comunes ─────────────────────────────────────────────
  function _esc(s){
    return String(s == null ? '' : s).replace(/[&<>"']/g, c => ({
      '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'
    }[c]));
  }
  function _hmToMin(hm){
    const p = String(hm || '').split(':').map(Number);
    return (p[0] || 0) * 60 + (p[1] || 0);
  }
  function _minToHM(min){
    return `${String(Math.floor(min/60)).padStart(2,'0')}:${String(min%60).padStart(2,'0')}`;
  }
  function _qs(sel, root){ return (root || document).querySelector(sel); }
  function _toast(msg, type){
    if (typeof global.ilusToast === 'function'){
      try { return global.ilusToast(msg, { type: type || 'info', duration: 4500 }); } catch(_){}
    }
    console.warn('[ILUS-cal]', type || 'info', msg);
  }

  function _estadoDeSlot(s){
    let e = s.estado;
    if (!e){
      if (s.lunch) e = 'colacion';
      else if (s.razon && !s.disponible) e = 'bloqueado';
      else if (!s.disponible) e = 'completo';
      else if ((s.ocupados || 0) > 0 && (s.ocupados || 0) < (s.max || 2)) e = 'ocupado';
      else e = 'disponible';
    }
    return e;
  }

  // ───── Instancia del calendario ────────────────────────────────────
  function mount(opts){
    const cfg = Object.assign({
      container: null,
      dateInput: null,
      summaryEl: null,
      quickActionsEl: null,
      hiddenDate: null,
      hiddenTimeFrom: null,
      hiddenTimeTo: null,
      includeOwners: false,
      suggestedDurationMin: 30,
      onChange: null,
      onLoadFail: null,
      // Permitir endpoint custom si alguna vista quisiera variantes
      apiUrl: '/retiros/api/disponibilidad-publica',
      // Fallbacks v3 (Daniel 2026-05-24): horario único 09:00-12:30 + 14:00-16:30
      lunchStartFallback: '12:30',
      lunchEndFallback: '14:00',
      slotMinFallback: 30,
      // Daniel 2026-05-24 (operador interno): el operador tiene libertad
      // total — puede agendar para HOY (no solo desde mañana) y puede
      // CRUZAR la colación si la factura es grande. Estas dos flags solo
      // las activa el wizard interno; el cliente público las deja false.
      allowToday: false,
      allowCrossLunch: false,
      // Daniel 2026-05-24: el operador interno necesita ver QUÉ slot
      // corresponde al retiro QUE ESTÁ EDITANDO (marca azul "🔵 Solicitud
      // del cliente"). El widget no busca solo: la vista padre le dice
      // qué ID y qué rango (fecha + hora) pidió el cliente.
      currentRequestId: null,           // int — el RET-XXXXXX actual
      requestedRange: null,             // {date, time_from, time_to} (opcional)
      // Daniel 2026-05-24: drag selection para que el operador pueda
      // "estirar" su selección con click+arrastre. Si false, sigue
      // funcionando con click-1 = inicio, click-2 = fin.
      enableDragSelect: false,
      // Daniel 2026-06-15: rangos multi-bloque (shift+click / drag / botones de
      // duración 1h/2h). En el flujo de propuesta del operador se monta en
      // false → SIEMPRE un solo bloque de 30 min. El código de rango queda
      // intacto y reversible (REGLA #4.2), solo se desactiva por flag.
      enableMultiBlock: false,
      // Daniel 2026-09-14: mini-calendario mensual opcional (ver doc arriba).
      monthContainer: null,
      // Texto de ayuda fijo bajo el calendario mensual (el formulario público
      // muestra "Necesitamos mínimo 24 horas de anticipación…"; el operador
      // interno puede poner otro, o nada).
      monthHelp: '',
      // Cliente público (seguimiento): token de SU retiro para que su propia
      // reserva no cuente contra el cupo (backend: ?exclude_token=). El
      // operador usa currentRequestId + include_owners en su lugar.
      excludeToken: null,
    }, opts || {});

    const grid     = (typeof cfg.container === 'string') ? _qs(cfg.container) : cfg.container;
    const dateInp  = (typeof cfg.dateInput === 'string') ? _qs(cfg.dateInput) : cfg.dateInput;
    const sumEl    = (typeof cfg.summaryEl === 'string') ? _qs(cfg.summaryEl) : cfg.summaryEl;
    const quickEl  = (typeof cfg.quickActionsEl === 'string') ? _qs(cfg.quickActionsEl) : cfg.quickActionsEl;
    const hidDate  = (typeof cfg.hiddenDate === 'string') ? _qs(cfg.hiddenDate) : cfg.hiddenDate;
    const hidTf    = (typeof cfg.hiddenTimeFrom === 'string') ? _qs(cfg.hiddenTimeFrom) : cfg.hiddenTimeFrom;
    const hidTt    = (typeof cfg.hiddenTimeTo === 'string') ? _qs(cfg.hiddenTimeTo) : cfg.hiddenTimeTo;
    const monthEl  = (typeof cfg.monthContainer === 'string') ? _qs(cfg.monthContainer) : cfg.monthContainer;
    if (!grid){
      console.warn('[ILUS-cal] mount: contenedor no encontrado', cfg);
      return null;
    }

    // Estado mutable de la instancia
    const state = {
      payload: null,   // respuesta del endpoint (cache)
      slots:   [],     // slots del día actual
      startIdx: null,
      endIdx:   null,
      currentDate: null,
      suggestedMin: cfg.suggestedDurationMin || 30,
      loading: false,
      month: { anio: null, mes: null },   // mes visible del mini-calendario
    };

    // ── Render helpers ───────────────────────────────────────────────
    function renderEmpty(msg){
      grid.innerHTML = '<div class="ilus-cal-empty"><i class="bi bi-calendar-x"></i>' + _esc(msg) + '</div>';
      if (quickEl) quickEl.style.display = 'none';
      updateSummary();
    }
    function renderSkeleton(){
      let html = '<div class="ilus-cal-skel" aria-hidden="true">';
      for (let i = 0; i < 10; i++) html += '<div></div>';
      html += '</div>';
      grid.innerHTML = html;
    }

    // ── Mini-calendario mensual (Daniel 2026-09-14: "tan potente como el
    // de las OT") ────────────────────────────────────────────────────
    // Densidad de cupos por día (heatmap), navegación mes a mes sobre los
    // 30 días que ya trae `state.payload.dias` (mismo payload que usa el
    // día suelto — CERO llamadas nuevas al backend). Clic en un día =
    // setDate(ese día), que sigue siendo la MISMA función de siempre.
    function _diaHeat(dia){
      if (!dia || !dia.disponible) return { cls: 'is-closed', libres: 0, total: 0 };
      const slots = dia.slots || [];
      let libres = 0, total = 0;
      slots.forEach(s => {
        const e = _estadoDeSlot(s);
        // Colación y "ya pasó"/min_notice no son cupos perdidos: no cuentan.
        if (e === 'colacion' || e === 'no_disponible' || e === 'fuera_horario') return;
        total++;
        if (e === 'disponible') libres++;
      });
      if (total === 0) return { cls: 'is-closed', libres, total };
      // Mismos umbrales que el calendario del formulario público
      // (static/retiros_public_request.js, dia.ocupacion_pct): >=80% ocupado
      // -> rojo, >=40% -> ámbar, si no -> verde. Acá lo derivamos de
      // libres/total porque este payload no trae ocupacion_pct por día.
      const pctOcupado = 1 - (libres / total);
      const cls = pctOcupado >= 0.80 ? 'is-full' : (pctOcupado >= 0.40 ? 'is-tight' : 'is-open');
      return { cls, libres, total };
    }
    const _MES_ES = ['enero','febrero','marzo','abril','mayo','junio','julio',
                      'agosto','septiembre','octubre','noviembre','diciembre'];
    // Línea de estado bajo el calendario — mismo texto/colores que
    // #cal_fecha_msg en el formulario público (onFechaChange).
    function _monthMsgHtml(){
      const f = state.currentDate;
      if (!f || !state.payload) return '';
      const dias = state.payload.dias || {};
      if (!Object.prototype.hasOwnProperty.call(dias, f)){
        return '<span style="color:#dc2626">Fecha fuera del rango agendable</span>';
      }
      const dia = dias[f];
      if (!dia || !dia.disponible){
        return '<span style="color:#dc2626">' + _esc((dia && dia.razon) || 'Día sin cupos') + '</span>';
      }
      const libres = (dia.slots || []).filter(s => s.puede_iniciar).length;
      const txt = libres + ' bloque' + (libres === 1 ? '' : 's') + ' libre' + (libres === 1 ? '' : 's') + ' este día';
      return '<span style="color:' + (libres > 0 ? '#16a34a' : '#dc2626') + '">' + txt + '</span>';
    }
    function renderMonthGrid(){
      if (!monthEl) return;
      if (!state.month.anio){
        const base = state.currentDate ? new Date(state.currentDate + 'T00:00:00') : new Date();
        state.month.anio = base.getFullYear();
        state.month.mes  = base.getMonth() + 1;
      }
      const a = state.month.anio, m = state.month.mes;
      const primerDia = new Date(a, m - 1, 1);
      const nDias = new Date(a, m, 0).getDate();
      let offset = primerDia.getDay() - 1;
      if (offset < 0) offset = 6;
      const hoyStr = new Date().toISOString().slice(0, 10);
      const diasPayload = (state.payload && state.payload.dias) || {};
      // Navegación acotada al horizonte del payload (igual que el formulario
      // público): fuera de [from, to] no hay datos que mostrar.
      const _mk = (y, mo) => y * 100 + mo;
      const _fromKey = state.payload && state.payload.from ? _mk(+state.payload.from.slice(0, 4), +state.payload.from.slice(5, 7)) : null;
      const _toKey   = state.payload && state.payload.to   ? _mk(+state.payload.to.slice(0, 4),   +state.payload.to.slice(5, 7))   : null;
      const _curKey  = _mk(a, m);
      const prevDis = (_fromKey !== null && _curKey <= _fromKey) ? ' disabled' : '';
      const nextDis = (_toKey   !== null && _curKey >= _toKey)   ? ' disabled' : '';
      let html = '<div class="ilus-cal-month-box" role="group" aria-label="Calendario de fechas disponibles">'
        + '<div class="ilus-cal-month-head">'
        + '<button type="button" class="ilus-cal-month-nav" data-cal-month-nav="-1" aria-label="Mes anterior"' + prevDis + '><i class="bi bi-chevron-left"></i></button>'
        + '<span class="ilus-cal-month-label" aria-live="polite">' + _esc(_MES_ES[m - 1]) + ' ' + a + '</span>'
        + '<button type="button" class="ilus-cal-month-nav" data-cal-month-nav="1" aria-label="Mes siguiente"' + nextDis + '><i class="bi bi-chevron-right"></i></button>'
        + '</div>'
        + '<div class="ilus-cal-month-dow" aria-hidden="true">' + ['Lu','Ma','Mi','Ju','Vi','Sá','Do'].map(d => '<span>' + d + '</span>').join('') + '</div>'
        + '<div class="ilus-cal-month-grid">';
      for (let i = 0; i < offset; i++) html += '<span class="ilus-cal-month-day is-blank" aria-hidden="true"></span>';
      for (let d = 1; d <= nDias; d++){
        const fecha = a + '-' + String(m).padStart(2, '0') + '-' + String(d).padStart(2, '0');
        const inRange = Object.prototype.hasOwnProperty.call(diasPayload, fecha);
        const heat = inRange ? _diaHeat(diasPayload[fecha]) : { cls: 'is-out', libres: 0, total: 0 };
        const cls = ['ilus-cal-month-day', heat.cls];
        const isToday = fecha === hoyStr;
        const isSelected = fecha === state.currentDate;
        if (isToday) cls.push('is-today');
        if (isSelected) cls.push('is-selected');
        const clickable = inRange && heat.cls !== 'is-closed';
        if (!clickable) cls.push('is-disabled');
        const aria = fecha + (!inRange ? ' — fuera de rango' : heat.cls === 'is-closed' ? ' — no disponible' : (' — ' + heat.libres + ' de ' + heat.total + ' bloques libres'));
        // Marca bajo el número: punto de color (libre/medio/lleno), candado
        // (cerrado/fin de semana) o nada (fuera de los 30 días con datos).
        let mark = '<span class="mark"></span>';
        if (heat.cls === 'is-closed') mark = '<i class="bi bi-lock-fill mark-lock"></i>';
        else if (heat.cls === 'is-out') mark = '';
        html += '<button type="button" class="' + cls.join(' ') + '"'
          + (clickable ? (' data-cal-month-day="' + fecha + '"') : ' disabled')
          + ' aria-label="' + _esc(aria) + '">'
          + '<span class="d">' + d + '</span>' + mark
          + '</button>';
      }
      html += '</div>'
        + '<div class="ilus-cal-month-legend">'
        + '<span><i class="dot" style="background:#16a34a"></i>Libre</span>'
        + '<span><i class="dot" style="background:#f59e0b"></i>Medio</span>'
        + '<span><i class="dot" style="background:#dc2626"></i>Lleno</span>'
        + '<span><i class="bi bi-lock-fill" style="color:#9ca3af;font-size:.7rem"></i>&nbsp;No disponible</span>'
        + '</div>'
        + '</div>'
        // Mismo par de líneas que el formulario público bajo el calendario:
        // ayuda fija (configurable por mount) + estado del día elegido.
        + (cfg.monthHelp ? '<div class="ilus-cal-month-help">' + _esc(cfg.monthHelp) + '</div>' : '')
        + '<small class="ilus-cal-month-msg" aria-live="polite">' + _monthMsgHtml() + '</small>';
      monthEl.innerHTML = html;
      monthEl.querySelectorAll('[data-cal-month-nav]').forEach(b => {
        b.addEventListener('click', () => {
          const dir = parseInt(b.dataset.calMonthNav, 10);
          let mm = state.month.mes + dir, aa = state.month.anio;
          if (mm < 1){ mm = 12; aa--; } else if (mm > 12){ mm = 1; aa++; }
          state.month.mes = mm; state.month.anio = aa;
          renderMonthGrid();
        });
      });
      monthEl.querySelectorAll('[data-cal-month-day]').forEach(b => {
        b.addEventListener('click', () => setDate(b.dataset.calMonthDay));
      });
    }

    function renderGrid(){
      if (!state.slots.length){
        grid.innerHTML = '<div class="ilus-cal-empty"><i class="bi bi-info-circle"></i>Sin bloques configurados para este día.</div>';
        return;
      }
      const lunchStart = (state.payload && state.payload.lunch_start) || cfg.lunchStartFallback;
      const lunchStartMin = _hmToMin(lunchStart);

      // Daniel 2026-05-24: ¿este slot coincide con la hora pedida por el
      // cliente? Calculamos UNA vez por render, no por cada slot.
      const reqRange = cfg.requestedRange;
      const isInRequestedRange = (slot) => {
        if (!reqRange || !reqRange.date || !reqRange.time_from) return false;
        if (state.currentDate !== reqRange.date) return false;
        const sM = _hmToMin(slot.time_from || slot.hora || '00:00');
        const eM = _hmToMin(slot.time_to || slot.hora || '00:00');
        const reqStart = _hmToMin(reqRange.time_from);
        const reqEnd   = reqRange.time_to ? _hmToMin(reqRange.time_to) : reqStart + 30;
        // Solape: el slot toca el rango pedido por el cliente
        return !(eM <= reqStart || sM >= reqEnd);
      };

      const slotHtml = (s, i) => {
        const cls = ['ilus-cal-slot'];
        let badge = '';
        let title = '';
        let ownersLine = '';
        let flagLine = '';
        const estado = _estadoDeSlot(s);
        const owners = Array.isArray(s.owners) ? s.owners : [];
        // Daniel 2026-05-24: para vista PÚBLICA (sin owners) el cliente solo
        // ve "libre" o "no disponible" con candado, SIN número de cupos ni
        // razón. La vista INTERNA (operador con includeOwners=true) sigue
        // mostrando el detalle 1/2 + capacidad parcial clickeable.
        const internalView = !!cfg.includeOwners;

        // ¿es slot del CURRENT request? (oro)
        const isCurrentOwner = internalView && cfg.currentRequestId &&
              owners.some(o => Number(o.request_id) === Number(cfg.currentRequestId));
        // ¿es el rango que pidió el cliente? (azul)
        const isRequestedByClient = isInRequestedRange(s);

        // Tooltip enriquecido (operador): cliente + estado + capacidad
        const buildRichTitle = () => {
          const parts = [];
          const hm = `${s.time_from || s.hora || ''}${s.time_to ? ' – ' + s.time_to : ''}`;
          parts.push(hm);
          if (estado === 'colacion') parts.push('Colación');
          else if (estado === 'completo') parts.push('Cupo lleno');
          else if (estado === 'bloqueado') parts.push(s.razon || 'Bloqueado');
          else if (estado === 'ocupado') parts.push(`Parcial (${(s.ocupacion_actual || s.ocupados || 1)}/${(s.capacidad_max || s.max || 2)})`);
          else parts.push('Disponible');
          if (owners.length > 0){
            owners.forEach(o => {
              parts.push(`${o.code} · ${o.customer_name}${o.status_label ? ' (' + o.status_label + ')' : ''}`);
            });
          }
          if (isRequestedByClient) parts.push('★ Hora pedida por el cliente');
          return parts.join('\n');
        };

        if (estado === 'colacion'){
          cls.push('is-lunch');
          if (cfg.allowCrossLunch) cls.push('is-lunch-crossable'); else cls.push('is-disabled');
          title = internalView ? 'Horario de colación' + (cfg.allowCrossLunch ? ' (cruzable con aviso)' : ' (no agendable)') : 'Hora no disponible';
        } else if (estado === 'completo'){
          cls.push('is-full', 'is-disabled');
          if (internalView){
            const oc = s.ocupacion_actual != null ? s.ocupacion_actual : (s.ocupados || 0);
            const mx = s.capacidad_max != null ? s.capacidad_max : (s.max || 2);
            title = `Cupo lleno (${oc}/${mx})`;
          } else {
            title = 'Hora no disponible';
          }
        } else if (estado === 'bloqueado'){
          cls.push('is-blocked', 'is-disabled');
          title = internalView ? (s.razon || 'Franja bloqueada') : 'Hora no disponible';
        } else if (estado === 'no_disponible' || estado === 'fuera_horario'){
          // "El horario ya pasó" (hoy, operador) o min_notice (cliente). Antes
          // caía al else y se pintaba como libre — y era clickeable.
          cls.push('is-blocked', 'is-disabled');
          title = internalView ? (s.razon || 'No disponible') : 'Hora no disponible';
        } else if (estado === 'ocupado'){
          if (internalView){
            cls.push('is-busy');
            const oc = s.ocupacion_actual != null ? s.ocupacion_actual : (s.ocupados || 1);
            const mx = s.capacidad_max != null ? s.capacidad_max : (s.max || 2);
            badge = `<span class="ilus-cal-badge">${oc}/${mx}</span>`;
            title = `Parcial (${oc}/${mx}) — aún puedes agendar`;
          } else {
            // Vista PÚBLICA: ocupado se ve igual que completo (no clickeable).
            cls.push('is-full', 'is-disabled');
            title = 'Hora no disponible';
          }
        } else {
          title = internalView
            ? `Disponible (${s.time_from || s.hora || ''} – ${s.time_to || ''})`
            : 'Disponible';
        }

        // Banderas visuales prioritarias (orden: current owner gana sobre solicitado)
        if (isCurrentOwner){
          cls.push('is-current-request');
          flagLine = `<span class="ilus-cal-flag is-current" title="Este retiro está aquí">★ ESTE</span>`;
        } else if (isRequestedByClient){
          cls.push('is-requested-by-client');
          flagLine = `<span class="ilus-cal-flag is-requested" title="Hora pedida por el cliente">🔵 Pedido</span>`;
        }

        // Solo modo INTERNO: mostrar dueños del slot bajo la hora.
        // Daniel 2026-06-15: mostrar QUIÉN tiene el (otro) medio cupo de forma
        // VISIBLE — el nombre del cliente, no solo el código RET-XXX (ese queda
        // en el tooltip). El operador necesita saber con quién compartiría.
        if (cfg.includeOwners && owners.length > 0){
          const _firstName = (nm) => _esc(String(nm || '').trim().split(/\s+/)[0] || '');
          if (owners.length === 1){
            const o = owners[0];
            const _nm = (o.customer_name || '').trim();
            ownersLine = `<span class="ilus-cal-owner-line" title="${_esc(o.code)} · ${_esc(_nm)}">${_nm ? _esc(_nm) : _esc(o.code)}</span>`;
          } else {
            const _full  = owners.map(o => _esc(o.code) + ' · ' + _esc(o.customer_name || '')).join('  /  ');
            const _short = owners.map(o => _firstName(o.customer_name || o.code)).join(' · ');
            ownersLine = `<span class="ilus-cal-owner-line" title="${_full}">${_short}</span>`;
          }
        }

        // Sobrescribe título plano con versión enriquecida (operador)
        if (internalView) title = buildRichTitle();

        if (state.startIdx !== null && state.endIdx !== null){
          if (i === state.startIdx && i === state.endIdx){
            cls.push('is-selected', 'is-start', 'is-end');
          } else if (i === state.startIdx){
            cls.push('is-selected', 'is-start');
          } else if (i === state.endIdx){
            cls.push('is-selected', 'is-end');
          } else if (i > state.startIdx && i < state.endIdx){
            cls.push('is-in-range');
          }
        }
        const hora = s.time_from || s.hora || '';
        const safeTitle = _esc(title);
        return `<div class="${cls.join(' ')}" data-ilus-cal-idx="${i}" title="${safeTitle}" role="button" tabindex="${cls.indexOf('is-disabled') === -1 ? 0 : -1}">${flagLine}${badge}<span class="ilus-cal-hora">${_esc(hora)}</span>${ownersLine}</div>`;
      };

      // Buscar primer slot post-colación (frontera Mañana / Tarde)
      let postLunchIdx = -1;
      for (let i = 0; i < state.slots.length; i++){
        const s = state.slots[i];
        const startMin = _hmToMin(s.time_from || s.hora || '00:00');
        const isLunch = (s.estado === 'colacion') || s.lunch;
        if (!isLunch && startMin >= lunchStartMin){ postLunchIdx = i; break; }
      }
      const hasMorning   = state.slots.some((s, i) => postLunchIdx === -1 ? true : i < postLunchIdx);
      const hasAfternoon = postLunchIdx !== -1;

      let html = '';
      if (hasMorning && hasAfternoon){
        html += '<div class="ilus-cal-section">Mañana</div>';
        html += '<div class="ilus-cal-slots-grid">' + state.slots.slice(0, postLunchIdx).map((s, i) => slotHtml(s, i)).join('') + '</div>';
        html += '<div class="ilus-cal-section">Tarde</div>';
        html += '<div class="ilus-cal-slots-grid">' + state.slots.slice(postLunchIdx).map((s, j) => slotHtml(s, postLunchIdx + j)).join('') + '</div>';
      } else {
        html = '<div class="ilus-cal-slots-grid">' + state.slots.map((s, i) => slotHtml(s, i)).join('') + '</div>';
      }
      grid.innerHTML = html;

      // Bind eventos — click simple + shift+click. Drag se enlaza UNA
      // sola vez a nivel de grid (los handlers globales se mantienen
      // estables entre renderGrid para evitar fugas).
      grid.querySelectorAll('[data-ilus-cal-idx]').forEach(el => {
        // Click (con soporte para shift+click = rango)
        el.addEventListener('click', (ev) => {
          if (_dragState && _dragState.suppressedClick){
            _dragState.suppressedClick = false;
            _dragState = null;
            return;
          }
          const i = parseInt(el.dataset.ilusCalIdx, 10);
          // Shift+click → seleccionar rango desde startIdx actual hasta i.
          // Daniel 2026-06-15: solo si la vista permite multi-bloque; en el
          // flujo de propuesta (bloque único) el shift+click se trata como
          // click normal → un solo bloque.
          if (cfg.enableMultiBlock && ev.shiftKey && state.startIdx !== null){
            tryExtendSelection(state.startIdx, i);
          } else {
            onSlotClick(i);
          }
        });
        el.addEventListener('keydown', (e) => {
          if (e.key === 'Enter' || e.key === ' '){
            e.preventDefault();
            onSlotClick(parseInt(el.dataset.ilusCalIdx, 10));
          }
        });
      });
    }

    // Drag selection (mouse). Solo si lo activan explícitamente.
    // Importante: los listeners globales se enlazan UNA SOLA VEZ por
    // instancia, no por cada renderGrid (que se llama muchas veces).
    let _dragState = null;
    const _isDraggableSlot = (el) => {
      if (!el) return false;
      if (el.classList.contains('is-disabled')) return false;
      if (el.classList.contains('is-full') || el.classList.contains('is-blocked')) return false;
      return true;
    };
    function _bindDragOnce(){
      if (!cfg.enableDragSelect) return;
      grid.addEventListener('mousedown', (ev) => {
        if (ev.button !== 0) return;
        const el = ev.target && ev.target.closest && ev.target.closest('[data-ilus-cal-idx]');
        if (!el || !_isDraggableSlot(el)) return;
        const i = parseInt(el.dataset.ilusCalIdx, 10);
        _dragState = { startIdx: i, currentIdx: i, moved: false, suppressedClick: false };
      });
      document.addEventListener('mousemove', (ev) => {
        if (!_dragState) return;
        const target = document.elementFromPoint(ev.clientX, ev.clientY);
        if (!target) return;
        const slotEl = target.closest && target.closest('[data-ilus-cal-idx]');
        if (!slotEl || !grid.contains(slotEl)) return;
        const i = parseInt(slotEl.dataset.ilusCalIdx, 10);
        if (i !== _dragState.currentIdx){
          _dragState.currentIdx = i;
          _dragState.moved = true;
          tryExtendSelection(_dragState.startIdx, i, /*silent=*/true);
        }
      });
      document.addEventListener('mouseup', (ev) => {
        if (!_dragState) return;
        const moved = _dragState.moved;
        if (moved){
          // Re-validar final con toast si hubo error
          const target = document.elementFromPoint(ev.clientX, ev.clientY);
          const slotEl = target && target.closest && target.closest('[data-ilus-cal-idx]');
          let finalIdx = _dragState.currentIdx;
          if (slotEl && grid.contains(slotEl)){
            finalIdx = parseInt(slotEl.dataset.ilusCalIdx, 10);
          }
          tryExtendSelection(_dragState.startIdx, finalIdx, /*silent=*/false);
          _dragState.suppressedClick = true;
          // No nullificar aún — esperamos al click event que viene
        } else {
          _dragState = null;
        }
      });
    }
    _bindDragOnce();

    // Extiende selección desde idxA a idxB validando estados intermedios.
    // Reutiliza la misma lógica que onSlotClick para mantener un único path.
    function tryExtendSelection(idxA, idxB, silent){
      const from = Math.min(idxA, idxB);
      const to   = Math.max(idxA, idxB);
      const internalView = !!cfg.includeOwners;
      let invalido = null;
      let cruzaColacion = false;
      for (let k = from; k <= to; k++){
        const ek = _estadoDeSlot(state.slots[k]);
        if (ek === 'colacion'){
          if (cfg.allowCrossLunch){ cruzaColacion = true; }
          else { invalido = 'colacion'; break; }
        }
        if (ek === 'completo'){ invalido = 'completo'; break; }
        if (ek === 'bloqueado' || ek === 'no_disponible' || ek === 'fuera_horario'){ invalido = 'bloqueado'; break; }
        if (!internalView && ek === 'ocupado'){ invalido = 'completo'; break; }
      }
      if (invalido){
        if (silent) return;  // drag-live: no romper, esperar al mouseup
        if (invalido === 'colacion'){
          const ls = (state.payload && state.payload.lunch_start) || cfg.lunchStartFallback;
          const le = (state.payload && state.payload.lunch_end) || cfg.lunchEndFallback;
          _toast(`No puede cruzar colación (${ls}-${le}). Acórtalo.`, 'warning');
        } else if (invalido === 'completo'){
          _toast('El rango cruza un bloque lleno. Acórtalo.', 'warning');
        } else {
          _toast('El rango cruza una franja bloqueada.', 'warning');
        }
        // Recortamos al inicio (operador puede volver a elegir)
        state.startIdx = state.endIdx = idxB;
      } else {
        state.startIdx = from; state.endIdx = to;
        if (cruzaColacion && cfg.allowCrossLunch && !silent){
          const ls = (state.payload && state.payload.lunch_start) || cfg.lunchStartFallback;
          const le = (state.payload && state.payload.lunch_end) || cfg.lunchEndFallback;
          _toast(`Cruza la colación del equipo (${ls}-${le}). Confirma con bodega.`, 'warning');
        }
      }
      updateSummary();
      renderGrid();
    }

    function onSlotClick(i){
      const s = state.slots[i];
      if (!s) return;
      const estado = _estadoDeSlot(s);
      const internalView = !!cfg.includeOwners;

      // Pasado / min_notice: no seleccionable en ninguna vista (el backend
      // igual lo rechazaría en el POST; mejor avisar aquí).
      if (estado === 'no_disponible' || estado === 'fuera_horario'){
        _toast(internalView ? (s.razon || 'Ese bloque ya no está disponible.') : 'Esa hora no está disponible. Elige otro bloque.', 'warning');
        return;
      }
      // Vista PÚBLICA (cliente externo): cualquier slot no plenamente
      // libre se rechaza con mensaje genérico.
      if (!internalView){
        if (estado === 'colacion' || estado === 'completo' ||
            estado === 'ocupado' || estado === 'bloqueado'){
          _toast('Esa hora no está disponible. Elige otro bloque.', 'warning');
          return;
        }
      } else {
        // Vista INTERNA (operador): mensajes detallados como antes,
        // y 'ocupado' (1/2) sigue siendo clickeable (puede compartir slot).
        if (estado === 'colacion'){
          const ls = (state.payload && state.payload.lunch_start) || cfg.lunchStartFallback;
          const le = (state.payload && state.payload.lunch_end) || cfg.lunchEndFallback;
          if (cfg.allowCrossLunch){
            // Daniel 2026-05-24: el operador puede tomar slots de colación
            // (factura grande). Aviso amarillo no bloqueante.
            _toast(`Estás tomando un slot de colación (${ls}-${le}). Confirma con bodega.`, 'warning');
            // No return → seguimos al flujo de selección normal.
          } else {
            _toast(`Ese horario es de colación (${ls}-${le}). Elige antes o después.`, 'warning');
            return;
          }
        }
        if (estado === 'completo'){ _toast('Ese bloque está lleno. Elige otro horario.', 'warning'); return; }
        if (estado === 'bloqueado'){ _toast(s.razon || 'Esa franja está bloqueada.', 'warning'); return; }
      }

      // Daniel 2026-06-15: CLICK SIMPLE = UN SOLO BLOQUE de 30 min.
      // El operador pidió explícitamente "seleccionar un solo bloque, no
      // todos". El modelo viejo (1er click = inicio, 2º click = fin) armaba
      // rangos gigantes (ej. 09:00→15:30 cruzando la colación) cuando quedaba
      // un inicio "pegado". Ahora cada click simple resetea a ese único bloque.
      // En la vista de propuesta del operador (enableMultiBlock=false) las vías
      // multi-bloque (shift+click, arrastrar, botones de duración) están
      // DESACTIVADAS por flag → siempre un solo bloque. Solo se reactivan si se
      // monta el widget con enableMultiBlock=true (otra vista futura).
      if (state.startIdx === i && state.endIdx === i){
        // Toggle: click en el bloque ya seleccionado solo → deseleccionar.
        state.startIdx = state.endIdx = null;
      } else {
        // Cualquier otro click → SOLO este bloque (sin rango automático).
        state.startIdx = state.endIdx = i;
      }
      updateSummary();
      renderGrid();
    }

    function setQuickRange(hours){
      if (!hours || hours <= 0){ clearSelection(); return; }
      const slotMin = (state.payload && state.payload.slot_minutes) || cfg.slotMinFallback;
      // Daniel 2026-06-15: en modo bloque único (!enableMultiBlock) cualquier
      // duración colapsa a 1 bloque — nunca arma rangos.
      const slotsNeeded = cfg.enableMultiBlock ? Math.max(1, Math.round(hours * 60 / slotMin)) : 1;
      const internalView = !!cfg.includeOwners;
      let foundStart = -1, foundEnd = -1;
      for (let i = 0; i < state.slots.length; i++){
        const s0 = state.slots[i];
        // En vista pública exigir slot plenamente libre; en interna basta
        // con que pueda iniciar (compatible con compartir capacidad 1/2).
        const ocup0 = (s0.ocupados || 0);
        const puedeIni = internalView
          ? ((s0.puede_iniciar !== undefined) ? s0.puede_iniciar : (s0.disponible && !s0.lunch))
          : (s0.disponible && !s0.lunch && ocup0 === 0);
        if (!puedeIni) continue;
        const endIdx = i + slotsNeeded - 1;
        if (endIdx >= state.slots.length) continue;
        let ok = true;
        for (let k = i; k <= endIdx; k++){
          const ek = _estadoDeSlot(state.slots[k]);
          if (ek === 'colacion' || ek === 'completo' || ek === 'bloqueado' ||
              ek === 'no_disponible' || ek === 'fuera_horario'){ ok = false; break; }
          if (!internalView && ek === 'ocupado'){ ok = false; break; }
        }
        if (ok){ foundStart = i; foundEnd = endIdx; break; }
      }
      if (foundStart < 0){
        _toast(`No hay ${hours} hora${hours===1?'':'s'} contiguas libres este día. Prueba otra fecha o menos tiempo.`, 'warning');
        return;
      }
      state.startIdx = foundStart; state.endIdx = foundEnd;
      updateSummary();
      renderGrid();
    }

    function clearSelection(){
      state.startIdx = state.endIdx = null;
      updateSummary();
      renderGrid();
    }

    function getSelection(){
      if (state.startIdx === null) return null;
      const slotMin = (state.payload && state.payload.slot_minutes) || cfg.slotMinFallback;
      const sS = state.slots[state.startIdx];
      const sE = state.slots[state.endIdx];
      if (!sS || !sE) return null;
      const startStr = sS.time_from || sS.hora;
      let endStr = sE.time_to;
      if (!endStr){
        const baseMin = _hmToMin(sE.time_from || sE.hora);
        endStr = _minToHM(baseMin + slotMin);
      }
      const nBlocks = state.endIdx - state.startIdx + 1;
      const totalMin = nBlocks * slotMin;
      return {
        date:    state.currentDate,
        time_from: startStr,
        time_to:   endStr,
        n_blocks:  nBlocks,
        duration_min: totalMin,
      };
    }

    function updateSummary(){
      const sel = getSelection();
      if (sumEl){
        if (!sel){
          sumEl.innerHTML = '<span class="ilus-cal-sum-empty"><i class="bi bi-arrow-up me-1"></i>Selecciona un bloque arriba</span>';
          sumEl.classList.remove('is-active');
        } else {
          let durTxt;
          const mins = sel.duration_min;
          if (mins < 60) durTxt = `${mins} min`;
          else if (mins % 60 === 0) durTxt = `${mins/60} h`;
          else durTxt = `${(mins/60).toFixed(1)} h`;
          sumEl.innerHTML = `<i class="bi bi-clock-fill me-2" style="color:#16a34a"></i><span class="ilus-cal-sum-range">${_esc(sel.time_from)} – ${_esc(sel.time_to)}</span><span class="ilus-cal-sum-dur"><i class="bi bi-stopwatch me-1"></i>${durTxt}</span>`;
          sumEl.classList.add('is-active');
        }
      }
      // Sincronizar inputs ocultos del form
      if (hidDate)  hidDate.value  = sel ? (sel.date || '') : '';
      if (hidTf)    hidTf.value    = sel ? sel.time_from   : '';
      if (hidTt)    hidTt.value    = sel ? sel.time_to     : '';
      // Callback externo
      if (typeof cfg.onChange === 'function'){
        try { cfg.onChange(sel); } catch(e){ console.error('[ILUS-cal] onChange', e); }
      }
    }

    // ── Carga del endpoint ──────────────────────────────────────────
    async function loadPayload(force){
      if (state.payload && !force) return state.payload;
      state.loading = true;
      try {
        let url = cfg.apiUrl;
        if (cfg.includeOwners){
          url += (url.indexOf('?') >= 0 ? '&' : '?') + 'include_owners=1';
          // FIX Daniel 2026-06-15: excluir el retiro EN GESTIÓN del conteo de
          // ocupación. Sin esto, el propio retiro se cuenta a sí mismo y su
          // bloque sale "completo" (ej: 2/2) cuando en realidad hay cupo. El
          // backend solo lo honra para operadores autenticados (include_owners).
          if (cfg.currentRequestId){
            url += '&exclude_id=' + encodeURIComponent(cfg.currentRequestId);
          }
        } else if (cfg.excludeToken){
          url += (url.indexOf('?') >= 0 ? '&' : '?') + 'exclude_token=' + encodeURIComponent(cfg.excludeToken);
        }
        const r = await fetch(url, { credentials: 'same-origin', cache: 'no-store' });
        if (!r.ok) throw new Error('HTTP ' + r.status);
        state.payload = await r.json();
        if (dateInp){
          // Daniel 2026-05-24: el operador interno (allowToday) puede agendar
          // desde HOY; el cliente público sigue limitado a mañana en adelante.
          const today = new Date();
          const todayStr = today.toISOString().slice(0,10);
          const tomorrow = new Date(Date.now() + 24*3600*1000);
          const tomorrowStr = tomorrow.toISOString().slice(0,10);
          const minStr = cfg.allowToday ? todayStr : tomorrowStr;
          dateInp.min = (state.payload.from < minStr) ? minStr : state.payload.from;
          dateInp.max = state.payload.to;
        }
        return state.payload;
      } catch (e){
        if (typeof cfg.onLoadFail === 'function'){
          try { cfg.onLoadFail(e); } catch(_){}
        }
        renderEmpty('No se pudo cargar la disponibilidad. Revisa tu conexión y reintenta.');
        throw e;
      } finally {
        state.loading = false;
      }
    }

    function setDate(fecha){
      state.currentDate = fecha || null;
      state.startIdx = state.endIdx = null;
      if (dateInp && dateInp.value !== fecha){
        dateInp.value = fecha || '';
        // Al elegir un día desde el mini-calendario (o por API) el input
        // cambia por JS y NO emite 'change' solo — los listeners externos
        // (hint de cupos del modal, preview del mensaje, semáforo de pasos)
        // se quedaban con el día anterior. Lo emitimos nosotros; el propio
        // listener del widget lo ignora vía _silenciarChange.
        state._silenciarChange = true;
        try { dateInp.dispatchEvent(new Event('change', { bubbles: true })); }
        catch(_){}
        finally { state._silenciarChange = false; }
      }
      // Mini-calendario: si el día cae en otro mes, salta ahí; siempre
      // repinta para reflejar la nueva selección (independiente de si el
      // día resulta agendable o no — cualquier salida abajo debe verse).
      if (fecha){
        const d0 = new Date(fecha + 'T00:00:00');
        state.month.anio = d0.getFullYear();
        state.month.mes  = d0.getMonth() + 1;
      }
      renderMonthGrid();

      if (!fecha){ renderEmpty('Selecciona una fecha primero.'); return; }
      // Defense in depth: el cliente público necesita ≥24h; el operador
      // (allowToday=true) puede agendar para HOY mismo. Nadie agenda pasado.
      const minDate = new Date();
      if (cfg.allowToday){
        minDate.setHours(0,0,0,0);
      } else {
        minDate.setTime(Date.now() + 24*3600*1000);
        minDate.setHours(0,0,0,0);
      }
      if (new Date(fecha + 'T00:00:00') < minDate){
        renderEmpty(cfg.allowToday
          ? 'Esa fecha ya pasó. Elige hoy o una fecha futura.'
          : 'Esa fecha ya no está disponible. Elige otra fecha del calendario.'
        );
        return;
      }
      if (!state.payload){
        renderEmpty('Cargando disponibilidad…');
        return;
      }
      const dia = (state.payload.dias || {})[fecha];
      if (!dia){
        renderEmpty('Sin datos para ' + fecha + '. Está fuera del rango de 30 días.');
        return;
      }
      if (!dia.disponible){
        renderEmpty((dia.razon || 'Día no operativo') + ' — ' + fecha);
        return;
      }
      state.slots = dia.slots || [];
      renderGrid();
      if (quickEl) quickEl.style.display = state.slots.some(s => s.puede_iniciar) ? 'flex' : 'none';
      // Auto-seleccionar duración sugerida (solo en modo multi-bloque y si no
      // hay nada seleccionado). Daniel 2026-06-15: en bloque único NO
      // pre-seleccionamos nada — el operador escoge la media hora a mano,
      // evitando el "bloque pegado" sorpresa.
      if (cfg.enableMultiBlock && state.suggestedMin && state.suggestedMin > 0){
        const hours = Math.max(0.5, state.suggestedMin / 60);
        setQuickRange(hours);
      }
      updateSummary();
    }

    function suggestDurationMinutes(min){
      state.suggestedMin = Math.max(0, min || 0);
      // Re-aplicar inmediato si el día está cargado y NO hay selección activa
      // (solo modo multi-bloque; en bloque único no auto-seleccionamos).
      if (cfg.enableMultiBlock && state.payload && state.slots.length && state.startIdx === null){
        const hours = Math.max(0.5, state.suggestedMin / 60);
        setQuickRange(hours);
      }
    }

    // ── Bindings ────────────────────────────────────────────────────
    if (dateInp){
      dateInp.addEventListener('change', () => {
        if (state._silenciarChange) return;   // lo emitió setDate() — ya está renderizado
        setDate(dateInp.value);
      });
    }
    if (quickEl){
      // Si trae chips con data-ilus-cal-quick="N" los conectamos
      quickEl.querySelectorAll('[data-ilus-cal-quick]').forEach(el => {
        el.addEventListener('click', () => {
          const h = parseFloat(el.dataset.ilusCalQuick);
          if (h === 0 || el.dataset.ilusCalClear === '1') clearSelection();
          else setQuickRange(h);
        });
      });
    }

    // ── Init ────────────────────────────────────────────────────────
    renderSkeleton();
    loadPayload().then(() => {
      const initial = (dateInp && dateInp.value) || null;
      if (initial) setDate(initial);
      else { renderEmpty('Selecciona una fecha para ver los bloques disponibles.'); renderMonthGrid(); }
    }).catch(() => {});

    // API pública de la instancia
    return {
      setDate,
      // Mostrar el mes de una fecha SIN seleccionarla (la agenda interna lo
      // usa al navegar semana a semana: el mini-calendario sigue la semana).
      showMonth: (fecha) => {
        if (!fecha) return;
        const d0 = new Date(String(fecha).slice(0, 10) + 'T00:00:00');
        if (isNaN(d0)) return;
        state.month.anio = d0.getFullYear();
        state.month.mes  = d0.getMonth() + 1;
        renderMonthGrid();
      },
      setQuickRange,
      clearSelection,
      getSelection,
      suggestDurationMinutes,
      reload: () => {
        state.payload = null;
        renderSkeleton();
        return loadPayload(true).then(() => {
          if (state.currentDate) setDate(state.currentDate); else renderMonthGrid();
        });
      },
      // Atajos para debug
      _state: () => Object.assign({}, state),
    };
  }

  // ───── Export global ────────────────────────────────────────────────
  global.IlusRetirosCalendar = {
    mount,
    _utils: { _esc, _hmToMin, _minToHM, _estadoDeSlot },
  };
})(window);

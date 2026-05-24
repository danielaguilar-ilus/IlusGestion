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
    }, opts || {});

    const grid     = (typeof cfg.container === 'string') ? _qs(cfg.container) : cfg.container;
    const dateInp  = (typeof cfg.dateInput === 'string') ? _qs(cfg.dateInput) : cfg.dateInput;
    const sumEl    = (typeof cfg.summaryEl === 'string') ? _qs(cfg.summaryEl) : cfg.summaryEl;
    const quickEl  = (typeof cfg.quickActionsEl === 'string') ? _qs(cfg.quickActionsEl) : cfg.quickActionsEl;
    const hidDate  = (typeof cfg.hiddenDate === 'string') ? _qs(cfg.hiddenDate) : cfg.hiddenDate;
    const hidTf    = (typeof cfg.hiddenTimeFrom === 'string') ? _qs(cfg.hiddenTimeFrom) : cfg.hiddenTimeFrom;
    const hidTt    = (typeof cfg.hiddenTimeTo === 'string') ? _qs(cfg.hiddenTimeTo) : cfg.hiddenTimeTo;
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

    function renderGrid(){
      if (!state.slots.length){
        grid.innerHTML = '<div class="ilus-cal-empty"><i class="bi bi-info-circle"></i>Sin bloques configurados para este día.</div>';
        return;
      }
      const lunchStart = (state.payload && state.payload.lunch_start) || cfg.lunchStartFallback;
      const lunchStartMin = _hmToMin(lunchStart);

      const slotHtml = (s, i) => {
        const cls = ['ilus-cal-slot'];
        let badge = '';
        let title = '';
        let ownersLine = '';
        const estado = _estadoDeSlot(s);
        const owners = Array.isArray(s.owners) ? s.owners : [];

        if (estado === 'colacion'){
          cls.push('is-lunch', 'is-disabled');
          title = 'Horario de colación (no agendable)';
        } else if (estado === 'completo'){
          cls.push('is-full', 'is-disabled');
          const oc = s.ocupacion_actual != null ? s.ocupacion_actual : (s.ocupados || 0);
          const mx = s.capacidad_max != null ? s.capacidad_max : (s.max || 2);
          title = `Cupo lleno (${oc}/${mx})`;
        } else if (estado === 'bloqueado'){
          cls.push('is-blocked', 'is-disabled');
          title = s.razon || 'Franja bloqueada';
        } else if (estado === 'ocupado'){
          cls.push('is-busy');
          const oc = s.ocupacion_actual != null ? s.ocupacion_actual : (s.ocupados || 1);
          const mx = s.capacidad_max != null ? s.capacidad_max : (s.max || 2);
          badge = `<span class="ilus-cal-badge">${oc}/${mx}</span>`;
          title = `Parcial (${oc}/${mx}) — aún puedes agendar`;
        } else {
          title = `Disponible (${s.time_from || s.hora || ''} – ${s.time_to || ''})`;
        }

        // Solo modo INTERNO: mostrar dueños del slot bajo la hora
        if (cfg.includeOwners && owners.length > 0){
          if (owners.length === 1){
            ownersLine = `<span class="ilus-cal-owner-line" title="${_esc(owners[0].code)} · ${_esc(owners[0].customer_name)}">${_esc(owners[0].code)}</span>`;
          } else {
            ownersLine = `<span class="ilus-cal-owner-line">${owners.length} retiros</span>`;
          }
        }

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
        return `<div class="${cls.join(' ')}" data-ilus-cal-idx="${i}" title="${safeTitle}" role="button" tabindex="${cls.indexOf('is-disabled') === -1 ? 0 : -1}">${badge}<span class="ilus-cal-hora">${_esc(hora)}</span>${ownersLine}</div>`;
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

      // Bind eventos
      grid.querySelectorAll('[data-ilus-cal-idx]').forEach(el => {
        el.addEventListener('click', () => onSlotClick(parseInt(el.dataset.ilusCalIdx, 10)));
        el.addEventListener('keydown', (e) => {
          if (e.key === 'Enter' || e.key === ' '){
            e.preventDefault();
            onSlotClick(parseInt(el.dataset.ilusCalIdx, 10));
          }
        });
      });
    }

    function onSlotClick(i){
      const s = state.slots[i];
      if (!s) return;
      const estado = _estadoDeSlot(s);
      if (estado === 'colacion'){
        const ls = (state.payload && state.payload.lunch_start) || cfg.lunchStartFallback;
        const le = (state.payload && state.payload.lunch_end) || cfg.lunchEndFallback;
        _toast(`Ese horario es de colación (${ls}-${le}). Elige antes o después.`, 'warning');
        return;
      }
      if (estado === 'completo'){ _toast('Ese bloque está lleno. Elige otro horario.', 'warning'); return; }
      if (estado === 'bloqueado'){ _toast(s.razon || 'Esa franja está bloqueada.', 'warning'); return; }

      if (state.startIdx === null){
        state.startIdx = state.endIdx = i;
      } else if (state.startIdx === i && state.endIdx === i){
        state.startIdx = state.endIdx = null;
      } else {
        const from = Math.min(state.startIdx, i);
        const to   = Math.max(state.startIdx, i);
        let invalido = null;
        for (let k = from; k <= to; k++){
          const ek = _estadoDeSlot(state.slots[k]);
          if (ek === 'colacion'){ invalido = 'colacion'; break; }
          if (ek === 'completo'){ invalido = 'completo'; break; }
          if (ek === 'bloqueado'){ invalido = 'bloqueado'; break; }
        }
        if (invalido === 'colacion'){
          const ls = (state.payload && state.payload.lunch_start) || cfg.lunchStartFallback;
          const le = (state.payload && state.payload.lunch_end) || cfg.lunchEndFallback;
          _toast(`El horario debe ser solo MAÑANA o solo TARDE — no puede cruzar la colación (${ls}-${le}).`, 'warning');
          state.startIdx = state.endIdx = i;
        } else if (invalido === 'completo'){
          _toast('El rango cruza un bloque lleno. Acórtalo o elige otra hora.', 'warning');
          state.startIdx = state.endIdx = i;
        } else if (invalido === 'bloqueado'){
          _toast('El rango cruza una franja bloqueada.', 'warning');
          state.startIdx = state.endIdx = i;
        } else {
          state.startIdx = from; state.endIdx = to;
        }
      }
      updateSummary();
      renderGrid();
    }

    function setQuickRange(hours){
      if (!hours || hours <= 0){ clearSelection(); return; }
      const slotMin = (state.payload && state.payload.slot_minutes) || cfg.slotMinFallback;
      const slotsNeeded = Math.max(1, Math.round(hours * 60 / slotMin));
      let foundStart = -1, foundEnd = -1;
      for (let i = 0; i < state.slots.length; i++){
        const s0 = state.slots[i];
        const puedeIni = (s0.puede_iniciar !== undefined) ? s0.puede_iniciar
                          : (s0.disponible && !s0.lunch);
        if (!puedeIni) continue;
        const endIdx = i + slotsNeeded - 1;
        if (endIdx >= state.slots.length) continue;
        let ok = true;
        for (let k = i; k <= endIdx; k++){
          const ek = _estadoDeSlot(state.slots[k]);
          if (ek === 'colacion' || ek === 'completo' || ek === 'bloqueado'){ ok = false; break; }
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
        }
        const r = await fetch(url, { credentials: 'same-origin', cache: 'no-store' });
        if (!r.ok) throw new Error('HTTP ' + r.status);
        state.payload = await r.json();
        if (dateInp){
          const tomorrow = new Date(Date.now() + 24*3600*1000);
          const tomorrowStr = tomorrow.toISOString().slice(0,10);
          dateInp.min = (state.payload.from < tomorrowStr) ? tomorrowStr : state.payload.from;
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
      if (dateInp && dateInp.value !== fecha) dateInp.value = fecha || '';

      if (!fecha){ renderEmpty('Selecciona una fecha primero.'); return; }
      // Defense in depth: no aceptar pasado/hoy
      const minDate = new Date(Date.now() + 24*3600*1000); minDate.setHours(0,0,0,0);
      if (new Date(fecha + 'T00:00:00') < minDate){
        renderEmpty('Necesitamos mínimo 24 horas de anticipación. Elige una fecha desde mañana.');
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
      // Auto-seleccionar duración sugerida (solo si no hay nada seleccionado)
      if (state.suggestedMin && state.suggestedMin > 0){
        const hours = Math.max(0.5, state.suggestedMin / 60);
        setQuickRange(hours);
      }
      updateSummary();
    }

    function suggestDurationMinutes(min){
      state.suggestedMin = Math.max(0, min || 0);
      // Re-aplicar inmediato si el día está cargado y NO hay selección activa
      if (state.payload && state.slots.length && state.startIdx === null){
        const hours = Math.max(0.5, state.suggestedMin / 60);
        setQuickRange(hours);
      }
    }

    // ── Bindings ────────────────────────────────────────────────────
    if (dateInp){
      dateInp.addEventListener('change', () => setDate(dateInp.value));
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
      else renderEmpty('Selecciona una fecha para ver los bloques disponibles.');
    }).catch(() => {});

    // API pública de la instancia
    return {
      setDate,
      setQuickRange,
      clearSelection,
      getSelection,
      suggestDurationMinutes,
      reload: () => {
        state.payload = null;
        renderSkeleton();
        return loadPayload(true).then(() => { if (state.currentDate) setDate(state.currentDate); });
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

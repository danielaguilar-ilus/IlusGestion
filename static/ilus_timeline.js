/* ═══════════════════════════════════════════════════════════════════════
   ilus_timeline.js — línea de tiempo de actividad COMPARTIDA
   (2026-09-26, Daniel: "aplica ese mismo componente/estilo... reutiliza un
   componente/parcial en vez de duplicar el HTML del timeline en cada
   pantalla"). Mismo lenguaje visual que "Actividad del ticket"
   (templates/tickets/ficha.html + static/tickets_ficha.js), sacado a un
   componente propio para que Incidencias y Repuestos lo usen sin copiar
   el markup del ticket (que trae lógica específica de mensajes/vistas).

   Uso:
     IlusTimeline.render(mountEl, eventos, {vacioTexto, vacioIcono});
   `eventos`: [{icono, color, bg, html, fecha}], más reciente PRIMERO.
     - icono: clase bootstrap-icons, ej. 'bi-eye-fill'
     - color/bg: colores del nodo circular (mismo criterio que tickets:
       ojo=vista, flecha=cambio de estado, sobre=correo, bandera=creación)
     - html: contenido YA escapado/armado por el caller (usar IlusTimeline.esc)
     - fecha: string YA formateado en hora Chile (REGLA #6) -- este
       componente no formatea fechas, solo las pinta
   ═══════════════════════════════════════════════════════════════════════ */
(function(global){
  'use strict';

  function esc(s){
    return String(s == null ? '' : s).replace(/[&<>"']/g, function(c){
      return ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'})[c];
    });
  }

  // Paleta de referencia (misma idea que ACT_DEF de tickets_ficha.js):
  // ojo=vista, flecha=cambio de estado, sobre=correo, bandera=creación.
  var ICONOS = {
    creacion:  {icono: 'bi-flag-fill',        color: '#0f172a', bg: '#e2e8f0'},
    vista:     {icono: 'bi-eye-fill',         color: '#3b82f6', bg: '#dbeafe'},
    cambio:    {icono: 'bi-arrow-repeat',     color: '#f97316', bg: '#fff7ed'},
    correo:    {icono: 'bi-envelope-fill',    color: '#8b5cf6', bg: '#f5f3ff'},
    eliminado: {icono: 'bi-trash-fill',       color: '#dc2626', bg: '#fee2e2'},
    foto:      {icono: 'bi-camera-fill',      color: '#16a34a', bg: '#dcfce7'},
    repuesto:  {icono: 'bi-tools',            color: '#b45309', bg: '#fff8e1'},
    default:   {icono: 'bi-clock-history',    color: '#6b7280', bg: '#f3f4f6'},
  };

  function itemHtml(ev){
    var def = ICONOS[ev.tipo] || ICONOS.default;
    var icono = ev.icono || def.icono, color = ev.color || def.color, bg = ev.bg || def.bg;
    return '<div class="ilus-tl-item">'
      + '<div class="ilus-tl-nodo" style="background:'+bg+';color:'+color+';"><i class="bi '+icono+'"></i></div>'
      + '<div class="ilus-tl-body"><div class="ilus-tl-linea">'
      + '<span class="ilus-tl-texto">'+(ev.html || '')+'</span>'
      + '<span class="ilus-tl-fecha">'+esc(ev.fecha || '')+'</span></div>'
      + (ev.extracto ? '<div class="ilus-tl-extracto">'+esc(ev.extracto)+'</div>' : '')
      + '</div></div>';
  }

  function render(mount, eventos, opts){
    if(!mount) return;
    opts = opts || {};
    if(!eventos || !eventos.length){
      mount.innerHTML = '<div class="ilus-tl-vacio"><i class="bi '+(opts.vacioIcono||'bi-clock-history')+'"></i>'
        + '<span>'+esc(opts.vacioTexto || 'Sin actividad registrada todavía.')+'</span></div>';
      return;
    }
    mount.className = (mount.className ? mount.className + ' ' : '') + 'ilus-tl';
    mount.innerHTML = eventos.map(itemHtml).join('');
  }

  global.IlusTimeline = {render: render, esc: esc, ICONOS: ICONOS};
})(window);

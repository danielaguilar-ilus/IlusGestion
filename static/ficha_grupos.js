/* ══════════════════════════════════════════════════════════════════════════════════════════════════════════
   FICHA DEL CLIENTE — SECCIONES (Daniel, 5-nov-2026: «ahorrar pestañas»)
   Agrupa las 15 pestañas de la ficha en 5 secciones SIN quitar ninguna (REGLA #4.2):
     · los botones ORIGINALES (.ftab-btn, con sus ids, contadores, onclick y listeners) se MUEVEN —no se clonan— al chip de su sección;
     · `switchTab(nombre)` sigue siendo la única puerta (aquí solo se envuelve para sincronizar la sección que se resalta);
     · cada sección recuerda su última pestaña; el atajo de una sección hace `click()` sobre el botón original, así corre el mismo
       `switchTab(...)` + `cargarX()` de siempre (carga perezosa intacta);
     · «Vista clásica» = la barra de 15 pestañas tal cual, en su orden original. 6-nov-2026 (Daniel: «no me gustaron las pestañas de los clientes… necesito todo
       ordenado como estaba antes, veo que ahora hay muy pocas»): **la clásica es la vista POR OMISIÓN**; las 5 secciones son opcionales («Vista nueva»,
       localStorage `ficha_vista_2` = 'nueva'). La clave subió de `ficha_vista` a `ficha_vista_2` a propósito: lo que alguien eligió con la vista nueva por
       omisión de la primera noche no cuenta.
   Si falta la estructura (ficha vieja en caché) o algo falla, no hace nada: la barra clásica queda como estaba.
   ══════════════════════════════════════════════════════════════════════════════════════════════════════════ */
(function () {
  'use strict';

  var KEY_VISTA = 'ficha_vista_2';        // 'clasica' | 'nueva' (por omisión, CLÁSICA: solo 'nueva' agrupa las pestañas)
  var KEY_SUB = 'ficha_sub_';             // + id de la sección: la última pestaña que se abrió en ella

  // Las 5 secciones y las pestañas que contiene (data-tab de los botones de la ficha). El orden de cada lista es el de los chips.
  var GRUPOS = [
    { id: 'resumen',      tabs: ['resumen', 'vida', 'intel'] },
    { id: 'equipos',      tabs: ['equipos', 'contrato', 'reportes'] },
    { id: 'servicio',     tabs: ['visitas', 'tickets', 'repuestos', 'evidencias'] },
    { id: 'comercial',    tabs: ['cotizaciones', 'finanzas', 'documentos'] },
    { id: 'comunicacion', tabs: ['comunicaciones', 'historial'] }
  ];

  var ORIGINAL = [];          // los botones en su orden de siempre (para la vista clásica)
  var listo = false;

  function lsGet(k) { try { return window.localStorage.getItem(k); } catch (e) { return null; } }
  function lsSet(k, v) { try { window.localStorage.setItem(k, v); } catch (e) { /* sin almacenamiento: la ficha funciona igual */ } }

  function vistaActual() { return lsGet(KEY_VISTA) === 'nueva' ? 'nueva' : 'clasica'; }

  function grupoDe(tab) {
    for (var i = 0; i < GRUPOS.length; i++) {
      if (GRUPOS[i].tabs.indexOf(tab) >= 0) return GRUPOS[i].id;
    }
    return null;
  }

  function botonDe(tab) { return document.querySelector('.ftab-btn[data-tab="' + tab + '"]'); }

  function tabActiva() {
    var b = document.querySelector('.ftab-btn.active');
    return b ? b.getAttribute('data-tab') : 'resumen';
  }

  /* Una pestaña nueva que alguien agregue a la ficha y no esté en la tabla de arriba NO se pierde: va a una sección «Más». */
  function asegurarSeccionMas(nav, subs) {
    var sueltas = ORIGINAL.filter(function (b) { return !grupoDe(b.getAttribute('data-tab')); });
    if (!sueltas.length || document.getElementById('fgBtn-mas')) return;
    GRUPOS.push({ id: 'mas', tabs: sueltas.map(function (b) { return b.getAttribute('data-tab'); }) });
    var btn = document.createElement('button');
    btn.type = 'button'; btn.className = 'fg-btn'; btn.id = 'fgBtn-mas'; btn.setAttribute('data-grupo', 'mas');
    btn.setAttribute('role', 'tab'); btn.setAttribute('aria-selected', 'false');
    btn.innerHTML = '<i class="bi bi-three-dots" aria-hidden="true"></i><span>Más</span>';
    nav.appendChild(btn);
    var sub = document.createElement('div');
    sub.className = 'fg-sub'; sub.setAttribute('data-grupo', 'mas'); sub.setAttribute('role', 'tablist');
    subs.appendChild(sub);
  }

  function aplicarVista(v) {
    var barra = document.querySelector('.ficha-tabs');
    var html = document.documentElement;
    var etiqueta = document.getElementById('fgVista');
    if (!barra) return;
    if (v === 'nueva') {
      html.classList.add('fg-nueva');
      GRUPOS.forEach(function (g) {
        var cont = document.querySelector('.fg-sub[data-grupo="' + g.id + '"]');
        if (!cont) return;
        g.tabs.forEach(function (t) { var b = botonDe(t); if (b) cont.appendChild(b); });
      });
    } else {
      html.classList.remove('fg-nueva');
      ORIGINAL.forEach(function (b) { barra.appendChild(b); });          // en el orden de siempre
    }
    if (etiqueta) {
      etiqueta.setAttribute('data-vista', v);
      etiqueta.innerHTML = v === 'nueva'
        ? '<i class="bi bi-layout-text-sidebar" aria-hidden="true"></i><span>Vista clásica</span>'
        : '<i class="bi bi-grid-1x2" aria-hidden="true"></i><span>Vista nueva</span>';
      etiqueta.setAttribute('title', v === 'nueva' ? 'Volver a la barra de 15 pestañas de siempre' : 'Agrupar las pestañas en 5 secciones');
    }
  }

  /* Resalta la sección de la pestaña activa y muestra SOLO sus chips. */
  function sincronizar(tab) {
    if (!listo) return;
    var g = grupoDe(tab) || (document.getElementById('fgBtn-mas') ? 'mas' : null);
    if (!g) return;
    lsSet(KEY_SUB + g, tab);
    document.querySelectorAll('.fg-btn').forEach(function (b) {
      var on = b.getAttribute('data-grupo') === g;
      b.classList.toggle('on', on);
      b.setAttribute('aria-selected', on ? 'true' : 'false');
      b.setAttribute('tabindex', on ? '0' : '-1');
    });
    document.querySelectorAll('.fg-sub').forEach(function (s) {
      s.classList.toggle('on', s.getAttribute('data-grupo') === g);
    });
  }

  /* El atajo de una sección abre su última pestaña (o la primera) con el click del botón ORIGINAL: corre el mismo switchTab + cargarX(). */
  function abrirSeccion(id) {
    var g = GRUPOS.filter(function (x) { return x.id === id; })[0];
    if (!g) return;
    var recordada = lsGet(KEY_SUB + id);
    var destino = (recordada && g.tabs.indexOf(recordada) >= 0 && botonDe(recordada)) ? recordada : g.tabs.filter(botonDe)[0];
    var btn = destino ? botonDe(destino) : null;
    if (btn) btn.click();
  }

  function moverFoco(delta) {
    var botones = [].slice.call(document.querySelectorAll('.fg-btn'));
    var i = botones.indexOf(document.activeElement);
    if (i < 0) return;
    var sig = botones[(i + delta + botones.length) % botones.length];
    sig.focus();
  }

  function init() {
    var nav = document.getElementById('fgNav');
    var subs = document.getElementById('fgSubs');
    var barra = document.querySelector('.ficha-tabs');
    if (!nav || !subs || !barra) return;                       // ficha sin la estructura: la barra clásica sigue como estaba
    ORIGINAL = [].slice.call(barra.querySelectorAll('.ftab-btn'));
    if (!ORIGINAL.length) return;
    asegurarSeccionMas(nav, subs);

    // switchTab sigue siendo la única puerta: se envuelve (no se reemplaza) para sincronizar la sección que se resalta.
    if (typeof window.switchTab === 'function' && !window.switchTab.__fg) {
      var original = window.switchTab;
      var envuelta = function (name) {
        var r = original.apply(this, arguments);
        try { sincronizar(name); } catch (e) { /* solo cosmético */ }
        return r;
      };
      envuelta.__fg = true;
      window.switchTab = envuelta;
    }

    nav.addEventListener('click', function (ev) {
      var b = ev.target.closest ? ev.target.closest('.fg-btn') : null;
      if (b) abrirSeccion(b.getAttribute('data-grupo'));
    });
    nav.addEventListener('keydown', function (ev) {
      if (ev.key === 'ArrowRight') { ev.preventDefault(); moverFoco(1); }
      else if (ev.key === 'ArrowLeft') { ev.preventDefault(); moverFoco(-1); }
    });
    var vista = document.getElementById('fgVista');
    if (vista) {
      vista.addEventListener('click', function () {
        // La vista de AHORA se lee de la página, no del almacenamiento: sin almacenamiento (modo privado) el botón igual alterna en los dos sentidos.
        var nueva = document.documentElement.classList.contains('fg-nueva') ? 'clasica' : 'nueva';
        lsSet(KEY_VISTA, nueva);
        aplicarVista(nueva);
        sincronizar(tabActiva());
      });
    }

    aplicarVista(vistaActual());
    listo = true;
    document.documentElement.classList.add('fg-listo');
    sincronizar(tabActiva());
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();

  // Para las pruebas y para quien necesite saber a qué sección pertenece una pestaña (solo lectura).
  window.FichaGrupos = { grupos: GRUPOS, grupoDe: grupoDe, vista: vistaActual };
})();

/* 📊 Costeo de órdenes de trabajo — comportamiento mínimo de la pantalla (Daniel 2026-10-25).
   Todo el contenido llega ya dibujado por el servidor; esto solo agrega:
     1. Paginación de cada lista [data-gc-pag] (REGLA #4.3): «Mostrando A–B de N», N por página a elección del usuario,
        Anterior / Página X de Y / Siguiente. Sin JS se ven todas las filas.
     2. Filtros de la trazabilidad (chips con data-gc-filtro): todas · cobertura · perdida · sin autorización.
     3. Tarjeta clicable → la OT (el enlace con el número sigue siendo el camino accesible).
   Al limpiar un filtro la lista se vuelve a calcular (REGLA #4.3, punto 4). Sin alert/confirm/prompt nativos (REGLA #1). */
(function () {
  'use strict';
  var TAMANOS = [5, 8, 10, 25, 50];

  function items(cont) { return Array.prototype.slice.call(cont.querySelectorAll(':scope > .gc-item')); }

  function Paginador(cont) {
    this.cont = cont;
    this.por = parseInt(cont.getAttribute('data-por'), 10) || 10;
    this.pag = 1;
    this.filtro = 'todas';
    this.pie = document.createElement('div');
    this.pie.className = 'gc-pag';
    this.pie.setAttribute('data-gc-pie', '1');
    cont.parentNode.insertBefore(this.pie, cont.nextSibling);
    this.render();
  }

  Paginador.prototype.visibles = function () {
    var f = this.filtro;
    return items(this.cont).filter(function (el) {
      if (f === 'todas') return true;
      if (f === 'sinaut') return el.getAttribute('data-sinaut') === '1';
      return el.getAttribute('data-tipo') === f;
    });
  };

  Paginador.prototype.render = function () {
    var self = this;
    var todos = items(this.cont);
    var vis = this.visibles();
    var n = vis.length;
    var paginas = Math.max(1, Math.ceil(n / this.por));
    if (this.pag > paginas) this.pag = paginas;
    var ini = (this.pag - 1) * this.por;
    var fin = Math.min(n, ini + this.por);
    todos.forEach(function (el) { el.hidden = true; });
    vis.slice(ini, fin).forEach(function (el) { el.hidden = false; });
    var opciones = TAMANOS.map(function (t) {
      return '<option value="' + t + '"' + (t === self.por ? ' selected' : '') + '>' + t + '</option>';
    }).join('');
    var texto = n ? 'Mostrando ' + (ini + 1) + '–' + fin + ' de ' + n : 'Sin resultados con este filtro';
    this.pie.innerHTML =
      '<span>' + texto + '</span>' +
      '<label>Por página <select aria-label="Filas por página">' + opciones + '</select></label>' +
      '<span class="gc-pag-btns"><button type="button" data-acc="ant"' + (this.pag <= 1 ? ' disabled' : '') + '>Anterior</button>' +
      '<span>Página ' + this.pag + ' de ' + paginas + '</span>' +
      '<button type="button" data-acc="sig"' + (this.pag >= paginas ? ' disabled' : '') + '>Siguiente</button></span>';
    this.pie.querySelector('select').onchange = function (e) {
      self.por = parseInt(e.target.value, 10) || 10;
      self.pag = 1;
      self.render();
    };
    Array.prototype.forEach.call(this.pie.querySelectorAll('button'), function (b) {
      b.onclick = function () {
        self.pag += b.getAttribute('data-acc') === 'sig' ? 1 : -1;
        self.render();
      };
    });
  };

  Paginador.prototype.filtrar = function (clave) {
    this.filtro = clave || 'todas';
    this.pag = 1;
    this.render();
  };

  function iniciar() {
    var pags = {};
    Array.prototype.forEach.call(document.querySelectorAll('[data-gc-pag]'), function (cont) {
      pags[cont.id || ('p' + Object.keys(pags).length)] = new Paginador(cont);
    });
    Array.prototype.forEach.call(document.querySelectorAll('[data-gc-filtro]'), function (chip) {
      chip.addEventListener('click', function () {
        var destino = chip.getAttribute('data-gc-destino');
        var p = pags[destino];
        if (!p) return;
        Array.prototype.forEach.call(document.querySelectorAll('[data-gc-filtro][data-gc-destino="' + destino + '"]'), function (c) {
          c.classList.toggle('on', c === chip);
        });
        p.filtrar(chip.getAttribute('data-gc-filtro'));
      });
    });
    // Tarjeta clicable: lleva a la OT salvo que el clic sea sobre un enlace, botón o desplegable.
    document.addEventListener('click', function (e) {
      if (e.target.closest('a, button, summary, details, select, input, label')) return;
      var tarjeta = e.target.closest('[data-href]');
      if (tarjeta && tarjeta.getAttribute('data-href')) window.location.href = tarjeta.getAttribute('data-href');
    });
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', iniciar);
  else iniciar();
})();

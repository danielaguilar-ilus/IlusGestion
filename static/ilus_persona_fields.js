/* ILUS — helpers de datos personales: RUT chileno y fecha de nacimiento.
   Sin dependencias del DOM (se prueban en Node desde
   tests/test_usuarios_datos_personales.py). La regla espejo del servidor vive
   en usuarios_perfil.py y en validar_rut() de app.py: si cambias una, cambia la otra. */
(function (global) {
  'use strict';

  var EDAD_MINIMA = 15;
  var EDAD_MAXIMA = 100;

  // Deja solo dígitos y K. La K solo vale como último carácter (es el dígito verificador).
  function rutLimpiar(v) {
    var s = String(v == null ? '' : v).toUpperCase().replace(/[^0-9K]/g, '');
    var conK = s.length > 0 && s.charAt(s.length - 1) === 'K';
    var out = s.replace(/K/g, '');
    if (conK && out.length) out += 'K';
    return out.slice(0, 9);
  }

  // Dígito verificador (módulo 11) de un cuerpo de solo dígitos.
  function rutDV(cuerpo) {
    var suma = 0, mult = 2;
    for (var i = cuerpo.length - 1; i >= 0; i--) {
      suma += parseInt(cuerpo.charAt(i), 10) * mult;
      mult = mult === 7 ? 2 : mult + 1;
    }
    var r = 11 - (suma % 11);
    return r === 11 ? '0' : (r === 10 ? 'K' : String(r));
  }

  // '255470655' → '25.547.065-5'
  function rutFormatear(v) {
    var c = rutLimpiar(v);
    if (c.length < 2) return c;
    var cuerpo = c.slice(0, -1), dv = c.slice(-1);
    return cuerpo.replace(/\B(?=(\d{3})+(?!\d))/g, '.') + '-' + dv;
  }

  // estado: 'vacio' | 'incompleto' | 'invalido' | 'valido'
  function rutEstado(v) {
    var c = rutLimpiar(v);
    if (!c) return { estado: 'vacio', limpio: '' };
    if (c.length < 8) return { estado: 'incompleto', limpio: c };
    var cuerpo = c.slice(0, -1), dv = c.slice(-1);
    if (!/^\d+$/.test(cuerpo)) return { estado: 'invalido', limpio: c, dvEsperado: null };
    var esperado = rutDV(cuerpo);
    if (dv !== esperado) return { estado: 'invalido', limpio: c, dvEsperado: esperado };
    return { estado: 'valido', limpio: c, formateado: rutFormatear(c) };
  }

  function edadEn(nacimiento, hoy) {
    var e = hoy.getFullYear() - nacimiento.getFullYear();
    var mm = hoy.getMonth() - nacimiento.getMonth();
    if (mm < 0 || (mm === 0 && hoy.getDate() < nacimiento.getDate())) e--;
    return e;
  }

  // iso = 'AAAA-MM-DD' (lo que entrega <input type="date">).
  // estado: 'vacio' | 'invalido' | 'valido'
  function fechaNacEstado(iso, hoy) {
    var txt = String(iso == null ? '' : iso).trim();
    if (!txt) return { estado: 'vacio' };
    var m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(txt);
    if (!m) return { estado: 'invalido', mensaje: 'Escribe día, mes y año.' };
    var y = +m[1], mo = +m[2], d = +m[3];
    var f = new Date(y, mo - 1, d);
    if (f.getFullYear() !== y || f.getMonth() !== mo - 1 || f.getDate() !== d) {
      return { estado: 'invalido', mensaje: 'Esa fecha no existe. Revisa el día y el mes.' };
    }
    var h0 = hoy || new Date();
    var h = new Date(h0.getFullYear(), h0.getMonth(), h0.getDate());
    if (f > h) return { estado: 'invalido', mensaje: 'La fecha de nacimiento no puede ser futura.' };
    var edad = edadEn(f, h);
    if (edad < EDAD_MINIMA) return { estado: 'invalido', mensaje: 'La persona tendría menos de ' + EDAD_MINIMA + ' años. Revisa el año.' };
    if (edad > EDAD_MAXIMA) return { estado: 'invalido', mensaje: 'La persona tendría más de ' + EDAD_MAXIMA + ' años. Revisa el año.' };
    return { estado: 'valido', edad: edad };
  }

  global.ilusPersona = {
    EDAD_MINIMA: EDAD_MINIMA,
    EDAD_MAXIMA: EDAD_MAXIMA,
    rutLimpiar: rutLimpiar,
    rutDV: rutDV,
    rutFormatear: rutFormatear,
    rutEstado: rutEstado,
    fechaNacEstado: fechaNacEstado
  };
})(typeof window !== 'undefined' ? window : globalThis);

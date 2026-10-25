# -*- coding: utf-8 -*-
"""2026-10-24 · REGLA #42 (Daniel: «pasan de rojo a verde… en el proyecto entero, cuando la tarjeta apruebe los caracteres, tiene que pasar de
rojo a verde»): `ilusPrompt` —el reemplazo de prompt() que usa todo el proyecto (50 archivos)— pinta el campo EN VIVO cuando se le pasa `minLength`
o `validate`: rojo con su motivo mientras no cumple, verde apenas cumple, sin color si está vacío. Aceptar sigue validando y bloqueando igual.
"""
import json
import os
import re
import shutil
import subprocess
import tempfile
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
with open(os.path.join(RAIZ, "static", "ilus_ui.js"), encoding="utf-8", newline="") as _fh:
    JS = _fh.read().replace("\r\n", "\n")
with open(os.path.join(RAIZ, "CLAUDE.md"), encoding="utf-8", newline="") as _fh:
    REGLAS = _fh.read().replace("\r\n", "\n")


def _prompt():
    i = JS.index("function ilusPrompt(opts = {}){")
    return JS[i:JS.index("// ── ilusActionSheet", i)]


class TestIlusPromptEnVivo(unittest.TestCase):
    def test_lee_minlength_y_validate(self):
        f = _prompt()
        self.assertIn("typeof opts.minLength === 'number' && opts.minLength > 0", f)
        self.assertIn("const _validate = typeof opts.validate === 'function' ? opts.validate : null;", f)
        self.assertIn("if (_minLen || _validate){", f)

    def test_pinta_rojo_verde_y_dice_el_motivo(self):
        f = _prompt()
        for pista in ("inputEl.classList.toggle('ilus-vc-ok', ok);", "inputEl.classList.toggle('ilus-vc-bad', !ok);",
                      "'Faltan ' + f + ' caracter'", "' (mínimo ' + _minLen + ').'", "ok ? 'Listo.' : msg",
                      "if (r && r.ok === false){ ok = false; msg = r.error || 'El valor no es válido.'; }"):
            self.assertIn(pista, f)
        # Vacío = sin color (lo cubre «obligatorio» al aceptar): nunca nace en rojo.
        self.assertIn("if (!len){", f)
        self.assertIn("inputEl.classList.remove('ilus-vc-ok', 'ilus-vc-bad');", f)

    def test_se_actualiza_al_escribir_y_despues_de_sanitize(self):
        f = _prompt()
        self.assertIn("inputEl.addEventListener('input', () => { _limpiarError(); _vcPintar(); });", f)
        # El sanitize (bloquea letras en un teléfono, etc.) corre ANTES: el color se calcula sobre el valor ya limpio.
        self.assertLess(f.index("if (_sanitize){"), f.index("if (_minLen || _validate){"))

    def test_aceptar_sigue_bloqueando_y_solo_hay_un_mensaje_bajo_el_campo(self):
        f = _prompt()
        self.assertIn("val.trim().length < _minLen", f)
        self.assertIn("'Escribe al menos ' + _minLen + ' caracteres.'", f)
        i = f.index("function _mostrarError(msg){")
        self.assertIn("if (_vcEl){ _vcEl.remove(); _vcEl = null; }", f[i:i + 400])

    def test_estilos(self):
        for pista in (".ilus-prompt-input.ilus-vc-bad", ".ilus-prompt-input.ilus-vc-ok", ".ilus-vc-msg.ok{color:#15803d}", ".ilus-vc-msg.bad{color:#b91c1c}"):
            self.assertIn(pista, JS)

    def test_los_prompts_de_siempre_no_cambian(self):
        f = _prompt()
        # Sin minLength ni validate no se engancha nada nuevo.
        self.assertIn("if (_minLen || _validate){", f)
        self.assertIn("'Este campo es obligatorio.'", f)
        self.assertIn("if (e.key === 'Enter' && !multiline){ e.preventDefault(); done(true); }", f)

    def test_la_regla_lo_dice(self):
        self.assertIn("REGLA #42", REGLAS)
        self.assertRegex(REGLAS, r"ilusPrompt\(\{[^}]*minLength")


def _generico():
    i = JS.index("//  🚦 REGLA #42 (Daniel 2026-10-24: «pasan de rojo a verde… en el proyecto entero")
    return JS[i:JS.index("//  COMPARADOR DE RUT CHILENO", i)]


class TestCamposConMinimoEnTodoElProyecto(unittest.TestCase):
    """Los campos que ya existen (placeholder «(mínimo N caracteres)» o data-min) pasan de rojo a verde solos."""

    def test_el_minimo_sale_de_data_min_o_del_placeholder(self):
        g = _generico()
        self.assertIn("el.getAttribute('data-min')", g)
        self.assertIn("/m[ií]nimo\\s+(\\d+)\\s+caracteres/i.exec(el.getAttribute('placeholder') || '')", g)

    def test_pinta_en_vivo_y_nunca_bloquea(self):
        g = _generico()
        for pista in ("el.classList.toggle('ilus-vc-ok', len > 0 && ok);", "el.classList.toggle('ilus-vc-bad', len > 0 && !ok);",
                      "el.setAttribute('aria-invalid'", "document.addEventListener('input', function(e){ pintar(e.target); }, true);",
                      "document.addEventListener('focusin'"):
            self.assertIn(pista, g)
        for prohibido in ("preventDefault", "stopPropagation", "disabled =", ".disabled=", "setCustomValidity"):
            self.assertNotIn(prohibido, g)               # solo informa

    def test_no_se_mete_donde_ya_hay_otro_motor(self):
        g = _generico()
        self.assertIn("el.classList.contains('ilus-prompt-input')", g)
        self.assertIn("el.closest('#ot2ModalCrear')", g)
        self.assertIn("if (el.disabled || el.readOnly) return false;", g)

    def test_no_rompe_filas_flex_ni_input_group(self):
        g = _generico()
        self.assertIn("/flex|grid/.test(disp)", g)
        self.assertIn("padre.classList.contains('input-group')", g)

    def test_vacio_sin_color_ni_mensaje(self):
        g = _generico()
        self.assertIn("if (!len){ if (m) m.remove(); return; }", g)

    def test_el_estilo_se_inyecta_una_sola_vez(self):
        g = _generico()
        self.assertIn("if (document.getElementById('__ilus_vc_css')) return;", g)
        self.assertIn("textarea.ilus-vc-bad,input.ilus-vc-bad{border-color:#dc2626 !important", g)
        self.assertIn("textarea.ilus-vc-ok,input.ilus-vc-ok{border-color:#16a34a !important", g)

    def test_la_regla_lo_dice(self):
        self.assertIn("placeholder diga «(mínimo N caracteres)»", REGLAS)


NODE = shutil.which("node")


def _node(guion, *args):
    with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False, encoding="utf-8") as f:
        f.write(guion)
        ruta = f.name
    try:
        r = subprocess.run([NODE, ruta] + list(args), capture_output=True, text=True, encoding="utf-8", timeout=60)
    finally:
        os.unlink(ruta)
    assert r.returncode == 0, r.stderr[-1500:]
    return json.loads(r.stdout.strip().splitlines()[-1])


@unittest.skipIf(not NODE, "Node no está instalado")
class TestContratoDeValidate(unittest.TestCase):
    """Revisión independiente (24-oct): un `validate` que devuelve un TEXTO de error (o null si está bien), como los formularios más viejos,
    se leía como «válido»: el campo se pintaba de verde («Listo.») aunque el dato estuviera mal y Aceptar tampoco lo frenaba."""

    @classmethod
    def setUpClass(cls):
        f = _prompt()
        i = f.index("const _lee = (r) => {")
        cuerpo = f[i:f.index("\n      };", i) + len("\n      };")]
        cls.o = _node(cuerpo + r"""
const casos = { falso: false, texto: 'Escribe al menos 10 caracteres.', vacio: '', espacios: '   ', nulo: null, indef: undefined, cierto: true,
  malo: { ok: false, error: 'No coincide' }, bueno: { ok: true, valor: 'X' }, sinOk: { error: 'x' } };
const out = {}; Object.keys(casos).forEach(function (k) { out[k] = _lee(casos[k]); });
console.log(JSON.stringify(out));
""")

    def test_un_texto_es_un_error(self):
        self.assertEqual(self.o["texto"], {"ok": False, "error": "Escribe al menos 10 caracteres."})
        self.assertEqual(self.o["falso"], {"ok": False})

    def test_nulo_vacio_y_cierto_significan_que_esta_bien(self):
        for k in ("vacio", "espacios", "nulo", "indef", "cierto"):
            self.assertEqual(self.o[k], {"ok": True}, k)

    def test_el_contrato_oficial_pasa_igual(self):
        self.assertEqual(self.o["malo"], {"ok": False, "error": "No coincide"})
        self.assertEqual(self.o["bueno"], {"ok": True, "valor": "X"})
        self.assertEqual(self.o["sinOk"], {"error": "x"})          # solo `ok === false` es un fallo

    def test_el_pintado_en_vivo_y_aceptar_usan_la_misma_lectura(self):
        f = _prompt()
        self.assertIn("r = _lee(_validate(v));", f)
        self.assertIn("const r = _lee(_validate(val));", f)
        self.assertNotIn("const r = _validate(val);", f)

    def test_los_dos_validadores_que_devolvian_un_texto_ya_usan_el_contrato(self):
        for ruta, error in ((("templates", "ot2", "detalle.html"), "Escribe al menos 10 caracteres."),
                            (("templates", "ot2", "_modal_crear.html"), "Ese RUT no es válido")):
            with open(os.path.join(RAIZ, *ruta), encoding="utf-8", newline="") as fh:
                src = fh.read()
            i = src.index("{ok: false, error: '" + error)
            self.assertIn("? {ok: true} : {ok: false, error: '" + error, src[i - 140:i + 80], ruta)
            self.assertNotIn("? null : '" + error, src, ruta)


@unittest.skipIf(not NODE, "Node no está instalado")
class TestPintadoGenericoAlAbrirUnModal(unittest.TestCase):
    """Revisión independiente (24-oct): un campo que el código vacía con `.value = ''` reabría con el borde verde y «Listo.» del texto anterior."""

    GUION = r"""
const fs = require('fs'), vm = require('vm');
const src = fs.readFileSync(process.argv[2], 'utf8').replace(/\r\n/g, '\n');
const i = src.indexOf("(function(){\n    if (document.getElementById('__ilus_vc_css')) return;");
if (i < 0) throw new Error('no se encontró el pintor genérico');
const j = src.indexOf('// ════', i + 10);
const oyentes = {};
function Campo(min) {
  const self = this; this.nodeType = 1; this.value = ''; this.disabled = false; this.readOnly = false; this.nextElementSibling = null;
  this._a = { 'data-min': String(min) }; this._c = new Set(); this.parentElement = { classList: { contains: function () { return false; } } };
  this.matches = function () { return true; }; this.closest = function () { return null; };
  this.getAttribute = function (k) { return self._a[k] == null ? null : self._a[k]; }; this.setAttribute = function (k, v) { self._a[k] = String(v); };
  this.classList = { toggle: function (c, f) { if (f) self._c.add(c); else self._c.delete(c); return f; }, contains: function (c) { return self._c.has(c); } };
  this.insertAdjacentElement = function (pos, el) { self.nextElementSibling = el; el._duenio = self; };
}
global.document = {
  head: { appendChild: function () {} }, documentElement: { appendChild: function () {} },
  getElementById: function () { return null; },
  createElement: function (t) {
    const e = { className: '', _h: '', lastChild: { textContent: '' }, classList: { contains: function (c) { return String(e.className).split(' ').indexOf(c) >= 0; } },
      remove: function () { if (e._duenio) e._duenio.nextElementSibling = null; } };
    Object.defineProperty(e, 'innerHTML', { get: function () { return e._h; }, set: function (v) { e._h = v; e.lastChild = { textContent: '' }; } });
    return e;
  },
  addEventListener: function (t, fn) { oyentes[t] = fn; },
};
global.getComputedStyle = function () { return { display: 'block' }; };
vm.runInThisContext(src.slice(i, j));
const a = new Campo(5), b = new Campo(5);
const escribir = function (c, v) { c.value = v; oyentes.input({ target: c }); };
const ver = function (c) { return { ok: c.classList.contains('ilus-vc-ok'), bad: c.classList.contains('ilus-vc-bad'), msg: c.nextElementSibling ? c.nextElementSibling.lastChild.textContent : null }; };
const out = {};
escribir(a, 'ab'); escribir(b, 'abcdefg');
out.antes = { a: ver(a), b: ver(b) };
// El código vacía el campo `a` (como al reabrir un formulario) y abre el modal
a.value = '';
const host = { querySelectorAll: function () { return [a, b].filter(function (c) { return c.classList.contains('ilus-vc-ok') || c.classList.contains('ilus-vc-bad'); }); } };
out.oyentes = Object.keys(oyentes).sort();
oyentes['show.bs.modal']({ target: host });
out.despues = { a: ver(a), b: ver(b) };
// Si lo vacía recién cuando ya terminó de abrirse, también se corrige
escribir(a, 'xy'); a.value = '';
oyentes['shown.bs.modal']({ target: host });
out.alTerminar = ver(a);
console.log(JSON.stringify(out));
"""

    @classmethod
    def setUpClass(cls):
        cls.o = _node(cls.GUION, os.path.join(RAIZ, "static", "ilus_ui.js"))

    def test_los_oyentes_existen(self):
        self.assertEqual(self.o["oyentes"], ["focusin", "input", "show.bs.modal", "shown.bs.modal"])

    def test_antes_de_abrir_el_modal_cada_campo_tiene_su_color(self):
        self.assertEqual(self.o["antes"]["a"]["bad"], True)
        self.assertIn("Faltan 3 caracteres", self.o["antes"]["a"]["msg"])
        self.assertEqual(self.o["antes"]["b"]["ok"], True)
        self.assertEqual(self.o["antes"]["b"]["msg"], "Listo.")

    def test_un_campo_vaciado_por_codigo_vuelve_a_quedar_sin_color_ni_mensaje(self):
        self.assertEqual(self.o["despues"]["a"], {"ok": False, "bad": False, "msg": None})
        self.assertEqual(self.o["alTerminar"], {"ok": False, "bad": False, "msg": None})

    def test_un_campo_con_texto_no_cambia(self):
        self.assertEqual(self.o["despues"]["b"], self.o["antes"]["b"])


if __name__ == "__main__":
    unittest.main()

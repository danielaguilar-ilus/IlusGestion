# -*- coding: utf-8 -*-
"""2026-10-24 · REGLA #42 (Daniel: «pasan de rojo a verde… en el proyecto entero, cuando la tarjeta apruebe los caracteres, tiene que pasar de
rojo a verde»): `ilusPrompt` —el reemplazo de prompt() que usa todo el proyecto (50 archivos)— pinta el campo EN VIVO cuando se le pasa `minLength`
o `validate`: rojo con su motivo mientras no cumple, verde apenas cumple, sin color si está vacío. Aceptar sigue validando y bloqueando igual.
"""
import os
import re
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


if __name__ == "__main__":
    unittest.main()

"""La ficha de ticket no usa curvas con rebote (Daniel 04-nov-2026, «Sí, corrígelos»; Impeccable: bounce-easing).
`--ease-spring: cubic-bezier(.34,1.56,.64,1)` se pasaba del valor final (el aviso del micrófono crecía hasta 1,0078 y las
tarjetas que bajan se hundían hasta 0,39 px antes de asentarse) y la usaban 21 reglas. Todo pasó a `--ease-out`
(cubic-bezier(.22,1,.36,1), la curva de Retiros) y el token se borró.

Ojo con borrar un token: si una regla sigue haciendo `var(--token)` sin que exista, el navegador descarta TODA la
declaración (en `animation:` la animación deja de correr sin ningún error). Por eso se vigila también que cada variable
usada en un `animation:` o `transition:` esté definida."""
import os
import re

_STATIC = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "static")
_ARCHIVOS = ("tickets_ficha.css", "tickets_ficha_v2.css")


def _css(nombre):
    with open(os.path.join(_STATIC, nombre), encoding="utf-8") as fh:
        return re.sub(r"/\*.*?\*/", "", fh.read(), flags=re.S)


def test_no_queda_el_token_de_rebote():
    for nombre in _ARCHIVOS:
        assert "ease-spring" not in _css(nombre), nombre + ": sigue usando --ease-spring (curva con rebote)"


def test_ninguna_curva_se_pasa_del_valor_final():
    """En cubic-bezier(x1,y1,x2,y2) un y1 o y2 fuera de 0..1 hace que la animación se pase y vuelva (rebote)."""
    for nombre in _ARCHIVOS:
        for m in re.finditer(r"cubic-bezier\(([^)]*)\)", _css(nombre)):
            v = [float(x) for x in m.group(1).split(",")]
            assert len(v) == 4, m.group(0)
            assert 0 <= v[1] <= 1 and 0 <= v[3] <= 1, "%s: %s se pasa de 0..1 (rebote)" % (nombre, m.group(0))


def test_animaciones_y_transiciones_usan_variables_que_existen():
    css = _css("tickets_ficha.css")
    definidas = set(re.findall(r"(--[\w-]+)\s*:", css))
    for m in re.finditer(r"\b(animation|transition)\s*:([^;}]*)", css):
        for var in re.findall(r"var\(\s*(--[\w-]+)", m.group(2)):
            assert var in definidas, "%s: usa %s y no está definida: el navegador descartaría la declaración" % (m.group(0)[:60], var)


def test_las_cuatro_entradas_siguen_teniendo_curva():
    """El aviso del micrófono, los campos vacíos, el detalle del calendario y la alerta de choque siguen animándose."""
    css = _css("tickets_ficha.css")
    for selector, nombre in ((".rp-mic-ok.show", "rpMicOkIn"), (".tk-id-vacios-wrap.show", "tkIdVaciosIn"),
                             (".tkcal-detalle", "tkcalDetIn"), (".tkcal-choque-alert", "tkcalDetIn")):
        m = re.search(r"(?:^|\})\s*" + re.escape(selector) + r"\s*\{([^}]*)\}", css)
        assert m, selector + " no existe"
        assert re.search(r"animation\s*:\s*" + nombre + r"\s+[\d.]+s\s+var\(--ease-out\)", m.group(1)), \
            "%s: su animación %s debe usar var(--ease-out)" % (selector, nombre)


def test_ease_out_esta_definido_en_root():
    assert re.search(r":root\s*\{[^}]*--ease-out\s*:\s*cubic-bezier\(", _css("tickets_ficha.css"))

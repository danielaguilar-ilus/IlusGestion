"""«Recorrido del ticket» más compacto (Daniel 03-nov-2026: «me cuesta más darle scroll»).
Medido con los estilos reales y datos de mentira: 224 px -> 126 px a 1280 px de ancho (236 -> ~130 en la ficha real).
Se editaron las reglas ORIGINALES del recorrido (sin una capa de anulación encima): estas pruebas cuidan eso."""
import os
import re

_STATIC = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "static")


def _css(nombre):
    with open(os.path.join(_STATIC, nombre), encoding="utf-8") as fh:
        return fh.read()


def _px(texto):
    return float(texto)


def test_la_linea_que_une_los_pasos_sigue_centrada_en_el_circulo():
    css = _css("tickets_ficha.css")
    ancho = _px(re.search(r"\.tk-step-circle\{width:([\d.]+)px;height:\1px", css).group(1))
    esperado = (ancho - 3) / 2          # la línea mide 3 px: su centro es el centro del círculo
    arriba = _px(re.search(r"\.tk-step:not\(:first-child\)::before\{content:'';position:absolute;top:([\d.]+)px", css).group(1))
    izquierda = _px(re.search(r"\.tk-step:not\(:first-child\)::before\{top:-6px;left:([\d.]+)px;width:3px", css).group(1))
    assert arriba == esperado, "la línea horizontal quedó descentrada respecto del círculo"
    assert izquierda == esperado, "la línea vertical (celular) quedó descentrada respecto del círculo"


def test_hora_y_quien_van_en_la_misma_linea_y_nada_se_quita():
    v2 = _css("tickets_ficha_v2.css")
    # grilla de dos columnas: la hora y quién la marcó comparten línea; la etiqueta y el tiempo entre pasos ocupan todo el ancho
    assert re.search(r"\.tk-step-info\{display:grid;grid-template-columns:auto auto;", v2)
    assert ".tk-step-info .tk-step-label{grid-column:1 / -1;" in v2
    assert re.search(r"\.tk-step-dur\{grid-column:1 / -1;", v2)
    # los cuatro datos siguen existiendo en el código que arma cada paso
    js = open(os.path.join(_STATIC, "tickets_ficha.js"), encoding="utf-8").read()
    ini = js.index("function renderStepper(")
    cuerpo = js[ini:js.index("// ══════════════ \"¿Qué falta?\"", ini)]
    for clase in ("tk-step-ts", "tk-step-quien", "tk-step-dur", "tk-trk-resumen", "tk-pending-pill"):
        assert clase in cuerpo, clase


def test_en_pantalla_ancha_el_resumen_va_al_lado_y_en_celular_debajo():
    v2 = _css("tickets_ficha_v2.css")
    ancho = re.search(r"@media \(min-width:1200px\)\{\s*\.tk-stepper-card\{grid-template-columns:minmax\(0,1fr\) auto;[^}]*\}\s*"
                      r"\.tk-trk-resumen\{flex-direction:column;[^}]*border-left:1px dashed", v2)
    assert ancho, "falta el resumen de tiempos al lado del recorrido en pantallas anchas"
    base = _css("tickets_ficha.css")
    assert re.search(r"\.tk-stepper-card\{display:grid;grid-template-columns:minmax\(0,1fr\);", base)   # una columna por defecto
    assert "grid-column:1/-1;justify-self:center" in base                                                # «En espera» ocupa todo el ancho


def test_no_se_compacto_con_una_capa_de_parches():
    """Las reglas del recorrido se editaron donde viven: sin #id para ganar especificidad ni !important."""
    for nombre in ("tickets_ficha.css", "tickets_ficha_v2.css"):
        css = _css(nombre)
        for regla in re.findall(r"(?m)^\s*([^\s@/}][^{]*\.tk-(?:step|stepper|trk)[^{]*)\{([^}]*)\}", css):
            selector, cuerpo = regla
            assert not selector.strip().startswith("#"), (nombre, selector)
            assert "!important" not in cuerpo, (nombre, selector)

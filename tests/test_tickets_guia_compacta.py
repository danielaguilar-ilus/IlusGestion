"""Cabecera de la ficha de ticket más compacta (Daniel 03-nov-2026: «me cuesta más darle scroll»).
Primero se compactó con un bloque de reglas «#tkGuia …» encima de las originales; el 03-nov Daniel preguntó si el código
estaba «sin parches» y se CONSOLIDÓ: los valores compactos viven en las reglas originales (.tk-gbar, .tk-gchip, .tk-gsig,
.tk-ea-col…) y no queda ningún #id puesto para ganar especificidad. Estas pruebas cuidan esa forma, no los píxeles
(los píxeles se midieron con una huella de estilos computados: 56 capturas, 0 diferencias antes/después)."""
import os
import re

_STATIC = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "static")
_CSS = os.path.join(_STATIC, "tickets_ficha_v2.css")


def _css():
    with open(_CSS, encoding="utf-8") as fh:
        return fh.read()


def _reglas(nombre):
    """(contexto @media, selector, cuerpo, línea) de cada regla de un CSS; sin comentarios."""
    with open(os.path.join(_STATIC, nombre), encoding="utf-8") as fh:
        css = fh.read()
    sin = re.sub(r"/\*.*?\*/", lambda m: "\n" * m.group(0).count("\n"), css, flags=re.S)
    salida, pila = [], []
    for m in re.finditer(r"([^{}]*)\{|\}", sin, re.S):
        if m.group(0) == "}":
            if pila:
                tipo, sel, ln, ini = pila.pop()
                if tipo == "regla":
                    salida.append((" ".join(p[1][:40] for p in pila if p[0] == "media"), sel, sin[ini:m.start()].strip(), ln))
            continue
        sel = m.group(1).strip()
        ln = sin.count("\n", 0, m.start(1)) + (len(m.group(1)) - len(m.group(1).lstrip()) and m.group(1)[:len(m.group(1)) - len(m.group(1).lstrip())].count("\n")) + 1
        if sel.startswith("@media") or sel.startswith("@supports"):
            pila.append(("media", sel, ln, m.end()))
        elif sel.startswith("@"):
            pila.append(("at", sel, ln, m.end()))
        else:
            pila.append(("regla", sel, ln, m.end()))
    return salida


def test_la_guia_y_el_estado_no_usan_id_para_ganar_especificidad():
    for ctx, sel, cuerpo, ln in _reglas("tickets_ficha_v2.css"):
        for s in [x.strip() for x in sel.split(",")]:
            assert not re.match(r"#tk(Guia|EstadoAvance)\b", s), "regla con #id puesto para ganar especificidad (línea %d): %s" % (ln, s)
    css = _css()
    assert "#tkGuia" not in re.sub(r"/\*.*?\*/", "", css, flags=re.S)


def test_ningun_selector_de_la_ficha_se_define_dos_veces_en_el_mismo_contexto():
    """La ficha tuvo 2-3 capas por selector (julio, «Conversación v2», 28-oct, 03-nov). Ahora cada selector vive una vez
    por contexto (@media distinto = otro contexto)."""
    patron = re.compile(r"\.(rp-|email-chip|ql-|tk-g(chip|bar|prog|sig|acc|btn|uia)|tk-ea|tk-step|tk-trk|tk-stepper)")
    vistos, repetidos = {}, []
    for nombre in ("tickets_ficha.css", "tickets_ficha_v2.css"):
        for ctx, sel, cuerpo, ln in _reglas(nombre):
            for s in [x.strip() for x in sel.split(",")]:
                if not patron.search(s):
                    continue
                clave = (ctx, s)
                if clave in vistos:
                    repetidos.append("«%s» %s:%d y %s" % (s, nombre, ln, vistos[clave]))
                vistos[clave] = "%s:%d" % (nombre, ln)
    assert len(vistos) > 150     # la prueba de verdad recorrió las reglas
    assert not repetidos, "selectores definidos más de una vez en el mismo contexto: " + "; ".join(repetidos)


def test_la_tarjeta_siguiente_es_una_fila_y_en_celular_vuelve_a_apilarse():
    css = _css()
    base = css.index(".tk-gsig{display:grid;grid-template-columns:auto minmax(0,1fr) auto;")
    movil = re.search(r"@media \(max-width:991px\)\{[^@]*?\.tk-gsig\{grid-template-columns:minmax\(0,1fr\);"
                      r"grid-template-areas:\"k\" \"t\" \"p\" \"a\";\}", css)
    assert movil and movil.start() > base, "el @media de celular debe ir DESPUÉS de la regla base (misma especificidad)"
    # los botones de la guía son más bajos que los del modal «Asociar OT» (que conservan la base)
    assert ".tk-guia .tk-gbtn{min-height:38px;" in css


def test_la_barra_fija_esconde_la_tarjeta_siguiente():
    # Regresión del 03-nov: una regla «#tkGuia .tk-gsig{display:grid}» le ganaba a «.tk-guia.is-fija .tk-gsig{display:none}».
    css = _css()
    assert ".tk-guia.is-fija .tk-gchips,.tk-guia.is-fija .tk-gsig{display:none;}" in css
    assert css.count(".tk-gsig{display:grid") == 1          # base única; la fija gana por especificidad de clases


def test_estado_y_avance_se_fusiona_en_la_barra_de_la_guia():
    js_ruta = os.path.join(_STATIC, "tickets_ficha_v2.js")
    with open(js_ruta, encoding="utf-8") as fh:
        js = fh.read()
    ini = js.index("function pintarGuia(")
    cuerpo = js[ini:js.index("function observarFija", ini)]
    # se saca ANTES de cualquier innerHTML (si no, se borra el nodo con sus ids) y se devuelve después
    assert cuerpo.index("sacarEstadoAvance(panel)") < cuerpo.index("panel.innerHTML = ''")
    redibujo = re.search(r"panel\.innerHTML =\s*\r?\n", cuerpo).start()   # el innerHTML grande de la guía
    assert cuerpo.rindex("sacarEstadoAvance(panel)") < redibujo < cuerpo.index("ponerEstadoAvance(panel)")
    assert "data-ea-hueco" in cuerpo
    css = _css()
    # la columna «Avance» repetía la guía: se oculta solo dentro de ella, con una regla que le gana a la de la columna
    assert ".tk-ea.is-en-guia .tk-ea-col.tk-ea-avance{display:none;}" in css
    assert css.index(".tk-ea.is-en-guia .tk-ea-col.tk-ea-avance{display:none;}") > css.index(".tk-ea.is-en-guia .tk-ea-col{display:grid")
    assert ".tk-ea.is-en-guia .tk-ea-sub{grid-column:1 / -1;width:0;min-width:100%;" in css
    # barra fija: solo las píldoras
    assert ".tk-guia.is-fija .tk-ea.is-en-guia .tk-ea-sub{display:none;}" in css

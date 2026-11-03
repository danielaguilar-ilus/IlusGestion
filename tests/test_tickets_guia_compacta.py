"""Cabecera de la ficha de ticket más compacta (Daniel 03-nov-2026: «me cuesta más darle scroll»).
Las reglas compactas deben ganar sobre las originales: por eso llevan #tkGuia / #tkEstadoAvance
(con solo la clase, la prueba en vivo mostró que «Estado y avance» no cambiaba)."""
import os
import re

_CSS = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "static", "tickets_ficha_v2.css")


def _css():
    with open(_CSS, encoding="utf-8") as fh:
        return fh.read()


def test_las_reglas_compactas_llevan_el_id_para_ganar_especificidad():
    css = _css()
    ini = css.index("Cabecera de la ficha más compacta")
    bloque = css[ini:]
    for sel in ("#tkEstadoAvance .tk-ea-col", "#tkGuia .tk-gsig", "#tkGuia .tk-gchip"):
        assert sel in bloque, sel
    # nada del bloque compacto debe usar un selector sin el id de su sección
    reglas = re.findall(r"(?m)^([^\s@/}][^{]*)\{", bloque)
    assert reglas and all(r.strip().startswith(("#tkGuia", "#tkEstadoAvance")) or "tkGuia" in r for r in reglas), reglas


def test_la_tarjeta_siguiente_vuelve_a_apilarse_en_celular():
    css = _css()
    assert re.search(r"@media \(max-width:991px\)\{\s*/\*[^*]*\*/\s*#tkGuia \.tk-gsig\{grid-template-columns:minmax\(0,1fr\)", css)


def test_la_barra_fija_sigue_escondiendo_la_tarjeta_siguiente():
    # Regresión del 03-nov: «#tkGuia .tk-gsig{display:grid}» le ganaba a «.tk-guia.is-fija .tk-gsig{display:none}».
    css = _css()
    assert "#tkGuia.is-fija .tk-gsig{display:none;}" in css
    assert css.index("#tkGuia.is-fija .tk-gsig{display:none;}") > css.index("#tkGuia .tk-gsig{display:grid")


def test_estado_y_avance_se_fusiona_en_la_barra_de_la_guia():
    js_ruta = os.path.join(os.path.dirname(_CSS), "tickets_ficha_v2.js")
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
    # la columna «Avance» repetía la guía: se oculta solo dentro de ella, con una regla que le gana a la del bloque
    assert "#tkGuia .tk-ea.is-en-guia .tk-ea-col.tk-ea-avance{display:none;}" in css
    assert "#tkGuia .tk-ea.is-en-guia .tk-ea-sub{grid-column:1 / -1;width:0;min-width:100%;" in css

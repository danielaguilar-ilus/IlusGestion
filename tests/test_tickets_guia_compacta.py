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

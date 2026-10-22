"""Red de seguridad: el JavaScript de las pantallas clave debe compilar.

Origen (auditoría Impeccable de repuestos, 21-oct-2026, hallazgo A1): un solo error de sintaxis (`function rsVista(v){function
rsVista(v){` duplicada) dejó TODO el script de /repuestos detenido ~21 horas (20-oct 15:01 → 21-oct 12:10); las solicitudes
«parecían borradas» y nadie lo notó. Ningún test miraba esas plantillas. Este archivo las pasa por `node --check`.

- Plantillas HTML: se extraen los <script> sin `src`, se neutralizan las marcas Jinja ({{ }}, {% %}, {# #}) y se compila cada bloque.
- Archivos estáticos: se compilan tal cual.
`templates/ot2/detalle.html` queda fuera a propósito: tiene bloques cuyo JS depende de `{% if %}` anidados y quitar solo las
marcas deja código inválido; se revisa a mano. Si se logra renderizarla con datos de mentira, súmala aquí.
"""
import os
import re
import shutil
import subprocess
import tempfile

import pytest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NODE = shutil.which("node")

PLANTILLAS = [
    "templates/clientes_hub/repuestos.html",
    "templates/mantenciones/incidencias.html",
    "templates/mantenciones/plantillas.html",
    "templates/catalogo/clases.html",
    "templates/ot2/panel.html",
    "templates/ot2/_modal_crear.html",
]
ESTATICOS = [
    "static/ot_acciones.js",
    "static/ot_hallazgos.js",
    "static/ot_que_paso.js",
    "static/catalogo_list.js",
]


def _sin_jinja(js):
    js = re.sub(r"\{#.*?#\}", "", js, flags=re.S)
    js = re.sub(r"\{%-?.*?-?%\}", "", js, flags=re.S)
    return re.sub(r"\{\{.*?\}\}", "0", js, flags=re.S)


def _compila(js):
    with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False, encoding="utf-8") as f:
        f.write(js)
        ruta = f.name
    try:
        r = subprocess.run([NODE, "--check", ruta], capture_output=True, text=True, timeout=60)
    finally:
        os.unlink(ruta)
    return r.returncode == 0, "\n".join(r.stderr.strip().splitlines()[:6])


@pytest.mark.skipif(not NODE, reason="Node no está instalado")
@pytest.mark.parametrize("rel", PLANTILLAS)
def test_los_scripts_de_la_plantilla_compilan(rel):
    ruta = os.path.join(RAIZ, *rel.split("/"))
    assert os.path.exists(ruta), rel
    src = open(ruta, encoding="utf-8").read()
    bloques = re.findall(r"<script(?![^>]*\bsrc=)[^>]*>(.*?)</script>", src, re.S)
    assert bloques, "%s no trae ningún <script> en línea: ¿se movió?" % rel
    for i, b in enumerate(bloques):
        ok, err = _compila(_sin_jinja(b))
        assert ok, "%s: el bloque <script> #%d no compila (esto apaga toda la pantalla):\n%s" % (rel, i, err)


@pytest.mark.skipif(not NODE, reason="Node no está instalado")
@pytest.mark.parametrize("rel", ESTATICOS)
def test_los_archivos_js_compilan(rel):
    ruta = os.path.join(RAIZ, *rel.split("/"))
    assert os.path.exists(ruta), rel
    ok, err = _compila(open(ruta, encoding="utf-8").read())
    assert ok, "%s no compila:\n%s" % (rel, err)


@pytest.mark.skipif(not NODE, reason="Node no está instalado")
def test_el_detector_realmente_detecta_la_falla_de_repuestos():
    """Prueba de la prueba: la llave duplicada que apagó /repuestos 21 h debe fallar."""
    roto = "function rsVista(v){function rsVista(v){ return 1; }\nvar x = 2;"
    ok, _ = _compila(roto)
    assert not ok

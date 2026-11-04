"""Formulario público de tickets: multidocumento con las mismas opciones que Retiros
(Daniel 2026-11-03: «debe ser multidocumento y tener las mismas opciones que Retiros:
N° de pedido, BLV o FCV»). Hasta 5 documentos; viajan en `numero_documento` como
«WEB-1136, FCV-10683, BLV-998», el texto que el backend ya sabe leer."""
import os
import re
import shutil
import subprocess
import tempfile

import pytest

import tickets_module as tm

_RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_SHOPIFY = os.path.join(_RAIZ, "Formulario_ticket_Shopify_ILUS.html")
_SERVIDOR = os.path.join(_RAIZ, "templates", "tickets", "soporte_publico.html")


def _leer(ruta):
    with open(ruta, encoding="utf-8") as fh:
        return fh.read()


@pytest.mark.parametrize("ruta", [_SHOPIFY, _SERVIDOR])
def test_los_dos_formularios_ofrecen_pedido_factura_y_boleta(ruta):
    src = _leer(ruta)
    assert 'data-doc-tipo' in src and 'data-docrow' in src
    for tido in ("WEB", "FCV", "BLV"):
        assert f'<option value="{tido}">' in src
    assert "Pedido web (ilusfitness.com)" in src
    assert "Factura electrónica" in src and "Boleta electrónica" in src
    assert "+ Agregar documento" in src
    assert re.search(r"DOC_MAX\s*=\s*5", src), "el tope de Retiros son 5 documentos"


def test_el_envio_usa_los_documentos_y_no_el_campo_viejo():
    sh = _leer(_SHOPIFY)
    assert "numero_documento: documentosDelFormulario().texto" in sh
    assert "numero_documento: $('itk_doc').value.trim()" not in sh
    sv = _leer(_SERVIDOR)
    assert "numero_documento: spDocumentos().texto" in sv
    assert "numero_documento: document.getElementById('spDoc').value.trim()" not in sv


def test_la_plantilla_del_servidor_compila_y_no_pierde_partes():
    jinja2 = pytest.importorskip("jinja2")
    src = _leer(_SERVIDOR)
    jinja2.Environment().parse(src)
    # «{#» abre un comentario de Jinja: pegado a un selector CSS se traga media página
    # sin avisar (pasó el 03-nov con «@media(...){#ilus-tk»). Nunca debe aparecer en CSS/JS.
    fuera_de_comentarios = re.sub(r"\{#.*?#\}", "", src, flags=re.S)
    assert "{#" not in fuera_de_comentarios
    env = jinja2.Environment()
    env.globals["url_for"] = lambda ep, **k: "/static/" + k.get("filename", "")
    html = env.from_string(src).render(
        tk_tipos_publicos=["tech_support"], tipo_label={}, tipo_publico_texto={},
        tipos_sin_equipo=(), ilus_t0=0)
    for marca in ('id="spDocs"', 'id="spDocAdd"', 'id="spDoc"', "</style>", "</html>"):
        assert marca in html, "se perdió " + marca + " al renderizar"


def test_el_texto_que_arma_el_formulario_lo_lee_el_backend():
    r = tm._tk_docs_desde_texto("WEB-1136, FCV-10683, BLV-998")
    assert r["estado"] == "ok"
    assert r["docs"] == [("WEB", "1136"), ("FCV", "10683"), ("BLV", "998")]


@pytest.mark.parametrize("ruta,nombre", [(_SHOPIFY, "documentosDelFormulario"), (_SERVIDOR, "spDocumentos")])
def test_la_serializacion_quita_duplicados_ceros_y_filas_vacias(ruta, nombre):
    node = shutil.which("node")
    if not node:
        pytest.skip("node no disponible")
    src = _leer(ruta)
    ini = src.index("function " + nombre)
    cierre = "\n}\n" if nombre == "spDocumentos" else "\n  }\n"
    cuerpo = src[ini:src.index(cierre, ini) + len(cierre)]
    digitos = "soloDigitos" if nombre == "documentosDelFormulario" else "spSoloDigitos"
    filas = "filasDoc" if nombre == "documentosDelFormulario" else "spFilasDoc"
    prueba = (
        "function " + digitos + "(v){ return String(v||'').replace(/\\D/g,'').slice(0,20); }\n"
        "const FILAS = [];\n"
        "function " + filas + "(){ return FILAS; }\n"
        "function fila(t, n){ return { querySelector: function(s){ return s === '[data-doc-num]' ? {value:n} : {value:t}; } }; }\n"
        + cuerpo + "\n"
        "const assert = require('assert');\n"
        "FILAS.push(fila('WEB','#1136'), fila('FCV','10683'), fila('BLV','0998'), fila('FCV','10683'), fila('FCV',''));\n"
        "let r = " + nombre + "();\n"
        "assert.strictEqual(r.texto, 'WEB-1136, FCV-10683, BLV-998');\n"
        "assert.strictEqual(r.malos.length, 0);\n"
        "FILAS.push(fila('BLV','abc'));\n"
        "r = " + nombre + "();\n"
        "assert.strictEqual(r.malos.length, 1);\n"
        "console.log('OK');\n"
    )
    with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False, encoding="utf-8") as fh:
        fh.write(prueba)
        ruta_js = fh.name
    r = subprocess.run([node, ruta_js], capture_output=True, text=True, timeout=60)
    assert r.returncode == 0 and "OK" in r.stdout, r.stderr or r.stdout

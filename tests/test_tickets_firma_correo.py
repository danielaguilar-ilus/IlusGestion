"""Firmas de correo que se guardaban como «fotos» una y otra vez (Daniel
2026-10-28: «siempre deja el pie de firma como adjunto y lo repite
intensamente»). Un ticket llegó a tener 136 fotos, casi todas el logo y el
banner de la firma."""
import os
import re
import shutil
import subprocess
import tempfile

import tickets_firma_correo as f

_RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def test_la_imagen_de_firma_incrustada_no_se_guarda():
    # el logo de ILUS y el banner InsertPic del caso real
    assert f.motivo_firma("image/png", "sphs-ilus-fitness.png", 7 * 1024, True)
    assert f.motivo_firma("image/jpeg", "InsertPic_7B1F.jpg", 660 * 1024, True)
    assert f.motivo_firma("image/png", "image001.png", 90 * 1024, True)
    # un ícono incrustado diminuto, sea cual sea su nombre
    assert f.motivo_firma("image/png", "x.png", 3 * 1024, True)


def test_una_foto_de_verdad_nunca_se_descarta():
    # adjunto aparte (no incrustado): aunque se llame como firma, se guarda
    assert f.motivo_firma("image/png", "image001.png", 90 * 1024, False) is None
    assert f.motivo_firma("image/jpeg", "IMG_20261028_101500.jpg", 2400 * 1024, False) is None
    # foto pegada dentro del cuerpo por el cliente: la primera vez se guarda
    assert f.motivo_firma("image/jpeg", "IMG_4410.jpg", 1800 * 1024, True, set()) is None
    # un PDF o Word nunca es firma
    assert f.motivo_firma("application/pdf", "firma.pdf", 5 * 1024, True) is None


def test_la_misma_imagen_incrustada_no_se_vuelve_a_guardar():
    vistas = {f.clave_tam("image/jpeg", 1800 * 1024)}
    assert f.motivo_firma("image/jpeg", "IMG_4410.jpg", 1800 * 1024, True, vistas) == "repetida en el ticket"
    # distinto tamaño = otra foto
    assert f.motivo_firma("image/jpeg", "IMG_4411.jpg", 1700 * 1024, True, vistas) is None


def test_incrustada_por_disposicion_o_por_cid_del_html():
    html = '<p>Hola</p><img src="cid:logo123@ilus"><img src="cid:otra">'
    cids = f.cids_referenciados(html)
    assert "logo123@ilus" in cids and "otra" in cids
    assert f.es_incrustada("inline; filename=a.png", None, cids)
    assert f.es_incrustada("attachment", "<logo123@ilus>", cids)
    assert not f.es_incrustada("attachment", "<no-esta>", cids)
    assert not f.es_incrustada("attachment", None, cids)


def test_la_ingesta_de_correo_usa_el_filtro_de_firmas():
    src = open(os.path.join(_RAIZ, "tickets_module.py"), encoding="utf-8").read()
    assert "import tickets_firma_correo as _tkfirma" in src
    i = src.index("_firmas_vistas = set()")
    cuerpo = src[i:i + 3500]
    assert "_tkfirma.motivo_firma(" in cuerpo and "_tkfirma.es_incrustada(" in cuerpo
    # el filtro corre ANTES de armar la lista de adjuntos a guardar
    assert cuerpo.index("_tkfirma.motivo_firma(") < cuerpo.index("adjuntos_ok.append(")


def test_la_ficha_pliega_las_firmas_repetidas_sin_borrar_nada():
    node = shutil.which("node")
    if not node:
        import pytest
        pytest.skip("node no disponible")
    js = open(os.path.join(_RAIZ, "static", "tickets_ficha.js"), encoding="utf-8").read()
    bloque = js[js.index("const TK_ADJ_NOMBRE_FIRMA"):js.index("window.tkAdjRepetidos = tkAdjRepetidos;")]
    prueba = r"""
function esImagen(m, n){ return /^image\//.test(m||''); }
""" + bloque + r"""
const assert = require('assert');
const adj = [];
let id = 1;
for (let i = 0; i < 40; i++) adj.push({id: id++, mime_type: 'image/png', archivo_nombre: i ? 'sphs-ilus-fitnes('+i+').png' : 'sphs-ilus-fitness.png', file_size_kb: 7});
for (let i = 0; i < 30; i++) adj.push({id: id++, mime_type: 'image/jpeg', archivo_nombre: 'InsertPic_'+i+'.jpg', file_size_kb: 660});
adj.push({id: id++, mime_type: 'image/jpeg', archivo_nombre: 'IMG_1.jpg', file_size_kb: 1800});   // foto real
adj.push({id: id++, mime_type: 'image/jpeg', archivo_nombre: 'IMG_2.jpg', file_size_kb: 2100});   // foto real
adj.push({id: id++, mime_type: 'application/pdf', archivo_nombre: 'IL26A058.pdf', file_size_kb: 162});
adj.push({id: id++, mime_type: 'application/pdf', archivo_nombre: 'IL26A058.pdf', file_size_kb: 162}); // PDF: nunca se pliega
const r = tkAdjRepetidos(adj);
assert.strictEqual(r.n, 39 + 29);                 // queda 1 de cada firma
assert.ok(!r.ocultos[1] && r.veces[1] === 40);    // la primera se conserva y dice ×40
assert.ok(!r.ocultos[41] && r.veces[41] === 30);
assert.ok(!r.ocultos[71] && !r.ocultos[72] && !r.ocultos[73] && !r.ocultos[74]);   // fotos reales y PDF intactos
// dos fotos iguales que NO son de firma tampoco se pliegan (solo si son 3 o más)
const dos = [{id:1,mime_type:'image/jpeg',archivo_nombre:'a.jpg',file_size_kb:900},{id:2,mime_type:'image/jpeg',archivo_nombre:'b.jpg',file_size_kb:900}];
assert.strictEqual(tkAdjRepetidos(dos).n, 0);
console.log('OK');
"""
    with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False, encoding="utf-8") as fh:
        fh.write(prueba)
        ruta = fh.name
    r = subprocess.run([node, ruta], capture_output=True, text=True, timeout=60)
    assert r.returncode == 0 and "OK" in r.stdout, r.stderr or r.stdout
    # el contador del paso 5 y el botón «Mostrar» existen
    v2 = open(os.path.join(_RAIZ, "static", "tickets_ficha_v2.js"), encoding="utf-8").read()
    assert "tkAdjRepetidos" in v2 and "repetidas de firma plegadas" in v2
    assert "tkAdjToggleRep()" in js

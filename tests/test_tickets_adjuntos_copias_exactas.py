"""«Fotos y documentos» de la ficha pliega las copias EXACTAS de una imagen (Daniel 04-nov-2026: «se siguen repitiendo las
imágenes»). Caso real, ticket 2304 (TAA-918): el correo de la clienta de las 18:44 entró dos veces (20:48 y 20:49) y
cada foto IMG_3681-3683 quedó dos veces con el mismo nombre, tipo y tamaño. La causa se corrige en la lectura de correo
(ver test_tickets_correo_reclamo_atomico.py); esto hace que lo ya guardado se vea una sola vez, sin borrar nada
(«Mostrar» las vuelve a abrir). Se ejecuta el JS real de la ficha con Node."""
import os
import shutil
import subprocess
import tempfile

import pytest

_RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _correr_node(cuerpo_js):
    node = shutil.which("node")
    if not node:
        pytest.skip("node no disponible")
    with open(os.path.join(_RAIZ, "static", "tickets_ficha.js"), encoding="utf-8") as fh:
        js = fh.read()
    bloque = js[js.index("const TK_ADJ_NOMBRE_FIRMA"):js.index("window.tkAdjRepetidos = tkAdjRepetidos;")]
    prueba = ("function esImagen(m, n){ return /^image\\//.test(m||''); }\n" + bloque
              + "\nconst assert = require('assert');\n" + cuerpo_js + "\nconsole.log('OK');\n")
    with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False, encoding="utf-8") as fh:
        fh.write(prueba)
        ruta = fh.name
    try:
        r = subprocess.run([node, ruta], capture_output=True, text=True, timeout=60)
    finally:
        os.unlink(ruta)
    assert r.returncode == 0 and "OK" in r.stdout, r.stderr or r.stdout


def test_el_caso_real_del_ticket_2304_muestra_cada_foto_una_vez():
    _correr_node(r"""
const adj = [
  {id: 676, mime_type: 'image/jpeg', archivo_nombre: 'image0.jpeg', file_size_kb: 147},
  {id: 677, mime_type: 'video/quicktime', archivo_nombre: 'Video.mov', file_size_kb: 1169},
  {id: 678, mime_type: 'image/jpeg', archivo_nombre: 'image1.jpeg', file_size_kb: 143},
  {id: 679, mime_type: 'image/jpeg', archivo_nombre: 'image2.jpeg', file_size_kb: 164},
  {id: 970, mime_type: 'image/jpeg', archivo_nombre: 'IMG_3683.jpeg', file_size_kb: 142},
  {id: 971, mime_type: 'image/jpeg', archivo_nombre: 'IMG_3682.jpeg', file_size_kb: 128},
  {id: 972, mime_type: 'image/jpeg', archivo_nombre: 'IMG_3681.jpeg', file_size_kb: 113},
  {id: 973, mime_type: 'image/jpeg', archivo_nombre: 'IMG_3683.jpeg', file_size_kb: 142},
  {id: 974, mime_type: 'image/jpeg', archivo_nombre: 'IMG_3682.jpeg', file_size_kb: 128},
  {id: 975, mime_type: 'image/jpeg', archivo_nombre: 'IMG_3681.jpeg', file_size_kb: 113},
];
const r = tkAdjRepetidos(adj);
assert.strictEqual(r.n, 3);
assert.deepStrictEqual([973, 974, 975].map(i => !!r.ocultos[i]), [true, true, true]);   // se pliega la segunda copia
assert.deepStrictEqual([970, 971, 972, 676, 677, 678, 679].map(i => !!r.ocultos[i]), [false, false, false, false, false, false, false]);
assert.deepStrictEqual([970, 971, 972].map(i => r.veces[i]), [2, 2, 2]);                 // la primera dice ×2
""")


def test_tres_copias_dejan_una_con_su_contador():
    _correr_node(r"""
const adj = [1, 2, 3].map(i => ({id: i, mime_type: 'image/png', archivo_nombre: 'Captura.PNG', file_size_kb: 300}));
const r = tkAdjRepetidos(adj);
assert.strictEqual(r.n, 2);
assert.ok(!r.ocultos[1] && r.ocultos[2] && r.ocultos[3] && r.veces[1] === 3);
""")


def test_el_mayusculas_o_minusculas_del_nombre_no_las_distingue():
    _correr_node(r"""
const r = tkAdjRepetidos([{id: 1, mime_type: 'image/jpeg', archivo_nombre: 'IMG_1.JPG', file_size_kb: 500},
                          {id: 2, mime_type: 'image/jpeg', archivo_nombre: 'img_1.jpg', file_size_kb: 500}]);
assert.strictEqual(r.n, 1);
""")


def test_lo_que_no_es_copia_exacta_no_se_pliega():
    _correr_node(r"""
const img = (id, nombre, kb, mime) => ({id: id, mime_type: mime || 'image/jpeg', archivo_nombre: nombre, file_size_kb: kb});
// mismo nombre pero distinto tamaño: son fotos distintas
assert.strictEqual(tkAdjRepetidos([img(1, 'IMG_1.jpg', 500), img(2, 'IMG_1.jpg', 501)]).n, 0);
// mismo tamaño pero distinto nombre: tampoco (regla de siempre: solo 3 o más iguales, o 2 con nombre de firma)
assert.strictEqual(tkAdjRepetidos([img(1, 'a.jpg', 900), img(2, 'b.jpg', 900)]).n, 0);
// mismo nombre y tamaño pero distinto tipo
assert.strictEqual(tkAdjRepetidos([img(1, 'foto', 500, 'image/jpeg'), img(2, 'foto', 500, 'image/png')]).n, 0);
// sin nombre no se adivina
assert.strictEqual(tkAdjRepetidos([img(1, '', 500), img(2, '', 500)]).n, 0);
// los documentos nunca se pliegan, aunque sean idénticos
const pdf = (id) => ({id: id, mime_type: 'application/pdf', archivo_nombre: 'IL26A058.pdf', file_size_kb: 162});
assert.strictEqual(tkAdjRepetidos([pdf(1), pdf(2)]).n, 0);
""")


def test_la_regla_de_las_firmas_sigue_igual():
    """40 logos de firma con el mismo tamaño siguen quedando en uno (la regla anterior no cambia)."""
    _correr_node(r"""
const adj = [];
for (let i = 0; i < 40; i++) adj.push({id: i + 1, mime_type: 'image/png', archivo_nombre: i ? 'sphs-ilus-fitnes(' + i + ').png' : 'sphs-ilus-fitness.png', file_size_kb: 7});
const r = tkAdjRepetidos(adj);
assert.strictEqual(r.n, 39);
assert.ok(!r.ocultos[1] && r.veces[1] === 40);
""")


def test_los_textos_ya_no_dicen_firma_de_correo_porque_ahora_hay_otras_repetidas():
    with open(os.path.join(_RAIZ, "static", "tickets_ficha.js"), encoding="utf-8") as fh:
        js = fh.read()
    with open(os.path.join(_RAIZ, "static", "tickets_ficha_v2.js"), encoding="utf-8") as fh:
        v2 = fh.read()
    assert "imágenes repetidas (firma de correo)" not in js
    assert "Se repite en el ticket (firma de correo)" not in js
    assert "repetidas de firma plegadas" not in v2
    assert "imágenes repetidas · Mostrar" in js and "repetidas plegadas" in v2

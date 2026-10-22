"""Menú «Copiar ítems desde otra plantilla» del editor de Plantillas (21-oct-2026).

Daniel: buscador, filtrar por la categoría de la plantilla abierta, y «copiar» = borrar todo, dejar arriba el N° de serie y
el GPS y agregar el resto SIN duplicarlos. La lógica de decidir qué ítem es «serie/GPS» se ejecuta de verdad con Node.
"""
import json
import os
import re
import shutil
import subprocess

import pytest

import plantillas_serie_gps as psg

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
HTML = open(os.path.join(RAIZ, "templates", "mantenciones", "plantillas.html"), encoding="utf-8").read()
NODE = shutil.which("node")


def _funcion_js(nombre):
    i = HTML.index("function " + nombre + "(")
    n = 0
    for j in range(HTML.index("{", i), len(HTML)):
        n += (HTML[j] == "{") - (HTML[j] == "}")
        if n == 0:
            return HTML[i:j + 1]
    raise AssertionError(nombre)


def _node(items):
    js = _funcion_js("_normBusca") + "\n" + _funcion_js("plEsSerieOGps") + "\n" + \
        "const items = " + json.dumps(items) + ";\nconsole.log(JSON.stringify(items.map(plEsSerieOGps)));"
    r = subprocess.run([NODE, "-e", js], capture_output=True, text=True, timeout=30)
    assert r.returncode == 0, r.stderr
    return json.loads(r.stdout)


@pytest.mark.skipif(not NODE, reason="Node no está instalado")
def test_que_items_cuentan_como_serie_o_gps():
    items = [
        {"titulo": "N° de serie del equipo", "tipo_respuesta": "serie"},          # el objeto
        {"titulo": "Ubicación (GPS)", "tipo_respuesta": "gps"},                    # el objeto
        {"titulo": "N° de serie del equipo", "tipo_respuesta": "texto"},           # texto libre que hacía de serie
        {"titulo": "Número de Serial", "tipo_respuesta": "check"},
        {"titulo": "Foto del N° de serie", "tipo_respuesta": "foto"},              # evidencia: se conserva
        {"titulo": "Foto de placa de serie", "tipo_respuesta": "texto"},           # evidencia: se conserva
        {"titulo": "¿Tiene N° de serie visible?", "tipo_respuesta": "sino"},       # pregunta: se conserva
        {"titulo": "Chequeo y apriete de pernos", "tipo_respuesta": "verificacion"},
    ]
    assert _node(items) == [True, True, True, True, False, False, False, False]


def test_los_objetos_estandar_son_los_mismos_que_en_el_servidor():
    """El editor y el servidor (plantillas_serie_gps.py) deben decir exactamente lo mismo."""
    assert "titulo: '%s'" % psg.SERIE_TITULO in HTML
    assert "titulo: '%s'" % psg.GPS_TITULO in HTML
    assert "descripcion: '%s'" % psg.SERIE_DESC in HTML
    assert "descripcion: '%s'" % psg.GPS_DESC in HTML


def test_el_menu_tiene_buscador_filtro_y_teclado():
    for marca in ('id="copyBuscar"', 'role="combobox"', 'id="copyLista"', "function copyRender", "function copyCategoria",
                  "function copyTecla", "copyCategoriaActiva()", "PL_CATEGORIAS.map", 'aria-label="Categoría"',
                  "Mostrando ' + _copyVisibles.length"):
        assert marca in HTML, marca


def test_el_filtro_arranca_en_la_categoria_de_la_plantilla_abierta():
    f = _funcion_js("copyCategoriaActiva")
    assert "ePCategoriaAdmin" in f and "_copyCat !== null" in f


def test_copiar_reemplaza_y_deja_serie_y_gps_arriba_sin_duplicar():
    f = _funcion_js("copiarItemsDesde")
    assert "!plEsSerieOGps(it)" in f                                     # el resto, sin serie/GPS
    assert "_items.length = 0" in f                                       # borra todo
    assert "_items.push(plItemEstandar(PL_STD_SERIE), plItemEstandar(PL_STD_GPS), ...copiados)" in f
    assert "ilusConfirm(" in f and "confirm(" not in re.sub(r"ilusConfirm\(", "", f)   # nada de confirm nativo (REGLA #1)
    # solo se pregunta si hay contenido de verdad (una plantilla nueva trae solo los dos objetos)
    assert "_items.some(it => !plEsSerieOGps(it))" in f


def test_plantilla_nueva_nace_con_los_dos_objetos_y_al_guardar_se_ofrece_agregarlos():
    a = _funcion_js("abrirEditor")
    assert "plItemEstandar(PL_STD_SERIE), plItemEstandar(PL_STD_GPS)" in a
    g = HTML[HTML.index("async function guardarPlantilla"):]
    assert "Falta el estándar ILUS" in g and "Guardar sin ellos" in g

# -*- coding: utf-8 -*-
"""Escáner de código de barras de Repuestos (Daniel, 2026-10-23).

Lenin: «el buscador del código de barra por cámara de Android no le funciona y cuesta muchísimo». Los dos botones de Repuestos
(buscador de la tabla y Ubicación) tenían cada uno su copia del escáner; ahora comparten el motor `static/ilus_barcode.js`.
Estas pruebas leen los archivos (no cargan app.py) y vigilan que:
  · el motor conserve lo que lo hace funcionar en Android (enfoque continuo, 1080p, zoom, linterna, respaldo al lector
    alternativo, Atrás cierra el escáner, la cámara se apaga siempre);
  · el panel use el motor y no vuelva a duplicar el escáner;
  · la búsqueda por texto y la REGLA #1 (nada de alert/confirm/prompt) sigan intactas.
"""
import os
import re
import shutil
import subprocess

import pytest

from tests._fuente_codigo import texto

MOTOR = "static/ilus_barcode.js"
PANEL = "templates/mantenciones/_repuestos_bodega_pane.html"


def _motor():
    return texto(MOTOR)


def _panel():
    return texto(PANEL)


def test_motor_tiene_lo_necesario_para_android():
    js = _motor()
    for pedazo in (
        "focusMode: 'continuous'",      # enfoque continuo (lo que más falla en Android)
        "ideal: 1920",                   # resolución para códigos 1D chicos
        "getCapabilities",               # zoom/linterna solo si el teléfono los trae
        "torch",
        "zoom",
        "enumerateDevices",              # cambio de lente trasero
        "BarcodeDetectionAPI",           # lector alternativo (zxing)
        "FALLAS_PARA_CAMBIAR",           # si el nativo falla en silencio, cambia solo
        "popstate",                      # tecla Atrás del celular
        "pagehide",
        "t.stop()",                      # la cámara siempre se apaga
        "100dvh",
    ):
        assert pedazo in js, "al motor del escáner le falta: " + pedazo


def test_motor_respeta_regla_1_y_pide_confirmar_formatos_dudosos():
    js = _motor()
    assert not re.search(r"(?<![.\w])(alert|confirm|prompt)\s*\(", js), "REGLA #1: sin alert/confirm/prompt nativos"
    assert "ilusToast" in js
    # ITF, Code 39 y UPC-E se confirman con una segunda lectura igual
    assert re.search(r"CONFIRMAR_DOS\s*=\s*\{[^}]*itf[^}]*code_39[^}]*upc_e", js)
    # Code 128 sigue siendo el primer formato (etiquetas de ubicación y SKU)
    assert re.search(r"FORMATOS\s*=\s*\[\s*'code_128'", js)


def test_motor_botones_tactiles_de_44px():
    js = _motor()
    assert "min-height:44px" in js and "min-width:44px" in js


@pytest.mark.skipif(shutil.which("node") is None, reason="node no está instalado")
def test_motor_sintaxis_valida():
    r = subprocess.run(["node", "--check", os.path.join(os.path.dirname(os.path.dirname(__file__)), "static", "ilus_barcode.js")],
                       capture_output=True, text=True)
    assert r.returncode == 0, r.stderr


def test_panel_usa_el_motor_unico_y_no_duplica_el_escaner():
    html = _panel()
    assert "filename='ilus_barcode.js'" in html
    # las dos entradas llaman al motor compartido
    assert html.count("window.IlusBarcode.escanear(") == 2
    # lo que se duplicaba ya no vive en el panel
    assert "getUserMedia" not in html
    assert "new RB_DETECTOR_CLASS" not in html
    assert "rbDetectorReady" not in html


def test_panel_conserva_funciones_botones_y_busqueda_por_texto():
    html = _panel()
    for nombre in ("rbEscanearUbicacion", "rbEscanearBusquedaPrincipal", "rbUbicacionResolverEscaneo", "rbInicializarEscaner"):
        assert "function " + nombre in html
    assert 'id="rbBtnEscanearBusqueda"' in html and 'id="rbBtnEscanear"' in html
    assert 'id="rbBqInput"' in html          # buscador por texto, intacto
    assert "input.form.submit()" in html     # al leer, filtra igual que si se hubiera tecleado el SKU


def test_panel_evita_abrir_dos_camaras_a_la_vez():
    html = _panel()
    assert html.count("if (RB_ESCANEANDO || !rbEscanerDisponible()) return;") == 2
    assert html.count("RB_ESCANEANDO = false;") >= 2

# -*- coding: utf-8 -*-
"""2026-10-07 · Componente único de firma (static/ilus_firma.js) y su uso en las 7 pantallas.

Sin BD, sin red, sin correo. La parte de navegador (Playwright) se salta sola si no hay Chromium.

    py -m pytest tests/test_ilus_firma.py -q
"""
import json
import os
import shutil
import subprocess

import pytest

_TESTS = os.path.dirname(os.path.abspath(__file__))
RAIZ = os.path.dirname(_TESTS)
JS = os.path.join(RAIZ, "static", "ilus_firma.js")
CSS = os.path.join(RAIZ, "static", "ilus_firma.css")

# plantilla -> id/clave que debe seguir existiendo (no se cambia el HTML exterior ni el campo que viaja)
PANTALLAS = {
    "templates/chofer/entrega.html": ["firmaCanvas", "limpiarFirma", "firma_data_url"],
    "templates/mantenciones/firma_publica.html": ['id="sig"', "deshacerFirma", "limpiarFirma", "firma_cliente"],
    "templates/mantenciones/ot_ficha.html": ["firmaCanvas", "limpiarFirmaPad", "guardarFirmaPad"],
    "templates/ot2/anexo_firma.html": ["firmaCanvas", "btnBorrar", "hFirmaPng", "firmaComoDataUrl"],
    "templates/ot2/detalle.html": ["otfCanvasTec", "otfCanvasCli", "otfCanvasAprob", "firma_tecnico", "firma_cliente", "firma_supervisor"],
    "templates/transporte/firma_retiro_publico.html": ["mrpFirmaCanvas", "mrpLimpiarFirma", "firma_canvas"],
    "templates/retiros/internal_detail.html": ["retiros_firma.js"],
}


def _leer(rel):
    with open(os.path.join(RAIZ, rel), encoding="utf-8") as f:
        return f.read()


def _node():
    n = shutil.which("node")
    if not n:
        pytest.skip("node no está instalado")
    return n


def test_el_componente_existe_y_el_js_pasa_node_check():
    assert os.path.exists(JS) and os.path.exists(CSS)
    subprocess.run([_node(), "--check", JS], check=True, capture_output=True)
    subprocess.run([_node(), "--check", os.path.join(RAIZ, "static", "retiros_firma.js")], check=True, capture_output=True)


def test_el_componente_cumple_el_encargo():
    js = _leer("static/ilus_firma.js")
    for pieza in ("window", "crear", "setPointerCapture", "devicePixelRatio", "dprMax", "ResizeObserver", "quadraticCurveTo",
                  "minLargo", "aDataUrl", "limpiar", "deshacer", "onCambio", "lineaBase", "marcaX", "aria-label", "maxAncho", "touchAction"):
        assert pieza in js, pieza
    assert "alert(" not in js and "confirm(" not in js   # REGLA #1
    css = _leer("static/ilus_firma.css")
    assert "touch-action:none" in css.replace(" ", "")


@pytest.mark.parametrize("rel", sorted(PANTALLAS))
def test_cada_pantalla_carga_el_componente_y_conserva_sus_ids(rel):
    src = _leer(rel)
    assert "filename='ilus_firma.js'" in src, rel
    assert "filename='ilus_firma.css'" in src, rel
    for clave in PANTALLAS[rel]:
        assert clave in src, f"{rel}: falta {clave}"
    if rel != "templates/retiros/internal_detail.html":
        assert "IlusFirma.crear" in src, rel


def test_retiros_js_usa_el_componente():
    assert "IlusFirma.crear" in _leer("static/retiros_firma.js")


def test_el_componente_carga_antes_que_retiros_firma():
    src = _leer("templates/retiros/internal_detail.html")
    assert src.index("ilus_firma.js") < src.index("retiros_firma.js")


def test_las_pantallas_ya_no_tienen_su_propio_dibujo():
    # el dibujo a mano (mousedown/touchstart + lineTo) ya no vive en las pantallas migradas
    for rel in PANTALLAS:
        src = _leer(rel)
        if "ot2/detalle" in rel:
            # detalle trae mucho otro JS: se mira solo la zona de firma
            zona = src[src.index("function initCanvas(id)"):src.index("// ── Etapa técnico / cliente")]
            assert "addEventListener('mousedown'" not in zona and "lineTo" not in zona
            continue
        assert "addEventListener('touchstart'" not in src or "entrega" not in rel, rel
    for rel in ("templates/chofer/entrega.html", "templates/mantenciones/ot_ficha.html", "templates/transporte/firma_retiro_publico.html"):
        assert "addEventListener('mousedown'" not in _leer(rel), rel


def test_las_plantillas_tocadas_se_parsean_con_jinja():
    from jinja2 import Environment, FileSystemLoader
    j = Environment(loader=FileSystemLoader(os.path.join(RAIZ, "templates")))
    for rel in PANTALLAS:
        j.parse(_leer(rel))
    j.parse(_leer("templates/retiros/_firma_bloque.html"))


# ── cálculos puros, con Node ──────────────────────────────────────────
def _correr_node(codigo):
    r = subprocess.run([_node(), "-e", codigo], capture_output=True, text=True, cwd=RAIZ)
    assert r.returncode == 0, r.stderr
    return json.loads(r.stdout)


def test_deteccion_de_vacio_con_node():
    out = _correr_node("""
      const F = require('./static/ilus_firma.js')._puro;
      const t = (pts) => ({ color: '#000', grosor: 2, pts });
      const linea = t([{x:0,y:0},{x:30,y:40}]);          // largo 50
      const punto = t([{x:5,y:5}]);                       // un toque
      console.log(JSON.stringify({
        vacio: F.hayFirma([], 20),
        punto: F.hayFirma([punto], 20),
        puntoMin0: F.hayFirma([punto], 0),
        linea: F.hayFirma([linea], 20),
        largo: F.largoTotal([linea]),
        dosCortas: F.hayFirma([t([{x:0,y:0},{x:8,y:0}]), t([{x:0,y:0},{x:8,y:0}])], 20),
        dosCortasMin10: F.hayFirma([t([{x:0,y:0},{x:8,y:0}]), t([{x:0,y:0},{x:8,y:0}])], 10),
      }));""")
    assert out == {"vacio": False, "punto": False, "puntoMin0": True, "linea": True, "largo": 50, "dosCortas": False, "dosCortasMin10": True}


def test_redimension_uniforme_y_centrada_con_node():
    out = _correr_node("""
      const F = require('./static/ilus_firma.js')._puro;
      console.log(JSON.stringify({
        igual: F.transformacion(300, 200, 300, 200),
        rotarAncho: F.transformacion(300, 200, 700, 200),    // más ancho: no se estira, se centra
        angosto: F.transformacion(300, 200, 150, 200),       // más angosto: se achica parejo
        oculto: F.transformacion(300, 200, 0, 0),
      }));""")
    assert out["igual"] == {"s": 1, "ox": 0, "oy": 0}
    assert out["rotarAncho"] == {"s": 1, "ox": 200, "oy": 0}
    assert out["angosto"] == {"s": 0.5, "ox": 0, "oy": 50}
    assert out["oculto"] == {"s": 1, "ox": 0, "oy": 0}


# ── navegador real (Chromium headless), solo si está disponible ────────────
def _pagina_aislada():
    return """<!doctype html><html><body style="margin:0"><div style="padding:10px">
      <canvas id="c" style="width:100%;height:200px;display:block;border:1px solid #999"></canvas><span id="g">guia</span></div>
      <script src="/static/ilus_firma.js"></script>
      <script>window.cambios=[]; window.f=IlusFirma.crear(document.getElementById('c'),{guia:'g',onCambio:function(x){window.cambios.push(x)}});</script>
      </body></html>"""


def test_navegador_segundo_dedo_toque_suelto_y_tamano():
    sync_api = pytest.importorskip("playwright.sync_api")
    with sync_api.sync_playwright() as p:
        try:
            br = p.chromium.launch()
        except Exception as e:  # sin Chromium instalado
            pytest.skip(f"sin Chromium: {e}")
        ctx = br.new_context(viewport={"width": 390, "height": 844}, device_scale_factor=3, has_touch=True, is_mobile=True)

        def servir(route):
            u = route.request.url
            if u.endswith("/pag"):
                return route.fulfill(status=200, content_type="text/html", body=_pagina_aislada())
            if "/static/ilus_firma.js" in u:
                return route.fulfill(status=200, content_type="text/javascript", body=open(JS, "rb").read())
            return route.fulfill(status=404, body="")
        ctx.route("**/*", servir)
        page = ctx.new_page()
        errs = []
        page.on("console", lambda m: errs.append(m.text) if m.type == "error" else None)
        page.on("pageerror", lambda e: errs.append(str(e)))
        page.goto("http://t.local/pag")
        cdp = ctx.new_cdp_session(page)
        box = page.locator("#c").bounding_box()
        x0, y0 = box["x"], box["y"]

        # 1) un toque suelto: se dibuja el punto pero NO cuenta como firma
        cdp.send("Input.dispatchTouchEvent", {"type": "touchStart", "touchPoints": [{"x": x0 + 40, "y": y0 + 40, "id": 1}]})
        cdp.send("Input.dispatchTouchEvent", {"type": "touchEnd", "touchPoints": []})
        assert page.evaluate("f.numTrazos()") == 1 and page.evaluate("f.firmado()") is False
        page.evaluate("f.limpiar()")

        # 2) con dos dedos el trazo NO salta al segundo: solo cuenta el primero
        cdp.send("Input.dispatchTouchEvent", {"type": "touchStart", "touchPoints": [{"x": x0 + 30, "y": y0 + 100, "id": 1}]})
        cdp.send("Input.dispatchTouchEvent", {"type": "touchStart", "touchPoints": [
            {"x": x0 + 30, "y": y0 + 100, "id": 1}, {"x": x0 + 300, "y": y0 + 20, "id": 2}]})
        for i in range(1, 12):
            cdp.send("Input.dispatchTouchEvent", {"type": "touchMove", "touchPoints": [
                {"x": x0 + 30 + i * 12, "y": y0 + 100, "id": 1}, {"x": x0 + 300, "y": y0 + 20 + i * 8, "id": 2}]})
        cdp.send("Input.dispatchTouchEvent", {"type": "touchEnd", "touchPoints": []})
        assert page.evaluate("f.numTrazos()") == 1
        assert page.evaluate("f.firmado()") is True
        largo = page.evaluate("f.largo()")
        assert 120 < largo < 160, largo           # ~ 11 pasos de 12 px del primer dedo; el segundo (≈ 90 px) no suma

        # 3) PNG: empieza con data:image/png, fondo blanco, < 300 KB aun con un garabato denso a DPR 3
        page.evaluate("""()=>{ f.limpiar(); const c=document.getElementById('c'), r=c.getBoundingClientRect();
            for (let k=0;k<40;k++){ const ev=(t,x,y)=>c.dispatchEvent(new PointerEvent(t,{pointerId:k+10,pointerType:'touch',clientX:r.left+x,clientY:r.top+y,bubbles:true,button:0,isPrimary:true}));
              ev('pointerdown',10,5+k*4); for(let i=0;i<150;i++) ev('pointermove',10+i*2.2,5+k*4+Math.sin(i/3+k)*9); ev('pointerup',300,5+k*4); } }""")
        assert page.evaluate("f.numTrazos()") == 40
        url = page.evaluate("f.aDataUrl()")
        assert url.startswith("data:image/png")
        assert (len(url) - 22) * 0.75 < 300 * 1024

        # 4) redimensionar el recuadro no pierde nada
        page.evaluate("document.getElementById('c').style.height='120px'"); page.wait_for_timeout(300)
        assert page.evaluate("f.numTrazos()") == 40 and page.evaluate("f.firmado()") is True
        reales = [e for e in errs if "Failed to load" not in e]
        assert not reales, reales
        br.close()

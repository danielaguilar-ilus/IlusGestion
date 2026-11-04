"""«Respuestas» de la ficha del ticket tras pasarle Impeccable (Daniel 03-nov-2026: «pásale Impeccable a los mensajes»).
Cada prueba cuida un problema MEDIDO en una copia local con el CSS y el JS reales:
  1. las pastillas de fecha pegajosas se apilaban y tapaban el mensaje  -> cada día va en su propio contenedor;
  2. el nombre de los adjuntos se cortaba con «…» (REGLA #15)             -> baja de línea;
  3. siete textos con contraste menor a 4,5:1 (evento 2,4:1, Traducir 2,6:1, «enviado» automático 2,8:1...);
  4. el visor no se podía recorrer con el teclado, imágenes sin alt, iniciales leídas dos veces;
  5. textos de 9,6–10,6 px en la línea de autor/fecha."""
import json
import os
import re
import shutil
import subprocess
from html.parser import HTMLParser

import pytest

_RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _leer(*partes):
    with open(os.path.join(_RAIZ, *partes), encoding="utf-8") as fh:
        return fh.read()


_CSS = _leer("static", "tickets_ficha.css")
_JS = _leer("static", "tickets_ficha.js")
_FICHA = _leer("templates", "tickets", "ficha.html")


# ───────────────────────── 1. Pastillas de fecha: una por día, dentro de su día ─────────────────────────
def _corte(ini, fin):
    a = _JS.index(ini)
    return _JS[a:_JS.index(fin, a)]


def _html_del_hilo(mensajes):
    """Ejecuta el renderThread REAL en Node con un DOM de mentira y devuelve el HTML que arma."""
    nodo = shutil.which("node")
    if not nodo:
        pytest.skip("Node no está instalado")
    stubs = (
        "var TH={innerHTML:'',childElementCount:0,scrollHeight:0,scrollTop:0,clientHeight:0,style:{},querySelectorAll:function(){return [];}};\n"
        "var document={getElementById:function(){return TH;}};\n"
        "function requestAnimationFrame(f){}\nfunction ajustarAltoHilo(){}\n"
        "function esc(s){return (s==null?'':String(s)).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/\"/g,'&quot;').replace(/'/g,'&#39;');}\n"
        "function fmtFechaHora(s){return s||'';}\nfunction safeUrl(u){return String(u||'');}\n"
        "var ticketActual={nombre_contacto:'Cliente de prueba'};\n"
    )
    codigo = (stubs
              + _corte("const _EXT_IMAGEN", "document.querySelectorAll('.tk-tab')")
              + _corte("function iconoAdjunto(", "/* ══ 2026-07-19 (Fable) — helpers")
              + _corte("/* ══ 2026-07-19 (Fable) — helpers", "// Firmas de correo repetidas")
              + _corte("// 2026-07-19 (Fable) — Conversación v2: marca de agua", "// ══════════════ Adjuntos — tarjeta")
              + "\nrenderThread(%s, []);\nprocess.stdout.write(JSON.stringify(TH.innerHTML));\n" % json.dumps(mensajes))
    r = subprocess.run([nodo, "-e", codigo], capture_output=True, text=True, encoding="utf-8", timeout=60)
    assert r.returncode == 0, r.stderr
    return json.loads(r.stdout)


class _Arbol(HTMLParser):
    def __init__(self):
        super().__init__()
        self.pila, self.padres_pastilla, self.padres_mensaje = [], [], []

    def handle_starttag(self, tag, attrs):
        if tag != "div":
            return
        clases = (dict(attrs).get("class") or "").split()
        padre = self.pila[-1] if self.pila else None
        if "rp-fecha-sep" in clases:
            self.padres_pastilla.append(padre)
        if "rp-msg" in clases:
            self.padres_mensaje.append(padre)
        self.pila.append(clases[0] if clases else "")

    def handle_endtag(self, tag):
        if tag == "div" and self.pila:
            self.pila.pop()


def _msg(i, tipo, fecha, usuario="Aarón Rivera", **extra):
    m = {"id": i, "tipo": tipo, "usuario": usuario, "contenido": "<p>mensaje %d</p>" % i, "created_at": fecha}
    m.update(extra)
    return m


def test_cada_dia_va_en_su_propio_contenedor_con_su_pastilla_adentro():
    msgs = [
        _msg(1, "client_message", "01/11/2026 09:00", usuario="", es_interno=0),
        _msg(2, "mensaje", "01/11/2026 09:10", estado_envio="enviado"),
        _msg(3, "cambio_estado", "02/11/2026 08:00", contenido="Abierto → En Curso"),
        _msg(4, "mensaje", "02/11/2026 08:30", estado_envio="enviado"),
        _msg(5, "comentario", "03/11/2026 10:00", es_interno=1),
    ]
    html = _html_del_hilo(msgs)
    assert html.count('<div class="rp-dia">') == 3
    assert html.count("<div") == html.count("</div>"), "el HTML del hilo quedó desbalanceado"
    arbol = _Arbol()
    arbol.feed(html)
    assert arbol.padres_pastilla == ["rp-dia"] * 3, "una pastilla de fecha quedó fuera de su día (se apilaría al hacer scroll)"
    assert len(arbol.padres_mensaje) == 5 and set(arbol.padres_mensaje) == {"rp-dia"}


def test_con_una_fecha_en_formato_raro_el_hilo_sigue_balanceado():
    """Si el servidor algún día manda una fecha sin el formato dd/mm/aaaa, no se abre contenedor y nada se rompe."""
    msgs = [_msg(1, "mensaje", "ayer en la tarde", estado_envio="enviado"),
            _msg(2, "mensaje", "03/11/2026 10:00", estado_envio="enviado")]
    html = _html_del_hilo(msgs)
    assert html.count("<div") == html.count("</div>")
    assert html.count('<div class="rp-dia">') == 1


def test_un_hilo_vacio_no_abre_contenedores():
    assert _html_del_hilo([]) == "<div style=\"color:#9ca3af;font-size:.85rem;text-align:center;padding:20px;\">Aún no hay mensajes.</div>"


# ───────────────────────── 2. Nombres de adjuntos completos (REGLA #15) ─────────────────────────
def _reglas(selector_parte):
    """Cuerpos de las reglas cuyo selector contiene la parte indicada."""
    return [c for s, c in re.findall(r"(?m)^\s*([^{}@/]+)\{([^}]*)\}", _CSS) if selector_parte in s]


def test_el_nombre_del_adjunto_no_se_corta_con_puntos_suspensivos():
    cuerpos = _reglas(".rp-adj-nom")
    assert cuerpos, "no se encontraron las reglas del nombre del adjunto"
    for c in cuerpos:
        assert "text-overflow:ellipsis" not in c.replace(" ", ""), c
        assert "white-space:nowrap" not in c.replace(" ", ""), c


# ───────────────────────── 3. Contraste ≥ 4,5:1 ─────────────────────────
def _lum(hexa):
    h = hexa.lstrip("#")
    c = [int(h[i:i + 2], 16) / 255 for i in (0, 2, 4)]
    c = [v / 12.92 if v <= 0.03928 else ((v + 0.055) / 1.055) ** 2.4 for v in c]
    return 0.2126 * c[0] + 0.7152 * c[1] + 0.0722 * c[2]


def _mezcla(fg, bg, alfa):
    f, b = fg.lstrip("#"), bg.lstrip("#")
    return "#%02x%02x%02x" % tuple(round(int(f[i:i + 2], 16) * alfa + int(b[i:i + 2], 16) * (1 - alfa)) for i in (0, 2, 4))


def _contraste(fg, bg, alfa=1.0):
    a, b = _lum(_mezcla(fg, bg, alfa)), _lum(bg)
    return (max(a, b) + 0.05) / (min(a, b) + 0.05)


def _color_de(selector_exacto):
    m = re.search(r"(?m)(?:^|[}\s,])" + re.escape(selector_exacto) + r"\{[^}]*?(?<![-\w])color:(#[0-9a-fA-F]{6})", _CSS)
    assert m, "no se encontró el color de " + selector_exacto
    return m.group(1)


def test_los_textos_chicos_del_hilo_cumplen_contraste_AA():
    pares = [
        ("evento (cambio de estado)", _color_de(".rp-msg.evento .rp-bubble"), "#f6f7f8", 1.0),
        ("fecha/autor del cliente", _color_de(".rp-msg.in .rp-meta"), "#ffffff", 1.0),
        ("fecha/autor del staff (extremo claro del degradado)", _color_de(".rp-msg.out .rp-meta"), "#252f3f", 1.0),
        ("fecha/autor del comentario interno", _color_de(".rp-msg.interno .rp-meta"), "#fffbeb", 1.0),
        ("fecha/autor del mensaje automático", _color_de(".rp-msg.out.auto .rp-meta"), "#ffffff", 1.0),
        ("«enviado» del mensaje automático", _color_de(".rp-msg.out.auto .rp-estado-ok"), "#ffffff", 1.0),
        ("etiqueta «Automático»", _color_de(".rp-auto-tag"), "#f3f4f6", 1.0),
    ]
    for nombre, fg, bg, alfa in pares:
        assert _contraste(fg, bg, alfa) >= 4.5, "%s: %s sobre %s = %.2f:1" % (nombre, fg, bg, _contraste(fg, bg, alfa))


def test_la_linea_de_fecha_no_pierde_contraste_por_una_opacidad_del_contenedor():
    cuerpo = [c for s, c in re.findall(r"(?m)^\s*([^{}@/]+)\{([^}]*)\}", _CSS) if s.strip() == ".rp-meta"][0]
    assert "opacity" not in cuerpo, cuerpo


def test_traducir_atenuado_sigue_siendo_legible():
    m = re.search(r"@media \(hover:hover\)\{\s*(?:/\*.*?\*/\s*)?\.rp-btn-traducir\{opacity:([\d.]+);", _CSS, re.S)
    assert m, "no se encontró la atenuación de Traducir"
    alfa = float(m.group(1))
    assert 0 < alfa < 1, "REGLA #4.2: Traducir es una función existente: se atenúa, nunca se oculta"
    assert _contraste(_color_de(".rp-msg.in .rp-btn-traducir"), "#ffffff", alfa) >= 4.5


# ───────────────────────── 4. Teclado y lectores de pantalla ─────────────────────────
def test_el_visor_se_recorre_con_teclado_y_tiene_nombre():
    visor = re.search(r'<div class="rp-thread" id="rpThread"[^>]*>', _FICHA).group(0)
    assert 'role="region"' in visor and 'aria-label="Conversación del ticket"' in visor and 'tabindex="0"' in visor
    assert "aria-live" not in visor, "el hilo se repinta completo cada pocos segundos: aria-live lo volvería a leer todo"
    assert ".rp-thread:focus-visible{outline:2px solid #dc2626;" in _CSS.replace(" ;", ";")


def test_imagenes_avatares_y_botones_de_icono_tienen_su_nombre():
    assert re.search(r'<img src="\'\+esc\(url\)\+\'" alt="" loading="lazy"', _JS), "la imagen del adjunto quedó sin alt (el nombre va al lado)"
    assert "'<div class=\"rp-avatar\" aria-hidden=\"true\">'" in _JS, "las iniciales se leerían dos veces (avatar y autor)"
    for bid in ("rpBtnDictar", "rpGuardarPlantilla", "rpBtnTraducir"):
        boton = re.search(r'<button[^>]*id="%s"[^>]*>' % bid, _FICHA).group(0)
        assert 'aria-label="' in boton, bid


# ───────────────────────── 5. Tamaño mínimo de la línea de autor y fecha ─────────────────────────
def _rem(selector_exacto):
    m = re.search(r"(?m)(?:^|[}\s,])" + re.escape(selector_exacto) + r"\{[^}]*?font-size:([\d.]+)rem", _CSS)
    assert m, selector_exacto
    return float(m.group(1))


def test_la_linea_de_autor_y_fecha_ya_no_baja_de_11_5_px():
    for sel in (".rp-meta", ".rp-meta-mini", ".rp-fecha-sep span", ".rp-adj-chip.es-img .rp-adj-nom", ".rp-adj-col .rp-adj-peso"):
        assert _rem(sel) * 16 >= 11.1, sel
    assert _rem(".rp-meta") * 16 >= 12
    assert _rem(".rp-msg.evento .rp-bubble") * 16 >= 12

# -*- coding: utf-8 -*-
"""CORREOS A PRUEBA DE OUTLOOK DE ESCRITORIO (Daniel, 6-nov-2026: «en Outlook no se ve bien… evalúalo para una buena vista en Outlook sin dañar las vistas de
celular y de Gmail»).

Outlook de escritorio dibuja el correo con el motor de Word, que:
  · aplica el `padding` de un <table> a CADA celda (entre fila y fila quedaban huecos de ~40 px: lo que se vio en las capturas),
  · ignora el `margin` de una <table>,
  · ignora el `padding` de un <a> (un botón con relleno se ve como un rectángulo diminuto con el texto pegado al borde).
Gmail y los celulares no tienen ese problema, así que lo que sirve para todos es un HTML «de tablas»: el relleno, el fondo y la raya de color van en una CELDA (<td>),
y el espacio exterior en una tabla envoltorio.

`outlook_safe(html)` reescribe, justo antes de enviar, SOLO esos dos patrones:
  1. `<table … style="…padding:P…[margin:M][background:B][border-left:L][border-radius:R]">filas</table>` →
       <table [margin:M]> <tr><td bgcolor=B style="background:B;border-left:L;border-radius:R;padding:P"> <table width=100%>filas</table> </td></tr> </table>
     El relleno, el fondo y la raya pasan a una celda (idéntico en el navegador). El MARGEN se conserva tal cual para Gmail y el celular (así los márgenes siguen «fusionándose» con
     los párrafos vecinos y el correo no crece ni un píxel) y, SOLO para Outlook de escritorio, se agrega un envoltorio con `padding` dentro de un comentario condicional
     `<!--[if mso]>…<![endif]-->` (los demás clientes lo ven como un comentario y lo ignoran).
  2. `<a style="display:inline-block;padding:P;background:#hex;…">texto</a>` → un botón «de verdad»: tabla de una celda con `bgcolor` y el relleno en el <td>, y el <a> de bloque.
El texto, los enlaces y los colores no cambian; lo que ya está bien hecho (tablas sin relleno ni margen, botones con el relleno en la celda) no se toca, y volver a pasar
el resultado por la función no lo cambia (idempotente). Módulo PURO (sin Flask ni base de datos), con pruebas; nunca lanza: si algo falla devuelve el HTML original.
"""
import re

_RX_TABLA = re.compile(r"<(/?)table\b([^>]*)>", re.IGNORECASE | re.DOTALL)
_RX_ANCLA = re.compile(r"<a\b([^>]*)>(.*?)</a>", re.IGNORECASE | re.DOTALL)
# Un párrafo que SOLO contiene un enlace-botón: al meter una <table> dentro de un <p> el navegador cierra el párrafo antes de la tabla y deja un <p> vacío (con su
# aire de 16 px) detrás; por eso ese párrafo pasa a <div> (conserva su estilo y sí acepta una tabla).
_RX_P_ANCLA = re.compile(r"<p\b([^>]*)>(\s*)(<a\b[^>]*>.*?</a>)(\s*)</p>", re.IGNORECASE | re.DOTALL)
_RX_STYLE = re.compile(r"""\sstyle\s*=\s*("([^"]*)"|'([^']*)')""", re.IGNORECASE)
_RX_HEX = re.compile(r"^#[0-9a-fA-F]{3,8}$")

_TABLA_BASE = 'role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0"'
_MSO_AB = "<!--[if mso]>"          # lo lee SOLO Outlook de escritorio; para todos los demás clientes es un comentario
_MSO_CE = "<![endif]-->"


def _separar_estilo(atributos):
    """(atributos_sin_style, [(propiedad, valor), …]) de la cadena de atributos de una etiqueta."""
    m = _RX_STYLE.search(atributos or "")
    if not m:
        return atributos or "", []
    estilo = m.group(2) if m.group(2) is not None else (m.group(3) or "")
    sin = (atributos[:m.start()] + atributos[m.end():])
    decls = []
    for trozo in estilo.split(";"):
        if ":" not in trozo:
            continue
        prop, _, val = trozo.partition(":")
        prop, val = prop.strip().lower(), val.strip()
        if prop:
            decls.append((prop, val))
    return sin, decls


def _unir(decls):
    return ";".join(f"{p}:{v}" for p, v in decls)


def _valor(decls, *props):
    for p, v in decls:
        if p in props:
            return v
    return ""


def _quitar(decls, *props):
    return [(p, v) for p, v in decls if p not in props]


def _es_cero(valor):
    """True si el valor es vacío o solo ceros («0», «0 0», «0px 0px»)."""
    return not re.search(r"[1-9]", valor or "")


def _abrir_tabla(atributos, envuelta=False):
    """(apertura, cierre) para un <table …> que trae `padding` o `margin` en su estilo; None si no hace falta tocarlo. `envuelta`: ya viene precedida de su envoltorio de
    Outlook (segunda pasada): no se le agrega otro."""
    sin_estilo, decls = _separar_estilo(atributos)
    relleno = _valor(decls, "padding")
    margen = _valor(decls, "margin")
    if _es_cero(relleno):
        relleno = ""
    if _es_cero(margen) or "auto" in margen.lower():          # «margin:0 auto» (centrar) y ceros: nada que reescribir
        margen = ""
    margen_para_outlook = "" if envuelta else margen         # el margen SE CONSERVA en la tabla (Gmail/celular); Outlook recibe además un envoltorio con ese espacio
    if not relleno and not margen_para_outlook:
        return None
    envoltorio_ab = envoltorio_ce = ""
    if margen_para_outlook:
        envoltorio_ab = f'{_MSO_AB}<table {_TABLA_BASE}><tr><td style="padding:{margen_para_outlook}">{_MSO_CE}'
        envoltorio_ce = f'{_MSO_AB}</td></tr></table>{_MSO_CE}'
    if relleno:
        fondo = _valor(decls, "background", "background-color")
        raya = _valor(decls, "border-left")
        radio = _valor(decls, "border-radius")
        resto = _quitar(decls, "padding", "background", "background-color", "border-left", "border-radius")      # el margen se queda (ver arriba)
        celda = []
        if fondo:
            celda.append(("background", fondo))
        if raya:
            celda.append(("border-left", raya))
        if radio:
            celda.append(("border-radius", radio))
        celda.append(("padding", relleno))
        bg = f' bgcolor="{fondo}"' if _RX_HEX.match(fondo or "") else ""
        estilo_tabla = f' style="{_unir(resto)}"' if resto else ""
        apertura = (envoltorio_ab + f"<table{sin_estilo}{estilo_tabla}>" + f"<tr><td{bg} style=\"{_unir(celda)}\">" + f"<table {_TABLA_BASE}>")
        cierre = "</table></td></tr></table>" + envoltorio_ce
        return apertura, cierre
    # solo margen: la tabla queda idéntica; solo se le agrega el envoltorio de Outlook
    estilo_tabla = f' style="{_unir(decls)}"' if decls else ""
    return envoltorio_ab + f"<table{sin_estilo}{estilo_tabla}>", "</table>" + envoltorio_ce


def _tablas(html):
    salida, pos, pila = [], 0, []
    for m in _RX_TABLA.finditer(html):
        salida.append(html[pos:m.start()])
        pos = m.end()
        if m.group(1):                                     # </table>
            salida.append(pila.pop() if pila else "</table>")
            continue
        r = _abrir_tabla(m.group(2), envuelta=html[max(0, m.start() - 40):m.start()].endswith(_MSO_CE))
        if r is None:
            pila.append("</table>")
            salida.append(m.group(0))
        else:
            pila.append(r[1])
            salida.append(r[0])
    salida.append(html[pos:])
    return "".join(salida)


_CONSERVA_EN_ENLACE = ("color", "font-size", "font-weight", "text-decoration", "font-family", "line-height", "letter-spacing", "text-transform")


def _boton(m):
    atributos, interior = m.group(1), m.group(2)
    sin_estilo, decls = _separar_estilo(atributos)
    relleno = _valor(decls, "padding")
    fondo = _valor(decls, "background", "background-color")
    if not relleno or not _RX_HEX.match(fondo or ""):
        return m.group(0)
    radio = _valor(decls, "border-radius")
    del_enlace = [("display", "block")] + [(p, v) for p, v in decls if p in _CONSERVA_EN_ENLACE]
    if not _valor(del_enlace, "font-size"):
        # el <a> heredaba el tamaño de su párrafo (13 px en los botones de los correos de la OT); dentro de una celda ya no hereda: se fija explícito
        del_enlace.append(("font-size", "13px"))
    celda = [("background", fondo)] + ([("border-radius", radio)] if radio else []) + [("padding", relleno)]
    return (f'<table role="presentation" cellspacing="0" cellpadding="0" border="0"><tr><td align="center" bgcolor="{fondo}" style="{_unir(celda)}">'
            f'<a{sin_estilo} style="{_unir(del_enlace)}">{interior}</a></td></tr></table>')


def _p_boton(m):
    a = _RX_ANCLA.fullmatch(m.group(3))
    if not a:
        return m.group(0)
    boton = _boton(a)
    if boton == a.group(0):                                 # no era un botón (sin fondo/relleno): el párrafo queda como estaba
        return m.group(0)
    return f"<div{m.group(1)}>{boton}</div>"


def outlook_safe(html):
    """El HTML del correo con las tablas con relleno/margen y los botones con relleno en el <a> reescritos para Outlook de escritorio. Nunca lanza."""
    if not html or not isinstance(html, str):
        return html
    try:
        return _RX_ANCLA.sub(_boton, _RX_P_ANCLA.sub(_p_boton, _tablas(html)))
    except Exception:
        return html


def problemas(html):
    """Lista de lo que Outlook de escritorio dibujaría mal en este HTML (para las pruebas y el diagnóstico): [] = limpio."""
    malos = []
    for m in _RX_TABLA.finditer(html or ""):
        if m.group(1):
            continue
        _sin, decls = _separar_estilo(m.group(2))
        if not _es_cero(_valor(decls, "padding")):
            malos.append("table con padding")
        margen = _valor(decls, "margin")
        if not _es_cero(margen) and "auto" not in margen.lower() and not (html or "")[max(0, m.start() - 40):m.start()].endswith(_MSO_CE):
            malos.append("table con margin")           # un margen CON su envoltorio de Outlook delante está bien
    for m in _RX_ANCLA.finditer(html or ""):
        _sin, decls = _separar_estilo(m.group(1))
        if _valor(decls, "padding") and _RX_HEX.match(_valor(decls, "background", "background-color") or ""):
            malos.append("enlace-botón con padding")
    return malos

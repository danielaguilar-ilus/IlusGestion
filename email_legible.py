# -*- coding: utf-8 -*-
"""LA FRASE DEL TICKET SE LEE (auditoría de diseño del 6-nov-2026, aplicada a pedido de Daniel).

Daniel pidió que ningún correo diga «responde este correo» y que el cliente cree un ticket: esa frase es la ÚNICA acción que le pedimos, y salía en gris claro de 12 px
(contraste 3,5:1, bajo el mínimo de 4,5:1): lo más débil del correo. Las plantillas guardadas en la base traen ese estilo «cocido» en su HTML, así que se corrige AL ENVIAR
(igual que `email_outlook_safe`), no plantilla por plantilla: así vale también para las que se editen o se siembren de nuevo.

`frase_ticket_legible(html)`: todo `<p>` que contenga «ticket de soporte con el botón de abajo» pasa a letra de 14 px, color #374151 (≥ 10:1 sobre blanco) y la parte
«Crea un ticket de soporte» en negrita. El resto del párrafo, su margen y todo lo demás quedan iguales. Módulo PURO, idempotente, nunca lanza.
"""
import re

from email_outlook_safe import _separar_estilo, _unir

_FRASE = re.compile(r"ticket de soporte con el bot[oó]n de abajo", re.IGNORECASE)
_PARRAFO = re.compile(r"<p\b([^>]*)>(.*?)</p>", re.IGNORECASE | re.DOTALL)
_NEGRITA = re.compile(r"(?<![>\w])([Cc]rea un ticket de soporte|[Cc]ree un ticket de soporte)(?=\s+con el bot)", re.IGNORECASE)


def _p(m):
    atributos, interior = m.group(1), m.group(2)
    if not _FRASE.search(interior):
        return m.group(0)
    sin, decls = _separar_estilo(atributos)
    nuevo = {"font-size": "14px", "color": "#374151", "line-height": "1.55"}
    decls = [(p, v) for p, v in decls if p not in nuevo] + list(nuevo.items())
    if "<strong" not in interior.lower():
        interior = _NEGRITA.sub(r"<strong>\1</strong>", interior, count=1)
    return f'<p{sin} style="{_unir(decls)}">{interior}</p>'


def frase_ticket_legible(html):
    """El HTML con la frase del ticket en letra de 14 px, oscura y con «Crea un ticket de soporte» en negrita. Nunca lanza."""
    if not html or not isinstance(html, str):
        return html
    try:
        return _PARRAFO.sub(_p, html)
    except Exception:
        return html

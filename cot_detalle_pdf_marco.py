# -*- coding: utf-8 -*-
"""Marco (encabezado y pie nativos) del PDF «Detalle de cálculo» de una cotización.

Daniel (6-nov-2026): «el cálculo que hicimos de la cotización no se apega para nada al diseño de la orden de trabajo».
El PDF de la OT (templates/mantenciones/ot_pdf.html) y el de la cotización llevan el mismo marco: banda negra a sangre con los dos logos
(SPHS en caja blanca + ILUS), el tipo de documento y el N° en rojo a la derecha, una franja roja/negra debajo, y un pie con borde rojo
(marca a la izquierda; documento y «Página X de Y» a la derecha). Este módulo arma ese marco para el Detalle de cálculo en A4 apaisado.

PURO (sin Flask ni base de datos): lo usan la ruta del PDF (tickets_module.py) y las pruebas. Es el MISMO HTML que Chromium pinta como
header_template / footer_template de page.pdf(); por eso todo el estilo va en línea (esas plantillas corren aisladas del documento).
"""
import html as _html

# Alto (mm) que ocupa el encabezado nativo ya MEDIDO con Playwright + PyMuPDF sobre el PDF real (caja del logo + padding + franja + el
# offset propio de Chromium) y el aire que se le deja. El @page de la plantilla debe decir lo mismo: en Chromium el @page es el que manda.
MARGEN_SUPERIOR_MM = 30
MARGEN_INFERIOR_MM = 14
MARGEN_LATERAL_MM = 0           # la banda va a sangre; el margen lateral real lo pone .sheet con su padding
LOGO_ALTO_MM = 11.0             # más chico que los 17,6 mm del PDF vertical: en apaisado cada milímetro de alto cuenta


def _esc(v):
    return _html.escape(str(v or ""), quote=True)


def encabezado_html(numero, empresa, logo_ilus_b64="", logo_shs_html=""):
    """Banda negra con los dos logos + tipo de documento + N° en rojo + cliente, y la franja roja/negra de abajo."""
    if logo_ilus_b64:
        ilus = (f'<img src="data:image/png;base64,{logo_ilus_b64}" '
                f'style="height:{LOGO_ALTO_MM}mm;max-width:{LOGO_ALTO_MM * 3.82:.1f}mm;object-fit:contain;">')
    else:
        ilus = ('<span style="font-weight:900;font-size:22px;color:#ffffff;letter-spacing:.02em;">ILUS'
                '<span style="color:#dc2626;">.</span></span>')
    return (
        '<div style="width:100%;font-family:Arial,Helvetica,sans-serif;">'
        '<div style="background:#0a0a0a;padding:3mm 12mm;box-sizing:border-box;display:flex;justify-content:space-between;'
        'align-items:center;-webkit-print-color-adjust:exact;print-color-adjust:exact;">'
        f'<div style="display:flex;align-items:center;gap:4mm;">{logo_shs_html}{ilus}</div>'
        '<div style="text-align:right;">'
        '<div style="font-size:13px;font-weight:800;color:#ffffff;letter-spacing:-.01em;">DETALLE DE CÁLCULO</div>'
        f'<div style="font-size:18px;font-weight:900;color:#dc2626;line-height:1.15;">N&#176; {_esc(numero)}</div>'
        '<div style="font-size:10px;color:#9ca3af;margin-top:1px;">DOCUMENTO INTERNO · SOLO SUPERADMIN</div>'
        '</div></div>'
        '<div style="height:2mm;background:linear-gradient(90deg,#dc2626 0 25%,#0a0a0a 25% 100%);'
        '-webkit-print-color-adjust:exact;print-color-adjust:exact;"></div>'
        '</div>'
    )


def pie_html(numero, empresa=""):
    """Pie con borde rojo: marca a la izquierda; documento, cliente y «Página X de Y» a la derecha (igual al de la OT)."""
    emp = (empresa or "").strip()
    if len(emp) > 48:
        emp = emp[:47].rstrip() + "…"
    cliente = f' · {_esc(emp)}' if emp else ""
    return (
        '<div style="width:100%;box-sizing:border-box;padding:0 12mm;font-family:Arial,Helvetica,sans-serif;font-size:9px;color:#6b7280;">'
        '<div style="display:flex;justify-content:space-between;align-items:center;border-top:1.5px solid #dc2626;padding-top:1.5mm;">'
        '<span><b style="color:#0a0a0a">ILUS Fitness</b> · www.ilusfitness.com · soportetec@sphs.cl</span>'
        f'<span><b style="color:#0a0a0a">{_esc(numero)}</b>{cliente} · Documento interno · '
        'Página <span class="pageNumber"></span> de <span class="totalPages"></span></span>'
        '</div></div>'
    )

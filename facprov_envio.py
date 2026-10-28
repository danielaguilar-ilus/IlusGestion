# -*- coding: utf-8 -*-
"""📧 Envío de lotes de Facturación de proveedor por correo — LÓGICA PURA (sin Flask ni base de datos), con pruebas en
tests/test_facprov_envio.py.

Daniel (2026-10-28, mostrando el correo que armaba a mano en Outlook con tres Excel de lotes y los montos de cada proveedor):
«necesito crear un envío de lotes seleccionados, con un mensaje y los Excel y los montos en el mail, y un cuerpo de correo bien
automático, que soy flojo».

Aquí vive todo lo que NO es plomería: el nombre corto de cada proveedor («LOGISTICA Y TRANSPORTES MILLING SPA» → «Milling»), la
validación de los correos (solo internos: el correo lleva plata), el asunto, el cuerpo en texto y en HTML con el mismo saludo, la lista
numerada de montos, el total y la firma, y el borrador `.eml` (con `X-Unsent: 1`, que Outlook abre como un correo SIN ENVIAR, con
destinatarios, asunto, cuerpo y los Excel ya puestos, para que Daniel lo revise y lo mande con su propia firma).
"""
import re
import unicodedata
from email.message import EmailMessage
from email import policy
from html import escape

DOMINIOS_INTERNOS = ("sphs.cl", "ilusfitness.com")
MAX_DESTINATARIOS = 10
MAX_LOTES = 30
MIME_XLSX = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

SALUDO_FALLBACK = "Hola,"
INTRO_DEFECTO = "Un gusto saludarte. Te envío el cierre de los proveedores con los siguientes montos:"
CIERRE_DEFECTO = "Cualquier duda me comentas."
DESPEDIDA = "Atte."

# Lo que se quita del nombre de una empresa para llamarla como la llama la gente («Milling», «DAP»). Si al quitarlo no queda nada, se usa el
# nombre completo: nunca se deja una etiqueta vacía.
_SOBRANTES = {
    "spa", "ltda", "limitada", "sa", "eirl", "e", "y", "de", "del", "la", "las", "los", "el", "en",
    "servicios", "servicio", "transportes", "transporte", "logistica", "comercial", "sociedad", "empresa", "hnos", "hermanos",
    "cia", "compania", "company", "inversiones", "ingenieria", "montajes", "montaje",
}
_RX_CORREO = re.compile(r"^[a-z0-9._%+\-]+@[a-z0-9\-]+(\.[a-z0-9\-]+)+$")


# ───────────────────────── formato ─────────────────────────
def clp(n):
    """$4.750.000 (sin decimales, punto de miles)."""
    try:
        return "$" + "{:,.0f}".format(float(n or 0)).replace(",", ".")
    except (TypeError, ValueError):
        return "$0"


def _sin_tildes(s):
    t = unicodedata.normalize("NFD", str(s or ""))
    return "".join(c for c in t if unicodedata.category(c) != "Mn")


def clave_proveedor(nombre):
    """La misma empresa escrita distinto («Milling», «MILLING  SpA») da la misma clave: minúsculas, sin tildes ni signos."""
    t = re.sub(r"[^a-z0-9]+", " ", _sin_tildes(nombre).lower()).strip()
    return t


def nombre_corto(nombre):
    """«LOGISTICA Y TRANSPORTES MILLING SPA» → «Milling»; «Transportes felcarm SPA» → «Felcarm»; «DAP Servicio» → «DAP». Siglas de hasta
    4 letras en mayúsculas se respetan («DAP»)."""
    original = re.sub(r"\s+", " ", str(nombre or "")).strip()
    if not original:
        return ""
    palabras = []
    for w in re.split(r"[\s,;/]+", original):
        base = re.sub(r"[^A-Za-zÁÉÍÓÚÜÑáéíóúüñ0-9]", "", w)
        if not base or _sin_tildes(base).lower() in _SOBRANTES:
            continue
        # sigla corta escrita en mayúsculas: se deja tal cual; lo demás va como nombre propio
        palabras.append(base if (base.isupper() and len(base) <= 4) else base[:1].upper() + base[1:].lower())
    return " ".join(palabras) or original


def etiqueta_de(proveedor, etiquetas_guardadas=None):
    """Cómo se llama el proveedor en el correo: la etiqueta que se guardó la última vez (si la hay) o su nombre corto."""
    guardada = (etiquetas_guardadas or {}).get(clave_proveedor(proveedor))
    return (guardada or "").strip() or nombre_corto(proveedor)


def etiquetas_a_guardar(lineas, auto_por_proveedor, etiquetas_previas, tope_chars=480):
    """Las etiquetas que la persona CAMBIÓ (distintas del nombre corto automático) para que la próxima vez salgan solas. `lineas` = [{proveedor,
    etiqueta}]. Se descartan las más antiguas si el JSON pasaría `tope_chars` (la regla donde se guarda mide 500)."""
    import json
    out = dict(etiquetas_previas or {})
    for ln in lineas:
        clave = clave_proveedor(ln.get("proveedor"))
        et = (ln.get("etiqueta") or "").strip()
        if not clave or not et:
            continue
        if et != auto_por_proveedor.get(clave, nombre_corto(ln.get("proveedor"))):
            out.pop(clave, None)
            out[clave] = et[:40]
        else:
            out.pop(clave, None)           # volvió al automático: ya no hace falta recordarla
    return json_acotado_dict(out, tope_chars)


def json_acotado_dict(d, tope_chars=480):
    """El mismo dict sin sus entradas más antiguas hasta que su JSON cabe en `tope_chars` (la regla donde se guarda mide 500)."""
    import json
    out = dict(d or {})
    while out and len(json.dumps(out, ensure_ascii=False)) > tope_chars:
        out.pop(next(iter(out)))           # la más antigua
    return out


def json_acotado(d, tope_chars=480):
    """Lo que se guarda en la regla: el JSON de `d`, acotado como `json_acotado_dict`. Vacío → '{}'."""
    import json
    return json.dumps(json_acotado_dict(d, tope_chars), ensure_ascii=False)


# ───────────────────────── correos ─────────────────────────
def correos_internos(entrada, dominios=DOMINIOS_INTERNOS):
    """→ (válidos, rechazados). Separa por «,», «;», espacios o saltos de línea, acepta «Nombre <correo>», pasa a minúsculas y no repite. Válido =
    forma de correo y dominio EXACTAMENTE uno de `dominios` (ni subdominios ni parecidos). El correo lleva montos de proveedores: solo equipo interno."""
    crudo = entrada if isinstance(entrada, (list, tuple)) else [entrada]
    partes = []
    for x in crudo:
        # «Nicolás Bastías <nicolas.bastias@sphs.cl>» → solo el correo (el nombre que lo antecede se descarta)
        s = re.sub(r"[^,;<>]*<([^<>]*)>", r" \1 ", str(x or ""))
        partes += re.split(r"[\s,;]+", s)
    validos, rechazados = [], []
    for x in partes:
        c = x.strip().strip("<>").lower()
        if not c:
            continue
        dominio = c.rsplit("@", 1)[-1] if "@" in c else ""
        if _RX_CORREO.match(c) and dominio in dominios and ".." not in c:
            if c not in validos:
                validos.append(c)
        elif c not in rechazados:
            rechazados.append(c)
    return validos, rechazados


def primer_nombre(nombre_completo):
    """«Nicolás Bastías» → «Nicolás»; un correo («nicolas.bastias@sphs.cl») → «Nicolas»."""
    t = re.sub(r"\s+", " ", str(nombre_completo or "")).strip()
    if "@" in t:
        t = t.split("@", 1)[0].split(".", 1)[0]
    return t.split(" ")[0][:1].upper() + t.split(" ")[0][1:] if t else ""


def saludo_defecto(nombre_completo, guardado=""):
    """Lo que se guardó la última vez («Nico,»); si no, «Hola Nicolás,»; si no hay nadie, «Hola,»."""
    g = (guardado or "").strip()
    if g:
        return g if g.endswith((",", ":", "!")) else g + ","
    pn = primer_nombre(nombre_completo)
    return f"Hola {pn}," if pn else SALUDO_FALLBACK


# ───────────────────────── el mensaje ─────────────────────────
def armar_lineas(lotes, etiquetas_guardadas=None):
    """De los lotes [{id, proveedor, monto, archivo…}] a las líneas del correo [{id, proveedor, etiqueta, monto, monto_txt}], en el orden dado."""
    out = []
    for lt in lotes:
        out.append({"id": lt.get("id"), "proveedor": lt.get("proveedor") or "", "monto": float(lt.get("monto") or 0),
                    "monto_txt": clp(lt.get("monto")), "etiqueta": etiqueta_de(lt.get("proveedor"), etiquetas_guardadas),
                    "archivo": lt.get("archivo") or ""})
    return out


def total_de(lineas):
    return sum(float(ln.get("monto") or 0) for ln in lineas)


def asunto_defecto(lineas, hoy):
    """«Cierre de proveedores — 3 lotes (28/10/2026)»; con un solo lote, «Cierre de proveedor — Milling (28/10/2026)»."""
    fecha = hoy.strftime("%d/%m/%Y")
    if len(lineas) == 1:
        return f"Cierre de proveedor — {lineas[0].get('etiqueta') or lineas[0].get('proveedor')} ({fecha})"
    return f"Cierre de proveedores — {len(lineas)} lotes ({fecha})"


def firma_de(nombre, cargo="", telefono="", correo=""):
    """Las líneas de la firma: «Atte.» y quien envía, con lo que se sepa de su ficha."""
    lineas = [DESPEDIDA]
    for x in (nombre, cargo, telefono, correo):
        x = re.sub(r"\s+", " ", str(x or "")).strip()
        if x:
            lineas.append(x)
    return lineas


def _parrafos(texto):
    return [p.strip() for p in re.split(r"\n\s*\n", str(texto or "").strip()) if p.strip()]


def armar_cuerpo(saludo, intro, lineas, cierre, firma, con_total=True):
    """El cuerpo del correo en texto plano y en HTML, con el mismo contenido. Todo lo que escribe una persona se escapa en el HTML."""
    firma = [str(x) for x in (firma or []) if str(x).strip()]
    n = len(lineas)
    tot = total_de(lineas)
    # ── texto ──
    t = []
    if (saludo or "").strip():
        t += [saludo.strip(), ""]
    if (intro or "").strip():
        t += [intro.strip(), ""]
    for i, ln in enumerate(lineas, 1):
        t.append(f"{i}. {ln.get('etiqueta') or ln.get('proveedor')} {ln.get('monto_txt') or clp(ln.get('monto'))}")
    if con_total and n > 1:
        t.append(f"Total {clp(tot)}")
    if lineas:
        t.append("")
    if (cierre or "").strip():
        t += [cierre.strip(), ""]
    t += firma
    texto = "\n".join(t).rstrip() + "\n"
    # ── HTML ──
    estilo = "font-family:Calibri,Arial,sans-serif;font-size:12pt;color:#1f2937;line-height:1.45"
    h = [f'<div style="{estilo}">']
    if (saludo or "").strip():
        h.append(f'<p style="margin:0 0 12px">{escape(saludo.strip())}</p>')
    for p in _parrafos(intro):
        h.append(f'<p style="margin:0 0 12px">{escape(p).replace(chr(10), "<br>")}</p>')
    if lineas:
        h.append('<ol style="margin:0 0 12px;padding-left:28px">')
        for ln in lineas:
            h.append(f'<li style="margin:2px 0">{escape(str(ln.get("etiqueta") or ln.get("proveedor") or ""))} '
                     f'<b>{escape(str(ln.get("monto_txt") or clp(ln.get("monto"))))}</b></li>')
        h.append("</ol>")
        if con_total and n > 1:
            h.append(f'<p style="margin:0 0 12px">Total <b>{escape(clp(tot))}</b></p>')
    for p in _parrafos(cierre):
        h.append(f'<p style="margin:0 0 12px">{escape(p).replace(chr(10), "<br>")}</p>')
    if firma:
        h.append('<p style="margin:16px 0 0">' + "<br>".join(escape(x) for x in firma) + "</p>")
    h.append("</div>")
    return {"texto": texto, "html": "".join(h)}


# ───────────────────────── el borrador para Outlook ─────────────────────────
def armar_eml(para, cc, asunto, texto, html, adjuntos):
    """Un `.eml` SIN ENVIAR: con `X-Unsent: 1` Outlook lo abre como un correo nuevo para revisar y mandar, con los destinatarios, el asunto, el
    cuerpo y los adjuntos ya puestos. `adjuntos` = [(nombre, bytes)]. Devuelve los bytes (CRLF, UTF-8)."""
    msg = EmailMessage(policy=policy.SMTP)
    msg["X-Unsent"] = "1"
    if para:
        msg["To"] = ", ".join(para)
    if cc:
        msg["Cc"] = ", ".join(cc)
    msg["Subject"] = asunto or ""
    msg.set_content(texto or "")
    msg.add_alternative(html or "", subtype="html")
    for nombre, contenido in adjuntos or []:
        maintype, subtype = MIME_XLSX.split("/", 1)
        msg.add_attachment(contenido, maintype=maintype, subtype=subtype, filename=nombre)
    return msg.as_bytes()


def nombre_borrador(hoy):
    return "cierre_proveedores_" + hoy.strftime("%d-%m-%Y") + ".eml"


# ───────────────────────── validación de lo que llega del navegador ─────────────────────────
def ids_validos(crudo, maximo=MAX_LOTES):
    """Los ids de lote que llegaron, como enteros únicos y en el mismo orden; lo que no sea un entero positivo se ignora. → (ids, excedido)."""
    out = []
    for x in crudo if isinstance(crudo, (list, tuple)) else []:
        try:
            n = int(x)
        except (TypeError, ValueError):
            continue
        if n > 0 and n not in out:
            out.append(n)
    return out[:maximo], len(out) > maximo

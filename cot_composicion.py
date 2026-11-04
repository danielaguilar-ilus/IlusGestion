"""Qué hay REALMENTE en una cotización de instalación: equipos, materiales y servicios (módulo puro, sin base de datos).

Daniel (30-oct-2026, COT-000195 de PHYSIOCORE HF SPA): el PDF decía «6 equipos considerados» e «Instalación de 6 equipo(s)», pero la
cotización era UN servicio («Instalación cable de fuerza», 1) más 5 unidades de un material («Cable de Fuerza 5mm Black»). El cliente
reclamó, con razón. El conteo sumaba las unidades de TODAS las líneas (5 + 1) y las llamaba equipos.

Aquí cada línea se clasifica en una de tres cosas y el PDF cuenta y redacta según lo que hay:
  · servicio  → mano de obra, despacho, descuento (SKU ZZ…/DE, o una descripción que empieza con «Instalación», «Mantención»…).
  · material  → accesorio o repuesto (clase del producto) o un insumo evidente (cable, piola, perno…).
  · equipo    → todo lo demás (lo que viene de un documento como producto).
La persona siempre puede escribir su propio alcance y recomendación en la cotización: si lo hace, manda lo suyo (solo se corrigen los
números). Pruebas: tests/test_cot_composicion.py.
"""
import re
import unicodedata

SERVICIO, MATERIAL, EQUIPO = "servicio", "material", "equipo"

_CLASES_MATERIAL = ("accesorio", "repuesto")
# 🏷️ 4-nov (Daniel, COT-000203: Aarón vio «metros de cable» en un EQUIPO, el «Freemotion Dual Cable Cross Negra»): el nombre de un equipo puede
# llevar la palabra «cable» o «cinta» sin ser un material. El categorizador del Catálogo (cat_clases_producto) ya sabe qué es cada producto:
# accesorio y repuesto son material; «otro» y «sin clasificar» son «no supe»; CUALQUIER otra clase (selectorizador, bicicleta, trotadora, racks,
# dual cable cross… y las que se agreguen en /catalogo/clases) es un EQUIPO. Con una clase de equipo, ni el nombre ni una palabra suelta lo
# vuelven material ni le ponen metros.
_CLASES_NO_EQUIPO = ("", "accesorio", "repuesto", "otro", "sin_clasificar")
_RE_SERVICIO = re.compile(
    r"^\s*(instalacion|desinstalacion|mantencion|mantenimiento|visita|despacho|flete|traslado|transporte|armado|puesta en marcha|"
    r"servicio|capacitacion|descuento|mano de obra|revision|diagnostico|reparacion|retiro)\b")
_RE_MATERIAL = re.compile(
    r"^\s*(cable|piola|cinta|perno|tornillo|golilla|tuerca|conector|enchufe|extension|prolongador|canaleta|tubo|abrazadera|"
    r"terminal|fusible|material|insumo|pegamento|silicona|cadena|correa)\b")


def _sin_acentos(txt):
    return "".join(c for c in unicodedata.normalize("NFD", str(txt or "")) if unicodedata.category(c) != "Mn").lower()


def _clase(clase):
    return str(clase or "").strip().lower()


def es_clase_de_equipo(clase):
    """¿El categorizador del Catálogo reconoce esta clase como un EQUIPO? (cualquier clase que no sea accesorio, repuesto, «otro» o vacía)."""
    return _clase(clase) not in _CLASES_NO_EQUIPO


def clasificar_linea(sku, descripcion, clase=None):
    """'servicio' | 'material' | 'equipo' para UNA línea de la cotización. Un servicio manda siempre; después manda la CLASE del categorizador
    (un equipo con «cinta» o «cable» en el nombre sigue siendo un equipo); solo sin clase útil decide el nombre."""
    s = str(sku or "").strip().upper()
    d = _sin_acentos(descripcion)
    if s.startswith("ZZ") or s == "DE" or _RE_SERVICIO.match(d):
        return SERVICIO
    if es_clase_de_equipo(clase):
        return EQUIPO
    if _clase(clase) in _CLASES_MATERIAL or _RE_MATERIAL.match(d):
        return MATERIAL
    return EQUIPO


_RE_METROS = re.compile(r"\b(cable|piola|cuerda|cordel|manguera)s?\b")
_RE_METROS_AL_INICIO = re.compile(r"^\s*(cable|piola|cuerda|cordel|manguera)s?\b")
_RE_NO_METROS = re.compile(r"\b(usb|hdmi|ethernet|cargador|adaptador|conector|poder|alimentacion|bluetooth|sensor|pulsometro)\b")


def unidad_de(descripcion, sku="", clase=None):
    """'m' si la línea se vende por metro (cable, piola, cuerda…), si no ''. Daniel (30-oct, COT-000195): «que diga metros en el cable».
    El sistema no guarda la unidad de medida de cada línea; se deduce de la clase y del nombre (y nunca de un servicio ZZ…):
      · clase de EQUIPO del categorizador → nunca («Freemotion Dual Cable Cross», COT-000203, no se vende por metro);
      · clase repuesto/accesorio → sí, aunque la palabra no vaya primera;
      · sin clase útil (vacía u «otro») → solo si el nombre EMPIEZA con la palabra («Cable de Fuerza 5mm Black»), no si va en el medio."""
    d = _sin_acentos(descripcion)
    if str(sku or "").strip().upper().startswith("ZZ") or _RE_SERVICIO.match(d):
        return ""
    if es_clase_de_equipo(clase):
        return ""
    if not _RE_METROS.search(d) or _RE_NO_METROS.search(d):
        return ""
    if _clase(clase) in _CLASES_MATERIAL:
        return "m"
    return "m" if _RE_METROS_AL_INICIO.match(d) else ""


def _cant_txt(n, unidad):
    return f"{n} {unidad}" if unidad else f"{n}"


def _plural(n, uno, varios):
    return f"{n} {uno if n == 1 else varios}"


def _lista(nombres, tope=3):
    nombres = [n for n in nombres if n]
    if not nombres:
        return ""
    extra = len(nombres) - tope
    corto = ", ".join(nombres[:tope])
    return corto + (f" y {extra} más" if extra > 0 else "")


def componer(items, dias_habiles=None):
    """items: [{sku, descripcion, cantidad, clase_producto}] → la composición de la cotización.

    Devuelve {equipos, materiales, servicios (unidades), n_equipos_lineas, categorias, nombres_*, cards, detalle_sub, solo_servicio}.
    `cards` es la lista [{num, lbl}] que pinta el PDF en vez del «Equipos considerados» de siempre."""
    eq, mat, srv = [], [], []
    for it in items or []:
        try:
            cant = int(float(it.get("cantidad") or 0))
        except (TypeError, ValueError):
            cant = 0
        if cant <= 0:
            continue
        tipo = clasificar_linea(it.get("sku"), it.get("descripcion"), it.get("clase_producto"))
        fila = {"nombre": str(it.get("descripcion") or it.get("sku") or "").strip(), "cant": cant,
                "clase": (str(it.get("clase_producto") or "").strip() or None),
                "unidad": unidad_de(it.get("descripcion"), it.get("sku"), it.get("clase_producto"))}
        {SERVICIO: srv, MATERIAL: mat, EQUIPO: eq}[tipo].append(fila)
    n_eq = sum(f["cant"] for f in eq)
    n_mat = sum(f["cant"] for f in mat)
    n_srv = len(srv)
    clases = {f["clase"] for f in eq if f["clase"]}
    categorias = len(clases) if clases else len(eq)
    cards = []
    if n_eq:
        cards.append({"num": n_eq, "lbl": "Equipos considerados"})
        cards.append({"num": categorias, "lbl": "Categorías de equipos"})
    if n_srv:
        cards.append({"num": n_srv, "lbl": "Servicio considerado" if n_srv == 1 else "Servicios considerados"})
    solo_metros = bool(mat) and all(f["unidad"] == "m" for f in mat)     # todo el material se vende por metro
    if n_mat:
        if solo_metros:
            cards.append({"num": f"{n_mat} m", "lbl": "de material"})
        else:
            cards.append({"num": n_mat, "lbl": "Unidad de material" if n_mat == 1 else "Unidades de material"})
    if dias_habiles:
        cards.append({"num": dias_habiles, "lbl": "Días hábiles estimados"})
    partes = []
    if n_eq:
        partes.append(_plural(n_eq, "equipo", "equipos"))
    if n_srv:
        partes.append(_plural(n_srv, "servicio", "servicios"))
    if n_mat:
        partes.append(f"{n_mat} m de material" if solo_metros else _plural(n_mat, "unidad de material", "unidades de material"))
    return {
        "equipos": n_eq, "materiales": n_mat, "servicios": n_srv, "categorias": categorias,
        "nombres_equipos": [f["nombre"] for f in eq], "nombres_materiales": [f"{f['nombre']} ({_cant_txt(f['cant'], f['unidad'] or 'u')})" for f in mat],
        "nombres_servicios": [f["nombre"] for f in srv],
        "solo_servicio": n_eq == 0, "cards": cards,
        "detalle_sub": (" · ".join(partes) + " · precios unitarios en UF") if partes else None,
    }


def textos_instalacion(comp, uf_info=False, previas=0):
    """Los textos automáticos de una Propuesta de Instalación según lo que hay: (parrafo, alcance, recomendacion, servicio_txt).
    Con equipos conserva la redacción de siempre (solo corrige el número); sin equipos habla de servicio y materiales."""
    if comp["equipos"]:
        alcance = [f"Instalación de {_plural(comp['equipos'], 'equipo', 'equipos')} en las dependencias del cliente."]
        if comp["materiales"]:
            alcance.append("Suministro de los materiales indicados en el detalle económico: " + _lista(comp["nombres_materiales"]) + ".")
        alcance += [
            "Desembalaje, armado y posicionamiento final según las condiciones del recinto.",
            "Nivelación, fijación y ajustes técnicos según especificación del fabricante.",
            "Pruebas de funcionamiento y calibración antes de la entrega.",
            "Retiro de embalajes y despeje del área de trabajo.",
        ]
        parrafo = ("La presente propuesta corresponde a la instalación de los equipos individualizados en el detalle adjunto, incluyendo "
                   "armado, nivelación y pruebas de funcionamiento antes de la entrega. Una instalación correcta asegura la operatividad y "
                   "seguridad del equipo desde el primer uso, y facilita la trazabilidad técnica para futuras mantenciones.")
        servicio = ("instalación, armado, nivelación y puesta en marcha de los equipos individualizados en el detalle adjunto.")
        reco_base = ["Registrar número de serie y fecha de puesta en marcha de cada equipo instalado.",
                     "Programar la primera mantención preventiva dentro de los primeros meses de uso.",
                     "Capacitar al personal del cliente en el uso y cuidado básico del equipo."]
        primera = "Primera instalación registrada para este cliente: se documentará la puesta en marcha de cada equipo como referencia técnica."
        recurrente = (f"Cliente recurrente: ya registra {previas} instalación(es) previa(s) con nosotros — se mantiene el mismo estándar "
                      "técnico en la puesta en marcha de los equipos.")
    else:
        que = _lista(comp["nombres_servicios"]) or "el servicio indicado"
        alcance = [f"Ejecución del servicio de instalación indicado en el detalle económico: {que}."]
        if comp["materiales"]:
            alcance.append("Suministro de los materiales indicados: " + _lista(comp["nombres_materiales"]) + ".")
        alcance += [
            "Conexión, fijación y ajustes técnicos según las condiciones del recinto.",
            "Pruebas de funcionamiento antes de la entrega.",
            "Retiro de residuos y despeje del área de trabajo.",
        ]
        parrafo = (f"La presente propuesta corresponde al servicio de instalación detallado en el cuadro adjunto ({que})"
                   + (", incluyendo el suministro de los materiales indicados" if comp["materiales"] else "")
                   + ", con pruebas de funcionamiento antes de la entrega. No considera la instalación de equipos adicionales a los "
                     "individualizados en el detalle.")
        servicio = "instalación de los servicios y materiales individualizados en el detalle adjunto."
        reco_base = ["Dejar registro de la fecha de ejecución y de los materiales instalados.",
                     "Programar una revisión de la instalación dentro de los primeros meses de uso.",
                     "Informar al personal del cliente cómo operar y cuidar lo instalado."]
        primera = "Primera instalación registrada para este cliente: se documentará lo instalado como referencia técnica."
        recurrente = (f"Cliente recurrente: ya registra {previas} instalación(es) previa(s) con nosotros — se mantiene el mismo estándar "
                      "técnico de ejecución.")
    if uf_info:
        parrafo += " Los precios están expresados en UF y se calculan con la UF vigente a la fecha del servicio."
    reco = [recurrente if previas else primera] + reco_base
    return parrafo, alcance, reco, servicio

# -*- coding: utf-8 -*-
"""Medidas por pieza y diagnóstico guiado «¿Qué le pasa?» de la barra de acciones de la OT (Daniel, 2026-10-20).

Pedido de Daniel (dueño): «que se vea inteligente, actuando como un técnico senior… diagnosticar en una desviación… y sobre todo en
las solicitudes hay que grabar datos: si es una piola, cuánto mide; si se rompió la cinta de trotadora, el ancho y el largo…
Imagínate que eres un técnico que no sabes nada y te va a llegar una solicitud: necesitas saber medidas y todo».

Este módulo es PURO (sin Flask, sin base de datos, sin red) para probarlo en milisegundos. Contiene:

  · las FICHAS de medidas (34 fichas + «otra pieza»): qué se mide de cada pieza, con qué unidad, en qué rango razonable, cuál es
    obligatoria, cómo se mide, qué foto sacar y qué hacer si no se puede medir. Una sola fuente de verdad: el servidor valida con
    estas mismas fichas y el teléfono las recibe tal cual en «repuestos-opciones»;
  · la validación: AVISA (rango razonable) y solo BLOQUEA lo imposible (cero, negativo, más de 5 veces el máximo) o lo que falta
    en una pieza CLAVE sin la salida «No pude medir»;
  · el resumen legible (la columna `medida` y las líneas que leen bodega y gestión) y la firma para el aprendizaje del catálogo;
  · el aprendizaje: dos medidas iguales del mismo modelo → «medida en terreno» SUGERIDA. Gestión la aprueba; nada se escribe solo
    en el catálogo (REGLA #32: el sistema sugiere, la persona decide);
  · el árbol de diagnóstico «¿Qué le pasa?» (una pregunta por pantalla, máximo 3, «Otra cosa» en cada una, atajo «Puede lastimar a
    alguien») que termina en una sugerencia. Nada se crea ni se marca solo.

Decisiones de Daniel (20-oct):
  1. Medidas OBLIGATORIAS solo en piezas clave: piola, cinta, pad, correa, pin y brazo de seguridad. Siempre existe «No pude medir»,
     que exige una foto de la pieza con la huincha al lado. Polea, rodamiento, motor y consola se identifican con la foto de su
     etiqueta. Las demás piezas tienen medidas OPCIONALES.
  2. Lo medido en terreno alimenta el catálogo del modelo (como ya hace la piola): tras 2 medidas iguales queda «sugerida» y gestión
     la aprueba.
  3. NO hay pie de metro: los diámetros se eligen con botones del valor más parecido y la foto con huincha los confirma.
  4. Elípticas y remos SÍ tienen sistema propio (piezas, fichas y camino de diagnóstico).

REGLA #19 y #26: aquí solo hay datos FÍSICOS (medidas, tipos, códigos grabados). Nunca un proveedor, un costo ni una compra.
"""
import json
import math
import re
import unicodedata

import ot_producto_estado as pe

MEDIDO_CON = (("huincha", "Medí con huincha"), ("catalogo", "Del catálogo o manual (no medí)"), ("no_pude", "No pude medir"))
MOTIVOS_NO_PUDE = (
    ("no_se_puede_sacar", "No se puede sacar la pieza"),
    ("no_hay_huincha", "No tengo huincha"),
    ("falta_trozo", "Falta un trozo o está cortada"),
    ("otro", "Otro motivo"),
)
# Dato que se pidió «Piezas clave»: obligatorias (con la salida «No pude medir»). Las demás son opcionales.
UMBRAL_SUGERIDA = 2          # medidas iguales del mismo modelo (de pedidos distintos) para que quede «sugerida»
FACTOR_IMPOSIBLE = 5         # más de 5 veces el máximo razonable = imposible (se bloquea)
MAX_TEXTO = 80


# ═══════════════════════════════ constructores de campos ═══════════════════════════════
def _num(k, t, u, mn, mx, obl=False, mas=False, c="", r=None, ap=True, tol=0.02, cond=None):
    return {"k": k, "t": t, "y": "num", "u": u, "mn": mn, "mx": mx, "obl": obl, "mas": mas, "c": c, "r": r, "ap": ap, "tol": tol,
            "cond": cond}


def _opcnum(k, t, u, valores, mn, mx, obl=False, mas=False, c="", r=None, ap=True, tol=0.02, cond=None):
    """Botones con el valor más parecido (ej. 4 · 5 · 6 mm) + «Otro» con número. Sin pie de metro (decisión de Daniel)."""
    op = [[v, (f"{_fmt(v)} {u}" + (f" · {extra}" if extra else ""))] for v, extra in valores]
    return {"k": k, "t": t, "y": "opcnum", "u": u, "op": op, "mn": mn, "mx": mx, "obl": obl, "mas": mas, "c": c, "r": r, "ap": ap,
            "tol": tol, "cond": cond}


def _opc(k, t, op, obl=False, mas=False, c="", r=None, ap=True, cond=None, ay=""):
    return {"k": k, "t": t, "y": "opc", "op": [list(x) for x in op], "obl": obl, "mas": mas, "c": c, "r": r, "ap": ap, "cond": cond,
            "ay": ay}


def _sino(k, t, obl=False, mas=False, c="", r=None, ap=False, cond=None):
    return {"k": k, "t": t, "y": "sino", "obl": obl, "mas": mas, "c": c, "r": r, "ap": ap, "cond": cond}


def _txt(k, t, obl=False, mas=False, c="", r=None, ap=False, cond=None, ph=""):
    return {"k": k, "t": t, "y": "txt", "obl": obl, "mas": mas, "c": c, "r": r, "ap": ap, "cond": cond, "ph": ph}


LADO = (("izq", "Izquierdo"), ("der", "Derecho"), ("ambos", "Los dos"))
_AYUDA_LADO = "Izquierdo = el de tu pie izquierdo, sentado en la bicicleta."
TERMINALES = (("bola", "Bola"), ("ojal", "Ojal"), ("esparrago", "Espárrago roscado"), ("tope", "Tope cilíndrico prensado"),
              ("gancho", "Gancho"), ("corte", "Corte simple"), ("otro", "Otro"))
ROSCAS_M = (("M6", "M6"), ("M8", "M8"), ("M10", "M10"), ("M12", "M12"), ("M16", "M16"), ("otra", "Otra"))
PIEZA_VIEJA = (("guardada", "La guardé"), ("no_se_pudo", "No se pudo sacar"), ("no_esta", "No la encuentro"))

# Campos que van en TODAS las fichas, siempre bajo «Más datos (opcional)».
_COMUNES = (
    _opc("pieza_vieja", "Pieza vieja", PIEZA_VIEJA, mas=True, ap=False,
         c="La pieza vieja es el molde de bodega: si la sacaste, guárdala."),
    _txt("cod_pieza", "Código o N° de pieza", mas=True, c="Solo si lo ves grabado. No lo inventes.", ph="Ej. A-1234"),
)


def _fmt(v):
    """Número a texto de Chile: 2,35 · 2.900 · 5."""
    try:
        f = float(v)
    except (TypeError, ValueError):
        return str(v)
    if f != f or f in (float("inf"), float("-inf")):
        return str(v)
    if f.is_integer():
        n = int(f)
        return f"{n:,}".replace(",", ".") if abs(n) >= 1000 else str(n)
    return f"{f:.2f}".rstrip("0").rstrip(".").replace(".", ",")


def _ficha(fid, tipo, titulo, ic, dib, campos, como, foto, sin, seg="", clave=False, corto=None, resumen=None, req_alt=None,
           etiqueta=False):
    campos = list(campos) + [dict(c) for c in _COMUNES]
    return {"id": fid, "tipo": tipo, "titulo": titulo, "corto": corto or titulo, "ic": ic, "dib": dib, "clave": clave,
            "campos": campos, "como": list(como), "foto": foto, "sin": sin, "seg": seg, "resumen": resumen or [],
            "req_alt": req_alt or [], "etiqueta": etiqueta}


# ═══════════════════════════════ las fichas ═══════════════════════════════
_FICHAS_LISTA = (
    # ── Familia A · cables y terminales ──
    _ficha("A1", "piola", "Piola o cable de acero", "piola", "piola", clave=True, corto="Piola",
           campos=[
               _num("largo_cm", "Largo total, punta a punta", "cm", 40, 800, obl=True, r="× {largo_cm} cm", tol=0.02,
                    c="Estirada en el suelo, apenas tensa, con los terminales incluidos."),
               _opcnum("diametro_mm", "Diámetro del cable", "mm", ((4, ""), (5, "3/16″"), (6, "1/4″")), 3, 10, obl=True,
                       r="Ø{diametro_mm} mm", c="Elige el botón más parecido. La foto con la huincha lo confirma."),
               _opc("term_pesas", "Terminal del lado de las pesas", TERMINALES, obl=True, r="{term_pesas}"),
               _opc("term_agarre", "Terminal del lado del agarre", TERMINALES, obl=True, r="{term_agarre}"),
               _sino("funda", "¿Lleva funda de plástico?", mas=True, r="funda"),
               _opc("rosca", "Rosca del espárrago", ROSCAS_M, mas=True,
                    cond={"k": "term_pesas", "in": ["esparrago"], "o": {"k": "term_agarre", "in": ["esparrago"]}}),
               _num("rosca_largo_mm", "Largo del espárrago", "mm", 10, 80, mas=True,
                    cond={"k": "term_pesas", "in": ["esparrago"], "o": {"k": "term_agarre", "in": ["esparrago"]}}),
           ],
           como=["Con el stack apoyado abajo, saca la piola y tiéndela recta en el suelo, apenas tensa.",
                 "Mide de punta a punta incluyendo los terminales (del borde de afuera de uno al borde de afuera del otro). "
                 "No midas siguiendo el recorrido en la máquina ni lo adivines.",
                 "Si está cortada, junta los pedazos y suma. Si falta un trozo, pasa un cordel por el mismo camino, estíralo y mide el cordel."],
           foto="La piola estirada con la huincha pegada y los dos terminales a la vista.",
           sin="Dinos por qué no pudiste. Sube una foto de la piola con la huincha o, si no se puede, de los dos terminales.",
           seg="Apoya el stack abajo con el pin en la placa más baja antes de soltar la piola.",
           resumen=[["Piola", "Ø{diametro_mm} mm", "× {largo_cm} cm"], ["{term_pesas} / {term_agarre}"]]),
    _ficha("A2", "mosqueton", "Mosquetón", "cable", "mosqueton", corto="Mosquetón",
           campos=[
               _num("largo_mm", "Largo total", "mm", 60, 140, r="{largo_mm} mm", c="Del ojo donde entra la piola a la punta de la boca."),
               _num("apertura_mm", "Apertura de la boca", "mm", 10, 40, r="boca {apertura_mm} mm", c="Lo máximo que se abre la boca."),
               _opc("tipo", "Tipo", (("resorte", "De resorte"), ("roscado", "Seguro roscado"), ("giratorio", "Giratorio")), r="{tipo}"),
               _num("carga_kg", "Carga estampada", "kg", 100, 1000, mas=True, c="Si viene grabada. Los de gimnasio traen ~300 kg."),
           ],
           como=["Largo: del ojo donde entra la piola a la punta de la boca.", "Apertura: lo máximo que se abre la boca."],
           foto="De frente, con la huincha al lado.", sin="Si es igual al de la otra piola, saca foto del que sigue puesto.",
           resumen=[["Mosquetón", "{largo_mm} mm", "boca {apertura_mm} mm"], ["{tipo}"]]),
    _ficha("A3", "agarre", "Agarre o manilla", "cable", "agarre", corto="Agarre",
           campos=[
               _opc("tipo", "Tipo", (("recta", "Barra recta"), ("v", "V"), ("cuerda", "Cuerda"), ("manilla", "Manilla individual"),
                                    ("dorsal", "Barra de dorsal"), ("tobillera", "Tobillera"), ("otro", "Otro")), r="{tipo}"),
               _num("largo_mm", "Largo, punta a punta", "mm", 100, 1300, r="{largo_mm} mm",
                    c="Largo del tubo, sin la cuerda. Manilla 100–220 · V 250–400 · recta 450–700 · dorsal 1000–1300."),
               _num("diam_tubo_mm", "Ø del tubo de agarre", "mm", 25, 38, r="Ø{diam_tubo_mm} mm", c="Huincha de canto junto al tubo."),
               _num("agujero_mm", "Ø del agujero donde se une", "mm", 8, 30, mas=True),
           ],
           como=["Largo: de punta a punta del tubo, sin la cuerda.", "Ø: pon la huincha de canto junto al tubo."],
           foto="El agarre completo con la huincha, más la unión de cerca.", sin="Foto con la huincha (obligatoria).",
           resumen=[["Agarre", "{tipo}", "{largo_mm} mm", "Ø{diam_tubo_mm} mm"]]),
    # ── Familia B · poleas, rodamientos y bujes ──
    _ficha("B1", "polea", "Polea", "polea", "polea", corto="Polea", etiqueta=True,
           campos=[
               _num("diam_mm", "Ø exterior de la rueda", "mm", 60, 180, r="Ø{diam_mm} mm", c="De borde a borde por el lado más ancho."),
               _txt("codigo_rod", "Código del rodamiento", c="Está grabado en el aro. Mira más abajo cómo verlo.", ph="Ej. 6203"),
               _num("ancho_mm", "Ancho total con rodamiento", "mm", 10, 35, mas=True, r="ancho {ancho_mm} mm"),
               _num("eje_mm", "Ø del agujero del eje", "mm", 8, 25, mas=True),
               _opc("material", "Material", (("nylon", "Nylon o plástico"), ("aluminio", "Aluminio"), ("acero", "Acero")), mas=True),
               _opc("ubicacion", "Ubicación", (("arriba", "Arriba"), ("abajo", "Abajo"), ("carro", "Carro"), ("brazo", "Brazo")), mas=True),
           ],
           como=["Con la huincha, mide de borde a borde por el lado más ancho de la rueda.",
                 "No saques la polea solo para medir. Antes de aflojar nada, pon el pin en la placa de abajo."],
           foto="La rueda de frente con la huincha por el diámetro. Una segunda foto del código del rodamiento, si se ve.",
           sin="Basta la foto, la ubicación y el modelo: bodega la identifica.",
           seg="Antes de aflojar nada, pon el pin en la placa de abajo.",
           resumen=[["Polea", "Ø{diam_mm} mm"], ["rod. {codigo_rod}"]]),
    _ficha("B2", "buje", "Buje o rodamiento", "buje", "rodamiento", corto="Buje/rodamiento", etiqueta=True,
           campos=[
               _opc("cual", "¿Cuál es?", (("rodamiento", "Rodamiento (tiene letras y números grabados)"), ("buje", "Buje (tubito liso)")),
                    r="{cual}"),
               _opc("codigo", "Código grabado", tuple((c, c) for c in ("608", "6000", "6001", "6002", "6200", "6201", "6202", "6203",
                                                                      "6204", "6205")) + (("otro", "Otro"),),
                    r="cód. {codigo}", c="Está en el aro de afuera, a veces bajo el sello de goma. Límpialo con un paño y fotografíalo con luz."),
               _num("int_mm", "Ø interior", "mm", 6, 40, r="Ø int {int_mm}", c="El eje que pasa por dentro.", cond={"vacio": "codigo"}),
               _num("ext_mm", "Ø exterior", "mm", 10, 62, r="ext {ext_mm}", cond={"vacio": "codigo"}),
               _num("ancho_mm", "Ancho (rodamiento) o largo (buje)", "mm", 5, 60, r="× {ancho_mm} mm", mas=True),
               _opc("material", "Material del buje", (("bronce", "Bronce"), ("plastico", "Plástico"), ("otro", "Otro")), mas=True),
           ],
           como=["El código está grabado en el aro de afuera, a veces bajo el sello de goma. Límpialo con un paño y fotografíalo de cerca con luz.",
                 "Si no hay código, mide el Ø interior (el eje que pasa por dentro), el Ø exterior y el ancho."],
           foto="El código de cerca (obligatoria si hay código). Si es buje: el buje viejo sobre la huincha y el pivote donde va.",
           sin="Foto del pivote donde va y el modelo: bodega define el kit.",
           resumen=[["{cual}", "cód. {codigo}", "Ø{int_mm} × {ext_mm} × {ancho_mm} mm"]]),
    # ── Familia C · pines, perillas y seguros ──
    _ficha("C1", "pin", "Pin selector", "pin", "pin", clave=True, corto="Pin selector",
           campos=[
               _opcnum("diam_mm", "Ø del eje", "mm", ((8, ""), (9.5, "3/8″"), (11, "7/16″"), (12.7, "1/2″")), 7, 13, obl=True,
                       r="Ø{diam_mm} mm", c="Elige el botón más parecido."),
               _num("largo_mm", "Largo útil", "mm", 60, 200, obl=True, r="× {largo_mm} mm", tol=0.03,
                    c="De la base de la cabeza a la punta."),
               _opc("cabeza", "Cabeza", (("t", "En T"), ("pomo", "Pomo redondo"), ("cordon", "Con cordón"), ("magnetico", "Magnético")),
                    obl=True, r="cabeza {cabeza}"),
               _num("cordon_cm", "Largo del cordón", "cm", 10, 60, mas=True, ap=False),
           ],
           como=["Con el pin sacado, mide solo el eje: de la base de la cabeza a la punta.",
                 "El grosor, con el botón más parecido y la huincha en la foto. Nunca pruebes con un tornillo o un clavo."],
           foto="El pin sobre la huincha con la cabeza visible. Si el pin no está: el agujero de la placa con la huincha y la varilla selectora.",
           sin="Dinos «igual al de otra máquina del mismo modelo» y sube una foto. Nada de «pin universal».",
           resumen=[["Pin selector", "Ø{diam_mm} mm", "× {largo_mm} mm"], ["{cabeza}"]]),
    _ficha("C2", "pin_seguro", "Pin de seguro, de altura o de seguridad", "pin", "pin", clave=True, corto="Pin de seguro",
           campos=[
               _opc("tipo", "Tipo", (("resorte", "Con resorte o bola"), ("chaveta", "Chaveta (R-clip)"), ("anillo", "Recto con anillo"),
                                    ("cordon", "Con cordón")), obl=True, r="{tipo}"),
               _opcnum("diam_mm", "Ø", "mm", ((6, ""), (8, ""), (10, ""), (12, ""), (16, "5/8″"), (25, "1″")), 5, 26, obl=True,
                       r="Ø{diam_mm} mm", c="En un rack los agujeros son de 5/8″ o 1″."),
               _num("largo_mm", "Largo", "mm", 30, 250, obl=True, r="× {largo_mm} mm", tol=0.03),
           ],
           como=["Igual que el pin selector: mide el eje de la base de la cabeza a la punta.",
                 "En un rack: mide el agujero del poste por dentro y la distancia de centro a centro entre dos agujeros seguidos."],
           foto="El pin sobre la huincha y el agujero del poste o pivote donde va.",
           sin="Foto y modelo del equipo.", resumen=[["Pin", "{tipo}", "Ø{diam_mm} mm", "× {largo_mm} mm"]]),
    _ficha("C3", "perilla", "Perilla o pin de ajuste", "pin", "pin", corto="Perilla / pin de ajuste",
           campos=[
               _opc("para_que", "¿Para qué sirve?", (("asiento", "Ajusta el asiento"), ("respaldo", "Ajusta el respaldo"),
                                                     ("peso", "Fija el peso"), ("brazo", "Fija un brazo"), ("otro", "Otro")),
                    r="{para_que}", ap=False),
               _opc("tipo", "Tipo", (("jalar", "De jalar, con pin y resorte"), ("roscada", "Roscada, de apriete"), ("palanca", "Palanca")),
                    r="{tipo}"),
               _num("vastago_mm", "Ø del vástago", "mm", 6, 14, r="Ø{vastago_mm} mm"),
               _num("largo_vastago_mm", "Largo del vástago que entra", "mm", 20, 120, r="× {largo_vastago_mm} mm"),
               _opc("rosca", "Rosca de la perilla", (("M8", "M8"), ("M10", "M10"), ("M12", "M12"), ("M16", "M16"), ("nose", "No sé")), mas=True),
           ],
           como=["Saca la perilla. Si es de jalar: jálala y mide cuánto sale el pin y su grosor.",
                 "La rosca no la midas: elige «No sé» y deja la huincha en la foto."],
           foto="La perilla completa junto a la huincha y el hueco donde entra.",
           sin="«No sé» está permitido. Foto y para qué sirve: bodega la identifica por modelo y foto.",
           resumen=[["Perilla", "{para_que}", "Ø{vastago_mm} mm", "× {largo_vastago_mm} mm"]]),
    # ── Familia D · pads, rodillos, niveladores y topes ──
    _ficha("D1", "pad", "Pad o tapiz", "pad", "pad", clave=True, corto="Pad",
           campos=[
               _num("largo_mm", "Largo", "mm", 200, 1300, obl=True, r="{largo_mm}", tol=0.03,
                    c="Asiento 250–450 · respaldo 400–1300. Por el borde más largo de la cara que se ve."),
               _num("ancho_mm", "Ancho", "mm", 100, 500, obl=True, r="× {ancho_mm} mm", tol=0.03, c="Por el borde más ancho."),
               _opc("que_pide", "¿Qué se pide?", (("funda", "Solo la funda"), ("completo", "El pad completo (espuma + base)")), obl=True,
                    r="{que_pide}"),
               _opc("cual", "¿Cuál es?", (("asiento", "Asiento"), ("respaldo", "Respaldo"), ("pecho", "Apoyo de pecho"),
                                          ("brazos", "Apoyabrazos"), ("rodillo", "Rodillo de apoyo"), ("otro", "Otro")), r="{cual}"),
               _opc("forma", "Forma", (("rect", "Rectangular"), ("redond", "Esquinas redondeadas"), ("curvo", "Curvo o con ángulo")), mas=True),
               _opc("fijacion", "Fijación", (("tornillos", "Tornillos por abajo"), ("pernos", "Pernos de la base"), ("velcro", "Velcro o pegado"),
                                              ("desliza", "Se desliza en el tubo")), mas=True),
               _num("sep_mm", "Separación entre agujeros (centro a centro)", "mm", 50, 1000, mas=True,
                    cond={"k": "fijacion", "in": ["tornillos", "pernos"]}),
               _num("espesor_mm", "Espesor", "mm", 25, 100, mas=True),
           ],
           como=["Pon la huincha sobre la cara que se ve: el largo por el borde más largo y el ancho por el más ancho.",
                 "El espesor, de costado. Si tiene base con agujeros, mide de centro a centro."],
           foto="Desde arriba con la huincha al largo. Si pides el pad completo, una segunda foto desde atrás con los agujeros.",
           sin="Foto con la huincha (obligatoria) o «igual al de otro equipo» con el modelo.",
           resumen=[["Pad", "{cual}", "{largo_mm}", "× {ancho_mm} mm"], ["{que_pide}"]]),
    _ficha("D2", "rodillo_apoyo", "Rodillo apoya piernas (espuma)", "pad", "rodillo", corto="Rodillo apoya piernas",
           campos=[
               _num("largo_mm", "Largo", "mm", 100, 400, r="{largo_mm} mm", c="De extremo a extremo."),
               _num("diam_mm", "Ø exterior", "mm", 50, 110, r="Ø{diam_mm} mm", c="Mira el rodillo de punta."),
               _num("hueco_mm", "Ø del hueco interior", "mm", 18, 40, mas=True),
           ],
           como=["Largo: de extremo a extremo. Ø: mira el rodillo de punta."],
           foto="De costado con la huincha y de punta para ver el hueco.", sin="Si es igual al del otro lado, dilo y saca foto.",
           resumen=[["Rodillo", "{largo_mm} mm", "Ø{diam_mm} mm"]]),
    _ficha("D3", "nivelador", "Nivelador", "tope", "nivelador", corto="Nivelador",
           campos=[
               _opc("rosca", "Rosca", ROSCAS_M, r="rosca {rosca}", c="Pruébala con una tuerca o un perno conocido."),
               _num("largo_mm", "Largo del vástago roscado", "mm", 20, 100, r="vástago {largo_mm} mm"),
               _num("base_mm", "Ø de la base", "mm", 25, 80, r="base Ø{base_mm} mm"),
               _opc("base", "Base", (("fija", "Fija"), ("rotula", "Con rótula (pivotante)")), r="{base}"),
           ],
           como=["Largo del vástago roscado y Ø de la base. La rosca, probándola con una tuerca."],
           foto="El nivelador sobre la huincha.", sin="Foto y modelo del equipo.",
           resumen=[["Nivelador", "{rosca}", "vástago {largo_mm} mm", "base Ø{base_mm} mm"]]),
    _ficha("D4", "tope_goma", "Tope de goma o taco", "tope", "tope", corto="Tope de goma",
           campos=[
               _opc("donde", "¿Dónde va?", (("stack", "Tope del stack"), ("brazo", "Brazo o palanca"), ("asiento", "Asiento"),
                                           ("piso", "Apoyo en el piso")), r="{donde}", ap=False),
               _opc("forma", "Forma", (("cilindrico", "Cilíndrico"), ("conico", "Cónico"), ("cuadrado", "Cuadrado")), r="{forma}"),
               _num("diam_mm", "Ø o lado", "mm", 15, 100, r="Ø{diam_mm} mm"),
               _num("alto_mm", "Alto", "mm", 8, 80, r="alto {alto_mm} mm"),
               _opc("fijacion", "Fijación", (("tornillo", "Tornillo"), ("pegado", "Pegado"), ("presion", "A presión")), mas=True),
           ],
           como=["Mide el Ø o el lado y el alto, con la huincha de canto. Si está roto, junta los pedazos."],
           foto="El tope con la huincha y el hueco donde va.", sin="Foto del hueco y modelo del equipo.",
           resumen=[["Tope de goma", "{forma}", "Ø{diam_mm} mm", "alto {alto_mm} mm"]]),
    # ── Familia E · trotadora ──
    _ficha("E1", "cinta", "Cinta (banda) de trotadora", "cinta", "cinta", clave=True, corto="Cinta",
           campos=[
               _num("ancho_mm", "Ancho de la cinta", "mm", 400, 620, obl=True, r="{ancho_mm}", tol=0.015,
                    c="De borde a borde, donde esté más ancha. Ej. 420, 520, 559."),
               _num("vuelta_mm", "Largo de la VUELTA COMPLETA", "mm", 2300, 3600, obl=True, r="× {vuelta_mm} mm (vuelta completa)", tol=0.015,
                    c="Todo el contorno, no la superficie para pisar. Ej. 2.510, 2.900, 3.020, 3.264."),
               _opc("espesor", "Espesor", (("liviana", "Liviana (1 capa, ~1,4–1,9 mm)"), ("comercial", "Comercial (2 capas, ~2,0–2,9 mm)"),
                                           ("nose", "No sé")), mas=True, r="{espesor}"),
               _num("pisar_mm", "Largo de la superficie para pisar", "mm", 1100, 1700, mas=True, ap=False,
                    c="Lo que dice el manual (ej. «20×60 pulgadas») casi siempre es esto, NO la vuelta."),
               _txt("codigo", "Código impreso en el borde interior", mas=True, ph="Ej. TB-2900"),
           ],
           como=["Ancho: de borde a borde donde esté más ancha.",
                 "Vuelta completa: pasa un cordel por donde pasa la cinta (alrededor de los dos rodillos), tenso. Marca dónde se juntan "
                 "las puntas, sácalo, estíralo y mide: ese largo es la vuelta. Si la cinta está cortada, tiéndela estirada y mide de punta a punta.",
                 "Ojo: lo que dice el manual (ej. «20×60 pulgadas») casi siempre es la superficie para pisar, NO la vuelta. Aquí anotamos la vuelta."],
           foto="La cinta (o el tramo roto) con la huincha cruzando el ancho. Opcional: el reverso o la costura.",
           sin="Usa «Medida del modelo» y sube la foto de la etiqueta del equipo. Una cinta gastada se estira y la nueva puede salir algo más corta.",
           resumen=[["Cinta", "{ancho_mm}", "× {vuelta_mm} mm (vuelta completa)"]]),
    _ficha("E2", "tabla", "Tabla (deck)", "tabla", "tabla", corto="Tabla",
           campos=[
               _opc("que_pasa", "¿Qué le pasa?", (("hundida", "Hundida"), ("rota", "Rota"), ("pulida", "Pulida o gastada")), r="{que_pasa}", ap=False),
               _num("largo_mm", "Largo", "mm", 1100, 1800, r="{largo_mm}", c="Por un borde visible. No sueltes la cinta solo para medir."),
               _num("ancho_mm", "Ancho", "mm", 380, 650, r="× {ancho_mm} mm"),
               _num("espesor_mm", "Espesor", "mm", 12, 30, mas=True),
           ],
           como=["Largo y ancho por un borde visible. No sueltes la cinta solo para medir."],
           foto="La zona dañada con la huincha.", sin="Se usa «medida del modelo». Foto del daño.",
           resumen=[["Tabla", "{largo_mm}", "× {ancho_mm} mm"], ["{que_pasa}"]]),
    _ficha("E3", "rodillo", "Rodillo de trotadora", "rodillo", "rodillo", corto="Rodillo",
           campos=[
               _opc("cual", "¿Cuál?", (("delantero", "Delantero (el del motor)"), ("trasero", "Trasero")), r="{cual}"),
               _num("diam_mm", "Ø del tubo", "mm", 45, 100, r="Ø{diam_mm} mm"),
               _num("largo_mm", "Largo total con ejes", "mm", 450, 720, r="{largo_mm} mm"),
               _num("eje_mm", "Ø del eje", "mm", 12, 25, mas=True),
               _sino("canales", "¿Lleva canales (poly-V) para la correa?", mas=True, r="con canales"),
           ],
           como=["Si no se puede sacar sin desarmar, no se mide: se pide por modelo.",
                 "Un video de 10 segundos del ruido ayuda más que las medidas."],
           foto="El rodillo desmontado con la huincha o, si no, la zona y la etiqueta del equipo.",
           sin="Por defecto: «medida del modelo» con el modelo y la serie del equipo.",
           resumen=[["Rodillo", "{cual}", "Ø{diam_mm} mm", "× {largo_mm} mm"]]),
    _ficha("E4", "motor", "Motor (tracción o inclinación)", "motor", "placa", corto="Motor", etiqueta=True,
           campos=[
               _opc("cual", "¿Cuál?", (("traccion", "De tracción (el grande, mueve la cinta)"),
                                       ("inclinacion", "De inclinación (el chico, sube la plataforma)")), r="{cual}", ap=False),
               _txt("placa", "Fabricante y código de la placa", c="Se LEE, no se mide. Con el equipo desenchufado, fotografía la placa de cerca y derecha.",
                    ph="Lo que diga la placa", r="placa {placa}"),
               _num("potencia_hp", "Potencia", "HP", 0.5, 7, r="{potencia_hp} HP", ap=False),
               _num("voltaje_v", "Voltaje", "V", 90, 240, r="{voltaje_v} V", ap=False),
               _opc("corriente", "Tipo de corriente", (("dc", "DC"), ("ac", "AC")), mas=True, ap=False),
               _num("rpm", "RPM", "rpm", 1500, 6500, mas=True, ap=False),
           ],
           como=["No se mide: se LEE. Con el equipo desenchufado, busca la placa del motor y fotografíala de cerca, derecha y con luz.",
                 "Escribe lo que se lea. No abras tapas eléctricas."],
           foto="La placa de cerca (obligatoria) y el motor con su polea.",
           sin="Si la placa no se lee: foto y modelo y serie del equipo. No abras tapas eléctricas.",
           seg="Con el equipo desenchufado. No abras tapas eléctricas.",
           resumen=[["Motor", "{cual}", "{potencia_hp} HP", "{voltaje_v} V"]]),
    _ficha("E5", "llave", "Llave magnética de emergencia", "llave", "llave", corto="Llave de emergencia",
           campos=[
               _opc("forma", "Forma", (("redonda", "Redonda"), ("rect", "Rectangular"), ("clip", "Con clip o pinza")), r="{forma}"),
               _sino("cordon", "¿Con cordón o clip?", r="con cordón"),
               _num("iman_mm", "Ø o ancho del imán", "mm", 15, 45, mas=True),
           ],
           como=["Mide el imán por su parte más ancha, o el hueco de la consola donde entra. Nunca la reemplaces con un imán suelto."],
           foto="La llave vieja y el receptáculo de la consola.", sin="Foto del receptáculo y modelo del equipo.",
           seg="Sin llave la trotadora no parte: nunca se puentea.", resumen=[["Llave", "{forma}"]]),
    _ficha("E6", "consola", "Consola o placa controladora", "consola", "placa", corto="Consola / placa", etiqueta=True,
           campos=[
               _txt("modelo", "Marca y modelo de la consola", c="Está en la etiqueta de atrás.", ph="Marca y modelo", r="{modelo}"),
               _opc("que_hace", "¿Qué hace?", (("no_enciende", "No enciende"), ("blanco", "Pantalla en blanco o con rayas"),
                                              ("botones", "Botones sin respuesta"), ("codigo", "Muestra un código de error")),
                    r="{que_hace}", ap=False),
               _txt("codigo_error", "Código de error", c="Tal como sale en pantalla.", ph="Ej. E06", r="error {codigo_error}",
                    cond={"k": "que_hace", "in": ["codigo"]}),
               _opc("alimentacion", "Alimentación", (("enchufe", "Enchufe"), ("pilas", "Pilas"), ("auto", "Autoalimentada (bicicleta)")), ap=False),
               _txt("codigo_placa", "Código impreso en la placa", mas=True, c="Solo si abriste la tapa de la placa y estás autorizado."),
               _opc("luces", "Luces de la placa", (("apagadas", "Apagadas"), ("verde", "Verde fijo"), ("rojo", "Rojo o parpadea")), mas=True, ap=False),
           ],
           como=["Se identifica, no se mide: modelo (etiqueta de atrás), código de error tal como sale y alimentación.",
                 "Solo se abre la tapa de la placa si estás autorizado; si no, dilo."],
           foto="La consola encendida con el mensaje o apagada con la etiqueta de atrás.",
           sin="Modelo y serie del equipo. Sin abrir tapas.", seg="Con el equipo desenchufado. No abras tapas eléctricas.",
           resumen=[["Consola", "{modelo}"], ["{que_hace}", "error {codigo_error}"]]),
    # ── Familia F · bicicleta, elíptica y remo ──
    _ficha("F1", "correa", "Correa de transmisión o del motor", "correa", "correa", clave=True, corto="Correa",
           req_alt=[["codigo"], ["canales", "largo_mm"]],
           campos=[
               _opc("tipo", "Tipo", (("canales", "Con canales (poly-V)"), ("dentada", "Dentada"), ("plana", "Plana")), obl=True, r="{tipo}"),
               _txt("codigo", "Código impreso en el lomo", c="Ej. 6PJ508 = 6 canales, perfil PJ, 508 mm. Léelo primero.",
                    ph="Ej. 6PJ508", r="cód. {codigo}", ap=True),
               _num("canales", "Nº de canales (cuéntalos)", "canales", 3, 14, r="{canales} canales", c="Las rayitas del lado de adentro.",
                    cond={"vacio": "codigo"}, tol=0.0),
               _num("largo_mm", "Largo de la vuelta", "mm", 300, 2300, r="× {largo_mm} mm", tol=0.02,
                    c="Con un cordel alrededor de las dos poleas, tenso, por dentro.", cond={"vacio": "codigo"}),
               _opc("perfil", "Perfil", (("PJ", "PJ"), ("PK", "PK"), ("PL", "PL"), ("J", "J"), ("K", "K"), ("L", "L"), ("nose", "No sé")),
                    mas=True, cond={"vacio": "codigo"}),
               _num("ancho_mm", "Ancho", "mm", 7, 35, mas=True, ap=False, c="Cruce: el perfil PJ mide cerca de 2,34 mm por canal."),
           ],
           como=["Lee primero lo impreso en el lomo (ej. 6PJ508 = 6 canales, perfil PJ, 508 mm).",
                 "Si no se lee: cuenta los canales (las rayitas del lado de adentro), mide el ancho y mide el largo con un cordel alrededor de "
                 "las dos poleas, tenso, por dentro."],
           foto="La correa con el código a la vista o desplegada con la huincha.",
           sin="Foto del código y modelo del equipo. En algunas elípticas se necesita el largo exacto.",
           resumen=[["Correa", "{tipo}", "cód. {codigo}"], ["{canales} canales", "× {largo_mm} mm"]]),
    _ficha("F2", "pedal", "Pedal", "pedal", "pedal", corto="Pedal",
           campos=[
               _opc("lado", "¿De qué lado?", LADO, r="lado {lado}", ap=True, ay=_AYUDA_LADO),
               _opc("rosca", "Rosca del eje", (("9/16", "9/16″"), ("1/2", "1/2″"), ("nose", "No sé")), r="rosca {rosca}",
                    c="La más común en bicis de gimnasio es 9/16″. El pedal izquierdo trae la rosca al revés."),
               _opc("tipo", "Tipo", (("correa", "Con correa o amarra"), ("plano", "Plano"), ("jaula", "Jaula"), ("zapatilla", "Zapatilla de ciclismo"),
                                    ("plataforma", "Plataforma de elíptica")), r="{tipo}"),
           ],
           como=["Mira de qué lado va, sentado en la bici. El izquierdo trae la rosca al revés.", "Fotografía el eje roscado."],
           foto="El eje roscado y la cara del pedal.", sin="«Igual al de la otra pierna»: el otro pedal sirve de muestra.",
           resumen=[["Pedal", "{lado}", "{tipo}", "rosca {rosca}"]]),
    _ficha("F3", "biela", "Biela", "biela", "biela", corto="Biela",
           campos=[
               _opc("lado", "¿De qué lado?", (("izq", "Izquierdo"), ("der", "Derecho")), r="lado {lado}", ay=_AYUDA_LADO),
               _num("largo_mm", "Largo entre centros", "mm", 150, 190, r="{largo_mm} mm entre centros", tol=0.02,
                    c="Del centro del agujero del eje central al centro del agujero del pedal. No uses el largo total."),
               _opc("union", "Unión al eje central", (("cuadrado", "Cuadrado cónico"), ("estriado", "Estriado"), ("chaveta", "Con chaveta"),
                                                        ("nose", "No sé")), r="{union}"),
               _txt("grabado", "Largo grabado en la biela", mas=True, c="Mira la cara de atrás.", ph="Ej. 170"),
           ],
           como=["Largo: del centro del agujero del eje central al centro del agujero del pedal. No uses el largo total.",
                 "Mira si el largo viene grabado en la cara de atrás."],
           foto="La biela completa con la huincha entre centros y el agujero del eje.", sin="Foto, modelo y largo grabado.",
           resumen=[["Biela", "{lado}", "{largo_mm} mm entre centros", "{union}"]]),
    _ficha("F4", "sillin", "Sillín o poste", "sillin", "poste", corto="Sillín / poste",
           campos=[
               _opc("que", "¿Qué es?", (("sillin", "Sillín"), ("poste_sillin", "Poste del sillín"), ("manubrio", "Manubrio"),
                                        ("poste_manubrio", "Poste del manubrio")), r="{que}", ap=False),
               _num("diam_tubo_mm", "Ø del tubo del poste", "mm", 22, 40, r="Ø{diam_tubo_mm} mm", c="Huincha de canto sobre el tubo.",
                    cond={"k": "que", "in": ["poste_sillin", "poste_manubrio"]}),
               _num("largo_mm", "Largo del poste", "mm", 150, 500, mas=True, r="× {largo_mm} mm"),
               _opc("fijacion", "Fijación del sillín", (("rieles", "Rieles estándar"), ("placa", "Placa propia con tornillos")), mas=True),
           ],
           como=["Ø del tubo del poste con la huincha de canto; largo del poste."],
           foto="La pieza con la huincha y la unión.", sin="Foto y modelo del equipo.",
           resumen=[["{que}", "Ø{diam_tubo_mm} mm", "× {largo_mm} mm"]]),
    _ficha("F5", "rueda", "Rodillo o rueda", "polea", "polea", corto="Rodillo / rueda",
           campos=[
               _opc("cual", "¿Cuál es?", (("carril", "Rodillo del carril (elíptica)"), ("pedal", "Rueda del pedal o del brazo (elíptica)"),
                                          ("carro", "Rodillo del carro (remo)"), ("otro", "Otro")), r="{cual}", ap=False),
               _num("diam_mm", "Ø exterior", "mm", 20, 120, r="Ø{diam_mm} mm", c="De borde a borde, con la huincha."),
               _num("ancho_mm", "Ancho", "mm", 8, 60, r="ancho {ancho_mm} mm"),
               _num("eje_mm", "Ø del eje o del agujero", "mm", 6, 25, mas=True),
               _opc("material", "Material", (("goma", "Goma"), ("plastico", "Plástico"), ("aluminio", "Aluminio"), ("otro", "Otro")), mas=True),
           ],
           como=["Mide el Ø exterior de borde a borde y el ancho. Si no se puede sacar, dilo y saca foto del lugar."],
           foto="La rueda de frente con la huincha por el diámetro.", sin="Foto del lugar, ubicación y modelo del equipo.",
           resumen=[["Rodillo/rueda", "{cual}", "Ø{diam_mm} mm", "ancho {ancho_mm} mm"]]),
    _ficha("F6", "brazo_eliptica", "Brazo o manija de elíptica", "pedal", "tubo", corto="Brazo / manija",
           campos=[
               _opc("cual", "¿Cuál es?", (("brazo", "Brazo móvil"), ("manija", "Manija fija"), ("poste", "Poste o soporte")), r="{cual}", ap=False),
               _opc("lado", "¿De qué lado?", LADO, r="lado {lado}", ay=_AYUDA_LADO),
               _num("largo_mm", "Largo", "mm", 300, 1500, r="{largo_mm} mm", c="Entre los centros de los dos agujeros de unión."),
               _num("diam_tubo_mm", "Ø del tubo", "mm", 20, 45, r="Ø{diam_tubo_mm} mm"),
               _opc("union", "Unión", (("pasador", "Pasador"), ("perno", "Perno"), ("nose", "No sé")), mas=True),
           ],
           como=["Largo entre los centros de los dos agujeros de unión. Ø del tubo con la huincha de canto."],
           foto="La pieza con la huincha y la unión.", sin="Foto y modelo del equipo.",
           resumen=[["{cual}", "{lado}", "{largo_mm} mm", "Ø{diam_tubo_mm} mm"]]),
    # ── Remo ──
    _ficha("R1", "tiron", "Cinta o cuerda de tirón (remo)", "cable", "piola", corto="Cinta / cuerda de tirón",
           campos=[
               _opc("tipo", "Tipo", (("cinta", "Cinta plana"), ("cuerda", "Cuerda"), ("cadena", "Cadena")), r="{tipo}"),
               _num("largo_cm", "Largo total", "cm", 150, 600, r="{largo_cm} cm", c="Estirada en el suelo, de punta a punta."),
               _num("ancho_mm", "Ancho (cinta) o Ø (cuerda)", "mm", 5, 60, r="{ancho_mm} mm"),
               _opc("terminal", "Terminal", (("mango", "Mango"), ("gancho", "Gancho"), ("lazo", "Lazo"), ("otro", "Otro")), mas=True),
           ],
           como=["Estírala en el suelo y mide de punta a punta. El ancho de la cinta o el Ø de la cuerda, con la huincha de canto."],
           foto="La cinta o cuerda estirada con la huincha.", sin="Foto y modelo del equipo.",
           resumen=[["Tirón", "{tipo}", "{largo_cm} cm", "{ancho_mm} mm"]]),
    _ficha("R2", "carro", "Asiento o carro (remo)", "sillin", "pad", corto="Asiento / carro",
           campos=[
               _opc("que_pide", "¿Qué se pide?", (("asiento", "Solo el asiento o tapiz"), ("carro", "El carro completo")), r="{que_pide}", ap=False),
               _num("largo_mm", "Largo", "mm", 200, 500, r="{largo_mm}", c="Por el borde más largo."),
               _num("ancho_mm", "Ancho", "mm", 200, 500, r="× {ancho_mm} mm"),
               _opc("fijacion", "Fijación", (("tornillos", "Tornillos"), ("pernos", "Pernos"), ("riel", "Se desliza en el riel")), mas=True),
           ],
           como=["Largo por el borde más largo y ancho por el más ancho, con la huincha."],
           foto="Desde arriba con la huincha y, si pides el carro, desde abajo con los rodillos.", sin="Foto y modelo del equipo.",
           resumen=[["{que_pide}", "{largo_mm}", "× {ancho_mm} mm"]]),
    # ── Familia G · rack, cargas libres y stack ──
    _ficha("G1", "brazo_seguridad", "Brazo de seguridad o J-hook", "cuerno", "poste", clave=True, corto="Brazo de seguridad",
           campos=[
               _opc("seccion", "Sección del poste", (("2x2", "2×2″"), ("2x3", "2×3″"), ("3x3", "3×3″"), ("otra", "Otra")), obl=True,
                    r="poste {seccion}", c="Mide el ancho y el fondo del tubo. Los de 3×3″ no calzan en 2×2″."),
               _opc("agujero", "Ø de los agujeros del poste", (("5/8", "5/8″ (≈16 mm)"), ("1", "1″ (≈25 mm)"), ("otro", "Otro")), obl=True,
                    r="agujeros {agujero}", c="¿Entra 5/8″ o 1″?"),
               _opc("separacion", "Separación entre agujeros, centro a centro", (("25", "25 mm (1″)"), ("50", "50 mm (2″)"), ("otra", "Otra")),
                    obl=True, r="separación {separacion}", c="De centro a centro de dos agujeros seguidos."),
               _num("largo_mm", "Largo del brazo", "mm", 250, 900, mas=True, r="brazo {largo_mm} mm"),
           ],
           como=["Ø del agujero: ¿entra 5/8″ o 1″? Separación: de centro a centro de dos agujeros seguidos.",
                 "Sección del poste: mide el ancho y el fondo del tubo."],
           foto="El poste con 3 o 4 agujeros y la huincha, más el brazo con la huincha al largo.",
           sin="Marca y modelo del rack (de la OT) y foto del poste.", seg="Mientras pones o sacas un brazo, que nadie use el rack.",
           resumen=[["Brazo de seguridad", "poste {seccion}", "agujeros {agujero}"], ["separación {separacion}"]]),
    _ficha("G2", "pernos", "Pernos y tuercas", "pin", "perno", corto="Pernos / tuercas",
           campos=[
               _opc("rosca", "Diámetro de rosca", (("M6", "M6"), ("M8", "M8"), ("M10", "M10"), ("M12", "M12"), ("M16", "M16"), ("3/8", "3/8″"),
                                                    ("1/2", "1/2″"), ("5/8", "5/8″")), r="{rosca}"),
               _num("largo_mm", "Largo bajo la cabeza", "mm", 15, 200, r="× {largo_mm} mm"),
               _opc("cabeza", "Cabeza", (("hex", "Hexagonal"), ("allen", "Allen"), ("nyloc", "Tuerca con seguro (nyloc)")), r="{cabeza}"),
               _num("cantidad", "Cantidad de cada tipo", "u", 1, 200, ap=False, r="{cantidad} u"),
           ],
           como=["Saca UN perno: grosor de la rosca y largo bajo la cabeza. Prueba con una tuerca."],
           foto="El perno sobre la huincha.", sin="«Igual a otro punto de la máquina»: foto.",
           resumen=[["Perno", "{rosca}", "× {largo_mm} mm", "{cabeza}"]]),
    _ficha("G3", "cuerno", "Cuerno porta discos", "cuerno", "cuerno", corto="Cuerno",
           campos=[
               _opc("diam", "Ø", (("olimpico", "Olímpico (≈50 mm)"), ("estandar", "Estándar (25–30 mm)"), ("otro", "Otro")), r="{diam}"),
               _num("largo_mm", "Largo útil", "mm", 150, 400, r="{largo_mm} mm"),
           ],
           como=["Ø: mide por el centro o prueba qué disco entra (olímpico = agujero grande de unos 5 cm). Largo: de la base a la punta."],
           foto="El cuerno con la huincha y la base.", sin="Foto y modelo del equipo.", resumen=[["Cuerno", "{diam}", "{largo_mm} mm"]]),
    _ficha("G4", "disco", "Disco", "cuerno", "disco", corto="Disco",
           campos=[
               _opcnum("peso_kg", "Peso marcado", "kg", ((1.25, ""), (2.5, ""), (5, ""), (10, ""), (15, ""), (20, ""), (25, "")), 0.5, 50,
                       r="{peso_kg} kg", c="El número grabado en la cara. Ojo con kg y lb."),
               _opc("agujero", "Agujero", (("olimpico", "Olímpico (≈51 mm)"), ("estandar", "Estándar (25–30 mm)")), r="{agujero}"),
               _num("diam_ext_mm", "Ø exterior", "mm", 150, 460, mas=True),
           ],
           como=["Peso: lee el número grabado en la cara (ojo con kg y lb). Agujero: de borde a borde por dentro (51 mm = olímpico)."],
           foto="El disco con la huincha por el agujero y la cara con el peso.", sin="Foto de la cara con el peso.",
           resumen=[["Disco", "{peso_kg} kg", "{agujero}"]]),
    _ficha("G5", "mancuerna", "Mancuerna", "cuerno", "disco", corto="Mancuerna",
           campos=[
               _num("peso_kg", "Peso marcado", "kg", 0.5, 60, r="{peso_kg} kg", c="El número grabado. Ojo con kg y lb."),
               _opc("forma", "Forma", (("hex", "Hexagonal"), ("redonda", "Redonda"), ("uretano", "Uretano"), ("ajustable", "Ajustable")), r="{forma}"),
               _num("mango_mm", "Ø del mango", "mm", 28, 35, mas=True),
               _opc("par", "¿Par o unidad?", (("par", "Par"), ("unidad", "Unidad")), r="{par}", ap=False),
           ],
           como=["Peso: el número grabado. Mango: huincha de canto."],
           foto="La mancuerna completa y el peso grabado.", sin="Foto del peso grabado.", resumen=[["Mancuerna", "{peso_kg} kg", "{forma}"]]),
    _ficha("G6", "barra", "Barra y collarín", "cuerno", "barra", corto="Barra / collarín",
           campos=[
               _num("largo_cm", "Largo de la barra", "cm", 120, 230, r="barra {largo_cm} cm"),
               _opc("agarre", "Ø del agarre", (("25", "25 mm"), ("28", "28 mm"), ("28.5", "28,5 mm"), ("29", "29 mm"), ("otro", "Otro")), r="agarre {agarre}"),
               _opc("manguito", "Ø del manguito (y del collarín)", (("50", "50 mm (olímpico)"), ("25-30", "25–30 mm (estándar)")), r="manguito {manguito}"),
               _opc("collarin", "Tipo de collarín", (("resorte", "Resorte"), ("tornillo", "Tornillo"), ("rapido", "Palanca rápida")), mas=True),
           ],
           como=["Largo total; agarre y manguito con la huincha de canto."],
           foto="La barra completa y el manguito.", sin="Foto y modelo.", resumen=[["Barra", "{largo_cm} cm", "agarre {agarre}"]]),
    _ficha("G7", "stack", "Stack: placa y varilla guía", "buje", "varilla", corto="Placa / varilla",
           campos=[
               _txt("placa_peso", "Placa: peso grabado", ph="Ej. 5 kg", r="placa {placa_peso}"),
               _num("placa_espesor_mm", "Placa: espesor", "mm", 10, 40, r="espesor {placa_espesor_mm} mm"),
               _num("varilla_diam_mm", "Varilla guía: Ø", "mm", 12, 30, r="varilla Ø{varilla_diam_mm} mm"),
               _num("varilla_largo_mm", "Varilla guía: largo", "mm", 600, 2200, r="× {varilla_largo_mm} mm"),
           ],
           como=["Placa: el número grabado. Varilla: Ø y largo total. El buje de la guía usa la ficha de bujes."],
           foto="La placa o la varilla con la huincha.", sin="Foto y modelo.", resumen=[["Varilla", "Ø{varilla_diam_mm} mm", "× {varilla_largo_mm} mm"]]),
    # ── Otra pieza ──
    _ficha("H", "otra", "Otra pieza (no sé cómo se llama)", "otra", "otra", corto="Otra pieza",
           campos=[
               _txt("que_es", "¿Qué es? Con tus palabras", ph="Ej. la perilla que sube el asiento", r="{que_es}"),
               _opc("para_que", "¿Para qué sirve?", (("ajusta", "Ajusta"), ("sujeta", "Sujeta"), ("gira", "Gira"), ("amortigua", "Amortigua"),
                                                     ("pisa", "Se pisa"), ("agarra", "Se agarra"), ("ve", "Se ve o se toca"), ("nose", "No sé")),
                    r="{para_que}", ap=False),
               _num("largo_mm", "Medida más larga, aproximada", "mm", 5, 3000, r="{largo_mm}", ap=False),
               _num("ancho_mm", "Medida más ancha, aproximada", "mm", 5, 3000, r="× {ancho_mm} mm", ap=False),
           ],
           como=["Largo y ancho aproximados, con la huincha. «Para qué sirve» en botones.",
                 "Si hace ruido o no gira, un video de 10 segundos ayuda más que las medidas."],
           foto="Foto 1: la pieza sola con la huincha. Foto 2: el lugar donde va.",
           sin="La pieza y el lugar bastan. Gestión decide cómo se llama.", resumen=[["{que_es}"], ["{largo_mm}", "× {ancho_mm} mm"]]),
)
FICHAS = {f["id"]: f for f in _FICHAS_LISTA}
CLAVES = tuple(f["id"] for f in _FICHAS_LISTA if f["clave"])

# Pieza (nombre que ve el técnico) → ficha. El orden importa: lo más específico primero.
_RX_FICHA = (
    ("F5", r"rodillo o rueda|rodillos? del carro|\brueda"),
    ("D2", r"rodillo apoya|apoya ?piernas"),
    ("D1", r"\bpad\b|tapiz|asiento, respaldo|respaldo|cojin"),
    ("E3", r"rodillo"),
    ("G1", r"brazo de seguridad|j-?hook"),
    ("F6", r"brazo o manija|manija"),
    ("R1", r"cinta o cuerda|cuerda de tir"),
    ("R2", r"asiento o carro"),
    ("A1", r"piola|guaya|chicote|cable de acero"),
    ("A2", r"mosquet"),
    ("A3", r"agarre|manilla|dorsal"),
    ("G7", r"varilla|placa del stack"),
    ("B1", r"polea|roldana"),
    ("B2", r"buje|rodamiento|rulema|bocina"),
    ("C1", r"pin selector"),
    ("C2", r"pin de (seguro|altura|seguridad)"),
    ("C3", r"perilla|pin o perilla|pin de ajuste"),
    ("D3", r"nivelador"),
    ("D4", r"tope de goma|goma o taco|taco de goma|goma de apoyo|\btaco\b"),
    ("E1", r"cinta|banda"),
    ("E2", r"tabla|\bdeck"),
    ("F1", r"correa"),
    ("E4", r"motor"),
    ("E5", r"llave"),
    ("E6", r"consola|placa controladora|tarjeta controladora|pantalla"),
    ("F2", r"pedal"),
    ("F3", r"biela"),
    ("F4", r"sill[ií]n|poste|manubrio"),
    ("G2", r"perno|tuerca|tapa de cabezal|tornillo"),
    ("G3", r"cuerno|porta ?disco"),
    ("G6", r"collar[ií]n|\bbarra\b"),
    ("G5", r"mancuerna"),
    ("G4", r"\bdisco"),
)
_RX_FICHA_C = tuple((fid, re.compile(rx)) for fid, rx in _RX_FICHA)


def _norm(s):
    s = unicodedata.normalize("NFD", str(s or ""))
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    return " ".join(s.lower().split())


def ficha_para(nombre):
    """Id de la ficha de una pieza por su nombre («Piola (cable de acero)» → A1). Cualquier nombre sin ficha propia → 'H'."""
    n = _norm(nombre)
    for fid, rx in _RX_FICHA_C:
        if rx.search(n):
            return fid
    return "H"


def es_clave(fid):
    f = FICHAS.get(fid)
    return bool(f and f["clave"])


# ═══════════════════════════════ números, condiciones, formato ═══════════════════════════════
_RX_MILES = re.compile(r"^\d{1,3}(\.\d{3})+(,\d+)?$")


def parse_num(valor):
    """Número desde lo que escribió el técnico: «2,35», «2.35», «2.900» (miles con punto), «235 cm»; None si no es un número."""
    if isinstance(valor, bool) or valor is None:
        return None
    if isinstance(valor, (int, float)):
        f = float(valor)
    else:
        t = str(valor).strip().replace(" ", "")
        t = re.sub(r"[^\d.,]+$", "", t)                 # «235cm», «5mm», «2,35 m»: se ignora lo que sobra al final
        if not t:
            return None
        if _RX_MILES.match(t):
            t = t.replace(".", "").replace(",", ".")
        else:
            t = t.replace(",", ".")
        try:
            f = float(t)
        except ValueError:
            return None
    if f != f or f in (float("inf"), float("-inf")):
        return None
    return f


def _tiene(campos, k):
    v = campos.get(k)
    if isinstance(v, bool):
        return True
    return v is not None and str(v).strip() != ""


def _cond_ok(cond, campos):
    """Un campo con `cond` solo se muestra (y se pide) si se cumple. {"k":…, "in":[…]} · {"vacio": k} · con «o» una alternativa."""
    if not cond:
        return True
    if "vacio" in cond:
        return not _tiene(campos, cond["vacio"])
    ok = str(campos.get(cond.get("k"), "")) in [str(x) for x in cond.get("in", [])]
    if not ok and cond.get("o"):
        return _cond_ok(cond["o"], campos)
    return ok


def _texto_opc(spec, valor):
    for v, t in spec.get("op", []):
        if str(v) == str(valor):
            return t
    return str(valor)


def _display(spec, valor, campos):
    """Cómo se lee un valor en el resumen (sin la unidad: la pone la plantilla del resumen)."""
    y = spec["y"]
    if y == "num":
        return _fmt(valor)
    if y == "opcnum":
        return _fmt(valor)
    if y == "opc":
        if str(valor) == "otro" and campos.get(spec["k"] + "_otro"):
            return str(campos.get(spec["k"] + "_otro"))
        t = _texto_opc(spec, valor)
        # en el resumen se usa el texto corto: lo anterior a « · », « (» o «:»
        return re.split(r"\s+·\s+|\s+\(|:", t)[0].strip()
    if y == "sino":
        return "sí" if valor else "no"
    return str(valor)


# ═══════════════════════════════ validación ═══════════════════════════════
def _aviso_rango(spec, f):
    u = spec.get("u", "")
    rango = f"{_fmt(spec['mn'])}–{_fmt(spec['mx'])} {u}".strip()
    cual = "muy bajo" if f < spec["mn"] else "muy alto"
    return (f"«{spec['t']}»: {_fmt(f)} {u} es {cual} para esta pieza (lo normal es {rango}). ¿Seguro? Revísalo con la huincha.").replace("  ", " ")


def normalizar_datos(ficha_id, datos):
    """Valida y limpia las medidas de UNA línea del pedido.

    datos: {"ficha": id, "medido_con": "huincha|catalogo|no_pude|", "motivo_no": clave, "campos": {k: valor}}.
    Devuelve (limpio, None) o (None, (mensaje, codigo)). `limpio` lleva «avisos» (rangos razonables y cruces: AVISAN, no bloquean),
    `completo` y el `resumen` legible. Solo BLOQUEA: ficha inexistente, un valor imposible (cero, negativo, más de 5 veces el
    máximo, una opción que no existe) y una pieza CLAVE sin medidas y sin la salida «No pude medir»."""
    ficha = FICHAS.get(str(ficha_id or ""))
    if not ficha:
        return None, ("Una de las piezas llegó con una ficha de medidas que no existe.", "FICHA_INVALIDA")
    d = datos if isinstance(datos, dict) else {}
    crudos = d.get("campos") if isinstance(d.get("campos"), dict) else {}
    medido = str(d.get("medido_con") or "").strip().lower()
    if medido not in ("", "huincha", "catalogo", "no_pude"):
        medido = ""
    motivo_no = str(d.get("motivo_no") or "").strip().lower()
    avisos, campos = [], {}
    for spec in ficha["campos"]:
        k, y = spec["k"], spec["y"]
        v = crudos.get(k)
        if v is None or (isinstance(v, str) and not v.strip()):
            continue
        if y in ("num", "opcnum"):
            f = parse_num(v)
            if f is None:
                if y == "opcnum" and str(v) == "otro":
                    continue
                return None, (f"«{spec['t']}» tiene que ser un número (por ejemplo 2,35).", "MEDIDA_INVALIDA")
            if f <= 0:
                return None, (f"«{spec['t']}» tiene que ser mayor que cero.", "MEDIDA_INVALIDA")
            if spec.get("mx") is not None and f > spec["mx"] * FACTOR_IMPOSIBLE:
                return None, (f"«{spec['t']}»: {_fmt(f)} {spec.get('u', '')} es imposible para esta pieza. Revisa la unidad "
                              f"({spec.get('u', '')}).".replace("  ", " "), "MEDIDA_INVALIDA")
            f = round(f, 3)
            campos[k] = int(f) if float(f).is_integer() else f
            if spec.get("mn") is not None and spec.get("mx") is not None and (f < spec["mn"] or f > spec["mx"]):
                if not _confirmado(d, k):
                    avisos.append(_aviso_rango(spec, f))
        elif y == "opc":
            sv = str(v).strip()
            if sv not in {str(x[0]) for x in spec["op"]}:
                return None, (f"«{spec['t']}»: esa opción no existe.", "MEDIDA_INVALIDA")
            campos[k] = sv
            if sv == "otro":
                otro = " ".join(str(crudos.get(k + "_otro") or "").split())[:MAX_TEXTO]
                if otro:
                    campos[k + "_otro"] = otro
        elif y == "sino":
            if isinstance(v, bool):
                campos[k] = v
            else:
                s = _norm(v)
                if s in ("si", "1", "true"):
                    campos[k] = True
                elif s in ("no", "0", "false"):
                    campos[k] = False
        elif y == "txt":
            t = " ".join(str(v).split())[:MAX_TEXTO]
            if t:
                campos[k] = t
    # un dato oculto (su condición no se cumple) no se guarda
    for spec in ficha["campos"]:
        if spec["k"] in campos and not _cond_ok(spec.get("cond"), campos):
            campos.pop(spec["k"], None)
            campos.pop(spec["k"] + "_otro", None)

    if medido == "no_pude":
        if motivo_no not in {k for k, _t in MOTIVOS_NO_PUDE}:
            return None, ("Cuéntanos por qué no pudiste medir: elige un motivo de la lista.", "MOTIVO_NO_PUDE_REQUERIDO")
    else:
        motivo_no = ""
        if not medido:
            medido = "huincha" if campos else ""
        if medido == "catalogo" and not campos:
            medido = ""
    # «Del catálogo» y «No pude medir» son las dos salidas de una pieza clave: ninguna exige las medidas una por una.
    faltan = faltan_obligatorios(ficha["id"], campos) if (ficha["clave"] and medido not in ("no_pude", "catalogo")) else []
    if faltan:
        return None, ("Faltan datos de «" + ficha["corto"] + "»: " + _lista_humana([f"«{t}»" for t in faltan]) + ". Si no pudiste medir, toca "
                      "«No pude medir» y sube una foto de la pieza con la huincha al lado.", "MEDIDA_REQUERIDA")
    avisos += cruces(ficha["id"], campos)
    limpio = {"ficha": ficha["id"], "tipo": ficha["tipo"], "medido_con": medido, "motivo_no": motivo_no, "campos": campos,
              "avisos": avisos[:8], "clave": bool(ficha["clave"])}
    if motivo_no == "otro":
        otro = " ".join(str(d.get("motivo_otro") or "").split())[:MAX_TEXTO]
        if otro:
            limpio["motivo_otro"] = otro
    limpio["completo"] = bool(medido in ("no_pude", "catalogo") or (campos and not faltan_obligatorios(ficha["id"], campos)))
    limpio["resumen"] = resumen(limpio)
    return limpio, None


def _confirmado(d, k):
    c = d.get("confirmados")
    return isinstance(c, dict) and bool(c.get(k))


def _lista_humana(items):
    if len(items) <= 1:
        return "".join(items)
    return ", ".join(items[:-1]) + " y " + items[-1]


def faltan_obligatorios(ficha_id, campos):
    """Títulos de lo que falta para dar por medida una pieza CLAVE: los campos obligatorios visibles y, si la ficha lo pide
    (correa: el código del lomo o canales + largo), al menos una de las alternativas."""
    ficha = FICHAS.get(ficha_id)
    if not ficha:
        return []
    campos = campos or {}
    faltan = []
    for spec in ficha["campos"]:
        if spec.get("obl") and _cond_ok(spec.get("cond"), campos) and not _tiene(campos, spec["k"]):
            faltan.append(spec["t"])
    if ficha["req_alt"]:
        if not any(all(_tiene(campos, k) for k in alt) for alt in ficha["req_alt"]):
            por_k = {c["k"]: c["t"] for c in ficha["campos"]}
            faltan.append(" o ".join(" y ".join(por_k.get(k, k) for k in alt) for alt in ficha["req_alt"]))
    return faltan


def debe_tener_foto_propia(lineas_con_clave):
    """Con 2 o más piezas CLAVE en el pedido, cada una lleva su propia foto (la pieza con la huincha al lado); con una sola basta la
    foto del pedido (Daniel: foto por línea cuando hay varias piezas con medida)."""
    return int(lineas_con_clave or 0) >= 2


# ═══════════════════════════════ cruces inteligentes (sugieren; la persona decide) ═══════════════════════════════
RODAMIENTOS = {
    "608": (8, 22, 7), "6000": (10, 26, 8), "6001": (12, 28, 8), "6002": (15, 32, 9), "6200": (10, 30, 9), "6201": (12, 32, 10),
    "6202": (15, 35, 11), "6203": (17, 40, 12), "6204": (20, 47, 14), "6205": (25, 52, 15),
}
_RX_CORREA = re.compile(r"^\s*(\d{1,2})\s*(PJ|PK|PL|J|K|L|M|H)\s*(\d{2,4})\s*$", re.I)


def rodamiento_sugerido(int_mm, ext_mm, ancho_mm, tol=0.6):
    """Código de rodamiento que calza con Ø interior × Ø exterior × ancho (mm), o None."""
    try:
        a, b, c = float(int_mm), float(ext_mm), float(ancho_mm)
    except (TypeError, ValueError):
        return None
    for cod, (i, e, w) in RODAMIENTOS.items():
        if abs(a - i) <= tol and abs(b - e) <= tol and abs(c - w) <= tol:
            return cod
    return None


def decodificar_correa(codigo):
    """«6PJ508» → {'canales': 6, 'perfil': 'PJ', 'largo_mm': 508}; None si no tiene esa forma."""
    m = _RX_CORREA.match(str(codigo or ""))
    if not m:
        return None
    return {"canales": int(m.group(1)), "perfil": m.group(2).upper(), "largo_mm": int(m.group(3))}


def cruces(ficha_id, campos):
    """Avisos de coherencia entre datos de una misma pieza. Solo AVISAN."""
    out = []
    c = campos or {}
    if ficha_id == "E1" and c.get("pisar_mm") and c.get("vuelta_mm"):
        r = float(c["vuelta_mm"]) / float(c["pisar_mm"])
        if r < 1.8 or r > 3.0:
            out.append("La vuelta completa de una cinta suele ser el doble de la superficie para pisar. ¿Mediste la vuelta o solo la "
                       "superficie?")
    if ficha_id == "F1":
        dec = decodificar_correa(c.get("codigo"))
        if dec and c.get("canales") and int(float(c["canales"])) != dec["canales"]:
            out.append(f"El código dice {dec['canales']} canales y anotaste {_fmt(c['canales'])}. Revísalo.")
        if c.get("canales") and c.get("ancho_mm") and (c.get("perfil") in (None, "PJ") or (dec and dec["perfil"] == "PJ")):
            esperado = float(c["canales"]) * 2.34
            if abs(float(c["ancho_mm"]) - esperado) > 3:
                out.append(f"Con {_fmt(c['canales'])} canales una correa PJ mide cerca de {_fmt(round(esperado, 1))} mm de ancho. Revisa el ancho.")
    if ficha_id == "B2" and not c.get("codigo") and c.get("int_mm") and c.get("ext_mm") and c.get("ancho_mm"):
        cod = rodamiento_sugerido(c["int_mm"], c["ext_mm"], c["ancho_mm"])
        if cod:
            out.append(f"Con {_fmt(c['int_mm'])} × {_fmt(c['ext_mm'])} × {_fmt(c['ancho_mm'])} mm suele ser un rodamiento {cod}. "
                       "Confírmalo con el código grabado.")
    return out


# ═══════════════════════════════ resumen y filas legibles ═══════════════════════════════
_RX_TOKEN = re.compile(r"\{(\w+)\}")


def _render(plantilla, spec_por_k, campos):
    """Reemplaza {k} por el valor leído; None si falta alguno (el trozo entero se omite)."""
    faltan = False

    def _r(m):
        nonlocal faltan
        k = m.group(1)
        if not _tiene(campos, k):
            faltan = True
            return ""
        spec = spec_por_k.get(k)
        return _display(spec, campos[k], campos) if spec else str(campos[k])

    out = _RX_TOKEN.sub(_r, plantilla)
    return None if faltan else out


def resumen(limpio):
    """Una línea legible de las medidas («Piola Ø5 mm × 235 cm · bola / ojal»). Es lo que va a la columna `medida` (80 letras) y a las
    pantallas de bodega y gestión. Una pieza «No pude medir» lo dice."""
    ficha = FICHAS.get((limpio or {}).get("ficha"))
    if not ficha:
        return ""
    campos = limpio.get("campos") or {}
    por_k = {c["k"]: c for c in ficha["campos"]}
    grupos = []
    for grupo in ficha["resumen"]:
        partes = [p for p in (_render(t, por_k, campos) for t in grupo) if p]
        if partes:
            grupos.append(" ".join(partes))
    txt = " · ".join(grupos)
    if limpio.get("medido_con") == "no_pude":
        motivo = dict(MOTIVOS_NO_PUDE).get(limpio.get("motivo_no"), "")
        extra = "sin medida en terreno" + (f" ({motivo.lower()})" if motivo else "")
        txt = (txt + " · " if txt else (ficha["corto"] + " · ")) + extra
    elif limpio.get("medido_con") == "catalogo":
        txt = (txt + " · " if txt else "") + "del catálogo, no medida en terreno"
    return txt.strip()


def resumen_corto(limpio, tope=80):
    """El resumen recortado a lo que cabe en mant_ot_repuesto_solicitudes.medida (VARCHAR 80)."""
    t = resumen(limpio)
    return t if len(t) <= tope else t[: tope - 1].rstrip() + "…"


def filas(limpio):
    """[(etiqueta, valor)] para mostrar las medidas completas a bodega y gestión, en el orden de la ficha."""
    ficha = FICHAS.get((limpio or {}).get("ficha"))
    if not ficha:
        return []
    campos = limpio.get("campos") or {}
    out = []
    for spec in ficha["campos"]:
        k = spec["k"]
        if not _tiene(campos, k):
            continue
        v = campos[k]
        y = spec["y"]
        if y in ("num", "opcnum"):
            txt = f"{_fmt(v)} {spec.get('u', '')}".strip()
            if spec.get("u") == "cm":
                txt += f" ({_fmt(float(v) / 100)} m)"
            elif spec.get("u") == "mm" and float(v) >= 1000:
                txt += f" ({_fmt(float(v) / 1000)} m)"
        elif y == "opc":
            txt = (str(campos.get(k + "_otro")) if str(v) == "otro" and campos.get(k + "_otro") else _texto_opc(spec, v))
        elif y == "sino":
            txt = "Sí" if v else "No"
        else:
            txt = str(v)
        out.append((spec["t"], txt))
    if limpio.get("medido_con") == "no_pude":
        motivo = dict(MOTIVOS_NO_PUDE).get(limpio.get("motivo_no"), "sin motivo")
        if limpio.get("motivo_otro"):
            motivo += " (" + limpio["motivo_otro"] + ")"
        out.insert(0, ("Medida", "No se pudo medir: " + motivo))
    elif limpio.get("medido_con") == "catalogo":
        out.insert(0, ("Medida", "Del catálogo o manual (no se midió en terreno)"))
    elif limpio.get("medido_con") == "huincha":
        out.insert(0, ("Medida", "Medida con huincha en terreno"))
    return out


def piola_metros(limpio):
    """Para la compatibilidad con el resto del sistema: la piola (A1) también llena `piola_metros`. None si no aplica."""
    if (limpio or {}).get("ficha") != "A1":
        return None
    cm = (limpio.get("campos") or {}).get("largo_cm")
    if cm is None:
        return None
    m = round(float(cm) / 100.0, 2)
    return m if 0 < m <= 200 else None


# ═══════════════════════════════ aprendizaje del catálogo del modelo ═══════════════════════════════
def campos_de_aprendizaje(ficha_id, campos):
    ficha = FICHAS.get(ficha_id)
    if not ficha:
        return {}
    c = campos or {}
    return {s["k"]: c[s["k"]] for s in ficha["campos"] if s.get("ap") and _tiene(c, s["k"])}


def misma_medida(ficha_id, a, b):
    """True si dos medidas de la misma pieza son «iguales»: mismas opciones y números dentro de la tolerancia de cada dato
    (la huincha no mide al milímetro: 235 y 236 cm son la misma piola). Exige al menos UN dato numérico u opción en común y que
    no haya ninguno que difiera."""
    ficha = FICHAS.get(ficha_id)
    if not ficha:
        return False
    a, b = a or {}, b or {}
    comunes = 0
    for spec in ficha["campos"]:
        if not spec.get("ap"):
            continue
        k = spec["k"]
        va, vb = a.get(k), b.get(k)
        if va is None or vb is None:
            continue
        comunes += 1
        if spec["y"] in ("num", "opcnum"):
            fa, fb = parse_num(va), parse_num(vb)
            if fa is None or fb is None:
                return False
            tol = float(spec.get("tol") or 0.0)
            if abs(fa - fb) > max(abs(fa), abs(fb)) * tol + 1e-9:
                return False
        elif _norm(va) != _norm(vb):
            return False
    return comunes > 0


def aprender(candidatos, ficha_id, campos, lote_id):
    """Decide qué hacer con UNA medida nueva frente a las ya vistas del mismo modelo y pieza.

    candidatos: [{id, datos(dict), n_pedidos, estado, lotes(list)}]. Devuelve:
      {"accion": "igual"|"sumar"|"nueva", "id": cid|None, "n": n_pedidos, "estado": estado, "lotes": [...]}
    «igual» = ese mismo pedido ya la sumó (no cuenta dos veces). Con UMBRAL_SUGERIDA pedidos distintos la medida queda «sugerida»;
    una ya «aprobada» o «descartada» por gestión conserva su estado (una persona decidió: no se deshace)."""
    nuevos = campos_de_aprendizaje(ficha_id, campos)
    if not nuevos:
        return {"accion": "nada", "id": None, "n": 0, "estado": None, "lotes": []}
    lote = str(lote_id or "")
    for c in candidatos or []:
        if misma_medida(ficha_id, c.get("datos"), nuevos):
            lotes = list(c.get("lotes") or [])
            if lote and lote in lotes:
                return {"accion": "igual", "id": c.get("id"), "n": int(c.get("n_pedidos") or 1), "estado": c.get("estado"), "lotes": lotes}
            if lote:
                lotes.append(lote)
            n = int(c.get("n_pedidos") or 1) + 1
            estado = c.get("estado") or "observada"
            if estado == "observada" and n >= UMBRAL_SUGERIDA:
                estado = "sugerida"
            return {"accion": "sumar", "id": c.get("id"), "n": n, "estado": estado, "lotes": lotes[-20:]}
    return {"accion": "nueva", "id": None, "n": 1, "estado": "observada", "lotes": [lote] if lote else []}


# ═══════════════════════════════ diagnóstico guiado «¿Qué le pasa?» ═══════════════════════════════
ACCIONES = ("pedir", "fs", "baja", "incidencia", "noesta", "nollego", "esperar")
ACCIONES_POR_TIPO = {"e": ("pedir", "fs", "baja", "incidencia", "noesta", "esperar"),
                     "i": ("pedir", "fs", "baja", "incidencia", "nollego", "esperar")}
MAX_PREGUNTAS = 3
# Atajo de seguridad (siempre visible): salta directo a «No se puede usar» con el riesgo como PREGUNTA, no como hecho.
ATAJO_PELIGRO = {"accion": "fs", "motivo": "pieza_rota", "riesgo": "si", "texto": "",
                 "por_que": "Si algo puede lastimar a alguien, lo primero es que nadie lo use. Tú confirmas el riesgo en la siguiente pantalla.",
                 "aviso": "Pon un letrero o huincha y desenchúfalo si tiene enchufe, antes de irte."}

# Zonas de «Hace ruido o se traba» por sistema: (id, texto, ícono, piezas si tiene juego, piezas si se traba, riesgo con juego, riesgo si se traba)
_ZONAS = {
    "selector_peso": (
        ("poleas", "Poleas y piola", "soundwave", ("Polea con rodamiento",), ("Piola (cable de acero)", "Polea con rodamiento"), False, True),
        ("stack", "Stack de placas y pin", "layers", ("Varilla guía o buje de guía",), ("Pin selector", "Varilla guía o buje de guía"), False, False),
        ("pivotes", "Brazos y pivotes", "arrows-move", ("Buje o rodamiento de pivote",), ("Buje o rodamiento de pivote",), False, False),
        ("asiento", "Asiento o respaldo", "person-standing", ("Perilla de ajuste de asiento",), ("Perilla de ajuste de asiento",), False, False),
    ),
    "carga_discos": (
        ("pivotes", "Pivotes y brazos", "arrows-move", ("Rodamiento o buje de pivote",), ("Rodamiento o buje de pivote",), False, False),
        ("cuernos", "Cuernos y seguros", "gear-wide-connected", ("Pin de seguro", "Cuerno porta discos"), ("Pin de seguro",), True, True),
        ("asiento", "Asiento o pad", "person-standing", ("Pad de asiento o respaldo",), ("Pad de asiento o respaldo",), False, False),
    ),
    "poleas": (
        ("poleas", "Poleas y cable", "soundwave", ("Polea con rodamiento",), ("Cable de acero (piola)", "Polea con rodamiento"), False, True),
        ("carro", "Carro y pin de altura", "arrows-vertical", ("Pin de altura",), ("Pin de altura",), True, False),
    ),
    "trotadora": (
        ("cinta", "Cinta o rodillos", "arrow-repeat", ("Rodillo (delantero o trasero)",), ("Rodillo (delantero o trasero)", "Cinta (banda)"), False, False),
        ("motor", "Motor", "activity", ("Motor de tracción",), ("Motor de tracción", "Correa del motor"), False, False),
        ("tabla", "Tabla: golpe al pisar", "hammer", ("Tabla (deck)",), ("Tabla (deck)",), False, False),
    ),
    "bicicleta": (
        ("pedales", "Pedales y biela", "bicycle", ("Pedales (par)", "Biela"), ("Pedales (par)", "Biela"), True, True),
        ("transmision", "Transmisión", "gear-wide-connected", ("Correa de transmisión",), ("Correa de transmisión",), False, False),
        ("asiento", "Sillín o manubrio", "person-standing", ("Sillín o poste",), ("Sillín o poste",), True, True),
    ),
    "eliptica": (
        ("pedales", "Pedales y biela", "bicycle", ("Pedales (par)", "Biela"), ("Pedales (par)", "Biela"), True, True),
        ("rodillos", "Rodillos o ruedas", "arrow-repeat", ("Rodillo o rueda",), ("Rodillo o rueda",), False, False),
        ("brazos", "Brazos o manijas", "arrows-move", ("Brazo o manija",), ("Brazo o manija",), True, True),
        ("transmision", "Transmisión", "gear-wide-connected", ("Correa de transmisión",), ("Correa de transmisión",), False, False),
    ),
    "remo": (
        ("tiron", "Cinta o cuerda de tirón", "soundwave", ("Cinta o cuerda de tirón",), ("Cinta o cuerda de tirón",), True, True),
        ("carro", "Asiento o carro", "arrows-move", ("Asiento o carro",), ("Asiento o carro", "Rodillos del carro"), False, False),
        ("rodillos", "Rodillos del carro", "arrow-repeat", ("Rodillos del carro",), ("Rodillos del carro",), False, False),
    ),
    "banco": (
        ("ajustes", "Ajustes y pines", "gear-wide-connected", ("Pin o perilla de ajuste",), ("Pin o perilla de ajuste",), True, True),
        ("estructura", "Estructura que se mueve", "arrows-move", ("Nivelador o taco de goma",), ("Nivelador o taco de goma",), False, False),
    ),
    "rack": (
        ("ajustes", "Pines y brazos de seguridad", "gear-wide-connected", ("Pin de seguridad", "Brazo de seguridad"), ("Pin de seguridad", "Brazo de seguridad"), True, True),
        ("estructura", "Estructura que se mueve", "arrows-move", ("Kit de pernos y tuercas",), ("Kit de pernos y tuercas",), True, True),
    ),
}
_SISTEMAS_ELECTRICOS = ("trotadora", "bicicleta", "eliptica", "remo")
# Pieza de seguridad: si la persona dice «se puede usar así», aviso ámbar (no bloquea).
PIEZAS_SEGURIDAD = ("Piola (cable de acero)", "Cable de acero (piola)", "Pin de seguro", "Pin de seguridad", "Pin de altura", "Brazo de seguridad",
                    "J-hook", "Cuerno porta discos", "Mosquetón", "Llave magnética de emergencia", "Biela", "Tuerca o tapa de cabezal",
                    "Sillín o poste", "Kit de pernos y tuercas")


def _o(t, ic, ir=None, res=None):
    return {"t": t, "ic": ic, "ir": ir, "res": res}


def _res(accion, **kw):
    r = {"accion": accion}
    r.update({k: v for k, v in kw.items() if v is not None})
    return r


def _otra_cosa(kind):
    return _o("Otra cosa", "megaphone", res=_res(
        "incidencia", por_que="Si no calza con nada de lo anterior, cuéntalo con una foto: bodega y gestión lo ven el mismo día.",
        texto=""))


def _usable(pregunta, piezas, falla, kind, por_que):
    """Pregunta «¿Se puede usar así?» al final de «Le falta una pieza» y «Está roto»."""
    seg = any(p in PIEZAS_SEGURIDAD for p in piezas)
    sug_riesgo = _res("pedir", piezas=list(piezas), falla=falla, por_que=por_que,
                      ademas=[_res("fs", motivo="pieza_rota", riesgo="si", rotulo="Dejarlo fuera de servicio: puede lastimar a alguien")],
                      aviso="Pon un letrero o huincha mientras llega la pieza.")
    return {
        "q": pregunta,
        "ops": [
            _o("Sí, funciona", "check-circle",
               res=_res("pedir", piezas=list(piezas), falla=falla, por_que=por_que,
                        aviso=("Esta pieza es de seguridad: ¿seguro que se puede usar así?" if seg else None))),
            _o("No se puede usar", "sign-stop",
               res=_res("pedir", piezas=list(piezas), falla=falla, por_que=por_que,
                        ademas=[_res("fs", motivo="pieza_rota", riesgo="no", rotulo="Dejarlo fuera de servicio mientras llega la pieza")])),
            _o("Puede lastimar a alguien", "cone-striped", res=sug_riesgo),
            _otra_cosa(kind),
        ],
    }


def arbol(kind, sistema):
    """Árbol de «¿Qué le pasa?» para un equipo de cliente (kind 'e') o un producto de la revisión (kind 'i') según su sistema.

    {"raiz": id, "nodos": {id: {"q": pregunta, "ops": [{"t","ic","ir","res"}]}}}. Una pregunta por pantalla, máximo
    MAX_PREGUNTAS contando la primera, y «Otra cosa» en cada una. Cada opción lleva a otra pregunta («ir») o termina en una
    sugerencia («res»). Nada se crea ni se marca solo: la sugerencia se confirma en el flujo de siempre."""
    kind = "i" if kind == "i" else "e"
    piezas_sistema = list(pe.SUGERIDOS_POR_SISTEMA.get(sistema or "", ()))
    zonas = _ZONAS.get(sistema or "", ())
    nodos = {}
    raiz = []

    # ── camino 1: no está el equipo (solo equipo de cliente) ──
    if kind == "e":
        nodos["n1"] = {"q": "¿Ya preguntaste al encargado o al cliente?", "ops": [
            _o("Todavía no", "chat-left-text", res=_res(
                "esperar", por_que="Antes de anotar que no está, pregunta al encargado o al cliente: a veces solo lo movieron.",
                tips=["Pregunta al encargado del recinto.", "Pregunta si alguien lo guardó o lo movió a otra sala."],
                botones=[{"t": "Ya pregunté", "ir": "n1b"}, {"t": "Seguir con otro equipo", "cerrar": True}])),
            _o("Sí, ya pregunté", "check2-circle", ir="n1b"),
            _otra_cosa(kind)]}
        nodos["n1b"] = {"q": "¿Qué te dijeron?", "ops": [
            _o("Lo movieron o lo guardaron", "arrows-move", res=_res(
                "noesta", opcion="no_esta", motivo="cliente_lo_movio", por_que="Queda anotado que el cliente lo movió o lo guardó: el equipo deja de bloquear la firma.")),
            _o("Lo sacó el cliente del recinto", "box-arrow-right", res=_res(
                "noesta", opcion="no_esta", motivo="cliente_lo_saco", por_que="Queda anotado y, además, conviene avisar a gestión.",
                ademas=[_res("incidencia", texto="El cliente sacó el equipo del recinto", por_que="Que gestión lo sepa el mismo día.")])),
            _o("Nadie sabe", "question-circle", res=_res(
                "noesta", opcion="no_esta", motivo="nadie_sabe", por_que="Si nadie sabe dónde está, puede ser una pérdida: avisa a gestión con una foto del lugar vacío.",
                ademas=[_res("incidencia", texto="Nadie sabe dónde está el equipo (posible pérdida)", por_que="Avisa con una foto del lugar vacío.")])),
            _o("No hay stock o está pedido y no llega", "box-seam", res=_res(
                "noesta", opcion="falta_stock", motivo="sin_stock", por_que="«No llegó · falta stock» no lleva foto: el equipo queda pendiente para gestión y bodega.")),
            _otra_cosa(kind)]}
        raiz.append(_o("No está el equipo", "geo-alt", ir="n1"))

    # ── camino 2: no funciona o no enciende (solo sistemas con electrónica) ──
    if sistema in _SISTEMAS_ELECTRICOS:
        trot = sistema == "trotadora"
        ops1 = []
        prueba_txt = ("¿Ya probaste otro enchufe, la llave de emergencia puesta y el automático del recinto?" if trot
                      else "¿Ya probaste otro enchufe, el adaptador o las pilas, y el automático del recinto?")
        nodos["n2a"] = {"q": prueba_txt, "ops": [
            _o("Todavía no los probé", "plug", res=_res(
                "esperar", por_que="Antes de pedir una pieza hay que descartar lo simple.",
                tips=["Prueba otro enchufe.", ("Revisa que la llave de emergencia esté puesta y firme." if trot else "Cambia las pilas o prueba el adaptador."),
                      "Revisa el automático del recinto.", "Espera 30 segundos y vuelve a encender."],
                botones=[{"t": "Ya probé: sigue igual", "ir": "n2a"}, {"t": "Seguir con otro equipo", "cerrar": True}])),
        ] + ([_o("La llave no está o está floja", "key", res=_res(
            "pedir", piezas=["Llave magnética de emergencia"], falla="se_perdio",
            por_que="Sin llave la trotadora no parte. Nunca se puentea.", aviso="No la puentees con un imán suelto."))] if trot else []) + [
            _o("Sí, probé todo y sigue sin luz ni pantalla", "x-circle", res=_res(
                "pedir", piezas=(["Placa controladora", "Consola"] if trot else ["Consola"]), falla="otro",
                por_que=("Si nada enciende, lo más probable es la placa; si algo enciende pero la pantalla no, la consola. Tú eliges."
                         if trot else "Con pilas nuevas y sin pantalla, lo más probable es la consola."),
                ademas=[_res("fs", motivo="no_enciende", riesgo="no", rotulo="Dejarlo fuera de servicio: no se puede usar")])),
            _otra_cosa(kind)]}
        ops1.append(_o("Nada: sin luces ni pantalla", "plug", ir="n2a"))
        if trot:
            nodos["n2b"] = {"q": "¿Se escucha el motor?", "ops": [
                _o("Zumba pero no gira", "soundwave", res=_res(
                    "pedir", piezas=["Motor de tracción", "Correa del motor"], falla="otro", video=True,
                    por_que="Si zumba y no gira, es el motor o la correa. Graba un video de 10 segundos con sonido.",
                    ademas=[_res("fs", motivo="no_enciende", riesgo="no", rotulo="Dejarlo fuera de servicio: no se puede usar")])),
                _o("Silencio total", "volume-mute", res=_res(
                    "pedir", piezas=["Placa controladora"], falla="otro", por_que="Sin sonido ni luces con el equipo energizado: la placa controladora.",
                    ademas=[_res("fs", motivo="no_enciende", riesgo="no", rotulo="Dejarlo fuera de servicio: no se puede usar")])),
                _o("Gira a tirones o patina", "arrow-repeat", res=_res(
                    "pedir", piezas=["Cinta (banda)", "Correa del motor"], falla="desgaste",
                    por_que="Si gira a tirones o patina, es la cinta o la correa del motor. Tú eliges.")),
                _otra_cosa(kind)]}
            ops1.append(_o("Enciende pero no parte o no gira", "lightning-charge", ir="n2b"))
        nodos["n2c"] = {"q": "¿De dónde viene el olor o el calor?", "ops": (
            [_o("Del motor", "activity", res=_res(
                "pedir", piezas=["Motor de tracción"], falla="otro", por_que="Olor o calor del motor: puede ser el motor. Desenchúfalo antes de tocarlo.",
                ademas=[_res("fs", motivo="no_enciende", riesgo="si", rotulo="Dejarlo fuera de servicio: puede lastimar a alguien")],
                aviso="Desenchúfalo ahora."))] if trot else []) + [
            _o("Del tablero o la consola", "display", res=_res(
                "pedir", piezas=(["Consola", "Placa controladora"] if trot else ["Consola"]), falla="otro",
                por_que="Olor o calor en el tablero: la consola o la placa. Desenchúfalo antes de tocarlo.",
                ademas=[_res("fs", motivo="no_enciende", riesgo="si", rotulo="Dejarlo fuera de servicio: puede lastimar a alguien")],
                aviso="Desenchúfalo ahora.")),
            _o("No sé", "question-circle", res=_res(
                "incidencia", por_que="Si no sabes de dónde viene, avisa con una foto y no lo uses.", texto="Se apaga solo o huele a quemado; no se sabe de dónde viene",
                ademas=[_res("fs", motivo="no_enciende", riesgo="si", rotulo="Dejarlo fuera de servicio: puede lastimar a alguien")],
                aviso="Desenchúfalo ahora.")),
            _otra_cosa(kind)]}
        ops1.append(_o("Se apaga solo o huele a quemado", "fire", ir="n2c"))
        ops1.append(_o("Muestra un código de error", "exclamation-diamond", res=_res(
            "incidencia", texto="Código de error: ", por_que="Escribe el código tal como sale y fotografía la pantalla: sin el código nadie puede diagnosticar.",
            ademas=[_res("pedir", piezas=(["Consola", "Placa controladora"] if trot else ["Consola"]), falla="otro", rotulo="Pedir la consola o la placa")])))
        ops1.append(_otra_cosa(kind))
        nodos["n2"] = {"q": "¿Qué hace cuando lo pruebas?", "ops": ops1}
        raiz.append(_o("No funciona o no enciende", "power", ir="n2"))

    # ── camino 3: hace ruido o se traba ──
    if zonas:
        ops3 = []
        for zid, ztxt, zic, juego, traba, r_j, r_t in zonas:
            nid = "n3_" + zid
            nodos[nid] = {"q": f"Muévelo con la mano, sin carga: {ztxt.lower()}, ¿tiene juego?", "ops": [
                _o("Sí, se mueve de lado o golpea", "arrows-move", res=_res(
                    "pedir", piezas=list(juego), falla="ruido", por_que="El juego es desgaste: lo que se cambia es la pieza de esa zona.",
                    ademas=([_res("fs", motivo="pieza_rota", riesgo="si", rotulo="Dejarlo fuera de servicio: puede lastimar a alguien")] if r_j else None))),
                _o("Firme, pero chirría o roza", "soundwave", res=_res(
                    "incidencia", texto=f"Ruido en {ztxt.lower()}, firme y sin juego (falta lubricar o ajustar)",
                    por_que="Si no tiene juego todavía no es repuesto: avisa para lubricar o ajustar. En una revisión se observa, no se lubrica.")),
                _o("Se traba o no se mueve", "lock", res=_res(
                    "pedir", piezas=list(traba), falla="ruido", por_que="Si se traba, lo normal es la pieza de esa zona (a veces dos: tú eliges).",
                    ademas=([_res("fs", motivo="se_traba", riesgo="si", rotulo="Dejarlo fuera de servicio: puede lastimar a alguien")] if r_t else
                            [_res("fs", motivo="se_traba", riesgo="no", rotulo="Dejarlo fuera de servicio si no se puede usar")]))),
                _otra_cosa(kind)]}
            ops3.append(_o(ztxt, zic, ir=nid))
        ops3.append(_otra_cosa(kind))
        nodos["n3"] = {"q": "¿De dónde viene?", "ops": ops3}
        raiz.append(_o("Hace ruido o se traba", "soundwave", ir="n3"))
    else:
        raiz.append(_o("Hace ruido o se traba", "soundwave", res=_res(
            "incidencia", texto="Hace ruido o se traba", por_que="Cuéntalo con una foto (y un video corto si hace ruido): bodega y gestión lo ven el mismo día.")))

    # ── camino 4: le falta una pieza ──
    if piezas_sistema:
        ops4 = []
        for i, p in enumerate(piezas_sistema):
            nid = f"n4_{i}"
            nodos[nid] = _usable("¿Se puede usar así?", [p], "se_perdio", kind, f"Le falta «{p}». Se pide con sus medidas para que bodega la busque.")
            ops4.append(_o(p, "puzzle", ir=nid))
        ops4.append(_o("No sé cómo se llama", "camera-fill", res=_res(
            "pedir", piezas=["Otra pieza"], falla="se_perdio", por_que="No necesitas saber el nombre: foto de la pieza y del lugar, y bodega la busca.")))
        ops4.append(_otra_cosa(kind))
        nodos["n4"] = {"q": "¿Qué pieza falta?", "ops": ops4}
        raiz.append(_o("Le falta una pieza", "puzzle", ir="n4"))
    else:
        raiz.append(_o("Le falta una pieza", "puzzle", res=_res(
            "pedir", piezas=["Otra pieza"], falla="se_perdio", por_que="Foto de la pieza y del lugar, y bodega la busca.")))

    # ── camino 5: roto, fisurado o gastado ──
    ops5 = []
    for i, p in enumerate(piezas_sistema):
        nid = f"n5_{i}"
        nodos[nid] = _usable("¿Se puede usar así?", [p], "se_corto", kind, f"«{p}» rota o gastada. Se pide con sus medidas para que bodega la busque.")
        ops5.append(_o(p, "tools", ir=nid))
    nodos["n5e"] = {"q": "¿Cómo está la estructura?", "ops": [
        _o("Fisura chica o soldadura abierta", "exclamation-octagon-fill", res=_res(
            "fs", motivo="estructura", riesgo="si", por_que="Una fisura o soldadura abierta puede ceder: no se usa hasta que se repare.",
            ademas=[_res("incidencia", texto="Fisura o soldadura abierta en la estructura (reparación en taller)", por_que="Que gestión lo sepa el mismo día.")],
            aviso="Pon un letrero o huincha antes de irte.")),
        _o("Doblada, quebrada o muy oxidada", "slash-circle", res=_res(
            "baja", motivo="estructura", por_que="Una estructura doblada, quebrada o muy oxidada no se recupera. Pide foto y diagnóstico.",
            ademas=[_res("fs", motivo="estructura", riesgo="si", rotulo="Dejarlo fuera de servicio mientras gestión decide")])),
        _otra_cosa(kind)]}
    ops5.append(_o("La estructura: tubo, soldadura, chasis", "exclamation-octagon", ir="n5e"))
    ops5.append(_o("Plástico o tapa", "brush", res=_res(
        "incidencia", texto="Plástico o tapa dañada", por_que="Es estético: avisa sin urgencia, con una foto.")))
    ops5.append(_otra_cosa(kind))
    nodos["n5"] = {"q": "¿Qué está roto, fisurado o gastado?", "ops": ops5}
    raiz.append(_o("Está roto, fisurado o gastado", "exclamation-octagon", ir="n5"))

    # ── camino 6: llegó incompleto o distinto (solo producto de la revisión) ──
    if kind == "i":
        ops6 = [
            _o("Faltan unidades", "123", res=_res("nollego", que="no_venia", por_que="Si faltan unidades, queda anotado como «No llegó» con las que faltan.")),
            _o("Llegó otro modelo", "arrow-left-right", res=_res(
                "nollego", que="otro_producto", por_que="Llegó otro producto: queda anotado. Saca foto de la etiqueta en una incidencia.")),
            _o("Llegó golpeado o dañado", "box-arrow-in-down", res=_res(
                "incidencia", texto="Llegó golpeado o dañado", por_que="Si llegó, el producto sí está y se revisa. El daño se informa con foto.")),
        ]
        if piezas_sistema:
            nodos["n6a"] = {"q": "¿Qué le falta?", "ops": [
                _o(p, "puzzle", res=_res("pedir", piezas=[p], falla="se_perdio",
                                          por_que="El producto sí llegó: lo que falta se pide como repuesto y el checklist sigue normal."))
                for p in piezas_sistema] + [
                _o("Manual o tornillería", "journal-text", res=_res(
                    "incidencia", texto="Llegó sin manual o sin tornillería", por_que="Se informa como incidencia; el producto se revisa igual.")),
                _otra_cosa(kind)]}
            ops6.insert(1, _o("Llegó, pero le falta una pieza o accesorio", "puzzle", ir="n6a"))
        ops6.append(_otra_cosa(kind))
        nodos["n6"] = {"q": "¿Qué pasó con este producto?", "ops": ops6}
        raiz.append(_o("Llegó incompleto o distinto", "boxes", ir="n6"))

    # ── camino 7: no hay acceso (solo equipo de cliente) ──
    if kind == "e":
        ops7 = []
        for motivo, txt, ic in (("sin_acceso", "No me dejaron entrar o no había quién abriera", "door-closed"),
                                ("equipo_en_uso", "Estaba en uso por socios", "people"),
                                ("cliente_no_permite", "El cliente no quiso que lo tocara", "person-x"),
                                ("sin_energia", "No hay energía", "plug"),
                                ("danado_inaccesible", "Está dañado o inaccesible", "exclamation-octagon")):
            nid = "n7_" + motivo
            extra = ([_res("incidencia", texto="El cliente no permitió revisar el equipo (anota el nombre y el cargo de quien lo dijo)",
                           por_que="Que gestión lo sepa el mismo día.")] if motivo in ("cliente_no_permite", "sin_acceso") else None)
            nodos[nid] = {"q": "¿Hay que volver?", "ops": [
                _o("Sí, otro día", "calendar-event", res=_res("noesta", opcion="no_revisado", motivo=motivo, texto="Hay que volver otro día (reagendar).",
                                                           por_que="Queda anotado el motivo y que hay que reagendar.", ademas=extra)),
                _o("No hace falta", "x-circle", res=_res("noesta", opcion="no_revisado", motivo=motivo, texto="No hace falta volver.",
                                                       por_que="Queda anotado el motivo, sin reagendar.", ademas=extra)),
                _o("No sé, que decida gestión", "question-circle", res=_res(
                    "noesta", opcion="no_revisado", motivo=motivo, texto="Que gestión decida si hay que volver.", por_que="Gestión decide si se vuelve.",
                    ademas=extra)),
                _otra_cosa(kind)]}
            ops7.append(_o(txt, ic, ir=nid))
        ops7.append(_otra_cosa(kind))
        nodos["n7"] = {"q": "¿Qué impidió revisarlo?", "ops": ops7}
        raiz.append(_o("No lo pude revisar: no hay acceso", "door-closed", ir="n7"))

    # ── camino 8: otra cosa ──
    nodos["n8"] = {"q": "¿Puede lastimar a alguien o frena el trabajo?", "ops": [
        _o("Sí", "cone-striped", res=_res(
            "fs", motivo="otro", riesgo="si", por_que="Si puede lastimar a alguien o frena el trabajo, no se usa hasta que se revise.",
            ademas=[_res("incidencia", texto="", por_que="Cuéntalo con una foto para que gestión y bodega lo vean.")],
            aviso="Pon un letrero o huincha antes de irte.")),
        _o("No", "check-circle", res=_res("incidencia", texto="", por_que="Cuéntalo con una foto y una frase (puedes dictarla).")),
        _otra_cosa(kind),
    ]}
    raiz.append(_o("Otra cosa", "megaphone", ir="n8"))

    nodos["q0"] = {"q": "¿Qué le pasa a este " + ("producto" if kind == "i" else "equipo") + "?", "ops": raiz}
    arb = {"raiz": "q0", "nodos": nodos}
    arb["prof"] = {nid: preguntas_maximas(arb, nid) for nid in nodos}      # «Pregunta 2 de 3» en pantalla
    return arb


def preguntas_maximas(arb, nodo=None, _vistos=None):
    """Mayor cantidad de preguntas (pantallas de pregunta) por un camino del árbol, contando la primera."""
    nodo = nodo or arb["raiz"]
    n = arb["nodos"][nodo]
    best = 0
    for op in n["ops"]:
        if op.get("ir"):
            best = max(best, preguntas_maximas(arb, op["ir"]))
    return 1 + best


def recorrer(arb):
    """Todos los caminos del árbol: [(lista de (pregunta, respuesta), resultado)]. Para las pruebas."""
    out = []

    def _r(nodo, camino):
        for op in arb["nodos"][nodo]["ops"]:
            paso = camino + [(arb["nodos"][nodo]["q"], op["t"])]
            if op.get("ir"):
                _r(op["ir"], paso)
            else:
                out.append((paso, op["res"]))

    _r(arb["raiz"], [])
    return out


# ═══════════════════════════════ lo que se envía al teléfono ═══════════════════════════════
_NO_PUBLICO_CAMPO = ("r", "ap", "tol")


def ficha_publica(fid):
    """La ficha tal como la recibe el teléfono: sin las plantillas del resumen ni los datos del aprendizaje."""
    f = FICHAS.get(fid)
    if not f:
        return None
    out = {k: v for k, v in f.items() if k not in ("resumen", "campos")}
    out["campos"] = []
    for c in f["campos"]:
        pc = {k: v for k, v in c.items() if k not in _NO_PUBLICO_CAMPO and v not in (None, "", False)}
        pc.update({"k": c["k"], "y": c["y"], "t": c["t"]})
        out["campos"].append(pc)
    return json.loads(json.dumps(out, ensure_ascii=False))


def para_front(sistema, kind, nombres_piezas=(), fichas_extra=()):
    """Lo que necesita el teléfono para pedir con medidas y para el «¿Qué le pasa?»: solo las fichas de las piezas de este equipo
    (más «Otra pieza»), qué ficha tiene cada nombre, el árbol y los datos de apoyo. Sin proveedores ni costos."""
    nombres = []
    for n in list(pe.SUGERIDOS_POR_SISTEMA.get(sistema or "", ())) + list(nombres_piezas or ()):
        if n not in nombres:
            nombres.append(n)
    ficha_de = {n: ficha_para(n) for n in nombres}
    arb = arbol(kind, sistema)
    ids = {"H"} | set(ficha_de.values()) | {f for f in (fichas_extra or ()) if f in FICHAS}
    for _c, res in recorrer(arb):
        for p in (res.get("piezas") or []):
            ficha_de.setdefault(p, ficha_para(p))
            ids.add(ficha_de[p])
    return {
        "sistema": sistema, "ficha_de": ficha_de, "fichas": {i: ficha_publica(i) for i in sorted(ids)},
        "claves": list(CLAVES), "arbol": arb, "seguridad": list(PIEZAS_SEGURIDAD), "atajo": ATAJO_PELIGRO,
        "medido_con": [list(x) for x in MEDIDO_CON], "motivos_no_pude": [list(x) for x in MOTIVOS_NO_PUDE],
        "rodamientos": {k: list(v) for k, v in RODAMIENTOS.items()}, "factor_imposible": FACTOR_IMPOSIBLE,
    }


# ═══════════════════════════════ el camino recorrido (para guardarlo con la solicitud) ═══════════════════════════════
def normalizar_diagnostico(d):
    """Limpia el «camino» que manda el teléfono para guardarlo junto a la solicitud. None si no hay nada válido. Solo texto corto:
    preguntas y respuestas, la acción sugerida y las piezas; nunca datos de nadie."""
    if not isinstance(d, dict):
        return None
    camino = []
    for p in (d.get("camino") or [])[:6]:
        if isinstance(p, dict):
            q = " ".join(str(p.get("q") or "").split())[:200]
            r = " ".join(str(p.get("r") or "").split())[:200]
            if q or r:
                camino.append({"q": q, "r": r})
    if not camino:
        return None
    accion = str(d.get("accion") or "")
    piezas = [" ".join(str(x).split())[:120] for x in (d.get("piezas") or [])[:6] if str(x or "").strip()]
    return {"camino": camino, "accion": accion if accion in ACCIONES else "", "piezas": piezas,
            "sistema": " ".join(str(d.get("sistema") or "").split())[:40],
            "siguio": bool(d.get("siguio")), "atajo": bool(d.get("atajo"))}


def diagnostico_txt(d):
    """Una línea para el ticket y las pantallas: «¿Qué le pasa? → Hace ruido → Poleas y piola → Sí, se mueve»."""
    if not d or not d.get("camino"):
        return ""
    return " → ".join(p.get("r") or "" for p in d["camino"] if p.get("r"))

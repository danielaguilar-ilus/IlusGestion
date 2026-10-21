"""Estándar «N° de serie + GPS» en TODA plantilla de checklist (REGLA #39). Funciones PURAS: sin base, sin Flask.

Daniel (2026-10-21): «el número de serie, vamos a tener que usar el objeto… no quiero un botón con una observación,
un texto libre… si no lo tienen, agrégalo a la fuerza para que las plantillas estén estandarizadas. Y todas las
plantillas al crearse lleven esto por obligación, el número de serie y el GPS». Motivo de negocio: la etiqueta de
respaldo/garantía y saber si los equipos fueron despegados o vulnerados.

«El objeto» es el tipo de respuesta `serie` (automático / manual / N-A) que ya existe; el GPS es el tipo `gps`.

Cómo se usa (REGLA #32: lo aplica una PERSONA, con vista previa y confirmación; el arranque jamás toca plantillas):
  · `analizar(items)` y `plan(items)` dicen qué le falta a una plantilla y qué se haría.
  · `huella(items)` identifica lo que la persona vio: si la plantilla cambió entre la vista previa y el clic, no se toca.
  · `completar_items_nueva(items)` hace que una plantilla NUEVA nazca con los dos objetos.
  · `faltantes_por_clase(...)` calcula qué clases del catálogo no tienen plantilla en cada categoría.
"""
import hashlib
import json
import unicodedata

SERIE_TITULO = "N° de serie del equipo"
SERIE_DESC = ("Número de serie real de fábrica. Si la ficha ya lo tiene se usa ese; si no, se anota el de la placa. "
              "Con la etiqueta de respaldo/garantía puesta, queda registrado para saber si el equipo fue manipulado.")
GPS_TITULO = "Ubicación del equipo (GPS)"
GPS_DESC = "Ubicación donde queda o se revisa el equipo. Se captura con el GPS del celular."

TIPO_SERIE = "serie"
TIPO_GPS = "gps"
# Tipos de respuesta que un ítem «N° de serie» escrito a mano puede tener antes de existir el objeto.
TIPOS_SERIE_CONVERTIBLES = ("texto", "check", "numero")

CATEGORIAS = ("instalacion", "mantencion", "visitas", "trabajo_interno")
# La vista previa deja marcadas estas categorías al crear las plantillas que faltan por clase (Daniel 21-oct: también
# «Trabajo interno», como plantilla de filtro con sufijo, sin la etiqueta de clasificación hacia el motor).
CATEGORIAS_CLASE_POR_DEFECTO = ("instalacion", "mantencion", "visitas", "trabajo_interno")
TIPO_VISITA_POR_CATEGORIA = {
    "instalacion": "instalacion", "mantencion": "preventiva", "visitas": "inspeccion", "trabajo_interno": "otro",
}


def norm(txt):
    """Minúsculas, sin tildes ni signos, espacios simples. Solo para COMPARAR."""
    s = unicodedata.normalize("NFD", str(txt or ""))
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    s = "".join(ch if ch.isalnum() else " " for ch in s.lower())
    return " ".join(s.split())


def es_lu(nombre):
    """Las plantillas «(LU) …» son de Lenin Urbina: solo se tocan con la confirmación expresa de Daniel."""
    return str(nombre or "").strip().upper().startswith("(LU)")


def es_bloque_revision(nombre):
    """Los bloques «Revisión — …» se aplican UNA vez por producto dentro de una OT interna: un GPS por cada producto
    no tiene sentido. Quedan sin marcar por defecto en la vista previa (la persona puede marcarlos)."""
    return norm(nombre).startswith("revision ")


def _tipo(it):
    return str((it or {}).get("tipo_respuesta") or "check").strip().lower()


def _es_serie_convertible(it):
    """Ítem de texto libre que en realidad es el N° de serie (nunca una foto ni una pregunta de sí/no)."""
    if _tipo(it) not in TIPOS_SERIE_CONVERTIBLES:
        return False
    t = norm((it or {}).get("titulo"))
    if not t or "foto" in t.split() or "placa" in t.split():
        return False
    return "serie" in t.split() or "serial" in t.split()


def _ordenar(items):
    def clave(it):
        try:
            o = int(it.get("orden") or 0)
        except (TypeError, ValueError):
            o = 0
        try:
            i = int(it.get("id") or 0)
        except (TypeError, ValueError):
            i = 0
        return (o, i)
    return sorted(list(items or []), key=clave)


def analizar(items):
    """Qué tiene hoy la plantilla. `items`: [{id, orden, titulo, tipo_respuesta}]."""
    its = _ordenar(items)
    tiene_serie = any(_tipo(i) == TIPO_SERIE for i in its)
    tiene_gps = any(_tipo(i) == TIPO_GPS for i in its)
    convertible = None
    if not tiene_serie:
        convertible = next((i for i in its if _es_serie_convertible(i)), None)
    return {"tiene_serie": tiene_serie, "tiene_gps": tiene_gps, "serie_convertible": convertible,
            "n_items": len(its)}


def plan(items):
    """Qué se haría para dejarla estándar. Devuelve:
      estado: 'ok' | 'falta_serie' | 'falta_gps' | 'falta_ambos'
      acciones: lista ordenada ('convertir_serie' | 'agregar_serie' | 'agregar_gps')
      agregan: títulos que se agregarían · convierte: título del ítem que pasaría a ser el objeto (o None)"""
    a = analizar(items)
    acciones = []
    if not a["tiene_serie"]:
        acciones.append("convertir_serie" if a["serie_convertible"] else "agregar_serie")
    if not a["tiene_gps"]:
        acciones.append("agregar_gps")
    falta_s = not a["tiene_serie"]
    falta_g = not a["tiene_gps"]
    estado = "ok" if not acciones else ("falta_ambos" if falta_s and falta_g else ("falta_serie" if falta_s else "falta_gps"))
    agregan = []
    if "agregar_serie" in acciones:
        agregan.append(SERIE_TITULO)
    if "agregar_gps" in acciones:
        agregan.append(GPS_TITULO)
    convierte = (a["serie_convertible"] or {}).get("titulo") if "convertir_serie" in acciones else None
    return {"estado": estado, "acciones": acciones, "agregan": agregan, "convierte": convierte,
            "n_items": a["n_items"], "id_convertible": (a["serie_convertible"] or {}).get("id")}


def huella(items):
    """Huella corta de lo que la persona vio. El servidor la recalcula antes de escribir."""
    base = [[int((i.get("id") or 0)), str(i.get("titulo") or ""), _tipo(i), int(i.get("orden") or 0)]
            for i in _ordenar(items)]
    return hashlib.sha1(json.dumps(base, ensure_ascii=False).encode("utf-8")).hexdigest()[:16]


def item_serie(orden):
    return {"orden": orden, "titulo": SERIE_TITULO, "descripcion": SERIE_DESC, "tipo_respuesta": TIPO_SERIE,
            "obligatoria": 1, "requiere_foto": 0, "foto_si_hallazgo": 0, "unidad": None, "rango_min": None,
            "rango_max": None, "opciones_lista_json": None, "target_field": None}


def item_gps(orden):
    return {"orden": orden, "titulo": GPS_TITULO, "descripcion": GPS_DESC, "tipo_respuesta": TIPO_GPS,
            "obligatoria": 1, "requiere_foto": 0, "foto_si_hallazgo": 0, "unidad": None, "rango_min": None,
            "rango_max": None, "opciones_lista_json": None, "target_field": None}


def ordenes_para_aplicar(items):
    """Cálculo PURO de lo que el servidor escribe, para probarlo sin base.

    Devuelve {'convertir_id': id|None, 'nuevos': [item...], 'desplazar': n}:
      · los ítems tocados (convertido / agregados) quedan ARRIBA: serie primero, GPS después;
      · el resto conserva su orden relativo y se corre `desplazar` lugares (los que no son el convertido)."""
    p = plan(items)
    tops = []
    convertir_id = None
    nuevos = []
    if "convertir_serie" in p["acciones"]:
        convertir_id = p["id_convertible"]
        tops.append("convertido")
    elif "agregar_serie" in p["acciones"]:
        tops.append("serie")
    if "agregar_gps" in p["acciones"]:
        tops.append("gps")
    for pos, que in enumerate(tops, start=1):
        if que == "serie":
            nuevos.append(item_serie(pos))
        elif que == "gps":
            nuevos.append(item_gps(pos))
    orden_convertido = (tops.index("convertido") + 1) if "convertido" in tops else None
    return {"convertir_id": convertir_id, "orden_convertido": orden_convertido, "nuevos": nuevos,
            "desplazar": len(tops)}


def completar_items_nueva(items_norm):
    """Para el POST que CREA una plantilla desde el editor: devuelve (items, agregados) con los dos objetos al
    principio y los demás renumerados. `items_norm`: ítems ya validados (dict con orden, titulo, tipo_respuesta…).
    Si ya traen el objeto, no se duplica."""
    its = list(items_norm or [])
    base = [{"id": i + 1, "orden": it.get("orden") or (i + 1), "titulo": it.get("titulo"),
             "tipo_respuesta": it.get("tipo_respuesta")} for i, it in enumerate(its)]
    p = plan(base)
    if not p["acciones"]:
        return its, []
    agregados = []
    tops = []
    resto = list(its)
    if "convertir_serie" in p["acciones"]:
        idx = (p["id_convertible"] or 0) - 1
        conv = dict(resto.pop(idx))
        conv.update({"tipo_respuesta": TIPO_SERIE, "obligatoria": 1})
        tops.append(conv)
        agregados.append("«" + str(conv.get("titulo") or SERIE_TITULO) + "» pasó a ser el objeto N° de serie")
    elif "agregar_serie" in p["acciones"]:
        tops.append(item_serie(0))
        agregados.append(SERIE_TITULO)
    if "agregar_gps" in p["acciones"]:
        tops.append(item_gps(0))
        agregados.append(GPS_TITULO)
    out = tops + resto
    for n, it in enumerate(out, start=1):
        it["orden"] = n
    return out, agregados


SUFIJO_TRABAJO_INTERNO = " (trabajo interno)"


def nombre_para_categoria(nombre_clase, categoria):
    """Nombre que lleva la plantilla de una clasificación en una categoría. «Trabajo interno» es de FILTRO: lleva el
    sufijo para que el motor (que busca por clasificación) nunca la elija."""
    n = str(nombre_clase or "").strip()
    return n + SUFIJO_TRABAJO_INTERNO if categoria == "trabajo_interno" else n


def faltantes_por_clase(clases, plantillas, categorias=CATEGORIAS):
    """Clases activas del catálogo sin plantilla en una categoría. Una plantilla cuenta si está LIGADA a la clase por su
    llave (`clase_id`, REGLA #40) o, si no tiene liga, si se llama como se llamaría (nombre normalizado + categoría).
    Cuenta TAMBIÉN las desactivadas (el índice único uq_plant_nombre_cat impediría crear otra con ese nombre).
    clases: [{id, nombre}] · plantillas: [{nombre, categoria_admin, clase_id?}] → [{clase_id, clase, categoria}]"""
    ya = set()
    ya_llave = set()
    for p in plantillas or []:
        cat = str(p.get("categoria_admin") or "").strip().lower()
        ya.add((norm(p.get("nombre")), cat))
        if p.get("clase_id"):
            ya_llave.add((int(p["clase_id"]), cat))
    out = []
    vistos = set()
    for c in clases or []:
        nombre = str(c.get("nombre") or "").strip()
        if not norm(nombre):
            continue
        cid = int(c.get("id") or 0)
        for cat in categorias:
            n = norm(nombre_para_categoria(nombre, cat))
            if (n, cat) in ya or (cid, cat) in ya_llave or (n, cat) in vistos:
                continue
            vistos.add((n, cat))
            out.append({"clase_id": cid, "clase": nombre, "categoria": cat})
    return out

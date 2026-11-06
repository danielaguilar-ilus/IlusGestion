# -*- coding: utf-8 -*-
"""FILAS DE EQUIPOS DEL ASISTENTE «Generar OT» (Paso «Equipos y su checklist») — una fila con CANTIDAD ↔ una fila POR UNIDAD, y el
checklist que se SUGIERE por producto y se RECUERDA en el catálogo (Daniel, 5-nov-2026).

Daniel, con la captura de 10 filas idénticas «Rubber EPDM Flooring Corner Ramp G/B 20mm» (una línea del documento de 10 unidades que el sistema
partió sola en 10 equipos de la ficha, cada uno con su serie genérica y su checklist por buscar): «quisiera tener la opción de REVERTIR… dame la
opción de MULTIPLICAR LA FILA o dejarlo con UNA FILA CON 18. Además me cuesta mucho seleccionar el checklist, esto parte tiene que ser
INTELIGENTE. Y también GUARDARLA EN LOS CATÁLOGOS si lo seleccionan de allí».

QUÉ HAY ACÁ (módulo PURO: sin base de datos, sin Flask; lo prueba tests/test_ot_equipos_filas.py y lo usa app.py):
  · `sugerir_conteo`   — «una fila con cantidad» o «una fila por unidad» para un producto, con el MOTIVO dicho a una persona (el sistema SUGIERE,
                         la persona decide: REGLA #32).
  · `armar_pista`      — todo lo que el asistente necesita saber de UN producto en una sola respuesta: su conteo sugerido, el checklist guardado
                         para ese producto y el que sale de su clase.
  · `validar_agrupar` / `validar_separar` — cuándo se puede JUNTAR N filas de la ficha en una sola con cantidad N, y cuándo se puede SEPARAR una fila
                         de cantidad N en N (o devolver las que se juntaron). Solo filas recién creadas por el asistente y que ninguna OT usa: lo que
                         ya tiene historia no se toca (la ficha es el centro de trazabilidad del cliente).

NADA se borra: juntar deja las filas sobrantes en estado «baja» (se pueden restaurar con «Separar») y todo queda en una tabla de agrupaciones y en
la bitácora (REGLA #5: soft-delete + auditoría). Los datos del ERP no se tocan (REGLA #4.1).
"""
import datetime as _dt
import re
import unicodedata

CONTEO_CANTIDAD = "cantidad"          # una fila con cantidad N
CONTEO_UNIDAD = "unidad"              # una fila por unidad (N filas de cantidad 1)

MAX_DIAS_ELEGIBLE = 7                 # una fila solo se junta o se separa si nació hace menos de esto…
MAX_UNIDADES_SEPARAR = 100            # …y dividir 400 pisos en 400 fichas no ayuda a nadie: sobre este tope se deja como cantidad
MAX_FILAS_AGRUPAR = 500


def normalizar(texto):
    """Minúsculas, sin tildes y con todo lo que no es letra o número reducido a un espacio («Rampa-Esquina» → «rampa esquina»)."""
    t = unicodedata.normalize("NFKD", str(texto or ""))
    t = "".join(ch for ch in t if not unicodedata.combining(ch)).lower()
    return " ".join(re.sub(r"[^a-z0-9]+", " ", t).split())


def clave_doc(doc):
    """El documento de origen sin separadores ni ceros a la izquierda del número («FCV 0011117» y «FCV-11117» son el mismo papel)."""
    s = re.sub(r"[^A-Z0-9]", "", str(doc or "").upper())
    return re.sub(r"(?<=[A-Z])0+(?=\d)", "", s)


def clave_sku(sku):
    return str(sku or "").strip().upper()


# ── Qué productos parecen un piso o un accesorio que se cuenta por cantidad (cuando el catálogo todavía no los clasificó) ─────────────────────
# Lista corta y a propósito conservadora: es solo una SUGERENCIA (la persona la cambia con un toque) y equivocarse hacia «una fila por unidad» es
# lo barato; hacerlo al revés dejaría tres máquinas reales en una sola fila. Palabras ya normalizadas (sin tildes, minúsculas).
PALABRAS_PISO_ACCESORIO = frozenset({
    "piso", "pisos", "flooring", "floor", "pavimento", "caucho", "rubber", "epdm", "tatami", "colchoneta", "colchonetas",
    "mat", "mats", "rampa", "rampas", "ramp", "accesorio", "accesorios", "disco", "discos", "mancuerna", "mancuernas",
    "collarin", "collarines", "tobillera", "tobilleras", "pelota", "pelotas", "bosu", "cuerda", "cuerdas", "soga", "sogas",
})
FRASES_PISO_ACCESORIO = ("banda elastica", "bandas elasticas", "liga elastica", "ligas elasticas", "correa de tobillo", "correas de tobillo")


def parece_piso_o_accesorio(nombre):
    """(sí, la palabra que lo delató). Solo por el NOMBRE: es una pista, no un hecho."""
    t = normalizar(nombre)
    if not t:
        return False, ""
    relleno = f" {t} "
    for f in FRASES_PISO_ACCESORIO:
        if f" {f} " in relleno:
            return True, f
    for palabra in t.split():
        if palabra in PALABRAS_PISO_ACCESORIO:
            return True, palabra
    return False, ""


def sugerir_conteo(nombre, cantidad, clasificacion=None):
    """Cómo se cuenta un producto de una línea con `cantidad` unidades. Devuelve {valor, motivo, origen}:
         valor  → 'cantidad' (una fila con cantidad N) | 'unidad' (una fila por unidad)
         motivo → la frase para una persona, con el ORIGEN a la vista (REGLA #32); '' si la línea es de una sola unidad
         origen → 'una_unidad' | 'clase' (lo dice la clasificación del catálogo) | 'nombre' (lo sugiere el nombre) | 'defecto'
       `clasificacion` = {'clase_nombre': str, 'repetible': True|False|None} (lo que devuelve _inc_clasificacion_sku). Piso/accesorio del catálogo
       (modelo_precio 'fijo') → por cantidad; equipo del catálogo → por unidad; sin clasificar → por unidad, salvo que el nombre delate un piso o un
       accesorio (entonces por cantidad, dicho así). Con la duda, por unidad: cada máquina lleva su serie y su checklist."""
    try:
        n = int(float(cantidad))
    except (TypeError, ValueError):
        n = 1
    if n < 2:
        return {"valor": CONTEO_UNIDAD, "motivo": "", "origen": "una_unidad"}
    c = clasificacion or {}
    clase = str(c.get("clase_nombre") or "").strip()
    if c.get("repetible") is True:
        return {"valor": CONTEO_CANTIDAD, "origen": "clase",
                "motivo": f"Sugerido: es un {(clase or 'piso o accesorio').lower()} (catálogo): se cuenta por cantidad, no por unidad."}
    if c.get("repetible") is False:
        return {"valor": CONTEO_UNIDAD, "origen": "clase",
                "motivo": f"Sugerido: es un equipo{(' (' + clase.lower() + ')') if clase else ''}: una fila por unidad, cada una con su serie y su checklist."}
    ok, palabra = parece_piso_o_accesorio(nombre)
    if ok:
        return {"valor": CONTEO_CANTIDAD, "origen": "nombre",
                "motivo": f"Sugerido: por su nombre («{palabra}») parece un piso o accesorio y se cuenta por cantidad. Si son máquinas, sepáralas por unidad."}
    return {"valor": CONTEO_UNIDAD, "origen": "defecto",
            "motivo": "Sin clasificar en el catálogo: una fila por unidad por seguridad (cada máquina lleva su serie). Si es un piso o accesorio, júntalas en una fila."}


def armar_pista(sku, nombre, cantidad, clasificacion=None, guardada=None, de_clase=None):
    """La respuesta del servidor para UN producto del asistente. `guardada` / `de_clase` = {plantilla_id, nombre, ...} o None. Lo que se ve elegido es
    lo que recibe el técnico (REGLA #32): esto solo SUGIERE, con su origen, y la persona decide."""
    c = clasificacion or {}
    return {
        "sku": clave_sku(sku),
        "clase": c.get("clase"), "clase_nombre": c.get("clase_nombre"), "repetible": c.get("repetible"),
        "conteo": sugerir_conteo(nombre, cantidad, c),
        "guardada": guardada or None,
        "de_clase": de_clase or None,
    }


# ── Juntar y separar filas de la ficha ───────────────────────────────────────────────────────────────────────────────────────────────────────
def _a_fecha(v):
    if isinstance(v, _dt.datetime):
        return v
    if isinstance(v, _dt.date):
        return _dt.datetime(v.year, v.month, v.day)
    try:
        return _dt.datetime.fromisoformat(str(v).strip()[:19].replace("T", " "))
    except (TypeError, ValueError):
        return None


def _cantidad(fila):
    try:
        return max(1, int(float(fila.get("cantidad") or 1)))
    except (TypeError, ValueError):
        return 1


def _muy_vieja(fila, ahora):
    creada = _a_fecha(fila.get("created_at"))
    if creada is None or ahora is None:
        return False                       # sin fecha no se puede juzgar: la guarda de «nadie la usa» sigue mandando
    return (ahora - creada) > _dt.timedelta(days=MAX_DIAS_ELEGIBLE)


def _usada(fila, en_uso):
    return fila.get("origen_visita_id") not in (None, "", 0) or int(fila.get("id") or 0) in set(int(x) for x in (en_uso or ()))


_TXT = {
    "POCAS": "Para juntar hacen falta al menos dos filas del mismo producto.",
    "MUCHAS": "Son demasiadas filas para juntarlas de una vez.",
    "CLIENTE": "Esas filas no son de la ficha de este cliente.",
    "BAJA": "Una de las filas ya está dada de baja.",
    "SKU": "Solo se pueden juntar filas del MISMO producto.",
    "DOC": "Solo se pueden juntar filas del MISMO documento de origen.",
    "USADA": "Una de las filas ya se usó en una OT (o está ligada a una): lo que tiene historia no se junta ni se separa.",
    "ANTIGUA": f"Esas filas son de hace más de {MAX_DIAS_ELEGIBLE} días: ya no son «recién creadas» y no se juntan ni se separan desde aquí.",
    "UNA": "Esa fila ya es de una sola unidad.",
    "MUCHAS_UNIDADES": f"Son más de {MAX_UNIDADES_SEPARAR} unidades: separarlas en una ficha cada una no ayuda. Déjalo como una fila con cantidad.",
    "NO_ENCONTRADA": "No se encontró la fila.",
}


def _no(codigo, extra=None):
    return (False, codigo, _TXT.get(codigo, codigo), extra)


def validar_agrupar(filas, cliente_id, ahora=None, en_uso=()):
    """¿Se pueden JUNTAR estas filas de la ficha en una sola con cantidad N? `filas` = dicts de mant_maquinas (id, cliente_id, sku, doc_origen, cantidad,
    estado, origen_visita_id, created_at). `en_uso` = ids que alguna OT ya usa. Devuelve (ok, codigo, texto, datos): con ok, datos =
    {principal_id, bajas:[ids], total}. Queda la fila de menor id; las demás pasan a «baja» (se pueden restaurar)."""
    filas = [f for f in (filas or []) if f]
    if len(filas) < 2:
        return _no("POCAS")
    if len(filas) > MAX_FILAS_AGRUPAR:
        return _no("MUCHAS")
    if any(int(f.get("cliente_id") or 0) != int(cliente_id or 0) for f in filas):
        return _no("CLIENTE")
    if any(str(f.get("estado") or "activo").strip().lower() == "baja" for f in filas):
        return _no("BAJA")
    if len({clave_sku(f.get("sku")) for f in filas}) != 1 or not clave_sku(filas[0].get("sku")):
        return _no("SKU")
    if len({clave_doc(f.get("doc_origen")) for f in filas}) != 1:
        return _no("DOC")
    if any(_usada(f, en_uso) for f in filas):
        return _no("USADA")
    if any(_muy_vieja(f, ahora) for f in filas):
        return _no("ANTIGUA")
    ordenadas = sorted(filas, key=lambda f: int(f["id"]))
    principal = int(ordenadas[0]["id"])
    return (True, "OK", "", {"principal_id": principal, "bajas": [int(f["id"]) for f in ordenadas[1:]],
                             "total": sum(_cantidad(f) for f in ordenadas)})


def validar_separar(fila, cliente_id, ahora=None, en_uso=(), agrupacion=()):
    """¿Se puede SEPARAR esta fila (cantidad N)? `agrupacion` = las filas hermanas que se juntaron en ella antes y siguen en «baja»
    [{hermana_id, cantidad_hermana, estado_hermana}]. Devuelve (ok, codigo, texto, datos). Con ok, datos = {modo, ...}:
      · modo 'revertir'   → se restauran las hermanas que se habían juntado: {restaurar:[ids], cantidad_principal}
      · modo 'multiplicar' → se crean N-1 filas nuevas de cantidad 1 y la fila queda en 1: {crear:N-1, cantidad_principal:1}"""
    if not fila:
        return _no("NO_ENCONTRADA")
    if int(fila.get("cliente_id") or 0) != int(cliente_id or 0):
        return _no("CLIENTE")
    if str(fila.get("estado") or "activo").strip().lower() == "baja":
        return _no("BAJA")
    if _usada(fila, en_uso):
        return _no("USADA")
    if _muy_vieja(fila, ahora):
        return _no("ANTIGUA")
    n = _cantidad(fila)
    hermanas = [h for h in (agrupacion or ()) if str(h.get("estado_hermana") or "").strip().lower() == "baja"]
    if hermanas:
        sumadas = sum(_cantidad({"cantidad": h.get("cantidad_hermana")}) for h in hermanas)
        return (True, "OK", "", {"modo": "revertir", "restaurar": [int(h["hermana_id"]) for h in hermanas],
                                 "cantidad_principal": max(1, n - sumadas)})
    if n < 2:
        return _no("UNA")
    if n > MAX_UNIDADES_SEPARAR:
        return _no("MUCHAS_UNIDADES")
    return (True, "OK", "", {"modo": "multiplicar", "crear": n - 1, "cantidad_principal": 1})


def resumen_agrupar(nombre, total):
    return f"Quedó una sola fila con cantidad {total} para «{nombre}». Puedes volver a separarlas cuando quieras."


def resumen_separar(nombre, modo, n):
    if modo == "revertir":
        return f"Se separaron las {n} filas de «{nombre}»: cada una vuelve con su serie."
    return f"«{nombre}» quedó en {n} filas, una por unidad, cada una con su serie."

# -*- coding: utf-8 -*-
"""Preparación de un retiro según CheckWMS (bodega): interpretar GetSeguimientoDespacho.

Funciones PURAS (sin BD, sin red, sin Flask): pickups_module.py consulta a Check (solo lectura,
REGLA #4.4) y entrega las filas ya leídas; aquí solo se decide qué significan.
Se prueban sin conexión en tests/test_retiros_check.py.

Daniel 2026-10-02: "quiero que el sistema identifique con Check si está preparado y lo
cambie automáticamente... que se entienda".

Qué trae Check por cada línea de un documento (muestra real BLV 23732, todo como texto):
solicitado, noAsignado, asignado, pickeado, revisado, despachado, cancelado, devuelto.
Recorrido de una unidad en bodega:
    pedido en Check → stock asignado → pickeado (juntado) → revisado → despachado.

REGLA DE «PREPARADO» (decisión de producto, ajustable aquí): el pedido está preparado cuando
TODAS las líneas pedidas (menos las canceladas) ya fueron pickeadas o están más adelante
(revisadas / despachadas). No se exige la revisión final: para un retiro el cliente se lleva
lo que bodega juntó.

La decisión se toma LÍNEA POR LÍNEA (una línea sobre-pickeada no compensa a otra sin empezar) y
es conservadora ante datos raros: números no finitos o negativos, filas de otro documento o
columnas incoherentes nunca dan «listo».

Ambigüedad conocida: no sabemos si las columnas son «cubetas» exclusivas (cada unidad cuenta en
UNA sola columna) o acumulativas. Por línea se detecta con una identidad: si
noAsignado+asignado+pickeado+revisado+despachado+cancelado == solicitado son exclusivas y
«unidades que ya pasaron de pickeado» = pickeado+revisado+despachado; si no, se toma la columna
más avanzada (máximo), que nunca da un falso «listo». Ante la duda, no marca preparado.

DESPACHADO: si Check ya da unidades por despachadas, el pedido puede estar entregado antes (doble
despacho). Eso se MUESTRA pero NO se marca solo (`listo_auto` = False): lo decide una persona.
"""
import math

ETAPAS = (
    # (clave, número, título corto, explicación en simple)
    ("pedido", 1, "Check recibió el pedido", "El documento ya está cargado en el sistema de bodega."),
    ("asignado", 2, "Check asignó el stock", "Ya se reservó la mercadería para este pedido."),
    ("pickeado", 3, "Bodega juntó los productos", "Los productos ya fueron sacados de su ubicación (pickeado)."),
    ("revisado", 4, "Revisión final", "Un segundo control de que lo juntado está correcto."),
    ("despachado", 5, "Entregado en Check", "Check ya lo dio por entregado/despachado."),
)

_CAMPOS = ("solicitado", "noAsignado", "asignado", "pickeado", "revisado", "despachado", "cancelado", "devuelto")
_EPS = 0.0001


def _num(valor):
    """Check manda los números como texto ('1', '0', ''). Vacío = 0. None si NO es un número
    utilizable (texto raro, infinito, NaN o negativo): esa línea no puede dar «listo»."""
    try:
        s = str(valor).strip().replace(",", ".") if valor is not None else ""
        if not s:
            return 0.0
        f = float(s)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(f) or f < 0:
        return None
    return f


def _digitos(valor):
    d = "".join(c for c in str(valor or "") if c.isdigit())
    return d.lstrip("0") or ("0" if d else "")


def resumir_filas(filas, tipo=None, num=None):
    """Líneas de UN documento. Si se pasan `tipo` y `num`, descarta las filas que Check marque como
    de OTRO documento. Devuelve None si no queda ninguna fila (Check no lo tiene)."""
    lineas = []
    for f in (filas or []):
        if not isinstance(f, dict):
            continue
        if tipo and f.get("tipoDocumento") not in (None, "") and str(f.get("tipoDocumento")).strip().upper() != str(tipo).strip().upper():
            continue
        if num and f.get("numeroDocumento") not in (None, "") and _digitos(f.get("numeroDocumento")) != _digitos(num):
            continue
        linea = {}
        malo = False
        for c in _CAMPOS:
            v = _num(f.get(c))
            if v is None:
                malo = True
                v = 0.0
            linea[c] = v
        linea["_malo"] = malo
        lineas.append(linea)
    if not lineas:
        return None
    return {"lineas": lineas}


def _exclusivas(l):
    suma = l["noAsignado"] + l["asignado"] + l["pickeado"] + l["revisado"] + l["despachado"] + l["cancelado"]
    return abs(suma - l["solicitado"]) < 0.01 or abs(suma + l["devuelto"] - l["solicitado"]) < 0.01


def _linea(l):
    """(pendiente, adelantadas, asignadas, revisadas, despachadas) de UNA línea, todas acotadas a lo pendiente."""
    pend = max(l["solicitado"] - l["cancelado"], 0.0)
    if _exclusivas(l):
        adel = l["pickeado"] + l["revisado"] + l["despachado"]
        asig = l["asignado"] + adel
        rev = l["revisado"] + l["despachado"]
    else:
        adel = max(l["pickeado"], l["revisado"], l["despachado"])
        asig = max(l["asignado"], adel)
        rev = max(l["revisado"], l["despachado"])
    return pend, min(adel, pend), min(asig, pend), min(rev, pend), min(l["despachado"], pend)


def evaluar(resumenes):
    """Estado de preparación de un retiro.

    `resumenes`: lista con UN resumen por documento (None = Check no tiene ese documento).
    Devuelve un dict listo para mostrar:
      estado   'sin_documento' | 'sin_datos' | 'en_proceso' | 'listo'
      listo    True solo si TODAS las líneas pedidas de TODOS los documentos están pickeadas o más allá
      listo_auto  listo Y sin unidades ya despachadas ni datos raros: lo único que se marca solo
      iniciada  bodega YA EMPEZÓ a juntar (alguna unidad pickeada o más allá) y los datos se entienden
      iniciada_auto  iniciada Y sin unidades despachadas: es la señal con la que ILUS pasa solo el retiro a «En preparación»
      pickeadas  unidades pickeadas (o más allá)
      pedidas  unidades pedidas (menos canceladas)
      etapas   por etapa: clave, n, titulo, texto, hechas, de, completa
      faltan   unidades que aún no están pickeadas
      alerta   texto de advertencia (despachado / datos raros) o ''
      frase    una línea en simple para el operador
    """
    resumenes = list(resumenes or [])
    if not resumenes:
        return _vacio("sin_documento", "Este retiro no tiene documentos para buscar en Check.", 0)
    con_datos = [r for r in resumenes if r]
    if not con_datos:
        return _vacio("sin_datos", "Check todavía no tiene este documento. Puede tardar un poco en llegar a bodega.", len(resumenes))

    pedidas = adelantadas = asignadas = revisadas = despachadas = 0.0
    docs_pendientes = docs_completos = 0
    malos = False
    for r in con_datos:
        lineas_pend = lineas_ok = 0
        for l in r["lineas"]:
            if l.get("_malo"):
                malos = True
            pend, adel, asig, rev, desp = _linea(l)
            pedidas += pend
            adelantadas += adel
            asignadas += asig
            revisadas += rev
            despachadas += desp
            if pend > 0:
                lineas_pend += 1
                if adel >= pend - _EPS and not l.get("_malo"):
                    lineas_ok += 1
        if lineas_pend:
            docs_pendientes += 1
            if lineas_ok == lineas_pend:
                docs_completos += 1

    todos_con_datos = len(con_datos) == len(resumenes)
    listo = bool(todos_con_datos and not malos and docs_pendientes > 0 and docs_completos == docs_pendientes)
    faltan = max(pedidas - adelantadas, 0.0)
    alerta = ""
    if malos:
        alerta = "Check devolvió números que no se entienden: no se marca solo. Usa la lista manual."
    elif despachadas > 0:
        alerta = ("Check ya da unidades por DESPACHADAS. Verifica que este pedido no se haya entregado antes "
                  "de entregárselo al cliente: no se marca solo.")
    listo_auto = bool(listo and not alerta)
    # «Bodega ya empezó» (Daniel 2026-10-02: «envíes a preparación en automático»): alguna unidad ya fue pickeada. Si Check ya
    # da algo por despachado, el pedido pudo entregarse antes: se muestra pero NO se usa para mover el retiro solo.
    iniciada = bool(adelantadas > 0.0001 and not malos)       # misma tolerancia que _ent(): «0,0000001» no es una unidad pickeada
    iniciada_auto = bool(iniciada and despachadas <= 0)

    def etapa(clave, n, titulo, texto, hechas):
        return {"clave": clave, "n": n, "titulo": titulo, "texto": texto,
                "hechas": _ent(hechas), "de": _ent(pedidas),
                "completa": pedidas > 0 and hechas >= pedidas - _EPS}

    etapas = [
        etapa("pedido", 1, ETAPAS[0][2], ETAPAS[0][3], pedidas),
        etapa("asignado", 2, ETAPAS[1][2], ETAPAS[1][3], asignadas),
        etapa("pickeado", 3, ETAPAS[2][2], ETAPAS[2][3], adelantadas),
        etapa("revisado", 4, ETAPAS[3][2], ETAPAS[3][3], revisadas),
        etapa("despachado", 5, ETAPAS[4][2], ETAPAS[4][3], despachadas),
    ]
    if malos:
        estado, frase = "en_proceso", "Check devolvió datos que no se entienden. Usa la lista manual de abajo."
    elif pedidas <= 0:
        estado, frase = "en_proceso", "Check tiene este pedido cancelado o sin unidades pedidas."
    elif listo:
        estado, frase = "listo", "Check confirma que bodega ya juntó todo el pedido."
    elif not todos_con_datos:
        estado, frase = "en_proceso", "Check solo tiene algunos de los documentos de este retiro; falta el resto."
    elif adelantadas > 0:
        estado, frase = "en_proceso", f"Bodega va juntando el pedido: faltan {_ent(faltan)} de {_ent(pedidas)} unidades."
    else:
        estado, frase = "en_proceso", "Check ya tiene el pedido, pero bodega todavía no empieza a juntarlo."
    return {"estado": estado, "listo": listo, "listo_auto": listo_auto, "pedidas": _ent(pedidas), "faltan": _ent(faltan),
            "pickeadas": _ent(adelantadas), "iniciada": iniciada, "iniciada_auto": iniciada_auto,
            "etapas": etapas, "frase": frase, "alerta": alerta,
            "documentos_con_datos": len(con_datos), "documentos": len(resumenes)}


def _ent(x):
    """12.0 → 12 ; 2.5 → 2.5 (las unidades casi siempre son enteras)."""
    return int(x) if abs(x - round(x)) < 0.0001 else round(x, 2)


def _vacio(estado, frase, n_docs):
    return {"estado": estado, "listo": False, "listo_auto": False, "pedidas": 0, "faltan": 0, "etapas": [],
            "pickeadas": 0, "iniciada": False, "iniciada_auto": False,
            "frase": frase, "alerta": "", "documentos_con_datos": 0, "documentos": n_docs}

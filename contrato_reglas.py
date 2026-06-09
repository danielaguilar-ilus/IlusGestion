"""
Motor DETERMINISTA de análisis de contratos ILUS — CERO IA / CERO tokens.

Carga la biblioteca de reglas (contrato_reglas.json) y evalúa el TEXTO de un
contrato por coincidencia de patrones (presencia/ausencia), produciendo un
score 0-100, nivel de riesgo y las secciones (alertas críticas, puntos a
revisar, cláusulas relevantes, cláusulas favorables, propuestas de mejora).

Es ALIMENTABLE: para enseñarle algo nuevo, edita contrato_reglas.json
(agrega una regla con sus patrones). No usa IA ni red: misma entrada => misma salida.

Diseño de la biblioteca: workflow `disenar-reglas-contrato-ilus` (8 dimensiones).
La versión del motor se LEE del campo `version` del JSON (no se hardcodea).

Scoring: base 100 + bonificaciones - penalizaciones, con TOPE de penalización
por dimensión (evita que una avalancha de ausencias hunda el score), acotado a
[0,100]. nivel_riesgo: alto <60, medio 60-79, bajo >=80; override: cualquier
alerta crítica nunca deja el nivel en 'bajo' (mínimo 'medio').

Robustez para contratos ATÍPICOS (sin IA):
  - Detecta el TIPO de documento (mantención/arriendo/comodato/compraventa/
    garantía/otro) por palabras clave. Las reglas de AUSENCIA propias de
    mantención NO penalizan si el doc no es de servicio.
  - Si dispara <3 reglas Y el texto es corto (<800) Y no se extrajo ningún
    campo => nivel_riesgo 'indeterminado' + cobertura_parcial=True (no inventa
    'bajo' ni 'alto').
  - `_extraer_campos()` saca por regex (determinista) RUTs, montos CLP/UF,
    vigencia, plazo, frecuencia y SLA, y se incluyen en `campos_extraidos`.
"""
import os
import re
import json
import unicodedata

_DIR = os.path.dirname(os.path.abspath(__file__))
_REGLAS_PATH = os.path.join(_DIR, "contrato_reglas.json")
_CACHE = {"reglas": None, "mtime": None, "version": None}

# Versión por defecto si el JSON no la declara (evita None en el dict de retorno).
_VERSION_DEFAULT = "contrato-reglas-v1"


def _norm(s):
    """minúsculas + sin tildes + espacios colapsados (para matching robusto)."""
    s = (s or "").lower()
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    s = re.sub(r"\s+", " ", s)
    return s


def cargar_reglas(force=False):
    """Carga (y cachea por mtime) la biblioteca de reglas desde el JSON.

    Además guarda en caché la `version` declarada en el JSON (la expone
    `version_motor()`)."""
    try:
        mt = os.path.getmtime(_REGLAS_PATH)
    except OSError:
        mt = None
    if (not force) and _CACHE["reglas"] is not None and _CACHE["mtime"] == mt:
        return _CACHE["reglas"]
    version = _VERSION_DEFAULT
    try:
        with open(_REGLAS_PATH, encoding="utf-8") as f:
            data = json.load(f)
        reglas = data.get("reglas", []) or []
        version = (data.get("version") or _VERSION_DEFAULT)
    except Exception:
        reglas = []
    _CACHE["reglas"] = reglas
    _CACHE["mtime"] = mt
    _CACHE["version"] = version
    return reglas


def version_motor():
    """Versión del motor leída del JSON (cae al default si no se ha cargado)."""
    if _CACHE["version"] is None:
        cargar_reglas()
    return _CACHE["version"] or _VERSION_DEFAULT


# Detecta si justo antes del patrón (misma frase) hay una negación, para no
# disparar "incluye repuestos" cuando el texto dice "NO incluye repuestos", ni
# "arriendo" cuando dice "NO constituye arriendo". Ya no se ancla a un largo
# fijo: la ventana se recorta al inicio de la frase actual (último .;: antes
# del match), así "no constituye, salvo pacto, arriendo" sigue contando como
# negado aunque la negación esté lejos del patrón.
_NEG_RE = re.compile(
    r"\b(no|sin|ni|tampoco|jamas|nunca|excluye|excluid[oa]s?|salvo|excepto)\b[^.;:]*$"
)
# Separadores de frase: cualquiera marca el inicio de la "frase actual".
_SEP_RE = re.compile(r"[.;:]")


def _inicio_frase(t_norm, idx):
    """Índice del inicio de la frase que contiene la posición `idx`
    (carácter justo después del último separador .;: previo, o 0)."""
    ini = 0
    for m in _SEP_RE.finditer(t_norm, 0, idx):
        ini = m.end()
    return ini


def _negado_en(t_norm, idx):
    """True si entre el inicio de la frase y `idx` hay una negación pendiente."""
    pre = t_norm[_inicio_frase(t_norm, idx):idx]
    return bool(_NEG_RE.search(pre))


def _ocurre_no_negado(t_norm, patron):
    """True si el patrón aparece al menos una vez SIN una negación pendiente
    desde el inicio de su frase."""
    start = 0
    while True:
        idx = t_norm.find(patron, start)
        if idx == -1:
            return False
        if not _negado_en(t_norm, idx):
            return True  # ocurrencia no negada -> cuenta
        start = idx + len(patron)


def _ocurre_o_negado(t_norm, patron):
    """True si el patrón NO aparece en absoluto O aparece SIEMPRE negado.
    Es la condición que dispara una regla de AUSENCIA: la cláusula buena
    está ausente o está negada (ej. 'no incluye garantia legal')."""
    return not _ocurre_no_negado(t_norm, patron)


def _dispara(regla, t_norm):
    pres = regla.get("patrones_presencia") or []
    aus = regla.get("patrones_ausencia") or []
    # presencia: el patrón aparece (y NO está negado).
    # ausencia: dispara si TODOS los patrones están ausentes o negados (la
    # cláusula protectora no está presente afirmativamente).
    hit_pres = any(_ocurre_no_negado(t_norm, _norm(p)) for p in pres) if pres else None
    hit_aus = all(_ocurre_o_negado(t_norm, _norm(p)) for p in aus) if aus else None
    if pres and aus:
        return bool(hit_pres) and bool(hit_aus)
    if pres:
        return bool(hit_pres)
    if aus:
        return bool(hit_aus)
    return False


# ───────────────────────── EXTRACCIÓN DETERMINISTA ─────────────────────────
# Regex sobre el texto ORIGINAL (conserva mayúsculas/puntos) para RUTs/montos,
# y sobre el texto NORMALIZADO (_norm) para fechas/palabras. CERO IA.

_RE_RUT = re.compile(r"(\d{1,2}\.?\d{3}\.?\d{3}-[\dkK])")
_RE_CLP = re.compile(r"(?:\$|clp|pesos)\s*([\d\.]+)", re.IGNORECASE)
_RE_UF = re.compile(r"uf\s*([\d\.,]+)", re.IGNORECASE)
_RE_FECHA = re.compile(r"\b(\d{1,2}/\d{1,2}/\d{2,4})\b")
_RE_PLAZO = re.compile(r"plazo\s+de\s+(\d+)\s*(mes|meses|ano|anos|year|years)")
_RE_CADA = re.compile(r"cada\s+(\d+)\s*(mes|meses|dia|dias|semana|semanas)")
_RE_SLA = re.compile(r"(\d+)\s*horas?\s*(habiles|corridas)?")

_FREC_PALABRA = {
    "mensual": 1, "bimensual": 2, "bimestral": 2, "trimestral": 3,
    "cuatrimestral": 4, "semestral": 6, "anual": 12,
}


def _clp_a_int(s):
    """'2.226.000' -> 2226000 (quita los puntos de miles chilenos)."""
    try:
        return int(s.replace(".", "").strip())
    except (ValueError, AttributeError):
        return None


def _extraer_campos(texto):
    """Extracción determinista de campos clave del contrato (sin IA).

    Devuelve un dict con: ruts, montos_clp, monto_principal, montos_uf,
    vigencia_inicio, vigencia_fin, plazo_meses, es_indefinido,
    frecuencia_meses, sla_horas. Faltantes => None / [].
    """
    texto = texto or ""
    t_norm = _norm(texto)
    campos = {
        "ruts": [], "montos_clp": [], "monto_principal": None, "montos_uf": [],
        "vigencia_inicio": None, "vigencia_fin": None, "plazo_meses": None,
        "es_indefinido": False, "frecuencia_meses": None, "sla_horas": None,
    }

    # RUTs (texto original: conserva puntos y guion)
    ruts, seen = [], set()
    for m in _RE_RUT.finditer(texto):
        r = m.group(1)
        if r not in seen:
            seen.add(r)
            ruts.append(r)
    campos["ruts"] = ruts

    # Montos CLP (texto original) — normaliza miles quitando puntos.
    montos = []
    for m in _RE_CLP.finditer(texto):
        v = _clp_a_int(m.group(1))
        # descarta basura tipo "$0" o cifras sin sentido (1-2 dígitos sueltos)
        if v and v >= 1000:
            montos.append(v)
    campos["montos_clp"] = montos
    if montos:
        campos["monto_principal"] = max(montos)

    # Montos UF (texto original)
    ufs = []
    for m in _RE_UF.finditer(texto):
        raw = m.group(1).strip().rstrip(".,")
        if raw:
            ufs.append(raw)
    campos["montos_uf"] = ufs

    # Vigencia: indefinido / renovación automática
    if ("indefinido" in t_norm) or ("renovacion automatica" in t_norm) \
            or ("renueva automaticamente" in t_norm):
        campos["es_indefinido"] = True

    # Fechas DD/MM/YYYY (texto normalizado) — primera = inicio, segunda = fin
    fechas = _RE_FECHA.findall(t_norm)
    if fechas:
        campos["vigencia_inicio"] = fechas[0]
        if len(fechas) > 1:
            campos["vigencia_fin"] = fechas[1]

    # Plazo de N (mes|meses|año|años)
    mp = _RE_PLAZO.search(t_norm)
    if mp:
        n = int(mp.group(1))
        unidad = mp.group(2)
        campos["plazo_meses"] = n * 12 if unidad.startswith("ano") or unidad.endswith("years") or unidad == "year" else n

    # Frecuencia: "cada N (mes|dia|semana)" primero, luego palabra clave.
    mc = _RE_CADA.search(t_norm)
    if mc:
        n = int(mc.group(1))
        unidad = mc.group(2)
        if unidad.startswith("mes"):
            campos["frecuencia_meses"] = n
        elif unidad.startswith("semana"):
            campos["frecuencia_meses"] = max(1, round(n / 4.0))
        elif unidad.startswith("dia"):
            campos["frecuencia_meses"] = max(1, round(n / 30.0))
    if campos["frecuencia_meses"] is None:
        for palabra, meses in _FREC_PALABRA.items():
            if re.search(r"\b" + palabra + r"\b", t_norm):
                campos["frecuencia_meses"] = meses
                break

    # SLA: "N horas (habiles|corridas)"
    ms = _RE_SLA.search(t_norm)
    if ms:
        try:
            campos["sla_horas"] = int(ms.group(1))
        except ValueError:
            pass

    return campos


# ───────────────────────── TIPO DE DOCUMENTO ─────────────────────────
# Detección determinista del tipo de documento por palabras clave. El orden
# importa: mantención/servicio gana sobre arriendo cuando ambos aparecen
# fuertemente, salvo señales claras de arriendo/leasing/comodato/compraventa.
_TIPO_KEYWORDS = {
    "mantencion": ["servicio de mantencion", "servicios de mantencion", "contrato de mantencion",
                   "mantencion preventiva", "mantencion correctiva", "mantenimiento",
                   "servicio tecnico", "prestacion de servicios"],
    "arriendo": ["contrato de arriendo", "contrato de arrendamiento", "arrendamiento de equipos",
                 "arriendo de equipos", "el arrendador", "el arrendatario", "canon de arriendo",
                 "leasing", "arrendamiento financiero"],
    "comodato": ["comodato", "prestamo de uso", "cesion de uso", "equipos en consignacion"],
    "compraventa": ["compraventa", "contrato de compra", "venta de equipos", "venta de maquinas",
                    "transferencia de dominio", "transferencia de propiedad", "precio de venta"],
    "garantia": ["certificado de garantia", "poliza de garantia", "carta de garantia",
                 "garantia del fabricante"],
}


def _detectar_tipo_documento(t_norm):
    """Devuelve (tipo, scores) — tipo en {mantencion, arriendo, comodato,
    compraventa, garantia, otro}. Determinista por conteo de keywords."""
    scores = {}
    for tipo, kws in _TIPO_KEYWORDS.items():
        scores[tipo] = sum(1 for kw in kws if kw in t_norm)
    # mantención prevalece si tiene señal y nadie la supera claramente
    mant = scores.get("mantencion", 0)
    ganador, mejor = "otro", 0
    for tipo, sc in scores.items():
        if sc > mejor:
            ganador, mejor = tipo, sc
    if mejor == 0:
        return "otro", scores
    # Empate o cercanía: mantención gana (es el caso de negocio principal ILUS).
    if mant > 0 and mant >= mejor:
        return "mantencion", scores
    return ganador, scores


# Etiquetas legibles para el resumen.
_TIPO_LABEL = {
    "mantencion": "SERVICIO DE MANTENCIÓN", "arriendo": "ARRIENDO/LEASING",
    "comodato": "COMODATO", "compraventa": "COMPRAVENTA",
    "garantia": "GARANTÍA", "otro": "OTRO",
}

# Reglas de AUSENCIA que solo tienen sentido para contratos de servicio/
# mantención (no se penaliza su ausencia en comodato/compraventa/otro).
_DIMS_SOLO_MANTENCION = {
    "cobertura_exclusiones", "sla_respuesta", "inventario_equipos",
    "garantia_terceros",
}

# Reglas de AUSENCIA puntuales (dimensión tipo_contrato) que penalizan por
# faltar lenguaje de mantención. En un doc que NO es mantención, su "disparo"
# solo confirma el tipo y no debe penalizar (sería un 'alto' falso).
_REGLAS_AUSENCIA_MANTENCION = {
    "tipo_sin_mencion_mantencion", "tipo_objeto_contrato_ambiguo",
}


def _items(disparadas, tipo):
    return [{"id": r.get("id"), "mensaje": r.get("mensaje", ""),
             "severidad": r.get("severidad"), "categoria": r.get("categoria"),
             "propuesta": r.get("propuesta") or "", "base_legal": r.get("base_legal") or ""}
            for r in disparadas if r.get("tipo") == tipo]


# Tope de penalización acumulada por dimensión, para que una avalancha de
# reglas de ausencia en una misma dimensión no hunda artificialmente el score.
_TOPE_PENALIZACION_DIMENSION = -15


def _resumen_campos(campos):
    """Frase corta con los campos extraídos (vigencia/monto/frecuencia/partes)."""
    if not campos:
        return ""
    trozos = []
    vi, vf = campos.get("vigencia_inicio"), campos.get("vigencia_fin")
    if campos.get("es_indefinido"):
        trozos.append("Vigencia indefinida")
    elif vi and vf:
        trozos.append(f"Vigencia {vi}→{vf}")
    elif vi:
        trozos.append(f"Vigencia desde {vi}")
    if campos.get("plazo_meses"):
        trozos.append(f"plazo {campos['plazo_meses']} meses")
    if campos.get("monto_principal"):
        trozos.append(f"${campos['monto_principal']:,}".replace(",", ".") + "/mes")
    elif campos.get("montos_uf"):
        trozos.append(f"UF {campos['montos_uf'][0]}")
    if campos.get("frecuencia_meses"):
        trozos.append(f"frecuencia {campos['frecuencia_meses']} mes(es)")
    if campos.get("sla_horas"):
        trozos.append(f"SLA {campos['sla_horas']} h")
    if campos.get("ruts"):
        trozos.append("partes: " + ", ".join(campos["ruts"][:3]))
    return " · ".join(trozos)


def _resumen(score, nivel, alertas, puntos, favorables, tipo=None,
             campos=None, cobertura_parcial=False):
    if nivel == "indeterminado":
        return ("No reconocí la estructura de este contrato (puede ser atípico "
                "o incompleto): revísalo manualmente.")
    p = [f"Análisis determinista del contrato (sin IA). Score {score}/100, riesgo {nivel}."]
    if tipo and tipo != "mantencion":
        p.append(f"El documento parece {_TIPO_LABEL.get(tipo, tipo.upper())}; "
                 "las reglas de mantención no aplican plenamente.")
    extra = _resumen_campos(campos)
    if extra:
        p.append(extra + ".")
    if alertas:
        p.append(f"{len(alertas)} alerta(s) crítica(s) que exigen revisión humana.")
    if puntos:
        p.append(f"{len(puntos)} punto(s) a revisar.")
    if favorables:
        p.append(f"{len(favorables)} cláusula(s) favorable(s) para ILUS.")
    if not alertas and not puntos:
        p.append("No se detectaron riesgos relevantes con las reglas actuales.")
    if cobertura_parcial:
        p.append("Cobertura parcial: revisa manualmente lo no reconocido.")
    return " ".join(p)


def _score_acotado(disparadas):
    """Suma de pesos con tope de penalización por dimensión.

    Las penalizaciones (peso < 0) de una misma dimensión se acotan a
    `_TOPE_PENALIZACION_DIMENSION`. Las bonificaciones (peso > 0) suman sin tope.
    """
    pen_por_dim = {}
    bono = 0
    for r in disparadas:
        peso = int(r.get("peso_score") or 0)
        if peso >= 0:
            bono += peso
        else:
            dim = r.get("dimension") or r.get("categoria") or "_sin_dim"
            pen_por_dim[dim] = pen_por_dim.get(dim, 0) + peso
    pen_total = sum(max(_TOPE_PENALIZACION_DIMENSION, p) for p in pen_por_dim.values())
    return max(0, min(100, 100 + bono + pen_total))


def analizar_contrato(texto, reglas=None, overrides=None):
    """Analiza el texto del contrato (determinista). Devuelve un dict estable
    listo para el panel del front y para el PDF.

    overrides: dict {rule_id: {"activa": bool, "peso": int|None}} editable desde
    el front (panel de reglas) y persistido en BD. Desactiva reglas o ajusta su
    peso sin tocar el archivo base."""
    if reglas is None:
        reglas = cargar_reglas()
        motor_version = version_motor()
    else:
        motor_version = version_motor()
    if overrides:
        _aj = []
        for r in reglas:
            ov = overrides.get(r.get("id"))
            if ov:
                if ov.get("activa") is False:
                    continue  # regla desactivada desde el front
                if ov.get("peso") is not None:
                    r = {**r, "peso_score": int(ov["peso"])}
            _aj.append(r)
        reglas = _aj
    t = _norm(texto or "")
    base = {
        "motor": motor_version,
        "total_reglas": len(reglas),
        "alertas_criticas": [], "puntos_revisar": [], "clausulas_relevantes": [],
        "clausulas_favorables": [], "propuestas_mejora": [],
        "tipo_documento": None, "cobertura_parcial": False,
        "campos_extraidos": _extraer_campos(texto),
    }
    if not t.strip():
        base.update({"ok": False, "score": None, "nivel_riesgo": None, "disparadas": 0,
                     "resumen": "No se pudo leer texto del contrato (¿PDF escaneado o vacío?)."})
        return base

    campos = base["campos_extraidos"]
    tipo_doc, _tipo_scores = _detectar_tipo_documento(t)

    # Filtra reglas de AUSENCIA propias de mantención cuando el doc NO es de
    # servicio/mantención (no penalizar ausencias de mantención en comodato,
    # compraventa, garantía u otro). Las reglas de PRESENCIA (detectan tipo,
    # arriendo, etc.) y las de la dimensión tipo_contrato siempre se evalúan.
    es_servicio = tipo_doc == "mantencion"
    reglas_eval = []
    for r in reglas:
        dim = r.get("dimension") or ""
        rid = r.get("id")
        es_ausencia = bool(r.get("patrones_ausencia")) and not r.get("patrones_presencia")
        if (not es_servicio) and es_ausencia and (
                dim in _DIMS_SOLO_MANTENCION or rid in _REGLAS_AUSENCIA_MANTENCION):
            continue  # ausencia de mantención no aplica a este tipo de doc
        reglas_eval.append(r)

    disparadas = [r for r in reglas_eval if _dispara(r, t)]
    score = _score_acotado(disparadas)
    hay_critica = any(r.get("tipo") == "alerta_critica" for r in disparadas)

    # ── Documento atípico / incompleto: no inventar 'bajo' ni 'alto' ──
    campos_vacios = (not campos.get("ruts") and not campos.get("montos_clp")
                     and not campos.get("montos_uf")
                     and campos.get("frecuencia_meses") is None
                     and campos.get("sla_horas") is None
                     and not campos.get("vigencia_inicio")
                     and not campos.get("es_indefinido"))
    cobertura_parcial = False
    if len(disparadas) < 3 and len(t) < 800 and campos_vacios:
        nivel = "indeterminado"
        cobertura_parcial = True
        score = None
    else:
        if score < 60:
            nivel = "alto"
        elif score < 80:
            nivel = "medio"
        else:
            nivel = "medio" if hay_critica else "bajo"

    alertas = _items(disparadas, "alerta_critica")
    puntos = _items(disparadas, "punto_revisar")
    clausulas = _items(disparadas, "clausula_relevante")
    favorables = _items(disparadas, "clausula_favorable")
    props, seen = [], set()
    for r in disparadas:
        pr = (r.get("propuesta") or "").strip()
        if pr and pr.lower() not in seen:
            seen.add(pr.lower())
            props.append({"id": r.get("id"), "propuesta": pr, "base_legal": r.get("base_legal") or ""})

    base.update({
        "ok": True, "score": score, "nivel_riesgo": nivel,
        "hay_alerta_critica": hay_critica, "disparadas": len(disparadas),
        "tipo_documento": tipo_doc, "cobertura_parcial": cobertura_parcial,
        "alertas_criticas": alertas, "puntos_revisar": puntos,
        "clausulas_relevantes": clausulas, "clausulas_favorables": favorables,
        "propuestas_mejora": props,
        "resumen": _resumen(score, nivel, alertas, puntos, favorables,
                            tipo=tipo_doc, campos=campos,
                            cobertura_parcial=cobertura_parcial),
    })
    return base

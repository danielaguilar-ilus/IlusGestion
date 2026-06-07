"""
Motor DETERMINISTA de análisis de contratos ILUS — CERO IA / CERO tokens.

Carga la biblioteca de reglas (contrato_reglas.json) y evalúa el TEXTO de un
contrato por coincidencia de patrones (presencia/ausencia), produciendo un
score 0-100, nivel de riesgo y las secciones (alertas críticas, puntos a
revisar, cláusulas relevantes, cláusulas favorables, propuestas de mejora).

Es ALIMENTABLE: para enseñarle algo nuevo, edita contrato_reglas.json
(agrega una regla con sus patrones). No usa IA ni red: misma entrada => misma salida.

Diseño de la biblioteca: workflow `disenar-reglas-contrato-ilus` (36 reglas,
8 dimensiones). Scoring: base 100 + suma de peso_score de cada regla disparada,
acotado a [0,100]. nivel_riesgo: alto <60, medio 60-79, bajo >=80, con override:
cualquier alerta crítica disparada nunca deja el nivel en 'bajo' (mínimo 'medio').
"""
import os
import re
import json
import unicodedata

_DIR = os.path.dirname(os.path.abspath(__file__))
_REGLAS_PATH = os.path.join(_DIR, "contrato_reglas.json")
_CACHE = {"reglas": None, "mtime": None}


def _norm(s):
    """minúsculas + sin tildes + espacios colapsados (para matching robusto)."""
    s = (s or "").lower()
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    s = re.sub(r"\s+", " ", s)
    return s


def cargar_reglas(force=False):
    """Carga (y cachea por mtime) la biblioteca de reglas desde el JSON."""
    try:
        mt = os.path.getmtime(_REGLAS_PATH)
    except OSError:
        mt = None
    if (not force) and _CACHE["reglas"] is not None and _CACHE["mtime"] == mt:
        return _CACHE["reglas"]
    try:
        with open(_REGLAS_PATH, encoding="utf-8") as f:
            data = json.load(f)
        reglas = data.get("reglas", []) or []
    except Exception:
        reglas = []
    _CACHE["reglas"] = reglas
    _CACHE["mtime"] = mt
    return reglas


# Detecta si justo antes del patrón (misma frase) hay una negación, para no
# disparar "incluye repuestos" cuando el texto dice "NO incluye repuestos", ni
# "arriendo" cuando dice "NO constituye arriendo".
_NEG_RE = re.compile(r"\b(no|sin|ni|tampoco|jamas|nunca|excluye|excluido|excluidos)\b[^.;:]{0,20}$")


def _ocurre_no_negado(t_norm, patron):
    """True si el patrón aparece al menos una vez SIN una negación inmediatamente antes."""
    start = 0
    while True:
        idx = t_norm.find(patron, start)
        if idx == -1:
            return False
        pre = t_norm[max(0, idx - 26):idx]
        if not _NEG_RE.search(pre):
            return True  # ocurrencia no negada -> cuenta
        start = idx + len(patron)


def _dispara(regla, t_norm):
    pres = regla.get("patrones_presencia") or []
    aus = regla.get("patrones_ausencia") or []
    # presencia: el patrón aparece (y NO está negado). ausencia: el patrón no aparece en absoluto.
    hit_pres = any(_ocurre_no_negado(t_norm, _norm(p)) for p in pres) if pres else None
    hit_aus = (not any(_norm(p) in t_norm for p in aus)) if aus else None
    if pres and aus:
        return bool(hit_pres) and bool(hit_aus)
    if pres:
        return bool(hit_pres)
    if aus:
        return bool(hit_aus)
    return False


def _items(disparadas, tipo):
    return [{"id": r.get("id"), "mensaje": r.get("mensaje", ""),
             "severidad": r.get("severidad"), "categoria": r.get("categoria"),
             "propuesta": r.get("propuesta") or "", "base_legal": r.get("base_legal") or ""}
            for r in disparadas if r.get("tipo") == tipo]


def _resumen(score, nivel, alertas, puntos, favorables):
    p = [f"Análisis determinista del contrato (sin IA). Score {score}/100, riesgo {nivel}."]
    if alertas:
        p.append(f"{len(alertas)} alerta(s) crítica(s) que exigen revisión humana.")
    if puntos:
        p.append(f"{len(puntos)} punto(s) a revisar.")
    if favorables:
        p.append(f"{len(favorables)} cláusula(s) favorable(s) para ILUS.")
    if not alertas and not puntos:
        p.append("No se detectaron riesgos relevantes con las reglas actuales.")
    return " ".join(p)


def analizar_contrato(texto, reglas=None):
    """Analiza el texto del contrato (determinista). Devuelve un dict estable
    listo para el panel del front y para el PDF."""
    reglas = reglas if reglas is not None else cargar_reglas()
    t = _norm(texto or "")
    base = {
        "motor": "contrato-reglas-v1",
        "total_reglas": len(reglas),
        "alertas_criticas": [], "puntos_revisar": [], "clausulas_relevantes": [],
        "clausulas_favorables": [], "propuestas_mejora": [],
    }
    if not t.strip():
        base.update({"ok": False, "score": None, "nivel_riesgo": None, "disparadas": 0,
                     "resumen": "No se pudo leer texto del contrato (¿PDF escaneado o vacío?)."})
        return base

    disparadas = [r for r in reglas if _dispara(r, t)]
    score = max(0, min(100, 100 + sum(int(r.get("peso_score") or 0) for r in disparadas)))
    hay_critica = any(r.get("tipo") == "alerta_critica" for r in disparadas)
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
        "alertas_criticas": alertas, "puntos_revisar": puntos,
        "clausulas_relevantes": clausulas, "clausulas_favorables": favorables,
        "propuestas_mejora": props,
        "resumen": _resumen(score, nivel, alertas, puntos, favorables),
    })
    return base

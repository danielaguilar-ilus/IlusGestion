"""
Motor de tarifas de courier — réplica EXACTA de la macro Excel de SPHS
(Transporte_y_Distribucion_7.2.xlsm).

Modelo validado contra la macro (doc 10687, Temuco, peso 1123,92, seguro 31.930):
  Starken Enviame 460.031 · FedEx Enviame 451.265 · FedEx Directo 372.511
  Blue 824.840 · Starken Directo 522.858 · Felca 352.721 · Clickex 471.383
Todos cuadran al peso.

Reglas (igual que la macro):
  • peso <= LIGHT_MAX (100, Clickex 130): precio = celda de la tabla (col = kg exacto).
  • peso  > LIGHT_MAX: precio = factor_$/kg_del_tramo × peso.
  • Clickex en tramos altos puede traer "factor + fijo" → factor×peso + fijo.
  • + seguro = valor_declarado × 0,012  (se SUMA al precio del courier, como la macro).
  • NO se aplica margen ni IVA: la tabla YA es el precio final de SPHS.
"""
import json
import os
import unicodedata

_BASE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tarifas")
_CACHE = {}

# Tramos pesados por courier: (peso_min, peso_max, col_index). Tomados del CÓDIGO
# de la macro (no de las etiquetas de header, que a veces no coinciden).
TIERS = {
    "starken_enviame": [(101, 130, "107"), (131, 500, "108"), (501, 10**9, "109")],
    "fedex_enviame":   [(101, 10**9, "107")],
    "fedex_directo":   [(100, 499, "105"), (500, 1999, "106"), (2000, 3999, "107"),
                        (4000, 5999, "108"), (6000, 10**9, "109")],
    "blue_enviame":    [(101, 10**9, "105")],
    "starken_directo": [(101, 10**9, "104")],
    "milling":         [(101, 3999, "105")],
    "felca":           [(101, 3999, "105"), (4000, 5999, "106"), (6000, 20000, "107")],
    "clickex":         [(131, 500, "140"), (501, 1000, "141"), (1001, 5000, "142"),
                        (5001, 10000, "143"), (10001, 20000, "144")],
}
LIGHT_MAX = {"clickex": 130}  # resto = 100

# Felca/Milling sin tabla propia → estimar como FedEx Directo un poco más barato.
FALLBACK_FACTOR = 0.90

# Etiqueta amigable por slug (para la UI / trace)
NOMBRE = {
    "starken_enviame": "Starken (Envíame)",
    "fedex_enviame":   "FedEx (Envíame)",
    "fedex_directo":   "FedEx Directo",
    "blue_enviame":    "Blue Express",
    "starken_directo": "Starken Directo",
    "milling":         "Transportes Milling",
    "felca":           "Transportes Felca",
    "clickex":         "Clickex",
}


def _strip(s):
    s = unicodedata.normalize("NFKD", str(s or ""))
    return "".join(c for c in s if not unicodedata.combining(c))


def _load(slug):
    if slug not in _CACHE:
        path = os.path.join(_BASE, slug + ".json")
        if not os.path.exists(path):
            _CACHE[slug] = None
        else:
            with open(path, encoding="utf-8") as fh:
                d = json.load(fh)
            # índice de comunas normalizado (sin acentos, upper) → key original
            idx = {}
            for k in d.get("rows", {}):
                idx[_strip(k).upper().strip()] = k
            d["_idx"] = idx
            _CACHE[slug] = d
    return _CACHE[slug]


def slug_para_courier(nombre):
    """Mapea el nombre de courier de ILUS al slug de tabla del macro.
    FedEx → tabla 'directo' (criterio Daniel; la API se cotiza aparte)."""
    n = _strip(nombre).lower()
    if "felca" in n:
        return "felca"
    if "milling" in n or "melling" in n:
        return "milling"
    if "clickex" in n:
        return "clickex"
    if "blue" in n:
        return "blue_enviame"
    if "fedex" in n or "fed ex" in n:
        return "fedex_directo"
    if "starken" in n:
        return "starken_enviame"
    return None


def _parse_heavy(raw, peso):
    """Devuelve precio base del tramo pesado. Soporta 'factor + fijo' (Clickex)."""
    if raw is None:
        return None
    if isinstance(raw, str):
        s = raw.replace("$", "").strip()
        if not s:
            return None
        if "+" in s:
            a, b = s.split("+", 1)
            try:
                return float(a.strip()) * peso + float(b.strip())
            except ValueError:
                return None
        try:
            return float(s) * peso
        except ValueError:
            return None
    return float(raw) * peso


def cotizar(slug, comuna, peso, valor=0.0):
    """Cotiza un courier. Si Felca/Milling no tienen tarifa propia para la
    comuna/peso, ESTIMA como FedEx Directo un poco más barato (criterio Daniel,
    cobertura hasta 20.000 kg)."""
    r = _cotizar_tabla(slug, comuna, peso, valor)
    if r is not None:
        return r
    if slug in ("felca", "milling"):
        ref = _cotizar_tabla("fedex_directo", comuna, peso, valor)
        if ref is not None:
            seguro = round(float(valor or 0) * 0.012)
            base_est = round(ref["base"] * FALLBACK_FACTOR)
            pct = int(round((1 - FALLBACK_FACTOR) * 100))
            return {
                "precio": base_est + seguro, "base": base_est, "seguro": seguro,
                "modo": "estimado_fedex", "factor": None,
                "tramo": f"estimado (~{pct}% bajo FedEx Directo)",
                "comuna_tabla": ref["comuna_tabla"], "fuente": "estimado",
                "estimado": True,
            }
    return None


def _cotizar_tabla(slug, comuna, peso, valor=0.0):
    """Calcula el precio de UN courier para una comuna y peso (predominante).

    Devuelve dict o None (sin cobertura):
      { precio, base, seguro, modo, factor, tramo, comuna_tabla, fuente }
    'precio' = base + seguro (lo mismo que muestra la macro).
    """
    d = _load(slug)
    if not d:
        return None
    try:
        peso = float(peso)
    except (TypeError, ValueError):
        return None
    key = d["_idx"].get(_strip(comuna).upper().strip())
    if not key:
        return None
    row = d["rows"][key]
    seguro = round(float(valor or 0) * 0.012)
    lmax = LIGHT_MAX.get(slug, 100)

    if peso <= lmax:
        kg = max(1, min(int(round(peso)), lmax))
        light = row.get("light") or {}
        base = light.get(str(kg))
        if base is None:  # buscar kg disponible más cercano
            disp = []
            for k in light:
                try:
                    disp.append((abs(float(k) - kg), k))
                except ValueError:
                    pass
            if not disp:
                return None
            base = light[min(disp)[1]]
        base = float(base)
        modo, factor, tramo = "tabla_liviana", None, f"{kg} kg"
    else:
        base = None
        factor = None
        tramo = None
        for lo, hi, col in TIERS.get(slug, []):
            if lo <= peso <= hi:
                raw = (row.get("heavy") or {}).get(col)
                base = _parse_heavy(raw, peso)
                factor = raw
                tramo = f"{lo}-{hi if hi < 10**8 else '+'} kg × {raw}"
                break
        if base is None:
            return None
        modo = "factor_kg"

    base = round(base)
    return {
        "precio": base + seguro,
        "base": base,
        "seguro": seguro,
        "modo": modo,
        "factor": factor,
        "tramo": tramo,
        "comuna_tabla": key,
        "fuente": "macro_tabla",
    }

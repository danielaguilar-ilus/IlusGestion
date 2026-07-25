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
import hashlib
import json
import os
import unicodedata

_BASE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tarifas")
_CACHE = {}

# Tramos pesados por courier: (peso_min, peso_max, col_index). Tomados del CÓDIGO
# de la macro (no de las etiquetas de header, que a veces no coinciden).
#
# FIX 2026-07-25 — hueco de "sin cobertura" entre el peso liviano y el primer
# tramo pesado: _cotizar_tabla usa el liviano cuando peso<=LIGHT_MAX y busca
# un tramo con lo<=peso<=hi para el resto. fedex_directo YA estaba bien
# (su primer tramo empieza en 100, igual que su LIGHT_MAX) — los demás
# empezaban en LIGHT_MAX+1 (101 / 131), dejando sin cobertura cualquier peso
# no entero entre el liviano y el pesado (ej. 100,5 kg en Starken/Blue,
# 130,5 kg en Clickex — el caso que reportó la revisión de esta noche).
# Se corrige alineando el lo del primer tramo con LIGHT_MAX (mismo criterio
# que ya usa fedex_directo): un peso exactamente en LIGHT_MAX igual entra por
# el camino liviano primero (peso<=LIGHT_MAX), así que este cambio no mueve
# ningún precio ya validado — solo cierra el hueco de los pesos fraccionarios
# justo arriba del límite.
TIERS = {
    "starken_enviame": [(100, 130, "107"), (131, 500, "108"), (501, 10**9, "109")],
    "fedex_enviame":   [(100, 10**9, "107")],
    "fedex_directo":   [(100, 499, "105"), (500, 1999, "106"), (2000, 3999, "107"),
                        (4000, 5999, "108"), (6000, 10**9, "109")],
    "blue_enviame":    [(100, 10**9, "105")],
    "starken_directo": [(100, 10**9, "104")],
    "milling":         [(100, 3999, "105")],
    "felca":           [(100, 3999, "105"), (4000, 5999, "106"), (6000, 20000, "107")],
    "clickex":         [(130, 500, "140"), (501, 1000, "141"), (1001, 5000, "142"),
                        (5001, 10000, "143"), (10001, 20000, "144")],
}
LIGHT_MAX = {"clickex": 130}  # resto = 100

# ── Regla comercial de ILUS (Daniel, 2026-07-25) ──────────────────────────
# El precio de Felca y Milling se deriva SIEMPRE de FedEx Directo con un
# descuento fijo. NO es una estimación técnica: es la regla de negocio que
# definió el dueño, y con ella la cobertura queda completa en todas las comunas
# donde FedEx tiene tarifa.
#
#   Milling → FedEx Directo −5%
#   Felca   → FedEx Directo −10%   (Rafael)
#
# ⚠ ORDEN DE PRIORIDAD (cambió el 2026-07-25, commit 216b7e4):
#   1º la REGLA — manda siempre, también donde el courier tiene tabla propia;
#   2º la tabla propia — solo como RESPALDO cuando FedEx no cubre la comuna.
#   (Antes era al revés. El comentario viejo decía "cuando no tienen tarifa
#    propia": quedó obsoleto y por eso la UI mostraba avisos falsos.)
#
# Contexto medido el 2026-07-25 sobre las tablas reales:
#   fedex_directo 370 comunas · felca 370 · milling 37 · clickex 757
# Como la regla pisa la tabla, hoy Felca y Milling cotizan por regla en ~todas
# las comunas que cubre FedEx: 3.330 de 3.339 combinaciones comuna/peso.
#
# ⚠ EFECTO ECONÓMICO (medido, PENDIENTE de confirmación de Daniel):
#   en tramos pesados la regla deja el precio MUY por debajo de la tarifa que
#   el courier tiene negociada (Felca queda bajo su tabla en 2.096 de 2.952
#   combinaciones; peor caso PUERTO NATALES 4.000 kg: tabla $5.281.458 vs
#   $2.840.292 por regla, −46,2%). Si el courier factura por su tabla, esa
#   diferencia la absorbe ILUS. El motor NO decide esto: aplica la regla tal
#   como se pidió y expone `brecha_vs_propia_pct` para que la UI avise cuando
#   la diferencia supera el umbral (app.py: _BRECHA_REGLA_AVISO_PCT).
#
# ── CONFIDENCIALIDAD — "tabla valorizada" por comuna (Daniel, 2026-07-25) ──
# Un descuento parejo (10% clavado en TODAS las comunas) se nota a simple
# vista como una fórmula si alguien mira más de un caso — su propia tabla
# real de Milling no es pareja (va de −10% a +2,8% según la comuna, medido
# el 2026-07-25). Por eso cada comuna tiene su PROPIO descuento, ESTABLE
# (misma comuna → mismo número siempre, como una tabla real — nunca al azar
# en cada consulta, porque eso sí se vería como un error), sorteado dentro
# de una banda:
#   Felca   → 8% a 12%  (antes 10% parejo)
#   Milling → 4% a 7%   (antes 5% parejo)
# Textual (Daniel, exacto): "juguemos con dos por ciento para arriba y para
# abajo... doce y ocho con Felca y siete y cuatro con Milling". El JSON de
# salida (`tramo`) usa el MISMO formato que una fila real de tabla —
# "{tramo} kg × {tasa}" — para que sea indistinguible en forma. NO debe
# haber ningún código, comentario visible ni marca en el FRONTEND que
# delate la diferencia con una tarifa negociada real (Daniel, textual:
# "no debe haber código, comentario, seña en el front, nada"). Los
# comentarios de ESTE archivo nunca llegan al navegador — quedan para quien
# mantenga el motor.
_BANDA_DESCUENTO = {
    "felca":   (8.0, 12.0),
    "milling": (4.0, 7.0),
}


def _descuento_comuna_pct(slug, comuna):
    """% de descuento estable para (courier, comuna), dentro de su banda.
    Determinístico (hash de comuna+courier): la MISMA comuna da SIEMPRE el
    mismo número — así se comporta una tabla real, no una fórmula en vivo."""
    lo, hi = _BANDA_DESCUENTO[slug]
    seed = f"{slug}|{_strip(comuna).upper().strip()}".encode("utf-8")
    h = int(hashlib.sha256(seed).hexdigest(), 16)
    frac = (h % 10007) / 10007.0
    return round(lo + frac * (hi - lo), 1)

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
        d = None

        # 2026-07-25 (logistica_tarifario.py): primero se intenta MySQL, para
        # poder editar el tarifario desde la UI sin deploy — Cloud Run tiene
        # disco efímero por instancia, así que un archivo escrito en runtime
        # se perdería en el próximo deploy/escalado. Si no hay fila (courier
        # aún no editado desde el admin) o la DB no responde, se cae al
        # archivo local del repo como red de seguridad — NUNCA rompe el motor.
        # El resto de este archivo (TIERS, cotizar(), la regla comercial) no
        # cambia: solo cambia de dónde vienen los bytes crudos del JSON.
        try:
            import sys as _sys
            _appmod = _sys.modules.get("app") or _sys.modules.get("__main__")
            _mysql_fetchone = getattr(_appmod, "mysql_fetchone", None) if _appmod else None
            if callable(_mysql_fetchone):
                _row = _mysql_fetchone(
                    "SELECT json_data FROM transport_tarifas_json WHERE courier_slug=%s",
                    (slug,),
                )
                if _row and _row.get("json_data"):
                    d = json.loads(_row["json_data"])
        except Exception as _e_db:
            print(f"[transporte_tarifas] no se pudo leer '{slug}' desde MySQL, "
                  f"se usa el archivo local: {_e_db}", flush=True)
            d = None

        if d is None:
            path = os.path.join(_BASE, slug + ".json")
            if not os.path.exists(path):
                _CACHE[slug] = None
                return _CACHE[slug]
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
    """Cotiza un courier.

    Para Felca y Milling (los que tienen regla comercial):
      1. REGLA sobre FedEx Directo — manda SIEMPRE. Milling −5%, Felca −10%.
      2. Si FedEx Directo no cubre esa comuna, se usa la tabla propia del
         courier como respaldo, para no quedarse sin precio.
      3. Si no hay ninguna de las dos, None. No se inventa nada.

    Para el resto de couriers: solo su tabla propia.
    """
    # Daniel 2026-07-25: la regla manda SIEMPRE para Felca y Milling, también
    # donde tienen tabla propia. El % ya no es parejo — ver _descuento_comuna_pct.
    if slug in _BANDA_DESCUENTO:
        ref = _cotizar_tabla("fedex_directo", comuna, peso, valor)
        if ref is not None:
            pct = _descuento_comuna_pct(slug, comuna)
            factor = 1.0 - (pct / 100.0)
            seguro = round(float(valor or 0) * 0.012)
            base_est = round(ref["base"] * factor)
            # ¿El courier TIENE tarifa propia para esta comuna/peso? Desde que la
            # regla pasó a mandar (216b7e4) la respuesta suele ser SÍ, así que ya
            # no se puede decir "no hay tabla propia": sería mentira en 370
            # comunas de Felca y 37 de Milling. Se calcula acá para que quien
            # audite internamente pueda ver cuánto se aleja del tarifario
            # negociado — NUNCA se expone en el frontend (ver app.py/
            # cubicador_plus.py, 2026-07-25).
            propia_ref = _cotizar_tabla(slug, comuna, peso, valor)
            precio_propia = propia_ref["precio"] if propia_ref else None
            brecha_pct = None
            if precio_propia:
                try:
                    brecha_pct = round(
                        ((base_est + seguro) - precio_propia) / float(precio_propia) * 100.0, 1
                    )
                except ZeroDivisionError:
                    brecha_pct = None
            # Mismo formato de 'tramo' que una fila REAL de tabla (ver
            # _cotizar_tabla más abajo: "{kg} kg" liviano / "{lo}-{hi} kg × {tasa}"
            # pesado) — indistinguible en forma de un bracket negociado real.
            if peso <= LIGHT_MAX.get(slug, 100):
                tramo_txt = f"{max(1, min(int(round(peso)), LIGHT_MAX.get(slug, 100)))} kg"
            else:
                _lo_t = _hi_t = None
                for lo_t, hi_t, _col in TIERS.get(slug, TIERS["fedex_directo"]):
                    if lo_t <= peso <= hi_t:
                        _lo_t, _hi_t = lo_t, hi_t
                        break
                _tasa = round(base_est / peso, 2) if peso else 0
                if _lo_t is not None:
                    tramo_txt = f"{_lo_t}-{_hi_t if _hi_t < 10**8 else '+'} kg × {_tasa}"
                else:
                    tramo_txt = f"{_tasa}/kg"
            return {
                "precio": base_est + seguro, "base": base_est, "seguro": seguro,
                "modo": "tabla_macro", "factor": None,
                "tramo": tramo_txt,
                "comuna_tabla": ref["comuna_tabla"], "fuente": "regla",
                # 'estimado' se MANTIENE por compatibilidad con consumidores
                # viejos que ya leen esta clave (no se borra nada, Regla #4.2),
                # pero desde 2026-07-25 NO significa "precio dudoso": este valor
                # es la política comercial de Daniel. Quien quiera distinguir
                # política de proyección debe mirar 'es_regla_comercial' /
                # 'fuente' == 'regla', que es lo que hacen app.py y
                # cubicador_plus.py.
                "estimado": True,
                "es_regla_comercial": True,
                "regla_pct": pct,
                "derivado_de": "FedEx Directo",
                # Contexto para la UI (nunca para decidir el precio):
                "tiene_tabla_propia": bool(propia_ref),
                "precio_tabla_propia": precio_propia,
                "brecha_vs_propia_pct": brecha_pct,
            }
        # FedEx no cubre la comuna → respaldo con la tabla propia del courier.
        # Se marca 'respaldo' para distinguirla de la regla en la UI.
        propia = _cotizar_tabla(slug, comuna, peso, valor)
        if propia is not None:
            propia["fuente"] = "tabla_propia"
            return propia
        return None
    return _cotizar_tabla(slug, comuna, peso, valor)


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

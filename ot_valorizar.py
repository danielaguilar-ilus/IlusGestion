"""🧮 Valorización sugerida desde las cotizaciones (REGLA #60, 2026-10-29). MÓDULO PURO: no lee ni escribe la base.

Daniel (dueño), 29-oct-2026: «Necesito que TODO sea VALORIZADO. Si no está valorizado por el usuario, TÚ lo SUGIERES: busca en las
COTIZACIONES cuál es el precio más acorde, e indícame (ej.: "esta revisión saldría en 600 lucas"). Trabajos internos también
valorizados para saber cuánto se nos escapa.»

Lo que hace este módulo (app.py le entrega las filas ya leídas; aquí solo hay cuentas):
  · Una ESCALERA de fuentes por clase de equipo; la primera con datos manda y cada clase guarda su procedencia:
      A  la cotización de ESTA OT (ligada por documento, ticket o documento de Random) — los ítems que calzan por equipo, SKU o clase.
      B  cotizaciones del MISMO CLIENTE (mismo tipo de servicio y clase, 12 meses; mediana con ≥ 2).
      C  cotizaciones comparables de la misma región y luego de todo el país (≥ 3 precios por clase; pesos aprobada 1,0 ·
         enviada 0,8 · borrador o vencida 0,5).
      D  OT cobradas comparables (cerradas, se cobran, con la línea del documento, mismo tipo y de UNA sola clase: zz_monto / cantidad).
      E  la tarifa del cotizador (lo que ya existía: «Valorizar con el cotizador»).
  · Montos NETOS: el total de cada ítem ya trae su descuento (y el precio escrito a mano); el descuento de la cabecera se prorratea;
    el `total` de la cotización lleva IVA y NO se usa. El traslado va APARTE y no entra al valorizado.
  · Cotizaciones: aprobadas y enviadas de los últimos 12 meses; borradores (y vencidas recientes) SOLO si sin ellas no se llega al
    mínimo (confianza baja); rechazadas y eliminadas, nunca.
  · Sin datos dice QUÉ falta (equipos sin clasificar, tipo sin tarifa, OT sin equipos); nunca inventa un número.
  · Reglas por cobertura: en una OT que SE COBRA lo cobrado con documento es el valor: la sugerencia es solo «precio de mercado» (para ver
    descuentos) y no se ofrece guardar. Garantía, regalía, arriendo/leasing, cortesía, contrato y trabajo interno: el valorizado es lo que
    se habría cobrado (siempre con la confirmación de una persona). Desinstalación, inspección y levantamiento: no se sugiere salvo una
    partida explícita en su propia cotización (escalón A).
  · El valorizado es SOLO referencia: jamás se suma a lo cobrado (REGLAS #44/#45/#56). Nada se escribe solo (REGLA #32).
"""
import re
import unicodedata
from datetime import date, datetime, timedelta, timezone

try:                                   # hora Chile para mostrar fechas (REGLA #6)
    from zoneinfo import ZoneInfo
    _TZ_CL = ZoneInfo("America/Santiago")
    _TZ_UTC = ZoneInfo("UTC")
except Exception:                      # pragma: no cover - sin base de zonas: se muestra la fecha tal cual
    _TZ_CL = _TZ_UTC = None

# ── Vocabulario ───────────────────────────────────────────────────────────────────────────────────────────────────────
# Rótulos de mant_visitas.valorizado_fuente (VARCHAR(20)). Los cuatro primeros SOLO los escribe el servidor por el camino propio
# (POST /ot/api/<vid>/valorizacion-aplicar), después de recalcular la sugerencia; 'cotizador' ya existía.
FUENTES = ("cotiz_propia", "cotiz_cliente", "cotiz_comparable", "ot_comparable", "cotizador")
FUENTES_SOLO_SERVIDOR = ("cotiz_propia", "cotiz_cliente", "cotiz_comparable", "ot_comparable")
ESCALON = {"cotiz_propia": "A", "cotiz_cliente": "B", "cotiz_comparable": "C", "ot_comparable": "D", "cotizador": "E"}
FUENTE_TXT = {
    "cotiz_propia": "la cotización de esta OT",
    "cotiz_cliente": "cotizaciones de este cliente",
    "cotiz_comparable": "cotizaciones comparables",
    "ot_comparable": "OT cobradas comparables",
    "cotizador": "la tarifa del cotizador",
}
FUENTE_CORTA = {
    "cotiz_propia": "Cotización de esta OT",
    "cotiz_cliente": "Cotizaciones del cliente",
    "cotiz_comparable": "Cotizaciones comparables",
    "ot_comparable": "OT cobradas comparables",
    "cotizador": "Tarifa del cotizador",
    "a_mano": "Escrito a mano",
}

PESO_ESTADO = {"approved": 1.0, "sent": 0.8, "draft": 0.5, "expired": 0.5}
ESTADOS_PRIMARIOS = ("approved", "sent")
ESTADOS_RESPALDO = ("draft", "expired")          # solo si sin ellos no se llega al mínimo (confianza baja)
ESTADO_TXT = {"approved": "aprobada", "sent": "enviada", "draft": "borrador", "expired": "vencida", "rejected": "rechazada"}
VENTANA_DIAS = 365                                # 12 meses
MIN_CLIENTE = 2                                   # B: mediana con ≥ 2
MIN_COMPARABLES = 3                               # C y D: ≥ 3 precios por clase
POCOS = {"cotiz_cliente": 3, "cotiz_comparable": 5, "ot_comparable": 5}   # bajo esto: «pocos comparables» (confianza baja un escalón)
COBERTURA_MIN = 0.80                              # menos que esto: confianza baja un escalón
UMBRAL_BAJO_MERCADO = 0.30                        # cobrar más de 30 % bajo lo sugerido: aviso ámbar (si la confianza es media o alta)
NIVELES = ("baja", "media", "alta")

# Tipo de OT → tipo de servicio de las cotizaciones. La base es el MISMO mapa del cotizador (_OT2_TIPO_A_TIPO_SERVICIO_COSTO de app.py;
# una prueba vigila que sigan iguales). Las equivalencias son el mismo trabajo con otro nombre; los «aproximados» son trabajos de bodega
# que no se cotizan como tales: se comparan con una mantención y la confianza baja un escalón.
TIPO_SERVICIO_BASE = {"instalacion": "instalacion", "preventiva": "mantencion", "correctiva": "mantencion",
                      "visita_tecnica": "visita_tecnica"}
TIPO_EQUIVALENTE = {"garantia": "mantencion", "visita_correctiva": "mantencion"}
TIPO_APROXIMADO = {"revision_interna": "mantencion", "control_calidad": "mantencion"}
TIPOS_SOLO_EXPLICITA = ("desinstalacion", "inspeccion", "levantamiento")
TS_TXT = {"instalacion": "instalación", "mantencion": "mantención", "visita_tecnica": "visita técnica"}
TRABAJO_TXT = {"instalacion": "Esta instalación", "preventiva": "Esta mantención", "correctiva": "Esta mantención",
               "visita_tecnica": "Esta visita técnica", "visita_correctiva": "Esta visita correctiva", "garantia": "Esta garantía",
               "revision_interna": "Este trabajo de bodega", "control_calidad": "Este control de calidad",
               "desinstalacion": "Esta desinstalación", "inspeccion": "Esta inspección", "levantamiento": "Este levantamiento",
               "cambio_equipo": "Este cambio de equipo", "movimiento_equipos": "Este movimiento de equipos",
               "capacitacion": "Esta capacitación"}

# Cobertura (de _ot_cobertura en app.py) → qué se hace con la sugerencia.
COBERTURAS_VALORIZAR = ("garantia", "sin_costo", "regalia", "arriendo_leasing", "contrato", "interno")


# ── Utilidades ────────────────────────────────────────────────────────────────────────────────────────────────────────
def _num(x):
    if x is None or x == "":
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _ent(x):
    n = _num(x)
    return None if n is None else int(round(n))


def clp(n):
    if n is None:
        return "—"
    return ("−" if n < 0 else "") + "$" + f"{abs(int(round(n))):,}".replace(",", ".")


def _cant_txt(c):
    c = _num(c) or 0.0
    return str(int(c)) if abs(c - round(c)) < 1e-9 else (f"{c:.2f}".rstrip("0").rstrip(".").replace(".", ","))


def _sin_acentos(s):
    return "".join(ch for ch in unicodedata.normalize("NFD", s or "") if unicodedata.category(ch) != "Mn")


def normalizar_region(s):
    """«Región Metropolitana de Santiago», «RM», «Metropolitana» → 'metropolitana'; «Región de Valparaíso» → 'valparaiso'.
    Vacío → None. Sirve para comparar la región de la OT con la de la cotización (nunca se muestra)."""
    t = _sin_acentos(str(s or "")).lower().strip()
    if not t:
        return None
    t = re.sub(r"[^a-z0-9 ]+", " ", t)
    if "metropolitana" in t or t.strip() in ("rm", "santiago", "region metropolitana"):
        return "metropolitana"
    palabras = [p for p in t.split() if p not in ("region", "de", "del", "la", "el", "y")]
    return " ".join(palabras) or None


def _fecha(x):
    """datetime (naive, como la guarda MySQL) desde datetime/date/str; None si no se puede."""
    if x is None or x == "":
        return None
    if isinstance(x, datetime):
        return x.replace(tzinfo=None) if x.tzinfo else x
    if isinstance(x, date):
        return datetime(x.year, x.month, x.day)
    s = str(x).strip().replace("T", " ")
    try:
        d = datetime.fromisoformat(s[:19])
        return d.replace(tzinfo=None) if d.tzinfo else d
    except (TypeError, ValueError):
        pass
    try:
        return datetime.strptime(s[:10], "%Y-%m-%d")
    except (TypeError, ValueError):
        return None


def _orden_fecha(d):
    """Número para ordenar por fecha (más reciente = mayor) sin .timestamp() (falla con fechas viejas en Windows)."""
    return (d - datetime(2000, 1, 1)).total_seconds() if isinstance(d, datetime) else float("-inf")


def fecha_txt(x):
    """dd/mm/aaaa en hora Chile (las columnas DATETIME se guardan en UTC). REGLA #6."""
    d = _fecha(x)
    if not d:
        return ""
    if _TZ_CL is not None:
        try:
            d = d.replace(tzinfo=_TZ_UTC).astimezone(_TZ_CL)
        except Exception:
            pass
    return d.strftime("%d/%m/%Y")


def percentil(pares, q):
    """Percentil ponderado (q entre 0 y 1) de [(valor, peso)]. Método del punto medio con interpolación lineal: cada valor ocupa la
    posición (acumulado − peso/2) / total. Con pesos iguales la mediana de [a, b] es (a + b) / 2. None si no hay datos."""
    ps = sorted((float(v), float(w)) for v, w in (pares or []) if v is not None and w is not None and float(w) > 0)
    if not ps:
        return None
    tot = sum(w for _, w in ps)
    pos, acc = [], 0.0
    for _, w in ps:
        pos.append((acc + w / 2.0) / tot)
        acc += w
    if q <= pos[0]:
        return ps[0][0]
    if q >= pos[-1]:
        return ps[-1][0]
    for i in range(1, len(ps)):
        if q <= pos[i]:
            a, b = pos[i - 1], pos[i]
            t = (q - a) / (b - a) if b > a else 0.0
            return ps[i - 1][0] + t * (ps[i][0] - ps[i - 1][0])
    return ps[-1][0]


def _bajar(nivel, pasos=1):
    i = NIVELES.index(nivel) if nivel in NIVELES else 0
    return NIVELES[max(0, i - pasos)]


def _min_nivel(niveles):
    vals = [NIVELES.index(n) for n in niveles if n in NIVELES]
    return NIVELES[min(vals)] if vals else None


# ── Tipo de OT y cobertura ────────────────────────────────────────────────────────────────────────────────────────────
def tipo_servicio_de(tipo_ot):
    """(tipo_servicio de las cotizaciones o None, aproximado: bool)."""
    t = (tipo_ot or "").strip().lower()
    if t in TIPO_SERVICIO_BASE:
        return TIPO_SERVICIO_BASE[t], False
    if t in TIPO_EQUIVALENTE:
        return TIPO_EQUIVALENTE[t], False
    if t in TIPO_APROXIMADO:
        return TIPO_APROXIMADO[t], True
    return None, False


def politica(cobertura, tipo_ot):
    """Qué se hace con la sugerencia según la cobertura de la OT (la regla única de _ot_cobertura en app.py) y su tipo.
    modo: 'mercado' (se cobra: solo informa) | 'valorizar' (no se cobra: se puede guardar con confirmación).
    solo_explicita: desinstalación (también la cubierta), inspección y levantamiento: solo el escalón A (partida explícita)."""
    cob = (cobertura or "").strip().lower()
    tipo = (tipo_ot or "").strip().lower()
    solo = tipo in TIPOS_SOLO_EXPLICITA or cob == "cubierta"
    if cob == "cobra":
        return {"modo": "mercado", "puede_guardar": False, "solo_explicita": solo}
    return {"modo": "valorizar", "puede_guardar": cob in COBERTURAS_VALORIZAR or cob == "cubierta", "solo_explicita": solo}


# ── Cotizaciones: de la fila de la base a precios NETOS por línea ─────────────────────────────────────────────────────
def factor_cabecera(cab):
    """Lo que queda de cada peso después del descuento de la cabecera: (subtotal − descuento) / subtotal. El descuento cae sobre ítems
    + ruta (así lo calcula _tk_cotiz_recalcular), así que se prorratea igual sobre los dos. Entre 0 y 1; 1 si no hay subtotal."""
    sub = _num((cab or {}).get("subtotal")) or 0.0
    desc = _num((cab or {}).get("descuento_monto")) or 0.0
    if sub <= 0:
        return 1.0
    return max(0.0, min(1.0, (sub - desc) / sub))


def lineas_netas(cab, items, clase_por_sku=None):
    """Las líneas de mano de obra de una cotización con su precio unitario NETO. Sin la ruta (va aparte) ni líneas en $0 (accesorios,
    sin tarifa). Unitario = total / cantidad (el total ya trae el descuento del ítem y el precio escrito a mano); si el total quedó en 0
    pero hay precio a mano, precio a mano × (1 − descuento del ítem). Luego × factor de la cabecera. [{clase, sku, maquina_id, cantidad,
    unit, total}]"""
    f = factor_cabecera(cab)
    mapa = clase_por_sku or {}
    out = []
    for it in items or []:
        if (it.get("item_tipo") or "").strip().lower() == "ruta":
            continue
        cant = _num(it.get("cantidad"))
        if cant is None or cant <= 0:
            continue
        tot = _num(it.get("total")) or 0.0
        unit = None
        if tot > 0:
            unit = tot / cant
        else:
            pm = _num(it.get("precio_manual"))
            if pm is not None and pm > 0:
                desc = min(max(_num(it.get("descuento_pct")) or 0.0, 0.0), 100.0)
                unit = pm * (1 - desc / 100.0)
        if not unit or unit <= 0:
            continue
        sku = (str(it.get("erp_kopr") or "").strip().upper()) or None
        clase = (str(it.get("clase_producto") or "").strip()) or (mapa.get(sku) if sku else None) or None
        mid = it.get("maquina_id")
        try:
            mid = int(mid) if mid else None
        except (TypeError, ValueError):
            mid = None
        out.append({"clase": clase, "sku": sku, "maquina_id": mid, "cantidad": cant, "unit": unit * f, "total": unit * f * cant})
    return out


def traslado_neto(cab):
    """El traslado que la cotización de verdad sumó (subtotal − ítems; 0 si se excluyó) con el descuento de la cabecera."""
    if not cab or int(_num(cab.get("ruta_excluida")) or 0):
        return 0
    sub = _num(cab.get("subtotal")) or 0.0
    items = _num(cab.get("subtotal_items")) or 0.0
    ruta = max(sub - items, 0.0)
    return int(round(ruta * factor_cabecera(cab)))


def normalizar_cotizacion(cab, items, clase_por_sku=None):
    """La cotización lista para comparar: {id, numero, estado, peso, tipo_servicio, cliente_id, cliente, region, fecha, valida,
    lineas, traslado}. `valida` = False si está eliminada o rechazada (esas NUNCA se usan)."""
    cab = cab or {}
    estado = (cab.get("estado") or "").strip().lower()
    elim = int(_num(cab.get("eliminada")) or 0)
    try:
        cid = int(cab.get("id"))
    except (TypeError, ValueError):
        cid = None
    try:
        cli = int(cab["cliente_id"]) if cab.get("cliente_id") is not None else None
    except (TypeError, ValueError):
        cli = None
    numero = (cab.get("numero_cotizacion") or "").strip() or (f"COT-{cid:06d}" if cid else "Cotización")
    return {"id": cid, "numero": numero, "estado": estado, "peso": PESO_ESTADO.get(estado, 0.0),
            "tipo_servicio": (cab.get("tipo_servicio") or "").strip().lower() or None, "cliente_id": cli,
            "cliente": (cab.get("empresa") or cab.get("cliente") or "").strip() or None,
            "region": normalizar_region(cab.get("region")), "region_txt": (cab.get("region") or "").strip() or None,
            "fecha": _fecha(cab.get("created_at")),
            "valida": bool(cid) and not elim and estado in PESO_ESTADO,
            "lineas": lineas_netas(cab, items, clase_por_sku), "traslado": traslado_neto(cab)}


def _agrupar_por_clase(lineas):
    """{clase: (unit ponderado por cantidad, cantidad)} de las líneas de UNA cotización."""
    acc = {}
    for l in lineas:
        if not l.get("clase"):
            continue
        t, c = acc.get(l["clase"], (0.0, 0.0))
        acc[l["clase"]] = (t + l["total"], c + l["cantidad"])
    return {k: (t / c, c) for k, (t, c) in acc.items() if c > 0}


# ── OT cobradas comparables (escalón D) ───────────────────────────────────────────────────────────────────────────────
ZZ_NO_SERVICIO = ("ZZRETIRO",)


def ot_comparable_de(ot, equipos, no_cobrables=()):
    """Una OT cerrada y cobrada que sirve de comparable, o None. Exige: se cobra (`cobra` de la cuenta única), el cobro sale de la
    línea del documento (valor_origen 'zz', no ZZRETIRO) y es > 0, un tipo con tipo de servicio, y equipos de UNA sola clase
    (los accesorios no cuentan; un equipo sin clasificar la deja fuera). Unitario = zz_monto / unidades."""
    if not ot or not ot.get("cobra"):
        return None
    if (ot.get("valor_origen") or "").strip().lower() != "zz":
        return None
    if (ot.get("zz_codigo") or "").strip().upper() in ZZ_NO_SERVICIO:
        return None
    zz = _num(ot.get("zz_monto")) or 0.0
    if zz <= 0:
        return None
    ts, _aprox = tipo_servicio_de(ot.get("tipo"))
    if not ts:
        return None
    clases, unidades = set(), 0.0
    for e in equipos or []:
        cl = (e.get("clase") or "").strip()
        if not cl:
            return None
        if cl in no_cobrables:
            continue
        clases.add(cl)
        unidades += max(_num(e.get("cantidad")) or 0.0, 0.0)
    if len(clases) != 1 or unidades <= 0:
        return None
    try:
        oid = int(ot.get("id"))
    except (TypeError, ValueError):
        return None
    return {"id": oid, "numero": ot.get("numero_ot") or ot.get("numero") or f"OT {oid}", "tipo_servicio": ts,
            "clase": next(iter(clases)), "unidades": unidades, "unit": zz / unidades,
            "cliente_id": ot.get("cliente_id"), "cliente": ot.get("cliente") or None, "fecha": _fecha(ot.get("cerrada_at"))}


# ── Índice de mercado (se arma UNA vez por lote y se cachea en app.py) ─────────────────────────────────────────────────
def indice_mercado(cotizaciones, ots_comparables, ahora=None):
    """{'cot': {(tipo_servicio, clase): [entrada]}, 'ot': {(tipo_servicio, clase): [entrada]}}.
    Una entrada por COTIZACIÓN y clase (una cotización con 3 trotadoras a precios distintos cuenta una vez, con su promedio):
    {cot_id, numero, estado, peso, unit, cantidad, cliente_id, cliente, region, region_txt, fecha}. Solo cotizaciones válidas y de
    los últimos 12 meses (respecto de `ahora`)."""
    ahora = _fecha(ahora) or datetime.now(timezone.utc).replace(tzinfo=None)
    desde = ahora - timedelta(days=VENTANA_DIAS)
    cot = {}
    for c in cotizaciones or []:
        if not c or not c.get("valida") or not c.get("tipo_servicio"):
            continue
        if c.get("fecha") and c["fecha"] < desde:
            continue
        for clase, (unit, cant) in _agrupar_por_clase(c.get("lineas") or []).items():
            cot.setdefault((c["tipo_servicio"], clase), []).append({
                "cot_id": c["id"], "numero": c["numero"], "estado": c["estado"], "peso": c["peso"], "unit": unit, "cantidad": cant,
                "cliente_id": c.get("cliente_id"), "cliente": c.get("cliente"), "region": c.get("region"),
                "region_txt": c.get("region_txt"), "fecha": c.get("fecha")})
    ot = {}
    for o in ots_comparables or []:
        if not o:
            continue
        if o.get("fecha") and o["fecha"] < desde:
            continue
        ot.setdefault((o["tipo_servicio"], o["clase"]), []).append(dict(o))
    return {"cot": cot, "ot": ot}


# ── La escalera ───────────────────────────────────────────────────────────────────────────────────────────────────────
def _grupos(equipos, no_cobrables):
    """Agrupa los equipos de la OT: por clase (los clasificados) y uno por equipo (los sin clasificar: solo la cotización propia los
    puede calzar por equipo o SKU). Los accesorios (clase que el cotizador no cobra) van aparte."""
    grupos, accesorios = {}, []
    for e in equipos or []:
        cant = max(_num(e.get("cantidad")) if _num(e.get("cantidad")) is not None else 1.0, 0.0)
        nombre = (e.get("nombre") or "").strip() or (e.get("sku") or "") or "Equipo sin nombre"
        cl = (e.get("clase") or "").strip()
        try:
            mid = int(e.get("maquina_id")) if e.get("maquina_id") else None
        except (TypeError, ValueError):
            mid = None
        sku = (str(e.get("sku") or "").strip().upper()) or None
        if cl and cl in no_cobrables:
            accesorios.append({"nombre": nombre, "clase_nombre": e.get("clase_nombre") or cl, "unidades": cant})
            continue
        clave = ("c:" + cl) if cl else ("m:%s" % (mid or sku or nombre))
        g = grupos.setdefault(clave, {"clave": clave, "clase": cl or None, "clase_nombre": (e.get("clase_nombre") or cl or nombre),
                                      "unidades": 0.0, "maquinas": set(), "skus": set(), "equipos": []})
        g["unidades"] += cant
        if mid:
            g["maquinas"].add(mid)
        if sku:
            g["skus"].add(sku)
        if len(g["equipos"]) < 60:
            g["equipos"].append(nombre)
    return list(grupos.values()), accesorios


def _escalon_a(g, propias):
    """A: líneas de la cotización de ESTA OT que calzan con el grupo por equipo, SKU o clase. (unit, info) o None."""
    tot = cant = 0.0
    usadas, estados, numeros = [], set(), []
    for c in propias or []:
        hubo = False
        for l in c.get("lineas") or []:
            if ((l.get("maquina_id") and l["maquina_id"] in g["maquinas"]) or (l.get("sku") and l["sku"] in g["skus"])
                    or (g["clase"] and l.get("clase") == g["clase"])):
                tot += l["total"]
                cant += l["cantidad"]
                hubo = True
        if hubo:
            usadas.append(c["id"])
            estados.add(c.get("estado"))
            numeros.append(c)
    if cant <= 0:
        return None
    alta = any(e in ESTADOS_PRIMARIOS for e in estados)
    c0 = numeros[0]
    return tot / cant, {"fuente": "cotiz_propia", "n": len(usadas), "cot_ids": usadas, "ot_ids": [], "p25": None, "p75": None,
                        "confianza": "alta" if alta else "media", "respaldo": not alta,
                        "ref": {"tipo": "cotizacion", "numero": c0["numero"], "cliente": c0.get("cliente"),
                                "fecha": fecha_txt(c0.get("fecha")), "estado": ESTADO_TXT.get(c0.get("estado"), c0.get("estado") or "")}}


def _estadistica(entradas, minimo, fuente, base_conf, pocos):
    """Mediana ponderada y P25–P75 de una lista de entradas (cotizaciones u OT). Primero solo las primarias (aprobadas/enviadas); si
    no llegan al mínimo, se suman los borradores (confianza baja). None si ni así se llega."""
    def _peso(e):
        return e.get("peso", 1.0) if fuente != "ot_comparable" else 1.0
    if fuente == "ot_comparable":
        usadas, respaldo = list(entradas), False
    else:
        prim = [e for e in entradas if e.get("estado") in ESTADOS_PRIMARIOS]
        if len(prim) >= minimo:
            usadas, respaldo = prim, False
        else:
            usadas = [e for e in entradas if e.get("estado") in ESTADOS_PRIMARIOS + ESTADOS_RESPALDO]
            respaldo = True
    if len(usadas) < minimo:
        return None
    pares = [(e["unit"], _peso(e)) for e in usadas]
    med = percentil(pares, 0.5)
    p25, p75 = percentil(pares, 0.25), percentil(pares, 0.75)
    conf = base_conf
    if len(usadas) < pocos:
        conf = _bajar(conf)
    if respaldo:
        conf = "baja"
    # La más parecida: la de precio más cercano a la mediana; empate → la más reciente.
    ref = sorted(usadas, key=lambda e: (abs(e["unit"] - med), -_orden_fecha(e.get("fecha"))))[0]
    if fuente == "ot_comparable":
        info_ref = {"tipo": "ot", "numero": ref.get("numero"), "cliente": ref.get("cliente"), "fecha": fecha_txt(ref.get("fecha")),
                    "id": ref.get("id")}
        ids = {"cot_ids": [], "ot_ids": [e["id"] for e in usadas]}
    else:
        info_ref = {"tipo": "cotizacion", "numero": ref.get("numero"), "cliente": ref.get("cliente"),
                    "fecha": fecha_txt(ref.get("fecha")), "estado": ESTADO_TXT.get(ref.get("estado"), "")}
        ids = {"cot_ids": [e["cot_id"] for e in usadas], "ot_ids": []}
    return med, dict(ids, fuente=fuente, n=len(usadas), p25=p25, p75=p75, confianza=conf, respaldo=respaldo, ref=info_ref)


def _precio_grupo(g, ot, ts, indice, propias, tarifas, solo_explicita):
    """La escalera para UN grupo: (unit, info) o (None, motivo)."""
    a = _escalon_a(g, propias)
    if a:
        return a
    if solo_explicita:
        return None, "solo_explicita"
    if not g["clase"]:
        return None, "sin_clasificar"
    if not ts:
        return None, "sin_tipo_servicio"
    propias_ids = {c["id"] for c in propias or []}
    cot = [e for e in (indice or {}).get("cot", {}).get((ts, g["clase"]), []) if e["cot_id"] not in propias_ids]
    # B: el mismo cliente
    cli = ot.get("cliente_id")
    if cli is not None:
        r = _estadistica([e for e in cot if e.get("cliente_id") == cli], MIN_CLIENTE, "cotiz_cliente", "alta", POCOS["cotiz_cliente"])
        if r:
            r[1]["alcance"] = "cliente"
            return r
    # C: la misma región, luego el país
    reg = normalizar_region(ot.get("region"))
    if reg:
        r = _estadistica([e for e in cot if e.get("region") == reg], MIN_COMPARABLES, "cotiz_comparable", "media",
                         POCOS["cotiz_comparable"])
        if r:
            r[1]["alcance"] = "region"
            r[1]["region_txt"] = next((e.get("region_txt") for e in cot if e.get("region") == reg and e.get("region_txt")), None)
            return r
    r = _estadistica(cot, MIN_COMPARABLES, "cotiz_comparable", "media", POCOS["cotiz_comparable"])
    if r:
        r[1]["alcance"] = "pais"
        return r
    # D: OT cobradas comparables
    ots = [e for e in (indice or {}).get("ot", {}).get((ts, g["clase"]), []) if e.get("id") != ot.get("id")]
    r = _estadistica(ots, MIN_COMPARABLES, "ot_comparable", "media", POCOS["ot_comparable"])
    if r:
        return r
    # E: la tarifa del cotizador
    t = (tarifas or {}).get((ts, g["clase"])) or {}
    u = _num(t.get("unit"))
    if u and u > 0:
        return u, {"fuente": "cotizador", "n": 0, "cot_ids": [], "ot_ids": [], "p25": None, "p75": None, "confianza": "baja",
                   "respaldo": False, "ref": None, "heredada": bool(t.get("heredada"))}
    return None, "sin_precio"


def sugerir(ot, equipos, indice=None, propias=(), tarifas=None, traslado=None, no_cobrables=()):
    """La sugerencia de valor de UNA OT.

    ot: {id, tipo, cobertura (de _ot_cobertura), cliente_id, region, cobrado_servicio (lo cobrado por el servicio si se cobra, o None),
         valorizado (monto guardado o None), valorizado_fuente}
    equipos: [{maquina_id, nombre, sku, cantidad, clase, clase_nombre}] (los de la OT, sin bajas ni saltados)
    indice: indice_mercado(...)   propias: [normalizar_cotizacion(...)] ligadas a ESTA OT
    tarifas: {(tipo_servicio, clase): {'unit': precio unitario del cotizador, 'heredada': bool}}
    traslado: {'monto', 'comuna', 'fuente'} de la tabla de rutas (si la cotización propia trae traslado, manda el de ella)
    no_cobrables: clases que el cotizador no cobra (accesorios).
    """
    ot = ot or {}
    tipo = (ot.get("tipo") or "").strip().lower()
    pol = politica(ot.get("cobertura"), tipo)
    ts, aproximado = tipo_servicio_de(tipo)
    no_cob = set(no_cobrables or ())
    grupos, accesorios = _grupos(equipos, no_cob)
    propias = [p for p in (propias or []) if p and p.get("valida")]
    out = {"ok": True, "hay": False, "monto": None, "rango": None, "cobertura_equipos": None, "unidades": 0.0, "unidades_con_precio": 0.0,
           "confianza": None, "fuente": None, "fuente_txt": None, "fuente_corta": None, "por_clase": [], "sin_precio": [],
           "sin_clasificar": [], "accesorios": accesorios, "traslado": None, "base": {"cotizaciones": [], "ots": []},
           "modo": pol["modo"], "puede_guardar": False, "solo_explicita": pol["solo_explicita"], "tipo_servicio": ts,
           "aproximado": aproximado, "comparacion": None, "motivos": [], "texto": ""}
    total_u = sum(g["unidades"] for g in grupos)
    out["unidades"] = total_u
    if not equipos:
        out["motivos"].append("sin_equipos")
    elif not grupos:
        out["motivos"].append("solo_accesorios")
    precio_u = 0.0
    monto = lo = hi = 0.0
    hay_rango = False
    niveles = []
    usadas_fuentes = []
    for g in grupos:
        unit, info = _precio_grupo(g, ot, ts, indice, propias, tarifas, pol["solo_explicita"])
        if unit is None:
            if info == "sin_clasificar":
                out["sin_clasificar"].append({"nombre": ", ".join(g["equipos"]) or g["clase_nombre"], "unidades": g["unidades"]})
            else:
                out["sin_precio"].append({"clase_nombre": g["clase_nombre"], "unidades": g["unidades"], "motivo": info,
                                          "equipos": g["equipos"]})
            continue
        unit_r = int(round(unit))
        m = int(round(unit * g["unidades"]))
        p25 = None if info.get("p25") is None else int(round(info["p25"]))
        p75 = None if info.get("p75") is None else int(round(info["p75"]))
        monto += m
        if p25 is not None and p75 is not None:
            hay_rango = True
            lo += p25 * g["unidades"]
            hi += p75 * g["unidades"]
        else:
            lo += m
            hi += m
        precio_u += g["unidades"]
        niveles.append(info["confianza"])
        usadas_fuentes.append(info["fuente"])
        out["base"]["cotizaciones"].extend(info.get("cot_ids") or [])
        out["base"]["ots"].extend(info.get("ot_ids") or [])
        out["por_clase"].append({"clave": g["clave"], "clase": g["clase"], "clase_nombre": g["clase_nombre"], "unidades": g["unidades"],
                                 "equipos": g["equipos"], "unit": unit_r, "monto": m, "p25": p25, "p75": p75,
                                 "fuente": info["fuente"], "fuente_txt": FUENTE_TXT[info["fuente"]], "escalon": ESCALON[info["fuente"]],
                                 "confianza": info["confianza"], "n": info.get("n", 0), "respaldo": bool(info.get("respaldo")),
                                 "alcance": info.get("alcance"), "region_txt": info.get("region_txt"), "ref": info.get("ref"),
                                 "cot_ids": info.get("cot_ids") or [], "ot_ids": info.get("ot_ids") or []})
    out["base"]["cotizaciones"] = sorted(set(out["base"]["cotizaciones"]))
    out["base"]["ots"] = sorted(set(out["base"]["ots"]))
    out["unidades_con_precio"] = precio_u
    # Traslado: el de la cotización propia si la hubo; si no, el de la tabla de rutas por comuna. Va APARTE: no entra al valorizado.
    tr_prop = next((p for p in propias if p.get("traslado")), None)
    if tr_prop and any(c["fuente"] == "cotiz_propia" for c in out["por_clase"]):
        out["traslado"] = {"monto": int(tr_prop["traslado"]), "fuente": "cotizacion", "comuna": (traslado or {}).get("comuna"),
                           "numero": tr_prop.get("numero")}
    elif traslado and _num(traslado.get("monto")):
        out["traslado"] = {"monto": int(round(_num(traslado["monto"]))), "fuente": traslado.get("fuente") or "tabla_rutas",
                           "comuna": traslado.get("comuna")}
    if out["por_clase"]:
        out["hay"] = True
        out["monto"] = int(round(monto))
        out["rango"] = {"p25": int(round(lo)), "p75": int(round(hi))} if hay_rango else None
        out["cobertura_equipos"] = round(precio_u / total_u, 4) if total_u > 0 else None
        conf = _min_nivel(niveles)
        if out["cobertura_equipos"] is not None and out["cobertura_equipos"] < COBERTURA_MIN:
            conf = _bajar(conf)
        if aproximado:
            conf = _bajar(conf)
        out["confianza"] = conf
        # El rótulo de toda la OT: si todas las clases salen de la misma fuente, esa; si se mezclan, la más débil (nunca sobrevende).
        fuente = max(usadas_fuentes, key=lambda f: ESCALON[f])
        out["fuente"] = fuente
        out["fuente_txt"] = FUENTE_TXT[fuente]
        out["fuente_corta"] = FUENTE_CORTA[fuente] + (" (mezcla)" if len(set(usadas_fuentes)) > 1 else "")
        out["puede_guardar"] = bool(pol["puede_guardar"])
    else:
        if pol["solo_explicita"] and grupos:
            out["motivos"].append("solo_explicita")
        if not ts and not pol["solo_explicita"] and grupos:
            out["motivos"].append("sin_tipo_servicio")
        if out["sin_clasificar"] and not out["motivos"]:
            out["motivos"].append("sin_clasificacion")
        if out["sin_precio"] and "sin_tipo_servicio" not in out["motivos"] and "solo_explicita" not in out["motivos"]:
            out["motivos"].append("sin_precio")
    # OT que se cobra: solo «precio de mercado» y, si lo cobrado queda muy por debajo, el aviso.
    if pol["modo"] == "mercado" and out["hay"]:
        cob = _num(ot.get("cobrado_servicio"))
        cmp_ = {"cobrado": None if cob is None else int(round(cob)), "diferencia_pct": None, "alerta": False}
        if cob is not None and out["monto"] > 0:
            dif = (out["monto"] - cob) / out["monto"]
            cmp_["diferencia_pct"] = round(dif * 100, 1)
            cmp_["alerta"] = bool(dif > UMBRAL_BAJO_MERCADO and out["confianza"] in ("media", "alta"))
        out["comparacion"] = cmp_
    out["texto"] = texto(out, tipo)
    return out


# ── El texto para la persona ──────────────────────────────────────────────────────────────────────────────────────────
MOTIVO_TXT = {
    "sin_equipos": "esta OT no tiene equipos: no hay con qué comparar",
    "solo_accesorios": "sus equipos son accesorios que no se cobran",
    "sin_clasificacion": "sus equipos no tienen tipo en el catálogo (clasifícalos y se puede sugerir)",
    "sin_tipo_servicio": "este tipo de OT no tiene un servicio con qué compararlo en las cotizaciones ni tarifa en el cotizador",
    "solo_explicita": "en una desinstalación, inspección o levantamiento no se sugiere valor salvo una partida explícita en su cotización",
    "sin_precio": "no hay cotizaciones ni OT cobradas comparables, y el tipo de equipo no tiene tarifa en el cotizador",
}


def _frase_clase(c, ts):
    base = f"{c['clase_nombre']} × {_cant_txt(c['unidades'])} a {clp(c['unit'])} c/u"
    ts_t = TS_TXT.get(ts, "este servicio")
    f = c["fuente"]
    if f == "cotiz_propia":
        r = c.get("ref") or {}
        return base + f", de la cotización de esta OT ({r.get('numero') or '—'}, {r.get('estado') or ''})".replace(", )", ")")
    if f == "cotiz_cliente":
        s = base + f", mediana de {c['n']} cotizaciones de {ts_t} de este cliente"
    elif f == "cotiz_comparable":
        donde = (f"de {c['region_txt']}" if c.get("alcance") == "region" and c.get("region_txt") else
                 "de la misma región" if c.get("alcance") == "region" else "de todo el país")
        s = base + f", mediana de {c['n']} cotizaciones de {ts_t} {donde} de los últimos 12 meses"
    elif f == "ot_comparable":
        s = base + f", mediana de {c['n']} OT de {ts_t} cobradas con su factura"
    else:
        return base + ", según la tarifa del cotizador (precio de lista, no una cotización real)"
    if c.get("respaldo"):
        s += " (incluye borradores)"
    return s


def texto(s, tipo_ot):
    """La frase que lee la persona, p. ej.: «Esta mantención saldría en ≈ $600.000 (entre $540.000 y $660.000). Trotadora × 4 a
    $150.000 c/u, mediana de 6 cotizaciones…; la más parecida es la COT-000245 de Cliente Y, del 12/09/2026. Traslado a Viña del Mar
    $35.000. Confianza media. Quedan fuera 2 equipos sin clasificar.»"""
    trabajo = TRABAJO_TXT.get((tipo_ot or "").strip().lower(), "Este trabajo")
    if not s.get("hay"):
        mot = [MOTIVO_TXT.get(m, m) for m in s.get("motivos") or []] or ["no hay datos con qué compararlo"]
        return "Sin valor sugerido: " + "; ".join(mot) + "."
    partes = []
    rango = s.get("rango")
    if s["modo"] == "mercado":
        cab = f"Precio de mercado de {trabajo[0].lower() + trabajo[1:]}: ≈ {clp(s['monto'])}"
    else:
        cab = f"{trabajo} saldría en ≈ {clp(s['monto'])}"
    if rango and rango["p25"] != rango["p75"]:
        cab += f" (entre {clp(rango['p25'])} y {clp(rango['p75'])})"
    partes.append(cab + ".")
    clases = sorted(s["por_clase"], key=lambda c: -c["monto"])
    detalle = "; ".join(_frase_clase(c, s.get("tipo_servicio")) for c in clases)
    ref = next((c.get("ref") for c in clases if c.get("ref") and c["fuente"] in ("cotiz_cliente", "cotiz_comparable", "ot_comparable")),
               None)
    if ref and ref.get("numero"):
        detalle += f"; la más parecida es la {ref['numero']}" + (f" de {ref['cliente']}" if ref.get("cliente") else "") + \
                   (f", del {ref['fecha']}" if ref.get("fecha") else "")
    partes.append(detalle + ".")
    tr = s.get("traslado")
    if tr and tr.get("monto"):
        partes.append(("Traslado" + (f" a {tr['comuna']}" if tr.get("comuna") else "") + f" {clp(tr['monto'])}, aparte"
                       + (" (de la misma cotización)" if tr.get("fuente") == "cotizacion" else "") + "."))
    partes.append(f"Confianza {s['confianza']}" + (" (trabajo de bodega comparado con una mantención)" if s.get("aproximado") else "")
                  + (f"; cubre {int(round((s['cobertura_equipos'] or 0) * 100))} % de los equipos" if (s.get("cobertura_equipos") or 1) < 1 else "")
                  + ".")
    fuera = []
    n_sc = len(s.get("sin_clasificar") or [])
    if n_sc:
        fuera.append(f"{n_sc} equipo{'s' if n_sc != 1 else ''} sin clasificar")
    n_sp = len(s.get("sin_precio") or [])
    if n_sp:
        fuera.append(f"{n_sp} tipo{'s' if n_sp != 1 else ''} de equipo sin precio")
    if fuera:
        partes.append("Quedan fuera " + " y ".join(fuera) + ".")
    if s["modo"] == "mercado":
        c = s.get("comparacion") or {}
        if c.get("cobrado") is not None:
            p = c.get("diferencia_pct")
            if p is not None and p > 0:
                partes.append(f"Cobramos {clp(c['cobrado'])} por el servicio: {_cant_txt(round(p))} % bajo lo sugerido.")
            elif p is not None and p < 0:
                partes.append(f"Cobramos {clp(c['cobrado'])} por el servicio: {_cant_txt(round(-p))} % sobre lo sugerido.")
            else:
                partes.append(f"Cobramos {clp(c['cobrado'])} por el servicio: igual a lo sugerido.")
        partes.append("La OT se cobra con su documento: esto es solo referencia y no se guarda.")
    return " ".join(partes)


# ── Aplicar: la decisión de la persona, recalculada en el servidor ────────────────────────────────────────────────────
DECISIONES = ("aceptada", "editada", "descartada")
MOTIVO_MIN = 15
MONTO_MAX = 2_000_000_000


def decidir(sug, decision, monto, motivo):
    """Qué se escribe al aplicar. `sug` es la sugerencia RECALCULADA en el servidor (el navegador nunca fija el rótulo).
    Devuelve {'ok': True, 'escribe': bool, 'monto': int|None, 'fuente': str|None} o {'ok': False, 'codigo', 'error', 'http'}.
      · aceptada: el monto tiene que ser EXACTAMENTE el sugerido → rótulo de la fuente. Si no coincide: con motivo (≥ 15) queda
        'a_mano'; sin motivo, 409 SUGERENCIA_CAMBIO (la sugerencia cambió o el monto fue manipulado) y no se escribe nada.
      · editada: motivo obligatorio (≥ 15); 'a_mano' salvo que el monto sea igual al sugerido.
      · descartada: no escribe la OT (solo queda la constancia).
    En una OT que se cobra no se guarda por esta vía (SE_COBRA)."""
    decision = (decision or "").strip().lower()
    motivo = (motivo or "").strip()
    if decision not in DECISIONES:
        return {"ok": False, "codigo": "DECISION_INVALIDA", "http": 400,
                "error": "La decisión tiene que ser «aceptada», «editada» o «descartada»."}
    if decision == "descartada":
        return {"ok": True, "escribe": False, "monto": None, "fuente": None}
    if (sug or {}).get("modo") == "mercado":
        return {"ok": False, "codigo": "SE_COBRA", "http": 409,
                "error": "Esta OT se cobra: lo cobrado con su documento es el valor. La sugerencia es solo referencia de mercado y no se guarda."}
    m = _num(monto)
    if m is None or m <= 0 or m > MONTO_MAX:
        return {"ok": False, "codigo": "MONTO_INVALIDO", "http": 400, "error": "El monto no es válido."}
    m = int(round(m))
    sugerido = (sug or {}).get("monto") if (sug or {}).get("hay") else None
    coincide = sugerido is not None and int(sugerido) == m and (sug or {}).get("puede_guardar")
    if coincide:
        return {"ok": True, "escribe": True, "monto": m, "fuente": sug["fuente"]}
    if len(motivo) < MOTIVO_MIN:
        if decision == "aceptada":
            return {"ok": False, "codigo": "SUGERENCIA_CAMBIO", "http": 409,
                    "error": ("La sugerencia ya no es la que viste" + (f" (ahora es {clp(sugerido)})" if sugerido is not None else "")
                              + ". Revísala de nuevo, o escribe el motivo para guardarlo como valor escrito a mano."),
                    "sugerido": sugerido}
        return {"ok": False, "codigo": "MOTIVO_CORTO", "http": 400,
                "error": f"Escribe por qué cambias el valor sugerido (mínimo {MOTIVO_MIN} caracteres): queda registrado con tu nombre."}
    return {"ok": True, "escribe": True, "monto": m, "fuente": "a_mano"}


def base_json(sug):
    """Lo que se guarda como respaldo de la sugerencia (ids de cotizaciones y OT, percentiles, cobertura, traslado). Sin nombres."""
    s = sug or {}
    return {"monto": s.get("monto"), "rango": s.get("rango"), "cobertura_equipos": s.get("cobertura_equipos"), "confianza": s.get("confianza"),
            "fuente": s.get("fuente"), "traslado": (s.get("traslado") or {}).get("monto"),
            "por_clase": [{"clase": c.get("clase"), "unidades": c.get("unidades"), "unit": c.get("unit"), "p25": c.get("p25"),
                           "p75": c.get("p75"), "fuente": c.get("fuente"), "n": c.get("n"), "cot_ids": c.get("cot_ids"),
                           "ot_ids": c.get("ot_ids")} for c in s.get("por_clase") or []],
            "cotizaciones": s.get("base", {}).get("cotizaciones"), "ots": s.get("base", {}).get("ots")}


# ── Pantalla «Valorizar en lote» ──────────────────────────────────────────────────────────────────────────────────────
PERIODOS = ("todo", "semana", "mes", "trimestre", "rango")
ESTADOS_LOTE = ("cerradas", "abiertas", "todas")
POR_PAGINA = (25, 50, 100, 200)
COBERTURAS_LOTE = ("garantia", "regalia", "arriendo_leasing", "sin_costo", "contrato", "interno", "cubierta")


def leer_parametros_lote(args):
    """Filtros del lote sin confiar en nada (un valor raro vuelve al defecto). `args`: cualquier mapa con .get."""
    def g(k, d=""):
        try:
            return (args.get(k) or d)
        except Exception:
            return d
    modo = str(g("modo", "todo")).strip().lower()
    modo = modo if modo in PERIODOS else "todo"
    estado = str(g("estado", "cerradas")).strip().lower()
    estado = estado if estado in ESTADOS_LOTE else "cerradas"
    cob = str(g("cobertura", "")).strip().lower()
    cob = cob if cob in COBERTURAS_LOTE else ""
    conf = str(g("confianza", "")).strip().lower()
    conf = conf if conf in NIVELES else ""
    try:
        page = max(1, int(g("page", 1)))
    except (TypeError, ValueError):
        page = 1
    try:
        por = int(g("por", 50))
    except (TypeError, ValueError):
        por = 50
    por = por if por in POR_PAGINA else 50
    q = str(g("q", "")).strip()[:80]
    ver_desc = str(g("ver_descartadas", "")).strip() in ("1", "true", "si", "sí")
    return {"modo": modo, "ancla": _fecha(g("ancla")), "desde": _fecha(g("desde")), "hasta": _fecha(g("hasta")), "estado": estado,
            "cobertura": cob, "confianza": conf, "page": page, "por": por, "q": q, "ver_descartadas": ver_desc}


def filtrar_filas(filas, p):
    """Filtros que se aplican sobre la lista ya armada (búsqueda, cobertura, confianza, descartadas)."""
    out = []
    q = (p.get("q") or "").lower()
    for f in filas or []:
        if not p.get("ver_descartadas") and f.get("descartada"):
            continue
        if p.get("cobertura") and f.get("cobertura") != p["cobertura"]:
            continue
        if p.get("confianza") and f.get("confianza") != p["confianza"]:
            continue
        if q and q not in (" ".join(str(f.get(k) or "") for k in ("numero", "cliente", "tipo_txt"))).lower():
            continue
        out.append(f)
    return out


def paginar(filas, page, por):
    total = len(filas or [])
    paginas = max(1, (total + por - 1) // por)
    page = min(max(1, page), paginas)
    ini = (page - 1) * por
    return {"filas": (filas or [])[ini:ini + por], "total": total, "paginas": paginas, "page": page, "por": por,
            "desde": (ini + 1) if total else 0, "hasta": min(ini + por, total)}


def kpis_lote(valorizadas, sugeridas):
    """«Dejamos de cobrar» (Σ valorizado de lo NO cobrado, ya guardado) y «Se nos escapa en internos» (la parte de trabajos internos),
    más lo que falta valorizar (Σ sugerido). Nunca se mezcla con «Nos costó» (la caja): eso es otro número.
    valorizadas: [{cobertura, monto}] (OT que no se cobran y ya tienen valorizado); sugeridas: [{cobertura, monto|None}]."""
    dejamos = sum(int(round(_num(v.get("monto")) or 0)) for v in valorizadas or [])
    internos = sum(int(round(_num(v.get("monto")) or 0)) for v in valorizadas or [] if v.get("cobertura") == "interno")
    con_sug = [s for s in sugeridas or [] if s.get("monto")]
    return {"dejamos_de_cobrar": dejamos, "n_valorizadas": len(valorizadas or []),
            "internos": internos, "n_internos": sum(1 for v in valorizadas or [] if v.get("cobertura") == "interno"),
            "por_valorizar_n": len(sugeridas or []), "por_valorizar_con_sugerencia": len(con_sug),
            "por_valorizar_sugerido": sum(int(s["monto"]) for s in con_sug),
            "por_valorizar_internos": sum(int(s["monto"]) for s in con_sug if s.get("cobertura") == "interno")}

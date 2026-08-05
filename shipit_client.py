"""
Cliente Shipit — construcción de payloads y parseo de respuestas.

MÓDULO PURO: sin Flask, sin BD, sin red. Solo transforma datos. Así se puede
testear sin levantar la app (ver tests/test_shipit_payload.py). Las llamadas
HTTP reales viven en app.py (`_shipit_request`), igual que el patrón de
`simpliroute_client.py` y `fedex_labels.py`.

Contrato de la API (developers.shipit.cl, verificado 2026-08-04):
  · Base URL : https://api.shipit.cl
  · Auth     : headers  X-Shipit-Email + X-Shipit-Access-Token
               (NO la contraseña del panel — esa solo sirve para generar el
               token en Configuración → API dentro de app.shipit.cl)
  · Formato  : Content-Type: application/json
               Accept: application/vnd.shipit.v4
  · Cotizar  : POST /v/rates      → body {"parcel": {...}}
  · Comunas  : GET  /v/communes   → array plano, SIN paginación

Pedido del área comercial (2026-08-04, vía Daniel): sumar Shipit al
comparador de couriers, ADEMÁS de FedEx/Felca/Milling/Clickex (nunca en
reemplazo — Regla #4.2). Fase 1 del plan: solo cotizar, sin crear envíos
todavía.

⚠ Límites reales verificados contra la documentación (no de oídas):
  · UN bulto por envío. Multi-bulto = N envíos Shipit separados, cada uno
    con su propia etiqueta y su propio cobro (Centro de Ayuda Shipit,
    artículo 360046424353). El "20 kg" que se comentó no aparece en NINGUNA
    fuente oficial — el tope real depende del courier (5-500 kg) y hay que
    confirmarlo con el ejecutivo antes de la Fase 2.
"""

BASE_URL = "https://api.shipit.cl"

# Endpoints
EP_RATES = "/v/rates"
EP_COMMUNES = "/v/communes"


def _s(v, maxlen=None):
    """String limpio; '' si es None. Trunca si se pide."""
    out = "" if v is None else str(v).strip()
    return out[:maxlen] if maxlen else out


def _f(v):
    """Float o None (nunca lanza)."""
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _i(v):
    """Int o None (nunca lanza)."""
    if v is None or v == "":
        return None
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


# ── Normalización de comunas (para homologar contra el ERP) ─────────────
# Los nombres de Shipit vienen en MAYÚSCULAS sin tilde (ej. "LAS CONDES",
# "ACHAO" — ver ejemplo real de /v/communes). El ERP entrega nombres con
# tilde y mayúscula/minúscula mixta (ej. "Ñuñoa", "Lo Barnechea"). Se
# normalizan ambos lados igual antes de comparar.
_TILDES = str.maketrans("ÁÉÍÓÚÑáéíóúñ", "AEIOUNaeioun")


def normalizar_comuna(nombre):
    """Mayúsculas, sin tilde, sin espacios de más. '' si no hay nombre."""
    s = _s(nombre).translate(_TILDES).upper()
    return " ".join(s.split())


def find_commune_id(nombre_comuna, communes):
    """Busca el commune_id de Shipit para un nombre de comuna del ERP.

    `communes` es la lista cruda de GET /v/communes (o una ya cacheada en
    BD con el mismo shape: al menos 'id', 'name', 'is_available').

    Devuelve (commune_id, is_available) o (None, False) si no hay match.
    Si hay más de un match, prefiere el que tenga is_available=True.
    """
    target = normalizar_comuna(nombre_comuna)
    if not target or not communes:
        return None, False

    candidatos = [c for c in communes if normalizar_comuna(c.get("name")) == target]
    if not candidatos:
        return None, False

    disponibles = [c for c in candidatos if c.get("is_available")]
    elegido = disponibles[0] if disponibles else candidatos[0]
    return elegido.get("id"), bool(elegido.get("is_available"))


# ── Elegibilidad para Shipit (restricciones de negocio) ──────────────────
# Pedido de Daniel (2026-08-04): "que no exceda de un bulto y que no exceda
# de los quince kilos... si no, inmediatamente le hace una alerta... maneja
# la restricción indicada, o quince kilos o no es multibulto, dependiendo
# de cada caso -- no vayas a mandar algo genérico, algo específico."
#
# MAX_PESO_KG = 15.0 viene del Centro de Ayuda oficial de Shipit (retiro con
# flota propia "héroe": hasta 15 kg o arista < 60cm -- artículo 360007646813).
# Daniel decidió usarlo como tope operativo directo ("si tú me dices que son
# quince kilos, yo encantado de limitarla"), sin esperar la confirmación del
# ejecutivo -- acepta explícitamente que las barras de 20 kg queden fuera de
# Shipit y se coticen por otro courier. Constantes (no mágicas) para poder
# ajustarlas después sin tocar la lógica, si el contrato real de ILUS define
# otro tope por courier.
MAX_BULTOS = 1
MAX_PESO_KG = 15.0


def verificar_restricciones(n_bultos, peso_kg):
    """Restricciones de negocio de Shipit para ILUS.

    Devuelve una LISTA de mensajes ESPECÍFICOS -- uno por regla incumplida,
    nunca un texto genérico tipo "no cumple los requisitos" -- para que la
    alerta le diga a quien cotiza EXACTAMENTE cuál es el problema. Lista
    vacía = el envío puede cotizarse/crearse por Shipit.

    No valida cobertura de comuna: eso ya lo resuelve find_commune_id()
    devolviendo (None, False) cuando la comuna no está disponible -- ese
    caso se trata como "sin cobertura" (mismo criterio que otros couriers),
    no como una restricción de peso/bultos.
    """
    problemas = []
    n = _i(n_bultos)
    peso = _f(peso_kg)

    if n is not None and n > MAX_BULTOS:
        problemas.append(
            f"Este envío tiene {n} bultos — Shipit no acepta más de "
            f"{MAX_BULTOS} bulto por envío (cada bulto extra necesita su "
            f"propia guía, con cobro aparte)."
        )
    if peso is not None and peso > MAX_PESO_KG:
        problemas.append(
            f"Este envío pesa {peso:g} kg — Shipit no acepta más de "
            f"{MAX_PESO_KG:g} kg por bulto."
        )
    return problemas


# ── Cotizar (POST /v/rates) ──────────────────────────────────────────────

def build_rate_payload(*, length, width, height, weight, origin_id, destiny_id,
                       type_of_destiny="domicilio", courier_for_client=None):
    """Arma el body de POST /v/rates.

    Todos los campos de dimensión/peso son obligatorios en la API. Devuelve
    (payload_dict, errores_list) — si `errores` no está vacío, NO cotizar.
    """
    errores = []

    l, w, h = _f(length), _f(width), _f(height)
    peso = _f(weight)
    oid, did = _i(origin_id), _i(destiny_id)

    if not l or l <= 0:
        errores.append("largo inválido")
    if not w or w <= 0:
        errores.append("ancho inválido")
    if not h or h <= 0:
        errores.append("alto inválido")
    if not peso or peso <= 0:
        errores.append("peso inválido")
    if not oid:
        errores.append("comuna de origen sin commune_id de Shipit (falta homologar)")
    if not did:
        errores.append("comuna de destino sin commune_id de Shipit (falta homologar)")

    parcel = {
        "length": l or 0, "width": w or 0, "height": h or 0, "weight": peso or 0,
        "origin_id": oid, "destiny_id": did,
        "type_of_destiny": _s(type_of_destiny) or "domicilio",
    }
    cfc = _s(courier_for_client)
    if cfc:
        parcel["courier_for_client"] = cfc

    return {"parcel": parcel}, errores


def parse_rates_response(data):
    """Normaliza la respuesta de POST /v/rates a una lista de cotizaciones.

    Devuelve [{courier, servicio, precio, dias, peso_volumetrico,
    disponible, commune_destino_id}, ...] — una fila por courier que
    Shipit devolvió en 'prices'. Nunca lanza; con data rara devuelve [].
    """
    if not isinstance(data, dict):
        return []
    prices = data.get("prices")
    if not isinstance(prices, list):
        return []

    out = []
    for p in prices:
        if not isinstance(p, dict):
            continue
        courier = p.get("courier") or {}
        destiny = p.get("destiny") or {}
        out.append({
            "courier": _s(courier.get("name")),
            "servicio": _s(p.get("name")),
            "precio": _f(p.get("price")),
            "dias": _i(p.get("days")),
            "peso_volumetrico": _f(p.get("volumetric_weight")),
            "disponible": bool(p.get("available_to_shipping")),
            "commune_destino_id": destiny.get("commune_id"),
        })
    return out


def lower_price(data):
    """Extrae la opción más barata ('lower_price') ya elegida por Shipit.

    Devuelve el mismo shape que una fila de parse_rates_response(), o None.
    """
    if not isinstance(data, dict):
        return None
    lp = data.get("lower_price")
    if not isinstance(lp, dict) or not lp:
        return None
    parsed = parse_rates_response({"prices": [lp]})
    return parsed[0] if parsed else None


def extract_error_message(status_code, body):
    """Convierte un error de la API en un mensaje corto y accionable.

    Formato de error documentado (400): {"message": "...", "state": "error"}.
    Nunca lanza, siempre devuelve un string.
    """
    if status_code == 401:
        return ("Token de Shipit inválido o vencido (401). Revisa "
                "SHIPIT_API_TOKEN/SHIPIT_API_EMAIL en las variables de entorno.")
    if status_code == 403:
        return "El token no tiene permisos para esta operación en Shipit (403)."
    if status_code == 404:
        return "Endpoint de Shipit no encontrado (404)."

    if isinstance(body, dict):
        msg = _s(body.get("message"))
        if msg:
            return msg[:280]
        partes = []
        for campo, val in body.items():
            if campo in ("state",):
                continue
            if isinstance(val, (list, tuple)):
                partes.append(f"{campo}: {'; '.join(_s(x) for x in val)}")
            else:
                partes.append(f"{campo}: {_s(val)}")
        if partes:
            return " | ".join(partes)[:280]

    if body:
        return _s(body)[:280]
    return f"Error HTTP {status_code}"

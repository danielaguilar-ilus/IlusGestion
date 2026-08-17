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
# ── Tope de peso: 20 kg desde el 2026-08-17 (era 15) ────────────────────
# Pedido explícito de Daniel: "aumentemos la restricción de Shipit de 15 kg a
# 20 kg para realizar pruebas". Se le había informado antes, ese mismo día,
# que 15 kg es el tope OFICIAL documentado y decidió subirlo igual.
#
# ⚠️ QUÉ SIGNIFICA ESTO EN LA REALIDAD (verificado en el Centro de Ayuda de
# Shipit, artículos 360007646813 y 1260803326050 -- no de oídas):
#   · Retiro con la FLOTA PROPIA de Shipit ("héroe"), que es el flujo que usa
#     ILUS hoy: tope duro de 15 kg o arista mayor a 60 cm. Textual: "solo
#     retiramos con héroe productos que tengan un peso de hasta 15 kg". Si el
#     bulto excede, el héroe avisa al coordinador de flota EN LA BODEGA.
#   · Retiro directo por el courier (excepción manual, hay que avisarle a
#     Shipit antes de las 11:00 del mismo día): ahí el tope sube según el
#     operador -- Chilexpress/Bluexpress 100 kg, Starken 500 kg,
#     Sameday/Nextday 15-25 kg, 99minutos 25 kg, Spread 15 kg.
#
# O sea: entre 15 y 20 kg, ILUS ahora COTIZA por Shipit, pero el retiro puede
# rebotar en bodega salvo que se gestione la excepción con el ejecutivo. Por
# eso esto es una constante y no una regla enterrada: volver a 15 es cambiar
# este número (y su espejo SHIPIT_MAX_PESO_KG en static/cubicador_asignar.js).
MAX_BULTOS = 1
MAX_PESO_KG = 20.0


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


# ── Separar calle y número (obligatorio para crear envíos) ───────────────
# Shipit exige `street` y `number` como campos SEPARADOS y obligatorios al
# crear un envío. En ILUS la dirección es un solo texto libre
# (transport_commitments.direccion, VARCHAR(300)) escrito a mano o traído del
# ERP, así que hay que partirla.
#
# REGLA DE ORO (decisión de diseño): ante la duda NO se adivina. Un número
# inventado manda el paquete a otra casa; un envío bloqueado con un mensaje
# claro solo cuesta que alguien corrija la dirección. Por eso la función
# devuelve los problemas encontrados en vez de "hacer lo que puede".
#
# Módulo puro y SIN llamadores todavía: se puede probar y afinar sin tocar
# ningún flujo existente (Fase 2 la conecta después).

_NUM_PREFIJOS = ("n°", "nº", "n.°", "no.", "nro.", "nro", "num.", "num", "#")

# Palabras que, cuando aparecen JUSTO ANTES de un número, indican que ese
# número identifica una unidad interior (departamento, oficina, bodega...),
# NO la casa sobre la calle.
#
# Sin esto, "Los Aromos 145 depto 402" -- que llega sin coma -- se resolvía
# como calle="Los Aromos 145 depto", número="402": el paquete saldría con el
# número del departamento como número de calle y terminaría en otra
# dirección. Es el error más caro que puede cometer esta función, porque no
# falla ruidosamente: genera una guía perfectamente válida hacia el lugar
# equivocado.
_PALABRAS_UNIDAD = {
    "depto", "dpto", "dep", "depart", "departamento", "departamentos",
    "of", "ofic", "oficina", "casa", "blk", "block", "torre", "piso",
    "local", "bod", "bodega", "lote", "sitio", "parcela", "interior",
    "int", "pta", "puerta",
}

# Un número de casa chileno: 1 a 6 dígitos, opcionalmente con una letra
# pegada o separada por guion ("1234", "1234A", "145-B").
_RE_NUMERO = None


def _compilar_regex():
    global _RE_NUMERO
    if _RE_NUMERO is None:
        import re
        _RE_NUMERO = re.compile(r"^(\d{1,6})(?:\s*-\s*|\s*)?([A-Za-z]{1,2})?$")
    return _RE_NUMERO


def split_street_number(direccion):
    """Separa una dirección chilena en (calle, numero, problemas).

    `problemas` es una lista de mensajes ESPECÍFICOS -- vacía significa que se
    pudo separar con confianza. Si NO está vacía, calle/numero pueden venir
    incompletos y el envío NO debe crearse con esos datos.

    Ejemplos que resuelve bien:
        "Colon 1265"                                  -> ("Colon", "1265")
        "Av. Pdte. Eduardo Frei Montalva 9770, Bod 30" -> ("Av. Pdte. Eduardo Frei Montalva", "9770")
        "FRANCISCO DE VILLAGRA 327, GYM TORRE B"       -> ("FRANCISCO DE VILLAGRA", "327")
        "Los Aromos N° 145-B, depto 402"               -> ("Los Aromos", "145-B")

    Casos que marca como problema en vez de inventar:
        "Camino a Melipilla s/n"      -> sin número
        "Las Condes"                  -> sin número
        "1265"                        -> sin nombre de calle
        "Camino Rural Km 12"          -> kilometraje, necesita revisión manual
    """
    problemas = []
    texto = _s(direccion)
    if not texto:
        return "", "", ["La dirección está vacía."]

    # Solo interesa el primer tramo: lo que viene después de la primera coma
    # suele ser depto / oficina / block / comuna, no la calle.
    #   "Av. Frei 9770, Bod 30, Quilicura" -> "Av. Frei 9770"
    primer_tramo = texto.split(",")[0].strip()
    if not primer_tramo:
        return "", "", [f"No se pudo leer la calle en «{texto}»."]

    bajo = primer_tramo.lower()

    # "s/n" = sin número. Es explícito, no es una falla de parseo, pero Shipit
    # igual exige un número, así que el envío no puede salir sin intervención.
    if " s/n" in f" {bajo}" or bajo.endswith(" sn") or bajo == "s/n":
        return primer_tramo, "", [
            f"La dirección «{texto}» no tiene número (s/n). Shipit exige "
            f"calle y número por separado: hay que completarla antes de "
            f"generar la guía."
        ]

    # Kilometraje: "Camino a Melipilla Km 12". Es una dirección válida en
    # Chile pero no encaja en el par calle/número, y cada courier la trata
    # distinto. Se marca para que alguien la revise en vez de mandar "12".
    if " km " in f" {bajo} " or bajo.endswith(" km"):
        return primer_tramo, "", [
            f"La dirección «{texto}» está expresada en kilómetros. Necesita "
            f"revisión manual: no se puede convertir a calle + número sin "
            f"riesgo de equivocar el destino."
        ]

    # Se limpian los prefijos de número para que "N° 145" quede como "145".
    limpio = primer_tramo
    for pref in _NUM_PREFIJOS:
        import re as _re
        limpio = _re.sub(_re.escape(pref) + r"\s*", " ", limpio, flags=_re.IGNORECASE)
    tokens = limpio.split()
    if not tokens:
        return "", "", [f"No se pudo leer la calle en «{texto}»."]

    # Se busca el ÚLTIMO token que parezca número de casa. El último y no el
    # primero porque hay calles que llevan números en el nombre:
    # "Pasaje 5 Norte 1234" -> la casa es la 1234, no la 5.
    rx = _compilar_regex()
    idx_num = None
    for i in range(len(tokens) - 1, -1, -1):
        if not rx.match(tokens[i]):
            continue
        # Si el token anterior es "depto", "of", "casa"... este número es de
        # una unidad interior: se sigue buscando hacia atrás.
        anterior = tokens[i - 1].lower().strip(".,-°º") if i > 0 else ""
        if anterior in _PALABRAS_UNIDAD:
            continue
        idx_num = i
        break

    if idx_num is None:
        return primer_tramo, "", [
            f"La dirección «{texto}» no tiene un número de calle reconocible. "
            f"Shipit exige calle y número por separado."
        ]

    if idx_num == 0:
        # El número quedó primero y no hay nombre de calle antes.
        return "", tokens[0], [
            f"En «{texto}» se encontró un número pero no el nombre de la calle."
        ]

    m = rx.match(tokens[idx_num])
    numero = m.group(1) + (("-" + m.group(2).upper()) if m.group(2) else "")
    calle = " ".join(tokens[:idx_num]).strip(" .,-")

    if not calle:
        return "", numero, [
            f"En «{texto}» se encontró un número pero no el nombre de la calle."
        ]

    # Si después del número quedan tokens sueltos (sin coma que los separe),
    # suelen ser "depto 4", "casa B", "of 301". No es un error, pero se avisa
    # porque significa que la dirección venía sin la puntuación esperada.
    sobrante = " ".join(tokens[idx_num + 1:]).strip()
    if sobrante:
        problemas.append(
            f"En «{texto}» quedó texto después del número («{sobrante}»). "
            f"Se usó {numero} como número; conviene confirmarlo."
        )

    return calle, numero, problemas


def clasificar_direccion(direccion):
    """split_street_number() empaquetado para la pantalla. Nunca lanza.

    Devuelve un dict listo para responder por JSON:
        {"calle": str, "numero": str, "problemas": [str], "bloqueada": bool}

    `bloqueada=True` significa: NO se puede armar el par calle+número con
    confianza y hay que pedirle a una persona que lo complete antes de
    generar la guía. `bloqueada=False` con `problemas` no vacío significa que
    SÍ se pudo separar, pero conviene que alguien confirme el número (el caso
    típico: la dirección venía sin coma y quedó texto suelto después del
    número, como "Los Aromos 145 depto 402").

    La distinción existe para que la UI no trate igual "falta el número"
    (bloquea el despacho) que "revisa el número" (solo avisa). Vive acá y no
    en app.py a propósito: es lógica pura, se prueba sin levantar Flask, y el
    endpoint queda como una envoltura de 5 líneas.
    """
    calle, numero, problemas = split_street_number(direccion)
    return {
        "calle": calle,
        "numero": numero,
        "problemas": list(problemas or []),
        "bloqueada": not (calle and numero),
    }


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

"""
Cliente Clickex — construcción de payloads y parseo de respuestas.

MÓDULO PURO: sin Flask, sin BD, sin red. Solo transforma datos. Así se puede
testear sin levantar la app (ver tests/test_clickex_payload.py). Las llamadas
HTTP reales viven en app.py (`_clickex_request`), mismo patrón que
`shipit_client.py` / `simpliroute_client.py` / `fedex_labels.py`.

Contrato de la API (https://clickex.cl/apiv3/docs/openapi.yaml, verificado
2026-08-25 -- leído directo del spec, no de memoria):
  · Base URL : https://clickex.cl/apiv3
  · Auth     : header x-api-key (SIEMPRE) + headers username/password (en
               los endpoints que identifican al seller: matriz de tarifas y
               creación de envío). /tracking/{tracking} solo pide x-api-key.
  · Formato  : Content-Type: application/json

  · Tarifas ya negociadas del seller:
      GET /sellerShipmentMatrixCosts  (headers username+password, sin body)
      -> {"costs": [{"commune": str, "sla": int, "net_cost": number}, ...]}
      Verificado en vivo 2026-08-23/24: cuenta ILUS trae 37 comunas, TODAS
      Región Metropolitana, tarifa plana $3.520, SLA 1 día. Es la fuente de
      precio PRINCIPAL para Clickex -- no requiere cotizar por envío.

  · Cotización puntual por ciudad (Shopify-style), requiere sellerId propio:
      POST /costShopify/{sellerId}   body {"rate":{"destination":{"city":...}}}
      -> {"rates": [{service_name, service_code, total_price, ...}]}
      Sin sellerId confirmado todavía -- queda para cuando Clickex lo entregue.

  · Tracking:
      GET /tracking/{tracking}  (solo x-api-key)
      -> {"response": {"tracking": {...}, "shipments_status_log": [...], ...}}

  · Crear envío (acción REAL, con costo y despacho -- Fase 1 de esta
    integración NO la dispara desde ningún botón todavía, ver app.py
    _clickex_request / CLICKEX_FASE_ENVIOS):
      POST /shipmentsAdd  (headers username+password + x-api-key)
      body: ShipmentRequest -- ver build_shipment_payload() abajo.

Pedido de Daniel (2026-08-24/25, vía chat): activar Clickex "en el flujo
natural" -- cotizar, crear pedido, consultar estado y traer precios --
partiendo de la matriz de tarifas ya negociada ($3.520 plano, RM). Límite de
peso confirmado: 25 kg (no se limitan dimensiones de barras).
"""

BASE_URL = "https://clickex.cl/apiv3"

# Endpoints
EP_MATRIX_COSTS = "/sellerShipmentMatrixCosts"
EP_TRACKING = "/tracking/{tracking}"
EP_COST_SHOPIFY = "/costShopify/{sellerId}"
EP_SHIPMENTS_ADD = "/shipmentsAdd"


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
# La matriz de Clickex trae nombres "humanos" (ej. "Ñuñoa", "Providencia",
# a veces también "Nunoa" sin tilde como fila separada -- visto en vivo). El
# ERP entrega nombres con tilde y mayúscula/minúscula mixta. Mismo criterio
# que shipit_client.normalizar_comuna: comparar ambos lados normalizados.
_TILDES = str.maketrans("ÁÉÍÓÚÑáéíóúñ", "AEIOUNaeioun")


def normalizar_comuna(nombre):
    """Mayúsculas, sin tilde, sin espacios de más. '' si no hay nombre."""
    s = _s(nombre).translate(_TILDES).upper()
    return " ".join(s.split())


# ── Restricciones de negocio (confirmado por Daniel 2026-08-24) ─────────
# "el tres mil quinientos veinte, perfecto... con respecto al límite, sí,
# son veinticinco kilos" -- a diferencia de Shipit, Daniel pidió explícita-
# mente NO limitar dimensiones de barras, solo el peso.
MAX_PESO_KG = 25.0


def verificar_restricciones(peso_kg):
    """Restricción de negocio de Clickex para ILUS: solo peso, sin tope de
    dimensiones (pedido explícito de Daniel, a diferencia de Shipit).

    Devuelve una LISTA de mensajes ESPECÍFICOS -- vacía si el envío puede
    cotizarse/crearse por Clickex.
    """
    problemas = []
    peso = _f(peso_kg)
    if peso is not None and peso > MAX_PESO_KG:
        problemas.append(
            f"Este envío pesa {peso:g} kg — Clickex no acepta más de "
            f"{MAX_PESO_KG:g} kg por bulto."
        )
    return problemas


# ── Matriz de tarifas (GET /sellerShipmentMatrixCosts) ───────────────────

def parse_matrix_costs_response(data):
    """Normaliza la respuesta de GET /sellerShipmentMatrixCosts.

    Devuelve [{comuna, comuna_normalizada, sla_dias, costo_neto}, ...].
    Nunca lanza; con data rara devuelve [].
    """
    if not isinstance(data, dict):
        return []
    costs = data.get("costs")
    if not isinstance(costs, list):
        return []

    out = []
    for c in costs:
        if not isinstance(c, dict):
            continue
        comuna = _s(c.get("commune"))
        if not comuna:
            continue
        out.append({
            "comuna": comuna,
            "comuna_normalizada": normalizar_comuna(comuna),
            "sla_dias": _i(c.get("sla")),
            "costo_neto": _f(c.get("net_cost")),
        })
    return out


def find_tarifa_comuna(nombre_comuna, tarifas):
    """Busca la tarifa de Clickex para una comuna del ERP.

    `tarifas` es la lista ya parseada (parse_matrix_costs_response(), o una
    ya cacheada en BD con las mismas llaves 'comuna_normalizada'/'costo_neto'
    /'sla_dias'). Devuelve el dict de tarifa o None si no hay cobertura.
    Si hay más de un match (ej. "Ñuñoa" y "Nunoa" como filas separadas -- caso
    real visto en la cuenta de ILUS), se queda con el primero: todas las
    filas duplicadas vistas hasta ahora traen el mismo precio.
    """
    target = normalizar_comuna(nombre_comuna)
    if not target or not tarifas:
        return None
    for t in tarifas:
        if t.get("comuna_normalizada") == target:
            return t
    return None


# ── Crear envío (POST /shipmentsAdd) ──────────────────────────────────────
# NO tiene caller todavía (2026-08-25): módulo puro, se puede probar y
# afinar sin disparar un envío real. Ver app.py para el porqué de no
# conectarlo a un botón en vivo todavía.

def build_shipment_payload(*, nombre_dest, email_dest, telefono_dest,
                            comuna, calle, numero, complemento,
                            alto=None, ancho=None, largo=None,
                            peso_total=None, total_bultos=1,
                            valor_declarado=None, nombre_remitente=None,
                            referencia=None, rut_cliente=None,
                            tipo_entrega=None, monto_contraentrega=None,
                            comentario=None):
    """Arma el body de POST /shipmentsAdd. Devuelve (payload_dict, errores).

    Campos obligatorios según el spec (ShipmentRequest): addressee
    (name/email/phone), address (commune/street/complement -- 'complement'
    es obligatorio aunque venga vacío, así que se manda '' si no hay), y
    Package. Si `errores` no está vacío, NO crear el envío.
    """
    errores = []

    nombre_dest = _s(nombre_dest)
    email_dest = _s(email_dest)
    telefono_dest = _s(telefono_dest)
    if not nombre_dest:
        errores.append("falta el nombre del destinatario")
    if not email_dest:
        errores.append("falta el email del destinatario")
    if not telefono_dest:
        errores.append("falta el teléfono del destinatario")

    comuna_s = _s(comuna)
    calle_s = _s(calle)
    numero_s = _s(numero)
    if not comuna_s:
        errores.append("falta la comuna de destino")
    if not calle_s:
        errores.append("falta la calle de destino")
    if not numero_s:
        errores.append("falta el número de destino")

    peso = _f(peso_total)
    if not peso or peso <= 0:
        errores.append("peso inválido")

    problemas_peso = verificar_restricciones(peso) if peso else []
    errores.extend(problemas_peso)

    payload = {
        "addressee": {
            "name": nombre_dest,
            "email": email_dest,
            "phone": telefono_dest,
        },
        "address": {
            "commune": comuna_s,
            "street": calle_s,
            "number": numero_s,
            "complement": _s(complemento),
        },
        "Package": {
            "high": _f(alto) or 0,
            "width": _f(ancho) or 0,
            "long": _f(largo) or 0,
            "totalWeight": peso or 0,
            "totalPackages": _i(total_bultos) or 1,
        },
    }
    if valor_declarado is not None:
        payload["Package"]["amountDeclaredValue"] = _f(valor_declarado) or 0
    if nombre_remitente:
        payload["sender"] = {"name": _s(nombre_remitente)}
    if referencia:
        payload["referenciaEnvio"] = _s(referencia)
    if rut_cliente:
        payload["customer_identification_number"] = _s(rut_cliente)
    if tipo_entrega is not None:
        payload["deliveryType"] = _i(tipo_entrega)
    if monto_contraentrega is not None:
        payload["amountDelivery"] = _f(monto_contraentrega) or 0
    if comentario:
        payload["commentary"] = _s(comentario)

    return payload, errores


def parse_shipment_response(data):
    """Normaliza la respuesta de POST /shipmentsAdd. Nunca lanza.

    El spec no fija un schema estricto de respuesta ("Envío creado", sin más
    detalle) -- se devuelve el dict crudo bajo 'raw' además de los campos
    más probables (tracking/id), para no perder información si el shape
    real trae más de lo documentado.
    """
    if not isinstance(data, dict):
        return {"tracking": None, "raw": data}
    tracking = (data.get("tracking") or data.get("Tracking") or
                data.get("shipment_tracking") or data.get("id"))
    return {"tracking": _s(tracking) if tracking else None, "raw": data}


# ── Tracking (GET /tracking/{tracking}) ───────────────────────────────────

def parse_tracking_response(data):
    """Normaliza la respuesta de GET /tracking/{tracking}. Nunca lanza.

    Devuelve {tracking_info, historial, observacion} tal como vienen
    (additionalProperties: true en el spec -- no hay un shape cerrado que
    validar campo a campo).
    """
    if not isinstance(data, dict):
        return {"tracking_info": {}, "historial": [], "observacion": ""}
    resp = data.get("response")
    if not isinstance(resp, dict):
        return {"tracking_info": {}, "historial": [], "observacion": ""}
    historial = resp.get("shipments_status_log")
    return {
        "tracking_info": resp.get("tracking") or {},
        "historial": historial if isinstance(historial, list) else [],
        "observacion": _s(resp.get("checkout_observation")),
    }


def extract_error_message(status_code, body):
    """Convierte un error de la API en un mensaje corto y accionable.

    Formato de error documentado: {"error": "..."}. Nunca lanza.
    """
    if status_code == 401:
        return ("API key de Clickex inválida (401). Revisa CLICKEX_API_KEY "
                "en las variables de entorno.")
    if status_code == 404:
        return ("Clickex no encontró el recurso (404) -- usuario/contraseña "
                "incorrectos, o el tracking/ciudad no existe.")

    if isinstance(body, dict):
        msg = _s(body.get("error"))
        if msg:
            return msg[:280]

    if body:
        return _s(body)[:280]
    return f"Error HTTP {status_code}"

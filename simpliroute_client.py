"""
Cliente SimpliRoute — construcción de payloads y parseo de respuestas.

MÓDULO PURO: sin Flask, sin BD, sin red. Solo transforma datos. Así se puede
testear sin levantar la app (ver tests/test_simpliroute_payload.py). Las
llamadas HTTP reales viven en app.py (`_simpliroute_post_visits`), igual que
el patrón de `fedex_labels.py`.

Contrato de la API (documentation.simpliroute.com, verificado 2026-07-22):
  · Base URL : https://api.simpliroute.com
  · Auth     : header  Authorization: Token <TOKEN>   (literal "Token ", NO "Bearer")
  · Crear    : POST /v1/routes/visits/   ← acepta un objeto O UN ARRAY (bulk nativo)
  · Health   : GET  /v1/accounts/me/
  · Borrar   : POST /v1/bulk/delete/visits/   {"visits":[id,...]}
  ⚠ Es Django REST Framework: los paths llevan SLASH FINAL. Sin él → 301/405.

Campos OBLIGATORIOS de una visita: title, address, planned_date (YYYY-MM-DD).
La respuesta de creación trae `id` (int) y `tracking_id` (str) — ambos se
persisten en transport_manifest_items para no volver a crear la misma visita.
"""

BASE_URL = "https://api.simpliroute.com"

# Endpoints (con slash final — obligatorio en DRF)
EP_VISITS = "/v1/routes/visits/"
EP_ME = "/v1/accounts/me/"
EP_BULK_DELETE = "/v1/bulk/delete/visits/"

# Defaults operativos. Replican EXACTAMENTE lo que hoy va en el Excel
# "Felca y Milling" que el transportista sube a mano (app.py, formato
# 'simplyroute'), para que la carga por API no cambie el comportamiento:
#   Hora inicial 09:00:00 · Hora final 23:59:00 · Tiempo de servicio 3 (min)
DEFAULT_WINDOW_START = "09:00:00"
DEFAULT_WINDOW_END = "23:59:00"
DEFAULT_DURATION = "00:03:00"   # el Excel manda 3 (int minutos); la API exige HH:MM:SS


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


def build_visit_payload(item, *, planned_date, window_start=None, window_end=None,
                        duration=None):
    """Construye el JSON de UNA visita a partir de un item de manifiesto ILUS.

    `item` es una fila de _tr_manifiesto_items_simpliroute() (dict-like) con:
      tido, nudo, cliente_nombre, direccion, comuna, region, telefono, email,
      notas, n_bultos, peso_declarar, direccion_lat, direccion_lng.

    `planned_date` es la fecha del manifiesto en 'YYYY-MM-DD' (OBLIGATORIA
    para SimpliRoute; el Excel la dejaba vacía y el transportista la ponía).

    Devuelve (payload_dict, errores_list). Si `errores` no está vacío, la
    visita NO debe enviarse — le falta algo obligatorio.
    """
    errores = []

    nudo = _s(item.get("nudo"))
    cliente = _s(item.get("cliente_nombre"))
    direccion = _s(item.get("direccion"))
    comuna = _s(item.get("comuna"))
    region = _s(item.get("region"))

    # title — mismo formato que la columna "Título*" del Excel: "NUDO - CLIENTE"
    title = f"{nudo} - {cliente}".strip(" -")
    if not title:
        errores.append("sin título (falta nº documento y nombre de cliente)")

    # address — el Excel manda "direccion ,comuna". Para la API agregamos
    # región y país: SimpliRoute geocodifica el texto cuando no hay lat/lng,
    # y mientras más completo, mejor cae el pin.
    partes = [p for p in (direccion, comuna, region, "Chile") if p]
    address = ", ".join(partes)
    if not direccion:
        errores.append("sin dirección")

    planned = _s(planned_date)
    if not planned:
        errores.append("sin fecha planificada")

    payload = {
        "title": title,
        "address": address,
        "planned_date": planned,
    }

    # Coordenadas: opcionales. Si no van, SimpliRoute geocodifica el address
    # (es como funciona hoy el Excel, que manda Latitud/Longitud vacías).
    #
    # FIX 2026-07-25 — SimpliRoute RECHAZA más de 6 decimales:
    #   "latitude: Ensure that there are no more than 6 decimal places."
    # Google Places devuelve 13+ decimales, así que toda dirección validada con
    # el autocompletado hacía fallar la subida entera del manifiesto. Las
    # pruebas no lo detectaron porque usaban coordenadas escritas a mano de 4
    # decimales. 6 decimales son ~11 cm de precisión: no se pierde nada útil.
    lat = _f(item.get("direccion_lat"))
    lng = _f(item.get("direccion_lng"))
    if lat is not None and lng is not None:
        payload["latitude"] = round(lat, 6)
        payload["longitude"] = round(lng, 6)

    # load  = bultos   (paridad con la columna "Carga" del Excel)
    # load_2 = peso    (paridad con "Habilidades KILOS" del Excel)
    try:
        payload["load"] = max(1, int(float(item.get("n_bultos") or 1)))
    except (TypeError, ValueError):
        payload["load"] = 1
    peso = _f(item.get("peso_declarar"))
    if peso is not None and peso > 0:
        payload["load_2"] = round(peso, 2)

    payload["window_start"] = window_start or DEFAULT_WINDOW_START
    payload["window_end"] = window_end or DEFAULT_WINDOW_END
    payload["duration"] = duration or DEFAULT_DURATION

    # Contacto (opcional para la API, pero es lo que ve el chofer en su app)
    if cliente:
        payload["contact_name"] = _s(cliente, 100)
    tel = _s(item.get("telefono"), 30)
    if tel:
        payload["contact_phone"] = tel
    mail = _s(item.get("email"), 100)
    if mail:
        payload["contact_email"] = mail

    # reference: la doc lo define como nº de factura/pedido. Usamos TIDO-NUDO
    # para que sea único entre tipos de documento (FCV 100 ≠ VD 100).
    tido = _s(item.get("tido"))
    payload["reference"] = f"{tido}-{nudo}".strip("-") if (tido or nudo) else ""

    notas = _s(item.get("notas"), 500)
    if notas:
        payload["notes"] = notas

    return payload, errores


def build_visits_batch(items, *, planned_date, window_start=None, window_end=None,
                       duration=None):
    """Arma el array completo para el POST bulk.

    Devuelve (payloads, saltados) donde:
      payloads  = [(item_id, payload_dict), ...]  listos para enviar
      saltados  = [(item_id, "motivo"), ...]      con datos incompletos
    El item_id permite mapear la respuesta de vuelta a cada fila del manifiesto.
    """
    payloads, saltados = [], []
    for it in items:
        iid = it.get("item_id") or it.get("id")
        payload, errores = build_visit_payload(
            it, planned_date=planned_date, window_start=window_start,
            window_end=window_end, duration=duration)
        if errores:
            saltados.append((iid, "; ".join(errores)))
        else:
            payloads.append((iid, payload))
    return payloads, saltados


def parse_visits_response(data):
    """Normaliza la respuesta de POST /v1/routes/visits/ a una lista de dicts.

    La API devuelve un objeto si se mandó uno, o un array si se mandó array.
    Devuelve [{id, tracking_id, title, reference}, ...] en el MISMO orden en
    que se enviaron (SimpliRoute respeta el orden del array).
    """
    if data is None:
        return []
    if isinstance(data, dict):
        data = [data]
    if not isinstance(data, list):
        return []
    out = []
    for v in data:
        if not isinstance(v, dict):
            continue
        out.append({
            "id": v.get("id"),
            "tracking_id": v.get("tracking_id") or "",
            "title": v.get("title") or "",
            "reference": v.get("reference") or "",
        })
    return out


# ── TRACKING: traducir una visita de SimpliRoute al estado interno de ILUS ──
# Estados de SimpliRoute (doc oficial): pending | partial | completed | failed
# | canceled.  NO existe "in_progress": que el chofer va en camino se infiere
# de que `on_its_way` tenga timestamp.
# Estados internos de ILUS (ESTADOS_ENTREGA en app.py):
#   'En preparación' · 'Entregado a transporte' · 'En ruta' · 'Entregado'
#   · 'Entrega fallida' · 'Devolución'
SR_STATUS_COMPLETED = "completed"
SR_STATUS_FAILED = "failed"
SR_STATUS_PARTIAL = "partial"
SR_STATUS_CANCELED = "canceled"
SR_STATUS_PENDING = "pending"


def estado_ilus_from_visit(visit):
    """Traduce una visita de SimpliRoute a (estado_ilus, comentario).

    Devuelve (None, comentario) cuando NO hay que cambiar el estado — es el
    caso de una visita todavía pendiente sin movimiento, o cancelada en
    SimpliRoute (cancelar allá no significa que ILUS deba marcar devolución:
    eso es una decisión operativa, no automática).

    Nunca lanza: ante datos raros devuelve (None, "").
    """
    if not isinstance(visit, dict):
        return None, ""
    status = _s(visit.get("status")).lower()
    comentario_chofer = _s(visit.get("checkout_comment"))

    if status == SR_STATUS_COMPLETED:
        return "Entregado", comentario_chofer or "Entrega confirmada por el transportista"

    if status == SR_STATUS_FAILED:
        base = "No se pudo entregar"
        return "Entrega fallida", f"{base}: {comentario_chofer}" if comentario_chofer else base

    if status == SR_STATUS_PARTIAL:
        # Entregó parte de la carga. Se marca Entregado (sí hubo entrega) pero
        # el comentario deja constancia explícita de que fue parcial, para que
        # nadie lo lea como una entrega completa.
        base = "Entrega PARCIAL"
        return "Entregado", f"{base}: {comentario_chofer}" if comentario_chofer else base

    if status == SR_STATUS_CANCELED:
        # No se toca el estado: que el transportista cancele la visita en su
        # sistema no define el estado comercial del despacho en ILUS.
        return None, "La visita fue cancelada en el sistema del transportista"

    # pending → el único avance automático es "va en camino".
    if _s(visit.get("on_its_way")):
        return "En ruta", "El conductor va en camino al destino"

    return None, ""


def extract_pod(visit):
    """Extrae la prueba de entrega (POD) de una visita completada.

    Devuelve dict listo para transport_delivery_proof, o None si la visita no
    trae nada aprovechable. `signature` viene como URL en el GET de la visita;
    `pictures` como array de URLs absolutas (S3 firmadas).
    """
    if not isinstance(visit, dict):
        return None
    firma = _s(visit.get("signature"))
    fotos = [_s(u) for u in (visit.get("pictures") or []) if _s(u)]
    lat = _f(visit.get("checkout_latitude"))
    lng = _f(visit.get("checkout_longitude"))
    hora = _s(visit.get("checkout_time"))
    comentario = _s(visit.get("checkout_comment"))
    if not any((firma, fotos, lat, lng, hora)):
        return None
    return {
        # SimpliRoute no entrega el nombre de quien firmó; se usa el contacto
        # de la visita como referencia y, si no hay, un texto neutro.
        "receptor_nombre": _s(visit.get("contact_name"), 180) or "Recibido en destino",
        "firma_url": firma or None,
        "fotos": fotos,
        "lat": lat,
        "lng": lng,
        "entregado_at": hora or None,
        "notas": comentario or None,
    }


def extract_error_message(status_code, body):
    """Convierte un error de la API en un mensaje corto y accionable.

    El esquema de errores NO está documentado; al ser DRF se esperan dicts
    {campo: [mensajes]} en 400 y {"detail": "..."} en 401/403/404. Esta
    función es defensiva: nunca lanza, siempre devuelve un string.
    """
    if status_code == 401:
        return ("Token de SimpliRoute inválido o vencido (401). "
                "Revisa el token del courier en su perfil de SimpliRoute.")
    if status_code == 403:
        return "El token no tiene permisos para crear visitas (403)."
    if status_code == 404:
        return ("Endpoint no encontrado (404) — recuerda que las URLs de "
                "SimpliRoute llevan slash final.")

    partes = []
    if isinstance(body, dict):
        if "detail" in body:
            partes.append(_s(body["detail"]))
        for campo, val in body.items():
            if campo == "detail":
                continue
            if isinstance(val, (list, tuple)):
                partes.append(f"{campo}: {'; '.join(_s(x) for x in val)}")
            else:
                partes.append(f"{campo}: {_s(val)}")
    elif isinstance(body, list):
        for i, v in enumerate(body):
            if isinstance(v, dict) and v:
                sub = "; ".join(f"{k}: {v[k]}" for k in list(v)[:3])
                partes.append(f"visita {i + 1} → {sub}")
    elif body:
        partes.append(_s(body))

    msg = " | ".join(p for p in partes if p) or f"Error HTTP {status_code}"
    return msg[:280]

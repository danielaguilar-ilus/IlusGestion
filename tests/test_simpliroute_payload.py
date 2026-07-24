"""Tests del builder de payloads de SimpliRoute (módulo puro, sin red)."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from simpliroute_client import (  # noqa: E402
    build_visit_payload, build_visits_batch, parse_visits_response,
    extract_error_message, estado_ilus_from_visit, extract_pod,
)

ITEM_OK = {
    "item_id": 10, "tido": "FCV", "nudo": "8149",
    "cliente_nombre": "SOCIEDAD COMERCIAL BULLS GYM LIMITADA",
    "direccion": "Zapallar 664", "comuna": "Antofagasta",
    "region": "Región de Antofagasta",
    "telefono": "+56912345678", "email": "cliente@ejemplo.cl",
    "notas": "Entregar en bodega trasera",
    "n_bultos": 52, "peso_declarar": 62.1,
    "direccion_lat": -23.6509, "direccion_lng": -70.3975,
}


def test_campos_obligatorios_presentes():
    p, err = build_visit_payload(ITEM_OK, planned_date="2026-07-25")
    assert err == []
    for campo in ("title", "address", "planned_date"):
        assert p.get(campo), f"falta obligatorio {campo}"
    assert p["planned_date"] == "2026-07-25"


def test_title_igual_al_excel():
    """El Excel arma 'NUDO - CLIENTE' — la API debe hacer lo mismo."""
    p, _ = build_visit_payload(ITEM_OK, planned_date="2026-07-25")
    assert p["title"] == "8149 - SOCIEDAD COMERCIAL BULLS GYM LIMITADA"


def test_address_incluye_comuna_region_y_pais():
    p, _ = build_visit_payload(ITEM_OK, planned_date="2026-07-25")
    assert p["address"] == "Zapallar 664, Antofagasta, Región de Antofagasta, Chile"


def test_carga_bultos_y_peso():
    p, _ = build_visit_payload(ITEM_OK, planned_date="2026-07-25")
    assert p["load"] == 52          # bultos
    assert p["load_2"] == 62.1      # peso predominante


def test_coordenadas_cuando_existen():
    p, _ = build_visit_payload(ITEM_OK, planned_date="2026-07-25")
    assert p["latitude"] == -23.6509
    assert p["longitude"] == -70.3975


def test_sin_coordenadas_no_rompe():
    """Documentos viejos no tienen lat/lng: SimpliRoute geocodifica el address."""
    item = dict(ITEM_OK, direccion_lat=None, direccion_lng=None)
    p, err = build_visit_payload(item, planned_date="2026-07-25")
    assert err == []
    assert "latitude" not in p and "longitude" not in p


def test_duration_en_formato_hhmmss():
    """El Excel manda 3 (int minutos); la API exige HH:MM:SS."""
    p, _ = build_visit_payload(ITEM_OK, planned_date="2026-07-25")
    assert p["duration"] == "00:03:00"
    assert p["window_start"] == "09:00:00"
    assert p["window_end"] == "23:59:00"


def test_reference_usa_tido_y_nudo():
    p, _ = build_visit_payload(ITEM_OK, planned_date="2026-07-25")
    assert p["reference"] == "FCV-8149"


def test_falta_direccion_es_error():
    item = dict(ITEM_OK, direccion="")
    _, err = build_visit_payload(item, planned_date="2026-07-25")
    assert any("dirección" in e for e in err)


def test_falta_fecha_es_error():
    _, err = build_visit_payload(ITEM_OK, planned_date="")
    assert any("fecha" in e for e in err)


def test_bultos_invalido_cae_a_1():
    item = dict(ITEM_OK, n_bultos=None)
    p, _ = build_visit_payload(item, planned_date="2026-07-25")
    assert p["load"] == 1


def test_batch_separa_validos_de_incompletos():
    bueno = dict(ITEM_OK, item_id=1)
    malo = dict(ITEM_OK, item_id=2, direccion="")
    payloads, saltados = build_visits_batch([bueno, malo], planned_date="2026-07-25")
    assert [i for i, _ in payloads] == [1]
    assert [i for i, _ in saltados] == [2]


def test_parse_respuesta_array():
    data = [
        {"id": 111, "tracking_id": "SR111", "title": "a", "reference": "FCV-1"},
        {"id": 222, "tracking_id": "SR222", "title": "b", "reference": "FCV-2"},
    ]
    out = parse_visits_response(data)
    assert [v["id"] for v in out] == [111, 222]
    assert out[0]["tracking_id"] == "SR111"


def test_parse_respuesta_objeto_unico():
    out = parse_visits_response({"id": 5, "tracking_id": "SR5"})
    assert len(out) == 1 and out[0]["id"] == 5


def test_parse_respuesta_vacia_no_rompe():
    assert parse_visits_response(None) == []
    assert parse_visits_response("texto raro") == []


def test_error_401_mensaje_claro():
    msg = extract_error_message(401, {"detail": "Invalid token."})
    assert "inválido" in msg.lower() or "vencido" in msg.lower()


def test_error_400_por_campo():
    msg = extract_error_message(400, {"planned_date": ["This field is required."]})
    assert "planned_date" in msg


def test_error_nunca_lanza():
    for body in (None, "", [], {}, 123, [{"address": ["req"]}]):
        assert isinstance(extract_error_message(400, body), str)


# ── Tracking: traducción de estados ──────────────────────────────────────

def test_estado_completed_es_entregado():
    est, com = estado_ilus_from_visit({"status": "completed", "checkout_comment": "Recibido conforme"})
    assert est == "Entregado"
    assert "conforme" in com


def test_estado_failed_incluye_motivo():
    est, com = estado_ilus_from_visit({"status": "failed", "checkout_comment": "Cliente ausente"})
    assert est == "Entrega fallida"
    assert "Cliente ausente" in com


def test_estado_partial_avisa_que_es_parcial():
    """Parcial se marca Entregado pero el comentario NO puede ocultarlo."""
    est, com = estado_ilus_from_visit({"status": "partial", "checkout_comment": "Faltó 1 bulto"})
    assert est == "Entregado"
    assert "PARCIAL" in com


def test_estado_pending_con_on_its_way_es_en_ruta():
    est, _ = estado_ilus_from_visit({"status": "pending", "on_its_way": "2026-07-25T08:31:00Z"})
    assert est == "En ruta"


def test_estado_pending_sin_movimiento_no_cambia():
    est, _ = estado_ilus_from_visit({"status": "pending", "on_its_way": None})
    assert est is None


def test_estado_canceled_no_cambia_estado_ilus():
    """Cancelar en SimpliRoute no decide el estado comercial en ILUS."""
    est, com = estado_ilus_from_visit({"status": "canceled"})
    assert est is None
    assert com


def test_estado_nunca_lanza_con_basura():
    for v in (None, {}, "texto", 123, {"status": None}):
        est, com = estado_ilus_from_visit(v)
        assert est is None or isinstance(est, str)
        assert isinstance(com, str)


# ── Tracking: prueba de entrega ──────────────────────────────────────────

VISITA_ENTREGADA = {
    "status": "completed",
    "contact_name": "Bulls Gym",
    "checkout_time": "2026-07-25T14:40:45Z",
    "checkout_latitude": "-23.6509",
    "checkout_longitude": "-70.3975",
    "checkout_comment": "Recibido conforme",
    "signature": "https://s3.amazonaws.com/firma.png",
    "pictures": ["https://s3.amazonaws.com/f1.jpg", "https://s3.amazonaws.com/f2.jpg"],
}


def test_pod_extrae_todo():
    pod = extract_pod(VISITA_ENTREGADA)
    assert pod["firma_url"].endswith("firma.png")
    assert len(pod["fotos"]) == 2
    assert pod["lat"] == -23.6509 and pod["lng"] == -70.3975
    assert pod["entregado_at"].startswith("2026-07-25")
    assert pod["receptor_nombre"] == "Bulls Gym"


def test_pod_sin_datos_devuelve_none():
    assert extract_pod({"status": "pending"}) is None
    assert extract_pod(None) is None


def test_pod_receptor_por_defecto():
    pod = extract_pod({"checkout_time": "2026-07-25T14:40:45Z"})
    assert pod["receptor_nombre"] == "Recibido en destino"

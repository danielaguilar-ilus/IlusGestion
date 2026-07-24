"""Tests del builder de payloads de SimpliRoute (módulo puro, sin red)."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from simpliroute_client import (  # noqa: E402
    build_visit_payload, build_visits_batch, parse_visits_response,
    extract_error_message,
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

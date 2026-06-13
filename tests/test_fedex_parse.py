"""
Tests del parser de etiquetas FedEx (fedex_labels.parse_ship_response).

Cubre los formatos REALES que FedEx CL doméstico usa para un envío multi-bulto,
para garantizar que SIEMPRE extraemos las N etiquetas (1/3, 2/3, 3/3) y no
solo el bulto 1 (el bug que reportó Daniel).

Correr:  python3 tests/test_fedex_parse.py
"""
import io
import os
import sys
import base64

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import fedex_labels as fx


# ─── Utilidades de fixtures ──────────────────────────────────────────────────
def _make_multipage_pdf_b64(n_pages):
    """Genera un PDF real de n_pages páginas (base64). Simula el caso FedEx CL
    que mete las N etiquetas en UN solo PDF multipágina."""
    from PIL import Image
    pages = [Image.new("RGB", (400, 600), (255, 255, 255)) for _ in range(n_pages)]
    buf = io.BytesIO()
    pages[0].save(buf, format="PDF", save_all=True, append_images=pages[1:])
    return base64.b64encode(buf.getvalue()).decode("ascii")


def _make_single_pdf_b64():
    """PDF de 1 sola página (etiqueta master sola)."""
    return _make_multipage_pdf_b64(1)


PASSED = 0
FAILED = 0


def check(name, cond, detail=""):
    global PASSED, FAILED
    if cond:
        PASSED += 1
        print(f"  ✅ {name}")
    else:
        FAILED += 1
        print(f"  ❌ {name}  {detail}")


# ─── CASO 1: formato ideal — cada pieza con su packageDocuments ──────────────
def test_caso1_cada_pieza_con_label():
    print("\n[CASO 1] MPS ideal: 3 piezas, cada una con su etiqueta")
    data = {
        "output": {
            "transactionShipments": [{
                "masterTrackingNumber": "873034218595",
                "pieceResponses": [
                    {"trackingNumber": "873034218595",
                     "packageDocuments": [{"contentType": "LABEL", "encodedLabel": "LBL_1of3"}]},
                    {"trackingNumber": "873034218596",
                     "packageDocuments": [{"contentType": "LABEL", "encodedLabel": "LBL_2of3"}]},
                    {"trackingNumber": "873034218597",
                     "packageDocuments": [{"contentType": "LABEL", "encodedLabel": "LBL_3of3"}]},
                ],
            }]
        }
    }
    r = fx.parse_ship_response(data, label_format="PDF", expected_pieces=3)
    check("ok", r["ok"])
    check("master TN", r["master_tracking_number"] == "873034218595")
    check("3 trackings", len(r["piece_trackings"]) == 3, r["piece_trackings"])
    check("3 etiquetas", len(r["piece_labels"]) == 3, f"got {len(r['piece_labels'])}")
    labels = [p["label_b64"] for p in r["piece_labels"]]
    check("etiquetas distintas y correctas",
          labels == ["LBL_1of3", "LBL_2of3", "LBL_3of3"], labels)


# ─── CASO 2: el bug real — 3 piezas pero solo la 1ª trae etiqueta ────────────
def test_caso2_solo_primera_con_label():
    print("\n[CASO 2] FedEx CL parcial: 3 piezas, solo la 1ª con etiqueta")
    data = {
        "output": {
            "transactionShipments": [{
                "masterTrackingNumber": "873034218595",
                "pieceResponses": [
                    {"trackingNumber": "873034218595",
                     "packageDocuments": [{"encodedLabel": "LBL_MASTER"}]},
                    {"trackingNumber": "873034218596"},   # sin docs
                    {"trackingNumber": "873034218597"},   # sin docs
                ],
            }]
        }
    }
    r = fx.parse_ship_response(data, label_format="PDF", expected_pieces=3)
    # Acá NO podemos inventar etiquetas: el parser devuelve 3 trackings y 1
    # etiqueta. Lo importante: que REPORTE 3 trackings para que el caller sepa
    # que faltan 2 y dispare el fallback de OTs individuales.
    check("3 trackings detectados", len(r["piece_trackings"]) == 3, r["piece_trackings"])
    check("1 etiqueta (incompleto → caller hace fallback)",
          len(r["piece_labels"]) == 1, f"got {len(r['piece_labels'])}")
    check("debug refleja faltante",
          r["debug"]["n_piece_labels"] < r["debug"]["n_piece_trackings"])


# ─── CASO 3: PDF multipágina — 1 etiqueta que son 3 páginas ──────────────────
def test_caso3_pdf_multipagina():
    print("\n[CASO 3] FedEx CL: 3 piezas, UNA etiqueta = PDF de 3 páginas")
    pdf3 = _make_multipage_pdf_b64(3)
    data = {
        "output": {
            "transactionShipments": [{
                "masterTrackingNumber": "873034218595",
                "pieceResponses": [
                    {"trackingNumber": "873034218595",
                     "packageDocuments": [{"encodedLabel": pdf3}]},
                    {"trackingNumber": "873034218596"},
                    {"trackingNumber": "873034218597"},
                ],
            }]
        }
    }
    r = fx.parse_ship_response(data, label_format="PDF", expected_pieces=3)
    check("3 trackings", len(r["piece_trackings"]) == 3, r["piece_trackings"])
    check("3 etiquetas tras split del PDF multipágina",
          len(r["piece_labels"]) == 3, f"got {len(r['piece_labels'])}")
    # cada etiqueta debe ser un PDF de 1 página
    if len(r["piece_labels"]) == 3:
        ok_pages = all(
            fx.pdf_count_pages(base64.b64decode(p["label_b64"])) == 1
            for p in r["piece_labels"]
        )
        check("cada etiqueta es 1 página", ok_pages)


# ─── CASO 4: etiquetas a nivel txn (shipmentDocuments) ───────────────────────
def test_caso4_shipment_documents():
    print("\n[CASO 4] Etiquetas juntas en shipmentDocuments (una por pieza)")
    data = {
        "output": {
            "transactionShipments": [{
                "masterTrackingNumber": "111",
                "pieceResponses": [
                    {"trackingNumber": "111"},
                    {"trackingNumber": "112"},
                ],
                "shipmentDocuments": [
                    {"encodedLabel": "DOC_A"},
                    {"encodedLabel": "DOC_B"},
                ],
            }]
        }
    }
    r = fx.parse_ship_response(data, label_format="PDF", expected_pieces=2)
    check("2 etiquetas desde shipmentDocuments",
          len(r["piece_labels"]) == 2, f"got {len(r['piece_labels'])}")
    labels = [p["label_b64"] for p in r["piece_labels"]]
    check("contenido correcto", labels == ["DOC_A", "DOC_B"], labels)


# ─── CASO 5: mono-bulto (1 paquete, 1 etiqueta) ──────────────────────────────
def test_caso5_mono_bulto():
    print("\n[CASO 5] Mono-bulto: 1 paquete, 1 etiqueta")
    data = {
        "output": {
            "transactionShipments": [{
                "masterTrackingNumber": "999",
                "pieceResponses": [
                    {"trackingNumber": "999",
                     "packageDocuments": [{"encodedLabel": "SOLO_UNA"}]},
                ],
            }]
        }
    }
    r = fx.parse_ship_response(data, label_format="PDF", expected_pieces=1)
    check("1 tracking", len(r["piece_trackings"]) == 1)
    check("1 etiqueta", len(r["piece_labels"]) == 1)
    check("contenido", r["piece_labels"][0]["label_b64"] == "SOLO_UNA")


# ─── CASO 6: clave de etiqueta alternativa (docContent) ──────────────────────
def test_caso6_clave_alternativa():
    print("\n[CASO 6] FedEx usa 'docContent' en vez de 'encodedLabel'")
    data = {
        "output": {
            "transactionShipments": [{
                "masterTrackingNumber": "555",
                "pieceResponses": [
                    {"trackingNumber": "555",
                     "packageDocuments": [{"docContent": "VIA_DOCCONTENT"}]},
                ],
            }]
        }
    }
    r = fx.parse_ship_response(data, expected_pieces=1)
    check("extrae vía docContent",
          len(r["piece_labels"]) == 1 and r["piece_labels"][0]["label_b64"] == "VIA_DOCCONTENT")


# ─── CASO 7: respuesta vacía / sin transactionShipments ──────────────────────
def test_caso7_vacio():
    print("\n[CASO 7] Respuesta sin transactionShipments")
    r = fx.parse_ship_response({"output": {}}, expected_pieces=3)
    check("ok=False", r["ok"] is False)
    check("sin etiquetas", len(r["piece_labels"]) == 0)


if __name__ == "__main__":
    print("=" * 70)
    print("TESTS · Parser de etiquetas FedEx (fedex_labels.parse_ship_response)")
    print("=" * 70)
    test_caso1_cada_pieza_con_label()
    test_caso2_solo_primera_con_label()
    test_caso3_pdf_multipagina()
    test_caso4_shipment_documents()
    test_caso5_mono_bulto()
    test_caso6_clave_alternativa()
    test_caso7_vacio()
    print("\n" + "=" * 70)
    print(f"RESULTADO: {PASSED} passed · {FAILED} failed")
    print("=" * 70)
    sys.exit(1 if FAILED else 0)

"""
fedex_labels.py — Parser PURO de etiquetas FedEx (sin Flask, sin DB, sin red).

Aislado a propósito para poder TESTEARLO con JSONs de muestra reales
(ver tests/test_fedex_parse.py). app.py importa estas funciones y las usa
en _fedex_create_shipment / _fedex_item_labels / etc.

Única dependencia externa: pypdfium2 (wheel autocontenido con PDFium embebido,
NO requiere cryptography/cffi ni binarios de sistema). Si no está disponible,
las funciones de PDF degradan a [] / 0 sin romper.

Reglas del proyecto: este módulo NO modifica nada — solo lee y transforma.
"""
import io
import base64


# ─────────────────────────────────────────────────────────────────────────────
#  PDF helpers (contar / partir páginas) — usados para el caso "FedEx CL
#  entrega las N etiquetas de un multi-bulto dentro de UN solo PDF multipágina".
# ─────────────────────────────────────────────────────────────────────────────
def pdf_count_pages(raw_pdf_bytes):
    """Cuenta cuántas páginas tiene un PDF. Devuelve int (0 si no se pudo)."""
    if not raw_pdf_bytes:
        return 0
    # 1) pypdfium2 (autocontenido).
    try:
        import pypdfium2 as _pdfium
        doc = _pdfium.PdfDocument(raw_pdf_bytes)
        try:
            return len(doc)
        finally:
            try:
                doc.close()
            except Exception:
                pass
    except Exception:
        pass
    # 2) pypdf (fallback).
    try:
        from pypdf import PdfReader
        return len(PdfReader(io.BytesIO(raw_pdf_bytes)).pages)
    except Exception:
        return 0


def pdf_split_pages_b64(raw_pdf_bytes):
    """Divide un PDF multipágina en N PDFs de 1 página cada uno (base64).
    Devuelve list[str_b64] o [] si falla.

    Primario: pypdfium2 (robusto, sin dependencias de sistema). Fallback: pypdf.
    """
    if not raw_pdf_bytes:
        return []
    # 1) pypdfium2.
    try:
        import pypdfium2 as _pdfium
        src = _pdfium.PdfDocument(raw_pdf_bytes)
        try:
            out = []
            for i in range(len(src)):
                dst = _pdfium.PdfDocument.new()
                dst.import_pages(src, [i])
                b = io.BytesIO()
                dst.save(b)
                out.append(base64.b64encode(b.getvalue()).decode("ascii"))
                dst.close()
            return out
        finally:
            try:
                src.close()
            except Exception:
                pass
    except Exception:
        pass
    # 2) pypdf.
    try:
        from pypdf import PdfReader, PdfWriter
        reader = PdfReader(io.BytesIO(raw_pdf_bytes))
        out = []
        for page in reader.pages:
            w = PdfWriter()
            w.add_page(page)
            buf = io.BytesIO()
            w.write(buf)
            buf.seek(0)
            out.append(base64.b64encode(buf.getvalue()).decode("ascii"))
        return out
    except Exception:
        return []


# ─────────────────────────────────────────────────────────────────────────────
#  Parser de la respuesta de FedEx Ship API
# ─────────────────────────────────────────────────────────────────────────────
def label_from_doc(doc):
    """Saca el base64 de la etiqueta de un packageDocument, sea cual sea la
    clave que use FedEx (encodedLabel | encoded | docContent | content | label)."""
    if not isinstance(doc, dict):
        return ""
    return (doc.get("encodedLabel") or doc.get("encoded")
            or doc.get("docContent") or doc.get("content")
            or doc.get("label") or "")


def parse_ship_response(data, label_format="PDF", expected_pieces=0):
    """PARSER PURO de la respuesta de FedEx Ship API → etiquetas por bulto.

    Recibe el JSON ya parseado (`data`) y devuelve dónde están las etiquetas.
    Soporta los formatos que FedEx CL doméstico usa para multi-bulto:

      1. Cada pieceResponse trae SU packageDocuments con la etiqueta → ideal.
      2. Las etiquetas vienen juntas en transactionShipments[].shipmentDocuments
         (o packageDocuments a nivel txn), una por pieza.
      3. UNA sola etiqueta que es un PDF MULTIPÁGINA (1 página = 1 bulto) →
         se parte con pdf_split_pages_b64 y se asigna una a cada tracking.
      4. Última red: la master sola — el caller decide si dispara el fallback
         de OTs individuales.

    Args:
        data: dict — respuesta JSON de FedEx (con output.transactionShipments).
        label_format: "PDF" | "PNG" | "ZPLII" | "EPL2".
        expected_pieces: nº de bultos pedidos (de packages). Sirve para saber
            cuántas etiquetas faltan aunque FedEx mande menos trackings.

    Returns:
        dict {ok, master_tracking_number, piece_trackings, piece_labels,
              master_label_b64, debug, error?}
    """
    output = (data or {}).get("output", {}) or {}
    txns = output.get("transactionShipments", []) or []
    if not txns:
        return {"ok": False, "error": "FedEx no devolvió transactionShipments",
                "master_tracking_number": "", "piece_trackings": [],
                "piece_labels": [], "master_label_b64": "", "debug": {}}

    txn = txns[0]
    master_tn = txn.get("masterTrackingNumber") or txn.get("trackingNumber") or ""
    piece_resp = txn.get("pieceResponses", []) or []

    piece_trackings = []
    piece_labels = []
    master_label_b64 = ""

    # ── Formato 1: cada pieza con su packageDocuments ────────────────────────
    for pr in piece_resp:
        tn = pr.get("trackingNumber") or pr.get("masterTrackingNumber") or ""
        piece_trackings.append(tn)
        got = ""
        for doc in (pr.get("packageDocuments") or []):
            enc = label_from_doc(doc)
            if enc:
                got = enc
                break
        if got:
            piece_labels.append({"tracking_number": tn, "label_b64": got})
            if not master_label_b64:
                master_label_b64 = got

    # Si FedEx mandó menos trackings que bultos pedidos, completamos la lista
    # con el master (para que el conteo "faltan etiquetas" sea correcto).
    if expected_pieces and len(piece_trackings) < expected_pieces:
        while len(piece_trackings) < expected_pieces:
            piece_trackings.append(master_tn)

    # ── Formato 2: etiquetas juntas a nivel txn (shipmentDocuments) ──────────
    if len(piece_labels) < len(piece_trackings):
        ship_docs = txn.get("shipmentDocuments") or txn.get("packageDocuments") or []
        extra = [label_from_doc(d) for d in ship_docs]
        extra = [e for e in extra if e]
        if len(extra) > len(piece_labels):
            piece_labels = []
            for i, enc in enumerate(extra):
                tn_i = piece_trackings[i] if i < len(piece_trackings) else master_tn
                piece_labels.append({"tracking_number": tn_i, "label_b64": enc})
            master_label_b64 = extra[0]

    # ── Red mínima: al menos la master ───────────────────────────────────────
    if not master_label_b64:
        for doc in (txn.get("shipmentDocuments") or []):
            enc = label_from_doc(doc)
            if enc:
                master_label_b64 = enc
                piece_labels = [{"tracking_number": master_tn, "label_b64": enc}]
                break

    # ── Formato 3: UNA etiqueta PDF multipágina → partir en N ────────────────
    fmt_upper = (label_format or "PDF").upper()
    n_want = max(len(piece_trackings), expected_pieces or 0)
    if (fmt_upper == "PDF" and len(piece_labels) == 1 and n_want > 1
            and master_label_b64):
        try:
            raw_master = base64.b64decode(master_label_b64)
        except Exception:
            raw_master = b""
        n_pages = pdf_count_pages(raw_master) if raw_master else 0
        if n_pages >= n_want:
            split_b64s = pdf_split_pages_b64(raw_master)
            if len(split_b64s) >= n_want:
                while len(piece_trackings) < n_want:
                    piece_trackings.append(master_tn)
                piece_labels = []
                for i in range(n_want):
                    piece_labels.append({
                        "tracking_number": piece_trackings[i] if i < len(piece_trackings) else master_tn,
                        "label_b64":       split_b64s[i],
                    })

    debug = {
        "n_piece_responses": len(piece_resp),
        "n_piece_trackings": len(piece_trackings),
        "n_piece_labels":    len(piece_labels),
        "n_shipment_docs":   len(txn.get("shipmentDocuments") or []),
        "expected_pieces":   expected_pieces,
        "keys_txn":          sorted(list(txn.keys()))[:25],
        "keys_piece0":       sorted(list(piece_resp[0].keys()))[:25] if piece_resp else [],
    }
    return {
        "ok": True,
        "master_tracking_number": master_tn,
        "piece_trackings":        piece_trackings,
        "piece_labels":           piece_labels,
        "master_label_b64":       master_label_b64,
        "debug":                  debug,
    }

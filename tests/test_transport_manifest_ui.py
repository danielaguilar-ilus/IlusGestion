import json
import unittest
from html.parser import HTMLParser

from jinja2 import Environment, FileSystemLoader


class AttrDict(dict):
    __getattr__ = dict.get


class HtmlCapture(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.buttons = []
        self.script_sources = []

    def handle_starttag(self, tag, attrs):
        attributes = dict(attrs)
        if tag == "button":
            self.buttons.append(attributes)
        elif tag == "script" and attributes.get("src"):
            self.script_sources.append(attributes["src"])


def fake_url_for(endpoint, **values):
    if endpoint == "static":
        return f"/static/{values.get('filename', '')}?v=qa"
    return f"/{endpoint}"


class TransportManifestUiTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.environment = Environment(
            loader=FileSystemLoader("templates"),
            autoescape=True,
        )
        cls.environment.filters.update(
            {
                "fecha_es": lambda value, *args: str(value or ""),
                "chile_fmt": lambda value, *args: str(value or ""),
                "cloud_tx": lambda value, *args: str(value or ""),
            }
        )
        cls.environment.globals.update({"url_for": fake_url_for})

    def base_context(self):
        return {
            "current_user": AttrDict(role="admin", nombre="QA", foto_url=""),
            "embed_mode": True,
            "permissions": AttrDict(superadmin=True, transporte=True),
            "request": AttrDict(endpoint="tr_manifiesto_detalle"),
            "session": AttrDict(user_id=1),
            "has_logo": False,
            "is_tecnico": False,
            "last_sync": "",
            "mobile_module": "",
            "role_label": "Admin",
            "google_maps_api_key": "",
            "csrf_token": lambda: "csrf-qa",
            "get_flashed_messages": lambda **kwargs: [],
            "url_for": fake_url_for,
        }

    def test_tracking_buttons_carry_safe_json_and_read_only_policy(self):
        item = AttrDict(
            id=55,
            item_id=55,
            commitment_id=138762,
            tido="FCV",
            nudo="10644",
            doc="FCV 10644",
            cliente='Macarena "Prueba" O\'Hara',
            cliente_nombre='Macarena "Prueba" O\'Hara',
            telefono="+56982894105",
            email="qa@example.com",
            direccion="Avenida Siempre Viva 1234, Edificio Central",
            comuna="Las Condes",
            region="Region Metropolitana",
            cod_postal="7550000",
            clasificacion="despacho",
            autorizado_por="",
            motivo_envio="",
            n_bultos=3,
            ship_bultos=3,
            peso_export=25.5,
            peso_predominante=25.5,
            peso_real=25.5,
            peso_vol=20.0,
            volumen_m3=0.12,
            zz_envio=35000,
            costo_courier=20000,
            margen_clp=15000,
            margen_pct=42.8,
            estado_entrega="En ruta",
            es_garantia=False,
            es_perdida=False,
            tiene_saldo=True,
            sin_precio=False,
            master_tracking_number="",
            simpliroute_visit_id="visit-qa-001",
            ship_cancelled_at=None,
            ship_label_outdated=False,
            last_carrier_poll_at="",
            last_carrier_source="",
            productos=[
                AttrDict(
                    koprct="SKU-1",
                    nokopr="Producto prueba",
                    cantidad=1,
                    cant_despachada=1,
                    saldo=5,
                )
            ],
        )
        context = self.base_context()
        context.update(
            {
                "manifiesto": AttrDict(
                    id=11,
                    correlativo="MAN-2026-0010",
                    courier="Transporte Felca",
                    estado="En curso",
                    fecha="2026-07-26",
                    notas="",
                ),
                "items": [item],
                "logs": [],
                "estados_manifest": ["En preparacion", "En curso", "Cerrado"],
                "es_fedex": False,
                "n_con_ot": 0,
                "nlab": 0,
                "sin_ot": [],
            }
        )

        rendered = self.environment.get_template(
            "transporte/manifiesto_detalle.html"
        ).render(**context)
        capture = HtmlCapture()
        capture.feed(rendered)

        tracking_buttons = [
            button
            for button in capture.buttons
            if "js-open-simpliroute-actions"
            in (button.get("class") or "").split()
        ]
        self.assertEqual(2, len(tracking_buttons))
        for button in tracking_buttons:
            self.assertNotIn("onclick", button)
            payload = json.loads(button["data-sr-item"])
            self.assertEqual(138762, payload["id"])
            self.assertEqual(1, payload["edicion_bloqueada"])
            self.assertIn("courier", payload["edicion_motivo"].lower())

        label_buttons = [
            button
            for button in capture.buttons
            if "js-open-etiquetas-modal" in (button.get("class") or "").split()
        ]
        self.assertEqual(3, len(label_buttons))
        invoice_titles = [
            button.get("data-etiquetas-title", "")
            for button in label_buttons
            if "factura" in button.get("data-etiquetas-title", "").lower()
        ]
        self.assertEqual(2, len(invoice_titles))
        self.assertTrue(all("solo lectura" in title for title in invoice_titles))
        self.assertTrue(
            any(
                source.startswith("/static/transporte_manifiesto_acciones.js")
                for source in capture.script_sources
            )
        )
        self.assertNotIn('onclick="abrirSimpliRouteModal({', rendered)

    def test_transport_labels_render_square_and_locked(self):
        context = {
            "titulo": "Etiquetas QA",
            "csrf_token": lambda: "csrf-qa",
            "facturas": [
                AttrDict(
                    commitment_id=138762,
                    doc_tipo="FCV",
                    doc_numero="10644",
                    doc_full="FCV 10644",
                    cliente="Macarena Vergara",
                    telefono="+56982894105",
                    direccion="Colon 1265",
                    comuna="Las Condes",
                    region="Region Metropolitana",
                    total_bultos=3,
                    bultos=[
                        AttrDict(num=1, total=3),
                        AttrDict(num=2, total=3),
                        AttrDict(num=3, total=3),
                    ],
                    manifest_estado="En curso",
                    estado_entrega="En ruta",
                    bultos_editable=False,
                    bultos_edit_reason="Edicion bloqueada por gestion courier",
                )
            ],
            "bultos_editable": True,
            "courier": "Transporte Felca",
            "embed_mode": True,
            "fecha": "26-07-2026",
            "logo_url": "/static/logo.png",
            "logo_shs_url": "/static/logo-shs.png",
            "manifest_id": 11,
            "pdf_mode": False,
            "pdf_url": "/labels.pdf",
            "total_etiquetas": 3,
            "url_for": fake_url_for,
        }

        rendered = self.environment.get_template(
            "transporte/etiquetas.html"
        ).render(**context)

        self.assertIn("border-radius: 0;", rendered)
        self.assertIn("Edicion bloqueada por gestion courier", rendered)
        self.assertIn("Colon 1265", rendered)
        self.assertIn('grid-template-rows: 27mm minmax(25mm, 1fr);', rendered)
        self.assertIn('grid-column: 1 / -1;', rendered)
        self.assertIn('querySelectorAll(".js-fit-text").forEach(fitTextElement);', rendered)
        self.assertIn('document.fonts.ready.then(fitLabelDetails);', rendered)
        self.assertIn(
            'dataset.labelLayoutReady = layoutHasOverflow ? "0" : "1";',
            rendered,
        )
        self.assertIn('width: 96%;', rendered)
        self.assertIn('height: 8.5mm;', rendered)
        self.assertIn('format: "CODE128"', rendered)
        self.assertIn('margin: 12', rendered)
        self.assertIn('body.is-embedded .sheet { padding: 0; }', rendered)
        self.assertIn('width: 99.5mm;', rendered)
        self.assertIn('height: 149mm;', rendered)

    def test_manifest_operational_pdf_hides_financial_fields(self):
        item = AttrDict(
            commitment_id=1,
            tido="BLV",
            nudo="22703",
            cliente_nombre="Macarena Vergara",
            comuna="Independencia",
            n_bultos=3,
            peso_declarar=42.5,
            costo_courier=12000,
            zz_envio=25000,
            margen_clp=13000,
            es_perdida=False,
            estado_entrega="En preparacion",
            lineas=[AttrDict(sku="SKU-1", descripcion="Equipo fitness", cantidad=1)],
        )
        context = {
            "items": [item],
            "remitente": AttrDict(),
            "manifiesto": AttrDict(id=11, correlativo="MAN-2026-0010", courier="Transporte Felca"),
            "retiro": None,
            "fecha": "27-07-2026",
            "logo_url": "",
            "logo_shs_url": "",
            "pdf_url": "",
        }

        rendered = self.environment.get_template("transporte/manifiesto_firma.html").render(**context)

        self.assertIn("BLV", rendered)
        self.assertIn("Equipo fitness", rendered)
        self.assertNotIn("Costo courier", rendered)
        self.assertNotIn("25.000", rendered)
        self.assertNotIn("12.000", rendered)
        self.assertNotIn("Margen", rendered)

    def test_manifest_admin_pdf_contains_financial_fields(self):
        item = AttrDict(
            commitment_id=1,
            tido="BLV",
            nudo="22703",
            cliente_nombre="Macarena Vergara",
            comuna="Independencia",
            n_bultos=3,
            peso_declarar=42.5,
            costo_courier=12000,
            zz_envio=25000,
            margen_clp=13000,
            margen_pct=52,
            es_perdida=False,
            estado_entrega="En preparacion",
        )
        context = {
            "items": [item],
            "remitente": AttrDict(),
            "manifiesto": AttrDict(id=11, correlativo="MAN-2026-0010", courier="Transporte Felca"),
            "retiro": None,
            "fecha": "27-07-2026",
            "logo_url": "",
            "logo_shs_url": "",
        }

        rendered = self.environment.get_template("transporte/manifiesto_admin_email.html").render(**context)

        self.assertIn("RESPALDO ADMINISTRATIVO", rendered)
        self.assertIn("Costo courier", rendered)
        self.assertIn("Cobrado ZZ", rendered)
        self.assertIn("25.000", rendered)
        self.assertIn("12.000", rendered)

    def test_public_driver_signature_page_is_operational_only(self):
        item = AttrDict(
            commitment_id=1,
            tido="BLV",
            nudo="22703",
            cliente_nombre="Macarena Vergara",
            direccion="Colón 1265",
            comuna="Independencia",
            region="Región Metropolitana",
            telefono="+56982894105",
            n_bultos=3,
            peso_declarar=42.5,
            costo_courier=12000,
            zz_envio=25000,
            lineas=[AttrDict(sku="SKU-1", descripcion="Equipo fitness", cantidad=1)],
        )
        context = {
            "token": "token-prueba",
            "manifiesto": AttrDict(id=11, correlativo="MAN-2026-0010", courier="Transporte Felca"),
            "n_docs": 1,
            "n_bultos": 3,
            "items": [item],
            "courier_id": 1,
            "choferes": [],
            "retiro": None,
        }

        rendered = self.environment.get_template("transporte/firma_retiro_publico.html").render(**context)

        self.assertIn("Detalle a retirar", rendered)
        self.assertIn("Colón 1265", rendered)
        self.assertIn("Equipo fitness", rendered)
        self.assertIn("Firma real", rendered)
        self.assertNotIn("Costo courier", rendered)
        self.assertNotIn("Cobrado ZZ", rendered)
        self.assertNotIn("25.000", rendered)
        self.assertNotIn("12.000", rendered)


if __name__ == "__main__":
    unittest.main()

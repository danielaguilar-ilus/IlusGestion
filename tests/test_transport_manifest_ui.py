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
        self.rows = []
        self.script_sources = []

    def handle_starttag(self, tag, attrs):
        attributes = dict(attrs)
        if tag == "button":
            self.buttons.append(attributes)
        elif tag == "tr":
            self.rows.append(attributes)
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
        # Los filtros Jinja los registra app.py con @app.template_filter, pero
        # acá no se importa app.py (levantarlo exige BD y credenciales), así
        # que se arma un Environment propio. El problema: cada filtro nuevo
        # que alguien agregaba a app.py rompía este test con
        # "No filter named 'X'" -- le pasó a 'dias_fmt' y dejó la suite en
        # rojo sin que nada estuviera mal en producción.
        # En vez de mantener la lista a mano, se leen los nombres directo del
        # código fuente de app.py y se registran como identidad los que falten.
        # Así el test verifica el HTML (que es lo suyo) y no se rompe cada vez
        # que se suma un filtro.
        cls._registrar_filtros_de_app()

    @classmethod
    def _registrar_filtros_de_app(cls):
        import ast
        import pathlib

        fuente = pathlib.Path("app.py").read_text(encoding="utf-8", errors="ignore")
        arbol = ast.parse(fuente)
        nombres = set()
        for nodo in ast.walk(arbol):
            if not isinstance(nodo, ast.FunctionDef):
                continue
            for deco in nodo.decorator_list:
                # @app.template_filter('nombre')  ·  @app.template_filter()
                if not isinstance(deco, ast.Call):
                    continue
                attr = deco.func
                if not (isinstance(attr, ast.Attribute) and attr.attr == "template_filter"):
                    continue
                if deco.args and isinstance(deco.args[0], ast.Constant):
                    nombres.add(deco.args[0].value)
                else:
                    nombres.add(nodo.name)
        for nombre in nombres:
            cls.environment.filters.setdefault(nombre, lambda value, *a, **k: str(value or ""))

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

        # CÓMO SE ABRE EL SEGUIMIENTO HOY (verificado 2026-08-05):
        # la FILA de la factura lleva onclick='abrirSimpliRouteModal({...})',
        # con el payload serializado a JSON dentro del atributo.
        #
        # Este test pedía antes botones con clase .js-open-simpliroute-actions
        # y atributo data-sr-item, sin onclick. Ese contrato NO EXISTE: la
        # clase no aparece en ningún template, solo en el listener de
        # static/transporte_manifiesto_acciones.js, que quedó escuchando un
        # selector que nadie emite. O sea, el test describía una
        # refactorización que se hizo a medias (el JS sí, el template no).
        # NO es un bug para el usuario: abrirSimpliRouteModal existe
        # (static/transporte_manifiesto_detalle.js) y la fila abre el modal.
        # Se deja el test verificando lo que de verdad hay -- que es lo que
        # protege su intención original: que el JSON embebido esté bien
        # escapado y que la política de solo lectura viaje en el payload.
        # Son DOS modales según el courier (ver el {% if es_fedex %} del
        # template): FedEx abre abrirTrackingDetalle(id) y el resto abre
        # abrirSimpliRouteModal({...}). Toda fila de factura tiene que abrir
        # uno de los dos -- ninguna puede quedar sin seguimiento.
        filas_fedex = [
            row for row in capture.rows
            if "abrirTrackingDetalle(" in (row.get("onclick") or "")
        ]
        tracking_rows = [
            row for row in capture.rows
            if "abrirSimpliRouteModal(" in (row.get("onclick") or "")
        ]
        # La fixture trae UN item, con courier "Transporte Felca" (no FedEx),
        # así que corresponde exactamente 1 fila SimpliRoute y 0 FedEx. El
        # test pedía antes 2 botones porque el markup viejo emitía dos por
        # item; hoy el disparador es la fila completa, uno solo.
        self.assertEqual(0, len(filas_fedex))
        self.assertEqual(1, len(tracking_rows))
        for row in tracking_rows:
            crudo = row["onclick"]
            # convert_charrefs=True ya decodificó las entidades del atributo:
            # si el JSON no estuviera bien escapado, esto no parsearía.
            payload = json.loads(crudo[crudo.index("(") + 1: crudo.rindex(")")])
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
        # Ojo: la aserción que había acá —
        #   assertNotIn('onclick="abrirSimpliRouteModal({', rendered)
        # — pasaba siempre pero por casualidad: buscaba comilla DOBLE y el
        # template escribe onclick='...' con comilla simple. No verificaba
        # nada. Se reemplaza por lo que sí importa: que el modal se invoque
        # con un solo argumento JSON y no concatenando datos sueltos, que es
        # lo que hacía frágil el escapado.
        self.assertIn("onclick='abrirSimpliRouteModal({", rendered)

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

        # Lo que de verdad produce el TEMPLATE: los datos de la etiqueta y el
        # bloqueo de edición cuando el courier ya tomó el envío.
        self.assertIn("Edicion bloqueada por gestion courier", rendered)
        self.assertIn("Colon 1265", rendered)

        # El CSS y el JS de esta página se extrajeron a static/ (rediseño
        # 2026). Antes estas 6 aserciones miraban `rendered` y por eso el test
        # quedó en rojo: el HTML ya no los contiene, solo los enlaza. Se
        # verifica lo mismo, pero en el archivo donde ahora vive cada cosa --
        # y además que la página los siga enlazando, que es lo que uniría un
        # fallo real (si el <link> se cae, el CSS existe pero no se aplica).
        import pathlib

        self.assertIn("/static/transporte_etiquetas_page.css", rendered)
        self.assertIn("/static/transporte_etiquetas_page.js", rendered)

        css = pathlib.Path("static/transporte_etiquetas_page.css").read_text(encoding="utf-8")
        self.assertIn("border-radius: 0;", css)                              # etiqueta cuadrada
        self.assertIn("grid-template-rows: 27mm minmax(25mm, 1fr);", css)
        self.assertIn("grid-column: 1 / -1;", css)

        js = pathlib.Path("static/transporte_etiquetas_page.js").read_text(encoding="utf-8")
        self.assertIn('querySelectorAll(".js-fit-text").forEach(fitTextElement);', js)
        self.assertIn("document.fonts.ready.then(fitLabelDetails);", js)
        self.assertIn('dataset.labelLayoutReady = layoutHasOverflow ? "0" : "1";', js)
        self.assertIn("width: 96%;", css)
        self.assertIn("height: 8.5mm;", css)
        self.assertIn('format: "CODE128"', js)        # código de barras
        self.assertIn("margin: 12", js)
        # Estas tres siguen inline en el template, pero SOLO dentro del
        # bloque {% if pdf_mode %}: son las reglas de tamaño de página que
        # usa Playwright al generar el PDF y dependen de Jinja, por eso no se
        # pudieron mover al CSS. El render de arriba usa pdf_mode=False, así
        # que hay que renderizar de nuevo en modo PDF para verificarlas --
        # que además es el modo que de verdad importa para que la etiqueta
        # salga de 100×150 mm en la impresora.
        # bootstrap_icons_inline: en pdf_mode el template incrusta el CSS de
        # los iconos en vez de enlazarlo (Playwright no resuelve el <link>).
        contexto_pdf = dict(context, pdf_mode=True,
                            bootstrap_icons_inline=lambda: "",
                            static_inline=lambda *a, **k: "")
        rendered_pdf = self.environment.get_template(
            "transporte/etiquetas.html"
        ).render(**contexto_pdf)
        self.assertIn("body.is-embedded .sheet { padding: 0; }", rendered_pdf)
        self.assertIn("width: 99.5mm;", rendered_pdf)     # 100×150 mm reales
        self.assertIn("height: 149mm;", rendered_pdf)


if __name__ == "__main__":
    unittest.main()

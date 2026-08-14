"""Rediseño integral de /seguimiento (2026-08-13, misma noche que el
rediseño visual). Daniel trajo una auditoría de ChatGPT sobre el código
real del módulo de seguimiento público y confirmó, en el mismo mensaje:
"no quiero que solicites el RUT... el seguimiento debe ser amigable,
asócialo a nuestro diseño de retiros... sorpréndeme".

Cubre 4 cambios reales, no solo visuales:
  1. Código corto de seguimiento (tracking_code) reemplaza al RUT como
     forma de identificarse en /seguimiento.
  2. _tracking_payload ya no toma solo el ÚLTIMO manifest_item -- agrega
     TODOS los items del commitment y elige el representativo de forma
     conservadora (alerta primero, si no el menos avanzado).
  3. Productos reales del pedido en el payload público (antes solo bultos).
  4. GPS del chofer difuminado antes de salir al navegador.

Correr con:  py -m unittest tests.test_seguimiento_sin_rut -v
"""
import ast
import os
import sys
import unittest

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
APP_SRC = open(os.path.join(BASE_DIR, "app.py"), encoding="utf-8", errors="ignore").read()
_ARBOL = ast.parse(APP_SRC)

sys.path.insert(0, BASE_DIR)


def _fn_source(nombre):
    for n in ast.walk(_ARBOL):
        if isinstance(n, ast.FunctionDef) and n.name == nombre:
            return ast.unparse(n)
    raise AssertionError(f"no se encontró la función {nombre} en app.py")


def _fn_exec(nombre, ns=None):
    """Extrae y ejecuta una función PURA (sin DB/Flask) aislada del resto
    de app.py, para poder llamarla de verdad en el test."""
    src = _fn_source(nombre)
    ns = ns if ns is not None else {}
    exec(src, ns)
    return ns[nombre]


class TestTrackingCodeGeneracion(unittest.TestCase):
    """_tr_generar_tracking_code / _tr_normalizar_tracking_code son
    funciones puras -- se prueban ejecutándolas de verdad, no solo
    inspeccionando el código fuente."""

    def test_formato_ilus_guion_4_guion_4(self):
        import re
        import secrets as _secrets
        gen = _fn_exec("_tr_generar_tracking_code", {
            "secrets": _secrets,
            "_TR_TRACKING_CODE_ALPHABET": "23456789ABCDEFGHJKMNPQRSTUVWXYZ",
        })
        for _ in range(50):
            code = gen()
            self.assertRegex(code, r"^ILUS-[23456789ABCDEFGHJKMNPQRSTUVWXYZ]{4}-[23456789ABCDEFGHJKMNPQRSTUVWXYZ]{4}$")

    def test_alfabeto_excluye_caracteres_ambiguos(self):
        import secrets as _secrets
        gen = _fn_exec("_tr_generar_tracking_code", {
            "secrets": _secrets,
            "_TR_TRACKING_CODE_ALPHABET": "23456789ABCDEFGHJKMNPQRSTUVWXYZ",
        })
        ambiguos = set("01ILO")
        for _ in range(50):
            code = gen()
            chars = set(code.replace("ILUS-", "").replace("-", ""))
            self.assertFalse(chars & ambiguos,
                              f"código {code!r} contiene un carácter ambiguo")

    def test_normalizar_quita_prefijo_guiones_espacios(self):
        import re
        norm = _fn_exec("_tr_normalizar_tracking_code", {"re": re})
        self.assertEqual(norm("ILUS-7K9M-42QF"), "7K9M42QF")
        self.assertEqual(norm("  ilus 7k9m 42qf  "), "7K9M42QF")
        self.assertEqual(norm("7K9M42QF"), "7K9M42QF")

    def test_normalizar_vacio_no_revienta(self):
        import re
        norm = _fn_exec("_tr_normalizar_tracking_code", {"re": re})
        self.assertEqual(norm(""), "")
        self.assertEqual(norm(None), "")


class TestNoQuedaRutEnElFlujoPrincipal(unittest.TestCase):
    """Daniel: 'no quiero que solicites el RUT'. Verificable en el HTML
    real que ve el cliente, no solo en el código fuente."""

    def _render(self, template, **ctx):
        from flask import Flask, render_template
        app = Flask(__name__, template_folder=os.path.join(BASE_DIR, "templates"),
                    static_folder=os.path.join(BASE_DIR, "static"))

        @app.route("/seguimiento/buscar", methods=["POST"])
        def seguimiento_buscar():
            return "stub"

        @app.route("/seguimiento/codigo", methods=["POST"])
        def seguimiento_por_codigo():
            return "stub"

        @app.route("/seguimiento/recuperar", methods=["GET"])
        def seguimiento_recuperar_page():
            return "stub"

        @app.route("/seguimiento/recuperar", methods=["POST"])
        def seguimiento_recuperar_enviar():
            return "stub"

        @app.route("/seguimiento")
        def seguimiento_lookup_page():
            return "stub"

        with app.test_request_context():
            return render_template(template, **ctx)

    def test_seguimiento_lookup_no_pide_rut(self):
        html = self._render("transporte/seguimiento_lookup.html", error=None, prefill=None)
        self.assertNotIn('name="customer_rut"', html)
        self.assertNotIn("Tu RUT", html)

    def test_seguimiento_lookup_pide_codigo(self):
        html = self._render("transporte/seguimiento_lookup.html", error=None, prefill=None)
        self.assertIn('name="tracking_code"', html)
        self.assertIn("ILUS-XXXX-XXXX", html)

    def test_seguimiento_lookup_apunta_a_ruta_por_codigo(self):
        html = self._render("transporte/seguimiento_lookup.html", error=None, prefill=None)
        self.assertIn('action="/seguimiento/codigo"', html)

    def test_link_no_encuentro_mi_codigo_presente(self):
        html = self._render("transporte/seguimiento_lookup.html", error=None, prefill=None)
        self.assertIn("No encuentro mi código", html)
        self.assertIn('href="/seguimiento/recuperar"', html)

    def test_recuperar_estado_inicial_pide_email_no_rut(self):
        html = self._render("transporte/seguimiento_recuperar.html", enviado=False)
        self.assertIn('name="email"', html)
        self.assertNotIn("RUT", html)

    def test_recuperar_estado_enviado_es_generico(self):
        html = self._render("transporte/seguimiento_recuperar.html", enviado=True)
        self.assertIn("Revisa tu correo", html)
        # No debe confirmar ni negar si existe un pedido -- anti-enumeración.
        self.assertNotIn("no encontramos", html.lower())
        self.assertNotIn("no existe", html.lower())


class TestBackendSeguimientoSinRutIntacto(unittest.TestCase):
    """El flujo viejo (doc+RUT) NO se borra -- REGLA #4.2 -- queda como
    respaldo. Se verifica en el código fuente porque no depende de datos."""

    def test_seguimiento_buscar_sigue_existiendo(self):
        src = _fn_source("seguimiento_buscar")
        self.assertIn("customer_rut", src)
        self.assertIn("normalizar_rut", src)

    def test_seguimiento_por_codigo_no_usa_rut(self):
        # No se busca el string "rut" a secas -- el propio docstring dice
        # "(sin RUT)", que ya contiene esa subcadena. Se verifica que no
        # haya código real de RUT (el campo del form, o la función que ya
        # usa el flujo viejo para validarlo).
        src = _fn_source("seguimiento_por_codigo")
        self.assertNotIn("customer_rut", src)
        self.assertNotIn("normalizar_rut", src)

    def test_seguimiento_recuperar_enviar_es_generico(self):
        src = _fn_source("seguimiento_recuperar_enviar")
        # La misma plantilla/respuesta se devuelve exista o no el pedido --
        # no debe haber una rama que revele "no encontramos nada" al cliente.
        self.assertEqual(src.count("seguimiento_recuperar.html"), 1)


class TestPayloadAgregaTodosLosItems(unittest.TestCase):
    """_tracking_payload ya no debe traer solo el último manifest_item."""

    def test_query_de_items_no_tiene_limit_1(self):
        src = _fn_source("_tracking_payload")
        # La query de items_mi debe traer TODOS los items (sin LIMIT), a
        # diferencia de antes (ORDER BY tmi.id DESC LIMIT 1).
        idx_items = src.index("items_mi = mysql_fetchall")
        bloque = src[idx_items:idx_items + 400]
        self.assertNotIn("LIMIT 1", bloque)

    def test_prioridad_alerta_gana_sobre_avance(self):
        # ast.unparse normaliza comillas a simples y colapsa espacios --
        # se comparan las formas ya normalizadas, no el código fuente tal
        # cual se escribió.
        src = _fn_source("_tracking_payload")
        self.assertIn("'Problema', 'Entrega fallida', 'Devolución'", src)
        self.assertIn("return (0, 0)", src)

    def test_multi_envio_total_en_el_payload(self):
        src = _fn_source("_tracking_payload")
        self.assertIn("'multi_envio_total': multi_envio_total", src)

    def test_productos_en_el_payload_excluye_zz(self):
        src = _fn_source("_tracking_payload")
        self.assertIn("transport_commitment_lines", src)
        self.assertIn("startswith('ZZ')", src)
        self.assertIn("'productos': productos", src)


class TestGpsDifuminado(unittest.TestCase):
    def test_lat_lng_se_redondean_y_agregan_ruido(self):
        src = _fn_source("_tracking_driver_live")
        self.assertIn("_rnd.uniform", src)
        self.assertIn("round(", src)
        # La distancia/ETA deben calcularse ANTES del difuminado (con las
        # coordenadas reales), no después.
        idx_dist = src.index("dist_m = _haversine_m")
        idx_fuzz = src.index("lat_fuzz")
        self.assertLess(idx_dist, idx_fuzz)


class TestRenderRealMultiEnvioYProductos(unittest.TestCase):
    """public_tracking.html con datos sintéticos que imitan lo que ahora
    produce _tracking_payload."""

    def _chile_fmt(self, dt, fmt="%d/%m/%Y %H:%M"):
        return dt or ""

    def _render(self, gmaps_key="", **data_overrides):
        from flask import Flask, render_template
        app = Flask(__name__, template_folder=os.path.join(BASE_DIR, "templates"),
                    static_folder=os.path.join(BASE_DIR, "static"))
        app.jinja_env.filters["chile_fmt"] = self._chile_fmt
        app.jinja_env.filters["rut_fmt"] = lambda v: v

        @app.route("/t/<token>/status")
        def tr_public_tracking_status(token):
            return "stub"

        @app.route("/t/<token>")
        def tr_public_tracking(token):
            return "stub"

        data = {
            "ok": True, "doc": "FCV-11290", "cliente": "Cliente de prueba",
            "destino": "Las Condes", "region": "Región Metropolitana",
            "courier": "Transporte Felca", "courier_logo": "", "manifiesto": "MAN-2026-0042",
            "estado": "En ruta", "estado_color": "primary", "estado_icon": "bi-truck",
            "estado_step": 3, "eventos": [], "proof": None, "entregado_at": None,
            "bultos": 6, "fecha_emision": "2026-08-10", "fecha_entrega": "2026-08-13",
            "driver_live": None, "multi_envio_total": 0, "productos": [],
        }
        data.update(data_overrides)
        with app.test_request_context():
            return render_template("transporte/public_tracking.html",
                                    token="x" * 40, data=data, gmaps_key=gmaps_key)

    def test_sin_multi_envio_no_aparece_el_aviso(self):
        html = self._render(multi_envio_total=0)
        self.assertNotIn("se despacha en", html)

    def test_con_multi_envio_aparece_el_aviso_con_el_numero_real(self):
        html = self._render(multi_envio_total=3)
        self.assertIn("se despacha en 3 partes", html)

    def test_sin_productos_no_aparece_la_tarjeta(self):
        html = self._render(productos=[])
        self.assertNotIn("Qué viene en tu pedido", html)

    def test_con_productos_aparece_la_lista(self):
        html = self._render(productos=[
            {"sku": "TROT001", "nombre": "Trotadora Curva ILUS 6380", "cantidad": 1},
            {"sku": "BICI002", "nombre": "Bicicleta Spinning ILUS S10", "cantidad": 2},
        ])
        self.assertIn("Qué viene en tu pedido", html)
        self.assertIn("Trotadora Curva ILUS 6380", html)
        self.assertIn("×2", html)

    def test_mas_de_6_productos_recorta_y_avisa(self):
        productos = [{"sku": f"SKU{i}", "nombre": f"Producto {i}", "cantidad": 1} for i in range(9)]
        html = self._render(productos=productos)
        self.assertIn("Producto 5", html)   # el sexto (índice 5) sí entra
        self.assertNotIn("Producto 6", html)  # el séptimo ya no
        self.assertIn("+ 3 productos más", html)

    def test_hooks_criticos_de_js_siguen_intactos(self):
        html = self._render()
        for hook in ("id=\"tkState\"", "id=\"tkStateLabel\"", "id=\"tkSteps\"",
                     "id=\"tkTimeline\""):
            self.assertIn(hook, html)

    def test_live_map_sigue_intacto_cuando_hay_driver_live(self):
        # #liveMap solo se pinta cuando data.driver_live existe Y hay
        # gmaps_key (si no, cae al fallback de "distancia en km") -- con el
        # payload default no debe aparecer; con ambos datos reales, sí.
        html_sin = self._render(driver_live=None)
        self.assertNotIn('id="liveMap"', html_sin)
        html_con = self._render(gmaps_key="fake-key-para-test", driver_live={
            "lat": -33.45, "lng": -70.66, "dest_lat": -33.41, "dest_lng": -70.57,
            "age_s": 45, "dist_m": 4200, "eta_min": 12,
        })
        self.assertIn('id="liveMap"', html_con)


if __name__ == "__main__":
    unittest.main()

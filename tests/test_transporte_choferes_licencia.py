"""Choferes internos por courier: licencia, seguro de carga y contrato
(2026-08-06, pedido explícito de Daniel: "necesito que en transportes
elimines el menú de choferes y dejar un menú potente de los couriers con
sus choferes internos... seguro de carga, contrato, licencia de conducir
con filtro inteligente... clase A o B para camiones pequeños").

QUÉ SE PRUEBA
-------------
1. Las clases de licencia de conducir chilenas son las REALES (Ley de
   Tránsito N°18.290 / ChileAtiende) -- no categorías inventadas.
2. El filtro inteligente "camión pequeño" (B/A4) clasifica bien, incluso
   con licencias que traen varias clases separadas por coma/espacio/slash.
3. La clasificación de vencimiento (vigente/por_vencer/vencido) de
   licencia, seguro de carga y contrato -- la parte que hace que la
   pantalla sea "potente" y no una lista más (Regla #2: ámbar por vencer,
   rojo vencido).
4. Guardas de regresión sobre el código fuente:
   - las 8 columnas nuevas se agregan de forma idempotente en
     _ensure_transport_drivers_table (funciona con ILUS_SKIP_MIGRATIONS=1).
   - el sidebar (templates/base.html) ya NO linkea a tr_choferes, pero el
     endpoint sigue vivo en app.py (no se borró el backend).
   - la app móvil del chofer (login por PIN, captura, ruta, entrega) sigue
     intacta -- no se tocó una sola línea de esas funciones.
   - los endpoints que usa el selector de "chofer guardado" en captura de
     retiro (GET/POST /transporte/couriers/<cid>/choferes) siguen
     existiendo con la misma forma.

No usa pytest (no está instalado en este entorno) -- unittest estándar,
mismo patrón que el resto de tests/test_*.py: extraer código de app.py vía
AST en vez de importar el módulo completo (evita arrastrar Flask/MySQL).

Correr:  py -m unittest tests.test_transporte_choferes_licencia -v
"""
import ast
import pathlib
import unittest
from datetime import date, datetime, timedelta

RAIZ = pathlib.Path(__file__).resolve().parent.parent
APP_PY = RAIZ / "app.py"
BASE_HTML = RAIZ / "templates" / "base.html"

_SRC = None
_TREE = None


def _fuente_y_arbol():
    global _SRC, _TREE
    if _TREE is None:
        _SRC = APP_PY.read_text(encoding="utf-8")
        _TREE = ast.parse(_SRC)
    return _SRC, _TREE


def _fuente_funcion(nombre):
    src, arbol = _fuente_y_arbol()
    for nodo in arbol.body:
        if isinstance(nodo, ast.FunctionDef) and nodo.name == nombre:
            return ast.get_source_segment(src, nodo) or ""
    raise AssertionError(f"No se encontró {nombre}() en app.py")


_NS_CACHE = None


def cargar_namespace():
    """Ejecuta LICENCIA_CLASES_CHILE / LICENCIA_CAMION_PEQUENO / las 3
    funciones puras de licencia+vencimiento, con _now_chile() stubeado a
    una fecha fija (2026-08-06) para que los tests sean deterministas sin
    arrastrar zoneinfo/DB."""
    global _NS_CACHE
    if _NS_CACHE is not None:
        return _NS_CACHE
    src, arbol = _fuente_y_arbol()
    wanted_funcs = {
        "_tr_licencia_clases_normalizadas",
        "_tr_licencia_apta_camion_pequeno",
        "_tr_estado_vencimiento",
    }
    wanted_assigns = {"LICENCIA_CLASES_CHILE", "LICENCIA_CAMION_PEQUENO"}
    cuerpo = []
    for nodo in arbol.body:
        if isinstance(nodo, ast.FunctionDef) and nodo.name in wanted_funcs:
            cuerpo.append(nodo)
        elif isinstance(nodo, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id in wanted_assigns for t in nodo.targets
        ):
            cuerpo.append(nodo)
    faltan_f = wanted_funcs - {n.name for n in cuerpo if isinstance(n, ast.FunctionDef)}
    faltan_a = wanted_assigns - {
        t.id for n in cuerpo if isinstance(n, ast.Assign) for t in n.targets
    }
    if faltan_f or faltan_a:
        raise AssertionError(f"Faltan en app.py: funcs={faltan_f} consts={faltan_a}")
    ns = {
        "re": __import__("re"),
        "datetime": datetime,
        "_now_chile": lambda: datetime(2026, 8, 6, 12, 0, 0),
    }
    exec(compile(ast.Module(body=cuerpo, type_ignores=[]), "app.py", "exec"), ns)
    _NS_CACHE = ns
    return ns


class ClasesDeLicenciaChilenasTest(unittest.TestCase):
    """No son categorías inventadas: Ley de Tránsito N°18.290, ChileAtiende
    (fichas 20592 y 24034, consultado 2026-08-06)."""

    @classmethod
    def setUpClass(cls):
        cls.ns = cargar_namespace()
        cls.clases = cls.ns["LICENCIA_CLASES_CHILE"]

    def test_tiene_las_5_subclases_profesionales_de_carga_y_pasajeros(self):
        for c in ("A1", "A2", "A3", "A4", "A5"):
            self.assertIn(c, self.clases)

    def test_tiene_las_clases_no_profesionales_y_especiales(self):
        for c in ("B", "C", "D", "E", "F"):
            self.assertIn(c, self.clases)

    def test_no_inventa_clases_que_no_existen(self):
        # Ninguna clase G, Z, A6, etc.
        self.assertEqual(len(self.clases), 10)

    def test_a4_y_a5_son_las_de_carga(self):
        self.assertIn("carga", self.clases["A4"].lower())
        self.assertIn("carga", self.clases["A5"].lower())

    def test_b_es_particular_no_profesional(self):
        self.assertIn("particular", self.clases["B"].lower())


class FiltroCamionPequenoTest(unittest.TestCase):
    """Daniel, 2026-08-05: "filtro inteligente que debería ser tipo A o B
    para camiones pequeños". B (particular ≤3.500kg) + A4 (camión simple,
    la subclase de carga MÁS chica) -- A5 se deja fuera del preset porque
    también habilita articulados de mayor tonelaje."""

    @classmethod
    def setUpClass(cls):
        cls.ns = cargar_namespace()
        cls.apta = staticmethod(cls.ns["_tr_licencia_apta_camion_pequeno"])
        cls.normaliza = staticmethod(cls.ns["_tr_licencia_clases_normalizadas"])

    def test_preset_es_exactamente_b_y_a4(self):
        self.assertEqual(self.ns["LICENCIA_CAMION_PEQUENO"], {"B", "A4"})

    def test_clase_b_es_apta(self):
        self.assertTrue(self.apta("B"))

    def test_clase_a4_es_apta(self):
        self.assertTrue(self.apta("A4"))

    def test_clase_a5_no_entra_en_el_preset_pequeno(self):
        self.assertFalse(self.apta("A5"))

    def test_clase_a1_taxi_no_es_apta(self):
        self.assertFalse(self.apta("A1"))

    def test_licencia_vacia_no_es_apta(self):
        self.assertFalse(self.apta(""))
        self.assertFalse(self.apta(None))

    def test_licencia_con_varias_clases_separadas_por_coma(self):
        self.assertTrue(self.apta("B,A4"))
        self.assertEqual(self.normaliza("B,A4"), ["B", "A4"])

    def test_licencia_con_varias_clases_separadas_por_espacio_o_slash(self):
        self.assertEqual(self.normaliza("B A4"), ["B", "A4"])
        self.assertEqual(self.normaliza("B/A4"), ["B", "A4"])

    def test_normaliza_a_mayusculas(self):
        self.assertEqual(self.normaliza("b, a4"), ["B", "A4"])

    def test_ignora_tokens_que_no_son_clase_real(self):
        # Typo (ej. "A9") no revienta, simplemente no cuenta.
        self.assertEqual(self.normaliza("B, A9"), ["B"])

    def test_chofer_con_a5_y_b_es_apta_por_la_b(self):
        self.assertTrue(self.apta("A5,B"))

    def test_chofer_solo_con_a5_no_es_apto(self):
        self.assertFalse(self.apta("A5"))


class EstadoVencimientoTest(unittest.TestCase):
    """Licencia, seguro de carga y contrato comparten la misma regla: rojo
    vencido, ámbar a <=30 días, verde si falta más. "Hoy" fijo en 2026-08-06
    (stub de _now_chile) para que el test no dependa del reloj real."""

    @classmethod
    def setUpClass(cls):
        cls.ns = cargar_namespace()
        cls.estado = staticmethod(cls.ns["_tr_estado_vencimiento"])
        cls.hoy = date(2026, 8, 6)

    def test_sin_fecha_es_none(self):
        self.assertIsNone(self.estado(None))

    def test_fecha_pasada_es_vencido(self):
        self.assertEqual(self.estado(self.hoy - timedelta(days=1)), "vencido")

    def test_fecha_de_hoy_no_esta_vencida(self):
        # Vence HOY -> todavía "por vencer" (0 días), no "vencido".
        self.assertEqual(self.estado(self.hoy), "por_vencer")

    def test_dentro_de_30_dias_es_por_vencer(self):
        self.assertEqual(self.estado(self.hoy + timedelta(days=30)), "por_vencer")

    def test_31_dias_ya_es_vigente(self):
        self.assertEqual(self.estado(self.hoy + timedelta(days=31)), "vigente")

    def test_muy_lejos_en_el_futuro_es_vigente(self):
        self.assertEqual(self.estado(self.hoy + timedelta(days=400)), "vigente")

    def test_acepta_datetime_no_solo_date(self):
        # Por si algún día la columna viaja con hora.
        self.assertEqual(
            self.estado(datetime(2026, 7, 1, 8, 30)), "vencido")

    def test_umbral_de_alerta_configurable(self):
        # Contrato con umbral custom de 60 días (como usa el resto de
        # Transporte para "por vencer" en otras alertas).
        self.assertEqual(
            self.estado(self.hoy + timedelta(days=45), dias_alerta=60), "por_vencer")


class ColumnasNuevasIdempotentesTest(unittest.TestCase):
    """_ensure_transport_drivers_table debe seguir el patrón ALTER-si-no-
    existe (funciona con ILUS_SKIP_MIGRATIONS=1 en prod, Regla del proyecto:
    "Columnas nuevas que SOBREVIVAN al skip-migrations")."""

    @classmethod
    def setUpClass(cls):
        cls.src = _fuente_funcion("_ensure_transport_drivers_table")

    def test_las_7_columnas_nuevas_estan_en_la_funcion(self):
        for col in (
            "licencia_clase", "licencia_vencimiento", "licencia_doc_url",
            "seguro_carga_vigente", "seguro_carga_vencimiento", "seguro_carga_doc_url",
            "foto_url",
        ):
            with self.subTest(col=col):
                self.assertIn(f'"{col}"', self.src)

    def test_no_hay_columna_de_contrato_por_chofer(self):
        # Corrección de Daniel el mismo día: el contrato de prestación de
        # servicio es del COURIER (transport_courier_contratos), no de cada
        # chofer -- no debe reaparecer como columna de transport_drivers.
        self.assertNotIn('"contrato_doc_url"', self.src)
        self.assertNotIn('"contrato_vencimiento"', self.src)

    def test_cada_columna_se_agrega_solo_si_no_existe(self):
        self.assertIn("if _col not in existing:", self.src)
        self.assertIn("ALTER TABLE transport_drivers ADD COLUMN {_col}", self.src)

    def test_una_columna_que_falla_no_tumba_a_las_demas(self):
        # Un ALTER individual con try/except -- no todo el bloque en un
        # único try que se corta en la primera columna que falle.
        self.assertIn("except Exception as _col_err:", self.src)


class FichaTemplateNormalizaSeparadoresTest(unittest.TestCase):
    """El chip de filtro y las pastillas de clase en la tabla deben aceptar
    licencias con clases separadas por coma, espacio O slash (mismo criterio
    que _tr_licencia_clases_normalizadas del backend) -- un bug real: la
    primera versión solo BORRABA los espacios en vez de convertirlos en
    separador, lo que fusionaba 'B A4' en el token inválido 'BA4'."""

    @classmethod
    def setUpClass(cls):
        cls.html = (RAIZ / "templates" / "transporte" / "courier_ficha.html").read_text(encoding="utf-8")

    def test_atributo_data_clases_convierte_espacio_en_separador(self):
        self.assertIn(
            "data-clases=\"{{ (ch.licencia_clase or '')|replace('/', ',')|replace(' ', ',') }}\"",
            self.html,
        )

    def test_las_pastillas_de_clase_tambien_convierten_espacio_en_separador(self):
        self.assertIn(
            "{% for c in ch.licencia_clase.replace('/', ',').replace(' ', ',').split(',') %}",
            self.html,
        )


class SidebarSinChoferesTest(unittest.TestCase):
    """Daniel autorizó explícitamente sacar 'Choferes' del sidebar
    (2026-08-05) -- el link se va, el endpoint NO se borra."""

    @classmethod
    def setUpClass(cls):
        cls.base_html = BASE_HTML.read_text(encoding="utf-8")
        cls.app_src, _ = _fuente_y_arbol()

    def test_el_sidebar_ya_no_linkea_a_tr_choferes(self):
        self.assertNotIn("url_for('tr_choferes')", self.base_html)

    def test_el_sidebar_sigue_teniendo_couriers(self):
        self.assertIn("url_for('tr_couriers')", self.base_html)

    def test_el_endpoint_viejo_sigue_vivo_en_el_backend(self):
        # No se borró tr_choferes() -- solo se le quitó el link del menú.
        self.assertIn('def tr_choferes():', self.app_src)
        self.assertIn('@app.route("/transporte/choferes")', self.app_src)


class AppMovilDelChoferIntactaTest(unittest.TestCase):
    """La app móvil (login por PIN, captura, ruta, entrega) es un sistema
    aparte del menú admin -- no debía tocarse ni una línea."""

    @classmethod
    def setUpClass(cls):
        cls.app_src, _ = _fuente_y_arbol()

    def test_rutas_moviles_siguen_existiendo(self):
        for endpoint in (
            'def chofer_login():', 'def chofer_captura(mid):',
            'def chofer_ruta(mid):', 'def chofer_entrega(mid, commitment_id):',
            'def chofer_entrega_submit(mid, commitment_id):',
        ):
            with self.subTest(endpoint=endpoint):
                self.assertIn(endpoint, self.app_src)

    def test_login_movil_sigue_usando_pin_hash(self):
        login_src = _fuente_funcion("chofer_login")
        self.assertIn("pin_hash", login_src)


class EndpointsRosterSinRomperSelectorDeRetiroTest(unittest.TestCase):
    """GET/POST /transporte/couriers/<cid>/choferes los sigue usando el
    selector de 'chofer guardado' en captura de retiro (interno y público,
    static/transporte_manifiesto_detalle.js) -- no se les cambió la forma."""

    @classmethod
    def setUpClass(cls):
        cls.app_src, _ = _fuente_y_arbol()
        cls.listar_src = _fuente_funcion("tr_courier_choferes_listar")
        cls.crear_src = _fuente_funcion("tr_courier_choferes_crear")
        js = (RAIZ / "static" / "transporte_manifiesto_detalle.js").read_text(encoding="utf-8")
        cls.manifiesto_js = js

    def test_listar_sigue_filtrando_solo_activos(self):
        self.assertIn("AND activo=1", self.listar_src)

    def test_listar_sigue_devolviendo_los_campos_que_usa_el_selector(self):
        for campo in ("nombre", "rut", "telefono", "patente", "peso_max_kg", "volumen_max_m3"):
            self.assertIn(campo, self.listar_src)

    def test_crear_sigue_aceptando_los_campos_originales(self):
        for campo in ("nombre", "rut", "telefono", "patente"):
            self.assertIn(campo, self.crear_src)

    def test_el_frontend_de_captura_de_retiro_sigue_llamando_al_mismo_endpoint(self):
        self.assertIn("/choferes", self.manifiesto_js)

    def test_nuevos_endpoints_de_edicion_y_documentos_existen(self):
        self.assertIn('def tr_courier_chofer_editar(cid, chofer_id):', self.app_src)
        self.assertIn('def tr_courier_chofer_documento(cid, chofer_id):', self.app_src)

    def test_documentos_se_suben_a_gcs_nunca_cloudinary(self):
        doc_src = _fuente_funcion("tr_courier_chofer_documento")
        self.assertIn("_cloud_upload_raw", doc_src)
        # El código puede MENCIONAR "Cloudinary" en un comentario (para
        # explicar por qué NO se usa) -- lo que no debe aparecer es una
        # llamada real a su API.
        self.assertNotIn("cloudinary.uploader", doc_src.lower())
        self.assertNotIn("import cloudinary", doc_src.lower())

    def test_documento_ya_no_acepta_tipo_contrato(self):
        # Corrección de Daniel: el chofer no sube "su" contrato -- solo
        # licencia y seguro de carga.
        doc_src = _fuente_funcion("tr_courier_chofer_documento")
        self.assertNotIn('"contrato"', doc_src)


class FotoDeChoferTest(unittest.TestCase):
    """Foto de perfil (2026-08-06, pedido agregado por Daniel mientras se
    trabajaba en esto: "una fotito"). Mismo patrón que el logo del courier:
    _cloud_upload (imagen, con resize) → GCS, nunca Cloudinary."""

    @classmethod
    def setUpClass(cls):
        cls.app_src, _ = _fuente_y_arbol()
        cls.foto_src = _fuente_funcion("tr_courier_chofer_foto")

    def test_endpoint_de_foto_existe(self):
        self.assertIn('def tr_courier_chofer_foto(cid, chofer_id):', self.app_src)

    def test_usa_cloud_upload_de_imagen_no_el_de_archivos_crudos(self):
        # _cloud_upload (no _cloud_upload_raw): la foto SÍ debe redimensionarse
        # como cualquier otra imagen del proyecto (logo de courier, fotos de
        # producto), a diferencia de licencia/seguro que aceptan PDF.
        self.assertIn("_cloud_upload(file", self.foto_src)

    def test_nunca_cloudinary(self):
        self.assertNotIn("cloudinary.uploader", self.foto_src.lower())
        self.assertNotIn("import cloudinary", self.foto_src.lower())

    def test_guarda_en_la_columna_foto_url(self):
        self.assertIn("foto_url=%s", self.foto_src)

    def test_valida_que_sea_imagen(self):
        self.assertIn("allowed_file(file.filename)", self.foto_src)


class ContratoEsDelCourierNoDelChoferTest(unittest.TestCase):
    """Corrección explícita de Daniel el mismo día: "vamos a dejar en el
    courier el contrato de prestación de servicio" -- el chofer NO tiene su
    propio contrato, eso ya existe en transport_courier_contratos (otro
    agente lo construyó esa misma noche: contrato_tipo/inicio/fin/archivo_
    url/nombre, con tr_couriers() trayendo el vigente). Estos tests
    aseguran que la ficha de chofer no reinventa ese concepto."""

    @classmethod
    def setUpClass(cls):
        cls.app_src, _ = _fuente_y_arbol()
        cls.upsert_src = _fuente_funcion("_tr_courier_chofer_upsert")
        cls.editar_src = _fuente_funcion("tr_courier_chofer_editar")
        cls.crear_src = _fuente_funcion("tr_courier_choferes_crear")
        cls.html = (RAIZ / "templates" / "transporte" / "courier_ficha.html").read_text(encoding="utf-8")

    def test_upsert_de_chofer_no_escribe_contrato(self):
        self.assertNotIn("contrato_vencimiento", self.upsert_src)
        self.assertNotIn("contrato_doc_url", self.upsert_src)

    def test_editar_chofer_no_acepta_contrato(self):
        self.assertNotIn("contrato_vencimiento", self.editar_src)

    def test_crear_chofer_no_acepta_contrato(self):
        self.assertNotIn("contrato_vencimiento", self.crear_src)

    def test_transport_courier_contratos_sigue_siendo_la_fuente_del_contrato(self):
        # La tabla que YA existía (construida por otro agente esa misma
        # noche) sigue siendo la única fuente -- no se creó una tabla nueva.
        self.assertIn("transport_courier_contratos", self.app_src)

    def test_la_pestana_de_choferes_enlaza_al_contrato_del_courier(self):
        # El banner en la ficha usa la variable `contratos` (ya poblada por
        # tr_courier_ficha desde transport_courier_contratos) en vez de
        # inventar un campo por chofer.
        self.assertIn("Contrato de prestación de servicio", self.html)
        self.assertIn("contratos[0]", self.html)
        self.assertIn('data-bs-target="#tab-contratos"', self.html)


if __name__ == "__main__":
    unittest.main(verbosity=2)

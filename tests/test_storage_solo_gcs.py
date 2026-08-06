"""
TEST FILE -- NO ES PRODUCCION. NO IMPORTAR DESDE app.py.

Salida de Cloudinary (2026-08-05, pedido de Daniel: "necesito que saques a
Cloudinary, que todo sea por storage de Google. El desarrollo tiene que estar
centralizado alli.").

QUE VERIFICA (y por que cada cosa):

 1. ESTATICO sobre app.py / config.py / requirements.txt
    - No queda NINGUNA llamada real a Cloudinary: ni `import cloudinary`, ni
      `cloudinary.uploader`, ni CLOUDINARY_CONFIG, ni _CLD_READY.
      Por que importa: mientras exista uno solo de esos, una subida puede
      terminar en Cloudinary sin que nadie se entere.
    - La libreria salio de requirements.txt.
    - Las funciones _cloud_upload / _cloud_upload_image_full / _cloud_upload_raw
      / _cloud_delete / _cloud_delete_raw conservan nombre y firma exacta.
      Por que importa: las llaman decenas de sitios; cambiarles la firma
      romperia media aplicacion sin que el interprete avise hasta runtime.

 2. DE COMPORTAMIENTO (ejecutando las funciones reales extraidas de app.py
    con un bucket falso en memoria)
    - Con almacenamiento disponible: suben a GCS y devuelven /f/<key>.
    - SIN almacenamiento: FALLAN con excepcion. NO devuelven una URL de
      Cloudinary ni se quedan calladas. Este es el corazon del pedido.
    - Las firmas de las visitas son la unica excepcion deliberada: si no hay
      almacenamiento devuelven el data URL original, porque perder la firma
      de un cliente es peor que guardarla pesada en la base.
    - Borrar una URL historica de Cloudinary NO revienta y NO intenta
      borrarla (esa imagen no es nuestra, vive en la cuenta de Cloudinary).

 3. LECTURA DE LO HISTORICO (lo que NO se debe romper)
    - El filtro cloud_tx sigue achicando las URLs viejas de res.cloudinary.com
      y deja intactas las de Google (/f/...). Miles de fotos del catalogo y de
      las fichas dependen de eso.
    - La firma para abrir contratos viejos de Cloudinary se calcula sin la
      libreria; sin credenciales devuelve lista vacia (no explota).

Se corre con:  py -m unittest discover -s tests -q
"""
import ast
import os
import re
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
APP_PY = os.path.join(RAIZ, "app.py")
CONFIG_PY = os.path.join(RAIZ, "config.py")
REQS = os.path.join(RAIZ, "requirements.txt")

_FUENTE = None
_FUNCS = None


def _fuente_app():
    global _FUENTE
    if _FUENTE is None:
        with open(APP_PY, encoding="utf-8") as fh:
            _FUENTE = fh.read()
    return _FUENTE


def _funcs_app():
    """Indice {nombre: nodo} de las funciones de app.py (un solo parseo)."""
    global _FUNCS
    if _FUNCS is None:
        _FUNCS = {}
        for nodo in ast.walk(ast.parse(_fuente_app())):
            if isinstance(nodo, ast.FunctionDef):
                _FUNCS.setdefault(nodo.name, nodo)
    return _FUNCS


def _extraer(nombre, extras=None):
    """Extrae UNA funcion de app.py y la ejecuta en un namespace aislado.

    Mismo patron que tests/test_transport_caracterizacion.py: evita importar
    app.py entero (Flask + MySQL + ERP + credenciales) para probar codigo que
    en el fondo es puro.
    """
    nodo = _funcs_app().get(nombre)
    if nodo is None:
        raise AssertionError(f"No se encontro la funcion {nombre}() en app.py")
    ns = dict(extras or {})
    exec(compile(ast.Module(body=[nodo], type_ignores=[]), APP_PY, "exec"), ns)
    return ns[nombre]


def _sin_comentarios(src):
    """Quita comentarios de linea y docstrings triple-comilla.

    Los comentarios que EXPLICAN por que se saco Cloudinary son deseables; lo
    que no puede quedar es codigo vivo. Sin esta limpieza el test se caeria por
    su propia documentacion.
    """
    src = re.sub(r'"""(?:.|\n)*?"""', "", src)
    src = re.sub(r"'''(?:.|\n)*?'''", "", src)
    return "\n".join(re.sub(r"#.*$", "", l) for l in src.splitlines())


# ─────────────────────────────────────────────────────────────────────
# Bucket falso: imita lo justo de google.cloud.storage que usa la app.
# ─────────────────────────────────────────────────────────────────────
class _BlobFalso:
    def __init__(self, bucket, key):
        self.bucket, self.key = bucket, key
        self.cache_control = None

    def upload_from_string(self, data, content_type=None):
        self.bucket.objetos[self.key] = (data, content_type)

    def delete(self):
        self.bucket.borrados.append(self.key)
        self.bucket.objetos.pop(self.key, None)


class _BucketFalso:
    def __init__(self):
        self.objetos = {}
        self.borrados = []

    def blob(self, key):
        return _BlobFalso(self, key)


def _entorno_storage(bucket):
    """Namespace con las dependencias que necesitan las funciones extraidas.

    `bucket=None` simula "Google Cloud no responde".
    """
    def _gcs_bucket():
        return bucket

    def _gcs_ready():
        return bucket is not None

    def _gcs_key(folder, public_id, ext):
        folder = (folder or "ilus").strip("/")
        pid = str(public_id).strip("/")
        if ext and not ext.startswith("."):
            ext = "." + ext
        return f"{folder}/{pid}{ext}"

    def _storage_upload_bytes(data, key, content_type):
        b = _gcs_bucket()
        blob = b.blob(key)
        blob.cache_control = "public, max-age=2592000"
        blob.upload_from_string(data, content_type=content_type)
        return "/f/" + key

    def _img_resize_bytes(file_obj, max_dim=1600, quality=82):
        # La compresion real la hace Pillow; para este test alcanza con
        # devolver los bytes tal cual: lo que se verifica es el DESTINO.
        if hasattr(file_obj, "read"):
            file_obj.seek(0)
            return file_obj.read(), "image/jpeg"
        return bytes(file_obj or b""), "image/jpeg"

    return {
        "os": os, "time": __import__("time"), "re": re,
        "_gcs_bucket": _gcs_bucket,
        "_gcs_ready": _gcs_ready,
        "_gcs_key": _gcs_key,
        "_storage_upload_bytes": _storage_upload_bytes,
        "_img_resize_bytes": _img_resize_bytes,
        "_STORAGE_OFF_MSG": "El almacenamiento de Google Cloud no esta disponible.",
        "_RAW_CT_BY_EXT": {".pdf": "application/pdf"},
        "UPLOAD_FOLDER": os.path.join(RAIZ, "static", "uploads"),
    }


class _ArchivoFalso:
    """Imita un FileStorage de Flask (lo que llega de un <input type=file>)."""

    def __init__(self, datos=b"binario", filename="doc.pdf"):
        import io
        self._io = io.BytesIO(datos)
        self.filename = filename

    def seek(self, *a):
        return self._io.seek(*a)

    def read(self, *a):
        return self._io.read(*a)


# ═════════════════════════════════════════════════════════════════════
# 1. ESTATICO
# ═════════════════════════════════════════════════════════════════════
class TestNoQuedaCodigoCloudinary(unittest.TestCase):

    def test_app_py_sin_llamadas_a_cloudinary(self):
        codigo = _sin_comentarios(_fuente_app())
        prohibidos = [
            ("import cloudinary",   "importa la libreria de Cloudinary"),
            ("cloudinary.uploader", "sube o borra usando el SDK de Cloudinary"),
            ("cloudinary.utils",    "arma URLs con el SDK de Cloudinary"),
            ("cloudinary.config",   "configura credenciales de Cloudinary"),
            ("CLOUDINARY_CONFIG",   "lee las credenciales de Cloudinary desde config.py"),
            ("_CLD_READY",          "usa la bandera vieja de Cloudinary para decidir donde guardar"),
            ("_cloudinary_uploader", "conserva el objeto uploader de Cloudinary"),
        ]
        for aguja, motivo in prohibidos:
            self.assertNotIn(aguja, codigo,
                             f"app.py todavia {motivo} ('{aguja}')")

    def test_config_py_no_exporta_credenciales_cloudinary(self):
        with open(CONFIG_PY, encoding="utf-8") as fh:
            codigo = _sin_comentarios(fh.read())
        self.assertNotIn("CLOUDINARY_CONFIG", codigo)
        self.assertNotIn("CLOUDINARY_API_KEY", codigo)

    def test_libreria_fuera_de_requirements(self):
        with open(REQS, encoding="utf-8") as fh:
            lineas = [l.strip() for l in fh
                      if l.strip() and not l.strip().startswith("#")]
        self.assertFalse([l for l in lineas if l.lower().startswith("cloudinary")],
                         "la libreria cloudinary sigue en requirements.txt")
        self.assertTrue([l for l in lineas if l.startswith("google-cloud-storage")],
                        "falta google-cloud-storage en requirements.txt")

    def test_funciones_cloud_conservan_nombre_y_firma(self):
        """Las llaman decenas de sitios: solo cambiaron las tripas."""
        esperado = {
            "_cloud_upload":            ["file_obj", "public_id", "folder"],
            "_cloud_upload_image_full": ["file_obj", "public_id", "folder"],
            "_cloud_upload_raw":        ["file_obj", "public_id", "folder"],
            "_cloud_delete":            ["url_or_filename"],
            "_cloud_delete_raw":        ["public_id"],
            "_uploader_upload":         ["src", "public_id", "folder", "resource_type"],
            "_uploader_destroy":        ["public_id"],
        }
        funcs = _funcs_app()
        for nombre, args in esperado.items():
            self.assertIn(nombre, funcs, f"desaparecio {nombre}() de app.py")
            reales = [a.arg for a in funcs[nombre].args.args]
            self.assertEqual(reales, args,
                             f"cambio la firma de {nombre}(): {reales} != {args}")


# ═════════════════════════════════════════════════════════════════════
# 2. COMPORTAMIENTO
# ═════════════════════════════════════════════════════════════════════
class TestSubidasVanSoloAGoogle(unittest.TestCase):

    def test_imagen_va_a_gcs_y_devuelve_ruta_propia(self):
        bucket = _BucketFalso()
        fn = _extraer("_cloud_upload", _entorno_storage(bucket))
        url = fn(_ArchivoFalso(b"foto", "x.jpg"), "p1_123", folder="ilus/products")
        self.assertEqual(url, "/f/ilus/products/p1_123.jpg")
        self.assertIn("ilus/products/p1_123.jpg", bucket.objetos)
        self.assertNotIn("cloudinary", url)

    def test_imagen_full_devuelve_key_para_poder_borrarla(self):
        bucket = _BucketFalso()
        fn = _extraer("_cloud_upload_image_full", _entorno_storage(bucket))
        res = fn(_ArchivoFalso(b"foto", "x.jpg"), "eq_9", folder="ilus/maquinas")
        self.assertEqual(res["url"], "/f/ilus/maquinas/eq_9.jpg")
        self.assertEqual(res["public_id"], "ilus/maquinas/eq_9.jpg")
        self.assertEqual(res["size"], len(b"foto"))

    def test_pdf_va_a_gcs_con_su_extension(self):
        bucket = _BucketFalso()
        fn = _extraer("_cloud_upload_raw", _entorno_storage(bucket))
        res = fn(_ArchivoFalso(b"%PDF-1.4", "contrato.pdf"), "c_7")
        self.assertEqual(res["public_id"], "ilus/contratos/c_7.pdf")
        self.assertEqual(bucket.objetos["ilus/contratos/c_7.pdf"][1], "application/pdf")

    def test_sin_google_las_subidas_fallan_no_caen_a_cloudinary(self):
        """El corazon del pedido de Daniel: si Google no esta, se avisa.

        Antes estas funciones tenian un 'plan B' que terminaba en Cloudinary.
        Ese camino ya no existe, y este test lo deja clavado.
        """
        entorno = _entorno_storage(None)   # Google no responde
        casos = [
            ("_cloud_upload",            (_ArchivoFalso(b"x", "a.jpg"), "p1")),
            ("_cloud_upload_image_full", (_ArchivoFalso(b"x", "a.jpg"), "p1")),
            ("_cloud_upload_raw",        (_ArchivoFalso(b"x", "a.pdf"), "c1")),
            ("_uploader_upload",         (_ArchivoFalso(b"x", "a.jpg"),)),
        ]
        for nombre, args in casos:
            fn = _extraer(nombre, dict(entorno))
            with self.assertRaises(RuntimeError, msg=f"{nombre}() no fallo sin GCS"):
                fn(*args)

    def test_firma_de_visita_nunca_se_pierde(self):
        """Excepcion deliberada: sin almacenamiento se guarda el data URL.

        Una firma es la prueba de que el cliente recibio el servicio. Perderla
        por un problema de infraestructura seria peor que guardarla pesada.
        """
        data_url = "data:image/png;base64,aGVsbG8="
        fn_ok = _extraer("_subir_firma_storage", _entorno_storage(_BucketFalso()))
        self.assertTrue(fn_ok(data_url, 42, "cliente").startswith("/f/"))

        fn_sin = _extraer("_subir_firma_storage", _entorno_storage(None))
        self.assertEqual(fn_sin(data_url, 42, "cliente"), data_url)

    def test_alias_viejo_de_la_firma_sigue_existiendo(self):
        """_subir_firma_cloudinary se sigue llamando en 6 lugares."""
        codigo = _sin_comentarios(_fuente_app())
        self.assertIn("_subir_firma_cloudinary = _subir_firma_storage", codigo)


class TestBorradosSeguros(unittest.TestCase):

    def test_borra_de_google_lo_que_es_de_google(self):
        bucket = _BucketFalso()
        bucket.objetos["ilus/products/p1.jpg"] = (b"x", "image/jpeg")
        fn = _extraer("_cloud_delete", _entorno_storage(bucket))
        fn("/f/ilus/products/p1.jpg")
        self.assertEqual(bucket.borrados, ["ilus/products/p1.jpg"])

    def test_url_historica_de_cloudinary_no_revienta_ni_se_borra(self):
        """No es nuestra: vive en la cuenta de Cloudinary. Se ignora sin ruido."""
        bucket = _BucketFalso()
        fn = _extraer("_cloud_delete", _entorno_storage(bucket))
        fn("https://res.cloudinary.com/dbhlvyri8/image/upload/v1/ilus/x.jpg")
        self.assertEqual(bucket.borrados, [])

    def test_public_id_viejo_sin_extension_no_se_intenta_borrar(self):
        bucket = _BucketFalso()
        fn = _extraer("_cloud_delete_raw", _entorno_storage(bucket))
        fn("ilus/contratos/contrato_26_1700000000")   # sin extension = Cloudinary
        self.assertEqual(bucket.borrados, [])

    def test_key_de_google_con_extension_si_se_borra(self):
        bucket = _BucketFalso()
        fn = _extraer("_cloud_delete_raw", _entorno_storage(bucket))
        fn("ilus/contratos/contrato_26.pdf")
        self.assertEqual(bucket.borrados, ["ilus/contratos/contrato_26.pdf"])


# ═════════════════════════════════════════════════════════════════════
# 3. LO HISTORICO SE SIGUE VIENDO
# ═════════════════════════════════════════════════════════════════════
class TestImagenesHistoricasSiguenFuncionando(unittest.TestCase):

    def _tx(self):
        perfiles = {
            "thumb":  "f_auto,q_auto:eco,w_120,c_limit,dpr_auto",
            "card":   "f_auto,q_auto,w_400,c_limit,dpr_auto",
        }
        return _extraer("_cloud_url_tx", {
            "re": re,
            "_CLOUD_TX_PROFILES": perfiles,
            "_CLOUD_URL_RE": re.compile(
                r"^(https?://res\.cloudinary\.com/[^/]+/(?:image|video|raw)/upload/)(.+)$",
                re.IGNORECASE),
            "_CLOUD_TX_TOKEN_RE": re.compile(r"(?:^|,)(?:[a-z]_[^,/]+)"),
        })

    def test_foto_vieja_de_cloudinary_se_sigue_achicando(self):
        """Si esto se rompe, cada card baja la foto original de 5 MB."""
        url = "https://res.cloudinary.com/dbhlvyri8/image/upload/v123/ilus/maquinas/eq_1.jpg"
        salida = self._tx()(url, "card")
        self.assertIn("f_auto,q_auto,w_400", salida)
        self.assertIn("ilus/maquinas/eq_1.jpg", salida)

    def test_url_de_google_pasa_intacta(self):
        url = "/f/ilus/maquinas/eq_1.jpg"
        self.assertEqual(self._tx()(url, "thumb"), url)

    def test_valores_vacios_no_rompen_la_pagina(self):
        fn = self._tx()
        self.assertEqual(fn(None, "card"), "")
        self.assertEqual(fn("", "card"), "")
        self.assertEqual(fn("/static/uploads/p1.jpg", "card"), "/static/uploads/p1.jpg")


class TestContratosViejosSeSiguenPudiendoAbrir(unittest.TestCase):
    """Los contratos cargados antes de la salida siguen en Cloudinary, y a
    algunos PDF Cloudinary les responde 401 si no se pide con firma. La firma
    ahora se calcula sin la libreria."""

    def _fn(self):
        return _extraer("_cloudinary_urls_firmadas_legacy", {"os": os})

    def test_sin_credenciales_devuelve_vacio_y_no_explota(self):
        previo = {k: os.environ.pop(k, None)
                  for k in ("CLOUDINARY_CLOUD_NAME", "CLOUDINARY_API_SECRET")}
        try:
            self.assertEqual(self._fn()("ilus/contratos/c1"), [])
        finally:
            for k, v in previo.items():
                if v is not None:
                    os.environ[k] = v

    def test_con_credenciales_arma_las_dos_variantes_firmadas(self):
        previo = {k: os.environ.get(k)
                  for k in ("CLOUDINARY_CLOUD_NAME", "CLOUDINARY_API_SECRET")}
        os.environ["CLOUDINARY_CLOUD_NAME"] = "demo"
        os.environ["CLOUDINARY_API_SECRET"] = "secreto-de-prueba"
        try:
            urls = self._fn()("ilus/contratos/c1")
            self.assertEqual(len(urls), 2)
            self.assertTrue(any("/raw/upload/" in u for u in urls))
            self.assertTrue(any("/image/upload/" in u for u in urls))
            for u in urls:
                self.assertRegex(u, r"/s--[A-Za-z0-9_\-]{8}--/ilus/contratos/c1$")
        finally:
            for k, v in previo.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v

    def test_la_firma_coincide_con_el_algoritmo_de_cloudinary(self):
        """sha1(public_id + api_secret) -> base64 url-safe, primeros 8 chars.

        Es el mismo calculo que hacia cloudinary.utils.cloudinary_url(). Si
        alguien lo toca sin querer, este test lo detiene.
        """
        import base64
        import hashlib
        os.environ["CLOUDINARY_CLOUD_NAME"] = "demo"
        os.environ["CLOUDINARY_API_SECRET"] = "abc123"
        try:
            esperada = base64.urlsafe_b64encode(
                hashlib.sha1(b"ilus/contratos/c1" + b"abc123").digest()
            )[:8].decode("ascii")
            urls = self._fn()("ilus/contratos/c1")
            self.assertIn(f"/s--{esperada}--/", urls[0])
        finally:
            os.environ.pop("CLOUDINARY_CLOUD_NAME", None)
            os.environ.pop("CLOUDINARY_API_SECRET", None)


class TestHerramientaDeMigracionCubreLoImportante(unittest.TestCase):
    """El diagnostico de /admin/storage tiene que mirar TODAS las tablas con
    URLs de Cloudinary. Si mira de menos, da una falsa sensacion de 'ya casi
    no queda nada' y se podria cerrar la cuenta rompiendo fotos reales."""

    def _allowlist_src(self):
        src = _fuente_app()
        i = src.index("_STORAGE_MIG_ALLOWLIST = {")
        return src[i:src.index("\n}", i)]

    def test_incluye_contratos_y_adjuntos(self):
        bloque = self._allowlist_src()
        for tabla in ("mant_contratos", "mant_contrato_adjuntos",
                      "mant_tecnicos_externos", "transport_couriers",
                      "mant_reportes", "mant_visitas"):
            self.assertIn(tabla, bloque,
                          f"{tabla} no esta en el diagnostico de storage")

    def test_usa_las_constantes_de_tablas_configurables(self):
        """AUTH_TABLE/PHOTOS_TABLE salen de MYSQL_CONFIG: no se hardcodean."""
        bloque = self._allowlist_src()
        self.assertIn("AUTH_TABLE", bloque)
        self.assertIn("PHOTOS_TABLE", bloque)


if __name__ == "__main__":
    unittest.main(verbosity=2)

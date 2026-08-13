"""
TEST FILE -- NO ES PRODUCCION. NO IMPORTAR DESDE app.py.

Regresion de un bug REAL MEDIDO EN PRODUCCION (Cloud Run, 2026-08-13).

EL SINTOMA
----------
Al abrir el documento imprimible de una OT (/mantenciones/ot/<vid>/pdf) el
HTML pide ~40 fotos de golpe al proxy /f/<key>. Conteo real de una hora:

    42 respuestas 304
    36 respuestas 200
     5 respuestas 503     <-- ~6% de las fotos

Daniel imprimia la OT y salia con HUECOS donde debian ir las fotos, delante
del cliente.

LA CAUSA
--------
_gcs_bucket() marcaba la bandera "_GCS_INIT_DONE = True" ANTES de construir
el cliente. Entre esa marca y la asignacion real de _GCS_BUCKET_OBJ pasan
cientos de ms (import de google.cloud.storage + resolucion de credenciales
ADC contra el servidor de metadatos). Con `gunicorn --workers 2 --threads 8`
(Dockerfile), los otros 7 hilos que caian en esa ventana veian la bandera en
True y se llevaban un bucket que TODAVIA era None -> 503.

QUE FIJA ESTE TEST (para que nadie lo reintroduzca)
---------------------------------------------------
 1. INVARIANTE DE ORDEN (estatico, sobre el AST real de app.py): dentro de
    _gcs_bucket(), _GCS_BUCKET_OBJ se asigna con el cliente ANTES de que
    _GCS_INIT_DONE pase a True. El orden invertido ES el bug.
 2. CONCURRENCIA (ejecutando la funcion REAL extraida de app.py): 8 hilos
    simultaneos con un storage.Client() lento -> NINGUNO recibe None, y el
    cliente se construye UNA sola vez.
 3. NO SE LATCHEA UN FALLO PARA SIEMPRE: si el primer intento falla, pasado
    el cooldown se vuelve a intentar y se recupera solo. Antes, un solo
    parpadeo dejaba al worker sin almacenamiento hasta morir.
 4. EL KILL-SWITCH SIGUE MANDANDO: con ILUS_STORAGE_GCS=0 devuelve None y
    NUNCA intenta construir un cliente.

Se corre con:  py -m unittest discover -s tests -q
"""
import ast
import os
import sys
import threading
import time
import types
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
APP_PY = os.path.join(RAIZ, "app.py")

# Cuanto "tarda" el storage.Client() falso. Tiene que ser bastante mas grande
# que el ruido del planificador de hilos para que la ventana de carrera sea
# reproducible: con el codigo viejo este valor hacia fallar a 7 de 8 hilos.
DEMORA_CLIENT_S = 0.30


_NODO_CACHE = None


def _nodo_gcs_bucket():
    """Nodo AST de _gcs_bucket() tal como esta HOY en app.py.

    OJO con el rendimiento: app.py pesa ~4,9 MB y un ast.parse() completo
    tarda ~56 s en la maquina de Daniel. Por eso se recorta primero el texto
    de la funcion (desde 'def _gcs_bucket():' hasta la proxima linea a nivel
    cero) y se parsea solo ese pedazo: milisegundos, y sigue siendo el codigo
    REAL de produccion, no una copia. El resultado se cachea porque varios
    tests lo piden.
    """
    global _NODO_CACHE
    if _NODO_CACHE is not None:
        return _NODO_CACHE

    with open(APP_PY, encoding="utf-8") as fh:
        lineas = fh.read().splitlines()

    ini = None
    for i, linea in enumerate(lineas):
        if linea.startswith("def _gcs_bucket("):
            ini = i
            break
    if ini is None:
        raise AssertionError("No se encontro 'def _gcs_bucket(' en app.py")

    fin = len(lineas)
    for j in range(ini + 1, len(lineas)):
        l = lineas[j]
        if l and not l[0].isspace():      # volvio al nivel cero -> se acabo
            fin = j
            break

    trozo = "\n".join(lineas[ini:fin])
    for nodo in ast.parse(trozo).body:
        if isinstance(nodo, ast.FunctionDef) and nodo.name == "_gcs_bucket":
            _NODO_CACHE = nodo
            return nodo
    raise AssertionError("No se pudo aislar _gcs_bucket() de app.py")


class _BucketFalso:
    """Lo minimo que devuelve Client().bucket(...): un objeto no-None."""

    def __init__(self, nombre):
        self.nombre = nombre


class _ClienteFalso:
    """Imita google.cloud.storage.Client(). Tarda a proposito al construirse:
    eso es lo que abre la ventana de carrera en produccion (ADC + metadata)."""

    def __init__(self, contador, demora, explota=None):
        contador.append(1)
        if explota is not None:
            time.sleep(demora)
            raise explota
        time.sleep(demora)

    def bucket(self, nombre):
        return _BucketFalso(nombre)


def _entorno(gcs_enabled=True, demora=DEMORA_CLIENT_S, explota=None,
             cooldown=30.0, espera=15.0):
    """Namespace aislado con las dependencias reales que usa _gcs_bucket().

    Devuelve (funcion, namespace, contador_de_clientes_construidos).
    """
    construidos = []

    class _StorageFalso:
        @staticmethod
        def Client(*a, **kw):
            return _ClienteFalso(construidos, demora, explota)

    ns = {
        "threading": threading,
        "time": time,
        "GCS_ENABLED": gcs_enabled,
        "GCS_BUCKET": "ilus-app-fotos",
        "_GCS_BUCKET_OBJ": None,
        "_GCS_INIT_DONE": False,
        "_GCS_INIT_LOCK": threading.Lock(),
        "_GCS_INIT_FAIL_MONO": 0.0,
        "_GCS_RETRY_COOLDOWN_S": cooldown,
        "_GCS_INIT_WAIT_S": espera,
        "print": lambda *a, **kw: None,   # el test no ensucia la salida
        "_storage_falso": _StorageFalso,
    }
    nodo = _nodo_gcs_bucket()
    exec(compile(ast.Module(body=[nodo], type_ignores=[]), APP_PY, "exec"), ns)
    return ns["_gcs_bucket"], ns, construidos


class _GoogleCloudStorageStub:
    """Context manager: hace que `from google.cloud import storage` dentro de
    _gcs_bucket() resuelva al doble del test, sin tener instalada la libreria
    real (la maquina de Daniel no la tiene) y sin tocar credenciales."""

    def __init__(self, storage_falso):
        self.storage_falso = storage_falso
        self.previos = {}

    def __enter__(self):
        for nombre in ("google", "google.cloud"):
            self.previos[nombre] = sys.modules.get(nombre, None)
        mod_google = types.ModuleType("google")
        mod_cloud = types.ModuleType("google.cloud")
        mod_cloud.storage = self.storage_falso
        mod_google.cloud = mod_cloud
        sys.modules["google"] = mod_google
        sys.modules["google.cloud"] = mod_cloud
        return self

    def __exit__(self, *exc):
        for nombre, previo in self.previos.items():
            if previo is None:
                sys.modules.pop(nombre, None)
            else:
                sys.modules[nombre] = previo
        return False


def _correr_en_paralelo(fn, n_hilos=8):
    """Suelta n_hilos EXACTAMENTE a la vez (barrera) sobre fn. Devuelve la
    lista de resultados. Reproduce la rafaga de ~40 fotos de la OT."""
    barrera = threading.Barrier(n_hilos)
    resultados = [("sin-correr", None)] * n_hilos

    def trabajo(i):
        barrera.wait()
        try:
            resultados[i] = ("ok", fn())
        except Exception as e:          # pragma: no cover
            resultados[i] = ("excepcion", e)

    hilos = [threading.Thread(target=trabajo, args=(i,), daemon=True)
             for i in range(n_hilos)]
    for h in hilos:
        h.start()
    for h in hilos:
        h.join(timeout=30)
    return resultados


class TestOrdenDeInicializacion(unittest.TestCase):
    """El invariante que, si se rompe, devuelve el bug."""

    def test_el_bucket_se_asigna_antes_de_marcar_init_done(self):
        nodo = _nodo_gcs_bucket()

        pos_bucket_con_cliente = None   # _GCS_BUCKET_OBJ = ...Client()...
        pos_done_true = None            # _GCS_INIT_DONE = True

        for sub in ast.walk(nodo):
            if not isinstance(sub, ast.Assign):
                continue
            destinos = [t.id for t in sub.targets if isinstance(t, ast.Name)]
            fuente = ast.dump(sub.value)
            if "_GCS_BUCKET_OBJ" in destinos and "Client" in fuente:
                if pos_bucket_con_cliente is None:
                    pos_bucket_con_cliente = sub.lineno
            if "_GCS_INIT_DONE" in destinos and isinstance(sub.value, ast.Constant) \
                    and sub.value.value is True:
                # Interesa la marca del camino de EXITO, o sea la primera que
                # aparece despues de construir el cliente.
                if pos_bucket_con_cliente is not None and pos_done_true is None:
                    pos_done_true = sub.lineno

        self.assertIsNotNone(
            pos_bucket_con_cliente,
            "_gcs_bucket() ya no asigna _GCS_BUCKET_OBJ con storage.Client(): "
            "revisar este test junto con el cambio.")
        self.assertIsNotNone(
            pos_done_true,
            "No se encontro '_GCS_INIT_DONE = True' despues de construir el "
            "cliente. Si se elimino la bandera, actualizar este test.")
        self.assertLess(
            pos_bucket_con_cliente, pos_done_true,
            "REGRESION: _GCS_INIT_DONE se marca True ANTES de asignar "
            "_GCS_BUCKET_OBJ. Ese orden invertido es exactamente el bug que "
            "producia 503 intermitentes en /f/<key> (OT impresa con huecos). "
            "El objeto va PRIMERO, la bandera DESPUES.")

    def test_la_funcion_usa_un_lock(self):
        fuente = ast.dump(_nodo_gcs_bucket())
        self.assertIn(
            "_GCS_INIT_LOCK", fuente,
            "_gcs_bucket() dejo de usar el lock de inicializacion: sin el, "
            "varios hilos entran a construir el cliente a la vez y los que "
            "pierden se van con None (503).")


class TestConcurrencia(unittest.TestCase):
    """El corazon: la rafaga de fotos de la OT."""

    def test_ocho_hilos_simultaneos_ninguno_recibe_none(self):
        fn, ns, construidos = _entorno()
        with _GoogleCloudStorageStub(ns["_storage_falso"]):
            resultados = _correr_en_paralelo(fn, n_hilos=8)

        estados = [e for e, _ in resultados]
        self.assertNotIn("excepcion", estados,
                         f"_gcs_bucket() lanzo excepcion: {resultados}")
        self.assertNotIn("sin-correr", estados, "algun hilo no termino a tiempo")

        nones = [v for _, v in resultados if v is None]
        self.assertEqual(
            [], nones,
            f"{len(nones)} de 8 hilos recibieron None -> {len(nones)} "
            f"respuestas 503 en /f/<key>. Con el codigo viejo eran 7 de 8.")

    def test_el_cliente_se_construye_una_sola_vez(self):
        fn, ns, construidos = _entorno()
        with _GoogleCloudStorageStub(ns["_storage_falso"]):
            _correr_en_paralelo(fn, n_hilos=8)
        self.assertEqual(
            1, len(construidos),
            f"storage.Client() se construyo {len(construidos)} veces. Debe ser "
            f"UNA sola: cada construccion es una consulta al servidor de "
            f"metadatos de Cloud Run.")

    def test_todos_los_hilos_reciben_el_mismo_bucket(self):
        fn, ns, construidos = _entorno()
        with _GoogleCloudStorageStub(ns["_storage_falso"]):
            resultados = _correr_en_paralelo(fn, n_hilos=8)
        objetos = {id(v) for _, v in resultados}
        self.assertEqual(
            1, len(objetos),
            "Los hilos recibieron buckets distintos; debe ser el mismo objeto "
            "cacheado (si no, se pierde el pool de conexiones HTTP).")


class TestFalloNoEsPermanente(unittest.TestCase):
    """Antes, un solo fallo dejaba al worker sin almacenamiento hasta morir."""

    def test_tras_fallar_devuelve_none_sin_reintentar_en_cada_llamada(self):
        # cooldown alto: el segundo intento NO debe volver a construir cliente.
        fn, ns, construidos = _entorno(explota=RuntimeError("metadata caido"),
                                       demora=0.0, cooldown=30.0)
        with _GoogleCloudStorageStub(ns["_storage_falso"]):
            self.assertIsNone(fn())
            for _ in range(20):
                self.assertIsNone(fn())
        self.assertEqual(
            1, len(construidos),
            "Se reintento la conexion en cada llamada. Con 40 fotos en rafaga "
            "serian 40 golpes seguidos al servidor de metadatos.")

    def test_pasado_el_cooldown_se_reintenta_y_se_recupera(self):
        # cooldown 0 = "ya paso el tiempo de espera".
        fn, ns, construidos = _entorno(explota=RuntimeError("metadata caido"),
                                       demora=0.0, cooldown=0.0)
        with _GoogleCloudStorageStub(ns["_storage_falso"]):
            self.assertIsNone(fn(), "el primer intento debia fallar")

            # Google vuelve: el doble deja de explotar.
            class _StorageSano:
                @staticmethod
                def Client(*a, **kw):
                    return _ClienteFalso(construidos, 0.0, None)

            with _GoogleCloudStorageStub(_StorageSano):
                self.assertIsNotNone(
                    fn(),
                    "REGRESION: el worker quedo latcheado sin almacenamiento. "
                    "Un fallo transitorio debe recuperarse solo pasado el "
                    "cooldown, no durar hasta que muera el worker.")


class TestKillSwitch(unittest.TestCase):
    """ILUS_STORAGE_GCS=0 tiene que seguir apagando todo (REGLA #4.2: no se
    toca una funcionalidad existente al arreglar otra cosa)."""

    def test_deshabilitado_devuelve_none_y_no_construye_cliente(self):
        fn, ns, construidos = _entorno(gcs_enabled=False)
        with _GoogleCloudStorageStub(ns["_storage_falso"]):
            for _ in range(5):
                self.assertIsNone(fn())
        self.assertEqual(
            [], construidos,
            "Con el kill-switch apagado NO se debe construir ningun cliente.")


if __name__ == "__main__":
    unittest.main()

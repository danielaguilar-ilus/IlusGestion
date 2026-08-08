"""El modal de medidas de Transporte debe obedecer las MISMAS reglas que
Etiquetas al crear una ficha de producto.

VULNERABILIDAD REAL (2026-08-07). La detectó Alison gestionando etiquetas
desde Transporte. Daniel: "el modal que tenemos en transporte no obedece
ninguna de las restricciones, reglas o procesos que le establecimos durante
las etiquetas... terminó creando una máquina que no tiene nombre, que no
tiene código, que no tiene bultos, y me terminó dañando el proceso".

Caso concreto: el equipo **BBV9.0** quedó creado en el catálogo con el SKU
de nombre, sin código de impresión y con estado 'activo' (que ni siquiera
existe en Etiquetas). Hubo que borrarlo a mano de producción.

La raíz: `app_products` es la MISMA tabla del módulo Etiquetas, pero DOS
endpoints de Transporte escribían en ella por una puerta lateral que se
saltaba todas sus validaciones:

  1. cubicador_plus.cubicador_plus_post_medidas_por_sku  (modal del Cubicador)
  2. app.tr_inline_bulto                                  (modal del Monitor)

Arreglar solo uno deja el hueco abierto, así que estas pruebas cubren LOS
DOS. Se verifica contra el código real (no una copia) porque ambos endpoints
necesitan BD y sesión Flask para ejecutarse.

Correr con:  py -m unittest tests.test_modal_medidas_reglas_etiquetas -v
"""
import ast
import re
import unittest

APP_SRC = open("app.py", encoding="utf-8", errors="ignore").read()
CUB_SRC = open("cubicador_plus.py", encoding="utf-8", errors="ignore").read()


# app.py son ~88.000 líneas: se parsea UNA vez por archivo y se cachea el
# resultado. Sin esto, cada assert volvía a construir el AST completo y la
# suite se iba a varios minutos.
_ARBOLES = {}
_CACHE_FN = {}


def _fuente_funcion(src, nombre):
    """Código fuente de una función, extraído del archivo REAL."""
    clave = (id(src), nombre)
    if clave in _CACHE_FN:
        return _CACHE_FN[clave]
    if id(src) not in _ARBOLES:
        _ARBOLES[id(src)] = ast.parse(src)
    for nodo in ast.walk(_ARBOLES[id(src)]):
        if isinstance(nodo, ast.FunctionDef) and nodo.name == nombre:
            _CACHE_FN[clave] = ast.unparse(nodo)
            return _CACHE_FN[clave]
    raise AssertionError(f"No se encontró la función '{nombre}'")


class TestNingunaPuertaCreaFichasSinNombre(unittest.TestCase):
    """El fallback silencioso `nombre or sku` es LO QUE causó el bug: la
    máquina quedó llamándose igual que su propio código de barras."""

    def test_el_modal_del_cubicador_exige_nombre(self):
        fn = _fuente_funcion(CUB_SRC, "cubicador_plus_post_medidas_por_sku")
        self.assertNotIn("or sku", fn.replace('"', "'").replace("' or sku'", ""),
                         "volvió el fallback silencioso: la ficha se crearía "
                         "con el SKU de nombre, como pasó con BBV9.0")
        self.assertIn("SIN_NOMBRE", fn)

    def test_el_modal_del_monitor_exige_nombre(self):
        fn = _fuente_funcion(APP_SRC, "tr_inline_bulto")
        self.assertNotIn("nombre or sku", fn,
                         "volvió el fallback silencioso en tr_inline_bulto")
        self.assertIn("el nombre es obligatorio", fn.lower())


class TestNingunaPuertaCreaFichasSinCodigo(unittest.TestCase):
    """Etiquetas asigna un código de impresión a CADA producto. Sin él la
    ficha no se puede imprimir ni aparece bien en el catálogo — es la
    segunda cosa que Daniel notó del equipo mal creado."""

    def test_el_modal_del_cubicador_asigna_codigo(self):
        fn = _fuente_funcion(CUB_SRC, "cubicador_plus_post_medidas_por_sku")
        self.assertIn("next_codigo", fn,
                      "la ficha nacería sin código de impresión")
        self.assertIn("codigo", fn)

    def test_el_modal_del_monitor_asigna_codigo(self):
        fn = _fuente_funcion(APP_SRC, "tr_inline_bulto")
        self.assertIn("next_codigo", fn)

    def test_ambos_usan_el_generador_de_etiquetas_no_uno_propio(self):
        # next_codigo() es el generador REAL de Etiquetas. Si alguien inventa
        # un correlativo aparte, los códigos se pisan entre módulos.
        self.assertIn("def next_codigo", APP_SRC)


class TestEstadoValidoDeEtiquetas(unittest.TestCase):
    """Etiquetas usa Pendiente / Confirmado / Impreso. Los dos endpoints
    escribían 'activo', un estado que no existe en ese módulo: la ficha
    quedaba en un limbo que ningún filtro de Etiquetas mostraba."""

    ESTADOS_VALIDOS = ("Pendiente", "Confirmado", "Impreso")

    def test_el_modal_del_cubicador_no_escribe_activo(self):
        fn = _fuente_funcion(CUB_SRC, "cubicador_plus_post_medidas_por_sku")
        self.assertNotIn("'activo'", fn.lower(),
                         "volvió el estado 'activo', que no existe en Etiquetas")
        self.assertTrue(any(e in fn for e in self.ESTADOS_VALIDOS))

    def test_el_modal_del_monitor_no_escribe_activo(self):
        fn = _fuente_funcion(APP_SRC, "tr_inline_bulto")
        self.assertNotIn("'activo'", fn.lower())
        self.assertTrue(any(e in fn for e in self.ESTADOS_VALIDOS))


class TestSoloSkusQueExistenEnElErp(unittest.TestCase):
    """Regla de seguridad de Etiquetas (2026-05-26, Daniel): "solo se aceptan
    SKUs que existan en el ERP... para que no se puedan inventar productos".
    Los modales de Transporte no la aplicaban: cualquier texto servía."""

    def test_el_modal_del_cubicador_valida_contra_el_erp(self):
        fn = _fuente_funcion(CUB_SRC, "cubicador_plus_post_medidas_por_sku")
        self.assertIn("get_erp_product_by_sku", fn)
        self.assertIn("SKU_NO_EN_ERP", fn)

    def test_el_modal_del_monitor_valida_contra_el_erp(self):
        fn = _fuente_funcion(APP_SRC, "tr_inline_bulto")
        self.assertIn("get_erp_product_by_sku", fn)

    def test_una_caida_del_erp_no_bloquea_la_operacion(self):
        # Criterio deliberado: si el ERP no responde se deja pasar y queda en
        # el log. Bloquear dejaría a bodega sin poder trabajar por una caída
        # de un sistema externo — peor que el riesgo que evita.
        for fn in (_fuente_funcion(CUB_SRC, "cubicador_plus_post_medidas_por_sku"),
                   _fuente_funcion(APP_SRC, "tr_inline_bulto")):
            self.assertIn("except Exception", fn)


class TestSincronizaConEtiquetas(unittest.TestCase):
    """Etiquetas llama sync_erp_table en CADA alta y edición. El modal de
    Transporte no lo hacía nunca: la ficha quedaba desincronizada."""

    def test_guardar_medidas_sincroniza(self):
        fn = _fuente_funcion(CUB_SRC, "_guardar_medidas")
        self.assertIn("sync_erp_table", fn,
                      "las medidas cargadas desde Transporte no llegan a Etiquetas")


class TestTopesDeMedidaEnLasTRESPuertas(unittest.TestCase):
    """El caso que disparó todo: un bulto de 2006 × 965 × 1574 cm (20 metros
    por lado) dio 761.733 kg de peso volumétrico. El modal del Cubicador SÍ
    lo atajaba; Etiquetas y el modal del Monitor NO validaban nada."""

    def test_etiquetas_ahora_tiene_topes(self):
        fn = _fuente_funcion(APP_SRC, "validate_bultos_form")
        self.assertIn("MAX_BULTO_CM", fn,
                      "Etiquetas sigue aceptando cualquier medida")
        self.assertIn("MAX_BULTO_KG", fn)

    def test_el_modal_del_monitor_ahora_tiene_topes(self):
        fn = _fuente_funcion(APP_SRC, "tr_inline_bulto")
        self.assertIn("fuera de rango", fn.lower())

    def test_el_modal_del_cubicador_conserva_sus_topes(self):
        self.assertIn("_MAX_CM", CUB_SRC)
        self.assertIn("_MAX_KG", CUB_SRC)

    def test_los_tres_usan_el_mismo_limite(self):
        """Si los topes divergen, un dato entra por la puerta más floja y
        el sistema vuelve a quedar inconsistente consigo mismo."""
        cub_cm = re.search(r"_MAX_CM\s*=\s*([\d.]+)", CUB_SRC)
        app_cm = re.search(r"MAX_BULTO_CM\s*=\s*([\d.]+)", APP_SRC)
        inline_cm = re.search(r"_MAX_CM_INLINE\s*=\s*([\d.]+)", APP_SRC)
        self.assertIsNotNone(cub_cm)
        self.assertIsNotNone(app_cm)
        self.assertIsNotNone(inline_cm)
        self.assertEqual(float(cub_cm.group(1)), float(app_cm.group(1)))
        self.assertEqual(float(cub_cm.group(1)), float(inline_cm.group(1)))

    def test_el_caso_real_de_alison_seria_rechazado(self):
        """2006 cm supera el tope en las tres puertas."""
        tope = float(re.search(r"_MAX_CM\s*=\s*([\d.]+)", CUB_SRC).group(1))
        self.assertGreater(2006.0, tope,
                           "el bulto de 2006 cm que reventó el cálculo seguiría pasando")
        # Y una medida real de máquina grande (2,5 m) sigue siendo válida.
        self.assertLess(250.0, tope, "el tope es tan bajo que rechaza máquinas reales")


if __name__ == "__main__":
    unittest.main(verbosity=2)


class TestPaginaDeFichasIncompletas(unittest.TestCase):
    """Diagnóstico de las fichas que alcanzaron a entrar ANTES del arreglo.

    Daniel pidió una página, no una consulta suelta: "haz la página de
    diagnóstico". Es SOLO LECTURA a propósito — lista y explica qué le falta
    a cada ficha, pero no corrige ni borra: eso se hace a mano desde
    Etiquetas, que es donde viven las reglas.
    """

    def test_la_ruta_existe_y_es_solo_superadmin(self):
        fn = _fuente_funcion(APP_SRC, "admin_fichas_incompletas")
        self.assertIn("admin_fichas_incompletas", APP_SRC)
        i = APP_SRC.find("def admin_fichas_incompletas")
        cabecera = APP_SRC[max(0, i - 300):i]
        self.assertIn("superadmin", cabecera,
                      "la página quedó abierta a cualquier rol")
        self.assertIn("/admin/fichas-incompletas", cabecera)

    def test_no_escribe_nada_en_la_base(self):
        """Si algún día alguien le agrega un botón de 'corregir', que sea una
        decisión consciente y no algo que se cuele — esta página existe para
        MIRAR."""
        fn = _fuente_funcion(APP_SRC, "admin_fichas_incompletas")
        for peligro in ("UPDATE ", "DELETE ", "INSERT ", "mysql_execute"):
            with self.subTest(peligro):
                self.assertNotIn(peligro, fn,
                                 f"la página de diagnóstico ejecuta {peligro.strip()}: "
                                 f"dejó de ser solo lectura")

    def test_detecta_las_cuatro_senales_del_caso_bbv9(self):
        fn = _fuente_funcion(APP_SRC, "admin_fichas_incompletas")
        # Las mismas cuatro cosas que le faltaban al equipo mal creado.
        self.assertIn("codigo", fn)          # sin código de impresión
        self.assertIn("estado", fn)          # estado inválido ('activo')
        self.assertIn("nombre", fn)          # nombre == SKU
        self.assertIn("n_bultos", fn)        # sin bultos
        self.assertIn("motivos", fn,
                      "no explica QUÉ le falta a cada ficha")

    def test_los_estados_validos_son_los_de_etiquetas(self):
        """Se mira la TUPLA de estados válidos, no el texto del archivo: el
        comentario menciona 'activo' a propósito (es el valor que causó el
        bug) y un grep suelto lo confundiría con código."""
        fn = _fuente_funcion(APP_SRC, "admin_fichas_incompletas")
        m = re.search(r"ESTADOS_VALIDOS\s*=\s*\(([^)]*)\)", fn, re.IGNORECASE)
        self.assertIsNotNone(m, "no se encontró la tupla de estados válidos")
        valores = {v.strip().strip("'\"").lower() for v in m.group(1).split(",") if v.strip()}
        self.assertEqual({"pendiente", "confirmado", "impreso"}, valores)
        self.assertNotIn("activo", valores,
                         "'activo' se coló como estado válido: las fichas mal "
                         "creadas dejarían de detectarse")

    def test_esta_enlazada_en_el_menu(self):
        with open("templates/base.html", encoding="utf-8", errors="ignore") as fh:
            base = fh.read()
        self.assertIn("admin_fichas_incompletas", base,
                      "la página existe pero no hay cómo llegar a ella")

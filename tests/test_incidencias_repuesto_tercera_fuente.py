"""Incidencias como TERCERA FUENTE de repuestos + "vuelco" a la
conciliación (2026-09-26 -- Daniel: "está disfuncional... no selecciona
nada, no sé qué hace" sobre "Revisar diferencias"; y "los productos de
incidencias deben ser una TERCERA FUENTE al solicitar repuesto").

Prueba TRES funciones PURAS de app.py (sin BD ni Flask), extraídas con ast
-- mismo criterio que tests/test_repuestos_lote_validacion.py:

  - _inc_disponible_repuesto: cantidad - solicitudes activas ya vinculadas
    a esa incidencia (no tomar dos veces la misma pieza).
  - _inc_hallazgo_accion: qué botón mostrar por hallazgo de conciliación
    ('registrar' | 'ver' | None) -- el bug real era que TODAS las filas
    tenían cursor de mano pero solo unas pocas tenían onclick.
  - _inc_hallazgo_falta_registrar: arma el hallazgo con los datos para
    prellenar el alta (SKU, descripción, UA, ubicación, cantidad).

Correr con:  py -m unittest tests.test_incidencias_repuesto_tercera_fuente
(pytest NO esta instalado en el equipo de Daniel.)
"""
import ast
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
APP_PY = os.path.join(RAIZ, "app.py")


def _leer(path):
    with open(path, encoding="utf-8") as fh:
        return fh.read()


# 🔧 2026-09-26 (revisión post-merge): app.py ya pasa las 140 mil líneas --
# en máquinas o sandboxes con disco/CPU lentos, un solo `ast.parse` de este
# archivo puede tardar varios MINUTOS (medido: ~190 s en un entorno de CI
# restringido). Antes, CADA llamada a `_fuente_de`/`_cargar_funciones`
# volvía a leer y re-parsear el archivo completo desde cero -- con las 6
# llamadas de TestConciliacionExcluyeEliminadas más las de las demás clases,
# la suite completa se volvía impráctica de esperar. Se cachea el AST (y el
# código fuente) UNA sola vez por proceso: el resto de las llamadas reusan
# el mismo árbol ya parseado. No cambia lo que se prueba, solo cuánto tarda.
_AST_CACHE = {}


def _codigo_y_arbol():
    if "codigo" not in _AST_CACHE:
        codigo = _leer(APP_PY)
        _AST_CACHE["codigo"] = codigo
        _AST_CACHE["arbol"] = ast.parse(codigo)
    return _AST_CACHE["codigo"], _AST_CACHE["arbol"]


def _fuente_de(nombre_funcion):
    """Devuelve el CÓDIGO FUENTE (texto crudo) de una función de app.py por
    nombre, usando ast.get_source_segment -- para revisar SQL armado sin
    ejecutarlo (no hay BD en este test, ver módulo docstring)."""
    codigo, arbol = _codigo_y_arbol()
    for nodo in ast.walk(arbol):
        if isinstance(nodo, ast.FunctionDef) and nodo.name == nombre_funcion:
            return ast.get_source_segment(codigo, nodo)
    raise AssertionError(f"no se encontró la función '{nombre_funcion}' en app.py")


def _cargar_funciones(*nombres):
    """Extrae varias funciones/constantes de app.py y las ejecuta juntas en
    un ambito aislado (para que las que dependen de otras -- ej.
    _inc_hallazgo_falta_registrar usa el string INC_BODEGA_WMS -- lo
    encuentren)."""
    _, arbol = _codigo_y_arbol()
    ambito = {}
    # INC_BODEGA_WMS es un simple `NOMBRE = "literal"` a nivel de módulo;
    # se busca aparte porque no es un FunctionDef.
    for nodo in arbol.body:
        if (isinstance(nodo, ast.Assign) and len(nodo.targets) == 1
                and isinstance(nodo.targets[0], ast.Name)
                and nodo.targets[0].id == "INC_BODEGA_WMS"):
            exec(compile(ast.Module(body=[nodo], type_ignores=[]), "<app>", "exec"), ambito)
        if isinstance(nodo, ast.FunctionDef) and nodo.name in nombres:
            exec(compile(ast.Module(body=[nodo], type_ignores=[]), "<app>", "exec"), ambito)
    faltan = [n for n in nombres if n not in ambito]
    assert not faltan, f"no se encontraron en app.py: {faltan}"
    return ambito


class TestDisponibleRepuesto(unittest.TestCase):
    """_inc_disponible_repuesto: disponible = cantidad - solicitudes
    activas (definición documentada en app.py, Daniel pidió "define y
    documenta"): TODO estado resta salvo 'rechazado' -- incluso
    'instalado' sigue restando porque la pieza ya se usó."""

    @classmethod
    def setUpClass(cls):
        cls.disponible = staticmethod(_cargar_funciones("_inc_disponible_repuesto")["_inc_disponible_repuesto"])

    def test_sin_solicitudes_disponible_es_toda_la_cantidad(self):
        self.assertEqual(self.disponible(5, []), 5)

    def test_sin_solicitudes_lista_none_no_revienta(self):
        self.assertEqual(self.disponible(3, None), 3)

    def test_una_solicitud_resta_la_cantidad(self):
        self.assertEqual(self.disponible(5, [2]), 3)

    def test_varias_solicitudes_se_suman_y_restan(self):
        self.assertEqual(self.disponible(10, [2, 3, 1]), 4)

    def test_puede_quedar_en_cero(self):
        self.assertEqual(self.disponible(4, [4]), 0)

    def test_puede_quedar_negativo_si_se_sobrepasa(self):
        # No es un caso normal (el endpoint de creación rechaza esto antes
        # de insertar), pero la función en sí no debe esconder el exceso.
        self.assertEqual(self.disponible(2, [3]), -1)

    def test_cantidad_total_none_se_trata_como_cero(self):
        self.assertEqual(self.disponible(None, [1]), -1)

    def test_cantidades_none_dentro_de_la_lista_se_tratan_como_cero(self):
        # Una solicitud con cantidad NULL en la BD (no debería pasar, pero
        # defensivo) no debe tirar TypeError al sumar.
        self.assertEqual(self.disponible(5, [None, 2]), 3)

    def test_acepta_decimales(self):
        self.assertEqual(self.disponible(5.5, [1.5, 1.0]), 3.0)


class TestHallazgoAccion(unittest.TestCase):
    """_inc_hallazgo_accion: el bug real de Daniel -- todas las filas de
    'Revisar diferencias' tenían cursor de mano (CSS genérico) pero solo
    las que traían `ids` tenían onclick. 'falta_registrar' SIEMPRE es
    accionable (justo nace de no tener incidencia todavía, por eso nunca
    trae `ids`)."""

    @classmethod
    def setUpClass(cls):
        cls.accion = staticmethod(_cargar_funciones("_inc_hallazgo_accion")["_inc_hallazgo_accion"])

    def test_falta_registrar_siempre_es_registrar_aunque_no_tenga_ids(self):
        self.assertEqual(self.accion({"tipo": "falta_registrar"}), "registrar")

    def test_falta_registrar_es_registrar_incluso_con_ids_presente(self):
        # No debería pasar en la práctica (falta_registrar no construye
        # `ids`), pero el tipo manda sobre la presencia de ids.
        self.assertEqual(self.accion({"tipo": "falta_registrar", "ids": [1]}), "registrar")

    def test_dif_erp_con_ids_es_ver(self):
        self.assertEqual(self.accion({"tipo": "dif_erp", "ids": [10, 11]}), "ver")

    def test_sin_motivo_con_ids_es_ver(self):
        self.assertEqual(self.accion({"tipo": "sin_motivo", "ids": [5]}), "ver")

    def test_fuera_de_bodega_con_ids_es_ver(self):
        self.assertEqual(self.accion({"tipo": "fuera_de_bodega", "ids": [7]}), "ver")

    def test_dif_erp_sin_ids_es_registrar(self):
        # 🔧 2026-09-26 (Daniel, en vivo: "los hallazgos dif_erp sin ids...
        # deben poder registrarse"): un SKU que el ERP reporta pero que
        # nunca declaramos ahora SÍ abre el alta prellenada.
        self.assertEqual(self.accion({"tipo": "dif_erp", "ids": []}), "registrar")

    def test_dif_erp_sin_clave_ids_es_registrar(self):
        self.assertEqual(self.accion({"tipo": "dif_erp"}), "registrar")

    def test_no_en_erp_sin_ids_es_registrar(self):
        self.assertEqual(self.accion({"tipo": "no_en_erp", "ids": []}), "registrar")

    def test_tipo_desconocido_sin_ids_no_tiene_accion(self):
        self.assertIsNone(self.accion({"tipo": "otra-cosa"}))


class TestHallazgoFaltaRegistrar(unittest.TestCase):
    """_inc_hallazgo_falta_registrar: los datos para PRELLENAR el alta
    (SKU, descripción, UA, ubicación, cantidad) -- decisión de Daniel
    confirmada por AskUserQuestion el 26-sep."""

    @classmethod
    def setUpClass(cls):
        amb = _cargar_funciones("_inc_hallazgo_falta_registrar")
        cls.armar = staticmethod(amb["_inc_hallazgo_falta_registrar"])

    def test_trae_sku_descripcion_ubicacion_y_ua(self):
        h = self.armar("UA1007933", {"codigo": "TWMA305-140", "descripcion": "Acrílico frontal",
                                      "ubicacion": "A-12-03", "stFisico": 2})
        self.assertEqual(h["tipo"], "falta_registrar")
        self.assertEqual(h["ua"], "UA1007933")
        self.assertEqual(h["sku"], "TWMA305-140")
        self.assertEqual(h["descripcion"], "Acrílico frontal")
        self.assertEqual(h["ubicacion"], "A-12-03")
        self.assertEqual(h["cantidad"], 2)
        self.assertIn("BODEGA", h["detalle"])  # usa INC_BODEGA_WMS en el texto

    def test_stfisico_ausente_cae_a_cantidad_1(self):
        h = self.armar("UA1", {"codigo": "X", "descripcion": "Y"})
        self.assertEqual(h["cantidad"], 1)

    def test_stfisico_como_texto_se_convierte(self):
        h = self.armar("UA1", {"codigo": "X", "stFisico": "3"})
        self.assertEqual(h["cantidad"], 3)

    def test_stfisico_invalido_cae_a_1(self):
        h = self.armar("UA1", {"codigo": "X", "stFisico": "no-es-numero"})
        self.assertEqual(h["cantidad"], 1)

    def test_stfisico_cero_o_negativo_nunca_baja_de_1(self):
        h = self.armar("UA1", {"codigo": "X", "stFisico": 0})
        self.assertEqual(h["cantidad"], 1)
        h2 = self.armar("UA1", {"codigo": "X", "stFisico": -3})
        self.assertEqual(h2["cantidad"], 1)

    def test_fila_wms_none_no_revienta(self):
        h = self.armar("UA1", None)
        self.assertEqual(h["cantidad"], 1)
        self.assertIsNone(h["sku"])
        self.assertIsNone(h["descripcion"])


class TestComparativaUa(unittest.TestCase):
    """🔧 2026-09-26 (Daniel, en vivo: "necesito hacer una comparativa de
    la ubicación y de la UA de Check"). _inc_comparativa_ua es la función
    PURA que decide el semáforo de la tabla principal: verde (coincide),
    ámbar (distinta), rojo (no está en Check), gris (sin datos)."""

    @classmethod
    def setUpClass(cls):
        amb = _cargar_funciones("_inc_comparativa_ua", "_checkwms_norm_ua")
        cls.comparar = staticmethod(amb["_inc_comparativa_ua"])

    def test_checkwms_no_disponible_marca_sin_datos_para_todas(self):
        filas = [{"recomendacion": "UA1", "ubicacion": "A-1"}]
        out = self.comparar(filas, None)
        self.assertEqual(out[0]["chk_estado"], "sin_datos")
        self.assertIsNone(out[0]["chk_ubicacion"])

    def test_ubicacion_coincide_es_verde(self):
        filas = [{"recomendacion": "UA1007933", "ubicacion": "A-12-03"}]
        wms = [{"ua": "ua1007933", "ubicacion": "a-12-03"}]
        out = self.comparar(filas, wms)
        self.assertEqual(out[0]["chk_estado"], "coincide")
        self.assertEqual(out[0]["chk_ubicacion"], "a-12-03")

    def test_ubicacion_distinta_es_ambar(self):
        filas = [{"recomendacion": "UA1", "ubicacion": "A-1"}]
        wms = [{"ua": "UA1", "ubicacion": "B-9"}]
        out = self.comparar(filas, wms)
        self.assertEqual(out[0]["chk_estado"], "distinta")

    def test_ua_no_esta_en_wms_es_no_esta(self):
        filas = [{"recomendacion": "UA9", "ubicacion": "A-1"}]
        wms = [{"ua": "UA1", "ubicacion": "A-1"}]
        out = self.comparar(filas, wms)
        self.assertEqual(out[0]["chk_estado"], "no_esta")

    def test_sin_ua_declarada_no_compara(self):
        filas = [{"recomendacion": "", "ubicacion": "A-1"}]
        out = self.comparar(filas, [{"ua": "UA1", "ubicacion": "A-1"}])
        self.assertEqual(out[0]["chk_estado"], "sin_ua")

    def test_ua_en_wms_pero_sin_ubicacion_nuestra(self):
        filas = [{"recomendacion": "UA1", "ubicacion": ""}]
        out = self.comparar(filas, [{"ua": "UA1", "ubicacion": "A-1"}])
        self.assertEqual(out[0]["chk_estado"], "sin_ubicacion_nuestra")

    def test_no_muta_la_lista_de_wms_ni_falla_con_ua_repetida(self):
        filas = [{"recomendacion": "UA1", "ubicacion": "A-1"}]
        wms = [{"ua": "UA1", "ubicacion": "A-1"}, {"ua": "UA1", "ubicacion": "Z-9"}]
        out = self.comparar(filas, wms)
        # Se queda con la PRIMERA ocurrencia -- no revienta con UA duplicada.
        self.assertEqual(out[0]["chk_estado"], "coincide")


class TestConciliacionExcluyeEliminadas(unittest.TestCase):
    """🔧 2026-09-26 (revisión post-merge, coordinador): el borrado lógico
    quedó incompleto -- la conciliación seguía leyendo `mant_incidencias`
    sin excluir `eliminada=1`, así que una incidencia eliminada seguía
    contando como "nuestra BD" y la diferencia con el ERP/WMS nunca
    desaparecía. Sin BD disponible en este entorno de test, se revisa el
    SQL ARMADO (código fuente de la función) en vez de ejecutarlo -- mismo
    criterio que el resto del archivo (funciones puras vía ast)."""

    def test_conciliacion_excluye_eliminadas_del_where(self):
        src = _fuente_de("mant_api_incidencias_conciliacion")
        self.assertIn("FROM mant_incidencias WHERE estado='abierta' AND COALESCE(eliminada,0)=0", src,
                       "la consulta de incidencias abiertas para la conciliación debe excluir eliminada=1")

    def test_borrar_incidencia_no_permite_reeliminar(self):
        src = _fuente_de("mant_api_incidencias_borrar")
        self.assertIn("COALESCE(eliminada,0)=0", src)

    def test_editar_incidencia_rechaza_eliminadas(self):
        src = _fuente_de("mant_api_incidencias_editar")
        self.assertIn('antes.get("eliminada")', src)

    def test_solicitar_repuesto_de_incidencia_excluye_eliminadas(self):
        src = _fuente_de("mant_api_incidencia_solicitar_repuesto")
        self.assertIn("COALESCE(eliminada,0)=0", src)

    def test_disponibles_repuesto_excluye_eliminadas(self):
        src = _fuente_de("mant_api_incidencias_disponibles_repuesto")
        self.assertIn("COALESCE(eliminada,0)=0", src)

    def test_deduplicar_registra_log_antes_de_borrar(self):
        # REGLA #5: aunque este endpoint SÍ hace hard delete (duplicados
        # exactos del seed), debe llamar _inc_log ANTES del DELETE.
        src = _fuente_de("mant_api_incidencias_deduplicar")
        idx_log = src.index("_inc_log(")
        idx_delete = src.index("DELETE FROM mant_incidencias")
        self.assertLess(idx_log, idx_delete,
                         "el log de auditoría debe escribirse ANTES del DELETE, no después")


class TestSentenceCase(unittest.TestCase):
    """🔧 2026-09-26 (Daniel, viendo producción: "capitaliza cada motivo
    con mayúscula inicial de oración -- no Title Case por palabra")."""

    @classmethod
    def setUpClass(cls):
        cls.f = staticmethod(_cargar_funciones("_sentence_case")["_sentence_case"])

    def test_sube_solo_la_primera_letra(self):
        self.assertEqual(self.f("falta acrílico derecho"), "Falta acrílico derecho")

    def test_no_es_title_case(self):
        # "Falta Acrílico Derecho" sería Title Case -- NO es lo pedido.
        out = self.f("falta acrílico Derecho del Lado")
        self.assertEqual(out, "Falta acrílico Derecho del Lado")

    def test_ya_capitalizado_no_cambia(self):
        self.assertEqual(self.f("Ya viene bien"), "Ya viene bien")

    def test_respeta_mayusculas_propias_como_ua_o_siglas(self):
        self.assertEqual(self.f("UA1007933 sin llegar"), "UA1007933 sin llegar")

    def test_texto_vacio_no_revienta(self):
        self.assertEqual(self.f(""), "")
        self.assertEqual(self.f(None), "")

    def test_ignora_espacios_iniciales_al_capitalizar(self):
        self.assertEqual(self.f("  falta pieza"), "  Falta pieza")


class TestCandadoSistemaYUaUnica(unittest.TestCase):
    """🔧 2026-09-26 (Daniel: "candados de datos del sistema... UA como
    identificador único real"). Sin BD disponible en este entorno de test,
    se revisa el SQL/lógica ARMADA (código fuente) -- mismo criterio que
    TestConciliacionExcluyeEliminadas."""

    def test_editar_bloquea_ua_ubicacion_sku_descripcion_si_ya_tenia_ua(self):
        src = _fuente_de("mant_api_incidencias_editar")
        self.assertIn('antes.get("recomendacion")', src)
        # Los 4 campos deben quedar fijados a los valores de `antes`, no a
        # lo que venga en el payload, cuando la incidencia ya tenía UA.
        for campo in ('nuevos["recomendacion"] = antes.get("recomendacion")',
                      'nuevos["ubicacion"] = antes.get("ubicacion")',
                      'nuevos["sku"] = antes.get("sku")',
                      'nuevos["descripcion"] = antes.get("descripcion")'):
            self.assertIn(campo, src)

    def test_editar_permite_fijar_ua_si_no_tenia(self):
        src = _fuente_de("mant_api_incidencias_editar")
        self.assertIn("_inc_ua_duplicada(ua_norm, excluir_id=iid)", src)

    def test_crear_rechaza_ua_duplicada_activa(self):
        src = _fuente_de("mant_api_incidencias_crear")
        self.assertIn("_inc_ua_duplicada(ua_norm)", src)
        self.assertIn("409", src)

    def test_ua_duplicada_excluye_eliminadas_y_se_puede_autoexcluir(self):
        src = _fuente_de("_inc_ua_duplicada")
        self.assertIn("COALESCE(eliminada,0)=0", src)
        self.assertIn("id<>%s", src)


if __name__ == "__main__":
    unittest.main()

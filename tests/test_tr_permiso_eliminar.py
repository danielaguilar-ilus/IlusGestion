"""Alison puede eliminar manifiestos y pedidos SIN trazabilidad comprometida,
controlable desde Usuarios y roles -- sin tocar código cada vez.

2026-08-19. Daniel: "necesito que Alison pueda eliminar pedidos y manifiestos
que no tengan la trazabilidad comprometida... para que ella limpie la parte
de los manifiestos" y, acto seguido: "dejalo modificable para controlar
siempre desde el front para modificar en los roles".

Antes de este cambio, tr_manifiesto_eliminar y tr_manifiestos_bulk_eliminar
exigian superadmin de forma INCONDICIONAL -- incluso para un manifiesto
vacio, recien creado por error, que nunca toco a un courier. Alison tenia
que pedirle a Daniel que borrara cada uno.

El arreglo sigue el MISMO patron que ya existe en el proyecto para este
caso exacto: 'cat_eliminar' (Catalogo de Productos, 2026-07-21, mismas
palabras de Daniel: "eliminarlo solamente para el superadministrador con
opciones a agregarlo en los roles"). Un flag granular y aditivo
(g.permissions['tr_eliminar']), nace en False para TODOS los roles hasta
que Daniel lo prende desde /admin/roles -- Transporte -> Bloqueos ->
"Eliminar manifiestos y pedidos sin trazabilidad".

Lo que el flag NUNCA puede hacer: saltarse la guarda de trazabilidad
comprometida. Un manifiesto que ya se subio a un courier o tiene prueba de
entrega firmada sigue siendo exclusivo de superadmin + escribir el
correlativo, tal cual desde el 2026-07-25 -- eso no cambia.

Correr con:  py -m unittest tests.test_tr_permiso_eliminar -v
(pytest NO esta instalado en el equipo de Daniel.)
"""
import ast
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
APP_PY = os.path.join(RAIZ, "app.py")
LC_PY = os.path.join(RAIZ, "logistica_cotizaciones.py")
TEMPLATE = os.path.join(RAIZ, "templates", "transporte", "manifiestos.html")

_FUENTE = None
_ARBOL = None


def _fuente():
    global _FUENTE
    if _FUENTE is None:
        with open(APP_PY, encoding="utf-8") as fh:
            _FUENTE = fh.read()
    return _FUENTE


def _arbol():
    global _ARBOL
    if _ARBOL is None:
        _ARBOL = ast.parse(_fuente())
    return _ARBOL


def _nodo(nombre):
    for n in ast.walk(_arbol()):
        if isinstance(n, ast.FunctionDef) and n.name == nombre:
            return n
    raise AssertionError("no existe la funcion %s() en app.py" % nombre)


def _cuerpo(nombre):
    return ast.unparse(_nodo(nombre))


def _modulo_leer(nombre):
    for n in ast.walk(_arbol()):
        if isinstance(n, (ast.Assign, ast.AnnAssign)):
            objetivo = n.targets[0] if isinstance(n, ast.Assign) else n.target
            if isinstance(objetivo, ast.Name) and objetivo.id == nombre:
                return ast.literal_eval(n.value)
    raise AssertionError("no existe la variable de modulo %s en app.py" % nombre)


def _leer(ruta_abs):
    with open(ruta_abs, encoding="utf-8") as fh:
        return fh.read()


# ══════════════════════════════════════════════════════════════════════
#  1. El flag existe y esta cableado en la matriz -- EJECUTABLE de verdad
# ══════════════════════════════════════════════════════════════════════
class TestElFlagExisteYNaceEnFalse(unittest.TestCase):
    """PERMS_KEYS y _legacy_permission_set son puros (sin BD) -- se
    extraen y se EJECUTAN de verdad, no solo se les mira el texto."""

    @classmethod
    def setUpClass(cls):
        ns = {}
        # PERMS_KEYS es una tupla literal a nivel de modulo.
        cls.PERMS_KEYS = _modulo_leer("PERMS_KEYS")
        # _empty_perms() y _legacy_permission_set() dependen de PERMS_KEYS
        # via closure de modulo -- se inyecta en el namespace de ejecucion.
        ns["PERMS_KEYS"] = cls.PERMS_KEYS
        exec(compile(ast.Module(body=[_nodo("_empty_perms")], type_ignores=[]),
                     "<app.py>", "exec"), ns)
        exec(compile(ast.Module(body=[_nodo("_legacy_permission_set")], type_ignores=[]),
                     "<app.py>", "exec"), ns)
        cls.legacy = staticmethod(ns["_legacy_permission_set"])

    def test_tr_eliminar_esta_en_perms_keys(self):
        self.assertIn("tr_eliminar", self.PERMS_KEYS)

    def test_superadmin_tiene_tr_eliminar_en_true(self):
        self.assertTrue(self.legacy("superadmin")["tr_eliminar"])

    def test_el_rol_transporte_NACE_en_false(self):
        """El caso real de Alison: el rol 'transporte' (el que ella tiene)
        no debe traer tr_eliminar prendido por defecto -- Daniel lo prende
        el mismo desde /admin/roles cuando quiera."""
        self.assertFalse(self.legacy("transporte")["tr_eliminar"])

    def test_ningun_rol_legacy_trae_tr_eliminar_en_true_salvo_superadmin(self):
        for rol in ("admin", "ejecutivo", "tecnico", "editor", "lector", "vendedor"):
            with self.subTest(rol=rol):
                self.assertFalse(self.legacy(rol)["tr_eliminar"],
                                 f"el rol {rol} no deberia traer tr_eliminar en True "
                                 f"por defecto -- es un flag aditivo que Daniel prende")


# ══════════════════════════════════════════════════════════════════════
#  2. La matriz /admin/roles queda cableada correctamente
# ══════════════════════════════════════════════════════════════════════
class TestLaMatrizDeRolesQuedaCableada(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.matrix = _modulo_leer("PERMISSIONS_MATRIX")
        cls.meta = _modulo_leer("PERMISSIONS_META")

    def test_transporte_tiene_la_accion_eliminar(self):
        self.assertIn("eliminar", self.matrix["transporte"]["acciones"])

    def test_la_meta_existe_y_es_tipo_bloqueo(self):
        """tipo='bloqueo' es lo que hace que /admin/roles lo agrupe en la
        seccion visual de acciones sensibles, no de submodulos -- mismo
        grupo donde vive 'Eliminar producto' del Catalogo."""
        meta = self.meta["transporte"]["eliminar"]
        self.assertEqual(meta["tipo"], "bloqueo")

    def test_el_label_es_legible_para_daniel_no_jerga(self):
        meta = self.meta["transporte"]["eliminar"]
        self.assertIn("Eliminar", meta["label"])

    def test_sigue_el_mismo_patron_que_cat_eliminar(self):
        """Ambos deben ser 'bloqueo', ambos aditivos -- consistencia entre
        los dos flags granulares de eliminar que existen en el proyecto."""
        self.assertEqual(
            self.meta["catalogo"]["eliminar"]["tipo"],
            self.meta["transporte"]["eliminar"]["tipo"])

    def test_build_perms_from_matrix_computa_el_flag_plano(self):
        src = _cuerpo("_build_perms_from_matrix")
        self.assertIn("base['tr_eliminar']", src)
        self.assertIn("tra.get('eliminar')", src.replace('"', "'"))


# ══════════════════════════════════════════════════════════════════════
#  3. tr_manifiesto_eliminar -- el candado quedo donde debe
# ══════════════════════════════════════════════════════════════════════
class TestBorradoIndividual(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.src = _cuerpo("tr_manifiesto_eliminar")
        cls.plano = cls.src.replace('"', "'")

    def test_ya_no_hay_un_403_incondicional_de_entrada(self):
        """El bug que se corrige: antes CUALQUIERA sin superadmin rebotaba
        aca mismo, sin importar si el manifiesto tenia actividad o no."""
        i_docstring_fin = self.src.index('"""', self.src.index('"""') + 3) + 3
        cuerpo_ejecutable = self.src[i_docstring_fin:]
        i_mysql = cuerpo_ejecutable.find("mysql_fetchone")
        i_403_temprano = cuerpo_ejecutable.find("superadmin")
        # Si aparece "superadmin" ANTES de la primera consulta a la BD,
        # es el 403 incondicional viejo que se supone que ya no existe.
        if i_403_temprano != -1:
            self.assertGreater(i_403_temprano, i_mysql,
                "sigue habiendo un chequeo de superadmin ANTES de cargar "
                "el manifiesto -- eso es el 403 incondicional que se quito")

    def test_reusa_lc_manifiesto_tiene_actividad_no_duplica_sql(self):
        self.assertIn("_lc_manifiesto_tiene_actividad(mid)", self.src)
        # Regresion: que NO haya vuelto la consulta inline duplicada.
        self.assertNotIn("tracking_number IS NOT NULL", self.src)

    def test_con_actividad_sigue_exigiendo_superadmin(self):
        i_tiene = self.src.index("if tiene_actividad:")
        i_fin_bloque = self.src.index("elif not (es_superadmin")
        bloque_activo = self.src[i_tiene:i_fin_bloque]
        self.assertIn("if not es_superadmin:", bloque_activo)
        self.assertIn("403", bloque_activo)

    def test_con_actividad_tr_eliminar_NO_aparece_como_alternativa(self):
        """El flag nuevo NUNCA debe poder saltarse la guarda de
        trazabilidad comprometida -- solo superadmin, sin OR con tr_eliminar."""
        i_tiene = self.src.index("if tiene_actividad:")
        i_fin_bloque = self.src.index("elif not (es_superadmin")
        bloque_activo = self.src[i_tiene:i_fin_bloque]
        self.assertNotIn("tr_eliminar", bloque_activo)

    def test_con_actividad_sigue_pidiendo_confirm_text_del_correlativo(self):
        self.assertIn("confirm_text", self.src)
        self.assertIn("requiere_confirmacion", self.src)

    def test_sin_actividad_acepta_superadmin_o_tr_eliminar(self):
        i_elif = self.src.index("elif not (es_superadmin")
        linea = self.src[i_elif:i_elif + 200]
        self.assertIn("tr_eliminar", linea)

    def test_el_mensaje_de_error_sin_permiso_es_accionable(self):
        """REGLA #4: nada de codigos internos -- le dice a Alison
        exactamente que pedir y donde."""
        self.assertIn("Usuarios y roles", self.src)

    def test_sigue_liberando_los_items_a_pendiente(self):
        """H5 (2026-07-28) no se toco -- el soft-delete sigue soltando
        transport_manifest_items para que los documentos vuelvan a
        Pendiente."""
        self.assertIn("DELETE FROM transport_manifest_items WHERE manifest_id=", self.src)

    def test_sigue_siendo_soft_delete_nunca_drop_de_la_fila(self):
        """REGLA #5."""
        self.assertIn("eliminado=1", self.plano)
        self.assertNotIn("DELETE FROM transport_manifests", self.src)

    def test_sigue_dejando_rastro_en_la_trazabilidad(self):
        self.assertIn("_tr_log(", self.src)


# ══════════════════════════════════════════════════════════════════════
#  4. tr_manifiestos_bulk_eliminar -- mismo criterio, sin abrir un atajo
# ══════════════════════════════════════════════════════════════════════
class TestBorradoMasivo(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.src = _cuerpo("tr_manifiestos_bulk_eliminar")

    def test_acepta_superadmin_o_tr_eliminar_de_entrada(self):
        i_gate = self.src.index("if not bool(")
        linea_gate = self.src[i_gate:i_gate + 200]
        self.assertIn("superadmin", linea_gate)
        self.assertIn("tr_eliminar", linea_gate)

    def test_reusa_lc_manifiesto_tiene_actividad_no_duplica_sql(self):
        self.assertIn("_lc_manifiesto_tiene_actividad(", self.src)
        self.assertNotIn("tracking_number IS NOT NULL", self.src)

    def test_la_omision_de_activos_es_incondicional_al_rol(self):
        """Ni superadmin ni tr_eliminar pueden forzar el borrado masivo de
        un manifiesto activo -- se omite siempre, sin excepcion, y el
        resultado avisa cuantos quedaron afuera para revisarlos uno a uno."""
        i_loop = self.src.index("for r in rows:")
        cuerpo_loop = self.src[i_loop:i_loop + 400]
        self.assertIn("omitidos", cuerpo_loop)
        # El OR con el rol NO puede vivir dentro del loop -- si aparece
        # "tr_eliminar" o "superadmin" aca, alguien le agrego una excepcion
        # a la guarda de actividad, que es justo lo que no debe pasar.
        self.assertNotIn("superadmin", cuerpo_loop)
        self.assertNotIn("tr_eliminar", cuerpo_loop)

    def test_el_mensaje_de_error_sin_permiso_es_accionable(self):
        self.assertIn("Usuarios y roles", self.src)

    def test_sigue_topado_en_500_por_operacion(self):
        self.assertIn("500", self.src)

    def test_sigue_siendo_soft_delete(self):
        self.assertIn("eliminado=1", self.src.replace('"', "'"))


# ══════════════════════════════════════════════════════════════════════
#  5. La guarda de agregar/quitar items usa el MISMO motor -- consistencia
# ══════════════════════════════════════════════════════════════════════
class TestConsistenciaConLaGuardaDeItems(unittest.TestCase):
    """_tr_manifiesto_guard_actividad ya reusaba _lc_manifiesto_tiene_actividad
    antes de este cambio. Ahora los TRES puntos que deciden si un manifiesto
    'ya tiene actividad real' (guardar item, borrar 1, borrar N) llaman a la
    MISMA funcion -- si el criterio cambia algun dia, cambia en un solo
    lugar y los tres quedan sincronizados."""

    def test_la_guarda_de_items_tambien_la_usa(self):
        src = _cuerpo("_tr_manifiesto_guard_actividad")
        self.assertIn("_lc_manifiesto_tiene_actividad(mid)", src)

    def test_solo_existe_una_definicion_de_la_consulta_de_actividad(self):
        lc_src = _leer(LC_PY)
        self.assertEqual(
            lc_src.count("manifest_id=%s AND (tracking_number IS NOT NULL"), 1,
            "la consulta de actividad debe vivir en un unico lugar "
            "(_lc_manifiesto_tiene_actividad)")


# ══════════════════════════════════════════════════════════════════════
#  6. El frontend muestra los controles a quien SI puede usarlos
# ══════════════════════════════════════════════════════════════════════
class TestElFrontendMuestraLosControlesCorrectos(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.html = _leer(TEMPLATE)

    def test_la_columna_de_checkbox_del_encabezado_se_abre(self):
        self.assertIn("is_superadmin or permissions.tr_eliminar", self.html)

    def test_hay_al_menos_4_puntos_abiertos_a_tr_eliminar(self):
        """Encabezado de checkbox, checkbox por fila, link 'Eliminar
        manifiesto', y el contenedor de la barra flotante de seleccion --
        los 4 tienen que abrirse juntos o el flujo queda a medias (ej. el
        checkbox aparece pero no hay donde pintar la barra de accion)."""
        self.assertGreaterEqual(
            self.html.count("is_superadmin or permissions.tr_eliminar"), 4)

    def test_el_contenedor_de_la_barra_de_seleccion_tambien_se_abre(self):
        i = self.html.index('id="manBarraSeleccion"')
        contexto_antes = self.html[max(0, i - 250):i]
        self.assertIn("permissions.tr_eliminar", contexto_antes)

    def test_revisar_finanzas_sigue_exclusivo_de_superadmin(self):
        """REGLA #4.2: esta es una feature DISTINTA (auditoria financiera,
        2026-08-05) -- no se toca ni se amplia con el cambio de hoy."""
        i = self.html.index("btnAuditFin")
        contexto_antes = self.html[max(0, i - 400):i]
        self.assertIn("is_superadmin", contexto_antes)
        self.assertNotIn("tr_eliminar", contexto_antes)

    def test_no_quedo_ningun_confirm_nativo_nuevo(self):
        """REGLA #1 -- el flujo existente ya usaba ilusConfirm/ilusPrompt,
        no se toco el JS, pero se verifica que el HTML tampoco sume nada."""
        self.assertNotIn("onclick=\"confirm(", self.html)


if __name__ == "__main__":
    unittest.main(verbosity=2)

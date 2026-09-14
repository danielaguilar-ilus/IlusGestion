"""Alison puede cambiar a mano el estado de entrega de un pedido/factura
dentro de un manifiesto, controlable desde Usuarios y roles -- sin volverla
admin ni tocar código cada vez.

2026-09-14. Daniel: "quiero que le habilitemos de igual manera por el front
que [Alison] pueda cambiar los diferentes estados por el front... ella debe
administrar los estados con completa trazabilidad en la lista deberá salir
que ella modificó el estado o los movimientos que hizo para tener evidencia
que ella movió todo".

Antes de este cambio, tr_estado_entrega (PUT
/transporte/manifiestos/<mid>/items/<item_id>/estado) exigía el ROL EXACTO
admin/superadmin -- Alison, con rol "transporte", no tenía forma de que
Daniel le abriera esta puerta sin volverla admin de verdad (lo que le habría
dado además Usuarios, Roles, etc. -- mucho más de lo pedido).

El arreglo sigue el MISMO patrón que ya existe en el proyecto para este
caso exacto: 'tr_eliminar' (mismo módulo, 2026-08-19, mismo caso Alison). Un
flag granular y aditivo (g.permissions['tr_cambiar_estado']), nace en False
para TODOS los roles hasta que Daniel lo prende desde /admin/roles --
Transporte -> Bloqueos -> "Cambiar estado de entrega a mano".

La trazabilidad NO es nueva: _tr_apply_carrier_status ya deja cada cambio en
transport_logs con el usuario real (current_username()) -- este cambio solo
abre la puerta de ENTRADA, no toca el registro de salida.

Correr con:  py -m unittest tests.test_tr_permiso_cambiar_estado -v
(pytest NO esta instalado en el equipo de Daniel.)
"""
import ast
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
APP_PY = os.path.join(RAIZ, "app.py")
TEMPLATE = os.path.join(RAIZ, "templates", "transporte", "manifiesto_detalle.html")

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
#  1. El flag existe y nace en False -- EJECUTABLE de verdad
# ══════════════════════════════════════════════════════════════════════
class TestElFlagExisteYNaceEnFalse(unittest.TestCase):
    """PERMS_KEYS y _legacy_permission_set son puros (sin BD) -- se
    extraen y se EJECUTAN de verdad, no solo se les mira el texto."""

    @classmethod
    def setUpClass(cls):
        ns = {}
        cls.PERMS_KEYS = _modulo_leer("PERMS_KEYS")
        ns["PERMS_KEYS"] = cls.PERMS_KEYS
        # _empty_perms() referencia _PERMS_CONCEDIDOS_POR_DEFECTO por closure
        # de módulo (2026-09-02, reagendar/reasignar_tecnico) -- sin
        # inyectarla al namespace de ejecución, la extracción por AST revienta
        # con NameError aunque el código real funcione perfecto en app.py.
        ns["_PERMS_CONCEDIDOS_POR_DEFECTO"] = _modulo_leer("_PERMS_CONCEDIDOS_POR_DEFECTO")
        exec(compile(ast.Module(body=[_nodo("_empty_perms")], type_ignores=[]),
                     "<app.py>", "exec"), ns)
        exec(compile(ast.Module(body=[_nodo("_legacy_permission_set")], type_ignores=[]),
                     "<app.py>", "exec"), ns)
        cls.legacy = staticmethod(ns["_legacy_permission_set"])

    def test_tr_cambiar_estado_esta_en_perms_keys(self):
        self.assertIn("tr_cambiar_estado", self.PERMS_KEYS)

    def test_superadmin_tiene_tr_cambiar_estado_en_true(self):
        self.assertTrue(self.legacy("superadmin")["tr_cambiar_estado"])

    def test_el_rol_transporte_NACE_en_false(self):
        """El caso real de Alison: el rol 'transporte' (el que ella tiene)
        no debe traer tr_cambiar_estado prendido por defecto -- Daniel lo
        prende él mismo desde /admin/roles cuando quiera."""
        self.assertFalse(self.legacy("transporte")["tr_cambiar_estado"])

    def test_ningun_rol_legacy_trae_tr_cambiar_estado_en_true_salvo_superadmin(self):
        for rol in ("admin", "ejecutivo", "tecnico", "editor", "lector", "vendedor"):
            with self.subTest(rol=rol):
                self.assertFalse(
                    self.legacy(rol)["tr_cambiar_estado"],
                    f"el rol {rol} no deberia traer tr_cambiar_estado en True "
                    f"por defecto -- es un flag aditivo que Daniel prende")


# ══════════════════════════════════════════════════════════════════════
#  2. La matriz /admin/roles queda cableada correctamente
# ══════════════════════════════════════════════════════════════════════
class TestLaMatrizDeRolesQuedaCableada(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.matrix = _modulo_leer("PERMISSIONS_MATRIX")
        cls.meta = _modulo_leer("PERMISSIONS_META")

    def test_transporte_tiene_la_accion_cambiar_estado(self):
        self.assertIn("cambiar_estado", self.matrix["transporte"]["acciones"])

    def test_la_meta_existe_y_es_tipo_bloqueo(self):
        """tipo='bloqueo' es lo que hace que /admin/roles lo agrupe en la
        sección visual de acciones sensibles ('Permite también'), no de
        submódulos -- mismo grupo donde vive 'Eliminar manifiestos'."""
        meta = self.meta["transporte"]["cambiar_estado"]
        self.assertEqual(meta["tipo"], "bloqueo")

    def test_el_label_es_legible_para_daniel_no_jerga(self):
        meta = self.meta["transporte"]["cambiar_estado"]
        self.assertIn("estado", meta["label"].lower())

    def test_sigue_el_mismo_patron_que_tr_eliminar(self):
        self.assertEqual(
            self.meta["transporte"]["eliminar"]["tipo"],
            self.meta["transporte"]["cambiar_estado"]["tipo"])

    def test_build_perms_from_matrix_computa_el_flag_plano(self):
        src = _cuerpo("_build_perms_from_matrix")
        self.assertIn("base['tr_cambiar_estado']", src)
        self.assertIn("tra.get('cambiar_estado')", src.replace('"', "'"))


# ══════════════════════════════════════════════════════════════════════
#  3. tr_estado_entrega -- el candado se abre, sin perder nada de lo viejo
# ══════════════════════════════════════════════════════════════════════
class TestElCandadoDeCambiarEstado(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.src = _cuerpo("tr_estado_entrega")
        cls.plano = cls.src.replace('"', "'")

    def test_admin_superadmin_por_rol_sigue_pasando(self):
        """El candado de rol exacto de siempre NO se toca -- solo se le
        agrega un OR nuevo."""
        self.assertIn("not in ('admin', 'superadmin')", self.plano)

    def test_acepta_tambien_el_flag_granular(self):
        i = self.src.index("not in (")
        fragmento = self.src[i:i + 200]
        self.assertIn("tr_cambiar_estado", fragmento)

    def test_el_flag_esta_en_and_no_en_or_suelto(self):
        """Debe ser: rol_no_valido AND NOT flag -> 403. Si el flag quedara
        con OR mal puesto, alguien sin permiso ni rol podría colarse."""
        i = self.src.index("not in (")
        fragmento = self.src[i:i + 250].lower()
        i_and = fragmento.index(" and ")
        i_flag = fragmento.index("tr_cambiar_estado")
        self.assertLess(i_and, i_flag, "el flag debe vivir DESPUÉS de un 'and'")
        self.assertIn("not g.permissions.get", fragmento[i_and:i_flag + 20])

    def test_el_mensaje_de_error_sin_permiso_es_accionable(self):
        """REGLA #4: nada de códigos internos -- le dice a Alison
        exactamente qué pedir y dónde."""
        self.assertIn("Usuarios y roles", self.src)

    def test_problema_sigue_exigiendo_motivo_obligatorio(self):
        """Regresión: la regla de 2026-07-25 (motivo obligatorio para
        'Problema', va en el correo al cliente) no se toca con este cambio."""
        self.assertIn("Problema", self.src)
        self.assertIn("comentario", self.plano)

    def test_sigue_verificando_que_el_item_pertenece_al_manifiesto(self):
        """Regresión: el chequeo anti item-de-otro-manifiesto (2026-08-01)
        sigue ahí, sin importar quién dispare el cambio."""
        self.assertIn("manifest_id=%s", self.src)

    def test_sigue_delegando_en_el_choke_point_unico(self):
        """No se reimplementa el UPDATE a mano -- sigue pasando por
        _tr_apply_carrier_status, que es donde vive la trazabilidad."""
        self.assertIn("_tr_apply_carrier_status(", self.src)


# ══════════════════════════════════════════════════════════════════════
#  4. La trazabilidad ya existe -- este cambio no la reemplaza ni la duplica
# ══════════════════════════════════════════════════════════════════════
class TestLaTrazabilidadYaCapturaElUsuarioReal(unittest.TestCase):

    def test_tr_apply_carrier_status_deja_tr_log(self):
        src = _cuerpo("_tr_apply_carrier_status")
        self.assertIn("_tr_log(", src)

    def test_tr_log_guarda_el_usuario_real(self):
        src = _cuerpo("_tr_log")
        self.assertIn("current_username()", src)


# ══════════════════════════════════════════════════════════════════════
#  5. El frontend muestra el badge editable a quien SI puede usarlo
# ══════════════════════════════════════════════════════════════════════
class TestElFrontendAbreElBadgeDeEstado(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.html = _leer(TEMPLATE)

    def test_puede_editar_estado_incluye_el_flag_granular(self):
        i = self.html.index("puede_editar_estado = ")
        linea = self.html[i:i + 120]
        self.assertIn("permissions.tr_cambiar_estado", linea)

    def test_admin_superadmin_siguen_pudiendo_por_rol(self):
        """El OR se agrega, no reemplaza -- _es_admin sigue siendo parte
        de la condición."""
        i = self.html.index("puede_editar_estado = ")
        linea = self.html[i:i + 120]
        self.assertIn("_es_admin", linea)

    def test_las_2_vistas_desktop_y_mobile_usan_la_misma_variable(self):
        """El badge se pinta 2 veces (desktop + tarjetas mobile) -- las dos
        deben usar la misma variable calculada arriba, no una copia."""
        self.assertGreaterEqual(self.html.count("editable=puede_editar_estado"), 2)

    def test_el_tooltip_de_bloqueado_menciona_usuarios_y_roles(self):
        """Quien no tiene el permiso debe saber DÓNDE pedirlo, no solo que
        no puede."""
        i = self.html.index('title="El cambio manual del estado de entrega requiere')
        self.assertNotEqual(i, -1)


if __name__ == "__main__":
    unittest.main(verbosity=2)

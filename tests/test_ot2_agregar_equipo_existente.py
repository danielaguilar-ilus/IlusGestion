"""Agregar UN equipo nuevo a una OT ya creada, fuera del flujo de
levantamiento (Daniel, 2026-09-02, caso Aaron: "Aaron también quiere
agregar un equipo nuevo a la OT-154").

EL HUECO REAL: el mismo día se agregó el permiso granular "Agregar equipos
a OT ya creada" (mant_equipos_agregar_libre) conectado a mant_lev_item_crear
-- pero ESE endpoint exige una fila en mant_levantamientos, y desde el
Paso 5a (12-ago) esa fila YA NO se crea para tipos correctiva/preventiva/
instalación/garantía/inspección (solo para tipo='levantamiento'). Para la
OT-2026-00154 (Mantención Correctiva, verificado en vivo contra producción)
esa fila nunca existió -- con el permiso prendido y un botón en pantalla,
ese camino habría dado 404 "Levantamiento no encontrado" de todas formas.

EL FIX: un endpoint nuevo, independiente de cualquier levantamiento, que
agrega el equipo directo a mant_visita_tareas replicando el MISMO INSERT
que ya usa ot2_api_crear (~línea 78250) para cada equipo al crear la OT.
Mismo candado: superadmin o permissions.mant_equipos_agregar_libre, y la
OT no puede estar en un estado terminal (cerrada/completada/cancelada/
anulada) -- mismo criterio ya acordado para el camino de levantamiento
por descubrimiento.

app.py tiene 90k+ líneas -- se extrae el cuerpo de la función por slicing
de texto, mismo patrón que el resto de los tests de este módulo, sin pagar
el costo de un ast.parse() completo.

Correr con:  py -m unittest tests.test_ot2_agregar_equipo_existente -v
"""
import os
import unittest

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _fuente_funcion(nombre):
    with open(os.path.join(BASE_DIR, "app.py"), encoding="utf-8", errors="ignore") as f:
        src = f.read()
    i = src.index(f"\ndef {nombre}(")
    j = src.index("\ndef ", i + 10)
    return src[i:j]


SRC = _fuente_funcion("ot2_api_agregar_equipo")
# El docstring de la función EXPLICA a propósito por qué ya no depende de
# mant_levantamientos/mant_lev_item_crear (el bug real que se corrigió) --
# un assertNotIn ingenuo sobre SRC crudo encuentra esas palabras ahí mismo,
# en la prosa, no en código ejecutable. Se recorta todo lo que hay ANTES
# del cierre del docstring para probar solo el CUERPO real de la función
# (mismo patrón ya usado en otros tests de este módulo para el problema
# análogo con comentarios `#`).
_fin_docstring = SRC.index('"""', SRC.index('"""') + 3) + 3
SRC_CUERPO = SRC[_fin_docstring:]

with open(os.path.join(BASE_DIR, "templates", "ot2", "detalle.html"),
          encoding="utf-8", errors="ignore") as _f:
    HTML_SRC = _f.read()


class TestElEndpointExiste(unittest.TestCase):
    def test_la_ruta_esta_registrada(self):
        with open(os.path.join(BASE_DIR, "app.py"), encoding="utf-8", errors="ignore") as f:
            app_src = f.read()
        i = app_src.index("def ot2_api_agregar_equipo(")
        decoradores = app_src[max(0, i - 200):i]
        self.assertIn('@app.route("/ot/api/<int:vid>/equipos", methods=["POST"])', decoradores)
        self.assertIn("@_mant_required", decoradores)


class TestNoDependeDeUnLevantamiento(unittest.TestCase):
    """El bug real: el camino anterior (mant_lev_item_crear) exigía una
    fila en mant_levantamientos que, para este tipo de OT, nunca existe."""

    def test_no_consulta_mant_levantamientos(self):
        self.assertNotIn("mant_levantamientos", SRC_CUERPO)

    def test_no_llama_a_mant_lev_item_crear(self):
        self.assertNotIn("mant_lev_item_crear", SRC_CUERPO)

    def test_lee_la_ot_directo_de_mant_visitas(self):
        self.assertIn("FROM mant_visitas WHERE id=%s", SRC)


class TestElCandadoDePermiso(unittest.TestCase):
    def test_exige_superadmin_o_el_permiso_de_rol(self):
        i = SRC.index('if not (bool(g.permissions.get("superadmin"))')
        fragmento = SRC[i:i + 250]
        self.assertIn('g.permissions.get("mant_equipos_agregar_libre")', fragmento)

    def test_reusa_el_mismo_flag_que_el_camino_de_levantamiento(self):
        # No se crea un permiso nuevo -- mismo flag ya sembrado en
        # /admin/roles el mismo día para el caso de descubrimiento.
        self.assertIn('"mant_equipos_agregar_libre"', SRC)

    def test_el_mensaje_de_permiso_denegado_dirige_a_usuarios_y_roles(self):
        self.assertIn("Usuarios y roles", SRC)
        self.assertIn("Agregar equipos a", SRC)


class TestElCandadoDeEstadoTerminal(unittest.TestCase):
    """Único candado adicional que Daniel pidió explícitamente."""

    def test_usa_el_mismo_set_de_estados_terminales(self):
        self.assertIn('("cerrada", "completada", "cancelada", "anulada")', SRC)

    def test_pendiente_aprobacion_no_esta_en_la_lista_de_bloqueo(self):
        # Deliberadamente permitido -- Aaron lo necesita justo antes del
        # cierre final (mismo criterio que el camino de descubrimiento).
        i = SRC.index('("cerrada", "completada", "cancelada", "anulada")')
        fragmento = SRC[max(0, i - 50):i + 50]
        self.assertNotIn("pendiente_aprobacion", fragmento)


class TestValidacionesDeIntegridad(unittest.TestCase):
    def test_valida_que_el_equipo_pertenece_al_cliente_de_la_ot(self):
        self.assertIn('maq.get("cliente_id") != v.get("cliente_id")', SRC)

    def test_exige_plantilla_no_la_inventa_sola(self):
        # Daniel, 13-08: "no quiero nada automático, todo lo debe escoger
        # el usuario y si no escoge no lo debe dejar avanzar".
        self.assertIn("PLANTILLA_REQUERIDA", SRC)

    def test_rechaza_un_equipo_ya_agregado(self):
        self.assertIn("EQUIPO_YA_AGREGADO", SRC)

    def test_rechaza_checklist_vacio(self):
        self.assertIn("CHECKLIST_VACIO", SRC)


class TestElInsertReplicaElDeCreacion(unittest.TestCase):
    """Ningún checklist nuevo: el mismo INSERT que ya usa ot2_api_crear."""

    def test_usa_tarea_tipo_seguro(self):
        self.assertIn("_tarea_tipo_seguro(v.get(\"tipo\"))", SRC)

    def test_el_insert_trae_las_mismas_columnas_que_la_creacion(self):
        i = SRC.index("INSERT INTO mant_visita_tareas")
        fragmento = SRC[i:i + 500]
        for col in ("visita_id", "plantilla_id", "orden", "titulo", "descripcion",
                    "tipo", "maquina_id", "tipo_respuesta", "target_field",
                    "obligatoria", "requiere_foto", "estado_trabajo", "created_by"):
            self.assertIn(col, fragmento)

    def test_continua_el_orden_desde_el_maximo_existente(self):
        self.assertIn("COALESCE(MAX(orden),0)", SRC)

    def test_deja_auditoria_en_mant_logs(self):
        self.assertIn('_mant_log(', SRC)
        self.assertIn('"equipo_agregado"', SRC)


class TestElFrontendMuestraElBotonConElPermiso(unittest.TestCase):
    def test_el_boton_exige_permiso_o_superadmin(self):
        i = HTML_SRC.index("Agregar equipo</button>")
        fragmento = HTML_SRC[max(0, i - 600):i]
        self.assertIn("is_superadmin or permissions.mant_equipos_agregar_libre", fragmento)

    def test_el_boton_respeta_el_mismo_set_de_estados_terminales(self):
        i = HTML_SRC.index("Agregar equipo</button>")
        fragmento = HTML_SRC[max(0, i - 600):i]
        self.assertIn("('cerrada','completada','cancelada','anulada')", fragmento)

    def test_no_se_ofrece_en_trabajo_interno_sin_cliente(self):
        i = HTML_SRC.index("Agregar equipo</button>")
        fragmento = HTML_SRC[max(0, i - 600):i]
        self.assertIn("v.cliente_id", fragmento)

    def test_el_js_llama_al_endpoint_nuevo(self):
        self.assertIn("fetch('/ot/api/{{ v.id }}/equipos'", HTML_SRC)

    def test_no_usa_alertas_nativas(self):
        i = HTML_SRC.index("window.otdConfirmarAgregarEquipo")
        j = HTML_SRC.index("};", i)
        fragmento = HTML_SRC[i:j]
        self.assertNotIn("alert(", fragmento)
        self.assertNotIn("confirm(", fragmento)
        self.assertIn("ilusToast(", fragmento)

    def test_sugiere_plantilla_por_sku_sin_forzarla(self):
        # El humano siempre puede cambiar la sugerencia -- nunca se manda
        # el formulario sin que el usuario haya elegido explícitamente.
        self.assertIn("otdSugerirPlantillaEquipo", HTML_SRC)
        self.assertIn("plantillas/sugerida-por-sku", HTML_SRC)


if __name__ == "__main__":
    unittest.main(verbosity=2)

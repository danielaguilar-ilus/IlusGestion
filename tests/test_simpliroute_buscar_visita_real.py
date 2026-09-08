"""Búsqueda profunda de la visita real de un item SimpliRoute atascado
(2026-09-08).

Caso real que motivó esto: Daniel probó en vivo el fix de la ventana de
7->90 días (ver [[fix simpliroute ventana 90 dias]], PR #199) con dos
documentos concretos, BLV 22744 y FCV 11151, ambos "pending" desde hace
más de 40 días mientras el portal propio de Felca ya mostraba "Entrega
exitosa". El fix de la ventana restauró la visibilidad automática -- BLV
22744 se autorresolvió esa misma noche cuando el poller volvió a
revisarlo -- pero FCV 11151 siguió atascado: su entrega real ocurrió bajo
OTRA visita (creada por el propio courier) en una fecha (25-ago) que ni
la fecha del manifiesto, ni "hoy", ni los últimos 7 días desde hoy
(los 3 buckets que ya revisa _simpliroute_poll_batch) alcanzan a cubrir.

Este endpoint es la herramienta manual para ESE caso: barre día por día
desde la fecha del manifiesto hasta hoy (tope 90) buscando una visita más
avanzada con la misma reference, y aplica el mismo swap que ya sabe hacer
el poller automático -- sin agregarle ese barrido carísimo al ciclo de
cada 10 minutos.

Correr con:  py -m unittest tests.test_simpliroute_buscar_visita_real -v
"""
import ast
import unittest

APP_SRC = open("app.py", encoding="utf-8", errors="ignore").read()
JS_SRC = open("static/transporte_visitas_congeladas.js", encoding="utf-8", errors="ignore").read()
_ARBOL = ast.parse(APP_SRC)


def _fn(nombre):
    for n in ast.walk(_ARBOL):
        if isinstance(n, ast.FunctionDef) and n.name == nombre:
            return ast.unparse(n)
    raise AssertionError(f"no se encontro {nombre} en app.py")


class TestEndpointExisteYEstaProtegido(unittest.TestCase):
    def test_ruta_post_bajo_tr_required(self):
        i = APP_SRC.find("def tr_simpliroute_buscar_visita_real")
        self.assertGreater(i, 0)
        cab = APP_SRC[max(0, i - 250):i]
        self.assertIn("/simpliroute/buscar-visita-real", cab)
        self.assertIn('methods=["POST"]', cab)
        self.assertIn("@_tr_required", cab)

    def test_valida_item_id(self):
        f = _fn("tr_simpliroute_buscar_visita_real")
        self.assertIn("item_id inválido", f)


class TestNoEsElPollerAutomatico(unittest.TestCase):
    """No debe llamarse desde _simpliroute_poll_loop -- es carísimo (hasta
    90 requests) para correr cada 10 minutos; es una herramienta manual."""

    def test_el_poll_loop_no_la_invoca(self):
        f = _fn("_simpliroute_poll_loop")
        self.assertNotIn("tr_simpliroute_buscar_visita_real", f)
        self.assertNotIn("buscar_visita_real", f)


class TestBarridoAcotado(unittest.TestCase):
    """El barrido día por día tiene techo -- nunca 'todo el historial'."""

    def test_tope_90_dias(self):
        f = _fn("tr_simpliroute_buscar_visita_real")
        self.assertIn("90", f)
        self.assertIn("dias_totales", f)

    def test_arranca_desde_la_fecha_del_manifiesto(self):
        f = _fn("tr_simpliroute_buscar_visita_real")
        self.assertIn("fecha_manifiesto", f)

    def test_no_cuenta_la_visita_ya_guardada_como_hallazgo(self):
        # Sin este chequeo, el barrido "encontraría" la misma visita pending
        # que ya se tiene y reportaría un swap consigo misma.
        f = _fn("tr_simpliroute_buscar_visita_real")
        self.assertIn("str(v.get('id')) == vid_actual", f)


class TestReusaElMismoPatronDelPoller(unittest.TestCase):
    """No reimplementa la lógica de negocio -- reusa los mismos choke-points
    que ya usa _simpliroute_poll_batch para el mismo problema (Regla #4.2)."""

    def test_reusa_la_guarda_de_otro_manifiesto(self):
        f = _fn("tr_simpliroute_buscar_visita_real")
        self.assertIn("_sr_visita_pertenece_a_otro_manifiesto(", f)

    def test_reusa_el_normalizador_de_reference(self):
        f = _fn("tr_simpliroute_buscar_visita_real")
        self.assertIn("_sr_normalizar_reference(", f)
        # Busca tanto con prefijo de tido como pelada (mismo criterio que
        # el poller -- Felca a veces recrea la visita con el nudo pelado).
        self.assertIn("ref_con_tido", f)
        self.assertIn("ref_pelada", f)

    def test_reusa_el_traductor_de_estado(self):
        f = _fn("tr_simpliroute_buscar_visita_real")
        self.assertIn("_src.estado_ilus_from_visit(", f)

    def test_reusa_tr_apply_carrier_status_como_unico_choke_point(self):
        f = _fn("tr_simpliroute_buscar_visita_real")
        self.assertIn("_tr_apply_carrier_status(", f)
        self.assertIn("fuente='simpliroute'", f)

    def test_no_notifica_al_cliente_por_re_vinculacion(self):
        # Mismo criterio ya usado dentro de _simpliroute_poll_batch para
        # este caso exacto: "el cliente ya vivió su entrega, un correo
        # ahora solo confunde".
        f = _fn("tr_simpliroute_buscar_visita_real")
        self.assertIn("notify_cliente=False", f)

    def test_deja_auditoria(self):
        f = _fn("tr_simpliroute_buscar_visita_real")
        self.assertIn("_tr_log(", f)
        self.assertIn("búsqueda manual", f)


class TestUIBotonPorFila(unittest.TestCase):
    """El botón vive en el panel 'Visitas sin planificar' ya existente --
    no se construyó una pantalla nueva."""

    def test_boton_por_fila_llama_al_endpoint(self):
        self.assertIn("buscar-visita-real", JS_SRC)
        self.assertIn("buscarEntregaReal", JS_SRC)

    def test_deshabilita_el_boton_mientras_revisa(self):
        i = JS_SRC.find("async function buscarEntregaReal")
        self.assertGreater(i, 0)
        bloque = JS_SRC[i:i + 900]
        self.assertIn("btn.disabled = true", bloque)

    def test_usa_ilustoast_no_alert_nativo(self):
        i = JS_SRC.find("async function buscarEntregaReal")
        bloque = JS_SRC[i:i + 2000]
        self.assertIn("ilusToast(", bloque)
        self.assertNotIn("alert(", bloque.replace("ilusToast(", ""))


if __name__ == "__main__":
    unittest.main(verbosity=2)

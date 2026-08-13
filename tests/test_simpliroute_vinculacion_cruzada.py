"""FCV 11240 / manifiesto 17 vs 41: la misma visita SimpliRoute terminaba
vinculada a DOS manifiestos distintos, y el manifiesto viejo mostraba la
fecha/estado del despacho de HOY hecho en el manifiesto nuevo.

BUG REAL (2026-08-10). Daniel, por voz: "el manifiesto diecisiete tiene el
pedido... y está entregado a transporte el diez de agosto... no puede decir
entregado en el reporte en el manifiesto antiguo, que tiene dos días y
diecinueve horas, y decir que lo entregué hoy... como lo actualicé en el
otro manifiesto, entonces se está ligando la información."

CÓMO PASABA DE VERDAD (leyendo el código y los logs de Cloud Run):
  1. tr_asignar_a_manifiesto permite (con confirm_dup) agregar el MISMO
     commitment a un segundo manifiesto -- caso legítimo: la subida del
     manifiesto 17 nunca llegó a SimpliRoute de verdad, así que Daniel lo
     re-agregó al manifiesto 41, que sí subió bien (visit_id real).
  2. El item del manifiesto 17 quedó huérfano (sin simpliroute_visit_id).
  3. _simpliroute_reconciliar_huerfanos corre en CADA ciclo del poller y
     busca huérfanos por 'reference' (tido-nudo) en las visitas de
     SimpliRoute de hoy/ayer -- sin mirar si esa reference YA tenía una
     fila con visita activa en OTRO manifiesto. Encontró la visita real
     (la del manifiesto 41, entregada hoy) y se la pegó también al item
     del manifiesto 17.
  4. El poller normal, con ambos items apuntando al mismo visit_id, le
     copió a los dos el mismo estado_entrega/fecha -- el manifiesto 17
     "heredó" la entrega de hoy que en realidad es del 41.

DOS ARREGLOS:
  A. Causa raíz: _simpliroute_reconciliar_huerfanos ya NO considera huérfano
     a un item si su commitment tiene otra fila con visita activa en otro
     manifiesto -- esa es la vigente, y esta debe quedar honesta (huérfana)
     en vez de heredar datos ajenos.
  B. Corrección manual para vinculaciones cruzadas que ya existan (como la
     de FCV 11240 antes de este fix): nuevo endpoint que desvincula SOLO la
     fila local equivocada, SIN llamar a la API de SimpliRoute (esa visita
     es real y pertenece al otro manifiesto -- cancelarla la borraría ahí
     también).

Correr con:  py -m unittest tests.test_simpliroute_vinculacion_cruzada -v
"""
import ast
import unittest

APP_SRC = open("app.py", encoding="utf-8", errors="ignore").read()
MODAL_HTML = open("templates/transporte/_tracking_modals.html", encoding="utf-8", errors="ignore").read()
JS_SRC = open("static/transporte_manifiesto_detalle.js", encoding="utf-8", errors="ignore").read()
_ARBOL = ast.parse(APP_SRC)


def _fn(nombre):
    for n in ast.walk(_ARBOL):
        if isinstance(n, ast.FunctionDef) and n.name == nombre:
            return ast.unparse(n)
    raise AssertionError(f"no se encontro {nombre} en app.py")


class TestReconciliadorYaNoCruzaManifiestos(unittest.TestCase):
    """_simpliroute_reconciliar_huerfanos: la causa raíz."""

    def test_excluye_commitments_con_visita_activa_en_otro_manifiesto(self):
        f = _fn("_simpliroute_reconciliar_huerfanos")
        self.assertIn("NOT EXISTS", f)
        self.assertIn("mi2.manifest_id <> mi.manifest_id", f)
        self.assertIn("mi2.commitment_id = mi.commitment_id", f)
        self.assertIn("mi2.simpliroute_visit_id IS NOT NULL", f)

    def test_el_resto_del_filtro_original_sigue_intacto(self):
        """El fix agrega una condición, no reemplaza las que ya evitaban
        tocar items entregados o de hace mas de 7 dias."""
        f = _fn("_simpliroute_reconciliar_huerfanos")
        self.assertIn("'Entregado','Devolución'", f.replace('"', "'"))
        self.assertIn("INTERVAL 7 DAY", f)


class TestGuardaCompartida(unittest.TestCase):
    """_sr_visita_pertenece_a_otro_manifiesto: el freno que se reusa en los
    TRES sitios de _simpliroute_poll_batch donde el poller normal (no solo
    el reconciliador de huérfanos) puede "adoptar" una visita por reference
    -- ese es el mecanismo real que confirmaron los logs de producción:
    huerfanos.vinculados=0 en los 7 dias previos, o sea el reconciliador de
    huérfanos NUNCA disparó para este caso; el swap ocurrió en el poller
    principal, que no requiere que el item esté sin visit_id."""

    def _cargar(self):
        ns = {"mysql_fetchone": None}
        src = _fn("_sr_visita_pertenece_a_otro_manifiesto")
        exec(compile(ast.parse(src), "<guarda>", "exec"), ns)
        return ns["_sr_visita_pertenece_a_otro_manifiesto"]

    def test_false_sin_candidato(self):
        f = self._cargar()
        self.assertFalse(f(1, 2, None))
        self.assertFalse(f(1, 2, ""))

    def test_true_cuando_otra_fila_ya_tiene_esa_visita(self):
        ns = {}
        src = _fn("_sr_visita_pertenece_a_otro_manifiesto")
        calls = []

        def _fake_fetchone(sql, params):
            calls.append((sql, params))
            return {"id": 999}

        ns["mysql_fetchone"] = _fake_fetchone
        exec(compile(ast.parse(src), "<guarda>", "exec"), ns)
        resultado = ns["_sr_visita_pertenece_a_otro_manifiesto"](55, 41, "12345")
        self.assertTrue(resultado)
        self.assertEqual(len(calls), 1)
        sql, params = calls[0]
        self.assertIn("commitment_id=%s", sql)
        self.assertIn("manifest_id<>%s", sql)
        self.assertIn("simpliroute_visit_id=%s", sql)
        self.assertEqual(params, (55, 41, "12345"))

    def test_false_cuando_nadie_mas_la_tiene(self):
        ns = {"mysql_fetchone": lambda sql, params: None}
        src = _fn("_sr_visita_pertenece_a_otro_manifiesto")
        exec(compile(ast.parse(src), "<guarda>", "exec"), ns)
        self.assertFalse(ns["_sr_visita_pertenece_a_otro_manifiesto"](55, 41, "12345"))


class TestPollBatchUsaLaGuardaEnLosTresSitios(unittest.TestCase):
    """El bug real no pasaba por el reconciliador de huérfanos (ese exige
    simpliroute_visit_id NULL) sino por el poller normal, que reemplaza el
    visit_id de un item que YA tiene uno guardado, en tres puntos distintos
    del mismo ciclo (match directo por reference, fallback cuando el id
    guardado da 404, y el rescate de visitas "estancadas" de 7 días). Los
    tres tenían que quedar cubiertos -- tapar solo uno habría dejado el bug
    vivo por otra rama del mismo poller."""

    def test_manifest_id_se_selecciona_para_poder_comparar(self):
        f = _fn("_simpliroute_poll_batch")
        self.assertIn("mi.id AS item_id, mi.manifest_id, mi.commitment_id", f)

    def test_los_tres_sitios_llaman_a_la_guarda(self):
        f = _fn("_simpliroute_poll_batch")
        n = f.count("_sr_visita_pertenece_a_otro_manifiesto(")
        self.assertEqual(n, 3,
            f"se esperaban 3 usos de la guarda en _simpliroute_poll_batch, hay {n}")


class TestEndpointDeCorreccionManual(unittest.TestCase):
    """El endpoint que corrige una vinculación cruzada que ya haya ocurrido
    (como la de FCV 11240) sin tocar la visita real en SimpliRoute."""

    def test_existe_como_post(self):
        i = APP_SRC.find("def tr_item_simpliroute_desvincular_duplicado")
        self.assertGreater(i, 0)
        cab = APP_SRC[max(0, i - 250):i]
        self.assertIn("/simpliroute/desvincular-duplicado", cab)
        self.assertIn('methods=["POST"]', cab)
        self.assertIn("@_tr_required", cab)

    def test_no_llama_a_la_api_externa_de_simpliroute(self):
        """Diferencia critica con /cancelar: esa visita es real y pertenece
        al OTRO manifiesto -- llamar a EP_BULK_DELETE la borraria alla
        tambien. Este endpoint solo debe tocar MySQL local."""
        f = _fn("tr_item_simpliroute_desvincular_duplicado")
        self.assertNotIn("_simpliroute_request(", f)
        self.assertNotIn("EP_BULK_DELETE", f)

    def test_exige_que_la_visita_este_realmente_compartida(self):
        """Sin esta guarda, el boton podria usarse para abandonar un
        seguimiento real de un item que no tiene ningun duplicado -- para
        eso ya existe /cancelar."""
        f = _fn("tr_item_simpliroute_desvincular_duplicado")
        self.assertIn("mi2.simpliroute_visit_id=%s", f)
        self.assertIn("no está compartida", f)

    def test_limpia_los_campos_locales_de_simpliroute(self):
        f = _fn("tr_item_simpliroute_desvincular_duplicado")
        self.assertIn("simpliroute_visit_id=NULL", f)
        self.assertIn("simpliroute_tracking_id=NULL", f)

    def test_no_avisa_al_cliente(self):
        """Es una correccion de contabilidad interna de ILUS -- el cliente
        ya fue notificado por el despacho real en el otro manifiesto. Un
        segundo aviso de 'en preparacion' seria confuso y falso."""
        f = _fn("tr_item_simpliroute_desvincular_duplicado")
        self.assertIn("notify_cliente=False", f)

    def test_respeta_estados_terminales_como_cancelar(self):
        """Mismo cuidado que ya tiene /cancelar (SEV-4): si el item ya esta
        Entregado/Devolucion, no se revierte a 'En preparacion' solo por
        limpiar el vinculo."""
        f = _fn("tr_item_simpliroute_desvincular_duplicado")
        self.assertIn("ESTADOS_ENTREGA_TERMINALES", f)

    def test_deja_auditoria(self):
        f = _fn("tr_item_simpliroute_desvincular_duplicado")
        self.assertIn("_tr_log(", f)


class TestUIDeCorreccion(unittest.TestCase):
    """El boton debe existir en el modal de seguimiento y llamar al
    endpoint correcto -- Daniel no tiene forma de arreglar esto sin UI
    (REGLA #1: nada de curl/consola)."""

    def test_boton_en_el_modal(self):
        self.assertIn("srBtnDesvincular", MODAL_HTML)
        self.assertIn("srDesvincularDuplicado()", MODAL_HTML)

    def test_explica_que_no_toca_simpliroute(self):
        i = MODAL_HTML.find("srBtnDesvincular")
        bloque = MODAL_HTML[max(0, i - 500):i]
        self.assertIn("No llama a SimpliRoute", bloque)

    def test_js_llama_al_endpoint_correcto(self):
        i = JS_SRC.find("function srDesvincularDuplicado")
        self.assertGreater(i, 0)
        bloque = JS_SRC[i:i + 1500]
        self.assertIn("/simpliroute/desvincular-duplicado", bloque)
        self.assertIn("method: 'POST'", bloque)

    def test_usa_ilusconfirm_no_confirm_nativo(self):
        """REGLA #1 del proyecto."""
        i = JS_SRC.find("function srDesvincularDuplicado")
        bloque = JS_SRC[i:i + 600]
        self.assertIn("ilusConfirm(", bloque)
        self.assertNotIn("confirm(", bloque.replace("ilusConfirm(", ""))


if __name__ == "__main__":
    unittest.main(verbosity=2)

"""
Tests del polling FedEx con envíos MULTI-BULTO (_fedex_poll_batch).

EL BUG QUE ESTO PREVIENE
------------------------
Cuando FedEx CL no soporta MPS real, ILUS crea N envíos independientes (uno
por bulto) y guarda todos los tracking numbers en `piece_trackings_json`, pero
la columna `tracking_number` se queda solo con el PRIMERO.

El poller consultaba únicamente esa columna. Resultado: el despacho se marcaba
"Entregado" en cuanto llegaba el bulto 1, con los otros todavía en tránsito. Y
como "Entregado" es terminal, ningún poll posterior podía corregirlo — quedaba
mal para siempre salvo intervención manual.

CÓMO SE PRUEBA
--------------
`_fedex_poll_batch` habla con MySQL, con la Track API de FedEx y con la función
central de estados. Se extrae la función con AST y se le inyectan stubs de esas
tres dependencias, así se puede ejercitar la lógica REAL de agregación sin
tocar ninguna. No es un test de texto: corre el código de producción.

Correr:  python3 tests/test_fedex_multibulto_poll.py
"""
import ast
import json
import os
import sys
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, RAIZ)
APP_PY = os.path.join(RAIZ, "app.py")


_NODO = None


def _nodo_poll_batch():
    """Parsea app.py UNA vez para toda la suite. Son ~80K líneas: hacerlo por
    test costaba ~15s cada uno."""
    global _NODO
    if _NODO is None:
        with open(APP_PY, encoding="utf-8") as fh:
            for n in ast.walk(ast.parse(fh.read())):
                if isinstance(n, ast.FunctionDef) and n.name == "_fedex_poll_batch":
                    _NODO = n
                    break
        if _NODO is None:
            raise AssertionError("No se encontró _fedex_poll_batch() en app.py")
    return _NODO


def _cargar_poll_batch(rows, tn_status, aplicados, escrituras=None):
    """Extrae _fedex_poll_batch de app.py con las dependencias stubbeadas.

    rows       : lo que devolvería la query de candidatos
    tn_status  : {tracking_number: dict que devolvería la Track API}
    aplicados  : lista donde se registran las llamadas a _tr_apply_carrier_status
    escrituras : lista opcional donde se registran los UPDATE (mysql_execute)
    """
    nodo = _nodo_poll_batch()
    consultas = []

    def _track_lookup(tns):
        consultas.append(list(tns))
        return [tn_status[t] for t in tns if t in tn_status]

    def _apply(item_id, estado, fuente=None, payload=None, comentario=None,
               notify_cliente=True, **kw):
        aplicados.append({"item_id": item_id, "estado": estado,
                          "comentario": comentario, "payload": payload})
        return {"changed": True, "nuevo": estado, "anterior": "En ruta"}

    _escrituras = escrituras if escrituras is not None else []

    def _exec(sql, params=None):
        # Sin este stub, el UPDATE de bultos lanzaba NameError y el try/except
        # del código lo tragaba: los tests pasaban sin probar la persistencia.
        _escrituras.append({"sql": " ".join(str(sql).split()), "params": params})
        return 1

    ns = {
        "mysql_fetchall": lambda *a, **k: rows,
        "mysql_execute": _exec,
        "_fedex_track_lookup": _track_lookup,
        "_tr_apply_carrier_status": _apply,
        "json": json,
        # Credenciales presentes: si no, la función corta antes de empezar.
        "FEDEX_TRACK_CLIENT_ID": "x",
        "FEDEX_TRACK_CLIENT_SECRET": "y",
    }
    exec(compile(ast.Module(body=[nodo], type_ignores=[]), APP_PY, "exec"), ns)
    return ns["_fedex_poll_batch"], consultas


def _pieza(tn, estado, label=None):
    return {"tracking_number": tn, "estado_ilus": estado,
            "status_label": label or estado, "status_code": "XX",
            "eta": None, "last_event": label or estado, "scans": []}


class TestMultiBulto(unittest.TestCase):

    def test_no_marca_entregado_si_falta_un_bulto(self):
        """EL CASO DEL BUG. 3 bultos, solo el primero entregado.

        Antes: se marcaba "Entregado" (solo miraba el bulto 1).
        Ahora: NO puede quedar en Entregado mientras falten bultos.
        """
        rows = [{"id": 10, "tracking_number": "TN1",
                 "piece_trackings_json": json.dumps(["TN1", "TN2", "TN3"])}]
        tn_status = {
            "TN1": _pieza("TN1", "Entregado"),
            "TN2": _pieza("TN2", "En ruta"),
            "TN3": _pieza("TN3", "En ruta"),
        }
        aplicados = []
        poll, _ = _cargar_poll_batch(rows, tn_status, aplicados)
        res = poll(limit=25)

        self.assertTrue(res["ok"])
        self.assertEqual(len(aplicados), 1)
        self.assertNotEqual(
            aplicados[0]["estado"], "Entregado",
            "REGRESIÓN: se volvió a marcar Entregado con bultos en tránsito.",
        )
        self.assertEqual(aplicados[0]["estado"], "En ruta")
        self.assertIn("1 de 3", aplicados[0]["comentario"])

    def test_marca_entregado_solo_cuando_llegan_todos(self):
        rows = [{"id": 11, "tracking_number": "TN1",
                 "piece_trackings_json": json.dumps(["TN1", "TN2", "TN3"])}]
        tn_status = {t: _pieza(t, "Entregado") for t in ("TN1", "TN2", "TN3")}
        aplicados = []
        poll, _ = _cargar_poll_batch(rows, tn_status, aplicados)
        poll(limit=25)

        self.assertEqual(aplicados[0]["estado"], "Entregado")
        self.assertIn("3 bultos", aplicados[0]["comentario"])

    def test_un_bulto_fallido_no_deja_el_despacho_en_ruta_silenciosamente(self):
        """Si una pieza vuelve como 'Entrega fallida', ese estado gana sobre
        'En ruta': es la señal más urgente para el operador."""
        rows = [{"id": 12, "tracking_number": "TN1",
                 "piece_trackings_json": json.dumps(["TN1", "TN2"])}]
        tn_status = {
            "TN1": _pieza("TN1", "Entregado"),
            "TN2": _pieza("TN2", "Entrega fallida"),
        }
        aplicados = []
        poll, _ = _cargar_poll_batch(rows, tn_status, aplicados)
        poll(limit=25)

        self.assertEqual(aplicados[0]["estado"], "Entrega fallida")

    def test_un_bulto_devuelto_no_cierra_el_despacho_entero(self):
        """'Devolución' es terminal. Un solo bulto devuelto NO debe cerrar el
        despacho completo sin que lo mire una persona: queda en un estado no
        terminal para que siga siendo visible y gestionable.

        El comentario NO puede decir "a la espera del resto": ese bulto ya no
        va a llegar, hay que revisarlo.
        """
        rows = [{"id": 13, "tracking_number": "TN1",
                 "piece_trackings_json": json.dumps(["TN1", "TN2"])}]
        tn_status = {
            "TN1": _pieza("TN1", "Entregado"),
            "TN2": _pieza("TN2", "Devolución"),
        }
        aplicados = []
        poll, _ = _cargar_poll_batch(rows, tn_status, aplicados)
        poll(limit=25)

        self.assertNotIn(aplicados[0]["estado"], ("Entregado", "Devolución"))
        self.assertIn("devolución", aplicados[0]["comentario"].lower())
        self.assertNotIn("a la espera", aplicados[0]["comentario"].lower())

    def test_envio_devuelto_COMPLETO_no_se_reporta_como_En_ruta(self):
        """BUG REAL encontrado en revisión adversarial (2026-08-01).

        FedEx devuelve el envío ENTERO (códigos CA/RS → 'Devolución') y ningún
        bulto llegó. Antes: ninguna pieza calzaba con la lista de prioridad, el
        fallback dejaba "En ruta" y el cliente recibía "tu pedido va en camino"
        mientras el paquete volvía a origen. Peor todavía: al no haber ningún
        bulto entregado tampoco se marcaba parcial, así que la alerta nunca lo
        veía — invisible para el cliente Y para el negocio.

        Ahora se reporta como lo que es: Devolución.
        """
        rows = [{"id": 15, "tracking_number": "TN1",
                 "piece_trackings_json": json.dumps(["TN1", "TN2", "TN3"])}]
        tn_status = {t: _pieza(t, "Devolución") for t in ("TN1", "TN2", "TN3")}
        aplicados = []
        poll, _ = _cargar_poll_batch(rows, tn_status, aplicados)
        poll(limit=25)

        self.assertEqual(
            aplicados[0]["estado"], "Devolución",
            "Un envío que el courier devuelve entero no puede reportarse como "
            "otra cosa — menos como 'En ruta'.",
        )
        self.assertNotEqual(aplicados[0]["estado"], "En ruta")

    def test_devolucion_NO_se_cierra_con_bultos_sin_reportar(self):
        """BUG encontrado por Fable (revisión de lógica 2026-08-01).

        2 de 3 bultos reportan 'Devolución' y el tercero FedEx aún no lo
        indexa (None). Antes: como las piezas desconocidas se excluían del
        conteo, "todas las conocidas devueltas" bastaba para cerrar en
        'Devolución' — que es TERMINAL: si el bulto desconocido después se
        entregaba, nadie lo veía nunca más.

        Ahora: sin certeza de las N piezas, no se cierra en terminal.
        """
        rows = [{"id": 17, "tracking_number": "TN1",
                 "piece_trackings_json": json.dumps(["TN1", "TN2", "TN3"])}]
        tn_status = {
            "TN1": _pieza("TN1", "Devolución"),
            "TN2": _pieza("TN2", "Devolución"),
            # TN3 ausente: FedEx no lo reportó en este poll
        }
        aplicados = []
        poll, _ = _cargar_poll_batch(rows, tn_status, aplicados)
        poll(limit=25)

        self.assertNotEqual(
            aplicados[0]["estado"], "Devolución",
            "Se cerró en estado terminal con un bulto sin reportar.",
        )

    def test_devolucion_total_con_TODAS_reportadas_si_cierra(self):
        """El caso legítimo sigue funcionando: las 3 piezas confirmadas en
        Devolución → Devolución."""
        rows = [{"id": 18, "tracking_number": "TN1",
                 "piece_trackings_json": json.dumps(["TN1", "TN2", "TN3"])}]
        tn_status = {t: _pieza(t, "Devolución") for t in ("TN1", "TN2", "TN3")}
        aplicados = []
        poll, _ = _cargar_poll_batch(rows, tn_status, aplicados)
        poll(limit=25)
        self.assertEqual(aplicados[0]["estado"], "Devolución")

    def test_nunca_inventa_En_ruta_si_ninguna_pieza_va_en_ruta(self):
        """Parte entregada, parte devuelta, nada en movimiento. Decir "En ruta"
        es mentir: no se mueve nada. Se reporta 'Entrega fallida' — algo no se
        pudo entregar — que además no es terminal, así que sigue gestionable."""
        rows = [{"id": 16, "tracking_number": "TN1",
                 "piece_trackings_json": json.dumps(["TN1", "TN2"])}]
        tn_status = {
            "TN1": _pieza("TN1", "Entregado"),
            "TN2": _pieza("TN2", "Devolución"),
        }
        aplicados = []
        poll, _ = _cargar_poll_batch(rows, tn_status, aplicados)
        poll(limit=25)
        self.assertNotEqual(
            aplicados[0]["estado"], "En ruta",
            "Ninguna pieza está en ruta: reportarlo así es inventar.",
        )
        self.assertEqual(aplicados[0]["estado"], "Entrega fallida")


class TestPersistenciaDeBultos(unittest.TestCase):
    """El conteo persistido es lo que alimenta la alerta de envíos estancados.
    Si no se escribe (o se escribe de más), la alerta miente."""

    def _correr(self, rows, tn_status):
        escrituras, aplicados = [], []
        poll, _ = _cargar_poll_batch(rows, tn_status, aplicados, escrituras)
        poll(limit=25)
        return escrituras, aplicados

    def test_mono_bulto_NO_genera_escrituras_de_bultos(self):
        """Eran ~25-30 UPDATE inútiles cada 15 minutos sobre la mayoría de la
        tabla: un envío de un bulto no puede quedar a medias."""
        esc, _ = self._correr(
            [{"id": 50, "tracking_number": "TA", "piece_trackings_json": json.dumps(["TA"])}],
            {"TA": _pieza("TA", "En ruta")})
        self.assertEqual([e for e in esc if "bultos_total" in e["sql"]], [],
                         "Mono-bulto volvió a escribir el conteo de bultos.")

    def test_parcial_marca_la_fecha_sin_pisarla(self):
        esc, _ = self._correr(
            [{"id": 51, "tracking_number": "TA",
              "piece_trackings_json": json.dumps(["TA", "TB", "TC"])}],
            {"TA": _pieza("TA", "Entregado"), "TB": _pieza("TB", "En ruta"),
             "TC": _pieza("TC", "En ruta")})
        ups = [e for e in esc if "bultos_total" in e["sql"]]
        self.assertEqual(len(ups), 1)
        self.assertIn("COALESCE(parcial_desde, NOW())", ups[0]["sql"])
        self.assertEqual(ups[0]["params"], (3, 1, 51))

    def test_todo_entregado_limpia_el_parcial(self):
        esc, _ = self._correr(
            [{"id": 52, "tracking_number": "TA",
              "piece_trackings_json": json.dumps(["TA", "TB"])}],
            {"TA": _pieza("TA", "Entregado"), "TB": _pieza("TB", "Entregado")})
        ups = [e for e in esc if "bultos_total" in e["sql"]]
        self.assertEqual(len(ups), 1)
        self.assertIn("parcial_desde=NULL", ups[0]["sql"])

    def test_bulto_devuelto_con_otro_pendiente_SI_entra_en_la_alerta(self):
        """Sin esto, un envío con bultos devueltos y ninguno entregado no se
        marcaba parcial y quedaba invisible para la alerta."""
        esc, _ = self._correr(
            [{"id": 53, "tracking_number": "TA",
              "piece_trackings_json": json.dumps(["TA", "TB"])}],
            {"TA": _pieza("TA", "Devolución"), "TB": _pieza("TB", "En ruta")})
        ups = [e for e in esc if "bultos_total" in e["sql"]]
        self.assertEqual(len(ups), 1)
        self.assertIn("COALESCE(parcial_desde, NOW())", ups[0]["sql"])

    def test_envio_devuelto_completo_NO_se_marca_parcial(self):
        """Su estado ya dice 'Devolución': no es un parcial que haya que
        perseguir, y ensuciaría la alerta."""
        esc, apl = self._correr(
            [{"id": 54, "tracking_number": "TA",
              "piece_trackings_json": json.dumps(["TA", "TB"])}],
            {"TA": _pieza("TA", "Devolución"), "TB": _pieza("TB", "Devolución")})
        self.assertEqual(apl[0]["estado"], "Devolución")
        ups = [e for e in esc if "bultos_total" in e["sql"]]
        self.assertEqual(len(ups), 1)
        self.assertIn("parcial_desde=NULL", ups[0]["sql"])

    def test_el_payload_guarda_el_detalle_de_cada_bulto(self):
        """Para poder auditar después qué bulto iba dónde."""
        rows = [{"id": 14, "tracking_number": "TN1",
                 "piece_trackings_json": json.dumps(["TN1", "TN2"])}]
        tn_status = {"TN1": _pieza("TN1", "Entregado"),
                     "TN2": _pieza("TN2", "En ruta")}
        aplicados = []
        poll, _ = _cargar_poll_batch(rows, tn_status, aplicados)
        poll(limit=25)

        fx = aplicados[0]["payload"]["fedex"]
        self.assertEqual(fx["n_bultos"], 2)
        self.assertEqual(fx["n_entregados"], 1)
        self.assertEqual([p["tracking_number"] for p in fx["pieces"]], ["TN1", "TN2"])


class TestMonoBultoNoSeRompe(unittest.TestCase):
    """La enorme mayoría de los envíos son de un solo bulto: tienen que
    comportarse EXACTAMENTE igual que antes del fix."""

    def test_mono_bulto_entregado_marca_entregado(self):
        rows = [{"id": 20, "tracking_number": "TNX",
                 "piece_trackings_json": json.dumps(["TNX"])}]
        aplicados = []
        poll, _ = _cargar_poll_batch(
            rows, {"TNX": _pieza("TNX", "Entregado", "Entregado en recepción")}, aplicados)
        poll(limit=25)

        self.assertEqual(aplicados[0]["estado"], "Entregado")
        # El comentario sigue siendo el evento real de FedEx, no el texto de bultos.
        self.assertEqual(aplicados[0]["comentario"], "Entregado en recepción")

    def test_piece_trackings_json_nulo_se_trata_como_mono_bulto(self):
        """Filas viejas, anteriores a que existiera la columna."""
        rows = [{"id": 21, "tracking_number": "TNY", "piece_trackings_json": None}]
        aplicados = []
        poll, _ = _cargar_poll_batch(rows, {"TNY": _pieza("TNY", "Entregado")}, aplicados)
        poll(limit=25)
        self.assertEqual(aplicados[0]["estado"], "Entregado")

    def test_piece_trackings_json_corrupto_no_revienta(self):
        rows = [{"id": 22, "tracking_number": "TNZ",
                 "piece_trackings_json": "{esto no es json valido"}]
        aplicados = []
        poll, _ = _cargar_poll_batch(rows, {"TNZ": _pieza("TNZ", "En ruta")}, aplicados)
        res = poll(limit=25)
        self.assertTrue(res["ok"])
        self.assertEqual(aplicados[0]["estado"], "En ruta")

    def test_tracking_reasignado_a_mano_gana_sobre_un_json_viejo(self):
        """REGRESIÓN encontrada en revisión adversarial (2026-08-01).

        "Asignar OT" y la carga masiva de OTs por Excel reescriben SOLO la
        columna `tracking_number`, sin tocar `piece_trackings_json`. Cuando el
        poller empezó a priorizar el JSON, se quedó consultando los TN viejos
        (cancelados o reemplazados) para siempre, sin ver nunca el nuevo.

        Regla: si el tracking vigente no está en la lista de piezas, el JSON
        quedó obsoleto y manda la columna.
        """
        rows = [{"id": 24, "tracking_number": "TN_NUEVO",
                 "piece_trackings_json": json.dumps(["TN_VIEJO_1", "TN_VIEJO_2"])}]
        tn_status = {
            "TN_NUEVO":    _pieza("TN_NUEVO", "Entregado"),
            "TN_VIEJO_1":  _pieza("TN_VIEJO_1", "En ruta"),
            "TN_VIEJO_2":  _pieza("TN_VIEJO_2", "En ruta"),
        }
        aplicados = []
        poll, consultas = _cargar_poll_batch(rows, tn_status, aplicados)
        poll(limit=25)

        planos = [t for c in consultas for t in c]
        self.assertIn("TN_NUEVO", planos,
                      "El poller ni siquiera consultó el tracking vigente.")
        self.assertEqual(
            aplicados[0]["estado"], "Entregado",
            "El poller siguió el JSON viejo en vez del tracking reasignado.",
        )

    def test_si_el_tracking_de_la_columna_esta_en_el_json_se_respeta_el_json(self):
        """El caso normal de multi-bulto: la columna guarda el primer bulto y
        está dentro del JSON, así que el JSON manda (son todas las piezas)."""
        rows = [{"id": 25, "tracking_number": "TN1",
                 "piece_trackings_json": json.dumps(["TN1", "TN2"])}]
        tn_status = {"TN1": _pieza("TN1", "Entregado"),
                     "TN2": _pieza("TN2", "En ruta")}
        aplicados = []
        poll, _ = _cargar_poll_batch(rows, tn_status, aplicados)
        poll(limit=25)
        self.assertEqual(aplicados[0]["payload"]["fedex"]["n_bultos"], 2)
        self.assertNotEqual(aplicados[0]["estado"], "Entregado")

    def test_si_fedex_no_reporta_nada_no_se_toca_el_item(self):
        """Mejor no hacer nada que inventar un estado."""
        rows = [{"id": 23, "tracking_number": "TNQ",
                 "piece_trackings_json": json.dumps(["TNQ", "TNR"])}]
        aplicados = []
        poll, _ = _cargar_poll_batch(rows, {}, aplicados)
        res = poll(limit=25)
        self.assertTrue(res["ok"])
        self.assertEqual(aplicados, [])


class TestLimiteApiFedex(unittest.TestCase):

    def test_se_consultan_todas_las_piezas_en_tandas_de_30(self):
        """La Track API acepta 30 TNs por request y _fedex_track_lookup trunca
        en silencio. Con multi-bulto es fácil pasarse (30 items x 3 bultos = 90)
        y perder piezas sin enterarse: por eso el chunking es explícito."""
        rows, tn_status = [], {}
        for i in range(20):                       # 20 items x 3 bultos = 60 TNs
            tns = [f"T{i}_{k}" for k in range(3)]
            rows.append({"id": 100 + i, "tracking_number": tns[0],
                         "piece_trackings_json": json.dumps(tns)})
            for t in tns:
                tn_status[t] = _pieza(t, "En ruta")

        aplicados = []
        poll, consultas = _cargar_poll_batch(rows, tn_status, aplicados)
        res = poll(limit=25)

        self.assertEqual(res["tns_consultados"], 60)
        self.assertTrue(all(len(c) <= 30 for c in consultas),
                        f"Alguna tanda superó el límite de 30: {[len(c) for c in consultas]}")
        # Ninguna pieza puede quedar sin consultar.
        self.assertEqual(sorted(t for c in consultas for t in c), sorted(tn_status))
        self.assertEqual(len(aplicados), 20)

    def test_no_se_consulta_dos_veces_el_mismo_tracking(self):
        rows = [
            {"id": 30, "tracking_number": "TA", "piece_trackings_json": json.dumps(["TA", "TB"])},
            {"id": 31, "tracking_number": "TA", "piece_trackings_json": json.dumps(["TA", "TB"])},
        ]
        tn_status = {"TA": _pieza("TA", "En ruta"), "TB": _pieza("TB", "En ruta")}
        aplicados = []
        poll, consultas = _cargar_poll_batch(rows, tn_status, aplicados)
        res = poll(limit=25)

        planos = [t for c in consultas for t in c]
        self.assertEqual(len(planos), len(set(planos)), f"TNs repetidos: {planos}")
        self.assertEqual(res["tns_consultados"], 2)


class TestContratoDelLoop(unittest.TestCase):

    def test_polled_cuenta_items_para_que_el_daemon_no_corte_antes(self):
        """El loop daemon corta con `if polled == 0: break`. Si 'polled' pasara
        a contar tracking numbers en vez de items, el corte cambiaría de
        significado."""
        rows = [{"id": 40, "tracking_number": "TN1",
                 "piece_trackings_json": json.dumps(["TN1", "TN2", "TN3"])}]
        tn_status = {t: _pieza(t, "En ruta") for t in ("TN1", "TN2", "TN3")}
        poll, _ = _cargar_poll_batch(rows, tn_status, [])
        res = poll(limit=25)
        self.assertEqual(res["polled"], 1, "polled debe contar ITEMS, no piezas")

    def test_sin_candidatos_devuelve_polled_cero(self):
        poll, _ = _cargar_poll_batch([], {}, [])
        res = poll(limit=25)
        self.assertEqual(res["polled"], 0)
        self.assertTrue(res["ok"])


if __name__ == "__main__":
    unittest.main(verbosity=2)

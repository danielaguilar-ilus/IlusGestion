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


def _cargar_poll_batch(rows, tn_status, aplicados):
    """Extrae _fedex_poll_batch de app.py con las dependencias stubbeadas.

    rows       : lo que devolvería la query de candidatos
    tn_status  : {tracking_number: dict que devolvería la Track API}
    aplicados  : lista donde se registran las llamadas a _tr_apply_carrier_status
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

    ns = {
        "mysql_fetchall": lambda *a, **k: rows,
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
        terminal para que siga siendo visible y gestionable."""
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

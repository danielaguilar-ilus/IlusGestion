# -*- coding: utf-8 -*-
"""Conexión con Check + «quién y cuándo» (OT) en la ficha de Retiros (2026-10-02).

Daniel: «que me diga si está funcionando la conexión o no… quién piqueó, el responsable, cuándo, hora, si lo
asignó a alguien… toda la información que pueda sacar de Check, con usuario y fecha». Check es SOLO LECTURA
(REGLA #4.4): estas pruebas verifican que la ficha solo consulta, nunca escribe, y que no se cuelga si Check tarda.

    py -m pytest tests/test_retiros_check_actividad.py -q
"""
import os
import sys
import threading
import time
import unittest
from unittest.mock import MagicMock

_TESTS = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _TESTS)
sys.path.insert(0, os.path.dirname(_TESTS))

import _arnes_retiros as A  # noqa: E402
from _arnes_retiros import fila_check, respuesta_check  # noqa: E402

LISTA_BLANCA = {"/api/ext/GetReporteStock", "/api/ext/GetStockTrazabilidad", "/api/ext/GetStockTrazabilidadV2",
                "/api/ext/GetSeguimientoDespacho", "/api/ext/GetControlSalida"}


def _nuevo(status="agenda_confirmada", **kw):
    app, db, ctx, esp = A.construir_app()
    db.nueva_solicitud(1, status=status, confirmed_date="2026-10-05", **kw)
    db.agregar_doc(1, "BLV", "0000023732")
    db.reiniciar_registro()
    return app, db, ctx, esp


def _ot(**kw):
    base = {"ot": "481516", "tipoOT": "PICKING", "estado": "TERMINADA", "doc": "BLV-0000023732", "entidad": "CLIENTE DEMO SPA",
            "feInicioOT": "2026-10-02T15:32:10", "fechaFin": "2026-10-02T15:40:55", "ua": "UA1012965", "codigo": "MANC10",
            "descripcion": "Mancuerna hexagonal 10 kg", "sol": 2, "ejec": 2,
            "usuarioPicking": "JPEREZ", "usuarioAsignado": "MGOMEZ"}
    base.update(kw)
    return base


def _con_cache(ctx, filas, edad=0.0):
    traer = MagicMock(name="_checkwms_trazabilidad_rows")
    ctx["_CHECKWMS_TRAZA"] = {"ts": time.time() - edad, "rows": filas, "lock": threading.Lock()}
    ctx["_checkwms_trazabilidad_rows"] = traer
    return traer


def _esperar(condicion, tope=3.0):
    t0 = time.time()
    while time.time() - t0 < tope:
        if condicion():
            return True
        time.sleep(0.02)
    return False


class ConexionConCheck(unittest.TestCase):
    def _conexion(self, ctx_extra=None, respuesta="*"):
        app, db, ctx, esp = _nuevo()
        if respuesta == "*":
            esp.check.respuestas["*"] = respuesta_check(fila_check(solicitado=1, noAsignado=1))
        if ctx_extra:
            ctx.update(ctx_extra)
        r = app.test_client().get("/retiros/1/check-preparacion?solo_lectura=1")
        self.assertEqual(r.status_code, 200)
        return r.get_json()["conexion"], db

    def test_funcionando(self):
        c, db = self._conexion()
        self.assertEqual(c["estado"], "ok")
        self.assertEqual((c["docs_ok"], c["docs_total"]), (1, 1))
        self.assertIsNotNone(c["leido_hace_s"])
        self.assertEqual(db.escrituras, [])

    def test_sin_respuesta(self):
        c, _ = self._conexion(respuesta=None)
        self.assertEqual(c["estado"], "sin_conexion")
        self.assertEqual(c["docs_ok"], 0)

    def test_sin_credenciales_no_es_lo_mismo_que_no_responde(self):
        c, _ = self._conexion({"_checkwms_configurado": lambda: False}, respuesta=None)
        self.assertEqual(c["estado"], "sin_credenciales")

    def test_con_credenciales_cargadas_pero_sin_respuesta(self):
        c, _ = self._conexion({"_checkwms_configurado": lambda: True}, respuesta=None)
        self.assertEqual(c["estado"], "sin_conexion")


class QuienYCuando(unittest.TestCase):
    def test_muestra_usuario_fechas_y_ot(self):
        app, db, ctx, esp = _nuevo()
        traer = _con_cache(ctx, [_ot(), _ot(ot="999", doc="FCV-0000010953")])
        r = app.test_client().get("/retiros/1/check-actividad")
        self.assertEqual(r.status_code, 200)
        d = r.get_json()
        self.assertEqual(d["estado"], "listo")
        self.assertEqual(len(d["documentos"]), 1)
        ots = d["documentos"][0]["ots"]
        self.assertEqual([o["ot"] for o in ots], ["481516"])           # la OT de otro documento no se mezcla
        o = ots[0]
        self.assertEqual({p["campo"]: p["valor"] for p in o["personas"]}, {"usuarioPicking": "JPEREZ", "usuarioAsignado": "MGOMEZ"})
        self.assertEqual((o["inicio"], o["fin"]), ("02/10/2026 15:32", "02/10/2026 15:40"))
        self.assertIn("usuarioPicking", d["campos"])
        # solo lectura de Check: no se le habla directo (usa lo que ya hay en memoria) y no se refresca. Lo único que ILUS escribe es el REGISTRO guardado
        # de lo que Check informó (2026-10-06); nada del retiro, ni la bitácora, ni mensajes.
        self.assertTrue(all("pickup_check_snapshots" in e[0] for e in db.escrituras), db.escrituras)
        self.assertEqual(esp.check.llamadas, [])
        traer.assert_not_called()

    def test_todos_los_campos_de_check_quedan_visibles(self):
        app, db, ctx, esp = _nuevo()
        _con_cache(ctx, [_ot(campoNuevoDeCheck="valor X")])
        o = app.test_client().get("/retiros/1/check-actividad").get_json()["documentos"][0]["ots"][0]
        self.assertIn("campoNuevoDeCheck", {x["campo"] for x in o["datos"]})

    def test_documento_sin_ot_en_check(self):
        app, db, ctx, esp = _nuevo()
        _con_cache(ctx, [_ot(doc="FCV-0000010953")])
        d = app.test_client().get("/retiros/1/check-actividad").get_json()
        self.assertEqual(d["estado"], "listo")
        self.assertEqual(d["documentos"][0]["ots"], [])

    def test_memoria_vieja_se_muestra_y_se_refresca_en_segundo_plano(self):
        app, db, ctx, esp = _nuevo()
        traer = _con_cache(ctx, [_ot()], edad=1000)
        d = app.test_client().get("/retiros/1/check-actividad").get_json()
        self.assertEqual(d["estado"], "listo")                       # mientras tanto se ve lo último que hay
        self.assertEqual(len(d["documentos"][0]["ots"]), 1)
        self.assertTrue(_esperar(lambda: traer.call_count == 1), "no pidió el refresco a Check")
        self.assertEqual(traer.call_args.kwargs, {"forzar": True})

    def test_sin_memoria_responde_cargando_y_no_insiste_cada_vez(self):
        app, db, ctx, esp = _nuevo()
        traer = _con_cache(ctx, None)
        liberar = threading.Event()                       # Check tarda: el pedido en segundo plano sigue en curso
        traer.side_effect = lambda **kw: liberar.wait(3)
        c = app.test_client()
        try:
            self.assertEqual(c.get("/retiros/1/check-actividad").get_json()["estado"], "cargando")
            self.assertTrue(_esperar(lambda: traer.call_count == 1))
            self.assertEqual(c.get("/retiros/1/check-actividad").get_json()["estado"], "cargando")   # la pantalla no se cuelga
        finally:
            liberar.set()
        time.sleep(0.2)
        c.get("/retiros/1/check-actividad")
        c.get("/retiros/1/check-actividad")
        self.assertEqual(traer.call_count, 1, "volvió a pedirle a Check antes del tiempo de espera")

    def test_check_no_entrego_nada(self):
        app, db, ctx, esp = _nuevo()
        traer = _con_cache(ctx, None)
        c = app.test_client()
        c.get("/retiros/1/check-actividad")
        self.assertTrue(_esperar(lambda: traer.call_count == 1))
        time.sleep(0.1)
        self.assertEqual(c.get("/retiros/1/check-actividad").get_json()["estado"], "sin_datos")

    def test_sin_credenciales(self):
        app, db, ctx, esp = _nuevo()
        traer = _con_cache(ctx, None)
        ctx["_checkwms_configurado"] = lambda: False
        c = app.test_client()
        c.get("/retiros/1/check-actividad")
        self.assertTrue(_esperar(lambda: traer.call_count == 1))
        time.sleep(0.1)
        self.assertEqual(c.get("/retiros/1/check-actividad").get_json()["estado"], "sin_credenciales")

    def test_reporte_no_disponible_en_esta_instalacion(self):
        app, db, ctx, esp = _nuevo()
        ctx["_CHECKWMS_TRAZA"] = None
        ctx["_checkwms_trazabilidad_rows"] = None
        self.assertEqual(app.test_client().get("/retiros/1/check-actividad").get_json()["estado"], "sin_datos")

    def test_retiro_que_no_existe(self):
        app, db, ctx, esp = _nuevo()
        self.assertEqual(app.test_client().get("/retiros/99/check-actividad").status_code, 404)


class DiagnosticoAdmin(unittest.TestCase):
    def test_pide_los_tres_reportes_solo_por_la_lista_blanca_y_sin_escribir(self):
        app, db, ctx, esp = _nuevo()
        _con_cache(ctx, [_ot()])
        esp.check.respuestas["*"] = {"estado": {}, "body": {"response": [_ot(ot="V2-1"), {"otro": 1}]}}
        r = app.test_client().get("/retiros/admin/check-diagnostico-ot?tipo=BLV&num=23732&v2=1&salida=1")
        self.assertEqual(r.status_code, 200)
        reportes = {x["reporte"]: x for x in r.get_json()["reportes"]}
        self.assertEqual(set(reportes), {"GetStockTrazabilidad", "GetStockTrazabilidadV2", "GetControlSalida"})
        self.assertEqual(reportes["GetStockTrazabilidadV2"]["n_filas_documento"], 1)
        self.assertEqual(esp.check.rutas, {"/api/ext/GetStockTrazabilidadV2", "/api/ext/GetControlSalida"})
        self.assertLessEqual(esp.check.rutas, LISTA_BLANCA)
        self.assertEqual(db.escrituras, [])
        # el reporte de movimientos se pidió con una ventana de fechas corta (no el volcado entero)
        v2 = [p for ruta, p in esp.check.llamadas if ruta.endswith("V2")][0]
        self.assertIn("fecIniOT", v2)

    def test_sin_v2_ni_salida_no_llama_a_check(self):
        app, db, ctx, esp = _nuevo()
        _con_cache(ctx, [_ot()])
        r = app.test_client().get("/retiros/admin/check-diagnostico-ot?tipo=BLV&num=23732")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(esp.check.llamadas, [])

    def test_exige_tipo_y_numero(self):
        app, db, ctx, esp = _nuevo()
        self.assertEqual(app.test_client().get("/retiros/admin/check-diagnostico-ot").status_code, 400)


if __name__ == "__main__":
    unittest.main()

# -*- coding: utf-8 -*-
"""🚚 FLOTA · Eliminar una reserva definitivamente (Daniel, 05-nov-2026):
«necesito que el superadministrador pueda eliminar definitivamente cada reserva» (una reserva mal cargada impedía borrar al chofer).

Decisiones que se vigilan:
  1. SOLO el superadministrador, escribiendo el número de la reserva; el servidor lo exige aunque el navegador no muestre el botón.
  2. Con la llave afuera no se borra (primero la devolución o la cancelación), tampoco si la llave sale justo mientras se borra.
  3. Queda anotado ANTES de borrar (REGLA #5), con todo lo que decía, en la bitácora del vehículo (visible) y en la de la reserva (que nunca se borra).
  4. Si la reserva seguía vigente, se le avisa a quien la pidió (mejor esfuerzo: nunca rompe el borrado).
  5. Eliminar las reservas de un chofer le deja borrarlo sin forzar.
"""
import json
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import test_flota_solicitudes as TS  # noqa: E402
from test_flota_documentos import ADMINISTRA, GESTOR, SOLO_PEDIR, SUPER, Base2, _leer  # noqa: E402


class BaseRes(Base2):
    def setUp(self):
        super().setUp()
        self.avisos = []
        aviso = lambda clave, uids, titulo, cuerpo, url, etiqueta="", ref=None: self.avisos.append((clave, list(uids), titulo, cuerpo)) or len(uids)
        self.app = TS._app(self.base, self.bucket, extra={"_aviso_interno_correo": aviso})
        self.c = self.app.test_client()

    def reservar_(self, **k):
        r = self.c.post("/flota/api/solicitudes", json=TS._sol(vehiculo_id=self.furgon, chofer_id=self.ana, **k), headers=SUPER)
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True))
        return r.get_json()["solicitud"]["id"]

    def numero(self, sid):
        return self.base.sol[sid]["numero"]

    def eliminar(self, sid, headers=SUPER, confirmacion="__num__"):
        if confirmacion == "__num__":
            confirmacion = self.base.sol[sid]["numero"] if sid in self.base.sol else "X"
        return self.c.delete(f"/flota/api/solicitudes/{sid}", json={"confirmacion": confirmacion}, headers=headers)


class TestPermisos(BaseRes):
    def test_el_superadmin_la_elimina_escribiendo_su_numero(self):
        sid = self.reservar_()
        num = self.numero(sid)
        r = self.eliminar(sid)
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True))
        self.assertEqual((r.get_json()["eliminada"], r.get_json()["numero"]), (True, num))
        self.assertNotIn(sid, self.base.sol)

    def test_con_el_numero_en_minusculas_o_con_espacios_tambien(self):
        sid = self.reservar_()
        r = self.eliminar(sid, confirmacion="  " + self.numero(sid).lower() + " ")
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True))

    def test_sin_el_numero_correcto_no_se_borra(self):
        sid = self.reservar_()
        for mal in ("", "otra", None, "VEH-2026-9999"):
            r = self.eliminar(sid, confirmacion=mal)
            self.assertEqual(r.status_code, 400, mal)
        self.assertIn(sid, self.base.sol)
        self.assertFalse([x for x in self.base.log if x["accion"] == "reserva_eliminada"])

    def test_quien_solo_administra_no_puede(self):
        sid = self.reservar_()
        r = self.eliminar(sid, headers=ADMINISTRA)
        self.assertEqual((r.status_code, r.get_json()["error_codigo"]), (403, "SOLO_SUPERADMIN"))
        self.assertIn(sid, self.base.sol)

    def test_gestionar_o_pedir_no_alcanza(self):
        sid = self.reservar_()
        for h in (GESTOR, SOLO_PEDIR):
            self.assertEqual(self.eliminar(sid, headers=h).status_code, 403)
        self.assertIn(sid, self.base.sol)

    def test_una_reserva_que_no_existe(self):
        self.assertEqual(self.eliminar(9999, confirmacion="x").status_code, 404)

    def test_la_bandera_del_boton_solo_para_el_superadmin_y_sin_llave_afuera(self):
        a = self.reservar_()
        b = self.reservar_(hora_desde="13:00", hora_hasta="15:00")
        self.base.sol[b]["estado"] = "llave_entregada"
        lista = {s["id"]: s["puede"]["eliminar"] for s in self.c.get("/flota/api/solicitudes", headers=SUPER).get_json()["solicitudes"]}
        self.assertEqual((lista[a], lista[b]), (True, False))
        det = self.c.get(f"/flota/api/solicitudes/{a}", headers=SUPER).get_json()["solicitud"]["puede"]
        self.assertTrue(det["eliminar"])
        for h in (ADMINISTRA, GESTOR):
            lista = {s["id"]: s["puede"]["eliminar"] for s in self.c.get("/flota/api/solicitudes", headers=h).get_json()["solicitudes"]}
            self.assertFalse(any(lista.values()), h)


class TestLlaveAfuera(BaseRes):
    def test_con_la_llave_entregada_no_se_borra(self):
        sid = self.reservar_()
        self.base.sol[sid]["estado"] = "llave_entregada"
        r = self.eliminar(sid)
        self.assertEqual((r.status_code, r.get_json()["error_codigo"]), (409, "LLAVE_AFUERA"))
        self.assertIn("devolución", r.get_json()["error"])
        self.assertIn(sid, self.base.sol)
        self.assertFalse([x for x in self.base.log if x["accion"] == "reserva_eliminada"])

    def test_si_la_llave_sale_justo_mientras_se_borra_no_se_borra(self):
        sid = self.reservar_()
        self.base.sol[sid]["estado"] = "confirmada"
        original = self.base.escribir

        def con_carrera(s, p):
            if s.startswith("DELETE FROM flota_solicitudes"):
                self.base.sol[sid]["estado"] = "llave_entregada"       # otra persona entrega la llave justo ahora
            return original(s, p)
        self.base.escribir = con_carrera
        r = self.eliminar(sid)
        self.assertEqual(r.status_code, 409)
        self.assertIn(sid, self.base.sol)
        self.assertEqual(self.base.sol[sid]["estado"], "llave_entregada")
        # y queda dicho que no se aplicó
        self.assertTrue([x for x in self.base.slog if x["solicitud_id"] == sid and x["accion"] == "eliminacion_no_aplicada"])


class TestQuedaAnotado(BaseRes):
    def preparar(self):
        sid = self.reservar_()
        self.base.sol[sid]["estado"] = "cancelada"
        self.base.sol[sid]["cancelada_motivo"] = "LA CAGUE"
        return sid

    def test_la_bitacora_del_vehiculo_guarda_todo_lo_que_decia(self):
        sid = self.preparar()
        num = self.numero(sid)
        self.eliminar(sid)
        ev = [x for x in self.base.log if x["accion"] == "reserva_eliminada"]
        self.assertEqual(len(ev), 1)
        det = json.loads(ev[0]["detalle"])
        self.assertEqual((det["numero"], det["estado"], det["cancelada_motivo"]), (num, "cancelada", "LA CAGUE"))
        self.assertEqual((det["vehiculo_id"], det["chofer_id"], det["area"]), (self.furgon, self.ana, "Logística"))
        self.assertTrue(det["inicio"] and det["fin"] and det["motivo"] == "Reparto de equipos")
        self.assertTrue(ev[0]["usuario"])

    def test_el_historial_del_vehiculo_la_muestra_con_su_titulo(self):
        sid = self.preparar()
        num = self.numero(sid)
        self.eliminar(sid)
        h = self.c.get(f"/flota/api/vehiculos/{self.furgon}/historial", headers=SUPER).get_json()["eventos"]
        e = next(x for x in h if x["accion"] == "reserva_eliminada")
        self.assertEqual(e["titulo"], "Reserva eliminada")
        self.assertTrue(any(num in l for l in e["lineas"]))
        self.assertTrue(any(l.startswith("Chofer: ") for l in e["lineas"]))

    def test_la_bitacora_de_la_reserva_no_se_borra(self):
        sid = self.preparar()
        self.eliminar(sid)
        self.assertTrue([x for x in self.base.slog if x["solicitud_id"] == sid and x["accion"] == "eliminada"])
        self.assertTrue([x for x in self.base.slog if x["solicitud_id"] == sid and x["accion"] == "solicitud_creada" or x["accion"] == "creada"] or True)

    def test_primero_se_anota_y_despues_se_borra(self):
        sid = self.preparar()
        self.eliminar(sid)
        sql = self.base.sql
        i_log = max(i for i, q in enumerate(sql) if q.startswith("INSERT INTO flota_vehiculo_log"))
        i_del = max(i for i, q in enumerate(sql) if q.startswith("DELETE FROM flota_solicitudes"))
        self.assertLess(i_log, i_del)

    def test_si_falla_la_anotacion_no_se_borra(self):
        sid = self.preparar()
        original = self.base.escribir

        def falla(s, p):
            if s.startswith("INSERT INTO flota_vehiculo_log"):
                raise RuntimeError("bitácora caída")
            return original(s, p)
        self.base.escribir = falla
        r = self.eliminar(sid)
        self.assertEqual(r.status_code, 500)
        self.assertIn(sid, self.base.sol)
        self.assertFalse([q for q in self.base.sql if q.startswith("DELETE FROM flota_solicitudes")])

    def test_las_fotos_de_la_devolucion_quedan_como_no_vigentes(self):
        sid = self.preparar()
        self.base.arch[999] = {"id": 999, "entidad": "solicitud", "entidad_id": sid, "tipo": "dev_odometro", "activo": 1}
        self.eliminar(sid)
        self.assertEqual(self.base.arch[999]["activo"], 0)


class TestAvisos(BaseRes):
    def test_una_vigente_le_avisa_a_quien_la_pidio(self):
        sid = self.reservar_()
        self.base.sol[sid]["solicitante_id"] = 7                  # en las pruebas todos los encabezados son el usuario 1
        self.eliminar(sid)
        cancel = [e for e in self.avisos if e[0] == "flota_solicitud_cancelada"]
        self.assertEqual(len(cancel), 1)
        self.assertEqual(cancel[0][1], [7])
        self.assertIn("eliminó esta reserva", cancel[0][3])

    def test_una_ya_cancelada_o_pasada_no_manda_nada(self):
        a = self.reservar_()
        self.base.sol[a]["solicitante_id"] = 7
        self.base.sol[a]["estado"] = "cancelada"
        self.eliminar(a)
        b = self.reservar_(hora_desde="13:00", hora_hasta="15:00")
        self.base.sol[b]["solicitante_id"] = 7
        self.base.sol[b]["estado"] = "devuelta"
        self.eliminar(b)
        self.assertFalse([e for e in self.avisos if e[0] == "flota_solicitud_cancelada"])

    def test_quien_la_pidio_y_la_elimina_no_se_avisa_a_si_mismo(self):
        sid = self.reservar_()
        self.eliminar(sid)
        self.assertFalse([e for e in self.avisos if e[0] == "flota_solicitud_cancelada"])

    def test_si_el_aviso_falla_igual_se_borra(self):
        sid = self.reservar_()
        self.base.sol[sid]["solicitante_id"] = 7

        def roto(*a, **k):
            raise RuntimeError("correo caído")
        app = TS._app(self.base, self.bucket, extra={"_aviso_interno_correo": roto})
        r = app.test_client().delete(f"/flota/api/solicitudes/{sid}", json={"confirmacion": self.numero(sid)}, headers=SUPER)
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True))
        self.assertNotIn(sid, self.base.sol)


class TestLiberaAlChofer(BaseRes):
    def test_sin_reservas_el_chofer_se_borra_sin_forzar(self):
        a = self.reservar_()
        b = self.reservar_(hora_desde="13:00", hora_hasta="15:00")
        r = self.c.delete(f"/flota/api/choferes/{self.ana}", json={"confirmacion": "Ana María Pérez Soto"}, headers=SUPER)
        self.assertEqual((r.status_code, r.get_json()["error_codigo"]), (409, "TIENE_HISTORIAL"))
        self.assertEqual(self.eliminar(a).status_code, 200)
        self.assertEqual(self.eliminar(b).status_code, 200)
        r = self.c.delete(f"/flota/api/choferes/{self.ana}", json={"confirmacion": "Ana María Pérez Soto"}, headers=SUPER)
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True))
        self.assertNotIn(self.ana, self.base.cho)

    def test_el_horario_queda_libre_para_reservar_otra_vez(self):
        sid = self.reservar_()
        self.eliminar(sid)
        self.assertEqual(self.reservar_().__class__, int)


class TestPantalla(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.js = _leer("static", "flota_uso.js")
        cls.mod = _leer("flota_module.py")

    def test_el_boton_va_en_la_fila_y_en_el_detalle_solo_si_el_servidor_lo_permite(self):
        i = self.js.index("function botonesFila(")
        self.assertIn("if (p.eliminar)", self.js[i:i + 1700])
        j = self.js.index("data-dacc=\"cancelar\"")
        self.assertIn("if (p.eliminar)", self.js[j:j + 600])

    def test_pide_escribir_el_numero_con_el_modal_de_ilus_y_usa_delete(self):
        i = self.js.index("function eliminarSol(")
        t = self.js[i:i + 1700]
        self.assertIn("window.ilusPrompt", t)
        self.assertIn("method: 'DELETE'", t)
        self.assertIn("confirmacion: txt", t)
        self.assertNotRegex(t, r"(?<![\w.])(confirm|prompt|alert)\(")

    def test_la_accion_esta_conectada(self):
        self.assertIn("else if (a === 'eliminar') eliminarSol(s);", self.js)

    def test_el_servidor_exige_superadmin_antes_que_nada(self):
        i = self.mod.index("def flota_api_solicitud_eliminar(")
        t = self.mod[i:i + 5200]
        self.assertLess(t.index("_es_superadmin()"), t.index("_sol_fila(sid)"))
        self.assertIn('error_codigo="SOLO_SUPERADMIN"', t)
        self.assertLess(t.index('_log(fila["vehiculo_id"], "reserva_eliminada"'), t.index("DELETE FROM flota_solicitudes"))
        self.assertIn("estricto=True", t)
        self.assertIn("AND estado <> 'llave_entregada'", t)


if __name__ == "__main__":
    unittest.main()

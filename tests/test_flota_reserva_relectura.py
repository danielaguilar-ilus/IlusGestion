# -*- coding: utf-8 -*-
"""🚚 FLOTA · «agendó y le dio error, pero sí lo reservó» (Daniel, 05-nov-2026, con el caso de Víctor).

Causa (confirmada en los registros de producción: 6 respuestas 500 de POST /flota/api/solicitudes desde el 03-nov, 3 hoy, cada una seguida de
un 409 al reintentar): `_reservar` guarda por OTRA conexión (get_mysql) y la petición releía la fila con `mysql_fetchone`, que usa la conexión
del pool. Esa conexión puede traer abierto desde antes un snapshot REPEATABLE READ y NO ver la fila recién comiteada: devolvía None, la
respuesta decía «se guardó pero no se pudo leer» (500) y el correo a la gestión nunca salía.

Aquí el «pool ciego» imita eso: no ve ninguna reserva creada después de que abrió su snapshot.

Correr:  python -m pytest tests/test_flota_reserva_relectura.py -q -p no:warnings
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from test_flota_solicitudes import Falsa, SUPER, _app, _leer, _sol  # noqa: E402

MODULO = _leer("flota_module.py")
JS_USO = _leer("static", "flota_uso.js")


class BaseCiega(unittest.TestCase):
    """Como Base de test_flota_solicitudes, pero la conexión del pool NO ve las reservas nuevas (snapshot viejo) y los correos se anotan."""
    pool_ciego = True

    def setUp(self):
        self.base = Falsa()
        self.correos = []
        self.visibles = set()                       # ids de reservas que el snapshot viejo sí ve
        self.get_mysql_falla_desde = None           # n.º de llamada a get_mysql desde la cual se cae (None = nunca)
        self.llamadas_get_mysql = 0
        self.app = _app(self.base, extra={"mysql_fetchone": self._fetchone_del_pool, "get_mysql": self._get_mysql,
                                          "_aviso_interno_correo": self._correo})
        self.c = self.app.test_client()
        self.furgon = self._veh("TDVZ88", "Furgón", "Suzuki", "Swift")
        self.ana = self._chofer(1)

    # ── el mundo
    def _fetchone_del_pool(self, q, p=None):
        if self.pool_ciego and "WHERE s.id=%s" in q and p and p[0] in self.base.sol and p[0] not in self.visibles:
            return None                              # la fila existe, pero este snapshot no la ve
        return self.base.fetchone(q, p)

    def _get_mysql(self):
        self.llamadas_get_mysql += 1
        if self.get_mysql_falla_desde is not None and self.llamadas_get_mysql >= self.get_mysql_falla_desde:
            raise RuntimeError("sin conexión")
        return self.base.conexion()

    def _correo(self, clave, uids, titulo, cuerpo, enlace, modulo):
        self.correos.append({"clave": clave, "uids": list(uids), "titulo": titulo, "enlace": enlace})
        return len(uids)

    def _veh(self, patente, tipo, marca, modelo):
        from test_flota_solicitudes import FUTURO
        d = {"patente": patente, "tipo": tipo, "marca": marca, "modelo": modelo, "licencia_clases": ["B"], "revision_tecnica_vence": FUTURO,
             "permiso_circulacion_vence": FUTURO, "soap_vence": FUTURO, "seguro_vence": FUTURO}
        r = self.c.post("/flota/api/vehiculos", json=d, headers=SUPER)
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True))
        return r.get_json()["vehiculo"]["id"]

    def _chofer(self, uid):
        from test_flota_solicitudes import _datos_chofer
        r = self.c.post("/flota/api/choferes", json=dict(_datos_chofer(uid), usuario_id=uid), headers=SUPER)
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True))
        return r.get_json()["chofer"]["id"]

    def pedir(self, **k):
        d = _sol(vehiculo_id=self.furgon, chofer_id=self.ana)
        d.update(k)
        r = self.c.post("/flota/api/solicitudes", json=d, headers=SUPER)
        return r, r.get_json()


class TestLaReservaGuardadaNuncaSeNiega(BaseCiega):
    def test_con_el_pool_ciego_la_reserva_se_confirma_y_no_hay_error(self):
        r, d = self.pedir()
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True))
        self.assertTrue(d["ok"])
        self.assertEqual(len(self.base.sol), 1)
        self.assertRegex(d["solicitud"]["numero"], r"^VEH-\d{4}-0001$")
        self.assertEqual(d["solicitud"]["estado"], "solicitada")
        self.assertNotIn("no se pudo leer", r.get_data(as_text=True))

    def test_con_el_pool_ciego_igual_sale_el_correo_a_la_gestion(self):
        self.pedir()
        self.assertEqual(len(self.correos), 1)
        self.assertEqual(self.correos[0]["clave"], "flota_solicitud_nueva")
        self.assertEqual(self.correos[0]["uids"], [2])            # Beto es el gestor registrado; quien pide (1) no se avisa a sí mismo
        self.assertIn("VEH-", self.correos[0]["titulo"])

    def test_sin_el_problema_todo_sigue_igual(self):
        self.pool_ciego = False
        r, d = self.pedir()
        self.assertEqual(r.status_code, 200)
        self.assertEqual(d["solicitud"]["vehiculo"]["id"], self.furgon)
        self.assertEqual(len(self.correos), 1)

    def test_mover_una_reserva_que_ya_existia_sigue_funcionando_con_el_pool_ciego(self):
        self.pool_ciego = False
        _, d = self.pedir()
        sid = d["solicitud"]["id"]
        self.pool_ciego = True
        self.visibles = {sid}                        # una reserva vieja SÍ la ve el snapshot: solo las comiteadas después no
        cuerpo = _sol(vehiculo_id=self.furgon, chofer_id=self.ana)
        cuerpo.update(hora_desde="10:00", hora_hasta="12:00", motivo_cambio="Cambio de horario por la entrega")
        r = self.c.put(f"/flota/api/solicitudes/{sid}", json=cuerpo, headers=SUPER)
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True))
        self.assertTrue(r.get_json()["ok"])

    def test_si_ni_la_conexion_fresca_se_puede_abrir_igual_se_dice_que_se_guardo(self):
        self.get_mysql_falla_desde = self.llamadas_get_mysql + 2     # 1.ª = la transacción de la reserva; 2.ª = la relectura fresca
        r, d = self.pedir()
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True))
        self.assertTrue(d["ok"])
        self.assertEqual(len(self.base.sol), 1)                      # sí quedó guardada
        self.assertRegex(d["solicitud"]["numero"], r"^VEH-\d{4}-0001$")


class TestReintentoDeLaMismaSolicitud(BaseCiega):
    def test_reintentar_lo_mismo_devuelve_la_misma_solicitud_sin_crear_otra_ni_error(self):
        _, d1 = self.pedir()
        r2, d2 = self.pedir()
        self.assertEqual(r2.status_code, 200, r2.get_data(as_text=True))
        self.assertTrue(d2["ok"])
        self.assertTrue(d2.get("repetida"))
        self.assertEqual(d2["solicitud"]["id"], d1["solicitud"]["id"])
        self.assertEqual(len(self.base.sol), 1)
        self.assertEqual(len(self.correos), 1)                       # el aviso a la gestión sale una sola vez

    def test_cambiar_el_motivo_ya_no_es_la_misma_solicitud(self):
        self.pedir()
        r, d = self.pedir(motivo="Otro motivo distinto para el mismo horario")
        self.assertEqual(r.status_code, 409)
        self.assertEqual(d["error_codigo"], "CHOQUE_VEHICULO")
        self.assertEqual(len(self.base.sol), 1)

    def test_si_lo_pidio_otra_persona_es_un_choque_de_verdad(self):
        _, d1 = self.pedir()
        self.base.sol[d1["solicitud"]["id"]]["solicitante_id"] = 99
        r, d = self.pedir()
        self.assertEqual(r.status_code, 409)
        self.assertEqual(d["error_codigo"], "CHOQUE_VEHICULO")
        self.assertNotIn("repetida", d)

    def test_otro_horario_que_se_pisa_sigue_siendo_choque(self):
        self.pedir(hora_desde="09:00", hora_hasta="12:00")
        r, d = self.pedir(hora_desde="11:00", hora_hasta="13:00")
        self.assertEqual(r.status_code, 409)
        self.assertEqual(d["error_codigo"], "CHOQUE_VEHICULO")

    def test_una_reserva_cancelada_ya_no_cuenta_como_la_misma(self):
        _, d1 = self.pedir()
        self.base.sol[d1["solicitud"]["id"]]["estado"] = "cancelada"
        r, d = self.pedir()
        self.assertEqual(r.status_code, 200)
        self.assertNotEqual(d["solicitud"]["id"], d1["solicitud"]["id"])
        self.assertFalse(d.get("repetida"))


class TestLaPantallaNoLeDiceAlgoFalso(unittest.TestCase):
    def test_el_servidor_relee_por_una_conexion_nueva_y_nunca_con_la_del_pool(self):
        i = MODULO.index("def _respuesta_reserva(")
        cuerpo = MODULO[i:i + 1600]
        self.assertIn("_sol_fila_fresca(cuerpo[\"id\"])", cuerpo)
        self.assertNotIn("_sol_fila(cuerpo[\"id\"])", cuerpo)
        self.assertNotIn("no se pudo leer de vuelta", cuerpo)
        j = MODULO.index("def flota_api_solicitud_crear")
        self.assertIn("_sol_fila_fresca(cuerpo[\"id\"])", MODULO[j:j + 2600])
        k = MODULO.index("def _ot_reservar(")
        self.assertIn("_sol_fila_fresca(cuerpo[\"id\"])", MODULO[k:k + 2200])

    def test_la_guardia_de_reintento_compara_con_quien_esta_conectado(self):
        # El arnés de pruebas tiene una sola persona conectada (id 1), así que un `quien = 1` fijo pasaría igual: se vigila el código.
        self.assertIn("quien0 = solicitante[0] if solicitante else _uid()", MODULO)
        i = MODULO.index("quien0 = solicitante[0]")
        self.assertIn('(c_ig["id"], quien0, datos["vehiculo_id"], datos["chofer_id"], datos["area"], datos["motivo"], inicio, fin)', MODULO[i:i + 1200])

    def test_la_pantalla_avisa_si_ya_estaba_guardada_y_si_se_corto_la_conexion(self):
        i = JS_USO.index("function guardarSol()")
        f = JS_USO[i:i + 3200]
        self.assertIn("d.repetida", f)
        self.assertIn("ya estaba guardada", f)
        self.assertIn("if (!editando && d.status === 0) {", f)          # la condición completa, no solo sus pedazos
        self.assertIn("no sé si tu solicitud se guardó", f)
        self.assertIn("recargarSol()", f)


if __name__ == "__main__":
    unittest.main()

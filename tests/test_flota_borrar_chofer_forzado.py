# -*- coding: utf-8 -*-
"""🚚 FLOTA · Borrar un chofer que YA tiene reservas (Daniel, 05-nov-2026, en plena operación):
«necesito eliminar choferes así tengan agendas de momento, ya que están creados mal».

Decisiones que se vigilan:
  1. Sin reservas, borrar sigue como siempre (escribiendo el nombre completo).
  2. Con reservas, SOLO el superadministrador y a propósito (`forzar`): el servidor lo exige aunque el navegador lo mande.
  3. Se cancelan las reservas que aún no empezaban (con su motivo y su bitácora) y se avisa a quien las pidió; las pasadas se conservan.
  4. Con una LLAVE AFUERA nunca se borra, ni forzando. Si la llave sale justo durante el borrado, tampoco.
  5. Todo queda anotado ANTES de tocar nada (REGLA #5): quién, el chofer y las reservas que tenía.
  6. El navegador pregunta con ilusConfirm (nunca confirm nativo) y le dice con números qué pasará.
"""
import json
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import test_flota_solicitudes as TS  # noqa: E402
from test_flota_documentos import ADMINISTRA, SOLO_PEDIR, SUPER, Base2, _leer  # noqa: E402

NOMBRE = "Ana María Pérez Soto"


class Base3(Base2):
    def setUp(self):
        super().setUp()
        self.avisos = []
        aviso = lambda clave, uids, titulo, cuerpo, url, etiqueta="", ref=None: self.avisos.append((clave, list(uids), titulo, cuerpo)) or len(uids)
        self.app = TS._app(self.base, self.bucket, extra={"_aviso_interno_correo": aviso})
        self.c = self.app.test_client()

    def reservar_como(self, headers, **k):
        r = self.c.post("/flota/api/solicitudes", json=TS._sol(vehiculo_id=self.furgon, chofer_id=self.ana, **k), headers=headers)
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True))
        return r.get_json()["solicitud"]["id"]

    def borrar(self, headers=SUPER, forzar=None, nombre=NOMBRE, cid=None):
        cuerpo = {"confirmacion": nombre}
        if forzar is not None:
            cuerpo["forzar"] = forzar
        return self.c.delete(f"/flota/api/choferes/{cid or self.ana}", json=cuerpo, headers=headers)

    def estado(self, sid):
        return self.base.sol[sid]["estado"]


class TestSinForzar(Base3):
    def test_sin_reservas_se_borra_como_siempre(self):
        r = self.borrar(cid=self.beto, nombre="Humberto Soto Rivas")
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True))
        self.assertNotIn(self.beto, self.base.cho)
        self.assertEqual(r.get_json()["reservas_canceladas"], 0)

    def test_con_reservas_pregunta_y_no_toca_nada(self):
        a = self.reservar_como(SUPER)
        b = self.reservar_como(SUPER, hora_desde="13:00", hora_hasta="15:00")
        self.base.sol[b]["estado"] = "confirmada"
        r = self.borrar()
        d = r.get_json()
        self.assertEqual((r.status_code, d["error_codigo"]), (409, "TIENE_HISTORIAL"))
        self.assertEqual((d["puede_forzar"], d["reservas"], d["abiertas"], d["con_llave"]), (True, 2, 2, 0))
        self.assertIn(self.ana, self.base.cho)
        self.assertEqual((self.estado(a), self.estado(b)), ("solicitada", "confirmada"))
        self.assertFalse([x for x in self.base.clog if x["accion"] == "eliminado"])

    def test_quien_solo_administra_no_puede_forzar_ni_pidiendolo(self):
        sid = self.reservar_como(SUPER)
        for cuerpo in (None, True):
            r = self.borrar(headers=ADMINISTRA, forzar=cuerpo)
            d = r.get_json()
            self.assertEqual((r.status_code, d["error_codigo"], d["puede_forzar"]), (409, "TIENE_HISTORIAL", False))
        self.assertIn(self.ana, self.base.cho)
        self.assertEqual(self.estado(sid), "solicitada")

    def test_sin_el_nombre_completo_no_hay_borrado_aunque_se_fuerce(self):
        sid = self.reservar_como(SUPER)
        r = self.borrar(forzar=True, nombre="Ana")
        self.assertEqual(r.status_code, 400)
        self.assertIn(self.ana, self.base.cho)
        self.assertEqual(self.estado(sid), "solicitada")

    def test_una_persona_que_no_administra_no_entra(self):
        self.assertEqual(self.borrar(headers=SOLO_PEDIR, forzar=True).status_code, 403)
        self.assertIn(self.ana, self.base.cho)


class TestForzado(Base3):
    def preparar(self):
        # una por confirmar (la pidió otra persona), una confirmada y una ya devuelta (historia)
        self.a = self.reservar_como(SOLO_PEDIR)
        self.b = self.reservar_como(SUPER, hora_desde="13:00", hora_hasta="15:00")
        self.c_ = self.reservar_como(SUPER, hora_desde="16:00", hora_hasta="17:30")
        self.base.sol[self.b]["estado"] = "confirmada"
        self.base.sol[self.c_]["estado"] = "devuelta"
        self.base.sol[self.a]["solicitante_id"] = 7           # en las pruebas todos los encabezados son el usuario 1: la pidió «otra persona»

    def test_cancela_las_abiertas_borra_al_chofer_y_conserva_la_historia(self):
        self.preparar()
        r = self.borrar(forzar=True)
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True))
        d = r.get_json()
        self.assertEqual((d["eliminado"], d["reservas_total"], d["reservas_canceladas"]), (True, 3, 2))
        self.assertNotIn(self.ana, self.base.cho)
        self.assertEqual((self.estado(self.a), self.estado(self.b), self.estado(self.c_)), ("cancelada", "cancelada", "devuelta"))
        for sid in (self.a, self.b):
            x = self.base.sol[sid]
            self.assertIn(NOMBRE, x["cancelada_motivo"])
            self.assertTrue(x["cancelada_por"])
        self.assertLessEqual(len(self.base.sol[self.a]["cancelada_motivo"]), 300)

    def test_la_bitacora_de_cada_reserva_cancelada_cuenta_por_que(self):
        self.preparar()
        self.borrar(forzar=True)
        for sid in (self.a, self.b):
            ev = [x for x in self.base.slog if x["solicitud_id"] == sid and x["accion"] == "cancelada"]
            self.assertEqual(len(ev), 1, sid)
            det = json.loads(ev[0]["detalle"])
            self.assertEqual(det["chofer_eliminado"], NOMBRE)
            self.assertIn(det["estado_antes"], ("solicitada", "confirmada"))
        self.assertFalse([x for x in self.base.slog if x["solicitud_id"] == self.c_ and x["accion"] == "cancelada"])

    def test_queda_anotado_quien_el_chofer_y_las_reservas_que_tenia(self):
        self.preparar()
        self.borrar(forzar=True)
        ev = [x for x in self.base.clog if x["accion"] == "eliminado"]
        self.assertEqual(len(ev), 1)
        det = json.loads(ev[0]["detalle"])
        self.assertEqual(det["datos"]["nombre_completo"], NOMBRE)
        self.assertTrue(det["forzado"])
        self.assertEqual(det["reservas_total"], 3)
        self.assertEqual(sorted(x["estado"] for x in det["reservas"]), ["confirmada", "devuelta", "solicitada"])
        self.assertTrue(ev[0]["usuario"])

    def test_la_anotacion_va_antes_de_tocar_las_reservas(self):
        self.preparar()
        self.borrar(forzar=True)
        sql = self.base.sql
        i_log = next(i for i, q in enumerate(sql) if q.startswith("INSERT INTO flota_chofer_log"))
        i_cancel = min(i for i, q in enumerate(sql) if q.startswith("UPDATE flota_solicitudes SET estado='cancelada'"))
        i_borrar = max(i for i, q in enumerate(sql) if q.startswith("DELETE FROM flota_choferes"))
        self.assertLess(i_log, i_cancel)
        self.assertLess(i_cancel, i_borrar)

    def test_si_falla_la_anotacion_no_se_toca_nada(self):
        self.preparar()
        original = self.base.escribir

        def falla(s, p):
            if s.startswith("INSERT INTO flota_chofer_log"):
                raise RuntimeError("bitácora caída")
            return original(s, p)
        self.base.escribir = falla
        r = self.borrar(forzar=True)
        self.assertEqual(r.status_code, 500)
        self.assertIn(self.ana, self.base.cho)
        self.assertEqual((self.estado(self.a), self.estado(self.b), self.estado(self.c_)), ("solicitada", "confirmada", "devuelta"))

    def test_avisa_a_quien_pidio_cada_reserva_pero_no_a_quien_borra(self):
        self.preparar()
        self.borrar(forzar=True)
        cancel = [e for e in self.avisos if e[0] == "flota_solicitud_cancelada"]
        self.assertEqual(len(cancel), 1)                      # la que pidió otra persona; la de quien borra no se le avisa a sí mismo
        self.assertEqual(cancel[0][1], [7])
        self.assertIn("eliminó al chofer", cancel[0][3])
        self.assertIn(NOMBRE, cancel[0][3])

    def test_si_el_aviso_falla_igual_se_borra(self):
        self.preparar()

        def roto(*a, **k):
            raise RuntimeError("correo caído")
        app = TS._app(self.base, self.bucket, extra={"_aviso_interno_correo": roto})
        r = app.test_client().delete(f"/flota/api/choferes/{self.ana}", json={"confirmacion": NOMBRE, "forzar": True}, headers=SUPER)
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True))
        self.assertNotIn(self.ana, self.base.cho)

    def test_las_reservas_canceladas_liberan_el_horario(self):
        sid = self.reservar_como(SUPER)
        self.borrar(forzar=True)
        # el mismo vehículo y la misma hora quedan libres para otro chofer
        r = self.c.post("/flota/api/solicitudes", json=TS._sol(vehiculo_id=self.furgon, chofer_id=self.beto), headers=SUPER)
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True))
        self.assertEqual(self.estado(sid), "cancelada")


class TestLlaveAfuera(Base3):
    def test_con_la_llave_afuera_no_se_borra_ni_forzando(self):
        a = self.reservar_como(SUPER)
        b = self.reservar_como(SUPER, hora_desde="13:00", hora_hasta="15:00")
        self.base.sol[a]["estado"] = "llave_entregada"
        for forzar in (None, True):
            r = self.borrar(forzar=forzar)
            d = r.get_json()
            self.assertEqual((r.status_code, d["error_codigo"], d["puede_forzar"]), (409, "LLAVE_AFUERA", False), forzar)
            self.assertIn("devolución", d["error"])
        self.assertIn(self.ana, self.base.cho)
        self.assertEqual((self.estado(a), self.estado(b)), ("llave_entregada", "solicitada"))     # y no cancela nada a medias
        self.assertFalse([x for x in self.base.clog if x["accion"] == "eliminado"])

    def test_si_la_llave_sale_justo_mientras_se_borra_no_se_borra(self):
        a = self.reservar_como(SUPER)
        b = self.reservar_como(SUPER, hora_desde="13:00", hora_hasta="15:00")
        self.base.sol[b]["estado"] = "confirmada"
        orden = sorted((a, b))
        # al cancelar la primera abierta, otra persona entrega la llave de la segunda
        segunda = orden[1]
        self.base.al_cancelar = lambda: self.base.sol[segunda].update(estado="llave_entregada")
        r = self.borrar(forzar=True)
        d = r.get_json()
        self.assertEqual((r.status_code, d["error_codigo"]), (409, "LLAVE_AFUERA"))
        self.assertIn(self.ana, self.base.cho)
        self.assertEqual(self.estado(segunda), "llave_entregada")        # esa no se pisó
        self.assertFalse([q for q in self.base.sql if q.startswith("DELETE FROM flota_choferes")])


class TestPantalla(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.js = _leer("static", "flota_uso.js")
        cls.mod = _leer("flota_module.py")

    def test_el_navegador_pregunta_con_ilusconfirm_y_repite_con_forzar(self):
        i = self.js.index("function borrarCh(")
        trozo = self.js[i:i + 2400]
        self.assertIn("forzar: !!forzar", trozo)
        self.assertIn("puede_forzar", trozo)
        self.assertIn("window.ilusConfirm", trozo)
        self.assertIn("Borrar igual", trozo)
        self.assertIn("borrarCh(c, txt, true)", trozo)
        self.assertNotRegex(trozo, r"(?<![\w.])confirm\(")

    def test_el_primer_borrado_no_fuerza(self):
        i = self.js.index("function eliminarCh(")
        self.assertIn("borrarCh(c, txt, false)", self.js[i:i + 900])

    def test_el_servidor_exige_ser_superadmin_para_forzar(self):
        i = self.mod.index("def flota_api_chofer_eliminar(")
        trozo = self.mod[i:i + 7000]
        self.assertIn('forzar = bool(cuerpo.get("forzar")) and super_', trozo)
        self.assertIn("super_ = _es_superadmin()", trozo)
        self.assertLess(trozo.index('error_codigo="LLAVE_AFUERA"'), trozo.index("forzar = bool("))     # la llave afuera se mira antes de forzar

    def test_ninguna_ruta_nueva_ni_delete_nuevo(self):
        import re
        self.assertEqual(set(re.findall(r"DELETE\s+FROM\s+(\w+)", self.mod)), {"flota_vehiculos", "flota_choferes", "flota_solicitudes"})


if __name__ == "__main__":
    unittest.main()

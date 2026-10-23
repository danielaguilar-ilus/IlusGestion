# -*- coding: utf-8 -*-
"""2026-10-23 · «La OT de instalación y desinstalación está en un bucle: es por garantía o regalía y no me deja crear la OT por la
desinstalación» (Daniel; el aviso: «Esta instalación no se cobra… su desinstalación no puede quedar cubierta por ella»).

Causa: una instalación en $0 no puede «cubrir» a su desinstalación (REGLA #24) y tampoco tiene saldo que cobrar → ninguna opción servía.
Ahora la desinstalación hereda el MISMO $0 y el mismo motivo («cero»), siempre que ese $0 ya esté autorizado (la solicitud que creó la
instalación «Incluye desinstalación», o su $0 declarado/aprobado). Su costo y su centro de costo se registran como en cualquier OT $0 (lo que
«te cobran» se atribuye al centro de costo de la instalación). Sin respaldo, la puerta pide la autorización a Daniel (hay salida).
"""
import os
import unittest

import ot_eventos as E
from tests.test_privacidad_proveedores_tecnicos import _extraer

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _app():
    with open(os.path.join(RAIZ, "app.py"), encoding="utf-8") as fh:
        return fh.read()


class TestReglaPura(unittest.TestCase):
    def test_cobro_cero_es_valido_y_no_pide_saldo(self):
        self.assertIn(E.COBRO_CERO, E.COBROS_VALIDOS)
        self.assertEqual(E.validar_cobro_desinstalacion("cero", 0), (None, "cero"))
        self.assertEqual(E.validar_cobro_desinstalacion("cero", 500000), (None, "cero"))
        # lo de siempre no cambió
        self.assertEqual(E.validar_cobro_desinstalacion(None, 0), (None, "cubierta"))
        self.assertEqual(E.validar_cobro_desinstalacion("cobra", 0)[0]["codigo"], "SIN_SALDO_PARA_COBRAR")

    def test_motivo_cero_de_la_instalacion(self):
        f = E.motivo_cero_de_madre
        self.assertIsNone(f({}))
        self.assertIsNone(f({"modalidad_cobro": "pagado"}))
        self.assertEqual(f({"cobro_cero_motivo": "arriendo_leasing"}), "arriendo_leasing")
        self.assertEqual(f({"modalidad_cobro": "garantia"}), "garantia")
        self.assertEqual(f({"cubierto_por": "garantia"}), "garantia")
        self.assertEqual(f({"modalidad_cobro": "sin_costo"}), "regalia")


class TestRespaldoYPuerta(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.amb = _extraer(["_ot_madre_cero_respaldo", "_ot_puerta_via_cero_madre"],
                           extra={"_evt": E, "print": lambda *a, **k: None})

    def _bd(self, aprobadas=(), hija=None, madre=None):
        aprobadas = set(aprobadas)

        def fetchone(sql, params=()):
            if "FROM mant_ot_autorizaciones" in sql:
                return {"id": params[0], "argumento": "Garantía del mall: el cliente no paga"} if params[0] in aprobadas else None
            if "FROM mant_visitas WHERE id=%s" in sql and "SELECT *" in sql:
                return madre
            if "FROM mant_visitas" in sql:
                return hija
            return None
        self.amb["mysql_fetchone"] = fetchone

    def madre(self, **kw):
        m = {"id": 10, "numero_ot": "OT-2026-00400", "tipo": "instalacion", "modalidad_cobro": "garantia",
             "cobro_cero_motivo": "garantia", "desinstalacion_aut_id": 7, "cobro_cero_autorizacion_id": None, "cobro_cero_argumento": ""}
        m.update(kw)
        return m

    def test_respaldo_solo_con_autorizacion_aprobada(self):
        self._bd(aprobadas={7})
        r = self.amb["_ot_madre_cero_respaldo"](self.madre())
        self.assertEqual((r[0], r[1]), ("garantia", 7))
        self.assertIn("Garantía", r[2])
        self._bd(aprobadas=set())
        self.assertIsNone(self.amb["_ot_madre_cero_respaldo"](self.madre()), "sin autorización aprobada no hay herencia")
        self._bd(aprobadas={9})
        self.assertEqual(self.amb["_ot_madre_cero_respaldo"](self.madre(desinstalacion_aut_id=None, cobro_cero_autorizacion_id=9))[1], 9)
        # una instalación que SÍ se cobra no es un $0
        self.assertIsNone(self.amb["_ot_madre_cero_respaldo"]({"modalidad_cobro": "pagado", "desinstalacion_aut_id": 7}))

    def test_via_al_crear_la_hija(self):
        self._bd(aprobadas={7})
        v = self.amb["_ot_puerta_via_cero_madre"]({}, "crear", cero_de=self.madre())
        self.assertTrue(v["ok"])
        self.assertEqual((v["via"], v["autorizacion_id"], v["motivo"], v["madre_numero"]), ("cero_madre", 7, "garantia", "OT-2026-00400"))
        self._bd(aprobadas=set())
        self.assertIsNone(self.amb["_ot_puerta_via_cero_madre"]({}, "crear", cero_de=self.madre()),
                          "sin respaldo sigue el camino normal: la puerta pide la autorización a Daniel")

    def test_via_al_cerrar_relee_la_relacion_y_no_confia_en_el_body(self):
        hija = {"tipo": "desinstalacion", "ot_relacion": "desinstalacion", "ot_madre_id": 10, "cobro_cero_motivo": "garantia",
                "cobro_cero_autorizacion_id": 7}
        self._bd(aprobadas={7}, hija=hija, madre=self.madre())
        self.assertEqual(self.amb["_ot_puerta_via_cero_madre"]({"id": 55}, "cerrar")["via"], "cero_madre")
        self.assertIsNone(self.amb["_ot_puerta_via_cero_madre"]({}, "cerrar"), "sin id no hay nada que releer")
        # con otra autorización en la hija no vale
        self._bd(aprobadas={7}, hija=dict(hija, cobro_cero_autorizacion_id=99), madre=self.madre())
        self.assertIsNone(self.amb["_ot_puerta_via_cero_madre"]({"id": 55}, "cerrar"))
        # una hija sin su propio $0 declarado no pasa por esta vía
        self._bd(aprobadas={7}, hija=dict(hija, cobro_cero_motivo=None), madre=self.madre())
        self.assertIsNone(self.amb["_ot_puerta_via_cero_madre"]({"id": 55}, "cerrar"))
        # una OT que no es desinstalación tampoco
        self._bd(aprobadas={7}, hija=dict(hija, tipo="instalacion"), madre=self.madre())
        self.assertIsNone(self.amb["_ot_puerta_via_cero_madre"]({"id": 55}, "cerrar"))


class TestCaminosUsanLaHerencia(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = _app()

    def _fn(self, nombre, largo=60000):
        i = self.app.index(f"def {nombre}(")
        return self.app[i:i + largo]

    def test_la_puerta_cierra_y_aplica_la_via(self):
        p = self._fn("_ot_puerta_documento", 14000)
        self.assertIn("cero_de=None", p.split("):")[0])
        self.assertIn("_ot_puerta_via_cero_madre(v, momento, cero_de=cero_de)", p)
        self.assertIn('elif via == "cero_madre" and aid:', self._fn("_ot_puerta_aplicar", 9000))
        self.assertEqual(self.app.count('"documento", "cubierta_madre", "cero_madre",'), 2, "los dos candados de cierre")

    def test_validar_previo_hereda_el_cero_en_vez_de_bloquear(self):
        f = self._fn("_ot_desinst_validar_previo", 9000)
        self.assertIn("not _evt.madre_cobra(madre_fila)", f)
        self.assertIn("_evt.COBRO_CERO", f)

    def test_crear_hija_cero_registra_costo_y_centro_como_cualquier_ot_cero(self):
        f = self._fn("_ot_crear_desinstalacion", 14000)
        self.assertIn("if dd[\"cobro\"] == _evt.COBRO_CERO:", f)
        for pedazo in ('"cobro_cero_motivo": _mot_cero', '"garantia_aplica": _mot_cero == "garantia"', '"centro_costo": m.get("centro_costo")',
                       '"costo_proveedor": dd["costo_proveedor"]'):
            self.assertIn(pedazo, f, pedazo)

    def test_el_nucleo_pasa_la_instalacion_a_la_puerta_y_no_pide_documento_para_el_cero(self):
        n = self._fn("_ot2_crear_core", 120000)
        self.assertIn("cero_de=(madre if (es_hija and d.get(\"_desinst_cobro\") == _evt.COBRO_CERO) else None)", n)
        self.assertIn("not in (_evt.COBRO_PROPIO, _evt.COBRO_CERO)", n)

    def test_el_previo_no_pregunta_y_explica(self):
        f = self._fn("ot_api_desinstalacion_previo", 9000)
        self.assertIn("and _evt.madre_cobra(madre_virtual))", f)
        self.assertIn("Sin cobro, igual que la instalación", f)
        js = open(os.path.join(RAIZ, "static", "ot_desinst.js"), encoding="utf-8").read()
        self.assertNotIn("if (!madreCobra() && !(r.elige && S.cobro === 'cobra')) return false;", js)


if __name__ == "__main__":
    unittest.main()

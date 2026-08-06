"""Pruebas de redespacho_automatico.py -- el motor de decisión que dice qué
hacer ante una entrega fallida, courier por courier.

Daniel, 2026-08-05: "quiero agotar todas las instancias de manera
automática... rápido, automático y trazable... utiliza toda la inteligencia
posible".

LO QUE ESTAS PRUEBAS PROTEGEN: que el motor NUNCA finja poder automatizar
donde no puede. El riesgo real no es "no automatizar" -- es automatizar mal:
duplicar un envío FedEx (dinero real, doble cobro) o prometer un reintento
en un courier que no tiene API para eso.

Correr con:  py -m unittest tests.test_redespacho_automatico -v
"""
import datetime as dt
import unittest

import redespacho_automatico as ra


def _simpliroute_ok(nombre):
    """Simula _simpliroute_courier_integra: Felca y Milling sí, el resto no."""
    n = (nombre or "").lower()
    return "felca" in n or "milling" in n


class TestSimpliRouteAutomatiza(unittest.TestCase):
    def test_primer_fallo_se_automatiza(self):
        r = ra.evaluar_reintento("Transporte Felca", 0,
                                  simpliroute_courier_integra_fn=_simpliroute_ok)
        self.assertEqual(ra.ACCION_CREAR_VISITA_SIMPLIROUTE, r["accion"])
        self.assertTrue(r["puede_automatizar"])

    def test_segundo_fallo_dentro_del_tope_se_automatiza(self):
        r = ra.evaluar_reintento("Transportes Milling", 1,
                                  simpliroute_courier_integra_fn=_simpliroute_ok)
        self.assertEqual(ra.ACCION_CREAR_VISITA_SIMPLIROUTE, r["accion"])
        self.assertTrue(r["puede_automatizar"])

    def test_al_llegar_al_tope_se_detiene_y_pasa_a_humano(self):
        r = ra.evaluar_reintento("Transporte Felca", ra.MAX_REINTENTOS_AUTOMATICOS,
                                  simpliroute_courier_integra_fn=_simpliroute_ok)
        self.assertEqual(ra.ACCION_TOPE_ALCANZADO, r["accion"])
        self.assertFalse(r["puede_automatizar"])

    def test_tope_personalizado_se_respeta(self):
        r = ra.evaluar_reintento("Transporte Felca", 5,
                                  simpliroute_courier_integra_fn=_simpliroute_ok,
                                  max_reintentos=5)
        self.assertEqual(ra.ACCION_TOPE_ALCANZADO, r["accion"])

    def test_nunca_reintenta_infinito(self):
        # "agotar todas las instancias" no es "reintentar para siempre":
        # con un número absurdo de intentos previos, sigue topando.
        r = ra.evaluar_reintento("Transporte Felca", 999,
                                  simpliroute_courier_integra_fn=_simpliroute_ok)
        self.assertEqual(ra.ACCION_TOPE_ALCANZADO, r["accion"])


class TestFedexNuncaSeAutomatiza(unittest.TestCase):
    """El caso más peligroso: automatizar FedEx mal duplica un envío real."""

    def test_fedex_va_a_gestion_humana(self):
        r = ra.evaluar_reintento("FedEx", 0)
        self.assertEqual(ra.ACCION_GESTION_HUMANA_FEDEX, r["accion"])
        self.assertFalse(r["puede_automatizar"])

    def test_fedex_nunca_se_automatiza_ni_en_el_primer_intento(self):
        for intentos in (0, 1, 2, 10):
            r = ra.evaluar_reintento("FedEx Chile", intentos)
            self.assertFalse(r["puede_automatizar"], f"intentos={intentos}")

    def test_fedex_explica_por_que_no_se_automatiza(self):
        r = ra.evaluar_reintento("FedEx", 0)
        self.assertIn("duplicar", r["motivo_operativo"].lower())


class TestSinApiVaAHumano(unittest.TestCase):
    def test_shipit_no_finge_automatizacion(self):
        r = ra.evaluar_reintento("Shipit", 0)
        self.assertEqual(ra.ACCION_GESTION_HUMANA_SIN_API, r["accion"])
        self.assertFalse(r["puede_automatizar"])

    def test_clickex_no_finge_automatizacion(self):
        r = ra.evaluar_reintento("Clickex", 0)
        self.assertEqual(ra.ACCION_GESTION_HUMANA_SIN_API, r["accion"])
        self.assertFalse(r["puede_automatizar"])

    def test_courier_desconocido_no_se_automatiza_por_seguridad(self):
        r = ra.evaluar_reintento("Courier Inventado XYZ", 0)
        self.assertFalse(r["puede_automatizar"])

    def test_courier_vacio_no_revienta(self):
        r = ra.evaluar_reintento("", 0)
        self.assertFalse(r["puede_automatizar"])
        r2 = ra.evaluar_reintento(None, 0)
        self.assertFalse(r2["puede_automatizar"])


class TestMensajesAlCliente(unittest.TestCase):
    """Daniel: "no solo un mensaje que baje la ansiedad, sino la gestión
    automática... que me diga que se generó una entrega tal día"."""

    def test_todo_resultado_trae_frase_para_el_cliente(self):
        for courier in ("Transporte Felca", "FedEx", "Shipit", "Clickex", "Desconocido"):
            r = ra.evaluar_reintento(courier, 0, simpliroute_courier_integra_fn=_simpliroute_ok)
            self.assertTrue(r["motivo_cliente"])
            # nunca jerga interna en el texto de cliente
            self.assertNotIn("API", r["motivo_cliente"])
            self.assertNotIn("automátiz", r["motivo_cliente"].lower())

    def test_mensaje_simpliroute_incluye_fecha_concreta(self):
        msg = ra.mensaje_redespacho_cliente(
            courier_nombre="Transporte Felca", fecha_estimada_str="jueves 07/08/2026",
            accion=ra.ACCION_CREAR_VISITA_SIMPLIROUTE)
        self.assertIn("jueves 07/08/2026", msg)

    def test_mensaje_gestion_humana_no_inventa_fecha(self):
        msg = ra.mensaje_redespacho_cliente(
            courier_nombre="FedEx", fecha_estimada_str="",
            accion=ra.ACCION_GESTION_HUMANA_FEDEX)
        self.assertNotIn("None", msg)
        self.assertIn("próximo día hábil", msg)


class TestProximaFechaHabil(unittest.TestCase):
    def test_lunes_a_martes(self):
        # 2026-08-03 es lunes
        r = ra.proxima_fecha_habil(dt.date(2026, 8, 3))
        self.assertEqual(dt.date(2026, 8, 4), r)

    def test_viernes_salta_el_fin_de_semana(self):
        # 2026-08-07 es viernes -> el próximo hábil es el lunes 08-10, no sábado
        r = ra.proxima_fecha_habil(dt.date(2026, 8, 7))
        self.assertEqual(dt.date(2026, 8, 10), r)
        self.assertEqual(0, r.weekday())  # lunes

    def test_sabado_salta_a_lunes(self):
        r = ra.proxima_fecha_habil(dt.date(2026, 8, 8))
        self.assertEqual(dt.date(2026, 8, 10), r)

    def test_domingo_salta_a_lunes(self):
        r = ra.proxima_fecha_habil(dt.date(2026, 8, 9))
        self.assertEqual(dt.date(2026, 8, 10), r)

    def test_varios_dias_habiles_adelante(self):
        # Lunes + 3 días hábiles = jueves
        r = ra.proxima_fecha_habil(dt.date(2026, 8, 3), dias_habiles_despues=3)
        self.assertEqual(dt.date(2026, 8, 6), r)

    def test_acepta_datetime_no_solo_date(self):
        r = ra.proxima_fecha_habil(dt.datetime(2026, 8, 3, 14, 30))
        self.assertEqual(dt.date(2026, 8, 4), r)

    def test_nunca_devuelve_fin_de_semana(self):
        for offset in range(14):
            base = dt.date(2026, 8, 3) + dt.timedelta(days=offset)
            r = ra.proxima_fecha_habil(base)
            self.assertLess(r.weekday(), 5, f"base={base} -> {r}")


if __name__ == "__main__":
    unittest.main(verbosity=2)

# -*- coding: utf-8 -*-
"""Días especiales de Retiros: víspera de feriado y salida temprano (funciones puras)."""
import os
import unittest
from datetime import date

import retiros_horarios as rh


def _mundo(feriados=(), cerrados_extra=(), dias_trabajo=(1, 2, 3, 4, 5)):
    """cerrado/feriado sobre un calendario de juguete."""
    fer = {date.fromisoformat(f) for f in feriados}
    extra = {date.fromisoformat(f) for f in cerrados_extra}

    def feriado(d):
        return d in fer

    def cerrado(d):
        return d.isoweekday() not in dias_trabajo or d in fer or d in extra

    return cerrado, feriado


class TestVispera(unittest.TestCase):
    def test_viernes_antes_de_lunes_feriado(self):
        # 12-oct-2026 es lunes (Encuentro de Dos Mundos)
        cerrado, feriado = _mundo(["2026-10-12"])
        self.assertEqual(rh.vispera_de(date(2026, 10, 9), cerrado, feriado), date(2026, 10, 12))

    def test_jueves_antes_de_viernes_feriado(self):
        cerrado, feriado = _mundo(["2026-04-03"])   # Viernes Santo
        self.assertEqual(rh.vispera_de(date(2026, 4, 2), cerrado, feriado), date(2026, 4, 3))

    def test_un_viernes_normal_no_es_vispera(self):
        cerrado, feriado = _mundo(["2026-10-12"])
        self.assertFalse(rh.es_vispera(date(2026, 10, 2), cerrado, feriado))

    def test_el_dia_del_feriado_y_el_fin_de_semana_no_son_vispera(self):
        cerrado, feriado = _mundo(["2026-10-12"])
        self.assertFalse(rh.es_vispera(date(2026, 10, 12), cerrado, feriado))   # el feriado mismo
        self.assertFalse(rh.es_vispera(date(2026, 10, 10), cerrado, feriado))   # sábado

    def test_puente_largo_toma_el_primer_feriado(self):
        # jue 17 y vie 18 feriados: el miércoles es víspera del jueves
        cerrado, feriado = _mundo(["2026-09-17", "2026-09-18"])
        self.assertEqual(rh.vispera_de(date(2026, 9, 16), cerrado, feriado), date(2026, 9, 17))

    def test_un_cierre_de_dia_completo_no_es_feriado(self):
        # bloqueo de bodega mañana (inventario): cerrado sí, feriado no → hoy NO es víspera
        cerrado, feriado = _mundo(cerrados_extra=["2026-10-06"])
        self.assertFalse(rh.es_vispera(date(2026, 10, 5), cerrado, feriado))

    def test_cierre_extra_declarado_como_feriado(self):
        fer_extra = {date(2026, 12, 24)}
        cerrado = lambda d: d.isoweekday() > 5 or d in fer_extra
        feriado = lambda d: d in fer_extra
        self.assertTrue(rh.es_vispera(date(2026, 12, 23), cerrado, feriado))


class TestCierreEfectivo(unittest.TestCase):
    def test_dia_normal(self):
        self.assertEqual(rh.cierre_efectivo("16:30"), ("16:30", "normal", ""))

    def test_regla_manual_adelanta_el_cierre(self):
        r = {"hasta": "13:00", "motivo": "Cena de fin de año"}
        self.assertEqual(rh.cierre_efectivo("16:30", r), ("13:00", "temprano", "Cena de fin de año"))

    def test_regla_manual_mas_tarde_que_lo_normal_se_ignora(self):
        self.assertEqual(rh.cierre_efectivo("16:30", {"hasta": "18:00"})[1], "normal")

    def test_vispera_activa_aplica(self):
        self.assertEqual(
            rh.cierre_efectivo("16:30", None, vispera=True, vispera_activa=True, vispera_hasta="15:00"),
            ("15:00", "vispera", "Víspera de feriado"))

    def test_vispera_apagada_no_aplica(self):
        self.assertEqual(
            rh.cierre_efectivo("16:30", None, vispera=True, vispera_activa=False, vispera_hasta="15:00")[1], "normal")

    def test_quitar_la_vispera_de_un_dia(self):
        r = {"sin_vispera": True}
        self.assertEqual(
            rh.cierre_efectivo("16:30", r, vispera=True, vispera_activa=True, vispera_hasta="15:00")[1], "normal")

    def test_la_regla_manual_gana_a_la_vispera(self):
        r = {"hasta": "12:00", "motivo": "Reunión"}
        self.assertEqual(
            rh.cierre_efectivo("16:30", r, vispera=True, vispera_activa=True, vispera_hasta="15:00")[:2],
            ("12:00", "temprano"))

    def test_horas_invalidas_no_rompen(self):
        self.assertEqual(rh.cierre_efectivo("16:30", {"hasta": "abc"})[1], "normal")
        self.assertIsNone(rh.hhmm_a_min("25:00"))
        self.assertIsNone(rh.hhmm_a_min(None))


if __name__ == "__main__":
    unittest.main()


class FeriadosIrrenunciables(unittest.TestCase):
    """Calendario de días especiales (2026-10-02): distingue feriados irrenunciables de los normales."""

    def test_los_cinco_irrenunciables_del_comercio(self):
        from datetime import date
        for d in (date(2026, 1, 1), date(2026, 5, 1), date(2026, 9, 18), date(2026, 9, 19), date(2026, 12, 25)):
            self.assertTrue(rh.es_irrenunciable(d), d)

    def test_los_demas_feriados_son_normales(self):
        from datetime import date
        for d in (date(2026, 4, 3), date(2026, 5, 21), date(2026, 10, 12), date(2026, 12, 8), date(2026, 12, 31)):
            self.assertFalse(rh.es_irrenunciable(d), d)


class CalendarioDelPanel(unittest.TestCase):
    """El panel pide 400 días, muestra feriados con irrenunciable/confirmado y NO bloquea sin confirmación humana."""

    def test_la_api_entrega_los_datos_del_calendario(self):
        src = open(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "pickups_module.py"), encoding="utf-8").read()
        for clave in ('"feriados_cal": feriados_cal', '"work_days": sorted(c["work_days"])', "irrenunciable=_rh.es_irrenunciable(d)",
                      '"confirmado": iso in cierres_dia', "created_by FROM pickup_blocks"):
            self.assertIn(clave, src)

    def test_el_panel_pide_confirmacion_antes_de_bloquear(self):
        html = open(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "templates", "retiros", "internal_dashboard.html"), encoding="utf-8").read()
        i = html.index("$('hzSelOk').addEventListener('click'")
        cuerpo = html[i:i + 4500]
        self.assertIn("await ilusConfirm(", cuerpo)
        self.assertLess(cuerpo.index("await ilusConfirm("), cuerpo.index("await api("))     # el «sí» va ANTES de escribir
        self.assertIn("if (!ok) return;", cuerpo)

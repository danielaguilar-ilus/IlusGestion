# -*- coding: utf-8 -*-
"""🗓️ 2026-10-14 — Calendario de OT con los feriados y días no laborales de Chile, y vista responsiva.

Daniel: «Puedes mejorar el calendario de las Órdenes de trabajo considerando los días feriados o no laborales en
Chile y que tenga una hermosa vista responsiva ya que encuentro que está fallando un poco en la parte responsiva».

Se prueba: el cálculo de feriados para años sin lista oficial (traslados de ley), el resumen del mes que arma el
servidor (días hábiles, feriados, próximo feriado), y que la plantilla marque feriado / fin de semana, avise las OT en
días no laborales, muestre la agenda en celular, pliegue los filtros y abra el modal a pantalla completa.
"""
import datetime as dt
import os
import re
import unittest

import cl_feriados as F
from tests.test_fac_libro_saldo import _exec
from tests.test_ot_saldo_servicio import _fuente, _leer

H = _leer("templates/ot2/panel.html")


class TestFeriadosDeLey(unittest.TestCase):
    def test_2023_calculado_igual_al_oficial(self):
        # 2023 no está en la lista oficial: el cálculo debe dar los feriados reales (salvo el 2-ene, ley especial).
        oficial = {"2023-01-01", "2023-04-07", "2023-04-08", "2023-05-01", "2023-05-21", "2023-06-21", "2023-06-26",
                   "2023-07-16", "2023-08-15", "2023-09-18", "2023-09-19", "2023-10-09", "2023-10-27", "2023-11-01",
                   "2023-12-08", "2023-12-25"}
        self.assertEqual(set(F.feriados_chile(2023)), oficial)

    def test_2027_traslados(self):
        f = F.feriados_chile(2027)
        self.assertEqual(f["2027-06-28"], "San Pedro y San Pablo")        # 29-jun cae martes → lunes 28
        self.assertEqual(f["2027-10-11"], "Encuentro de Dos Mundos")      # 12-oct cae martes → lunes 11
        self.assertNotIn("2027-06-29", f)
        self.assertEqual(f["2027-06-21"], "Día de los Pueblos Indígenas")

    def test_2026_oficial_intacto(self):
        f = F.feriados_chile(2026)
        self.assertEqual(f["2026-10-12"], "Encuentro de Dos Mundos")
        self.assertEqual(f["2026-10-31"], "Día de las Iglesias Evangélicas")
        self.assertEqual(len(f), 16)


class TestResumenDelMes(unittest.TestCase):
    def _f(self):
        amb = {"_OT2_DOW_ES": ("Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo"),
               "print": lambda *a, **k: None}
        return _exec("_ot2_cal_no_laborales", amb)

    def test_octubre_2026(self):
        r = self._f()(2026, 10, dt.date(2026, 10, 14))
        self.assertEqual(r["habiles"], 21)                         # 22 días de semana menos el lunes 12
        self.assertEqual([(x["dia"], x["dow"]) for x in r["lista"]], [(12, "Lun"), (31, "Sáb")])
        self.assertTrue(r["lista"][0]["pasado"])
        self.assertTrue(r["lista"][1]["finde"])
        self.assertEqual(r["habiles_restantes"], 13)               # del 14 al 30, de lunes a viernes
        self.assertEqual((r["proximo"]["fecha"], r["proximo"]["dias"]), ("31/10", 17))

    def test_otro_mes_no_cuenta_restantes_y_cruza_el_anio(self):
        r = self._f()(2026, 11, dt.date(2026, 12, 26))             # mirando noviembre desde el 26-dic
        self.assertIsNone(r["habiles_restantes"])
        self.assertEqual((r["proximo"]["nombre"], r["proximo"]["fecha"]), ("Año Nuevo", "01/01"))   # ya es de 2027


class TestServidorCalendario(unittest.TestCase):
    def test_cada_dia_sabe_si_es_feriado_o_fin_de_semana(self):
        f = _fuente("ot2_panel")
        self.assertIn("cal_no_laborales = _ot2_cal_no_laborales(cal_anio, cal_mes_num, hoy)", f)
        for k in ('"feriado": _fer_d', '"finde": _f_d.weekday() >= 5', '"pasado": _f_d < hoy', '"n": len(dia_filas)'):
            self.assertIn(k, f)
        self.assertIn("cal_ot_no_laboral.append(", f)
        self.assertIn("cal_no_laborales=cal_no_laborales, cal_ot_no_laboral=cal_ot_no_laboral", f)


class TestPlantilla(unittest.TestCase):
    def test_celda_marca_feriado_fin_de_semana_y_aviso(self):
        self.assertIn("{{ ' feriado' if celda.feriado }}", H)
        self.assertIn("{{ ' finde' if celda.finde and not celda.feriado }}", H)
        self.assertIn('class="bi bi-exclamation-triangle-fill ot2-nolab"', H)
        self.assertIn('data-feriado="{{ celda.feriado or \'\' }}"', H)

    def test_nombres_ya_no_se_cortan_a_14_letras(self):   # REGLA #15
        self.assertNotIn("[:14]", H)
        self.assertIn("{{ f.razon_social or 'Trabajo interno' }}</span>", H)

    def test_celular_agenda_filtros_y_modal(self):
        self.assertIn('<section class="ot2-agenda"', H)
        self.assertIn('class="ot2-filtros-btn"', H)
        self.assertIn('id="ot2Bar"', H)
        self.assertIn("modal-fullscreen-sm-down", H)
        self.assertIn('#ot2Bar:not(.abierto) > :not(.ot2-search):not(input[type="hidden"]){display:none}', H)

    def test_dia_feriado_sin_ot_avisa_y_el_modal_dice_no_laboral(self):
        self.assertIn("ilusToast('Feriado: ' + feriado + ' · sin OT agendadas'", H)
        self.assertIn("Es un día no laboral: confirma que la visita esté acordada con el cliente y el técnico.", H)

    def test_sin_alert_nativo(self):   # REGLA #1
        i = H.index("function abrir(celda){")
        bloque = H[i:i + 4000]
        self.assertIsNone(re.search(r"(?<![\w.])(alert|confirm|prompt)\(", bloque))


if __name__ == "__main__":
    unittest.main()

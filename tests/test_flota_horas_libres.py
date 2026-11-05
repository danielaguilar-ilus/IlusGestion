# -*- coding: utf-8 -*-
"""🚚 FLOTA · hora de inicio y de término LIBRES (Daniel, 05-nov-2026): «necesito que en el horario del camión me deje seleccionar de
12:30 a 15:00 y no me deja. Quiero que se defina una hora de inicio (cualquiera) y una hora de término cualquiera mayor a la de inicio».

Antes los campos Desde / Hasta vivían plegados ("Ajustar el día y la hora a mano") con paso de 15 minutos, y la forma normal de elegir era
tocar bloques de media hora (el término de un bloque es 30 min después de su inicio: para terminar a las 15:00 había que tocar el de 14:30).
Ahora Desde / Hasta están SIEMPRE a la vista, sin paso, y los bloques solo ayudan a llenarlos.

Correr:  python -m pytest tests/test_flota_horas_libres.py -q -p no:warnings
"""
import datetime as dt
import os
import re
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import flota_reglas as R  # noqa: E402
from test_flota_solicitudes import _leer  # noqa: E402

HTML = _leer("templates", "flota", "index.html")
CSS = _leer("static", "flota.css")
JS_USO = _leer("static", "flota_uso.js")

AHORA = dt.datetime(2026, 11, 5, 8, 0)            # jueves 08:00 (hora de Chile, sin zona)


def _pedido(desde, hasta, fecha="2026-11-06", fecha_hasta=None):
    return {"vehiculo_id": 1, "chofer_id": 1, "area": "Logística", "motivo": "Llevar mercadería a un cliente",
            "fecha": fecha, "hora_desde": desde, "hora_hasta": hasta, "fecha_hasta": fecha_hasta or ""}


class TestElServidorAceptaCualquierHora(unittest.TestCase):
    def test_el_caso_de_daniel_12_30_a_15_00(self):
        datos, errores = R.validar_solicitud(_pedido("12:30", "15:00"), AHORA)
        self.assertEqual(errores, {})
        self.assertEqual(datos["inicio"], dt.datetime(2026, 11, 6, 12, 30))
        self.assertEqual(datos["fin"], dt.datetime(2026, 11, 6, 15, 0))

    def test_minutos_sueltos_que_no_son_multiplo_de_15_ni_de_30(self):
        for desde, hasta in (("12:10", "15:07"), ("07:41", "19:03"), ("00:01", "23:59"), ("9:05", "9:06")):
            with self.subTest(desde=desde, hasta=hasta):
                _, errores = R.validar_solicitud(_pedido(desde, hasta), AHORA)
                self.assertEqual(errores, {})

    def test_la_hora_de_inicio_puede_caer_en_la_colacion_o_fuera_del_horario_de_llaves(self):
        for desde, hasta in (("13:15", "13:45"), ("06:00", "07:00"), ("18:30", "21:00")):
            with self.subTest(desde=desde, hasta=hasta):
                _, errores = R.validar_solicitud(_pedido(desde, hasta), AHORA)
                self.assertEqual(errores, {})        # solo se AVISA en pantalla (horario de llaves), no se bloquea

    def test_el_termino_tiene_que_ser_mayor_al_inicio(self):
        for desde, hasta in (("15:00", "12:30"), ("12:30", "12:30"), ("12:31", "12:30")):
            with self.subTest(desde=desde, hasta=hasta):
                _, errores = R.validar_solicitud(_pedido(desde, hasta), AHORA)
                self.assertEqual(errores.get("hora_hasta"), "La hora de término debe ser después de la de inicio.")

    def test_un_minuto_de_diferencia_alcanza(self):
        _, errores = R.validar_solicitud(_pedido("12:30", "12:31"), AHORA)
        self.assertEqual(errores, {})

    def test_cruzar_la_medianoche_se_hace_con_el_dia_de_devolucion_no_con_una_hora_menor(self):
        _, errores = R.validar_solicitud(_pedido("22:00", "06:00"), AHORA)
        self.assertIn("hora_hasta", errores)
        _, errores = R.validar_solicitud(_pedido("22:00", "06:00", fecha_hasta="2026-11-07"), AHORA)
        self.assertEqual(errores, {})

    def test_el_choque_se_mide_al_minuto_y_terminar_justo_cuando_empieza_otro_no_choca(self):
        otra = (dt.datetime(2026, 11, 6, 15, 0), dt.datetime(2026, 11, 6, 18, 30))
        self.assertFalse(R.se_cruzan(dt.datetime(2026, 11, 6, 12, 30), dt.datetime(2026, 11, 6, 15, 0), *otra))
        self.assertTrue(R.se_cruzan(dt.datetime(2026, 11, 6, 12, 10), dt.datetime(2026, 11, 6, 15, 7), *otra))
        self.assertTrue(R.se_cruzan(dt.datetime(2026, 11, 6, 15, 59), dt.datetime(2026, 11, 6, 16, 0), *otra))


class TestDesdeHastaVivenPlegadosYLosBloquesSonLoUnicoVisible(unittest.TestCase):
    """Daniel (05-nov, tras ver #256): «en el horario está uno como el de retiros y otro con bloques: dejemos solo como el de retiros».
    Eligió «Bloques a la vista, Desde/Hasta plegadas»."""
    PLEGADO = re.compile(r'<details class="fl-pk-manual" id="flPkManual">.*?</details>', re.S)

    def test_desde_y_hasta_viven_dentro_del_bloque_plegado_y_no_en_otro_lado(self):
        plegado = self.PLEGADO.search(HTML).group(0)
        self.assertIn('name="hora_desde"', plegado)
        self.assertIn('name="hora_hasta"', plegado)
        self.assertEqual(HTML.count('name="hora_desde"'), 1)
        self.assertEqual(HTML.count('name="hora_hasta"'), 1)

    def test_el_bloque_plegado_nace_cerrado(self):
        self.assertIn('<details class="fl-pk-manual" id="flPkManual">', HTML)          # sin `open`
        self.assertIn("$('flPkManual').open = false;", JS_USO)                          # y cada vez que se abre el formulario

    def test_la_columna_de_los_bloques_ya_no_trae_las_casillas(self):
        i = HTML.index('<div class="fl-pk-der">')
        j = HTML.index('id="flPkSel"', i)
        self.assertNotIn("flPkHoras", HTML[i:j])
        self.assertNotIn('type="time"', HTML[i:j])
        self.assertLess(HTML.index('id="flPkBloques"'), HTML.index('id="flPkHoras"'))

    def test_cada_campo_existe_una_sola_vez_y_con_su_mensaje(self):
        # campoSol(c) busca por data-campo: un duplicado pintaría el error en el campo equivocado.
        for campo in ("hora_desde", "hora_hasta", "fecha", "fecha_hasta"):
            with self.subTest(campo=campo):
                self.assertEqual(HTML.count('data-campo="%s"' % campo), 1)
                self.assertEqual(HTML.count('name="%s"' % campo), 1)
        for ident in ("flSolDesde", "flSolHasta"):
            self.assertEqual(HTML.count('id="%s"' % ident), 1)

    def test_sin_paso_de_15_minutos_cualquier_minuto_es_valido_para_el_navegador(self):
        for ident in ("flSolDesde", "flSolHasta"):
            m = re.search(r'<input type="time" id="%s"[^>]*>' % ident, HTML)
            self.assertIsNotNone(m, ident)
            self.assertNotIn('step="900"', m.group(0))
            self.assertNotIn('step="1800"', m.group(0))
            self.assertIn('step="60"', m.group(0))

    def test_no_se_quito_nada_el_dia_los_bloques_y_el_calendario_siguen(self):
        plegado = self.PLEGADO.search(HTML).group(0)
        self.assertIn('name="fecha"', plegado)
        self.assertIn('name="fecha_hasta"', plegado)
        self.assertIn("Ajustar el día y la hora a mano", plegado)
        for ident in ("flPkBloques", "flPkSel", "flPkMes", "flSolOtroDia"):      # el calendario y los bloques siguen (REGLA #4.2)
            self.assertIn('id="%s"' % ident, HTML)

    def test_la_ayuda_explica_que_el_bloque_termina_30_minutos_despues_y_manda_a_la_hora_exacta(self):
        self.assertIn("para terminar a las 15:00, toca el de 14:30", HTML)
        self.assertIn("Para una hora exacta, abre «Ajustar el día y la hora a mano»", HTML)

    def test_el_bloque_tiene_estilo_y_cabe_en_el_celular(self):
        self.assertRegex(CSS, r"\.fl-pk-horas\{[^}]*border-radius:14px")
        self.assertRegex(CSS, r"\.fl-pk-horas-g\{display:grid;grid-template-columns:repeat\(2,minmax\(0,1fr\)\)")
        self.assertRegex(CSS, r"\.fl-pk-horas-g input\[type=time\]\{[^}]*min-height:44px")
        self.assertIn(".fl-pk-manual .fl-pk-horas{margin:0 0 12px}", CSS)

    def test_si_la_hora_a_mano_esta_mal_se_abre_el_bloque_para_que_se_vea_el_error(self):
        self.assertIn("$('flPkManual').contains(malo)", JS_USO)


class TestEscribirLaHoraNoDejaUnToqueATrasMano(unittest.TestCase):
    def test_escribir_desde_o_hasta_reinicia_el_primer_toque_del_bloque(self):
        i = JS_USO.index("fs.addEventListener('input'")
        bloque = JS_USO[i:i + 520]
        self.assertIn("e.target.name === 'hora_desde' || e.target.name === 'hora_hasta'", bloque)
        self.assertIn("S.pk.ancla = null", bloque)
        self.assertIn("evaluarSolLuego()", bloque)

    def test_la_validacion_de_pantalla_no_exige_multiplos_de_media_hora(self):
        i = JS_USO.index("function reglaHoraHasta()")
        regla = JS_USO[i:JS_USO.index("function reglaFechaHasta()")]
        self.assertIn("La hora de término debe ser después de la de inicio.", regla)
        self.assertNotRegex(regla, r"% ?(15|30)")
        j = JS_USO.index("function reglaHoraDesde()")
        self.assertNotRegex(JS_USO[j:i], r"% ?(15|30)")


if __name__ == "__main__":
    unittest.main()

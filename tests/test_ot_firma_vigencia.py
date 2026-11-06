# -*- coding: utf-8 -*-
"""El ENLACE DE FIRMA REMOTA del cliente vence en 48 HORAS HÁBILES (Daniel, 5-nov-2026).

Daniel, con su manual MAN-SST-INS-01 (§4 «Firma remota del cliente: 48 horas hábiles, desde el envío del enlace») y tras decirle que hoy dura 120
horas corridas («vence en 5 días»): «sí, cambia el enlace de firma a 48 horas hábiles».

«Horas hábiles» = como la anticipación de 48 horas de Retiros: solo cuenta el tiempo de los DÍAS HÁBILES (lunes a viernes sin feriados).

Se prueba:
  1. El módulo puro `ot_firma_vigencia.py` con los ejemplos del calendario (jueves→lunes, viernes→martes, sábado→miércoles 00:00, feriado en medio).
  2. Las funciones REALES de app.py: el token vence EXACTAMENTE en el instante calculado, y los demás enlaces (el documento firmado, 30 días; el de
     siempre, 120 h) siguen igual.
  3. Cableado: el envío por correo, por WhatsApp y por ambos canales dicen hasta cuándo sirve el enlace, ya nadie dice «5 días», y el aviso del
     modal de la ficha de la OT también.

Correr:  python -m pytest tests/test_ot_firma_vigencia.py -q -p no:warnings
"""
import datetime as dt
import re
import types
import unittest
import zoneinfo

import ot_firma_vigencia as V
from tests import _fuente_codigo as FC

TZ = zoneinfo.ZoneInfo("America/Santiago")
HABIL = lambda d: d.weekday() < 5                         # noqa: E731  lunes a viernes, sin feriados


def F(d, h=0, m=0):
    """Noviembre de 2026: jue 5 · vie 6 · sáb 7 · dom 8 · lun 9 · mar 10 · mié 11 · jue 12."""
    return dt.datetime(2026, 11, d, h, m)


class TestCalendario(unittest.TestCase):
    def test_enviado_el_jueves_vence_el_lunes_a_la_misma_hora(self):
        # viernes completo (24 h, desde las 15:00 del jueves son 9 h + 24 h = 33 h) + el lunes hasta las 15:00 (15 h) = 48 h; sábado y domingo no cuentan
        self.assertEqual(V.vence_el(F(5, 15, 0), HABIL), F(9, 15, 0))

    def test_enviado_el_viernes_vence_el_martes(self):
        self.assertEqual(V.vence_el(F(6, 15, 0), HABIL), F(10, 15, 0))

    def test_enviado_un_dia_de_semana_corriente_vence_dos_dias_despues(self):
        self.assertEqual(V.vence_el(F(9, 9, 0), HABIL), F(11, 9, 0))                       # lunes 09:00 → miércoles 09:00
        self.assertEqual(V.vence_el(F(10, 17, 30), HABIL), F(12, 17, 30))

    def test_enviado_en_fin_de_semana_empieza_a_contar_el_lunes_a_las_00(self):
        for desde in (F(7, 10, 0), F(7, 23, 59), F(8, 0, 0), F(8, 18, 0)):                 # sábado y domingo
            self.assertEqual(V.vence_el(desde, HABIL), F(11, 0, 0), desde)                  # miércoles 00:00 = fin del martes

    def test_un_feriado_en_medio_tampoco_cuenta(self):
        habil = lambda d: d.weekday() < 5 and d != dt.date(2026, 11, 10)                   # martes 10 feriado (solo para la prueba)
        self.assertEqual(V.vence_el(F(9, 9, 0), habil), F(12, 9, 0))                        # lunes 09:00 → jueves 09:00

    def test_con_un_puente_largo_no_se_pierde(self):
        habil = lambda d: d.weekday() < 5 and d not in (dt.date(2026, 11, 9), dt.date(2026, 11, 10))
        # viernes 12:00 → 12 h ese día; sábado, domingo, lunes y martes no cuentan; miércoles completo (24 h) = 36 h; jueves hasta las 12:00 = 48 h
        self.assertEqual(V.vence_el(F(6, 12, 0), habil), F(12, 12, 0))

    def test_sin_calendario_de_feriados_usa_lunes_a_viernes(self):
        def roto(d):
            raise RuntimeError("no hay calendario")
        self.assertEqual(V.vence_el(F(5, 15, 0), roto), F(9, 15, 0))

    def test_cero_horas_es_ahora_y_el_tope_evita_un_bucle_infinito(self):
        self.assertEqual(V.vence_el(F(5, 15, 0), HABIL, horas=0), F(5, 15, 0))
        nunca = lambda d: False                                                             # un calendario sin días hábiles no cuelga el envío   # noqa: E731
        V.vence_el(F(5, 15, 0), nunca)

    def test_el_texto_dice_el_dia_la_fecha_y_la_hora_de_chile(self):
        self.assertEqual(V.texto_vence(F(9, 15, 0)), "el lunes 09/11/2026 a las 15:00")
        self.assertEqual(V.texto_vence(F(10, 9, 5)), "el martes 10/11/2026 a las 09:05")
        # un vencimiento a las 00:00 se dice como el final del día anterior, para que nadie entienda que le queda un día más
        self.assertEqual(V.texto_vence(F(11, 0, 0)), "el martes 10/11/2026 a las 23:59")

    def test_el_plazo_es_de_48_horas_habiles(self):
        self.assertEqual((V.HORAS_VIGENCIA, V.TEXTO_PLAZO), (48, "48 horas hábiles"))


# ═══════════════════════════════════════════════════════════════════════════════════════════════════
#  Las funciones REALES de app.py
# ═══════════════════════════════════════════════════════════════════════════════════════════════════
class _Reloj:
    def __init__(self, t):
        self.t = t

    def time(self):
        return self.t


class TestTokenReal(unittest.TestCase):
    def setUp(self):
        self.reloj = _Reloj(dt.datetime(2026, 11, 5, 15, 0, tzinfo=TZ).timestamp())
        self.amb = {"app": types.SimpleNamespace(secret_key="clave-de-prueba"), "time": self.reloj, "_ofv": V,
                    "print": lambda *a, **k: None}
        FC.extraer(self.amb, "app.py", ("_ot_firma_key", "_ot_firma_token", "_ot_firma_token_validar", "_ot_firma_remota_vigencia"), ())
        self.amb["_now_chile"] = lambda: dt.datetime.fromtimestamp(self.reloj.t, TZ)

    def test_el_token_de_la_firma_remota_vence_exactamente_en_el_instante_que_se_dice(self):
        epoch, txt = self.amb["_ot_firma_remota_vigencia"]()
        self.assertEqual(txt, "el lunes 09/11/2026 a las 15:00")
        self.assertEqual(epoch, int(dt.datetime(2026, 11, 9, 15, 0, tzinfo=TZ).timestamp()))
        tok = self.amb["_ot_firma_token"](77, exp_epoch=epoch)
        self.reloj.t = epoch - 60
        self.assertEqual(self.amb["_ot_firma_token_validar"](tok), 77)                       # un minuto antes: sirve
        self.reloj.t = epoch + 1
        self.assertIsNone(self.amb["_ot_firma_token_validar"](tok))                          # un segundo después: venció

    def test_un_token_alterado_no_sirve_aunque_se_le_estire_la_fecha(self):
        import base64
        epoch, _ = self.amb["_ot_firma_remota_vigencia"]()
        tok = self.amb["_ot_firma_token"](77, exp_epoch=epoch)
        raw = base64.urlsafe_b64decode(tok + "=" * (-len(tok) % 4)).decode()
        vid, exp, sig = raw.split(":")
        falso = base64.urlsafe_b64encode(f"{vid}:{int(exp) + 10 * 86400}:{sig}".encode()).decode().rstrip("=")
        self.assertIsNone(self.amb["_ot_firma_token_validar"](falso))

    def test_los_otros_enlaces_siguen_con_su_vigencia_de_siempre(self):
        tok = self.amb["_ot_firma_token"](77)                                                  # por omisión: 120 horas corridas
        self.reloj.t += 119 * 3600
        self.assertEqual(self.amb["_ot_firma_token_validar"](tok), 77)
        self.reloj.t += 2 * 3600
        self.assertIsNone(self.amb["_ot_firma_token_validar"](tok))
        self.reloj.t = dt.datetime(2026, 11, 5, 15, 0, tzinfo=TZ).timestamp()
        largo = self.amb["_ot_firma_token"](77, ttl_horas=24 * 30)                             # el documento firmado: 30 días
        self.reloj.t += 29 * 86400
        self.assertEqual(self.amb["_ot_firma_token_validar"](largo), 77)

    def test_si_el_calendario_falla_cae_a_48_horas_corridas_y_no_rompe_el_envio(self):
        self.amb["_ofv"] = types.SimpleNamespace(HORAS_VIGENCIA=48, vence_el=lambda *a, **k: 1 / 0, texto_vence=V.texto_vence)
        epoch, txt = self.amb["_ot_firma_remota_vigencia"]()
        self.assertEqual(epoch, int(self.reloj.t) + 48 * 3600)
        self.assertEqual(txt, "en 48 horas")


# ═══════════════════════════════════════════════════════════════════════════════════════════════════
#  Cableado
# ═══════════════════════════════════════════════════════════════════════════════════════════════════
class TestCableado(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.envio = FC.funcion_top("app.py", "mant_ot_enviar_firma_remota")
        cls.app = FC.texto("app.py")

    def test_el_envio_fija_la_expiracion_con_las_48_horas_habiles(self):
        self.assertIn("_exp_firma, _vence_txt = _ot_firma_remota_vigencia()", self.envio)
        self.assertIn("_ot_firma_token(vid, exp_epoch=_exp_firma)", self.envio)
        self.assertNotIn("_ot_firma_token(vid)", self.envio)

    def test_ningun_texto_de_la_firma_remota_dice_5_dias(self):
        codigo = "\n".join(ln for ln in self.envio.splitlines() if not ln.strip().startswith("#"))     # los comentarios cuentan la historia; el código no
        self.assertNotRegex(codigo, r"5 d[ií]as")
        # WhatsApp, correo, respaldo del correo y los mensajes de respuesta del correo y de «ambos canales»
        self.assertEqual(self.envio.count("{_vence_txt}"), 5)
        self.assertIn("{_ofv.TEXTO_PLAZO}", self.envio)

    def test_las_tres_respuestas_dicen_hasta_cuando_sirve_el_enlace(self):
        self.assertEqual(self.envio.count('"vence": _vence_txt'), 3)                           # whatsapp, email y ambos
        self.assertIn("El enlace vence {_vence_txt}", self.envio)

    def test_el_modal_de_la_ficha_de_la_ot_dice_48_horas_habiles(self):
        html = FC.texto("templates/ot2/detalle.html")
        self.assertIn("El link vence en 48 horas hábiles", html)
        self.assertNotIn("El link vence en 5 días", html)

    def test_el_documento_firmado_y_la_vista_publica_no_cambian(self):
        com = FC.funcion_top("app.py", "_ot_com_enlace_firmada")
        self.assertIn("ttl_horas=24 * _ocom.TOKEN_DIAS", com)                                   # 30 días, REGLA #37
        self.assertNotIn("exp_epoch", com)
        self.assertEqual(len(re.findall(r"_ot_firma_remota_vigencia\(", self.app)), 2)        # la definición y el envío: nadie más

    def test_el_modulo_puro_no_depende_de_nada_pesado(self):
        src = FC.texto("ot_firma_vigencia.py")
        self.assertNotIn("import flask", src.lower())
        self.assertNotIn("pymysql", src)


if __name__ == "__main__":
    unittest.main()

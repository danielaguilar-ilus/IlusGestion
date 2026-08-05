"""Pruebas de _tr_proximo_slot_sync() / _tr_horas_sync_cfg() — cadencia del
sync automático de transporte.

Daniel, 2026-08-05 (cambio de cadencia): "dejaría a las nueve, once, una,
tres y cuatro... de lunes a viernes... extenderlo fuera de horario laboral
o fines de semana es innecesario".

El caso que rompe si el cálculo del próximo turno se hace "a mano" sumando
un día (como hacía el código viejo): un viernes después del último turno
tiene que saltar a LUNES 09:00, no a sábado. Sin solo_habiles, saltaría a
sábado 09:00 -- exactamente lo que Daniel pidió evitar.

No se importa app.py directamente: al nivel de módulo abre conexiones a
BD/GCS con credenciales que no existen en este entorno. Se extrae la fuente
real de las dos funciones con `ast` y se ejecuta aislada -- mismo patrón que
tests/test_password_email_templates.py, ya validado en este repo. Así los
tests corren sin BD y verifican el código que de verdad se despliega, no una
copia que se puede desincronizar.

Correr con:  py -m unittest tests.test_transporte_scheduler -v
"""
import ast
import datetime as dt
import os
import unittest
from zoneinfo import ZoneInfo

APP_SRC = open("app.py", encoding="utf-8", errors="ignore").read()
_ARBOL = ast.parse(APP_SRC)


def _extraer_funcion(nombre):
    for nodo in ast.walk(_ARBOL):
        if isinstance(nodo, ast.FunctionDef) and nodo.name == nombre:
            return ast.unparse(nodo)
    raise AssertionError(f"No se encontró la función '{nombre}' en app.py")


_NS = {"os": os, "ZoneInfo": ZoneInfo, "print": print}
exec(_extraer_funcion("_tr_horas_sync_cfg"), _NS)
exec(_extraer_funcion("_tr_proximo_slot_sync"), _NS)
_tr_horas_sync_cfg = _NS["_tr_horas_sync_cfg"]
_tr_proximo_slot_sync = _NS["_tr_proximo_slot_sync"]

TZ = ZoneInfo("America/Santiago")
HORAS = [9, 11, 13, 15, 16]


def _cl(y, m, d, h, mi=0):
    return dt.datetime(y, m, d, h, mi, tzinfo=TZ)


class TestHorasSyncCfg(unittest.TestCase):
    def setUp(self):
        os.environ.pop("ILUS_TR_SYNC_HORAS", None)

    def tearDown(self):
        os.environ.pop("ILUS_TR_SYNC_HORAS", None)

    def test_default_es_9_11_13_15_16(self):
        self.assertEqual([9, 11, 13, 15, 16], _tr_horas_sync_cfg())

    def test_se_puede_ajustar_por_env_sin_deploy(self):
        os.environ["ILUS_TR_SYNC_HORAS"] = "16,9,13"
        self.assertEqual([9, 13, 16], _tr_horas_sync_cfg())  # ordenadas

    def test_env_invalida_cae_al_default_sin_lanzar(self):
        os.environ["ILUS_TR_SYNC_HORAS"] = "nueve,once"
        self.assertEqual([9, 11, 13, 15, 16], _tr_horas_sync_cfg())

    def test_horas_fuera_de_rango_se_descartan(self):
        os.environ["ILUS_TR_SYNC_HORAS"] = "9,25,-1,13"
        self.assertEqual([9, 13], _tr_horas_sync_cfg())


class TestProximoSlotDentroDelDia(unittest.TestCase):
    def test_antes_del_primer_turno(self):
        self.assertEqual(_cl(2026, 8, 5, 9, 0),
                          _tr_proximo_slot_sync(_cl(2026, 8, 5, 8, 0), HORAS, True))

    def test_entre_dos_turnos_va_al_siguiente(self):
        # 2026-08-05 es miércoles.
        self.assertEqual(_cl(2026, 8, 5, 13, 0),
                          _tr_proximo_slot_sync(_cl(2026, 8, 5, 11, 30), HORAS, True))

    def test_justo_en_un_turno_no_se_repite_a_si_mismo(self):
        # A las 11:00 en punto, el próximo turno es 13:00, no 11:00 de nuevo.
        self.assertEqual(_cl(2026, 8, 5, 13, 0),
                          _tr_proximo_slot_sync(_cl(2026, 8, 5, 11, 0), HORAS, True))


class TestFinDeSemana(unittest.TestCase):
    """El caso que rompía el cálculo viejo (sumar un día a mano)."""

    def test_viernes_despues_del_ultimo_turno_salta_a_lunes(self):
        # 2026-08-07 es viernes. Después de las 16:00, el próximo turno
        # DEBE ser el lunes 2026-08-10 a las 09:00 -- no sábado.
        viernes_tarde = _cl(2026, 8, 7, 16, 30)
        resultado = _tr_proximo_slot_sync(viernes_tarde, HORAS, True)
        self.assertEqual(_cl(2026, 8, 10, 9, 0), resultado)
        self.assertEqual(0, resultado.weekday())  # lunes

    def test_sabado_a_cualquier_hora_salta_a_lunes(self):
        sabado = _cl(2026, 8, 8, 14, 0)  # 2026-08-08 es sábado
        resultado = _tr_proximo_slot_sync(sabado, HORAS, True)
        self.assertEqual(_cl(2026, 8, 10, 9, 0), resultado)

    def test_domingo_noche_salta_a_lunes(self):
        domingo = _cl(2026, 8, 9, 23, 0)  # 2026-08-09 es domingo
        resultado = _tr_proximo_slot_sync(domingo, HORAS, True)
        self.assertEqual(_cl(2026, 8, 10, 9, 0), resultado)

    def test_sin_solo_habiles_si_corre_el_sabado(self):
        # Confirma que el flag realmente controla el comportamiento: en
        # False, un viernes de noche SÍ cae en sábado 09:00.
        viernes_tarde = _cl(2026, 8, 7, 16, 30)
        resultado = _tr_proximo_slot_sync(viernes_tarde, HORAS, False)
        self.assertEqual(_cl(2026, 8, 8, 9, 0), resultado)
        self.assertEqual(5, resultado.weekday())  # sábado


class TestUltimoTurnoDelDia(unittest.TestCase):
    def test_despues_de_las_16_dentro_de_semana_pasa_al_dia_siguiente(self):
        # 2026-08-05 es miércoles: el día siguiente es jueves, hábil.
        self.assertEqual(_cl(2026, 8, 6, 9, 0),
                          _tr_proximo_slot_sync(_cl(2026, 8, 5, 16, 1), HORAS, True))


class TestNuncaLanzaConEntradasRazonables(unittest.TestCase):
    def test_una_sola_hora_configurada(self):
        r = _tr_proximo_slot_sync(_cl(2026, 8, 5, 10, 0), [9], True)
        self.assertEqual(_cl(2026, 8, 6, 9, 0), r)

    def test_horas_desordenadas_igual_encuentra_la_correcta(self):
        r = _tr_proximo_slot_sync(_cl(2026, 8, 5, 12, 0), [16, 9, 13], True)
        self.assertEqual(_cl(2026, 8, 5, 13, 0), r)


if __name__ == "__main__":
    unittest.main(verbosity=2)

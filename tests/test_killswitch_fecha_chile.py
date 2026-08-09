"""La "Última actualización" de la Llave de paso (Comunicaciones) debía
mostrarse en hora Chile Y en formato día/mes/año, y solo cumplía lo primero.

BUG REAL (2026-08-09, Daniel: "ultima actualizacion en fecha gringa
arreglalo"). Ejemplo real de la captura: "2026-05-18 07:14:35" para el
módulo "Comunicación interna".

Un fix anterior (2026-07-25) ya había corregido la CONVERSIÓN de huso
horario -- antes se mostraba la hora cruda en UTC, "en el futuro" respecto
al reloj de Santiago. Pero el string de formato que se le pasaba a
chile_fmt_filter seguía siendo "%Y-%m-%d %H:%M:%S" (orden año-mes-día, el
mismo que usa MySQL/ISO), en vez de "%d/%m/%Y %H:%M:%S" como exige la
Regla #6 de CLAUDE.md ("ninguna fecha se muestra jamás en inglés ni en
formato ISO crudo") y como se ve en el resto del proyecto.

Se verifica sobre el código real de app.py (ast.unparse), reproduciendo el
mismo caso de la captura (18 de mayo, un mes SIN horario de verano en
Chile) para no depender de asumir el offset UTC vigente.

Correr con:  py -m unittest tests.test_killswitch_fecha_chile -v
"""
import ast
import datetime
import re
import unittest
import zoneinfo

APP_SRC = open("app.py", encoding="utf-8", errors="ignore").read()
_ARBOL = ast.parse(APP_SRC)


def _extraer_funcion(nombre):
    for nodo in ast.walk(_ARBOL):
        if isinstance(nodo, ast.FunctionDef) and nodo.name == nombre:
            return ast.unparse(nodo)
    raise AssertionError(f"No se encontró la función '{nombre}' en app.py")


class _AppStub:
    """Doble mínimo de Flask `app`: solo lo que necesita el decorador
    @app.template_filter(...) para no romper el exec."""

    def template_filter(self, *a, **k):
        return lambda fn: fn


_DIAS_ES = ["Lunes", "Martes", "Miercoles", "Jueves", "Viernes", "Sabado", "Domingo"]
_DIAS_ES_ABBR = ["Lun", "Mar", "Mie", "Jue", "Vie", "Sab", "Dom"]
_MESES_ES = ["enero", "febrero", "marzo", "abril", "mayo", "junio", "julio", "agosto",
             "septiembre", "octubre", "noviembre", "diciembre"]

_NS = {"datetime": datetime, "zoneinfo": zoneinfo, "app": _AppStub(),
       "_DIAS_ES": _DIAS_ES, "_DIAS_ES_ABBR": _DIAS_ES_ABBR, "_MESES_ES": _MESES_ES}
exec(_extraer_funcion("to_chile_filter"), _NS)
exec(_extraer_funcion("chile_fmt_filter"), _NS)
chile_fmt = _NS["chile_fmt_filter"]


class TestKillswitchUsaFormatoChileno(unittest.TestCase):

    def test_el_endpoint_ya_no_pide_formato_iso(self):
        """El bug real: el código pasaba "%Y-%m-%d" (orden ISO) al filtro
        de hora Chile. La conversión de huso horario estaba bien; el
        formato de texto era el gringo.

        ast.unparse normaliza las comillas a simples, así que se compara
        sin comillas para no depender de ese detalle de formato."""
        fn = _extraer_funcion("comm_killswitch_get")
        self.assertNotIn("%Y-%m-%d %H:%M:%S", fn,
                         "volvió el formato ISO/gringo en 'Última actualización'")
        self.assertIn("%d/%m/%Y %H:%M:%S", fn)

    def test_una_fecha_real_sale_en_dd_mm_yyyy(self):
        # Caso real de la captura: 18 de mayo (Chile en UTC-4, sin DST ese mes).
        utc_dt = datetime.datetime(2026, 5, 18, 10, 14, 35,
                                   tzinfo=zoneinfo.ZoneInfo("UTC"))
        resultado = chile_fmt(utc_dt, "%d/%m/%Y %H:%M:%S")
        self.assertRegex(resultado, r"^\d{2}/\d{2}/2026 \d{2}:\d{2}:\d{2}$")
        self.assertFalse(resultado.startswith("2026-"),
                         "la fecha sigue saliendo en formato ISO (yyyy-mm-dd)")

    def test_la_conversion_de_huso_horario_no_se_rompio(self):
        """No es un test nuevo de ese fix (ya existía desde 2026-07-25) --
        es una guarda para no arreglar el formato rompiendo la conversión."""
        utc_dt = datetime.datetime(2026, 5, 18, 10, 14, 35,
                                   tzinfo=zoneinfo.ZoneInfo("UTC"))
        resultado = chile_fmt(utc_dt, "%d/%m/%Y %H:%M:%S")
        hora = int(resultado.split(" ")[1].split(":")[0])
        self.assertLess(hora, 10,
                        "la hora ya no se está convirtiendo de UTC a Chile")


if __name__ == "__main__":
    unittest.main(verbosity=2)

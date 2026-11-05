# -*- coding: utf-8 -*-
"""📍 6-nov-2026 — «no trae la dirección, siempre trae La Serena»: el ERP guarda la comuna como código de 3 letras que varias comunas
comparten («LAS» = La Serena, Las Cabras, Las Condes). El resolvedor se quedaba con la primera (La Serena) sin mirar la región (CIEN).
Correr:  python -m pytest tests/test_comuna_ambigua_erp.py -q -p no:warnings"""
import os
import re
import sys
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, RAIZ)

import erp_engine  # noqa: E402

with open(os.path.join(RAIZ, "app.py"), encoding="utf-8") as fh:
    _APP = fh.read().replace("\r\n", "\n")


def _resolvedor(cache):
    """La función REAL de app.py, ejecutada aislada con un catálogo TABCM de mentira (sin ERP, sin base)."""
    m = re.search(r"def _resolve_comuna_erp\(.*?\n(?=\n\ndef )", _APP, re.S)
    assert m, "no se encontró _resolve_comuna_erp"
    ns = {"_TABCM_CACHE": cache, "_random_load_tabcm_cache": lambda: True, "erp_engine": erp_engine}
    exec(m.group(0), ns)
    return ns["_resolve_comuna_erp"]


def _cache(por_codigo, completo=None):
    return {"by_kopa_koci_kocm": completo or {}, "by_kocm_any": por_codigo, "loaded_at": 1}


class TestComunaAmbigua(unittest.TestCase):
    def test_las_en_region_metropolitana_es_las_condes_no_la_serena(self):
        r = _resolvedor(_cache({"LAS": ["LA SERENA", "LAS CABRAS", "LAS CONDES"]}))
        self.assertEqual(r("LAS", "013"), "Las Condes")          # caso real: GDV 34190 de Desarrollos Inmobiliarios Iv Spa

    def test_la_misma_sigla_en_otra_region_se_respeta(self):
        r = _resolvedor(_cache({"LAS": ["LA SERENA", "LAS CABRAS", "LAS CONDES"]}))
        self.assertEqual(r("LAS", "004"), "La Serena")
        self.assertEqual(r("LAS", "6"), "Las Cabras")

    def test_el_match_exacto_de_tabcm_sigue_mandando(self):
        r = _resolvedor(_cache({"LAS": ["LA SERENA", "LAS CONDES"]}, {"CL|013|LAS": "LAS CONDES (TABCM)"}))
        self.assertEqual(r("LAS", "013"), "LAS CONDES (TABCM)")

    def test_un_codigo_sin_ambiguedad_no_cambia(self):
        r = _resolvedor(_cache({"TAL": ["TALCA"]}))
        self.assertEqual(r("TAL", "007"), "TALCA")

    def test_si_la_region_no_ayuda_queda_el_comportamiento_de_antes(self):
        r = _resolvedor(_cache({"LAS": ["LA SERENA", "LAS CONDES"]}))
        self.assertEqual(r("LAS", ""), "LA SERENA")               # sin región no hay con qué desempatar
        self.assertEqual(r("LAS", "099"), "LA SERENA")            # región desconocida

    def test_la_correccion_usa_el_modulo_importado_arriba_no_un_import_local(self):
        i = _APP.index("🔧 2026-11-06 (Daniel: «no trae la dirección, siempre trae La Serena»)")
        f = _APP[i:i + 1400]
        self.assertIn("erp_engine.CMEN_MAP", f)                   # `_erpe` es un import LOCAL de otras funciones: aquí daría NameError
        self.assertNotIn("_erpe.", f)


if __name__ == "__main__":
    unittest.main()

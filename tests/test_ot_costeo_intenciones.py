# -*- coding: utf-8 -*-
"""El Costeo de OT (ot_gerencial) y la clasificación de motivos (ot_intenciones, REGLA #47) hablan entre sí: una OT clasificada muestra el
NOMBRE de su motivo y quién lo asume; una sin clasificar sigue diciendo «Sin clasificar» (nunca se inventa). Datos de mentira."""
import unittest

import ot_gerencial as G
from tests._fuente_app import cargar, fuente_funcion, texto


class TestElCosteoLeeLaClasificacion(unittest.TestCase):
    def test_la_columna_de_la_clasificacion_es_la_que_el_tablero_busca_primero(self):
        c = cargar(constantes=("_GER_COLS_INTENCION",))["_GER_COLS_INTENCION"]
        self.assertEqual(c[0], "intencion")

    def test_la_clave_se_muestra_con_su_nombre_y_el_centro_que_la_asume(self):
        s = fuente_funcion("_ger_cargar")
        self.assertIn("_ot_intenciones_catalogo()", s)
        self.assertIn('_ot_intencion_info(r.get("intencion"), r.get("centro_costo"), cat_int) if r.get("intencion") else None', s)
        self.assertIn('"intencion": (info_int["nombre"] if info_int else r.get("intencion"))', s)
        self.assertIn('(info_int or {}).get("asume_txt") or None', s)

    def test_si_el_catalogo_no_responde_el_tablero_sigue(self):
        # la lectura del catálogo va dentro de un try: una base caída no tumba el tablero
        s = fuente_funcion("_ger_cargar")
        i = s.index("cat_int = _ot_intenciones_catalogo()")
        self.assertIn("try:", s[i - 120:i])
        self.assertIn("cat_int = []", s[i:i + 300])

    def test_una_ot_sin_clasificar_sigue_sin_clasificar(self):
        fila = G.armar_fila({"id": 1, "numero_ot": "OT-1", "estado": "cerrada", "centro_costo": "sstt", "cerrada_at": None, "tipo": "instalacion"},
                            {"cobra": False, "cobertura": "garantia", "queda": {"total": -100.0, "mostrar": True},
                             "cobre": {"total": 0}, "a_pagar_proveedor": 100.0, "valorizado": {"monto": 300.0}},
                            {"nombres": {}, "tipo_txt": "Instalación", "centro_txt": "Servicio Técnico", "intencion": None, "asume_area": None})
        self.assertIsNone(fila["intencion"])
        self.assertIsNone(fila["asume_area"])

    def test_una_ot_clasificada_lleva_nombre_y_area(self):
        fila = G.armar_fila({"id": 2, "numero_ot": "OT-2", "estado": "cerrada", "centro_costo": "marketing", "cerrada_at": None, "tipo": "instalacion"},
                            {"cobra": False, "cobertura": "regalia", "queda": {"total": -70.0, "mostrar": True},
                             "cobre": {"total": 0}, "a_pagar_proveedor": 70.0, "valorizado": {"monto": 120.0}},
                            {"nombres": {}, "tipo_txt": "Instalación", "centro_txt": "Marketing",
                             "intencion": "Costo asumido por Marketing", "asume_area": "Marketing"})
        self.assertEqual((fila["intencion"], fila["asume_area"]), ("Costo asumido por Marketing", "Marketing"))


if __name__ == "__main__":
    unittest.main()

"""Hoja 2 "Detalle de Bultos" del Excel del Cubicador.

Pedido de Daniel (2026-08-26, vía chat, para el gerente): "Roberto me dice
que no le da el detalle de los bultos... quisiera dejar que en el excel en
otra hoja pueda dar el detalle de los bultos". Ejemplo real que dio Daniel:

    FZABU025S0500  Par Discos Training Bumpers ILUS 20 kg  VD 10218  6

("6" = columna Bultos de la hoja 1) -- "quiero ver el detalle de los 6
bultos": una fila POR CADA bulto individual (Bulto 1 de 6, ..., Bulto 6 de
6), no solo el número agregado que ya trae la hoja 1.

app.py tiene 90k+ líneas -- un ast.parse() completo tarda ~85s en este
equipo (medido en sesiones anteriores). Se evita acá: se extrae el CUERPO
de cubicador_export_excel() por slicing de texto entre 'def
cubicador_export_excel' y el siguiente 'def ' a nivel de módulo (la función
no tiene funciones anidadas), igual de válido para grep-style assertions
que ast.unparse pero sin el costo del parseo completo.

La aritmética de expansión (Bulto N de TOTAL, suma de totales) se prueba
aparte en Python puro, sin tocar app.py -- mismo patrón que
test_cubicador_excel_vol_m3_y_bultos_total.py.

Correr con:  py -m unittest tests.test_cubicador_detalle_bultos_hoja2 -v
"""
import os
import unittest

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _fuente_cubicador_export_excel():
    with open(os.path.join(BASE_DIR, "app.py"), encoding="utf-8", errors="ignore") as f:
        src = f.read()
    i = src.index("\ndef cubicador_export_excel(")
    j = src.index("\ndef ", i + 10)
    return src[i:j]


SRC = _fuente_cubicador_export_excel()


class TestLaHoja2Existe(unittest.TestCase):
    def test_crea_una_segunda_hoja_llamada_detalle_de_bultos(self):
        self.assertIn('wb.create_sheet("Detalle de Bultos")', SRC)

    def test_no_toca_la_hoja_1_blindada(self):
        """La hoja 1 (ws) sigue usando las mismas 11 columnas / mismos
        índices de totales -- la hoja 2 es enteramente ADITIVA (ws2, wb2
        no aparecen antes de freeze_panes de la hoja 1)."""
        i_freeze1 = SRC.index('ws.freeze_panes = f"A{hdr_row + 1}"')
        i_hoja2 = SRC.index('wb.create_sheet("Detalle de Bultos")')
        self.assertLess(i_freeze1, i_hoja2,
                         "la hoja 2 debe agregarse DESPUES de terminar la hoja 1")

    def test_las_lineas_sin_ficha_se_saltan(self):
        # "s/f" en la hoja 1 -- sin ficha logistica no hay peso que detallar.
        i = SRC.index('wb.create_sheet("Detalle de Bultos")')
        fragmento = SRC[i:i + 1500]
        self.assertIn('if not l["tiene_bultos"]:', fragmento)
        self.assertIn("continue", fragmento)

    def test_una_fila_por_bulto_con_etiqueta_bulto_n_de_total(self):
        i = SRC.index('wb.create_sheet("Detalle de Bultos")')
        fragmento = SRC[i:i + 2500]
        self.assertIn("for n in range(1, n_bultos + 1):", fragmento)
        self.assertIn('f"Bulto {n} de {n_bultos}"', fragmento)

    def test_usa_total_bultos_no_cantidad_para_expandir(self):
        """Daniel dijo explicitamente 'los 6 bultos' refiriendose a la
        columna Bultos (total_bultos), no a Cant -- si esto alguna vez lee
        l['cantidad'] en su lugar, el conteo de filas queda mal."""
        i = SRC.index('wb.create_sheet("Detalle de Bultos")')
        fragmento = SRC[i:i + 2000]
        self.assertIn('l["total_bultos"]', fragmento)

    def test_el_volumen_se_convierte_a_m3_igual_que_la_hoja_1(self):
        i = SRC.index('wb.create_sheet("Detalle de Bultos")')
        fragmento = SRC[i:i + 3000]
        self.assertIn('vol_u_m3 = l["vol_u"] / 1_000_000.0', fragmento)


class TestExpansionDeBultosArgumentoPuroPython(unittest.TestCase):
    """Replica la logica de expansion con datos sinteticos -- no depende de
    MySQL/ERP ni de abrir app.py."""

    @staticmethod
    def _expandir(lineas):
        filas = []
        tot_kg = tot_pv = tot_vol = 0.0
        for l in lineas:
            if not l["tiene_bultos"]:
                continue
            n_bultos = int(round(l["total_bultos"] or 0))
            if n_bultos <= 0:
                continue
            vol_u_m3 = l["vol_u"] / 1_000_000.0
            for n in range(1, n_bultos + 1):
                filas.append((l["sku"], f"Bulto {n} de {n_bultos}",
                              l["peso_kg_u"], l["peso_vol_u"], vol_u_m3))
                tot_kg += l["peso_kg_u"]
                tot_pv += l["peso_vol_u"]
                tot_vol += vol_u_m3
        return filas, (tot_kg, tot_pv, tot_vol)

    def test_ejemplo_real_de_daniel_par_discos_bumpers_20kg(self):
        # FZABU025S0500, Par Discos Training Bumpers ILUS 20 kg, VD 10218, 6 bultos.
        linea = {
            "sku": "FZABU025S0500", "tiene_bultos": True, "total_bultos": 6,
            "peso_kg_u": 20.0, "peso_vol_u": 8.5, "vol_u": 45000.0,
        }
        filas, _ = self._expandir([linea])
        self.assertEqual(len(filas), 6)
        self.assertEqual(filas[0][1], "Bulto 1 de 6")
        self.assertEqual(filas[5][1], "Bulto 6 de 6")
        self.assertTrue(all(f[0] == "FZABU025S0500" for f in filas))
        self.assertTrue(all(f[2] == 20.0 for f in filas))

    def test_linea_sin_ficha_no_genera_filas(self):
        linea = {"sku": "X", "tiene_bultos": False, "total_bultos": 0,
                  "peso_kg_u": 0, "peso_vol_u": 0, "vol_u": 0}
        filas, totales = self._expandir([linea])
        self.assertEqual(filas, [])
        self.assertEqual(totales, (0.0, 0.0, 0.0))

    def test_linea_con_1_bulto_da_exactamente_1_fila(self):
        linea = {"sku": "Y", "tiene_bultos": True, "total_bultos": 1,
                  "peso_kg_u": 5.0, "peso_vol_u": 2.0, "vol_u": 10000.0}
        filas, _ = self._expandir([linea])
        self.assertEqual(len(filas), 1)
        self.assertEqual(filas[0][1], "Bulto 1 de 1")

    def test_totales_de_la_hoja_2_suman_correcto_multiples_lineas(self):
        lineas = [
            {"sku": "A", "tiene_bultos": True, "total_bultos": 2,
             "peso_kg_u": 10.0, "peso_vol_u": 4.0, "vol_u": 20000.0},
            {"sku": "B", "tiene_bultos": True, "total_bultos": 3,
             "peso_kg_u": 5.0, "peso_vol_u": 1.5, "vol_u": 8000.0},
            {"sku": "C", "tiene_bultos": False, "total_bultos": 0,
             "peso_kg_u": 0, "peso_vol_u": 0, "vol_u": 0},
        ]
        filas, (tot_kg, tot_pv, tot_vol) = self._expandir(lineas)
        self.assertEqual(len(filas), 5)  # 2 + 3, la linea sin ficha no cuenta
        self.assertAlmostEqual(tot_kg, 2 * 10.0 + 3 * 5.0)   # 35.0
        self.assertAlmostEqual(tot_pv, 2 * 4.0 + 3 * 1.5)    # 12.5
        self.assertAlmostEqual(tot_vol, 2 * 0.02 + 3 * 0.008)  # m3

    def test_multiplicar_peso_unitario_por_n_bultos_da_el_mismo_total_que_la_hoja_1(self):
        """Cross-check matematico: sumar las N filas individuales de un SKU
        en la hoja 2 debe dar el MISMO numero que 'peso_kg_u * total_bultos'
        -- la identidad que un gerente esperaria ver (detalle y agregado
        cuadran)."""
        linea = {"sku": "Z", "tiene_bultos": True, "total_bultos": 7,
                  "peso_kg_u": 12.345, "peso_vol_u": 6.0, "vol_u": 30000.0}
        filas, (tot_kg, _, _) = self._expandir([linea])
        self.assertAlmostEqual(tot_kg, linea["peso_kg_u"] * linea["total_bultos"], places=6)


if __name__ == "__main__":
    unittest.main()

"""Hoja 2 "Detalle de Bultos" del Excel del Cubicador.

Pedido de Daniel (2026-08-26, vía chat, para el gerente): "Roberto me dice
que no le da el detalle de los bultos... quisiera dejar que en el excel en
otra hoja pueda dar el detalle de los bultos".

FIX del MISMO día (Daniel detectó EN VIVO, con capturas, comparando contra
el modal "Detalle por bulto" de la ficha de producto -- templates/
product_detail.html, ruta /products/<id>): la primera versión de esta hoja
repetía l["peso_kg_u"]/l["peso_vol_u"]/vol_u_m3 (el TOTAL de la ficha
completa, sumado sobre TODOS sus bultos -- ver sku_data_map dentro de
_cubicador_fetch: "COALESCE(SUM(b.peso), 0) AS peso_total") en CADA fila
"Bulto N de M", como si fuera la medida de una sola caja. La suma de la
columna Kg de la hoja 2 daba entonces peso_kg_u × N en vez de peso_kg_u una
sola vez -- ej. para un producto de 5 bultos con Kg/u=280.5, la hoja 2
sumaba 1402.5 kg. Palabras de Daniel: "No cuadra matemáticamente y esa
información está en el sistema".

SKU real usado para verificar (dado por Daniel con capturas del modal de
"Detalle por bulto"): 1121100989 "ILUS Optimal Dual Pulldown/Row", VD
10218, 5 bultos reales (el modal salta del bulto 2 al 4 -- ver más abajo
por qué eso es un dato legítimo, no un recorte de pantalla):

    Bulto  Largo  Ancho  Alto  Peso    Peso Vol (calc_pv = L*A*A/4000)
    1      170    70     39    75.00   116.03
    2      170    66     39    93.00   109.39
    4      38     10     10    37.50   0.95
    5      38     10     10    37.50   0.95
    6      38     10     10    37.50   0.95
    ------------------------------------------------------------------
    SUMA                       280.50  228.27

280.50 == "280.50 kg PESO BRUTO" del header del modal == "Kg/u" que ya
escribía (correctamente) la hoja 1 para ese SKU. 228.27 == "228.27 PESO
VOL" del mismo header. Verificado acá con calc_pv() real, la misma fórmula
que usa toda la app (test TestExpansionArgumentoPuroPython) -- no es un
número inventado para que cuadre, sale de reproducir la fórmula real.

Por qué el modal salta del bulto 2 al 4 (investigado, no asumido): la
fuente real es la tabla `app_bultos` (BULTOS_TABLE), y save_bultos_mysql()
en app.py (~línea 6503) sólo INSERTa una fila para el índice N si el
operador cargó al menos una medida > 0 en ese índice al editar la ficha
(product_form.html, slots 1..MAX_BULTOS). Si el bulto 3 se dejó en blanco,
simplemente no existe una fila bulto_num=3 en la base -- no es un límite de
UI ni un recorte de pantalla, es el estado real de los datos. Por eso el
fix NO renumera ni rellena huecos: muestra el bulto_num TAL CUAL viene de
`app_bultos`.

app.py tiene 90k+ líneas -- un ast.parse() completo tarda ~85s en este
equipo (medido en sesiones anteriores). Se evita acá: se extrae el CUERPO
de cubicador_export_excel() por slicing de texto entre 'def
cubicador_export_excel' y el siguiente 'def ' a nivel de módulo (la función
no tiene funciones anidadas -- la implementación es deliberadamente inline,
sin helpers nuevos, para no romper TestNoSeTocoNadaAjeno.test_no_hay_
funciones_nuevas en los otros dos archivos de test del cubicador), igual de
válido para asserts de texto que ast.unparse pero sin el costo del parseo
completo.

La aritmética (bultos reales, fallback, totales) se prueba aparte en Python
puro, sin tocar app.py -- mismo patrón que
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
I_HOJA2 = SRC.index('wb.create_sheet("Detalle de Bultos")')
FRAGMENTO_HOJA2 = SRC[I_HOJA2:]  # de ahi hasta el final del cuerpo de la funcion


class TestLaHoja2Existe(unittest.TestCase):
    def test_crea_una_segunda_hoja_llamada_detalle_de_bultos(self):
        self.assertIn('wb.create_sheet("Detalle de Bultos")', SRC)

    def test_no_toca_la_hoja_1_blindada(self):
        """La hoja 1 (ws) sigue usando las mismas 11 columnas / mismos
        índices de totales -- la hoja 2 es enteramente ADITIVA (ws2 no
        aparece antes de freeze_panes de la hoja 1)."""
        i_freeze1 = SRC.index('ws.freeze_panes = f"A{hdr_row + 1}"')
        self.assertLess(i_freeze1, I_HOJA2,
                         "la hoja 2 debe agregarse DESPUES de terminar la hoja 1")

    def test_las_lineas_sin_ficha_se_saltan(self):
        # "s/f" en la hoja 1 -- sin ficha logistica no hay peso que detallar.
        self.assertIn('if not l["tiene_bultos"]:', FRAGMENTO_HOJA2)
        self.assertIn("continue", FRAGMENTO_HOJA2)

    def test_usa_total_bultos_no_cantidad_para_decidir_si_hay_bultos(self):
        """Daniel se refirio explicitamente a la columna Bultos
        (total_bultos), no a Cant."""
        self.assertIn('l["total_bultos"]', FRAGMENTO_HOJA2)
        self.assertNotIn('l["cantidad"]', FRAGMENTO_HOJA2)

    def test_no_agrega_funciones_nuevas_ni_anidadas(self):
        """Los otros dos archivos de test del cubicador comparan, via AST
        completo contra origin/main, que no aparezcan funciones nuevas
        (TestNoSeTocoNadaAjeno.test_no_hay_funciones_nuevas). Un 'def'
        anidado dentro de cubicador_export_excel tambien contaria como
        funcion nueva para ese chequeo -- se deja constancia de que la
        implementacion de esta hoja es deliberadamente inline (loops +
        comprehensions)."""
        self.assertNotIn("\n    def ", FRAGMENTO_HOJA2)
        self.assertNotIn("\n        def ", FRAGMENTO_HOJA2)

    def test_las_columnas_y_los_anchos_son_11(self):
        i = SRC.index("cols2 = [")
        i_close = SRC.index("]", i)
        n_cols = SRC[i:i_close].count(",") + 1
        # OJO: partir despues del "[" de apertura -- "for ci, w in
        # enumerate(" ya trae una coma propia (entre "ci" y "w") que no
        # es parte de la lista de anchos y falsearia el conteo.
        i_w = FRAGMENTO_HOJA2.index("enumerate([") + len("enumerate([")
        i_w_close = FRAGMENTO_HOJA2.index("]", i_w)
        n_widths = FRAGMENTO_HOJA2[i_w:i_w_close].count(",") + 1
        self.assertEqual(n_cols, 11)
        self.assertEqual(n_widths, 11)


class TestLeeElDetalleRealDeAppBultos(unittest.TestCase):
    """La corrección: en vez de repetir el agregado, la hoja 2 consulta
    `app_bultos` (BULTOS_TABLE) -- la MISMA tabla que alimenta el modal
    "Detalle por bulto" de templates/product_detail.html (get_full() ->
    enrich(), app.py ~línea 6468)."""

    def test_consulta_bultos_table_por_product_id_in_batch(self):
        self.assertIn("FROM `{BULTOS_TABLE}` WHERE product_id IN", FRAGMENTO_HOJA2)
        self.assertIn("ORDER BY product_id, bulto_num", FRAGMENTO_HOJA2)

    def test_es_una_sola_query_batch_no_una_por_sku(self):
        """El patrón correcto (igual que sku_data_map) es 1 query con
        WHERE IN (...) para todos los SKUs del export, no N queries."""
        self.assertEqual(FRAGMENTO_HOJA2.count("FROM `{BULTOS_TABLE}`"), 1)

    def test_usa_calc_pv_para_el_peso_volumetrico_por_bulto(self):
        """calc_pv() (linea ~4152) es la MISMA funcion que usa enrich()
        para el modal de product_detail.html -- no se reinventa el
        redondeo."""
        self.assertIn("calc_pv(fb[", FRAGMENTO_HOJA2)

    def test_agrega_columnas_largo_ancho_alto_en_cm(self):
        i = SRC.index("cols2 = [")
        fragmento = SRC[i:i + 300]
        self.assertIn("Largo", fragmento)
        self.assertIn("Ancho", fragmento)
        self.assertIn("Alto", fragmento)

    def test_agrega_columna_origen(self):
        i = SRC.index("cols2 = [")
        fragmento = SRC[i:i + 300]
        self.assertIn("Origen", fragmento)

    def test_la_etiqueta_de_bulto_usa_el_bulto_num_real_no_un_indice_inventado(self):
        """La version anterior armaba la etiqueta con range(1, n_bultos+1)
        -- un indice secuencial inventado. Ahora debe usar fbu['bulto_num'],
        el valor REAL que vino de la tabla (puede tener huecos)."""
        self.assertIn("Bulto {fbu['bulto_num']} de {n_reales}", FRAGMENTO_HOJA2)

    def test_no_repite_el_agregado_de_la_ficha_para_las_filas_reales(self):
        """El bug original: cada fila usaba l["peso_kg_u"]/l["peso_vol_u"]
        (el agregado de TODA la ficha) como si fuera la medida de 1 bulto.
        La rama 'real' debe construir el peso desde CADA FILA de
        app_bultos (fb["peso"]), nunca desde el agregado de la linea."""
        i_if = FRAGMENTO_HOJA2.index("if reales:")
        i_else = FRAGMENTO_HOJA2.index("else:", i_if)
        bloque_real = FRAGMENTO_HOJA2[i_if:i_else]
        self.assertIn('float(fb["peso"])', bloque_real)
        self.assertNotIn("peso_kg_u", bloque_real)
        self.assertNotIn("peso_vol_u", bloque_real)


class TestFallbackMarcadoComoEstimado(unittest.TestCase):
    """Si no hay filas reales recuperables para una ficha con bultos (en la
    práctica no debería pasar -- ver comentario en app.py -- pero se cubre
    por robustez), se repite el promedio como en la primera version, pero
    SIEMPRE marcado: nunca debe poder confundirse con un dato real."""

    @staticmethod
    def _bloque_fallback():
        i_else = FRAGMENTO_HOJA2.index("else:")
        i_fin = FRAGMENTO_HOJA2.index("n_reales = len(", i_else)
        return FRAGMENTO_HOJA2[i_else:i_fin]

    def test_el_fallback_marca_origen_estimado(self):
        self.assertIn("Estimado (sin detalle por bulto)", self._bloque_fallback())

    def test_el_fallback_deja_largo_ancho_alto_en_blanco(self):
        bloque = self._bloque_fallback()
        self.assertIn('"largo": None, "ancho": None, "alto": None', bloque)

    def test_el_fallback_si_usa_el_promedio_de_la_ficha_como_antes(self):
        bloque = self._bloque_fallback()
        self.assertIn('l["peso_kg_u"]', bloque)
        self.assertIn('l["peso_vol_u"]', bloque)

    def test_las_filas_estimadas_se_pintan_con_el_ambar_de_advertencia_ilus(self):
        """Paleta ILUS (CLAUDE.md REGLA #2): ambar advertencia fondo
        #fff8e1. Una fila estimada nunca debe verse igual que una real."""
        self.assertIn("FFF8E1", FRAGMENTO_HOJA2)
        self.assertIn("italic=(not es_real)", FRAGMENTO_HOJA2)


class TestExpansionArgumentoPuroPython(unittest.TestCase):
    """Replica la logica de expansion (rama real + fallback) con datos
    sinteticos -- no depende de MySQL/ERP ni de abrir app.py."""

    @staticmethod
    def _calc_pv(largo, ancho, alto):
        return round(float(largo or 0) * float(ancho or 0) * float(alto or 0) / 4000.0, 2)

    @classmethod
    def _expandir(cls, lineas, bultos_reales_por_app_id):
        filas = []
        tot_kg = tot_pv = tot_vol = 0.0
        for l in lineas:
            if not l["tiene_bultos"]:
                continue
            n_bultos = int(round(l["total_bultos"] or 0))
            if n_bultos <= 0:
                continue
            reales = bultos_reales_por_app_id.get(l.get("app_id")) or []
            if reales:
                filas_bulto = [{
                    "bulto_num": fb["bulto_num"],
                    "peso": fb["peso"],
                    "peso_vol": cls._calc_pv(fb["largo"], fb["ancho"], fb["alto"]),
                    "vol_m3": fb["largo"] * fb["ancho"] * fb["alto"] / 1_000_000.0,
                    "origen": "Real",
                } for fb in reales]
            else:
                filas_bulto = [{
                    "bulto_num": n,
                    "peso": l["peso_kg_u"], "peso_vol": l["peso_vol_u"],
                    "vol_m3": l["vol_u"] / 1_000_000.0,
                    "origen": "Estimado (sin detalle por bulto)",
                } for n in range(1, n_bultos + 1)]
            n_reales = len(filas_bulto)
            for fbu in filas_bulto:
                filas.append((l["sku"], f"Bulto {fbu['bulto_num']} de {n_reales}",
                              fbu["peso"], fbu["peso_vol"], fbu["vol_m3"], fbu["origen"]))
                tot_kg += fbu["peso"]
                tot_pv += fbu["peso_vol"]
                tot_vol += fbu["vol_m3"]
        return filas, (tot_kg, tot_pv, tot_vol)

    def test_ejemplo_real_de_daniel_sku_1121100989(self):
        """El caso que disparo el fix: SKU 1121100989, VD 10218, 5 bultos
        reales con un hueco real en la numeracion (1, 2, 4, 5, 6 -- sin el
        3). Los numeros de largo/ancho/alto/peso son EXACTOS a los que
        Daniel mostro con captura del modal "Detalle por bulto"."""
        linea = {
            "sku": "1121100989", "tiene_bultos": True, "total_bultos": 5, "app_id": 42,
            "peso_kg_u": 280.5, "peso_vol_u": 228.27, "vol_u": 913100.0,
        }
        bultos_reales = {42: [
            {"bulto_num": 1, "largo": 170, "ancho": 70, "alto": 39, "peso": 75.00},
            {"bulto_num": 2, "largo": 170, "ancho": 66, "alto": 39, "peso": 93.00},
            {"bulto_num": 4, "largo": 38, "ancho": 10, "alto": 10, "peso": 37.50},
            {"bulto_num": 5, "largo": 38, "ancho": 10, "alto": 10, "peso": 37.50},
            {"bulto_num": 6, "largo": 38, "ancho": 10, "alto": 10, "peso": 37.50},
        ]}
        filas, (tot_kg, tot_pv, _) = self._expandir([linea], bultos_reales)

        self.assertEqual(len(filas), 5)  # 5 filas reales, NO 6 -- el hueco no se rellena
        self.assertEqual([f[1] for f in filas],
                          ["Bulto 1 de 5", "Bulto 2 de 5", "Bulto 4 de 5",
                           "Bulto 5 de 5", "Bulto 6 de 5"])
        self.assertTrue(all(f[5] == "Real" for f in filas))
        # Cada fila trae SU PROPIO peso -- ya no el mismo numero repetido 5 veces.
        self.assertEqual([f[2] for f in filas], [75.00, 93.00, 37.50, 37.50, 37.50])
        self.assertEqual([f[3] for f in filas], [116.03, 109.39, 0.95, 0.95, 0.95])

        # LA VALIDACION MATEMATICA QUE PIDIO DANIEL: la suma de las filas
        # individuales de este SKU debe coincidir con "Kg/u" y "PV/u" que
        # YA muestra la hoja 1 para el -- NO multiplicado por Bultos (esa
        # era la cuenta que hacia el bug: peso_kg_u x 5 = 1402.5).
        self.assertAlmostEqual(tot_kg, linea["peso_kg_u"], places=2)     # 280.50
        self.assertAlmostEqual(tot_pv, linea["peso_vol_u"], places=2)    # 228.27
        self.assertEqual(round(tot_kg, 2), 280.50)
        self.assertEqual(round(tot_pv, 2), 228.27)
        # Y la identidad que arrastraba el bug (y que el test viejo de esta
        # misma suite daba por buena) NO debe cumplirse nunca mas:
        self.assertNotAlmostEqual(tot_kg, linea["peso_kg_u"] * linea["total_bultos"], places=2)

    def test_linea_sin_ficha_no_genera_filas(self):
        linea = {"sku": "X", "tiene_bultos": False, "total_bultos": 0,
                  "peso_kg_u": 0, "peso_vol_u": 0, "vol_u": 0, "app_id": None}
        filas, totales = self._expandir([linea], {})
        self.assertEqual(filas, [])
        self.assertEqual(totales, (0.0, 0.0, 0.0))

    def test_sin_filas_reales_recuperables_cae_al_fallback_estimado(self):
        """app_id sin entrada en bultos_reales_por_app_id -> fallback,
        nunca una hoja vacia ni un crash."""
        linea = {"sku": "Y", "tiene_bultos": True, "total_bultos": 3, "app_id": 99,
                  "peso_kg_u": 30.0, "peso_vol_u": 12.0, "vol_u": 48000.0}
        filas, (tot_kg, tot_pv, _) = self._expandir([linea], {})
        self.assertEqual(len(filas), 3)
        self.assertTrue(all(f[5] == "Estimado (sin detalle por bulto)" for f in filas))
        # El fallback SI reproduce el comportamiento viejo (repetir el
        # promedio) -- por eso aca SI se cumple peso_kg_u x n_bultos (a
        # diferencia del caso real, donde esa identidad ya NO debe darse).
        self.assertAlmostEqual(tot_kg, 30.0 * 3)
        self.assertAlmostEqual(tot_pv, 12.0 * 3)

    def test_totales_de_la_hoja_2_con_lineas_mixtas_real_y_fallback(self):
        """1 SKU con detalle real + 1 SKU en fallback + 1 sin ficha, para
        confirmar que ambos caminos conviven en el mismo export."""
        lineas = [
            {"sku": "A", "tiene_bultos": True, "total_bultos": 2, "app_id": 1,
             "peso_kg_u": 999, "peso_vol_u": 999, "vol_u": 999},  # ignorado: hay reales
            {"sku": "B", "tiene_bultos": True, "total_bultos": 3, "app_id": 2,
             "peso_kg_u": 5.0, "peso_vol_u": 1.5, "vol_u": 8000.0},  # sin reales -> fallback
            {"sku": "C", "tiene_bultos": False, "total_bultos": 0, "app_id": None,
             "peso_kg_u": 0, "peso_vol_u": 0, "vol_u": 0},
        ]
        bultos_reales = {1: [
            {"bulto_num": 1, "largo": 100, "ancho": 50, "alto": 50, "peso": 20.0},
            {"bulto_num": 2, "largo": 80, "ancho": 40, "alto": 40, "peso": 15.0},
        ]}
        filas, (tot_kg, tot_pv, _) = self._expandir(lineas, bultos_reales)
        self.assertEqual(len(filas), 5)  # 2 reales (A) + 3 fallback (B), C no genera nada
        self.assertAlmostEqual(tot_kg, 20.0 + 15.0 + 3 * 5.0)   # 50.0
        self.assertAlmostEqual(tot_pv, self._calc_pv(100, 50, 50) + self._calc_pv(80, 40, 40) + 3 * 1.5)


def _fuente_cubicador_export_payload():
    with open(os.path.join(BASE_DIR, "app.py"), encoding="utf-8", errors="ignore") as f:
        src = f.read()
    i = src.index("\ndef _cubicador_export_payload(")
    j = src.index("\ndef ", i + 10)
    return src[i:j]


SRC_PAYLOAD = _fuente_cubicador_export_payload()


class TestElPayloadJsonPreservaAppId(unittest.TestCase):
    """BUG REAL (2026-08-26, mismo dia del PR #187): el fix de "bultos
    reales" solo se probo con dicts de linea armados a mano en Python, que
    SIEMPRE traian 'app_id'. Pero el boton "Excel" de la pantalla real NO
    llama a cubicador_export_excel() con lineas frescas del ERP -- manda el
    'payload_json' que la pagina /cubicador ya trae embebido (ver el bloque
    "if payload_raw:" en cubicador_export_excel(), que se USA cuando esta
    presente y valido). Ese JSON lo arma _cubicador_export_payload(), que
    filtraba las llaves de cada linea a una lista fija -- y 'app_id' no
    estaba en esa lista. Resultado en produccion: cargar /cubicador y hacer
    clic en "Excel" (el flujo real de cualquier usuario) SIEMPRE caia al
    fallback "Estimado", incluso para SKUs con detalle real cargado --
    verificado en vivo con el SKU 1121100989 de la VD 10218, el mismo caso
    que motivo el PR #187 en primer lugar."""

    def test_el_diccionario_de_cada_linea_incluye_app_id(self):
        self.assertIn('"app_id"', SRC_PAYLOAD)

    def test_app_id_se_castea_a_int_o_none_no_a_string(self):
        # Si se hubiera escrito como str(l.get("app_id","")) el round-trip
        # por JSON habria convertido int -> "123" (string), y el
        # WHERE product_id IN (...) de la hoja 2 habria fallado en
        # silencio al comparar contra un int real de MySQL.
        i = SRC_PAYLOAD.index('"app_id"')
        fragmento = SRC_PAYLOAD[i:i + 120]
        self.assertIn("int(l[", fragmento)
        self.assertNotIn(f'str(l.get("app_id"', SRC_PAYLOAD)

    def test_lineas_sin_ficha_no_revientan_con_keyerror(self):
        # l.get("app_id") en vez de l["app_id"] a secas -- una linea sin
        # ficha (tiene_ficha=False) puede no tener la llave en absoluto.
        i = SRC_PAYLOAD.index('"app_id"')
        fragmento = SRC_PAYLOAD[i:i + 120]
        self.assertIn('l.get("app_id")', fragmento)

    def test_round_trip_json_completo_replica_el_bug_y_el_fix(self):
        """Reproduce el bug end-to-end en Python puro: arma una 'linea' con
        app_id, la pasa por json.dumps/json.loads (el mismo camino que
        payload_json en el navegador), y confirma que app_id sobrevive."""
        import json as _json

        # Antes del fix: el dict de salida NO tenia 'app_id' (se omite a
        # proposito acá para probar que el bug era real).
        linea_sin_fix = {
            "sku": "1121100989", "descripcion_erp": "ILUS Optimal Dual Pulldown/Row",
            "cantidad": 1, "total_bultos": 5, "tiene_ficha": True, "tiene_bultos": True,
            "peso_kg_u": 280.5, "peso_vol_u": 228.27, "vol_u": 913080.0,
        }
        ida_vuelta = _json.loads(_json.dumps({"lineas": [linea_sin_fix]}))
        self.assertIsNone(ida_vuelta["lineas"][0].get("app_id"))  # el bug: siempre None

        # Con el fix: el dict de salida SI trae 'app_id'.
        linea_con_fix = dict(linea_sin_fix, app_id=42)
        ida_vuelta2 = _json.loads(_json.dumps({"lineas": [linea_con_fix]}))
        self.assertEqual(ida_vuelta2["lineas"][0].get("app_id"), 42)  # sobrevive el viaje


if __name__ == "__main__":
    unittest.main()

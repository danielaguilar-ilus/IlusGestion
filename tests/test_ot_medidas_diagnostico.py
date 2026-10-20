# -*- coding: utf-8 -*-
"""📏 2026-10-20 — Medidas por pieza y diagnóstico guiado «¿Qué le pasa?» en la barra de acciones de la OT.

Pedido de Daniel (dueño): «que se vea inteligente, actuando como un técnico senior… diagnosticar en una desviación… y sobre todo en
las solicitudes hay que grabar datos: si es una piola, cuánto mide; si se rompió la cinta de trotadora, el ancho y el largo…
imagínate que eres un técnico que no sabes nada y te va a llegar una solicitud: necesitas saber medidas y todo».

Decisiones de Daniel que se prueban:
  1. Medidas OBLIGATORIAS solo en piezas clave (piola, cinta, pad, correa, pin, brazo de seguridad), siempre con la salida «No pude
     medir» (que exige foto con la huincha). Las demás son OPCIONALES.
  2. Lo medido en terreno alimenta el catálogo del modelo: con 2 medidas iguales queda «sugerida»; gestión la aprueba (REGLA #32).
  3. NO hay pie de metro: los diámetros se eligen con botones.
  4. Elípticas y remos tienen sistema propio, con piezas, fichas y camino de diagnóstico.

Se prueba (sin BD, sin Flask, sin ERP, sin correos: REGLA #4.1 y #22):
  1. La lógica pura (ot_piezas_medidas.py): fichas, validación (AVISA, no bloquea), resumen, aprendizaje y el árbol.
  2. El servidor (app.py): el lote transporta las medidas por línea, las valida, las guarda, las muestra y no filtra proveedores ni costos.
  3. El teléfono (static/ot_acciones.js y .css): botón «¿Qué le pasa?», tarjeta «Medidas», foto por pieza, semilla después de init().
  4. El espejo JS ↔ Python de la validación (corre las funciones del JS en node).
Correr:  python -m pytest tests/test_ot_medidas_diagnostico.py -q -p no:warnings
"""
import json
import os
import re
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import ot_piezas_medidas as pm
import ot_producto_estado as pe

from tests.test_fac_libro_saldo import _exec
from tests.test_ot_revision_v3 import HAY_NODE, _js_funcion, _node
from tests.test_ot_saldo_servicio import _arbol, _decoradores, _fuente, _leer
from tests.test_repuestos_lote_validacion import _cargar_validador

ACC_JS = _leer("static/ot_acciones.js")
ACC_CSS = _leer("static/ot_acciones.css")
ICONOS_CSS = _leer("static/bootstrap-icons.css")
REP = _leer("templates/clientes_hub/repuestos.html")
DET = _leer("templates/ot2/detalle.html")
TKF = _leer("templates/tickets/ficha.html")
_PLATA = re.compile(r"proveedor|costo|precio|margen|\$\s?\d|valorizado|factura de proveedor|n[°º] de oc", re.I)
ICONOS = set(re.findall(r"\.bi-([a-z0-9-]+)::before", ICONOS_CSS))
FICHAS = pm.FICHAS


def _sin_docstring(src):
    return src.split('"""', 2)[-1] if src.count('"""') >= 2 else src


def _datos(ficha, campos, **kw):
    d = {"ficha": ficha, "campos": campos}
    d.update(kw)
    return pm.normalizar_datos(ficha, d)


A1_OK = {"largo_cm": "235", "diametro_mm": "5", "term_pesas": "bola", "term_agarre": "ojal"}


# ═══════════════════════════════ 1) las fichas ═══════════════════════════════
class TestFichas(unittest.TestCase):
    def test_hay_34_fichas_y_las_claves_son_las_que_dijo_daniel(self):
        self.assertEqual(len(FICHAS), 34)                                     # 30 de la propuesta + rueda, brazo de elíptica, tirón y carro del remo
        self.assertEqual(set(pm.CLAVES), {"A1", "C1", "C2", "D1", "E1", "F1", "G1"})
        self.assertEqual(FICHAS["A1"]["tipo"], "piola")
        self.assertEqual(FICHAS["E1"]["tipo"], "cinta")
        self.assertEqual(FICHAS["D1"]["tipo"], "pad")
        self.assertEqual(FICHAS["F1"]["tipo"], "correa")
        self.assertEqual({FICHAS[i]["tipo"] for i in ("C1", "C2")}, {"pin", "pin_seguro"})
        self.assertEqual(FICHAS["G1"]["tipo"], "brazo_seguridad")
        # polea, rodamiento, motor y consola se IDENTIFICAN con la foto de su etiqueta: nunca obligatorias
        for fid in ("B1", "B2", "E4", "E6"):
            self.assertFalse(FICHAS[fid]["clave"], fid)
            self.assertTrue(FICHAS[fid]["etiqueta"], fid)

    def test_toda_pieza_sugerida_de_todo_sistema_tiene_ficha_propia(self):
        sin = []
        for sistema, piezas in pe.SUGERIDOS_POR_SISTEMA.items():
            for p in piezas:
                if pm.ficha_para(p) == "H" and p != "Bandeja del rack":       # la bandeja es la única que va a «Otra pieza» (propuesta 1.3)
                    sin.append((sistema, p))
        self.assertEqual(sin, [])

    def test_nombre_de_pieza_a_ficha_sin_confusiones(self):
        esperado = {"Piola (cable de acero)": "A1", "Cable de acero (piola)": "A1", "Pin selector": "C1", "Pin de seguro": "C2",
                    "Pin de altura": "C2", "Pin de seguridad": "C2", "Pin o perilla de ajuste": "C3", "Perilla de ajuste": "C3",
                    "Pad de asiento, respaldo o rodillo": "D1", "Rodillo apoya piernas": "D2", "Rodillo (delantero o trasero)": "E3",
                    "Cinta (banda)": "E1", "Tabla (deck)": "E2", "Motor de tracción": "E4", "Motor de inclinación": "E4",
                    "Correa del motor": "F1", "Correa de transmisión": "F1", "Consola": "E6", "Placa controladora": "E6",
                    "Varilla guía o buje de guía": "G7", "Buje o rodamiento de pivote": "B2", "Brazo de seguridad": "G1", "J-hook": "G1",
                    "Rodillo o rueda": "F5", "Brazo o manija": "F6", "Cinta o cuerda de tirón": "R1", "Asiento o carro": "R2",
                    "Rodillos del carro": "F5", "Cuerno porta discos": "G3", "Collarín": "G6", "Nivelador o taco de goma": "D3",
                    "Mosquetón": "A2", "Agarre (manilla)": "A3", "Llave magnética de emergencia": "E5", "Pedales (par)": "F2", "Biela": "F3"}
        for nombre, ficha in esperado.items():
            self.assertEqual(pm.ficha_para(nombre), ficha, nombre)
        self.assertEqual(pm.ficha_para("Una cosa rara"), "H")

    def test_cada_ficha_esta_bien_armada(self):
        for fid, f in FICHAS.items():
            claves = [c["k"] for c in f["campos"]]
            self.assertEqual(len(claves), len(set(claves)), fid)                  # sin campos repetidos
            visibles = [c for c in f["campos"] if not c.get("mas")]
            self.assertLessEqual(len(visibles), 4, f"{fid}: máximo 4 datos a la vista, el resto va en «Más datos»")
            self.assertTrue(f["como"] and f["foto"] and f["sin"], f"{fid}: cómo se mide, qué foto y qué hacer si no se puede")
            for c in f["campos"]:
                self.assertIn(c["y"], ("num", "opcnum", "opc", "sino", "txt"), (fid, c["k"]))
                if c["y"] in ("num", "opcnum"):
                    self.assertTrue(c["u"] and 0 < c["mn"] < c["mx"], (fid, c["k"]))
                if c["y"] in ("opc", "opcnum"):
                    vals = [str(o[0]) for o in c["op"]]
                    self.assertEqual(len(vals), len(set(vals)), (fid, c["k"]))
                cond = c.get("cond")
                if cond:
                    ref = cond.get("vacio") or cond.get("k")
                    self.assertIn(ref, claves, (fid, c["k"], "cond"))
            for alt in f["req_alt"]:
                for k in alt:
                    self.assertIn(k, claves, (fid, "req_alt"))
            for grupo in f["resumen"]:
                for t in grupo:
                    for k in re.findall(r"\{(\w+)\}", t):
                        self.assertIn(k, claves, (fid, "resumen", t))
        # solo las piezas clave llevan campos obligatorios (decisión de Daniel #1)
        for fid, f in FICHAS.items():
            if f["clave"]:
                self.assertTrue(any(c.get("obl") for c in f["campos"]) or f["req_alt"], fid)
            else:
                self.assertFalse(any(c.get("obl") for c in f["campos"]) or f["req_alt"], fid)

    def test_los_diametros_se_eligen_con_botones_sin_pie_de_metro(self):    # decisión de Daniel #3
        d = {c["k"]: c for c in FICHAS["A1"]["campos"]}["diametro_mm"]
        self.assertEqual(d["y"], "opcnum")
        self.assertEqual([o[0] for o in d["op"]], [4, 5, 6])
        self.assertEqual([o[1] for o in FICHAS["A1"]["campos"][1]["op"]], ["4 mm", "5 mm · 3/16″", "6 mm · 1/4″"])
        pin = {c["k"]: c for c in FICHAS["C1"]["campos"]}["diam_mm"]
        self.assertEqual([o[0] for o in pin["op"]], [8, 9.5, 11, 12.7])
        for medido, _t in pm.MEDIDO_CON:
            self.assertNotIn("pie", medido)
        textos = json.dumps(FICHAS, ensure_ascii=False).lower()
        self.assertNotIn("pie de metro", textos)

    def test_los_dibujos_y_los_iconos_de_cada_ficha_existen_en_el_js(self):
        bloque = ACC_JS[ACC_JS.index("var DIB = {"):ACC_JS.index("function dibujo(k)")]
        dibs = set(re.findall(r"^    (\w+): ", bloque, re.M))
        icbloque = ACC_JS[ACC_JS.index("var IC = {"):ACC_JS.index("var RX_IC = [")]
        ics = set(re.findall(r"^    (\w+): ", icbloque, re.M))
        for fid, f in FICHAS.items():
            self.assertIn(f["dib"], dibs, f"{fid}: falta el dibujo «{f['dib']}»")
            self.assertIn(f["ic"], ics, f"{fid}: falta el ícono «{f['ic']}»")

    def test_ninguna_ficha_nombra_proveedor_ni_costo(self):                   # REGLA #19 y #26
        self.assertIsNone(_PLATA.search(json.dumps(FICHAS, ensure_ascii=False)))
        self.assertIsNone(_PLATA.search(json.dumps(pm.para_front("selector_peso", "e"), ensure_ascii=False)))

    def test_el_sistema_de_elipticas_y_remos(self):                           # decisión de Daniel #4
        for s in ("eliptica", "remo"):
            self.assertIn(s, pe.SUGERIDOS_POR_SISTEMA)
            self.assertGreaterEqual(len(pe.SUGERIDOS_POR_SISTEMA[s]), 4)
        self.assertEqual(pe.sistema_por_nombre("Eliptica ILUS E1"), "eliptica")
        self.assertEqual(pe.sistema_por_nombre("Remadora Concept2 RowErg"), "remo")
        self.assertEqual(pe.sistema_por_nombre("Remo sentado selector 300"), "selector_peso")   # una máquina de fuerza NO es un remo
        self.assertEqual(pe.refinar_sistema("bicicleta", "Elíptica ILUS PRO"), "eliptica")      # comparte el bloque «Bicicleta»: se afina
        self.assertEqual(pe.refinar_sistema("bicicleta", "Bicicleta Spinning"), "bicicleta")
        self.assertEqual(pe.refinar_sistema("trotadora", "Eliptica"), "trotadora")
        self.assertEqual(pe.sistema_de_clase("eliptica"), "eliptica")                           # listo para cuando exista la clase
        self.assertIsNone(pe.sistema_de_clase("trotadora"))
        self.assertTrue({"Rodillo o rueda", "Brazo o manija", "Pedales (par)", "Correa de transmisión", "Consola"} <= set(pe.SUGERIDOS_POR_SISTEMA["eliptica"]))
        self.assertTrue({"Cinta o cuerda de tirón", "Asiento o carro", "Rodillos del carro", "Consola"} <= set(pe.SUGERIDOS_POR_SISTEMA["remo"]))
        for s in ("eliptica", "remo"):
            self.assertTrue(pm.arbol("e", s)["nodos"], s)                                       # y su camino de diagnóstico


# ═══════════════════════════════ 2) la validación: avisa, no bloquea ═══════════════════════════════
class TestValidacion(unittest.TestCase):
    def test_piola_completa_y_su_resumen(self):
        limpio, err = _datos("A1", A1_OK)
        self.assertIsNone(err)
        self.assertEqual(limpio["campos"]["largo_cm"], 235)
        self.assertEqual(limpio["medido_con"], "huincha")
        self.assertEqual(limpio["resumen"], "Piola Ø5 mm × 235 cm · Bola / Ojal")
        self.assertEqual(pm.piola_metros(limpio), 2.35)                       # compatibilidad con piola_metros
        self.assertEqual(pm.filas(limpio)[0], ("Medida", "Medida con huincha en terreno"))
        self.assertIn(("Largo total, punta a punta", "235 cm (2,35 m)"), pm.filas(limpio))
        self.assertLessEqual(len(pm.resumen_corto(limpio)), 80)               # cabe en mant_ot_repuesto_solicitudes.medida

    def test_acepta_coma_punto_y_miles_con_punto(self):
        for texto, esperado in (("2,35", 2.35), ("2.35", 2.35), ("2.900", 2900.0), ("2900", 2900.0), ("235 cm", 235.0), ("1.234,5", 1234.5), (" 520 ", 520.0)):
            self.assertEqual(pm.parse_num(texto), esperado, texto)
        for malo in ("", None, "abc", "1,2,3", True):
            self.assertIsNone(pm.parse_num(malo), malo)
        limpio, err = _datos("E1", {"ancho_mm": "520", "vuelta_mm": "2.900"})
        self.assertIsNone(err)
        self.assertEqual((limpio["campos"]["ancho_mm"], limpio["campos"]["vuelta_mm"]), (520, 2900))
        self.assertEqual(limpio["resumen"], "Cinta 520 × 2.900 mm (vuelta completa)")

    def test_fuera_de_lo_normal_AVISA_pero_no_bloquea(self):
        limpio, err = _datos("A1", dict(A1_OK, largo_cm="12"))
        self.assertIsNone(err)                                                # se guarda igual
        self.assertEqual(len(limpio["avisos"]), 1)
        self.assertIn("muy bajo", limpio["avisos"][0])
        self.assertIn("40–800 cm", limpio["avisos"][0])
        limpio, err = _datos("A1", dict(A1_OK, largo_cm="900"))
        self.assertIsNone(err)
        self.assertIn("muy alto", limpio["avisos"][0])
        # si la persona lo confirma («Sí, está bien así»), ya no avisa
        limpio, err = _datos("A1", dict(A1_OK, largo_cm="12"), confirmados={"largo_cm": 1})
        self.assertEqual(limpio["avisos"], [])
        limpio, _e = _datos("A1", A1_OK)
        self.assertEqual(limpio["avisos"], [])

    def test_solo_BLOQUEA_lo_imposible(self):
        for valor in ("0", "-5", "abc", "99999"):                              # cero, negativo, no es número, más de 5 veces el máximo
            limpio, err = _datos("A1", dict(A1_OK, largo_cm=valor))
            self.assertIsNone(limpio, valor)
            self.assertEqual(err[1], "MEDIDA_INVALIDA", valor)
        self.assertEqual(pm.FACTOR_IMPOSIBLE, 5)
        limpio, err = _datos("A1", dict(A1_OK, largo_cm="4000"))               # justo en 5 × 800: todavía solo un aviso
        self.assertIsNone(err)
        limpio, err = _datos("A1", dict(A1_OK, diametro_mm="otro"))            # «Otro» sin número escrito: no bloquea, falta el dato
        self.assertEqual(err[1], "MEDIDA_REQUERIDA")
        limpio, err = _datos("A1", dict(A1_OK, term_pesas="inventado"))
        self.assertEqual(err[1], "MEDIDA_INVALIDA")                            # una opción que no existe
        self.assertEqual(pm.normalizar_datos("ZZ", {})[1][1], "FICHA_INVALIDA")

    def test_pieza_clave_sin_medidas_exige_las_medidas_o_la_salida_no_pude(self):
        for fid in pm.CLAVES:
            limpio, err = pm.normalizar_datos(fid, {"ficha": fid, "campos": {}})
            self.assertIsNone(limpio, fid)
            self.assertEqual(err[1], "MEDIDA_REQUERIDA", fid)
            self.assertIn("No pude medir", err[0], fid)                        # el mensaje dice cuál es la salida
        # la salida: «No pude medir» + un motivo de la lista (la foto con huincha se exige en el teléfono y en la evidencia del pedido)
        limpio, err = pm.normalizar_datos("A1", {"ficha": "A1", "medido_con": "no_pude", "motivo_no": "no_hay_huincha", "campos": {}})
        self.assertIsNone(err)
        self.assertEqual(limpio["medido_con"], "no_pude")
        self.assertIn("sin medida en terreno", limpio["resumen"])
        self.assertIn("no tengo huincha", limpio["resumen"])
        self.assertEqual(pm.normalizar_datos("A1", {"ficha": "A1", "medido_con": "no_pude", "campos": {}})[1][1], "MOTIVO_NO_PUDE_REQUERIDO")
        self.assertEqual(pm.normalizar_datos("A1", {"ficha": "A1", "medido_con": "no_pude", "motivo_no": "porque_si", "campos": {}})[1][1],
                         "MOTIVO_NO_PUDE_REQUERIDO")
        self.assertEqual(pm.filas(limpio)[0], ("Medida", "No se pudo medir: No tengo huincha"))
        # la otra salida: la medida del catálogo del modelo, marcada «no medida en terreno»
        limpio, err = pm.normalizar_datos("A1", {"ficha": "A1", "medido_con": "catalogo", "campos": {"largo_cm": 235, "diametro_mm": 5}})
        self.assertIsNone(err)
        self.assertIn("del catálogo, no medida en terreno", limpio["resumen"])

    def test_las_piezas_opcionales_nunca_obligan(self):
        for fid, f in FICHAS.items():
            if f["clave"]:
                continue
            limpio, err = pm.normalizar_datos(fid, {"ficha": fid, "campos": {}})
            self.assertIsNone(err, fid)                                        # vacía vale: nadie obliga a medir una polea
            self.assertEqual(limpio["campos"], {}, fid)

    def test_correa_el_codigo_del_lomo_o_canales_y_largo(self):
        self.assertIsNone(_datos("F1", {"tipo": "canales", "codigo": "6PJ508"})[1])
        self.assertIsNone(_datos("F1", {"tipo": "canales", "canales": "6", "largo_mm": "508"})[1])
        self.assertEqual(_datos("F1", {"tipo": "canales", "canales": "6"})[1][1], "MEDIDA_REQUERIDA")          # falta el largo
        self.assertEqual(_datos("F1", {"codigo": "6PJ508"})[1][1], "MEDIDA_REQUERIDA")                           # falta el tipo
        limpio, _e = _datos("F1", {"tipo": "canales", "codigo": "6PJ508", "canales": "6", "largo_mm": "508"})
        self.assertNotIn("canales", limpio["campos"])                           # con código no se piden (campos ocultos)
        self.assertEqual(pm.decodificar_correa("6PJ508"), {"canales": 6, "perfil": "PJ", "largo_mm": 508})
        self.assertIsNone(pm.decodificar_correa("cualquier cosa"))

    def test_campos_condicionales_ocultos_no_se_guardan(self):
        limpio, _e = _datos("A1", dict(A1_OK, rosca="M8", rosca_largo_mm="40"))
        self.assertNotIn("rosca", limpio["campos"])                              # no hay espárrago: la rosca no aplica
        limpio, _e = _datos("A1", dict(A1_OK, term_pesas="esparrago", rosca="M8", rosca_largo_mm="40"))
        self.assertEqual((limpio["campos"]["rosca"], limpio["campos"]["rosca_largo_mm"]), ("M8", 40))
        self.assertIn("Espárrago roscado", [t for _k, t in pm.filas(limpio) if _k == "Terminal del lado de las pesas"][0])

    def test_otro_con_texto_y_si_no(self):
        limpio, _e = _datos("B2", {"cual": "rodamiento", "codigo": "otro", "codigo_otro": "6807 2RS"})
        self.assertEqual(limpio["campos"]["codigo_otro"], "6807 2RS")
        limpio, _e = _datos("A1", dict(A1_OK, funda="si"))
        self.assertIs(limpio["campos"]["funda"], True)
        limpio, _e = _datos("A1", dict(A1_OK, funda=False))
        self.assertIs(limpio["campos"]["funda"], False)
        limpio, _e = _datos("A1", dict(A1_OK, desconocido="x"))
        self.assertNotIn("desconocido", limpio["campos"])                        # lo que no es de la ficha se descarta

    def test_cruces_inteligentes_sugieren_y_la_persona_decide(self):
        limpio, _e = _datos("B2", {"cual": "rodamiento", "int_mm": "17", "ext_mm": "40", "ancho_mm": "12"})
        self.assertIn("6203", " ".join(limpio["avisos"]))
        self.assertEqual(pm.rodamiento_sugerido(17, 40, 12), "6203")
        self.assertEqual(pm.rodamiento_sugerido(20, 47, 14), "6204")
        self.assertIsNone(pm.rodamiento_sugerido(18, 41, 13))
        limpio, _e = _datos("E1", {"ancho_mm": "520", "vuelta_mm": "1400", "pisar_mm": "1300"})
        self.assertTrue(any("superficie" in a for a in limpio["avisos"]))        # midió la superficie, no la vuelta
        limpio, _e = _datos("E1", {"ancho_mm": "520", "vuelta_mm": "2900", "pisar_mm": "1300"})
        self.assertFalse([a for a in limpio["avisos"] if "superficie" in a])
        limpio, _e = _datos("F1", {"tipo": "canales", "codigo": "6PJ508", "canales": "8", "largo_mm": "508"})
        self.assertIsNotNone(limpio)

    def test_foto_por_pieza_solo_con_dos_o_mas_piezas_clave(self):
        self.assertFalse(pm.debe_tener_foto_propia(0))
        self.assertFalse(pm.debe_tener_foto_propia(1))
        self.assertTrue(pm.debe_tener_foto_propia(2))


# ═══════════════════════════════ 3) el modelo aprende de lo medido ═══════════════════════════════
class TestAprendizaje(unittest.TestCase):
    CAMPOS = {"largo_cm": 235, "diametro_mm": 5, "term_pesas": "bola", "term_agarre": "ojal"}

    def test_dos_medidas_iguales_de_pedidos_distintos_quedan_sugeridas(self):
        r1 = pm.aprender([], "A1", self.CAMPOS, "lote-a")
        self.assertEqual((r1["accion"], r1["n"], r1["estado"]), ("nueva", 1, "observada"))
        cand = [{"id": 1, "datos": pm.campos_de_aprendizaje("A1", self.CAMPOS), "n_pedidos": 1, "estado": "observada", "lotes": ["lote-a"]}]
        r2 = pm.aprender(cand, "A1", dict(self.CAMPOS, largo_cm=236), "lote-b")        # la huincha no mide al milímetro: 235 ≈ 236
        self.assertEqual((r2["accion"], r2["id"], r2["n"], r2["estado"]), ("sumar", 1, 2, "sugerida"))
        self.assertEqual(r2["lotes"], ["lote-a", "lote-b"])
        self.assertEqual(pm.UMBRAL_SUGERIDA, 2)

    def test_el_mismo_pedido_no_cuenta_dos_veces(self):
        cand = [{"id": 1, "datos": pm.campos_de_aprendizaje("A1", self.CAMPOS), "n_pedidos": 1, "estado": "observada", "lotes": ["lote-a"]}]
        r = pm.aprender(cand, "A1", self.CAMPOS, "lote-a")
        self.assertEqual((r["accion"], r["n"]), ("igual", 1))

    def test_una_medida_distinta_es_otra_medida(self):
        cand = [{"id": 1, "datos": pm.campos_de_aprendizaje("A1", self.CAMPOS), "n_pedidos": 1, "estado": "observada", "lotes": ["a"]}]
        for cambio in ({"largo_cm": 260}, {"diametro_mm": 6}, {"term_agarre": "gancho"}):
            r = pm.aprender(cand, "A1", dict(self.CAMPOS, **cambio), "b")
            self.assertEqual(r["accion"], "nueva", cambio)
        self.assertFalse(pm.misma_medida("A1", {"largo_cm": 235}, {"diametro_mm": 5}))        # nada en común: no se puede decir que es igual

    def test_lo_que_gestion_decidio_no_se_deshace(self):
        datos = pm.campos_de_aprendizaje("A1", self.CAMPOS)
        for estado in ("aprobada", "descartada"):
            cand = [{"id": 7, "datos": datos, "n_pedidos": 3, "estado": estado, "lotes": ["x"]}]
            r = pm.aprender(cand, "A1", self.CAMPOS, "nuevo")
            self.assertEqual((r["accion"], r["estado"], r["n"]), ("sumar", estado, 4), estado)

    def test_solo_aprenden_los_datos_que_identifican_la_pieza(self):
        c = pm.campos_de_aprendizaje("A1", dict(self.CAMPOS, funda=True, cod_pieza="X-1", pieza_vieja="guardada"))
        self.assertEqual(set(c), {"largo_cm", "diametro_mm", "term_pesas", "term_agarre"})
        self.assertEqual(pm.campos_de_aprendizaje("H", {"largo_mm": 100}), {})                  # «otra pieza» no aprende
        self.assertEqual(pm.aprender([], "H", {"largo_mm": 100}, "x")["accion"], "nada")


# ═══════════════════════════════ 4) el árbol «¿Qué le pasa?» ═══════════════════════════════
SISTEMAS = list(pe.SUGERIDOS_POR_SISTEMA) + [None]


class TestArbol(unittest.TestCase):
    def test_una_pregunta_por_pantalla_maximo_tres_y_otra_cosa_en_cada_una(self):
        for kind in ("e", "i"):
            for s in SISTEMAS:
                a = pm.arbol(kind, s)
                self.assertLessEqual(pm.preguntas_maximas(a), pm.MAX_PREGUNTAS, (kind, s))
                for nid, n in a["nodos"].items():
                    self.assertTrue(n["q"].endswith("?"), (kind, s, nid))
                    self.assertIn("Otra cosa", [o["t"] for o in n["ops"]], (kind, s, nid))      # «Otra cosa» en CADA pregunta
                    for o in n["ops"]:
                        self.assertTrue(bool(o.get("ir")) ^ bool(o.get("res")), (kind, s, nid, o["t"]))      # o sigue o termina
                        if o.get("ir"):
                            self.assertIn(o["ir"], a["nodos"], (kind, s, o["t"]))
                self.assertEqual(a["prof"]["q0"], pm.preguntas_maximas(a))

    def test_todo_camino_termina_en_una_sugerencia_valida(self):
        for kind in ("e", "i"):
            for s in SISTEMAS:
                a = pm.arbol(kind, s)
                permitidas = pm.ACCIONES_POR_TIPO[kind]
                piezas_ok = set(pe.SUGERIDOS_POR_SISTEMA.get(s or "", ())) | {"Otra pieza"}
                caminos = pm.recorrer(a)
                self.assertGreater(len(caminos), 5, (kind, s))
                for camino, res in caminos:
                    ctx = (kind, s, [r for _q, r in camino])
                    self.assertIn(res["accion"], permitidas, ctx)
                    self.assertLessEqual(len(camino), pm.MAX_PREGUNTAS, ctx)
                    self.assertTrue(res.get("por_que") or res["accion"] == "esperar", ctx)      # siempre dice POR QUÉ
                    for p in res.get("piezas") or []:
                        self.assertIn(p, piezas_ok, ctx)                                       # solo piezas del sistema: con su ficha
                        self.assertIn(pm.ficha_para(p), FICHAS, ctx)
                    if res["accion"] == "pedir":
                        self.assertTrue(res.get("piezas"), ctx)
                    for ad in res.get("ademas") or []:
                        self.assertIn(ad["accion"], permitidas, ctx)

    def test_motivos_y_opciones_de_la_sugerencia_son_los_del_flujo_de_siempre(self):
        fs = {k for k, _t in pe.motivos_de(pe.ESTADO_FUERA_SERVICIO)}
        baja = {k for k, _t in pe.motivos_de(pe.ESTADO_BAJA)}
        noesta = {"no_esta": {k for k, _t in pe.motivos_equipo("no_esta")}, "falta_stock": {k for k, _t in pe.motivos_equipo("falta_stock")},
                  "no_revisado": {k for k, _t in pe.motivos_equipo("no_revisado")}}
        fallas = set(re.findall(r"\['(\w+)', '[^']*'\]", ACC_JS[ACC_JS.index("var FALLAS = ["):ACC_JS.index("function falla_txt")]))
        que_nl = set(re.findall(r"\['(\w+)', '[^']*'\]", ACC_JS[ACC_JS.index("var QUE_NL = ["):ACC_JS.index("var nollego = {")]))
        self.assertTrue({"se_corto", "ruido", "se_perdio"} <= fallas and {"no_venia", "otro_producto"} == que_nl)
        for kind in ("e", "i"):
            for s in SISTEMAS:
                a = pm.arbol(kind, s)
                todos = [res for _c, res in pm.recorrer(a)]
                todos += [ad for res in todos for ad in (res.get("ademas") or [])]
                for res in todos:
                    ctx = (kind, s, res)
                    if res.get("falla"):
                        self.assertIn(res["falla"], fallas, ctx)
                    if res["accion"] == "fs" and res.get("motivo"):
                        self.assertIn(res["motivo"], fs, ctx)
                    if res["accion"] == "baja" and res.get("motivo"):
                        self.assertIn(res["motivo"], baja, ctx)
                    if res["accion"] == "noesta":
                        self.assertIn(res["motivo"], noesta[res["opcion"]], ctx)
                    if res["accion"] == "nollego":
                        self.assertIn(res["que"], que_nl, ctx)
                    if res.get("riesgo"):
                        self.assertIn(res["riesgo"], ("si", "no"), ctx)
        self.assertIn(pm.ATAJO_PELIGRO["motivo"], fs)
        self.assertEqual(pm.ATAJO_PELIGRO["accion"], "fs")

    def test_los_botones_de_esperar_apuntan_a_preguntas_que_existen(self):
        for s in SISTEMAS:
            a = pm.arbol("e", s)
            for _c, res in pm.recorrer(a):
                for b in res.get("botones") or []:
                    self.assertTrue(b.get("cerrar") or b["ir"] in a["nodos"], (s, b))

    def test_los_iconos_del_arbol_existen_en_bootstrap_icons_1_11_3(self):
        self.assertIn("Bootstrap Icons v1.11.3", ICONOS_CSS[:200])
        usados = set()
        for kind in ("e", "i"):
            for s in SISTEMAS:
                for n in pm.arbol(kind, s)["nodos"].values():
                    usados |= {o["ic"] for o in n["ops"]}
        faltan = sorted(i for i in usados if i not in ICONOS)
        self.assertEqual(faltan, [], "íconos que la versión de Bootstrap Icons de la app NO trae")
        self.assertNotIn("stethoscope", ICONOS)                                                # el de la propuesta no existía: se usa otro
        self.assertIn("clipboard2-pulse", ICONOS)

    def test_los_ejemplos_de_daniel_en_pocos_toques(self):
        # «le falta un buje»: Le falta una pieza → Buje o rodamiento de pivote → Sí, funciona
        a = pm.arbol("e", "selector_peso")
        op = {o["t"]: o for o in a["nodos"]["q0"]["ops"]}["Le falta una pieza"]
        n4 = a["nodos"][op["ir"]]
        buje = {o["t"]: o for o in n4["ops"]}["Buje o rodamiento de pivote"]
        sigue = {o["t"]: o for o in a["nodos"][buje["ir"]]["ops"]}["Sí, funciona"]["res"]
        self.assertEqual((sigue["accion"], sigue["piezas"]), ("pedir", ["Buje o rodamiento de pivote"]))
        self.assertEqual(pm.ficha_para(sigue["piezas"][0]), "B2")                              # abre la ficha de bujes y rodamientos
        # «se cortó la piola y puede lastimar»: Está roto → Piola → Puede lastimar
        op = {o["t"]: o for o in a["nodos"]["q0"]["ops"]}["Está roto, fisurado o gastado"]
        piola = {o["t"]: o for o in a["nodos"][op["ir"]]["ops"]}["Piola (cable de acero)"]
        riesgo = {o["t"]: o for o in a["nodos"][piola["ir"]]["ops"]}["Puede lastimar a alguien"]["res"]
        self.assertEqual((riesgo["accion"], riesgo["piezas"], riesgo["falla"]), ("pedir", ["Piola (cable de acero)"], "se_corto"))
        self.assertEqual(riesgo["ademas"][0]["accion"], "fs")                                  # + sugerencia «No se puede usar» con riesgo
        self.assertEqual(riesgo["ademas"][0]["riesgo"], "si")
        self.assertEqual(pm.ficha_para("Piola (cable de acero)"), "A1")                        # y la ficha pide largo, Ø, terminales y la foto
        # «no está la máquina»: No está → ya pregunté → lo movieron
        e = pm.arbol("e", "selector_peso")
        no_esta = {o["t"]: o for o in e["nodos"]["q0"]["ops"]}["No está el equipo"]
        ya = {o["t"]: o for o in e["nodos"][no_esta["ir"]]["ops"]}["Sí, ya pregunté"]
        movido = {o["t"]: o for o in e["nodos"][ya["ir"]]["ops"]}["Lo movieron o lo guardaron"]["res"]
        self.assertEqual((movido["accion"], movido["opcion"], movido["motivo"]), ("noesta", "no_esta", "cliente_lo_movio"))

    def test_nada_se_crea_solo_y_se_respetan_las_piezas_de_seguridad(self):
        a = pm.arbol("e", "rack")
        n4 = a["nodos"][{o["t"]: o for o in a["nodos"]["q0"]["ops"]}["Le falta una pieza"]["ir"]]
        brazo = a["nodos"][{o["t"]: o for o in n4["ops"]}["Brazo de seguridad"]["ir"]]
        sigue = {o["t"]: o for o in brazo["ops"]}["Sí, funciona"]["res"]
        self.assertIn("seguridad", sigue["aviso"])                                           # aviso ámbar si dicen «se puede usar»
        self.assertNotIn("aviso", {o["t"]: o for o in brazo["ops"]}["No se puede usar"]["res"])
        self.assertIn("Brazo de seguridad", pm.PIEZAS_SEGURIDAD)

    def test_producto_de_revision_y_equipo_de_cliente_se_diferencian(self):
        e = [o["t"] for o in pm.arbol("e", "trotadora")["nodos"]["q0"]["ops"]]
        i = [o["t"] for o in pm.arbol("i", "trotadora")["nodos"]["q0"]["ops"]]
        self.assertIn("No está el equipo", e)
        self.assertIn("No lo pude revisar: no hay acceso", e)
        self.assertNotIn("Llegó incompleto o distinto", e)
        self.assertIn("Llegó incompleto o distinto", i)
        self.assertNotIn("No está el equipo", i)
        for _c, res in pm.recorrer(pm.arbol("i", "trotadora")):
            self.assertNotEqual(res["accion"], "noesta")
        for _c, res in pm.recorrer(pm.arbol("e", "trotadora")):
            self.assertNotEqual(res["accion"], "nollego")
        # «Llegó incompleto» NUNCA es «No llegó» cuando falta solo una pieza: el producto sí llegó (hueco 1 de la propuesta)
        a = pm.arbol("i", "selector_peso")
        n6 = {o["t"]: o for o in a["nodos"]["n6"]["ops"]}
        inc = a["nodos"][n6["Llegó, pero le falta una pieza o accesorio"]["ir"]]
        self.assertEqual(n6["Faltan unidades"]["res"]["accion"], "nollego")
        for o in inc["ops"]:
            if o["t"] not in ("Otra cosa", "Manual o tornillería"):
                self.assertEqual(o["res"]["accion"], "pedir", o["t"])

    def test_sin_electronica_no_hay_pregunta_de_no_enciende(self):
        for s in ("selector_peso", "carga_discos", "poleas", "banco", "rack", "cargas_libres"):
            self.assertNotIn("No funciona o no enciende", [o["t"] for o in pm.arbol("e", s)["nodos"]["q0"]["ops"]], s)
        for s in ("trotadora", "bicicleta", "eliptica", "remo"):
            self.assertIn("No funciona o no enciende", [o["t"] for o in pm.arbol("e", s)["nodos"]["q0"]["ops"]], s)

    def test_para_front_trae_solo_lo_necesario(self):
        f = pm.para_front("selector_peso", "e", fichas_extra={"E1", "ZZ"})
        self.assertEqual(f["sistema"], "selector_peso")
        self.assertTrue({"A1", "C1", "B1", "B2", "D1", "H", "E1"} <= set(f["fichas"]))
        self.assertNotIn("ZZ", f["fichas"])
        self.assertLess(len(f["fichas"]), len(FICHAS))
        self.assertEqual(f["ficha_de"]["Piola (cable de acero)"], "A1")
        self.assertIn("arbol", f)
        self.assertNotIn('"tol"', json.dumps(f["fichas"]))                                    # los datos del aprendizaje no viajan al teléfono
        self.assertNotIn('"ap"', json.dumps(f["fichas"]))
        self.assertNotIn('"resumen"', json.dumps(f["fichas"]))

    def test_camino_recorrido_se_limpia_antes_de_guardarlo(self):
        d = pm.normalizar_diagnostico({"camino": [{"q": "¿Qué le pasa?", "r": "Hace ruido"}] * 9, "accion": "pedir", "piezas": ["Piola", 5, ""],
                                       "sistema": "selector_peso", "siguio": 1, "atajo": 0})
        self.assertEqual(len(d["camino"]), 6)
        self.assertEqual((d["accion"], d["piezas"], d["siguio"], d["atajo"]), ("pedir", ["Piola", "5"], True, False))
        self.assertEqual(pm.diagnostico_txt({"camino": [{"q": "a", "r": "Hace ruido"}, {"q": "b", "r": "Poleas"}]}), "Hace ruido → Poleas")
        self.assertIsNone(pm.normalizar_diagnostico({"camino": []}))
        self.assertIsNone(pm.normalizar_diagnostico("texto"))
        self.assertEqual(pm.normalizar_diagnostico({"camino": [{"q": "x", "r": "y"}], "accion": "hackear"})["accion"], "")


# ═══════════════════════════════ 5) el servidor ═══════════════════════════════
class TestServidor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.validar = staticmethod(_cargar_validador())
        cls.aplicar = staticmethod(_exec("_otrep_med_aplicar", {"_pm": pm}))

    def test_el_lote_transporta_las_medidas_por_linea_y_la_ref(self):         # el hueco que encontró el técnico senior
        lineas, err = self.validar([{"origen": "manual", "repuesto_nombre": "Piola (cable de acero)", "cantidad": 1, "ref": "ab12-CD_3!!",
                                     "ficha": "A1", "datos": {"ficha": "A1", "campos": A1_OK}}], {})
        self.assertIsNone(err)
        self.assertEqual(lineas[0]["ref"], "ab12-CD_3")                       # solo letras, números, guion
        self.assertEqual(lineas[0]["ficha"], "A1")
        self.assertEqual(lineas[0]["datos_raw"]["campos"]["largo_cm"], "235")
        lineas, _e = self.validar([{"origen": "manual", "repuesto_nombre": "Perilla", "cantidad": 1}], {})
        self.assertEqual((lineas[0]["datos_raw"], lineas[0]["ficha"], lineas[0]["ref"]), (None, None, None))

    def test_dos_piolas_de_medidas_distintas_son_dos_lineas_aunque_sean_el_mismo_repuesto(self):
        stock = {7: {"id": 7, "sku": "PIO-5", "descripcion": "Piola 5 mm", "cantidad": 10, "proveedor_id": 3}}
        mk = lambda cm, ref: {"origen": "bodega", "repuesto_stock_id": 7, "cantidad": 1, "ref": ref, "ficha": "A1",
                              "datos": {"ficha": "A1", "campos": dict(A1_OK, largo_cm=cm)}}
        lineas, err = self.validar([mk("235", "a"), mk("310", "b")], stock)
        self.assertIsNone(err)
        self.assertEqual(len(lineas), 2)
        self.assertEqual([l["ref"] for l in lineas], ["a", "b"])
        # sin medidas, el repuesto repetido se sigue sumando como siempre
        lineas, _e = self.validar([{"origen": "bodega", "repuesto_stock_id": 7, "cantidad": 2}, {"origen": "bodega", "repuesto_stock_id": 7, "cantidad": 3}], stock)
        self.assertEqual((len(lineas), lineas[0]["cantidad"]), (1, 5))

    def test_aplicar_valida_cada_linea_y_deja_medida_y_piola_metros(self):
        lineas, _e = self.validar([{"origen": "manual", "repuesto_nombre": "Piola (cable de acero)", "cantidad": 1, "ficha": "A1",
                                    "datos": {"ficha": "A1", "campos": dict(A1_OK, largo_cm="12")}},
                                   {"origen": "manual", "repuesto_nombre": "Perilla", "cantidad": 1}], {})
        self.assertIsNone(self.aplicar(lineas))
        self.assertEqual(lineas[0]["medida"], "Piola Ø5 mm × 12 cm · Bola / Ojal")
        self.assertEqual(lineas[0]["piola_metros"], 0.12)
        self.assertEqual(len(lineas[0]["avisos_med"]), 1)                       # un largo raro AVISA, el pedido sale igual
        self.assertEqual(lineas[0]["datos"]["campos"]["largo_cm"], 12)
        self.assertIsNone(lineas[1]["datos"])                                   # una pieza sin medidas no guarda ruido

    def test_aplicar_rechaza_lo_imposible_y_la_pieza_clave_sin_medidas(self):
        malo = lambda campos, **kw: self.validar([dict({"origen": "manual", "repuesto_nombre": "Cinta (banda)", "cantidad": 1, "ficha": "E1",
                                                          "datos": dict({"ficha": "E1", "campos": campos}, **kw)})], {})[0]
        err = self.aplicar(malo({"ancho_mm": "-3", "vuelta_mm": "2900"}))
        self.assertEqual(err[1], "MEDIDA_INVALIDA")
        err = self.aplicar(malo({}))
        self.assertEqual(err[1], "MEDIDA_REQUERIDA")
        self.assertIn("Cinta (banda)", err[0])
        self.assertIsNone(self.aplicar(malo({}, medido_con="no_pude", motivo_no="falta_trozo")))   # la salida «No pude medir» siempre existe
        err = self.aplicar([{"nombre": "X", "ficha": "NOEXISTE", "datos_raw": None}])
        self.assertEqual(err[1], "FICHA_INVALIDA")

    def test_la_columna_y_la_tabla_son_baratas_y_de_una_clausula_REGLA_18(self):
        f = _fuente("_ensure_otrep_medidas")
        self.assertIn("ADD COLUMN datos_json TEXT NULL", f)
        self.assertIn("ADD COLUMN diagnostico_json TEXT NULL", f)
        self.assertIn("information_schema.COLUMNS", f)                          # una consulta y listo si ya existen
        self.assertIn("CREATE TABLE IF NOT EXISTS cat_medidas_terreno", f)       # la guarda de mysql_execute la salta si ya existe
        self.assertNotRegex(f, r"ADD COLUMN[^\"]*,\s*ADD")                       # una sentencia = una cláusula
        self.assertNotIn("MODIFY", f)
        self.assertNotIn("mant_tarea_plantilla", f)                              # REGLA #32: nada de plantillas
        self.assertIn("estado VARCHAR(12)", f)                                   # sin ENUM: nunca hay que reescribir la tabla
        codigo = _arbol()[0]
        i = codigo.index("        _ensure_ot_producto_estado()\n        _ensure_otrep_medidas()")
        self.assertLess(i, codigo.index("except Exception as _ensure_etapa3_err"))
        self.assertIn("_otrep_med_ok()", _fuente("_otrep_med_guardar"))          # sin la columna, el pedido igual se guarda

    def test_las_dos_rutas_validan_guardan_y_responden_con_las_medidas(self):
        for ruta in ("ot2_api_equipo_solicitar_repuestos", "ot2_api_producto_solicitar_repuestos"):
            r = _fuente(ruta)
            self.assertIn("_otrep_med_aplicar(lineas_ok)", r, ruta)
            self.assertIn("_otrep_diag_de_form(fd)", r, ruta)
            self.assertLess(r.index("_otrep_med_aplicar(lineas_ok)"), r.index("_otrep_crear_fila_solicitud("), ruta)       # se rechaza ANTES de escribir
            self.assertIn('medida=li.get("medida"), piola_metros=li.get("piola_metros")', r, ruta)
            self.assertIn("_otrep_med_guardar(cur, sol_ids, lineas_ok, diag)", r, ruta)
            self.assertLess(r.index("_otrep_med_guardar(cur, sol_ids, lineas_ok, diag)"), r.index("conn.commit()"), ruta)    # misma transacción
            self.assertIn('"ref": li.get("ref"), "medida": li.get("medida")', r, ruta)                                   # cada foto vuelve a su pieza
            self.assertIn("avisos.extend(_otrep_med_avisos(lineas_ok))", r, ruta)
            self.assertIn("_med_aprender_lote(", r, ruta)
            self.assertIn("diag=diag", r, ruta)                                  # el ticket lleva medidas y camino
        self.assertIn("_otrep_med_txt(li)", _fuente("_otrep_ticket_anotar_lote"))
        self.assertIn("_otrep_med_txt(li)", _fuente("_otrep_ticket_anotar_producto"))
        self.assertIn("Cómo lo diagnosticó el técnico", _fuente("_otrep_ticket_anotar_lote"))
        # las rutas de evidencia ya aceptan solicitud_id: la foto de UNA pieza se asocia solo a SU solicitud
        self.assertIn('request.form.get("solicitud_id")', _fuente("ot2_api_equipo_evidencia"))
        self.assertIn('request.form.get("solicitud_id")', _fuente("ot2_api_producto_evidencia"))

    def test_repuestos_opciones_manda_sistema_fichas_y_arbol(self):
        for ruta, kind in (("ot2_api_equipo_repuestos_opciones", '"e"'), ("ot2_api_producto_repuestos_opciones", '"i"')):
            r = _fuente(ruta)
            self.assertIn('"sistema":', r, ruta)
            self.assertIn("_otrep_med_front(" + kind, r, ruta)
        f = _sin_docstring(_fuente("_otrep_med_front"))
        self.assertIn("_pm.para_front(sistema, kind", f)
        self.assertIn("_med_modelo_aprobadas(modelos, sku)", f)
        self.assertIn("_pe.sistema_de_clase(slug)", _fuente("_rsug_sistema_de_producto"))  # clases «eliptica» / «remo» cuando existan
        self.assertIn("_pe.refinar_sistema(s, nombre)", _fuente("_rsug_sistema_de_producto"))
        self.assertEqual(_fuente("_otrep_sugeridos_de_equipo").count("_otrep_sistema_de_equipo(v, m)"), 1)

    def test_la_fila_de_la_solicitud_entrega_las_medidas_legibles(self):
        f = _fuente("_otrep_fila")
        for pieza in ('s.pop("datos_json", None)', 's["datos_txt"] = _pm.resumen(_dd)', 's["datos_filas"] = [list(x) for x in _pm.filas(_dd)]',
                      's.pop("diagnostico_json", None)', 's["diagnostico_txt"] = _pm.diagnostico_txt(_gd)'):
            self.assertIn(pieza, f)
        self.assertNotIn('s["datos_json"]', f)                                   # el JSON crudo nunca sale
        # «Mis pedidos» del técnico lleva su propia medida
        self.assertIn('"medida": s.get("datos_txt") or s.get("medida") or ""', _fuente("ot2_detalle"))

    def test_el_aprendizaje_nunca_escribe_en_el_catalogo_ni_rompe_el_pedido(self):
        f = _sin_docstring(_fuente("_med_aprender_lote"))
        self.assertIn("except Exception as e:", f)                               # un dato de apoyo: nunca tumba el pedido
        self.assertIn("INSERT INTO cat_medidas_terreno", f)
        self.assertNotIn("cat_producto_piolas", f)                               # REGLA #32: nada se escribe solo en el catálogo
        self.assertNotIn("UPDATE cat_productos", f)
        self.assertIn('dat.get("medido_con") != "huincha"', f)                   # solo cuenta lo medido en terreno
        self.assertIn("_pm.aprender(cands", f)

    def test_pantalla_de_gestion_solo_para_gestion_nunca_tecnicos(self):
        for fn in ("repstock_medidas_terreno_listar", "repstock_medidas_terreno_decidir"):
            self.assertIn("_otrep_manual_required", _decoradores(fn), fn)         # excluye a CUALQUIER técnico, interno o externo
        codigo = _arbol()[0]
        self.assertIn('@app.route("/repuestos/api/medidas-terreno", methods=["GET"])', codigo)
        self.assertIn('@app.route("/repuestos/api/medidas-terreno/<int:mid>/decidir", methods=["POST"])', codigo)
        lst = _fuente("repstock_medidas_terreno_listar")
        self.assertIn("LIMIT %s OFFSET %s", lst)                                  # REGLA #4.3: paginada en el servidor
        self.assertIn("chile_fmt_filter", lst)                                    # REGLA #6: hora Chile
        dec = _fuente("repstock_medidas_terreno_decidir")
        self.assertIn('"descartar": "descartada"', dec)
        self.assertIn("Cuenta por qué se descarta", dec)                          # el motivo queda escrito
        self.assertIn("_mant_log(", dec)
        self.assertIn("decidido_por", dec)

    def test_nada_de_plata_ni_proveedores_en_lo_nuevo_REGLA_19_y_26(self):
        for fn in ("_otrep_med_aplicar", "_otrep_med_guardar", "_otrep_med_txt", "_otrep_med_avisos", "_otrep_med_front", "_med_modelo_aprobadas",
                   "_med_aprender_lote", "_otrep_diag_de_form", "_otrep_sistema_de_equipo", "_otrep_sistema_de_producto"):
            self.assertIsNone(_PLATA.search(_sin_docstring(_fuente(fn))), fn)
        modulo = re.sub(r'"""[\s\S]*?"""', "", _leer("ot_piezas_medidas.py"))      # sin los docstrings (explican que NO hay plata)
        self.assertIsNone(_PLATA.search(modulo), "ot_piezas_medidas.py")
        # un técnico ve las medidas que él mismo ingresó (viajan en la fila), nunca los datos de gestión
        self.assertNotIn("proveedor", json.dumps(pm.para_front("trotadora", "e"), ensure_ascii=False).lower())


# ═══════════════════════════════ 6) las pantallas de gestión, ticket y OT ═══════════════════════════════
class TestPantallas(unittest.TestCase):
    def test_gestion_ve_las_medidas_en_el_centro_de_solicitudes(self):
        for pieza in ("function rsMedidasHtml(s)", "rsMedidasHtml(s) + liga", "s.datos_filas", "Cómo lo diagnosticó el técnico", "s.datos_txt"):
            self.assertIn(pieza, REP, pieza)
        self.assertIn("s.medida && !(s.datos_filas || []).length", REP)          # sin duplicar el resumen viejo
        self.assertNotIn("proveedor_nombre", REP[REP.index("function rsMedidasHtml(s)"):REP.index("function rsMedidasHtml(s)") + 1500])

    def test_pantalla_de_medidas_por_aprobar_dentro_de_repuestos(self):
        self.assertIn('id="rsVistaMedidas"', REP)
        self.assertIn('id="rsVistaPaneMedidas"', REP)
        # el botón y el pane van dentro del `if not oculta_proveedores`: un técnico no los ve
        i = REP.index('id="rsVistaCompras"')
        self.assertIn("{% if not oculta_proveedores %}", REP[i - 200:i])
        self.assertIn("{% endif %}", REP[REP.index('id="rsVistaMedidas"'):REP.index('id="rsVistaMedidas"') + 400])
        self.assertIn("['Lista','Proveedor','Cliente','Compras','Medidas']", REP)
        self.assertIn("else if (v === 'medidas') rmCargar();", REP)
        # REGLA #4.3: paginación como Etiquetas (Mostrando A–B de N · tamaño · Anterior / Página X de Y / Siguiente)
        for pieza in ("'Mostrando ' + desde", "'Página ' + j.pagina + ' de ' + j.paginas", 'id="rmPageSize"', 'onclick="rmPag(-1)"', 'onclick="rmPag(1)"'):
            self.assertIn(pieza, REP, pieza)
        # REGLA #1: nada de popups nativos
        bloque = REP[REP.index("async function rmDecidir"):REP.index("function rsVista(v){")]
        self.assertIn("ilusConfirm(", bloque)
        self.assertIn("ilusPrompt(", bloque)
        self.assertNotRegex(bloque, r"(?<![\w.])(alert|confirm|prompt)\(")
        self.assertIn("/repuestos/api/medidas-terreno", bloque)

    def test_las_medidas_se_ven_en_la_tarjeta_del_equipo_en_la_ot_y_en_el_ticket(self):
        self.assertEqual(DET.count("{% if s.datos_filas %}"), 2)                  # equipo de ficha y producto de la revisión
        self.assertIn("s.datos_filas.map(function(f)", DET)                       # el repintado sin recargar también
        self.assertIn(".otd-rep-it .inf .otd-med", DET)
        self.assertIn("s.diagnostico_txt", DET)
        self.assertIn("s.datos_filas", TKF)
        self.assertIn("Diagnóstico guiado:", TKF)
        self.assertIn("{% if s.piola_metros and not s.datos_filas %}", DET)


# ═══════════════════════════════ 7) el teléfono ═══════════════════════════════
class TestTelefono(unittest.TestCase):
    def test_boton_que_le_pasa_va_primero_y_no_quita_los_otros(self):        # REGLA #4.2
        i = ACC_JS.index("function barHTML(key, d)")
        barra = ACC_JS[i:ACC_JS.index("function pintar(key)")]
        self.assertIn('data-f="diag"', barra)
        self.assertIn("¿Qué le pasa a ", barra)
        self.assertIn("Te ayudo a decidir qué hacer", barra)
        self.assertIn("(inter ? 'este producto' : 'este equipo')", barra)
        self.assertLess(barra.index("oa-diag"), barra.index('class="oa-grid"'))                      # primero, antes de los demás
        self.assertLess(barra.index("Fuera de lo normal"), barra.index("oa-diag"))                    # dentro de «Fuera de lo normal»
        for accion in ("'fs'", "'baja'", "'nollego'", "'noesta'", "'incidencia'", 'data-f="pedir"'):  # los otros cinco siguen
            self.assertIn(accion, barra, accion)
        self.assertIn("oa-primary", barra)                                                           # el pulso se queda en «Pedir repuesto»
        self.assertNotIn("oa-primary", barra[barra.index("oa-diag"):barra.index('class="oa-grid"')])
        self.assertIn("var puedeDiag = !bj && !nlTotal && !np && (p.ped || g.length > 0);", barra)

    def test_boton_compacto_en_el_computador_y_grande_en_el_celular(self):
        self.assertIn(".otd-acc .oa-diag{width:100%;min-height:56px", ACC_CSS)                      # celular: ancho y cómodo para el dedo
        i = ACC_CSS.index(".otd-acc .oa-diag{width:auto;flex:none;min-height:40px")
        self.assertIn("@media(min-width:768px){", ACC_CSS[i - 40:i])                                 # computador (≥768 px): ≈40 px, como la barra compacta
        self.assertIn(".otd-acc .oa-diag small{display:none}", ACC_CSS[i:i + 400])
        # la barra compacta de main (cba837fc) sigue ahí
        for css in ("@media(min-width:768px){\n  .otd-acc{flex-direction:row;flex-wrap:wrap", ".otd-acc .oa-grid .oa-btn{min-height:38px"):
            self.assertIn(css, ACC_CSS, css)
        # toques ≥ 44 px
        for css in (".oa-dop{display:grid;grid-template-columns:34px 1fr 18px;gap:12px;align-items:center;text-align:left;min-height:64px",
                    ".oa-peligro{display:flex;align-items:center;gap:12px;width:100%;min-height:56px", ".oa-ch{min-height:46px"):
            self.assertIn(css, ACC_CSS, css)
        for regla in (r"\.oa-pend-i \.oa-btn\{min-height:44px", r"\.oa-mod-i \.oa-btn\{min-height:44px"):
            self.assertRegex(ACC_CSS, regla)

    def test_la_semilla_se_aplica_DESPUES_de_init(self):                       # nota técnica de la propuesta: init() borra `extra`
        a = ACC_JS[ACC_JS.index("  function abrir(flow, key, extra) {"):ACC_JS.index("  function cerrar(vinoDeAtras)")]
        self.assertLess(a.index("FLUJO[flow].init(S)"), a.index("FLUJO[flow].semilla(S, sem)"))
        self.assertIn("delete ex.semilla;", a)
        self.assertIn("if (sem && FLUJO[flow].semilla)", a)
        for flujo in ("fuera", "baja", "nollego", "noesta", "incidencia", "pedir"):
            ini = ACC_JS.index(f"var {flujo} = {{")
            objeto = ACC_JS[ini:ACC_JS.index("\n  };", ini)]
            self.assertIn("semilla: function (s, sem)", objeto, flujo)
        self.assertIn("pedir.aplicarSugeridas(s);", ACC_JS)
        self.assertIn("if (s.sem && s.sem.ops) return;", ACC_JS)                 # no se piden dos veces las piezas

    def test_nada_se_crea_ni_se_marca_solo(self):                              # REGLA #32
        d = ACC_JS[ACC_JS.index("var PERM = { pedir: 'ped'"):ACC_JS.index("  /* ───────────────────────────── pantalla: MIS PEDIDOS")]
        self.assertNotIn("api('POST'", d)                                      # el diagnóstico NUNCA escribe: solo sugiere
        self.assertNotIn("fd.append", d)
        self.assertIn("Nada se crea ni se marca solo", d)
        self.assertIn("Seguir con esto", d)
        self.assertIn("Elegir otra cosa", d)
        self.assertIn("Te sugiero…", d)
        self.assertIn("Puede lastimar a alguien", d)
        self.assertIn('data-a="peligro"', d)
        self.assertIn("Pregunta ' + k + ' de ' + M", d)
        # el riesgo llega como PREGUNTA (sugerido), no como hecho: no se marca solo
        fs = ACC_JS[ACC_JS.index("var fuera = {"):ACC_JS.index("var baja = {")]
        self.assertIn("semilla: function (s, sem) { semillaBase(s, sem); if (sem.motivo) s.motivo = sem.motivo; if (sem.texto) s.detalle = sem.texto; },", fs)
        self.assertNotIn("s.riesgo = sem.riesgo", fs)
        self.assertIn("'Sugerido: confírmalo tú'", fs)

    def test_el_rol_manda_un_externo_no_da_de_baja(self):
        f = ACC_JS[ACC_JS.index("function permitida(s, accion)"):ACC_JS.index("function tituloRes(r)")]
        for pieza in ("PERM = { pedir: 'ped'", "if (accion === 'esperar') return true;", "ya está fuera de servicio",
                      "que ILUS decida", "Tú no das de baja: avisas que no tiene arreglo y ILUS decide."):
            self.assertIn(pieza, ACC_JS, pieza)
        self.assertIn("return null;", f)                                        # sin acción permitida no se inventa nada

    def test_tarjeta_medidas_con_dibujo_toques_y_salida_no_pude(self):
        for pieza in ("Medidas de la pieza", "Cómo se mide", "No pude medir esta pieza", "Sube una foto de la pieza con la huincha al lado",
                      "Más datos (opcional)", "Según el catálogo de este modelo", "Solo ayuda: confírmala con tu huincha.",
                      "Sí, está bien así", "Pedir cada una por separado", "inputmode=\"decimal\"", "Foto de ESTA pieza con la huincha al lado",
                      "Foto de la pieza con la huincha al lado", "Obligatorias", "aria-label=\"Cómo se mide\""):
            self.assertIn(pieza, ACC_JS, pieza)
        self.assertIn("Medidas, foto y envío", ACC_JS)
        self.assertIn("Foto y envío", ACC_JS)                                    # las mismas palabras del prototipo siguen
        # eco para cazar el 23,5 contra 235
        for pieza in ("function ecoDe(f, u)", "' cm = '", "' mm = '", 'class="oa-eco"'):
            self.assertIn(pieza, ACC_JS, pieza)
        # sin pie de metro (en el texto que ve el técnico; el comentario del código sí lo explica)
        visible = re.sub(r"/\*.*?\*/", "", ACC_JS, flags=re.S)
        self.assertNotIn("pie de metro", visible.lower())
        self.assertIn("Elige el botón más parecido", _leer("ot_piezas_medidas.py"))

    def test_las_medidas_viajan_por_linea_y_se_guardan_en_el_borrador(self):
        for pieza in ("var dd = datosParaEnviar(s, l);", "if (dd) { o.datos = dd; o.ficha = dd.ficha; }", "ref: l.uid",
                      "fd.append('diagnostico'", "refs: S.fotos.map(function (f) { return f.ref || ''; })",
                      "diag: S.diag || null, sug: S.sug || null, pend: S.pend || null", "cargarFotosBorrador(s, b.nFotos, b.refs)"):
            self.assertIn(pieza, ACC_JS, pieza)
        g = ACC_JS[ACC_JS.index("function guardarBorrador(S)"):ACC_JS.index("function cargarFotosBorrador")]
        self.assertIn("sel: S.sel", g)                                           # l.datos viaja dentro de cada línea: el borrador y el modo sin señal las conservan

    def test_foto_por_pieza_clave_y_mas_de_3_fotos_por_pedido(self):          # hueco 3 de la propuesta
        self.assertIn("var MAX_FOTOS = 8;", ACC_JS)
        for pieza in ("propias: claves.length >= 2", "sinFoto.length ? { t: 'la foto de '", "f2.append('solicitud_id', sidRef)",
                      "s.fotos = fotosOrdenadas(s, e.me.propias);", "camlinea: function", "ref: S.camRef || ''"):
            self.assertIn(pieza, ACC_JS, pieza)
        self.assertIn("fd.append('evidencia', s.fotos[0].file", ACC_JS)           # la foto general va primero

    def test_los_iconos_que_usa_el_js_existen(self):
        usados = set(re.findall(r"bi bi-([a-z0-9]+(?:-[a-z0-9]+)*)(?=['\"\s<])", ACC_JS))          # los armados con ' + x se prueban aparte
        usados |= {"chevron-up", "chevron-down"}                                                   # 'bi bi-chevron-' + (l.mas ? 'up' : 'down')
        faltan = sorted(i for i in usados if i not in ICONOS)
        self.assertEqual(faltan, [])
        self.assertIn("clipboard2-pulse", usados)

    def test_el_js_sin_popups_nativos_ni_plata(self):
        codigo = re.sub(r"/\*.*?\*/", "", ACC_JS, flags=re.S)
        codigo = re.sub(r"(?m)^\s*//.*$", "", codigo)
        self.assertIsNone(_PLATA.search(codigo), _PLATA.search(codigo))
        self.assertNotRegex(codigo, r"(?<![\w.])(alert|confirm|prompt)\(")
        self.assertEqual(ACC_JS.count("document.createElement('div')"), 1)

    @unittest.skipUnless(HAY_NODE, "node no está instalado")
    def test_el_js_compila(self):
        import subprocess
        r = subprocess.run(["node", "--check", os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "static", "ot_acciones.js")],
                           capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, r.stderr)


# ═══════════════════════════════ 8) espejo JS ↔ Python (corre el JS en node) ═══════════════════════════════
@unittest.skipUnless(HAY_NODE, "node no está instalado")
class TestEspejoJsPython(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        fns = ["tiene", "parseNum", "fmtNum", "ecoDe", "condOk", "altTxt", "factor", "datosDe", "medCheck"]
        cls.js = "\n".join(_js_funcion(n, ACC_JS) for n in fns)

    def _correr(self, expr):
        return _node(self.js + "\nvar MED = function(){ return null; };\nconsole.log(JSON.stringify(" + expr + "));")

    def test_parse_num_igual_en_js_y_python(self):
        casos = ["2,35", "2.35", "2.900", "2900", "235 cm", "1.234,5", " 520 ", "", "abc", "1,2,3", "-5", "0"]
        js = self._correr("[" + ",".join(json.dumps(c) for c in casos) + "].map(parseNum)")
        for c, j in zip(casos, js):
            p = pm.parse_num(c)
            if c in ("-5",):
                self.assertTrue(j is None or j == p, c)          # el JS no acepta el signo: lo marca como «escribe un número»
                continue
            self.assertEqual(j, p, c)

    def test_fmt_y_eco(self):
        self.assertEqual(self._correr("[fmtNum(2.35), fmtNum(2900), fmtNum(5), fmtNum(12.5)]"), ["2,35", "2.900", "5", "12,5"])
        self.assertEqual([pm._fmt(x) for x in (2.35, 2900, 5, 12.5)], ["2,35", "2.900", "5", "12,5"])
        self.assertEqual(self._correr("[ecoDe(235,'cm'), ecoDe(2900,'mm'), ecoDe(520,'mm'), ecoDe(50,'mm'), ecoDe(0,'cm')]"),
                         ["235 cm = 2,35 m", "2.900 mm = 2,9 m", "520 mm = 52 cm", "", ""])

    def test_validacion_igual_en_js_y_python(self):
        def js_check(fid, campos, medido=""):
            ficha = pm.ficha_publica(fid)
            expr = ("(function(){ var f = %s; var l = {datos:{ficha:f.id, medido_con:%s, motivo_no:'', campos:%s, confirmados:{}, otro:{}}};"
                    " var r = medCheck({}, l, f); return {faltan:r.faltan.length, imp:Object.keys(r.imp), av:Object.keys(r.av), completo:r.completo}; })()"
                    % (json.dumps(ficha), json.dumps(medido), json.dumps(campos)))
            return self._correr(expr)

        casos = [
            ("A1", A1_OK, ""), ("A1", dict(A1_OK, largo_cm="12"), ""), ("A1", dict(A1_OK, largo_cm="99999"), ""), ("A1", dict(A1_OK, largo_cm="-3"), ""),
            ("A1", {}, ""), ("A1", {"largo_cm": "235"}, ""), ("A1", {}, "no_pude"), ("A1", dict(A1_OK, term_pesas="esparrago"), ""),
            ("E1", {"ancho_mm": "520", "vuelta_mm": "2.900"}, ""), ("E1", {"ancho_mm": "5200", "vuelta_mm": "2900"}, ""), ("E1", {"ancho_mm": "520"}, ""),
            ("F1", {"tipo": "canales", "codigo": "6PJ508"}, ""), ("F1", {"tipo": "canales", "canales": "6"}, ""), ("F1", {"codigo": "6PJ508"}, ""),
            ("D1", {"largo_mm": "350", "ancho_mm": "200", "que_pide": "funda"}, ""), ("D1", {"largo_mm": "350"}, ""),
            ("C1", {"diam_mm": "8", "largo_mm": "100", "cabeza": "t"}, ""), ("C1", {"diam_mm": "8", "largo_mm": "10"}, ""),
            ("G1", {"seccion": "2x2", "agujero": "5/8", "separacion": "50"}, ""), ("G1", {"seccion": "2x2"}, ""),
            ("B1", {}, ""), ("B1", {"diam_mm": "90"}, ""), ("B1", {"diam_mm": "9000"}, ""),
        ]
        for fid, campos, medido in casos:
            ctx = (fid, campos, medido)
            j = js_check(fid, campos, medido)
            limpio, err = pm.normalizar_datos(fid, {"ficha": fid, "campos": campos, "medido_con": medido, "motivo_no": "no_hay_huincha" if medido == "no_pude" else ""})
            codigo = err[1] if err else None
            self.assertEqual(bool(j["faltan"]), codigo == "MEDIDA_REQUERIDA", ctx)
            self.assertEqual(bool(j["imp"]), codigo == "MEDIDA_INVALIDA", ctx)
            if limpio is not None:
                self.assertEqual(len(j["av"]), len(limpio["avisos"]), ctx)                  # los mismos rangos avisan

"""Shipit — elegir el operador + conectar el separador de calle/número.

Dos pendientes que Daniel pidió cerrar el 2026-08-05:

1. ELEGIR EL OPERADOR. Shipit es un agregador (Starken, Chilexpress, Global
   Tracking, Blue Express...). El backend tomaba `disponibles[0]` — siempre el
   más barato. Textual: "siento que me está obligando a escoger Global
   Tracking, solamente porque es más barato". El más barato no siempre es el
   correcto: hay operadores más caros que llegan antes o cubren una comuna
   difícil. La decisión es de logística.

2. SEPARAR CALLE Y NÚMERO. Shipit los exige por separado; ILUS guarda un solo
   texto. split_street_number() ya existe y está probada — faltaba conectarla
   a la pantalla.

La lógica de (1) y la mitad de UI de (2) viven en JavaScript, así que este
módulo corre los harness de node (tests/test_shipit_*.js) contra el archivo
REAL static/cubicador_asignar.js. Los fixtures del separador NO se inventan:
salen de shipit_client.clasificar_direccion(), así la prueba cruza de verdad
el límite Python↔JS.

Si node no está instalado, esas dos pruebas se saltan (no fallan) — el resto
sigue verificándose en Python puro.

Correr con:  py -m unittest tests.test_shipit_operador_seleccion -v
"""
import json
import os
import re
import shutil
import subprocess
import tempfile
import unittest

import shipit_client as sc

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
JS_ASIGNAR = os.path.join(RAIZ, "static", "cubicador_asignar.js")
TPL_ASIGNAR = os.path.join(RAIZ, "templates", "cubicador", "asignar.html")

# Direcciones que usa el harness de UI. Se resuelven con la función real.
DIRECCIONES_FIXTURE = [
    "Colon 1265",
    "Los Aromos 145 depto 402",
    "Camino a Melipilla s/n",
    "Otra calle 10",
]

NODE = shutil.which("node")


def _leer(path):
    with open(path, encoding="utf-8") as fh:
        return fh.read()


def _bloque_shipit(js):
    """Solo el código NUEVO de Shipit dentro de cubicador_asignar.js.

    Las reglas del proyecto se verifican sobre lo que se agregó, no sobre las
    ~3.700 líneas que ya estaban (ahí hay legacy con `alert()` que el shim
    global de ilus_ui.js ya enruta, y comentarios que nombran a otros couriers
    a propósito). Sin este recorte la prueba mide el archivo entero y deja de
    decir algo útil.
    """
    ini = js.index("SHIPIT — EL OPERADOR")
    fin = js.index("function actualizarTarifas(", ini)
    return js[ini:fin]


def _correr_node(script, *args):
    """Corre un harness de node y devuelve su salida. Falla con el stderr."""
    proc = subprocess.run(
        [NODE, os.path.join(RAIZ, "tests", script)] + list(args),
        capture_output=True, text=True, encoding="utf-8", errors="replace",
        cwd=RAIZ, timeout=120,
    )
    if proc.returncode != 0:
        raise AssertionError(
            f"{script} falló (exit {proc.returncode})\n"
            f"--- stdout ---\n{proc.stdout}\n--- stderr ---\n{proc.stderr}"
        )
    return proc.stdout


# ══════════════════════════════════════════════════════════════════════════
#  1) clasificar_direccion() — el empaquetado que consume la pantalla
# ══════════════════════════════════════════════════════════════════════════
class TestClasificarDireccion(unittest.TestCase):
    """`bloqueada` separa "falta el número" (traba el despacho) de "revisa el
    número" (solo avisa). La UI trata los dos casos distinto, así que la
    distinción tiene que ser confiable."""

    def test_direccion_limpia_no_bloquea_y_no_avisa(self):
        r = sc.clasificar_direccion("Colon 1265")
        self.assertEqual(("Colon", "1265"), (r["calle"], r["numero"]))
        self.assertFalse(r["bloqueada"])
        self.assertEqual([], r["problemas"])

    def test_depto_sin_coma_avisa_pero_no_bloquea(self):
        # El caso caro: tomar el 402 del depto genera una guía VÁLIDA a la
        # dirección EQUIVOCADA. Se resuelve con 145 y se pide confirmación.
        r = sc.clasificar_direccion("Los Aromos 145 depto 402")
        self.assertEqual("145", r["numero"])
        self.assertNotEqual("402", r["numero"])
        self.assertFalse(r["bloqueada"], "hay número: no debe trabar, solo avisar")
        self.assertTrue(r["problemas"], "pero tiene que avisar")

    def test_sin_numero_bloquea(self):
        for direccion in ("Camino a Melipilla s/n", "Las Condes",
                          "Camino Rural Km 12", ""):
            with self.subTest(direccion=direccion):
                r = sc.clasificar_direccion(direccion)
                self.assertTrue(r["bloqueada"], f"«{direccion}» debe bloquear")
                self.assertTrue(r["problemas"], "y decir por qué")

    def test_numero_sin_calle_bloquea(self):
        r = sc.clasificar_direccion("1265")
        self.assertTrue(r["bloqueada"])
        self.assertEqual("", r["calle"])

    def test_los_mensajes_son_especificos_no_genericos(self):
        # Daniel, textual: "no vayas a mandar algo genérico, algo específico".
        # Cada problema debe citar la dirección concreta que lo provocó.
        for direccion in ("Camino a Melipilla s/n", "Camino Rural Km 12",
                          "Las Condes"):
            with self.subTest(direccion=direccion):
                r = sc.clasificar_direccion(direccion)
                self.assertTrue(
                    any(direccion in p for p in r["problemas"]),
                    f"el mensaje debe citar «{direccion}»: {r['problemas']}")

    def test_nunca_lanza(self):
        for entrada in (None, "", 12345, "   ", ",,,"):
            with self.subTest(entrada=entrada):
                r = sc.clasificar_direccion(entrada)
                self.assertIn("bloqueada", r)
                self.assertIsInstance(r["problemas"], list)

    def test_modulo_sigue_puro(self):
        # shipit_client.py no puede importar Flask/BD/red: es lo que permite
        # probarlo sin levantar la app.
        fuente = _leer(os.path.join(RAIZ, "shipit_client.py"))
        for prohibido in ("import flask", "from flask", "import requests",
                          "import pymysql", "urllib.request"):
            self.assertNotIn(prohibido, fuente,
                             f"shipit_client.py debe seguir siendo puro ({prohibido})")


# ══════════════════════════════════════════════════════════════════════════
#  2) Contrato frontend → backend (lo que hay que soportar en app.py)
# ══════════════════════════════════════════════════════════════════════════
class TestContratoConElBackend(unittest.TestCase):
    """El operador elegido y la calle/número corregidos tienen que VIAJAR al
    backend. Si alguien renombra un campo, esto lo caza antes del deploy."""

    def setUp(self):
        self.js = _leer(JS_ASIGNAR)

    def test_payload_del_manifiesto_lleva_el_operador(self):
        for campo in ("courier_operador", "courier_operador_servicio",
                      "courier_operador_precio", "courier_operador_dias",
                      "courier_operador_manual"):
            self.assertIn(campo + ":", self.js,
                          f"el payload de enviar-manifiesto debe incluir {campo}")

    def test_payload_del_manifiesto_lleva_calle_y_numero(self):
        for campo in ("shipit_calle", "shipit_numero"):
            self.assertIn(campo + ":", self.js,
                          f"el payload debe incluir {campo} para la Fase 2 de Shipit")

    def test_usa_el_endpoint_del_separador(self):
        self.assertIn("/transporte/api/shipit/direccion", self.js)

    def test_no_reimplementa_el_separador_en_javascript(self):
        # La regla del separador (18 palabras de unidad, s/n, km...) vive en
        # Python y tiene 17 tests. Duplicarla en JS es garantía de que las dos
        # copias se separen y una mande el paquete a otra parte. Se buscan
        # marcas que SOLO aparecerían en una reimplementación (nadie escribe
        # "dpto" o "_PALABRAS_UNIDAD" en JS por otro motivo).
        for palabra in ("_PALABRAS_UNIDAD", "dpto", "ofic", "parcela"):
            self.assertNotIn(
                palabra, self.js,
                f"«{palabra}» sugiere que se reimplementó el separador en JS")

    def test_reusa_el_excel_de_diagnostico_existente(self):
        tpl = _leer(TPL_ASIGNAR)
        self.assertIn("diagnostico-direcciones.xlsx", tpl,
                      "el modal debe llevar al Excel que ya existe, no a uno nuevo")


# ══════════════════════════════════════════════════════════════════════════
#  3) Reglas del proyecto sobre lo nuevo
# ══════════════════════════════════════════════════════════════════════════
class TestReglasDelProyecto(unittest.TestCase):

    def setUp(self):
        self.js = _leer(JS_ASIGNAR)
        self.tpl = _leer(TPL_ASIGNAR)

    def test_sin_alert_confirm_prompt_nativos(self):
        # CLAUDE.md Regla #1. Se buscan llamadas, no las palabras sueltas.
        nuevo = _bloque_shipit(self.js)
        for rx in (r"(?<![\w.])alert\s*\(", r"(?<![\w.])confirm\s*\(",
                   r"(?<![\w.])prompt\s*\("):
            self.assertIsNone(
                re.search(rx, nuevo),
                f"código nuevo con popup nativo ({rx}) — usar ilusAlert/ilusConfirm")

    def test_touch_targets_y_font_size_en_movil(self):
        # Regla #3: 44px de alto en botones, 16px en inputs (anti-zoom iOS).
        self.assertIn("min-height:44px", self.tpl)
        self.assertIn("font-size:16px", self.tpl)

    def test_paleta_ilus(self):
        # Regla #2: el rojo de selección es el rojo ILUS.
        self.assertIn("#dc2626", self.tpl)

    def test_no_filtra_la_politica_de_felca_milling(self):
        # Confidencial: los precios de Felca/Milling derivan de FedEx menos un
        # descuento interno. Nada de eso puede aparecer en lo nuevo.
        nuevo = _bloque_shipit(self.js)
        for rx in (r"regla_pct", r"es_regla_comercial", r"derivado_de",
                   r"FALLBACK_FACTOR", r"descuento", r"felca", r"milling"):
            self.assertIsNone(re.search(rx, nuevo, re.I),
                              f"«{rx}» no puede aparecer en el código nuevo")

    def test_limites_de_shipit_son_los_reales(self):
        # 1 bulto y 15 kg (NO 20). Espejo de shipit_client.
        self.assertIn("SHIPIT_MAX_BULTOS  = 1", self.js)
        self.assertIn("SHIPIT_MAX_PESO_KG = 15", self.js)
        self.assertEqual(1, sc.MAX_BULTOS)
        self.assertEqual(15.0, sc.MAX_PESO_KG)

    def test_no_se_borro_nada_de_la_pantalla(self):
        # Regla #4.2: se AGREGA la posibilidad de elegir; lo que ya existía
        # tiene que seguir ahí.
        for pieza in ("modalManifiesto", "modalAudit", "courierList",
                      "cli-dir", "cubicador_asignar.css", "cubicador_tabs.js"):
            self.assertIn(pieza, self.tpl, f"se perdió «{pieza}» del template")
        for fn in ("function renderCouriers(", "function setCourier(",
                   "function _shipitDesgloseHtml(", "function abrirAuditoriaCourier(",
                   "async function enviarAManifiesto("):
            self.assertIn(fn, self.js, f"se perdió «{fn}» del JS")


# ══════════════════════════════════════════════════════════════════════════
#  4) Comportamiento real en el navegador (harness de node)
# ══════════════════════════════════════════════════════════════════════════
@unittest.skipIf(NODE is None, "node no está instalado en este equipo")
class TestComportamientoEnElNavegador(unittest.TestCase):

    def test_seleccion_de_operador(self):
        """Varios operadores · uno solo · ninguno · el elegido desaparece."""
        salida = _correr_node("test_shipit_operador_seleccion.js")
        self.assertIn("selección de operador OK", salida, salida)
        # Los cuatro escenarios pedidos, cada uno con su marca.
        for marca in ("ok 1/4", "ok 2/4", "ok 3/4", "ok 4/4"):
            self.assertIn(marca, salida, salida)

    def test_separador_de_calle_y_numero(self):
        """El separador REAL (Python) alimenta la UI y esta bloquea bien."""
        fixtures = {d: sc.clasificar_direccion(d) for d in DIRECCIONES_FIXTURE}
        # Guarda contra un fixture que deje de ser representativo.
        self.assertTrue(fixtures["Camino a Melipilla s/n"]["bloqueada"])
        self.assertEqual("145", fixtures["Los Aromos 145 depto 402"]["numero"])

        tmp = tempfile.NamedTemporaryFile(
            "w", suffix=".json", delete=False, encoding="utf-8")
        try:
            json.dump(fixtures, tmp, ensure_ascii=False)
            tmp.close()
            salida = _correr_node("test_shipit_direccion_ui.js", tmp.name)
        finally:
            os.unlink(tmp.name)
        self.assertIn("separador de calle/número OK", salida, salida)
        for marca in ("ok 1/6", "ok 2/6", "ok 3/6", "ok 4/6", "ok 5/6", "ok 6/6"):
            self.assertIn(marca, salida, salida)

    def test_el_javascript_compila(self):
        proc = subprocess.run([NODE, "--check", JS_ASIGNAR],
                              capture_output=True, text=True, cwd=RAIZ, timeout=60)
        self.assertEqual(0, proc.returncode, proc.stderr)


if __name__ == "__main__":
    unittest.main(verbosity=2)

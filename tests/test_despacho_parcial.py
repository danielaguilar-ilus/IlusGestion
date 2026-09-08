"""Despacho parcial por línea (2026-09-08). Pedido explícito de Daniel:
"que podamos modificar las cantidades solo en términos menores... si un
cliente quiere comprar 100 unidades de algo que podamos despachar solo 20
en caso de quiebres de stock o flexibilidad ante alguna desviación."

Respuestas de Daniel a las 3 preguntas de diseño (AskUserQuestion,
2026-09-08):
  1. El saldo restante queda pendiente en el Monitor -> tabla nueva
     transport_cantidad_saldo, igual patrón que transport_zz_saldo.
  2. El flete (ZZ Envío) se prorratea según % despachado.
  3. Reducir la cantidad exige motivo obligatorio (auditoría).

Este archivo testea la función PURA (_dp_validar_linea_parcial, sin
Flask ni DB) que decide si una declaración de despacho parcial es válida.
La orquestación (leer transport_commitment_lines, upsert de
transport_cantidad_saldo, _tr_log) vive dentro de
tr_cubicador_enviar_manifiesto y requiere DB real -- fuera de alcance de
estos tests unitarios, cubierta por revisión de código + prueba manual en
el navegador.

Correr con:  py -m unittest tests.test_despacho_parcial -v
"""
import ast
import unittest

APP_SRC = open("app.py", encoding="utf-8", errors="ignore").read()
JS_SRC = open("static/cubicador_asignar.js", encoding="utf-8", errors="ignore").read()
_ARBOL = ast.parse(APP_SRC)


def _fn(nombre):
    for n in ast.walk(_ARBOL):
        if isinstance(n, ast.FunctionDef) and n.name == nombre:
            return ast.unparse(n)
    raise AssertionError(f"no se encontro {nombre} en app.py")


def _cargar_validador():
    ns = {}
    src = _fn("_dp_validar_linea_parcial")
    exec(compile(ast.parse(src), "<dp>", "exec"), ns)
    return ns["_dp_validar_linea_parcial"]


class TestValidarLineaParcial(unittest.TestCase):
    """_dp_validar_linea_parcial: la regla de negocio en un solo lugar."""

    def setUp(self):
        self.f = _cargar_validador()

    def test_despacho_parcial_normal_ok(self):
        # Caso del ejemplo de Daniel: 100 comprados, despacha 20, quedan 80.
        ok, err, saldo = self.f("SKU1", 20, 100, "quiebre de stock")
        self.assertTrue(ok)
        self.assertIsNone(err)
        self.assertEqual(saldo, 80)

    def test_despacha_el_techo_completo_no_exige_motivo(self):
        # Si se despacha TODO lo que queda, no hay nada que explicar.
        ok, err, saldo = self.f("SKU1", 100, 100, "")
        self.assertTrue(ok)
        self.assertEqual(saldo, 0)

    def test_rechaza_mas_del_techo(self):
        # "solo en términos menores" -- nunca se puede pedir MÁS de lo
        # comprado (o de lo que ya quedaba pendiente).
        ok, err, saldo = self.f("SKU1", 150, 100, "motivo")
        self.assertFalse(ok)
        self.assertIn("SKU1", err)
        self.assertIn("100", err)
        self.assertIsNone(saldo)

    def test_rechaza_cero(self):
        ok, err, _ = self.f("SKU1", 0, 100, "motivo")
        self.assertFalse(ok)

    def test_rechaza_negativo(self):
        ok, err, _ = self.f("SKU1", -5, 100, "motivo")
        self.assertFalse(ok)

    def test_exige_motivo_si_queda_saldo(self):
        ok, err, saldo = self.f("SKU1", 20, 100, "")
        self.assertFalse(ok)
        self.assertIn("motivo", err.lower())
        self.assertIsNone(saldo)

    def test_motivo_con_solo_espacios_no_cuenta(self):
        ok, err, _ = self.f("SKU1", 20, 100, "   ")
        self.assertFalse(ok)

    def test_segundo_despacho_parcial_usa_el_saldo_previo_como_techo(self):
        # Ya se habían despachado 20 de 100 (quedaban 80) -- ahora se
        # despachan otras 30: el techo es 80, NO 100 de nuevo.
        ok, err, saldo = self.f("SKU1", 30, 80, "sigue el quiebre")
        self.assertTrue(ok)
        self.assertEqual(saldo, 50)

    def test_no_se_puede_resetear_el_saldo_pidiendo_mas_que_lo_que_queda(self):
        # Si ya quedaban 80 pendientes, pedir 100 (la cantidad ORIGINAL del
        # documento) debe rechazarse -- el techo real es 80, no 100.
        ok, err, _ = self.f("SKU1", 100, 80, "motivo")
        self.assertFalse(ok)
        self.assertIn("80", err)

    def test_cantidad_invalida_no_revienta(self):
        ok, err, saldo = self.f("SKU1", "no-es-numero", 100, "motivo")
        self.assertFalse(ok)
        self.assertIsNone(saldo)


class TestEndpointUsaElValidador(unittest.TestCase):
    """tr_cubicador_enviar_manifiesto: el endpoint real llama al validador
    puro (no reimplementa la regla en línea) y corta con 400 antes de
    tocar el manifiesto si algo no cuadra."""

    def test_llama_al_validador_puro(self):
        f = _fn("tr_cubicador_enviar_manifiesto")
        self.assertIn("_dp_validar_linea_parcial(", f)

    def test_no_reimplementa_el_limite_en_linea(self):
        # Verificación adversarial: si alguien vuelve a escribir el chequeo
        # `cant_desp > techo` a mano en el endpoint (además de llamar al
        # validador), es una duplicación silenciosa de la regla de negocio.
        f = _fn("tr_cubicador_enviar_manifiesto")
        self.assertNotIn("cant_desp > techo", f)

    def test_techo_sale_de_transport_commitment_lines_no_del_cliente(self):
        # El techo la PRIMERA vez debe venir de una consulta a la BD, nunca
        # solo del payload del cliente (que podría mandar cualquier cosa).
        f = _fn("tr_cubicador_enviar_manifiesto")
        self.assertIn("transport_commitment_lines", f)
        self.assertIn("_cantidad_saldo_get_map(", f)

    def test_guarda_en_transport_cantidad_saldo(self):
        f = _fn("tr_cubicador_enviar_manifiesto")
        self.assertIn("INSERT INTO transport_cantidad_saldo", f)
        self.assertIn("ON DUPLICATE KEY UPDATE", f)

    def test_deja_auditoria(self):
        f = _fn("tr_cubicador_enviar_manifiesto")
        i = f.find("transport_cantidad_saldo")
        bloque = f[i:i + 2000]
        self.assertIn("_tr_log(", bloque)
        self.assertIn("despacho parcial", bloque.lower())


class TestTablaCantidadSaldo(unittest.TestCase):
    """La tabla nueva sigue el mismo patrón (aislada del sync del ERP) que
    transport_zz_saldo -- ver comentario en _ensure_transport_cantidad_saldo_table."""

    def test_tabla_tiene_unique_key_por_linea(self):
        f = _fn("_ensure_transport_cantidad_saldo_table")
        self.assertIn("CREATE TABLE IF NOT EXISTS transport_cantidad_saldo", f)
        self.assertIn("UNIQUE KEY uq_linea (tido, nudo, koprct)", f)

    def test_se_registra_al_boot_siempre_incluso_skip_migrations(self):
        self.assertIn("_ensure_transport_cantidad_saldo_table()", APP_SRC)


class TestUIDespachoParcial(unittest.TestCase):
    """El frontend no usa prompt() nativo (REGLA #1) y solo permite reducir,
    nunca superar el techo -- el input queda con max=techoParcial."""

    def test_usa_ilusprompt_no_prompt_nativo(self):
        i = JS_SRC.find("async function cambiarCantidadDespacho")
        self.assertGreater(i, 0)
        bloque = JS_SRC[i:i + 1800]
        self.assertIn("ilusPrompt(", bloque)
        self.assertNotIn("prompt(", bloque.replace("ilusPrompt(", ""))

    def test_input_tiene_techo_como_max(self):
        self.assertIn('max="${techoParcial}"', JS_SRC)

    def test_prorratea_el_flete_reusando_el_mecanismo_existente(self):
        i = JS_SRC.find("function actualizarZzEnvioProrateado")
        self.assertGreater(i, 0)
        bloque = JS_SRC[i:i + 1000]
        # Reusa guardarZzEnvioSaldo -- no duplica la persistencia del $.
        self.assertIn("guardarZzEnvioSaldo", bloque)

    def test_payload_manda_cantidad_a_despachar_solo_si_es_menor(self):
        i = JS_SRC.find("cantidad_a_despachar:")
        self.assertGreater(i, 0)
        bloque = JS_SRC[max(0, i - 300):i + 200]
        self.assertIn("_cantDeclarada", bloque)

    def test_recotiza_couriers_al_cambiar_la_cantidad(self):
        # Hallazgo real probando en vivo (BLV 23313, 2026-09-08): al bajar
        # la cantidad, la tarjeta de couriers se quedaba con la tarifa de
        # los 100 originales -- exactamente el patrón "el sistema calcula
        # bien y la pantalla muestra otra cosa" ya sufrido en este módulo.
        i = JS_SRC.find("async function cambiarCantidadDespacho")
        self.assertGreater(i, 0)
        bloque = JS_SRC[i:i + 2200]
        self.assertIn("actualizarTarifas", bloque)


if __name__ == "__main__":
    unittest.main(verbosity=2)

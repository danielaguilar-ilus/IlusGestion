"""★ Vida del cliente (2026-09-26, Etapa B -- Daniel: "en la ficha tiene
que estar toda la vida del cliente con nosotros... todo debe estar
conectado: repuestos, incidencias, tickets, OT y sus costos").

Prueba las funciones PURAS que clasifican datos para la pestaña nueva
(sin BD ni Flask), extraidas de app.py con ast -- mismo patron que
tests/test_repuestos_compras_fase5.py:

  _vida_cliente_bucket(tipo, tiene_repuestos)
      "Por que fuimos": instalacion / mantencion / inst_repuestos /
      correctiva / otros. Una OT con repuestos instalados SIEMPRE cae en
      "inst_repuestos", sin importar su `tipo` real en mant_visitas (una
      OT de instalacion de repuestos nace con tipo='correctiva', ver
      _ot_tipo_label_efectivo).

  _vida_margen_clase(margen_pct, margen_clp)
      Semaforo del margen por OT / del resultado total -- MISMO umbral
      que la tarjeta Finanzas de la OT (templates/ot2/detalle.html,
      `OTD_FIN_UMBRAL_BAJO = 10`): verde desde 10 %, ambar bajo 10 %,
      rojo en perdida.

Correr con:  py -m unittest tests.test_vida_cliente
(pytest NO esta instalado en el equipo de Daniel.)
"""
import ast
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
APP_PY = os.path.join(RAIZ, "app.py")


def _leer(path):
    with open(path, encoding="utf-8") as fh:
        return fh.read()


def _cargar_con_dependencias(nombre_funcion, nombres_globales=(), funciones_extra=()):
    """Extrae UNA funcion de app.py (por nombre) + las asignaciones de
    modulo que necesita (constantes/diccionarios top-level) + otras
    FUNCIONES puras de las que depende (`funciones_extra`), y las ejecuta
    juntas en un ambito aislado. Solo sirve para dependencias PURAS (sin
    Flask/MySQL) -- ver _cargar_funcion en test_repuestos_compras_fase5.py
    para el caso sin dependencias."""
    arbol = ast.parse(_leer(APP_PY))
    nodos = []
    pendientes_fn = set(funciones_extra) | {nombre_funcion}
    pendientes_glob = set(nombres_globales)
    for nodo in arbol.body:
        if isinstance(nodo, ast.FunctionDef) and nodo.name in pendientes_fn:
            nodos.append(nodo)
            pendientes_fn.discard(nodo.name)
        elif isinstance(nodo, ast.Assign) and pendientes_glob:
            for target in nodo.targets:
                if isinstance(target, ast.Name) and target.id in pendientes_glob:
                    nodos.append(nodo)
                    pendientes_glob.discard(target.id)
    assert not pendientes_glob, f"no se encontraron estas globales en app.py: {pendientes_glob}"
    assert not pendientes_fn, f"no se encontraron estas funciones en app.py: {pendientes_fn}"
    ambito = {}
    exec(compile(ast.Module(body=nodos, type_ignores=[]), "<app>", "exec"), ambito)
    assert nombre_funcion in ambito, f"no se encontro {nombre_funcion} en app.py"
    return ambito[nombre_funcion]


class TestVidaClienteBucket(unittest.TestCase):
    """'Por que fuimos' -- 4 KPI de la pestaña nueva."""

    @classmethod
    def setUpClass(cls):
        # staticmethod(): sin esto, `self.bucket(...)` la haría un metodo
        # ligado y le colaria `self` como primer argumento posicional.
        cls.bucket = staticmethod(_cargar_con_dependencias(
            "_vida_cliente_bucket", nombres_globales=["_VIDA_CLIENTE_TIPO_BUCKET"]))

    def test_instalacion(self):
        self.assertEqual(self.bucket("instalacion", False), "instalacion")

    def test_preventiva_es_mantencion(self):
        self.assertEqual(self.bucket("preventiva", False), "mantencion")

    def test_correctiva(self):
        self.assertEqual(self.bucket("correctiva", False), "correctiva")

    def test_retroactiva_cuenta_como_correctiva(self):
        self.assertEqual(self.bucket("retroactiva", False), "correctiva")

    def test_tipo_desconocido_es_otros(self):
        self.assertEqual(self.bucket("algo_raro", False), "otros")

    def test_none_es_otros(self):
        self.assertEqual(self.bucket(None, False), "otros")

    def test_tiene_repuestos_manda_SIEMPRE_sobre_el_tipo(self):
        # Una OT de instalacion de repuestos nace con tipo='correctiva'
        # (no se cambia -- ver _ot_tipo_label_efectivo), pero acá debe
        # clasificar como "inst_repuestos", no como "correctiva".
        self.assertEqual(self.bucket("correctiva", True), "inst_repuestos")
        self.assertEqual(self.bucket("preventiva", True), "inst_repuestos")
        self.assertEqual(self.bucket(None, True), "inst_repuestos")

    def test_case_insensitive_y_espacios(self):
        self.assertEqual(self.bucket("  Preventiva ", False), "mantencion")


class TestVidaMargenClase(unittest.TestCase):
    """Semaforo del margen -- mismo umbral que la OT (verde >=10%,
    ambar 0-10%, rojo en perdida)."""

    @classmethod
    def setUpClass(cls):
        cls.clase = staticmethod(_cargar_con_dependencias("_vida_margen_clase"))

    def test_margen_alto_es_verde(self):
        self.assertEqual(self.clase(41.0, 420000), "ok")

    def test_margen_justo_en_el_umbral_es_verde(self):
        self.assertEqual(self.clase(10.0, 10000), "ok")

    def test_margen_bajo_10_es_ambar(self):
        self.assertEqual(self.clase(0.8, 2000), "bajo")

    def test_margen_cero_es_ambar(self):
        self.assertEqual(self.clase(0.0, 0), "bajo")

    def test_margen_negativo_pct_es_rojo(self):
        self.assertEqual(self.clase(-5.0, -5000), "rojo")

    def test_perdida_en_clp_sin_pct_es_rojo(self):
        # OT-00219 de la maqueta: $0 cobrado (sin pct), -$95.000 de margen.
        self.assertEqual(self.clase(None, -95000), "rojo")

    def test_sin_dato_cuando_no_hay_pct_ni_perdida(self):
        self.assertEqual(self.clase(None, None), "sin_dato")
        self.assertEqual(self.clase(None, 0), "sin_dato")


def _rep(costo=0.0, bodega=0.0, compra=0.0, manual=0.0, n_sin_costo=0):
    return {"costo": costo, "por_origen": {"bodega": bodega, "compra": compra, "manual": manual},
            "n_sin_costo": n_sin_costo}


class TestOtResultadoFinanciero(unittest.TestCase):
    """★ D2 (revisión Opus 2026-09-26 -- Daniel: "la tarjeta de la OT y la
    fila de esa OT en Vida deben dar EXACTAMENTE el mismo Cobramos/Nos
    cuesta/Queda/semáforo"): _ot_resultado_financiero es la ÚNICA función
    que ambas vistas usan -- se prueban los 5 casos que pidió la revisión:
    costo incompleto, garantía, con repuestos, interna, contrato."""

    @classmethod
    def setUpClass(cls):
        cls.resultado = staticmethod(_cargar_con_dependencias(
            "_ot_resultado_financiero", funciones_extra=["_ot_es_interna", "_vida_margen_clase"]))

    def _v(self, **kw):
        base = {"tipo": "correctiva", "cubierto_por": "cliente", "costo": 100000,
                "costo_proveedor": 30000, "costo_despacho": 10000,
                "modalidad_cobro": "pagado", "cliente_id": 1}
        base.update(kw)
        return base

    def test_ot_interna_excluida_del_calculo(self):
        r = self.resultado(self._v(modalidad_cobro="interno"), None)
        self.assertEqual(r["estado"], "interna")
        self.assertEqual(r["clase"], "gris")
        self.assertIsNone(r["costo_total"])
        self.assertIsNone(r["margen_pct"])

    def test_costo_incompleto_sin_costo_proveedor(self):
        r = self.resultado(self._v(costo_proveedor=None), None)
        self.assertEqual(r["estado"], "incompleto")
        self.assertEqual(r["label"], "Falta un costo")
        self.assertIsNone(r["costo_total"])
        self.assertIsNone(r["margen_pct"])

    def test_costo_incompleto_sin_costo_despacho(self):
        r = self.resultado(self._v(costo_despacho=None), None)
        self.assertEqual(r["estado"], "incompleto")

    def test_garantia_sin_cobro_es_ambar_no_rojo(self):
        r = self.resultado(self._v(cubierto_por="garantia", costo=0), None)
        self.assertEqual(r["estado"], "garantia_sin_cobro")
        self.assertEqual(r["clase"], "ambar")
        self.assertEqual(r["costo_total"], 40000)
        self.assertLess(r["margen_clp"], 0)  # es costo real, aunque no rojo

    def test_contrato_no_es_rojo_aunque_no_haya_cobro(self):
        r = self.resultado(self._v(cubierto_por="contrato", costo=0), None)
        self.assertEqual(r["estado"], "contrato")
        self.assertEqual(r["clase"], "gris")
        self.assertEqual(r["costo_total"], 40000)

    def test_ok_normal_con_margen_verde(self):
        r = self.resultado(self._v(costo=100000, costo_proveedor=30000, costo_despacho=10000), None)
        self.assertEqual(r["estado"], "ok")
        self.assertIsNone(r["label"])
        self.assertEqual(r["costo_total"], 40000)
        self.assertEqual(r["margen_clp"], 60000)
        self.assertEqual(r["clase"], "ok")

    def test_con_repuestos_suma_al_costo_total(self):
        # M4: el costo de repuestos entra al Nos cuesta/Queda, igual que
        # técnico/despacho -- mismo resultado que pintaría la tarjeta OT.
        rep = _rep(costo=15000, bodega=15000)
        r = self.resultado(self._v(costo=100000, costo_proveedor=30000, costo_despacho=10000), rep)
        self.assertEqual(r["costo_repuestos"], 15000)
        self.assertEqual(r["costo_total"], 55000)
        self.assertEqual(r["margen_clp"], 45000)
        self.assertEqual(r["repuestos_desglose"]["bodega"], 15000)

    def test_repuestos_sin_costo_no_rompe_el_calculo(self):
        rep = _rep(costo=0, n_sin_costo=2)
        r = self.resultado(self._v(), rep)
        self.assertEqual(r["estado"], "ok")
        self.assertEqual(r["repuestos_sin_costo"], 2)
        self.assertEqual(r["costo_repuestos"], 0)

    def test_perdida_pura_es_rojo(self):
        r = self.resultado(self._v(costo=10000, costo_proveedor=30000, costo_despacho=10000), None)
        self.assertEqual(r["estado"], "ok")
        self.assertEqual(r["clase"], "rojo")
        self.assertLess(r["margen_clp"], 0)


class TestParseMontoClp(unittest.TestCase):
    """M2 (revisión Opus 2026-09-26 -- "parseo de montos CLP robusto:
    '15.000' = 15000, sin decimales raros"). Un float() a secas
    malinterpreta el punto de miles chileno ('15.000' -> 15.0)."""

    @classmethod
    def setUpClass(cls):
        cls.parse = staticmethod(_cargar_con_dependencias("_parse_monto_clp"))

    def test_punto_de_miles_chileno(self):
        self.assertEqual(self.parse("15.000"), 15000.0)

    def test_punto_de_miles_con_varios_grupos(self):
        self.assertEqual(self.parse("1.250.000"), 1250000.0)

    def test_numero_plano(self):
        self.assertEqual(self.parse("15000"), 15000.0)

    def test_cero_es_valido_no_none(self):
        # M2: 0 es un costo VALIDO (garantia de proveedor), no "vacio".
        self.assertEqual(self.parse("0"), 0.0)
        self.assertEqual(self.parse(0), 0.0)

    def test_vacio_o_none_es_none(self):
        self.assertIsNone(self.parse(""))
        self.assertIsNone(self.parse(None))
        self.assertIsNone(self.parse("   "))

    def test_negativo_es_none(self):
        self.assertIsNone(self.parse("-500"))

    def test_no_numerico_es_none(self):
        self.assertIsNone(self.parse("abc"))

    def test_simbolo_peso(self):
        self.assertEqual(self.parse("$15.000"), 15000.0)

    def test_decimal_real_con_1_o_2_digitos(self):
        self.assertEqual(self.parse("15.5"), 15.5)
        self.assertEqual(self.parse("15.50"), 15.5)


class TestVidaClienteEndpointGuards(unittest.TestCase):
    """El endpoint depende de Flask/MySQL -- se revisa por TEXTO FUENTE
    (ast.unparse, sin ejecutar nada) que los candados de la Etapa B
    siguen presentes, mismo criterio que test_repuestos_kardex.py."""

    @classmethod
    def setUpClass(cls):
        arbol = ast.parse(_leer(APP_PY))
        cls.fuente = None
        for nodo in arbol.body:
            if isinstance(nodo, ast.FunctionDef) and nodo.name == "mant_vida_cliente_api":
                cls.fuente = ast.unparse(nodo)
                break
        assert cls.fuente, "no se encontro mant_vida_cliente_api en app.py"

    def test_bloquea_tecnico_en_el_endpoint(self):
        self.assertIn("_es_rol_tecnico()", self.fuente)

    def test_no_confia_en_tk_tickets_cliente_id(self):
        # tk_tickets NO tiene cliente_id real -- la liga es vía
        # mant_ot_repuesto_solicitudes.ticket_id (que sí tiene cliente_id).
        self.assertNotIn("t.cliente_id", self.fuente)
        self.assertIn("s.ticket_id=t.id", self.fuente)

    def test_d3_liga_tickets_tambien_por_rut(self):
        self.assertIn("_rut_norm", self.fuente)

    def test_margen_por_ot_pagina_en_python(self):
        self.assertIn("mg_page", self.fuente)
        self.assertIn("mg_total_pages" if False else "mg_pages", self.fuente)

    def test_productos_pagina_con_limit_offset_real(self):
        self.assertIn("LIMIT %s OFFSET %s", self.fuente)

    def test_usa_la_funcion_compartida_de_resultado_financiero(self):
        # D2: Vida del cliente NO debe recalcular el margen por su cuenta
        # -- tiene que pasar por _ot_resultado_financiero, la MISMA que
        # usa la tarjeta de la OT.
        self.assertIn("_ot_resultado_financiero(", self.fuente)
        self.assertIn("_ot_repuestos_desglose(", self.fuente)

    def test_m8_solo_evita_recalcular_todo(self):
        self.assertIn("_calc_linea", self.fuente)
        self.assertIn("_calc_productos", self.fuente)

    def test_n_contrato_no_es_un_residual(self):
        # BAJA: antes n_contrato = veces_fuimos - cobradas - garantia (un
        # residual que se comía cualquier cubierto_por raro). Ahora tiene
        # que ser un conteo directo de cubierto_por='contrato'.
        self.assertIn("n_otros_cobro", self.fuente)


class TestOtResultadoFinancieroEndpointGuards(unittest.TestCase):
    """GET /ot/api/<vid>/resultado-financiero -- la tarjeta Finanzas de la
    OT (D2). Revisión por texto fuente, mismo criterio que arriba."""

    @classmethod
    def setUpClass(cls):
        arbol = ast.parse(_leer(APP_PY))
        cls.fuente = None
        for nodo in arbol.body:
            if isinstance(nodo, ast.FunctionDef) and nodo.name == "ot2_api_resultado_financiero":
                cls.fuente = ast.unparse(nodo)
                break
        assert cls.fuente, "no se encontro ot2_api_resultado_financiero en app.py"

    def test_bloquea_tecnico(self):
        self.assertIn("_es_rol_tecnico()", self.fuente)

    def test_usa_la_funcion_compartida(self):
        self.assertIn("_ot_resultado_financiero(", self.fuente)
        self.assertIn("_ot_repuestos_desglose(", self.fuente)

    def test_no_toca_el_gate_de_cierre(self):
        # D2: los repuestos son informativos, NUNCA bloquean el cierre --
        # este endpoint no debe llamar al gate real de la OT.
        self.assertNotIn("_ot2_finanzas_estado(", self.fuente)


class TestOtRepuestosDesgloseM4(unittest.TestCase):
    """M4: una sola regla de atribución del repuesto a la OT en TODO el
    proyecto -- COALESCE(ot_generada_id, visita_id), nunca
    `visita_id=%s OR ot_generada_id=%s` (que podía atribuir el mismo
    repuesto a dos OT a la vez)."""

    @classmethod
    def setUpClass(cls):
        arbol = ast.parse(_leer(APP_PY))
        cls.fuente_desglose = cls.fuente_endpoint = None
        for nodo in arbol.body:
            if isinstance(nodo, ast.FunctionDef) and nodo.name == "_ot_repuestos_desglose":
                cls.fuente_desglose = ast.unparse(nodo)
            if isinstance(nodo, ast.FunctionDef) and nodo.name == "ot2_api_repuestos_costo":
                cls.fuente_endpoint = ast.unparse(nodo)
        assert cls.fuente_desglose, "no se encontro _ot_repuestos_desglose en app.py"
        assert cls.fuente_endpoint, "no se encontro ot2_api_repuestos_costo en app.py"

    def test_regla_unica_de_atribucion(self):
        self.assertIn("COALESCE(ot_generada_id, visita_id)", self.fuente_desglose)

    def test_solo_cuenta_instalado(self):
        self.assertIn("estado='instalado'", self.fuente_desglose)

    def test_endpoint_viejo_ya_no_usa_or_visita_ot(self):
        self.assertNotIn("visita_id=%s OR ot_generada_id=%s", self.fuente_endpoint)
        self.assertIn("_ot_repuestos_desglose(", self.fuente_endpoint)


if __name__ == "__main__":
    unittest.main()

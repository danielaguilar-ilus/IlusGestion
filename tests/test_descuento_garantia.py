"""Candado "solo superadmin puede aplicar descuentos por garantía".

Daniel (2026-09-26): "en las cotizaciones solo yo puedo aplicar descuentos
de garantías". Hoy cualquier usuario que edita una cotización puede dejar
la cabecera o una línea a costo $0 sin ningún candado.

El único caso INEQUÍVOCO de "descuento por garantía" en el modelo de datos
actual (no existe un campo/motivo "garantía" separado del comercial) es un
descuento que deja el total en $0 con subtotal > 0:
  · cabecera en modo '%' al 100%,
  · cabecera en modo '$' (monto fijo) que iguala o supera el subtotal,
  · un ítem con precio_manual=0 (nunca un 'accesorio': ese es $0 por
    diseño de categoría, no por descuento).

Este archivo:
  1. Extrae y EJECUTA DE VERDAD (sin Flask ni MySQL) las funciones puras
     de tickets_module.py que decidan la clave/detección de un ítem en
     garantía (_tk_item_clave_garantia, _tk_item_precio_manual_cero,
     _tk_cotiz_clase_no_cobrable) usando el mismo patrón de
     tests/test_avisar_courier.py (ast.parse + exec del FunctionDef
     aislado) -- no hay entorno de test con DB en este proyecto.
  2. Verifica, con esas funciones reales, los 3 casos que pidió la tarea:
     superadmin sí / no-superadmin no / edición sin tocar un descuento ya
     existente sí -- tanto para el candado de ítem (precio_manual=0) como
     para la decisión de cabecera (100% o monto ≥ subtotal), esta última
     reproducida con la MISMA condición que quedó en el código (verificada
     además por inspección de fuente en la clase TestElCodigoFuente).

Correr con:  py -m unittest tests.test_descuento_garantia -v
(pytest NO está instalado en el equipo de Daniel.)
"""
import ast
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TICKETS_MODULE_PY = os.path.join(RAIZ, "tickets_module.py")

_FUENTE = None
_ARBOL = None


def _fuente():
    global _FUENTE
    if _FUENTE is None:
        with open(TICKETS_MODULE_PY, encoding="utf-8") as fh:
            _FUENTE = fh.read()
    return _FUENTE


def _arbol():
    global _ARBOL
    if _ARBOL is None:
        _ARBOL = ast.parse(_fuente())
    return _ARBOL


def _nodo(nombre):
    """Busca un FunctionDef por nombre en TODO el árbol (incluye funciones
    nesteadas dentro de register_tickets_routes, que es como vive la
    mayoría de la lógica de Cotizaciones en este archivo)."""
    for n in ast.walk(_arbol()):
        if isinstance(n, ast.FunctionDef) and n.name == nombre:
            return n
    raise AssertionError("no existe la función %s() en tickets_module.py" % nombre)


def _cuerpo(nombre):
    return ast.unparse(_nodo(nombre))


def _ejecutable(nombre, ambito=None):
    """Extrae y ejecuta SOLO ese FunctionDef en un namespace aislado --
    misma técnica que tests/test_avisar_courier.py -- para probar la
    función real sin necesitar Flask/MySQL."""
    ns = dict(ambito or {})
    exec(compile(ast.Module(body=[_nodo(nombre)], type_ignores=[]),
                 "<tickets_module.py>", "exec"), ns)
    return ns[nombre]


def _constante(nombre):
    """Lee una constante module-level (frozenset/dict/etc) evaluando su
    asignación real en el árbol -- para no hardcodear en el test un valor
    que en tickets_module.py podría cambiar (ej. se agrega otra clase
    'no cobrable'). ast.literal_eval no entiende `frozenset({...})` (no es
    un literal), así que se compila y evalúa la expresión real con los
    builtins normales -- sigue sin ejecutar nada del resto del archivo."""
    for n in ast.walk(_arbol()):
        if (isinstance(n, ast.Assign) and len(n.targets) == 1
                and isinstance(n.targets[0], ast.Name) and n.targets[0].id == nombre):
            expr = ast.Expression(body=n.value)
            ast.fix_missing_locations(expr)
            return eval(compile(expr, "<tickets_module.py>", "eval"))
    raise AssertionError("no existe la constante %s en tickets_module.py" % nombre)


# Dependencias reales de _tk_item_precio_manual_cero -- función y
# constante module-level, se extraen y ejecutan igual (sin closures que
# resolver).
_TK_COTIZ_CLASES_NO_COBRABLES = _constante("_TK_COTIZ_CLASES_NO_COBRABLES")
_tk_cotiz_clase_no_cobrable = _ejecutable(
    "_tk_cotiz_clase_no_cobrable", {"_TK_COTIZ_CLASES_NO_COBRABLES": _TK_COTIZ_CLASES_NO_COBRABLES})
_tk_item_clave_garantia = _ejecutable("_tk_item_clave_garantia")
_tk_item_precio_manual_cero = _ejecutable(
    "_tk_item_precio_manual_cero", {"_tk_cotiz_clase_no_cobrable": _tk_cotiz_clase_no_cobrable})


# ══════════════════════════════════════════════════════════════════════
#  1. Las funciones puras reales, ejecutadas de verdad
# ══════════════════════════════════════════════════════════════════════
class TestFuncionesPurasReales(unittest.TestCase):

    def test_accesorio_nunca_cuenta_como_descuento_de_garantia(self):
        self.assertFalse(_tk_item_precio_manual_cero(
            {"precio_manual": 0}, "accesorio"))

    def test_item_cobrable_a_precio_manual_cero_si_cuenta(self):
        self.assertTrue(_tk_item_precio_manual_cero(
            {"precio_manual": 0}, "motor"))

    def test_precio_manual_none_o_vacio_no_es_garantia(self):
        self.assertFalse(_tk_item_precio_manual_cero({"precio_manual": None}, "motor"))
        self.assertFalse(_tk_item_precio_manual_cero({"precio_manual": ""}, "motor"))
        self.assertFalse(_tk_item_precio_manual_cero({}, "motor"))

    def test_precio_manual_no_numerico_no_revienta(self):
        self.assertFalse(_tk_item_precio_manual_cero({"precio_manual": "n/a"}, "motor"))

    def test_clave_prioriza_sku_mas_documento_erp(self):
        self.assertEqual(
            _tk_item_clave_garantia("sku-1", "33", "1234", "Motor"),
            ("doc", "SKU-1", "33", "1234"))

    def test_clave_cae_a_sku_mas_descripcion_sin_documento(self):
        self.assertEqual(
            _tk_item_clave_garantia("sku-1", None, None, "Motor Grande"),
            ("simple", "SKU-1", "motor grande"))

    def test_mismo_item_misma_clave_pese_a_mayusculas_y_espacios(self):
        a = _tk_item_clave_garantia(" sku-1 ", " 33 ", " 1234 ", " Motor ")
        b = _tk_item_clave_garantia("SKU-1", "33", "1234", "motor")
        self.assertEqual(a, b)


# ══════════════════════════════════════════════════════════════════════
#  2. La decisión de candado (superadmin sí / no-superadmin no / edición
#     sin tocar un descuento existente sí) -- ítem, con las funciones
#     REALES de arriba.
# ══════════════════════════════════════════════════════════════════════
def item_bloqueado(items_payload, clases_por_sku, items_antes, es_superadmin):
    """Reproduce la decisión de tk_api_cotizacion_actualizar (y, con
    items_antes=[], la de tk_api_cotizacion_desde_erp) usando las
    funciones REALES extraídas arriba."""
    pm_cero_antes = set()
    for itp in items_antes:
        if _tk_item_precio_manual_cero(itp, itp.get("clase_producto")):
            pm_cero_antes.add(_tk_item_clave_garantia(
                itp.get("erp_kopr"), itp.get("erp_tido"), itp.get("erp_nudo"), itp.get("descripcion")))
    if es_superadmin:
        return False
    for it in items_payload:
        sku = (it.get("sku") or "").strip()
        clase = it.get("clase_producto") or clases_por_sku.get(sku.upper())
        if not _tk_item_precio_manual_cero(it, clase):
            continue
        clave = _tk_item_clave_garantia(sku, it.get("tido"), it.get("nudo"), it.get("nombre"))
        if clave not in pm_cero_antes:
            return True
    return False


class TestCandadoItem(unittest.TestCase):

    def test_crear_no_superadmin_con_item_a_cero_bloqueado(self):
        items = [{"sku": "SKU-1", "nombre": "Motor", "precio_manual": 0, "clase_producto": "motor"}]
        self.assertTrue(item_bloqueado(items, {}, [], es_superadmin=False))

    def test_crear_superadmin_con_item_a_cero_permitido(self):
        items = [{"sku": "SKU-1", "nombre": "Motor", "precio_manual": 0, "clase_producto": "motor"}]
        self.assertFalse(item_bloqueado(items, {}, [], es_superadmin=True))

    def test_crear_accesorio_a_cero_nunca_bloquea(self):
        items = [{"sku": "SKU-2", "nombre": "Tapa", "precio_manual": None, "clase_producto": "accesorio"}]
        self.assertFalse(item_bloqueado(items, {}, [], es_superadmin=False))

    def test_actualizar_no_superadmin_sin_tocar_item_ya_en_cero_permitido(self):
        antes = [{"erp_kopr": "SKU-1", "erp_tido": None, "erp_nudo": None,
                  "descripcion": "Motor", "clase_producto": "motor", "precio_manual": 0}]
        items = [{"sku": "SKU-1", "nombre": "Motor", "precio_manual": 0, "clase_producto": "motor"}]
        self.assertFalse(item_bloqueado(items, {}, antes, es_superadmin=False))

    def test_actualizar_no_superadmin_agrega_segundo_item_a_cero_bloqueado(self):
        antes = [{"erp_kopr": "SKU-1", "erp_tido": None, "erp_nudo": None,
                  "descripcion": "Motor", "clase_producto": "motor", "precio_manual": 0}]
        items = [
            {"sku": "SKU-1", "nombre": "Motor", "precio_manual": 0, "clase_producto": "motor"},
            {"sku": "SKU-9", "nombre": "Correa", "precio_manual": 0, "clase_producto": "correa"},
        ]
        self.assertTrue(item_bloqueado(items, {}, antes, es_superadmin=False))

    def test_actualizar_superadmin_agrega_segundo_item_a_cero_permitido(self):
        antes = [{"erp_kopr": "SKU-1", "erp_tido": None, "erp_nudo": None,
                  "descripcion": "Motor", "clase_producto": "motor", "precio_manual": 0}]
        items = [
            {"sku": "SKU-1", "nombre": "Motor", "precio_manual": 0, "clase_producto": "motor"},
            {"sku": "SKU-9", "nombre": "Correa", "precio_manual": 0, "clase_producto": "correa"},
        ]
        self.assertFalse(item_bloqueado(items, {}, antes, es_superadmin=True))


# ══════════════════════════════════════════════════════════════════════
#  3. Cabecera: 100% y monto fijo ≥ subtotal.
#     _tk_cotiz_preview_subtotal necesita _tk_cotiz_pricing_config()
#     (lee tk_settings de MySQL) y el catálogo de tarifas -- no es
#     ejecutable sin DB. Se prueba entonces la MISMA condición de decisión
#     que usa el endpoint (descuento_monto_in >= preview_subtotal > 0),
#     parametrizada con un subtotal ya conocido, Y se verifica por
#     inspección de fuente (TestElCodigoFuente) que el endpoint real
#     contiene exactamente esa condición -- mismo patrón que
#     tests/test_avisar_courier.py.
# ══════════════════════════════════════════════════════════════════════
def header_bloqueado_pct(descuento_tipo, descuento_pct, antes_full, es_superadmin):
    return descuento_tipo == "pct" and descuento_pct >= 100 and not antes_full and not es_superadmin


def header_bloqueado_monto(descuento_tipo, descuento_monto, preview_subtotal, antes_full_monto, es_superadmin):
    if es_superadmin or antes_full_monto:
        return False
    return descuento_tipo == "monto" and preview_subtotal > 0 and descuento_monto >= preview_subtotal


class TestCandadoCabecera(unittest.TestCase):

    def test_pct_100_nuevo_no_superadmin_bloqueado(self):
        self.assertTrue(header_bloqueado_pct("pct", 100.0, antes_full=False, es_superadmin=False))

    def test_pct_100_superadmin_permitido(self):
        self.assertFalse(header_bloqueado_pct("pct", 100.0, antes_full=False, es_superadmin=True))

    def test_pct_100_ya_existia_no_bloquea_edicion_de_no_superadmin(self):
        self.assertFalse(header_bloqueado_pct("pct", 100.0, antes_full=True, es_superadmin=False))

    def test_pct_parcial_comercial_nunca_bloquea(self):
        self.assertFalse(header_bloqueado_pct("pct", 20.0, antes_full=False, es_superadmin=False))

    def test_monto_iguala_subtotal_nuevo_no_superadmin_bloqueado(self):
        self.assertTrue(header_bloqueado_monto("monto", 50000, 50000, antes_full_monto=False, es_superadmin=False))

    def test_monto_supera_subtotal_nuevo_no_superadmin_bloqueado(self):
        self.assertTrue(header_bloqueado_monto("monto", 60000, 50000, antes_full_monto=False, es_superadmin=False))

    def test_monto_superadmin_permitido(self):
        self.assertFalse(header_bloqueado_monto("monto", 50000, 50000, antes_full_monto=False, es_superadmin=True))

    def test_monto_ya_cubria_todo_antes_no_bloquea_edicion_de_no_superadmin(self):
        self.assertFalse(header_bloqueado_monto("monto", 50000, 50000, antes_full_monto=True, es_superadmin=False))

    def test_monto_parcial_comercial_nunca_bloquea(self):
        # Un monto que deja subtotal > 0 restante (descuento normal, ej.
        # $10.000 de $50.000) sigue siendo comercial, no garantía.
        self.assertFalse(header_bloqueado_monto("monto", 10000, 50000, antes_full_monto=False, es_superadmin=False))

    def test_subtotal_cero_no_dispara_el_candado(self):
        # Cotización sin ítems clasificados/tarifados todavía -- subtotal 0,
        # no hay "garantía" que proteger (no hay nada que costear).
        self.assertFalse(header_bloqueado_monto("monto", 0, 0, antes_full_monto=False, es_superadmin=False))


# ══════════════════════════════════════════════════════════════════════
#  4. El código real contiene el candado (inspección de fuente, mismo
#     patrón que TestElEndpointEsHonesto en test_avisar_courier.py) --
#     para que un refactor futuro que borre la validación sin querer
#     rompa ESTE test, no solo el de arriba (que prueba la lógica
#     reproducida, no el archivo).
# ══════════════════════════════════════════════════════════════════════
class TestElCodigoFuente(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.crear = _cuerpo("tk_api_cotizacion_desde_erp")
        cls.actualizar = _cuerpo("tk_api_cotizacion_actualizar")

    def test_mensaje_de_error_es_el_mismo_en_ambos_endpoints(self):
        msg = "Solo el superadmin puede aplicar descuentos por garantía."
        self.assertIn(msg, self.crear)
        self.assertIn(msg, self.actualizar)

    def test_candado_pct_100_en_creacion(self):
        # ast.unparse normaliza los literales de string a comillas simples.
        self.assertIn("descuento_tipo == 'pct' and descuento_pct >= 100", self.crear)

    def test_candado_monto_en_creacion_usa_preview_subtotal(self):
        self.assertIn("_tk_cotiz_preview_subtotal(", self.crear)
        self.assertIn("descuento_monto_in >= _preview_subtotal", self.crear)
        self.assertIn("_preview_subtotal > 0", self.crear)

    def test_candado_monto_en_edicion_compara_contra_el_antes(self):
        self.assertIn("_antes_full_header_monto", self.actualizar)
        self.assertIn("_tk_cotiz_preview_subtotal(", self.actualizar)
        self.assertIn("descuento_monto_in >= _preview_subtotal", self.actualizar)

    def test_edicion_no_bloquea_si_el_100_ya_existia(self):
        self.assertIn("_antes_full_header", self.actualizar)
        self.assertIn("not _antes_full_header", self.actualizar)

    def test_auditoria_registra_quien_cuando_monto(self):
        # ast.unparse normaliza a comillas simples -- se busca la palabra,
        # no el literal con comillas exactas.
        self.assertIn("descuento_garantia", self.crear)
        self.assertIn("descuento_garantia", self.actualizar)
        self.assertIn("descuento_monto_cabecera", self.actualizar)


if __name__ == "__main__":
    unittest.main(verbosity=2)

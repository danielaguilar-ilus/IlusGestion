"""El nº de documento se muestra SIN los ceros de relleno del ERP.

Pedido de Daniel el 2026-08-17, con foto de una etiqueta impresa que decía
"0000023102": "¿es posible eliminar todos esos ceros de la etiqueta?".

El relleno a 10 caracteres es del formato interno del ERP Random. Nadie lo
dice en voz alta y, más importante, el despachador de Felca busca "23102" en
SimpliRoute mientras ILUS le mandaba "0000023102" — el mismo pedido con dos
nombres distintos (mismo problema ya documentado en _sr_normalizar_reference,
caso BLV 22890).

Los tres canales tienen que decir lo MISMO, porque son la misma caja:
  1. la etiqueta impresa (incluido su código de barras),
  2. el Excel de carga masiva que se sube al panel de SimpliRoute,
  3. la conexión directa (API) que crea las visitas.

⚠️ Lo que NO cambia y estos tests protegen: el valor guardado en
transport_commitments.nudo sigue con relleno. Es parte de la UNIQUE KEY
uq_doc(tido, nudo) y perder el relleno ya generó documentos duplicados antes.
Esto es SOLO presentación.

Correr con:  py -m unittest tests.test_etiqueta_sin_ceros
(pytest NO está instalado en el equipo de Daniel.)
"""
import ast
import os
import re
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
APP_PY = os.path.join(RAIZ, "app.py")
TPL = os.path.join(RAIZ, "templates", "transporte", "etiquetas.html")


def _texto_app():
    with open(APP_PY, encoding="utf-8") as fh:
        return fh.read()


def _nodo_de(nombre_funcion):
    """Nodo AST de una función de app.py, sin importar app.py.

    Importar app.py levanta Flask, la base y los hilos de cron: para
    verificar QUÉ hace una función alcanza con leer su árbol sintáctico.
    """
    arbol = ast.parse(_texto_app())
    for nodo in ast.walk(arbol):
        if isinstance(nodo, ast.FunctionDef) and nodo.name == nombre_funcion:
            return nodo
    raise AssertionError(f"no existe la función {nombre_funcion} en app.py")


def _fuente_de(nombre_funcion):
    """Código normalizado de una función de app.py."""
    return ast.unparse(_nodo_de(nombre_funcion))


def _ejecutar_funcion_de_app(nombre_funcion):
    """Extrae UNA función de app.py y la devuelve ejecutable.

    Sirve para las funciones puras (sin Flask, sin base): se comprueba su
    COMPORTAMIENTO real en vez de buscar texto en el código, que es frágil —
    una prueba que busca "lstrip" falla cuando alguien reescribe lo mismo
    con int(), aunque el comportamiento sea idéntico.
    """
    ambito = {}
    exec(compile(ast.Module(body=[_nodo_de(nombre_funcion)], type_ignores=[]),
                 "<app.py-extraido>", "exec"), ambito)
    return ambito[nombre_funcion]


# ══════════════════════════════════════════════════════════════════════
#  1. Canal etiqueta — se limpia en el ORIGEN, no en la plantilla
# ══════════════════════════════════════════════════════════════════════
class TestEtiquetaSeLimpiaEnElOrigen(unittest.TestCase):
    """Por qué en el origen y no con un filtro en la plantilla:

    doc_numero alimenta a la vez el número impreso, el data-code del código
    de barras y los data-doc que usa el filtro de rangos de bultos
    ("23102:1,3,5-8"). Si se limpiara solo la parte visible, el filtro
    compararía "23102" contra data-doc="0000023102", devolvería cero
    resultados y mostraría un aviso VERDE de éxito. Limpiando el origen,
    los dos lados cambian juntos.
    """

    @classmethod
    def setUpClass(cls):
        cls.src = _fuente_de("_tr_etiqueta_facturas")

    def test_doc_numero_pasa_por_el_helper_de_limpieza(self):
        self.assertIn("'doc_numero': nudo_fmt_filter(c.get('nudo'))", self.src)

    def test_doc_full_usa_doc_label(self):
        self.assertIn("doc = _doc_label(c.get('tido'), c.get('nudo'))", self.src)

    def test_ya_no_queda_el_nudo_crudo_como_doc_numero(self):
        # La forma vieja: "doc_numero": (c.get("nudo") or "").strip()
        self.assertNotIn("'doc_numero': (c.get('nudo') or '').strip()", self.src)

    def test_el_valor_crudo_del_erp_sigue_disponible(self):
        # No se pierde información: quien necesite el valor exacto de la base
        # lo tiene sin volver a consultarla. REGLA #4.2 — se agrega, no se quita.
        self.assertIn("'doc_numero_erp': (c.get('nudo') or '').strip()", self.src)


class TestPlantillaNoNecesitoCambios(unittest.TestCase):
    """La plantilla sigue pintando doc_numero tal cual llega.

    Si alguien "arregla" esto metiendo | nudo_fmt en la plantilla, el valor
    quedaría limpio dos veces (inofensivo) PERO invita a limpiar solo la
    parte visible y desincronizar data-doc. Este test documenta la decisión.
    """

    @classmethod
    def setUpClass(cls):
        with open(TPL, encoding="utf-8") as fh:
            cls.html = fh.read()

    def test_el_codigo_de_barras_usa_el_mismo_valor_que_el_texto(self):
        # data-code (lo que se codifica) y el texto impreso salen del MISMO
        # doc_numero. Que el humano lea un número y el lector emita otro sería
        # imposible de depurar después.
        self.assertIn('data-code="{{ f.doc_numero }}"', self.html)

    def test_data_doc_sale_del_mismo_doc_numero_que_el_filtro_de_rangos(self):
        # El filtro de rangos compara contra data-doc como texto exacto.
        self.assertIn('data-doc="{{ f.doc_numero }}"', self.html)
        # Y el placeholder que le dice al usuario qué escribir sale de lo mismo.
        self.assertIn("{{ facturas[0].doc_numero }}", self.html)

    def test_nadie_metio_un_filtro_a_medias_en_la_plantilla(self):
        # Si aparece "| nudo_fmt" en la plantilla, alguien está limpiando en
        # dos capas: revisar que data-doc y data-code hayan cambiado también.
        self.assertNotIn("doc_numero | nudo_fmt", self.html)
        self.assertNotIn("doc_numero|nudo_fmt", self.html)


# ══════════════════════════════════════════════════════════════════════
#  2. Canal conexión directa (API) — módulo puro, se prueba de verdad
# ══════════════════════════════════════════════════════════════════════
class TestPayloadSimpliRouteSinCeros(unittest.TestCase):

    ITEM = {
        "item_id": 1, "tido": "BLV", "nudo": "0000023102",
        "cliente_nombre": "Pedro Pablo Ormazabal Vergara",
        "direccion": "San Nicolás", "comuna": "Santiago",
        "region": "Región Metropolitana",
        "telefono": "+56977555464", "n_bultos": 1,
    }

    def _payload(self, **cambios):
        from simpliroute_client import build_visit_payload
        item = dict(self.ITEM, **cambios)
        p, errores = build_visit_payload(item, planned_date="2026-08-17")
        self.assertEqual(errores, [], f"la visita no debería tener errores: {errores}")
        return p

    def test_el_titulo_va_sin_ceros(self):
        # Es lo que el despachador de Felca ve en la lista de visitas.
        self.assertEqual(
            self._payload()["title"],
            "23102 - Pedro Pablo Ormazabal Vergara")

    def test_la_referencia_va_sin_ceros_pero_conserva_el_tipo(self):
        # El tido se mantiene: FCV 100 y BLV 100 son documentos distintos.
        self.assertEqual(self._payload()["reference"], "BLV-23102")

    def test_un_nudo_que_ya_venia_limpio_no_se_altera(self):
        p = self._payload(nudo="8149", tido="FCV")
        self.assertEqual(p["reference"], "FCV-8149")
        self.assertTrue(p["title"].startswith("8149 - "))

    def test_un_documento_de_puros_ceros_no_queda_vacio(self):
        # Un título vacío hace que SimpliRoute rechace la visita entera.
        # Preferimos mandar algo raro antes que perder el despacho.
        p = self._payload(nudo="0000000000")
        self.assertTrue(p["title"].startswith("0000000000 - "))
        self.assertEqual(p["reference"], "BLV-0000000000")

    def test_nudo_vacio_no_revienta(self):
        from simpliroute_client import build_visit_payload
        item = dict(self.ITEM, nudo="")
        p, errores = build_visit_payload(item, planned_date="2026-08-17")
        # Sin nudo el título sigue siendo válido gracias al nombre del cliente.
        self.assertEqual(errores, [])
        self.assertEqual(p["title"], "Pedro Pablo Ormazabal Vergara")

    def test_la_conciliacion_sigue_reconociendo_las_visitas_viejas(self):
        """El riesgo real de cambiar lo que se manda: las visitas creadas
        ANTES de hoy quedaron en SimpliRoute con la referencia rellena. Si la
        conciliación dejara de reconocerlas, un despacho ya entregado en la
        calle quedaría congelado en ILUS (eso pasó de verdad en agosto: 30
        envíos congelados 4 días, 9 ya entregados).

        No se comprueba leyendo el código: se EJECUTA la función real.
        """
        normalizar = _ejecutar_funcion_de_app("_sr_normalizar_reference")

        vieja = normalizar("BLV-0000023102")   # lo que se subió hasta ayer
        nueva = normalizar("BLV-23102")        # lo que se sube desde hoy
        self.assertEqual(vieja, nueva,
                         "la visita vieja y la nueva tienen que ser el mismo pedido")

        # Y el formato del Excel (nudo pelado, sin tipo) también converge.
        self.assertEqual(normalizar("0000023102"), normalizar("23102"))

    def test_la_conciliacion_no_confunde_documentos_distintos(self):
        # Contraparte del test anterior: que normalice de más sería peor que
        # normalizar de menos (dos pedidos distintos tratados como uno).
        normalizar = _ejecutar_funcion_de_app("_sr_normalizar_reference")
        self.assertNotEqual(normalizar("BLV-23102"), normalizar("FCV-23102"))
        self.assertNotEqual(normalizar("BLV-23102"), normalizar("BLV-23103"))


# ══════════════════════════════════════════════════════════════════════
#  3. Canal Excel de carga masiva — lo que se sube a mano al panel
# ══════════════════════════════════════════════════════════════════════
class TestExcelCargaMasivaSinCeros(unittest.TestCase):
    """El Excel "Felca y Milling" es el otro camino hacia SimpliRoute.

    Si solo se arreglara la conexión directa, quien exporte el archivo
    seguiría subiendo "0000023102" y el problema volvería por la otra puerta.
    """

    @classmethod
    def setUpClass(cls):
        texto = _texto_app()
        i = texto.find('ws.title = "Felca y Milling"')
        assert i > 0, "no se encontró el bloque del Excel de SimplyRoute"
        cls.bloque = texto[i:i + 2200]

    def test_el_nudo_del_excel_pasa_por_el_helper(self):
        self.assertIn("nudo   = nudo_fmt_filter(it.get(\"nudo\"))", self.bloque)

    def test_ya_no_toma_el_nudo_crudo(self):
        self.assertNotIn('nudo   = it.get("nudo") or ""', self.bloque)

    def test_el_titulo_y_la_referencia_salen_del_mismo_nudo_limpio(self):
        # titulo usa {nudo}; la columna "Id de referencia" usa nudo directo.
        self.assertIn('titulo = (f"{nudo} - {nombre}")', self.bloque)
        self.assertIn("nudo, comuna, peso, nombre", self.bloque)


# ══════════════════════════════════════════════════════════════════════
#  4. El buscador interno tiene que encontrar lo que la etiqueta muestra
# ══════════════════════════════════════════════════════════════════════
class TestBuscadorAceptaLasDosFormas(unittest.TestCase):
    """Sin esto, la primera persona que lea "23102" en la caja y lo teclee en
    /transporte/buscar recibe "sin resultados" para un documento que existe.
    """

    @classmethod
    def setUpClass(cls):
        cls.src = _fuente_de("tr_buscar_interno")

    def test_acepta_el_numero_rellenado(self):
        self.assertIn("LPAD(%s, 10, '0')", self.src)

    def test_acepta_comparando_ambos_lados_sin_ceros(self):
        self.assertIn("TRIM(LEADING '0' FROM nudo)", self.src)

    def test_no_se_quito_la_busqueda_exacta_original(self):
        # REGLA #4.2: se amplía, no se reemplaza.
        self.assertIn("WHERE nudo = %s", self.src)

    def test_no_se_quitaron_las_busquedas_por_rut_ni_por_nombre(self):
        self.assertIn("cliente_rut", self.src)
        self.assertIn("cliente_nombre", self.src)

    def test_sigue_parametrizado_sin_concatenar(self):
        # REGLA #4: nunca f-strings con entrada del usuario dentro del SQL.
        sql = re.search(r"mysql_fetchall\((.*?)\)\s*or \[\]", self.src, re.S)
        self.assertIsNotNone(sql, "no se encontró la consulta")
        self.assertNotIn("f'''", sql.group(1))
        self.assertNotIn('f"""', sql.group(1))


# ══════════════════════════════════════════════════════════════════════
#  5. Lo que NO debe cambiar
# ══════════════════════════════════════════════════════════════════════
class TestElValorGuardadoNoSeToca(unittest.TestCase):
    """La limpieza es de presentación. Si alguien la aplica al guardar,
    rompe la UNIQUE KEY uq_doc(tido, nudo) y reaparecen los documentos
    duplicados que costó fusionar (71 grupos, 2026-08)."""

    def test_el_sync_del_erp_no_pela_ceros_al_guardar(self):
        src = _fuente_de("_tr_etiqueta_facturas")
        # _tr_etiqueta_facturas solo LEE (SELECT). Que no aparezca ningún
        # INSERT/UPDATE es la garantía de que esto no toca lo guardado.
        self.assertNotIn("INSERT INTO transport_commitments", src)
        self.assertNotIn("UPDATE transport_commitments", src)


if __name__ == "__main__":
    unittest.main(verbosity=2)

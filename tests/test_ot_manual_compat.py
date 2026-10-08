"""Compatibilidad real + Manual del equipo en el modal "Solicitar repuestos" de la OT
(Daniel, 2026-10-01).

Pedido: "acercarnos más a la compatibilidad real. Deja el Buscar en bodega, deja el
escribir manual. Y que el técnico tenga acceso al MANUAL del equipo (lo sube Juan en el
Catálogo), relacionado con las piolas, para que si el repuesto no está, lo busque en el
manual". En la OT el manual se puede SOLO VER.

Qué se prueba (sin BD ni Flask: funciones de app.py extraídas con ast, con una "BD" falsa,
más revisiones del código fuente y de la plantilla):
  - normalización de nombres/SKU y resolución de TODOS los modelos que representan un equipo;
  - palabras distintivas de un modelo y su calce ("Trotadora ILUS X1" calza con "Motor de
    inclinación trotadora x1" y NO con una "Escaladora ILUS X1");
  - que los manuales nunca expongan gcs_key ni uploaded_by y usen la URL propia de la OT;
  - que las rutas nuevas tengan sus decoradores y candados;
  - la rama "modelo escrito a mano" (no duplica modelos del ERP) y los textos de la UI.

Correr con:  py -m unittest tests.test_ot_manual_compat
"""
import os
import re
import shutil
import subprocess
import sys
import tempfile
import unicodedata
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tests.test_incidencias_bajas import _cargar, _decoradores  # noqa: E402
from tests.test_incidencias_repuesto_tercera_fuente import _fuente_de  # noqa: E402

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DETALLE = os.path.join(RAIZ, "templates", "ot2", "detalle.html")
BUSCADOR_JS = os.path.join(RAIZ, "static", "repuestos_buscador.js")


def _leer(ruta):
    with open(ruta, encoding="utf-8") as fh:
        return fh.read()


class _BDFalsa:
    """mysql_fetchall de mentira: responde según un fragmento del SQL y guarda cada llamada."""

    def __init__(self, respuestas):
        self.respuestas = respuestas
        self.llamadas = []

    def __call__(self, sql, params=None):
        self.llamadas.append((sql, params))
        for fragmento, filas in self.respuestas:
            if fragmento in sql:
                if isinstance(filas, Exception):
                    raise filas
                return filas
        return []


# ─────────────────────────────────────────────────────────────────────────────
class TestNormalizacion(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        amb = _cargar(["_otrep_norm_nombre", "_otrep_norm_sku"], extra={"re": re})
        cls.nombre = staticmethod(amb["_otrep_norm_nombre"])
        cls.sku = staticmethod(amb["_otrep_norm_sku"])

    def test_el_nombre_colapsa_espacios_y_recorta(self):
        self.assertEqual(self.nombre("  Trotadora   ILUS\tX1 \n"), "Trotadora ILUS X1")

    def test_el_nombre_no_toca_mayusculas_ni_acentos(self):
        # Eso lo resuelve la collation utf8mb4_0900_ai_ci en SQL, no Python.
        self.assertEqual(self.nombre("Elíptica ILUS PRO"), "Elíptica ILUS PRO")

    def test_nombre_vacio_o_none(self):
        self.assertEqual(self.nombre(None), "")
        self.assertEqual(self.nombre("   "), "")

    def test_el_sku_va_en_mayusculas_y_sin_espacios(self):
        self.assertEqual(self.sku(" abc 123 "), "ABC123")
        self.assertEqual(self.sku("1010 0897 01"), "1010089701")
        self.assertEqual(self.sku(None), "")


# ─────────────────────────────────────────────────────────────────────────────
class TestPalabrasDistintivas(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        amb = _cargar(
            ["_otrep_palabras_de_texto", "_otrep_palabras_distintivas", "_otrep_calza_palabras"],
            ["_OTREP_PALABRAS_IGNORADAS"], extra={"re": re, "unicodedata": unicodedata})
        cls.dist = staticmethod(amb["_otrep_palabras_distintivas"])
        cls.calza = staticmethod(amb["_otrep_calza_palabras"])
        cls.ignoradas = amb["_OTREP_PALABRAS_IGNORADAS"]

    def test_trotadora_ilus_x1(self):
        self.assertEqual(self.dist("Trotadora ILUS X1"), ["trotadora", "x1"])

    def test_se_ignoran_la_marca_y_los_conectores(self):
        for palabra in ("ilus", "fitness", "de", "del", "la", "el", "los", "las", "para", "con", "y", "en"):
            self.assertIn(palabra, self.ignoradas)
        self.assertEqual(self.dist("Bicicleta de Spinning para el Gimnasio ILUS Fitness"),
                         ["bicicleta", "spinning", "gimnasio"])

    def test_sin_acentos_en_minusculas_y_sin_repetir(self):
        self.assertEqual(self.dist("Elíptica Elíptica ILUS PRO"), ["eliptica", "pro"])

    def test_tokens_de_un_caracter_no_cuentan(self):
        self.assertEqual(self.dist("Trotadora ILUS X 1"), ["trotadora"])

    def test_el_motor_de_la_trotadora_x1_calza(self):
        pal = self.dist("Trotadora ILUS X1")
        self.assertTrue(self.calza("Motor de inclinación trotadora x1", pal))
        self.assertTrue(self.calza("MOTOR INCLINACION TROTADORA ILUS X1", pal))

    def test_un_repuesto_de_la_escaladora_x1_no_calza_con_la_trotadora(self):
        pal = self.dist("Trotadora ILUS X1")
        self.assertFalse(self.calza("Pantalla Escaladora ILUS X1", pal))
        self.assertFalse(self.calza("Perno de la Escaladora X1", pal))
        # y al revés: lo de la trotadora no es de la escaladora
        self.assertFalse(self.calza("Motor de inclinación trotadora x1", self.dist("Escaladora ILUS X1")))

    def test_son_palabras_completas_x1_no_calza_con_x10(self):
        pal = self.dist("Trotadora ILUS X1")
        self.assertFalse(self.calza("Correa trotadora X10", pal))
        self.assertFalse(self.calza("Correa trotadora X12", pal))

    def test_los_separadores_no_estorban(self):
        pal = self.dist("Trotadora ILUS X1")
        self.assertTrue(self.calza("Correa, trotadora-X1 (negra)", pal))

    def test_sin_palabras_no_calza_nada(self):
        self.assertFalse(self.calza("cualquier repuesto", []))
        self.assertEqual(self.dist("ILUS Fitness de la"), [])


# ─────────────────────────────────────────────────────────────────────────────
class TestModelosDeMaquina(unittest.TestCase):
    """_otrep_modelos_de_maquina con una BD falsa."""

    @classmethod
    def setUpClass(cls):
        cls.base = _cargar(
            ["_otrep_norm_nombre", "_otrep_norm_sku", "_otrep_modelos_de_maquina"],
            ["_OTREP_MODELOS_TOPE"], extra={"re": re, "mysql_fetchall": None, "_otrep_producto_de_maquina": None})

    def _correr(self, maquina, principal, respuestas):
        amb = dict(self.base)
        bd = _BDFalsa(respuestas)
        amb["mysql_fetchall"] = bd
        amb["_otrep_producto_de_maquina"] = lambda m: principal
        # Las funciones ya viven en el ámbito `base`: se vuelven a asociar con la BD falsa.
        fn = amb["_otrep_modelos_de_maquina"]
        fn.__globals__.update(mysql_fetchall=bd, _otrep_producto_de_maquina=amb["_otrep_producto_de_maquina"])
        return fn(maquina), bd

    def test_el_principal_por_sku_va_primero_y_no_se_repite(self):
        principal = {"id": 10, "sku": "1010089701", "nombre": "Trotadora ILUS X1"}
        out, _ = self._correr(
            {"sku": "1010089701", "nombre": "Trotadora ILUS X1"}, principal,
            [("UPPER(REPLACE(sku", [{"id": 10, "sku": "1010089701", "nombre": "Trotadora ILUS X1"},
                                    {"id": 11, "sku": "1010 089701", "nombre": "Trotadora ILUS X1 (otro SKU)"}]),
             ("REGEXP_REPLACE", [{"id": 10, "sku": "1010089701", "nombre": "Trotadora ILUS X1"},
                                 {"id": 12, "sku": "MOD-0003", "nombre": "Trotadora ILUS X1"}])])
        self.assertEqual([x["id"] for x in out], [10, 11, 12])
        self.assertEqual([x["via"] for x in out], ["sku", "sku_normalizado", "nombre"])
        self.assertEqual(out[0], {"id": 10, "sku": "1010089701", "nombre": "Trotadora ILUS X1", "via": "sku"})

    def test_sin_principal_por_sku_se_deduce_por_el_nombre_del_equipo(self):
        out, _ = self._correr(
            {"sku": "MAN-0012", "nombre": "Trotadora ILUS X1"}, None,
            [("REGEXP_REPLACE", [{"id": 21, "sku": "1010089701", "nombre": "Trotadora ILUS X1"},
                                 {"id": 22, "sku": "MOD-0003", "nombre": "trotadora  ilus x1"}])])
        self.assertEqual(out[0]["id"], 21)
        self.assertEqual(out[0]["via"], "nombre_maquina")
        self.assertEqual(out[1]["via"], "nombre")

    def test_sin_sku_y_sin_nombre_no_hay_modelos(self):
        out, bd = self._correr({"sku": "", "nombre": None}, None, [])
        self.assertEqual(out, [])
        self.assertEqual(bd.llamadas, [])  # ni siquiera pregunta a la BD

    def test_si_el_sku_exacto_no_existe_el_normalizado_pasa_a_principal(self):
        out, _ = self._correr(
            {"sku": "abc 123", "nombre": "Bici X"}, None,
            [("UPPER(REPLACE(sku", [{"id": 31, "sku": "ABC123", "nombre": "Bici X"}])])
        self.assertEqual((out[0]["id"], out[0]["via"]), (31, "sku_normalizado"))

    def test_tope_de_10_modelos(self):
        filas = [{"id": 100 + i, "sku": f"S{i}", "nombre": "Cinta X"} for i in range(25)]
        out, _ = self._correr({"sku": "", "nombre": "Cinta X"}, None, [("REGEXP_REPLACE", filas)])
        self.assertEqual(len(out), 10)

    def test_un_fallo_de_bd_no_rompe_y_deja_el_principal(self):
        principal = {"id": 10, "sku": "S", "nombre": "N"}
        out, _ = self._correr({"sku": "S", "nombre": "N"}, principal,
                              [("UPPER(REPLACE(sku", RuntimeError("bd caida")),
                               ("REGEXP_REPLACE", RuntimeError("bd caida"))])
        self.assertEqual([x["id"] for x in out], [10])

    def test_el_sql_va_parametrizado_y_excluye_las_lineas_zz(self):
        _, bd = self._correr({"sku": "S 1", "nombre": "Mi Modelo Raro"}, None, [])
        self.assertEqual(len(bd.llamadas), 2)
        for sql, params in bd.llamadas:
            self.assertNotIn("Mi Modelo Raro", sql)
            self.assertNotIn("S1", sql)
            self.assertTrue(params)
        sql_nombre = [s for s, _p in bd.llamadas if "REGEXP_REPLACE" in s][0]
        self.assertIn("sku NOT LIKE 'ZZ%%'", sql_nombre)
        self.assertIn("activo=1", sql_nombre)
        self.assertEqual([p for s, p in bd.llamadas if "UPPER(REPLACE(sku" in s][0], ("S1",))


# ─────────────────────────────────────────────────────────────────────────────
class TestPosiblesYOrigen(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.amb = _cargar(
            ["_otrep_palabras_de_texto", "_otrep_palabras_distintivas", "_otrep_calza_palabras",
             "_otrep_posibles_compatibles", "_otrep_marcar_modelos_origen"],
            ["_OTREP_PALABRAS_IGNORADAS", "_OTREP_POSIBLES_TOPE"],
            extra={"re": re, "unicodedata": unicodedata, "mysql_fetchall": None,
                   "_OTREP_SQL_STOCK": "SELECT rs.id FROM mant_repuestos_stock rs",
                   "_otrep_fmt_stock": None})

    def _posibles(self, modelo, filas, excluir=()):
        bd = _BDFalsa([("mant_repuestos_stock rs", filas)])
        llamadas_fmt = []

        def fmt(r, para_ot=False):
            llamadas_fmt.append(para_ot)
            return {"id": r["id"], "descripcion": r["descripcion"]}

        fn = self.amb["_otrep_posibles_compatibles"]
        fn.__globals__.update(mysql_fetchall=bd, _otrep_fmt_stock=fmt)
        return fn(modelo, set(excluir)), bd, llamadas_fmt

    def test_trae_los_que_nombran_el_modelo_y_deja_fuera_los_de_otro(self):
        filas = [
            {"id": 1, "descripcion": "Motor de inclinación trotadora x1"},
            {"id": 2, "descripcion": "Pantalla Escaladora ILUS X1"},
            {"id": 3, "descripcion": "Correa trotadora X10"},
            {"id": 4, "descripcion": "Sensor Trotadora ILUS X1 negro"},
        ]
        out, bd, fmt = self._posibles({"id": 9, "nombre": "Trotadora ILUS X1"}, filas)
        self.assertEqual([x["id"] for x in out], [1, 4])
        self.assertTrue(all(fmt), "tiene que formatear con para_ot=True (sin costo ni proveedor para el técnico)")

    def test_prefiltro_sql_con_like_por_palabra_y_parametrizado(self):
        _, bd, _ = self._posibles({"id": 9, "nombre": "Trotadora ILUS X1"}, [])
        sql, params = bd.llamadas[0]
        self.assertIn("rs.descripcion LIKE %s", sql)
        self.assertEqual(sorted(params), ["%trotadora%", "%x1%"])
        self.assertNotIn("trotadora", sql)

    def test_los_ya_compatibles_no_se_repiten(self):
        filas = [{"id": 1, "descripcion": "Motor trotadora x1"}, {"id": 2, "descripcion": "Correa trotadora x1"}]
        out, _, _ = self._posibles({"id": 9, "nombre": "Trotadora ILUS X1"}, filas, excluir=[1])
        self.assertEqual([x["id"] for x in out], [2])

    def test_tope_de_30(self):
        filas = [{"id": i, "descripcion": f"Pieza {i} trotadora x1"} for i in range(1, 80)]
        out, _, _ = self._posibles({"id": 9, "nombre": "Trotadora ILUS X1"}, filas)
        self.assertEqual(len(out), 30)

    def test_modelo_sin_nombre_distintivo_no_consulta_nada(self):
        out, bd, _ = self._posibles({"id": 9, "nombre": "ILUS Fitness"}, [{"id": 1, "descripcion": "x"}])
        self.assertEqual(out, [])
        self.assertEqual(bd.llamadas, [])
        out, bd, _ = self._posibles(None, [])
        self.assertEqual(out, [])

    def test_modelos_origen_dice_contra_cual_modelo_se_declaro(self):
        modelos = [{"id": 10, "sku": "A1", "nombre": "Trotadora ILUS X1", "via": "sku"},
                   {"id": 12, "sku": "MOD-0003", "nombre": "Trotadora ILUS X1", "via": "nombre"}]
        items = [{"id": 1}, {"id": 2}, {"id": 3}]
        bd = _BDFalsa([("mant_repuestos_stock_modelos", [
            {"repuesto_id": 1, "producto_id": 12}, {"repuesto_id": 2, "producto_id": 10},
            {"repuesto_id": 2, "producto_id": 12}])])
        fn = self.amb["_otrep_marcar_modelos_origen"]
        fn.__globals__.update(mysql_fetchall=bd)
        fn(items, modelos)
        self.assertEqual(items[0]["modelos_origen"], [{"nombre": "Trotadora ILUS X1", "sku": "MOD-0003"}])
        self.assertEqual([x["sku"] for x in items[1]["modelos_origen"]], ["A1", "MOD-0003"])  # principal primero
        self.assertEqual(items[2]["modelos_origen"], [])
        # solo nombre y SKU: nada de proveedor ni costo
        for it in items:
            for mo in it["modelos_origen"]:
                self.assertEqual(set(mo), {"nombre", "sku"})


# ─────────────────────────────────────────────────────────────────────────────
class TestManuales(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.amb = _cargar(["_otrep_manuales_de_modelos"], extra={"mysql_fetchall": None})

    def _manuales(self, modelos, multi, legado, vid=5, mid=9):
        bd = _BDFalsa([("cat_producto_manuales", multi), ("manual_pdf_key", legado)])
        fn = self.amb["_otrep_manuales_de_modelos"]
        fn.__globals__.update(mysql_fetchall=bd)
        return fn(modelos, vid, mid), bd

    MODELOS = [{"id": 10, "sku": "A1", "nombre": "Trotadora ILUS X1", "via": "sku"},
               {"id": 12, "sku": "MOD-0003", "nombre": "Trotadora ILUS X1", "via": "nombre"}]

    def test_nunca_expone_gcs_key_ni_uploaded_by(self):
        multi = [{"id": 1, "producto_id": 10, "gcs_key": "catalogo/manuales/secreto.pdf", "uploaded_by": "juan",
                  "nombre_archivo": "Manual X1.pdf", "size_kb": 2048, "orden": 1}]
        legado = [{"id": 12, "manual_pdf_key": "catalogo/legado/otro.pdf", "manual_pdf_nombre": "Manual viejo.pdf",
                   "manual_pdf_size_kb": 300}]
        out, _ = self._manuales(self.MODELOS, multi, legado)
        texto = repr(out)
        self.assertNotIn("gcs_key", texto)
        self.assertNotIn("secreto", texto)
        self.assertNotIn("catalogo/legado", texto)
        self.assertNotIn("uploaded_by", texto)
        self.assertNotIn("juan", texto)
        for m in out:
            self.assertEqual(set(m), {"id", "tipo", "nombre", "size_kb", "modelo", "url"})
            self.assertEqual(set(m["modelo"]), {"sku", "nombre"})

    def test_usa_las_rutas_propias_de_la_ot_no_las_del_catalogo(self):
        multi = [{"id": 1, "producto_id": 10, "gcs_key": "k1", "nombre_archivo": "A.pdf", "size_kb": 1, "orden": 1}]
        legado = [{"id": 12, "manual_pdf_key": "k2", "manual_pdf_nombre": "B.pdf", "manual_pdf_size_kb": 2}]
        out, _ = self._manuales(self.MODELOS, multi, legado, vid=5, mid=9)
        urls = {m["tipo"]: m["url"] for m in out}
        self.assertEqual(urls["multi"], "/ot/5/equipo/9/manual/1")
        self.assertEqual(urls["legado"], "/ot/5/equipo/9/manual-legado/12")
        self.assertFalse(any("/catalogo" in m["url"] for m in out))

    def test_orden_principal_primero_y_luego_por_orden(self):
        multi = [{"id": 7, "producto_id": 12, "gcs_key": "a", "nombre_archivo": "del equivalente.pdf", "size_kb": 1, "orden": 1},
                 {"id": 3, "producto_id": 10, "gcs_key": "b", "nombre_archivo": "segundo.pdf", "size_kb": 1, "orden": 2},
                 {"id": 2, "producto_id": 10, "gcs_key": "c", "nombre_archivo": "primero.pdf", "size_kb": 1, "orden": 1}]
        out, _ = self._manuales(self.MODELOS, multi, [])
        self.assertEqual([m["nombre"] for m in out], ["primero.pdf", "segundo.pdf", "del equivalente.pdf"])
        self.assertEqual(out[0]["modelo"], {"sku": "A1", "nombre": "Trotadora ILUS X1"})
        self.assertEqual(out[2]["modelo"]["sku"], "MOD-0003")

    def test_el_legado_que_ya_salio_como_multi_no_se_repite(self):
        multi = [{"id": 1, "producto_id": 10, "gcs_key": "misma-clave", "nombre_archivo": "A.pdf", "size_kb": 1, "orden": 1}]
        legado = [{"id": 10, "manual_pdf_key": "misma-clave", "manual_pdf_nombre": "A.pdf", "manual_pdf_size_kb": 1}]
        out, _ = self._manuales(self.MODELOS, multi, legado)
        self.assertEqual(len(out), 1)

    def test_sin_modelos_no_consulta(self):
        out, bd = self._manuales([], [], [])
        self.assertEqual(out, [])
        self.assertEqual(bd.llamadas, [])

    def test_consultas_parametrizadas(self):
        _, bd = self._manuales(self.MODELOS, [], [])
        for sql, params in bd.llamadas:
            self.assertEqual(params, (10, 12))
            self.assertNotIn("10", sql)


# ─────────────────────────────────────────────────────────────────────────────
class TestRutasDelManual(unittest.TestCase):
    def test_las_rutas_exigen_login_mantenciones_y_poder_ver_la_ot(self):
        for fn in ("ot2_equipo_manual_ver", "ot2_equipo_manual_legado_ver"):
            decs = _decoradores(fn)
            self.assertIn("app.route", decs, fn)
            self.assertIn("_mant_required", decs, fn)
            self.assertIn("_ot_can_view", decs, fn)
            # el orden importa: login/permiso primero, luego el candado de la OT
            self.assertLess(decs.index("_mant_required"), decs.index("_ot_can_view"), fn)

    def test_las_urls_son_get_y_van_sin_api(self):
        codigo = _leer(os.path.join(RAIZ, "app.py"))
        for ruta in ('"/ot/<int:vid>/equipo/<int:mid>/manual/<int:manual_id>"',
                     '"/ot/<int:vid>/equipo/<int:mid>/manual-legado/<int:producto_id>"'):
            self.assertIn('@app.route(' + ruta + ', methods=["GET"])', codigo)
        # se abren en una pestaña nueva: nada de '/api/' (los errores son páginas, no JSON)
        self.assertNotIn('"/ot/api/<int:vid>/equipo/<int:mid>/manual', codigo)

    def test_el_candado_idor_valida_ot_equipo_y_pertenencia_del_manual(self):
        src = _fuente_de("_otrep_servir_manual")
        self.assertIn("_otrep_cargar_ot_y_equipo(vid, mid)", src)       # OT <-> equipo <-> cliente
        self.assertIn("_otrep_modelos_de_maquina(m)", src)              # modelos de ESE equipo
        self.assertRegex(src, r'fila\.get\("producto_id"\) in ids')     # manual multi pertenece
        self.assertRegex(src, r"producto_id in ids")                    # manual legado pertenece
        self.assertIn("404", src)

    def test_entrega_inline_con_send_file_y_nombre_de_descarga(self):
        src = _fuente_de("_otrep_servir_manual")
        self.assertIn("send_file(", src)
        self.assertIn('mimetype="application/pdf"', src)
        self.assertIn("as_attachment=False", src)
        self.assertIn("download_name=nombre", src)
        self.assertIn("io.BytesIO(data)", src)
        self.assertNotIn("Content-Disposition", src)   # no se arma a mano (nombres con acentos/comillas)
        self.assertNotIn("as_attachment=True", src)

    def test_los_errores_son_pagina_amable_no_json_crudo(self):
        src = _fuente_de("_otrep_servir_manual")
        self.assertGreaterEqual(src.count("_friendly_error_page("), 5)
        self.assertNotIn("jsonify", src)
        # REGLA #4: la excepción no se le cuenta al usuario (ni la clave del blob)
        self.assertNotIn("str(e)", src)
        self.assertIn("type(e).__name__", src)

    def test_el_cliente_gcs_es_el_mismo_que_usa_el_catalogo(self):
        self.assertIn("_gcs_bucket()", _fuente_de("_otrep_servir_manual"))


# ─────────────────────────────────────────────────────────────────────────────
class TestEndpointOpciones(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.src = _fuente_de("ot2_api_equipo_repuestos_opciones")

    def test_sigue_gateado_como_antes(self):
        decs = _decoradores("ot2_api_equipo_repuestos_opciones")
        self.assertIn("_mant_required", decs)
        self.assertIn("_ot_can_configurar", decs)

    def test_compatibles_de_cualquiera_de_los_modelos_equivalentes(self):
        self.assertIn("_otrep_modelos_de_maquina(m)", self.src)
        self.assertIn("sm.producto_id IN (", self.src)
        self.assertIn("_otrep_marcar_modelos_origen(compatibles, modelos)", self.src)

    def test_la_respuesta_trae_modelo_modelos_posibles_y_manuales(self):
        for clave in ('"modelo":', '"modelos":', '"compatibles":', '"posibles":', '"manuales":', '"piolas":'):
            self.assertIn(clave, self.src)

    def test_los_posibles_se_formatean_igual_que_los_compatibles_sin_armar_filas_propias(self):
        src = _fuente_de("_otrep_posibles_compatibles")
        self.assertIn("_otrep_fmt_stock(r, para_ot=True)", src)
        # Patrones de CÓDIGO (una columna, una clave o un alias), no la palabra suelta en un comentario.
        uso_proveedor = re.compile(r"""proveedor_|["']proveedor|\bpv\.|costo_unitario""")
        for fn in ("_otrep_posibles_compatibles", "_otrep_marcar_modelos_origen", "_otrep_manuales_de_modelos"):
            self.assertIsNone(uso_proveedor.search(_fuente_de(fn)), f"{fn} no debe armar filas con proveedor ni costo")

    def test_las_piolas_siguen_saliendo_del_modelo_principal(self):
        self.assertIn('_otrep_piolas_de_modelo(prod["id"])', self.src)

    def test_no_se_toco_el_helper_original(self):
        src = _fuente_de("_otrep_producto_de_maquina")
        self.assertIn("SELECT id, sku, nombre FROM cat_productos WHERE sku=%s LIMIT 1", src)


# ─────────────────────────────────────────────────────────────────────────────
class TestAsociarModeloEscritoAMano(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.src = _fuente_de("repstock_modelo_asociar")

    def test_busca_en_cualquier_origen_antes_de_crear_un_MOD(self):
        i_busca = self.src.index("REGEXP_REPLACE(TRIM(nombre)")
        i_crea = self.src.index("_repstock_next_sku_modelo_manual(conn)")
        self.assertLess(i_busca, i_crea)
        bloque = self.src[i_busca - 200:i_busca + 400]
        self.assertIn("sku NOT LIKE 'ZZ%%'", bloque)
        self.assertIn("activo=1", bloque)
        self.assertNotIn("origen='manual'", bloque)   # ya no solo contra los escritos a mano
        self.assertIn("(nombre_manual,)", bloque)      # parametrizado

    def test_responde_que_reuso_un_modelo_del_catalogo(self):
        self.assertIn('resp_ok["reusado_de_catalogo"] = True', self.src)
        self.assertIn('resp_ok["sku"]', self.src)
        self.assertIn("reusado_de_catalogo = True", self.src)

    def test_no_se_borra_ni_fusiona_nada(self):
        for peligro in ("DELETE FROM cat_productos", "UPDATE cat_productos SET activo"):
            self.assertNotIn(peligro, self.src)

    def test_sigue_exigiendo_supervisor_para_escribir_a_mano(self):
        self.assertIn("_repstock_puede_gestionar_modelos()", self.src)


# ─────────────────────────────────────────────────────────────────────────────
class TestTextosYUI(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.html = _leer(DETALLE)
        cls.buscador = _leer(BUSCADOR_JS)

    def test_el_origen_manual_se_lee_escrito_a_mano(self):
        amb = _cargar([], ["_OTREP_ORIGEN_LABEL"])
        self.assertEqual(amb["_OTREP_ORIGEN_LABEL"]["manual"], "Escrito a mano · por validar")
        self.assertIn("'manual'", _leer(os.path.join(RAIZ, "app.py")))  # el origen en BD sigue siendo 'manual'

    def test_la_pestana_manual_pasa_a_escribir_a_mano_pero_conserva_su_data_tab(self):
        self.assertRegex(self.html, r'data-tab="manual" onclick="otrepTab\(\'manual\'\)"><i class="bi bi-pencil-square me-1"></i>Escribir a mano</button>')
        self.assertNotRegex(self.html, r'data-tab="manual"[^>]*>Manual</button>')

    def test_no_quedan_los_textos_antiguos(self):
        for viejo in ("escríbelo manual", "Escríbelo manual", "Manual · por validar", "(manual · por validar)"):
            self.assertNotIn(viejo, self.html)
        self.assertIn("escríbelo a mano", self.html)
        self.assertIn("Escrito a mano · por validar", self.html)
        self.assertNotIn("escríbelo manual", self.buscador)
        self.assertIn("escríbelo a mano", self.buscador)

    def test_cuarta_pestana_manual_del_equipo(self):
        self.assertRegex(self.html, r'data-tab="docs" onclick="otrepTab\(\'docs\'\)"><i class="bi bi-file-earmark-pdf me-1"></i>Manual del equipo <span id="otrepNDocs">\(0\)</span>')
        self.assertIn('id="otrepPaneDocs"', self.html)
        # sigue habiendo Compatibles, Buscar en bodega y Escribir a mano (REGLA #4.2)
        for tab in ("compat", "bodega", "manual", "docs"):
            self.assertIn(f'data-tab="{tab}"', self.html)

    def test_las_tarjetas_de_pdf_solo_ven_en_pestana_nueva(self):
        bloque = self.html.split("function otrepDocsPintar(j){")[1].split("/* Gestión confirma")[0]
        self.assertIn('target="_blank" rel="noopener"', bloque)
        self.assertIn("Ver manual", bloque)
        self.assertIn("Este modelo aún no tiene manual en el Catálogo.", bloque)
        # sin correo, sin descarga extra, sin culpar a nadie
        for prohibido in ("mailto", "download", "enviar", "correo", "descargar", "Juan"):
            self.assertNotIn(prohibido, bloque)
        # el nombre del PDF va completo y escapado
        self.assertIn("otdEsc(mn.nombre", bloque)
        self.assertNotIn("substring", bloque)
        self.assertNotIn("slice(", bloque)

    def test_las_piolas_del_modelo_son_solo_lectura(self):
        bloque = self.html.split("function otrepDocsPintar(j){")[1].split("/* Gestión confirma")[0]
        self.assertIn("Piolas de este modelo", self.html)
        piolas = bloque.split("var pio =")[1]
        self.assertNotIn("onclick", piolas)

    def test_abrir_la_pestana_docs_no_altera_la_seleccion(self):
        fn = self.html.split("function otrepTab(t){")[1].split("function otrepPintarOpciones")[0]
        self.assertIn("document.getElementById('otrepPaneDocs')", fn)
        linea_docs = [l for l in fn.split("\n") if "(t === 'docs')" in l]
        self.assertTrue(linea_docs)
        self.assertNotIn("otrepSelManual", " ".join(linea_docs))
        self.assertIn("if (t === 'manual') otrepSelManual();", fn)   # 'manual' sigue como antes
        self.assertNotIn("t === 'docs') otrepSelManual", fn)

    def test_enlaces_al_manual_desde_escribir_a_mano_y_desde_las_piolas(self):
        self.assertIn("¿No sabes el nombre exacto? Revisa el Manual del equipo", self.html)
        self.assertIn("Ver manual del equipo", self.html)
        self.assertIn('id="otrepPiolasManualLnk" onclick="otrepTab(\'docs\')"', self.html)

    def test_encabezado_del_modelo_chip_de_origen_y_posibles(self):
        self.assertIn("Modelo: <b>", self.html)
        self.assertIn("' · SKU '", self.html)
        self.assertIn("equivalente", self.html)
        self.assertIn("Modelo deducido por el nombre del equipo.", self.html)
        self.assertIn("Declarado para: ", self.html)
        self.assertIn("Posibles compatibles", self.html)
        self.assertIn('<span class="otrep-pill ambar">sin confirmar</span>', self.html)

    def test_los_posibles_se_eligen_con_la_misma_funcion_que_un_compatible(self):
        fn = self.html.split("function otrepSelStock(id, origen, btn){")[1].split("function otrepSelManual")[0]
        self.assertIn("opciones.posibles", fn)
        self.assertIn("onclick=\"otrepSelStock(", self.html)

    def test_confirmar_compatibilidad_solo_gestion_y_con_el_contrato_de_la_ruta(self):
        self.assertIn("var _OTREP_ES_TECNICO = {{ (es_tecnico or false) | tojson }};", self.html)
        self.assertIn("confirmar: !_OTREP_ES_TECNICO", self.html)
        bloque = self.html.split("async function otrepConfirmarCompat(repId, btn){")[1].split("async function otrepRecargarOpciones")[0]
        self.assertIn("'/mantenciones/api/repuestos-stock/' + repId + '/modelos'", bloque)
        self.assertIn("method: 'POST'", bloque)
        self.assertIn("JSON.stringify({producto_id: mod.id})", bloque)
        self.assertIn("ilusToast(", bloque)
        self.assertIn("otrepRecargarOpciones(mid)", bloque)

    def test_no_hay_dialogos_nativos_en_lo_nuevo(self):
        bloque = self.html.split("var _OTREP_ES_TECNICO")[1].split("SOLICITAR REPUESTO / CAMBIO DE PIOLA / DAR DE BAJA")[0]
        codigo = re.sub(r"//[^\n]*|/\*.*?\*/", "", bloque, flags=re.S)
        for nativo in ("alert", "confirm", "prompt"):
            self.assertIsNone(re.search(r"(?<![\w.$])" + nativo + r"\(", codigo), f"hay un {nativo}() nativo")

    def test_el_css_nuevo_va_en_su_propio_style_justo_antes_del_modal(self):
        i_buscador = self.html.index("<script src=\"{{ url_for('static', filename='repuestos_buscador.js') }}\"></script>")
        i_modal = self.html.index('<div class="modal fade" id="modalRepSol"')
        zona = re.sub(r"\{#.*?#\}", "", self.html[i_buscador:i_modal], flags=re.S)  # sin comentarios Jinja
        self.assertEqual(zona.count("<style>"), 1)
        self.assertEqual(zona.count("</style>"), 1)
        css = zona.split("<style>")[1].split("</style>")[0]
        self.assertIn("grid-template-columns:1fr 1fr", css)
        self.assertIn("min-height:44px", css)
        self.assertIn("white-space:normal", css)
        self.assertIn("min-height:56px", css)
        self.assertIn("overflow-wrap:anywhere", css)
        self.assertIn("@media (max-width:640px)", css)

    @unittest.skipUnless(shutil.which("node"), "node no está instalado")
    def test_el_javascript_del_modal_no_tiene_errores_de_sintaxis(self):
        try:
            import jinja2
        except ImportError:
            self.skipTest("jinja2 no está instalado")
        env = jinja2.Environment()
        env.globals["url_for"] = lambda *a, **k: "/x"
        for marca in ("var _OTREP_ES_TECNICO", "const _OTREP_EQ_NOMBRE = {};"):
            i = self.html.index(marca)
            ini = self.html.rfind("<script>", 0, i) + len("<script>")
            fin = self.html.index("</script>", i)
            js = env.from_string(self.html[ini:fin]).render(equipos=[], es_tecnico=False, es_tecnico_externo=False)
            with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False, encoding="utf-8") as fh:
                fh.write(js)
                ruta = fh.name
            try:
                res = subprocess.run(["node", "--check", ruta], capture_output=True, text=True)
                self.assertEqual(res.returncode, 0, res.stderr[:800])
            finally:
                os.unlink(ruta)

    def test_jinja_de_la_plantilla_parsea(self):
        try:
            import jinja2
        except ImportError:
            self.skipTest("jinja2 no está instalado")
        jinja2.Environment().parse(self.html)


if __name__ == "__main__":
    unittest.main()

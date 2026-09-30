"""Dar de baja pendientes e incidencias + nombres desde Random (Daniel, 2026-09-30).

Pedido: "quiero que todos los usuarios puedan gestionar y dar de baja productos sin
eliminar para tener trazabilidad de por qué algún día estuvo" -- y, viendo la tabla
de Pendientes: "existen SKU que no existen en Random, no sé de dónde los sacaste"
(eran productos REALES de Random: el nombre solo salía de CheckWMS y quedaba en "—").

Decisiones de Daniel (2026-09-30):
  - "Dar de baja" va en Pendientes Y en la tabla de Incidencias.
  - "Eliminar" (papelera) se queda SOLO para administradores.
  - Si algo dado de baja vuelve con números distintos, vuelve a salir marcado
    "antes dado de baja" (no queda oculto para siempre).

Sin BD ni Flask: funciones puras de app.py extraídas con ast (mismo criterio que
tests/test_incidencias_repuesto_tercera_fuente.py, cuyo AST cacheado se reusa), más
revisiones del código fuente de los endpoints y de la plantilla.

Correr con:  py -m unittest tests.test_incidencias_bajas
"""
import ast
import os
import re
import shutil
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tests.test_incidencias_repuesto_tercera_fuente import _codigo_y_arbol, _fuente_de  # noqa: E402

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PLANTILLA = os.path.join(RAIZ, "templates", "mantenciones", "incidencias.html")


def _cargar(funciones=(), constantes=(), extra=None):
    """Extrae funciones y constantes `NOMBRE = <literal>` de app.py y las ejecuta
    juntas en un ámbito aislado."""
    _, arbol = _codigo_y_arbol()
    ambito = dict(extra or {})
    for nodo in arbol.body:
        if (isinstance(nodo, ast.Assign) and len(nodo.targets) == 1
                and isinstance(nodo.targets[0], ast.Name) and nodo.targets[0].id in constantes):
            exec(compile(ast.Module(body=[nodo], type_ignores=[]), "<app>", "exec"), ambito)
        if isinstance(nodo, ast.FunctionDef) and nodo.name in funciones:
            exec(compile(ast.Module(body=[nodo], type_ignores=[]), "<app>", "exec"), ambito)
    faltan = [n for n in tuple(funciones) + tuple(constantes) if n not in ambito]
    assert not faltan, f"no se encontraron en app.py: {faltan}"
    return ambito


def _decoradores(nombre_funcion):
    """Nombres de los decoradores de una función de app.py (solo los que son
    `@nombre`; las rutas `@app.route(...)` se devuelven como 'app.route')."""
    _, arbol = _codigo_y_arbol()
    for nodo in ast.walk(arbol):
        if isinstance(nodo, ast.FunctionDef) and nodo.name == nombre_funcion:
            out = []
            for d in nodo.decorator_list:
                if isinstance(d, ast.Name):
                    out.append(d.id)
                elif isinstance(d, ast.Call) and isinstance(d.func, ast.Attribute):
                    out.append(f"{ast.unparse(d.func)}")
            return out
    raise AssertionError(f"no se encontró la función '{nombre_funcion}' en app.py")


def _plantilla():
    with open(PLANTILLA, encoding="utf-8") as fh:
        return fh.read()


class TestValidarMotivoDeBaja(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        amb = _cargar(["_inc_baja_validar"], ["INC_BAJA_MOTIVOS"])
        cls.validar = staticmethod(amb["_inc_baja_validar"])
        cls.motivos = amb["INC_BAJA_MOTIVOS"]

    def test_motivo_de_la_lista_es_valido_sin_detalle(self):
        cod, txt, err = self.validar("regularizado_random", "")
        self.assertEqual((cod, txt, err), ("regularizado_random", None, None))

    def test_sin_motivo_o_motivo_inventado_se_rechaza(self):
        self.assertIsNotNone(self.validar("", "algo")[2])
        self.assertIsNotNone(self.validar(None, None)[2])
        self.assertIsNotNone(self.validar("me_dio_la_gana", "algo largo de verdad")[2])

    def test_otro_motivo_exige_explicarlo(self):
        self.assertIsNotNone(self.validar("otro", "")[2])
        self.assertIsNotNone(self.validar("otro", "corto")[2])
        cod, txt, err = self.validar("otro", "  Quedó en otra bodega por traslado  ")
        self.assertIsNone(err)
        self.assertEqual(txt, "Quedó en otra bodega por traslado")

    def test_el_detalle_se_corta_a_500(self):
        _, txt, err = self.validar("salio_resuelto", "x" * 900)
        self.assertIsNone(err)
        self.assertEqual(len(txt), 500)

    def test_hay_un_motivo_otro_y_ninguno_vacio(self):
        self.assertIn("otro", self.motivos)
        self.assertTrue(all(v.strip() for v in self.motivos.values()))


class TestClaveYNumerosDeBaja(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        amb = _cargar(["_inc_baja_clave", "_inc_baja_num", "_inc_baja_mismos_numeros"])
        cls.clave = staticmethod(amb["_inc_baja_clave"])
        cls.iguales = staticmethod(amb["_inc_baja_mismos_numeros"])

    def test_clave_por_sku_para_las_diferencias_de_cantidad(self):
        self.assertEqual(self.clave({"tipo": "dif_erp", "sku": "1120100314 "}), "dif_erp|1120100314")
        self.assertEqual(self.clave({"tipo": "dif_wms", "sku": "ES809"}), "dif_wms|ES809")

    def test_clave_por_ua_y_sku_para_lo_que_depende_de_una_ua(self):
        self.assertEqual(self.clave({"tipo": "falta_registrar", "ua": "ua1014582", "sku": "1127100788"}),
                         "falta_registrar|UA1014582|1127100788")

    def test_clave_de_sin_motivo_es_la_incidencia(self):
        self.assertEqual(self.clave({"tipo": "sin_motivo", "ids": [41, 42]}), "sin_motivo|41")

    def test_misma_clave_aunque_cambien_los_numeros(self):
        a = self.clave({"tipo": "dif_erp", "sku": "X1", "erp": 4})
        b = self.clave({"tipo": "dif_erp", "sku": "X1", "erp": 5})
        self.assertEqual(a, b)

    def test_mismos_numeros_ignora_ceros_a_la_derecha_y_tipos(self):
        baja = {"nuestra_bd": "0.00", "erp": 4.0, "wms": None}
        self.assertTrue(self.iguales(baja, {"nuestra_bd": 0, "erp": 4, "wms": None}))

    def test_cambia_un_numero_ya_no_es_lo_mismo(self):
        baja = {"nuestra_bd": 0, "erp": 4, "wms": None}
        self.assertFalse(self.iguales(baja, {"nuestra_bd": 0, "erp": 5, "wms": None}))

    def test_aparecer_o_desaparecer_un_dato_cuenta_como_cambio(self):
        baja = {"nuestra_bd": 0, "erp": 4, "wms": None}
        self.assertFalse(self.iguales(baja, {"nuestra_bd": 0, "erp": 4, "wms": 2}))
        self.assertFalse(self.iguales({"nuestra_bd": 0, "erp": 4, "wms": 2}, {"nuestra_bd": 0, "erp": 4}))


class TestAplicarBajas(unittest.TestCase):
    """Decisión de Daniel: lo dado de baja se oculta mientras sus números no cambien;
    si cambian, vuelve a salir marcado "antes dado de baja"."""

    @classmethod
    def setUpClass(cls):
        amb = _cargar(["_inc_aplicar_bajas", "_inc_baja_clave", "_inc_baja_num", "_inc_baja_mismos_numeros",
                       "_inc_baja_publica"], ["INC_BAJA_MOTIVOS"])
        cls.aplicar = staticmethod(amb["_inc_aplicar_bajas"])

    @staticmethod
    def _h(sku, erp, bd=0, wms=None, ids=None):
        h = {"tipo": "dif_erp", "sku": sku, "erp": erp, "nuestra_bd": bd, "wms": wms, "ids": ids or []}
        h["clave"] = f"dif_erp|{sku}"
        h["baja_hallazgo"] = not h["ids"]
        return h

    @staticmethod
    def _baja(sku, erp, bd=0, wms=None):
        return {"clave": f"dif_erp|{sku}", "nuestra_bd": bd, "erp": erp, "wms": wms, "motivo_codigo": "regularizado_random",
                "motivo_texto": None, "baja_by": "felipe", "baja_at": None, "reactivada_at": None}

    def test_sin_bajas_no_se_oculta_nada(self):
        hs = [self._h("A", 4), self._h("B", 2)]
        self.assertEqual(len(self.aplicar(hs, {})), 2)

    def test_baja_con_los_mismos_numeros_oculta_el_pendiente(self):
        hs = [self._h("A", 4), self._h("B", 2)]
        vis = self.aplicar(hs, {"dif_erp|A": self._baja("A", 4)})
        self.assertEqual([h["sku"] for h in vis], ["B"])

    def test_si_cambian_los_numeros_vuelve_a_salir_marcado(self):
        hs = [self._h("A", 5)]
        vis = self.aplicar(hs, {"dif_erp|A": self._baja("A", 4)})
        self.assertEqual(len(vis), 1)
        previa = vis[0]["baja_previa"]
        self.assertEqual(previa["baja_by"], "felipe")
        self.assertEqual(previa["erp"], 4.0)            # cómo estaba cuando se dio de baja
        self.assertEqual(previa["motivo"], "Ya está regularizado en Random")
        self.assertTrue(previa["vigente"])

    def test_un_pendiente_con_incidencia_nunca_se_oculta_por_aca(self):
        hs = [self._h("A", 4, ids=[9])]
        vis = self.aplicar(hs, {"dif_erp|A": self._baja("A", 4)})
        self.assertEqual(len(vis), 1)
        self.assertNotIn("baja_previa", vis[0])

    def test_la_baja_de_un_sku_no_toca_a_los_otros(self):
        hs = [self._h("A", 4), self._h("B", 4)]
        vis = self.aplicar(hs, {"dif_erp|A": self._baja("A", 4)})
        self.assertEqual([h["sku"] for h in vis], ["B"])


class TestNombresDesdeRandom(unittest.TestCase):
    """El nombre del producto de un pendiente salía solo de CheckWMS. Se completa con
    el maestro de productos de Random: SOLO lectura (REGLA #4.1)."""

    @classmethod
    def setUpClass(cls):
        cls.capturadas = []

        def falso_random(sql, params=None, max_rows=500):
            cls.capturadas.append((sql, params))
            return [{"sku": "1120100314", "nombre": "Par Mancuernas Ajustables ILUS 36 kg"},
                    {"sku": "es809", "nombre": "Hip Adduction/Abduction Freemotion"}]

        cls.amb = _cargar(["_erp_nombres_por_sku", "_inc_completar_nombres", "_random_sql_validate"],
                          ["_ERP_NOMBRES_CACHE", "_ERP_NOMBRES_TTL", "_RANDOM_FORBIDDEN_TOKENS"],
                          extra={"time": __import__("time"), "_random_sql_query": falso_random, "print": lambda *a, **k: None})

    def setUp(self):
        self.amb["_ERP_NOMBRES_CACHE"].clear()
        self.capturadas.clear()

    def test_la_consulta_es_solo_select_y_pasa_la_validacion_del_erp(self):
        self.amb["_erp_nombres_por_sku"](["1120100314", "ES809"])
        self.assertEqual(len(self.capturadas), 1)
        sql, params = self.capturadas[0]
        self.amb["_random_sql_validate"](sql)           # lanza PermissionError si no es segura
        self.assertTrue(sql.strip().upper().startswith("SELECT"))
        self.assertIn("FROM MAEPR", sql)
        self.assertEqual(sorted(params), ["1120100314", "ES809"])   # parametrizado, nunca f-string
        self.assertEqual(sql.count("%s"), 2)

    def test_devuelve_los_nombres_sin_importar_mayusculas(self):
        nombres = self.amb["_erp_nombres_por_sku"](["1120100314", "ES809"])
        self.assertEqual(nombres["1120100314"], "Par Mancuernas Ajustables ILUS 36 kg")
        self.assertEqual(nombres["ES809"], "Hip Adduction/Abduction Freemotion")

    def test_segunda_llamada_usa_la_cache_y_no_vuelve_a_consultar(self):
        self.amb["_erp_nombres_por_sku"](["1120100314"])
        self.amb["_erp_nombres_por_sku"](["1120100314"])
        self.assertEqual(len(self.capturadas), 1)

    def test_completa_solo_los_hallazgos_sin_nombre(self):
        hs = [{"sku": "1120100314", "descripcion": None},
              {"sku": "ES809", "descripcion": "   "},
              {"sku": "TWMA303-140", "descripcion": "Low Cable Row (del WMS)"}]
        self.amb["_inc_completar_nombres"](hs)
        self.assertEqual(hs[0]["descripcion"], "Par Mancuernas Ajustables ILUS 36 kg")
        self.assertEqual(hs[0]["descripcion_fuente"], "erp")
        self.assertEqual(hs[1]["descripcion"], "Hip Adduction/Abduction Freemotion")
        self.assertEqual(hs[2]["descripcion"], "Low Cable Row (del WMS)")   # el del WMS no se pisa
        self.assertNotIn("descripcion_fuente", hs[2])

    def test_si_todos_tienen_nombre_no_se_consulta_el_erp(self):
        self.amb["_inc_completar_nombres"]([{"sku": "X", "descripcion": "Con nombre"}])
        self.assertEqual(self.capturadas, [])


class TestEndpointsDeBaja(unittest.TestCase):
    """Lo que Daniel decidió, verificado en el código de los endpoints."""

    def test_eliminar_sigue_siendo_solo_de_administradores(self):
        src = _fuente_de("mant_api_incidencias_borrar")
        self.assertIn('perms.get("admin") or perms.get("superadmin")', src)
        self.assertIn("Solo un administrador puede eliminar incidencias", src)

    def test_dar_de_baja_es_de_todos_los_usuarios_del_modulo(self):
        for nombre in ("mant_api_incidencia_baja", "mant_api_incidencias_baja_hallazgo",
                       "mant_api_incidencias_baja_reactivar", "mant_api_incidencias_bajas_list"):
            with self.subTest(nombre):
                decs = _decoradores(nombre)
                self.assertIn("_mant_required", decs)
                self.assertIn("_no_tecnico_salvo_taller", decs)
                src = _fuente_de(nombre)
                self.assertNotIn('perms.get("admin")', src)
                self.assertNotIn("superadmin", src)

    def test_la_baja_de_una_incidencia_deja_la_bitacora_antes_de_tocar_la_fila(self):
        src = _fuente_de("mant_api_incidencia_baja")
        self.assertLess(src.index("_inc_log(iid, \"baja\""), src.index("UPDATE mant_incidencias SET estado='resuelta'"))

    def test_la_baja_de_una_incidencia_no_borra_nada(self):
        src = _fuente_de("mant_api_incidencia_baja")
        self.assertNotIn("DELETE", src.upper().replace("DELETED", ""))
        self.assertNotIn("eliminada=1", src)
        self.assertIn("estado='resuelta'", src)

    def test_la_baja_de_un_pendiente_toma_los_numeros_del_servidor(self):
        src = _fuente_de("mant_api_incidencias_baja_hallazgo")
        self.assertIn("_inc_calcular_hallazgos()", src)
        self.assertIn("h.get(\"nuestra_bd\")", src)
        # no lee los números que mande el navegador
        self.assertNotIn('data.get("nuestra_bd")', src)
        self.assertNotIn('data.get("erp")', src)

    def test_solo_se_da_de_baja_como_pendiente_lo_que_no_tiene_incidencia(self):
        src = _fuente_de("_inc_calcular_hallazgos")
        self.assertIn('h["baja_hallazgo"] = not h.get("ids")', src)
        self.assertIn('h["clave"] = _inc_baja_clave(h)', src)
        self.assertIn("_inc_completar_nombres(hallazgos)", src)

    def test_la_conciliacion_aplica_las_bajas_y_cuenta_las_vigentes(self):
        src = _fuente_de("mant_api_incidencias_conciliacion")
        self.assertIn("_inc_aplicar_bajas(calc[\"hallazgos\"], _inc_bajas_hallazgo_vigentes())", src)
        self.assertIn('"dadas_de_baja"', src)

    def test_la_tabla_principal_no_muestra_lo_dado_de_baja(self):
        src = _fuente_de("mant_api_incidencias_list")
        self.assertIn("_inc_ids_en_baja()", src)

    def test_una_incidencia_dada_de_baja_libera_su_ua(self):
        src = _fuente_de("_inc_ua_duplicada")
        self.assertIn("estado='abierta'", src)

    def test_reactivar_pide_motivo_y_revisa_que_la_ua_siga_libre(self):
        src = _fuente_de("mant_api_incidencias_baja_reactivar")
        self.assertIn("len(motivo) < 10", src)
        self.assertIn("_inc_ua_duplicada(ua, excluir_id=inc[\"id\"])", src)
        self.assertIn("reactivada_at=NOW()", src)

    def test_la_tabla_de_bajas_se_crea_en_el_arranque_incluso_con_skip_migrations(self):
        codigo, _ = _codigo_y_arbol()
        self.assertIn("_ensure_incidencia_bajas_table()", codigo.split("def _ensure_incidencia_bajas_table")[1])
        src = _fuente_de("_ensure_incidencia_bajas_table")
        self.assertIn("CREATE TABLE IF NOT EXISTS mant_incidencia_bajas", src)   # una sentencia: la guardia de DDL la salta

    def test_la_etiqueta_no_existe_en_el_erp_ya_no_engana(self):
        codigo, _ = _codigo_y_arbol()
        bloque = codigo.split("INC_HALLAZGO_INFO = {")[1].split("\n}\n")[0]
        # Los comentarios explican el cambio citando la etiqueta vieja: no cuentan.
        bloque = "\n".join(l for l in bloque.splitlines() if not l.strip().startswith("#"))
        self.assertNotIn("No existe en el ERP", bloque)
        self.assertIn("Sin stock en Random", bloque)


class TestDesgloseValidar(unittest.TestCase):
    """Desglosar por motivo (Daniel, 2026-09-30): "si me entran tres productos de un
    mismo SKU, que pueda separar el producto en las cantidades según el motivo"."""

    @classmethod
    def setUpClass(cls):
        amb = _cargar(["_inc_desglose_validar", "_sentence_case"], ["INC_DESGLOSE_MAX_GRUPOS", "INC_MOTIVO_MAX"])
        cls.validar = staticmethod(amb["_inc_desglose_validar"])

    MOT_A = "falta el acrílico derecho del selector de pesos"
    MOT_B = "pantalla defectuosa, el equipo llegó sin pantalla"

    def test_tres_unidades_dos_motivos_es_valido(self):
        limpios, err = self.validar(3, [{"cantidad": 2, "motivo": self.MOT_A}, {"cantidad": 1, "motivo": self.MOT_B}])
        self.assertIsNone(err)
        self.assertEqual([g["cantidad"] for g in limpios], [2, 1])

    def test_el_motivo_se_capitaliza_como_en_el_resto_del_modulo(self):
        limpios, _ = self.validar(2, [{"cantidad": 1, "motivo": "  falta una pieza importante"},
                                      {"cantidad": 1, "motivo": "otra pieza distinta rota"}])
        self.assertTrue(limpios[0]["motivo"].startswith("Falta"))

    def test_la_suma_tiene_que_dar_exactamente_el_total(self):
        for cants in ((1, 1), (2, 2), (3, 1)):
            _, err = self.validar(3, [{"cantidad": c, "motivo": self.MOT_A} for c in cants])
            self.assertIsNotNone(err, cants)
            self.assertIn("suman", err)

    def test_hace_falta_mas_de_un_grupo_y_mas_de_una_unidad(self):
        self.assertIsNotNone(self.validar(3, [{"cantidad": 3, "motivo": self.MOT_A}])[1])
        self.assertIsNotNone(self.validar(1, [{"cantidad": 1, "motivo": self.MOT_A}, {"cantidad": 0, "motivo": self.MOT_B}])[1])
        self.assertIsNotNone(self.validar(3, None)[1])
        self.assertIsNotNone(self.validar(3, "x")[1])

    def test_cada_grupo_necesita_su_motivo(self):
        _, err = self.validar(3, [{"cantidad": 2, "motivo": self.MOT_A}, {"cantidad": 1, "motivo": "corto"}])
        self.assertIn("Grupo 2", err)

    def test_cantidades_invalidas_se_rechazan(self):
        for mala in (0, -1, "abc", None, 1.5, "1,5"):
            _, err = self.validar(3, [{"cantidad": 2, "motivo": self.MOT_A}, {"cantidad": mala, "motivo": self.MOT_B}])
            self.assertIsNotNone(err, mala)

    def test_acepta_cantidades_como_texto(self):
        limpios, err = self.validar("3", [{"cantidad": "2", "motivo": self.MOT_A}, {"cantidad": "1", "motivo": self.MOT_B}])
        self.assertIsNone(err)
        self.assertEqual(sum(g["cantidad"] for g in limpios), 3)

    def test_no_puede_haber_mas_grupos_que_unidades(self):
        gs = [{"cantidad": 1, "motivo": self.MOT_A}] * 3
        self.assertIsNotNone(self.validar(2, gs)[1])

    def test_el_motivo_se_corta_al_largo_de_la_columna(self):
        limpios, err = self.validar(2, [{"cantidad": 1, "motivo": "x" * 500}, {"cantidad": 1, "motivo": self.MOT_B}])
        self.assertIsNone(err)
        self.assertEqual(len(limpios[0]["motivo"]), 200)


class TestEndpointDesglosar(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.src = _fuente_de("mant_api_incidencias_desglosar")

    def test_es_de_todos_los_usuarios_del_modulo(self):
        decs = _decoradores("mant_api_incidencias_desglosar")
        self.assertIn("_mant_required", decs)
        self.assertIn("_no_tecnico_salvo_taller", decs)
        self.assertNotIn('perms.get("admin")', self.src)

    def test_la_bitacora_del_original_queda_antes_de_tocar_la_fila(self):
        s = self.src
        self.assertLess(s.index('_inc_log(iid, "desglosada", "motivo"'), s.index("UPDATE mant_incidencias SET cantidad"))
        self.assertLess(s.index('_inc_log(iid, "desglosada", "cantidad"'), s.index("UPDATE mant_incidencias SET cantidad"))

    def test_el_original_se_actualiza_solo_si_nadie_lo_cambio_antes(self):
        self.assertIn("AND estado='abierta' AND COALESCE(eliminada,0)=0 AND cantidad=%s", self.src)
        self.assertIn("if not cur.rowcount:", self.src)

    def test_las_partes_nuevas_nacen_sin_ua_y_la_primera_conserva_la_ua(self):
        s = self.src
        # desglose de una incidencia: los clones se insertan SIN `recomendacion`
        ins_clon = s.split("INSERT INTO mant_incidencias")[1].split("VALUES")[0]
        self.assertNotIn("recomendacion", ins_clon)
        # desglose de un pendiente: solo el primer grupo lleva la UA
        self.assertIn("ua if k == 0 else None", s)

    def test_un_desglose_es_una_sola_transaccion(self):
        for trozo in self.src.split('if origen == "hallazgo":'):
            self.assertEqual(trozo.count("db.commit()"), 1 if "INSERT" in trozo else trozo.count("db.commit()"))
        self.assertIn("db.rollback()", self.src)

    def test_no_deja_la_cantidad_por_debajo_de_lo_ya_tomado_como_repuesto(self):
        self.assertIn("mant_ot_repuesto_solicitudes", self.src)
        self.assertIn("tomadas > limpios[0][\"cantidad\"]", self.src)

    def test_el_pendiente_se_recalcula_en_el_servidor_y_no_se_borra_nada(self):
        self.assertIn("_inc_calcular_hallazgos()", self.src)
        self.assertNotIn("DELETE", self.src.upper().replace("DELETED", ""))
        self.assertNotIn("eliminada=1", self.src)

    def test_solo_desglosa_pendientes_sin_incidencia(self):
        self.assertIn('if not h.get("baja_hallazgo"):', self.src)


class TestSqlDeBajas(unittest.TestCase):
    """REGLA #5 (verificar las columnas contra el CREATE TABLE): `ast.parse` no ve los
    nombres de columnas dentro del SQL, y un error acá solo aparecería en producción."""

    @staticmethod
    def _unir_literales(src):
        # "a" \n "b"  ->  "ab"  (concatenación implícita de literales de Python)
        return re.sub(r'"\s*\n\s*"', "", src)

    @classmethod
    def setUpClass(cls):
        create = _fuente_de("_ensure_incidencia_bajas_table")
        cls.columnas = set(re.findall(r"^\s+(\w+)\s+(?:INT|VARCHAR|DECIMAL|DATETIME|DATE)\b", create, re.M))

    def test_la_tabla_declara_las_columnas_esperadas(self):
        for c in ("origen", "clave", "incidencia_id", "motivo_codigo", "motivo_texto", "estado_antes",
                  "fecha_res_antes", "baja_by", "baja_at", "reactivada_at", "reactivada_by", "reactivada_motivo"):
            self.assertIn(c, self.columnas)

    def test_las_columnas_insertadas_existen_y_cuadran_con_los_valores(self):
        for nombre in ("mant_api_incidencias_baja_hallazgo", "mant_api_incidencia_baja"):
            with self.subTest(nombre):
                src = self._unir_literales(_fuente_de(nombre))
                m = re.search(r"INSERT INTO mant_incidencia_bajas \(([^)]*)\)\s*VALUES \(([^)]*)\)", src)
                self.assertIsNotNone(m, "no se encontró el INSERT")
                cols = [c.strip() for c in m.group(1).split(",")]
                vals = [v.strip() for v in m.group(2).split(",")]
                self.assertEqual(len(cols), len(vals), "columnas y valores no cuadran")
                self.assertTrue(set(cols) <= self.columnas, f"columnas que no existen: {set(cols) - self.columnas}")

    def test_los_update_solo_tocan_columnas_que_existen(self):
        usados = set()
        for nombre in ("mant_api_incidencias_baja_hallazgo", "mant_api_incidencia_baja",
                       "mant_api_incidencias_baja_reactivar"):
            src = self._unir_literales(_fuente_de(nombre))
            for m in re.finditer(r"UPDATE mant_incidencia_bajas SET ([^\"]*?) WHERE", src):
                usados |= set(re.findall(r"(\w+)\s*=", m.group(1)))
        self.assertTrue(usados)
        self.assertTrue(usados <= self.columnas, f"columnas que no existen: {usados - self.columnas}")


class TestPlantillaBajas(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.html = _plantilla()

    def test_el_modal_y_el_acceso_directo_existen(self):
        for ident in ('id="incBajaModal"', 'id="incBajaMotivos"', 'id="incBajaBtnOk"', 'id="incBtnBajas"'):
            self.assertIn(ident, self.html)

    def test_los_motivos_salen_del_servidor_no_escritos_a_mano(self):
        self.assertIn("var INC_BAJA_MOTIVOS = {{ (motivos_baja or [])|tojson }};", self.html)
        self.assertNotIn("Ya está regularizado en Random", self.html)

    def test_dar_de_baja_esta_en_las_dos_tablas_y_eliminar_sigue_solo_para_admins(self):
        self.assertIn('data-accion="baja" data-id="', self.html)           # tabla principal
        self.assertIn('data-accion="baja"><i class="bi bi-archive', self.html)   # pendientes
        self.assertIn("(incCanDelete ? '<li><hr class=\"dropdown-divider\"></li>", self.html)
        self.assertIn("if(h.accion === 'ver' && incCanDelete)", self.html)

    def test_pendientes_usa_el_formato_de_retiros(self):
        self.assertIn('id="incConcTabla"', self.html)
        self.assertIn("table-mobile-cards rm-table", self.html)
        self.assertIn("data-alerta=", self.html)
        # ya no se arma la tabla vieja de cabecera negra propia
        self.assertNotIn("'<div class=\"inc-wrap\"><table class=\"inc-tabla\">", self.html)

    def test_desglosar_esta_en_las_dos_tablas_y_solo_con_dos_o_mas_unidades(self):
        self.assertIn('id="incDesgloseModal"', self.html)
        # tabla principal: solo si la fila tiene 2+ unidades
        self.assertIn("(parseInt(r.cantidad, 10) || 0) >= 2 ? '<li><button type=\"button\" class=\"dropdown-item inc-acc\" data-accion=\"desglosar\"", self.html)
        # pendientes: solo los que no tienen incidencia y traen 2+ unidades
        self.assertIn("if(h.baja_hallazgo && h.accion === 'registrar' && (parseInt(h.cantidad, 10) || 0) >= 2)", self.html)
        self.assertIn("data-accion=\"desglosar\"><i class=\"bi bi-diagram-3", self.html)

    def test_el_desglose_valida_la_suma_y_el_motivo_antes_de_enviar(self):
        bloque = self.html.split("function incDesEnviar(){")[1].split("// Todo conectado por código")[0]
        self.assertIn("suma !== _incDes.total", bloque)
        self.assertIn("INC_DES_MIN_MOTIVO", bloque)
        self.assertIn("/mantenciones/api/incidencias/desglosar", bloque)

    def test_no_se_usan_dialogos_nativos(self):
        # REGLA #1: alert/confirm/prompt nativos prohibidos (solo ilus*).
        codigo = re.sub(r"//[^\n]*", "", self.html)
        for nativo in ("alert", "confirm", "prompt"):
            self.assertIsNone(re.search(r"(?<![\w.$])" + nativo + r"\(", codigo),
                              f"hay un {nativo}() nativo en la plantilla")

    def test_los_datos_del_historial_van_escapados(self):
        # Todo texto que viene del servidor o del usuario pasa por incEsc.
        bloque = self.html.split("function incBajasPintar(d){")[1].split("function incPedirTexto")[0]
        for campo in ("b.motivo_texto", "b.baja_by", "b.descripcion", "b.sku", "b.reactivada_motivo"):
            self.assertRegex(bloque, r"incEsc\(" + re.escape(campo))

    @unittest.skipUnless(shutil.which("node"), "node no está instalado")
    def test_el_javascript_de_la_plantilla_no_tiene_errores_de_sintaxis(self):
        try:
            import jinja2
        except ImportError:
            self.skipTest("jinja2 no está instalado")
        bloques = re.findall(r"<script>(.*?)</script>", self.html, re.S)
        env = jinja2.Environment()
        env.globals["url_for"] = lambda *a, **k: "/x"

        class P(dict):
            __getattr__ = dict.get

        for i, b in enumerate(bloques):
            js = env.from_string(b).render(permissions=P(admin=True, superadmin=False),
                                           motivos_baja=[{"codigo": "otro", "texto": "Otro motivo"}])
            with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False, encoding="utf-8") as fh:
                fh.write(js)
                ruta = fh.name
            try:
                res = subprocess.run(["node", "--check", ruta], capture_output=True, text=True)
                self.assertEqual(res.returncode, 0, res.stderr[:800])
            finally:
                os.unlink(ruta)


if __name__ == "__main__":
    unittest.main()

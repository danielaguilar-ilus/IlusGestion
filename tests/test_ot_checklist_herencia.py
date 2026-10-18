# -*- coding: utf-8 -*-
"""🧩 2026-10-17 — Checklist HEREDADO por sistema (Daniel, NVI 687 del evento de Felipe Fierro).

Daniel: «si tengo un producto selector de peso, vamos a buscarle el selector de peso, pero si tengo una trotadora, no
le puedo buscar la piola de la trotadora, sino revisarle la cinta… esos son los detalles que quiero ir heredando de las
plantillas». Decidió (17-oct): base del trabajo + UN bloque del sistema (≤8 ítems c/u), «Estructura y tapiz» solo si
la base marca observación, excepción por producto sobre la clase, accesorios por línea con conteo.

Se prueba: la regla pura (_chk_herencia_armar) con la NVI 687 completa, la semilla de bloques, la excepción por
producto (quién y cuándo, bitácora antes de escribir), el armado de la OT en el servidor (no confía en el navegador),
la facturación de la OT interna con NVI, el asistente y la ficha (bloque condicional plegado).
Sin BD, sin Flask, sin ERP (REGLA #4.1).  Correr:  python -m pytest tests/test_ot_checklist_herencia.py -q
"""
import ast
import re
import unittest

from tests.test_fac_libro_saldo import _exec
from tests.test_ot_saldo_servicio import _arbol, _decoradores, _fuente, _leer

M = _leer("templates/ot2/_modal_crear.html")
D = _leer("templates/ot2/detalle.html")

BASE = {"id": 10, "nombre": "Control de Calidad — Registro por equipo (bodega)"}
COND = {"id": 11, "nombre": "Control de Calidad — Detalle estructura y tapiz"}
SELECTOR = {"id": 12, "nombre": "Control de Calidad — Detalle carga, cables y movimiento"}
DISCOS = {"id": 13, "nombre": "Control de Calidad — Detalle carga de discos"}
LIBRES = {"id": 14, "nombre": "Control de Calidad — Detalle cargas libres (por línea)"}
POLEAS = {"id": 15, "nombre": "Control de Calidad — Detalle poleas ajustables"}
BANCOS = {"id": 16, "nombre": "Control de Calidad — Detalle bancos y ajustes"}


def _regla(ambito, clave, rol, b):
    return {"ambito": ambito, "clave": clave, "rol": rol, "plantilla_id": b["id"], "nombre": b["nombre"]}


REGLAS = [
    _regla("tipo", "control_calidad", "base", BASE),
    _regla("tipo", "control_calidad", "condicional", COND),
    _regla("clase", "selectorizador_pesos", "sistema", SELECTOR),
    _regla("clase", "booty_builder_p", "sistema", DISCOS),
    _regla("clase", "accesorio", "sistema", LIBRES),
    _regla("clase", "rack_accesorios", "sistema", LIBRES),
    _regla("clase", "dual_cable_cross", "sistema", POLEAS),
    _regla("clase", "bancos_plano_ajustable", "sistema", BANCOS),
    # La «Selectorized Pendulum Kick Back» es de la línea Glute Builder (clase de discos) pero lleva selector.
    _regla("producto", "spkb", "sistema", SELECTOR),
]


def _c(slug, nombre, repetible=False):
    return {"clase": slug, "clase_nombre": nombre, "repetible": repetible}


# Clases SUPUESTAS de las 19 líneas de la NVI 687 (se confirman contra el catálogo en producción).
NVI_687 = {
    "1120100884": _c("rack_accesorios", "Rack de accesorios"),            # Set mancuernas 2,5–25 kg + rack
    "1121100986": _c("selectorizador_pesos", "Selectorizador de pesos"),  # Optimal Leg Extension/Curl
    "1121100987": _c("selectorizador_pesos", "Selectorizador de pesos"),  # Optimal Chest Press
    "1121100988": _c("selectorizador_pesos", "Selectorizador de pesos"),  # Optimal Shoulder Press
    "1121100993": _c("selectorizador_pesos", "Selectorizador de pesos"),  # Optimal Lat Pulldown
    "1121101056": _c("booty_builder_p", "Booty Builder P"),               # Glute Hack Squat
    "1121100901700": _c("booty_builder_p", "Booty Builder P"),            # Glute Press
    "SPKB": _c("booty_builder_p", "Booty Builder P"),                     # Selectorized Pendulum Kick Back
    "PSP": _c("booty_builder_p", "Booty Builder P"),                      # Pendulum Squat Pro
    "ABDUS074": _c("booty_builder_p", "Booty Builder P"),                 # 3D Multi Abductor
    "350": _c("booty_builder_p", "Booty Builder P"),                      # Gymleco Biceps Curl 300
    "334": _c("booty_builder_p", "Booty Builder P"),                      # Gymleco Side Lateral 300
    "326": _c("booty_builder_p", "Booty Builder P"),                      # Gymleco Wide Chest 300
    "1127100789": _c("bancos_plano_ajustable", "Bancos plano / ajustable"),
    "1118100901905": _c("accesorio", "Accesorio", True),                  # Par discos 15 kg
    "1118100901907": _c("accesorio", "Accesorio", True),                  # Par discos 25 kg
    "1118100901904": _c("accesorio", "Accesorio", True),                  # Par discos 10 kg
    "1121100909": _c("dual_cable_cross", "Dual cable Cross"),             # Optimal Dual Cable
    "1121101061": _c("dual_cable_cross", "Dual cable Cross"),             # Optimal Dual Adjustable Pulley
}


class TestReglaPura(unittest.TestCase):
    def setUp(self):
        self.armar = _exec("_chk_herencia_armar", {})

    def test_nvi_687_cada_linea_hereda_su_bloque(self):
        r = self.armar(list(NVI_687), NVI_687, REGLAS)
        self.assertTrue(r["activo"])
        esperado = {
            "1120100884": LIBRES["id"], "1121100986": SELECTOR["id"], "1121100987": SELECTOR["id"],
            "1121100988": SELECTOR["id"], "1121100993": SELECTOR["id"], "1121101056": DISCOS["id"],
            "1121100901700": DISCOS["id"], "SPKB": SELECTOR["id"], "PSP": DISCOS["id"], "ABDUS074": DISCOS["id"],
            "350": DISCOS["id"], "334": DISCOS["id"], "326": DISCOS["id"], "1127100789": BANCOS["id"],
            "1118100901905": LIBRES["id"], "1118100901907": LIBRES["id"], "1118100901904": LIBRES["id"],
            "1121100909": POLEAS["id"], "1121101061": POLEAS["id"],
        }
        self.assertEqual({k: v["sistema"]["id"] for k, v in r["skus"].items()}, esperado)
        for v in r["skus"].values():   # base y condicional para todos
            self.assertEqual((v["base"]["id"], v["condicional"]["id"]), (BASE["id"], COND["id"]))

    def test_la_excepcion_del_producto_gana_a_su_clase(self):
        r = self.armar(["SPKB"], NVI_687, REGLAS)["skus"]["SPKB"]
        self.assertEqual((r["sistema"]["id"], r["sistema"]["origen"]), (SELECTOR["id"], "producto"))
        self.assertEqual(r["sistema_clase"]["id"], DISCOS["id"])     # lo que habría dado su clase

    def test_accesorios_van_por_linea(self):
        r = self.armar(["1118100901905", "1121100987"], NVI_687, REGLAS)["skus"]
        self.assertIs(r["1118100901905"]["repetible"], True)
        self.assertIs(r["1121100987"]["repetible"], False)

    def test_sin_clase_queda_solo_con_la_base(self):
        r = self.armar(["XYZ-1"], NVI_687, REGLAS)["skus"]["XYZ-1"]
        self.assertIsNone(r["sistema"])
        self.assertIsNone(r["clase"])
        self.assertEqual(r["base"]["id"], BASE["id"])

    def test_sin_base_el_tipo_no_usa_herencia(self):
        r = self.armar(["1121100987"], NVI_687, [x for x in REGLAS if x["ambito"] != "tipo"])
        self.assertFalse(r["activo"])

    def test_mayusculas_y_repetidos(self):
        r = self.armar(["spkb", "SPKB", " spkb "], {"spkb": NVI_687["SPKB"]}, REGLAS)
        self.assertEqual(list(r["skus"]), ["spkb", "SPKB"])          # cada clave escrita, una vez
        self.assertEqual(r["skus"]["spkb"]["sistema"]["origen"], "producto")

    def test_bloques_para_elegir_unicos_y_ordenados(self):
        r = self.armar([], {}, REGLAS)
        ids = [b["id"] for b in r["bloques_sistema"]]
        self.assertEqual(sorted(ids), sorted({SELECTOR["id"], DISCOS["id"], LIBRES["id"], POLEAS["id"], BANCOS["id"]}))
        nombres = [b["nombre"].lower() for b in r["bloques_sistema"]]
        self.assertEqual(nombres, sorted(nombres))


def _constantes_chk():
    _codigo, arbol = _arbol()
    amb = {}
    for n in arbol.body:
        if isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id.startswith("_CHK_") for t in n.targets):
            exec(compile(ast.Module(body=[n], type_ignores=[]), "<app>", "exec"), amb)
    return amb


class TestBloques(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.k = _constantes_chk()

    def test_ninguno_pasa_de_8_items_y_tipos_validos(self):   # «amigable»
        validos = {"check", "texto", "sino", "numero", "verificacion", "gps", "lista", "fecha_hora", "foto",
                   "serie", "asistencia"}
        for nombre, _d, items in self.k["_CHK_BLOQUES_SEED"]:
            self.assertLessEqual(len(items), 8, nombre)
            self.assertEqual([it[0] for it in items], list(range(1, len(items) + 1)), nombre)
            for it in items:
                self.assertIn(it[3], validos, (nombre, it[1]))
                self.assertEqual(len(it), 10)

    def test_nombres_unicos_de_control_de_calidad(self):
        nombres = [b[0] for b in self.k["_CHK_BLOQUES_SEED"]]
        self.assertEqual(len(nombres), len(set(nombres)))
        self.assertTrue(all(n.startswith("Control de Calidad — ") for n in nombres))

    def test_a_la_trotadora_se_le_revisa_la_cinta_no_la_piola(self):
        trot = next(b for b in self.k["_CHK_BLOQUES_SEED"] if "trotadora" in b[0].lower())
        textos = " ".join((it[1] + " " + (it[2] or "")).lower() for it in trot[2])
        self.assertIn("cinta", textos)
        self.assertNotIn("piola", textos)
        self.assertNotIn("cable de acero", textos)

    def test_el_mapa_de_clases_apunta_a_bloques_que_existen(self):
        nombres = {b[0] for b in self.k["_CHK_BLOQUES_SEED"]} | {self.k["_CHK_SELECTOR"]}
        for slug, bloque in self.k["_CHK_HERENCIA_CLASES_SEED"].items():
            self.assertIn(bloque, nombres, slug)
        cat = _leer("catalogo_module.py")
        for slug in self.k["_CHK_HERENCIA_CLASES_SEED"]:
            self.assertTrue(f'"{slug}"' in cat or f"'{slug}'" in cat, slug)

    def test_reusa_las_plantillas_de_la_nvi_552(self):
        app = _leer("app.py")
        for n in (self.k["_CHK_BASE_CONTROL_CALIDAD"], self.k["_CHK_COND_CONTROL_CALIDAD"], self.k["_CHK_SELECTOR"]):
            self.assertIn(f'("{n}", "inspeccion",', app)

    def test_semilla_barata_y_sin_pisar_ediciones(self):   # REGLA #18
        f = _fuente("_ensure_checklist_herencia")
        self.assertIn("CREATE TABLE IF NOT EXISTS mant_checklist_herencia", f)
        self.assertIn("UNIQUE KEY uq_chk_herencia (ambito, clave, tipo_ot, rol)", f)
        self.assertIn("SELECT id FROM mant_tarea_plantillas WHERE nombre=%s LIMIT 1", f)
        self.assertIn("WHERE tipo_ot='control_calidad'", f)
        self.assertIn("INSERT IGNORE INTO mant_checklist_herencia", f)
        self.assertIn("_chk_her = _ensure_checklist_herencia()", _leer("app.py"))


class _Req:
    def __init__(self, d):
        self._d = d

    def get_json(self, silent=False):
        return self._d


class TestExcepcionPorProducto(unittest.TestCase):
    def _llamar(self, cuerpo):
        log = []
        armar = _exec("_chk_herencia_armar", {})
        amb = {
            "request": _Req(cuerpo), "jsonify": lambda d: d, "current_username": lambda: "daniel",
            "_chk_herencia_resolver": lambda skus, tipo: armar(skus, NVI_687, REGLAS if tipo == "control_calidad" else []),
            "_mant_log": lambda *a: log.append(("log",) + a),
            "mysql_execute": lambda sql, params=None: log.append(("sql", " ".join(sql.split()), params)),
        }
        return _exec("ot2_api_checklist_heredado_producto", amb)(), log

    def test_guarda_quien_y_deja_bitacora_antes(self):
        r, log = self._llamar({"sku": "PSP", "tipo": "control_calidad", "plantilla_id": SELECTOR["id"]})
        self.assertTrue(r["ok"])
        self.assertEqual([x[0] for x in log], ["log", "sql"])            # REGLA #5: bitácora antes
        self.assertIn("ON DUPLICATE KEY UPDATE", log[1][1])
        self.assertEqual(log[1][2], ("PSP", "control_calidad", SELECTOR["id"], "daniel"))

    def test_solo_bloques_de_sistema(self):
        r, log = self._llamar({"sku": "PSP", "tipo": "control_calidad", "plantilla_id": BASE["id"]})
        self.assertEqual(r[1], 400)
        self.assertEqual(log, [])

    def test_tipo_sin_herencia_no_escribe(self):
        r, log = self._llamar({"sku": "PSP", "tipo": "capacitacion", "plantilla_id": SELECTOR["id"]})
        self.assertEqual(r[1], 400)
        self.assertEqual(log, [])

    def test_vacio_vuelve_a_lo_de_su_clase(self):
        r, log = self._llamar({"sku": "SPKB", "tipo": "control_calidad", "plantilla_id": ""})
        self.assertTrue(r["ok"])
        self.assertTrue(log[1][1].startswith("DELETE FROM mant_checklist_herencia WHERE ambito='producto'"))

    def test_decoradores(self):   # REGLA #26: ninguna ruta financiera, pero escribir es solo de gestión
        self.assertIn("_no_tecnico", _decoradores("ot2_api_checklist_heredado_producto"))
        self.assertIn("_mant_required", _decoradores("ot2_api_checklist_heredado"))


class TestCrearOT(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.f = _fuente("_ot2_crear_core")

    def test_la_fila_automatica_se_resuelve_en_el_servidor(self):
        self.assertIn('"checklist_auto": bool(_tm.get("checklist_auto"))', self.f)
        self.assertIn("_chk_herencia_resolver([_t[\"sku\"] for _t in _chk_auto], tipo_ot)", self.f)
        self.assertIn("_chk_plantillas_validas(", self.f)

    def test_copia_base_bloque_y_condicional_no_obligatorio(self):
        self.assertIn("for _tm_pid in _tm_ids:", self.f)
        self.assertIn("0 if _tm_cond else (it.get(\"obligatoria\") or 0)", self.f)
        self.assertIn("_tm_ids = [_tm[\"plantilla_id\"]]", self.f)       # la plantilla única de siempre sigue

    def test_validar_no_cruza_categorias(self):
        f = _fuente("_chk_plantillas_validas")
        self.assertIn("_categoria_admin_para_tipo(tipo_ot)", f)
        self.assertIn("COALESCE(p.activa, 1) = 1", f)

    def test_ot_interna_con_su_nvi_no_se_factura(self):
        app = _leer("app.py")
        i = app.index('elif es_interna and _fin_nudo and (_fin_tido or "").upper() == "NVI":')
        j = app.index('elif _fin_nudo and (_fin_tido or "").upper() in ("NVV", "NVI"):')
        self.assertLess(i, j)


class TestPantallas(unittest.TestCase):
    def test_asistente_hereda_desde_documento_y_catalogo(self):
        self.assertIn("_o2mAgregarItemsHeredando(items, true);", M)          # catálogo / documento (buscador)
        self.assertIn("_o2mAgregarItemsHeredando(items, false);", M)         # líneas del documento asociado
        self.assertIn("'/ot/api/checklist-heredado?tipo='", M)
        self.assertIn("var porLinea = h.repetible === true;", M)
        self.assertIn("' (' + u + ' de ' + it.qty + ')'", M)               # una tarjeta por unidad
        self.assertIn("porSku[it.sku].qty += q;", M)                        # mismo producto en dos líneas suma
        self.assertIn("function comoSiempre()", M)                          # sin herencia: igual que antes
        self.assertIn("Recordar para este producto", M)
        self.assertIn("setBloqueTareaManual:setBloqueTareaManual, recordarBloqueProducto:recordarBloqueProducto", M)

    def test_asistente_sin_popups_nativos(self):   # REGLA #1
        i = M.index("function _o2mAgregarItemsHeredando(")
        j = M.index("function recordarBloqueProducto(")
        bloque = M[i:j + 2500]
        self.assertIsNone(re.search(r"(?<![\w.])(alert|confirm|prompt)\(", bloque))

    def test_ficha_pliega_el_condicional(self):
        self.assertIn("return g.condicional ? otdChkCondEnvolver(g, grupos, _html) : _html;", D)
        self.assertIn("op.startsWith('no apto') || op.startsWith('apto con')", D)
        f = _fuente("mant_visita_checklist_get")
        self.assertIn('grp["condicional"] = bool(', f)
        self.assertIn('not any(x.get("obligatoria") for x in grp["tareas"])', f)


if __name__ == "__main__":
    unittest.main()

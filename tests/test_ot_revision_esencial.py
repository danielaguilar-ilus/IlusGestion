# -*- coding: utf-8 -*-
"""✂️ 2026-10-18 — Revisión de equipos, ETAPA 2: bloques ESENCIALES y resultado por producto.

Daniel probó en terreno: «encuentro que hay bastantes tareas… es foto por foto por foto… ideal que con que tome una
foto yo tengo y las otras las vas alimentando del lado… muy largo. Dejémoslo esencial. Pero que si conseguimos una
falla, sea porque el técnico no la quiso identificar». Y: «arriba debe tener las opciones para dar de baja, para poner
fuera de servicio, para decir que el producto no está».

Se prueba:
  1. La versión esencial exacta de los 10 bloques (opciones nuevas del Resultado, 1 sola foto obligatoria, observación
     obligatoria siempre, foto_si_hallazgo) y que la versión larga quedó guardada como «anterior».
  2. La MIGRACIÓN de producción en un mundo en memoria: solo se migra lo que nadie editó, con la bitácora antes, en
     una transacción, idempotente y barata con la marca puesta.
  3. foto_si_hallazgo: columna, copia en TODOS los INSERT que copian ítems de plantilla, editor de plantillas, y
     candados (foto del hallazgo pendiente hasta que haya foto).
  4. «No llegó»: fuente única (_ot_chk_revision_pendientes / _ot_items_manuales_no_llego) llamada por TODOS los
     candados de firma y cierre, y el JS de la ficha.
  5. Regla dura: servidor y ficha.
  6. otdChkBaseMarcoObs con las opciones nuevas.
  7. Espejo JS ↔ Python: otdChkEvaluar (detalle.html) y _chk_revision_evaluar (app.py) dan lo mismo en los mismos casos.

Sin BD, sin Flask, sin ERP (REGLA #4.1). Las funciones JS corren de verdad en node si está instalado.
Correr:  python -m pytest tests/test_ot_revision_esencial.py -q -p no:warnings
"""
import ast
import copy
import json
import re
import unicodedata
import unittest

from tests.test_ot_saldo_servicio import _arbol, _fuente, _leer
from tests.test_ot_revision_v3 import HAY_NODE, _js_funcion, _node

D = _leer("templates/ot2/detalle.html")
P = _leer("templates/mantenciones/plantillas.html")


def _cargar(funcs=(), consts=(), amb=None):
    """Ejecuta en un mismo espacio las funciones (sin decoradores) y constantes de app.py indicadas."""
    amb = {} if amb is None else amb
    amb.setdefault("json", json)
    amb.setdefault("unicodedata", unicodedata)
    amb.setdefault("re", re)
    _codigo, arbol = _arbol()
    for n in arbol.body:
        if isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id in consts for t in n.targets):
            exec(compile(ast.Module(body=[n], type_ignores=[]), "<app>", "exec"), amb)
    for n in arbol.body:
        if isinstance(n, ast.FunctionDef) and n.name in funcs:
            n2 = ast.parse(ast.unparse(n)).body[0]
            n2.decorator_list = []
            exec(compile(ast.Module(body=[n2], type_ignores=[]), "<app>", "exec"), amb)
    faltan = [x for x in tuple(funcs) + tuple(consts) if x not in amb]
    assert not faltan, f"no están en app.py: {faltan}"
    return amb


_CONSTS = ("_CHK_RESULTADO_OPCIONES_ANTERIOR", "_CHK_BLOQUES_SEED_ANTERIOR", "_CHK_RESULTADO_OPCIONES",
           "_CHK_BLOQUES_SEED", "_CHK_BASE_CONTROL_CALIDAD", "_CHK_COND_CONTROL_CALIDAD", "_CHK_SELECTOR",
           "_CHK_RES_APTO", "_CHK_RES_APTO_OBS", "_CHK_RES_NO_LLEGO", "_CHK_MOTIVO_TXT", "_CHK_ESENCIAL_MARCA")
_PURAS = ("_tarea_vj", "_chk_txt_norm", "_chk_opciones", "_chk_es_resultado", "_chk_opcion", "_chk_valor_verif",
          "_chk_grupo_clave", "_chk_revision_evaluar", "_chk_resultado_rechazo", "_chk_titulos",
          "_chk_esencial_plan", "_fsh_ins")


def _k():
    return _cargar(_PURAS, _CONSTS)


# ═══════════════════════════════ 1) versión esencial ═══════════════════════════════
class TestVersionEsencial(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.k = _k()
        cls.seed = {b[0]: b for b in cls.k["_CHK_BLOQUES_SEED"]}

    def test_opciones_del_resultado(self):
        self.assertEqual(json.loads(self.k["_CHK_RESULTADO_OPCIONES"]),
                         ["Apto", "Apto con observaciones", "Reparar", "Dar de baja", "Fuera de servicio",
                          "No llegó", "Pendiente de diagnóstico"])

    def test_cantidad_de_items_por_bloque(self):   # «dejémoslo esencial»
        esperado = {
            "Revisión — Registro por equipo": 5, "Revisión — Estructura y tapiz": 4,
            "Revisión — Selector de peso": 5, "Revisión — Carga de discos": 5, "Revisión — Poleas ajustables": 5,
            "Revisión — Bancos y ajustes": 3, "Revisión — Cargas libres (por línea)": 3,
            "Revisión — Trotadora (cinta y motor)": 5, "Revisión — Bicicleta (transmisión y consola)": 4,
            "Revisión — Rack y soportes": 4,
        }
        self.assertEqual({n: len(b[2]) for n, b in self.seed.items()}, esperado)
        for b in self.k["_CHK_BLOQUES_SEED"]:
            for it in b[2]:
                self.assertEqual(len(it), 11, (b[0], it[1]))
                self.assertIn(it[10], (0, 1))

    def test_una_sola_foto_obligatoria_y_es_la_general(self):   # «con que tome una foto yo tengo»
        fotos_fijas = [(b[0], it[1]) for b in self.k["_CHK_BLOQUES_SEED"] for it in b[2]
                       if it[3] == "foto" or it[5]]
        self.assertEqual(fotos_fijas, [("Revisión — Registro por equipo", "Foto general del equipo")])
        base = self.seed["Revisión — Registro por equipo"][2]
        self.assertEqual([it[1] for it in base], ["Foto general del equipo", "N° de serie",
                                                  "¿Faltan piezas o accesorios?", "Resultado del control",
                                                  "Observación del equipo"])
        self.assertEqual([it[4] for it in base], [1, 1, 1, 1, 1])        # observación obligatoria SIEMPRE
        resultado = base[3]
        self.assertEqual((resultado[3], resultado[9], resultado[10]),
                         ("lista", self.k["_CHK_RESULTADO_OPCIONES"], 1))
        self.assertIn("No llegó", base[0][2])                             # con «No llegó» no se pide

    def test_foto_si_hallazgo_segun_la_propuesta(self):
        fsh = {(b[0], it[0]): it[10] for b in self.k["_CHK_BLOQUES_SEED"] for it in b[2]}
        sel = "Revisión — Selector de peso"
        self.assertEqual([fsh[(sel, i)] for i in range(1, 6)], [1, 0, 1, 0, 0])
        trot = "Revisión — Trotadora (cinta y motor)"
        self.assertEqual([fsh[(trot, i)] for i in range(1, 6)], [0, 1, 1, 0, 1])
        self.assertEqual([fsh[("Revisión — Estructura y tapiz", i)] for i in range(1, 5)], [1, 1, 1, 1])
        self.assertEqual([fsh[("Revisión — Rack y soportes", i)] for i in range(1, 5)], [1, 1, 1, 1])
        for b in self.k["_CHK_BLOQUES_SEED"]:                              # solo verificación o lista
            for it in b[2]:
                if it[10]:
                    self.assertIn(it[3], ("verificacion", "lista"), (b[0], it[1]))

    def test_criterios_completos_y_repuestos(self):
        for b in self.k["_CHK_BLOQUES_SEED"]:
            for it in b[2]:
                self.assertTrue((it[2] or "").strip(), (b[0], it[1]))        # ningún ítem sin criterio
            if b[0] != "Revisión — Registro por equipo":
                self.assertIn("Repuestos sugeridos:", b[1], b[0])
        piola = self.seed["Revisión — Selector de peso"][2][0]
        self.assertEqual(piola[1], "CRÍTICO · Piola o correa y terminales")
        self.assertIn("Con Falla, el equipo no se usa.", piola[2])

    def test_la_version_larga_queda_como_anterior(self):
        ant = {b[0]: b for b in self.k["_CHK_BLOQUES_SEED_ANTERIOR"]}
        self.assertEqual(set(ant), set(self.seed))
        self.assertEqual(len(ant["Revisión — Registro por equipo"][2]), 8)
        self.assertEqual(ant["Revisión — Registro por equipo"][2][6][9], self.k["_CHK_RESULTADO_OPCIONES_ANTERIOR"])
        self.assertTrue(all(len(it) == 10 for b in ant.values() for it in b[2]))


# ═══════════════════════════════ 2) migración ═══════════════════════════════
def _bd_desde(bloques, pid0=100, editar=None):
    """Bloques de la base sembrados con `bloques` (formato seed). editar: {nombre: f(items)->items}."""
    filas, items, iid = [], [], 1
    for i, (nombre, desc, its) in enumerate(bloques):
        pid = pid0 + i
        filas.append({"id": pid, "nombre": nombre, "descripcion": desc})
        lista = [{"plantilla_id": pid, "id": None, "orden": it[0], "titulo": it[1]} for it in its]
        if editar and nombre in editar:
            lista = editar[nombre](lista)
        for x in lista:
            x["id"] = iid
            iid += 1
        items.extend(lista)
    return filas, items


class TestPlanDeMigracion(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.k = _k()

    def test_sin_editar_se_migra_con_su_descripcion(self):
        bloques, items = _bd_desde(self.k["_CHK_BLOQUES_SEED_ANTERIOR"])
        plan = self.k["_chk_esencial_plan"](bloques, items)
        self.assertEqual(len(plan), 10)
        self.assertTrue(all(p["accion"] == "migrar" for p in plan))
        nuevas = {b[0]: b[1] for b in self.k["_CHK_BLOQUES_SEED"]}
        self.assertTrue(all(p["desc_nueva"] == nuevas[p["nombre"]] for p in plan))

    def test_un_titulo_editado_no_se_toca(self):
        def editar(lista):
            lista[2]["titulo"] = "Foto del mecanismo (Jaizer)"
            return lista
        bloques, items = _bd_desde(self.k["_CHK_BLOQUES_SEED_ANTERIOR"],
                                   editar={"Revisión — Registro por equipo": editar})
        plan = {p["nombre"]: p["accion"] for p in self.k["_chk_esencial_plan"](bloques, items)}
        self.assertEqual(plan["Revisión — Registro por equipo"], "editado")
        self.assertEqual(plan["Revisión — Selector de peso"], "migrar")

    def test_reordenado_o_con_un_item_mas_tampoco(self):
        def reordenar(lista):
            lista[0]["orden"], lista[1]["orden"] = lista[1]["orden"], lista[0]["orden"]
            return lista

        def agregar(lista):
            return lista + [{"plantilla_id": lista[0]["plantilla_id"], "id": None, "orden": 99, "titulo": "Extra"}]
        bloques, items = _bd_desde(self.k["_CHK_BLOQUES_SEED_ANTERIOR"],
                                   editar={"Revisión — Selector de peso": reordenar,
                                           "Revisión — Rack y soportes": agregar})
        plan = {p["nombre"]: p["accion"] for p in self.k["_chk_esencial_plan"](bloques, items)}
        self.assertEqual(plan["Revisión — Selector de peso"], "editado")
        self.assertEqual(plan["Revisión — Rack y soportes"], "editado")

    def test_ya_esencial_y_descripcion_editada(self):
        bloques, items = _bd_desde(self.k["_CHK_BLOQUES_SEED"])
        self.assertTrue(all(p["accion"] == "ya" for p in self.k["_chk_esencial_plan"](bloques, items)))
        bloques, items = _bd_desde(self.k["_CHK_BLOQUES_SEED_ANTERIOR"])
        bloques[0]["descripcion"] = "Descripción escrita por Lenin"
        p0 = self.k["_chk_esencial_plan"](bloques, items)[0]
        self.assertEqual((p0["accion"], p0["desc_nueva"]), ("migrar", None))   # los ítems sí, el texto suyo no


class _Mundo:
    """Base en memoria con lo justo para _ensure_bloques_revision_esencial."""

    def __init__(self, k, editar=None):
        self.k = k
        bloques, items = _bd_desde(k["_CHK_BLOQUES_SEED_ANTERIOR"], editar=editar)
        self.plantillas = {b["id"]: dict(b) for b in bloques}
        self.items = {}
        for it in items:
            self.items.setdefault(it["plantilla_id"], []).append(
                {"titulo": it["titulo"], "orden": it["orden"], "id": it["id"], "foto_si_hallazgo": 0})
        self.logs, self.sql, self.consultas = [], [], 0

    # mysql_* del pool
    def fetchone(self, sql, params=None):
        self.consultas += 1
        if "FROM mant_logs" in sql:
            hay = any(l[0] == "sistema" and l[2] == params[2] for l in self.logs)
            return {"id": 1} if hay else None
        raise AssertionError(sql)

    def fetchall(self, sql, params=None):
        self.consultas += 1
        if "FROM mant_tarea_plantillas" in sql:
            return [dict(p) for p in self.plantillas.values() if p["nombre"] in params]
        if "FROM mant_tarea_plantilla_items" in sql:
            return [dict(it, plantilla_id=pid) for pid, its in self.items.items() if pid in params for it in its]
        raise AssertionError(sql)

    def execute(self, sql, params=None):
        self.consultas += 1
        if "INSERT INTO mant_logs" in sql:
            self.logs.append(params[:4] if len(params) > 4 else params)
            return 1
        raise AssertionError(sql)

    def mant_log(self, entidad, eid, accion, detalle=""):
        self.logs.append((entidad, eid, accion, detalle))

    def get_mysql(self):
        return _Conn(self)


class _Conn:
    def __init__(self, m):
        self.m = m
        self.tx_items = copy.deepcopy(m.items)
        self.tx_plant = copy.deepcopy(m.plantillas)
        self.tx_logs = []
        self.res = []

    def cursor(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        s = " ".join(sql.split())
        self.m.sql.append(s)
        if s.startswith("SELECT titulo FROM mant_tarea_plantilla_items") and s.endswith("FOR UPDATE"):
            self.res = [{"titulo": x["titulo"]} for x in
                        sorted(self.tx_items.get(params[0], []), key=lambda x: (x["orden"], x["id"]))]
        elif s.startswith("INSERT INTO mant_logs"):
            self.tx_logs.append(params[:4])
        elif s.startswith("DELETE FROM mant_tarea_plantilla_items"):
            self.tx_items[params[0]] = []
        elif s.startswith("INSERT INTO mant_tarea_plantilla_items"):
            self.assertTrue("foto_si_hallazgo" in s)
            pid = params[0]
            self.tx_items.setdefault(pid, []).append(
                {"titulo": params[2], "orden": params[1], "id": 10000 + len(self.tx_items[pid]),
                 "foto_si_hallazgo": params[11]})
        elif s.startswith("UPDATE mant_tarea_plantillas SET descripcion"):
            self.tx_plant[params[1]]["descripcion"] = params[0]
        else:
            raise AssertionError(s)

    @staticmethod
    def assertTrue(x):
        assert x

    def fetchall(self):
        return self.res

    def commit(self):
        self.m.items = self.tx_items
        self.m.plantillas = self.tx_plant
        self.m.logs.extend(self.tx_logs)

    def rollback(self):
        self.tx_items = copy.deepcopy(self.m.items)
        self.tx_logs = []

    def close(self):
        pass


class TestMigracionEnMemoria(unittest.TestCase):
    def _correr(self, mundo, fsh=True):
        amb = _cargar(_PURAS + ("_ensure_bloques_revision_esencial",), _CONSTS)
        amb.update({
            "_fsh_ok": lambda: fsh, "_ensure_mant_logs_entidad_varchar": lambda: None,
            "mysql_fetchone": mundo.fetchone, "mysql_fetchall": mundo.fetchall, "mysql_execute": mundo.execute,
            "get_mysql": mundo.get_mysql, "_mant_log": mundo.mant_log, "print": lambda *a, **k: None,
        })
        return amb["_ensure_bloques_revision_esencial"]()

    def test_migra_lo_no_editado_conserva_lo_editado_y_deja_marca(self):
        k = _k()

        def editar(lista):
            lista[0]["titulo"] = "Cable revisado por Jaizer"
            return lista
        m = _Mundo(k, editar={"Revisión — Rack y soportes": editar})
        out = self._correr(m)
        self.assertEqual(len(out["migrados"]), 9)
        self.assertEqual(out["editados"], ["Revisión — Rack y soportes"])
        ese = {b[0]: b for b in k["_CHK_BLOQUES_SEED"]}
        for pid, p in m.plantillas.items():
            titulos = [x["titulo"] for x in sorted(m.items[pid], key=lambda x: (x["orden"], x["id"]))]
            if p["nombre"] == "Revisión — Rack y soportes":
                self.assertEqual(titulos[0], "Cable revisado por Jaizer")         # NO se tocó
            else:
                self.assertEqual(titulos, [it[1] for it in ese[p["nombre"]][2]])
                self.assertEqual([x["foto_si_hallazgo"] for x in sorted(m.items[pid], key=lambda x: x["orden"])],
                                 [it[10] for it in ese[p["nombre"]][2]])
                self.assertEqual(p["descripcion"], ese[p["nombre"]][1])
        acciones = [l[2] for l in m.logs]
        self.assertEqual(acciones.count("revision_esencial"), 9)
        self.assertEqual(acciones.count("revision_esencial_omitida"), 1)
        self.assertEqual(acciones[-1], "revision_esencial_aplicada")              # la marca, al final

    def test_bitacora_antes_y_todo_en_una_transaccion(self):   # REGLA #5
        m = _Mundo(_k())
        self._correr(m)
        s = m.sql
        i_log = next(i for i, x in enumerate(s) if x.startswith("INSERT INTO mant_logs"))
        i_del = next(i for i, x in enumerate(s) if x.startswith("DELETE FROM mant_tarea_plantilla_items"))
        i_lock = next(i for i, x in enumerate(s) if x.endswith("FOR UPDATE"))
        self.assertLess(i_lock, i_log)
        self.assertLess(i_log, i_del)

    def test_idempotente_y_barata_con_la_marca(self):   # REGLA #18
        m = _Mundo(_k())
        self._correr(m)
        antes = copy.deepcopy(m.items)
        m.consultas, m.sql = 0, []
        out = self._correr(m)
        self.assertTrue(out["ya"])
        self.assertEqual(m.consultas, 1)
        self.assertEqual(m.sql, [])
        self.assertEqual(m.items, antes)

    def test_sin_la_columna_no_migra_ni_marca(self):
        m = _Mundo(_k())
        out = self._correr(m, fsh=False)
        self.assertEqual(out["migrados"], [])
        self.assertEqual(m.logs, [])

    def test_si_cambio_entre_medio_no_se_toca(self):
        k = _k()
        m = _Mundo(k)
        real = m.get_mysql

        antes = copy.deepcopy(m.items)

        def conn_que_ve_un_cambio():   # alguien editó todos los bloques entre el plan y el candado
            c = real()
            for pid in c.tx_items:
                c.tx_items[pid][0]["titulo"] += " (editado por Lenin recién)"
            return c
        m.get_mysql = conn_que_ve_un_cambio
        out = self._correr(m)
        self.assertEqual(out["migrados"], [])                                   # releyó con FOR UPDATE y no tocó
        self.assertEqual(m.items, antes)

    def test_corre_al_arrancar_despues_de_la_semilla(self):
        app = _leer("app.py")
        i_cols = app.index("        _ensure_foto_si_hallazgo_cols()\nexcept Exception as _ensure_fsh_err:")
        i_her = app.index("_chk_her = _ensure_checklist_herencia()")
        i_mig = app.index("_rev_es = _ensure_bloques_revision_esencial()")
        self.assertLess(i_cols, i_her)
        self.assertLess(i_her, i_mig)


# ═══════════════════════════════ 3) foto_si_hallazgo ═══════════════════════════════
class TestFotoSiHallazgoColumna(unittest.TestCase):
    def test_columnas_baratas_una_clausula_por_sentencia(self):   # REGLA #18
        f = _fuente("_ensure_foto_si_hallazgo_cols")
        self.assertIn("information_schema.COLUMNS", f)
        self.assertIn("ADD COLUMN foto_si_hallazgo TINYINT(1) NOT NULL DEFAULT 0", f)
        self.assertIn('for tabla in ("mant_tarea_plantilla_items", "mant_visita_tareas"):', f)
        self.assertNotIn("ADD COLUMN foto_si_hallazgo TINYINT(1) NOT NULL DEFAULT 0, ADD", f)

    def test_piezas_del_insert(self):
        k = _k()
        self.assertEqual(k["_fsh_ins"](True, {"foto_si_hallazgo": 1}), (", foto_si_hallazgo", ",%s", (1,)))
        self.assertEqual(k["_fsh_ins"](True, {}), (", foto_si_hallazgo", ",%s", (0,)))
        self.assertEqual(k["_fsh_ins"](False, {"foto_si_hallazgo": 1}), ("", "", ()))   # sin columna, como antes

    def test_todos_los_insert_que_copian_items_de_plantilla(self):
        for nombre in ("_mant_visita_crear_core", "_ot2_crear_core", "_ot2_equipo_insertar",
                       "_sincronizar_grupo_desde_plantilla", "mant_visita_aplicar_plantilla",
                       "_ot_aplicar_plantilla_a_equipos", "_ot_aplicar_plantilla_a_equipo_sin_ficha"):
            f = _fuente(nombre)
            self.assertIn("_fsh_ins(_fsh_ok(), it)", f, nombre)
            self.assertIn('+ _p + ")"', f, nombre)
        self.assertEqual(_fuente("_ot2_crear_core").count("_fsh_ins(_fsh_ok(), it)"), 2)   # equipos y productos
        # los SELECT con columnas explícitas la traen
        for nombre in ("_ot2_crear_core", "_ot2_equipo_insertar", "_sincronizar_grupo_desde_plantilla"):
            self.assertIn('opciones_lista_json, target_field" + _fsh_col() + " "', _fuente(nombre), nombre)
        # el tramo de tareas manuales (productos de trabajo interno) también
        self.assertIn('"   item_manual_id" + _c + ") "', _fuente("_ot2_crear_core"))

    def test_editor_de_plantillas(self):
        self.assertIn('"foto_si_hallazgo": 1 if d.get("foto_si_hallazgo") else 0,', _fuente("_validar_item_plantilla"))
        for nombre in ("mant_plantilla_crear", "mant_plantilla_actualizar"):
            self.assertIn("_fsh_ins(_fsh_ok(), it)", _fuente(nombre), nombre)
        self.assertIn('"foto_si_hallazgo": bool(it.get("foto_si_hallazgo")),', _fuente("mant_plantilla_detalle"))
        self.assertIn(">Foto solo si hay hallazgo</label>", P)
        self.assertIn("onchange=\"actualizarItem(${idx}, 'foto_si_hallazgo', this.checked)\"", P)
        self.assertIn("foto_si_hallazgo: !!it.foto_si_hallazgo && (it.tipo_respuesta === 'verificacion' || "
                      "it.tipo_respuesta === 'lista'),", P)

    def test_editar_la_plantilla_no_mezcla_los_productos_de_una_revision(self):
        f = _fuente("_sincronizar_grupo_desde_plantilla")
        self.assertIn("AND maquina_id IS NULL AND item_manual_id=%s", f)
        self.assertIn('_c += ", item_manual_id"', f)
        self.assertIn("pid_destino in _chk_condicional_ids()", f)
        self.assertIn("AND maquina_id IS NULL AND item_manual_id=%s", _fuente("_grupo_ot_sin_editar"))
        put = _fuente("mant_plantilla_actualizar")
        self.assertIn("SELECT DISTINCT visita_id, maquina_id, item_manual_id FROM mant_visita_tareas", put)
        self.assertIn("_sincronizar_grupo_desde_plantilla(_vid_g, _mid_g, pid, item_manual_id=_imid_g)", put)

    def test_la_ficha_recibe_la_columna(self):
        f = _fuente("mant_visita_checklist_get")
        self.assertIn('("t.foto_si_hallazgo" if _fsh_ok() else "0 AS foto_si_hallazgo")', f)


# ═══════════════════════════════ 4) regla pura + espejo JS ═══════════════════════════════
OPC = json.dumps(["Apto", "Apto con observaciones", "Reparar", "Dar de baja", "Fuera de servicio", "No llegó",
                  "Pendiente de diagnóstico"])


def _t(i, tr, titulo, obl=1, comp=0, vj=None, fsh=0, fotos=0, imid=1, mid=None, ops=None):
    return {"id": i, "maquina_id": mid, "item_manual_id": None if mid else imid, "tipo_respuesta": tr,
            "titulo": titulo, "obligatoria": obl, "completada": comp, "valor_json": vj, "foto_si_hallazgo": fsh,
            "n_fotos": fotos, "opciones_lista_json": ops, "plantilla_id": 7 if tr != "check" else None}


def _producto(imid=1, res=None, obs="", falla=False, alerta=False, fotos_res=0, fotos_verif=0, base=10, mid=None):
    """Un producto de la revisión: ancla, foto general, serie, faltan piezas, resultado, observación y 2 ítems del
    bloque (uno con foto si hallazgo)."""
    b = base
    v1 = {"valor": "falla"} if falla else ({"valor": "alerta"} if alerta else {"valor": "aprobado"})
    return [
        _t(b + 0, "check", f"SKU{imid} — Producto", imid=imid, mid=mid),
        _t(b + 1, "foto", "Foto general del equipo", imid=imid, mid=mid),
        _t(b + 2, "serie", "N° de serie", imid=imid, mid=mid),
        _t(b + 3, "sino", "¿Faltan piezas o accesorios?", imid=imid, mid=mid),
        _t(b + 4, "lista", "Resultado del control", comp=1 if res else 0, vj={"opcion": res} if res else None,
           fsh=1, fotos=fotos_res, ops=OPC, imid=imid, mid=mid),
        _t(b + 5, "texto", "Observación del equipo", comp=1 if len(obs) >= 10 else 0,
           vj={"texto": obs} if obs else None, imid=imid, mid=mid),
        _t(b + 6, "verificacion", "CRÍTICO · Piola o correa y terminales", comp=1, vj=v1, fsh=1, fotos=fotos_verif,
           imid=imid, mid=mid),
        _t(b + 7, "verificacion", "Poleas, pivotes y bujes", comp=1, vj={"valor": "aprobado"}, imid=imid, mid=mid),
    ]


CASOS = {
    "no_llego_sin_argumento": _producto(res="No llegó"),
    "no_llego_con_argumento": _producto(res="No llegó", obs="Se esperaba el 17, preguntado a bodega"),
    "falla_sin_foto": _producto(res="Reparar", obs="Piola con hebra rota", falla=True),
    "falla_con_fotos": _producto(res="Reparar", obs="Piola con hebra rota", falla=True, fotos_res=1, fotos_verif=2),
    "apto_con_falla": _producto(res="Apto", obs="sin novedad, todo ok", falla=True, fotos_verif=1),
    "apto_obs_con_alerta": _producto(res="Apto con observaciones", obs="Óxido superficial leve", alerta=True,
                                     fotos_verif=1),
    "apto_limpio": _producto(res="Apto", obs="sin novedad, todo ok"),
    "equipo_con_ficha_no_llego": _producto(res="No llegó", mid=55),
    "dos_productos": (_producto(imid=1, res="No llegó", base=10)
                      + _producto(imid=2, res="Apto", obs="sin novedad, todo ok", base=30)),
    "na": [dict(_t(1, "verificacion", "Freno", vj={"na": True, "na_motivo": "no tiene"}, fsh=1, comp=1))],
}


def _con_resultado_na(filas, tid):
    for f in filas:
        if f["id"] == tid:
            f["valor_json"] = {"na": True, "na_motivo": "no corresponde revisarlo"}
            f["completada"] = 1
    return filas


# 🧾 2026-10-19 (revisión de la Etapa 2): un Resultado en «No aplica» no es un resultado.
CASOS["resultado_na_con_falla"] = _con_resultado_na(
    _producto(obs="sin novedad, todo ok", falla=True, fotos_verif=1), 14)
CASOS["resultado_na_equipo_con_ficha"] = _con_resultado_na(
    _producto(obs="sin novedad, todo ok", mid=56, base=40), 44)


class TestReglaPura(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.k = _k()
        cls.ev = staticmethod(cls.k["_chk_revision_evaluar"])

    def _pend(self, caso):
        r = self.ev(CASOS[caso])
        return {tid: e["motivo"] for tid, e in r["por_tarea"].items() if e["exigible"] and not e["hecha"]}, r

    def test_no_llego_solo_pide_el_argumento(self):
        pend, r = self._pend("no_llego_sin_argumento")
        self.assertEqual(r["no_llego"], {1})
        self.assertEqual(pend, {15: "argumento"})
        for tid in (10, 11, 12, 13):
            self.assertTrue(r["por_tarea"][tid]["dispensada"])
        self.assertEqual(self._pend("no_llego_con_argumento")[0], {})

    def test_cambiar_el_resultado_vuelve_a_exigir_todo(self):
        pend, r = self._pend("apto_limpio")
        self.assertEqual(r["no_llego"], set())
        self.assertEqual(set(pend), {10, 11, 12, 13})
        self.assertTrue(all(m == "obligatoria" for m in pend.values()))

    def test_foto_si_hallazgo(self):
        pend, _ = self._pend("falla_sin_foto")
        self.assertEqual(pend[14], "foto_hallazgo")     # Resultado «Reparar» sin foto del daño
        self.assertEqual(pend[16], "foto_hallazgo")     # verificación en Falla sin foto
        self.assertNotIn(17, pend)                       # Aprobado: no pide foto
        pend, _ = self._pend("falla_con_fotos")
        self.assertNotIn(14, pend)
        self.assertNotIn(16, pend)

    def test_apto_con_falla_es_incoherente(self):
        pend, _ = self._pend("apto_con_falla")
        self.assertEqual(pend[14], "incoherente")
        pend, _ = self._pend("apto_obs_con_alerta")
        self.assertEqual(pend.get(14), "foto_hallazgo")   # con una Alerta vale (no es incoherente); pide la foto

    def test_no_llego_solo_dispensa_productos_de_trabajo_interno(self):
        pend, r = self._pend("equipo_con_ficha_no_llego")
        self.assertEqual(r["no_llego"], set())
        self.assertIn(10, pend)

    def test_cada_producto_es_su_propio_grupo(self):
        pend, r = self._pend("dos_productos")
        self.assertEqual(r["no_llego"], {1})
        self.assertEqual(set(pend), {15, 30, 31, 32, 33})

    def test_no_aplica_no_pide_foto(self):
        self.assertEqual(self._pend("na")[0], {})

    def test_resultado_en_no_aplica_queda_pendiente(self):
        """🧾 2026-10-19: el N/A del Resultado saltaba la regla dura y la foto del daño, y la tarea quedaba hecha."""
        pend, _ = self._pend("resultado_na_con_falla")
        self.assertEqual(pend.get(14), "resultado_na")
        pend, _ = self._pend("resultado_na_equipo_con_ficha")
        self.assertEqual(pend.get(44), "resultado_na")


@unittest.skipUnless(HAY_NODE, "node no está instalado")
class TestEspejoJS(unittest.TestCase):
    FUNCS = ("otdChkNorm", "otdChkVj", "otdChkOpciones", "otdChkEsResultado", "otdChkOpcion", "otdChkValorVerif",
             "otdChkGrupoClave", "otdChkEvaluar", "otdChkBaseMarcoObs")

    def _js(self, cuerpo):
        fuente = "\n".join(_js_funcion(n, D) for n in self.FUNCS)
        return _node(fuente + "\n" + cuerpo)

    def test_misma_respuesta_que_python_en_todos_los_casos(self):
        ev = _k()["_chk_revision_evaluar"]
        js = self._js("const C = " + json.dumps(CASOS, ensure_ascii=False) + ";\n"
                      "const out = {}; for (const k in C){ const r = otdChkEvaluar(C[k]); "
                      "out[k] = {no_llego: r.no_llego.sort(), por_tarea: r.por_tarea}; }\n"
                      "console.log(JSON.stringify(out));")
        for caso, tareas in CASOS.items():
            py = ev(tareas)
            self.assertEqual(sorted(py["no_llego"]), js[caso]["no_llego"], caso)
            self.assertEqual({str(k): v for k, v in py["por_tarea"].items()}, js[caso]["por_tarea"], caso)

    def test_estructura_y_tapiz_con_las_opciones_nuevas(self):
        def caso(res=None, falta=None, verif=None):
            base = [_t(1, "lista", "Resultado del control", vj={"opcion": res} if res else None, ops=OPC),
                    _t(2, "sino", "¿Faltan piezas o accesorios?", vj={"valor": falta} if falta else None)]
            sist = [_t(3, "verificacion", "Stack", vj={"valor": verif} if verif else None)]
            return [{"tareas": base}, {"tareas": sist}, {"tareas": []}]
        casos = {"apto": caso("Apto"), "no_llego": caso("No llegó"), "reparar": caso("Reparar"),
                 "dar_baja": caso("Dar de baja"), "fuera": caso("Fuera de servicio"),
                 "pendiente": caso("Pendiente de diagnóstico"), "apto_obs": caso("Apto con observaciones"),
                 "falta_si": caso("Apto", falta="si"), "alerta": caso("Apto", verif="alerta"),
                 "falla": caso(verif="falla"), "nada": caso()}
        out = self._js("const C = " + json.dumps(casos, ensure_ascii=False) + ";\n"
                       "const o = {}; for (const k in C){ o[k] = otdChkBaseMarcoObs(C[k], C[k][2]); }\n"
                       "console.log(JSON.stringify(o));")
        self.assertEqual(out, {"apto": False, "no_llego": False, "reparar": True, "dar_baja": True, "fuera": True,
                               "pendiente": True, "apto_obs": True, "falta_si": True, "alerta": True,
                               "falla": True, "nada": False})


class TestFichaJS(unittest.TestCase):
    def test_la_ficha_usa_el_espejo_para_contar(self):
        r = _js_funcion("otdRenderChecklistEquipo", D)
        self.assertIn("const _ev = otdChkEvaluar(_todas);", r)
        self.assertIn("const oblT = g.tareas.filter(t => otdChkExig(t)).length;", r)
        self.assertIn("if (otdChkExig(t)){ oblTot++; if (otdChkHecha(t)) oblOk++; }", r)
        self.assertIn("otdChkBarraResultado(_resultado, _ev.no_llego.length > 0)", r)

    def test_no_llego_y_foto_se_ven(self):
        barra = _js_funcion("otdChkBarraResultado", D)
        self.assertIn("<b>No llegó</b> · solo se exige el argumento", barra)
        for o in ("'dar de baja'", "'fuera de servicio'", "'no llego'"):
            self.assertIn(o, barra)
        self.assertIn("otdChkResultadoRapido(", barra)
        rap = _js_funcion("otdChkResultadoRapido", D)
        self.assertIn("await ilusConfirm(", rap)
        self.assertIn("await otdChkGuardar(tid, opcion);", rap)      # escribe el MISMO Resultado (fuente única)
        tarea = _js_funcion("otdChkTareaHtml", D)
        self.assertIn("Falta la foto del hallazgo", tarea)
        self.assertIn("No se exige · no llegó", tarea)
        self.assertIn("Con una Falla no vale «Apto»", tarea)
        self.assertIn("'Falta foto del hallazgo'", _js_funcion("otdChkFotoBtnHtml", D))

    def test_regla_dura_en_la_ficha(self):
        g = _js_funcion("otdChkGuardar", D)
        self.assertIn("if ((_op === 'apto' || _op === 'apto con observaciones') && _fallas.length){", g)
        self.assertIn("await ilusAlert({", g)
        self.assertIn("if (_op === 'apto' && _herm.some(h => otdChkValorVerif(h) === 'alerta')){", g)
        self.assertIn("await ilusConfirm({", g)
        self.assertIn("d.error_codigo === 'RESULTADO_CON_FALLA'", g)

    def test_no_llego_no_pide_foto_en_la_ficha(self):
        """🧾 2026-10-19 (revisión de la Etapa 2): con «No llegó», la Foto general salía como «Paso actual» con
        «Necesita foto» y «Falta foto» al lado de «No se exige · no llegó»."""
        r = _js_funcion("otdRenderChecklistEquipo", D)
        self.assertIn("if (t._ev && t._ev.dispensada) return false;", r)
        self.assertIn("return !_noLlegoP && !t.completada;", r)
        self.assertNotIn("g.tareas.findIndex(t => !t.completada)", r)
        tarea = _js_funcion("otdChkTareaHtml", D)
        self.assertIn("if ((t.requiere_foto || tipo === 'foto') && !(_evT && _evT.dispensada)){", tarea)
        self.assertIn("if (esSiguiente && !(otdChkExig(t) ? otdChkHecha(t) : t.completada)){", tarea)
        foto = _js_funcion("otdChkFotoBtnHtml", D)
        self.assertIn("const need = !_disp && !(t.fotos && t.fotos.length);", foto)

    def test_sin_popups_nativos(self):   # REGLA #1
        for n in ("otdChkEvaluar", "otdChkBarraResultado", "otdChkResultadoRapido", "otdChkGuardar",
                  "otdChkBaseMarcoObs", "otdChkHermanas"):
            self.assertIsNone(re.search(r"(?<![\w.])(alert|confirm|prompt)\(", _js_funcion(n, D)), n)

    def test_toques_de_44px(self):   # REGLA #3
        self.assertIn(".otd-chk-resbar button{min-height:44px;", D)


# ═══════════════════════════════ 5) regla dura en el servidor ═══════════════════════════════
class TestReglaDuraServidor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.rech = staticmethod(_k()["_chk_resultado_rechazo"])

    def test_con_falla_no_vale_apto_ni_apto_con_observaciones(self):
        herm = [{"titulo": "CRÍTICO · Piola", "tipo_respuesta": "verificacion", "valor_json": {"valor": "falla"}}]
        for op in ("Apto", "Apto con observaciones", "  apto  "):
            err, aviso = self.rech(op, herm)
            self.assertIn("Con un ítem en Falla no vale", err)
            self.assertIn("CRÍTICO · Piola", err)
        for op in ("Reparar", "Dar de baja", "Fuera de servicio", "No llegó", "Pendiente de diagnóstico"):
            self.assertEqual(self.rech(op, herm), (None, None))

    def test_con_alerta_apto_solo_avisa(self):
        herm = [{"titulo": "Stack", "tipo_respuesta": "verificacion", "valor_json": json.dumps({"valor": "alerta"})}]
        err, aviso = self.rech("Apto", herm)
        self.assertIsNone(err)
        self.assertIn("Apto con observaciones", aviso)
        self.assertEqual(self.rech("Apto con observaciones", herm), (None, None))

    def test_una_falla_marcada_no_aplica_no_cuenta(self):
        herm = [{"titulo": "Freno", "tipo_respuesta": "verificacion",
                 "valor_json": {"na": True, "valor_previo": {"valor": "falla"}}}]
        self.assertEqual(self.rech("Apto", herm), (None, None))

    def test_el_resultado_no_admite_no_aplica(self):
        """🧾 2026-10-19 (revisión de la Etapa 2): «No aplica» en el Resultado no pasaba por la regla dura."""
        f = _fuente("mant_visita_tarea_respuesta")
        self.assertIn('if tipo == "lista" and _chk_es_resultado(tar):', f)
        self.assertIn('"error_codigo": "RESULTADO_SIN_NA"', f)
        self.assertLess(f.index("RESULTADO_SIN_NA"), f.index("_na_ok = True"))
        self.assertLess(f.index('if d.get("na") and tipo in'), f.index("RESULTADO_SIN_NA"))
        # la ficha tampoco ofrece el botón en el Resultado
        na = _js_funcion("otdChkNoAplicaHtml", D)
        self.assertIn("if (otdChkEsResultado(t)) return '';", na)
        self.assertIn("Elige un resultado: «No aplica» no vale aquí", _js_funcion("otdChkTareaHtml", D))

    def test_el_endpoint_rechaza_antes_de_guardar(self):
        f = _fuente("mant_visita_tarea_respuesta")
        self.assertIn("_rech, _aviso = _chk_resultado_rechazo(v, _chk_hermanas(vid, tar))", f)
        self.assertIn('"error_codigo": "RESULTADO_CON_FALLA", "error": _rech}), 409', f)
        self.assertLess(f.index("RESULTADO_CON_FALLA"), f.index("UPDATE mant_visita_tareas SET {"))
        self.assertIn("item_manual_id, opciones_lista_json", f)
        h = _fuente("_chk_hermanas")
        self.assertIn("maquina_id IS NULL AND item_manual_id=%s AND id<>%s", h)


# ═══════════════════════════════ 6) candados: fuente única ═══════════════════════════════
class TestCandadosFuenteUnica(unittest.TestCase):
    def test_cada_candado_llama_a_la_funcion_unica(self):
        for nombre, llamada in (
                ("mant_ot_firmar_revision", "_rev = _ot_chk_revision_pendientes(vid, excluir_maquinas)"),
                ("mant_ot_puede_firmar", "_rev = _ot_chk_revision_pendientes(vid, excluir_maquinas)"),
                ("_ot_validar_cierre", "_rev = _ot_chk_revision_pendientes(vid, excluir_maquinas)"),
                ("ot2_detalle", "_rev_detalle = _ot_chk_revision_pendientes(vid)")):
            self.assertIn(llamada, _fuente(nombre), nombre)
        self.assertIn("if faltan_rows or _rev[\"faltantes\"]:", _fuente("mant_ot_firmar_revision"))
        self.assertIn("if faltan_rows or _rev[\"faltantes\"]:", _fuente("mant_ot_puede_firmar"))
        self.assertIn("_ot_chk_revision_pendientes(vid)", _fuente("_ot_items_manuales_no_llego"))
        self.assertIn("ev = _chk_revision_evaluar(filas)", _fuente("_ot_chk_revision_pendientes"))
        d = _fuente("ot2_detalle")
        self.assertIn('_ev_t = _rev_detalle["por_tarea"].get(t.get("id")) if t.get("item_manual_id") else None', d)
        self.assertIn('_obl += int((_rev_detalle.get("kpi_delta") or {}).get("obl") or 0)', d)

    def _pendientes(self, filas, excl=()):
        amb = _cargar(_PURAS + ("_ot_chk_revision_pendientes", "_ot_items_manuales_no_llego"), _CONSTS)
        amb.update({"_fsh_ok": lambda: True, "_ot_maquinas_excluidas_cierre": lambda vid: set(excl),
                    "mysql_fetchall": lambda sql, params=None: [dict(f) for f in filas],
                    "print": lambda *a, **k: None})
        return amb

    def test_no_llego_en_la_firma_y_el_cierre(self):
        amb = self._pendientes(CASOS["dos_productos"])
        r = amb["_ot_chk_revision_pendientes"](1)
        self.assertEqual(r["no_llego"], {1})
        self.assertEqual([p["tarea_id"] for p in r["pendientes"] if p["item_manual_id"] == 1], [15])
        self.assertEqual(r["faltantes"], ["SKU1 — Producto: falta el argumento de «No llegó» en la observación",
                                          "SKU2 — Producto: 4 tarea(s) obligatoria(s) sin completar"])
        self.assertEqual((r["internas_obl"], r["internas_obl_ok"]), (10, 5))
        self.assertEqual(amb["_ot_items_manuales_no_llego"](1), {1})

    def test_equipo_con_ficha_solo_suma_foto_e_incoherencia(self):
        filas = _producto(res="Apto", obs="sin novedad, todo ok", falla=True, mid=55)
        for f in filas:
            f["maquina_nombre"] = "Optimal Chest Press"
        r = self._pendientes(filas)["_ot_chk_revision_pendientes"](1)
        motivos = sorted(p["motivo"] for p in r["pendientes"])
        self.assertEqual(motivos, ["foto_hallazgo", "incoherente"])   # sus obligatorias las cuentan los de siempre
        self.assertEqual(r["kpi_delta"], {"obl": 0, "obl_ok": -2})
        self.assertEqual(self._pendientes(filas, excl=(55,))["_ot_chk_revision_pendientes"](1)["pendientes"], [])

    def test_resultado_en_no_aplica_en_los_candados(self):
        r = self._pendientes(CASOS["resultado_na_con_falla"])["_ot_chk_revision_pendientes"](1)
        self.assertIn((14, "resultado_na"), [(p["tarea_id"], p["motivo"]) for p in r["pendientes"]])
        self.assertTrue(any("«No aplica»" in x for x in r["faltantes"]))
        filas = [dict(f, maquina_nombre="Optimal Chest Press") for f in CASOS["resultado_na_equipo_con_ficha"]]
        r = self._pendientes(filas)["_ot_chk_revision_pendientes"](1)
        self.assertEqual([p["motivo"] for p in r["pendientes"]], ["resultado_na"])   # en un equipo: solo el ajuste

    def test_si_la_consulta_falla_no_inventa_pendientes(self):
        amb = self._pendientes([])

        def falla(*a, **k):
            raise RuntimeError("sin conexión")
        amb["mysql_fetchall"] = falla
        r = amb["_ot_chk_revision_pendientes"](1)
        self.assertEqual((r["pendientes"], r["faltantes"]), ([], []))


@unittest.skipUnless(HAY_NODE, "node no está instalado")
class TestFotoNoLlegoEnNode(unittest.TestCase):
    def test_boton_neutro_sin_need(self):
        previo = ("function otdBloqueadaPorCierre(){ return false; }\n")
        fuente = previo + _js_funcion("otdChkFotoBtnHtml", D)
        out = _node(fuente + "\n"
                    "const disp = {id: 1, maquina_id: null, tipo_respuesta: 'foto', requiere_foto: 1, fotos: [],"
                    " _ev: {exigible: false, hecha: false, motivo: null, dispensada: true}};\n"
                    "const norm = Object.assign({}, disp, {_ev: {exigible: true, hecha: false, motivo: 'obligatoria',"
                    " dispensada: false}});\n"
                    "const a = otdChkFotoBtnHtml(disp), b = otdChkFotoBtnHtml(norm);\n"
                    "console.log(JSON.stringify([a.includes(' need'), a.includes('Falta foto'), a.includes('Agregar foto'),"
                    " b.includes(' need'), b.includes('Falta foto')]));")
        self.assertEqual(out, [False, False, True, True, True])


# ═══════════════════════════════ 7) PDF probatorio = misma fuente que firma y cierre ═══════════════════════════════
_PDF_CONSTS = ("_OT_PDF_REVISION", "_OT_PDF_RAZON_SALTADO", "_OT_PDF_ESTADO_CAPTURADO", "_OT_PDF_CAUSA_RAIZ",
               "_OT_PDF_TIPO_FOTO", "_OT_PDF_REV_MOTIVO")


def _pdf_amb():
    amb = _cargar(_PURAS + ("_tarea_es_hallazgo", "_ot_pdf_probatorio"), _CONSTS + _PDF_CONSTS)
    amb.update({"chile_fmt_filter": lambda v, fmt=None: str(v), "_ot_pdf_ident_cuenta": lambda *a: "",
                "mysql_fetchone": lambda *a, **k: None, "_ot_tipo_label_efectivo": lambda *a: "Control de calidad",
                "_formato_rut_chile": lambda r: r, "_ot_pdf_hhmm": lambda v: "", "_ot_pdf_anexo_max": lambda: 12,
                "ILUS_BRAND": "ILUS Fitness", "ILUS_LEGAL": "x", "ILUS_RUT": "x",
                "_tarea_es_seguridad": lambda *a: False})
    return amb


def _pdf_filas(tareas):
    return [{"id": t["id"], "titulo": t["titulo"], "tiene_foto": bool(t.get("n_fotos")), "resultado_clase": None}
            for t in tareas]


class TestPdfProbatorio(unittest.TestCase):
    """🧾 2026-10-19 (revisión de la Etapa 2, Daniel: la OT es evidencia): el PDF no puede decir «obligatoria sin
    completar» de lo que la firma y el cierre ya no exigen («No llegó»), y nombra los pendientes de la revisión."""

    def _pdf(self, tareas, con_eval=True):
        amb = _pdf_amb()
        tareas = copy.deepcopy(tareas)
        for t in tareas:
            if t["tipo_respuesta"] == "foto":
                t["requiere_foto"] = 1
        ev = amb["_chk_revision_evaluar"](tareas) if con_eval else None
        return amb["_ot_pdf_probatorio"]({}, [], tareas, _pdf_filas(tareas), [], rev_eval=ev)

    def test_no_llego_no_suma_obligatorias_ni_fotos(self):
        prod = _producto(res="No llegó", obs="Se esperaba el 17, preguntado a bodega")
        sin = self._pdf(prod, con_eval=False)
        self.assertEqual(sin["tareas_oblig_pend_n"], 4)          # lo que decía antes (contradecía al cierre)
        con = self._pdf(prod)
        self.assertEqual(con["tareas_oblig_pend_n"], 0)
        self.assertEqual(con["tareas_sin_foto_n"], 0)
        self.assertFalse([h for h in con["hallazgos"] if h["cat"] == "Tarea obligatoria sin completar"])
        disp = {f["id"] for f in con["tareas_chk"] if f["no_se_exige"]}
        self.assertTrue({10, 11, 12, 13} <= disp)
        self.assertFalse({14, 15} & disp)          # el propio «No llegó» y la observación (el argumento)

    def test_pendientes_de_la_revision_con_nombre_propio(self):
        r = self._pdf(_producto(res="Reparar", obs="Piola con hebra rota", falla=True))
        cats = [h["cat"] for h in r["hallazgos"]]
        self.assertEqual(cats.count("Foto del hallazgo sin registrar"), 2)
        r = self._pdf(_producto(res="Apto", obs="sin novedad, todo ok", falla=True, fotos_verif=1))
        self.assertIn("Resultado «Apto» con un ítem en Falla", [h["cat"] for h in r["hallazgos"]])

    def test_el_contexto_pasa_la_fuente_unica(self):
        f = _fuente("_ot_pdf_context")
        self.assertIn("_rev_pdf = _ot_chk_revision_pendientes(vid)", f)
        self.assertIn("rep_por_maquina=rep_por_maquina, rev_eval=_rev_pdf))", f)
        self.assertIn('"       t.item_manual_id, "', f)
        tpl = _leer("templates/mantenciones/ot_pdf.html")
        self.assertEqual(tpl.count("No se exige · no llegó"), 2)
        self.assertEqual(tpl.count("{{ t.rev_pendiente }}"), 2)


if __name__ == "__main__":
    unittest.main()
